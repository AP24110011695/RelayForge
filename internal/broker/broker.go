// Package broker implements the RelayForge broker: the network-facing layer
// over the log engine. Phase 5 serves a single implicit default topic
// ("default") with one partition (0), already stored under the final layout
// (topics/<name>/partition-<n>) so Phase 6 generalizes without migration.
package broker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"relayforge/internal/logengine"
	"relayforge/internal/record"
)

const (
	// DefaultTopic is the only topic broker v1 serves.
	DefaultTopic = "default"
	// DefaultFetchMaxBytes is used when a FetchRequest carries max_bytes <= 0.
	DefaultFetchMaxBytes = 1 << 20
	// HardFetchMaxBytesCap bounds a single fetch regardless of request.
	HardFetchMaxBytesCap = 8 << 20
	// MaxBatchMessages caps one Produce RPC.
	MaxBatchMessages = 1000
	// MaxBatchBytes caps the total key+value bytes of one Produce RPC.
	MaxBatchBytes            = 8 << 20
	defaultFetchPollInterval = 20 * time.Millisecond
)

// Errors returned by the broker. The gRPC layer maps them to status codes
// (docs/PROTOCOL.md); transport errors from the engine surface unchanged.
var (
	// ErrUnknownTopicOrPartition is returned until topics arrive in
	// Phase 6: broker v1 serves only ("default", 0).
	ErrUnknownTopicOrPartition = errors.New("broker: unknown topic or partition")
	// ErrEmptyBatch is returned for a Produce RPC without messages.
	ErrEmptyBatch = errors.New("broker: produce batch is empty")
	// ErrBatchTooLarge is returned when a Produce RPC exceeds the batch
	// message-count or byte limits.
	ErrBatchTooLarge = errors.New("broker: produce batch too large")
)

// Ack is the broker-facing durability request; the server maps the proto
// AckMode onto it.
type Ack int

const (
	AckNone Ack = iota
	AckWritten
	AckFlushed
)

func (a Ack) durability() logengine.Durability {
	switch a {
	case AckFlushed:
		return logengine.DurabilityFlushed
	case AckNone:
		return logengine.DurabilityNone
	default:
		return logengine.DurabilityWritten
	}
}

// Message is one record to produce. Nil Key/Value are treated as empty.
type Message struct {
	Key   []byte
	Value []byte
}

// FetchedRecord is one record returned by Fetch.
type FetchedRecord struct {
	Offset    uint64
	Key       []byte
	Value     []byte
	Timestamp int64 // milliseconds since the Unix epoch, UTC
}

// Options configures Open. Zero fields take defaults.
type Options struct {
	DataDir           string
	MaxRecordSize     int           // passthrough to the log engine
	FetchPollInterval time.Duration // long-poll cadence; 0 selects the default
}

// Broker serves the default topic over one log-engine partition.
type Broker struct {
	log       *logengine.Log
	fetchPoll time.Duration
}

// Open opens (or creates) the broker's data layout and recovers it.
func Open(opts Options) (*Broker, error) {
	if opts.DataDir == "" {
		return nil, errors.New("broker: DataDir is required")
	}
	dir := filepath.Join(opts.DataDir, "topics", DefaultTopic, "partition-0")
	log, err := logengine.Open(dir, logengine.Options{MaxRecordSize: opts.MaxRecordSize})
	if err != nil {
		return nil, err
	}
	poll := opts.FetchPollInterval
	if poll <= 0 {
		poll = defaultFetchPollInterval
	}
	return &Broker{log: log, fetchPoll: poll}, nil
}

// Produce appends messages and returns their assigned offsets, in request
// order. If an append fails partway, the error is returned together with
// the offsets that did land: those records are visible to consumers but
// were never acknowledged (at-least-once on producer retry).
func (b *Broker) Produce(ctx context.Context, topic string, partition int32, ack Ack, msgs []Message) ([]uint64, error) {
	if err := b.checkRoute(topic, partition); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, ErrEmptyBatch
	}
	if len(msgs) > MaxBatchMessages {
		return nil, fmt.Errorf("%w: %d messages > limit %d", ErrBatchTooLarge, len(msgs), MaxBatchMessages)
	}
	total := 0
	for i := range msgs {
		total += len(msgs[i].Key) + len(msgs[i].Value)
	}
	if total > MaxBatchBytes {
		return nil, fmt.Errorf("%w: %d bytes > limit %d", ErrBatchTooLarge, total, MaxBatchBytes)
	}
	dur := ack.durability()
	offsets := make([]uint64, 0, len(msgs))
	for i := range msgs {
		rec := record.Record{Key: msgs[i].Key, Value: msgs[i].Value}
		off, err := b.log.Append(rec, dur)
		if err != nil {
			return offsets, err
		}
		offsets = append(offsets, off)
	}
	return offsets, nil
}

// Fetch returns records starting at offset, up to maxBytes. When the log is
// at the requested offset (nothing new yet), it long-polls up to maxWait
// for new data; max_wait <= 0 returns immediately. An offset beyond the log
// end fails with logengine.ErrOffsetOutOfRange (mapped to OUT_OF_RANGE).
// Reads stop at the segment boundary; resume from the returned next offset.
func (b *Broker) Fetch(ctx context.Context, topic string, partition int32, offset uint64, maxBytes int, maxWait time.Duration) ([]FetchedRecord, uint64, uint64, error) {
	if err := b.checkRoute(topic, partition); err != nil {
		return nil, 0, 0, err
	}
	if maxBytes <= 0 {
		maxBytes = DefaultFetchMaxBytes
	}
	if maxBytes > HardFetchMaxBytesCap {
		maxBytes = HardFetchMaxBytesCap
	}
	var deadline time.Time
	if maxWait > 0 {
		deadline = time.Now().Add(maxWait)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, err
		}
		hw := b.log.NextOffset()
		recs, next, err := b.log.ReadFrom(offset, maxBytes)
		if err != nil {
			return nil, 0, 0, err
		}
		if len(recs) > 0 {
			return convert(offset, recs), next, b.log.NextOffset(), nil
		}
		if next > offset {
			// Segment boundary: more data may be reachable immediately.
			offset = next
			continue
		}
		if maxWait <= 0 || !time.Now().Before(deadline) {
			return nil, offset, hw, nil
		}
		select {
		case <-ctx.Done():
			return nil, 0, 0, ctx.Err()
		case <-time.After(b.fetchPoll):
		}
	}
}

func (b *Broker) checkRoute(topic string, partition int32) error {
	if topic != DefaultTopic || partition != 0 {
		return fmt.Errorf("%w: broker v1 serves only (%q, %d)", ErrUnknownTopicOrPartition, DefaultTopic, 0)
	}
	return nil
}

// convert pairs records with their offsets: ReadFrom returns records
// contiguously starting at `start`.
func convert(start uint64, recs []record.Record) []FetchedRecord {
	out := make([]FetchedRecord, len(recs))
	for i, r := range recs {
		out[i] = FetchedRecord{Offset: start + uint64(i), Key: r.Key, Value: r.Value, Timestamp: r.Timestamp}
	}
	return out
}

// NextOffset exposes the log end (the high watermark for fetches).
func (b *Broker) NextOffset() uint64 { return b.log.NextOffset() }

// Close stops the broker.
func (b *Broker) Close() error { return b.log.Close() }
