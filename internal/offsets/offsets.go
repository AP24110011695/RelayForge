// Package offsets stores committed consumer-group offsets durably: one
// append-only log per group (the record format carries the CRC and the
// valid-prefix recovery), with the commit payload as a versioned binary
// record. Committed-offset convention: a committed offset is the NEXT
// offset to consume.
package offsets

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"relayforge/internal/logengine"
	"relayforge/internal/record"
)

// PayloadVersion is the only defined commit-payload version.
const PayloadVersion = 1

// MinPayloadSize: version(1) + partition(4) + offset(8) + topicLen(4).
const MinPayloadSize = 17

// MaxPayloadSize bounds a commit payload (topic names are bounded by the
// metadata store at 128 bytes).
const MaxPayloadSize = 512

// Record is one committed offset.
type Record struct {
	Topic     string
	Partition int32
	Offset    uint64
}

// Manager is the durable offset store for one consumer group.
type Manager struct {
	mu        sync.RWMutex
	log       *logengine.Log
	committed map[int32]uint64
}

// OpenManager opens (or creates) the group's offsets log under
// <dataDir>/groups/<group>/offsets and replays the valid prefix into the
// in-memory table (last write per partition wins).
func OpenManager(dataDir, group string) (*Manager, error) {
	dir := filepath.Join(dataDir, "groups", group, "offsets")
	log, err := logengine.Open(dir, logengine.Options{MaxRecordSize: MaxPayloadSize + record.HeaderSize + 64})
	if err != nil {
		return nil, err
	}
	m := &Manager{log: log, committed: map[int32]uint64{}}
	err = log.Scan(func(rec record.Record, offset uint64) error {
		r, err := decodePayload(rec.Value)
		if err != nil {
			return fmt.Errorf("offsets: replay failed at log offset %d: %w", offset, err)
		}
		m.committed[r.Partition] = r.Offset
		return nil
	})
	if err != nil {
		log.Close()
		return nil, err
	}
	return m, nil
}

// Commit durably appends one committed offset (fsynced: commits are
// low-rate and the offset is the durability anchor of at-least-once
// processing).
func (m *Manager) Commit(rec Record) error {
	payload, err := encodePayload(rec)
	if err != nil {
		return err
	}
	if _, err := m.log.Append(record.Record{Value: payload}, logengine.DurabilityFlushed); err != nil {
		return err
	}
	m.mu.Lock()
	m.committed[rec.Partition] = rec.Offset
	m.mu.Unlock()
	return nil
}

// Get returns the committed offset for a partition (false when none).
func (m *Manager) Get(partition int32) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	off, ok := m.committed[partition]
	return off, ok
}

// All returns a copy of the committed table.
func (m *Manager) All() map[int32]uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[int32]uint64, len(m.committed))
	for k, v := range m.committed {
		out[k] = v
	}
	return out
}

// Close closes the underlying log.
func (m *Manager) Close() error { return m.log.Close() }

func encodePayload(rec Record) ([]byte, error) {
	if rec.Partition < 0 {
		return nil, fmt.Errorf("offsets: invalid partition %d", rec.Partition)
	}
	buf := make([]byte, MinPayloadSize+len(rec.Topic))
	buf[0] = PayloadVersion
	binary.LittleEndian.PutUint32(buf[1:5], uint32(rec.Partition))
	binary.LittleEndian.PutUint64(buf[5:13], rec.Offset)
	binary.LittleEndian.PutUint32(buf[13:17], uint32(len(rec.Topic)))
	copy(buf[17:], rec.Topic)
	return buf, nil
}

func decodePayload(payload []byte) (Record, error) {
	if len(payload) < MinPayloadSize {
		return Record{}, fmt.Errorf("%w: payload %d bytes < minimum %d", ErrMalformedPayload, len(payload), MinPayloadSize)
	}
	if payload[0] != PayloadVersion {
		return Record{}, fmt.Errorf("%w: version %d", ErrUnsupportedPayload, payload[0])
	}
	partition := int32(binary.LittleEndian.Uint32(payload[1:5]))
	if partition < 0 {
		return Record{}, fmt.Errorf("%w: negative partition %d", ErrMalformedPayload, partition)
	}
	topicLen := binary.LittleEndian.Uint32(payload[13:17])
	if int(topicLen) != len(payload)-MinPayloadSize {
		return Record{}, fmt.Errorf("%w: topic length %d does not match payload", ErrMalformedPayload, topicLen)
	}
	return Record{
		Partition: partition,
		Offset:    binary.LittleEndian.Uint64(payload[5:13]),
		Topic:     string(payload[17:]),
	}, nil
}

// Errors for the commit-payload decoder.
var (
	ErrMalformedPayload     = errors.New("offsets: malformed commit payload")
	ErrUnsupportedPayload   = errors.New("offsets: unsupported commit payload version")
)
