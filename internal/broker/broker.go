// Package broker implements the RelayForge broker: topics with fixed
// partition counts, deterministic key routing, per-partition bounded
// produce queues served by one writer goroutine each (the ordering
// guarantee), and enforced backpressure with explicit overload errors.
//
// Data layout: <dataDir>/topics/<topic>/partition-<n>/ (one log-engine
// directory per partition). Topic metadata lives in internal/metadata.
package broker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"relayforge/internal/group"
	"relayforge/internal/logengine"
	"relayforge/internal/metadata"
	"relayforge/internal/observability"
	"relayforge/internal/offsets"
	"relayforge/internal/record"
)

const (
	// DefaultTopic is created automatically with one partition.
	DefaultTopic = metadata.DefaultTopic
	// DefaultFetchMaxBytes is used when a FetchRequest carries max_bytes <= 0.
	DefaultFetchMaxBytes = 1 << 20
	// HardFetchMaxBytesCap bounds a single fetch regardless of request.
	HardFetchMaxBytesCap = 8 << 20
	// MaxBatchMessages caps one Produce RPC.
	MaxBatchMessages = 1000
	// MaxBatchBytes caps the total key+value bytes of one Produce RPC.
	MaxBatchBytes = 8 << 20
	// DefaultProduceQueueDepth is the bounded per-partition queue depth.
	DefaultProduceQueueDepth = 256
	defaultFetchPollInterval = 20 * time.Millisecond
)

// Errors returned by the broker. The gRPC layer maps them to status codes
// (docs/PROTOCOL.md); transport errors from the engine surface unchanged.
var (
	// ErrUnknownTopicOrPartition means the route does not exist.
	ErrUnknownTopicOrPartition = errors.New("broker: unknown topic or partition")
	// ErrEmptyBatch is returned for a Produce RPC without messages.
	ErrEmptyBatch = errors.New("broker: produce batch is empty")
	// ErrBatchTooLarge is returned when a Produce RPC exceeds the batch
	// message-count or byte limits.
	ErrBatchTooLarge = errors.New("broker: produce batch too large")
	// ErrOverloaded means a partition queue stayed full for the whole
	// (deadline-bounded) wait: explicit, retryable backpressure.
	ErrOverloaded = errors.New("broker: partition queue overloaded")
	// ErrPartitionFailed means the partition's writer panicked and the
	// partition no longer accepts produces (other partitions unaffected).
	ErrPartitionFailed = errors.New("broker: partition writer failed")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("broker: broker is closed")
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

// TopicInfo describes a topic (mirrors metadata.Topic for callers).
type TopicInfo = metadata.Topic

// Options configures Open. Zero fields take defaults.
type Options struct {
	DataDir           string
	MaxRecordSize     int           // passthrough to the log engine
	FetchPollInterval time.Duration // long-poll cadence; 0 selects the default
	ProduceQueueDepth int           // bounded per-partition queue; 0 selects the default
	Metrics           *observability.Metrics
}

type partition struct {
	topic     string
	id        int32
	log       *logengine.Log
	queue     chan produceItem
	done      chan struct{}
	failed    atomic.Bool
	metrics   *observability.Metrics
	writeHook func() // test-only stall point before the append; nil in production
}

type produceItem struct {
	rec    record.Record
	dur    logengine.Durability
	result chan produceResult
}

type produceResult struct {
	offset uint64
	err    error
}

// Broker serves topics over partitioned logs.
type Broker struct {
	mu            sync.RWMutex
	closed        bool
	inflight      sync.WaitGroup // produces between the closed-check and queue send
	store         *metadata.Store
	parts         map[string][]*partition // topic -> partitions
	groups        *group.Coordinator
	metrics       *observability.Metrics
	queueDepth    int
	maxRecordSize int
	rr            atomic.Uint32 // keyless round-robin counter
	fetchPoll     time.Duration
}

// Open opens (or creates) the data layout, loads topic metadata, and opens
// every partition of every topic. A corrupt metadata snapshot makes Open
// fail (never guessed at).
func Open(opts Options) (*Broker, error) {
	if opts.DataDir == "" {
		return nil, errors.New("broker: DataDir is required")
	}
	store, err := metadata.Open(opts.DataDir)
	if err != nil {
		return nil, err
	}
	depth := opts.ProduceQueueDepth
	if depth <= 0 {
		depth = DefaultProduceQueueDepth
	}
	poll := opts.FetchPollInterval
	if poll <= 0 {
		poll = defaultFetchPollInterval
	}
	b := &Broker{
		store:         store,
		parts:         map[string][]*partition{},
		queueDepth:    depth,
		maxRecordSize: opts.MaxRecordSize,
		metrics:       opts.Metrics,
		fetchPoll:     poll,
	}
	if _, ok := store.Get(DefaultTopic); !ok {
		if err := store.Create(DefaultTopic, 1); err != nil {
			return nil, err
		}
	}
	for _, t := range store.List() {
		parts := make([]*partition, 0, t.PartitionCount)
		for i := int32(0); i < t.PartitionCount; i++ {
			p, err := b.openPartition(t.Name, i)
			if err != nil {
				b.closePartsAndWait(map[string][]*partition{t.Name: parts})
				return nil, err
			}
			parts = append(parts, p)
		}
		b.parts[t.Name] = parts
	}
	b.groups = group.NewCoordinator(opts.DataDir, func(topic string) (int32, bool) {
		t, ok := store.Get(topic)
		if !ok {
			return 0, false
		}
		return t.PartitionCount, true
	}, 0)
	var recovered int64
	for _, list := range b.parts {
		for _, p := range list {
			recovered += p.log.TruncatedOnRecover()
		}
	}
	b.metrics.RecoveryObs(recovered)
	return b, nil
}

func (b *Broker) openPartition(topic string, id int32) (*partition, error) {
	dir := filepath.Join(b.store.Dir(), "topics", topic, fmt.Sprintf("partition-%d", id))
	log, err := logengine.Open(dir, logengine.Options{MaxRecordSize: b.maxRecordSize})
	if err != nil {
		return nil, err
	}
	p := &partition{
		topic:   topic,
		id:      id,
		log:     log,
		queue:   make(chan produceItem, b.queueDepth),
		done:    make(chan struct{}),
		metrics: b.metrics,
	}
	go p.writerLoop()
	return p, nil
}

// writerLoop is the single serializer for a partition: it owns the log's
// append path, so queue arrival order is append order. A panicking append
// marks the partition failed; the loop keeps consuming and fails fast so
// no producer is ever left waiting and other partitions are unaffected.
func (p *partition) writerLoop() {
	defer close(p.done)
	for item := range p.queue {
		if p.failed.Load() {
			item.result <- produceResult{err: ErrPartitionFailed}
			continue
		}
		p.process(item)
	}
}

func (p *partition) process(item produceItem) {
	defer func() {
		if r := recover(); r != nil {
			p.failed.Store(true)
			p.metrics.PartitionFailInc()
			item.result <- produceResult{err: fmt.Errorf("%w: %v", ErrPartitionFailed, r)}
		}
	}()
	if p.writeHook != nil {
		p.writeHook()
	}
	start := time.Now()
	off, err := p.log.Append(item.rec, item.dur)
	p.metrics.ProduceObs(p.topic, p.id, 1, len(item.rec.Key)+len(item.rec.Value), ackName(item.dur), time.Since(start))
	p.metrics.QueueObs(p.topic, p.id, len(p.queue))
	item.result <- produceResult{offset: off, err: err}
}

func ackName(d logengine.Durability) string {
	switch d {
	case logengine.DurabilityFlushed:
		return "flushed"
	case logengine.DurabilityNone:
		return "none"
	default:
		return "written"
	}
}

// CreateTopic adds a topic with a fixed partition count (resizing is not
// supported) and opens its partitions.
func (b *Broker) CreateTopic(name string, partitionCount int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if _, ok := b.parts[name]; ok {
		return fmt.Errorf("%w: %q", metadata.ErrTopicExists, name)
	}
	if err := b.store.Create(name, partitionCount); err != nil {
		return err
	}
	parts := make([]*partition, 0, partitionCount)
	for i := int32(0); i < partitionCount; i++ {
		p, err := b.openPartition(name, i)
		if err != nil {
			// Roll back: close the partitions opened so far and remove
			// the metadata entry, so the broker never serves a
			// half-created topic.
			for _, p := range parts {
				p.closeAndWait()
			}
			_ = b.store.Delete(name)
			return err
		}
		parts = append(parts, p)
	}
	b.parts[name] = parts
	return nil
}

// ListTopics returns all topics sorted by name.
func (b *Broker) ListTopics() []TopicInfo { return b.store.List() }

// DescribeTopic returns one topic.
func (b *Broker) DescribeTopic(name string) (TopicInfo, error) {
	t, ok := b.store.Get(name)
	if !ok {
		return TopicInfo{}, fmt.Errorf("%w: %q", ErrUnknownTopicOrPartition, name)
	}
	return t, nil
}

// Produce routes messages (keyed: FNV-1a; keyless: round-robin), submits
// them to the per-partition bounded queues, and returns one offset per
// message in request order.
//
// Backpressure: a full partition queue blocks until the context deadline
// (the producer's timeout), then fails with ErrOverloaded. Memory is
// bounded by queue depth × max record size per partition.
func (b *Broker) Produce(ctx context.Context, topic string, partitionID int32, ack Ack, msgs []Message) ([]uint64, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrClosed
	}
	// Registered under the lock so Close (which takes the write lock,
	// then waits for in-flight produces) can never close a queue while a
	// producer is still submitting.
	b.inflight.Add(1)
	parts, ok := b.parts[topic]
	b.mu.RUnlock()
	defer b.inflight.Done()
	if !ok || partitionID >= int32(len(parts)) {
		return nil, fmt.Errorf("%w: (%q, %d)", ErrUnknownTopicOrPartition, topic, partitionID)
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

	// Routing: a non-negative partition field pins every message to that
	// partition (explicit routing); a negative field routes per message —
	// keyed messages hash deterministically (FNV-1a), keyless ones
	// round-robin. Grouping preserves per-partition order.
	type routed struct {
		p     *partition
		items []produceItem
	}
	groups := map[int32]*routed{}
	order := []int32{}
	for i := range msgs {
		var target int32
		if partitionID >= 0 {
			target = partitionID
		} else {
			target = b.route(msgs[i].Key, len(parts))
		}
		g := groups[target]
		if g == nil {
			g = &routed{p: parts[target]}
			groups[target] = g
			order = append(order, target)
		}
		g.items = append(g.items, produceItem{
			rec:    record.Record{Key: msgs[i].Key, Value: msgs[i].Value},
			dur:    ack.durability(),
			result: make(chan produceResult, 1),
		})
	}

	// Submit in partition order; a full queue blocks until the deadline.
	for _, pid := range order {
		g := groups[pid]
		for j := range g.items {
			select {
			case g.p.queue <- g.items[j]:
			case <-ctx.Done():
				b.metrics.OverloadInc()
				return nil, fmt.Errorf("%w: queue full for partition %d while submitting: %v",
					ErrOverloaded, pid, ctx.Err())
			}
		}
	}
	// Collect results in request order.
	offsets := make([]uint64, len(msgs))
	i := 0
	for _, pid := range order {
		g := groups[pid]
		for j := range g.items {
			select {
			case res := <-g.items[j].result:
				if res.err != nil {
					return offsets, res.err
				}
				offsets[i] = res.offset
			case <-ctx.Done():
				return offsets, ctx.Err()
			}
			i++
		}
	}
	return offsets, nil
}

// route picks a partition for auto-routed messages: keyed messages hash
// deterministically (FNV-1a), keyless messages round-robin.
func (b *Broker) route(key []byte, count int) int32 {
	if count == 1 {
		return 0
	}
	if key != nil {
		h := fnv.New32a()
		h.Write(key)
		return int32(h.Sum32() % uint32(count))
	}
	return int32(b.rr.Add(1)-1) % int32(count)
}

// Fetch reads from one partition, long-polling when at the log end.
func (b *Broker) Fetch(ctx context.Context, topic string, partitionID int32, offset uint64, maxBytes int, maxWait time.Duration) ([]FetchedRecord, uint64, uint64, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, 0, 0, ErrClosed
	}
	parts, ok := b.parts[topic]
	b.mu.RUnlock()
	if !ok || partitionID < 0 || int(partitionID) >= len(parts) {
		return nil, 0, 0, fmt.Errorf("%w: (%q, %d)", ErrUnknownTopicOrPartition, topic, partitionID)
	}
	p := parts[partitionID]
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
	start := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, err
		}
		hw := p.log.NextOffset()
		recs, next, err := p.log.ReadFrom(offset, maxBytes)
		if err != nil {
			return nil, 0, 0, err
		}
		if len(recs) > 0 {
			var bytesOut int
			for _, r := range recs {
				bytesOut += len(r.Key) + len(r.Value)
			}
			b.metrics.FetchObs(topic, partitionID, len(recs), bytesOut, time.Since(start))
			return convert(offset, recs), next, p.log.NextOffset(), nil
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

// convert pairs records with their offsets: ReadFrom returns records
// contiguously starting at `start`.
func convert(start uint64, recs []record.Record) []FetchedRecord {
	out := make([]FetchedRecord, len(recs))
	for i, r := range recs {
		out[i] = FetchedRecord{Offset: start + uint64(i), Key: r.Key, Value: r.Value, Timestamp: r.Timestamp}
	}
	return out
}

// Close rejects new work, waits for in-flight produces, drains the bounded
// queues (their appends complete), and closes all partition logs. Metadata
// persists synchronously on every mutation, so no metadata close step is
// needed.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	b.closed = true
	parts := b.parts
	b.parts = map[string][]*partition{}
	b.mu.Unlock()
	// In-flight producers finish before any queue is closed: with the
	// closed flag set under the lock, no producer can start or be
	// mid-submit once this wait returns.
	b.inflight.Wait()
	b.closePartsAndWait(parts)
	b.groups.Close()
	return nil
}

// --- Consumer groups (delegating to the coordinator) ---

// JoinGroup adds (or re-joins) a member to a consumer group and triggers a
// rebalance.
func (b *Broker) JoinGroup(group, memberID, topic string, sessionTimeout time.Duration) (int64, string, error) {
	gen, id, err := b.groups.Join(group, memberID, topic, sessionTimeout)
	if err == nil {
		if _, members, derr := b.groups.Describe(group); derr == nil {
			b.metrics.MembersObs(group, len(members))
		}
	}
	return gen, id, err
}

// SyncGroup returns the member's subscribed topic and its assignment for
// the given generation.
func (b *Broker) SyncGroup(group string, generation int64, memberID string) (string, map[int32]bool, error) {
	return b.groups.Sync(group, generation, memberID)
}

// Heartbeat refreshes a member's session; stale generations are fenced.
func (b *Broker) Heartbeat(group string, generation int64, memberID string) error {
	return b.groups.Heartbeat(group, generation, memberID)
}

// CommitOffset durably stores a fenced offset commit. The committed offset
// is the NEXT offset to consume.
func (b *Broker) CommitOffset(groupID string, generation int64, memberID, topic string, partition int32, offset uint64) error {
	err := b.groups.Commit(groupID, generation, memberID, offsets.Record{
		Topic: topic, Partition: partition, Offset: offset,
	})
	switch {
	case err == nil:
		b.metrics.CommitObs("ok")
	case errors.Is(err, group.ErrFencedGeneration):
		b.metrics.CommitObs("fenced")
	default:
		b.metrics.CommitObs("error")
	}
	return err
}

// GetOffsets returns committed offsets for the requested partitions (0
// partitions = all partitions of the topic).
func (b *Broker) GetOffsets(group, topic string, partitions []int32) ([]offsets.Record, error) {
	count, ok := b.store.Get(topic)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTopicOrPartition, topic)
	}
	ids := partitions
	if len(ids) == 0 {
		for i := int32(0); i < count.PartitionCount; i++ {
			ids = append(ids, i)
		}
	}
	out := make([]offsets.Record, 0, len(ids))
	for _, p := range ids {
		if p < 0 || p >= count.PartitionCount {
			return nil, fmt.Errorf("%w: (%q, %d)", ErrUnknownTopicOrPartition, topic, p)
		}
		off, committed, err := b.groups.Committed(group, p)
		if err != nil {
			return nil, err
		}
		if committed {
			out = append(out, offsets.Record{Topic: topic, Partition: p, Offset: off})
		}
	}
	return out, nil
}

// DescribeGroup returns the generation and member assignments.
func (b *Broker) DescribeGroup(group string) (int64, []group.MemberInfo, error) {
	return b.groups.Describe(group)
}

// LagSnapshot computes consumer lag (log end minus committed offset) for
// every known group over this broker's partitions, fresh at call time.
func (b *Broker) LagSnapshot() []observability.LagRow {
	b.mu.RLock()
	parts := b.parts
	b.mu.RUnlock()
	var out []observability.LagRow
	for _, g := range b.groups.Groups() {
		committed, ok := b.groups.CommittedAll(g)
		if !ok {
			continue
		}
		for topic, list := range parts {
			for _, p := range list {
				hw := p.log.NextOffset()
				if c, ok := committed[p.id]; ok && hw >= c {
					out = append(out, observability.LagRow{Group: g, Topic: topic, Partition: p.id, Lag: hw - c})
				}
			}
		}
	}
	return out
}

func (b *Broker) closePartsAndWait(parts map[string][]*partition) {
	var wg sync.WaitGroup
	for _, list := range parts {
		for _, p := range list {
			wg.Add(1)
			go func(p *partition) {
				defer wg.Done()
				p.closeAndWait()
			}(p)
		}
	}
	wg.Wait()
}

func (p *partition) closeAndWait() {
	close(p.queue)
	<-p.done
	_ = p.log.Close()
}
