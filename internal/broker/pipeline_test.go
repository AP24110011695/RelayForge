package broker

// Phase 6 pipeline tests: routing, ordering, backpressure, writer-panic
// containment, metadata recovery, and shutdown lifecycle.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relayforgev1 "relayforge/api/proto/relayforge/v1"
)

// TestRoutingDeterminism: the same key always routes to the same
// partition; keyless messages round-robin across all partitions; single-
// partition topics always return 0.
func TestRoutingDeterminism(t *testing.T) {
	b, _ := openBroker(t)
	if err := b.CreateTopic("multi", 4); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("user-1"), []byte("user-2"), []byte(""), {0x00, 0xFF}} {
		first := b.route(key, 4)
		if first < 0 || first >= 4 {
			t.Fatalf("route out of range: %d", first)
		}
		for i := 0; i < 50; i++ {
			if got := b.route(key, 4); got != first {
				t.Fatalf("key %q routed to %d then %d", key, first, got)
			}
		}
	}
	// Keyless round-robin must cover all partitions.
	seen := map[int32]bool{}
	for i := 0; i < 64; i++ {
		seen[b.route(nil, 4)] = true
	}
	if len(seen) != 4 {
		t.Errorf("round-robin covered %d of 4 partitions", len(seen))
	}
	if got := b.route([]byte("x"), 1); got != 0 {
		t.Errorf("single-partition route = %d, want 0", got)
	}
}

// TestProduceFetchMultiPartition: keyed messages with the same key land in
// one partition and stay in order; explicit pins work.
func TestProduceFetchMultiPartition(t *testing.T) {
	b, _ := openBroker(t)
	if err := b.CreateTopic("events", 3); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var msgs []Message
	for i := 0; i < 30; i++ {
		msgs = append(msgs, Message{Key: []byte(fmt.Sprintf("user-%d", i%5)), Value: []byte(fmt.Sprintf("m%02d", i))})
	}
	if _, err := b.Produce(ctx, "events", -1, AckWritten, msgs); err != nil {
		t.Fatal(err)
	}
	// Per-partition reads: every partition's records are contiguous and
	// each key's sequence stays in order.
	for p := int32(0); p < 3; p++ {
		recs, err := readAllPartitions(b, "events", p)
		if err != nil {
			t.Fatal(err)
		}
		lastSeq := map[string]int{}
		for _, r := range recs {
			var seq int
			if _, err := fmt.Sscanf(string(r.Value), "m%d", &seq); err != nil {
				t.Fatal(err)
			}
			key := string(r.Key)
			if prev, ok := lastSeq[key]; ok && seq < prev {
				t.Errorf("partition %d: key %q went backwards: %d after %d", p, key, seq, prev)
			}
			lastSeq[key] = seq
		}
	}
	// Explicit pin: all messages in partition 1 regardless of key.
	offsets, err := b.Produce(ctx, "events", 1, AckWritten,
		[]Message{{Key: []byte("a"), Value: []byte("p1a")}, {Key: []byte("b"), Value: []byte("p1b")}})
	if err != nil {
		t.Fatal(err)
	}
	recs, err := readAllPartitions(b, "events", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) < 2 || string(recs[len(recs)-2].Value) != "p1a" || string(recs[len(recs)-1].Value) != "p1b" {
		t.Errorf("pinned messages missing from partition 1 (offsets %v)", offsets)
	}
}

func readAllPartitions(b *Broker, topic string, partition int32) ([]FetchedRecord, error) {
	var out []FetchedRecord
	var cursor uint64 = 0
	for {
		recs, next, _, err := b.Fetch(context.Background(), topic, partition, cursor, 0, 0)
		if err != nil {
			return out, err
		}
		out = append(out, recs...)
		if next == cursor {
			return out, nil
		}
		cursor = next
	}
}

// TestBackpressure: with a stalled writer and a tiny queue, produces block
// until the deadline and then fail with ErrOverloaded; memory stays bounded
// (the queue never exceeds its capacity); and the partition recovers after
// the stall.
func TestBackpressure(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(Options{DataDir: dir, ProduceQueueDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic("bp", 1); err != nil {
		t.Fatal(err)
	}
	p := b.parts["bp"][0]
	release := make(chan struct{})
	stallCount := 0
	var mu sync.Mutex
	p.writeHook = func() {
		mu.Lock()
		stallCount++
		n := stallCount
		mu.Unlock()
		if n == 1 {
			<-release // stall only the first append
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	// Five messages against a queue of depth 2 with a stalled writer: the
	// submit path must hit the full queue and fail with ErrOverloaded.
	_, err = b.Produce(ctx, "bp", -1, AckWritten, []Message{
		{Value: []byte("stalled-0")}, {Value: []byte("stalled-1")},
		{Value: []byte("stalled-2")}, {Value: []byte("stalled-3")},
		{Value: []byte("stalled-4")},
	})
	cancel()
	if !errors.Is(err, ErrOverloaded) {
		t.Errorf("produce into stalled partition: error = %v, want ErrOverloaded", err)
	}
	if len(p.queue) > 2 {
		t.Errorf("queue exceeded capacity: %d items", len(p.queue))
	}
	// Unstall unconditionally (no t.Fatalf before this point: the deferred
	// Close must never wait on a stalled writer).
	close(release)
	// The submitted messages append (unacked but durable); after the drain
	// the pipeline works again.
	off, err := b.Produce(context.Background(), "bp", -1, AckWritten, []Message{{Value: []byte("after")}})
	if err != nil {
		t.Fatalf("produce after stall: %v", err)
	}
	recs, err := readAllPartitions(b, "bp", 0)
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	for _, r := range recs {
		values = append(values, string(r.Value))
	}
	if len(values) != 4 || values[0] != "stalled-0" || values[1] != "stalled-1" ||
		values[2] != "stalled-2" || values[3] != "after" || off[0] != 3 {
		t.Errorf("post-stall state: values %v, after-offset %v", values, off)
	}
}

// TestPartitionWriterPanicContainment: a panicking writer fails only its
// own partition; producers get fast errors there and other partitions keep
// working.
func TestPartitionWriterPanicContainment(t *testing.T) {
	b, _ := openBroker(t)
	defer b.Close()
	if err := b.CreateTopic("panic", 2); err != nil {
		t.Fatal(err)
	}
	p0 := b.parts["panic"][0]
	p0.writeHook = func() { panic("writer exploded") }

	_, err := b.Produce(context.Background(), "panic", 0, AckWritten, []Message{{Value: []byte("x")}})
	if !errors.Is(err, ErrPartitionFailed) {
		t.Fatalf("first produce to panicking partition: %v", err)
	}
	// Fail-fast on subsequent produces to the failed partition.
	if _, err := b.Produce(context.Background(), "panic", 0, AckWritten, []Message{{Value: []byte("y")}}); !errors.Is(err, ErrPartitionFailed) {
		t.Fatalf("second produce: %v", err)
	}
	// The healthy partition is unaffected.
	offsets, err := b.Produce(context.Background(), "panic", 1, AckWritten, []Message{{Value: []byte("fine")}})
	if err != nil || offsets[0] != 0 {
		t.Fatalf("healthy partition: offsets %v err %v", offsets, err)
	}
	// Shutdown still drains the failed partition's queue cleanly.
}

func TestMetadataPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("events", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Produce(context.Background(), "events", -1, AckFlushed, []Message{{Value: []byte("persisted")}}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := Open(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()
	topics := b2.ListTopics()
	found := false
	for _, tp := range topics {
		if tp.Name == "events" && tp.PartitionCount == 3 {
			found = true
		}
	}
	if !found {
		t.Fatalf("topic lost across reopen: %+v", topics)
	}
	recs, err := readAllPartitions(b2, "events", 0)
	if err != nil {
		t.Fatal(err)
	}
	all := ""
	for _, r := range recs {
		all += string(r.Value)
	}
	if !strings.Contains(all, "persisted") {
		t.Errorf("produced records lost across reopen (partition reads: %q)", all)
	}
}

func TestCreateTopicViaGRPC(t *testing.T) {
	env := startGRPC(t)
	ctx := context.Background()
	if _, err := env.client.CreateTopic(ctx, &relayforgev1.CreateTopicRequest{Name: "orders", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.CreateTopic(ctx, &relayforgev1.CreateTopicRequest{Name: "orders", PartitionCount: 2}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate create: %v", err)
	}
	if _, err := env.client.CreateTopic(ctx, &relayforgev1.CreateTopicRequest{Name: "bad name", PartitionCount: 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid name: %v", err)
	}
	lresp, err := env.client.ListTopics(ctx, &relayforgev1.ListTopicsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(lresp.GetTopics()) != 2 {
		t.Fatalf("list: %+v", lresp.GetTopics())
	}
	dresp, err := env.client.DescribeTopic(ctx, &relayforgev1.DescribeTopicRequest{Name: "orders"})
	if err != nil || dresp.GetTopic().GetPartitionCount() != 2 {
		t.Fatalf("describe: %+v err %v", dresp, err)
	}
	if _, err := env.client.DescribeTopic(ctx, &relayforgev1.DescribeTopicRequest{Name: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("describe missing: %v", err)
	}
	// Produce/fetch on the created topic through the wire API.
	if _, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{
		Topic: "orders", Messages: []*relayforgev1.Message{{Key: []byte("k"), Value: []byte("v")}},
	}); err != nil {
		t.Fatal(err)
	}
	fresp, err := env.client.Fetch(ctx, &relayforgev1.FetchRequest{Topic: "orders"})
	if err != nil || len(fresp.GetRecords()) != 1 {
		t.Fatalf("fetch on created topic: %v records=%d", err, len(fresp.GetRecords()))
	}
}

func TestCorruptMetadataRefusesBrokerStart(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("events", 2); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	jsonPath := dir + "/metadata.json"
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/3] ^= 0xFF
	if err := os.WriteFile(jsonPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = Open(Options{DataDir: dir})
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("broker started with corrupt metadata: %v", err)
	}
}

// TestCloseDrainsAndStopsWriters: after Close, in-flight appends completed
// and writer goroutines exited (no leaks).
func TestCloseDrainsAndStopsWriters(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("drain", 2); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	// A produce in flight while Close is called must resolve.
	result := make(chan error, 1)
	go func() {
		_, err := b.Produce(context.Background(), "drain", -1, AckFlushed, []Message{{Value: []byte("in-flight")}})
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("in-flight produce failed during close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight produce hung during close")
	}
	time.Sleep(100 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutine leak after close: %d > %d", after, before)
	}
	if _, err := b.Produce(context.Background(), "drain", -1, AckWritten, []Message{{Value: []byte("late")}}); !errors.Is(err, ErrClosed) {
		t.Errorf("produce after close: %v", err)
	}
	if err := b.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second close: %v", err)
	}
}

// TestUnknownRouteAfterTopics: P6 keeps broker v1's unknown-route errors,
// now against the topic table.
func TestUnknownRouteAfterTopics(t *testing.T) {
	b, _ := openBroker(t)
	if _, err := b.Produce(context.Background(), "missing", -1, AckWritten, []Message{{Value: []byte("x")}}); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Errorf("unknown topic: %v", err)
	}
	if _, err := b.Produce(context.Background(), DefaultTopic, 5, AckWritten, []Message{{Value: []byte("x")}}); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Errorf("unknown partition: %v", err)
	}
	if err := b.CreateTopic("one", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Produce(context.Background(), "one", 1, AckWritten, []Message{{Value: []byte("x")}}); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Errorf("partition beyond count: %v", err)
	}
}
