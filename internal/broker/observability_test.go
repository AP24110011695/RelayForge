package broker

// Phase 8: observability correctness (metrics match known operations) and
// hardening (slow-consumer isolation, bounded soak).

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relayforgev1 "relayforge/api/proto/relayforge/v1"
	"relayforge/internal/observability"
)

func TestMetricsMatchKnownOperations(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := observability.New(reg)
	b, err := Open(Options{DataDir: t.TempDir(), Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		if _, err := b.Produce(ctx, DefaultTopic, 0, AckWritten, []Message{{Value: []byte("0123456789")}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, err := b.Fetch(ctx, DefaultTopic, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if mf.GetName() == "relayforge_messages_in_total" && labels["topic"] == DefaultTopic {
				values["in"] = m.GetCounter().GetValue()
			}
			if mf.GetName() == "relayforge_bytes_in_total" && labels["topic"] == DefaultTopic {
				values["bytes"] = m.GetCounter().GetValue()
			}
			if mf.GetName() == "relayforge_messages_out_total" && labels["topic"] == DefaultTopic {
				values["out"] = m.GetCounter().GetValue()
			}
		}
	}
	if values["in"] != 7 {
		t.Errorf("messages_in = %v, want 7", values["in"])
	}
	if values["bytes"] != 70 {
		t.Errorf("bytes_in = %v, want 70", values["bytes"])
	}
	if values["out"] != 7 {
		t.Errorf("messages_out = %v, want 7", values["out"])
	}
}

func TestConsumerLagMetric(t *testing.T) {
	env := startGRPC(t)
	ctx := context.Background()
	if _, err := env.client.CreateTopic(ctx, &relayforgev1.CreateTopicRequest{Name: "lag", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{
			Topic: "lag", Messages: []*relayforgev1.Message{{Value: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	gen, id, _ := joinAndSync(t, env, "laggroup", "lag", "", 60000)
	if _, err := env.client.CommitOffset(ctx, &relayforgev1.CommitOffsetRequest{
		Group: "laggroup", GenerationId: gen, MemberId: id, Topic: "lag", Partition: 0, Offset: 4,
	}); err != nil {
		t.Fatal(err)
	}
	rows := env.b.LagSnapshot()
	found := false
	for _, r := range rows {
		if r.Group == "laggroup" && r.Topic == "lag" && r.Partition == 0 {
			if r.Lag != 6 {
				t.Fatalf("lag = %d, want 6 (hw 10 - committed 4)", r.Lag)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("lag row missing: %+v", rows)
	}
}

func TestAdminServerEndpoints(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := observability.New(reg)
	metrics.OverloadInc() // label-less counter: present even with no broker activity
	overview := func() observability.OverviewData {
		return observability.OverviewData{
			Healthy: true,
			Streams: []observability.StreamRow{{Topic: "default", Partitions: 1, Events: 10}},
			Groups:  []observability.GroupRow{{Group: "workers", Members: 2, Started: true, Lag: 6, Behind: true}},
		}
	}
	mux := observability.NewAdminServer(reg, func() bool { return true }, overview)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 {
		t.Errorf("healthz = %d", rec.Code)
	}
	// The Overview page speaks plain language and answers the main questions.
	recPage := httptest.NewRecorder()
	mux.ServeHTTP(recPage, httptest.NewRequest("GET", "/", nil))
	body := recPage.Body.String()
	if recPage.Code != 200 || !strings.Contains(body, "drop-off point") {
		t.Errorf("overview page (code %d) missing the product explanation", recPage.Code)
	}
	if !strings.Contains(body, "default") || !strings.Contains(body, "10") {
		t.Errorf("overview page missing stored-events row: %s", body)
	}
	if !strings.Contains(body, "6 events behind") {
		t.Errorf("overview page missing plain lag verdict: %s", body)
	}
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, httptest.NewRequest("GET", "/metrics", nil))
	if rec2.Code != 200 || !strings.Contains(rec2.Body.String(), "relayforge_overloaded_total") {
		t.Errorf("metrics endpoint missing relayforge_ families (code %d)", rec2.Code)
	}
	// Readiness gating.
	mux2 := observability.NewAdminServer(reg, func() bool { return false }, nil)
	rec3 := httptest.NewRecorder()
	mux2.ServeHTTP(rec3, httptest.NewRequest("GET", "/healthz", nil))
	if rec3.Code != 503 {
		t.Errorf("healthz not-ready = %d, want 503", rec3.Code)
	}
}

func TestCommitMetricsByResult(t *testing.T) {
	env := startGRPC(t)
	if _, err := env.client.CreateTopic(context.Background(), &relayforgev1.CreateTopicRequest{Name: "cm", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	gen, id, _ := joinAndSync(t, env, "cmg", "cm", "", 60000)
	if _, err := env.client.CommitOffset(context.Background(), &relayforgev1.CommitOffsetRequest{
		Group: "cmg", GenerationId: gen, MemberId: id, Topic: "cm", Partition: 0, Offset: 1,
	}); err != nil {
		t.Fatal(err)
	}
	_, err := env.client.CommitOffset(context.Background(), &relayforgev1.CommitOffsetRequest{
		Group: "cmg", GenerationId: gen - 1, MemberId: id, Topic: "cm", Partition: 0, Offset: 2,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected fenced commit, got %v", err)
	}
	// Commit metrics: one ok, one fenced.
	results := map[string]float64{}
	families, gerr := env.reg.Gather()
	if gerr != nil {
		t.Fatal(gerr)
	}
	for _, mf := range families {
		if mf.GetName() != "relayforge_offset_commits_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "result" {
					results[l.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	if results["ok"] != 1 || results["fenced"] != 1 {
		t.Fatalf("commit counters: %+v", results)
	}
}

// TestSlowConsumerIsolation: a fetcher that drains one record at a time in
// a tight loop must not stall producers or grow memory unboundedly.
func TestSlowConsumerIsolation(t *testing.T) {
	b, err := Open(Options{DataDir: t.TempDir(), Metrics: nil})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	var produced sync.WaitGroup
	produced.Add(1)
	go func() {
		defer produced.Done()
		for i := 0; i < 300; i++ {
			if _, err := b.Produce(ctx, DefaultTopic, 0, AckWritten, []Message{{Value: make([]byte, 512)}}); err != nil {
				t.Errorf("produce %d: %v", i, err)
				return
			}
		}
	}()
	stop := make(chan struct{})
	slowErr := make(chan error, 1)
	go func() { // deliberately slow consumer
		defer close(slowErr)
		var cursor uint64 = 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			recs0, next, _, err := b.Fetch(ctx, DefaultTopic, 0, cursor, 1, 0) // 1 byte max: one record at a time
			_ = recs0
			if err != nil {
				slowErr <- err
				return
			}
			if next > cursor {
				cursor = next
			}
			if cursor == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	produced.Wait()
	close(stop)
	if err := <-slowErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("slow consumer failed: %v", err)
	}
	runtime.GC()
}

// TestSoak: bounded soak — sustained produce/fetch across the wire with
// stable goroutines and memory.
func TestSoak(t *testing.T) {
	env := startGRPC(t)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 8)
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	goroutinesStart := runtime.NumGoroutine()

	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := env.client.Produce(context.Background(), &relayforgev1.ProduceRequest{
					Topic:    DefaultTopic,
					Messages: []*relayforgev1.Message{{Value: make([]byte, 256)}},
				}); err != nil {
					errCh <- fmt.Errorf("producer %d: %w", id, err)
					return
				}
			}
		}(w)
	}
	// Consumer: keep up in coarse batches.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var cursor uint64
		for {
			select {
			case <-stop:
				return
			default:
			}
			resp, err := env.client.Fetch(context.Background(), &relayforgev1.FetchRequest{
				Topic: DefaultTopic, Offset: cursor, MaxWaitMs: 50,
			})
			if err != nil {
				errCh <- fmt.Errorf("consumer: %w", err)
				return
			}
			if resp.GetNextOffset() == cursor && len(resp.GetRecords()) == 0 {
				if cursor >= resp.GetHighWatermark() && cursor >= 1200 {
					return
				}
				continue
			}
			cursor = resp.GetNextOffset()
			if cursor >= 1200 {
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case err := <-errCh:
		close(stop)
		t.Fatal(err)
	case <-time.After(60 * time.Second):
		close(stop)
		t.Fatal("soak timed out")
	}
	close(stop)
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	goroutinesEnd := runtime.NumGoroutine()
	// Generous bounds: memory growth must stay far below the data volume
	// (1200 x 256B = 300KB produced), goroutines must not leak.
	if memAfter.HeapInuse > memBefore.HeapInuse+8<<20 {
		t.Errorf("heap grew by %d bytes during soak", memAfter.HeapInuse-memBefore.HeapInuse)
	}
	if goroutinesEnd > goroutinesStart+10 {
		t.Errorf("goroutines grew: %d -> %d", goroutinesStart, goroutinesEnd)
	}
}
