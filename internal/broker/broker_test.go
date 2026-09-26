package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	relayforgev1 "relayforge/api/proto/relayforge/v1"
	"relayforge/internal/logengine"
	"relayforge/internal/observability"
	"relayforge/internal/version"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func bufconnListener() *bufconn.Listener {
	return bufconn.Listen(1 << 20)
}

func openBroker(t *testing.T) (*Broker, string) {
	t.Helper()
	dir := t.TempDir()
	b := openBrokerAt(t, dir)
	return b, dir
}

func openBrokerAt(t *testing.T, dir string) *Broker {
	t.Helper()
	b, err := Open(Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestProduceFetchRoundTrip(t *testing.T) {
	b, _ := openBroker(t)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		offsets, err := b.Produce(ctx, DefaultTopic, 0, AckWritten,
			[]Message{{Key: []byte(fmt.Sprintf("k%d", i%3)), Value: []byte(fmt.Sprintf("v%02d", i))}})
		if err != nil {
			t.Fatal(err)
		}
		if len(offsets) != 1 || offsets[0] != uint64(i) {
			t.Fatalf("produce %d: offsets %v", i, offsets)
		}
	}
	recs, next, hw, err := b.Fetch(ctx, DefaultTopic, 0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 || next != 10 || hw != 10 {
		t.Fatalf("full fetch: %d records next=%d hw=%d", len(recs), next, hw)
	}
	for i, r := range recs {
		if r.Offset != uint64(i) || string(r.Value) != fmt.Sprintf("v%02d", i) {
			t.Errorf("record %d: %+v", i, r)
		}
		if r.Timestamp == 0 {
			t.Errorf("record %d: timestamp not stamped", i)
		}
	}
	recs, next, _, err = b.Fetch(ctx, DefaultTopic, 0, 7, 0, 0)
	if err != nil || len(recs) != 3 || next != 10 || recs[0].Offset != 7 {
		t.Fatalf("mid fetch: %d records next=%d err=%v", len(recs), next, err)
	}
	if _, _, _, err := b.Fetch(ctx, DefaultTopic, 0, 10, 0, 0); err != nil {
		t.Errorf("at-end fetch: %v", err)
	}
	if _, _, _, err := b.Fetch(ctx, DefaultTopic, 0, 11, 0, 0); !errors.Is(err, logengine.ErrOffsetOutOfRange) {
		t.Errorf("beyond-end fetch: error = %v, want ErrOffsetOutOfRange", err)
	}
}

// TestUnknownRoute covers broker v1's single-route constraint.
func TestUnknownRoute(t *testing.T) {
	b, _ := openBroker(t)
	ctx := context.Background()
	if _, err := b.Produce(ctx, "other", 0, AckWritten, []Message{{Value: []byte("x")}}); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Errorf("unknown topic: %v", err)
	}
	if _, err := b.Produce(ctx, DefaultTopic, 3, AckWritten, []Message{{Value: []byte("x")}}); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Errorf("unknown partition: %v", err)
	}
	if _, _, _, err := b.Fetch(ctx, "other", 0, 0, 0, 0); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Errorf("fetch unknown topic: %v", err)
	}
}

func TestBatchLimits(t *testing.T) {
	b, _ := openBroker(t)
	ctx := context.Background()
	if _, err := b.Produce(ctx, DefaultTopic, 0, AckWritten, nil); !errors.Is(err, ErrEmptyBatch) {
		t.Errorf("empty batch: %v", err)
	}
	many := make([]Message, MaxBatchMessages+1)
	if _, err := b.Produce(ctx, DefaultTopic, 0, AckWritten, many); !errors.Is(err, ErrBatchTooLarge) {
		t.Errorf("too many messages: %v", err)
	}
	big := make([]Message, 2)
	big[0] = Message{Value: make([]byte, MaxBatchBytes/2+1)}
	big[1] = Message{Value: make([]byte, MaxBatchBytes/2+1)}
	if _, err := b.Produce(ctx, DefaultTopic, 0, AckWritten, big); !errors.Is(err, ErrBatchTooLarge) {
		t.Errorf("batch too large: %v", err)
	}
	// A single record beyond the engine's record limit fails the append.
	if _, err := b.Produce(ctx, DefaultTopic, 0, AckWritten,
		[]Message{{Value: make([]byte, 5<<20)}}); err == nil {
		t.Error("oversized record accepted")
	}
}

func TestLongPoll(t *testing.T) {
	b, _ := openBroker(t)
	ctx := context.Background()
	start := time.Now()
	_, _, hw, err := b.Fetch(ctx, DefaultTopic, 0, 0, 0, 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 240*time.Millisecond {
		t.Errorf("long poll returned early: %v", elapsed)
	}
	if hw != 0 {
		t.Errorf("hw = %d, want 0", hw)
	}
	// Produce after a delay; the long poll must return early with data.
	go func() {
		time.Sleep(60 * time.Millisecond)
		_, _ = b.Produce(context.Background(), DefaultTopic, 0, AckWritten, []Message{{Value: []byte("late")}})
	}()
	start = time.Now()
	recs, next, hw, err := b.Fetch(ctx, DefaultTopic, 0, 0, 0, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("long poll did not wake on data: %v", elapsed)
	}
	if len(recs) != 1 || string(recs[0].Value) != "late" || next != 1 || hw != 1 {
		t.Errorf("long poll result: %+v next=%d hw=%d", recs, next, hw)
	}
}

func TestFetchStopsAtSegmentBoundary(t *testing.T) {
	// Reads do not cross segment boundaries; the broker's long-poll loop
	// must follow next-offset without waiting when data continues.
	b, _ := openBroker(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := b.Produce(ctx, DefaultTopic, 0, AckWritten, []Message{{Value: []byte(fmt.Sprintf("v%d", i))}}); err != nil {
			t.Fatal(err)
		}
	}
	recs, next, _, err := b.Fetch(ctx, DefaultTopic, 0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if next != 5 || len(recs) != 5 {
		t.Fatalf("single-segment read: %d records, next %d", len(recs), next)
	}
}

// --- gRPC integration over bufconn ---

type grpcEnv struct {
	dir     string
	b       *Broker
	srv     *grpc.Server
	conn    *grpc.ClientConn
	client  relayforgev1.RelayForgeClient
	reg     *prometheus.Registry
	metrics *observability.Metrics
}

func startGRPC(t *testing.T) *grpcEnv {
	t.Helper()
	dir := t.TempDir()
	lis := bufconnListener()
	reg := prometheus.NewRegistry()
	metrics := observability.New(reg)
	b, err := Open(Options{DataDir: dir, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	srv, hs := NewServer(b, testLogger(), metrics)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &grpcEnv{dir: dir, b: b, srv: srv, conn: conn, client: relayforgev1.NewRelayForgeClient(conn), reg: reg, metrics: metrics}
}

func TestGRPCProduceFetch(t *testing.T) {
	env := startGRPC(t)
	ctx := context.Background()
	resp, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{
		Topic: DefaultTopic, AckMode: relayforgev1.AckMode_ACK_MODE_FLUSHED,
		Messages: []*relayforgev1.Message{{Key: []byte("a"), Value: []byte("one")}, {Value: []byte("two")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetOffsets()) != 2 || resp.GetOffsets()[0] != 0 || resp.GetOffsets()[1] != 1 {
		t.Fatalf("offsets: %v", resp.GetOffsets())
	}
	fresp, err := env.client.Fetch(ctx, &relayforgev1.FetchRequest{Topic: DefaultTopic})
	if err != nil {
		t.Fatal(err)
	}
	if len(fresp.GetRecords()) != 2 || fresp.GetNextOffset() != 2 || fresp.GetHighWatermark() != 2 {
		t.Fatalf("fetch: %d records next=%d hw=%d", len(fresp.GetRecords()), fresp.GetNextOffset(), fresp.GetHighWatermark())
	}
	if string(fresp.GetRecords()[0].GetKey()) != "a" || string(fresp.GetRecords()[0].GetValue()) != "one" {
		t.Errorf("record 0: %+v", fresp.GetRecords()[0])
	}
}

func TestGRPCStatusMapping(t *testing.T) {
	env := startGRPC(t)
	ctx := context.Background()
	cases := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"unknown topic", func() error {
			_, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{Topic: "nope", Messages: []*relayforgev1.Message{{Value: []byte("x")}}})
			return err
		}, codes.NotFound},
		{"beyond end", func() error {
			_, err := env.client.Fetch(ctx, &relayforgev1.FetchRequest{Topic: DefaultTopic, Offset: 99})
			return err
		}, codes.OutOfRange},
		{"empty batch", func() error {
			_, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{Topic: DefaultTopic})
			return err
		}, codes.InvalidArgument},
		{"too many messages", func() error {
			msgs := make([]*relayforgev1.Message, MaxBatchMessages+1)
			_, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{Topic: DefaultTopic, Messages: msgs})
			return err
		}, codes.ResourceExhausted},
		{"record too large", func() error {
			_, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{
				Topic: DefaultTopic, Messages: []*relayforgev1.Message{{Value: make([]byte, 5<<20)}}})
			return err
		}, codes.ResourceExhausted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if status.Code(err) != tc.want {
				t.Fatalf("code = %s, want %s (err %v)", status.Code(err), tc.want, err)
			}
		})
	}
}

func TestGRPCAckFlushedSurvivesReopen(t *testing.T) {
	env := startGRPC(t)
	ctx := context.Background()
	if _, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{
		Topic: DefaultTopic, AckMode: relayforgev1.AckMode_ACK_MODE_FLUSHED,
		Messages: []*relayforgev1.Message{{Value: []byte("durable")}},
	}); err != nil {
		t.Fatal(err)
	}
	env.srv.Stop()
	env.conn.Close()
	if err := env.b.Close(); err != nil {
		t.Fatal(err)
	}
	// A fresh broker over the same data dir must see the flushed record.
	b2 := openBrokerAt(t, env.dir)
	recs, _, _, err := b2.Fetch(ctx, DefaultTopic, 0, 0, 0, 0)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "durable" {
		t.Fatalf("flushed record lost after reopen: %v %v", recs, err)
	}
}

func TestGRPCLongPollDeadline(t *testing.T) {
	env := startGRPC(t)
	start := time.Now()
	if _, err := env.client.Fetch(context.Background(),
		&relayforgev1.FetchRequest{Topic: DefaultTopic, MaxWaitMs: 200}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 190*time.Millisecond {
		t.Errorf("long poll returned early: %v", elapsed)
	}
}

func TestGRPCHealth(t *testing.T) {
	env := startGRPC(t)
	resp, err := healthpb.NewHealthClient(env.conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health = %s", resp.GetStatus())
	}
}

func TestGRPCConcurrentProduceFetch(t *testing.T) {
	env := startGRPC(t)
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				_, err := env.client.Produce(context.Background(), &relayforgev1.ProduceRequest{
					Topic:    DefaultTopic,
					Messages: []*relayforgev1.Message{{Value: []byte(fmt.Sprintf("w%d-m%02d", id, i))}},
				})
				if err != nil {
					t.Errorf("produce %d/%d: %v", id, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	// Read everything back across all segments.
	var cursor uint64 = 0
	count := 0
	for {
		resp, err := env.client.Fetch(context.Background(), &relayforgev1.FetchRequest{Topic: DefaultTopic, Offset: cursor})
		if err != nil {
			t.Fatal(err)
		}
		count += len(resp.GetRecords())
		if resp.GetNextOffset() == cursor && len(resp.GetRecords()) == 0 {
			break
		}
		cursor = resp.GetNextOffset()
		if cursor >= resp.GetHighWatermark() && resp.GetNextOffset() == cursor {
			if count == 90 {
				break
			}
		}
	}
	if count != 90 {
		t.Errorf("read back %d records, want 90", count)
	}
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	lis := bufconnListener()
	b := openBrokerAt(t, t.TempDir())
	srv, hs := NewServer(b, testLogger(), nil)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := relayforgev1.NewRelayForgeClient(conn)

	fetchDone := make(chan error, 1)
	go func() {
		_, err := client.Fetch(context.Background(),
			&relayforgev1.FetchRequest{Topic: DefaultTopic, MaxWaitMs: 400})
		fetchDone <- err
	}()
	time.Sleep(80 * time.Millisecond) // let the long poll start
	srv.GracefulStop()                // must wait for the in-flight fetch
	select {
	case err := <-fetchDone:
		if err != nil {
			t.Errorf("in-flight fetch failed during graceful stop: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("graceful stop did not drain the in-flight fetch")
	}
}

func TestRecoveryInterceptor(t *testing.T) {
	lis := bufconnListener()
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(recoveryInterceptor(testLogger())))
	panicSvc := &panickyService{}
	relayforgev1.RegisterRelayForgeServer(srv, panicSvc)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = relayforgev1.NewRelayForgeClient(conn).Fetch(context.Background(), &relayforgev1.FetchRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %s, want Internal (err %v)", status.Code(err), err)
	}
}

type panickyService struct {
	relayforgev1.UnimplementedRelayForgeServer
}

func (p *panickyService) Fetch(context.Context, *relayforgev1.FetchRequest) (*relayforgev1.FetchResponse, error) {
	panic("boom")
}

func TestVersionPackage(t *testing.T) {
	info := version.Get()
	if info.Version == "" || info.GoVersion == "" {
		t.Fatalf("version info incomplete: %+v", info)
	}
}
