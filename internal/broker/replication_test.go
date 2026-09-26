package broker

// Phase 10 replication tests at the validated FALLBACK boundary: async
// follower catch-up, follower produce rejection, and manual promotion via
// epoch bump.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"relayforge/internal/crcfile"
)

// grpcNewBufconnClient dials a bufconn listener as a gRPC client.
func grpcNewBufconnClient(lis *bufconn.Listener) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// startFollower builds a follower broker whose replication loop talks to
// the leader over bufconn.
func startFollower(t *testing.T, dir string, lis *bufconn.Listener, poll time.Duration) *Broker {
	t.Helper()
	conn, err := grpcNewBufconnClient(lis)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	b, err := Open(Options{DataDir: dir, Replication: &Replication{ClientConn: conn, PollInterval: poll}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestFollowerReplicatesAndRejectsProduces(t *testing.T) {
	lis := bufconnListener()
	leader := openBrokerAt(t, t.TempDir())
	lSrv, _ := NewServer(leader, testLogger(), nil)
	go func() { _ = lSrv.Serve(lis) }()
	defer lSrv.Stop()

	follower := startFollower(t, t.TempDir(), lis, 30*time.Millisecond)
	if err := follower.CreateTopic("rep", 1); err != nil {
		t.Fatal(err)
	}
	// The leader needs the same topic (static placement MVP: created on
	// both brokers by the operator).
	if err := leader.CreateTopic("rep", 1); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	const total = 50
	for i := 0; i < total; i++ {
		if _, err := leader.Produce(ctx, "rep", 0, AckFlushed, []Message{{Value: []byte(fmt.Sprintf("replicated-%02d", i))}}); err != nil {
			t.Fatal(err)
		}
	}
	// Wait for async catch-up.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if follower.parts["rep"][0].log.NextOffset() == total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := follower.parts["rep"][0].log.NextOffset(); got != total {
		t.Fatalf("follower caught up to %d of %d", got, total)
	}
	// The replicated history matches the leader's, record for record.
	for i := 0; i < total; i++ {
		recs, _, _, err := follower.Fetch(ctx, "rep", 0, uint64(i), 1, 0)
		if err != nil || len(recs) != 1 || string(recs[0].Value) != fmt.Sprintf("replicated-%02d", i) {
			t.Fatalf("follower record %d: %v %+v", i, err, recs)
		}
	}
	// A follower rejects produces.
	if _, err := follower.Produce(ctx, "rep", 0, AckWritten, []Message{{Value: []byte("x")}}); err == nil {
		t.Fatal("produce on follower accepted")
	} else {
		msg := err.Error()
		if !contains(msg, "follower") {
			t.Errorf("error = %v, want the not-leader refusal", err)
		}
	}
}

func TestPromotionBumpsEpochs(t *testing.T) {
	dir := t.TempDir()
	// Standalone start #1: "default" gets epoch 1; the topic is created
	// after Open, so it is first covered by start #2.
	b1 := openBrokerAt(t, dir)
	if err := b1.CreateTopic("p", 2); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	epochPath0 := filepath.Join(dir, "topics", "p", "partition-0", "epoch")
	// Standalone start #2 (first covering topic "p"): epoch 1.
	b2 := openBrokerAt(t, dir)
	data, err := crcfile.Load(epochPath0)
	if err != nil {
		t.Fatalf("epoch file missing after standalone start: %v", err)
	}
	var ef epochFile
	if err := json.Unmarshal(data, &ef); err != nil || ef.Epoch != 1 {
		t.Fatalf("first covering start: epoch %d err %v", ef.Epoch, err)
	}
	if err := b2.Close(); err != nil {
		t.Fatal(err)
	}
	// Standalone start #3 (the promotion path): epoch bumped to 2.
	b3 := openBrokerAt(t, dir)
	defer b3.Close()
	data, err = crcfile.Load(epochPath0)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &ef); err != nil || ef.Epoch != 2 {
		t.Fatalf("promoted start: epoch %d err %v", ef.Epoch, err)
	}
	// The promoted broker accepts produces (it is the leader now).
	if _, err := b3.Produce(context.Background(), "p", 0, AckWritten, []Message{{Value: []byte("promoted")}}); err != nil {
		t.Fatal(err)
	}
}

// TestFollowerDoesNotBumpEpochs: the follower role never claims leadership.
func TestFollowerDoesNotBumpEpochs(t *testing.T) {
	lis := bufconnListener()
	leader := openBrokerAt(t, t.TempDir())
	lSrv, _ := NewServer(leader, testLogger(), nil)
	go func() { _ = lSrv.Serve(lis) }()
	defer lSrv.Stop()

	dir := t.TempDir()
	follower := startFollower(t, dir, lis, time.Second)
	defer follower.Close()
	if _, err := os.Stat(filepath.Join(dir, "topics", "default", "partition-0", "epoch")); !os.IsNotExist(err) {
		t.Errorf("follower wrote an epoch file (err %v)", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && stringsIndex(s, sub) >= 0
}

func stringsIndex(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
