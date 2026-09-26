package broker

// Phase 7 wire-level consumer-group tests: assignment exclusivity across
// two members, rebalance on heartbeat timeout, generation fencing over the
// wire, and the redelivery half of at-least-once.

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relayforgev1 "relayforge/api/proto/relayforge/v1"
)

func joinAndSync(t *testing.T, env *grpcEnv, group, topic, memberID string, sessionMs int32) (int64, string, map[int32]bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	jresp, err := env.client.JoinGroup(ctx, &relayforgev1.JoinGroupRequest{
		Group: group, MemberId: memberID, Topic: topic, SessionTimeoutMs: sessionMs,
	})
	if err != nil {
		t.Fatal(err)
	}
	sresp, err := env.client.SyncGroup(ctx, &relayforgev1.SyncGroupRequest{
		Group: group, GenerationId: jresp.GetGenerationId(), MemberId: jresp.GetMemberId(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assigned := map[int32]bool{}
	for _, a := range sresp.GetAssignment() {
		for _, p := range a.GetPartitions() {
			assigned[p] = true
		}
	}
	return jresp.GetGenerationId(), jresp.GetMemberId(), assigned
}

func TestGroupsTwoMembersDisjointAssignment(t *testing.T) {
	env := startGRPC(t)
	if _, err := env.client.CreateTopic(context.Background(), &relayforgev1.CreateTopicRequest{Name: "events", PartitionCount: 3}); err != nil {
		t.Fatal(err)
	}
	_, id1, _ := joinAndSync(t, env, "app", "events", "", 60000)
	gen2, id2, a2 := joinAndSync(t, env, "app", "events", "", 60000)
	// Every join rebalances and fences earlier views (documented MVP
	// protocol), so member 1 re-joins for a current view. Assignments are
	// a deterministic function of the member set, so the two views are
	// comparable.
	_, _, a1 := joinAndSync(t, env, "app", "events", id1, 60000)
	for p := range a1 {
		if a2[p] {
			t.Errorf("partition %d assigned to both members", p)
		}
	}
	if len(a1)+len(a2) != 3 {
		t.Errorf("assignments cover %d of 3 partitions", len(a1)+len(a2))
	}
	if len(a1) == 0 || len(a2) == 0 {
		t.Errorf("unbalanced assignment: %v / %v", a1, a2)
	}
	// Member 2's generation is now stale (member 1's re-join bumped it):
	// its heartbeat and commits must be fenced with the FENCED prefix.
	_, err := env.client.Heartbeat(context.Background(), &relayforgev1.HeartbeatRequest{Group: "app", GenerationId: gen2, MemberId: id2})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "FENCED") {
		t.Fatalf("stale heartbeat: %v", err)
	}
	_, err = env.client.CommitOffset(context.Background(), &relayforgev1.CommitOffsetRequest{
		Group: "app", GenerationId: gen2, MemberId: id2, Topic: "events", Partition: 0, Offset: 5,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale commit: %v", err)
	}
	// Member 1 holds the current generation: a valid commit lands.
	gen3, _, a1cur := joinAndSync(t, env, "app", "events", id1, 60000)
	if _, err := env.client.CommitOffset(context.Background(), &relayforgev1.CommitOffsetRequest{
		Group: "app", GenerationId: gen3, MemberId: id1, Topic: "events",
		Partition: firstOf(a1cur), Offset: 7,
	}); err != nil {
		t.Fatalf("valid commit: %v", err)
	}
	gresp, err := env.client.GetOffsets(context.Background(), &relayforgev1.GetOffsetsRequest{Group: "app", Topic: "events"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, o := range gresp.GetOffsets() {
		if o.GetPartition() == firstOf(a1cur) && o.GetOffset() == 7 && o.GetCommitted() {
			found = true
		}
	}
	if !found {
		t.Fatalf("committed offset not returned: %+v", gresp.GetOffsets())
	}
}

func TestRedeliveryAfterCrashBeforeCommit(t *testing.T) {
	env := startGRPC(t)
	ctx := context.Background()
	if _, err := env.client.Produce(ctx, &relayforgev1.ProduceRequest{
		Topic: DefaultTopic,
		Messages: []*relayforgev1.Message{
			{Value: []byte("m0")}, {Value: []byte("m1")}, {Value: []byte("m2")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	gen, _, assigned := joinAndSync(t, env, "workers", DefaultTopic, "", 600)
	if !assigned[0] {
		t.Fatalf("partition 0 not assigned to the only member: %v", assigned)
	}
	// "Process" 3 records but crash BEFORE committing: the offsets stay
	// uncommitted, so a restarted member must see them again.
	fresp, err := env.client.Fetch(ctx, &relayforgev1.FetchRequest{Topic: DefaultTopic, Partition: 0})
	if err != nil || len(fresp.GetRecords()) != 3 {
		t.Fatalf("fetch: %v records=%d", err, len(fresp.GetRecords()))
	}
	// Restart: a fresh member joins (new id — the crashed one is gone).
	// The dead member's short session must expire before the fresh member
	// can own partition 0 (timeout-only membership), so loop.
	var gen2 int64
	var id2 string
	var assigned2 map[int32]bool
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		gen2, id2, assigned2 = joinAndSync(t, env, "workers", DefaultTopic, "", 600)
		if assigned2[0] {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !assigned2[0] {
		t.Fatalf("partition 0 not assigned after restart: %v", assigned2)
	}
	offResp, err := env.client.GetOffsets(ctx, &relayforgev1.GetOffsetsRequest{Group: "workers", Topic: DefaultTopic, Partitions: []int32{0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(offResp.GetOffsets()) != 0 {
		t.Fatalf("offsets committed despite no commit: %+v", offResp.GetOffsets())
	}
	fresp2, err := env.client.Fetch(ctx, &relayforgev1.FetchRequest{Topic: DefaultTopic, Partition: 0})
	if err != nil || len(fresp2.GetRecords()) != 3 || string(fresp2.GetRecords()[0].GetValue()) != "m0" {
		t.Fatalf("redelivery expected: %v %v", fresp2.GetRecords(), err)
	}
	// Commit after processing: the redelivered range is not delivered again
	// to a consumer that resumes from the committed offset.
	if _, err := env.client.CommitOffset(ctx, &relayforgev1.CommitOffsetRequest{
		Group: "workers", GenerationId: gen2, MemberId: id2, Topic: DefaultTopic, Partition: 0, Offset: 3,
	}); err != nil {
		t.Fatal(err)
	}
	offResp2, err := env.client.GetOffsets(ctx, &relayforgev1.GetOffsetsRequest{Group: "workers", Topic: DefaultTopic, Partitions: []int32{0}})
	if err != nil || len(offResp2.GetOffsets()) != 1 || offResp2.GetOffsets()[0].GetOffset() != 3 {
		t.Fatalf("committed offset: %+v err %v", offResp2.GetOffsets(), err)
	}
	_ = gen
}

// TestHeartbeatTimeoutRebalance: a member that stops heartbeating is
// expired by the janitor; its partitions are reassigned and the survivor
// can pick them up by re-joining.
func TestHeartbeatTimeoutRebalance(t *testing.T) {
	env := startGRPC(t)
	if _, err := env.client.CreateTopic(context.Background(), &relayforgev1.CreateTopicRequest{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	// Short session for the victim; janitor sweeps every second.
	_, _, _ = joinAndSync(t, env, "app", "events", "", 700)
	_, id2, _ := joinAndSync(t, env, "app", "events", "", 60000)
	// The victim (id1) never heartbeats again. The survivor re-joins with
	// its own id until the expiry rebalance gives it both partitions.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
		_, _, assigned := joinAndSync(t, env, "app", "events", id2, 60000)
		if len(assigned) == 2 {
			return
		}
	}
	t.Fatal("timed out waiting for the expired member's partitions to be reassigned")
}

func firstOf(m map[int32]bool) int32 {
	for k := range m {
		return k
	}
	return -1
}
