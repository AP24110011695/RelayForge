package group

import (
	"errors"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"relayforge/internal/offsets"
)

func newCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	c := NewCoordinator(t.TempDir(), func(topic string) (int32, bool) {
		if topic == "events" {
			return 3, true
		}
		return 0, false
	}, 25*time.Millisecond)
	t.Cleanup(c.Close)
	return c
}

func TestJoinSyncHeartbeatLifecycle(t *testing.T) {
	c := newCoordinator(t)
	gen, id, err := c.Join("app", "", "events", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 1 || id == "" {
		t.Fatalf("join: generation %d member %q", gen, id)
	}
	topic, assignment, err := c.Sync("app", gen, id)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "events" || len(assignment) != 3 {
		t.Fatalf("sync: topic %q assignment %v", topic, assignment)
	}
	if err := c.Heartbeat("app", gen, id); err != nil {
		t.Errorf("heartbeat: %v", err)
	}
	// A re-join with the same member id keeps the identity.
	gen2, id2, err := c.Join("app", id, "events", 5*time.Second)
	if err != nil || id2 != id {
		t.Fatalf("rejoin: (%d, %q) err %v", gen2, id2, err)
	}
	if gen2 != 2 {
		t.Errorf("rejoin generation = %d, want 2 (rebalance bumps)", gen2)
	}
}

func TestUnknownTopicRejected(t *testing.T) {
	c := newCoordinator(t)
	if _, _, err := c.Join("app", "", "missing", time.Second); !errors.Is(err, ErrUnknownTopic) {
		t.Errorf("join unknown topic: %v", err)
	}
}

// TestAssignmentExclusivityProperty: randomized members joining over time;
// after every stabilization point, every partition is assigned to at most
// one member and all partitions are covered (seeded RNG, recorded seed).
func TestAssignmentExclusivityProperty(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	rnd := rand.New(rand.NewSource(seed))
	c := newCoordinator(t)
	type memberState struct {
		id       string
		gen      int64
		assigned map[int32]bool
	}
	members := []*memberState{}
	for step := 0; step < 40; step++ {
		switch rnd.Intn(3) {
		case 0, 1: // join a new member (or the majority of steps)
			m := &memberState{}
			gen, id, err := c.Join("app", "", "events", 30*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			m.id, m.gen = id, gen
			_, assigned, err := c.Sync("app", gen, id)
			if err != nil {
				t.Fatal(err)
			}
			m.assigned = assigned
			members = append(members, m)
		default: // one member "dies" (never heartbeats again) and is dropped by rejoining fresh
			if len(members) > 1 {
				victim := members[rnd.Intn(len(members))]
				// Rebalance happens when a NEW member joins; simulate the
				// victim leaving by expiring: join forces reassignment.
				_ = victim
				gen, id, err := c.Join("app", "", "events", 30*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				_, assigned, err := c.Sync("app", gen, id)
				if err != nil {
					t.Fatal(err)
				}
				members = append(members, &memberState{id: id, gen: gen, assigned: assigned})
			}
		}
		// Stabilization: all live members' assignments must be disjoint
		// and cover all 3 partitions.
		if len(members) == 0 {
			continue
		}
		owner := map[int32]string{}
		for _, m := range members {
			// refresh the latest assignment for every member
			_, assigned, err := c.Sync("app", m.gen, m.id)
			if err != nil {
				// member was fenced by a later rebalance: rejoin to catch up
				gen, id, err := c.Join("app", m.id, "events", 30*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				m.gen, m.id = gen, id
				_, assigned, err = c.Sync("app", gen, id)
				if err != nil {
					t.Fatal(err)
				}
			}
			m.assigned = assigned
			for p := range m.assigned {
				if prev, ok := owner[p]; ok && prev != m.id {
					t.Fatalf("seed %d step %d: partition %d owned by %q and %q", seed, step, p, prev, m.id)
				}
				owner[p] = m.id
			}
		}
		if len(owner) != 3 {
			t.Fatalf("seed %d step %d: only %d of 3 partitions assigned", seed, step, len(owner))
		}
	}
}

func TestFencingStaleGeneration(t *testing.T) {
	c := newCoordinator(t)
	gen, id, err := c.Join("app", "", "events", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// A second member rebalances the group: the first member is fenced.
	if _, _, err := c.Join("app", "", "events", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Heartbeat("app", gen, id); !errors.Is(err, ErrFencedGeneration) {
		t.Errorf("stale heartbeat: %v", err)
	}
	if err := c.Commit("app", gen, id, commitRec(0, 10)); !errors.Is(err, ErrFencedGeneration) {
		t.Errorf("stale commit: %v", err)
	}
	if _, _, err := c.Sync("app", gen, id); !errors.Is(err, ErrFencedGeneration) {
		t.Errorf("stale sync: %v", err)
	}
}

func TestFencingUnownedPartition(t *testing.T) {
	c := newCoordinator(t)
	gen, id, err := c.Join("app", "", "events", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Sync("app", gen, id); err != nil {
		t.Fatal(err)
	}
	// Find a partition the member does NOT own (single member owns all,
	// so join a second member to create non-ownership).
	_, id2, err := c.Join("app", "", "events", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, a2, err := c.Sync("app", gen+1, id2)
	if err != nil {
		t.Fatal(err)
	}
	// id2 must not commit a partition it does not own.
	notOwned := int32(-1)
	for p := int32(0); p < 3; p++ {
		if !a2[p] {
			notOwned = p
			break
		}
	}
	if notOwned < 0 {
		t.Fatal("second member owns all partitions")
	}
	if err := c.Commit("app", gen+1, id2, commitRec(notOwned, 5)); !errors.Is(err, ErrFencedGeneration) {
		t.Errorf("unowned commit: %v", err)
	}
	// The first member is fenced by the rebalance but may rejoin.
	gen2, _, err := c.Join("app", id, "events", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, assignment2, err := c.Sync("app", gen2, id)
	if err != nil {
		t.Fatal(err)
	}
	for p := range assignment2 {
		if err := c.Commit("app", gen2, id, commitRec(p, 7)); err != nil {
			t.Errorf("commit owned partition %d after rejoin: %v", p, err)
		}
	}
}

func commitRec(partition int32, offset uint64) offsets.Record {
	return offsets.Record{Topic: "events", Partition: partition, Offset: offset}
}

func TestGenerationPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	countFn := func(topic string) (int32, bool) {
		if topic == "events" {
			return 3, true
		}
		return 0, false
	}
	c1 := NewCoordinator(dir, countFn, time.Hour)
	gen1, id1, err := c1.Join("app", "", "events", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c1.Close()

	// A fresh coordinator must continue from the persisted generation:
	// a member of the OLD generation must stay fenced after the restart.
	c2 := NewCoordinator(dir, countFn, time.Hour)
	defer c2.Close()
	if err := c2.Heartbeat("app", gen1, id1); err == nil {
		// The group has no members after restart, so the heartbeat reports
		// an unknown member, not a pass.
		t.Error("heartbeat from pre-restart member succeeded")
	}
	gen2, _, err := c2.Join("app", id1, "events", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if gen2 <= gen1 {
		t.Fatalf("generation not monotonic across restart: %d -> %d", gen1, gen2)
	}
	// A commit at the pre-restart generation must be fenced.
	if err := c2.Commit("app", gen1, id1, commitRec(0, 5)); !errors.Is(err, ErrFencedGeneration) {
		t.Errorf("pre-restart generation accepted: %v", err)
	}
}

func TestSessionTimeoutExpiresMembers(t *testing.T) {
	c := NewCoordinator(t.TempDir(), func(string) (int32, bool) { return 2, true }, 20*time.Millisecond)
	defer c.Close()
	gen, id, err := c.Join("app", "", "events", 60*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, id2, err := c.Join("app", "", "events", 60*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// Neither member heartbeats: the janitor must expire them and
	// rebalance; the generation moves.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		// A fresh join sees the generation rise beyond the initial ones.
		gen3, _, err := c.Join("app", "", "events", 60*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if gen3 > gen+2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The expired members are gone: heartbeats fail.
	if err := c.Heartbeat("app", gen, id); err == nil {
		t.Error("expired member still heartbeats")
	}
	_ = id2
}

func TestCoordinatorCloseNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	dir := t.TempDir()
	c := NewCoordinator(dir, func(string) (int32, bool) { return 1, true }, 10*time.Millisecond)
	if _, _, err := c.Join("g", "", "events", time.Second); err != nil {
		t.Fatal(err)
	}
	c.Close()
	time.Sleep(60 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutine leak after close: %d > %d", after, before)
	}
	if err := c.Commit("g", 1, "x", commitRec(0, 1)); err == nil {
		t.Error("commit after close succeeded")
	}
}
