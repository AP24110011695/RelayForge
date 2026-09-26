// Package group implements the consumer-group coordinator: membership with
// heartbeats and session timeouts, broker-computed balanced assignments,
// generation-based fencing of offset commits, and a generation counter
// persisted across broker restarts (so stale members stay fenced).
//
// Documented MVP simplifications (see docs/GUARANTEES.md):
//   - single-topic subscriptions per member;
//   - assignment is computed by the broker at join/leave time (no
//     client-leader sync round);
//   - exclusivity converges through fencing: after a rebalance, stale
//     members' commits are rejected and they must rejoin — the fetch path
//     itself stays stateless;
//   - membership ends via session timeout only (no explicit Leave RPC).
package group

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"relayforge/internal/crcfile"
	"relayforge/internal/offsets"
)

// Errors returned by the coordinator.
var (
	// ErrUnknownGroup means the group has no members (and no commits).
	ErrUnknownGroup = errors.New("group: unknown group")
	// ErrUnknownMemberID means the member is not part of the group.
	ErrUnknownMemberID = errors.New("group: unknown member")
	// ErrUnknownTopic means the subscribed topic does not exist.
	ErrUnknownTopic = errors.New("group: unknown topic")
	// ErrFencedGeneration means the caller's generation is stale or the
	// member does not own the partition it commits for: the client must
	// rejoin.
	ErrFencedGeneration = errors.New("group: fenced generation")
)

// MemberInfo is a member snapshot for Describe.
type MemberInfo struct {
	ID         string
	Topic      string
	Assignment map[int32]bool
}

type member struct {
	id             string
	topic          string
	sessionTimeout time.Duration
	lastSeen       time.Time
	assignment     map[int32]bool
}

type groupState struct {
	name       string
	generation int64
	members    map[string]*member
}

// Coordinator manages all consumer groups on the broker.
type Coordinator struct {
	mu             sync.Mutex
	dir            string // <dataDir>/groups
	groups         map[string]*groupState
	managers       map[string]*offsets.Manager
	partitionCount func(topic string) (int32, bool)
	cancel         context.CancelFunc
	closed         bool
}

// NewCoordinator creates the coordinator and starts the session-timeout
// janitor (one goroutine; sweepInterval <= 0 selects 1s).
func NewCoordinator(dataDir string, partitionCount func(topic string) (int32, bool), sweepInterval time.Duration) *Coordinator {
	if sweepInterval <= 0 {
		sweepInterval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		dir:            filepath.Join(dataDir, "groups"),
		groups:         map[string]*groupState{},
		managers:       map[string]*offsets.Manager{},
		partitionCount: partitionCount,
		cancel:         cancel,
	}
	go c.janitorLoop(ctx, sweepInterval)
	return c
}

func (c *Coordinator) janitorLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.sweep(time.Now())
		}
	}
}

// sweep expires timed-out members and rebalances affected groups.
func (c *Coordinator) sweep(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	for _, g := range c.groups {
		expired := false
		for id, m := range g.members {
			if now.Sub(m.lastSeen) > m.sessionTimeout {
				delete(g.members, id)
				expired = true
			}
		}
		if expired {
			_ = c.rebalanceLocked(g) // best-effort: retried on the next sweep
		}
	}
}

// Join adds (or re-joins) a member and triggers a rebalance: the
// generation is bumped and persisted BEFORE the response, so a crash after
// the response cannot resurrect the previous generation's fencing state.
func (c *Coordinator) Join(group, memberID, topic string, sessionTimeout time.Duration) (int64, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, "", errors.New("group: coordinator is closed")
	}
	if sessionTimeout <= 0 {
		return 0, "", fmt.Errorf("group: session timeout must be positive")
	}
	if _, ok := c.partitionCount(topic); !ok {
		return 0, "", fmt.Errorf("%w: %q", ErrUnknownTopic, topic)
	}
	g := c.groups[group]
	if g == nil {
		gen, err := c.loadGeneration(group)
		if err != nil {
			return 0, "", err
		}
		g = &groupState{name: group, generation: gen, members: map[string]*member{}}
		c.groups[group] = g
	}
	if memberID == "" {
		id, err := newMemberID()
		if err != nil {
			return 0, "", err
		}
		memberID = id
	}
	m, ok := g.members[memberID]
	if !ok {
		m = &member{id: memberID}
		g.members[memberID] = m
	}
	m.topic = topic
	m.sessionTimeout = sessionTimeout
	m.lastSeen = time.Now()
	if err := c.rebalanceLocked(g); err != nil {
		return 0, "", err
	}
	return g.generation, memberID, nil
}

// Sync returns the member's subscribed topic and assignment for its
// current generation.
func (c *Coordinator) Sync(group string, generation int64, memberID string) (string, map[int32]bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, err := c.memberLocked(group, generation, memberID)
	if err != nil {
		return "", nil, err
	}
	out := make(map[int32]bool, len(m.assignment))
	for p := range m.assignment {
		out[p] = true
	}
	return m.topic, out, nil
}

// Heartbeat refreshes a member's session; a stale generation is fenced.
func (c *Coordinator) Heartbeat(group string, generation int64, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.memberLocked(group, generation, memberID)
	return err
}

// Commit fences the member against its generation AND its assignment, then
// durably stores the committed offset.
func (c *Coordinator) Commit(group string, generation int64, memberID string, rec offsets.Record) error {
	c.mu.Lock()
	m, err := c.memberLocked(group, generation, memberID)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if !m.assignment[rec.Partition] {
		c.mu.Unlock()
		return fmt.Errorf("%w: member %s does not own partition %d", ErrFencedGeneration, memberID, rec.Partition)
	}
	manager, err := c.managerLocked(group)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	return manager.Commit(rec)
}

// Committed returns the committed offset for a group/partition (read-only:
// no fencing applies).
func (c *Coordinator) Committed(group string, partition int32) (uint64, bool, error) {
	c.mu.Lock()
	manager, err := c.managerLocked(group)
	c.mu.Unlock()
	if err != nil {
		return 0, false, err
	}
	off, ok := manager.Get(partition)
	return off, ok, nil
}

// Describe returns the generation and member snapshot.
func (c *Coordinator) Describe(group string) (int64, []MemberInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		return 0, nil, fmt.Errorf("%w: %q", ErrUnknownGroup, group)
	}
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]MemberInfo, 0, len(ids))
	for _, id := range ids {
		m := g.members[id]
		a := make(map[int32]bool, len(m.assignment))
		for p := range m.assignment {
			a[p] = true
		}
		out = append(out, MemberInfo{ID: id, Topic: m.topic, Assignment: a})
	}
	return g.generation, out, nil
}

// Close stops the janitor and closes the offsets managers.
func (c *Coordinator) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	managers := c.managers
	c.managers = map[string]*offsets.Manager{}
	c.mu.Unlock()
	c.cancel()
	for _, m := range managers {
		_ = m.Close()
	}
}

// memberLocked validates group/generation/member (caller holds the lock).
func (c *Coordinator) memberLocked(group string, generation int64, memberID string) (*member, error) {
	g := c.groups[group]
	if g == nil {
		return nil, fmt.Errorf("%w: %q", ErrUnknownGroup, group)
	}
	if generation != g.generation {
		return nil, fmt.Errorf("%w: member %s at generation %d, current %d",
			ErrFencedGeneration, memberID, generation, g.generation)
	}
	m := g.members[memberID]
	if m == nil {
		return nil, fmt.Errorf("%w: %q", ErrUnknownMemberID, memberID)
	}
	m.lastSeen = time.Now()
	return m, nil
}

// rebalanceLocked bumps the generation (persisting it first), reassigns
// partitions over the current members (balanced round-robin over sorted
// member ids), and resets their timeouts.
func (c *Coordinator) rebalanceLocked(g *groupState) error {
	g.generation++
	if err := c.persistGeneration(g.name, g.generation); err != nil {
		g.generation--
		return fmt.Errorf("group: persisting generation for %q: %w", g.name, err)
	}
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	count, ok := c.partitionCount(g.members[ids[0]].topic)
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownTopic, g.members[ids[0]].topic)
	}
	for _, id := range ids {
		g.members[id].assignment = map[int32]bool{}
	}
	for p := int32(0); p < count; p++ {
		owner := ids[int(p)%len(ids)]
		g.members[owner].assignment[p] = true
	}
	return nil
}

func (c *Coordinator) managerLocked(group string) (*offsets.Manager, error) {
	if m, ok := c.managers[group]; ok {
		return m, nil
	}
	m, err := offsets.OpenManager(filepath.Dir(c.dir), group)
	if err != nil {
		return nil, err
	}
	c.managers[group] = m
	return m, nil
}

// generationFile is the persisted generation counter.
type generationFile struct {
	Generation int64 `json:"generation"`
}

func (c *Coordinator) generationPath(group string) string {
	return filepath.Join(c.dir, group, "generation")
}

func (c *Coordinator) loadGeneration(group string) (int64, error) {
	data, err := crcfile.Load(c.generationPath(group))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil // new group: generation starts at 0, first join -> 1
		}
		return 0, fmt.Errorf("group: %w", err)
	}
	var gf generationFile
	if err := json.Unmarshal(data, &gf); err != nil {
		return 0, fmt.Errorf("group: generation file for %q is corrupt: %w", group, err)
	}
	return gf.Generation, nil
}

func (c *Coordinator) persistGeneration(group string, generation int64) error {
	if err := os.MkdirAll(filepath.Dir(c.generationPath(group)), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(generationFile{Generation: generation})
	if err != nil {
		return err
	}
	return crcfile.Write(c.generationPath(group), data)
}

func newMemberID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
