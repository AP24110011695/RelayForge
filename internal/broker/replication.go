package broker

// Phase 10 replication — the plan's VALIDATED FALLBACK BOUNDARY (see
// docs/REPLICATION.md): single-follower ASYNC replication with static
// placement. A follower pulls records from its leader and appends them
// locally; produces are rejected on followers; promotion is a manual
// restart without the follower role, which bumps a persisted per-partition
// epoch. A zombie leader that keeps running in leader mode is NOT
// automatically fenced — the operator kills it first (DEMO.md procedure);
// acks=all is explicitly unsupported in this boundary. This is not
// consensus, not HA, and not exactly-once.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relayforgev1 "relayforge/api/proto/relayforge/v1"
	"relayforge/internal/crcfile"
	"relayforge/internal/logengine"
	"relayforge/internal/record"
)

// Replication configures the follower role. nil (the default) means this
// broker is a standalone/leader broker.
type Replication struct {
	// PeerAddr is the leader's gRPC address.
	PeerAddr string
	// PollInterval is the fetch cadence; <= 0 selects 100ms.
	PollInterval time.Duration
	// ClientConn, when set (tests), replaces dialing PeerAddr.
	ClientConn *grpc.ClientConn
}

// ErrNotLeader is returned when a produce reaches a follower.
var ErrNotLeader = errors.New("broker: this broker is a replication follower (not the leader); produce to the leader")

// epochFile is the persisted per-partition leader epoch.
type epochFile struct {
	Epoch int64 `json:"epoch"`
}

type replicationState struct {
	cancel  context.CancelFunc
	done    sync.WaitGroup
	mu      sync.Mutex
	covered map[string]bool
}

type repClient struct {
	client relayforgev1.RelayForgeClient
}

// startReplication launches the follower supervisor: one goroutine that
// periodically discovers this broker's topics/partitions and starts a pull
// loop for each new one. Topics created after startup are picked up within
// the discovery interval.
func (b *Broker) startReplication(cfg Replication) error {
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	conn := cfg.ClientConn
	if conn == nil {
		var err error
		conn, err = grpc.NewClient(cfg.PeerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.repl = &replicationState{cancel: cancel, covered: map[string]bool{}}
	rc := repClient{client: relayforgev1.NewRelayForgeClient(conn)}
	discover := func() {
		b.mu.RLock()
		snapshot := map[string][]*partition{}
		for topic, parts := range b.parts {
			snapshot[topic] = append([]*partition(nil), parts...)
		}
		b.mu.RUnlock()
		for topic, parts := range snapshot {
			for _, p := range parts {
				key := fmt.Sprintf("%s/%d", topic, p.id)
				b.repl.mu.Lock()
				if b.repl.covered[key] {
					b.repl.mu.Unlock()
					continue
				}
				b.repl.covered[key] = true
				b.repl.done.Add(1)
				b.repl.mu.Unlock()
				go b.followerLoop(ctx, rc, topic, p, interval)
			}
		}
	}
	discover()
	b.repl.done.Add(1)
	go func() {
		defer b.repl.done.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				discover()
			}
		}
	}()
	return nil
}

// followerLoop pulls records from the leader and appends them locally.
// Delivery is async and best-effort at the tail (fallback boundary): the
// loop retries transient errors with backoff and keeps the local log a
// valid prefix of the leader's history.
func (b *Broker) followerLoop(ctx context.Context, rc repClient, topic string, p *partition, interval time.Duration) {
	defer b.repl.done.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		next := p.log.NextOffset()
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := rc.client.ReplicateFetch(rctx, &relayforgev1.ReplicateFetchRequest{
			Topic: topic, Partition: p.id, FromOffset: next,
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.metrics.ReplicationErrorInc()
			continue
		}
		for _, r := range resp.GetRecords() {
			if _, err := p.log.Append(toRecord(r), logengine.DurabilityWritten); err != nil {
				break // keep the valid prefix; retry next round
			}
		}
		lag := resp.GetHighWatermark() - p.log.NextOffset()
		b.metrics.ReplicationLagObs(topic, p.id, lag)
	}
}

func toRecord(r *relayforgev1.FetchedRecord) record.Record {
	return record.Record{Key: r.GetKey(), Value: r.GetValue(), Timestamp: r.GetTimestampMs()}
}

// StopReplication stops the follower loops (Close path).
func (b *Broker) stopReplication() {
	if b.repl != nil {
		b.repl.cancel()
		b.repl.done.Wait()
		b.repl = nil
	}
}

// bumpEpochs asserts leadership after a manual promotion: every partition's
// persisted epoch is incremented. Called on standalone (non-follower)
// startup.
func (b *Broker) bumpEpochs() error {
	for topic, parts := range b.parts {
		for _, p := range parts {
			path := epochPath(b.store.Dir(), topic, p.id)
			current := int64(0)
			if data, err := crcfile.Load(path); err == nil {
				var ef epochFile
				if err := json.Unmarshal(data, &ef); err != nil {
					return fmt.Errorf("replication: epoch file for %s/%d is corrupt: %w", topic, p.id, err)
				}
				current = ef.Epoch
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("replication: reading epoch for %s/%d: %w", topic, p.id, err)
			}
			data, err := json.Marshal(epochFile{Epoch: current + 1})
			if err != nil {
				return err
			}
			if err := crcfile.Write(path, data); err != nil {
				return err
			}
		}
	}
	return nil
}

func epochPath(dataDir, topic string, partition int32) string {
	return filepath.Join(dataDir, "topics", topic, fmt.Sprintf("partition-%d", partition), "epoch")
}
