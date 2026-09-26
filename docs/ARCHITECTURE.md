# Architecture

The broker's concurrency and ownership model — the "who owns what" map that
the code follows. Component responsibilities live in the package docs; this
file records the invariants.

## Component stack

```text
relayctl / rfbench          (clients)
        | gRPC (unary, protobuf)
server.go: interceptors (recovery → metrics → logging) + status mapping
internal/broker: Produce pipeline | Fetch (long-poll) | group coordinator
internal/metadata: CRC'd topic table (refuse-to-start on corruption)
internal/group + internal/offsets: membership, assignments, fencing, durable commits
internal/logengine: segment set, sparse indexes, durability ladder, recovery, retention
internal/record: format v1 (CRC32C, little-endian)
filesystem: write → page cache → fsync (+ directory fsyncs)
```

## Data layout

```text
<data-dir>/
  metadata.json + metadata.crc      # topic table (atomic, CRC'd)
  topics/<topic>/partition-<n>/     # one log-engine directory per partition
    <base>.log / <base>.index       # segments + sparse indexes (derived)
    epoch                           # leader epoch (crcfile; Phase 10)
  groups/<group>/
    offsets/                        # durable commit log (record format)
    generation                      # persisted generation counter (crcfile)
```

## Produce pipeline (the ordering guarantee)

```text
Produce RPC
  → route (partition pin | FNV-1a(key) | round-robin)
  → per-partition bounded queue (chan, depth = ProduceQueueDepth)
  → ONE writer goroutine per partition (owns the log's append path)
  → logengine.Append (durability per ack mode)
  → per-item result channel → RPC response (offsets in request order)
```

Invariants:

- **Ordering**: one writer per partition ⇒ queue arrival order is append
  order. Per-partition only — never global, never cross-partition.
- **Backpressure**: a full queue blocks the RPC until its deadline, then
  fails with `ErrOverloaded` (`RESOURCE_EXHAUSTED`, retryable). Memory is
  bounded by `queue depth × max record size` per partition.
- **Failure containment**: a writer panic marks only its own partition
  failed (fast-fail for new items; other partitions unaffected).
- **Shutdown**: `closed` flag under RWMutex → in-flight WaitGroup drained →
  queues closed → writers exit → logs closed. Acked records are durable
  after shutdown per their mode.

## Fetch path

Stateless by design: `Fetch(topic, partition, offset)` reads from the
partition's log with a long-poll (polling loop) when at the log end.
Exclusivity is enforced at assignment + commit fencing (Phase 7), not in
the fetch path — a fenced member's fetches still work, but its commits are
rejected, which is what forces the rejoin.

## Consumer groups

Coordinator (one janitor goroutine): membership by heartbeat/session
timeout, broker-computed balanced assignment on every join/expiry, a
**persisted generation counter** (crcfile) so stale members stay fenced
across broker restarts. Commits are fenced by generation AND assignment
ownership, then appended durably to the group's offsets log.

## Durability ladder (recap)

`None` / `Written` (page cache) / `Flushed` (fsync before return; degraded
state on fsync errors). Details and the honest testing boundary:
[STORAGE.md](STORAGE.md), [GUARANTEES.md](GUARANTEES.md).

## Replication (Phase 10 fallback boundary)

Follower pull loops (one per partition) append the leader's records
locally; produces are rejected by role; promotion = restart without the
follower role + persisted epoch bump. Explicitly not consensus/HA; zombie
leaders are handled by operator procedure. [REPLICATION.md](REPLICATION.md).
