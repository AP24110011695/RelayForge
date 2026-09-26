# RelayForge

RelayForge is a durable, partitioned event-streaming platform: a Go event
broker with a gRPC API, an append-only segment-based log storage engine,
consumer groups with at-least-once delivery, and — only after all of that is
proven — limited leader/follower replication with manual failover.

It is deliberately **not** a Kafka clone, and it is not an AI project. The
deliverable is a set of precisely stated, test-backed guarantees (ordering,
durability per ack mode, valid-prefix crash recovery, backpressure, delivery
semantics, group fencing) plus reproducible benchmarks.

## Status

**Phase 4 of 10 complete — Durability + Crash Recovery + Segments + Indexes + Retention.**

- Verified WSL2 (Ubuntu 26.04 LTS) toolchain; see
  [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md).
- `internal/logengine`: segment-set log engine — rolling segments with
  sparse indexes, the durability ladder (`None`/`Written`/`Flushed`), crash
  recovery to the longest valid prefix, retention, and a SIGKILL crash
  harness ([docs/STORAGE.md](docs/STORAGE.md),
  [docs/GUARANTEES.md](docs/GUARANTEES.md)).
- `internal/record`: record format v1 (golden + fuzz tested,
  [docs/RECORD_FORMAT.md](docs/RECORD_FORMAT.md)).
- No broker behavior yet, by design. The gRPC API arrives in Phase 5.

Authoritative documents:

- [PLAN.md](PLAN.md) — the 10-phase master execution roadmap.
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) — the coding-agent
  operating manual (the `Execute Phase N` workflow).

The 15-phase `ROADMAP.md` and per-phase `PHASE-1-PLAN.md` from the previous
planning round were retired in favor of `PLAN.md` (see
[docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md), ADR-2; both remain
recoverable from git history).

## Core guarantees (target contract)

- **Ordering** within a partition only — never global, never cross-partition.
- **Durability ack modes**: `ack=flushed` (fsync before ack), `ack=written`
  (page cache; default design target), `ack=none`.
- **Delivery**: at-least-once; duplicates are possible and documented. Not
  exactly-once.
- **Recovery**: crash → longest valid prefix; torn/corrupt tails are
  truncated, never served.
- **Backpressure**: bounded queues; explicit overload errors; unbounded
  memory is a bug.
- **Replication (Phase 10)**: leader/follower, ISR, high-watermark, manual
  failover — explicitly not consensus, not HA, not exactly-once.

Full definitions: [PLAN.md §2](PLAN.md).

## Development

```bash
make build      # go build ./...
make test       # go test ./...
make test-race  # go test -race ./...
make vet        # go vet ./...  + gofmt -l .
make fmt        # gofmt -l -w .
make fuzz       # run each fuzz target for FUZZTIME (first target: Phase 2)
```

- Environment: WSL2 (Ubuntu 26.04 LTS), Go 1.27.1, local git repository (no
  remote, by policy). All build/test/benchmark execution runs in WSL on
  ext4 — never against `/mnt/c` (see [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md)).
- Architecture decisions: [docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md).

Current layout:

```text
PLAN.md, IMPLEMENTATION_PLAN.md   # roadmap + agent operating manual
docs/                             # environment, storage, guarantees, formats, decisions
internal/framing/                 # length-prefix wire framing (tested + fuzzed)
internal/logengine/               # segment-set log engine (durability, recovery, retention)
internal/record/                  # record format v1 (golden + fuzz tested)
internal/version/                 # build information
```

The tree grows only when its functionality exists.
