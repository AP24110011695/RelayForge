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

**Phase 2 of 10 complete — Networking Fundamentals + Binary Protocol Concepts.**

- Verified WSL2 (Ubuntu 26.04 LTS) toolchain; see
  [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md).
- `internal/framing`: tested and fuzzed length-prefix wire framing
  ([docs/FRAMING.md](docs/FRAMING.md)); TCP/framing labs live in `~/rf-labs`.
- `internal/version`: build information (first library package, Phase 1).
- No broker behavior yet, by design. The record format and log engine start
  in Phase 3; the gRPC API in Phase 5.

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
docs/                             # environment, framing, design decisions
internal/framing/                 # length-prefix wire framing (tested + fuzzed)
internal/version/                 # build information (first library package)
```

The tree grows only when its functionality exists.
