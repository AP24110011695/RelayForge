# RelayForge

RelayForge is a durable, partitioned event-streaming platform: a Go event
broker with a gRPC API, an append-only segment-based log storage engine,
consumer groups with at-least-once delivery, and — only after all of that is
proven — limited leader/follower replication with manual failover.

It is deliberately **not** a Kafka clone. The deliverable is a set of
precisely stated, test-backed guarantees (ordering, durability per ack mode,
valid-prefix crash recovery, backpressure, delivery semantics, group
fencing) plus reproducible benchmarks. Scope rules and the full 15-phase
plan: [ROADMAP.md](ROADMAP.md).

## Status

**Phase 1 of 15 — foundations (complete):** WSL2 + Go toolchain set up;
concurrency labs completed in a separate scratch directory (`~/rf-labs`,
intentionally not part of this repository); repository skeleton created.

There is **no RelayForge implementation code yet, by design**: the
repository contains zero `.go` files at the end of Phase 1. The storage
engine (record format, append-only log) starts in Phase 3.

## Development

- Environment: WSL2 (Ubuntu), Go 1.27.1, local git repository (remote not
  configured yet).
- `make build` / `make test` / `make race` / `make vet` / `make fmt` —
  package targets are honest no-ops until Go code exists (Phase 3).
- Architecture decisions: [docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md).
