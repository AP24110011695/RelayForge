# Limitations

The honest list. Anything absent from this file but true of the system
would be a documentation failure — the same one the project exists to
avoid.

## Replication (Phase 10) — the fallback boundary

- **Async, single-follower, static placement.** The tail of the leader's
  log that has not been pulled is lost when the leader dies.
- **`acks=all` is not supported.** With replication enabled, produces
  accept `none`/`written`/`flushed` against the leader only. A future
  full-boundary implementation would gate acks on a high-watermark.
- **No automatic zombie fencing.** A leader left running while its follower
  is promoted keeps accepting produces (split brain). The supported
  procedure stops the old leader first (docs/DEMO.md).
- **Not consensus, not Raft, not HA, not exactly-once.** It is a
  manually-failed-over async copy. PLAN.md forbids presenting it otherwise;
  so does this file.
- Clients are re-pointed manually after failover.

## MVP scope boundaries (by design, per PLAN.md §13)

- No TLS/authentication (plain gRPC for local/lab use).
- No Kafka protocol compatibility, schema registry, or stream processing.
- No Kubernetes; Docker Compose artifacts are authored but were not
  verified live (Docker Desktop WSL integration disabled at execution
  time — docs/ENVIRONMENT.md).
- No streaming RPCs (unary only); no auto-commit; no partition resizing;
  no compression; no offset-log compaction; no cooperative rebalancing.
- Consumer groups: single-topic subscriptions, broker-computed eager
  rebalance, exclusivity enforced by generation fencing + client rejoin
  (the fetch path is stateless by design).
- No file locking: opening the same log directory twice is unsupported.
- No multi-region, multi-tenancy, quotas, billing, cross-partition
  transactions.

## Environment-bound results

- Benchmark numbers are specific to the recorded WSL2 environment
  (docs/BENCHMARKS.md); nothing is a universal performance claim.
- Power-loss durability of `ack=flushed` is by construction (fsync
  ordering) plus documented filesystem assumptions — never claimed as
  power-loss TESTED.
- The Compose/Grafana demo stack was not executed in CI for the same
  Docker reason; the metrics themselves are unit-verified.
