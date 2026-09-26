# Demo — showing behavior, not UI

The portfolio demonstration: every step shows a real system property.
Run from the WSL2 ext4 checkout. Single-broker steps 1–12 cover the MVP;
step 13 needs two brokers (Phase 10, fallback boundary).

```sh
# Terminal 0: build once
go build -o /tmp/rf-demo/rfbroker ./cmd/rfbroker
go build -o /tmp/rf-demo/relayctl ./cmd/relayctl
```

## Single broker (steps 1–12)

```sh
# Start the broker + metrics (Prometheus/Grafana via docker compose when
# Docker is available; /metrics works regardless).
/tmp/rf-demo/rfbroker -addr 127.0.0.1:7000 -admin-addr 127.0.0.1:7001 -data-dir ~/rf-demo-data
```

1. **Produce events** — `... | relayctl produce -addr 127.0.0.1:7000`
   (one message per stdin line) or `-m "hello"`.
2. **Show partitions** — `relayctl topic create -name orders -partitions 4`
   then `relayctl topic list`; produce with `-key user-1` and fetch each
   partition: same keys always land in the same partition.
3. **Consumers processing** — `relayctl consume -group workers -topic orders`
   in 2–3 terminals: each shows its assigned partitions (generation printed).
4. **Pause a consumer** — Ctrl-Z (or kill it): keep producing.
5. **Consumer lag rises** — after the session timeout the survivors rebalance
   and catch up; observe `relayforge_consumer_lag` on
   `curl 127.0.0.1:7001/metrics` while paused (and Grafana when composed).
6. **Bounded memory / backpressure** — stop ALL consumers, keep producing:
   queues fill, then `relayforge_overloaded_total` increases and producers
   get `RESOURCE_EXHAUSTED` — memory stays flat (that is the point).
7. **Restart a consumer** — `relayctl consume ...` again: it rejoins, gets
   its assignments, and resumes from committed offsets.
8. **Catch-up** — watch lag gauges fall to 0.
9. **Crash the broker** — `kill -9 <rfbroker-pid>` mid-load.
10. **Recovery** — restart with the same `-data-dir`: the log recovers to
    the longest valid prefix (`relayforge_recovery_truncated_bytes` shows
    what a crash tore off), acked records are all present.
11. **Benchmarks** — `rfbench -mode produce -ack written -records 20000`
    and `-ack flushed`: the fsync cost is the latency difference.
12. **p95/p99** — in the rfbench summary and the
    `relayforge_append_latency_seconds` histogram.

## Replication (step 13 — fallback boundary, two brokers)

```sh
/tmp/rf-demo/rfbroker -addr 127.0.0.1:7100 -admin-addr 127.0.0.1:7101 -data-dir ~/rf-demo-leader
/tmp/rf-demo/rfbroker -addr 127.0.0.1:7200 -admin-addr 127.0.0.1:7201 -data-dir ~/rf-demo-follower -replica-of 127.0.0.1:7100
relayctl topic create -addr 127.0.0.1:7100 -name rep -partitions 1
relayctl topic create -addr 127.0.0.1:7200 -name rep -partitions 1
relayctl produce -addr 127.0.0.1:7100 -topic rep -m "r1"   # leader accepts
relayctl produce -addr 127.0.0.1:7200 -topic rep -m "r2"   # follower REJECTS (UNAVAILABLE: not the leader)
relayctl fetch   -addr 127.0.0.1:7200 -topic rep           # the copy is readable on the follower
# Manual failover:
kill -TERM <leader-pid>                                     # operator stops the old leader FIRST
relayctl fetch -addr 127.0.0.1:7200 -topic rep              # unreplicated tail may be absent (async; report it)
/tmp/rf-demo/rfbroker -addr 127.0.0.1:7200 ... -data-dir ~/rf-demo-follower   # restart WITHOUT -replica-of: promoted, epoch bumped
relayctl produce -addr 127.0.0.1:7200 -topic rep -m "r3"   # promoted leader accepts
# Restart the old leader WITH -replica-of 127.0.0.1:7200: it rejoins as follower.
```

Honesty note for the audience: this is async, manually-failed-over
replication at the documented fallback boundary — not consensus, not HA.
Split brain is prevented by procedure (stop the old leader first), not by
machinery. docs/REPLICATION.md and docs/LIMITATIONS.md say exactly what
the system does and does not guarantee.
