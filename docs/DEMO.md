# The 4-step RelayForge demo

Show what the product does — no architecture slides. Every step uses the
built binaries and proves one plain-language promise. (Deep technical
narration lives in the other `docs/` files; bring it up only if asked.)

Setup (once):

```sh
go build -o /tmp/rf-demo/rfbroker ./cmd/rfbroker
go build -o /tmp/rf-demo/relayctl ./cmd/relayctl
/tmp/rf-demo/rfbroker                       # terminal 1 — Overview at http://localhost:7001/
```

## Step 1 — Send events

```sh
/tmp/rf-demo/relayctl produce -m "order 42 placed"
/tmp/rf-demo/relayctl produce -m "file uploaded"
```

**Promise shown:** every event gets an immediate confirmation —
`✓ stored as event #0` — meaning it is safely on disk before the
command returns. The Overview page now lists the stream and its count.

## Step 2 — Start receivers

```sh
/tmp/rf-demo/relayctl consume -group workers -topic default   # terminal 2
/tmp/rf-demo/relayctl consume -group audit   -topic default   # terminal 3
```

**Promise shown:** each receiver collects every event **in order**, prints
`✓ caught up — listening for new events…`, and remembers where it stopped.
Two independent groups each get their own complete copy of the stream.

## Step 3 — Pause one receiver and watch the Overview

Keep Step 1 producing; pause terminal 2 (Ctrl-Z, or just kill it).

**Promise shown:** nothing breaks and nothing is dropped. The Overview page
shows `workers` as **"N events behind"** while `audit` stays **"✓ caught
up"** — per-receiver progress is tracked, not guessed.

## Step 4 — Resume, crash, and prove recovery

```sh
fg     # resume the paused receiver — it catches up and says so
kill -9 <rfbroker-pid>            # crash the broker mid-load
/tmp/rf-demo/rfbroker             # restart with the same data dir
```

**Promise shown:** the restarted broker recovers to the last valid state
(`relayforge_recovery_truncated_bytes` on `/metrics` shows exactly what a
crash tore off), every acknowledged event is still there, and receivers
pick up where they stopped.

## Optional step — replication (Phase 10, documented fallback boundary)

Two brokers: a leader and an async follower; kill the leader, restart the
follower without its role, and it becomes the leader. Run the commands
verbatim from [REPLICATION.md](REPLICATION.md) §"Manual failover procedure"
and read its honesty note first: this is manually-failed-over async
replication — not consensus, not HA.

## Where the engineering lives (only if asked)

[STORAGE.md](STORAGE.md) (segments, recovery) · [PROTOCOL.md](PROTOCOL.md)
(gRPC API) · [ARCHITECTURE.md](ARCHITECTURE.md) (concurrency model) ·
[FAILURE_MATRIX.md](FAILURE_MATRIX.md) (what fails and how it's tested) ·
[BENCHMARKS.md](BENCHMARKS.md) (measured numbers) ·
[OBSERVABILITY.md](OBSERVABILITY.md) (metrics) ·
[REPLICATION.md](REPLICATION.md) / [LIMITATIONS.md](LIMITATIONS.md).
