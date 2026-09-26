# Replication (Phase 10 — validated FALLBACK boundary)

**What was built and verified is the plan's fallback boundary, stated
plainly.** Per PLAN.md Phase 10, the phase must land either the full model
or its validated fallback and say which one it is. This implementation is
the **fallback boundary**:

- single-follower **async** replication with **static placement**;
- manual promotion by restarting the follower without the follower role
  (a persisted per-partition **leader epoch** is bumped on every standalone
  start, asserting the leadership claim);
- `acks=all` is **explicitly unsupported** while replication is enabled:
  produces are `ack=none`/`written`/`flushed` against the leader only;
- a running **zombie leader is NOT automatically fenced** — the operator
  stops the old leader BEFORE promoting the follower (see DEMO.md); a
  broker restarted in follower mode rejects produces by role.

It is **not**: consensus, Raft, automatic election, high availability,
exactly-once, or a replication protocol with leader-derived read cutoffs.
Any documentation or demo must not present it otherwise.

## How it works

- A follower is started with `rfbroker -replica-of <leader:port>`.
- One goroutine per topic/partition pulls records from the leader via the
  internal `ReplicateFetch` RPC and appends them to the local log with
  `ack=written` durability. Pulls are asynchronous; the tail may lag.
- A supervisor discovers new topics/partitions every 2 s (topics are
  created on both brokers by the operator — static placement).
- The follower **rejects produces** (`ErrNotLeader` → `UNAVAILABLE`) and
  never bumps epochs (only standalone starts do).
- `relayforge_replication_lag{topic,partition}` reports the follower's
  distance from the leader's log end; `relayforge_replication_errors_total`
  counts fetch failures.

## Manual failover procedure (the only supported one)

1. Stop the old leader (`SIGTERM` to rfbroker). Unreplicated tail is lost —
   with async replication that is expected and must be reported.
2. Restart the follower **without** `-replica-of`. It bumps every
   partition's epoch and accepts produces (it is the leader now).
3. The old leader may be restarted **with** `-replica-of` pointing at the
   new leader — it then runs as the follower and rejects produces by role.
4. Clients are re-pointed at the new leader manually (no transparent
   redirection).

Guarantee statement: with the old leader stopped first, no acked record is
lost (`flushed`/`written` acks survived the restart — the crash-harness
property applies to the promoted follower's log). If the old leader is NOT
stopped first, it keeps accepting produces into its own log — split brain;
that is the documented limit of this boundary, not a hidden one.

## What Phase 10's full boundary would add (not built)

ISR tracking with shrink/grow, a high-watermark gating reads and
`acks=all`, epoch-based truncation on promotion, and automatic zombie
fencing. If this project continues, those are the next steps — until then
this is the fallback boundary, per PLAN.md.

## Verified by

- `TestFollowerReplicatesAndRejectsProduces` — 50 records, flushed acks on
  the leader, async catch-up verified record-for-record; produce rejected.
- `TestPromotionBumpsEpochs` — standalone starts bump the persisted epoch
  (1 → 2) and the promoted broker accepts produces.
- `TestFollowerDoesNotBumpEpochs` — the follower role never claims
  leadership.
