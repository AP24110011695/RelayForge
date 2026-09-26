# Failure Matrix

Every important failure: how it is injected, the expected behavior, the
invariant, the possible loss/duplication, the recovery, and the automated
verification. Rows marked `DEFERRED(P10)` are activated with replication.

| # | Failure | Injection | Expected behavior | Invariant | Possible loss/duplication | Recovery | Automated verification |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Crash during append | SIGKILL of a child appender at varied points; per-byte truncation of the active segment | torn tail truncated | longest valid prefix; offsets never reused | unacked records only (ack=none/written); acked flushed records kept | automatic at Open | `TestCrashHarness/*`, `TestRecoveryTruncationMatrix`, `TestTornTailRecovery` |
| 2 | Crash before acknowledgement | harness kill after appends but before acks reach the parent | record may exist without ack | same as #1 | producer retry duplicates it (at-least-once, documented) | automatic | `TestCrashHarness/*` |
| 3 | Crash after acknowledgement | harness kill after acks observed | acked records present after restart | flushed/written acks survive process death | none for the acked set | automatic | `TestCrashHarness/after-*-acks-*` |
| 4 | Truncated record | truncate active segment at every byte | decode fails cleanly; prefix preserved | no torn record ever served | the partial record only | automatic truncation | `TestRecoveryTruncationMatrix`, record `TestDecodeTruncationMatrix` |
| 5 | Corrupted record | byte flips across CRC-protected region | checksum mismatch; never served | corrupt data never returned | corrupt record + everything after it (active segment) | truncate-from-first-bad | record `TestCRCMatrix`, `TestCorruptTailRecovery`, `TestCorruptMiddlePolicy` |
| 6 | Corrupted index | flip/truncate/delete `.index` files | index rebuilt from log | log is source of truth; reads stay correct | none (index is derived) | automatic rebuild | `TestIndexCorruptionRecovery`, `TestActiveIndexRewriteOnOpen` |
| 7 | Metadata corruption | flip bytes in `metadata.json` (CRC stale) | broker refuses to start | never guesses topics | none (startup refusal) | manual inspection required by policy | metadata `TestCorruptSnapshotRefusesOpen`, broker `TestCorruptMetadataRefusesBrokerStart` |
| 8 | Disk full | fault wrapper: partial write + ENOSPC | append error; offset not consumed; partial bytes truncated | log stays a valid prefix | none for acked records | retry after space returns | `TestAppendENOSPC` |
| 9 | Slow disk | fault wrapper: sync latency injection | flushed appends pay the latency before ack; written do not | fsync-before-ack ordering | none | n/a (behavior, not failure) | `TestFsyncBeforeReturnSeam` |
| 10 | Overloaded producers | stalled writer + full bounded queue + deadline | explicit `ErrOverloaded` (RESOURCE_EXHAUSTED, retryable) | memory bounded by queue depth × max record | nothing (request rejected) | queue drains; producers retry | `TestBackpressure` |
| 11 | Slow consumer | fetcher draining 1 byte at a time | producers unaffected | consumer cannot stall the produce path | none | consumer catches up | `TestSlowConsumerIsolation` |
| 12 | Consumer crash | consumer stops heartbeating / process ends before commit | session expiry; partitions reassigned; uncommitted range redelivered | at-least-once; committed ranges are never re-committed blindly | duplicates after crash-before-commit (by design) | rejoin + resume from committed offset | `TestRedeliveryAfterCrashBeforeCommit`, `TestHeartbeatTimeoutRebalance` |
| 13 | Malformed RPC | adversarial field values (offsets, sizes, counts, hostile length fields) | explicit validation errors; no allocation bombs | bounded memory; connection stays usable | none | n/a | status-mapping table test, `FuzzReadFrame`, `FuzzDecode`, `FuzzCommitPayload`, engine limit tests |
| 14 | Network delay | long-poll windows; deadline propagation | RPC honors client deadlines; fetch waits only up to max_wait | no phantom acks | none | n/a | `TestLongPoll`, `TestGRPCLongPollDeadline`, deadline tests |
| 15 | Network disconnect | client context cancellation; conn close mid-RPC | in-flight produce: appended-or-error, never a phantom ack; server stays up | at-least-once on retry | possible duplicate on retry (by design) | client reconnects | `TestGracefulShutdownDrainsInFlight`, deadline/cancel handling in Produce |
| 16 | Follower lag | `DEFERRED(P10)` | — | — | — | — | Phase 10 |
| 17 | Leader crash | `DEFERRED(P10)` | — | — | — | — | Phase 10 |
| 18 | Zombie leader | `DEFERRED(P10)` | — | — | — | — | Phase 10 |

**Documented assumptions (not automatable in CI):** power-loss-class
durability of `ack=flushed` rests on ext4 fsync semantics and directory
fsyncs — proven by construction (fsync ordering tests) and stated in
[GUARANTEES.md](GUARANTEES.md), never claimed as tested.
