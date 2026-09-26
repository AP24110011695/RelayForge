# Guarantees

The contract RelayForge makes to its users, and the state of its proof.
Every guarantee here is tested — anything not yet tested is listed as a
target, never as a property. Master definitions: [PLAN.md §2](../PLAN.md).

## Durability (producer acknowledgement modes)

| Mode | Acknowledged after | Survives process crash | Survives power loss |
| --- | --- | --- | --- |
| `ack=flushed` | fsync completed | yes | intended, subject to assumptions below |
| `ack=written` | bytes reached the page cache | yes | no |
| `ack=none` | nothing | no | no |

Iron rule: a record is never acknowledged before it has reached the
durability state promised by its mode.

**Proof status (Phase 4):**

- fsync-before-return for `flushed` is proven by the seam test
  (`TestFsyncBeforeReturnSeam`: injected fsync latency must delay flushed
  appends and must not delay written ones) and end-to-end in the SIGKILL
  crash harness, where the child acks only after Append returns.
- Process-crash survival is proven by the crash harness: after SIGKILL at
  many points, every record whose ack reached the parent is present after
  restart, and the log is always a valid prefix.
- **Assumptions for power-loss class failures**: the filesystem honors
  fsync (ext4 on WSL2 ext4 in the reference environment), directory entries
  are fsynced when segments/indexes are created, and the storage does not
  lie about persistence. These cannot be exercised in CI; power-loss
  testing is out of scope and never claimed as tested.

## Recovery

- After any crash, each log recovers to the **longest valid prefix** of its
  records; torn or corrupt tails are truncated; corrupt records are never
  served as valid records.
- Offsets are never reused: appends after recovery continue the sequence.
- Proof: the per-byte truncation matrix (every truncation point of the
  active segment), corrupt-tail / corrupt-middle injections, garbage
  segments, index corruption/deletion, and the SIGKILL crash harness.

## Deliverability of other guarantees

| Guarantee | Status |
| --- | --- |
| At-least-once delivery | designed; broker-level proof lands in Phase 7 |
| Per-partition ordering | lands in Phase 6 |
| Backpressure / bounded memory | lands in Phase 6 |
| Consumer groups + fencing | lands in Phase 7 |
| Replication (limited, manual failover) | lands in Phase 10 — explicitly not consensus, not HA |

## Where the details live

- Storage mechanics and recovery algorithm: [STORAGE.md](STORAGE.md)
- Record format: [RECORD_FORMAT.md](RECORD_FORMAT.md)
- Failure scenarios and their automated verification: [FAILURE_MATRIX.md](FAILURE_MATRIX.md) (Phase 8)
