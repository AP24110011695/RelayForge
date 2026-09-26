# Storage

How RelayForge stores events on disk. `internal/logengine` implements this;
`internal/record` defines the record format ([RECORD_FORMAT.md](RECORD_FORMAT.md)).

## On-disk layout

A log is a **directory** of segments:

```text
data/
  00000000000000000000.log     # segment 0: raw records (record format v1)
  00000000000000000000.index   # segment 0: sparse index (derived)
  00000000000000000042.log     # segment 42
  00000000000000000042.index
  ...
```

- Segment names are the 20-digit zero-padded **base offset** (the offset of
  the segment's first record) plus `.log` / `.index`. Bases sort
  lexicographically and strictly increase.
- All segments except the last are **sealed** and immutable. The last is the
  **active** segment; appends go only there.
- The active segment **rolls** (seals; a new segment opens at the next
  offset) when the next record would push it past `MaxSegmentBytes`. A
  record is never split across segments; rolling never produces an empty
  segment.
- Index entry: 16 bytes — uint64 offset + uint64 position, little-endian.

## Sparse index

- One entry is written roughly every `IndexIntervalBytes` of the active
  segment (default 4096); a segment's first record is always indexed
  (entry `(base, 0)`).
- Lookup: binary search for the greatest entry with `offset <= target`,
  then a forward scan decoding records from that position. Record
  validation (length framing + CRC) is always applied — the index only
  speeds up seeking and is **never trusted for correctness**.
- **Derived state principle**: the `.log` files are the source of truth. A
  missing, truncated, or corrupt index is detected at Open and rebuilt by
  re-scanning the segment's log. Index write errors during appends are
  ignored on purpose (an incomplete index heals at the next Open).
- Sealed indexes are held in memory for lookups (bounded by segment size /
  interval; ~16 KiB per 4 MiB segment at the default interval).

## Recovery (Open)

1. Discover segment bases from the directory; remove stale `*.tmp` files.
2. **Sealed segments**: cheap consistency checks on the index (parseable,
   anchored at `(base, 0)`, monotonic offsets/positions, positions within
   the log size). An invalid index is rebuilt by fully re-scanning that
   log; if that scan finds the log itself undecodable, Open fails. Sealed
   logs whose indexes look consistent are NOT scanned at Open — corruption
   there surfaces through reads and `Scan`, and is never auto-truncated
   (see below).
3. **Active segment**: full validating scan from its base. Everything from
   the first invalid record onward (torn tail from a crash mid-append, or
   corruption) is truncated; the discarded byte count is reported by
   `TruncatedOnRecover()`. The active index is rewritten from the scan when
   it differs.
4. Offset continuity across segments is verified by `Scan` (the
   deep-verify utility): a hole in the segment sequence is an
   `ErrOffsetHole` error.

Policies and rationale:

- *Active-segment corruption* truncates from the first invalid record: the
  only safe recovery after a crash is the longest fully valid prefix, and
  records after a corrupt record cannot be trusted.
- *Sealed-segment log corruption* is never auto-truncated: sealed history
  is immutable, and silently deleting it would be undeclared data loss.
  Detection happens on read or via `Scan` (Open does not scan sealed logs,
  so corruption does not block startup when the index is consistent; when
  an index IS invalid and its rebuild scan finds the log undecodable, Open
  fails with an error naming the segment).
- *Transient I/O errors during the recovery scan* fail Open without
  modifying the file — they are not corruption.

## Durability ladder

| Append mode | Promise when Append returns | Survives process crash | Survives power loss |
| --- | --- | --- | --- |
| `DurabilityFlushed` | fsync of the active segment completed | yes | intended, subject to documented assumptions |
| `DurabilityWritten` | bytes reached the page cache | yes | no |
| `DurabilityNone` | none (bytes are still written; see below) | no promise | no promise |

- `DurabilityFlushed` calls `file.Sync()` (fsync, not fdatasync — chosen
  for simplicity and stronger guarantees; fdatasync is a possible Phase 9
  optimization) **before** Append returns. The ordering is proven by the
  seam test (`TestFsyncBeforeReturnSeam`) and by the crash harness, where
  the child prints its ack only after Append returns.
- **Sync-error policy**: a failed fsync returns the error, consumes the
  offset (the bytes are in the file), and sets the degraded state
  (`Degraded()`); the caller must not treat the record as flushed. While
  degraded, flushed appends keep retrying the fsync and keep failing until
  it succeeds; written-mode appends are unaffected.
- **Partial writes**: a short `WriteAt` (or a short write with nil error,
  which violates the io.Writer contract) fails the append, consumes no
  offset, and truncates the file back to the pre-append position.
- **Directory fsync**: creating a segment or atomically replacing an index
  (temp + rename) is followed by an fsync of the directory, so the
  namespace entry survives crashes — otherwise a flushed record could land
  in a file whose directory entry is lost.
- **Group commit** (batching fsyncs across appends) is deliberately not
  implemented: correctness first; it is a Phase 9 optimization candidate
  and must preserve the ack-after-durable invariant.

**Honest testing boundary**: SIGKILL crash tests prove durability against
*process* death (the page cache survives). Power-loss durability of
`DurabilityFlushed` is proven by construction (fsync before return) plus
the documented filesystem assumptions — it cannot be exercised in CI.
Assumptions: ext4 (or similar) with working fsync semantics; no
`O_DIRECT`, no exotic syscalls.

## Retention

- Policies: `MaxAge` (per-segment log-file mtime) and `MaxBytes` (total
  across segments), applied at segment granularity, oldest sealed first.
- The active segment is never deleted; `MaxBytes` therefore floors at
  `MaxSegmentBytes`.
- `ApplyRetention(now)` is synchronous and bounded (drops at most all
  sealed segments per call); `StartRetention(ctx, interval)` runs it on a
  ticker until the context is cancelled. The engine has no logger, so
  ticker errors are dropped — callers needing visibility call
  `ApplyRetention` directly.
- Deleted ranges read as `ErrOffsetOutOfRange`; retained ranges read
  normally. Retention never changes `NextOffset`.

## Concurrency

One RWMutex. Appends hold the write lock through their write (and fsync),
so readers — who take the read lock and use position-based `ReadAt` — only
ever see fully written records; a torn record is unobservable. Retention
also takes the write lock. All file access is position-based; the file
offset is never used.

## Configuration knobs

| Knob | Default | Meaning |
| --- | --- | --- |
| `MaxRecordSize` | 4 MiB | full encoded size cap per record |
| `MaxSegmentBytes` | 64 MiB | active-segment roll threshold (must be ≥ MaxRecordSize) |
| `IndexIntervalBytes` | 4096 | sparse-index cadence |
| `Retention.MaxAge` | off | drop sealed segments older than this |
| `Retention.MaxBytes` | off | keep total segment bytes at or below this |

## Known limitations

- No file locking: opening the same log directory twice concurrently is
  unsupported and undefined (hardening candidate for Phase 8).
- In-memory index entries per sealed segment trade memory for lookup
  speed; very large segment counts raise memory use (bounded and
  documented; revisit with Phase 9 measurements).
- Phase 3's single-file log format is superseded by the segment-set layout;
  no migration is provided (no data existed in the wild).
- Manual segment deletion (outside retention) can create holes; Scan
  detects them. Automated tooling never creates them.
