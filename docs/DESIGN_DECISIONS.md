# Design Decisions

Architectural decision records for RelayForge. Format per entry: context,
decision, consequences. New ADRs are appended; superseded ones stay.

---

## ADR-1: Why Go, and why an append-only log

Date: 2026-09-26 · Status: accepted · Decided during: Phase 1

**Context.** RelayForge is a learning-first systems project whose deliverable
is precisely stated, test-backed guarantees. It needs a language whose
concurrency is worth studying, honest access to file I/O, single-binary
deployment, and a stack new to the portfolio (existing projects are
Python-heavy application/ML work).

**Decision.**

1. **Go** as the implementation language.
2. **Append-only log** as the storage engine's core: length-framed,
   CRC-checked records (Phase 3), segmented with sparse indexes (Phase 5) —
   not a database engine, not an LSM tree, not B-trees.

**Why Go — evidence from the Phase 1 labs, not vibes:**

- The bounded-queue lab made backpressure concrete: a buffered channel's
  capacity *is* a bounded queue, and `select` on `ctx.Done()` *is* a
  cancellation policy. The broker's core mechanisms (inflight queues,
  graceful drain, per-partition serialization) map directly onto language
  primitives with no framework in between.
- The race detector turned "sometimes wrong" into a reproducible report
  (Lab A: naive counter finished at 46,006 instead of 80,000, with the two
  conflicting goroutines named). For a project whose deliverable is
  test-backed guarantees, that tool is load-bearing.
- Measured costs are reachable from day one: three counter fixes benchmarked
  at ~3.3 ns (racy), ~10 ns (atomic), ~44 ns (mutex) — performance reasoning
  starts with `go test -bench`, not folklore.
- Single static binary; a stdlib (`net`, `os`, `testing`, `pprof`, `context`)
  rich enough that the hard parts remain ours to build.

**Why an append-only log:**

- Writes become sequential appends — the cheapest durable I/O pattern disks
  offer, and the reason log-structured systems get O(1)-amortized writes.
- Immutability makes concurrency tractable: sealed segments never change, so
  readers need no locks. The Phase 1 labs showed where races live (shared
  mutable state) — immutable state has none.
- Offsets are logical, position-independent identifiers, so recovery becomes
  "truncate to the longest valid CRC-checked prefix" — a guarantee you can
  actually test (Phase 4).
- Deletion becomes whole-segment retention instead of in-place rewriting.

**Consequences.**

- Reads are O(log n) via a sparse index plus forward scan; random in-place
  writes are unsupported by design.
- The record format must carry a version field from day one (Phase 3).
- Dependency minimalism is the default: all Phase 1 labs ran on the stdlib
  alone, and any future dependency needs a written reason (working agreement
  in PHASE-1-PLAN).
