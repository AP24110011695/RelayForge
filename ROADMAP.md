# RelayForge — 15-Phase Implementation & Learning Roadmap

**Version:** 1.0 · **Date:** 2026-09-26 · **Status:** Planning approved for execution — no code written yet, by design.

**How to use this document.** Work strictly phase by phase. Do not start a phase until the previous phase's Definition of Done is met *and* you can answer its Learning Checkpoint out loud, without notes. Every phase has a Theory → Exercise → Project Work → Testing → Interview Check loop. Exercises live in a separate sibling directory (`C:\Projects\rf-labs\`), never in the portfolio repo — the RelayForge repo stays professional and clean; the labs stay as your personal study notes.

---

# Part A — Architecture Overview

## A.1 What RelayForge is

RelayForge is a single-binary Go event broker with a gRPC API, a CLI (`relayctl`), and an append-only, segment-based, checksummed log storage engine. Clients produce records to topics (routed to partitions by key hash), and consumers fetch records by offset and commit offsets durably, organized into consumer groups with heartbeats and rebalancing. The final phase adds static leader/follower replication per partition with a high-watermark acknowledgement policy and **manual** failover. It is deliberately *not* Kafka: no Kafka protocol, no transactions, no automatic leader election, no schema registry, no stream processing.

## A.2 Layered architecture

```
┌──────────────────────────────────────────────────────────┐
│  relayctl CLI          loadgen / bench harness           │
├──────────────────────────────────────────────────────────┤
│  gRPC API (Protobuf)  + health + interceptors            │
├──────────────────────────────────────────────────────────┤
│  Broker            ← backpressure policy, timeouts       │
├──────────────────────────────┬───────────────────────────┤
│  Topic / Partition Manager   │  Group Coordinator        │
│  (key → partition routing)   │  (join/sync/heartbeat)    │
├──────────────────────────────┤  Offset Manager           │
│  Partition Log (per partition)│ (durable commits, fencing)│
├──────────────────────────────┴───────────────────────────┤
│  Log Engine: segments, sparse index, retention, recovery │
├──────────────────────────────────────────────────────────┤
│  Record Format: length-framed, CRC32C, offsets           │
├──────────────────────────────────────────────────────────┤
│  Filesystem (write → page cache → fsync)                 │
└──────────────────────────────────────────────────────────┘

Surrounding subsystems (built progressively, never "later all at once"):
  Observability (slog from P6, Prometheus/Grafana/pprof in P12)
  Testing (unit from P3, race from P8, integration from P6, fuzz/fault-injection in P13)
  Benchmarking (P14)   Replication (P15 only, after single-node is proven)
```

**Produce path:** `client → gRPC Produce → topic lookup → key hash → partition → bounded inflight queue → append (per-partition lock) → optional fsync (ack mode) → ack with assigned offset`.
**Consume path:** `client → gRPC Fetch(topic, partition, offset, maxBytes, maxWait) → sparse-index binary search → pread segment files → return records + log-end-offset`. Fetches read immutable sealed segment files and the active segment; they do **not** contend with the append lock (this is a deliberate design outcome of Phase 8).
**Group path:** `JoinGroup → SyncGroup → Heartbeat loop → (on timeout/leave) rebalance → fenced OffsetCommit`.

## A.3 Guarantee cards (the correctness contract)

These are the project's spine. Each phase that implements part of a card writes it into `docs/GUARANTEES.md` — what it is, why it exists, where it is implemented, how it can fail, how it is tested, and what it does **not** guarantee.

1. **Ordering.** Records in one partition are served in append order. There is **no** cross-partition ordering and **no** global topic order. With multiple producers sending the same key, the relative order of *different producers'* records is unspecified; a single producer per key gets its own order preserved.
2. **Durability (per ack mode).** `ack=flushed`: record is fsynced before the ack — survives process crash *and* power loss. `ack=written`: record is written to the OS (page cache) before ack — survives process crash, not guaranteed to survive power loss. `ack=none`: record is accepted into the broker's queue — may be lost on crash. Default: `ack=written`. Rejected records are never acked and never appended.
3. **Delivery.** At-least-once. Consumers commit **after** processing; a crash between processing and committing causes redelivery (duplicates). Exactly-once is **not** provided; making processing idempotent is the consumer's job.
4. **Recovery.** After any crash, each partition's log is recovered to the longest prefix of CRC-valid, length-consistent records; any torn or corrupt tail is truncated. Bytes after the truncation point were never acked (under `ack≥written`), so **no acked record is lost**.
5. **Backpressure.** Every buffer between client and disk is bounded. When a partition's inflight queue is full, Produce waits up to a configurable timeout, then fails with an explicit `OVERLOADED` error. The broker never drops an acked record, never buffers unboundedly, and stays responsive under sustained overload.
6. **Consumer groups.** Within one generation, every partition of a subscribed topic is assigned to at most one member. Commits from a stale generation are rejected (fencing). On member death (heartbeat timeout), its partitions are reassigned after rebalance.
7. **Replication (Phase 15, advanced only).** With `acks=all`, a record is acked only when it is on all in-sync replicas (high-watermark). If at least one in-sync follower survives, acked records survive leader failure under **manual** failover. NOT claimed: consensus, automatic safe election, high availability, exactly-once.

## A.4 Evolving repository structure

The structure is grown with the architecture, not decided upfront. Each layer is built and tested as a library *before* the layer above it exists.

| Phase | Directories introduced | Why then |
|---|---|---|
| P1 | repo root: `README.md` (1-page stub), `Makefile`, `.gitignore`, `docs/DESIGN_DECISIONS.md` | tooling and the ADR habit before any code |
| P3 | `internal/logengine/` | record format + log as a pure, testable library with no network dependency |
| P6 | `api/proto/`, `internal/broker/`, `internal/metadata/`, `internal/observability/`, `cmd/relayserve/`, `cmd/relayctl/` | network/API layer arrives; process model (two binaries) becomes real |
| P7 | `internal/partition/` | partition routing is its own concept, separate from storage |
| P10 | `internal/offsets/` | durable offset store is distinct from group logic |
| P11 | `internal/group/` | coordination protocol isolated from broker data path |
| P13 | `internal/faulttest/`, `.github/workflows/` | fault-injection helpers and CI arrive with the hardening phase |
| P14 | `bench/`, `scripts/` | benchmark harness and reproducibility scripts |
| P15 | cluster config files under `configs/` | static multi-broker topology |

Documentation grows alongside code (not as a late phase): `docs/RECORD_FORMAT.md` (P3), `docs/GUARANTEES.md` (P4, living), `docs/ARCHITECTURE.md` + `docs/PROTOCOL.md` (P6, living), consumer-groups doc (P11), `docs/FAILURE_MATRIX.md` (P13), `docs/BENCHMARKS.md` (P14), `docs/LIMITATIONS.md` (P15). The README stays technically honest: what the system guarantees, what it does not, and measured numbers only.

## A.5 Development environment (important, Windows host)

Your host is Windows. **Use WSL2 (Ubuntu) as the primary dev/test environment from Phase 1.** Reasons: fault injection needs `kill -9` semantics (P13), disk-full simulation is easiest with small loopback/tmpfs volumes (P9, P13), and fsync behavior matches Linux where brokers actually run. Go is cross-platform, so the code stays portable, but the *tests that simulate failure* target Linux. Docker Desktop (which uses WSL2) runs the Prometheus/Grafana/broker compose stack from Phase 12. Keep the repo checked out inside the WSL2 filesystem for real file-I/O semantics.

---

# Part B — Dependency Graph

```
P1 Go foundations & toolchain
 └─► P2 TCP, framing, RPC concepts          (labs only)
      └─► P3 binary record format + append-only log
           └─► P4 durability, checksums, crash recovery
                └─► P5 segments, sparse index, retention
                     └─► P6 broker v1: gRPC + CLI  ──► [observability stub starts here]
                          └─► P7 topics, partitions, ordering
                               └─► P8 concurrency architecture
                                    └─► P9 backpressure, overload, cancellation
                                         └─► P10 consumers, offsets, at-least-once
                                              └─► P11 consumer groups, rebalance
                                                   └─► P12 observability: metrics, dashboards, pprof
                                                        └─► P13 hardening: fuzz, fault injection, CI
                                                             └─► P14 benchmarking & performance
                                                                  └─► P15 replication & controlled failover
```

Cross-cutting edges: P12 also depends on P6 (health/logging), P8 (queue depth) and P11 (consumer lag). P13 fuzzes decoders from P3, P5 and P10, and its crash matrix exercises P4. P14 depends on P12 (server-side metrics) and P13 (stability to measure). Labs (P2, P3, P4, P6) can run slightly ahead of the main line but never instead of it.

**Changes to your suggested progression (and why):**

1. **Split your Phase 1 into "Go + concurrency" (P1) and "networking" (P2).** Your original P1 mixed four large topics; networking in Go (`net`, partial reads, deadlines, framing) deserves its own phase and feeds directly into understanding why gRPC/Protobuf exist.
2. **Observability skeleton pulled forward to P6; full observability phase stays at P12.** From P8 onward you will be debugging concurrency and backpressure — doing that with no structured logs or counters is blind flying, and retrofitting logging into a working broker is miserable. P6 adds slog, health endpoints, and a swappable metrics stub; P12 replaces the stub with Prometheus and adds Grafana + pprof.
3. **Testing is made progressive instead of concentrated in P13.** Your own spec says "build testing progressively," which contradicts a test-only phase at 13. So: unit tests from P3, integration tests from P6, race tests from P8, property tests from P5. P13 becomes the *hardening* phase — native fuzzing, the crash/corruption matrix, network fault injection, CI — the systematic adversarial pass over everything already tested piecemeal.
4. **Consumer-side flow control moved from P9 to P10.** Backpressure against slow consumers requires consumers to exist. P9 is producer/ingest backpressure + cancellation + timeouts; P10 adds `maxBytes`/`maxWait` fetch flow control. The split is explicit so neither phase is half-understood.
5. **Docker Compose enters at P12, not at the end.** The observability stack (broker + Prometheus + Grafana) *is* a compose stack; the MVP demo compose is finalized in P14 with the load generator.

Everything else in your progression is kept as-is; it is technically sound (in particular, building the storage engine as a library before putting gRPC on top, and keeping replication after the single-node system is proven).

---

# Part C — The 15-Phase Plan

Conventions: exercises live in `C:\Projects\rf-labs\<lab-name>\`. "Race-clean" means `go test -race ./...` passes. Effort assumes ~15 h/week. Every phase ends with documentation updates if its concepts touched `docs/`.

## Phase 1 — Go Foundations & the Go Toolchain

### Objective
Become fluent enough in Go to build the Phase 3 storage library from scratch: modules, testing, goroutines, channels, mutexes, benchmarks, and the race detector.

### Why this phase exists
Go is the new language of your portfolio, and every later phase assumes it. Weak foundations here multiply into lost weeks later; one focused phase now is the cheapest insurance in the project.

### Concepts to study
**THEORY:** Go syntax, structs/slices/maps (slice internals: len/cap/aliasing), pointers vs values, interfaces and implicit satisfaction, error handling idioms, defer/panic/recover; goroutines vs OS threads (growable stacks, M:N scheduling), unbuffered vs buffered channels, `select`, closing channels, `sync.Mutex`/`RWMutex`, `sync.WaitGroup`, `sync/atomic`, `context.Context` basics; `go.mod`/workspaces; table-driven tests; `go test -race`; `go test -bench`; what the memory model's "happens-before" means (introduction level); `gofmt`, `go vet`.

### Prerequisites
Comfortable programming in Python/C-style languages. Nothing Go-specific.

### Small exercise
In `rf-labs/`: **lab1-bounded-queue** — bounded producer–consumer with multiple producers, a buffered channel, and graceful close without deadlocks. **lab1-counter** — the same counter three ways (naive → mutex → atomic), demonstrating a real data race and the race detector's report. **lab1-bench** — your first Go benchmark: compare `map+Mutex` vs `sync.Map` under read-heavy and write-heavy load. **lab1-race-demo** — a seeded race the detector catches; understand the output line by line.

### RelayForge implementation tasks
Install Go inside WSL2. Create the `relayforge` repo: `git init`, `.gitignore`, `Makefile` with `build / test / race / vet / fmt` targets, one-page honest `README.md`, `docs/DESIGN_DECISIONS.md` with your first ADR ("why Go, why append-only logs"). Nothing else.

### Tests
The labs' own tests, all race-clean.

### Expected output
Three passing labs; a repo skeleton with a working Makefile; you can write a table-driven test from memory.

### Definition of Done
All labs pass under `-race`; you can explain channel vs mutex choice, buffered vs unbuffered semantics, and what the race detector does and does not prove; Makefile targets work from a clean clone.

### Learning checkpoint
Explain: goroutine vs OS thread; "share memory by communicating" and when a mutex is actually the right tool; what `-race` detects (sampling at runtime, not proof); the WaitGroup drain pattern; why `context.Value` is not for optional parameters.

### Interview questions
Goroutine vs thread — cost model? When channel vs mutex? What does `go test -race` actually do? Slice aliasing bugs? Why can a nil-valued interface not equal a wrapped nil pointer? How does `defer` order interact with named returns?

### Estimated time
2–3 weeks.

### Risks
Tutorial rabbit-holing (cap at what P3 needs); staying on Windows-only habits — set up WSL2 now, fault injection later assumes it.

### What NOT to build yet
No networking, no file I/O, no RelayForge logic, no repo scaffolding beyond the root files above.

---

## Phase 2 — Networking in Go: TCP, Framing, RPC Concepts

### Objective
Understand raw TCP and wire framing by hand, so gRPC in Phase 6 is a power tool you understand rather than magic.

### Why this phase exists
A broker is a network server at heart. The classic failure modes (partial reads, message boundaries, slow clients, deadlines) must be learned on a 200-line server where you can see everything, before they hide inside gRPC.

### Concepts to study
**THEORY:** sockets: `net.Listen`/`Accept`/`Dial`; **`conn.Read` can return fewer bytes than requested — partial reads**; `io.ReadFull` vs plain `Read`; length-prefix framing and why it enables resynchronization; deadlines (`SetDeadline`/`SetReadDeadline`); goroutine-per-connection and its limits; half-close and graceful teardown; what a wire protocol is; RPC vs REST vs raw TCP; preview: gRPC = HTTP/2 + Protobuf + generated stubs — what problems each part solves.

### Prerequisites
Phase 1.

### Small exercise
**lab2-tcp-echo** — concurrent echo server with per-connection deadlines and a clean shutdown path. **lab2-framed-kv** — a length-prefixed binary SET/GET/DEL protocol over TCP with a client library; the killer test: a client that writes each request **one byte at a time** and then dies mid-message — the server must never desync and must time out cleanly.

### RelayForge implementation tasks
None in the main repo. Record lessons (framing, partial I/O, deadline strategy) in `docs/DESIGN_DECISIONS.md` — they will justify Phase 3's format and Phase 6's API.

### Tests
Partial-write tests (1-byte-chunk writes), concurrent-clients test, deadline test, abrupt client death test.

### Expected output
A working framed KV server/client in `rf-labs`; notes you can cite later.

### Definition of Done
All lab tests pass under `-race`; you can explain why TCP has no message boundaries, how length-prefix framing resyncs, and where the server could deadlock.

### Learning checkpoint
Explain: stream vs message; what happens on a partial read and partial write; why deadlines must wrap whole requests; the resource cost of goroutine-per-connection at 10k connections; what problem HTTP/2 multiplexing solves for RPC.

### Interview questions
How do you frame messages on a TCP stream? How do you handle a truncated frame? Why is blocking I/O in Go acceptable at scale where it isn't in Java-1.4-era models? What does gRPC add over raw TCP? How would you detect a dead peer?

### Estimated time
1 week.

### Risks
Over-engineering the mini-protocol (timebox: 2–3 days); skipping the partial-write tests — they are the entire point.

### What NOT to build yet
No gRPC, no Protobuf, no storage, no broker.

---

## Phase 3 — Binary Record Format + Append-Only Log (as a Library)

### Objective
Implement `internal/logengine`: the RelayForge record format and a single-file append-only log that assigns offsets at append time and supports read-by-offset. No network, no broker — a pure library.

### Why this phase exists
The record format is the system's DNA; every guarantee (ordering, durability, recovery) is expressed in it. Building it first as a standalone library makes it exhaustively testable and forces the right layering for everything above.

### Concepts to study
**THEORY:** `encoding/binary`, endianness (choose little-endian; justify: x86/ARM natives, industry trend); why the frame length comes first (scan/skip/arithmetic); CRC32C vs CRC32 vs Adler-32 (error-detection strength, hardware acceleration on modern CPUs); logical offsets vs byte positions; log-end-offset (LEO); buffered I/O pitfalls when durability matters; `io.ReaderAt` for random access; `O_APPEND` vs explicit seek+write under a lock.

**Record format (v1) — the spec you will implement and document:**

| File offset | Size | Field |
|---|---|---|
| 0 | 4 | frame length `L` (uint32 LE) = bytes from offset 8 to end of value |
| 4 | 4 | CRC32C (uint32 LE) over bytes `[8, 8+L)` |
| 8 | 1 | format version (0) |
| 9 | 1 | attributes (reserved, 0) |
| 10 | 8 | timestamp, unix milliseconds (int64 LE) |
| 18 | 4 | key length `K` (uint32 LE, 0 = no key) |
| 22 | K | key bytes |
| 22+K | 4 | value length `V` (uint32 LE) |
| 26+K | V | value bytes |

Records are packed back-to-back: the next record starts at `position + 8 + L`. Every multibyte integer is little-endian. The CRC covers everything after the CRC field itself, so any in-place corruption of version/timestamp/key/value/lengths is detectable.

### Prerequisites
Phase 2.

### Small exercise
**lab3-appendlog-kv** — append-only KV store: SET appends a framed record, startup replays the file into a map, GET reads from the map. No updates in place — feel why logs are the source of truth.

### RelayForge implementation tasks
`internal/logengine`: `Record` type; `Encode`/`Decode` per the spec above (decoder returns typed errors like `ErrCorrupt`, **never panics** — future fuzz target); `SingleFileLog`: `Append(key, value) → (offset, error)`, `ReadAt(offset)`, `ReadFrom(offset, maxBytes)`, `LogEndOffset()`. ADRs: endianness, CRC choice, length-first framing, why offsets are logical not byte-based. Write `docs/RECORD_FORMAT.md`.

### Tests
Unit: encode/decode roundtrip (empty key, empty value, large value, unicode); bit-flip in any field → CRC rejection with a clear error; offset monotonicity; `ReadFrom` from an interior offset; truncated final record → `ErrCorrupt` on decode. Property-style loops with seeded random payloads.

### Expected output
A race-clean library usable from a test binary; documented format.

### Definition of Done
`go test -race ./...` green; you can draw the record format field-by-field from memory and justify each field; `docs/RECORD_FORMAT.md` matches the code exactly.

### Learning checkpoint
Explain: why a CRC before use; why logical offsets (segment files will move/rename; byte positions wouldn't survive); why the length field precedes the CRC; what happens if two goroutines append without a lock (preview of P8).

### Interview questions
Design a binary record format for an append-only log — what fields and why? Why a per-record checksum? Why little-endian? How would you version this format for future evolution? How do you distinguish corruption from truncation?

### Estimated time
2 weeks.

### Risks
Premature optimization (batching/compression are explicitly forbidden here); sloppy partial-decode paths (a panic now is a fuzz failure in P13).

### What NOT to build yet
No segments, no index, no rotation, no retention, no fsync policy decisions (buffered writes + sync-on-close are fine for now), no network, no broker.

---

## Phase 4 — Durability, Checksums in Anger, Crash Recovery

### Objective
Define durability semantics precisely (ack modes), and implement startup recovery that always yields a valid log prefix.

### Why this phase exists
This is where "it works" becomes "it is correct under failure" — the heart of the project's identity. The recovery guarantee (card 4) is the first guarantee you will defend in an interview with evidence.

### Concepts to study
**THEORY:** the write path: `write(2)` → page cache → `fsync(2)`/`fdatasync(2)`; what page cache means for durability; process crash vs OS crash vs power loss vs disk failure; torn/partial writes (why a record can be half on disk); why the CRC enables truncate-on-recovery; fsync latency and why durability costs throughput; fsync-on-directory for file creation (needed in P10/P7 renames); the "valid prefix" definition: *after recovery, the log contains exactly the longest prefix of CRC-valid records; the truncation point is never inside an acked record under `ack≥written`*; ack modes:

| Ack mode | Meaning | Survives process crash | Survives power loss |
|---|---|---|---|
| `flushed` | fsync before ack | yes | yes |
| `written` (default) | write() to OS before ack | yes | not guaranteed |
| `none` | accepted into queue | not guaranteed | not guaranteed |

### Prerequisites
Phase 3.

### Small exercise
**lab4-crash-recovery** — a lab program that appends N records then `os.Exit(1)` (simulated crash), with and without `Sync`; inspect the raw file with `xxd`; verify which records survived and why. Repeat with random kill timings to *feel* the page cache.

### RelayForge implementation tasks
`logengine/recovery`: scan from the tail on startup; on the first torn/invalid frame, truncate the file to the last valid boundary and report records recovered/discarded; extend the log with ack modes wired into `Append`; a simple disk-full error path (append fails → LEO rolls back cleanly, no partial accepted record); metric hooks (stub): records_recovered, bytes_truncated; `docs/GUARANTEES.md` v1 with the durability and recovery cards.

### Tests
Unit: corrupt *last* record → earlier records intact, tail truncated; truncation mid-frame; garbage tail bytes; zero-length file; empty directory. Mini crash matrix: subprocess appends i records and hard-exits for i = 0..N, parent verifies the valid-prefix property every time (seeded, runs ~50 randomized cases).

### Expected output
Kill the process at any point → restart → log is always a valid prefix; the three ack modes demonstrably differ in the lab.

### Definition of Done
Recovery property tests green (including the subprocess matrix); you can state each ack mode's guarantee *and* its non-guarantee without notes; `docs/GUARANTEES.md` exists and is honest.

### Learning checkpoint
Explain: what fsync actually does and its latency profile; why `written` survives process crash but maybe not power loss; why torn writes happen (write size vs block size, drive reorder); precisely what truncation discards and why that is safe; why you must never ack before the durability you promise.

### Interview questions
Write vs fsync — what does each guarantee? What does the page cache change about durability claims? How do you recover a log with a torn record? Design ack modes for a broker. A client got an ack but the record is gone after a power cut — what ack mode and why?

### Estimated time
2 weeks.

### Risks
Windows fsync semantics differ from Linux — run fault tests under WSL2; scope creep toward group commit (note it as a P14 idea and stop); disk-full rollback is subtle — test it explicitly, don't hand-wave.

### What NOT to build yet
No multiple segments, no index, no retention, no RPC, no topics.

---

## Phase 5 — Segments, Sparse Indexes & Retention

### Objective
Turn the single file into segment files with sparse indexes: offset lookup via binary search, size/age rotation, and retention by deletion of whole segments.

### Why this phase exists
Segments + sparse indexes are what make an append-only log *practical*: bounded file sizes, cheap offset lookup, O(1)-ish deletion. This is also where you learn the power of **immutability** — sealed segments never change, which will make the Phase 8 concurrency design almost free.

### Concepts to study
**THEORY:** segment = file named by its base offset (`{baseOffset}.log` + `{baseOffset}.index`); sparse vs dense indexes (index size vs lookup cost); index entry: relative offset (uint32) + file position (uint64), 12 bytes; in-memory sorted index + binary search, falling back to a forward scan from the found position; mmap vs `pread` for index/log reads (choose plain `ReadAt` for MVP, note mmap as stretch); rotation by max segment bytes and max age; retention deleting whole segments (why partial deletion is wrong for an offset-addressed log); rebuilding indexes during recovery by scanning; index lag (index may be behind the log — handle it).

### Prerequisites
Phase 4.

### Small exercise
**lab5-index-lookup** — fixed-size sorted index: build from a record stream, then property-test binary-search lookup against linear scan over thousands of seeded cases.

### RelayForge implementation tasks
Segment set in `logengine`: active segment (appends) + sealed segments (immutable reads); `FindOffset(target) → (segment, filePos)` via binary search; `ReadFrom(offset, maxBytes)` now spans segments; rotation at a configurable size (default 128 MiB); retention by age and by total log bytes (delete oldest whole segments); recovery extended to rebuild missing/stale indexes; index write interval ≈ every 4 KiB of log. Document the storage design (`docs/STORAGE.md`).

### Tests
Unit: rotation at exact boundaries; binary search vs brute force (property); cross-segment `ReadFrom`; retention deletes only fully-eligible segments and never the active one; recovery rebuilds indexes; offset lookup on an empty log / before first / after last.

### Expected output
A complete storage engine: append, read from any offset, rotate, retain, recover — race-clean, still no network.

### Definition of Done
Property tests green; you can explain the sparse-index amortized cost and the lookup algorithm on a whiteboard; `docs/STORAGE.md` written.

### Learning checkpoint
Explain: why sparse (12-byte entries every 4 KiB vs per-record); how lookup resolves (base offset → relative → binary search → forward scan); why sealed segments are immutable and what that buys; why retention deletes whole segments only.

### Interview questions
How does Kafka's sparse index work — compare to yours? Dense vs sparse tradeoffs? Why segment files instead of one big file? What bounds index memory? What happens if a crash lands between log append and index flush?

### Estimated time
2 weeks.

### Risks
Index/log inconsistency edge cases (index lags → always keep the forward-scan fallback); rotation while readers hold file handles (immutability saves you — lean on it).

### What NOT to build yet
No RPC, no broker, no topics, no compression, no change to fsync policy.

---

## Phase 6 — Broker v1: gRPC/Protobuf, CLI, First End-to-End

### Objective
Put a gRPC API on the storage engine: a single-binary broker, a `relayctl` CLI, topics with exactly **one** partition, health/readiness, structured logging, a metrics stub, and graceful shutdown. First end-to-end demo.

### Why this phase exists
The system becomes real: an API, a process, a CLI. Also the first integration tests (real broker, real restart) and the observability skeleton you'll need from Phase 8 onward.

### Concepts to study
**THEORY:** Protobuf: schema design, field numbers and backward compatibility, scalar types vs `bytes`, why timestamps are server-assigned; gRPC: server lifecycle, status codes and error mapping, interceptors, deadline propagation from client to server; unary RPC first (why streaming is a stretch, not a start); config loading and precedence (flags > env > file); structured logging with `slog` (JSON handler, request IDs via interceptor); graceful shutdown: SIGTERM → cancel context → drain in-flight RPCs → close storage; gRPC health protocol; the metrics stub pattern (interface now, Prometheus in P12).

**RPC surface (v1):** `CreateTopic(name)`, `ListTopics`, `Produce(topic, key, value, ackMode) → (partition, offset)`, `Fetch(topic, partition, offset, maxBytes) → (records, logEndOffset)`, plus health. Topics are created with partitions=1 in this phase; the `partition` field exists in the schema so the API doesn't break in Phase 7.

### Prerequisites
Phase 5.

### Small exercise
**lab6-grpc-greeter** — tiny gRPC service: unary call, deadline exceeded, cancellation, custom status codes. Half a day, just to know the tool.

### RelayForge implementation tasks
`api/proto/relayforge/v1/relay.proto` + generated stubs; `internal/broker` wiring CreateTopic → metadata (`docs` say: atomic file write — temp + fsync + rename + dir fsync), Produce → partition append with ack mode, Fetch → `ReadFrom`; `internal/metadata` (topics file); `internal/observability` (slog setup, metrics stub, health); `cmd/relayserve`, `cmd/relayctl` (`create-topic`, `produce`, `fetch`); config; interceptors for logging/recovery/ deadlines. Docs: `docs/ARCHITECTURE.md` v1 and `docs/PROTOCOL.md` v1. Directory structure appears as per Part A.4.

### Tests
First integration tests (in-process broker over a real gRPC listener + real filesystem): produce 1,000 records → fetch them all back; kill and restart broker → fetch again (P4 recovery through the RPC path); bad topic → `NOT_FOUND`; offset past LEO → `OUT_OF_RANGE`; client deadline on a slow fetch → `DEADLINE_EXCEEDED`; graceful shutdown under an in-flight request → no error surfaced to that client.

### Expected output
`relayctl produce --topic t --key k --value v` against a running broker, and reading it back. The project's first demo.

### Definition of Done
Integration tests incl. restart/recovery green; proto evolution policy documented (add fields, never reuse numbers); README quickstart honest and minimal; metrics stub counts produce/fetch/errors.

### Learning checkpoint
Explain: why Protobuf over JSON here; field-number discipline; why unary-first; how a client deadline travels to the server and what the server should do with it; how to shut down without dropping in-flight RPCs.

### Interview questions
Protobuf vs JSON for an internal protocol? What does an interceptor do — name three uses? Design an RPC for "append with durability choice" — what goes in the request vs response? Why cap `maxBytes` server-side? Walk through a clean shutdown sequence.

### Estimated time
2.5 weeks.

### Risks
Proto churn (write `PROTOCOL.md` before generating anything); the partitions=1 limitation confusing future-you — document it loudly in the code and docs.

### What NOT to build yet
Multi-partition routing (P7), consumer offsets/commits (P10), groups (P11), queues/backpressure (P8–P9), replication (P15).

---

## Phase 7 — Topics, Partitions & Ordering Semantics

### Objective
Real multi-partition topics: key-based routing, independent per-partition logs and offsets, and an ordering guarantee that is precise and testable.

### Why this phase exists
Partitioning is the central idea of event streaming: the unit of parallelism *and* the unit of ordering. Getting the guarantee exactly right — and writing it as a testable sentence — is the learning goal.

### Concepts to study
**THEORY:** partition as unit of ordering + parallelism; key hashing: implement FNV-1a, sanity-check its distribution, and know that Kafka uses murmur2 (compare briefly); keyless routing: round-robin for MVP (note sticky-partitioning as the modern alternative); why partition count is **fixed at creation** (offsets are per-partition; adding partitions breaks key→partition affinity and ordering assumptions); total vs partial order; what "per-partition order" means with concurrent producers: the broker serializes appends, so the log order is real, but *cross-producer* order of the same key is undefined unless a single producer sends that key; atomic metadata writes (temp + fsync + rename + dir fsync).

### Prerequisites
Phase 6.

### Small exercise
Distribution check on your FNV-1a over 1M random keys into 16 buckets — plot counts, compute the spread; then corrupt the hash deliberately (e.g., mod-only) and see the skew.

### RelayForge implementation tasks
`internal/partition`: N independent logs per topic; Produce routes by key hash or round-robin when key is empty; metadata v2 with partition count + creation config; `DescribeTopic`; per-partition LEO returned in Produce acks and Fetch responses; relayctl gains `--partition` visibility.

### Tests
Unit: FNV-1a against published test vectors; routing determinism (same key → same partition across a million calls); metadata corrupt → broker refuses to start with a clear error (never silently resets). Property/integration: 8 partitions; single producer, single key, 10k records → all land on one partition in strictly increasing order; multi-producer same key → per-partition order still holds, cross-producer order explicitly not asserted (document why).

### Expected output
An 8-partition topic with keys evenly distributed and per-partition order proven by tests.

### Definition of Done
Ordering property tests green; the ordering guarantee sentence in `docs/GUARANTEES.md` is exact (you can defend every clause); metadata crash-safety tested.

### Learning checkpoint
Explain: why no global order; why fixed partition count; what a client must do (single producer or sequence numbers) to get key order across producers; hash quality vs partition skew.

### Interview questions
How does key-based partitioning work? Why can't you safely add partitions later? How would a client achieve per-key ordering with multiple producers? What goes wrong with a weak hash? Round-robin vs sticky partitioning?

### Estimated time
1.5 weeks.

### Risks
Underestimating the ordering nuance — write the guarantee sentence *before* coding; over-raiding Kafka features (no compaction, no transactional partitioning).

### What NOT to build yet
Concurrency tuning (P8), consumer offsets (P10), groups (P11), replication (P15).

## Phase 8 — Concurrency Architecture: Goroutines, Bounded Queues, Shutdown

### Objective
Make the concurrency model deliberate: decide what is lock-protected vs actor-per-partition, add bounded inflight queues, and prove the whole thing race-clean under load with graceful drain.

### Why this phase exists
This is where Go theory meets systems reality. A broker that corrupts or deadlocks under concurrent load is worse than no broker; the discipline here (shared-state inventory, bounded resources, drainable shutdown) is exactly what "systems engineering" means in interviews.

### Concepts to study
**THEORY:** mutex vs channel-actor tradeoffs (shared state + short critical sections → mutex; state machine with an inbox → actor); why Fetch needs **no append lock**: sealed segments are immutable (P5), the active segment is read via `ReadAt`, and LEO is a single atomic — the reader only reads positions below LEO; bounded channels as queues; `errgroup` for worker lifecycles; context cancellation and propagation; `sync/atomic` for counters/gauges; the WaitGroup drain pattern; happens-before applied to append→fetch visibility; the race detector: what it catches, what it cannot prove; false sharing (awareness level).

### Prerequisites
Phase 7.

### Small exercise
**lab8-shutdown** — a worker pool draining a bounded queue on context cancel, guaranteed to finish within a deadline without losing accepted items. Reuse lab1 intuition at production seriousness.

### RelayForge implementation tasks
Chosen model (default, to be *defended*, not assumed): per-partition `sync.Mutex` around append (short critical section: encode + write + advance LEO), lock-free fetch via `ReadAt` + atomic LEO, one bounded inflight queue per partition (configurable depth), append workers consuming from queues; graceful shutdown: stop accepting → drain queues (with timeout) → fsync → close files; metrics stub hooks: queue depth gauge, append duration, in-flight count; config knobs: queue depth, drain timeout. Write the one-page ADR: "mutex-per-partition vs actor-per-partition," including the experiment you ran to decide.

### Tests
All under `-race`, mandatory: concurrent producers on one partition (order still strict); producers + concurrent fetchers; **shutdown under load** (trigger shutdown mid-storm → in-flight results settle, no panic, every acked record is on disk); queue saturation smoke (full queue blocks with timeout — full policy lands in P9); benchmark smoke to see lock contention appear in `go test -bench` output.

### Expected output
8 producers hammering one partition: no races, no lost acks, clean shutdown < 2 s; a written concurrency model you can defend.

### Definition of Done
Entire suite race-clean; shutdown-under-load test green; ADR documents the model decision with evidence; no unbounded goroutine or buffer growth (audit list written).

### Learning checkpoint
Explain: every shared mutable point in the broker and what protects it; why fetch takes no lock and what invariant makes that safe (immutability + LEO); what happens-before guarantees the fetcher sees a completed append; what `-race` does **not** prove.

### Interview questions
RWMutex vs Mutex here — why? Channel-actor vs lock-per-partition — when does each win? How would you detect lock contention in Go (mutex/block profiles)? How do you drain workers without losing or duplicating work? Where do atomics beat mutexes, honestly?

### Estimated time
2 weeks.

### Risks
Redesigning the storage API for concurrency (resist — P5's API was designed for this); false confidence from race-free tests (the detector samples, it doesn't prove).

### What NOT to build yet
Overload policy decisions (P9), consumer offsets (P10), groups (P11).

---

## Phase 9 — Backpressure, Overload Handling, Cancellation & Timeouts

### Objective
Define and implement the backpressure contract: the broker degrades predictably under overload — bounded memory, explicit errors, zero silently-dropped acked records — instead of dying or buffering without limit.

### Why this phase exists
Every real system eventually meets more load than it can absorb. What separates an engineered system from a demo is the *defined* behavior at the boundary. This phase produces guarantee card 5 and the fault-injection habits that Phase 13 industrializes.

### Concepts to study
**THEORY:** backpressure end-to-end: TCP send/receive buffers → gRPC/HTTP2 flow control → your bounded queues → disk latency; block-with-timeout vs fail-fast vs drop (and why drop is never acceptable for acked records); producer timeout semantics (timeout ≠ failure of durability: a timed-out produce may still have been appended — this ambiguity is *inherent* and must be documented, not hidden); unbounded queues are latency lies; context timeout/cancellation through every layer; admission control basics; queue depth as the key overload signal; coordinated omission (introduction — full treatment in P14).

### Prerequisites
Phase 8.

### Small exercise
Extend **lab1-bounded-queue** with both policies (block-with-timeout, fail-fast); microbenchmark both under 2× overload and compare latency distributions and memory.

### RelayForge implementation tasks
Backpressure contract v1: Produce waits up to `produce.timeout` for queue space, then returns explicit `OVERLOADED` — a rejected record is never appended and never acked; per-partition inflight cap; fetch `maxBytes` hard cap enforced server-side; disk-full: append fails → LEO rollback verified, error surfaces as a produce failure (broker stays alive); slow-disk fault wrapper around the storage file interface (inject e.g. 50 ms/write); cancellation: client cancels RPC mid-produce → no orphaned half-appends (verify by scanning the log). Document card 5 in `docs/GUARANTEES.md` and enumerate every buffer in the produce path with its bound (the "unbounded buffer audit").

### Tests
Integration overload: sustained 3× capacity for 30 s → broker alive, RSS flat, all failures are `OVERLOADED` (not timeouts, not crashes), zero acked-then-lost records; slow-disk sim → queue fills → clean overload, no OOM; disk-full sim on a small loopback file; timeout propagation; cancel-mid-produce test; memory ceiling assertion via `runtime.ReadMemStats` bounds in tests.

### Expected output
A broker you cannot kill with load: memory flat, errors explicit and typed, behavior written down.

### Definition of Done
Overload integration tests green; the "when the queue is full, exactly this happens" paragraph exists and matches the code; unbounded-buffer audit complete (every channel/buffer/slice with a bound listed).

### Learning checkpoint
Trace one Produce call through every buffer from client to disk, naming each bound. Explain why a timed-out produce is ambiguous and why that leads to at-least-once *on the produce side too* (duplicates from retries). Why reject-not-drop? Why does a bounded queue + timeout give a latency ceiling?

### Interview questions
Design backpressure for a broker — what bounds exist between client and disk? Block vs reject tradeoffs under overload? What happens when the disk fills mid-append? How do you bound memory under attack-by-load? What is coordinated omission?

### Estimated time
1.5 weeks.

### Risks
Disk-full rollback edge cases (partial write then error — the recovery scan from P4 is your safety net; test that interaction explicitly); knob explosion — keep two knobs (queue depth, timeout), no more.

### What NOT to build yet
Consumer-side flow control detail (P10), groups (P11), replication (P15).

---

## Phase 10 — Consumers, Offsets & At-Least-Once Delivery

### Objective
Durable consumer offsets, ergonomic fetches (`maxBytes`, `maxWait` long-polling), and at-least-once delivery with an *honest* duplicate analysis — duplicates must be observed in a test, not just claimed.

### Why this phase exists
Delivery semantics are the most interview-dense topic in messaging. You will build at-least-once, deliberately observe its duplicate, and be able to explain exactly why exactly-once requires more machinery than a broker offset file.

### Concepts to study
**THEORY:** fetch loop design (position-managed consumer vs broker-pushed subscription — why pull wins for log systems); commit semantics: commit **after** processing ⇒ at-least-once + duplicates on crash; commit **before** ⇒ at-most-once + loss — walk both truth tables; idempotency (deduping lives in the *consumer's* processing, not the broker; where a broker-side idempotent producer would live — note as stretch); offset storage: one file per (group, topic, partition), atomic replacement (temp + fsync + rename + dir fsync), CRC on the offset record; out-of-range policies (offset below retention start / beyond LEO → `earliest | latest | error`, configurable per group); long-poll `maxWait`: block until data arrives or deadline — condition/channel notification per partition; flow control via `maxBytes` (consumer-side of card 5).

### Prerequisites
Phase 9.

### Small exercise
**lab10-longpoll** — a notify-on-append channel/condvar wait with no missed wakeups, proven by a `-race` property test hammering appenders against waiters.

### RelayForge implementation tasks
Fetch gains `maxWait` (long-poll) and enforced `maxBytes`; `internal/offsets`: durable offset store (group-scoped files, atomic writes, corrupt-file → refuse with clear error, never silently reset); RPCs `CommitOffset`, `FetchOffsets`; relayctl `consume` command (poll → print → commit after print); duplicate-scenario harness used by tests.

### Tests
Unit: offset file roundtrip; torn/corrupt offset file → refuse to start (documented error); long-poll wakes on append within timeout; commit visibility. Integration: produce 10k → consume all exactly once (no duplicates in a clean run); **kill consumer after processing, before commit** → restart → redelivery observed and logged as a duplicate (the at-least-once proof); kill after commit → no redelivery; offset out-of-range → each policy behaves as configured; broker restart mid-consume → offsets durable.

### Expected output
`relayctl consume` works against a restarted broker; a test log line that literally says a duplicate was delivered after a crash.

### Definition of Done
At-least-once demonstrated by test; delivery card in `docs/GUARANTEES.md` states where duplicates arise and what would make processing safe; all flow-control knobs enforced server-side.

### Learning checkpoint
Explain both commit-order truth tables from memory; where exactly duplicates come from in *your* system (produce retries AND consumer redelivery); why exactly-once needs idempotent processing or transactional commit (and why you didn't build either); how long-poll avoids both busy-wait and missed wakeups.

### Interview questions
Compare at-most-once / at-least-once / exactly-once — mechanisms and costs. Where do duplicates come from in your design? Design durable offset storage. What breaks if you commit before processing? How does long-polling work end-to-end?

### Estimated time
2.5 weeks.

### Risks
Long-poll wakeup races (the property test is mandatory); scope creep toward transactions/producer idempotence (explicitly deferred to Part G).

### What NOT to build yet
Group rebalancing (P11 — offsets are stored per group name, one active consumer per group assumed); replication (P15).

---

## Phase 11 — Consumer Groups: Coordination, Heartbeats, Rebalance

### Objective
The group protocol: JoinGroup/SyncGroup/Heartbeat, generation-ID fencing, round-robin assignment, and automatic partition takeover when a member dies.

### Why this phase exists
Coordination under partial membership is your first taste of distributed-protocol thinking — still single-broker, so the difficulty is the protocol logic and its edge cases, not the network. This is also where consumer lag becomes a first-class number.

### Concepts to study
**THEORY:** the coordinator role (single broker → trivially located; the protocol is still real); the join/sync handshake and why assignment is computed once per generation (not per member); session timeout vs heartbeat interval vs (Kafka's) max poll interval — you implement session timeout only and document the rest; generation IDs as fencing tokens for commits; rebalance triggers: join, leave, death-by-timeout; assignment strategies — implement round-robin; rebalance storms and simple backoff; ephemeral membership vs durable offsets (membership dies with the broker process, offsets survive — and what that means on broker restart: everyone rejoins, nobody loses position).

### Prerequisites
Phase 10.

### Small exercise
Paper exercise: write out the timeline of a 3-member rebalance on paper — every RPC, every timeout — *before* implementing; then make the implementation match the paper.

### RelayForge implementation tasks
RPCs `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`; `internal/group`: coordinator with per-group state machine (Empty → PreparingRebalance → Stable), session timeouts via timer wheel or per-member timers, round-robin assignor, generation stamps on commits (stale generation → `REBALANCE_IN_PROGRESS` / `FENCED` error); relayctl `group list/describe`; consumer lag = LEO − committed per (group, topic, partition).

### Tests
Unit: assignment edge cases (more members than partitions, fewer, zero); fencing rejects stale-generation commits. Integration: 3 consumers join → every partition covered exactly once per generation; `kill -9` one member → rebalance completes within session timeout, coverage restored; rebalance during active consumption → no partition double-assigned within any generation; commit from fenced generation rejected; broker restart → members rejoin, committed offsets intact.

### Expected output
A live demo: 3 `relayctl consume --group g` processes; kill one; watch the others take over its partitions within the session timeout; lag visible in `group describe`.

### Definition of Done
Rebalance test matrix green; you can narrate one full rebalance timeline (every RPC and timer) from memory; consumer-groups doc written, including the "coordinator is a single point, membership is ephemeral" honesty.

### Learning checkpoint
Explain: why generation fencing (what stale-commit bug it prevents); session timeout vs heartbeat tuning tradeoffs; why assignment must be atomic per generation; why the single coordinator is acceptable now and what would change with multiple brokers.

### Interview questions
Walk through a consumer-group rebalance. What does fencing prevent? Heartbeat vs poll timeout — what does each detect? Design a group protocol in 5 messages. What happens if the coordinator dies — what did *you* lose? Why can't two members share a partition in one generation?

### Estimated time
3 weeks.

### Risks
The classic rebalance rabbit hole — timebox to the MVP protocol, write the edge-case list first, and explicitly defer exotic features (static membership, incremental rebalance) to `docs/LIMITATIONS.md`.

### What NOT to build yet
Multi-broker anything, replication (P15), max-poll-interval enforcement (document as a limitation).

---

## Phase 12 — Observability: Metrics, Structured Logs, pprof, Dashboards

### Objective
Replace the metrics stub with Prometheus, add Grafana dashboards, expose pprof, and tighten logging — observability whose only purpose is explaining *this* system's behavior.

### Why this phase exists
You are about to harden (P13) and optimize (P14) the broker; you need eyes first. Every metric must answer a named question about the system — no vanity dashboards.

### Concepts to study
**THEORY:** the Prometheus model (counters, gauges, histograms, summaries) and label cardinality discipline (per-partition labels yes, per-client-id no); mapping metrics to questions (rate/errors/duration per endpoint; queue depth during overload; lag for consumers); histogram buckets that match your actual latencies; RED and USE methods applied to a broker; pprof profiles — cpu, heap, goroutine, block, mutex — and reading flame graphs; log discipline: one line per meaningful event, request IDs via interceptor, levels that mean something; golden signals for the dashboard.

### Prerequisites
Phase 11 (lag metric needs groups); the P6 metrics stub.

### Small exercise
pprof one of your own labs: CPU-profile lab1-bench, find the top function, and explain the flame graph in your notes.

### RelayForge implementation tasks
Swap stub → `prometheus/client_golang`; `/metrics` on a separate HTTP port (localhost-bound); full metric set: `rf_produce_requests_total{result}`, `rf_records_appended_total{topic,partition}`, `rf_append_duration_seconds` (histogram), `rf_fsync_duration_seconds`, `rf_queue_depth{topic,partition}` (gauge), `rf_inflight_requests`, `rf_fetch_bytes_total`, `rf_consumer_lag{group,topic,partition}`, `rf_log_end_offset{topic,partition}`, `rf_committed_offset{...}`, `rf_errors_total{kind}`, `rf_recovery_duration_seconds`, `rf_segment_count`, `rf_disk_bytes`; pprof endpoints (localhost only — document the risk); first `docker-compose.yml`: broker + Prometheus + Grafana with provisioned dashboards; two dashboards: **broker-golden** (produce rate, error rate, append latency p50/p95/p99, queue depth) and **consumer-health** (lag, fetch throughput); request-ID propagation in logs via interceptors.

### Tests
Integration: scrape `/metrics` → expected metric families present and advancing under load; histogram bucket sanity; compose smoke script (up → scrape → down) runnable locally.

### Expected output
`docker compose up` → run load → live dashboards; a saved CPU flame graph of the broker under load in `docs/` (it becomes P14's "before" picture).

### Definition of Done
Every metric documented with the question it answers; dashboards screenshot honestly (no cherry-picked windows) into `docs/`; pprof endpoints work; compose stack comes up from a clean machine with one command.

### Learning checkpoint
Use only the dashboards to answer: where does append time go (p50 vs p99)? What does queue depth do during your P9 overload test? What's the lag shape when a consumer dies? Explain label cardinality with an example from your own metrics.

### Interview questions
Counter vs gauge vs histogram — when each? Why does label cardinality hurt Prometheus? What does the USE method tell you to instrument on a broker? How do you read a flame graph? How would you detect a slow consumer from metrics alone?

### Estimated time
1.5 weeks.

### Risks
Dashboard vanity (build only question-answering panels); pprof exposed publicly (bind localhost, note in docs).

### What NOT to build yet
Performance claims or optimizations (P14), tracing (Part G stretch).

---

## Phase 13 — Hardening: Fuzzing, Fault Injection, Integration & CI

### Objective
Adversarial confidence: native Go fuzzing for every decoder, a crash/corruption matrix that turns the failure list into automated tests, network fault injection, and a CI workflow — so every guarantee card is backed by a test you can point to.

### Why this phase exists
Everything so far was tested by *your* scenarios. This phase lets the machine search for scenarios you didn't think of, and converts the failure engineering list (Appendix) from prose into executable proof. This is the phase interviewers remember.

### Concepts to study
**THEORY:** Go native fuzzing (`go test -fuzz`, coverage-guided corpus, seeds as regression cases); property-based thinking (invariants over inputs, not example values); crash injection with subprocesses (parent spawns broker, lets it append N records, `kill -9` at deterministic points, restarts, asserts); deterministic seeds for reproducible failures; corruption injection; simulating disk-full and slow disk; network delay/disconnect via a TCP proxy in front of the broker; flaky-test discipline (generous timeouts, seeded randomness, retry hygiene); CI with GitHub Actions (test matrix + `-race` + 60-second fuzz smoke).

### Prerequisites
Phases 3–11 (this phase hardens all of them); WSL2 for `kill -9` semantics.

### Small exercise
Retroactively fuzz your Phase 2 framed-KV decoder — the first corpus entry should be the 1-byte-chunk partial write from that lab.

### RelayForge implementation tasks
Fuzz targets: record `Decode`, segment scanner, offset-file decoder, metadata loader — run each to ≥10⁶ executions locally, keep failing/interesting seeds in `testdata/fuzz`. `internal/faulttest`: crash-point hooks (crash after N appends / after append before ack / after ack), bit-flip corruption injector, slow-disk wrapper, tiny-loopback disk-full, TCP proxy with delay/disconnect. **Crash matrix test**: for crash-point × ack-mode × seeds → assert invariants: every acked record survives (per its ack mode), every served record is CRC-valid, LEO matches file contents, recovery terminates. Extend the P4 corruption tests: flip a bit at a random offset in any segment → recovery detects and truncates at/prior to it, and no client is ever served corrupt data. Malformed-RPC fuzz (random protobuf bytes to a test server → error codes, never panic). `docs/FAILURE_MATRIX.md` (the Appendix table, now with test names per row). GitHub Actions workflow: lint + vet + `test -race` + fuzz smoke.

### Tests
This phase *is* tests; expected coverage: ≥1 automated test per failure-matrix row; fuzzers clean at target executions; CI green from a clean clone.

### Expected output
A failure matrix where every row cites a passing test; fuzz seeds checked in; CI badge that means something.

### Definition of Done
For every row of the failure matrix you can name the invariant and the test; zero panics reachable from any fuzz corpus; CI runs the full matrix on every push.

### Learning checkpoint
For each failure type, state the expected behavior without looking. Explain the valid-prefix guarantee precisely (what is truncated, why nothing acked is lost, per ack mode). Why `kill -9` testing differs from graceful-shutdown testing — what does each prove?

### Interview questions
How would you test crash recovery of a log? What does fuzzing prove and not prove? How do you make a randomized test reproducible? What's your flaky-test policy? Design a fault-injection harness for a storage engine.

### Estimated time
2 weeks.

### Risks
Flaky CI (deterministic seeds, generous timeouts, separate slow-vs-fast job); Windows/WSL2 split for kill semantics (the matrix runs in WSL2, unit tests run anywhere).

### What NOT to build yet
Performance optimization (P14 — fix correctness only), replication (P15).

---

## Phase 14 — Benchmarking & Performance Engineering

### Objective
A reproducible benchmark harness with an honest methodology; profile-driven optimization with before/after evidence; published numbers that are yours and defensible.

### Why this phase exists
"Measurable results" is a project requirement — but a wrong benchmark is worse than none. This phase teaches measurement discipline (open-loop load, warmup, medians, page-cache honesty) and the pprof-driven optimization loop on a system you fully understand.

### Concepts to study
**THEORY:** benchmark design: fixed seeds, warmup, ≥3 runs reporting median and spread, pinned hardware conditions, machine specs published with results; **coordinated omission** — why closed-loop clients hide latency under load, and why your loadgen is open-loop (paced schedule, per-record timestamps); tail latency (p50/p95/p99) vs means; page-cache honesty (label runs by ack mode; `flushed` runs are the durable ones); GC influence on tails (`GODEBUG=gctrace=1`); allocation reduction: buffer reuse, `sync.Pool`, preallocation; batching: throughput/latency tradeoff analysis (decide implement-or-defer on *evidence*); the optimization loop: hypothesis → profile → change → re-benchmark → keep-or-revert.

### Prerequisites
Phases 12 (server metrics), 13 (stability worth measuring).

### Small exercise
**lab14-loadgen** — a 100-line open-loop load generator against a stub server; deliberately overload it and *see* coordinated omission appear in closed-loop numbers and stay honest in open-loop ones.

### RelayForge implementation tasks
`bench/`: loadgen client (configurable record size, partitions, producers, ack mode, duration, pacing; collects client-side latency and throughput) + runner scripts that set up, run, and dump raw results; run the matrix: record size {100 B, 1 KiB, 10 KiB, 100 KiB} × partitions {1, 4, 16} × producers {1, 8} × ack {none, written, flushed} × 60 s. Profile with pprof (cpu, heap, block, mutex) under the heavy runs; implement the top 2–3 *safe* optimizations the profiles justify (likely: encode-buffer reuse via `sync.Pool`, index caching, write-buffer sizing); re-run the affected matrix cells; `docs/BENCHMARKS.md`: methodology, environment description, results tables, before/after with profile evidence, and an explicit "things I chose not to optimize and why." Decide batching on evidence; if deferred, it goes in Part G with the profile that would justify it.

### Tests
Harness determinism smoke (same seed → same schedule); correctness suites must stay green after every optimization (perf work never breaks guarantees — the race suite runs after each change).

### Expected output
Results tables (events/s, MB/s, p50/p95/p99, per dimension) built from *your* runs; before/after optimization table with flame graphs as evidence.

### Definition of Done
Anyone can reproduce a benchmark from `scripts/` + documented environment; every number in `docs/BENCHMARKS.md` traces to a stored raw result; every optimization claim has a profile; no invented numbers anywhere.

### Learning checkpoint
Explain your best throughput number and the two bottlenecks behind it (from profiles, not folklore). Why open-loop load generation? Why must page-cache-only runs be labeled as such? What did you refuse to optimize, and what evidence justified stopping?

### Interview questions
How would you benchmark a log storage system honestly? Mean vs p99 — when do they disagree and why? What is coordinated omission? How does GC affect tail latency? Batching — throughput wins, latency costs; how do you decide?

### Estimated time
2.5 weeks.

### Risks
Optimizing microbenchmarks that don't matter (only optimize what the *matrix* shows); laptop noise (power plugged in, background apps closed, specs documented); unfair config comparisons (change one dimension at a time).

### What NOT to build yet
Replication (P15).

---

## Phase 15 — Replication: Leader/Follower, High-Watermark, Controlled Failover & Final Polish

### Objective
Limited, honest replication per partition: leader + follower(s), ISR and high-watermark tracking, `acks=all`, follower catch-up, **manual** failover with zombie-leader fencing — then final documentation polish. No consensus, no automatic election, no HA claims.

### Why this phase exists
Replication is where single-node correctness meets distributed reality. Building a *small* replication layer — with precisely stated, tested guarantees — teaches high-watermarks, ISR, catch-up, and fencing without the swamp of real consensus. Knowing why automatic election is hard is the lesson; shipping a fake version of it would be the mistake.

### Concepts to study
**THEORY:** leader/follower per partition; replica fetchers (followers pull from the leader — same log mechanics you already built, used recursively); high-watermark (HW) = min LEO across in-sync replicas; ISR membership and lag-based eviction/rejoin; ack semantics extended: `acks=1` (leader-local, current behavior) vs `acks=all` (wait for HW); follower catch-up from its own LEO after pause/restart; failover: static/manual promotion (operator or config selects the new leader *from the ISR*) — and a precise explanation of why automatic election is hard (split-brain, stale leader, need for consensus/leases); zombie-leader fencing via leader epochs (each leader term has an epoch; brokers reject writes carrying an old epoch); what can still be lost (all ISR copies fail before fsync under `written`; leader + all followers lose disk simultaneously) — stated, not hidden.

### Prerequisites
Everything through P14; the single-node system proven correct is a hard gate.

### Small exercise
Read Raft §5.1–5.3 *after* implementing (or alongside, late in the phase) and map each Raft concept (term, commitIndex, log matching) to what you built and what you deliberately did not. The mapping table goes in your notes.

### RelayForge implementation tasks
Cluster mode: N broker processes; partition→(leader, followers) assignment in static config (`configs/`); follower fetcher loop (pull records from leader from its own LEO, append locally, report replica LEO); leader-side ISR tracking (lag-based in/out) and HW computation; `acks=all` produce path waits for HW; follower catch-up on restart; `relayctl assign-leader` (manual failover) or config-reload equivalent; leader-epoch file + epoch checks to fence zombies (basic version); metrics: `rf_replication_lag{partition,replica}`, `rf_isr_size`, `rf_hwm{topic,partition}`; `docs/GUARANTEES.md` replication card: exact claim ("with acks=all and manual failover, an acked record survives leader failure iff at least one in-sync follower survives"), preconditions, and non-claims. Final polish: all docs current, failure matrix extended with replication rows (leader crash, follower lag/eviction, zombie leader, all-replicas-down), honest README summary, one scripted demo.

### Tests
Unit: HW computation (follower sets in various states); ISR eviction/rejoin logic; epoch fencing rejects stale-epoch writes. Integration: follower pauses → replication lag grows → catch-up returns it to ISR; `acks=all` + `kill -9` leader → manual promotion → **no acked record lost** (verified by comparing acked offsets pre-crash vs readable post-failover); old leader isolated and restored → writes rejected by epoch fencing; all replicas down → produce unavailable with a clear error (documented behavior); replication lag visible under load via metrics.

### Expected output
A 3-broker compose demo: kill the leader, promote a follower with one command, clients reconnect and continue; a limitations section you can read aloud without flinching.

### Definition of Done
Every replication claim in the guarantee card is backed by a test; you can explain HW, ISR, catch-up, and fencing, and why auto-election was excluded; the full failure matrix passes; docs and README final.

### Learning checkpoint
Why does `acks=all` + HW prevent acked loss, and what are its exact preconditions? Where can this design still lose data (state the disk-durability interaction precisely)? Why is fencing necessary — narrate the zombie-leader incident your test simulates. Map Raft vocabulary onto what you built and what you didn't.

### Interview questions
Explain high-watermark and ISR. Why is automatic leader election hard — what does it require? What is split-brain and how does your epoch fencing address one instance of it? What does `acks=all` *not* guarantee in your system? Compare your replication design to Kafka's conceptually — where does Kafka need consensus (KRaft) and why?

### Estimated time
3.5–4 weeks.

### Risks
The largest scope risk in the project — timebox hard, keep assignment static, cut epoch fencing to its simplest correct form; cluster config bugs eat days (deterministic configs, one broker per terminal habit).

### What NOT to build ever (in this project)
Consensus, automatic safe leader election, HA claims, exactly-once, cross-partition transactions, Kafka protocol compatibility.

# Part D — Milestone Summary

| Milestone | Phases | What you can claim at the end of it |
|---|---|---|
| **Foundation** | P1–P2 | Fluent Go, race-clean concurrent code, hand-rolled TCP framing — you understand the network before hiding it behind gRPC |
| **Core Storage** | P3–P5 | A standalone log engine: binary record format, CRC, ack modes, valid-prefix recovery, segments, sparse index, retention — resume-worthy as a library on its own |
| **Broker** | P6–P7 | End-to-end gRPC broker + CLI, multi-partition topics, exact ordering semantics |
| **Concurrency** | P8–P9 | A deliberate concurrency model, bounded everything, defined overload behavior — the broker cannot be killed by load |
| **Messaging Semantics** | P10–P11 | At-least-once delivery with observed duplicates, durable offsets, consumer groups with heartbeat-driven rebalance and fencing |
| **Observability** | P12 (hooks from P6) | Prometheus + Grafana + pprof; every metric answers a question |
| **Reliability** | P13 (founded in P4) | Fuzzed decoders, automated crash/corruption matrix, failure matrix as tests, CI |
| **Performance** | P14 | Reproducible benchmarks, profile-driven optimizations with before/after evidence, honest published numbers |
| **Advanced Distributed** | P15 | Leader/follower replication, high-watermark acks, manual failover with fencing — with claims that match the tests |

# Part E — Final MVP

**The MVP is the completion of Phases 1–14** (everything except replication), estimated at ~24–28 weeks at ~15 h/week. Mapping of your required MVP checklist to phases:

| MVP requirement | Phase(s) |
|---|---|
| Go broker, gRPC/Protobuf API, CLI, health/readiness, structured logs | P6 (logs tightened P12) |
| Topic creation, configurable partitions, key-based partitioning, per-partition ordering | P7 |
| Custom binary record format, checksums | P3 |
| Append-only segment files, sparse index | P5 |
| Startup recovery | P4 (+P5 index rebuild) |
| Fetch by offset | P6, long-poll/maxBytes P10 |
| Persistent consumer offsets, at-least-once semantics | P10 |
| Bounded queues, overload handling | P8, P9 |
| Basic consumer groups | P11 |
| Prometheus metrics | P12 |
| Unit tests | P3 onward, continuously |
| Integration tests | P6 onward, P13 systematic |
| Race tests | P8 onward, mandatory |
| Fuzzing for record decoding | P13 |
| Crash/restart tests | P4 mini-matrix, P13 full matrix |
| Reproducible load tests | P14 |
| Docker Compose for local demonstration | P12 (observability stack), P14 (final demo compose) |

The MVP is complete, demonstrable, and resume-worthy with **no replication**.

# Part F — Advanced Version

**The advanced version is Phase 15.** It adds: multiple broker processes, static partition placement, leader/follower replication per partition, replica lag tracking, configurable ack policies (`acks=1` / `acks=all`), follower catch-up, controlled/manual leader failover, zombie-leader fencing via epochs, and replication metrics. Its claims are exactly the replication guarantee card in Part A.3 — and its non-claims (no consensus, no automatic election, no HA, no exactly-once) are stated in `docs/GUARANTEES.md` and `docs/LIMITATIONS.md`.

# Part G — Final Stretch Features (all optional, none required for any claim)

1. **Raft-lite learning module** — a separate spike branch implementing leader election + log replication on toy state (never merged into the broker's claims). Value: real consensus understanding. Risk: time sink; strictly post-P15.
2. **Idempotent producer** — producer ID + per-partition sequence numbers, broker-side dedupe window. Value: kills produce-side duplicates. Risk: protocol surface growth.
3. **Record batching** — group records into batches on the wire and in one fsync (only if P14 profiles justify it; otherwise documented as rejected-for-now with the profile that would change your mind).
4. **mmap read path** — experiment vs `ReadAt` with benchmarks. Pure learning experiment; keep whichever wins with evidence.
5. **OpenTelemetry tracing** — one traced produce→consume path to learn span thinking. Only if it survives the "does it answer questions" test from P12.
6. **Streaming Produce RPC** — client-side batching over a gRPC stream. Natural follow-on to batching; measure before keeping.

# Part H — Final Technology Stack

| Technology | Why it's needed | What you learn from it | MVP/Advanced |
|---|---|---|---|
| **Go (current stable, ≥1.24)** | The systems language of the project; static binaries, first-class concurrency, superb tooling | concurrency, memory model, profiling culture, a second language family after Python | MVP (P1) |
| **WSL2 (Ubuntu) on your Windows host** | Real `kill -9`, real fsync semantics, easy disk-full sims — where brokers actually run | Linux process/filesystem behavior your fault tests depend on | MVP (set up P1) |
| **Protobuf + protoc (+ generated Go stubs)** | Typed, evolvable wire contract | schema discipline, field-number evolution, codegen | MVP (P6) |
| **grpc-go** (incl. interceptors, health protocol, status codes) | Production-grade RPC with deadlines, cancellation, flow control | RPC lifecycle, error mapping, deadline propagation | MVP (P6) |
| **Go stdlib: `net`, `os`, `encoding/binary`, `hash/crc32` (Castagnoli), `slog`, `testing`, native fuzzing, pprof, `sync`, `context`, `atomic`** | Deliberately minimal dependencies — the hard work must be yours | everything: framing, file I/O, checksums, structured logging, property/fuzz testing, profiling | MVP (throughout) |
| **prometheus/client_golang + Prometheus server** | Metrics the industry uses; histogram semantics | metric modeling, cardinality, scraping | MVP (P12) |
| **Grafana** | Dashboards over Prometheus | reading your own system's behavior visually | MVP (P12) |
| **Docker + Docker Compose** | One-command demo/repro stack (broker + Prometheus + Grafana; 3-broker topology in P15) | containerized dev environments | MVP (P12), Advanced topology (P15) |
| **GitHub Actions (optional)** | CI: vet + `-race` + fuzz smoke on every push | CI discipline | P13, optional |
| **cobra (only if the CLI outgrows stdlib `flag`)** | Subcommand ergonomics — but the CLI is deliberately boring; `flag` + dispatch is enough | when *not* to add a dependency | optional, likely never |
| **Raft (paper only; library use: never)** | Vocabulary for P15's "why no auto-election" lesson | what problem consensus solves and what you didn't build | Advanced/stretch |

Explicitly rejected (per your scope rules and with reasons recorded in ADRs as they arise): Kafka protocol compat, React dashboards, PostgreSQL/Redis (no state belongs in a database here — the log *is* the database), Kubernetes, schema registry, OTel in the MVP.

# Part I — Learning Curriculum (study checklist)

Work these alongside the phases; each item maps to where it gets used.

**Go**
- [ ] Slices: len/cap/aliasing/growth (P1)
- [ ] Interfaces: implicit satisfaction, nil-interface trap (P1)
- [ ] Goroutine scheduler: G/M/P model, preemption (P1)
- [ ] Channels: buffered/unbuffered, close semantics, nil channels (P1)
- [ ] `sync` toolkit: Mutex, RWMutex, WaitGroup, Once, atomic (P1, P8)
- [ ] Memory model: happens-before, what `-race` checks (P1, P8)
- [ ] `context`: cancellation, deadlines, values (P6, P8–P9)
- [ ] pprof: cpu/heap/goroutine/block/mutex profiles, flame graphs (P12, P14)
- Resources: *The Go Programming Language* (Donovan & Kernighan); *Concurrency in Go* (Cox-Buday); Go blog: memory model, race detector, pprof.

**Networking**
- [ ] TCP streams vs messages; partial reads/writes (P2)
- [ ] Connection lifecycle, half-close, TIME_WAIT intuition (P2)
- [ ] Deadlines/timeouts at every layer (P2, P6, P9)
- [ ] HTTP/2 multiplexing as gRPC's substrate (P6)
- [ ] Flow control: TCP windows → HTTP/2 → application queues (P9)
- Resources: Beej's Guide; TCP/IP Illustrated Vol. 1 (selected chapters); gRPC docs.

**Linux I/O**
- [ ] `write(2)`/`read(2)`, page cache, `fsync`/`fdatasync`, fsync-on-directory (P4)
- [ ] Torn writes and sector atomicity (P4)
- [ ] `pread`/`pwrite`, file descriptors vs offsets (P5)
- [ ] Buffered vs direct I/O intuition (P4, P14)
- Resources: man7.org man pages; Kerrisk, *The Linux Programming Interface* ch. 4, 13.

**Binary formats**
- [ ] Endianness, fixed vs variable-length encoding (P3)
- [ ] Length-prefix framing; CRC32C properties (P2, P3)
- [ ] Versioning a binary format in place (P3, P6)
- Resources: Protobuf encoding spec; Kafka wire-format docs (as a studied example).

**Durable storage**
- [ ] Append-only logs: why O(1) amortized writes win (P3)
- [ ] Offsets vs positions; sparse indexing; segment rotation (P5)
- [ ] Recovery as valid-prefix truncation (P4)
- [ ] Retention by whole-segment deletion (P5)
- Resources: Kleppmann, *DDIA* ch. 3; the Kafka SIGMOD'11 paper; Kafka "log design" docs.

**Concurrency**
- [ ] Lock design: critical sections small, invariants written (P8)
- [ ] Actor vs shared state; bounded queues (P8–P9)
- [ ] Graceful shutdown/drain patterns (P8)
- [ ] Deadlock/race/livelock taxonomy (P8, P13)
- Resources: *Concurrency in Go*; Go blog on race detector.

**Distributed systems basics**
- [ ] Partial failure; the two-generals shape of produce-ack ambiguity (P9)
- [ ] Replication: leader/follower, HW, ISR, catch-up (P15)
- [ ] Fencing stale leaders/writers; epochs (P15)
- [ ] Why consensus (Raft) exists and what it costs (P15 stretch)
- Resources: *DDIA* ch. 5, 8, 9; Raft paper §5; Jepsen writings on delivery semantics.

**Messaging semantics**
- [ ] At-most/at-least/exactly-once truth tables (P10)
- [ ] Commit ordering → duplicates vs loss (P10)
- [ ] Idempotency placement (consumer vs producer) (P10, Part G)
- [ ] Group coordination and rebalance (P11)
- Resources: Kafka delivery-semantics docs; *DDIA* ch. 11.

**Observability**
- [ ] Metric types and cardinality (P12)
- [ ] RED/USE applied to a broker (P12)
- [ ] Structured logging discipline (P6, P12)
- [ ] Flame graph literacy (P12, P14)
- Resources: Prometheus docs (metric types, histograms); Google SRE book ch. 6; Go blog: pprof.

**Performance engineering**
- [ ] Open- vs closed-loop load; coordinated omission (P14)
- [ ] Tail latency; GC effects (P14)
- [ ] Optimization loop: profile → change → re-measure → revert (P14)
- [ ] Benchmark honesty: warmup, medians, labeled durability modes (P14)
- Resources: Brendan Gregg's site (USE method, flame graphs); Go blog: profiling.

**Testing**
- [ ] Table-driven tests; property-based thinking (P3, P5)
- [ ] Race detector practice (P1, P8)
- [ ] Native fuzzing with corpora (P13)
- [ ] Crash/fault injection via subprocess and hooks (P4, P13)
- [ ] Flaky-test hygiene and CI (P13)
- Resources: Go fuzzing tutorial; `testing` package docs.

# Part J — Interview Preparation (consolidated question bank)

Per-phase questions are in Part C; this is the cross-cutting bank to drill before interviews.

**Foundation (Go/tooling):** goroutine vs thread cost model · channel vs mutex decision · what `-race` does and doesn't prove · slice/interface/defer gotchas · how the Go scheduler differs from an OS-thread pool.

**Storage:** why append-only logs dominate write-heavy systems · record format design exercise (you have a real one to draw) · sparse index lookup walkthrough · fsync semantics and ack modes · valid-prefix recovery — narrate a crash · why segments make deletion O(1).

**Networking:** framing on a byte stream · partial read/write handling · deadlines end-to-end · why gRPC over HTTP/2 · where flow control lives at each layer.

**Concurrency:** inventory your locks (you can, by name) · why fetch is lock-free and the invariant that makes it safe · drain-and-shutdown pattern · block vs mutex profiles · bounded queue design and its latency ceiling.

**Messaging:** delivery-semantics truth tables · where duplicates arise in *your* system (produce retries + consumer redelivery) · long-poll mechanics · rebalance walkthrough · generation fencing · consumer lag — definition and detection.

**Reliability:** your failure matrix, row by row · crash-point testing methodology · what fuzzing proves · the produce-timeout ambiguity and why at-least-once is unavoidable · disk-full behavior end-to-end.

**Performance:** your throughput number and its two bottlenecks (from profiles) · open-loop load generation · p99 vs mean · GC and tails · an optimization you made and one you rejected, both with evidence.

**Distributed systems:** high-watermark and ISR, precisely · why `acks=all` has preconditions · zombie leader and epoch fencing · why automatic leader election requires consensus · where your design would need Raft and what Raft would cost · map Raft terms to your P15 structures.

# Part K — Final Portfolio Positioning

Your three projects form a deliberate stack:

| | CodeGraph | PulseGuard | **RelayForge** |
|---|---|---|---|
| Identity | AI / developer intelligence / application engineering | ML / statistics / MLOps | **Systems / infrastructure / reliability / performance** |
| Stack center | FastAPI + Postgres/pgvector + LLMs | Python ML lifecycle + evaluation rigor | **Go + gRPC + durable storage + concurrency** |
| Hard problems | Retrieval quality, code understanding, async pipelines | Model validity, drift, honest evaluation | **Correctness under failure, ordering, durability, tail latency** |
| Evidence | Working product + search quality | Metrics-backed model lifecycle | **Guarantee cards with passing tests + reproducible benchmarks** |

RelayForge fills the gap the first two leave: nothing in CodeGraph or PulseGuard demonstrates low-level systems engineering — no binary formats, no file I/O, no concurrency under load, no failure injection, no performance engineering. RelayForge adds that, in a language new to your portfolio, with the two things interviewers probe hardest in systems discussions: **precisely stated guarantees** (each backed by a named test) and **honest measured numbers** (each backed by a reproducible benchmark). The trio reads as an engineer who can build the application, the models, *and* the infrastructure underneath — and who knows the difference between claiming reliability and testing it.

When presenting it, lead with: the guarantee cards, the failure matrix, one benchmark table, and the one-sentence scope statement ("deliberately not Kafka — a well-specified single-node broker with limited replication, where the guarantees are the deliverable").

---

# Appendix — Failure Matrix

Every row is implemented and tested in the cited phases; `docs/FAILURE_MATRIX.md` (P13) keeps this table with per-row test names.

| Failure | Injected when | Expected behavior | Invariant preserved | Duplicates? | Loss? | Delay? | Recovery | Tested in |
|---|---|---|---|---|---|---|---|---|
| Crash during append | mid-record write | torn tail detected via CRC/length; truncated on restart | valid prefix; nothing acked lost (ack≥written) | no | no (for acked) | no | restart → truncate tail | P4, P13 |
| Crash after append, before ack | between durable write and RPC reply | client sees timeout; record may exist | no acked loss; produce-side at-least-once on retry | yes (on producer retry) | no | no | client retry | P13 matrix |
| Crash after ack | post-ack kill | record readable after restart | durability per ack mode | no | no | no | none needed | P13 matrix |
| Power-loss class failure | ack-mode = written vs flushed | `written` not guaranteed; `flushed` guaranteed | per-mode durability | no | possibly, `written`+power loss only | no | restart | P4 lab, P13 |
| Partial record (truncated frame) | random truncation | decode fails; recovery truncates | no corrupt data ever served | no | unacked only | no | scan + truncate | P3, P4, P13 |
| Corrupted record (bit flip) | random offset flip | CRC mismatch at read or recovery; truncated at/prior to it | CRC-valid reads only | no | affected tail only | no | truncate | P13 |
| Corrupted index | index bit flip | lookup falls back to forward scan; index rebuilt on recovery | reads still correct | no | no | slight | rebuild | P5, P13 |
| Metadata (topics file) corruption | topics.json flip | broker refuses to start with clear error; never auto-resets | no silent data-semantics change | no | no | no | manual repair | P7, P13 |
| Disk full | small loopback volume | append errors; LEO rolls back; broker alive; produces fail explicitly | no partial accepted records | no | no | no | free space → resume | P9, P13 |
| Slow disk | fault wrapper adds latency | queues fill → `OVERLOADED` after timeout; memory bounded | backpressure contract | no | no (rejected ≠ dropped acked) | yes (by design) | remove fault | P9, P13 |
| Producer overload | 3× capacity load | explicit `OVERLOADED`; no crash; RSS flat | bounded buffers | no | no | yes | shed load | P9 |
| Slow consumer | tiny maxBytes, high latency | fetch bounded; produce path unaffected; lag grows | isolation of produce/consume paths | no | no | yes (consumer-side) | consumer catches up | P10 |
| Consumer crash | kill mid-process | uncommitted records redelivered | at-least-once | yes | no | no | resume from commit | P10 |
| Commit-before-crash race | kill between process and commit | same as above — observed duplicate | commit-after-process rule | yes | no | no | resume | P10 |
| Network disconnect (client) | proxy cut mid-RPC | client timeout; broker completes or rejects atomically | no half-visible records | yes (retry) | no | no | reconnect | P13 |
| Malformed RPC / random bytes | fuzzed protobuf | typed error codes; never a panic | broker liveness | no | no | no | none needed | P13 |
| Offset file corruption | flip commit file | refuse to start group's offset with clear error | no silent position reset | no | no | no | manual repair | P10, P13 |
| Member death in group | kill -9 consumer | rebalance within session timeout; partitions reassigned | ≤1 owner per partition per generation | no (within generation) | no | yes (timeout window) | rebalance | P11 |
| Stale-generation commit | fenced commit | rejected with `FENCED`/`REBALANCE_IN_PROGRESS` | fencing | no | no | no | rejoin | P11 |
| Replica lag (P15) | pause follower | follower evicted from ISR past threshold; catch-up on resume | HW never exceeds ISR min | no | no | yes (followers) | catch-up | P15 |
| Leader failure (P15) | kill -9 leader | manual promotion from ISR; acks=all records survive iff ≥1 in-sync follower survives | acked ⊆ HW prefix on new leader | no (acks=all) | only if all ISR copies fail | yes (failover window) | assign-leader + client reconnect | P15 |
| Zombie leader (P15) | old leader restored isolated | writes rejected via leader epoch | single writer per epoch | no | no | no | epoch fencing | P15 |
| All replicas down (P15) | kill cluster | produce unavailable with clear error; documented, not "HA" | honesty | no | no | yes | operator restarts | P15 |

---

*End of roadmap. Next action when you are ready: begin Phase 1 (environment setup + labs) — on your go, not mine.*


