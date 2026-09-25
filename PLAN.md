# RelayForge — Master Execution Plan

**Document status:** Authoritative project roadmap. Owner-approved.
**Companion document:** `IMPLEMENTATION_PLAN.md` (operating rules for the coding agent — read both before any work).
**Last updated:** 2026-09-26

This document answers: *"What exactly do I need to build in each of the 10 phases?"*
`IMPLEMENTATION_PLAN.md` answers: *"How must the coding agent behave while building those phases?"*

---

## 0. How to Use This Document

The complete project is driven by one command pattern:

```text
Read PLAN.md and IMPLEMENTATION_PLAN.md.
Execute Phase N.
```

Each phase in [§19](#19-the-ten-phases) is a complete, independently executable unit: it specifies scope, tests, failure cases, verification, and a Definition of Done. The agent executes the phase, reports, and stops. It must never begin Phase N+1 on its own.

Rules of engagement for the agent live in `IMPLEMENTATION_PLAN.md`. This document defines the **what**; that document defines the **how**. Neither may be contradicted by ad-hoc instructions without the owner's explicit approval.

There are **exactly 10 phases**. If scope pressure arises during execution, the agent adjusts *within* a phase and documents the outcome — it never invents an 11th phase and never silently drops scope (undelivered scope becomes a documented known limitation).

---

## 1. Project Identity

RelayForge is a deliberately scoped **Go event-streaming platform / event broker**. It is the third and final project of a three-project portfolio:

```text
CodeGraph   → AI / developer intelligence
PulseGuard  → ML / statistical systems
RelayForge  → systems / infrastructure / reliability / performance
```

RelayForge must remain fundamentally different from the other two projects. Its core identity:

> **durability + concurrency + networking + messaging semantics + reliability + measurable performance**

Built around: Go, concurrency, TCP/RPC networking concepts, gRPC, Protocol Buffers, durable append-only logs, custom binary record formats, partitions, ordering, offsets, consumer groups, backpressure, crash recovery, observability, fault injection, reproducible benchmarking, and limited manual-failover replication.

RelayForge is deliberately **not Kafka**. It must never be transformed into an AI/ML project. The log engine itself is the central storage engine — it is never replaced by PostgreSQL, Redis, Elasticsearch, or any external store.

---

## 2. Core Guarantees

These guarantees are the contract. Every phase that touches them must preserve them, test them, and document them truthfully.

### 2.1 Ordering

- Records **within one partition** are served in append order.
- There is **no global topic ordering** and **no cross-partition ordering** — never claimed, never promised.
- The partition is the unit of ordering and of parallelism.

### 2.2 Durability (producer acknowledgement modes)

| Mode | Acknowledged after | Survives process crash | Survives power loss | Notes |
| --- | --- | --- | --- | --- |
| `ack=flushed` | Record is **fsynced** | Yes | Intended, subject to documented filesystem/storage assumptions | Strongest mode |
| `ack=written` | Record has reached the **OS/page cache** | Yes | **Not guaranteed** | **Default design target** |
| `ack=none` | Record accepted for processing | No | No | No durability guarantee |

Iron rule: **never acknowledge a record before it has reached the durability state promised by that acknowledgement mode.** (Batched/group fsync is permitted only if the ack still happens strictly after the data is durable per the mode.)

### 2.3 Delivery

- RelayForge provides **at-least-once delivery**. It does **not** provide exactly-once.
- Duplicates are possible and intentional, including from:
  - producer timeout after append but before response (retry → duplicate),
  - consumer processing followed by crash before offset commit (redelivery).

### 2.4 Recovery

After a crash, each partition must recover to the **longest valid prefix** of its log.

- Invalid/torn/corrupt tail data is truncated.
- Corrupt records are never served as valid records.
- Recovery uses: length framing, checksums, record validation, truncation.

### 2.5 Backpressure

- All buffers/queues are **bounded**.
- When a partition queue is full: the producer waits only within a configurable timeout, then receives an **explicit overload error**.
- Memory must never grow without bound.

### 2.6 Consumer groups

Within one generation:

- a partition is assigned to **at most one** group member,
- stale-generation commits are **rejected** (fencing),
- failed members are **detected** (heartbeat/session timeout),
- partitions are **reassigned**,
- generation fencing prevents stale members from committing after reassignment.

### 2.7 Replication stance

Replication is an **advanced, limited, manual-failover** feature. It must never be presented as consensus, Raft, automatic safe election, full HA, or exactly-once. The intended model:

- leader/follower with **static placement**,
- **ISR** (in-sync replicas) and **replication lag**,
- **high-watermark (HW)** reads and `acks=all`,
- **manual failover** only,
- **leader epochs** with **zombie-leader fencing**.

Raft may only be considered as a separate learning experiment **after** this project is complete — never inside RelayForge.

---

## 3. Architecture

Target end-state architecture (evolved incrementally — never built all at once):

```text
+------------------------------------------------------+
| relayctl CLI / loadgen / benchmark harness           |
+------------------------------------------------------+
| gRPC API + Protobuf + health + interceptors          |
+------------------------------------------------------+
| Broker                                               |
|  - timeout policy                                    |
|  - backpressure policy                               |
+---------------------------+--------------------------+
| Topic / Partition Manager | Group Coordinator        |
| key -> partition          | join/sync/heartbeat      |
+---------------------------+--------------------------+
| Partition Log             | Offset Manager           |
| per-partition storage     | durable commits/fencing  |
+------------------------------------------------------+
| Log Engine                                           |
| segments / sparse index / retention / recovery       |
+------------------------------------------------------+
| Record Format                                        |
| length / CRC32C / version / timestamp / key / value  |
+------------------------------------------------------+
| Linux filesystem                                     |
| write -> page cache -> fsync                         |
+------------------------------------------------------+
```

Storage evolution ladder (each rung is a real engineering stage):

```text
single-file append-only log
        ↓
durability + recovery
        ↓
segment files
        ↓
sparse indexes
        ↓
retention
        ↓
partition log used by broker
```

Repository evolution rule: **only create directories when their corresponding engineering functionality exists.** The tree grows phase by phase; early phases must not scaffold the final layout.

Expected late-stage tree (illustrative, not mandated early):

```text
api/proto/
cmd/
internal/
    logengine/
    broker/
    metadata/
    partition/
    offsets/
    group/
    observability/
    faulttest/
bench/
scripts/
configs/
docs/
```

---

## 4. Storage Design

### 4.1 Record format v1 (authoritative)

| Field        | Size     |
| ------------ | -------: |
| frame length | 4 bytes  |
| CRC32C       | 4 bytes  |
| version      | 1 byte   |
| attributes   | 1 byte   |
| timestamp    | 8 bytes  |
| key length   | 4 bytes  |
| key          | variable |
| value length | 4 bytes  |
| value        | variable |

- All multi-byte values are **little-endian**.
- **CRC32C** (Castagnoli) covers the record body **after the CRC field** — i.e., `version` through the end of `value` (18 + len(key) + len(value) bytes).
- **Frame length** counts every byte after the length field itself (CRC through end of value = 22 + len(key) + len(value) bytes).
- Fixed header overhead: 26 bytes + key + value.
- `version` starts at 1; unknown versions on decode are rejected.
- `attributes` is reserved in v1 (must be 0; compression is a non-goal).
- Timestamps: int64 milliseconds since Unix epoch, UTC; broker sets at append when the producer supplies zero.

### 4.2 Log semantics

- Per-partition offsets are 0-based and strictly contiguous (+1 per record) at the engine level.
- The index is **derived state** — the log is the source of truth; a corrupt/missing index is rebuilt, never trusted for correctness.
- The log is append-only; sealed segments are immutable; only the active segment may be appended to, and only at its end.

---

## 5. Partitioning & Routing

- Topics contain a configurable partition count, **fixed at topic creation** for the MVP (no resize).
- Routing:

```text
keyed event     → deterministic hash (FNV-1a over key bytes, mod partition count)
keyless event   → round-robin-style routing (atomic counter)
```

- Partitions provide: parallelism, independent logs, independent offsets, independent ordering domains.
- Never claim global ordering — not in code, docs, README, or demo.

---

## 6. Consumer Model

- Consumers fetch using: `topic`, `partition`, `offset`, `maxBytes`, `maxWait`.
- Fundamental loop: **fetch → process → commit offset**.
- A crash between processing and committing **may cause redelivery — that is intentional** (at-least-once).
- Committed-offset convention (MVP): a committed offset means **the next offset to consume**.
- Offset resets when no committed offset exists are explicit per fetch (`earliest`/`latest`). No auto-commit in the MVP.
- Consumer groups: broker-side coordinator with join/sync/heartbeat, generation fencing, broker-computed (range) assignment. Membership is ephemeral; committed offsets are durable; the generation counter is persisted so fencing survives broker restarts.

---

## 7. Concurrency Principles

Go concurrency is a first-class deliverable, not an implementation detail.

Mechanisms in play: goroutines, channels, `sync.Mutex`, `sync.RWMutex`, `sync/atomic`, `context.Context`, `WaitGroup`, bounded queues.

Standing requirements:

- short critical sections,
- bounded queues everywhere,
- graceful shutdown with bounded drain,
- cancellation propagated end-to-end,
- no uncontrolled goroutine growth (leak checks in tests),
- race-detector validation (`go test -race`) on every concurrency-bearing component,
- clear ownership of mutable state (one writer per partition log; documented lock discipline).

---

## 8. Networking

The project must explicitly demonstrate understanding of: TCP streams, message framing, partial reads, partial writes, connection lifecycle, deadlines, timeouts, cancellation, and RPC concepts.

The broker itself uses **gRPC + Protocol Buffers**: unary RPCs (MVP), deadline propagation end-to-end, explicit status/error mapping, interceptors (recovery, logging, metrics), and the gRPC health protocol. Length-prefix framing is learned on raw TCP (Phase 2 labs + in-repo framing codec) and reused conceptually by the on-disk record format.

Default ports (configurable by flags): gRPC `:7000`, admin HTTP (metrics/pprof/healthz) `:7001`.

---

## 9. Observability

Observability answers real engineering questions — never dashboard decoration.

- Structured logs (`log/slog`), health/readiness, Prometheus, Grafana, pprof.
- Key metrics: throughput (msg/s, MB/s), append latency (p50/p95/p99), queue depth, consumer lag, recovery duration, replication lag (Phase 10), CPU, memory, goroutines, mutex/block contention.
- Every metric documented with the question it answers.

---

## 10. Failure Scenario Taxonomy

The project must eventually cover every scenario below. Phase 8 consolidates them into `docs/FAILURE_MATRIX.md`; storage/concurrency subsets are proven earlier (Phases 4–7). Scenarios marked *(P10)* are activated with replication.

| # | Scenario | Category |
| --- | --- | --- |
| 1 | Crash during append | storage |
| 2 | Crash before acknowledgement | storage / durability |
| 3 | Crash after acknowledgement | durability |
| 4 | Truncated record | storage |
| 5 | Corrupted record | storage |
| 6 | Corrupted index | storage |
| 7 | Metadata corruption | broker |
| 8 | Disk full | storage |
| 9 | Slow disk | storage / backpressure |
| 10 | Overloaded producers | backpressure |
| 11 | Slow consumer | isolation |
| 12 | Consumer crash | groups / redelivery |
| 13 | Malformed RPC | hardening |
| 14 | Network delay | networking |
| 15 | Network disconnect | networking |
| 16 | Follower lag *(P10)* | replication |
| 17 | Leader crash *(P10)* | replication |
| 18 | Zombie leader *(P10)* | replication |

Each important failure defines: failure injection → expected behavior → invariant → possible loss/duplication → recovery behavior → automated verification. Power-loss-class behavior that cannot be automated in CI is proven by construction (fsync-before-ack) and recorded as a documented assumption, never claimed as tested.

---

## 11. Performance Methodology

Performance is **measured, never guessed, never invented**.

- Measure: events/sec, MB/sec, p50/p95/p99, consumer lag, queue depth, CPU, memory, disk usage, recovery time.
- Benchmark methodology must record: hardware, OS, Go version, disk characteristics, record size, partition count, producer concurrency, acknowledgement mode, warmup, duration, number of runs, median/spread.
- Optimization loop (mandatory discipline):

```text
Hypothesis → Profile → Change → Benchmark → Compare → Keep or revert
```

- Any performance number published in docs/README must come from a recorded, reproducible run. Runs that cannot be executed are recorded as `NOT MEASURED` with the reason — never fabricated.

---

## 12. Technology Stack & Approved Dependencies

**Core:** Go (target 1.27.1), Linux/WSL2 (Ubuntu LTS, x86_64), Go standard library.

Standard-library pillars: `net`, `os`, `io`, `encoding/binary`, `hash/crc32`, `hash/fnv`, `sync`, `sync/atomic`, `context`, `testing` (+ fuzzing), `log/slog`, `net/http/pprof`, `runtime/debug`.

**Approved third-party dependencies (complete list):**

| Dependency | Purpose | Introduced |
| --- | --- | --- |
| `google.golang.org/grpc` | broker RPC API | Phase 5 |
| `google.golang.org/protobuf` (+ `protoc-gen-go`, `protoc-gen-go-grpc` tools) | API schemas/codegen | Phase 5 |
| `github.com/prometheus/client_golang` | metrics exposition | Phase 8 |

Anything else requires the owner's explicit authorization before it enters `go.mod`.

**Observability/deployment:** Prometheus, Grafana, Docker, Docker Compose (Phase 8+). **CI:** GitHub Actions is deferred; GitHub is not connected and must not be connected unless explicitly requested.

---

## 13. Explicit Exclusions

Never added merely to sound impressive:

React dashboard as a core requirement · PostgreSQL as event storage · Redis · Elasticsearch/OpenSearch · LLM/AI features · Kubernetes in the MVP · cloud dependency for correctness · Kafka protocol compatibility · schema registry · stream-processing DSL · Kafka Connect · cross-region architecture · multi-tenancy · billing · cross-partition transactions · exactly-once processing · automatic leader election · consensus · fake Raft · "production-ready" claims.

Additional MVP-scoped exclusions: TLS/auth, streaming RPCs, auto-commit, partition-count resize, compression, offset-log compaction, cooperative rebalancing, OpenTelemetry tracing.

---

## 14. Development Environment

Target environment:

```text
Windows host + WSL2 (Ubuntu LTS, x86_64)
Go 1.27.1 (record the actually installed version; report discrepancies)
Docker Desktop (WSL2 integration)
Git (local only — no remote, no GitHub connection)
```

- Primary repository (intended): `~/projects/relayforge` on the **WSL2 ext4 filesystem**. Labs: `~/rf-labs`.
- All broker file-I/O testing, crash testing, and benchmarking **must run on Linux ext4 in WSL2**.
- **Never run real broker storage tests or benchmarks against `/mnt/c`** (9p filesystem — misleading I/O characteristics). The Windows checkout (e.g., `C:\Projects\RelayForge`) is an editing location only; the WSL2 ext4 clone is the execution repository.
- Phase 1 verifies and records the actual environment in `docs/ENVIRONMENT.md`; every later phase assumes that record exists.

---

## 15. MVP Boundary

**The MVP is complete at the end of Phase 9.** It contains, at minimum:

Go broker · gRPC/Protobuf · CLI · topics · partitions · ordering · custom binary records · checksums · append-only segments · sparse index · retention · recovery · producer acknowledgements · consumer offsets · at-least-once semantics · consumer groups · bounded queues · backpressure · Prometheus · Grafana · pprof · race testing · integration testing · fuzzing · fault injection · crash/restart testing · reproducible benchmarks · Docker Compose demonstration.

Phase 10 is the advanced replication / final integration phase and is **not** part of the MVP. Phase 10 must define — and honestly land on — either the full replication boundary or its validated fallback (see Phase 10 scope).

---

## 16. Portfolio Demonstration

The final system demonstrates **behavior**, not UI. Target demo topology:

```text
3 consumers
     |
     v
RelayForge broker(s)
     |
Prometheus + Grafana
```

Demonstration steps (final script lives in `docs/DEMO.md`, executed in Phase 10):

1. produce events
2. show partitions
3. show consumers processing
4. pause a consumer
5. show consumer lag increasing
6. show bounded memory/backpressure behavior
7. restart consumer
8. show catch-up
9. crash/restart broker
10. show recovery
11. show benchmark results
12. show p95/p99 latency
13. if Phase 10 replication is complete: show leader failure and manual follower promotion

---

## 17. Phase Overview

| Phase | Title | Headline deliverable |
| --- | --- | --- |
| 1 | Go Foundations + Development Environment | Verified WSL2 toolchain, repository scaffold, conventions, `docs/ENVIRONMENT.md` |
| 2 | Networking Fundamentals + Binary Protocol Concepts | TCP/framing labs in `~/rf-labs` + in-repo `internal/framing` length-prefix codec |
| 3 | Binary Record Format + Append-Only Storage | `internal/record` + single-file log engine + `docs/RECORD_FORMAT.md` |
| 4 | Durability + Crash Recovery + Segments + Indexes + Retention | Segment/index engine, recovery + retention, crash harness, `docs/STORAGE.md` |
| 5 | Broker v1 + gRPC + Protobuf + CLI | `rfbroker` + `relayctl`, v1 API over default topic, `docs/PROTOCOL.md` |
| 6 | Topics + Partitions + Ordering + Concurrency + Backpressure | Multi-topic/partition produce path, bounded queues, durable metadata, `docs/ARCHITECTURE.md` |
| 7 | Consumers + Offsets + At-Least-Once + Consumer Groups | Group coordinator, offset manager, fencing, reference consumer |
| 8 | Observability + Reliability + Fault Injection + Hardening | Metrics/pprof/health, Compose observability stack, `docs/FAILURE_MATRIX.md` |
| 9 | Benchmarking + Profiling + Performance Engineering | `rfbench` harness, real recorded benchmarks + one optimization loop, `docs/BENCHMARKS.md` — **MVP complete** |
| 10 | Replication + Manual Failover + Final Integration + Portfolio Demo | Leader/follower + ISR + HW + `acks=all` + manual failover (or validated fallback), executed demo |

Guarantee → phase map (used in completion reports):

| Guarantee | Established | Hardened/verified |
| --- | --- | --- |
| Durability (ack modes) | P4 (engine), P5 (API) | P8 fault matrix |
| Recovery (valid prefix) | P4 | P5 broker-level, P8 |
| Ordering (per-partition) | P6 | P8 soak |
| Backpressure / bounded memory | P6 | P8, P9 |
| At-least-once + consumer groups + fencing | P7 | P8 |
| Replication (limited, manual) | P10 (or validated fallback) | P10 |

---

## 18. Phase Execution Contract

For every phase, "Execute Phase N" means the full cycle below — with no manual subtask orchestration by the owner:

```text
inspect current implementation
        ↓
understand previous phase
        ↓
implement everything in the phase
        ↓
write/update tests
        ↓
run tests
        ↓
run race tests where relevant
        ↓
perform failure/recovery verification
        ↓
inspect resulting repository
        ↓
verify Definition of Done
        ↓
report completion
        ↓
STOP
```

The precise behavioral rules, the mandatory report format, and the stop question are defined in `IMPLEMENTATION_PLAN.md`.

---

## 19. The Ten Phases

### Phase 1 — Go Foundations + Development Environment

#### Purpose

Establish the verified Linux/WSL2 development environment and the repository skeleton every later phase builds on, and lock in engineering conventions (module layout, formatting, vet, race testing, error idioms, test style) before any broker code exists. A systems project's correctness story starts with a controlled environment — real file I/O on ext4 — and a disciplined baseline.

#### Engineering concepts

- Go modules and project layout (`cmd/`, `internal/`); module path selection (recorded decision, default `relayforge`).
- Toolchain discipline: `gofmt`, `go vet`, table-driven tests, stdlib-only testing (no assertion frameworks).
- Race detector basics: what `-race` detects and its cost.
- Error-handling idioms: wrapping, sentinel errors, `errors.Is/As`.
- `context.Context` conventions; `log/slog` basics.
- WSL2 networking and filesystem realities: ext4 vs `/mnt/c` (9p) I/O characteristics; why broker I/O must be on ext4.
- Local git discipline (init, identity, commit hygiene; no remote).

#### Implementation scope

1. Verify the toolchain **inside WSL2**: `go version`, `git --version`, `docker version`, distribution. Record actual values in `docs/ENVIRONMENT.md` (never assume; report discrepancies vs Go 1.27.1 target).
2. Ensure the primary working clone sits on WSL2 ext4 at `~/projects/relayforge`; if work starts from the Windows checkout, create/verify the WSL clone and designate it the execution repo. Record both paths in `docs/ENVIRONMENT.md`.
3. Initialize the repository: `git init` (local only — **no remote**), `.gitignore`, `go.mod` (module path decision documented), `README.md` (identity, current status, truthful build/run instructions).
4. Create `internal/version` (version/commit/build info via `runtime/debug`) with a table-driven test — the first real package and the convention exemplar.
5. Add a `Makefile` with working targets: `build`, `test`, `test-race`, `vet`, `fmt`, `fuzz` (short budget), each runnable from WSL2.
6. Write `docs/ENVIRONMENT.md` (real versions, paths, filesystem, Docker status). Create no other docs yet.

#### Files/components expected

`go.mod`, `Makefile`, `.gitignore`, `README.md`, `docs/ENVIRONMENT.md`, `internal/version/` (+ test). `PLAN.md` / `IMPLEMENTATION_PLAN.md` already exist. Tree stays minimal — no speculative directories.

#### Dependencies

None (first phase).

#### Tests

- `internal/version` unit test (table-driven, subtests) — trivial by design; the deliverable is the working toolchain and conventions.
- All baseline commands demonstrated green under `-race` where applicable.

#### Failure cases

- Missing/mismatched Go version; WSL2 not installed or Docker not integrated (report; do not fake).
- Repository located on `/mnt/c` (must not become the execution repo; document why).
- Git identity unconfigured; `make` target failures.

#### Verification

From WSL2, all executed and recorded: `go build ./...`, `go vet ./...`, `gofmt -l .` (empty output), `go test ./...`, `go test -race ./...`, each `make` target, `git log` showing the initial commits.

#### Definition of Done

- Repository initialized on ext4 with module + `internal/version` + Makefile.
- All Phase-1 verification commands pass from WSL2.
- `docs/ENVIRONMENT.md` records the real toolchain, paths, and filesystem.
- README states identity and status truthfully; work committed locally.

#### Deliverables

Initialized repository, `internal/version` + test, `Makefile`, `README.md`, `docs/ENVIRONMENT.md`, local git history.

#### Explicit non-goals

No broker code, no gRPC, no networking, no storage code, no CI, no third-party dependencies, no labs as deliverables (optional `~/rf-labs` warmups permitted but not required).

#### Exit gate

All Phase-1 verification green on the WSL2 clone; conventions documented; git history started.

---

### Phase 2 — Networking Fundamentals + Binary Protocol Concepts

#### Purpose

Build the networking intuitions the broker depends on — TCP byte streams, framing, partial reads/writes, deadlines, connection lifecycle, graceful shutdown — before gRPC abstracts them away. Produce labs in `~/rf-labs` plus a reusable in-repo length-prefix framing package whose discipline the on-disk record format (Phase 3) will follow.

#### Engineering concepts

- TCP as an unframed byte stream; why explicit framing is required.
- Length-prefix framing; hostile length fields; `MaxFrameSize` as an allocation-bomb guard.
- Partial reads/writes; `io.ReadFull` / `io.CopyN` semantics; short writes.
- Deadlines (`SetDeadline`/`SetReadDeadline`/`SetWriteDeadline`) and their interaction with `context` cancellation.
- Goroutine-per-connection; `WaitGroup` lifetime management; graceful listener shutdown (unblocking `Accept`, double-close semantics).
- Error taxonomy: clean EOF vs truncated frame vs deadline vs connection reset.
- Write coalescing / buffering awareness (TCP_NODELAY, `bufio` trade-offs) — conceptual.

#### Implementation scope

1. **Labs (`~/rf-labs`, outside the module):** TCP echo server + client with graceful SIGINT shutdown; concurrent clients; deadline/cancellation demos; a concurrently served connection handler. Labs include README notes on observed behavior.
2. **In-repo `internal/framing`:** `WriteFrame(w io.Writer, payload []byte)` writing a 4-byte little-endian length prefix + payload (complete writes guaranteed); `ReadFrame(r io.Reader, max int)` using `io.ReadFull`, distinguishing `io.EOF` (clean boundary) vs `io.ErrUnexpectedEOF` (truncated mid-frame) vs `ErrFrameTooLarge` — **with no unbounded allocation** on hostile lengths.
3. Tests per the Tests section; fuzz `ReadFrame` with committed seed corpus (`testdata/fuzz`).
4. `docs/FRAMING.md`: wire layout (4-byte LE length + payload), size limits, error taxonomy, and rationale. (Phase 3 may merge it into `docs/RECORD_FORMAT.md` — either way content must survive.)
5. README status update.

#### Files/components expected

`internal/framing/` (+ tests, fuzz corpus), `docs/FRAMING.md`, `~/rf-labs` TCP programs. No broker/RPC code.

#### Dependencies

Phase 1 (toolchain, conventions, module).

#### Tests

- Round-trip; chunked-write reassembly (frames delivered in random-size chunks across an `io.Pipe`); per-byte truncation matrix (truncate a valid frame at **every** byte offset → correct error, never a bad payload); oversized declared length → `ErrFrameTooLarge` with bounded memory; empty-payload frame; failing/short writer paths.
- `go test -fuzz` on `ReadFrame` for a fixed short budget (recorded), seeds committed; invariant: no panic, error taxonomy respected, allocation bounded by `max`.
- Labs compile and run; manual smoke notes (echo round-trip, clean Ctrl-C).

#### Failure cases

Truncated frame at every byte position; oversized/absurd length prefix; zero-length frame; deadline expiry mid-frame; connection reset mid-read (lab level).

#### Verification

`go build ./...`, `go vet ./...`, `gofmt -l .`, `go test ./...`, `go test -race ./...`, `make fuzz` (recorded result), docs-vs-code review, labs smoke-run recorded.

#### Definition of Done

- `internal/framing` complete with full error taxonomy, tests, and fuzz seeds.
- Labs demonstrate TCP lifecycle + graceful shutdown and are documented.
- `docs/FRAMING.md` matches the code exactly; all verification green incl. `-race`; committed.

#### Deliverables

`internal/framing`, `docs/FRAMING.md`, fuzz corpus, `~/rf-labs` TCP programs, updated README.

#### Explicit non-goals

No gRPC, no broker, no record format (framing must not embed CRC/version — that is Phase 3's format), no TLS, no broker API protocol design.

#### Exit gate

Fuzz clean for its budget; per-byte truncation tests pass; race clean; labs documented.

---

### Phase 3 — Binary Record Format + Append-Only Storage

#### Purpose

Implement the heart of the system: the record format v1 exactly as specified in §4.1 and the single-file append-only log engine with append/read/scan semantics, offset management, and first torn-tail handling. No segments, indexes, or retention yet — those are Phase 4.

#### Engineering concepts

- On-disk binary format design: fixed header + variable body; little-endian via `encoding/binary`; `version` as a forward-compatibility door.
- CRC32C (Castagnoli): what it protects, what it cannot (adversarial corruption), and its exact coverage (§4.1).
- Offset semantics: per-log, 0-based, contiguous sequence numbers — distinct from file positions.
- Append-only write path: single-writer discipline; write vs fsync durability states (full ack-mode semantics mature in Phase 4).
- Read path: sequential scan; `ReadFrom(offset, maxBytes)`; `ErrOffsetOutOfRange` vs "offset at/beyond end" distinction.
- Max record size enforcement (bounded memory); optional key; zero-length value allowed.
- Concurrency: mutex-guarded single writer; concurrent readers never observe torn records.
- Property-style testing: encode→decode identity; prefix invariance under truncation.
- Fuzzing the decoder.

#### Implementation scope

1. `internal/record`: `Record` type (Key, Value, Timestamp, Attributes), `EncodeTo`/`DecodeFrom` (stream + at-position variants), `Validate`, size math per §4.1, CRC32C over the specified body, version/attributes validation, `MaxRecordSize` enforcement (default: 4 MiB total record, configurable).
2. `internal/logengine` (single-file Log): `Open(path, opts)`, `Append(rec, opts{Sync bool}) (offset, error)`, `ReadFrom(offset, maxBytes)`, full-validation `Scan`, `NextOffset`/`LastOffset`, `Close`. In-memory offset→position map **documented as a temporary device** (replaced by segments + sparse index in Phase 4).
3. Minimal recovery-on-open: scan to the last valid prefix; truncate a torn/corrupt tail (strict, simple, scan-based). Full recovery machinery + fault injection is Phase 4.
4. Timestamp policy: broker-set at append when zero (documented).
5. Durability hook: `Sync` option performs `file.Sync()` before `Append` returns; the engine never reports success before the requested durability state is reached.
6. `docs/RECORD_FORMAT.md` (authoritative byte table, endianness, CRC scope, version policy, timestamp semantics, limits; absorb or cross-reference `docs/FRAMING.md`).
7. README status update.

#### Files/components expected

`internal/record/` (+ tests, fuzz corpus, golden data), `internal/logengine/` (+ tests), `docs/RECORD_FORMAT.md`.

#### Dependencies

Phases 1–2 (framing discipline, conventions, module).

#### Tests

- **Golden byte-layout test:** exact hex encoding of a known record — pins the format against accidental change; reviewed against the docs table.
- Round-trip property tests (random keys/values incl. empty and max-size edges); offsets strictly +1.
- CRC corruption matrix: flip each body byte → decode fails cleanly.
- Version rejection; attributes-must-be-zero; max-size enforcement; fuzz `DecodeFrom` (no panic, bounded allocation).
- Log: append/read round-trips; `ReadFrom` mid-log; `ErrOffsetOutOfRange`; empty-log behavior; concurrent appends (serialized, unique offsets, `-race`); concurrent readers during appends (complete records only, never torn).
- Torn tail: append partial bytes to a file, reopen → truncated to last valid prefix, offset continuity preserved.
- Sync semantics: flushed data readable after `Close` + reopen from disk (documented limit: this proves write-path correctness, not power-loss — see Phase 4).
- Informational micro-benchmarks (`go test -bench`) for encode/append — recorded, not optimized.

#### Failure cases

Torn tail on reopen (tested); corrupted middle record (strict error surfaced; full policy lands in Phase 4); oversized length field in file (reject, no allocation bomb); empty/garbage file; disk-full during append (error propagated; offset not advanced; log state consistent).

#### Verification

`go test ./...`, `go test -race ./...`, `make fuzz` (recorded), vet/fmt clean, golden test reviewed against `docs/RECORD_FORMAT.md`, doc-vs-code audit.

#### Definition of Done

Record codec + single-file log per the format table; property/golden/fuzz/race suites green; torn-tail-on-open implemented and tested; `docs/RECORD_FORMAT.md` authoritative and accurate; committed.

#### Deliverables

`internal/record`, `internal/logengine`, `docs/RECORD_FORMAT.md`, fuzz corpus + golden data, README update.

#### Explicit non-goals

No segments/sparse index/retention (Phase 4), no broker/RPC (Phase 5), no group commit, no compression (attributes bit reserved, unimplemented), no mmap, no multi-file layouts, no producer/consumer concepts.

#### Exit gate

Fuzz clean; truncation/corruption suites pass; race clean; format doc verified against golden bytes.

---

### Phase 4 — Durability + Crash Recovery + Segments + Indexes + Retention

#### Purpose

Turn the single-file log into a real storage engine: segment rolling, sparse indexes for offset lookup, the durability ladder with explicit guarantees, retention, and — critically — crash recovery to the longest valid prefix, proven by automated fault injection and a real SIGKILL crash harness.

#### Engineering concepts

- Segment model: base-offset naming (`<base>.log` / `<base>.index`), active vs sealed segments, rolling by size, sealed-segment immutability.
- Sparse index: entries every N bytes, offset→position, binary search (`sort.Search`), `ReadAt` random reads, rebuild-from-log.
- Durability ladder: write (page cache) vs flush (fsync) vs none; what each survives; fsync error reporting on Linux and the degraded-state policy.
- Group-commit concept (optional here, correctness-first): batch fsync, ack strictly after durable.
- Crash recovery: length framing + CRC + version validation; truncate-to-valid-prefix; torn tail vs corrupted-middle policy; index rebuild; recovery-duration hook (formal metric in Phase 8).
- Retention: segment-granularity deletion (age/size), never the active segment; `ErrOffsetOutOfRange` for deleted ranges.
- Torn records and partial writes at the OS level; why prefix truncation is the only safe response.
- Testing crash behavior without real crashes: fault-injection seams (wrapper file layer) + subprocess SIGKILL harness. Honest limit: process-crash tests prove `written` durability; power-loss durability is by construction (fsync-before-ack) + documented assumptions.

#### Implementation scope

1. Rework `internal/logengine` into a segment set: `MaxSegmentBytes` rolling; reads routed via indexes; active segment appends unchanged semantically from Phase 3.
2. Sparse index: `(offset, position)` entries every `IndexIntervalBytes`; binary-search lookup; **derived-state principle** — corrupt/missing index → rebuild, never serve wrong data.
3. Durability API: `Write` (default) / `Flush` (fsync before ack) / `None`; batched sync allowed iff ack-after-durable holds; fsync-error policy: propagate, mark degraded until a successful re-sync, never ack `flushed` without a completed fsync (documented in `docs/STORAGE.md`).
4. Recovery on open: full validation scan of the active segment from its base; size/index consistency checks on sealed segments; truncate invalid active tail; rebuild indexes; corrupted-middle-of-active policy = truncate from first invalid record (documented); deep-verify utility for tests.
5. Retention: age and/or size policies, segment granularity, oldest-sealed-first; safe deletion (close handles, remove file pair); removed ranges → `ErrOffsetOutOfRange`; periodic bounded trigger.
6. Fault-injection seams + **crash harness**: test-only file-layer wrapper (inject truncation mid-record, byte corruption, partial writes, ENOSPC, latency) + subprocess child that appends records and is SIGKILLed at controlled/random points, with parent-side reopen verification.
7. `docs/STORAGE.md`: segments, indexes, step-by-step recovery algorithm, retention, durability ladder + assumptions, config knobs. Seed `docs/GUARANTEES.md` durability/recovery sections with explicit assumption boundaries.
8. README status update.

#### Files/components expected

Extended `internal/logengine/` (segments, index, recovery, retention), fault-injection test utilities (e.g., under `internal/logengine` test helpers or `internal/faulttest`), crash harness, `docs/STORAGE.md`, `docs/GUARANTEES.md` (seed).

#### Dependencies

Phases 1–3 (record codec, single-file log, conventions).

#### Tests

- Segments: rolling at threshold; offset→segment routing; reads across boundaries; sealed segments never appended.
- Index: lookup property test (random appends → every offset resolves to the correct record); interval effects; rebuild produces an equivalent index; corrupt/missing index → rebuilt on open, reads correct.
- Recovery: **truncation matrix** — truncate the log file at every byte boundary, reopen → longest valid prefix preserved, next offset correct, subsequent appends continue the sequence; corrupted tail bytes → truncated; corrupted middle of active segment → truncate-from-first-bad policy verified; partial-write injection.
- Crash harness: child appends N records (mixed modes), SIGKILL at varying points, parent reopens → prefix invariant holds; every record acked as `flushed` or `written` is present after restart (process-crash semantics; `ack=none` records asserted only via the prefix invariant).
- fsync-before-ack: seam test asserts `Sync()` completed before `Append` returns in flush mode (and not in written mode).
- ENOSPC injection: append error, no offset advance, no ack, log remains readable.
- Retention: age/size triggers; active segment never deleted; deleted ranges → `ErrOffsetOutOfRange`; retention concurrent with readers (`-race`, no hangs).
- All suites under `go test -race`.

#### Failure cases

Crash during append; crash before/after acknowledgement (harness); truncated record; corrupted record; corrupted index (rebuilt); disk full; slow disk (latency injection → slow appends, zero correctness change); metadata corruption is deferred to Phase 6 (no topic metadata exists yet — noted in the storage docs).

#### Verification

Full suite + `-race`; crash harness executed multiple iterations (record seed/iteration count); fuzz still clean; docs-vs-code audit for `docs/STORAGE.md` and `docs/GUARANTEES.md`; vet/fmt clean.

#### Definition of Done

Segment + index + recovery + retention engine passes all the above; durability ladder verified (fsync ordering + crash harness); recovery proven to the longest valid prefix; `docs/STORAGE.md` + seeded `docs/GUARANTEES.md` accurate; committed.

#### Deliverables

Reworked log engine, fault-injection utilities, crash harness, `docs/STORAGE.md`, `docs/GUARANTEES.md` (seed), README update.

#### Explicit non-goals

No compression, no mmap, no group-commit tuning (P9 may optimize with measurements), no topic metadata, no broker/network, no exotic syscalls beyond `fsync`/`fdatasync` (pick one, document, move on), no power-loss claims beyond documented assumptions.

#### Exit gate

Valid-prefix invariant proven by the truncation matrix + SIGKILL harness; every corruption class handled per documented policy; race clean; docs accurate.

---

### Phase 5 — Broker v1 + gRPC + Protobuf + CLI

#### Purpose

Put a real, versioned network API on the storage engine: a gRPC service with Protocol Buffers, unary RPCs with deadline propagation, explicit status mapping, interceptors, the health protocol, and the `rfbroker`/`relayctl` binaries. At this phase the broker serves **one implicit default topic with a single partition**; topics/partitions arrive in Phase 6 — but the protobuf API carries topic/partition fields from day one so it never churns.

#### Engineering concepts

- Protobuf schema design and additive evolution; protoc codegen pipeline (`protoc-gen-go`, `protoc-gen-go-grpc`, `go:generate`); generated-code hygiene.
- gRPC server lifecycle: `Serve`/`GracefulStop` (with timeout → `Stop` fallback), explicit max message sizes both directions (bounded memory).
- Deadline propagation end-to-end: RPC deadline → append path → fsync wait.
- Status-code mapping: `InvalidArgument` (validation), `NotFound` (unknown topic/range), `OutOfRange` (offset), `ResourceExhausted` (size limits; overload later), `Internal` (unexpected), `Unavailable` (retryable/overload). Documented in `docs/PROTOCOL.md`.
- Interceptors: panic-recovery (→ `Internal`, server survives), request logging (`slog`); metrics interceptor arrives Phase 8.
- gRPC health protocol (`grpc_health_v1`): serving/not-serving semantics.
- `bufconn`-based integration testing over the real engine on temp dirs.
- CLI design: subcommands, flags, deterministic exit codes; client timeout handling. TLS and auth are explicit non-goals (documented).

#### Implementation scope

1. `api/proto/relayforge/v1/`: service `RelayForge` with `Produce` (batched records, `ack_mode` enum `NONE/WRITTEN/FLUSHED`, default `WRITTEN`; per-record offsets in response) and `Fetch` (`topic`, `partition`, `offset`, `max_bytes`, `max_wait_ms`). Messages carry topic/partition from v1; broker v1 enforces `topic == "default"` and `partition == 0` (auto-provisioned), rejecting others with clear errors until Phase 6.
2. Codegen pipeline pinned and idempotent; generated code committed; `docs/PROTOCOL.md` documents the API, status mapping, and versioning policy.
3. `internal/broker` v1: wraps one engine log (default topic, single partition). Produce: validation (sizes, ack mode) → append per mode → offsets. Fetch: long-poll — if no data at offset, wait up to `max_wait` for arrival (append-notification); semantics for fetch-at-end (waits) vs fetch-beyond-end (`OutOfRange`) chosen and documented.
4. Server wiring: max receive/send sizes, recovery + logging interceptors, health service, graceful shutdown that drains in-flight RPCs (bounded).
5. `cmd/rfbroker`: flags (listen addr default `:7000`, admin addr `:7001` reserved, data dir, MaxSegmentBytes, MaxRecordSize), version flag, SIGTERM/SIGINT graceful shutdown, `slog` setup.
6. `cmd/relayctl`: global flags (addr, timeout); subcommands: `produce` (args/stdin/file, `--key`, `--ack`), `fetch` (`--offset`, `--max-bytes`, `--max-wait`), `health`, `version`. Deterministic exit codes; plain-text output.
7. Integration tests via `bufconn` + real engine in `t.TempDir()`; CLI smoke tests spawning `rfbroker` on a temp dir + random port and driving `relayctl` via `exec`.
8. `docs/GUARANTEES.md`: ack modes now user-visible — exact semantics per mode + the first at-least-once duplicate scenario (producer timeout after append → retry duplicates). README quickstart with commands that actually work.

#### Files/components expected

`api/proto/` + generated code, `internal/broker/`, `cmd/rfbroker/`, `cmd/relayctl/`, `docs/PROTOCOL.md`, updated `docs/GUARANTEES.md`.

#### Dependencies

Phases 1–4. First use of approved deps `google.golang.org/grpc` + `google.golang.org/protobuf`.

#### Tests

- Broker unit tests: validation, offset semantics, ack-mode dispatch.
- Integration (bufconn): produce/fetch round-trips; ack-mode behavior (flushed data readable by a **newly opened** engine instance); deadline propagation (fetch `max_wait` honored; produce deadline mid-append → appended-or-error, **never a phantom ack**); status-mapping table test; message-size limits enforced with the connection remaining usable; health service; concurrent producers/fetchers; graceful shutdown (in-flight RPCs complete or fail cleanly, no hang).
- CLI smoke: `rfbroker` + `relayctl` end-to-end (`exec`-driven, exit codes asserted).
- Adversarial validation values (negative offsets, zero `max_bytes`, oversized batches) — unit-tested; record-format fuzz stays green.
- Race on all integration tests.

#### Failure cases

Client deadline mid-produce (no phantom ack; appended-anyway documented); fetch at/past end (documented wait vs error semantics); oversized record/batch → `ResourceExhausted`; unknown topic/partition until Phase 6 (clear error); broker SIGKILL mid-load → restart → engine recovery through the broker path.

#### Verification

Full suite + `-race`; protoc regeneration is diff-idempotent; CLI smoke green; README quickstart executed literally by the agent (recorded); docs-vs-code audit for `PROTOCOL.md`/`GUARANTEES.md`.

#### Definition of Done

Broker v1 serves the default topic per documented semantics; `rfbroker` + `relayctl` work end-to-end on WSL2 ext4; status mapping + limits tested; health serving; graceful shutdown proven; docs accurate; committed.

#### Deliverables

`api/proto` + generated code, `internal/broker` v1, `cmd/rfbroker`, `cmd/relayctl`, `docs/PROTOCOL.md`, `docs/GUARANTEES.md` durability update, integration + CLI suites, README quickstart.

#### Explicit non-goals

No multi-topic/multi-partition (Phase 6), no consumer groups/offsets (Phase 7), no metrics (Phase 8), no TLS/auth, no streaming RPCs (unary only), no config files beyond flags, no Docker (Phase 8+), no schema registry.

#### Exit gate

CLI → broker → durable log → CLI fetch works on WSL2 ext4; guarantees so far documented; suite + race green; no Phase 6+ features leaked.

---

### Phase 6 — Topics + Partitions + Ordering + Concurrency + Backpressure

#### Purpose

Build the broker's real shape: multi-topic, multi-partition produce/fetch with deterministic routing, per-partition ordering and independent logs, deliberate concurrency (per-partition writer loops over bounded queues), enforced backpressure with explicit overload behavior, and durable topic metadata with corruption handling.

#### Engineering concepts

- Partitioning as the unit of ordering/parallelism; **no cross-partition ordering** (documented and demonstrated).
- Deterministic key routing: FNV-1a (stdlib `hash/fnv`) mod partition count; keyless round-robin via atomic counter (fairness caveats documented).
- Metadata as critical state: CRC-protected versioned snapshot, atomic replace (temp file + rename + directory fsync), strict load validation, **refuse-to-start on unrecoverable corruption** (never guess or rebuild topics).
- Producer pipeline: RPC handler → per-partition **bounded queue** (channel) → dedicated writer goroutine → engine append (per ack mode) → completion delivered to the waiting RPC via a per-request future (channel). Batched fsync across a queue drain is permitted (ack-after-durable invariant).
- Backpressure: full queue → bounded wait (min(produce deadline, queue-wait config)) → explicit `ResourceExhausted` overload error (retryable, documented); a documented memory bound (queue depth × max record size).
- Ordering invariant: one writer per partition ⇒ append order = queue arrival order (FIFO channels).
- Graceful shutdown ordering: stop accepting → bounded drain → final sync → close logs → stop RPC; acked records are durable after shutdown; nothing is acked beyond its mode's promise.
- Lock discipline: short critical sections, clear ownership (writer goroutine owns its partition's log handle), `RWMutex` for metadata maps, atomic counters, context cancellation through the pipeline. Goroutine-leak checks as a standard test helper.

#### Implementation scope

1. Proto additions (additive): `CreateTopic{name, partition_count}` (fixed at creation; validated 1..MaxPartitions), `ListTopics`, `DescribeTopic`. Produce/Fetch honor arbitrary topics/partitions. `docs/PROTOCOL.md` updated.
2. `internal/metadata` (may live inside `internal/broker` if small): topic table (name → partitions; logs under `data/topics/<topic>/partition-<n>/`), CRC'd versioned snapshot with atomic replace + dir fsync, load-on-start with strict validation, refuse-to-start on corruption with an error naming the file and failure.
3. Partition manager: per-partition struct (log, bounded queue, writer goroutine, lifecycle), routing function (documented hash + tests), keyless round-robin, fixed partition count (resize is a non-goal).
4. Produce pipeline with backpressure and ack-mode handling: `none` → ack after enqueue (loss documented, **writer still appends**), `written` → after engine write, `flushed` → after fsync; overload → `ResourceExhausted` (retryable).
5. Fetch routed per partition; per-partition append notification for long-poll; independent offsets per partition (engine-level).
6. Graceful pipeline shutdown (bounded drain timeout; acked=durable after shutdown; stuck-full queue → bounded shutdown).
7. Concurrency audit pass: custom goroutine-leak test helper (NumGoroutine before/after with tolerance+retry — no new deps), `-race` across pipelines, ownership model documented.
8. `docs/ARCHITECTURE.md` (broker concurrency model: components, goroutines, channels, locks, ownership, shutdown ordering); `docs/PROTOCOL.md` (new RPCs, overload semantics, explicit "no global ordering" statement); `docs/GUARANTEES.md` ordering + backpressure sections; README update.
9. CLI: `topic create/list/describe`; `produce --topic --key`; `fetch --topic --partition`.

#### Files/components expected

Proto additions + regen, `internal/metadata/`, partition plumbing in `internal/broker/`, `docs/ARCHITECTURE.md`, doc updates, leak-check helper.

#### Dependencies

Phases 1–5.

#### Tests

- Routing: determinism property (same key → same partition); distribution sanity (no strict uniformity); keyless round-robin incl. counter wrap and single-partition; fuzz routing (any key bytes → valid partition, deterministic).
- Ordering: concurrent producers with per-key affinity → strict per-partition offset+1 order and per-key order preserved; cross-partition interleaving observed and documented as intentional.
- Backpressure: block the writer (fault injection) → queue fills → producers block until timeout → `ResourceExhausted`; queue never exceeds config under load (bounded memory); service resumes after drain.
- Metadata: create → persist → restart → topics intact; corrupt metadata → refuse-to-start with precise error; repeated creates are atomic; concurrent same-name creates → exactly one succeeds.
- Pipeline lifecycle: graceful shutdown with in-flight produces (acked ⇒ durable per mode); drain timeout; writer goroutines exit; leak checks pass.
- Ack modes through the pipeline (fsync-before-ack seam test for `flushed`).
- Writer-panic containment: a panicking writer marks its partition failed (`Unavailable` on that partition) without killing the broker (tested).
- Everything under `go test -race`.

#### Failure cases

Queue overload (tested); slow disk → pipeline backup → backpressure, no unbounded memory (tested); metadata corruption (refuse-to-start, tested); crash between enqueue and append (`ack=none` loss documented; `written`/`flushed` ack-after-durable invariant held); partition writer panic (contained, tested).

#### Verification

Full suite + `-race` + leak checks; property tests with recorded seeds; CLI end-to-end: create multi-partition topic → keyed produce → verify same-partition routing via fetch → concurrent load smoke → clean shutdown; docs-vs-code audit.

#### Definition of Done

Multi-topic/partition broker with routing, ordering, backpressure, and metadata per docs; overload + corruption + shutdown + panic-containment behaviors proven; concurrency model documented; committed.

#### Deliverables

Proto additions, `internal/metadata`, partition pipeline, `docs/ARCHITECTURE.md`, `PROTOCOL.md`/`GUARANTEES.md` updates, leak-check helper, new suites, README update.

#### Explicit non-goals

No consumer groups (Phase 7), no automatic partition-count changes, no compression, no batch-size tuning (Phase 9), no metrics (Phase 8), no cross-topic transactions, no client libraries beyond `relayctl`.

#### Exit gate

Ordering + backpressure + metadata-recovery suites green incl. `-race` and leak checks; policies match behavior; no group/offset features leaked.

---

### Phase 7 — Consumers + Offsets + At-Least-Once + Consumer Groups

#### Purpose

Complete the consumption side: durable committed offsets, group membership (join/sync/heartbeat), exclusive partition assignment per generation with fencing, rebalance on failure, and the at-least-once contract — with its duplicate consequences documented and demonstrated by tests.

#### Engineering concepts

- Fetch → process → commit; position vs committed offset; redelivery on crash-before-commit (intentional).
- Offset storage: commits per (group, topic, partition) in an **offsets log reusing the record format** (versioned, CRC'd payload); recovery = valid prefix (reusing engine recovery patterns); commit durability policy (default flushed — commits are low-rate; documented).
- Consumer groups: generation IDs, member IDs, session timeouts, heartbeats, rebalance on join/leave/timeout; **broker-computed range assignment** (documented simplification vs Kafka's client-leader sync); join barrier semantics.
- Fencing: commits carry (generation, member); stale → explicit fenced error; fencing prevents zombie commits after reassignment. **Generation counter persisted** so fencing survives broker restarts.
- Offset-reset policy explicit per fetch (`earliest`/`latest`); no auto-commit in MVP.
- Consumer lag = latest − committed (per partition); computed here, exposed as a metric in Phase 8.
- At-least-once honesty: duplicates from producer-timeout retries (Phase 5) and consumer crash-before-commit — both demonstrated in tests.

#### Implementation scope

1. Proto additions (additive): `JoinGroup` (empty member_id = new member; session timeout), `SyncGroup` (returns assignment), `Heartbeat` (ok | rebalance-in-progress), `CommitOffset` (group, generation, member, topic, partition, offset), `GetOffsets` (committed offsets for starting positions), `DescribeGroup`. Fenced commits map to a documented status code + reason.
2. `internal/offsets`: `OffsetManager` per group (offsets log under `data/groups/<group>/`), commit API (validate generation+membership → durable write → ack), load-on-start with valid-prefix recovery (torn tail truncated), committed-offset convention per §6.
3. `internal/group` (Group Coordinator): simplified Kafka-like state machine (documented), member registry, bounded heartbeat tracking (sweep or timer — documented, no per-heartbeat goroutine explosion), rebalance orchestration (generation bump → collect → broker-computed range assignment → hand out at sync), fencing at commit, empty-group expiry (offsets retained, documented), **persisted generation counter** (in metadata or offsets log — decided + documented).
4. Fetch path stays stateless/intentional: exclusivity is enforced by assignment + fencing, not by the fetch path (documented rationale).
5. `docs/GUARANTEES.md`: at-least-once formalized (both duplicate sources with concrete scenarios), fencing guarantees, rebalance semantics (in-flight processing at rebalance is the client's responsibility; redelivery possible). `docs/PROTOCOL.md`: group RPCs + fenced-error mapping.
6. CLI: `relayctl consume` (reference consumer loop with explicit `--commit-after N`), `group describe` (members, generation, assignments), `group offsets`.
7. Concurrency: bounded coordinator sweeps; leak checks; `-race`.

#### Files/components expected

Proto additions, `internal/offsets/`, `internal/group/`, CLI additions, doc updates.

#### Dependencies

Phases 1–6.

#### Tests

- Offsets: commit → restart → recovered; torn offsets-log tail → truncated + prefix recovered; fenced commits (wrong generation/member) rejected with the documented error; concurrent commits to the same partition serialize (last-writer-wins documented).
- Groups: full lifecycle (join → sync → stable → heartbeat → leave → rebalance); join triggers rebalance (generation bump); heartbeat timeout → rebalance + reassignment (short session timeouts in tests); **exclusivity property test** — randomized members joining/leaving; after each stabilization every partition appears in ≤1 member's assignment; fencing: a generation-G member committing after rebalance to G+1 gets the fenced error; **generation persistence across broker restart** proven.
- At-least-once integration: producer-timeout duplicate path (response lost → retry → duplicate present, documented as expected); consumer SIGKILL before commit → restart → redelivery of the uncommitted range; crash after commit → no redelivery of the committed range.
- Multi-consumer scripted run: 2–3 consumer processes on a multi-partition topic → balanced assignment; kill one → rebalance → full coverage maintained.
- Offsets-payload fuzz (versioned value payload); `-race` + leak checks throughout.

#### Failure cases

Coordinator restart mid-rebalance (membership is ephemeral; members rejoin; persisted generation prevents stale commits — tested); offsets-log corruption (valid-prefix recovery); rebalance during in-flight fetch (fetch completes; commit may be fenced — documented + tested); heartbeat storms (bounded sweeps).

#### Verification

Full suite + `-race` + leak checks; scripted multi-consumer integration recorded; docs-vs-code audit (`GUARANTEES.md` at-least-once language must match actual behavior); `relayctl consume` demo works.

#### Definition of Done

Groups/offsets/fencing/rebalance implemented and proven; at-least-once documented with both duplicate sources demonstrated in tests; generation persistence across restart proven; docs consistent; committed.

#### Deliverables

Proto additions, `internal/offsets`, `internal/group`, CLI consumer + group commands, `GUARANTEES.md`/`PROTOCOL.md` updates, new suites, README update.

#### Explicit non-goals

No exactly-once, no transactions, no auto-commit, no client-leader sync protocol, no static membership, no cooperative (incremental) rebalancing — eager only, no offsets-log compaction, no quotas, no cross-group coordination.

#### Exit gate

Exclusivity + fencing + redelivery + generation-persistence suites green (race + leaks); docs consistent; no replication features leaked.

---

### Phase 8 — Observability + Reliability + Fault Injection + Hardening

#### Purpose

Make the broker measurable and prove reliability under failure: Prometheus metrics that answer real questions, pprof, health/readiness, structured-logging maturity, a Docker Compose observability stack, a fault-injection framework with the consolidated failure matrix, and hardening against malformed input and resource exhaustion. No new broker features in this phase.

#### Engineering concepts

- Metrics semantics (counter/histogram/gauge); **label-cardinality discipline** (bounded label sets: topic/partition/group — never member IDs or unbounded strings); histogram buckets designed for latency questions.
- Prometheus exposition (`/metrics` via `client_golang`); Grafana dashboards as provisioned JSON reflecting real metric names.
- pprof on the admin port; actually reading goroutine/heap/CPU/mutex-block profiles under load (method, not endpoint existence).
- Readiness vs liveness: ready = recovered + queues healthy; documented transitions (gRPC health + `/healthz`).
- Fault-injection framework: file-layer seams (ENOSPC, short writes, latency, corruption, truncation), broker-level hooks, generalized subprocess SIGKILL crash harness.
- Failure-matrix methodology: injection → expected behavior → invariant → possible loss/duplication → recovery → automated verification (each row maps to a test or an explicit, justified "documented assumption").
- Hardening: field validation everywhere; explicit limits (max message size, batch size, partitions per topic, groups, gRPC `MaxConcurrentStreams`); slow-client isolation; allocator bounds.

#### Implementation scope

1. `internal/observability`: metrics registry (minimum set below), gRPC metrics interceptor, HTTP admin server (`:7001`: `/metrics`, `/debug/pprof/*`, `/healthz`), readiness gating defined precisely.
   Minimum metric set: `messages_in_total`, `bytes_in_total`, `messages_out_total`, `bytes_out_total` (by topic/partition); `append_latency_seconds` (by ack_mode); `queue_depth` (by topic/partition); overload-rejection counter; `fetch_latency_seconds`; `consumer_lag` (by group/topic/partition); commit success/failure counters; `group_members` (by group); `group_generations_total`; `recovery_duration_seconds`; Go runtime + process metrics. Each documented with the question it answers.
2. Grafana dashboard(s) (`dashboards/*.json`) provisioned via Compose; reflect real metric names.
3. Docker: multi-stage `Dockerfile` (static binary; base image decision documented); `docker-compose.yml` with broker (data volume) + Prometheus (scrape config) + Grafana (provisioning) — the observability stack; full demo compose matures in Phases 9–10.
4. `internal/faulttest`: file-layer fault wrapper, broker fault hooks (test-only), generalized crash harness (reusing Phase 4's).
5. `docs/FAILURE_MATRIX.md`: all §10 rows as a table — injection method, expected behavior, invariant, possible loss/duplication, recovery, automated verification (test name, or "documented assumption" with reason, e.g., power loss). Phase-10 rows (follower lag, leader crash, zombie leader) written now, marked `DEFERRED(P10)`.
6. Implement/consolidate automated tests for all MVP-scope matrix rows (many exist from Phases 4–7; gaps closed here).
7. Hardening: validation audit on every RPC field (negative/zero/huge); limits enforced + tested; slow-consumer isolation test (tiny-`max_bytes` slow fetcher loops; producers unaffected; queue/memory stable); malformed-RPC fuzz-lite; bounded soak test (sustained produce/consume, memory + goroutines stable).
8. `docs/OBSERVABILITY.md` (metric catalog: name/type/labels/question); doc cross-links; README observability section.

#### Files/components expected

`internal/observability/`, `internal/faulttest/`, `dashboards/`, `Dockerfile`, `docker-compose.yml` + prometheus config, `docs/FAILURE_MATRIX.md`, `docs/OBSERVABILITY.md`, doc updates.

#### Dependencies

Phases 1–7; Docker Desktop available in WSL2. New approved dep: `github.com/prometheus/client_golang`.

#### Tests

- Metrics correctness (known ops → expected samples via the test registry); lag metric correctness (produce N, commit K → lag N−K); interceptor duration recording; readiness transitions.
- Fault-matrix tests — every MVP row automated.
- Hardening limits (each enforced + tested); slow-consumer isolation; malformed-input handling.
- Bounded soak test (duration sized for CI; results recorded).
- Compose smoke: `docker compose up` → Prometheus scrapes the broker → example queries return series (requires Docker; if unavailable, report honestly — do not claim it ran).
- `-race` + leak checks.

#### Failure cases

The matrix itself; plus observability-specific: `/metrics` under load (no stalls), scrape during recovery, pprof fetch during load with the broker alive.

#### Verification

Full suite + `-race` + leaks; Compose stack verified live (recorded output); matrix↔tests audit (every row mapped); soak results recorded; a real pprof profile fetched and briefly analyzed (top functions noted — real analysis); docs-vs-code audit.

#### Definition of Done

Metrics/dashboards/pprof/health live and correct; failure matrix complete with all MVP rows automated; hardening limits enforced and tested; Compose stack verified; docs accurate; committed.

#### Deliverables

Metrics package + interceptors, admin server, dashboard JSON, `Dockerfile` + Compose + Prometheus config, `internal/faulttest`, `docs/FAILURE_MATRIX.md`, `docs/OBSERVABILITY.md`, hardening + soak tests, README update.

#### Explicit non-goals

No tracing/OpenTelemetry, no alertmanager rules, no log aggregation stack, no Kubernetes, no new broker features, no replication metrics beyond reserved names.

#### Exit gate

Compose metrics demo working live; matrix fully mapped; race/leaks clean; all limits tested.

---

### Phase 9 — Benchmarking + Profiling + Performance Engineering

#### Purpose

Measure real performance with a reproducible methodology, profile under load, execute at least one documented optimization loop (hypothesis → profile → change → benchmark → compare → keep/revert), and finalize the Docker Compose demo environment. **The MVP is complete at the end of this phase.**

#### Engineering concepts

- Throughput vs latency vs resource ceilings; warmup effects; run-to-run variance; median/spread reporting; sufficient samples for p99; coordinated-omission awareness in producer-latency measurement (send→ack includes queue wait — methodology documented honestly).
- Percentile computation from sorted samples (documented accuracy; no new deps).
- Reproducibility: pinned environment recording (hardware, kernel, disk model/filesystem, Go version, commit), fixed parameters, repeat count, cold/warm notes.
- Profiling with pprof: CPU, heap, mutex/block contention, goroutines; interpreting profiles at literacy level.
- Optimization discipline: measure → profile → identify → change → re-measure → keep/revert; every change documented with numbers; no optimization without profile evidence.
- Relevant Go performance levers (measured, not vibes): allocation reduction, buffer reuse/`sync.Pool`, group-commit batching, syscall overhead, channel-vs-mutex handoff cost, index-interval tuning, gRPC copy costs.

#### Implementation scope

1. `cmd/rfbench` (load generator + benchmark driver): modes produce / consume / end-to-end; parameters: addr, topic, partitions, producers, record size, key distribution, ack mode, batch size, duration, warmup, consumers; outputs machine-readable JSON + human summary (counts, throughput, p50/p95/p99/max, errors, resource snapshots).
2. `bench/` measurement core (latency recorder, percentile math, environment recorder) with unit tests; fast micro-benchmarks (`go test -bench`): record encode/decode, framing, index lookup, queue submit — CI-friendly, informally tracked (no strict CI regression gating).
3. Recorded scenario benchmarks on WSL2 ext4: produce throughput+latency per ack mode × record size × producer count; consume throughput; end-to-end lag catch-up; recovery time vs log size (crash harness); overload behavior (rejection rate, latency cliff).
4. Profile under representative load; ≥1 complete optimization loop executed and documented with before/after numbers (kept or reverted — either outcome is a valid, valuable result).
5. `docs/BENCHMARKS.md`: methodology (warmup, duration, runs, median/spread), environment block, exact commands, raw results committed under `bench/results/<date>-<commit>/`, machine-specific disclaimer. **Unrun benchmarks are recorded `NOT MEASURED` with the reason — never fabricated.**
6. Compose demo environment finalized (broker + Prometheus + Grafana + loadgen profile); full multi-consumer demo staged for Phase 10.
7. README performance section linking `docs/BENCHMARKS.md` (claims only from recorded runs).

#### Files/components expected

`cmd/rfbench/`, `bench/` (+ results), `docs/BENCHMARKS.md`, Compose demo profile, README updates.

#### Dependencies

Phases 1–8 (metrics make benchmarks observable; harness uses the gRPC API).

#### Tests

- Harness unit tests: percentile math (empty, single, heavy tails), config parsing, environment-recorder completeness, monotonic-clock usage.
- Micro-benchmarks exist and run (short).
- A tiny end-to-end benchmark smoke run keeps the harness honest.
- Full existing suite + `-race` green (no correctness regressions from any optimization — **a kept optimization must not degrade guarantees; its tests still pass**).

#### Failure cases

Harness vs dead broker (clean errors); partial failures mid-run (counted and flagged — never silently dropped); disk-full mid-benchmark (clean abort + report).

#### Verification

Real benchmark runs executed and recorded with environment metadata; ≥1 optimization loop documented (before/after profiles + numbers); Compose demo stack up with loadgen + Grafana data (recorded); full suite + `-race` green; MVP checklist (§15) verified item by item.

#### Definition of Done

Reproducible benchmark commands documented and demonstrably run; `docs/BENCHMARKS.md` contains real numbers + methodology + environment; one profile-driven optimization documented; MVP feature set complete and verified; committed.

#### Deliverables

`cmd/rfbench`, `bench/` core + micro-benchmarks, `bench/results/` raw outputs, `docs/BENCHMARKS.md`, finalized Compose demo stack, README updates.

#### Explicit non-goals

No new broker features (pure measurement/optimization), no replication benchmarks (doesn't exist yet), no performance claims without recorded runs, no micro-optimization without profile evidence.

#### Exit gate

Numbers are real, reproducible, documented; optimization loop evidenced; MVP checklist fully green.

---

### Phase 10 — Replication + Manual Failover + Final Integration + Portfolio Demo

#### Purpose

The advanced capstone: leader/follower replication with ISR + high-watermark + `acks=all`, leader epochs with zombie-leader fencing, **manual** failover (explicit operator action — no automatic election, no consensus claims), plus final integration and the executed portfolio demonstration. If the full model cannot be safely completed, the phase must land the **explicitly defined validated fallback boundary** instead — and say so honestly in `docs/LIMITATIONS.md`.

#### Engineering concepts

- Leader/follower replication per partition with **static placement** (config declares broker IDs, peer addresses, partition→leader/follower roles).
- Replication protocol: epoch-aware follower fetch from leader; follower appends to its own log (same engine); follower LEO tracked by the leader.
- ISR: replicas within lag threshold (offset distance and/or time); ISR shrink/grow maintained by the leader; persisted/annotated for failover decisions.
- High-watermark: min(ISR LEOs); when replication is enabled, consumers read **only up to HW**; `acks=all` = ack at HW advance; leader-local `written`/`flushed` remain unreplicated (risk documented per mode).
- Leader epochs: monotonic per-partition epoch persisted with the log; epoch used to fence stale leaders and to truncate divergent follower tails to the epoch-consistent prefix on leader change.
- Zombie-leader fencing: an isolated old leader is rejected once fenced/demoted (MVP mechanism is admin/config-driven — the "limited" part of limited replication — documented honestly; automatic discovery is a non-goal).
- Manual failover: operator action (`relayctl`/admin RPC) with safety checks; forced-promotion flag for lagging followers with explicitly documented loss; epoch bump on promotion; clients re-pointed manually (documented).
- Data-loss accounting per ack mode × ISR state — stated precisely in `docs/LIMITATIONS.md`.
- `min.insync`-style policy: `acks=all` with ISR below the configured minimum → fails `Unavailable` after timeout — **it must never silently degrade to acks=written behavior.**

#### Implementation scope

1. Proto/config additions: replication fetch RPC, admin `PromoteLeader` (+ `DescribeReplication`), broker identity/leader-epoch fields where needed; static cluster config file (broker id, peers, partition placement).
2. `internal/replication` (or `internal/broker/replication`): leader path (follower progress, ISR maintenance, HW computation, `acks=all` gating, epoch stamping), follower path (fetch loop, local append, lag metric), persisted epoch table, epoch-based truncation on promotion, fencing checks on mutating paths.
3. Manual failover: promotion command with checks; epoch bump; old-leader demotion/fencing path; rejoin-as-follower behavior documented.
4. Metrics + dashboards: follower lag, ISR size, HW-vs-LEO gap, replication latency; Phase-8 reserved names activated.
5. Failure-matrix rows activated (follower lag, leader crash, zombie leader) with tests.
6. **Boundary decision (mandatory, made at execution time and stated in the report):**
   - **FULL boundary:** replication + ISR + HW + `acks=all` + manual failover + zombie fencing — all test-verified.
   - **FALLBACK boundary:** single-leader async replication (follower catch-up verified), manual promotion verified, `acks=all` returns `Unavailable("not supported: replication fallback mode")` while replication is enabled, possible tail loss documented, zombie fencing limited to reject-produce-after-demote.
   The report states which boundary was achieved; `docs/LIMITATIONS.md` matches reality. **Never present the fallback as the full model.**
7. Final integration: multi-broker Compose (2 brokers + Prometheus + Grafana + loadgen + 3 consumers); `docs/DEMO.md` with the 13 steps of §16 — executed and recorded.
8. Final documentation pass: `ARCHITECTURE`, `PROTOCOL`, `GUARANTEES`, `LIMITATIONS`, `FAILURE_MATRIX`, `BENCHMARKS`, README — audited against the implementation; portfolio-facing summary honest about scope.

#### Files/components expected

`internal/replication/`, proto/config additions, CLI failover commands, extended dashboards, `docs/REPLICATION.md`, updated `LIMITATIONS`/`FAILURE_MATRIX`/`GUARANTEES`, `docs/DEMO.md`.

#### Dependencies

Phases 1–9 (everything).

#### Tests

- Replication: follower catches up (follower prefix == leader prefix at HW — property test); HW advances only when ISR covers; `acks=all` acked records survive leader SIGKILL + promotion (integration kill test); unreplicated-tail loss with `acks=written` demonstrated and documented; ISR shrink on follower pause/stall; `acks=all` with ISR below minimum → `Unavailable` (never silent degradation); follower-lag metric correctness; epoch stamping + divergent-tail truncation on promotion.
- Failover lifecycle: leader SIGKILL → manual promote → produce/fetch continue on the new leader → old leader restarts → fenced (produce rejected) → documented rejoin path.
- Fencing unit tests on every mutating path (epoch checks).
- Demo smoke: scripted Compose demo executes; consumers distribute; lag observable; recovery observable; replication steps per achieved boundary.
- Full regression: entire suite + `-race` + leaks.

#### Failure cases

Leader crash; follower crash + catch-up; zombie leader (fencing); leader/follower network partition (injected → ISR shrink → defined `acks=all` behavior, tested); split-brain produce attempts (fencing); promotion of a lagging follower (forced flag; actual lost-record count reported honestly).

#### Verification

All suites + `-race` + leaks; replication kill/failover integration executed and recorded; Compose demo executed step-by-step per `docs/DEMO.md` (step 13 conditional on boundary); chosen boundary explicitly stated; final docs audit (each doc checked against behavior; discrepancies fixed); MVP + Phase-10 checklists verified.

#### Definition of Done

Either the FULL or the FALLBACK boundary achieved, tested, and stated; demo executed; docs final and honest; `docs/LIMITATIONS.md` explicit that replication is not consensus, not HA, not exactly-once; committed.

#### Deliverables

Replication implementation, admin/CLI failover commands, extended metrics/dashboards, `docs/REPLICATION.md`, updated `FAILURE_MATRIX`/`GUARANTEES`/`LIMITATIONS`, `docs/DEMO.md` + executed demo record, final README.

#### Explicit non-goals

**No automatic leader election · no consensus/Raft · no automatic failover · no exactly-once · no cross-partition replication transactions · no transparent client redirection · no multi-region · no "production-ready" claims.** Raft remains a separate post-project learning experiment, outside RelayForge.

#### Exit gate

Chosen boundary validated by tests; demo steps 1–12 executed (13 conditional on the replication boundary); documentation audit clean; all prior guarantees still verified by full regression.

---

## 20. Document Maintenance

- `PLAN.md` is the master roadmap. It is edited only when the owner requests it or when execution uncovers a genuine contradiction (which the agent reports before editing).
- Implementation status is tracked in the README status line and git history — not in per-phase planning files (which are forbidden by `IMPLEMENTATION_PLAN.md`).
- Engineering truth lives in `docs/`: `RECORD_FORMAT.md`, `STORAGE.md`, `GUARANTEES.md`, `ARCHITECTURE.md`, `PROTOCOL.md`, `FAILURE_MATRIX.md`, `OBSERVABILITY.md`, `BENCHMARKS.md`, `REPLICATION.md`, `LIMITATIONS.md`, `ENVIRONMENT.md`, `DEMO.md`.
