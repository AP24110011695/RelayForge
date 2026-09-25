# RelayForge — Implementation Plan (Agent Operating Manual)

**Document status:** Authoritative operating rules for the coding AI agent. Owner-approved.
**Companion document:** `PLAN.md` (the 10-phase master roadmap — read both before any work).
**Last updated:** 2026-09-26

This document is **not** another roadmap. It defines how the coding agent must behave whenever the owner says:

```text
Execute Phase N
```

`PLAN.md` answers *"what to build in each phase."* This document answers *"how the agent must work while building it."* If an instruction outside these two documents contradicts them, the agent flags the contradiction and asks — it does not silently pick one.

---

## 1. The Execution Contract

- Trigger phrases: `Execute Phase N` (optionally preceded by `Read PLAN.md and IMPLEMENTATION_PLAN.md.`).
- The agent executes **exactly one phase** end-to-end: inspect → implement → test → verify → report → **STOP**.
- The agent **never** begins Phase N+1 automatically. It always ends with the stop question in §10.
- Phases are executed strictly in order (1 → 10). Each phase assumes all previous phases exist.
- The agent must be able to do all of this from a **cold session** with no conversation history. All required context lives in `PLAN.md`, this document, `README.md`, `docs/`, and the git history — which is why keeping them current is mandatory, not optional.

---

## 2. Mandatory Phase Execution Protocol

When the owner says `Execute Phase N`, the agent performs **all** of the following, in order:

**Step 1 — Read the plans.**
Read `PLAN.md` and `IMPLEMENTATION_PLAN.md` in full. Do not rely on memory of a previous session.

**Step 2 — Read the repository.**
Perform the full repository inspection of §3.

**Step 3 — Identify the exact Phase N scope.**
Extract the phase from `PLAN.md` §19: scope items, tests, failure cases, verification, Definition of Done, non-goals, exit gate. Build an internal checklist from them and work against it.

**Step 4 — Implement the entire phase.**
Every scope item. No partial deliveries, no silent descoping, no features from other phases.

**Step 5 — Create/update tests.**
Tests per `PLAN.md` (phase Tests section) and §7 below. Features are incomplete without tests.

**Step 6 — Run all appropriate verification.**
Execute the real commands (§8) and record actual results.

**Step 7 — Review against the Definition of Done.**
Go through the phase's DoD item by item and confirm each one with evidence.

**Step 8 — Verify phase isolation.**
Check that no future-phase work was accidentally introduced (diff review against the phase's non-goals; see §5).

**Step 9 — Provide the completion report.**
Use the exact template in §11.

**Step 10 — STOP.**
End the turn with the closing question in §10. Do not execute Phase N+1.

### Re-execution semantics

If the owner says `Execute Phase N` for a phase that is already complete: re-inspect the repository, re-run the phase's verification, confirm the DoD still holds, and report status — without re-implementing. If it is partially complete, finish only the missing scope.

### Abort semantics

If a phase cannot be completed (broken environment, defect too large, missing prerequisite the owner must resolve): **STOP and report honestly** with status `INCOMPLETE — BLOCKED`, the diagnosis, and what is needed. Never fake completion. Never mark a phase complete with failing or skipped tests.

---

## 3. Repository Inspection (Before Modifying Anything)

Every phase execution starts with inspection. Do not assume the repository matches the intended architecture — verify.

Inspect:

1. `PLAN.md` and `IMPLEMENTATION_PLAN.md` (current versions).
2. Repository layout (`ls`/tree) — what actually exists.
3. `README.md` — especially the project status line (current phase).
4. `docs/` — all existing engineering documents relevant to the phase.
5. Existing source files relevant to the phase (read them; don't guess interfaces).
6. Existing tests — what is covered, what conventions they use.
7. `go.mod` — module path, current dependencies.
8. Git state if available: `git log` (recent phases/commits), `git status` (uncommitted work).
9. `docs/ENVIRONMENT.md` — toolchain, paths, filesystem notes.

Record findings briefly at the top of the working notes (the completion report summarizes what matters). If the repository state contradicts what the phase assumes, resolve per §9 (dependencies) or report per §2 (abort).

---

## 4. Existing Implementation — Preserve, Don't Rewrite

- Do not overwrite working code blindly. Preserve correct previous work.
- Modify existing components when appropriate; avoid unnecessary rewrites.
- Refactors are allowed only when the current phase requires them, and must be covered by the existing test suite staying green.
- If previous-phase code has a defect that blocks the current phase, fix the defect minimally, state it explicitly in the report ("fixed previous-phase defect: …"), and keep its tests green.
- Never redesign the project silently. Architecture changes require the owner's approval and a documented decision (§12).

---

## 5. Phase Isolation & Future-Work Containment

- Implement **only** the requested phase. Never start Phase N+1.
- If Phase N exposes a need that belongs to a later phase: **document it** (in the report's known limitations and, where appropriate, `docs/LIMITATIONS.md`) instead of implementing it.
- Small interface seams required *by the current phase* (e.g., a `Sync` option needed for ack modes) are fine; whole features from later phases are not.
- Quick containment check used in Step 8: read the phase's **Explicit non-goals** list and the next phase's **Implementation scope**, then diff the changeset against both. Anything matching the next phase goes out.

---

## 6. No Fake Implementation — Absolute Rules

Never create:

- placeholder functionality presented as complete,
- fake benchmark results or performance numbers,
- fake metrics or fake dashboards data,
- fake or unexecuted test results,
- mocked behavior pretending to be real infrastructure,
- documentation claiming guarantees the implementation does not have (including "production-ready" claims),
- stubbed error paths that pretend to handle failures,
- `NOT MEASURED` results silently turned into numbers.

Every claim in code, docs, README, and reports must be backed by something that actually exists and actually ran. When something is not done, say exactly that.

---

## 7. Tests

Features are incomplete without appropriate tests. The discipline:

```text
implement → test → intentionally test failure behavior where relevant → verify guarantee
```

Requirements:

- **Unit tests** for storage and core components (table-driven, stdlib `testing`; no assertion frameworks).
- **Integration tests** for broker-level behavior (`bufconn`, temp dirs via `t.TempDir()`, subprocess harnesses where real process boundaries matter).
- **Race testing**: `go test -race ./...` on every phase with concurrency; race findings are bugs, not warnings.
- **Property-style tests** for offsets, index lookup, encoding/decoding, partition routing (seeded RNG; record seeds so failures reproduce).
- **Fuzzing** per `PLAN.md` targets (record decoder, segment scanner, metadata/offset decoding, framing, malformed RPC inputs). Commit seed corpora under `testdata/fuzz`. Run each fuzz target for a fixed short budget each phase (`make fuzz`); record results.
- **Fault injection** tests verify actual failure behavior, not just happy paths — every important failure per `PLAN.md` §10 has an automated test or an explicit documented assumption.
- Tests must be deterministic where possible (seeded randomness, no wall-clock dependence, generous-but-bounded timeouts, no cross-test shared state).
- **No `t.Skip` without the owner's explicit approval**; environmental skips must be reported, never silent.
- Test helpers (leak checks, crash harness, fault wrappers) are maintained as real code and reused, not re-invented per phase.

---

## 8. Real Verification — Commands Must Actually Run

The agent runs the commands appropriate to the phase and reports actual results. Standard suite (from WSL2, on the ext4 clone):

```bash
go build ./...
go vet ./...
gofmt -l .                 # must output nothing
go test ./...
go test -race ./...        # whenever concurrency is touched
make fuzz                  # fixed short budget per fuzz target, when fuzz targets exist
go test -bench=...         # when benchmarks are in scope (Phase 3+, informational; Phase 9, mandatory)
protoc regeneration        # Phase 5+: must be diff-idempotent
docker compose up / config # Phase 8+: when the Compose stack is in scope
```

- Use only commands relevant to the current phase — but never skip the standard suite.
- **Never report a test as passing unless it was actually executed and passed in this session.** Summaries in the report come from real output.
- If a command is unavailable in the environment (e.g., Docker down), report that fact — do not claim the verification ran.

### Environment rules

- All build/test/benchmark execution happens **inside WSL2 on the ext4 clone** (`~/projects/relayforge`).
- **Never run broker storage tests, crash tests, or benchmarks against `/mnt/c`.** The Windows checkout is an editing location only.
- Record environment-dependent results (benchmarks, I/O-sensitive tests) with the environment context from `docs/ENVIRONMENT.md`.

---

## 9. Failure Handling & Dependencies

### When something fails

1. **Diagnose** it (read the error, reproduce, isolate).
2. **Fix** it if it belongs to the current phase.
3. **Rerun** the relevant verification after the fix.
4. **Report** the issue if it remains unresolved — with the diagnosis and what was attempted.

Do not hide failures. Do not weaken tests to make them pass. Do not delete failing tests.

### Missing previous-phase work

If Phase N depends on work that Phase N−1 should have delivered:

- Determine whether the missing piece can be safely completed as part of the current phase.
- If it is genuinely a previous-phase defect, fix the defect minimally (§4) and say so in the report.
- Do not silently redesign the project to route around missing work; escalate per §2 (abort) if the gap is structural.

---

## 10. Scope Control

- Build only what `PLAN.md` requires. No technologies, libraries, or architecture beyond it.
- **Approved dependencies (complete list):** `google.golang.org/grpc`, `google.golang.org/protobuf` (+ `protoc-gen-go`, `protoc-gen-go-grpc` as tools), `github.com/prometheus/client_golang`. Anything else requires the owner's explicit authorization before it enters `go.mod`.
- Never introduce: AI/LLM features, PostgreSQL, Redis, Elasticsearch/OpenSearch, Kubernetes, Kafka compatibility, Raft/consensus, frontend work, schema registries, streaming DSLs, tracing stacks, or unrelated libraries — unless explicitly authorized by the roadmap or the owner.
- Documentation-only or tooling-only additions beyond the roadmap also require authorization. `PLAN.md` may be updated only per its Document Maintenance section (§20 there).

---

## 11. Completion Report (Mandatory Template)

Step 9 of the protocol produces exactly this report, filled from real results:

```text
Phase:              N — <title>
Status:             COMPLETE | COMPLETE WITH LIMITATIONS | INCOMPLETE — BLOCKED
Implemented:        <bullet list of scope items delivered>
Files changed:      <list of created/modified/deleted files>
Tests executed:     <commands run + pass/fail counts>
Verification results: <actual results: test summary, vet/fmt status, fuzz budget results,
                       race results, benchmark numbers if in scope, compose status if in scope>
Guarantees verified:  <which guarantees from PLAN.md §2 this phase's tests prove, and how>
Known limitations:    <documented gaps, deferred items, environmental constraints>
Anything incomplete:  <explicit list, or "None">
```

Rules:

- Every field is truthful and evidence-based. `COMPLETE` with failing or skipped tests is forbidden.
- `COMPLETE WITH LIMITATIONS` is the honest state for phases where documented gaps remain (e.g., a `NOT MEASURED` benchmark, an environmental skip) — the limitations field says exactly what and why.
- `INCOMPLETE — BLOCKED` includes the diagnosis and what the owner must provide.
- After the report, update the README status line to reflect the completed phase (part of finishing the phase, not an extra document).

---

## 12. Architecture Decisions

For **technically significant** implementation decisions (format details, concurrency structures, error-policy choices, protocol mappings, recovery policies), record in `docs/DECISIONS.md` a short entry:

```text
### <short title> (Phase N, <date>)
- Decision:      <what was chosen>
- Why:           <the driving constraint or evidence>
- Alternatives:  <main alternative(s) considered>
- Trade-offs:    <what was given up>
- Guarantees affected: <links to PLAN.md §2 items>
- Tests:         <tests proving the behavior>
```

- Keep entries short — one decision, one screen. Do not generate documentation essays for trivial changes; trivial choices just follow the code.
- If `docs/DECISIONS.md` does not exist yet, create it on the first significant decision.
- Decisions must never contradict `PLAN.md`; if one seems to, stop and ask.

---

## 13. Code Quality

- Idiomatic Go: standard project layout, `gofmt`-clean, `go vet`-clean.
- Clear naming; no abbreviations that require a glossary.
- Small focused components; explicit error handling (no swallowed errors, wrap with context).
- Bounded resource usage: bounded queues, bounded reads, bounded goroutines.
- `context.Context` cancellation where appropriate, propagated end-to-end.
- Safe concurrency per §14.
- Deterministic tests where possible.
- No unnecessary abstraction — no interfaces with a single production implementation "for flexibility"; add seams when a test or phase actually needs them.
- Comments state constraints the code cannot show (invariants, durability contracts) — not narration.

---

## 14. Concurrency

Concurrency must be deliberate. On every phase that touches it, check:

- races (`go test -race ./...`),
- deadlocks (shutdown paths exercised in tests),
- goroutine leaks (leak-check helper in pipeline tests),
- unbounded goroutine creation (every `go func()` has a lifecycle owner),
- channel misuse (ownership: who sends, who closes, who ranges; never close from the receiver side without a documented protocol),
- unsafe shared state (clear ownership; short critical sections; `sync/atomic` for counters),
- excessive lock contention (noted when observed; optimized only per Phase 9 discipline).

Rules:

- Short critical sections; no I/O or RPC while holding a lock unless that is the documented design.
- Every component has a documented shutdown path; graceful shutdown drains with a bounded timeout.
- Backpressure is enforced by bounded queues — never by unbounded buffering.
- The concurrency/ownership model lives in `docs/ARCHITECTURE.md` and stays current.

---

## 15. Storage Safety

For every phase touching storage:

- validate lengths before allocating,
- validate checksums before serving,
- handle partial records (torn tails) and EOF correctly — clean EOF is not an error mid-scan; truncated frames are,
- handle corruption per the documented policy (truncate-to-valid-prefix on the active tail; refuse/rebuild per policy elsewhere),
- make no unsafe assumptions about disk state (assume any byte can be anything after a crash),
- preserve valid prefixes — recovery never loses valid data,
- never serve a record that failed validation,
- never acknowledge beyond the durability state promised by the mode,
- test crash/recovery behavior (truncation matrices, fault injection, SIGKILL harness).

The index and any derived state are rebuildable; the log is the source of truth.

---

## 16. Performance

- Do not optimize prematurely. Correctness and clarity first; Phases 3 and 4 record informational micro-benchmarks only.
- When performance work is explicitly in scope (Phase 9, or a measured defect earlier):

```text
measure → profile → identify bottleneck → change → measure again → keep or revert
```

- Never invent performance improvements or numbers. Never claim an optimization without before/after measurements.
- A kept optimization must not weaken guarantees — its tests still pass, and the decision is recorded per §12.
- Benchmark methodology and environment recording follow `PLAN.md` §11 exactly.

---

## 17. Documentation

- Documentation must reflect the **actual** implementation. If behavior changes, update the relevant doc in the same phase.
- Preferred living documents (create each when its content first becomes real — not before):

```text
docs/ENVIRONMENT.md      docs/RECORD_FORMAT.md    docs/FRAMING.md (may merge into RECORD_FORMAT.md)
docs/STORAGE.md          docs/GUARANTEES.md       docs/ARCHITECTURE.md
docs/PROTOCOL.md         docs/FAILURE_MATRIX.md   docs/OBSERVABILITY.md
docs/BENCHMARKS.md       docs/REPLICATION.md      docs/LIMITATIONS.md
docs/DEMO.md             docs/DECISIONS.md
```

- Do not create a new generic planning document per phase. `PLAN.md` remains the master roadmap; this file remains the operating manual.
- Do not create phase-specific plan files (`PHASE-1-PLAN.md`, …) — they are forbidden. Phase scope lives only in `PLAN.md`.
- README stays truthful: identity, status line (current phase), build/run instructions that actually work, performance claims only from recorded runs.

---

## 18. Git & Repository Rules

- Git is **local only**: no remote, no push, no GitHub connection (the owner connects it explicitly if/when desired — do not run `git remote add` on your own).
- Commit in logical units during the phase; suggested message style: `phase<N>: <imperative summary>`.
- The working tree is clean (committed) by the end of every phase — the completion report references the final commit.
- The repository tree evolves per `PLAN.md` §3: create directories only when their functionality exists.
- `~/rf-labs` stays outside the main module; lab code never becomes a production dependency.

---

## 19. Boundaries

- **MVP boundary:** complete at the end of **Phase 9** (`PLAN.md` §15 checklist). Phase 9's exit gate includes verifying that checklist item by item.
- **Phase 10 boundary:** replication is an advanced feature with two explicitly defined outcomes — the FULL boundary or the validated FALLBACK boundary (`PLAN.md` Phase 10 scope). The phase report and `docs/LIMITATIONS.md` state which was achieved. Presenting the fallback as the full model is a violation of §6.
- **Exactly 10 phases.** Scope pressure is handled within phases (with documented limitations), never by adding phases or silently dropping scope.

---

## 20. Stop Rule

After the completion report, the agent ends with exactly:

> **Phase N is complete. Do you want to proceed to Phase N+1?**

…with N replaced by the actual numbers, and then **stops**. It does not begin the next phase, does not propose a schedule, does not continue working. The owner decides when — and whether — the next phase starts.
