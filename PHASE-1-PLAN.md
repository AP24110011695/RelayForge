# RelayForge — Phase 1 Execution Plan

**Phase:** 1 of 15 — *Go Foundations & the Go Toolchain* (see [ROADMAP.md](ROADMAP.md), Part C, Phase 1)
**Status:** Approved for execution. This is a plan, not code — the repo still contains zero Go source, and it must still contain zero at the end of Phase 1.
**Time budget:** ~10 working sessions of 2.5–3 h (≈ 25–30 h, ≈ 2 weeks at 15 h/week; 3 weeks is fine — depth beats pace).
**Phase 15 note (per review):** advanced/optional. Nothing in Phases 1–14 depends on it.

---

## 0. Working agreement (how every coding session is run)

This is the strict part. It binds the coding agent (me, in future sessions) as much as you.

1. **Named slices only.** Every session opens by stating the one slice being worked (one lab step, one repo file). Nothing outside that slice gets generated — no "let me set up the whole structure while I'm here."
2. **You write first, the agent explains and reviews.** For labs, the default mode is: you attempt, the agent reviews, explains, and helps fix. Full solutions only on your explicit request, and then only with a line-by-line walkthrough before moving on.
3. **Mechanical scaffolding may be generated** (`.gitignore`, Makefile, README stub) — these are not learning objects.
4. **No test run, no progress.** A slice is done only when its `go test -race` output has been run and looked at together.
5. **Zero third-party dependencies in Phase 1.** Any new dependency needs a written reason in `docs/DESIGN_DECISIONS.md` first. Target `go.mod` content at end of Phase 1: module line only.
6. **Size caps are hard stops.** If a step exceeds its cap, stop, cut scope, move on. Over-building is the failure mode this agreement exists to prevent.

The workflow for every step is exactly: **learn → implement the small piece → test it → explain it back → proceed.**

---

## 1. Step 0 — Environment (Session 1)

### 1.1 Install WSL2 + Ubuntu (Windows PowerShell, run as Administrator)

```powershell
wsl --install -d Ubuntu-24.04
wsl --set-default-version 2
```

Reboot when prompted, then launch Ubuntu and create your Linux username/password. Verify:

```powershell
wsl -l -v
```

**Expected output:** `Ubuntu-24.04` listed with `STATE=Stopped/Running` and `VERSION=2`. If it says version 1, run `wsl --set-version Ubuntu-24.04 2`.

### 1.2 Inside Ubuntu: base tools

```bash
sudo apt update && sudo apt install -y build-essential git curl xxd
```

`xxd` (hex viewer) is not used until Phase 4 but belongs in your toolbox from day one — you will read raw log files with it.

### 1.3 Install Go

Go to <https://go.dev/dl/>, note the newest stable **linux-amd64** tarball (≥ 1.24; substitute the version you see), then:

```bash
curl -LO https://go.dev/dl/go1.2x.x.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.2x.x.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc && source ~/.bashrc
go version
```

Do **not** install Go via `apt` — Ubuntu's package is usually several minors behind, and Go's own toolchain updates matter.

**Expected output:** `go version go1.2x.x linux/amd64`. Also run `go env` once and find `GOPATH` and `GOMODCACHE` in the output — know where these live.

### 1.4 Editor

Install VS Code on Windows plus the **Go** and **WSL** extensions; open WSL projects with `code .` from inside Ubuntu. The language server (`gopls`) will flag errors as you learn — that's fine, it's a spellchecker, not a tutor; type code yourself.

### 1.5 Verify the toolchain end-to-end

```bash
mkdir -p ~/scratch/hello && cd ~/scratch/hello
go mod init example/hello
printf 'package main\n\nimport "fmt"\n\nfunc main() { fmt.Println("relayforge pre-flight") }\n' > main.go
go run .
```

**Expected output:** `relayforge pre-flight`. Then delete `~/scratch` — this was plumbing, not a lab.

### 1.6 Where code lives (important)

| Location | What | Why |
|---|---|---|
| `~/rf-labs/` (WSL, ext4) | Labs A–C and later lab2-*, lab3-* … | Labs are personal study material; never in the portfolio repo |
| `~/projects/relayforge/` (WSL, ext4) | **Primary** repo clone | Real file-I/O semantics; `fsync` on `/mnt/c` (a Windows drive mounted via 9P) is slow and behaves differently — fault tests in Phases 4/13 would lie to you |
| `C:\Projects\RelayForge\` (Windows) | Planning docs: `ROADMAP.md`, this file | Where you and the agent plan; docs get copied into the repo |
| GitHub (private repo `relayforge`) | Sync point | One source of truth; clone from both sides |

### 1.7 Git + repo creation

```bash
git config --global user.name  "Your Name"
git config --global user.email "you@example.com"
```

Create the private repo `relayforge` on GitHub (web UI, or `gh repo create relayforge --private` if you install the GitHub CLI), then:

```bash
mkdir -p ~/projects && cd ~/projects
git clone git@github.com:<you>/relayforge.git   # or https URL
cp /mnt/c/Projects/RelayForge/ROADMAP.md ~/projects/relayforge/
cp /mnt/c/Projects/RelayForge/PHASE-1-PLAN.md ~/projects/relayforge/
cd relayforge && git add . && git commit -m "roadmap + phase 1 plan" && git push
```

The two planning documents are project artifacts — they belong in the repo.

### 1.8 Linux essentials for this project (self-study, ~2 h total, spread over the phase)

You need *working* Linux, not sysadmin depth. Learn each command by running it and reading `man <cmd>`:

1. Navigation and inspection: `pwd`, `cd`, `ls -la`, `cat`, `less`, `head`, `tail`.
2. Permissions: `chmod`, `ls -l` output (why `+x` matters for scripts).
3. Packages: `apt search/install/update` (you already used it).
4. Processes: `ps aux`, `top`/`htop` — find your own Go test processes.
5. Signals: **`kill <pid>` sends SIGTERM (a request); `kill -9` sends SIGKILL (no chance to clean up)**. This distinction is the entire basis of Phase 8's graceful-shutdown tests and Phase 13's crash matrix — internalize it now.
6. Disk: `df -h`, `du -sh` (you will need small volumes for disk-full tests later).
7. Search: `grep -r`, `find`.
8. Hex inspection: `xxd file | head` (Phase 4 recovery work).
9. The `man` habit — the answer is local, no browser needed.
10. **ext4 (`~/...`) vs `/mnt/c` (Windows drives via 9P)** — different filesystem stacks, wildly different I/O performance and fsync behavior. This is why §1.6's table exists.

---

## 2. Study curriculum (exact concepts, exact resources)

Format per block: study → tiny inline exercises (in the Tour, in scratch files) → the "can you explain" gate. Do not write lab code before its block is studied. Total study ≈ 12–14 h across the phase; the rest is lab time.

### Block 1 — Syntax & data structures (Sessions 2–3)

| Concept | Depth needed for Phase 3 |
|---|---|
| Packages, imports, visibility (uppercase = exported) | fluent |
| Variables, zero values, type inference, constants | fluent |
| Functions, multiple returns (Go's error pattern), variadic | fluent |
| Control flow: `for` (the only loop), `if` with init, `switch` | fluent |
| Arrays vs **slices** — len/cap, growth, aliasing/sharing, `append` | deep — you will encode/decode byte slices for a living in P3 |
| Maps: iteration order (deliberately random), presence check, deletion | fluent |
| Structs, embedding vs inheritance | fluent |
| Pointers: when to use, no pointer arithmetic | fluent |
| Strings/`[]byte` conversions, `strconv`, `fmt` verbs | fluent |

Resources: **A Tour of Go** (Basics + Methods and interfaces sections, doing every exercise); Go blog *"Go Slices: usage and internals"* (read twice); Go by Example as quick reference.

**Gate:** without notes — explain what `append` does when cap is exceeded, what two slices sharing an array implies, and why map iteration order is randomized.

### Block 2 — Errors, defer, interfaces (Session 3, continued)

| Concept | Depth |
|---|---|
| `error` values, errors as data, wrapping (`fmt.Errorf` + `%w`, `errors.Is/As`) | deep — `logengine` will define `ErrCorrupt` etc. |
| `defer`: LIFO order, argument evaluation time, defer + named returns | deep |
| `panic`/`recover`: what they're for (almost never for control flow) | working |
| Interfaces: implicit satisfaction, small interfaces, the nil-interface trap | deep |
| Type switches/assertions (sparingly) | working |

Resources: Tour of Go (Methods and interfaces); Go blog *"Error handling and Go"*; *The Go Programming Language* ch. 5 (functions) skimmed for defer semantics.

**Gate:** explain why a `nil` `*MyErr` stored in an `error` interface is not `== nil`, and design (on paper) an error taxonomy for "corrupt record / truncated file / bad offset."

### Block 3 — Concurrency (Sessions 5–6) — the core of this phase

| Concept | Depth |
|---|---|
| Goroutines: cheap, growable stacks, M:N scheduling, `GOMAXPROCS` | deep |
| Unbuffered channels: rendezvous semantics | deep |
| Buffered channels: capacity, when full → sender blocks | deep — this *is* the backpressure primitive |
| Channel close: only by sender; `v, ok := <-ch`; `range` over channels | deep |
| `select`: multiple waits, `default`, timeout patterns | deep |
| `sync.WaitGroup`: Add/Wait pairing, draining workers | deep |
| `sync.Mutex` / `RWMutex`: critical sections, why short | deep |
| `sync/atomic`: single-word counters/flags; when it beats a mutex | working |
| `context.Context`: cancellation & deadlines (values: avoid) | working (goes deep in P6/P9) |
| Memory model intro: happens-before; why a race can "work" for a year | conceptual |

Resources: Tour of Go (Concurrency — all of it); Go blog *"Share Memory By Communicating"*; Go blog *"Introducing the Go Race Detector"*; *The Go Programming Language* ch. 8 (the best single chapter on channels — work through it); *Concurrency in Go* ch. 3 selected sections; Go memory model doc — skim, re-read in P8.

**Gate:** explain (a) goroutine vs OS thread cost model, (b) unbuffered vs buffered channel behavior when full, (c) who closes a channel and why, (d) the WaitGroup drain pattern, (e) a deadlock you could create with two channels in under 30 seconds.

### Block 4 — Testing, race detector, benchmarks (Session 9, but used from Session 4)

| Concept | Depth |
|---|---|
| Table-driven tests, `t.Run` subtests, `t.Parallel` (sparingly) | deep |
| `go test -race`: what it detects (actual conflicting accesses at runtime), cost (≈5–10× CPU, ≈5–10× memory), what it cannot prove | deep |
| `-count=N` repeated runs to shake out flaky/racy timing | working |
| Benchmarks: `b.N` loop model, `-bench`, `-benchmem`, `-benchtime`, `-count`; reading ns/op, B/op, allocs/op | deep — P14 is built on this |
| Benchmark hygiene: warmup effects, noise, medians over single runs | introduced (full treatment P14) |

Resources: Go testing package docs (the "Subtests and Sub-benchmarks" section); Go blog *"Using Subtests and Sub-benchmarks"*; race detector blog post above.

**Gate:** given a benchmark output line, read it aloud correctly; state what a data race is (two concurrent accesses, at least one write, no ordering) and why the detector needs actual execution to find one.

---

## 3. The three labs (`~/rf-labs/`)

Rules for all labs: stdlib only; every lab is its own `go mod` module; all tests pass under `go test -race -count=10 ./...`; you write the first draft; each lab ends with a `NOTES.md` in its directory containing: your benchmark/race outputs, environment line (CPU, cores, WSL2), and answers to the understand-checks in your own words.

### Lab A — `lab1-counter`: concurrent counter + race detector

**Goal:** see a data race, then fix it two ways, then measure both fixes.

**Spec:** a tiny package exposing three counters behind one interface (`Increment()`, `Value()`): (1) naive `counter++` guarded by nothing, (2) `sync.Mutex`, (3) `sync/atomic`. Test: launch N = 8 goroutines, each incrementing M = 10,000 times; assert final value == N×M.

**Steps:**
1. Write the naive version and its test. Run `go test -race`. **Read the race report properly**: it shows two goroutines with conflicting accesses — find the "write at … by goroutine X" / "previous read/write at … by goroutine Y" pair and map it to your line of code.
2. Run the same test *without* `-race`, several times (`go test -count=20`). Note that it often passes — that's the point. Explain why in NOTES.md (lost updates depend on scheduling; a race is a correctness bug, not a guaranteed crash).
3. Mutex version → `-race` clean. Atomic version → `-race` clean.
4. Benchmark all three (feeds Lab C).

**Accept:** `-race -count=10` green; race report screenshot/paste in NOTES.md with your line-by-line reading of it.

**Understand-checks:** why isn't `counter++` atomic (it's a read-modify-write)? What ordering does the mutex establish between the last increment and the reader? When is atomic enough vs when do you need a mutex (one word vs an invariant across several fields)?

**Size cap:** ≤ 120 lines of implementation + tests. **Time cap:** 2 sessions.

### Lab B — `lab1-bounded-queue`: producer/consumer with graceful drain

**Goal:** the single most reused shape in this project — bounded queue, multiple producers, consumers that drain and exit cleanly, cancellation that actually cancels.

**Spec:** capacity-K bounded queue (buffered channel, K = 16); P = 4 producers each send M = 1,000 items (tagged with producer id + sequence) then signal completion via WaitGroup; C = 3 consumers run until all producers are done **and** the queue is drained, then exit. Total consumed must equal P×M with every item seen exactly once (check per-producer sequence continuity). Then add `context.Context`: on cancel mid-flight, all goroutines exit within a timeout, and *you decide and document* what happens to an item a producer was blocked sending when cancel arrives (candidates: send returns error; drop with explicit accounting — either is fine, pick one and write the rule down; this is your first guarantee sentence).

**Steps:**
1. Channel version, no context: make the happy path pass. The classic failure: consumers exit before draining, or `WaitGroup` misused → deadlock. Hit the deadlock once on purpose (e.g., wait for consumers before producers finish with a full queue) so you know its face.
2. Add the drain guarantee: all accepted items processed before exit. Test it.
3. Add context cancellation. Test that everything exits within, say, 500 ms of cancel under `-race -count=10`.
4. Stretch (only if on schedule): reimplement with a slice + `sync.Cond` behind a mutex, same tests, benchmark vs the channel version.

**Accept:** all tests green under `-race -count=10`; the cancellation rule written as a one-sentence guarantee in NOTES.md.

**Understand-checks:** why is `close(ch)` done only by the sender side? How do consumers distinguish "queue momentarily empty" from "work is done" (this exact question returns as long-poll in Phase 10)? What precisely does "graceful drain" mean? What single line change reintroduces the deadlock?

**Size cap:** ≤ 150 lines + tests. **Time cap:** 2 sessions.

### Lab C — `lab1-bench`: contention benchmark (your first Go benchmark)

**Goal:** learn `go test -bench` and start the benchmark honesty habit that Phase 14 depends on.

**Spec:** benchmark a shared map under two workloads — read-heavy (90% read / 10% write) and write-heavy (10/90) — with two implementations: `map` + `sync.RWMutex` and `sync.Map`. Fixed key space (1,024 keys, pre-populated before the timer via `b.ResetTimer`), `b.ReportAllocs()`.

**Steps:**
1. Write both implementations behind one interface; write the four benchmark functions.
2. Run `go test -bench=. -benchmem -count=5`. Record all five runs.
3. In NOTES.md: report the median ns/op and allocs/op per configuration, plus an environment line (CPU model, core count, "WSL2, plugged in, no background load").
4. Explain the pattern you observe (expect: `sync.Map` wins read-heavy, loses write-heavy — verify against your own numbers; if your numbers disagree with the folklore, trust your numbers).

**Accept:** benchmark table in NOTES.md with medians across 5 runs + environment line.

**Understand-checks:** read one output line aloud: what exactly is `ns/op`, `B/op`, `allocs/op`? Why `-count=5` and medians (noise) rather than one run? Why does write contention flip the winner? Why does laptop state matter already now (this is the seed of Phase 14's methodology section)?

**Size cap:** ≤ 100 lines. **Time cap:** 1 session.

---

## 4. Repo skeleton (Session 10 — the *only* repo work in Phase 1)

In `~/projects/relayforge/`. Nothing else. In particular: **no Go source files in the repo** — `go.mod` at repo root is allowed but will contain only the module declaration until Phase 3.

| Artifact | Content |
|---|---|
| `README.md` | ≤ 1 page: what RelayForge is (two sentences, technically honest), current status ("pre-storage-engine"), pointer to ROADMAP.md. No buzzwords. |
| `.gitignore` | Go defaults (binaries, test coverage files), editor dirs |
| `Makefile` | targets: `build` (`go build ./...`), `test` (`go test ./...`), `race` (`go test -race ./...`), `vet` (`go vet ./...` + `gofmt -l .` must print nothing), `fmt` (`gofmt -w .`) |
| `docs/DESIGN_DECISIONS.md` | ADR log. **ADR-1: "Why Go and why an append-only log for RelayForge"** — 10–15 lines, your words, written after the labs (it should cite something you *felt*: e.g., how the bounded queue made backpressure thinkable) |
| `ROADMAP.md`, `PHASE-1-PLAN.md` | copied in during §1.7 |
| `go.mod` | `go mod init github.com/<you>/relayforge` — module line only, zero requires |

Commit each artifact as its own commit (practice small, well-messaged commits now — they're part of the portfolio evidence). Push. Then verify from a clean clone: `git clone` to a fresh directory → `make race` passes (vacuously, no code) → `make vet` passes.

---

## 5. Schedule (10 sessions)

| Session | Content |
|---|---|
| 1 | §1 Environment: WSL2, Go, editor, hello check, git + repo, planning docs copied in |
| 2 | Block 1 study: Tour of Go basics + slices blog post; inline exercises |
| 3 | Block 1 finish + Block 2: errors/defer/interfaces; the error-taxonomy gate |
| 4 | Block 3 study begins (goroutines, channels — ch. 8 of the book); start Lab A step 1–2 (naive counter + reading the race report) |
| 5 | Lab A finish: mutex + atomic + benchmarks; NOTES.md |
| 6 | Block 3 finish: select, WaitGroup, mutexes, context intro; concurrency gate questions |
| 7 | Lab B steps 1–2: happy path + drain guarantee (meet the deadlock on purpose) |
| 8 | Lab B finish: context cancellation + stress; cancellation guarantee sentence; stretch: condvar version |
| 9 | Block 4 study + Lab C: benchmarks, `-benchmem`, medians, NOTES.md |
| 10 | §4 Repo skeleton + ADR-1 + README; **DoD review: answer every Phase 1 checkpoint question out loud** |

---

## 6. Expected outputs (what "working" looks like)

- `go version` → `go version go1.2x.x linux/amd64` (whatever is current; ≥ 1.24).
- `go test -race` on the naive counter → a report beginning `WARNING: DATA RACE`, with two goroutine sections showing the conflicting read/write and both stack traces pointing into your counter code. You should be able to annotate both stacks.
- `go test -race` on the fixed versions → `ok <module> <time>` and nothing else.
- `go test -bench=. -benchmem` → lines like `BenchmarkMutexIncrement-8  20000000  85.3 ns/op  0 B/op  0 allocs/op` (your numbers will differ — record *yours*; the trailing `-8` is `GOMAXPROCS`).
- `go test -race -count=10` → ten consecutive `ok` lines; this repeated-run habit is what shakes out racy timing from here to Phase 15.
- NOTES.md files in all three lab dirs with outputs + your answers in your own words.
- A GitHub repo whose latest commit is the repo skeleton, and whose tree contains **zero** `.go` files.

---

## 7. Phase 1 Definition of Done

- [ ] `wsl -l -v` shows Ubuntu-24.04 on WSL2; `go version` ≥ 1.24 inside it
- [ ] All three labs pass `go test -race -count=10 ./...`
- [ ] Every lab has a NOTES.md: pasted outputs, environment line, answers to the understand-checks
- [ ] Race report from Lab A annotated line-by-line in NOTES.md
- [ ] Repo on GitHub; clean clone passes `make race` and `make vet`; each artifact in its own commit
- [ ] `go.mod` contains zero third-party dependencies
- [ ] Repo contains no `.go` files (labs live in `~/rf-labs`)
- [ ] README ≤ 1 page, honest; ADR-1 written in your own words
- [ ] **Spoken checkpoint, no notes:** (1) goroutine vs OS thread — the cost model; (2) when a channel vs a mutex; (3) buffered vs unbuffered channel, behavior when full; (4) what `go test -race` does and what it does not prove; (5) the WaitGroup drain pattern; (6) happens-before in one paragraph (intro level is enough)

## 8. Scope guards (read before every session)

- No Phase 2/3 content early: no networking code, no file I/O, no `logengine` package, no topics, no broker — curiosity goes into NOTES.md as questions, not code.
- No new dependencies, no linters beyond `go vet`, no CI yet (CI arrives in Phase 13), no Docker yet (Phase 12).
- Labs are small by design: at the size caps above, they are complete. Extra ideas → NOTES.md backlog.
- If any step exceeds its time cap by 50%, stop and shrink the lab — the phase's goal is fluency, not feature count.
- The agent never generates a whole lab or a whole repo in one turn; slices only (§0).

**Exit:** when every DoD box is checked and the spoken checkpoint is done, Phase 2 begins — TCP echo + framed KV in `rf-labs`, still zero code in the portfolio repo.
