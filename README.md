# RelayForge

**Your app's dependable drop-off point for updates.**

You send RelayForge events — "order 42 placed", "file uploaded" — and it
keeps them safe on disk. Any number of receivers then collect them at their
own pace, each remembering exactly where it stopped. If anything crashes —
including RelayForge itself — nothing that was confirmed is lost.

RelayForge is for developers building apps whose parts need the same
updates without being wired directly together: your checkout says
"order placed" once, and email, inventory, and analytics each get it,
on their own schedule.

## What you give it

**Events** — small messages, sent by a program or typed at the command line.

## What it does

1. **Confirms each event is safely on disk** before acknowledging it
   (survives process crashes; `flushed` mode is intended to survive power
   loss).
2. **Keeps events in order** per stream and hands each one to every
   receiver group.
3. **Remembers where each receiver stopped** — slow, paused, or crashed
   receivers resume where they left off, with nothing missed.
4. **Slows senders down instead of dropping anything** when receivers fall
   behind (bounded memory, explicit overload signals).

## What you get

- Instant confirmation per event: `✓ stored as event #41`.
- Every receiver sees events in order and resumes where it stopped.
- One **Overview page** with the whole picture in plain language: what's
  stored, which receivers are caught up, who's behind and by how much, and
  recovery proof after a crash.

## Quickstart (3 steps)

Requires Go + WSL2/Linux (see [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md)).

```sh
go build -o rfbroker ./cmd/rfbroker && go build -o relayctl ./cmd/relayctl

# 1. Start the broker (Overview page at http://localhost:7001/)
./rfbroker

# 2. Send events (any terminal)
./relayctl produce -m "order 42 placed"
./relayctl produce -m "hello world"

# 3. Start a receiver (another terminal) — watch it collect and confirm
./relayctl consume -group workers -topic default
```

Tip: `echo "one per line" | ./relayctl produce` sends a whole batch from
stdin.

Open **http://localhost:7001/** — the Overview page shows what's stored and
where every receiver is, in plain words.

### See why it matters (the 4-step demo)

[docs/DEMO.md](docs/DEMO.md) walks the payoff: pause a receiver and watch
it fall behind → restart it and watch it catch up → crash the broker with
`kill -9` → restart it and confirm every acknowledged event survived.

## Command overview

| Command | What it does |
| --- | --- |
| `rfbroker` | runs the broker (+ Overview/metrics on the admin port) |
| `relayctl produce` | send events |
| `relayctl consume` | run a receiver that collects, confirms, and resumes |
| `relayctl topic create/list/describe` | manage streams (partition counts are fixed at creation) |
| `relayctl group describe` | who's in a receiver group and where they are |
| `rfbench` | load generator + benchmark driver |

## Technical details

The user-facing layer above sits on a deliberately deep engineering core —
all of it documented, tested (unit/integration/race/fuzz), and benchmarked:

- **Storage**: append-only segment logs, sparse indexes, CRC32C record
  format, crash recovery to the longest valid prefix, retention
  — [docs/STORAGE.md](docs/STORAGE.md), [docs/RECORD_FORMAT.md](docs/RECORD_FORMAT.md),
  [docs/GUARANTEES.md](docs/GUARANTEES.md)
- **Wire & API**: gRPC/protobuf, unary RPCs, status mapping
  — [docs/PROTOCOL.md](docs/PROTOCOL.md), [api/proto/](api/proto/)
- **Concurrency**: per-partition single-writer ordering, bounded queues,
  backpressure — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Consumer groups**: generation fencing, durable offsets
  — [docs/GUARANTEES.md](docs/GUARANTEES.md)
- **Reliability**: failure matrix with automated verification
  — [docs/FAILURE_MATRIX.md](docs/FAILURE_MATRIX.md)
- **Performance**: methodology + recorded results
  — [docs/BENCHMARKS.md](docs/BENCHMARKS.md)
- **Operations**: metrics, pprof, health
  — [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md), admin port `:7001`
- **Replication**: async follower + manual failover (documented fallback
  boundary — not consensus, not HA)
  — [docs/REPLICATION.md](docs/REPLICATION.md),
  [docs/LIMITATIONS.md](docs/LIMITATIONS.md)
- **Environment**: [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md)
- Roadmap and agent workflow: [PLAN.md](PLAN.md),
  [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)
