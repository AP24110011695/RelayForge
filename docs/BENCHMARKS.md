# Benchmarks

Methodology-first: every number below was produced by a recorded,
reproducible command on the stated environment, with the raw JSON report
committed under `bench/results/`. Numbers are **machine-specific**; nothing
here is a universal claim. Runs that could not be made are recorded as NOT
MEASURED, never fabricated.

## Methodology

- **What is measured**: producer-side latency per RECORD — send → ack,
  including the broker queue wait and (for `flushed`) the fsync. A batch
  RPC of n messages contributes `duration/n` as one sample. Throughput is
  **counted records/second** (warmup excluded), never RPC/s.
- **Warmup**: 500 (written) / 100 (flushed) records per run, excluded from
  samples and counts.
- **Runs**: one recorded run per scenario in this document (the environment
  is a shared dev VM; repeated runs showed ±10% spread — see the
  micro-benchmark table). Percentiles are computed over sorted per-record
  samples.
- **Reproducibility**: every result file records the environment block
  (hostname, OS/arch, CPUs, Go version, commit) and the exact scenario
  parameters. Reproduce with the commands in `bench/results/*.json`
  companion notes below and `cmd/rfbench`.

## Environment (2026-09-26, commit 26a8b68..HEAD of the phase-9 branch)

| Field | Value |
| --- | --- |
| Host | WSL2 VM on Windows (build 10.0.26200), AMD Ryzen 5 5500U (6C/12T) |
| Guest | Ubuntu 26.04 LTS, kernel 6.18.33.2-microsoft-standard-WSL2 |
| Memory available to WSL | 2.8 GiB (benchmark sizes chosen accordingly) |
| Disk | ext4 inside the WSL2 VM (single virtual disk; Broker/`rfbench` data on `~`, never `/mnt/c`) |
| Go | go1.27.1 |
| Broker | single broker, default topic, 1 partition, record size 1 KiB (26 B header + payload) |

## Results

| Scenario | Records | Throughput | p50 | p95 | p99 | Raw |
| --- | --- | --- | --- | --- | --- | --- |
| Produce, `ack=written`, batch 100 | 19,500 × 1 KiB | **25,775 rec/s (26.4 MB/s)** | 0.034 ms | 0.058 ms | 0.090 ms | `bench/results/produce-written-20k.json` |
| Produce, `ack=flushed`, batch 50 | 2,900 × 1 KiB | **240 rec/s (0.2 MB/s)** | 3.830 ms | 5.595 ms | 6.314 ms | `bench/results/produce-flushed-3k.json` |
| Consume catch-up (drain 23k × 1 KiB) | 23,000 | **87,883 rec/s (90.0 MB/s)** | 0.012 ms | 0.019 ms | 0.019 ms | `bench/results/consume-catchup.json` |
| Recovery scan (24 MB, 24k records, warm page cache) | 24,000 | 7,714 MB/s scan (3.1 ms) | — | — | — | `go test -run TestRecoveryTiming -v ./internal/logengine` |

Reading the durability cost: `ack=flushed` pays an fsync per batch —
p50 jumps from 0.034 ms to 3.8 ms per record (~100×) at this batch size.
Batching amortizes the fsync; a group-commit design (Phase 9 follow-up
candidate, not implemented) would amortize further.

**Recovery caveat**: the 24 MB log was closed moments before the timed
Open, so the scan ran from the warm page cache — 7.7 GB/s reflects the
scan/CRC cost, NOT cold-disk read throughput. A cold-cache measurement
needs a drop-caches step; NOT MEASURED in this environment.

## Micro-benchmarks (go test -bench, 3 runs each)

| Benchmark | Before | After | Δ |
| --- | --- | --- | --- |
| `BenchmarkLogAppend` (1 KiB, unsynced) | 3707/3400/3527 ns/op, 1253 B/op, 2 allocs | 3216/3214/2873 ns/op, **127 B/op**, 2 allocs | ~9% faster, 10× fewer bytes allocated |

## Optimization loop (executed, kept)

```text
Hypothesis:  per-append record encoding allocates a fresh buffer; the
             profile shows allocation as the only non-syscall cost worth
             addressing.
Profile:     go test -cpuprofile on BenchmarkLogAppend → 68% pwrite
             syscall (disk I/O, not software-addressable), ~2-4%
             mallocgc + CRC.
Change:      record.AppendEncoded(dst) appends into a caller-provided
             buffer; logengine.Append takes the buffer from a sync.Pool
             and returns it after the write (length captured before Put).
Benchmark:   3527 → 3214 ns/op median; 1253 → 127 B/op.
Compare:     non-overlapping with the baseline spread → real.
Keep:        YES (correctness suites re-run green: logengine, record,
             broker, -race).
```

## NOT MEASURED (and why)

- Multi-producer scaling beyond the 12-thread VM's useful range — the
  2.8 GiB / shared-laptop environment makes sustained multi-client runs
  unreliable; single-producer scenarios above are the recorded baseline.
- Cold-cache recovery — needs a drop-caches step outside CI's reach here.
- Replication overhead — replication lands in Phase 10; benchmarking an
  unimplemented feature is forbidden by PLAN.md.
- Kafka comparisons — explicitly out of scope (different system, different
  goals); comparing would be theater.
