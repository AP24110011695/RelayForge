# RelayForge — Development Environment

Recorded: 2026-09-26, during Phase 1 execution. Update this file whenever the
environment changes; phases must verify against reality, not assumptions.

## Host

- Windows host: build 10.0.26200, x86_64, running WSL2.
- WSL2 distro: Ubuntu 26.04 LTS (x86_64), kernel
  6.18.33.2-microsoft-standard-WSL2.
- CPU: AMD Ryzen 5 5500U with Radeon Graphics (6 cores / 12 threads).
- Memory visible to WSL2: 2.8 GiB total. This is the WSL VM allocation, not
  necessarily host RAM — load and benchmark runs must be sized for it.
- Disk (WSL root fs): 1007 GB volume, ~953 GB free, ext4 (/dev/sdd on /).

## Toolchain (as actually installed in WSL)

| Tool | Version / state | Notes |
| --- | --- | --- |
| Go | go1.27.1 linux/amd64 | installed at /usr/local/go; matches the 1.27.1 target |
| git | 2.53.0 | global identity configured (owner) |
| make | GNU Make 4.4.1 | |
| gcc | 15.2.0 (Ubuntu) | required by `go test -race` (cgo) — present; race detector works |
| Docker | Docker Desktop installed on Windows; WSL integration OFF | `docker` is not callable inside WSL yet; must be enabled before Phase 8 |

## Go on PATH

`/usr/local/go/bin` is not on the default WSL PATH. Phase 1 wired it up by
symlinking into `/usr/local/bin` (already on PATH):

- `/usr/local/bin/go` → `/usr/local/go/bin/go`
- `/usr/local/bin/gofmt` → `/usr/local/go/bin/gofmt`

The Makefile additionally pins `GO := /usr/local/go/bin/go` (with a PATH
fallback), so make targets work in any shell.

## Git configuration notes

- The canonical repo sets `core.filemode=false`: files were staged via
  `/mnt/c` (mode 755) and mode tracking carries no signal for this tree;
  it also makes the mirrored `.git` work correctly on Windows NTFS without
  per-mirror fixes.
- Text files are LF everywhere (`.gitattributes`: `* text=auto eol=lf`).

## Repositories and sync workflow

- **Canonical execution repo**: `~/projects/relayforge` (WSL2 ext4). All
  build/test/fuzz/benchmark execution and all git commits happen here.
- **Editing mirror**: `C:\Projects\RelayForge` on Windows
  (`/mnt/c/Projects/RelayForge` from WSL), for editing and inspection only.
  It is refreshed from the canonical repo after each phase. Git object files
  are read-only (mode 444), so the `.git` directory must be removed before
  copying:
  `rm -rf /mnt/c/Projects/RelayForge/.git && cp -a ~/projects/relayforge/. /mnt/c/Projects/RelayForge/`
- If files are edited in the Windows mirror, copy them into the canonical
  repo and re-verify before committing; commits are made from WSL only.

## Filesystem rules

- Broker storage tests, crash tests, and benchmarks run only on ext4 inside
  WSL2.
- **Never run real broker I/O tests or benchmarks against `/mnt/c`** (9p
  filesystem: misleading I/O semantics and performance). Copying files across
  is fine; executing I/O-sensitive work there is not.

## Labs

- `~/rf-labs` holds the Phase 1 concurrency labs (lab1-bench,
  lab1-bounded-queue, lab1-counter). Phase 2 adds the TCP/framing labs there.
  Labs stay outside the module and are never a production dependency.

## Discrepancies vs. target environment

- Go: none — go1.27.1 matches the target exactly.
- Docker Desktop WSL integration is disabled. Enabling it is a Docker Desktop
  settings change on the Windows side; required by Phase 8 (Compose), not a
  Phase 1 gap.
- WSL memory allocation is 2.8 GiB; benchmark scenarios in Phase 9 must be
  sized accordingly.
- GitHub: not connected, by policy (see IMPLEMENTATION_PLAN.md, Git rules).
