# EtcFS — Agent Instructions

## State

**Implemented and under hardening.** Go + C codebase, `make` build, Go test suite, chaos/stress harness against real AWS infra.

Layout: `cmd/etcfuse` (C FUSE daemon) · `cmd/etcfuse-meta` (Go metadata daemon) · `pkg/{metadata,fencing,blockio,arena,scrub,walgo,fuse,block}` · `internal/{ipc,config}` · `test/harness` (Go, mock-store + simulator) · `scripts/{infra,test}` (AWS provisioning + chaos).

The two daemons talk over a hand-rolled length-prefixed binary protocol on a Unix socket (`internal/ipc/socket.go`). There is no gRPC/protobuf anywhere in the wire path — an earlier `proto/ipc.proto` sketch was removed as dead weight.

### Chaos suite status

**7 of 7 assertions pass** (`chaos-report-20260730-180644`, commit `660a14a`), verified on real AWS infra: S1, S2, S3 (both assertions), S5, S6, S7. Full scenario descriptions, the two product bugs that were fixed to get here (an inode-numbering collision with the FUSE root directory, and a `readdirplus` parser desync), the harness bugs found along the way, and known gaps not covered by this suite are all in `docs/chaos-reports/2026-07-30-fresh-cluster-per-scenario.md` — read that before touching `scripts/test/chaos-test.sh` or the write path. Single-cluster sequential and randomized-fuzz results are in `docs/chaos-reports/2026-07-31-single-cluster-and-fuzz.md`.

Not yet guarded: namespace mutations (create/mkdir/unlink/rename/setattr) still commit without a generation guard — only the data path (extent writes) is covered. See `docs/architecture/fencing-generation-protocol.md` § Implementation Status.

## Document map

| Document | Purpose |
|----------|---------|
| `docs/architecture/*.md` | Per-subsystem design docs (~24 files) — fencing, WAL, write ordering, schema, coherence, scrubber |
| `docs/etcd_raft_research.md` | etcd/Raft internals research — transaction model, leases, watches, scaling limits (~1M files per cluster), watch amplification, pagination |
| `docs/research/cluster-fs-survey.md` | Cluster/distributed filesystem survey — GFS2, OCFS2, CephFS, GlusterFS, Lustre, EBS Multi-Attach failure modes, design lessons |
| `docs/research/vfs_fuse_block_research.md` | Linux VFS, FUSE protocol/operations/capabilities, O_DIRECT alignment, io_uring, design implications |
| `docs/research/userspace_filesystem_research.md` | FUSE daemon design patterns, metadata caching, language choice (Rust vs Go), test harnesses (Jepsen, FoundationDB simulation, xfstests) |
| `scripts/infra/` | AWS EC2 + EBS Multi-Attach provisioning, etcd deployment, FUSE daemon bootstrap (template), node add/remove |
| `scripts/test/` | Chaos engineering, fencing validation, epoch tests, network isolation (adapted from QAttach) |

## Architecture

Four subsystems per node (FUSE userspace daemon):
1. **FUSE frontend** — VFS ops → metadata + data calls
2. **Metadata client** — etcd as single source of truth for inodes, dirents, locks, allocation
3. **Data engine** — O_DIRECT/io_uring pread/pwrite against shared raw block device
4. **Membership/fencing agent** — etcd lease heartbeat + external fencing controller

Key design invariants:
- Data-then-metadata ordering for writes (crash consistency without disk journal)
- Metadata-then-data ordering for truncates
- Fencing: self-fencing watchdog → dual-confirmed external fencing → generation-stamped extent scrubber
- No directory-level locking — namespace mutations via atomic etcd Txn
- Block device carries only raw extents of file content — no filesystem format

## Conventions

- `docs/architecture/*.md` is the authoritative reference — consult the relevant subsystem doc before making any design decision.
- Language/build decisions are made: C (FUSE frontend, C11, `-Wall -Wextra -Werror`) + Go (metadata backend). Build with `make`; `make check` runs lint + tests.
- The fencing/generation invariants (see `docs/architecture/fencing-generation-protocol.md`) are the most safety-critical part of the design. Any change touching `pkg/fencing`, `pkg/metadata/gen.go`, or the extent-commit path must preserve them.
- etcd value encodings are **not uniform**: `inode_alloc_counter` and dirent values are 8-byte big-endian; `gen:<node>` is decimal ASCII; `extent:<ino>/<chunk>` is comma-separated ASCII. Match the existing encoding when seeding keys by hand or via `etcdctl`.
- Chaos scenarios cost real AWS resources and take ~4 min each to provision. Run a single scenario (`chaos-test.sh 1`) while iterating, not `all`.
- The chaos harness swallows errors (`readf`/`writef`/`runcmd` return `""` on any failure), so an SSH timeout looks identical to data loss. Confirm a failure's cause in `/tmp/meta.log` and `/tmp/fuse.log` on the node before treating it as a product bug.
