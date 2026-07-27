# EtcFS — Agent Instructions

## State

**Design phase only.** No code, no build system, no tests, no CI, no deps, not a git repo.

## Document map

| Document | Purpose |
|----------|---------|
| `docs/plans/init_plan.md` | Authoritative architectural design — subsystems, etcd schema, locking, data path, fencing, invariants |
| `docs/plans/02_implementation_phases.md` | 12-phase build plan (Phase 0–11) with dependencies and research-informed rationale |
| `docs/etcd_raft_research.md` | etcd/Raft internals research — transaction model, leases, watches, scaling limits (~1M files per cluster), watch amplification, pagination |
| `docs/research/cluster-fs-survey.md` | Cluster/distributed filesystem survey — GFS2, OCFS2, CephFS, GlusterFS, Lustre, EBS Multi-Attach failure modes, design lessons |
| `docs/research/vfs_fuse_block_research.md` | Linux VFS, FUSE protocol/operations/capabilities, O_DIRECT alignment, io_uring, design implications |
| `docs/research/userspace_filesystem_research.md` | FUSE daemon design patterns, metadata caching, language choice (Rust vs Go), test harnesses (Jepsen, FoundationDB simulation, xfstests) |
| `scripts/infra/` | AWS EC2 + EBS Multi-Attach provisioning, etcd deployment, FUSE daemon bootstrap (template), node add/remove |
| `scripts/test/` | Chaos engineering, fencing validation, epoch tests, network isolation (adapted from QAttach) |

## Architecture (from `docs/plans/init_plan.md`)

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

- `docs/plans/init_plan.md` is the authoritative reference — consult it before making any design decision.
- All implementation decisions (language, build system, test framework, etc.) are **not yet made**.
- The fencing/epoch invariants in §9 of init_plan are the most safety-critical part of the design.
- The 12-phase build order in `02_implementation_phases.md` supersedes the 6-step sketch in init_plan §15.
- Phase 4 (fault-injection harness) must be built before trusting any phase that writes to real block devices.
