# EtcFS — Agent Instructions

## State

**Implemented and under hardening.** Go + C codebase, `make` build, Go test suite, chaos/stress harness against real AWS infra. Phases 0–6 are built; Phase 11 hardening is in progress.

Layout: `cmd/etcfuse` (C FUSE daemon) · `cmd/etcfuse-meta` (Go metadata daemon) · `pkg/{metadata,fencing,blockio,arena,scrub,compaction,wal,walgo,fuse,block}` · `internal/{ipc,config}` · `test/harness` (Go, mock-store + simulator) · `scripts/{infra,test}` (AWS provisioning + chaos).

The two daemons talk over a hand-rolled length-prefixed binary protocol on a Unix socket (`internal/ipc/socket.go`). `proto/ipc.proto` documents the intended message shapes but is **not** the wire format in use — do not assume gRPC/protobuf at runtime.

### Chaos suite status

`chaos-report-20260730-165637`: **5 pass / 2 fail of 7 assertions** (6 scenarios; S3 asserts twice; there is no S4), up from 1/7. S2, S3 (both), S6, S7 pass. Fixed in the 2026-07-30 pass:

- **Inode 1 collision (product bug).** `allocInode` handed out inode 1 to the first regular file, but 1 is `FUSE_ROOT_ID` — the root directory. The first file created overwrote the root inode record, so every subsequent op on the mount returned EIO. Allocation now starts at `metadata.FirstUsableIno` (2). This blocked every scenario.
- **readdirplus parser desync (product bug).** `ec_readdirplus` skipped already-returned entries with `continue` *before* consuming their attr block and timeouts, desynchronising the response parser and emitting phantom directory entries with garbage names. Entry is now fully parsed before the skip check.
- **Harness: poisoned seed.** The script seeded `inode_alloc_counter` with ASCII `1`, but the daemon stores it as 8-byte big-endian; `DecodeUint64` returns 0 for short values, making the allocator's CAS unsatisfiable (`create` → ENOSPC). The seed is gone — the absent-key path bootstraps correctly.
- **Harness: S6/S7 restart raced a 10s SSH timeout** while sleeping 9s; S3 never restarted N1 after its (correct) self-fence. Both now use `restart_daemons` under a 60s budget.

Also fixed after that run:

- **Fencing generation is now enforced on writes (S5).** The data path previously stamped the generation onto extents but committed them with unguarded `Put`s, so a fenced node's writes were accepted. `handleWriteBlock` now commits the extent and any inode size change in a single transaction guarded by `metadata.WithGenerationGuard`, and rejects the write when the guard fails. `Service.InitGeneration` seeds `gen:<node_id>` at startup (the guard compares the key's *value*, and a value comparison against a missing key always fails) and caches the generation the node started with; a restarted node re-reads the current generation and resumes normally. `Service.IsFenced()` is also checked before touching the device.
- **S1's remount failure was two harness bugs**, not a daemon defect. `pkill` matches the process name as an *unanchored* regex, so `pkill -9 etcfuse` also killed `etcfuse-meta`; the scenario then restarted only the C daemon, which had no Go daemon left to talk to. S1 additionally ran `rm -f /tmp/etcfuse.sock`, unlinking the socket the Go daemon owns. Either way `connect()` failed with ENOENT and the C daemon exited before `fuse_session_mount` was reached — the mount-retry loop from `72180f6` never ran. Now `pkill -9 -x etcfuse`, and the socket is left alone; remount completes in ~1s. (S2 was unaffected: its pattern `etcfuse-meta` cannot match `etcfuse`, and it restarts both daemons.)

Verified on AWS after these fixes: **S1 PASS** (`s1-data` read back, no remount warning) and **S5 PASS** (write rejected with EIO, `write: rejected, node has been fenced` in the daemon log).

Caveat: S2/S3/S6/S7 last passed in `chaos-report-20260730-165637`, which predates the generation-guard change. They have not been re-run since and should be before the suite is called green.

Not yet guarded: namespace mutations (create/mkdir/unlink/rename/setattr) still commit without a generation guard — only the data path is covered.

See `docs/plans/04_hardening_plan.md` for the tracked gap ledger; verify each item against code before trusting its status.

## Document map

| Document | Purpose |
|----------|---------|
| `docs/plans/init_plan.md` | Authoritative architectural design — subsystems, etcd schema, locking, data path, fencing, invariants |
| `docs/plans/02_implementation_phases.md` | 12-phase build plan (Phase 0–11) with dependencies and research-informed rationale |
| `docs/plans/04_hardening_plan.md` | Gap ledger between design and implementation (Critical/Major/Minor) |
| `docs/architecture/*.md` | Per-subsystem design docs (~24 files) — fencing, WAL, write ordering, schema, coherence, scrubber |
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
- Language/build decisions are made: C (FUSE frontend, C11, `-Wall -Wextra -Werror`) + Go (metadata backend). Build with `make`; `make check` runs lint + tests.
- The fencing/epoch invariants in §9 of init_plan are the most safety-critical part of the design. Any change touching `pkg/fencing`, `pkg/metadata/gen.go`, or the extent-commit path must preserve them.
- The 12-phase build order in `02_implementation_phases.md` supersedes the 6-step sketch in init_plan §15.
- Phase 4 (fault-injection harness) must be built before trusting any phase that writes to real block devices.
- etcd value encodings are **not uniform**: `inode_alloc_counter` and dirent values are 8-byte big-endian; `gen:<node>` is decimal ASCII; `extent:<ino>/<chunk>` is comma-separated ASCII. Match the existing encoding when seeding keys by hand or via `etcdctl`.
- Chaos scenarios cost real AWS resources and take ~4 min each to provision. Run a single scenario (`chaos-test.sh 1`) while iterating, not `all`.
- The chaos harness swallows errors (`readf`/`writef`/`runcmd` return `""` on any failure), so an SSH timeout looks identical to data loss. Confirm a failure's cause in `/tmp/meta.log` and `/tmp/fuse.log` on the node before treating it as a product bug.
