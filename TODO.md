# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing below the first section is a
known bug; it is accumulated shape debt and planned work, ordered roughly by
payoff.

## Results (2026-08-16/17 benchmark run)

All benchmarks below are done. Full reports in `docs/reports/benchmark-reports/`.

- **EtcFS wins:** etcd-degraded (reads fully decoupled from etcd health),
  leave/rejoin under load (~1% survivor impact, arenas stable), online volume
  growth (3.2s, zero restart), node-count scaling (disjoint writes + metadata
  scale cleanly to 8 nodes).
- **EtcFS loses:** small-file storm (140x behind gfs2), fsync-heavy writes
  (slowest of five), metadata concurrency (worst absolute ops/s), single-node
  IOPS retention (22% of raw), deep-walk warm-cache (no benefit at all, vs
  gfs2's 70x) — all consistent with the per-write Raft-commit cost.
  Confirms the metadata-lookup-caching item is the right next feature.
  Skipped: backup-cost (blocked on the backup feature, not implemented).
- **Inconclusive / needs follow-up:** cross-node handoff (shared volume capped
  everyone before the gap could show), node-kill (etcfs slowest, but other
  backends' kill methods likely under-simulate a real failure), join latency
  (31% survivor impact vs. predicted "none" — conflates reattach cost with
  steady-state join cost), arena-soak (10-minute run too short, headline
  metric needs a fixed baseline).
- **Found along the way:** a real source bug (1 MiB IPC frame-length cap
  off-by-header-size, fixed) and a real design gap (a graceful node leave
  still gets externally fenced today — see the new reliability item below).

## Differentiating features

What a shared block device plus disaggregated metadata makes possible and the
alternatives (NFS, JuiceFS, GFS2, OCFS2) cannot do cheaply.

- [x] ~~**Snapshots and clones from etcd MVCC.**~~ **Not going to implement.**
      The metadata half is free — writes are already copy-on-write, so etcd
      MVCC can resolve any past revision's extents. The data half is not: it
      needs blocks to stay pinned instead of being freed after the write that
      buried them, which means holding space proportional to churn, a durable
      pin record the arena rebuild respects, a scrubber that knows the floor,
      and etcd compaction held back. Freeing blocks promptly is worth more here
      than time travel. Closed deliberately, not deferred.
- [ ] **Sub-second recovery, measured.** GFS2/OCFS2 fence by STONITH and replay
      the dead node's journal before anyone resumes I/O. Here metadata is
      already Raft-committed and fencing is a generation guard, so recovery is
      lease expiry with no reboot and no replay. Nothing exercises this as a
      *number* yet: add a node-kill recovery benchmark (below) and publish it.
- [ ] **Zero-copy handoff as a first-class op.** A producer writes, publishes,
      and a consumer on another node reads the same physical blocks at device
      bandwidth — only the extent map crosses the network. Expose it as an
      explicit publish (flush + yield the lock, without waiting for a recall),
      so pipelines get handoff at device speed.
- [ ] **Backup and restore, driven by the revision log.** A backup at revision
      R is two paired artifacts: `etcdctl snapshot save` for the namespace, and
      the blocks the extent keys at R name, streamed to a second volume or to
      object storage. Paired, they restore a point-in-time filesystem;
      separately, neither is worth much.
      Incremental falls out of the design: diffing etcd revisions R1→R2 yields
      exactly the extent keys that changed, so the changed blocks are known
      without a scan, a hash pass, or dirty-bit tracking.
      Needs a *bounded* pin — blocks referenced at R must survive until the
      copy has read them, and are freed as soon as the run finishes. That is a
      far cheaper thing than the open-ended pinning snapshots wanted, and it is
      the only reason to keep any pin mechanism at all. Restore is the half to
      write first — an untested restore is not a backup.
      Nothing of this exists yet: no `pkg/backup`, no `etcfsctl backup`/
      `restore`, no bounded pin. The backup-cost benchmark below is blocked on
      it and was deliberately not written.
- [x] ~~**Metadata-lookup caching.**~~ **Done**, as two kernel-side caches on
      the existing `dirent:` watch, with no new coherence protocol.
      - Negative-dentry caching: a LOOKUP the store confirms is absent now
        answers with a negative entry (errno 0, `ino=0`, `entry_timeout=1`)
        instead of ENOENT, so the kernel can cache the absence. A lookup that
        could not be *decided* — etcd failure, or a dirent naming a missing
        inode — still returns an errno, which caches nothing.
      - Directory-listing caching: `opendir` returns `FOPEN_CACHE_DIR`, so a
        repeat walk stops re-issuing READDIR and, behind it, an etcd prefix
        scan per directory. The kernel drops a cached listing on the parent's
        `i_version` (bumped by every `inval_entry`) or its mtime (moved in etcd
        by every create and unlink), so the notification makes it prompt and
        the mtime bounds it at `attr_timeout` if the notification never comes.
        The mtime half only works where `FUSE_AUTO_INVAL_DATA` is negotiated —
        the kernel gates the refresh on it — so `init` now asks for it
        explicitly and the listing cache is switched off entirely on a kernel
        that does not offer it. Root is excluded too: it has no inode record,
        its attrs are synthesised with a fixed mtime, and it would have only
        the notification.
      - Still open, and the cold half of the same problem: every READDIR does a
        full `ListDirents` prefix scan and then slices out the page asked for,
        so filling a large directory's listing costs one whole scan per page.
        The kernel cache removes the *refills*, not that. The fix is neither
        cache — page the etcd read itself (`GetPrefix` from the last key
        returned, with a limit), which is strictly less work at identical
        linearizability.
      - The daemon-side readdir cache in the original sketch was deliberately
        not written. It is the one piece with no fail-safe: directories take no
        inode lock, so there is nothing to key it by the way the metadata cache
        is keyed, and a watch-only invalidation would weaken readdir from
        linearizable to bounded-staleness with nothing bounding it. Moving the
        same listing into the kernel's cache gets the repeat-walk win with an
        invalidation path that already existed.
      - Found along the way: the `dirent:` watch was drained by a single
        `range` and never re-opened, so an etcd compaction past the watched
        revision ended every invalidation for the life of the daemon while it
        went on serving. Fixed — it re-opens and says so.
      - Still unmeasured: the deep-walk benchmark covers the listing half cold
        and warm, but nothing exercises repeated `stat` on names that do not
        exist, which is what the negative cache is for.
- [ ] Put etcd's WAL on its own volume (`--wal-dir`) in `deploy/`. Ops config
      rather than a feature, but it stops WAL fsyncs competing with snapshot
      and compaction I/O, which is the standard etcd recommendation and cheap.

## Ideas to Improve Benchmarks Results

Worst cases (write-per-Raft-commit pain):

- Batch creates — coalesce multiple inode creates into one Raft proposal (smallfile storm, fsync writes)
- ~~Metadata-lookup caching~~ — done; needs a re-run of the deep-walk benchmark to say how much of the warm-cache gap it actually closed
- WAL on its own volume — cheap, already a TODO item, helps commit latency under load generally

Best cases (push the wins further):

- Zero-copy handoff as explicit op (TODO item) — turn "should win" into a guaranteed win instead of device-ceiling-masked
- Backup/restore from revision log — unlocks a whole benchmark category currently blocked, plays to the etcd-MVCC strength
- Departing-node protocol (from today's finding) — let a clean leave skip fencing, makes join/rejoin latency a clean win instead of bundling in reattach cost

Single biggest lever either way: metadata-lookup caching. It's the one fix that both shrinks a loss (deep-walk, smallfile storm indirectly) and requires no new coherence protocol — TODO already scoped it.

## Check if these are true or if they need a fix

Real source bug found and fixed (with your sign-off): IPC frame-length cap off-by-header-size, rejecting every exact-1MiB write.

Real design finding, documented not silently patched: a graceful node leave still gets externally fenced today because etcd can't distinguish an explicit lease revoke from a TTL expiry — added to TODO.md with the fix sketched (departing-node marker protocol), scripts worked around it honestly instead.

## Benchmarks — where EtcFS should win

New scripts under `scripts/bench/compare/`, run against the same four backends
`run-all.sh` already provisions. Each needs a headline number, not a table.

- [x] ~~**Cross-node handoff.**~~ **Done, inconclusive.** All five backends
      landed in the same 60-330 MiB/s band — the shared 1000-IOPS volume
      capped everyone before the expected gap could show. Report:
      `docs/reports/benchmark-reports/2026-08-16-cross-node-handoff.md`.
      Original text: Node A writes an N-GiB file, fsyncs; node B reads
      it immediately. Measure B's read throughput and time-to-first-byte.
      EtcFS and GFS2/OCFS2 read from the device; NFS and JuiceFS move every
      byte over the network (JuiceFS through object storage). Sweep N = 1 MiB
      to 8 GiB — the gap should widen with size.
- [x] ~~**Node-kill recovery.**~~ **Done.** EtcFS resumed slowest (0.25s,
      worst-case 3.5s stall) of the five — but the other backends' kill
      mechanisms likely under-simulate a real failure (nfs/juicefs resumed in
      ~10-15ms), so this is not read as a clean loss. Report:
      `docs/reports/benchmark-reports/2026-08-16-node-kill-recovery.md`.
      Original text: Kill a node holding locks under load, measure how
      long until a surviving node's I/O to those inodes resumes. EtcFS: lease
      expiry, no journal replay, no STONITH. GFS2/OCFS2: fence plus replay.
      NFS: server kill is total outage. This is the flagship number.
- [ ] **Backup cost.** Time for a full and an incremental backup of a populated
      filesystem, and the throughput hit on writers while one runs. The
      incremental number is the interesting one: it should scale with churn
      since the last run, not with filesystem size, because the changed blocks
      come from a revision diff rather than a scan.
      Blocked: there is no backup or restore implementation to benchmark, so no
      script was written — a harness that snapshotted etcd and copied blocks
      without a restore path would be measuring something the product cannot
      do. Unblocks with the feature item above.
      No benchmark script written: blocked on the restore half, which does not
      exist yet (no `pkg/backup`, no `etcfsctl` backup/restore path), and
      timing a backup nothing can restore from would publish a number for a
      feature that isn't there.
- [x] ~~**Metadata under concurrency.**~~ **Done, EtcFS lost.** Worst
      absolute ops/s at every node count (~10x behind gfs2 at 3 nodes), though
      its own curve did climb 1→2 nodes before flattening. Report:
      `docs/reports/benchmark-reports/2026-08-16-metadata-concurrency.md`.
      Original text: Parallel create/stat/unlink in one shared
      directory across all nodes. GFS2/OCFS2 bounce the directory's DLM lock
      per operation; EtcFS pays one Raft commit but no lock ping-pong. Report
      operations/second against node count.
- [ ] **Read-mostly with warm page cache.** Extends the existing
      `bench-etcfs-pagecache.sh` to every backend, `direct=0`, repeated reads
      of a working set that fits in RAM. Tests whether the lock-scoped page
      cache is competitive with backends that cache without a coherence
      protocol.

## Benchmarks — where EtcFS should lose

Equally important: knowing when not to use it. Publish these alongside the
wins, and let a bad result stand.

- [x] ~~**Small-file metadata storm.**~~ **Done, EtcFS lost as expected.**
      140x slower than gfs2 (69 min vs 30s for 80k files). Report:
      `docs/reports/benchmark-reports/2026-08-16-smallfile-storm.md`.
      Original text: Untar a kernel-source-sized tree
      (~80k small files) on one node. Every create is a Raft commit; local
      filesystems and NFS with a fast server should beat it outright. Expect
      EtcFS to be worst here — quantify how much, and whether batching creates
      would close it.
- [x] ~~**Single-node throughput.**~~ **Done.** EtcFS retains 60% of raw
      sequential bandwidth but only 22% of raw random-write IOPS — worst IOPS
      retention of the five. Report:
      `docs/reports/benchmark-reports/2026-08-16-single-node-ceiling.md`.
      Original text: One writer, no sharing. The whole
      coordination layer is pure overhead; a bare filesystem on the same
      volume is the ceiling. Report the percentage of raw device throughput
      retained.
- [x] ~~**Deep directory walks.**~~ **Done.** Ties gfs2 on cold `find` (9s)
      but shows zero warm-cache benefit (gfs2 drops to 0.125s warm, EtcFS
      stays at 8.8s) — the exact gap the metadata-lookup-caching item above
      would close. Report:
      `docs/reports/benchmark-reports/2026-08-16-deep-directory-walks.md`.
      Original text: `find`/`du` over a large tree. Every LOOKUP is
      an etcd read; NFS with attribute caching and GFS2 reading local metadata
      both have less to do.
- [x] ~~**fsync-heavy small writes.**~~ **Done, EtcFS lost.** Slowest of the
      five (154 IOPS vs gfs2's 989). Report:
      `docs/reports/benchmark-reports/2026-08-16-fsync-heavy-writes.md`.
      Original text: `O_DSYNC` disables write deferral, so every
      write costs a device write plus a Raft commit. Compare against GFS2,
      whose journal absorbs the same pattern locally.
- [x] ~~**etcd degraded.**~~ **Done, confirms the design claim.** Read IOPS
      held flat (1016) through every degradation phase while write IOPS fell
      1001→870→14 — cached-lock reads genuinely never touch etcd. Report:
      `docs/reports/benchmark-reports/2026-08-16-etcd-degraded.md`.
      Original text: Run with one etcd member down, and with 50 ms latency
      injected between members. Everything gets slower by design; the number
      worth publishing is how gracefully, and whether reads still serve from
      cached locks.

## Benchmarks — scalability and elasticity

The axis where GFS2/OCFS2 are structurally weakest (fixed journal count, node
limits, offline resize) and nothing currently measures.

- [x] ~~**Node-count scaling curve.**~~ **Done, at 2/4/8 nodes.** Disjoint
      writes and metadata ops scale cleanly for EtcFS to 8 nodes; shared-file
      writes collapse past 2 nodes (lock serialization). juicefs/gluster
      correctly capped at 7 (client-node limit). Found and fixed two script
      bugs along the way (a jq null-divide crash on partial failure, an
      unbound-array race at 8-node scale). Report:
      `docs/reports/benchmark-reports/2026-08-16-node-count-scaling.md`.
      Original text: Aggregate throughput and metadata
      operations/second at 2, 4, 8, 16, 32 nodes, on both a shared working set
      and disjoint per-node working sets. GFS2 needs a journal per node
      pre-allocated at mkfs and is not usually run past ~16; EtcFS adds a node
      with no on-disk change. Find where each curve bends.
- [x] ~~**Join latency.**~~ **Done.** 4.49s including EBS reattach;
      survivor impact 31% (not the predicted "none" — needs a follow-up
      isolating reattach cost from steady-state join cost). Uncovered that a
      graceful leave still gets externally fenced by design (see the new
      reliability item below) and two script races (daemon-restart socket
      race, no poll timeout) — both fixed. Report:
      `docs/reports/benchmark-reports/2026-08-17-join-latency.md`.
      Original text: Time from process start to first successful write on a
      new node, and the impact that join has on existing nodes' latency
      (should be none — a new node claims its own arena). Compare against
      adding a GFS2 node, which needs a free journal.
- [x] ~~**Leave and rejoin under load.**~~ **Done, confirms the design
      claim.** Near-zero survivor impact (1.37% dip) and arena count exactly
      stable across 3 cycles. Report:
      `docs/reports/benchmark-reports/2026-08-17-leave-and-rejoin-under-load.md`.
      Original text: Clean leave, then rejoin, repeatedly,
      while the rest of the cluster writes. Measure any throughput dip on the
      survivors and confirm arena reclaim keeps up.
- [x] ~~**Online volume growth.**~~ **Done, confirms the design claim.** New
      space allocatable in 3.2s with zero daemon restart. Report:
      `docs/reports/benchmark-reports/2026-08-17-online-volume-growth.md`.
      Original text: Grow the shared volume under a running cluster
      and measure time until the new space is allocatable. Blocked on the
      device-size-read-once item below; GFS2 needs `gfs2_grow` plus a mount
      that notices, NFS grows server-side transparently.
- [x] ~~**Arena fragmentation over time.**~~ **Done, 10-minute soak, no
      fragmentation trend found** — but the script's own headline ratio is
      misleading (compares against a pre-churn outlier sample); flagged
      rather than trusted, and a real day-long soak (the script already
      supports it) is still unrun. Report:
      `docs/reports/benchmark-reports/2026-08-17-arena-fragmentation-soak.md`.
      Original text: Long soak with churn (create, grow,
      delete, rejoin), tracking allocatable space versus live data and how
      many arenas end up partly used. This is the number that decides whether
      the per-node arena model holds at scale.

## Structure / SOLID

- [x] ~~`internal/ipc.Service` is a god object: ~20 fields, 5 mutexes, 113~~ **Done.** Split into collaborators the handlers hold: `lockMap`, `recallSet`,
      `openFiles`, `writeOp`. One mutex left on Service (the generation guard).
      Original text: `internal/ipc.Service` is a god object: ~20 fields, 5 mutexes, 113
      methods across 8 files — lock cache, write delegation, page-cache state,
      open-file accounting, fencing generation, history, block device.
      Every new subsystem widens the same struct. Split along the seams that
      already exist (`lockcache.go`, `delegate.go`) into collaborators the
      handlers hold, rather than fields on one type.
- [x] ~~`handleWriteBlock` (`internal/ipc/datapath.go:125`) is ~390 lines with~~ **Done.** Now `newWriteOp`/`allocate`/`writeThrough`/`proposal` in writeop.go, with
      `bufferWriteOp` and `commitWriteOp` as the two paths.
      Original text: `handleWriteBlock` (`internal/ipc/datapath.go:125`) is ~390 lines with
      two closures (`freeRuns`, `writeThrough`) carrying state through it.
      Extract: allocate + pad, write-through, proposal build, commit/reclaim.
- [x] ~~`handleSetattr` (`internal/ipc/handlers.go:577`) does flag decode, lock,~~ **Done.** `applySetattr(rec, valid, fields, now)` is pure and tested directly.
      Original text: `handleSetattr` (`internal/ipc/handlers.go:577`) does flag decode, lock,
      truncate, field application and the CAS in one body. The field
      application block is a pure function of `(rec, valid, fields)` — lift it
      out and test it directly.
- [x] ~~`handlers.go` (781 lines) mixes namespace ops, open/release refcounting~~ **Done.** Descriptor accounting is `openFiles` in opendescriptors.go.
      Original text: `handlers.go` (781 lines) mixes namespace ops, open/release refcounting
      and statfs. Move the descriptor accounting (`retain`/`release`/
      `heldOpen`/`orphaned`) to its own file.
- [x] ~~Service configuration is six `SetX` setters called after construction~~ **Done.** `ipc.Options` passed to `NewService`; the setters are gone.
      Original text: Service configuration is six `SetX` setters called after construction
      (`cmd/etcfuse-meta/main.go:233-310`). Nothing enforces they run before
      the socket accepts, and each is mutable at any time. Prefer an options
      struct passed to `NewService`.

## Wire protocol

- [x] ~~The IPC layout is hand-encoded twice — Go `buf`/`reader`, C `wb_*`/`rb_*`~~ **Done.** Every fixed-width reply is pinned from both sides now, in
      socket_test.go and test_ops.c.
      Original text: The IPC layout is hand-encoded twice — Go `buf`/`reader`, C `wb_*`/`rb_*`
      — kept in sync by comments and three width tests (`attrWireSize`,
      `setattrPayloadLen`, the CREATE keep_cache test). Every other op is
      unguarded; a field added on one side only desynchronises the parser
      silently. Either a table-driven layout shared by a generator, or a width
      test per opcode.
- [x] ~~78 call sites return bare negative errno literals (`int32Resp(-11)`,~~ **Done.** Named in internal/ipc/errno.go; no bare literals left in the package.
      Original text: 78 call sites return bare negative errno literals (`int32Resp(-11)`,
      `-5`, `-28`). Name them once (`errAgain`, `errIO`, …); `-11` vs `-22`
      is a typo away from a wrong errno with no compiler help.
- [x] ~~`pkg/fuse/ops.c` (1467 lines): ~20 op functions repeat the same~~ **Done.** `ipc_call` and `ipc_reply_status` absorbed the repetition: 1467 to 1315
      lines, 43 `fuse_reply_err(req, EIO)` down to 20.
      Original text: `pkg/fuse/ops.c` (1467 lines): ~20 op functions repeat the same
      marshal → `ipc_sync` → `rb_*` → reply sequence, with
      `fuse_reply_err(req, EIO)` written 43 times. A small
      request/reply/parse helper would remove most of the file's bulk and the
      "did this one free `resp` on every path?" review burden.

## Simplification

- [x] ~~`scripts/bench/compare/bench-{gfs2,gluster,juicefs}.sh` (~90-100 lines~~ **Done.** `compare_begin`, `compare_install_fio`, `compare_shared_device` and
      `compare_finish` now live in compare-lib.sh.
      Original text: `scripts/bench/compare/bench-{gfs2,gluster,juicefs}.sh` (~90-100 lines
      each) repeat provision, package install and mount blocks that
      `compare-lib.sh` already partly owns. Push the shared parts down.
- [x] ~~`scripts/bench/compare/bench-etcfs-pagecache.sh` duplicates~~ **Done.** Folded into bench-etcfs.sh behind `ETCFS_BENCH_DIRECT=0`.
      Original text: `scripts/bench/compare/bench-etcfs-pagecache.sh` duplicates
      `bench-etcfs.sh` to change one `run_fio` argument (`direct=0`). Fold it
      into `bench-etcfs.sh` behind an env var, or track it deliberately —
      right now it is untracked and excluded from `run-all.sh`.
- [x] ~~The entry-response wire format is transcribed verbatim in three docs~~ **Done.** Described once in fuse-request-dispatch.md; the other two link to it.
      Original text: The entry-response wire format is transcribed verbatim in three docs
      (`fuse-architecture.md`, `fuse-request-dispatch.md`,
      `fuse-read-operations.md`). Describe it once and link.

## Known ceilings (deliberate, revisit under load)

The `ponytail:` markers are scattered; they are the same class of decision and
worth one review pass together when the cluster gets bigger:

- [ ] Linear sweeps: lock-cache eviction (now `lockmap.go`), buffered-run scan
      (`delegate.go`), flusher tick over the lock cache (`delegate.go`), arena
      bit scan (`pkg/arena/allocator.go`). Reviewed: all four are pure
      performance, none is on a path with a measured problem, and each already
      names its upgrade. The arena scan's marker was stale — it has had a
      rotating start hint for a while — and now says so. Left alone
      deliberately; revisit with a profile, not by guessing.
- [ ] One notify socket and one C-side thread (`notify.go`): a slow
      `INVAL_INODE` serialises every other invalidation, and invalidation
      blocks a lock release. Reviewed and left: a second connection changes the
      order invalidations reach the kernel in, which is exactly what the cache
      coherence argument rests on. The unresponsive-client breaker already
      bounds the damage a wedged client can do.
- [ ] `pagesCached` is a one-way latch for the process lifetime
      (`service.go`): once any open was answered cacheable, every later key
      release pays an invalidation round trip even for inodes never cached.
      Per-inode tracking would cost a map but skip the common case.
      Reviewed and left: deciding per inode that an invalidation can be skipped
      is a coherence decision, not an optimisation — the latch is the
      fail-safe direction, and the cost is one round trip on a path that is
      already yielding a lock. Worth doing only with a measurement that says
      it matters and a test that pins the skip condition.

## Reliability / resilience / hardening

- [ ] **No way for a leaving node to skip external fencing.** A graceful
      shutdown (`leaveCluster` in `cmd/etcfuse-meta/main.go`) already revokes
      the node's membership lease synchronously before exit — but etcd's watch
      API cannot tell an explicit `Revoke()` from a TTL timeout; both surface
      to a watcher as the same delete event. The surviving nodes' fencing
      controller therefore treats every membership-key disappearance as a
      possible crash and detaches the departing node's EBS volume regardless
      of intent, discovered when `scripts/bench/compare/bench-join-latency.sh`
      and `bench-rejoin-load.sh`'s own "clean leave" comment turned out not to
      hold: even a SIGTERM'd node gets its device detached out from under it,
      and the benchmark scripts now reattach it manually before restarting
      (`compare_reattach_volume_if_missing` in `compare-backends.sh`) rather
      than measuring a leave path the daemon cannot currently promise.
      This is arguably correct as a safety default — a node's own claim to be
      leaving cleanly is exactly the kind of self-report a fencing system
      should not trust blindly, since a partitioned node could make the same
      claim. Closing the gap for the common case (an intentional leave, not a
      partition) needs a real protocol addition, not a one-line fix: something
      like a "departing" marker key the leaving node writes and revokes
      *before* dropping its membership lease, which the fencing controller
      checks for and honors within a short grace window before deciding to
      fence. Getting the grace window and the check ordering right against a
      genuinely partitioned node claiming the same thing is the hard part.
- [x] ~~The C side never reconnects.~~ **Done.** `ipc_sync` drops the thread's
      fd on any failure (broken stream, oversized frame, truncation) and
      `SIGPIPE` is ignored, so the next request reconnects; covered by
      `scripts/test/chaos-ipc-restart.sh` and a unit test.
      Original text: The C side never reconnects. `etcfs_ipc_fd` (`pkg/fuse/fuse.c:186`)
      caches one fd per worker thread and never invalidates it; `ipc_sync`
      returns -1 on a broken stream and every handler turns that into `EIO`.
      A daemon restart therefore leaves the mount returning `EIO` forever on
      every thread that had connected. Drop the thread's fd on a failed
      `ipc_sync` and reconnect on the next call.
- [x] ~~`ipc_sync` failure is indistinguishable~~ **Done** with the reconnect
      above: any framing failure closes the connection.
      Original text: `ipc_sync` failure is indistinguishable from a desynchronised stream: a
      short read mid-frame closes nothing, so the next request on that fd
      reads the previous reply's tail. Close on any framing failure.
- [x] ~~Device size is read once~~ **Done.** `Device.RefreshSize` re-reads it
      when an arena acquisition fails for space, and the allocator retries once
      if the volume grew. Original text: Device size is read once, at `SetBlockDevice` (`internal/ipc/service.go:161`).
      An EBS volume grown under a running cluster is invisible until every
      daemon restarts — arenas cannot be handed out from the new space and
      `statfs` under-reports. Re-read the size periodically or on an
      allocation failure.
- [x] ~~Quotas are computed and set but never enforced~~ **Done** by documenting
      them as advisory: `etcfsctl quota`/`quota set` now say so, and
      configuration.md no longer claims enforcement. Inline enforcement stays
      rejected — it costs a parent pointer or a Raft round trip per write.
      Original text: Quotas are computed and set but never enforced: `pkg/quota` is reachable
      only from `etcfsctl`, and no write path consults a limit. A quota set
      today silently does nothing. Either enforce at the write/create path or
      document it as reporting-only.
- [x] ~~`statfs` free space is derived from `alloc.LiveRatio()`~~ **Done.**
      Unclaimed space now comes from a cluster-wide count of arena ownership
      records, plus this node's own slack; it under-reports in one direction
      instead of being wrong in two. Original text: `statfs` free space is derived from `alloc.LiveRatio()` — this node's own
      arenas — so `df` reports a local view of a shared device. Under several
      writers the number is wrong in both directions.
- [x] ~~No health or readiness endpoint.~~ **Done.** `/healthz` and `/readyz`
      (serving + lease live + not fenced) on the metrics listener, which now has
      read/write/idle timeouts. Original text: No health or readiness endpoint. `/metrics` exists
      (`pkg/metrics/metrics.go:206`) but nothing reports "mounted, lease live,
      not fenced" for an orchestrator to probe; `metrics.Serve` also runs
      without read/write timeouts.
- [x] ~~Retry budget is one shape for every etcd call~~ **Done.** Backoff is
      jittered by a quarter in either direction and plain etcd calls get five
      attempts, which spans a leader election. Original text: Retry budget is one shape for every etcd call: 3 attempts, 10-50 ms
      backoff, no jitter (`internal/ipc/retry.go:34`). A leader election that
      outlasts ~100 ms surfaces as `EIO` across every node at once, and the
      un-jittered backoff makes them retry in lockstep.
- [x] ~~`notifyAckTimeout` is 5 s~~ **Done.** Three ack timeouts in a row
      declare the client unresponsive for 30 s; acknowledged sends then fail
      immediately, and the release fails rather than assuming the cache is gone.
      Original text: `notifyAckTimeout` is 5 s (`notify.go:41`) and the invalidation path is
      one socket, one thread. A client wedged but not disconnected stalls
      every lock release behind it for 5 s each — worth a circuit breaker that
      declares the client gone after repeated timeouts.
- [x] ~~Write backpressure is per inode only (`flushMaxBytes`).~~ **Done.**
      A 256 MiB process-wide cap drains other inodes' buffers before a write
      joins; the pending gauges are now process-wide too.
      Original text: Write backpressure is per inode only (`flushMaxBytes`). Nothing bounds
      total buffered bytes across inodes, so many hot files can hold an
      unbounded amount of acknowledged-but-unpublished data in RAM. Add a
      process-wide cap.
- [x] ~~Fault injection covers chaos scenarios end to end but not the IPC
      transport itself~~ **Done.** `chaos-ipc-restart.sh` kills the Go daemon
      under a live mount; frame truncation and oversized frames are driven
      against `ipc_sync` directly in `test/c/test_ops.c`.
      Original text: Fault injection covers chaos scenarios end to end but not the IPC
      transport itself: no test kills the Go daemon under a live mount, and
      none corrupts or truncates a frame. Both are cheap to add to
      `scripts/test/` and would have caught the reconnect gap above.
