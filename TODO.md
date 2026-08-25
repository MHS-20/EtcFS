# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`, with `benchmark-reports/overview.md` as the ledger of where
EtcFS wins and loses.

## Ideas to improve the worst cases

Each heading is a measured loss from `overview.md`. None of these is measured;
they are the hypotheses worth testing, with the risk each one carries.

**Small-file storm — 3327 s for an 80k untar, 112x behind gfs2.
Metadata concurrency — 180 ops/s at 3 nodes, 8.4x behind.**
Both are one Raft commit per create and nothing else.

- *Batch namespace commits.* Hold created inodes in a short write-behind queue
  (single-digit ms) and flush them as one etcd Txn. A tar is a single-threaded
  stream of creates, so nothing else is in flight to pipeline — deferral is the
  only way to get more than one create into a commit. Risk: a create returns
  before it is durable, so the queue must be flushed by `fsync`, `fsyncdir` and
  `sync`, and the crash window must be documented as the one place where
  data-then-metadata ordering is relaxed.
- *Coalesce the dirent and inode writes.* Worth confirming how many keys one
  create touches — if the dirent and the inode are separate round trips rather
  than one Txn, that is a 2x with no semantic change at all. (The inode counter
  is already handed out in blocks, so it is not in the hot path.)

**fsync-heavy writes — 155 O_DSYNC IOPS, 6.4x behind gfs2.**

- *Do not commit metadata on `fdatasync`.* `fdatasync(2)` is explicitly allowed
  to skip metadata that is not needed to read the data back — timestamps
  qualify, size and the extent map do not. If an overwrite of an
  already-allocated extent currently forces a commit only to move `mtime`, that
  commit can be dropped entirely for `fdatasync`, leaving `fsync` to pay it.
  Check what the write path actually commits before assuming it does.

**Deep walks — `du -s` 197 s over 80k files, 480x behind nfs.
Warm `find` 10.99 s, no warm benefit at all.**

- *Raise the entry and attribute timeouts.* Both are hardcoded to 1.0 s
  (`ETCFS_ENTRY_TIMEOUT` / `ETCFS_ATTR_TIMEOUT`, `pkg/fuse/fuse.h`), which is
  what caps every warm-cache result in the suite: a sweep of 80k files takes
  ~11 s, so every name has expired before the walk returns to it. The timeout is
  belt-and-braces — peers already push `INVAL_ENTRY` through the notify socket
  when they change a name (`internal/ipc/notify.go`) — so a much larger value,
  or none, is defensible. Risk: staleness is then bounded by watch delivery
  rather than by a clock, so a dropped or lagging watch becomes visible where it
  previously self-healed within a second. Wants a resync on watch reconnect
  first.
- *Serve `stat` out of the directory listing.* `du` stats every entry
  immediately after reading the directory. If `readdirplus` already returns
  attributes, those should populate the attribute cache so the stats cost
  nothing; if it does not, one range read per directory beats 100 point reads.

**Cold negative lookup — 1474 µs, 140x behind gfs2.**

- *Prefetch the directory's name set on the first miss.* One range read over the
  directory's `dirent:` prefix answers every subsequent miss in that directory
  locally, under the same `INVAL_ENTRY` invalidation that already exists.
  Bounded by directory size, so cap it and fall back to point lookups above the
  cap. This is the compiler-include-path pattern the scenario was built for.

**Shared-file write bandwidth — 61.7 MiB/s at 6 nodes, 7.3x behind gfs2, and
falling as nodes are added.**

- *Byte-range locks.* The whole inode is the lock unit today, so six writers to
  disjoint offsets of one file serialise on a single key. Range locks would let
  them proceed in parallel; the cost is a range structure in the lock key and a
  harder recall path.
- *Lock hysteresis.* Hold a lock for a minimum quantum before yielding it, so a
  writer amortises the handover over several operations instead of paying one
  round trip per turn. Cheap to try, and it trades the waiter's latency for
  throughput — measure both.

**Single-node random writes — 21.7% of the raw device's IOPS.**

- *Profile before proposing anything.* 78% of the device's IOPS is going
  somewhere between the FUSE upcall, the IPC round trip and the O_DIRECT write,
  and nothing here says which. The sequential number retains 65.6%, so the loss
  is per-operation rather than per-byte, which points at the round trip. io_uring
  on the data path and a larger `max_write` are the candidates once there is a
  profile to justify one.

**Cluster-wide, and cheap:**

- *etcd's WAL on its own volume* (`--wal-dir` in `deploy/`). Stops WAL fsyncs
  competing with snapshot and compaction I/O; the standard etcd recommendation,
  and it lands under every commit-bound row above. Ops config, not a feature.

## Outstanding benchmark work

- [ ] **A fenced GFS2 comparison.** The node-kill run measures an *unfenced*
      GFS2 cluster, where the survivors' lockspace stops for good — so there is
      no recovery time to divide by, and none should be quoted. Configure real
      STONITH (`fence_aws` stops the instance through the AWS API) and re-run
      `bench-node-kill.sh` to get a like-for-like number against EtcFS's 2.19 s.
- [ ] **`bench-node-scaling.sh` at 8 and 16 nodes.** Over 2/4/6 nodes GFS2 loses
      47% of its shared-directory metadata throughput while EtcFS gains 34%, but
      GFS2 is still 4x faster in absolute terms at six, so "Nx more scalable" is
      not claimable yet. The crossing point, if there is one, is at 8/16
      (`ETCFS_SCALE_NODES="2 4 8 16"`); this account's EC2 capacity is what
      capped the run at six.
- [ ] **`bench-arena-soak.sh`'s headline metric.** `avail_lost_per_live_byte`
      baselines on the script's first sample, taken before the churn has written
      anything, so the ratio is mostly the filesystem filling up for the first
      time (19.4 over six hours, 27.4 over ten minutes — neither meaningful).
      Baseline it on the first sample after live bytes stabilise. The six-hour
      run itself shows no fragmentation trend.
- [ ] **Arena accounting across membership cycles.** `bench-rejoin-load.sh`
      ended three leave/rejoin cycles owning one more arena than it started with
      (3 → 4). An in-flight reclaim explains it, but that is a guess — sample
      again after a settle period, or over more cycles.
- [ ] **Cross-node handoff on a larger instance class.** At 255.95 MiB/s the
      8 GiB handoff is pinned to the t3.medium's own EBS ceiling (254.14 MiB/s
      measured on the same instance), so the scenario currently measures the
      hardware rather than any backend.


# Correctness Patch
The scenario: a node holds a shared (read) lock and resolves an extent. Its lease is lost while etcd itself stays reachable — an unobserved, in-between state. A peer takes the now unguarded inode, buries that same extent (as part of an ordinary overwrite). This node's own background scrubber pass — which does *not* consult per-inode locks, by design (topic #7), because taking a lock per inode across a whole-keyspace scan would be its own scalability problem — completes, sees the extent as dead, and frees its blocks. The allocator hands those blocks to a different file. All of this has to land inside the single read that started it, which requires a full scrub pass to complete within one operation's duration — narrow, but not impossible by construction.

Two things are worth being precise about here. First, the existing safeguard — the scrubber's revision-conditional delete — does **not** close this: that comparison exists to prevent double freeing the same extent across two scrub passes, not to protect a concurrent reader who resolved the extent before the free happened. Second, the actual fix is named and simple in shape (the scrubber checking the lock cache before freeing — a local map lookup, not a round trip) but **is not yet built.** Stating both of those exactly, rather than either overclaiming closure or underselling how narrow the window is, is the honest and defensible position — and demonstrates the design was analyzed rigorously enough to find its own remaining gap rather than merely asserting correctness.

# Future Extension
Cross-file / cross-directory atomicity does not exist. Each inode is independently consistent; there is no multi-inode transaction, no snapshot spanning several files. An application that needs "these three files change together, atomically, cluster-wide" gets nothing from the filesystem for that — it has to build it itself.
