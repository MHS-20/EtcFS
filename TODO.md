# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`.

## Differentiating features

What a shared block device plus disaggregated metadata makes possible and the
alternatives (NFS, JuiceFS, GFS2, OCFS2) cannot do cheaply.

- [ ] **Sub-second recovery, measured.** GFS2/OCFS2 fence by STONITH and replay
      the dead node's journal before anyone resumes I/O. Here metadata is
      already Raft-committed and fencing is a generation guard, so recovery is
      lease expiry with no reboot and no replay.
      The claim needs restating before it is published, because "sub-second" is
      not true of the tail and cannot be made true by tuning: a survivor blocked
      on a dead node's cached lock waits for that lock's lease to expire, and
      the lease TTL has a floor. `SelfFenceWindow(ttl) = 2*ttl` must exceed
      `RequestTimeout` (10s), so a TTL below ~5s is rejected outright — the
      daemon would exit before a stalled request could fail cleanly. The
      published 0.249s median with a 3.501s worst case is therefore expected
      behaviour under the 10s default, not a bug.
      What is defensible: *sub-second median resume, with the tail bounded by
      the lock lease TTL and no journal replay at any point*. Lowering the floor
      at all means moving `RequestTimeout` too, which is a separate decision
      about how long a stalled request may hang.
      The fault injection is now the same across the three shared-device
      backends, so the comparison is finally worth running (see the follow-ups).
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
      Without it the scheme is silently wrong, not merely lossy: writes are
      copy-on-write, so modifying a file frees its old blocks immediately
      (`freeBlocks` → `alloc.Free` returns the range to the node's bitmap on the
      spot), and a block reallocated to another inode before the backup reads it
      makes the copy hold that other file's bytes under the first file's name.
      Likely cheapest shape — a **free floor** rather than a pinned block set:
      one cluster-visible epoch saying "nothing freed after revision R may be
      reallocated until this run ends". One number instead of a set, consulted
      once per free instead of per range, and self-limiting because the run is
      bounded; blocks freed during the run simply leak until it finishes.
      It has to be cluster-wide either way — the run happens on one node while
      the blocks sit in arenas owned by others, each with its own in-memory
      bitmap — so the free path, `Allocator.Reconstruct` (which rebuilds the
      bitmap from live extents, and would otherwise un-pin across a restart) and
      the scrubber all have to respect it, and etcd compaction has to be held
      past R for the duration.
      Nothing of this exists yet: no `pkg/backup`, no `etcfsctl backup`/
      `restore`, no floor.
- [ ] Put etcd's WAL on its own volume (`--wal-dir`) in `deploy/`. Ops config
      rather than a feature, but it stops WAL fsyncs competing with snapshot
      and compaction I/O, which is the standard etcd recommendation and cheap.

## Ideas to improve benchmark results

Worst cases (write-per-Raft-commit pain):

- Batch creates — coalesce multiple inode creates into one Raft proposal.
  The remaining lever for the small-file storm and fsync-heavy writes, both of
  which are dominated by one commit per create.
- WAL on its own volume — cheap, already an item above, helps commit latency
  under load generally.

Best cases (push the wins further):

- Backup/restore from the revision log — unlocks a whole benchmark category
  currently blocked, and plays to the etcd-MVCC strength.

## Benchmarks — not yet written

- [ ] **Backup cost.** Time for a full and an incremental backup of a populated
      filesystem, and the throughput hit on writers while one runs. The
      incremental number is the interesting one: it should scale with churn
      since the last run, not with filesystem size, because the changed blocks
      come from a revision diff rather than a scan.
      Blocked on the feature above: timing a backup nothing can restore from
      would publish a number for something the product cannot do.
- [ ] **Read-mostly with warm page cache.** Extends `bench-etcfs.sh` with
      `ETCFS_BENCH_DIRECT=0` to every backend, repeated reads of a working set
      that fits in RAM. Tests whether the lock-scoped page cache is competitive
      with backends that cache without a coherence protocol.
- [ ] **Repeated `stat` on names that do not exist.** Nothing exercises the
      negative-dentry cache: the suite is block I/O plus the deep walk, and
      neither probes for absent files the way a compiler walking an include
      path or a package manager looking for an optional config does.

## Benchmarks — re-runs and follow-ups

Existing scripts and reports, with a reason to run them again.

- [ ] **Deep directory walks, after the metadata caching.** The published run
      is the "before": zero warm-cache benefit, 8.8s warm against gfs2's
      0.125s. Kernel-side dentry and listing caching plus the readdir cursor
      have landed since; nothing has re-measured the gap they were meant to
      close.
- [ ] **Join latency, without the reattach.** The published 4.49s and 31%
      survivor impact both include an EBS reattach that a clean leave no longer
      causes, since a departing node is not fenced any more. Re-running now
      measures steady-state join cost rather than reattach cost, which is what
      the number was supposed to be.
- [ ] **Cross-node handoff, on a device that does not cap it, and through the
      explicit publish.** All five backends landed in the same 60-330 MiB/s
      band because the shared 1000-IOPS volume was the bottleneck. Needs a
      faster volume before the expected gap can show at all — and the producer
      side of `bench-handoff.sh` should now set `user.etcfs.publish` before the
      consumer starts, which is what the number was supposed to measure.
- [ ] **Node-kill recovery, with comparable kill mechanisms.** EtcFS resumed
      slowest, but nfs/juicefs resumed in ~10-15ms, which suggests their kill
      paths under-simulate a real failure rather than that they recover 20x
      faster. Not readable as a result until the five are killed alike.
- [ ] **Arena fragmentation, over a day rather than ten minutes.** No
      fragmentation trend appeared in the short soak, but the run is too short
      to mean much and the script's headline ratio compares against a pre-churn
      outlier sample. The script already supports the long run; it is unrun,
      and the metric needs a fixed baseline first.

## Known ceilings (deliberate, revisit under load)

The `ponytail:` markers are scattered; they are the same class of decision and
worth one review pass together when the cluster gets bigger:

- [ ] Linear sweeps: lock-cache eviction (`lockmap.go`), buffered-run scan
      (`delegate.go`), flusher tick over the lock cache (`delegate.go`), arena
      bit scan (`pkg/arena/allocator.go`), readdir-cursor eviction
      (`readdircursor.go`). Reviewed: all are pure performance, none is on a
      path with a measured problem, and each already names its upgrade. Left
      alone deliberately; revisit with a profile, not by guessing.
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
