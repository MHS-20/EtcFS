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


## Benchmarks — etcfs measured, other backends outstanding

Each of these has a validated script and an etcfs number. None can be published
as a comparison until the other four backends are run under the *same* harness,
and one of them cannot be published even for etcfs yet.

- [ ] **Negative lookup, all backends.** etcfs: 1073.5 us cold, 2.10 us warm,
      511x, on 200 names swept 200 times. A single-column report is honest but
      the scenario exists to be compared — nfs has attribute caching of its own
      and gfs2 reads metadata off the local device, so both should do well here.
      Note the working set must sweep inside the client's entry timeout: 2000
      names gives 1.54x on etcfs purely because names expire mid-sweep.
- [ ] **Warm page cache, all backends.** Blocked on the page-cache defect below
      rather than on machine time. etcfs currently measures 1.08x with every
      read reaching the daemon, so publishing it would document a bug as a
      design property. Fix first, then measure all five.
- [ ] **Deep walk at 80k files.** The 20k run gives 5.768s cold / 2.316s warm
      (2.49x, against no warm benefit at all before the metadata caching). It
      cannot replace the published figure because that one is 80k files — the
      other backends do not need re-running, only etcfs at a matching size.
- [ ] **Node kill, all five backends.** etcfs under the hardened fault: 0.007s
      resume, 1.513s max stall, 1 failed op — against 0.249s / 3.501s published
      under the old one. This one *must* be all five: the harness change was to
      every backend (sysrq power-off for the shared-device three, port blocks
      for the two server-mediated ones), so the published gfs2/gluster/nfs/
      juicefs numbers were measured under a milder fault. Dropping the new etcfs
      column into that table would compare a power-off against an orderly
      membership change, which flatters etcfs and is the same unfairness the
      hardening removed, pointed the other way.

## Benchmarks — re-runs and follow-ups

Existing scripts and reports, with a reason to run them again.

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
- [ ] **Arena fragmentation, over a day rather than ten minutes.** No
      fragmentation trend appeared in the short soak, but the run is too short
      to mean much and the script's headline ratio compares against a pre-churn
      outlier sample. The script already supports the long run; it is unrun,
      and the metric needs a fixed baseline first.

## Reliability

- [ ] **The kernel page cache is inert.** `--page-cache` defaults to true,
      reports itself available, and does nothing: a warm pass over a 256 MiB
      working set that had just been read end to end still sent every one of its
      30,283 reads to the daemon, and ran at the volume's IOPS rather than RAM
      speed. The cache-invalidation client was connected on all three nodes, so
      `cacheableOpen` returns true and the open is answered with `keep_cache=1`
      and `direct_io=0` — the kernel is permitted to cache and does not.
      This is not a benchmark artifact and it is not the negative-dentry or
      readdir caching, both of which demonstrably work on the same mount. It
      also means every read number in the suite is measured on a filesystem
      whose read cache does nothing, so the coordination layer is currently
      carrying blame for a ceiling the caching layer was meant to lift.
      Two things to check, cheapest first: log the `keep_cache` decision in
      `handleOpen` and read it off a live node, to confirm what is actually on
      the wire rather than inferring it; and build with `FUSE_CAP_AUTO_INVAL_DATA`
      unset, since that flag makes the kernel revalidate attributes and drop the
      data cache when they appear to move, and it is enabled by libfuse default.
      `bench-warm-cache.sh` already reports `daemon_reads_during_warm`, so either
      test is one run.
- [ ] **The C notify client never retries.** `notify_thread` in `pkg/fuse/fuse.c`
      makes exactly one `connect()`; on failure it closes the socket and returns,
      with no retry and no log line. A lost startup race therefore leaves the
      node unable to invalidate anything for the life of the process, and leaves
      `--page-cache` silently off with nothing reporting it. Not the cause of the
      inert cache above — the client was connected — but the same failure would
      be invisible if it ever happened. `compare_mount_etcfs` now warns when it
      has, which is a benchmark-side workaround for something the daemon should
      say itself.
