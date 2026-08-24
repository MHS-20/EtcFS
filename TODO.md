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
- [ ] **Warm page cache, all backends.** The page-cache defect that blocked this
      is fixed but the fix is unmeasured: the 1.08x on record was taken with
      every read reaching the daemon, so it documents a bug rather than a design
      property. Re-run etcfs first and check `daemon_reads_during_warm`, then
      measure all five.
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

- [x] **The kernel page cache was inert.** A warm pass over a 256 MiB working
      set that had just been read end to end still sent every one of its 30,283
      reads to the daemon, at the volume's IOPS rather than RAM speed. The cause
      was the notification protocol, not the caching logic: `INVAL_ENTRY` carried
      its name with no length, so the C reader recovered it as "whatever arrived
      with the header" and two messages sharing one read desynchronised the
      stream for good. After that `INVAL_INODE` was never recognised, its
      acknowledgement never came, the five-second wait expired and the connection
      was dropped — and since the C side connected once at startup and never
      again, `cacheableOpen` answered every later open with `keep_cache=0` for
      the life of the mount. The startup connect the benchmark probes for had
      succeeded, which is why the mount reported the cache as available while
      serving nothing from it. Names are length-prefixed now, the client
      reconnects, and the daemon warns the first time it has to answer an open
      as non-cacheable.
      Still to do: re-run `bench-warm-cache.sh` and confirm
      `daemon_reads_during_warm` collapses. Every read number in the suite was
      measured with the read cache doing nothing, so they understate the
      filesystem and overstate the coordination layer's share of the ceiling.
