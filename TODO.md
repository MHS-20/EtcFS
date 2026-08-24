# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`.

## Ideas to improve benchmark results

Worst cases (write-per-Raft-commit pain):

- Batch creates — coalesce multiple inode creates into one Raft proposal.
  The remaining lever for the small-file storm and fsync-heavy writes, both of
  which are dominated by one commit per create.
- WAL on its own volume — cheap, already an item above, helps commit latency
  under load generally. Put etcd's WAL on its own volume (`--wal-dir`) in `deploy/`. Ops config rather than a feature, but it stops WAL fsyncs competing with       snapshot and compaction I/O, which is the standard etcd recommendation and cheap.


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

## Headline claims — what would actually evidence them

Claims worth making, matched to the scenario that would support each. None of
these is measured yet; the numbers are what decide whether the claim survives.

- [ ] **"Nx faster fencing and recovery than GFS2/OCFS2."** `bench-node-kill.sh`
      across all five backends is the evidence, and it is the run in flight
      below. The multiple cannot be stated until the competitor columns exist —
      etcfs measures 0.007s resume and 1.513s max stall under the hardened
      fault, but there is nothing current to divide it by.
      Constrained by the entry at the top of this file: the tail is bounded by
      the lock lease TTL and cannot be tuned below it, so the defensible shape
      is *sub-second median resume with no journal replay at any point*, plus a
      stated tail. A single "Nx" over the median alone would be selling the
      median as the whole distribution.
- [ ] **"Nx more scalable than GFS2."** Two scenarios, neither run for any
      backend:
      - `bench-metadata-concurrency.sh` is the direct test and the stronger one.
        Parallel create/stat/unlink in *one shared directory* against node
        count. GFS2 bounce that directory's DLM lock between nodes on
        every operation, so their curve should flatten or fall as nodes are
        added, while etcfs pays one Raft commit per mutation with no lock
        ping-pong. A ratio taken at the widest node count is the honest form of
        this claim, and it is a curve rather than a single number.
      - `bench-node-scaling.sh` gives aggregate bandwidth and metadata ops
        against node count on shared and disjoint working sets. Note its
        structural finding, which may be worth more than any ratio: GFS2 cannot
        be swept past the journal count fixed at mkfs time, so a sweep point
        beyond that simply fails to mount.
      Both default to sweeps that provision at the largest point (2/4/8), so
      each is a bigger and longer run than anything below.

## Benchmarks — etcfs measured, other backends outstanding

Each of these has a validated script and an etcfs number. None can be published
as a comparison until the other four backends are run under the *same* harness,
and one of them cannot be published even for etcfs yet.

- [x] **Negative lookup, all backends.** Done, written up in
      `docs/reports/benchmark-reports/negative-lookup.md`. Warm us/lookup:
      etcfs 2.10, nfs 3.64, gfs2 5.40, juicefs 322.89, gluster 693.36.
      etcfs wins warm — 2.6x over gfs2, 1.7x over nfs — but the 511x ratio is
      not the story: etcfs has the *worst* cold path of the five at 1073.50 us
      against gfs2's 9.50 us, a 113x deficit, and a ratio rewards a slow start.
      gfs2's 1.76x is not a caching failure; it resolves missing names from
      local kernel structures, so `compare_drop_caches` cannot make it cold.
      gluster and juicefs do not cache absences at all at this set size.
      Caveat carried into the report: the etcfs column is from a separate run
      to the other four, so small differences are noise. Worth re-running etcfs
      alongside them for a same-harness column.
- [x] **Warm page cache, all backends.** Done, written up in
      `docs/reports/benchmark-reports/warm-page-cache.md`. etcfs 622.31x
      (626,047 warm / 1,006 cold, zero daemon reads), gfs2 611.89x, gluster
      610.01x, juicefs 3.04x, nfs 1.21x.
      The conclusion is that **this scenario does not differentiate etcfs**: all
      five converge on ~600k warm IOPS, which is RAM. The three shared-device
      backends score ~610-622x only because all three are genuinely device-bound
      cold, and the gap between them is inside run-to-run noise on a shared
      1000-IOPS volume. nfs and juicefs score low ratios because their "cold"
      pass was never cold — both serve from caches `compare_drop_caches` cannot
      reach. Do not quote 622x as an advantage over gfs2's 611x.
      A workload that would actually differentiate the backends has to make the
      coherence protocol do work: concurrent readers and writers on the same
      inodes across nodes, i.e. `bench-metadata-concurrency.sh` and
      `bench-handoff.sh`.
- [ ] **Deep walk at 80k files.** The 20k run gives 5.768s cold / 2.316s warm
      (2.49x, against no warm benefit at all before the metadata caching). It
      cannot replace the published figure because that one is 80k files — the
      other backends do not need re-running, only etcfs at a matching size.
- [ ] **Node kill, all five backends.** *In flight.* etcfs under the hardened fault: 0.007s
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

## Every benchmark still needing an etcfs run

One list, because the etcfs column is the one that gates publishing and it was
scattered across three sections above. Ordered by what unblocks the most.

- [ ] `bench-node-kill.sh` — **in flight.** Re-run under the hardened fault
      alongside the other four, so the flagship table compares one fault rather
      than five. Feeds the fencing/recovery claim.
- [ ] `bench-metadata-concurrency.sh` — never run, for any backend. The direct
      test of the shared-directory scalability claim.
- [ ] `bench-node-scaling.sh` — never run, for any backend. Bandwidth and
      metadata ops against node count; also pins down where GFS2 stops being
      sweepable at all.
- [ ] `bench-deep-walk.sh` at 80k files — etcfs only. The 20k run (5.768s cold /
      2.316s warm, 2.49x) cannot replace a published 80k figure, and the other
      backends do not need re-running for it.
- [ ] `bench-join-latency.sh` — etcfs only. The published 4.49s and 31% survivor
      impact both include an EBS reattach a clean leave no longer causes.
- [ ] `bench-handoff.sh` — etcfs only, and needs a faster volume than the shared
      1000-IOPS one that flattened all five backends into the same band. The
      producer should also set `user.etcfs.publish` before the consumer starts,
      which is what the number was meant to measure.
- [ ] `bench-arena-soak.sh` over a day — etcfs only, and unrun at that length.
      Needs a fixed baseline first: the headline ratio currently compares
      against a pre-churn outlier sample.

Not on this list, and deliberately: `bench-warm-cache.sh` and
`bench-negative-lookup.sh` both have a current etcfs number. They are waiting on
the other four backends, not on etcfs.

