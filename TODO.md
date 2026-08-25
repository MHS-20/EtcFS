# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`.

## Ideas to improve benchmark results

Worst cases (write-per-Raft-commit pain):

- Batch creates — coalesce multiple inode creates into one Raft proposal.
  The remaining lever for the small-file storm and fsync-heavy writes, both of
  which are dominated by one commit per create. Measured cost of not doing it:
  112x behind gfs2 on an 80k-file untar, 8.4x on shared-directory metadata.
- WAL on its own volume — cheap, already an item above, helps commit latency
  under load generally. Put etcd's WAL on its own volume (`--wal-dir`) in `deploy/`. Ops config rather than a feature, but it stops WAL fsyncs competing with       snapshot and compaction I/O, which is the standard etcd recommendation and cheap.
- Metadata cache entry timeout — the 1 s timeout is why the 80k-file warm walk
  gets no benefit at all while a 20k one gets 2.49x, and why negative-lookup
  caching collapses past ~500 names. A longer timeout costs coherence latency
  on names another node creates; the trade has never been measured.

## Differentiating features

What a shared block device plus disaggregated metadata makes possible and the
alternatives (NFS, JuiceFS, GFS2, OCFS2) cannot do cheaply.

- [x] **Sub-second recovery, measured.** Measured across all five backends under
      one identical fault (power-off), written up in
      `docs/reports/benchmark-reports/node-kill-recovery.md`. The defensible
      claim is now evidenced: etcfs takes over a dead node's locks in 2.19 s
      with no fence device and no journal replay, while its survivors never stop
      serving I/O (0.113 s worst gap). GFS2's survivors stop entirely — the DLM
      lockspace goes to `kern_stop` / "wait fencing" and stays there — and nfs
      and juicefs simply end. The tail is still bounded by the lock lease TTL,
      which cannot go below ~5 s while `RequestTimeout` is 10 s, so the shape to
      publish remains *sub-second median resume, tail bounded by the lease, no
      journal replay at any point*.

## Headline claims — what would actually evidence them

- [~] **"Nx faster fencing and recovery than GFS2/OCFS2."** Half evidenced. The
      measured multiple is **10.3x against gluster** (22.64 s → 2.19 s), the only
      competitor that recovered at all. Against GFS2 there is no multiple and
      there should not be one: it never recovered inside the 180 s window, and a
      ratio against "never" is just a restatement of how long the window was
      (82x at 180 s, 274x at 600 s — the number grows with the watching, which is
      the tell that it is the wrong instrument).
      What is claimable against GFS2 today is categorical: *etcfs recovers a dead
      node's locks with no fence device, no operator and no journal replay, while
      an unfenced GFS2 cluster's survivors stop entirely and stay stopped.*
      To earn a real number, configure genuine STONITH for the gfs2 harness —
      `fence_aws` stops the instance through the AWS API and is the right agent
      here — and re-run `bench-node-kill.sh`. A fenced GFS2 recovers in
      fence-device latency plus journal replay, which could plausibly land in the
      same order as etcfs's 2.19 s; publishing 82x against an unfenced cluster
      would be the same unfairness the harness rewrite removed, pointed the other
      way.
- [ ] **"Nx more scalable than GFS2."** Partly evidenced, and the honest answer
      is *not yet, at this width*. Over a 2/4/6-node sweep GFS2 loses 47% of its
      shared-directory metadata throughput (1522 → 756 ops/s) while etcfs gains
      34% (141 → 188) — the curves converge, but GFS2 is still 4x faster in
      absolute terms at six nodes, so there is no multiple to quote yet. The
      sweep that would show a crossing is 8/16 nodes
      (`ETCFS_SCALE_NODES="2 4 8 16"`), and this account's EC2 capacity is what
      capped the run at six.

## Benchmarks — status

Every scenario below has been run for etcfs and, where it is a comparison, for
all five backends in one session (2026-08-24/25). Reports are under
`docs/reports/benchmark-reports/`, and `overview.md` is the cross-scenario
summary of where etcfs wins and loses and by how much.

- [x] Negative lookup, all five backends.
- [x] Warm page cache, all five backends.
- [x] Deep walk at 80k files, etcfs.
- [x] Node kill, all five backends, under one identical fault.
- [x] Metadata concurrency, all five backends.
- [x] Node-count scaling (2/4/6), all five backends.
- [x] Elasticity — clean leave and rejoin under load, all five backends. New
      scenario (`bench-elasticity.sh`), written for the scalability/elasticity
      question below.
- [x] Join latency, etcfs, without the reattach a clean leave no longer causes.
- [x] Cross-node handoff, etcfs, on a 16,000-IOPS volume and through the
      explicit publish.
- [x] Arena fragmentation soak, etcfs, six hours.
- [x] 4 KiB IOPS comparison, all five backends plus the etcfs page-cache
      variant.
- [x] Single-node ceiling, fsync-heavy writes, small-file storm, etcd degraded,
      online volume growth, leave/rejoin under load — etcfs re-runs.

Outstanding:

- [ ] `bench-node-scaling.sh` at 8 and 16 nodes — the point where the etcfs and
      GFS2 metadata curves would cross. Needs EC2 capacity beyond what three
      concurrent clusters left free.
- [ ] `bench-arena-soak.sh` headline metric — `avail_lost_per_live_byte`
      baselines on the script's first sample, which is taken before the churn
      has written anything, so the ratio is mostly the filesystem filling up for
      the first time (19.4 over six hours, 27.4 over ten minutes, neither
      meaningful). Baseline it on the first sample after live bytes stabilise.
- [ ] Arena accounting across membership cycles — `bench-rejoin-load.sh` ended
      three leave/rejoin cycles owning one more arena than it started with
      (3 → 4). Explained by an in-flight reclaim, but that is a guess; sample
      the count again after a settle period, or over more cycles.
- [ ] Cross-node handoff on an instance class with more EBS bandwidth. At
      256 MiB/s the 8 GiB handoff is now pinned to the t3.medium's own device
      ceiling (254 MiB/s measured), so the scenario currently measures the
      hardware rather than any backend.

# New benchmarks for Scalability, Fencing & Recovery

Done, in two scenarios:

- **Fencing and recovery** — `bench-node-kill.sh`, rewritten so all five
  backends take one identical fault, the moment of death is measured at 5 Hz
  rather than assumed, and a takeover probe forces the survivor to acquire a
  lock the dead node held (the only probe that exercises recovery at all).
  Report: `node-kill-recovery.md`.
- **Scalability and elasticity** — `bench-elasticity.sh`, new: a node leaves and
  rejoins cleanly under load while the numbers are taken on the *other* nodes.
  Report: `elasticity.md`. Its finding is that a planned membership change costs
  nobody the world, GFS2 included; the stop-the-world GFS2 is known for shows up
  under an *unplanned* departure, which is the node-kill scenario.
