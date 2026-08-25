# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`, with `benchmark-reports/overview.md` as the ledger of where
EtcFS wins and loses.

## Ideas to improve the worst cases

A measured loss from `overview.md` with no decision recorded against it yet.
The rest have one — see `docs/design-decisions.md`.

**Single-node random writes — 21.7% of the raw device's IOPS.**

- *Profile before proposing anything.* 78% of the device's IOPS is going
  somewhere between the FUSE upcall, the IPC round trip and the O_DIRECT write,
  and nothing here says which. The sequential number retains 65.6%, so the loss
  is per-operation rather than per-byte, which points at the round trip. io_uring
  on the data path and a larger `max_write` are the candidates once there is a
  profile to justify one.

## Next steps, ranked

**1. Split the cache-invalidation channel.** `pkg/fuse/fuse.c`'s `notify_thread`
serves two kinds of traffic on one serial loop: `NOTIFY_INVAL_ENTRY`, which is
fire-and-forget and arrives once per create from the dirent watch, and
`NOTIFY_INVAL_INODE`, which the backend *blocks on* because it may not yield an
inode's lock until the kernel's pages for it are gone. An unpacking archive
floods the thread with the first, the second queues behind thousands of them,
the backend's read deadline expires, and the connection is dropped as out of
step — which disables page caching for the whole mount until the next open
re-enables it, and then it happens again.

This is head-of-line blocking between acknowledged and unacknowledged traffic
sharing one channel, not slowness, so the fix is to separate them: their own
socket and thread, or an entry-invalidation queue the notify thread drains
without holding up acks. It is what blocks putting the inode's lock in the
create transaction — one Raft commit per created-and-written file — and it is a
fragility that will bite any future change touching invalidation concurrency.
See `docs/design-decisions.md#the-create-time-lock-key-was-reverted`.

**2. Guardrails on the comparison harness.** Refuse to start a run while another
is in flight, and check `CPUCreditBalance` after any burstable-instance run. The
combination of the two cost most of a day and manufactured a 1.54x "regression"
that did not exist.

**3. Decide about the batched cross-inode flush.** `Service.flushEntries` is in
the tree, is correct, and is unmeasured — and it cannot help the small-file
storm by construction, since `close()` publishes each file before the interval
sweep ever sees it. Either measure it where it applies (concurrent writers
across many inodes) or take it out. Unmeasured performance code is debt.

**4. `bench-node-scaling.sh` at 8 and 16 nodes.** The one measurement that would
support the central claim. Over 2/4/6 nodes GFS2 loses 47% of its
shared-directory metadata throughput while EtcFS gains 34%, but GFS2 is still 4x
faster in absolute terms at six, so "more scalable" is not claimable yet.

## Pending benchmark work

- [ ] **Benchmark on non-burstable instances, serially.** `t3.medium` runs out
      of CPU credits within minutes of a sustained untar — CloudWatch showed
      `CPUCreditBalance` pinned at 0 for the whole of an hour-long run — and
      several clusters were running concurrently on one account. That combination
      manufactured a 1.54x "regression" out of nothing. Use
      `ETCFS_INSTANCE_TYPE=m5.large` for long runs, one at a time, and check the
      credit balance afterwards. The storm's run-to-run spread at 10k files on
      t3.medium is ±20%, which is larger than anything the commit count can move.
- [ ] **A fenced GFS2 comparison.** The node-kill run measures an *unfenced*
      GFS2 cluster, where the survivors' lockspace stops for good — so there is
      no recovery time to divide by, and none should be quoted. Configure real
      STONITH (`fence_aws` stops the instance through the AWS API) and re-run
      `bench-node-kill.sh` to get a like-for-like number against EtcFS's 2.19 s.

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


# Future Extension
Cross-file / cross-directory atomicity does not exist. Each inode is independently consistent; there is no multi-inode transaction, no snapshot spanning several files. An application that needs "these three files change together, atomically, cluster-wide" gets nothing from the filesystem for that — it has to build it itself.
