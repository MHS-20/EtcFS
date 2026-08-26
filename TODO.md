# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`, with `benchmark-reports/overview.md` as the ledger of where
EtcFS wins and loses.

## Next steps, ranked

**1. Label the last unattributed commit.** `etcfuse_etcd_txn_origin_total` puts
1.026 commits per file under `other`, which is the extent publication at
`close()` — the tag on `flushLocked` does not reach whatever path actually
commits it. Small, and it is the only part of the storm's cost still unnamed.

**2. Decide whether mode and ownership belong under the inode lock.** `getattr`
costs ~1.5 ms per file reading etcd for a record the lock's own snapshot already
holds, and it cannot use that snapshot today: `setattr` changes mode and
ownership under a bare compare-and-set and takes no lock, so a peer can rewrite
those fields of an inode this node holds exclusively. Bringing them under the
lock would make the snapshot authoritative for the whole record — and would make
a `chmod` on a file another node holds force a handover. That is a change to how
`chmod` behaves cluster-wide, not an optimisation, and it is the prerequisite
for the `getattr` saving rather than a separate idea. `getxattr` (~1.5 ms more)
needs the same question answered for xattr keys.

**3. Guardrails on the comparison harness.** Refuse to start a run while another
is in flight, and check `CPUCreditBalance` after any burstable-instance run. The
combination of the two cost most of a day and manufactured a 1.54x "regression"
that did not exist.

**4. Decide about the batched cross-inode flush.** `Service.flushEntries` is in
the tree, is correct, and is unmeasured — and it cannot help the small-file
storm by construction, since `close()` publishes each file before the interval
sweep ever sees it. Either measure it where it applies (concurrent writers
across many inodes) or take it out. Unmeasured performance code is debt.

**5. `bench-node-scaling.sh` at 8 and 16 nodes.** The one measurement that would
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
- [ ] **Re-measure the five-way comparison on current code.** Every competitor
      row in the storm and metadata reports is `t3.medium` against an etcfs build
      that predates the 2026-08-26 commit reductions (~6.2 per file to 4.18).
      The published multiples are therefore against an older etcfs than the one
      in the tree.
- [ ] **A real-infrastructure device-fencing chaos run.** The bulk of the chaos
      suite runs against Docker, which cannot exercise device-enforced fencing at
      all — there are no NVMe reservations on a loopback device. The provisioned
      run is owed, and its cost is the reason it has not happened.
- [ ] **Multi-hour fuzzing.** One hour-scale run (279k operations, 158 injected
      faults) looks clean on memory, file descriptors and store size. That earns
      "stable for an hour", not "no slow-leak class of bug exists".
- [ ] **Paired A/B for any storm wall-clock claim.** Five runs that day spanned
      1159–1440 s with four of them the same build, because each run provisions
      its own cluster. Wall clock at n=1 cannot resolve anything smaller than
      ~20%; run both builds on one cluster, alternating, or quote commits.
- [ ] **Cross-node handoff on a larger instance class.** At 255.95 MiB/s the
      8 GiB handoff is pinned to the t3.medium's own EBS ceiling (254.14 MiB/s
      measured on the same instance), so the scenario currently measures the
      hardware rather than any backend.


# Future Extensions

Sized by effort and by how far the change reaches.

**Backup and restore. Large; allocator, metadata, new tool.** Nothing today could
restore this filesystem's data if the shared device were lost. The path is clean
— two etcd revisions diff to exactly the changed extents — but a backup that
reads a block already reused reads another file's bytes into itself, so it needs
the same pinning machinery snapshots do. That pinning is the work.

**Snapshots. Large; same pinning, plus a namespace clone.** Shares all of its
hard part with backup, so the two are one project rather than two.

**Cross-node `fcntl`/`flock`. Medium; a key namespace and two handlers.** Today
`SETLK` always succeeds and `GETLK` always reports the range free, so an
application coordinating through file locks silently gets nothing. `SETLKW`
needs blocking semantics against a lease, which is the design cost. Unrelated to
the per-inode lease lock the data path uses, which works.

**A production caller for arena rebalancing. Small; contained.** The mechanism
exists and nothing invokes it, so an imbalanced cluster has no remedy.

**Shard the inode counter. Small to medium; `inodealloc.go` and one key.**
Contention grows with node count by design; named as the structure most likely
to need reworking first if metadata-creation throughput becomes a target.

**Cross-file / cross-directory atomicity. Structural; not planned.**
Cross-file / cross-directory atomicity does not exist. Each inode is independently consistent; there is no multi-inode transaction, no snapshot spanning several files. An application that needs "these three files change together, atomically, cluster-wide" gets nothing from the filesystem for that — it has to build it itself.
