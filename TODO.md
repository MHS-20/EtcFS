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

## Outstanding benchmark work

- [ ] **Re-run the 80k storm and the comparison table.** The headline figures in
      `smallfile-storm.md` and `overview.md` predate the 2026-08-25 commit
      reduction (lock key in the create transaction, batched key releases) and
      have not been re-measured; each arm is ~40 minutes.
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
