# TODO — features, benchmarks, code quality

Throwaway working list. Not documentation. Nothing here is a known bug; it is
accumulated shape debt and planned work, ordered roughly by payoff. Completed
work is not kept here — it lives in the docs and in the reports under
`docs/reports/`, with `benchmark-reports/overview.md` as the ledger of where
EtcFS wins and loses.

## Next steps, ranked

**1. Decide whether mode and ownership belong under the inode lock.** `getattr`
costs ~1.5 ms per file reading etcd for a record the lock's own snapshot already
holds, and it cannot use that snapshot today: `setattr` changes mode and
ownership under a bare compare-and-set and takes no lock, so a peer can rewrite
those fields of an inode this node holds exclusively. Bringing them under the
lock would make the snapshot authoritative for the whole record — and would make
a `chmod` on a file another node holds force a handover. That is a change to how
`chmod` behaves cluster-wide, not an optimisation, and it is the prerequisite
for the `getattr` saving rather than a separate idea. `getxattr` (~1.5 ms more)
needs the same question answered for xattr keys.

## Pending benchmark work

- [ ] **Multi-hour fuzzing beyond two hours.** A two-hour run (319,465
      operations, 317 injected faults) is clean on memory, file descriptors and
      store size. That earns "stable for two hours", not "no slow-leak class of
      bug exists".
- [ ] **The arena soak's residual drift.** Allocatable space still trends down
      1.50 GiB/h over half an hour after the reclaim fix, against 25.55 before
      it. Too small to reach ENOSPC and too large to call zero; separating a
      smaller leak from the scrubber's cadence needs allocator-level free-block
      accounting rather than `df`.
- [ ] **GFS2's takeover under fencing.** With `fence_aws` confirmed at ~10 s the
      survivors keep serving, but the dead node's inode was still not recovered
      inside 180 s. Needs `dlm_tool ls` and the recovery journal inspected while
      it is happening to say whether a step is missing from the setup or that is
      GFS2's own behaviour.


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
