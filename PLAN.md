# Plan — cut etcd round trips off the data path

Throwaway working list. Not documentation.

Goal: with a cached lock, a read should be pure device I/O and land near the
disk's IOPS ceiling. Today it costs two linearizable etcd reads.

Baseline (1000-IOPS io2): etcfs 237 randwrite / 208 randread, raw 1033/1006.

## 0. Measure first

- [x] Per-stage timing on read + write path via `pkg/metrics` histograms
      (etcd read, etcd commit, device, FUSE/IPC). Added `EtcdReadDuration`
      (`Store.Get`/`getPrefix`/`GetRevision`) and `BlockIODuration`
      (`Device.ReadAt`/`WriteAt`); `EtcdTxnDuration` and `FuseOpDuration`
      already existed and cover commit + end-to-end.
- [ ] Re-run the 1000-IOPS io2 benchmark with the new histograms scraped and
      confirm the decomposition sums close to end-to-end handler latency.

## 1. Cheap wins (no new invariants)

- [x] `WithSerializable()` on read path's `GetInode` + `GetExtents` — done as
      part of the fold below (`Store.GetInodeAndExtents`).
- [x] Pin serializable reads to the local colocated etcd member:
      `-etcd-local-endpoint` + `Store.SetLocalClient`; every read tries the
      pinned client first and falls back to the round-robin one.
- [x] Fold inode + extents into one unconditional `Txn` (`Store.readTxn`,
      also now used by `GetMany`). 2 RPCs → 1.
- [ ] Re-benchmark. Expect most of the win here.

## 2. Lock coverage gap (prerequisite for 3, and a latent bug now)

`lockInode` has only two callers. These mutate extents unlocked:

- [ ] `truncate` (`datapath.go:555`, via setattr / `O_TRUNC`)
- [ ] `handleFallocate` (`sparse.go:84`)
- [ ] scrubber extent deletes (`scrub/scrubber.go:180`) — decide: take the
      lock, or justify why owning-node + dead-extent checks are sufficient
- [ ] Check for other unlocked extent/size writers before assuming this list
      is complete

Racy today (concurrent truncate vs write); becomes correctness-critical the
moment metadata is cached under the lock.

## 3. Metadata cache under the cached lock (the real fix)

Read → 0 RPCs. Write → 1 commit, extent list maintained from our own delta.

- [ ] Cache inode record + extent list in `lockEntry`, valid only while the
      lock is held
- [ ] Invalidate on recall, eviction, mode change — the GFS2 obligation.
      `lock-caching.md` currently says "nothing to invalidate"; that sentence
      becomes false and must be rewritten, not left.
- [ ] Tie validity to `concurrency.Session.Done()` — on lease loss, drop
      every cached lock *and* its metadata
- [ ] Decide + document the stale-read window: today a partitioned node's read
      fails at etcd; cached, it serves stale bytes until the watchdog fires
      (2–3× membership lease, 20–30 s, vs 2 s lock session TTL). This widens a
      safety property. Own the decision explicitly.

## 4. Consistency / corruption review

- [ ] Write-after-recall: node writes device bytes, loses lock mid-op, commits
      after another node wrote the same range — is the generation guard enough
      or does the extent CAS carry it?
- [ ] Shared-lock reader vs. concurrent reclaim: reader resolves an extent,
      writer buries + frees those blocks, allocator hands them out, reader
      reads reused blocks. Does the shared lock actually close this, given the
      reclaim now rides the write txn?
- [ ] Cached shared lock + our own exclusive upgrade: key deleted then
      re-acquired, non-atomic — what can slip into that window?
- [ ] Cache eviction under load vs. in-flight op (`isCurrent` check) — confirm
      no path proceeds on an orphaned entry
- [ ] Fenced node holding cached locks + cached metadata: reads are not
      generation-guarded at all. Guard reads too, or bound by session validity?
- [ ] Extent list maintained incrementally (tier 3) drifting from etcd truth —
      needs a periodic or debug-mode cross-check

## 5. Formal verification updates

- [ ] TLA+ (`specs/Fencing.tla`): model the lock as cached/recallable, not
      per-operation. Add want-key + recall, minimum hold time, and lease loss
      while holding a cached lock. Assert no two nodes hold conflicting locks
      and no stale holder writes.
- [ ] New or extended spec covering metadata caching once tier 3 lands: cached
      view vs. etcd truth, invalidation on recall.
- [ ] Porcupine lock model (`test/verify/lock.go`): hold intervals are now the
      operation's, a subset of the true etcd hold. Confirm the checker is
      still sound with that and does not need the wider interval.
- [ ] Extent model: check it still holds when reads are served from a cached
      list rather than a fresh etcd snapshot.
- [ ] Chaos: add a scenario driving cross-node contention on one inode so
      recall actually fires — the 1000-IOPS run was single-client and never
      exercised it.

## 6. Cleanup

- [x] Dangling refs to the deleted `docs/TODO.md` / `docs/NEXT_STEPS.md` in:
      `AGENTS.md`, `docs/index.md`, `docs/deployment/configuration.md`,
      `docs/architecture/reliability/performance-benchmarks.md`,
      `pkg/metadata/xattr.go`, two files under `docs/reports/`. Docs CI runs a
      link checker — this will fail it.
