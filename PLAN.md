# Plan — cut etcd round trips off the data path

Throwaway working list. Not documentation.

Goal: with a cached lock, a read should be pure device I/O and land near the
disk's IOPS ceiling. Today it costs two linearizable etcd reads.

Baseline (1000-IOPS io2): etcfs 237 randwrite / 208 randread, raw 1033/1006.
After sections 1–4: etcfs 331 randwrite / 1015 randread. Reads are at the
device ceiling and done. Writes still pay one Raft commit each — sections 7–9.

## 0. Measure first

- [x] Per-stage timing on read + write path via `pkg/metrics` histograms
      (etcd read, etcd commit, device, FUSE/IPC). Added `EtcdReadDuration`
      (`Store.Get`/`getPrefix`/`GetRevision`) and `BlockIODuration`
      (`Device.ReadAt`/`WriteAt`); `EtcdTxnDuration` and `FuseOpDuration`
      already existed and cover commit + end-to-end.
- [ ] Re-run the 1000-IOPS io2 benchmark with the new histograms scraped and
      confirm the decomposition sums close to end-to-end handler latency.
- [ ] remove instrumentation in code

## 1. Cheap wins (no new invariants)

- [x] `WithSerializable()` on read path's `GetInode` + `GetExtents` — done as
      part of the fold below (`Store.GetInodeAndExtents`).
- [x] Pin serializable reads to the local colocated etcd member:
      `-etcd-local-endpoint` + `Store.SetLocalClient`; every read tries the
      pinned client first and falls back to the round-robin one.
- [x] Fold inode + extents into one unconditional `Txn` (`Store.readTxn`,
      also now used by `GetMany`). 2 RPCs → 1.
- [x] Re-benchmark. Expect most of the win here.

## 2. Lock coverage gap (prerequisite for 3, and a latent bug now)

`lockInode` has only two callers. These mutate extents unlocked:

- [x] `truncate` (via setattr / `O_TRUNC`) — exclusive lock in both handlers
- [x] `handleFallocate` — exclusive lock, both served modes
- [x] scrubber extent deletes — no lock (whole-key-space pass); the reclaiming
      delete is now conditional on the record's ModRevision from the scan,
      which closes the double-free window an unconditional delete left open
- [x] Swept the rest: the only extent/size writers are the write path,
      reclaimCovered's callers, growTo and the scrubber, all now covered

Racy today (concurrent truncate vs write); becomes correctness-critical the
moment metadata is cached under the lock.

## 3. Metadata cache under the cached lock (the real fix)

Read → 0 RPCs. Write → 1 commit, extent list maintained from our own delta.

- [x] Cache inode record + extent list in `lockEntry`, valid only while the
      lock is held
- [x] Invalidate on recall, eviction, mode change — all funnel through
      `releaseKeyLocked`; `lock-caching.md` section rewritten
- [x] Tie validity to the lock session — `LockSessionAlive`, checked by
      `ensureLockKey` on every operation
- [x] Decided + documented the stale-read window: today a partitioned node's read
      fails at etcd; cached, it serves stale bytes until the watchdog fires
      (2–3× membership lease, 20–30 s, vs 2 s lock session TTL). This widens a
      safety property. Own the decision explicitly.

## 4. Consistency / corruption review

- [x] Write-after-recall: node writes device bytes, loses lock mid-op, commits
      after another node wrote the same range — is the generation guard enough
      or does the extent CAS carry it?
- [x] Shared-lock reader vs. concurrent reclaim: reader resolves an extent,
      writer buries + frees those blocks, allocator hands them out, reader
      reads reused blocks. Does the shared lock actually close this, given the
      reclaim now rides the write txn?
- [x] Cached shared lock + our own exclusive upgrade: key deleted then
      re-acquired, non-atomic — what can slip into that window?
- [x] Cache eviction under load vs. in-flight op (`isCurrent` check) — confirm
      no path proceeds on an orphaned entry
- [x] Fenced node holding cached locks + cached metadata: reads are not
      generation-guarded at all. Guard reads too, or bound by session validity?
- [x] Extent list maintained incrementally (tier 3) drifting from etcd truth —
      needs a periodic or debug-mode cross-check

## 7. Write delegation — defer extent publication (target: ~1000 randwrite)

A write is device I/O plus one Raft commit (~1 + ~2.2 ms = 331 IOPS). While
this node holds the inode's exclusive lock no peer can even take a shared
lock, so the extent list does not have to be in etcd until the lock is given
up. Buffer it, flush in batches. Accepted trade: writes not yet flushed are
lost on a crash (POSIX-legal, `write()` never promised durability).

### 7.1 Buffer

- [x] `lockEntry` gains pending state next to `meta`: new extents, size/mode
      delta, and the `reclaimPlan`s whose blocks must not be freed yet.
      Guarded by `keyMu` like the rest; only valid while `metaFor == holder`.
      Coalesced by key, and a key's comparison is taken the first time the
      buffer touches it — a later write's revision for that key exists only in
      this node's memory.
- [x] `handleWriteBlock` stops committing: device write, then fold the
      proposal into the buffer, then reply. `meta` is updated in the same step
      (`afterCommit`'s replay applied to the buffer), under the same mutex.
- [x] Blocks stay reserved: `freeReclaimed` moved behind the flush.
      `deferredReclaim` is non-empty only when one write buries more extents
      than a transaction can carry; that write commits synchronously instead.
- [x] Cap the buffer (`maxWriteTxnOps`, 16 MiB) and flush when it fills.

### 7.2 Flush

- [x] Comparisons: the buffered ones, plus `CreateRevision(LockKey(ino, mode,
      ourHolder)) != 0` — exact holder token, not a prefix.
- [x] Triggers: fsync/flush IPC, recall, eviction, observed session loss,
      self-fence, shutdown, buffer cap, sync write, any operation that plans
      from etcd rather than the snapshot, and a timer
      (`--metadata-flush-interval`, default 100ms, 0 = commit per write).
- [x] Recall flushes before `dropCachedLock`, and refuses to yield the key if
      the flush fails.
- [x] Failed flush keeps the buffer and fails every later `fsync` with `EIO`
      until one commits.
- [x] Lease lost / lock key gone / fenced: buffer discarded, blocks freed,
      logged loudly.

### 7.3 Durability surface

- [x] `ec_fsync` and `ec_flush` send IPC and block; `fsyncdir` stays a no-op.
- [x] `O_SYNC`/`O_DSYNC` disable deferral per write, from the write request's
      own flags, forwarded by the C side.
- [x] Measured the async direct-IO path (`fuse_direct_IO` →
      `fuse_async_req_send`, i.e. AIO and io_uring): it carries the same flags.
      io_uring with `O_DIRECT|O_DSYNC` reached the daemon as flags=53250, both
      bits set. Guarantee holds on every submission path; docs widened.
- [x] Flush on `flush` (close) and before any namespace operation naming the
      inode.
- [x] Buffered-IO mode: `fsync` flushes the device too.
- [ ] One run of `nvme id-ctrl` on a real io2 attachment, recording ONCS bit 0
      and FUSES bit 0. Needs hardware; not needed for anything planned.

### 7.4 Local coherence

- [x] `handleGetattr`, `handleLookup` and readdirplus serve this inode's size
      from the buffer while this node holds the lock.
- [x] Metrics: pending extents and bytes, flush latency, flush failures,
      flushes by trigger.
- [x] Re-benchmark: 996 randwrite (from 331), 1004 randread, p99 72ms -> 15ms.
      Required one more fix: the cached extent list was rebuilt per write,
      which became the bottleneck once the commit was gone. seqwrite 128k fell
      52 -> 36 MiB/s, from arena fragmentation left by a 3x faster random
      phase, not from the write path; coalescing at flush is what closes it.

## 8. Caching past the device ceiling

§7 gets writes to the device's ceiling by removing the Raft commit. Neither
read nor write can go past that ceiling while every operation still costs one
device I/O. Going past it means serving from RAM, which is what NFS does and
where its benchmark numbers above the backing store come from: client page
cache on reads, async writes plus COMMIT on writes.

Both are legitimate here for one reason only — the lock. While this node holds
an inode's lock no peer can read or write it, so RAM copies of its *data* are
as sound as the metadata snapshot already cached beside them. Recall is what
makes them safe, and recall is what has to invalidate them.

Caching raises the peak, never the sustained average: a working set larger
than RAM is still device-bound, and a cold random read is device-bound for
everyone. Sequence after §7, before verification, so §9 checks the final
shape rather than an intermediate one.

### 8.1 Read: kernel page cache for held inodes

- [ ] Allow the kernel to cache data pages for an inode this node holds a lock
      on — `keep_cache`, dropping `direct_io` for that open. Today both are off
      unconditionally (`pkg/fuse/ops.c`), decided per open, so the daemon has
      to decide at OPEN and invalidate later if the lock goes.
- [ ] Invalidate on recall, before the key is yielded, via
      `fuse_lowlevel_notify_inval_inode` — the daemon already runs a notify
      socket and already uses `INVAL_INODE` for attributes, so this is the
      existing path pointed at data.
- [ ] Invalidation must not run on a request thread. `notify_inval_inode` can
      block against the kernel's own writeback and deadlock the daemon if
      called from one; the recall path needs its own thread and the peer must
      not proceed until invalidation has completed.
- [ ] Scope it to reads first: keep writes write-through to the daemon rather
      than enabling the kernel's writeback cache. Writeback changes the shape
      of every write request and is a separate project.
- [ ] Expected: re-reads cost nothing, cold reads unchanged at ~1015. Will not
      show in the current benchmark — fio's `direct=1` bypasses the client page
      cache, and the 8 MB files would sit entirely in RAM anyway. Needs a
      benchmark that measures what it changes, or it is unmeasured.

### 8.2 Write: buffer data in RAM, not only metadata

- [ ] Buffer the bytes alongside the pending extents in the `lockEntry` and
      reply before the device write, so a write costs no device I/O either.
      Blocks are still reserved from the arena at write time, so offsets are
      known and the flush has somewhere to put them.
- [ ] **Flush order is data then metadata, always.** Device writes first, then
      the etcd transaction. Publishing an extent whose bytes are not yet on the
      volume is the one inversion that turns a lost write into a read of
      garbage, and it is the invariant the current write path is built around.
- [ ] Reads on this node must consult the buffer before the device, or a node
      cannot read back what it just wrote.
- [ ] Bound the buffer in bytes, with backpressure: past the cap a write waits
      for a flush rather than growing memory without limit.
- [ ] Coalesce adjacent buffered writes into larger device I/Os at flush. This
      is the one part that raises the *sustained* rate rather than the peak —
      4 K random writes become fewer, larger, more sequential ones.
- [ ] Crash exposure is unchanged in kind and larger in size: the bytes are now
      lost with the mapping instead of being stranded on the volume. Observably
      identical — an unpublished extent was unreachable either way — but say so
      in the docs rather than leaving it implied.
- [ ] `fsync` flushes both, in that order, and keeps §7's error semantics.

## 9. Verification — make the guarantees checkable, not argued

Existing machinery to extend rather than replace: `test/verify` (four
Porcupine models: namespace, extent, lock, generation), `specs/Fencing.tla`
(+ 8 `.cfg`s), `scripts/test/chaos-*.sh` (S1–S7), `scripts/test/tla-check.sh`,
`scripts/test/integrity-fuzz.sh`, `test/pjdfstest`.

### 9.1 Invariants to state once and check everywhere

- [ ] Write them down as a numbered list, each with where it is enforced and
      which check would catch a violation:
      1. No two nodes hold conflicting locks on one inode, and no lock
         decision is ever made from a read — only from a transaction, or from
         a cached key whose lease identity still matches the session's.
      2. No node publishes an extent for an inode it does not hold the lock
         for, and no flush commits after the lock key is gone.
      3. Every block referenced by a live extent is owned by exactly one
         inode; no block is freed while an extent references it, and no block
         is freed twice.
      4. A read never returns bytes from a block that was reallocated after
         the extent naming it was resolved.
      5. A fenced node never publishes anything.
      6. Data acked to `fsync` is in etcd; data not acked may vanish but never
         appears half-published.
      7. No cached copy of an inode — metadata snapshot, kernel page, or
         buffered write — survives the yielding of that inode's lock.
      8. No extent is published before the bytes it names are on the volume.

### 9.2 Model checking

- [ ] TLA+: extend `Fencing.tla` (or a sibling spec) with the cached lock,
      modelled as cached and recallable rather than per-operation —
      acquire/want-key/recall/minimum hold time/lease loss while holding one —
      and the delegation: buffered extents and data, flush, flush rejected
      after key loss. Invariants: `NoTwoHolders`, `NoPublishWithoutLock`,
      `NoFlushAfterKeyLoss`, `NoStaleHolderWrite`, plus the existing
      `NoDoubleWriter`/`StaleWriteRejected`. Add `.cfg`s that break each guard
      (the existing `Fencing*Bug.cfg` pattern) so the spec is shown to catch
      its own violations.
- [ ] Model the cached view against etcd truth explicitly: what a node believes
      an inode's extents and size are versus what is published, and that a
      recall reconciles them. This is the property the metadata cache and both
      data caches all rest on, and no current spec names it.
- [ ] Porcupine `lock.go`: hold intervals are now the operation's, a subset of
      the true etcd hold. Confirm sound, or widen to the cached hold.
- [ ] Porcupine `extent.go`: reads may now be served from a cached list, from
      a kernel page this node cached, or from a write still buffered in RAM,
      and writes may be unpublished at crash time. Add an fsync barrier event
      to the history and relax the model to "a read never contradicts any write
      that was flushed, or any write from the same node".
- [ ] New model: block ownership over time, from the extent history — catches
      invariant 3 and 4 directly, which no current model covers.
- [ ] Page-cache invalidation as its own property: no node serves a kernel-
      cached page for an inode after it has yielded that inode's lock. Needs a
      history event for the invalidation, or it cannot be checked at all.

### 9.3 Chaos

- [ ] S8 cross-node contention on one inode: two nodes writing the same file
      so recall actually fires. Nothing in S1–S7 exercises it — the 1000-IOPS
      run was single-client.
- [ ] S9 crash with a full buffer: assert exactly the unflushed writes are
      missing, everything fsynced survives, and no extent references a freed
      block afterwards (run `fsck` + a scrub pass as the assertion).
- [ ] S10 lease loss under sustained write load: node keeps writing, its
      session dies, a peer takes the inode. Assert the partitioned node's
      flush is rejected and its blocks come back.
- [ ] S11 flush failure injection: etcd unavailable at flush time. Assert the
      buffer survives, `fsync` returns `EIO` and keeps returning it, and no
      partial publication.
- [ ] S12 recall storm: many inodes contended at once, assert no deadlock, no
      lock left behind, bounded latency. Recall now invalidates kernel pages
      too, on a path that can deadlock against writeback — this is where that
      shows up if it is wrong.
- [ ] S13 read-after-recall across nodes with page caching on: node A reads
      (kernel caches), node B writes, node A reads again. Must see B's data.
      The single-client benchmark run never exercised any of this.
- [ ] Turn `VERIFY_HISTORY=1` on by default in the docker chaos suite, and
      make a violation fail the run rather than warn.
- [ ] Re-run `pjdfstest`, `integrity-fuzz.sh`, `stress-test.sh` against the
      deferred path.

### 9.4 Race and fault coverage

- [ ] `go test -race` on the full integration suite in CI, not by hand.
- [ ] Fault injection at each flush trigger (kill between device write and
      buffer, between buffer and flush, mid-flush) — table-driven, in-process,
      no cluster needed.

## 10. Narrow the remaining windows

- [ ] Stale read after undetected lease loss: bound is `inodeLockTTL` (2 s).
      Measure the cost of lowering it and pick a number, rather than leaving
      the default unexamined.
- [ ] Detect session loss sooner than the next operation: hook the session's
      `Done()` and drop every cached lock and buffer immediately, instead of
      waiting for `ensureLockKey` to notice. Load-bearing once writes are
      buffered — every millisecond of late detection is acked writes that the
      flush will reject and discard.
- [ ] Lost updates: quantify what the flush interval actually costs — a peer's
      `stat` lag and the crash window are both it. Default 100 ms, document
      the knob, and make `0` (commit per write) a supported setting for anyone
      who wants today's durability back.
- [ ] Re-benchmark after each of 7, 8 and 10; keep the table in
      `docs/architecture/reliability/performance-benchmarks.md` current. §8
      needs a benchmark that a client-side `direct=1` does not defeat, or its
      effect is invisible.
- [ ] Every cache gets an off switch, and the fully-synchronous configuration
      (no delegation, no data caching) stays supported and tested — it is the
      one that loses nothing, and some deployments will want it.
