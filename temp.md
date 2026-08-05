# EtcFS — system explainer for your tutor conversation

Written from direct code inspection, not from the architecture docs alone — several of
those docs describe planned/aspirational behavior that differs from what actually runs.
Every claim below is either verified against source or explicitly marked as unverified/
aspirational. Where a doc contradicts the code, the code wins and the doc is flagged.

---

## 1. What it is, in one paragraph

EtcFS is a cluster filesystem for a shared raw block device (AWS EBS Multi-Attach). It
throws out the traditional design — an on-disk filesystem format (inodes, bitmaps, a
journal) plus a bolted-on Distributed Lock Manager to arbitrate access to it (this is
what GFS2/OCFS2 do) — and inverts it: **etcd's replicated Raft log is the only durable
truth for every structural fact** (what files exist, metadata, locks, who owns which
disk range). The shared disk itself holds nothing but file bytes, addressed by extents
`(logical_offset, disk_offset, length)` recorded in etcd. No on-disk format, no kernel
module, no custom DLM.

This is the direct outcome of the project that came before it (QAttach), which tried
the opposite approach — keep GFS2 but replace its kernel-level lock manager with an
etcd-backed one — and hit an architectural wall. See §5 for why that failure shaped
every major decision here.

## 2. Architecture at a glance

Two processes per node, not one:

- **`etcfuse` (C)** — the FUSE frontend. Owns the libfuse session, translates kernel VFS
  calls into IPC requests. Nothing here talks to etcd or the block device directly.
- **`etcfuse-meta` (Go)** — owns the etcd client, the block device fd, membership,
  fencing. Talks to the C process over a length-prefixed binary protocol on a Unix
  socket.

**Why split them:** FUSE needs a responsive single-threaded event loop answering kernel
upcalls. etcd and block I/O involve network round trips and variable-latency retryable
failures — goroutines, connection pools, retry logic. Mixing the two means a slow etcd
call stalls the kernel-facing loop. Splitting means neither failure/latency model
contaminates the other.

Four logical subsystems inside the Go daemon: metadata client (`pkg/metadata`), data
engine (`pkg/blockio`, `pkg/arena`), membership/fencing (`pkg/membership`,
`pkg/fencing`), continuous verification (`pkg/scrub`, `pkg/fsck`, `pkg/compaction`).

## 3. Metadata model, briefly (you said you don't need the schema)

Everything is a key in etcd: `inode:<ino>`, `dirent:<parent>/<name>`, `lock:<ino>`,
`arena:<node_id>`, `gen:<node_id>`, `extent:<ino>/<chunk>`, `membership:<node_id>`. One
thing worth knowing regardless: **inode allocation and arena allocation are both a
single global etcd counter, CAS-retried on conflict** — not the per-node sharded ranges
the README describes (this is a real doc/code mismatch, detailed in §7).

---

## 4. The safety model — what's actually guaranteed

This is the part your tutor will probe. Three independent mechanisms combine; none of
them alone is sufficient, and knowing *why* each exists is more useful than memorizing
what it does.

### 4.1 Cache coherence (single-node-writes-visible-everywhere)

Three cache layers can make a write invisible to another node: the writer's kernel page
cache, the NVMe controller's write cache, and EBS Multi-Attach's own propagation delay
between attachments. The write path handles all three explicitly:

1. Acquire exclusive lock on the inode (see §4.2)
2. Write via `O_DIRECT` (bypasses kernel page cache entirely)
3. `BLKFLSBUF` ioctl — flushes the NVMe controller's write cache
4. `sync_file_range` — second barrier
5. **Read-back verify** — the writer reads back what it just wrote before declaring
   success. If it reads stale data, it retries.
6. Commit the extent to etcd (generation-guarded, see §4.3)
7. Release lock

The read path mirrors this: the reader issues its own `BLKFLSBUF` before reading (to
invalidate *its* stale cache), then reads via `O_DIRECT`.

**What this actually guarantees:** if a write returns success, the data is durably on
the shared volume and any subsequent read from any node will see it. The exclusive
lock held for the full write+verify cycle means no reader ever observes a torn or
partial write.

**What it does not guarantee:** the *kernel's* view of file metadata (size, in
particular) can lag behind etcd's committed state until `attr_timeout` expires or a
watch-driven invalidation arrives (see below). The extent data itself is never stale —
only the kernel's cached `stat()` result can be, for up to `attr_timeout` (1s default).

**Watch-driven invalidation** closes the *namespace* half of this gap: every node
watches the `dirent:` prefix in etcd. When any node creates/deletes/renames, the watch
fires on every other node's Go daemon, which pushes a `fuse_lowlevel_notify_inval_entry`
call into the kernel via a dedicated notification socket. This is what makes cross-node
`ls`, `stat`, and `open` work correctly without polling. It does **not** cover inode
attribute changes (size after truncate) — those rely purely on `attr_timeout` expiring.

### 4.2 Locking — two classes, and an important honest gap

**Data locks** (`lock:<ino>`, shared/exclusive, lease-backed) are what the read/write
path above actually uses. Acquisition is a CAS transaction; the lease has a TTL (2s in
the cache-coherence doc's description of the read/write path; 5s default in the
concurrency-control doc for the general model — these numbers differ between docs, I
did not fully reconcile which is authoritative for which code path, flag this if it
comes up). If the holder crashes, etcd's own lease-expiry mechanism deletes the lock
key automatically — no crash-recovery protocol needed, no risk of the lock being stuck
forever waiting on a dead node to release it.

**Namespace mutations** (create/delete/rename) use **no lock at all**. Every mutation
is a single atomic etcd transaction (e.g., create = "insert dirent if absent, insert
inode if absent," both in one `Txn`). Two nodes creating different files in the same
directory never contend on anything except the global inode counter. This is the
direct opposite of GFS2's model, where every namespace op needs the directory's glock.

**The honest gap — POSIX `fcntl`/`flock` locks are not enforced at all, currently.** I
verified this directly in `internal/ipc/handlers.go`:

```go
func (s *Service) handleGetlk(...) {
    // always returns F_UNLCK — "range reported free" — no conflict is ever detected
}
func (s *Service) handleSetlk(...) {
    return okResp(), nil  // always succeeds, unconditionally
}
```

So if two processes on different nodes call `fcntl(fd, F_SETLK, ...)` on overlapping
byte ranges, **both succeed**, and neither is actually granted exclusivity against the
other. The `lock:<ino>` mechanism described above is used internally by the read/write
data path (whole-inode, one lock per operation) — it is a completely separate thing
from the POSIX advisory locks an application asks for by name. If your tutor asks "is
`flock`-based coordination between processes on different nodes safe," the honest
answer is **no, not yet** — this is documented in the architecture docs themselves as
deferred ("Phase 7"), and I confirmed the deferral is still current in the actual
handler code.

### 4.3 Fencing — three layers, but one doesn't do what the docs say it does

This is the mechanism that prevents a node that's lost contact with the cluster (but
is still alive and potentially still writing) from corrupting shared state. I read
`pkg/fencing/watchdog.go`, `pkg/fencing/controller.go`, and the generation-guard code
directly, because several architecture docs describe a mechanism that isn't there.

**Layer 1 — Self-fencing (real, verified).** Each node polls its own etcd lease health.
If the lease has been unreachable for **2× the lease TTL** (default 5s TTL → 10s total),
the node's watchdog calls `os.Exit(77)`. This is deliberately just a process exit, not
a graceful shutdown — the reasoning documented (and consistent with the code) is that
once a node can't trust its own view of the world, trying to flush/clean up locally
could itself cause corruption. Process death forces the kernel to tear down `/dev/fuse`
and return `EIO` to any open handles, which is the correct behavior for a node that no
longer trusts itself.

**Layer 2 — External fencing (real behavior is simpler than documented).** Several
docs (`self-fencing-watchdog.md`, `concurrency-control.md`, and the README) describe
this as: the controller detects membership-lease expiry, calls a cloud API to detach
the EBS volume from the dead instance, **polls until dual-confirmed**, and only then
bumps the fencing generation. **I checked `pkg/fencing/controller.go` directly — none
of that exists.** There is zero AWS SDK usage anywhere in `pkg/fencing`. The actual
code:

```go
// fenceNode: called the instant a membership key's lease expires
currentGen, _ := c.store.GetGeneration(ctx, nodeID)
newGen, _ := c.store.BumpGeneration(ctx, nodeID, currentGen)
// that's it — no volume detach, no polling, no dual confirmation
```

The controller's own doc comment in the source is honest about this: *"In production,
the Controller is backed by AWS APIs... For local testing, the Controller bumps the
generation directly."* But there is no code branch that does the AWS-backed version —
it's aspirational, described in a comment, never implemented. **The generation bump
happens immediately on lease expiry, with no confirmation of anything beyond "the
lease expired."** This is a materially weaker guarantee than the docs claim, and if
your tutor asks about it, the accurate description is "single-signal fencing on lease
expiry," not "dual-confirmed detachment."

**Layer 3 — Generation guard (real, and this is what actually carries the safety
argument, given Layer 2 is weaker than advertised).** Every etcd transaction that
mutates metadata carries a comparison against the node's own fencing generation,
established at daemon startup and cached for the process's lifetime (never re-read —
this is deliberate: if it re-read the generation on every write, a write issued *after*
a fence would just read the new value and pass its own guard trivially). If the
generation has been bumped since startup, every subsequent transaction from that node
fails, forever — the node cannot mutate anything else until it restarts and gets a
fresh generation.

I made this guard **store-wide** in the last work session (previously it only covered
extent writes on the data path; namespace mutations — create/mkdir/unlink/rename/
setattr — went through unguarded, meaning a fenced node could still corrupt the
*namespace* even though it couldn't corrupt file bytes). It's now applied uniformly to
every `Store.Txn`/`Put`/`Delete`/`DeletePrefix` call, with three explicit exceptions
that must stay unguarded (creating the generation key itself, and the two operations
that constitute the fence — guarding those would make fencing impossible). Verified
21/21 on real Docker and AWS clusters.

**Net effect of the three layers together:** self-fencing is fast (~10s) but advisory
(a stuck process might not exit). External fencing in its *actual* implementation is
just "bump on lease loss" — fast, but not confirmed against anything physical. The
generation guard is the actual backstop: even if both prior layers fail to stop a
node's process, its writes to etcd will be rejected once *any* fence has been recorded
for it. This is why the guard's correctness matters more than the docs' description of
Layer 2 would suggest — it isn't a third redundant check, it's doing most of the real
work.

### 4.4 The Kleppmann stale-write hazard — the actual open question

This is the most substantive design document in the repo
(`docs/architecture/storage/kleppmann-stale-write-analysis.md`) and the best thing to
bring up if your tutor wants to test whether you understand the real risk surface, not
just the marketing description.

**The classic argument:** a client's lease can expire between the moment it checks
validity and the moment its write physically lands (GC pause, scheduling delay). A
second client acquires the lease, writes correctly. The first client's delayed write
then arrives and clobbers it. The fix (a fencing token validated **at the storage
resource**) doesn't work here because **EBS Multi-Attach has no such mechanism** — every
attached instance can write every sector at any time, unlike a SAN with SCSI-3
Persistent Reservations.

**Why EtcFS survives this anyway, for file-data writes specifically:** not because the
stale write is prevented — it can still land on disk — but because it's made
*unreachable*. Three things combine: (1) data is written to disk before the extent
metadata is committed, and reads resolve bytes *only* through the committed extent
list, so orphan bytes nothing points to are invisible; (2) the commit that publishes an
extent is the generation-guarded transaction from §4.3 — a fenced node's publish fails,
full stop; (3) extents are append-only (never overwritten in place), so even an
in-flight stale write from a fenced node can only land in *its own* previously-claimed
range, never inside a range some other, live node is now using.

**Where the hazard actually still applies — this is the important part.** All three of
those protections assume two nodes never believe they own the same disk arena. If that
assumption breaks, the guard has *nothing to reject*, because both writers are healthy,
unfenced nodes with valid current generations — every check passes, and the two files
silently share and corrupt each other's bytes. This did happen: `Allocator.
existingArenaIDs` used to scan the **entire** `arena:` prefix on restart (every node's
arena, not just its own) and adopt all of them into its own free-list. A node
restarting would then hand out offsets inside a live peer's arena — no fence, no
partition, no fault injection needed, an ordinary restart was sufficient. This was
fixed (recovery now scopes strictly to the node's own record), and it's covered by a
real regression test (`pkg/arena/allocator_integration_test.go`, confirmed to fail
without the fix) plus chaos scenarios S8–S10 run on both Docker and AWS.

**What's still open (documented, not fixed, no production caller yet so not urgent):**

- `RebalanceArena` transfers arena ownership with **no generation guard, no lease
  check, no drain of in-flight writes**. It has zero production callers today — but if
  it ever gets one (e.g., as part of a future load-balancing feature), it's a direct
  re-opening of exactly this hazard.
- Freed arenas (`free_arena:` keys) are recorded but never reused — so there's no
  reclamation path to get wrong yet, but this also means arena space currently leaks
  permanently on graceful node departure.
- Reads don't check the generation stamp on extents inline — only the offline scrubber
  does, after the fact. A stale-generation extent would currently be served to a reader
  rather than rejected at read time.

The four invariants the docs state must hold for *any* future change to allocation:
disjoint ownership at every instant, recovery reads only the node's own records (never
widens a claim from a cluster-wide scan), an extent is only reachable via a
generation-guarded commit, and an arena can only be reissued once the previous owner is
*provably* done with it — and since EBS gives no such proof, that last one can only
ever be a time-bound argument, not something etcd state alone can certify. That fourth
invariant currently has no implementation, which is an honest, explicitly stated
limitation, not an oversight.

---

## 5. Why this design, not another one (QAttach, and the road not taken)

The most useful thing you can say to a tutor asking "why did you build it this way" is
the negative space — what was tried and rejected, and the specific technical reason.

**Why not keep GFS2 and just swap the lock manager for etcd (QAttach, the project
before this one)?** GFS2's `lm_lockops` interface assumes a lock manager that responds
in **microseconds** (DLM, with a single master node per lock serializing requests with
no race window). etcd/Raft consensus operates in **milliseconds** — about 5ms per lock
operation, 50–65ms for a full contended handoff cycle in measured tests. In that
window, GFS2's `find_first_holder` check (which prevents demoting a glock while a
process still holds it — a real correctness guarantee, not a bug) stays true longer
than expected, and the node that just released a lock tends to re-request it before the
intended next holder can claim it — a race that, without DLM's single-master
serialization, an etcd-backed protocol loses systematically. A second, more structural
problem: DLM supports converting a lock's mode in place (exclusive → shared) without a
full release/reacquire cycle; etcd has no equivalent, so every demotion needs a full
unlock-then-reacquire, opening a window where the lock is briefly unowned and the
holder can race back into it. Twelve different fixes were tried; none closed the gap,
because the cause wasn't a bug in a particular fix — it was a synchronous kernel
interface fundamentally mismatched to an asynchronous, higher-latency consensus
backend. This is why EtcFS moved entirely to userspace (no kernel module, no
`lm_lockops`): a FUSE daemon gets to define its own locking semantics against etcd
directly, instead of being forced through an interface built for microsecond-latency
DLM.

**Why etcd/Raft as the *only* source of truth, instead of an on-disk journal (ext4/GFS2
style)?** A traditional journal is a bespoke crash-recovery protocol you have to get
right yourself. etcd's Raft log already **is** a durable, replicated write-ahead
record — every metadata mutation commits to it before the client call returns. Building
on top of that gets atomicity, consistency, and quorum-replication essentially for
free, instead of re-implementing recovery logic. The tradeoff, paid deliberately: every
*structural* operation (create, lock, allocate) costs a network round trip. This is
mitigated by keeping the *hot data path* — reads/writes to already-allocated extents —
entirely on direct block I/O with zero etcd round trips.

**Why no directory locking at all?** Because coarse-grained locks are exactly the
contention GFS2/OCFS2 pay for. Making every namespace mutation a single atomic CAS
transaction against etcd's own serialization gives full concurrency for unrelated
operations in the same directory, with no lock manager needed at all — the tradeoff
being the POSIX-lock gap in §4.2, which is a real, currently-unfilled scope reduction,
not free.

**Why lease-based membership instead of Corosync/CPG (the Pacemaker approach the
project's very first phase used)?** Pacemaker cluster membership changes require a
coordinated, relatively delicate reconfiguration of the whole cluster, with a
transition window where quorum is uncertain — fine for a fixed, known cluster size,
bad for a cluster that needs to scale elastically. A lease-backed key per node makes
joining "start heartbeating and read current metadata" and leaving "stop
heartbeating" — no cluster-wide stop-the-world step. This was verified directly:
12/12 pass on both Docker and AWS for sequential add/remove, and a follow-up test
(9/9, twice) proved two nodes can join **simultaneously** without collision, something
Pacemaker's model would need explicit serialization to attempt safely at all.

## 6. What's tested vs. what's an open gap right now

Verified with real chaos/fault-injection tests on both Docker and real AWS
infrastructure (not just unit tests): daemon crash + restart (individual and all-3
simultaneous), network partition + self-fence + rejoin, fencing-generation bump
rejecting a write, mid-write crash + WAL replay, elastic scale-out/in (sequential and
concurrent joins), namespace mutations correctly rejected on a fenced node (create,
mkdir, unlink, rename, truncate — this was a real gap found and closed in the last
session, see §4.3).

Explicitly open, not yet built: fault injection *during* the join/leave window itself
(killing a node mid-join, partitioning during a graceful leave — this is the next item
being worked on), multi-hour/long-duration fuzzing (longest run so far is 240s), and the
`RebalanceArena`/arena-reuse gaps in §4.4.

## 7. One correction I found and haven't fixed (a good "what would you do differently"
   answer if asked)

The README describes inode allocation as sharded per-node ("a node CASes a small
`inode_range` table once, then hands out numbers locally"). I checked the actual
request path — `Service.allocInode` calls `Store.NextCounter` against **one global
etcd key**, CAS-retried on conflict, on every single file creation, from every node.
The per-node-range code (`Manager.ReserveInodeRange`) exists but has zero callers
outside the test harness. This is a real, live decision point, not a bug: the global
counter is simpler and fine at current scale (a handful of nodes, light metadata
churn), but it doesn't scale the way the arena allocator does (which genuinely is
per-node, touching etcd only once per ~1GB of writes). Fixing this means either
correcting the docs to describe reality, or actually wiring the daemon to the
range-based design — a real tradeoff, not a typo, and I left it as an open decision
rather than picking one silently.
