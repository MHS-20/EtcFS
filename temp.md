# EtcFS — short version

## What it is
- Cluster filesystem over shared raw block device (EBS Multi-Attach).
- etcd/Raft = only source of truth for metadata. Disk holds only file bytes.
- Two processes/node: `etcfuse` (C, FUSE frontend) + `etcfuse-meta` (Go, etcd + disk + fencing).

## Inodes
- One global etcd counter (`inode_alloc_counter`), CAS-retried, +1 per file creation.
- **Not** per-node sharded (docs used to claim this — wrong, fixed).
- Floor = 2 (inode 1 reserved for root dir — handing it out once broke the whole mount).
- Cost: one etcd round-trip per create, from every node. Doesn't scale with node count.

## Arenas (block allocation)
- 1 GiB disk ranges, one global counter hands out arena IDs.
- A node acquires an arena **lazily**, on its first write — not at join.
- Once owned, node allocates blocks from it locally (no etcd round-trip per block).
- Ownership record (`arena:<node>`) is a plain key — **not lease-bound**.
- **Never reclaimed.** No code path releases an arena on node departure, graceful or not. Leaks permanently.
- Past bug (fixed): restart used to scan *every* node's arenas and adopt them all — two nodes could end up writing into the same arena. Now scoped to the node's own record only.

## Extents
- `extent:<ino>/<chunk>` = `(logical_off, disk_off, len)`, stamped with writer's fencing generation.
- **Append-only** — never overwritten in place. A stale/delayed write can only land in a range the writer already owns.
- Write order: data to disk **first**, then extent committed to etcd. Crash between = orphan bytes, harmless (nothing points to them).
- Truncate: opposite order — metadata first, then blocks freed. Avoids exposing already-reused blocks.
- Reads resolve bytes *only* via committed extents in etcd — nothing else is reachable.
- Generation stamp is **not checked on read** — only the offline scrubber checks it.

## Compaction
- Runs hourly in production (`comp.Run(ctx, time.Hour)`), wired in `main.go`.
- Per-arena: if live-data ratio < 50%, copies live extents to a fresh arena, updates their metadata, returns old arena to a free pool.
- Free pool (`free_arena:`) is **populated but never consumed** — nothing reissues a freed arena yet.

## Scrubber
- Runs every 30s in production, wired in `main.go`.
- Checks (all offline/after-the-fact, not preventive):
  - extent collisions (two inodes claiming same disk range)
  - orphan extents (data with no owning inode)
  - range validity
  - generation consistency (stale-gen extents)
  - nlink consistency
- Does not block or reject anything live — reports only.

## Locking
- **Data lock** (`lock:<ino>`, whole-inode, lease-backed): used internally by read/write path. Works.
- **POSIX `fcntl`/`flock`**: NOT enforced across nodes. `GETLK` always reports free, `SETLK` always succeeds. Deliberately deferred, still true.
- No directory locks at all — namespace ops (create/rename/unlink) are single atomic etcd `Txn`, no lock needed.

## Fencing (3 layers)
1. **Self-fencing**: node polls own lease; if last successful keepalive older than TTL, `os.Exit(77)`. (Fixed this session — used to never fire under a real partition, because the old check only cleared on channel-close, which never happens under a total cut.)
2. **External fencing**: controller watches membership keys, bumps `gen:<node>` on lease expiry. **No cloud API call** — despite docs describing "dual-confirmed EBS detach," that's not implemented. Just bumps generation on lease loss.
3. **Generation guard**: every etcd mutation carries a `Cmp` against the node's generation, cached at startup, never re-read. Once bumped, all further mutations from that node fail — forever, until restart. This is the real backstop; layers 1–2 just narrow the window.

## Known unbounded-latency issue
- FUSE handlers run with `context.Background()` — no deadline.
- Most etcd calls are wrapped in a bounded retry (`retryEtcd`, ~6s ceiling) — but `lockInode` (first call in read/write path) is not, and 35 other direct store calls aren't either.
- Under partition, a request can hang until the daemon self-fences (or forever, pre-fix). Not fixed yet.

## Current guarantees — the honest list

**Held:**
- Write returns success ⇒ data durable + visible cluster-wide (O_DIRECT + flush + read-back verify).
- No reader sees a torn/partial write (exclusive lock held for full write+verify).
- Fenced node's metadata mutations are rejected, permanently, cluster-wide.
- Two live, unfenced nodes cannot both claim the same arena (fixed bug, now tested).
- Namespace ops (create/rename/unlink) are atomic; no half-applied state.
- Crash-safety: WAL reconciles in-flight writes on restart against etcd's committed state.

**Not held / open:**
- Cross-node `flock`/`fcntl` — unenforced.
- Arena space — never reclaimed, leaks on every departure.
- Read path — doesn't check generation stamp, only scrubber does (after the fact).
- FUSE request latency — unbounded under partition/etcd hiccup outside self-fence window.
- `RebalanceArena` — exists, unguarded, no production caller (landmine if wired up later).
