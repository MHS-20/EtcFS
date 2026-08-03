# EtcFS

<https://mhs-20.github.io/EtcFS/>

A cluster filesystem where **etcd/Raft is the only source of durable truth**, and a shared raw block device (e.g. AWS EBS Multi-Attach) holds nothing but file bytes. No on-disk filesystem format, no kernel module, no bespoke distributed lock manager — a userspace FUSE daemon on each node presents POSIX semantics, backed by etcd for everything structural (namespace, inode metadata, locks, allocation) and direct block I/O for file content.

Status: implemented and under hardening (Phases 0–6 built, Phase 11 hardening in progress). See [State](#state) below before relying on this for real data.

## Table of contents

- [The idea](#the-idea)
- [Architecture](#architecture)
- [Metadata model](#metadata-model)
- [Data path and crash consistency](#data-path-and-crash-consistency)
- [Locking](#locking)
- [Fencing and split-brain avoidance](#fencing-and-split-brain-avoidance)
- [Elasticity](#elasticity)
- [Journaling](#journaling--what-replaces-it)
- [POSIX semantics: what's supported, what's not](#posix-semantics-whats-supported-whats-not)
- [How to use it](#how-to-use-it)
- [Testing](#testing)
- [State](#state)
- [Document map](#document-map)

## The idea

Traditional cluster filesystems (GFS2, OCFS2) keep durable truth *on disk* — inodes, bitmaps, a journal — and bolt a distributed lock manager (DLM) on top to arbitrate access to it. EtcFS inverts that: etcd's replicated Raft log *is* the durable truth for every structural fact (what files exist, their metadata, who holds what lock, which extents belong to which file), and the disk is demoted to a flat, unformatted array of bytes that only ever holds file content, addressed by extents `(logical_offset, disk_offset, length)` recorded in etcd.

This means atomicity, consistency, and metadata recovery come from etcd's existing quorum-replicated log almost for free, instead of reimplementing a bespoke recovery protocol. The tradeoff is that every structural operation is an etcd round trip — mitigated by aggressive client-side metadata caching with watch-based invalidation, and by keeping the hot data path (reads/writes to already-allocated extents) entirely on direct block I/O with no etcd round trip at all.

Full rationale: [`docs/architecture/`](docs/architecture/) — read the relevant subsystem doc before making any design decision that touches fencing, the write path, or the metadata schema.

## Architecture

Each node runs two cooperating processes, not one monolith:

```
┌─────────────────────────┐        Unix socket        ┌──────────────────────────┐
│  etcfuse (C)             │  length-prefixed binary   │  etcfuse-meta (Go)        │
│  FUSE frontend           │◄──────── IPC ─────────────►│  metadata + data engine   │
│                          │                            │                          │
│  - libfuse session       │                            │  - etcd client            │
│  - VFS op → IPC request  │                            │  - inode/dirent/lock store│
│  - mounts the filesystem │                            │  - arena allocator        │
└─────────────────────────┘                            │  - O_DIRECT block I/O     │
                                                          │  - membership/fencing     │
                                                          │  - scrubber, compactor    │
                                                          └──────────────────────────┘
                                                                       │
                                                          ┌────────────┴────────────┐
                                                          ▼                         ▼
                                                   etcd cluster            shared raw block
                                                   (metadata,               device (EBS
                                                   locks, fencing)          Multi-Attach)
```

**Why two processes, not one:** FUSE protocol handling needs timely response to kernel upcalls — a single-threaded libfuse event loop, synchronous IPC per request. Metadata and data I/O involve network round trips (etcd) and block device access with variable latency and retryable failures — goroutines, connection pools, retry logic. Splitting them means neither concern's failure/latency model contaminates the other. Full detail: [`docs/architecture/fuse-architecture.md`](docs/architecture/fuse-architecture.md).

The C daemon owns all FUSE state (session, mount, request handles); the Go daemon owns all etcd state (client connection, lease keepalives, watch channels). The wire protocol is a hand-rolled length-prefixed binary format on a Unix socket (`internal/ipc/socket.go`); there's no gRPC/protobuf in the hot path.

Four logical subsystems live inside the Go daemon:

1. **Metadata client** (`pkg/metadata`) — inode table, directory entries, locks, allocator state, all as etcd transactions.
2. **Data engine** (`pkg/blockio`, `pkg/arena`, `pkg/walgo`) — O_DIRECT `pread`/`pwrite` against the shared block device at extents handed down by the metadata layer, plus a small local write-ahead buffer for crash-safety ordering.
3. **Membership/fencing agent** (`pkg/membership`, `pkg/fencing`) — etcd lease heartbeat, watches on cluster membership, self-fencing watchdog, coordination with an external fencing controller.
4. **Continuous verification** (`pkg/scrub`, `pkg/compaction`, `pkg/fsck`) — background scrubbing of etcd metadata against actual disk state, arena compaction, offline consistency checking.

## Metadata model

Everything in etcd is a key:

```
inode:<ino>                    → {mode, uid, gid, size, nlink, mtime, ctime,
                                    extents: [(logical_off, disk_off, len), ...],
                                    generation}
dirent:<parent_ino>/<name>     → <ino>
inode_alloc_counter             → next free inode number (8-byte big-endian; sharded per-node in practice)
lock:<ino>                      → {mode: shared|exclusive, holders: [node_id, ...], lease_id}
arena:<node_id>                 → {disk_range: (start,end), free_list}
arena_alloc_log                 → append-only record of arena grants
membership:<node_id>            → lease-backed liveness key
gen:<node_id>                   → fencing generation/epoch counter (decimal ASCII)
```

Every inode carries its own extent list — extents are only ever touched together with their owning file's metadata, so colocating them keeps the natural transaction boundary (a write commit) aligned with a single etcd key update, no extra round trips. Directory listings are prefix range-scans over `dirent:<parent_ino>/`.

**Why not a single global counter or bitmap:** a lone `inode_alloc_counter` or free-space bitmap becomes a hot key under concurrent create/delete load — every mutation CASes against it. Both inode allocation and block allocation are sharded instead (below). Directory entries and per-file locks don't have this problem, since each key is naturally partitioned by parent/inode.

Note: etcd value encodings are **not uniform** — `inode_alloc_counter` and dirent values are 8-byte big-endian, `gen:<node>` is decimal ASCII, `extent:<ino>/<chunk>` is comma-separated ASCII. Match the existing encoding exactly if seeding keys by hand or via `etcdctl` (inode `1` is reserved for the FUSE root directory — see [`docs/architecture/metadata-schema.md`](docs/architecture/metadata-schema.md) § Reserved inode numbers).

**Sharding hot structures.** Inode numbers are allocated from per-node ranges (a node CASes a small `inode_range` table once, then hands out numbers locally until exhausted). Block allocation uses **arenas** — large contiguous ranges of the raw device (e.g. 1GB) leased exclusively to one node at a time via a transaction against `arena:<node_id>`. A node allocates from its own arena using a local free-list, only touching etcd to acquire or return an arena — roughly once per GB of write activity, not once per block. Both follow the same pattern: infrequent coarse-grained etcd coordination, frequent fine-grained local decision-making.

## Data path and crash consistency

File growth: the node writes data into free space in its own arena, **then** commits the updated extent list to `inode:<ino>` in etcd — data-then-metadata, in that order. An extent is only "real" once etcd has durably recorded it as part of the file; a crash between the data write and the metadata commit leaves orphaned-but-harmless bytes, reclaimed later by compaction, never a file referencing data that was never actually written.

Truncate is the mirror: commit the new, smaller extent list to etcd **first**, then treat the freed range as reclaimable — metadata-then-data, because here the risk being avoided is the opposite one: a reader must never see a shrunk file whose blocks were already reused for something else.

Reads and writes to already-allocated extents go straight to the block device via O_DIRECT at the offsets in the current inode record — no etcd round trip on the hot path.

**Fragmentation and compaction.** Deletes and truncates free ranges within a node's arena without compacting the device, so fragmentation accumulates like any log-structured allocator. A background compaction process (`pkg/compaction`), run per-arena when its live-data ratio falls below a threshold, copies live extents into a fresh arena, atomically updates affected files' extent lists (batched transactions), and returns the old arena to the free pool — throttled to avoid contending with foreground I/O. Orphaned extents from a crash between data-write and metadata-commit are swept up by the same pass once a grace period has elapsed.

## Locking

Two distinct lock classes, deliberately not conflated:

**Data locks** (per-inode, shared/exclusive) — modeled as etcd leases attached to `lock:<ino>`. Acquiring a write lock is a single CAS: succeed only if no conflicting lease exists, then attach a lease renewed by heartbeat while held. Lease expiry is the trigger to *reclaim* a lock from a presumed-dead holder — but reclaiming the lock is **not** sufficient to let another node write until fencing has independently confirmed the prior holder is actually gone (see below). This is the single most safety-critical rule in the system.

**Namespace mutations** (create/delete/rename/mkdir) — never lock a directory. Every namespace change is a single atomic etcd `Txn`: a create is "insert dirent if absent, insert inode if absent" in one transaction; a rename is "delete old dirent, insert new dirent, bump ctime" in one transaction, with a defined key-ordering rule for cross-directory renames to avoid transaction deadlocks. This gives full concurrency for unrelated creates in the same directory.

POSIX `flock`/`fcntl` map onto the data lock class. Shared writable `mmap` across nodes is explicitly **not supported** — it would require a full cross-node cache-coherence protocol this design doesn't build (GFS2 has similar restrictions). `mmap` works fine locally when a node genuinely holds the file's exclusive lock, since no coherence problem exists in that case.

## Fencing and split-brain avoidance

Lease expiry tells you a node stopped renewing its lease — it does **not** tell you the node stopped writing to disk. A node under a long GC pause or a partition that only affects its etcd-facing path (while its EBS path stays alive) can miss heartbeats while still issuing writes. Treating lease expiry as "node is dead" is exactly the bug class that causes silent corruption on Multi-Attach, so three independent layers exist rather than trusting any one:

1. **Self-fencing (first line of defense).** Each node watches its own lease locally. If it fails to renew within a margin (2x the heartbeat TTL), the node assumes it's being evicted and immediately stops issuing writes itself, without waiting to be fenced externally. Same principle as Ceph OSDs / Kubernetes kubelets: if you can't prove membership, assume you've been evicted.
2. **External fencing (guaranteed backstop)**, for a node too wedged to self-fence (a true kernel hang). The controller doesn't act on lease expiry alone — it requires dual confirmation (e.g. EBS force-detach API success *and* polled `DescribeVolumes` state actually reporting detached) before bumping the fencing epoch. Mirrors why production Pacemaker deployments configure two independent STONITH devices.
3. **Generation-stamped extents (detection layer)** for the residual risk neither real-time mechanism eliminates. Every lock grant carries the current fencing generation; every extent write is stamped with the generation active when it was written. The continuous scrubber flags any extent whose stamped generation doesn't match what the metadata layer expects, surfacing a fencing bug within a scrub cycle instead of as silent corruption discovered months later.

Reclaim sequence: lease expires (suspicion) → fencing controller notified via etcd watch, races the node's own self-fencing watchdog → controller waits for dual confirmation → writes a `gen:<node_id>` epoch bump → locks/arenas previously held by that node are reassigned only after the bump, structurally enforced by requiring every lock-grant transaction to CAS against the current generation.

A secondary split-brain vector — etcd's own partition behavior — already benefits from Raft without extra work: anyone partitioned from the etcd majority can't write an epoch bump or acquire a lock, since every metadata mutation is itself a quorum operation. Full detail: [`docs/architecture/fencing-generation-protocol.md`](docs/architecture/fencing-generation-protocol.md).

## Elasticity

Every node registers a `membership:<node_id>` key backed by an etcd lease (2–5s TTL, tunable). A lease expiry is the liveness-loss signal that kicks off fencing. Because membership is just etcd leases rather than a totem/CPG protocol, nodes join or leave far more cheaply than in DLM: joining is "start heartbeating and read current metadata," leaving is "stop heartbeating" — no cluster-wide stop-the-world recovery step for membership churn itself. Adding or removing a node only affects the locks and arenas that node personally held; etcd's watch mechanism surfaces this incrementally, not as a synchronous global barrier. No manual rebalancing step is needed — inode-range and arena acquisition happen automatically on a new node's first write.

Verified in practice: `docs/chaos-reports/2026-07-31-elastic-scale-out-in.md` — 3→5→3 node scale-out/scale-in, both local and on real AWS infrastructure.

## Journaling — what replaces it

There is deliberately no on-disk journal in the ext4/GFS2 sense. **etcd's Raft log is the durable, replicated write-ahead record for all metadata** — every inode/dirent/lock mutation is committed to a replicated log before the etcd client call returns. What's built on top of that is data-path crash safety: the data-then-metadata / metadata-then-data ordering rules above, plus a **small local WAL** (`pkg/walgo`) covering only the short window between issuing a data write and committing its metadata — on restart, a node reconciles any locally-recorded in-flight writes against etcd's current state, discarding anything not reflected in a committed inode record. This is not a full filesystem journal; it never needs to be, because the durable source of truth for "did this write happen" is etcd's log, not the local WAL.

Node restart is cheap versus GFS2-style recovery: reconnect to etcd, re-register membership, replay the small local WAL, resume. There's no cluster-wide recovery barrier, because no other node's metadata access was ever blocked by this node's absence — locks it held stay held (and unavailable to others) until fencing confirms it's actually gone.

## POSIX semantics: what's supported, what's not

Supported: standard file/directory CRUD, `read`/`write`/`truncate`, `rename` (including cross-directory), hard links, symlinks, `flock`/`fcntl` byte-range and whole-file locks (data-lock class above), directory listing via `readdir`/`readdirplus`.

Not supported: shared writable `mmap` across nodes (see [Locking](#locking)) — an explicit rejected case, not an ambiguous gap. Multi-node coherence for other unusual access patterns is documented per-subsystem in `docs/architecture/`; when in doubt, check [`docs/architecture/multi-node-coherence.md`](docs/architecture/multi-node-coherence.md) and [`docs/architecture/cache-coherence.md`](docs/architecture/cache-coherence.md) before assuming a POSIX guarantee holds identically to a local filesystem.

## How to use it

### Build

Requires Go 1.24+, a C11 compiler, `libfuse3-dev`, `protoc` (for `make proto`).

```
make all      # builds bin/etcfuse-meta (Go) and bin/etcfuse (C)
make check    # lint + test — run before every push (also wired as a pre-push git hook, see `make hooks`)
```

### Run locally (Docker)

`deploy/docker/docker-compose.yml` runs a full 3-node cluster in containers (3x etcd, 3x `etcfuse-meta`, 3x `etcfuse`, sharing a file-backed loopback block device):

```
docker compose -f deploy/docker/docker-compose.yml up -d --build
# FUSE mount is at /mnt/etcfuse inside each etcfuse<N> container
docker compose -f deploy/docker/docker-compose.yml down -v
```

`make dev` / `make dev-down` do the same for the lighter etcd-only development environment.

### Run on AWS

`scripts/infra/create-infra.sh` provisions a 3-node EC2 cluster with etcd colocated on the compute nodes and a shared io2 EBS Multi-Attach volume as the raw block device (no dedicated etcd instances, no filesystem format on the volume):

```
ETCFS_KEY_NAME=<your-keypair> ETCFS_COMPUTE_NODES=3 ETCFS_VOLUME_SIZE=30 \
  ./scripts/infra/create-infra.sh
```

`scripts/infra/add-compute-node.sh` / `destroy-infra.sh` handle elastic add and teardown; `scripts/infra/setup-compute.sh` installs and starts the daemons on already-provisioned nodes.

### Run the daemons directly

```
etcfuse-meta --listen=/tmp/etcfuse.sock --etcd-endpoints=http://127.0.0.1:2379 \
  --node-id=n1 --cluster-name=my-cluster --lease-ttl=10s --block-device=/dev/nvme1n1

etcfuse --socket=/tmp/etcfuse.sock --node-id=n1 /mnt/etcfuse
```

Full flag reference: `internal/config/config.go` (Go daemon), `etcfuse --help` (C daemon).

## Testing

Four tiers, from fast/local to slow/real-infrastructure:

1. **Unit tests** (`go test -race ./...`, `make test`) — per-package, in-memory mock etcd store (`pkg/metadata/mock_store.go`), no network or disk dependency.
2. **Deterministic simulator** (`test/harness`) — Jepsen-style fault injection against a simulated cluster: node death at every point in the write/allocate/lock-acquire sequence, partition between a node and etcd while the disk path stays alive (the exact scenario self-fencing exists for), etcd leader election or majority loss during in-flight transactions. This was built *before* the fencing/allocator logic it exercises, per the design's stated build order (`init_plan.md` §15) — treat it as a first-class correctness gate, not an afterthought.
3. **Single-scenario chaos tests** (`scripts/test/chaos-test.sh`), against real AWS infrastructure — a fresh 3-node cluster per scenario: C-daemon SIGKILL, Go-daemon SIGKILL, network partition + self-fence + rejoin, fencing-generation bump + rejection, all-3-simultaneous crash, mid-write crash + WAL replay. Current status and the two product bugs found/fixed to reach 7/7: [`docs/chaos-reports/2026-07-30-fresh-cluster-per-scenario.md`](docs/chaos-reports/2026-07-30-fresh-cluster-per-scenario.md).
4. **Compound chaos tests**, against both local Docker and real AWS, each exercising the *same* cluster across multiple operations rather than a fresh one per test:
   - **Sequential faults** (`scripts/test/chaos-test-single-cluster.sh`) — all six single-scenario faults back to back on one cluster, verifying recovery from repeated unrelated faults in sequence, not just from a fault on a pristine cluster.
   - **Randomized fuzz** (`scripts/test/chaos-fuzz.sh`) — concurrent random read/write/delete/rename/mkdir traffic from all nodes against random files, while a chaos injector randomly kills daemons, partitions nodes, bumps fencing generations, or crashes all nodes simultaneously on a randomized cadence; a liveness monitor asserts the cluster never goes fully unwritable.
   - **Elastic scale-out/scale-in** (`scripts/test/chaos-elastic.sh`) — adds 2 nodes to a running cluster one at a time, verifies each sees pre-existing data and its writes propagate, then removes both gracefully, verifying correctness at every step.

   Results and the harness bugs found along the way (all in test tooling, not the daemons): [`docs/chaos-reports/2026-07-31-single-cluster-and-fuzz.md`](docs/chaos-reports/2026-07-31-single-cluster-and-fuzz.md), [`docs/chaos-reports/2026-07-31-elastic-scale-out-in.md`](docs/chaos-reports/2026-07-31-elastic-scale-out-in.md).

Chaos scenarios cost real AWS resources and take a few minutes each to provision — iterate against Docker or a single scenario, not `all`, while developing.

## State

Implemented and under hardening — this is not yet a system to trust with data you can't afford to lose. In particular:

- Namespace mutations (create/mkdir/unlink/rename/setattr) still commit without a generation guard — only the data path (extent writes) is currently covered by the fencing-generation check. See [`docs/architecture/fencing-generation-protocol.md`](docs/architecture/fencing-generation-protocol.md) § Implementation Status.
- The chaos/fuzz testing tiers above stress crash recovery, fencing, and elastic membership changes fairly hard, but do not yet cover: concurrent multi-node scale-out, fault injection *during* a join/leave, or long-duration (multi-hour+) fuzz runs that would surface slow-leak bugs.

## Document map

| Document | Purpose |
|---|---|
| `docs/architecture/*.md` | Per-subsystem design docs (~24 files) — fencing, WAL, write ordering, schema, coherence, scrubber |
| [`docs/chaos-reports/`](docs/chaos-reports/) | Chaos/stress testing results, by date |
| [`docs/etcd_raft_research.md`](docs/etcd_raft_research.md) | etcd/Raft internals research — transactions, leases, watches, scaling limits |
| [`docs/research/cluster-fs-survey.md`](docs/research/cluster-fs-survey.md) | Cluster/distributed filesystem survey — GFS2, OCFS2, CephFS, GlusterFS, Lustre, EBS Multi-Attach failure modes |
| [`docs/research/vfs_fuse_block_research.md`](docs/research/vfs_fuse_block_research.md) | Linux VFS, FUSE protocol/operations/capabilities, O_DIRECT alignment, io_uring |
| [`docs/research/userspace_filesystem_research.md`](docs/research/userspace_filesystem_research.md) | FUSE daemon design patterns, metadata caching, language choice, test harnesses |
| `scripts/infra/` | AWS EC2 + EBS Multi-Attach provisioning, etcd deployment, daemon bootstrap, node add/remove |
| `scripts/test/` | Chaos engineering, fencing validation, elasticity testing |
| [`AGENTS.md`](AGENTS.md) | Instructions and conventions for AI agents working in this repo |
