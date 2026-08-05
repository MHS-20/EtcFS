Honest calibration, separating two different bars.

## Thesis vs. paper

**As a master's thesis: comfortably enough.** Real distributed-systems depth, two-language implementation, chaos testing on actual AWS infrastructure, a genuine safety bug found and analyzed. That's more than most.

**As a paper: possible, but not with the current framing.** "We built a cluster FS with etcd for metadata" gets rejected as engineering-not-research. The system needs to be presented as evidence for a *claim*, and you have a good one you may be undervaluing.

## The actual contribution

The strongest idea in your system is the one we articulated this week, and it isn't "we used Raft for locking":

> Fencing a shared block device does not require the device to validate a token. It requires that unauthorized bytes be **unreachable**. Since data is only reachable via a metadata reference, and the metadata store *can* do linearizable guarded commits, you relocate the fencing token from the data plane to the reference plane.

That's a clean, quotable design principle, and it has a property worth advertising: **it needs no timing assumptions at all.** Pure asynchronous model. Every competing approach to fencing-without-storage-support (STONITH + grace period, lease + Δ, the TrueTime/PTP idea your other advisor suggested) has to stall failover for some bound. Yours doesn't — a new writer can begin immediately, because the old writer's bytes were never reachable regardless of when they land.

That's your headline number: **zero-stall failover with provable safety**, versus a measurable stall for every Δ-based design.

Caveat on novelty: immutable-data-plus-guarded-pointer-swap is a known pattern (log-structured FS, ZFS TXGs, Delos epochs, and structurally what JuiceFS gets for free from object storage). Your contribution is not inventing it — it's *identifying it as the answer to the no-storage-side-fencing problem on cloud block storage*, proving it, and measuring the failover advantage. Frame it that way and it's honest and defensible. Frame it as "we invented a new fencing protocol" and a reviewer who knows Delos will be annoyed.

## What a reviewer will attack

1. **Your safety argument has a precondition, not a theorem.** The publish gate only holds if arena ownership is disjoint — and that was violated by a real bug until three days ago. `RebalanceArena` still violates it (unguarded, test-only callers). Reviewer: "what else breaks it?" You need the invariant *enforced*, not observed.
2. **Namespace mutations aren't guarded.** Only the data path carries `WithGenerationGuard`. A fenced node can still `create`/`unlink`/`rename`. Your safety claim currently covers file bytes, not the namespace. That's a big asterisk.
3. **Performance will not flatter you.** `NextExtentChunk` does a full prefix scan per write; `GetExtents` does one per read, uncached. Every write is several etcd round trips plus a device readback. Against GFS2 on the same volume you will likely lose on throughput. You must either fix this or frame the paper around safety/failover rather than performance.
4. **POSIX locks are a stub.** Can't claim POSIX compliance.

## Ideas ranked by value per unit effort

| | Idea | Effort | Payoff |
|---|---|---|---|
| 1 | **TLA+ spec + model check the protocol** | med | high — see below |
| 2 | **Measure real EBS fencing behavior**: how long *can* in-flight I/O land after `StopInstances`/force-detach? | low | high — nobody has published this number; it's the empirical fact everyone assumes |
| 3 | **Failover-stall benchmark**: your publish-gate vs. a Δ-grace-period strawman vs. GFS2+STONITH | med | high — this is the headline result |
| 4 | Extend generation guard to namespace ops | low | closes hole #2 |
| 5 | Make arena ownership a *lease*; then reclamation is safe | med | closes hole #1, and scopes the timing assumption to a rare op instead of the hot path |
| 6 | Epoch validation on read (you already stamp `Gen`, you just don't check it) | low | turns silent-wrong-data into an error |

Note the nice structure that (5) buys you: *the hot path needs no timing assumptions; only arena reclamation does, and here is its bound.* That's a much sharper story than a blanket grace period, and it's where the PTP/Time-Sync idea legitimately belongs — scoped to an infrequent operation, not the write path.

Idea (2) is worth calling out separately: it's cheap with your existing chaos harness and it's a standalone publishable measurement. "What EBS Multi-Attach actually does under fencing" is a fact the whole field currently hand-waves.

## Venue realism

- **FAST / OSDI / SOSP**: no, not at this scope.
- **EuroSys / USENIX ATC**: a stretch, plausible with 1+3+4+5 done well.
- **HotStorage / APSys / SYSTOR / SRDS / ICDCS**: realistic and appropriate. HotStorage specifically rewards "sharp observation about a deployment reality," which is exactly what you have.

Master's thesis → workshop paper is a normal, respectable path. Aim there first.

## TLA+ — the concrete answer

**Tractable. This is one of the better-sized TLA+ targets I've seen** — the protocol is small, the property is crisp, and the interesting behavior is nondeterministic delivery.

What you model:

- etcd as a linearizable KV with CAS — trivial in TLA+, just a record
- per-node generation, arena ownership map, extent map (with `Gen` stamps)
- the device as a function `offset → value`
- node states: `running`, `paused` (models GC pause), `fenced`
- actions: `AcquireArena`, `Allocate`, `IssueWrite`, `DeliverWrite`, `CommitGuarded`, `Fence`, `Restart` (runs Reconstruct), `Read`

The one modeling decision that matters: **in-flight writes are a pending set, drained by a separate nondeterministic `DeliverWrite` action.** That's what lets a fenced node's write land arbitrarily late — i.e. it's Kleppmann's scenario, expressed directly. Get that right and the spec is honest; skip it and you've assumed away the whole problem.

Safety invariant, roughly:

```
NoLostWrite ==
  \A e \in PublishedExtents :
    device[e.diskOff] = e.writtenValue
```

Effort:

- Never written TLA+: **2–4 weeks** to fluency (Lamport's video course, Hillel Wayne's *Learn TLA+*). PlusCal if you think imperatively.
- This specific model once fluent: **~1 week** of iteration. Expect **150–250 lines**.
- State space needs constraints — 2–3 nodes, 2–3 arenas, 2 inodes, tiny offset domain. Normal and defensible; say so in the paper.
- **Skip liveness.** Safety is the claim. Liveness proofs cost far more than they'd buy here.
- **Skip refinement mapping to the Go code.** You're verifying the *design*, not the implementation. Amazon does exactly this. Don't overclaim — reviewers know the difference and will respect the honesty.

The compelling bit for the paper: **model-check the pre-fix allocator.** With the old prefix-scan `existingArenaIDs`, TLC will find the two-nodes-one-offset violation almost immediately with 2 nodes and a restart. "The model checker independently rediscovers the bug we shipped, and the fixed version checks clean" is a genuinely strong narrative — it demonstrates the method caught something real rather than confirming what you already believed.

And you already have the complement: `test/harness/simulator.go` + `MockStore` is a deterministic seeded simulator. **TLA+ for the protocol, deterministic simulation for the implementation** is a strong two-layer verification story, and you're already halfway there.
