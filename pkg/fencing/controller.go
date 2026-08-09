package fencing

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// Controller watches etcd membership keys and performs external fencing
// when a node's lease expires.  It bumps the fencing generation for the
// expired node, which prevents any stale lock grants and marks the node
// as fenced for the scrubber.
//
// Fencing is dual-signalled when a Fencer is configured: the lease expiry is
// only a suspicion, and the generation is not bumped until the expired node
// is *confirmed* to have lost access to the shared device — by an NVMe
// reservation preempt (NVMeFencer) or an EBS detach (EBSDetacher).  Without a
// Fencer the controller degrades to single-signal fencing — bumping on lease
// expiry alone — which is correct for Docker and single-host testing where
// there is no shared device to cut off, but is a weaker guarantee: it stops
// the node publishing metadata without stopping it writing bytes.  See
// SetFencer.
type Controller struct {
	store      *metadata.Store
	log        *config.Logger
	nodeID     string
	membership *metadata.Membership // own membership for leader election
	fencer     Fencer               // nil = single-signal fencing

	sweepInterval time.Duration
	claimTTL      time.Duration
}

// Reconciliation timing.
//
// sweepInterval is how long a fence that failed on every survivor can stay
// owed.  It trades retry latency against a periodic etcd range read of a
// prefix that is empty in a healthy cluster, so it is cheap to keep short.
//
// claimTTL must exceed the longest a single fence attempt can take — the EBS
// detach path polls for up to a minute — or a second controller would start
// fencing a node the first is still fencing.  That duplicate is benign (both
// Fencer implementations are idempotent and the generation CAS serialises the
// bumps), so erring long costs only retry latency after a crash, while erring
// short reintroduces the very race the claim removes.
const (
	defaultSweepInterval = 30 * time.Second
	defaultClaimTTL      = 120 * time.Second
)

// SetFencer enables dual-confirmed fencing.  A nil fencer (the default)
// leaves the controller in single-signal mode.
func (c *Controller) SetFencer(f Fencer) {
	c.fencer = f
}

// NewController creates a fencing controller.
func NewController(store *metadata.Store, membership *metadata.Membership, log *config.Logger) *Controller {
	return &Controller{
		store:         store,
		log:           log,
		nodeID:        membership.NodeID(),
		membership:    membership,
		sweepInterval: defaultSweepInterval,
		claimTTL:      defaultClaimTTL,
	}
}

// Run starts the controller.  It watches the membership key prefix for
// deletions (lease expiry) and fences expired nodes, and runs a periodic
// reconciliation sweep that retries any fence the watch path did not finish.
//
// Blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	c.log.Info("fencing controller started", "node", c.nodeID)

	go c.runSweep(ctx)

	// WithPrevKV: a delete event carries the key but not the value, and the
	// value is where the expired node recorded its EC2 instance ID.  Without
	// the previous value there is nothing to detach — the node is already
	// gone, so it cannot be asked.
	watchOpts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithPrevKV()}
	watchCh := c.store.Watch(ctx, metadata.PrefixMembership, watchOpts...)

	for {
		select {
		case <-ctx.Done():
			c.log.Info("fencing controller stopped")
			return
		case resp, ok := <-watchCh:
			if !ok {
				c.log.Warn("fencing watch channel closed, reconnecting")
				watchCh = c.store.Watch(ctx, metadata.PrefixMembership, watchOpts...)
				continue
			}
			for _, ev := range resp.Events {
				if ev.Type != clientv3.EventTypeDelete {
					continue
				}
				nodeID := extractNodeID(string(ev.Kv.Key))
				var instanceID string
				if ev.PrevKv != nil {
					instanceID = metadata.InstanceIDFromMembership(ev.PrevKv.Value)
				}
				c.log.Warn("membership key deleted, initiating fence",
					"node", nodeID, "instance", instanceID)
				// Record the intent on the watch goroutine, before the fence
				// runs asynchronously: this is the only moment the instance ID
				// is still readable, and an intent recorded here survives even
				// a controller that dies before fenceNode gets to run.
				if err := c.store.RecordFenceIntent(ctx, nodeID, instanceID); err != nil {
					c.log.Error("failed to record fence intent, fence will not be retried if it fails",
						"node", nodeID, "error", err)
				}
				go c.fenceNode(ctx, nodeID, instanceID, false)
			}
		}
	}
}

// fenceNode performs external fencing for a node whose membership key expired.
// It bumps the fencing generation to prevent stale locks and marks the node as fenced.
//
// fromSweep marks the reconciliation sweep as the caller.  The sweep decides
// what to fence from a ListFenceIntents snapshot, and that snapshot can go
// stale while this call waits on the claim — see the re-check below.  The
// watch path passes false: it acts on a single DELETE event it observed
// itself, and records the intent immediately before calling, so there is no
// snapshot to go stale.
func (c *Controller) fenceNode(ctx context.Context, nodeID, instanceID string, fromSweep bool) {
	// Cluster-wide dedup, not merely per-process: every survivor watches the
	// same membership prefix and sees the same DELETE, so without this all of
	// them fence the same node simultaneously.  Observed directly on AWS
	// (2026-08-06) with two survivors fencing n1 within 2ms of each other.
	leaseID, won, err := c.store.ClaimFence(ctx, nodeID, c.claimTTL)
	if err != nil {
		c.log.Error("failed to claim fence, leaving it to the reconciliation sweep",
			"node", nodeID, "error", err)
		return
	}
	if !won {
		c.log.Info("fence already claimed by another controller", "node", nodeID)
		return
	}
	defer func() {
		if rerr := c.store.ReleaseFenceClaim(context.Background(), leaseID); rerr != nil {
			c.log.Warn("failed to release fence claim, it will expire with its lease",
				"node", nodeID, "error", rerr)
		}
	}()

	// Winning the claim is not proof the fence is still owed.  Two sweeps can
	// list the same intent before either clears it; the first completes the
	// fence and releases its claim, and the second — still holding a snapshot
	// taken before any of that — then wins the free claim and would replay the
	// whole fence, bumping the generation a second time and re-issuing a real
	// preempt or detach.  The claim only serialises concurrent execution; it
	// cannot tell a straggler its snapshot is obsolete.  Re-reading the intent
	// here, after the claim, is what closes that.  Observed on Docker with 3
	// controllers, 2026-08-06.
	if fromSweep {
		pending, perr := c.store.Get(ctx, metadata.FencePendingKey(nodeID))
		if perr != nil {
			c.log.Error("cannot re-check fence intent, skipping this attempt",
				"node", nodeID, "error", perr)
			return
		}
		if pending == nil {
			c.log.Info("fence already completed by another controller, nothing owed",
				"node", nodeID)
			return
		}
	}

	c.log.Info("fencing node", "node_id", nodeID, "instance_id", instanceID)

	// Cut the node off from the device *before* bumping the generation, and
	// only proceed once that is confirmed.
	//
	// The order is the entire point.  Bumping first would advertise "this
	// node is fenced, its arenas and locks may be reclaimed" while the node
	// may still be issuing writes to the device — the reclaiming node would
	// then allocate into a range the fenced node is actively writing, which
	// is the arena-collision hazard in kleppmann-stale-write-analysis.md, and
	// no guard would catch it because both nodes pass their own checks.
	//
	// A failed fence therefore aborts rather than falling back to bumping
	// anyway: an uncut node the cluster believes is fenced is more dangerous
	// than one it knows it has not fenced.
	//
	// A failed fence is retried, not abandoned: the fence_pending intent
	// recorded before this point outlives the attempt, and the reconciliation
	// sweep re-runs it until the generation is bumped or the node re-registers.
	// Every return path below therefore leaves the intent in place.
	if c.fencer != nil {
		if err := c.fencer.Fence(ctx, nodeID, instanceID); err != nil {
			c.log.Error("device access not confirmed severed, NOT bumping generation",
				"node", nodeID, "instance", instanceID, "error", err)
			return
		}
		c.log.Info("device access severance confirmed", "node", nodeID, "instance", instanceID)
	}

	// Get current generation
	currentGen, err := c.store.GetGeneration(ctx, nodeID)
	if err != nil {
		c.log.Error("failed to get current generation", "node", nodeID, "error", err)
		return
	}

	// Bump generation — this is the fencing epoch
	newGen, err := c.store.BumpGeneration(ctx, nodeID, currentGen)
	if err != nil {
		c.log.Error("failed to bump generation", "node", nodeID, "error", err)
		return
	}

	// Reclaim the fenced node's arena, but only where the fence was
	// device-enforced.
	//
	// This is invariant 4 from kleppmann-stale-write-analysis.md: an arena may
	// return to the pool only once the previous owner is provably done with
	// it.  A confirmed Fencer.Fence is exactly that proof — the device itself
	// is already rejecting the node's writes, so the range can be reissued
	// immediately, with no grace period and no clock-bound argument.  In
	// single-signal mode (no Fencer) there is no such proof: the node's
	// metadata mutations are rejected, but its kernel may still be writing
	// bytes, and handing its arena to a live node would put both of them in
	// the same range.  Leaking the arena is the correct trade there.
	if c.fencer != nil {
		released, err := c.store.ReleaseArena(ctx, nodeID)
		switch {
		case err != nil:
			c.log.Warn("fenced node's arenas not all reclaimed, that space stays leaked",
				"node", nodeID, "reclaimed", released, "error", err)
		case len(released) > 0:
			c.log.Info("reclaimed fenced node's arenas", "node", nodeID, "arenas", released)
		}
	}

	// Only now is the fence complete, so only now is nothing owed.
	if err := c.store.ClearFenceIntent(ctx, nodeID); err != nil {
		c.log.Warn("fence complete but intent not cleared, sweep will re-fence harmlessly",
			"node", nodeID, "error", err)
	}

	c.log.Info("node fenced", "node_id", nodeID, "generation", newGen, "previous", currentGen)
}

// runSweep periodically retries fences that were never completed.
//
// Blocks until ctx is cancelled.
func (c *Controller) runSweep(ctx context.Context) {
	ticker := time.NewTicker(c.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

// reconcile retries every fence still recorded as owed.
//
// A node that has re-registered is no longer owed one: it holds a live
// membership lease again, which means its own self-fencing watchdog did not
// stop it and it is reachable from etcd. Fencing it at that point would sever
// a healthy node's device access on the strength of an expiry it has already
// recovered from — and the epoch separation the generation provides is only
// needed against a node that cannot be told it is gone.
func (c *Controller) reconcile(ctx context.Context) {
	intents, err := c.store.ListFenceIntents(ctx)
	if err != nil {
		c.log.Error("fence reconciliation sweep failed to list intents", "error", err)
		return
	}
	for nodeID, instanceID := range intents {
		alive, err := c.store.Get(ctx, metadata.MembershipKey(nodeID))
		if err != nil {
			c.log.Error("fence reconciliation sweep: cannot read membership",
				"node", nodeID, "error", err)
			continue
		}
		if alive != nil {
			c.log.Info("node re-registered before its fence completed, dropping intent",
				"node", nodeID)
			if err := c.store.ClearFenceIntent(ctx, nodeID); err != nil {
				c.log.Warn("failed to drop fence intent", "node", nodeID, "error", err)
			}
			continue
		}
		c.log.Warn("retrying incomplete fence", "node", nodeID, "instance", instanceID)
		c.fenceNode(ctx, nodeID, instanceID, true)
	}
}

// extractNodeID extracts the node ID from a membership key.
// Key format: membership:<node_id>
func extractNodeID(key string) string {
	prefix := metadata.PrefixMembership
	if len(key) > len(prefix) {
		return key[len(prefix):]
	}
	return key
}

// IsNodeFenced checks whether a node has been fenced (generation > 0).
func (c *Controller) IsNodeFenced(ctx context.Context, nodeID string) (bool, error) {
	return c.store.IsFenceActive(ctx, nodeID)
}

// GenerationGuard returns a Cmp that can be used in transactions
// to guard operations against stale fence generations.
func (c *Controller) GenerationGuard(ctx context.Context, nodeID string) (clientv3.Cmp, error) {
	gen, err := c.store.GetGeneration(ctx, nodeID)
	if err != nil {
		return clientv3.Cmp{}, fmt.Errorf("generation guard: %w", err)
	}
	return metadata.WithGenerationGuard(nodeID, gen), nil
}
