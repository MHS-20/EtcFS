package fencing

import (
	"context"
	"fmt"
	"sync"
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

	mu           sync.Mutex
	activeFences map[string]time.Time // nodeID → when fence started
}

// SetFencer enables dual-confirmed fencing.  A nil fencer (the default)
// leaves the controller in single-signal mode.
func (c *Controller) SetFencer(f Fencer) {
	c.fencer = f
}

// NewController creates a fencing controller.
func NewController(store *metadata.Store, membership *metadata.Membership, log *config.Logger) *Controller {
	return &Controller{
		store:        store,
		log:          log,
		nodeID:       membership.NodeID(),
		membership:   membership,
		activeFences: make(map[string]time.Time),
	}
}

// Run starts the controller.  It watches the membership key prefix for
// deletions (lease expiry) and fences expired nodes.
//
// Blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	c.log.Info("fencing controller started", "node", c.nodeID)

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
				go c.fenceNode(ctx, nodeID, instanceID)
			}
		}
	}
}

// fenceNode performs external fencing for a node whose membership key expired.
// It bumps the fencing generation to prevent stale locks and marks the node as fenced.
func (c *Controller) fenceNode(ctx context.Context, nodeID, instanceID string) {
	c.mu.Lock()
	if _, active := c.activeFences[nodeID]; active {
		c.mu.Unlock()
		c.log.Info("fence already in progress", "node", nodeID)
		return
	}
	c.activeFences[nodeID] = time.Now()
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.activeFences, nodeID)
		c.mu.Unlock()
	}()

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
	// This does NOT retry automatically. The watch that triggers fenceNode
	// fires once, on the membership key's DELETE event; that key is already
	// gone, so no further event exists to re-trigger on. A failed fence
	// leaves the node in the limbo state external-fencing-controller.md
	// describes — generation not bumped, requiring either the node's own
	// self-fencing watchdog to have already stopped it, or operator
	// intervention. No periodic reconciliation sweep exists to retry a failed
	// fence; building one is out of scope here and is not implied by
	// anything above.
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

	c.log.Info("node fenced", "node_id", nodeID, "generation", newGen, "previous", currentGen)
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
