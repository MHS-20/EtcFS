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
// In production, the Controller is backed by AWS APIs (DetachVolume,
// DescribeInstances) for dual-confirmed fencing.  For local testing,
// the Controller bumps the generation directly on membership expiry.
type Controller struct {
	store      *metadata.Store
	log        *config.Logger
	nodeID     string
	membership *metadata.Membership // own membership for leader election

	mu           sync.Mutex
	activeFences map[string]time.Time // nodeID → when fence started
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

	watchCh := c.store.Watch(ctx, metadata.PrefixMembership, clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			c.log.Info("fencing controller stopped")
			return
		case resp, ok := <-watchCh:
			if !ok {
				c.log.Warn("fencing watch channel closed, reconnecting")
				watchCh = c.store.Watch(ctx, metadata.PrefixMembership, clientv3.WithPrefix())
				continue
			}
			for _, ev := range resp.Events {
				if ev.Type == clientv3.EventTypeDelete {
					nodeID := extractNodeID(string(ev.Kv.Key))
					c.log.Warn("membership key deleted, initiating fence", "node", nodeID)
					go c.fenceNode(ctx, nodeID)
				}
			}
		}
	}
}

// fenceNode performs external fencing for a node whose membership key expired.
// It bumps the fencing generation to prevent stale locks and marks the node as fenced.
func (c *Controller) fenceNode(ctx context.Context, nodeID string) {
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

	c.log.Info("fencing node", "node_id", nodeID)

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
