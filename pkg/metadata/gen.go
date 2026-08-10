package metadata

import (
	"context"
	"fmt"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Fencing generation primitives.
//
// Each node has a gen:<node_id> key storing a monotonically increasing
// generation counter.  The generation is bumped only by the fencing
// controller after confirming a node has been fenced.
//
// Every lock-grant transaction must CAS-check that the fencing generation
// is current.  Every extent write is stamped with the writer's generation
// for the scrubber to cross-check.

// GetGeneration returns the current fencing generation for a node.
func (s *Store) GetGeneration(ctx context.Context, nodeID string) (uint64, error) {
	key := GenKey(nodeID)
	value, err := s.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("get generation %s: %w", nodeID, err)
	}
	if value == nil {
		// No generation key → node has never been fenced.
		// Treat as generation 0 (initial state).
		return 0, nil
	}
	return strconv.ParseUint(string(value), 10, 64)
}

// GetMyGeneration returns this node's current fencing generation.
func (s *Store) GetMyGeneration(ctx context.Context) (uint64, error) {
	return s.GetGeneration(ctx, s.nodeID)
}

// PutGeneration atomically sets the fencing generation for a node.
// Used by the fencing controller after confirmed fence.
func (s *Store) PutGeneration(ctx context.Context, nodeID string, gen uint64) error {
	// Unguarded: sets the generation the guard compares against.  Guarding it
	// would make the fencing controller unable to fence a node.
	_, err := s.putRaw(ctx, GenKey(nodeID), []byte(strconv.FormatUint(gen, 10)))
	return err
}

// BumpGeneration atomically increments a node's fencing generation.
// The CAS ensures no concurrent bumps — the generation transitions
// from expectedOld to expectedOld+1.  Returns the new generation.
//
// This is the most safety-critical operation in the system.
// Called by the fencing controller after dual-confirmed fence.
//
// If the generation key does not exist yet, it is treated as generation 0.
//
// EnsureGenerationKey runs at every node's startup and creates gen:<node_id>
// at "0" before the node serves anything, so by the time a fence can be
// triggered the key almost always already exists with that value — the
// "key never existed" case below is a fallback for a node fenced before its
// own daemon ever ran, not the common path.
func (s *Store) BumpGeneration(ctx context.Context, nodeID string, expectedOld uint64) (uint64, error) {
	key := GenKey(nodeID)
	newGen := expectedOld + 1
	op := clientv3.OpPut(key, strconv.FormatUint(newGen, 10))

	// Unguarded: this transaction *is* the fence.  Guarding a generation bump
	// by the generation it changes would make the fencing controller unable to
	// fence a node once that node's own store carried a guard.
	valCmp := clientv3.Compare(clientv3.Value(key), "=", strconv.FormatUint(expectedOld, 10))
	ok, err := s.txnRaw(ctx, []clientv3.Cmp{valCmp}, []clientv3.Op{op}, nil)
	if err != nil {
		return 0, fmt.Errorf("bump generation %s: %w", nodeID, err)
	}

	// A value comparison against a genuinely missing key always evaluates
	// false, indistinguishable from a stale expectedOld — so on a rejected
	// bump at expectedOld=0, separately check for "key never existed" and
	// accept that too, atomically, before treating it as a real CAS failure.
	if !ok && expectedOld == 0 {
		absentCmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		ok, err = s.txnRaw(ctx, []clientv3.Cmp{absentCmp}, []clientv3.Op{op}, nil)
		if err != nil {
			return 0, fmt.Errorf("bump generation %s: %w", nodeID, err)
		}
	}

	if !ok {
		current, _ := s.GetGeneration(ctx, nodeID)
		return 0, fmt.Errorf("bump generation %s: CAS failed (expected %d, got %d)", nodeID, expectedOld, current)
	}
	return newGen, nil
}

// EnsureGenerationKey makes sure gen:<node_id> exists, creating it at
// generation 0 if it does not, and returns the current generation.
//
// The key must exist before any generation-guarded transaction runs:
// WithGenerationGuard compares the key's *value*, and a value comparison
// against a missing key always fails, which would reject every write rather
// than only a fenced node's writes.
func (s *Store) EnsureGenerationKey(ctx context.Context, nodeID string) (uint64, error) {
	key := GenKey(nodeID)

	// Unguarded: creates the very key the guard compares against.  A guarded
	// call here could never succeed on a fresh node — the guard would compare
	// against a key that does not exist yet, which always evaluates false.
	created, err := s.txnRaw(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(key, "0")}, nil)
	if err != nil {
		return 0, fmt.Errorf("ensure generation key %s: %w", nodeID, err)
	}
	if created {
		return 0, nil
	}
	return s.GetGeneration(ctx, nodeID)
}

// WithGenerationGuard wraps a transaction with a fencing generation check.
//
// Every metadata mutation that modifies extents MUST be guarded:
// the transaction must verify that the writer's current generation has
// not been bumped (i.e., the writer has not been fenced).
//
// Returns a Cmp that checks gen:<nodeID> == expectedGen.
func WithGenerationGuard(nodeID string, expectedGen uint64) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(GenKey(nodeID)), "=",
		strconv.FormatUint(expectedGen, 10))
}

// IsFenceActive returns true if a fencing event has been recorded for
// the given node (generation > 0).  A node with gen=0 has never been fenced.
func (s *Store) IsFenceActive(ctx context.Context, nodeID string) (bool, error) {
	gen, err := s.GetGeneration(ctx, nodeID)
	if err != nil {
		return false, err
	}
	return gen > 0, nil
}
