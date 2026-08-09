package metadata

import (
	"context"
	"fmt"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Arena pool keys.
//
//	arena:<node_id>/<arena_id>          an arena a node currently owns
//	free_arena:<arena_id>               an arena returned to the global pool
//
// An arena in the free pool has no owner, so a node may claim it without
// colliding with anyone.  It is not necessarily empty: an arena released by a
// departing or fenced node keeps whatever live extents its files still have,
// and the claiming node must mark those ranges as allocated before it writes
// (see pkg/arena.Allocator.AcquireArena).  An arena released because its owner
// emptied it (ReleaseEmptyArenas) holds nothing, but the claimer cannot tell
// the two apart and rebuilds the bitmap either way.

func FreeArenaKey(arenaID uint64) string {
	return fmt.Sprintf("%s%d", PrefixFreeArena, arenaID)
}

// ReleaseArena returns every arena nodeID owns to the global free pool,
// reporting the arenas actually released.
//
// All of them, not just one: a node acquires a further arena whenever its
// current ones fill up, so releasing a single record on departure or fencing
// would strand the rest — owned by a node that is gone, and never reissued.
//
// One transaction per arena rather than one for all of them.  Each is guarded
// on that record's own existence, so an arena released concurrently fails its
// own CAS without aborting the release of the others; a partial release is
// recoverable (the leftovers are released on the next attempt, and fsck reports
// them meanwhile), a wholesale abort is not.  A node with no ownership records
// has nothing to release, the normal case for a node that never wrote anything.
func (s *Store) ReleaseArena(ctx context.Context, nodeID string) ([]uint64, error) {
	kvs, err := s.GetPrefix(ctx, ArenaNodePrefix(nodeID))
	if err != nil {
		return nil, fmt.Errorf("release arenas of %s: %w", nodeID, err)
	}

	var released []uint64
	for _, kv := range kvs {
		_, arenaID, ok := ParseArenaKey(string(kv.Key))
		if !ok {
			continue
		}
		won, err := s.ReleaseArenaID(ctx, nodeID, arenaID)
		if err != nil {
			return released, err
		}
		if won {
			released = append(released, arenaID)
		}
	}
	return released, nil
}

// ReleaseArenaID returns one arena to the global free pool, reporting whether
// this call is the one that released it.
//
// The move is a single transaction: an arena listed as free while still
// recorded as owned would be handed to a second node, which is the
// two-writers-one-range case the arena scheme exists to prevent.
//
// Written raw rather than through the generation guard because the caller is
// often a *peer* acting on a node that has just been fenced — a fencing
// controller whose write was rejected on the fenced node's behalf could never
// reclaim anything.  Releasing an arena is safe on a fenced node by
// definition: its device access has already been confirmed severed.
func (s *Store) ReleaseArenaID(ctx context.Context, nodeID string, arenaID uint64) (bool, error) {
	key := ArenaOwnerKey(nodeID, arenaID)
	won, err := s.txnRaw(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "!=", 0)},
		[]clientv3.Op{
			clientv3.OpDelete(key),
			clientv3.OpPut(FreeArenaKey(arenaID), "free"),
		}, nil)
	if err != nil {
		return false, fmt.Errorf("release arena %d of %s: %w", arenaID, nodeID, err)
	}
	return won, nil
}

// ClaimFreeArena takes one arena out of the global free pool, reporting the
// arena claimed and whether the pool had one.
//
// The delete is the claim: it is conditioned on the key still existing, so of
// several nodes reaching for the same freed arena exactly one wins and the
// losers move on to the next candidate.  Handing the same arena to two nodes
// would put both of them writing into one range with nothing to detect it,
// which is why this cannot be a plain read-then-delete.
func (s *Store) ClaimFreeArena(ctx context.Context) (uint64, bool, error) {
	kvs, err := s.GetPrefix(ctx, PrefixFreeArena)
	if err != nil {
		return 0, false, fmt.Errorf("claim free arena: %w", err)
	}
	for _, kv := range kvs {
		key := string(kv.Key)
		arenaID, perr := strconv.ParseUint(key[len(PrefixFreeArena):], 10, 64)
		if perr != nil {
			continue
		}
		won, err := s.txnRaw(ctx,
			[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "!=", 0)},
			[]clientv3.Op{clientv3.OpDelete(key)}, nil)
		if err != nil {
			return 0, false, fmt.Errorf("claim free arena %d: %w", arenaID, err)
		}
		if won {
			return arenaID, true, nil
		}
	}
	return 0, false, nil
}
