package metadata

import (
	"context"
	"fmt"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Arena pool keys.
//
//	arena:<node_id>      = <arena_id>   the arena a node currently owns
//	free_arena:<arena_id>               an arena returned to the global pool
//
// An arena in the free pool has no owner, so a node may claim it without
// colliding with anyone.  It is not necessarily empty: an arena released by a
// departing or fenced node keeps whatever live extents its files still have,
// and the claiming node must mark those ranges as allocated before it writes
// (see pkg/arena.Allocator.AcquireArena).  Only the compactor releases an
// arena it has already evacuated.

func FreeArenaKey(arenaID uint64) string {
	return fmt.Sprintf("%s%d", PrefixFreeArena, arenaID)
}

// ReleaseArena returns nodeID's arena to the global free pool, reporting the
// arena released and whether there was one.
//
// The move is a single transaction guarded on the ownership record's current
// value: a node that has already been given a different arena (by the
// compactor, or by re-acquiring after a restart) must not have that newer
// arena freed by a release racing on stale state.  A node with no ownership
// record has nothing to release, which is the normal case for a node that
// never wrote anything.
//
// Written raw rather than through the generation guard because the caller is
// usually a *peer* acting on a node that has just been fenced — a fencing
// controller whose write was rejected on the fenced node's behalf could never
// reclaim anything.  Releasing an arena is safe on a fenced node by
// definition: its device access has already been confirmed severed.
func (s *Store) ReleaseArena(ctx context.Context, nodeID string) (uint64, bool, error) {
	key := ArenaKey(nodeID)
	v, err := s.Get(ctx, key)
	if err != nil {
		return 0, false, fmt.Errorf("release arena of %s: %w", nodeID, err)
	}
	if len(v) != 8 {
		return 0, false, nil
	}
	arenaID := DecodeUint64(v)
	ok, err := s.txnRaw(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value(key), "=", string(v))},
		[]clientv3.Op{
			clientv3.OpDelete(key),
			clientv3.OpPut(FreeArenaKey(arenaID), "free"),
		}, nil)
	if err != nil {
		return 0, false, fmt.Errorf("release arena %d of %s: %w", arenaID, nodeID, err)
	}
	return arenaID, ok, nil
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
