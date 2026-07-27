package metadata

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Inode number allocation via per-node range reservation.
//
// To avoid the hot-key problem of a single global inode counter, each node
// reserves a block of inode numbers via a CAS against a shared counter.
// Once reserved, the node hands out numbers from its local range without
// touching etcd until the range is exhausted.
//
// This is the same sharding pattern used for arena allocation (§6 of init_plan).

// AllocRangeSize is the number of inode numbers per range reservation.
const AllocRangeSize = 1_000_000

// AllocInodeRange reserves the next N inode numbers for this node.
// Returns the start of the reserved range (inclusive) and end (exclusive).
func (s *Store) AllocInodeRange(ctx context.Context) (start, end uint64, err error) {
	key := KeyInodeAllocCounter

	// Read current counter
	value, err := s.Get(ctx, key)
	if err != nil {
		return 0, 0, fmt.Errorf("alloc inode range: %w", err)
	}

	current := uint64(0)
	if value != nil {
		current = DecodeUint64(value)
	}

	next := current + AllocRangeSize

	var cmps []clientv3.Cmp
	if current == 0 {
		// Key doesn't exist yet — first allocation
		cmps = []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)}
	} else {
		cmps = []clientv3.Cmp{clientv3.Compare(clientv3.Value(key), "=", string(EncodeUint64(current)))}
	}

	op := clientv3.OpPut(key, string(EncodeUint64(next)))

	ok, err := s.Txn(ctx, cmps, []clientv3.Op{op}, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("alloc inode range: %w", err)
	}
	if !ok {
		return 0, 0, fmt.Errorf("alloc inode range: CAS conflict (another node reserved concurrently)")
	}

	return current, next, nil
}

// NodeInodeAlloc manages a local inode number free-list.
// After reserving a range from etcd via AllocInodeRange, the node
// uses this allocator to hand out individual inode numbers locally.
type NodeInodeAlloc struct {
	nodeID string
	start  uint64 // first usable inode in current range
	end    uint64 // one past the last usable inode
	next   uint64 // next inode to allocate
}

// NewNodeInodeAlloc creates a local inode allocator.
func NewNodeInodeAlloc(nodeID string) *NodeInodeAlloc {
	return &NodeInodeAlloc{nodeID: nodeID}
}

// Reserve obtains the next inode range from etcd.
func (a *NodeInodeAlloc) Reserve(ctx context.Context, store *Store) error {
	start, end, err := store.AllocInodeRange(ctx)
	if err != nil {
		return err
	}
	a.start = start
	a.end = end
	a.next = start
	return nil
}

// Allocate returns the next available inode number from the local range.
// If the range is exhausted, the caller must call Reserve again.
func (a *NodeInodeAlloc) Allocate() (uint64, error) {
	if a.next >= a.end {
		return 0, fmt.Errorf("inode range exhausted (need to reserve)")
	}
	ino := a.next
	a.next++
	return ino, nil
}

// Available returns the number of inode numbers remaining in the local range.
func (a *NodeInodeAlloc) Available() uint64 {
	if a.next >= a.end {
		return 0
	}
	return a.end - a.next
}
