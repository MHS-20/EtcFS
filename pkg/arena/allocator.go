// Package arena implements the arena-based block allocator.
//
// The raw block device is divided into arenas (~1 GB contiguous ranges),
// each leased exclusively to one node via an etcd transaction on
// arena:<node_id>.  A node allocates blocks from its arena using a
// local free-list, only touching etcd when acquiring or releasing arenas.
//
// This converts the classic distributed-allocator hot-key problem into
// an infrequent etcd operation (~one per GB of writes).
//
// Phase 0: types and interfaces defined.  Implementation in Phase 6.
package arena

import (
	"sync"
)

// ArenaSizeBytes is the default arena size (1 GiB).
const ArenaSizeBytes = 1 << 30

// Allocator manages block allocation within this node's arenas.
type Allocator struct {
	mu     sync.Mutex
	nodeID string
	arenas []*Arena
}

// Arena represents a single contiguous range on the block device.
type Arena struct {
	DiskStart uint64
	DiskEnd   uint64

	// FreeList is a bitmap or free-list of available blocks within
	// this arena.  A zero byte means the block is free.
	freeList []byte

	// BlockSize is the allocation granularity within this arena.
	BlockSize uint32
}

// NewAllocator creates an Allocator for the given node.
func NewAllocator(nodeID string) *Allocator {
	return &Allocator{
		nodeID: nodeID,
	}
}

// Allocate finds a contiguous free range of the given size within the
// node's arenas.  Returns the disk offset and length, or an error if
// no arena has sufficient free space.
func (a *Allocator) Allocate(size uint64) (diskOff uint64, length uint64, err error) {
	// Phase 6: implement arena-local allocation with best-fit search.
	return 0, 0, nil
}

// Free marks a range as reclaimable in the node's local free-list.
func (a *Allocator) Free(diskOff, length uint64) error {
	// Phase 6: return blocks to the owning arena's free-list.
	return nil
}

// LiveRatio returns the fraction of allocated blocks across all arenas.
// Values below the compaction threshold trigger arena compaction.
func (a *Allocator) LiveRatio() float64 {
	return 1.0
}

// NodeID returns the owning node.
func (a *Allocator) NodeID() string {
	return a.nodeID
}
