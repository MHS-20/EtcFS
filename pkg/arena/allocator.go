// Package arena implements the arena-based block allocator.
//
// The raw block device is divided into arenas (~1 GiB contiguous ranges),
// each leased exclusively to one node via an etcd transaction on
// arena:<node_id>.  A node allocates blocks from its arena using a
// local free-list, only touching etcd when acquiring or releasing arenas.
//
// This converts the classic distributed-allocator hot-key problem into
// an infrequent etcd operation (~one per GB of writes).
package arena

import (
	"context"
	"fmt"
	"math/bits"
	"sync"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// ArenaSizeBytes is the default arena size (1 GiB).
const ArenaSizeBytes = 1 << 30

// BlockSize is the allocation granularity within an arena (4 KiB).
const BlockSize = 4096

// BlocksPerArena is the number of allocatable blocks per arena.
const BlocksPerArena = ArenaSizeBytes / BlockSize

// Allocator manages block allocation within this node's arenas.
type Allocator struct {
	mu     sync.Mutex
	nodeID string
	store  *metadata.Store
	arenas []*Arena
}

// Arena represents a single contiguous range on the block device.
type Arena struct {
	ID        uint64 // unique arena identifier
	DiskStart uint64 // byte offset on the block device
	DiskEnd   uint64 // byte offset (exclusive)

	// bitmap tracks allocated blocks.  bit=1 means allocated, bit=0 means free.
	bitmap []uint64
}

// NewAllocator creates an Allocator for the given node.
func NewAllocator(nodeID string, store *metadata.Store) *Allocator {
	return &Allocator{
		nodeID: nodeID,
		store:  store,
	}
}

// AcquireArena reserves a new arena from the global pool via etcd.
// The arena is leased exclusively to this node.
func (a *Allocator) AcquireArena(ctx context.Context) (*Arena, error) {
	arenaID, err := a.allocateArenaID(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire arena: %w", err)
	}

	diskStart := arenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes

	// Record ownership before using the arena.  Without this the node has no
	// durable claim on the range it is about to write into, and a restart
	// cannot tell which arenas were its own.
	if _, err := a.store.Put(ctx, metadata.ArenaKey(a.nodeID), metadata.EncodeUint64(arenaID)); err != nil {
		return nil, fmt.Errorf("acquire arena %d: record ownership: %w", arenaID, err)
	}

	free := &Arena{
		ID:        arenaID,
		DiskStart: diskStart,
		DiskEnd:   diskEnd,
		bitmap:    make([]uint64, BlocksPerArena/64),
	}

	a.mu.Lock()
	a.arenas = append(a.arenas, free)
	a.mu.Unlock()

	return free, nil
}

// allocateArenaID reserves the next arena ID via etcd CAS.
func (a *Allocator) allocateArenaID(ctx context.Context) (uint64, error) {
	return a.store.NextCounter(ctx, metadata.PrefixArenaLog, 0)
}

// Allocate finds and marks a contiguous range of blocks as allocated.
// Returns the byte offset on the block device.
func (a *Allocator) Allocate(size uint64) (diskOff uint64, err error) {
	blocks := (size + BlockSize - 1) / BlockSize

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, free := range a.arenas {
		start := free.findContiguous(blocks)
		if start < BlocksPerArena {
			// Mark blocks as allocated
			for i := uint64(0); i < blocks; i++ {
				free.markAllocated(start + i)
			}
			return free.DiskStart + start*BlockSize, nil
		}
	}
	return 0, fmt.Errorf("no arena has %d contiguous free blocks", blocks)
}

// Free marks a range of blocks as free.
func (a *Allocator) Free(diskOff uint64, size uint64) {
	blocks := (size + BlockSize - 1) / BlockSize

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, free := range a.arenas {
		if diskOff >= free.DiskStart && diskOff < free.DiskEnd {
			start := (diskOff - free.DiskStart) / BlockSize
			for i := uint64(0); i < blocks && start+i < BlocksPerArena; i++ {
				free.markFree(start + i)
			}
			return
		}
	}
}

// LiveRatio returns the fraction of allocated blocks across all arenas.
func (a *Allocator) LiveRatio() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	total := uint64(0)
	used := uint64(0)
	for _, free := range a.arenas {
		total += BlocksPerArena
		used += free.countAllocated()
	}
	if total == 0 {
		return 1.0
	}
	return float64(used) / float64(total)
}

// NodeID returns the owning node.
func (a *Allocator) NodeID() string { return a.nodeID }

// Reconstruct rebuilds the arena free-list from existing extents in etcd.
// Called at startup after reconnecting to etcd.
func (a *Allocator) Reconstruct(ctx context.Context) error {
	arenaIDs, err := a.existingArenaIDs(ctx)
	if err != nil {
		return err
	}

	a.mu.Lock()
	a.arenas = nil
	a.mu.Unlock()

	for _, id := range arenaIDs {
		free := &Arena{
			ID:        id,
			DiskStart: id * ArenaSizeBytes,
			DiskEnd:   (id + 1) * ArenaSizeBytes,
			bitmap:    make([]uint64, BlocksPerArena/64),
		}
		a.mu.Lock()
		a.arenas = append(a.arenas, free)
		a.mu.Unlock()
	}

	extents, err := a.store.AllExtents(ctx)
	if err != nil {
		return err
	}
	for _, ext := range extents {
		for _, free := range a.arenas {
			if ext.WithinDisk(free.DiskStart, free.DiskEnd) {
				start := (ext.DiskOff - free.DiskStart) / BlockSize
				blocks := (ext.Length + BlockSize - 1) / BlockSize
				for i := uint64(0); i < blocks && start+i < BlocksPerArena; i++ {
					free.markAllocated(start + i)
				}
			}
		}
	}
	return nil
}

// existingArenaIDs returns the arenas this node owns, read from its own
// arena:<node_id> ownership record.
//
// Only this node's record is read, never the whole arena: prefix.  Scanning the
// prefix would pull every *other* live node's arenas into this node's
// free-list, and Allocate would then hand out disk offsets inside a range
// another node is actively writing.  Both nodes would write different data to
// the same block and both extent commits would succeed, because neither node
// is fenced and the generation guard has nothing to object to — silent
// corruption, detectable only after the fact by the scrubber's
// CheckExtentCollisions.
//
// ponytail: one arena recorded per node, so a node holding several recovers
// only its most recent one after a restart and leaks the others (safe, they
// are simply never reallocated). Move to arena:<node_id>/<arena_id> records if
// multi-arena recovery starts to matter.
func (a *Allocator) existingArenaIDs(ctx context.Context) ([]uint64, error) {
	v, err := a.store.Get(ctx, metadata.ArenaKey(a.nodeID))
	if err != nil {
		return nil, err
	}
	// A record is exactly the 8-byte big-endian arena ID.  Anything else is
	// malformed, and adopting it would mean allocating into an arena this node
	// may not own — treat it as "nothing recorded" instead, which only costs a
	// fresh arena.  Note arena ID 0 is a valid ID, so a present record must be
	// distinguished by length, not by a zero test.
	if len(v) != 8 {
		return nil, nil
	}
	return []uint64{metadata.DecodeUint64(v)}, nil
}

// ArenaCount returns the number of arenas managed by this allocator.
func (a *Allocator) ArenaCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.arenas)
}

// ---- bitmap operations ----

func (ar *Arena) findContiguous(blocks uint64) uint64 {
	count := uint64(0)
	start := uint64(0)
	for i := uint64(0); i < BlocksPerArena; i++ {
		if ar.isFree(i) {
			if count == 0 {
				start = i
			}
			count++
			if count >= blocks {
				return start
			}
		} else {
			count = 0
		}
	}
	return BlocksPerArena
}

func (ar *Arena) isFree(block uint64) bool {
	idx := block / 64
	bit := block % 64
	return (ar.bitmap[idx] & (1 << bit)) == 0
}

func (ar *Arena) markAllocated(block uint64) {
	idx := block / 64
	bit := block % 64
	ar.bitmap[idx] |= (1 << bit)
}

func (ar *Arena) markFree(block uint64) {
	idx := block / 64
	bit := block % 64
	ar.bitmap[idx] &^= (1 << bit)
}

func (ar *Arena) countAllocated() uint64 {
	var count uint64
	for _, word := range ar.bitmap {
		count += uint64(bits.OnesCount64(word))
	}
	return count
}
