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
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

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
	key := metadata.PrefixArenaLog

	for attempt := 0; attempt < 5; attempt++ {
		v, err := a.store.Get(ctx, key)
		if err != nil {
			return 0, fmt.Errorf("read arena counter: %w", err)
		}
		current := uint64(0)
		if v != nil {
			current = metadata.DecodeUint64(v)
		}
		next := current + 1

		var cmps []clientv3.Cmp
		if current == 0 {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)}
		} else {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.Value(key), "=", string(metadata.EncodeUint64(current)))}
		}

		op := clientv3.OpPut(key, string(metadata.EncodeUint64(next)))
		ok, err := a.store.Txn(ctx, cmps, []clientv3.Op{op}, nil)
		if err != nil {
			return 0, err
		}
		if ok {
			return current, nil
		}
	}
	return 0, fmt.Errorf("arena ID exhausted")
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

	extKvs, _ := a.store.GetPrefix(ctx, "extent:")
	for _, kv := range extKvs {
		var logOff, diskOff, length, gen uint64
		_, _ = fmt.Sscanf(string(kv.Value), "%d,%d,%d,%d", &logOff, &diskOff, &length, &gen)
		for _, free := range a.arenas {
			if diskOff >= free.DiskStart && diskOff+length <= free.DiskEnd {
				start := (diskOff - free.DiskStart) / BlockSize
				blocks := (length + BlockSize - 1) / BlockSize
				for i := uint64(0); i < blocks && start+i < BlocksPerArena; i++ {
					free.markAllocated(start + i)
				}
			}
		}
	}
	return nil
}

func (a *Allocator) existingArenaIDs(ctx context.Context) ([]uint64, error) {
	kvs, err := a.store.GetPrefix(ctx, metadata.PrefixArena)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, kv := range kvs {
		id := metadata.DecodeUint64(kv.Value)
		if id > 0 {
			ids = append(ids, id)
		}
	}
	return ids, nil
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
	for i := uint64(0); i < BlocksPerArena; i++ {
		if !ar.isFree(i) {
			count++
		}
	}
	return count
}
