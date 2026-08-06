package compaction

import (
	"context"
	"strconv"
	"strings"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

const (
	DefaultCompactRatio = 0.5
	ArenaSizeBytes      = 1 << 30
	BlockSize           = 4096
)

// MetadataStore is the slice of the metadata store the compactor needs.
type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
	Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error)
	Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error)
}

type Compactor struct {
	Store  MetadataStore
	NodeID string
	Ratio  float64
}

func New(store MetadataStore, nodeID string) *Compactor {
	return &Compactor{Store: store, NodeID: nodeID, Ratio: DefaultCompactRatio}
}

func (c *Compactor) NeedsCompaction(ctx context.Context) (bool, []uint64) {
	arenas, err := c.Store.GetPrefix(ctx, metadata.PrefixArena)
	if err != nil || len(arenas) == 0 {
		return false, nil
	}

	var compactable []uint64
	for _, kv := range arenas {
		arenaID := c.arenaIDFromKey(string(kv.Key))
		usage := c.arenaUsage(ctx, arenaID)
		if usage < c.Ratio {
			compactable = append(compactable, arenaID)
		}
	}
	return len(compactable) > 0, compactable
}

func (c *Compactor) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			needs, candidates := c.NeedsCompaction(ctx)
			if !needs {
				continue
			}
			dstID := candidates[0] + 1024
			_, _ = c.CompactArena(ctx, candidates[0], dstID)
		}
	}
}

func (c *Compactor) arenaUsage(ctx context.Context, arenaID uint64) float64 {
	diskStart := arenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes
	totalBlocks := ArenaSizeBytes / BlockSize

	used := uint64(0)
	for _, ext := range c.extentsIn(ctx, diskStart, diskEnd) {
		used += (ext.Length + BlockSize - 1) / BlockSize
	}
	return float64(used) / float64(totalBlocks)
}

func (c *Compactor) CompactArena(ctx context.Context, srcArenaID, dstArenaID uint64) (int, error) {
	diskStart := srcArenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes

	var toRemap []metadata.Extent
	for _, ext := range c.extentsIn(ctx, diskStart, diskEnd) {
		if _, err := c.Store.Get(ctx, metadata.InodeKey(ext.Ino())); err == nil {
			toRemap = append(toRemap, ext)
		}
	}

	dstStart := dstArenaID * ArenaSizeBytes
	batchSize := 128
	remapped := 0
	for i := 0; i < len(toRemap); i += batchSize {
		end := i + batchSize
		if end > len(toRemap) {
			end = len(toRemap)
		}
		batch := toRemap[i:end]

		var ops []clientv3.Op
		for _, m := range batch {
			moved := m
			moved.DiskOff = dstStart + (m.DiskOff - diskStart)
			ops = append(ops, clientv3.OpPut(m.Key, moved.Encode()))
		}
		_, err := c.Store.Txn(ctx, nil, ops, nil)
		if err != nil {
			return remapped, err
		}
		remapped += len(batch)
	}

	c.markGlobalArenaAvailable(ctx, srcArenaID)
	c.markGlobalArenaAcquired(ctx, dstArenaID, c.NodeID)
	return remapped, nil
}

func (c *Compactor) markGlobalArenaAvailable(ctx context.Context, arenaID uint64) {
	_, _ = c.Store.Put(ctx, metadata.FreeArenaKey(arenaID), []byte("free"))
}

// markGlobalArenaAcquired records that nodeID owns arenaID.
//
// The value must be the same 8-byte big-endian encoding that membership writes
// and that the allocator reads back on restart: an ASCII form here decodes to
// arena 0, which would hand the node an arena it does not own.
func (c *Compactor) markGlobalArenaAcquired(ctx context.Context, arenaID uint64, nodeID string) {
	key := metadata.ArenaKey(nodeID)
	_, _ = c.Store.Put(ctx, key, metadata.EncodeUint64(arenaID))
}

// extentsIn returns every extent lying entirely within the device byte range
// [diskStart, diskEnd) — i.e. the live contents of one arena.
func (c *Compactor) extentsIn(ctx context.Context, diskStart, diskEnd uint64) []metadata.Extent {
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixExtent)
	var in []metadata.Extent
	for _, ext := range metadata.DecodeExtents(kvs) {
		if ext.WithinDisk(diskStart, diskEnd) {
			in = append(in, ext)
		}
	}
	return in
}

func (c *Compactor) arenaIDFromKey(key string) uint64 {
	trimmed := strings.TrimPrefix(key, metadata.PrefixArena)
	id, _ := strconv.ParseUint(trimmed, 10, 64)
	return id
}

func (c *Compactor) ArenaLiveExtents(ctx context.Context, arenaID uint64) int {
	diskStart := arenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes
	return len(c.extentsIn(ctx, diskStart, diskEnd))
}
