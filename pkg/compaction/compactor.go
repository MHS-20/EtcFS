package compaction

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

const (
	DefaultCompactRatio = 0.5
	ArenaSizeBytes      = 1 << 30
	BlockSize           = 4096
)

type MetadataStore = metadata.MetadataStore

type Compactor struct {
	Store  MetadataStore
	NodeID string
	Ratio  float64
}

type ExtentMapping struct {
	Key     string
	LogOff  uint64
	DiskOff uint64
	Length  uint64
	Gen     uint64
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

func (c *Compactor) arenaUsage(ctx context.Context, arenaID uint64) float64 {
	diskStart := arenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes
	totalBlocks := ArenaSizeBytes / BlockSize

	used := uint64(0)
	extKvs, _ := c.Store.GetPrefix(ctx, "extent:")
	for _, kv := range extKvs {
		var logOff, diskOff, length, gen uint64
		_, _ = fmt.Sscanf(string(kv.Value), "%d,%d,%d,%d", &logOff, &diskOff, &length, &gen)
		if diskOff >= diskStart && diskOff+length <= diskEnd {
			blocks := (length + BlockSize - 1) / BlockSize
			used += blocks
		}
	}
	return float64(used) / float64(totalBlocks)
}

func (c *Compactor) CompactArena(ctx context.Context, srcArenaID, dstArenaID uint64) (int, error) {
	diskStart := srcArenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes

	extKvs, _ := c.Store.GetPrefix(ctx, "extent:")
	var toRemap []ExtentMapping
	for _, kv := range extKvs {
		var logOff, diskOff, length, gen uint64
		_, _ = fmt.Sscanf(string(kv.Value), "%d,%d,%d,%d", &logOff, &diskOff, &length, &gen)
		if diskOff >= diskStart && diskOff+length <= diskEnd {
			ino := c.inoFromExtentKey(string(kv.Key))
			if _, err := c.Store.Get(ctx, metadata.InodeKey(ino)); err == nil {
				toRemap = append(toRemap, ExtentMapping{
					Key: string(kv.Key), LogOff: logOff,
					DiskOff: diskOff, Length: length, Gen: gen,
				})
			}
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
			newDiskOff := dstStart + (m.DiskOff - diskStart)
			newVal := fmt.Sprintf("%d,%d,%d,%d", m.LogOff, newDiskOff, m.Length, m.Gen)
			ops = append(ops, clientv3.OpPut(m.Key, newVal))
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
	key := fmt.Sprintf("%s%d", "free_arena:", arenaID)
	_, _ = c.Store.Put(ctx, key, []byte("free"))
}

func (c *Compactor) markGlobalArenaAcquired(ctx context.Context, arenaID uint64, nodeID string) {
	key := metadata.ArenaKey(nodeID)
	_, _ = c.Store.Put(ctx, key, []byte(fmt.Sprintf("id=%d", arenaID)))
}

func (c *Compactor) inoFromExtentKey(key string) uint64 {
	trimmed := strings.TrimPrefix(key, "extent:")
	parts := strings.SplitN(trimmed, "/", 2)
	ino, _ := strconv.ParseUint(parts[0], 10, 64)
	return ino
}

func (c *Compactor) arenaIDFromKey(key string) uint64 {
	trimmed := strings.TrimPrefix(key, metadata.PrefixArena)
	id, _ := strconv.ParseUint(trimmed, 10, 64)
	return id
}

func (c *Compactor) ArenaLiveExtents(ctx context.Context, arenaID uint64) int {
	diskStart := arenaID * ArenaSizeBytes
	diskEnd := diskStart + ArenaSizeBytes
	count := 0
	extKvs, _ := c.Store.GetPrefix(ctx, "extent:")
	for _, kv := range extKvs {
		var _, diskOff, length, _ uint64
		_, _ = fmt.Sscanf(string(kv.Value), "%d,%d,%d,%d",
			new(uint64), &diskOff, &length, new(uint64))
		if diskOff >= diskStart && diskOff+length <= diskEnd {
			count++
		}
	}
	_ = diskEnd
	return count
}
