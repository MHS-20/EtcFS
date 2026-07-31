package fsinfo

import (
	"context"
	"fmt"
	"strings"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
}

type Info struct {
	TotalInodes      uint64
	TotalFiles       uint64
	TotalDirs        uint64
	TotalSize        uint64
	TotalExtents     uint64
	TotalLocks       uint64
	TotalDirents     uint64
	ArenaCount       uint64
	MemberCount      uint64
	ArenaUtilization map[string]float64
}

func Collect(ctx context.Context, store MetadataStore) (*Info, error) {
	info := &Info{
		ArenaUtilization: make(map[string]float64),
	}

	inodeKvs, _ := store.GetPrefix(ctx, "inode:")
	info.TotalInodes = uint64(len(inodeKvs))

	for _, kv := range inodeKvs {
		rec := metadata.DecodeInode(kv.Value)
		if rec != nil {
			if rec.Mode&metadata.ModeDir != 0 {
				info.TotalDirs++
			} else {
				info.TotalFiles++
			}
			info.TotalSize += rec.Size
		}
	}

	extKvs, _ := store.GetPrefix(ctx, metadata.PrefixExtent)
	info.TotalExtents = uint64(len(extKvs))

	direntKvs, _ := store.GetPrefix(ctx, "dirent:")
	info.TotalDirents = uint64(len(direntKvs))

	lockKvs, _ := store.GetPrefix(ctx, "lock:")
	info.TotalLocks = uint64(len(lockKvs))

	arenaKvs, _ := store.GetPrefix(ctx, "arena:")
	info.ArenaCount = uint64(len(arenaKvs))

	for _, kv := range arenaKvs {
		node := strings.TrimPrefix(string(kv.Key), "arena:")
		freeKvs, _ := store.GetPrefix(ctx, metadata.PrefixFreeArena)
		info.ArenaUtilization[node] = float64(len(freeKvs))
	}

	memKvs, _ := store.GetPrefix(ctx, "membership:")
	info.MemberCount = uint64(len(memKvs))

	return info, nil
}

func (i *Info) String() string {
	return fmt.Sprintf(
		"inodes=%d files=%d dirs=%d size=%d extents=%d dirents=%d locks=%d arenas=%d members=%d",
		i.TotalInodes, i.TotalFiles, i.TotalDirs, i.TotalSize,
		i.TotalExtents, i.TotalDirents, i.TotalLocks,
		i.ArenaCount, i.MemberCount,
	)
}
