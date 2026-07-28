package harness

import (
	"context"
	"time"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

type Cluster struct {
	Store *MockStore
	Nodes []*Simulator
}

func NewCluster(nodes int) *Cluster {
	store := NewMockStore()
	c := &Cluster{Store: store}
	for i := 0; i < nodes; i++ {
		c.Nodes = append(c.Nodes, NewSimulatorWithStore(int64(9000+i), store))
	}
	return c
}

func (c *Cluster) FreshGetAttr(ctx context.Context, ino uint64) *metadata.InodeRecord {
	val, _ := c.Store.Get(ctx, metadata.InodeKey(ino))
	if val == nil {
		return nil
	}
	return metadata.DecodeInode(val)
}

func (c *Cluster) FreshLookup(ctx context.Context, parent uint64, name string) uint64 {
	val, _ := c.Store.Get(ctx, metadata.DirentKey(parent, name))
	if val == nil {
		return 0
	}
	return metadata.DecodeUint64(val)
}

func (c *Cluster) FreshListDir(ctx context.Context, parent uint64) []string {
	prefix := metadata.DirentPrefix(parent)
	kvs, _ := c.Store.GetPrefix(ctx, prefix)
	names := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		names = append(names, string(kv.Key[len(prefix):]))
	}
	return names
}

func (c *Cluster) tryAcquireLock(ctx context.Context, ino uint64) bool {
	key := metadata.LockKey(ino)
	return c.Store.CASPut(ctx, key, []byte("locked"))
}

func (c *Cluster) releaseLock(ctx context.Context, ino uint64) {
	_ = c.Store.Delete(ctx, metadata.LockKey(ino))
}

func (c *Cluster) createDirIfMissing(ctx context.Context, parent uint64, name string, ino uint64) {
	if c.FreshLookup(ctx, parent, name) != 0 {
		return
	}
	rec := &metadata.InodeRecord{
		Ino: ino, Mode: metadata.ModeDir | 0755, Nlink: 1, Blksize: 4096,
		Atime: time.Now(), Mtime: time.Now(), Ctime: time.Now(),
	}
	_, _ = c.Store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	_, _ = c.Store.Put(ctx, metadata.DirentKey(parent, name), metadata.EncodeUint64(ino))
}

func (c *Cluster) checkAllInvariants() int {
	total := 0
	for _, n := range c.Nodes {
		total += n.checkInvariants()
	}
	return total
}
