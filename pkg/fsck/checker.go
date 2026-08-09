package fsck

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// MetadataStore is the interface required by the checker.
type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
}

// Finding represents a single check result.
type Finding struct {
	Level   string // "error", "warning", "info"
	Message string
	Details map[string]any
}

type Checker struct {
	Store    MetadataStore
	Findings []Finding
}

func New(store MetadataStore) *Checker {
	return &Checker{Store: store}
}

func (c *Checker) Run(ctx context.Context) []Finding {
	c.Findings = nil

	c.checkInodesDecodable(ctx)
	c.checkDirentsReferenced(ctx)
	c.checkNlinkConsistency(ctx)
	c.checkExtentValidity(ctx)
	c.checkArenaBoundaries(ctx)
	c.checkArenaOrphans(ctx)

	return c.Findings
}

func (c *Checker) ErrorCount() int {
	n := 0
	for _, f := range c.Findings {
		if f.Level == "error" {
			n++
		}
	}
	return n
}

func (c *Checker) WarningCount() int {
	n := 0
	for _, f := range c.Findings {
		if f.Level == "warning" {
			n++
		}
	}
	return n
}

// ---- individual checks ----

func (c *Checker) checkInodesDecodable(ctx context.Context) {
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixInode)
	for _, kv := range kvs {
		if len(kv.Value) < 72 {
			c.Findings = append(c.Findings, Finding{
				Level:   "error",
				Message: fmt.Sprintf("corrupt inode: %s (len=%d)", string(kv.Key), len(kv.Value)),
			})
		}
	}
}

func (c *Checker) checkDirentsReferenced(ctx context.Context) {
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixDirent)
	seenInos := c.collectInodeSet(ctx)
	for _, kv := range kvs {
		ino := decodeUint64(kv.Value)
		if ino == 0 {
			continue
		}
		if !seenInos[ino] {
			c.Findings = append(c.Findings, Finding{
				Level:   "error",
				Message: fmt.Sprintf("dirent %s points to missing inode %d", string(kv.Key), ino),
			})
		}
	}
}

func (c *Checker) checkNlinkConsistency(ctx context.Context) {
	refCount := make(map[uint64]uint32)
	direntKvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixDirent)
	for _, kv := range direntKvs {
		ino := decodeUint64(kv.Value)
		if ino != 0 {
			refCount[ino]++
		}
	}

	inodeKvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixInode)
	for _, kv := range inodeKvs {
		rec := metadata.DecodeInode(kv.Value)
		if rec == nil {
			continue
		}
		ino := inoFromKey(string(kv.Key))
		// A directory's count is fixed, not counted: directories cannot be
		// hard-linked, and this filesystem does not model the ".." link a
		// subdirectory contributes to its parent.
		expected := refCount[ino]
		if rec.Mode&metadata.S_IFMT == metadata.ModeDir {
			expected = metadata.InitialNlink(rec.Mode)
		}
		if rec.Nlink != expected {
			c.Findings = append(c.Findings, Finding{
				Level: "warning",
				Message: fmt.Sprintf("nlink mismatch: ino=%d nlink=%d expected=%d",
					ino, rec.Nlink, expected),
			})
		}
	}
}

func (c *Checker) checkExtentValidity(ctx context.Context) {
	seenInos := c.collectInodeSet(ctx)
	extKvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixExtent)
	for _, ext := range metadata.DecodeExtents(extKvs) {
		if !seenInos[ext.Ino()] {
			c.Findings = append(c.Findings, Finding{
				Level:   "warning",
				Message: fmt.Sprintf("orphan extent %s (no inode)", ext.Key),
			})
		}
		if ext.DiskOff+ext.Length > maxArenaRange {
			c.Findings = append(c.Findings, Finding{
				Level: "error",
				Message: fmt.Sprintf("extent %s beyond arena range: disk_off=%d len=%d",
					ext.Key, ext.DiskOff, ext.Length),
			})
		}
	}
}

func (c *Checker) checkArenaBoundaries(ctx context.Context) {
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixArena)
	if len(kvs) == 0 {
		c.Findings = append(c.Findings, Finding{
			Level:   "info",
			Message: "no arena keys found",
		})
	}
	for _, kv := range kvs {
		node, id, ok := metadata.ParseArenaKey(string(kv.Key))
		if !ok {
			c.Findings = append(c.Findings, Finding{
				Level:   "error",
				Message: fmt.Sprintf("malformed arena ownership key %q", string(kv.Key)),
			})
			continue
		}
		if id > maxArenaID {
			c.Findings = append(c.Findings, Finding{
				Level:   "warning",
				Message: fmt.Sprintf("node %s holds arena %d, an excessive ID", node, id),
			})
		}
	}
}

// checkArenaOrphans reports arena space no node can ever reissue.
//
// Every arena ID the allocator has handed out is below the arena_alloc_log
// high-water mark, and must be either owned by a node or sitting in the free
// pool.  An ID in neither is lost space: nothing will re-adopt it on restart
// and nothing will claim it from the pool.  An ID in both is worse — the free
// pool would hand a second node into a range someone still owns.
//
// Reported, not repaired: deciding an arena is truly unowned needs the
// operator's knowledge of which nodes are permanently gone.
func (c *Checker) checkArenaOrphans(ctx context.Context) {
	// The counter holds the next unissued ID, so every arena ever handed out
	// is below it.
	counter, _ := c.Store.Get(ctx, metadata.PrefixArenaLog)
	highWater := decodeUint64(counter)

	owners := make(map[uint64]string)
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixArena)
	for _, kv := range kvs {
		if node, id, ok := metadata.ParseArenaKey(string(kv.Key)); ok {
			owners[id] = node
		}
	}

	free := make(map[uint64]bool)
	freeKvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixFreeArena)
	for _, kv := range freeKvs {
		id, err := strconv.ParseUint(strings.TrimPrefix(string(kv.Key), metadata.PrefixFreeArena), 10, 64)
		if err == nil {
			free[id] = true
		}
	}

	for id := uint64(0); id < highWater; id++ {
		node, owned := owners[id]
		switch {
		case owned && free[id]:
			c.Findings = append(c.Findings, Finding{
				Level: "error",
				Message: fmt.Sprintf("arena %d is in the free pool while %s still owns it",
					id, node),
			})
		case !owned && !free[id]:
			c.Findings = append(c.Findings, Finding{
				Level: "warning",
				Message: fmt.Sprintf("arena %d is orphaned: owned by no node and not in the free pool",
					id),
			})
		}
	}
}

// ---- helpers ----

const maxArenaRange = 1024 * (1 << 30) // 1 TB
const maxArenaID = 1024

func (c *Checker) collectInodeSet(ctx context.Context) map[uint64]bool {
	set := make(map[uint64]bool)
	kvs, _ := c.Store.GetPrefix(ctx, metadata.PrefixInode)
	for _, kv := range kvs {
		ino := inoFromKey(string(kv.Key))
		set[ino] = true
	}
	return set
}

func inoFromKey(key string) uint64 {
	trimmed := strings.TrimPrefix(key, metadata.PrefixInode)
	ino, _ := strconv.ParseUint(trimmed, 10, 64)
	return ino
}

func decodeUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	var v uint64
	for i := 0; i < 8; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v
}
