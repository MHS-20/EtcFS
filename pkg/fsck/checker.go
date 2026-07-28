package fsck

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
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
	c.checkOrphanExtents(ctx)
	c.checkArenaBoundaries(ctx)

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
	kvs, _ := c.Store.GetPrefix(ctx, "inode:")
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
	kvs, _ := c.Store.GetPrefix(ctx, "dirent:")
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
	direntKvs, _ := c.Store.GetPrefix(ctx, "dirent:")
	for _, kv := range direntKvs {
		ino := decodeUint64(kv.Value)
		if ino != 0 {
			refCount[ino]++
		}
	}

	inodeKvs, _ := c.Store.GetPrefix(ctx, "inode:")
	for _, kv := range inodeKvs {
		ino := inoFromKey(string(kv.Key))
		nlink := nlinkFromValue(kv.Value)
		if nlink != refCount[ino] {
			c.Findings = append(c.Findings, Finding{
				Level: "warning",
				Message: fmt.Sprintf("nlink mismatch: ino=%d nlink=%d dirents=%d",
					ino, nlink, refCount[ino]),
			})
		}
	}
}

func (c *Checker) checkExtentValidity(ctx context.Context) {
	seenInos := c.collectInodeSet(ctx)
	extKvs, _ := c.Store.GetPrefix(ctx, "extent:")
	for _, kv := range extKvs {
		ino := inoFromExtentKey(string(kv.Key))
		if !seenInos[ino] {
			c.Findings = append(c.Findings, Finding{
				Level:   "warning",
				Message: fmt.Sprintf("orphan extent %s (no inode)", string(kv.Key)),
			})
		}
		var logOff, diskOff, length, gen uint64
		_, _ = fmt.Sscanf(string(kv.Value), "%d,%d,%d,%d", &logOff, &diskOff, &length, &gen)
		if diskOff+length > maxArenaRange {
			c.Findings = append(c.Findings, Finding{
				Level: "error",
				Message: fmt.Sprintf("extent %s beyond arena range: disk_off=%d len=%d",
					string(kv.Key), diskOff, length),
			})
		}
	}
}

func (c *Checker) checkOrphanExtents(ctx context.Context) {
	extKvs, _ := c.Store.GetPrefix(ctx, "extent:")
	for _, kv := range extKvs {
		if _, ok := c.collectInodeSet(ctx)[inoFromExtentKey(string(kv.Key))]; !ok {
			break // already reported in checkExtentValidity
		}
	}
	_ = extKvs
}

func (c *Checker) checkArenaBoundaries(ctx context.Context) {
	kvs, _ := c.Store.GetPrefix(ctx, "arena:")
	if len(kvs) == 0 {
		c.Findings = append(c.Findings, Finding{
			Level:   "info",
			Message: "no arena keys found",
		})
	}
	for _, kv := range kvs {
		id := decodeUint64(kv.Value)
		if id > maxArenaID {
			c.Findings = append(c.Findings, Finding{
				Level:   "warning",
				Message: fmt.Sprintf("arena %s has excessive ID %d", string(kv.Key), id),
			})
		}
	}
}

// ---- helpers ----

const maxArenaRange = 1024 * (1 << 30) // 1 TB
const maxArenaID = 1024

func (c *Checker) collectInodeSet(ctx context.Context) map[uint64]bool {
	set := make(map[uint64]bool)
	kvs, _ := c.Store.GetPrefix(ctx, "inode:")
	for _, kv := range kvs {
		ino := inoFromKey(string(kv.Key))
		set[ino] = true
	}
	return set
}

func inoFromKey(key string) uint64 {
	trimmed := strings.TrimPrefix(key, "inode:")
	ino, _ := strconv.ParseUint(trimmed, 10, 64)
	return ino
}

func inoFromExtentKey(key string) uint64 {
	trimmed := strings.TrimPrefix(key, "extent:")
	parts := strings.SplitN(trimmed, "/", 2)
	ino, _ := strconv.ParseUint(parts[0], 10, 64)
	return ino
}

func nlinkFromValue(val []byte) uint32 {
	if len(val) < 6 {
		return 0
	}
	return uint32(val[5])<<24 | uint32(val[4])<<16 | uint32(val[3])<<8 | uint32(val[2])
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
