// Package scrub implements the continuous background verification scrubber.
//
// The scrubber periodically cross-checks etcd metadata against stored state
// to detect:
//  1. Extent collisions — two inodes claiming the same disk offset
//  2. Range violations — extent outside its owning arena's range
//  3. Orphan extents — allocated blocks with no inode reference
//  4. Generation mismatches — extent stamped with wrong fencing generation
//  5. Nlink inconsistencies — inode nlink doesn't match dirent count
//
// Anomalies are logged and emitted as metrics.  The scrubber can reclaim
// safe anomalies (orphans) automatically.  Unsafe anomalies (collisions,
// range violations) are alerted but not auto-remediated.
package scrub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// Result records a single scrub finding.
type Result struct {
	Type    string // "collision", "orphan", "range", "generation", "nlink"
	Detail  string
	Ino     uint64
	DiskOff uint64
	Key     string // etcd key the finding refers to, when it is a single key
	AutoFix bool   // true if the scrubber can safely remediate
}

// Scrubber runs continuous verification of filesystem invariants.
type Scrubber struct {
	store     metadata.MetadataStore
	log       Logger
	nodeID    string
	interval  time.Duration
	rateLimit float64 // max fraction of foreground I/O bandwidth to use

	mu           sync.Mutex
	lastRun      time.Time
	anomalies    []Result
	totalChecked int64
}

// Logger is the logging interface used by the scrubber.
type Logger interface {
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// New creates a Scrubber.
func New(store metadata.MetadataStore, nodeID string, interval time.Duration, log Logger) *Scrubber {
	return &Scrubber{
		store:     store,
		log:       log,
		nodeID:    nodeID,
		interval:  interval,
		rateLimit: 0.1, // default: 10% of I/O bandwidth
	}
}

// Run starts the scrub loop.  Blocks until ctx is cancelled.
func (s *Scrubber) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.log.Info("scrubber started", "interval", s.interval)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scrubber stopped")
			return
		case <-ticker.C:
			s.RunScrubPass(ctx)
		}
	}
}

// runScrubPass executes a single full scrub cycle.
func (s *Scrubber) RunScrubPass(ctx context.Context) {
	s.mu.Lock()
	s.lastRun = time.Now()
	s.mu.Unlock()

	s.log.Info("scrub pass starting")

	collisions := s.CheckExtentCollisions(ctx)
	orphans := s.CheckOrphanExtents(ctx)
	rangeV := s.CheckRangeValidity(ctx)
	genM := s.CheckGenerationConsistency(ctx)
	nlinkV := s.CheckNlinkConsistency(ctx)

	s.mu.Lock()
	s.anomalies = append(s.anomalies, collisions...)
	s.anomalies = append(s.anomalies, orphans...)
	s.anomalies = append(s.anomalies, rangeV...)
	s.anomalies = append(s.anomalies, genM...)
	s.anomalies = append(s.anomalies, nlinkV...)
	s.totalChecked++
	s.mu.Unlock()

	for _, r := range orphans {
		if r.AutoFix {
			s.log.Info("scrub auto-fix: reclaiming orphan extent", "detail", r.Detail)
			_ = s.store.Delete(ctx, r.Key)
		}
	}

	total := len(collisions) + len(orphans) + len(rangeV) + len(genM) + len(nlinkV)
	if total > 0 {
		s.log.Warn("scrub found anomalies", "count", total, "collisions", len(collisions),
			"orphans", len(orphans), "range", len(rangeV), "generation", len(genM), "nlink", len(nlinkV))
	} else {
		s.log.Info("scrub pass clean")
	}
}

// ---- scrub checks ----

// checkExtentCollisions detects two different inodes claiming the same disk offset.
func (s *Scrubber) CheckExtentCollisions(ctx context.Context) []Result {
	kvs, err := s.store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		s.log.Error("scrub: cannot scan extents", "error", err)
		return nil
	}

	// Map: disk_off → list of inode claimers
	seen := make(map[uint64][]uint64)
	var results []Result

	for _, ext := range metadata.DecodeExtents(kvs) {
		ino := ext.Ino()
		for _, existingIno := range seen[ext.DiskOff] {
			if existingIno != ino {
				results = append(results, Result{
					Type:    "collision",
					Detail:  fmt.Sprintf("ino %d and %d both claim disk_off=%d", ino, existingIno, ext.DiskOff),
					Ino:     ino,
					DiskOff: ext.DiskOff,
					Key:     ext.Key,
				})
			}
		}
		seen[ext.DiskOff] = append(seen[ext.DiskOff], ino)
	}
	return results
}

// checkOrphanExtents detects allocated extents with no inode reference.
func (s *Scrubber) CheckOrphanExtents(ctx context.Context) []Result {
	extKvs, err := s.store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		return nil
	}

	var results []Result
	for _, kv := range extKvs {
		key := string(kv.Key)
		ino := metadata.InoFromExtentKey(key)

		// Check if the inode exists
		val, _ := s.store.Get(ctx, metadata.InodeKey(ino))
		if val == nil {
			results = append(results, Result{
				Type:    "orphan",
				Detail:  fmt.Sprintf("extent %s has no inode reference", key),
				Ino:     ino,
				Key:     key,
				AutoFix: true,
			})
		}
	}
	return results
}

// checkRangeValidity detects extents outside their arena range.
func (s *Scrubber) CheckRangeValidity(ctx context.Context) []Result {
	// Simplified: check that extents fall within the global arena range (0 to MaxArena)
	const maxArena = 1024 // up to 1024 arenas (1 TB total)
	const arenaSize = 1 << 30

	var results []Result
	extKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixExtent)
	for _, ext := range metadata.DecodeExtents(extKvs) {
		if ext.DiskOff+ext.Length > maxArena*arenaSize {
			results = append(results, Result{
				Type:    "range",
				Detail:  fmt.Sprintf("extent %s disk_off=%d+%d exceeds arena range", ext.Key, ext.DiskOff, ext.Length),
				Ino:     ext.Ino(),
				DiskOff: ext.DiskOff,
				Key:     ext.Key,
			})
		}
	}
	return results
}

// checkGenerationConsistency detects extents with stale fencing generations.
func (s *Scrubber) CheckGenerationConsistency(ctx context.Context) []Result {
	// Read all known node generations
	nodeGens := make(map[string]uint64)
	genKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixGen)
	for _, kv := range genKvs {
		nodeID := string(kv.Key[len(metadata.PrefixGen):])
		n := uint64(0)
		_, _ = fmt.Sscanf(string(kv.Value), "%d", &n)
		nodeGens[nodeID] = n
	}
	maxGen := uint64(0)
	for _, g := range nodeGens {
		if g > maxGen {
			maxGen = g
		}
	}

	var results []Result
	extKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixExtent)
	for _, ext := range metadata.DecodeExtents(extKvs) {
		ino, gen := ext.Ino(), ext.Gen

		if gen < maxGen {
			results = append(results, Result{
				Type: "generation",
				Detail: fmt.Sprintf("extent %s stamped gen=%d, max_cluster_gen=%d",
					ext.Key, gen, maxGen),
				Ino:     ino,
				DiskOff: ext.DiskOff,
				Key:     ext.Key,
			})
		}
	}
	return results
}

// checkNlinkConsistency verifies every inode's nlink matches dirent count.
func (s *Scrubber) CheckNlinkConsistency(ctx context.Context) []Result {
	// Count dirent references per inode
	refCount := make(map[uint64]uint32)
	direntKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixDirent)
	for _, kv := range direntKvs {
		ino := metadata.DecodeUint64(kv.Value)
		refCount[ino]++
	}

	var results []Result
	inodeKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixInode)
	for _, kv := range inodeKvs {
		rec := metadata.DecodeInode(kv.Value)
		if rec == nil {
			continue
		}
		expected := refCount[rec.Ino]
		if rec.Nlink != expected {
			results = append(results, Result{
				Type:   "nlink",
				Detail: fmt.Sprintf("ino=%d nlink=%d dirents=%d", rec.Ino, rec.Nlink, expected),
				Ino:    rec.Ino,
			})
		}
	}
	return results
}

// Stats returns scrub statistics.
func (s *Scrubber) Stats() (passes int64, anomalies int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalChecked, len(s.anomalies)
}

// Anomalies returns all detected anomalies.
func (s *Scrubber) Anomalies() []Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Result{}, s.anomalies...)
}

// LastRun returns the time of the last scrub pass.
func (s *Scrubber) LastRun() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRun
}
