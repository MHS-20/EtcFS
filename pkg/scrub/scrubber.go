// Package scrub implements the continuous background verification scrubber.
//
// The scrubber periodically cross-checks etcd metadata against stored state
// to detect:
//  1. Extent collisions — two inodes claiming the same disk offset
//  2. Range violations — extent outside its owning arena's range
//  3. Orphan extents — allocated blocks with no inode reference
//  4. Generation mismatches — extent stamped with wrong fencing generation
//  5. Nlink inconsistencies — inode nlink doesn't match dirent count
//  6. Unreferenced inodes — inode records no directory entry names
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

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// MetadataStore is the slice of the metadata store the scrubber needs: it
// reads keys and deletes the orphans it is allowed to reclaim, and nothing
// else.  Declaring only that keeps the scrubber testable against a stub and
// makes the blast radius of a scrub bug obvious from its dependencies.
type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
	Delete(ctx context.Context, key string) error
}

// Result records a single scrub finding.
type Result struct {
	Type    string // "collision", "orphan", "range", "generation", "nlink"
	Detail  string
	Ino     uint64
	DiskOff uint64
	Length  uint64 // length of the disk range, when the finding refers to one
	Key     string // etcd key the finding refers to, when it is a single key
	AutoFix bool   // true if the scrubber can safely remediate
}

// Scrubber runs continuous verification of filesystem invariants.
type Scrubber struct {
	store     MetadataStore
	log       Logger
	nodeID    string
	interval  time.Duration
	rateLimit float64 // max fraction of foreground I/O bandwidth to use
	reclaimer Reclaimer

	// deviceSize bounds where an extent may legitimately live.  Zero means the
	// scrubber was not told, and the range check is skipped rather than run
	// against a guessed limit.
	deviceSize uint64

	mu           sync.Mutex
	lastRun      time.Time
	anomalies    []Result
	totalChecked int64
}

// Reclaimer returns a disk range to the block allocator, and reports which
// ranges this node is entitled to reclaim.  Implemented by pkg/arena.Allocator.
//
// Owns is what keeps reclamation from leaking across nodes.  The free list is
// per-process and in-memory, so an orphaned extent inside a peer's arena has to
// be left to that peer: deleting the extent record here would remove the only
// reference its bitmap is rebuilt from, and those blocks would stay marked
// allocated there until the peer restarts.  Each node reclaims its own arenas,
// and every arena has exactly one owner, so every orphan is still covered.
type Reclaimer interface {
	Free(diskOff, size uint64)
	Owns(diskOff uint64) bool
}

// Logger is the logging interface used by the scrubber.
type Logger interface {
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// New creates a Scrubber.
func New(store MetadataStore, nodeID string, interval time.Duration, log Logger) *Scrubber {
	return &Scrubber{
		store:     store,
		log:       log,
		nodeID:    nodeID,
		interval:  interval,
		rateLimit: 0.1, // default: 10% of I/O bandwidth
	}
}

// SetDeviceSize tells the scrubber how large the shared device is, so its
// range check uses the same number the allocator does rather than a hardcoded
// copy of it.
func (s *Scrubber) SetDeviceSize(bytes uint64) {
	s.deviceSize = bytes
}

// SetReclaimer attaches the block allocator whose space the scrubber may
// return on an auto-fix.  Without one the scrubber still deletes orphaned
// extent records, but their blocks stay allocated.
func (s *Scrubber) SetReclaimer(r Reclaimer) {
	s.reclaimer = r
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
	dead := s.CheckDeadExtents(ctx)
	rangeV := s.CheckRangeValidity(ctx)
	genM := s.CheckGenerationConsistency(ctx)
	nlinkV := s.CheckNlinkConsistency(ctx)
	unref := s.CheckUnreferencedInodes(ctx)

	s.record(collisions, orphans, dead, rangeV, genM, nlinkV, unref)

	// Reclaim only what this node owns.  An orphan in a peer's arena is that
	// peer's to clean up, and one in a free-pool arena is cleaned up by whoever
	// claims it next — the claimer marks it live from the extent record that is
	// still there, then its own scrub pass finds it orphaned and reclaims it.
	// So every orphan is still covered, without a node ever deleting the record
	// another node's bitmap is rebuilt from.
	reclaimable := make([]Result, 0, len(orphans)+len(dead))
	reclaimable = append(reclaimable, orphans...)
	reclaimable = append(reclaimable, dead...)

	for _, r := range reclaimable {
		if !r.AutoFix {
			continue
		}
		if s.reclaimer == nil || !s.reclaimer.Owns(r.DiskOff) {
			continue
		}
		s.log.Info("scrub auto-fix: reclaiming extent", "type", r.Type, "detail", r.Detail)
		// Delete first: the blocks must stop being reachable through metadata
		// before they can be handed to another allocation, or a reader
		// resolving the extent could land on data already overwritten.
		if err := s.store.Delete(ctx, r.Key); err != nil {
			s.log.Error("scrub auto-fix: extent not deleted, blocks not reclaimed",
				"key", r.Key, "error", err)
			continue
		}
		if r.Length > 0 {
			s.reclaimer.Free(r.DiskOff, r.Length)
		}
	}

	total := len(collisions) + len(orphans) + len(dead) + len(rangeV) + len(genM) + len(nlinkV) + len(unref)
	if total > 0 {
		s.log.Warn("scrub found anomalies", "count", total, "collisions", len(collisions),
			"orphans", len(orphans), "dead", len(dead), "range", len(rangeV),
			"generation", len(genM), "nlink", len(nlinkV), "unreferenced", len(unref))
	} else {
		s.log.Info("scrub pass clean")
	}
}

// record folds a pass's findings into the anomaly list.
//
// The list is deduplicated by type and key and capped: a permanent anomaly is
// re-found by every pass, so an append-only list grew without bound for as long
// as the daemon ran — one finding every 30 seconds, forever, for a condition an
// operator may well have decided to live with.  Keeping the newest is the right
// end to keep: the oldest entries describe passes whose findings have since
// been re-reported anyway.
func (s *Scrubber) record(passes ...[]Result) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]bool, len(s.anomalies))
	for _, r := range s.anomalies {
		seen[r.Type+"\x00"+r.Key] = true
	}
	for _, pass := range passes {
		for _, r := range pass {
			id := r.Type + "\x00" + r.Key
			if seen[id] {
				continue
			}
			seen[id] = true
			s.anomalies = append(s.anomalies, r)
		}
	}
	if excess := len(s.anomalies) - maxAnomalies; excess > 0 {
		s.anomalies = append(s.anomalies[:0], s.anomalies[excess:]...)
	}
	s.totalChecked++
}

// maxAnomalies bounds the retained findings.  Large enough that a real burst is
// still legible in full, small enough that a permanently unfixable anomaly
// cannot consume the process.
const maxAnomalies = 1000

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
			// The disk range is carried along, not just the key: deleting the
			// extent record makes the metadata clean but leaves the blocks
			// behind it marked allocated forever.  Reclaiming them is what
			// makes deletion actually return space.  A malformed value yields
			// a zero range, and Free ignores ranges outside this node's own
			// arenas, so the metadata cleanup still happens either way.
			ext, _ := metadata.DecodeExtent(key, kv.Value)
			results = append(results, Result{
				Type:    "orphan",
				Detail:  fmt.Sprintf("extent %s has no inode reference", key),
				Ino:     ino,
				DiskOff: ext.DiskOff,
				Length:  ext.Length,
				Key:     key,
				AutoFix: true,
			})
		}
	}
	return results
}

// CheckDeadExtents detects extents of a *live* inode that nothing can read any
// more: those left entirely past the file's size by a truncate, and those a
// later write has fully covered.
//
// The orphan check cannot see either of them — it looks for extents whose inode
// is gone, and here the inode is very much alive.  Without this the blocks
// behind them stay allocated for the lifetime of the file.
//
// Both cases arise on the node that performed the operation as well, but that
// node reclaims what it owns inline (see ipc.Service.dropExtent).  What reaches
// here is the cross-node remainder: a truncate or overwrite issued from one
// node against bytes sitting in another node's arena, which only the owner may
// reclaim.
func (s *Scrubber) CheckDeadExtents(ctx context.Context) []Result {
	extKvs, err := s.store.GetPrefix(ctx, metadata.PrefixExtent)
	if err != nil {
		s.log.Error("scrub: cannot scan extents", "error", err)
		return nil
	}

	sizes := make(map[uint64]uint64)
	inodeKvs, err := s.store.GetPrefix(ctx, metadata.PrefixInode)
	if err != nil {
		s.log.Error("scrub: cannot scan inodes", "error", err)
		return nil
	}
	for _, kv := range inodeKvs {
		if rec := metadata.DecodeInode(kv.Value); rec != nil {
			sizes[rec.Ino] = rec.Size
		}
	}

	byIno := make(map[uint64][]metadata.Extent)
	for _, ext := range metadata.DecodeExtents(extKvs) {
		byIno[ext.Ino()] = append(byIno[ext.Ino()], ext)
	}

	var results []Result
	for ino, extents := range byIno {
		size, alive := sizes[ino]
		if !alive {
			continue // no inode: the orphan check owns this one
		}
		for _, ext := range extents {
			reason := deadReason(ext, size, extents)
			if reason == "" {
				continue
			}
			results = append(results, Result{
				Type:    "dead",
				Detail:  fmt.Sprintf("extent %s is unreachable: %s", ext.Key, reason),
				Ino:     ino,
				DiskOff: ext.DiskOff,
				Length:  ext.Length,
				Key:     ext.Key,
				AutoFix: true,
			})
		}
	}
	return results
}

// deadReason explains why ext can no longer be read, or "" if it still can.
//
// An extent only partly past the end of the file is left alone: trimming it is
// a rewrite rather than a delete, and the surviving head is still live data.
// Its tail is reclaimed when the file is deleted or fully overwritten.
func deadReason(ext metadata.Extent, size uint64, siblings []metadata.Extent) string {
	if ext.LogOff >= size {
		return fmt.Sprintf("it starts at %d, past the file size of %d", ext.LogOff, size)
	}
	for _, other := range siblings {
		if other.Supersedes(ext) {
			return fmt.Sprintf("extent %s overwrote it", other.Key)
		}
	}
	return ""
}

// CheckRangeValidity detects extents that do not fit on the device.
//
// Skipped entirely when the device size is unknown: the previous version
// compared against a hardcoded 1 TiB, which was neither the device's size nor
// the limit fsck used.
func (s *Scrubber) CheckRangeValidity(ctx context.Context) []Result {
	if s.deviceSize == 0 {
		return nil
	}

	extKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixExtent)
	results := make([]Result, 0, len(extKvs))
	for _, ext := range metadata.DecodeExtents(extKvs) {
		if ext.DiskOff+ext.Length > s.deviceSize {
			results = append(results, Result{
				Type: "range",
				Detail: fmt.Sprintf("extent %s disk_off=%d+%d is past the end of the %d byte device",
					ext.Key, ext.DiskOff, ext.Length, s.deviceSize),
				Ino:     ext.Ino(),
				DiskOff: ext.DiskOff,
				Key:     ext.Key,
			})
		}
	}
	return results
}

// CheckGenerationConsistency detects extents stamped with a generation their
// writer has never reached.
//
// The stamp is the writer's fencing generation at commit time, and every commit
// is guarded by that generation, so an extent can never legitimately carry one
// above its writer's current value.  One that does means the guard let a write
// through it should have rejected, or the record was written outside the daemon.
//
// It deliberately does *not* flag extents stamped below the current generation.
// Those are simply older than the node's last fence, which is what every extent
// written before a fence looks like.  The check used to compare against the
// maximum generation across the whole cluster, so one node ever being fenced
// turned every extent written by every other node into an anomaly.
func (s *Scrubber) CheckGenerationConsistency(ctx context.Context) []Result {
	nodeGens := make(map[string]uint64)
	genKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixGen)
	for _, kv := range genKvs {
		nodeID := string(kv.Key[len(metadata.PrefixGen):])
		n := uint64(0)
		_, _ = fmt.Sscanf(string(kv.Value), "%d", &n)
		nodeGens[nodeID] = n
	}

	extKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixExtent)
	results := make([]Result, 0, len(extKvs))
	for _, ext := range metadata.DecodeExtents(extKvs) {
		current, known := nodeGens[ext.Node]
		if !known || ext.Gen <= current {
			continue
		}
		results = append(results, Result{
			Type: "generation",
			Detail: fmt.Sprintf("extent %s stamped gen=%d, but %s has only reached %d",
				ext.Key, ext.Gen, ext.Node, current),
			Ino:     ext.Ino(),
			DiskOff: ext.DiskOff,
			Key:     ext.Key,
		})
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
		expected := expectedNlink(rec, refCount[rec.Ino])
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

// CheckUnreferencedInodes detects inode records that no directory entry names.
//
// Nothing can reach such an inode: it does not appear in any listing, and its
// extents are invisible to the orphan check, which looks for extents whose
// inode is *missing* rather than unreachable.  Every creating operation is a
// single transaction, so this should never appear; when it does, it is either a
// leak from an older write path or genuine corruption.
//
// It is reported, never auto-fixed.  Deleting an inode is not reversible, and
// the blocks behind it are reclaimed by the orphan check once it goes — so an
// operator, or fsck, decides.
func (s *Scrubber) CheckUnreferencedInodes(ctx context.Context) []Result {
	referenced := make(map[uint64]bool)
	direntKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixDirent)
	for _, kv := range direntKvs {
		referenced[metadata.DecodeUint64(kv.Value)] = true
	}

	inodeKvs, _ := s.store.GetPrefix(ctx, metadata.PrefixInode)
	results := make([]Result, 0, len(inodeKvs))
	for _, kv := range inodeKvs {
		rec := metadata.DecodeInode(kv.Value)
		// The root has no dirent by construction — it is the directory every
		// path starts from, so nothing names it.
		if rec == nil || rec.Ino == metadata.RootIno || referenced[rec.Ino] {
			continue
		}
		results = append(results, Result{
			Type:   "unreferenced",
			Detail: fmt.Sprintf("inode %d has no directory entry", rec.Ino),
			Ino:    rec.Ino,
			Key:    string(kv.Key),
		})
	}
	return results
}

// expectedNlink is the link count an inode should carry given how many dirents
// point at it.
//
// For a directory the answer is fixed rather than counted. Directories cannot
// be hard-linked, and this filesystem does not model the ".." link each
// subdirectory would contribute to its parent, so every directory keeps the
// count it was created with — counting dirents instead would flag every
// directory in the filesystem.
func expectedNlink(rec *metadata.InodeRecord, dirents uint32) uint32 {
	if rec.Mode&metadata.S_IFMT == metadata.ModeDir {
		return metadata.InitialNlink(rec.Mode)
	}
	return dirents
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
