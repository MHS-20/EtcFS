package ipc

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/arena"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

const (
	// maxWriteTxnOps bounds what one write's transaction may carry, under
	// etcd's own --max-txn-ops (128 by default).  The margin covers the ops a
	// proposal adds outside the reclaim loop: the new extents, the inode's
	// size, and the lock release.
	maxWriteTxnOps = 120

	// maxReclaimOps is the most ops one buried extent contributes — the two
	// puts of a split.
	maxReclaimOps = 2
)

// Data path: everything that touches the shared block device.
//
// A write is durable on the device *before* its extent is committed to etcd,
// and the commit is guarded by this node's fencing generation.  A fenced node
// therefore leaves bytes on the device that nothing references, which is the
// safe direction to fail in — the alternative, a referenced extent written by
// a node that has lost its lease, is corruption.

// ioBuffer returns a buffer of at least n bytes usable for device I/O, along
// with the function that releases it.
//
// With O_DIRECT the buffer must be sector-aligned in both address and length,
// which needs an explicit allocation; without it a plain slice will do.  The
// returned buffer may be longer than n when rounded up to a sector.
func (s *Service) ioBuffer(n int) (b []byte, free func()) {
	if s.dev != nil && s.dev.IsDirect() {
		ss := s.dev.SectorSize()
		alignedLen := (n + ss - 1) / ss * ss
		if aligned, err := blockio.AlignedBuffer(alignedLen, ss); err == nil {
			return aligned, func() { _ = blockio.FreeBuffer(aligned) }
		}
		// Fall through: an unaligned buffer will be rejected by the device,
		// but the caller's error path is better than a panic here.
	}
	return make([]byte, n), func() {}
}

// directSafe reports whether buf can be handed to the device as-is, i.e. it is
// sector-aligned in both address and length.  Always true without O_DIRECT.
func (s *Service) directSafe(buf []byte) bool {
	if s.dev == nil || !s.dev.IsDirect() || len(buf) == 0 {
		return true
	}
	ss := uintptr(s.dev.SectorSize())
	return uintptr(len(buf))%ss == 0 && uintptr(unsafe.Pointer(&buf[0]))%ss == 0
}

// WRITE payload: [u64:ino][u64:offset][u32:data_len][data]
// Response: [i32:error][u32:written]
func (s *Service) handleWrite(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, offset := r.u64(), r.u64()
	data := r.blob()
	if !r.ok {
		return int32Resp(-22), nil
	}
	dataLen := uint32(len(data))

	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(-2), nil
	}

	if s.dev != nil {
		return s.handleWriteBlock(ctx, ino, offset, data, rec)
	}

	// No block device — update size only (metadata-only mode)
	newEnd := offset + uint64(dataLen)
	if newEnd > rec.Size {
		rec.Size = newEnd
		_, _ = s.store.Put(ctx, metadata.InodeKey(ino), metadata.EncodeInode(rec))
	}

	return writtenResp(dataLen), nil
}

func (s *Service) handleWriteBlock(ctx context.Context, ino uint64, offset uint64,
	data []byte, rec *metadata.InodeRecord) ([]byte, error) {

	dataLen := len(data)
	if dataLen == 0 {
		return writtenResp(0), nil
	}

	// A self-fenced node must not touch the shared device at all.  The
	// generation guard below is the authoritative check, but refusing here
	// avoids writing bytes we already know will never be referenced.
	if s.IsFenced() {
		s.log.Error("write: rejected, node has self-fenced", "ino", ino)
		return int32Resp(-5), nil
	}

	lk, err := s.lockInode(ctx, ino, metadata.LockExclusive)
	if err != nil {
		return int32Resp(-11), nil // EAGAIN after retries
	}
	defer lk.Release()

	runs, err := s.allocateBlocks(ctx, uint64(dataLen))
	if err != nil {
		s.log.Warn("write: cannot allocate blocks", "ino", ino, "error", err)
		return int32Resp(-28), nil
	}
	freeRuns := func() {
		for _, r := range runs {
			s.alloc.Free(r.DiskOff, r.Length)
		}
	}

	// Every run starts and ends on a block boundary, so a single buffer padded
	// out to whole blocks can be sliced per run and still satisfy O_DIRECT.
	// The padding past dataLen is written but never referenced by an extent.
	padded := (dataLen + arena.BlockSize - 1) / arena.BlockSize * arena.BlockSize
	writeData := data
	if padded != dataLen || !s.directSafe(data) {
		aligned, free := s.ioBuffer(padded)
		defer free()
		if !s.directSafe(aligned) {
			freeRuns()
			return int32Resp(-12), nil // ENOMEM
		}
		copy(aligned, data)
		writeData = aligned[:padded]
	}

	gen := s.writeGeneration(ctx)

	// The device write is independent of how the extents are numbered, so it
	// happens once even if the proposal below has to be rebuilt.
	pos := uint64(0)
	for _, r := range runs {
		if werr := s.writeRun(writeData[pos:pos+r.Length], r.DiskOff); werr != nil {
			freeRuns()
			s.log.Warn("write: block device write failed", "error", werr)
			return int32Resp(-5), nil
		}
		pos += r.Length
	}

	// One read of the inode's extents answers every question this write has:
	// which chunk numbers are free, what sequence it takes, and which existing
	// extents it is about to bury.  Both counters run one past the highest in
	// use rather than off the extent count — a truncate deletes records from
	// the middle, and counting would hand back a number that is still live.
	//
	// The first read is serializable: it only builds the proposal, and the
	// commit below refuses to overwrite a chunk that already exists, so a stale
	// answer costs a retry rather than a buried extent.  That retry re-reads
	// linearizably, which is the only way to be sure the second attempt is not
	// working from the same stale view.
	var existing []metadata.Extent
	var chunk, seq uint64
	readExtents := func(opts ...clientv3.OpOption) error {
		var xerr error
		existing, xerr = s.store.GetExtents(ctx, ino, opts...)
		if xerr != nil {
			return xerr
		}
		chunk, seq = 0, 0
		for _, e := range existing {
			if e.Chunk >= chunk {
				chunk = e.Chunk + 1
			}
			if e.Seq >= seq {
				seq = e.Seq + 1
			}
		}
		return nil
	}

	// One extent per run, in order, so the logical range stays contiguous even
	// though the device ranges behind it are not.  Rebuilt on a retry, since
	// both counters move.
	//
	// Everything this write does to metadata goes into this one transaction:
	// the new extents, the size change, the rewrite of every extent the write
	// buries, and the release of the inode lock.  Each of those used to be its
	// own round trip after the commit, and each was a Raft commit on the
	// critical path of every write.  Folding them also makes the write atomic
	// in a way it was not: a buried extent stops being referenced at the same
	// revision the extent burying it appears, and the lock is dropped exactly
	// when the work it protected becomes visible.
	end := offset
	var plans []*reclaimPlan
	var deferredReclaim []metadata.Extent
	var nextChunk uint64
	var foldedRelease bool
	proposal := func() ([]clientv3.Cmp, []clientv3.Op) {
		cmps := make([]clientv3.Cmp, 0, len(runs))
		ops := make([]clientv3.Op, 0, len(runs)+2)
		plans = plans[:0]
		deferredReclaim = deferredReclaim[:0]
		end = offset
		pos := uint64(0)
		next := chunk
		for _, r := range runs {
			// The final run is padded; its extent covers only the real bytes.
			extLen := min(r.Length, uint64(dataLen)-pos)
			// Every run of one write shares a sequence: they are one write, and
			// they cover disjoint logical ranges, so nothing needs to order them
			// against each other.
			ext := metadata.Extent{
				LogOff: offset + pos, DiskOff: r.DiskOff, Length: extLen,
				Gen: gen, Seq: seq, Node: s.store.NodeID(),
			}
			key := metadata.ExtentKey(ino, next)
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
			ops = append(ops, clientv3.OpPut(key, ext.Encode()))
			next++
			end = ext.End()
			pos += r.Length
		}
		// Data is already durable on the device; if the guard rejects the
		// commit the bytes stay unreferenced and the blocks go back to the
		// arena.
		if end > rec.Size {
			sized := *rec
			sized.Size = end
			ops = append(ops, clientv3.OpPut(metadata.InodeKey(ino), string(metadata.EncodeInode(&sized))))
		}

		// The write is about to bury these, so they stop being readable through
		// the extents that held them.  Reclaiming here rather than leaving it
		// all to the scrubber keeps an overwrite-heavy workload from holding a
		// scrub interval's worth of buried blocks alive at all times.
		for _, old := range existing {
			if old.LogOff >= end || offset >= old.End() {
				continue
			}
			// etcd caps a transaction's size, and one write can bury many
			// extents.  What does not fit is reclaimed afterwards, in the round
			// trips of its own this folding exists to avoid — correct either
			// way, just not free.
			if len(ops)+maxReclaimOps+1 > maxWriteTxnOps || len(cmps)+1 > maxWriteTxnOps {
				deferredReclaim = append(deferredReclaim, old)
				continue
			}
			if p := s.planReclaim(old, offset, end, &next); p != nil {
				cmps = append(cmps, p.cmp)
				ops = append(ops, p.ops...)
				plans = append(plans, p)
			}
		}

		// Only when this transaction finishes the write: a reclaim left over
		// for afterwards still needs the inode held, or another node could
		// rewrite those extents between the commit and the reclaim and turn a
		// leak into a lost update.
		foldedRelease = len(deferredReclaim) == 0
		if foldedRelease {
			ops = append(ops, lk.ReleaseOp())
		}
		nextChunk = next
		return cmps, ops
	}

	if xerr := readExtents(clientv3.WithSerializable()); xerr != nil {
		freeRuns()
		s.log.Warn("write: cannot read existing extents", "ino", ino, "error", xerr)
		return int32Resp(-5), nil
	}
	cmps, ops := proposal()
	committed, fenced, cerr := s.commitGuarded(ctx, cmps, ops)
	if cerr == nil && !committed && !fenced {
		// The serializable read missed a chunk that exists, so this node was
		// talking to a member that had not caught up.  Re-read from the leader
		// and propose again.
		s.log.Debug("write: chunk numbering was stale, retrying linearizably", "ino", ino)
		if xerr := readExtents(); xerr != nil {
			freeRuns()
			s.log.Warn("write: cannot read existing extents", "ino", ino, "error", xerr)
			return int32Resp(-5), nil
		}
		cmps, ops = proposal()
		committed, fenced, cerr = s.commitGuarded(ctx, cmps, ops)
	}
	if cerr != nil {
		freeRuns()
		s.log.Warn("write: metadata commit failed", "ino", ino, "error", cerr)
		return int32Resp(-5), nil
	}
	if fenced {
		freeRuns()
		s.log.Error("write: rejected, node has been fenced",
			"ino", ino, "start_generation", s.startGen)
		return int32Resp(-5), nil
	}
	if !committed {
		freeRuns()
		s.log.Error("write: extent numbering still contended after a fresh read", "ino", ino)
		return int32Resp(-5), nil
	}
	rec.Size = max(rec.Size, end)
	if foldedRelease {
		lk.Folded()
	}

	// Only after the commit — before it, a transaction the generation guard
	// rejects would have freed blocks the file still refers to.
	for _, p := range plans {
		s.freeReclaimed(p)
	}

	// What did not fit in the transaction, in a round trip each.  The write
	// itself is published and correct; failing to reclaim what it buried only
	// leaks blocks the scrubber will find.
	for _, old := range deferredReclaim {
		if rerr := s.reclaimCovered(ctx, old, offset, end, &nextChunk); rerr != nil {
			s.log.Warn("buried extent not reclaimed, the scrubber will pick it up",
				"ino", ino, "error", rerr)
		}
	}

	return writtenResp(uint32(dataLen)), nil
}

// writeRun puts one run on the device.
//
// With O_DIRECT on a device that acknowledges a write only once it is durable —
// an EBS io2 Multi-Attach volume — the pwrite is the whole publication: no cache
// on this node holds the bytes, so no barrier can make another attacher see them
// any sooner.  The three extra round trips that used to follow every write are
// therefore behind writeBarriers, for a device with a volatile write cache and
// for buffered mode, where the page cache genuinely does hold the bytes back.
//
// The barrier readback is not a verification — the bytes are discarded.  It is
// the round trip itself that matters, so one sector establishes it as well as
// the whole buffer does.
func (s *Service) writeRun(buf []byte, diskOff uint64) error {
	n, err := s.dev.WriteAt(buf, int64(diskOff))
	if err != nil {
		return err
	}
	if !s.writeBarriers {
		return nil
	}

	_ = s.dev.FlushDevice()
	if err := s.dev.SyncRange(int64(diskOff), int64(n)); err != nil {
		s.log.Warn("write: sync failed", "error", err)
	}

	readback, freeReadback := s.ioBuffer(min(len(buf), s.dev.SectorSize()))
	_, _ = s.dev.ReadAt(readback, int64(diskOff))
	freeReadback()
	return nil
}

// allocateBlocks reserves device space, expanding into a fresh arena if the
// arenas this node already holds cannot satisfy the request.
func (s *Service) allocateBlocks(ctx context.Context, size uint64) ([]arena.Run, error) {
	if s.alloc.ArenaCount() > 0 {
		if runs, err := s.alloc.Allocate(size); err == nil {
			return runs, nil
		}
	}
	if _, err := s.alloc.AcquireArena(ctx); err != nil {
		return nil, err
	}
	return s.alloc.Allocate(size)
}

// writeGeneration returns the fencing generation to stamp on an extent: this
// node's own, which is 0 until it is first fenced.
//
// It used to floor the value at 1, so that a missing stamp stayed
// distinguishable from a real generation.  That is no longer a distinction
// worth making — every field of an extent value is required, so a missing stamp
// is a decode failure rather than a zero — and the floor put every extent
// written by a never-fenced node one generation ahead of the node itself, which
// the scrubber correctly reports as an extent from the future.
func (s *Service) writeGeneration(ctx context.Context) uint64 {
	gen, _ := s.store.GetMyGeneration(ctx)
	return gen
}

// READ payload: [u64:ino][u64:offset][u32:size]
// Response: [i32:error][u32:data_len][data_bytes]
func (s *Service) handleRead(ctx context.Context, payload []byte) ([]byte, error) {
	r := newReader(payload)
	ino, offset, size := r.u64(), r.u64(), r.u32()
	if !r.ok {
		return int32Resp(-22), nil
	}

	s.log.Debug("READ", "ino", ino, "offset", offset, "size", size)

	if s.dev == nil {
		return int32Resp(-5), nil
	}

	// Clamp to the file's size before anything else.  A read is answered with
	// the whole requested range, holes included, so a request reaching past the
	// end would otherwise come back as a full buffer of zeroes instead of a
	// short read — and a reader that never sees a short read never sees EOF.
	// The kernel usually clamps for us, from the size it last cached; it does
	// not always, and "usually" is not what a read loop terminates on.
	rec, err := s.store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		return int32Resp(-2), nil
	}
	if offset >= rec.Size {
		return dataResp(nil), nil
	}
	if remaining := rec.Size - offset; uint64(size) > remaining {
		size = uint32(remaining)
	}

	// A shared lock keeps a concurrent writer off the range while it is read.
	// Best effort: a read that cannot take the lock still returns data, since
	// the extent list it works from is itself a consistent etcd snapshot.
	if lk, err := s.lockInode(ctx, ino, metadata.LockShared); err == nil {
		defer lk.Release()
	}

	// Only useful against a cache this read could otherwise be served from: an
	// O_DIRECT read consults none, so the ioctl would cost a device round trip
	// per read to invalidate nothing.
	if s.writeBarriers {
		_ = s.dev.FlushDevice()
	}

	var extents []metadata.Extent
	s.retryKV(ctx, func(ictx context.Context) error {
		var gerr error
		extents, gerr = s.store.GetExtents(ictx, ino)
		return gerr
	})
	s.log.Debug("READ extents", "ino", ino, "count", len(extents))

	// The buffer starts zeroed, so a hole costs nothing: only the ranges an
	// extent actually covers are filled in, and the rest reads back as the
	// zeroes a sparse file is supposed to return.
	data, free := s.ioBuffer(int(size))
	defer free()
	data = data[:size]

	// Extents arrive ordered by logical offset, newest first where two cover
	// the same one, so walking forward and taking the first extent that reaches
	// the cursor resolves an overwrite to the later write.
	reqEnd := offset + uint64(size)
	pos := offset
	for _, ext := range extents {
		if pos >= reqEnd || ext.LogOff >= reqEnd {
			break
		}
		if ext.End() <= pos {
			continue // already behind the cursor, or buried by a newer extent
		}
		if ext.LogOff > pos {
			pos = ext.LogOff // hole: the buffer is already zero across it
		}

		within := pos - ext.LogOff
		n := min(ext.Length-within, reqEnd-pos)
		if _, err := s.readInto(data[pos-offset:pos-offset+n], int64(ext.DiskOff+within)); err != nil {
			s.log.Warn("read: block device read failed", "ino", ino, "error", err)
			return int32Resp(-5), nil
		}
		pos += n
	}

	// The whole requested range is returned, holes included — clamped to the
	// file's size above, so a hole reads back as zeroes and the end of the file
	// reads back short.
	return dataResp(data), nil
}

// readInto fills dst from the device at diskOff, bouncing through an aligned
// buffer when dst itself cannot be the target of an O_DIRECT read.
func (s *Service) readInto(dst []byte, diskOff int64) (int, error) {
	if s.directSafe(dst) {
		return s.dev.ReadAt(dst, diskOff)
	}

	bounce, free := s.ioBuffer(len(dst))
	defer free()

	if _, err := s.dev.ReadAt(bounce, diskOff); err != nil {
		return 0, err
	}
	return copy(dst, bounce), nil
}

// truncate reclaims every extent of an inode that lies beyond newSize.
//
// A truncate is the same operation as an overwrite of everything from newSize
// to the end of the address space, so it goes through the same path.
// truncate releases what a shrink leaves past newSize, reporting a failure
// rather than swallowing it: a fenced node's writes are rejected by the
// generation guard, and a truncate that answered success while every extent
// stayed in place told the caller its file had shrunk when nothing had.
// Unlike a write, a truncate takes no inode lock, so an extent it planned to
// rewrite can be rewritten under it by another node.  Each reclaim refuses to
// apply in that case; the whole pass is retried from a fresh read, which is the
// only view a correct plan can be built from.
func (s *Service) truncate(ctx context.Context, ino uint64, newSize uint64) error {
	return retry(ctx, etcdAttempts, func() error {
		return s.truncateOnce(ctx, ino, newSize)
	})
}

// truncateToZero empties a regular file and publishes the new size, with the
// mtime/ctime update POSIX requires of a truncating open.  Opening a directory
// or a device node with O_TRUNC changes nothing, and neither does opening a
// file that is already empty.
func (s *Service) truncateToZero(ctx context.Context, ino uint64) error {
	rec, rev, err := s.store.GetInodeRev(ctx, ino)
	if err != nil {
		return fmt.Errorf("open truncate ino %d: %w", ino, err)
	}
	if rec == nil {
		return metadata.ErrNotFound
	}
	if rec.Mode&metadata.S_IFMT != metadata.ModeFile || rec.Size == 0 {
		return nil
	}

	if terr := s.truncate(ctx, ino, 0); terr != nil {
		return terr
	}

	rec.Size = 0
	rec.Mtime = time.Now()
	rec.Ctime = rec.Mtime
	ok, err := s.store.Txn(ctx,
		[]clientv3.Cmp{metadata.InodeUnchanged(ino, rev)},
		[]clientv3.Op{clientv3.OpPut(metadata.InodeKey(ino), string(metadata.EncodeInode(rec)))}, nil)
	if err != nil {
		return fmt.Errorf("open truncate ino %d: %w", ino, err)
	}
	if !ok {
		return fmt.Errorf("open truncate ino %d: %w", ino, metadata.ErrConflict)
	}
	return nil
}

func (s *Service) truncateOnce(ctx context.Context, ino uint64, newSize uint64) error {
	extents, err := s.store.GetExtents(ctx, ino)
	if err != nil {
		return fmt.Errorf("truncate ino %d: read extents: %w", ino, err)
	}
	chunk := uint64(0)
	for _, e := range extents {
		if e.Chunk >= chunk {
			chunk = e.Chunk + 1
		}
	}
	for _, ext := range extents {
		if ext.End() > newSize {
			if rerr := s.reclaimCovered(ctx, ext, newSize, ^uint64(0), &chunk); rerr != nil {
				return fmt.Errorf("truncate ino %d: %w", ino, rerr)
			}
		}
	}
	return nil
}

// errExtentChanged reports that an extent this node meant to rewrite was
// modified after it was read, so the rewrite was refused rather than applied
// from a stale view.  Retryable: the caller re-reads and plans again.
var errExtentChanged = errors.New("extent changed since it was read")

// reclaimPlan is the metadata rewrite that gives back what a write buried,
// together with the device range that becomes free once that rewrite commits.
//
// It is a plan rather than an action so the write path can fold it into the
// transaction publishing the write itself — one round trip for both, and the
// buried extent stops being referenced at the same revision the extent burying
// it appears.
type reclaimPlan struct {
	// cmp requires the record to be untouched since it was read.  Without it a
	// stale read could resurrect an extent that was deleted in between, since
	// the rewrite is a blind put of a value derived from that read.
	cmp     clientv3.Cmp
	ops     []clientv3.Op
	freeOff uint64
	freeLen uint64
}

// planReclaim builds the rewrite of old into the part a write over
// [start, end) leaves readable.  Returns nil when there is nothing to do.
//
// It is a no-op for a range this node does not own.  The extent record is the
// only durable reference the owning node's in-memory bitmap is rebuilt from, so
// rewriting it from here would strand those blocks as allocated on that node
// until it restarted — the record has to outlive this call for its owner to
// find it.  The owner's scrubber reclaims it instead, at the cost of a scrub
// interval's delay.
//
// A write landing strictly inside old leaves two readable pieces, so the second
// one is written under a fresh key from nextChunk, which is advanced.  Both
// pieces carry old's sequence number, which is what keeps them as old as the
// extent they were cut from: the key says only which record this is, never how
// recent it is, so neither piece can outrank a genuinely newer extent
// overlapping it.
func (s *Service) planReclaim(old metadata.Extent, start, end uint64, nextChunk *uint64) *reclaimPlan {
	if s.dev == nil || !s.alloc.Owns(old.DiskOff) {
		return nil
	}

	head, tail := metadata.SplitAround(old, start, end)

	// The write covered less than a whole block at the front of old, so the
	// split snapped back to where it began and there is nothing to give back.
	if head == nil && tail != nil && tail.LogOff == old.LogOff {
		return nil
	}

	p := &reclaimPlan{cmp: clientv3.Compare(clientv3.ModRevision(old.Key), "=", old.ModRevision)}

	switch {
	case head == nil && tail == nil:
		p.ops = []clientv3.Op{clientv3.OpDelete(old.Key)}

	case head != nil && tail != nil:
		// Both pieces in one transaction: applying only one would leave the
		// middle described twice, once by each half of a record that no longer
		// exists.
		p.ops = []clientv3.Op{
			clientv3.OpPut(old.Key, head.Encode()),
			clientv3.OpPut(metadata.ExtentKey(old.Ino(), *nextChunk), tail.Encode()),
		}
		*nextChunk++

	default:
		survivor := head
		if survivor == nil {
			survivor = tail
		}
		p.ops = []clientv3.Op{clientv3.OpPut(old.Key, survivor.Encode())}
	}

	p.freeOff, p.freeLen = metadata.CoveredBlocks(old, head, tail)
	return p
}

// reclaimCovered applies a plan on its own, for callers with no transaction of
// their own to fold it into.
//
// Metadata first, then the free: a reader resolving the old record must never
// be sent to blocks that have already been handed to another allocation.
func (s *Service) reclaimCovered(ctx context.Context, old metadata.Extent,
	start, end uint64, nextChunk *uint64) error {

	p := s.planReclaim(old, start, end, nextChunk)
	if p == nil {
		return nil
	}

	ok, err := s.store.Txn(ctx, []clientv3.Cmp{p.cmp}, p.ops, nil)
	if err != nil {
		return fmt.Errorf("reclaim covered extent %s: %w", old.Key, err)
	}
	if !ok {
		// The guard is checked by the store, which reports a fence as an error
		// rather than a rejection — so reaching here means the record itself
		// moved, not that this node was fenced.
		return fmt.Errorf("reclaim covered extent %s: %w", old.Key, errExtentChanged)
	}

	s.freeReclaimed(p)
	return nil
}

// freeReclaimed returns a committed plan's blocks to the arena.
func (s *Service) freeReclaimed(p *reclaimPlan) {
	if p.freeLen > 0 {
		s.alloc.Free(p.freeOff, p.freeLen)
	}
}
