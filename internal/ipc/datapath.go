package ipc

import (
	"context"
	"unsafe"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/arena"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	wal "github.com/MHS-20/EtcFS/pkg/walgo"
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

	release, err := s.lockInode(ctx, ino, metadata.LockExclusive)
	if err != nil {
		return int32Resp(-11), nil // EAGAIN after retries
	}
	defer release()

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

	// One read of the inode's extents answers every question this write has:
	// which chunk numbers are free, what sequence it takes, and which existing
	// extents it is about to bury.  Both counters run one past the highest in
	// use rather than off the extent count — a truncate deletes records from
	// the middle, and counting would hand back a number that is still live.
	existing, xerr := s.store.GetExtents(ctx, ino)
	if xerr != nil {
		freeRuns()
		s.log.Warn("write: cannot read existing extents", "ino", ino, "error", xerr)
		return int32Resp(-5), nil
	}
	chunk, seq := uint64(0), uint64(0)
	for _, e := range existing {
		if e.Chunk >= chunk {
			chunk = e.Chunk + 1
		}
		if e.Seq >= seq {
			seq = e.Seq + 1
		}
	}

	// One extent per run, in order, so the logical range stays contiguous even
	// though the device ranges behind it are not.
	ops := make([]clientv3.Op, 0, len(runs)+1)
	end := offset
	pos := uint64(0)
	for _, r := range runs {
		if werr := s.writeRun(writeData[pos:pos+r.Length], r.DiskOff); werr != nil {
			freeRuns()
			s.log.Warn("write: block device write failed", "error", werr)
			return int32Resp(-5), nil
		}

		// The final run is padded; its extent covers only the real bytes.
		extLen := min(r.Length, uint64(dataLen)-pos)
		// Every run of one write shares a sequence: they are one write, and
		// they cover disjoint logical ranges, so nothing needs to order them
		// against each other.
		ext := metadata.Extent{
			LogOff: offset + pos, DiskOff: r.DiskOff, Length: extLen,
			Gen: gen, Seq: seq, Node: s.store.NodeID(),
		}
		if s.wal != nil {
			_ = s.wal.Append(&wal.Entry{
				Ino:        ino,
				LogicalOff: ext.LogOff,
				DiskOff:    r.DiskOff,
				Length:     extLen,
				Generation: gen,
			})
		}
		ops = append(ops, clientv3.OpPut(metadata.ExtentKey(ino, chunk), ext.Encode()))
		chunk++
		end = ext.End()
		pos += r.Length
	}

	// Commit every extent and any size change together, guarded by this node's
	// fencing generation.  Data is already durable on the device; if the guard
	// rejects the commit the bytes stay unreferenced and the blocks go back to
	// the arena.
	if end > rec.Size {
		rec.Size = end
		ops = append(ops, clientv3.OpPut(metadata.InodeKey(ino), string(metadata.EncodeInode(rec))))
	}

	committed, cerr := s.commitGuarded(ops)
	if cerr != nil {
		freeRuns()
		s.log.Warn("write: metadata commit failed", "ino", ino, "error", cerr)
		return int32Resp(-5), nil
	}
	if !committed {
		freeRuns()
		s.log.Error("write: rejected, node has been fenced",
			"ino", ino, "start_generation", s.startGen)
		return int32Resp(-5), nil
	}

	if s.wal != nil {
		_ = s.wal.MarkCommitted(ino, offset)
	}

	// The write is published, so whatever it covers is now unreadable through
	// the extents that held it.  Reclaim here rather than leaving it all to the
	// scrubber: an overwrite-heavy workload on one node would otherwise keep a
	// scrub interval's worth of buried blocks alive at all times.
	//
	// Only after the commit — before it, a transaction the generation guard
	// rejects would have freed blocks the file still refers to.
	for _, old := range existing {
		if old.LogOff < end && offset < old.End() {
			s.reclaimCovered(ctx, old, offset, end, &chunk)
		}
	}

	return writtenResp(uint32(dataLen)), nil
}

// writeRun puts one run on the device and makes it visible to the volume's
// other attachers.
//
// The readback is not a verification — the bytes are discarded.  It is the
// round trip that publishes the write to the other attachers of an EBS
// Multi-Attach volume.
func (s *Service) writeRun(buf []byte, diskOff uint64) error {
	n, err := s.dev.WriteAt(buf, int64(diskOff))
	if err != nil {
		return err
	}

	_ = s.dev.FlushDevice()
	if err := s.dev.SyncRange(int64(diskOff), int64(n)); err != nil {
		s.log.Warn("write: sync failed", "error", err)
	}

	readback, freeReadback := s.ioBuffer(len(buf))
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

// writeGeneration returns the fencing generation to stamp on an extent.
// Generation 0 means "never fenced"; extents are stamped from 1 so that a
// missing stamp stays distinguishable from a genuine generation.
func (s *Service) writeGeneration(ctx context.Context) uint64 {
	gen, _ := s.store.GetMyGeneration(ctx)
	if gen < 1 {
		return 1
	}
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

	// A shared lock keeps a concurrent writer off the range while it is read.
	// Best effort: a read that cannot take the lock still returns data, since
	// the extent list it works from is itself a consistent etcd snapshot.
	if release, err := s.lockInode(ctx, ino, metadata.LockShared); err == nil {
		defer release()
	}

	_ = s.dev.FlushDevice()

	var extents []metadata.Extent
	s.retryKV(func(ictx context.Context) error {
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

	// The whole requested range is returned, holes included.  The kernel has
	// already clamped the request to the size it last saw, so there is nothing
	// past the end of the file in it to over-report.
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
func (s *Service) truncate(ctx context.Context, ino uint64, newSize uint64) {
	extents, err := s.store.GetExtents(ctx, ino)
	if err != nil {
		s.log.Warn("truncate: cannot read extents", "ino", ino, "error", err)
		return
	}
	chunk := uint64(0)
	for _, e := range extents {
		if e.Chunk >= chunk {
			chunk = e.Chunk + 1
		}
	}
	for _, ext := range extents {
		if ext.End() > newSize {
			s.reclaimCovered(ctx, ext, newSize, ^uint64(0), &chunk)
		}
	}
}

// reclaimCovered rewrites old to the part a write over [start, end) leaves
// readable, and returns the blocks it gives up to the arena.
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
//
// Metadata first, then the free: a reader resolving the old record must never
// be sent to blocks that have already been handed to another allocation.
func (s *Service) reclaimCovered(ctx context.Context, old metadata.Extent,
	start, end uint64, nextChunk *uint64) {

	if s.dev == nil || !s.alloc.Owns(old.DiskOff) {
		return
	}

	head, tail := metadata.SplitAround(old, start, end)

	// The write covered less than a whole block at the front of old, so the
	// split snapped back to where it began and there is nothing to give back.
	if head == nil && tail != nil && tail.LogOff == old.LogOff {
		return
	}

	switch {
	case head == nil && tail == nil:
		if err := s.store.Delete(ctx, old.Key); err != nil {
			s.log.Warn("cannot delete covered extent, blocks not reclaimed",
				"key", old.Key, "error", err)
			return
		}

	case head != nil && tail != nil:
		// One transaction: a crash between the two puts would leave the middle
		// described twice, once by each half of a record that no longer exists.
		ops := []clientv3.Op{
			clientv3.OpPut(old.Key, head.Encode()),
			clientv3.OpPut(metadata.ExtentKey(old.Ino(), *nextChunk), tail.Encode()),
		}
		if ok, err := s.store.Txn(ctx, nil, ops, nil); err != nil || !ok {
			s.log.Warn("cannot split covered extent, blocks not reclaimed",
				"key", old.Key, "committed", ok, "error", err)
			return
		}
		*nextChunk++

	default:
		survivor := head
		if survivor == nil {
			survivor = tail
		}
		if _, err := s.store.Put(ctx, old.Key, []byte(survivor.Encode())); err != nil {
			s.log.Warn("cannot trim covered extent, blocks not reclaimed",
				"key", old.Key, "error", err)
			return
		}
	}

	if off, length := metadata.CoveredBlocks(old, head, tail); length > 0 {
		s.alloc.Free(off, length)
	}
}
