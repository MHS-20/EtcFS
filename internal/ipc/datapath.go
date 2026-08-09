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
	if len(payload) < 20 {
		return int32Resp(-22), nil
	}
	ino, rest := readU64(payload)
	offset, rest := readU64(rest)
	dataLen, rest := readU32(rest)
	data := rest[:dataLen]

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

	// One read of the inode's extents answers both questions this write has:
	// which chunk number is free, and which existing extents it is about to
	// bury.  The chunk must be one past the highest in use, not the extent
	// count — a truncate deletes chunks from the middle, and counting would
	// hand back a number that is still live.
	existing, xerr := s.store.GetExtents(ctx, ino)
	if xerr != nil {
		freeRuns()
		s.log.Warn("write: cannot read existing extents", "ino", ino, "error", xerr)
		return int32Resp(-5), nil
	}
	chunk := uint64(0)
	for _, e := range existing {
		if e.Chunk >= chunk {
			chunk = e.Chunk + 1
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
		ext := metadata.Extent{
			LogOff: offset + pos, DiskOff: r.DiskOff, Length: extLen, Gen: gen,
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

	// The write is published, so anything it fully covers is now dead.  Reclaim
	// it here rather than leaving it all to the scrubber: an overwrite-heavy
	// workload on one node would otherwise keep a scrub interval's worth of
	// superseded blocks alive at all times.  Only after the commit — before it,
	// a rejected transaction would have freed blocks the file still refers to.
	newest := metadata.Extent{Chunk: chunk, LogOff: offset, Length: uint64(dataLen)}
	for _, old := range existing {
		if newest.Supersedes(old) {
			s.dropExtent(ctx, old)
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
	if len(payload) < 20 {
		return int32Resp(-22), nil
	}
	ino, rest := readU64(payload)
	offset, rest := readU64(rest)
	size, _ := readU32(rest)

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
	if len(extents) == 0 {
		return dataResp(nil), nil
	}

	data, free := s.ioBuffer(int(size))
	defer free()
	data = data[:size]

	bytesRead := uint32(0)
	rem := size

	for _, ext := range extents {
		eStart, eEnd := ext.LogOff, ext.End()

		// Sparse region before this extent: leave the buffer zeroed and skip
		// past the hole.
		if offset >= eEnd || offset+uint64(rem) <= eStart {
			if offset < eStart && offset+uint64(rem) > eStart {
				gapLen := min(eStart-offset, uint64(rem))
				bytesRead += uint32(gapLen)
				rem -= uint32(gapLen)
				offset += gapLen
			}
			continue
		}

		readStart := uint64(0)
		if offset > eStart {
			readStart = offset - eStart
		}
		readLen := min(ext.Length-readStart, uint64(rem))

		dst := data[bytesRead : bytesRead+uint32(readLen)]
		n, err := s.readInto(dst, int64(ext.DiskOff+readStart))
		if err != nil {
			break
		}

		bytesRead += uint32(n)
		rem -= uint32(readLen)
		if rem == 0 {
			break
		}
		offset = eStart + readLen
	}

	return dataResp(data[:bytesRead]), nil
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

// truncate drops or shortens every extent of an inode that lies beyond
// newSize, returning the freed device ranges to the arena.
//
// Only the extents whose device range this node owns are touched — see
// dropExtent.  The rest are left exactly as they are, and the inode's smaller
// size is what keeps them from being read: the kernel clamps a read to the
// size it last saw, so bytes past EOF are unreachable whether or not an extent
// still describes them.  Their owner's scrubber reclaims them.
func (s *Service) truncate(ctx context.Context, ino uint64, newSize uint64) {
	extents, err := s.store.GetExtents(ctx, ino)
	if err != nil {
		s.log.Warn("truncate: cannot read extents", "ino", ino, "error", err)
		return
	}
	for _, ext := range extents {
		if ext.End() <= newSize {
			continue
		}
		if ext.LogOff >= newSize {
			s.dropExtent(ctx, ext)
			continue
		}
		s.shortenExtent(ctx, ext, newSize-ext.LogOff)
	}
}

// dropExtent removes an extent record and returns its blocks.
//
// It is a no-op for a range this node does not own.  The extent record is the
// only durable reference the owning node's in-memory bitmap is rebuilt from, so
// deleting it from here would strand those blocks as allocated on that node
// until it restarted — the record has to outlive this call for its owner to
// find it.  The owner's scrubber reclaims it instead, at the cost of a scrub
// interval's delay.
func (s *Service) dropExtent(ctx context.Context, ext metadata.Extent) {
	if s.dev == nil || !s.alloc.Owns(ext.DiskOff) {
		return
	}
	if err := s.store.Delete(ctx, ext.Key); err != nil {
		s.log.Warn("cannot delete extent, blocks not reclaimed",
			"key", ext.Key, "error", err)
		return
	}
	s.alloc.Free(ext.DiskOff, ext.Length)
}

// shortenExtent trims an extent to keepLen bytes and returns the tail.
// Owner-only, for the same reason as dropExtent.
func (s *Service) shortenExtent(ctx context.Context, ext metadata.Extent, keepLen uint64) {
	if s.dev == nil || !s.alloc.Owns(ext.DiskOff) {
		return
	}
	shortened := ext
	shortened.Length = keepLen
	if _, err := s.store.Put(ctx, ext.Key, []byte(shortened.Encode())); err != nil {
		s.log.Warn("cannot shorten extent, tail not reclaimed",
			"key", ext.Key, "error", err)
		return
	}
	s.alloc.Free(ext.DiskOff+keepLen, ext.Length-keepLen)
}
