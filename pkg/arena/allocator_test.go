package arena

import (
	"context"
	"errors"
	"testing"
)

// fullArena returns an arena with every block marked allocated, so a test can
// carve out exactly the holes it wants to reason about.
func fullArena(id uint64) *Arena {
	ar := &Arena{
		ID:        id,
		DiskStart: id * ArenaSizeBytes,
		DiskEnd:   (id + 1) * ArenaSizeBytes,
		bitmap:    make([]uint64, BlocksPerArena/64),
	}
	for i := range ar.bitmap {
		ar.bitmap[i] = ^uint64(0)
	}
	return ar
}

func freeBlocks(ar *Arena, blocks ...uint64) {
	for _, b := range blocks {
		ar.markFree(b)
	}
}

func testAllocator(arenas ...*Arena) *Allocator {
	return &Allocator{nodeID: "test", arenas: arenas}
}

// A request larger than any single hole must be satisfied from several, not
// rejected.  Rejecting it is what used to push the write onto a fresh arena and
// grow the device for space that was already free.
func TestAllocateSpansSeveralHoles(t *testing.T) {
	ar := fullArena(0)
	freeBlocks(ar, 10, 11, 100, 101, 102)
	a := testAllocator(ar)

	runs, err := a.Allocate(5 * BlockSize)
	if err != nil {
		t.Fatalf("allocate across holes: %v", err)
	}

	want := []Run{
		{DiskOff: 10 * BlockSize, Length: 2 * BlockSize},
		{DiskOff: 100 * BlockSize, Length: 3 * BlockSize},
	}
	if len(runs) != len(want) {
		t.Fatalf("got %d runs, want %d: %+v", len(runs), len(want), runs)
	}
	for i, w := range want {
		if runs[i] != w {
			t.Errorf("run %d = %+v, want %+v", i, runs[i], w)
		}
	}

	if got := ar.countAllocated(); got != BlocksPerArena {
		t.Errorf("arena has %d allocated blocks, want all %d", got, BlocksPerArena)
	}
}

// A request the arenas cannot cover must leave the bitmap untouched.  Keeping
// the partial reservation would leak every block taken on the way to failing.
func TestAllocateUndoesPartialReservationOnFailure(t *testing.T) {
	ar := fullArena(0)
	freeBlocks(ar, 7, 8)
	a := testAllocator(ar)

	if _, err := a.Allocate(3 * BlockSize); err == nil {
		t.Fatal("allocate succeeded with too few free blocks")
	}

	for _, b := range []uint64{7, 8} {
		if !ar.isFree(b) {
			t.Errorf("block %d stayed allocated after the failed request", b)
		}
	}
}

// Allocation continues into the next arena when the first cannot finish it.
func TestAllocateContinuesIntoNextArena(t *testing.T) {
	first, second := fullArena(0), fullArena(1)
	freeBlocks(first, 5)
	freeBlocks(second, 0)
	a := testAllocator(first, second)

	runs, err := a.Allocate(2 * BlockSize)
	if err != nil {
		t.Fatalf("allocate across arenas: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("got %d runs, want 2: %+v", len(runs), runs)
	}
	if runs[0].DiskOff != 5*BlockSize {
		t.Errorf("first run at %d, want %d", runs[0].DiskOff, 5*BlockSize)
	}
	if runs[1].DiskOff != ArenaSizeBytes {
		t.Errorf("second run at %d, want %d", runs[1].DiskOff, uint64(ArenaSizeBytes))
	}
}

// Freeing everything an arena holds is what makes it eligible for release, so
// the emptiness test has to agree with Free.
func TestArenaIsEmptyAfterFreeingEverything(t *testing.T) {
	ar := fullArena(0)
	freeBlocks(ar, 3, 4)
	a := testAllocator(ar)

	runs, err := a.Allocate(2 * BlockSize)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if ar.isEmpty() {
		t.Fatal("arena reported empty while blocks were allocated")
	}

	// Release the two just taken and the rest of the arena with them.
	for i := range ar.bitmap {
		ar.bitmap[i] = 0
	}
	for _, r := range runs {
		a.Free(r.DiskOff, r.Length)
	}
	if !ar.isEmpty() {
		t.Fatal("arena not reported empty after every block was freed")
	}
}

// Owns is what stops one node's scrubber deleting an extent record another
// node's bitmap is rebuilt from.
func TestOwnsCoversOnlyHeldArenas(t *testing.T) {
	a := testAllocator(fullArena(1))

	if !a.Owns(ArenaSizeBytes) {
		t.Error("first byte of a held arena reported as not owned")
	}
	if !a.Owns(2*ArenaSizeBytes - 1) {
		t.Error("last byte of a held arena reported as not owned")
	}
	if a.Owns(0) {
		t.Error("a byte in another arena reported as owned")
	}
	if a.Owns(2 * ArenaSizeBytes) {
		t.Error("first byte past the held arena reported as owned")
	}
}

// Arena IDs are handed out by a counter and turned into offsets by multiplying
// by the arena size, with nothing else to stop them running past the end of the
// device. A write there fails at the pwrite with a short write or EINVAL, which
// surfaces as EIO — a disk error — rather than as a full filesystem.
func TestAcquireArenaRefusesToRunPastTheDevice(t *testing.T) {
	a := NewAllocator("node-A", nil)
	a.SetDeviceSize(ArenaSizeBytes / 2)

	_, err := a.AcquireArena(context.Background())
	if !errors.Is(err, ErrNoSpace) {
		t.Fatalf("arena past the end of the device: err = %v, want ErrNoSpace", err)
	}
}

// statfs derives free space from this, so answering 1.0 with nothing held made
// df report the filesystem full before the first write took an arena.
func TestLiveRatioIsZeroWithNoArenas(t *testing.T) {
	if got := NewAllocator("node-A", nil).LiveRatio(); got != 0 {
		t.Errorf("LiveRatio with no arenas = %v, want 0", got)
	}
}

// Freeing a block that is already free means two callers believe they own it,
// and the next allocation hands a live range to a second writer. The write
// path, the scrubber and the failed-allocation undo all reach this.
func TestFreeCountsDoubleFrees(t *testing.T) {
	a := NewAllocator("node-A", nil)
	ar := fullArena(0)
	a.arenas = append(a.arenas, ar)

	a.Free(ar.DiskStart, BlockSize)
	if got := a.DoubleFrees(); got != 0 {
		t.Fatalf("freeing an allocated block counted %d double frees", got)
	}
	a.Free(ar.DiskStart, BlockSize)
	if got := a.DoubleFrees(); got != 1 {
		t.Errorf("freeing the same block twice counted %d double frees, want 1", got)
	}
}

// Allocation used to restart its scan at block 0 every time, so a nearly-full
// arena cost a sweep of the whole bitmap per call. The hint carries the search
// forward, and wraps so nothing is missed.
func TestAllocationResumesFromWhereItLeftOff(t *testing.T) {
	ar := fullArena(0)
	for i := uint64(0); i < 4; i++ {
		ar.markFree(i)
	}
	ar.markFree(BlocksPerArena - 1)

	// Each run is marked allocated as the allocator itself would.
	take := func(max uint64) (uint64, uint64) {
		start, length := ar.findRun(max)
		for i := uint64(0); i < length; i++ {
			ar.markAllocated(start + i)
		}
		return start, length
	}

	if start, length := take(2); start != 0 || length != 2 {
		t.Fatalf("first run = (%d, %d), want (0, 2)", start, length)
	}
	if start, length := take(2); start != 2 || length != 2 {
		t.Fatalf("second run = (%d, %d), want (2, 2)", start, length)
	}
	if start, length := take(1); start != BlocksPerArena-1 || length != 1 {
		t.Fatalf("third run = (%d, %d), want the last block", start, length)
	}
	// Past the end, the search wraps rather than reporting the arena full.
	ar.markFree(1)
	if start, length := take(1); start != 1 || length != 1 {
		t.Fatalf("wrapped run = (%d, %d), want (1, 1)", start, length)
	}
}
