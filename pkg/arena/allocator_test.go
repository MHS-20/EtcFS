package arena

import "testing"

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
