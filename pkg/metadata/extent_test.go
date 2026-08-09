package metadata

import (
	"testing"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

func TestExtentRoundTrip(t *testing.T) {
	e := Extent{Key: ExtentKey(42, 7), Chunk: 7, LogOff: 4096, DiskOff: 1 << 30, Length: 512, Gen: 3}
	got, ok := DecodeExtent(e.Key, []byte(e.Encode()))
	if !ok || got != e {
		t.Fatalf("round trip: got %+v ok=%v, want %+v", got, ok, e)
	}
	if got.Ino() != 42 || got.End() != 4608 {
		t.Fatalf("derived fields: ino=%d end=%d", got.Ino(), got.End())
	}
}

func TestDecodeExtentRejectsGarbage(t *testing.T) {
	for _, v := range []string{"", "1,2,3", "1,2,3,4,5", "1,2,3,x", "-1,2,3,4"} {
		if _, ok := DecodeExtent("extent:1/0", []byte(v)); ok {
			t.Errorf("accepted malformed value %q", v)
		}
	}
}

func TestParseExtentKey(t *testing.T) {
	if ino, chunk, ok := ParseExtentKey("extent:9/12"); !ok || ino != 9 || chunk != 12 {
		t.Fatalf("got %d/%d ok=%v", ino, chunk, ok)
	}
	for _, k := range []string{"inode:9", "extent:9", "extent:x/1", "extent:9/y"} {
		if _, _, ok := ParseExtentKey(k); ok {
			t.Errorf("accepted malformed key %q", k)
		}
	}
}

// Extents must come back in logical order even though etcd returns keys
// lexicographically (chunk 10 sorts before chunk 2).
func TestDecodeExtentsSortsByLogicalOffset(t *testing.T) {
	kvs := []*mvccpb.KeyValue{
		{Key: []byte("extent:1/0"), Value: []byte("0,0,4096,1")},
		{Key: []byte("extent:1/1"), Value: []byte("4096,4096,4096,1")},
		{Key: []byte("extent:1/10"), Value: []byte("40960,40960,4096,1")},
		{Key: []byte("extent:1/2"), Value: []byte("8192,8192,4096,1")},
		{Key: []byte("extent:1/bad"), Value: []byte("nonsense")},
	}
	got := DecodeExtents(kvs)
	if len(got) != 4 {
		t.Fatalf("want 4 extents, got %d", len(got))
	}
	for i, want := range []uint64{0, 4096, 8192, 40960} {
		if got[i].LogOff != want {
			t.Fatalf("extent %d: log_off %d, want %d", i, got[i].LogOff, want)
		}
	}
}

// A write allocates fresh blocks and appends an extent, so overwriting a range
// leaves two extents covering it.  A reader takes the first one that covers the
// offset it wants, so the newer — higher chunk — has to sort first, and has to
// do so deterministically: sort.Slice is not stable, and ordering on offset
// alone let the same file read back differently from one call to the next.
func TestDecodeExtentsPutsTheNewestWriteFirst(t *testing.T) {
	kvs := []*mvccpb.KeyValue{
		{Key: []byte("extent:1/0"), Value: []byte("0,4096,4096,1")},
		{Key: []byte("extent:1/7"), Value: []byte("0,8192,4096,1")},
		{Key: []byte("extent:1/3"), Value: []byte("0,12288,4096,1")},
	}
	for i := 0; i < 32; i++ {
		got := DecodeExtents(kvs)
		if got[0].Chunk != 7 {
			t.Fatalf("chunk %d sorted ahead of the newer chunk 7", got[0].Chunk)
		}
		if got[0].DiskOff != 8192 {
			t.Fatalf("reader would land on disk_off %d, want 8192", got[0].DiskOff)
		}
	}
}

func TestSupersedes(t *testing.T) {
	at := func(chunk, logOff, length uint64) Extent {
		return Extent{Key: ExtentKey(1, chunk), Chunk: chunk, LogOff: logOff, Length: length}
	}

	cases := []struct {
		name       string
		newer, old Extent
		want       bool
	}{
		{"same range, later chunk", at(2, 0, 4096), at(1, 0, 4096), true},
		{"wider range, later chunk", at(2, 0, 8192), at(1, 4096, 4096), true},
		{"same range, earlier chunk", at(1, 0, 4096), at(2, 0, 4096), false},
		{"same chunk", at(1, 0, 4096), at(1, 0, 4096), false},
		{"covers only the head", at(2, 0, 4096), at(1, 0, 8192), false},
		{"covers only the tail", at(2, 4096, 4096), at(1, 0, 8192), false},
		{"disjoint", at(2, 8192, 4096), at(1, 0, 4096), false},
	}
	for _, c := range cases {
		if got := c.newer.Supersedes(c.old); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// SplitAround decides what survives an overwrite, and every case has a way to
// go wrong: dropping live bytes, or handing back blocks that still hold them.
func TestSplitAround(t *testing.T) {
	// One extent, four blocks, at a block-aligned device offset.
	old := Extent{
		Key: ExtentKey(1, 3), Chunk: 3,
		LogOff: 8192, DiskOff: 1 << 30, Length: 4 * BlockSize, Gen: 2,
	}

	type want struct {
		head, tail *Extent
	}
	at := func(logOff, diskOff, length uint64) *Extent {
		return &Extent{LogOff: logOff, DiskOff: diskOff, Length: length, Gen: old.Gen}
	}

	cases := []struct {
		name       string
		start, end uint64
		want       want
	}{
		{
			"covered entirely",
			old.LogOff, old.End(),
			want{nil, nil},
		},
		{
			"covered from beyond both ends",
			0, ^uint64(0),
			want{nil, nil},
		},
		{
			"tail overwritten, head survives",
			old.LogOff + BlockSize, old.End(),
			want{at(old.LogOff, old.DiskOff, BlockSize), nil},
		},
		{
			"head overwritten, tail survives and stays block-aligned",
			0, old.LogOff + BlockSize,
			want{nil, at(old.LogOff+BlockSize, old.DiskOff+BlockSize, 3*BlockSize)},
		},
		{
			"middle overwritten, both survive",
			old.LogOff + BlockSize, old.LogOff + 2*BlockSize,
			want{
				at(old.LogOff, old.DiskOff, BlockSize),
				at(old.LogOff+2*BlockSize, old.DiskOff+2*BlockSize, 2*BlockSize),
			},
		},
	}

	for _, c := range cases {
		head, tail := SplitAround(old, c.start, c.end)
		if !sameExtent(head, c.want.head) {
			t.Errorf("%s: head = %v, want %v", c.name, head, c.want.head)
		}
		if !sameExtent(tail, c.want.tail) {
			t.Errorf("%s: tail = %v, want %v", c.name, tail, c.want.tail)
		}
	}
}

// The surviving tail's device offset has to stay block-aligned, because a read
// reaches it through O_DIRECT, where the offset must be sector-aligned.  The
// rounding that guarantees that must go *down*: rounding up would leave the
// bytes between the write's end and the tail's start described by no extent at
// all, and they would read back as zeroes.
func TestSplitAroundRoundsTheTailDownToABlock(t *testing.T) {
	old := Extent{LogOff: 0, DiskOff: 1 << 30, Length: 4 * BlockSize, Gen: 1}

	// A write ending one byte into the second block.
	_, tail := SplitAround(old, 0, BlockSize+1)
	if tail == nil {
		t.Fatal("no tail survived a write covering only the first block and a byte")
	}
	if tail.DiskOff%BlockSize != 0 {
		t.Errorf("tail disk offset %d is not block-aligned", tail.DiskOff)
	}
	if tail.LogOff > BlockSize+1 {
		t.Errorf("tail starts at %d, past the write's end — the gap would read as zeroes",
			tail.LogOff)
	}
	if tail.LogOff+tail.Length != old.End() {
		t.Errorf("tail ends at %d, want %d", tail.LogOff+tail.Length, old.End())
	}
}

// A write covering less than one whole block at the front cannot give anything
// back, and must not pretend otherwise.
func TestSplitAroundKeepsEverythingForASubBlockWrite(t *testing.T) {
	old := Extent{LogOff: 0, DiskOff: 1 << 30, Length: 2 * BlockSize, Gen: 1}

	head, tail := SplitAround(old, 0, 100)
	if head != nil {
		t.Errorf("head appeared for a write starting at the extent's own start: %v", head)
	}
	if tail == nil || tail.LogOff != old.LogOff || tail.Length != old.Length {
		t.Fatalf("tail = %v, want the extent unchanged", tail)
	}
	if _, length := CoveredBlocks(old, head, tail); length != 0 {
		t.Errorf("freed %d bytes from a sub-block overwrite", length)
	}
}

// CoveredBlocks must never hand back a block a survivor still reads from.
func TestCoveredBlocksNeverFreesALiveBlock(t *testing.T) {
	old := Extent{LogOff: 0, DiskOff: 1 << 30, Length: 4 * BlockSize, Gen: 1}

	// Head keeps one byte, so it still owns the whole first block.
	head := &Extent{LogOff: 0, DiskOff: old.DiskOff, Length: 1}
	off, length := CoveredBlocks(old, head, nil)
	if off != old.DiskOff+BlockSize {
		t.Errorf("freed range starts at %d, inside the head's own block (want %d)",
			off, old.DiskOff+BlockSize)
	}
	if length != 3*BlockSize {
		t.Errorf("freed %d bytes, want %d", length, 3*BlockSize)
	}

	// Whole extent dead: every block it owned comes back, padding included.
	off, length = CoveredBlocks(old, nil, nil)
	if off != old.DiskOff || length != 4*BlockSize {
		t.Errorf("full reclaim gave back %d bytes at %d, want %d at %d",
			length, off, 4*BlockSize, old.DiskOff)
	}

	// A trailing partial block belongs to the extent, so it is reclaimable too.
	ragged := Extent{LogOff: 0, DiskOff: old.DiskOff, Length: BlockSize + 10}
	if _, length = CoveredBlocks(ragged, nil, nil); length != 2*BlockSize {
		t.Errorf("ragged extent gave back %d bytes, want %d", length, 2*BlockSize)
	}
}

func sameExtent(a, b *Extent) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	}
	return a.LogOff == b.LogOff && a.DiskOff == b.DiskOff &&
		a.Length == b.Length && a.Gen == b.Gen
}
