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
