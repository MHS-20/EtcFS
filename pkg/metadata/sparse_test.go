package metadata

import (
	"reflect"
	"testing"
)

// ext builds an extent as GetExtents would return it. Seq orders overlapping
// writes; the sort GetExtents applies is reproduced by sortForSeek below.
func ext(logOff, length, seq uint64) Extent {
	return Extent{LogOff: logOff, Length: length, Seq: seq}
}

// sortForSeek puts extents in the order GetExtents guarantees — ascending
// logical offset, newest first where two share one — so these tests exercise
// DataRanges against the same input the read path sees.
func sortForSeek(extents []Extent) []Extent {
	out := append([]Extent(nil), extents...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			a, b := out[j-1], out[j]
			if a.LogOff < b.LogOff || (a.LogOff == b.LogOff && a.Seq >= b.Seq) {
				break
			}
			out[j-1], out[j] = b, a
		}
	}
	return out
}

func TestDataRanges(t *testing.T) {
	cases := []struct {
		name    string
		extents []Extent
		size    uint64
		want    [][2]uint64
	}{
		{
			name: "an empty file is all hole",
			size: 100,
		},
		{
			name:    "one extent covering the whole file",
			extents: []Extent{ext(0, 100, 1)},
			size:    100,
			want:    [][2]uint64{{0, 100}},
		},
		{
			name:    "a hole between two extents",
			extents: []Extent{ext(0, 10, 1), ext(50, 10, 2)},
			size:    100,
			want:    [][2]uint64{{0, 10}, {50, 60}},
		},
		{
			name:    "adjacent extents merge into one run",
			extents: []Extent{ext(0, 10, 1), ext(10, 10, 2)},
			size:    100,
			want:    [][2]uint64{{0, 20}},
		},
		{
			// The older extent reaches further than the newer one, so its tail
			// is still live and must still count as data.
			name:    "an overwrite shorter than what it overwrites",
			extents: []Extent{ext(0, 100, 1), ext(0, 50, 2)},
			size:    100,
			want:    [][2]uint64{{0, 100}},
		},
		{
			name:    "data past the end of the file is clamped away",
			extents: []Extent{ext(0, 100, 1)},
			size:    40,
			want:    [][2]uint64{{0, 40}},
		},
		{
			name:    "a file that is entirely a hole before its data",
			extents: []Extent{ext(4096, 100, 1)},
			size:    8192,
			want:    [][2]uint64{{4096, 4196}},
		},
	}

	for _, c := range cases {
		got := DataRanges(sortForSeek(c.extents), c.size)
		if len(got) == 0 && len(c.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: DataRanges = %v, want %v", c.name, got, c.want)
		}
	}
}

// SEEK_DATA and SEEK_HOLE have to agree with what a read of the same offset
// returns, so they are checked against the same extent layouts.
func TestSeekDataAndSeekHole(t *testing.T) {
	// Bytes 0-10 and 50-60 hold data; the file is 100 long.
	extents := sortForSeek([]Extent{ext(0, 10, 1), ext(50, 10, 2)})
	const size = 100

	dataCases := []struct {
		offset uint64
		want   uint64
		ok     bool
	}{
		{0, 0, true},   // already on data
		{5, 5, true},   // inside the first run
		{10, 50, true}, // in the hole: the next data is the second run
		{55, 55, true}, // inside the second run
		{60, 0, false}, // only hole remains, so ENXIO
		{100, 0, false},
		{200, 0, false}, // past the end
	}
	for _, c := range dataCases {
		got, ok := SeekData(extents, size, c.offset)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("SeekData(%d) = (%d, %v), want (%d, %v)", c.offset, got, ok, c.want, c.ok)
		}
	}

	holeCases := []struct {
		offset uint64
		want   uint64
		ok     bool
	}{
		{0, 10, true},  // in data: the hole starts where the run ends
		{5, 10, true},  //
		{10, 10, true}, // already in the hole
		{50, 60, true}, // in the second run
		{60, 60, true}, // in the trailing hole
		{99, 99, true},
		{100, 0, false}, // at the end, so ENXIO
	}
	for _, c := range holeCases {
		got, ok := SeekHole(extents, size, c.offset)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("SeekHole(%d) = (%d, %v), want (%d, %v)", c.offset, got, ok, c.want, c.ok)
		}
	}
}

// A fully allocated file has no hole until its very end, which is the case
// that separates a correct SEEK_HOLE from one that reports a hole inside data.
func TestSeekHoleOnFullyAllocatedFileIsEOF(t *testing.T) {
	extents := sortForSeek([]Extent{ext(0, 100, 1)})
	got, ok := SeekHole(extents, 100, 0)
	if !ok || got != 100 {
		t.Fatalf("SeekHole = (%d, %v), want (100, true)", got, ok)
	}
}

// An empty file has no data anywhere, so SEEK_DATA must report ENXIO rather
// than offset 0 — a backup tool would otherwise copy a hole as data.
func TestSeekDataOnEmptyFile(t *testing.T) {
	if _, ok := SeekData(nil, 100, 0); ok {
		t.Fatal("SeekData found data in a file with no extents")
	}
	got, ok := SeekHole(nil, 100, 0)
	if !ok || got != 0 {
		t.Fatalf("SeekHole = (%d, %v), want (0, true)", got, ok)
	}
}
