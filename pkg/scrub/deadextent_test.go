package scrub

import (
	"testing"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

func ext(seq, logOff, length uint64) metadata.Extent {
	return metadata.Extent{
		Key:   metadata.ExtentKey(1, seq),
		Chunk: seq,
		Seq:   seq,
		// Disk offsets do not matter to deadReason; keep them distinct so a
		// mix-up shows up as a failure rather than a coincidence.
		DiskOff: seq * length,
		LogOff:  logOff,
		Length:  length,
	}
}

// An extent left entirely past the file's size by a truncate is unreachable —
// the kernel clamps reads to the inode size — but nothing else in the scrubber
// notices, because the inode is still very much alive.
func TestDeadReasonFindsExtentsPastEOF(t *testing.T) {
	live := ext(0, 0, 4096)
	past := ext(1, 8192, 4096)
	all := []metadata.Extent{live, past}

	if r := deadReason(past, 4096, all); r == "" {
		t.Error("extent starting past the file size was treated as live")
	}
	if r := deadReason(live, 4096, all); r != "" {
		t.Errorf("live extent reported dead: %s", r)
	}
}

// An extent only partly past the end still holds live bytes at its head, so it
// must survive: reclaiming it would take the head with it.
func TestDeadReasonKeepsPartiallyTruncatedExtent(t *testing.T) {
	e := ext(0, 0, 8192)
	if r := deadReason(e, 4096, []metadata.Extent{e}); r != "" {
		t.Errorf("extent straddling the new EOF reported dead: %s", r)
	}
}

// A later write that fully covers an earlier one makes the earlier extent's
// blocks dead — this is the overwrite leak, invisible to the orphan check.
func TestDeadReasonFindsSupersededExtents(t *testing.T) {
	old := ext(1, 0, 4096)
	newer := ext(2, 0, 4096)
	all := []metadata.Extent{newer, old}

	if r := deadReason(old, 4096, all); r == "" {
		t.Error("overwritten extent was treated as live")
	}
	if r := deadReason(newer, 4096, all); r != "" {
		t.Errorf("the overwriting extent was reported dead: %s", r)
	}
}

// A partial overwrite leaves both extents readable, so neither may be
// reclaimed; the read path resolves them by chunk order instead.
func TestDeadReasonKeepsPartiallyOverwrittenExtent(t *testing.T) {
	old := ext(1, 0, 8192)
	newer := ext(2, 0, 4096)
	all := []metadata.Extent{newer, old}

	if r := deadReason(old, 8192, all); r != "" {
		t.Errorf("partly overwritten extent reported dead: %s", r)
	}
}
