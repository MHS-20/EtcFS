package ipc

import (
	"testing"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// attrWireSize is what rb_attr() in pkg/fuse/ops.c consumes per attr block:
// six u64 plus nine u32.  Readdirplus writes one attr block per entry, so a
// mismatch here silently desynchronises the C-side response parser.
const attrWireSize = 6*8 + 9*4

func TestAttrBlockMatchesCDaemonWidth(t *testing.T) {
	var b buf
	b.wAttr(&metadata.InodeRecord{})
	if len(b.b) != attrWireSize {
		t.Fatalf("wAttr wrote %d bytes, ops.c rb_attr reads %d", len(b.b), attrWireSize)
	}
}
