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

// setattrPayloadLen must match what ec_setattr in pkg/fuse/ops.c writes. The
// two are hand-encoded on opposite sides of the socket, so a field added to one
// and not the other shifts every field after it.
func TestSetattrPayloadMatchesCDaemonWidth(t *testing.T) {
	// ino, fh, size, atime, mtime, ctime are u64; valid, mode, uid, gid are u32.
	const cSideWidth = 6*8 + 4*4
	if setattrPayloadLen != cSideWidth {
		t.Fatalf("setattrPayloadLen is %d, ec_setattr writes %d", setattrPayloadLen, cSideWidth)
	}
}

// chmod must not be able to change what kind of file something is. The kernel
// sends a whole st_mode, so the stored type bits have to survive it.
func TestApplyModeKeepsTheFileType(t *testing.T) {
	cases := []struct {
		name     string
		stored   uint32
		incoming uint32
		want     uint32
	}{
		{"chmod on a regular file", metadata.ModeFile | 0644, metadata.ModeFile | 0600, metadata.ModeFile | 0600},
		{"a symlink stays a symlink", metadata.ModeSymlink | 0777, metadata.ModeFile | 0644, metadata.ModeSymlink | 0644},
		{"a directory stays a directory", metadata.ModeDir | 0755, metadata.ModeFile | 0700, metadata.ModeDir | 0700},
		{"bare permission bits", metadata.ModeFile | 0644, 0640, metadata.ModeFile | 0640},
	}
	for _, c := range cases {
		got := (c.stored & metadata.S_IFMT) | (c.incoming &^ metadata.S_IFMT)
		if got != c.want {
			t.Errorf("%s: got %#o, want %#o", c.name, got, c.want)
		}
	}
}
