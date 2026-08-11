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

// Every opcode the C daemon can send must resolve to an entry, and anything
// else must be ENOSYS rather than a nil handler.  The table is the only place
// an operation is registered, so a handler added without its entry is
// unreachable and this is what says so.
func TestDispatchTableCoversEveryOpcode(t *testing.T) {
	for _, code := range []uint16{
		ipcOpLookup, ipcOpGetattr, ipcOpReaddir, ipcOpReadlink, ipcOpCreate,
		ipcOpMkdir, ipcOpUnlink, ipcOpRmdir, ipcOpRename, ipcOpSymlink,
		ipcOpLink, ipcOpSetattr, ipcOpOpen, ipcOpRelease, ipcOpOpendir,
		ipcOpReleasedir, ipcOpStatfs, ipcOpAlloc, ipcOpCommit, ipcOpRead,
		ipcOpWrite, ipcOpFsync, ipcOpMknod, ipcOpFlush, ipcOpReadDirPlus,
	} {
		entry, found := ops[code]
		if !found {
			t.Errorf("opcode %d has no dispatch entry", code)
			continue
		}
		if entry.handle == nil || entry.name == "" {
			t.Errorf("opcode %d has an incomplete entry: %+v", code, entry)
		}
	}

	// 27 and 28 were GETLK/SETLK; they must stay unserved.
	for _, code := range []uint16{0, 27, 28, 999} {
		if _, found := ops[code]; found {
			t.Errorf("opcode %d should not be served", code)
		}
		if opName(code) != "unknown" {
			t.Errorf("opcode %d should have no metric name", code)
		}
	}
}
