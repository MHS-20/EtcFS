//go:build integration
// +build integration

// Integration tests that drive the IPC handlers against a real etcd and a real
// block device, which is the only place the wire encoding, the metadata layer
// and the device I/O are exercised together.
//
// Requires a running etcd. Start one with:
//
//	docker run -d -p 2379:2379 quay.io/coreos/etcd:v3.5.18 \
//	  /usr/local/bin/etcd --data-dir=/etcd-data \
//	  --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://0.0.0.0:2379
//
// Run with:
//
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 ./...
package ipc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/fencing"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	"github.com/MHS-20/EtcFS/test/etcdtest"
)

// deviceBytes is one arena plus room to grow, sparse on disk.
const deviceBytes = 2 << 30

// newTestService builds a Service backed by real etcd and a file standing in
// for the shared block device.
func newTestService(t *testing.T) (*Service, *metadata.Store) {
	t.Helper()

	cli := etcdtest.Client(t)

	path := filepath.Join(t.TempDir(), "device.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create device file: %v", err)
	}
	if err := f.Truncate(deviceBytes); err != nil {
		t.Fatalf("size device file: %v", err)
	}
	_ = f.Close()

	dev, err := blockio.OpenBuffered(path)
	if err != nil {
		t.Fatalf("open device: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })

	store := metadata.NewStore(cli, "node-test")
	membership := metadata.NewMembership(cli, "node-test", "etcfuse", 10*time.Second)
	svc := NewService(store, membership, fencing.NewWatchdog(membership, 10*time.Second),
		config.NewLogger(0))

	if err := svc.InitGeneration(context.Background()); err != nil {
		t.Fatalf("init generation: %v", err)
	}
	svc.InstallStoreGuard()
	svc.SetBlockDevice(dev, false)

	return svc, store
}

// seedFile creates an inode the handlers can operate on.
func seedFile(t *testing.T, store *metadata.Store, ino uint64, mode uint32) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.AtomicCreateFile(ctx, metadata.RootIno, t.Name(), ino, mode, 1000, 1000); err != nil {
		t.Fatalf("seed inode %d: %v", ino, err)
	}
}

// setattrPayload builds a SETATTR request. Fields the mask does not select are
// still carried, and must be ignored by the handler.
func setattrPayload(ino uint64, valid uint32, size uint64, mode, uid, gid uint32,
	atime, mtime, ctime uint64) []byte {

	var b buf
	b.w64(ino)
	b.w64(0) // fh
	b.w32(valid)
	b.w64(size)
	b.w32(mode)
	b.w32(uid)
	b.w32(gid)
	b.w64(atime)
	b.w64(mtime)
	b.w64(ctime)
	b.w32(0) // atime_nsec
	b.w32(0) // mtime_nsec
	b.w32(0) // ctime_nsec
	return b.b
}

// writePayload builds a WRITE request from the given caller's uid.
func writePayload(ino, offset uint64, data []byte, uid uint32) []byte {
	var b buf
	b.w64(ino)
	b.w64(offset)
	b.w32(uint32(len(data)))
	b.b = append(b.b, data...)
	b.w32(uid)
	return b.b
}

func mustSetattr(t *testing.T, svc *Service, payload []byte) {
	t.Helper()
	resp, err := svc.handleSetattr(context.Background(), payload)
	if err != nil {
		t.Fatalf("setattr: %v", err)
	}
	if code := int32(uint32(resp[0])<<24 | uint32(resp[1])<<16 | uint32(resp[2])<<8 | uint32(resp[3])); code != 0 {
		t.Fatalf("setattr returned errno %d", -code)
	}
}

// chmod, chown and utimensat used to return success and change nothing: the C
// side sent only st_size, and the handler read only st_size.
func TestIntegration_SetattrAppliesEverySelectedField(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 7001
	seedFile(t, store, ino, metadata.ModeFile|0644)

	const (
		wantUID          = 4242
		wantGID          = 4343
		wantAtime        = 1_700_000_000
		wantMtime        = 1_700_000_500
		selected  uint32 = fattrMode | fattrUID | fattrGID | fattrAtime | fattrMtime
	)
	mustSetattr(t, svc, setattrPayload(ino, selected,
		999, // size is not selected and must be ignored
		metadata.ModeFile|0600, wantUID, wantGID, wantAtime, wantMtime, 0))

	rec, err := store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		t.Fatalf("read back inode: %v", err)
	}
	if got := rec.Mode & ^metadata.S_IFMT; got != 0600 {
		t.Errorf("mode = %#o, want 0600", got)
	}
	if rec.Mode&metadata.S_IFMT != metadata.ModeFile {
		t.Errorf("chmod changed the file type to %#o", rec.Mode&metadata.S_IFMT)
	}
	if rec.UID != wantUID || rec.GID != wantGID {
		t.Errorf("owner = %d:%d, want %d:%d", rec.UID, rec.GID, wantUID, wantGID)
	}
	if rec.Atime.Unix() != wantAtime || rec.Mtime.Unix() != wantMtime {
		t.Errorf("times = %d/%d, want %d/%d",
			rec.Atime.Unix(), rec.Mtime.Unix(), wantAtime, wantMtime)
	}
	if rec.Size != 0 {
		t.Errorf("size changed to %d although FATTR_SIZE was not set", rec.Size)
	}
}

// Growing a file with ftruncate has to move the size; the bytes it exposes are
// a hole, which reads back as zeroes.
func TestIntegration_SetattrGrowsAndTheGapReadsAsZeroes(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 7002
	seedFile(t, store, ino, metadata.ModeFile|0644)

	const grown = 12288
	mustSetattr(t, svc, setattrPayload(ino, fattrSize, grown, 0, 0, 0, 0, 0, 0))

	rec, err := store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		t.Fatalf("read back inode: %v", err)
	}
	if rec.Size != grown {
		t.Fatalf("size = %d, want %d", rec.Size, grown)
	}

	var rq buf
	rq.w64(ino)
	rq.w64(0)
	rq.w32(grown)
	resp, err := svc.handleRead(ctx, rq.b)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	got := readPayload(t, resp)
	if len(got) != grown {
		t.Fatalf("read returned %d bytes, want %d", len(got), grown)
	}
	if !bytes.Equal(got, make([]byte, grown)) {
		t.Error("the grown range did not read back as zeroes")
	}
}

// A hole *before* data is where the read path went wrong: it placed the
// extent's bytes at the running output position rather than at the offset they
// belong to, so everything after a gap came back shifted.
func TestIntegration_ReadFillsHolesAroundData(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 7003
	seedFile(t, store, ino, metadata.ModeFile|0644)

	// Data at 8 KiB, leaving [0,8192) a hole and [12288,16384) a tail hole.
	const dataOff, dataLen, readLen = 8192, 4096, 16384
	payloadData := bytes.Repeat([]byte{0xAB}, dataLen)

	if _, err := svc.handleWrite(ctx, writePayload(ino, dataOff, payloadData, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}

	mustSetattr(t, svc, setattrPayload(ino, fattrSize, readLen, 0, 0, 0, 0, 0, 0))

	var rq buf
	rq.w64(ino)
	rq.w64(0)
	rq.w32(readLen)
	resp, err := svc.handleRead(ctx, rq.b)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	got := readPayload(t, resp)
	if len(got) != readLen {
		t.Fatalf("read returned %d bytes, want %d", len(got), readLen)
	}

	want := make([]byte, readLen)
	copy(want[dataOff:], payloadData)
	if !bytes.Equal(got, want) {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("first mismatch at byte %d: got %#x, want %#x", i, got[i], want[i])
			}
		}
	}
}

// readPayload strips the error code and length header from a READ response.
func readPayload(t *testing.T, resp []byte) []byte {
	t.Helper()
	if len(resp) < 8 {
		t.Fatalf("read response too short: %d bytes", len(resp))
	}
	code := int32(uint32(resp[0])<<24 | uint32(resp[1])<<16 | uint32(resp[2])<<8 | uint32(resp[3]))
	if code != 0 {
		t.Fatalf("read returned errno %d", -code)
	}
	n := uint32(resp[4])<<24 | uint32(resp[5])<<16 | uint32(resp[6])<<8 | uint32(resp[7])
	return resp[8 : 8+n]
}

// ---- caller credentials ----

// Everything used to be created owned by a hardcoded uid 1000 with the umask
// thrown away, so the filesystem had no notion of who owned what.
func TestIntegration_CreatedFilesCarryTheCallersCredentials(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()

	const (
		uid   = 5150
		gid   = 5151
		umask = 0027
	)

	name := func(op string) []byte { return []byte(op) }

	t.Run("create", func(t *testing.T) {
		var b buf
		b.w64(metadata.RootIno)
		b.w32(uint32(len(name("cfile"))))
		b.b = append(b.b, name("cfile")...)
		b.w32(metadata.ModeFile | 0666)
		b.w32(0) // flags
		b.w32(umask)
		b.w32(uid)
		b.w32(gid)
		assertOwned(t, svc, store, ctx, svc.handleCreate, b.b, uid, gid, 0640)
	})

	t.Run("mkdir", func(t *testing.T) {
		var b buf
		b.w64(metadata.RootIno)
		b.w32(uint32(len(name("cdir"))))
		b.b = append(b.b, name("cdir")...)
		b.w32(0777)
		b.w32(umask)
		b.w32(uid)
		b.w32(gid)
		assertOwned(t, svc, store, ctx, svc.handleMkdir, b.b, uid, gid, 0750)
	})

	t.Run("mknod", func(t *testing.T) {
		var b buf
		b.w64(metadata.RootIno)
		b.w32(uint32(len(name("cnod"))))
		b.b = append(b.b, name("cnod")...)
		b.w32(0666)
		b.w32(0) // rdev
		b.w32(umask)
		b.w32(uid)
		b.w32(gid)
		assertOwned(t, svc, store, ctx, svc.handleMknod, b.b, uid, gid, 0640)
	})

	t.Run("symlink", func(t *testing.T) {
		var b buf
		b.w64(metadata.RootIno)
		b.w32(uint32(len(name("clink"))))
		b.b = append(b.b, name("clink")...)
		b.w32(uint32(len("/some/target")))
		b.b = append(b.b, []byte("/some/target")...)
		b.w32(uid)
		b.w32(gid)
		// A symlink's own permission bits are not meaningful and the umask
		// never applies to them.
		assertOwned(t, svc, store, ctx, svc.handleSymlink, b.b, uid, gid, 0777)
	})
}

// assertOwned runs a creating handler and checks the inode it produced.
func assertOwned(t *testing.T, svc *Service, store *metadata.Store, ctx context.Context,
	handler func(context.Context, []byte) ([]byte, error), payload []byte,
	wantUID, wantGID, wantPerm uint32) {

	t.Helper()
	resp, err := handler(ctx, payload)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	code := int32(uint32(resp[0])<<24 | uint32(resp[1])<<16 | uint32(resp[2])<<8 | uint32(resp[3]))
	if code != 0 {
		t.Fatalf("handler returned errno %d", -code)
	}
	ino := uint64(resp[4])<<56 | uint64(resp[5])<<48 | uint64(resp[6])<<40 | uint64(resp[7])<<32 |
		uint64(resp[8])<<24 | uint64(resp[9])<<16 | uint64(resp[10])<<8 | uint64(resp[11])

	rec, err := store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		t.Fatalf("read back inode %d: %v", ino, err)
	}
	if rec.UID != wantUID || rec.GID != wantGID {
		t.Errorf("owner = %d:%d, want %d:%d", rec.UID, rec.GID, wantUID, wantGID)
	}
	if got := rec.Mode & 0777; got != wantPerm {
		t.Errorf("permissions = %#o, want %#o", got, wantPerm)
	}
}

// A read is answered with the whole requested range, holes included, so it has
// to be clamped to the file's size: without that, a request reaching past the
// end comes back as a full buffer of zeroes instead of a short read, and a
// reader that never sees a short read never sees EOF. `cat` on a 7-byte file
// produced hundreds of megabytes of NULs.
func TestIntegration_ReadStopsAtTheEndOfTheFile(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 7004
	seedFile(t, store, ino, metadata.ModeFile|0644)

	payloadData := []byte("s1-data")
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, payloadData, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The kernel's typical request is far larger than the file.
	var rq buf
	rq.w64(ino)
	rq.w64(0)
	rq.w32(131072)
	resp, err := svc.handleRead(ctx, rq.b)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := readPayload(t, resp); !bytes.Equal(got, payloadData) {
		t.Fatalf("read returned %d bytes (%q), want the file's %d", len(got), got, len(payloadData))
	}

	// And a read starting at or past the end is empty, not a buffer of zeroes.
	var eof buf
	eof.w64(ino)
	eof.w64(uint64(len(payloadData)))
	eof.w32(4096)
	resp, err = svc.handleRead(ctx, eof.b)
	if err != nil {
		t.Fatalf("read at EOF: %v", err)
	}
	if got := readPayload(t, resp); len(got) != 0 {
		t.Fatalf("read at EOF returned %d bytes", len(got))
	}
}

// O_TRUNC used to be dropped on the floor: the C daemon answered open locally
// and never told the backend, so `> file` left the old contents in place.
func TestIntegration_OpenWithTruncEmptiesTheFile(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 7005
	seedFile(t, store, ino, metadata.ModeFile|0644)

	payloadData := []byte("before")
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, payloadData, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}

	before, err := store.GetInode(ctx, ino)
	if err != nil || before == nil {
		t.Fatalf("read back inode: %v", err)
	}

	var oq buf
	oq.w64(ino)
	oq.w32(oTrunc)
	resp, err := svc.handleOpen(ctx, oq.b)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if code := int32(uint32(resp[0])<<24 | uint32(resp[1])<<16 | uint32(resp[2])<<8 | uint32(resp[3])); code != 0 {
		t.Fatalf("open returned errno %d", -code)
	}

	rec, err := store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		t.Fatalf("read back inode: %v", err)
	}
	if rec.Size != 0 {
		t.Errorf("size = %d after O_TRUNC, want 0", rec.Size)
	}
	if rec.Mtime.Before(before.Mtime) || rec.Ctime.Before(before.Ctime) {
		t.Errorf("O_TRUNC moved mtime/ctime backwards: %v/%v", rec.Mtime, rec.Ctime)
	}

	var rq buf
	rq.w64(ino)
	rq.w64(0)
	rq.w32(4096)
	resp, err = svc.handleRead(ctx, rq.b)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := readPayload(t, resp); len(got) != 0 {
		t.Fatalf("read after O_TRUNC returned %d bytes (%q)", len(got), got)
	}
}

// A write by an unprivileged user has to cost the file its set-user-ID bits,
// or an ordinary user can change what a setuid binary does while it keeps
// running as its owner.
func TestIntegration_WriteClearsSetIDBits(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 7006
	seedFile(t, store, ino, metadata.ModeFile|metadata.S_ISUID|metadata.S_ISGID|0777)

	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, []byte("x"), 65534)); err != nil {
		t.Fatalf("write: %v", err)
	}
	rec, err := store.GetInode(ctx, ino)
	if err != nil || rec == nil {
		t.Fatalf("read back inode: %v", err)
	}
	if got := rec.Mode & ^metadata.S_IFMT; got != 0777 {
		t.Errorf("mode = %#o after an unprivileged write, want 0777", got)
	}

	// Root keeps them, the way a process holding CAP_FSETID does.
	const rootIno = 7007
	if _, err := store.AtomicCreateFile(ctx, metadata.RootIno, t.Name()+"-root", rootIno,
		metadata.ModeFile|metadata.S_ISUID|0777, 0, 0); err != nil {
		t.Fatalf("seed inode: %v", err)
	}
	if _, err := svc.handleWrite(ctx, writePayload(rootIno, 0, []byte("x"), 0)); err != nil {
		t.Fatalf("write: %v", err)
	}
	rec, err = store.GetInode(ctx, rootIno)
	if err != nil || rec == nil {
		t.Fatalf("read back inode: %v", err)
	}
	if rec.Mode&metadata.S_ISUID == 0 {
		t.Errorf("a write by root cleared the set-user-ID bit: mode = %#o", rec.Mode)
	}
}

// A file with an open descriptor has to survive the unlink of its last name:
// the record used to go immediately, so a read through the descriptor came
// back ENOENT and tmpfile(3)-style scratch files were unusable.
func TestIntegration_UnlinkKeepsAnOpenFileAlive(t *testing.T) {
	svc, store := newTestService(t)
	ctx := context.Background()
	const ino = 7008
	name := t.Name()
	seedFile(t, store, ino, metadata.ModeFile|0644)

	var oq buf
	oq.w64(ino)
	oq.w32(0)
	if _, err := svc.handleOpen(ctx, oq.b); err != nil {
		t.Fatalf("open: %v", err)
	}
	payloadData := []byte("still here")
	if _, err := svc.handleWrite(ctx, writePayload(ino, 0, payloadData, 0)); err != nil {
		t.Fatalf("write: %v", err)
	}

	var uq buf
	uq.w64(metadata.RootIno)
	uq.w32(uint32(len(name)))
	uq.b = append(uq.b, name...)
	if _, err := svc.handleUnlink(ctx, uq.b); err != nil {
		t.Fatalf("unlink: %v", err)
	}

	rec, err := store.GetInode(ctx, ino)
	if err != nil {
		t.Fatalf("read back inode: %v", err)
	}
	if rec == nil {
		t.Fatal("unlink deleted an inode that was still open")
	}
	if rec.Nlink != 0 {
		t.Errorf("nlink = %d after the last name went, want 0", rec.Nlink)
	}

	var rq buf
	rq.w64(ino)
	rq.w64(0)
	rq.w32(4096)
	resp, err := svc.handleRead(ctx, rq.b)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := readPayload(t, resp); !bytes.Equal(got, payloadData) {
		t.Errorf("read through the open descriptor returned %q, want %q", got, payloadData)
	}

	// The last close is what finally removes it.
	var rel buf
	rel.w64(ino)
	if _, err := svc.handleRelease(ctx, rel.b); err != nil {
		t.Fatalf("release: %v", err)
	}
	rec, err = store.GetInode(ctx, ino)
	if err != nil {
		t.Fatalf("read back inode: %v", err)
	}
	if rec != nil {
		t.Error("the last release left the inode behind")
	}
	orphans, err := store.ListOrphans(ctx, store.NodeID())
	if err != nil {
		t.Fatalf("list orphans: %v", err)
	}
	if len(orphans) != 0 {
		t.Errorf("orphan marker survived the last release: %v", orphans)
	}
}
