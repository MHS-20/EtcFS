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
//	ETCD_ENDPOINTS=http://localhost:2379 go test -tags=integration -count=1 ./internal/ipc/
package ipc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/pkg/blockio"
	"github.com/MHS-20/EtcFS/pkg/fencing"
	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// deviceBytes is one arena plus room to grow, sparse on disk.
const deviceBytes = 2 << 30

// newTestService builds a Service backed by real etcd and a file standing in
// for the shared block device.
func newTestService(t *testing.T) (*Service, *metadata.Store) {
	t.Helper()

	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "http://localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("cannot connect to etcd at %s: %v", endpoints, err)
	}

	for _, prefix := range []string{
		metadata.PrefixInode, metadata.PrefixDirent, metadata.PrefixExtent,
		metadata.PrefixArena, metadata.PrefixFreeArena, metadata.PrefixGen,
		metadata.PrefixLock,
	} {
		if _, derr := cli.Delete(context.Background(), prefix, clientv3.WithPrefix()); derr != nil {
			t.Fatalf("clear %s: %v", prefix, derr)
		}
	}
	if _, derr := cli.Delete(context.Background(), metadata.PrefixArenaLog); derr != nil {
		t.Fatalf("clear arena counter: %v", derr)
	}
	t.Cleanup(func() { _ = cli.Close() })

	path := filepath.Join(t.TempDir(), "device.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create device file: %v", err)
	}
	if err := f.Truncate(deviceBytes); err != nil {
		t.Fatalf("size device file: %v", err)
	}
	_ = f.Close()

	dev, err := blockio.Open(path)
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
	svc.SetBlockDevice(dev)

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

	var wq buf
	wq.w64(ino)
	wq.w64(dataOff)
	wq.w32(dataLen)
	wq.b = append(wq.b, payloadData...)
	if _, err := svc.handleWrite(ctx, wq.b); err != nil {
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
