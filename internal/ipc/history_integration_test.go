//go:build integration
// +build integration

package ipc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/MHS-20/EtcFS/internal/config"
	"github.com/MHS-20/EtcFS/internal/history"
	"github.com/MHS-20/EtcFS/pkg/fencing"
	"github.com/MHS-20/EtcFS/pkg/metadata"
	"github.com/MHS-20/EtcFS/test/etcdtest"
	"github.com/MHS-20/EtcFS/test/verify"
)

// Two nodes contending over one directory, recorded through the daemon's own
// dispatch path and checked for linearizability.
//
// The unit tests in test/verify check the checker; this one checks the
// filesystem. It lives here rather than beside them because driving a Service
// means going through dispatch, and exporting an entry point into production
// code so a test can reach it would be a worse trade than this import.
func TestIntegration_RecordedNamespaceHistoryIsLinearizable(t *testing.T) {
	cli := etcdtest.Client(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history.jsonl")

	store := metadata.NewStore(cli, "n1")
	if _, err := store.CreateInode(ctx, metadata.RootIno, metadata.ModeDir|0755, 0, 0); err != nil {
		t.Fatalf("seed root: %v", err)
	}

	services := make([]*Service, 0, 2)
	for _, node := range []string{"n1", "n2"} {
		st := metadata.NewStore(cli, node)
		membership := metadata.NewMembership(cli, node, "verify", 10*time.Second)
		svc := NewService(st, membership, fencing.NewWatchdog(membership, 10*time.Second),
			config.NewLogger(0))
		if err := svc.InitGeneration(ctx); err != nil {
			t.Fatalf("init generation for %s: %v", node, err)
		}
		svc.InstallStoreGuard()

		rec, err := history.NewRecorder(path, node)
		if err != nil {
			t.Fatalf("open recorder for %s: %v", node, err)
		}
		t.Cleanup(func() { _ = rec.Close() })
		svc.SetHistoryRecorder(rec)
		services = append(services, svc)
	}

	// A handful of names, contended by both nodes: the interleavings a
	// single-node test cannot produce are the ones linearizability is about.
	const rounds = 10
	var wg sync.WaitGroup
	for i, svc := range services {
		wg.Add(1)
		go func(i int, svc *Service) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				name := fmt.Sprintf("contended-%d", round%3)
				ino := uint64(3000 + i*1000 + round)
				_, _ = svc.observedDispatch(ipcOpCreate, createPayload(metadata.RootIno, name, ino))
				_, _ = svc.observedDispatch(ipcOpLookup, lookupPayload(metadata.RootIno, name))
				_, _ = svc.observedDispatch(ipcOpUnlink, lookupPayload(metadata.RootIno, name))
			}
		}(i, svc)
	}
	wg.Wait()

	entries, err := history.Load(path)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no operations were recorded")
	}
	ops, err := verify.DecodeNamespace(entries)
	if err != nil {
		t.Fatalf("decode history: %v", err)
	}
	t.Logf("checking %d recorded namespace operations from %d entries", len(ops), len(entries))

	res := verify.Check(verify.NamespaceModel, verify.Operations(ops), verify.AllLinearizable, 0, 60*time.Second)
	switch res {
	case porcupine.Ok:
	case porcupine.Unknown:
		t.Fatalf("the checker did not finish in time on %d operations", len(ops))
	default:
		dump, _ := os.ReadFile(path)
		t.Fatalf("the recorded history is not linearizable:\n%s", dump)
	}
}

// A checker that passes on every history it is given proves nothing, so the
// recorded history is also perturbed in the one way a real lost update would
// show up — a name reported created twice with no unlink between — and the
// check has to reject it.
func TestIntegration_TamperedHistoryIsRejected(t *testing.T) {
	cli := etcdtest.Client(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history.jsonl")

	st := metadata.NewStore(cli, "n1")
	if _, err := st.CreateInode(ctx, metadata.RootIno, metadata.ModeDir|0755, 0, 0); err != nil {
		t.Fatalf("seed root: %v", err)
	}
	membership := metadata.NewMembership(cli, "n1", "verify", 10*time.Second)
	svc := NewService(st, membership, fencing.NewWatchdog(membership, 10*time.Second), config.NewLogger(0))
	if err := svc.InitGeneration(ctx); err != nil {
		t.Fatalf("init generation: %v", err)
	}
	svc.InstallStoreGuard()
	rec, err := history.NewRecorder(path, "n1")
	if err != nil {
		t.Fatalf("open recorder: %v", err)
	}
	defer func() { _ = rec.Close() }()
	svc.SetHistoryRecorder(rec)

	_, _ = svc.observedDispatch(ipcOpCreate, createPayload(metadata.RootIno, "dup", 0))
	_, _ = svc.observedDispatch(ipcOpLookup, lookupPayload(metadata.RootIno, "dup"))

	entries, err := history.Load(path)
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	ops, err := verify.DecodeNamespace(entries)
	if err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(ops) < 2 {
		t.Fatalf("recorded %d operations, want at least 2", len(ops))
	}

	// The second create is the sabotage: the same name created again, after
	// the first succeeded, with nothing removing it in between.
	dup := ops[0]
	dup.Ino++
	dup.Call = ops[len(ops)-1].Ret + 10
	dup.Ret = dup.Call + 10
	tampered := append(append([]verify.Op{}, ops...), dup)

	res := verify.Check(verify.NamespaceModel, verify.Operations(tampered), verify.AllLinearizable, 0, 60*time.Second)
	if res == porcupine.Ok {
		t.Fatal("a history with a duplicated create was accepted; the check has no teeth")
	}
}

// createPayload is a CREATE request: the inode number is allocated by the
// handler, so the one a caller has in mind is not part of it.
func createPayload(parent uint64, name string, _ uint64) []byte {
	var b buf
	b.w64(parent)
	b.w32(uint32(len(name)))
	b.b = append(b.b, name...)
	b.w32(0644) // mode
	b.w32(0)    // flags
	b.w32(0)    // umask
	b.w32(0)    // uid
	b.w32(0)    // gid
	return b.b
}

func lookupPayload(parent uint64, name string) []byte {
	var b buf
	b.w64(parent)
	b.w32(uint32(len(name)))
	b.b = append(b.b, name...)
	return b.b
}
