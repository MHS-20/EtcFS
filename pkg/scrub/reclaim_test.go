package scrub

import (
	"context"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// revStore is the smallest store the auto-fix path needs: one extent key at a
// known revision, and a Txn that honours a ModRevision comparison.  The shared
// mock does not track revisions, so it cannot tell the two cases below apart.
type revStore struct {
	key    string
	value  string
	rev    int64
	nowRev int64 // revision the key is actually at, if it moved since the scan
	gone   bool
	dels   int
}

func (s *revStore) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (s *revStore) GetPrefix(_ context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	if prefix != metadata.PrefixExtent || s.gone {
		return nil, nil
	}
	return []*mvccpb.KeyValue{{
		Key: []byte(s.key), Value: []byte(s.value), ModRevision: s.rev,
	}}, nil
}

func (s *revStore) Txn(_ context.Context, ifs []clientv3.Cmp, thens, _ []clientv3.Op) (bool, error) {
	current := s.nowRev
	if s.gone {
		current = 0
	}
	for _, c := range ifs {
		if (*pb.Compare)(&c).GetModRevision() != current {
			return false, nil
		}
	}
	for range thens {
		s.dels++
	}
	s.gone = true
	return true, nil
}

type countingReclaimer struct{ freed uint64 }

func (r *countingReclaimer) Free(_, size uint64) { r.freed += size }
func (r *countingReclaimer) Owns(uint64) bool    { return true }

type silentLogger struct{}

func (silentLogger) Warn(string, ...any)  {}
func (silentLogger) Info(string, ...any)  {}
func (silentLogger) Error(string, ...any) {}

func orphanStore(rev, nowRev int64) *revStore {
	e := metadata.Extent{
		Key: metadata.ExtentKey(7, 0), Chunk: 0, Seq: 0,
		DiskOff: 1 << 20, LogOff: 0, Length: 4096,
	}
	// No inode key is served, so the extent is an orphan and auto-fixable.
	return &revStore{key: e.Key, value: e.Encode(), rev: rev, nowRev: nowRev}
}

func TestAutoFixReclaimsUnchangedExtent(t *testing.T) {
	store := orphanStore(11, 11)
	rec := &countingReclaimer{}
	s := New(store, "node", time.Hour, silentLogger{})
	s.SetReclaimer(rec)

	s.RunScrubPass(context.Background())

	if store.dels != 1 {
		t.Fatalf("orphan extent not deleted: %d deletes", store.dels)
	}
	if rec.freed != 4096 {
		t.Fatalf("blocks not reclaimed: freed %d", rec.freed)
	}
}

// The window this closes: the scan finds the extent, a truncate rewrites it and
// frees its blocks, the allocator hands them to another file, and the scrubber
// then frees the same range a second time.  An unconditional delete cannot see
// the difference — it succeeds on a key that is already gone.
func TestAutoFixLeavesExtentThatMovedSinceTheScan(t *testing.T) {
	store := orphanStore(11, 12)
	rec := &countingReclaimer{}
	s := New(store, "node", time.Hour, silentLogger{})
	s.SetReclaimer(rec)

	s.RunScrubPass(context.Background())

	if store.dels != 0 {
		t.Fatalf("extent deleted despite having changed since the scan: %d deletes", store.dels)
	}
	if rec.freed != 0 {
		t.Fatalf("blocks freed from a stale finding: freed %d", rec.freed)
	}
}
