package ipc

import (
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// The buffer's whole safety argument rests on which comparison survives when
// several writes touch one key: the one describing etcd's real state, taken the
// first time the key was seen.  A comparison from a later write refers to a
// revision that only ever existed inside this buffer, so carrying it would make
// the flush impossible to commit.
func TestPendingKeepsTheFirstComparisonPerKey(t *testing.T) {
	var p pending

	// First write: a fresh chunk, asserted not to exist yet.
	p.add(
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision("extent:1/0"), "=", 0)},
		[]clientv3.Op{clientv3.OpPut("extent:1/0", "first")},
		nil, 4096)

	// Second write buries it, and plans against the revision the first write's
	// replay stamped on it — which is not a revision etcd has ever issued.
	p.add(
		[]clientv3.Cmp{
			clientv3.Compare(clientv3.ModRevision("extent:1/0"), "=", 0),
			clientv3.Compare(clientv3.CreateRevision("extent:1/1"), "=", 0),
		},
		[]clientv3.Op{
			clientv3.OpDelete("extent:1/0"),
			clientv3.OpPut("extent:1/1", "second"),
		},
		nil, 4096)

	cmps, ops := p.txn()
	if len(ops) != 2 {
		t.Fatalf("coalesced ops = %d, want 2 (one per key)", len(ops))
	}
	if got := string(ops[0].KeyBytes()); got != "extent:1/0" || !ops[0].IsDelete() {
		t.Errorf("first key = %q delete=%v, want extent:1/0 deleted by the later write",
			got, ops[0].IsDelete())
	}
	if got := string(ops[1].KeyBytes()); got != "extent:1/1" {
		t.Errorf("second key = %q, want extent:1/1", got)
	}

	if len(cmps) != 2 {
		t.Fatalf("comparisons = %d, want one per key", len(cmps))
	}
	// The surviving comparison for the reused key must be the create check the
	// first write brought, not the second write's revision check.
	if cmps[0].Target != clientv3.Compare(clientv3.CreateRevision("x"), "=", 0).Target {
		t.Errorf("comparison for the reused key was replaced by a later write's")
	}

	if p.bytes != 8192 {
		t.Errorf("buffered bytes = %d, want 8192", p.bytes)
	}
	if p.empty() {
		t.Error("buffer reports empty with two writes in it")
	}
	if plans := p.reset(); plans != nil || !p.empty() {
		t.Error("reset left the buffer non-empty")
	}
}
