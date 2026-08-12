package metadata

import (
	"context"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Fencing control-plane keys: the durable record of a fence that is owed, and
// the claim that stops two controllers doing it at once.
//
// The membership watch that triggers a fence is edge-triggered on a DELETE
// event.  Once it fires the key is gone, so a fence that fails, times out, or
// dies with the controller process has nothing left to re-trigger it — the
// node stays unfenced while the cluster believes nothing is owed.
//
//	fence_pending:<node_id> = <instance_id>
//
// records that intent durably.  It is written before the fence is attempted
// and deleted only after the generation bump succeeds, so anything left behind
// is by definition a fence that did not complete, and the reconciliation sweep
// retries it.  The instance ID is stored because it lives only in the
// membership key's value, which is already deleted by the time a retry runs.
//
//	fence_claim:<node_id> = <fencer node_id>   (lease-bound)
//
// is the cross-node dedup.  Both the watch and the sweep can observe the same
// expired node from every survivor at once; the claim's create-CAS lets
// exactly one proceed and the rest skip.  It is lease-bound rather than
// plainly deleted on completion so that a controller which crashes mid-fence
// releases it automatically — otherwise a failed fence would be retried by
// nobody, which is the gap this whole mechanism exists to close.
//
// Both are written raw: they are control-plane state maintained *about* a
// node by its peers, not metadata mutations by the node itself, and a fencing
// controller whose own generation guard rejected them could never fence
// anyone.

func FencePendingKey(nodeID string) string {
	return fmt.Sprintf("%s%s", PrefixFencePending, nodeID)
}

func FenceClaimKey(nodeID string) string {
	return fmt.Sprintf("%s%s", PrefixFenceClaim, nodeID)
}

// RecordFenceIntent durably records that nodeID is owed a fence.  Idempotent:
// re-recording an existing intent just refreshes the instance ID.
func (s *Store) RecordFenceIntent(ctx context.Context, nodeID, instanceID string) error {
	_, err := s.putRaw(ctx, FencePendingKey(nodeID), []byte(instanceID))
	if err != nil {
		return fmt.Errorf("record fence intent %s: %w", nodeID, err)
	}
	return nil
}

// ClearFenceIntent removes the record of an owed fence.  Called only after a
// confirmed fence, or when the node has re-registered and is no longer owed
// one.
func (s *Store) ClearFenceIntent(ctx context.Context, nodeID string) error {
	if _, err := s.client.Delete(ctx, FencePendingKey(nodeID)); err != nil {
		return fmt.Errorf("clear fence intent %s: %w", nodeID, err)
	}
	return nil
}

// FenceIntentRevision returns the create-revision of a node's fence intent,
// and whether the intent exists at all.
//
// This is the fence's incarnation marker.  etcd keeps a key's create-revision
// stable across overwrites, so re-recording an intent for the same departure
// leaves it unchanged; it moves only when the key has been deleted and made
// again, which happens exactly when the node came back and then left once
// more.  A fence that captures this at its start and re-checks it before each
// irreversible step is therefore comparing incarnations, not liveness -- and
// liveness is not enough, because a node that departs, restarts, and departs
// again is absent at both ends while being a different node in between.
func (s *Store) FenceIntentRevision(ctx context.Context, nodeID string) (int64, bool, error) {
	resp, err := s.client.Get(ctx, FencePendingKey(nodeID))
	if err != nil {
		return 0, false, fmt.Errorf("read fence intent revision %s: %w", nodeID, err)
	}
	if len(resp.Kvs) == 0 {
		return 0, false, nil
	}
	return resp.Kvs[0].CreateRevision, true, nil
}

// ListFenceIntents returns every node currently owed a fence, mapped to the
// instance ID recorded for it (empty where none was known).
func (s *Store) ListFenceIntents(ctx context.Context) (map[string]string, error) {
	kvs, err := s.GetPrefix(ctx, PrefixFencePending)
	if err != nil {
		return nil, fmt.Errorf("list fence intents: %w", err)
	}
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key[len(PrefixFencePending):])] = string(kv.Value)
	}
	return out, nil
}

func FenceDoneKey(nodeID string) string {
	return fmt.Sprintf("%s%s", PrefixFenceDone, nodeID)
}

// MarkFenceComplete records that nodeID has been fenced, at the generation the
// fence bumped it to.
//
// The reconciliation sweep is authoritative — it fences any known node missing
// from membership, whether or not a watch event was ever seen — so it needs to
// tell "departed and already fenced" from "departed and still owed a fence".
// A completed fence leaves nothing else behind that says so: the intent is
// cleared, and the generation alone cannot distinguish a node fenced now from
// one fenced during an earlier departure it has since recovered from.
//
// The mark is cleared when the node re-registers, so a node that leaves twice
// is fenced twice.
func (s *Store) MarkFenceComplete(ctx context.Context, nodeID string, gen uint64) error {
	_, err := s.putRaw(ctx, FenceDoneKey(nodeID), []byte(strconv.FormatUint(gen, 10)))
	if err != nil {
		return fmt.Errorf("mark fence complete %s: %w", nodeID, err)
	}
	return nil
}

// ClearFenceMark forgets that a node was fenced, so its next departure is
// fenced again.  Called when the node is seen alive in membership.
func (s *Store) ClearFenceMark(ctx context.Context, nodeID string) error {
	if _, err := s.client.Delete(ctx, FenceDoneKey(nodeID)); err != nil {
		return fmt.Errorf("clear fence mark %s: %w", nodeID, err)
	}
	return nil
}

// ListFencedNodes returns every node currently marked as fenced.
func (s *Store) ListFencedNodes(ctx context.Context) (map[string]bool, error) {
	kvs, err := s.GetPrefix(ctx, PrefixFenceDone)
	if err != nil {
		return nil, fmt.Errorf("list fenced nodes: %w", err)
	}
	out := make(map[string]bool, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key[len(PrefixFenceDone):])] = true
	}
	return out, nil
}

// ListKnownNodes returns every node that has ever started, taken from the
// gen:<node> keys each one creates before serving anything.  It is the set the
// sweep compares against live membership.
func (s *Store) ListKnownNodes(ctx context.Context) ([]string, error) {
	kvs, err := s.GetPrefix(ctx, PrefixGen)
	if err != nil {
		return nil, fmt.Errorf("list known nodes: %w", err)
	}
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, string(kv.Key[len(PrefixGen):]))
	}
	return out, nil
}

// ClaimFence attempts to become the single controller fencing nodeID.  Returns
// the lease backing the claim and whether it was won; a lost claim is not an
// error, it means another controller is already on it.
//
// The lease is deliberately left to expire on its own rather than kept alive:
// ttl only has to exceed a fence attempt's own bounded duration, and a claim
// that outlives its holder's crash by at most ttl is precisely what makes the
// sweep able to retry.
func (s *Store) ClaimFence(ctx context.Context, nodeID string, ttl time.Duration) (clientv3.LeaseID, bool, error) {
	leaseID, err := s.GrantLease(ctx, ttl)
	if err != nil {
		return 0, false, fmt.Errorf("claim fence %s: %w", nodeID, err)
	}
	key := FenceClaimKey(nodeID)
	won, err := s.txnRaw(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)},
		[]clientv3.Op{clientv3.OpPut(key, s.nodeID, clientv3.WithLease(leaseID))}, nil)
	if err != nil || !won {
		_ = s.RevokeLease(ctx, leaseID)
		if err != nil {
			return 0, false, fmt.Errorf("claim fence %s: %w", nodeID, err)
		}
		return 0, false, nil
	}
	return leaseID, true, nil
}

// ReleaseFenceClaim drops a claim won by ClaimFence.  Revoking the lease
// removes the key with it.
func (s *Store) ReleaseFenceClaim(ctx context.Context, leaseID clientv3.LeaseID) error {
	return s.RevokeLease(ctx, leaseID)
}
