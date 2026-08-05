package metadata

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Membership manages this node's presence in the EtcFS cluster.
//
// On start, it creates a lease-backed membership key in etcd:
//
//	membership:<node_id> = {node_id, cluster_name, joined_at, instance_id}
//
// instance_id is the cloud instance backing this node, recorded so the
// fencing controller can detach the shared volume from a node that has
// already expired and can no longer be asked.  Empty when not running on a
// cloud instance (Docker, bare metal); external fencing then degrades to a
// generation bump without a detach.
//
// The node's liveness is tied to the etcd lease — if the node stops
// sending keepalives, the key is auto-deleted and other nodes (including
// the fencing controller) detect the expiry.
type Membership struct {
	client     *clientv3.Client
	nodeID     string
	cluster    string
	leaseTTL   time.Duration
	instanceID string

	mu        sync.Mutex
	leaseID   clientv3.LeaseID
	alive     bool
	lastAlive time.Time
}

// NewMembership creates a Membership for the given node.
func NewMembership(client *clientv3.Client, nodeID, cluster string, leaseTTL time.Duration) *Membership {
	return &Membership{
		client:   client,
		nodeID:   nodeID,
		cluster:  cluster,
		leaseTTL: leaseTTL,
	}
}

// SetInstanceID records the cloud instance backing this node.  It must be set
// before Run, because the value is written into the membership key and cannot
// be added afterwards — by the time the fencing controller needs it, the key
// has already been deleted by lease expiry.
func (m *Membership) SetInstanceID(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instanceID = id
}

// Run starts the membership heartbeat loop.  Blocks until ctx is cancelled.
func (m *Membership) Run(ctx context.Context) {
	// Grant a lease and register
	leaseID, err := m.grantAndRegister(ctx)
	if err != nil {
		// The watchdog will detect this as a failure.
		return
	}

	// Keepalive loop
	keepCh, err := m.client.KeepAlive(ctx, leaseID)
	if err != nil {
		return
	}

	m.updateAlive()

	for {
		select {
		case <-ctx.Done():
			m.revokeLease(context.Background())
			return
		case resp, ok := <-keepCh:
			if !ok {
				// KeepAlive channel closed — lease may be expired.
				// Try to re-establish.
				newID, err := m.grantAndRegister(ctx)
				if err != nil {
					m.setAlive(false)
					return
				}
				keepCh, err = m.client.KeepAlive(ctx, newID)
				if err != nil {
					m.setAlive(false)
					return
				}
				continue
			}
			if resp != nil {
				m.updateAlive()
			}
		}
	}
}

func (m *Membership) grantAndRegister(ctx context.Context) (clientv3.LeaseID, error) {
	resp, err := m.client.Grant(ctx, int64(m.leaseTTL.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("membership: grant lease: %w", err)
	}

	m.mu.Lock()
	instanceID := m.instanceID
	m.mu.Unlock()
	value := fmt.Sprintf(`{"node_id":"%s","cluster":"%s","joined_at":"%s","instance_id":"%s"}`,
		m.nodeID, m.cluster, time.Now().UTC().Format(time.RFC3339), instanceID)

	_, err = m.client.Put(ctx, MembershipKey(m.nodeID), value, clientv3.WithLease(resp.ID))
	if err != nil {
		_, _ = m.client.Revoke(ctx, resp.ID)
		return 0, fmt.Errorf("membership: put key: %w", err)
	}

	m.mu.Lock()
	m.leaseID = resp.ID
	m.alive = true
	m.lastAlive = time.Now()
	m.mu.Unlock()

	return resp.ID, nil
}

func (m *Membership) revokeLease(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leaseID != 0 {
		_, _ = m.client.Revoke(ctx, m.leaseID)
		m.leaseID = 0
	}
	m.alive = false
}

func (m *Membership) updateAlive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive = true
	m.lastAlive = time.Now()
}

func (m *Membership) setAlive(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive = v
}

// IsAlive reports whether this node's membership lease is currently active.
//
// Being "alive" requires two things: the keepalive loop has not observed a
// terminal failure, *and* the last successful keepalive is recent enough that
// etcd cannot already have expired the lease server-side.
//
// The second condition is not redundant.  Under a total network partition the
// etcd client's KeepAlive channel is never closed — the client retries
// indefinitely and reports nothing — so the loop in Run never reaches the
// reconnect path that clears the flag.  Without a deadline check a partitioned
// node believes itself alive forever, the self-fencing watchdog never fires,
// and the node keeps serving while etcd has already handed its lease's
// expiry to the fencing controller.  Verified: 8+ minutes partitioned with no
// self-fence before this check existed (docs/chaos-reports/
// 2026-08-05-fault-injection-during-join-leave.md).
//
// The lease TTL is the correct threshold because it is exactly the point at
// which etcd expires the lease after the last renewal.  The client library
// renews at roughly TTL/3, so a healthy node has ~3x margin and this cannot
// produce a false positive from ordinary jitter.
func (m *Membership) IsAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.alive || m.lastAlive.IsZero() {
		return false
	}
	return time.Since(m.lastAlive) <= m.leaseTTL
}

// LastAlive returns the time of the last successful keepalive.
func (m *Membership) LastAlive() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastAlive
}

// NodeID returns this node's identifier.
func (m *Membership) NodeID() string {
	return m.nodeID
}

// LeaseTTL returns the configured lease TTL.
func (m *Membership) LeaseTTL() time.Duration {
	return m.leaseTTL
}

// InstanceIDFromMembership extracts instance_id from a membership key's value.
//
// The value is hand-rolled JSON (see grantAndRegister), so this is a
// deliberately narrow scan rather than a full unmarshal: it must not fail or
// panic on a value written by an older node that has no instance_id field at
// all, which is exactly the case during a rolling upgrade.  A missing field
// yields "", and the caller treats that as "cannot detach".
func InstanceIDFromMembership(value []byte) string {
	const key = `"instance_id":"`
	s := string(value)
	i := strings.Index(s, key)
	if i < 0 {
		return ""
	}
	rest := s[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}
