package metadata

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Membership manages this node's presence in the EtcFS cluster.
//
// On start, it creates a lease-backed membership key in etcd:
//
//	membership:<node_id> = {node_id, cluster_name, joined_at, address}
//
// The node's liveness is tied to the etcd lease — if the node stops
// sending keepalives, the key is auto-deleted and other nodes (including
// the fencing controller) detect the expiry.
type Membership struct {
	client   *clientv3.Client
	nodeID   string
	cluster  string
	leaseTTL time.Duration

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

	value := fmt.Sprintf(`{"node_id":"%s","cluster":"%s","joined_at":"%s"}`,
		m.nodeID, m.cluster, time.Now().UTC().Format(time.RFC3339))

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

// IsAlive returns true if the membership lease is currently active.
func (m *Membership) IsAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alive
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
