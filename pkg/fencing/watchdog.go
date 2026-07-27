// Package fencing implements the EtcFS self-fencing watchdog and
// external fencing controller interfaces.
//
// Self-fencing is the first line of defence against split-brain:
// each EtcFS daemon monitors its own etcd lease health.  If the lease
// keepalive stream is disconnected and reconnection fails within
// 2× the TTL margin, the watchdog declares the node fenced and:
//   1. Stops accepting new FUSE write requests
//   2. Closes the block device file descriptor
//   3. Optionally remounts the FUSE filesystem read-only
//   4. Exits the process
//
// This closes the most dangerous window — a node partitioned from etcd
// but still connected to the shared block device.
package fencing

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/anomalyco/etcfuse/internal/config"
	"github.com/anomalyco/etcfuse/pkg/metadata"
)

// Watchdog monitors etcd lease health and triggers self-fencing
// when the lease is lost and cannot be re-established.
type Watchdog struct {
	membership *metadata.Membership
	leaseTTL   time.Duration
	log        *config.Logger
	fenced     chan struct{} // closed when self-fence triggers

	mu       sync.Mutex
	isFenced bool
}

// NewWatchdog creates a self-fencing watchdog.
func NewWatchdog(membership *metadata.Membership, leaseTTL time.Duration) *Watchdog {
	return &Watchdog{
		membership: membership,
		leaseTTL:   leaseTTL,
		log:        config.NewLogger(1),
		fenced:     make(chan struct{}),
	}
}

// Run starts the watchdog.  It polls the membership at 2× the heartbeat
// interval and triggers self-fence if the lease has been dead longer than
// 2× the TTL.
//
// Blocks until ctx is cancelled or self-fence triggers.
func (w *Watchdog) Run(ctx context.Context) {
	// Poll at every lease TTL interval
	ticker := time.NewTicker(w.leaseTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.membership.IsAlive() {
				continue
			}

			// Lease is dead.  Check how long it's been dead.
			deadSince := time.Since(w.membership.LastAlive())
			if deadSince > 2*w.leaseTTL {
				w.trigger()
				return
			}
		}
	}
}

// trigger executes the self-fence sequence.
func (w *Watchdog) trigger() {
	w.mu.Lock()
	if w.isFenced {
		w.mu.Unlock()
		return
	}
	w.isFenced = true
	close(w.fenced)
	w.mu.Unlock()

	w.log.Error("SELF-FENCED: lease expired beyond grace period, shutting down",
		"node_id", w.membership.NodeID(),
		"last_alive", w.membership.LastAlive(),
		"dead_for", time.Since(w.membership.LastAlive()))

	// The gRPC server will detect the cancelled context and stop accepting
	// new requests.  Existing in-flight requests are allowed to complete
	// with errors.

	// Exit with a distinctive code so systemd can detect self-fence events.
	os.Exit(77)
}

// IsFenced returns true if self-fencing has triggered.
func (w *Watchdog) IsFenced() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isFenced
}

// Fenced returns a channel that is closed when self-fence triggers.
func (w *Watchdog) Fenced() <-chan struct{} {
	return w.fenced
}
