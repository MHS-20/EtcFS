package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// Kernel cache invalidation.
//
// The C daemon holds the FUSE session handle, so anything that has to reach the
// kernel's own caches goes over this socket for it to call.  Every message is
// one fixed header, then a name of the length that header declares:
//
//	[u32:type][u64:ino][u32:name_len][name]
//
//	1  INVAL_ENTRY  a dentry another node changed; ino is the parent
//	2  INVAL_INODE  the data pages of one inode; name_len is 0
//
// The length is not decoration.  This is a stream socket, so a reader that has
// to recover the name as "whatever arrived with the header" gets it right only
// while exactly one message is in flight: two written back to back share a read,
// the second is swallowed as part of the first one's name, and every header
// after that is taken from the middle of a message.  Nothing recovers from that
// — the acknowledged messages below stop being recognised, the release waiting
// on one times out, and the connection is dropped as unusable.
//
// INVAL_INODE is acknowledged and INVAL_ENTRY is not, because only the first
// has a caller that must not proceed until it has happened: the pages of an
// inode whose lock is being yielded have to be gone before the peer can take it,
// or that peer's writes become invisible to every later read on this node.
//
// ponytail: one socket and one thread in the C daemon, so a slow INVAL_INODE
// delays the entry invalidations queued behind it.  A stale dentry is bounded by
// entry_timeout anyway, and the delay is bounded by the same recall the peer is
// already waiting on.  A second connection is the upgrade if that stops holding.
const (
	notifyInvalEntry = 1
	notifyInvalInode = 2

	// notifyHeaderLen is the fixed part of every message: type, inode and the
	// length of the name that follows.
	notifyHeaderLen = 16

	// notifyMaxName is MAX_NAME_LEN in pkg/fuse/fuse.h, which is NAME_MAX.  A
	// longer name cannot be created through the mount, so one reaching here is a
	// record this daemon did not write; it is dropped rather than framed, since
	// the C side rejects the frame by closing a connection that carries every
	// other invalidation too.
	notifyMaxName = 255

	// notifyAckTimeout bounds how long a lock release waits for the kernel to
	// drop an inode's pages.  Generous, because the call is against the kernel
	// rather than the network, and a release that gives up early would yield the
	// inode with stale pages still cached.
	notifyAckTimeout = 5 * time.Second

	// notifyBreakerTrips is how many acknowledgements in a row may time out
	// before the client is treated as unresponsive.  A client that is wedged but
	// still connected answers nothing while keeping the socket open, so without
	// this every lock release pays the full ack timeout, one after another,
	// through a path that is one socket and one thread.
	notifyBreakerTrips = 3

	// notifyBreakerCooldown is how long acknowledged messages fail immediately
	// once the breaker has tripped.  Long enough that a wedged client stops
	// costing anything, short enough that one which recovers is picked up again
	// without operator action.
	notifyBreakerCooldown = 30 * time.Second
)

// errNotifyClientUnresponsive reports that the client has stopped answering
// acknowledged messages.
//
// Deliberately not errNoNotifyClient.  A client that has gone away took its
// FUSE session and its cached pages with it, so there is nothing left to
// invalidate and a lock release may proceed.  A client that is merely wedged
// still has that session, and its pages may still hold what a peer is about to
// overwrite — so the release has to fail rather than assume the cache is gone.
var errNotifyClientUnresponsive = errors.New("the cache-invalidation client is not answering")

// errNoNotifyClient reports that no C daemon is connected to be told about an
// invalidation.
var errNoNotifyClient = errors.New("no cache-invalidation client is connected")

type notifyServer struct {
	mu   sync.Mutex
	conn net.Conn

	// ackTimeout is notifyAckTimeout, as a field so a test can shorten it.
	ackTimeout time.Duration

	// ackTimeouts counts acknowledgements that timed out in a row, across
	// reconnections: a client that wedges, is dropped, reconnects and wedges
	// again is the case the breaker exists for, and a counter reset by the
	// reconnection would never reach its limit.  Any successful ack clears it.
	ackTimeouts int
	brokenUntil time.Time
}

// notifyMsg frames one notification, header and name together, so that it
// reaches the socket as a single write and the reader can take it apart without
// guessing where it ends.
func notifyMsg(typ uint32, ino uint64, name string) []byte {
	b := make([]byte, notifyHeaderLen+len(name))
	binary.BigEndian.PutUint32(b[0:4], typ)
	binary.BigEndian.PutUint64(b[4:12], ino)
	binary.BigEndian.PutUint32(b[12:16], uint32(len(name)))
	copy(b[notifyHeaderLen:], name)
	return b
}

// send writes one message and, when ack is set, waits for the C daemon to
// confirm it has been carried out.
func (n *notifyServer) send(msg []byte, ack bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn == nil {
		return errNoNotifyClient
	}
	if ack && time.Now().Before(n.brokenUntil) {
		return errNotifyClientUnresponsive
	}

	var b [1]byte
	err := func() error {
		if _, werr := n.conn.Write(msg); werr != nil {
			return werr
		}
		if !ack {
			return nil
		}
		if derr := n.conn.SetReadDeadline(time.Now().Add(n.ackTimeoutOrDefault())); derr != nil {
			return derr
		}
		_, rerr := n.conn.Read(b[:])
		return rerr
	}()
	if err == nil && ack && b[0] != 0 {
		// The stream is still in step — the client answered — so the connection
		// stays up and only this call failed.
		return errors.New("the client reported the invalidation failed")
	}
	if err != nil {
		// The stream carries a reply per acknowledged message, so a connection
		// that has failed part way through one can no longer be read in step.
		// It is reported as a lost client rather than as an I/O error, because
		// what the caller has to know is that there is nobody there — not which
		// syscall said so.
		_ = n.conn.Close()
		n.conn = nil

		var netErr net.Error
		if ack && errors.As(err, &netErr) && netErr.Timeout() {
			n.ackTimeouts++
			if n.ackTimeouts >= notifyBreakerTrips {
				n.brokenUntil = time.Now().Add(notifyBreakerCooldown)
				return fmt.Errorf("%w: %d acknowledgements in a row timed out",
					errNotifyClientUnresponsive, n.ackTimeouts)
			}
		}
		return fmt.Errorf("%w: %v", errNoNotifyClient, err)
	}
	if ack {
		n.ackTimeouts = 0
	}
	return nil
}

func (n *notifyServer) ackTimeoutOrDefault() time.Duration {
	if n.ackTimeout > 0 {
		return n.ackTimeout
	}
	return notifyAckTimeout
}

// set installs a new connection, closing whatever it replaces.
func (n *notifyServer) set(conn net.Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn != nil {
		_ = n.conn.Close()
	}
	n.conn = conn
}

func (n *notifyServer) connected() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.conn != nil
}

// StartNotificationServer watches every dirent in the cluster and tells the
// kernel about each change, so a name created or removed on one node stops
// being cached on the others.
func (s *Service) StartNotificationServer(ctx context.Context) {
	go s.watchDirents(ctx, func() clientv3.WatchChan {
		return s.store.Watch(ctx, metadata.PrefixDirent, clientv3.WithPrefix())
	})
}

// rewatchDelay keeps a watch that fails immediately and repeatedly from
// becoming a busy loop against etcd.  Short enough that the gap it adds stays
// well inside the entry timeout that bounds the staleness anyway.
const rewatchDelay = 100 * time.Millisecond

// watchDirents turns every dirent change into an invalidation, re-opening the
// watch each time it ends.
//
// The re-opening matters more than it looks: this is the only thing that
// invalidates a cached name, a cached absence of a name, and a cached directory
// listing, and etcd ends a watch for reasons that have nothing to do with this
// process stopping — a compaction past the watched revision is the usual one.
// A single drain of the channel used to end the invalidations for the lifetime
// of the daemon while it went on serving happily, with nothing in the logs to
// say so.
//
// A re-opened watch starts from current, so changes during the gap are missed
// rather than replayed.  That is what the entry and attribute timeouts are for:
// they bound how long any of those caches can outlive a notification that never
// came, which is why they stay short whether the watch is healthy or not.
func (s *Service) watchDirents(ctx context.Context, open func() clientv3.WatchChan) {
	prefix := metadata.PrefixDirent
	for ctx.Err() == nil {
		for resp := range open() {
			for _, ev := range resp.Events {
				key := string(ev.Kv.Key)
				parts := strings.SplitN(key[len(prefix):], "/", 2)
				if len(parts) != 2 {
					continue
				}
				s.sendInvalEntry(parts[0], parts[1])
			}
		}
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("the dirent watch ended; re-establishing it. Names changed " +
			"elsewhere in the gap stay cached on this node until they time out")
		select {
		case <-ctx.Done():
			return
		case <-time.After(rewatchDelay):
		}
	}
}

func (s *Service) sendInvalEntry(parent, name string) {
	var parentIno uint64
	_, _ = fmt.Sscanf(parent, "%d", &parentIno)
	if parentIno == 0 {
		return
	}
	if len(name) > notifyMaxName {
		s.log.Warn("notify: dirent name too long to invalidate; it stays cached on this node until it times out",
			"parent", parentIno, "len", len(name))
		return
	}
	s.log.Info("notify: inval_entry", "parent", parentIno, "name", name)

	if err := s.notifyServer.send(notifyMsg(notifyInvalEntry, parentIno, name), false); err != nil &&
		!errors.Is(err, errNoNotifyClient) {
		s.log.Warn("notify: inval_entry not delivered", "parent", parentIno, "name", name, "error", err)
	}
}

// invalidatePages drops the kernel's cached data pages for an inode and waits
// for that to have happened.
//
// It is called before the inode's lock key is given up, and its failure stops
// the release: a peer that took the inode while this node still had its pages
// would have its writes hidden behind them, with nothing to expire since a page
// cache has no timeout.  Making the peer wait is the safe direction to fail in,
// which is the same choice the flush makes.
//
// A no-op when nothing could have been cached — page caching switched off, or
// no open ever told the kernel it could cache this filesystem's data.
//
// A client that has gone away is also a no-op, and deliberately so: the client
// is the FUSE session, so its pages died with it and there is nothing left to
// invalidate.  Refusing to yield in that case would leave every inode this node
// had cached locked against the whole cluster until the process exited, which is
// an outage in exchange for invalidating a cache that no longer exists.  A
// client that is still there and reports the invalidation failed is the real
// failure, and that one does stop the release.
func (s *Service) invalidatePages(ino uint64) error {
	if !s.pagesCacheable() {
		// Recorded rather than passed over in silence: the obligation is
		// discharged here too, and a history that simply omits the event is
		// indistinguishable from one where the invalidation was skipped while
		// pages *were* cached.  The checker cannot tell "nothing to do" from
		// "not done" unless the daemon says which it was.
		now := time.Now()
		s.recordPageInval(ino, pageInvalNotCached, now, now)
		return nil
	}
	call := time.Now()
	err := s.notifyServer.send(notifyMsg(notifyInvalInode, ino, ""), true)
	switch {
	case err == nil:
		s.recordPageInval(ino, pageInvalDone, call, time.Now())
		return nil
	case errors.Is(err, errNoNotifyClient):
		s.recordPageInval(ino, pageInvalNoClient, call, time.Now())
		// Nothing can be cached again until a client reconnects and an open is
		// answered, and that open sets the flag afresh.
		s.pagesCached.Store(false)
		s.log.Warn("no client to invalidate kernel pages; its FUSE session and their pages are gone with it",
			"ino", ino, "error", err)
		return nil
	default:
		s.recordPageInval(ino, pageInvalFailed, call, time.Now())
		return fmt.Errorf("invalidate kernel pages for ino %d: %w", ino, err)
	}
}

func (s *Service) acceptNotifyConn(conn net.Conn) {
	s.log.Info("notify: client connected", "remote", conn.RemoteAddr())
	s.notifyServer.set(conn)
}

func (s *Service) RunNotifyListener(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("notify accept: %w", err)
		}
		s.acceptNotifyConn(conn)
	}
}

func StartNotifyServer(svc *Service, sockPath string) error {
	listener, err := ListenPrivate(sockPath)
	if err != nil {
		return err
	}
	return svc.RunNotifyListener(listener)
}
