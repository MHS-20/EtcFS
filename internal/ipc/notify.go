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
// kernel's own caches goes over this socket for it to call.  Two messages:
//
//	1  INVAL_ENTRY  [u32:1][u64:parent][name]  — a dentry another node changed
//	2  INVAL_INODE  [u32:2][u64:ino]           — the data pages of one inode
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

	// notifyAckTimeout bounds how long a lock release waits for the kernel to
	// drop an inode's pages.  Generous, because the call is against the kernel
	// rather than the network, and a release that gives up early would yield the
	// inode with stale pages still cached.
	notifyAckTimeout = 5 * time.Second
)

// errNoNotifyClient reports that no C daemon is connected to be told about an
// invalidation.
var errNoNotifyClient = errors.New("no cache-invalidation client is connected")

type notifyServer struct {
	mu   sync.Mutex
	conn net.Conn
}

// send writes one message and, when ack is set, waits for the C daemon to
// confirm it has been carried out.
func (n *notifyServer) send(msg []byte, ack bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn == nil {
		return errNoNotifyClient
	}

	var b [1]byte
	err := func() error {
		if _, werr := n.conn.Write(msg); werr != nil {
			return werr
		}
		if !ack {
			return nil
		}
		if derr := n.conn.SetReadDeadline(time.Now().Add(notifyAckTimeout)); derr != nil {
			return derr
		}
		_, rerr := n.conn.Read(b[:])
		return rerr
	}()
	if err == nil && ack {
		if b[0] != 0 {
			// The stream is still in step — the client answered — so the
			// connection stays up and only this call failed.
			return errors.New("the client reported the invalidation failed")
		}
	}
	if err != nil {
		// The stream carries a reply per acknowledged message, so a connection
		// that has failed part way through one can no longer be read in step.
		_ = n.conn.Close()
		n.conn = nil
	}
	return err
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

func (s *Service) StartNotificationServer(ctx context.Context) {
	prefix := metadata.PrefixDirent
	ch := s.store.Watch(ctx, prefix, clientv3.WithPrefix())

	go func() {
		for resp := range ch {
			for _, ev := range resp.Events {
				key := string(ev.Kv.Key)
				parts := strings.SplitN(key[len(prefix):], "/", 2)
				if len(parts) != 2 {
					continue
				}
				s.sendInvalEntry(parts[0], parts[1])
			}
		}
	}()
}

func (s *Service) sendInvalEntry(parent, name string) {
	var parentIno uint64
	_, _ = fmt.Sscanf(parent, "%d", &parentIno)
	if parentIno == 0 {
		return
	}
	s.log.Info("notify: inval_entry", "parent", parentIno, "name", name)

	buf := make([]byte, 12+len(name))
	binary.BigEndian.PutUint32(buf[0:4], notifyInvalEntry)
	binary.BigEndian.PutUint64(buf[4:12], parentIno)
	copy(buf[12:], name)
	if err := s.notifyServer.send(buf, false); err != nil && !errors.Is(err, errNoNotifyClient) {
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
func (s *Service) invalidatePages(ino uint64) error {
	if !s.pagesCacheable() {
		return nil
	}
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], notifyInvalInode)
	binary.BigEndian.PutUint64(buf[4:12], ino)
	if err := s.notifyServer.send(buf, true); err != nil {
		return fmt.Errorf("invalidate kernel pages for ino %d: %w", ino, err)
	}
	return nil
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
