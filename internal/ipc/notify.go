package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

type notifyServer struct {
	mu   sync.Mutex
	conn net.Conn
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

	n := s.notifyServer
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conn == nil {
		return
	}
	buf := make([]byte, 12+len(name))
	binary.BigEndian.PutUint32(buf[0:4], 1)
	binary.BigEndian.PutUint64(buf[4:12], parentIno)
	copy(buf[12:], []byte(name))
	_, _ = n.conn.Write(buf)
}

func (s *Service) acceptNotifyConn(conn net.Conn) {
	s.log.Info("notify: client connected", "remote", conn.RemoteAddr())
	n := &notifyServer{conn: conn}
	s.notifyServer = n
	buf := make([]byte, 1)
	for {
		if _, err := conn.Read(buf); err != nil {
			break
		}
	}
}

func (s *Service) RunNotifyListener(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("notify accept: %w", err)
		}
		go s.acceptNotifyConn(conn)
	}
}

func StartNotifyServer(svc *Service, sockPath string) error {
	listener, err := ListenPrivate(sockPath)
	if err != nil {
		return err
	}
	return svc.RunNotifyListener(listener)
}
