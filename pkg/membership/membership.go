package membership

import (
	"context"
	"fmt"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/MHS-20/EtcFS/pkg/metadata"
)

// MetadataStore is the slice of the metadata store membership needs.
type MetadataStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetPrefix(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
	Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error)
	Delete(ctx context.Context, key string) error
	Txn(ctx context.Context, ifs []clientv3.Cmp, thens, elses []clientv3.Op) (bool, error)
}

type Manager struct {
	Store  MetadataStore
	NodeID string
}

func New(store MetadataStore, nodeID string) *Manager {
	return &Manager{Store: store, NodeID: nodeID}
}

func (m *Manager) Join(ctx context.Context) error {
	_ = m.registerMembership(ctx)

	arenas, _ := m.Store.GetPrefix(ctx, metadata.PrefixArena)
	for _, kv := range arenas {
		node := string(kv.Key[len(metadata.PrefixArena):])
		m.registerRecognition(ctx, node)
	}

	return m.AcquireArena(ctx)
}

func (m *Manager) registerMembership(ctx context.Context) error {
	key := metadata.MembershipKey(m.NodeID)
	val := []byte(fmt.Sprintf(`{"joined":%d}`, time.Now().Unix()))
	_, err := m.Store.Put(ctx, key, val)
	return err
}

func (m *Manager) registerRecognition(ctx context.Context, peerID string) {
	key := fmt.Sprintf("peers:%s:%s", m.NodeID, peerID)
	_, _ = m.Store.Put(ctx, key, []byte("known"))
}

func (m *Manager) AcquireArena(ctx context.Context) error {
	key := metadata.PrefixArenaLog

	for attempt := 0; attempt < 5; attempt++ {
		val, err := m.Store.Get(ctx, key)
		if err != nil {
			return err
		}
		current := uint64(0)
		if val != nil {
			current = metadata.DecodeUint64(val)
		}
		next := current + 1

		var cmps []clientv3.Cmp
		if current == 0 {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)}
		} else {
			cmps = []clientv3.Cmp{clientv3.Compare(clientv3.Value(key), "=", string(metadata.EncodeUint64(current)))}
		}
		op := clientv3.OpPut(key, string(metadata.EncodeUint64(next)))
		ok, txErr := m.Store.Txn(ctx, cmps, []clientv3.Op{op}, nil)
		if txErr != nil {
			return txErr
		}
		if ok {
			arenaKey := metadata.ArenaKey(m.NodeID)
			_, _ = m.Store.Put(ctx, arenaKey, metadata.EncodeUint64(current))
			return nil
		}
	}
	return fmt.Errorf("arena acquisition exhausted retries")
}

func (m *Manager) LeaveGraceful(ctx context.Context) error {
	arenas, _ := m.Store.GetPrefix(ctx, metadata.PrefixArena)
	var toRelease []uint64
	for _, kv := range arenas {
		arenaKey := string(kv.Key)
		if arenaKey == metadata.ArenaKey(m.NodeID) {
			id := metadata.DecodeUint64(kv.Value)
			toRelease = append(toRelease, id)
		}
	}
	for _, id := range toRelease {
		m.releaseArena(ctx, id)
	}
	_ = m.Store.Delete(ctx, metadata.MembershipKey(m.NodeID))
	return nil
}

func (m *Manager) releaseArena(ctx context.Context, arenaID uint64) {
	_ = m.Store.Delete(ctx, metadata.ArenaKey(m.NodeID))
	freeKey := fmt.Sprintf("%s%d", metadata.PrefixFreeArena, arenaID)
	_, _ = m.Store.Put(ctx, freeKey, []byte("free"))
}

func (m *Manager) LeaveUngraceful(ctx context.Context) {
	_ = m.LeaveGraceful(ctx)
}

func (m *Manager) RebalanceArena(ctx context.Context, fromNode, toNode string, arenaID uint64) error {
	fromKey := metadata.ArenaKey(fromNode)
	fromVal, _ := m.Store.Get(ctx, fromKey)
	if fromVal == nil {
		return fmt.Errorf("source node %s has no arena registered", fromNode)
	}
	existingID := metadata.DecodeUint64(fromVal)
	if existingID != arenaID {
		return fmt.Errorf("source arena ID mismatch: expected %d, got %d", arenaID, existingID)
	}
	_ = m.Store.Delete(ctx, fromKey)
	toKey := metadata.ArenaKey(toNode)
	_, _ = m.Store.Put(ctx, toKey, metadata.EncodeUint64(arenaID))
	return nil
}

func (m *Manager) IsMember(ctx context.Context, nodeID string) bool {
	val, _ := m.Store.Get(ctx, metadata.MembershipKey(nodeID))
	return val != nil
}

func (m *Manager) ArenaCount(ctx context.Context, nodeID string) int {
	key := metadata.ArenaKey(nodeID)
	val, _ := m.Store.Get(ctx, key)
	if val == nil {
		return 0
	}
	return 1
}
