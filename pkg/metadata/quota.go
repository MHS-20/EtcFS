package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// QuotaRecord is the JSON value stored at quota:<ino>. A zero field means that
// dimension is unlimited, so a root can cap bytes without capping file count.
type QuotaRecord struct {
	Bytes  uint64 `json:"bytes"`
	Inodes uint64 `json:"inodes"`
}

// SetQuota marks a directory as a quota root with the given limits.
//
// Only a directory may be one: the limits apply to a subtree, and a file has
// no subtree. The check is a comparison inside the transaction rather than a
// read beforehand, so a directory removed in between cannot leave a quota key
// behind pointing at nothing.
func (s *Store) SetQuota(ctx context.Context, ino uint64, limits QuotaRecord) error {
	rec, err := s.GetInode(ctx, ino)
	if err != nil {
		return fmt.Errorf("set quota on %d: %w", ino, err)
	}
	if rec == nil {
		return ErrNotFound
	}
	if rec.Mode&S_IFMT != ModeDir {
		return ErrNotDir
	}

	value, err := json.Marshal(limits)
	if err != nil {
		return fmt.Errorf("set quota on %d: %w", ino, err)
	}
	ok, err := s.Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(InodeKey(ino)), ">", 0)},
		[]clientv3.Op{clientv3.OpPut(QuotaKey(ino), string(value))}, nil)
	if err != nil {
		return fmt.Errorf("set quota on %d: %w", ino, err)
	}
	if !ok {
		return ErrNotFound
	}
	return nil
}

// ClearQuota removes a directory's quota root marking.
func (s *Store) ClearQuota(ctx context.Context, ino uint64) error {
	if err := s.Delete(ctx, QuotaKey(ino)); err != nil {
		return fmt.Errorf("clear quota on %d: %w", ino, err)
	}
	return nil
}

// ListQuotas returns every quota root and its limits.
func (s *Store) ListQuotas(ctx context.Context) (map[uint64]QuotaRecord, error) {
	kvs, err := s.GetPrefix(ctx, PrefixQuota)
	if err != nil {
		return nil, fmt.Errorf("list quotas: %w", err)
	}
	out := make(map[uint64]QuotaRecord, len(kvs))
	for _, kv := range kvs {
		ino, ok := ParseQuotaKey(string(kv.Key))
		if !ok {
			continue
		}
		var rec QuotaRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		out[ino] = rec
	}
	return out, nil
}
