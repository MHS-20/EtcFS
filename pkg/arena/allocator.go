package arena

// Allocator manages block allocation within this node's arenas.
// Phase 6: full implementation with etcd-backed arena lease management.
type Allocator struct {
	nodeID string
}

// ArenaSizeBytes is the default arena size (1 GiB).
const ArenaSizeBytes = 1 << 30

// NewAllocator creates an Allocator for the given node.
func NewAllocator(nodeID string) *Allocator {
	return &Allocator{nodeID: nodeID}
}

// NodeID returns the owning node.
func (a *Allocator) NodeID() string {
	return a.nodeID
}
