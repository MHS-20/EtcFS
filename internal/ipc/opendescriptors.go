package ipc

import "sync"

// openFiles counts this node's open descriptors per inode, so that unlinking
// the last name of a file something still has open does not delete it out from
// under the descriptor — POSIX's rule that a file lives until its last name and
// its last descriptor are both gone.
//
// Node-local on purpose. A descriptor is a property of the process that opened
// it, so the count cannot be shared, and the inode's own record carries the
// orphan marker that lets a peer see the file is unreachable. What this tracks
// is only the half this node is responsible for finishing.
type openFiles struct {
	mu    sync.Mutex
	count map[uint64]int

	// orphaned names the inodes this node unlinked while still holding them
	// open: the last release deletes the record.
	orphaned map[uint64]bool
}

func newOpenFiles() *openFiles {
	return &openFiles{count: make(map[uint64]int), orphaned: make(map[uint64]bool)}
}

// retain records one more open descriptor for an inode on this node.
func (o *openFiles) retain(ino uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.count[ino]++
}

// release drops one descriptor and reports whether the inode was the last one
// standing for a file that has already lost its name.
func (o *openFiles) release(ino uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.count[ino] > 1 {
		o.count[ino]--
		return false
	}
	delete(o.count, ino)
	if o.orphaned[ino] {
		delete(o.orphaned, ino)
		return true
	}
	return false
}

// heldOpen reports whether this node still has the inode open, and remembers
// that its deletion is now this node's responsibility.  Called from inside the
// unlink transaction's planning, under the same lock a release takes, so a
// close racing the unlink cannot leave the record with nobody to remove it.
func (o *openFiles) heldOpen(ino uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.count[ino] == 0 {
		return false
	}
	o.orphaned[ino] = true
	return true
}
