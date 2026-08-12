package verify

import (
	"fmt"
	"maps"
	"sort"

	"github.com/anishathalye/porcupine"
)

// The namespace model: one directory is a map from name to inode number, and
// every operation either reads it or changes it.
//
// Partitioning by parent directory is what keeps the search tractable —
// operations on different directories cannot constrain each other's order.
// Rename is the exception, since it touches two directories at once, and it is
// handled by placing it in both partitions with the half that partition can see
// (see splitRename).

// dirState is the contents of one directory. Values are inode numbers; the
// zero value is a directory that does not exist yet as far as this history
// shows, which is not the same as an empty one.
type dirState struct {
	entries map[string]uint64
	// known records whether the history has established what this directory
	// holds. A history that starts mid-life must not treat "we never saw it"
	// as "it was empty", or the first unlink of a pre-existing file reads as a
	// violation.
	known map[string]bool
}

func newDirState() dirState {
	return dirState{entries: map[string]uint64{}, known: map[string]bool{}}
}

func (d dirState) clone() dirState {
	return dirState{entries: maps.Clone(d.entries), known: maps.Clone(d.known)}
}

func (d dirState) equal(o dirState) bool {
	return maps.Equal(d.entries, o.entries) && maps.Equal(d.known, o.known)
}

// step applies one operation to a directory, reporting whether the recorded
// outcome is possible from this state.
//
// An operation whose outcome is not determined by the namespace alone —
// EACCES from a permission check, EIO from a fenced node, ENOSPC — is accepted
// without constraining the state: the model describes the namespace, not the
// whole filesystem, and treating an unmodelled failure as impossible would
// report the model's own gaps as violations.
func (d dirState) step(op Op) (bool, dirState) {
	name := op.Name
	if op.Kind == KindRename {
		name = op.NewName
	}
	ino, present := d.entries[name]
	// An entry the history proves exists without saying which inode it names
	// matches whatever a later read reports.
	matches := func(want uint64) bool { return ino == unknownIno || ino == want }

	switch op.Kind {
	case KindReaddir:
		return d.stepReaddir(op)

	case KindLookup:
		switch op.Errno {
		case 0:
			// The name has to be there, and to name what was found.
			if d.known[name] && !present {
				return false, d
			}
			if present && !matches(op.Ino) {
				return false, d
			}
			return true, d.with(name, op.Ino)
		case errnoENOENT:
			if present {
				return false, d
			}
			return true, d.without(name)
		}
		return true, d

	case KindCreate, KindMkdir, KindMknod, KindSymlnk, KindLink, KindRename:
		switch op.Errno {
		case 0:
			// A name that is taken cannot be created again; replacing it is
			// rename's job, and a rename's destination is allowed to exist.
			if present && op.Kind != KindRename {
				return false, d
			}
			// A rename's response carries no inode number, so the name is
			// recorded as taken by something unidentified.
			made := op.Ino
			if made == 0 {
				made = unknownIno
			}
			next := d.with(name, made)
			if op.Kind == KindRename && op.Parent == op.NewParent && op.Name != op.NewName {
				next = next.without(op.Name)
			}
			return true, next
		case errnoEEXIST:
			if d.known[name] && !present {
				return false, d
			}
			return true, d.with(name, unknownIno)
		}
		return true, d

	case KindUnlink, KindRmdir:
		switch op.Errno {
		case 0:
			if d.known[name] && !present {
				return false, d
			}
			return true, d.without(name)
		case errnoENOENT:
			if present {
				return false, d
			}
			return true, d.without(name)
		}
		return true, d
	}
	return true, d
}

// stepReaddir applies one page of a directory listing.
//
// A readdir is paginated, so a response is a window into the listing rather
// than the listing itself, and treating it as complete would report a
// perfectly ordinary large directory as missing half its entries. What makes
// it useful anyway is that the entries come straight out of an etcd prefix
// scan and are therefore a *contiguous run in sorted order*: every name that
// sorts between the first and last of a page must be on that page. A name the
// model knows exists and that falls inside that range, but is absent from the
// page, has been dropped by the listing.
//
// Nothing is concluded about names sorting past the end of the page: they are
// on a later page this response says nothing about. The one exception is a
// page that starts at offset 0, whose first entry is the smallest name in the
// whole directory -- so a known name sorting before it cannot exist either.
func (d dirState) stepReaddir(op Op) (bool, dirState) {
	if op.Errno != 0 {
		return true, d
	}

	listed := make(map[string]uint64, len(op.Entries))
	for _, e := range op.Entries {
		listed[e.Name] = e.Ino
	}

	// Every name the page returned has to exist, and to be the inode the page
	// says it is.
	next := d
	for _, e := range op.Entries {
		if ino, present := next.entries[e.Name]; present && ino != unknownIno && ino != e.Ino {
			return false, d
		}
		if next.known[e.Name] && !hasName(next, e.Name) {
			return false, d
		}
		next = next.with(e.Name, e.Ino)
	}

	// An empty page at offset 0 is a directory with nothing in it at all; one
	// at any other offset only says the listing has run out, which says
	// nothing about what came before it.
	if len(op.Entries) == 0 {
		if op.Offset == 0 && len(d.entries) > 0 {
			return false, d
		}
		return true, next
	}

	lo := op.Entries[0].Name
	hi := op.Entries[len(op.Entries)-1].Name
	for name := range d.entries {
		if _, ok := listed[name]; ok {
			continue
		}
		switch {
		case name > lo && name < hi:
			// Inside the contiguous run the page covers, and missing from it.
			return false, d
		case op.Offset == 0 && name < lo:
			// Nothing can sort before the first entry of the whole directory.
			return false, d
		}
	}
	return true, next
}

// hasName reports whether the state records this name as present.
func hasName(d dirState, name string) bool {
	_, present := d.entries[name]
	return present
}

// with records that a name is taken, by ino when the history says which.
func (d dirState) with(name string, ino uint64) dirState {
	next := d.clone()
	next.entries[name] = ino
	next.known[name] = true
	return next
}

// without records that a name is free.
func (d dirState) without(name string) dirState {
	next := d.clone()
	delete(next.entries, name)
	next.known[name] = true
	return next
}

const (
	errnoENOENT = 2
	errnoEEXIST = 17
	// unknownIno stands for an entry the history proves exists without ever
	// saying which inode it names.
	unknownIno = ^uint64(0)
)

// NamespaceModel is the sequential specification of one directory.
var NamespaceModel = porcupine.Model{
	Partition: partitionByDirectory,
	Init:      func() interface{} { return newDirState() },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		return state.(dirState).step(input.(Op))
	},
	Equal: func(a, b interface{}) bool {
		return a.(dirState).equal(b.(dirState))
	},
	DescribeOperation: func(input, output interface{}) string {
		op := input.(Op)
		if op.Kind == KindRename {
			return fmt.Sprintf("%s(%d/%q → %d/%q) -> errno %d",
				op.Kind, op.Parent, op.Name, op.NewParent, op.NewName, op.Errno)
		}
		if op.Kind == KindReaddir {
			return fmt.Sprintf("readdir(%d, off %d) -> %d entries, errno %d",
				op.Parent, op.Offset, len(op.Entries), op.Errno)
		}
		return fmt.Sprintf("%s(%d/%q) -> ino %d, errno %d", op.Kind, op.Parent, op.Name, op.Ino, op.Errno)
	},
}

// partitionByDirectory groups operations by the directory they act on. A
// rename across two directories is split into the half each side observes: the
// source loses a name, the destination gains one. Splitting it is weaker than
// checking the two together — it cannot catch a rename that is atomic in
// neither direction — and it is what keeps every other directory's check
// independent. A rename within one directory stays whole.
func partitionByDirectory(h []porcupine.Operation) [][]porcupine.Operation {
	byDir := map[uint64][]porcupine.Operation{}
	for _, o := range h {
		op := o.Input.(Op)
		if op.Kind == KindRename && op.Parent != op.NewParent {
			src, dst := splitRename(op)
			byDir[op.Parent] = append(byDir[op.Parent], withInput(o, src))
			byDir[op.NewParent] = append(byDir[op.NewParent], withInput(o, dst))
			continue
		}
		byDir[op.Parent] = append(byDir[op.Parent], o)
	}

	dirs := make([]uint64, 0, len(byDir))
	for d := range byDir {
		dirs = append(dirs, d)
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i] < dirs[j] })

	out := make([][]porcupine.Operation, 0, len(dirs))
	for _, d := range dirs {
		out = append(out, byDir[d])
	}
	return out
}

// splitRename expresses a cross-directory rename as what each end of it sees:
// the source directory loses the old name, the destination gains the new one.
func splitRename(op Op) (src, dst Op) {
	src = op
	src.Kind = KindUnlink
	src.NewParent, src.NewName = 0, ""

	// The destination sees a name appear, by something it cannot name — the
	// rename's response carries no inode number — and possibly over a file
	// that was already there, which is a rename's privilege and no other
	// operation's. It stays a rename for exactly that reason.
	dst = op
	dst.Parent, dst.Name = op.NewParent, op.NewName
	dst.NewParent, dst.NewName = op.NewParent, op.NewName
	dst.Ino = unknownIno
	return src, dst
}

func withInput(o porcupine.Operation, in Op) porcupine.Operation {
	o.Input = in
	return o
}
