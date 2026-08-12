package verify

import (
	"time"

	"github.com/anishathalye/porcupine"
)

// Consistency models weaker than linearizability, expressed as transformations
// of the history rather than as changes to the checker.
//
// Linearizability is two things at once: a sequential specification, and a
// real-time order that any valid sequential explanation must respect. Every
// weaker model in this file keeps the first and loosens the second, which
// means the transformation *is* the definition — a reader can see exactly what
// was checked, instead of it being buried in a patched solver. Porcupine
// itself is used unmodified.
//
// The direction matters: moving an operation's invocation earlier can only add
// valid orderings, so a relaxed check never rejects a history the strict check
// accepts. A relaxation that accepted everything would be useless rather than
// wrong, which is why the tests include a history that must fail even relaxed.

// Consistency is the guarantee one operation is entitled to.
type Consistency int

const (
	// Linearizable: the operation takes effect at some instant inside its own
	// call/return interval. EtcFS's namespace operations and every guarded
	// commit are this, because etcd transactions are.
	Linearizable Consistency = iota
	// BoundedStale: a read may return a value that was current up to some
	// bound before it was issued. This is what the FUSE layer's attribute and
	// entry caches give an application: the daemon answers linearizably, but
	// the kernel may answer from its own copy for up to the cache timeout.
	BoundedStale
	// Serializable: some sequential order explains the history, with no
	// constraint from real time at all. Two operations that never overlapped
	// may still be explained in either order.
	Serializable
)

// Classifier decides what guarantee each operation is entitled to. Returning
// Linearizable for everything is the strictest — and correct — choice for a
// history recorded at the daemon's own socket.
type Classifier func(op Op) Consistency

// AllLinearizable is the classifier for a history recorded inside the daemon.
func AllLinearizable(Op) Consistency { return Linearizable }

// ReadsAreCached treats every read as allowed to be out of date by the
// staleness Relax is given, and every mutation as linearizable. It is the
// model for a history recorded above the kernel's attribute and entry caches,
// whose timeout is that staleness.
func ReadsAreCached(op Op) Consistency {
	if op.Kind == KindLookup {
		return BoundedStale
	}
	return Linearizable
}

// Relax rewrites a history so that checking it for linearizability checks the
// weaker guarantee each operation was entitled to.
//
//   - Linearizable operations are untouched.
//   - A BoundedStale operation has its invocation moved back by staleness, so
//     it may be ordered against anything that was current within that window.
//     A staleness of 0 leaves it linearizable; an unbounded one is expressed by
//     passing a duration longer than the history.
//   - A Serializable operation loses its real-time constraint entirely: its
//     invocation moves to the start of the history and its return to the end,
//     so only the sequential specification still binds it.
//
// The returned history is a copy; the input is not modified.
func Relax(h []porcupine.Operation, class Classifier, staleness time.Duration) []porcupine.Operation {
	if len(h) == 0 {
		return nil
	}
	first, last := h[0].Call, h[0].Return
	for _, o := range h {
		if o.Call < first {
			first = o.Call
		}
		if o.Return > last {
			last = o.Return
		}
	}

	out := make([]porcupine.Operation, len(h))
	copy(out, h)
	for i := range out {
		switch class(out[i].Input.(Op)) {
		case Linearizable:
		case BoundedStale:
			out[i].Call -= int64(staleness)
			if out[i].Call < first {
				out[i].Call = first
			}
		case Serializable:
			out[i].Call, out[i].Return = first, last
		}
	}
	return out
}

// Operations converts decoded operations into the form Porcupine checks.
//
// One client per node: Porcupine uses the client only for visualisation, and a
// node is the closest thing to a client this history has, since the daemon
// serves many kernel threads over one socket.
func Operations(ops []Op) []porcupine.Operation {
	clients := map[string]int{}
	out := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		id, seen := clients[op.Node]
		if !seen {
			id = len(clients)
			clients[op.Node] = id
		}
		out = append(out, porcupine.Operation{
			ClientId: id,
			Input:    op,
			Output:   op.Errno,
			Call:     op.Call,
			Return:   op.Ret,
		})
	}
	return out
}

// Check reports whether a history is explained by the model under the
// guarantees the classifier assigns. A timeout of 0 means no limit.
func Check(model porcupine.Model, h []porcupine.Operation, class Classifier,
	staleness, timeout time.Duration) porcupine.CheckResult {

	return porcupine.CheckOperationsTimeout(model, Relax(h, class, staleness), timeout)
}
