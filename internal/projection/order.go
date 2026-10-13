// Package projection turns the fact archive into projected records
// (PROTOCOL §7.2).
package projection

// Node is a causal node's ordering data.
type Node struct {
	ID             string
	NormalizedTime int64
}

// Less is the ready-set priority of PROTOCOL §6.9: (normalized time, ID bytewise).
func Less(a, b Node) bool {
	if a.NormalizedTime != b.NormalizedTime {
		return a.NormalizedTime < b.NormalizedTime
	}
	return a.ID < b.ID
}

// InsertionIndex returns where a new sink node x belongs in an existing causal
// order: immediately before the first node after its last dependency (at index
// lastDep, -1 if none) whose priority is greater than x's, else at the end.
// Inserting there yields exactly the order Kahn's algorithm produces for the
// enlarged graph, and leaves every other node's relative order unchanged.
func InsertionIndex(order []Node, lastDep int, x Node) int {
	for i := lastDep + 1; i < len(order); i++ {
		if Less(x, order[i]) {
			return i
		}
	}
	return len(order)
}
