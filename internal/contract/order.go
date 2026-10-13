package contract

import (
	"container/heap"
	"errors"
	"math"
)

// OrderedFact is the minimal view of a fact needed for causal ordering.
type OrderedFact struct {
	ID             string   // op or decision ID
	Dependencies   []string // IDs that must precede this fact
	NormalizedTime int64    // ms since the epoch, from the receipt
}

// Ordering errors.
var (
	ErrDependencyMissing = errors.New("contract: dependency missing from fact set")
	ErrDependencyCycle   = errors.New("contract: dependency cycle")
	ErrDuplicateFact     = errors.New("contract: duplicate fact ID")
)

type readyQueue []*OrderedFact

func (q readyQueue) Len() int { return len(q) }
func (q readyQueue) Less(i, j int) bool {
	if q[i].NormalizedTime != q[j].NormalizedTime {
		return q[i].NormalizedTime < q[j].NormalizedTime
	}
	return q[i].ID < q[j].ID // bytewise
}
func (q readyQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *readyQueue) Push(x any)   { *q = append(*q, x.(*OrderedFact)) }
func (q *readyQueue) Pop() any {
	old := *q
	x := old[len(old)-1]
	*q = old[:len(old)-1]
	return x
}

// CausalOrder returns the deterministic topological order of PROTOCOL §6.9: Kahn's
// algorithm, always emitting the ready fact with the smallest (normalized time,
// ID). The set must be closed under dependencies.
func CausalOrder(facts []OrderedFact) ([]string, error) {
	byID := make(map[string]*OrderedFact, len(facts))
	for i := range facts {
		f := &facts[i]
		if _, dup := byID[f.ID]; dup {
			return nil, ErrDuplicateFact
		}
		byID[f.ID] = f
	}
	waiting := make(map[string]int, len(facts))
	dependents := make(map[string][]*OrderedFact, len(facts))
	ready := &readyQueue{}
	for i := range facts {
		f := &facts[i]
		seen := make(map[string]bool, len(f.Dependencies))
		for _, d := range f.Dependencies {
			if seen[d] {
				continue
			}
			seen[d] = true
			if _, ok := byID[d]; !ok {
				return nil, ErrDependencyMissing
			}
			waiting[f.ID]++
			dependents[d] = append(dependents[d], f)
		}
		if waiting[f.ID] == 0 {
			heap.Push(ready, f)
		}
	}
	order := make([]string, 0, len(facts))
	for ready.Len() > 0 {
		f := heap.Pop(ready).(*OrderedFact)
		order = append(order, f.ID)
		for _, d := range dependents[f.ID] {
			waiting[d.ID]--
			if waiting[d.ID] == 0 {
				heap.Push(ready, d)
			}
		}
	}
	if len(order) != len(facts) {
		return nil, ErrDependencyCycle
	}
	return order, nil
}

// NormalizeTime applies PROTOCOL §6 at first acceptance: clamp the client's time into
// [max dependency normalized time, server receive time]; if the server's clock is
// behind the dependencies, use the dependency lower bound.
func NormalizeTime(clientTime, receiveTime int64, dependencyTimes []int64) int64 {
	lower := int64(math.MinInt64)
	for _, t := range dependencyTimes {
		if t > lower {
			lower = t
		}
	}
	if receiveTime < lower {
		return lower
	}
	t := clientTime
	if t < lower {
		t = lower
	}
	if t > receiveTime {
		t = receiveTime
	}
	return t
}
