package queue

import (
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// Placed is a queue member with its position: a contract-v1 key and the
// position-write ID it was allocated for.
type Placed struct {
	Item
	Key        contract.QueueKey
	PositionID [16]byte
}

// Rekey gives the members, in their sorted order, increasing keys (PROTOCOL §7.6 "Result"): members
// along a longest increasing subsequence of their current keys keep their keys and position IDs;
// the others get fresh keys between their sorted neighbours, from position IDs drawn by newID. Only
// allocation is economised: the decision still asserts every member's key.
func Rekey(sorted []Placed, newID func() [16]byte) ([]Placed, error) {
	keep := lis(sorted)
	out := append([]Placed(nil), sorted...)
	for i := 0; i < len(out); {
		if keep[i] {
			i++
			continue
		}
		// A run of members to re-key between the previous member's key and the
		// next kept key.
		j := i
		for j < len(out) && !keep[j] {
			j++
		}
		var high *contract.QueueKey
		if j < len(out) {
			high = &out[j].Key
		}
		for k := i; k < j; k++ {
			var low *contract.QueueKey
			if k > 0 {
				low = &out[k-1].Key
			}
			u := newID()
			key, err := contract.QueueKeyBetween(low, high, u)
			if err != nil {
				return nil, err
			}
			out[k].Key, out[k].PositionID = key, u
		}
		i = j
	}
	return out, nil
}

// lis marks the lexicographically first (by sorted position) longest strictly
// increasing subsequence of keys: from the left, each kept member is the
// earliest one that still allows a longest run (PROTOCOL §7.6, as in the
// independent reference). Quadratic, which is fine for queue sizes.
func lis(ps []Placed) []bool {
	n := len(ps)
	keep := make([]bool, n)
	if n == 0 {
		return keep
	}
	// from[i]: the length of the longest increasing run starting at i.
	from := make([]int, n)
	longest := 0
	for i := n - 1; i >= 0; i-- {
		from[i] = 1
		for j := i + 1; j < n; j++ {
			if contract.CompareQueueKeys(ps[i].Key, ps[j].Key) < 0 && from[j]+1 > from[i] {
				from[i] = from[j] + 1
			}
		}
		longest = max(longest, from[i])
	}
	need, last := longest, -1
	for i := 0; i < n && need > 0; i++ {
		if from[i] == need && (last < 0 || contract.CompareQueueKeys(ps[last].Key, ps[i].Key) < 0) {
			keep[i] = true
			last = i
			need--
		}
	}
	return keep
}

// Candidate is an in-member's key before materialization: its published key,
// else the key its latest placement carried; OK is false when it has none.
type Candidate struct {
	Key        contract.QueueKey
	PositionID [16]byte
	OK         bool
}

// Assigned is an in-member's materialized key.
type Assigned struct {
	Entry      Entry
	Key        contract.QueueKey
	PositionID [16]byte
	Kept       bool // its candidate key (false: a fresh one)
}

// Materialize gives the replay's order of in-members increasing keys for a
// layout (queue replay PROTOCOL §7.6): the members along the first
// longest increasing run of candidates keep theirs (all of them when the
// candidates already increase), and the others get fresh keys left to right,
// each between the previous assigned key and the next kept key, from
// position IDs drawn by newID in that order.
func Materialize(order []Entry, cands map[Entry]Candidate, newID func() [16]byte) ([]Assigned, error) {
	out := make([]Assigned, len(order))
	keys := make([]*contract.QueueKey, len(order))
	for i, e := range order {
		c := cands[e]
		out[i] = Assigned{Entry: e, Key: c.Key, PositionID: c.PositionID, Kept: c.OK}
		if c.OK {
			k := c.Key
			keys[i] = &k
		}
	}
	keep := lisOf(keys)
	for i := 0; i < len(out); {
		if keep[i] {
			i++
			continue
		}
		j := i
		for j < len(out) && !keep[j] {
			j++
		}
		var high *contract.QueueKey
		if j < len(out) {
			high = &out[j].Key
		}
		for k := i; k < j; k++ {
			var low *contract.QueueKey
			if k > 0 {
				low = &out[k-1].Key
			}
			u := newID()
			key, err := contract.QueueKeyBetween(low, high, u)
			if err != nil {
				return nil, err
			}
			out[k].Key, out[k].PositionID, out[k].Kept = key, u, false
		}
		i = j
	}
	return out, nil
}

// lisOf marks the lexicographically first longest strictly increasing run of
// keys, skipping members without one (they are never kept).
func lisOf(keys []*contract.QueueKey) []bool {
	n := len(keys)
	keep := make([]bool, n)
	from := make([]int, n)
	longest := 0
	for i := n - 1; i >= 0; i-- {
		if keys[i] == nil {
			continue
		}
		from[i] = 1
		for j := i + 1; j < n; j++ {
			if keys[j] != nil && contract.CompareQueueKeys(*keys[i], *keys[j]) < 0 && from[j]+1 > from[i] {
				from[i] = from[j] + 1
			}
		}
		longest = max(longest, from[i])
	}
	need, last := longest, -1
	for i := 0; i < n && need > 0; i++ {
		if keys[i] != nil && from[i] == need && (last < 0 || contract.CompareQueueKeys(*keys[last], *keys[i]) < 0) {
			keep[i] = true
			last = i
			need--
		}
	}
	return keep
}
