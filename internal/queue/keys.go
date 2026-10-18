package queue

import (
	"encoding/binary"
	"math/big"
	"sort"

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

// lis marks the members along the lexicographically first longest strictly
// increasing run of their keys (PROTOCOL §7.6, as in the independent
// reference).
func lis(ps []Placed) []bool {
	keys := make([]*contract.QueueKey, len(ps))
	for i := range ps {
		keys[i] = &ps[i].Key
	}
	keep, _ := lisCounted(keys)
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
	keep, _ := lisCounted(keys)
	return keep
}

// lisCounted is lisOf, with the number of key comparisons it made. From the
// right, each keyed member's longest increasing run starting there is one
// more than the longest among those after it that start above its key: the
// greatest key starting a run of each length after it decreases with the
// length, so a binary search finds it (O(n log n), not every pair). Then
// from the left, each kept member is the earliest that still allows a
// longest run and is above the last kept one, as before. Keys compare as
// their order strings, built once each, so a comparison allocates nothing.
func lisCounted(keys []*contract.QueueKey) ([]bool, int) {
	n := len(keys)
	keep := make([]bool, n)
	order := make([]string, n)
	for i, k := range keys {
		if k != nil {
			order[i] = keyOrder(*k)
		}
	}
	comparisons := 0
	from := make([]int, n) // the longest increasing run starting at each keyed member
	var best []string      // best[l]: the greatest key starting a run of length l+1 after i; decreasing
	longest := 0
	for i := n - 1; i >= 0; i-- {
		if keys[i] == nil {
			continue
		}
		l := sort.Search(len(best), func(x int) bool {
			comparisons++
			return best[x] <= order[i]
		})
		from[i] = l + 1
		if l == len(best) {
			best = append(best, order[i])
		} else {
			best[l] = order[i]
		}
		longest = max(longest, from[i])
	}
	need, last := longest, -1
	for i := 0; i < n && need > 0; i++ {
		if keys[i] == nil || from[i] != need {
			continue
		}
		if last >= 0 {
			comparisons++
			if order[last] >= order[i] {
				continue
			}
		}
		keep[i] = true
		last = i
		need--
	}
	return keep, comparisons
}

// keyOrder is a string whose bytewise order is the keys' numeric order: the
// key M / 2^N lies in [2^-(z+1), 2^-z) for z = N - bitlen(M) leading zero
// bits, so the string is MaxQueueKeyExponent - z (four bytes, big-endian),
// then M's bits from its highest, packed into bytes and padded with zeros
// (an odd M ends in a one bit, so of two keys with the same z the one whose
// bits run on is the greater, as bytewise order has it).
func keyOrder(k contract.QueueKey) string {
	bits := k.M.BitLen()
	z := uint32(contract.MaxQueueKeyExponent - (int(k.N) - bits))
	m := new(big.Int).Lsh(k.M, uint((8-bits%8)%8)).Bytes()
	b := make([]byte, 4, 4+len(m))
	binary.BigEndian.PutUint32(b, z)
	return string(append(b, m...))
}
