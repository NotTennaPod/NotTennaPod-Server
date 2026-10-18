package queue

import (
	"math/bits"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// lisQuadratic is the layout's run selection as it was before it searched,
// kept as the oracle: every pair of keyed members compared.
func lisQuadratic(keys []*contract.QueueKey) []bool {
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

// keyPool returns n increasing keys of varied exponents: each after the one
// before, or between it and an earlier one, as devices allocate them.
func keyPool(r *mrand.Rand, n int) []contract.QueueKey {
	newID := func() [16]byte {
		var u [16]byte
		for i := range u {
			u[i] = byte(r.IntN(256))
		}
		return u
	}
	var keys []contract.QueueKey
	for len(keys) < n {
		var low, high *contract.QueueKey
		if len(keys) > 0 {
			i := r.IntN(len(keys))
			low = &keys[i]
			if i+1 < len(keys) && r.IntN(3) == 0 {
				high = &keys[i+1]
			}
		}
		k, err := contract.QueueKeyBetween(low, high, newID())
		if err != nil {
			panic(err)
		}
		at, _ := slices.BinarySearchFunc(keys, k, contract.CompareQueueKeys)
		keys = slices.Insert(keys, at, k)
	}
	return keys
}

// Key order strings order exactly as the keys do, keys of every exponent.
func TestKeyOrderIsTheKeysOrder(t *testing.T) {
	r := mrand.New(mrand.NewPCG(5, 8))
	pool := keyPool(r, 400)
	for range 20_000 {
		a, b := pool[r.IntN(len(pool))], pool[r.IntN(len(pool))]
		if got, want := strings.Compare(keyOrder(a), keyOrder(b)), contract.CompareQueueKeys(a, b); got != want {
			t.Fatalf("%s vs %s: order strings %d, keys %d", a, b, got, want)
		}
	}
}

// The run kept is the quadratic selection's, member for member, over random
// queues of every shape: random keys, members without keys, equal keys,
// already increasing, reversed, a few moved, and long sorted runs side by
// side. So the same members keep their keys and position IDs, and the fresh
// keys are drawn in the same order.
func TestLISIsTheQuadraticRun(t *testing.T) {
	r := mrand.New(mrand.NewPCG(13, 21))
	pool := keyPool(r, 600)
	shapes := map[string]func(n int) []int{
		"random": func(n int) []int {
			out := make([]int, n)
			for i := range out {
				out[i] = r.IntN(len(pool))
			}
			return out
		},
		"increasing": func(n int) []int {
			start := r.IntN(len(pool) - n + 1)
			out := make([]int, n)
			for i := range out {
				out[i] = start + i
			}
			return out
		},
		"reversed": func(n int) []int {
			out := make([]int, n)
			for i := range out {
				out[i] = n - 1 - i
			}
			return out
		},
		"a few moved": func(n int) []int {
			out := make([]int, n)
			for i := range out {
				out[i] = i
			}
			for range 1 + r.IntN(4) {
				i, j := r.IntN(n), r.IntN(n)
				x := out[i]
				out = slices.Insert(slices.Delete(out, i, i+1), min(j, n-1), x)
			}
			return out
		},
		"sorted runs": func(n int) []int {
			var out []int
			for len(out) < n {
				start, l := r.IntN(len(pool)), 1+r.IntN(n)
				for i := start; i < min(len(pool), start+l) && len(out) < n; i++ {
					out = append(out, i)
				}
			}
			return out
		},
		"equal keys": func(n int) []int {
			out := make([]int, n)
			for i := range out {
				out[i] = r.IntN(4)
			}
			return out
		},
	}
	for name, shape := range shapes {
		for trial := range 300 {
			n := 1 + r.IntN(min(len(pool), 1+trial*2))
			keys := make([]*contract.QueueKey, n)
			for i, x := range shape(n) {
				if r.IntN(8) > 0 {
					k := pool[x]
					keys[i] = &k
				}
			}
			if got, want := lisOf(keys), lisQuadratic(keys); !slices.Equal(got, want) {
				t.Fatalf("%s, trial %d (%d members): kept %v, the quadratic run %v", name, trial, n, got, want)
			}
		}
	}
	if got := lisOf(nil); len(got) != 0 {
		t.Fatalf("an empty queue: %v", got)
	}
}

// A layout's run over 10,000 members (one moved) makes O(Q log Q) key
// comparisons (the quadratic one made Q × (Q − 1) / 2, about 50 million)
// and allocates per member, not per pair.
func TestLISWorkIsQLogQ(t *testing.T) {
	r := mrand.New(mrand.NewPCG(3, 4))
	const q = 10_000
	pool := keyPool(r, q)
	keys := make([]*contract.QueueKey, q)
	for i := range keys {
		keys[i] = &pool[i]
	}
	keys = append(keys[1:], keys[0]) // the first member moved to the end
	keep, comparisons := lisCounted(keys)
	limit := q * (bits.Len(q) + 2)
	if kept := countTrue(keep); kept != q-1 {
		t.Fatalf("kept %d", kept)
	}
	allocs := testing.AllocsPerRun(3, func() { lisCounted(keys) })
	t.Logf("%d members: %d comparisons, %.0f allocations", q, comparisons, allocs)
	if comparisons > limit || allocs > 4*q+64 {
		t.Fatalf("%d comparisons (want at most %d), %.0f allocations (want at most %d)", comparisons, limit, allocs, 4*q+64)
	}
}

func countTrue(bs []bool) int {
	n := 0
	for _, b := range bs {
		if b {
			n++
		}
	}
	return n
}
