package projection

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// TestIncrementalOrderEqualsKahn inserts random sinks one at a time (the only
// way the server ever adds nodes) and compares with a full Kahn run after each.
func TestIncrementalOrderEqualsKahn(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for trial := 0; trial < 2000; trial++ {
		var order []Node
		deps := map[string][]string{}
		all := []contract.OrderedFact{}
		n := 1 + r.IntN(40)
		for i := 0; i < n; i++ {
			x := Node{ID: fmt.Sprintf("%08x", r.Uint32()), NormalizedTime: r.Int64N(8)}
			var d []string
			for k := r.IntN(min(i, 3) + 1); k > 0 && i > 0; k-- {
				d = append(d, order[r.IntN(len(order))].ID)
			}
			slices.Sort(d)
			d = slices.Compact(d)
			// Time normalization keeps a node's time >= its dependencies' times.
			for _, id := range d {
				for _, o := range order {
					if o.ID == id && o.NormalizedTime > x.NormalizedTime {
						x.NormalizedTime = o.NormalizedTime
					}
				}
			}
			last := -1
			for idx, o := range order {
				if slices.Contains(d, o.ID) {
					last = idx
				}
			}
			at := InsertionIndex(order, last, x)
			order = slices.Insert(order, at, x)
			deps[x.ID] = d
			all = append(all, contract.OrderedFact{ID: x.ID, Dependencies: d, NormalizedTime: x.NormalizedTime})
			want, err := contract.CausalOrder(slices.Clone(all))
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(order))
			for k, o := range order {
				got[k] = o.ID
			}
			if !slices.Equal(got, want) {
				t.Fatalf("trial %d step %d: incremental %v, Kahn %v", trial, i, got, want)
			}
		}
	}
}

// Late facts with old times (no normalization lower bound beyond their deps)
// are the case that inserts early in the order.
func TestIncrementalOrderWithLateOldFacts(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	for trial := 0; trial < 2000; trial++ {
		var order []Node
		all := []contract.OrderedFact{}
		for i := 0; i < 30; i++ {
			x := Node{ID: fmt.Sprintf("%08x", r.Uint32()), NormalizedTime: int64(i * 10)}
			if r.IntN(3) == 0 {
				x.NormalizedTime = r.Int64N(int64(i*10 + 1)) // arrives late, old time
			}
			var d []string
			if len(order) > 0 && r.IntN(2) == 0 {
				d = []string{order[r.IntN(len(order))].ID}
				for _, o := range order {
					if o.ID == d[0] && o.NormalizedTime > x.NormalizedTime {
						x.NormalizedTime = o.NormalizedTime
					}
				}
			}
			last := -1
			for idx, o := range order {
				if slices.Contains(d, o.ID) {
					last = idx
				}
			}
			order = slices.Insert(order, InsertionIndex(order, last, x), x)
			all = append(all, contract.OrderedFact{ID: x.ID, Dependencies: d, NormalizedTime: x.NormalizedTime})
		}
		want, _ := contract.CausalOrder(all)
		got := make([]string, len(order))
		for k, o := range order {
			got[k] = o.ID
		}
		if !slices.Equal(got, want) {
			t.Fatalf("trial %d: incremental %v, Kahn %v", trial, got, want)
		}
	}
}
