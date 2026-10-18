package ptree

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"
)

type entry struct {
	k string
	v int64
}

var spec = &Spec[string, int64]{
	Compare:  strings.Compare,
	Priority: func(k string) uint64 { return Hash(k) },
	Measure:  func(_ string, v int64) (int64, int64, int64) { return v, 2 * v, v },
}

// model is a sorted slice of entries.
type model []entry

func (m model) find(k string) (int, bool) {
	return slices.BinarySearchFunc(m, k, func(e entry, k string) int { return strings.Compare(e.k, k) })
}

func (m model) set(k string, v int64) model {
	i, ok := m.find(k)
	out := slices.Clone(m)
	if ok {
		out[i].v = v
		return out
	}
	return slices.Insert(out, i, entry{k, v})
}

func (m model) del(k string) model {
	i, ok := m.find(k)
	if !ok {
		return m
	}
	return slices.Delete(slices.Clone(m), i, i+1)
}

func (m model) agg(from, to int) Agg {
	a := Empty
	for _, e := range m[from:to] {
		a = a.Plus(Agg{N: 1, A: e.v, B: 2 * e.v, Max: e.v})
	}
	return a
}

func check(t *testing.T, step int, tr Tree[string, int64], m model) {
	t.Helper()
	var got model
	for k, v := range tr.All() {
		got = append(got, entry{k, v})
	}
	if !slices.Equal(got, m) {
		t.Fatalf("step %d: entries differ:\n%v\n%v", step, got, m)
	}
	if tr.Len() != len(m) || tr.Agg() != m.agg(0, len(m)) {
		t.Fatalf("step %d: len %d agg %+v, model %d %+v", step, tr.Len(), tr.Agg(), len(m), m.agg(0, len(m)))
	}
	// The walks from either end.
	var fwd, rev model
	for it := tr.First(); ; {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		fwd = append(fwd, entry{k, v})
	}
	for it := tr.Last(); ; {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		rev = append(rev, entry{k, v})
	}
	slices.Reverse(rev)
	if !slices.Equal(fwd, m) || !slices.Equal(rev, m) {
		t.Fatalf("step %d: the walk from the first or the last entry differs", step)
	}
}

// The treap against a sorted-slice model, 10^5 random operations per
// seed: every read (get, rank, prefix sums, selection, seek both ways, the
// maximum search) agrees after each, earlier versions never change, and
// the shape is the one FromSorted builds from the same entries.
func TestPtreeAgainstASortedSlice(t *testing.T) {
	steps := 100_000
	if os.Getenv("NTP_READMODEL_LONG") != "" {
		steps *= 10
	}
	for _, seed := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Logf("seed %d, %d operations", seed, steps)
			r := rand.New(rand.NewPCG(seed, 7))
			key := func() string { return fmt.Sprintf("k%04d", r.IntN(3000)) }
			tr, m := New(spec), model{}
			type version struct {
				tr Tree[string, int64]
				m  model
			}
			var kept []version
			for step := range steps {
				switch r.IntN(10) {
				case 0, 1, 2, 3, 4:
					k, v := key(), r.Int64N(2000)-1000
					tr, m = tr.Set(k, v), m.set(k, v)
				case 5, 6, 7:
					k := key()
					tr, m = tr.Delete(k), m.del(k)
				case 8:
					if len(m) > 0 { // delete a present key
						k := m[r.IntN(len(m))].k
						tr, m = tr.Delete(k), m.del(k)
					}
				default:
					if len(m) > 0 { // replace a present key's value
						k := m[r.IntN(len(m))].k
						v := r.Int64N(2000) - 1000
						tr, m = tr.Set(k, v), m.set(k, v)
					}
				}
				k := key()
				i, found := m.find(k)
				if v, ok := tr.Get(k); ok != found || ok && v != m[i].v {
					t.Fatalf("step %d: get %s = %d %v", step, k, v, ok)
				}
				if tr.Rank(k) != i || tr.PrefixAgg(k) != m.agg(0, i) {
					t.Fatalf("step %d: rank %d prefix %+v, model %d %+v", step, tr.Rank(k), tr.PrefixAgg(k), i, m.agg(0, i))
				}
				if len(m) > 0 {
					j := r.IntN(len(m))
					if sk, sv, ok := tr.Select(j); !ok || sk != m[j].k || sv != m[j].v {
						t.Fatalf("step %d: select %d = %s", step, j, sk)
					}
				}
				if _, _, ok := tr.Select(len(m)); ok {
					t.Fatalf("step %d: select past the end", step)
				}
				if len(m) > 0 { // iterators from an index, both ways
					j := r.IntN(len(m))
					fit, rit := tr.SeekAt(j), tr.SeekAtRev(j)
					for n := j; n < min(j+3, len(m)); n++ {
						if sk, _, ok := fit.Next(); !ok || sk != m[n].k {
							t.Fatalf("step %d: seek at %d gave %s, want %s", step, j, sk, m[n].k)
						}
					}
					for n := j; n >= max(j-2, 0); n-- {
						if sk, _, ok := rit.Next(); !ok || sk != m[n].k {
							t.Fatalf("step %d: reverse seek at %d gave %s, want %s", step, j, sk, m[n].k)
						}
					}
				}
				if _, _, ok := tr.SeekAt(len(m)).Next(); ok {
					t.Fatalf("step %d: seek past the end", step)
				}
				it := tr.Seek(k)
				for n := i; n < min(i+3, len(m)); n++ {
					if sk, _, ok := it.Next(); !ok || sk != m[n].k {
						t.Fatalf("step %d: seek %s gave %s, want %s", step, k, sk, m[n].k)
					}
				}
				back := i - 1
				if found {
					back = i
				}
				rit := tr.SeekRev(k)
				for n := back; n >= max(back-2, 0); n-- {
					if sk, _, ok := rit.Next(); !ok || sk != m[n].k {
						t.Fatalf("step %d: reverse seek %s gave %s, want %s", step, k, sk, m[n].k)
					}
				}
				c := r.Int64N(2000) - 1000
				want := -1
				for n := i - 1; n >= 0; n-- {
					if m[n].v > c {
						want = n
						break
					}
				}
				if mk, _, ok := tr.MaxBelowWithMax(k, c); ok != (want >= 0) || ok && mk != m[want].k {
					t.Fatalf("step %d: max below %s with max > %d: %s %v, want index %d", step, k, c, mk, ok, want)
				}
				if step%997 == 0 {
					check(t, step, tr, m)
					kept = append(kept, version{tr, m})
					keys, vals := make([]string, len(m)), make([]int64, len(m))
					for n, e := range m {
						keys[n], vals[n] = e.k, e.v
					}
					built, err := FromSorted(spec, keys, vals)
					if err != nil {
						t.Fatal(err)
					}
					if d := Diff(tr, built, func(a, b int64) bool { return a == b }, func(k string) string { return k }); d != "" {
						t.Fatalf("step %d: not the canonical shape: %s", step, d)
					}
				}
			}
			for n, v := range kept { // persistence: nothing reachable from a kept version changed
				check(t, n, v.tr, v.m)
			}
		})
	}
}

// The shape is a function of the entries: any insertion order, with deletes
// of other keys on the way, gives FromSorted's tree.
func TestPtreeShapeIsCanonical(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 12))
	keys := make([]string, 2000)
	for i := range keys {
		keys[i] = fmt.Sprintf("e1:%016x", r.Uint64())
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	vals := make([]int64, len(keys))
	built, err := FromSorted(spec, keys, vals)
	if err != nil {
		t.Fatal(err)
	}
	for round := range 3 {
		order := slices.Clone(keys)
		r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		tr := New(spec)
		for i, k := range order {
			tr = tr.Set(k, 0)
			if i%5 == 0 {
				tr = tr.Set("zz-extra", 1).Delete("zz-extra")
			}
		}
		if d := Diff(tr, built, func(a, b int64) bool { return a == b }, func(k string) string { return k }); d != "" {
			t.Fatalf("round %d: %s", round, d)
		}
	}
	if _, err := FromSorted(spec, []string{"b", "a"}, []int64{0, 0}); err == nil {
		t.Fatal("FromSorted took unsorted keys")
	}
}

// Depth stays within 4 log2 n for hash-like and sequential IDs.
func TestPtreeDepth(t *testing.T) {
	for _, kind := range []string{"hashed", "sequential"} {
		for _, n := range []int{1000, 50_400} {
			tr := New(spec)
			for i := range n {
				k := fmt.Sprintf("e:%06d", i)
				if kind == "hashed" {
					k = fmt.Sprintf("e1:%016x", SplitMix64(uint64(i)))
				}
				tr = tr.Set(k, int64(i))
			}
			limit := 4 * bits.Len(uint(n))
			if d := tr.Depth(); d > limit {
				t.Errorf("%s %d: depth %d > %d", kind, n, d, limit)
			}
		}
	}
}

// An update copies only its path: a later tree shares every other node with
// the earlier one, which is unchanged.
func TestPtreeUpdatesCopyOnlyTheirPath(t *testing.T) {
	tr := New(spec)
	for i := range 10_000 {
		tr = tr.Set(fmt.Sprintf("k%05d", i), int64(i))
	}
	before := slices.Collect(func(yield func(entry) bool) {
		for k, v := range tr.All() {
			if !yield(entry{k, v}) {
				return
			}
		}
	})
	_, alloc0 := Counts()
	next := tr.Set("k05000", -1)
	_, alloc1 := Counts()
	if shared, limit := Shared(tr, next), 10_000-4*bits.Len(10_000); shared < limit {
		t.Errorf("only %d nodes shared after one update", shared)
	}
	if alloc1-alloc0 > int64(4*bits.Len(10_000)) {
		t.Errorf("one update allocated %d nodes", alloc1-alloc0)
	}
	check(t, 0, tr, before)
	if v, _ := next.Get("k05000"); v != -1 {
		t.Fatal("the update is missing")
	}
	_ = math.MaxInt
}
