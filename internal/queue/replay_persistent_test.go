package queue

// The persistent State against the frozen reference (replay_reference_test.go)
// after every node of long seeded random sequences, its snapshots against
// the reference's copies, the label maintenance at its edges, and the work
// a queue edit costs as the history grows.

import (
	"fmt"
	"math"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// gen draws replay nodes: places, moves, sorts (canonical and random),
// policies, imports, holds, ghosts (automatic adds that do not apply, ghost-only
// move members), anchors without entries, removals and bare membership events.
type gen struct {
	r        *mrand.Rand
	universe []Entry
	values   map[Entry]Values
	n        int
	seq      int64
}

func newGen(seed uint64, size int) *gen {
	g := &gen{r: mrand.New(mrand.NewPCG(seed, seed*7919+1)), values: map[Entry]Values{}}
	for i := range size {
		e := Entry(fmt.Sprintf("g%d/e%05d", i%3, i))
		g.universe = append(g.universe, e)
		// Few distinct values, so sorts meet ties.
		g.values[e] = Values{Title: fmt.Sprintf("T%02d", g.r.IntN(40)), PodcastID: fmt.Sprintf("p%d", g.r.IntN(5)),
			PodcastTitle: fmt.Sprintf("P%d", g.r.IntN(5)), PublicationTime: int64(g.r.IntN(50)) * 1000, Duration: int64(g.r.IntN(20)) * 60000}
	}
	return g
}

// hot is the part of the universe most commands name: a person's queue is a
// small, busy part of a long history.
func (g *gen) entry() Entry {
	if g.r.IntN(4) == 0 {
		return g.universe[g.r.IntN(len(g.universe))]
	}
	return g.universe[g.r.IntN(min(len(g.universe), 60))]
}

func (g *gen) run(n int) []Entry {
	out := make([]Entry, 0, n)
	for range n {
		out = append(out, g.entry())
	}
	return out
}

func (g *gen) place(visible []Entry) Place {
	switch k := g.r.IntN(10); {
	case k < 3:
		return Place{Kind: PlaceStart}
	case k < 5:
		return Place{Kind: PlaceEnd}
	case k < 8 && len(visible) > 0:
		// After a member, often the same few (gaps run out there).
		return Place{Kind: PlaceAfter, After: visible[g.r.IntN(min(len(visible), 3))]}
	case k < 9:
		return Place{Kind: PlaceAfter, After: g.entry()} // possibly an entry that has no place yet
	}
	return Place{Kind: PlaceAfter, After: Entry(fmt.Sprintf("missing%d", g.r.IntN(3)))}
}

func (g *gen) keys(run []Entry) map[Entry]Key {
	out := map[Entry]Key{}
	for _, e := range run {
		if g.r.IntN(5) == 0 {
			continue
		}
		g.seq++
		seq := g.seq
		if g.r.IntN(6) == 0 {
			seq = g.r.Int64N(g.seq + 1) // a late write ranked before later ones
		}
		out[e] = Key{Key: fmt.Sprintf("k%d", g.r.IntN(1<<20)), PositionID: fmt.Sprintf("u%d", seq),
			Rank: Rank{Epoch: 1, Seq: seq, Index: int64(g.r.IntN(3)), ID: fmt.Sprintf("n%d", g.r.IntN(9))}}
	}
	return out
}

func (g *gen) vals(es []Entry) map[Entry]Values {
	if g.r.IntN(8) == 0 {
		return nil // no record: the latest placement's values, else the fallback
	}
	out := map[Entry]Values{}
	for _, e := range es {
		if g.r.IntN(10) > 0 {
			out[e] = g.values[e]
		}
	}
	return out
}

var sortOrders = []string{"episode_title_a_z", "episode_title_z_a", "feed_title_a_z", "feed_title_z_a", "duration_short_long",
	"duration_long_short", "date_old_new", "date_new_old", "smart_shuffle_old_new", "smart_shuffle_new_old", "unknown_order"}

// node draws the next node, given the queue as it stands (anchors and sorts
// name its members).
func (g *gen) node(visible []Entry) Node {
	g.n++
	n := Node{ID: fmt.Sprintf("n%06d", g.n)}
	ins := func(es []Entry) {
		for _, e := range es {
			n.Events = append(n.Events, Event{Entry: e, In: true})
		}
	}
	switch k := g.r.IntN(100); {
	case k < 30: // an add (or a restore), its membership taking effect
		run := g.run(1 + g.r.IntN(3))
		if g.r.IntN(5) == 0 {
			run = append(run, run[0]) // a duplicate
		}
		ins(run)
		trig := run
		if g.r.IntN(3) == 0 {
			trig = append(slices.Clone(visible), run...) // a sort trigger's record
		}
		n.Step = &Step{Kind: StepPlace, Restore: g.r.IntN(10) == 0, Run: run, Place: g.place(visible), Keys: g.keys(run), Values: g.vals(trig)}
	case k < 36: // an automatic add whose membership may not apply: a ghost
		e := g.entry()
		if g.r.IntN(2) == 0 {
			ins([]Entry{e})
		}
		n.Step = &Step{Kind: StepPlace, Run: []Entry{e}, Place: g.place(visible), Keys: g.keys([]Entry{e}), Values: g.vals([]Entry{e})}
	case k < 56: // a move or a run move, sometimes with ghost-only members
		var run []Entry
		if len(visible) > 0 && g.r.IntN(4) > 0 {
			for range 1 + g.r.IntN(3) {
				run = append(run, visible[g.r.IntN(len(visible))])
			}
		} else {
			run = g.run(1 + g.r.IntN(2))
		}
		st := &Step{Kind: StepMove, Run: run, Place: g.place(visible), Keys: g.keys(run), Values: g.vals(run)}
		if g.r.IntN(6) == 0 {
			st.GhostOnly = map[Entry]bool{g.entry(): true, run[0]: g.r.IntN(2) == 0}
			st.Run = append(st.Run, g.entry())
		}
		n.Step = st
	case k < 76: // a removal (an out event), now and then two
		for range 1 + g.r.IntN(2) {
			e := g.entry()
			if len(visible) > 0 && g.r.IntN(3) > 0 {
				e = visible[g.r.IntN(len(visible))]
			}
			n.Events = append(n.Events, Event{Entry: e, In: false})
		}
	case k < 81: // a membership without a step (appended at the end)
		ins(g.run(1 + g.r.IntN(2)))
	case k < 85: // a canonical sort
		n.Step = &Step{Kind: StepSort, Order: sortOrders[g.r.IntN(len(sortOrders))], Values: g.vals(visible)}
	case k < 88: // a random sort of what the device observed
		perm := slices.Clone(visible)
		g.r.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
		if len(perm) > 0 && g.r.IntN(3) == 0 {
			perm = append(perm, perm[0], g.entry()) // a duplicate and a stranger
		}
		observed := perm
		if len(observed) > 2 && g.r.IntN(2) == 0 {
			observed = observed[:len(observed)/2]
		}
		n.Step = &Step{Kind: StepSort, Order: "random", Perm: perm, Observed: observed}
	case k < 92: // the sorting register
		order := sortOrders[g.r.IntN(len(sortOrders)-1)]
		if g.r.IntN(5) == 0 {
			order = ""
		}
		n.Step = &Step{Kind: StepPolicy, Sorted: g.r.IntN(3) == 0, SortBy: order}
	case k < 96: // an import's episodes held, with their memberships
		run := g.run(1 + g.r.IntN(3))
		ins(run)
		n.Step = &Step{Kind: StepHold, Keys: g.keys(run)}
	default: // an import's queue at the end
		run := g.run(1 + g.r.IntN(4))
		if g.r.IntN(2) == 0 {
			ins(run[:1])
		}
		n.Step = &Step{Kind: StepImport, Run: run, Values: g.vals(run)}
	}
	return n
}

func (g *gen) fallback(e Entry) Values {
	return Values{Title: "fallback " + string(e), Duration: int64(len(e))}
}

// sameState reports the first difference between the persistent state and
// the reference in what the replay exposes: the queue, the structure, the
// membership, the key writes, the register and the value index.
func sameState(s *State, ref *refState) string {
	if a, b := joined(s.Visible()), joined(ref.Visible()); a != b {
		return fmt.Sprintf("queue\n %s\nreference\n %s", a, b)
	}
	if a, b := joined(s.Structure()), joined(ref.Structure()); a != b {
		return fmt.Sprintf("structure\n %s\nreference\n %s", a, b)
	}
	if s.in.Len() != len(ref.in) {
		return fmt.Sprintf("%d members, reference %d", s.in.Len(), len(ref.in))
	}
	for e := range ref.in {
		if !s.In(e) {
			return fmt.Sprintf("%s is not a member", e)
		}
	}
	if s.keys.Len() != len(ref.keys) {
		return fmt.Sprintf("%d key writes, reference %d", s.keys.Len(), len(ref.keys))
	}
	for e, k := range ref.keys {
		if got, ok := s.KeyOf(e); !ok || got != k {
			return fmt.Sprintf("key write of %s: %v, reference %v", e, got, k)
		}
	}
	if s.values.Len() != len(ref.values) {
		return fmt.Sprintf("%d values, reference %d", s.values.Len(), len(ref.values))
	}
	for e, v := range ref.values {
		if got, ok := s.SortValues(e); !ok || got != v {
			return fmt.Sprintf("values of %s: %v, reference %v", e, got, v)
		}
	}
	if a1, b1 := s.Sorting(); true {
		if a2, b2 := ref.Sorting(); a1 != a2 || b1 != b2 {
			return fmt.Sprintf("sorting %v %s, reference %v %s", a1, b1, a2, b2)
		}
	}
	if s.Count != ref.Count || s.Last != ref.Last {
		return fmt.Sprintf("count %d %s, reference %d %s", s.Count, s.Last, ref.Count, ref.Last)
	}
	return ""
}

// consistent checks the state's own indexes: labels strictly increase along
// the structure, at is each entry's label, inOrder is exactly the members
// with a place.
func consistent(s *State) string {
	n := 0
	for l, e := range s.structure.All() {
		if at, ok := s.at.Get(e); !ok || at != l {
			return fmt.Sprintf("%s is labelled %d in the structure, %d in at", e, l, at)
		}
		if l == math.MaxUint64 {
			return "an entry is labelled MaxUint64"
		}
		if got, ok := s.inOrder.Get(l); s.In(e) != ok || (ok && got != e) {
			return fmt.Sprintf("inOrder at %d: %s %v, member %v", l, got, ok, s.In(e))
		}
		n++
	}
	if n != s.at.Len() {
		return fmt.Sprintf("%d entries in the structure, %d in at", n, s.at.Len())
	}
	members := 0
	for e := range s.in.All() {
		if s.has(e) {
			members++
		}
	}
	if members != s.inOrder.Len() {
		return fmt.Sprintf("%d members with a place, %d in inOrder", members, s.inOrder.Len())
	}
	return ""
}

// After every node of three seeded sequences of 20,000 nodes, the
// persistent State equals the frozen reference in everything the replay
// exposes and in its diagnostics; snapshots kept along the way never change
// as their clones move on, and resuming from one gives the full replay.
func TestReplayPersistentStateEqualsReference(t *testing.T) {
	nodes := 20000
	if testing.Short() {
		nodes = 4000
	}
	for _, c := range []struct {
		seed uint64
		size int
	}{{1, 200}, {2, 800}, {3, 2000}} {
		t.Run(fmt.Sprintf("seed %d", c.seed), func(t *testing.T) {
			g := newGen(c.seed, c.size)
			s, ref := NewState(), newRefState()
			s.Fallback, ref.Fallback = g.fallback, g.fallback
			relabels := relabelled.Load()
			type kept struct {
				s     *State
				ref   *refState
				at    int
				shown string
			}
			var snaps []kept
			var all []Node
			for i := range nodes {
				n := g.node(ref.Visible())
				all = append(all, n)
				d1, d2 := s.Apply(n), ref.Apply(n)
				if !slices.Equal(d1, d2) {
					t.Fatalf("node %d: diagnostics %v, reference %v", i, d1, d2)
				}
				if d := sameState(s, ref); d != "" {
					t.Fatalf("node %d (%+v): %s", i, n.Step, d)
				}
				if i%997 == 0 {
					if d := consistent(s); d != "" {
						t.Fatalf("node %d: %s", i, d)
					}
					c := kept{s: s.Clone(), ref: ref.Clone(), at: i + 1}
					c.shown = joined(c.s.Structure())
					snaps = append(snaps, c)
				}
			}
			if d := consistent(s); d != "" {
				t.Fatal(d)
			}
			if relabelled.Load() == relabels {
				t.Fatal("the sequence never relabelled: the label maintenance went untested")
			}
			for _, k := range snaps {
				if got := joined(k.s.Structure()); got != k.shown {
					t.Fatalf("the snapshot after %d nodes changed as its clone moved on", k.at)
				}
				if d := sameState(k.s, k.ref); d != "" {
					t.Fatalf("the snapshot after %d nodes: %s", k.at, d)
				}
			}
			// Resuming from a kept snapshot replays to the same state.
			for _, k := range []kept{snaps[0], snaps[len(snaps)/2], snaps[len(snaps)-1]} {
				r := k.s.Clone()
				for _, n := range all[k.at:] {
					r.Apply(n)
				}
				if d := sameState(r, ref); d != "" {
					t.Fatalf("resumed after %d nodes: %s", k.at, d)
				}
			}
			t.Logf("%d nodes, %d entries, %d in the queue, %d relabelled", nodes, s.structure.Len(), s.inOrder.Len(), relabelled.Load()-relabels)
		})
	}
}

// WrittenSince lists every entry whose key write changed after a snapshot,
// once, on both branches of a fork, for forks after every node and long ones.
func TestWrittenSinceListsTheChangedKeys(t *testing.T) {
	g := newGen(9, 300)
	s := NewState()
	s.Fallback = g.fallback
	check := func(base, x *State) int {
		t.Helper()
		listed := map[Entry]bool{}
		for _, e := range x.WrittenSince(base.Count) {
			if listed[e] {
				t.Fatalf("%s listed twice", e)
			}
			listed[e] = true
		}
		for e, k := range x.KeyWrites() {
			if old, ok := base.KeyOf(e); (!ok || old != k) && !listed[e] {
				t.Fatalf("after %d nodes, %d later: %s's key write changed but is not listed", base.Count, x.Count-base.Count, e)
			}
		}
		return len(listed)
	}
	listed := 0
	for range 3000 {
		base := s.Clone()
		s.Apply(g.node(s.Visible()))
		listed += check(base, s)
	}
	if listed == 0 {
		t.Fatal("no key write listed")
	}
	base := s.Clone()
	a, b := s, base.Clone()
	for range 500 {
		a.Apply(g.node(a.Visible()))
	}
	for range 200 {
		b.Apply(g.node(b.Visible()))
	}
	if check(base, a) == 0 || check(base, b) == 0 {
		t.Fatal("no key write listed")
	}
}

// Label maintenance at its edges: inserts at the start until the labels
// below the first run out, inserts after the same entry, and appends near
// the top of the label space; the order is the reference's and the indexes
// stay consistent.
func TestReplayLabelEdges(t *testing.T) {
	s, ref := NewState(), newRefState()
	apply := func(n Node) {
		t.Helper()
		s.Apply(n)
		ref.Apply(n)
		if d := sameState(s, ref); d != "" {
			t.Fatalf("%s: %s", n.ID, d)
		}
		if d := consistent(s); d != "" {
			t.Fatalf("%s: %s", n.ID, d)
		}
	}
	add := func(id string, p Place, e Entry) {
		apply(Node{ID: id, Events: []Event{{Entry: e, In: true}}, Step: &Step{Kind: StepPlace, Run: []Entry{e}, Place: p}})
	}
	before := relabelled.Load()
	for i := range 200 {
		add(fmt.Sprintf("s%03d", i), Place{Kind: PlaceStart}, Entry(fmt.Sprintf("s%03d", i)))
	}
	for i := range 200 {
		add(fmt.Sprintf("a%03d", i), Place{Kind: PlaceAfter, After: "s100"}, Entry(fmt.Sprintf("a%03d", i)))
	}
	if relabelled.Load() == before {
		t.Fatal("no relabelling")
	}
	// The top of the label space: an entry labelled just below MaxUint64,
	// then appends after it.
	top := NewState()
	top.label("x", math.MaxUint64-2)
	top.setIn("x", true)
	for i := range 70 {
		e := Entry(fmt.Sprintf("y%02d", i))
		top.setIn(e, true)
		top.ensure(e)
		if d := consistent(top); d != "" {
			t.Fatalf("append %d: %s", i, d)
		}
	}
	if got := joined(top.Visible()); !strings.HasPrefix(got, "x,y00,y01,") || !strings.HasSuffix(got, "y68,y69") {
		t.Fatalf("appends at the top of the label space: %s", got)
	}
	// The bottom: an entry labelled 0, then inserts at the start.
	bottom := NewState()
	bottom.label("x", 0)
	for i := range 70 {
		e := Entry(fmt.Sprintf("z%02d", 69-i))
		bottom.link(e, 0, false)
		if d := consistent(bottom); d != "" {
			t.Fatalf("insert %d: %s", i, d)
		}
	}
	if got := joined(bottom.Structure()); !strings.HasPrefix(got, "z00,z01,") || !strings.HasSuffix(got, "z69,x") {
		t.Fatalf("inserts at the bottom of the label space: %s", got)
	}
}

// The replay's part of a queue edit's work: with a queue of 10 members and a history of
// 1,000, 10,000 and 50,000 entries, a queue edit (a clone, an add, a move,
// a removal, the queue read) visits and copies tree nodes in proportion to
// (live + touched) · log n, not to the history: the work at 50,000 is within
// a small factor of the work at 1,000 (log n grows by 1.6×), and far below
// the history.
func TestReplayEditWorkBudget(t *testing.T) {
	type work struct{ visited, allocated int64 }
	measure := func(history int) work {
		s := NewState()
		var nodes int
		next := func() string { nodes++; return fmt.Sprintf("n%07d", nodes) }
		// A long history: every entry was queued once, then removed.
		for i := range history {
			e := Entry(fmt.Sprintf("h%06d", i))
			s.Apply(Node{ID: next(), Events: []Event{{Entry: e, In: true}},
				Step: &Step{Kind: StepPlace, Run: []Entry{e}, Place: Place{Kind: PlaceEnd},
					Keys: map[Entry]Key{e: {Key: "k", Rank: Rank{Seq: int64(i)}}}}})
			s.Apply(Node{ID: next(), Events: []Event{{Entry: e, In: false}}})
		}
		live := make([]Entry, 10)
		for i := range live {
			live[i] = Entry(fmt.Sprintf("l%02d", i))
			s.Apply(Node{ID: next(), Events: []Event{{Entry: live[i], In: true}}, Step: &Step{Kind: StepPlace, Run: []Entry{live[i]}, Place: Place{Kind: PlaceEnd}}})
		}
		v0, a0 := ptree.Counts()
		for i := range 20 {
			c := s.Clone()
			e := Entry(fmt.Sprintf("x%02d", i))
			c.Apply(Node{ID: next(), Events: []Event{{Entry: e, In: true}}, Step: &Step{Kind: StepPlace, Run: []Entry{e},
				Place: Place{Kind: PlaceAfter, After: live[i%10]}, Keys: map[Entry]Key{e: {Key: "k", Rank: Rank{Seq: int64(history + i)}}}}})
			c.Apply(Node{ID: next(), Step: &Step{Kind: StepMove, Run: []Entry{live[(i+3)%10]}, Place: Place{Kind: PlaceStart}}})
			c.Apply(Node{ID: next(), Events: []Event{{Entry: e, In: false}}})
			if got := len(c.Visible()); got != 10 {
				t.Fatalf("queue of %d", got)
			}
			c.WrittenSince(s.Count)
			s = c
		}
		v1, a1 := ptree.Counts()
		return work{(v1 - v0) / 20, (a1 - a0) / 20}
	}
	small, mid, large := measure(1000), measure(10000), measure(50000)
	t.Logf("per edit: history 1,000: %+v; 10,000: %+v; 50,000: %+v", small, mid, large)
	for _, w := range []work{mid, large} {
		if w.visited > 3*small.visited || w.allocated > 3*small.allocated {
			t.Fatalf("an edit's work grows with the history: %+v at 1,000, %+v later", small, w)
		}
	}
	if large.visited > 2000 || large.allocated > 1000 {
		t.Fatalf("an edit at a history of 50,000 visits %d and copies %d tree nodes", large.visited, large.allocated)
	}
}
