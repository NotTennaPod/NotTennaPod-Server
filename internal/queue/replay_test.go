package queue

// The replay's cases (PROTOCOL §7.6) that the replay
// itself decides, with inputs as the server hands them over: nodes in the
// deterministic order, mapped entries and membership events.

import (
	"crypto/rand"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// script builds a replay's nodes.
type script struct {
	nodes  []Node
	titles map[Entry]Values // every entry's canonical values
}

func newScript() *script { return &script{titles: map[Entry]Values{}} }

func (s *script) node(step *Step, events ...Event) {
	s.nodes = append(s.nodes, Node{ID: fmt.Sprintf("n%03d", len(s.nodes)), Events: events, Step: step})
}

func (s *script) values(es ...Entry) map[Entry]Values {
	out := map[Entry]Values{}
	for _, e := range es {
		v, ok := s.titles[e]
		if !ok {
			v = Values{Title: string(e), PodcastID: "p"}
		}
		out[e] = v
	}
	return out
}

func ins(es ...Entry) []Event {
	var out []Event
	for _, e := range es {
		out = append(out, Event{Entry: e, In: true})
	}
	return out
}

func outs(es ...Entry) []Event {
	var out []Event
	for _, e := range es {
		out = append(out, Event{Entry: e, In: false})
	}
	return out
}

// add is an explicit add (its membership takes effect for every member).
func (s *script) add(p Place, run ...Entry) {
	s.node(&Step{Kind: StepPlace, Run: run, Place: p, Values: s.values(run...)}, ins(run...)...)
}

// trigger is an explicit add whose receipt records every member then in
// the queue (the server admitted it under keep-sorted).
func (s *script) trigger(p Place, queue []Entry, run ...Entry) {
	s.node(&Step{Kind: StepPlace, Run: run, Place: p, Values: s.values(append(slices.Clone(queue), run...)...)}, ins(run...)...)
}

// auto is an automatic add (a filing); applies says whether its membership
// effect takes effect (no explicit membership decision exists).
func (s *script) auto(applies bool, p Place, e Entry) {
	var ev []Event
	if applies {
		ev = ins(e)
	}
	s.node(&Step{Kind: StepPlace, Run: []Entry{e}, Place: p, Values: s.values(e)}, ev...)
}

func (s *script) restore(p Place, e Entry) {
	s.node(&Step{Kind: StepPlace, Restore: true, Run: []Entry{e}, Place: p, Values: s.values(e)}, ins(e)...)
}

func (s *script) remove(es ...Entry) { s.node(nil, outs(es...)...) }

func (s *script) move(p Place, run ...Entry) { s.node(&Step{Kind: StepMove, Run: run, Place: p}) }

func (s *script) sortBy(order string, recorded ...Entry) {
	s.node(&Step{Kind: StepSort, Order: order, Values: s.values(recorded...)})
}

func (s *script) shuffle(perm ...Entry) {
	s.node(&Step{Kind: StepSort, Order: "random", Perm: perm, Observed: perm})
}

func (s *script) policy(sorted bool, order string) {
	s.node(&Step{Kind: StepPolicy, Sorted: sorted, SortBy: order})
}

func end() Place               { return Place{Kind: PlaceEnd} }
func front() Place             { return Place{Kind: PlaceStart} }
func after(e Entry) Place      { return Place{Kind: PlaceAfter, After: e} }
func (s *script) run() *State  { st, _ := s.replay(); return st }
func joined(es []Entry) string { return strings.Join(toStrings(es), ",") }
func toStrings(es []Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = string(e)
	}
	return out
}

func (s *script) replay() (*State, []Diagnostic) {
	st := NewState()
	var diags []Diagnostic
	for _, n := range s.nodes {
		diags = append(diags, st.Apply(n)...)
	}
	return st, diags
}

func (s *script) expect(t *testing.T, want string) {
	t.Helper()
	if got := joined(s.run().Visible()); got != want {
		t.Fatalf("queue %s, want %s", got, want)
	}
	// Every prefix replayed from a snapshot of every earlier prefix agrees
	// with the full replay (PROTOCOL §7.6's cross-check, at the replay level).
	full := NewState()
	snaps := []*State{full.Clone()}
	for _, n := range s.nodes {
		full.Apply(n)
		snaps = append(snaps, full.Clone())
	}
	for i := range s.nodes {
		resumed := snaps[i].Clone()
		for _, n := range s.nodes[i:] {
			resumed.Apply(n)
		}
		if a, b := joined(resumed.Structure()), joined(full.Structure()); a != b {
			t.Fatalf("resumed after %d nodes: structure %s, full %s", i, a, b)
		}
	}
}

// PROTOCOL §7.6: the six examples.
func TestReplayExamples(t *testing.T) {
	t.Run("back adds keep the order they were made in", func(t *testing.T) {
		s := newScript()
		s.add(end(), "Z")
		s.add(end(), "X") // Monday, the iPhone
		s.add(end(), "Y") // Tuesday, the Android phone
		s.expect(t, "Z,X,Y")
	})
	t.Run("front adds", func(t *testing.T) {
		s := newScript()
		s.add(end(), "Z")
		s.add(front(), "X")
		s.add(front(), "Y")
		s.expect(t, "Y,X,Z")
	})
	t.Run("two phones, three back adds", func(t *testing.T) {
		s := newScript()
		s.add(end(), "Z")
		s.add(end(), "X") // time 1
		s.add(end(), "Y") // time 2
		s.add(end(), "W") // time 3
		s.expect(t, "Z,X,Y,W")
	})
	t.Run("keep-sorted turned on, then an add elsewhere", func(t *testing.T) {
		s := newScript()
		s.add(end(), "Z", "A")
		s.policy(true, "episode_title_a_z")
		s.trigger(end(), []Entry{"Z", "A"}, "B")
		s.expect(t, "A,B,Z")
	})
	t.Run("two drags to just after P", func(t *testing.T) {
		s := newScript()
		s.add(end(), "P", "Q", "E", "F")
		s.move(after("P"), "E") // first
		s.move(after("P"), "F") // later
		s.expect(t, "P,F,E,Q")
	})
	t.Run("an add of an episode already queued is ignored", func(t *testing.T) {
		s := newScript()
		s.add(end(), "A")
		s.add(end(), "X") // time 1, the iPhone
		s.add(end(), "Y") // time 2, the Android phone
		s.add(end(), "X") // time 3, the Android phone, which had not seen X
		s.expect(t, "A,X,Y")
	})
}

// Under the replay: [Z,A], an offline add of B under keep-sorted
// (sorts to A,B,Z), keep-sorted turned off, Z moved to the front: Z,A,B.
func TestReplayKeepSortedOffThenMoveToFront(t *testing.T) {
	s := newScript()
	s.add(end(), "Z", "A")
	s.policy(true, "episode_title_a_z")
	s.trigger(end(), []Entry{"Z", "A"}, "B")
	s.policy(false, "episode_title_a_z")
	s.move(front(), "Z")
	s.expect(t, "Z,A,B")
}

// A sort orders the in-members only: Smart Shuffle after a removal is
// Smart Shuffle of the queue, not of every episode ever placed.
func TestReplaySortsInMembersOnly(t *testing.T) {
	s := newScript()
	s.titles["A1"] = Values{Title: "A1", PodcastID: "pa", PublicationTime: 1}
	s.titles["B1"] = Values{Title: "B1", PodcastID: "pb", PublicationTime: 1}
	s.titles["B2"] = Values{Title: "B2", PodcastID: "pb", PublicationTime: 2}
	s.add(end(), "A1", "B1", "B2")
	s.remove("B1")
	s.sortBy("smart_shuffle_old_new", "A1", "B2")
	want, _ := Sort("smart_shuffle_old_new", []Item{{EpisodeID: "A1", PodcastID: "pa", Title: "A1", PublicationTime: 1},
		{EpisodeID: "B2", PodcastID: "pb", Title: "B2", PublicationTime: 2}})
	s.expect(t, want[0].EpisodeID+","+want[1].EpisodeID)
}

// X added after an automatic filing that a concurrent removal suppresses:
// the filing leaves a ghost, X lands after it, and the ghost is never shown.
func TestReplayGhostOfSuppressedFiling(t *testing.T) {
	s := newScript()
	s.add(end(), "Z", "Y")
	s.auto(false, after("Z"), "D")
	s.add(after("D"), "X")
	s.expect(t, "Z,X,Y")
	if got := joined(s.run().Structure()); got != "Z,D,X,Y" {
		t.Fatalf("structure %s", got)
	}
}

// A late explicit removal suppresses D's earlier filing; replayed from
// the start, D becomes a ghost and X stays where its device put it.
func TestReplayLateSuppressionOfAnAnchorsFiling(t *testing.T) {
	for _, suppressed := range []bool{false, true} {
		s := newScript()
		s.add(end(), "Z", "Y")
		s.auto(!suppressed, after("Z"), "D")
		s.add(after("D"), "X")
		if suppressed {
			s.expect(t, "Z,X,Y")
		} else {
			s.expect(t, "Z,D,X,Y")
		}
	}
}

// Undo after a front add on the same phone: the saved index resolved at
// Undo time ([X,A,C], index 1) is after X: X,B,A,C.
func TestReplayUndoAfterFrontAdd(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "B", "C")
	s.remove("B")
	s.add(front(), "X")
	s.restore(after("X"), "B")
	s.expect(t, "X,B,A,C")
}

// Undo never sorts, even under keep-sorted.
func TestReplayRestoreNeverSorts(t *testing.T) {
	s := newScript()
	s.add(end(), "C", "A", "B")
	s.policy(true, "episode_title_a_z")
	s.remove("C")
	s.restore(front(), "C")
	s.expect(t, "C,A,B")
}

// A random sort replays its recorded permutation of the observed
// in-members in the slots they occupy; an unobserved member keeps its slot.
func TestReplayRandomSort(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "B", "C")
	s.add(after("A"), "D") // another phone's add the shuffling phone did not see
	s.shuffle("C", "A", "B")
	s.expect(t, "C,D,A,B")
}

// A random sort permutes only the in-members it observed (PROTOCOL §7.6): a permutation member
// outside the mapped observed set keeps its slot.
func TestReplayRandomSortOnlyObserved(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "B", "C")
	s.node(&Step{Kind: StepSort, Order: "random", Perm: []Entry{"C", "B", "A"}, Observed: []Entry{"A", "C"}})
	s.expect(t, "C,B,A")
	s.node(&Step{Kind: StepSort, Order: "random", Perm: []Entry{"B", "A", "C"}, Observed: nil})
	s.expect(t, "C,B,A")
}

// A move's ghost-only member (a generation that is not live) creates its
// missing ghost but never moves an existing entry.
func TestReplayGhostOnlyMoveMember(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "B")
	s.node(&Step{Kind: StepMove, Run: []Entry{"B"}, Place: Place{Kind: PlaceStart}, GhostOnly: map[Entry]bool{"B": true}})
	s.expect(t, "A,B")
	s.node(&Step{Kind: StepMove, Run: []Entry{"G"}, Place: Place{Kind: PlaceStart}, GhostOnly: map[Entry]bool{"G": true}})
	s.add(after("G"), "X")
	s.expect(t, "X,A,B")
}

// A permutation naming two aliases of one survivor keeps the first.
func TestReplayPermutationDeduplicates(t *testing.T) {
	s := newScript()
	s.add(end(), "Q", "E")
	s.shuffle("E", "Q", "E")
	s.expect(t, "E,Q")
}

// PROTOCOL §7.6: partial bulk duplicates place only the absent members; a wholly
// duplicate add sorts nothing.
func TestReplayPartialDuplicates(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "Z")
	s.add(end(), "B", "A", "C")
	s.expect(t, "A,Z,B,C")
	s.policy(true, "episode_title_a_z")
	s.trigger(end(), []Entry{"A", "Z", "B", "C"}, "A") // places nothing: no sort
	s.expect(t, "A,Z,B,C")
	s.trigger(end(), []Entry{"A", "Z", "B", "C"}, "D") // places D: sorts
	s.expect(t, "A,B,C,D,Z")
}

// PROTOCOL §7.6: a bulk add with a member on a closed generation (its membership
// never takes effect) places the others and leaves the closed one a ghost.
func TestReplayBulkWithClosedMember(t *testing.T) {
	s := newScript()
	s.add(end(), "Z")
	s.node(&Step{Kind: StepPlace, Run: []Entry{"A", "C"}, Place: front(), Values: s.values("A", "C")}, ins("A")...)
	s.expect(t, "A,Z")
	if got := joined(s.run().Structure()); got != "A,C,Z" {
		t.Fatalf("structure %s", got)
	}
}

// A run naming two aliases of one survivor keeps the first; a run
// anchored to its own (merged) member goes after the anchor's nearest
// predecessor outside the run.
func TestReplayMergedRunAndSelfAnchor(t *testing.T) {
	s := newScript()
	s.add(end(), "P", "E", "Q")
	s.remove("E")
	// [E2, X] after E1, with E1 and E2 one survivor E.
	s.add(after("E"), "E", "X", "E")
	s.expect(t, "P,E,X,Q")
}

// Move to top of a selected run [B,D] gives B,D,A,C.
func TestReplayBulkMoveToTop(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "B", "C", "D")
	s.move(front(), "B", "D")
	s.expect(t, "B,D,A,C")
}

// Moves act on out entries too: a move never adds.
func TestReplayMoveOfOutEntry(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "B", "C")
	s.remove("A")
	s.move(after("C"), "A")
	s.expect(t, "B,C")
	if got := joined(s.run().Structure()); got != "B,C,A" {
		t.Fatalf("structure %s", got)
	}
	s.add(front(), "A") // a later add supplies its own place
	s.expect(t, "A,B,C")
}

// An anchor without any entry (a client fault): the end, with a diagnostic.
func TestReplayMissingAnchor(t *testing.T) {
	s := newScript()
	s.add(end(), "A", "B")
	s.add(after("Nowhere"), "X")
	st, diags := s.replay()
	if got := joined(st.Visible()); got != "A,B,X" {
		t.Fatalf("queue %s", got)
	}
	if len(diags) != 1 || diags[0].Anchor != "Nowhere" {
		t.Fatalf("diagnostics %+v", diags)
	}
}

// An import is one step for the whole imported queue, in the backup's
// global order, however its podcasts' decisions are ordered.
func TestReplayImportGlobalOrder(t *testing.T) {
	s := newScript()
	// import.episode decisions, podcast by podcast: A1, A2, then B1; their
	// episodes wait for the import's queue step.
	hold := &Step{Kind: StepHold}
	s.node(hold, ins("A1")...)
	s.node(hold, ins("A2")...)
	s.node(hold, ins("B1")...)
	s.expect(t, "")
	// The manifest: the global Queue order.
	s.node(&Step{Kind: StepImport, Run: []Entry{"A1", "B1", "A2"}, Values: s.values("A1", "B1", "A2")})
	s.expect(t, "A1,B1,A2")
	// Placements after the import anchor to its entries.
	s.add(after("B1"), "X")
	s.expect(t, "A1,B1,X,A2")
}

// At the replay: a completion's removal at 30 and a sort at 40 give [A];
// a claim at 25 that cancels the completion gives [A,Z].
func TestReplayCancelledCompletionJoinsTheSort(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		s := newScript()
		s.add(end(), "Z", "A")
		if !cancelled {
			s.remove("Z") // the completion bundle fired at its place
		} else {
			s.node(nil) // the completion: its bundle did not fire
		}
		s.sortBy("episode_title_a_z", "Z", "A")
		if cancelled {
			s.expect(t, "A,Z")
		} else {
			s.expect(t, "A")
		}
	}
}

// Cancelling the only explicit removal reactivates an earlier automatic
// add, which then takes part in a sort before the removal's place.
func TestReplayReactivatedAutomaticAdd(t *testing.T) {
	for _, reactivated := range []bool{false, true} {
		s := newScript()
		s.add(end(), "Z")
		s.auto(reactivated, end(), "D")
		s.add(end(), "A")
		s.sortBy("episode_title_a_z", "Z", "D", "A")
		if reactivated {
			s.expect(t, "A,D,Z")
		} else {
			s.expect(t, "A,Z")
		}
	}
}

// Ghosts keep their slots during a sort; a later anchor to the ghost
// lands between its new neighbours.
func TestReplayGhostAnchorAfterSort(t *testing.T) {
	s := newScript()
	s.add(end(), "Z")
	s.auto(false, end(), "G")
	s.add(end(), "A")
	s.sortBy("episode_title_a_z", "Z", "A")
	if got := joined(s.run().Structure()); got != "A,G,Z" {
		t.Fatalf("structure %s", got)
	}
	s.add(after("G"), "X")
	s.expect(t, "A,X,Z")
}

// PROTOCOL §7.6: empty and no-op sorts.
func TestReplayEmptyAndNoopSorts(t *testing.T) {
	s := newScript()
	s.sortBy("episode_title_a_z")
	s.expect(t, "")
	s.add(end(), "A", "B")
	s.sortBy("episode_title_a_z", "A", "B")
	s.expect(t, "A,B")
}

// PROTOCOL §7.6: every canonical order, with ties: the in-members are exactly the
// stable canonical sort of the queue, ghosts and out entries in between.
func TestReplayEveryOrderWithTies(t *testing.T) {
	vals := map[Entry]Values{
		"A": {Title: "b", PodcastID: "p2", PodcastTitle: "y", PublicationTime: 3, Duration: 10},
		"B": {Title: "a", PodcastID: "p1", PodcastTitle: "x", PublicationTime: 1, Duration: 20},
		"C": {Title: "b", PodcastID: "p2", PodcastTitle: "x", PublicationTime: 1, Duration: 10},
		"D": {Title: "c", PodcastID: "p1", PodcastTitle: "y", PublicationTime: 2, Duration: 20},
		"E": {Title: "a", PodcastID: "p3", PodcastTitle: "z", PublicationTime: 3, Duration: 5},
	}
	for order := range Orders {
		if order == "random" {
			continue
		}
		s := newScript()
		s.titles = vals
		s.add(end(), "A", "B")
		s.auto(false, end(), "G")
		s.add(end(), "C", "D", "X", "E")
		s.remove("X")
		s.sortBy(order, "A", "B", "C", "D", "E")
		var items []Item
		for _, e := range []Entry{"A", "B", "C", "D", "E"} {
			v := vals[e]
			items = append(items, Item{EpisodeID: string(e), PodcastID: v.PodcastID, Title: v.Title, PodcastTitle: v.PodcastTitle,
				PublicationTime: v.PublicationTime, Duration: v.Duration})
		}
		want, err := Sort(order, items)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, it := range want {
			ids = append(ids, it.EpisodeID)
		}
		t.Run(order, func(t *testing.T) { s.expect(t, strings.Join(ids, ",")) })
	}
}

// PROTOCOL §7.6: a sort uses its trigger's recorded values, and for a member its
// record does not cover, the values of that member's latest placement at or
// before the sort; a later placement never rewrites them.
func TestReplaySortValues(t *testing.T) {
	s := newScript()
	s.titles["X"] = Values{Title: "Z"}
	s.titles["Y"] = Values{Title: "M"}
	s.add(end(), "X", "Y")
	// The sort's record covers only Y (X's placement was admitted later but
	// is earlier in the order): X's own placement values, title Z.
	s.node(&Step{Kind: StepSort, Order: "episode_title_a_z", Values: map[Entry]Values{"Y": {Title: "M"}}})
	s.expect(t, "Y,X")
	// A trigger that recorded X as A sorts it first.
	s.node(&Step{Kind: StepSort, Order: "episode_title_a_z", Values: map[Entry]Values{"X": {Title: "A"}, "Y": {Title: "M"}}})
	s.expect(t, "X,Y")
}

// PROTOCOL §7.6: old and new generations of one podcast are different entries: an
// anchor recorded against the closed G1 never follows G2's new placement.
func TestReplayGenerationClasses(t *testing.T) {
	s := newScript()
	s.add(end(), "Z")
	// G1's X: placed on a generation that closes (its membership never
	// takes effect: hidden), so only a ghost.
	s.node(&Step{Kind: StepPlace, Run: []Entry{"g1:X"}, Place: end(), Values: s.values("g1:X")})
	s.add(end(), "A")
	s.add(front(), "g2:X") // re-subscribed: a new entry
	s.add(after("g1:X"), "Y")
	s.expect(t, "g2:X,Z,Y,A")
}

// A member that becomes in without a step placing it has a place at the end.
func TestReplayOrphanMember(t *testing.T) {
	s := newScript()
	s.add(end(), "A")
	s.node(nil, ins("B")...)
	s.expect(t, "A,B")
}

func randomID() [16]byte {
	var u [16]byte
	rand.Read(u[:])
	return u
}

func cand(t *testing.T, low, high *contract.QueueKey) Candidate {
	u := randomID()
	k, err := contract.QueueKeyBetween(low, high, u)
	if err != nil {
		t.Fatal(err)
	}
	return Candidate{Key: k, PositionID: u, OK: true}
}

// PROTOCOL §7.6: keys are kept where they already fit; otherwise the first longest
// increasing run keeps its keys and the others get fresh ones, in order.
func TestMaterialize(t *testing.T) {
	a := cand(t, nil, nil)
	b := cand(t, &a.Key, nil)
	c := cand(t, &b.Key, nil)
	cands := map[Entry]Candidate{"A": a, "B": b, "C": c}
	got, err := Materialize([]Entry{"A", "B", "C"}, cands, randomID)
	if err != nil {
		t.Fatal(err)
	}
	for i, x := range got {
		if !x.Kept || x.Key != cands[x.Entry].Key {
			t.Fatalf("member %d not kept", i)
		}
	}
	got, err = Materialize([]Entry{"C", "A", "N", "B"}, cands, randomID)
	if err != nil {
		t.Fatal(err)
	}
	kept := ""
	for i, x := range got {
		if x.Kept {
			kept += string(x.Entry)
		}
		if i > 0 && contract.CompareQueueKeys(got[i-1].Key, x.Key) >= 0 {
			t.Fatalf("keys do not increase at %d", i)
		}
		if !contract.QueueKeyCarries(x.Key, x.PositionID) {
			t.Fatalf("key %d does not carry its position ID", i)
		}
	}
	if kept != "AB" {
		t.Fatalf("kept %q, want the first longest run AB", kept)
	}
}
