package queue

// The queue replay (PROTOCOL §7.6): the queue's order is what
// one device would show had every queue command been made on it, one after
// another, in the deterministic order (PROTOCOL §6.9), each executed as Android
// 3.12.1 executes it at the place its device resolved.
//
// The replay is a pure function of its inputs, given here node by node in the deterministic order:
// each node's membership events (the episode fold's effects of its membership facts that took
// effect at its place) and its replay step, if it is one (PROTOCOL §7.6). The caller maps every
// reference through the final identity and generation mapping (PROTOCOL §7.6) before handing it
// over; entries are episodes in one generation class.

import (
	"sort"
)

// Entry is a structural entry: an episode in one generation class (the
// caller's episode scope).
type Entry string

// PlaceKind is a placement's resolved gap (PROTOCOL §7.6).
type PlaceKind uint8

const (
	PlaceEnd PlaceKind = iota
	PlaceStart
	PlaceAfter
)

// Place is where a placement command goes: the start, right after an entry,
// or the end.
type Place struct {
	Kind  PlaceKind
	After Entry // for PlaceAfter
}

// Values are an entry's canonical sort values as the server recorded them
// (PROTOCOL §7.6).
type Values struct {
	Title, PodcastID, PodcastTitle string
	PublicationTime, Duration      int64
}

// Rank is a key write's original receipt rank (PROTOCOL §7.6): its
// commit's (epoch, seq), its index in the commit (a fact first, then its
// decisions, then the commit's layout) and the writing node's ID.
type Rank struct {
	Epoch, Seq, Index int64
	ID                string
}

// Less orders ranks.
func (r Rank) Less(o Rank) bool {
	if r.Epoch != o.Epoch {
		return r.Epoch < o.Epoch
	}
	if r.Seq != o.Seq {
		return r.Seq < o.Seq
	}
	if r.Index != o.Index {
		return r.Index < o.Index
	}
	return r.ID < o.ID
}

// Key is a key write: a placement's key with its position ID, as its command
// carried it (a device's prediction, a server resolution's, a filing's or an
// import's), at its writer's rank.
type Key struct {
	Key, PositionID string
	Rank            Rank
}

// StepKind is a replay step's kind.
type StepKind uint8

const (
	// StepPlace is an add, a restore, a play bundle, a filing or a
	// server-resolved add: it places the members not queued immediately
	// before it.
	StepPlace StepKind = iota + 1
	// StepMove is a move or a multi-item move: the run goes to its place.
	StepMove
	// StepSort is the sort dialog: a canonical order or a recorded
	// permutation of the in-members.
	StepSort
	// StepPolicy sets the sorting register.
	StepPolicy
	// StepImport is an import's whole queue at the end, in the backup's
	// global order (PROTOCOL §7.6).
	StepImport
	// StepHold is an imported episode's membership: it waits for its
	// import's queue step for a place (until then it has none and is not
	// shown, so the queue is never assembled from partial podcast queues).
	// Its key write exists at once (PROTOCOL §7.6: an imported key exists before the
	// import completes).
	StepHold
)

// Step is one replay step.
type Step struct {
	Kind    StepKind
	Restore bool    // a restore never sorts
	Run     []Entry // members in their requested order (mapped; duplicates are dropped here, first kept)
	// Keys are the step's key writes, for the members whose own checks
	// passed (PROTOCOL §7.6 selector): a placement's count for the members it
	// actually places, a move's for every member, an import episode's
	// (StepHold) at once.
	Keys  map[Entry]Key
	Place Place
	Order string  // StepSort: the sort order ("random" uses Permutation)
	Perm  []Entry // StepSort, random: the device's resulting order of what it observed
	// Observed is a sort's mapped observed episodes: a random sort permutes
	// only the in-members both observed and in its permutation (PROTOCOL §7.6).
	Observed []Entry
	// GhostOnly marks a move's members whose generation is not live: they
	// create their missing ghost and never move an existing entry.
	GhostOnly map[Entry]bool
	Sorted    bool             // StepPolicy: keep-sorted
	SortBy    string           // StepPolicy: its sort order
	Values    map[Entry]Values // recorded with this step: its own members' and, for a sort trigger, every member then in the queue
}

// Event is a membership effect that took effect at its node: the entry is in
// or out of the queue from there on.
type Event struct {
	Entry Entry
	In    bool
}

// Node is one node of the deterministic order with its membership events and
// its step (nil when it is not a replay step).
type Node struct {
	ID     string
	Events []Event
	Step   *Step
}

// slot is one entry's place in the structure: a doubly linked list over a
// slice, ordered by label (order maintenance: labels grow along the list).
type slot struct {
	entry      Entry
	prev, next int32
	label      uint64
}

// State is the replay's state after some prefix of the order (PROTOCOL §7.6's
// snapshot): the structure with ghosts and out entries, membership at that
// step, the sorting register, the sort-value index and the key candidates.
type State struct {
	slots      []slot
	head, tail int32
	at         map[Entry]int32
	in         map[Entry]bool
	values     map[Entry]Values // values recorded with each entry's latest placement (PROTOCOL §7.6)
	keys       map[Entry]Key    // each entry's highest-ranked key write so far (PROTOCOL §7.6)
	keepSorted bool
	sortOrder  string
	// Count is the number of nodes processed; Last the last one's ID.
	Count int
	Last  string
	// Fallback gives the values of an in-member no step recorded any for
	// (only inputs a server would not have written): its current canonical
	// values (the reference's reading). Set by the caller
	// for each run; nil gives Android's defaults.
	Fallback func(Entry) Values
}

// Default sorting register (Android 3.12.1: keep-sorted off, date new→old).
const defaultSortOrder = "date_new_old"

// NewState is the empty queue (a new account, PROTOCOL §7.6).
func NewState() *State {
	return &State{head: -1, tail: -1, at: map[Entry]int32{}, in: map[Entry]bool{}, values: map[Entry]Values{},
		keys: map[Entry]Key{}, sortOrder: defaultSortOrder}
}

// Clone copies the state (a snapshot).
func (s *State) Clone() *State {
	c := *s
	c.slots = append([]slot(nil), s.slots...)
	c.at = make(map[Entry]int32, len(s.at))
	for k, v := range s.at {
		c.at[k] = v
	}
	c.in = make(map[Entry]bool, len(s.in))
	for k, v := range s.in {
		c.in[k] = v
	}
	c.values = make(map[Entry]Values, len(s.values))
	for k, v := range s.values {
		c.values[k] = v
	}
	c.keys = make(map[Entry]Key, len(s.keys))
	for k, v := range s.keys {
		c.keys[k] = v
	}
	return &c
}

// Visible is the queue: the in-members in structural order.
func (s *State) Visible() []Entry {
	out := make([]Entry, 0, len(s.in))
	for i := s.head; i >= 0; i = s.slots[i].next {
		if e := s.slots[i].entry; s.in[e] {
			out = append(out, e)
		}
	}
	return out
}

// Structure is every entry in structural order, ghosts and out entries
// included (tests and diagnostics).
func (s *State) Structure() []Entry {
	out := make([]Entry, 0, len(s.slots))
	for i := s.head; i >= 0; i = s.slots[i].next {
		out = append(out, s.slots[i].entry)
	}
	return out
}

// In reports whether an entry is in the queue at this step.
func (s *State) In(e Entry) bool { return s.in[e] }

// KeyOf is an entry's highest-ranked key write from the replay's steps.
func (s *State) KeyOf(e Entry) (Key, bool) {
	k, ok := s.keys[e]
	return k, ok
}

// KeyWrites are every entry's highest-ranked key write from the replay's
// steps (read only).
func (s *State) KeyWrites() map[Entry]Key { return s.keys }

// Sorting is the sorting register at this step.
func (s *State) Sorting() (bool, string) { return s.keepSorted, s.sortOrder }

// SortValues is the value index entry of e (its latest placement's values).
func (s *State) SortValues(e Entry) (Values, bool) {
	v, ok := s.values[e]
	return v, ok
}

// Diagnostic reports an anchor without a structural entry: the command went
// to the end (PROTOCOL §7.6: a client fault, logged, never a failure).
type Diagnostic struct {
	Node   string
	Anchor Entry
}

// Apply processes one node: its membership events, then its step, then the
// entries that became members without a step placing them (appended at the
// end, so every member has a place).
func (s *State) Apply(n Node) []Diagnostic {
	var diags []Diagnostic
	st := n.Step
	var before map[Entry]bool
	if st != nil && (st.Kind == StepPlace || st.Kind == StepImport) {
		before = make(map[Entry]bool, len(st.Run))
		for _, e := range st.Run {
			before[e] = s.in[e]
		}
	}
	var turned []Entry
	for _, ev := range n.Events {
		if ev.In {
			if !s.in[ev.Entry] {
				turned = append(turned, ev.Entry)
			}
			s.in[ev.Entry] = true
		} else {
			delete(s.in, ev.Entry)
		}
	}
	if st != nil {
		switch st.Kind {
		case StepPlace:
			diags = s.place(n.ID, st, before)
		case StepMove:
			// A move is a placement too: it records its members' values. A
			// ghost-only member (a generation that is not live) only creates
			// its missing ghost; it never moves an existing entry.
			var run []Entry
			for _, e := range dedup(st.Run) {
				if st.GhostOnly[e] {
					if _, has := s.at[e]; !has {
						s.ensure(e)
						run = append(run, e)
					}
					continue
				}
				s.ensure(e)
				if v, ok := st.Values[e]; ok {
					s.values[e] = v
				}
				// A move writes for every member that carries a key, in
				// the queue or out (PROTOCOL §7.6).
				s.write(e, st)
				run = append(run, e)
			}
			if d, ok := s.insert(run, st.Place, n.ID); !ok {
				diags = append(diags, d)
			}
		case StepSort:
			if st.Order == "random" {
				s.permute(dedup(st.Perm), st.Observed)
			} else {
				s.sortIn(st.Order, st.Values)
			}
		case StepPolicy:
			s.keepSorted, s.sortOrder = st.Sorted, st.SortBy
			if s.sortOrder == "" {
				s.sortOrder = defaultSortOrder
			}
		case StepHold:
			for e := range st.Keys {
				s.write(e, st)
			}
		case StepImport:
			var run []Entry
			for _, e := range dedup(st.Run) {
				if s.in[e] {
					s.record(n.ID, e, st)
					run = append(run, e)
				} else if _, has := s.at[e]; !has {
					run = append(run, e) // a ghost
				}
			}
			for _, e := range run {
				s.ensure(e)
			}
			s.insert(run, Place{Kind: PlaceEnd}, n.ID)
		}
	}
	if st == nil || st.Kind != StepHold {
		sort.Slice(turned, func(i, j int) bool { return turned[i] < turned[j] })
		for _, e := range turned {
			if _, has := s.at[e]; !has && s.in[e] {
				s.ensure(e)
			}
		}
	}
	s.Count++
	s.Last = n.ID
	return diags
}

// place executes an add-like step: only the members not queued immediately
// before it are placed (Android's addQueueItem and addQueueItemAt skip queued
// episodes); a member whose membership did not take effect (an automatic add
// that does not apply, a closed generation) leaves a ghost if it has no entry
// yet; if anything was placed under keep-sorted, the queue is sorted (never
// for a restore).
func (s *State) place(node string, st *Step, before map[Entry]bool) []Diagnostic {
	var run []Entry
	placed := false
	for _, e := range dedup(st.Run) {
		switch {
		case before[e]:
			// Already queued: no placement.
		case s.in[e]:
			placed = true
			s.record(node, e, st)
			run = append(run, e)
		default:
			if _, has := s.at[e]; !has {
				run = append(run, e) // a ghost, so later anchors resolve
			}
		}
	}
	var diags []Diagnostic
	if len(run) > 0 {
		for _, e := range run {
			s.ensure(e)
		}
		if d, ok := s.insert(run, st.Place, node); !ok {
			diags = append(diags, d)
		}
	}
	if placed && s.keepSorted && !st.Restore {
		s.sortIn(s.sortOrder, st.Values)
	}
	return diags
}

// record notes a placement's values and key write for an entry it placed.
func (s *State) record(node string, e Entry, st *Step) {
	if v, ok := st.Values[e]; ok {
		s.values[e] = v
	}
	s.write(e, st)
}

// write notes the step's key write for e if it ranks above e's current one
// (the published key is the latest write in rank order, whatever the order
// the replay meets the writes in).
func (s *State) write(e Entry, st *Step) {
	k, ok := st.Keys[e]
	if !ok {
		return
	}
	if cur, has := s.keys[e]; has && !cur.Rank.Less(k.Rank) {
		return
	}
	s.keys[e] = k
}

// dedup keeps the first occurrence of each entry.
func dedup(run []Entry) []Entry {
	if len(run) < 2 {
		return run
	}
	seen := make(map[Entry]bool, len(run))
	out := make([]Entry, 0, len(run))
	for _, e := range run {
		if !seen[e] {
			seen[e] = true
			out = append(out, e)
		}
	}
	return out
}

// ensure gives an entry a slot (at the end) if it has none.
func (s *State) ensure(e Entry) int32 {
	if i, ok := s.at[e]; ok {
		return i
	}
	i := int32(len(s.slots))
	s.slots = append(s.slots, slot{entry: e, prev: -1, next: -1})
	s.at[e] = i
	s.linkAfter(i, s.tail)
	return i
}

// insert takes the run's entries out and inserts them, in order, at the
// place, resolved against the structure (ghosts included): an anchor in the
// run itself means its nearest structural predecessor outside the run (or the
// start); an anchor without an entry means the end, with a diagnostic.
func (s *State) insert(run []Entry, p Place, node string) (Diagnostic, bool) {
	if len(run) == 0 {
		return Diagnostic{}, true
	}
	inRun := make(map[int32]bool, len(run))
	for _, e := range run {
		inRun[s.at[e]] = true
	}
	ok := true
	var diag Diagnostic
	anchor := int32(-1)
	toEnd := false
	switch p.Kind {
	case PlaceStart:
	case PlaceAfter:
		a, has := s.at[p.After]
		if !has {
			toEnd, ok, diag = true, false, Diagnostic{Node: node, Anchor: p.After}
			break
		}
		for a >= 0 && inRun[a] {
			a = s.slots[a].prev
		}
		anchor = a
	default:
		toEnd = true
	}
	for _, e := range run {
		s.unlink(s.at[e])
	}
	if toEnd {
		anchor = s.tail
	}
	for _, e := range run {
		i := s.at[e]
		s.linkAfter(i, anchor)
		anchor = i
	}
	return diag, ok
}

func (s *State) unlink(i int32) {
	sl := &s.slots[i]
	if sl.prev >= 0 {
		s.slots[sl.prev].next = sl.next
	} else {
		s.head = sl.next
	}
	if sl.next >= 0 {
		s.slots[sl.next].prev = sl.prev
	} else {
		s.tail = sl.prev
	}
	sl.prev, sl.next = -1, -1
}

// linkAfter links slot i after slot a (a < 0: at the start) and labels it.
func (s *State) linkAfter(i, a int32) {
	next := s.head
	if a >= 0 {
		next = s.slots[a].next
	}
	s.slots[i].prev, s.slots[i].next = a, next
	if a >= 0 {
		s.slots[a].next = i
	} else {
		s.head = i
	}
	if next >= 0 {
		s.slots[next].prev = i
	} else {
		s.tail = i
	}
	var lo, hi uint64
	if a >= 0 {
		lo = s.slots[a].label
	}
	hi = ^uint64(0)
	if next >= 0 {
		hi = s.slots[next].label
	}
	const step = 1 << 32
	switch {
	case next < 0 && hi-lo > step:
		s.slots[i].label = lo + step
	case hi-lo >= 2:
		s.slots[i].label = lo + (hi-lo)/2
	default:
		s.relabel()
	}
}

// relabel spaces every label evenly along the list.
func (s *State) relabel() {
	n := uint64(len(s.slots)) + 1
	gap := (^uint64(0) / 2) / n
	if gap == 0 {
		gap = 1
	}
	l := gap
	for i := s.head; i >= 0; i = s.slots[i].next {
		s.slots[i].label = l
		l += gap
	}
}

// members are the in-members' slots in structural order.
func (s *State) members() []int32 {
	out := make([]int32, 0, len(s.in))
	for e := range s.in {
		if i, ok := s.at[e]; ok {
			out = append(out, i)
		}
	}
	sort.Slice(out, func(a, b int) bool { return s.slots[out[a]].label < s.slots[out[b]].label })
	return out
}

// sortIn stable-sorts the in-members by a canonical order, permuting them
// among the slots they occupy (out entries and ghosts keep theirs): values
// come from the trigger's record, else from each member's latest placement
// (PROTOCOL §7.6), else Android's defaults.
func (s *State) sortIn(order string, trigger map[Entry]Values) {
	slots := s.members()
	if len(slots) < 2 {
		return
	}
	items := make([]Item, len(slots))
	for i, x := range slots {
		e := s.slots[x].entry
		v, ok := trigger[e]
		if !ok {
			if v, ok = s.values[e]; !ok && s.Fallback != nil {
				v = s.Fallback(e)
			}
		}
		items[i] = Item{EpisodeID: string(e), PodcastID: v.PodcastID, Title: v.Title, PodcastTitle: v.PodcastTitle,
			PublicationTime: v.PublicationTime, Duration: v.Duration}
	}
	sorted, err := Sort(order, items)
	if err != nil {
		return // validated orders only; an unknown one sorts nothing
	}
	for i, it := range sorted {
		e := Entry(it.EpisodeID)
		s.slots[slots[i]].entry = e
		s.at[e] = slots[i]
	}
}

// permute orders the in-members a random sort observed by its recorded
// permutation, in the slots they occupy; other in-members keep their slots.
func (s *State) permute(perm, observed []Entry) {
	seen := make(map[Entry]bool, len(observed))
	for _, e := range observed {
		seen[e] = true
	}
	var order []Entry
	var slots []int32
	for _, e := range perm {
		if i, ok := s.at[e]; ok && s.in[e] && seen[e] {
			order = append(order, e)
			slots = append(slots, i)
		}
	}
	sort.Slice(slots, func(a, b int) bool { return s.slots[slots[a]].label < s.slots[slots[b]].label })
	for i, e := range order {
		s.slots[slots[i]].entry = e
		s.at[e] = slots[i]
	}
}
