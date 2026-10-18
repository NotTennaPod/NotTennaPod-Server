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
	"math"
	"sort"
	"sync/atomic"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
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

// State is the replay's state after some prefix of the order (PROTOCOL §7.6's
// snapshot): the structure with ghosts and out entries, membership at that
// step, the sorting register, the sort-value index and the key candidates.
//
// It is persistent: every part is a persistent tree (ptree), so Clone copies
// a few roots and a kept snapshot never changes when its clone moves on. The
// structure orders entries by label (order maintenance: labels grow along
// the structure; they are internal, only their order is meaningful); inOrder
// is the same for the in-members alone, so the queue is read in O(live).
type State struct {
	structure  ptree.Tree[uint64, Entry] // every entry, by label
	inOrder    ptree.Tree[uint64, Entry] // the in-members that have an entry, by label
	at         ptree.Tree[Entry, uint64] // each entry's label
	in         ptree.Tree[Entry, struct{}]
	values     ptree.Tree[Entry, Values] // values recorded with each entry's latest placement (PROTOCOL §7.6)
	keys       KeyMap                    // each entry's highest-ranked key write so far (PROTOCOL §7.6)
	journal    *wrote                    // the key writes, newest first (WrittenSince)
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

// wrote is one key write: the entry, and the node count it was made at
// (the State's Count while its node was applied). A persistent list: states
// that share a past share its cells.
type wrote struct {
	entry Entry
	count int
	next  *wrote
}

func compareLabels(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func compareEntries(a, b Entry) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func entryPriority(e Entry) uint64 { return ptree.Hash(string(e)) }

var (
	labelSpec  = &ptree.Spec[uint64, Entry]{Compare: compareLabels, Priority: ptree.SplitMix64}
	atSpec     = &ptree.Spec[Entry, uint64]{Compare: compareEntries, Priority: entryPriority}
	inSpec     = &ptree.Spec[Entry, struct{}]{Compare: compareEntries, Priority: entryPriority}
	valuesSpec = &ptree.Spec[Entry, Values]{Compare: compareEntries, Priority: entryPriority}
	keysSpec   = &ptree.Spec[Entry, Key]{Compare: compareEntries, Priority: entryPriority}
)

// KeyMap is a persistent map of entries' keys: an update returns a new map
// and copies only the path it changes, so a kept map never changes. The zero
// KeyMap is empty.
type KeyMap struct{ t ptree.Tree[Entry, Key] }

// Get returns e's key.
func (m KeyMap) Get(e Entry) (Key, bool) { return m.t.Get(e) }

// Set returns the map with e's key set to k.
func (m KeyMap) Set(e Entry, k Key) KeyMap {
	if m.t.Len() == 0 {
		m.t = ptree.New(keysSpec)
	}
	return KeyMap{m.t.Set(e, k)}
}

// Delete returns the map without e.
func (m KeyMap) Delete(e Entry) KeyMap {
	if m.t.Len() == 0 {
		return m
	}
	return KeyMap{m.t.Delete(e)}
}

// Len is the number of entries.
func (m KeyMap) Len() int { return m.t.Len() }

// All walks the entries in entry order.
func (m KeyMap) All() func(yield func(Entry, Key) bool) {
	if m.t.Len() == 0 {
		return func(func(Entry, Key) bool) {}
	}
	return m.t.All()
}

// KeyMapOf is the persistent map of a map's entries.
func KeyMapOf(src map[Entry]Key) KeyMap {
	es := make([]Entry, 0, len(src))
	for e := range src {
		es = append(es, e)
	}
	sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
	ks := make([]Key, len(es))
	for i, e := range es {
		ks[i] = src[e]
	}
	t, err := ptree.FromSorted(keysSpec, es, ks)
	if err != nil {
		panic(err) // distinct map keys, sorted: never
	}
	return KeyMap{t}
}

// Default sorting register (Android 3.12.1: keep-sorted off, date new→old).
const defaultSortOrder = "date_new_old"

// NewState is the empty queue (a new account, PROTOCOL §7.6).
func NewState() *State {
	return &State{structure: ptree.New(labelSpec), inOrder: ptree.New(labelSpec), at: ptree.New(atSpec), in: ptree.New(inSpec),
		values: ptree.New(valuesSpec), sortOrder: defaultSortOrder}
}

// Clone copies the state (a snapshot): O(1), the trees are shared.
func (s *State) Clone() *State {
	c := *s
	return &c
}

// Visible is the queue: the in-members in structural order.
func (s *State) Visible() []Entry {
	out := make([]Entry, 0, s.inOrder.Len())
	for _, e := range s.inOrder.All() {
		out = append(out, e)
	}
	return out
}

// Structure is every entry in structural order, ghosts and out entries
// included (tests and diagnostics).
func (s *State) Structure() []Entry {
	out := make([]Entry, 0, s.structure.Len())
	for _, e := range s.structure.All() {
		out = append(out, e)
	}
	return out
}

// In reports whether an entry is in the queue at this step.
func (s *State) In(e Entry) bool {
	_, ok := s.in.Get(e)
	return ok
}

// KeyOf is an entry's highest-ranked key write from the replay's steps.
func (s *State) KeyOf(e Entry) (Key, bool) { return s.keys.Get(e) }

// KeyWrites are every entry's highest-ranked key write from the replay's
// steps (a new map: O(entries), for a full publication).
func (s *State) KeyWrites() map[Entry]Key {
	out := make(map[Entry]Key, s.keys.Len())
	for e, k := range s.keys.All() {
		out[e] = k
	}
	return out
}

// KeyMap is every entry's highest-ranked key write from the replay's steps,
// as a persistent map (shared, O(1)).
func (s *State) KeyMap() KeyMap { return s.keys }

// WrittenSince lists the entries whose key write changed while the nodes
// from count on were applied (count is a Count of a state this one descends
// from), each once: O(those writes).
func (s *State) WrittenSince(count int) []Entry {
	var out []Entry
	seen := map[Entry]bool{}
	for w := s.journal; w != nil && w.count >= count; w = w.next {
		if !seen[w.entry] {
			seen[w.entry] = true
			out = append(out, w.entry)
		}
	}
	return out
}

// Sorting is the sorting register at this step.
func (s *State) Sorting() (bool, string) { return s.keepSorted, s.sortOrder }

// SortValues is the value index entry of e (its latest placement's values).
func (s *State) SortValues(e Entry) (Values, bool) { return s.values.Get(e) }

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
			before[e] = s.In(e)
		}
	}
	var turned []Entry
	for _, ev := range n.Events {
		if ev.In {
			if !s.In(ev.Entry) {
				turned = append(turned, ev.Entry)
			}
			s.setIn(ev.Entry, true)
		} else {
			s.setIn(ev.Entry, false)
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
					if !s.has(e) {
						s.ensure(e)
						run = append(run, e)
					}
					continue
				}
				s.ensure(e)
				if v, ok := st.Values[e]; ok {
					s.values = s.values.Set(e, v)
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
			// In entry order: the journal is then the same whatever order a
			// map iteration gives.
			es := make([]Entry, 0, len(st.Keys))
			for e := range st.Keys {
				es = append(es, e)
			}
			sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
			for _, e := range es {
				s.write(e, st)
			}
		case StepImport:
			var run []Entry
			for _, e := range dedup(st.Run) {
				if s.In(e) {
					s.record(n.ID, e, st)
					run = append(run, e)
				} else if !s.has(e) {
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
			if !s.has(e) && s.In(e) {
				s.ensure(e)
			}
		}
	}
	s.Count++
	s.Last = n.ID
	return diags
}

// has reports whether e has a structural entry.
func (s *State) has(e Entry) bool {
	_, ok := s.at.Get(e)
	return ok
}

// setIn sets e's membership, keeping inOrder (the in-members with an entry).
func (s *State) setIn(e Entry, in bool) {
	_, was := s.in.Get(e)
	if was == in {
		return
	}
	l, linked := s.at.Get(e)
	if in {
		s.in = s.in.Set(e, struct{}{})
		if linked {
			s.inOrder = s.inOrder.Set(l, e)
		}
		return
	}
	s.in = s.in.Delete(e)
	if linked {
		s.inOrder = s.inOrder.Delete(l)
	}
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
		case s.In(e):
			placed = true
			s.record(node, e, st)
			run = append(run, e)
		default:
			if !s.has(e) {
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
		s.values = s.values.Set(e, v)
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
	if cur, has := s.keys.Get(e); has && !cur.Rank.Less(k.Rank) {
		return
	}
	s.keys = s.keys.Set(e, k)
	s.journal = &wrote{entry: e, count: s.Count, next: s.journal}
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

// ensure gives an entry a place (at the end) if it has none.
func (s *State) ensure(e Entry) {
	if s.has(e) {
		return
	}
	tail, ok := s.last()
	s.link(e, tail, ok)
}

// last is the last entry's label.
func (s *State) last() (uint64, bool) {
	l, _, ok := s.structure.Last().Next()
	return l, ok
}

// before is the label of the entry before the one labelled l.
func (s *State) before(l uint64) (uint64, bool) {
	if l == 0 {
		return 0, false
	}
	p, _, ok := s.structure.SeekRev(l - 1).Next()
	return p, ok
}

// insert takes the run's entries out and inserts them, in order, at the
// place, resolved against the structure (ghosts included): an anchor in the
// run itself means its nearest structural predecessor outside the run (or the
// start); an anchor without an entry means the end, with a diagnostic.
func (s *State) insert(run []Entry, p Place, node string) (Diagnostic, bool) {
	if len(run) == 0 {
		return Diagnostic{}, true
	}
	inRun := make(map[Entry]bool, len(run))
	for _, e := range run {
		inRun[e] = true
	}
	ok := true
	var diag Diagnostic
	var anchor Entry // the entry the run goes after
	hasAnchor := false
	toEnd := false
	switch p.Kind {
	case PlaceStart:
	case PlaceAfter:
		l, has := s.at.Get(p.After)
		if !has {
			toEnd, ok, diag = true, false, Diagnostic{Node: node, Anchor: p.After}
			break
		}
		a := p.After
		for inRun[a] {
			prev, more := s.before(l)
			if !more {
				break
			}
			l = prev
			a, _ = s.structure.Get(l)
		}
		if !inRun[a] {
			anchor, hasAnchor = a, true
		}
	default:
		toEnd = true
	}
	for _, e := range run {
		s.unlink(e)
	}
	if toEnd {
		if l, has := s.last(); has {
			anchor, hasAnchor = s.structure.Get(l)
		}
	}
	for _, e := range run {
		var l uint64
		if hasAnchor {
			l, _ = s.at.Get(anchor)
		}
		s.link(e, l, hasAnchor)
		anchor, hasAnchor = e, true
	}
	return diag, ok
}

// unlink takes e out of the structure (its label stays in at until link
// gives it a new one).
func (s *State) unlink(e Entry) {
	l, ok := s.at.Get(e)
	if !ok {
		return
	}
	s.structure = s.structure.Delete(l)
	s.inOrder = s.inOrder.Delete(l)
}

// label gives e the label l in every index.
func (s *State) label(e Entry, l uint64) {
	s.structure = s.structure.Set(l, e)
	s.at = s.at.Set(e, l)
	if s.In(e) {
		s.inOrder = s.inOrder.Set(l, e)
	}
}

// labelMember gives the in-member e the label l (a sort's permutation).
func (s *State) labelMember(e Entry, l uint64) {
	s.structure = s.structure.Set(l, e)
	s.at = s.at.Set(e, l)
	s.inOrder = s.inOrder.Set(l, e)
}

// link places e (not in the structure) right after the entry labelled lo
// (hasLo false: at the start) and labels it: between its neighbours' labels,
// a fixed step past the last one at the end, else after relabelling the
// smallest aligned label range around the place that is sparse enough.
func (s *State) link(e Entry, lo uint64, hasLo bool) {
	var it *ptree.Iter[uint64, Entry]
	if hasLo {
		it = s.structure.Seek(lo + 1)
	} else {
		it = s.structure.First()
	}
	hi, _, hasHi := it.Next()
	low, high := lo, uint64(math.MaxUint64)
	if !hasLo {
		low = 0
	}
	if hasHi {
		high = hi
	}
	const step = 1 << 32
	switch {
	case !hasHi && high-low > step:
		s.label(e, low+step)
	case high-low >= 2:
		s.label(e, low+(high-low)/2)
	default:
		s.relabel(e, lo, hasLo, hi)
	}
}

// capacity[i] is the most entries an aligned label range of 2^i labels may
// hold after a relabelling: (4/3)^i (list labelling with density threshold
// (3/2)^-i, Bender et al.), so relabelling costs O(log n) amortized entries.
var capacity = func() [65]uint64 {
	var c [65]uint64
	for i := range c {
		c[i] = uint64(math.Pow(4.0/3.0, float64(i)))
	}
	return c
}()

// relabelled counts the entries relabelled since the process started (tests).
var relabelled atomic.Int64

// relabel places e right after the entry labelled lo (hasLo false: before
// the entry labelled hi, the first) when no label is free there: it finds the
// smallest aligned range of labels around the place whose entries, e
// included, are within its capacity, and spaces them evenly in it. Labels
// outside the range and the order of all entries are unchanged.
func (s *State) relabel(e Entry, lo uint64, hasLo bool, hi uint64) {
	point := hi
	if hasLo {
		point = lo
	}
	for i := 1; i <= 64; i++ {
		var base, size uint64
		var count int
		if i == 64 {
			count = s.structure.Len()
		} else {
			size = 1 << i
			base = point &^ (size - 1)
			end := base + size
			if end < base {
				end = math.MaxUint64 // the top range (no entry is labelled MaxUint64)
			}
			count = s.structure.Rank(end) - s.structure.Rank(base)
		}
		if uint64(count+1) > capacity[i] && i < 64 {
			continue
		}
		// The range's entries in order, e at its place.
		var seq []Entry
		var old []uint64
		if !hasLo {
			seq = append(seq, e)
		}
		for it := s.structure.Seek(base); ; {
			l, x, ok := it.Next()
			if !ok || (i < 64 && l-base >= size) {
				break
			}
			old = append(old, l)
			seq = append(seq, x)
			if hasLo && l == lo {
				seq = append(seq, e)
			}
		}
		for _, l := range old {
			s.structure = s.structure.Delete(l)
			s.inOrder = s.inOrder.Delete(l)
		}
		m := uint64(len(seq))
		var gap uint64
		if i == 64 {
			gap = math.MaxUint64 / m
		} else {
			gap = size / m
		}
		for j, x := range seq {
			s.label(x, base+uint64(j)*gap+gap/2)
		}
		relabelled.Add(int64(len(seq)))
		return
	}
}

// members are the in-members' labels and entries in structural order.
func (s *State) members() ([]uint64, []Entry) {
	ls := make([]uint64, 0, s.inOrder.Len())
	es := make([]Entry, 0, s.inOrder.Len())
	for l, e := range s.inOrder.All() {
		ls = append(ls, l)
		es = append(es, e)
	}
	return ls, es
}

// sortIn stable-sorts the in-members by a canonical order, permuting them
// among the places they occupy (out entries and ghosts keep theirs): values
// come from the trigger's record, else from each member's latest placement
// (PROTOCOL §7.6), else Android's defaults.
func (s *State) sortIn(order string, trigger map[Entry]Values) {
	labels, entries := s.members()
	if len(labels) < 2 {
		return
	}
	items := make([]Item, len(entries))
	for i, e := range entries {
		v, ok := trigger[e]
		if !ok {
			if v, ok = s.values.Get(e); !ok && s.Fallback != nil {
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
		if e := Entry(it.EpisodeID); e != entries[i] {
			s.labelMember(e, labels[i])
		}
	}
}

// permute orders the in-members a random sort observed by its recorded
// permutation, in the places they occupy; other in-members keep theirs.
func (s *State) permute(perm, observed []Entry) {
	seen := make(map[Entry]bool, len(observed))
	for _, e := range observed {
		seen[e] = true
	}
	var order []Entry
	var labels []uint64
	for _, e := range perm {
		if l, ok := s.at.Get(e); ok && s.In(e) && seen[e] {
			order = append(order, e)
			labels = append(labels, l)
		}
	}
	sort.Slice(labels, func(a, b int) bool { return labels[a] < labels[b] })
	for i, e := range order {
		if cur, _ := s.structure.Get(labels[i]); cur != e {
			s.labelMember(e, labels[i])
		}
	}
}
