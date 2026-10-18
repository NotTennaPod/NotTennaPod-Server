package views

import (
	"container/heap"
	"slices"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// Lists as merges of ranges (PROTOCOL §12's episode lists and the queue).
//
// A list is a plan: ranges of one tree of the version, each a run of
// consecutive entries (a shown podcast's episodes of one filter class in a
// sort order, or a shown podcast's queued episodes), and the order of rows
// across ranges. Its rows are the k-way merge of the ranges. A plan keeps
// only its ranges (24 bytes each); every read places them in the tree
// (O(R log n)) and seeks on its own: a position in the list is a position
// in each range (Cursor), found from an offset by multi-range selection,
// from an episode by its rank in each range, or carried from the page
// before. Reading a page costs O(R log n + limit · log R) from a cursor,
// O(R · log n · log N) more to select an offset; R is the plan's ranges, N
// its rows.

// Cursor is a position in a list: the index in each of its ranges of the
// first row not yet read, at an offset (4 bytes a range).
type Cursor struct {
	Offset int
	pos    []int32
}

// span is a range of a plan: its podcast, its class (order trees only) and
// its length.
type span struct {
	podcast string
	class   Class
	n       int32
}

type plan[K any] struct {
	t     ptree.Tree[K, *EpisodeItem]
	spans []span
	total int
	lo    func(span) K            // the range's lower bound
	cmp   func(a, b K) int        // the rows' order, across ranges
	keyOf func(*EpisodeItem) K    // an episode's key, under its own range
	has   func(*EpisodeItem) bool // whether an episode has a key (a place) in the order
	rekey func(K, span) K         // a key's place under another range
}

// placed is a plan placed in its tree: each range's first index.
type placed[K any] struct {
	*plan[K]
	starts []int
}

func (p *plan[K]) add(s span) {
	if s.n > 0 {
		p.spans = append(p.spans, s)
		p.total += int(s.n)
	}
}

// done fixes the plan's ranges in an allocation of their exact length.
func (p *plan[K]) done() *plan[K] {
	p.spans = append(make([]span, 0, len(p.spans)), p.spans...)
	return p
}

func (p *plan[K]) place() placed[K] {
	starts := make([]int, len(p.spans))
	for r, s := range p.spans {
		starts[r] = p.t.Rank(p.lo(s))
	}
	return placed[K]{p, starts}
}

// below is the number of rows of range r before key k in the list's order.
func (p placed[K]) below(r int, k K) int32 {
	n := p.t.Rank(p.rekey(k, p.spans[r])) - p.starts[r]
	return int32(min(max(n, 0), int(p.spans[r].n)))
}

type head[K any] struct {
	r  int
	k  K
	e  *EpisodeItem
	it *ptree.Iter[K, *EpisodeItem]
}

type mergeHeap[K any] struct {
	items []head[K]
	cmp   func(a, b K) int
	rev   bool
}

func (h *mergeHeap[K]) Len() int { return len(h.items) }
func (h *mergeHeap[K]) Less(i, j int) bool {
	c := h.cmp(h.items[i].k, h.items[j].k)
	if h.rev {
		return c > 0
	}
	return c < 0
}
func (h *mergeHeap[K]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap[K]) Push(x any)    { h.items = append(h.items, x.(head[K])) }
func (h *mergeHeap[K]) Pop() any {
	x := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return x
}

// clonePos copies a cursor's positions into an allocation of their exact
// length.
func clonePos(p []int32) []int32 {
	out := make([]int32, len(p))
	copy(out, p)
	return out
}

// read returns up to limit rows from the cursor, and the cursor after them.
func (p placed[K]) read(c Cursor, limit int) ([]*EpisodeItem, Cursor) {
	next := Cursor{Offset: c.Offset, pos: clonePos(c.pos)}
	if limit <= 0 || c.Offset >= p.total {
		return nil, next
	}
	h := &mergeHeap[K]{cmp: p.cmp}
	for r, s := range p.spans {
		if c.pos[r] < s.n {
			it := p.t.SeekAt(p.starts[r] + int(c.pos[r]))
			k, e, _ := it.Next()
			h.items = append(h.items, head[K]{r, k, e, it})
		}
	}
	heap.Init(h)
	var rows []*EpisodeItem
	for len(rows) < limit && h.Len() > 0 {
		top := h.items[0]
		rows = append(rows, top.e)
		next.pos[top.r]++
		next.Offset++
		if next.pos[top.r] < p.spans[top.r].n {
			k, e, _ := top.it.Next()
			h.items[0] = head[K]{top.r, k, e, top.it}
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}
	}
	return rows, next
}

// back moves the cursor n rows back (fewer at the start).
func (p placed[K]) back(c Cursor, n int) Cursor {
	prev := Cursor{Offset: c.Offset, pos: clonePos(c.pos)}
	h := &mergeHeap[K]{cmp: p.cmp, rev: true}
	for r := range p.spans {
		if c.pos[r] > 0 {
			it := p.t.SeekAtRev(p.starts[r] + int(c.pos[r]) - 1)
			k, e, _ := it.Next()
			h.items = append(h.items, head[K]{r, k, e, it})
		}
	}
	heap.Init(h)
	for ; n > 0 && h.Len() > 0; n-- {
		top := h.items[0]
		prev.pos[top.r]--
		prev.Offset--
		if prev.pos[top.r] > 0 {
			k, e, _ := top.it.Next()
			h.items[0] = head[K]{top.r, k, e, top.it}
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}
	}
	return prev
}

// at is the cursor at an offset (clamped to the list): multi-range
// selection by weighted medians. Each round takes each range's median
// candidate, weighs it by the range's candidates, and ranks the weighted
// median M in every range; M's offset either is the one sought, or rules out
// the candidates on one side of M, at least a quarter of them.
func (p placed[K]) at(offset int) Cursor {
	offset = min(max(offset, 0), p.total)
	c := Cursor{Offset: offset, pos: make([]int32, len(p.spans))}
	if offset == 0 {
		return c
	}
	if offset == p.total {
		for r, s := range p.spans {
			c.pos[r] = s.n
		}
		return c
	}
	lo, hi := make([]int32, len(p.spans)), make([]int32, len(p.spans))
	for r, s := range p.spans {
		hi[r] = s.n
	}
	type cand struct {
		r int
		w int32
		k K
	}
	cands := make([]cand, 0, len(p.spans))
	below := make([]int32, len(p.spans))
	for {
		cands = cands[:0]
		var weight int64
		for r := range p.spans {
			if n := hi[r] - lo[r]; n > 0 {
				k, _, _ := p.t.Select(p.starts[r] + int(lo[r]+n/2))
				cands = append(cands, cand{r, n, k})
				weight += int64(n)
			}
		}
		slices.SortFunc(cands, func(a, b cand) int { return p.cmp(a.k, b.k) })
		var m cand
		var acc int64
		for i := range cands {
			if acc += int64(cands[i].w); 2*acc >= weight {
				m = cands[i]
				break
			}
		}
		// M's offset: every range's rows before it. A range without
		// candidates has its bounds on either side of every candidate.
		g := 0
		for r := range p.spans {
			switch {
			case r == m.r:
				below[r] = lo[r] + (hi[r]-lo[r])/2
			case lo[r] == hi[r]:
				below[r] = lo[r]
			default:
				below[r] = p.below(r, m.k)
			}
			g += int(below[r])
		}
		switch {
		case g == offset:
			c.pos = clonePos(below)
			return c
		case g < offset: // the row sought comes after M
			for r := range p.spans {
				b := below[r]
				if r == m.r {
					b++
				}
				lo[r] = max(lo[r], b)
			}
		default: // before M
			for r := range p.spans {
				hi[r] = min(hi[r], below[r])
			}
		}
	}
}

// of is the cursor at the place key k has in the list (whether or not a row
// of the list has it): O(R log n).
func (p placed[K]) of(k K) Cursor {
	c := Cursor{pos: make([]int32, len(p.spans))}
	for r := range p.spans {
		c.pos[r] = p.below(r, k)
		c.Offset += int(c.pos[r])
	}
	return c
}

// rowSource is a plan of any key type.
type rowSource interface {
	Total() int
	ranges() int
	read(c Cursor, limit int) ([]*EpisodeItem, Cursor)
	back(c Cursor, n int) Cursor
	at(offset int) Cursor
	ofEpisode(e *EpisodeItem) Cursor
	window(offset int, c *Cursor, limit int) ([]*EpisodeItem, Cursor)
	anchor(e *EpisodeItem, before int) Cursor
	places(e *EpisodeItem) bool
}

// window reads limit rows at offset, from the cursor when it is at that
// offset (else selecting it), placing the plan once.
func (p *plan[K]) window(offset int, c *Cursor, limit int) ([]*EpisodeItem, Cursor) {
	pl := p.place()
	if c != nil && c.Offset == offset && len(c.pos) == len(p.spans) {
		return pl.read(*c, limit)
	}
	return pl.read(pl.at(offset), limit)
}

// anchor is the cursor before rows ahead of the episode's place.
func (p *plan[K]) anchor(e *EpisodeItem, before int) Cursor {
	pl := p.place()
	return pl.back(pl.of(p.keyOf(e)), before)
}

func (p *plan[K]) Total() int                                        { return p.total }
func (p *plan[K]) ranges() int                                       { return len(p.spans) }
func (p *plan[K]) read(c Cursor, limit int) ([]*EpisodeItem, Cursor) { return p.place().read(c, limit) }
func (p *plan[K]) back(c Cursor, n int) Cursor                       { return p.place().back(c, n) }
func (p *plan[K]) at(offset int) Cursor                              { return p.place().at(offset) }
func (p *plan[K]) ofEpisode(e *EpisodeItem) Cursor                   { return p.place().of(p.keyOf(e)) }
func (p *plan[K]) places(e *EpisodeItem) bool                        { return p.has == nil || p.has(e) }

// ListQuery is an episode list: a filter, a sort order (OrderNames) and,
// for a podcast's list, the podcast.
type ListQuery struct {
	Filter  Filter
	Order   string
	Podcast string
}

// EpisodeList is a list of a version's episodes, read at any offset.
type EpisodeList struct {
	v   *Version
	src rowSource
}

// Total is the list's length.
func (l *EpisodeList) Total() int { return l.src.Total() }

// Ranges is the number of ranges the list merges.
func (l *EpisodeList) Ranges() int { return l.src.ranges() }

// Version is the version the list reads.
func (l *EpisodeList) Version() *Version { return l.v }

// Read returns up to limit rows at the cursor and the cursor after them.
func (l *EpisodeList) Read(c Cursor, limit int) ([]*EpisodeItem, Cursor) { return l.src.read(c, limit) }

// At is the cursor at an offset.
func (l *EpisodeList) At(offset int) Cursor { return l.src.at(offset) }

// Anchor is the cursor before rows ahead of the place of episode id in the
// list (whether or not the list has it), if the version stores it and it
// has a place in the list's order (every stored episode has one in a sort
// order; in the queue only a queued one); else (found false) the cursor
// that many rows ahead of index, clamped.
func (l *EpisodeList) Anchor(id string, index, before int) (c Cursor, found bool) {
	if e, ok := l.v.stored.Get(id); ok && id != "" && l.src.places(e) {
		return l.src.anchor(e, before), true
	}
	return l.src.at(min(max(index, 0), l.src.Total()) - before), false
}

// orderCmp is a sort order's order of rows across ranges: its column in its
// direction, then rank, then ID (Sort's stable sort over Rank order).
func orderCmp(desc bool) func(a, b orderKey) int {
	return func(a, b orderKey) int {
		c := compareColumns(a.col, b.col)
		if desc {
			c = -c
		}
		switch {
		case c != 0:
			return c
		case a.rank != b.rank:
			if a.rank < b.rank {
				return -1
			}
			return 1
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		}
		return 0
	}
}

// Episodes is an episode list of shown podcasts: the merge, in the sort
// order, of each shown podcast's ranges of the classes the filter passes
// (the podcast's own when the query names one). Its length comes from the
// podcasts' class counts: O(P × classes) to plan.
func (l *Library) Episodes(q ListQuery) *EpisodeList {
	v := l.V
	i, ok := OrderIndex(q.Order)
	if !ok {
		i, _ = OrderIndex("date_new_old")
	}
	d := orderDefs[i]
	p := &plan[orderKey]{t: v.orders[i], cmp: orderCmp(d.desc), keyOf: d.key,
		lo: func(s span) orderKey { return orderKey{podcast: s.podcast, class: s.class, bound: -1} },
		rekey: func(k orderKey, s span) orderKey {
			return orderKey{podcast: s.podcast, class: s.class, col: k.col, rank: k.rank, id: k.id}
		},
	}
	add := func(pod *PodcastItem) {
		if !q.Filter.StateOK(pod.State) {
			return
		}
		agg, ok := v.agg.Get(pod.ID)
		if !ok {
			return
		}
		for c := range Class(NumClasses) {
			if n := agg.Hist[c]; n > 0 && q.Filter.MatchClass(c) {
				p.add(span{pod.ID, c, n})
			}
		}
	}
	if q.Podcast != "" {
		if pod, ok := v.podcasts.Get(q.Podcast); ok {
			add(pod)
		}
	} else {
		for _, pod := range v.podcasts.All() {
			add(pod)
		}
	}
	return &EpisodeList{v: v, src: p.done()}
}

// Queue is the queue of shown podcasts: the merge, in queue order, of each
// shown podcast's queued episodes.
func (l *Library) Queue() *EpisodeList {
	v := l.V
	p := &plan[queueKey]{t: v.queue, cmp: compareQueueRows, keyOf: queueOrder, has: func(e *EpisodeItem) bool { return e.Queued },
		lo:    func(s span) queueKey { return queueKey{podcast: s.podcast, bound: -1} },
		rekey: func(k queueKey, s span) queueKey { return queueKey{podcast: s.podcast, key: k.key, id: k.id} }}
	for id := range v.podcasts.All() {
		if agg, ok := v.agg.Get(id); ok {
			p.add(span{podcast: id, n: int32(queued(agg))})
		}
	}
	return &EpisodeList{v: v, src: p.done()}
}

// queued is a podcast's queued episodes.
func queued(a *PodAgg) int {
	n := 0
	for c := range Class(NumClasses) {
		if c.Queued() {
			n += int(a.Hist[c])
		}
	}
	return n
}
