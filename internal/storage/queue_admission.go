package storage

import (
	"maps"
	"slices"
	"sync/atomic"
)

// AddQueueNode records a queue membership/position node: frontier nodes that
// are its ancestors leave the frontier, and it joins, unless it is on the
// frontier already or a frontier node descends from it (one recorded first,
// as a decision resolving this node's effect is).
//
// The relationships are found by walking the causal graph from the node. A
// walk ends at a frontier node it reaches (frontier nodes are never each
// other's ancestors) and reads a node's edges a page at a time. A node whose
// whole past (or future) is small, as an imported episode's is, is resolved
// by that walk alone. Otherwise the frontier is read and three searches take
// turns: the node's walk, a walk from each unresolved frontier node toward
// the node (a frontier node is related when the walks meet, unrelated when
// either ends), and a sweep of the edges ranked between them in rank order,
// as the earlier admission read them all, which resolves each frontier node
// it passes. The sweep advances by queueSweepRatio edges for each query of
// the walks, so the work is about that of the history relating them when
// that is short, and never much more than one read of the ranks between
// them when it is not.
func (a *AccountTx) AddQueueNode(node string) error {
	rank, err := a.NodeRank(node)
	if err != nil {
		return err
	}
	var on int
	if err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM queue_frontier WHERE account_id = ? AND node_id = ?`, a.id, node).Scan(&on); err != nil {
		return err
	}
	if on > 0 {
		return nil
	}
	q := &queueRelation{a: a, node: node, rank: rank}
	later, err := q.find(true)
	if err != nil || len(later) > 0 {
		return err
	}
	past, err := q.find(false)
	if err != nil {
		return err
	}
	for _, f := range slices.Sorted(maps.Keys(past)) {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM queue_frontier WHERE account_id = ? AND node_id = ?`, a.id, f); err != nil {
			return err
		}
	}
	_, err = a.w.tx.ExecContext(a.w.ctx, `INSERT INTO queue_frontier (account_id, node_id) VALUES (?, ?)`, a.id, node)
	return err
}

const (
	// queueProbeRows is how many rows the walk from an admitted node reads
	// before it reads the frontier.
	queueProbeRows = 16
	// queuePageRows is the most edges a walk reads of one node at a time.
	queuePageRows = 256
	// queueSweepRatio is how many edges the sweep may read for each walk
	// query: a walk's point query costs about as much as reading that many
	// edges in rank order.
	queueSweepRatio = 24
	// queueSweepPage is the fewest edges the sweep reads at a time.
	queueSweepPage = 256
)

// queueAdmissionRows counts the rows queue admission has read since the
// process started (causal edges and frontier nodes), and
// queueAdmissionQueries the walks' queries (tests bound them).
var queueAdmissionRows, queueAdmissionQueries atomic.Int64

// QueueAdmissionRowsRead is the number of rows queue admission has read so far.
func QueueAdmissionRowsRead() int64 { return queueAdmissionRows.Load() }

// QueueAdmissionQueries is the number of walk queries queue admission has made so far.
func QueueAdmissionQueries() int64 { return queueAdmissionQueries.Load() }

// queueRelation finds an admitted node's relationships to the queue frontier,
// which it reads at most once.
type queueRelation struct {
	a        *AccountTx
	node     string
	rank     int64
	frontier []rankedNode
	read     bool
}

type rankedNode struct {
	id   string
	rank int64
}

// walkNode is a node a walk reached, with how far it has read its edges.
type walkNode struct {
	rankedNode
	afterID  string // dependencies: the last dependency read
	afterRow int64  // dependents: the last edge read (its rowid)
}

// memberWalk is a frontier node's walk toward the admitted node.
type memberWalk struct {
	rankedNode
	seen     map[string]bool
	pending  []walkNode
	resolved bool
}

// relationSearch is one direction's search: the frontier nodes the admitted
// node descends from (later false), or whether one descends from it.
type relationSearch struct {
	q       *queueRelation
	later   bool
	found   map[string]bool
	seen    map[string]bool // the nodes the admitted node's walk reached, the node included
	known   map[string]bool // the admitted node's ancestors (or descendants) found so far, by the walk or the sweep
	pending []walkNode
	open    []*memberWalk // ascending rank; resolved ones are dropped as the turns come round
	byID    map[string]*memberWalk
	owners  map[string][]*memberWalk       // the frontier walks that reached a node
	lists   map[edgeList][]causalNeighbour // nodes' whole edge lists read so far (walks converging on one node read it once)
	probing bool                           // the frontier is not read yet

	probeCost, cost, memberCost int // rows read: the probe's, and the walks' that resolved nothing
	turn, lo, hi                int // the next frontier walk; the lowest and highest unresolved ones, found lazily
	resolved                    int // frontier nodes resolved so far

	allowance  int             // the edges the sweep may read next
	sweepTop   int64           // the sweep has read every edge from a node ranked above it,
	sweepAfter string          // and those from the node ranked at it up to this dependency
	reach      map[string]bool // later: nodes in the past of a frontier node, as the sweep found them
	swept      bool            // the sweep reached its end: every frontier node is resolved
}

func (q *queueRelation) find(later bool) (map[string]bool, error) {
	return q.search(later).run()
}

func (q *queueRelation) search(later bool) *relationSearch {
	return &relationSearch{q: q, later: later, found: map[string]bool{}, seen: map[string]bool{q.node: true}, known: map[string]bool{q.node: true},
		pending: []walkNode{{rankedNode: rankedNode{q.node, q.rank}}}, byID: map[string]*memberWalk{},
		owners: map[string][]*memberWalk{}, lists: map[edgeList][]causalNeighbour{}, probing: true, hi: -1}
}

func (s *relationSearch) run() (map[string]bool, error) {
	for {
		if s.later && len(s.found) > 0 {
			return s.found, nil
		}
		if s.probing {
			if len(s.pending) == 0 {
				return s.found, nil // the node's whole past (or future) is walked
			}
			if s.probeCost < queueProbeRows {
				if err := s.step(queueProbeRows - s.probeCost); err != nil {
					return nil, err
				}
				continue
			}
			if err := s.begin(); err != nil {
				return nil, err
			}
		}
		if s.turn >= len(s.open) {
			// A round is over: drop the resolved frontier nodes.
			s.open = slices.DeleteFunc(s.open, func(m *memberWalk) bool { return m.resolved })
			s.turn, s.lo, s.hi = 0, 0, len(s.open)-1
		}
		if len(s.open) == 0 || s.swept {
			return s.found, nil
		}
		if len(s.pending) == 0 {
			return s.found, nil // the node's walk is over: the rest are unrelated
		}
		var err error
		switch {
		case s.allowance >= queueSweepPage:
			err = s.sweep()
		case s.cost < s.memberCost:
			err = s.step(queuePageRows)
		default:
			err = s.memberStep()
		}
		if err != nil {
			return nil, err
		}
	}
}

// begin reads the frontier and starts a walk from each frontier node on the
// search's side of the admitted node that the probe did not reach.
func (s *relationSearch) begin() error {
	q := s.q
	if !q.read {
		if err := q.readFrontier(); err != nil {
			return err
		}
	}
	s.probing = false
	for _, f := range q.frontier {
		if (f.rank > q.rank) == s.later && !s.found[f.id] {
			m := &memberWalk{rankedNode: f, seen: map[string]bool{f.id: true}, pending: []walkNode{{rankedNode: f}}}
			s.open = append(s.open, m)
			s.byID[f.id] = m
		}
	}
	s.hi = len(s.open) - 1
	if s.later {
		if len(s.open) > 0 {
			s.sweepTop = s.open[len(s.open)-1].rank
		}
		s.reach = map[string]bool{}
		for _, m := range s.open {
			s.reach[m.id] = true
		}
	} else {
		s.sweepTop = q.rank
	}
	return nil
}

func (s *relationSearch) resolve(m *memberWalk, related bool) {
	if !m.resolved {
		m.resolved = true
		s.resolved++
		if related {
			s.found[m.id] = true
		}
	}
}

// bound is the lowest (or, later, the highest) rank of an unresolved
// frontier node; false when every one is resolved.
func (s *relationSearch) bound() (int64, bool) {
	if s.later {
		for s.hi >= 0 && s.open[s.hi].resolved {
			s.hi--
		}
		if s.hi < 0 {
			return 0, false
		}
		return s.open[s.hi].rank, true
	}
	for s.lo < len(s.open) && s.open[s.lo].resolved {
		s.lo++
	}
	if s.lo >= len(s.open) {
		return 0, false
	}
	return s.open[s.lo].rank, true
}

// within reports whether a node on the admitted node's walk can still lead
// to an unresolved frontier node (one ranked beyond it cannot).
func (s *relationSearch) within(r int64) bool {
	if s.probing {
		return true
	}
	b, ok := s.bound()
	if !ok {
		return false
	}
	if s.later {
		return r <= b
	}
	return r >= b
}

// spent accounts for a walk query: each lets the sweep read further, so
// the walks and the sweep race, and a query that resolved nothing lets its
// walk's side go on. The frontier walks take their turns first, and the
// admitted node's walk goes on only as far as their unproductive rows.
func (s *relationSearch) spent(rows int, member bool, resolvedBefore int) {
	if s.probing {
		s.probeCost += max(rows, 1)
		return
	}
	s.allowance += queueSweepRatio + rows
	if s.resolved > resolvedBefore || (s.later && len(s.found) > 0) {
		return
	}
	if member {
		s.memberCost += rows
	} else {
		s.cost += rows
	}
}

// step reads a page of the next node's edges on the admitted node's walk
// (all of the admitted node's own).
func (s *relationSearch) step(limit int) error {
	x := s.pending[len(s.pending)-1]
	s.pending = s.pending[:len(s.pending)-1]
	if !s.within(x.rank) {
		return nil
	}
	if x.id == s.q.node {
		limit = -1 // the node's own edges are the command's: read them whole
	}
	before := s.resolved
	next, err := s.neighbours(x, s.later, limit)
	if err != nil {
		return err
	}
	if len(next) == limit {
		last := next[len(next)-1]
		x.afterID, x.afterRow = last.id, last.row
		s.pending = append(s.pending, x)
	}
	for _, y := range next {
		if s.seen[y.id] {
			continue
		}
		s.seen[y.id], s.known[y.id] = true, true
		if y.onFrontier {
			// A frontier node ends the walk: no other one is its ancestor
			// (or descendant).
			s.found[y.id] = true
			if m := s.byID[y.id]; m != nil {
				s.resolve(m, true)
			}
			continue
		}
		for _, m := range s.owners[y.id] {
			s.resolve(m, true)
		}
		s.pending = append(s.pending, walkNode{rankedNode: y.rankedNode})
	}
	s.spent(len(next), false, before)
	return nil
}

// memberStep reads a page of the next node's edges on the next unresolved
// frontier node's walk.
func (s *relationSearch) memberStep() error {
	m := s.open[s.turn]
	s.turn++
	if m.resolved {
		return nil
	}
	q := s.q
	x := m.pending[len(m.pending)-1]
	m.pending = m.pending[:len(m.pending)-1]
	before := s.resolved
	next, err := s.neighbours(x, !s.later, queuePageRows)
	if err != nil {
		return err
	}
	if len(next) == queuePageRows {
		last := next[len(next)-1]
		x.afterID, x.afterRow = last.id, last.row
		m.pending = append(m.pending, x)
	}
	for _, y := range next {
		if s.known[y.id] {
			s.resolve(m, true)
			break
		}
		if m.seen[y.id] || (s.later && y.rank <= q.rank) || (!s.later && y.rank >= q.rank) {
			continue
		}
		m.seen[y.id] = true
		s.owners[y.id] = append(s.owners[y.id], m)
		m.pending = append(m.pending, walkNode{rankedNode: y.rankedNode})
	}
	if len(m.pending) == 0 {
		s.resolve(m, false) // its walk ended short of the node
	}
	s.spent(len(next), true, before)
	return nil
}

// sweep reads the next edges in descending rank order of their nodes,
// between the admitted node and the farthest unresolved frontier node, as
// the earlier admission read them: the admitted node's ancestors are marked
// from the top down (or, later, the past of the frontier nodes), so each
// frontier node the sweep has passed is resolved exactly.
func (s *relationSearch) sweep() error {
	q := s.q
	limit := s.allowance
	s.allowance = 0
	bottom := q.rank + 1 // later: the lowest rank the sweep reads
	if !s.later {
		b, ok := s.bound()
		if !ok {
			return nil
		}
		bottom = b
	}
	if s.sweepTop < bottom {
		s.swept = true
		return nil
	}
	edges, err := q.a.edgesRanked(bottom, s.sweepTop, s.sweepAfter, limit)
	if err != nil {
		return err
	}
	end := len(edges) < limit
	if !end {
		last := edges[len(edges)-1]
		s.sweepTop, s.sweepAfter = last.rank, last.to
	}
	for _, e := range edges {
		if s.later {
			if !s.reach[e.from] && len(s.owners[e.from]) == 0 {
				continue
			}
			if s.known[e.to] || s.known[e.from] {
				s.found[e.to] = true // a frontier node's past holds the node
				return nil
			}
			s.reach[e.to] = true
			continue
		}
		if !s.known[e.from] || s.known[e.to] {
			continue
		}
		s.known[e.to] = true
		if m := s.byID[e.to]; m != nil {
			s.resolve(m, true)
		}
		for _, m := range s.owners[e.to] {
			s.resolve(m, true)
		}
	}
	s.swept = end
	if s.later {
		return nil // at its end, the sweep reached the node: nothing above holds it
	}
	// Every frontier node ranked at or above the last node read is resolved
	// now: each of the node's ancestors ranked above it is marked (its own
	// edges do not matter).
	for _, m := range s.open {
		if !m.resolved && (end || m.rank >= s.sweepTop) {
			s.resolve(m, s.known[m.id])
		}
	}
	return nil
}

type edgeList struct {
	node       string
	dependents bool
}

// neighbours returns a page of a node's edges, from the lists the search has
// read whole when it has.
func (s *relationSearch) neighbours(x walkNode, dependents bool, limit int) ([]causalNeighbour, error) {
	first := x.afterID == "" && x.afterRow == 0
	key := edgeList{x.id, dependents}
	if list, ok := s.lists[key]; ok && first {
		return list, nil
	}
	next, err := s.q.a.causalNeighbours(x, dependents, limit)
	if err == nil && first && len(next) != limit {
		s.lists[key] = next
	}
	return next, err
}

// readFrontier reads the queue frontier with its ranks, ascending.
func (q *queueRelation) readFrontier() error {
	rows, err := q.a.w.tx.QueryContext(q.a.w.ctx, `SELECT f.node_id, o.rank FROM queue_frontier f
		CROSS JOIN node_order o ON o.account_id = f.account_id AND o.node_id = f.node_id WHERE f.account_id = ? ORDER BY o.rank`, q.a.id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var f rankedNode
		if err := rows.Scan(&f.id, &f.rank); err != nil {
			return err
		}
		q.frontier = append(q.frontier, f)
	}
	queueAdmissionRows.Add(int64(len(q.frontier)))
	q.read = true
	return rows.Err()
}

type causalNeighbour struct {
	rankedNode
	onFrontier bool
	row        int64 // a dependent's edge rowid
}

// causalNeighbours returns up to limit of a node's dependencies (or
// dependents) after those already read, with their ranks and whether they
// are on the queue frontier: one index seek.
func (a *AccountTx) causalNeighbours(x walkNode, dependents bool, limit int) ([]causalNeighbour, error) {
	query, after := causalDependenciesQuery, any(x.afterID)
	if dependents {
		query, after = causalDependentsQuery, x.afterRow
	}
	rows, err := a.w.tx.QueryContext(a.w.ctx, query, a.id, x.id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []causalNeighbour
	for rows.Next() {
		var n causalNeighbour
		if err := rows.Scan(&n.id, &n.rank, &n.onFrontier, &n.row); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	queueAdmissionQueries.Add(1)
	queueAdmissionRows.Add(int64(len(out)))
	return out, rows.Err()
}

type rankedEdge struct {
	rank     int64
	from, to string
}

// edgesRanked returns up to limit edges of the nodes ranked from top down to
// bottom (both included), by descending rank of their node, then dependency,
// after the edge (top, after): one range seek.
func (a *AccountTx) edgesRanked(bottom, top int64, after string, limit int) ([]rankedEdge, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, rankedEdgesQuery, a.id, bottom, top, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []rankedEdge
	for rows.Next() {
		var e rankedEdge
		if err := rows.Scan(&e.rank, &e.from, &e.to); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	queueAdmissionRows.Add(int64(len(out)))
	return out, rows.Err()
}

const (
	causalDependenciesQuery = `SELECT e.dependency_id, o.rank, f.node_id IS NOT NULL, 0 FROM causal_edges e
		CROSS JOIN node_order o ON o.account_id = e.account_id AND o.node_id = e.dependency_id
		LEFT JOIN queue_frontier f ON f.account_id = e.account_id AND f.node_id = e.dependency_id
		WHERE e.account_id = ? AND e.node_id = ? AND e.dependency_id > ? ORDER BY e.dependency_id LIMIT ?`
	causalDependentsQuery = `SELECT e.node_id, o.rank, f.node_id IS NOT NULL, e.rowid FROM causal_edges e INDEXED BY causal_edges_dependency
		CROSS JOIN node_order o ON o.account_id = e.account_id AND o.node_id = e.node_id
		LEFT JOIN queue_frontier f ON f.account_id = e.account_id AND f.node_id = e.node_id
		WHERE e.account_id = ? AND e.dependency_id = ? AND e.rowid > ? ORDER BY e.rowid LIMIT ?`
	rankedEdgesQuery = `SELECT o.rank, e.node_id, e.dependency_id FROM node_order o INDEXED BY node_order_rank
		CROSS JOIN causal_edges e ON e.account_id = o.account_id AND e.node_id = o.node_id
		WHERE o.account_id = ?1 AND o.rank >= ?2 AND o.rank <= ?3 AND (o.rank < ?3 OR e.dependency_id > ?4)
		ORDER BY o.rank DESC, e.dependency_id LIMIT ?5`
)
