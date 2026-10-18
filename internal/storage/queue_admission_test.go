package storage

import (
	"context"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Queue admission (PROTOCOL §7.6): the queue frontier is the admitted queue
// nodes no other admitted queue node descends from. AddQueueNode finds the
// node's relationships to it by walking the causal graph; these tests compare
// it with the admission it replaced, which read every edge ranked between
// the frontier and the node, and bound its work.

// oracleAddQueueNode is the admission as it was before it walked the graph,
// kept as the oracle: the frontier after admitting node to frontier.
func oracleAddQueueNode(a *AccountTx, frontier []string, node string) ([]string, error) {
	rank, err := a.NodeRank(node)
	if err != nil {
		return nil, err
	}
	minRank := int64(-1)
	for _, f := range frontier {
		if f == node {
			return frontier, nil
		}
		r, err := a.NodeRank(f)
		if err != nil {
			return nil, err
		}
		if minRank < 0 || r < minRank {
			minRank = r
		}
		if r > rank {
			past, err := a.Ancestors(f, rank)
			if err != nil {
				return nil, err
			}
			if past[node] {
				return frontier, nil
			}
		}
	}
	var out []string
	if minRank >= 0 {
		past, err := a.Ancestors(node, minRank)
		if err != nil {
			return nil, err
		}
		for _, f := range frontier {
			if !past[f] {
				out = append(out, f)
			}
		}
	}
	out = append(out, node)
	slices.Sort(out)
	return out, nil
}

// causalGraph builds a valid causal graph in one account.
type causalGraph struct {
	t     *testing.T
	a     *AccountTx
	n     int
	clock int64
	last  int64 // the highest rank a bulk chain used
}

func (g *causalGraph) node(at int64, deps ...string) string {
	g.t.Helper()
	g.n++
	id := fmt.Sprintf("n%07d", g.n)
	slices.Sort(deps)
	deps = slices.Compact(deps)
	if err := g.a.InsertNode(id, "decision", at, deps); err != nil {
		g.t.Fatal(err)
	}
	return id
}

// chain appends h nodes after every node, each depending on the one before
// (the first on from), written directly: unrelated history of any length,
// placed as the causal order places a new node after the last.
func (g *causalGraph) chain(from string, h int) string {
	g.t.Helper()
	ctx, tx := g.a.w.ctx, g.a.w.tx
	var top int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rank), 0) FROM node_order WHERE account_id = ?`, g.a.id).Scan(&top); err != nil {
		g.t.Fatal(err)
	}
	nodes, err := tx.PrepareContext(ctx, `INSERT INTO causal_nodes (account_id, node_id, kind, normalized_time) VALUES (?, ?, 'decision', ?)`)
	if err != nil {
		g.t.Fatal(err)
	}
	defer nodes.Close()
	edges, err := tx.PrepareContext(ctx, `INSERT INTO causal_edges (account_id, node_id, dependency_id) VALUES (?, ?, ?)`)
	if err != nil {
		g.t.Fatal(err)
	}
	defer edges.Close()
	order, err := tx.PrepareContext(ctx, `INSERT INTO node_order (account_id, node_id, rank) VALUES (?, ?, ?)`)
	if err != nil {
		g.t.Fatal(err)
	}
	defer order.Close()
	prev := from
	for range h {
		g.n++
		id := fmt.Sprintf("n%07d", g.n)
		top += rankGap
		g.clock++
		if _, err := nodes.ExecContext(ctx, g.a.id, id, g.clock); err != nil {
			g.t.Fatal(err)
		}
		if _, err := edges.ExecContext(ctx, g.a.id, id, prev); err != nil {
			g.t.Fatal(err)
		}
		if _, err := order.ExecContext(ctx, g.a.id, id, top); err != nil {
			g.t.Fatal(err)
		}
		prev = id
	}
	return prev
}

func (g *causalGraph) now() int64 {
	g.clock++
	return g.clock
}

func (g *causalGraph) admit(node string) {
	g.t.Helper()
	if err := g.a.AddQueueNode(node); err != nil {
		g.t.Fatal(err)
	}
}

func (g *causalGraph) frontier() []string {
	g.t.Helper()
	f, err := g.a.QueueFrontier()
	if err != nil {
		g.t.Fatal(err)
	}
	return f
}

// withGraph runs fn on a new account's causal graph in one write transaction.
func withGraph(t *testing.T, fn func(g *causalGraph)) {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(ctx, func(tx *WriteTx) error {
		acct, err := tx.CreateAccount()
		if err != nil {
			return err
		}
		fn(&causalGraph{t: t, a: tx.Account(acct.ID), clock: 1_000_000})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// After every admission of long random sequences, the frontier is the one
// the earlier admission computes: devices on concurrent branches (some
// offline for long stretches), nodes placed before later ones by an older
// time, admissions of nodes recorded earlier (with dependents already, as a
// node whose resolving decision is admitted first), long unrelated gaps,
// imports' concurrent queues with their manifest, layouts (which depend on
// the frontier), long histories descending from a frontier node, wide facts
// followed by small commands, and old nodes admitted after a long future.
// Each admission also stays within a bound of the range scan's work: its
// walks make at most about one query for 12 nodes ranked between the node
// and the frontier. NTP_QUEUE_ADMISSION_LONG=1 runs 12 seeds of 1,500 steps.
func TestQueueAdmissionMatchesTheRangeScan(t *testing.T) {
	seeds, steps := uint64(4), 800
	if os.Getenv("NTP_QUEUE_ADMISSION_LONG") != "" {
		seeds, steps = 12, 1500
	} else if raceEnabled {
		seeds, steps = 1, 120 // the race detector's size; make check runs the full one without it
	}
	for seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			withGraph(t, func(g *causalGraph) {
				r := mrand.New(mrand.NewPCG(seed, 76))
				var all []string
				want := []string{}
				devices := make([]string, 4)
				offline := make([]int, len(devices)) // steps a device stays without pulling
				history := g.node(g.now())
				all = append(all, history)
				admit := func(node string) {
					t.Helper()
					next, err := oracleAddQueueNode(g.a, want, node)
					if err != nil {
						t.Fatal(err)
					}
					nodes, edges := g.span(node, want)
					frontier := len(want)
					want = next
					w := measureAdmission(func() { g.admit(node) })
					if got := g.frontier(); !slices.Equal(got, want) {
						t.Fatalf("after %s: frontier %v, the range scan's %v", node, got, want)
					}
					// Never much costlier than one read of the ranks between the node
					// and the frontier, which the range scan read.
					if limit := int64(2*(queueProbeRows+1+frontier)) + nodes/12 + 64; w.queries > limit {
						t.Fatalf("admitting %s: %d walk queries, more than %d (%d nodes, %d edges between it and the frontier)", node, w.queries, limit,
							nodes, edges)
					}
					if limit := 2*(nodes+edges) + int64(512*frontier) + 2048; w.rows > limit {
						t.Fatalf("admitting %s: %d rows, more than %d (%d nodes, %d edges between it and the frontier)", node, w.rows, limit, nodes, edges)
					}
				}
				recent := func(k int) []string {
					var out []string
					for range k {
						lo := max(0, len(all)-40)
						out = append(out, all[lo+r.IntN(len(all)-lo)])
					}
					return out
				}
				// at is a node's time: usually now, sometimes old, so the
				// causal order places it before nodes recorded earlier.
				at := func() int64 {
					if r.IntN(6) == 0 {
						return g.clock - int64(r.IntN(500))
					}
					return g.now()
				}
				for step := range steps {
					switch k := r.IntN(100); {
					case k < 45: // a device's command
						d := r.IntN(len(devices))
						deps := []string{}
						if devices[d] != "" {
							deps = append(deps, devices[d])
						}
						if offline[d] > 0 {
							offline[d]--
						} else {
							deps = append(deps, recent(1+r.IntN(3))...)
							if r.IntN(10) == 0 {
								offline[d] = 5 + r.IntN(60)
							}
						}
						n := g.node(at(), deps...)
						devices[d] = n
						all = append(all, n)
						if r.IntN(4) > 0 {
							admit(n)
						}
					case k < 65: // other history; sometimes a long unrelated chain
						if r.IntN(15) == 0 {
							end := g.chain(all[r.IntN(len(all))], 50+r.IntN(400))
							all = append(all, end)
						} else {
							all = append(all, g.node(at(), recent(1+r.IntN(2))...))
						}
					case k < 75: // a node recorded earlier, admitted now
						admit(all[r.IntN(len(all))])
					case k < 85: // a node whose resolving decision is admitted first
						n := g.node(at(), recent(1+r.IntN(2))...)
						d := g.node(g.now(), n)
						all = append(all, n, d)
						admit(d)
						admit(n)
					case k < 87: // a layout: it depends on the queue frontier
						l := g.node(g.now(), append(slices.Clone(want), recent(1)...)...)
						all = append(all, l)
						admit(l)
					case k < 90: // an import: concurrent queued episodes, then its manifest
						var queued []string
						pods := 1 + r.IntN(4)
						roots := make([]string, pods)
						for i := range roots {
							roots[i] = g.node(g.now())
						}
						for range 5 + r.IntN(80) {
							e := g.node(g.now(), roots[r.IntN(pods)])
							queued = append(queued, e)
							admit(e)
						}
						m := g.node(g.now(), queued...)
						all = append(all, m)
						if r.IntN(2) == 0 {
							admit(g.node(g.now(), m, all[len(all)-2]))
						}
					case k < 93: // a device's long history after its last queue command (listening), then a command
						d := r.IntN(len(devices))
						if devices[d] == "" {
							devices[d] = g.node(g.now(), recent(1)...)
							admit(devices[d])
						}
						end := g.chain(devices[d], 200+r.IntN(1500))
						deps := []string{end}
						if r.IntN(2) == 0 {
							deps = append(deps, recent(1)...)
						}
						n := g.node(g.now(), deps...)
						devices[d] = n
						all = append(all, end, n)
						admit(n)
					case k < 95: // a wide fact on the frontier, then small commands after it
						w := g.node(g.now(), all[max(0, len(all)-300):]...)
						admit(w)
						prev := w
						for range 1 + r.IntN(4) {
							prev = g.node(g.now(), prev)
							admit(prev)
						}
						all = append(all, w, prev)
					case k < 97: // an old node with a long future, admitted after it
						x := all[r.IntN(len(all))]
						c := g.node(g.now(), g.chain(x, 200+r.IntN(800)))
						admit(c)
						admit(x)
						admit(all[r.IntN(len(all))])
						all = append(all, c)
					default: // a device that pulled everything since
						n := g.node(g.now(), recent(5)...)
						all = append(all, n)
						admit(n)
					}
					if step%250 == 249 {
						t.Logf("step %d: %d nodes, frontier %d", step, g.n, len(want))
					}
				}
			})
		})
	}
}

// span counts the nodes and edges ranked between a node and a frontier
// (both included): what the range scan could read admitting it.
func (g *causalGraph) span(node string, frontier []string) (nodes, edges int64) {
	g.t.Helper()
	lo, err := g.a.NodeRank(node)
	if err != nil {
		g.t.Fatal(err)
	}
	hi := lo
	for _, f := range frontier {
		r, err := g.a.NodeRank(f)
		if err != nil {
			g.t.Fatal(err)
		}
		lo, hi = min(lo, r), max(hi, r)
	}
	if err := g.a.w.tx.QueryRowContext(g.a.w.ctx, `SELECT COUNT(*), COALESCE(SUM((SELECT COUNT(*) FROM causal_edges e WHERE e.account_id = o.account_id
		AND e.node_id = o.node_id)), 0) FROM node_order o WHERE o.account_id = ? AND o.rank BETWEEN ? AND ?`, g.a.id, lo, hi).Scan(&nodes, &edges); err != nil {
		g.t.Fatal(err)
	}
	return nodes, edges
}

type admissionWork struct{ rows, queries int64 }

// measureAdmission returns the rows and walk queries queue admission made
// while fn ran.
func measureAdmission(fn func()) admissionWork {
	r, q := QueueAdmissionRowsRead(), QueueAdmissionQueries()
	fn()
	return admissionWork{QueueAdmissionRowsRead() - r, QueueAdmissionQueries() - q}
}

// raceSize is a test's size: full, or smaller under the race detector (make
// check runs the full size without it, `make check`'s nonrace storage line).
func raceSize(full, race int) int {
	if raceEnabled {
		return race
	}
	return full
}

// admissionRows returns the rows queue admission read while fn ran.
func admissionRows(fn func()) int64 {
	before := QueueAdmissionRowsRead()
	fn()
	return QueueAdmissionRowsRead() - before
}

// An import's queued episodes are concurrent: each depends only on its
// podcast. Admitting 5,000 of them reads a few rows each, never the growing
// frontier (the earlier admission read 5,000 × 4,999 / 2 frontier rows).
func TestImportQueueAdmissionIsLinear(t *testing.T) {
	q := raceSize(5000, 1000)
	withGraph(t, func(g *causalGraph) {
		pods := make([]string, 70)
		for i := range pods {
			pods[i] = g.node(g.now())
		}
		rows := admissionRows(func() {
			for i := range q {
				g.admit(g.node(g.now(), pods[i*len(pods)/q]))
			}
		})
		if f := g.frontier(); len(f) != q {
			t.Fatalf("frontier %d, want %d", len(f), q)
		}
		t.Logf("%d admissions read %d rows", q, rows)
		if rows > int64(2*q) {
			t.Fatalf("%d admissions read %d rows, want at most %d", q, rows, 2*q)
		}
	})
}

// Concurrent queue nodes admitted against the causal order (each ranked
// before every frontier node) read a few rows each: the earlier admission
// read every edge since each later frontier node, about Q³/6 rows.
func TestOutOfRankQueueAdmissionIsLinear(t *testing.T) {
	q := raceSize(400, 100)
	withGraph(t, func(g *causalGraph) {
		root := g.node(g.now())
		nodes := make([]string, q)
		for i := range nodes {
			nodes[i] = g.node(g.now(), root)
		}
		rows := admissionRows(func() {
			for i := q - 1; i >= 0; i-- {
				g.admit(nodes[i])
			}
		})
		if f := g.frontier(); len(f) != q {
			t.Fatalf("frontier %d, want %d", len(f), q)
		}
		t.Logf("%d admissions read %d rows", q, rows)
		if rows > int64(4*q) {
			t.Fatalf("%d admissions read %d rows, want at most %d", q, rows, 4*q)
		}
	})
}

// Routine admission costs what relates the node to the frontier, whatever
// unrelated history lies between them: it is the same after 50,000 or
// 100,000 unrelated nodes (and no more after 1,000), for the first command
// after an import (its 1,000 queued episodes leave the frontier: their walks
// let the sweep read up to 25 edges a query, so the work is bounded by
// them), for an ordinary next command, and for every command of a device
// that has not seen another device's concurrent one (which stays on the
// frontier).
func TestQueueAdmissionIsFlatInUnrelatedHistory(t *testing.T) {
	type scenario struct {
		name  string
		limit int64
		// run builds the graph, with the gap (a chain from a node, or from a
		// new root for ""), and returns the commands to admit after it and
		// the frontier wanted then.
		run func(g *causalGraph, gap func(from string) string) (cmds []func() string, want func([]string) bool)
	}
	members, gaps := 1000, []int{1000, 50_000, 100_000}
	if raceEnabled {
		members, gaps = 100, []int{1000, 4000, 8000} // the race detector's size; make check runs the full one without it
	}
	scenarios := []scenario{
		{name: "the first command after an import", limit: int64(30 * members), run: func(g *causalGraph, gap func(from string) string) ([]func() string, func([]string) bool) {
			pods := make([]string, 70)
			for i := range pods {
				pods[i] = g.node(g.now())
			}
			var queued []string
			for i := range members {
				e := g.node(g.now(), pods[i%len(pods)])
				queued = append(queued, e)
				g.admit(e)
			}
			manifest := g.node(g.now(), queued...)
			end := gap("")
			var n string
			return []func() string{func() string { n = g.node(g.now(), manifest, end); return n }},
				func(f []string) bool { return slices.Equal(f, []string{n}) }
		}},
		{name: "a command after unrelated history", limit: 4 * queueProbeRows, run: func(g *causalGraph, gap func(from string) string) ([]func() string, func([]string) bool) {
			f := g.node(g.now())
			g.admit(f)
			end := gap("")
			var n string
			return []func() string{func() string { n = g.node(g.now(), f, end); return n }},
				func(fr []string) bool { return slices.Equal(fr, []string{n}) }
		}},
		{name: "commands that have not seen a concurrent one", limit: 3 * 4 * queueProbeRows, run: func(g *causalGraph, gap func(from string) string) ([]func() string, func([]string) bool) {
			base := g.node(g.now())
			b := g.node(g.now(), base) // the other device's command, which this device never sees
			g.admit(b)
			a := g.node(g.now(), base)
			g.admit(a)
			end := gap("")
			var cmds []func() string
			for range 3 {
				cmds = append(cmds, func() string { a = g.node(g.now(), a, end); return a })
			}
			return cmds, func(fr []string) bool { return slices.Equal(fr, sorted(a, b)) }
		}},
		{name: "commands that have not seen a concurrent one others observed", limit: 3 * 4 * queueProbeRows, run: func(g *causalGraph, gap func(from string) string) ([]func() string, func([]string) bool) {
			base := g.node(g.now())
			a := g.node(g.now(), base)
			g.admit(a)
			b := g.node(g.now(), base)
			g.admit(b)
			// The gap observes b; this device, offline, sees none of it.
			gap(b)
			var cmds []func() string
			for range 3 {
				cmds = append(cmds, func() string { a = g.node(g.now(), a); return a })
			}
			return cmds, func(fr []string) bool { return slices.Equal(fr, sorted(a, b)) }
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			var counts []int64
			for _, h := range gaps {
				withGraph(t, func(g *causalGraph) {
					var cmds []func() string
					var want func([]string) bool
					cmds, want = sc.run(g, func(from string) string {
						if from == "" {
							from = g.node(g.now())
						}
						return g.chain(from, h)
					})
					var total int64
					for _, cmd := range cmds {
						n := cmd()
						rows := admissionRows(func() { g.admit(n) })
						total += rows
					}
					if f := g.frontier(); !want(f) {
						t.Fatalf("gap %d: frontier %v", h, f)
					}
					t.Logf("gap %d: %d commands read %d rows", h, len(cmds), total)
					if total > sc.limit {
						t.Fatalf("gap %d: %d commands read %d rows, want at most %d", h, len(cmds), total, sc.limit)
					}
					counts = append(counts, total)
				})
			}
			if counts[2] != counts[1] {
				t.Fatalf("rows grew with the gap: %v", counts)
			}
		})
	}
}

// A small command after a wide fact (one with thousands of dependencies,
// as a device's first command after an import is) reads rows bounded by the
// command, not by the wide fact: a walk ends at the frontier node it reaches
// and reads a wide node's edges a page at a time. Five one-dependency
// successors of a wide frontier node, and a command whose only dependency is
// a wide fact that is not a queue node, read the same few rows with 1,000 or
// 10,000 dependencies behind them.
func TestSmallCommandsAfterAWideFact(t *testing.T) {
	type result struct{ successors, through int64 }
	var results []result
	for _, n := range []int{raceSize(1000, 300), raceSize(10_000, 1500)} {
		withGraph(t, func(g *causalGraph) {
			old := make([]string, n)
			for i := range old {
				old[i] = g.node(g.now())
			}
			// Five small commands, each after the last, after a wide one.
			wide := g.node(g.now(), old...)
			g.admit(wide)
			prev := wide
			var successors int64
			for range 5 {
				prev = g.node(g.now(), prev)
				successors += admissionRows(func() { g.admit(prev) })
			}
			if f := g.frontier(); !slices.Equal(f, []string{prev}) {
				t.Fatalf("frontier %v, want %v", f, prev)
			}
			// A command whose dependency is a wide fact that is no queue
			// node, which depends on the frontier node.
			fact := g.node(g.now(), append(slices.Clone(old), prev)...)
			cmd := g.node(g.now(), fact)
			through := admissionRows(func() { g.admit(cmd) })
			if f := g.frontier(); !slices.Equal(f, []string{cmd}) {
				t.Fatalf("frontier %v, want %v", f, cmd)
			}
			t.Logf("%d dependencies: five successors read %d rows, a command after a wide fact %d", n, successors, through)
			if successors > 5*2 || through > 3*queueProbeRows {
				t.Fatalf("%d dependencies: five successors read %d rows (want at most %d), a command after a wide fact %d (want at most %d)", n,
					successors, 5*2, through, 3*queueProbeRows)
			}
			results = append(results, result{successors, through})
		})
	}
	if results[1] != results[0] {
		t.Fatalf("rows grew with the wide fact: %v", results)
	}
}

// When everything between a frontier node and a command descends from the
// frontier node (a device's checkpoints and unfinished listening, which do
// not move the queue frontier), the walks are as long as that history; the
// sweep then reads it once in rank order, as the range scan did, so the
// command makes at most about one walk query for 24 nodes of it, whatever
// its length. The next command reads a few rows. With two such histories
// that never observe each other, the first command reads both once and the
// next ones a few rows each, though the other stays on the frontier.
func TestConnectedHistoryCostsOneReadOfIt(t *testing.T) {
	for _, h := range []int{1000, raceSize(100_000, 5000)} {
		t.Run(fmt.Sprint(h), func(t *testing.T) {
			withGraph(t, func(g *causalGraph) {
				f := g.node(g.now())
				g.admit(f)
				n := g.node(g.now(), g.chain(f, h))
				first := measureAdmission(func() { g.admit(n) })
				next := g.node(g.now(), n)
				second := measureAdmission(func() { g.admit(next) })
				if fr := g.frontier(); !slices.Equal(fr, []string{next}) {
					t.Fatalf("frontier %v", fr)
				}
				t.Logf("history %d: the first command %d queries, %d rows; the next %d queries, %d rows", h, first.queries, first.rows, second.queries,
					second.rows)
				if q := int64(h/queueSweepRatio + 4*queueProbeRows); first.queries > q || first.rows > int64(h+h/4+8*queueProbeRows) {
					t.Fatalf("the first command: %d queries (want at most %d), %d rows (want at most %d)", first.queries, q, first.rows, h+h/4+8*queueProbeRows)
				}
				if second.rows > 2 {
					t.Fatalf("the next command read %d rows", second.rows)
				}
			})
			withGraph(t, func(g *causalGraph) {
				base := g.node(g.now())
				a := g.node(g.now(), base)
				g.admit(a)
				b := g.node(g.now(), base)
				g.admit(b)
				g.chain(b, h) // the other device's history
				aEnd := g.chain(a, h)
				var work []admissionWork
				cmd := aEnd
				for range 3 {
					cmd = g.node(g.now(), cmd)
					work = append(work, measureAdmission(func() { g.admit(cmd) }))
				}
				if fr := g.frontier(); !slices.Equal(fr, sorted(cmd, b)) {
					t.Fatalf("frontier %v, want %v", fr, sorted(cmd, b))
				}
				t.Logf("two histories of %d: commands %v", h, work)
				if q := int64(2*h/queueSweepRatio + 4*queueProbeRows); work[0].queries > q || work[0].rows > int64(2*h+h/2+8*queueProbeRows) {
					t.Fatalf("the first command: %+v, want at most %d queries and %d rows", work[0], q, 2*h+h/2+8*queueProbeRows)
				}
				for _, w := range work[1:] {
					if w.rows > 2*queueProbeRows {
						t.Fatalf("a next command: %+v", w)
					}
				}
			})
		})
	}
}

// The sweep resolves a frontier node only once every edge from nodes ranked
// above it is read, whatever its pages: run alone, one edge a page, on two
// interleaved histories (from a frontier node at the bottom, and from one
// half way up whose next node is ranked right after it) with a concurrent
// frontier node, every frontier node it has resolved after each page is
// related exactly when it is an ancestor of the command; and, the other way,
// it finds the frontier node whose past holds an old node exactly when one
// does.
func TestSweepResolvesAtEveryPageBoundary(t *testing.T) {
	withGraph(t, func(g *causalGraph) {
		base := g.node(g.now())
		f := g.node(g.now(), base)
		other := g.node(g.now(), base)
		g.admit(f)
		g.admit(other)
		a, b, m := f, "", ""
		for i := range 300 {
			a = g.node(g.now(), a)
			switch {
			case i == 150:
				m = g.node(g.now(), base)
				g.admit(m)
				b = m
			case i > 150:
				b = g.node(g.now(), b)
			}
		}
		n := g.node(g.now(), a, b)
		rank, err := g.a.NodeRank(n)
		if err != nil {
			t.Fatal(err)
		}
		past, err := g.a.Ancestors(n, 0)
		if err != nil {
			t.Fatal(err)
		}
		s := (&queueRelation{a: g.a, node: n, rank: rank}).search(false)
		if err := s.begin(); err != nil {
			t.Fatal(err)
		}
		if len(s.open) != 3 {
			t.Fatalf("%d frontier walks", len(s.open))
		}
		open := func() bool { return slices.ContainsFunc(s.open, func(w *memberWalk) bool { return !w.resolved }) }
		for pages := 0; !s.swept && open(); pages++ {
			if pages > 10_000 {
				t.Fatal("the sweep does not end")
			}
			s.allowance = 1
			if err := s.sweep(); err != nil {
				t.Fatal(err)
			}
			for _, w := range s.open {
				if w.resolved && s.found[w.id] != past[w.id] {
					t.Fatalf("after %d pages: %s resolved as related %v, an ancestor %v", pages+1, w.id, s.found[w.id], past[w.id])
				}
			}
		}
		for _, w := range s.open {
			if !w.resolved {
				t.Fatalf("%s unresolved after the sweep", w.id)
			}
		}
		// The other way: an old node, and frontier nodes after it, one
		// whose past holds it through a long history and one whose does not.
		old := g.node(g.now(), base)
		holder := g.node(g.now(), g.chain(old, 300))
		g.admit(holder)
		for _, c := range []struct {
			node string
			held bool
		}{{old, true}, {g.node(g.now(), base), false}} {
			r, err := g.a.NodeRank(c.node)
			if err != nil {
				t.Fatal(err)
			}
			s := (&queueRelation{a: g.a, node: c.node, rank: r}).search(true)
			if err := s.begin(); err != nil {
				t.Fatal(err)
			}
			for pages := 0; !s.swept && len(s.found) == 0; pages++ {
				if pages > 10_000 {
					t.Fatal("the sweep does not end")
				}
				s.allowance = 1
				if err := s.sweep(); err != nil {
					t.Fatal(err)
				}
			}
			if (len(s.found) > 0) != c.held {
				t.Fatalf("%s: a frontier node descends from it: %v, want %v", c.node, len(s.found) > 0, c.held)
			}
		}
	})
}

func sorted(ids ...string) []string {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}

// Admission's walks are index seeks of a page of one node's edges (a
// dependency's or a dependent's), in the index's order; its sweep is one
// range seek of the rank index. None scans the account's edges or sorts.
func TestQueueAdmissionWalksSeekTheIndexes(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var account string
	if err := s.Write(ctx, func(tx *WriteTx) error {
		a, err := tx.CreateAccount()
		account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		query, first string
		args         []any
	}{
		{causalDependenciesQuery, "SEARCH e USING", []any{account, "n", "", 64}},
		{causalDependentsQuery, "SEARCH e USING INDEX causal_edges_dependency", []any{account, "n", 0, 64}},
		{rankedEdgesQuery, "SEARCH o USING INDEX node_order_rank", []any{account, 1, 2, "", 64}},
	} {
		plan := queryPlan(t, s, c.query, c.args...)
		if !strings.HasPrefix(plan[0], c.first) || slices.ContainsFunc(plan, func(l string) bool {
			return strings.HasPrefix(l, "SCAN") || strings.Contains(l, "TEMP B-TREE")
		}) {
			t.Fatalf("%s\nplan:\n%s", c.query, strings.Join(plan, "\n"))
		}
	}
}
