package storage

import (
	"context"
	"database/sql"
	"fmt"
	mrand "math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// rankedIDs returns the account's nodes in rank order and their ranks.
func (g *causalGraph) rankedIDs() ([]string, map[string]int64) {
	g.t.Helper()
	rows, err := g.a.w.tx.QueryContext(g.a.w.ctx, `SELECT node_id, rank FROM node_order WHERE account_id = ? ORDER BY rank`, g.a.id)
	if err != nil {
		g.t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	ranks := map[string]int64{}
	for rows.Next() {
		var id string
		var r int64
		if err := rows.Scan(&id, &r); err != nil {
			g.t.Fatal(err)
		}
		ids = append(ids, id)
		ranks[id] = r
	}
	return ids, ranks
}

// laterHistory builds an old node and 2,000 later ones after it: an
// account's history since an offline device last pulled.
func (g *causalGraph) laterHistory() (old string) {
	old = g.node(1000)
	g.chain(old, 2000)
	return old
}

// An offline device's burst of 300 old facts, each after the one before and
// before the same later history: finding each fact's place reads a few rows,
// not the 2,000 later nodes of greater priority, and the order is the causal
// order (the burst, in its order, right after the old node). Its ranks are
// the earlier server's (TestRanksArePlacedAsTheEarlierServerPlacedThem), so
// a used-up gap renumbers the account.
func TestOfflineBurstPlacementReadsAFewRows(t *testing.T) {
	withGraph(t, func(g *causalGraph) {
		old := g.laterHistory()
		before, _ := g.rankedIDs()
		rows := rankPlacementRows.Load()
		var burst []string
		prev := old
		for i := range 300 {
			prev = g.node(int64(1001+i), prev)
			burst = append(burst, prev)
		}
		rows = rankPlacementRows.Load() - rows
		after, _ := g.rankedIDs()
		i := slices.Index(after, old)
		if want := slices.Concat(before[:i+1], burst, before[i+1:]); !slices.Equal(after, want) {
			t.Fatal("the order is not the causal order")
		}
		t.Logf("300 facts placed reading %d rows", rows)
		if rows > 4*300 {
			t.Fatalf("300 facts placed reading %d rows, want at most %d", rows, 4*300)
		}
	})
}

// oldPlace is the place the earlier lookup found, kept as the oracle: the
// least rank after last among the nodes of greater priority, and the rank
// right before it.
func oldPlace(a *AccountTx, last, t int64, id string) (prev, next int64, found bool, err error) {
	var nextRank sql.NullInt64
	if err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT MIN(o.rank) FROM causal_nodes n INDEXED BY causal_nodes_priority
		CROSS JOIN node_order o ON o.account_id = n.account_id AND o.node_id = n.node_id
		WHERE n.account_id = ?1 AND n.normalized_time >= ?3 AND (n.normalized_time > ?3 OR n.node_id > ?4) AND o.rank > ?2`,
		a.id, last, t, id).Scan(&nextRank); err != nil {
		return 0, 0, false, err
	}
	if nextRank.Valid {
		err = a.w.tx.QueryRowContext(a.w.ctx, `SELECT COALESCE(MAX(rank), ?2) FROM node_order WHERE account_id = ?1 AND rank > ?2 AND rank < ?3`,
			a.id, last, nextRank.Int64).Scan(&prev)
		return prev, nextRank.Int64, true, err
	}
	err = a.w.tx.QueryRowContext(a.w.ctx, `SELECT COALESCE(MAX(rank), ?2) FROM node_order WHERE account_id = ?1 AND rank > ?2`, a.id, last).Scan(&prev)
	return prev, 0, false, err
}

// Every insertion's place is the earlier lookup's, over random graphs whose
// nodes share a few times (large tied blocks), with old and new times and
// dependencies anywhere: the same next node and the same node before it.
func TestPlacementIsTheEarlierLookups(t *testing.T) {
	seeds, inserts := uint64(3), 1500
	if raceEnabled {
		seeds, inserts = 1, 300 // the race detector's size; make check runs the full one without it
	}
	for seed := range seeds {
		withGraph(t, func(g *causalGraph) {
			r := mrand.New(mrand.NewPCG(seed, 31))
			var all []string
			for i := range inserts {
				id := fmt.Sprintf("%08x", r.Uint32())
				t0 := int64(r.IntN(6)) * 1000 // few times: tied blocks
				var deps []string
				if len(all) > 0 && r.IntN(4) > 0 {
					deps = append(deps, all[r.IntN(len(all))])
				}
				last, err := g.a.lastRank(deps)
				if err != nil {
					t.Fatal(err)
				}
				p1, n1, f1, err := g.a.placeAfter(last, t0, id)
				if err != nil {
					t.Fatal(err)
				}
				p0, n0, f0, err := oldPlace(g.a, last, t0, id)
				if err != nil {
					t.Fatal(err)
				}
				if p1 != p0 || n1 != n0 || f1 != f0 {
					t.Fatalf("seed %d, insert %d: place (%d, %d, %v), the earlier lookup's (%d, %d, %v)", seed, i, p1, n1, f1, p0, n0, f0)
				}
				if err := g.a.InsertNode(id, "fact", t0, deps); err != nil {
					t.Fatal(err)
				}
				all = append(all, id)
			}
		})
	}
}

// The priority scan seeks to its cursor's time and ID together, so it reads
// no node of the cursor's time that sorts before the cursor.
func TestPriorityScanSeeksItsCursor(t *testing.T) {
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	plan := queryPlan(t, s, priorityScanQuery, "a", 1, "x", 10)
	if !strings.Contains(plan[0], "USING COVERING INDEX causal_nodes_priority (account_id=? AND (normalized_time,node_id)>(?,?))") {
		t.Fatalf("plan:\n%s", strings.Join(plan, "\n"))
	}
}
