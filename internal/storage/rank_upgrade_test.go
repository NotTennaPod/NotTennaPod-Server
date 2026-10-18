package storage

import (
	"database/sql"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"slices"
	"testing"
)

// earlierInsertRank is the causal rank placement of the earlier server,
// kept as the oracle: immediately before the first node after the last
// dependency whose priority is greater (else last); the midpoint of the gap;
// and a gap used up renumbers the whole account to (i+1)·rankGap. A database
// an earlier server wrote holds exactly these values, and its rebuild
// replays them, so this server must place every node as it did.
func earlierInsertRank(a *AccountTx, nodeID string, t int64, deps []string) error {
	ctx, tx := a.w.ctx, a.w.tx
	last, err := a.lastRank(deps)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 2; attempt++ {
		var nextRank sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MIN(o.rank) FROM causal_nodes n INDEXED BY causal_nodes_priority
			CROSS JOIN node_order o ON o.account_id = n.account_id AND o.node_id = n.node_id
			WHERE n.account_id = ?1 AND n.normalized_time >= ?3 AND (n.normalized_time > ?3 OR n.node_id > ?4) AND o.rank > ?2`,
			a.id, last, t, nodeID).Scan(&nextRank); err != nil {
			return err
		}
		next, found := nextRank.Int64, nextRank.Valid
		var prev int64
		if found {
			err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rank), ?2) FROM node_order WHERE account_id = ?1 AND rank > ?2 AND rank < ?3`,
				a.id, last, next).Scan(&prev)
		} else {
			err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rank), ?2) FROM node_order WHERE account_id = ?1 AND rank > ?2`, a.id, last).Scan(&prev)
		}
		if err != nil {
			return err
		}
		var rank int64
		switch {
		case !found:
			rank = prev + rankGap
		case next-prev >= 2:
			rank = prev + (next-prev)/2
		default:
			ids, err := queryStrings(ctx, tx, `SELECT node_id FROM node_order WHERE account_id = ? ORDER BY rank`, a.id)
			if err != nil {
				return err
			}
			for i, id := range ids {
				if _, err := tx.ExecContext(ctx, `UPDATE node_order SET rank = ? WHERE account_id = ? AND node_id = ?`, -int64(i+1), a.id, id); err != nil {
					return err
				}
			}
			for i, id := range ids {
				if _, err := tx.ExecContext(ctx, `UPDATE node_order SET rank = ? WHERE account_id = ? AND node_id = ?`, int64(i+1)*rankGap, a.id, id); err != nil {
					return err
				}
			}
			if last, err = a.lastRank(deps); err != nil {
				return err
			}
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO node_order (account_id, node_id, rank) VALUES (?, ?, ?)`, a.id, nodeID, rank)
		return err
	}
	return errors.New("cannot place node")
}

// Every node is placed exactly as the earlier server placed it, after every
// insert of random sequences that do what its databases did: tied times,
// siblings of one dependency placed before one another, runs after one
// another that use up a gap (so the earlier server renumbered), offline
// chains before later history, and late arrivals anywhere. So a database it
// wrote rebuilds to the same ranks (the rebuild invariant, and make
// acceptance-witness on an upgraded server).
func TestRanksArePlacedAsTheEarlierServerPlacedThem(t *testing.T) {
	for seed := range uint64(3) {
		withGraph(t, func(g *causalGraph) {
			tx := g.a.w
			other, err := tx.CreateAccount()
			if err != nil {
				t.Fatal(err)
			}
			old := tx.Account(other.ID)
			r := mrand.New(mrand.NewPCG(seed, 61))
			var all []string
			insert := func(id string, at int64, deps ...string) {
				t.Helper()
				slices.Sort(deps)
				deps = slices.Compact(deps)
				if err := g.a.InsertNode(id, "fact", at, deps); err != nil {
					t.Fatal(err)
				}
				if _, err := old.w.tx.ExecContext(old.w.ctx, `INSERT INTO causal_nodes (account_id, node_id, kind, normalized_time) VALUES (?, ?, 'fact', ?)`,
					old.id, id, at); err != nil {
					t.Fatal(err)
				}
				if err := earlierInsertRank(old, id, at, deps); err != nil {
					t.Fatal(err)
				}
				all = append(all, id)
				got, want := ranksOf(t, g.a), ranksOf(t, old)
				for node, rank := range want {
					if got[node] != rank {
						t.Fatalf("seed %d, after %s (%d nodes): %s at rank %d, the earlier server's %d", seed, id, len(all), node, got[node], rank)
					}
				}
			}
			n := 0
			fresh := func() string { n++; return fmt.Sprintf("x%06d", n) }
			for step := 0; len(all) < 600; step++ {
				switch k := r.IntN(10); {
				case k < 3 || len(all) == 0: // a new node, now
					insert(fresh(), int64(step*10), pickDeps(r, all, 2)...)
				case k < 5: // siblings of one dependency, at one time, placed before one another
					d, at := all[r.IntN(len(all))], int64(step*10)
					for i := range 5 + r.IntN(25) {
						insert(fmt.Sprintf("s%06d-%02d", step, 99-i), at, d)
					}
				case k < 7: // a run after one another at one time: it halves one gap
					d, at := all[r.IntN(len(all))], int64(step*10)
					for i := range 5 + r.IntN(25) {
						insert(fmt.Sprintf("r%06d-%02d", step, i), at, d)
					}
				case k < 9: // an offline chain of old facts, each after the one before
					prev := all[r.IntN(len(all))]
					for range 5 + r.IntN(25) {
						id := fresh()
						insert(id, int64(r.IntN(step*10+1)), prev)
						prev = id
					}
				default: // a late arrival with an old time
					insert(fresh(), int64(r.IntN(step*10+1)), pickDeps(r, all, 1)...)
				}
			}
		})
	}
}

func pickDeps(r *mrand.Rand, all []string, k int) []string {
	var out []string
	for range r.IntN(k + 1) {
		if len(all) > 0 {
			out = append(out, all[r.IntN(len(all))])
		}
	}
	return out
}

func ranksOf(t *testing.T, a *AccountTx) map[string]int64 {
	t.Helper()
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT node_id, rank FROM node_order WHERE account_id = ?`, a.id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var id string
		var r int64
		if err := rows.Scan(&id, &r); err != nil {
			t.Fatal(err)
		}
		out[id] = r
	}
	return out
}
