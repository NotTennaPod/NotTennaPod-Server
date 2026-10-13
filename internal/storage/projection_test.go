package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// Ranks kept by InsertNode equal contract.CausalOrder after every insertion,
// including insertions that exhaust a gap and force renumbering.
func TestRanksFollowCausalOrderIncludingRenumbering(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 1)
	acct := ids[0]
	var all []contract.OrderedFact
	insert := func(id string, tm int64, deps []string) {
		t.Helper()
		err := s.Write(ctx, func(tx *WriteTx) error { return tx.Account(acct).InsertNode(id, "fact", tm, deps) })
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, contract.OrderedFact{ID: id, Dependencies: deps, NormalizedTime: tm})
	}
	check := func(step string) {
		t.Helper()
		want, err := contract.CausalOrder(slices.Clone(all))
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		s.Read(ctx, func(r *ReadTx) error { got, err = r.Account(acct).OrderedNodeIDs(); return err })
		if !slices.Equal(got, want) {
			t.Fatalf("%s: ranks %v\nKahn %v", step, got, want)
		}
	}
	insert("a", 0, nil)
	insert("z", 100, nil)
	// 40 nodes that each land right after "a" (decreasing IDs, same time) halve
	// the same gap until it is exhausted, forcing renumbering.
	for i := 0; i < 40; i++ {
		insert(fmt.Sprintf("m%03d", 999-i), 1, []string{"a"})
		check(fmt.Sprintf("gap insertion %d", i))
	}
	// Random late arrivals with old times and random dependencies.
	r := rand.New(rand.NewPCG(9, 10))
	for i := 0; i < 200; i++ {
		var deps []string
		var lower int64
		if r.IntN(2) == 0 {
			d := all[r.IntN(len(all))]
			deps, lower = []string{d.ID}, d.NormalizedTime
		}
		tm := max(lower, r.Int64N(120))
		insert(fmt.Sprintf("r%04d", r.IntN(10000))+fmt.Sprint(i), tm, deps)
		check(fmt.Sprintf("random insertion %d", i))
	}
}

// Reading one scope's contributions must use the scope index and look ranks up
// by node, never walk the account's rank index (an unsubscribe refolds every
// episode scope of a generation).
func TestContributionsQueryUsesScopeIndex(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 1)
	err := s.Write(context.Background(), func(tx *WriteTx) error {
		rows, err := tx.tx.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+contributionsQuery, ids[0], "episode:x")
		if err != nil {
			return err
		}
		defer rows.Close()
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				return err
			}
			plan = append(plan, detail)
		}
		joined := strings.Join(plan, "; ")
		if strings.Contains(joined, "rank") || strings.Contains(joined, "SCAN") || strings.Contains(joined, "TEMP B-TREE") {
			t.Fatalf("contributions query plan: %s", joined)
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
}
