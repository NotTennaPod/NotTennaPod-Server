package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A queue command's publication reads the layout key writes ranked after the
// last rank it saw with one range seek of queue_keys_by_rank, never a scan of
// the account's layout writes; the rows come in rank order and only those
// after the rank, and KeyWriteOf reads one scope's row by its key.
func TestKeyWritesSinceSeeksTheRankIndex(t *testing.T) {
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
		if err != nil {
			return err
		}
		acct := tx.Account(account)
		for i := range 200 {
			if err := acct.WriteKey(KeyWrite{Scope: fmt.Sprintf("g/e%03d", i), Key: "k", PositionID: "p", Node: fmt.Sprintf("n%03d", i%7),
				Epoch: 1 + int64(i/100), Seq: int64(i % 50), Index: int64(i % 3)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	plan := queryPlan(t, s, keyWritesSinceQuery, account, 1, 40, 1, "n003")
	if !slices.ContainsFunc(plan, func(l string) bool {
		return strings.HasPrefix(l, "SEARCH queue_keys USING INDEX queue_keys_by_rank") && strings.Contains(l, "account_id=?") &&
			strings.Contains(l, ">")
	}) || slices.ContainsFunc(plan, func(l string) bool { return strings.Contains(l, "TEMP B-TREE") || strings.HasPrefix(l, "SCAN") }) {
		t.Fatalf("plan:\n%s", strings.Join(plan, "\n"))
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		acct := tx.Account(account)
		rows, err := acct.KeyWritesSince(1, 40, 1, "n003")
		if err != nil {
			return err
		}
		all, err := acct.KeyWrites()
		if err != nil {
			return err
		}
		var want []string
		after := func(k KeyWrite) bool {
			a := [3]int64{k.Epoch, k.Seq, k.Index}
			b := [3]int64{1, 40, 1}
			if a != b {
				return slices.Compare(a[:], b[:]) > 0
			}
			return k.Node > "n003"
		}
		for _, k := range all {
			if after(k) {
				want = append(want, k.Scope)
			}
		}
		var got []string
		for i, k := range rows {
			if i > 0 {
				p := rows[i-1]
				if slices.Compare([]int64{p.Epoch, p.Seq, p.Index}, []int64{k.Epoch, k.Seq, k.Index}) > 0 {
					return fmt.Errorf("rows out of rank order: %+v before %+v", p, k)
				}
			}
			got = append(got, k.Scope)
		}
		slices.Sort(want)
		slices.Sort(got)
		if !slices.Equal(got, want) || len(got) == 0 || len(got) == len(all) {
			return fmt.Errorf("rows after the rank: %v, want %v", got, want)
		}
		w, ok, err := acct.KeyWriteOf("g/e007")
		if err != nil || !ok || w.Scope != "g/e007" {
			return fmt.Errorf("KeyWriteOf: %+v %v %v", w, ok, err)
		}
		if _, ok, err := acct.KeyWriteOf("g/none"); ok || err != nil {
			return fmt.Errorf("KeyWriteOf of a scope without a write: %v %v", ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
