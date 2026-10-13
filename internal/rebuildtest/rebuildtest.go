// Package rebuildtest holds the rebuild invariant (PROTOCOL §6.11) the tests of
// every package that writes account state end with: rebuilding an account
// from its archive reproduces its published records with their provenance
// and its whole derived state, and appends no commit.
package rebuildtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Check runs the invariant on one account. normalize, when set, rewrites
// every derived-state row before the comparison (data written before a
// format change).
func Check(t testing.TB, svc *protocol.Service, account string, normalize func(string) string) {
	t.Helper()
	if t.Failed() || t.Skipped() {
		return
	}
	ctx := context.Background()
	before, err := svc.Store.DerivedState(ctx, account)
	if errors.Is(err, storage.ErrClosed) {
		return
	}
	if err != nil {
		t.Errorf("rebuild invariant: %v", err)
		return
	}
	headBefore := head(t, svc.Store, account)
	report, err := svc.Rebuild(ctx, account)
	after, derr := svc.Store.DerivedState(ctx, account)
	if derr != nil {
		t.Errorf("rebuild invariant: %v", derr)
		return
	}
	switch {
	case err != nil:
		t.Errorf("rebuild invariant: %v\n%s", err, Describe(report))
	case report.Committed || len(report.Changed) > 0:
		t.Errorf("rebuild invariant: the rebuild changed published records\n%s", Describe(report))
	default:
		t.Logf("rebuild invariant: ok (%d facts, %d decisions, %v)", report.Facts, report.Decisions, report.Duration)
	}
	if h := head(t, svc.Store, account); h != headBefore && !report.Committed {
		t.Errorf("rebuild invariant: the head moved from %d to %d without a commit", headBefore, h)
	}
	if !report.Committed {
		if normalize != nil {
			for _, state := range []map[string][]string{before, after} {
				for table, rows := range state {
					for i, r := range rows {
						rows[i] = normalize(r)
					}
					slices.Sort(rows)
					state[table] = rows
				}
			}
		}
		if n := DropUnarchivedEvidence(before, after); n > 0 {
			t.Logf("rebuild invariant: %d fetch evidence rows no archived entry names", n)
		}
		if diff := Diff(before, after); diff != "" {
			t.Errorf("rebuild invariant: derived state differs after the rebuild:\n%s", diff)
		}
	}
}

func head(t testing.TB, store *storage.Store, account string) int64 {
	var seq int64
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(account).Head()
		seq = h.Seq
		return err
	}); err != nil {
		t.Errorf("rebuild invariant: head: %v", err)
	}
	return seq
}

// Describe lists a rebuild report's counts, mismatches and changed records.
func Describe(r protocol.RebuildReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  %d facts, %d decisions (%d made again, %d admitted), lost rows %d\n", r.Facts, r.Decisions, r.Consumed, r.Admitted, r.LostRows)
	for _, m := range r.Mismatches {
		fmt.Fprintf(&b, "  mismatch: %s\n", m)
	}
	for _, u := range r.Unsupported {
		fmt.Fprintf(&b, "  unsupported: %s\n", u)
	}
	for i, rec := range r.Changed {
		if i == 20 {
			fmt.Fprintf(&b, "  … %d more changed records\n", len(r.Changed)-20)
			break
		}
		c, _ := contract.Canonical(rec.Object())
		fmt.Fprintf(&b, "  changed: %s\n", c)
	}
	return b.String()
}

// DropUnarchivedEvidence removes from before the identity evidence a rebuild cannot derive: rows a
// server fetch recorded (by the podcast's identity node) for an item it wrote no record of, which
// no archived entry names. The rebuild may lose such rows; it never adds or changes one. It returns
// how many it removed.
func DropUnarchivedEvidence(before, after map[string][]string) int {
	creating := map[string]bool{}
	for _, row := range before["podcasts"] {
		if v, ok := field(row, "creating_op"); ok {
			creating[v] = true
		}
	}
	var kept []string
	dropped := 0
	for _, row := range before["identity_occurrences"] {
		if _, ok := slices.BinarySearch(after["identity_occurrences"], row); !ok {
			if node, _ := field(row, "node_id"); creating[node] {
				dropped++
				continue
			}
		}
		kept = append(kept, row)
	}
	if kept == nil {
		kept = []string{}
	}
	before["identity_occurrences"] = kept
	return dropped
}

func field(row, name string) (string, bool) {
	for _, f := range strings.Fields(row) {
		if v, ok := strings.CutPrefix(f, name+"="); ok {
			return v, true
		}
	}
	return "", false
}

// Diff lists the rows that differ per table (a few each).
func Diff(before, after map[string][]string) string {
	var tables []string
	for t := range before {
		tables = append(tables, t)
	}
	slices.Sort(tables)
	var out []string
	for _, table := range tables {
		b, a := before[table], after[table]
		if slices.Equal(a, b) {
			continue
		}
		out = append(out, fmt.Sprintf("  %s: %d rows before, %d after", table, len(b), len(a)))
		shown := 0
		for _, x := range b {
			if _, ok := slices.BinarySearch(a, x); !ok && shown < 3 {
				out = append(out, "    - "+x)
				shown++
			}
		}
		shown = 0
		for _, x := range a {
			if _, ok := slices.BinarySearch(b, x); !ok && shown < 3 {
				out = append(out, "    + "+x)
				shown++
			}
		}
	}
	return strings.Join(out, "\n")
}
