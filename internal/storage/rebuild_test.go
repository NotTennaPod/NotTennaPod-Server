package storage

import (
	"slices"
	"testing"
)

// Every account-scoped table is classified for rebuilds (PROTOCOL §6.11): cleared and
// derived again, saved aside, or kept as it is.
func TestRebuildTablesCoverEveryAccountTable(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	rows, err := s.reader.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, n)
	}
	rows.Close()
	for _, table := range tables {
		var has int
		if err := s.reader.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = 'account_id'`, table).Scan(&has); err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, list := range [][]string{rebuildCleared, rebuildSaved, rebuildKept} {
			if slices.Contains(list, table) {
				n++
			}
		}
		switch {
		case has == 0 && n > 0:
			t.Errorf("%s has no account_id but is classified", table)
		case has == 1 && n != 1:
			t.Errorf("account table %s must be in exactly one of rebuildCleared, rebuildSaved and rebuildKept", table)
		}
	}
}
