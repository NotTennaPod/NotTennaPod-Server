package storage

// Deleting an account deletes everything it owns (PROTOCOL §5; every per-account table).

import (
	"context"
	"testing"
)

// Every table with an account_id column cascades from accounts, directly or
// through a parent that does, so a new table cannot miss it.
func TestEveryAccountTableCascadesFromItsAccount(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	rows, err := s.reader.QueryContext(ctx, `SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	type foreignKey struct {
		parent   string
		columns  map[string]bool
		cascades bool
	}
	keys := func(table string) []foreignKey {
		rows, err := s.reader.QueryContext(ctx, `SELECT id, "table", "from", on_delete FROM pragma_foreign_key_list(?)`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		byID := map[int]*foreignKey{}
		var order []int
		for rows.Next() {
			var id int
			var parent, from, onDelete string
			if err := rows.Scan(&id, &parent, &from, &onDelete); err != nil {
				t.Fatal(err)
			}
			if byID[id] == nil {
				byID[id] = &foreignKey{parent: parent, columns: map[string]bool{}, cascades: onDelete == "CASCADE"}
				order = append(order, id)
			}
			byID[id].columns[from] = true
		}
		var out []foreignKey
		for _, id := range order {
			out = append(out, *byID[id])
		}
		return out
	}
	hasAccountColumn := func(table string) bool {
		var n int
		if err := s.reader.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info(?) WHERE name = 'account_id'`, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	var owned func(table string, seen map[string]bool) bool
	owned = func(table string, seen map[string]bool) bool {
		if table == "accounts" {
			return true
		}
		if seen[table] {
			return false
		}
		seen[table] = true
		for _, k := range keys(table) {
			if k.columns["account_id"] && k.cascades && owned(k.parent, seen) {
				return true
			}
		}
		return false
	}
	checked := 0
	for _, table := range tables {
		if table == "accounts" || !hasAccountColumn(table) {
			continue
		}
		checked++
		if !owned(table, map[string]bool{}) {
			t.Errorf("%s has an account_id but no ON DELETE CASCADE from accounts", table)
		}
	}
	if checked < 20 {
		t.Fatalf("only %d per-account tables found", checked)
	}
}

// Migration 0021 removes the rows that deleted accounts left in the seven
// tables migrations 0012, 0013 and 0019 gave no owner, keeps the others'
// rows, and from then on deleting an account deletes its rows.
func TestMigrationGivesIdentityTablesTheirAccount(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:20]})
	ids := createAccounts(t, s, 2)
	inserts := map[string]string{
		"generation_population": `INSERT INTO generation_population (account_id, generation_id, node_id) VALUES (?, 'g', 'n')`,
		"identity_aliases":      `INSERT INTO identity_aliases (account_id, podcast_id, kind, value, episode_id, node_id) VALUES (?, 'p', 'guid', 'g', 'e', 'n')`,
		"identity_disputes": `INSERT INTO identity_disputes (account_id, podcast_id, kind, value, node_id)
			VALUES (?, 'p', 'enclosure', 'https://cdn.example.com/a?token=secret', 'n')`,
		"identity_releases": `INSERT INTO identity_releases (account_id, podcast_id, guid, episode_id, pub_date, first_enclosure, node_id, enclosures, original)
			VALUES (?, 'p', 'g', 'e', 1, 'https://cdn.example.com/a?token=secret', 'n', '["https://cdn.example.com/a?token=secret"]', 1)`,
		"identity_release_additions": `INSERT INTO identity_release_additions (account_id, podcast_id, guid, episode_id, enclosure, node_id)
			VALUES (?, 'p', 'g', 'e', 'https://cdn.example.com/b?token=secret', 'n')`,
		"episode_merges": `INSERT INTO episode_merges (account_id, from_episode, into_episode, node_id, podcast_id) VALUES (?, 'a', 'b', 'n', 'p')`,
		"podcast_merges": `INSERT INTO podcast_merges (account_id, from_podcast, into_podcast, from_generation, into_generation, node_id)
			VALUES (?, 'p', 'q', 'g', 'h', 'n')`,
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, id := range ids {
			for _, q := range inserts {
				if _, err := tx.tx.ExecContext(tx.ctx, q, id); err != nil {
					return err
				}
			}
		}
		return tx.DeleteAccount(ids[0])
	}); err != nil {
		t.Fatal(err)
	}
	count := func(s *Store, table, account string) int {
		var n int
		if err := s.reader.QueryRowContext(ctx, `SELECT count(*) FROM `+table+` WHERE account_id = ?`, account).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for table := range inserts {
		if count(s, table, ids[0]) != 1 {
			t.Fatalf("precondition: before 0021 a deleted account's %s row stays", table)
		}
	}
	s.Close()
	s = openTest(t, config{dataDir: dir})
	for table := range inserts {
		if n := count(s, table, ids[0]); n != 0 {
			t.Errorf("%s: %d rows of the deleted account after the migration", table, n)
		}
		if n := count(s, table, ids[1]); n != 1 {
			t.Errorf("%s: %d rows of the other account after the migration, want 1", table, n)
		}
	}
	var release, enclosures string
	var original int
	if err := s.reader.QueryRowContext(ctx, `SELECT first_enclosure, enclosures, original FROM identity_releases WHERE account_id = ?`, ids[1]).
		Scan(&release, &enclosures, &original); err != nil || release != "https://cdn.example.com/a?token=secret" || original != 1 ||
		enclosures != `["https://cdn.example.com/a?token=secret"]` {
		t.Errorf("a kept release changed: %q %q %d %v", release, enclosures, original, err)
	}
	if err := s.Write(ctx, func(tx *WriteTx) error { return tx.DeleteAccount(ids[1]) }); err != nil {
		t.Fatal(err)
	}
	for table := range inserts {
		if n := count(s, table, ids[1]); n != 0 {
			t.Errorf("%s: %d rows left after deleting the account", table, n)
		}
	}
}
