package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Migration 0024 keeps a stored response's re-parse outcome in the clear
// beside it (PROTOCOL §9.4), so diagnostics list it without opening the response.
// Responses stored before keep their bytes and time and have none; the
// sealed bytes stay the last column, so reading the outcome never walks
// them; a re-parse writes it with the response; a new response clears it;
// only a category and counts fit.
func TestMigrationKeepsReparseOutcomes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:23]})
	acct := createAccounts(t, s, 1)[0]
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, p := range []string{"p1:a", "p1:b"} {
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op-1')`, acct, p); err != nil {
				return err
			}
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO feed_responses (account_id, podcast_id, sealed, stored_at) VALUES (?, ?, ?, 7)`,
				acct, p, []byte("sealed "+p)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = openTest(t, config{dataDir: dir})
	var sealed []byte
	var storedAt int64
	if err := s.reader.QueryRowContext(ctx, `SELECT sealed, stored_at FROM feed_responses WHERE account_id = ? AND podcast_id = 'p1:b'`, acct).
		Scan(&sealed, &storedAt); err != nil || string(sealed) != "sealed p1:b" || storedAt != 7 {
		t.Fatalf("a response stored before the migration: %q %d %v", sealed, storedAt, err)
	}
	var last string
	if err := s.reader.QueryRowContext(ctx, `SELECT name FROM pragma_table_info('feed_responses') ORDER BY cid DESC LIMIT 1`).Scan(&last); err != nil ||
		last != "sealed" {
		t.Fatalf("the last column is %q (%v), want the sealed bytes", last, err)
	}
	read := func(podcast string) (ReparseOutcome, []ReparseFailure) {
		t.Helper()
		var o ReparseOutcome
		var f []ReparseFailure
		if err := s.Read(ctx, func(tx *ReadTx) error {
			var err error
			if o, err = tx.Account(acct).ReparseOutcome(podcast); err != nil {
				return err
			}
			f, err = tx.Account(acct).ReparseFailures()
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return o, f
	}
	if o, f := read("p1:a"); o != (ReparseOutcome{}) || len(f) != 0 {
		t.Fatalf("a response stored before the migration: %+v %+v", o, f)
	}
	put := func(podcast string, o *ReparseOutcome) error {
		return s.Write(ctx, func(tx *WriteTx) error {
			if o == nil {
				return tx.Account(acct).PutFeedResponse(podcast, []byte{2})
			}
			return tx.Account(acct).PutReparsedFeedResponse(podcast, []byte{3}, *o)
		})
	}
	given := ReparseOutcome{Failure: "timeout", Timeouts: 3, Parser: "ntp-go/1"}
	counting := ReparseOutcome{Timeouts: 1, Parser: "ntp-go/1"}
	if err := put("p1:a", &given); err != nil {
		t.Fatal(err)
	}
	if err := put("p1:b", &counting); err != nil {
		t.Fatal(err)
	}
	if o, f := read("p1:a"); o != given || !slices.Equal(f, []ReparseFailure{{"p1:a", given}}) {
		t.Errorf("given up: %+v, listed %+v", o, f)
	}
	if o, _ := read("p1:b"); o != counting {
		t.Errorf("counting: %+v", o)
	}
	if err := put("p1:a", nil); err != nil {
		t.Fatal(err)
	}
	if o, f := read("p1:a"); o != (ReparseOutcome{}) || len(f) != 0 {
		t.Errorf("after a new response: %+v %+v", o, f)
	}
	// Only a category and counts: no publisher text, no negative count.
	for _, bad := range []ReparseOutcome{{Failure: `Website title: "Show"`, Timeouts: 3}, {Failure: "timeout", Timeouts: -1}} {
		if err := put("p1:a", &bad); err == nil {
			t.Errorf("stored %+v", bad)
		}
	}
}

// Upgrading a populated database copies every stored response exactly. A
// full disk during the copy (SQLITE_FULL, through the database's page limit)
// leaves schema 23 and every response as it was, and the next start
// upgrades.
func TestMigration0024UpgradesAPopulatedDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:23]})
	accounts := createAccounts(t, s, 2)
	type response struct {
		account, podcast string
		sealed           []byte
		storedAt         int64
	}
	var responses []response
	for i := range 3 {
		r := response{accounts[i%2], fmt.Sprintf("p1:%d", i), make([]byte, 64<<10), int64(1000 + i)}
		rand.Read(r.sealed)
		responses = append(responses, r)
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, r := range responses {
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op')`, r.account, r.podcast); err != nil {
				return err
			}
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO feed_responses (account_id, podcast_id, sealed, stored_at) VALUES (?, ?, ?, ?)`,
				r.account, r.podcast, r.sealed, r.storedAt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	same := func(s *Store, version int) {
		t.Helper()
		if v, _ := s.SchemaVersion(ctx); v != version {
			t.Fatalf("schema version %d, want %d", v, version)
		}
		for _, r := range responses {
			var sealed []byte
			var storedAt int64
			if err := s.reader.QueryRowContext(ctx, `SELECT sealed, stored_at FROM feed_responses WHERE account_id = ? AND podcast_id = ?`,
				r.account, r.podcast).Scan(&sealed, &storedAt); err != nil || !bytes.Equal(sealed, r.sealed) || storedAt != r.storedAt {
				t.Fatalf("%s: %d bytes at %d (%v)", r.podcast, len(sealed), storedAt, err)
			}
		}
		var copies int
		if err := s.reader.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name = 'feed_responses_reparse'`).Scan(&copies); err != nil || copies != 0 {
			t.Fatalf("the migration's copy table is left: %d %v", copies, err)
		}
	}

	// A full disk: the database may grow by two pages, the copy needs dozens.
	db, err := sql.Open("sqlite", filepath.Join(dir, "nottennapod.db"))
	if err != nil {
		t.Fatal(err)
	}
	var pages int
	if err := db.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	db.Close()
	m := embeddedMigrations[23]
	full := append(slices.Clone(embeddedMigrations[:23]), newMigration(m.name, fmt.Sprintf("PRAGMA max_page_count = %d;\n", pages+2)+m.sql))
	_, err = open(ctx, config{dataDir: dir, syncDir: noSync, migrations: full})
	if err == nil || !strings.Contains(err.Error(), "database or disk is full") {
		t.Fatalf("the migration on a full disk: %v", err)
	}
	t.Logf("on a full disk: %v", err)
	s = openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:23]})
	same(s, 23)
	s.Close()

	s = openTest(t, config{dataDir: dir})
	same(s, len(embeddedMigrations))
	for _, r := range responses {
		var o ReparseOutcome
		if err := s.Read(ctx, func(tx *ReadTx) error {
			var err error
			o, err = tx.Account(r.account).ReparseOutcome(r.podcast)
			return err
		}); err != nil || o != (ReparseOutcome{}) {
			t.Errorf("%s after the upgrade: %+v %v", r.podcast, o, err)
		}
	}
}
