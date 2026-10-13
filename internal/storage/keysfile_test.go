package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// PROTOCOL §11: receipt keys live in receipt.keys, never in the database.

func rawDB(t *testing.T, dir string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, databaseName)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestReceiptKeysLiveInTheirOwnFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	ks, _ := s.ReceiptKeys(ctx)
	if len(ks) != 1 || ks[0].ID != "k1" || len(ks[0].Secret) != 32 {
		t.Fatalf("keys %+v", ks)
	}
	fi, err := os.Stat(filepath.Join(dir, receiptKeysName))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("receipt.keys: %v %v", fi, err)
	}
	var f receiptKeysJSON
	data, _ := os.ReadFile(filepath.Join(dir, receiptKeysName))
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	id, _ := s.ServerID(ctx)
	if f.ServerID != id || len(f.Keys) != 1 || f.Keys[0].KeyID != "k1" {
		t.Fatalf("file %s", data)
	}
	if n := countRows(t, rawDB(t, dir), `SELECT COUNT(*) FROM receipt_keys`); n != 0 {
		t.Fatalf("%d keys in the database", n)
	}
	s.Close()
	s2 := openTest(t, config{dataDir: dir})
	again, _ := s2.ReceiptKeys(ctx)
	if !sameKeys(ks, again) {
		t.Fatal("a restart changed the receipt key")
	}
}

// putKeysBackInTheDatabase turns a data directory into one from before
// PROTOCOL §11: the key is a database row and receipt.keys does not exist.
func putKeysBackInTheDatabase(t *testing.T, dir string, k ReceiptKey) {
	t.Helper()
	if _, err := rawDB(t, dir).Exec(`INSERT INTO receipt_keys (key_id, secret, created_at) VALUES (?, ?, 1)`, k.ID, k.Secret); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, receiptKeysName)); err != nil {
		t.Fatal(err)
	}
}

func TestReceiptKeysMoveOutOfTheDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	ks, _ := s.ReceiptKeys(ctx)
	s.Close()
	putKeysBackInTheDatabase(t, dir, ks[0])
	s = openTest(t, config{dataDir: dir})
	got, _ := s.ReceiptKeys(ctx)
	if !sameKeys(got, ks) {
		t.Fatal("the key changed when it moved out of the database")
	}
	if n := countRows(t, rawDB(t, dir), `SELECT COUNT(*) FROM receipt_keys`); n != 0 {
		t.Fatalf("%d rows left", n)
	}
	if _, fileKeys, ok, err := readReceiptKeysFile(dir); !ok || err != nil || !sameKeys(fileKeys, ks) {
		t.Fatalf("receipt.keys %v %v", ok, err)
	}
}

// PROTOCOL §11.1: interrupted between writing the file and deleting the rows, the next
// start finds both, checks they are equal, and finishes.
func TestReceiptKeysMoveSurvivesACrash(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	ks, _ := s.ReceiptKeys(ctx)
	s.Close()
	putKeysBackInTheDatabase(t, dir, ks[0])
	crash := errors.New("crash")
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync, hooks: hooks{afterReceiptKeysFile: func() error { return crash }}}); !errors.Is(err, crash) {
		t.Fatalf("open: %v", err)
	}
	if _, fileKeys, ok, _ := readReceiptKeysFile(dir); !ok || !sameKeys(fileKeys, ks) {
		t.Fatal("the file was not written before the rows were deleted")
	}
	if n := countRows(t, rawDB(t, dir), `SELECT COUNT(*) FROM receipt_keys`); n != 1 {
		t.Fatalf("%d rows, want the row still there", n)
	}
	s = openTest(t, config{dataDir: dir})
	got, _ := s.ReceiptKeys(ctx)
	if !sameKeys(got, ks) || countRows(t, rawDB(t, dir), `SELECT COUNT(*) FROM receipt_keys`) != 0 {
		t.Fatal("the next start did not finish the move")
	}
}

func TestReceiptKeysOfAnotherServerAreRefused(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	s.Close()
	// Rows that differ from the file: refused, nothing deleted.
	if _, err := rawDB(t, dir).Exec(`INSERT INTO receipt_keys (key_id, secret, created_at) VALUES ('k1', ?, 1)`, bytes.Repeat([]byte{7}, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync}); !errors.Is(err, ErrKeysMismatch) {
		t.Fatalf("different keys: %v", err)
	}
	if n := countRows(t, rawDB(t, dir), `SELECT COUNT(*) FROM receipt_keys`); n != 1 {
		t.Fatal("the refused start deleted the rows")
	}
	// A file of another server.
	other := t.TempDir()
	o := openTest(t, config{dataDir: other})
	o.Close()
	data, _ := os.ReadFile(filepath.Join(other, receiptKeysName))
	os.WriteFile(filepath.Join(dir, receiptKeysName), data, 0o600)
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync}); !errors.Is(err, ErrOtherServer) {
		t.Fatalf("another server's keys: %v", err)
	}
}

// Lost keys of a database holding signed data are never replaced by new ones;
// the server refuses to start until the key bundle puts them back.
func TestLostKeysAreInstalledFromTheBundle(t *testing.T) {
	fx := newRestoreFixture(t)
	c := fx.clone(t)
	if err := os.Remove(filepath.Join(c.dir, receiptKeysName)); err != nil {
		t.Fatal(err)
	}
	if _, err := open(context.Background(), config{dataDir: c.dir, syncDir: noSync}); !errors.Is(err, ErrKeysMissing) {
		t.Fatalf("a database with signed receipts got new keys: %v", err)
	}
	if err := InstallKeys(context.Background(), c.dir, fx.bundle, []byte("wrong")); !errors.Is(err, ErrRestoreRefused) {
		t.Fatalf("a wrong passphrase installed keys: %v", err)
	}
	if err := InstallKeys(context.Background(), c.dir, fx.bundle, fx.pass); err != nil {
		t.Fatal(err)
	}
	s, err := open(context.Background(), config{dataDir: c.dir, syncDir: noSync})
	if err != nil {
		t.Fatalf("after installing the keys: %v", err)
	}
	s.Close()
}
