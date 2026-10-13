package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The update copy's transaction.

// updatedServer is a stopped server's data directory with two accounts, its
// update copy saved, and then a new version's migration applied, as a
// candidate that migrated and failed leaves it.
func updatedServer(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	createAccounts(t, s, 2)
	s.Close()
	if err := SaveForUpdate(ctx, dir); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE newer (x); INSERT INTO schema_migrations VALUES (999, 'x', 0)`); err != nil {
		t.Fatal(err)
	}
	return dir
}

// opensAsBefore checks the previous version can open the database again.
func opensAsBefore(t *testing.T, dir string) {
	t.Helper()
	s, err := open(context.Background(), config{dataDir: dir, syncDir: noSync})
	if err != nil {
		t.Fatalf("the previous version cannot open the database: %v", err)
	}
	defer s.Close()
	var accounts, newer int
	s.reader.QueryRow(`SELECT COUNT(*) FROM accounts`).Scan(&accounts)
	s.reader.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name = 'newer'`).Scan(&newer)
	if accounts != 2 || newer != 0 {
		t.Fatalf("the database put back has %d accounts and %d newer tables", accounts, newer)
	}
}

// A cut at any step of putting the copy back is finished by the next
// start, from the copy, which stays until the update is resolved; the failed
// version's database is kept.
func TestUpdateRollbackResumesAfterACutAtEveryStep(t *testing.T) {
	ctx := context.Background()
	for _, point := range []string{"rollback-copied", "rollback-moved-wal", "rollback-moved-shm", "rollback-moved", "rollback-placed", "rollback-metadata"} {
		t.Run(point, func(t *testing.T) {
			dir := updatedServer(t)
			if err := BeginRollback(ctx, dir); err != nil {
				t.Fatal(err)
			}
			if err := BinaryRestored(dir); err != nil {
				t.Fatal(err)
			}
			cut := errors.New("cut")
			updateFault = func(p string) error {
				if p == point {
					return cut
				}
				return nil
			}
			err := FinishRollback(ctx, dir)
			updateFault = func(string) error { return nil }
			if !errors.Is(err, cut) {
				t.Fatalf("the cut at %s was not reached: %v", point, err)
			}
			opensAsBefore(t, dir) // the start finishes the rollback
			if step, _ := UpdateStep(dir); step != UpdateDBRestored {
				t.Fatalf("step %q after the start", step)
			}
			if _, err := os.Stat(filepath.Join(UpdateCopyDir(dir), databaseName)); err != nil {
				t.Fatalf("the copy went before the update was resolved: %v", err)
			}
			failed, _ := filepath.Glob(filepath.Join(dir, updateFailedPfx+"*"))
			if len(failed) == 0 {
				t.Fatal("the failed version's database was not kept")
			}
			if err := FinishRollback(ctx, dir); err != nil {
				t.Fatalf("repeating the finished rollback: %v", err)
			}
			opensAsBefore(t, dir)
		})
	}
}

// While the previous binary is not back, no server opens the database,
// and nothing is moved.
func TestNoStartDuringARollbackWaitingForTheBinary(t *testing.T) {
	ctx := context.Background()
	dir := updatedServer(t)
	if err := BeginRollback(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync}); !errors.Is(err, ErrUpdateRollback) {
		t.Fatalf("a start during the rollback: %v", err)
	}
	if err := FinishRollback(ctx, dir); err == nil {
		t.Fatal("the copy went into place before the binary was back")
	}
	db, _ := sql.Open("sqlite", filepath.Join(dir, databaseName)+"?mode=ro")
	defer db.Close()
	var newer int
	db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name = 'newer'`).Scan(&newer)
	if newer != 1 {
		t.Fatal("the live database was changed")
	}
}

// A rollback needs the copy there and verified before anything moves.
func TestRollbackNeedsAVerifiedCopy(t *testing.T) {
	ctx := context.Background()
	for name, spoil := range map[string]func(copyPath string){
		"missing": func(p string) { os.Remove(p) },
		"damaged": func(p string) { os.WriteFile(p, []byte("not a database"), 0o600) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := updatedServer(t)
			spoil(filepath.Join(UpdateCopyDir(dir), databaseName))
			if err := BeginRollback(ctx, dir); !errors.Is(err, ErrNoUpdateCopy) {
				t.Fatalf("got %v", err)
			}
			if step, _ := UpdateStep(dir); step != UpdateSaved {
				t.Fatalf("step %q", step)
			}
			if _, err := os.Stat(filepath.Join(dir, databaseName)); err != nil {
				t.Fatalf("the live database moved: %v", err)
			}
		})
	}
}

// An earlier copy whose record exists is never replaced, and a missing
// database is never created by the update's reads.
func TestUpdateCopiesAreNeverReplaced(t *testing.T) {
	ctx := context.Background()
	dir := updatedServer(t)
	before, _ := os.ReadFile(filepath.Join(UpdateCopyDir(dir), databaseName))
	if err := SaveForUpdate(ctx, dir); !errors.Is(err, ErrUpdatePending) {
		t.Fatalf("a second save: %v", err)
	}
	if after, _ := os.ReadFile(filepath.Join(UpdateCopyDir(dir), databaseName)); string(after) != string(before) {
		t.Fatal("the copy changed")
	}
	empty := t.TempDir()
	if err := SaveForUpdate(ctx, empty); err == nil {
		t.Fatal("an update copy of no database")
	}
	if _, err := os.Stat(filepath.Join(empty, databaseName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the update's read created a database: %v", err)
	}
}
