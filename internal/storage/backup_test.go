package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// openCopy opens a backup as the database of a fresh data directory, leaving the
// backup file itself untouched.
func openCopy(t *testing.T, backup string) *Store {
	t.Helper()
	data, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, databaseName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return openTest(t, config{dataDir: dir})
}

// A backup taken while writers push is a consistent snapshot: every account's
// chain verifies, caller rows exist for exactly the copied commits, and each head
// is a commit of the live log with the same digest.
func TestBackupWhileWriting(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 8)
	backups := t.TempDir()
	const writers, perWriter = 8, 40
	var wg sync.WaitGroup
	errs := make(chan error, writers+1)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := appendWithCallerRow(ctx, s, ids[(w+i)%len(ids)]); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	var infos []BackupInfo
	for i := 0; i < 3; i++ {
		info, err := s.Backup(ctx, backups)
		if err != nil {
			t.Fatal(err)
		}
		infos = append(infos, info)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	live := map[string][]StoredCommit{}
	if err := s.Read(ctx, func(r *ReadTx) error {
		for _, id := range ids {
			c, err := r.Account(id).Commits(0, 1<<40, 0)
			if err != nil {
				return err
			}
			live[id] = c
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, info := range infos {
		if info.Size == 0 || !strings.HasPrefix(filepath.Base(info.Path), "nottennapod-") {
			t.Fatalf("backup %+v", info)
		}
		if st, err := os.Stat(info.Path); err != nil || st.Mode().Perm() != 0o600 {
			t.Fatalf("backup mode %v %v", st.Mode(), err)
		}
		if _, err := os.Stat(info.Path + "-wal"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("backup has a WAL file: %v", err)
		}
		c := openCopy(t, info.Path)
		heads := verifyStore(t, c)
		if err := c.Read(ctx, func(r *ReadTx) error {
			for id, seq := range heads {
				h, err := r.Account(id).Head()
				if err != nil {
					return err
				}
				if seq == 0 {
					continue
				}
				if want := live[id][seq-1]; want.Seq != seq || !bytes.Equal(want.Digest, h.Digest) {
					t.Errorf("account %s head %d is not a live commit", id, seq)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(backups)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(infos) {
		t.Fatalf("backup directory holds %d entries, want %d", len(entries), len(infos))
	}
}

// Every backup carries the marker that startup uses to recognise a copy put back
// without the restore procedure.
func TestBackupIsMarked(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+info.Path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var marker, journal string
	if err := db.QueryRow("SELECT value FROM server_meta WHERE key = 'backup'").Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(marker, `"taken_at":`) {
		t.Fatalf("marker %s", marker)
	}
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "delete" {
		t.Fatalf("journal mode %q %v", journal, err)
	}
	var live int
	if err := s.reader.QueryRow("SELECT COUNT(*) FROM server_meta WHERE key = 'backup'").Scan(&live); err != nil || live != 0 {
		t.Fatalf("live database marked: %d %v", live, err)
	}
}

// Temporaries of an interrupted backup are removed by the next one; a second
// concurrent backup is refused rather than queued.
func TestBackupCleansUpAndRunsOnce(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	dir := t.TempDir()
	stale := filepath.Join(dir, backupTempPrefix+"1.tmp")
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.backupMu.Lock()
	if _, err := s.Backup(ctx, dir); !errors.Is(err, ErrBackupRunning) {
		t.Fatalf("concurrent backup: %v", err)
	}
	s.backupMu.Unlock()
	if _, err := s.Backup(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temporary survived: %v", err)
	}
}

// Verification refuses a damaged file and a schema other than the expected one.
func TestVerifyDatabaseFile(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	createAccounts(t, s, 2)
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want, err := s.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyDatabaseFile(ctx, info.Path, want); err != nil {
		t.Fatal(err)
	}
	if err := VerifyDatabaseFile(ctx, info.Path, want-1); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("wrong schema accepted: %v", err)
	}
	data, err := os.ReadFile(info.Path)
	if err != nil {
		t.Fatal(err)
	}
	// Overwrite the middle of the file: pages of the copy no longer verify.
	for i := len(data) / 2; i < len(data)/2+4096 && i < len(data); i++ {
		data[i] = 0xA5
	}
	damaged := filepath.Join(t.TempDir(), "damaged.db")
	if err := os.WriteFile(damaged, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyDatabaseFile(ctx, damaged, want); err == nil {
		t.Fatal("damaged file verified")
	}
}
