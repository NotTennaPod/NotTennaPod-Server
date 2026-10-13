package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// ErrBackupRunning is returned while another backup of the same store runs.
var ErrBackupRunning = errors.New("storage: a backup is already running")

// BackupInfo describes a published backup.
type BackupInfo struct {
	Path    string
	Size    int64
	TakenAt int64 // Unix milliseconds
}

const backupTempPrefix = ".nottennapod-backup-"

// Backup writes a consistent, verified copy of the database into dir (PROTOCOL
// §11). It reads one snapshot on its own connection, outside write
// serialization, so pushes continue; the copy is marked as a backup, reopened
// and checked, then made durable and renamed into place. Only one backup runs
// at a time.
func (s *Store) Backup(ctx context.Context, dir string) (BackupInfo, error) {
	return s.BackupAs(ctx, dir, func(taken time.Time) (string, error) { return backupName(dir, taken) })
}

// BackupAs is Backup with the published file's path chosen by name, given
// the backup's time (scheduled backups carry their publication counter in
// the name, PROTOCOL §14).
func (s *Store) BackupAs(ctx context.Context, dir string, name func(taken time.Time) (string, error)) (BackupInfo, error) {
	if !s.backupMu.TryLock() {
		return BackupInfo{}, ErrBackupRunning
	}
	defer s.backupMu.Unlock()
	if s.closed.Load() {
		return BackupInfo{}, ErrClosed
	}
	// The folder must belong to the server's user and is made 0700.
	if err := private.Dir(dir); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: backup directory: %w", err)
	}
	if err := private.Tree(dir); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: backup directory: %w", err)
	}
	removeStaleBackupTemps(dir)
	want, err := s.SchemaVersion(ctx)
	if err != nil {
		return BackupInfo{}, err
	}
	var incarnation string
	if err := s.reader.QueryRowContext(ctx, "SELECT COALESCE((SELECT value FROM server_meta WHERE key = 'incarnation'), '')").Scan(&incarnation); err != nil {
		return BackupInfo{}, err
	}
	serverID, err := s.ServerID(ctx)
	if err != nil {
		return BackupInfo{}, err
	}
	if len(s.receiptKeys) == 0 {
		return BackupInfo{}, errors.New("storage: a backup needs the server's receipt keys")
	}
	taken := s.now().UTC()
	// VACUUM INTO needs an empty or missing target. The file is created
	// exclusively under a random name (0600), and its descriptor stays open:
	// the MAC is computed and written through it, and the file published is
	// checked to be this one, never another reached by name.
	f, err := os.CreateTemp(dir, backupTempPrefix+"*.tmp")
	if err != nil {
		return BackupInfo{}, fmt.Errorf("storage: creating backup file: %w", err)
	}
	temp := f.Name()
	defer os.Remove(temp) // a no-op once renamed
	defer f.Close()
	if err := private.Protect(f); err != nil {
		return BackupInfo{}, err
	}
	same := func(path string) error {
		if err := sameFile(f, path); err != nil {
			return fmt.Errorf("storage: the backup file was replaced while it was written: %w", err)
		}
		return nil
	}
	if err := s.copyInto(ctx, temp); err != nil {
		return BackupInfo{}, err
	}
	if err := markBackup(ctx, temp, incarnation, taken.UnixMilli()); err != nil {
		return BackupInfo{}, err
	}
	if err := same(temp); err != nil {
		return BackupInfo{}, err
	}
	if err := VerifyDatabaseFile(ctx, temp, want); err != nil {
		return BackupInfo{}, err
	}
	if err := same(temp); err != nil {
		return BackupInfo{}, err
	}
	if err := signBackup(f, serverID, s.receiptKeys[0]); err != nil {
		return BackupInfo{}, err
	}
	if err := f.Sync(); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: making backup durable: %w", err)
	}
	final, err := name(taken)
	if err != nil {
		return BackupInfo{}, err
	}
	if err := same(temp); err != nil {
		return BackupInfo{}, err
	}
	if err := os.Rename(temp, final); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: publishing backup: %w", err)
	}
	if err := same(final); err != nil {
		os.Remove(final)
		return BackupInfo{}, err
	}
	if err := syncDir(dir); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: making backup durable: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		return BackupInfo{}, err
	}
	return BackupInfo{Path: final, Size: st.Size(), TakenAt: taken.UnixMilli()}, nil
}

// sameFile checks that path names the regular file f has open.
func sameFile(f *os.File, path string) error {
	held, err := f.Stat()
	if err != nil {
		return err
	}
	named, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !named.Mode().IsRegular() || !os.SameFile(held, named) {
		return fmt.Errorf("%s is not the file written", filepath.Base(path))
	}
	return nil
}

// copyInto copies one read snapshot of the live database into path.
func (s *Store) copyInto(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", filepath.Join(s.dir, databaseName)+"?"+url.Values{"_pragma": {"busy_timeout(5000)", "trusted_schema(0)"}}.Encode())
	if err != nil {
		return fmt.Errorf("storage: opening backup reader: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", path); err != nil {
		return fmt.Errorf("storage: copying database: %w", err)
	}
	return nil
}

// markBackup turns the copy into a self-contained rollback-journal file and
// records that it is a backup, so a copy put back without the restore procedure is
// recognised at startup (PROTOCOL §11).
func markBackup(ctx context.Context, path, incarnation string, taken int64) error {
	db, err := sql.Open("sqlite", path+"?"+url.Values{"_pragma": {"trusted_schema(0)"}}.Encode())
	if err != nil {
		return fmt.Errorf("storage: opening backup copy: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=DELETE"); err != nil {
		return fmt.Errorf("storage: backup journal mode: %w", err)
	}
	marker, err := json.Marshal(map[string]any{"incarnation": incarnation, "taken_at": taken})
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO server_meta (key, value) VALUES ('backup', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", string(marker)); err != nil {
		return fmt.Errorf("storage: marking backup: %w", err)
	}
	return db.Close()
}

// VerifyDatabaseFile opens a database file read-only and checks its integrity,
// its foreign keys, that its schema version is want (or, with want < 0, any
// version this server knows) and that its schema is exactly the one that
// version's migrations create.
func VerifyDatabaseFile(ctx context.Context, path string, want int) error {
	db, err := sql.Open("sqlite", "file:"+path+"?"+url.Values{"mode": {"ro"}, "_pragma": {"trusted_schema(0)", "query_only(1)"}}.Encode())
	if err != nil {
		return fmt.Errorf("storage: opening %s: %w", filepath.Base(path), err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := checkIntegrity(ctx, db); err != nil {
		return err
	}
	var version int
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&version); err != nil {
		return fmt.Errorf("%w: reading schema version: %v", ErrIntegrity, err)
	}
	latest := embeddedMigrations[len(embeddedMigrations)-1].version
	switch {
	case version > latest:
		return fmt.Errorf("%w: schema %d", ErrSchemaTooNew, version)
	case want >= 0 && version != want:
		return fmt.Errorf("%w: schema %d, want %d", ErrIntegrity, version, want)
	}
	return checkSchema(ctx, db, embeddedMigrations)
}

// checkIntegrity runs SQLite's full integrity check and its foreign key check.
func checkIntegrity(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIntegrity, err)
	}
	var problems []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			return err
		}
		if line != "ok" {
			problems = append(problems, line)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrIntegrity, strings.Join(problems, "; "))
	}
	fk, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("%w: foreign_key_check: %v", ErrIntegrity, err)
	}
	broken := fk.Next()
	fk.Close()
	if broken {
		return fmt.Errorf("%w: foreign key violations", ErrIntegrity)
	}
	return nil
}

// backupName picks nottennapod-<UTC time>-<n>.db, the first n not yet used.
func backupName(dir string, t time.Time) (string, error) {
	stamp := t.Format("20060102-150405")
	for n := 1; n < 1000; n++ {
		p := filepath.Join(dir, fmt.Sprintf("nottennapod-%s-%d.db", stamp, n))
		if _, err := os.Lstat(p); errors.Is(err, os.ErrNotExist) {
			return p, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("storage: too many backups named %s", stamp)
}

// removeStaleBackupTemps deletes temporaries left by interrupted backups.
func removeStaleBackupTemps(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), backupTempPrefix) {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("storage: making backup durable: %w", err)
	}
	return nil
}
