package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// BackupOptions guard a backup's writes (backupsched.Run gives every backup
// its free-space checks).
type BackupOptions struct {
	// Start is called with the backup lock held, after the folder's
	// temporaries left by an interrupted backup are removed and before
	// anything is written; an error refuses the backup.
	Start func() error
	// Space is asked before each 32 MiB the backup writes itself (the
	// compressed copy); an error stops the backup.
	Space func(writing int64) error
}

// Backup writes a consistent, verified, compressed and signed copy of the
// database into dir (PROTOCOL §11), named by BackupName, with no free-space
// guard: the server's backups go through backupsched, which adds it.
func (s *Store) Backup(ctx context.Context, dir string) (BackupInfo, error) {
	return s.BackupAs(ctx, dir, func(taken time.Time) (string, error) { return BackupName(dir, taken) }, BackupOptions{})
}

// BackupAs writes a backup into dir (PROTOCOL §11.2), named by name given
// the backup's time (scheduled backups carry their publication counter in the
// name, PROTOCOL §14). It reads one snapshot on its own connection, outside
// write serialization, so pushes continue; the copy is marked as a backup,
// reopened and checked, then compressed and signed, the result read back
// through the restore's own reader (MAC first, then the decoder) and compared
// with the copy, made durable and renamed into place. Only one backup runs at
// a time. A crash leaves only temporaries named .nottennapod-backup-*, which
// the next backup into the folder removes first.
func (s *Store) BackupAs(ctx context.Context, dir string, name func(taken time.Time) (string, error), opts BackupOptions) (BackupInfo, error) {
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
	// What an interrupted backup left goes before the space is checked: it
	// must never stop the backup that removes it.
	removeStaleBackupTemps(dir)
	if opts.Start != nil {
		if err := opts.Start(); err != nil {
			return BackupInfo{}, err
		}
	}
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
	key := s.receiptKeys[0]
	taken := s.now().UTC()
	// A simulated crash (tests) leaves the temporaries, as a real one does.
	crashed := false
	fault := func(point string) error {
		if h := s.hooks.backupFault; h != nil {
			if err := h(point); err != nil {
				crashed = true
				return err
			}
		}
		return nil
	}
	// Each file is created exclusively under a random name (0600) and its
	// descriptor stays open: everything is read and written through it, and the
	// file published is checked to be this one, never another reached by name.
	// VACUUM INTO needs an empty or missing target.
	newTemp := func() (*os.File, error) {
		f, err := os.CreateTemp(dir, backupTempPrefix+"*.tmp")
		if err != nil {
			return nil, fmt.Errorf("storage: creating backup file: %w", err)
		}
		if err := private.Protect(f); err != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, err
		}
		return f, nil
	}
	sameAs := func(f *os.File, path string) error {
		if err := sameFile(f, path); err != nil {
			return fmt.Errorf("storage: the backup file was replaced while it was written: %w", err)
		}
		return nil
	}
	raw, err := newTemp()
	if err != nil {
		return BackupInfo{}, err
	}
	defer func() {
		raw.Close()
		if !crashed {
			os.Remove(raw.Name()) // a no-op once removed
		}
	}()
	if err := s.copyInto(ctx, raw.Name()); err != nil {
		return BackupInfo{}, err
	}
	// Writes made during the copy waited in the log behind its read.
	s.CheckpointWAL(ctx, "backup")
	if err := markBackup(ctx, raw.Name(), incarnation, taken.UnixMilli()); err != nil {
		return BackupInfo{}, err
	}
	if err := sameAs(raw, raw.Name()); err != nil {
		return BackupInfo{}, err
	}
	if err := VerifyDatabaseFile(ctx, raw.Name(), want); err != nil {
		return BackupInfo{}, err
	}
	if err := sameAs(raw, raw.Name()); err != nil {
		return BackupInfo{}, err
	}
	if err := fault("raw"); err != nil {
		return BackupInfo{}, err
	}
	rawInfo, err := raw.Stat()
	if err != nil {
		return BackupInfo{}, err
	}
	gz, err := newTemp()
	if err != nil {
		return BackupInfo{}, err
	}
	defer func() {
		gz.Close()
		if !crashed {
			os.Remove(gz.Name()) // a no-op once renamed
		}
	}()
	t, err := compressBackup(ctx, io.NewSectionReader(raw, 0, rawInfo.Size()), gz, opts.Space)
	if err != nil {
		return BackupInfo{}, fmt.Errorf("storage: compressing the backup: %w", err)
	}
	if err := fault("compressed"); err != nil {
		return BackupInfo{}, err
	}
	if t, err = signBackupV2(ctx, gz, t, serverID, key); err != nil {
		return BackupInfo{}, err
	}
	if err := gz.Sync(); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: making backup durable: %w", err)
	}
	if err := fault("signed"); err != nil {
		return BackupInfo{}, err
	}
	// The published file is read back as a restore reads it: the trailer, the
	// MAC with this server's key, then the decoder, which checks the size and
	// SHA-256 of the copy that was verified.
	if err := sameAs(gz, gz.Name()); err != nil {
		return BackupInfo{}, err
	}
	gzInfo, err := gz.Stat()
	if err != nil {
		return BackupInfo{}, err
	}
	back, err := readTrailerV2(gz, gzInfo.Size())
	if err == nil && (back.DatabaseSHA256 != t.DatabaseSHA256 || back.DatabaseSize != rawInfo.Size()) {
		err = errors.New("the trailer does not describe the copy")
	}
	var a authentic
	if err == nil {
		a, err = authenticate(ctx, gz, back, map[string][]byte{key.ID: key.Secret}, serverID)
	}
	if err == nil {
		err = decompress(ctx, a, io.Discard, nil)
	}
	if err != nil {
		return BackupInfo{}, fmt.Errorf("storage: the compressed backup does not read back: %w", err)
	}
	if err := fault("self-checked"); err != nil {
		return BackupInfo{}, err
	}
	raw.Close()
	if err := os.Remove(raw.Name()); err != nil {
		return BackupInfo{}, err
	}
	final, err := name(taken)
	if err != nil {
		return BackupInfo{}, err
	}
	if err := sameAs(gz, gz.Name()); err != nil {
		return BackupInfo{}, err
	}
	if err := os.Rename(gz.Name(), final); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: publishing backup: %w", err)
	}
	if err := sameAs(gz, final); err != nil {
		os.Remove(final)
		return BackupInfo{}, err
	}
	if err := syncDir(dir); err != nil {
		return BackupInfo{}, fmt.Errorf("storage: making backup durable: %w", err)
	}
	if err := fault("renamed"); err != nil {
		return BackupInfo{}, err
	}
	return BackupInfo{Path: final, Size: gzInfo.Size(), TakenAt: taken.UnixMilli()}, nil
}

// VerifyBackup checks a backup of this server in place, with its keys: the
// MAC first, then, for a version 2 backup, the decoder over an exclusive copy
// made beside it (space is asked before each 32 MiB of it), whose size and
// SHA-256 must be the ones signed, the database this server verified before
// it signed it; for a version 1 backup, the database checks of
// VerifyDatabaseFile. It is the daily backups' check at start (backupsched).
func (s *Store) VerifyBackup(ctx context.Context, path string, space func(int64) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", filepath.Base(path))
	}
	serverID, err := s.ServerID(ctx)
	if err != nil {
		return err
	}
	keyByID := map[string][]byte{}
	for _, k := range s.receiptKeys {
		keyByID[k.ID] = k.Secret
	}
	switch backupFormat(f, fi.Size()) {
	case 1:
		t, err := readBackupTrailer(f, fi.Size())
		if err != nil {
			return err
		}
		if t.ServerID != serverID {
			return fmt.Errorf("the backup belongs to server %s, not %s", t.ServerID, serverID)
		}
		if err := checkBackupMAC(ctx, f, t, keyByID); err != nil {
			return err
		}
		return VerifyDatabaseFile(ctx, path, -1)
	case 2:
		c, _, err := copyExclusive(ctx, f, filepath.Dir(path), backupTempPrefix+"verify-*", space)
		if err != nil {
			return fmt.Errorf("storage: copying the backup to check it: %w", err)
		}
		defer os.Remove(c.Name())
		defer c.Close()
		ci, err := c.Stat()
		if err != nil {
			return err
		}
		t, err := readTrailerV2(c, ci.Size())
		if err != nil {
			return err
		}
		a, err := authenticate(ctx, c, t, keyByID, serverID)
		if err != nil {
			return err
		}
		return decompress(ctx, a, io.Discard, nil)
	default:
		return errUnsigned
	}
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

// BackupName picks nottennapod-<UTC time>-<n>.db.gz in dir, the first n
// used by neither a compressed backup nor an older .db one (PROTOCOL §11.2).
func BackupName(dir string, t time.Time) (string, error) {
	stamp := t.UTC().Format("20060102-150405")
	for n := 1; n < 1000; n++ {
		base := filepath.Join(dir, fmt.Sprintf("nottennapod-%s-%d.db", stamp, n))
		free := true
		for _, p := range []string{base, base + ".gz"} {
			if _, err := os.Lstat(p); err == nil {
				free = false
			} else if !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
		}
		if free {
			return base + ".gz", nil
		}
	}
	return "", fmt.Errorf("storage: too many backups named %s", stamp)
}

// removeStaleBackupTemps deletes temporaries left by interrupted backups and
// backup checks.
func removeStaleBackupTemps(dir string) { removeTemps(dir, backupTempPrefix) }

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
