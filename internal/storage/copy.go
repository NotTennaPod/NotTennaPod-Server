package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// ErrNoDatabase is returned by OpenCopy for a directory without a server
// database.
var ErrNoDatabase = errors.New("storage: the directory holds no NotTennaPod server database")

// copiedFiles are what OpenCopy copies: the database with its write-ahead
// log or rollback journal (committed transactions a stopped server left
// there), and the keys its receipts and secrets need.
var copiedFiles = []string{databaseName, databaseName + "-wal", databaseName + "-journal", receiptKeysName, keysSecretName}

// OpenCopy copies a stopped server's data directory into work and opens the
// copy for inspection (PROTOCOL §16 witness, PROTOCOL §14). The
// copy is made while dataDir's existing lock is held, so a running server is
// refused (ErrLocked) and none starts meanwhile. Nothing in dataDir is
// created, written or re-permissioned: a directory without its lock file is
// refused. The copy holds the database, any write-ahead log or journal, and
// the receipt and secrets keys. It must be a server's database, with its
// schema, its server ID and its receipt.keys: nothing is set up or made for
// it. It opens as Open checks a database (quick check, schema, migrations,
// every account's head), with its receipt keys, but with none of a server
// start's steps: no update is resumed, no restore detected and no
// incarnation written. work must be missing or empty; the caller closes the
// store and removes work.
func OpenCopy(ctx context.Context, dataDir, work string) (*Store, error) {
	src, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(filepath.Join(src, databaseName)); err != nil || !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s", ErrNoDatabase, src)
	}
	if err := private.Dir(work); err != nil {
		return nil, fmt.Errorf("storage: the copy's folder: %w", err)
	}
	if entries, err := os.ReadDir(work); err != nil {
		return nil, err
	} else if len(entries) > 0 {
		return nil, fmt.Errorf("storage: the copy's folder %s is not empty", work)
	}
	lock, err := lockExisting(src)
	if err != nil {
		return nil, err
	}
	for _, name := range copiedFiles {
		if err := copyPrivate(filepath.Join(src, name), filepath.Join(work, name)); err != nil {
			lock.Close()
			return nil, err
		}
	}
	if err := lock.Close(); err != nil {
		return nil, err
	}
	return open(ctx, config{dataDir: work, mode: modeCopy, source: src})
}

// lockExisting takes dir's lock as lockDirectory does, through a read-only
// descriptor of the existing lock file: it never creates the file or changes
// its mode.
func lockExisting(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockName)
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("storage: %s has no %s: it is not a server's data directory, or its server never started", dir, lockName)
	}
	if err != nil {
		return nil, fmt.Errorf("storage: opening lock file: %w", err)
	}
	if fi, err := f.Stat(); err != nil || !fi.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("storage: %s is not a regular file", path)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, fmt.Errorf("storage: locking data directory: %w", err)
	}
	return f, nil
}

// requireServerDatabase refuses, before anything is migrated or made, a
// copied database that is not a server's: one without the schema, the
// server ID or the receipt.keys (in dir, the copy) a server's first start
// writes (an empty file, say). source names the copied directory.
func requireServerDatabase(ctx context.Context, q rowQuerier, dir, source string) error {
	refuse := func(why string) error {
		return fmt.Errorf("%w: %s: its database is not a NotTennaPod server's (%s)", ErrNoDatabase, source, why)
	}
	for _, table := range []string{"schema_migrations", "server_meta"} {
		if ok, err := hasTable(ctx, q, table); err != nil {
			return err
		} else if !ok {
			return refuse("no " + table)
		}
	}
	if id, ok, err := getMeta(ctx, q, "server_id"); err != nil {
		return err
	} else if !ok || id == "" {
		return refuse("no server ID")
	}
	if _, _, exists, err := readReceiptKeysFile(dir); err != nil {
		return err
	} else if !exists {
		return refuse("no " + receiptKeysName + " beside it")
	}
	return nil
}

// copyPrivate copies the regular file at from, if there is one, to a new
// private file to. from is opened as it is (no symlink followed, nothing
// changed).
func copyPrivate(from, to string) error {
	in, err := os.OpenFile(from, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: copying %s: %w", filepath.Base(from), err)
	}
	defer in.Close()
	if fi, err := in.Stat(); err != nil || !fi.Mode().IsRegular() {
		return fmt.Errorf("storage: copying %s: not a regular file", filepath.Base(from))
	}
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("storage: copying %s: %w", filepath.Base(from), err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return fmt.Errorf("storage: copying %s: %w", filepath.Base(from), err)
	}
	return out.Close()
}

// CheckIntegrity runs SQLite's full integrity check and its foreign key
// check on the database (Open runs the quick check only).
func (s *Store) CheckIntegrity(ctx context.Context) error {
	if s.closed.Load() {
		return ErrClosed
	}
	return checkIntegrity(ctx, s.reader)
}

// VerifyArchive checks the archive with this data directory's receipt keys
// as a restore checks a backup's (verifyMACs): every fact against its stored
// content hash and its signed receipt, every receipt's and decision's MAC,
// the superseded receipts, the positions, ranks and times stored beside
// them, and every account's log. It returns the first failure, which names
// its account.
func (s *Store) VerifyArchive(ctx context.Context) error {
	keyByID := map[string][]byte{}
	for _, k := range s.receiptKeys {
		keyByID[k.ID] = k.Secret
	}
	return s.Read(ctx, func(r *ReadTx) error {
		if err := verifyMACs(ctx, r.tx, keyByID); err != nil {
			return fmt.Errorf("checked with this data directory's %s: %w", receiptKeysName, err)
		}
		return nil
	})
}

// VerifySealed opens every sealed value (feed logins, stored feed responses,
// staged parts) with this data directory's secrets key, as a restore does a
// backup's (verifySealed). The key is read, never made: without it, any
// sealed value fails. The first failure names its account.
func (s *Store) VerifySealed(ctx context.Context) error {
	key, err := private.ReadFile(filepath.Join(s.dir, keysSecretName))
	if errors.Is(err, os.ErrNotExist) {
		return s.Read(ctx, func(r *ReadTx) error {
			for _, t := range sealedTables {
				if ok, err := hasTable(ctx, r.tx, t.table); err != nil || !ok {
					if err != nil {
						return err
					}
					continue
				}
				var account string
				err := r.tx.QueryRowContext(ctx, `SELECT account_id FROM `+t.table+` LIMIT 1`).Scan(&account)
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				return fmt.Errorf("%s is missing, and a sealed value in %s (account %s) needs it", keysSecretName, t.table, account)
			}
			return nil
		})
	}
	if err != nil {
		return err
	}
	if len(key) != 32 {
		return fmt.Errorf("%s must hold 32 bytes", keysSecretName)
	}
	return s.Read(ctx, func(r *ReadTx) error {
		if err := verifySealed(ctx, r.tx, key); err != nil {
			return fmt.Errorf("checked with this data directory's %s: %w", keysSecretName, err)
		}
		return nil
	})
}
