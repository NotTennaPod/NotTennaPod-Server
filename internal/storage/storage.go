// Package storage is the server's durable store: one SQLite database per data
// directory, a single serialized writer, a pool of snapshot readers, versioned
// migrations, and the account-scoped change log (PROTOCOL §14).
//
// All SQL for account data lives in this package. Callers reach account data only
// through WriteTx.Account and ReadTx.Account, whose methods bind the account ID.
package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Errors returned by Open.
var (
	ErrLocked          = errors.New("storage: another server is using this data directory")
	ErrSchemaTooNew    = errors.New("storage: database schema is newer than this server")
	ErrMigrationEdited = errors.New("storage: an applied migration has changed")
	ErrIntegrity       = errors.New("storage: integrity check failed")
	ErrClosed          = errors.New("storage: store is closed")
)

const (
	databaseName = "nottennapod.db"
	lockName     = "server.lock"
)

// DefaultDataDir returns the platform's default data directory.
func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, "Library", "Application Support", "NotTennaPod-Server"), nil
	}
	if xdg := os.Getenv("XDG_DATA_HOME"); filepath.IsAbs(xdg) {
		return filepath.Join(xdg, "nottennapod-server"), nil
	}
	return filepath.Join(home, ".local", "share", "nottennapod-server"), nil
}

// Store is an open data directory.
type Store struct {
	dir    string
	dbName string
	lock   *os.File
	writer *sql.DB
	reader *sql.DB
	dbPath string
	hooks  hooks

	// verify is a test's own read connection (VerifyReadForTesting).
	verifyMu sync.Mutex
	verify   *sql.DB

	// The server's restore metadata and receipt keys (PROTOCOL §11): meta is
	// nil for a staging database, which has neither.
	meta            *metaFile
	dbIdentity      fileIdentity
	receiptKeys     []ReceiptKey
	restoredBecause string // why startup ran the transition, if it did

	// life is held shared by every Write and exclusively by Close, so Close waits
	// for writes in progress before releasing the database and the lock. (Reads do
	// not hold it; never call Read inside a Write callback.)
	life   sync.RWMutex
	closed atomic.Bool

	backupMu sync.Mutex   // one backup at a time (backup.go)
	memo     sync.Map     // in-memory caches of derived state (AccountTx.Memo)
	notes    notifier     // commit listeners (notify.go)
	instance atomic.Value // this run's instance ID (update.go)

	// now is the server's wall clock (WithClock): the time of what the
	// store records, and of a backup.
	now func() time.Time

	// capture: the schema has the read model's change capture (viewchanges.go).
	capture bool
	// gate is held through each write while a test's commit observer is
	// installed; opening keeps the opening writes' accounts for it until
	// the store is open (viewobserver.go).
	gate    sync.Mutex
	opening map[string]CommittedAccount
}

// hooks are test-only interruption points inside a write transaction, around
// the restore metadata writes and inside the restore procedure. An error from a
// state or restore hook is a simulated crash: nothing more is written.
type hooks struct {
	afterLogInsert       func() error
	afterHeadUpdate      func() error
	beforeCommit         func() error
	duringMigration      func(version int) error
	afterReceiptKeysFile func() error
	beforeStateWrite     func(phase string) error
	afterStateWrite      func(phase string) error
	restoreFault         func(point string) error
	backupFault          func(point string) error
	walCheckpoint        func(after string, res WALCheckpoint, err error) error
	viewCapture          func(point string) error // a suspended capture's steps (viewchanges.go)
}

func call(h func() error) error {
	if h == nil {
		return nil
	}
	return h()
}

// storeMode says which startup an opened database gets.
type storeMode int

const (
	// modeServer is the server's data directory: receipt keys from
	// receipt.keys, the restore metadata, detection and a new incarnation.
	modeServer storeMode = iota
	// modeStaging is PROTOCOL §11.5 import staging database: the main
	// server's ID and receipt keys, given by the caller, and nothing else.
	modeStaging
	// modeRaw is a database the restore procedure stages in a data directory
	// whose lock it already holds: migrations only.
	modeRaw
	// modeCopy is a copy of a stopped server's data directory opened for
	// inspection (OpenCopy): it must already be a server's database; its
	// receipt keys, and none of a server start's provisioning, update,
	// restore or incarnation steps.
	modeCopy
)

type config struct {
	dataDir    string
	dbName     string // default nottennapod.db
	mode       storeMode
	serverID   string       // modeStaging
	keys       []ReceiptKey // modeStaging
	syncDir    func(dir string) error
	migrations []migration
	hooks      hooks
	now        func() time.Time // default time.Now
	source     string           // modeCopy: the directory copied
	// The restore's free space (default statfs) and how often it is watched
	// while SQLite writes the staged copy (default 50 ms).
	free       func(dir string) (int64, error)
	spaceWatch time.Duration
}

// OpenOption configures Open and OpenStaging.
type OpenOption func(*config)

// WithClock sets the wall clock the store records times by (the server's
// one clock, PROTOCOL §16, which --dev-clock-offset moves): accounts',
// logins', installations' and credentials' times, commits', secrets',
// stored feed responses', parts' and migrations' times, imports' times (an
// import's server time), and a backup's time and name. The default is the
// system clock.
func WithClock(now func() time.Time) OpenOption {
	return func(c *config) { c.now = now }
}

// Open provisions the data directory, takes the process lock, finishes an
// interrupted restore, opens and migrates the database, loads the receipt keys,
// runs the startup checks of PROTOCOL §11 (moving a restored or replaced
// database to a new timeline), makes the directory chain durable and checks
// every account's log head before returning.
func Open(ctx context.Context, dataDir string, opts ...OpenOption) (*Store, error) {
	cfg := config{dataDir: dataDir}
	for _, o := range opts {
		o(&cfg)
	}
	return open(ctx, cfg)
}

// OpenStaging opens or creates dir/nottennapod.db as a staging database for
// PROTOCOL §11.5 import: locked, checked and migrated like Open, with
// server_meta's server ID set to serverID and the given receipt keys served by
// ReceiptKeys, so decisions made in staging carry MACs that verify on the main
// server. It reads and writes no restore metadata, receipt.keys or secrets.key,
// keeps no incarnation or auth revision and detects nothing.
func OpenStaging(ctx context.Context, dir, serverID string, keys []ReceiptKey, opts ...OpenOption) (*Store, error) {
	if serverID == "" || len(keys) == 0 {
		return nil, errors.New("storage: a staging database needs the server ID and receipt keys")
	}
	cfg := config{dataDir: dir, mode: modeStaging, serverID: serverID, keys: keys}
	for _, o := range opts {
		o(&cfg)
	}
	return open(ctx, cfg)
}

func open(ctx context.Context, cfg config) (_ *Store, err error) {
	if cfg.syncDir == nil {
		cfg.syncDir = syncDir
	}
	if cfg.migrations == nil {
		cfg.migrations = embeddedMigrations
	}
	if cfg.dbName == "" {
		cfg.dbName = databaseName
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	dir, err := filepath.Abs(cfg.dataDir)
	if err != nil {
		return nil, err
	}
	if strings.ContainsAny(dir, "?#") {
		return nil, fmt.Errorf("storage: data directory path must not contain '?' or '#': %s", dir)
	}
	// The directory, or the one a symlink at dir points to, must be this
	// user's, and is made 0700. A staging database's folder is inside the data
	// directory and must be a real folder.
	mkdir := private.Dir
	if cfg.mode == modeStaging {
		mkdir = private.RealDir
	}
	if err := mkdir(dir); err != nil {
		return nil, fmt.Errorf("storage: data directory: %w", err)
	}
	s := &Store{dir: dir, dbName: cfg.dbName, hooks: cfg.hooks, now: cfg.now}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	if cfg.mode != modeRaw {
		if s.lock, err = lockDirectory(dir); err != nil {
			return nil, err
		}
	}
	if cfg.mode == modeServer {
		// Nothing the directory holds keeps an ACL entry, which an
		// inheritable entry may have passed on.
		if err := private.Tree(dir); err != nil {
			return nil, fmt.Errorf("storage: data directory: %w", err)
		}
		// The keys and the metadata are regular files of this user, never
		// symlinks, and private.
		for _, name := range []string{receiptKeysName, keysSecretName, stateName} {
			if err := private.File(filepath.Join(dir, name)); err != nil {
				return nil, fmt.Errorf("storage: %w", err)
			}
		}
		// An update's rollback waiting for the previous binary stops the
		// start; one whose binary is back is finished first (update.go).
		if err := resumeUpdate(ctx, dir); err != nil {
			return nil, err
		}
		// A staged rebuild cut short by a stop leaves its copy (PROTOCOL §6.11).
		_ = os.RemoveAll(filepath.Join(dir, rebuildStagingDir))
		if s.meta, err = loadMeta(dir, &s.hooks); err != nil {
			return nil, err
		}
		// An interrupted restore finishes before the database is opened (PROTOCOL §14).
		if st := s.meta.state(); st != nil && st.Restore != nil {
			if err := resumeRestore(ctx, cfg, dir, s.meta); err != nil {
				return nil, err
			}
		} else {
			// A copy staged by a restore that stopped before recording itself,
			// and what an interrupted restore's verification left.
			_ = removeStagedCopy(dir)
			removeTemps(dir, restoreTempPrefix)
		}
	}
	dbPath := filepath.Join(dir, cfg.dbName)
	// SQLite follows symlinks and keeps the modes it finds: the database and
	// any WAL, shared-memory or journal file left beside it must be regular
	// files of this user, and are made 0600.
	for _, sfx := range []string{"-wal", "-shm", "-journal"} {
		if err := private.File(dbPath + sfx); err != nil {
			return nil, fmt.Errorf("storage: %w", err)
		}
	}
	// Create the file ourselves so it gets mode 0600 (SQLite gives its WAL and
	// shared-memory files the database file's permissions).
	f, err := private.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storage: opening the database: %w", err)
	}
	f.Close()
	if s.writer, err = openDB(dbPath, false); err != nil {
		return nil, err
	}
	s.writer.SetMaxOpenConns(1)
	s.writer.SetMaxIdleConns(1)
	s.writer.SetConnMaxLifetime(0)
	if err := s.checkPragmas(ctx, s.writer, false); err != nil {
		return nil, err
	}
	var check string
	if err := s.writer.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&check); err != nil || check != "ok" {
		return nil, fmt.Errorf("%w: quick_check: %q %v", ErrIntegrity, check, err)
	}
	// A copy must already be a server's database: nothing is set up for it.
	if cfg.mode == modeCopy {
		if err := requireServerDatabase(ctx, s.writer, dir, cfg.source); err != nil {
			return nil, err
		}
	}
	// The read model's capture triggers, when absent and nothing else
	// differs, are created again first (viewchanges.go); a copy never is.
	if cfg.mode == modeServer || cfg.mode == modeStaging {
		if err := s.repairViewCapture(ctx, cfg.migrations); err != nil {
			return nil, err
		}
	}
	// The schema is exactly what the migrations create, before they run
	// (they could fire a trigger of the database's own) and after.
	if err := checkSchema(ctx, s.writer, cfg.migrations); err != nil {
		return nil, err
	}
	if err := s.migrate(ctx, cfg.migrations); err != nil {
		return nil, err
	}
	if err := checkSchema(ctx, s.writer, cfg.migrations); err != nil {
		return nil, err
	}
	// Every server's chain has the capture (only a test's shorter chain
	// lacks it, and then nothing is captured or suspended).
	if s.capture, err = hasTable(ctx, s.writer, "view_clock"); err != nil {
		return nil, err
	}
	if err := s.openViewAudit(ctx); err != nil {
		return nil, err
	}
	switch cfg.mode {
	case modeServer:
		if err := s.ensureServerID(ctx); err != nil {
			return nil, err
		}
		if err := s.loadReceiptKeys(ctx); err != nil {
			return nil, err
		}
		if err := s.startup(ctx); err != nil {
			return nil, err
		}
	case modeStaging:
		if err := s.Write(ctx, func(tx *WriteTx) error { return setMeta(ctx, tx.tx, "server_id", cfg.serverID) }); err != nil {
			return nil, err
		}
		s.receiptKeys = append([]ReceiptKey(nil), cfg.keys...)
	case modeCopy:
		// Its server ID and receipt.keys exist (requireServerDatabase), so
		// this loads and checks them and makes none.
		if err := s.loadReceiptKeys(ctx); err != nil {
			return nil, err
		}
	}
	if s.reader, err = openDB(dbPath, true); err != nil {
		return nil, err
	}
	s.dbPath = dbPath
	s.reader.SetMaxOpenConns(4)
	if err := s.checkPragmas(ctx, s.reader, true); err != nil {
		return nil, err
	}
	if err := syncChain(dir, cfg.syncDir); err != nil {
		return nil, err
	}
	if physical, err := filepath.EvalSymlinks(dir); err != nil {
		return nil, fmt.Errorf("storage: resolving data directory: %w", err)
	} else if physical != dir {
		// Reached through a symlink: the physical directories hold the files.
		if err := syncChain(physical, cfg.syncDir); err != nil {
			return nil, err
		}
	}
	if err := s.checkHeads(ctx); err != nil {
		return nil, err
	}
	s.observeOpening()
	return s, nil
}

func openDB(path string, readOnly bool) (*sql.DB, error) {
	q := url.Values{}
	for _, p := range []string{"journal_mode(WAL)", "synchronous(FULL)", "fullfsync(1)", "checkpoint_fullfsync(1)",
		"foreign_keys(1)", "busy_timeout(5000)", "trusted_schema(0)", fmt.Sprintf("journal_size_limit(%d)", walSizeLimit)} {
		q.Add("_pragma", p)
	}
	if readOnly {
		q.Add("_pragma", "query_only(1)")
	} else {
		q.Set("_txlock", "immediate")
		// The one writer connection keeps a larger page cache: a rebuild
		// (PROTOCOL §6.11) reads and writes every derived table of an account.
		q.Add("_pragma", "cache_size(-65536)")
	}
	db, err := sql.Open("sqlite", path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("storage: opening database: %w", err)
	}
	return db, nil
}

// Pragmas reports the durability settings of a writer or reader connection, for
// tests and diagnostics.
func (s *Store) Pragmas(ctx context.Context, reader bool) (map[string]string, error) {
	db := s.writer
	if reader {
		db = s.reader
	}
	out := map[string]string{}
	for _, p := range []string{"journal_mode", "synchronous", "fullfsync", "checkpoint_fullfsync", "foreign_keys", "trusted_schema", "query_only",
		"journal_size_limit"} {
		var v string
		if err := db.QueryRowContext(ctx, "PRAGMA "+p).Scan(&v); err != nil {
			return nil, err
		}
		out[p] = v
	}
	return out, nil
}

func (s *Store) checkPragmas(ctx context.Context, db *sql.DB, reader bool) error {
	want := map[string]string{"journal_mode": "wal", "synchronous": "2", "fullfsync": "1", "checkpoint_fullfsync": "1",
		"foreign_keys": "1", "trusted_schema": "0", "query_only": "0", "journal_size_limit": strconv.Itoa(walSizeLimit)}
	if reader {
		want["query_only"] = "1"
	}
	for p, v := range want {
		var got string
		if err := db.QueryRowContext(ctx, "PRAGMA "+p).Scan(&got); err != nil {
			return fmt.Errorf("storage: reading PRAGMA %s: %w", p, err)
		}
		if got != v {
			return fmt.Errorf("storage: PRAGMA %s is %q, want %q", p, got, v)
		}
	}
	return nil
}

// lockDirectory takes an exclusive advisory lock held for the process lifetime.
func lockDirectory(dir string) (*os.File, error) {
	f, err := private.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storage: opening lock file: %w", err)
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

// syncDir makes a directory's entries durable: F_FULLFSYNC on macOS (Go's
// File.Sync), fsync elsewhere.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// syncChain synchronizes dir and every ancestor up to the root, deepest first.
// Open calls it for the path as given (whose parents hold any symlinks on the
// way) and, if different, for the physical path the symlinks resolve to.
func syncChain(dir string, sync func(string) error) error {
	for {
		if err := sync(dir); err != nil {
			return fmt.Errorf("storage: making directory durable: %s: %w", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

// Close waits for writes in progress, then releases the database and the
// directory lock. Later writes fail with ErrClosed.
func (s *Store) Close() error {
	s.life.Lock()
	defer s.life.Unlock()
	if s.closed.Swap(true) {
		return nil
	}
	var errs []error
	if s.reader != nil {
		errs = append(errs, s.reader.Close())
	}
	s.verifyMu.Lock()
	if s.verify != nil {
		errs = append(errs, s.verify.Close())
	}
	s.verifyMu.Unlock()
	if s.writer != nil {
		errs = append(errs, s.writer.Close())
	}
	if s.lock != nil {
		errs = append(errs, s.lock.Close()) // closing releases the flock
	}
	return errors.Join(errs...)
}

// Dir returns the absolute data directory.
func (s *Store) Dir() string { return s.dir }

func (s *Store) ensureServerID(ctx context.Context) error {
	return s.Write(ctx, func(tx *WriteTx) error {
		_, err := tx.tx.ExecContext(ctx, "INSERT INTO server_meta (key, value) VALUES ('server_id', ?) ON CONFLICT (key) DO NOTHING", newUUID())
		return err
	})
}

// ServerID returns the server's stable random identity.
func (s *Store) ServerID(ctx context.Context) (string, error) {
	var id string
	err := s.reader.QueryRowContext(ctx, "SELECT value FROM server_meta WHERE key = 'server_id'").Scan(&id)
	return id, err
}

// newUUID returns a random version-4 UUID in contract form.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Now is the store's wall clock (WithClock).
func (s *Store) Now() time.Time { return s.now() }

func (s *Store) nowMillis() int64   { return s.now().UnixMilli() }
func (w *WriteTx) nowMillis() int64 { return w.store.nowMillis() }
