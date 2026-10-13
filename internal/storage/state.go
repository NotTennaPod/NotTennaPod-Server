package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/keys"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// The restore metadata (PROTOCOL §11, with the auth revision of PROTOCOL §11.3):
// restore-state.json in the data directory, outside every backup and never rolled
// back. Every write goes to a temporary file that is synced, renamed into place,
// and the directory synced.

const stateName = "restore-state.json"

// Errors returned by Open and Restore.
var (
	ErrOtherServer    = errors.New("storage: this database belongs to another server")
	ErrKeysMismatch   = errors.New("storage: the receipt keys do not match")
	ErrStateDamaged   = errors.New("storage: restore-state.json is damaged")
	ErrRestoreRefused = errors.New("storage: restore refused")
	ErrKeysMissing    = errors.New("storage: the receipt keys are missing")
)

// errCrashed stops every later metadata write once a test hook has simulated a
// crash, as a dead process writes nothing more.
var errCrashed = errors.New("storage: simulated crash")

type restoreState struct {
	Version             int              `json:"version"`
	ServerID            string           `json:"server_id"`
	EpochHighWater      int64            `json:"epoch_high_water"`
	Incarnation         string           `json:"incarnation"`
	NextIncarnation     string           `json:"next_incarnation,omitempty"`
	DBFile              *fileIdentity    `json:"db_file,omitempty"`
	AuthRevision        int64            `json:"auth_revision"`
	PendingAuthRevision int64            `json:"pending_auth_revision,omitempty"`
	Restore             *restoreProgress `json:"restore,omitempty"`
	KeysExportedAt      int64            `json:"keys_exported_at"`
}

// restoreProgress is a restore procedure in progress (PROTOCOL §11.3). Everything a resumed
// run needs to reach the same result is fixed when the restore starts or when
// its step is recorded.
type restoreProgress struct {
	Step         string         `json:"step"`
	Backup       string         `json:"backup"`
	BackupSHA256 string         `json:"backup_sha256"` // the verified bytes; every staging checks its copy
	StartedAt    int64          `json:"started_at"`
	Incarnation  string         `json:"incarnation"`
	PreRestore   string         `json:"pre_restore"`
	Epoch        int64          `json:"epoch,omitempty"`
	AuthRevision int64          `json:"auth_revision,omitempty"`
	Report       *RestoreReport `json:"report,omitempty"`
}

// fileIdentity is the database file's device and inode (PROTOCOL §11.3).
type fileIdentity struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

func identityOf(path string) (fileIdentity, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fileIdentity{}, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, fmt.Errorf("storage: no file identity for %s", path)
	}
	return fileIdentity{Dev: uint64(st.Dev), Ino: uint64(st.Ino)}, nil
}

func (st *restoreState) clone() *restoreState {
	c := *st
	if st.DBFile != nil {
		id := *st.DBFile
		c.DBFile = &id
	}
	if st.Restore != nil {
		p := *st.Restore
		c.Restore = &p
	}
	return &c
}

// metaFile holds the metadata in memory and writes every change through to the
// file. Updates are serialized; a write that fails leaves the old value in
// memory.
type metaFile struct {
	dir     string
	hooks   *hooks
	mu      sync.Mutex
	st      *restoreState // nil while no file exists and none is being created
	crashed atomic.Bool
}

// loadMeta reads restore-state.json (nil state when missing) and removes
// temporaries of interrupted writes.
func loadMeta(dir string, h *hooks) (*metaFile, error) {
	removeStaleTemps(dir, stateName, receiptKeysName, keysSecretName)
	m := &metaFile{dir: dir, hooks: h}
	data, err := private.ReadFile(filepath.Join(dir, stateName))
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	var st restoreState
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStateDamaged, err)
	}
	if st.Version != 1 || st.ServerID == "" {
		return nil, fmt.Errorf("%w: version %d, server %q", ErrStateDamaged, st.Version, st.ServerID)
	}
	m.st = &st
	return m, nil
}

// removeStaleTemps deletes the temporaries keys.WriteFile leaves when a write
// is interrupted (".<name>.*.tmp").
func removeStaleTemps(dir string, names ...string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		for _, n := range names {
			if strings.HasPrefix(e.Name(), "."+n+".") && strings.HasSuffix(e.Name(), ".tmp") {
				os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
}

// state returns a copy of the current metadata (nil if none).
func (m *metaFile) state() *restoreState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.st == nil {
		return nil
	}
	return m.st.clone()
}

// update applies fn to a copy of the metadata (a fresh version 1 record when
// none exists), writes it durably and then makes it current. phase names the
// write for test hooks.
func (m *metaFile) update(phase string, fn func(st *restoreState)) error {
	return m.updateIf(phase, func(st *restoreState) bool { fn(st); return true })
}

// updateIf is update for a change that may turn out unnecessary: nothing is
// written when fn returns false.
func (m *metaFile) updateIf(phase string, fn func(st *restoreState) bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := &restoreState{Version: 1}
	if m.st != nil {
		next = m.st.clone()
	}
	if !fn(next) {
		return nil
	}
	if next.ServerID == "" {
		return fmt.Errorf("storage: restore metadata without a server ID")
	}
	if err := m.write(phase, next); err != nil {
		return err
	}
	m.st = next
	return nil
}

func (m *metaFile) write(phase string, st *restoreState) error {
	if m.crashed.Load() {
		return errCrashed
	}
	if err := m.fault(m.hooks.beforeStateWrite, phase); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := keys.WriteFile(filepath.Join(m.dir, stateName), append(data, '\n')); err != nil {
		return fmt.Errorf("storage: writing %s: %w", stateName, err)
	}
	return m.fault(m.hooks.afterStateWrite, phase)
}

// fault runs a test hook; an error from it is a simulated crash.
func (m *metaFile) fault(h func(string) error, phase string) error {
	if h == nil {
		return nil
	}
	if err := h(phase); err != nil {
		m.crashed.Store(true)
		return fmt.Errorf("%w at %s: %w", errCrashed, phase, err)
	}
	return nil
}

// ---- database-side values in server_meta

const (
	metaIncarnation  = "incarnation"
	metaRestoreFlag  = "restore_metadata"
	metaAuthRevision = "auth_revision"
	metaBackup       = "backup"
)

type execQuerier interface {
	rowQuerier
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func getMeta(ctx context.Context, q rowQuerier, key string) (string, bool, error) {
	var v string
	err := q.QueryRowContext(ctx, "SELECT value FROM server_meta WHERE key = ?", key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, err
}

func setMeta(ctx context.Context, q execQuerier, key, value string) error {
	_, err := q.ExecContext(ctx, "INSERT INTO server_meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, value)
	return err
}

// dbFacts are what startup and the transition read from a database.
type dbFacts struct {
	serverID     string
	incarnation  string
	flag         bool // restore_metadata: the metadata file has existed
	marker       bool // a backup marker (PROTOCOL §11.3)
	authRevision int64
	maxEpoch     int64
}

func readDBFacts(ctx context.Context, q rowQuerier) (dbFacts, error) {
	var f dbFacts
	var err error
	if f.serverID, _, err = getMeta(ctx, q, "server_id"); err != nil {
		return f, err
	}
	if f.incarnation, _, err = getMeta(ctx, q, metaIncarnation); err != nil {
		return f, err
	}
	flag, _, err := getMeta(ctx, q, metaRestoreFlag)
	if err != nil {
		return f, err
	}
	f.flag = flag == "1"
	if _, f.marker, err = getMeta(ctx, q, metaBackup); err != nil {
		return f, err
	}
	if v, ok, err := getMeta(ctx, q, metaAuthRevision); err != nil {
		return f, err
	} else if ok {
		if f.authRevision, err = strconv.ParseInt(v, 10, 64); err != nil {
			return f, fmt.Errorf("%w: auth revision %q", ErrIntegrity, v)
		}
	}
	err = q.QueryRowContext(ctx, `SELECT MAX((SELECT COALESCE(MAX(epoch), 0) FROM account_logs), (SELECT COALESCE(MAX(epoch), 0) FROM log_commits))`).
		Scan(&f.maxEpoch)
	return f, err
}
