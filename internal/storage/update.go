package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/keys"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// The update's database copy and the server's instance ID.
//
// service update stops the server and saves a consistent copy of the
// database, with the restore metadata, before the new version starts. If the
// new version is not ready, the copy is put back, but only when the server
// accepted nothing in between: the account logs' heads, the accounts and
// logins, the auth revision and the metadata's counters are what they were.
// A new version that migrated the database and then failed therefore leaves
// the previous version a database it can open.
//
// The copy's record (update-rollback/record.json) is the data directory's
// side of the update transaction, and every step is recorded before it is
// taken, so each one can be repeated:
//   - saved: the copy is complete and verified; the new version may run.
//   - rolling-back: the copy will go back; no server may open the database
//     until the previous binary is back (Open refuses).
//   - binary-restored: the previous binary is back; the copy goes into place
//     (Open finishes it).
//   - db-restored: the copy is in place; the copy itself stays until the
//     service has seen the previous version running.
//
// Nothing removes or overwrites a copy whose record exists, except
// DiscardUpdateCopy once the update is resolved.

const (
	updateCopyDir    = "update-rollback"
	updateRecordName = "record.json"
	updateFailedPfx  = databaseName + ".update-failed-"
	updateTempName   = databaseName + ".rollback"
	instanceName     = "server.instance"
)

// The recorded steps of the copy.
const (
	UpdateSaved          = "saved"
	UpdateRollingBack    = "rolling-back"
	UpdateBinaryRestored = "binary-restored"
	UpdateDBRestored     = "db-restored"
)

// Errors of the update transaction.
var (
	// ErrUpdateAccepted reports a server that accepted changes after the
	// update copy was saved: putting the copy back would lose them.
	ErrUpdateAccepted = errors.New("storage: the server accepted changes since the update copy was saved")
	// ErrUpdatePending reports an update copy that is still needed.
	ErrUpdatePending = errors.New("storage: an earlier update's database copy is still kept")
	// ErrUpdateRollback refuses to open a database whose rollback waits for
	// the previous binary.
	ErrUpdateRollback = errors.New("storage: an update is being rolled back; run `nottennapod-server service update` to finish it")
	// ErrNoUpdateCopy reports a rollback without a usable copy.
	ErrNoUpdateCopy = errors.New("storage: there is no usable update copy")
)

type updateRecord struct {
	Version     int             `json:"version"`
	Step        string          `json:"step"`
	State       json.RawMessage `json:"state,omitempty"` // restore-state.json as it was; absent if there was none
	Fingerprint string          `json:"fingerprint"`
	SavedAt     int64           `json:"saved_at"`
	FailedName  string          `json:"failed_name,omitempty"` // where the failed version's database goes
}

// UpdateCopyDir is where SaveForUpdate keeps its copy.
func UpdateCopyDir(dataDir string) string { return filepath.Join(dataDir, updateCopyDir) }

// readUpdateRecord reads the copy's record (nil if there is none).
func readUpdateRecord(dir string) (*updateRecord, error) {
	data, err := private.ReadFile(filepath.Join(dir, updateCopyDir, updateRecordName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: the update copy's record: %w", err)
	}
	var rec updateRecord
	if err := json.Unmarshal(data, &rec); err != nil || rec.Version != 1 || rec.Fingerprint == "" {
		return nil, fmt.Errorf("storage: the update copy's record is damaged: %v", err)
	}
	switch rec.Step {
	case UpdateSaved, UpdateRollingBack, UpdateBinaryRestored, UpdateDBRestored:
	default:
		return nil, fmt.Errorf("storage: the update copy's record names an unknown step %q", rec.Step)
	}
	return &rec, nil
}

func writeUpdateRecord(dir string, rec *updateRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return keys.WriteFile(filepath.Join(dir, updateCopyDir, updateRecordName), append(data, '\n'))
}

// UpdateStep returns the recorded step of dataDir's update copy ("" when
// there is none).
func UpdateStep(dataDir string) (string, error) {
	rec, err := readUpdateRecord(dataDir)
	if rec == nil || err != nil {
		return "", err
	}
	return rec.Step, nil
}

// SaveForUpdate copies a stopped server's database consistently (as a backup
// does) with its restore metadata into the data directory's update-rollback
// folder. It takes the data directory's lock, so it fails with ErrLocked while
// the server runs, and it refuses (ErrUpdatePending) while an earlier copy's
// record exists. A folder without a record is a copy cut short, never used.
func SaveForUpdate(ctx context.Context, dataDir string) error {
	dir, lock, err := lockForUpdate(dataDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	if rec, err := readUpdateRecord(dir); err != nil {
		return err
	} else if rec != nil {
		return fmt.Errorf("%w (step %s, in %s)", ErrUpdatePending, rec.Step, UpdateCopyDir(dir))
	}
	copyDir := filepath.Join(dir, updateCopyDir)
	if err := os.RemoveAll(copyDir); err != nil {
		return err
	}
	if err := private.Dir(copyDir); err != nil {
		return err
	}
	live := filepath.Join(dir, databaseName)
	fp, err := updateFingerprint(ctx, dir, live)
	if err != nil {
		return fmt.Errorf("storage: reading the database before the update: %w", err)
	}
	copyPath := filepath.Join(copyDir, databaseName)
	db, err := sql.Open("sqlite", live+"?"+url.Values{"_pragma": {"busy_timeout(5000)", "trusted_schema(0)"}}.Encode())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(ctx, "VACUUM INTO ?", copyPath)
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("storage: copying the database before the update: %w", err)
	}
	if err := selfContained(ctx, copyPath); err != nil {
		return err
	}
	if err := VerifyDatabaseFile(ctx, copyPath, -1); err != nil {
		return fmt.Errorf("storage: the database copy before the update does not verify: %w", err)
	}
	rec := &updateRecord{Version: 1, Step: UpdateSaved, Fingerprint: fp, SavedAt: time.Now().UnixMilli()}
	if st, err := private.ReadFile(filepath.Join(dir, stateName)); err == nil {
		rec.State = st
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeUpdateRecord(dir, rec); err != nil {
		return err
	}
	return syncDir(dir)
}

// BeginRollback starts putting the update copy back in a stopped server's
// data directory: the copy must be there and verify, and the server must
// have accepted nothing since it was saved (otherwise ErrUpdateAccepted, and
// nothing changes). From then on no server opens the database until
// BinaryRestored. Repeating it once begun does nothing more.
func BeginRollback(ctx context.Context, dataDir string) error {
	dir, lock, err := lockForUpdate(dataDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	rec, err := readUpdateRecord(dir)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("%w: %s has no record", ErrNoUpdateCopy, UpdateCopyDir(dir))
	}
	if rec.Step != UpdateSaved {
		return nil
	}
	copyPath := filepath.Join(dir, updateCopyDir, databaseName)
	if err := existingFile(copyPath); err != nil {
		return fmt.Errorf("%w: %v", ErrNoUpdateCopy, err)
	}
	if err := VerifyDatabaseFile(ctx, copyPath, -1); err != nil {
		return fmt.Errorf("%w: it does not verify: %v", ErrNoUpdateCopy, err)
	}
	fp, err := updateFingerprint(ctx, dir, filepath.Join(dir, databaseName))
	if err != nil {
		return fmt.Errorf("storage: reading the database after the update: %w", err)
	}
	if fp != rec.Fingerprint {
		return ErrUpdateAccepted
	}
	rec.Step, rec.FailedName = UpdateRollingBack, updateFailedPfx+time.Now().UTC().Format("20060102-150405")
	return writeUpdateRecord(dir, rec)
}

// BinaryRestored records that the previous binary is back: from then on the
// copy may go into place, and a server start finishes it.
func BinaryRestored(dataDir string) error {
	dir, lock, err := lockForUpdate(dataDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	rec, err := readUpdateRecord(dir)
	if err != nil {
		return err
	}
	if rec == nil || rec.Step == UpdateSaved {
		return fmt.Errorf("storage: no rollback has begun in %s", dir)
	}
	if rec.Step != UpdateRollingBack {
		return nil
	}
	rec.Step = UpdateBinaryRestored
	return writeUpdateRecord(dir, rec)
}

// FinishRollback puts the copy in place, once the previous binary is back:
// the failed version's database is kept as nottennapod.db.update-failed-<UTC>
// and the metadata goes back with the copy. The copy itself stays until
// DiscardUpdateCopy. Repeating it gives the same result.
func FinishRollback(ctx context.Context, dataDir string) error {
	dir, lock, err := lockForUpdate(dataDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	rec, err := readUpdateRecord(dir)
	if err != nil {
		return err
	}
	if rec == nil || rec.Step == UpdateSaved || rec.Step == UpdateRollingBack {
		return fmt.Errorf("storage: the previous binary is not back yet in the rollback in %s", dir)
	}
	return finishRollback(ctx, dir, rec)
}

// finishRollback is FinishRollback with the data directory locked.
func finishRollback(ctx context.Context, dir string, rec *updateRecord) error {
	if rec.Step == UpdateDBRestored {
		return nil
	}
	// A temporary copy of the copy, verified, so the copy stays the input.
	copyPath := filepath.Join(dir, updateCopyDir, databaseName)
	temp := filepath.Join(dir, updateTempName)
	if err := os.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := existingFile(copyPath); err != nil {
		return fmt.Errorf("%w: %v", ErrNoUpdateCopy, err)
	}
	if err := copyFile(copyPath, temp); err != nil {
		return fmt.Errorf("storage: copying the update copy: %w", err)
	}
	if err := VerifyDatabaseFile(ctx, temp, -1); err != nil {
		return fmt.Errorf("%w: it does not verify: %v", ErrNoUpdateCopy, err)
	}
	if err := updateFault("rollback-copied"); err != nil {
		return err
	}
	// The failed version's database moves aside once: when its name is
	// taken, the swap already happened.
	live, failed := filepath.Join(dir, databaseName), filepath.Join(dir, rec.FailedName)
	if _, err := os.Lstat(failed); errors.Is(err, os.ErrNotExist) {
		for _, sfx := range []string{"-wal", "-shm", ""} {
			if err := os.Rename(live+sfx, failed+sfx); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := updateFault("rollback-moved" + sfx); err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}
	if err := os.Rename(temp, live); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	if err := updateFault("rollback-placed"); err != nil {
		return err
	}
	// The metadata goes back with the database, naming the file now in place.
	statePath := filepath.Join(dir, stateName)
	if rec.State == nil {
		if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		var st restoreState
		if err := json.Unmarshal(rec.State, &st); err != nil {
			return fmt.Errorf("%w: %v", ErrStateDamaged, err)
		}
		if st.DBFile != nil {
			id, err := identityOf(live)
			if err != nil {
				return err
			}
			st.DBFile = &id
		}
		out, err := json.MarshalIndent(&st, "", "  ")
		if err != nil {
			return err
		}
		if err := keys.WriteFile(statePath, append(out, '\n')); err != nil {
			return err
		}
	}
	if err := updateFault("rollback-metadata"); err != nil {
		return err
	}
	rec.Step = UpdateDBRestored
	return writeUpdateRecord(dir, rec)
}

// resumeUpdate runs at a server's start, with the data directory locked: a
// rollback waiting for the previous binary stops the start, and one whose
// binary is back puts the copy in place first.
func resumeUpdate(ctx context.Context, dir string) error {
	rec, err := readUpdateRecord(dir)
	if err != nil || rec == nil {
		return err
	}
	switch rec.Step {
	case UpdateRollingBack:
		return ErrUpdateRollback
	case UpdateBinaryRestored:
		return finishRollback(ctx, dir, rec)
	}
	return nil
}

// DiscardUpdateCopy removes the update copy once the update is resolved.
func DiscardUpdateCopy(dataDir string) error {
	return os.RemoveAll(filepath.Join(dataDir, updateCopyDir))
}

// SetAsideUpdateCopy moves the update copy to dataDir/name, kept: a kept
// update's copy that can no longer undo the changes the new version accepted.
// It does nothing when the copy is already gone and refuses to replace anything
// at name.
func SetAsideUpdateCopy(dataDir, name string) error {
	from, to := filepath.Join(dataDir, updateCopyDir), filepath.Join(dataDir, name)
	if _, err := os.Lstat(from); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if _, err := os.Lstat(to); err == nil {
		return fmt.Errorf("storage: %s is in the way of the kept update copy", to)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return syncDir(dataDir)
}

// ---- the pending-update marker

// updatePendingName marks a data directory whose service update is not
// resolved, from its first step to the end of its cleanup: restore refuses
// while it is there.
const updatePendingName = "update-pending"

// MarkUpdatePending writes the marker, naming the installation directory.
func MarkUpdatePending(dataDir, installDir string) error {
	data, err := json.Marshal(map[string]any{"installation": installDir, "since": time.Now().UnixMilli()})
	if err != nil {
		return err
	}
	return keys.WriteFile(filepath.Join(dataDir, updatePendingName), append(data, '\n'))
}

// UpdatePending reports the marker.
func UpdatePending(dataDir string) (bool, error) {
	_, err := os.Lstat(filepath.Join(dataDir, updatePendingName))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// ClearUpdatePending removes the marker.
func ClearUpdatePending(dataDir string) error {
	if err := os.Remove(filepath.Join(dataDir, updatePendingName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(dataDir)
}

// updateFault is a test hook: an error simulates a crash at point.
var updateFault = func(point string) error { return nil }

// existingFile checks a server file that must be there.
func existingFile(path string) error {
	if _, err := os.Lstat(path); err != nil {
		return err
	}
	return private.File(path)
}

func lockForUpdate(dataDir string) (string, *os.File, error) {
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return "", nil, err
	}
	if err := private.Dir(dir); err != nil {
		return "", nil, fmt.Errorf("storage: data directory: %w", err)
	}
	lock, err := lockDirectory(dir)
	if err != nil {
		return "", nil, err
	}
	return dir, lock, nil
}

// updateFingerprint hashes what a server that accepted anything would have
// changed: every account log's head (pushes and the server's own commits),
// the accounts and logins, the auth revision (sign-outs and revocations) and
// the metadata's counters.
func updateFingerprint(ctx context.Context, dir, path string) (string, error) {
	// A missing database is no database: opening it would create one.
	if err := existingFile(path); err != nil {
		return "", err
	}
	// Read-write but query-only: the server is stopped and the lock is held,
	// and a WAL the server left needs recovering to be read.
	db, err := sql.Open("sqlite", path+"?"+url.Values{"_pragma": {"busy_timeout(5000)", "trusted_schema(0)", "query_only(1)"}}.Encode())
	if err != nil {
		return "", err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	h := sha256.New()
	for _, q := range []string{
		`SELECT account_id, epoch, head_seq, hex(head_digest) FROM account_logs ORDER BY account_id`,
		`SELECT account_id, state FROM accounts ORDER BY account_id`,
		`SELECT account_id, username, password_hash, credentials_version, is_admin FROM account_logins ORDER BY account_id`,
		`SELECT key, value FROM server_meta WHERE key IN ('server_id', 'auth_revision') ORDER BY key`,
	} {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return "", err
		}
		cols, _ := rows.Columns()
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		fmt.Fprintf(h, "%s\x00", q)
		for rows.Next() {
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return "", err
			}
			for _, v := range vals {
				fmt.Fprintf(h, "%d:%s\x00", len(v.String), v.String)
			}
			h.Write([]byte{'\n'})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return "", err
		}
	}
	if data, err := private.ReadFile(filepath.Join(dir, stateName)); err == nil {
		var st restoreState
		if err := json.Unmarshal(data, &st); err != nil {
			return "", fmt.Errorf("%w: %v", ErrStateDamaged, err)
		}
		fmt.Fprintf(h, "state %d %d %d %d\n", st.EpochHighWater, st.AuthRevision, st.PendingAuthRevision, st.KeysExportedAt)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ---- the instance ID

// WriteInstance gives this run of the server a random instance ID, written
// to server.instance (0600) in the data directory and reported by /v1/info.
// service update's readiness check needs both to match, so another process
// answering on the server's port cannot pass for it.
func (s *Store) WriteInstance() (string, error) {
	id, err := NewInstance(s.dir)
	if err != nil {
		return "", err
	}
	s.instance.Store(id)
	return id, nil
}

// NewInstance writes a new random instance ID to dataDir's server.instance.
func NewInstance(dataDir string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b[:])
	if err := keys.WriteFile(filepath.Join(dataDir, instanceName), []byte(id+"\n")); err != nil {
		return "", fmt.Errorf("storage: writing %s: %w", instanceName, err)
	}
	return id, nil
}

// InstanceID is the ID WriteInstance gave this run ("" if none).
func (s *Store) InstanceID() string {
	id, _ := s.instance.Load().(string)
	return id
}

// ReadInstance reads the instance ID the server in dataDir wrote at its
// last start.
func ReadInstance(dataDir string) (string, error) {
	data, err := private.ReadFile(filepath.Join(dataDir, instanceName))
	if err != nil {
		return "", err
	}
	if len(data) != 33 || data[32] != '\n' {
		return "", fmt.Errorf("storage: %s is damaged", instanceName)
	}
	return string(data[:32]), nil
}

// ClearInstance removes the instance ID, before a start that must write a
// new one.
func ClearInstance(dataDir string) error {
	err := os.Remove(filepath.Join(dataDir, instanceName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
