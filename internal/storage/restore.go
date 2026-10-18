package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/keys"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

// The restore procedure (PROTOCOL §11, with PROTOCOL §11.4 for logins and PROTOCOL §11.4 for the
// auth revision and imports). It runs with the server stopped and holds the data-directory lock.
// Verification changes nothing; from then on each step is recorded in restore-state.json before its
// effects, and everything a step depends on is fixed in the record, so a crash at any point resumes
// or repeats the step with the same database, metadata and report. Open finishes an interrupted
// restore before serving.

const (
	stagedName       = databaseName + ".restoring"
	stagedSourceName = stagedName + ".src" // the backup's bytes, copied for staging
	preRestorePrefix = databaseName + ".pre-restore-"
	// restoreTempPrefix names the temporaries of a restore's verification.
	restoreTempPrefix = ".nottennapod-restore-"
)

// Recorded steps.
const (
	stepStage      = "stage"      // copy the backup to the staged file and migrate it
	stepTransition = "transition" // the new epoch is persisted; transition the staged copy
	stepSwap       = "swap"       // keep the old database aside and put the staged one in place
	stepDone       = "done"       // finished; the report waits to be shown
)

// ErrRestoreInProgress reports another backup's restore that has already
// replaced the database and must be finished first.
var ErrRestoreInProgress = errors.New("storage: another restore is finishing")

// RestoreOptions are the restore command's inputs.
type RestoreOptions struct {
	DataDir    string
	Backup     string // the backup file
	Bundle     []byte // the key bundle file's contents
	Passphrase []byte
	Out        io.Writer // receives the report before the restore record is cleared; may be nil
}

// RestoreReport is what a restore did (PROTOCOL §11.4). Accounts are named by username
// (by account ID when they have no login).
type RestoreReport struct {
	Backup     string `json:"backup"`
	Epoch      int64  `json:"epoch"`
	PreRestore string `json:"pre_restore,omitempty"` // file name the old database is kept as
	// LoginsFrom says where logins came from and, for "backup", why.
	LoginsFrom       string   `json:"logins_from"`
	Restored         []string `json:"restored"`
	NotRestored      []string `json:"not_restored"`      // created after the backup
	ReturnedDisabled []string `json:"returned_disabled"` // deleted after the backup
	KeptActive       []string `json:"kept_active"`       // returning administrators kept active: no other active administrator
	Renamed          []string `json:"renamed"`           // "old → new"
	LoginsCarried    []string `json:"logins_carried"`
	LoginsKept       []string `json:"logins_kept"`
}

// String is the report as the restore command prints it.
func (r *RestoreReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Restored the database from %s.\n", r.Backup)
	fmt.Fprintf(&b, "Every account is now at epoch %d.\n", r.Epoch)
	if r.PreRestore != "" {
		fmt.Fprintf(&b, "The previous database is kept as %s (it is never deleted automatically).\n", r.PreRestore)
	}
	list := func(title string, names []string) {
		if len(names) > 0 {
			fmt.Fprintf(&b, "%s (%d): %s\n", title, len(names), strings.Join(names, ", "))
		}
	}
	list("Accounts restored", r.Restored)
	list("Accounts created after the backup, not restored", r.NotRestored)
	list("Accounts deleted after the backup, returned disabled", r.ReturnedDisabled)
	list("Accounts deleted after the backup, kept active because no other administrator is active", r.KeptActive)
	list("Renamed because the username is in use", r.Renamed)
	list("Logins carried from the previous database", r.LoginsCarried)
	list("Logins kept from the backup", r.LoginsKept)
	fmt.Fprintf(&b, "Logins: %s.\n", r.LoginsFrom)
	b.WriteString("Every device and browser must sign in again; devices then return what the backup lacks.\n")
	return b.String()
}

// Restore runs the restore procedure on a stopped server's data directory. An
// interrupted restore of the same backup is resumed.
func Restore(ctx context.Context, opts RestoreOptions) (*RestoreReport, error) {
	return restore(ctx, config{dataDir: opts.DataDir}, opts)
}

type restorer struct {
	ctx       context.Context
	dir       string
	cfg       config
	m         *metaFile
	staged    bool // the staged copy was made by this run
	prestaged bool // the staged file is the verified bytes, copied before the restore was recorded
}

func restore(ctx context.Context, cfg config, opts RestoreOptions) (*RestoreReport, error) {
	cfg.restoreDefaults()
	dir, err := filepath.Abs(cfg.dataDir)
	if err != nil {
		return nil, err
	}
	backup, err := filepath.Abs(opts.Backup)
	if err != nil {
		return nil, err
	}
	// A new data directory (missing or empty) is left exactly as it is until
	// the backup and the key bundle verify: a refused restore creates nothing
	// there, not even the folder or its lock, so it is still new afterwards.
	// The verification reads nothing of an empty folder, so it holds once the
	// folder is locked if the lock is still all it holds. (A compressed
	// backup is verified in a copy in the folder, made for it and taken back
	// with the copy on a refusal.)
	var early *verifiedBackup
	var made *newFolder
	if newDataDir(dir) {
		made = noteNewFolder(dir)
		pre := &restorer{ctx: ctx, dir: dir, cfg: cfg, m: &metaFile{dir: dir, hooks: &cfg.hooks}}
		if early, err = pre.verifyBackup(backup, opts); err != nil {
			made.undo()
			return nil, err
		}
		if h := cfg.hooks.restoreFault; h != nil {
			if err := h("verified-before-lock"); err != nil {
				made.undo()
				return nil, err
			}
		}
	}
	if err := private.Dir(dir); err != nil {
		return nil, fmt.Errorf("storage: data directory: %w", err)
	}
	lock, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	// What an interrupted verification left.
	removeTemps(dir, restoreTempPrefix)
	// An unfinished service update owns the database until it is resolved:
	// its marker is there from its first step to the end of its cleanup,
	// and its copy's record while the copy is kept.
	pending, err := UpdatePending(dir)
	if err != nil {
		return nil, err
	}
	if rec, err := readUpdateRecord(dir); err != nil {
		return nil, err
	} else if rec != nil || pending {
		return nil, fmt.Errorf("%w: an update is unfinished; run `nottennapod-server service update` to finish it first", ErrRestoreRefused)
	}
	m, err := loadMeta(dir, &cfg.hooks)
	if err != nil {
		return nil, err
	}
	// Staged files with no restore recorded (a run stopped between copying
	// the backup and recording itself) are this procedure's own leftovers, as
	// start-up treats them: they go before anything is verified or counted,
	// so they never stop the restore that would remove them.
	if st := m.state(); st == nil || st.Restore == nil {
		if err := removeStagedCopy(dir); err != nil {
			return nil, err
		}
	}
	r := &restorer{ctx: ctx, dir: dir, cfg: cfg, m: m}
	if st := m.state(); st != nil && st.Restore != nil {
		p := st.Restore
		switch {
		case p.Backup == backup:
			return r.finish(opts.Out)
		case p.Step == stepSwap || p.Step == stepDone:
			return nil, fmt.Errorf("%w: the restore from %s has replaced the database; run restore again with --from %s to finish it",
				ErrRestoreInProgress, p.Backup, p.Backup)
		}
		// Another backup's restore stopped before its swap: the live database
		// is untouched, so this restore starts over.
	}
	v := early
	if v == nil || !onlyLock(dir) {
		if v, err = r.verifyBackup(backup, opts); err != nil {
			made.undo()
			return nil, err
		}
	}
	bundle, serverID, digest := v.bundle, v.serverID, v.digest
	if h := cfg.hooks.restoreFault; h != nil {
		if err := h("verified"); err != nil {
			return nil, err
		}
	}
	// The verified bytes are the ones staged: the copy is made now, before
	// anything is written for the restore, from the file verified, and
	// checked against what was verified. A backup changed or replaced since
	// is refused with the data directory as it was (a new one not even
	// created); afterwards the backup is not read again by this run.
	if err := r.prestage(backup, v); err != nil {
		made.undo()
		return nil, err
	}
	if err := r.installKeys(bundle); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	pre, err := preRestoreName(dir, now)
	if err != nil {
		return nil, err
	}
	// The bundle this restore opened is an export of these keys (PROTOCOL
	// §11.1): the service page and the start log stop asking for one.
	exported := bundle.CreatedAt
	if exported <= 0 {
		exported = now.UnixMilli()
	}
	if err := m.update("restore-"+stepStage, func(st *restoreState) {
		if st.ServerID == "" {
			st.ServerID = serverID
		}
		st.KeysExportedAt = max(st.KeysExportedAt, exported)
		st.Restore = &restoreProgress{Step: stepStage, Backup: backup, BackupSHA256: digest, StartedAt: now.UnixMilli(), Incarnation: newUUID(),
			PreRestore: pre}
	}); err != nil {
		return nil, err
	}
	return r.finish(opts.Out)
}

// verifiedBackup is what verification established.
type verifiedBackup struct {
	bundle   keys.Bundle
	serverID string
	digest   string       // the verified bytes' SHA-256
	identity fileIdentity // the verified file
}

// verifyBackup verifies the backup and the bundle (verify) and binds the
// restore to the file and the bytes it verified: the staged copy is checked
// against both.
func (r *restorer) verifyBackup(backup string, opts RestoreOptions) (*verifiedBackup, error) {
	id, err := identityOf(backup)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read the backup %s: %v", ErrRestoreRefused, backup, err)
	}
	digest, err := fileDigest(backup)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read the backup %s: %v", ErrRestoreRefused, backup, err)
	}
	bundle, serverID, err := r.verify(backup, opts, digest)
	if err != nil {
		return nil, err
	}
	if after, err := fileDigest(backup); err != nil || after != digest {
		return nil, fmt.Errorf("%w: the backup changed while it was being verified", ErrRestoreRefused)
	}
	if after, err := identityOf(backup); err != nil || after != id {
		return nil, fmt.Errorf("%w: the backup was replaced while it was being verified", ErrRestoreRefused)
	}
	return &verifiedBackup{bundle: bundle, serverID: serverID, digest: digest, identity: id}, nil
}

// prestage copies the backup to the staged source from the file that was
// verified and checks that the copy is the bytes verified; on a refusal it
// leaves no copy. This is the run's one copy of the backup: stage uses it.
func (r *restorer) prestage(backup string, v *verifiedBackup) (err error) {
	refuse := fmt.Errorf("%w: the backup %s is not the file that was verified: it changed or was replaced since; run restore again",
		ErrRestoreRefused, backup)
	if err := removeStagedCopy(r.dir); err != nil {
		return err
	}
	in, err := os.Open(backup)
	if err != nil {
		return fmt.Errorf("%w: cannot read the backup %s: %v", ErrRestoreRefused, backup, err)
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); !ok || (fileIdentity{Dev: uint64(st.Dev), Ino: uint64(st.Ino)}) != v.identity {
		return refuse
	}
	if err := r.needFree(fi.Size(), 0, "copy the backup into it"); err != nil {
		return fmt.Errorf("%w: %w", ErrRestoreRefused, err)
	}
	defer func() {
		if err != nil {
			_ = removeStagedCopy(r.dir)
		}
	}()
	sum, err := r.copySource(in)
	if errors.Is(err, ErrNoSpace) {
		return fmt.Errorf("%w: %w", ErrRestoreRefused, err)
	}
	if errors.Is(err, errTooLong) {
		return refuse
	}
	if err != nil {
		return fmt.Errorf("storage: copying the backup %s: %w", backup, err)
	}
	if sum != v.digest {
		return refuse
	}
	r.prestaged = true
	return nil
}

// copySource copies in to the staged source, created exclusively (0600) and
// made durable; it returns the copy's SHA-256.
func (r *restorer) copySource(in *os.File) (string, error) {
	out, err := private.OpenFile(r.path(stagedSourceName), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	fi, err := in.Stat()
	if err != nil {
		out.Close()
		return "", err
	}
	h := sha256.New()
	_, err = copyGuarded(r.ctx, io.MultiWriter(out, h), io.NewSectionReader(in, 0, 1<<62), r.spaceGuard, fi.Size())
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), r.cfg.syncDir(r.dir)
}

// removeStagedCopy removes the staged file, anything SQLite left beside it,
// and the staged source.
func removeStagedCopy(dir string) error {
	staged := filepath.Join(dir, stagedName)
	for _, sfx := range []string{"", "-wal", "-shm", "-journal", ".src"} {
		if err := os.Remove(staged + sfx); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// newFolder is what a restore into a new data directory makes before
// anything is recorded (the folder, its missing parents, its lock), so a
// refusal then leaves it as it was.
type newFolder struct {
	dir     string
	existed bool        // an empty folder: its mode is put back
	mode    os.FileMode //
	created []string    // the folders made, deepest first
}

func noteNewFolder(dir string) *newFolder {
	f := &newFolder{dir: dir}
	if fi, err := os.Stat(dir); err == nil {
		f.existed, f.mode = true, fi.Mode().Perm()
		return f
	}
	for d := dir; ; {
		if _, err := os.Lstat(d); err == nil {
			break
		}
		f.created = append(f.created, d)
		parent := filepath.Dir(d)
		if parent == d {
			break
		}
		d = parent
	}
	return f
}

// undo puts the folder back as long as it holds nothing but the restore's
// lock, or nothing: anything else there is not this restore's to remove.
func (f *newFolder) undo() {
	if f == nil || !(onlyLock(f.dir) || isEmptyDir(f.dir)) {
		return
	}
	_ = os.Remove(filepath.Join(f.dir, lockName))
	if f.existed {
		_ = os.Chmod(f.dir, f.mode)
		return
	}
	for _, d := range f.created {
		_ = os.Remove(d) // only an empty folder goes
	}
}

// newDataDir reports a data directory that does not exist yet or is empty.
func newDataDir(dir string) bool {
	entries, err := os.ReadDir(dir)
	return errors.Is(err, os.ErrNotExist) || (err == nil && len(entries) == 0)
}

// isEmptyDir reports an existing folder that holds nothing.
func isEmptyDir(dir string) bool {
	entries, err := os.ReadDir(dir)
	return err == nil && len(entries) == 0
}

// onlyLock reports a data directory that holds nothing but its lock file.
func onlyLock(dir string) bool {
	entries, err := os.ReadDir(dir)
	return err == nil && len(entries) == 1 && entries[0].Name() == lockName
}

// resumeRestore finishes an interrupted restore at startup (PROTOCOL §11.4); it needs the
// backup file only if the database has not been replaced yet.
func resumeRestore(ctx context.Context, cfg config, dir string, m *metaFile) error {
	cfg.restoreDefaults()
	p := m.state().Restore
	slog.Warn("finishing an interrupted restore", "backup", p.Backup, "step", p.Step)
	r := &restorer{ctx: ctx, dir: dir, cfg: cfg, m: m}
	if err := r.run(); err != nil {
		return fmt.Errorf("storage: finishing the interrupted restore from %s: %w", p.Backup, err)
	}
	rep := m.state().Restore.Report
	slog.Warn("restore finished", "report", rep.String())
	return m.update("restore-clear", func(st *restoreState) { st.Restore = nil })
}

// finish runs the remaining steps, shows the report and clears the record.
func (r *restorer) finish(out io.Writer) (*RestoreReport, error) {
	if err := r.run(); err != nil {
		return nil, err
	}
	rep := r.m.state().Restore.Report
	if out != nil {
		fmt.Fprint(out, rep.String())
	}
	if err := r.m.update("restore-clear", func(st *restoreState) { st.Restore = nil }); err != nil {
		return rep, err
	}
	return rep, nil
}

func (r *restorer) fault(point string) error {
	if h := r.cfg.hooks.restoreFault; h != nil {
		if err := h(point); err != nil {
			r.m.crashed.Store(true)
			return fmt.Errorf("%w at %s: %w", errCrashed, point, err)
		}
	}
	return nil
}

func (r *restorer) path(name string) string { return filepath.Join(r.dir, name) }

// run performs the recorded step and every later one.
func (r *restorer) run() error {
	for {
		st := r.m.state()
		p := st.Restore
		switch p.Step {
		case stepStage:
			staged, err := r.stage(p)
			if err != nil {
				return err
			}
			live := inspectLive(r.ctx, r.path(databaseName))
			// PROTOCOL §11.4: E is above the high-water mark and every epoch of
			// the backup and the live database, and is persisted before any
			// database write; the auth revision rises above every value known.
			epoch := max(st.EpochHighWater, staged.maxEpoch, live.facts.maxEpoch) + 1
			rev := max(staged.authRevision, live.facts.authRevision, st.AuthRevision, st.PendingAuthRevision) + 1
			if err := r.m.update("restore-"+stepTransition, func(st *restoreState) {
				st.EpochHighWater = max(st.EpochHighWater, epoch)
				st.Restore.Step, st.Restore.Epoch, st.Restore.AuthRevision = stepTransition, epoch, rev
			}); err != nil {
				return err
			}
		case stepTransition:
			if !r.staged {
				// A resumed run starts again from the backup, so the staged copy
				// is transitioned exactly once.
				if _, err := r.stage(p); err != nil {
					return err
				}
			}
			rep, err := r.transition(p, st.ServerID)
			if err != nil {
				return err
			}
			if err := r.m.update("restore-"+stepSwap, func(st *restoreState) {
				st.Restore.Step, st.Restore.Report = stepSwap, rep
			}); err != nil {
				return err
			}
		case stepSwap:
			if err := r.swap(p); err != nil {
				return err
			}
			id, err := identityOf(r.path(databaseName))
			if err != nil {
				return err
			}
			if err := r.m.update("restore-"+stepDone, func(st *restoreState) {
				st.Incarnation, st.NextIncarnation, st.DBFile = p.Incarnation, "", &id
				st.EpochHighWater = max(st.EpochHighWater, p.Epoch)
				st.AuthRevision = max(st.AuthRevision, p.AuthRevision)
				if st.PendingAuthRevision <= p.AuthRevision {
					st.PendingAuthRevision = 0
				}
				st.Restore.Step = stepDone
			}); err != nil {
				return err
			}
		case stepDone:
			return nil
		default:
			return fmt.Errorf("%w: unknown restore step %q", ErrStateDamaged, p.Step)
		}
	}
}

// ---- verification (PROTOCOL §11.4): reads only

// verify checks the backup and the key bundle without changing anything and
// returns the bundle and the backup's server ID. digest is the backup's
// SHA-256, taken before. A compressed backup is checked in two temporaries in
// the data folder (a copy of it, then its database), which it removes.
func (r *restorer) verify(backup string, opts RestoreOptions, digest string) (_ keys.Bundle, _ string, err error) {
	refuse := func(format string, args ...any) (keys.Bundle, string, error) {
		return keys.Bundle{}, "", fmt.Errorf("%w: "+format, append([]any{ErrRestoreRefused}, args...)...)
	}
	ctx := r.ctx
	if fi, err := os.Stat(backup); err != nil || !fi.Mode().IsRegular() {
		return refuse("cannot read the backup %s: %v", backup, err)
	}
	if hasSidecars(backup) {
		return refuse("the backup %s has SQLite journal files beside it; a backup is one self-contained file", backup)
	}
	bundle, err := keys.Open(opts.Bundle, opts.Passphrase)
	switch {
	case errors.Is(err, keys.ErrPassphrase):
		return refuse("the passphrase is wrong or the key bundle is damaged")
	case err != nil:
		return refuse("the key bundle cannot be read: %v", err)
	}
	// The MAC is checked before anything else is read from the file: before
	// it, only the trailer's fixed framing.
	file, err := os.Open(backup)
	if err != nil {
		return refuse("cannot read the backup %s: %v", backup, err)
	}
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		return refuse("cannot read the backup %s: %v", backup, err)
	}
	size := fi.Size()
	var v1 backupTrailer
	var v2 trailerV2
	var serverID string
	format := backupFormat(file, size)
	switch format {
	case 1:
		v1, err = readBackupTrailer(file, size)
		serverID = v1.ServerID
	case 2:
		v2, err = readTrailerV2(file, size)
		serverID = v2.ServerID
	default:
		err = errUnsigned
	}
	switch {
	case errors.Is(err, errUnsigned):
		return refuse("the backup %s is unsigned (%v): backups made before October 2026 cannot be restored; take a new backup with this server", backup, err)
	case err != nil:
		return refuse("cannot read the backup %s: %v", backup, err)
	}
	if bundle.ServerID != serverID {
		return refuse("the key bundle belongs to server %s, the backup to server %s", bundle.ServerID, serverID)
	}
	if st := r.m.state(); st != nil && st.ServerID != serverID {
		return refuse("the backup belongs to server %s, this data directory to server %s", serverID, st.ServerID)
	}
	var bundleKeys []ReceiptKey
	keyByID := map[string][]byte{}
	for _, k := range bundle.ReceiptKeys {
		if len(k.Key) != 32 {
			return refuse("the key bundle's receipt key %s is not 32 bytes", k.ID)
		}
		bundleKeys = append(bundleKeys, ReceiptKey{ID: k.ID, Secret: k.Key})
		keyByID[k.ID] = k.Key
	}
	// The data directory's keys, when present, must be the bundle's.
	fileServer, fileKeys, exists, err := readReceiptKeysFile(r.dir)
	if err != nil {
		return refuse("%v", err)
	}
	if exists && (fileServer != serverID || !sameKeys(fileKeys, bundleKeys)) {
		return refuse("the key bundle's receipt keys differ from this data directory's %s", receiptKeysName)
	}
	if secret, err := private.ReadFile(r.path(keysSecretName)); err == nil && !bytes.Equal(secret, bundle.SecretsKey) {
		return refuse("the key bundle's secrets key differs from this data directory's %s", keysSecretName)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return refuse("cannot read %s: %v", keysSecretName, err)
	}
	dbPath, dbSize, temps := backup, size, int64(0)
	if format == 1 {
		if err := checkBackupMAC(ctx, file, v1, keyByID); err != nil {
			return refuse("%v", err)
		}
		dbSize = v1.Size
	} else {
		var done func()
		dbPath, done, err = r.verifyCompressed(file, size, v2, keyByID, digest)
		defer done()
		if err != nil {
			return keys.Bundle{}, "", err
		}
		dbSize, temps = v2.DatabaseSize, size+v2.DatabaseSize
	}
	// Second lines behind the MAC: the database checks (integrity, foreign
	// keys, the exact schema of its version), then every sealed value and
	// signed object.
	if err := VerifyDatabaseFile(ctx, dbPath, -1); err != nil {
		return refuse("the backup does not verify: %v", err)
	}
	db, err := openReadOnly(dbPath)
	if err != nil {
		return refuse("cannot open the backup: %v", err)
	}
	defer db.Close()
	f, err := readDBFacts(ctx, db)
	if err != nil || f.serverID != serverID {
		return refuse("the backup's server ID is not the one it is signed for: %v", err)
	}
	// A backup from before the key bundle (PROTOCOL §11.1) still holds its receipt keys.
	inBackup, err := receiptKeyRows(ctx, db)
	if err != nil {
		return refuse("cannot read the backup's receipt keys: %v", err)
	}
	if len(inBackup) > 0 && !sameKeys(inBackup, bundleKeys) {
		return refuse("the backup holds receipt keys that differ from the key bundle's")
	}
	if err := verifySealed(ctx, db, bundle.SecretsKey); err != nil {
		return refuse("%v", err)
	}
	if err := verifyMACs(ctx, db, keyByID); err != nil {
		return refuse("%v", err)
	}
	// The whole restore must fit (PROTOCOL §11.4), counted from the backup's
	// own sizes: its copy, its database and what SQLite writes updating it,
	// beside the database it replaces, which is kept. The temporaries of
	// this check are gone by then.
	version, err := schemaVersionOf(ctx, db)
	if err != nil {
		return refuse("cannot read the backup's schema version: %v", err)
	}
	if err := r.needFree(restoreNeed(format, size, dbSize, version), temps, "restore this backup"); err != nil {
		return keys.Bundle{}, "", fmt.Errorf("%w: %w", ErrRestoreRefused, err)
	}
	return bundle, f.serverID, nil
}

// restoreNeed is the most a restore of a backup of size bytes (holding a
// database of dbSize bytes at schema version) adds to the data folder at
// once: the copy it stages, the database it decompresses from it, and
// SQLite's writes while it updates the database.
func restoreNeed(format int, size, dbSize int64, version int) int64 {
	sqlite := dbSize + sqliteAllowance(dbSize, version < latestSchema())
	if format == 2 {
		return max(size+dbSize, sqlite)
	}
	return max(size, sqlite)
}

func latestSchema() int { return embeddedMigrations[len(embeddedMigrations)-1].version }

func schemaVersionOf(ctx context.Context, q rowQuerier) (int, error) {
	var v int
	err := q.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&v)
	return v, err
}

// verifyCompressed checks a compressed backup (PROTOCOL §11.2) in the data
// folder: a copy of its bytes, whose digest must be digest; the trailer and
// the MAC of that copy with the bundle's keys; then the copy's database,
// decoded into a second temporary, whose size and SHA-256 must be the signed
// ones. It returns that database's path and a function removing both.
func (r *restorer) verifyCompressed(file *os.File, size int64, t trailerV2, keyByID map[string][]byte, digest string) (string, func(), error) {
	refuse := func(format string, args ...any) error {
		for _, a := range args {
			if err, ok := a.(error); ok && errors.Is(err, ErrNoSpace) {
				return fmt.Errorf("%w: %w", ErrRestoreRefused, err)
			}
		}
		return fmt.Errorf("%w: "+format, append([]any{ErrRestoreRefused}, args...)...)
	}
	crashed := false
	var temps []string
	done := func() {
		if !crashed {
			for _, p := range temps {
				os.Remove(p)
			}
		}
	}
	fault := func(point string) error {
		if err := r.fault(point); err != nil {
			crashed = true
			return err
		}
		return nil
	}
	if _, ok := keyByID[t.KeyID]; !ok {
		return "", done, refuse("the backup is signed with receipt key %s, which the key bundle does not hold", t.KeyID)
	}
	if err := private.Dir(r.dir); err != nil {
		return "", done, fmt.Errorf("storage: data directory: %w", err)
	}
	removeTemps(r.dir, restoreTempPrefix)
	if err := r.needFree(size, 0, "check the backup"); err != nil {
		return "", done, refuse("%v", err)
	}
	c, sum, err := copyExclusive(r.ctx, file, r.dir, restoreTempPrefix+"verify-*.gz", r.spaceGuard)
	if errors.Is(err, ErrNoSpace) {
		return "", done, refuse("%v", err)
	} else if errors.Is(err, errTooLong) {
		return "", done, refuse("the backup changed while it was being verified")
	} else if err != nil {
		return "", done, refuse("cannot copy the backup to check it: %v", err)
	}
	defer c.Close()
	temps = append(temps, c.Name())
	if sum != digest {
		return "", done, refuse("the backup changed while it was being verified")
	}
	if err := fault("verify-copied"); err != nil {
		return "", done, err
	}
	ct, err := readTrailerV2(c, size)
	if err != nil || ct != t {
		return "", done, refuse("the backup changed while it was being verified")
	}
	a, err := authenticate(r.ctx, c, ct, keyByID, ct.ServerID)
	if err != nil {
		return "", done, refuse("%v", err)
	}
	if err := fault("verify-authenticated"); err != nil {
		return "", done, err
	}
	if err := r.needFree(ct.DatabaseSize, 0, "check the backup's database"); err != nil {
		return "", done, refuse("%v", err)
	}
	out, err := os.CreateTemp(r.dir, restoreTempPrefix+"verify-*.db")
	if err != nil {
		return "", done, err
	}
	temps = append(temps, out.Name())
	err = decompress(r.ctx, a, out, r.spaceGuard)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", done, refuse("%v", err)
	}
	if err := fault("verify-decompressed"); err != nil {
		return "", done, err
	}
	return out.Name(), done, nil
}

func openReadOnly(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?"+url.Values{"mode": {"ro"}, "_pragma": {"trusted_schema(0)", "query_only(1)"}}.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func hasTable(ctx context.Context, q rowQuerier, name string) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&n)
	return n > 0, err
}

func receiptKeyRows(ctx context.Context, q rowQuerier) ([]ReceiptKey, error) {
	if ok, err := hasTable(ctx, q, "receipt_keys"); err != nil || !ok {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, `SELECT key_id, secret FROM receipt_keys`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReceiptKey
	for rows.Next() {
		var k ReceiptKey
		if err := rows.Scan(&k.ID, &k.Secret); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// sealedTables are the data sealed under secrets.key and the additional data
// each was sealed with: feed credentials (protocol storeSecrets), stored feed
// responses (feed.StoredResponse.Seal), staged push parts (protocol sealPart;
// PROTOCOL §7.2) and recovery upload jobs' requests (protocol recoveryJobAD;
// PROTOCOL §6.11). The formats must stay equal to those writers'.
var sealedTables = []struct {
	table, query string
	keys         int // text columns before the sealed bytes
	ad           func(k []string) []byte
}{
	{"secrets", `SELECT account_id, secret_id, sealed FROM secrets`, 2, func(k []string) []byte {
		return []byte("secret\x00" + k[0] + "\x00" + k[1])
	}},
	{"feed_responses", `SELECT account_id, podcast_id, sealed FROM feed_responses`, 2, func(k []string) []byte {
		return []byte("feed-response\x00" + k[0] + "\x00" + k[1])
	}},
	{"push_parts", `SELECT account_id, install_id, CAST(install_seq AS TEXT), CAST(part_index AS TEXT), data FROM push_parts`, 4, func(k []string) []byte {
		return []byte("part\x00" + k[0] + "\x00" + k[1] + "\x00" + k[2] + "\x00" + k[3])
	}},
	{"recovery_jobs", `SELECT account_id, recovery_id, request_key, request FROM recovery_jobs`, 3, func(k []string) []byte {
		return []byte("recovery-job\x00" + k[0] + "\x00" + k[1] + "\x00" + k[2])
	}},
}

// verifySealed opens every sealed value of the backup with the bundle's
// secrets key.
func verifySealed(ctx context.Context, q rowQuerier, secretsKey []byte) error {
	box, err := sealed.New(secretsKey)
	if err != nil {
		return err
	}
	for _, t := range sealedTables {
		if ok, err := hasTable(ctx, q, t.table); err != nil {
			return err
		} else if !ok {
			continue
		}
		rows, err := q.QueryContext(ctx, t.query)
		if err != nil {
			return err
		}
		for rows.Next() {
			k := make([]string, t.keys)
			var data []byte
			dest := []any{}
			for i := range k {
				dest = append(dest, &k[i])
			}
			if err := rows.Scan(append(dest, &data)...); err != nil {
				rows.Close()
				return err
			}
			if _, err := box.Open(data, t.ad(k)); err != nil {
				rows.Close()
				return fmt.Errorf("a sealed value in %s (account %s) does not open with the key bundle's secrets key", t.table, k[0])
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}

// verifyMACs checks every receipt and decision of the backup against the
// bundle's receipt keys (0002: never discard or fabricate receipts), and
// then the history stored beside them (verifyHistory).
func verifyMACs(ctx context.Context, q rowQuerier, keyByID map[string][]byte) error {
	// A receipt authenticates its fact: the fact's content hash must be the
	// stored one and the signed one, under the same op ID.
	rows, err := q.QueryContext(ctx, `SELECT account_id, op_id, content_hash, fact, receipt FROM facts`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var account, id, hash string
		var fact, receipt []byte
		if err := rows.Scan(&account, &id, &hash, &fact, &receipt); err != nil {
			rows.Close()
			return err
		}
		if !factMatches(id, hash, fact, receipt) {
			rows.Close()
			return fmt.Errorf("the fact %s in account %s does not match its signed receipt", id, account)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, t := range []struct{ what, query string }{
		{"receipt", `SELECT account_id, op_id, receipt FROM facts`},
		{"decision", `SELECT account_id, decision_id, decision FROM decisions`},
	} {
		rows, err := q.QueryContext(ctx, t.query)
		if err != nil {
			return err
		}
		for rows.Next() {
			var account, id string
			var data []byte
			if err := rows.Scan(&account, &id, &data); err != nil {
				rows.Close()
				return err
			}
			if !verifySigned(keyByID, account, data) {
				rows.Close()
				return fmt.Errorf("the %s of %s in account %s does not verify with the key bundle's receipt keys", t.what, id, account)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return verifyHistory(ctx, q, keyByID)
}

// factMatches reports a fact whose canonical content hashes to the stored
// hash, which its receipt signs for the same op ID.
func factMatches(opID, hash string, fact, receipt []byte) bool {
	fv, err := contract.DecodeJSON(fact)
	if err != nil {
		return false
	}
	fo, ok := fv.(map[string]any)
	if !ok {
		return false
	}
	h, err := contract.ContentHash(fo)
	if err != nil || h != hash {
		return false
	}
	rv, err := contract.DecodeJSON(receipt)
	if err != nil {
		return false
	}
	ro, ok := rv.(map[string]any)
	return ok && ro["op_id"] == opID && ro["content_hash"] == hash
}

func verifySigned(keyByID map[string][]byte, accountID string, data []byte) bool {
	v, err := contract.DecodeJSON(data)
	if err != nil {
		return false
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return false
	}
	id, _ := obj["key_id"].(string)
	key, ok := keyByID[id]
	return ok && contract.VerifyMAC(key, accountID, obj)
}

// installKeys puts the bundle's keys into a data directory that lacks them (a
// new machine); present keys were checked equal.
func (r *restorer) installKeys(b keys.Bundle) error {
	if _, _, exists, err := readReceiptKeysFile(r.dir); err != nil {
		return err
	} else if !exists {
		var ks []ReceiptKey
		for _, k := range b.ReceiptKeys {
			ks = append(ks, ReceiptKey{ID: k.ID, Secret: k.Key})
		}
		if err := writeReceiptKeysFile(r.dir, b.ServerID, newestFirst(ks)); err != nil {
			return err
		}
	}
	if _, err := os.Stat(r.path(keysSecretName)); errors.Is(err, os.ErrNotExist) {
		if err := keys.WriteFile(r.path(keysSecretName), b.SecretsKey); err != nil {
			return fmt.Errorf("storage: writing %s: %w", keysSecretName, err)
		}
	} else if err != nil {
		return err
	}
	return nil
}

// preRestoreName picks nottennapod.db.pre-restore-<UTC>, unused.
func preRestoreName(dir string, t time.Time) (string, error) {
	base := preRestorePrefix + t.Format("20060102-150405")
	for n := 1; n < 1000; n++ {
		name := base
		if n > 1 {
			name = fmt.Sprintf("%s-%d", base, n)
		}
		taken := false
		for _, sfx := range []string{"", "-wal", "-shm"} {
			if _, err := os.Lstat(filepath.Join(dir, name+sfx)); err == nil {
				taken = true
			} else if !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
		}
		if !taken {
			return name, nil
		}
	}
	return "", fmt.Errorf("storage: too many databases named %s", base)
}

// ---- staging (PROTOCOL §11.4)

// stage puts the verified backup's database in the staged file (mode 0600),
// checks it with the data directory's keys and migrates it; it returns the
// staged database's facts. The run's copy of the backup (the staged source)
// is the one prestage made; a resumed run makes it again and checks it
// against the verified bytes. A compressed backup's copy is authenticated
// before it is decoded.
func (r *restorer) stage(p *restoreProgress) (dbFacts, error) {
	staged, src := r.path(stagedName), r.path(stagedSourceName)
	refuseChanged := fmt.Errorf("%w: the backup %s is not the file that was verified; run restore again", ErrRestoreRefused, p.Backup)
	if r.prestaged {
		// This run copied the verified bytes before recording the restore.
		r.prestaged = false
		if got, err := fileDigest(src); err != nil || got != p.BackupSHA256 {
			return dbFacts{}, refuseChanged
		}
	} else {
		if err := removeStagedCopy(r.dir); err != nil {
			return dbFacts{}, err
		}
		removeTemps(r.dir, restoreTempPrefix)
		in, err := os.Open(p.Backup)
		if err != nil {
			return dbFacts{}, fmt.Errorf("%w: cannot read the backup %s: %v", ErrRestoreRefused, p.Backup, err)
		}
		fi, err := in.Stat()
		if err == nil {
			err = r.needFree(fi.Size(), 0, "copy the backup into it")
		}
		var sum string
		if err == nil {
			sum, err = r.copySource(in)
		}
		in.Close()
		if err != nil {
			_ = removeStagedCopy(r.dir)
			if errors.Is(err, ErrNoSpace) {
				return dbFacts{}, fmt.Errorf("%w: %w", ErrRestoreRefused, err)
			}
			if errors.Is(err, errTooLong) {
				return dbFacts{}, refuseChanged
			}
			return dbFacts{}, fmt.Errorf("storage: copying the backup %s: %w", p.Backup, err)
		}
		if sum != p.BackupSHA256 {
			return dbFacts{}, refuseChanged
		}
	}
	if err := r.fault("staged-source-copied"); err != nil {
		return dbFacts{}, err
	}
	// The format is the copy's own; its digest ties it to the bytes verified.
	if err := r.unpackSource(); err != nil {
		if errors.Is(err, ErrNoSpace) {
			_ = removeStagedCopy(r.dir)
		}
		return dbFacts{}, err
	}
	if err := r.fault("staged-copied"); err != nil {
		return dbFacts{}, err
	}
	if err := VerifyDatabaseFile(r.ctx, staged, -1); err != nil {
		return dbFacts{}, fmt.Errorf("storage: the staged copy of %s does not verify: %w", p.Backup, err)
	}
	// SQLite's own writes while it migrates and checkpoints the copy are
	// allowed for, and watched (PROTOCOL §11.4).
	need, err := r.sqliteNeed(staged)
	if err != nil {
		return dbFacts{}, err
	}
	var f dbFacts
	if err := r.sqliteWrites(need, "update the restored database", func(ctx context.Context) error {
		s, err := r.openStaged(ctx)
		if err != nil {
			return err
		}
		f, err = readDBFacts(ctx, s.writer)
		if cerr := s.Close(); err == nil {
			err = cerr
		}
		return err
	}); err != nil {
		return dbFacts{}, err
	}
	if err := r.fault("staged-migrated"); err != nil {
		return dbFacts{}, err
	}
	r.staged = true
	return f, nil
}

// unpackSource turns the staged source into the staged database: a version 1
// copy is renamed and its MAC checked and cut off; a version 2 copy is
// authenticated and only then decoded, and removed. Either way the
// database's sealed values and signed objects are then checked with the data
// directory's keys (checked equal to the bundle's when the restore began).
func (r *restorer) unpackSource() error {
	refuse := func(err error) error {
		if errors.Is(err, ErrNoSpace) {
			return fmt.Errorf("%w: %w", ErrRestoreRefused, err)
		}
		return fmt.Errorf("%w: the staged copy does not verify: %v", ErrRestoreRefused, err)
	}
	serverID, ks, exists, err := readReceiptKeysFile(r.dir)
	if err != nil || !exists {
		return refuse(fmt.Errorf("the data directory has no receipt keys: %v", err))
	}
	secret, err := private.ReadFile(r.path(keysSecretName))
	if err != nil {
		return refuse(err)
	}
	keyByID := map[string][]byte{}
	for _, k := range ks {
		keyByID[k.ID] = k.Secret
	}
	staged, src := r.path(stagedName), r.path(stagedSourceName)
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	fi, err := in.Stat()
	if err != nil {
		in.Close()
		return err
	}
	switch backupFormat(in, fi.Size()) {
	case 1:
		in.Close()
		if err := os.Rename(src, staged); err != nil {
			return err
		}
		if err := unsignStagedCopy(r.ctx, staged, serverID, keyByID); err != nil {
			return refuse(err)
		}
	case 2:
		err := r.decodeSource(in, fi.Size(), serverID, keyByID)
		in.Close()
		if errors.Is(err, errCrashed) {
			return err
		}
		if err != nil {
			return refuse(err)
		}
		if err := os.Remove(src); err != nil {
			return err
		}
		if err := r.cfg.syncDir(r.dir); err != nil {
			return err
		}
		if err := r.fault("staged-source-removed"); err != nil {
			return err
		}
	default:
		in.Close()
		return refuse(errUnsigned)
	}
	db, err := openReadOnly(staged)
	if err != nil {
		return refuse(err)
	}
	defer db.Close()
	if err := verifySealed(r.ctx, db, secret); err != nil {
		return refuse(err)
	}
	if err := verifyMACs(r.ctx, db, keyByID); err != nil {
		return refuse(err)
	}
	return nil
}

// decodeSource authenticates the compressed staged source in with the data
// directory's server ID and keys, then decodes it into the staged database,
// created exclusively (0600) and made durable.
func (r *restorer) decodeSource(in *os.File, size int64, serverID string, keyByID map[string][]byte) (err error) {
	t, err := readTrailerV2(in, size)
	if err != nil {
		return err
	}
	a, err := authenticate(r.ctx, in, t, keyByID, serverID)
	if err != nil {
		return err
	}
	if err := r.fault("staged-authenticated"); err != nil {
		return err
	}
	if err := r.needFree(t.DatabaseSize, 0, "decompress the backup"); err != nil {
		return err
	}
	staged := r.path(stagedName)
	out, err := private.OpenFile(staged, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	err = decompress(r.ctx, a, out, r.spaceGuard)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(staged)
		return err
	}
	if err := r.cfg.syncDir(r.dir); err != nil {
		return err
	}
	return r.fault("staged-decompressed")
}

// sqliteNeed is the space SQLite may write while it opens the staged
// database: migrations, if its schema is older, and the checkpoint.
func (r *restorer) sqliteNeed(staged string) (int64, error) {
	fi, err := os.Stat(staged)
	if err != nil {
		return 0, err
	}
	db, err := openReadOnly(staged)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	version, err := schemaVersionOf(r.ctx, db)
	if err != nil {
		return 0, err
	}
	return sqliteAllowance(fi.Size(), version < latestSchema()), nil
}

func (r *restorer) openStaged(ctx context.Context) (*Store, error) {
	return open(ctx, config{dataDir: r.dir, dbName: stagedName, mode: modeRaw, syncDir: r.cfg.syncDir,
		hooks: hooks{duringMigration: r.cfg.hooks.duringMigration}})
}

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// ---- free space (PROTOCOL §11.4): a restore never takes the data folder's
// last restoreReserve bytes.

func (c *config) restoreDefaults() {
	if c.syncDir == nil {
		c.syncDir = syncDir
	}
	if c.free == nil {
		c.free = freeBytes
	}
	if c.spaceWatch == 0 {
		c.spaceWatch = 50 * time.Millisecond
	}
}

func (r *restorer) free() (int64, error) {
	free, err := freeNear(r.cfg.free, r.dir)
	if err != nil {
		return 0, fmt.Errorf("cannot tell the free space of the data folder: %w", err)
	}
	return free, nil
}

// needFree refuses unless writing n bytes, once freed bytes the restore has
// in temporaries are given back, leaves the reserve free.
func (r *restorer) needFree(n, freed int64, what string) error {
	free, err := r.free()
	if err != nil {
		return err
	}
	if free+freed-n < restoreReserve {
		return NoSpace("not enough free space in the data folder to %s: %d MB free, %d MB needed and %d MB kept free",
			what, mb(free+freed), mb(n), mb(restoreReserve))
	}
	return nil
}

// spaceGuard is asked before each chunk a restore copies or decodes.
func (r *restorer) spaceGuard(n int64) error {
	free, err := r.free()
	if err != nil {
		return err
	}
	if free-n < restoreReserve {
		return NoSpace("the restore was stopped: the data folder's free space fell to %d MB, and %d MB are kept free", mb(free), mb(restoreReserve))
	}
	return nil
}

// sqliteWrites runs fn, which has SQLite write the staged database, given
// need bytes: refused unless they leave the reserve free, which keeps the
// reserve against the restore's own writes. Another program taking space
// meanwhile cannot be prevented (no writer can, pushes included): it is
// detected late, by a watch every spaceWatch that cancels fn's context
// (interrupting SQLite) once the free space is under the reserve, and by a
// check when fn has returned, after SQLite's checkpoint at close. The staged
// files are then removed, so the space is given back, the database the
// restore would replace is kept, and the restore resumes from the backup once
// space is freed.
func (r *restorer) sqliteWrites(need int64, what string, fn func(ctx context.Context) error) error {
	refuse := func(err error) error {
		_ = removeStagedCopy(r.dir)
		return fmt.Errorf("%w: %w", ErrRestoreRefused, err)
	}
	if err := r.needFree(need, 0, what); err != nil {
		return refuse(err)
	}
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	var low error
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(r.cfg.spaceWatch)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if err := r.spaceGuard(0); err != nil {
				low = err
				cancel()
				if h := r.cfg.hooks.restoreFault; h != nil {
					_ = h("space-watch-stopped") // tests: the watch has stopped SQLite
				}
				return
			}
		}
	}()
	err := fn(ctx)
	cancel()
	<-stopped
	if low == nil {
		low = r.spaceGuard(0)
	}
	if low != nil {
		return refuse(low)
	}
	return err
}

// ---- the transition of the staged copy (PROTOCOL §11.4)

type liveLogin struct {
	username, hash string
	version        int64
	admin          bool
}

// liveDB is what the restore reads from the database it replaces.
type liveDB struct {
	readable bool
	facts    dbFacts
	states   map[string]string // account ID → state
	logins   map[string]liveLogin
}

// inspectLive reads the live (pre-restore) database if it exists and is
// readable; it never fails the restore.
func inspectLive(ctx context.Context, path string) liveDB {
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		return liveDB{}
	}
	// Read-write but query-only: the server is stopped and the lock is held, and
	// a database left by a crash needs its WAL recovered to be read.
	db, err := sql.Open("sqlite", path+"?"+url.Values{"_pragma": {"busy_timeout(5000)", "trusted_schema(0)", "query_only(1)"}}.Encode())
	if err != nil {
		return liveDB{}
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var check string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&check); err != nil || check != "ok" {
		return liveDB{}
	}
	f, err := readDBFacts(ctx, db)
	if err != nil {
		return liveDB{}
	}
	out := liveDB{facts: f, states: map[string]string{}, logins: map[string]liveLogin{}}
	rows, err := db.QueryContext(ctx, `SELECT a.account_id, a.state, l.account_id IS NOT NULL, COALESCE(l.username, ''), COALESCE(l.password_hash, ''),
		COALESCE(l.credentials_version, 0), COALESCE(l.is_admin, 0) FROM accounts a LEFT JOIN account_logins l USING (account_id)`)
	if err != nil {
		return liveDB{facts: f}
	}
	defer rows.Close()
	for rows.Next() {
		var id, state string
		var has bool
		var l liveLogin
		if err := rows.Scan(&id, &state, &has, &l.username, &l.hash, &l.version, &l.admin); err != nil {
			return liveDB{facts: f}
		}
		out.states[id] = state
		if has {
			out.logins[id] = l
		}
	}
	if rows.Err() != nil {
		return liveDB{facts: f}
	}
	out.readable = true
	return out
}

// transition applies the transition to the staged copy in one transaction,
// carrying logins from the live database, then leaves the copy self-contained
// (rollback-journal mode, no WAL) for the swap. It returns the report.
func (r *restorer) transition(p *restoreProgress, serverID string) (*RestoreReport, error) {
	live := inspectLive(r.ctx, r.path(databaseName))
	rep := &RestoreReport{Backup: p.Backup, Epoch: p.Epoch}
	if _, err := os.Stat(r.path(databaseName)); err == nil {
		rep.PreRestore = p.PreRestore
	}
	if err := r.sqliteWrites(sqliteAllowance(0, false), "update the restored database", func(ctx context.Context) error {
		s, err := r.openStaged(ctx)
		if err != nil {
			return err
		}
		err = s.Write(ctx, func(w *WriteTx) error {
			if err := w.applyTransition(p.Epoch, p.Incarnation, p.AuthRevision); err != nil {
				return err
			}
			return w.carryLogins(live, serverID, p.StartedAt, rep)
		})
		if cerr := s.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		return selfContained(ctx, r.path(stagedName))
	}); err != nil {
		return nil, err
	}
	if err := r.fault("staged-transitioned"); err != nil {
		return nil, err
	}
	return rep, nil
}

// selfContained switches a closed database to rollback-journal mode, which
// folds its WAL into the file, and syncs it; the server's open switches it back.
func selfContained(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", path+"?"+url.Values{"_pragma": {"trusted_schema(0)"}}.Encode())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=DELETE").Scan(&mode); err != nil || mode != "delete" {
		db.Close()
		return fmt.Errorf("storage: preparing the staged database: journal mode %q: %v", mode, err)
	}
	if err := db.Close(); err != nil {
		return err
	}
	return syncFile(path)
}

// carryLogins applies PROTOCOL §11.4 to the staged copy: each account's login comes from
// the live database when it is readable and of this server (credentials
// version raised by one); accounts deleted after the backup return disabled; a
// username clash renames the returning account.
func (w *WriteTx) carryLogins(live liveDB, serverID string, at int64, rep *RestoreReport) error {
	ctx, tx := w.ctx, w.tx
	type account struct {
		id, state, username, hash string
		hasLogin, admin           bool
		version                   int64
		carried, returning        bool
	}
	rows, err := tx.QueryContext(ctx, `SELECT a.account_id, a.state, l.account_id IS NOT NULL, COALESCE(l.username, ''), COALESCE(l.password_hash, ''),
		COALESCE(l.credentials_version, 0), COALESCE(l.is_admin, 0) FROM accounts a LEFT JOIN account_logins l USING (account_id) ORDER BY a.account_id`)
	if err != nil {
		return err
	}
	var accounts []*account
	inBackup := map[string]bool{}
	for rows.Next() {
		a := &account{}
		if err := rows.Scan(&a.id, &a.state, &a.hasLogin, &a.username, &a.hash, &a.version, &a.admin); err != nil {
			rows.Close()
			return err
		}
		accounts = append(accounts, a)
		inBackup[a.id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	carry := live.readable && live.facts.serverID == serverID
	switch {
	case carry:
		rep.LoginsFrom = "carried from the previous database where the account still exists"
	case !live.readable && live.facts.serverID == "":
		rep.LoginsFrom = "kept from the backup (there is no readable previous database)"
	default:
		rep.LoginsFrom = "kept from the backup (the previous database belongs to another server or cannot be read)"
	}
	for _, a := range accounts {
		a.version++ // every restore raises every credentials version
		if !carry {
			continue
		}
		if l, ok := live.logins[a.id]; ok && a.hasLogin {
			a.username, a.hash, a.admin, a.version, a.carried = l.username, l.hash, l.admin, max(l.version, a.version-1)+1, true
		}
		if state, ok := live.states[a.id]; ok {
			a.state = state
		} else {
			a.state, a.returning = "disabled", true
		}
	}
	// Usernames: carried logins keep theirs; the others keep theirs when free,
	// and otherwise take <name>-restored, <name>-restored-2, ….
	taken := map[string]bool{}
	for _, a := range accounts {
		if a.hasLogin && a.carried {
			taken[strings.ToLower(a.username)] = true
		}
	}
	var clashes []*account
	for _, a := range accounts {
		if a.hasLogin && !a.carried {
			if key := strings.ToLower(a.username); taken[key] {
				clashes = append(clashes, a)
			} else {
				taken[key] = true
			}
		}
	}
	for _, a := range clashes {
		name := a.username + "-restored"
		for n := 2; taken[strings.ToLower(name)]; n++ {
			name = fmt.Sprintf("%s-restored-%d", a.username, n)
		}
		rep.Renamed = append(rep.Renamed, a.username+" → "+name)
		a.username = name
		taken[strings.ToLower(name)] = true
	}
	// Returning administrators stay active when no other administrator would be
	// (otherwise nobody could administer the server).
	activeAdmin := false
	for _, a := range accounts {
		activeAdmin = activeAdmin || (a.hasLogin && a.admin && a.state == "active")
	}
	if !activeAdmin {
		for _, a := range accounts {
			if a.returning && a.hasLogin && a.admin {
				a.state = "active"
				rep.KeptActive = append(rep.KeptActive, a.username)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE account_logins SET username_key = char(1) || account_id`); err != nil {
		return err
	}
	for _, a := range accounts {
		name := a.id
		if a.hasLogin {
			name = a.username
			if _, err := tx.ExecContext(ctx, `UPDATE account_logins SET username = ?, username_key = ?, password_hash = ?, credentials_version = ?,
				is_admin = ?, credentials_changed_at = ? WHERE account_id = ?`,
				a.username, strings.ToLower(a.username), a.hash, a.version, boolInt(a.admin), at, a.id); err != nil {
				return err
			}
			if a.carried {
				rep.LoginsCarried = append(rep.LoginsCarried, name)
			} else {
				rep.LoginsKept = append(rep.LoginsKept, name)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET state = ? WHERE account_id = ?`, a.state, a.id); err != nil {
			return err
		}
		rep.Restored = append(rep.Restored, name)
		if a.returning && a.state == "disabled" {
			rep.ReturnedDisabled = append(rep.ReturnedDisabled, name)
		}
	}
	if carry {
		for id := range live.states {
			if !inBackup[id] {
				name := id
				if l, ok := live.logins[id]; ok {
					name = l.username
				}
				rep.NotRestored = append(rep.NotRestored, name)
			}
		}
	}
	for _, l := range [][]string{rep.Restored, rep.NotRestored, rep.ReturnedDisabled, rep.KeptActive, rep.Renamed, rep.LoginsCarried, rep.LoginsKept} {
		slices.Sort(l)
	}
	return nil
}

// ---- swap (PROTOCOL §11.4)

// swap keeps the old database (with its WAL and shared memory) as the
// pre-restore file and renames the staged copy into place. Each rename is
// atomic and the order makes every crash point resumable: the old WAL moves
// before its database, and the staged file, which has no WAL, moves last.
func (r *restorer) swap(p *restoreProgress) error {
	live, staged, pre := r.path(databaseName), r.path(stagedName), r.path(p.PreRestore)
	if _, err := os.Stat(staged); errors.Is(err, os.ErrNotExist) {
		return r.cfg.syncDir(r.dir) // swapped already
	} else if err != nil {
		return err
	}
	_, liveErr := os.Stat(live)
	for _, sfx := range []string{"-wal", "-shm"} {
		// The old database's files go with it; without it they are orphans,
		// which must never meet the restored database.
		if _, err := os.Stat(live + sfx); err == nil {
			if err := os.Rename(live+sfx, pre+sfx); err != nil {
				return err
			}
		}
	}
	if err := r.fault("old-wal-moved"); err != nil {
		return err
	}
	if liveErr == nil {
		if err := os.Rename(live, pre); err != nil {
			return err
		}
	}
	if err := r.fault("old-moved"); err != nil {
		return err
	}
	if err := os.Rename(staged, live); err != nil {
		return err
	}
	if err := r.cfg.syncDir(r.dir); err != nil {
		return err
	}
	if err := r.fault("staged-moved"); err != nil {
		return err
	}
	// The old timeline's snapshot views.
	removeViews(r.dir)
	return nil
}

// hasSidecars reports SQLite files beside a database (a WAL, its index, a
// rollback journal): a backup is one self-contained file.
func hasSidecars(path string) bool {
	for _, sfx := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + sfx); err == nil {
			return true
		}
	}
	return false
}

// fileDigest is a file's SHA-256, lowercase hex.
func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// InstallKeys puts a stopped server's lost keys back from its key bundle: the bundle must belong to
// this server, its keys must equal any key file still present, and they must open every sealed
// value and verify every receipt, fact and decision of the database.
func InstallKeys(ctx context.Context, dataDir string, bundleData, passphrase []byte) error {
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	if err := private.Dir(dir); err != nil {
		return fmt.Errorf("storage: data directory: %w", err)
	}
	lock, err := lockDirectory(dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	refuse := func(format string, args ...any) error {
		return fmt.Errorf("%w: "+format, append([]any{ErrRestoreRefused}, args...)...)
	}
	bundle, err := keys.Open(bundleData, passphrase)
	switch {
	case errors.Is(err, keys.ErrPassphrase):
		return refuse("the passphrase is wrong or the key bundle is damaged")
	case err != nil:
		return refuse("the key bundle cannot be read: %v", err)
	}
	db, err := openReadOnly(filepath.Join(dir, databaseName))
	if err != nil {
		return err
	}
	defer db.Close()
	f, err := readDBFacts(ctx, db)
	if err != nil || f.serverID == "" {
		return refuse("the data directory has no server database: %v", err)
	}
	if bundle.ServerID != f.serverID {
		return refuse("the key bundle belongs to server %s, this data directory to server %s", bundle.ServerID, f.serverID)
	}
	var ks []ReceiptKey
	keyByID := map[string][]byte{}
	for _, k := range bundle.ReceiptKeys {
		ks = append(ks, ReceiptKey{ID: k.ID, Secret: k.Key})
		keyByID[k.ID] = k.Key
	}
	fileServer, fileKeys, exists, err := readReceiptKeysFile(dir)
	if err != nil {
		return refuse("%v", err)
	}
	if exists && (fileServer != f.serverID || !sameKeys(fileKeys, ks)) {
		return refuse("the key bundle's receipt keys differ from %s", receiptKeysName)
	}
	secretPath := filepath.Join(dir, keysSecretName)
	secret, serr := private.ReadFile(secretPath)
	secretMissing := errors.Is(serr, os.ErrNotExist)
	switch {
	case serr == nil && !bytes.Equal(secret, bundle.SecretsKey):
		return refuse("the key bundle's secrets key differs from %s", keysSecretName)
	case serr != nil && !secretMissing:
		return serr
	}
	if err := verifySealed(ctx, db, bundle.SecretsKey); err != nil {
		return refuse("%v", err)
	}
	if err := verifyMACs(ctx, db, keyByID); err != nil {
		return refuse("%v", err)
	}
	if !exists {
		if err := writeReceiptKeysFile(dir, f.serverID, newestFirst(ks)); err != nil {
			return err
		}
	}
	if secretMissing {
		if err := keys.WriteFile(secretPath, bundle.SecretsKey); err != nil {
			return err
		}
	}
	return nil
}
