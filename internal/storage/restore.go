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
	preRestorePrefix = databaseName + ".pre-restore-"
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
	ctx    context.Context
	dir    string
	cfg    config
	m      *metaFile
	staged bool // the staged copy was made by this run
}

func restore(ctx context.Context, cfg config, opts RestoreOptions) (*RestoreReport, error) {
	if cfg.syncDir == nil {
		cfg.syncDir = syncDir
	}
	dir, err := filepath.Abs(cfg.dataDir)
	if err != nil {
		return nil, err
	}
	backup, err := filepath.Abs(opts.Backup)
	if err != nil {
		return nil, err
	}
	if err := private.Dir(dir); err != nil {
		return nil, fmt.Errorf("storage: data directory: %w", err)
	}
	lock, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
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
	digest, err := fileDigest(backup)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read the backup %s: %v", ErrRestoreRefused, backup, err)
	}
	bundle, serverID, err := r.verify(backup, opts)
	if err != nil {
		return nil, err
	}
	// The restore is bound to the bytes it verified: every staging checks the
	// copy against this digest.
	if after, err := fileDigest(backup); err != nil || after != digest {
		return nil, fmt.Errorf("%w: the backup changed while it was being verified", ErrRestoreRefused)
	}
	if err := r.installKeys(bundle); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	pre, err := preRestoreName(dir, now)
	if err != nil {
		return nil, err
	}
	if err := m.update("restore-"+stepStage, func(st *restoreState) {
		if st.ServerID == "" {
			st.ServerID = serverID
		}
		st.Restore = &restoreProgress{Step: stepStage, Backup: backup, BackupSHA256: digest, StartedAt: now.UnixMilli(), Incarnation: newUUID(),
			PreRestore: pre}
	}); err != nil {
		return nil, err
	}
	return r.finish(opts.Out)
}

// resumeRestore finishes an interrupted restore at startup (PROTOCOL §11.4); it needs the
// backup file only if the database has not been replaced yet.
func resumeRestore(ctx context.Context, cfg config, dir string, m *metaFile) error {
	if cfg.syncDir == nil {
		cfg.syncDir = syncDir
	}
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
// returns the bundle and the backup's server ID.
func (r *restorer) verify(backup string, opts RestoreOptions) (keys.Bundle, string, error) {
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
	// The MAC is checked before anything else is read from the file.
	file, trailer, err := openSignedBackup(backup)
	switch {
	case errors.Is(err, errUnsigned):
		return refuse("the backup %s is unsigned (%v): backups made before October 2026 cannot be restored; take a new backup with this server", backup, err)
	case err != nil:
		return refuse("cannot read the backup %s: %v", backup, err)
	}
	defer file.Close()
	if bundle.ServerID != trailer.ServerID {
		return refuse("the key bundle belongs to server %s, the backup to server %s", bundle.ServerID, trailer.ServerID)
	}
	if st := r.m.state(); st != nil && st.ServerID != trailer.ServerID {
		return refuse("the backup belongs to server %s, this data directory to server %s", trailer.ServerID, st.ServerID)
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
	if exists && (fileServer != trailer.ServerID || !sameKeys(fileKeys, bundleKeys)) {
		return refuse("the key bundle's receipt keys differ from this data directory's %s", receiptKeysName)
	}
	if secret, err := private.ReadFile(r.path(keysSecretName)); err == nil && !bytes.Equal(secret, bundle.SecretsKey) {
		return refuse("the key bundle's secrets key differs from this data directory's %s", keysSecretName)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return refuse("cannot read %s: %v", keysSecretName, err)
	}
	if err := checkBackupMAC(file, trailer, keyByID); err != nil {
		return refuse("%v", err)
	}
	// Second lines behind the MAC: the database checks (integrity, foreign
	// keys, the exact schema of its version), then every sealed value and
	// signed object.
	if err := VerifyDatabaseFile(ctx, backup, -1); err != nil {
		return refuse("the backup does not verify: %v", err)
	}
	db, err := openReadOnly(backup)
	if err != nil {
		return refuse("cannot open the backup: %v", err)
	}
	defer db.Close()
	f, err := readDBFacts(ctx, db)
	if err != nil || f.serverID != trailer.ServerID {
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
	return bundle, f.serverID, nil
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
// responses (feed.StoredResponse.Seal) and staged push parts (protocol sealPart;
// PROTOCOL §7.2). The formats must stay equal to those writers'.
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

// stage copies the backup to the staged file (mode 0600), verifies the copy
// and migrates it; it returns the staged database's facts.
func (r *restorer) stage(p *restoreProgress) (dbFacts, error) {
	staged := r.path(stagedName)
	for _, sfx := range []string{"", "-wal", "-shm", "-journal"} {
		if err := os.Remove(staged + sfx); err != nil && !errors.Is(err, os.ErrNotExist) {
			return dbFacts{}, err
		}
	}
	if err := copyFile(p.Backup, staged); err != nil {
		return dbFacts{}, fmt.Errorf("storage: copying the backup %s: %w", p.Backup, err)
	}
	if got, err := fileDigest(staged); err != nil || got != p.BackupSHA256 {
		return dbFacts{}, fmt.Errorf("%w: the backup %s is not the file that was verified; run restore again", ErrRestoreRefused, p.Backup)
	}
	// The staged copy itself, a single file, is verified with the keys the restore installed:
	// no sidecar can stand in for its contents. Its MAC is checked and its trailer cut off
	// first.
	if err := r.verifyStagedCopy(staged); err != nil {
		return dbFacts{}, err
	}
	if err := r.fault("staged-copied"); err != nil {
		return dbFacts{}, err
	}
	if err := VerifyDatabaseFile(r.ctx, staged, -1); err != nil {
		return dbFacts{}, fmt.Errorf("storage: the staged copy of %s does not verify: %w", p.Backup, err)
	}
	s, err := r.openStaged()
	if err != nil {
		return dbFacts{}, err
	}
	f, err := readDBFacts(r.ctx, s.writer)
	if cerr := s.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return dbFacts{}, err
	}
	if err := r.fault("staged-migrated"); err != nil {
		return dbFacts{}, err
	}
	r.staged = true
	return f, nil
}

func (r *restorer) openStaged() (*Store, error) {
	return open(r.ctx, config{dataDir: r.dir, dbName: stagedName, mode: modeRaw, syncDir: r.cfg.syncDir})
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
	s, err := r.openStaged()
	if err != nil {
		return nil, err
	}
	err = s.Write(r.ctx, func(w *WriteTx) error {
		if err := w.applyTransition(p.Epoch, p.Incarnation, p.AuthRevision); err != nil {
			return err
		}
		return w.carryLogins(live, serverID, p.StartedAt, rep)
	})
	if cerr := s.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if err := selfContained(r.ctx, r.path(stagedName)); err != nil {
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

// verifyStagedCopy opens every sealed value and verifies every fact,
// receipt and decision of the staged copy with the data directory's keys
// (checked equal to the bundle's when the restore began).
func (r *restorer) verifyStagedCopy(staged string) error {
	refuse := func(err error) error {
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
	if err := unsignStagedCopy(staged, serverID, keyByID); err != nil {
		return refuse(err)
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
