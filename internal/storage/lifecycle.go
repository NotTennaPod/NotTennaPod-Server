package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Startup detection, the restore transition and the auth revision
// (PROTOCOL §11).

// viewsDirName is the snapshot views' spool directory in the data directory
// (the protocol's ViewRegistry; PROTOCOL §6.4 "Durable views").
const viewsDirName = "views"

// startup runs after the lock, quick_check, migrations and the receipt keys,
// before serving (PROTOCOL §11.3). It reads the metadata, refuses another server's
// database, moves a restored or replaced database to a new timeline, finalizes
// an auth revision that committed (PROTOCOL §11.3) and starts a new incarnation.
func (s *Store) startup(ctx context.Context) error {
	f, err := readDBFacts(ctx, s.writer)
	if err != nil {
		return err
	}
	if s.dbIdentity, err = identityOf(filepath.Join(s.dir, s.dbName)); err != nil {
		return err
	}
	var reasons []string
	st := s.meta.state()
	switch {
	case st == nil && f.flag:
		// The metadata is lost: treat the database as restored. The transition
		// writes a new file.
		reasons = append(reasons, "restore-state.json is missing although it existed")
	case st == nil:
		// A first start, or a database from before restore metadata (PROTOCOL §11.3): the metadata
		// starts from the database, with the high-water mark at its highest
		// epoch, and the epoch does not change. The file is created by the first
		// write below (the transition's or the new incarnation's first phase).
		if f.marker {
			reasons = append(reasons, "it is a backup copy")
		}
	default:
		if st.ServerID != f.serverID {
			return fmt.Errorf("%w: restore-state.json belongs to server %s, the database to server %s; to put back a backup, use the restore command",
				ErrOtherServer, st.ServerID, f.serverID)
		}
		if f.incarnation != st.Incarnation && (st.NextIncarnation == "" || f.incarnation != st.NextIncarnation) {
			reasons = append(reasons, "its incarnation is not the current one")
		}
		if f.marker {
			reasons = append(reasons, "it is a backup copy")
		}
		if st.DBFile != nil && *st.DBFile != s.dbIdentity {
			reasons = append(reasons, "the database file was replaced")
		}
		if f.authRevision < st.AuthRevision {
			reasons = append(reasons, "its auth revision is older than the server's")
		}
		if p := st.PendingAuthRevision; p != 0 && f.authRevision < p {
			// PROTOCOL §11.3: the change may never have committed, or committed and been
			// rolled back; only the second matters, and it cannot be told apart.
			reasons = append(reasons, "an access-removing change may have been rolled back")
		}
	}
	if len(reasons) > 0 {
		reason := strings.Join(reasons, "; ")
		s.restoredBecause = reason
		if err := s.Write(ctx, func(w *WriteTx) error { return w.RestoreTransition(reason) }); err != nil {
			return err
		}
		// A detected restore has no pre-restore database (PROTOCOL §11.3).
		slog.Warn("kept the logins of the restored database; every device and browser signs in again")
		return nil
	}
	return s.newIncarnation(ctx, f)
}

// newIncarnation writes a new incarnation in two phases so that a crash never
// causes a false detection (PROTOCOL §11.3): the metadata names the next incarnation, the
// database takes it, and the metadata then makes it current. The first phase
// also finalizes an auth revision whose transaction committed (PROTOCOL §11.3).
func (s *Store) newIncarnation(ctx context.Context, f dbFacts) error {
	t := newUUID()
	if err := s.meta.update("incarnation-next", func(st *restoreState) {
		if st.ServerID == "" { // a first start creates the file
			id := s.dbIdentity
			st.ServerID, st.Incarnation, st.DBFile = f.serverID, f.incarnation, &id
		}
		if st.PendingAuthRevision != 0 && f.authRevision >= st.PendingAuthRevision {
			st.PendingAuthRevision = 0
		}
		st.AuthRevision = max(st.AuthRevision, f.authRevision)
		st.EpochHighWater = max(st.EpochHighWater, f.maxEpoch)
		st.NextIncarnation = t
	}); err != nil {
		return err
	}
	if err := s.Write(ctx, func(w *WriteTx) error {
		if err := setMeta(ctx, w.tx, metaIncarnation, t); err != nil {
			return err
		}
		return setMeta(ctx, w.tx, metaRestoreFlag, "1")
	}); err != nil {
		return err
	}
	id := s.dbIdentity
	return s.meta.update("incarnation-final", func(st *restoreState) {
		st.Incarnation, st.NextIncarnation, st.DBFile = t, "", &id
	})
}

// transition is a restore transition made in a write transaction.
type transition struct {
	reason       string
	epoch        int64
	incarnation  string
	authRevision int64
}

// RestoreTransition moves the whole server to a new timeline inside this write
// transaction (PROTOCOL §11.3): startup runs it for a detected
// restore, and PROTOCOL §6.11 runs it when receipt evidence proves a lost
// timeline. The new epoch E is persisted as the high-water mark first; then
// every account moves to E (head seq and digest unchanged), every device token
// and web session is deleted, the old timeline's push-part assemblies go, the
// backup marker is cleared, a new incarnation is written, unfinished imports
// fail, and the auth revision rises above both the database's and the
// metadata's. After the commit the old timeline's snapshot views are deleted and
// the metadata takes the new incarnation and auth revision. Calling it twice in
// one transaction has no further effect.
func (w *WriteTx) RestoreTransition(reason string) error {
	m := w.store.meta
	if m == nil {
		return errors.New("storage: a restore transition needs the server's restore metadata")
	}
	if w.transition != nil {
		return nil
	}
	f, err := readDBFacts(w.ctx, w.tx)
	if err != nil {
		return err
	}
	t := &transition{reason: reason, incarnation: newUUID()}
	if err := m.update("transition-begin", func(st *restoreState) {
		if st.ServerID == "" {
			st.ServerID = f.serverID
		}
		t.epoch = max(st.EpochHighWater, f.maxEpoch, w.evidenceEpoch) + 1
		t.authRevision = max(f.authRevision, st.AuthRevision, st.PendingAuthRevision) + 1
		st.EpochHighWater, st.NextIncarnation, st.PendingAuthRevision = t.epoch, t.incarnation, t.authRevision
	}); err != nil {
		return err
	}
	w.transition = t
	return w.applyTransition(t.epoch, t.incarnation, t.authRevision)
}

// applyTransition is the database side of the transition, shared with the
// restore procedure's staged database (PROTOCOL §11.3 step 2).
func (w *WriteTx) applyTransition(epoch int64, incarnation string, authRevision int64) error {
	ctx, tx := w.ctx, w.tx
	// The head seq and digest stay: the first commit of E chains from the
	// restored head's digest (contract PROTOCOL §11.3).
	if _, err := tx.ExecContext(ctx, `UPDATE account_logs SET epoch = ?`, epoch); err != nil {
		return fmt.Errorf("storage: restore transition: %w", err)
	}
	for _, q := range []string{
		`DELETE FROM auth_tokens`,
		`DELETE FROM push_parts`,
		`DELETE FROM recovery_frames`,
		`DELETE FROM server_meta WHERE key = 'backup'`,
		// Keys live outside the database (PROTOCOL §11.3); a backup from before
		// PROTOCOL §11 still holds them, checked equal to the bundle's.
		`DELETE FROM receipt_keys`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("storage: restore transition: %w", err)
		}
	}
	if err := setMeta(ctx, tx, metaIncarnation, incarnation); err != nil {
		return err
	}
	if err := setMeta(ctx, tx, metaRestoreFlag, "1"); err != nil {
		return err
	}
	if err := w.FailUnfinishedImports(); err != nil {
		return err
	}
	return setMeta(ctx, tx, metaAuthRevision, strconv.FormatInt(authRevision, 10))
}

// removeViews deletes every published snapshot view: after a transition they
// all belong to the old timeline. Views being built are left to the registry,
// whose next sweep removes them as stale.
func removeViews(dataDir string) {
	dir := filepath.Join(dataDir, viewsDirName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") || strings.HasSuffix(e.Name(), ".jsonl") {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// ---- the auth revision (PROTOCOL §11.3)

// markAuthChange records that this transaction removes access; Write raises the
// auth revision around its commit.
func (w *WriteTx) markAuthChange() { w.authChange = true }

// raiseAuthRevision is the first phase of an auth revision raise, run just
// before the commit: the metadata records the pending revision, then the
// transaction writes it.
func (w *WriteTx) raiseAuthRevision() error {
	m := w.store.meta
	if !w.authChange || w.transition != nil || m == nil {
		return nil
	}
	f, err := readDBFacts(w.ctx, w.tx)
	if err != nil {
		return err
	}
	var r int64
	if err := m.update("auth-pending", func(st *restoreState) {
		r = max(f.authRevision, st.AuthRevision, st.PendingAuthRevision) + 1
		st.PendingAuthRevision = r
	}); err != nil {
		return err
	}
	w.raised = r
	return setMeta(w.ctx, w.tx, metaAuthRevision, strconv.FormatInt(r, 10))
}

// finish runs after the commit: the metadata takes the committed revision and,
// after a transition, the new incarnation. A failure here leaves a pending
// revision equal to the database's, which the next start finalizes.
func (w *WriteTx) finish() {
	s := w.store
	if s.meta == nil {
		return
	}
	if t := w.transition; t != nil {
		removeViews(s.dir)
		id := s.dbIdentity
		err := s.meta.update("transition-final", func(st *restoreState) {
			if st.NextIncarnation == t.incarnation {
				st.NextIncarnation = ""
			}
			st.Incarnation, st.DBFile = t.incarnation, &id
			st.AuthRevision = max(st.AuthRevision, t.authRevision)
			if st.PendingAuthRevision == t.authRevision {
				st.PendingAuthRevision = 0
			}
		})
		logFinishError(err)
		slog.Warn("the database was restored or rolled back: every account moved to a new epoch and every device and browser was signed out",
			"reason", t.reason, "epoch", t.epoch)
		return
	}
	if r := w.raised; r != 0 {
		logFinishError(s.meta.update("auth-final", func(st *restoreState) {
			st.AuthRevision = max(st.AuthRevision, r)
			if st.PendingAuthRevision == r {
				st.PendingAuthRevision = 0
			}
		}))
	}
}

// revert undoes the first phase of a raise or transition whose transaction
// certainly did not commit, so the next start sees no pending revision. The
// high-water mark stays (it only increases).
func (w *WriteTx) revert() {
	m := w.store.meta
	if m == nil || (w.raised == 0 && w.transition == nil) {
		return
	}
	logFinishError(m.updateIf("revert", func(st *restoreState) bool {
		changed := false
		if w.raised != 0 && st.PendingAuthRevision == w.raised {
			st.PendingAuthRevision, changed = 0, true
		}
		if t := w.transition; t != nil {
			if st.PendingAuthRevision == t.authRevision {
				st.PendingAuthRevision, changed = 0, true
			}
			if st.NextIncarnation == t.incarnation {
				st.NextIncarnation, changed = "", true
			}
		}
		return changed
	}))
}

func logFinishError(err error) {
	if err != nil && !errors.Is(err, errCrashed) {
		slog.Warn("could not update restore-state.json; the next start finishes it", "error", err)
	}
}
