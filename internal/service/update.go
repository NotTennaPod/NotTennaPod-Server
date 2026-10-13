package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The update transaction (PROTOCOL §14).
//
// An update keeps a durable record, update.json in the installation
// directory, and a pending marker in the data directory, both written before
// anything changes. The record holds what a resumed run needs (the previous
// manifest, the data directory and the new version) and the phase the
// update reached, each phase recorded before its step. The rollback pair is
// the previous binary, kept as .prev (a hard link), and the database copy
// (storage's update copy, with its own record of the rollback's steps).
//
// While a record exists, a new update first resolves the earlier one and then
// stops, except to repair a kept new version (below). The record is checked
// whole before anything changes; a damaged one changes nothing. Nothing
// removes either half of the pair before the record names the outcome:
//   - Before the binary was replaced (the phase says so; only while it is
//     being placed do the files decide, and a missing .prev then decides
//     nothing): the update is undone and the server started again.
//   - After it: with .prev missing, nothing more happens. If the new version
//     answers, the update is finished; otherwise it is rolled back, step by
//     step and repeatably: the copy must be there and verify and the new
//     version must have accepted nothing; the previous binary and manifest go
//     back; the copy goes into place; the previous version must answer.
//   - When the new version had already accepted changes, nothing goes back:
//     the record stays kept-new. A retry starts it if it is stopped and
//     finishes once it answers. `service update FIXED` repairs it forwards: the
//     earlier pair, which can no longer undo the accepted changes, is moved
//     aside and kept, and a new update starts from the kept version and the
//     current database.
//   - Once the outcome is recorded (the new version answered, the previous
//     one answered, or the update was undone), cleanup removes the pair, the
//     marker and the record, never stopping a server and never needing what
//     it already removed, and reports the recorded outcome. An undone update
//     whose server does not start keeps its record, so the next run tries
//     again.

// updateRecordName is the update's record in the installation directory.
const updateRecordName = "update.json"

// Phases of the record. The first five come in this order.
const (
	phasePrepared    = "prepared"  // the record and the marker are written; nothing else changed
	phasePreserved   = "preserved" // .prev is the installed binary
	phaseSaved       = "saved"     // the server is stopped and its database copied
	phasePlacing     = "placing"   // the new binary is replacing the installed one
	phasePlaced      = "placed"    // the new binary is installed
	phaseRollingBack = "rolling-back"
	phaseKeptNew     = "kept-new"
	phaseDone        = "done" // Outcome names how it ended; cleanup may remain
)

// Outcomes of a done update.
const (
	outcomeNew     = "new"     // the new version answered
	outcomeOld     = "old"     // rolled back; the previous version answered
	outcomeAborted = "aborted" // undone before the binary was replaced
)

type updateRecord struct {
	Version     int             `json:"version"`
	Phase       string          `json:"phase"`
	Outcome     string          `json:"outcome,omitempty"`
	DataDir     string          `json:"data_dir"`
	OldManifest json.RawMessage `json:"old_manifest"`
	NewVersion  string          `json:"new_version"`
	Cause       string          `json:"cause,omitempty"` // why it rolled back, was kept or undone
	Aside       *asidePair      `json:"aside,omitempty"` // where a kept update's pair goes on repair
	StartedAt   int64           `json:"started_at"`
}

// asidePair is where a kept-new update's pair is moved when it is repaired.
type asidePair struct {
	Binary string `json:"binary"`
	Copy   string `json:"copy"`
}

func (m *Manager) recordPath() string  { return filepath.Join(m.InstallDir, updateRecordName) }
func (m *Manager) prevPath() string    { return m.BinaryPath() + ".prev" }
func (m *Manager) restorePath() string { return m.BinaryPath() + ".restore" }

// readRecord reads the update record (nil when no update is unfinished).
func (m *Manager) readRecord() (*updateRecord, error) {
	data, err := private.ReadFile(m.recordPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("service: the update record: %w", err)
	}
	var rec updateRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, m.damaged(&rec, err)
	}
	if err := rec.validate(); err != nil {
		return nil, m.damaged(&rec, err)
	}
	return &rec, nil
}

// ValidVersion checks a version a binary reports: 1 to 128 letters, digits
// and . - _ + ~, as git describe gives. Anything else is not a
// NotTennaPod server build's version.
func ValidVersion(v string) error {
	if v == "" {
		return errors.New("it is empty")
	}
	if len(v) > 128 {
		return errors.New("it is longer than 128 characters")
	}
	for _, c := range v {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune(".-_+~", c)) {
			return fmt.Errorf("%q holds %q", v, c)
		}
	}
	return nil
}

// complete reports a manifest that lacks what an update needs to check the
// server it starts.
func (man Manifest) complete() error {
	switch {
	case ValidVersion(man.Version) != nil:
		return fmt.Errorf("its version: %v", ValidVersion(man.Version))
	case man.Listen == "":
		return errors.New("it names no listen address")
	case man.DataDir() == "" || !filepath.IsAbs(man.DataDir()):
		return errors.New("it names no absolute data directory")
	}
	return nil
}

// validate checks the whole record before anything is changed: a known phase and outcome, a
// complete previous manifest whose data directory is the record's, and the new version.
func (rec *updateRecord) validate() error {
	phases := []string{phasePrepared, phasePreserved, phaseSaved, phasePlacing, phasePlaced, phaseRollingBack, phaseKeptNew, phaseDone}
	switch {
	case rec.Version != 1:
		return fmt.Errorf("version %d", rec.Version)
	case !slices.Contains(phases, rec.Phase):
		return fmt.Errorf("unknown phase %q", rec.Phase)
	case rec.Phase == phaseDone && !slices.Contains([]string{outcomeNew, outcomeOld, outcomeAborted}, rec.Outcome):
		return fmt.Errorf("unknown outcome %q", rec.Outcome)
	case rec.DataDir == "" || !filepath.IsAbs(rec.DataDir):
		return errors.New("no absolute data directory")
	case ValidVersion(rec.NewVersion) != nil:
		return fmt.Errorf("the new version: %v", ValidVersion(rec.NewVersion))
	case !bytes.HasPrefix(bytes.TrimSpace(rec.OldManifest), []byte("{")):
		return errors.New("no previous manifest")
	}
	old, _, err := rec.manifests()
	if err != nil {
		return fmt.Errorf("the previous manifest: %v", err)
	}
	if err := old.complete(); err != nil {
		return fmt.Errorf("the previous manifest: %v", err)
	}
	if filepath.Clean(old.DataDir()) != filepath.Clean(rec.DataDir) {
		return fmt.Errorf("the previous manifest's data directory %s is not the record's %s", old.DataDir(), rec.DataDir)
	}
	return nil
}

// damaged is the refusal of a damaged record: nothing changes.
func (m *Manager) damaged(rec *updateRecord, cause error) error {
	copyDir := "the data directory's update-rollback folder"
	if rec != nil && rec.DataDir != "" {
		copyDir = storage.UpdateCopyDir(rec.DataDir)
	}
	return fmt.Errorf("service: the update record %s is damaged (%v); nothing was changed: the previous binary %s and the database copy in %s are kept. "+
		"Check which binary and database work before removing anything by hand", m.recordPath(), cause, m.prevPath(), copyDir)
}

func (m *Manager) writeRecord(rec *updateRecord) error {
	// The writer checks what the reader will: no value the updater
	// writes can make its own record damaged.
	if err := rec.validate(); err != nil {
		return fmt.Errorf("service: the update record would be unreadable (%v); not written", err)
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(m.recordPath(), append(data, '\n'), 0o600)
}

// advance records the next phase before its step.
func (m *Manager) advance(rec *updateRecord, phase string) error {
	rec.Phase = phase
	return m.writeRecord(rec)
}

func (m *Manager) removeRecord() error {
	if err := os.Remove(m.recordPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(m.InstallDir)
}

// manifests are the record's previous manifest and the new one.
func (rec *updateRecord) manifests() (old, next Manifest, err error) {
	if err := json.Unmarshal(rec.OldManifest, &old); err != nil {
		return old, next, err
	}
	next = old
	next.Version = rec.NewVersion
	return old, next, nil
}

// prevState reads the files while the new binary is being placed: whether
// .prev is there, and whether the installed binary is another file.
func (m *Manager) prevState() (present, replaced bool, err error) {
	pi, err := os.Lstat(m.prevPath())
	if errors.Is(err, os.ErrNotExist) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	bi, err := os.Lstat(m.BinaryPath())
	if err != nil {
		return true, false, err
	}
	return true, !os.SameFile(pi, bi), nil
}

// crash is a test hook: it is called at each step and may panic or exit, as
// a process that dies there.
var crash = func(point string) {}

func (m *Manager) note(s string) {
	if m.Note != nil {
		m.Note(s)
	}
}

// Update replaces the installed binary with a new one and restarts the
// service, confirming the new version answers. It holds the installation's
// lock. An unfinished earlier update is resolved first, and Update then
// stops with what happened (run it again), except a kept new version, which
// this update repairs.
func (m *Manager) Update(ctx context.Context, binary string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	unlock, err := m.lock()
	if err != nil {
		return "", err
	}
	defer unlock()
	kept, err := m.readRecord()
	if err != nil {
		return "", err
	}
	if kept != nil && kept.Phase != phaseKeptNew {
		outcome, err := m.resolve(ctx, kept)
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("service: an unfinished update was found and resolved first: %s; run the update again", outcome)
	}
	// Stage first: a candidate that does not run sets nothing aside.
	staged, v, err := m.stage(ctx, binary)
	if err != nil {
		return "", err
	}
	defer os.Remove(staged) // a no-op once in place
	if kept != nil {
		note, err := m.setAside(kept)
		if err != nil {
			return "", err
		}
		m.note(note)
	}
	oldManifest, err := private.ReadFile(m.ManifestPath())
	if err != nil {
		return "", fmt.Errorf("service: no installation to update (%w); run service install", err)
	}
	var old Manifest
	if err := json.Unmarshal(oldManifest, &old); err != nil {
		return "", fmt.Errorf("service: the installed manifest %s is damaged (%v); run service install again", m.ManifestPath(), err)
	}
	if err := old.complete(); err != nil {
		return "", fmt.Errorf("service: the installed manifest %s is incomplete (%v); run service install again", m.ManifestPath(), err)
	}
	dataDir := old.DataDir()
	// A rollback pair that no record explains is never removed: it may be
	// the only working binary or database.
	if present, replaced, err := m.prevState(); err != nil {
		return "", err
	} else if present && replaced {
		return "", fmt.Errorf("service: %s holds a previous binary that no update record explains; check which binary works, remove the other one, and run the update again; nothing was changed", m.prevPath())
	} else if present {
		if err := os.Remove(m.prevPath()); err != nil {
			return "", err
		}
	}
	if step, err := storage.UpdateStep(dataDir); err != nil {
		return "", err
	} else if step != "" {
		return "", fmt.Errorf("service: %s holds an earlier update's database copy (%s) that no update record explains; nothing was changed", storage.UpdateCopyDir(dataDir), step)
	}
	rec := &updateRecord{Version: 1, Phase: phasePrepared, DataDir: dataDir, OldManifest: oldManifest, NewVersion: v, StartedAt: time.Now().UnixMilli()}
	if err := storage.MarkUpdatePending(dataDir, m.InstallDir); err != nil {
		return "", fmt.Errorf("service: marking the update in the data directory (%v); nothing was changed", err)
	}
	if err := m.writeRecord(rec); err != nil {
		_ = storage.ClearUpdatePending(dataDir)
		return "", fmt.Errorf("service: writing the update record (%v); nothing was changed", err)
	}
	crash("recorded")
	// Keep the running binary under a second name until the new one has
	// proved itself.
	if err := os.Link(m.BinaryPath(), m.prevPath()); err != nil {
		return "", m.abort(ctx, rec, fmt.Errorf("the previous binary could not be kept (%w)", err))
	}
	if err := syncDir(m.InstallDir); err != nil {
		return "", m.abort(ctx, rec, err)
	}
	if err := m.advance(rec, phasePreserved); err != nil {
		return "", m.abort(ctx, rec, err)
	}
	crash("preserved")
	// Stop the server and copy its database, for a new version that
	// migrates it and then fails.
	if err := m.stopIfRunning(ctx); err != nil {
		return "", m.abort(ctx, rec, fmt.Errorf("the running server did not stop (%w)", err))
	}
	crash("stopped")
	if err := storage.SaveForUpdate(ctx, dataDir); err != nil {
		return "", m.abort(ctx, rec, fmt.Errorf("the database could not be copied before the update (%w)", err))
	}
	if err := m.advance(rec, phaseSaved); err != nil {
		return "", m.abort(ctx, rec, err)
	}
	crash("saved")
	if err := m.advance(rec, phasePlacing); err != nil {
		return "", m.abort(ctx, rec, err)
	}
	if err := m.place(staged); err != nil {
		return "", m.abort(ctx, rec, fmt.Errorf("the new binary could not be put in place (%w)", err))
	}
	if err := m.advance(rec, phasePlaced); err != nil {
		return "", m.rollback(ctx, rec, err)
	}
	crash("placed")
	if err := m.startNew(ctx, rec); err != nil {
		return "", m.rollback(ctx, rec, err)
	}
	crash("started")
	_, next, err := rec.manifests()
	if err != nil {
		return "", err
	}
	if err := m.ready(ctx, next); err != nil {
		return "", m.rollback(context.WithoutCancel(ctx), rec, err)
	}
	crash("ready")
	if err := m.conclude(ctx, rec, outcomeNew); err != nil {
		return "", err
	}
	return v, nil
}

// Resume resolves an unfinished update, if there is one, and says what
// happened ("" when there was none).
func (m *Manager) Resume(ctx context.Context) (string, error) {
	unlock, err := m.lock()
	if err != nil {
		return "", err
	}
	defer unlock()
	rec, err := m.readRecord()
	if err != nil {
		return "", err
	}
	if rec == nil {
		return m.clearStaleMarker()
	}
	return m.resolve(ctx, rec)
}

// clearStaleMarker removes a data directory's pending marker that no record
// explains: it was written just before a record that never was.
func (m *Manager) clearStaleMarker() (string, error) {
	b, err := private.ReadFile(m.ManifestPath())
	if err != nil {
		return "", nil
	}
	var man Manifest
	if json.Unmarshal(b, &man) != nil || man.DataDir() == "" {
		return "", nil
	}
	if pending, err := storage.UpdatePending(man.DataDir()); err != nil || !pending {
		return "", err
	}
	if err := storage.ClearUpdatePending(man.DataDir()); err != nil {
		return "", err
	}
	return "an update marker without an update record was removed from the data directory", nil
}

// pending refuses a service change while an update is unfinished.
func (m *Manager) pending() error {
	rec, err := m.readRecord()
	if err != nil {
		return err
	}
	if rec != nil {
		return errors.New("service: an update is unfinished; run `nottennapod-server service update` to finish it first")
	}
	return nil
}

// resolve finishes an unfinished update as its record says.
func (m *Manager) resolve(ctx context.Context, rec *updateRecord) (string, error) {
	switch rec.Phase {
	case phaseDone:
		if err := m.cleanup(ctx, rec); err != nil {
			return "", err
		}
		return m.outcome(rec), nil
	case phaseKeptNew:
		return m.resolveKept(ctx, rec)
	case phaseRollingBack:
		return "", m.rollback(ctx, rec, errors.New(rec.Cause))
	case phasePrepared, phasePreserved, phaseSaved:
		return m.undo(ctx, rec)
	case phasePlacing:
		present, replaced, err := m.prevState()
		switch {
		case err != nil:
			return "", err
		case !present:
			return "", m.missingPrev(rec)
		case !replaced:
			return m.undo(ctx, rec)
		}
	}
	// The new binary is installed.
	if _, err := os.Lstat(m.prevPath()); err != nil {
		return "", m.missingPrev(rec)
	}
	if !m.Status(ctx).Running {
		if err := m.startNew(ctx, rec); err != nil {
			return "", m.rollback(ctx, rec, err)
		}
	}
	_, next, _ := rec.manifests()
	if err := m.ready(ctx, next); err != nil {
		return "", m.rollback(context.WithoutCancel(ctx), rec, err)
	}
	if err := m.conclude(ctx, rec, outcomeNew); err != nil {
		return "", err
	}
	return m.outcome(rec), nil
}

// missingPrev refuses to go on without the previous binary, once the new one
// may be installed: nothing else changes.
func (m *Manager) missingPrev(rec *updateRecord) error {
	return fmt.Errorf("service: the update to %s replaced the binary, and the previous binary %s is missing; nothing more was changed: "+
		"the database copy in %s and the update record are kept. Put the previous binary back as %s, then run service update again",
		rec.NewVersion, m.prevPath(), storage.UpdateCopyDir(rec.DataDir), m.prevPath())
}

// undo ends an update that never replaced the binary.
func (m *Manager) undo(ctx context.Context, rec *updateRecord) (string, error) {
	if err := m.conclude(ctx, rec, outcomeAborted); err != nil {
		return "", err
	}
	return m.outcome(rec), nil
}

// abort undoes an update during the run, before its binary was replaced.
func (m *Manager) abort(ctx context.Context, rec *updateRecord, cause error) error {
	rec.Cause = cause.Error()
	if err := m.conclude(ctx, rec, outcomeAborted); err != nil {
		return err
	}
	return fmt.Errorf("service: %v; nothing was changed and the server is running", cause)
}

// conclude records the outcome, then cleans up.
func (m *Manager) conclude(ctx context.Context, rec *updateRecord, outcome string) error {
	rec.Phase, rec.Outcome = phaseDone, outcome
	if err := m.writeRecord(rec); err != nil {
		return fmt.Errorf("service: the update's outcome could not be recorded (%v); the previous binary and the database copy are kept; run service update again", err)
	}
	crash("outcome")
	return m.cleanup(ctx, rec)
}

// cleanup ends a done update: the pair, the marker, then the record, each
// step repeatable and none stopping a server. An undone update's server is
// started if it is not running; if it does not start, the record stays.
func (m *Manager) cleanup(ctx context.Context, rec *updateRecord) error {
	if err := storage.DiscardUpdateCopy(rec.DataDir); err != nil {
		return err
	}
	crash("discarded")
	for _, p := range []string{m.prevPath(), m.restorePath()} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	crash("prev-removed")
	if rec.Outcome == outcomeAborted {
		// The undone update ends with the previous version answering, as a
		// rollback does: a start the service manager accepted proves nothing. Until then the record
		// stays, so the next run tries again.
		old, _, _ := rec.manifests()
		failed := func(err error) error {
			why, state := "", "it is stopped"
			if rec.Cause != "" {
				why = " (" + rec.Cause + ")"
			}
			if m.Status(ctx).Running {
				state = "it runs but does not answer"
			}
			return fmt.Errorf("service: the update to %s was undone%s, but the server is not running: %w; %s. Look at the log in %s, then run `nottennapod-server service update` to try starting it again",
				rec.NewVersion, why, err, state, m.LogDir)
		}
		if !m.Status(ctx).Running {
			if err := storage.ClearInstance(rec.DataDir); err != nil {
				return failed(err)
			}
			if err := m.start(ctx); err != nil {
				return failed(err)
			}
		}
		if err := m.ready(ctx, old); err != nil {
			return failed(err)
		}
	}
	if err := storage.ClearUpdatePending(rec.DataDir); err != nil {
		return err
	}
	crash("marker-cleared")
	return m.removeRecord()
}

// outcome says how a done update ended.
func (m *Manager) outcome(rec *updateRecord) string {
	switch rec.Outcome {
	case outcomeNew:
		return fmt.Sprintf("the update to %s is finished and the new version answers", rec.NewVersion)
	case outcomeOld:
		return fmt.Sprintf("the update to %s failed (%s); the previous version and its database are back and running", rec.NewVersion, rec.Cause)
	}
	return fmt.Sprintf("the update to %s never replaced the binary; it was undone and the server started again", rec.NewVersion)
}

// resolveKept retries a kept new version: it is started if it is
// stopped, and the update finishes once it answers.
func (m *Manager) resolveKept(ctx context.Context, rec *updateRecord) (string, error) {
	if rec.Aside != nil {
		note, err := m.setAside(rec)
		if err != nil {
			return "", err
		}
		return note + "; run the update again", nil
	}
	_, next, _ := rec.manifests()
	if !m.Status(ctx).Running {
		if err := storage.ClearInstance(rec.DataDir); err != nil {
			return "", err
		}
		if err := m.start(ctx); err != nil {
			return "", fmt.Errorf("service: the update to %s, kept because it had accepted changes, did not start (%v); install a fixed version over it with `nottennapod-server service update FILE`",
				rec.NewVersion, err)
		}
	}
	if err := m.ready(ctx, next); err != nil {
		return "", fmt.Errorf("service: the update to %s accepted changes before it failed (%s), so it was kept, and it still does not answer (%v). "+
			"Install a fixed version over it with `nottennapod-server service update FILE`; the previous binary %s and its database copy in %s can no longer undo the accepted changes. Look at the log in %s",
			rec.NewVersion, rec.Cause, err, m.prevPath(), storage.UpdateCopyDir(rec.DataDir), m.LogDir)
	}
	if err := m.conclude(ctx, rec, outcomeNew); err != nil {
		return "", err
	}
	return fmt.Sprintf("the update to %s, kept because it had accepted changes, now answers; it is finished", rec.NewVersion), nil
}

// setAside closes a kept update so a new one can repair it forwards: its
// pair, which can no longer undo the changes the kept version accepted, is
// moved aside and kept, never deleted; the installed manifest is the kept
// version's. Each step can be repeated.
func (m *Manager) setAside(rec *updateRecord) (string, error) {
	if rec.Aside == nil {
		stamp := time.Now().UTC().Format("20060102-150405")
		rec.Aside = &asidePair{Binary: m.BinaryPath() + ".kept-" + stamp, Copy: storage.UpdateCopyDir(rec.DataDir) + ".kept-" + stamp}
		if err := m.writeRecord(rec); err != nil {
			return "", err
		}
	}
	if _, err := os.Lstat(m.prevPath()); err == nil {
		if _, err := os.Lstat(rec.Aside.Binary); err == nil {
			return "", fmt.Errorf("service: %s is in the way of the kept previous binary; nothing was changed", rec.Aside.Binary)
		}
		if err := os.Rename(m.prevPath(), rec.Aside.Binary); err != nil {
			return "", err
		}
		if err := syncDir(m.InstallDir); err != nil {
			return "", err
		}
	}
	crash("aside-binary")
	if err := storage.SetAsideUpdateCopy(rec.DataDir, filepath.Base(rec.Aside.Copy)); err != nil {
		return "", err
	}
	crash("aside-copy")
	_, next, _ := rec.manifests()
	mb, _ := json.MarshalIndent(next, "", "  ")
	if err := writeAtomic(m.ManifestPath(), mb, 0o600); err != nil {
		return "", err
	}
	if err := storage.ClearUpdatePending(rec.DataDir); err != nil {
		return "", err
	}
	if err := m.removeRecord(); err != nil {
		return "", err
	}
	return fmt.Sprintf("the earlier update to %s accepted changes, so its rollback pair can no longer undo them; it is kept aside: the previous binary as %s and its database copy in %s",
		rec.NewVersion, rec.Aside.Binary, rec.Aside.Copy), nil
}

// startNew writes the new manifest and starts the new version.
func (m *Manager) startNew(ctx context.Context, rec *updateRecord) error {
	_, next, err := rec.manifests()
	if err != nil {
		return err
	}
	mb, _ := json.MarshalIndent(next, "", "  ")
	if err := writeAtomic(m.ManifestPath(), mb, 0o600); err != nil {
		return err
	}
	crash("manifest")
	if err := storage.ClearInstance(rec.DataDir); err != nil {
		return err
	}
	return m.start(ctx)
}

// rollback stops the new version and, if it accepted nothing, puts the
// previous binary, manifest and database back and checks the previous
// version answers. Each step is recorded first and can be
// repeated; the pair stays until the outcome is recorded.
func (m *Manager) rollback(ctx context.Context, rec *updateRecord, cause error) error {
	old, _, err := rec.manifests()
	if err != nil {
		return err
	}
	if rec.Phase != phaseRollingBack {
		rec.Phase, rec.Cause = phaseRollingBack, cause.Error()
		if err := m.writeRecord(rec); err != nil {
			return fmt.Errorf("service: the update failed (%v), and the rollback could not be recorded: %w; nothing more was changed", cause, err)
		}
	}
	if _, err := os.Lstat(m.prevPath()); err != nil {
		return m.missingPrev(rec)
	}
	keep := fmt.Sprintf("the previous binary is kept as %s and its database copy in %s", m.prevPath(), storage.UpdateCopyDir(rec.DataDir))
	if err := m.stopIfRunning(ctx); err != nil {
		return fmt.Errorf("service: the update failed (%v), and the new version did not stop (%v); %s; run service update again", cause, err, keep)
	}
	err = storage.BeginRollback(ctx, rec.DataDir)
	if errors.Is(err, storage.ErrLocked) {
		// The service manager restarted the failing version meanwhile.
		if err = m.stopIfRunning(ctx); err == nil {
			err = storage.BeginRollback(ctx, rec.DataDir)
		}
	}
	switch {
	case errors.Is(err, storage.ErrUpdateAccepted):
		rec.Phase = phaseKeptNew
		if werr := m.writeRecord(rec); werr != nil {
			return fmt.Errorf("service: the update failed (%v), and its record could not be updated: %w", cause, werr)
		}
		crash("kept-new")
		started := "and was started again"
		if serr := m.start(ctx); serr != nil {
			started = fmt.Sprintf("but did not start again (%v)", serr)
		}
		return fmt.Errorf("service: the update failed (%v). The new version had already accepted changes, so it was not rolled back: "+
			"putting the previous database back would lose them. The new version stays installed %s; %s, but they can no longer undo the accepted changes. "+
			"Run service update again once it answers, or install a fixed version over it with `nottennapod-server service update FILE`. Look at the log in %s",
			cause, started, keep, m.LogDir)
	case err != nil:
		return fmt.Errorf("service: the update failed (%v), and the rollback cannot go on (%w); the new version stays installed and stopped; %s",
			cause, err, keep)
	}
	crash("rollback-begun")
	if present, replaced, err := m.prevState(); err != nil {
		return err
	} else if !present {
		return m.missingPrev(rec)
	} else if replaced {
		// A second name for .prev, renamed over the binary: .prev stays.
		if err := os.Remove(m.restorePath()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Link(m.prevPath(), m.restorePath()); err != nil {
			return fmt.Errorf("service: the update failed (%v), and putting the previous binary back failed (%w); %s", cause, err, keep)
		}
		if err := os.Rename(m.restorePath(), m.BinaryPath()); err != nil {
			return fmt.Errorf("service: the update failed (%v), and putting the previous binary back failed (%w); %s", cause, err, keep)
		}
		if err := syncDir(m.InstallDir); err != nil {
			return err
		}
	}
	if err := writeAtomic(m.ManifestPath(), rec.OldManifest, 0o600); err != nil {
		return fmt.Errorf("service: the update failed (%v), and putting the previous manifest back failed (%w); %s", cause, err, keep)
	}
	crash("rollback-binary")
	if err := storage.BinaryRestored(rec.DataDir); err != nil {
		return fmt.Errorf("service: the update failed (%v), and the rollback could not be recorded (%w); %s", cause, err, keep)
	}
	crash("rollback-binary-recorded")
	if err := storage.FinishRollback(ctx, rec.DataDir); err != nil {
		return fmt.Errorf("service: the update failed (%v), and the database copy could not be put back (%w); %s; run service update again", cause, err, keep)
	}
	crash("rollback-database")
	if err := storage.ClearInstance(rec.DataDir); err != nil {
		return err
	}
	if err := m.start(ctx); err != nil {
		return fmt.Errorf("service: the update failed (%v); the previous version and its database are back but did not start (%v); %s; run service update again", cause, err, keep)
	}
	if err := m.ready(ctx, old); err != nil {
		return fmt.Errorf("service: the update failed (%v); the previous version and its database are back, but %v; %s; look at the log in %s and run service update again",
			cause, err, keep, m.LogDir)
	}
	crash("rollback-ready")
	if err := m.conclude(ctx, rec, outcomeOld); err != nil {
		return err
	}
	return fmt.Errorf("service: the update failed (%v); the previous version and its database are back and running", cause)
}

// ready waits until the service answers /v1/info with the manifest's version
// and the instance ID its data directory holds: the process just started,
// not another one on the port. It fails when it has nothing to check with.
func (m *Manager) ready(ctx context.Context, man Manifest) error {
	if m.Info == nil {
		return errors.New("no readiness check is configured")
	}
	if err := man.complete(); err != nil {
		return fmt.Errorf("the manifest cannot be checked: %v", err)
	}
	wait := m.HealthWait
	if wait <= 0 {
		wait = 30 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		info, err := m.Info(ctx, man)
		if err == nil {
			id, ierr := storage.ReadInstance(man.DataDir())
			switch {
			case info.Version != man.Version:
				err = fmt.Errorf("it answers with version %q, not %s", info.Version, man.Version)
			case ierr != nil:
				err = fmt.Errorf("it has not written its instance ID (%v)", ierr)
			case info.InstanceID == "" || info.InstanceID != id:
				err = errors.New("the server answering is not the one just started (another instance ID)")
			default:
				return nil
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("the restarted server is not ready: %w", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
}
