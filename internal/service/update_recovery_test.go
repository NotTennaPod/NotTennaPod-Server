package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The update transaction's recovery. The service manager is a fake
// throughout; nothing is installed.

// cutAt runs an update that dies at point (a panic, as a process exit).
func cutAt(t *testing.T, m *Manager, src, point string) {
	t.Helper()
	hit := false
	crash = func(p string) {
		if p == point {
			hit = true
			panic("cut")
		}
	}
	func() {
		defer func() { recover() }()
		m.Update(context.Background(), src)
	}()
	crash = func(string) {}
	if !hit {
		t.Fatalf("the cut at %s was not reached", point)
	}
}

func accountCount(t *testing.T, data string) int {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(data, "nottennapod.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM accounts").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func acceptAccount(t *testing.T, data string) {
	t.Helper()
	s, err := storage.Open(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), func(w *storage.WriteTx) error { _, err := w.CreateAccount(); return err }); err != nil {
		t.Fatal(err)
	}
}

func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// A rollback that died in its cleanup, after the outcome was recorded,
// finishes without stopping the recovered server, as often as it is retried.
func TestARollbackCutInItsCleanupFinishes(t *testing.T) {
	for _, point := range []string{"outcome", "discarded", "prev-removed", "marker-cleared"} {
		t.Run(point, func(t *testing.T) {
			m, f, src := newFakeService(t)
			f.start = migratingCandidate(t, f, false)
			cutAt(t, m, src, point)
			for i := 0; i < 2; i++ {
				outcome, err := m.Resume(context.Background())
				if err != nil {
					t.Fatalf("retry %d: %v", i, err)
				}
				if i == 0 && !strings.Contains(outcome, "previous version and its database are back") {
					t.Fatalf("retry %d reports %q", i, outcome)
				}
				if f.running != "working old executable" {
					t.Fatalf("retry %d: running %q", i, f.running)
				}
			}
			if exists(m.recordPath()) {
				t.Fatal("the finished rollback is still pending")
			}
		})
	}
}

// A successful update interrupted in its cleanup reports its recorded
// outcome, even with .prev already gone.
func TestASuccessfulUpdateCutInItsCleanupReportsItsOutcome(t *testing.T) {
	m, f, src := newFakeService(t)
	cutAt(t, m, src, "discarded")
	os.Remove(m.prevPath())
	outcome, err := m.Resume(context.Background())
	if err != nil || !strings.Contains(outcome, "is finished and the new version answers") {
		t.Fatalf("got %q %v", outcome, err)
	}
	if f.running != "new executable" || exists(m.recordPath()) {
		t.Fatalf("running %q, record %v", f.running, exists(m.recordPath()))
	}
}

// A missing .prev after the binary was replaced decides nothing: the
// database copy and the record stay.
func TestAMissingPrevKeepsEverythingElse(t *testing.T) {
	for _, point := range []string{"placed", "started", "rollback-begun"} {
		t.Run(point, func(t *testing.T) {
			m, f, src := newFakeService(t)
			f.start = migratingCandidate(t, f, false)
			cutAt(t, m, src, point)
			os.Remove(m.prevPath())
			_, err := m.Resume(context.Background())
			if err == nil || !strings.Contains(err.Error(), "is missing") {
				t.Fatalf("got %v", err)
			}
			if !exists(filepath.Join(storage.UpdateCopyDir(f.data), "nottennapod.db")) || !exists(m.recordPath()) {
				t.Fatal("the database copy or the record went")
			}
		})
	}
}

// A damaged record changes nothing and says so; readiness fails closed.
func TestADamagedRecordChangesNothing(t *testing.T) {
	for name, damage := range map[string]func(r map[string]any){
		"null manifest":     func(r map[string]any) { r["old_manifest"] = nil },
		"no listen address": func(r map[string]any) { r["old_manifest"].(map[string]any)["listen"] = "" },
		"other data dir":    func(r map[string]any) { r["data_dir"] = "/elsewhere" },
		"no new version":    func(r map[string]any) { r["new_version"] = "" },
		"unknown phase":     func(r map[string]any) { r["phase"] = "nonsense" },
		"not JSON":          nil,
	} {
		t.Run(name, func(t *testing.T) {
			m, f, src := newFakeService(t)
			f.start = migratingCandidate(t, f, false)
			cutAt(t, m, src, "placed")
			if damage == nil {
				os.WriteFile(m.recordPath(), []byte("{"), 0o600)
			} else {
				var r map[string]any
				b, _ := os.ReadFile(m.recordPath())
				json.Unmarshal(b, &r)
				damage(r)
				b, _ = json.Marshal(r)
				os.WriteFile(m.recordPath(), b, 0o600)
			}
			before, _ := os.ReadFile(m.recordPath())
			for _, try := range []func() error{
				func() error { _, err := m.Resume(context.Background()); return err },
				func() error { _, err := m.Update(context.Background(), src); return err },
			} {
				if err := try(); err == nil || !strings.Contains(err.Error(), "damaged") {
					t.Fatalf("got %v", err)
				}
			}
			after, _ := os.ReadFile(m.recordPath())
			if string(after) != string(before) || !exists(m.prevPath()) || !exists(filepath.Join(storage.UpdateCopyDir(f.data), "nottennapod.db")) {
				t.Fatal("a damaged record changed something")
			}
		})
	}
	m, _, _ := newFakeService(t)
	m.Info = func(context.Context, Manifest) (ServerInfo, error) { return ServerInfo{}, nil }
	if err := m.ready(context.Background(), Manifest{Version: "1"}); err == nil {
		t.Fatal("readiness passed with nothing to check")
	}
	m.Info = nil
	if err := m.ready(context.Background(), Manifest{Version: "1", Listen: "127.0.0.1:1", Args: []string{"--data-dir", "/x"}}); err == nil {
		t.Fatal("readiness passed without a check")
	}
}

// Restore refuses at every point of a pending update, its first steps
// and its cleanup included; install and uninstall too.
func TestRestoreRefusesThroughoutAPendingUpdate(t *testing.T) {
	for _, point := range []string{"recorded", "preserved", "stopped", "saved", "placed", "started", "rollback-begun", "outcome", "discarded", "prev-removed"} {
		t.Run(point, func(t *testing.T) {
			m, f, src := newFakeService(t)
			if strings.HasPrefix(point, "rollback-") {
				f.start = migratingCandidate(t, f, false)
			}
			cutAt(t, m, src, point)
			_, err := storage.Restore(context.Background(), storage.RestoreOptions{DataDir: f.data, Backup: filepath.Join(t.TempDir(), "absent")})
			if err == nil || !strings.Contains(err.Error(), "update is unfinished") {
				t.Fatalf("restore: %v", err)
			}
			if err := m.Install(context.Background(), src, plan(t.TempDir()), Manifest{}); err == nil || !strings.Contains(err.Error(), "unfinished") {
				t.Fatalf("install: %v", err)
			}
			if err := m.Uninstall(context.Background()); err == nil || !strings.Contains(err.Error(), "unfinished") {
				t.Fatalf("uninstall: %v", err)
			}
			// Once resolved, the marker is gone.
			m.Resume(context.Background())
			if pending, _ := storage.UpdatePending(f.data); pending {
				t.Fatal("the marker outlived the update")
			}
		})
	}
}

// keptNew leaves an update kept: the new version accepted an account, then
// never answered.
func keptNew(t *testing.T) (*Manager, *fakeService, string) {
	t.Helper()
	m, f, src := newFakeService(t)
	accepted := false
	m.Info = func(context.Context, Manifest) (ServerInfo, error) {
		if !accepted {
			acceptAccount(t, f.data)
			accepted = true
		}
		return ServerInfo{}, errors.New("the new version fails after accepting an account")
	}
	if _, err := m.Update(context.Background(), src); err == nil || !strings.Contains(err.Error(), "already accepted changes") {
		t.Fatalf("setup: %v", err)
	}
	if rec, err := m.readRecord(); err != nil || rec == nil || rec.Phase != phaseKeptNew {
		t.Fatalf("record %+v %v", rec, err)
	}
	return m, f, src
}

// A kept new version is repaired forwards: the fixed binary is staged
// and installed over it, from its database; the earlier pair is set aside,
// kept and named.
func TestAKeptUpdateIsRepairedForwards(t *testing.T) {
	m, f, _ := keptNew(t)
	m.Info = f.info
	f.running = ""
	fixed := filepath.Join(t.TempDir(), "fixed")
	os.WriteFile(fixed, []byte("fixed executable"), 0o700)
	staged := false
	m.VersionOf = func(context.Context, string) (string, error) { staged = true; return "3", nil }
	var notes []string
	m.Note = func(s string) { notes = append(notes, s) }
	base := f.info
	m.Info = func(ctx context.Context, man Manifest) (ServerInfo, error) {
		if f.running == "fixed executable" {
			return ServerInfo{Version: "3", InstanceID: f.id}, nil
		}
		return base(ctx, man)
	}
	v, err := m.Update(context.Background(), fixed)
	if err != nil || v != "3" || !staged {
		t.Fatalf("the repair: %q %v (staged %v)", v, err, staged)
	}
	if f.running != "fixed executable" || accountCount(t, f.data) != 1 {
		t.Fatalf("running %q, %d accounts", f.running, accountCount(t, f.data))
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "kept aside") {
		t.Fatalf("notes %q", notes)
	}
	asideBin, _ := filepath.Glob(m.BinaryPath() + ".kept-*")
	asideCopy, _ := filepath.Glob(storage.UpdateCopyDir(f.data) + ".kept-*")
	if len(asideBin) != 1 || len(asideCopy) != 1 || !strings.Contains(notes[0], asideBin[0]) || !strings.Contains(notes[0], asideCopy[0]) {
		t.Fatalf("the set-aside pair: %v %v, note %q", asideBin, asideCopy, notes[0])
	}
	if b, _ := os.ReadFile(asideBin[0]); string(b) != "working old executable" {
		t.Fatalf("the set-aside binary is %q", b)
	}
	if exists(m.recordPath()) || exists(m.prevPath()) {
		t.Fatal("the repair left a record or .prev")
	}
}

// A retry of a kept update starts it when it is stopped, and finishes
// once it answers.
func TestARetryStartsAStoppedKeptUpdate(t *testing.T) {
	m, f, _ := keptNew(t)
	f.running = ""
	m.Info = f.info
	outcome, err := m.Resume(context.Background())
	if err != nil || f.running != "new executable" || !strings.Contains(outcome, "now answers") {
		t.Fatalf("got %q %v, running %q", outcome, err, f.running)
	}
	if accountCount(t, f.data) != 1 {
		t.Fatal("the accepted account went")
	}
}

// An undone update whose server does not start says so, with the real
// error, shows it stopped, and keeps the record to try again.
func TestAnUndoneUpdateReportsAFailedStart(t *testing.T) {
	m, f, src := newFakeService(t)
	cutAt(t, m, src, "stopped")
	run := m.Run
	refuse := true
	m.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "launchctl" && args[0] == "kickstart" && refuse {
			return nil, errors.New("service manager rejected start")
		}
		return run(ctx, name, args...)
	}
	_, err := m.Resume(context.Background())
	if err == nil || !strings.Contains(err.Error(), "service manager rejected start") || !strings.Contains(err.Error(), "it is stopped") {
		t.Fatalf("got %v", err)
	}
	if f.running != "" || !exists(m.recordPath()) {
		t.Fatalf("running %q, record %v", f.running, exists(m.recordPath()))
	}
	refuse = false
	if outcome, err := m.Resume(context.Background()); err != nil || !strings.Contains(outcome, "never replaced") || f.running != "working old executable" {
		t.Fatalf("the retry: %q %v, running %q", outcome, err, f.running)
	}
}

// A candidate that is not a regular file (a FIFO) is refused at once,
// without waiting for a writer, and a cancelled update does nothing.
func TestAFIFOCandidateIsRefusedAtOnce(t *testing.T) {
	m, f, _ := newFakeService(t)
	fifo := filepath.Join(t.TempDir(), "candidate")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := m.Update(context.Background(), fifo); done <- err }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(5 * time.Second):
		w, _ := os.OpenFile(fifo, os.O_WRONLY, 0)
		w.Close()
		t.Fatal("the update waited on the FIFO")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Update(ctx, fifo); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled update: %v", err)
	}
	if f.running != "working old executable" {
		t.Fatalf("running %q", f.running)
	}
}

// process plays the fake service across processes: the running state is a
// file in the installation directory.
type processFixture struct {
	Layout         Layout
	Data, Src, Cut string
	CandidateWorks bool
}

func processManager(t *testing.T, p processFixture) (*Manager, *fakeService) {
	m := &Manager{Layout: p.Layout, UID: 501, HealthWait: time.Millisecond}
	f := &fakeService{m: m, data: p.Data}
	state := filepath.Join(m.InstallDir, "fake-running")
	b, _ := os.ReadFile(state)
	f.running = string(b)
	f.id, _ = storage.ReadInstance(f.data)
	f.start = migratingCandidate(t, f, p.CandidateWorks)
	m.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		b, err := f.run(ctx, name, args...)
		os.WriteFile(state, []byte(f.running), 0o600)
		return b, err
	}
	m.Info = f.info
	m.VersionOf = func(context.Context, string) (string, error) { return "2", nil }
	return m, f
}

// With real process exits, an updater that exits at any step of
// an update or its rollback leaves what the next run resolves, with a
// server running and its database readable.
func TestAnUpdaterExitAtEveryStepIsResolved(t *testing.T) {
	if path := os.Getenv("NTP_UPDATE_EXIT_FIXTURE"); path != "" {
		var p processFixture
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(b, &p)
		m, _ := processManager(t, p)
		crash = func(s string) {
			if s == p.Cut {
				os.Exit(86)
			}
		}
		_, err = m.Update(context.Background(), p.Src)
		t.Fatalf("no exit at %s: %v", p.Cut, err)
	}
	for _, works := range []bool{false, true} {
		points := []string{"recorded", "preserved", "stopped", "saved", "placed", "manifest", "started", "ready", "outcome", "discarded", "prev-removed", "marker-cleared"}
		if !works {
			points = []string{"recorded", "preserved", "stopped", "saved", "placed", "manifest", "started", "rollback-begun", "rollback-binary",
				"rollback-binary-recorded", "rollback-database", "rollback-ready", "outcome", "discarded", "prev-removed", "marker-cleared"}
		}
		for _, point := range points {
			t.Run(fmt.Sprintf("works=%v/%s", works, point), func(t *testing.T) {
				m, f, src := newFakeService(t)
				p := processFixture{m.Layout, f.data, src, point, works}
				os.WriteFile(filepath.Join(m.InstallDir, "fake-running"), []byte(f.running), 0o600)
				path := filepath.Join(t.TempDir(), "fixture.json")
				b, _ := json.Marshal(p)
				os.WriteFile(path, b, 0o600)
				c := exec.Command(os.Args[0], "-test.run=^TestAnUpdaterExitAtEveryStepIsResolved$")
				c.Env = append(os.Environ(), "NTP_UPDATE_EXIT_FIXTURE="+path)
				out, err := c.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 86 {
					t.Fatalf("the updater did not exit at %s: %v %s", point, err, out)
				}
				mm, ff := processManager(t, p)
				outcome, err := mm.Resume(context.Background())
				if exists(mm.recordPath()) {
					t.Fatalf("the update is still pending: %q %v", outcome, err)
				}
				if ff.running == "" {
					t.Fatalf("no server runs: %q %v", outcome, err)
				}
				if !works && ff.running != "working old executable" {
					t.Fatalf("running %q after %q %v", ff.running, outcome, err)
				}
				if !works {
					s, err := storage.Open(context.Background(), f.data)
					if err != nil {
						t.Fatalf("the previous version cannot open its database: %v", err)
					}
					s.Close()
				}
			})
		}
	}
}

// The service manager accepts the restart of an undone update but the server
// dies starting; the undo says so, keeps its record, and the next run starts
// it.
func TestAnUndoneUpdateWhoseServerDiesKeepsItsRecord(t *testing.T) {
	m, f, src := newFakeService(t)
	m.HealthWait = time.Nanosecond
	cutAt(t, m, src, "stopped")
	f.start = func(string) error { return errors.New("server exits during startup") }
	outcome, err := m.Resume(context.Background())
	if err == nil || strings.Contains(outcome, "started again") || !strings.Contains(err.Error(), "it is stopped") {
		t.Fatalf("got %q %v", outcome, err)
	}
	if !exists(m.recordPath()) {
		t.Fatal("the record went although the server is not running")
	}
	f.start = nil
	if outcome, err := m.Resume(context.Background()); err != nil || f.running != "working old executable" || exists(m.recordPath()) {
		t.Fatalf("the retry: %q %v, running %q", outcome, err, f.running)
	}
}

// A candidate that reports no usable version is refused before anything is
// recorded, and nothing the updater writes can make its own record damaged.
func TestAnUnusableVersionIsRefusedBeforeAnyRecord(t *testing.T) {
	for _, v := range []string{"", " ", "2\n3", "a b", strings.Repeat("9", 200)} {
		m, f, src := newFakeService(t)
		m.VersionOf = func(context.Context, string) (string, error) { return v, nil }
		_, err := m.Update(context.Background(), src)
		if err == nil || !strings.Contains(err.Error(), "no usable version") {
			t.Fatalf("version %q: %v", v, err)
		}
		if exists(m.recordPath()) || exists(m.prevPath()) {
			t.Fatalf("version %q left a record or .prev", v)
		}
		if pending, _ := storage.UpdatePending(f.data); pending || f.running != "working old executable" {
			t.Fatalf("version %q: pending %v, running %q", v, pending, f.running)
		}
	}
	m, _, _ := newFakeService(t)
	rec := &updateRecord{Version: 1, Phase: phasePrepared, DataDir: "/data", OldManifest: json.RawMessage(`{"version":"1","listen":"127.0.0.1:1","args":["--data-dir","/data"]}`)}
	if err := m.writeRecord(rec); err == nil || exists(m.recordPath()) {
		t.Fatalf("a record without a new version was written: %v", err)
	}
}
