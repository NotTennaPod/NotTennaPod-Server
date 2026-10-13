package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Update safety. Nothing here installs a service: launchd and the server are played by fakes.

// fakeService plays launchd and the server for update's tests. kickstart
// starts the installed binary, which writes a new instance ID as serve does
// (or fails, as start says); kill stops it; print reports it; info answers as
// the running binary.
type fakeService struct {
	m       *Manager
	data    string
	running string // the running binary's content; "" when stopped
	id      string
	start   func(binary string) error
}

func (f *fakeService) run(_ context.Context, name string, args ...string) ([]byte, error) {
	switch {
	case name == "launchctl" && args[0] == "kill":
		f.running = ""
	case name == "launchctl" && args[0] == "kickstart":
		b, _ := os.ReadFile(f.m.BinaryPath())
		f.running = ""
		if f.start != nil {
			if err := f.start(string(b)); err != nil {
				return nil, nil // launchd accepted the start; the process failed
			}
		}
		id, err := storage.NewInstance(f.data)
		if err != nil {
			return nil, err
		}
		f.running, f.id = string(b), id
	case name == "launchctl" && args[0] == "print":
		if f.running == "" {
			return []byte("x = {\n\tstate = not running\n}\n"), nil
		}
		return []byte("x = {\n\tstate = running\n\tpid = 9\n}\n"), nil
	}
	return nil, nil
}

func (f *fakeService) info(context.Context, Manifest) (ServerInfo, error) {
	switch f.running {
	case "":
		return ServerInfo{}, errors.New("connection refused")
	case "working old executable":
		return ServerInfo{Version: "1", InstanceID: f.id}, nil
	}
	return ServerInfo{Version: "2", InstanceID: f.id}, nil
}

// newFakeService is an installation whose old version is running.
func newFakeService(t *testing.T) (*Manager, *fakeService, string) {
	t.Helper()
	m, src := installedFor(t, nil)
	b, _ := os.ReadFile(m.ManifestPath())
	f := &fakeService{m: m, running: "working old executable"}
	var man Manifest
	if err := json.Unmarshal(b, &man); err != nil {
		t.Fatal(err)
	}
	f.data = man.DataDir()
	// The running server wrote its instance ID when it started.
	id, err := storage.NewInstance(f.data)
	if err != nil {
		t.Fatal(err)
	}
	f.id = id
	m.Run, m.Info = f.run, f.info
	m.AgentFile = filepath.Join(t.TempDir(), "agent.plist")
	os.WriteFile(m.AgentFile, []byte("x"), 0o600)
	return m, f, src
}

// Staging files are created exclusively under random names, so
// planted symlinks at the old fixed names are never followed.
func TestStagingDoesNotFollowPlantedFiles(t *testing.T) {
	m, _, src := newFakeService(t)
	victim := filepath.Join(t.TempDir(), "valuable-file")
	os.WriteFile(victim, []byte("valuable"), 0o600)
	os.Symlink(victim, m.BinaryPath()+".new")
	os.Symlink(victim, m.ManifestPath()+".tmp")
	// The installation directory holds no symlinks: the update refuses
	// and writes nothing through them.
	if _, err := m.Update(context.Background(), src); !errors.Is(err, private.ErrSymlink) {
		t.Fatalf("an installation directory with planted symlinks: %v", err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "valuable" {
		t.Fatalf("the update wrote through a planted symlink: %q", b)
	}
	if err := writeAtomic(filepath.Join(m.InstallDir, "service.json"), []byte("manifest"), 0o600); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "valuable" {
		t.Fatalf("writeAtomic wrote through a planted symlink: %q", b)
	}
}

// An existing installation directory open to others is made
// private by install.
func TestInstallMakesTheInstallationDirectoryPrivate(t *testing.T) {
	m, home := manager(t, "darwin", &recorder{})
	os.MkdirAll(m.InstallDir, 0o777)
	os.Chmod(m.InstallDir, 0o777)
	if err := m.Install(context.Background(), binary(t, "1"), plan(home), Manifest{}); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(m.InstallDir); fi.Mode().Perm() != 0o700 {
		t.Fatalf("the installation directory is %04o", fi.Mode().Perm())
	}
}

// A second service change while an update runs is refused, and
// the first update's rollback keeps the working binary.
func TestConcurrentServiceChangesAreRefused(t *testing.T) {
	m, _, src := newFakeService(t)
	ready, release := make(chan struct{}), make(chan struct{})
	var once bool
	m.Info = func(ctx context.Context, man Manifest) (ServerInfo, error) {
		if !once {
			once = true
			close(ready)
			<-release
		}
		return ServerInfo{}, errors.New("the candidate cannot start")
	}
	done := make(chan error, 1)
	go func() { _, err := m.Update(context.Background(), src); done <- err }()
	<-ready
	other := *m
	other.VersionOf = func(context.Context, string) (string, error) { return "", errors.New("bad executable") }
	if _, err := other.Update(context.Background(), binary(t, "bad second candidate")); err == nil || !strings.Contains(err.Error(), "another service command") {
		t.Errorf("a second update during the first: %v", err)
	}
	if err := other.Stop(context.Background()); err == nil || !strings.Contains(err.Error(), "another service command") {
		t.Errorf("a stop during an update: %v", err)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("the failing update succeeded")
	}
	if b, _ := os.ReadFile(m.BinaryPath()); string(b) != "working old executable" {
		t.Fatalf("the working binary was lost: %q", b)
	}
}

// Nothing is replaced unless the previous binary is kept.
func TestUpdateIsRefusedUnlessThePreviousBinaryIsKept(t *testing.T) {
	m, f, src := newFakeService(t)
	os.Mkdir(m.BinaryPath()+".prev", 0o700)
	os.WriteFile(filepath.Join(m.BinaryPath()+".prev", "keep"), []byte("x"), 0o600)
	_, err := m.Update(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "nothing was changed") {
		t.Fatalf("got %v", err)
	}
	if b, _ := os.ReadFile(m.BinaryPath()); string(b) != "working old executable" {
		t.Fatalf("the candidate was installed: %q", b)
	}
	if f.running != "working old executable" {
		t.Fatalf("the running server was touched: %q", f.running)
	}
}

// A candidate that migrates the database and then fails gets
// the previous binary and the database copy back, and the previous version
// can open it and is checked running.
func TestRollbackPutsBackTheDatabaseTheCandidateMigrated(t *testing.T) {
	m, f, src := newFakeService(t)
	f.start = func(bin string) error {
		if bin == "working old executable" {
			return nil
		}
		// A newer server's migration, then a failure before it listens.
		db, err := sql.Open("sqlite", filepath.Join(f.data, "nottennapod.db"))
		if err != nil {
			return err
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE TABLE newer (x); INSERT INTO schema_migrations VALUES (999, 'x', 0)`); err != nil {
			t.Fatal(err)
		}
		return errors.New("fails after migrating")
	}
	_, err := m.Update(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "previous version and its database are back and running") {
		t.Fatalf("got %v", err)
	}
	if b, _ := os.ReadFile(m.BinaryPath()); string(b) != "working old executable" {
		t.Fatalf("binary %q", b)
	}
	s, err := storage.Open(context.Background(), f.data)
	if err != nil {
		t.Fatalf("the previous version cannot open the database put back: %v", err)
	}
	defer s.Close()
	if left, _ := filepath.Glob(filepath.Join(f.data, "nottennapod.db.update-failed-*")); len(left) == 0 {
		t.Error("the failed version's database was not kept")
	}
}

// Rollback is reported running only when the previous version
// answers.
func TestRollbackIsReportedRunningOnlyWhenItAnswers(t *testing.T) {
	m, f, src := newFakeService(t)
	f.start = func(string) error { return errors.New("neither version starts") }
	_, err := m.Update(context.Background(), src)
	if err == nil || strings.Contains(err.Error(), "back and running") || !strings.Contains(err.Error(), "is not ready") {
		t.Fatalf("got %v", err)
	}
}

// A candidate that accepted changes is not rolled back: the
// previous database would lose them. The update says so and keeps both.
func TestRollbackIsRefusedAfterTheCandidateAcceptedChanges(t *testing.T) {
	m, f, src := newFakeService(t)
	m.Info = func(context.Context, Manifest) (ServerInfo, error) {
		// The candidate accepts a change, then never answers.
		s, err := storage.Open(context.Background(), f.data)
		if err != nil {
			return ServerInfo{}, err
		}
		defer s.Close()
		err = s.Write(context.Background(), func(w *storage.WriteTx) error { _, err := w.CreateAccount(); return err })
		if err != nil {
			t.Fatal(err)
		}
		m.Info = func(context.Context, Manifest) (ServerInfo, error) { return ServerInfo{}, errors.New("not answering") }
		return ServerInfo{}, errors.New("not answering")
	}
	_, err := m.Update(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "already accepted changes") {
		t.Fatalf("got %v", err)
	}
	if b, _ := os.ReadFile(m.BinaryPath()); string(b) != "new executable" {
		t.Fatalf("binary %q", b)
	}
	if _, err := os.Stat(m.BinaryPath() + ".prev"); err != nil {
		t.Errorf("the previous binary is not kept: %v", err)
	}
	if _, err := os.Stat(storage.UpdateCopyDir(f.data)); err != nil {
		t.Errorf("the database copy is not kept: %v", err)
	}
}

// Install refuses a service definition directory other users can write, on both
// layouts, and changes nothing there.
func TestInstallRefusesASharedDefinitionDirectory(t *testing.T) {
	for _, goos := range []string{"darwin", "linux"} {
		t.Run(goos, func(t *testing.T) {
			r := &recorder{}
			m, home := manager(t, goos, r)
			parent := filepath.Dir(m.AgentFile)
			os.MkdirAll(parent, 0o755)
			os.Chmod(parent, 0o777)
			err := m.Install(context.Background(), binary(t, "1"), plan(home), Manifest{})
			if !errors.Is(err, private.ErrUntrusted) || !strings.Contains(err.Error(), "service definition") {
				t.Fatalf("got %v", err)
			}
			if fi, _ := os.Stat(parent); fi.Mode().Perm() != 0o777 {
				t.Fatalf("install changed the shared directory to %04o", fi.Mode().Perm())
			}
			if _, err := os.Stat(m.AgentFile); !errors.Is(err, os.ErrNotExist) || len(r.calls) != 0 {
				t.Fatalf("install went on: %v %q", err, r.calls)
			}
			// A writable directory above it is refused the same way.
			os.Chmod(parent, 0o755)
			os.Chmod(filepath.Dir(parent), 0o777)
			if err := m.Install(context.Background(), binary(t, "1"), plan(home), Manifest{}); !errors.Is(err, private.ErrUntrusted) {
				t.Fatalf("under a writable directory: %v", err)
			}
			os.Chmod(filepath.Dir(parent), 0o755)
			if err := m.Install(context.Background(), binary(t, "1"), plan(home), Manifest{}); err != nil {
				t.Fatalf("a private definition directory: %v", err)
			}
		})
	}
}

// migratingCandidate makes the new version migrate the database and then
// fail (or, with works, run), as a release with a migration would.
func migratingCandidate(t *testing.T, f *fakeService, works bool) func(string) error {
	return func(bin string) error {
		if bin == "working old executable" {
			return nil
		}
		db, err := sql.Open("sqlite", filepath.Join(f.data, "nottennapod.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS newer (x); INSERT OR IGNORE INTO schema_migrations VALUES (999, 'x', 0)`); err != nil {
			t.Fatal(err)
		}
		if works {
			return nil
		}
		return errors.New("fails after migrating")
	}
}

// An updater that dies at any step, of the update or of its rollback, leaves
// what the next `service update` needs to resolve it; nothing loses the
// previous binary or its database.
func TestAnInterruptedUpdateIsResolvedFromEveryStep(t *testing.T) {
	failing := []string{"recorded", "preserved", "stopped", "saved", "placed", "manifest", "started",
		"rollback-begun", "rollback-binary", "rollback-binary-recorded", "rollback-database", "rollback-ready"}
	working := []string{"ready", "discarded"}
	for _, point := range append(failing, working...) {
		t.Run(point, func(t *testing.T) {
			works := slices.Contains(working, point)
			m, f, src := newFakeService(t)
			f.start = migratingCandidate(t, f, works)
			crash = func(p string) {
				if p == point {
					panic("the updater died at " + p)
				}
			}
			func() {
				defer func() { recover() }()
				m.Update(context.Background(), src)
				t.Fatalf("the updater did not die at %s", point)
			}()
			crash = func(string) {}
			// The next update resolves the interrupted one and stops.
			_, err := m.Update(context.Background(), src)
			if err == nil || !strings.Contains(err.Error(), "resolved first") && !strings.Contains(err.Error(), "back and running") {
				t.Fatalf("the next update: %v", err)
			}
			bin, _ := os.ReadFile(m.BinaryPath())
			switch {
			case works:
				if string(bin) != "new executable" || f.running != "new executable" {
					t.Fatalf("binary %q, running %q", bin, f.running)
				}
			default:
				if string(bin) != "working old executable" || f.running != "working old executable" {
					t.Fatalf("binary %q, running %q", bin, f.running)
				}
				s, err := storage.Open(context.Background(), f.data)
				if err != nil {
					t.Fatalf("the previous version cannot open its database: %v", err)
				}
				s.Close()
			}
			for _, p := range []string{m.BinaryPath() + ".prev", storage.UpdateCopyDir(f.data), filepath.Join(m.InstallDir, "update.json")} {
				if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("%s is left after the update was resolved", filepath.Base(p))
				}
			}
			// And a new update runs (from the previous version: this
			// updater does not know the new version's migration).
			if !works {
				f.start = migratingCandidate(t, f, true)
				if v, err := m.Update(context.Background(), src); err != nil || v != "2" {
					t.Fatalf("the following update: %q %v", v, err)
				}
			}
		})
	}
}

// The updater dies after the new version migrated the database; the retry rolls
// back to the working binary and a database it opens, never losing either on
// the way.
func TestARetryAfterAMigratingCandidateKeepsTheRollbackPair(t *testing.T) {
	m, f, src := newFakeService(t)
	migrate := migratingCandidate(t, f, false)
	f.start = func(bin string) error {
		if bin != "working old executable" {
			migrate(bin)
			panic("the updater died after the candidate migrated")
		}
		return nil
	}
	func() {
		defer func() { recover() }()
		m.Update(context.Background(), src)
	}()
	if prev, _ := os.ReadFile(m.BinaryPath() + ".prev"); string(prev) != "working old executable" {
		t.Fatalf(".prev %q", prev)
	}
	f.start = func(bin string) error {
		if bin != "working old executable" {
			return errors.New("candidate broken")
		}
		return nil
	}
	_, err := m.Update(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "back and running") {
		t.Fatalf("the retry: %v", err)
	}
	if bin, _ := os.ReadFile(m.BinaryPath()); string(bin) != "working old executable" {
		t.Fatalf("binary %q", bin)
	}
	s, err := storage.Open(context.Background(), f.data)
	if err != nil {
		t.Fatalf("the previous version cannot open its database: %v", err)
	}
	s.Close()
}

// A rollback whose copy is gone moves nothing and keeps the record.
func TestARollbackWithoutItsCopyMovesNothing(t *testing.T) {
	m, f, src := newFakeService(t)
	f.start = migratingCandidate(t, f, false)
	crash = func(p string) {
		if p == "placed" {
			panic("died")
		}
	}
	func() {
		defer func() { recover() }()
		m.Update(context.Background(), src)
	}()
	crash = func(string) {}
	os.Remove(filepath.Join(storage.UpdateCopyDir(f.data), "nottennapod.db"))
	_, err := m.Update(context.Background(), src)
	if !errors.Is(err, storage.ErrNoUpdateCopy) {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.data, "nottennapod.db")); err != nil {
		t.Fatalf("the live database moved: %v", err)
	}
	if prev, _ := os.ReadFile(m.BinaryPath() + ".prev"); string(prev) != "working old executable" {
		t.Fatalf("the previous binary is not kept: %q", prev)
	}
	if _, err := os.Stat(filepath.Join(m.InstallDir, "update.json")); err != nil {
		t.Fatalf("the record went: %v", err)
	}
	if err := m.Uninstall(context.Background()); err == nil || !strings.Contains(err.Error(), "unfinished") {
		t.Fatalf("uninstall during an unfinished update: %v", err)
	}
}
