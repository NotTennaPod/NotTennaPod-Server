package service

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func plan(dir string) Plan {
	return Plan{
		Label:  Label,
		Binary: filepath.Join(dir, "Application Support", "NotTennaPod", "bin", "nottennapod-server"),
		Args:   []string{"serve", "--data-dir", filepath.Join(dir, "My Data & <Stuff>"), "--log-dir", filepath.Join(dir, "Logs")},
		LogDir: filepath.Join(dir, "Logs"),
		Path:   MinimalPath("darwin", "/opt/homebrew/bin"),
		Home:   dir,
	}
}

func TestLaunchdPlistRestartsAfterFailuresOnlyAndQuotesPaths(t *testing.T) {
	dir := t.TempDir()
	b := plan(dir).LaunchdPlist()
	var v any
	if err := xml.Unmarshal(b, &v); err != nil {
		t.Fatalf("not XML: %v", err)
	}
	s := string(b)
	for _, want := range []string{
		"<key>KeepAlive</key>\n\t<dict>\n\t\t<key>SuccessfulExit</key>\n\t\t<false/>",
		"<key>ThrottleInterval</key>\n\t<integer>30</integer>",
		"<key>Umask</key>\n\t<integer>63</integer>",
		"<key>RunAtLoad</key>\n\t<true/>",
		"My Data &amp; &lt;Stuff&gt;",
		"<string>/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>",
		filepath.Join(dir, "Logs", "early.log"),
	} {
		if !strings.Contains(s, want) {
			t.Errorf("plist lacks %q", want)
		}
	}
	if runtime.GOOS == "darwin" {
		if _, err := exec.LookPath("plutil"); err == nil {
			f := filepath.Join(dir, "agent.plist")
			os.WriteFile(f, b, 0o600)
			if out, err := exec.Command("plutil", "-lint", f).CombinedOutput(); err != nil {
				t.Errorf("plutil: %v: %s", err, out)
			}
		}
	}
}

func TestSystemdUnitRetriesForeverAndQuotesArguments(t *testing.T) {
	u := string(plan("/home/o w").SystemdUnit())
	for _, want := range []string{
		"Restart=on-failure",
		"RestartSec=30",
		"StartLimitIntervalSec=0",
		"UMask=0077",
		`"/home/o w/My Data & <Stuff>"`,
		`Environment="PATH=/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"`,
	} {
		if !strings.Contains(u, want) {
			t.Errorf("unit lacks %q:\n%s", want, u)
		}
	}
	if got := systemdQuote(`100% $HOME "x"`); got != `"100%% $$HOME \"x\""` {
		t.Errorf("quote = %s", got)
	}
}

func TestLayouts(t *testing.T) {
	l, _ := DefaultLayout("darwin", "/Users/o", Label)
	if l.AgentFile != "/Users/o/Library/LaunchAgents/app.nottennapod.server.plist" || l.InstallDir != "/Users/o/Library/Application Support/NotTennaPod/bin" || l.LogDir != "/Users/o/Library/Logs/NotTennaPod" {
		t.Errorf("darwin layout %+v", l)
	}
	t.Setenv("XDG_STATE_HOME", "")
	l, _ = DefaultLayout("linux", "/home/o", Label)
	if l.AgentFile != "/home/o/.config/systemd/user/nottennapod-server.service" || l.InstallDir != "/home/o/.local/lib/nottennapod" {
		t.Errorf("linux layout %+v", l)
	}
	l, _ = DefaultLayout("darwin", "/Users/o", "app.nottennapod.server.test")
	if !strings.HasSuffix(l.InstallDir, "app.nottennapod.server.test") || !strings.HasSuffix(l.AgentFile, "app.nottennapod.server.test.plist") {
		t.Errorf("a test label must keep apart: %+v", l)
	}
	if _, err := DefaultLayout("windows", "C:/", Label); err == nil {
		t.Error("windows has no service")
	}
}

// recorder records the service manager's commands and plays launchd: the
// service runs after bootstrap or kickstart (writing a new instance ID in
// data, as serve does) and stops after kill.
type recorder struct {
	calls   []string
	data    string
	stopped bool
	id      string
}

func (r *recorder) run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, name+" "+strings.Join(args, " "))
	if name != "launchctl" || len(args) == 0 {
		return nil, nil
	}
	switch args[0] {
	case "print":
		if r.stopped {
			return []byte("gui/501/app.nottennapod.server = {\n\tstate = not running\n}\n"), nil
		}
		return []byte("gui/501/app.nottennapod.server = {\n\tactive count = 1\n\tstate = running\n\n\tpid = 4242\n\tendpoints = {\n\t\t\"x\" = {\n\t\t\tstate = active\n\t\t}\n\t}\n\tsockets = {\n\t\tstate = active\n\t}\n}\n"), nil
	case "kill":
		r.stopped = true
	case "kickstart", "bootstrap":
		r.stopped = false
		if r.data != "" {
			r.id, _ = storage.NewInstance(r.data)
		}
	}
	return nil, nil
}

// newDataDir is a stopped server's data directory.
func newDataDir(t *testing.T) string {
	t.Helper()
	d := filepath.Join(t.TempDir(), "data")
	s, err := storage.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	return d
}

func manager(t *testing.T, goos string, r *recorder) (*Manager, string) {
	home := t.TempDir()
	l, err := DefaultLayout(goos, home, Label)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{Layout: l, UID: 501, Run: r.run}
	m.VersionOf = func(_ context.Context, bin string) (string, error) {
		b, _ := os.ReadFile(bin)
		if strings.HasPrefix(string(b), "broken") {
			return "", errors.New("exec format error")
		}
		return strings.TrimSpace(string(b)), nil
	}
	return m, home
}

func binary(t *testing.T, content string) string {
	f := filepath.Join(t.TempDir(), "nottennapod-server")
	if err := os.WriteFile(f, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestInstallUpdateUninstallOnMacOS(t *testing.T) {
	data := newDataDir(t)
	r := &recorder{data: data}
	m, home := manager(t, "darwin", r)
	p := plan(home)
	p.Binary = m.BinaryPath()
	if err := m.Install(context.Background(), binary(t, "1.0"), p, Manifest{Label: Label, Listen: "127.0.0.1:8080",
		Args: []string{"serve", "--data-dir", data}}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(m.BinaryPath()); string(b) != "1.0" {
		t.Errorf("installed binary %q", b)
	}
	if fi, _ := os.Stat(m.LogDir); fi.Mode().Perm() != 0o700 {
		t.Errorf("log directory mode %v", fi.Mode().Perm())
	}
	if fi, err := os.Stat(filepath.Join(m.LogDir, "early.log")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("early.log must exist and be private before launchd opens it: %v %v", fi, err)
	}
	want := []string{"launchctl bootout gui/501/app.nottennapod.server", "launchctl bootstrap gui/501 " + m.AgentFile}
	if strings.Join(r.calls, "\n") != strings.Join(want, "\n") {
		t.Errorf("calls %q, want %q", r.calls, want)
	}
	if s := m.Status(context.Background()); !s.Installed || !s.Running || s.PID != "4242" {
		t.Errorf("status %+v", s)
	}

	// A binary that does not run never replaces the installed one.
	r.calls = nil
	if _, err := m.Update(context.Background(), binary(t, "broken")); err == nil {
		t.Fatal("a broken binary was accepted")
	}
	if b, _ := os.ReadFile(m.BinaryPath()); string(b) != "1.0" {
		t.Errorf("after a failed update the binary is %q", b)
	}
	if left, _ := filepath.Glob(filepath.Join(m.InstallDir, ".nottennapod-server.*")); len(left) != 0 {
		t.Errorf("the staging file was left behind: %v", left)
	}
	if len(r.calls) != 0 {
		t.Errorf("a failed update restarted the service: %q", r.calls)
	}

	// A good one stops the server, replaces the binary, starts it, and is
	// confirmed by the server just started: its version and instance ID.
	asked := 0
	m.Info = func(_ context.Context, man Manifest) (ServerInfo, error) {
		asked++
		if man.Listen != "127.0.0.1:8080" {
			t.Errorf("asked %s", man.Listen)
		}
		return ServerInfo{Version: "1.1", InstanceID: r.id}, nil
	}
	v, err := m.Update(context.Background(), binary(t, "1.1"))
	if err != nil || v != "1.1" || asked == 0 {
		t.Fatalf("update: %q %v (asked %d)", v, err, asked)
	}
	target := "gui/501/app.nottennapod.server"
	want = []string{"launchctl print " + target, "launchctl kill SIGTERM " + target, "launchctl print " + target, "launchctl kickstart " + target}
	if strings.Join(r.calls, "\n") != strings.Join(want, "\n") {
		t.Errorf("update calls %q, want %q", r.calls, want)
	}
	if _, err := os.Stat(storage.UpdateCopyDir(data)); !errors.Is(err, os.ErrNotExist) {
		t.Error("the database copy was left behind after a good update")
	}

	r.calls = nil
	if err := m.Uninstall(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{m.AgentFile, m.BinaryPath(), m.ManifestPath()} {
		if _, err := os.Stat(f); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s remains after uninstall", f)
		}
	}
	if m.Status(context.Background()).Installed {
		t.Error("still installed")
	}
}

func TestInstallOnLinuxEnablesTheUserUnit(t *testing.T) {
	r := &recorder{}
	m, home := manager(t, "linux", r)
	p := plan(home)
	p.Binary = m.BinaryPath()
	if err := m.Install(context.Background(), binary(t, "1.0"), p, Manifest{Label: Label}); err != nil {
		t.Fatal(err)
	}
	want := "systemctl --user daemon-reload\nsystemctl --user enable --now nottennapod-server.service"
	if strings.Join(r.calls, "\n") != want {
		t.Errorf("calls %q", r.calls)
	}
	if b, _ := os.ReadFile(m.AgentFile); !strings.Contains(string(b), "Restart=on-failure") {
		t.Error("no unit written")
	}
	r.calls = nil
	_ = m.Stop(context.Background())
	_ = m.Start(context.Background())
	if strings.Join(r.calls, "\n") != "systemctl --user stop nottennapod-server.service\nsystemctl --user start nottennapod-server.service" {
		t.Errorf("calls %q", r.calls)
	}
}

func TestDataDirectoryMustBeRecognized(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	absent := filepath.Join(root, "absent")
	if err := CheckDataDir(ctx, absent, false); err == nil {
		t.Error("an absent directory without --new-server was accepted")
	}
	if err := CheckDataDir(ctx, absent, true); err != nil {
		t.Errorf("an absent directory with --new-server: %v", err)
	}
	empty := filepath.Join(root, "empty")
	os.Mkdir(empty, 0o700)
	if err := CheckDataDir(ctx, empty, true); err != nil {
		t.Errorf("an empty directory with --new-server: %v", err)
	}
	if err := CheckDataDir(ctx, empty, false); err == nil {
		t.Error("an empty directory without --new-server was accepted")
	}
	other := filepath.Join(root, "other")
	os.Mkdir(other, 0o700)
	os.WriteFile(filepath.Join(other, "notes.txt"), []byte("x"), 0o600)
	if err := CheckDataDir(ctx, other, true); err == nil {
		t.Error("a non-empty directory without a database was accepted for a new server")
	}
	notOurs := filepath.Join(root, "not-ours")
	os.Mkdir(notOurs, 0o700)
	os.WriteFile(filepath.Join(notOurs, databaseName), []byte("not sqlite"), 0o600)
	if err := CheckDataDir(ctx, notOurs, false); err == nil {
		t.Error("a file that is not a server database was accepted")
	}
	real := filepath.Join(root, "real data")
	s, err := storage.Open(ctx, real)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckDataDir(ctx, real, false); err != nil {
		t.Errorf("a running server's directory: %v", err) // read while the server holds it open
	}
	s.Close()
	if err := CheckDataDir(ctx, real, true); err == nil {
		t.Error("--new-server over an existing server was accepted")
	}
}

// ---- installation and service commands

func installedFor(t *testing.T, run Runner) (*Manager, string) {
	t.Helper()
	d := t.TempDir()
	m := &Manager{Layout: Layout{GOOS: "darwin", Label: Label, InstallDir: d, LogDir: t.TempDir()}, UID: 501, Run: run,
		VersionOf: func(context.Context, string) (string, error) { return "2", nil }, HealthWait: 300 * time.Millisecond, StopWait: 300 * time.Millisecond}
	os.WriteFile(m.BinaryPath(), []byte("working old executable"), 0o700)
	b, _ := json.Marshal(Manifest{Version: "1", Listen: "127.0.0.1:8080", Args: []string{"serve", "--data-dir", newDataDir(t)}})
	os.WriteFile(m.ManifestPath(), b, 0o600)
	src := filepath.Join(t.TempDir(), "release")
	os.WriteFile(src, []byte("new executable"), 0o700)
	return m, src
}

func assertOld(t *testing.T, m *Manager) {
	t.Helper()
	if got, _ := os.ReadFile(m.BinaryPath()); string(got) != "working old executable" {
		t.Errorf("installed after a failed update: %q", got)
	}
	var man Manifest
	b, _ := os.ReadFile(m.ManifestPath())
	json.Unmarshal(b, &man)
	if man.Version != "1" {
		t.Errorf("manifest version after a failed update: %q", man.Version)
	}
	if _, err := os.Stat(m.BinaryPath() + ".prev"); err == nil {
		t.Error("the kept previous binary was left behind")
	}
}

func TestAFailedRestartPutsThePreviousVersionBack(t *testing.T) {
	m, _, src := newFakeService(t)
	starts := 0
	run := m.Run
	m.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "launchctl" && args[0] == "kickstart" {
			if starts++; starts == 1 {
				return nil, errors.New("restart failed")
			}
		}
		return run(ctx, name, args...)
	}
	if _, err := m.Update(context.Background(), src); err == nil || !strings.Contains(err.Error(), "previous version and its database are back and running") {
		t.Fatalf("update: %v", err)
	}
	assertOld(t, m)
	if starts != 2 {
		t.Errorf("the previous version was started %d times, want once after the failure", starts-1)
	}
}

func TestAFailedHealthCheckPutsThePreviousVersionBack(t *testing.T) {
	m, src := installedFor(t, func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
	m.Info = func(context.Context, Manifest) (ServerInfo, error) {
		return ServerInfo{}, errors.New("the new server cannot start")
	}
	if _, err := m.Update(context.Background(), src); err == nil {
		t.Fatal("an update whose server never answered succeeded")
	}
	// The previous version is back, but until it answers the rollback pair
	// and the record stay.
	if got, _ := os.ReadFile(m.BinaryPath()); string(got) != "working old executable" {
		t.Fatalf("installed after a failed update: %q", got)
	}
	if _, err := os.Stat(m.BinaryPath() + ".prev"); err != nil {
		t.Fatalf("the previous binary is not kept while the rollback is unconfirmed: %v", err)
	}
	var man Manifest
	b, _ := os.ReadFile(m.ManifestPath())
	json.Unmarshal(b, &man)
	var id string
	m.Run = func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "launchctl" && args[0] == "kickstart" {
			id, _ = storage.NewInstance(man.DataDir()) // the previous version starts
		}
		return nil, nil
	}
	m.Info = func(context.Context, Manifest) (ServerInfo, error) {
		return ServerInfo{Version: "1", InstanceID: id}, nil
	}
	if _, err := m.Resume(context.Background()); err == nil || !strings.Contains(err.Error(), "back and running") {
		t.Fatalf("resuming once the previous version answers: %v", err)
	}
	assertOld(t, m)
}

func TestStopWaitsForTheServerToExit(t *testing.T) {
	prints := 0
	state := func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "launchctl" && args[0] == "print" {
			prints++
			if prints < 3 {
				return []byte("x = {\n\tstate = running\n\tpid = 9\n}\n"), nil
			}
			return []byte("x = {\n\tstate = not running\n}\n"), nil
		}
		return nil, nil
	}
	m, _ := installedFor(t, state)
	m.StopWait = 5 * time.Second
	os.MkdirAll(filepath.Dir(m.AgentFile), 0o700)
	m.AgentFile = filepath.Join(m.InstallDir, "agent.plist")
	os.WriteFile(m.AgentFile, []byte("x"), 0o600)
	if err := m.Stop(context.Background()); err != nil || prints < 3 {
		t.Fatalf("stop returned %v after %d status reads", err, prints)
	}
	always := func(_ context.Context, name string, args ...string) ([]byte, error) {
		return []byte("x = {\n\tstate = running\n\tpid = 9\n}\n"), nil
	}
	m.Run = always
	if err := m.Stop(context.Background()); err == nil {
		t.Error("stop reported success while the server kept running")
	}
}
