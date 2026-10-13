// Package service runs the server as a per-user service (PROTOCOL §14): a launchd agent on macOS
// and a systemd user unit on Linux, started at login and restarted after a crash, never after a
// clean stop. Only the owner's `service install` installs one; nothing in the server or its
// tests does.
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Label is the service's name: the launchd label, and the systemd unit's name.
const Label = "app.nottennapod.server"

// unitName is the systemd unit for a label.
func unitName(label string) string {
	if label == Label {
		return "nottennapod-server.service"
	}
	return label + ".service"
}

// Layout is where a service's files live.
type Layout struct {
	GOOS       string
	Label      string
	InstallDir string // the installed binary and its manifest
	AgentFile  string // the launchd plist or systemd unit
	LogDir     string
}

// DefaultLayout is the layout for home on goos (PROTOCOL §14).
func DefaultLayout(goos, home, label string) (Layout, error) {
	l := Layout{GOOS: goos, Label: label}
	switch goos {
	case "darwin":
		l.InstallDir = filepath.Join(home, "Library", "Application Support", "NotTennaPod", "bin")
		l.AgentFile = filepath.Join(home, "Library", "LaunchAgents", label+".plist")
		l.LogDir = filepath.Join(home, "Library", "Logs", "NotTennaPod")
	case "linux":
		l.InstallDir = filepath.Join(home, ".local", "lib", "nottennapod")
		l.AgentFile = filepath.Join(home, ".config", "systemd", "user", unitName(label))
		state := os.Getenv("XDG_STATE_HOME")
		if !filepath.IsAbs(state) {
			state = filepath.Join(home, ".local", "state")
		}
		l.LogDir = filepath.Join(state, "nottennapod", "logs")
	default:
		return l, fmt.Errorf("service: running as a service is supported on macOS and Linux, not %s", goos)
	}
	if label != Label {
		// A test label keeps its own binary and logs beside the real ones.
		l.InstallDir = filepath.Join(l.InstallDir, label)
		l.LogDir = filepath.Join(l.LogDir, label)
	}
	return l, nil
}

// BinaryPath is the installed binary.
func (l Layout) BinaryPath() string { return filepath.Join(l.InstallDir, "nottennapod-server") }

// ManifestPath is the record of what was installed (the listen address for update's check).
func (l Layout) ManifestPath() string { return filepath.Join(l.InstallDir, "service.json") }

// Plan is what the agent runs: the installed binary with serve's arguments,
// every path absolute, and a minimal environment (PROTOCOL §14).
type Plan struct {
	Label  string
	Binary string
	Args   []string
	LogDir string
	Path   string // PATH: the system's and the resolved ffmpeg's directory
	Home   string
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// LaunchdPlist is the agent: run at load, restarted after a crash or any
// non-zero exit but not after a clean exit, throttled to 30 s, umask 077, and
// early failures in a private early.log (PROTOCOL §14).
func (p Plan) LaunchdPlist() []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>` + xmlEscape(p.Label) + `</string>
	<key>ProgramArguments</key>
	<array>
`)
	for _, a := range append([]string{p.Binary}, p.Args...) {
		b.WriteString("\t\t<string>" + xmlEscape(a) + "</string>\n")
	}
	b.WriteString(`	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>PATH</key>
		<string>` + xmlEscape(p.Path) + `</string>
		<key>HOME</key>
		<string>` + xmlEscape(p.Home) + `</string>
	</dict>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>ThrottleInterval</key>
	<integer>30</integer>
	<key>ProcessType</key>
	<string>Background</string>
	<key>Umask</key>
	<integer>63</integer>
	<key>StandardOutPath</key>
	<string>` + xmlEscape(filepath.Join(p.LogDir, "early.log")) + `</string>
	<key>StandardErrorPath</key>
	<string>` + xmlEscape(filepath.Join(p.LogDir, "early.log")) + `</string>
</dict>
</plist>
`)
	return []byte(b.String())
}

// systemdQuote quotes one argument for ExecStart.
func systemdQuote(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "%", "%%", "$", "$$")
	return `"` + r.Replace(s) + `"`
}

// SystemdUnit is the user unit: restarted on failure every 30 s forever, umask 077 (PROTOCOL §14).
func (p Plan) SystemdUnit() []byte {
	args := []string{systemdQuote(p.Binary)}
	for _, a := range p.Args {
		args = append(args, systemdQuote(a))
	}
	return []byte(`[Unit]
Description=NotTennaPod server
After=network-online.target
StartLimitIntervalSec=0

[Service]
ExecStart=` + strings.Join(args, " ") + `
Environment=` + systemdQuote("PATH="+p.Path) + `
Restart=on-failure
RestartSec=30
UMask=0077
StandardOutput=append:` + filepath.Join(p.LogDir, "early.log") + `
StandardError=append:` + filepath.Join(p.LogDir, "early.log") + `

[Install]
WantedBy=default.target
`)
}

// Manifest records an installation.
type Manifest struct {
	Label   string   `json:"label"`
	Listen  string   `json:"listen"`
	HTTPS   bool     `json:"https"` // the server serves HTTPS (--tls-cert): update's check uses it
	Args    []string `json:"args"`
	Version string   `json:"version"`
	// Tailscale is the tailscale command install verified, the only one the
	// service asks.
	Tailscale string `json:"tailscale,omitempty"`
}

// arg is the value of serve's flag name in the installed arguments.
func (man Manifest) arg(name string) string {
	for i := 0; i+1 < len(man.Args); i++ {
		if man.Args[i] == name {
			return man.Args[i+1]
		}
	}
	return ""
}

// DataDir is the data directory the service serves.
func (man Manifest) DataDir() string { return man.arg("--data-dir") }

// TLSCert is the certificate the service serves HTTPS with ("" for HTTP).
func (man Manifest) TLSCert() string { return man.arg("--tls-cert") }

// Runner runs the service manager's commands (launchctl, systemctl); tests record them.
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// serviceCommandTimeout bounds one service-manager command: a stop may wait
// for the server's graceful shutdown (systemd's default is 90 s), but a hung
// launchctl or systemctl must not hold an update, and its lock, for ever.
var serviceCommandTimeout = 3 * time.Minute

// ExecRunner runs commands for real, each within serviceCommandTimeout; a
// child left holding the output pipe does not keep it waiting either.
func ExecRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, serviceCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = 2 * time.Second
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return out, fmt.Errorf("%s %s did not finish within %s", name, strings.Join(args, " "), serviceCommandTimeout)
		}
		return out, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

// Manager installs and controls one service.
type Manager struct {
	Layout
	UID int
	Run Runner
	// VersionOf runs a binary with --version (staging checks a new binary runs, PROTOCOL §14).
	VersionOf func(ctx context.Context, binary string) (string, error)
	// Info reads /v1/info from the running service: its version and instance
	// ID (update's readiness check). Nil skips the check.
	Info func(ctx context.Context, m Manifest) (ServerInfo, error)
	// HealthWait bounds update's wait for the new version (default 30 s); StopWait, stop's for the exit.
	HealthWait, StopWait time.Duration
	// Note receives what an update tells the owner besides its result (a
	// kept update's pair set aside); nil drops it.
	Note func(string)
}

// ServerInfo is what update's readiness check reads from /v1/info.
type ServerInfo struct {
	Version    string `json:"version"`
	InstanceID string `json:"instance_id"`
}

// lockName is the installation's lock file.
const lockName = ".service.lock"

// lock takes the installation's exclusive lock for a service change, after
// making sure the installation directory belongs to this user and is 0700.
func (m *Manager) lock() (func(), error) {
	if err := private.Dir(m.InstallDir); err != nil {
		return nil, fmt.Errorf("service: the installation directory: %w", err)
	}
	// The binary and its kept previous name may share their file: that is
	// the update's recorded pair (PROTOCOL §14).
	if err := private.Tree(m.InstallDir, m.BinaryPath(), m.prevPath(), m.restorePath()); err != nil {
		return nil, fmt.Errorf("service: the installation directory: %w", err)
	}
	f, err := private.OpenFile(filepath.Join(m.InstallDir, lockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("service: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.New("service: another service command is changing this installation; wait for it to finish")
		}
		return nil, fmt.Errorf("service: locking the installation: %w", err)
	}
	return func() { f.Close() }, nil
}

func (m *Manager) target() string { return fmt.Sprintf("gui/%d/%s", m.UID, m.Label) }

// Install copies the binary into place, writes the agent and starts it.
func (m *Manager) Install(ctx context.Context, binary string, plan Plan, manifest Manifest) error {
	// The service definition decides what runs as this user: no other user may
	// be able to replace it, through its directory or any directory on the way.
	// A shared directory is the owner's to fix; install changes nothing there.
	agentDir := filepath.Dir(m.AgentFile)
	if err := os.MkdirAll(agentDir, 0o700); err != nil {
		return err
	}
	if err := private.TrustedDir(agentDir); err != nil {
		return fmt.Errorf("service: the service definition's directory: %w; install does not put the definition where another user could replace it", err)
	}
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	if err := m.pending(); err != nil {
		return err
	}
	if err := private.Dir(m.LogDir); err != nil {
		return fmt.Errorf("service: the log directory: %w", err)
	}
	if err := private.Tree(m.LogDir); err != nil {
		return fmt.Errorf("service: the log directory: %w", err)
	}
	// launchd and systemd create early.log before the service's umask
	// applies; created here first, it stays private (PROTOCOL §14).
	early, err := private.OpenFile(filepath.Join(m.LogDir, "early.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("service: %w", err)
	}
	early.Close()
	staged, v, err := m.stage(ctx, binary)
	if err != nil {
		return err
	}
	defer os.Remove(staged) // a no-op once in place
	if err := m.place(staged); err != nil {
		return err
	}
	manifest.Version = v
	mb, _ := json.MarshalIndent(manifest, "", "  ")
	if err := writeAtomic(m.ManifestPath(), mb, 0o600); err != nil {
		return err
	}
	agent := plan.LaunchdPlist()
	if m.GOOS == "linux" {
		agent = plan.SystemdUnit()
	}
	if err := writeAtomic(m.AgentFile, agent, 0o644); err != nil {
		return err
	}
	switch m.GOOS {
	case "darwin":
		_, _ = m.Run(ctx, "launchctl", "bootout", m.target()) // a previous installation, if loaded
		_, err = m.Run(ctx, "launchctl", "bootstrap", fmt.Sprintf("gui/%d", m.UID), m.AgentFile)
	case "linux":
		if _, err = m.Run(ctx, "systemctl", "--user", "daemon-reload"); err == nil {
			_, err = m.Run(ctx, "systemctl", "--user", "enable", "--now", unitName(m.Label))
		}
	}
	return err
}

// Uninstall stops and removes the agent and the installed binary; the data
// directory, backups and keys stay (PROTOCOL §14).
func (m *Manager) Uninstall(ctx context.Context) error {
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	if err := m.pending(); err != nil {
		return err
	}
	switch m.GOOS {
	case "darwin":
		_, _ = m.Run(ctx, "launchctl", "bootout", m.target())
	case "linux":
		_, _ = m.Run(ctx, "systemctl", "--user", "disable", "--now", unitName(m.Label))
	}
	if err := os.Remove(m.AgentFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if m.GOOS == "linux" {
		_, _ = m.Run(ctx, "systemctl", "--user", "daemon-reload")
	}
	for _, f := range []string{m.BinaryPath(), m.ManifestPath()} {
		if err := os.Remove(f); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// Start starts a stopped service.
func (m *Manager) Start(ctx context.Context) error {
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	return m.start(ctx)
}

// Stop stops the service cleanly; it stays stopped until started (a clean exit is never restarted).
func (m *Manager) Stop(ctx context.Context) error {
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	return m.stop(ctx)
}

// Restart stops and starts the service.
func (m *Manager) Restart(ctx context.Context) error {
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	if m.GOOS == "darwin" {
		_, err := m.Run(ctx, "launchctl", "kickstart", "-k", m.target())
		return err
	}
	_, err = m.Run(ctx, "systemctl", "--user", "restart", unitName(m.Label))
	return err
}

// stopIfRunning stops the service; launchd's kill needs a running process.
func (m *Manager) stopIfRunning(ctx context.Context) error {
	if m.GOOS == "darwin" && !m.Status(ctx).Running {
		return nil
	}
	return m.stop(ctx)
}

func (m *Manager) start(ctx context.Context) error {
	if m.GOOS == "darwin" {
		_, err := m.Run(ctx, "launchctl", "kickstart", m.target())
		return err
	}
	_, err := m.Run(ctx, "systemctl", "--user", "start", unitName(m.Label))
	return err
}

func (m *Manager) stop(ctx context.Context) error {
	if m.GOOS == "darwin" {
		if _, err := m.Run(ctx, "launchctl", "kill", "SIGTERM", m.target()); err != nil {
			return err
		}
		// launchctl returns at once: wait until the server has exited and let
		// go of its data directory, so keys export and restore can run (PROTOCOL §14).
		wait := m.StopWait
		if wait <= 0 {
			wait = 30 * time.Second
		}
		deadline := time.Now().Add(wait)
		for m.Status(ctx).Running {
			if time.Now().After(deadline) {
				return fmt.Errorf("service: the server did not stop within %v", wait)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
		return nil
	}
	_, err := m.Run(ctx, "systemctl", "--user", "stop", unitName(m.Label))
	return err
}

// Status is what the service manager says about the service.
type Status struct {
	Installed bool
	Running   bool
	PID       string
	Detail    string
}

// Status asks the service manager.
func (m *Manager) Status(ctx context.Context) Status {
	var s Status
	if _, err := os.Stat(m.AgentFile); err != nil {
		return s
	}
	s.Installed = true
	if m.GOOS == "darwin" {
		out, err := m.Run(ctx, "launchctl", "print", m.target())
		if err != nil {
			s.Detail = "not loaded"
			return s
		}
		s.Running, s.Detail, s.PID = parseLaunchctlPrint(string(out))
		return s
	}
	out, _ := m.Run(ctx, "systemctl", "--user", "is-active", unitName(m.Label))
	s.Detail = strings.TrimSpace(string(out))
	s.Running = s.Detail == "active"
	return s
}

// parseLaunchctlPrint reads the service's own state and pid from `launchctl
// print`: the top-level lines (one tab deep), not its endpoints' nested states.
func parseLaunchctlPrint(out string) (running bool, state, pid string) {
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "\t\t") {
			continue
		}
		k, v, ok := strings.Cut(strings.TrimSpace(line), " = ")
		switch {
		case ok && k == "state" && state == "":
			state = v
		case ok && k == "pid" && pid == "":
			pid = v
		}
	}
	return state == "running", state, pid
}

// stage copies src to a new, randomly named file beside the installed binary
// (created exclusively, so nothing planted there is followed), syncs it and
// checks it runs; it returns the staged file and the new binary's version.
//
// The candidate must be a regular file: it is checked before it is opened,
// opened without blocking (a FIFO never holds the installation's lock), and
// checked again through its descriptor. Copying stops when ctx ends.
func (m *Manager) stage(ctx context.Context, src string) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	fi, err := os.Stat(src)
	if err != nil {
		return "", "", err
	}
	if !fi.Mode().IsRegular() {
		return "", "", fmt.Errorf("service: %s is not a regular file; give the new binary itself", src)
	}
	in, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", "", err
	}
	defer in.Close()
	if ifi, err := in.Stat(); err != nil {
		return "", "", err
	} else if !ifi.Mode().IsRegular() || !os.SameFile(fi, ifi) {
		return "", "", fmt.Errorf("service: %s changed while it was opened; give the new binary itself", src)
	}
	out, err := os.CreateTemp(m.InstallDir, ".nottennapod-server.*.new")
	if err != nil {
		return "", "", err
	}
	staging := out.Name()
	fail := func(err error) (string, string, error) {
		out.Close()
		os.Remove(staging)
		return "", "", err
	}
	buf := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		n, rerr := in.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				return fail(err)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fail(rerr)
		}
	}
	if err := out.Chmod(0o700); err != nil {
		return fail(err)
	}
	if err := out.Sync(); err != nil {
		return fail(err)
	}
	if err := out.Close(); err != nil {
		return fail(err)
	}
	v := "unknown"
	if m.VersionOf != nil {
		if v, err = m.VersionOf(ctx, staging); err != nil {
			os.Remove(staging)
			return "", "", fmt.Errorf("service: the new binary does not run: %w", err)
		}
	}
	// Before anything is recorded, a version the record could not hold
	// is no NotTennaPod server build's.
	if err := ValidVersion(v); err != nil {
		os.Remove(staging)
		return "", "", fmt.Errorf("service: the new binary reports no usable version (%v); is it a nottennapod-server build? Nothing was changed", err)
	}
	return staging, v, nil
}

// place renames a staged binary into place and syncs the directory.
func (m *Manager) place(staged string) error {
	if err := os.Rename(staged, m.BinaryPath()); err != nil {
		return err
	}
	return syncDir(m.InstallDir)
}

// writeAtomic writes data to path through a new, randomly named temporary
// file (created exclusively), synced and renamed into place.
func writeAtomic(path string, data []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	fail := func(err error) error {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Chmod(mode); err != nil {
		return fail(err)
	}
	// No ACL entry, as an inheritable one of its directory would give.
	if err := private.Protect(f); err != nil {
		return fail(err)
	}
	if _, err := f.Write(data); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// MinimalPath is the agent's PATH: the system directories and the given ones (ffmpeg's).
func MinimalPath(goos string, extra ...string) string {
	dirs := []string{}
	seen := map[string]bool{}
	for _, d := range extra {
		if d != "" && !seen[d] {
			seen[d] = true
			dirs = append(dirs, d)
		}
	}
	for _, d := range []string{"/usr/bin", "/bin", "/usr/sbin", "/sbin"} {
		if !seen[d] {
			seen[d] = true
			dirs = append(dirs, d)
		}
	}
	return strings.Join(dirs, ":")
}
