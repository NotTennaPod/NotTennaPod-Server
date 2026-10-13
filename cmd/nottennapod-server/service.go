package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/listen"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/logfile"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/service"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/version"
)

const serviceUsage = `Usage:
  nottennapod-server service install --data-dir D [--new-server] [--listen ADDR] [--public-url URL] [--allow-host NAME]...
      [--backup-dir DIR] [--refresh-interval DUR] [--binary FILE]
      run the server at login and after a crash (launchd on macOS, systemd on Linux)
  nottennapod-server service update [FILE]
      replace the installed binary with FILE, restart and check the new version answers;
      without FILE, finish an update that was interrupted
  nottennapod-server service start | stop | restart | status
  nottennapod-server service uninstall
      stop and remove the service; the data directory, backups and keys stay
`

// serviceCmd is `service …` (PROTOCOL §14).
func serviceCmd(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, serviceUsage)
		return 2
	}
	cmd, rest := args[0], args[1:]
	label := service.Label
	if l := os.Getenv("NTP_SERVICE_LABEL"); l != "" {
		label = l // service tests install under their own label (PROTOCOL §14)
	}
	m, err := newManager(label)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	m.Note = func(note string) { fmt.Fprintln(stdout, note) }
	switch cmd {
	case "install":
		return serviceInstall(ctx, m, rest, stdout, stderr)
	case "update":
		if len(rest) == 0 {
			outcome, err := m.Resume(ctx)
			switch {
			case err != nil:
				fmt.Fprintln(stderr, err)
				return 1
			case outcome == "":
				fmt.Fprintln(stdout, "No update is unfinished.")
			default:
				fmt.Fprintf(stdout, "An unfinished update was resolved: %s.\n", outcome)
			}
			return 0
		}
		if len(rest) != 1 {
			fmt.Fprint(stderr, serviceUsage)
			return 2
		}
		v, err := m.Update(ctx, rest[0])
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "Updated to %s; the service is running it.\n", v)
		return 0
	case "start", "stop", "restart", "uninstall":
		var err error
		switch cmd {
		case "start":
			err = m.Start(ctx)
		case "stop":
			err = m.Stop(ctx)
		case "restart":
			err = m.Restart(ctx)
		case "uninstall":
			err = m.Uninstall(ctx)
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	case "status":
		s := m.Status(ctx)
		switch {
		case !s.Installed:
			fmt.Fprintln(stdout, "not installed")
			return 3
		case s.Running:
			fmt.Fprintf(stdout, "running (pid %s)\n", s.PID)
		default:
			fmt.Fprintf(stdout, "installed, not running (%s)\n", s.Detail)
		}
		return 0
	}
	fmt.Fprintf(stderr, "nottennapod-server service: unknown command %q\n\n%s", cmd, serviceUsage)
	return 2
}

func newManager(label string) (*service.Manager, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	layout, err := service.DefaultLayout(runtime.GOOS, home, label)
	if err != nil {
		return nil, err
	}
	return &service.Manager{
		Layout:    layout,
		UID:       os.Getuid(),
		Run:       service.ExecRunner,
		VersionOf: versionOf,
		Info:      serverInfo,
	}, nil
}

// serverInfo reads /v1/info from the installed service on this machine. Over
// HTTPS the certificate names the public address, not the one connected to,
// so it is pinned instead: the server must present the configured
// certificate.
func serverInfo(ctx context.Context, man service.Manifest) (service.ServerInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	transport := &http.Transport{}
	if man.HTTPS {
		pin, err := leafCertificate(man.TLSCert())
		if err != nil {
			return service.ServerInfo{}, err
		}
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true, // replaced by the pin below
			VerifyConnection: func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) == 0 || !bytes.Equal(cs.PeerCertificates[0].Raw, pin) {
					return errors.New("the server does not present the configured certificate")
				}
				return nil
			},
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL(man.Listen, man.HTTPS), nil)
	if err != nil {
		return service.ServerInfo{}, err
	}
	res, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return service.ServerInfo{}, err
	}
	defer res.Body.Close()
	var info service.ServerInfo
	err = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&info)
	return info, err
}

// leafCertificate is the first certificate of a PEM chain, DER-encoded.
func leafCertificate(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for {
		var b *pem.Block
		if b, data = pem.Decode(data); b == nil {
			return nil, fmt.Errorf("%s holds no certificate", path)
		}
		if b.Type == "CERTIFICATE" {
			return b.Bytes, nil
		}
	}
}

// infoURL is /v1/info at the listen address, a wildcard host asked on loopback.
func infoURL(listen string, https bool) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "http://" + listen + "/v1/info"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	scheme := "http"
	if https {
		scheme = "https"
	}
	return scheme + "://" + net.JoinHostPort(host, port) + "/v1/info"
}

// serviceInstall resolves every path to an absolute one, checks the data
// directory is this server's (or new and empty with --new-server), and
// installs the agent with serve's arguments (PROTOCOL §14).
func serviceInstall(ctx context.Context, m *service.Manager, args []string, stdout, stderr io.Writer) int {
	fs := newFlags("service install", stderr)
	fs.Usage = func() { fmt.Fprint(stderr, serviceUsage) }
	dataDir := fs.String("data-dir", "", "data directory (required)")
	newServer := fs.Bool("new-server", false, "start a new server in an empty or absent --data-dir")
	listen_ := fs.String("listen", "127.0.0.1:8080", "HTTP listen address")
	publicURL := fs.String("public-url", "", "origin browsers use")
	var allowHosts hostList
	fs.Var(&allowHosts, "allow-host", "also answer requests for this host name (repeatable)")
	backupDir := fs.String("backup-dir", "", "backup directory (default <data-dir>/backups)")
	refresh := fs.Duration("refresh-interval", time.Hour, "how often the server refreshes each podcast")
	backupTime := fs.String("backup-time", "03:30", "the daily backup's local time, or off")
	binary := fs.String("binary", "", "the binary to install (default: this one)")
	tlsCert := fs.String("tls-cert", "", "serve HTTPS with this PEM certificate chain")
	tlsKey := fs.String("tls-key", "", "the certificate's PEM private key")
	allowInsecure := fs.Bool("allow-insecure-listen", false, "allow plain HTTP on an address other than loopback or Tailscale")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 || *dataDir == "" {
		fmt.Fprint(stderr, "nottennapod-server service install: --data-dir is required\n\n"+serviceUsage)
		return 2
	}
	abs := func(p string) (string, error) {
		if p == "" {
			return "", nil
		}
		return filepath.Abs(p)
	}
	data, err := abs(*dataDir)
	if err == nil {
		err = service.CheckDataDir(ctx, data, *newServer)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	backups, err := abs(*backupDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		fmt.Fprintln(stderr, "nottennapod-server service install: give both --tls-cert and --tls-key, or neither")
		return 2
	}
	cert, err := abs(*tlsCert)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	key, err := abs(*tlsKey)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	// The tailscale command, when there is one, is resolved and verified now
	// and recorded: the service asks only that one.
	tailscale, tserr := listen.FindTailscale()
	if tserr != nil {
		tailscale = ""
	}
	// The same listen rules the server applies, checked now rather than in a restart loop.
	if warning, err := listen.Check(ctx, *listen_, listen.Rules{TLS: cert != "", AllowInsecure: *allowInsecure, Tailscale: tailscale}); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	} else if warning != "" {
		fmt.Fprintln(stderr, "warning:", warning)
	}
	src := *binary
	if src == "" {
		if src, err = os.Executable(); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	if src, err = filepath.Abs(src); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	serveArgs := []string{"serve", "--data-dir", data, "--listen", *listen_, "--log-dir", m.LogDir, "--refresh-interval", refresh.String()}
	if cert != "" {
		serveArgs = append(serveArgs, "--tls-cert", cert, "--tls-key", key)
	}
	if *allowInsecure {
		serveArgs = append(serveArgs, "--allow-insecure-listen")
	}
	if tailscale != "" {
		serveArgs = append(serveArgs, "--tailscale", tailscale)
	}
	if backups != "" {
		serveArgs = append(serveArgs, "--backup-dir", backups)
	}
	if *publicURL != "" {
		serveArgs = append(serveArgs, "--public-url", *publicURL)
	}
	for _, h := range allowHosts {
		serveArgs = append(serveArgs, "--allow-host", h)
	}
	if *backupTime != "off" {
		if _, err := parseClock(*backupTime); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}
	serveArgs = append(serveArgs, "--backup-time", *backupTime)
	// ffmpeg and ffprobe are found through PATH (PROTOCOL §13): the agent's
	// minimal PATH carries the directories they resolve to now. The tailscale
	// command is not looked up on PATH: the service has its recorded path.
	var extra []string
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if p, err := exec.LookPath(tool); err == nil {
			if real, err := filepath.EvalSymlinks(p); err == nil {
				p = real
			}
			extra = append(extra, filepath.Dir(p))
		}
	}
	home, _ := os.UserHomeDir()
	plan := service.Plan{
		Label:  m.Label,
		Binary: m.BinaryPath(),
		Args:   serveArgs,
		LogDir: m.LogDir,
		Path:   service.MinimalPath(runtime.GOOS, extra...),
		Home:   home,
	}
	if err := m.Install(ctx, src, plan, service.Manifest{Label: m.Label, Listen: *listen_, HTTPS: cert != "", Args: serveArgs,
		Tailscale: tailscale}); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "Installed %s (%s).\nIt starts at login and after a crash. Logs: %s\n", m.Label, m.AgentFile, m.LogDir)
	if *newServer {
		fmt.Fprintf(stdout, "The setup code is in %s.\n", filepath.Join(data, "setup-code"))
	}
	return 0
}

// watchCertificate reloads the HTTPS certificate on SIGHUP and warns daily
// before it expires (PROTOCOL §14).
func watchCertificate(ctx context.Context, cert *listen.Certificate) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	check := func() {
		if w := cert.ExpiryWarning(time.Now()); w != "" {
			slog.Warn(w, "not_after", cert.NotAfter())
		}
	}
	check()
	daily := time.NewTicker(24 * time.Hour)
	defer daily.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			if err := cert.Reload(); err != nil {
				slog.Error("the HTTPS certificate was not reloaded; the previous one stays", "error", err)
			} else {
				slog.Info("the HTTPS certificate was reloaded", "not_after", cert.NotAfter())
			}
			check()
		case <-daily.C:
			check()
		}
	}
}

// parseClock reads a local time of day, HH:MM.
func parseClock(v string) (time.Duration, error) {
	t, err := time.Parse("15:04", v)
	if err != nil {
		return 0, fmt.Errorf("%q is not a time of day like 03:30", v)
	}
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute, nil
}

// printVersion is --version.
func printVersion(stdout io.Writer) int {
	fmt.Fprintf(stdout, "nottennapod-server %s\n", version.Version)
	return 0
}

// logToFile is set when the server logs to its own file (as a service): the
// setup code then stays out of the log.
var logToFile bool

// openLog makes dir's log the server's log and its standard output and error
// (PROTOCOL §14), checking every minute for output written around it.
func openLog(ctx context.Context, dir string) (func(), error) {
	l, err := logfile.Open(dir)
	if err != nil {
		return nil, err
	}
	logToFile = true
	slog.SetDefault(slog.New(slog.NewTextHandler(l, nil)))
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				if err := l.Check(); err != nil {
					slog.Warn("log rotation failed", "error", err)
				}
			}
		}
	}()
	slog.Info("nottennapod-server starting", "version", version.Version, "pid", os.Getpid())
	return func() { close(done) }, nil
}
