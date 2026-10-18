package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/api"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/backupsched"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/clock"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/httpserver"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/listen"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/media"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	apiviews "github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/webui"
)

const usage = `Usage:
  nottennapod-server [--listen ADDR] [--data-dir D] [--backup-dir DIR] [--public-url URL] [--allow-host NAME]...
      [--refresh-interval DUR]
      serve (the default)
  nottennapod-server backup --data-dir D --out DIR
      write a verified backup while the server is stopped
  nottennapod-server keys export --data-dir D --out FILE [--passphrase-file F]
      write the server's keys, protected by a passphrase; one export serves every backup
  nottennapod-server keys install --data-dir D --keys FILE [--passphrase-file F]
      put lost keys back from the key bundle while the server is stopped
  nottennapod-server restore --data-dir D --from BACKUP --keys FILE [--passphrase-file F]
      restore a backup while the server is stopped
  nottennapod-server service install | update | start | stop | restart | status | uninstall
      run the server at login and after a crash (see: nottennapod-server service)
  nottennapod-server --version
`

// noRecoveryKeyWarning is the start log's warning until a recovery key (the
// key bundle) is saved, in plain words: the web app saves one. OPERATIONS
// describes the command for people who prefer it.
const noRecoveryKeyWarning = "This server has no saved recovery key. If this computer is lost or replaced, " +
	"its backups can only be put back with the recovery key. To save it, open the NotTennaPod web app as an administrator, " +
	"go to Settings and choose Save recovery key, then keep the file and its passphrase somewhere safe, apart from the backups."

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run dispatches a subcommand (PROTOCOL §11); without one it
// serves, with the same flags as before.
func run(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "--version" || args[0] == "-version") {
		return printVersion(stdout)
	}
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, rest := args[0], args[1:]
		switch cmd {
		case "serve":
			return serve(ctx, rest)
		case "backup":
			return backup(ctx, rest, stdout, stderr)
		case "keys":
			switch {
			case len(rest) > 0 && rest[0] == "export":
				return exportKeys(ctx, rest[1:], stdin, stdout, stderr)
			case len(rest) > 0 && rest[0] == "install":
				return installKeys(ctx, rest[1:], stdin, stdout, stderr)
			}
			fmt.Fprint(stderr, "nottennapod-server keys: the commands are export and install\n\n"+usage)
			return 2
		case "restore":
			return restore(ctx, rest, stdin, stdout, stderr)
		case "service":
			return serviceCmd(ctx, rest, stdout, stderr)
		case "version":
			return printVersion(stdout)
		case "help":
			fmt.Fprint(stdout, usage)
			return 0
		default:
			fmt.Fprintf(stderr, "nottennapod-server: unknown command %q\n\n%s", cmd, usage)
			return 2
		}
	}
	return serve(ctx, args)
}

func defaultDataDir() string {
	dir, err := storage.DefaultDataDir()
	if err != nil {
		slog.Error("cannot determine the default data directory", "error", err)
		os.Exit(1)
	}
	return dir
}

// newFlags returns a flag set that reports errors with the usage text.
func newFlags(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	return fs
}

func serve(ctx context.Context, args []string) int {
	fs := newFlags("nottennapod-server", os.Stderr)
	address := fs.String("listen", "127.0.0.1:8080", "HTTP listen address")
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory (database, lock file)")
	backupDir := fs.String("backup-dir", "", "where POST /v1/admin/backup writes backups (default <data-dir>/backups)")
	publicURL := fs.String("public-url", "", "origin browsers use, e.g. https://macbook.tailnet.ts.net behind tailscale serve")
	var allowHosts hostList
	fs.Var(&allowHosts, "allow-host", "also answer requests for this host name (repeatable); loopback, the listen address, --public-url's host "+
		"and this machine's Tailscale names are always answered")
	refreshInterval := fs.Duration("refresh-interval", time.Hour, "how often the server refreshes each podcast (±10 % jitter)")
	logDir := fs.String("log-dir", "", "write the log to this private directory, rotated (service install sets it)")
	tlsCert := fs.String("tls-cert", "", "serve HTTPS with this PEM certificate chain (reloaded on SIGHUP)")
	tlsKey := fs.String("tls-key", "", "the certificate's PEM private key")
	allowInsecure := fs.Bool("allow-insecure-listen", false, "allow plain HTTP on an address other than loopback or this machine's Tailscale address")
	tailscale := fs.String("tailscale", "", "the tailscale command for the listen check, an absolute path (service install records it; default: found and verified)")
	backupTime := fs.String("backup-time", "03:30", "the daily backup's local time (±15 min), or off; the last 30 are kept in <backup-dir>/daily")
	webDir := fs.String("web-dir", "", "serve the web app from this build directory instead of the embedded one (development)")
	loopbackFeeds := fs.Bool("dev-allow-loopback-feeds", false, "development and tests only: let the server fetch feeds and media from this machine (127.0.0.1)")
	clockOffset := fs.Duration("dev-clock-offset", 0, "development and tests only: move the server's wall clock by this much (e.g. 72h, -72h): "+
		"receive times, so time normalization, the refresh and backup schedules, and the times it records; never timers, retries or time-outs")
	ffmpegPath := fs.String("ffmpeg", "", "the ffmpeg binary for browser media compatibility (default: ffmpeg on PATH)")
	ffprobePath := fs.String("ffprobe", "", "the ffprobe binary (default: next to --ffmpeg, else on PATH)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		slog.Error("unexpected positional arguments")
		return 2
	}
	if *logDir != "" {
		closeLog, err := openLog(ctx, *logDir)
		if err != nil {
			slog.Error("cannot open the log", "dir", *logDir, "error", err)
			return 1
		}
		defer closeLog()
	}
	// The server's one wall clock (PROTOCOL §16): what it accepts,
	// normalizes, schedules and records by. Monotonic time stays the system's.
	wall := clock.Offset(*clockOffset)
	if *clockOffset != 0 {
		slog.Warn("the server's clock is moved (--dev-clock-offset): for development and tests only", "offset", *clockOffset,
			"server_time", wall.Now().UTC().Format(time.RFC3339))
	}
	store, err := storage.Open(ctx, *dataDir, storage.WithClock(wall.Now))
	if err != nil {
		slog.Error("cannot open the data directory", "dir", *dataDir, "error", err)
		return 1
	}
	defer store.Close()
	// service update's readiness check asks for this run's instance ID.
	if _, err := store.WriteInstance(); err != nil {
		slog.Error("cannot write the instance ID", "error", err)
		return 1
	}
	var backups *backupsched.Scheduler
	// Daily backups (PROTOCOL §14): a verified copy at a quiet hour,
	// missed days coalesced, the last 30 kept.
	if *backupTime != "off" {
		at, err := parseClock(*backupTime)
		if err != nil {
			slog.Error("invalid --backup-time", "error", err)
			return 2
		}
		backups = backupsched.New(store, backupsched.Config{BackupDir: *backupDir, At: at, AtSet: true, Clock: wall})
		if err := backups.Start(ctx); err != nil {
			slog.Error("cannot start the daily backups", "error", err)
			return 1
		}
		defer backups.Stop()
	}
	if store.KeysExportedAt() == 0 {
		slog.Warn(noRecoveryKeyWarning)
	}
	setup, err := setupState(ctx, store)
	if err != nil {
		slog.Error("cannot prepare setup", "error", err)
		return 1
	}
	views, err := protocol.NewViewRegistry(filepath.Join(store.Dir(), "views"), nil)
	if err != nil {
		slog.Error("cannot prepare snapshot views", "error", err)
		return 1
	}
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		slog.Error("cannot load the secrets key", "error", err)
		return 1
	}
	registry := protocol.NewRegistry()
	ops.Register(registry)
	sync := &protocol.Service{Store: store, Registry: registry, Now: wall.Now, Views: views, Box: box}
	views.Sweep(sync.CurrentLineage(ctx)) // before serving: drop expired and stale-lineage views
	go sync.SweepViews(ctx, 10*time.Minute)
	// The server's own feed refresh (PROTOCOL §9.4): stopped before the
	// store closes.
	fetcher := feed.NewFetcher()
	if *loopbackFeeds {
		slog.Warn("fetching feeds from this machine is allowed (--dev-allow-loopback-feeds): for development and tests only")
		fetcher.AllowLoopback(func(string) bool { return true })
	}
	// Library imports (PROTOCOL §11.5): staged in <data-dir>/imports, resumed
	// at start, stopped before the store closes. A published import counts its
	// podcasts' first refresh from the refresher's ended attempts.
	imports := &importer.Runner{Store: store, Service: sync, Dir: filepath.Join(store.Dir(), "imports"),
		OpenStaging: importer.StagingOpener(store)}
	refresher := refresh.New(sync, refresh.Config{Interval: *refreshInterval, Fetcher: fetcher, Clock: wall,
		AfterAttempt: imports.AfterAttempt})
	imports.Cohort = refresh.Cohort
	// The read API's list views (PROTOCOL §12), in memory; the folder where
	// earlier versions kept them, <data-dir>/read-views, is removed.
	readViews, err := apiviews.NewSpool(filepath.Join(store.Dir(), "read-views"), nil)
	if err != nil {
		slog.Error("cannot prepare read views", "error", err)
		return 1
	}
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				readViews.Sweep()
			}
		}
	}()
	// Browser media (PROTOCOL §13): the relay, and the compatibility path
	// when ffmpeg is installed; private copies in <data-dir>/media-spool,
	// emptied at start.
	mediaSvc, err := media.New(media.Config{Fetcher: fetcher, FFmpeg: *ffmpegPath, FFprobe: *ffprobePath,
		SpoolDir: filepath.Join(store.Dir(), "media-spool"), DataDir: store.Dir()})
	if err != nil {
		slog.Error("cannot prepare browser media", "error", err)
		return 1
	}
	mediaSvc.Start(ctx)
	defer mediaSvc.Wait()
	defer mediaSvc.Close()
	// The service page (PROTOCOL §14) reports what this run serves.
	info := &serviceInfo{store: store, backups: backups, backupDir: *backupDir, refresher: refresher, imports: imports, started: time.Now(), listen: *address,
		https: *tlsCert != ""}
	handler, err := api.New(api.Config{Store: store, PublicURL: *publicURL, Setup: setup, Protocol: sync, Refresher: refresher, Service: info.report, Now: wall.Now,
		BackupDir: *backupDir, Imports: imports, Views: readViews, Fetcher: fetcher, Media: mediaSvc})
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		return 1
	}
	defer handler.Close() // the read model's warmers stop before the store closes
	if err := imports.Start(ctx); err != nil {
		slog.Error("cannot resume library imports", "error", err)
		return 1
	}
	defer imports.Stop()
	refresher.Start(ctx)
	defer refresher.Stop()
	// Recovery upload jobs (PROTOCOL §6.11): resumed at start, run in the
	// server's lifetime, stopped (their rows kept) before the store closes.
	jobsCtx, stopRecoveryJobs := context.WithCancel(ctx)
	waitRecoveryJobs, err := sync.StartRecoveryJobs(jobsCtx)
	if err != nil {
		stopRecoveryJobs()
		slog.Error("cannot resume recovery uploads", "error", err)
		return 1
	}
	defer func() {
		stopRecoveryJobs()
		waitRecoveryJobs()
	}()
	web := webui.Handler()
	if *webDir != "" {
		web = webui.DirHandler(*webDir)
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		slog.Error("give both --tls-cert and --tls-key, or neither")
		return 2
	}
	// Plain HTTP only on loopback or this machine's Tailscale address (PROTOCOL §14).
	if warning, err := listen.Check(ctx, *address, listen.Rules{TLS: *tlsCert != "", AllowInsecure: *allowInsecure, Tailscale: *tailscale}); err != nil {
		slog.Error("refusing to listen", "error", err)
		return 1
	} else if warning != "" {
		slog.Warn(warning)
		info.addWarning(warning)
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		slog.Error("listen failed", "error", err)
		return 1
	}
	// The server never fetches its own API as a feed or as media.
	fetcher.RefuseListener(listener.Addr())
	if *tlsCert != "" {
		cert, err := listen.LoadCertificate(*tlsCert, *tlsKey)
		if err != nil {
			listener.Close()
			slog.Error("cannot load the HTTPS certificate", "error", err)
			return 1
		}
		listener = tls.NewListener(listener, cert.Config())
		go watchCertificate(ctx, cert)
		info.cert = cert
	}
	slog.Info("server listening", "address", listener.Addr(), "data_dir", store.Dir(), "sync_available", handler.SyncAvailable())
	// The warm-ups: once serving, the queue replay (PROTOCOL §7.6) and then
	// the read model (§12) of each account active in the last 30 days are
	// rebuilt in the background, one account at a time, so the first queue
	// command and the first read after a restart find them ready. They end
	// before the store closes.
	warmed := make(chan struct{})
	go func() {
		defer close(warmed)
		ops.WarmQueues(ctx, sync)
		handler.WarmLibraries(ctx)
	}()
	defer func() { <-warmed }()
	// Only known Host names are answered.
	hosts := serverHosts(*address, *publicURL, allowHosts, *tailscale)
	if err := httpserver.Serve(ctx, listener, httpserver.Config{API: handler, Web: web, Hosts: hosts}); err != nil {
		slog.Error("server stopped with an error", "error", err)
		return 1
	}
	return 0
}

// openStopped opens an existing data directory for a command that needs the
// server stopped (it takes the same lock). A directory without a database is
// refused rather than set up as a new server (a mistyped --data-dir).
func openStopped(ctx context.Context, dataDir string, stderr io.Writer) (*storage.Store, bool) {
	if _, err := os.Stat(filepath.Join(dataDir, "nottennapod.db")); err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: %s holds no NotTennaPod server database\n", dataDir)
		return nil, false
	}
	store, err := storage.Open(ctx, dataDir)
	if errors.Is(err, storage.ErrLocked) {
		fmt.Fprintf(stderr, "nottennapod-server: the server is running on %s; stop it first (a running server makes backups from the admin page)\n", dataDir)
		return nil, false
	}
	if err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: cannot open the data directory %s: %v\n", dataDir, err)
		return nil, false
	}
	return store, true
}

// backupGuard keeps the backup command's disks safe (tests change it).
var backupGuard backupsched.Guard

// backup writes a verified, compressed backup with the server stopped
// (PROTOCOL §11).
func backup(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlags("backup", stderr)
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory")
	out := fs.String("out", "", "directory to write the backup into")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" || fs.NArg() != 0 {
		fmt.Fprint(stderr, "nottennapod-server backup: --out is required\n\n"+usage)
		return 2
	}
	store, ok := openStopped(ctx, *dataDir, stderr)
	if !ok {
		return 1
	}
	defer store.Close()
	// The same free-space guard as the server's own backups (PROTOCOL §11.2).
	info, err := backupsched.Manual(ctx, store, *out, backupGuard)
	if err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: backup failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Wrote %s (%d bytes).\n", info.Path, info.Size)
	return 0
}

// exportKeys writes the protected key bundle (PROTOCOL §11).
func exportKeys(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) int {
	fs := newFlags("keys export", stderr)
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory")
	out := fs.String("out", "", "key bundle file to write")
	passFile := fs.String("passphrase-file", "", "read the passphrase from this file instead of the terminal")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" || fs.NArg() != 0 {
		fmt.Fprint(stderr, "nottennapod-server keys export: --out is required\n\n"+usage)
		return 2
	}
	pass, err := passphrase(*passFile, true, stdin, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: %v\n", err)
		return 1
	}
	store, ok := openStopped(ctx, *dataDir, stderr)
	if !ok {
		return 1
	}
	defer store.Close()
	if err := store.ExportKeys(ctx, *out, pass); err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: exporting the keys failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Wrote the key bundle to %s. Keep it, and its passphrase, apart from the backups: restoring any backup needs both.\n", *out)
	return 0
}

// installKeys puts lost keys back from the key bundle: the bundle must belong to this server and
// verify its database.
func installKeys(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) int {
	fs := newFlags("keys install", stderr)
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory")
	keysFile := fs.String("keys", "", "key bundle file (from keys export)")
	passFile := fs.String("passphrase-file", "", "read the passphrase from this file instead of the terminal")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *keysFile == "" || fs.NArg() != 0 {
		fmt.Fprint(stderr, "nottennapod-server keys install: --keys is required\n\n"+usage)
		return 2
	}
	bundle, err := os.ReadFile(*keysFile)
	if err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: %v\n", err)
		return 1
	}
	pass, err := passphrase(*passFile, false, stdin, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: %v\n", err)
		return 1
	}
	if err := storage.InstallKeys(ctx, *dataDir, bundle, pass); err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: installing the keys failed: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "The keys are back. The server can start.")
	return 0
}

// restore runs the restore procedure (PROTOCOL §11) with the server stopped.
func restore(ctx context.Context, args []string, stdin *os.File, stdout, stderr io.Writer) int {
	fs := newFlags("restore", stderr)
	dataDir := fs.String("data-dir", defaultDataDir(), "data directory")
	from := fs.String("from", "", "backup file to restore")
	keysFile := fs.String("keys", "", "key bundle file (from keys export)")
	passFile := fs.String("passphrase-file", "", "read the passphrase from this file instead of the terminal")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *from == "" || *keysFile == "" || fs.NArg() != 0 {
		fmt.Fprint(stderr, "nottennapod-server restore: --from and --keys are required\n\n"+usage)
		return 2
	}
	bundle, err := os.ReadFile(*keysFile)
	if err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: cannot read the key bundle: %v\n", err)
		return 1
	}
	pass, err := passphrase(*passFile, false, stdin, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "nottennapod-server: %v\n", err)
		return 1
	}
	_, err = storage.Restore(ctx, storage.RestoreOptions{DataDir: *dataDir, Backup: *from, Bundle: bundle, Passphrase: pass, Out: stdout})
	switch {
	case errors.Is(err, storage.ErrLocked):
		fmt.Fprintf(stderr, "nottennapod-server: the server is running on %s; stop it before restoring\n", *dataDir)
		return 1
	case err != nil:
		fmt.Fprintf(stderr, "nottennapod-server: %v\n", err)
		return 1
	}
	return 0
}

// setupState returns the setup code holder: a fresh code (logged and written to
// the data directory) while no administrator exists, otherwise an empty one.
func setupState(ctx context.Context, store *storage.Store) (*api.SetupState, error) {
	var hasAdmin bool
	err := store.Read(ctx, func(r *storage.ReadTx) error {
		var err error
		hasAdmin, err = r.HasAdmin()
		return err
	})
	if err != nil {
		return nil, err
	}
	path := filepath.Join(store.Dir(), "setup-code")
	if hasAdmin {
		_ = os.Remove(path)
		return &api.SetupState{}, nil
	}
	st, err := api.NewSetupState(path)
	if err != nil {
		return nil, err
	}
	if logToFile {
		// The code is never written to the service's log (PROTOCOL §14).
		slog.Info("this server is not set up yet: open the web app and enter the setup code from the file", "file", path)
	} else {
		slog.Info("this server is not set up yet: open the web app and enter the setup code", "setup_code", st.Code(), "file", path)
	}
	return st, nil
}
