package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/backupsched"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/service"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/version"
)

func runCmd(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	devNull, _ := os.Open(os.DevNull)
	defer devNull.Close()
	code := run(context.Background(), args, devNull, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestVersion(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}} {
		if code, out, _ := runCmd(t, args...); code != 0 || out != "nottennapod-server "+version.Version+"\n" {
			t.Errorf("%v: %d %q", args, code, out)
		}
	}
}

// service install refuses what would run the wrong server, before it touches
// launchd or systemd (PROTOCOL §14). Nothing here installs a service.
func TestServiceInstallRefusesUnrecognizedDataDirectories(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("NTP_SERVICE_LABEL", "app.nottennapod.server.test")
	if code, _, errs := runCmd(t, "service", "install"); code != 2 || !strings.Contains(errs, "--data-dir is required") {
		t.Errorf("no --data-dir: %d %s", code, errs)
	}
	missing := filepath.Join(home, "nowhere")
	if code, _, errs := runCmd(t, "service", "install", "--data-dir", missing); code != 1 || !strings.Contains(errs, "--new-server") {
		t.Errorf("a missing data directory: %d %s", code, errs)
	}
	other := filepath.Join(home, "Documents")
	os.Mkdir(other, 0o700)
	os.WriteFile(filepath.Join(other, "letter.txt"), []byte("x"), 0o600)
	if code, _, errs := runCmd(t, "service", "install", "--data-dir", other, "--new-server"); code != 1 || !strings.Contains(errs, "not empty") {
		t.Errorf("a non-empty directory for a new server: %d %s", code, errs)
	}
	for _, d := range []string{"Library", ".config"} {
		if _, err := os.Stat(filepath.Join(home, d)); err == nil {
			t.Errorf("a refused install wrote %s", d)
		}
	}
	if code, _, errs := runCmd(t, "service", "frobnicate"); code != 2 || !strings.Contains(errs, "unknown command") {
		t.Errorf("unknown command: %d %s", code, errs)
	}
}

// The service page's report: what the owner needs, as JSON, with the
// keys-not-exported warning until `keys export` (PROTOCOL §14).
func TestServiceReport(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sched := backupsched.New(store, backupsched.Config{})
	info := &serviceInfo{store: store, backups: sched, started: time.Now().Add(-time.Minute), listen: "127.0.0.1:8080"}
	info.addWarning("serving plain HTTP on every network interface")
	v, err := info.report(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Version  string   `json:"version"`
		Uptime   int64    `json:"uptime_seconds"`
		Schema   int      `json:"schema_version"`
		Warnings []string `json:"warnings"`
		Backups  struct {
			Dir  string `json:"dir"`
			Kept int    `json:"kept"`
		} `json:"backups"`
		Listen struct {
			Address string `json:"address"`
			HTTPS   bool   `json:"https"`
		} `json:"listen"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Version == "" || got.Uptime < 59 || got.Schema == 0 || got.Backups.Dir == "" || got.Listen.Address != "127.0.0.1:8080" {
		t.Errorf("report %s", b)
	}
	if len(got.Warnings) != 2 || got.Warnings[1] != "keys_not_exported" {
		t.Errorf("warnings %v", got.Warnings)
	}
}

// update's check asks the running server over HTTPS when it serves HTTPS, and a
// wildcard address on loopback. The HTTPS check pins the configured
// certificate.
func TestUpdateCheckSpeaksTheInstallationsScheme(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"version":"2","instance_id":"abc"}`))
	}))
	defer srv.Close()
	certFile := func(der []byte) string {
		p := filepath.Join(t.TempDir(), "cert.pem")
		os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
		return p
	}
	listen := strings.TrimPrefix(srv.URL, "https://")
	man := service.Manifest{Listen: listen, HTTPS: true, Args: []string{"serve", "--tls-cert", certFile(srv.Certificate().Raw)}}
	if info, err := serverInfo(context.Background(), man); err != nil || info.Version != "2" || info.InstanceID != "abc" {
		t.Errorf("an HTTPS installation's check: %+v %v", info, err)
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "127.0.0.1"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	otherCert, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	man.Args[2] = certFile(otherCert)
	if _, err := serverInfo(context.Background(), man); err == nil || !strings.Contains(err.Error(), "configured certificate") {
		t.Errorf("a server with another certificate was asked: %v", err)
	}
	for listen, want := range map[string]string{
		"0.0.0.0:8443":   "https://127.0.0.1:8443/v1/info",
		"[::]:8443":      "https://127.0.0.1:8443/v1/info",
		"127.0.0.1:8080": "https://127.0.0.1:8080/v1/info",
	} {
		if got := infoURL(listen, true); got != want {
			t.Errorf("infoURL(%s) = %s, want %s", listen, got, want)
		}
	}
	if got := infoURL("127.0.0.1:8080", false); got != "http://127.0.0.1:8080/v1/info" {
		t.Errorf("plain: %s", got)
	}
}

// The service page lists the files of deleted imports that could not be
// removed, with their paths.
func TestServiceReportListsImportFilesNotRemoved(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	dir := filepath.Join(t.TempDir(), "imports")
	upload := filepath.Join(dir, "5d1c1c2e-7c1e-4b9a-9f0e-2a1b3c4d5e6f.upload")
	if err := os.MkdirAll(upload, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(upload, "000000"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(upload, 0o500); err != nil { // the chunk cannot be unlinked
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(upload, 0o700) })
	imports := &importer.Runner{Store: store, Dir: dir}
	if err := imports.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer imports.Stop()
	info := &serviceInfo{store: store, imports: imports, started: time.Now(), listen: "127.0.0.1:8080"}
	v, err := info.report(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Warnings      []string `json:"warnings"`
		ImportCleanup struct {
			Pending int      `json:"pending"`
			Paths   []string `json:"paths"`
		} `json:"import_cleanup"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.ImportCleanup.Pending != 1 || len(got.ImportCleanup.Paths) != 1 || got.ImportCleanup.Paths[0] != upload {
		t.Errorf("report %s", b)
	}
	want := "files of deleted imports that could not be removed: 1 (" + upload + ")"
	if !strings.Contains(strings.Join(got.Warnings, "\n"), want) {
		t.Errorf("warnings %v", got.Warnings)
	}
}
