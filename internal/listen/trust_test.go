package listen

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

func fakeTailscale(t *testing.T, mode os.FileMode, addrs ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "tailscale")
	if err := os.WriteFile(p, []byte("#!/bin/sh\necho '"+strings.Join(addrs, "\n")+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	os.Chmod(p, mode)
	return p
}

// A fake tailscale earlier on PATH cannot make a LAN address pass.
func TestFakeTailscaleCannotBlessALANAddress(t *testing.T) {
	t.Setenv("PATH", filepath.Dir(fakeTailscale(t, 0o700, "192.168.1.20")))
	_, err := Check(context.Background(), "192.168.1.20:8080", Rules{})
	if err == nil || !strings.Contains(err.Error(), "not a Tailscale address") {
		t.Fatalf("a LAN address reported by a fake tailscale: %v", err)
	}
	// Nor can the list given to the check.
	if _, err := Check(context.Background(), "192.168.1.20:8080", Rules{TailscaleAddrs: tailnet("192.168.1.20")}); err == nil {
		t.Fatal("a LAN address in the Tailscale list passed")
	}
}

// The tailscale command must belong to root or the server's user
// and not be writable by others; a recorded path is the only one asked.
func TestTailscaleCommandIsVerifiedAndPinned(t *testing.T) {
	ctx := context.Background()
	writable := fakeTailscale(t, 0o722, "100.100.1.1")
	if err := VerifyTool(writable); err == nil || !strings.Contains(err.Error(), "changed by other users") {
		t.Fatalf("a tailscale writable by others: %v", err)
	}
	if _, err := Check(ctx, "100.100.1.1:8080", Rules{Tailscale: writable}); err == nil {
		t.Fatal("a tailscale writable by others was asked")
	}
	if err := VerifyTool("tailscale"); err == nil {
		t.Fatal("a relative tailscale path was accepted")
	}
	pinned := fakeTailscale(t, 0o700, "100.100.1.1")
	t.Setenv("PATH", filepath.Dir(fakeTailscale(t, 0o700, "100.100.2.2")))
	if _, err := Check(ctx, "100.100.1.1:8080", Rules{Tailscale: pinned}); err != nil {
		t.Fatalf("the pinned tailscale's address: %v", err)
	}
	if _, err := Check(ctx, "100.100.2.2:8080", Rules{Tailscale: pinned}); err == nil {
		t.Fatal("the tailscale on PATH was asked instead of the pinned one")
	}
	if found, err := FindTailscale(); err != nil || !filepath.IsAbs(found) {
		t.Fatalf("FindTailscale: %q %v", found, err)
	}
}

// A TLS key readable by others is refused, through a symlink too,
// at start and on reload; the message says how to fix it.
func TestPublicTLSKeyIsRefused(t *testing.T) {
	d := t.TempDir()
	cf, kf := writeCert(t, d, "server", 1, time.Now().Add(time.Hour))
	outside := filepath.Join(t.TempDir(), "public.key")
	os.Rename(kf, outside)
	os.Symlink(outside, kf)
	os.Chmod(outside, 0o644)
	if _, err := LoadCertificate(cf, kf); !errors.Is(err, private.ErrExposed) || !strings.Contains(err.Error(), "chmod 600") {
		t.Fatalf("a world-readable key: %v", err)
	}
	os.Chmod(outside, 0o600)
	c, err := LoadCertificate(cf, kf)
	if err != nil {
		t.Fatalf("a private key through a symlink: %v", err)
	}
	before := c.NotAfter()
	writeCert(t, d, "server", 2, time.Now().Add(48*time.Hour))
	os.Chmod(kf, 0o640)
	if err := c.Reload(); !errors.Is(err, private.ErrExposed) {
		t.Fatalf("reload of a group-readable key: %v", err)
	}
	if !c.NotAfter().Equal(before) {
		t.Fatal("the refused key replaced the certificate in use")
	}
}

// The Host check answers the MagicDNS name and Tailscale
// addresses that `tailscale status --json` reports for this machine, without
// the name's final dot and only inside Tailscale's ranges.
func TestTailscaleStatusNames(t *testing.T) {
	addrs, name, err := parseStatus([]byte(`{"Self":{"DNSName":"macbook.tail1234.ts.net.","TailscaleIPs":["100.101.102.103","fd7a:115c:a1e0::1","192.168.1.5"]},"Peer":{}}`))
	if err != nil || name != "macbook.tail1234.ts.net" || len(addrs) != 2 || addrs[0].String() != "100.101.102.103" {
		t.Fatalf("%v %q %v", addrs, name, err)
	}
	if _, _, err := parseStatus([]byte(`not json`)); err == nil {
		t.Fatal("a broken status was read")
	}
}

// The tailscale command is trusted only when no other user can replace it or
// anything on its way, symlinks and their targets included; the Mac app's
// bundle path keeps working.
func TestTailscalePathMustBeBeyondOtherUsers(t *testing.T) {
	d := t.TempDir()
	os.Chmod(d, 0o777)
	p := filepath.Join(d, "tailscale")
	os.WriteFile(p, []byte("#!/bin/sh\necho 100.64.1.2\n"), 0o755)
	if err := VerifyTool(p); !errors.Is(err, private.ErrUntrusted) {
		t.Fatalf("a tailscale under a directory anyone can write: %v", err)
	}
	if _, err := Check(context.Background(), "100.64.1.2:8080", Rules{Tailscale: p}); err == nil {
		t.Fatal("its answer was used")
	}
	good := t.TempDir()
	link := filepath.Join(good, "tailscale")
	os.Symlink(p, link)
	if err := VerifyTool(link); !errors.Is(err, private.ErrUntrusted) {
		t.Fatalf("a symlink to it from a private directory: %v", err)
	}
	app := filepath.Join(t.TempDir(), "Tailscale.app", "Contents", "MacOS", "Tailscale")
	os.MkdirAll(filepath.Dir(app), 0o755)
	os.WriteFile(app, []byte("#!/bin/sh\necho 100.64.1.2\n"), 0o755)
	old := cliPaths
	defer func() { cliPaths = old }()
	cliPaths = []string{"tailscale-not-installed", app}
	want, _ := filepath.EvalSymlinks(app)
	if got, err := FindTailscale(); err != nil || got != want {
		t.Fatalf("the app's bundle path: %q %v", got, err)
	}
}

// A --tailscale given as a symlink runs the verified file it resolves to, never
// the spelling given.
func TestTailscaleRunsTheVerifiedFile(t *testing.T) {
	d := t.TempDir()
	real := filepath.Join(d, "real-tailscale")
	os.WriteFile(real, []byte("#!/bin/sh\necho 100.64.1.2\n"), 0o700)
	link := filepath.Join(d, "tailscale")
	os.Symlink(real, link)
	got, err := ResolveTool(link)
	want, _ := filepath.EvalSymlinks(real)
	if err != nil || got != want {
		t.Fatalf("ResolveTool(%s) = %q %v, want %q", link, got, err, want)
	}
	if _, err := Check(context.Background(), "100.64.1.2:8080", Rules{Tailscale: link}); err != nil {
		t.Fatalf("through the link: %v", err)
	}
}
