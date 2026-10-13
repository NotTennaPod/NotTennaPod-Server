package listen

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func tailnet(addrs ...string) func(context.Context) ([]netip.Addr, error) {
	return func(context.Context) ([]netip.Addr, error) {
		var out []netip.Addr
		for _, a := range addrs {
			out = append(out, netip.MustParseAddr(a))
		}
		return out, nil
	}
}

func TestPlainHTTPOnlyOnLoopbackOrVerifiedTailscale(t *testing.T) {
	ctx := context.Background()
	ts := Rules{TailscaleAddrs: tailnet("100.101.102.103", "fd7a:115c:a1e0::1")}
	for _, ok := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080", "127.0.0.2:9", "100.101.102.103:8080", "[fd7a:115c:a1e0::1]:8080"} {
		if w, err := Check(ctx, ok, ts); err != nil || w != "" {
			t.Errorf("%s refused: %v %q", ok, err, w)
		}
	}
	for _, bad := range []string{
		"0.0.0.0:8080", "[::]:8080", ":8080", // wildcards, IPv4 and IPv6
		"192.168.1.20:8080",  // the LAN
		"100.64.0.7:8080",    // in Tailscale's range, but not this machine's Tailscale address
		"macbook.local:8080", // a name
	} {
		if _, err := Check(ctx, bad, ts); err == nil {
			t.Errorf("%s accepted for plain HTTP", bad)
		}
		if w, err := Check(ctx, bad, Rules{TailscaleAddrs: ts.TailscaleAddrs, AllowInsecure: true}); err != nil || w == "" {
			t.Errorf("%s with --allow-insecure-listen: %v, warning %q", bad, err, w)
		}
		if w, err := Check(ctx, bad, Rules{TLS: true}); err != nil || w != "" {
			t.Errorf("%s with TLS: %v %q", bad, err, w)
		}
	}
	unknown := Rules{TailscaleAddrs: func(context.Context) ([]netip.Addr, error) { return nil, errors.New("not installed") }}
	if _, err := Check(ctx, "100.101.102.103:8080", unknown); err == nil || !strings.Contains(err.Error(), "cannot be checked") {
		t.Errorf("an unverifiable Tailscale address: %v", err)
	}
	if got := parseAddrs("100.101.102.103\nfd7a:115c:a1e0::1\n"); len(got) != 2 {
		t.Errorf("parse %v", got)
	}
}

func writeCert(t *testing.T, dir, name string, serial int64, notAfter time.Time) (string, string) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "nottennapod.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	cf, kf := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	os.WriteFile(cf, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(kf, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
	return cf, kf
}

func TestCertificateServesReloadsAndWarns(t *testing.T) {
	dir := t.TempDir()
	cf, kf := writeCert(t, dir, "server", 1, time.Now().Add(90*24*time.Hour))
	c, err := LoadCertificate(cf, kf)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })}
	go srv.Serve(tls.NewListener(ln, c.Config()))
	defer srv.Close()
	serial := func() int64 {
		conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		return conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64()
	}
	if s := serial(); s != 1 {
		t.Fatalf("serial %d", s)
	}
	if w := c.ExpiryWarning(time.Now()); w != "" {
		t.Errorf("warning for a 90-day certificate: %q", w)
	}
	// A renewed certificate in the same files is served after a reload (SIGHUP).
	writeCert(t, dir, "server", 2, time.Now().Add(10*24*time.Hour))
	if err := c.Reload(); err != nil {
		t.Fatal(err)
	}
	if s := serial(); s != 2 {
		t.Fatalf("after reload serial %d", s)
	}
	if w := c.ExpiryWarning(time.Now()); !strings.Contains(w, "expires in") {
		t.Errorf("no warning for a certificate expiring in 10 days: %q", w)
	}
	if w := c.ExpiryWarning(time.Now().Add(11 * 24 * time.Hour)); !strings.Contains(w, "expired") {
		t.Errorf("no warning for an expired certificate: %q", w)
	}
	// A broken file keeps the certificate in use.
	os.WriteFile(cf, []byte("not a certificate"), 0o600)
	if err := c.Reload(); err == nil {
		t.Error("a broken certificate file was accepted")
	}
	if s := serial(); s != 2 {
		t.Errorf("a failed reload changed the certificate: %d", s)
	}
}
