package webui

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingConn counts the bytes a client reads from the wire.
type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.n.Add(int64(n))
	return n, err
}

// The app's text (its page, scripts and styles) goes to a browser that accepts
// gzip compressed, decoded to exactly the file's bytes; pictures go as they
// are, and so does everything to a browser that refuses gzip.
func TestAppIsSentCompressed(t *testing.T) {
	dir := t.TempDir()
	files := map[string][]byte{
		"index.html":       []byte("<!doctype html><title>NotTennaPod</title>" + strings.Repeat("<div class=\"row\">text</div>\n", 200)),
		"assets/app-1.js":  []byte(strings.Repeat("export function f(a,b){return a+b}\n", 3000)),
		"assets/app-1.css": []byte(strings.Repeat(".row{display:flex;gap:8px}\n", 500)),
		"assets/tiny.css":  []byte(".a{}"),
		"assets/icon.png":  bytes.Repeat([]byte{0x89, 'P', 'N', 'G', 0, 1, 2, 3}, 1000),
	}
	for name, data := range files {
		os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o700)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(DirHandler(dir))
	defer srv.Close()
	get := func(method, path, acceptEncoding string) (*http.Response, []byte, int64) {
		var wire atomic.Int64
		dialer := &net.Dialer{}
		tr := &http.Transport{DisableCompression: true, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return countingConn{c, &wire}, nil
		}}
		defer tr.CloseIdleConnections()
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		if acceptEncoding != "" {
			req.Header.Set("Accept-Encoding", acceptEncoding)
		}
		res, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil || res.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: %d %v", method, path, res.StatusCode, err)
		}
		return res, body, wire.Load()
	}
	for _, tc := range []struct {
		path, file string
		compressed bool
	}{
		{"/", "index.html", true},
		{"/podcasts/p1:abc", "index.html", true}, // an app route
		{"/assets/app-1.js", "assets/app-1.js", true},
		{"/assets/app-1.css", "assets/app-1.css", true},
		{"/assets/tiny.css", "assets/tiny.css", false},
		{"/assets/icon.png", "assets/icon.png", false},
	} {
		want := files[tc.file]
		res, body, plainWire := get(http.MethodGet, tc.path, "")
		if res.Header.Get("Content-Encoding") != "" || !bytes.Equal(body, want) {
			t.Fatalf("%s without Accept-Encoding: %v", tc.path, res.Header)
		}
		res, body, zWire := get(http.MethodGet, tc.path, "gzip, deflate, br")
		if !tc.compressed {
			if res.Header.Get("Content-Encoding") != "" || !bytes.Equal(body, want) {
				t.Fatalf("%s: encoded %v", tc.path, res.Header)
			}
			continue
		}
		if res.Header.Get("Content-Encoding") != "gzip" || res.Header.Get("Vary") != "Accept-Encoding" ||
			res.Header.Get("Content-Length") != strconv.Itoa(len(body)) || zWire >= plainWire/2 {
			t.Fatalf("%s: %v, %d bytes on the wire for %d", tc.path, res.Header, zWire, plainWire)
		}
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if decoded, err := io.ReadAll(zr); err != nil || !bytes.Equal(decoded, want) {
			t.Fatalf("%s: the decoded body differs from the file (%v)", tc.path, err)
		}
		// A HEAD answers with the same headers.
		head, _, _ := get(http.MethodHead, tc.path, "gzip")
		if head.Header.Get("Content-Encoding") != "gzip" || head.Header.Get("Content-Length") != res.Header.Get("Content-Length") {
			t.Fatalf("HEAD %s: %v", tc.path, head.Header)
		}
		// A refusal, and a client that lists only other codings.
		for _, ae := range []string{"gzip;q=0", "gzip;q=0, *", "br"} {
			res, body, _ := get(http.MethodGet, tc.path, ae)
			if res.Header.Get("Content-Encoding") != "" || !bytes.Equal(body, want) || res.Header.Get("Vary") != "Accept-Encoding" {
				t.Fatalf("%s with %q: %v", tc.path, ae, res.Header)
			}
		}
	}
	// The cache rules are unchanged.
	if res, _, _ := get(http.MethodGet, "/assets/app-1.js", "gzip"); res.Header.Get("Cache-Control") != "public, max-age=31536000, immutable" {
		t.Errorf("asset cache %q", res.Header.Get("Cache-Control"))
	}
	if res, _, _ := get(http.MethodGet, "/", "gzip"); res.Header.Get("Cache-Control") != "no-cache" {
		t.Errorf("index cache %q", res.Header.Get("Cache-Control"))
	}
}
