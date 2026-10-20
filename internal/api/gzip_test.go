package api

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/httpgzip"
)

// rawDo serves one request in process and returns the response as sent,
// undecoded.
func (e *env) rawDo(method, path string, body any, opts ...opt) *httptest.ResponseRecorder {
	e.t.Helper()
	raw := encode(e.t, method, body)
	var rd io.Reader
	if raw != nil {
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, testOrigin+path, rd)
	req.RemoteAddr = loopbackPeer
	if raw != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, o := range opts {
		o(req)
	}
	rec := httptest.NewRecorder()
	e.srv.ServeHTTP(rec, req)
	return rec
}

func gunzipped(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// gzipEnv is an account with a phone that pushed enough to make its pull,
// snapshot and read responses larger than httpgzip.MinSize.
func gzipEnv(t *testing.T) (*readEnv, *phone) {
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	p := r.phoneOf("owner", "correct horse battery")
	var ops []op
	for i := int64(1); i <= 20; i++ {
		ops = append(ops, op{seq: i, fact: p.noteFact(strings.Repeat("a note to compress ", 10))})
	}
	pushed(t, p.push(ops...))
	return r, p
}

// PROTOCOL §6: JSON responses are gzip-compressed when the request's
// Accept-Encoding allows it and the body is at least 1 KiB; the decoded body
// is exactly the identity response's bytes, its final line break included.
// An explicit refusal, a wildcard refused, other codings only and no header
// all get identity.
func TestGzipNegotiation(t *testing.T) {
	r, p := gzipEnv(t)
	genesis := genesisOf(t, p.cursor)
	calls := []struct {
		name, method, path string
		body               any
		cred               opt
	}{
		{"pull", http.MethodPost, "/v1/pull", map[string]any{"cursor": genesis, "install_id": p.install, "acked_install_seq": 0}, p.cred},
		{"snapshot", http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": p.install, "acked_install_seq": 0}, p.cred},
		{"read list", http.MethodGet, "/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed", nil, r.web},
		{"read summary", http.MethodGet, "/v1/read/summary", nil, r.web},
	}
	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			identity := r.rawDo(c.method, c.path, c.body, c.cred)
			plain := identity.Body.Bytes()
			if identity.Code != http.StatusOK || identity.Header().Get("Content-Encoding") != "" || identity.Header().Get("Vary") != "Accept-Encoding" {
				t.Fatalf("identity: %d %v %s", identity.Code, identity.Header(), plain)
			}
			if len(plain) < httpgzip.MinSize && c.name != "read summary" {
				t.Fatalf("the %s response is %d bytes; the test needs at least %d", c.name, len(plain), httpgzip.MinSize)
			}
			if !bytes.HasSuffix(plain, []byte("}\n")) || identity.Header().Get("Content-Length") != strconv.Itoa(len(plain)) {
				t.Fatalf("identity body %q, Content-Length %q", plain[max(0, len(plain)-8):], identity.Header().Get("Content-Length"))
			}
			compress := len(plain) >= httpgzip.MinSize
			for _, ae := range []string{"gzip", "gzip, deflate, br", "*", "br;q=1, GZIP;q=0.5", "x-gzip"} {
				z := r.rawDo(c.method, c.path, c.body, c.cred, hdr("Accept-Encoding", ae))
				sent := z.Body.Bytes()
				if z.Code != http.StatusOK || z.Header().Get("Vary") != "Accept-Encoding" || z.Header().Get("Content-Length") != strconv.Itoa(len(sent)) ||
					z.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(z.Header().Get("Content-Type"), "application/json") {
					t.Fatalf("%q: %d %v", ae, z.Code, z.Header())
				}
				if !compress {
					if z.Header().Get("Content-Encoding") != "" || !bytes.Equal(sent, plain) {
						t.Fatalf("%q: a %d-byte body was encoded", ae, len(plain))
					}
					continue
				}
				if z.Header().Get("Content-Encoding") != "gzip" || len(sent) >= len(plain) {
					t.Fatalf("%q: Content-Encoding %q, %d bytes for %d", ae, z.Header().Get("Content-Encoding"), len(sent), len(plain))
				}
				if decoded := gunzipped(t, sent); !bytes.Equal(decoded, plain) {
					t.Fatalf("%q: the decoded body differs from the identity response:\n%s\n%s", ae, decoded, plain)
				}
			}
			for _, ae := range []string{"gzip;q=0", "gzip;q=0.000, *", "*;q=0", "identity", "br, deflate", "gzip;q=2"} {
				z := r.rawDo(c.method, c.path, c.body, c.cred, hdr("Accept-Encoding", ae))
				if z.Code != http.StatusOK || z.Header().Get("Content-Encoding") != "" || !bytes.Equal(z.Body.Bytes(), plain) {
					t.Fatalf("%q: %d %v, identical %v", ae, z.Code, z.Header(), bytes.Equal(z.Body.Bytes(), plain))
				}
			}
		})
	}
	// Bodies under 1 KiB, errors among them, are never encoded.
	for _, path := range []string{"/v1/info", "/v1/nothing"} {
		rec := r.rawDo(http.MethodGet, path, nil, hdr("Accept-Encoding", "gzip"))
		if rec.Body.Len() >= httpgzip.MinSize || rec.Header().Get("Content-Encoding") != "" || !bytes.HasPrefix(rec.Body.Bytes(), []byte("{")) {
			t.Fatalf("%s: %d bytes, %v", path, rec.Body.Len(), rec.Header())
		}
	}
}

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

// A real Go http.Client, as a phone's or the CLI's would, asks for gzip by
// itself and gets the page compressed on the wire, decoded to the same bytes.
func TestGzipOverTheWire(t *testing.T) {
	r, p := gzipEnv(t)
	srv := httptest.NewServer(r.srv)
	defer srv.Close()
	fetch := func(disable bool) ([]byte, *http.Response, int64) {
		var wire atomic.Int64
		dialer := &net.Dialer{}
		tr := &http.Transport{DisableCompression: disable, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return countingConn{c, &wire}, nil
		}}
		defer tr.CloseIdleConnections()
		body := `{"cursor":"` + genesisOf(t, p.cursor) + `","install_id":"` + p.install + `","acked_install_seq":0}`
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/pull", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+p.token)
		res, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil || res.StatusCode != http.StatusOK {
			t.Fatalf("%d %v %s", res.StatusCode, err, data)
		}
		return data, res, wire.Load()
	}
	plain, pres, plainWire := fetch(true)
	decoded, zres, zWire := fetch(false)
	if pres.Uncompressed || pres.Header.Get("Content-Encoding") != "" {
		t.Fatalf("a client without compression got %v", pres.Header)
	}
	if !zres.Uncompressed {
		t.Fatalf("the default client's response was not compressed: %v", zres.Header)
	}
	if !bytes.Equal(decoded, plain) {
		t.Fatal("the decoded page differs from the identity page")
	}
	if zWire >= plainWire*3/4 {
		t.Fatalf("%d bytes on the wire compressed, %d identity", zWire, plainWire)
	}
	t.Logf("pull page: %d bytes identity, %d on the wire compressed (%d identity)", len(plain), zWire, plainWire)
}

// The event stream is never encoded, whatever the client accepts, and its
// heartbeats still reach the client as they are written.
func TestGzipNeverEncodesEvents(t *testing.T) {
	old := heartbeatInterval
	heartbeatInterval = 20 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = old })
	r, _ := gzipEnv(t)
	srv := httptest.NewServer(r.srv)
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/events", nil)
	req.AddCookie(sessionCookieOf(r.session))
	req.Header.Set("Accept-Encoding", "gzip")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Encoding") != "" || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("events: %d %v", res.StatusCode, res.Header)
	}
	rd := bufio.NewReader(res.Body)
	got := make(chan string, 1)
	go func() {
		var seen []string
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				got <- strings.Join(seen, "")
				return
			}
			seen = append(seen, line)
			if strings.HasPrefix(line, ": heartbeat") {
				got <- strings.Join(seen, "")
				return
			}
		}
	}()
	select {
	case s := <-got:
		if !strings.HasPrefix(s, "event: changed\n") || !strings.Contains(s, ": heartbeat\n") {
			t.Fatalf("stream %q", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no heartbeat arrived")
	}
}

// Media is sent as is to a client that accepts gzip: whole, by range, and a
// media error.
func TestGzipNeverEncodesMedia(t *testing.T) {
	m := newMediaEnv(t)
	_, _, ep := m.subscribe(m.pub.srv.URL+"/feed.xml", nil)
	d := m.play(ep, m.web, nil).must(http.StatusOK)
	url := d.str("url")
	ae := hdr("Accept-Encoding", "gzip")
	if all := m.raw(http.MethodGet, url, cookieOnly(m.session), ae); all.Code != http.StatusOK || all.Header().Get("Content-Encoding") != "" ||
		!bytes.Equal(all.Body.Bytes(), m.pub.audio) {
		t.Fatalf("whole: %d %v", all.Code, all.Header())
	}
	if part := m.raw(http.MethodGet, url, cookieOnly(m.session), ae, hdr("Range", "bytes=100-4195")); part.Code != http.StatusPartialContent ||
		part.Header().Get("Content-Encoding") != "" || !bytes.Equal(part.Body.Bytes(), m.pub.audio[100:4196]) {
		t.Fatalf("range: %d %v", part.Code, part.Header())
	}
	if gone := m.raw(http.MethodGet, "/v1/media/s/"+strings.Repeat("0", 32), cookieOnly(m.session), ae); gone.Code < 400 ||
		gone.Header().Get("Content-Encoding") != "" || !bytes.HasPrefix(gone.Body.Bytes(), []byte("{")) {
		t.Fatalf("a media error: %d %v %q", gone.Code, gone.Header(), gone.Body.Bytes())
	}
}
