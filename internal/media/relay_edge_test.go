package media

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

// An answer longer than the pinned length (here a chunked one,
// which declares no length) is never cut to size and called complete: its
// last bytes are withheld, the response aborted, and the status failed.
func TestLongerAnswerIsNotComplete(t *testing.T) {
	t.Parallel()
	var grown atomic.Bool
	p := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		if !grown.Load() {
			w.Header().Set("Content-Length", "100")
			w.Write(bytes.Repeat([]byte("A"), 100))
			return
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		w.Write(bytes.Repeat([]byte("B"), 200))
	}))
	defer p.Close()
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", p.URL, "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	grown.Store(true)
	r := b.get(*d.URL, "")
	st := s.statusOf(t, d, "a")
	if st.State == "complete" || r.err == nil || len(r.body) >= 100 {
		t.Fatalf("a longer answer: %d bytes, err %v, status %+v", len(r.body), r.err, st)
	}
	if st.State != "failed" || st.Error.Error != "media_changed" {
		t.Fatalf("status %+v", st)
	}
	// An answer exactly as long as pinned still completes.
	grown.Store(false)
	d = play(t, s, episode("a", p.URL, "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if r := b.get(*d.URL, ""); r.err != nil || len(r.body) != 100 {
		t.Fatalf("an exact answer: %d bytes, err %v", len(r.body), r.err)
	}
	if st := s.statusOf(t, d, "a"); st.State != "complete" {
		t.Fatalf("an exact answer's status %+v", st)
	}
}

// The representation is bound to the resource the redirects led
// to. Another resource with the same ETag and length is a change; a
// redirect whose query alone changes (tracking and signing parameters) is
// the same resource.
func TestRedirectRepresentation(t *testing.T) {
	t.Parallel()
	p := newPublisher(t)
	p.set("/one", &file{data: bytes.Repeat([]byte("A"), 1000), ctype: "audio/mpeg", etag: `"v1"`, ranges: true})
	p.set("/two", &file{data: bytes.Repeat([]byte("B"), 1000), ctype: "audio/mpeg", etag: `"v1"`, ranges: true})
	p.set("/ep", &file{redirect: p.url("/one")})
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", p.url("/ep"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if first := b.get(*d.URL, "bytes=0-99"); first.status != 206 || first.body[0] != 'A' {
		t.Fatalf("first %d", first.status)
	}
	p.set("/ep", &file{redirect: p.url("/one?request_event_id=2&sig=x&utm_source=feed")})
	if r := b.get(*d.URL, "bytes=100-199"); r.status != 206 || r.body[0] != 'A' {
		t.Fatalf("a change of volatile parameters only: %d", r.status)
	}
	p.set("/ep", &file{redirect: p.url("/two")})
	next := b.get(*d.URL, "bytes=100-199")
	if next.status != 412 || bytes.Contains(next.body, []byte("B")) {
		t.Fatalf("another resource under the same representation: %d %q", next.status, next.body)
	}
	if st := s.statusOf(t, d, "a"); st.State != "failed" || st.Error.Error != "media_changed" {
		t.Fatalf("status %+v", st)
	}
	// The probe facts and copies are keyed by the resource too.
	r1 := &rep{source: "s", resource: "https://a:443/one?"}
	r2 := &rep{source: "s", resource: "https://a:443/two?"}
	if r1.key("acct") == r2.key("acct") {
		t.Fatal("two resources share a representation key")
	}
}

// A strong representation's ETag is required on every answer but a
// 206 to our If-Range: an unconditional 200 without it is a change, not
// relabelled with the pinned ETag.
func TestMissingValidator(t *testing.T) {
	t.Parallel()
	p := newPublisher(t)
	p.set("/ep", &file{data: bytes.Repeat([]byte("A"), 1000), ctype: "audio/mpeg", etag: `"v1"`, ranges: true})
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", p.url("/ep"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	p.set("/ep", &file{data: bytes.Repeat([]byte("B"), 1000), ctype: "audio/mpeg", ranges: true})
	r := b.get(*d.URL, "")
	if r.status != 412 || bytes.Contains(r.body, []byte("B")) || r.header.Get("ETag") != "" {
		t.Fatalf("an unconditional 200 without the ETag: %d etag %q", r.status, r.header.Get("ETag"))
	}
	// A weak representation's Last-Modified is required the same way.
	p.set("/weak", &file{data: bytes.Repeat([]byte("A"), 1000), ctype: "audio/mpeg", lastModified: lm})
	d = play(t, s, episode("a", p.url("/weak"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	p.set("/weak", &file{data: bytes.Repeat([]byte("B"), 1000), ctype: "audio/mpeg"})
	if r := b.get(*d.URL, ""); r.status != 412 {
		t.Fatalf("a weak answer without its Last-Modified: %d", r.status)
	}
}

// A redirect (or a 401) whose body never comes does not hold the
// descriptor: its body is closed at once.
func TestRedirectBodyStall(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	var once atomic.Bool
	p := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/next")
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(302)
		w.(http.Flusher).Flush()
		if once.CompareAndSwap(false, true) {
			close(entered)
		}
		<-r.Context().Done()
	}))
	defer p.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(401)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer auth.Close()
	s := newService(t, serviceOpts{limits: func(l *Limits) { l.Stall = 50 * time.Millisecond; l.ProbeDeadline = 50 * time.Millisecond }})
	for _, ep := range []Episode{episode("a", p.URL, "audio/mpeg"), episode("a", auth.URL, "audio/mpeg")} {
		ep.Credentials = &feed.Credentials{Username: "u", Password: "p", ApprovedOrigins: []string{feed.Origin(mustURL(auth.URL))}}
		done := make(chan *Descriptor, 1)
		go func() {
			d, _ := s.Play(context.Background(), ep, PlayRequest{Capabilities: caps("all")})
			done <- d
		}()
		select {
		case d := <-done:
			if d == nil || d.Mode != "failed" {
				t.Fatalf("%s: %+v", ep.SourceURL, d)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the descriptor hung on a body that never came", ep.SourceURL)
		}
	}
	<-entered
	// The pin as a whole is bounded too.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	s2 := newService(t, serviceOpts{limits: func(l *Limits) { l.PinDeadline = 100 * time.Millisecond }})
	start := time.Now()
	if e := playFailure(t, s2, episode("a", slow.URL, "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); e.Label != "time_limit" ||
		time.Since(start) > 5*time.Second {
		t.Fatalf("a silent host: %+v after %v", e, time.Since(start))
	}
}

// A copy being read is never evicted for another, even while no
// request is open (a reader's lease), and the server says it is busy.
func TestSpoolActiveEviction(t *testing.T) {
	t.Parallel()
	p := newPublisher(t)
	for _, path := range []string{"/0", "/1", "/2"} {
		p.set(path, &file{data: pattern(100 << 10), ctype: "audio/mpeg", noLength: true})
	}
	s := newService(t, serviceOpts{})
	var ds []*Descriptor
	for _, path := range []string{"/0", "/1"} {
		ds = append(ds, play(t, s, episode("a", p.url(path), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}))
	}
	for _, d := range ds {
		k, _ := s.Lookup(*d.Token, "a", "cred-a")
		if err := k.t.spool.waitDone(context.Background()); err != nil {
			t.Fatal(err)
		}
		r, err := k.t.source().read(context.Background(), 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		k.t.spool.mu.Lock()
		active := k.t.spool.active
		k.t.spool.mu.Unlock()
		if active != 1 {
			t.Fatalf("an open reader holds %d leases", active)
		}
	}
	if _, err := s.Play(context.Background(), episode("a", p.url("/2"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); !isBusy(err) {
		t.Fatalf("a third copy while both are read: %v", err)
	}
	k, _ := s.Lookup(*ds[0].Token, "a", "cred-a")
	r, err := k.t.source().read(context.Background(), 1000, 1999)
	if err != nil {
		t.Fatalf("the copy being read was evicted: %v", err)
	}
	r.Close()
}

// HTTP's deflate is zlib-wrapped; a raw deflate stream, which some
// servers send, is read too.
func TestRawDeflateIsRead(t *testing.T) {
	t.Parallel()
	want := bytes.Repeat([]byte("audio"), 100)
	var wrapped, raw bytes.Buffer
	zw := zlib.NewWriter(&wrapped)
	zw.Write(want)
	zw.Close()
	fw, _ := flate.NewWriter(&raw, flate.DefaultCompression)
	fw.Write(want)
	fw.Close()
	for name, body := range map[string][]byte{"zlib": wrapped.Bytes(), "raw": raw.Bytes()} {
		p := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("Content-Encoding", "deflate")
			w.Write(body)
		}))
		defer p.Close()
		s := newService(t, serviceOpts{})
		b := newBrowser(t, s, "a")
		d := play(t, s, episode("a", p.URL, "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
		if r := b.get(*d.URL, ""); r.status != 200 || !bytes.Equal(r.body, want) {
			t.Fatalf("%s deflate: %d %q", name, r.status, r.body)
		}
	}
}

// The production fetcher refuses loopback; a reader capability is exact and
// revocable; a source whose timestamps start late keeps its timeline and
// completes; a MOV external data reference is never fetched.
func TestProductionFetcherRefusesLoopback(t *testing.T) {
	t.Parallel()
	p := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("the production fetcher reached loopback") }))
	defer p.Close()
	resp, _, err := feed.NewFetcher().OpenMedia(context.Background(), feed.MediaRequest{URL: p.URL})
	if resp != nil {
		resp.Body.Close()
	}
	if fe, ok := err.(*feed.FetchError); !ok || fe.Label != "refused_destination" {
		t.Fatalf("loopback: %v", err)
	}
}

func TestReaderCapability(t *testing.T) {
	t.Parallel()
	s := newService(t, serviceOpts{})
	input := filepath.Join(t.TempDir(), "source")
	os.WriteFile(input, []byte("one representation"), 0o600)
	_, in, revoke, err := s.reader.grant(context.Background(), fileSource{input}, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The capability is the reader's proxy credentials.
	proxy := strings.TrimPrefix(in.env[0], "http_proxy=")
	get := func(proxyURL string) int {
		pu, err := neturl.Parse(proxyURL)
		if err != nil {
			t.Fatal(err)
		}
		c := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(pu)}}
		defer c.CloseIdleConnections()
		r, err := c.Get(in.url)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		io.Copy(io.Discard, r.Body)
		return r.StatusCode
	}
	pu, _ := neturl.Parse(proxy)
	bare := "http://" + pu.Host
	if code := get(bare); code != http.StatusProxyAuthRequired {
		t.Fatalf("no capability %d", code)
	}
	if code := get(strings.Replace(proxy, "g:", "g:x", 1)); code != http.StatusProxyAuthRequired {
		t.Fatalf("unknown capability %d", code)
	}
	if code := get(proxy); code != 200 {
		t.Fatalf("valid capability %d", code)
	}
	revoke()
	if code := get(proxy); code != http.StatusProxyAuthRequired {
		t.Fatalf("revoked capability %d", code)
	}
}

func TestNonzeroStartTime(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	dir := t.TempDir()
	input, output := filepath.Join(dir, "in.m4a"), filepath.Join(dir, "out.m4a")
	os.WriteFile(input, toneFile(t, "m4a"), 0o600)
	if out, err := exec.Command(ffmpegBinary(t), "-v", "error", "-i", input, "-c", "copy", "-output_ts_offset", "30", output).CombinedOutput(); err != nil {
		t.Fatalf("%s: %v", out, err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	p := newPublisher(t)
	p.set("/ep", &file{data: data, ctype: "audio/mp4", etag: etagOf(data), ranges: true})
	s := newService(t, serviceOpts{withFFmp: true})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", p.url("/ep"), "audio/mp4"), PlayRequest{Capabilities: caps("all"), Force: "compat", StartMS: 7500})
	if d.Token == nil {
		t.Fatalf("descriptor %+v", d)
	}
	r := b.get(*d.URL, "")
	checkTimeline(t, frequencies(t, r.body, "aac"), 7500, "timestamps starting at 30 s, from 7.5 s")
	if st := s.statusOf(t, d, "a"); st.State != "complete" || *d.DurationMS > 21000 {
		t.Fatalf("status %+v, duration %d", st, *d.DurationMS)
	}
}

func TestExternalMOVReference(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	target := newPublisher(t)
	target.set("/private.aac", &file{data: pattern(1000), ctype: "audio/aac"})
	var rewrite func([]byte) []byte
	rewrite = func(data []byte) []byte {
		var out []byte
		for len(data) >= 8 {
			n := int(binary.BigEndian.Uint32(data[:4]))
			if n < 8 || n > len(data) {
				t.Fatal("bad fixture atom")
			}
			typ, payload := string(data[4:8]), append([]byte(nil), data[8:n]...)
			switch typ {
			case "moov", "trak", "mdia", "minf", "dinf":
				payload = rewrite(payload)
			case "dref":
				payload = append(payload[:8:8], rewrite(payload[8:])...)
			case "url ":
				payload = append([]byte{0, 0, 0, 0}, append([]byte(target.url("/private.aac")), 0)...)
			}
			h := make([]byte, 8)
			binary.BigEndian.PutUint32(h, uint32(len(payload)+8))
			copy(h[4:], typ)
			out = append(append(out, h...), payload...)
			data = data[n:]
		}
		return append(out, data...)
	}
	data := rewrite(toneFile(t, "m4a-tail"))
	if !strings.Contains(string(data), target.url("/private.aac")) {
		t.Fatal("the fixture has no external reference")
	}
	pub := newPublisher(t)
	pub.set("/ep", &file{data: data, ctype: "audio/mp4", etag: etagOf(data), ranges: true})
	s := newService(t, serviceOpts{withFFmp: true})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", pub.url("/ep"), "audio/mp4"), PlayRequest{Capabilities: caps("all"), Force: "compat"})
	if d.Token != nil {
		b.get(*d.URL, "")
	}
	if got := target.seen(); len(got) != 0 {
		t.Fatalf("an external MOV reference was fetched: %+v", got)
	}
}
