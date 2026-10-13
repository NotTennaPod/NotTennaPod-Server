package media

// Bounded spooling (PROTOCOL §13) and the metadata assets (PROTOCOL §13).

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

func spoolFiles(t *testing.T, s *Service) []string {
	t.Helper()
	entries, _ := os.ReadDir(s.cfg.SpoolDir)
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "spool-") {
			names = append(names, e.Name())
		}
	}
	return names
}

// An upstream of unknown length, or one that sends a content coding
// whatever was asked, is copied (decoded) and served from the copy with
// real ranges and the copy's own validators; the copy is deleted when its
// token goes.
func TestSpoolUnknownLengthAndEncoding(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	data := pattern(300 << 10)
	pub.set("/chunked.mp3", &file{data: data, ctype: "audio/mpeg", noLength: true})
	pub.set("/gzip.mp3", &file{data: data, ctype: "audio/mpeg", gzip: true, etag: `"g"`})
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	for _, path := range []string{"/chunked.mp3", "/gzip.mp3"} {
		d := play(t, s, episode("a", pub.url(path), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
		if d.Mode != "relay" || *d.Representation != "spool" || !d.Seekable {
			t.Fatalf("%s: descriptor %+v", path, d)
		}
		r := b.get(*d.URL, "bytes=1000-1999")
		if r.status != 206 || !bytes.Equal(r.body, data[1000:2000]) || r.header.Get("Content-Range") != "bytes 1000-1999/307200" ||
			!strings.HasPrefix(r.header.Get("ETag"), `"ntp-`) || r.header.Get("Accept-Ranges") != "bytes" {
			t.Fatalf("%s: %d %v", path, r.status, r.header)
		}
		if r := b.get(*d.URL, ""); r.status != 200 || !bytes.Equal(r.body, data) {
			t.Fatalf("%s: the whole copy %d", path, r.status)
		}
		if head := b.do(http.MethodHead, *d.URL, nil); head.header.Get("Content-Length") != "307200" {
			t.Fatalf("%s: HEAD %v", path, head.header)
		}
		if st := s.statusOf(t, d, "a"); st.State != "complete" {
			t.Fatalf("%s: status %+v", path, st)
		}
	}
	if n := len(spoolFiles(t, s)); n != 2 {
		t.Fatalf("%d copies", n)
	}
	s.tokens.dropAll()
	if n := len(spoolFiles(t, s)); n != 0 {
		t.Fatalf("%d copies after their tokens went", n)
	}
}

// The cap: a representation longer than it is unsupported with that
// reason; one whose length shows only while copying fails the stream.
func TestSpoolCap(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	pub.set("/long.mp3", &file{data: pattern(5000), ctype: "audio/mpeg", gzip: true})
	pub.set("/chunked.mp3", &file{data: pattern(5000), ctype: "audio/mpeg", noLength: true})
	s := newService(t, serviceOpts{limits: func(l *Limits) { l.SpoolMax = 1000 }})
	b := newBrowser(t, s, "a")
	for _, path := range []string{"/long.mp3", "/chunked.mp3"} {
		d := play(t, s, episode("a", pub.url(path), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
		if d.Mode != "relay" {
			t.Fatalf("%s: %+v", path, d)
		}
		if r := b.get(*d.URL, ""); r.status != 502 || !strings.Contains(string(r.body), "media_unavailable") {
			t.Fatalf("%s: %d %s", path, r.status, r.body)
		}
		if st := s.statusOf(t, d, "a"); st.State != "failed" || st.Error.Label != "size_limit" {
			t.Fatalf("%s: status %+v", path, st)
		}
	}
	// A declared length beyond the cap: unsupported before any copy.
	pub.set("/declared.mp3", &file{data: pattern(5000), ctype: "audio/mpeg"})
	s2 := newService(t, serviceOpts{limits: func(l *Limits) { l.SpoolMax = 1000 }})
	sp := s2.spools
	if _, err := sp.acquire(context.Background(), "k", &upstream{fetcher: s2.cfg.Fetcher, rep: &rep{source: pub.url("/declared.mp3"), length: 5000}}, s2.now()); err != errTooLarge {
		t.Fatalf("over the cap: %v", err)
	}
}

// A copy is shared by another descriptor only when a strong ETag shows it
// holds the same bytes. A feed that reuses one address for each new
// episode, served without validators or a length, gets each episode's own
// bytes.
func TestSpoolReusedOnlyWithAStrongValidator(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	old, fresh := bytes.Repeat([]byte("A"), 5000), bytes.Repeat([]byte("B"), 8000)
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	for _, weak := range []*file{
		{ctype: "audio/mpeg", noLength: true},
		{ctype: "audio/mpeg", noLength: true, lastModified: "Tue, 06 Oct 2026 08:00:00 GMT"},
		{ctype: "audio/mpeg", gzip: true},
		{ctype: "audio/mpeg", noLength: true, etag: `W/"latest"`},
	} {
		first, second := *weak, *weak
		first.data, second.data = old, fresh
		pub.set("/latest.mp3", &first)
		ep := episode("a", pub.url("/latest.mp3"), "audio/mpeg")
		d := play(t, s, ep, PlayRequest{Capabilities: caps("all")})
		if r := b.get(*d.URL, ""); r.status != 200 || !bytes.Equal(r.body, old) {
			t.Fatalf("%+v: the first episode %d", weak, r.status)
		}
		pub.set("/latest.mp3", &second)
		ep.ID = "the-next-episode"
		d2 := play(t, s, ep, PlayRequest{Capabilities: caps("all")})
		if r := b.get(*d2.URL, ""); r.status != 200 || !bytes.Equal(r.body, fresh) {
			t.Fatalf("%+v: the next episode got %d bytes (%q…), HTTP %d", weak, len(r.body), r.body[:min(1, len(r.body))], r.status)
		}
		// The first descriptor still plays its own copy.
		if r := b.get(*d.URL, ""); r.status != 200 || !bytes.Equal(r.body, old) {
			t.Fatalf("%+v: the first descriptor afterwards %d", weak, r.status)
		}
	}
	// With a strong ETag a second descriptor shares the copy: one fetch.
	pub.set("/tagged.mp3", &file{data: old, ctype: "audio/mpeg", noLength: true, etag: `"v1"`})
	ep := episode("a", pub.url("/tagged.mp3"), "audio/mpeg")
	d := play(t, s, ep, PlayRequest{Capabilities: caps("all")})
	b.get(*d.URL, "")
	pub.reset()
	d2 := play(t, s, ep, PlayRequest{Capabilities: caps("all")})
	if r := b.get(*d2.URL, ""); r.status != 200 || !bytes.Equal(r.body, old) {
		t.Fatalf("a shared copy: %d", r.status)
	}
	for _, h := range pub.seen() {
		if h.rng != "bytes=0-1" {
			t.Fatalf("a copy with a strong ETag was fetched again: %+v", pub.seen())
		}
	}
}

// At most two copies per server. A third evicts the least recently used
// copy that holds no lease and serves no live playback; a copy in use by a
// stream, conversion or reader, or the current playback of an account
// within the grace time, is never evicted, and then the server is busy.
func TestSpoolLimit(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	for _, p := range []string{"/1", "/2", "/3", "/4"} {
		pub.set(p, &file{data: pattern(10 << 10), ctype: "audio/mpeg", noLength: true})
	}
	now := time.Now()
	var clock sync.Mutex
	s := newService(t, serviceOpts{})
	s.cfg.Now = func() time.Time { clock.Lock(); defer clock.Unlock(); return now }
	advance := func(d time.Duration) { clock.Lock(); now = now.Add(d); clock.Unlock() }
	ba, bb := newBrowser(t, s, "a"), newBrowser(t, s, "b")
	da := play(t, s, episode("a", pub.url("/1"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	db := play(t, s, episode("b", pub.url("/2"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if r := ba.get(*da.URL, "bytes=0-9"); r.status != 206 {
		t.Fatalf("a: %d", r.status)
	}
	if r := bb.get(*db.URL, "bytes=0-9"); r.status != 206 {
		t.Fatalf("b: %d", r.status)
	}
	// Both are current playbacks, used just now: a third copy must wait.
	if _, err := s.Play(context.Background(), episode("c", pub.url("/3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); !isBusy(err) {
		t.Fatalf("two live playbacks: %v", err)
	}
	// b plays on; a's playback is abandoned beyond the grace time and goes.
	advance(spoolGrace + time.Second)
	bb.get(*db.URL, "bytes=10-19")
	if d := play(t, s, episode("c", pub.url("/3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); d.Mode != "relay" {
		t.Fatalf("third copy %+v", d)
	}
	if r := ba.get(*da.URL, ""); r.status == 200 {
		t.Fatal("an evicted copy still served")
	}
	if r := bb.get(*db.URL, "bytes=20-29"); r.status != 206 {
		t.Fatalf("the live playback's copy was evicted: %d", r.status)
	}
	// Leases held beyond the grace time (readers open, as a stream or a
	// conversion holds them): never evicted.
	advance(spoolGrace + time.Second)
	for _, f := range s.spools.files {
		r, err := spoolSource{f: f, declared: -1, encoded: true, now: s.now}.read(context.Background(), 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
	}
	advance(spoolGrace + time.Second)
	if _, err := s.Play(context.Background(), episode("d", pub.url("/4"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); !isBusy(err) {
		t.Fatalf("all copies leased: %v", err)
	}
}

func isBusy(err error) bool {
	e, ok := err.(*Error)
	return ok && e.Code == "server_busy"
}

// Artwork, chapters and transcripts: their own types and caps, never SVG or
// HTML, served with the media headers.
func TestAssets(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	png := append([]byte("\x89PNG\r\n\x1a\n"), pattern(100)...)
	pub.set("/cover.png", &file{data: png, ctype: "image/png"})
	pub.set("/cover-untyped", &file{data: png, ctype: "application/octet-stream"})
	pub.set("/cover.svg", &file{data: []byte(`<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>`), ctype: "image/svg+xml"})
	pub.set("/cover.html", &file{data: []byte(`<html><script>alert(1)</script></html>`), ctype: "text/html"})
	pub.set("/big.png", &file{data: append(png, make([]byte, 2000)...), ctype: "image/png"})
	pub.set("/chapters.json", &file{data: []byte(`{"version":"1.2.0","chapters":[{"startTime":0,"title":"One"}]}`), ctype: "application/json+chapters"})
	pub.set("/chapters.bad", &file{data: []byte(`<html>`), ctype: "application/json"})
	pub.set("/t.srt", &file{data: []byte("1\n00:00:00,000 --> 00:00:01,000\n<script>x</script>\n"), ctype: "text/html"})
	pub.set("/t.json", &file{data: []byte(`{"segments":[]}`), ctype: "text/plain"})
	pub.set("/private.png", &file{data: png, ctype: "image/png", user: "u", pass: "p"})
	s := newService(t, serviceOpts{limits: func(l *Limits) { l.ImageMax = 1000 }})
	get := func(kind, path, declared string, creds *feed.Credentials) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/v1/media/e/"+kind, nil)
		if e := s.Asset(w, r, "a", kind, pub.url(path), declared, creds); e != nil {
			w.Code = e.Status
			w.Body.WriteString(e.Code + " " + e.Label)
		}
		return w
	}
	cases := []struct {
		kind, path, declared string
		status               int
		ctype                string
	}{
		{AssetImage, "/cover.png", "", 200, "image/png"},
		{AssetImage, "/cover-untyped", "", 200, "image/png"},
		{AssetImage, "/cover.svg", "", 502, ""},
		{AssetImage, "/cover.html", "", 502, ""},
		{AssetImage, "/big.png", "", 502, ""},
		{AssetChapters, "/chapters.json", "", 200, "application/json"},
		{AssetChapters, "/chapters.bad", "", 502, ""},
		{AssetTranscript, "/t.srt", "application/srt", 200, "text/plain; charset=utf-8"},
		{AssetTranscript, "/t.json", "application/json", 200, "application/json"},
		{AssetImage, "/private.png", "", 502, ""},
		{AssetImage, "/missing.png", "", 502, ""},
	}
	for _, c := range cases {
		w := get(c.kind, c.path, c.declared, nil)
		if w.Code != c.status || (c.ctype != "" && w.Header().Get("Content-Type") != c.ctype) {
			t.Errorf("%s %s: %d %q %s", c.kind, c.path, w.Code, w.Header().Get("Content-Type"), w.Body)
			continue
		}
		if c.status == 200 {
			h := w.Header()
			if h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Content-Security-Policy") != "sandbox; default-src 'none'" ||
				h.Get("Cross-Origin-Resource-Policy") != "same-origin" {
				t.Errorf("%s: headers %v", c.path, h)
			}
		}
	}
	creds := &feed.Credentials{Username: "u", Password: "p", ApprovedOrigins: []string{feed.Origin(mustURL(pub.url("/")))}}
	if w := get(AssetImage, "/private.png", "", creds); w.Code != 200 {
		t.Errorf("with credentials: %d", w.Code)
	}
}

// A copy of unknown length completing while requests wait for its length wakes
// every one of them: none waits on a wake channel the completion already
// replaced. The race detector's scheduling makes the lost wake show reliably
// (make check runs it so).
func TestWaitTotalSeesCompletion(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	stalled := 0
	for range 200 {
		f := &spoolFile{total: -1, written: 100, grew: make(chan struct{})}
		start := make(chan struct{})
		for range 32 {
			wg.Go(func() {
				<-start
				if n, err := f.waitTotal(ctx, -1, false); err != nil || n != 100 {
					mu.Lock()
					stalled++
					mu.Unlock()
				}
			})
		}
		wg.Go(func() {
			<-start
			f.settle(nil)
		})
		close(start)
	}
	wg.Wait()
	if stalled > 0 {
		t.Fatalf("%d requests waited on after their copy completed", stalled)
	}
}
