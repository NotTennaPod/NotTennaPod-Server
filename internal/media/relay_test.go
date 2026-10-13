package media

// The media relay (PROTOCOL §13), without
// ffmpeg: decisions use the declared types.

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

const lm = "Mon, 05 Oct 2026 10:00:00 GMT"

// HEAD and GET, exact 206s, suffix and multiple ranges, 416, and what goes
// upstream: the pinned strong ETag as If-Range, identity encoding, the
// server's User-Agent, and nothing of the browser's.
func TestRelayRanges(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	data := pattern(300 << 10)
	pub.set("/ep.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), lastModified: lm, ranges: true})
	s := newService(t, serviceOpts{})
	d := play(t, s, episode("a", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if d.Mode != "relay" || !d.Seekable || *d.Representation != "strong" || *d.Type != "audio/mpeg" || !strings.HasPrefix(*d.URL, "/v1/media/s/") {
		t.Fatalf("descriptor %+v", d)
	}
	b := newBrowser(t, s, "a")
	pub.reset()

	head := b.do(http.MethodHead, *d.URL, nil)
	if head.status != 200 || head.header.Get("Content-Length") != "307200" || head.header.Get("Accept-Ranges") != "bytes" ||
		head.header.Get("ETag") != etagOf(data) || len(head.body) != 0 {
		t.Fatalf("HEAD %d %v", head.status, head.header)
	}
	if len(pub.seen()) != 0 {
		t.Fatalf("HEAD went upstream: %+v", pub.seen())
	}

	whole := b.get(*d.URL, "")
	if whole.status != 200 || !bytes.Equal(whole.body, data) || whole.header.Get("Accept-Ranges") != "bytes" {
		t.Fatalf("GET %d %d bytes", whole.status, len(whole.body))
	}
	r := b.get(*d.URL, "bytes=1000-1999")
	if r.status != 206 || !bytes.Equal(r.body, data[1000:2000]) || r.header.Get("Content-Range") != "bytes 1000-1999/307200" ||
		r.header.Get("Content-Length") != "1000" {
		t.Fatalf("range %d %v", r.status, r.header)
	}
	r = b.get(*d.URL, "bytes=-100")
	if r.status != 206 || !bytes.Equal(r.body, data[len(data)-100:]) {
		t.Fatalf("suffix %d %v", r.status, r.header)
	}
	r = b.get(*d.URL, "bytes=10-19, 50-59")
	if r.status != 206 || !bytes.Equal(r.body, data[10:20]) || r.header.Get("Content-Range") != "bytes 10-19/307200" {
		t.Fatalf("multiple ranges %d %v", r.status, r.header)
	}
	r = b.get(*d.URL, "bytes=307200-")
	if r.status != 416 || r.header.Get("Content-Range") != "bytes */307200" {
		t.Fatalf("past the end %d %v", r.status, r.header)
	}
	r = b.get(*d.URL, "bytes=307000-")
	if r.status != 206 || !bytes.Equal(r.body, data[307000:]) {
		t.Fatalf("the tail %d", r.status)
	}
	for _, h := range pub.seen() {
		if h.encoding != "identity" || h.ua != "NotTennaPod-Server" || h.cookie != "" || h.auth != "" || h.origin != "" {
			t.Fatalf("upstream request %+v", h)
		}
		if h.rng != "" && h.ifRange != etagOf(data) {
			t.Fatalf("a range request without the pinned If-Range: %+v", h)
		}
	}
	// The last request delivered the final byte: complete.
	if st := s.statusOf(t, d, "a"); st.State != "complete" || st.StreamID == 0 {
		t.Fatalf("status %+v", st)
	}
}

// A representation that changes mid-session is never spliced: a range
// request after the change answers 412 media_changed, and the status says
// so; a later range request that would read the new bytes fails too.
func TestRelayRepresentationChange(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	old, changed := pattern(200<<10), bytes.Repeat([]byte{0xAB}, 200<<10)
	pub.set("/ep.mp3", &file{data: old, ctype: "audio/mpeg", etag: etagOf(old), ranges: true})
	s := newService(t, serviceOpts{})
	d := play(t, s, episode("a", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	b := newBrowser(t, s, "a")
	if r := b.get(*d.URL, "bytes=0-99"); r.status != 206 || !bytes.Equal(r.body, old[:100]) {
		t.Fatalf("before %d", r.status)
	}
	// Same length, new ETag: the upstream answers If-Range with 200.
	pub.with("/ep.mp3", func(f *file) { f.data, f.etag = changed, etagOf(changed) })
	r := b.get(*d.URL, "bytes=100-199")
	if r.status != 412 || bytes.Contains(r.body, changed[:10]) || !strings.Contains(string(r.body), "media_changed") {
		t.Fatalf("after the change %d %s", r.status, r.body)
	}
	if st := s.statusOf(t, d, "a"); st.State != "failed" || st.Error.Error != "media_changed" {
		t.Fatalf("status %+v", st)
	}
	// Another length: a 206 with another total is a change too.
	pub.with("/ep.mp3", func(f *file) { f.data, f.etag = old[:1000], etagOf(old) })
	if r := b.get(*d.URL, "bytes=0-9"); r.status != 412 {
		t.Fatalf("another length %d", r.status)
	}
	// A whole request whose ETag changed.
	pub.with("/ep.mp3", func(f *file) { f.data, f.etag = changed, `"other"` })
	if r := b.get(*d.URL, ""); r.status != 412 {
		t.Fatalf("whole after the change %d", r.status)
	}
	// A new descriptor pins afresh and plays the new file.
	d2 := play(t, s, episode("a", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if r := b.get(*d2.URL, "bytes=0-9"); r.status != 206 || !bytes.Equal(r.body, changed[:10]) {
		t.Fatalf("new descriptor %d", r.status)
	}
}

// Weak representations (no strong ETag) and hosts that ignore Range are
// served as one whole upstream response per request with Accept-Ranges:
// none; the descriptor says they are not seekable. A Last-Modified change
// is a change.
func TestRelayWeakAndUnranged(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	data := pattern(100 << 10)
	pub.set("/weak.mp3", &file{data: data, ctype: "audio/mpeg", etag: `W/"weak"`, lastModified: lm, ranges: true})
	pub.set("/noranges.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data)})
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	for _, path := range []string{"/weak.mp3", "/noranges.mp3"} {
		d := play(t, s, episode("a", pub.url(path), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
		if d.Mode != "relay" || d.Seekable {
			t.Fatalf("%s: descriptor %+v", path, d)
		}
		pub.reset()
		r := b.get(*d.URL, "bytes=5000-")
		if r.status != 200 || !bytes.Equal(r.body, data) || r.header.Get("Accept-Ranges") != "none" || r.header.Get("ETag") != "" {
			t.Fatalf("%s: %d %v", path, r.status, r.header)
		}
		if h := pub.seen(); len(h) != 1 || h[0].rng != "" {
			t.Fatalf("%s: upstream %+v", path, h)
		}
		if head := b.do(http.MethodHead, *d.URL, nil); head.header.Get("Accept-Ranges") != "none" {
			t.Fatalf("%s: HEAD %v", path, head.header)
		}
	}
	d := play(t, s, episode("a", pub.url("/weak.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if *d.Representation != "weak" {
		t.Fatalf("representation %v", *d.Representation)
	}
	pub.with("/weak.mp3", func(f *file) { f.lastModified = "Tue, 06 Oct 2026 10:00:00 GMT" })
	if r := b.get(*d.URL, ""); r.status != 412 {
		t.Fatalf("weak after a change %d", r.status)
	}
	// A host that gave ranges at the pin and ignores them later: the whole
	// file with Accept-Ranges: none, and the token is no longer seekable.
	pub.set("/fickle.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true})
	d = play(t, s, episode("a", pub.url("/fickle.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	pub.with("/fickle.mp3", func(f *file) { f.ranges = false })
	if r := b.get(*d.URL, "bytes=100-"); r.status != 200 || !bytes.Equal(r.body, data) || r.header.Get("Accept-Ranges") != "none" {
		t.Fatalf("ranges ignored later %d %v", r.status, r.header)
	}
	if head := b.do(http.MethodHead, *d.URL, nil); head.header.Get("Accept-Ranges") != "none" {
		t.Fatalf("HEAD after ranges went away %v", head.header)
	}
}

// The decision table for upstream answers (PROTOCOL §13), on the upstream
// itself.
func TestUpstreamDecisionTable(t *testing.T) {
	t.Parallel()
	var answer func(w http.ResponseWriter, r *http.Request)
	pub := newPublisher(t)
	pub.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { answer(w, r) })
	f := feed.NewFetcher()
	f.AllowLoopback(func(string) bool { return true })
	up := &upstream{fetcher: f, rep: &rep{source: pub.url("/x"), resource: resourceOf(mustURL(pub.url("/x"))), length: 1000, etag: `"e1"`, ranges: true}}
	cases := []struct {
		name   string
		answer func(w http.ResponseWriter, r *http.Request)
		code   string // "" accepted
		status int
	}{
		{"206 as asked", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"e1"`)
			w.Header().Set("Content-Range", "bytes 100-199/1000")
			w.WriteHeader(206)
			w.Write(make([]byte, 100))
		}, "", 206},
		{"206 to If-Range without an ETag", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Range", "bytes 100-199/1000")
			w.WriteHeader(206)
			w.Write(make([]byte, 100))
		}, "", 206},
		{"200 without the ETag", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1000")
			w.Write(make([]byte, 1000))
		}, "media_changed", 0},
		{"206 with a weak ETag", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `W/"e1"`)
			w.Header().Set("Content-Range", "bytes 100-199/1000")
			w.WriteHeader(206)
			w.Write(make([]byte, 100))
		}, "media_changed", 0},
		{"206 at another offset", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Range", "bytes 0-99/1000")
			w.WriteHeader(206)
			w.Write(make([]byte, 100))
		}, "media_unavailable", 0},
		{"206 with another total", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Range", "bytes 100-199/2000")
			w.WriteHeader(206)
			w.Write(make([]byte, 100))
		}, "media_changed", 0},
		{"206 with another ETag", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"e2"`)
			w.Header().Set("Content-Range", "bytes 100-199/1000")
			w.WriteHeader(206)
			w.Write(make([]byte, 100))
		}, "media_changed", 0},
		{"encoded 206", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Range", "bytes 100-199/1000")
			w.WriteHeader(206)
			w.Write(make([]byte, 100))
		}, "media_unavailable", 0},
		{"200 with the pinned validators", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"e1"`)
			w.Header().Set("Content-Length", "1000")
			w.Write(make([]byte, 1000))
		}, "", 200},
		{"200 with another length", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"e1"`)
			w.Header().Set("Content-Length", "999")
			w.Write(make([]byte, 999))
		}, "media_changed", 0},
		{"200 with another ETag", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"e2"`)
			w.Write(make([]byte, 1000))
		}, "media_changed", 0},
		{"416 with the pinned length", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Range", "bytes */1000")
			w.WriteHeader(416)
		}, "", 416},
		{"416 with another length", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Range", "bytes */500")
			w.WriteHeader(416)
		}, "media_changed", 0},
		{"401", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }, "media_unauthorized", 0},
		{"403", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }, "media_unauthorized", 0},
		{"404", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }, "media_not_found", 0},
		{"503", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, "media_unavailable", 0},
	}
	for _, c := range cases {
		answer = c.answer
		o, err := up.open(context.Background(), 100, 199, true)
		switch {
		case c.code == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.code != "" && (err == nil || err.(*Error).Code != c.code):
			t.Errorf("%s: got %v, want %s", c.name, err, c.code)
		case c.code == "" && o.status != c.status:
			t.Errorf("%s: status %d", c.name, o.status)
		}
		if o != nil {
			o.Close()
		}
	}
}

// A hostile publisher's HTML, SVG, script and cookies never become a page
// of ours: the type is the declared media type, the response is sandboxed,
// unsniffable and same-origin only, and no upstream header but the allowed
// ones passes.
func TestRelayHostileContent(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	html := []byte(`<html><script>document.cookie</script><img src=x onerror=alert(1)></html>`)
	svg := []byte(`<svg xmlns="http://www.w3.org/2000/svg" onload="alert(1)"><script>alert(2)</script></svg>`)
	extra := map[string]string{"Set-Cookie": "ntp_session=evil; Path=/", "Location": "https://evil.example/",
		"Content-Security-Policy": "default-src *", "Access-Control-Allow-Origin": "*", "Refresh": "0; url=https://evil.example/",
		"Link": "<https://evil.example/x.js>; rel=preload", "X-Frame-Options": "ALLOWALL"}
	pub.set("/html", &file{data: html, ctype: "text/html", etag: `"h"`, ranges: true, extra: extra})
	pub.set("/svg", &file{data: svg, ctype: "image/svg+xml", etag: `"s"`, ranges: true, extra: extra})
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	for _, c := range []struct{ path, declared, want string }{
		{"/html", "audio/mpeg", "audio/mpeg"},
		{"/svg", "video/mp4", "video/mp4"},
		{"/html", "text/html", "audio/mpeg"},
		{"/svg", "image/svg+xml", "audio/mpeg"},
	} {
		d := play(t, s, episode("a", pub.url(c.path), c.declared), PlayRequest{Capabilities: caps("all")})
		for _, r := range []reply{b.get(*d.URL, ""), b.get(*d.URL, "bytes=0-9"), b.do(http.MethodHead, *d.URL, nil)} {
			h := r.header
			if h.Get("Content-Type") != c.want || h.Get("X-Content-Type-Options") != "nosniff" ||
				h.Get("Content-Security-Policy") != "sandbox; default-src 'none'" || h.Get("Content-Disposition") != "inline" ||
				h.Get("Cross-Origin-Resource-Policy") != "same-origin" {
				t.Fatalf("%s declared %s: headers %v", c.path, c.declared, h)
			}
			for _, k := range []string{"Set-Cookie", "Location", "Access-Control-Allow-Origin", "Refresh", "Link", "X-Frame-Options"} {
				if h.Get(k) != "" {
					t.Fatalf("%s: %s passed through", c.path, k)
				}
			}
		}
	}
}

// Redirects are followed with the fetch rules on every hop; credentials go
// only to their approved origins, after a 401, and never the browser's
// cookie; too many redirects fail.
func TestRelayRedirectsAndCredentials(t *testing.T) {
	t.Parallel()
	feedHost, cdn := newPublisher(t), newPublisher(t)
	data := pattern(50 << 10)
	cdn.set("/private.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true, user: "listener", pass: "pässword"})
	feedHost.set("/ep.mp3", &file{redirect: cdn.url("/private.mp3")})
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	ep := episode("a", feedHost.url("/ep.mp3"), "audio/mpeg")

	// Credentials approved for the feed's origin only: the CDN asks, gets nothing.
	feedOrigin := feed.Origin(mustURL(feedHost.url("/")))
	ep.Credentials = &feed.Credentials{Username: "listener", Password: "pässword", Revision: "r1", ApprovedOrigins: []string{feedOrigin}}
	if e := playFailure(t, s, ep, PlayRequest{Capabilities: caps("all")}); e.Error != "media_unauthorized" || e.Label != "unauthorized" ||
		!strings.HasPrefix(e.Message, "Authentication error.") {
		t.Fatalf("unapproved origin: %+v", e)
	}
	for _, h := range cdn.seen() {
		if h.auth != "" {
			t.Fatalf("credentials reached an unapproved origin: %+v", h)
		}
	}
	// Approved for the CDN: the retry after its 401 carries them (UTF-8 after ISO-8859-1).
	ep.Credentials.ApprovedOrigins = []string{feed.Origin(mustURL(cdn.url("/")))}
	d := play(t, s, ep, PlayRequest{Capabilities: caps("all")})
	if d.Mode != "relay" {
		t.Fatalf("descriptor %+v", d)
	}
	if r := b.get(*d.URL, "bytes=0-99"); r.status != 206 || !bytes.Equal(r.body, data[:100]) {
		t.Fatalf("through the redirect %d", r.status)
	}
	for _, h := range feedHost.seen() {
		if h.auth != "" {
			t.Fatalf("credentials went to the unapproved feed host: %+v", h)
		}
	}
	// Direct playback is never offered with credentials, even over https.
	if d.Mode == "direct" {
		t.Fatal("direct with credentials")
	}

	// Too many redirects.
	loop := newPublisher(t)
	loop.set("/a", &file{redirect: loop.url("/b")})
	loop.set("/b", &file{redirect: loop.url("/a")})
	if e := playFailure(t, s, episode("a", loop.url("/a"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); e.Error != "media_unavailable" || e.Label != "too_many_redirects" {
		t.Fatalf("redirect loop: %+v", e)
	}
	// Not found.
	if e := playFailure(t, s, episode("a", loop.url("/missing"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); e.Error != "media_not_found" || e.Label != "not_found" {
		t.Fatalf("missing: %+v", e)
	}
}

// Loopback destinations are refused, on the first hop and after a redirect.
func TestRelayRefusedDestination(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	pub.set("/ep.mp3", &file{data: pattern(100), ctype: "audio/mpeg"})
	s := newService(t, serviceOpts{refuse: true})
	if e := playFailure(t, s, episode("a", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); e.Error != "media_refused" || e.Label != "refused_destination" {
		t.Fatalf("loopback: %+v", e)
	}
	if len(pub.seen()) != 0 {
		t.Fatal("a refused destination was contacted")
	}
}

// The descriptor's selection (PROTOCOL §13) for direct playback: an https URL
// without credentials whose type the browser plays is direct, even when the
// server cannot reach it; after a direct failure, or for http, the relay.
func TestDescriptorDirect(t *testing.T) {
	t.Parallel()
	s := newService(t, serviceOpts{refuse: true})
	ep := episode("a", "https://127.0.0.1:1/ep.mp3?x=1#frag", "audio/mpeg")
	d := play(t, s, ep, PlayRequest{Capabilities: caps("all")})
	if d.Mode != "direct" || *d.URL != "https://127.0.0.1:1/ep.mp3?x=1" || d.Token != nil {
		t.Fatalf("direct %+v", d)
	}
	// The browser cannot play the declared type: not direct.
	if e := playFailure(t, s, ep, PlayRequest{Capabilities: caps("nothing")}); e.Error != "media_refused" {
		t.Fatalf("unplayable type: %+v", e)
	}
	// After a direct failure: the relay (which here cannot reach the host).
	if e := playFailure(t, s, ep, PlayRequest{Capabilities: caps("all"), DirectFailed: true}); e.Error != "media_refused" {
		t.Fatalf("direct_failed: %+v", e)
	}
	if _, err := s.Play(context.Background(), ep, PlayRequest{Capabilities: caps("all"), Force: "remux"}); err != ErrInvalid {
		t.Fatalf("unknown force: %v", err)
	}
	// Without ffmpeg, a type the browser cannot play is unsupported, with the reason.
	pub := newPublisher(t)
	pub.set("/ep.ogg", &file{data: pattern(1000), ctype: "audio/ogg", etag: `"o"`, ranges: true})
	s2 := newService(t, serviceOpts{})
	d = play(t, s2, episode("a", pub.url("/ep.ogg"), "audio/ogg"), PlayRequest{Capabilities: caps("webkit-no-ogg")})
	if d.Mode != "unsupported" || *d.Reason != "no_ffmpeg" || d.Message == nil {
		t.Fatalf("no ffmpeg %+v", d)
	}
}

// The browser going away closes the upstream connection; a stalled
// upstream ends the stream after the stall time; a truncated one fails it;
// failures abort the response and show in the status.
func TestRelayCancellationStallTruncation(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	data := pattern(4 << 20)
	release := make(chan struct{})
	pub.set("/slow.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true, chunkDelay: 20 * time.Millisecond})
	pub.set("/stall.mp3", &file{data: data, ctype: "audio/mpeg", etag: `"st"`, ranges: true, stallAt: 64 << 10, release: release})
	pub.set("/short.mp3", &file{data: data, ctype: "audio/mpeg", etag: `"sh"`, ranges: true, truncateAt: 100 << 10})
	s := newService(t, serviceOpts{limits: func(l *Limits) { l.Stall = 300 * time.Millisecond }})
	b := newBrowser(t, s, "a")

	// Cancellation.
	d := play(t, s, episode("a", pub.url("/slow.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.srv.URL+*d.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32<<10)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	resp.Body.Close()
	waitFor(t, "the upstream connection to close after the browser left", func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return pub.active == 0
	})
	if st := s.statusOf(t, d, "a"); st.State != "streaming" {
		t.Fatalf("a cancelled stream is neutral: %+v", st)
	}

	// Stall.
	d = play(t, s, episode("a", pub.url("/stall.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	r := b.get(*d.URL, "")
	if r.err == nil || len(r.body) >= len(data) {
		t.Fatalf("a stalled stream ended clean with %d bytes", len(r.body))
	}
	if st := s.statusOf(t, d, "a"); st.State != "failed" || st.Error.Label != "time_limit" {
		t.Fatalf("stall status %+v", st)
	}
	close(release)

	// Truncation.
	d = play(t, s, episode("a", pub.url("/short.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	r = b.get(*d.URL, "")
	if r.err == nil {
		t.Fatalf("a truncated stream ended clean with %d bytes", len(r.body))
	}
	if st := s.statusOf(t, d, "a"); st.State != "failed" || st.Error.Label != "wrong_size" {
		t.Fatalf("truncation status %+v", st)
	}
}

// At most four streams per account: a fifth answers 429 server_busy with
// Retry-After; another account is not affected; a finished stream frees
// its place.
func TestRelayAccountLimit(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	data := pattern(1 << 20)
	release := make(chan struct{})
	pub.set("/ep.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true, stallAt: 1 << 10, release: release})
	s := newService(t, serviceOpts{})
	ba, bb := newBrowser(t, s, "a"), newBrowser(t, s, "b")
	da := play(t, s, episode("a", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	db := play(t, s, episode("b", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	var bodies []*http.Response
	for i := 0; i < 4; i++ {
		resp, err := http.Get(ba.srv.URL + *da.URL)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("stream %d: %v", i, err)
		}
		bodies = append(bodies, resp)
	}
	r := ba.get(*da.URL, "")
	if r.status != 429 || r.header.Get("Retry-After") == "" || !strings.Contains(string(r.body), "server_busy") {
		t.Fatalf("fifth stream %d %v %s", r.status, r.header, r.body)
	}
	resp, err := http.Get(bb.srv.URL + *db.URL)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("another account: %v %v", err, resp)
	}
	bodies = append(bodies, resp)
	close(release)
	for _, b := range bodies {
		b.Body.Close()
	}
	waitFor(t, "the streams to end", func() bool {
		s.relayMu.Lock()
		defer s.relayMu.Unlock()
		return len(s.relay) == 0
	})
	if r := ba.get(*da.URL, "bytes=0-9"); r.status != 206 {
		t.Fatalf("after the streams ended %d", r.status)
	}
}

// Tokens belong to one account and one session credential, and expire
// after the idle time.
func TestTokens(t *testing.T) {
	t.Parallel()
	pub := newPublisher(t)
	data := pattern(1000)
	pub.set("/ep.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true})
	now := time.Now()
	s := newService(t, serviceOpts{})
	s.cfg.Now = func() time.Time { return now }
	d := play(t, s, episode("a", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if len(*d.Token) != 32 {
		t.Fatalf("token %q", *d.Token)
	}
	if _, ok := s.Lookup(*d.Token, "b", "cred-a"); ok {
		t.Fatal("another account's token")
	}
	if _, ok := s.Lookup(*d.Token, "a", "cred-other-session"); ok {
		t.Fatal("another session's token")
	}
	if _, ok := s.Lookup(*d.Token, "a", "cred-a"); !ok {
		t.Fatal("own token")
	}
	now = now.Add(11 * time.Hour)
	s.tokens.sweep(now)
	if _, ok := s.Lookup(*d.Token, "a", "cred-a"); !ok {
		t.Fatal("expired before 12 idle hours")
	}
	now = now.Add(12*time.Hour + time.Minute)
	s.tokens.sweep(now)
	if _, ok := s.Lookup(*d.Token, "a", "cred-a"); ok {
		t.Fatal("alive after 12 idle hours")
	}
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

// The status waits up to two seconds for a stream still finishing, so an
// `ended` cannot outrun the completion (PROTOCOL §13); the newest of a completion
// and a failure decides, and cancelled streams are neutral.
func TestStatusOrdering(t *testing.T) {
	t.Parallel()
	tok := &token{changed: make(chan struct{})}
	if st := tok.status(context.Background(), time.Second); st.State != "streaming" || st.StreamID != 0 {
		t.Fatalf("before any stream %+v", st)
	}
	a := tok.begin(time.Now())
	go func() {
		time.Sleep(100 * time.Millisecond)
		tok.finish(a, outComplete, nil)
	}()
	if st := tok.status(context.Background(), 2*time.Second); st.State != "complete" || st.StreamID != a {
		t.Fatalf("the finishing stream %+v", st)
	}
	b := tok.begin(time.Now())
	tok.finish(b, outCancelled, nil)
	if st := tok.status(context.Background(), time.Second); st.State != "complete" {
		t.Fatalf("after a cancelled stream %+v", st)
	}
	c := tok.begin(time.Now())
	tok.finish(c, outFailed, errChanged)
	if st := tok.status(context.Background(), time.Second); st.State != "failed" || st.StreamID != c || st.Error.Error != "media_changed" {
		t.Fatalf("after a failure %+v", st)
	}
	d := tok.begin(time.Now())
	tok.finish(d, outComplete, nil)
	if st := tok.status(context.Background(), time.Second); st.State != "complete" || st.StreamID != d {
		t.Fatalf("a later completion %+v", st)
	}
	// A stream that never finishes: the status answers after the wait.
	tok.begin(time.Now())
	start := time.Now()
	if st := tok.status(context.Background(), 200*time.Millisecond); st.State != "complete" || time.Since(start) < 200*time.Millisecond {
		t.Fatalf("with a stream running %+v", st)
	}
}
