package media

// The descriptor's fast path for direct playback (PROTOCOL §13) and the
// direct-failure memo: a direct candidate whose declared type the browser
// says it plays is answered without fetching anything; every other request
// pins (and probes) as before; a direct failure makes the account's later
// descriptors of the source choose the relay, for a day, within a bound.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

// clock is a settable time for the memo's lifetime.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// mp3Publisher is an HTTPS publisher of one audio/mpeg file with a strong
// ETag and ranges, at /ep.mp3 and at /ep (no extension).
func mp3Publisher(t *testing.T) (*publisher, []byte) {
	pub := newHTTPSPublisher(t)
	data := pattern(4000)
	f := &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true}
	pub.set("/ep.mp3", f)
	pub.set("/ep", f)
	return pub, data
}

func TestPlayFastDirectMakesNoUpstreamRequest(t *testing.T) {
	t.Parallel()
	pub, _ := mp3Publisher(t)
	s := newService(t, serviceOpts{})
	ep := episode("a", pub.url("/ep.mp3?x=1#t=0"), "audio/mpeg")
	ep.DurationMS = 61_000
	d := play(t, s, ep, PlayRequest{Capabilities: caps("all"), StartMS: 5_000})
	if d.Mode != "direct" || *d.URL != pub.url("/ep.mp3?x=1") || *d.Type != "audio/mpeg" || !d.Seekable || d.DurationMS == nil || *d.DurationMS != 61_000 ||
		d.Token != nil || d.Representation != nil || d.Profile != nil || d.OffsetMS != 0 {
		t.Fatalf("descriptor %+v", d)
	}
	// Without a declared type, the extension's.
	d = play(t, s, episode("a", pub.url("/ep.mp3"), ""), PlayRequest{Capabilities: caps("all")})
	if d.Mode != "direct" || *d.Type != "audio/mpeg" || d.DurationMS != nil {
		t.Fatalf("by extension %+v", d)
	}
	if n := len(pub.seen()); n != 0 {
		t.Fatalf("the fast path made %d upstream requests: %+v", n, pub.seen())
	}
}

// Every request the fast path does not take pins (and probes) the
// representation, and is answered as before.
func TestPlayFastPathNotTaken(t *testing.T) {
	t.Parallel()
	pub, _ := mp3Publisher(t)
	plain := newPublisher(t)
	data := pattern(4000)
	plain.set("/ep.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true})
	s := newService(t, serviceOpts{})
	creds := &feed.Credentials{Username: "u", Password: "p", ApprovedOrigins: []string{feed.Origin(mustURL(pub.url("/")))}}
	cases := []struct {
		name  string
		ep    Episode
		req   PlayRequest
		want  string // the mode
		ctype string // the descriptor's type, when it has one
		pub   *publisher
	}{
		// No declared type and no extension: the publisher's Content-Type decides, after the pin.
		{"unknown type", episode("a", pub.url("/ep"), ""), PlayRequest{Capabilities: caps("all")}, "direct", "audio/mpeg", pub},
		// A declared type the browser does not play, of a file it does: direct after the pin, as before.
		{"unplayable declared type", episode("a", pub.url("/ep.mp3"), "audio/ogg"), PlayRequest{Capabilities: caps("mp3-and-aac")}, "direct", "audio/mpeg", pub},
		{"credentials", Episode{Account: "a", Credential: "cred-a", ID: "e1", SourceURL: pub.url("/ep.mp3"), DeclaredType: "audio/mpeg", Credentials: creds},
			PlayRequest{Capabilities: caps("all")}, "relay", "audio/mpeg", pub},
		{"http", episode("a", plain.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}, "relay", "audio/mpeg", plain},
		{"direct_failed", episode("b", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all"), DirectFailed: true}, "relay", "audio/mpeg", pub},
		// Compat without ffmpeg is unsupported, with the reason; the pin still came first.
		{"force compat", episode("c", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all"), Force: "compat"}, "unsupported", "", pub},
		{"nothing playable", episode("c", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("nothing")}, "unsupported", "", pub},
	}
	for _, c := range cases {
		before := len(c.pub.seen())
		d := play(t, s, c.ep, c.req)
		if d.Mode != c.want || (c.ctype != "" && (d.Type == nil || *d.Type != c.ctype)) {
			t.Fatalf("%s: %+v, want %s", c.name, d, c.want)
		}
		pinned := false
		for _, h := range c.pub.seen()[before:] {
			pinned = pinned || h.rng == "bytes=0-1"
		}
		if !pinned {
			t.Fatalf("%s: the representation was not pinned: %+v", c.name, c.pub.seen()[before:])
		}
	}
}

// A direct failure gives the relay, and the account's later ordinary
// descriptors of the source stay on the relay for a day (the failure is not
// tried again); other accounts and sources still play directly.
func TestDirectFailedUsesRelayAndRemembers(t *testing.T) {
	t.Parallel()
	pub, data := mp3Publisher(t)
	other := newHTTPSPublisher(t)
	other.set("/other.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true})
	start := time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC)
	c := &clock{t: start}
	s := newService(t, serviceOpts{now: c.now})
	b := newBrowser(t, s, "a")
	ep := episode("a", pub.url("/ep.mp3"), "audio/mpeg")
	ordinary := PlayRequest{Capabilities: caps("all")}

	if d := play(t, s, ep, ordinary); d.Mode != "direct" || len(pub.seen()) != 0 {
		t.Fatalf("first: %+v after %d requests", d, len(pub.seen()))
	}
	// The browser could not play the publisher's URL: it asks again with direct_failed.
	d := play(t, s, ep, PlayRequest{Capabilities: caps("all"), DirectFailed: true})
	if d.Mode != "relay" || d.Token == nil {
		t.Fatalf("after the failure: %+v", d)
	}
	if r := b.get(*d.URL, "bytes=0-99"); r.status != 206 || string(r.body) != string(data[:100]) {
		t.Fatalf("the relay serves the file: %d", r.status)
	}
	// Coming back to the episode: an ordinary request is relayed, never direct again.
	for _, at := range []time.Duration{time.Minute, 24*time.Hour - time.Millisecond} {
		c.set(start.Add(at))
		pinned := len(pub.seen())
		if d := play(t, s, ep, ordinary); d.Mode != "relay" || d.Token == nil {
			t.Fatalf("an ordinary request %s after the failure: %+v", at, d)
		}
		if len(pub.seen()) == pinned {
			t.Fatal("the relay pins the representation")
		}
	}
	// Another account, and another source of this account, still play directly.
	seen := len(pub.seen())
	if d := play(t, s, episode("b", pub.url("/ep.mp3"), "audio/mpeg"), ordinary); d.Mode != "direct" {
		t.Fatalf("another account: %+v", d)
	}
	if d := play(t, s, episode("a", other.url("/other.mp3"), "audio/mpeg"), ordinary); d.Mode != "direct" {
		t.Fatalf("another source: %+v", d)
	}
	if len(pub.seen()) != seen || len(other.seen()) != 0 {
		t.Fatal("a direct descriptor fetched something")
	}
	// A day after the failure the publisher's URL is tried again.
	c.set(start.Add(24 * time.Hour))
	if d := play(t, s, ep, ordinary); d.Mode != "direct" {
		t.Fatalf("a day later: %+v", d)
	}
	// A restarted server has forgotten; the browser remembers, and asks with direct_failed (the relay).
	s2 := newService(t, serviceOpts{now: c.now})
	if d := play(t, s2, ep, PlayRequest{Capabilities: caps("all"), DirectFailed: true}); d.Mode != "relay" {
		t.Fatalf("after a restart, with direct_failed: %+v", d)
	}
	if d := play(t, s2, ep, ordinary); d.Mode != "relay" {
		t.Fatalf("after a restart, then an ordinary request: %+v", d)
	}
}

// The memo keeps the most recently failed sources, at most directFailureMax, each for a day.
func TestDirectFailureMemoExpiryAndBound(t *testing.T) {
	t.Parallel()
	m := newDirectFailures()
	t0 := time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC)
	src := func(i int) string { return fmt.Sprintf("https://publisher.test/%d.mp3", i) }
	for i := range directFailureMax {
		m.remember("a", src(i), t0.Add(time.Duration(i)*time.Millisecond))
	}
	if m.len() != directFailureMax || !m.failed("a", src(0), t0) || m.failed("b", src(0), t0) {
		t.Fatalf("full: %d", m.len())
	}
	m.remember("a", src(0), t0.Add(time.Second)) // failed again: the most recent
	m.remember("a", src(directFailureMax), t0.Add(time.Second))
	if m.len() != directFailureMax || !m.failed("a", src(0), t0.Add(time.Second)) || m.failed("a", src(1), t0.Add(time.Second)) {
		t.Fatalf("the least recently failed goes first: %d", m.len())
	}
	if !m.failed("a", src(directFailureMax), t0.Add(time.Second+directFailureTTL-time.Millisecond)) {
		t.Fatal("kept for a day")
	}
	if m.failed("a", src(directFailureMax), t0.Add(time.Second+directFailureTTL)) || m.len() != directFailureMax-1 {
		t.Fatalf("expired after a day, and dropped: %d", m.len())
	}
}

// Asking for a descriptor still ends the account's previous playback, also on the fast path.
func TestDescriptorSideEffectsUnchanged(t *testing.T) {
	t.Parallel()
	pub, _ := mp3Publisher(t)
	plain := newPublisher(t)
	data := pattern(4000)
	plain.set("/ep.mp3", &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true})
	s := newService(t, serviceOpts{})
	current := func() string {
		s.curMu.Lock()
		defer s.curMu.Unlock()
		return s.current["a"]
	}
	d := play(t, s, episode("a", plain.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if d.Mode != "relay" || current() != *d.Token {
		t.Fatalf("relay %+v, current %q", d, current())
	}
	if d := play(t, s, episode("a", pub.url("/ep.mp3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); d.Mode != "direct" {
		t.Fatalf("direct %+v", d)
	}
	if current() != "" {
		t.Fatalf("the relay's playback is still current: %q", current())
	}
}
