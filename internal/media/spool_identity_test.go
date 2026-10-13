package media

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// (a) The query is part of the resource: /download?id=A and ?id=B are two
// resources, whatever their ETag and length; only a change of signature,
// expiry or tracking parameters keeps the resource.
func TestQueryIdentity(t *testing.T) {
	t.Parallel()
	p := newPublisher(t)
	p.set("/download?id=A", &file{data: bytes.Repeat([]byte("A"), 1000), ctype: "audio/mpeg", etag: `"v1"`, ranges: true})
	p.set("/download?id=B", &file{data: bytes.Repeat([]byte("B"), 1000), ctype: "audio/mpeg", etag: `"v1"`, ranges: true})
	p.set("/ep", &file{redirect: p.url("/download?id=A")})
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", p.url("/ep"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if r := b.get(*d.URL, "bytes=0-99"); r.status != 206 || r.body[0] != 'A' {
		t.Fatalf("first %d", r.status)
	}
	p.set("/ep", &file{redirect: p.url("/download?id=B")})
	if r := b.get(*d.URL, "bytes=100-199"); r.status != 412 || bytes.Contains(r.body, []byte("B")) {
		t.Fatalf("?id=B under ?id=A's representation: %d %q", r.status, r.body)
	}

	// Signatures and expiries that change on every redirect: still the same resource.
	p.set("/cdn/episode.mp3", &file{data: bytes.Repeat([]byte("A"), 1000), ctype: "audio/mpeg", etag: `"v1"`, ranges: true})
	signed := func(n int) string {
		return p.url("/cdn/episode.mp3?id=A&Expires=" + string(rune('0'+n)) + "&Signature=sig" + string(rune('0'+n)) +
			"&Key-Pair-Id=K&X-Amz-Date=2026100" + string(rune('0'+n)) + "&utm_campaign=c" + string(rune('0'+n)))
	}
	p.set("/signed", &file{redirect: signed(1)})
	d = play(t, s, episode("a", p.url("/signed"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	for n := 2; n <= 4; n++ {
		p.set("/signed", &file{redirect: signed(n)})
		if r := b.get(*d.URL, "bytes=0-99"); r.status != 206 || r.body[0] != 'A' {
			t.Fatalf("a new signature and expiry (%d): %d", n, r.status)
		}
	}
	if st := s.statusOf(t, d, "a"); st.State == "failed" {
		t.Fatalf("status %+v", st)
	}
}

func TestStableQuery(t *testing.T) {
	t.Parallel()
	same := [][2]string{
		{"id=A&b=2", "b=2&id=A"}, // order
		{"id=A%20B", "id=A+B"},   // encoding
		{"id=A&Signature=1&Expires=2&Key-Pair-Id=k&Policy=p", "id=A"}, // CloudFront
		{"id=A&X-Amz-Signature=1&x-amz-date=2&X-Goog-Expires=3", "id=A"},
		{"id=A&sig=1&signature=2&token=3&__token__=4&hdnts=5&hmac=6&exp=7&expires=8", "id=A"},
		{"id=A&st=1&e=2", "id=A"},  // a start and an end
		{"id=A&st=1&se=2", "id=A"}, // Azure's
		{"id=A&utm_source=x&utm_medium=y&_gl=z&awCollectionId=1&awEpisodeId=2&request_event_id=3&timestamp=4", "id=A"},
	}
	for _, c := range same {
		if stableQuery(c[0]) != stableQuery(c[1]) {
			t.Errorf("%q and %q differ: %q, %q", c[0], c[1], stableQuery(c[0]), stableQuery(c[1]))
		}
	}
	differ := [][2]string{
		{"id=A", "id=B"},
		{"id=A", "id=A&id=A"}, // a multiset: repeats count
		{"id=A&st=1", "id=A"}, // st alone is not a token's start
		{"id=A&e=1", "id=A"},
		{"file=x.mp3", "file=y.mp3"},
	}
	for _, c := range differ {
		if stableQuery(c[0]) == stableQuery(c[1]) {
			t.Errorf("%q and %q are the same: %q", c[0], c[1], stableQuery(c[0]))
		}
	}
}

// (b) Eviction protects live playback, not every recent copy: two copies
// played to the end (complete, no lease) give way to a third playback; a
// copy superseded by its account's next descriptor gives way too.
func TestSpoolCompletedOrSuperseded(t *testing.T) {
	t.Parallel()
	p := newPublisher(t)
	for _, path := range []string{"/0", "/1", "/2", "/3"} {
		p.set(path, &file{data: pattern(50 << 10), ctype: "audio/mpeg", noLength: true})
	}
	s := newService(t, serviceOpts{})
	b := newBrowser(t, s, "a")
	var ds []*Descriptor
	for _, path := range []string{"/0", "/1"} {
		d := play(t, s, episode("a", p.url(path), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
		if r := b.get(*d.URL, ""); r.status != 200 {
			t.Fatalf("%s: %d", path, r.status)
		}
		if st := s.statusOf(t, d, "a"); st.State != "complete" {
			t.Fatalf("%s: %+v", path, st)
		}
		ds = append(ds, d)
	}
	d := play(t, s, episode("a", p.url("/2"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if d.Mode != "relay" || *d.Representation != "spool" {
		t.Fatalf("a third playback after two completed: %+v", d)
	}
	// Superseded, though not complete: account a plays /3 after only
	// starting /2; b's paused playback is not touched.
	if r := b.get(*d.URL, "bytes=0-9"); r.status != 206 {
		t.Fatalf("/2 started: %d", r.status)
	}
	bb := newBrowser(t, s, "b")
	db := play(t, s, episode("b", p.url("/0"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if r := bb.get(*db.URL, "bytes=0-9"); r.status != 206 {
		t.Fatalf("b's playback: %d", r.status)
	}
	d3 := play(t, s, episode("a", p.url("/3"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if d3.Mode != "relay" {
		t.Fatalf("a's next episode after a superseded one: %+v", d3)
	}
	if r := bb.get(*db.URL, "bytes=10-19"); r.status != 206 {
		t.Fatalf("b's paused playback lost its copy: %d", r.status)
	}
}

// A paused but current playback keeps its copy within the grace time; an
// abandoned one (idle beyond it) gives way.
func TestSpoolPausedPlaybackProtected(t *testing.T) {
	t.Parallel()
	p := newPublisher(t)
	for _, path := range []string{"/0", "/1", "/2"} {
		p.set(path, &file{data: pattern(50 << 10), ctype: "audio/mpeg", noLength: true})
	}
	now := time.Now()
	var clock sync.Mutex
	s := newService(t, serviceOpts{})
	s.cfg.Now = func() time.Time { clock.Lock(); defer clock.Unlock(); return now }
	advance := func(d time.Duration) { clock.Lock(); now = now.Add(d); clock.Unlock() }
	for i, account := range []string{"a", "b"} {
		b := newBrowser(t, s, account)
		d := play(t, s, episode(account, p.url("/"+string(rune('0'+i))), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
		if r := b.get(*d.URL, "bytes=0-9"); r.status != 206 {
			t.Fatalf("%s: %d", account, r.status)
		}
	}
	advance(spoolGrace - time.Minute)
	if _, err := s.Play(context.Background(), episode("c", p.url("/2"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); !isBusy(err) {
		t.Fatalf("two paused current playbacks: %v", err)
	}
	advance(2 * time.Minute)
	if d := play(t, s, episode("c", p.url("/2"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")}); d.Mode != "relay" {
		t.Fatalf("after the grace time: %+v", d)
	}
}
