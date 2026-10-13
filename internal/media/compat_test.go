package media

// The compatibility path (PROTOCOL §13) with tone files made by ffmpeg in the
// test: second k of every file is a tone of 300 + 50k Hz, so the decoded output
// shows which media time it carries.

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

var toneTypes = map[string]string{
	"ogg-vorbis": "audio/ogg", "ogg-opus": "audio/ogg", "mp3": "audio/mpeg", "m4a": "audio/mp4",
	"m4a-tail": "audio/mp4", "flac": "audio/flac", "webm": "audio/webm",
}

// Every format converts from a time and keeps the timeline: the output's
// first second is the original's at start_ms, its length is what remains,
// a seek (a reload with start_ms) lands on its time, and a run that reached
// the end is complete.
func TestCompatFormats(t *testing.T) {
	ffmpegBinary(t)
	for _, format := range []string{"ogg-vorbis", "ogg-opus", "mp3", "m4a", "flac", "webm"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			data := toneFile(t, format)
			pub := newPublisher(t)
			pub.set("/ep", &file{data: data, ctype: toneTypes[format], etag: etagOf(data), ranges: true})
			s := newService(t, serviceOpts{withFFmp: true})
			b := newBrowser(t, s, "a")
			// A browser that plays only MP3 and ADTS AAC; MP3 itself is forced.
			d := play(t, s, episode("a", pub.url("/ep"), toneTypes[format]),
				PlayRequest{Capabilities: caps("mp3-and-aac"), StartMS: 7500, Force: map[bool]string{true: "compat"}[format == "mp3"]})
			if d.Mode != "compat" || *d.Profile != "aac" || *d.Type != "audio/aac" || d.OffsetMS != 7500 || d.Seekable {
				t.Fatalf("descriptor %+v", d)
			}
			if d.DurationMS == nil || math.Abs(float64(*d.DurationMS)-20000) > 100 {
				t.Fatalf("duration %v", d.DurationMS)
			}
			r := b.get(*d.URL, "bytes=0-")
			if r.status != 200 || r.err != nil || r.header.Get("Content-Type") != "audio/aac" || r.header.Get("Accept-Ranges") != "none" ||
				r.header.Get("Content-Security-Policy") != "sandbox; default-src 'none'" {
				t.Fatalf("stream %d %v %v", r.status, r.err, r.header)
			}
			freqs := frequencies(t, r.body, "aac")
			checkTimeline(t, freqs, 7500, format+" from 7.5 s")
			if got := float64(len(freqs)) * 0.25; got < 12 || got > 13 {
				t.Fatalf("%.2f s of output from 7.5 s of 20 s", got)
			}
			if st := s.statusOf(t, d, "a"); st.State != "complete" {
				t.Fatalf("status %+v", st)
			}
			// A seek reloads the same token at another time.
			r = b.get(*d.URL+"?start_ms=15000", "")
			checkTimeline(t, frequencies(t, r.body, "aac"), 15000, format+" from 15 s")
			if st := s.statusOf(t, d, "a"); st.State != "complete" {
				t.Fatalf("status after the seek %+v", st)
			}
			// A byte offset into a conversion has no meaning.
			if r := b.get(*d.URL, "bytes=1000-"); r.status != 416 {
				t.Fatalf("a byte range into a conversion: %d", r.status)
			}
		})
	}
}

// The probe's facts and each engine's answers decide (PROTOCOL §13): every
// engine relays what it plays; a WebKit without Ogg gets Ogg and WebM
// through the compatibility path.
func TestCompatDecisionPerEngine(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	pub := newPublisher(t)
	s := newService(t, serviceOpts{withFFmp: true})
	for _, format := range []string{"ogg-vorbis", "ogg-opus", "mp3", "m4a", "m4a-tail", "flac", "webm"} {
		data := toneFile(t, format)
		// Declared as MP3 on purpose: the probe, not the feed, decides.
		pub.set("/"+format, &file{data: data, ctype: "audio/mpeg", etag: etagOf(data), ranges: true})
		for _, engine := range []string{"all", "webkit-no-ogg"} {
			d := play(t, s, episode("a", pub.url("/"+format), "audio/mpeg"), PlayRequest{Capabilities: caps(engine)})
			want := "relay"
			if engine == "webkit-no-ogg" && (strings.HasPrefix(format, "ogg") || format == "webm") {
				want = "compat"
			}
			if d.Mode != want {
				t.Errorf("%s in %s: %s, want %s", format, engine, d.Mode, want)
			}
			wantType := map[string]string{"ogg-vorbis": "audio/ogg", "ogg-opus": "audio/ogg", "mp3": "audio/mpeg", "m4a": "audio/mp4",
				"m4a-tail": "audio/mp4", "flac": "audio/flac", "webm": "audio/webm"}[format]
			if want == "relay" && *d.Type != wantType {
				t.Errorf("%s: served as %s, want %s", format, *d.Type, wantType)
			}
		}
	}
}

// On a host without ranges, the probe reads from the start within its
// budget, and a conversion decodes from the start and discards to the time
// (PROTOCOL §13): the output still starts at start_ms.
func TestCompatWithoutRanges(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	data := toneFile(t, "ogg-opus")
	pub := newPublisher(t)
	pub.set("/ep.opus", &file{data: data, ctype: "audio/ogg", etag: etagOf(data)})
	s := newService(t, serviceOpts{withFFmp: true})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("webkit-no-ogg"), StartMS: 7500})
	if d.Mode != "compat" {
		t.Fatalf("descriptor %+v", d)
	}
	pub.reset()
	r := b.get(*d.URL, "")
	checkTimeline(t, frequencies(t, r.body, "aac"), 7500, "without ranges")
	for _, h := range pub.seen() {
		if h.rng != "" {
			t.Fatalf("a range went to a host without them: %+v", h)
		}
	}
	if st := s.statusOf(t, d, "a"); st.State != "complete" {
		t.Fatalf("status %+v", st)
	}
	// The relay of a type the browser plays on such a host is not seekable;
	// a seek beyond what was buffered asks for the compatibility path.
	d = play(t, s, episode("a", pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("all")})
	if d.Mode != "relay" || d.Seekable {
		t.Fatalf("relay without ranges %+v", d)
	}
	d = play(t, s, episode("a", pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("all"), Force: "compat", StartMS: 12000})
	if d.Mode != "compat" || d.OffsetMS != 12000 {
		t.Fatalf("forced %+v", d)
	}
	checkTimeline(t, frequencies(t, b.get(*d.URL, "").body, *d.Profile), 12000, "forced from 12 s")
}

// A feed that reuses one address for each new episode, served without
// validators or a length: a conversion of the next episode reads its own
// copy and takes its duration from it, not the earlier episode's facts.
func TestCompatNextEpisodeAtTheSameAddress(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	short, long := toneFile(t, "ogg-opus"), toneFile(t, "ogg-opus-long")
	pub := newPublisher(t)
	s := newService(t, serviceOpts{withFFmp: true})
	ep := episode("a", pub.url("/latest.opus"), "audio/ogg")
	for i, c := range []struct {
		data     []byte
		duration float64
	}{{short, 20000}, {long, 600000}} {
		pub.set("/latest.opus", &file{data: c.data, ctype: "audio/ogg", noLength: true})
		ep.ID = "episode-" + strconv.Itoa(i)
		d := play(t, s, ep, PlayRequest{Capabilities: caps("webkit-no-ogg"), StartMS: 1000})
		if d.Mode != "compat" || *d.Representation != "spool" {
			t.Fatalf("episode %d: descriptor %+v", i, d)
		}
		if d.DurationMS == nil {
			t.Fatalf("episode %d: no duration", i)
		}
		if got := float64(*d.DurationMS); math.Abs(got-c.duration) > 100 {
			t.Fatalf("episode %d: duration %.0f ms, want %.0f", i, got, c.duration)
		}
	}
}

// The same, served with its length and ranges but no strong ETag, the two
// episodes padded to one length (an MP4 free box): the next episode is
// probed again, so its duration and the seek it allows are its own.
func TestCompatEqualLengthNextEpisodeAtTheSameAddress(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	short, long := toneFile(t, "m4a"), toneFile(t, "m4a-long")
	size := max(len(short), len(long))
	padded := func(b []byte) []byte {
		pad := size - len(b)
		if pad == 0 {
			return bytes.Clone(b)
		}
		if pad < 8 {
			t.Fatalf("%d bytes are too few for an MP4 free box", pad)
		}
		box := make([]byte, pad)
		binary.BigEndian.PutUint32(box, uint32(pad))
		copy(box[4:], "free")
		return append(bytes.Clone(b), box...)
	}
	pub := newPublisher(t)
	s := newService(t, serviceOpts{withFFmp: true})
	ep := episode("a", pub.url("/latest.m4a"), "audio/mp4")
	for i, c := range []struct {
		data             []byte
		duration, offset int64
	}{{padded(short), 20000, 20000}, {padded(long), 600000, 300000}} {
		pub.set("/latest.m4a", &file{data: c.data, ctype: "audio/mp4", ranges: true})
		ep.ID = "episode-" + strconv.Itoa(i)
		d := play(t, s, ep, PlayRequest{Capabilities: caps("all"), Force: "compat", StartMS: 300000})
		if d.Mode != "compat" || d.DurationMS == nil {
			t.Fatalf("episode %d: descriptor %+v", i, d)
		}
		if got := *d.DurationMS; math.Abs(float64(got-c.duration)) > 100 || d.OffsetMS != c.offset {
			t.Fatalf("episode %d: duration %d ms, offset %d ms; want %d and %d", i, got, d.OffsetMS, c.duration, c.offset)
		}
	}
}

// A tail-index M4A on a host without ranges: the index is beyond the probe
// budget, so the server copies the file (PROTOCOL §13) and serves it with real
// ranges and its own validators; a conversion reads the copy.
func TestCompatTailIndexSpool(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	data := toneFile(t, "m4a-tail")
	pub := newPublisher(t)
	pub.set("/ep.m4a", &file{data: data, ctype: "audio/mp4", etag: etagOf(data)})
	// A budget below the file's length: the index at its end is beyond it.
	s := newService(t, serviceOpts{withFFmp: true, limits: func(l *Limits) { l.ProbeBudget = 128 << 10 }})
	if len(data) <= 128<<10 {
		t.Fatalf("the tone file is only %d bytes", len(data))
	}
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", pub.url("/ep.m4a"), "audio/mp4"), PlayRequest{Capabilities: caps("all")})
	if d.Mode != "relay" || *d.Representation != "spool" || !d.Seekable {
		t.Fatalf("descriptor %+v", d)
	}
	r := b.get(*d.URL, "")
	if !bytes.Equal(r.body, data) {
		t.Fatal("the copy differs")
	}
	// The copy's own validators once it is complete: its length and a hash of its first MiB.
	r = b.get(*d.URL, "bytes=1000-1999")
	if r.status != 206 || !bytes.Equal(r.body, data[1000:2000]) || !strings.HasPrefix(r.header.Get("ETag"), `"ntp-`) ||
		!strings.HasSuffix(r.header.Get("ETag"), "-"+strconv.Itoa(len(data))+`"`) {
		t.Fatalf("from the copy %d %v", r.status, r.header)
	}
	d = play(t, s, episode("a", pub.url("/ep.m4a"), "audio/mp4"), PlayRequest{Capabilities: caps("webkit-no-ogg"), Force: "compat", StartMS: 4000})
	if d.Mode != "compat" || *d.Representation != "spool" {
		t.Fatalf("compat descriptor %+v (%s)", d, *d.Reason)
	}
	checkTimeline(t, frequencies(t, b.get(*d.URL, "").body, "aac"), 4000, "tail index from the copy")
}

// The browser going away kills the conversion: its slot is free, the
// process reaped, and the upstream connection closed.
func TestCompatKilledOnDisconnect(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	data := toneFile(t, "ogg-opus")
	pub := newPublisher(t)
	pub.set("/ep.opus", &file{data: data, ctype: "audio/ogg", etag: etagOf(data), ranges: true, chunkDelay: 50 * time.Millisecond})
	s := newService(t, serviceOpts{withFFmp: true})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("webkit-no-ogg")})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.srv.URL+*d.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resp.Body.Read(make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}
	s.conv.mu.Lock()
	running := s.conv.running
	s.conv.mu.Unlock()
	if running != 1 {
		t.Fatalf("%d conversions running", running)
	}
	cancel()
	resp.Body.Close()
	waitFor(t, "the conversion to end", func() bool {
		s.conv.mu.Lock()
		defer s.conv.mu.Unlock()
		return s.conv.running == 0
	})
	waitFor(t, "the reader's upstream to close", func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return pub.active == 0
	})
	if st := s.statusOf(t, d, "a"); st.State == "complete" || st.State == "failed" {
		t.Fatalf("a cancelled conversion is neutral: %+v", st)
	}
}

// A producer that ends early never completes: the conversion fails, the
// response is aborted, and the status says failed.
func TestCompatTruncatedProducer(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	data := toneFile(t, "ogg-opus")
	pub := newPublisher(t)
	pub.set("/ep.opus", &file{data: data, ctype: "audio/ogg", etag: etagOf(data), ranges: true})
	s := newService(t, serviceOpts{withFFmp: true})
	b := newBrowser(t, s, "a")
	d := play(t, s, episode("a", pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("webkit-no-ogg")})
	pub.with("/ep.opus", func(f *file) { f.truncateAt = int64(len(data) / 2) })
	r := b.get(*d.URL, "")
	if r.err == nil {
		t.Fatalf("a truncated conversion ended clean (%d bytes)", len(r.body))
	}
	st := s.statusOf(t, d, "a")
	if st.State != "failed" {
		t.Fatalf("status %+v", st)
	}
}

// At most two conversions per server and one per account: a third account
// is told the server is busy; the same account's new stream ends its old
// one (a seek), which is not a failure. The episodes are ten minutes long,
// so a conversion whose output nobody reads stays running.
func TestCompatLimits(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	data := toneFile(t, "ogg-opus-long")
	pub := newPublisher(t)
	pub.set("/ep.opus", &file{data: data, ctype: "audio/ogg", etag: etagOf(data), ranges: true})
	s := newService(t, serviceOpts{withFFmp: true})
	descriptors, browsers := map[string]*Descriptor{}, map[string]*browser{}
	for _, account := range []string{"a", "b", "c"} {
		browsers[account] = newBrowser(t, s, account)
		descriptors[account] = play(t, s, episode(account, pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("webkit-no-ogg")})
	}
	open := func(account, query string) *http.Response {
		resp, err := http.Get(browsers[account].srv.URL + *descriptors[account].URL + query)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	running := func() int {
		s.conv.mu.Lock()
		defer s.conv.mu.Unlock()
		return s.conv.running
	}
	ra, rb := open("a", ""), open("b", "")
	if ra.StatusCode != 200 || rb.StatusCode != 200 || running() != 2 {
		t.Fatalf("first two: %d %d, %d running", ra.StatusCode, rb.StatusCode, running())
	}
	rc := open("c", "")
	if rc.StatusCode != http.StatusServiceUnavailable || rc.Header.Get("Retry-After") == "" {
		t.Fatalf("a third conversion: %d", rc.StatusCode)
	}
	rc.Body.Close()
	if st := s.statusOf(t, descriptors["c"], "c"); st.State != "failed" || st.Error.Error != "server_busy" {
		t.Fatalf("the refused stream's status %+v", st)
	}
	// Account a seeks: its new stream replaces the old one at once.
	ra2 := open("a", "?start_ms=5000")
	if ra2.StatusCode != 200 || running() != 2 {
		t.Fatalf("the seek: %d, %d running", ra2.StatusCode, running())
	}
	if _, err := io.ReadAll(ra.Body); err == nil {
		t.Fatal("the replaced stream ended clean")
	}
	for _, r := range []*http.Response{ra, rb, ra2} {
		r.Body.Close()
	}
	waitFor(t, "the conversions to end", func() bool { return running() == 0 })
	if st := s.statusOf(t, descriptors["a"], "a"); st.State == "failed" {
		t.Fatalf("a replaced stream counted as failed: %+v", st)
	}
}

// ffmpeg is confined (PROTOCOL §13): it reads only a loopback capability, with the
// protocol and format whitelists, the probed demuxer named; a playlist or
// concat script as the enclosure is refused, and what it names is never
// fetched.
func TestCompatConfinement(t *testing.T) {
	ffmpegBinary(t)
	t.Parallel()
	args := conversionArgs(&Facts{Demuxer: "mov", Audio: "aac", Channels: 6}, &Profiles[0], 61500, true, "http://127.0.0.1:1/c/x")
	joined := strings.Join(args, " ")
	for _, want := range []string{"-protocol_whitelist http,tcp", "-format_whitelist " + formatWhitelist, "-enable_drefs 0", "-f mov -ss 61.500 -i http://127.0.0.1:1/c/x",
		"-map 0:a:0", "-xerror", "-output_ts_offset 0", "-avoid_negative_ts make_zero", "-ac 2", "-progress pipe:3"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("conversion arguments lack %q: %s", want, joined)
		}
	}
	if args := conversionArgs(&Facts{Demuxer: "ogg", Audio: "opus"}, &Profiles[0], 2000, false, "http://x/c/y"); !slices.Contains(args, "-seekable") ||
		slices.Index(args, "-ss") < slices.Index(args, "-i") {
		t.Fatalf("without ranges the start is an output seek: %v", args)
	}

	elsewhere := newPublisher(t)
	elsewhere.set("/segment.ts", &file{data: pattern(1000), ctype: "video/mp2t"})
	pub := newPublisher(t)
	pub.set("/ep.m3u8", &file{data: []byte("#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\n" + elsewhere.url("/segment.ts") + "\n#EXT-X-ENDLIST\n"),
		ctype: "application/vnd.apple.mpegurl", etag: `"m"`, ranges: true})
	pub.set("/ep.ffconcat", &file{data: []byte("ffconcat version 1.0\nfile " + elsewhere.url("/segment.ts") + "\n"), ctype: "audio/mpeg", etag: `"c"`, ranges: true})
	s := newService(t, serviceOpts{withFFmp: true})
	for _, path := range []string{"/ep.m3u8", "/ep.ffconcat"} {
		d := play(t, s, episode("a", pub.url(path), "audio/mpeg"), PlayRequest{Capabilities: caps("all"), Force: "compat"})
		if d.Mode != "unsupported" || *d.Reason != "unknown_format" {
			t.Fatalf("%s: %+v", path, d)
		}
	}
	if h := elsewhere.seen(); len(h) != 0 {
		t.Fatalf("ffmpeg fetched what a playlist named: %+v", h)
	}
	// The readers listen on loopback only.
	if !isLoopbackAddr(s.reader.ln.Addr()) {
		t.Fatalf("the reader listens on %v", s.reader.ln.Addr())
	}
}

// The startup check covers the whole matrix (PROTOCOL §13); without ffmpeg the
// compatibility path reports unsupported, with the reason, and the relay
// still works.
func TestVerifyMatrix(t *testing.T) {
	ff := ffmpegBinary(t)
	m := checkedMatrix(t, ff).m
	if m.State != "ready" || len(m.Problems) != 0 {
		t.Fatalf("check %s: %s %+v", m.State, m.Reason, m.Problems)
	}
	for _, src := range matrixSources {
		if got := m.Verified[src.key]; len(got) != len(Profiles) {
			t.Errorf("%s converts to %v", src.name, got)
		}
	}
	s, err := New(Config{FFmpeg: "/nonexistent/ffmpeg", SpoolDir: t.TempDir(), Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Verify(context.Background())
	if st := s.MatrixStatus(); st.State != "unavailable" || !strings.Contains(st.Reason, "ffmpeg") || st.Usable {
		t.Fatalf("without ffmpeg: %+v", st)
	}
	data := toneFile(t, "ogg-opus")
	pub := newPublisher(t)
	pub.set("/ep.opus", &file{data: data, ctype: "audio/ogg", etag: etagOf(data), ranges: true})
	s.cfg.Fetcher.AllowLoopback(func(string) bool { return true })
	d := play(t, s, episode("a", pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("webkit-no-ogg")})
	if d.Mode != "unsupported" || *d.Reason != "no_ffmpeg" {
		t.Fatalf("without ffmpeg: %+v", d)
	}
	if d := play(t, s, episode("a", pub.url("/ep.opus"), "audio/ogg"), PlayRequest{Capabilities: caps("all")}); d.Mode != "relay" {
		t.Fatalf("the relay without ffmpeg: %+v", d)
	}
}

// A new server's data directory has no spool folder yet: the startup check
// creates it and the compatibility path is usable on the first run.
func TestVerifyOnANewDataDirectory(t *testing.T) {
	ff := ffmpegBinary(t)
	s, err := New(Config{FFmpeg: ff, SpoolDir: filepath.Join(t.TempDir(), "data", "media-spool"), Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Verify(context.Background())
	if st := s.MatrixStatus(); !st.Usable {
		t.Fatalf("a new data directory: %+v", st)
	}
}
