package media

// Shared fixtures for PROTOCOL §13 tests (PROTOCOL §13): a fixture
// publisher whose behaviour each test sets (ranges or not, weak or strong
// validators, redirects, credentials, hostile types, slow, stalled and
// broken bodies), tone files generated with ffmpeg's lavfi sources (a
// different frequency every second marks the media time), and helpers that
// drive the service as the API and a browser do. Nothing binary is
// committed: every media file is made by the tests.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

var fixtureDir string

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ntp-media-test-")
	if err != nil {
		panic(err)
	}
	fixtureDir = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// ffmpegBinary finds ffmpeg for the tests that need it. Without it they
// skip, unless NTP_REQUIRE_FFMPEG is set (the acceptance target, PROTOCOL §13).
func ffmpegBinary(t testing.TB) string {
	t.Helper()
	for _, c := range []string{"ffmpeg", "/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg", "/usr/bin/ffmpeg"} {
		if p, err := exec.LookPath(c); err == nil {
			if _, err := exec.LookPath(filepath.Join(filepath.Dir(p), "ffprobe")); err == nil {
				return p
			}
		}
	}
	if os.Getenv("NTP_REQUIRE_FFMPEG") != "" {
		t.Fatal("ffmpeg and ffprobe are required (NTP_REQUIRE_FFMPEG) but not installed")
	}
	t.Skip("ffmpeg is not installed: the compatibility path is not tested")
	return ""
}

// ---- the fixture publisher

type file struct {
	data         []byte
	ctype        string
	etag         string // as sent, "W/…" for a weak one; "" none
	lastModified string
	ranges       bool   // honours Range (and If-Range)
	noLength     bool   // chunked, without Content-Length
	gzip         bool   // sends it gzip-encoded whatever was asked
	user, pass   string // Basic credentials it requires
	redirect     string // answers 302 to this URL
	extra        map[string]string
	truncateAt   int64         // closes the connection after this many body bytes (0: never)
	stallAt      int64         // stops sending after this many body bytes until released or the client goes
	release      chan struct{} // ends a stall
	chunkDelay   time.Duration // between 16 KiB chunks
}

type hit struct {
	method, path, rng, ifRange, auth, cookie, ua, encoding, origin string
}

type publisher struct {
	t   testing.TB
	srv *httptest.Server

	mu       sync.Mutex
	files    map[string]*file
	requests []hit
	active   int // bodies being sent
}

func newPublisher(t testing.TB) *publisher {
	p := &publisher{t: t, files: map[string]*file{}}
	p.srv = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *publisher) url(path string) string { return p.srv.URL + path }

func (p *publisher) set(path string, f *file) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.files[path] = f
}

func (p *publisher) with(path string, fn func(f *file)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(p.files[path])
}

func (p *publisher) seen() []hit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]hit(nil), p.requests...)
}

func (p *publisher) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = nil
}

func (p *publisher) serve(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	p.requests = append(p.requests, hit{method: r.Method, path: r.URL.Path, rng: r.Header.Get("Range"), ifRange: r.Header.Get("If-Range"),
		auth: r.Header.Get("Authorization"), cookie: r.Header.Get("Cookie"), ua: r.Header.Get("User-Agent"),
		encoding: r.Header.Get("Accept-Encoding"), origin: r.Header.Get("Origin")})
	f := p.files[r.URL.RequestURI()] // a file at a path with a query, else at the path
	if f == nil {
		f = p.files[r.URL.Path]
	}
	var c file
	if f != nil {
		c = *f
	}
	p.mu.Unlock()
	if f == nil {
		http.NotFound(w, r)
		return
	}
	if c.redirect != "" {
		http.Redirect(w, r, c.redirect, http.StatusFound)
		return
	}
	if c.user != "" {
		u, pw, ok := r.BasicAuth()
		if !ok || u != c.user || pw != c.pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="feed"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	h := w.Header()
	for k, v := range c.extra {
		h.Set(k, v)
	}
	if c.ctype != "" {
		h.Set("Content-Type", c.ctype)
	}
	if c.etag != "" {
		h.Set("ETag", c.etag)
	}
	if c.lastModified != "" {
		h.Set("Last-Modified", c.lastModified)
	}
	data := c.data
	if c.gzip {
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		zw.Write(data)
		zw.Close()
		data = b.Bytes()
		h.Set("Content-Encoding", "gzip")
	}
	status, start := http.StatusOK, int64(0)
	body := data
	if c.ranges && !c.gzip {
		h.Set("Accept-Ranges", "bytes")
		if rng := r.Header.Get("Range"); rng != "" && (r.Header.Get("If-Range") == "" || r.Header.Get("If-Range") == c.etag) {
			a, b, ok := parseOneRange(rng)
			total := int64(len(data))
			if ok && a >= total {
				h.Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if ok {
				if b < 0 || b >= total {
					b = total - 1
				}
				status, start, body = http.StatusPartialContent, a, data[a:b+1]
				h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, total))
			}
		}
	}
	if !c.noLength {
		h.Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	p.mu.Lock()
	p.active++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	}()
	_ = start
	rc := http.NewResponseController(w)
	var sent int64
	for len(body) > 0 {
		n := min(len(body), 16<<10)
		if c.truncateAt > 0 && sent+int64(n) > c.truncateAt {
			n = int(c.truncateAt - sent)
		}
		if c.stallAt > 0 && sent+int64(n) > c.stallAt {
			n = int(c.stallAt - sent)
		}
		if n > 0 {
			if _, err := w.Write(body[:n]); err != nil {
				return
			}
			rc.Flush()
			body, sent = body[n:], sent+int64(n)
		}
		if c.truncateAt > 0 && sent >= c.truncateAt {
			panic(http.ErrAbortHandler)
		}
		if c.stallAt > 0 && sent >= c.stallAt {
			c.stallAt = 0
			select {
			case <-c.release:
			case <-r.Context().Done():
				return
			}
		}
		if c.chunkDelay > 0 {
			select {
			case <-time.After(c.chunkDelay):
			case <-r.Context().Done():
				return
			}
		}
	}
}

// ---- the service under test

type serviceOpts struct {
	limits   func(*Limits)
	noTools  bool
	refuse   bool // the fetcher refuses loopback, as in production
	withFFmp bool
}

func newService(t testing.TB, o serviceOpts) *Service {
	t.Helper()
	f := feed.NewFetcher()
	if !o.refuse {
		f.AllowLoopback(func(string) bool { return true })
	}
	l := DefaultLimits()
	if o.limits != nil {
		o.limits(&l)
	}
	cfg := Config{Fetcher: f, SpoolDir: filepath.Join(t.TempDir(), "spool"), Limits: l, NoTools: o.noTools || !o.withFFmp, Logger: quiet}
	if o.withFFmp && !o.noTools {
		cfg.FFmpeg = ffmpegBinary(t)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); s.Wait() })
	if cfg.NoTools {
		s.Verify(context.Background())
	} else {
		shared := checkedMatrix(t, cfg.FFmpeg)
		s.mu.Lock()
		s.tl, s.matrix = shared.tl, shared.m
		s.mu.Unlock()
		close(s.verified)
	}
	return s
}

// checkedMatrix runs the startup check once for the package.
var matrixOnce struct {
	sync.Once
	tl *toolset
	m  *Matrix
}

type shared struct {
	tl *toolset
	m  *Matrix
}

func checkedMatrix(t testing.TB, ffmpeg string) shared {
	matrixOnce.Do(func() {
		dir, _ := os.MkdirTemp(fixtureDir, "check")
		s, err := New(Config{FFmpeg: ffmpeg, SpoolDir: dir, Logger: quiet})
		if err != nil {
			panic(err)
		}
		s.Verify(context.Background())
		matrixOnce.tl, matrixOnce.m = s.tools(), s.matrix
		s.Close()
	})
	if matrixOnce.m == nil || !matrixOnce.m.Usable {
		t.Fatalf("the startup check found no working conversion: %+v", matrixOnce.m)
	}
	return shared{matrixOnce.tl, matrixOnce.m}
}

// playFailure asks for a descriptor that must fail, and returns why.
func playFailure(t testing.TB, s *Service, ep Episode, req PlayRequest) *ErrorDetail {
	t.Helper()
	d, err := s.Play(context.Background(), ep, req)
	if err != nil {
		t.Fatalf("play: %v", err)
	}
	if d.Mode != "failed" || d.Error == nil {
		t.Fatalf("play: %+v, want failed", d)
	}
	return d.Error
}

// play asks for a descriptor as the API would.
func play(t testing.TB, s *Service, ep Episode, req PlayRequest) *Descriptor {
	t.Helper()
	d, err := s.Play(context.Background(), ep, req)
	if err != nil {
		t.Fatalf("play: %v", err)
	}
	return d
}

func episode(account, source, declared string) Episode {
	return Episode{Account: account, Credential: "cred-" + account, ID: "e1", SourceURL: source, DeclaredType: declared}
}

// caps are canPlayType answers; the engine sets mirror Playwright's
// Chromium, Firefox and WebKit (PROTOCOL §13 notes), and
// "safari-no-ogg" a WebKit without Ogg.
func caps(engine string) map[string]string {
	yes := map[string]string{}
	for _, c := range Capabilities {
		yes[c] = "probably"
	}
	switch engine {
	case "webkit-no-ogg":
		for _, c := range Capabilities {
			if strings.HasPrefix(c, "audio/ogg") || strings.HasPrefix(c, "audio/webm") || strings.HasPrefix(c, "video/webm") {
				yes[c] = ""
			}
		}
	case "mp3-and-aac":
		for _, c := range Capabilities {
			if c != "audio/mpeg" && c != "audio/aac" {
				yes[c] = ""
			}
		}
	case "nothing":
		for _, c := range Capabilities {
			yes[c] = ""
		}
	}
	return yes
}

// browserGet sends a media request the way an <audio> element does, through
// an httptest server that runs Serve for the token.
type browser struct {
	t   testing.TB
	s   *Service
	srv *httptest.Server
}

func newBrowser(t testing.TB, s *Service, account string) *browser {
	b := &browser{t: t, s: s}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/v1/media/s/")
		k, ok := s.Lookup(id, account, "cred-"+account)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if e := s.Serve(w, r, k); e != nil {
			w.Header().Set("Content-Type", "application/json")
			retryHeader(w.Header(), e)
			w.WriteHeader(e.Status)
			json.NewEncoder(w).Encode(e.Body())
		}
	}))
	t.Cleanup(b.srv.Close)
	return b
}

type reply struct {
	status int
	header http.Header
	body   []byte
	err    error // the body ended in an error (an aborted response)
}

func (b *browser) do(method, url string, header map[string]string) reply {
	b.t.Helper()
	req, _ := http.NewRequest(method, b.srv.URL+url, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return reply{status: resp.StatusCode, header: resp.Header, body: body, err: err}
}

func (b *browser) get(url, rng string) reply {
	h := map[string]string{}
	if rng != "" {
		h["Range"] = rng
	}
	return b.do(http.MethodGet, url, h)
}

func (s *Service) statusOf(t testing.TB, d *Descriptor, account string) Status {
	t.Helper()
	k, ok := s.Lookup(*d.Token, account, "cred-"+account)
	if !ok {
		t.Fatalf("token %s unknown", *d.Token)
	}
	return s.Status(context.Background(), k)
}

// waitFor polls a condition (no fixed sleeps).
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---- media made by the tests

// pattern is n bytes that differ by position, so spliced or shifted bytes
// show.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func etagOf(b []byte) string {
	h := sha256.Sum256(b)
	return `"` + hex.EncodeToString(h[:8]) + `"`
}

// toneFormats are the formats the compatibility tests cover (PROTOCOL §13).
var toneFormats = map[string][]string{
	"ogg-opus-long": {"-c:a", "libopus", "-b:a", "24k", "-f", "ogg"}, // ten minutes
	"m4a-long":      {"-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", "-f", "mp4"},
	"ogg-vorbis":    {"-c:a", "vorbis", "-strict", "experimental", "-ac", "2", "-f", "ogg"},
	"ogg-opus":      {"-c:a", "libopus", "-b:a", "64k", "-f", "ogg"},
	"mp3":           {"-c:a", "libmp3lame", "-b:a", "96k", "-f", "mp3"},
	"m4a":           {"-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", "-f", "mp4"},
	"m4a-tail":      {"-c:a", "aac", "-b:a", "96k", "-f", "mp4"},
	"flac":          {"-c:a", "flac", "-f", "flac"},
	"webm":          {"-c:a", "libopus", "-b:a", "64k", "-f", "webm"},
}

var (
	tonesMu sync.Mutex // one generation at a time: parallel tests share the files
	tones   = map[string][]byte{}
)

// toneFile is a 20-second file whose second k is a tone of 300 + 50k Hz
// (the "-long" formats: ten minutes of one tone).
func toneFile(t testing.TB, format string) []byte {
	t.Helper()
	ff := ffmpegBinary(t)
	tonesMu.Lock()
	defer tonesMu.Unlock()
	if b, ok := tones[format]; ok {
		return b
	}
	out := filepath.Join(fixtureDir, format+".tone")
	src := "aevalsrc=sin(2*PI*(300+50*floor(t))*t):s=48000:d=20"
	if strings.HasSuffix(format, "-long") {
		src = "sine=frequency=440:sample_rate=48000:duration=600"
	}
	args := append([]string{"-nostdin", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", src}, toneFormats[format]...)
	args = append(args, "-y", out)
	if out, err := exec.Command(ff, args...).CombinedOutput(); err != nil {
		t.Fatalf("making the %s tone file: %v %s", format, err, out)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	tones[format] = b
	return b
}

// frequencies decodes media (of the given demuxer) and measures its
// frequency in quarter-second windows, by zero crossings.
func frequencies(t testing.TB, data []byte, demuxer string) []float64 {
	t.Helper()
	cmd := exec.Command(ffmpegBinary(t), "-nostdin", "-hide_banner", "-loglevel", "error", "-f", demuxer, "-i", "pipe:0",
		"-ac", "1", "-ar", "48000", "-f", "s16le", "pipe:1")
	cmd.Stdin = bytes.NewReader(data)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("decoding the output: %v %s", err, stderr.String())
	}
	pcm := out.Bytes()
	n := len(pcm) / 2
	sample := func(i int) int16 { return int16(uint16(pcm[2*i]) | uint16(pcm[2*i+1])<<8) }
	const win = 12000
	var f []float64
	for w := 0; w+win <= n; w += win {
		zc := 0
		for i := w + 1; i < w+win; i++ {
			if (sample(i-1) < 0) != (sample(i) < 0) {
				zc++
			}
		}
		f = append(f, float64(zc)/2/(float64(win)/48000))
	}
	return f
}

// toneAt is the tone of original second k.
func toneAt(k int) float64 { return float64(300 + 50*k) }

// checkTimeline checks decoded output that should start at startMS of the
// tone file: the windows well inside each second carry that second's tone.
func checkTimeline(t testing.TB, freqs []float64, startMS int64, what string) {
	t.Helper()
	checked := 0
	for i, f := range freqs {
		if i == 0 {
			continue // the encoder's priming (AAC's first 2112 samples) is in the first window
		}
		mid := float64(startMS)/1000 + float64(i)*0.25 + 0.125 // the window's middle on the original timeline
		frac := mid - math.Floor(mid)
		if frac < 0.2 || frac > 0.8 {
			continue // near a tone change
		}
		want := toneAt(int(math.Floor(mid)))
		if math.Abs(f-want) > 20 {
			t.Fatalf("%s: at %.2f s of the original the tone is %.0f Hz, want %.0f (windows %v)", what, mid, f, want, freqs)
		}
		checked++
	}
	if checked < 4 {
		t.Fatalf("%s: only %d windows checked (%d decoded)", what, checked, len(freqs))
	}
}

// listener addresses on loopback are what the readers bind.
func isLoopbackAddr(a net.Addr) bool {
	tcp, ok := a.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}
