package media

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
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

// deadlineRecorder records a response's write deadlines; a write blocks
// until a deadline at or before now is set, as a socket write to a reader
// that stopped reading does.
type deadlineRecorder struct {
	h       http.Header
	block   bool
	entered chan struct{}
	mu      sync.Mutex
	set     []time.Time
	expired chan struct{}
	once    sync.Once
	wrote   bytes.Buffer
}

func newDeadlineRecorder(block bool) *deadlineRecorder {
	return &deadlineRecorder{h: http.Header{}, block: block, entered: make(chan struct{}, 1), expired: make(chan struct{})}
}

func (w *deadlineRecorder) Header() http.Header { return w.h }
func (w *deadlineRecorder) WriteHeader(int)     {}
func (w *deadlineRecorder) Write(p []byte) (int, error) {
	if !w.block {
		return w.wrote.Write(p)
	}
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.expired
	return 0, io.ErrClosedPipe
}

func (w *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	w.set = append(w.set, t)
	w.mu.Unlock()
	if !t.After(time.Now()) {
		w.once.Do(func() { close(w.expired) })
	}
	return nil
}

func (w *deadlineRecorder) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.set)
}

// A stream renews its write deadline before every write,
// so a long healthy stream is never cut and a stalled reader frees it.
func TestStreamsRenewTheirWriteDeadline(t *testing.T) {
	s := &Service{cfg: Config{Limits: DefaultLimits()}}
	w := newDeadlineRecorder(false)
	body := bytes.Repeat([]byte("x"), 3*64<<10+1)
	outcome, e := s.copyOut(w, httptest.NewRequest(http.MethodGet, "/", nil), bytes.NewReader(body), int64(len(body)))
	if outcome != outComplete || e != nil || !bytes.Equal(w.wrote.Bytes(), body) {
		t.Fatalf("copy: %d %v", outcome, e)
	}
	if n := w.count(); n < 4 {
		t.Fatalf("%d write deadlines for 4 writes", n)
	}
}

// An asset frees its slot once its fetch ends, so a browser that stops reading
// (or goes away) never blocks the account's next asset.
func TestAssetSlotIsFreedBeforeWriting(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("image fixture"))
	}))
	defer upstream.Close()
	f := feed.NewFetcher()
	f.AllowLoopback(func(h string) bool { return h == "127.0.0.1" })
	limits := DefaultLimits()
	limits.AssetsPerAccount = 1
	s, err := New(Config{Fetcher: f, NoTools: true, SpoolDir: t.TempDir(), Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer s.cancel()
	slow := newDeadlineRecorder(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Asset(slow, httptest.NewRequest(http.MethodGet, "/a", nil).WithContext(ctx), "account", AssetImage, upstream.URL, "", nil)
		close(done)
	}()
	<-slow.entered
	next, nextCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer nextCancel()
	rec := httptest.NewRecorder()
	if e := s.Asset(rec, httptest.NewRequest(http.MethodGet, "/b", nil).WithContext(next), "account", AssetImage, upstream.URL, "", nil); e != nil || rec.Code != http.StatusOK {
		t.Fatalf("the next asset waited for the slow reader's slot: %v %d", e, rec.Code)
	}
	cancel() // the slow request goes away: its blocked write is interrupted
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled asset's write was not interrupted")
	}
}

// A lookup checks idle expiry before renewing, so a token
// past it is never revived before the next sweep.
func TestExpiredTokensAreNotRevived(t *testing.T) {
	s := newService(t, serviceOpts{})
	now := time.Now()
	s.cfg.Now = func() time.Time { return now }
	tok := &token{id: newTokenID(), account: "a", credential: "cred-a"}
	s.tokens.put(tok, now)
	if _, ok := s.Lookup(tok.id, "a", "cred-a"); !ok {
		t.Fatal("a fresh token was not found")
	}
	now = now.Add(s.cfg.Limits.TokenIdle + time.Second)
	if _, ok := s.Lookup(tok.id, "a", "cred-a"); ok {
		t.Fatal("an expired token was accepted and renewed")
	}
	s.tokens.sweep(now)
	if _, ok := s.Lookup(tok.id, "a", "cred-a"); ok {
		t.Fatal("the expired token came back")
	}
}

// A failed copy removes its partial file and frees its
// place at once, and failed playback is not protected from eviction: two
// accounts' failed copies never block a third account's playback.
func TestFailedCopiesFreeTheirPlace(t *testing.T) {
	p := newPublisher(t)
	p.set("/bad", &file{data: pattern(256 << 10), ctype: "audio/mpeg", noLength: true})
	p.set("/good", &file{data: pattern(100), ctype: "audio/mpeg", noLength: true})
	s := newService(t, serviceOpts{limits: func(l *Limits) { l.SpoolMax = 128 << 10 }})
	for _, a := range []string{"a", "b"} {
		d := play(t, s, episode(a, p.url("/bad"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
		k, ok := s.Lookup(*d.Token, a, "cred-"+a)
		if !ok {
			t.Fatal("token missing")
		}
		if err := k.t.spool.waitDone(context.Background()); err == nil {
			t.Fatal("the copy did not exceed its cap")
		}
		if r := newBrowser(t, s, a).get(*d.URL, ""); r.status != http.StatusBadGateway {
			t.Fatalf("a failed copy's response: %d", r.status)
		}
		if st := s.statusOf(t, d, a); st.State != "failed" {
			t.Fatalf("the failure was not recorded: %+v", st)
		}
		if _, err := os.Stat(k.t.spool.path); err == nil {
			t.Error("a failed copy kept its partial file")
		}
	}
	d, err := s.Play(context.Background(), episode("c", p.url("/good"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	if err != nil || d.Mode != "relay" {
		t.Fatalf("after two failed copies, another account's playback: %+v %v", d, err)
	}
}

// A copy is deleted only when no token holds it and no
// lease uses it: evicting the token of a copy a conversion still reads
// keeps the copy until the reading ends.
func TestALeasedCopyOutlivesItsToken(t *testing.T) {
	p := newPublisher(t)
	p.set("/copy", &file{data: pattern(256 << 10), ctype: "audio/mpeg", noLength: true})
	p.set("/plain", &file{data: pattern(100), ctype: "audio/mpeg"})
	s := newService(t, serviceOpts{limits: func(l *Limits) { l.TokensPerAccount = 32 }})
	d := play(t, s, episode("a", p.url("/copy"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	k, _ := s.Lookup(*d.Token, "a", "cred-a")
	f := k.t.spool
	if err := f.waitDone(context.Background()); err != nil {
		t.Fatal(err)
	}
	release := f.lease(s.now) // a conversion between its range reads
	for i := 0; i < 32; i++ {
		play(t, s, episode("a", p.url("/plain"), "audio/mpeg"), PlayRequest{Capabilities: caps("all")})
	}
	if _, ok := s.Lookup(*d.Token, "a", "cred-a"); ok {
		t.Fatal("the token was not evicted")
	}
	body, err := f.reader(context.Background(), 100, 199, s.now)
	if err != nil {
		t.Fatalf("the leased copy went with its token: %v", err)
	}
	body.Close()
	release()
	if _, err := os.Stat(f.path); err == nil {
		t.Fatal("the copy stayed after its last lease and token ended")
	}
}

// A failed probe logs a fixed category, never ffprobe's
// words, which can quote the media's content (a URL with credentials).
func TestProbeLogsKeepNoMediaContent(t *testing.T) {
	s := newService(t, serviceOpts{withFFmp: true})
	var logs bytes.Buffer
	s.cfg.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	secret := "https://user:private-password@publisher.invalid/private?token=private-token"
	doc := append([]byte{0x42, 0x82, byte(0x80 + len(secret))}, []byte(secret)...)
	data := append([]byte{0x1a, 0x45, 0xdf, 0xa3, byte(0x80 + len(doc))}, doc...)
	path := filepath.Join(t.TempDir(), "malformed.mkv")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, u, revoke, err := s.reader.grant(context.Background(), fileSource{path}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer revoke()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.ffprobe(ctx, u, true)
	if strings.Contains(logs.String(), "private-password") || strings.Contains(logs.String(), "private-token") {
		t.Fatalf("media content reached the log: %s", logs.String())
	}
}

// The reader's capability never appears in ffmpeg's or
// ffprobe's command line, which other local users can read (ps, /proc): the
// tools read a URL without any secret through the reader as their proxy,
// with the capability as the proxy's credentials in their environment. A
// local client that knows only what the command line shows gets 407.
func TestReaderCapabilityIsNotInTheCommandLine(t *testing.T) {
	s := newService(t, serviceOpts{withFFmp: true})
	path := filepath.Join(t.TempDir(), "private.opus")
	if err := os.WriteFile(path, toneFile(t, "ogg-opus-long"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, in, revoke, err := s.reader.grant(ctx, fileSource{path}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer revoke()
	c, err := s.startConversion(ctx, in.env, conversionArgs(&Facts{Demuxer: "ogg", Audio: "opus", Channels: 1}, &Profiles[0], 0, true, in.url))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.kill(); c.wait() }()
	args, err := exec.Command("/bin/ps", "-ww", "-p", strconv.Itoa(c.cmd.Process.Pid), "-o", "args=").Output()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(args, []byte(g.id)) || !bytes.Contains(args, []byte(readerURL)) {
		t.Fatalf("the command line: %s", args)
	}
	// What the command line shows reaches nothing.
	proxy, _ := neturl.Parse(strings.TrimPrefix(in.env[0], "http_proxy="))
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(&neturl.URL{Scheme: "http", Host: proxy.Host})}}
	defer client.CloseIdleConnections()
	res, err := client.Get(readerURL)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("a client without the capability: %d", res.StatusCode)
	}
	// The conversion itself reads through the reader.
	buf := make([]byte, 4096)
	if n, _ := c.read(buf, 10*time.Second); n == 0 {
		t.Fatal("the conversion read nothing through the reader")
	}
}
