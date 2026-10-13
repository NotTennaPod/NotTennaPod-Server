package refresh

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

const (
	minute = int64(60_000)
	hour   = 60 * minute
	day    = 24 * hour
)

// ---- a clock tests move by hand

type testClock struct {
	mu      sync.Mutex
	now     time.Time
	mono    time.Duration // the monotonic time: a sleep moves only the wall clock
	waiters []clockWaiter
}

type clockWaiter struct {
	at time.Time
	ch chan time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) ms() int64 { return c.Now().UnixMilli() }

func (c *testClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, clockWaiter{c.now.Add(d), ch})
	return ch
}

func (c *testClock) Elapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

// Advance moves the wall and the monotonic clock together, as time passes.
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.mono += d
	c.mu.Unlock()
	c.moveWall(d)
}

// Sleep moves only the wall clock: a suspended Mac, or the clock set by hand.
func (c *testClock) Sleep(d time.Duration) { c.moveWall(d) }

func (c *testClock) moveWall(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(c.now) {
			w.ch <- c.now
		} else {
			kept = append(kept, w)
		}
	}
	c.waiters = kept
}

// fixedRandom draws the given values in turn, then repeats the last.
func fixedRandom(values ...float64) func() float64 {
	var mu sync.Mutex
	i := 0
	return func() float64 {
		mu.Lock()
		defer mu.Unlock()
		v := values[min(i, len(values)-1)]
		i++
		return v
	}
}

// testFetcher is the production fetcher allowed to reach the tests' servers on
// 127.0.0.1.
func testFetcher() *feed.Fetcher {
	f := feed.NewFetcher()
	f.AllowLoopback(func(host string) bool { return host == "127.0.0.1" })
	return f
}

// ---- an account with one device

type env struct {
	t         *testing.T
	svc       *protocol.Service
	account   string
	namespace string
	clock     *testClock
	inst      storage.Installation
	seq       int64 // install_seq
	author    int64
	last      string
}

var idMu sync.Mutex
var idCounter int

func newID() string {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", idCounter)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, clock: &testClock{now: time.UnixMilli(1_700_000_000_000)}}
	err = store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		e.account, e.namespace = a.ID, a.Namespace
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewRegistry()
	ops.Register(reg)
	views, err := protocol.NewViewRegistry(filepath.Join(store.Dir(), "views"), nil)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = &protocol.Service{Store: store, Registry: reg, Views: views, Box: box, Now: e.clock.Now}
	e.inst = storage.Installation{InstallID: newID(), InstanceNonce: newID(), Name: "phone", Platform: "ios", AppVersion: "1",
		Capabilities: `["favorites","history","inbox","queue","statistics"]`}
	if err := store.Write(ctx, func(tx *storage.WriteTx) error { return tx.Account(e.account).InsertInstallation(e.inst) }); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) refresher(f Fetcher, cfg Config) *Refresher {
	cfg.Fetcher, cfg.Clock = f, e.clock
	if cfg.Random == nil {
		cfg.Random = fixedRandom(0.5) // no jitter: exact times
	}
	return New(e.svc, cfg)
}

func (e *env) fact(typ string, targets []any, payload map[string]any) map[string]any {
	e.author++
	op := newID()
	b, _ := contract.Canonical(payload)
	p, _ := contract.DecodeJSON(b)
	f := map[string]any{"op_id": op, "author_id": e.inst.InstallID, "author_seq": e.author, "dependencies": []any{},
		"type": typ, "type_version": int64(1), "client_time": e.clock.ms(), "targets": targets, "payload": p}
	if e.last != "" {
		f["previous_op_id"] = e.last
	}
	e.last = op
	return f
}

func (e *env) head() int64 {
	var seq int64
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(e.account).Head()
		seq = h.Seq
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return seq
}

// push sends one fact (with secrets) and expects it applied.
func (e *env) push(f map[string]any, secrets ...protocol.Secret) {
	e.t.Helper()
	a, err := protocol.ParseFact(f)
	if err != nil {
		e.t.Fatal(err)
	}
	var h contract.Cursor
	e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		hd, err := r.Account(e.account).Head()
		h = contract.Cursor{LogID: hd.LogID, Epoch: hd.Epoch, Seq: hd.Seq, Digest: hd.Digest}
		return err
	})
	acked := e.seq
	e.seq++
	inst := e.inst
	resp, err := e.svc.Push(context.Background(), protocol.PushRequest{AccountID: e.account, Cursor: h, InstallID: inst.InstallID,
		AckedInstallSeq: acked, Operations: []protocol.PushOp{{InstallSeq: e.seq, Fact: a}}, Secrets: secrets},
		func(tx *storage.WriteTx) (*storage.Installation, error) {
			i, err := tx.Account(e.account).Installation(inst.InstallID)
			return &i, err
		})
	if err != nil {
		e.t.Fatal(err)
	}
	if o, _ := resp.Results[0].Receipt["outcome"].(string); o != "applied" {
		e.t.Fatalf("%s: %+v", f["type"], resp.Results[0])
	}
}

// subscribe subscribes to a feed URL, with credentials when secret is set.
func (e *env) subscribe(feedURL string, secret *protocol.Secret) (podcast, generation string) {
	e.t.Helper()
	payload := map[string]any{"feed_url": feedURL}
	var secrets []protocol.Secret
	if secret != nil {
		payload["credentials_secret"] = secret.ID
		secrets = append(secrets, *secret)
	}
	f := e.fact("podcast.subscribe", []any{}, payload)
	e.push(f, secrets...)
	p := contract.PodcastID(e.namespace, f["op_id"].(string))
	return p, contract.GenerationID(e.namespace, p, f["op_id"].(string))
}

func podTarget(p, g string) []any {
	return []any{map[string]any{"entity": "podcast", "id": p, "generation": g}}
}

func (e *env) records() map[string]string {
	out := map[string]string{}
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		rows, err := r.Account(e.account).Records(nil)
		for _, x := range rows {
			out[x.Entity+"/"+x.EntityID+"/"+x.FieldGroup] = x.Value
		}
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}

func (e *env) episode(podcast, title string) string {
	id, err := contract.EpisodeID(e.namespace, podcast, contract.KindRSSGUID, "guid-"+title)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *env) episodeRecord(podcast, title, group string) string {
	return e.records()["episode/"+e.episode(podcast, title)+"/"+group]
}

func (e *env) podcastRecord(podcast, group string) string {
	return e.records()["podcast/"+podcast+"/"+group]
}

// putRow writes a podcast's schedule row as a stopped server would have left it.
func (e *env) putRow(row storage.RefreshRow) {
	e.t.Helper()
	err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
		return c.Account.PutRefreshSchedule(row)
	})
	if err != nil {
		e.t.Fatal(err)
	}
}

// row is a podcast's schedule row.
func (e *env) row(podcast string) storage.RefreshRow {
	e.t.Helper()
	var row storage.RefreshRow
	var ok bool
	err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
		var err error
		row, ok, err = c.Account.RefreshSchedule(podcast)
		return err
	})
	if err != nil || !ok {
		e.t.Fatalf("schedule row of %s: %v %v", podcast, ok, err)
	}
	return row
}

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	v, err := contract.DecodeJSON([]byte(s))
	if err != nil {
		t.Fatalf("%q: %v", s, err)
	}
	m, _ := v.(map[string]any)
	return m
}

// num reads an integer field of a decoded record (nil → -1).
func num(m map[string]any, k string) int64 {
	if m[k] == nil {
		return -1
	}
	var n int64
	fmt.Sscan(fmt.Sprint(m[k]), &n)
	return n
}

// ---- runs

// runDue runs passes until nothing is due at the clock's time and no fetch is
// running (the loop's work, without its timers and sleep detection).
func runDue(t *testing.T, r *Refresher) {
	t.Helper()
	for {
		res, err := r.pass(context.Background(), false, false)
		if err != nil {
			t.Fatal(err)
		}
		r.mu.Lock()
		n := len(r.running)
		r.mu.Unlock()
		if n == 0 && res.started == 0 && res.skipped == 0 {
			return
		}
		if n > 0 {
			<-r.wake
		}
	}
}

// ---- a feed server

// item is a feed item. Its GUID is "guid-" and its title up to any " (", so
// a retitled item ("One (fixed)") keeps its identity.
type item struct {
	title    string
	pub      int64
	playable bool
}

type redirect struct {
	status int
	to     string
}

type request struct {
	path          string
	ifNoneMatch   string
	authorization string
}

type feedServer struct {
	t   *testing.T
	srv *httptest.Server

	mu          sync.Mutex
	items       []item
	extra       string // channel XML (itunes:new-feed-url)
	status      int    // non-zero: answer with this status
	retryAfter  string
	user, pass  string // non-empty: Basic credentials required
	redirects   map[string]redirect
	requests    []request
	gate        chan struct{} // non-nil: requests wait for it to close
	arrived     chan string   // non-nil: each request's path, on arrival
	unprotected map[string]bool
}

func newFeedServer(t *testing.T) *feedServer {
	fs := &feedServer{t: t, redirects: map[string]redirect{}, unprotected: map[string]bool{}}
	fs.srv = httptest.NewServer(http.HandlerFunc(fs.serve))
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *feedServer) url(path string) string { return fs.srv.URL + path }

func (fs *feedServer) set(items ...item) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.items = items
}

func (fs *feedServer) with(f func(fs *feedServer)) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f(fs)
}

func (fs *feedServer) body() []byte {
	var b strings.Builder
	b.WriteString(`<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"><channel><title>Show</title>`)
	b.WriteString(fs.extra)
	for _, it := range fs.items {
		guid, _, _ := strings.Cut(it.title, " (")
		fmt.Fprintf(&b, `<item><title>%s</title><guid>guid-%s</guid><pubDate>%s</pubDate>`, it.title, guid,
			time.UnixMilli(it.pub).UTC().Format(time.RFC1123Z))
		if it.playable {
			fmt.Fprintf(&b, `<enclosure url="https://cdn.example.com/%s.mp3" length="20000" type="audio/mpeg"/>`, strings.ReplaceAll(it.title, " ", "-"))
		}
		b.WriteString(`</item>`)
	}
	b.WriteString(`</channel></rss>`)
	return []byte(b.String())
}

func (fs *feedServer) serve(w http.ResponseWriter, r *http.Request) {
	fs.mu.Lock()
	fs.requests = append(fs.requests, request{path: r.URL.Path, ifNoneMatch: r.Header.Get("If-None-Match"), authorization: r.Header.Get("Authorization")})
	gate, arrived := fs.gate, fs.arrived
	fs.mu.Unlock()
	if arrived != nil {
		arrived <- r.URL.Path
	}
	if gate != nil {
		<-gate
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if rd, ok := fs.redirects[r.URL.Path]; ok {
		http.Redirect(w, r, rd.to, rd.status)
		return
	}
	if fs.status != 0 {
		if fs.retryAfter != "" {
			w.Header().Set("Retry-After", fs.retryAfter)
		}
		w.WriteHeader(fs.status)
		return
	}
	if fs.user != "" && !fs.unprotected[r.URL.Path] {
		if u, p, ok := r.BasicAuth(); !ok || u != fs.user || p != fs.pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="feed"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	body := fs.body()
	sum := sha256.Sum256(body)
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/rss+xml")
	w.Write(body)
}

func (fs *feedServer) count() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.requests)
}

// seen returns the requests so far and forgets them.
func (fs *feedServer) seen() []request {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := fs.requests
	fs.requests = nil
	return out
}

func paths(rs []request) string {
	var out []string
	for _, r := range rs {
		out = append(out, r.path)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}
