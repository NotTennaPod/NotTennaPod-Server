package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Authenticated assets are served private, no-cache with an ETag, so a browser
// revalidates every reuse. Only an authorized revalidation answers 304; another
// account, or a session that signed out, gets its own answer and never the
// cached copy.
func TestAssetsAreRevalidatedForEachAccount(t *testing.T) {
	m := newMediaEnv(t)
	podcast, _, ep := m.subscribe(m.pub.srv.URL+"/feed.xml", nil)
	m.createAccount(m.session, "other", "other-password-1", false)
	other := m.env.session("other", "other-password-1")
	m.registerWeb(other, newID(), newID()).must(http.StatusOK)
	fetches := func() int {
		paths, _ := m.pub.requests()
		n := 0
		for _, p := range paths {
			if p == "/cover.png" || p == "/chapters.json" {
				n++
			}
		}
		return n
	}
	for _, path := range []string{"/v1/media/podcasts/" + podcast + "/image", "/v1/media/" + ep + "/image", "/v1/media/" + ep + "/chapters"} {
		first := m.raw(http.MethodGet, path, cookieOnly(m.session))
		etag := first.Header().Get("ETag")
		if first.Code != http.StatusOK || first.Header().Get("Cache-Control") != "private, no-cache" || !strings.HasPrefix(etag, `"`) {
			t.Fatalf("%s: %d %v", path, first.Code, first.Header())
		}
		before := fetches()
		again := m.raw(http.MethodGet, path, cookieOnly(m.session), hdr("If-None-Match", etag))
		if again.Code != http.StatusNotModified || again.Body.Len() != 0 || again.Header().Get("ETag") != etag ||
			again.Header().Get("Cache-Control") != "private, no-cache" || fetches() != before {
			t.Fatalf("%s: an authorized revalidation: %d %v", path, again.Code, again.Header())
		}
		if stale := m.raw(http.MethodGet, path, cookieOnly(m.session), hdr("If-None-Match", `"another"`)); stale.Code != http.StatusOK ||
			!bytes.Equal(stale.Body.Bytes(), first.Body.Bytes()) {
			t.Fatalf("%s: another validator: %d", path, stale.Code)
		}
		if rec := m.raw(http.MethodGet, path, cookieOnly(other), hdr("If-None-Match", etag)); rec.Code != http.StatusNotFound {
			t.Fatalf("%s: another account revalidating: %d %v", path, rec.Code, rec.Header())
		}
	}
	path := "/v1/media/podcasts/" + podcast + "/image"
	etag := m.raw(http.MethodGet, path, cookieOnly(m.session)).Header().Get("ETag")
	m.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(m.session)).must(http.StatusOK)
	if rec := m.raw(http.MethodGet, path, cookieOnly(m.session), hdr("If-None-Match", etag)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("a signed-out session revalidating: %d %v", rec.Code, rec.Header())
	}
}

// afterCredentialRead runs f once, just after authentication has read the
// caller's credential (authentication then asks the clock): the handler's
// own reads start only after f has committed.
func afterCredentialRead(e *env, f func()) {
	var once oneShot
	once.set(f)
	e.srv.cfg.Now = func() time.Time { once.run(); return e.clock.Now() }
}

// A pull, and a snapshot of every mode and page, held after authentication
// while the device is revoked, never serves content written after the
// revocation.
func TestSyncReadsCheckTheCallerAgain(t *testing.T) {
	for _, mode := range []string{"pull", "attach", "upgrade", "recovery", "recovery page"} {
		t.Run(mode, func(t *testing.T) {
			r := newRecoveryEnv(t)
			for i := int64(1); i <= 3; i++ {
				pushed(t, r.p.push(op{seq: i, fact: r.p.noteFact("existing private history")}))
			}
			other := r.e.phoneOf("alice", "alice-password-1")
			path := "/v1/snapshot"
			body := map[string]any{"mode": mode, "install_id": r.p.install, "acked_install_seq": 0}
			switch mode {
			case "pull":
				path = "/v1/pull"
				delete(body, "mode")
				body["cursor"] = r.p.cursor
			case "upgrade":
				body["cursor"] = r.p.cursor
				body["field_groups"] = []string{"metadata"}
			case "recovery":
				body["recovery_id"] = r.rid
			case "recovery page":
				first := r.e.do("POST", path, map[string]any{"mode": "recovery", "recovery_id": r.rid, "install_id": r.p.install,
					"acked_install_seq": 0, "limit": 1}, r.p.cred).must(http.StatusOK)
				body["mode"], body["recovery_id"], body["page"] = "recovery", r.rid, first.str("next")
			}
			raw, _ := json.Marshal(body)
			res := delayed(r.e, "POST", path, string(raw), r.p.cred, func() {
				r.e.revoke(other.cred, r.p.install).must(http.StatusOK)
				pushed(t, other.push(op{seq: 1, fact: other.noteFact("written after the revocation")}))
			})
			res.expect(http.StatusUnauthorized, "unauthorized")
		})
	}
}

// A page token serves only the mode and recovery it was issued for; a
// recovery's page asked as an attach is refused.
func TestSnapshotPageTokensKeepTheirMode(t *testing.T) {
	r := newRecoveryEnv(t)
	for i := int64(1); i <= 3; i++ {
		pushed(t, r.p.push(op{seq: i, fact: r.p.noteFact("private history")}))
	}
	first := r.e.do("POST", "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": r.rid, "install_id": r.p.install,
		"acked_install_seq": 0, "limit": 1}, r.p.cred).must(http.StatusOK)
	page := first.str("next")
	if page == "" {
		t.Fatalf("no second page: %s", first.raw)
	}
	r.e.do("POST", "/v1/snapshot", map[string]any{"mode": "attach", "install_id": r.p.install, "acked_install_seq": 0, "page": page},
		r.p.cred).expect(http.StatusBadRequest, "invalid_request")
	r.e.do("POST", "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": newID(), "install_id": r.p.install,
		"acked_install_seq": 0, "page": page}, r.p.cred).expect(http.StatusBadRequest, "invalid_request")
	r.e.do("POST", "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": r.rid, "install_id": r.p.install,
		"acked_install_seq": 0, "page": page}, r.p.cred).must(http.StatusOK)
}

// A feed secret is read in the transaction that checks the caller, so a
// password reset landing during the request wins.
func TestSecretsCheckTheCallerAgain(t *testing.T) {
	m := newMediaEnv(t)
	secretID := newID()
	m.subscribe(m.pub.srv.URL+"/private/feed.xml", map[string]any{"secret_id": secretID, "username": "listener", "password": "feed-password"})
	m.do(http.MethodGet, "/v1/secrets/"+secretID, nil, m.phone.cred).must(http.StatusOK)
	m.createAccount(m.session, "second", "second-password", true)
	root := m.env.session("second", "second-password")
	afterCredentialRead(m.env, func() { m.resetPassword(root, m.account, "changed-password").must(http.StatusOK) })
	got := m.do(http.MethodGet, "/v1/secrets/"+secretID, nil, m.phone.cred)
	if got.status != http.StatusUnauthorized || strings.Contains(string(got.raw), "feed-password") {
		t.Fatalf("a reset credential read the feed secret: %d %s", got.status, got.raw)
	}
}

// The read API checks the caller in its read transaction, so an account
// disabled during the request reads nothing; a list's later page is checked
// again too.
func TestReadAPIChecksTheCallerAgain(t *testing.T) {
	e := newReadEnv(t)
	e.importInto(e.account, "emulator-parity-library.db")
	e.createAccount(e.session, "second", "second-password", true)
	root := e.env.session("second", "second-password")
	first := e.get("/v1/read/episodes?limit=1").must(http.StatusOK)
	next := first.str("next_page_token")
	if next == "" {
		t.Fatalf("no second page: %s", first.raw)
	}
	for _, path := range []string{"/v1/read/summary", "/v1/read/subscriptions", "/v1/read/episodes", "/v1/read/queue",
		"/v1/read/episodes?limit=1&page_token=" + next} {
		afterCredentialRead(e.env, func() { e.setState(root, e.account, "disabled").must(http.StatusOK) })
		e.get(path).expect(http.StatusForbidden, "forbidden")
		e.setState(root, e.account, "active").must(http.StatusOK)
	}
}

// A descriptor is not issued to a session that ended
// during the request, a media response checks its session first, and a
// sign-out drops the session's tokens at once.
func TestMediaChecksTheSessionAgain(t *testing.T) {
	m := newMediaEnv(t)
	_, _, ep := m.subscribe(m.pub.srv.URL+"/feed.xml", nil)
	play := `{"capabilities":{"audio/mpeg":"probably"},"start_ms":0}`
	delayed(m.env, http.MethodPost, "/v1/media/"+ep+"/play", play, m.web, func() {
		m.do(http.MethodPost, "/v1/auth/web/sign-out", nil, m.web).must(http.StatusOK)
	}).expect(http.StatusUnauthorized, "unauthorized")

	session := m.env.session("owner", "correct horse battery")
	m.registerWeb(session, newID(), newID()).must(http.StatusOK)
	d := m.play(ep, browser(session), nil).must(http.StatusOK)
	url, token := d.str("url"), d.str("token")
	credential := hex.EncodeToString(auth.HashCredential(session))
	afterCredentialRead(m.env, func() { m.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(session)).must(http.StatusOK) })
	if rec := m.raw(http.MethodGet, url, cookieOnly(session)); rec.Code < 400 || bytes.Contains(rec.Body.Bytes(), m.pub.audio[:64]) {
		t.Fatalf("media bytes after a sign-out during the request: %d, %d bytes", rec.Code, rec.Body.Len())
	}
	if _, ok := m.srv.cfg.Media.Lookup(token, m.account, credential); ok {
		t.Fatal("the signed-out session's token is still live")
	}

	// A revoked browser installation's tokens go too.
	third := m.env.session("owner", "correct horse battery")
	install := newID()
	m.registerWeb(third, install, newID()).must(http.StatusOK)
	token = m.play(ep, browser(third), nil).must(http.StatusOK).str("token")
	m.revoke(m.phone.cred, install).must(http.StatusOK)
	if _, ok := m.srv.cfg.Media.Lookup(token, m.account, hex.EncodeToString(auth.HashCredential(third))); ok {
		t.Fatal("the revoked installation's token is still live")
	}
}

// Media verification starts only for a caller who is
// still an administrator.
func TestMediaVerificationChecksTheAdministratorAgain(t *testing.T) {
	m := newMediaEnv(t)
	m.createAccount(m.session, "second", "second-password", true)
	root := m.env.session("second", "second-password")
	delayed(m.env, http.MethodPost, "/v1/admin/media/verify", `{}`, m.web, func() {
		m.adminDo(root, http.MethodPatch, "/v1/admin/accounts/"+m.account, map[string]any{"is_admin": false}).must(http.StatusOK)
	}).expect(http.StatusForbidden, "forbidden")
}

// A refresh request authorizes in its scheduling
// transaction; a device revoked during the request schedules nothing.
func TestRefreshRequestChecksTheCallerAgain(t *testing.T) {
	m := newMediaEnv(t)
	podcast, _, _ := m.subscribe(m.pub.srv.URL+"/feed.xml", nil)
	m.srv.cfg.Refresher = refresh.New(m.srv.cfg.Protocol, refresh.Config{Fetcher: m.fetcher})
	schedule := func() storage.RefreshRow {
		var row storage.RefreshRow
		if err := m.store.Write(context.Background(), func(tx *storage.WriteTx) error {
			var err error
			row, _, err = tx.Account(m.account).RefreshSchedule(podcast)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return row
	}
	before := schedule()
	delayed(m.env, http.MethodPost, "/v1/refresh", fmt.Sprintf(`{"podcast_id":%q}`, podcast), m.phone.cred, func() {
		m.revoke(m.web, m.phone.install).must(http.StatusOK)
	}).expect(http.StatusUnauthorized, "unauthorized")
	if after := schedule(); after != before {
		t.Fatalf("a revoked device rescheduled the refresh: %+v, was %+v", after, before)
	}
}

// Each event checks the credential in the read that
// supplies it; a stream whose session ended sends no later change.
func TestEventsCheckTheCallerAgain(t *testing.T) {
	e := newReadEnv(t)
	p := e.phoneOf("owner", "correct horse battery")
	srv := httptest.NewServer(e.srv)
	defer srv.Close()
	defer e.srv.Shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/events", nil)
	cookieOnly(e.session)(req)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	rd := bufio.NewReader(res.Body)
	next := func() string {
		var b strings.Builder
		for {
			line, err := rd.ReadString('\n')
			b.WriteString(line)
			if err != nil || line == "\n" {
				return b.String()
			}
		}
	}
	if first := next(); !strings.HasPrefix(first, "event: changed") {
		t.Fatalf("first event %q", first)
	}
	e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, e.web).must(http.StatusOK)
	pushed(t, p.push(op{seq: 1, fact: p.noteFact("after the sign-out")}))
	if got := next(); !strings.HasPrefix(got, "event: closed") {
		t.Fatalf("after the sign-out the stream sent %q", got)
	}
}

// blockedStream is a response whose writes block until a deadline at or
// before now is set (a client that stopped reading); it records deadlines.
type blockedStream struct {
	h       http.Header
	entered chan struct{}
	expired chan struct{}
	once    sync.Once
	mu      sync.Mutex
	set     []time.Time
}

func (w *blockedStream) Header() http.Header { return w.h }
func (w *blockedStream) WriteHeader(int)     {}
func (w *blockedStream) Flush()              {}
func (w *blockedStream) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.expired
	return 0, io.ErrClosedPipe
}

func (w *blockedStream) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	w.set = append(w.set, t)
	w.mu.Unlock()
	if !t.After(time.Now()) {
		w.once.Do(func() { close(w.expired) })
	}
	return nil
}

// The event stream sets a write deadline before it writes, and shutdown
// interrupts a write blocked on a client that stopped reading.
func TestEventStreamWritesAreBounded(t *testing.T) {
	e := newReadEnv(t)
	w := &blockedStream{h: http.Header{}, entered: make(chan struct{}, 1), expired: make(chan struct{})}
	r := httptest.NewRequest(http.MethodGet, testOrigin+"/v1/events", nil)
	e.web(r)
	done := make(chan struct{})
	go func() { e.srv.ServeHTTP(w, r); close(done) }()
	select {
	case <-w.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream never wrote")
	}
	w.mu.Lock()
	first := append([]time.Time(nil), w.set...)
	w.mu.Unlock()
	if len(first) == 0 || !first[0].After(time.Now()) {
		t.Fatalf("the stream wrote without a deadline: %v", first)
	}
	e.srv.Shutdown()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		w.once.Do(func() { close(w.expired) })
		<-done
		t.Fatal("shutdown did not interrupt the blocked write")
	}
}

// A push or a recovery upload carries at most as many
// secrets as operations, each of bounded size, refused before any
// transaction (40,000 secrets held the writer for seconds).
func TestSecretsPerRequestAreBounded(t *testing.T) {
	r := newRecoveryEnv(t)
	secrets := func(n int, password string) []any {
		list := make([]any, n)
		for i := range list {
			list[i] = map[string]any{"secret_id": fmt.Sprintf("00000000-0000-4000-8000-%012d", i), "username": "listener", "password": password}
		}
		return list
	}
	push := func(list []any) *response {
		return r.e.do(http.MethodPost, "/v1/push", map[string]any{"cursor": r.p.cursor, "install_id": r.p.install, "acked_install_seq": 0,
			"operations": []any{}, "secrets": list}, r.p.cred)
	}
	push(secrets(501, "pw")).expect(http.StatusRequestEntityTooLarge, "payload_too_large")
	push(secrets(1, strings.Repeat("p", 4097))).expect(http.StatusBadRequest, "invalid_request")
	many := secrets(1, "pw")
	many[0].(map[string]any)["approved_origins"] = make([]any, 17)
	push(many).expect(http.StatusBadRequest, "invalid_request")
	push(secrets(500, strings.Repeat("p", 4096))).must(http.StatusOK)
	r.e.do(http.MethodPost, "/v1/sync/recovery/upload", map[string]any{"recovery_id": r.rid, "entries": []any{}, "secrets": secrets(501, "pw")},
		r.p.cred).expect(http.StatusRequestEntityTooLarge, "payload_too_large")
}

// Previews running at once are bounded per account and
// over the server; beyond the bound a preview answers server_busy with
// Retry-After at once (20 previews of one account all fetched before).
func TestPreviewsAreBounded(t *testing.T) {
	e := newReadEnv(t)
	entered := make(chan struct{}, 32)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		io.WriteString(w, `<rss version="2.0"><channel><title>ok</title></channel></rss>`)
	}))
	defer upstream.Close()
	f := feed.NewFetcher()
	f.AllowLoopback(func(h string) bool { return h == "127.0.0.1" })
	e.srv.cfg.Fetcher = f
	e.createAccount(e.session, "second", "second-password", false)
	second := e.env.session("second", "second-password")
	e.registerWeb(second, newID(), newID()).must(http.StatusOK)
	e.createAccount(e.session, "third", "third-password", false)
	third := e.env.session("third", "third-password")
	e.registerWeb(third, newID(), newID()).must(http.StatusOK)
	preview := func(session string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, testOrigin+"/v1/read/preview", strings.NewReader(fmt.Sprintf(`{"url":%q}`, upstream.URL)))
		r.RemoteAddr = loopbackPeer
		r.Header.Set("Content-Type", "application/json")
		browser(session)(r)
		w := httptest.NewRecorder()
		e.srv.ServeHTTP(w, r)
		return w
	}
	var wg sync.WaitGroup
	start := func(session string) {
		wg.Add(1)
		go func() { defer wg.Done(); preview(session) }()
		<-entered
	}
	// The owner's two, then the second account's two: the server's four.
	start(e.session)
	start(e.session)
	if w := preview(e.session); w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" ||
		!strings.Contains(w.Body.String(), "server_busy") {
		t.Fatalf("a third preview of one account: %d %s", w.Code, w.Body)
	}
	start(second)
	start(second)
	if w := preview(third); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("a fifth preview on the server: %d %s", w.Code, w.Body)
	}
	close(release)
	wg.Wait()
	if w := preview(third); w.Code != http.StatusOK {
		t.Fatalf("a preview once the others ended: %d %s", w.Code, w.Body)
	}
}
