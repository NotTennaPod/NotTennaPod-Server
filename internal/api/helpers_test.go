package api

// Shared helpers for PROTOCOL §5 acceptance tests. Every expectation in the
// tests is derived from PROTOCOL §5, §10; the helpers only drive the API the way a phone app or a
// browser would.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

const (
	testHost     = "nottennapod.test"
	testOrigin   = "http://" + testHost
	loopbackPeer = "127.0.0.1:41000"
	remotePeer   = "192.0.2.10:41000" // a tailnet peer (TEST-NET-1), not loopback
	otherPeer    = "192.0.2.11:41000"
)

// fastParams keeps Argon2id cheap except in the tests about its cost.
var fastParams = auth.Params{Memory: 1024, Time: 1, Threads: 1}

// fakeClock starts at the real time (the store stamps rows with the real clock)
// and moves only when a test advances it.
type fakeClock struct {
	mu   sync.Mutex
	now  time.Time
	next oneShot // a test's action at the clock's next reading
}

func (c *fakeClock) Now() time.Time {
	c.next.run()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// oneShot is a test hook that runs at most once.
type oneShot struct{ f atomic.Pointer[func()] }

func (h *oneShot) set(f func()) { h.f.Store(&f) }

func (h *oneShot) run() {
	if p := h.f.Swap(nil); p != nil {
		(*p)()
	}
}

func (h *oneShot) pending() bool { return h.f.Load() != nil }

type env struct {
	t           *testing.T
	dir         string
	store       *storage.Store
	srv         *Server
	clock       *fakeClock
	setup       *SetupState
	afterVerify oneShot
	beforeIssue oneShot
	// cookie authentication: the presented sessions read and classified,
	// before any write
	afterSessionRead oneShot
	beforeSignOut    oneShot // a web sign-out: authenticated, before its write

	mu      sync.Mutex
	secrets []string
}

func withHasher(h *auth.Hasher) func(*Config) { return func(c *Config) { c.Hasher = h } }
func withPublicURL(u string) func(*Config)    { return func(c *Config) { c.PublicURL = u } }

// newEnv opens a fresh data directory with a setup code, as a first start does.
func newEnv(t *testing.T, opts ...func(*Config)) *env {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := &env{t: t, dir: dir, store: store, clock: &fakeClock{now: time.Now()}}
	if e.setup, err = NewSetupState(filepath.Join(dir, "setup-code")); err != nil {
		t.Fatal(err)
	}
	e.addSecret(e.setup.Code())
	cfg := Config{Store: store, Hasher: auth.NewHasher(fastParams), Limiter: auth.NewLimiter(e.clock.Now), Setup: e.setup,
		Now: e.clock.Now, afterPasswordVerify: e.afterVerify.run, beforeIssue: e.beforeIssue.run,
		afterSessionRead: e.afterSessionRead.run, beforeSignOut: e.beforeSignOut.run}
	for _, o := range opts {
		o(&cfg)
	}
	if e.srv, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Protocol != nil {
		// A recovery upload's job may outlast its request: it ends before
		// the store closes and the data directory goes.
		t.Cleanup(cfg.Protocol.WaitRecoveryJobsForTest)
	}
	return e
}

func (e *env) addSecret(s string) {
	if s == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !slices.Contains(e.secrets, s) {
		e.secrets = append(e.secrets, s)
	}
}

func (e *env) allSecrets() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.secrets)
}

// ---- requests

type opt func(*http.Request)

// rawBody is sent as is.
type rawBody string

func bearer(token string) opt {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

// sessionCookieOf is the cookie a browser holds a session in: its own name
// (anything that is not a credential under the legacy name).
func sessionCookieOf(session string) *http.Cookie {
	if !auth.WellFormed(session) {
		return &http.Cookie{Name: legacySessionCookie, Value: session}
	}
	return &http.Cookie{Name: sessionCookieName(auth.HashCredential(session)), Value: session}
}

// browser sends the session cookie with our Origin, as the web app does.
func browser(session string) opt {
	return func(r *http.Request) {
		r.AddCookie(sessionCookieOf(session))
		r.Header.Set("Origin", testOrigin)
	}
}

// cookieOnly sends the session cookie without any Origin.
func cookieOnly(session string) opt {
	return func(r *http.Request) { r.AddCookie(sessionCookieOf(session)) }
}

func withOrigin(o string) opt {
	return func(r *http.Request) {
		if o == "" {
			r.Header.Del("Origin")
		} else {
			r.Header.Set("Origin", o)
		}
	}
}

func from(addr string) opt { return func(r *http.Request) { r.RemoteAddr = addr } }

func ctype(ct string) opt {
	return func(r *http.Request) {
		if ct == "" {
			r.Header.Del("Content-Type")
		} else {
			r.Header.Set("Content-Type", ct)
		}
	}
}

func hdr(k, v string) opt { return func(r *http.Request) { r.Header.Set(k, v) } }

// viaTLS makes the request arrive over TLS.
func viaTLS() opt { return func(r *http.Request) { r.TLS = &tls.ConnectionState{} } }

func encode(t testing.TB, method string, body any) []byte {
	switch b := body.(type) {
	case nil:
		if method == http.MethodGet {
			return nil
		}
		return []byte("{}")
	case rawBody:
		return []byte(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
}

// do serves one request in process, from a loopback peer over plain HTTP unless
// options say otherwise.
func (e *env) do(method, path string, body any, opts ...opt) *response {
	e.t.Helper()
	raw := encode(e.t, method, body)
	var rd io.Reader
	if raw != nil {
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, testOrigin+path, rd)
	req.RemoteAddr = loopbackPeer
	if raw != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, o := range opts {
		o(req)
	}
	rec := httptest.NewRecorder()
	e.srv.ServeHTTP(rec, req)
	return e.check(method+" "+path, rec.Result())
}

// netDo sends a request over a real connection to a test server.
func (e *env) netDo(client *http.Client, base, method, path string, body any, headers map[string]string) *response {
	raw := encode(e.t, method, body)
	req, err := http.NewRequest(method, base+path, bytes.NewReader(raw))
	if err != nil {
		e.t.Error(err)
		return &response{t: e.t, what: method + " " + path}
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		e.t.Errorf("%s %s: %v", method, path, err)
		return &response{t: e.t, what: method + " " + path}
	}
	return e.check(method+" "+path, res)
}

// ---- responses

type response struct {
	t       testing.TB
	what    string
	status  int
	header  http.Header
	raw     []byte
	body    map[string]any
	cookies []*http.Cookie
}

// errorStatus pairs every error code with its HTTP status: PROTOCOL §10 table plus
// the codes PROTOCOL §5 introduces ("Error codes").
var errorStatus = map[string][]int{
	"method_not_allowed":      {405},
	"unsupported_media_type":  {415},
	"password_incorrect":      {400},
	"passphrase_invalid":      {400},
	"passphrase_weak":         {400},
	"unauthorized":            {401},
	"forbidden":               {403},
	"rate_limited":            {429},
	"server_busy":             {503},
	"payload_too_large":       {413},
	"invalid_request":         {400},
	"setup_complete":          {409},
	"installation_conflict":   {409},
	"installation_superseded": {409},
	"not_found":               {404},
	"last_admin":              {409},
	"username_taken":          {409},
}

// check applies the rules every response must follow (every response is JSON with Cache-Control:
// no-store; errors use the PROTOCOL §10 body; secrets never appear in error messages).
func (e *env) check(what string, res *http.Response) *response {
	e.t.Helper()
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		e.t.Errorf("%s: reading body: %v", what, err)
	}
	r := &response{t: e.t, what: what, status: res.StatusCode, header: res.Header, raw: raw, cookies: res.Cookies()}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		e.t.Errorf("%s: Content-Type %q, want JSON (%d %q)", what, ct, r.status, raw)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
		e.t.Errorf("%s: Cache-Control %q, want no-store", what, cc)
	}
	if err := json.Unmarshal(raw, &r.body); err != nil || r.body == nil {
		e.t.Errorf("%s: body is not a JSON object: %q", what, raw)
	}
	if r.status >= 400 {
		e.checkError(r)
	}
	if tok, ok := r.body["token"].(string); ok {
		e.addSecret(tok)
	}
	for _, c := range r.cookies {
		if isSessionCookie(c.Name) {
			e.addSecret(c.Value)
		}
	}
	return r
}

func (e *env) checkError(r *response) {
	e.t.Helper()
	code, _ := r.body["error"].(string)
	msg, _ := r.body["message"].(string)
	if code == "" || msg == "" {
		e.t.Errorf("%s: error body %q lacks error and message", r.what, r.raw)
	}
	for k := range r.body {
		if k != "error" && k != "message" && k != "retry_after" {
			e.t.Errorf("%s: error body has unexpected field %q", r.what, k)
		}
	}
	if v, ok := r.body["retry_after"]; ok {
		if n, isNum := v.(float64); !isNum || n < 1 || n != math.Trunc(n) {
			e.t.Errorf("%s: retry_after %v is not a positive number of seconds", r.what, v)
		}
	} else if code == "rate_limited" || code == "server_busy" {
		e.t.Errorf("%s: %s without retry_after", r.what, code)
	}
	if want, ok := errorStatus[code]; !ok {
		e.t.Errorf("%s: unknown error code %q (%d)", r.what, code, r.status)
	} else if !slices.Contains(want, r.status) {
		e.t.Errorf("%s: error %q with status %d, want %v", r.what, code, r.status, want)
	}
	for _, s := range e.allSecrets() {
		if bytes.Contains(r.raw, []byte(s)) {
			e.t.Errorf("%s: error response reveals a secret: %q", r.what, r.raw)
		}
	}
	if c := r.cookie(); c != nil {
		e.t.Errorf("%s: a refused request set a session cookie", r.what)
	}
}

func (r *response) must(status int) *response {
	r.t.Helper()
	if r.status != status {
		r.t.Fatalf("%s: got %d %s, want %d", r.what, r.status, bytes.TrimSpace(r.raw), status)
	}
	return r
}

func (r *response) expect(status int, code string) *response {
	r.t.Helper()
	if r.status != status || r.str("error") != code {
		r.t.Errorf("%s: got %d %s, want %d %s", r.what, r.status, bytes.TrimSpace(r.raw), status, code)
	}
	return r
}

// refused expects any error status and no credential in the response.
func (r *response) refused() *response {
	r.t.Helper()
	if r.status < 400 {
		r.t.Errorf("%s: got %d %s, want a refusal", r.what, r.status, bytes.TrimSpace(r.raw))
	}
	if r.str("token") != "" {
		r.t.Errorf("%s: a refused request returned a token", r.what)
	}
	return r
}

func (r *response) str(k string) string {
	s, _ := r.body[k].(string)
	return s
}

func (r *response) obj(k string) map[string]any {
	m, _ := r.body[k].(map[string]any)
	return m
}

func (r *response) retryAfter() int {
	n, _ := r.body["retry_after"].(float64)
	return int(n)
}

// cookie returns the session cookie the response sets (not a deletion), or nil.
func (r *response) cookie() *http.Cookie {
	for _, c := range r.cookies {
		if isSessionCookie(c.Name) && c.Value != "" && c.MaxAge >= 0 {
			return c
		}
	}
	return nil
}

// ---- domain helpers

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func device(name string, caps ...string) map[string]any {
	if caps == nil {
		caps = []string{}
	}
	return map[string]any{"name": name, "platform": "ios", "app_version": "1.0", "capabilities": caps}
}

// setupAdmin performs first setup and returns the administrator's session.
func (e *env) setupAdmin(username, password string) (session, accountID string) {
	e.t.Helper()
	e.addSecret(password)
	r := e.do(http.MethodPost, "/v1/setup", map[string]any{"username": username, "password": password, "setup_code": e.setup.Code()},
		withOrigin(testOrigin)).must(http.StatusCreated)
	c := r.cookie()
	if c == nil {
		e.t.Fatal("setup did not sign the browser in")
	}
	id, _ := r.obj("account")["account_id"].(string)
	return c.Value, id
}

func (e *env) createAccount(admin, username, password string, isAdmin bool) string {
	e.t.Helper()
	e.addSecret(password)
	r := e.do(http.MethodPost, "/v1/admin/accounts", map[string]any{"username": username, "password": password, "is_admin": isAdmin},
		browser(admin)).must(http.StatusCreated)
	id, _ := r.obj("account")["account_id"].(string)
	if id == "" {
		e.t.Fatalf("create account: no account_id in %s", r.raw)
	}
	return id
}

func (e *env) signInAs(username, password, installID, nonce string, dev map[string]any, opts ...opt) *response {
	e.t.Helper()
	e.addSecret(password)
	return e.do(http.MethodPost, "/v1/auth/device", map[string]any{"username": username, "password": password,
		"install_id": installID, "instance_nonce": nonce, "device": dev}, opts...)
}

func (e *env) signIn(username, password, installID, nonce string, opts ...opt) *response {
	e.t.Helper()
	return e.signInAs(username, password, installID, nonce, device("iPhone"), opts...)
}

// token signs a device in and returns its token.
func (e *env) token(username, password, installID, nonce string) string {
	e.t.Helper()
	r := e.signIn(username, password, installID, nonce).must(http.StatusOK)
	tok := r.str("token")
	if tok == "" {
		e.t.Fatalf("device sign-in returned no token: %s", r.raw)
	}
	return tok
}

func (e *env) webSignIn(username, password string, opts ...opt) *response {
	e.t.Helper()
	e.addSecret(password)
	return e.do(http.MethodPost, "/v1/auth/web", map[string]any{"username": username, "password": password},
		append([]opt{withOrigin(testOrigin)}, opts...)...)
}

// session signs a browser in and returns its session cookie value.
func (e *env) session(username, password string) string {
	e.t.Helper()
	c := e.webSignIn(username, password).must(http.StatusOK).cookie()
	if c == nil {
		e.t.Fatal("web sign-in set no session cookie")
	}
	return c.Value
}

func (e *env) registerWeb(session, installID, nonce string) *response {
	e.t.Helper()
	return e.do(http.MethodPost, "/v1/installations", map[string]any{"install_id": installID, "instance_nonce": nonce,
		"name": "Firefox", "app_version": "1.0", "capabilities": []string{"queue"}}, browser(session))
}

func (e *env) successor(o opt, installID, nonce string) *response {
	e.t.Helper()
	return e.do(http.MethodPost, "/v1/installations/successor", map[string]any{"install_id": installID, "instance_nonce": nonce,
		"device": device("successor")}, o)
}

func (e *env) list(o opt) *response {
	e.t.Helper()
	return e.do(http.MethodGet, "/v1/installations", nil, o)
}

func (e *env) revoke(o opt, installID string) *response {
	e.t.Helper()
	return e.do(http.MethodPost, "/v1/installations/"+installID+"/revoke", nil, o)
}

// installations lists the caller's installations by ID.
func (e *env) installations(o opt) map[string]map[string]any {
	e.t.Helper()
	r := e.list(o).must(http.StatusOK)
	out := map[string]map[string]any{}
	list, ok := r.body["installations"].([]any)
	if !ok {
		e.t.Fatalf("installations list missing: %s", r.raw)
	}
	for _, v := range list {
		m := v.(map[string]any)
		out[m["install_id"].(string)] = m
	}
	return out
}

// current returns the caller's own installation as the list reports it.
func (e *env) current(o opt) string {
	e.t.Helper()
	cur := ""
	for id, m := range e.installations(o) {
		if m["current"] == true {
			if cur != "" {
				e.t.Errorf("two installations are current: %s and %s", cur, id)
			}
			cur = id
		}
	}
	return cur
}

func (e *env) adminDo(admin, method, path string, body any) *response {
	e.t.Helper()
	return e.do(method, path, body, browser(admin))
}

func (e *env) resetPassword(admin, accountID, password string) *response {
	e.t.Helper()
	e.addSecret(password)
	return e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+accountID+"/password", map[string]any{"password": password})
}

func (e *env) setState(admin, accountID, state string) *response {
	e.t.Helper()
	return e.adminDo(admin, http.MethodPatch, "/v1/admin/accounts/"+accountID, map[string]any{"state": state})
}

// accounts returns the administrator's account list by username.
func (e *env) accounts(admin string) map[string]map[string]any {
	e.t.Helper()
	r := e.adminDo(admin, http.MethodGet, "/v1/admin/accounts", nil).must(http.StatusOK)
	out := map[string]map[string]any{}
	list, _ := r.body["accounts"].([]any)
	for _, v := range list {
		m := v.(map[string]any)
		out[m["username"].(string)] = m
	}
	return out
}

// count runs a COUNT query on the database file through a separate read-only
// connection (evidence of what is stored, independent of the API).
func (e *env) count(query string, args ...any) int {
	e.t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(e.dir, "nottennapod.db")+"?_pragma=query_only(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		e.t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		e.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (e *env) fileExists(name string) bool {
	_, err := os.Stat(filepath.Join(e.dir, name))
	return err == nil
}
