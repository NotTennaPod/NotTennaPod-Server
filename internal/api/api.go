// Package api serves the server's JSON HTTP API: setup, authentication,
// installations and administration (PROTOCOL §5).
// The sync endpoints (PROTOCOL §6) go through the same middleware.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/media"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
)

// Config wires the API to the store and the authentication primitives.
type Config struct {
	Store     *storage.Store
	Hasher    *auth.Hasher
	Limiter   *auth.Limiter
	PublicURL string // optional browser-facing origin, e.g. https://macbook.tailnet.ts.net
	Setup     *SetupState
	Now       func() time.Time
	Protocol  *protocol.Service  // sync endpoints; nil disables them
	Refresher *refresh.Refresher // POST /v1/refresh (PROTOCOL §9.4); nil disables it
	BackupDir string             // POST /v1/admin/backup writes here; default <data-dir>/backups (PROTOCOL §11)
	Imports   *importer.Runner   // the library import (PROTOCOL §11.5); nil disables it
	Views     *views.Spool       // the read API's list views (PROTOCOL §12); nil disables the read API
	Fetcher   *feed.Fetcher      // the feed preview's fetcher; default feed.NewFetcher()
	Media     *media.Service     // the media relay and compatibility path (PROTOCOL §13); nil disables them

	// Service reports the server's state for the service page (PROTOCOL §14); nil disables it.
	Service func(ctx context.Context) (any, error)

	// Test-only interruption points.
	afterPasswordVerify func()
	beforeIssue         func()
	duringBackup        func()
}

// Server is the API handler.
type Server struct {
	cfg        Config
	public     *url.URL
	mux        *http.ServeMux
	backupBusy atomic.Bool // an administrator's backup is running

	streamsDone chan struct{} // closed by Shutdown: event streams end
	streamsOnce sync.Once
	libraries   *views.Cache // the read API's libraries by account and head
	previews    *admission   // feed previews running
}

const (
	maxBody         = 64 << 10
	sessionCookie   = "ntp_session"
	sessionLifetime = 30 * 24 * time.Hour
	touchInterval   = time.Minute
	sessionExtend   = time.Hour
)

// New returns the API handler.
func New(cfg Config) (*Server, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Hasher == nil {
		cfg.Hasher = auth.NewHasher(auth.DefaultParams)
	}
	if cfg.Limiter == nil {
		cfg.Limiter = auth.NewLimiter(cfg.Now)
	}
	if cfg.Setup == nil {
		cfg.Setup = &SetupState{}
	}
	if cfg.BackupDir == "" && cfg.Store != nil {
		cfg.BackupDir = filepath.Join(cfg.Store.Dir(), "backups")
	}
	s := &Server{cfg: cfg, mux: http.NewServeMux(), streamsDone: make(chan struct{}), libraries: views.NewCache(8),
		previews: newAdmission(previewsServer, previewsPerAccount)}
	if cfg.PublicURL != "" {
		u, err := url.Parse(cfg.PublicURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("api: --public-url must be an origin like https://host[:port]: %q", cfg.PublicURL)
		}
		s.public = u
	}
	s.routes()
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// ---- errors

type apiError struct {
	status     int
	code       string
	message    string
	retryAfter time.Duration
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func fail(status int, code, message string) *apiError {
	return &apiError{status: status, code: code, message: message}
}

var (
	errUnauthorized   = fail(http.StatusUnauthorized, "unauthorized", "Sign in again.")
	errForbidden      = fail(http.StatusForbidden, "forbidden", "This account is disabled or not allowed to do this.")
	errBadCreds       = fail(http.StatusUnauthorized, "unauthorized", "The username or password is incorrect.")
	errNotFound       = fail(http.StatusNotFound, "not_found", "Not found.")
	errAccountChanged = fail(http.StatusConflict, "account_changed", "This browser signed in to another account; reload.")
	errBusy           = &apiError{status: http.StatusServiceUnavailable, code: "server_busy", message: "The server is busy; try again shortly.", retryAfter: 2 * time.Second}
)

func invalid(message string) *apiError {
	return fail(http.StatusBadRequest, "invalid_request", message)
}

// encodedBody is a response body already encoded as JSON (sync pages, whose
// embedded canonical bytes must reach the client unchanged).
type encodedBody []byte

func writeJSON(w http.ResponseWriter, status int, v any) {
	if b, ok := v.(encodedBody); ok {
		writeEncoded(w, status, b)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false) // embedded canonical bytes stay byte-identical
	_ = enc.Encode(v)
}

// pageChunk is how much of a sync page one write sends.
const pageChunk = 64 << 10

// writeEncoded writes a sync page (pull and snapshot pages, up to megabytes)
// in bounded chunks, renewing the write deadline before each, as the event
// stream does: a healthy slow client is never cut by the ordinary response
// deadline, and a stalled one is.
func writeEncoded(w http.ResponseWriter, status int, b encodedBody) {
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
	body := append([]byte(b), '\n')
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	for len(body) > 0 {
		n := min(len(body), pageChunk)
		_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
		if _, err := w.Write(body[:n]); err != nil {
			return
		}
		body = body[n:]
	}
}

func writeError(w http.ResponseWriter, err error) {
	var e *apiError
	if !errors.As(err, &e) {
		if errors.Is(err, auth.ErrBusy) {
			e = errBusy
		} else {
			e = fail(http.StatusServiceUnavailable, "server_busy", "The server could not complete the request; try again.")
			e.retryAfter = 5 * time.Second
		}
	}
	body := map[string]any{"error": e.code, "message": e.message}
	if e.retryAfter > 0 {
		secs := int64((e.retryAfter + time.Second - 1) / time.Second)
		body["retry_after"] = secs
		w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
	}
	writeJSON(w, e.status, body)
}

// ---- request plumbing

type endpoint struct {
	method       string
	auth         authMode
	originNeeded bool  // creates or uses a session cookie even without a cookie present
	supersededOK bool  // allowed for a superseded installation's credentials
	maxBody      int64 // body limit; 0 means the default 64 KiB
	successor    bool  // the successor handoff itself (does not finish supersession)
	handle       func(*request) (int, any, error)
}

type authMode int

const (
	authNone   authMode = iota
	authAny             // device token or web session
	authDevice          // device token only
	authWeb             // web session only
	authAdmin           // administrator's web session
	authSync            // device token or bound web session; superseded refused
	authRead            // bound web session only (the read API); superseded refused
)

type request struct {
	s         *Server
	endpoint  endpoint
	w         http.ResponseWriter
	r         *http.Request
	ctx       context.Context
	principal *principal
	origin    string
}

type principal struct {
	cred   storage.Credential
	viaWeb bool
	secret string // the presented credential, to refresh the session cookie
}

func (p *principal) accountID() string { return p.cred.AccountID }

// route registers path with one endpoint per method; other methods get a JSON 405.
func (s *Server) route(path string, endpoints ...endpoint) {
	byMethod := map[string]endpoint{}
	for _, e := range endpoints {
		byMethod[e.method] = e
	}
	s.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		e, ok := byMethod[r.Method]
		if !ok {
			writeError(w, fail(http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed."))
			return
		}
		s.serve(w, r, e)
	})
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request, e endpoint) {
	req := &request{s: s, w: w, r: r, ctx: r.Context(), origin: s.browserOrigin(r), endpoint: e}
	status, body, err := req.run(e)
	if err != nil {
		writeError(w, err)
		return
	}
	if sessionEnding[r.Pattern] {
		s.dropEndedMediaSessions(req.ctx)
	}
	if _, ok := body.(streamed); ok {
		return
	}
	if body == nil {
		body = map[string]any{}
	}
	writeJSON(w, status, body)
}

func (req *request) run(e endpoint) (int, any, error) {
	r := req.r
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mt != "application/json" {
			return 0, nil, fail(http.StatusUnsupportedMediaType, "unsupported_media_type", "Requests must be JSON (Content-Type: application/json).")
		}
		limit := int64(maxBody)
		if e.maxBody > 0 {
			limit = e.maxBody
		}
		r.Body = http.MaxBytesReader(req.w, r.Body, limit)
	}
	// Any Origin must be ours; cookie-creating endpoints need one.
	if o := r.Header.Get("Origin"); o != "" {
		if !sameOrigin(o, req.origin) {
			return 0, nil, fail(http.StatusForbidden, "forbidden", "Cross-origin requests are not allowed.")
		}
	} else if e.originNeeded {
		return 0, nil, fail(http.StatusForbidden, "forbidden", "This request must come from the NotTennaPod web app.")
	}
	if e.auth != authNone {
		if err := req.authenticate(e); err != nil {
			return 0, nil, err
		}
	}
	return e.handle(req)
}

// decodeEmpty accepts an empty body or exactly an empty JSON object.
func (req *request) decodeEmpty() error {
	data, err := io.ReadAll(req.r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return fail(http.StatusRequestEntityTooLarge, "payload_too_large", "The request is too large.")
		}
		return invalid("The request body could not be read.")
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	var body struct{}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return invalid("This request takes no parameters.")
	}
	if _, err := dec.Token(); err != io.EOF {
		return invalid("Unexpected data after the request body.")
	}
	return nil
}

// decode reads a JSON body strictly (unknown fields and trailing data rejected).
func (req *request) decode(v any) error {
	dec := json.NewDecoder(req.r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return fail(http.StatusRequestEntityTooLarge, "payload_too_large", "The request is too large.")
		}
		return invalid("The request body is not valid: " + err.Error())
	}
	if _, err := dec.Token(); err != io.EOF {
		return invalid("Unexpected data after the request body.")
	}
	return nil
}

// ---- origin

func (s *Server) browserOrigin(r *http.Request) string {
	if s.public != nil {
		return normalizeOrigin(s.public.Scheme, s.public.Host)
	}
	scheme := "http"
	if r.TLS != nil || (isLoopback(r) && strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")) {
		scheme = "https"
	}
	return normalizeOrigin(scheme, r.Host)
}

func normalizeOrigin(scheme, host string) string {
	host = strings.ToLower(host)
	if h, p, err := net.SplitHostPort(host); err == nil && ((scheme == "https" && p == "443") || (scheme == "http" && p == "80")) {
		host = h
		if strings.Contains(h, ":") {
			host = "[" + h + "]"
		}
	}
	return scheme + "://" + host
}

func sameOrigin(header, ours string) bool {
	u, err := url.Parse(header)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	return normalizeOrigin(strings.ToLower(u.Scheme), u.Host) == ours
}

func isLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func peerIP(r *http.Request) string {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

func (req *request) secure() bool { return strings.HasPrefix(req.origin, "https://") }

func (req *request) setSessionCookie(secret string) {
	http.SetCookie(req.w, &http.Cookie{Name: sessionCookie, Value: secret, Path: "/", HttpOnly: true, Secure: req.secure(),
		SameSite: http.SameSiteStrictMode, MaxAge: int(sessionLifetime / time.Second)})
}

func (req *request) clearSessionCookie() {
	http.SetCookie(req.w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", HttpOnly: true, Secure: req.secure(),
		SameSite: http.SameSiteStrictMode, MaxAge: -1})
}

// ---- setup code

// SetupState holds the setup code while no administrator exists.
type SetupState struct {
	mu   sync.Mutex
	code string
	path string
}

// NewSetupState generates a code and writes it to path (mode 0600).
func NewSetupState(path string) (*SetupState, error) {
	st := &SetupState{code: auth.NewSetupCode(), path: path}
	if path != "" {
		_ = os.Remove(path) // so the new file is created with mode 0600
		if err := os.WriteFile(path, []byte(st.code+"\n"), 0o600); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// Code returns the current setup code ("" once setup is complete).
func (st *SetupState) Code() string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.code
}

// Complete forgets the code and deletes its file.
func (st *SetupState) Complete() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.code = ""
	if st.path != "" {
		_ = os.Remove(st.path)
	}
}
