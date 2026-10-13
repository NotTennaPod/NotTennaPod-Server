// Package media is the browser media compatibility (PROTOCOL §13): the playback descriptor, the
// media relay, bounded spooling, and the compatibility path that converts what a browser cannot
// play with ffmpeg, keeping the media timeline.
//
// The relay is episode- and account-scoped, never a URL proxy: the API
// resolves the episode, and this package fetches its enclosure with the
// feed fetcher's rules (destination checks on every hop, credentials only
// to approved origins). Relayed bytes are served with headers that keep them
// from ever becoming a page of the app's origin (PROTOCOL §13).
package media

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Limits are the media bounds (PROTOCOL §13); tests lower some.
type Limits struct {
	RelayPerAccount    int           // concurrent media streams per account
	ConversionsServer  int           // ffmpeg conversions per server
	ProbesServer       int           // ffprobe runs per server
	ProbeDeadline      time.Duration // one probe, waiting for a slot included
	PinDeadline        time.Duration // the pin's request, redirects included
	ProbeBudget        int64         // the most a probe reads
	Stall              time.Duration // an upstream silent this long ends the stream
	TokenIdle          time.Duration // a token unused this long expires
	TokensPerAccount   int
	SpoolFiles         int   // spool copies per server
	SpoolMax           int64 // the largest copy
	StatusWait         time.Duration
	AssetsPerAccount   int
	ImageMax, TextMax  int64
	AssetTotal         time.Duration
	ConversionWatchdog time.Duration // ffmpeg silent this long while its reader is served is killed (0: twice Stall)
	StreamWrite        time.Duration // one write of a stream may wait this long for a browser that stopped reading (0: 2 min)
}

// DefaultLimits are the documented bounds.
func DefaultLimits() Limits {
	return Limits{RelayPerAccount: 4, ConversionsServer: 2, ProbesServer: 2, ProbeDeadline: 30 * time.Second, PinDeadline: 30 * time.Second,
		ProbeBudget: 16 << 20, Stall: 60 * time.Second, TokenIdle: 12 * time.Hour, TokensPerAccount: 32,
		SpoolFiles: 2, SpoolMax: 1 << 30, StatusWait: 2 * time.Second, AssetsPerAccount: 6,
		ImageMax: 10 << 20, TextMax: 5 << 20, AssetTotal: 60 * time.Second, StreamWrite: 2 * time.Minute}
}

// streamWrite is a stream's write deadline from now: renewed before every
// write, so a long healthy stream is never cut, and a browser that stops
// reading frees the stream.
func (s *Service) streamWrite() time.Time {
	d := s.cfg.Limits.StreamWrite
	if d <= 0 {
		d = 2 * time.Minute
	}
	return time.Now().Add(d)
}

// spoolGrace is how long an account's current playback stays protected
// from eviction after its token was last used: a pause while it plays.
const spoolGrace = 5 * time.Minute

// Config wires the service.
type Config struct {
	Fetcher  *feed.Fetcher
	FFmpeg   string // --ffmpeg: the ffmpeg binary; "" looks it up on PATH
	FFprobe  string // --ffprobe: "" looks next to --ffmpeg, then on PATH
	NoTools  bool   // run without ffmpeg whatever is installed (tests of the degraded mode)
	SpoolDir string // private copies (PROTOCOL §13)
	DataDir  string // the database's directory, for the free-space headroom
	Now      func() time.Time
	Logger   *slog.Logger
	Limits   Limits
}

// Service is the media relay and compatibility path.
type Service struct {
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
	reader *loopback
	tokens *tokenTable
	probes *probeCache
	spools *spools
	conv   *conversions

	curMu   sync.Mutex
	current map[string]string // account → the token of its latest descriptor ("" when it has none)

	relayMu sync.Mutex
	relay   map[string]int // media streams per account

	assetMu sync.Mutex
	assets  map[string]chan struct{}

	mu       sync.RWMutex
	tl       *toolset
	matrix   *Matrix
	verified chan struct{} // closed when the first check has finished
	checking atomic.Bool
	wg       sync.WaitGroup
	once     sync.Once
}

// New returns the service; Start begins its checks and housekeeping.
func New(cfg Config) (*Service, error) {
	if cfg.Fetcher == nil {
		cfg.Fetcher = feed.NewFetcher()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Limits == (Limits{}) {
		cfg.Limits = DefaultLimits()
	}
	if cfg.Limits.ConversionWatchdog == 0 {
		cfg.Limits.ConversionWatchdog = 2 * cfg.Limits.Stall
	}
	if cfg.SpoolDir == "" {
		return nil, errors.New("media: a spool directory is required")
	}
	// A real, private folder: emptying a symlinked one would empty its target.
	if err := private.RealDir(cfg.SpoolDir); err != nil {
		return nil, fmt.Errorf("media: the spool folder: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{cfg: cfg, ctx: ctx, cancel: cancel, reader: &loopback{},
		probes:   &probeCache{entries: map[string]*probeEntry{}, sem: make(chan struct{}, cfg.Limits.ProbesServer)},
		conv:     &conversions{max: cfg.Limits.ConversionsServer, byAccount: map[string]*slot{}},
		relay:    map[string]int{},
		current:  map[string]string{},
		assets:   map[string]chan struct{}{},
		verified: make(chan struct{}),
	}
	s.spools = &spools{dir: cfg.SpoolDir, max: cfg.Limits.SpoolFiles, maxBytes: cfg.Limits.SpoolMax,
		headroom: s.headroom, free: freeSpace, playing: s.playing, files: map[string]*spoolFile{}}
	s.tokens = &tokenTable{byID: map[string]*token{}, idle: cfg.Limits.TokenIdle, perAccount: cfg.Limits.TokensPerAccount,
		dropped: func(t *token) {
			if t.spool != nil {
				s.spools.release(t.spool)
			}
		}}
	clearDir(cfg.SpoolDir)
	return s, nil
}

// Start finds ffmpeg and verifies the coverage matrix in the background
// (PROTOCOL §13), and expires idle tokens. ctx ends everything (shutdown).
func (s *Service) Start(ctx context.Context) {
	stop := context.AfterFunc(ctx, s.Close)
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.Verify(s.ctx)
	}()
	go func() {
		defer s.wg.Done()
		defer stop()
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.tokens.sweep(s.now())
			}
		}
	}()
}

// Close ends every stream and process and deletes the spool copies.
func (s *Service) Close() {
	s.once.Do(func() {
		s.cancel()
		s.tokens.dropAll()
		s.spools.closeAll()
		s.reader.close()
	})
}

// Wait waits for the background work to end after Close.
func (s *Service) Wait() { s.wg.Wait() }

func (s *Service) now() time.Time { return s.cfg.Now() }

// headroom is the free-space rule of PROTOCOL §14: the larger of 1 GiB and twice
// the database (with its WAL).
func (s *Service) headroom() int64 {
	return max(int64(1<<30), 2*databaseSize(s.cfg.DataDir))
}

// Episode is an episode the API resolved for a descriptor: one the account
// holds in a subscribed, archived or kept podcast.
type Episode struct {
	Account      string
	Credential   string // the session credential the token is bound to
	ID           string
	SourceURL    string // metadata media.url, else the occurrence's enclosure_url
	DeclaredType string // the feed's enclosure type
	DurationMS   int64  // the library's (measured, else the feed's); 0 unknown
	SecretID     string // the podcast's credential secret ("" without): a new one is a new token
	Credentials  *feed.Credentials
}

// PlayRequest is the player's descriptor request (PROTOCOL §13).
type PlayRequest struct {
	Capabilities map[string]string `json:"capabilities"`  // canPlayType answers for Capabilities
	StartMS      int64             `json:"start_ms"`      // where playback starts, on the original timeline
	DirectFailed bool              `json:"direct_failed"` // the publisher's URL failed in the element
	Force        string            `json:"force"`         // "compat": a seek beyond what a non-seekable relay buffered
}

// Descriptor is how the player plays an episode (PROTOCOL §13).
type Descriptor struct {
	Mode           string       `json:"mode"` // direct, relay, compat, unsupported or failed
	URL            *string      `json:"url"`
	Token          *string      `json:"token"`
	Type           *string      `json:"type"`
	Profile        *string      `json:"profile"`
	OffsetMS       int64        `json:"offset_ms"`
	DurationMS     *int64       `json:"duration_ms"`
	Seekable       bool         `json:"seekable"`
	Representation *string      `json:"representation"`
	Reason         *string      `json:"reason"`
	Message        *string      `json:"message"`
	Error          *ErrorDetail `json:"error"` // failed: the publisher's or the network's fault, with Android's label
}

func ptr[T any](v T) *T { return &v }

// Unsupported reasons.
var unsupportedMessages = map[string]string{
	"no_ffmpeg":      "This browser cannot play this episode's format, and the server cannot convert it: ffmpeg is not installed on the server.",
	"unknown_format": "This browser cannot play this episode's format, and the server could not read it.",
	"no_profile":     "This browser cannot play this episode's format or any format the server converts to.",
	"too_large":      "The episode's file is too large for the server to prepare for this browser.",
}

func unsupported(reason string, durationMS int64) *Descriptor {
	d := &Descriptor{Mode: "unsupported", Reason: ptr(reason), Message: ptr(unsupportedMessages[reason])}
	if durationMS > 0 {
		d.DurationMS = ptr(durationMS)
	}
	return d
}

// failed is the descriptor for a failure the player shows in Android's
// download-error wording; only a busy server is an HTTP error.
func failed(err error) (*Descriptor, error) {
	e := fetchFailure(err)
	if e.Code == "server_busy" {
		return nil, e
	}
	return &Descriptor{Mode: "failed", Error: e.Detail()}, nil
}

// NormalizeSource is the enclosure URL as the relay requests it and as a
// token records it: http(s) only, without userinfo or fragment (a media
// fragment like #t=0 is the player's, never sent).
func NormalizeSource(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", false
	}
	u.User, u.Fragment, u.RawFragment = nil, "", ""
	return u.String(), true
}

// ErrInvalid is a request the player should not have sent.
var ErrInvalid = errors.New("media: invalid request")

// Play decides how the player plays an episode (PROTOCOL §13):
//   - direct when the browser decodes the source type, the URL is https:,
//     the podcast has no credentials, and the player has not seen direct
//     playback fail;
//   - else relay when the browser decodes the type;
//   - else compat, a conversion starting at start_ms;
//   - else unsupported, with the reason.
//
// The relay and compat modes pin the representation and return a token
// media URL.
func (s *Service) Play(ctx context.Context, ep Episode, req PlayRequest) (*Descriptor, error) {
	if req.Force != "" && req.Force != "compat" || req.StartMS < 0 {
		return nil, ErrInvalid
	}
	// Asking for a descriptor ends the account's previous playback: its
	// copy is no longer protected (unless this one shares it).
	s.setCurrent(ep.Account, "")
	source, ok := NormalizeSource(ep.SourceURL)
	if !ok {
		return failed(newError(http.StatusBadGateway, "media_unavailable", "io"))
	}
	src, _ := url.Parse(source)
	declared := mediaType(ep.DeclaredType)
	if declared == "" {
		declared = typeFromURL(source)
	}
	direct := src.Scheme == "https" && ep.Credentials == nil && !req.DirectFailed && req.Force != "compat"

	pinCtx, cancelPin := context.WithTimeout(ctx, s.cfg.Limits.PinDeadline)
	r, pinErr := pin(pinCtx, s.cfg.Fetcher, source, ep.Credentials)
	cancelPin()
	id := newTokenID() // the descriptor's token
	var up *upstream
	var facts *Facts
	var key string // the representation's copy and probes
	over := false
	if pinErr == nil {
		up = &upstream{fetcher: s.cfg.Fetcher, stall: s.cfg.Limits.Stall, rep: r}
		// Another descriptor's copy and probe facts are shared only when a
		// strong ETag shows they are of the same bytes. Without one (a feed
		// reusing an address for each new episode, served without
		// validators, even at an equal length) this descriptor's copy, and
		// what is probed, are its own.
		key = r.key(ep.Account)
		if !r.strong() {
			key += "\x00" + id
		}
		if r.encoding == "" && r.length >= 0 {
			facts, over = s.probe(ctx, key, upstreamSource{up})
		}
	}
	served := declared
	if r != nil && r.ctype != "" {
		served = r.ctype
	}
	playable := served == "" || plays(req.Capabilities, served)
	if facts != nil {
		served = facts.bareType()
		playable = plays(req.Capabilities, facts.sourceMIME())
	}
	duration := ep.DurationMS
	if facts != nil && facts.DurationMS > 0 && (!facts.Estimated || duration <= 0) {
		duration = facts.DurationMS
	}
	if served == "" {
		served = "audio/mpeg"
	}

	if direct && playable {
		d := &Descriptor{Mode: "direct", URL: ptr(source), Type: ptr(served), Seekable: true}
		if duration > 0 {
			d.DurationMS = ptr(duration)
		}
		return d, nil
	}
	if pinErr != nil {
		return failed(pinErr)
	}
	spooled := r.encoding != "" || r.length < 0 || (over && !r.ranges)
	t := &token{id: id, account: ep.Account, credential: ep.Credential, episode: ep.ID, sourceURL: source,
		up: up, facts: facts, durationMS: duration, ctype: served, now: s.now}
	t.credRev = ep.SecretID
	describe := func(d *Descriptor) *Descriptor {
		d.URL, d.Token = ptr("/v1/media/s/"+t.id), ptr(t.id)
		if duration > 0 {
			d.DurationMS = ptr(duration)
		}
		switch {
		case t.spool != nil:
			d.Representation = ptr("spool")
		case r.strong():
			d.Representation = ptr("strong")
		default:
			d.Representation = ptr("weak")
		}
		return d
	}
	if spooled {
		f, err := s.spools.acquire(s.ctx, key, up, s.now())
		switch {
		case errors.Is(err, errTooLarge):
			return unsupported("too_large", duration), nil
		case err != nil:
			return failed(err)
		}
		t.spool = f
	}
	release := func() {
		if t.spool != nil {
			s.spools.release(t.spool)
		}
	}

	if req.Force != "compat" && playable {
		t.mode = "relay"
		s.issue(t)
		return describe(&Descriptor{Mode: "relay", Type: ptr(served),
			Seekable: t.spool != nil || (r.strong() && r.ranges)}), nil
	}

	// The compatibility path.
	m := s.awaitMatrix(ctx)
	if m == nil || s.tools() == nil {
		release()
		return unsupported("no_ffmpeg", duration), nil
	}
	if facts == nil && t.spool != nil {
		release := t.spool.lease(s.now)
		facts, _ = s.probe(ctx, key+"\x00spool", t.source())
		release()
		if facts != nil && facts.DurationMS > 0 && (!facts.Estimated || duration <= 0) {
			duration, t.durationMS = facts.DurationMS, facts.DurationMS
		}
	}
	if facts == nil {
		release()
		return unsupported("unknown_format", duration), nil
	}
	t.facts = facts
	for i := range Profiles {
		p := &Profiles[i]
		if plays(req.Capabilities, p.MIME) && m.allows(facts, p.Name) {
			t.profile = p
			break
		}
	}
	if t.profile == nil {
		release()
		return unsupported("no_profile", duration), nil
	}
	t.mode, t.ctype = "compat", t.profile.Type
	t.startMS = req.StartMS
	if duration > 0 && t.startMS > duration {
		t.startMS = duration
	}
	s.issue(t)
	return describe(&Descriptor{Mode: "compat", Type: ptr(t.profile.Type), Profile: ptr(t.profile.Name), OffsetMS: t.startMS}), nil
}

// issue makes a token live and the account's current playback.
func (s *Service) issue(t *token) {
	s.tokens.put(t, s.now())
	s.setCurrent(t.account, t.id)
}

func (s *Service) setCurrent(account, id string) {
	s.curMu.Lock()
	defer s.curMu.Unlock()
	s.current[account] = id
}

// playing reports whether a copy serves live playback: a token holding it is its account's
// current playback, was used within the grace time, and has not completed.
// A completed stream, a descriptor for something else, and an abandoned
// token each end the protection; operation leases protect apart.
func (s *Service) playing(f *spoolFile, now time.Time) bool {
	// A failed copy serves no playback: it is never protected.
	f.mu.Lock()
	failed := f.err != nil
	f.mu.Unlock()
	if failed {
		return false
	}
	s.curMu.Lock()
	current := make(map[string]string, len(s.current))
	for a, id := range s.current {
		current[a] = id
	}
	s.curMu.Unlock()
	s.tokens.mu.Lock()
	var holders []*token
	for _, t := range s.tokens.byID {
		if t.spool == f && current[t.account] == t.id {
			holders = append(holders, t)
		}
	}
	s.tokens.mu.Unlock()
	for _, t := range holders {
		t.mu.Lock()
		live := now.Sub(t.lastUsed) < spoolGrace && !(t.complete > 0 && t.complete > t.failed)
		t.mu.Unlock()
		if live {
			return true
		}
	}
	return false
}

// source is where a token's bytes come from.
func (t *token) source() source {
	if t.spool != nil {
		return spoolSource{f: t.spool, declared: t.up.rep.length, encoded: t.up.rep.encoding != "", now: t.now}
	}
	return upstreamSource{t.up}
}

// Token is a session's media token, for the API's re-checks.
type Token struct{ t *token }

// Episode, Source and CredentialRevision are what the token was issued for.
func (k Token) Episode() string            { return k.t.episode }
func (k Token) Source() string             { return k.t.sourceURL }
func (k Token) CredentialRevision() string { return k.t.credRev }

// Lookup finds a token for the session that holds it.
func (s *Service) Lookup(id, account, credential string) (Token, bool) {
	t := s.tokens.lookup(id, account, credential, s.now())
	return Token{t}, t != nil
}

// DropSessions drops the tokens of sessions gone reports ended (a sign-out,
// a revocation, a reset, a disable): their URLs stop answering at once and
// what they hold is released.
func (s *Service) DropSessions(gone func(account, credential string) bool) {
	s.tokens.dropWhere(gone)
}

// Changed records that what the token was issued for changed (the episode's
// URL or the podcast's credentials): its streams fail with media_changed.
func (s *Service) Changed(k Token) *Error {
	k.t.fail(errChanged)
	return errChanged
}

// Status is the token's out-of-band outcome (PROTOCOL §13).
func (s *Service) Status(ctx context.Context, k Token) Status {
	return k.t.status(ctx, s.cfg.Limits.StatusWait)
}

// Serve answers a media request on a token: GET or HEAD. It returns an
// error only when it has written nothing; the caller writes it as JSON.
func (s *Service) Serve(w http.ResponseWriter, r *http.Request, k Token) *Error {
	t := k.t
	// Shutdown ends streams at once rather than after the server's grace period.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	r = r.WithContext(ctx)
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(s.streamWrite())
	stopDeadline := context.AfterFunc(ctx, func() { _ = rc.SetWriteDeadline(time.Now()) })
	defer stopDeadline()
	if t.mode == "compat" {
		if r.Method == http.MethodHead {
			h := w.Header()
			writeMediaHeaders(h, t.ctype)
			h.Set("Accept-Ranges", "none")
			h.Set("Cache-Control", "no-store, no-transform")
			w.WriteHeader(http.StatusOK)
			return nil
		}
		start := t.startMS
		if v := r.URL.Query().Get("start_ms"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return newError(http.StatusBadRequest, "invalid_request", "http_status")
			}
			start = n
			if t.durationMS > 0 && start > t.durationMS {
				start = t.durationMS
			}
		}
		// A conversion is a stream: it starts at a time, never at a byte.
		if a, _, ok := parseOneRange(r.Header.Get("Range")); ok && a > 0 {
			writeMediaHeaders(w.Header(), t.ctype)
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return nil
		}
		if !s.takeStream(t.account) {
			t.fail(errAccountMax)
			return errAccountMax
		}
		defer s.dropStream(t.account)
		return s.serveCompat(w, r, t, start)
	}
	if r.Method == http.MethodHead {
		s.headRelay(w, t)
		return nil
	}
	if !s.takeStream(t.account) {
		t.fail(errAccountMax)
		return errAccountMax
	}
	defer s.dropStream(t.account)
	if t.spool != nil {
		return s.serveSpool(w, r, t)
	}
	return s.serveUpstream(w, r, t)
}

// takeStream admits a media stream: at most a few per account (PROTOCOL §13).
func (s *Service) takeStream(account string) bool {
	s.relayMu.Lock()
	defer s.relayMu.Unlock()
	if s.relay[account] >= s.cfg.Limits.RelayPerAccount {
		return false
	}
	s.relay[account]++
	return true
}

func (s *Service) dropStream(account string) {
	s.relayMu.Lock()
	defer s.relayMu.Unlock()
	if s.relay[account]--; s.relay[account] <= 0 {
		delete(s.relay, account)
	}
}
