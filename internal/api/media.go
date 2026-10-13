package api

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/media"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
)

// Browser media (PROTOCOL §13, §15.6): the playback descriptor, token media URLs
// that authenticate with the session cookie alone, their out-of-band status, the
// metadata assets, and the compatibility path's check for administrators.
// Every endpoint takes a web session bound to its installation (the read
// API's authRead), and an episode the account does not hold in a
// subscribed, archived or kept podcast answers 404, as an unknown one does.
func (s *Server) mediaRoutes() {
	if s.cfg.Media == nil || s.cfg.Views == nil {
		return
	}
	s.route("/v1/media/types", endpoint{method: http.MethodGet, auth: authRead, handle: (*request).mediaTypes})
	// One pattern serves /v1/media/{episode}/play|image|chapters|transcript and
	// /v1/media/s/{token}: the two shapes would otherwise overlap.
	s.route("/v1/media/{a}/{b}",
		endpoint{method: http.MethodPost, auth: authRead, handle: (*request).mediaPost},
		endpoint{method: http.MethodGet, auth: authRead, handle: (*request).mediaGet},
		endpoint{method: http.MethodHead, auth: authRead, handle: (*request).mediaGet})
	s.route("/v1/media/s/{token}/status", endpoint{method: http.MethodGet, auth: authRead, handle: (*request).mediaStatus})
	s.route("/v1/media/podcasts/{id}/image",
		endpoint{method: http.MethodGet, auth: authRead, handle: (*request).podcastImage},
		endpoint{method: http.MethodHead, auth: authRead, handle: (*request).podcastImage})
	s.route("/v1/admin/media", endpoint{method: http.MethodGet, auth: authAdmin, handle: (*request).adminMedia})
	s.route("/v1/admin/media/verify", endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).adminMediaVerify})
}

// sessionEnding are the routes whose success can end sessions (a sign-out,
// a revocation, a password change or reset, an account disabled or
// deleted): after them, the media tokens of ended sessions are dropped.
var sessionEnding = map[string]bool{
	"/v1/auth/web/sign-out": true, "/v1/auth/device/sign-out": true, "/v1/installations/{id}/revoke": true,
	"/v1/account/password": true, "/v1/admin/accounts/{id}": true, "/v1/admin/accounts/{id}/password": true,
	"/v1/admin/accounts/{id}/delete": true,
}

// dropEndedMediaSessions drops the media tokens of sessions that are no
// longer valid, so their URLs stop answering at once. Every media response also checks its session.
func (s *Server) dropEndedMediaSessions(ctx context.Context) {
	if s.cfg.Media == nil {
		return
	}
	now := s.cfg.Now().UnixMilli()
	s.cfg.Media.DropSessions(func(account, credential string) bool {
		hash, err := hex.DecodeString(credential)
		if err != nil {
			return true
		}
		var cred storage.Credential
		err = s.cfg.Store.Read(ctx, func(r *storage.ReadTx) error {
			var err error
			cred, err = r.CredentialByHash(hash)
			return err
		})
		if errors.Is(err, storage.ErrNotFound) {
			return true
		}
		if err != nil {
			return false // unknown: kept, and still checked by each response
		}
		return cred.AccountID != account || cred.AccountState != "active" || (cred.ExpiresAt != nil && now >= *cred.ExpiresAt) ||
			(cred.Installation != nil && cred.Installation.RevokedAt != nil)
	})
}

// checkCaller checks the caller again in a read of its own, for work that
// reads nothing from the database (a token's status, a verification).
func (req *request) checkCaller() error {
	return req.read(func(*storage.ReadTx) error { return nil })
}

// mediaFail writes a media error in PROTOCOL §10 form, its message in
// Android's wording; the label is in the token's status.
func (req *request) mediaFail(e *media.Error) (int, any, error) {
	if e.RetryAfter > 0 {
		req.w.Header().Set("Retry-After", strconv.FormatInt(e.RetrySeconds(), 10))
	}
	writeJSON(req.w, e.Status, e.Body())
	return 0, streamed{}, nil
}

func (req *request) mediaTypes() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	// No account data, but every authenticated read checks its caller again alike.
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	profiles := []any{}
	for _, p := range media.Profiles {
		profiles = append(profiles, map[string]any{"name": p.Name, "mime": p.MIME, "type": p.Type})
	}
	return http.StatusOK, map[string]any{"capabilities": media.Capabilities, "profiles": profiles}, nil
}

// credentialID is the session credential tokens are bound to.
func (req *request) credentialID() string { return hex.EncodeToString(req.principal.cred.Hash) }

func (req *request) mediaPost() (int, any, error) {
	if req.r.PathValue("a") == "s" || req.r.PathValue("b") != "play" {
		return 0, nil, errNotFound
	}
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	var body media.PlayRequest
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	if body.Force != "" && body.Force != "compat" {
		return 0, nil, invalid(`force must be "compat".`)
	}
	if body.StartMS < 0 {
		return 0, nil, invalid("start_ms must not be negative.")
	}
	if len(body.Capabilities) > 128 {
		return 0, nil, invalid("Too many capabilities.")
	}
	for _, v := range body.Capabilities {
		if v != "" && v != "maybe" && v != "probably" {
			return 0, nil, invalid(`Capabilities are canPlayType answers: "", "maybe" or "probably".`)
		}
	}
	lib, err := req.library()
	if err != nil {
		return 0, nil, err
	}
	ep, err := req.mediaEpisode(lib, req.r.PathValue("a"))
	if err != nil {
		return 0, nil, err
	}
	d, err := req.s.cfg.Media.Play(req.ctx, ep, body)
	var me *media.Error
	switch {
	case errors.As(err, &me):
		return req.mediaFail(me)
	case errors.Is(err, media.ErrInvalid):
		return 0, nil, invalid("The request is not valid.")
	case err != nil:
		return 0, nil, err
	}
	// The library was read with the caller checked; a session that ended
	// while the token was made gets no descriptor, and its token goes.
	if err := req.checkCaller(); err != nil {
		req.s.dropEndedMediaSessions(req.ctx)
		return 0, nil, err
	}
	return http.StatusOK, d, nil
}

// episodeSource is the enclosure the relay fetches (PROTOCOL §13): the
// canonical metadata.media.url, else the occurrence's enclosure_url.
func episodeSource(e *views.Episode) (source, declared string) {
	if m, ok := e.Records["metadata"].Field("media").(map[string]any); ok {
		source, _ = m["url"].(string)
		declared, _ = m["mime_type"].(string)
	}
	if source == "" {
		source, _ = e.Occurrence["enclosure_url"].(string)
	}
	return source, declared
}

// mediaEpisode resolves an episode the account holds for the descriptor.
func (req *request) mediaEpisode(lib *views.Library, id string) (media.Episode, error) {
	e := lib.ByID[id]
	if e == nil {
		return media.Episode{}, errNoEntity
	}
	source, declared := episodeSource(e)
	if source == "" {
		return media.Episode{}, fail(http.StatusNotFound, "not_found", "This episode has no media file.")
	}
	creds, err := req.feedCredentials(e.Podcast)
	if err != nil {
		return media.Episode{}, err
	}
	return media.Episode{Account: req.principal.accountID(), Credential: req.credentialID(), ID: e.ID,
		SourceURL: source, DeclaredType: declared, DurationMS: e.Duration, SecretID: secretID(e.Podcast), Credentials: creds}, nil
}

// secretID is the podcast's current credential secret, "" without.
func secretID(p *views.Podcast) string {
	id, _ := p.Records["credentials"].Field("secret_id").(string)
	return id
}

// feedCredentials are the podcast's credentials with the origins they may
// go to (PROTOCOL §9.4): the secret's approved origins, else the origin of the URL
// the user had set when the secret was set.
func (req *request) feedCredentials(p *views.Podcast) (*feed.Credentials, error) {
	id := secretID(p)
	svc := req.s.cfg.Protocol
	if id == "" || svc == nil || svc.Box == nil {
		return nil, nil
	}
	account := req.principal.accountID()
	v, err := svc.SecretValue(req.ctx, account, id)
	if err != nil {
		return nil, nil // as the refresh does: without credentials the publisher says what it wants
	}
	c := &feed.Credentials{Revision: id}
	c.Username, _ = v["username"].(string)
	c.Password, _ = v["password"].(string)
	if list, ok := v["approved_origins"].([]any); ok {
		c.ApprovedOrigins = []string{}
		for _, o := range list {
			if s, ok := o.(string); ok {
				c.ApprovedOrigins = append(c.ApprovedOrigins, s)
			}
		}
		return c, nil
	}
	var credURL string
	err = svc.ServerCommit(req.ctx, account, func(ac *protocol.ApplyContext) error {
		var err error
		credURL, err = ops.CredentialURL(ac, p.ID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if u, err := url.Parse(credURL); err == nil && u.Host != "" {
		c.ApprovedOrigins = []string{feed.Origin(u)}
	}
	return c, nil
}

func (req *request) mediaGet() (int, any, error) {
	a, b := req.r.PathValue("a"), req.r.PathValue("b")
	if a == "s" {
		return req.mediaStream(b)
	}
	switch b {
	case media.AssetImage, media.AssetChapters, media.AssetTranscript:
		return req.episodeAsset(a, b)
	}
	return 0, nil, errNotFound
}

// mediaToken finds the session's token and re-checks it: the episode must
// still be the account's, with the same enclosure and credentials.
func (req *request) mediaToken(id string) (media.Token, error) {
	m := req.s.cfg.Media
	k, ok := m.Lookup(id, req.principal.accountID(), req.credentialID())
	if !ok {
		return k, errNotFound
	}
	lib, err := req.library()
	if err != nil {
		return k, err
	}
	e := lib.ByID[k.Episode()]
	if e == nil {
		return k, errNotFound
	}
	source, _ := episodeSource(e)
	if normalized, _ := media.NormalizeSource(source); normalized != k.Source() || secretID(e.Podcast) != k.CredentialRevision() {
		return k, m.Changed(k)
	}
	return k, nil
}

func (req *request) mediaStream(id string) (int, any, error) {
	if _, err := req.query("start_ms"); err != nil {
		return 0, nil, err
	}
	k, err := req.mediaToken(id)
	var me *media.Error
	if errors.As(err, &me) {
		return req.mediaFail(me)
	}
	if err != nil {
		return 0, nil, err
	}
	if e := req.s.cfg.Media.Serve(req.w, req.r, k); e != nil {
		return req.mediaFail(e)
	}
	return 0, streamed{}, nil
}

func (req *request) mediaStatus() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	m := req.s.cfg.Media
	k, ok := m.Lookup(req.r.PathValue("token"), req.principal.accountID(), req.credentialID())
	if !ok {
		return 0, nil, errNotFound
	}
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	return http.StatusOK, m.Status(req.ctx, k), nil
}

// episodeAsset serves an episode's artwork (its own, else its podcast's, as
// Android shows), chapters or transcript (PROTOCOL §13).
func (req *request) episodeAsset(id, kind string) (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	lib, err := req.library()
	if err != nil {
		return 0, nil, err
	}
	e := lib.ByID[id]
	if e == nil {
		return 0, nil, errNoEntity
	}
	meta := e.Records["metadata"]
	var source, declared string
	switch kind {
	case media.AssetImage:
		source, _ = meta.Field("image_url").(string)
		if source == "" {
			source = e.Podcast.ImageURL
		}
	case media.AssetChapters:
		source, _ = meta.Field("chapter_url").(string)
	case media.AssetTranscript:
		source, _ = meta.Field("transcript_url").(string)
		declared, _ = meta.Field("transcript_type").(string)
	}
	return req.asset(e.Podcast, kind, source, declared)
}

// podcastImage serves a podcast's artwork.
func (req *request) podcastImage() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	lib, err := req.library()
	if err != nil {
		return 0, nil, err
	}
	p := lib.Podcasts[req.r.PathValue("id")]
	if p == nil {
		return 0, nil, errNoEntity
	}
	return req.asset(p, media.AssetImage, p.ImageURL, "")
}

func (req *request) asset(p *views.Podcast, kind, source, declared string) (int, any, error) {
	if source == "" {
		return 0, nil, fail(http.StatusNotFound, "not_found", "This episode has no "+kind+".")
	}
	creds, err := req.feedCredentials(p)
	if err != nil {
		return 0, nil, err
	}
	if e := req.s.cfg.Media.Asset(req.w, req.r, req.principal.accountID(), kind, source, declared, creds); e != nil {
		return req.mediaFail(e)
	}
	return 0, streamed{}, nil
}

// The compatibility path's check, for the service page (PROTOCOL §13).

func (req *request) adminMedia() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	// The administrator is checked again before the answer.
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"compat": req.s.cfg.Media.MatrixStatus()}, nil
}

func (req *request) adminMediaVerify() (int, any, error) {
	if err := req.decodeEmpty(); err != nil {
		return 0, nil, err
	}
	// The administrator is checked again just before the work starts.
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	req.s.cfg.Media.Recheck()
	return http.StatusAccepted, map[string]any{"compat": req.s.cfg.Media.MatrixStatus()}, nil
}
