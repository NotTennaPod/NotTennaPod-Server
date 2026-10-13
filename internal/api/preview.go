package api

import (
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The feed preview (PROTOCOL §12): Android's OnlineFeedViewActivity for the
// browser, which cannot fetch feeds itself (its content security policy
// allows only this server). The server fetches the address as Android does
// (feed.Discover): a feed, a page's feed links, or the error Android shows. A
// 401 names the origin that asked, so the browser asks for credentials for
// that origin, and they go only there. Every URL and text in the answer has
// URL credentials removed.
func (s *Server) previewRoutes() {
	if s.cfg.Views == nil {
		return
	}
	s.route("/v1/read/preview", endpoint{method: http.MethodPost, auth: authRead, handle: (*request).preview})
}

// previewItems is how many episodes a preview lists.
const previewItems = 500

// Previews running at a time, over the server and per account, as the refresh
// schedule bounds its fetches; beyond them a preview answers server_busy at
// once. Each can hold a 64 MiB response while it parses.
const (
	previewsServer     = 4
	previewsPerAccount = 2
)

var errPreviewBusy = &apiError{status: http.StatusServiceUnavailable, code: "server_busy",
	message: "The server is busy fetching other podcasts; try again shortly.", retryAfter: 2 * time.Second}

// admission bounds work running at a time, overall and per account.
type admission struct {
	mu        sync.Mutex
	max, per  int
	total     int
	byAccount map[string]int
}

func newAdmission(max, per int) *admission {
	return &admission{max: max, per: per, byAccount: map[string]int{}}
}

// take admits one more for account, or reports that the bound is reached.
func (a *admission) take(account string) (release func(), ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.total >= a.max || a.byAccount[account] >= a.per {
		return nil, false
	}
	a.total++
	a.byAccount[account]++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.total--
			if a.byAccount[account]--; a.byAccount[account] <= 0 {
				delete(a.byAccount, account)
			}
		})
	}, true
}

func (req *request) preview() (int, any, error) {
	var body struct {
		URL      string  `json:"url"`
		Username *string `json:"username"`
		Password *string `json:"password"`
		Origin   string  `json:"origin"` // the origin that asked for credentials (a 401's), which the user approved
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	var creds *feed.Credentials
	if body.Username != nil || body.Password != nil {
		if body.Username == nil || body.Password == nil {
			return 0, nil, invalid("Give both a username and a password.")
		}
		// The limits a push holds a secret to, so a preview never accepts
		// credentials the subscription then cannot sync.
		if len(*body.Username) > maxSecretField || len(*body.Password) > maxSecretField {
			return 0, nil, invalid("A username and a password are at most 4096 bytes each.")
		}
		creds = &feed.Credentials{Username: *body.Username, Password: *body.Password}
	}
	fetcher := req.s.cfg.Fetcher
	if fetcher == nil {
		fetcher = feed.NewFetcher()
	}
	release, ok := req.s.previews.take(req.principal.accountID())
	if !ok {
		return 0, nil, errPreviewBusy
	}
	defer release()
	prepared, err := feed.PrepareURL(body.URL)
	if err != nil {
		return 0, nil, invalid("This is not a podcast address.")
	}
	u, err := url.Parse(prepared)
	if err != nil {
		return 0, nil, invalid("This is not a podcast address.")
	}
	if creds != nil {
		// Credentials go only to the origin the user gave them for: the one
		// whose 401 asked (after redirects), else the address's own.
		origin := feed.Origin(u)
		if body.Origin != "" {
			o, err := url.Parse(body.Origin)
			if err != nil || (o.Scheme != "http" && o.Scheme != "https") || o.Host == "" || o.User != nil ||
				(o.Path != "" && o.Path != "/") || o.RawQuery != "" || feed.Origin(o) != body.Origin {
				return 0, nil, invalid("origin must be scheme://host:port as the 401 named it.")
			}
			origin = body.Origin
		}
		creds.ApprovedOrigins = []string{origin}
	}
	u.User = nil
	out := map[string]any{"url": u.String()}
	// The account's podcast at this address, if it has one: Android offers
	// to open it instead of subscribing again.
	var owner string
	if err := req.read(func(r *storage.ReadTx) error {
		var err error
		owner, err = r.Account(req.principal.accountID()).PodcastOwning(contract.NormalizeURL(u.String()))
		return err
	}); err != nil {
		return 0, nil, err
	}
	if owner != "" {
		out["podcast_id"] = owner
	}
	res, err := fetcher.Discover(req.ctx, body.URL, creds, req.s.cfg.Now())
	if errors.Is(err, feed.ErrAddress) {
		return 0, nil, invalid("This is not a podcast address.")
	}
	var fe *feed.FetchError
	if errors.As(err, &fe) {
		e := map[string]any{"label": fe.Label, "code": fe.Code, "detail": ops.StripUserinfo(fe.Detail)}
		if fe.Origin != "" {
			e["origin"] = fe.Origin
		}
		out["error"] = e
		return http.StatusOK, out, nil
	}
	if err != nil {
		return 0, nil, err
	}
	if res.Err != nil {
		out["error"] = map[string]any{"label": res.Err.Label, "detail": ops.StripUserinfo(res.Err.Error())}
	}
	if len(res.Links) > 0 {
		links := []any{}
		for _, l := range res.Links {
			links = append(links, map[string]any{"url": ops.StripUserinfo(l.URL), "title": ops.StripUserinfo(l.Title)})
		}
		out["links"] = links
	}
	if f := res.Feed; f != nil {
		items := []any{}
		for i, it := range f.Items {
			if i == previewItems {
				break
			}
			var media any
			if it.Media != nil && it.Media.URL != nil {
				media = map[string]any{"duration_ms": it.Media.Duration, "size": it.Media.Size, "mime_type": it.Media.MIMEType}
			}
			items = append(items, map[string]any{"title": scrubPtr(it.Title), "pub_date": it.PubDate, "description": scrubPtr(it.Description),
				"image_url": scrubPtr(it.ImageURL), "link": scrubPtr(it.Link), "media": media})
		}
		out["feed"] = map[string]any{"title": scrubPtr(f.Title), "author": scrubPtr(f.Author), "description": scrubPtr(f.Description), "language": f.Language,
			"link": scrubPtr(f.Link), "image_url": scrubPtr(f.ImageURL), "type": f.Type, "items": items, "item_count": len(f.Items)}
	}
	return http.StatusOK, out, nil
}

func scrubPtr(s *string) any {
	if s == nil {
		return nil
	}
	return ops.StripUserinfo(*s)
}
