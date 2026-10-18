package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
)

// The read API (PROTOCOL §12): server-computed views for the browser,
// each at one head. Only a web session bound to its web installation reads;
// the phones never do.
func (s *Server) readRoutes() {
	if s.cfg.Views == nil {
		return
	}
	get := func(path string, h func(*request) (int, any, error)) {
		s.route(path, endpoint{method: http.MethodGet, auth: authRead, handle: h})
	}
	get("/v1/read/summary", (*request).readSummary)
	get("/v1/read/subscriptions", (*request).readSubscriptions)
	get("/v1/read/podcasts/{id}", (*request).readPodcast)
	get("/v1/read/podcasts/{id}/episodes", (*request).readPodcastEpisodes)
	get("/v1/read/episodes", (*request).readEpisodes)
	get("/v1/read/episodes/{id}", (*request).readEpisode)
	get("/v1/read/queue", (*request).readQueue)
	get("/v1/read/inbox", (*request).readInbox)
	get("/v1/read/favorites", (*request).readFavorites)
	get("/v1/read/history", (*request).readHistory)
	get("/v1/read/statistics", (*request).readStatistics)
	get("/v1/read/home", (*request).readHome)
	get("/v1/read/frontier", (*request).readFrontier)
	s.route("/v1/admin/read-model/verify", endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).verifyReadModel})
}

// verifyReadModel checks every account's cached library against a full
// build (views.Cache.Verify; a mismatch purges the account) and answers each
// account's result.
func (req *request) verifyReadModel() (int, any, error) {
	if err := req.decodeEmpty(); err != nil {
		return 0, nil, err
	}
	// The administrator is checked again just before the work starts.
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	out := map[string]string{}
	for _, account := range req.s.libraries.Accounts() {
		result, err := req.s.libraries.Verify(req.ctx, account)
		if err != nil {
			return 0, nil, err
		}
		out[account] = result
	}
	return http.StatusOK, map[string]any{"accounts": out}, nil
}

var (
	errViewExpired = fail(http.StatusGone, "view_expired", "This list changed; load it again.")
	errNoEntity    = fail(http.StatusNotFound, "not_found", "Not found.")
)

const (
	defaultPage = 100
	maxPage     = 500
)

// headJSON is a view's head: the cursor a push names, and its parts.
func headJSON(h storage.Head) (map[string]any, error) {
	c, err := contract.Cursor{LogID: h.LogID, Epoch: h.Epoch, Seq: h.Seq, Digest: h.Digest}.Encode()
	if err != nil {
		return nil, err
	}
	return map[string]any{"cursor": c, "log_id": h.LogID, "epoch": h.Epoch, "seq": h.Seq}, nil
}

// read runs f in a read transaction that first checks the caller again
// (PROTOCOL §5 revalidate, extended to reads): a revocation, reset,
// disable or demotion that landed after authentication wins over content
// read in the same transaction.
func (req *request) read(f func(r *storage.ReadTx) error) error {
	return req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		if _, err := req.revalidateRead(r); err != nil {
			return err
		}
		return f(r)
	})
}

// library loads the account's library at one snapshot: its version at the
// snapshot's view key (views.Cache).
func (req *request) library() (*views.Library, error) { return req.libraryRead(nil) }

// libraryRead runs f in a read transaction that checked the caller again at
// its start and waited for nothing, with the account's library at that
// snapshot (views.Cache.Get; a request that finds another building the
// account's library releases its reader, waits and starts again with the
// caller checked again). The list views are told the library's identity (a
// newer lineage or generation expires the older ones' views).
func (req *request) libraryRead(f func(r *storage.ReadTx, lib *views.Library) error) (*views.Library, error) {
	account := req.principal.accountID()
	lib, err := req.s.libraries.Get(req.ctx, account, func(r *storage.ReadTx) error {
		_, err := req.revalidateRead(r)
		return err
	}, func(r *storage.ReadTx, lib *views.Library) error {
		if req.s.cfg.Views != nil {
			req.s.cfg.Views.Observe(account, lib)
		}
		if f != nil {
			return f(r, lib)
		}
		return nil
	})
	if errors.Is(err, views.ErrBusy) {
		return nil, errBusy
	}
	return lib, err
}

// admitted reads the account's library (libraryRead) and admits a response
// made from it (views.Cache.Admit), which pins it until the hold is
// released: at once when a slot is free, else after dropping the library,
// waiting for a slot and reading it again. The caller releases the hold.
func (req *request) admitted(read func(r *storage.ReadTx, lib *views.Library) error) (*views.Library, *views.Hold, error) {
	account := req.principal.accountID()
	lib, err := req.libraryRead(read)
	if err != nil {
		return nil, nil, err
	}
	if h, ok := req.s.libraries.TryAdmit(account); ok {
		h.Pin(lib)
		return lib, h, nil
	}
	lib = nil // never pinned while waiting
	h, err := req.s.libraries.Admit(req.ctx, account)
	if errors.Is(err, views.ErrBusy) {
		return nil, nil, errBusy
	}
	if err != nil {
		return nil, nil, err
	}
	if lib, err = req.libraryRead(read); err != nil {
		h.Release()
		return nil, nil, err
	}
	h.Pin(lib)
	return lib, h, nil
}

// screen serves a response made from the account's library (read in
// libraryRead, then made by respond), sent only if the library is still
// valid once it is made: a purge since it was read discards it, and it is
// made again from a new snapshot (views.Cache.Valid).
func (req *request) screen(read func(r *storage.ReadTx, lib *views.Library) error, respond func(lib *views.Library) (int, any, error)) (int, any, error) {
	for range 3 {
		lib, h, err := req.admitted(read)
		if err != nil {
			return 0, nil, err
		}
		status, body, err := respond(lib)
		valid := req.s.libraries.Valid(req.principal.accountID(), lib)
		h.Release()
		if valid {
			return status, body, err
		}
	}
	return 0, nil, errBusy
}

// query returns the request's query values, refusing unknown names.
func (req *request) query(allowed ...string) (url.Values, error) {
	q := req.r.URL.Query()
	for k, v := range q {
		ok := false
		for _, a := range allowed {
			ok = ok || k == a
		}
		if !ok {
			return nil, invalid("Unknown parameter: " + k)
		}
		if len(v) != 1 {
			return nil, invalid("Repeated parameter: " + k)
		}
	}
	return q, nil
}

// anchor is a first request's window (PROTOCOL §12): the list read around
// an episode (or, when the version no longer stores it, an index), with
// before rows ahead of it.
type anchor struct {
	set    bool
	id     string
	index  int
	before int
}

const maxBefore = 200

// parseAnchor reads a list's anchor parameters.
func parseAnchor(q url.Values) (anchor, error) {
	var a anchor
	a.id = q.Get("anchor")
	if s := q.Get("anchor_index"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return a, invalid("anchor_index must be an index.")
		}
		a.index = n
	}
	if s := q.Get("before"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n > maxBefore {
			return a, invalid("before must be 0 to " + strconv.Itoa(maxBefore) + ".")
		}
		a.before = n
	}
	a.set = q.Has("anchor") || q.Has("anchor_index") || q.Has("before")
	if a.set && q.Get("page_token") != "" {
		return a, invalid("A page token is not anchored.")
	}
	return a, nil
}

// windowParams are a list's paging parameters, which its view does not
// depend on.
var windowParams = []string{"page_token", "limit", "anchor", "anchor_index", "before"}

// list serves a list view: the first request plans the list at one head and
// registers the plan; page tokens read it at any offset (PROTOCOL §12). An
// anchored first request reads a window around its anchor, and says where
// the window is (offset) with tokens for the page before it and the start.
func (req *request) list(q url.Values, build func(lib *views.Library) (views.Source, error)) (int, any, error) {
	limit := defaultPage
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxPage {
			return 0, nil, invalid("limit must be 1 to " + strconv.Itoa(maxPage) + ".")
		}
		limit = n
	}
	win, err := parseAnchor(q)
	if err != nil {
		return 0, nil, err
	}
	params := req.r.URL.Path + "?" + viewParams(q)
	account := req.principal.accountID()
	spool := req.s.cfg.Views
	var page views.Page
	var readErr error
	if token := q.Get("page_token"); token != "" {
		id, offset, err := views.ParsePageToken(token)
		if err != nil {
			return 0, nil, invalid("The page token is not valid.")
		}
		// The account's identity now (two point reads, with the caller
		// checked again): a view of an older lineage or generation expires
		// at once, whether or not another request has seen the new one.
		cur, err := req.identity()
		if err != nil {
			return 0, nil, err
		}
		req.s.libraries.Touch(account)
		page, readErr = spool.Read(account, id, params, offset, limit, nil, cur) // admitted inside
		if readErr == nil {
			readErr = req.final(page)
		}
	} else {
		// The view is registered under the incarnation read before the
		// library: a purge in between refuses it (or expires it before its
		// page is released), and the request starts again from a new
		// snapshot; so does an identity changed by the time the page is
		// made.
		for attempt := 0; ; attempt++ {
			inc := spool.Incarnation(account)
			lib, h, err := req.admitted(nil)
			if err != nil {
				return 0, nil, err
			}
			src, err := build(lib)
			if err != nil {
				h.Release()
				return 0, nil, err
			}
			id, err := spool.Put(account, inc, lib, params, src, map[string]any{"counters": lib.Counters()})
			if errors.Is(err, views.ErrRetired) {
				h.Release()
				if attempt < 3 {
					continue
				}
				return 0, nil, errBusy
			}
			if err != nil {
				h.Release()
				return 0, nil, err
			}
			offset, at := 0, (*views.Cursor)(nil)
			if l, ok := src.(*views.EpisodeList); ok && win.set {
				c, _ := l.Anchor(win.id, win.index, win.before)
				offset, at = c.Offset, &c
			}
			page, readErr = spool.ReadHeld(h, account, id, params, offset, limit, at, lib.Identity())
			h.Release()
			if readErr == nil {
				readErr = req.final(page)
			}
			if errors.Is(readErr, views.ErrExpired) && attempt < 3 {
				continue
			}
			break
		}
	}
	switch err := readErr; {
	case errors.Is(err, views.ErrBusy):
		return 0, nil, errBusy
	case errors.Is(err, views.ErrExpired):
		return 0, nil, errViewExpired
	case errors.Is(err, views.ErrParams):
		return 0, nil, invalid("The page token belongs to a list with other parameters.")
	case err != nil:
		return 0, nil, err
	}
	head, err := headJSON(page.Head)
	if err != nil {
		return 0, nil, err
	}
	items := page.Items
	if items == nil {
		items = []json.RawMessage{}
	}
	var next any
	if page.Next != "" {
		next = page.Next
	}
	body := map[string]any{"head": head, "view": page.Extra, "total": page.Total, "items": items, "next_page_token": next}
	if win.set {
		var prev any
		if page.Offset > 0 {
			prev = views.PageToken(page.ViewID, max(0, page.Offset-limit))
		}
		body["offset"], body["prev_page_token"], body["start_page_token"] = page.Offset, prev, views.PageToken(page.ViewID, 0)
	}
	return http.StatusOK, body, nil
}

// viewParams is the canonical form of a list's parameters without its
// paging parameters (page tokens bind them).
func viewParams(q url.Values) string {
	var keys []string
	for k := range q {
		if !slices.Contains(windowParams, k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(url.QueryEscape(k) + "=" + url.QueryEscape(q.Get(k)) + "&")
	}
	return b.String()
}

// identity is the account's current identity (views.CurrentIdentity), read
// with the caller checked again.
func (req *request) identity() (views.Identity, error) {
	var cur views.Identity
	err := req.read(func(r *storage.ReadTx) error {
		var err error
		cur, err = views.CurrentIdentity(r.Account(req.principal.accountID()))
		return err
	})
	return cur, err
}

// final is a page's last checks, after it was made: the caller is checked
// again, and a page of a view whose account has since changed its lineage
// (a restore) or generation (an import, a rebuild) is not served
// (views.ErrExpired). The registry is the ordering authority, asked last:
// such a commit, like a purge, retires the account's views before its write
// returns (Server.New), so it is either seen here or ordered after the
// response; the identity read in a snapshot of its own could predate it.
func (req *request) final(p views.Page) error {
	cur, err := req.identity()
	if err != nil {
		return err
	}
	if cur != p.Identity || !req.s.cfg.Views.Current(req.principal.accountID(), p) {
		return views.ErrExpired
	}
	return nil
}

// episodeRows are rows of the library's version.
func episodeRows(lib *views.Library, list []*views.EpisodeItem) ([]any, error) {
	out := make([]any, len(list))
	for i, e := range list {
		m, err := views.EpisodeJSON(lib.V, e)
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

func parseFilter(s string) (views.Filter, error) {
	f, err := views.ParseFilter(s)
	switch {
	case errors.Is(err, views.ErrDownloads):
		return f, invalid("Downloads stay on your devices; the browser cannot filter by them.")
	case err != nil:
		return f, invalid("Unknown filter.")
	}
	return f, nil
}

func parseSort(s, def string) (string, error) {
	if s == "" {
		return def, nil
	}
	if !views.Orders[s] {
		return "", invalid("Unknown sort order.")
	}
	return s, nil
}

func parseBool(s string) (bool, error) {
	switch s {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	}
	return false, invalid("Use true or false.")
}

// ---- screens

func (req *request) readSummary() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	var insts []storage.Installation
	var imp *storage.Import
	return req.screen(func(r *storage.ReadTx, _ *views.Library) error {
		acct := r.Account(req.principal.accountID())
		var err error
		imp = nil
		if insts, err = acct.Installations(); err != nil {
			return err
		}
		cur, err := acct.CurrentImport()
		if err == nil {
			imp = &cur
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return nil
	}, func(lib *views.Library) (int, any, error) { return req.summary(lib, insts, imp) })
}

func (req *request) summary(lib *views.Library, insts []storage.Installation, imp *storage.Import) (int, any, error) {
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	devices := []any{}
	for _, i := range insts {
		devices = append(devices, installationJSON(i, req.principal.cred.InstallID))
	}
	var importStatus any
	if imp != nil {
		importStatus = map[string]any{"import_id": imp.ImportID, "status": imp.Status, "kind": imp.Kind}
	}
	account, err := lib.AccountRecords()
	if err != nil {
		return 0, nil, err
	}
	discovery := "inbox"
	if v, ok := account["discovery_policy"].Field("new_episodes_action").(string); ok {
		discovery = v
	}
	queue := map[string]any{"enqueue_location": projection.DefaultEnqueueLocation, "keep_sorted": false, "sort_order": projection.DefaultSortOrder}
	if r, ok := account["queue_policy"]; ok {
		for _, k := range []string{"enqueue_location", "keep_sorted", "sort_order"} {
			queue[k] = r.Field(k)
		}
	}
	return http.StatusOK, map[string]any{
		"head": head, "counters": lib.Counters(), "account_id": req.principal.accountID(),
		"policies":      map[string]any{"new_episodes_action": discovery, "queue": queue},
		"records":       account,
		"installations": devices, "import": importStatus,
	}, nil
}

func (req *request) readSubscriptions() (int, any, error) {
	q, err := req.query("state", "order", "counter", "filter", "page_token", "limit")
	if err != nil {
		return 0, nil, err
	}
	o, err := views.ParseSubscriptionOptions(q.Get("state"), q.Get("order"), q.Get("counter"), q.Get("filter"))
	if err != nil {
		return 0, nil, invalid("Unknown subscription list option.")
	}
	return req.list(q, func(lib *views.Library) (views.Source, error) { return lib.SubscriptionView(o), nil })
}

func (req *request) readEpisodesList(q url.Values, f views.Filter, order, podcast string) (int, any, error) {
	return req.list(q, func(lib *views.Library) (views.Source, error) {
		if podcast != "" && lib.Podcast(podcast) == nil {
			return nil, errNoEntity
		}
		return lib.Episodes(views.ListQuery{Filter: f, Order: order, Podcast: podcast}), nil
	})
}

// readEpisodes is Android's Episodes screen: its filter (favorites include
// every podcast state, as AllEpisodesFragment does) and sort order.
func (req *request) readEpisodes() (int, any, error) {
	q, err := req.query("filter", "sort", "page_token", "limit", "anchor", "anchor_index", "before")
	if err != nil {
		return 0, nil, err
	}
	f, err := parseFilter(q.Get("filter"))
	if err != nil {
		return 0, nil, err
	}
	if f.Favorite {
		f = f.AllStates()
	}
	order, err := parseSort(q.Get("sort"), "date_new_old")
	if err != nil {
		return 0, nil, err
	}
	return req.readEpisodesList(q, f, order, "")
}

// readPodcastEpisodes is a podcast's episode list: every feed state.
func (req *request) readPodcastEpisodes() (int, any, error) {
	q, err := req.query("filter", "sort", "page_token", "limit", "anchor", "anchor_index", "before")
	if err != nil {
		return 0, nil, err
	}
	f, err := parseFilter(q.Get("filter"))
	if err != nil {
		return 0, nil, err
	}
	order, err := parseSort(q.Get("sort"), "date_new_old")
	if err != nil {
		return 0, nil, err
	}
	return req.readEpisodesList(q, f.AllStates(), order, req.r.PathValue("id"))
}

func (req *request) readInbox() (int, any, error) {
	q, err := req.query("sort", "page_token", "limit", "anchor", "anchor_index", "before")
	if err != nil {
		return 0, nil, err
	}
	order, err := parseSort(q.Get("sort"), "date_new_old")
	if err != nil {
		return 0, nil, err
	}
	return req.readEpisodesList(q, views.Filter{New: true}, order, "")
}

func (req *request) readFavorites() (int, any, error) {
	q, err := req.query("sort", "page_token", "limit", "anchor", "anchor_index", "before")
	if err != nil {
		return 0, nil, err
	}
	order, err := parseSort(q.Get("sort"), "date_new_old")
	if err != nil {
		return 0, nil, err
	}
	return req.readEpisodesList(q, views.Filter{Favorite: true}.AllStates(), order, "")
}

// readHistory is Android's playback history: every podcast state, newest first.
func (req *request) readHistory() (int, any, error) {
	q, err := req.query("page_token", "limit", "anchor", "anchor_index", "before")
	if err != nil {
		return 0, nil, err
	}
	return req.readEpisodesList(q, views.Filter{InHistory: true}.AllStates(), "completion_date_new_old", "")
}

func (req *request) readQueue() (int, any, error) {
	q, err := req.query("page_token", "limit", "anchor", "anchor_index", "before")
	if err != nil {
		return 0, nil, err
	}
	return req.list(q, func(lib *views.Library) (views.Source, error) { return lib.Queue(), nil })
}

func (req *request) readPodcast() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	return req.screen(nil, req.podcast)
}

func (req *request) podcast(lib *views.Library) (int, any, error) {
	p := lib.Podcast(req.r.PathValue("id"))
	if p == nil {
		return 0, nil, errNoEntity
	}
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	out, err := views.PodcastJSON(p)
	if err != nil {
		return 0, nil, err
	}
	if p.HasCredentials {
		creds := map[string]any{"set": true, "username": nil}
		if id := p.SecretID; id != "" && req.s.cfg.Protocol != nil {
			if v, err := req.s.cfg.Protocol.SecretValue(req.ctx, req.principal.accountID(), id); err == nil {
				creds["username"] = v["username"]
			}
		}
		out["credentials"] = creds
	} else {
		out["credentials"] = map[string]any{"set": false}
	}
	// The feed statistics card: all time, marked as played included (Android's
	// FeedStatisticsFragment), for a podcast the statistics screen lists.
	if r, ok := lib.PodcastStatistics(p); ok {
		out["statistics"] = statisticsRow(r)
	}
	return http.StatusOK, map[string]any{"head": head, "counters": lib.Counters(), "podcast": out}, nil
}

func (req *request) readEpisode() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	id := req.r.PathValue("id")
	var meta *storage.RecordRow
	return req.screen(func(r *storage.ReadTx, _ *views.Library) error {
		acct := r.Account(req.principal.accountID())
		meta = nil
		m, err := acct.Record("episode", id, "metadata")
		if err == nil {
			meta = &m
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return nil
	}, func(lib *views.Library) (int, any, error) { return episodeBody(lib, id, meta) })
}

func episodeBody(lib *views.Library, id string, meta *storage.RecordRow) (int, any, error) {
	e, p := lib.Episode(id)
	if e == nil {
		return 0, nil, errNoEntity
	}
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	out, err := views.EpisodeJSON(lib.V, e)
	if err != nil {
		return 0, nil, err
	}
	if meta != nil {
		// The whole metadata record, show notes included (sanitized by the browser).
		out["metadata"] = json.RawMessage(meta.Value)
	}
	pod, err := views.PodcastJSON(p)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"head": head, "counters": lib.Counters(), "episode": out, "podcast": pod}, nil
}

func statisticsRow(r views.PodcastStatistics) map[string]any {
	var oldest any
	if r.OldestDate > 0 {
		oldest = r.OldestDate
	}
	return map[string]any{
		"played_ms": r.Stats.PlayedMS, "played_marked_ms": r.Stats.PlayedMarkedMS,
		"episodes_started": r.Stats.EpisodesStarted, "episodes_started_marked": r.Stats.EpisodesStartedMarked,
		"episodes": r.Stats.Episodes, "total_ms": r.Stats.TotalMS, "oldest_date": oldest,
	}
}

func (req *request) readStatistics() (int, any, error) {
	q, err := req.query("from", "to", "include_marked")
	if err != nil {
		return 0, nil, err
	}
	from, to := int64(0), int64(1<<63-1)
	if s := q.Get("from"); s != "" {
		if from, err = strconv.ParseInt(s, 10, 64); err != nil {
			return 0, nil, invalid("from must be a time in milliseconds.")
		}
	}
	if s := q.Get("to"); s != "" {
		if to, err = strconv.ParseInt(s, 10, 64); err != nil {
			return 0, nil, invalid("to must be a time in milliseconds.")
		}
	}
	marked, err := parseBool(q.Get("include_marked"))
	if err != nil {
		return 0, nil, err
	}
	return req.screen(nil, func(lib *views.Library) (int, any, error) { return req.statistics(lib, from, to, marked) })
}

func (req *request) statistics(lib *views.Library, from, to int64, marked bool) (int, any, error) {
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	st := lib.Statistics(from, to, marked, req.s.cfg.Now().UnixMilli())
	rows := []any{}
	var total int64
	for _, r := range st.Podcasts {
		row := statisticsRow(r)
		row["podcast"] = map[string]any{"podcast_id": r.Podcast.ID, "title": r.Podcast.Title, "display_title": r.Podcast.DisplayTitle,
			"image_url": r.Podcast.ImageURL, "state": r.Podcast.State}
		rows = append(rows, row)
		if marked {
			total += r.Stats.PlayedMarkedMS
		} else {
			total += r.Stats.PlayedMS
		}
	}
	months := []any{}
	for _, m := range st.Months {
		months = append(months, m)
	}
	return http.StatusOK, map[string]any{"head": head, "counters": lib.Counters(), "podcasts": rows, "months": months,
		"played_ms": total, "oldest_date": st.OldestDate}, nil
}

func (req *request) readHome() (int, any, error) {
	q, err := req.query("seed", "inbox_sort", "include_marked")
	if err != nil {
		return 0, nil, err
	}
	var seed int64
	if s := q.Get("seed"); s != "" {
		if seed, err = strconv.ParseInt(s, 10, 64); err != nil {
			return 0, nil, invalid("seed must be an integer.")
		}
	}
	order, err := parseSort(q.Get("inbox_sort"), "date_new_old")
	if err != nil {
		return 0, nil, err
	}
	marked, err := parseBool(q.Get("include_marked"))
	if err != nil {
		return 0, nil, err
	}
	return req.screen(nil, func(lib *views.Library) (int, any, error) { return req.home(lib, seed, order, marked) })
}

func (req *request) home(lib *views.Library, seed int64, order string, marked bool) (int, any, error) {
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	h := lib.Home(req.s.cfg.Now().UnixMilli(), seed, order, marked)
	classics := []any{}
	for _, p := range h.Classics {
		m, err := views.PodcastJSON(p)
		if err != nil {
			return 0, nil, err
		}
		classics = append(classics, m)
	}
	body := map[string]any{"head": head, "counters": lib.Counters(), "new_count": h.NewCount, "classics": classics}
	for k, list := range map[string][]*views.EpisodeItem{"continue": h.Continue, "new": h.New, "surprise": h.Surprise} {
		if body[k], err = episodeRows(lib, list); err != nil {
			return 0, nil, err
		}
	}
	return http.StatusOK, body, nil
}

// readFrontier returns the causal frontier at a view's head, for a cut built
// on that view (PROTOCOL §12): exactly what the view's head held.
func (req *request) readFrontier() (int, any, error) {
	q, err := req.query("cursor")
	if err != nil {
		return 0, nil, err
	}
	c, err := contract.DecodeCursor(q.Get("cursor"))
	if err != nil {
		return 0, nil, invalid("The cursor is not valid.")
	}
	var frontier []string
	var cur storage.Head
	err = req.read(func(r *storage.ReadTx) error {
		acct := r.Account(req.principal.accountID())
		var err error
		if cur, err = acct.Head(); err != nil {
			return err
		}
		if cur.LogID != c.LogID || cur.Epoch != c.Epoch || c.Seq > cur.Seq {
			return errViewExpired
		}
		frontier, err = acct.FrontierAt(c.Epoch, c.Seq)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"cursor": q.Get("cursor"), "frontier": frontier}, nil
}
