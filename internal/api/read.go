package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
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

// library loads the account's library from one snapshot.
func (req *request) library() (*views.Library, error) {
	var lib *views.Library
	err := req.read(func(r *storage.ReadTx) error {
		var err error
		lib, err = req.s.libraries.Load(r.Account(req.principal.accountID()), req.principal.accountID())
		return err
	})
	return lib, err
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

// list serves a materialized list view: the first request computes the list
// at one head and spools it; page tokens read the spool (PROTOCOL §12).
func (req *request) list(q url.Values, build func(lib *views.Library) ([]any, map[string]any, error)) (int, any, error) {
	limit := defaultPage
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxPage {
			return 0, nil, invalid("limit must be 1 to " + strconv.Itoa(maxPage) + ".")
		}
		limit = n
	}
	params := req.r.URL.Path + "?" + viewParams(q)
	account := req.principal.accountID()
	spool := req.s.cfg.Views
	id, offset := "", 0
	if token := q.Get("page_token"); token != "" {
		var err error
		if id, offset, err = views.ParsePageToken(token); err != nil {
			return 0, nil, invalid("The page token is not valid.")
		}
	} else {
		lib, err := req.library()
		if err != nil {
			return 0, nil, err
		}
		items, extra, err := build(lib)
		if err != nil {
			return 0, nil, err
		}
		extra["counters"] = lib.Counters()
		if id, err = spool.Put(account, lib.Head, params, extra, items); err != nil {
			return 0, nil, err
		}
	}
	page, err := spool.Read(account, id, params, offset, limit)
	switch {
	case errors.Is(err, views.ErrExpired):
		return 0, nil, errViewExpired
	case errors.Is(err, views.ErrParams):
		return 0, nil, invalid("The page token belongs to a list with other parameters.")
	case err != nil:
		return 0, nil, err
	}
	if err := req.sameLineage(page.Head); err != nil {
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
	return http.StatusOK, map[string]any{"head": head, "view": page.Extra, "total": page.Total, "items": items, "next_page_token": next}, nil
}

// viewParams is the canonical form of a list's parameters (page tokens bind them).
func viewParams(q url.Values) string {
	var keys []string
	for k := range q {
		if k != "page_token" && k != "limit" {
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

// sameLineage refuses a view of a lineage the account no longer has (a
// restore). A list page read from its spool is gated by it, so the caller is
// checked again before the page is served.
func (req *request) sameLineage(h storage.Head) error {
	var cur storage.Head
	err := req.read(func(r *storage.ReadTx) error {
		var err error
		cur, err = r.Account(req.principal.accountID()).Head()
		return err
	})
	if err != nil {
		return err
	}
	if cur.LogID != h.LogID || cur.Epoch != h.Epoch {
		return errViewExpired
	}
	return nil
}

func episodeItems(list []*views.Episode) []any {
	out := make([]any, len(list))
	for i, e := range list {
		out[i] = views.EpisodeJSON(e)
	}
	return out
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
	var lib *views.Library
	var insts []storage.Installation
	var imp *storage.Import
	err := req.read(func(r *storage.ReadTx) error {
		acct := r.Account(req.principal.accountID())
		var err error
		if lib, err = req.s.libraries.Load(acct, req.principal.accountID()); err != nil {
			return err
		}
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
	})
	if err != nil {
		return 0, nil, err
	}
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
	discovery := "inbox"
	if v, ok := lib.Account["discovery_policy"].Field("new_episodes_action").(string); ok {
		discovery = v
	}
	queue := map[string]any{"enqueue_location": projection.DefaultEnqueueLocation, "keep_sorted": false, "sort_order": projection.DefaultSortOrder}
	if r, ok := lib.Account["queue_policy"]; ok {
		for _, k := range []string{"enqueue_location", "keep_sorted", "sort_order"} {
			queue[k] = r.Field(k)
		}
	}
	return http.StatusOK, map[string]any{
		"head": head, "counters": lib.Counters(), "account_id": req.principal.accountID(),
		"policies":      map[string]any{"new_episodes_action": discovery, "queue": queue},
		"records":       lib.Account,
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
	return req.list(q, func(lib *views.Library) ([]any, map[string]any, error) {
		rows := lib.Subscriptions(o)
		items := make([]any, len(rows))
		for i, r := range rows {
			p := views.PodcastJSON(r.Podcast)
			p["counter"] = r.Counter
			items[i] = p
		}
		return items, map[string]any{}, nil
	})
}

func (req *request) readEpisodesList(q url.Values, f views.Filter, order, podcast string) (int, any, error) {
	return req.list(q, func(lib *views.Library) ([]any, map[string]any, error) {
		if podcast != "" && lib.Podcasts[podcast] == nil {
			return nil, nil, errNoEntity
		}
		return episodeItems(lib.EpisodesWhere(f, order, podcast)), map[string]any{}, nil
	})
}

// readEpisodes is Android's Episodes screen: its filter (favorites include
// every podcast state, as AllEpisodesFragment does) and sort order.
func (req *request) readEpisodes() (int, any, error) {
	q, err := req.query("filter", "sort", "page_token", "limit")
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
	q, err := req.query("filter", "sort", "page_token", "limit")
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
	q, err := req.query("sort", "page_token", "limit")
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
	q, err := req.query("sort", "page_token", "limit")
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
	q, err := req.query("page_token", "limit")
	if err != nil {
		return 0, nil, err
	}
	return req.readEpisodesList(q, views.Filter{InHistory: true}.AllStates(), "completion_date_new_old", "")
}

func (req *request) readQueue() (int, any, error) {
	q, err := req.query("page_token", "limit")
	if err != nil {
		return 0, nil, err
	}
	return req.list(q, func(lib *views.Library) ([]any, map[string]any, error) {
		return episodeItems(lib.Queue), map[string]any{}, nil
	})
}

func (req *request) readPodcast() (int, any, error) {
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
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	out := views.PodcastJSON(p)
	if c, ok := p.Records["credentials"]; ok {
		creds := map[string]any{"set": true, "username": nil}
		if id, _ := c.Field("secret_id").(string); id != "" && req.s.cfg.Protocol != nil {
			if v, err := req.s.cfg.Protocol.SecretValue(req.ctx, req.principal.accountID(), id); err == nil {
				creds["username"] = v["username"]
			}
		}
		out["credentials"] = creds
	} else {
		out["credentials"] = map[string]any{"set": false}
	}
	// The feed statistics card: all time, marked as played included (Android's
	// FeedStatisticsFragment).
	st := lib.Statistics(0, 1<<63-1, true, req.s.cfg.Now().UnixMilli())
	for _, r := range st.Podcasts {
		if r.Podcast == p {
			out["statistics"] = statisticsRow(r)
		}
	}
	return http.StatusOK, map[string]any{"head": head, "counters": lib.Counters(), "podcast": out}, nil
}

func (req *request) readEpisode() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	id := req.r.PathValue("id")
	var lib *views.Library
	var meta *storage.RecordRow
	err := req.read(func(r *storage.ReadTx) error {
		acct := r.Account(req.principal.accountID())
		var err error
		if lib, err = req.s.libraries.Load(acct, req.principal.accountID()); err != nil {
			return err
		}
		m, err := acct.Record("episode", id, "metadata")
		if err == nil {
			meta = &m
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	e := lib.ByID[id]
	if e == nil {
		return 0, nil, errNoEntity
	}
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	out := views.EpisodeJSON(e)
	if meta != nil {
		// The whole metadata record, show notes included (sanitized by the browser).
		out["metadata"] = json.RawMessage(meta.Value)
	}
	return http.StatusOK, map[string]any{"head": head, "counters": lib.Counters(), "episode": out, "podcast": views.PodcastJSON(e.Podcast)}, nil
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
	lib, err := req.library()
	if err != nil {
		return 0, nil, err
	}
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
	lib, err := req.library()
	if err != nil {
		return 0, nil, err
	}
	head, err := headJSON(lib.Head)
	if err != nil {
		return 0, nil, err
	}
	h := lib.Home(req.s.cfg.Now().UnixMilli(), seed, order, marked)
	classics := []any{}
	for _, p := range h.Classics {
		classics = append(classics, views.PodcastJSON(p))
	}
	return http.StatusOK, map[string]any{"head": head, "counters": lib.Counters(),
		"continue": episodeItems(h.Continue), "new": episodeItems(h.New), "new_count": h.NewCount,
		"surprise": episodeItems(h.Surprise), "classics": classics}, nil
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
