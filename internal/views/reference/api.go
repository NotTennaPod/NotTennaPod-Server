package reference

import (
	"encoding/json"
	"net/url"
)

// The read API's responses as the server composed them from Load's library
// before the read model (api/read.go at 9ee1e1d), apart from the head and
// what does not come from the library (installations, the import, the
// credentials' username, an episode's full metadata record): the values the
// query matrix compares the served bytes with. List items are as the spool wrote them
// (json.Marshal), the rest as the handlers returned them.

// ListItems are a list endpoint's items for its path (a podcast's list:
// "/v1/read/podcasts/{id}/episodes" with the podcast given) and query (the
// parameters a first request takes), encoded as the spool encoded them,
// and the counters its view shows; ok false for a query the old server
// refused or a podcast it did not show.
func (l *Library) ListItems(path, podcast string, q url.Values) (items []json.RawMessage, counters json.RawMessage, ok bool) {
	var rows []any
	order := func(def string) (string, bool) {
		s := q.Get("sort")
		if s == "" {
			return def, true
		}
		return s, Orders[s]
	}
	episodes := func(list []*Episode) {
		for _, e := range list {
			rows = append(rows, EpisodeJSON(e))
		}
	}
	switch {
	case path == "/v1/read/subscriptions":
		o, err := ParseSubscriptionOptions(q.Get("state"), q.Get("order"), q.Get("counter"), q.Get("filter"))
		if err != nil {
			return nil, nil, false
		}
		for _, r := range l.Subscriptions(o) {
			p := PodcastJSON(r.Podcast)
			p["counter"] = r.Counter
			rows = append(rows, p)
		}
	case path == "/v1/read/episodes":
		f, err := ParseFilter(q.Get("filter"))
		s, sok := order("date_new_old")
		if err != nil || !sok {
			return nil, nil, false
		}
		if f.Favorite {
			f = f.AllStates()
		}
		episodes(l.EpisodesWhere(f, s, ""))
	case path == "/v1/read/queue":
		episodes(l.Queue)
	case path == "/v1/read/inbox":
		s, sok := order("date_new_old")
		if !sok {
			return nil, nil, false
		}
		episodes(l.EpisodesWhere(Filter{New: true}, s, ""))
	case path == "/v1/read/favorites":
		s, sok := order("date_new_old")
		if !sok {
			return nil, nil, false
		}
		episodes(l.EpisodesWhere(Filter{Favorite: true}.AllStates(), s, ""))
	case path == "/v1/read/history":
		episodes(l.EpisodesWhere(Filter{InHistory: true}.AllStates(), "completion_date_new_old", ""))
	default: // /v1/read/podcasts/{id}/episodes
		id := podcast
		if l.Podcasts[id] == nil {
			return nil, nil, false
		}
		f, err := ParseFilter(q.Get("filter"))
		s, sok := order("date_new_old")
		if err != nil || !sok {
			return nil, nil, false
		}
		episodes(l.EpisodesWhere(f.AllStates(), s, id))
	}
	for _, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			return nil, nil, false
		}
		items = append(items, b)
	}
	counters, err := json.Marshal(map[string]any{"counters": l.Counters()})
	if err != nil {
		return nil, nil, false
	}
	return items, counters, true
}

func statisticsRow(r PodcastStatistics) map[string]any {
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

// PodcastBody is a podcast view's podcast, with the credentials the handler
// added (as given) and its statistics card; ok false when not shown.
func (l *Library) PodcastBody(id string, credentials map[string]any, now int64) (map[string]any, bool) {
	p := l.Podcasts[id]
	if p == nil {
		return nil, false
	}
	out := PodcastJSON(p)
	out["credentials"] = credentials
	st := l.Statistics(0, 1<<63-1, true, now)
	for _, r := range st.Podcasts {
		if r.Podcast == p {
			out["statistics"] = statisticsRow(r)
		}
	}
	return out, true
}

// EpisodeBody is an episode view's episode (with the metadata record
// given) and podcast; ok false when not shown.
func (l *Library) EpisodeBody(id string, metadata json.RawMessage) (episode, podcast map[string]any, ok bool) {
	e := l.ByID[id]
	if e == nil {
		return nil, nil, false
	}
	out := EpisodeJSON(e)
	if metadata != nil {
		out["metadata"] = metadata
	}
	return out, PodcastJSON(e.Podcast), true
}

// StatisticsBody is the statistics view without its head.
func (l *Library) StatisticsBody(from, to int64, marked bool, now int64) map[string]any {
	st := l.Statistics(from, to, marked, now)
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
	return map[string]any{"counters": l.Counters(), "podcasts": rows, "months": months, "played_ms": total, "oldest_date": st.OldestDate}
}

// HomeBody is the Home view without its head; Surprise by its
// specification (SurpriseF5).
func (l *Library) HomeBody(now, seed int64, inboxOrder string, marked bool) map[string]any {
	h := l.Home(now, seed, inboxOrder, marked)
	items := func(list []*Episode) []any {
		out := make([]any, len(list))
		for i, e := range list {
			out[i] = EpisodeJSON(e)
		}
		return out
	}
	classics := []any{}
	for _, p := range h.Classics {
		classics = append(classics, PodcastJSON(p))
	}
	return map[string]any{"counters": l.Counters(), "continue": items(h.Continue), "new": items(h.New), "new_count": h.NewCount,
		"surprise": items(l.SurpriseF5(now, seed)), "classics": classics}
}

// SummaryBody is the summary view's values from the library: counters,
// policies and the account's records.
func (l *Library) SummaryBody(defaultLocation, defaultOrder string) map[string]any {
	discovery := "inbox"
	if v, ok := l.Account["discovery_policy"].Field("new_episodes_action").(string); ok {
		discovery = v
	}
	queue := map[string]any{"enqueue_location": defaultLocation, "keep_sorted": false, "sort_order": defaultOrder}
	if r, ok := l.Account["queue_policy"]; ok {
		for _, k := range []string{"enqueue_location", "keep_sorted", "sort_order"} {
			queue[k] = r.Field(k)
		}
	}
	return map[string]any{"counters": l.Counters(), "policies": map[string]any{"new_episodes_action": discovery, "queue": queue}, "records": l.Account}
}
