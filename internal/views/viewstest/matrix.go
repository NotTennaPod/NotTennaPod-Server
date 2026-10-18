package viewstest

import (
	"net/url"
	"slices"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
)

// MatrixFilters are the query matrix's episode filters: the 19 Android filter screens'
// properties, then 16 fixed combinations.
var MatrixFilters = []string{
	"", "played", "unplayed", "new", "paused", "not_paused", "queued", "not_queued", "has_media", "no_media", "is_favorite", "not_favorite",
	"is_in_history,include_subscribed,include_archived,include_not_subscribed", "include_archived", "include_not_subscribed",
	"include_subscribed,include_archived,include_not_subscribed", "unplayed,paused,queued", "played,unplayed",
	"is_favorite,include_subscribed,include_archived,include_not_subscribed",
	"played,new", "unplayed,new", "paused,not_paused", "queued,not_queued", "has_media,no_media", "is_favorite,not_favorite",
	"is_in_history,no_media", "new,is_favorite,include_archived", "unplayed,has_media,not_queued", "played,is_in_history",
	"paused,is_favorite,include_not_subscribed", "include_archived,include_not_subscribed", "not_paused,not_queued,not_favorite",
	"new,queued", "no_media,include_subscribed,include_archived", "played,unplayed,new,paused,queued,has_media,is_favorite,is_in_history",
}

// Query is one list query of the matrix: an endpoint and its parameters, as
// the read API takes them; Podcast names a podcast's list.
type Query struct {
	Path, Podcast string
	Q             url.Values
}

func (q Query) String() string { return q.Path + "?" + q.Q.Encode() }

// QueryMatrix is the query matrix's list queries over a library's podcasts: every filter
// in every order over every podcast and over four fixed ones (the first,
// the last and two between, by ID), the Inbox and Favourites in every
// order, the history, the queue, and the subscription list's states,
// orders, counters and filters.
func QueryMatrix(podcasts []string) []Query {
	pods := slices.Sorted(slices.Values(podcasts))
	if len(pods) > 4 {
		pods = []string{pods[0], pods[len(pods)/3], pods[2*len(pods)/3], pods[len(pods)-1]}
	}
	var out []Query
	add := func(path, podcast string, kv ...string) {
		q := url.Values{}
		for i := 0; i < len(kv); i += 2 {
			if kv[i+1] != "" {
				q.Set(kv[i], kv[i+1])
			}
		}
		out = append(out, Query{path, podcast, q})
	}
	for _, f := range MatrixFilters {
		for _, o := range views.OrderNames {
			add("/v1/read/episodes", "", "filter", f, "sort", o)
			for _, p := range pods {
				add("/v1/read/podcasts/"+p+"/episodes", p, "filter", f, "sort", o)
			}
		}
	}
	for _, o := range views.OrderNames {
		add("/v1/read/inbox", "", "sort", o)
		add("/v1/read/favorites", "", "sort", o)
	}
	add("/v1/read/history", "")
	add("/v1/read/queue", "")
	for _, state := range []string{"subscribed", "archived"} {
		for _, order := range []string{"counter", "alphabetical", "most_played", "most_recent_episode"} {
			for _, counter := range []string{"new", "unplayed", "none"} {
				add("/v1/read/subscriptions", "", "state", state, "order", order, "counter", counter)
			}
		}
	}
	for _, filter := range []string{"counter_greater_zero", "enabled_updates", "disabled_updates,counter_greater_zero"} {
		add("/v1/read/subscriptions", "", "filter", filter, "counter", "unplayed")
	}
	return out
}
