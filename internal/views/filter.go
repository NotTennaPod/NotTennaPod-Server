package views

import (
	"errors"
	"sort"
	"strings"
)

// Filter is Android's FeedItemFilter as FeedItemFilterQuery applies it in
// SQL: in each pair the first property wins, and items without media match
// neither paused nor in history (their media columns are NULL).
type Filter struct {
	Played, Unplayed, New               bool
	Paused, NotPaused                   bool
	Queued, NotQueued                   bool
	HasMedia, NoMedia                   bool
	Favorite, NotFavorite               bool
	InHistory                           bool
	Subscribed, Archived, NotSubscribed bool // include_*: none means subscribed only
}

// ErrFilter reports an unknown or unavailable filter property.
var ErrFilter = errors.New("views: unknown filter")

// ErrDownloads reports a downloads filter: downloads stay on devices.
var ErrDownloads = errors.New("views: downloads are not available in the browser")

// ParseFilter reads a comma-separated list of Android's filter properties.
func ParseFilter(s string) (Filter, error) {
	var f Filter
	if s == "" {
		return f, nil
	}
	for _, p := range strings.Split(s, ",") {
		switch p {
		case "played":
			f.Played = true
		case "unplayed":
			f.Unplayed = true
		case "new":
			f.New = true
		case "paused":
			f.Paused = true
		case "not_paused":
			f.NotPaused = true
		case "queued":
			f.Queued = true
		case "not_queued":
			f.NotQueued = true
		case "has_media":
			f.HasMedia = true
		case "no_media":
			f.NoMedia = true
		case "is_favorite":
			f.Favorite = true
		case "not_favorite":
			f.NotFavorite = true
		case "is_in_history":
			f.InHistory = true
		case "include_subscribed":
			f.Subscribed = true
		case "include_archived":
			f.Archived = true
		case "include_not_subscribed":
			f.NotSubscribed = true
		case "downloaded", "not_downloaded":
			return f, ErrDownloads
		default:
			return f, ErrFilter
		}
	}
	return f, nil
}

// AllStates includes every feed state (Android's INCLUDE_ALL_FEED_STATES).
func (f Filter) AllStates() Filter {
	f.Subscribed, f.Archived, f.NotSubscribed = true, true, true
	return f
}

// Match reports whether the episode passes the filter.
func (f Filter) Match(e *Episode) bool {
	switch {
	case f.Played:
		if e.Read != 1 {
			return false
		}
	case f.Unplayed:
		if e.Read == 1 {
			return false
		}
	case f.New:
		if e.Read != -1 {
			return false
		}
	}
	paused := e.HasMedia && e.Position > 0
	if (f.Paused && !paused) || (!f.Paused && f.NotPaused && paused) {
		return false
	}
	if (f.Queued && !e.Queued) || (!f.Queued && f.NotQueued && e.Queued) {
		return false
	}
	if (f.HasMedia && !e.HasMedia) || (!f.HasMedia && f.NoMedia && e.HasMedia) {
		return false
	}
	if (f.Favorite && !e.Favorite) || (!f.Favorite && f.NotFavorite && e.Favorite) {
		return false
	}
	if f.InHistory && !(e.HasMedia && e.History > 0) {
		return false
	}
	if f.Subscribed && f.Archived && f.NotSubscribed {
		return true
	}
	switch e.Podcast.State {
	case "subscribed":
		return f.Subscribed || (!f.Archived && !f.NotSubscribed)
	case "archived":
		return f.Archived
	default:
		return f.NotSubscribed
	}
}

// Sort orders of Android's episode lists (SortOrder, FeedItemSortQuery).
var Orders = map[string]bool{
	"date_old_new": true, "date_new_old": true,
	"episode_title_a_z": true, "episode_title_z_a": true,
	"duration_short_long": true, "duration_long_short": true,
	"size_small_large": true, "size_large_small": true,
	"episode_filename_a_z": true, "episode_filename_z_a": true,
	"completion_date_new_old": true,
}

// key is one sort column: SQLite's order, where NULL is the smallest value.
type key struct {
	null bool
	n    int64
	s    string
}

func compareKeys(a, b key) int {
	switch {
	case a.null && b.null:
		return 0
	case a.null:
		return -1
	case b.null:
		return 1
	case a.s < b.s:
		return -1
	case a.s > b.s:
		return 1
	case a.n < b.n:
		return -1
	case a.n > b.n:
		return 1
	}
	return 0
}

// Sort orders episodes as Android's ORDER BY does: by the order's column,
// ascending or descending, with ties in item order (the input's Rank order).
// Titles and links compare as SQLite's BINARY collation (UTF-8 bytes).
func Sort(list []*Episode, order string) {
	var col func(e *Episode) key
	desc := false
	media := func(e *Episode, v int64) key { return key{null: !e.HasMedia, n: v} }
	text := func(s *string) key {
		if s == nil {
			return key{null: true}
		}
		return key{s: *s}
	}
	switch order {
	case "date_old_new":
		col = func(e *Episode) key { return key{n: e.PubDate} }
	case "episode_title_a_z", "episode_title_z_a":
		col, desc = func(e *Episode) key { return text(e.Title) }, order == "episode_title_z_a"
	case "duration_short_long", "duration_long_short":
		col, desc = func(e *Episode) key { return media(e, e.Duration) }, order == "duration_long_short"
	case "size_small_large", "size_large_small":
		col, desc = func(e *Episode) key { return media(e, e.Size) }, order == "size_large_small"
	case "episode_filename_a_z", "episode_filename_z_a":
		col, desc = func(e *Episode) key { return text(e.Link) }, order == "episode_filename_z_a"
	case "completion_date_new_old":
		col, desc = func(e *Episode) key { return media(e, e.History) }, true
	default: // date_new_old
		col, desc = func(e *Episode) key { return key{n: e.PubDate} }, true
	}
	sort.SliceStable(list, func(i, j int) bool {
		c := compareKeys(col(list[i]), col(list[j]))
		if desc {
			return c > 0
		}
		return c < 0
	})
}

// Episodes returns the library's episodes passing the filter, sorted.
func (l *Library) EpisodesWhere(f Filter, order string, podcast string) []*Episode {
	var out []*Episode
	for _, e := range l.Episodes {
		if podcast != "" && e.Podcast.ID != podcast {
			continue
		}
		if f.Match(e) {
			out = append(out, e)
		}
	}
	Sort(out, order)
	return out
}
