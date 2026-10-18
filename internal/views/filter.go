package views

import (
	"errors"
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

// MatchClass reports whether an episode of the class passes the filter's
// properties other than the podcast's state: Match is MatchClass of the
// episode's class and StateOK of its podcast's state.
func (f Filter) MatchClass(c Class) bool {
	switch {
	case f.Played:
		if c.Read() != 1 {
			return false
		}
	case f.Unplayed:
		if c.Read() == 1 {
			return false
		}
	case f.New:
		if c.Read() != -1 {
			return false
		}
	}
	if (f.Paused && !c.Paused()) || (!f.Paused && f.NotPaused && c.Paused()) {
		return false
	}
	if (f.Queued && !c.Queued()) || (!f.Queued && f.NotQueued && c.Queued()) {
		return false
	}
	if (f.HasMedia && !c.Media()) || (!f.HasMedia && f.NoMedia && c.Media()) {
		return false
	}
	if (f.Favorite && !c.Fav()) || (!f.Favorite && f.NotFavorite && c.Fav()) {
		return false
	}
	return !f.InHistory || c.History()
}

// StateOK reports whether the filter includes episodes of a podcast in the
// state (subscribed, archived or not_subscribed).
func (f Filter) StateOK(state string) bool {
	if f.Subscribed && f.Archived && f.NotSubscribed {
		return true
	}
	switch state {
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
