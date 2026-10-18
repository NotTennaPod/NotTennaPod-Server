package views

import (
	"errors"
	"sort"
	"strings"
	"unicode"
	"unicode/utf16"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
)

// The screens other than lists, from the podcasts' aggregates and the
// version's trees: O(P × classes + P log n), never a walk of the episodes.

// ---- navigation counters

// Counters are the navigation counters Android's drawer shows.
type Counters struct {
	Queue int `json:"queue"`
	Inbox int `json:"inbox"` // NEW episodes of subscribed podcasts
}

// Counters returns the navigation counters: the shown podcasts' queued
// episodes, and the subscribed podcasts' new ones.
func (l *Library) Counters() Counters {
	var c Counters
	for id, p := range l.V.podcasts.All() {
		agg, ok := l.V.agg.Get(id)
		if !ok {
			continue
		}
		c.Queue += queued(agg)
		if p.State == "subscribed" {
			c.Inbox += count(agg, func(k Class) bool { return k.Read() == -1 })
		}
	}
	return c
}

// count is a podcast's episodes of the classes f passes.
func count(a *PodAgg, f func(Class) bool) int {
	n := 0
	for c := range Class(NumClasses) {
		if a.Hist[c] > 0 && f(c) {
			n += int(a.Hist[c])
		}
	}
	return n
}

// ---- subscriptions (DBReader.getNavDrawerData)

// SubscriptionOptions are the subscription screen's settings.
type SubscriptionOptions struct {
	State   string // subscribed (default) or archived
	Order   string // counter (default), alphabetical, most_played, most_recent_episode
	Counter string // new (default), unplayed, none
	// Filters (SubscriptionsFilter): counter_greater_zero, enabled_updates,
	// disabled_updates. The download and notification filters are device settings.
	CounterAboveZero, UpdatesEnabled, UpdatesDisabled bool
}

// ErrOption reports an unknown screen option.
var ErrOption = errors.New("views: unknown option")

// ParseSubscriptionOptions reads the subscription screen's query values.
func ParseSubscriptionOptions(state, order, counter, filter string) (SubscriptionOptions, error) {
	o := SubscriptionOptions{State: or(state, "subscribed"), Order: or(order, "counter"), Counter: or(counter, "new")}
	if o.State != "subscribed" && o.State != "archived" {
		return o, ErrOption
	}
	switch o.Order {
	case "counter", "alphabetical", "most_played", "most_recent_episode":
	default:
		return o, ErrOption
	}
	switch o.Counter {
	case "new", "unplayed", "none":
	default:
		return o, ErrOption
	}
	if filter != "" {
		for _, p := range strings.Split(filter, ",") {
			switch p {
			case "counter_greater_zero":
				o.CounterAboveZero = true
			case "enabled_updates":
				o.UpdatesEnabled = true
			case "disabled_updates":
				o.UpdatesDisabled = true
			default:
				return o, ErrOption
			}
		}
	}
	return o, nil
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// Subscription is a row of the subscription list.
type Subscription struct {
	Podcast *PodcastItem
	Counter int
}

// Subscriptions returns the podcasts of a state in Android's order, with
// their counters. As on Android, counters count only subscribed podcasts.
// A podcast's newest episode is the first of its class ranges in the
// newest-first order: O(P × classes × log n).
func (l *Library) Subscriptions(o SubscriptionOptions) []Subscription {
	v := l.V
	dateNewOld, _ := OrderIndex("date_new_old")
	var rows []Subscription
	played, recent := map[string]int{}, map[string]int64{}
	for id, p := range v.podcasts.All() {
		if p.State != o.State {
			continue
		}
		if (o.UpdatesEnabled && !p.KeepUpdated) || (o.UpdatesDisabled && p.KeepUpdated) {
			continue
		}
		counter := 0
		agg, _ := v.agg.Get(id)
		if agg != nil && p.State == "subscribed" {
			switch o.Counter {
			case "new":
				counter = count(agg, func(c Class) bool { return c.Read() == -1 })
			case "unplayed":
				counter = count(agg, func(c Class) bool { return c.Read() != 1 })
			}
			played[id] = count(agg, func(c Class) bool { return c.Read() == 1 })
		}
		if o.CounterAboveZero && counter <= 0 {
			continue
		}
		if o.Order == "most_recent_episode" && agg != nil {
			for c := range Class(NumClasses) {
				if agg.Hist[c] == 0 {
					continue
				}
				_, e, ok := v.orders[dateNewOld].Seek(orderKey{podcast: id, class: c, bound: -1}).Next()
				if ok && e.PodcastID == id && e.PubDate > recent[id] {
					recent[id] = e.PubDate
				}
			}
		}
		rows = append(rows, Subscription{Podcast: p, Counter: counter})
	}
	// getFeedList: by title, case-insensitive for ASCII (COLLATE NOCASE).
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i].Podcast, rows[j].Podcast
		if c := strings.Compare(asciiLower(title(a)), asciiLower(title(b))); c != 0 {
			return c < 0
		}
		if a.Rank != b.Rank {
			return a.Rank < b.Rank
		}
		return a.ID < b.ID
	})
	var less func(a, b Subscription) int
	switch o.Order {
	case "counter":
		less = func(a, b Subscription) int {
			if a.Counter != b.Counter {
				return b.Counter - a.Counter
			}
			return compareIgnoreCase(title(a.Podcast), title(b.Podcast))
		}
	case "alphabetical":
		less = func(a, b Subscription) int {
			switch {
			case a.Podcast.Title == nil:
				return 1
			case b.Podcast.Title == nil:
				return -1
			}
			return compareIgnoreCase(*a.Podcast.Title, *b.Podcast.Title)
		}
	case "most_played":
		less = func(a, b Subscription) int {
			if pa, pb := played[a.Podcast.ID], played[b.Podcast.ID]; pa != pb {
				return pb - pa
			}
			return compareIgnoreCase(title(a.Podcast), title(b.Podcast))
		}
	default:
		less = func(a, b Subscription) int {
			ra, rb := recent[a.Podcast.ID], recent[b.Podcast.ID]
			switch {
			case ra > rb:
				return -1
			case ra < rb:
				return 1
			}
			return 0
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return less(rows[i], rows[j]) < 0 })
	return rows
}

func title(p *PodcastItem) string {
	if p.Title == nil {
		return ""
	}
	return *p.Title
}

func asciiLower(s string) string {
	return strings.Map(func(r rune) rune {
		if 'A' <= r && r <= 'Z' {
			return r + 'a' - 'A'
		}
		return r
	}, s)
}

// compareIgnoreCase is Java's String.compareToIgnoreCase over UTF-16 units.
func compareIgnoreCase(a, b string) int {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		c1, c2 := rune(ua[i]), rune(ub[i])
		if c1 == c2 {
			continue
		}
		c1, c2 = unicode.ToUpper(c1), unicode.ToUpper(c2)
		if c1 == c2 {
			continue
		}
		c1, c2 = unicode.ToLower(c1), unicode.ToLower(c2)
		if c1 != c2 {
			return int(c1 - c2)
		}
	}
	return len(ua) - len(ub)
}

// ---- statistics (DBReader.getStatistics, getMonthlyTimeStatistics)

// PodcastStatistics is a podcast's statistics row.
type PodcastStatistics struct {
	Podcast    *PodcastItem
	Stats      stats.Podcast
	OldestDate int64 // the earliest positive last played; 0 when none
}

// Statistics is the statistics screen.
type Statistics struct {
	Podcasts   []PodcastStatistics
	Months     []MonthStatistics
	OldestDate int64 // Android's StatisticsResult.oldestDate (now when nothing was played)
}

// MonthStatistics is a month's played time.
type MonthStatistics struct {
	Year     int   `json:"year"`
	Month    int   `json:"month"`
	PlayedMS int64 `json:"played_ms"`
}

// podcastRow is a podcast's statistics row over [from, to): the values
// without dates from its aggregates, the dated ones from its units' prefix
// sums; windowMarked dates the marked-played value too. O(log n).
func (l *Library) podcastRow(p *PodcastItem, from, to int64, windowMarked bool) PodcastStatistics {
	v := l.V
	agg := v.PodcastAgg(p.ID)
	played, marked := v.PlayedBetween(p.ID, from, to)
	row := PodcastStatistics{Podcast: p, Stats: stats.Podcast{PlayedMS: played, PlayedMarkedMS: agg.MarkedAll, EpisodesStarted: agg.Started,
		EpisodesStartedMarked: agg.StartedMarked, Episodes: agg.Parts, TotalMS: agg.TotalMS}, OldestDate: v.OldestPlayed(p.ID)}
	if windowMarked {
		row.Stats.PlayedMarkedMS = marked
	}
	return row
}

// PodcastStatistics is a podcast's statistics card (all time, as the
// statistics screen shows its row), for a subscribed or archived podcast.
func (l *Library) PodcastStatistics(p *PodcastItem) (PodcastStatistics, bool) {
	if p.State == "not_subscribed" {
		return PodcastStatistics{}, false
	}
	return l.podcastRow(p, 0, stats.Forever, false), true
}

// Statistics computes the statistics screen: subscribed and archived
// podcasts, ordered by the played time shown (with "include marked as played",
// all time, as Android forces), ties in podcast order; months over every
// shown podcast, as Android's media table holds them.
func (l *Library) Statistics(from, to int64, includeMarked bool, now int64) Statistics {
	st := l.statistics(from, to, includeMarked, false, now)
	for k, ms := range l.V.Months() {
		y, m := MonthOf(k)
		st.Months = append(st.Months, MonthStatistics{Year: y, Month: m, PlayedMS: ms})
	}
	return st
}

// statistics is the podcasts' rows of the statistics (Home's classics read
// them without the months).
func (l *Library) statistics(from, to int64, includeMarked, windowMarked bool, now int64) Statistics {
	out := Statistics{OldestDate: now}
	for _, p := range l.V.podcasts.All() {
		if p.State == "not_subscribed" {
			continue
		}
		row := l.podcastRow(p, from, to, windowMarked)
		if row.Stats.Episodes > 0 && row.OldestDate > 0 && row.OldestDate < out.OldestDate {
			out.OldestDate = row.OldestDate
		}
		out.Podcasts = append(out.Podcasts, row)
	}
	shown := func(r PodcastStatistics) int64 {
		if includeMarked {
			return r.Stats.PlayedMarkedMS
		}
		return r.Stats.PlayedMS
	}
	sort.Slice(out.Podcasts, func(i, j int) bool { return podcastLess(out.Podcasts[i].Podcast, out.Podcasts[j].Podcast) })
	sort.SliceStable(out.Podcasts, func(i, j int) bool { return shown(out.Podcasts[i]) > shown(out.Podcasts[j]) })
	return out
}

// podcastLess is podcast order: Android's feed IDs, the order of creation.
func podcastLess(a, b *PodcastItem) bool {
	if a.Rank != b.Rank {
		return a.Rank < b.Rank
	}
	return a.ID < b.ID
}
