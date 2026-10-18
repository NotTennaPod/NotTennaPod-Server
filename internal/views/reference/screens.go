package reference

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"sort"
	"strings"
	"unicode"
	"unicode/utf16"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
)

// ---- navigation counters

// Counters are the navigation counters Android's drawer shows.
type Counters struct {
	Queue int `json:"queue"`
	Inbox int `json:"inbox"` // NEW episodes of subscribed podcasts
}

// Counters returns the navigation counters.
func (l *Library) Counters() Counters {
	c := Counters{Queue: len(l.Queue)}
	for _, e := range l.Episodes {
		if e.Read == -1 && e.Podcast.State == "subscribed" {
			c.Inbox++
		}
	}
	return c
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
	Podcast *Podcast
	Counter int
}

// Subscriptions returns the podcasts of a state in Android's order, with
// their counters. As on Android, counters count only subscribed podcasts.
func (l *Library) Subscriptions(o SubscriptionOptions) []Subscription {
	counters, played, recent := map[string]int{}, map[string]int{}, map[string]int64{}
	for _, e := range l.Episodes {
		id := e.Podcast.ID
		if e.PubDate > recent[id] {
			recent[id] = e.PubDate
		}
		if e.Podcast.State != "subscribed" {
			continue
		}
		if e.Read == 1 {
			played[id]++
		}
		switch {
		case o.Counter == "new" && e.Read == -1, o.Counter == "unplayed" && e.Read != 1:
			counters[id]++
		}
	}
	// getFeedList: by title, case-insensitive for ASCII (COLLATE NOCASE).
	var rows []Subscription
	for _, p := range l.Podcasts {
		if p.State != o.State {
			continue
		}
		if (o.UpdatesEnabled && !p.KeepUpdated) || (o.UpdatesDisabled && p.KeepUpdated) {
			continue
		}
		if o.CounterAboveZero && counters[p.ID] <= 0 {
			continue
		}
		rows = append(rows, Subscription{Podcast: p, Counter: counters[p.ID]})
	}
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

func title(p *Podcast) string {
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
	Podcast    *Podcast
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

func (l *Library) parts(e *Episode) []stats.Part {
	recs := map[string]map[string]any{}
	for g, r := range e.Records {
		recs[g] = r.fields
	}
	meta := e.Records["metadata"].fields
	if meta == nil {
		meta = map[string]any{}
	}
	return stats.Parts(recs, meta)
}

// Statistics computes the statistics screen: subscribed and archived
// podcasts, ordered by the played time shown (with "include marked as played",
// all time, as Android forces), ties in podcast order; months over every
// podcast, as Android's media table holds them.
func (l *Library) Statistics(from, to int64, includeMarked bool, now int64) Statistics {
	return l.statistics(from, to, includeMarked, false, now)
}

// statistics with windowMarked applies the date filter to the marked-played
// values too, as Android's SQL does for Home (getStatistics with a range).
func (l *Library) statistics(from, to int64, includeMarked, windowMarked bool, now int64) Statistics {
	byPodcast := map[string][]stats.Part{}
	var all []stats.Part
	for _, e := range l.Episodes {
		ps := l.parts(e)
		all = append(all, ps...)
		if e.Podcast.State != "not_subscribed" {
			byPodcast[e.Podcast.ID] = append(byPodcast[e.Podcast.ID], ps...)
		}
	}
	out := Statistics{OldestDate: now}
	for _, p := range l.Podcasts {
		if p.State == "not_subscribed" {
			continue
		}
		parts := byPodcast[p.ID]
		row := PodcastStatistics{Podcast: p, Stats: stats.PerPodcast(parts, from, to)}
		if windowMarked {
			row.Stats.PlayedMarkedMS = stats.PlayedMarkedBetween(parts, from, to)
		}
		for _, x := range parts {
			if x.LastPlayed > 0 && (row.OldestDate == 0 || x.LastPlayed < row.OldestDate) {
				row.OldestDate = x.LastPlayed
			}
		}
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
	for m, v := range stats.Monthly(all) {
		out.Months = append(out.Months, MonthStatistics{Year: m.Year, Month: m.Month, PlayedMS: v})
	}
	sort.Slice(out.Months, func(i, j int) bool {
		a, b := out.Months[i], out.Months[j]
		return a.Year < b.Year || (a.Year == b.Year && a.Month < b.Month)
	})
	return out
}

// podcastLess is podcast order: Android's feed IDs, the order of creation.
func podcastLess(a, b *Podcast) bool {
	if a.Rank != b.Rank {
		return a.Rank < b.Rank
	}
	return a.ID < b.ID
}

// ---- Home (HomeFragment's sections)

// Home is Android Home's sections that sync: continue listening (the queue's
// paused episodes), see what's new (the Inbox), get surprised and check your
// classics. Downloads stay on devices; the Echo review is seasonal.
type Home struct {
	Continue []*Episode
	New      []*Episode
	NewCount int
	Surprise []*Episode
	Classics []*Podcast
}

const (
	homeContinue = 8 // QueueSection.NUM_EPISODES
	homeNew      = 2 // InboxSection.NUM_EPISODES
	homeSurprise = 8 // EpisodesSurpriseSection.NUM_EPISODES
	homeClassics = 8 // SubscriptionsSection.NUM_FEEDS
	day          = int64(24 * 3600 * 1000)
)

// Home computes the Home sections at time now. seed picks the surprise
// sample, as Android's per-process seed does; includeMarked is the statistics
// screen's setting, which Android's classics section follows.
func (l *Library) Home(now int64, seed int64, inboxOrder string, includeMarked bool) Home {
	var h Home
	// getPausedQueue: started or just played first, by last played, then queue order.
	h.Continue = append([]*Episode(nil), l.Queue...)
	recent := func(e *Episode) int64 {
		if e.HasMedia && (e.Position >= 1000 || e.LastPlayed >= now-30_000) {
			return e.LastPlayed
		}
		return 0
	}
	sort.SliceStable(h.Continue, func(i, j int) bool { return recent(h.Continue[i]) > recent(h.Continue[j]) })
	if len(h.Continue) > homeContinue {
		h.Continue = h.Continue[:homeContinue]
	}
	inbox := l.EpisodesWhere(Filter{New: true}, inboxOrder, "")
	h.NewCount = len(inbox)
	h.New = inbox[:min(len(inbox), homeNew)]
	// getRandomEpisodesCursor: new or unplayed episodes of subscribed podcasts
	// from the last two years (Android's 2 x 356 days), not played more than
	// an hour ago; one per podcast, then a seeded order.
	pick := map[string]*Episode{}
	for _, e := range l.Episodes {
		if (e.Read != -1 && e.Read != 0) || !e.HasMedia || e.Podcast.State != "subscribed" ||
			e.PubDate <= now-2*356*day || !(e.LastPlayed == 0 || e.LastPlayed > now-3600_000) {
			continue
		}
		if cur, ok := pick[e.Podcast.ID]; !ok || sample(seed, e.ID) > sample(seed, cur.ID) {
			pick[e.Podcast.ID] = e
		}
	}
	for _, e := range pick {
		h.Surprise = append(h.Surprise, e)
	}
	sort.Slice(h.Surprise, func(i, j int) bool {
		a, b := sample(seed*3, h.Surprise[i].ID), sample(seed*3, h.Surprise[j].ID)
		if a != b {
			return a > b
		}
		return h.Surprise[i].ID < h.Surprise[j].ID
	})
	if len(h.Surprise) > homeSurprise {
		h.Surprise = h.Surprise[:homeSurprise]
	}
	// SubscriptionsSection: the last three years' statistics by played time.
	st := l.statistics(now-3*365*day, 1<<63-1, includeMarked, true, now)
	for _, r := range st.Podcasts {
		if len(h.Classics) == homeClassics {
			break
		}
		if r.Podcast.State == "subscribed" {
			h.Classics = append(h.Classics, r.Podcast)
		}
	}
	return h
}

// sample is a seeded pseudo-random number in Android's range [0, 46471).
func sample(seed int64, id string) uint64 {
	f := fnv.New64a()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seed))
	f.Write(b[:])
	f.Write([]byte(id))
	return f.Sum64() % 46471
}
