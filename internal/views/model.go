package views

// The incremental library model: an account's library at one snapshot as an
// immutable Version of persistent, canonical trees (internal/views/ptree).
// A version is built in full from one snapshot (BuildVersion), or made from
// the account's previous version by applying exactly the entities the read
// model's change capture stamped since (Version.Apply, storage
// ViewChangesSince). Both give equal versions (EqualVersions).
//
// Invariants (docs/ARCHITECTURE.md, "Read-model change capture"):
//   - Versions: a version for the key (log, epoch, generation, tick) is
//     either built in full from a snapshot with that key, or applied to the
//     account's version of the same lineage and generation at a tick at or
//     below it, with every entity stamped above that tick read from the
//     snapshot; either way it equals the full build.
//   - Immutability: nothing reachable from a version is written again:
//     trees copy their paths, entities are replaced, never changed.
//   - Work: an apply costs O(changed entities × log n) plus the bytes of
//     their records, plus, for a podcast whose shown state changed, its
//     statistics months; never a walk of the library.
//
// Shown podcasts are applied at query time: the episode structures hold
// every stored episode keyed by its podcast first, so a podcast's state
// change re-keys nothing but its statistics months.

import (
	"cmp"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// RawRecord is a published record as stored: its value (list views' form:
// episode metadata without description and chapters) and its provenance,
// both canonical JSON. Each record is held once, undecoded.
type RawRecord struct {
	Value      json.RawMessage `json:"value"`
	Provenance json.RawMessage `json:"provenance"`
}

// PodcastItem is a podcast the library shows (subscribed, archived or not
// subscribed), with the values its screens use.
type PodcastItem struct {
	ID             string
	Records        map[string]RawRecord
	State          string // subscribed, archived or not_subscribed
	Title          *string
	DisplayTitle   string // the title, else the feed's address
	ImageURL       string
	KeepUpdated    bool
	Rank           int64  // creation order: Android's feed IDs
	GenerationID   string // the subscription's live generation
	SecretID       string // the credentials' secret, "" without
	HasCredentials bool

	generation any // the subscription's generation_id as its record holds it (the rows' value)
}

// EpisodeItem is a stored episode: registered, identified and not merged,
// whatever its podcast's state, with Android's derived columns.
type EpisodeItem struct {
	ID, PodcastID string
	Records       map[string]RawRecord
	Placeholder   bool // no catalog metadata yet: occurrence evidence stands in

	Title          *string
	PubDate        int64
	Read           int // -1 new, 0 unplayed, 1 played
	Position       int64
	HasMedia       bool
	Duration       int64
	Size           int64
	Link           *string
	Queued         bool
	QueueKey       contract.QueueKey
	Favorite       bool
	History        int64
	LastPlayed     int64
	Rank           int64 // catalog insertion order: Android's item IDs
	ImageURL       string
	SourceURL      string // the enclosure the relay fetches
	DeclaredType   string
	ChapterURL     string
	TranscriptURL  string
	TranscriptType string
	Parts          []stats.Part // the statistics units, the episode's own first
	Class          Class
}

// Class is an episode's filter class: its read state, and whether it is
// paused, queued, has media, is a favourite and is in history; Android's
// episode filters are functions of it and the podcast's state
// (Filter.MatchClass, Filter.StateOK).
type Class uint8

// NumClasses is the number of classes: 3 read states × 2^5.
const NumClasses = 96

// ClassOf is the class of an episode with these values.
func ClassOf(read int, hasMedia bool, position int64, queued, favorite bool, history int64) Class {
	b := func(v bool) Class {
		if v {
			return 1
		}
		return 0
	}
	paused := hasMedia && position > 0
	inHistory := hasMedia && history > 0
	return Class(read+1)*32 + b(paused)*16 + b(queued)*8 + b(hasMedia)*4 + b(favorite)*2 + b(inHistory)
}

// Read returns the class's read state (-1 new, 0 unplayed, 1 played).
func (c Class) Read() int     { return int(c/32) - 1 }
func (c Class) Paused() bool  { return c&16 != 0 }
func (c Class) Queued() bool  { return c&8 != 0 }
func (c Class) Media() bool   { return c&4 != 0 }
func (c Class) Fav() bool     { return c&2 != 0 }
func (c Class) History() bool { return c&1 != 0 }

// PodAgg is a podcast's aggregates over its stored episodes, whatever its
// state: the count of each class, and the statistics that take no dates.
type PodAgg struct {
	Hist          [NumClasses]int32
	Parts         int64 // statistics units (Android's episodes)
	TotalMS       int64
	MarkedAll     int64 // "include marked as played", all time
	Started       int64
	StartedMarked int64
}

func (a *PodAgg) empty() bool { return *a == PodAgg{} }

// Version is an account's library at one snapshot. Its trees are persistent:
// a later version shares every node it did not change.
type Version struct {
	Account string
	Head    storage.Head
	Clock   storage.ViewClock

	// accountRows are the account entity's records by entity ID (one, the
	// account's own, in practice); AccountRecords merges them as Load does.
	accountRows    map[string]map[string]RawRecord
	AccountRecords map[string]RawRecord

	podcasts ptree.Tree[string, *PodcastItem] // shown podcasts
	stored   ptree.Tree[string, *EpisodeItem]
	agg      ptree.Tree[string, *PodAgg] // by podcast, over its stored episodes

	orders [numOrders]ptree.Tree[orderKey, *EpisodeItem]

	// Queue structures, keyed by podcast first: the queue, and Home's
	// Continue partitions (recent = LastPlayed when it counts, else 0).
	queue    ptree.Tree[queueKey, *EpisodeItem] // every queued episode
	pos      ptree.Tree[queueKey, *EpisodeItem] // media, position ≥ 1000, last played > 0: recent > 0
	recent   ptree.Tree[queueKey, *EpisodeItem] // media, position < 1000, last played > 0: recent > 0 within 30 s
	zero     ptree.Tree[queueKey, *EpisodeItem] // recent 0 whatever the time
	timeZero ptree.Tree[queueKey, *EpisodeItem] // recent's set, in queue order: 0 once 30 s old
	neg      ptree.Tree[queueKey, *EpisodeItem] // media, position ≥ 1000, last played < 0

	// Surprise candidates: new or unplayed with media, never played (Max =
	// publication date), and those played (by last played).
	surprise     ptree.Tree[surpriseKey, *EpisodeItem]
	recentPlayed ptree.Tree[playedKey, *EpisodeItem]

	// Statistics: every unit by (podcast, its own last played), A = played
	// ms, B = the marked value; played ms by (podcast, UTC month) and by
	// month over the shown podcasts.
	stats     ptree.Tree[statKey, statValue]
	podMonths ptree.Tree[podMonthKey, int64]
	months    ptree.Tree[int, int64]
}

// ---- tree keys and specs

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	}
	return 1
}

// column is one sort column: SQLite's order, where NULL is the smallest.
type column struct {
	null bool
	n    int64
	s    string
}

func compareColumns(a, b column) int {
	switch {
	case a.null && b.null:
		return 0
	case a.null:
		return -1
	case b.null:
		return 1
	}
	return cmp.Or(strings.Compare(a.s, b.s), cmp.Compare(a.n, b.n))
}

// orderKey places an episode in one sort order: (podcast, class, the order's
// column in its direction, rank, ID), so a list is a merge of (shown
// podcast, matching class) ranges and ties keep item order (Sort's stable
// sort over Rank order).
type orderKey struct {
	podcast string
	class   Class
	bound   int8 // −1 before every key of its (podcast, class), 1 after; 0 for an episode's key
	col     column
	rank    int64
	id      string
}

// OrderNames are the sort orders, in index order.
var OrderNames = [numOrders]string{
	"completion_date_new_old", "date_new_old", "date_old_new", "duration_long_short", "duration_short_long",
	"episode_filename_a_z", "episode_filename_z_a", "episode_title_a_z", "episode_title_z_a", "size_large_small", "size_small_large",
}

const numOrders = 11

type orderDef struct {
	col  func(e *EpisodeItem) column
	desc bool
}

func textColumn(s *string) column {
	if s == nil {
		return column{null: true}
	}
	return column{s: *s}
}

func mediaColumn(e *EpisodeItem, v int64) column { return column{null: !e.HasMedia, n: v} }

var orderDefs = [numOrders]orderDef{
	{func(e *EpisodeItem) column { return mediaColumn(e, e.History) }, true},
	{func(e *EpisodeItem) column { return column{n: e.PubDate} }, true},
	{func(e *EpisodeItem) column { return column{n: e.PubDate} }, false},
	{func(e *EpisodeItem) column { return mediaColumn(e, e.Duration) }, true},
	{func(e *EpisodeItem) column { return mediaColumn(e, e.Duration) }, false},
	{func(e *EpisodeItem) column { return textColumn(e.Link) }, false},
	{func(e *EpisodeItem) column { return textColumn(e.Link) }, true},
	{func(e *EpisodeItem) column { return textColumn(e.Title) }, false},
	{func(e *EpisodeItem) column { return textColumn(e.Title) }, true},
	{func(e *EpisodeItem) column { return mediaColumn(e, e.Size) }, true},
	{func(e *EpisodeItem) column { return mediaColumn(e, e.Size) }, false},
}

func (d orderDef) key(e *EpisodeItem) orderKey {
	return orderKey{podcast: e.PodcastID, class: e.Class, col: d.col(e), rank: e.Rank, id: e.ID}
}

var orderSpecs = func() (out [numOrders]*ptree.Spec[orderKey, *EpisodeItem]) {
	for i, d := range orderDefs {
		desc := d.desc
		out[i] = &ptree.Spec[orderKey, *EpisodeItem]{
			Compare: func(a, b orderKey) int {
				c := compareColumns(a.col, b.col)
				if desc {
					c = -c
				}
				if p := cmp.Or(strings.Compare(a.podcast, b.podcast), cmp.Compare(a.class, b.class), cmp.Compare(a.bound, b.bound)); p != 0 || a.bound != 0 {
					return p
				}
				return cmp.Or(c, cmp.Compare(a.rank, b.rank), strings.Compare(a.id, b.id))
			},
			Priority: func(k orderKey) uint64 { return ptree.Hash(k.id) },
		}
	}
	return out
}()

// queueKey places a queued episode: (podcast, last played descending for
// the partitions ordered by it, queue key, ID).
type queueKey struct {
	podcast string
	bound   int8  // −1 before every key of its podcast, 1 after; 0 for an episode's key
	last    int64 // negated last played in the descending partitions, else 0
	key     contract.QueueKey
	id      string
}

func compareQueueKey(a, b queueKey) int {
	if c := cmp.Or(strings.Compare(a.podcast, b.podcast), cmp.Compare(a.bound, b.bound)); c != 0 || a.bound != 0 {
		return c
	}
	return compareQueueRows(a, b)
}

// compareQueueRows orders queue keys of any podcasts: (last, queue key, ID).
func compareQueueRows(a, b queueKey) int {
	if c := cmp.Compare(a.last, b.last); c != 0 {
		return c
	}
	return cmp.Or(contract.CompareQueueKeys(a.key, b.key), strings.Compare(a.id, b.id))
}

var queueSpec = &ptree.Spec[queueKey, *EpisodeItem]{
	Compare:  compareQueueKey,
	Priority: func(k queueKey) uint64 { return ptree.Hash(k.id) },
}

func queueOrder(e *EpisodeItem) queueKey {
	return queueKey{podcast: e.PodcastID, key: e.QueueKey, id: e.ID}
}

func recentOrder(e *EpisodeItem) queueKey {
	return queueKey{podcast: e.PodcastID, last: -e.LastPlayed, key: e.QueueKey, id: e.ID}
}

// surpriseKey orders a podcast's never-played candidates by their score
// base u(ID).
type surpriseKey struct {
	podcast string
	u       uint64
	id      string
}

// SurpriseBase is an episode's score base u(ID): SplitMix64 of FNV-1a of
// its ID.
func SurpriseBase(id string) uint64 { return ptree.Hash(id) }

var surpriseSpec = &ptree.Spec[surpriseKey, *EpisodeItem]{
	Compare: func(a, b surpriseKey) int {
		return cmp.Or(strings.Compare(a.podcast, b.podcast), cmp.Compare(a.u, b.u), strings.Compare(a.id, b.id))
	},
	// A priority apart from the key's own hash, which would make the
	// tree a list.
	Priority: func(k surpriseKey) uint64 { return ptree.Hash("surprise", k.id) },
	Measure:  func(_ surpriseKey, e *EpisodeItem) (int64, int64, int64) { return 0, 0, e.PubDate },
}

// playedKey orders a podcast's played candidates by last played.
type playedKey struct {
	podcast string
	last    int64
	id      string
}

var playedSpec = &ptree.Spec[playedKey, *EpisodeItem]{
	Compare: func(a, b playedKey) int {
		return cmp.Or(strings.Compare(a.podcast, b.podcast), cmp.Compare(a.last, b.last), strings.Compare(a.id, b.id))
	},
	Priority: func(k playedKey) uint64 { return ptree.Hash(k.id) },
}

// statKey places one statistics unit at its own last played.
type statKey struct {
	podcast string
	last    int64
	id      string
	part    int
}

type statValue struct{ played, marked int64 }

var statSpec = &ptree.Spec[statKey, statValue]{
	Compare: func(a, b statKey) int {
		return cmp.Or(strings.Compare(a.podcast, b.podcast), cmp.Compare(a.last, b.last), strings.Compare(a.id, b.id), cmp.Compare(a.part, b.part))
	},
	Priority: func(k statKey) uint64 { return ptree.Hash(k.id, strconv.Itoa(k.part)) },
	Measure:  func(_ statKey, v statValue) (int64, int64, int64) { return v.played, v.marked, 0 },
}

// markedValue is a unit's "include marked as played" value: its played
// time, else its duration when it is played.
func markedValue(x stats.Part) int64 {
	if x.PlayedMS != 0 {
		return x.PlayedMS
	}
	if x.Played {
		return x.DurationMS
	}
	return 0
}

type podMonthKey struct {
	podcast string
	month   int // year × 12 + month − 1, UTC
}

var podMonthSpec = &ptree.Spec[podMonthKey, int64]{
	Compare: func(a, b podMonthKey) int {
		return cmp.Or(strings.Compare(a.podcast, b.podcast), cmp.Compare(a.month, b.month))
	},
	Priority: func(k podMonthKey) uint64 { return ptree.Hash(k.podcast, strconv.Itoa(k.month)) },
	Measure:  func(_ podMonthKey, v int64) (int64, int64, int64) { return v, 0, 0 },
}

var monthSpec = &ptree.Spec[int, int64]{
	Compare:  cmp.Compare[int],
	Priority: func(k int) uint64 { return ptree.Hash(strconv.Itoa(k)) },
	Measure:  func(_ int, v int64) (int64, int64, int64) { return v, 0, 0 },
}

// monthOf is the UTC month of a time in ms.
func monthOf(ms int64) int {
	t := time.UnixMilli(ms).UTC()
	return t.Year()*12 + int(t.Month()) - 1
}

// MonthOf splits a month key into the year and month.
func MonthOf(k int) (year, month int) { return k / 12, k%12 + 1 }

var (
	podcastSpec = &ptree.Spec[string, *PodcastItem]{Compare: strings.Compare, Priority: func(k string) uint64 { return ptree.Hash(k) }}
	storedSpec  = &ptree.Spec[string, *EpisodeItem]{Compare: strings.Compare, Priority: func(k string) uint64 { return ptree.Hash(k) }}
	aggSpec     = &ptree.Spec[string, *PodAgg]{Compare: strings.Compare, Priority: func(k string) uint64 { return ptree.Hash(k) }}
)

// emptyVersion is a version without entities.
func emptyVersion(account string) *Version {
	v := &Version{Account: account, accountRows: map[string]map[string]RawRecord{}, AccountRecords: map[string]RawRecord{},
		podcasts: ptree.New(podcastSpec), stored: ptree.New(storedSpec), agg: ptree.New(aggSpec),
		queue: ptree.New(queueSpec), pos: ptree.New(queueSpec), recent: ptree.New(queueSpec), zero: ptree.New(queueSpec),
		timeZero: ptree.New(queueSpec), neg: ptree.New(queueSpec), surprise: ptree.New(surpriseSpec), recentPlayed: ptree.New(playedSpec),
		stats: ptree.New(statSpec), podMonths: ptree.New(podMonthSpec), months: ptree.New(monthSpec)}
	for i := range v.orders {
		v.orders[i] = ptree.New(orderSpecs[i])
	}
	return v
}

// ---- the version's reads (the seams the screens use)

// Podcast returns a shown podcast.
func (v *Version) Podcast(id string) (*PodcastItem, bool) { return v.podcasts.Get(id) }

// Episode returns a stored episode, whatever its podcast's state.
func (v *Version) Episode(id string) (*EpisodeItem, bool) { return v.stored.Get(id) }

// Counts are the version's sizes.
func (v *Version) Counts() (podcasts, episodes, queued int) {
	return v.podcasts.Len(), v.stored.Len(), v.queue.Len()
}

// PodcastAgg returns a podcast's aggregates over its stored episodes.
func (v *Version) PodcastAgg(id string) PodAgg {
	if a, ok := v.agg.Get(id); ok {
		return *a
	}
	return PodAgg{}
}

// PlayedBetween sums a podcast's units last played in [from, to): their
// played time, and their marked values.
func (v *Version) PlayedBetween(podcast string, from, to int64) (played, marked int64) {
	if to <= from {
		return 0, 0
	}
	lo := v.stats.PrefixAgg(statKey{podcast: podcast, last: from})
	hi := v.stats.PrefixAgg(statKey{podcast: podcast, last: to})
	return hi.A - lo.A, hi.B - lo.B
}

// OldestPlayed is a podcast's earliest positive last played (0 when none).
func (v *Version) OldestPlayed(podcast string) int64 {
	k, _, ok := v.stats.Seek(statKey{podcast: podcast, last: 1}).Next()
	if !ok || k.podcast != podcast {
		return 0
	}
	return k.last
}

// Months walks the played time by UTC month (a month key, MonthOf) over
// the shown podcasts, in month order.
func (v *Version) Months() func(yield func(month int, playedMS int64) bool) { return v.months.All() }

// OrderIndex returns a sort order's index in OrderNames.
func OrderIndex(order string) (int, bool) {
	for i, n := range OrderNames {
		if n == order {
			return i, true
		}
	}
	return 0, false
}

// OrderEntries walks a sort order (OrderNames[order]) range by range: each
// podcast's episodes of one class in that order, the ranges by podcast and
// class.
func (v *Version) OrderEntries(order int) func(yield func(*EpisodeItem) bool) {
	return func(yield func(*EpisodeItem) bool) {
		for _, e := range v.orders[order].All() {
			if !yield(e) {
				return
			}
		}
	}
}

// AllPodcasts walks the shown podcasts by ID.
func (v *Version) AllPodcasts() func(yield func(string, *PodcastItem) bool) { return v.podcasts.All() }

// AllEpisodes walks the stored episodes by ID.
func (v *Version) AllEpisodes() func(yield func(string, *EpisodeItem) bool) { return v.stored.All() }
