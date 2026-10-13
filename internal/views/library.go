// Package views computes the browser's read views (PROTOCOL §12) from an
// account's published records at one head: Android 3.12.1's subscription
// list, episode filters and sort orders, Inbox, queue, history, statistics and
// Home sections. The views read published records only, never the archive, so
// they show exactly what phones show. The Android behaviour each view follows
// is listed in PROTOCOL §12.
package views

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Record is a published record: its value and the nodes that determine it.
type Record struct {
	Value      json.RawMessage `json:"value"`
	Provenance []string        `json:"provenance"`

	fields map[string]any
}

// Field returns a top-level field of the record's value.
func (r Record) Field(name string) any { return r.fields[name] }

// Podcast is a podcast the account's library shows (subscribed, archived, or
// not subscribed: a preview the account keeps).
type Podcast struct {
	ID           string
	Records      map[string]Record
	State        string // subscribed, archived or not_subscribed
	Title        *string
	DisplayTitle string // the title, else the feed's address (Android's human-readable identifier)
	ImageURL     string
	KeepUpdated  bool
	Rank         int64 // creation order: Android's feed IDs
}

// Episode is an episode of a shown podcast with its derived values.
type Episode struct {
	ID          string
	Podcast     *Podcast
	Records     map[string]Record
	Occurrence  map[string]any
	Placeholder bool // no catalog metadata yet: occurrence evidence stands in

	Title      *string
	PubDate    int64
	Read       int // Android's read column: -1 new, 0 unplayed, 1 played
	Position   int64
	HasMedia   bool
	Duration   int64 // the measured duration, else the feed's (with media)
	Size       int64
	Link       *string
	Queued     bool
	Favorite   bool
	History    int64 // 0 when not in history
	LastPlayed int64
	Rank       int64 // catalog insertion order: Android's item IDs
	queueKey   contract.QueueKey
}

// Library is an account's records at one head.
type Library struct {
	Head     storage.Head
	Account  map[string]Record
	Podcasts map[string]*Podcast
	Episodes []*Episode // in Rank order
	ByID     map[string]*Episode
	Queue    []*Episode // in queue key order
}

// Load reads an account's library from one read snapshot.
func Load(acct *storage.AccountReader) (*Library, error) {
	head, err := acct.Head()
	if err != nil {
		return nil, err
	}
	rows, err := acct.ViewRecords()
	if err != nil {
		return nil, err
	}
	lib := &Library{Head: head, Account: map[string]Record{}, Podcasts: map[string]*Podcast{}, ByID: map[string]*Episode{}}
	byEntity := map[[2]string]map[string]Record{}
	for _, r := range rows {
		rec, err := decodeRecord(r)
		if err != nil {
			return nil, err
		}
		if r.Entity == "account" {
			lib.Account[r.FieldGroup] = rec
			continue
		}
		k := [2]string{r.Entity, r.EntityID}
		if byEntity[k] == nil {
			byEntity[k] = map[string]Record{}
		}
		byEntity[k][r.FieldGroup] = rec
	}
	ranks, err := acct.PodcastRanks()
	if err != nil {
		return nil, err
	}
	for k, recs := range byEntity {
		if k[0] != "podcast" {
			continue
		}
		state, _ := recs["subscription"].Field("state").(string)
		if state != "subscribed" && state != "archived" && state != "not_subscribed" {
			continue // removed or merged: Android has no such feed
		}
		p := &Podcast{ID: k[1], Records: recs, State: state, Rank: ranks[k[1]]}
		if t, ok := recs["metadata"].Field("title").(string); ok {
			p.Title = &t
		}
		// Android's getHumanReadableIdentifier: the title, else the feed's address.
		p.DisplayTitle, _ = recs["identity"].Field("refresh_url").(string)
		if p.Title != nil && *p.Title != "" {
			p.DisplayTitle = *p.Title
		}
		p.ImageURL, _ = recs["metadata"].Field("image_url").(string)
		p.KeepUpdated, _ = recs["policy"].Field("keep_updated").(bool)
		lib.Podcasts[p.ID] = p
	}
	catalog, err := acct.CatalogRows()
	if err != nil {
		return nil, err
	}
	pubDates, order := map[string]int64{}, map[string]int64{}
	for _, c := range catalog {
		pubDates[c.EpisodeID], order[c.EpisodeID] = c.PubDate, c.Order
	}
	registered, err := acct.EpisodeRows()
	if err != nil {
		return nil, err
	}
	for _, row := range registered {
		recs := byEntity[[2]string{"episode", row.EpisodeID}]
		identity, ok := recs["identity"]
		if !ok || identity.Field("merged_into") != nil {
			continue // hidden, or a redirect to its survivor
		}
		podcastID, _ := identity.Field("podcast_id").(string)
		p := lib.Podcasts[podcastID]
		if p == nil {
			continue
		}
		e := &Episode{ID: row.EpisodeID, Podcast: p, Records: recs, Rank: 1 << 62} // not in the catalog: after every catalog item
		if occ, ok := identity.Field("occurrence").(map[string]any); ok {
			e.Occurrence = occ
		}
		if r, ok := order[e.ID]; ok {
			e.Rank = r
		}
		derive(e, pubDates)
		lib.Episodes = append(lib.Episodes, e)
		lib.ByID[e.ID] = e
	}
	sort.SliceStable(lib.Episodes, func(i, j int) bool {
		a, b := lib.Episodes[i], lib.Episodes[j]
		if a.Rank != b.Rank {
			return a.Rank < b.Rank
		}
		return a.ID < b.ID
	})
	for _, e := range lib.Episodes {
		if e.Queued {
			lib.Queue = append(lib.Queue, e)
		}
	}
	sort.SliceStable(lib.Queue, func(i, j int) bool {
		if c := contract.CompareQueueKeys(lib.Queue[i].queueKey, lib.Queue[j].queueKey); c != 0 {
			return c < 0
		}
		return lib.Queue[i].ID < lib.Queue[j].ID
	})
	return lib, nil
}

func decodeRecord(r storage.RecordRow) (Record, error) {
	rec := Record{Value: json.RawMessage(r.Value), Provenance: []string{}}
	dec := json.NewDecoder(bytes.NewReader([]byte(r.Value)))
	dec.UseNumber()
	if err := dec.Decode(&rec.fields); err != nil {
		return rec, err
	}
	if r.Provenance != "" {
		if err := json.Unmarshal([]byte(r.Provenance), &rec.Provenance); err != nil {
			return rec, err
		}
	}
	return rec, nil
}

// derive computes the values Android's columns hold for an episode.
func derive(e *Episode, pubDates map[string]int64) {
	meta, hasMeta := e.Records["metadata"]
	e.Placeholder = !hasMeta
	if hasMeta {
		if t, ok := meta.Field("title").(string); ok {
			e.Title = &t
		}
		if l, ok := meta.Field("link").(string); ok {
			e.Link = &l
		}
		if m, ok := meta.Field("media").(map[string]any); ok {
			if u, _ := m["url"].(string); u != "" {
				e.HasMedia = true
				e.Duration, _ = Int(m["duration"])
				e.Size, _ = Int(m["size"]) // a decimal string (PROTOCOL §9.4)
			}
		}
	} else {
		if t, ok := e.Occurrence["title"].(string); ok {
			e.Title = &t
		}
		u, _ := e.Occurrence["enclosure_url"].(string)
		e.HasMedia = u != ""
	}
	// Android's pubDate: the feed's date, else the date it first saw the item.
	if d, ok := Int(meta.Field("pub_date")); ok && hasMeta {
		e.PubDate = d
	} else if d, ok := pubDates[e.ID]; ok {
		e.PubDate = d
	} else if d, ok := Int(e.Occurrence["publication_time"]); ok {
		e.PubDate = d
	}
	if d, ok := Int(e.Records["measured_duration"].Field("duration_ms")); ok && d > 0 {
		e.Duration = d
	}
	switch e.Records["play_state"].Field("play_state") {
	case "new":
		e.Read = -1
	case "played":
		e.Read = 1
	}
	e.Position, _ = Int(e.Records["position"].Field("position"))
	e.Favorite, _ = e.Records["favorite"].Field("on").(bool)
	e.History, _ = Int(e.Records["history"].Field("time"))
	e.LastPlayed, _ = Int(e.Records["last_played"].Field("time"))
	if in, _ := e.Records["queue_membership"].Field("in_queue").(bool); in {
		if s, ok := e.Records["queue_position"].Field("key").(string); ok {
			if k, err := contract.ParseQueueKey(s); err == nil {
				e.Queued, e.queueKey = true, k
			}
		}
	}
}

// Int reads an integer JSON value, or an integer in a decimal string.
func Int(v any) (int64, bool) {
	switch n := v.(type) {
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case float64:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}
