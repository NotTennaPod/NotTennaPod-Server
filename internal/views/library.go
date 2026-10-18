// Package views computes the browser's read views (PROTOCOL §12) from an
// account's published records at one head: Android 3.12.1's subscription
// list, episode filters and sort orders, Inbox, queue, history, statistics and
// Home sections. The views read published records only, never the archive, so
// they show exactly what phones show. The Android behaviour each view follows
// is listed in PROTOCOL §12.
//
// The views are served from the account's incremental library model
// (model.go): a Version at the snapshot's view key, kept current by applying
// the read model's captured changes (Cache), and read in O(output + ranges ×
// log n) per screen (query.go, screens.go, home.go).
package views

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Record is a published record as rows show it: its value and the nodes that
// determine it.
type Record struct {
	Value      json.RawMessage `json:"value"`
	Provenance []string        `json:"provenance"`

	fields map[string]any
}

// Field returns a top-level field of the record's value (records from
// Library.AccountRecords).
func (r Record) Field(name string) any { return r.fields[name] }

// record makes a row's record from a stored one: its provenance decoded and,
// with fields, its value's top-level fields.
func record(r RawRecord, fields bool) (Record, error) {
	rec := Record{Value: r.Value, Provenance: []string{}}
	if len(r.Provenance) > 0 {
		if err := json.Unmarshal(r.Provenance, &rec.Provenance); err != nil {
			return rec, err
		}
	}
	if fields {
		dec := json.NewDecoder(bytes.NewReader(r.Value))
		dec.UseNumber()
		if err := dec.Decode(&rec.fields); err != nil {
			return rec, err
		}
	}
	return rec, nil
}

func records(recs map[string]RawRecord, skip string) (map[string]Record, error) {
	out := make(map[string]Record, len(recs))
	for g, r := range recs {
		if g == skip {
			continue
		}
		rec, err := record(r, false)
		if err != nil {
			return nil, err
		}
		out[g] = rec
	}
	return out, nil
}

// Library is an account's library at one read snapshot: the version at the
// snapshot's view key, and the snapshot's head (which a version shares with
// every later head that changed no view input).
type Library struct {
	Head storage.Head
	V    *Version

	inc uint64 // the account's incarnation it was published or served under (Cache)
	pub uint64 // its publication's number in the Cache, 0 for a library made elsewhere
}

// AccountRecords returns the account's own records, with their fields.
func (l *Library) AccountRecords() (map[string]Record, error) {
	out := make(map[string]Record, len(l.V.AccountRecords))
	for g, r := range l.V.AccountRecords {
		rec, err := record(r, true)
		if err != nil {
			return nil, err
		}
		out[g] = rec
	}
	return out, nil
}

// Podcast returns a podcast the library shows, nil for another.
func (l *Library) Podcast(id string) *PodcastItem {
	p, _ := l.V.podcasts.Get(id)
	return p
}

// Episode returns an episode the library shows (stored, of a shown podcast)
// and its podcast; nil for another. O(log n).
func (l *Library) Episode(id string) (*EpisodeItem, *PodcastItem) {
	e, ok := l.V.stored.Get(id)
	if !ok {
		return nil, nil
	}
	p, ok := l.V.podcasts.Get(e.PodcastID)
	if !ok {
		return nil, nil
	}
	return e, p
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
