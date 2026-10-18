package views

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// ---- work counters

// WorkCounts are the read model's work since the process started, for
// tests that bound it: records decoded, entities built, full builds,
// applies, changed entities read, storage rows read, and tree nodes
// visited and allocated.
type WorkCounts struct {
	RecordsDecoded, BytesDecoded, EntitiesBuilt, FullBuilds, Applies, Changes, RowsRead int64
	NodesVisited, NodesAllocated                                                        int64
	// The screens' work: rows encoded, and Home's candidates looked at
	// (Surprise's picks and recent plays, Continue's skipped entries).
	RowsEncoded, Candidates int64
}

var work struct {
	recordsDecoded, bytesDecoded, entitiesBuilt, fullBuilds, applies, changes, rowsRead atomic.Int64
	rowsEncoded, candidates                                                             atomic.Int64
}

// Work returns the work counters.
func Work() WorkCounts {
	v, a := ptree.Counts()
	return WorkCounts{RecordsDecoded: work.recordsDecoded.Load(), BytesDecoded: work.bytesDecoded.Load(), EntitiesBuilt: work.entitiesBuilt.Load(),
		FullBuilds: work.fullBuilds.Load(), Applies: work.applies.Load(), Changes: work.changes.Load(), RowsRead: work.rowsRead.Load(),
		NodesVisited: v, NodesAllocated: a, RowsEncoded: work.rowsEncoded.Load(), Candidates: work.candidates.Load()}
}

// Minus is the work between two readings.
func (w WorkCounts) Minus(o WorkCounts) WorkCounts {
	return WorkCounts{w.RecordsDecoded - o.RecordsDecoded, w.BytesDecoded - o.BytesDecoded, w.EntitiesBuilt - o.EntitiesBuilt, w.FullBuilds - o.FullBuilds,
		w.Applies - o.Applies, w.Changes - o.Changes, w.RowsRead - o.RowsRead, w.NodesVisited - o.NodesVisited, w.NodesAllocated - o.NodesAllocated,
		w.RowsEncoded - o.RowsEncoded, w.Candidates - o.Candidates}
}

// ---- builders: Load's derivation, per entity

// decoded is an entity's record values, decoded for the build and dropped
// after it.
type decoded map[string]map[string]any

func decodeRecords(recs map[string]RawRecord) (decoded, error) {
	out := make(decoded, len(recs))
	var size int64
	for _, r := range recs {
		size += int64(len(r.Value))
	}
	work.bytesDecoded.Add(size)
	for g, r := range recs {
		var fields map[string]any
		dec := json.NewDecoder(bytes.NewReader(r.Value))
		dec.UseNumber()
		if err := dec.Decode(&fields); err != nil {
			return nil, fmt.Errorf("views: record %s: %w", g, err)
		}
		out[g] = fields
	}
	work.recordsDecoded.Add(int64(len(recs)))
	return out, nil
}

func str(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// buildPodcast derives a podcast from its records and rank, as Load does;
// nil when the library does not show it (removed or merged).
func buildPodcast(id string, recs map[string]RawRecord, rank int64) (*PodcastItem, error) {
	work.entitiesBuilt.Add(1)
	if len(recs) == 0 {
		return nil, nil
	}
	d, err := decodeRecords(recs)
	if err != nil {
		return nil, err
	}
	state, _ := str(d["subscription"]["state"])
	if state != "subscribed" && state != "archived" && state != "not_subscribed" {
		return nil, nil
	}
	p := &PodcastItem{ID: id, Records: recs, State: state, Rank: rank}
	if t, ok := str(d["metadata"]["title"]); ok {
		p.Title = &t
	}
	// Android's getHumanReadableIdentifier: the title, else the feed's address.
	p.DisplayTitle, _ = str(d["identity"]["refresh_url"])
	if p.Title != nil && *p.Title != "" {
		p.DisplayTitle = *p.Title
	}
	p.ImageURL, _ = str(d["metadata"]["image_url"])
	p.KeepUpdated, _ = d["policy"]["keep_updated"].(bool)
	p.generation = d["subscription"]["generation_id"]
	p.GenerationID, _ = str(p.generation)
	_, p.HasCredentials = recs["credentials"]
	p.SecretID, _ = str(d["credentials"]["secret_id"])
	return p, nil
}

// buildEpisode derives an episode from its records, its registration and
// its catalog item, as Load and derive do; nil when it is not stored
// (unregistered, without identity, or merged into another).
func buildEpisode(id string, recs map[string]RawRecord, registered bool, cat *storage.CatalogRow) (*EpisodeItem, error) {
	work.entitiesBuilt.Add(1)
	if !registered {
		return nil, nil
	}
	if _, ok := recs["identity"]; !ok {
		return nil, nil
	}
	d, err := decodeRecords(recs)
	if err != nil {
		return nil, err
	}
	identity := d["identity"]
	if identity["merged_into"] != nil {
		return nil, nil // a redirect to its survivor
	}
	e := &EpisodeItem{ID: id, Records: recs, Rank: 1 << 62} // not in the catalog: after every catalog item
	e.PodcastID, _ = str(identity["podcast_id"])
	occ, _ := identity["occurrence"].(map[string]any)
	if cat != nil {
		e.Rank = cat.Order
	}
	meta := d["metadata"]
	_, hasMeta := recs["metadata"]
	e.Placeholder = !hasMeta
	if hasMeta {
		if t, ok := str(meta["title"]); ok {
			e.Title = &t
		}
		if l, ok := str(meta["link"]); ok {
			e.Link = &l
		}
		if m, ok := meta["media"].(map[string]any); ok {
			if u, _ := str(m["url"]); u != "" {
				e.HasMedia = true
				e.Duration, _ = Int(m["duration"])
				e.Size, _ = Int(m["size"]) // a decimal string (PROTOCOL §9.4)
			}
		}
	} else {
		if t, ok := str(occ["title"]); ok {
			e.Title = &t
		}
		u, _ := str(occ["enclosure_url"])
		e.HasMedia = u != ""
	}
	// Android's pubDate: the feed's date, else the date it first saw the item.
	if n, ok := Int(meta["pub_date"]); ok && hasMeta {
		e.PubDate = n
	} else if cat != nil {
		e.PubDate = cat.PubDate
	} else if n, ok := Int(occ["publication_time"]); ok {
		e.PubDate = n
	}
	if n, ok := Int(d["measured_duration"]["duration_ms"]); ok && n > 0 {
		e.Duration = n
	}
	switch d["play_state"]["play_state"] {
	case "new":
		e.Read = -1
	case "played":
		e.Read = 1
	}
	e.Position, _ = Int(d["position"]["position"])
	e.Favorite, _ = d["favorite"]["on"].(bool)
	e.History, _ = Int(d["history"]["time"])
	e.LastPlayed, _ = Int(d["last_played"]["time"])
	if in, _ := d["queue_membership"]["in_queue"].(bool); in {
		if s, ok := str(d["queue_position"]["key"]); ok {
			if k, err := contract.ParseQueueKey(s); err == nil {
				e.Queued, e.QueueKey = true, k
			}
		}
	}
	// The media endpoints' values.
	e.ImageURL, _ = str(meta["image_url"])
	if m, ok := meta["media"].(map[string]any); ok {
		e.SourceURL, _ = str(m["url"])
		e.DeclaredType, _ = str(m["mime_type"])
	}
	if e.SourceURL == "" {
		e.SourceURL, _ = str(occ["enclosure_url"])
	}
	e.ChapterURL, _ = str(meta["chapter_url"])
	e.TranscriptURL, _ = str(meta["transcript_url"])
	e.TranscriptType, _ = str(meta["transcript_type"])
	if meta == nil {
		meta = map[string]any{}
	}
	e.Parts = stats.Parts(d, meta)
	e.Class = ClassOf(e.Read, e.HasMedia, e.Position, e.Queued, e.Favorite, e.History)
	return e, nil
}

// mergeAccount merges the account entity's records as Load does: by entity
// ID, then field group, the last one winning.
func mergeAccount(rows map[string]map[string]RawRecord) map[string]RawRecord {
	out := map[string]RawRecord{}
	ids := make([]string, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		for g, r := range rows[id] {
			out[g] = r
		}
	}
	return out
}

func rawRecord(r storage.RecordRow) RawRecord {
	return RawRecord{Value: json.RawMessage(r.Value), Provenance: json.RawMessage(r.Provenance)}
}

// ---- an episode's places in the structures

// partitions are a queued episode's Continue partitions.
func partitions(e *EpisodeItem) (pos, recent, zero, neg bool) {
	switch {
	case !e.HasMedia:
		return false, false, true, false
	case e.Position >= 1000:
		return e.LastPlayed > 0, false, e.LastPlayed == 0, e.LastPlayed < 0
	default:
		return false, e.LastPlayed > 0, e.LastPlayed <= 0, false
	}
}

// surpriseCandidate reports an episode's Surprise structure: never played,
// or played.
func surpriseCandidate(e *EpisodeItem) (never, played bool) {
	if (e.Read != -1 && e.Read != 0) || !e.HasMedia {
		return false, false
	}
	return e.LastPlayed == 0, e.LastPlayed > 0
}

// monthly is an episode's played time by UTC month: units last played and
// played.
func monthly(e *EpisodeItem) map[int]int64 {
	var out map[int]int64
	for _, x := range e.Parts {
		if x.LastPlayed > 0 && x.PlayedMS > 0 {
			if out == nil {
				out = map[int]int64{}
			}
			out[monthOf(x.LastPlayed)] += x.PlayedMS
		}
	}
	return out
}

// contribute adds (sign 1) or removes (sign −1) an episode's counts and
// statistics to its podcast's aggregates.
func (a *PodAgg) contribute(e *EpisodeItem, sign int64) {
	a.Hist[e.Class] += int32(sign)
	for _, x := range e.Parts {
		a.Parts += sign
		a.TotalMS += sign * x.DurationMS
		if 0 <= x.LastPlayed {
			a.MarkedAll += sign * markedValue(x)
		}
		started := x.History && x.PlayedMS > 0
		if started {
			a.Started += sign
		}
		if started || x.Played || x.PositionMS > 0 {
			a.StartedMarked += sign
		}
	}
}

// ---- the full build

type sortedEntries[K, V any] struct {
	keys []K
	vals []V
}

func (s *sortedEntries[K, V]) add(k K, v V) { s.keys, s.vals = append(s.keys, k), append(s.vals, v) }

func (s *sortedEntries[K, V]) tree(spec *ptree.Spec[K, V]) (ptree.Tree[K, V], error) {
	idx := make([]int, len(s.keys))
	for i := range idx {
		idx[i] = i
	}
	slices.SortFunc(idx, func(a, b int) int { return spec.Compare(s.keys[a], s.keys[b]) })
	keys, vals := make([]K, len(idx)), make([]V, len(idx))
	for i, j := range idx {
		keys[i], vals[i] = s.keys[j], s.vals[j]
	}
	return ptree.FromSorted(spec, keys, vals)
}

// BuildVersion builds the account's version in full from one snapshot: the
// reads Load makes, the same derivation per entity, every structure built
// from its sorted entries.
func BuildVersion(acct *storage.AccountReader, account string) (*Version, error) {
	work.fullBuilds.Add(1)
	head, err := acct.Head()
	if err != nil {
		return nil, err
	}
	clock, err := acct.ViewClock()
	if err != nil {
		return nil, err
	}
	rows, err := acct.ViewRecords()
	if err != nil {
		return nil, err
	}
	ranks, err := acct.PodcastRanks()
	if err != nil {
		return nil, err
	}
	catalog, err := acct.CatalogRows()
	if err != nil {
		return nil, err
	}
	registered, err := acct.EpisodeRows()
	if err != nil {
		return nil, err
	}
	work.rowsRead.Add(int64(len(rows) + len(ranks) + len(catalog) + len(registered)))
	v := emptyVersion(account)
	v.Head, v.Clock = head, clock
	byEntity := map[[2]string]map[string]RawRecord{}
	for _, r := range rows {
		if r.Entity == "account" {
			if v.accountRows[r.EntityID] == nil {
				v.accountRows[r.EntityID] = map[string]RawRecord{}
			}
			v.accountRows[r.EntityID][r.FieldGroup] = rawRecord(r)
			continue
		}
		k := [2]string{r.Entity, r.EntityID}
		if byEntity[k] == nil {
			byEntity[k] = map[string]RawRecord{}
		}
		byEntity[k][r.FieldGroup] = rawRecord(r)
	}
	v.AccountRecords = mergeAccount(v.accountRows)
	var podcasts sortedEntries[string, *PodcastItem]
	for k, recs := range byEntity {
		if k[0] != "podcast" {
			continue
		}
		p, err := buildPodcast(k[1], recs, ranks[k[1]])
		if err != nil {
			return nil, err
		}
		if p != nil {
			podcasts.add(p.ID, p)
		}
	}
	if v.podcasts, err = podcasts.tree(podcastSpec); err != nil {
		return nil, err
	}
	cat := make(map[string]*storage.CatalogRow, len(catalog))
	for i := range catalog {
		cat[catalog[i].EpisodeID] = &catalog[i]
	}
	var stored sortedEntries[string, *EpisodeItem]
	var orders [numOrders]sortedEntries[orderKey, *EpisodeItem]
	var queue, pos, recent, zero, timeZero, neg sortedEntries[queueKey, *EpisodeItem]
	var surprise sortedEntries[surpriseKey, *EpisodeItem]
	var played sortedEntries[playedKey, *EpisodeItem]
	var statEntries sortedEntries[statKey, statValue]
	aggs := map[string]*PodAgg{}
	podMonths := map[podMonthKey]int64{}
	for _, row := range registered {
		e, err := buildEpisode(row.EpisodeID, byEntity[[2]string{"episode", row.EpisodeID}], true, cat[row.EpisodeID])
		if err != nil {
			return nil, err
		}
		if e == nil {
			continue
		}
		stored.add(e.ID, e)
		for i, d := range orderDefs {
			orders[i].add(d.key(e), e)
		}
		if e.Queued {
			queue.add(queueOrder(e), e)
			p, r, z, n := partitions(e)
			switch {
			case p:
				pos.add(recentOrder(e), e)
			case r:
				recent.add(recentOrder(e), e)
				timeZero.add(queueOrder(e), e)
			case z:
				zero.add(queueOrder(e), e)
			case n:
				neg.add(recentOrder(e), e)
			}
		}
		switch never, wasPlayed := surpriseCandidate(e); {
		case never:
			surprise.add(surpriseKey{e.PodcastID, SurpriseBase(e.ID), e.ID}, e)
		case wasPlayed:
			played.add(playedKey{e.PodcastID, e.LastPlayed, e.ID}, e)
		}
		for i, x := range e.Parts {
			statEntries.add(statKey{e.PodcastID, x.LastPlayed, e.ID, i}, statValue{x.PlayedMS, markedValue(x)})
		}
		a := aggs[e.PodcastID]
		if a == nil {
			a = &PodAgg{}
			aggs[e.PodcastID] = a
		}
		a.contribute(e, 1)
		for m, ms := range monthly(e) {
			podMonths[podMonthKey{e.PodcastID, m}] += ms
		}
	}
	trees := []func() error{
		func() (err error) { v.stored, err = stored.tree(storedSpec); return err },
		func() (err error) { v.queue, err = queue.tree(queueSpec); return err },
		func() (err error) { v.pos, err = pos.tree(queueSpec); return err },
		func() (err error) { v.recent, err = recent.tree(queueSpec); return err },
		func() (err error) { v.zero, err = zero.tree(queueSpec); return err },
		func() (err error) { v.timeZero, err = timeZero.tree(queueSpec); return err },
		func() (err error) { v.neg, err = neg.tree(queueSpec); return err },
		func() (err error) { v.surprise, err = surprise.tree(surpriseSpec); return err },
		func() (err error) { v.recentPlayed, err = played.tree(playedSpec); return err },
		func() (err error) { v.stats, err = statEntries.tree(statSpec); return err },
	}
	for i := range orders {
		trees = append(trees, func() (err error) { v.orders[i], err = orders[i].tree(orderSpecs[i]); return err })
	}
	for _, f := range trees {
		if err := f(); err != nil {
			return nil, err
		}
	}
	var aggEntries sortedEntries[string, *PodAgg]
	for p, a := range aggs {
		aggEntries.add(p, a)
	}
	var pm sortedEntries[podMonthKey, int64]
	months := map[int]int64{}
	for k, ms := range podMonths {
		pm.add(k, ms)
		if _, shown := v.podcasts.Get(k.podcast); shown {
			months[k.month] += ms
		}
	}
	var ms sortedEntries[int, int64]
	for m, n := range months {
		ms.add(m, n)
	}
	if v.agg, err = aggEntries.tree(aggSpec); err != nil {
		return nil, err
	}
	if v.podMonths, err = pm.tree(podMonthSpec); err != nil {
		return nil, err
	}
	if v.months, err = ms.tree(monthSpec); err != nil {
		return nil, err
	}
	return v, nil
}

// EntityRecords groups record rows by entity ID.
func entityRecords(rows []storage.RecordRow) map[string]map[string]RawRecord {
	out := map[string]map[string]RawRecord{}
	for _, r := range rows {
		if out[r.EntityID] == nil {
			out[r.EntityID] = map[string]RawRecord{}
		}
		out[r.EntityID][r.FieldGroup] = rawRecord(r)
	}
	return out
}
