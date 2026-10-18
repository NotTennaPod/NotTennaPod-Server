package views_test

// The incremental model against a full build after every step of long,
// realistic sequences of commits, with retained versions unchanged and
// the shown projection against the oracle on the way.

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// sequence drives one account through random commits and checks the
// incremental version after each.
type sequence struct {
	t        *testing.T
	store    *storage.Store
	account  string
	r        *rand.Rand
	now      int64
	podcasts []string // known podcast IDs, in creation order
	episodes []string // known episode IDs, in creation order
	n        int      // a counter for fresh IDs and values
	prev     *views.Version
	trace    []string
	bins     map[string]int
	paths    map[views.Path]int
	retained []retainedVersion
	refEvery int
	// The query matrix on the way: the whole matrix every matrixEvery steps (else
	// at the last step), and rotating queries at every step (each list
	// compared whole, or, windowed, at two windows).
	matrixEvery int
	rotating    int
	windowed    bool
	steps       int
	forced      bool   // the step must change something: its pickers find a target that allows it
	appended    bool   // the transaction appended its commit
	witness     int    // the next Continue witness
	note        string // a step's own bin, as well as its kind's
	queried     int    // matrix queries compared
}

type retainedVersion struct {
	v           *views.Version
	fingerprint string
}

func newSequence(t *testing.T, store *storage.Store, account string, seed uint64, now int64) *sequence {
	s := &sequence{t: t, store: store, account: account, r: rand.New(rand.NewPCG(seed, 404)), now: now, bins: map[string]int{},
		paths: map[views.Path]int{}, refEvery: 50}
	viewstest.Skip(store) // this sequence checks every commit itself
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		v, err := views.BuildVersion(r.Account(account), account)
		if err != nil {
			return err
		}
		s.prev = v
		for id := range v.AllPodcasts() {
			s.podcasts = append(s.podcasts, id)
		}
		for id := range v.AllEpisodes() {
			s.episodes = append(s.episodes, id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Podcasts the library does not show, and episodes it does not store,
	// are known too: their records exist.
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		rows, err := r.Account(account).ViewRecords()
		if err != nil {
			return err
		}
		seenP, seenE := map[string]bool{}, map[string]bool{}
		for _, id := range s.podcasts {
			seenP[id] = true
		}
		for _, id := range s.episodes {
			seenE[id] = true
		}
		for _, row := range rows {
			switch {
			case row.Entity == "podcast" && !seenP[row.EntityID]:
				seenP[row.EntityID] = true
				s.podcasts = append(s.podcasts, row.EntityID)
			case row.Entity == "episode" && !seenE[row.EntityID]:
				seenE[row.EntityID] = true
				s.episodes = append(s.episodes, row.EntityID)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(s.podcasts)
	slices.Sort(s.episodes)
	return s
}

func (s *sequence) pickEpisode() string { return s.episodes[s.r.IntN(len(s.episodes))] }
func (s *sequence) pickPodcast() string { return s.podcasts[s.r.IntN(len(s.podcasts))] }

// pickWhere picks a stored episode of the last version passing f; ""
// when none passes. (The step checks its precondition again in its
// transaction, which earlier changes of the step may have changed.)
func (s *sequence) pickWhere(f func(*views.EpisodeItem) bool) string {
	var ids []string
	for id, e := range s.prev.AllEpisodes() {
		if f(e) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return ""
	}
	return ids[s.r.IntN(len(ids))]
}

// changed reports a delete or an update that may have changed nothing.
func changed(found bool, err error) error {
	if err == nil && !found {
		return errSkipped
	}
	return err
}

// pickPlaceholder picks a stored episode without metadata, or returns
// otherwise.
func (s *sequence) pickPlaceholder(otherwise string) string {
	var ids []string
	for id, e := range s.prev.AllEpisodes() {
		if e.Placeholder {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return otherwise
	}
	return ids[s.r.IntN(len(ids))]
}

func (s *sequence) fresh() int { s.n++; return s.n }

func (s *sequence) prov() string { return fmt.Sprintf(`["s%d"]`, s.r.IntN(50)) }

// edit changes a record's top-level fields (nil deletes a field), keeping
// it canonical: errSkipped, and nothing written, when they change nothing.
func edit(a *storage.AccountTx, entity, id, group string, fields map[string]any, prov string) error {
	cur := map[string]any{}
	var before []byte
	if rec, err := a.Record(entity, id, group); err == nil {
		dec := json.NewDecoder(strings.NewReader(rec.Value))
		dec.UseNumber()
		if err := dec.Decode(&cur); err != nil {
			return err
		}
		if before, err = json.Marshal(cur); err != nil {
			return err
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	for k, v := range fields {
		if v == nil {
			delete(cur, k)
		} else {
			cur[k] = v
		}
	}
	b, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	if before != nil && string(before) == string(b) {
		return errSkipped
	}
	return a.PutRecord(storage.RecordRow{Entity: entity, EntityID: id, FieldGroup: group, Value: string(b), Provenance: prov})
}

// errSkipped reports a change whose precondition did not hold (nothing to
// change): it is not counted in the coverage bins.
var errSkipped = errors.New("nothing to change")

type seqStep struct {
	name   string
	weight int
	run    func(s *sequence, a *storage.AccountTx) error
	bulk   bool // a suspended rewrite: a new generation
}

// restoreTransition is the step that moves the store to a new lineage (its
// run is not called).
const restoreTransition = "restore transition"

// put writes an episode's record: errSkipped, and nothing written, when
// it holds that value already (a step counts only a change it made).
func (s *sequence) put(a *storage.AccountTx, e, group, value string) error {
	return s.putRecord(a, "episode", e, group, value)
}

func (s *sequence) putRecord(a *storage.AccountTx, entity, id, group, value string) error {
	rec, err := a.Record(entity, id, group)
	switch {
	case err == nil && rec.Value == value:
		return errSkipped
	case err != nil && !errors.Is(err, storage.ErrNotFound):
		return err
	}
	return a.PutRecord(storage.RecordRow{Entity: entity, EntityID: id, FieldGroup: group, Value: value, Provenance: s.prov()})
}

// puts writes an episode's records (group, value pairs): nil when one of
// them changed.
func (s *sequence) puts(a *storage.AccountTx, e string, pairs ...string) error {
	changed := false
	for i := 0; i < len(pairs); i += 2 {
		switch err := s.put(a, e, pairs[i], pairs[i+1]); {
		case err == nil:
			changed = true
		case !errors.Is(err, errSkipped):
			return err
		}
	}
	if !changed {
		return errSkipped
	}
	return nil
}

// queued reports whether the episode is queued in the transaction's state.
func queued(a *storage.AccountTx, e string) (bool, error) {
	rec, err := a.Record("episode", e, "queue_membership")
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	return err == nil && rec.Value == `{"in_queue":true}`, ignoreNotFound(err)
}

func (s *sequence) newEpisode(a *storage.AccountTx, p string) (string, error) {
	e := fmt.Sprintf("e1:new-%06d", s.fresh())
	recs, registered, cat := viewstest.EpisodeRecords(s.r, viewstest.Spec{Now: s.now, Months: 24}, p, e, 999, s.n, "")
	if registered {
		if err := a.AddEpisode(e, p, `{}`, "n"); err != nil {
			return "", err
		}
	}
	if cat != nil {
		if err := a.PutCatalogItem(*cat); err != nil {
			return "", err
		}
	}
	for _, r := range recs {
		if err := a.PutRecord(r); err != nil {
			return "", err
		}
	}
	s.episodes = append(s.episodes, e)
	return e, nil
}

var seqSteps = []seqStep{
	{"play state", 6, func(s *sequence, a *storage.AccountTx) error {
		return s.put(a, s.pickEpisode(), "play_state", fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, []string{"new", "unplayed", "played"}[s.r.IntN(3)]))
	}, false},
	{"position", 5, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickEpisode()
		if s.r.IntN(5) == 0 {
			return changed(a.DeleteRecord("episode", e, "position"))
		}
		return s.put(a, e, "position", fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, []int64{0, 999, 1000, 1001, 600_000}[s.r.IntN(5)]))
	}, false},
	{"last played", 5, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickEpisode()
		last := []int64{s.now, s.now - 29_999, s.now - 30_000, s.now - 3_600_000, s.now - 3_600_001, 0, -3, s.now - int64(s.r.IntN(3000))*viewstest.Day}[s.r.IntN(8)]
		return s.puts(a, e, "last_played", fmt.Sprintf(`{"time":%d}`, last), "played_time", fmt.Sprintf(`{"played_ms":%d}`, int64(s.r.IntN(30))*60_000))
	}, false},
	{"history", 3, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickEpisode()
		if s.r.IntN(3) == 0 {
			return changed(a.DeleteRecord("episode", e, "history"))
		}
		return s.put(a, e, "history", fmt.Sprintf(`{"time":%d}`, s.now-int64(s.r.IntN(400))*viewstest.Day))
	}, false},
	{"favourite", 3, func(s *sequence, a *storage.AccountTx) error {
		return s.put(a, s.pickEpisode(), "favorite", fmt.Sprintf(`{"on":%v}`, s.r.IntN(2) == 0))
	}, false},
	{"queue add", 4, func(s *sequence, a *storage.AccountTx) error {
		// An episode not queued becomes queued.
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return !e.Queued })
		if in, err := queued(a, e); e == "" || in || err != nil {
			return cmp.Or(err, errSkipped)
		}
		if err := s.put(a, e, "queue_membership", `{"in_queue":true}`); err != nil {
			return err
		}
		if err := s.put(a, e, "queue_position", fmt.Sprintf(`{"key":%q}`, viewstest.QueueKeyAt(s.r.IntN(400)))); err != nil && !errors.Is(err, errSkipped) {
			return err
		}
		return nil
	}, false},
	{"queue remove", 3, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return e.Queued })
		if e == "" {
			return errSkipped
		}
		if in, err := queued(a, e); !in || err != nil {
			return cmp.Or(err, errSkipped) // not queued: nothing to remove
		}
		if s.r.IntN(2) == 0 {
			return changed(a.DeleteRecord("episode", e, "queue_membership"))
		}
		return s.put(a, e, "queue_membership", `{"in_queue":false}`)
	}, false},
	{"queue move", 3, func(s *sequence, a *storage.AccountTx) error {
		// A queued episode moves to another key.
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return e.Queued })
		if in, err := queued(a, e); e == "" || !in || err != nil {
			return cmp.Or(err, errSkipped)
		}
		return s.put(a, e, "queue_position", fmt.Sprintf(`{"key":%q}`, viewstest.QueueKeyAt(400+s.fresh())))
	}, false},
	{"metadata", 5, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickEpisode()
		f := map[string]any{}
		switch s.r.IntN(6) {
		case 0:
			f["title"] = []any{"alpha", "Alpha", "zeta", "Émile", "2 things", "10 things"}[s.r.IntN(6)]
		case 1:
			f["pub_date"] = s.now - int64(s.r.IntN(900))*viewstest.Day
		case 2:
			f["media"] = map[string]any{"duration": s.r.IntN(5) * 600_000, "mime_type": "audio/mpeg", "size": strconv.Itoa(s.r.IntN(4) * 1_000_000),
				"url": fmt.Sprintf("https://example.invalid/m/%d.mp3", s.fresh())}
		case 3:
			f["media"] = json.RawMessage("null") // no media
		case 4:
			f["link"] = []any{"https://example.invalid/a", "https://example.invalid/B"}[s.r.IntN(2)]
		default:
			f["image_url"] = fmt.Sprintf("https://example.invalid/i/%d.jpg", s.fresh())
		}
		return edit(a, "episode", e, "metadata", f, s.prov())
	}, false},
	{"metadata removed", 1, func(s *sequence, a *storage.AccountTx) error {
		return changed(a.DeleteRecord("episode", s.pickWhere(func(e *views.EpisodeItem) bool { return !e.Placeholder }), "metadata"))
	}, false},
	{"description only", 2, func(s *sequence, a *storage.AccountTx) error {
		// Only what list views drop changes: the projected value stays.
		return edit(a, "episode", s.pickEpisode(), "metadata", map[string]any{"description": fmt.Sprintf("<p>%d</p>", s.fresh()),
			"chapters": []any{map[string]any{"start": s.r.IntN(9), "title": "c"}}}, s.prov())
	}, false},
	{"provenance only", 3, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickWhere(func(e *views.EpisodeItem) bool { _, ok := e.Records["play_state"]; return ok })
		rec, err := a.Record("episode", e, "play_state")
		if errors.Is(err, storage.ErrNotFound) {
			return errSkipped
		}
		if err != nil {
			return err
		}
		return a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: "play_state", Value: rec.Value, Provenance: fmt.Sprintf(`["p%d"]`, s.fresh())})
	}, false},
	{"identity move", 3, func(s *sequence, a *storage.AccountTx) error {
		return edit(a, "episode", s.pickEpisode(), "identity", map[string]any{"podcast_id": s.pickPodcast()}, s.prov())
	}, false},
	{"merged", 2, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickEpisode()
		if s.r.IntN(2) == 0 {
			return edit(a, "episode", e, "identity", map[string]any{"merged_into": s.pickEpisode()}, s.prov())
		}
		return edit(a, "episode", e, "identity", map[string]any{"merged_into": nil}, s.prov())
	}, false},
	{"register", 3, func(s *sequence, a *storage.AccountTx) error {
		if s.r.IntN(2) == 0 {
			_, err := s.newEpisode(a, s.pickPodcast())
			return err
		}
		err := a.AddEpisode(s.pickEpisode(), s.pickPodcast(), `{}`, "n")
		if err != nil && strings.Contains(err.Error(), "UNIQUE") {
			return errSkipped // registered already
		}
		return err
	}, false},
	{"unregister", 2, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickWhere(func(*views.EpisodeItem) bool { return true })
		if e == "" {
			return errSkipped
		}
		return changed(a.UnregisterEpisodeForTesting(e)) // not registered: nothing to remove
	}, false},
	{"catalog reinsert", 3, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return e.Rank != 1<<62 })
		it, ok, err := a.CatalogItem(e)
		if err != nil {
			return err
		}
		if !ok {
			return errSkipped
		}
		if err := a.DeleteCatalogItem(e); err != nil {
			return err
		}
		return a.PutCatalogItem(it) // a new insertion rank
	}, false},
	{"catalog date", 2, func(s *sequence, a *storage.AccountTx) error {
		// Half of them for an episode without metadata, whose date is
		// the catalog's.
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return e.Rank != 1<<62 })
		if s.r.IntN(2) == 0 {
			e = s.pickPlaceholder(e)
		}
		it, ok, err := a.CatalogItem(e)
		if err != nil {
			return err
		}
		if !ok {
			return errSkipped
		}
		it.PubDate = s.now - int64(s.r.IntN(900))*viewstest.Day
		return a.PutCatalogItem(it)
	}, false},
	{"catalog delete", 1, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return e.Rank != 1<<62 })
		if _, ok, err := a.CatalogItem(e); err != nil || !ok {
			return cmp.Or(err, errSkipped)
		}
		return a.DeleteCatalogItem(e)
	}, false},
	{"podcast state", 3, func(s *sequence, a *storage.AccountTx) error {
		return edit(a, "podcast", s.pickPodcast(), "subscription", map[string]any{"state": []any{"subscribed", "archived", "not_subscribed",
			"removed", "merged", "subscribed"}[s.r.IntN(6)]}, s.prov())
	}, false},
	{"podcast metadata", 2, func(s *sequence, a *storage.AccountTx) error {
		return edit(a, "podcast", s.pickPodcast(), "metadata", map[string]any{"title": []any{"alpha", "Beta", "", "Émile"}[s.r.IntN(4)],
			"image_url": fmt.Sprintf("https://example.invalid/p/%d.jpg", s.fresh())}, s.prov())
	}, false},
	{"podcast policy", 1, func(s *sequence, a *storage.AccountTx) error {
		return edit(a, "podcast", s.pickPodcast(), "policy", map[string]any{"keep_updated": s.r.IntN(2) == 0}, s.prov())
	}, false},
	{"podcast created", 1, func(s *sequence, a *storage.AccountTx) error {
		n := s.fresh()
		p, node := fmt.Sprintf("p1:new-%06d", n), fmt.Sprintf("c0de%04x-0000-4000-8000-%012x", n, n)
		if err := a.CreatePodcast(p, node); err != nil {
			return err
		}
		if s.r.IntN(2) == 0 { // its creating node placed now, else later
			if err := a.InsertNode(node, "decision", s.now, nil); err != nil {
				return err
			}
		}
		for _, r := range viewstest.PodcastRecords(s.r, n, p) {
			if err := a.PutRecord(r); err != nil {
				return err
			}
		}
		s.podcasts = append(s.podcasts, p)
		for range 1 + s.r.IntN(4) {
			if _, err := s.newEpisode(a, p); err != nil {
				return err
			}
		}
		return nil
	}, false},
	{"node placed", 2, func(s *sequence, a *storage.AccountTx) error {
		// A podcast's creating node placed late, or another node: ranks
		// stay, or are renumbered when a gap runs out.
		n := s.fresh()
		for i := range 1 + s.r.IntN(30) {
			if err := a.InsertNode(fmt.Sprintf("k%06d-%02d", 999999-n, 99-i), "fact", 1, nil); err != nil {
				return err
			}
		}
		return nil
	}, false},
	{"account policy", 1, func(s *sequence, a *storage.AccountTx) error {
		return s.putRecord(a, "account", s.account, "discovery_policy", fmt.Sprintf(`{"new_episodes_action":%q}`, []string{"inbox", "nothing"}[s.r.IntN(2)]))
	}, false},
	{"statistics units", 2, func(s *sequence, a *storage.AccountTx) error {
		e := s.pickEpisode()
		if s.r.IntN(3) == 0 {
			return changed(a.DeleteRecord("episode", e, "statistics_units"))
		}
		var units []string
		for k := range 1 + s.r.IntN(3) {
			when := s.now - int64(k*40+s.r.IntN(30))*viewstest.Day
			units = append(units, fmt.Sprintf(`{"duration_ms":600000,"history":%d,"last_played":%d,"played":%v,"played_ms":%d,"position":%d}`,
				when, when, s.r.IntN(2) == 0, int64(s.r.IntN(20))*60_000, s.r.IntN(2)*5000))
		}
		return s.put(a, e, "statistics_units", `{"units":[`+strings.Join(units, ",")+`]}`)
	}, false},
	{"measured duration", 2, func(s *sequence, a *storage.AccountTx) error {
		return s.put(a, s.pickEpisode(), "measured_duration", fmt.Sprintf(`{"duration_ms":%d}`, int64(s.r.IntN(5))*60_000))
	}, false},
	{"many marked played", 1, func(s *sequence, a *storage.AccountTx) error {
		// One commit of 1,001 episodes (more than a page of changes).
		start, prov := s.r.IntN(len(s.episodes)), fmt.Sprintf(`["m%d"]`, s.fresh())
		for i := range 1001 {
			e := s.episodes[(start+i)%len(s.episodes)]
			// Each a change: a new provenance, if not a new value.
			if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: "play_state",
				Value: `{"generation_id":"g","play_state":"played"}`, Provenance: prov}); err != nil {
				return err
			}
		}
		return nil
	}, false},
	{"continue witness", 3, func(s *sequence, a *storage.AccountTx) error {
		// A queued episode with media taken across a Continue partition
		// boundary: positions 999, 1000 and 1001, positive, zero and
		// negative last played, a play inside the 30 s window.
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return e.Queued && e.HasMedia })
		if in, err := queued(a, e); e == "" || !in || err != nil {
			return cmp.Or(err, errSkipped)
		}
		k := s.witness % len(continueWitnesses)
		w := continueWitnesses[k]
		last := w.last
		if w.recent {
			last += s.now
		}
		if err := s.puts(a, e, "position", fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, w.position),
			"last_played", fmt.Sprintf(`{"time":%d}`, last)); err != nil {
			return err
		}
		s.witness++
		s.note = fmt.Sprintf("continue witness %d", k) // each case is counted
		return nil
	}, false},
	{"surprise witness", 2, func(s *sequence, a *storage.AccountTx) error {
		// An episode with media: new, unplayed or played, never played or
		// played within the hour.
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return e.HasMedia })
		if e == "" {
			return errSkipped
		}
		return s.puts(a, e, "play_state", fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, []string{"new", "unplayed", "played"}[s.r.IntN(3)]),
			"last_played", fmt.Sprintf(`{"time":%d}`, []int64{0, s.now - 60_000, s.now - 3_600_001}[s.r.IntN(3)]))
	}, false},
	{"episode assets", 2, func(s *sequence, a *storage.AccountTx) error {
		// Chapters and transcripts: set, changed, null or removed.
		e := s.pickWhere(func(e *views.EpisodeItem) bool { return !e.Placeholder })
		f := map[string]any{}
		switch s.r.IntN(4) {
		case 0:
			f["chapter_url"] = fmt.Sprintf("https://example.invalid/c/%d.json", s.fresh())
		case 1:
			f["transcript_url"] = fmt.Sprintf("https://example.invalid/t/%d.vtt", s.fresh())
			f["transcript_type"] = []any{"text/vtt", "application/srt", "application/json"}[s.r.IntN(3)]
		case 2:
			f["chapter_url"], f["transcript_url"], f["transcript_type"] = json.RawMessage("null"), json.RawMessage("null"), json.RawMessage("null")
		default:
			f["chapter_url"], f["transcript_url"], f["transcript_type"] = nil, nil, nil // removed
		}
		if _, err := a.Record("episode", e, "metadata"); err != nil {
			return cmp.Or(ignoreNotFound(err), errSkipped)
		}
		return edit(a, "episode", e, "metadata", f, s.prov())
	}, false},
	{"podcast credentials", 1, func(s *sequence, a *storage.AccountTx) error {
		p := s.pickPodcast()
		switch s.r.IntN(3) {
		case 0:
			return changed(a.DeleteRecord("podcast", p, "credentials"))
		case 1:
			return s.putRecord(a, "podcast", p, "credentials", `{"secret_id":null,"username":"u"}`)
		}
		return s.putRecord(a, "podcast", p, "credentials", fmt.Sprintf(`{"secret_id":"s%d","username":"u"}`, s.fresh()))
	}, false},
	{"podcast generation", 1, func(s *sequence, a *storage.AccountTx) error {
		g := any(fmt.Sprintf("g1:%016x", s.fresh()))
		if s.r.IntN(4) == 0 {
			g = json.RawMessage("null")
		}
		return edit(a, "podcast", s.pickPodcast(), "subscription", map[string]any{"generation_id": g}, s.prov())
	}, false},
	{"log only", 2, func(s *sequence, a *storage.AccountTx) error {
		// A commit to the log alone: the head moves, the view's tick
		// does not.
		if s.appended {
			return errSkipped
		}
		s.appended = true
		_, _, err := a.AppendCommit(storage.CommitBody{})
		return err
	}, false},
	{"new generation", 1, func(s *sequence, a *storage.AccountTx) error {
		// A bulk rewrite (as an import's or a rebuild's publication): the
		// transaction suspends the capture (step).
		prov := fmt.Sprintf(`["b%d"]`, s.fresh())
		for range 20 {
			if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: s.pickEpisode(), FieldGroup: "favorite", Value: `{"on":true}`, Provenance: prov}); err != nil {
				return err
			}
		}
		return nil
	}, true},
}

func init() {
	seqSteps = append(seqSteps, seqStep{name: restoreTransition, weight: 1})
	for _, st := range seqSteps {
		forcedPlan = append(forcedPlan, st.name)
	}
	for range len(continueWitnesses) - 1 {
		forcedPlan = append(forcedPlan, "continue witness")
	}
}

// continueWitnesses are the Continue partition boundaries a queued
// episode with media is taken across: positions 999, 1000 and 1001;
// positive, zero and negative last played; plays within the 30 s window
// (recent: last played counted from now).
var continueWitnesses = []struct {
	position, last int64
	recent         bool
}{{999, 1, false}, {1000, 1, false}, {1001, -3, false}, {1000, 0, false}, {999, -3, false}, {0, 0, false}, {2000, -29_999, true},
	{999, 0, true}, {1000, -1, false}}

// forcedPlan is what the first steps of every sequence change, one each:
// every kind of change, and every Continue witness.
var forcedPlan []string

func stepNamed(name string) seqStep {
	for _, st := range seqSteps {
		if st.name == name {
			return st
		}
	}
	panic(name)
}

func ignoreNotFound(err error) error {
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	return err
}

// run makes the step's change; a forced step tries other targets until it
// changes something, and fails when it cannot.
func (s *sequence) run(st seqStep, a *storage.AccountTx) error {
	s.note = ""
	err := st.run(s, a)
	for attempt := 1; s.forced && (errors.Is(err, errSkipped) || errors.Is(err, storage.ErrNotFound)); attempt++ {
		if attempt == 100 {
			return fmt.Errorf("a %s change could not be made: %w", st.name, errUnforceable)
		}
		err = st.run(s, a) // another target
	}
	return err
}

var errUnforceable = errors.New("no target allows it")

// step commits one random transaction of one to three changes, then checks
// the version advanced from the last against a full build in the
// snapshot the commit produced.
func (s *sequence) step(i int) {
	t := s.t
	t.Helper()
	ctx := context.Background()
	total := 0
	for _, st := range seqSteps {
		total += st.weight
	}
	var names []string
	bulk := false
	s.appended = false
	err := s.store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(s.account)
		for c := range 1 + s.r.IntN(3) {
			n := s.r.IntN(total)
			var st seqStep
			for _, c := range seqSteps {
				if n < c.weight {
					st = c
					break
				}
				n -= c.weight
			}
			// The first steps go through every kind of change (and every
			// Continue witness) once, each tried until it changes something:
			// every run covers them all.
			s.forced = c == 0 && i < len(forcedPlan)
			if s.forced {
				st = stepNamed(forcedPlan[i])
			}
			if st.name == restoreTransition {
				if err := tx.RestoreTransition("a read-model test"); err != nil {
					return err
				}
				bulk = true
				names = append(names, st.name)
				s.bins[st.name]++
				continue
			}
			if st.bulk {
				if err := tx.SuspendViewCapture(s.account); err != nil {
					return err
				}
				bulk = true
			}
			switch err := s.run(st, a); {
			case errors.Is(err, errSkipped), errors.Is(err, storage.ErrNotFound):
				names = append(names, st.name+" (nothing to change)")
				continue
			case err != nil:
				return fmt.Errorf("%s: %w", st.name, err)
			}
			names = append(names, st.name)
			s.bins[st.name]++
			if s.note != "" {
				s.bins[s.note]++
			}
		}
		if !s.appended && s.r.IntN(4) == 0 { // the views' changes with a commit to the log
			s.appended = true
			if _, _, err := a.AppendCommit(storage.CommitBody{}); err != nil {
				return err
			}
			names = append(names, "with a commit")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("step %d %v: %v", i, names, err)
	}
	s.trace = append(s.trace, fmt.Sprintf("%d %s", i, strings.Join(names, ", ")))
	if err := s.store.Read(ctx, func(r *storage.ReadTx) error {
		acct := r.Account(s.account)
		head, err := acct.Head()
		if err != nil {
			return err
		}
		clock, err := acct.ViewClock()
		if err != nil {
			return err
		}
		next, path, err := views.Advance(s.prev, acct, s.account)
		if err != nil {
			return fmt.Errorf("advance: %w", err)
		}
		full, err := views.BuildVersion(acct, s.account)
		if err != nil {
			return err
		}
		for what, v := range map[string]*views.Version{path.String(): next, "full build": full} {
			if msg := viewstest.IdentityMismatch(v, s.account, head, clock); msg != "" {
				return fmt.Errorf("the %s %s", what, msg)
			}
		}
		if err := views.EqualVersions(next, full); err != nil {
			return fmt.Errorf("the %s differs from a full build: %w", path, err)
		}
		checkStructures(t, next)
		switch {
		case bulk && path != views.PathGeneration:
			return fmt.Errorf("a new generation was %s", path)
		case !bulk && path == views.PathGeneration, path == views.PathFirst:
			return fmt.Errorf("a routine commit was a %s", path)
		}
		s.paths[path]++
		checkpoint := s.refEvery > 0 && i%s.refEvery == 0
		whole := s.matrixEvery > 0 && i%s.matrixEvery == 0 || s.matrixEvery == 0 && i == s.steps-1
		if checkpoint || whole || s.rotating > 0 {
			ref, err := reference.Load(r, s.account)
			if err != nil {
				return err
			}
			if checkpoint {
				compareWithReference(t, next, ref)
			}
			lib := &views.Library{Head: head, V: next}
			qr := rand.New(rand.NewPCG(uint64(i), 7))
			switch {
			case whole:
				lists, rows := compareQueries(t, lib, ref, s.now, qr, true, 0)
				s.queried += lists
				t.Logf("step %d: the whole query matrix, %d lists, %d rows", i, lists, rows)
			case s.windowed:
				s.queried += compareWindows(t, lib, ref, s.now, qr, s.rotating)
			default:
				lists, _ := compareQueries(t, lib, ref, s.now, qr, false, s.rotating)
				s.queried += lists
			}
		}
		if i%25 == 0 {
			s.retained = append(s.retained, retainedVersion{next, views.Fingerprint(next)})
		}
		s.prev = next
		return nil
	}); err != nil {
		t.Fatalf("step %d %v: %v", i, names, err)
	}
}

// finish checks the retained versions and the coverage bins, and on
// failure saves the trace.
func (s *sequence) finish(seed uint64, minPerStep int) {
	t := s.t
	t.Helper()
	for i, r := range s.retained {
		if views.Fingerprint(r.v) != r.fingerprint {
			t.Errorf("retained version %d changed after later applies", i)
		}
	}
	for k := range continueWitnesses {
		if name := fmt.Sprintf("continue witness %d", k); s.bins[name] < 1 {
			t.Errorf("no %q change", name)
		}
	}
	for _, st := range seqSteps {
		if s.bins[st.name] < max(1, minPerStep) {
			t.Errorf("only %d %q changes", s.bins[st.name], st.name)
		}
	}
	t.Logf("seed %d: %d steps; paths %v; changes %v", seed, len(s.trace), s.paths, s.bins)
}

// failureTrace is a failed sequence's replayable trace: its test, seed and
// steps (NTP_READMODEL_REPLAY=<file> runs exactly it again), with the
// changes each step committed.
type failureTrace struct {
	Test  string   `json:"test"`
	Seed  uint64   `json:"seed"`
	Steps int      `json:"steps"`
	Trace []string `json:"trace"`
}

func (s *sequence) saveTraceOnFailure(seed uint64, steps int) {
	if !s.t.Failed() {
		return
	}
	dir := filepath.Join("..", "..", ".work", "readmodel-failures")
	if os.MkdirAll(dir, 0o700) == nil {
		name := filepath.Join(dir, fmt.Sprintf("%s-%d.json", strings.ReplaceAll(s.t.Name(), "/", "_"), seed))
		b, _ := json.MarshalIndent(failureTrace{s.t.Name(), seed, steps, s.trace}, "", "  ")
		if os.WriteFile(name, b, 0o600) == nil {
			s.t.Logf("the trace is in %s; NTP_READMODEL_REPLAY=%s replays it", name, name)
		}
	}
}

// seeds are the seeds to run: NTP_READMODEL_REPLAY's (a saved trace's),
// NTP_READMODEL_SEED's, or def.
func seeds(def ...uint64) []uint64 {
	if tr, ok := replay(); ok {
		return []uint64{tr.Seed}
	}
	if v, err := strconv.ParseUint(os.Getenv("NTP_READMODEL_SEED"), 10, 64); err == nil {
		return []uint64{v}
	}
	return def
}

func replay() (failureTrace, bool) {
	var tr failureTrace
	name := os.Getenv("NTP_READMODEL_REPLAY")
	if name == "" {
		return tr, false
	}
	b, err := os.ReadFile(name)
	if err != nil || json.Unmarshal(b, &tr) != nil {
		panic(fmt.Sprintf("NTP_READMODEL_REPLAY: %v", err))
	}
	return tr, true
}

// raceFactor divides the default step counts (race_test.go).
var raceFactor = 1

// skipUnderRace skips a single-goroutine test at scale under the race
// detector, which has nothing to find in it and makes it take hours; it
// runs without.
func skipUnderRace(t *testing.T) {
	t.Helper()
	if raceFactor > 1 {
		t.Skip("a single-goroutine test at scale: it runs without the race detector")
	}
}

// stepCount is the steps to run: the replayed trace's, NTP_READMODEL_STEPS,
// or def × NTP_READMODEL_LONG's factor.
func stepCount(def int) int {
	if tr, ok := replay(); ok {
		return tr.Steps
	}
	if v, err := strconv.Atoi(os.Getenv("NTP_READMODEL_STEPS")); err == nil {
		return v
	}
	return max(1, def*long()/raceFactor)
}

// long is NTP_READMODEL_LONG's factor for the step counts.
func long() int {
	if os.Getenv("NTP_READMODEL_LONG") != "" {
		return 10
	}
	return 1
}

// openLibrary opens a store whose account holds the generated library
// (and, when imported, the Android fixture imported for real first).
func openLibrary(t *testing.T, spec viewstest.Spec, imported bool) (*storage.Store, string) {
	t.Helper()
	ctx := context.Background()
	var store *storage.Store
	var account string
	if imported {
		store, account = importAccount(t, mutatedFixture(t, "emulator-parity-library.db", spec.Seed))
	} else {
		var err error
		if store, err = storage.Open(ctx, filepath.Join(t.TempDir(), "data")); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		if err := store.Write(ctx, func(tx *storage.WriteTx) error {
			a, err := tx.CreateAccount()
			account = a.ID
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	viewstest.Skip(store)
	if _, err := viewstest.Generate(ctx, store, account, spec); err != nil {
		t.Fatal(err)
	}
	return store, account
}

// On a medium library (the Android fixture imported for real, plus
// 120 generated podcasts of 50 episodes), random commits of every kind of
// change the read model sees; after each, the advanced version equals a
// full build in the same snapshot, every 50th is compared with the oracle,
// and retained versions never change. Three seeds of 1,000 steps, in
// parallel.
func TestIncrementalEqualsFullBuild(t *testing.T) {
	steps := stepCount(1000)
	for _, seed := range seeds(1, 2, 3) {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Parallel()
			pods := make([]int, 120)
			for i := range pods {
				pods[i] = 50
			}
			store, account := openLibrary(t, viewstest.Spec{Seed: seed, Podcasts: pods, Queue: 300, Now: viewstest.FixedNow, Months: 36, Prefix: "m"}, true)
			runSequence(t, store, account, seed, steps, 50, func(s *sequence) { s.matrixEvery, s.rotating = 250, 8 })
		})
	}
}

func runSequence(t *testing.T, store *storage.Store, account string, seed uint64, steps, refEvery int, configure func(*sequence)) {
	t.Helper()
	s := newSequence(t, store, account, seed, viewstest.FixedNow)
	s.refEvery, s.steps = refEvery, steps
	configure(s)
	defer s.saveTraceOnFailure(seed, steps)
	_, stored, queued := s.prev.Counts()
	t.Logf("seed %d (NTP_READMODEL_SEED replays it): %d podcasts and %d episodes known, %d stored, %d queued; %d steps", seed, len(s.podcasts),
		len(s.episodes), stored, queued, steps)
	start := time.Now()
	for i := range steps {
		s.step(i)
	}
	s.finish(seed, steps/200)
	t.Logf("seed %d: %.2f s a step; %d queries compared with the oracle", seed, time.Since(start).Seconds()/float64(max(1, steps)), s.queried)
}

// The same on a power user's library: 120 podcasts and over 50,400
// stored episodes, one podcast of 4,063, a 5,000-episode queue, ten years of
// history and merged statistics units in distinct months; the version
// equals a full build after every step, and the oracle every 20th. One seed
// of 60 steps; NTP_READMODEL_LONG: three of 600.
func TestIncrementalEqualsFullBuildAtScale(t *testing.T) {
	skipUnderRace(t)
	steps := stepCount(60)
	runSeeds := seeds(11)
	if long() > 1 {
		runSeeds = seeds(11, 12, 13)
	}
	for _, seed := range runSeeds {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			// 52,000 episodes, of which about 50,500 are stored (the rest
			// unregistered or merged, as in a real library).
			const total = 52000
			pods := []int{4063}
			for i := range 119 {
				pods = append(pods, (total-4063)/119+map[bool]int{true: 1, false: 0}[i < (total-4063)%119])
			}
			store, account := openLibrary(t, viewstest.Spec{Seed: seed, Podcasts: pods, Queue: 5200, Now: viewstest.FixedNow, Months: 120, Prefix: "L"}, false)
			// 16 queries a step at two windows each; the whole matrix at
			// the last step.
			runSequence(t, store, account, seed, steps, 20, func(s *sequence) { s.rotating, s.windowed = 16, true })
		})
	}
}

// The sequences count only changes that happened: on a library with nothing
// queued, a queue move, a queue removal and a Continue witness change
// nothing and say so; writing a record's own value again is no change; and
// a forced step finds a target that allows it or fails.
func TestSequenceStepsCountOnlyChanges(t *testing.T) {
	s := witnessLibrary(t)
	q := newSequence(t, s.store, s.account, 1, viewstest.FixedNow)
	if err := s.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a := tx.Account(s.account)
		for _, name := range []string{"queue move", "queue remove", "continue witness"} {
			for range 20 {
				if err := stepNamed(name).run(q, a); !errors.Is(err, errSkipped) {
					t.Errorf("%s on a library with nothing queued: %v", name, err)
				}
			}
		}
		e := s.lib.Episodes[0][0]
		if err := q.put(a, e, "play_state", `{"generation_id":"g","play_state":"unplayed"}`); !errors.Is(err, errSkipped) {
			t.Errorf("writing a record's own value again: %v", err)
		}
		if err := q.put(a, e, "play_state", `{"generation_id":"g","play_state":"played"}`); err != nil {
			t.Errorf("a change: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// An episode the last version has queued, unqueued earlier in the
	// transaction: a queue move or a Continue witness finds nothing.
	one := openShaped(t, "c", []viewstest.Shape{{Episodes: 2, Queued: 1}})
	oq := newSequence(t, one.store, one.account, 1, viewstest.FixedNow)
	if err := one.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a := tx.Account(one.account)
		if err := oq.put(a, one.lib.Episodes[0][0], "queue_membership", `{"in_queue":false}`); err != nil {
			return err
		}
		for _, name := range []string{"queue move", "continue witness", "queue remove"} {
			for range 20 {
				if err := stepNamed(name).run(oq, a); !errors.Is(err, errSkipped) {
					t.Errorf("%s of an episode unqueued in the transaction: %v", name, err)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Forced, a step finds a target that allows it, or fails the step.
	q.forced = true
	err := s.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		if err := q.run(stepNamed("queue add"), tx.Account(s.account)); err != nil {
			t.Errorf("a forced queue add: %v", err)
		}
		return q.run(stepNamed("queue move"), tx.Account(s.account))
	})
	if !errors.Is(err, errUnforceable) {
		t.Fatalf("a forced queue move with nothing queued as of the last version: %v", err)
	}
}
