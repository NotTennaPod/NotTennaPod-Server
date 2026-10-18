package views_test

// Boundaries of the apply (change pages, entity batches, a page straddled
// by one podcast's moves, 26,000 changes in one commit, an idle account
// catching up) and of statistics by each part's own date.

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

func (s *shaped) allEpisodes() []string {
	var out []string
	for _, eps := range s.lib.Episodes {
		out = append(out, eps...)
	}
	slices.Sort(out)
	return out
}

var setSeq int

// setStates writes the episodes' play states, each a change (a new
// provenance) whatever the state was.
func setStates(ids []string, state string) func(a *storage.AccountTx) error {
	setSeq++
	prov := fmt.Sprintf(`["set%d"]`, setSeq)
	return func(a *storage.AccountTx) error {
		for _, e := range ids {
			if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: "play_state",
				Value: fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, state), Provenance: prov}); err != nil {
				return err
			}
		}
		return nil
	}
}

// Changed-entity counts around a change page (999, 1,000, 1,001,
// 1,999, 2,000, 2,001) and an entity batch (499, 500, 501), each one apply
// that misses nothing; one podcast's episodes moved across a page boundary;
// 26,000 episodes marked played in one commit; and an idle account catching
// up on 6,300 distinct changes; never a full build.
func TestBoundaryChangePagesAndBatches(t *testing.T) {
	skipUnderRace(t)
	page, batch := storage.ViewChangesPage, storage.ViewBatch
	if page != 1000 || batch != 500 {
		t.Fatalf("pages of %d and batches of %d: the cases below assume 1,000 and 500", page, batch)
	}
	s := openShaped(t, "b", repeatShape(60, viewstest.Shape{Episodes: 500, Queued: 10, Months: 4}))
	all := s.allEpisodes()
	state := "played"
	offset := 0
	for _, k := range []int{batch - 1, batch, batch + 1, page - 1, page, page + 1, 2*page - 1, 2 * page, 2*page + 1} {
		ids := all[offset : offset+k]
		offset = (offset + k) % (len(all) - 2*page - 1)
		w, path := s.commit(setStates(ids, state))
		if path != views.PathApply || w.Changes != int64(k) || w.EntitiesBuilt != int64(k) || w.FullBuilds != 0 {
			t.Errorf("%d changes: the %s, %+v", k, path, w)
		}
		for _, id := range []string{ids[0], ids[k-1], ids[min(k-1, batch)], ids[min(k-1, page)]} {
			if e, ok := s.v.Episode(id); !ok || e.Read != map[string]int{"played": 1, "unplayed": 0}[state] {
				t.Errorf("%d changes: %s is not %s", k, id, state)
			}
		}
		state = map[string]string{"played": "unplayed", "unplayed": "played"}[state]
	}

	// One podcast's episodes moved to another, straddling the first page:
	// in tick and ID order the changes put its moves at 990 to 1,009.
	from, to := s.lib.Podcasts[2], s.lib.Podcasts[5]
	changed := all[10:1020]
	moved := changed[990:]
	for _, e := range moved {
		if ep, _ := s.v.Episode(e); ep.PodcastID != from {
			t.Fatalf("episode %s is of %s, not %s", e, ep.PodcastID, from)
		}
	}
	beforeFrom, beforeTo := s.v.PodcastAgg(from), s.v.PodcastAgg(to)
	w, path := s.commit(func(a *storage.AccountTx) error {
		if err := setStates(changed[:990], "played")(a); err != nil {
			return err
		}
		for _, e := range moved {
			if err := edit(a, "episode", e, "identity", map[string]any{"podcast_id": to}, `["b"]`); err != nil {
				return err
			}
		}
		return nil
	})
	if path != views.PathApply || w.Changes != 1010 || w.FullBuilds != 0 {
		t.Errorf("moves across a page: the %s, %+v", path, w)
	}
	afterFrom, afterTo := s.v.PodcastAgg(from), s.v.PodcastAgg(to)
	count := func(a views.PodAgg) (n int32) {
		for _, c := range a.Hist {
			n += c
		}
		return n
	}
	if count(afterFrom) != count(beforeFrom)-int32(len(moved)) || count(afterTo) != count(beforeTo)+int32(len(moved)) {
		t.Errorf("moves across a page: %d and %d episodes, then %d and %d (%d moved)", count(beforeFrom), count(beforeTo),
			count(afterFrom), count(afterTo), len(moved))
	}
	for _, e := range moved {
		if ep, _ := s.v.Episode(e); ep.PodcastID != to {
			t.Errorf("episode %s is of %s after its move", e, ep.PodcastID)
		}
	}

	// 26,000 episodes marked played in one commit.
	w, path = s.commit(setStates(all[:26000], "played"))
	t.Logf("26,000 marked played: %+v; the apply %.2f s, a full build %.2f s", w, s.applyTime.Seconds(), s.fullTime.Seconds())
	if path != views.PathApply || w.Changes != 26000 || w.FullBuilds != 0 {
		t.Errorf("26,000 marked played: the %s, %+v", path, w)
	}

	// An idle account: 63 commits of 100 distinct episodes each, then one
	// catch-up from the version before them.
	base := s.v
	for i := range 63 {
		if err := s.store.Write(context.Background(), func(tx *storage.WriteTx) error {
			return setStates(all[26000-i*100-100:26000-i*100], "unplayed")(tx.Account(s.account))
		}); err != nil {
			t.Fatal(err)
		}
	}
	s.v = base
	w, path = s.commit(func(*storage.AccountTx) error { return nil })
	t.Logf("catching up on 63 commits: %+v; the apply %.2f s, a full build %.2f s", w, s.applyTime.Seconds(), s.fullTime.Seconds())
	if path != views.PathApply || w.Changes != 6300 || w.EntitiesBuilt != 6300 || w.FullBuilds != 0 {
		t.Errorf("catching up on 6,300 changes: the %s, %+v", path, w)
	}
}

func utc(y int, m time.Month, d, h, min, sec, ms int) int64 {
	return time.Date(y, m, d, h, min, sec, ms*1_000_000, time.UTC).UnixMilli()
}

// A podcast's played and marked time in [from, to), with parts last
// played exactly at from and at to, a millisecond either side, and an empty
// window. The values are written out by hand.
func TestPartDatesWindows(t *testing.T) {
	s := openShaped(t, "w", []viewstest.Shape{{Episodes: 2}, {Episodes: 1}})
	p, e := s.lib.Podcasts[0], s.lib.Episodes[0][0]
	T := utc(2026, 5, 10, 8, 0, 0, 0)
	s.commit(func(a *storage.AccountTx) error {
		if err := put(a, "episode", e, "last_played", fmt.Sprintf(`{"time":%d}`, T)); err != nil {
			return err
		}
		if err := put(a, "episode", e, "played_time", `{"played_ms":60000}`); err != nil {
			return err
		}
		return put(a, "episode", e, "statistics_units", fmt.Sprintf(`{"units":[`+
			`{"duration_ms":600000,"history":null,"last_played":%d,"played":true,"played_ms":0,"position":0},`+
			`{"duration_ms":600000,"history":%d,"last_played":%d,"played":false,"played_ms":30000,"position":0}]}`, T+1, T-1, T-1))
	})
	const forever = int64(1) << 62
	for _, c := range []struct {
		from, to       int64
		played, marked int64
	}{
		{T, T + 1, 60_000, 60_000},
		{T, T, 0, 0},
		{T + 1, T + 1, 0, 0},
		{T - 1, T, 30_000, 30_000},
		{T + 1, T + 2, 0, 600_000},
		{T - 1, T + 2, 90_000, 690_000},
		{T - 2, T - 1, 0, 0},
		{T + 2, forever, 0, 0},
		{0, T - 1, 0, 0},
		{0, forever, 90_000, 690_000},
		{T + 1, T, 0, 0},
	} {
		played, marked := s.v.PlayedBetween(p, c.from, c.to)
		if played != c.played || marked != c.marked {
			t.Errorf("[T%+d, T%+d): played %d, marked %d; want %d, %d", c.from-T, c.to-T, played, marked, c.played, c.marked)
		}
	}
	if got := s.v.OldestPlayed(p); got != T-1 {
		t.Errorf("oldest played T%+d, want T-1", got-T)
	}
}

// An episode's merged statistics units in distinct months (two either
// side of a year's end) count in their own months and for the podcast at
// their own dates; moved to another podcast they go with it; with that
// podcast hidden they leave the months, and come back when it is shown.
func TestPartDatesMovedAndHidden(t *testing.T) {
	s := openShaped(t, "m", []viewstest.Shape{{Episodes: 2}, {Episodes: 2}, {Episodes: 1}})
	x, y, e := s.lib.Podcasts[0], s.lib.Podcasts[1], s.lib.Episodes[0][0]
	own := utc(2026, 5, 10, 8, 0, 0, 0)
	u1, u2, u3 := utc(2026, 3, 10, 0, 0, 0, 0), utc(2025, 12, 31, 23, 59, 59, 999), utc(2026, 1, 1, 0, 0, 0, 0)
	s.commit(func(a *storage.AccountTx) error {
		if err := put(a, "episode", e, "last_played", fmt.Sprintf(`{"time":%d}`, own)); err != nil {
			return err
		}
		if err := put(a, "episode", e, "played_time", `{"played_ms":1000}`); err != nil {
			return err
		}
		return put(a, "episode", e, "statistics_units", fmt.Sprintf(`{"units":[`+
			`{"duration_ms":600000,"history":%[1]d,"last_played":%[1]d,"played":false,"played_ms":20000,"position":0},`+
			`{"duration_ms":600000,"history":%[2]d,"last_played":%[2]d,"played":false,"played_ms":300000,"position":0},`+
			`{"duration_ms":600000,"history":%[3]d,"last_played":%[3]d,"played":true,"played_ms":4000000,"position":0}]}`, u1, u2, u3))
	})
	months := func() map[string]int64 {
		out := map[string]int64{}
		for k, ms := range s.v.Months() {
			yy, mm := views.MonthOf(k)
			out[fmt.Sprintf("%04d-%02d", yy, mm)] = ms
		}
		return out
	}
	withE := map[string]int64{"2026-05": 1000, "2026-03": 20000, "2025-12": 300000, "2026-01": 4000000}
	check := func(when string, holder string, wantMonths map[string]int64) {
		t.Helper()
		for _, p := range []string{x, y} {
			for _, c := range []struct {
				from, to int64
				want     int64
			}{{own, own + 1, 1000}, {u1, u1 + 1, 20000}, {u2, u2 + 1, 300000}, {u3, u3 + 1, 4000000}, {u2, u3, 300000}, {u2 + 1, u3 + 1, 4000000},
				{u2, own + 1, 4321000}} {
				want := int64(0)
				if p == holder {
					want = c.want
				}
				if played, _ := s.v.PlayedBetween(p, c.from, c.to); played != want {
					t.Errorf("%s: podcast %s played %d in [%d, %d), want %d", when, p, played, c.from, c.to, want)
				}
			}
		}
		if got := months(); !mapsEqual(got, wantMonths) {
			t.Errorf("%s: months %v, want %v", when, got, wantMonths)
		}
	}
	check("in its podcast", x, withE)
	s.commit(func(a *storage.AccountTx) error {
		return edit(a, "episode", e, "identity", map[string]any{"podcast_id": y}, `["b"]`)
	})
	check("moved", y, withE)
	s.commit(func(a *storage.AccountTx) error {
		return put(a, "podcast", y, "subscription", `{"generation_id":"g1:0000000000000001","state":"removed"}`)
	})
	check("its podcast hidden", y, map[string]int64{})
	s.commit(func(a *storage.AccountTx) error {
		return put(a, "podcast", y, "subscription", `{"generation_id":"g1:0000000000000001","state":"subscribed"}`)
	})
	check("its podcast shown again", y, withE)
	s.commit(func(a *storage.AccountTx) error {
		return edit(a, "episode", e, "identity", map[string]any{"podcast_id": x}, `["b"]`)
	})
	check("moved back", x, withE)
}

func mapsEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}
