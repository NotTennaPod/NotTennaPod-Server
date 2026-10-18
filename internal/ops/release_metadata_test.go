package ops

import (
	"context"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Each release keeps its own metadata. The cases kept as rule vectors are in
// testdata/release_metadata and contract/vectors/rules_release_metadata.json.

// When a later fetch first disputes a GUID and splits its earlier
// evidence into releases, each earlier fetch's item metadata moves to its
// own release at once, without another fetch.
func TestDelayedDisputeMovesEachReleasesMetadata(t *testing.T) {
	c := newCatalogEnv(t)
	d1, d2 := c.clock-40*day, c.clock-day
	u, v := "https://cdn.example.com/latest.mp3", "https://cdn.example.com/latest.mp3?sig=old"
	for _, it := range []idItem{{"g", "One", u, d1}, {"g", "Two", u, d2}, {"g", "One", v, d1}} {
		c.clock += minute
		c.serverFetch(t, idFeed(t, c.clock, it), "")
	}
	later, _ := contract.EpisodeID(c.namespace, c.p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u))
	if got := c.episodeRecord(later, "metadata"); !strings.Contains(got, `"title":"Two"`) {
		t.Errorf("the later release has no metadata of its own: %s", got)
	}
	if got := c.episodeRecord(c.idOf("g"), "metadata"); !strings.Contains(got, `"title":"One"`) {
		t.Errorf("the original release lost its metadata: %s", got)
	}
}

// A podcast with two releases merged into a podcast without evidence of
// the GUID keeps each release's metadata.
func TestPodcastMergeKeepsEachReleasesMetadata(t *testing.T) {
	c := newCatalogEnv(t)
	a := c.device("A")
	sub := c.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://mirror.example.com/feed"})
	c.pushOK(t, a, sub, "applied")
	p := contract.PodcastID(c.namespace, sub["op_id"].(string))
	g := contract.GenerationID(c.namespace, p, sub["op_id"].(string))
	d1, d2 := c.clock-40*day, c.clock-day
	u, v := "https://cdn.example.com/a.mp3", "https://cdn.example.com/b.mp3"
	c.fetchFor(t, p, g, idFeed(t, c.clock, idItem{"g", "One", u, d1}, idItem{"g", "Two", v, d2}), "")
	c.clock += minute
	c.fetchFor(t, p, g, idFeed(t, c.clock), "https://example.com/feed")
	later, _ := contract.EpisodeID(c.namespace, c.p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", v))
	if got := c.episodeRecord(later, "metadata"); !strings.Contains(got, `"title":"Two"`) {
		t.Errorf("the merge dropped the later release's metadata: %s", got)
	}
}

// Deleting an account deletes its enclosure additions, signed URLs
// included (with every other per-account identity row: migration 0021).
func TestDeletedAccountKeepsNoEnclosureAdditions(t *testing.T) {
	c := newCatalogEnv(t)
	d1, d2 := c.clock-40*day, c.clock-day
	for _, it := range []idItem{{"g", "One", "https://cdn.example.com/one", d1}, {"g", "Two", "https://cdn.example.com/two", d2},
		{"g", "Two", "https://cdn.example.com/two?token=example", d2}} {
		c.clock += minute
		c.serverFetch(t, idFeed(t, c.clock, it), "")
	}
	checkRebuild(t, c.svc, c.account)
	c.noRebuild = "the test deletes the account and its archive"
	err := c.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		adds, err := tx.Account(c.account).ReleaseAdditions(c.p, "g")
		if err != nil {
			return err
		}
		if len(adds) != 1 {
			t.Fatalf("precondition: one enclosure addition, got %+v", adds)
		}
		if err := tx.DeleteAccount(c.account); err != nil {
			return err
		}
		if adds, err = tx.Account(c.account).ReleaseAdditions(c.p, "g"); err != nil {
			return err
		}
		if len(adds) != 0 {
			t.Errorf("the deleted account keeps its enclosure additions: %+v", adds)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// fetchImported applies a server fetch of an imported podcast.
func (e *env) fetchImported(t *testing.T, p ImportedPodcast, items ...idItem) {
	t.Helper()
	e.clock += minute
	err := e.svc.ServerCommit(context.Background(), e.account, func(ctx *protocol.ApplyContext) error {
		rc, err := CaptureRefresh(ctx, p.PodcastID)
		if err != nil {
			return err
		}
		return ApplyServerFetch(ctx, ServerFetch{PodcastID: p.PodcastID, GenerationID: p.GenerationID, RefreshBy: rc.RefreshBy, RefreshID: newID(),
			FetchTime: e.clock, Namespace: e.namespace, Bases: rc.Bases, Feed: idFeed(t, e.clock, items...)})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// groupedImport imports two Android rows that share a GUID and an enclosure,
// 39 days apart, so the import groups them into one episode. The later row
// is played, a favorite and queued; the earlier row is played too when both
// is set, else untouched.
func groupedImport(t *testing.T, e *env, both bool) (ImportedPodcast, string) {
	t.Helper()
	d1, d2 := e.clock-40*day, e.clock-day
	h1, h2 := e.clock-50*day, e.clock-1000
	u := "https://cdn.example.com/latest.mp3"
	first := ImportRow{}
	if both {
		first = ImportRow{PlayState: "played", Position: 45000, PlayedTime: 100000, History: &h1, LastPlayed: h1}
	}
	x := importPodcast(e.clock,
		importItem{"g", u, d1, first},
		importItem{"g", u, d2, ImportRow{PlayState: "played", Favorite: true, Position: 123000, PlayedTime: 600000, History: &h2, LastPlayed: h2,
			QueueIndex: 0, QueueKey: "80", PositionID: ImportPositionID(testImportID, 0)}})
	p := e.applyImport(t, x)
	if p.EpisodeOf[1] != p.EpisodeOf[2] {
		t.Fatalf("precondition: the import groups the two rows: %+v", p.EpisodeOf)
	}
	later, _ := contract.EpisodeID(e.namespace, p.PodcastID, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u))
	return p, later
}

// disputeImported fetches a signed variant of the earlier release: the first
// dispute splits the GUID's evidence into its two releases.
func disputeImported(t *testing.T, e *env, p ImportedPodcast) {
	t.Helper()
	e.fetchImported(t, p, idItem{"g", "One", "https://cdn.example.com/latest.mp3?sig=old", e.clock - 40*day})
}

// Imported rows the import grouped follow the release each row's own
// occurrence resolves to when a later dispute splits them: the later row's
// favorite, position, history, statistics and queue entry go to the later
// release, and the earlier release keeps the earlier row's state.
func TestGroupedImportFollowsEachRowsRelease(t *testing.T) {
	e := newEnv(t)
	p, later := groupedImport(t, e, false)
	disputeImported(t, e, p)
	rec := e.records()
	older, newer := "episode/"+p.EpisodeOf[1]+"/", "episode/"+later+"/"
	for _, g := range []string{"favorite", "position", "history", "played_time", "queue_membership", "queue_position"} {
		if rec[newer+g] == "" {
			t.Errorf("%s: the later row's state stayed on the earlier release (%s)", g, rec[older+g])
		}
	}
	if !strings.Contains(rec[newer+"favorite"], `"on":true`) || !strings.Contains(rec[newer+"position"], `123000`) ||
		!strings.Contains(rec[newer+"played_time"], `"played_ms":600000`) || !strings.Contains(rec[newer+"queue_membership"], `"in_queue":true`) {
		t.Errorf("the later release: favorite %s, position %s, played %s, queue %s", rec[newer+"favorite"], rec[newer+"position"],
			rec[newer+"played_time"], rec[newer+"queue_membership"])
	}
	if strings.Contains(rec[older+"favorite"], `"on":true`) || strings.Contains(rec[older+"queue_membership"], `"in_queue":true`) ||
		!strings.Contains(rec[older+"play_state"], `"unplayed"`) {
		t.Errorf("the earlier release keeps the later row's state: favorite %s, queue %s, play state %s", rec[older+"favorite"],
			rec[older+"queue_membership"], rec[older+"play_state"])
	}
}

// After the split, a history clear and a statistics reset cover the
// imported state in both releases.
func TestCutsCoverASplitImport(t *testing.T) {
	e := newEnv(t)
	p, later := groupedImport(t, e, true)
	disputeImported(t, e, p)
	rec := e.records()
	for _, ep := range []string{p.EpisodeOf[1], later} {
		if strings.Contains(rec["episode/"+ep+"/history"], `"time":null`) || strings.Contains(rec["episode/"+ep+"/played_time"], `"played_ms":0`) {
			t.Fatalf("precondition: each release has its row's history and statistics: %s %s", rec["episode/"+ep+"/history"],
				rec["episode/"+ep+"/played_time"])
		}
	}
	a := e.device("A")
	e.pushOK(t, a, e.lfact(a, "history.clear", nil, map[string]any{}, e.allNodes(t)...), "applied")
	e.pushOK(t, a, e.lfact(a, "statistics.reset", nil, map[string]any{}, e.allNodes(t)...), "applied")
	rec = e.records()
	for _, ep := range []string{p.EpisodeOf[1], later} {
		if !strings.Contains(rec["episode/"+ep+"/history"], `"time":null`) {
			t.Errorf("%s: history not cleared: %s", ep, rec["episode/"+ep+"/history"])
		}
		if !strings.Contains(rec["episode/"+ep+"/played_time"], `"played_ms":0`) {
			t.Errorf("%s: statistics not reset: %s", ep, rec["episode/"+ep+"/played_time"])
		}
	}
}

// Cuts made before the split follow the imported state to both releases,
// for a grouped import and for one row alone.
func TestCutsBeforeTheSplitFollowTheImport(t *testing.T) {
	for _, grouped := range []bool{true, false} {
		t.Run(map[bool]string{true: "grouped", false: "one row"}[grouped], func(t *testing.T) {
			e := newEnv(t)
			var p ImportedPodcast
			var later string
			if grouped {
				p, later = groupedImport(t, e, true)
			} else {
				h := e.clock - 1000
				u := "https://cdn.example.com/latest.mp3"
				p = e.applyImport(t, importPodcast(e.clock, importItem{"g", u, e.clock - day, ImportRow{PlayState: "played", PlayedTime: 600000,
					History: &h, LastPlayed: h}}))
				later, _ = contract.EpisodeID(e.namespace, p.PodcastID, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u))
			}
			a := e.device("A")
			e.pushOK(t, a, e.lfact(a, "history.clear", nil, map[string]any{}, e.allNodes(t)...), "applied")
			e.pushOK(t, a, e.lfact(a, "statistics.reset", nil, map[string]any{}, e.allNodes(t)...), "applied")
			disputeImported(t, e, p)
			rec := e.records()
			eps := []string{later}
			if grouped {
				eps = append(eps, p.EpisodeOf[1])
			}
			for _, ep := range eps {
				if h := rec["episode/"+ep+"/history"]; h != "" && !strings.Contains(h, `"time":null`) {
					t.Errorf("%s: the cleared history came back: %s", ep, h)
				}
				if pt := rec["episode/"+ep+"/played_time"]; pt != "" && !strings.Contains(pt, `"played_ms":0`) {
					t.Errorf("%s: the reset statistics came back: %s", ep, pt)
				}
			}
			if rec["episode/"+later+"/played_time"] == "" {
				t.Errorf("the imported statistics did not move to the later release")
			}
		})
	}
}

// Rows that stay together after the split are aggregated again: the
// earlier release takes the state of its most recently played row and keeps
// its other row as a statistics unit; the later release has its own row
// only.
func TestSplitImportAggregatesEachRelease(t *testing.T) {
	e := newEnv(t)
	d1, d2 := e.clock-40*day, e.clock-day
	h1, h2 := e.clock-30*day, e.clock-1000
	u := "https://cdn.example.com/latest.mp3"
	p := e.applyImport(t, importPodcast(e.clock,
		importItem{"g", u, d1, ImportRow{PlayState: "played", PlayedTime: 100000, Position: 1000, LastPlayed: h1 - 1000}},
		importItem{"g", u, d1 + day, ImportRow{PlayState: "played", PlayedTime: 200000, Position: 2000, History: &h1, LastPlayed: h1}},
		importItem{"g", u, d2, ImportRow{PlayState: "played", PlayedTime: 600000, Position: 123000, History: &h2, LastPlayed: h2}}))
	if p.EpisodeOf[1] != p.EpisodeOf[3] || p.Episodes != 1 {
		t.Fatalf("precondition: the import groups the three rows: %+v", p)
	}
	rec := e.records()
	if units := rec["episode/"+p.EpisodeOf[1]+"/statistics_units"]; strings.Count(units, `"id"`) != 2 {
		t.Fatalf("precondition: two rows are statistics units before the split: %s", units)
	}
	disputeImported(t, e, p)
	later, _ := contract.EpisodeID(e.namespace, p.PodcastID, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u))
	rec = e.records()
	older := "episode/" + p.EpisodeOf[1] + "/"
	if !strings.Contains(rec[older+"position"], `"position":2000`) || !strings.Contains(rec[older+"played_time"], `"played_ms":200000`) {
		t.Errorf("the earlier release does not take its most recently played row: position %s, played %s", rec[older+"position"], rec[older+"played_time"])
	}
	if units := rec[older+"statistics_units"]; strings.Count(units, `"id"`) != 1 || !strings.Contains(units, testImportID+"/1") {
		t.Errorf("the earlier release's units: %s", units)
	}
	newer := "episode/" + later + "/"
	if !strings.Contains(rec[newer+"position"], `"position":123000`) || !strings.Contains(rec[newer+"played_time"], `"played_ms":600000`) {
		t.Errorf("the later release: position %s, played %s", rec[newer+"position"], rec[newer+"played_time"])
	}
	if units := rec[newer+"statistics_units"]; strings.Contains(units, `"id"`) {
		t.Errorf("the later release has another row's unit: %s", units)
	}
}
