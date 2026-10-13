package ops

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// PROTOCOL §11.5: imported state, baselines and the first fetch after an
// import (PROTOCOL §11.5).

const testImportID = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"

type importItem struct {
	guid, enclosure string
	pub             int64
	row             ImportRow
}

func importPodcast(now int64, items ...importItem) ImportPodcast {
	x := ImportPodcast{ImportID: testImportID, SourceFeeds: []int64{1}, URL: "https://example.com/feed", State: "subscribed",
		KeepUpdated: true, NewEpisodes: "global", Time: now}
	title := "Show"
	x.Feed = ImportFeedFields(&feed.Feed{Type: "rss", Title: &title, Funding: []feed.Funding{}})
	for i, it := range items {
		guid, enclosure, pub := it.guid, it.enclosure, it.pub
		t := strings.TrimPrefix(guid, "guid-")
		fi := &feed.Item{GUID: &guid, Title: &t, PubDate: &pub, Media: &feed.Media{URL: &enclosure, Size: 1000}}
		r := it.row
		r.SourceItem, r.SourceMedia = int64(i+1), int64(i+1)
		if r.PlayState == "" {
			r.PlayState = "unplayed"
		}
		if r.QueueIndex == 0 && r.QueueKey == "" {
			r.QueueIndex = -1
		}
		r.Item = ImportItem("rss", fi)
		x.Rows = append(x.Rows, r)
	}
	return x
}

func (e *env) applyImport(t *testing.T, x ImportPodcast) ImportedPodcast {
	t.Helper()
	var got ImportedPodcast
	err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
		var err error
		got, err = ApplyImportPodcast(c, x)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// The import's completing decision, as the importer writes it after its
	// podcasts: its queue is one replay step there (PROTOCOL §7.6).
	if err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
		// The manifest's counts are its completeness evidence (PROTOCOL §11.5): every podcast and episode
		// decision of the import so far.
		counts := map[string]int64{}
		for _, typ := range []string{"import.podcast", "import.episode"} {
			rows, err := c.Account.DecisionsOf(typ)
			if err != nil {
				return err
			}
			for _, row := range rows {
				d, err := decodeObject(row.Decision)
				if err != nil {
					return err
				}
				_, inputs, result := decisionParts(d)
				if id, _ := str(inputs, "import_id"); id != x.ImportID {
					continue
				}
				counts[typ]++
				if _, queued := result["queue_index"]; queued {
					counts["queue"]++
				}
			}
		}
		_, err := ApplyImportManifest(c, x.ImportID, map[string]any{"import_id": x.ImportID, "result": map[string]any{"imported": map[string]any{
			"podcasts": counts["import.podcast"], "episodes": counts["import.episode"], "queue": counts["queue"]}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func (e *env) pushOK(t *testing.T, d *device, f map[string]any, want string) {
	t.Helper()
	if r := e.push(d, f); outcome(r[0]) != want {
		t.Fatalf("%s: %s, want %s", f["type"], outcome(r[0]), want)
	}
}

// allNodes is the account's causal frontier as a device that pulled
// everything holds it.
func (e *env) allNodes(t *testing.T) []string {
	var ids []string
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		ids, err = r.Account(e.account).OrderedNodeIDs()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func epRef(p ImportedPodcast, x ImportPodcast, item int64) map[string]any {
	occ := map[string]any{}
	for k, v := range x.Rows[item-1].Item.Occurrence {
		occ[k] = v
	}
	return map[string]any{"episode_id": p.EpisodeOf[item], "podcast_id": p.PodcastID, "generation_id": p.GenerationID, "occurrence": occ}
}

// Scenario 36 and PROTOCOL §11.5: imported baselines count until a statistics reset
// covers them; rows merged into one episode keep their own statistics as
// units; a second application of the same podcast is refused, so retries
// never recreate baselines.
func TestImportBaselinesAndReset(t *testing.T) {
	e := newEnv(t)
	day := int64(86_400_000)
	jan, feb := int64(1_767_225_600_000), int64(1_769_904_000_000) // 2026-01-01, 2026-02-01 UTC
	h := jan + day
	x := importPodcast(e.clock,
		importItem{"guid-A", "https://cdn.example.com/a.mp3", jan - 10*day, ImportRow{PlayState: "played", PlayedTime: 600_000, LastPlayed: jan + day, History: &h}},
		importItem{"guid-B", "https://cdn.example.com/b1.mp3", jan - 3*day, ImportRow{PlayedTime: 100_000, LastPlayed: jan + 2*day}},
		importItem{"guid-B", "https://cdn.example.com/b2.mp3", jan - 2*day, ImportRow{PlayedTime: 200_000, LastPlayed: feb + day, Position: 30_000}})
	p := e.applyImport(t, x)
	if len(p.Merged) != 1 || p.EpisodeOf[2] != p.EpisodeOf[3] {
		t.Fatalf("the two B rows are one episode: %+v", p)
	}
	recs := e.records()
	a, b := "episode/"+p.EpisodeOf[1]+"/", "episode/"+p.EpisodeOf[2]+"/"
	if got := recs[a+"played_time"]; !strings.Contains(got, `"played_ms":600000`) {
		t.Errorf("A played time %s", got)
	}
	if got := recs[b+"played_time"]; !strings.Contains(got, `"played_ms":200000`) {
		t.Errorf("B played time (its state row only) %s", got)
	}
	if got := recs[b+"last_played"]; !strings.Contains(got, `"time":1769990400000`) {
		t.Errorf("B last played (the state row's) %s", got)
	}
	if got := recs[b+"position"]; !strings.Contains(got, `"position":30000`) {
		t.Errorf("B position %s", got)
	}
	if got := recs[b+"statistics_units"]; !strings.Contains(got, `"played_ms":100000`) || !strings.Contains(got, `"last_played":1767398400000`) {
		t.Errorf("B's other row as a unit %s", got)
	}
	if got := recs[a+"history"]; !strings.Contains(got, `"time":1767312000000`) {
		t.Errorf("A history %s", got)
	}
	if got := recs[b+"history"]; got != "" {
		t.Errorf("B has no history: %s", got)
	}
	// A device that received the import resets statistics.
	d := e.device("A")
	e.pushOK(t, d, e.lfact(d, "statistics.reset", nil, map[string]any{}, e.allNodes(t)...), "applied")
	recs = e.records()
	if got := recs[a+"played_time"]; !strings.Contains(got, `"played_ms":0`) {
		t.Errorf("A after the reset %s", got)
	}
	if got := recs[b+"played_time"]; !strings.Contains(got, `"played_ms":0`) {
		t.Errorf("B after the reset %s", got)
	}
	if got := recs[b+"statistics_units"]; !strings.Contains(got, `"units":[]`) {
		t.Errorf("B's units after the reset %s", got)
	}
	// The same podcast again: refused, nothing recreated.
	err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
		_, err := ApplyImportPodcast(c, x)
		return err
	})
	if err == nil {
		t.Fatal("a second application was accepted")
	}
	if got := e.records()[a+"played_time"]; !strings.Contains(got, `"played_ms":0`) {
		t.Errorf("a refused retry changed A: %s", got)
	}
}

// PROTOCOL §11.5: NEW is the Inbox until an explicit interaction, as an automatic
// filing is; a history clear covers imported history.
func TestImportInboxAndHistoryClear(t *testing.T) {
	e := newEnv(t)
	h := e.clock - 86_400_000
	x := importPodcast(e.clock,
		importItem{"guid-N", "https://cdn.example.com/n.mp3", e.clock - 2*86_400_000, ImportRow{PlayState: "new"}},
		importItem{"guid-H", "https://cdn.example.com/h.mp3", e.clock - 3*86_400_000, ImportRow{History: &h, LastPlayed: h}})
	p := e.applyImport(t, x)
	n, hs := "episode/"+p.EpisodeOf[1]+"/", "episode/"+p.EpisodeOf[2]+"/"
	if got := e.records()[n+"play_state"]; !strings.Contains(got, `"play_state":"new"`) {
		t.Fatalf("NEW imported as %s", got)
	}
	d := e.device("A")
	e.clock += minute
	e.pushOK(t, d, e.lfact(d, "session.start", epRef(p, x, 1), start(newID(), 0, true), e.allNodes(t)...), "applied")
	if got := e.records()[n+"play_state"]; !strings.Contains(got, `"play_state":"unplayed"`) {
		t.Errorf("playing a NEW episode leaves the Inbox: %s", got)
	}
	e.pushOK(t, d, e.lfact(d, "history.clear", nil, map[string]any{}, e.allNodes(t)...), "applied")
	if got := e.records()[hs+"history"]; !strings.Contains(got, `"time":null`) {
		t.Errorf("a history clear covers imported history: %s", got)
	}
	if got := e.records()[hs+"last_played"]; !strings.Contains(got, `"time":`) {
		t.Errorf("last played stays: %s", got)
	}
}

// PROTOCOL §11.5: an imported generation is populated, so the first server fetch after
// the import files a release newer than the imported items, and nothing the
// backup held.
func TestFirstFetchAfterImportFilesNewReleases(t *testing.T) {
	e := newEnv(t)
	day := int64(86_400_000)
	x := importPodcast(e.clock,
		importItem{"guid-Old", "https://cdn.example.com/Old.mp3", e.clock - 5*day, ImportRow{}},
		importItem{"guid-Kept", "https://cdn.example.com/Kept.mp3", e.clock - 2*day, ImportRow{}})
	p := e.applyImport(t, x)
	e.clock += day
	f := serverFeed(t, time.UnixMilli(e.clock), feedItem{"Old", e.clock - 6*day, true}, feedItem{"Kept", e.clock - 3*day, true},
		feedItem{"New", e.clock - minute, true})
	err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
		g, err := generationState(c, p.PodcastID, p.GenerationID)
		if err != nil {
			return err
		}
		return ApplyServerFetch(c, ServerFetch{PodcastID: p.PodcastID, GenerationID: p.GenerationID, RefreshBy: g.RefreshBy,
			RefreshID: newID(), FetchTime: e.clock, Feed: f, Namespace: e.namespace, Bases: map[string]string{}})
	})
	if err != nil {
		t.Fatal(err)
	}
	admitted := 0
	for k, v := range e.records() {
		if strings.HasSuffix(k, "/admission") && v != "" {
			admitted++
			if strings.Contains(k, p.EpisodeOf[1]) || strings.Contains(k, p.EpisodeOf[2]) {
				t.Errorf("an imported episode was filed: %s", k)
			}
		}
	}
	if admitted != 1 {
		t.Errorf("%d admissions, want the new release only", admitted)
	}
}

// A merged episode keeps its state row's own history, so "episodes started"
// counts as Android counts rows: the older row with history is started (its
// unit), the newer state row without history is not.
func TestMergedRowsKeepTheirOwnHistory(t *testing.T) {
	e := newEnv(t)
	day := int64(86_400_000)
	h := e.clock - 5*day
	x := importPodcast(e.clock,
		importItem{"guid-M", "https://cdn.example.com/m1.mp3", e.clock - 20*day, ImportRow{PlayedTime: 100_000, LastPlayed: e.clock - 5*day, History: &h}},
		importItem{"guid-M", "https://cdn.example.com/m2.mp3", e.clock - 19*day, ImportRow{PlayedTime: 200_000, LastPlayed: e.clock - day}})
	p := e.applyImport(t, x)
	if p.EpisodeOf[1] != p.EpisodeOf[2] {
		t.Fatalf("the two rows are not one episode: %+v", p)
	}
	recs := e.records()
	pre := "episode/" + p.EpisodeOf[1] + "/"
	if got := recs[pre+"history"]; got != "" {
		t.Errorf("the state row has no history, but the episode has %s", got)
	}
	if got := recs[pre+"statistics_units"]; !strings.Contains(got, `"history":`+strconv.FormatInt(h, 10)) {
		t.Errorf("the older row's unit lost its history: %s", got)
	}
}
