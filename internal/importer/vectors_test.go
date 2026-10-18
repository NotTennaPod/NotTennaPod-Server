package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The independent import reference's vectors (contract/vectors/import,
// contract/reference/import_android.py): the server's import of each file
// matches them as docs/CONTRACT.md ("Import vectors") says a Go test compares.

type importVector struct {
	Fixture string `json:"fixture"`
	Result  struct {
		Imported   Counts            `json:"imported"`
		Skipped    []json.RawMessage `json:"skipped"`
		Ambiguous  []json.RawMessage `json:"ambiguous"`
		Merged     []json.RawMessage `json:"merged"`
		NotCarried json.RawMessage   `json:"not_carried"`
	} `json:"result"`
	Podcasts []struct {
		CreatingFeedID    int64   `json:"creating_feed_id"`
		SourceFeedIDs     []int64 `json:"source_feed_ids"`
		URL               string  `json:"url"`
		State             string  `json:"state"`
		KeepUpdated       bool    `json:"keep_updated"`
		NewEpisodesAction string  `json:"new_episodes_action"`
		Credentials       bool    `json:"credentials"`
		Populated         bool    `json:"populated"`
		Episodes          []struct {
			SourceItemIDs []int64 `json:"source_item_ids"`
			Identity      struct {
				K            string `json:"k"`
				V            string `json:"v"`
				Reason       string `json:"reason"`
				SourceItemID int64  `json:"source_item_id"`
			} `json:"identity"`
			PlayState  string `json:"play_state"`
			PositionMS int64  `json:"position_ms"`
			Favorite   bool   `json:"favorite"`
			Queued     bool   `json:"queued"`
			DurationMS *int64 `json:"duration_ms"`
			History    *int64 `json:"history"` // the latest completion among the rows
			Baseline   *struct {
				PlayedMS   int64  `json:"played_ms"`
				LastPlayed *int64 `json:"last_played"`
				History    *int64 `json:"history"`
			} `json:"baseline"`
			Units []struct {
				SourceMediaID int64  `json:"source_media_id"`
				PlayedMS      int64  `json:"played_ms"`
				LastPlayed    *int64 `json:"last_played"`
				History       *int64 `json:"history"`
				DurationMS    int64  `json:"duration_ms"`
				Played        bool   `json:"played"`
				PositionMS    *int64 `json:"position_ms"`
			} `json:"units"`
			Chapters json.RawMessage `json:"chapters"` // the metadata's expected chapters
		} `json:"episodes"`
	} `json:"podcasts"`
	Queue []int64 `json:"queue"`
}

func TestImportVectors(t *testing.T) {
	files, _ := filepath.Glob("../../contract/vectors/import/*.json")
	if len(files) == 0 {
		t.Skip("no import vectors")
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".json"), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var suite struct {
				Cases []importVector `json:"cases"`
			}
			if err := json.Unmarshal(data, &suite); err != nil {
				t.Fatal(err)
			}
			for _, v := range suite.Cases {
				checkImportVector(t, v)
			}
		})
	}
}

func checkImportVector(t *testing.T, v importVector) {
	ctx := context.Background()
	e := newEnv(t)
	lib, err := androiddb.Read(ctx, filepath.Join("../..", v.Fixture), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const importID = "0e5a1c2d-3b4f-4a6e-8c7d-9f0a1b2c3d4e"
	plan, err := FromAndroid(lib, importID, e.clock)
	if err != nil {
		t.Fatal(err)
	}
	pods, err := Apply(ctx, e.svc, e.account, plan, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Complete(ctx, e.svc, e.account, importID, "", plan); err != nil {
		t.Fatal(err)
	}
	res, err := Summarize(ctx, e.svc.Store, e.account, plan)
	if err != nil {
		t.Fatal(err)
	}
	recs := e.records()
	var namespace string
	if err := e.svc.Store.Read(ctx, func(r *storage.ReadTx) error {
		namespace, err = r.Account(e.account).Namespace()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Result counts and lists.
	if res.Imported != v.Result.Imported {
		t.Errorf("imported %+v, want %+v", res.Imported, v.Result.Imported)
	}
	compareEntries(t, "skipped", res.Skipped, v.Result.Skipped)
	compareEntries(t, "ambiguous", res.Ambiguous, v.Result.Ambiguous)
	compareEntries(t, "merged", res.Merged, v.Result.Merged)
	if b, _ := json.Marshal(res.NotCarried); canonical(t, b) != canonical(t, v.Result.NotCarried) {
		t.Errorf("not carried %s, want %s", b, v.Result.NotCarried)
	}
	// Podcasts.
	if len(plan.Podcasts) != len(v.Podcasts) {
		t.Fatalf("%d podcasts, want %d", len(plan.Podcasts), len(v.Podcasts))
	}
	episodeOf := map[int64]string{}
	for i, want := range v.Podcasts {
		p, got := plan.Podcasts[i], pods[i]
		for item, ep := range got.EpisodeOf {
			episodeOf[item] = ep
		}
		if fmt.Sprint(p.SourceFeeds) != fmt.Sprint(want.SourceFeedIDs) || p.URL != want.URL {
			t.Errorf("podcast %d: feeds %v %s, want %v %s", i, p.SourceFeeds, p.URL, want.SourceFeedIDs, want.URL)
		}
		pre := "podcast/" + got.PodcastID + "/"
		if s := recs[pre+"subscription"]["state"]; s != want.State {
			t.Errorf("podcast %d: state %v, want %s", want.CreatingFeedID, s, want.State)
		}
		pol := recs[pre+"policy"]
		if pol["keep_updated"] != want.KeepUpdated || pol["new_episodes_action"] != want.NewEpisodesAction {
			t.Errorf("podcast %d: policy %v, want keep_updated %v action %s", want.CreatingFeedID, pol, want.KeepUpdated, want.NewEpisodesAction)
		}
		if _, has := recs[pre+"credentials"]; has != want.Credentials {
			t.Errorf("podcast %d: credentials %v, want %v", want.CreatingFeedID, has, want.Credentials)
		}
		populated := false
		if err := e.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
			node, err := tx.Account(e.account).Population(got.GenerationID)
			populated = node != ""
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if populated != want.Populated {
			t.Errorf("podcast %d: populated %v, want %v", want.CreatingFeedID, populated, want.Populated)
		}
		// Episodes: the same partition, IDs, state, baselines and units.
		for _, we := range want.Episodes {
			ep := episodeOf[we.SourceItemIDs[0]]
			for _, item := range we.SourceItemIDs {
				if got := got.EpisodeOf[item]; got != ep {
					t.Errorf("items %v are not one episode (%d is %s)", we.SourceItemIDs, item, got)
				}
			}
			switch {
			case we.Identity.K == "e0" && we.Identity.Reason == "no_guid_no_enclosure":
				if want := ops.ImportOccurrenceID(importID, we.Identity.SourceItemID); ep != want {
					t.Errorf("items %v: %s, want %s", we.SourceItemIDs, ep, want)
				}
			case we.Identity.K == "e0":
				if !strings.HasPrefix(ep, "e0:") {
					t.Errorf("items %v: %s, want an e0: ID", we.SourceItemIDs, ep)
				}
			default:
				want, err := contract.EpisodeID(namespace, got.PodcastID, we.Identity.K, we.Identity.V)
				if err != nil {
					t.Fatal(err)
				}
				if ep != want {
					t.Errorf("items %v: %s, want %s (%s %q)", we.SourceItemIDs, ep, want, we.Identity.K, we.Identity.V)
				}
			}
			r := func(group string) map[string]any { return recs["episode/"+ep+"/"+group] }
			what := fmt.Sprintf("items %v", we.SourceItemIDs)
			if s := r("play_state")["play_state"]; s != we.PlayState {
				t.Errorf("%s: play state %v, want %s", what, s, we.PlayState)
			}
			if n := num(r("position")["position"]); n != we.PositionMS {
				t.Errorf("%s: position %d, want %d", what, n, we.PositionMS)
			}
			if (r("favorite")["on"] == true) != we.Favorite {
				t.Errorf("%s: favorite %v, want %v", what, r("favorite"), we.Favorite)
			}
			if (r("queue_membership")["in_queue"] == true) != we.Queued {
				t.Errorf("%s: queued %v, want %v", what, r("queue_membership"), we.Queued)
			}
			wantDur := int64(0)
			if we.DurationMS != nil {
				wantDur = *we.DurationMS
			}
			if n := num(r("measured_duration")["duration_ms"]); n != wantDur {
				t.Errorf("%s: duration %d, want %d", what, n, wantDur)
			}
			var played, last, hist int64
			if b := we.Baseline; b != nil {
				played = b.PlayedMS
				if b.LastPlayed != nil {
					last = *b.LastPlayed
				}
				if b.History != nil {
					hist = *b.History
				}
			}
			hist = 0
			if we.History != nil {
				hist = *we.History // the episode's history: the latest of its rows
			}
			if n := num(r("played_time")["played_ms"]); n != played {
				t.Errorf("%s: played time %d, want %d", what, n, played)
			}
			if n := num(r("last_played")["time"]); n != last {
				t.Errorf("%s: last played %d, want %d", what, n, last)
			}
			if n := num(r("history")["time"]); n != hist {
				t.Errorf("%s: history %d, want %d", what, n, hist)
			}
			// Stored chapters: exactly the expected list (a merged episode's
			// from its first row with any, PROTOCOL §11.5).
			if gotChapters, _ := json.Marshal(r("metadata")["chapters"]); canonical(t, gotChapters) != canonical(t, we.Chapters) {
				t.Errorf("%s: chapters %s, want %s", what, gotChapters, we.Chapters)
			}
			gotUnits := map[int64]map[string]any{}
			if list, ok := r("statistics_units")["units"].([]any); ok {
				for _, x := range list {
					u := x.(map[string]any)
					id := u["id"].(string)
					m, _ := strconv.ParseInt(id[strings.LastIndexByte(id, '/')+1:], 10, 64)
					gotUnits[m] = u
				}
			}
			if len(gotUnits) != len(we.Units) {
				t.Errorf("%s: %d units, want %d", what, len(gotUnits), len(we.Units))
			}
			for _, wu := range we.Units {
				gu := gotUnits[wu.SourceMediaID]
				if gu == nil {
					t.Errorf("%s: no unit for media %d", what, wu.SourceMediaID)
					continue
				}
				var wl, wh int64
				if wu.LastPlayed != nil {
					wl = *wu.LastPlayed
				}
				if wu.History != nil {
					wh = *wu.History
				}
				if num(gu["played_ms"]) != wu.PlayedMS || num(gu["last_played"]) != wl || num(gu["history"]) != wh ||
					num(gu["duration_ms"]) != wu.DurationMS || gu["played"] != wu.Played ||
					(wu.PositionMS != nil && num(gu["position"]) != *wu.PositionMS) {
					t.Errorf("%s: unit %d is %v, want %+v", what, wu.SourceMediaID, gu, wu)
				}
			}
		}
	}
	// The queue, front to back.
	type entry struct{ key, ep string }
	var queue []entry
	for k, val := range recs {
		if strings.HasSuffix(k, "/queue_position") {
			ep := strings.TrimSuffix(strings.TrimPrefix(k, "episode/"), "/queue_position")
			if recs["episode/"+ep+"/queue_membership"]["in_queue"] == true {
				queue = append(queue, entry{val["key"].(string), ep})
			}
		}
	}
	sort.Slice(queue, func(i, j int) bool { return queue[i].key < queue[j].key })
	if len(queue) != len(v.Queue) {
		t.Fatalf("%d queued, want %d", len(queue), len(v.Queue))
	}
	for i, item := range v.Queue {
		if queue[i].ep != episodeOf[item] {
			t.Errorf("queue %d: %s, want item %d's %s", i, queue[i].ep, item, episodeOf[item])
		}
	}
}

// compareEntries compares result entries as canonical JSON sets.
func compareEntries(t *testing.T, what string, got []Entry, want []json.RawMessage) {
	t.Helper()
	g := map[string]bool{}
	for _, e := range got {
		b, _ := json.Marshal(e)
		g[canonical(t, b)] = true
	}
	w := map[string]bool{}
	for _, raw := range want {
		w[canonical(t, raw)] = true
	}
	for k := range w {
		if !g[k] {
			t.Errorf("%s: missing %s", what, k)
		}
	}
	for k := range g {
		if !w[k] {
			t.Errorf("%s: unexpected %s", what, k)
		}
	}
}

func canonical(t *testing.T, raw []byte) string {
	v, err := contract.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	b, err := contract.Canonical(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
