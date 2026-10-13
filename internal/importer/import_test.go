package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/rebuildtest"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
)

type env struct {
	t       *testing.T
	svc     *protocol.Service
	account string
	clock   int64
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, clock: 1_790_000_000_000}
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		e.account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewRegistry()
	ops.Register(reg)
	views, err := protocol.NewViewRegistry(filepath.Join(store.Dir(), "views"), nil)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = &protocol.Service{Store: store, Registry: reg, Views: views, Box: box, Now: func() time.Time { return time.UnixMilli(e.clock) }}
	// Every test ends with the rebuild invariant (PROTOCOL §6.11) on the account as
	// it is then (before the store closes).
	t.Cleanup(func() { rebuildtest.Check(t, e.svc, e.account, nil) })
	return e
}

func (e *env) records() map[string]map[string]any {
	out := map[string]map[string]any{}
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		rows, err := r.Account(e.account).Records(nil)
		for _, x := range rows {
			if x.Value == "" {
				continue
			}
			var v map[string]any
			if err := json.Unmarshal([]byte(x.Value), &v); err != nil {
				return err
			}
			out[x.Entity+"/"+x.EntityID+"/"+x.FieldGroup] = v
		}
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}

func (e *env) importFixture(path string) (*androiddb.Library, Plan, importOutcome) {
	e.t.Helper()
	ctx := context.Background()
	lib, err := androiddb.Read(ctx, path, e.t.TempDir())
	if err != nil {
		e.t.Fatal(err)
	}
	const importID = "6a1f6a0e-8a9b-4c55-9f6e-2b1d3c4e5f60"
	plan, err := FromAndroid(lib, importID, e.clock)
	if err != nil {
		e.t.Fatal(err)
	}
	pods, err := Apply(ctx, e.svc, e.account, plan, 0)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := Complete(ctx, e.svc, e.account, importID, "", plan); err != nil {
		e.t.Fatal(err)
	}
	res, err := Summarize(ctx, e.svc.Store, e.account, plan)
	if err != nil {
		e.t.Fatal(err)
	}
	return lib, plan, importOutcome{Podcasts: pods, Imported: res.Imported}
}

type importOutcome struct {
	Podcasts []ops.ImportedPodcast
	Imported Counts
}

func num(v any) int64 {
	f, _ := v.(float64)
	return int64(f)
}

// Every imported row's records show its Android state: play state (NEW is the
// Inbox), position, favorite, history, last played and played time, and the
// queue keeps Android's order (PROTOCOL §11.5).
func TestImportReproducesAndroidState(t *testing.T) {
	for _, name := range []string{"emulator-parity-library.db", "archived-credentials-library.db", "authentication-preview-library.db",
		"first-journey-downloaded.db"} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			lib, plan, res := e.importFixture(filepath.Join("androiddb/testdata/android-3.12.1", name))
			recs := e.records()
			episodeOf := map[int64]string{}
			merged := map[int64]bool{}
			for _, p := range res.Podcasts {
				for item, ep := range p.EpisodeOf {
					episodeOf[item] = ep
				}
				for _, g := range p.Merged {
					for _, id := range g {
						merged[id] = true
					}
				}
			}
			checked := 0
			skipped := map[int64]bool{}
			for _, sk := range plan.Skipped {
				skipped[sk.SourceFeedID] = true
			}
			for _, f := range lib.Feeds {
				if f.Local() || skipped[f.ID] {
					continue
				}
				for _, ep := range f.Episodes {
					id := episodeOf[ep.ID]
					if id == "" {
						t.Fatalf("item %d has no episode", ep.ID)
					}
					if merged[ep.ID] {
						continue
					}
					checked++
					want := map[int32]string{1: "played", 0: "unplayed", -1: "new"}[ep.Read]
					if got := recs["episode/"+id+"/play_state"]["play_state"]; got != want {
						t.Errorf("item %d play state %v, want %s", ep.ID, got, want)
					}
					if ep.Favorite != (recs["episode/"+id+"/favorite"]["on"] == true) {
						t.Errorf("item %d favorite %v", ep.ID, recs["episode/"+id+"/favorite"])
					}
					if m := ep.Media; m != nil {
						if got := num(recs["episode/"+id+"/position"]["position"]); got != int64(m.Position) {
							t.Errorf("item %d position %d, want %d", ep.ID, got, m.Position)
						}
						if m.PlayedDuration > 0 && num(recs["episode/"+id+"/played_time"]["played_ms"]) != m.PlayedDuration {
							t.Errorf("item %d played time %v, want %d", ep.ID, recs["episode/"+id+"/played_time"], m.PlayedDuration)
						}
						if m.LastPlayedTime > 0 && num(recs["episode/"+id+"/last_played"]["time"]) != m.LastPlayedTime {
							t.Errorf("item %d last played %v, want %d", ep.ID, recs["episode/"+id+"/last_played"], m.LastPlayedTime)
						}
						h, has := recs["episode/"+id+"/history"]
						if (m.CompletionDate > 0) != has || (has && num(h["time"]) != m.CompletionDate) {
							t.Errorf("item %d history %v, want %d", ep.ID, h, m.CompletionDate)
						}
					}
				}
			}
			if checked == 0 {
				t.Fatal("no rows checked")
			}
			t.Logf("%d rows checked, %d queued, %d podcasts, result %+v", checked, len(lib.Queue), len(plan.Podcasts), res.Imported)
			// The queue: Android's order by key.
			type entry struct {
				key  string
				item int64
			}
			var queue []entry
			itemOf := map[string]int64{}
			for item, ep := range episodeOf {
				itemOf[ep] = item
			}
			for k, v := range recs {
				if len(k) > 15 && k[len(k)-15:] == "/queue_position" {
					if m := recs[k[:len(k)-15]+"/queue_membership"]; m["in_queue"] == true {
						ep := k[len("episode/") : len(k)-15]
						queue = append(queue, entry{v["key"].(string), itemOf[ep]})
					}
				}
			}
			sort.Slice(queue, func(i, j int) bool { return queue[i].key < queue[j].key })
			if len(queue) != len(lib.Queue) {
				t.Fatalf("%d queued, want %d", len(queue), len(lib.Queue))
			}
			for i, q := range queue {
				if episodeOf[q.item] != episodeOf[lib.Queue[i]] {
					t.Errorf("queue %d is item %d, want %d", i, q.item, lib.Queue[i])
				}
			}
			// Subscriptions: lifecycle, keep updated, new-episodes action, credentials.
			for i, p := range plan.Podcasts {
				pid := res.Podcasts[i].PodcastID
				sub := recs["podcast/"+pid+"/subscription"]
				if sub["state"] != p.State {
					t.Errorf("podcast %v state %v, want %s", p.SourceFeeds, sub["state"], p.State)
				}
			}
		})
	}
}

// library is the account's read views, as /v1/read shows them.
func (e *env) library() *views.Library {
	e.t.Helper()
	var lib *views.Library
	if err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		lib, err = views.Load(r.Account(e.account))
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	return lib
}

// decisions are the account's decisions of one type, in receipt order.
func (e *env) decisions(typ string) []map[string]any {
	e.t.Helper()
	var out []map[string]any
	if err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		rows, err := r.Account(e.account).DecisionsOfType(typ)
		for _, raw := range rows {
			d, err := decodeDecision(raw)
			if err != nil {
				return err
			}
			out = append(out, d)
		}
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// populated is the node that recorded a generation's first population ("":
// unpopulated) and the episodes in its podcast's server fetch history.
func (e *env) populated(podcast, generation string) (node string, seen []string) {
	e.t.Helper()
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		var err error
		if node, err = a.Population(generation); err != nil {
			return err
		}
		seen, err = a.ServerSeenEpisodes(podcast)
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	return node, seen
}

// rssItem is a feed item with an enclosure, published at pub.
type rssItem struct {
	name string
	pub  int64
}

// fetch applies a server fetch of an RSS feed titled title with these items,
// capturing the podcast's refresh context as the refresher does.
func (e *env) fetch(podcast, refreshID, title string, items ...rssItem) {
	e.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, `<rss version="2.0"><channel><title>%s</title>`, title)
	for _, it := range items {
		fmt.Fprintf(&b, `<item><title>%s</title><guid>guid-%s</guid><pubDate>%s</pubDate>`, it.name, it.name, time.UnixMilli(it.pub).UTC().Format(time.RFC1123Z))
		fmt.Fprintf(&b, `<enclosure url="https://cdn.example.invalid/%s.mp3" length="20000" type="audio/mpeg"/></item>`, it.name)
	}
	b.WriteString(`</channel></rss>`)
	f, err := feed.Parse(context.Background(), []byte(b.String()), "https://odd.example.invalid/feed.xml", time.UnixMilli(e.clock))
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
		rc, err := ops.CaptureRefresh(c, podcast)
		if err != nil {
			return err
		}
		return ops.ApplyServerFetch(c, ops.ServerFetch{PodcastID: podcast, GenerationID: rc.GenerationID, RefreshBy: rc.RefreshBy,
			RefreshID: refreshID, FetchTime: e.clock, Feed: f, Bases: rc.Bases, Namespace: c.Namespace})
	}); err != nil {
		e.t.Fatal(err)
	}
}

// part0 is a server fetch's part 0.
func (e *env) part0(podcast, refreshID string) map[string]any {
	e.t.Helper()
	for _, d := range e.decisions("catalog.refresh") {
		if d["subject"] == "refresh:"+podcast+":"+refreshID+":0" {
			return d
		}
	}
	e.t.Fatalf("no part 0 of refresh %s", refreshID)
	return nil
}

// PROTOCOL §11.5 "feed metadata" and PROTOCOL §11.5 (found in a hands-on check: an imported
// podcast without episodes showed its URL on the web and a blank tile on the
// phone): a subscribed feed with a title and no episodes (feeds-lifecycle
// feed 8, "Odd action") keeps the backup's podcast record, with no revision
// base and the import's time, so it is named before any fetch. Its generation
// stays unpopulated, without seen history, so its first fetch is still a
// first population that files nothing, and that fetch's record replaces the
// imported one.
func TestImportedPodcastWithoutEpisodesKeepsItsName(t *testing.T) {
	e := newEnv(t)
	imported := e.clock
	_, plan, res := e.importFixture("../../contract/vectors/import/feeds-lifecycle.db")
	var p ops.ImportedPodcast
	for i, x := range plan.Podcasts {
		if x.SourceFeeds[0] == 8 {
			p = res.Podcasts[i]
		}
	}
	if p.PodcastID == "" || p.Episodes != 0 {
		t.Fatalf("feed 8 imported as %+v", p)
	}
	pod := e.library().Podcasts[p.PodcastID]
	if pod == nil || pod.State != "subscribed" || pod.Title == nil || *pod.Title != "Odd action" || pod.DisplayTitle != "Odd action" {
		t.Fatalf("the imported podcast reads as %+v", pod)
	}
	if node, seen := e.populated(p.PodcastID, p.GenerationID); node != "" || len(seen) != 0 {
		t.Fatalf("the generation without episodes is populated by %q with seen history %v", node, seen)
	}
	// Its record: one part 0 with the feed record and nothing else.
	var parts []map[string]any
	for _, d := range e.decisions("catalog.refresh") {
		if in := d["inputs"].(map[string]any); in["podcast_id"] == p.PodcastID {
			parts = append(parts, d)
		}
	}
	if len(parts) != 1 {
		t.Fatalf("%d catalog.refresh decisions for the podcast, want 1", len(parts))
	}
	in, result := parts[0]["inputs"].(map[string]any), parts[0]["result"].(map[string]any)
	imp, _ := result["feed"].(map[string]any)
	if fmt.Sprint(in["part"]) != "0" || fmt.Sprint(in["fetch_time"]) != fmt.Sprint(imported) || in["generation_id"] != p.GenerationID ||
		len(result) != 2 || imp == nil || imp["base"] != nil || len(result["items"].([]any)) != 0 {
		t.Fatalf("the imported record: inputs %v, result %v", in, result)
	}
	if fields := imp["fields"].(map[string]any); fields["title"] != "Odd action" || fields["type"] != "rss" {
		t.Errorf("the imported record's fields %v", fields)
	}
	importPart := parts[0]["decision_id"].(string)

	// The first fetch: a first population that files nothing, whose record
	// replaces the imported one.
	day := int64(86_400_000)
	e.clock += day
	first := "3f0c2a8e-4b1d-4e6f-9a2c-5d7e8f901234"
	e.fetch(p.PodcastID, first, "Odd Action Feed", rssItem{"Old", e.clock - 30*day}, rssItem{"New", e.clock - 60_000})
	fetched := e.part0(p.PodcastID, first)
	r := fetched["result"].(map[string]any)
	if r["population"] != true || len(r["items"].([]any)) != 2 {
		t.Fatalf("the first fetch's part 0: %v", r)
	}
	if node, seen := e.populated(p.PodcastID, p.GenerationID); node != fetched["decision_id"] || len(seen) != 2 {
		t.Errorf("first population by %q with %d seen, want %s with 2", node, len(seen), fetched["decision_id"])
	}
	if got := e.decisions("discovery.admission"); len(got) != 0 {
		t.Errorf("the first population filed %d", len(got))
	}
	if base := r["feed"].(map[string]any)["base"]; base != importPart {
		t.Errorf("the fetch's record replaces %v, want the imported record %s", base, importPart)
	}
	meta := e.records()["podcast/"+p.PodcastID+"/metadata"]
	if meta["title"] != "Odd Action Feed" || meta["revision"] != fetched["decision_id"] {
		t.Errorf("metadata after the first fetch %v", meta)
	}
	if pod := e.library().Podcasts[p.PodcastID]; pod.Title == nil || *pod.Title != "Odd Action Feed" || pod.DisplayTitle != "Odd Action Feed" {
		t.Errorf("after the first fetch the podcast reads as %+v", pod)
	}
	// Populated from then on, as any fetched generation: a later release is filed.
	e.clock += day
	e.fetch(p.PodcastID, "7a9d4c1b-2e3f-4a5b-8c6d-0e1f2a3b4c5d", "Odd Action Feed",
		rssItem{"Old", e.clock - 31*day}, rssItem{"New", e.clock - day - 60_000}, rssItem{"Newer", e.clock - 60_000})
	if got := e.decisions("discovery.admission"); len(got) != 1 {
		t.Errorf("the next fetch filed %d, want the later release", len(got))
	}
}
