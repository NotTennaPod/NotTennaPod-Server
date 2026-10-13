package ops

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// PROTOCOL §9.4: catalog observations, discovery proposals and admissions,
// and the server's own fetches.

type catalogEnv struct{ *queueEnv }

func newCatalogEnv(t *testing.T, titles ...string) *catalogEnv {
	return &catalogEnv{newQueueEnv(t, titles...)}
}

// itemRef is an episode's whole reference, as an observation names it.
func (c *catalogEnv) itemRef(title string) map[string]any {
	return maps.Clone(c.refs[title])
}

// fullItem completes an observed episode metadata value: an observation sends
// whole records (PROTOCOL §9.4), so every field it does not set is null.
func fullItem(fields map[string]any) map[string]any {
	out := maps.Clone(fields)
	for _, k := range episodeMetadataFields {
		if _, ok := out[k]; !ok {
			out[k] = nil
		}
	}
	return out
}

// observe builds a catalog.observe fact of the named episodes; playable ones
// have an enclosure.
func (c *catalogEnv) observe(d *device, pub map[string]int64, playable ...string) map[string]any {
	titles := make([]string, 0, len(pub))
	for title := range pub {
		titles = append(titles, title)
	}
	sort.Strings(titles)
	var items []any
	targets := []any{}
	for _, title := range titles {
		fields := map[string]any{"title": title, "pub_date": pub[title], "media": nil}
		for _, p := range playable {
			if p == title {
				fields["media"] = map[string]any{"url": "https://cdn.example.com/" + title + ".mp3", "size": "1000", "mime_type": "audio/mpeg", "duration": int64(0)}
			}
		}
		items = append(items, map[string]any{"episode": c.itemRef(title), "fields": fullItem(fields)})
		targets = append(targets, map[string]any{"entity": "episode", "id": c.refs[title]["episode_id"]})
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].(map[string]any)["id"].(string) < targets[j].(map[string]any)["id"].(string)
	})
	targets = append(targets, map[string]any{"entity": "podcast", "id": c.p})
	payload := map[string]any{"podcast_id": c.p, "batch_id": newID(), "parser_version": "test/1", "fetch_time": c.clock, "items": items}
	b, _ := contract.Canonical(payload)
	v, _ := contract.DecodeJSON(b)
	deps := []string{}
	if d.last != "" {
		deps = append(deps, d.last)
	}
	return c.fact(d, "catalog.observe", deps, targets, v.(map[string]any))
}

// propose builds a discovery.propose fact.
func (c *catalogEnv) propose(d *device, title, action string, pub, newest any) map[string]any {
	return c.lfact(d, "discovery.propose", c.refs[title], map[string]any{"podcast_id": c.p, "generation_id": c.g, "batch_id": newID(),
		"pub_date": pub, "newest_before_batch": newest, "action": action, "policy_node": nil})
}

func (c *catalogEnv) record(title, group string) string {
	return c.records()["episode/"+c.refs[title]["episode_id"].(string)+"/"+group]
}

// A device's eligible proposal is admitted once: its Inbox filing applies at
// its place; later or ineligible proposals change nothing (PROTOCOL §9.4).
func TestDeviceProposalAdmission(t *testing.T) {
	c := newCatalogEnv(t, "E", "X", "Old")
	a, b := c.device("A"), c.device("B")
	day := int64(86_400_000)
	c.pushOK(t, a, c.observe(a, map[string]int64{"E": c.clock - day, "X": c.clock - day, "Old": c.clock - 30*day}, "E", "Old"), "applied")
	c.clock += minute
	c.pushOK(t, a, c.propose(a, "X", "inbox", c.clock-day, c.clock-2*day), "not_applied/not_playable")
	c.pushOK(t, a, c.propose(a, "Old", "inbox", c.clock-30*day, c.clock-2*day), "not_applied/ineligible")
	c.pushOK(t, a, c.propose(a, "E", "inbox", c.clock-day, c.clock-2*day), "applied")
	c.pushOK(t, b, c.propose(b, "E", "inbox", nil, nil), "not_applied/already_admitted")
	if got := c.record("E", "play_state"); !strings.Contains(got, `"play_state":"new"`) {
		t.Errorf("play state %s", got)
	}
	if got := c.record("E", "admission"); !strings.Contains(got, `"action":"inbox"`) || !strings.Contains(got, `"source":"`+a.inst.InstallID) {
		t.Errorf("admission %s", got)
	}
	if got := c.record("E", "metadata"); !strings.Contains(got, `"title":"E"`) {
		t.Errorf("metadata %s", got)
	}
}

// An admission files nothing where a barrier exists (an explicit play state).
func TestAdmissionRespectsBarriers(t *testing.T) {
	c := newCatalogEnv(t, "E")
	a := c.device("A")
	c.pushOK(t, a, c.observe(a, map[string]int64{"E": c.clock}, "E"), "applied")
	c.pushOK(t, a, c.lfact(a, "episode.mark_played", c.refs["E"], map[string]any{"reset_position": false}), "applied")
	c.clock += minute
	c.pushOK(t, a, c.propose(a, "E", "inbox", c.clock, c.clock), "applied")
	if got := c.record("E", "play_state"); !strings.Contains(got, `"play_state":"played"`) {
		t.Errorf("an admission undid an explicit play state: %s", got)
	}
}

// A queue filing from a device without the queue capability is resolved by
// the server at the account's enqueue location, and sorted under keep-sorted.
func TestQueueFilingResolvedAndSorted(t *testing.T) {
	c := newCatalogEnv(t, "A", "B", "Z")
	d := c.device("D")
	c.seed(t, d, "B", "Z")
	c.pushOK(t, d, c.policy(d, map[string]any{"sorting": map[string]any{"keep_sorted": true, "sort_order": "episode_title_a_z"}}), "applied")
	w := c.device("web")
	c.setCapabilities(t, w, `["favorites","history","inbox","statistics"]`)
	c.clock += minute
	c.pushOK(t, w, c.observe(w, map[string]int64{"A": c.clock}, "A"), "applied")
	c.pushOK(t, w, c.propose(w, "A", "queue", c.clock, c.clock-minute), "applied")
	if got := c.order(t); got != "A,B,Z" {
		t.Errorf("queue %s, want A,B,Z", got)
	}
	if got := c.record("A", "admission"); !strings.Contains(got, `"action":"queue"`) || !strings.Contains(got, `"key"`) && !strings.Contains(got, `"admission_id"`) {
		t.Errorf("admission %s", got)
	}
}

// serverFeed is an RSS feed of the named items: title → (pub date, playable).
func serverFeed(t *testing.T, now time.Time, items ...feedItem) *feed.Feed {
	t.Helper()
	var b strings.Builder
	b.WriteString(`<rss version="2.0"><channel><title>Show</title>`)
	for _, it := range items {
		fmt.Fprintf(&b, `<item><title>%s</title><guid>guid-%s</guid><pubDate>%s</pubDate>`, it.title, it.title,
			time.UnixMilli(it.pub).UTC().Format(time.RFC1123Z))
		if it.playable {
			fmt.Fprintf(&b, `<enclosure url="https://cdn.example.com/%s.mp3" length="20000" type="audio/mpeg"/>`, it.title)
		}
		b.WriteString(`</item>`)
	}
	b.WriteString(`</channel></rss>`)
	f, err := feed.Parse(context.Background(), []byte(b.String()), "https://example.com/feed", now)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type feedItem struct {
	title    string
	pub      int64
	playable bool
}

// serverFetch applies a server fetch of f, capturing the generation, its
// refresh-URL write and the current revisions as the scheduler does.
func (c *catalogEnv) serverFetch(t *testing.T, f *feed.Feed, move string, aliases ...string) {
	t.Helper()
	x := ServerFetch{PodcastID: c.p, GenerationID: c.g, RefreshID: newID(), FetchTime: c.clock, Feed: f, Move: move, Aliases: aliases,
		Namespace: c.namespace, Bases: map[string]string{}}
	err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error {
		g, err := generationState(ctx, c.p, c.g)
		if err != nil {
			return err
		}
		x.RefreshBy = g.RefreshBy
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error { return ApplyServerFetch(ctx, x) }); err != nil {
		t.Fatal(err)
	}
}

// The server's first fetch of a generation is its first population: catalog
// records, no filing. A later release is admitted and filed.
func TestServerPopulationThenDiscovery(t *testing.T) {
	c := newCatalogEnv(t, "One", "Two", "Three", "Backfill")
	day := int64(86_400_000)
	now := time.UnixMilli(c.clock)
	c.serverFetch(t, serverFeed(t, now, feedItem{"One", c.clock - 3*day, true}, feedItem{"Two", c.clock - 2*day, true}), "")
	if got := c.record("One", "metadata"); !strings.Contains(got, `"title":"One"`) {
		t.Fatalf("metadata %s", got)
	}
	if got := c.record("Two", "admission"); got != "" {
		t.Errorf("first population filed: %s", got)
	}
	c.clock += day
	now = time.UnixMilli(c.clock)
	c.serverFetch(t, serverFeed(t, now, feedItem{"One", c.clock - 4*day, true}, feedItem{"Two", c.clock - 3*day, true},
		feedItem{"Three", c.clock - minute, true}, feedItem{"Backfill", c.clock - 30*day, true}), "")
	if got := c.record("Backfill", "admission"); got != "" {
		t.Errorf("an old item new to the server was filed: %s", got)
	}
	if got := c.record("Three", "admission"); !strings.Contains(got, `"source":"server"`) {
		t.Errorf("a release was not admitted: %s", got)
	}
	if got := c.record("Three", "play_state"); !strings.Contains(got, `"play_state":"new"`) {
		t.Errorf("play state %s", got)
	}
	if got := c.record("Two", "admission"); got != "" {
		t.Errorf("an old episode was filed: %s", got)
	}
}

// A device that observed a release and never proposed it cannot strand it:
// the server discovers it against its own fetch history.
func TestServerDiscoversWhatADeviceOnlyObserved(t *testing.T) {
	c := newCatalogEnv(t, "One", "New")
	a := c.device("A")
	day := int64(86_400_000)
	c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - day, true}), "")
	c.clock += minute
	c.pushOK(t, a, c.observe(a, map[string]int64{"New": c.clock - minute}, "New"), "applied")
	c.clock += minute
	c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - day, true}, feedItem{"New", c.clock - 2*minute, true}), "")
	if got := c.record("New", "admission"); !strings.Contains(got, `"source":"server"`) {
		t.Errorf("stranded release: %s", got)
	}
}

// A move changes the refresh URL only while the user's URL is the one the
// fetch started with; unclaimed redirect targets become aliases.
func TestServerMove(t *testing.T) {
	c := newCatalogEnv(t, "One")
	day := int64(86_400_000)
	f := serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - day, true})
	c.serverFetch(t, f, "https://new.example.com/feed", "https://new.example.com/feed", "https://mid.example.com/feed")
	id := c.records()["podcast/"+c.p+"/identity"]
	if !strings.Contains(id, `"refresh_url":"https://new.example.com/feed"`) || !strings.Contains(id, "mid.example.com") {
		t.Fatalf("identity after a move: %s", id)
	}
	// A user's URL edit made while a fetch ran wins over that fetch's move.
	a := c.device("A")
	var x ServerFetch
	x = ServerFetch{PodcastID: c.p, GenerationID: c.g, RefreshID: newID(), FetchTime: c.clock, Feed: f, Move: "https://other.example.com/feed",
		Namespace: c.namespace, Bases: map[string]string{}}
	c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error {
		g, err := generationState(ctx, c.p, c.g)
		x.RefreshBy = g.RefreshBy
		return err
	})
	c.clock += minute
	c.pushOK(t, a, c.fact(a, "podcast.change_url", nil, podTarget(c.p, c.g), map[string]any{"url": "https://user.example.com/feed"}), "applied")
	if err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error { return ApplyServerFetch(ctx, x) }); err != nil {
		t.Fatal(err)
	}
	if id := c.records()["podcast/"+c.p+"/identity"]; !strings.Contains(id, `"refresh_url":"https://user.example.com/feed"`) {
		t.Errorf("a stale move overrode the user's URL: %s", id)
	}
}

// The canonical replacement rule (PROTOCOL §9.4).
func TestCanonicalReplacementRule(t *testing.T) {
	obs := func(node string, rank int64, base string, fetch int64, title string) projection.Contribution {
		return projection.Contribution{Node: node, Rank: rank, Effect: projection.Effect{Kind: projection.KindObservation, RevisionBase: base,
			FetchTime: fetch, Observed: map[string]any{"title": title}}}
	}
	cases := []struct {
		name string
		cs   []projection.Contribution
		want string
	}{
		{"first", []projection.Contribution{obs("a", 1, "", 10, "A")}, "A"},
		{"after observing it", []projection.Contribution{obs("a", 1, "", 10, "A"), obs("b", 2, "a", 5, "B")}, "B"},
		{"concurrent, later fetch", []projection.Contribution{obs("a", 1, "", 10, "A"), obs("b", 2, "x", 11, "B")}, "B"},
		{"concurrent, earlier fetch", []projection.Contribution{obs("a", 1, "", 10, "A"), obs("b", 2, "x", 9, "B")}, "A"},
		{"re-parse against a newer correction", []projection.Contribution{obs("s", 1, "", 10, "S"), obs("p", 2, "s", 20, "P"),
			obs("r", 3, "s", 10, "S'")}, "P"},
	}
	for _, c := range cases {
		rec, _ := projection.FoldCatalog(c.cs)
		if rec.Value["title"] != c.want {
			t.Errorf("%s: %v, want %s", c.name, rec.Value["title"], c.want)
		}
	}
}

// Scenario 14: two phones and the server discover the same release while
// apart; it is filed once, as the first admission says, and an explicit action
// survives.
func TestScenario14FiledOnce(t *testing.T) {
	for _, serverFirst := range []bool{false, true} {
		c := newCatalogEnv(t, "One", "New")
		a, b := c.device("A"), c.device("B")
		day := int64(86_400_000)
		c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - day, true}), "")
		c.clock += minute
		release := c.clock - minute
		propose := func(d *device) {
			c.pushOK(t, d, c.observe(d, map[string]int64{"New": release}, "New"), "applied")
			c.push(d, c.propose(d, "New", "inbox", release, c.clock-day))
		}
		if serverFirst {
			c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - day, true}, feedItem{"New", release, true}), "")
		}
		propose(a)
		// B played it before its proposal reached the server: the explicit
		// state survives the filing.
		c.pushOK(t, b, c.lfact(b, "episode.mark_played", c.refs["New"], map[string]any{"reset_position": false}), "applied")
		propose(b)
		if !serverFirst {
			c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - day, true}, feedItem{"New", release, true}), "")
		}
		var admissions int
		c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error {
			id, err := ctx.Account.DecisionBySubject("discovery.admission", "admission:"+c.g+":"+c.refs["New"]["episode_id"].(string))
			if id != "" {
				admissions++
			}
			return err
		})
		if admissions != 1 {
			t.Errorf("server first %v: %d admissions", serverFirst, admissions)
		}
		if got := c.record("New", "play_state"); !strings.Contains(got, `"play_state":"played"`) {
			t.Errorf("server first %v: the explicit play state was lost: %s", serverFirst, got)
		}
	}
}

// Scenario 17b: a phone that can reach a feed the server cannot corrects an
// episode's title for every device; the server's failed fetches keep it.
func TestScenario17bPhoneCorrectsMetadata(t *testing.T) {
	c := newCatalogEnv(t, "One")
	a := c.device("A")
	day := int64(86_400_000)
	c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - day, true}), "")
	meta, _ := contract.DecodeJSON([]byte(c.record("One", "metadata")))
	rev := meta.(map[string]any)["revision"].(string)
	c.clock += minute
	payload := map[string]any{"podcast_id": c.p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": c.clock,
		"items": []any{map[string]any{"episode": c.itemRef("One"), "base": rev, "fields": fullItem(map[string]any{"title": "One (corrected)", "pub_date": c.clock - day,
			"media": map[string]any{"url": "https://cdn.example.com/One.mp3", "size": "20000", "mime_type": "audio/mpeg", "duration": int64(0)}})}}}
	b, _ := contract.Canonical(payload)
	v, _ := contract.DecodeJSON(b)
	targets := []any{map[string]any{"entity": "episode", "id": c.refs["One"]["episode_id"]}, map[string]any{"entity": "podcast", "id": c.p}}
	c.pushOK(t, a, c.fact(a, "catalog.observe", nil, targets, v.(map[string]any)), "applied")
	c.clock += minute
	c.serverFetch(t, nil, "") // the server cannot reach the publisher
	if got := c.record("One", "metadata"); !strings.Contains(got, `"title":"One (corrected)"`) {
		t.Errorf("the correction did not stand: %s", got)
	}
}
