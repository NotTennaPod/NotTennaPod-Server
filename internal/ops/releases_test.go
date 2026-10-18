package ops

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
)

// Releases: enclosures, first fetches, imported state, disputes, merges and
// rotation. The differential cases are rule vectors in testdata/releases.

// A newly observed signed enclosure URL of an existing release is added
// to that release (an enclosure addition decision), so an undated reference
// by that URL routes to it and identity_rules lists it.
func TestSignedEnclosureJoinsItsRelease(t *testing.T) {
	c := newCatalogEnv(t)
	a := c.device("A")
	d1, d2 := c.clock-40*day, c.clock-day
	u1, u2, u3 := "https://cdn.example.com/one.mp3", "https://cdn.example.com/two.mp3", "https://cdn.example.com/two.mp3?sig=new"
	c.serverFetch(t, idFeed(t, c.clock, idItem{"g", "One", u1, d1}), "")
	c.serverFetch(t, idFeed(t, c.clock, idItem{"g", "Two", u2, d2}), "")
	c.serverFetch(t, idFeed(t, c.clock, idItem{"g", "Two", u3, d2}), "")
	ref := c.idRef("g", u3, d2)
	delete(ref["occurrence"].(map[string]any), "publication_time")
	c.pushOK(t, a, c.lfact(a, "favorite.set", ref, map[string]any{"on": true}), "applied")
	later, _ := contract.EpisodeID(c.namespace, c.p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u2))
	if !strings.Contains(c.episodeRecord(later, "favorite"), `"on":true`) {
		t.Errorf("an undated reference by a known signed enclosure went to the wrong release: rules %s", c.records()["podcast/"+c.p+"/identity_rules"])
	}
	if rules := c.records()["podcast/"+c.p+"/identity_rules"]; !strings.Contains(rules, `"`+u2+`","`+u3+`"`) {
		t.Errorf("identity_rules does not list the addition after the release's enclosure: %s", rules)
	}
}

// A newest-first feed reusing a GUID keeps both releases on its first
// fetch: every item's evidence and dispute is settled before any item is
// derived.
func TestNewestFirstFirstFetch(t *testing.T) {
	c := newCatalogEnv(t)
	d1, d2 := c.clock-40*day, c.clock-day
	u1, u2 := "https://cdn.example.com/one.mp3", "https://cdn.example.com/two.mp3"
	c.serverFetch(t, idFeed(t, c.clock, idItem{"g", "Two", u2, d2}, idItem{"g", "One", u1, d1}), "")
	later, _ := contract.EpisodeID(c.namespace, c.p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u2))
	if !strings.Contains(c.episodeRecord(c.idOf("g"), "metadata"), `"title":"One"`) || !strings.Contains(c.episodeRecord(later, "metadata"), `"title":"Two"`) {
		t.Errorf("the first fetch lost or mislabeled a release: original %s, later %s", c.episodeRecord(c.idOf("g"), "metadata"),
			c.episodeRecord(later, "metadata"))
	}
}

// Imported state follows its release when the GUID is disputed later.
func TestImportedStateFollowsItsRelease(t *testing.T) {
	e := newEnv(t)
	d1, d2 := e.clock-40*day, e.clock-day
	h := e.clock - 1000
	u1, u2 := "https://cdn.example.com/one.mp3", "https://cdn.example.com/two.mp3"
	x := importPodcast(e.clock, importItem{"g", u2, d2, ImportRow{PlayState: "played", Favorite: true, Position: 123000, PlayedTime: 600000,
		History: &h, LastPlayed: h, QueueIndex: 0, QueueKey: "80", PositionID: ImportPositionID(testImportID, 0)}})
	p := e.applyImport(t, x)
	err := e.svc.ServerCommit(context.Background(), e.account, func(ctx *protocol.ApplyContext) error {
		rc, err := CaptureRefresh(ctx, p.PodcastID)
		if err != nil {
			return err
		}
		return ApplyServerFetch(ctx, ServerFetch{PodcastID: p.PodcastID, GenerationID: p.GenerationID, RefreshBy: rc.RefreshBy, RefreshID: newID(),
			FetchTime: e.clock + 1000, Namespace: e.namespace, Bases: rc.Bases, Feed: idFeed(t, e.clock, idItem{"g", "One", u1, d1})})
	})
	if err != nil {
		t.Fatal(err)
	}
	later, _ := contract.EpisodeID(e.namespace, p.PodcastID, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u2))
	rec := e.records()
	newKey := "episode/" + later + "/"
	for _, g := range []string{"favorite", "position", "history", "played_time", "queue_membership", "queue_position"} {
		if rec[newKey+g] == "" {
			t.Errorf("%s: the imported state stayed on the earlier release (later release: none, earlier: %s)", g, rec["episode/"+p.EpisodeOf[1]+"/"+g])
		}
	}
	if !strings.Contains(rec[newKey+"favorite"], `"on":true`) || !strings.Contains(rec[newKey+"position"], `123000`) {
		t.Errorf("favorite %s, position %s", rec[newKey+"favorite"], rec[newKey+"position"])
	}
}

// When a GUID's dispute is first detected late, the earlier
// observations are partitioned by release, so a later release seen before
// the dispute keeps its own episode and state.
func TestDelayedDisputePartitionsReleases(t *testing.T) {
	c := newCatalogEnv(t)
	a := c.device("A")
	d1, d2 := c.clock-40*day, c.clock-day
	u1, u2 := "https://cdn.example.com/latest.mp3", "https://cdn.example.com/latest.mp3?sig=old"
	for _, it := range []idItem{{"g", "One", u1, d1}, {"g", "Two", u1, d2}, {"g", "One", u2, d1}, {"g", "Two", u1, d2}} {
		c.clock += minute
		c.serverFetch(t, idFeed(t, c.clock, it), "")
	}
	c.pushOK(t, a, c.lfact(a, "favorite.set", c.idRef("g", u1, d2), map[string]any{"on": true}), "applied")
	later, _ := contract.EpisodeID(c.namespace, c.p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("g", u1))
	if !strings.Contains(c.episodeRecord(later, "favorite"), `"on":true`) {
		t.Errorf("the later release is missing from the resolver; rules %s", c.records()["podcast/"+c.p+"/identity_rules"])
	}
	if !strings.Contains(c.episodeRecord(later, "metadata"), `"title":"Two"`) {
		t.Errorf("the later release's metadata: %s", c.episodeRecord(later, "metadata"))
	}
}

// A podcast merge carries an episode with neither GUID nor enclosure
// (an occurrence ID) and its state to the survivor.
func TestMergeCarriesAnonymousEpisodes(t *testing.T) {
	c := newCatalogEnv(t)
	h := c.clock - 1000
	x := importPodcast(c.clock, importItem{"", "", c.clock - day, ImportRow{PlayState: "played", Favorite: true, Position: 123000, History: &h,
		LastPlayed: h, QueueIndex: 0, QueueKey: "80", PositionID: ImportPositionID(testImportID, 0)}})
	x.URL = "https://mirror.example.com/feed"
	x.Rows[0].Item.Occurrence = map[string]any{"k": "none", "v": "", "title": "Anonymous"}
	x.Rows[0].Item.Fields["title"] = "Anonymous"
	x.Rows[0].Item.Fields["media"] = nil
	p := c.applyImport(t, x)
	ep := p.EpisodeOf[1]
	visible := func() bool {
		found := false
		if err := c.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
			lib, err := reference.Load(r, c.account)
			if err == nil {
				found = lib.ByID[ep] != nil
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return found
	}
	if !visible() {
		t.Fatal("not visible before the merge")
	}
	c.clock += minute
	c.fetchFor(t, p.PodcastID, p.GenerationID, idFeed(t, c.clock), "https://example.com/feed")
	if !strings.Contains(c.records()["podcast/"+p.PodcastID+"/subscription"], `"merged"`) {
		t.Fatalf("no merge: %s", c.records()["podcast/"+p.PodcastID+"/subscription"])
	}
	if !visible() {
		t.Errorf("the merge hid the anonymous episode: identity %s", c.episodeRecord(ep, "identity"))
	}
	if !strings.Contains(c.episodeRecord(ep, "favorite"), `"on":true`) {
		t.Errorf("its favorite: %s", c.episodeRecord(ep, "favorite"))
	}
}

// A whole-feed GUID and enclosure change is placed and published once
// per fetch. NTP_IDENTITY_PERF sets the episode count (default 300; the
// target is a 3,000-episode rotation in seconds).
func TestRotationPerformance(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("load test")
	}
	n := 300
	if s := os.Getenv("NTP_IDENTITY_PERF"); s != "" {
		fmt.Sscan(s, &n)
	}
	c := newCatalogEnv(t)
	items := make([]idItem, n)
	for i := range items {
		items[i] = idItem{fmt.Sprintf("old-%d", i), fmt.Sprintf("Episode %d", i), fmt.Sprintf("https://old.example.com/%d.mp3", i), c.clock - int64(n-i)*day}
	}
	start := time.Now()
	c.serverFetch(t, idFeed(t, c.clock, items...), "")
	t.Logf("n=%d first fetch %v", n, time.Since(start))
	for i := range items {
		items[i].guid = fmt.Sprintf("new-%d", i)
		items[i].enclosure = fmt.Sprintf("https://new.example.com/%d.mp3", i)
	}
	c.clock += minute
	start = time.Now()
	c.serverFetch(t, idFeed(t, c.clock, items...), "")
	took := time.Since(start)
	t.Logf("n=%d rotated fetch %v", n, took)
	if limit := time.Duration(n) * 20 * time.Millisecond; took > limit && took > 10*time.Second {
		t.Errorf("the rotation of %d episodes took %v", n, took)
	}
	if got := c.episodeRecord(c.idOf("old-0"), "metadata"); !strings.Contains(got, `new.example.com/0.mp3`) {
		t.Errorf("the rotated episode's record: %s", got)
	}
}
