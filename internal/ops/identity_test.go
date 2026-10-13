package ops

import (
	"context"

	"fmt"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

// PROTOCOL §9.7: identity reconciliation.

type idItem struct {
	guid, title, enclosure string
	pub                    int64
}

const day = int64(86_400_000)

func idFeed(t *testing.T, now int64, items ...idItem) *feed.Feed {
	t.Helper()
	var b strings.Builder
	b.WriteString(`<rss version="2.0"><channel><title>Show</title>`)
	for _, it := range items {
		fmt.Fprintf(&b, `<item><title>%s</title><guid>%s</guid><pubDate>%s</pubDate><enclosure url="%s" length="20000" type="audio/mpeg"/></item>`,
			it.title, it.guid, time.UnixMilli(it.pub).UTC().Format(time.RFC1123Z), it.enclosure)
	}
	b.WriteString(`</channel></rss>`)
	f, err := feed.Parse(context.Background(), []byte(b.String()), "https://example.com/feed", time.UnixMilli(now))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// ref is an episode reference for a GUID with an occurrence's evidence.
func (c *catalogEnv) idRef(guid, enclosure string, pub int64) map[string]any {
	id, _ := contract.EpisodeID(c.namespace, c.p, contract.KindRSSGUID, guid)
	return map[string]any{"episode_id": id, "podcast_id": c.p, "generation_id": c.g,
		"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": guid, "guid": guid, "enclosure_url": enclosure, "publication_time": pub}}
}

func (c *catalogEnv) episodeRecord(episode, group string) string {
	return c.records()["episode/"+episode+"/"+group]
}

func (c *catalogEnv) idOf(guid string) string {
	id, _ := contract.EpisodeID(c.namespace, c.p, contract.KindRSSGUID, guid)
	return id
}

// Scenario 17: a bulletin feed reusing /latest.mp3 with new GUIDs and the same
// title every few hours gives a new episode each time; its enclosure becomes
// disputed and nothing inherits the previous one's played state.
func TestScenario17Bulletin(t *testing.T) {
	c := newCatalogEnv(t)
	a := c.device("A")
	t0 := c.clock - day
	c.serverFetch(t, idFeed(t, c.clock, idItem{"bulletin-1", "News", "https://cdn.example.com/latest.mp3", t0}), "")
	first := c.idOf("bulletin-1")
	ref := c.idRef("bulletin-1", "https://cdn.example.com/latest.mp3", t0)
	c.pushOK(t, a, c.lfact(a, "episode.mark_played", ref, map[string]any{"reset_position": false}), "applied")
	c.clock += 6 * 3600 * 1000
	c.serverFetch(t, idFeed(t, c.clock, idItem{"bulletin-2", "News", "https://cdn.example.com/latest.mp3", t0 + 6*3600*1000}), "")
	second := c.idOf("bulletin-2")
	if got := c.episodeRecord(second, "metadata"); !strings.Contains(got, `"title":"News"`) {
		t.Fatalf("the second bulletin is no episode of its own: %s", got)
	}
	if got := c.episodeRecord(second, "play_state"); strings.Contains(got, "played") {
		t.Errorf("the second bulletin inherited played state: %s", got)
	}
	if got := c.episodeRecord(first, "play_state"); !strings.Contains(got, `"play_state":"played"`) {
		t.Errorf("the first bulletin lost its state: %s", got)
	}
	if got := c.records()["podcast/"+c.p+"/identity_rules"]; !strings.Contains(got, "latest.mp3") {
		t.Errorf("the reused enclosure is not disputed: %s", got)
	}
}

// Scenarios 18 and 46: a hosting move changing every GUID and enclosure keeps
// identity where titles and dates match one to one, whether the server or a
// phone saw the new GUIDs first; competing candidates merge nothing.
func TestScenario18HostingMove(t *testing.T) {
	for _, phoneFirst := range []bool{false, true} {
		c := newCatalogEnv(t)
		a := c.device("A")
		d1, d2 := c.clock-3*day, c.clock-2*day
		c.serverFetch(t, idFeed(t, c.clock,
			idItem{"old-a", "Episode A", "https://old.example.com/a.mp3", d1},
			idItem{"old-b", "Episode B", "https://old.example.com/b.mp3", d2},
			idItem{"old-c", "Twin", "https://old.example.com/c1.mp3", d2 - 1000},
			idItem{"old-d", "Twin", "https://old.example.com/c2.mp3", d2 - 1000}), "")
		oldA := c.idOf("old-a")
		c.pushOK(t, a, c.lfact(a, "episode.mark_played", c.idRef("old-a", "https://old.example.com/a.mp3", d1), map[string]any{"reset_position": false}), "applied")
		c.clock += day
		moved := idFeed(t, c.clock,
			idItem{"new-a", "Episode A", "https://new.example.com/a.mp3", d1},
			idItem{"new-b", "Episode B", "https://new.example.com/b.mp3", d2},
			idItem{"new-c", "Twin", "https://new.example.com/c1.mp3", d2 - 1000},
			idItem{"new-d", "Twin", "https://new.example.com/c2.mp3", d2 - 1000})
		newA := c.idOf("new-a")
		if phoneFirst {
			// A phone saw the moved feed first and favorited A under its new ID.
			obsRef := c.idRef("new-a", "https://new.example.com/a.mp3", d1)
			payload := map[string]any{"podcast_id": c.p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": c.clock,
				"items": []any{map[string]any{"episode": obsRef, "fields": fullItem(map[string]any{"title": "Episode A", "pub_date": d1,
					"media": map[string]any{"url": "https://new.example.com/a.mp3", "size": "20000", "mime_type": "audio/mpeg", "duration": int64(0)}})}}}
			b, _ := contract.Canonical(payload)
			v, _ := contract.DecodeJSON(b)
			targets := []any{map[string]any{"entity": "episode", "id": newA}, map[string]any{"entity": "podcast", "id": c.p}}
			c.pushOK(t, a, c.fact(a, "catalog.observe", nil, targets, v.(map[string]any)), "applied")
			c.pushOK(t, a, c.lfact(a, "favorite.set", c.idRef("new-a", "https://new.example.com/a.mp3", d1), map[string]any{"on": true}), "applied")
		}
		c.serverFetch(t, moved, "")
		if got := c.episodeRecord(oldA, "play_state"); !strings.Contains(got, `"play_state":"played"`) {
			t.Errorf("phone first %v: the moved episode lost its state: %s", phoneFirst, got)
		}
		if got := c.episodeRecord(oldA, "metadata"); !strings.Contains(got, "new.example.com/a.mp3") {
			t.Errorf("phone first %v: the moved episode's enclosure is not updated: %s", phoneFirst, got)
		}
		if phoneFirst {
			if got := c.episodeRecord(oldA, "favorite"); !strings.Contains(got, `"on":true`) {
				t.Errorf("the phone's favorite under the new ID did not follow the merge: %s", got)
			}
			if got := c.episodeRecord(newA, "identity"); !strings.Contains(got, `"merged_into":"`+oldA+`"`) {
				t.Errorf("the new ID does not name its survivor: %s", got)
			}
		} else if got := c.episodeRecord(newA, "metadata"); got != "" {
			t.Errorf("the moved episode got a second ID: %s", got)
		}
		// The two "Twin" episodes compete: nothing merges.
		if got := c.episodeRecord(c.idOf("new-c"), "metadata"); got == "" {
			t.Errorf("phone first %v: competing candidates were merged", phoneFirst)
		}
	}
}

// Scenario 34 (occurrences): two releases of one reused GUID, met offline,
// keep their own state; signed URLs of one release (same date) stay one.
func TestScenario34ReusedGUID(t *testing.T) {
	c := newCatalogEnv(t)
	a, b := c.device("A"), c.device("B")
	d1 := c.clock - 40*day
	d2 := c.clock - day
	c.serverFetch(t, idFeed(t, c.clock, idItem{"weekly", "Weekly", "https://cdn.example.com/w1.mp3", d1}), "")
	orig := c.idOf("weekly")
	c.pushOK(t, a, c.lfact(a, "episode.mark_played", c.idRef("weekly", "https://cdn.example.com/w1.mp3", d1), map[string]any{"reset_position": false}), "applied")
	// B, offline, met the newer release under the same GUID and favorited it.
	c.pushOK(t, b, c.lfact(b, "favorite.set", c.idRef("weekly", "https://cdn.example.com/w2.mp3", d2), map[string]any{"on": true}), "applied")
	// The server sees the newer release: the GUID becomes disputed.
	c.clock += minute
	c.serverFetch(t, idFeed(t, c.clock, idItem{"weekly", "Weekly", "https://cdn.example.com/w2.mp3", d2}), "")
	newer, _ := contract.EpisodeID(c.namespace, c.p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("weekly", "https://cdn.example.com/w2.mp3"))
	if got := c.episodeRecord(orig, "play_state"); !strings.Contains(got, `"play_state":"played"`) {
		t.Errorf("the first release lost its played mark: %s", got)
	}
	if got := c.episodeRecord(orig, "favorite"); strings.Contains(got, `"on":true`) {
		t.Errorf("the newer release's favorite stayed on the first: %s", got)
	}
	if got := c.episodeRecord(newer, "favorite"); !strings.Contains(got, `"on":true`) {
		t.Errorf("the newer release lost its favorite: %s", got)
	}
	if got := c.episodeRecord(newer, "play_state"); strings.Contains(got, "played") {
		t.Errorf("the newer release inherited played: %s", got)
	}
	// A late fact naming the original ID with the newer release's evidence
	// goes to the newer release.
	c.pushOK(t, a, c.lfact(a, "episode.mark_played", c.idRef("weekly", "https://cdn.example.com/w2.mp3", d2), map[string]any{"reset_position": false}), "applied")
	if got := c.episodeRecord(newer, "play_state"); !strings.Contains(got, `"play_state":"played"`) {
		t.Errorf("a late fact was not routed to its release: %s", got)
	}
	// Signed URLs of one release, same date: one episode, no new release.
	c.clock += minute
	c.serverFetch(t, idFeed(t, c.clock, idItem{"weekly", "Weekly", "https://cdn.example.com/w2.mp3?sig=abc", d2}), "")
	signed, _ := contract.EpisodeID(c.namespace, c.p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("weekly", "https://cdn.example.com/w2.mp3?sig=abc"))
	if got := c.episodeRecord(signed, "metadata"); got != "" {
		t.Errorf("a signed URL of one release became another episode: %s", got)
	}
}

// A merge during an ongoing session: the next checkpoint, naming either ID,
// continues the session under the survivor (PROTOCOL §9.7).
func TestMergeDuringSession(t *testing.T) {
	for _, nameOld := range []bool{true, false} {
		c := newCatalogEnv(t)
		a := c.device("A")
		d1 := c.clock - 3*day
		c.serverFetch(t, idFeed(t, c.clock, idItem{"old-a", "Episode A", "https://old.example.com/a.mp3", d1}), "")
		oldA := c.idOf("old-a")
		// A phone already on the new feed starts listening under the new ID.
		newRef := c.idRef("new-a", "https://new.example.com/a.mp3", d1)
		obs := map[string]any{"podcast_id": c.p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": c.clock,
			"items": []any{map[string]any{"episode": newRef,
				"fields": fullItem(map[string]any{"title": "Episode A", "pub_date": d1,
					"media": map[string]any{"url": "https://new.example.com/a.mp3", "size": "20000", "mime_type": "audio/mpeg", "duration": int64(0)}})}}}
		bb, _ := contract.Canonical(obs)
		v, _ := contract.DecodeJSON(bb)
		targets := []any{map[string]any{"entity": "episode", "id": newRef["episode_id"]}, map[string]any{"entity": "podcast", "id": c.p}}
		c.pushOK(t, a, c.fact(a, "catalog.observe", nil, targets, v.(map[string]any)), "applied")
		s := newID()
		c.pushOK(t, a, c.lfact(a, "session.start", newRef, start(s, 0, true)), "applied")
		c.clock += minute
		c.serverFetch(t, idFeed(t, c.clock, idItem{"new-a", "Episode A", "https://new.example.com/a.mp3", d1}), "")
		ref := newRef
		if !nameOld {
			ref = c.idRef("old-a", "https://old.example.com/a.mp3", d1)
		}
		c.pushOK(t, a, c.lfact(a, "session.checkpoint", ref, checkpoint(s, 1, 120000, 120000)), "applied")
		if got := c.episodeRecord(oldA, "position"); !strings.Contains(got, `"position":120000`) {
			t.Errorf("naming the old ID %v: the session did not continue on the survivor: %s", !nameOld, got)
		}
	}
}

// fetchFor applies a server fetch of another podcast, capturing as serverFetch.
func (c *catalogEnv) fetchFor(t *testing.T, podcast, generation string, f *feed.Feed, aliases ...string) {
	t.Helper()
	x := ServerFetch{PodcastID: podcast, GenerationID: generation, RefreshID: newID(), FetchTime: c.clock, Feed: f, Aliases: aliases,
		Namespace: c.namespace, Bases: map[string]string{}}
	if err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error {
		rc, err := CaptureRefresh(ctx, podcast)
		x.RefreshBy, x.Bases = rc.RefreshBy, rc.Bases
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error { return ApplyServerFetch(ctx, x) }); err != nil {
		t.Fatal(err)
	}
}

// Podcast merges (PROTOCOL §9.7): a permanent redirect from one podcast's feed to the
// other's URL merges the later-created into the earlier, moving listening
// state to the survivor's episodes.
func TestPodcastMerge(t *testing.T) {
	for _, survivorRemoved := range []bool{false, true} {
		c := newCatalogEnv(t)
		a := c.device("A")
		d1 := c.clock - day
		c.serverFetch(t, idFeed(t, c.clock, idItem{"ep", "Episode", "https://cdn.example.com/ep.mp3", d1}), "")
		if survivorRemoved {
			c.pushOK(t, a, c.fact(a, "podcast.unsubscribe", nil, podTarget(c.p, c.g), map[string]any{}), "applied")
		}
		// The same feed subscribed again at a mirror URL: a second podcast.
		c.clock += minute
		sub := c.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://mirror.example.com/feed"})
		c.pushOK(t, a, sub, "applied")
		p2 := contract.PodcastID(c.namespace, sub["op_id"].(string))
		g2 := contract.GenerationID(c.namespace, p2, sub["op_id"].(string))
		c.fetchFor(t, p2, g2, idFeed(t, c.clock, idItem{"ep", "Episode", "https://cdn.example.com/ep.mp3", d1}))
		ref2 := c.episodeRef(p2, g2, "ep")
		ref2["occurrence"].(map[string]any)["title"] = "Episode"
		c.pushOK(t, a, c.lfact(a, "favorite.set", ref2, map[string]any{"on": true}), "applied")
		// The mirror now redirects permanently to the original URL.
		c.clock += minute
		c.fetchFor(t, p2, g2, idFeed(t, c.clock, idItem{"ep", "Episode", "https://cdn.example.com/ep.mp3", d1}), "https://example.com/feed")
		e1 := c.idOf("ep")
		if got := c.episodeRecord(e1, "favorite"); !strings.Contains(got, `"on":true`) {
			t.Errorf("survivor removed %v: the favorite did not follow the merge: %s", survivorRemoved, got)
		}
		if got := c.records()["podcast/"+p2+"/subscription"]; !strings.Contains(got, `"state":"merged"`) || !strings.Contains(got, c.p) {
			t.Errorf("survivor removed %v: the source is not merged: %s", survivorRemoved, got)
		}
		sub1 := c.records()["podcast/"+c.p+"/subscription"]
		if !strings.Contains(sub1, `"state":"subscribed"`) {
			t.Errorf("survivor removed %v: survivor subscription %s", survivorRemoved, sub1)
		}
		if survivorRemoved {
			if !strings.Contains(sub1, g2) {
				t.Errorf("the source's live generation did not become the survivor's: %s", sub1)
			}
			// A late fact from the survivor's closed generation is refused.
			old := c.idRef("ep", "https://cdn.example.com/ep.mp3", d1)
			c.pushOK(t, a, c.lfact(a, "favorite.set", old, map[string]any{"on": false}), "not_applied/generation_closed")
		}
		// A late fact naming the source's generation follows the merge.
		c.pushOK(t, a, c.lfact(a, "episode.mark_played", ref2, map[string]any{"reset_position": false}), "applied")
		if got := c.episodeRecord(e1, "play_state"); !strings.Contains(got, `"play_state":"played"`) {
			t.Errorf("survivor removed %v: a late fact did not follow the merge: %s", survivorRemoved, got)
		}
	}
}
