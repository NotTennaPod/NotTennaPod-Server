package views_test

// The screens on hand-made libraries, read from versions the apply made:
// the subscription orders, Surprise and Home's classics window.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// tEp is a hand-made episode.
type tEp struct {
	read                                    int // -1 new, 0 unplayed, 1 played
	pubDate, lastPlayed, playedMS, position int64
	noMedia                                 bool
	queue                                   string // its queue key, "" when not queued
}

// tPod is a hand-made podcast; "" titles are null.
type tPod struct {
	id, title, state string
	eps              []tEp
}

func podcastID(id string) string        { return "p1:" + id }
func episodeID(id string, j int) string { return fmt.Sprintf("e1:%s-%02d", id, j) }

// handLibrary writes the podcasts, in creation order, into a new account
// and returns its library at that snapshot, applied to the empty
// account's version (and equal to a full build), with its store.
func handLibrary(t *testing.T, pods []tPod) (*views.Library, *storage.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	viewstest.Skip(store)
	var account string
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var empty *views.Version
	if err := store.Read(ctx, func(r *storage.ReadTx) error {
		empty, err = views.BuildVersion(r.Account(account), account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(account)
		put := func(entity, id, group, value string) error {
			return a.PutRecord(storage.RecordRow{Entity: entity, EntityID: id, FieldGroup: group, Value: value, Provenance: `["h"]`})
		}
		for i, p := range pods {
			pid, node := podcastID(p.id), fmt.Sprintf("0000%04x-0000-4000-8000-%012x", i, i)
			if err := a.InsertNode(node, "decision", int64(i+1), nil); err != nil {
				return err
			}
			if err := a.CreatePodcast(pid, node); err != nil {
				return err
			}
			title := "null"
			if p.title != "" {
				title = fmt.Sprintf("%q", p.title)
			}
			if err := put("podcast", pid, "subscription", fmt.Sprintf(`{"generation_id":"g1:%s","state":%q}`, p.id, p.state)); err != nil {
				return err
			}
			if err := put("podcast", pid, "metadata", fmt.Sprintf(`{"author":null,"description":null,"image_url":null,"link":null,"title":%s}`, title)); err != nil {
				return err
			}
			for j, e := range p.eps {
				eid := episodeID(p.id, j)
				media := `{"duration":1800000,"mime_type":"audio/mpeg","size":"1000","url":"https://example.invalid/e.mp3"}`
				if e.noMedia {
					media = "null"
				}
				recs := map[string]string{
					"identity": fmt.Sprintf(`{"occurrence":{"enclosure_url":"https://example.invalid/e.mp3","guid":%q,"k":"rss-guid","publication_time":%d,"title":"E","v":%q},"podcast_id":%q}`,
						eid, e.pubDate, eid, pid),
					"metadata":   fmt.Sprintf(`{"chapter_url":null,"description":null,"image_url":null,"link":null,"media":%s,"pub_date":%d,"title":"E %d","transcript_type":null,"transcript_url":null}`, media, e.pubDate, j),
					"play_state": fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, map[int]string{-1: "new", 0: "unplayed", 1: "played"}[e.read]),
				}
				if e.lastPlayed != 0 {
					recs["last_played"] = fmt.Sprintf(`{"time":%d}`, e.lastPlayed)
				}
				if e.playedMS != 0 {
					recs["played_time"] = fmt.Sprintf(`{"played_ms":%d}`, e.playedMS)
				}
				if e.position != 0 {
					recs["position"] = fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, e.position)
				}
				if e.queue != "" {
					recs["queue_membership"], recs["queue_position"] = `{"in_queue":true}`, fmt.Sprintf(`{"key":%q}`, e.queue)
				}
				if err := a.AddEpisode(eid, pid, `{}`, node); err != nil {
					return err
				}
				if err := a.PutCatalogItem(storage.CatalogItem{EpisodeID: eid, PodcastID: pid, FirstNode: "n", PubDate: e.pubDate}); err != nil {
					return err
				}
				for g, v := range recs {
					if err := put("episode", eid, g, v); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var lib *views.Library
	if err := store.Read(ctx, func(r *storage.ReadTx) error {
		acct := r.Account(account)
		v, path, err := views.Advance(empty, acct, account)
		if err != nil {
			return err
		}
		if path != views.PathApply {
			return fmt.Errorf("the library was a %s", path)
		}
		full, err := views.BuildVersion(acct, account)
		if err != nil {
			return err
		}
		if err := views.EqualVersions(v, full); err != nil {
			return err
		}
		head, err := acct.Head()
		lib = &views.Library{Head: head, V: v}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return lib, store, account
}

func eps(reads ...int) []tEp {
	var out []tEp
	for i, r := range reads {
		out = append(out, tEp{read: r, pubDate: int64(i)})
	}
	return out
}

// getNavDrawerData's orders over getFeedList's title order: counter (most
// first, ties by Java's compareToIgnoreCase), alphabetical (null titles last),
// most played, and most recent episode (ties keep the title order).
func TestSubscriptionOrders(t *testing.T) {
	arch := eps(-1, -1, -1)
	arch[2].pubDate = 99
	lib, _, _ := handLibrary(t, []tPod{
		{"A", "beta", "subscribed", eps(-1, -1, 1)},
		{"B", "Alpha", "subscribed", eps(-1, 0, 1, 1)},
		{"C", "alpha", "subscribed", eps(0)},
		{"D", "", "subscribed", nil},
		{"E", "archived", "archived", arch},
		// Its one episode is dated before 1970: its newest episode counts
		// as 0, as getNavDrawerData's MAX over the items (and no items)
		// ties with D's and C's.
		{"F", "aaa", "subscribed", []tEp{{read: 1, pubDate: -5}}},
	})
	order := func(rows []views.Subscription) string {
		s := ""
		for _, r := range rows {
			s += strings.TrimPrefix(r.Podcast.ID, "p1:")
		}
		return s
	}
	opts := func(o, counter, filter string) views.SubscriptionOptions {
		x, err := views.ParseSubscriptionOptions("", o, counter, filter)
		if err != nil {
			t.Fatal(err)
		}
		return x
	}
	for _, tc := range []struct {
		o, counter, filter, want string
	}{
		{"counter", "new", "", "ABDFC"},             // A 2 new, B 1; C, D and F 0, and "" sorts before "aaa" before "alpha"
		{"counter", "unplayed", "", "BACDF"},        // A and B 2 ("Alpha" before "beta"), C 1, D and F 0
		{"alphabetical", "new", "", "FBCAD"},        // aaa, Alpha = alpha (title order, then creation), beta, no title last
		{"most_played", "new", "", "BFADC"},         // B 2 played, F and A 1 ("aaa" before "beta"), then "" before "alpha"
		{"most_recent_episode", "new", "", "BADFC"}, // B newest (3), A (2); C, D and F tie at 0 in title order ("" first)
		{"counter", "new", "counter_greater_zero", "AB"},
	} {
		got := order(lib.Subscriptions(opts(tc.o, tc.counter, tc.filter)))
		if got != tc.want {
			t.Errorf("%s/%s/%s: %s, want %s", tc.o, tc.counter, tc.filter, got, tc.want)
		}
	}
	// Archived podcasts list apart, and count nothing (Android counts subscribed feeds only).
	rows := lib.Subscriptions(views.SubscriptionOptions{State: "archived", Order: "counter", Counter: "new"})
	if len(rows) != 1 || rows[0].Podcast.ID != podcastID("E") || rows[0].Counter != 0 {
		t.Fatalf("archived %+v", rows)
	}
	if _, err := views.ParseSubscriptionOptions("", "", "", "enabled_auto_download"); err == nil {
		t.Fatal("a device-only filter was accepted")
	}
}

const day = int64(24 * 3600 * 1000)

// Get surprised: one episode per subscribed podcast, new or unplayed, with
// media, from the last 712 days, never played or played in the last hour;
// eight at most; the same seed gives the same sample, the specification's
// (the oracle's naive scoring of every candidate).
func TestSurprise(t *testing.T) {
	now := viewstest.FixedNow
	var pods []tPod
	for i := range 12 {
		p := tPod{id: string(rune('A' + i)), title: "p", state: "subscribed", eps: eps(-1, 0, 1, 0, 0, 0)}
		if i == 11 {
			p.state = "archived"
		}
		for j := range p.eps {
			p.eps[j].pubDate = now - int64(j)*day
		}
		p.eps[3].lastPlayed = now - 2*3600_000 // played two hours ago
		p.eps[4].pubDate = now - 2*356*day - 1 // too old
		p.eps[5].lastPlayed = now - 600_000    // played ten minutes ago: still a candidate
		pods = append(pods, p)
	}
	lib, store, account := handLibrary(t, pods)
	var ref *reference.Library
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		ref, err = reference.Load(r, account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	seeds := map[string]bool{}
	for _, seed := range []int64{0, 1, 42, -7, 1 << 40} {
		a, b := lib.Surprise(now, seed), lib.Surprise(now, seed)
		want := ref.SurpriseF5(now, seed)
		if len(a) != 8 || len(want) != 8 {
			t.Fatalf("seed %d: %d episodes, the specification %d", seed, len(a), len(want))
		}
		seen, ids := map[string]bool{}, ""
		for i, e := range a {
			if e != b[i] {
				t.Fatal("the same seed gave another sample")
			}
			if e.ID != want[i].ID {
				t.Fatalf("seed %d: pick %d is %s, the specification's %s", seed, i, e.ID, want[i].ID)
			}
			p := lib.Podcast(e.PodcastID)
			if seen[e.PodcastID] || p.State != "subscribed" || e.Read == 1 || !(e.LastPlayed == 0 || e.LastPlayed > now-3600_000) || e.PubDate <= now-2*356*day {
				t.Fatalf("ineligible pick %+v", e)
			}
			seen[e.PodcastID] = true
			ids += e.ID
		}
		seeds[ids] = true
	}
	if len(seeds) < 2 {
		t.Fatal("every seed gave the same sample")
	}
}

// Check your classics counts the last three years, "include marked as played"
// too (Android's SubscriptionsSection passes the range with the setting).
func TestClassicsWindowWithMarkedPlayed(t *testing.T) {
	now := viewstest.FixedNow
	lib, _, _ := handLibrary(t, []tPod{
		{"O", "old", "subscribed", []tEp{{read: 0, lastPlayed: now - 4*365*day, playedMS: 10 * 3600_000}}},
		{"R", "recent", "subscribed", []tEp{{read: 0, lastPlayed: now - day, playedMS: 3600_000}}},
	})
	for _, marked := range []bool{false, true} {
		h := lib.Home(now, 1, "date_new_old", marked)
		if len(h.Classics) != 2 || h.Classics[0].ID != podcastID("R") {
			t.Errorf("marked %v: classics %v first", marked, h.Classics[0].ID)
		}
	}
	// The statistics screen keeps all time for marked-as-played.
	st := lib.Statistics(now-3*365*day, 1<<63-1, true, now)
	if st.Podcasts[0].Podcast.ID != podcastID("O") {
		t.Errorf("statistics with marked played: %s first", st.Podcasts[0].Podcast.ID)
	}
}

// allRows reads a whole list.
func allRows(l *views.EpisodeList) []*views.EpisodeItem {
	rows, _ := l.Read(l.At(0), l.Total())
	return rows
}
