package views_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// The oracle is Android itself: an Android 3.12.1 database (made on the
// emulator, then given random library states) is queried with Android's own
// SQL (FeedItemFilterQuery and FeedItemSortQuery, verbatim), and imported
// into an account (PROTOCOL §11.5), whose views must list the same episodes in
// the same order. Items Android orders as ties may come in either order; the
// sequence of sort values must be Android's.

const fixtures = "../importer/androiddb/testdata/android-3.12.1/"

type androidFilter struct {
	name  string
	props []string
}

var androidFilters = []androidFilter{
	{"none", nil},
	{"played", []string{"played"}},
	{"unplayed", []string{"unplayed"}},
	{"new", []string{"new"}},
	{"paused", []string{"paused"}},
	{"not_paused", []string{"not_paused"}},
	{"queued", []string{"queued"}},
	{"not_queued", []string{"not_queued"}},
	{"has_media", []string{"has_media"}},
	{"no_media", []string{"no_media"}},
	{"favorite", []string{"is_favorite"}},
	{"not_favorite", []string{"not_favorite"}},
	{"history", []string{"is_in_history", "include_subscribed", "include_archived", "include_not_subscribed"}},
	{"archived", []string{"include_archived"}},
	{"not_subscribed", []string{"include_not_subscribed"}},
	{"all_states", []string{"include_subscribed", "include_archived", "include_not_subscribed"}},
	{"unplayed_paused_queued", []string{"unplayed", "paused", "queued"}},
	{"played_unplayed", []string{"played", "unplayed"}},
	{"favorite_all", []string{"is_favorite", "include_subscribed", "include_archived", "include_not_subscribed"}},
}

var androidSorts = map[string]string{
	"date_new_old":            "FeedItems.pubDate DESC",
	"date_old_new":            "FeedItems.pubDate ASC",
	"episode_title_a_z":       "FeedItems.title ASC",
	"episode_title_z_a":       "FeedItems.title DESC",
	"duration_short_long":     "FeedMedia.duration ASC",
	"duration_long_short":     "FeedMedia.duration DESC",
	"size_small_large":        "FeedMedia.filesize ASC",
	"size_large_small":        "FeedMedia.filesize DESC",
	"completion_date_new_old": "FeedMedia.playback_completion_date DESC",
	"episode_filename_a_z":    "link ASC",
	"episode_filename_z_a":    "link DESC",
}

// androidWhere is FeedItemFilterQuery.generateFrom.
func androidWhere(props []string) string {
	has := func(p string) bool {
		for _, x := range props {
			if x == p {
				return true
			}
		}
		return false
	}
	var st []string
	switch {
	case has("played"):
		st = append(st, "FeedItems.read = 1 ")
	case has("unplayed"):
		st = append(st, " NOT FeedItems.read = 1 ")
	case has("new"):
		st = append(st, "FeedItems.read = -1 ")
	}
	switch {
	case has("paused"):
		st = append(st, " (FeedMedia.position NOT NULL AND FeedMedia.position > 0 ) ")
	case has("not_paused"):
		st = append(st, " (FeedMedia.position IS NULL OR FeedMedia.position = 0 ) ")
	}
	switch {
	case has("queued"):
		st = append(st, "FeedItems.id IN (SELECT feeditem FROM Queue) ")
	case has("not_queued"):
		st = append(st, "FeedItems.id NOT IN (SELECT feeditem FROM Queue) ")
	}
	switch {
	case has("has_media"):
		st = append(st, "FeedMedia.id NOT NULL ")
	case has("no_media"):
		st = append(st, "FeedMedia.id IS NULL ")
	}
	switch {
	case has("is_favorite"):
		st = append(st, "FeedItems.id IN (SELECT feeditem FROM Favorites) ")
	case has("not_favorite"):
		st = append(st, "FeedItems.id NOT IN (SELECT feeditem FROM Favorites) ")
	}
	if has("is_in_history") {
		st = append(st, "FeedMedia.playback_completion_date > 0 ")
	}
	sub, arch, not := has("include_subscribed"), has("include_archived"), has("include_not_subscribed")
	if !(sub && arch && not) {
		var states []string
		if sub {
			states = append(states, "0")
		}
		if arch {
			states = append(states, "2")
		}
		if not {
			states = append(states, "1")
		}
		if len(states) == 0 {
			states = []string{"0"}
		}
		st = append(st, "FeedItems.feed IN (SELECT id FROM Feeds WHERE state IN ("+strings.Join(states, ",")+"))")
	}
	if len(st) == 0 {
		return ""
	}
	return " WHERE  (" + strings.Join(st, " AND ") + ") "
}

func copyFile(t *testing.T, from, to string) {
	in, err := os.Open(from)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

// mutate gives the library random states Android allows: play states,
// positions, favorites, a queue (never of NEW episodes: queueing clears NEW),
// history and statistics, feed states, and colliding titles, dates,
// durations, sizes and links for ties.
// mutateNow is the time the mutated libraries' dates count back from.
var mutateNow = viewstest.FixedNow

func mutate(t *testing.T, db *sql.DB, r *rand.Rand) {
	exec := func(q string, args ...any) {
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	ids := func(q string) []int64 {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		return out
	}
	titles := []string{"alpha", "Alpha", "beta", "Émile", "zeta", "Zeta", "éclair", "10 things", "2 things", "alpha"}
	links := []any{nil, "https://example.com/a", "https://example.com/B", "https://example.com/b", nil}
	base := mutateNow // fixed, so that a seed replays exactly
	days := func(n int) int64 { return base - int64(n)*24*3600*1000 }
	for _, f := range ids("SELECT id FROM Feeds WHERE download_url NOT LIKE 'antennapod_local:%'") {
		exec("UPDATE Feeds SET state = ? WHERE id = ?", []int{0, 0, 0, 1, 2}[r.IntN(5)], f)
	}
	for _, it := range ids("SELECT id FROM FeedItems") {
		exec("UPDATE FeedItems SET read = ?, title = ?, link = ?, pubDate = ? WHERE id = ?",
			[]int{-1, 0, 0, 1}[r.IntN(4)], titles[r.IntN(len(titles))], links[r.IntN(len(links))], days(r.IntN(40)*10), it)
	}
	for _, m := range ids("SELECT id FROM FeedMedia") {
		duration := int64(r.IntN(6)) * 600_000
		position := int64(0)
		if r.IntN(3) == 0 && duration > 0 {
			position = 1000 + r.Int64N(duration-1000)
		}
		history, last, played := int64(0), int64(0), int64(0)
		if r.IntN(3) == 0 {
			history = days(r.IntN(300))
			last, played = history, int64(r.IntN(60))*60_000
		}
		exec(`UPDATE FeedMedia SET duration = ?, position = ?, filesize = ?, playback_completion_date = ?, last_played_time = ?, played_duration = ?
			WHERE id = ?`, duration, position, int64(r.IntN(4))*1_000_000, history, last, played, m)
	}
	exec("UPDATE FeedMedia SET position = 0 WHERE feeditem IN (SELECT id FROM FeedItems WHERE read = 1)")
	exec("DELETE FROM Favorites")
	exec("DELETE FROM Queue")
	items := ids("SELECT id FROM FeedItems")
	r.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
	for i, it := range items {
		if r.IntN(4) == 0 {
			exec("INSERT INTO Favorites (feeditem, feed) SELECT id, feed FROM FeedItems WHERE id = ?", it)
		}
		if i%3 == 0 {
			exec("UPDATE FeedItems SET read = 0 WHERE id = ? AND read = -1", it)
			exec("INSERT INTO Queue (feeditem, feed) SELECT id, feed FROM FeedItems WHERE id = ?", it)
		}
	}
}

type imported struct {
	lib       *views.Library
	episodeOf map[int64]string
	merged    map[string]bool
}

func importLibrary(t *testing.T, path string) imported {
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
	var account string
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewRegistry()
	ops.Register(reg)
	svc := &protocol.Service{Store: store, Registry: reg, Box: box, Now: time.Now}
	lib, err := androiddb.Read(ctx, path, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const importID = "6a1f6a0e-8a9b-4c55-9f6e-2b1d3c4e5f60"
	plan, err := importer.FromAndroid(lib, importID, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	pods, err := importer.Apply(ctx, svc, account, plan, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Complete(ctx, svc, account, importID, "", plan); err != nil {
		t.Fatal(err)
	}
	out := imported{episodeOf: map[int64]string{}, merged: map[string]bool{}}
	for _, p := range pods {
		for item, ep := range p.EpisodeOf {
			out.episodeOf[item] = ep
		}
		for _, group := range p.Merged {
			for _, item := range group {
				out.merged[p.EpisodeOf[item]] = true
			}
		}
	}
	out.lib = throughApply(t, store, account)
	return out
}

// throughApply returns the account's library as the apply makes it: every
// podcast and episode the account has is changed (a record's provenance)
// in one commit, and the version before it advanced by that commit; it
// equals a full build.
func throughApply(t *testing.T, store *storage.Store, account string) *views.Library {
	t.Helper()
	ctx := context.Background()
	var base *views.Version
	var rows []storage.RecordRow
	if err := store.Read(ctx, func(r *storage.ReadTx) error {
		var err error
		if base, err = views.BuildVersion(r.Account(account), account); err != nil {
			return err
		}
		rows, err = r.Account(account).ViewRecords()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(account)
		touched := map[string]bool{}
		for _, r := range rows {
			if k := r.Entity + r.EntityID; !touched[k] && r.Entity != "account" {
				touched[k] = true
				full, err := a.Record(r.Entity, r.EntityID, r.FieldGroup)
				if err != nil {
					return err
				}
				full.Provenance = `["touched"]`
				if err := a.PutRecord(full); err != nil {
					return err
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
		v, path, err := views.Advance(base, acct, account)
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
	return lib
}

func TestEpisodeListsMatchAndroid(t *testing.T) {
	for _, base := range []string{"emulator-parity-library.db", "authentication-preview-library.db"} {
		for seed := uint64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("%s/%d", base, seed), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "AntennaPodBackup.db")
				copyFile(t, fixtures+base, path)
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				mutate(t, db, rand.New(rand.NewPCG(seed, 9)))
				imp := importLibrary(t, path)
				checked := 0
				for _, f := range androidFilters {
					filter, err := views.ParseFilter(strings.Join(f.props, ","))
					if err != nil {
						t.Fatal(err)
					}
					for order, col := range androidSorts {
						q := "SELECT FeedItems.id, quote(" + strings.Fields(col)[0] + ") FROM FeedItems LEFT JOIN FeedMedia ON FeedItems.id = FeedMedia.feeditem " +
							androidWhere(f.props) + "ORDER BY " + col
						rows, err := db.Query(q)
						if err != nil {
							t.Fatalf("%s: %v", q, err)
						}
						keyOf := map[string]string{}
						var want []string
						for rows.Next() {
							var id int64
							var key string
							if err := rows.Scan(&id, &key); err != nil {
								t.Fatal(err)
							}
							ep, ok := imp.episodeOf[id]
							if !ok || imp.merged[ep] {
								continue // not imported (the import's skips) or merged with another row
							}
							keyOf[ep] = key
							want = append(want, ep)
						}
						rows.Close()
						var got []string
						for _, e := range allRows(imp.lib.Episodes(views.ListQuery{Filter: filter, Order: order})) {
							if !imp.merged[e.ID] {
								got = append(got, e.ID)
							}
						}
						if len(got) != len(want) {
							t.Fatalf("%s/%s: %d episodes, Android lists %d", f.name, order, len(got), len(want))
						}
						for i := range got {
							k, ok := keyOf[got[i]]
							if !ok || k != keyOf[want[i]] {
								t.Fatalf("%s/%s: position %d is %s (%q), Android has %s (%q)", f.name, order, i, got[i], k, want[i], keyOf[want[i]])
							}
						}
						checked += len(got)
					}
				}
				if checked == 0 {
					t.Fatal("nothing compared")
				}
			})
		}
	}
}

// androidStatistics is getFeedStatisticsCursor: per feed, as Android computes it.
func androidStatistics(t *testing.T, db *sql.DB, includeMarked bool, from, to int64) map[int64][5]int64 {
	wasStarted := "FeedMedia.playback_completion_date > 0 AND FeedMedia.played_duration > 0"
	playedTime := "FeedMedia.played_duration"
	if includeMarked {
		wasStarted = "(" + wasStarted + ") OR FeedItems.read=1 OR FeedMedia.position> 0"
		playedTime = "(CASE WHEN " + playedTime + " != 0 THEN " + playedTime + " ELSE (CASE WHEN FeedItems.read=1 THEN FeedMedia.duration ELSE 0 END) END)"
	}
	timeFilter := fmt.Sprintf("FeedMedia.last_played_time>=%d AND FeedMedia.last_played_time<%d", from, to)
	q := "SELECT Feeds.id, COUNT(*), " +
		"SUM(CASE WHEN (" + wasStarted + ") THEN 1 ELSE 0 END), " +
		"IFNULL(SUM(CASE WHEN (" + timeFilter + ") THEN (" + playedTime + ") ELSE 0 END), 0), " +
		"IFNULL(SUM(FeedMedia.duration), 0), " +
		"MIN(CASE WHEN FeedMedia.last_played_time > 0 THEN FeedMedia.last_played_time ELSE 9223372036854775807 END) " +
		"FROM FeedItems LEFT JOIN FeedMedia ON FeedItems.id = FeedMedia.feeditem INNER JOIN Feeds ON FeedItems.feed = Feeds.id " +
		"WHERE Feeds.state != 1 GROUP BY Feeds.id"
	rows, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int64][5]int64{}
	for rows.Next() {
		var id int64
		var v [5]int64
		if err := rows.Scan(&id, &v[0], &v[1], &v[2], &v[3], &v[4]); err != nil {
			t.Fatal(err)
		}
		out[id] = v
	}
	return out
}

func queryIDs(t *testing.T, db *sql.DB, q string, args ...any) []int64 {
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

// The queue, the navigation and podcast counters, the statistics and Home's
// continue-listening section, against Android's SQL.
func TestScreensMatchAndroid(t *testing.T) {
	recent := map[string]int64{} // the 100-day window's played time, by fixture
	for _, base := range []string{"emulator-parity-library.db", "authentication-preview-library.db"} {
		defer func() {
			if !t.Failed() && recent[base] == 0 {
				t.Errorf("%s: no seed played anything in the 100-day window: the window checks nothing", base)
			}
		}()
		for seed := uint64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("%s/%d", base, seed), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "AntennaPodBackup.db")
				copyFile(t, fixtures+base, path)
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				mutate(t, db, rand.New(rand.NewPCG(seed, 9)))
				imp := importLibrary(t, path)
				lib := imp.lib
				ep := func(items []int64) []string {
					var out []string
					for _, it := range items {
						if e, ok := imp.episodeOf[it]; ok {
							out = append(out, e)
						}
					}
					return out
				}
				// The queue, in Android's order.
				want := ep(queryIDs(t, db, "SELECT feeditem FROM Queue ORDER BY id"))
				var got []string
				for _, e := range allRows(lib.Queue()) {
					got = append(got, e.ID)
				}
				if strings.Join(got, ",") != strings.Join(want, ",") {
					t.Fatalf("queue\n got %v\nwant %v", got, want)
				}
				// The Inbox counter: NEW episodes of subscribed podcasts.
				inbox := len(ep(queryIDs(t, db, "SELECT FeedItems.id FROM FeedItems WHERE read = -1 AND feed IN (SELECT id FROM Feeds WHERE state = 0)")))
				if c := lib.Counters(); c.Inbox != inbox || c.Queue != len(want) {
					t.Fatalf("counters %+v, Android inbox %d queue %d", c, inbox, len(want))
				}
				// Podcasts that are one Android feed each.
				feedsOf := map[string]map[int64]bool{}
				for item, e := range imp.episodeOf {
					var feed int64
					if err := db.QueryRow("SELECT feed FROM FeedItems WHERE id = ?", item).Scan(&feed); err != nil {
						t.Fatal(err)
					}
					shown, _ := lib.Episode(e)
					p := shown.PodcastID
					if feedsOf[p] == nil {
						feedsOf[p] = map[int64]bool{}
					}
					feedsOf[p][feed] = true
				}
				feedOf := map[string]int64{}
				for p, set := range feedsOf {
					if len(set) == 1 {
						for f := range set {
							feedOf[p] = f
						}
					}
				}
				// Subscription counters (new and unplayed), subscribed podcasts only.
				for _, counter := range []string{"new", "unplayed"} {
					where := map[string]string{"new": "read = -1", "unplayed": "(read = -1 OR read = 0)"}[counter]
					for _, state := range []string{"subscribed", "archived"} {
						o, err := views.ParseSubscriptionOptions(state, "alphabetical", counter, "")
						if err != nil {
							t.Fatal(err)
						}
						for _, row := range lib.Subscriptions(o) {
							f, ok := feedOf[row.Podcast.ID]
							if !ok {
								continue
							}
							var n int
							if err := db.QueryRow("SELECT COUNT(*) FROM FeedItems WHERE feed = ? AND "+where+
								" AND feed IN (SELECT id FROM Feeds WHERE state = 0)", f).Scan(&n); err != nil {
								t.Fatal(err)
							}
							if row.Counter != n {
								t.Fatalf("%s %s counter of feed %d: %d, Android %d", state, counter, f, row.Counter, n)
							}
						}
					}
				}
				// Statistics per podcast, at the clock the library's dates
				// count from (mutate).
				now := mutateNow
				windows := [][2]int64{{0, 1<<63 - 1}, {now - 100*24*3600*1000, now}, {now - 3*365*24*3600*1000, 1<<63 - 1}}
				for _, marked := range []bool{false, true} {
					for _, w := range windows {
						if marked && w != windows[0] {
							continue // Android shows marked-as-played statistics for all time only
						}
						android := androidStatistics(t, db, marked, w[0], w[1])
						if w == windows[1] && !marked {
							for _, a := range android {
								recent[base] += a[2] // the 100-day window's played time
							}
						}
						st := lib.Statistics(w[0], w[1], marked, now)
						seen := 0
						for _, row := range st.Podcasts {
							f, ok := feedOf[row.Podcast.ID]
							if !ok {
								continue
							}
							a, ok := android[f]
							if !ok {
								t.Fatalf("feed %d has no Android statistics row", f)
							}
							played, started := row.Stats.PlayedMS, row.Stats.EpisodesStarted
							if marked {
								played, started = row.Stats.PlayedMarkedMS, row.Stats.EpisodesStartedMarked
							}
							oldest := row.OldestDate
							if oldest == 0 {
								oldest = 1<<63 - 1
							}
							if got := [5]int64{row.Stats.Episodes, started, played, row.Stats.TotalMS, oldest}; got != a {
								t.Fatalf("statistics of feed %d (marked %v, %v): got %v, Android %v", f, marked, w, got, a)
							}
							seen++
						}
						if seen == 0 && len(android) > 0 {
							t.Log("no single-feed podcast compared")
						}
					}
				}
				// Monthly played time over every podcast.
				rows, err := db.Query(`SELECT SUM(played_duration), strftime('%m', datetime(last_played_time/1000, 'unixepoch')),
					strftime('%Y', datetime(last_played_time/1000, 'unixepoch')) AS y FROM FeedMedia
					WHERE last_played_time > 0 AND played_duration > 0 GROUP BY y, 2 ORDER BY y, 2`)
				if err != nil {
					t.Fatal(err)
				}
				var months []string
				for rows.Next() {
					var total int64
					var m, y string
					if err := rows.Scan(&total, &m, &y); err != nil {
						t.Fatal(err)
					}
					months = append(months, fmt.Sprintf("%s-%s:%d", y, m, total))
				}
				rows.Close()
				var gotMonths []string
				for _, m := range lib.Statistics(0, 1<<63-1, false, now).Months {
					gotMonths = append(gotMonths, fmt.Sprintf("%04d-%02d:%d", m.Year, m.Month, m.PlayedMS))
				}
				if strings.Join(gotMonths, " ") != strings.Join(months, " ") {
					t.Fatalf("months\n got %v\nwant %v", gotMonths, months)
				}
				// Home: continue listening (getPausedQueueCursor).
				recent := fmt.Sprintf("FeedMedia.position >= 1000 OR FeedMedia.last_played_time >= %d", now-30000)
				want = ep(queryIDs(t, db, "SELECT FeedItems.id FROM Queue INNER JOIN FeedItems ON FeedItems.id = Queue.feeditem "+
					"LEFT JOIN FeedMedia ON FeedItems.id = FeedMedia.feeditem ORDER BY (CASE WHEN "+recent+
					" THEN FeedMedia.last_played_time ELSE 0 END) DESC, Queue.id LIMIT 8"))
				got = nil
				for _, e := range lib.Home(now, 1, "date_new_old", false).Continue {
					got = append(got, e.ID)
				}
				if strings.Join(got, ",") != strings.Join(want, ",") {
					t.Fatalf("continue listening\n got %v\nwant %v", got, want)
				}
			})
		}
	}
}
