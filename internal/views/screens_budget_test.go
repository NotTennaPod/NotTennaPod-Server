package views_test

// Per-request budgets of the screens and the Continue, sort, queue and
// Surprise witnesses: deterministic counters
// at the trees' loops (nodes visited), rows encoded and candidates looked
// at, at 5,000 and 50,400 episodes. A budget fails when a screen's work
// grows with the library, the queue, the list's depth or the history
// instead of with what it shows.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/bits"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

func measure(f func()) views.WorkCounts {
	before := views.Work()
	f()
	return views.Work().Minus(before)
}

func lg(n int) int64 { return int64(bits.Len(uint(max(n, 1)))) }

// within checks the large library's work against the small one's: at most
// 1.5 × plus a log term.
func within(t *testing.T, what string, small, large, term int64) {
	t.Helper()
	if large > small*3/2+term {
		t.Errorf("%s: %d at 50,400 episodes, %d at 5,000: more than 1.5 × plus %d", what, large, small, term)
	}
}

// TestScreenBudget runs the screens' per-request budgets on the budget
// libraries.
func TestScreenBudget(t *testing.T) {
	skipUnderRace(t)
	small := openShaped(t, "s", budgetSmallShapes)
	large := openShaped(t, "l", budgetLargeShapes)
	type result struct {
		summary, detail, card, queue, page, anchored, homeSeed, homeNewSeed, homeCheckpoint views.WorkCounts
		seeks                                                                               map[int]views.WorkCounts
		missing                                                                             views.WorkCounts
		ranges                                                                              int
		queueDeep, queueAnchored, statistics, subscriptions, media                          views.WorkCounts
		queueRanges, months                                                                 int
	}
	run := func(s *shaped) result {
		lib, _ := libOf(t, s)
		r := result{seeks: map[int]views.WorkCounts{}}
		ep := s.lib.Episodes[budgetSmallPodcast][3]
		if s == large {
			ep = s.lib.Episodes[budgetLargePodcast][3]
		}
		r.summary = measure(func() { lib.Counters(); lib.AccountRecords() })
		r.detail = measure(func() {
			e, p := lib.Episode(ep)
			views.EpisodeJSON(lib.V, e)
			views.PodcastJSON(p)
			lib.Counters()
		})
		r.card = measure(func() {
			p := lib.Podcast(s.lib.Podcasts[0])
			lib.PodcastStatistics(p)
			views.PodcastJSON(p)
			lib.Counters()
		})
		r.queue = measure(func() { lib.Queue().Page(0, 100, nil) })
		r.queueRanges = lib.Queue().Ranges()
		depth := 4000 // the queue's first podcast queues every episode
		if s == large {
			depth = 40000
		}
		r.queueDeep = measure(func() { lib.Queue().Page(depth, 100, nil) })
		r.queueAnchored = measure(func() {
			q := lib.Queue()
			c, found := q.Anchor(s.lib.Episodes[0][depth], 0, 50)
			if !found {
				t.Fatal("a queued anchor not found")
			}
			q.Page(c.Offset, 100, &c)
		})
		now := viewstest.FixedNow
		r.statistics = measure(func() { r.months = len(lib.Statistics(0, stats.Forever, false, now).Months) })
		r.subscriptions = measure(func() {
			for _, order := range []string{"counter", "alphabetical", "most_played", "most_recent_episode"} {
				o, err := views.ParseSubscriptionOptions("subscribed", order, "unplayed", "")
				if err != nil {
					t.Fatal(err)
				}
				lib.SubscriptionView(o).Page(0, 100, nil)
			}
		})
		r.media = measure(func() { // the media checks: one episode, one podcast
			lib.Episode(ep)
			lib.Podcast(s.lib.Podcasts[0])
		})
		all := everyEpisodeList(lib)
		r.ranges = all.Ranges()
		r.page = measure(func() { everyEpisodeList(lib).Page(0, 100, nil) })
		r.anchored = measure(func() {
			l := everyEpisodeList(lib)
			c, _ := l.Anchor(ep, 0, 50)
			l.Page(c.Offset, 100, &c)
		})
		for _, depth := range []int{500, 4500} {
			r.seeks[depth] = measure(func() { everyEpisodeList(lib).Page(depth, 100, nil) })
		}
		if s == large {
			r.seeks[45000] = measure(func() { everyEpisodeList(lib).Page(45000, 100, nil) })
		}
		r.missing = measure(func() {
			l := everyEpisodeList(lib)
			c, _ := l.Anchor("e1:gone", 4000, 20)
			l.Page(c.Offset, 100, &c)
		})
		if s == large { // a missing anchor's index near the start and deep
			for _, idx := range []int{500, 45000} {
				r.seeks[-idx] = measure(func() {
					l := everyEpisodeList(lib)
					c, _ := l.Anchor("e1:gone", idx, 20)
					l.Page(c.Offset, 100, &c)
				})
			}
		}
		r.homeSeed = measure(func() { lib.Home(now, 1, "date_new_old", false) })
		r.homeNewSeed = measure(func() { lib.Home(now, 987654321, "date_new_old", false) })
		s.commit(checkpoint(ep, now-1000))
		lib, _ = libOf(t, s)
		r.homeCheckpoint = measure(func() { lib.Home(now, 1, "date_new_old", false) })
		return r
	}
	rs, rl := run(small), run(large)
	n, p := large.n, int64(len(budgetLargeShapes))
	log := func(x int) int64 { return lg(x) }
	t.Logf("summary %+v / %+v", rs.summary, rl.summary)
	t.Logf("detail %+v / %+v; card %+v / %+v", rs.detail, rl.detail, rs.card, rl.card)
	t.Logf("queue page (%d / %d ranges) %+v / %+v; deep %+v / %+v; anchored %+v / %+v", rs.queueRanges, rl.queueRanges, rs.queue, rl.queue,
		rs.queueDeep, rl.queueDeep, rs.queueAnchored, rl.queueAnchored)
	t.Logf("statistics (%d / %d months) %+v / %+v; subscriptions %+v / %+v; media %+v / %+v", rs.months, rl.months, rs.statistics, rl.statistics,
		rs.subscriptions, rl.subscriptions, rs.media, rl.media)
	t.Logf("Episodes page 1 (%d / %d ranges) %+v / %+v; anchored %+v / %+v", rs.ranges, rl.ranges, rs.page, rl.page, rs.anchored, rl.anchored)
	t.Logf("seeks %v / %v; missing anchor %+v / %+v", rs.seeks, rl.seeks, rs.missing, rl.missing)
	t.Logf("Home %+v, %+v, %+v / %+v, %+v, %+v", rs.homeSeed, rs.homeNewSeed, rs.homeCheckpoint, rl.homeSeed, rl.homeNewSeed, rl.homeCheckpoint)
	// (b) O(P log n) screens; the detail encodes one row, a queue page its limit.
	for _, c := range []struct {
		name string
		s, l views.WorkCounts
	}{{"summary", rs.summary, rl.summary}, {"detail", rs.detail, rl.detail}, {"card", rs.card, rl.card}} {
		if c.l.NodesVisited > p*8*(log(n)+1) {
			t.Errorf("%s: %d nodes visited, more than P × 8 log n", c.name, c.l.NodesVisited)
		}
		within(t, c.name, c.s.NodesVisited, c.l.NodesVisited, p*8*(log(n)-log(small.n)+1))
	}
	if rs.detail.RowsEncoded != 1 || rl.detail.RowsEncoded != 1 || rs.queue.RowsEncoded != 100 || rl.queue.RowsEncoded != 100 {
		t.Errorf("rows encoded: detail %d / %d, queue page %d / %d", rs.detail.RowsEncoded, rl.detail.RowsEncoded, rs.queue.RowsEncoded, rl.queue.RowsEncoded)
	}
	// (c) a first page and an anchored window: R ranges placed and sought,
	// then limit rows from the merge.
	for _, c := range []struct {
		name   string
		w      views.WorkCounts
		n, r   int
		rows   int64
		before int64
	}{{"page 1", rs.page, small.n, rs.ranges, 100, 0}, {"page 1", rl.page, large.n, rl.ranges, 100, 0},
		{"anchored", rs.anchored, small.n, rs.ranges, 100, 50}, {"anchored", rl.anchored, large.n, rl.ranges, 100, 50}} {
		budget := int64(c.r)*(3*log(c.n)+4) + (c.rows+c.before)*(log(c.r)+4)
		if c.w.RowsEncoded != c.rows || c.w.NodesVisited > budget {
			t.Errorf("%s at %d: %+v; budget %d rows, %d nodes", c.name, c.n, c.w, c.rows, budget)
		}
	}
	within(t, "page 1", rs.page.NodesVisited, rl.page.NodesVisited, int64(rl.ranges)*3*(log(n)-log(small.n)+1))
	// (b, c, g) the queue: its podcasts' ranges placed and sought, then the
	// rows; a deep page selects its offset; an anchored window ranks the
	// anchor and steps back.
	for _, c := range []struct {
		name string
		w    views.WorkCounts
		n, r int
		deep bool
		back int64
	}{{"queue page", rs.queue, small.n, rs.queueRanges, false, 0}, {"queue page", rl.queue, large.n, rl.queueRanges, false, 0},
		{"deep queue page", rs.queueDeep, small.n, rs.queueRanges, true, 0}, {"deep queue page", rl.queueDeep, large.n, rl.queueRanges, true, 0},
		{"anchored queue", rs.queueAnchored, small.n, rs.queueRanges, false, 50}, {"anchored queue", rl.queueAnchored, large.n, rl.queueRanges, false, 50}} {
		budget := int64(c.r)*(3*log(c.n)+4) + (100+c.back)*(log(c.r)+4)
		if c.deep {
			budget = int64(c.r)*2*log(c.n)*(2*log(c.n)+2) + 100*(log(c.r)+4)
		}
		if c.w.RowsEncoded != 100 || c.w.NodesVisited > budget {
			t.Errorf("%s at %d: %+v; budget %d nodes", c.name, c.n, c.w, budget)
		}
	}
	within(t, "queue page", rs.queue.NodesVisited, rl.queue.NodesVisited, int64(rl.queueRanges)*3*(log(n)-log(small.n)+1))
	// The statistics: P podcasts' prefix sums and the months they return;
	// the subscription orders: P podcasts' counts and newest episodes; the
	// media checks: two lookups.
	for _, c := range []struct {
		name   string
		s, l   views.WorkCounts
		budget int64
	}{{"statistics", rs.statistics, rl.statistics, p*12*(log(n)+1) + 2*int64(rl.months) + 16},
		{"subscriptions", rs.subscriptions, rl.subscriptions, 4 * p * 3 * (log(n) + 1)},
		{"media", rs.media, rl.media, 4 * (log(n) + 1)}} {
		if c.l.NodesVisited > c.budget || c.s.NodesVisited > c.budget {
			t.Errorf("%s: %d / %d nodes visited, budget %d", c.name, c.s.NodesVisited, c.l.NodesVisited, c.budget)
		}
	}
	// (g) a page at any depth, and a missing anchor's index: R · log n ·
	// log N, the same at depth 500 and 45,000.
	for depth, w := range rl.seeks {
		budget := int64(rl.ranges)*2*log(n)*(2*log(n)+2) + 100*(log(rl.ranges)+4)
		if w.NodesVisited > budget || w.RowsEncoded != 100 {
			t.Errorf("a page at depth %d: %+v; budget %d nodes", depth, w, budget)
		}
	}
	within(t, "a page at depth 45,000 against 500", rl.seeks[500].NodesVisited, rl.seeks[45000].NodesVisited, int64(rl.ranges)*2*log(n)*4)
	within(t, "a missing anchor at index 45,000 against 500", rl.seeks[-500].NodesVisited, rl.seeks[-45000].NodesVisited, int64(rl.ranges)*2*log(n)*4)
	within(t, "a missing anchor", rs.missing.NodesVisited, rl.missing.NodesVisited, int64(rl.ranges)*2*log(n)*(log(n)-log(small.n)+4))
	// (d) Home: any seed, and after a checkpoint, O(P log n + 8); its
	// candidates at most the podcasts and the plays of the last hour.
	for _, w := range []views.WorkCounts{rl.homeSeed, rl.homeNewSeed, rl.homeCheckpoint} {
		if w.NodesVisited > p*40*(log(n)+1)+8*16 || w.Candidates > p+2 {
			t.Errorf("Home: %+v; budget %d nodes, %d candidates", w, p*40*(log(n)+1)+8*16, p+2)
		}
	}
	within(t, "Home", rs.homeSeed.NodesVisited, rl.homeSeed.NodesVisited, p*40*(log(n)-log(small.n)+1))
}

// Continue reads the same at a queue of 500 and of 5,000.
func TestContinueBudgetDoesNotGrowWithTheQueue(t *testing.T) {
	var work []views.WorkCounts
	for _, queued := range []int{500, 5000} {
		s := openShaped(t, fmt.Sprintf("c%d", queued), []viewstest.Shape{{Episodes: queued, Queued: queued, Months: 1}, {Episodes: 20, Queued: 10}})
		lib, ref := libOf(t, s)
		var got []*views.EpisodeItem
		work = append(work, measure(func() { got = lib.Continue(viewstest.FixedNow) }))
		want := ref.Home(viewstest.FixedNow, 0, "date_new_old", false).Continue
		if len(got) != len(want) {
			t.Fatalf("queue %d: %d, the oracle %d", queued, len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Fatalf("queue %d: continue %d is %s, the oracle's %s", queued, i, got[i].ID, want[i].ID)
			}
		}
	}
	t.Logf("Continue at queues of 500 and 5,000: %+v, %+v", work[0], work[1])
	within(t, "Continue", work[0].NodesVisited, work[1].NodesVisited, 3*2*(lg(5020)-lg(520)+1)*2)
}

// Continue with ties of recent across its positive partitions, a play
// exactly at now − 30 s and a millisecond before, negative last played,
// and 49,900 negative-recent entries before 8 zero-recent ones: 8 rows,
// the oracle's, from at most 16 entries looked at past the seeks.
func TestContinueWitnesses(t *testing.T) {
	now := viewstest.FixedNow
	key := func(i int) string { return viewstest.QueueKeyAt(i) }
	lib, store, account := handLibrary(t, []tPod{
		{"A", "a", "subscribed", []tEp{
			{read: 0, pubDate: 1, position: 2000, lastPlayed: now - 5000, queue: key(5)},  // pos, ties with B's recent
			{read: 0, pubDate: 2, position: 500, lastPlayed: now - 30_000, queue: key(1)}, // recent, exactly at the window's edge: counts
			{read: 0, pubDate: 3, position: 500, lastPlayed: now - 30_001, queue: key(2)}, // past the window: zero
			{read: 0, pubDate: 4, position: 2000, lastPlayed: -3, queue: key(3)},          // negative
			{read: 0, pubDate: 5, position: 2000, lastPlayed: -1, queue: key(9)},          // negative, nearer 0
		}},
		{"B", "b", "subscribed", []tEp{
			{read: 0, pubDate: 1, position: 700, lastPlayed: now - 5000, queue: key(4)}, // recent, ties with A's pos: queue order
			{read: 0, pubDate: 2, noMedia: true, lastPlayed: now, queue: key(0)},        // no media: zero
			{read: 0, pubDate: 3, position: 1000, queue: key(6)},                        // never played: zero
		}},
	})
	var ref *reference.Library
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		ref, err = reference.Load(r, account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, want := lib.Continue(now), ref.Home(now, 0, "date_new_old", false).Continue
	if len(got) != len(want) || len(got) != 8 {
		t.Fatalf("continue %d, the oracle %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID {
			t.Fatalf("continue %d is %s, the oracle's %s", i, got[i].ID, want[i].ID)
		}
	}
	// 49,900 negative recents, then 8 zero ones: a single-goroutine test at
	// scale, which runs without the race detector (skipUnderRace).
	if raceFactor > 1 {
		return
	}
	s := openShaped(t, "n", []viewstest.Shape{{Episodes: 49908, Queued: 49908}})
	s.commit(func(a *storage.AccountTx) error {
		for i, e := range s.lib.Episodes[0] {
			pos, last := int64(2000), int64(-1-i%7)
			if i >= 49900 {
				pos, last = 0, 0
			}
			if err := put(a, "episode", e, "position", fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, pos)); err != nil {
				return err
			}
			if err := put(a, "episode", e, "last_played", fmt.Sprintf(`{"time":%d}`, last)); err != nil {
				return err
			}
		}
		return nil
	})
	big, bigRef := libOf(t, s)
	var rows []*views.EpisodeItem
	w := measure(func() { rows = big.Continue(now) })
	wantBig := bigRef.Home(now, 0, "date_new_old", false).Continue
	for i := range wantBig {
		if rows[i].ID != wantBig[i].ID || rows[i].Position != 0 {
			t.Fatalf("continue %d is %s, the oracle's %s", i, rows[i].ID, wantBig[i].ID)
		}
	}
	// The seeks of 6 partitions' ranges, then the 8 rows (and their
	// iterators' steps).
	if budget := 6*2*(lg(s.n)+1) + 16*2; len(rows) != 8 || w.NodesVisited > budget || w.Candidates > 16 {
		t.Fatalf("continue over 49,900 negative recents: %d rows, %+v; budget %d nodes", len(rows), w, budget)
	}
}

// Rows with equal sort values in different (podcast, class) ranges
// keep item order (rank), here the opposite of their IDs' order.
func TestSortTiesAcrossRangesFollowRank(t *testing.T) {
	// B's episodes are created first: their ranks are lower, their IDs
	// higher.
	lib, store, account := handLibrary(t, []tPod{
		{"B", "b", "subscribed", []tEp{{read: 0, pubDate: 100}, {read: -1, pubDate: 100}, {read: 1, pubDate: 50}}},
		{"A", "a", "subscribed", []tEp{{read: 0, pubDate: 100}, {read: -1, pubDate: 100}, {read: 1, pubDate: 50}}},
	})
	var ref *reference.Library
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		ref, err = reference.Load(r, account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, order := range []string{"date_new_old", "date_old_new", "duration_short_long", "episode_title_a_z"} {
		got := allRows(lib.Episodes(views.ListQuery{Filter: views.Filter{}.AllStates(), Order: order}))
		want := ref.EpisodesWhere(reference.Filter{}.AllStates(), order, "")
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Fatalf("%s: row %d is %s, the oracle's %s", order, i, got[i].ID, want[i].ID)
			}
		}
		if order == "date_new_old" && got[0].ID != episodeID("B", 0) {
			t.Fatalf("date_new_old starts with %s: ties go by rank", got[0].ID)
		}
	}
}

// A queue mostly of hidden podcasts' episodes: 49,900 hidden, 100
// shown; the first page visits the shown ones only.
func TestQueueSkipsHiddenPodcasts(t *testing.T) {
	skipUnderRace(t)
	s := openShaped(t, "h", []viewstest.Shape{{Episodes: 49900, Queued: 49900, State: "removed"}, {Episodes: 100, Queued: 100}})
	lib, ref := libOf(t, s)
	var items []json.RawMessage
	w := measure(func() { items, _, _ = lib.Queue().Page(0, 100, nil) })
	want, _, _ := ref.ListItems("/v1/read/queue", "", nil)
	if len(items) != 100 || len(want) != 100 {
		t.Fatalf("%d rows, the oracle %d", len(items), len(want))
	}
	for i := range want {
		if !bytes.Equal(items[i], want[i]) {
			t.Fatalf("row %d differs", i)
		}
	}
	if budget := 2*(lg(s.n)+1) + 100*4; w.NodesVisited > budget {
		t.Fatalf("the first page visited %d nodes, budget %d", w.NodesVisited, budget)
	}
}

// Surprise looks at each podcast's pick and at each play of the last
// hour once; as the clock passes a candidate's 712-day or one-hour edge,
// with the version and the seed unchanged, the section follows the
// specification.
func TestSurpriseWindowsAndCandidates(t *testing.T) {
	now := viewstest.FixedNow
	var pods []tPod
	for i := range 10 {
		p := tPod{id: fmt.Sprintf("P%d", i), title: "p", state: "subscribed"}
		for j := range 6 {
			e := tEp{read: 0, pubDate: now - int64(j)*100*day}
			switch j {
			case 4:
				e.pubDate = now - 2*356*day + 60_000 // leaves the window a minute from now
			case 5:
				e.lastPlayed = now - 3600_000 + 120_000 // a play leaving the hour in two minutes
			}
			p.eps = append(p.eps, e)
		}
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
	for _, seed := range []int64{1, 2, 3, 4, 5, 6, 7} {
		for _, at := range []int64{now, now + 59_999, now + 60_000, now + 60_001, now + 119_999, now + 120_000, now + 120_001} {
			var got []*views.EpisodeItem
			w := measure(func() { got = lib.Surprise(at, seed) })
			want := ref.SurpriseF5(at, seed)
			if len(got) != len(want) {
				t.Fatalf("seed %d at +%d: %d picks, the specification %d", seed, at-now, len(got), len(want))
			}
			for i := range want {
				if got[i].ID != want[i].ID {
					t.Fatalf("seed %d at +%d: pick %d is %s, the specification's %s", seed, at-now, i, got[i].ID, want[i].ID)
				}
			}
			// Each podcast's never-played pick (and its ties), and its one
			// play of the last hour.
			if w.Candidates > 2*int64(len(pods)) {
				t.Fatalf("seed %d at +%d: %d candidates looked at", seed, at-now, w.Candidates)
			}
		}
	}
}

// Structurally, a view keeps no rows, only its ranges (24 bytes
// each) and its resume cursors (4 bytes a range each, at most 4, in slots
// that are reused): at most 40 bytes a range and 1 KiB besides its extra,
// however many pages were read. The footprint walks everything the view
// reaches apart from its version, so a page cache, a growing slice or a
// cursor kept past its slot is counted. Lists of 300 podcasts' 600 ranges,
// the queue's and the subscriptions'.
func TestViewsKeepNoRows(t *testing.T) {
	s := openShaped(t, "m", repeatShape(300, viewstest.Shape{Episodes: 10, Queued: 2, Months: 1}))
	lib, _ := libOf(t, s)
	reg, _ := newRegistry(t)
	extra := map[string]any{"counters": lib.Counters()}
	exb, _ := json.Marshal(extra)
	subs, err := views.ParseSubscriptionOptions("subscribed", "alphabetical", "new", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		src  views.Source
	}{{"episodes", everyEpisodeList(lib)}, {"queue", lib.Queue()}, {"subscriptions", lib.SubscriptionView(subs)}} {
		id, err := reg.Put(s.account, 0, lib, c.name, c.src, extra)
		if err != nil {
			t.Fatal(err)
		}
		var base uintptr
		for i, offset := 0, 0; offset < c.src.Total(); i, offset = i+1, offset+50 {
			if _, err := reg.Read(s.account, id, c.name, offset, 50, nil, lib.Identity()); err != nil {
				t.Fatal(err)
			}
			f, _ := views.ViewFootprint(reg, id)
			if i == 0 {
				base = f.ResumeBase
			}
			if f.ResumeCap > 4 || f.ResumeBase != base {
				t.Fatalf("%s: after %d pages the resume slots are %d at another address", c.name, i+1, f.ResumeCap)
			}
		}
		f, ok := views.ViewFootprint(reg, id)
		bound := int64(40*f.Ranges + 1024 + len(exb))
		if !ok || f.Positions > 4*f.Ranges || f.Bytes > bound {
			t.Fatalf("%s: a view of %d ranges keeps %d positions in %d bytes, more than %d", c.name, f.Ranges, f.Positions, f.Bytes, bound)
		}
		t.Logf("%s: a view of %d ranges keeps %d positions in %d bytes (bound %d)", c.name, f.Ranges, f.Positions, f.Bytes, bound)
	}
	// The ranges' dimension of the page budgets, at 600 ranges.
	R, n := int64(everyEpisodeList(lib).Ranges()), s.n
	first := measure(func() { everyEpisodeList(lib).Page(0, 100, nil) })
	deep := measure(func() { everyEpisodeList(lib).Page(n-200, 100, nil) })
	if budget := R*(3*lg(n)+4) + 100*(lg(int(R))+4); first.NodesVisited > budget {
		t.Errorf("page 1 of %d ranges: %d nodes, budget %d", R, first.NodesVisited, budget)
	}
	if budget := R*2*lg(n)*(2*lg(n)+2) + 100*(lg(int(R))+4); deep.NodesVisited > budget {
		t.Errorf("a deep page of %d ranges: %d nodes, budget %d", R, deep.NodesVisited, budget)
	}
	t.Logf("%d ranges: page 1 %d nodes, at %d %d nodes", R, first.NodesVisited, n-200, deep.NodesVisited)
}
