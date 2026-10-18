package views_test

// The screens served from the model against the oracle (the views'
// earlier code, frozen in internal/views/reference), byte for byte where
// they are bytes: every list of the query matrix walked through the views
// registry in pages of random sizes, the subscription orders, statistics
// windows at their boundaries, Home's sections, podcast cards and episode
// rows.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// matrixFilters are the matrix's filters: the Android filter screens'
// properties (androidFilters, checked here), then the combinations.
var matrixFilters = func() []string {
	for i, f := range androidFilters {
		if viewstest.MatrixFilters[i] != strings.Join(f.props, ",") {
			panic("the matrix's filters are not the Android screens'")
		}
	}
	return viewstest.MatrixFilters
}()

// matrixQuery is one list query: an endpoint and its parameters, as the API
// maps them to a list.
type matrixQuery struct {
	path, podcast string
	q             url.Values
}

func (m matrixQuery) String() string { return m.path + "?" + m.q.Encode() }

// list is the query's list, as the API's handlers make it.
func (m matrixQuery) list(lib *views.Library) (views.Source, bool) {
	order := m.q.Get("sort")
	if order == "" {
		order = "date_new_old"
	}
	switch m.path {
	case "/v1/read/subscriptions":
		o, err := views.ParseSubscriptionOptions(m.q.Get("state"), m.q.Get("order"), m.q.Get("counter"), m.q.Get("filter"))
		return lib.SubscriptionView(o), err == nil
	case "/v1/read/queue":
		return lib.Queue(), true
	case "/v1/read/inbox":
		return lib.Episodes(views.ListQuery{Filter: views.Filter{New: true}, Order: order}), true
	case "/v1/read/favorites":
		return lib.Episodes(views.ListQuery{Filter: views.Filter{Favorite: true}.AllStates(), Order: order}), true
	case "/v1/read/history":
		return lib.Episodes(views.ListQuery{Filter: views.Filter{InHistory: true}.AllStates(), Order: "completion_date_new_old"}), true
	}
	f, err := views.ParseFilter(m.q.Get("filter"))
	if err != nil {
		return nil, false
	}
	if m.podcast != "" {
		if lib.Podcast(m.podcast) == nil {
			return nil, false
		}
		return lib.Episodes(views.ListQuery{Filter: f.AllStates(), Order: order, Podcast: m.podcast}), true
	}
	if f.Favorite {
		f = f.AllStates()
	}
	return lib.Episodes(views.ListQuery{Filter: f, Order: order}), true
}

// queryMatrix is the list matrix over the library's podcasts
// (viewstest.QueryMatrix).
func queryMatrix(ref *reference.Library) []matrixQuery {
	var pods []string
	for id := range ref.Podcasts {
		pods = append(pods, id)
	}
	var out []matrixQuery
	for _, q := range viewstest.QueryMatrix(pods) {
		out = append(out, matrixQuery{q.Path, q.Podcast, q.Q})
	}
	return out
}

// compareList walks the query's list through a views registry in pages
// of random sizes (1 to 500), and compares its rows, total and counters
// with the oracle's, byte for byte.
func compareList(t *testing.T, lib *views.Library, ref *reference.Library, m matrixQuery, r *rand.Rand) int {
	t.Helper()
	want, counters, ok := ref.ListItems(strings.SplitN(m.path, "?", 2)[0], m.podcast, m.q)
	src, okNew := m.list(lib)
	if ok != okNew {
		t.Fatalf("%s: the oracle answers %v, the model %v", m, ok, okNew)
	}
	if !ok {
		return 0
	}
	spool, _ := views.NewSpool("", func() time.Time { return time.UnixMilli(0) })
	id, err := spool.Put(lib.V.Account, 0, lib, m.String(), src, map[string]any{"counters": lib.Counters()})
	if err != nil {
		t.Fatal(err)
	}
	var got []json.RawMessage
	offset := 0
	for {
		page, err := spool.Read(lib.V.Account, id, m.String(), offset, 1+r.IntN(500), nil, lib.Identity())
		if err != nil {
			t.Fatalf("%s: %v", m, err)
		}
		if page.Total != len(want) {
			t.Fatalf("%s: total %d, the oracle %d", m, page.Total, len(want))
		}
		if !bytes.Equal(page.Extra, counters) {
			t.Fatalf("%s: the view %s, the oracle %s", m, page.Extra, counters)
		}
		got = append(got, page.Items...)
		if page.Next == "" {
			break
		}
		_, offset, err = views.ParsePageToken(page.Next)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, the oracle %d", m, len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("%s: row %d:\n%s\nthe oracle:\n%s", m, i, got[i], want[i])
		}
	}
	return len(got)
}

// statisticsWindows are the statistics windows around the library's dates: all
// time, Android's three-year and 100-day ones, a play's own date exactly
// and a millisecond on either side, empty and reversed windows.
func statisticsWindows(ref *reference.Library, now int64, r *rand.Rand) [][2]int64 {
	w := [][2]int64{{0, stats.Forever}, {now - 3*365*day, stats.Forever}, {now - 100*day, now}, {now, now}, {now, now - 1}}
	var dates []int64
	for _, e := range ref.Episodes {
		for _, x := range reference.Parts(ref, e) {
			if x.LastPlayed > 0 {
				dates = append(dates, x.LastPlayed)
			}
		}
	}
	for range 4 {
		if len(dates) == 0 {
			break
		}
		d := dates[r.IntN(len(dates))]
		w = append(w, [2]int64{d, d + 1}, [2]int64{d - 1, d}, [2]int64{d + 1, stats.Forever}, [2]int64{0, d})
	}
	return w
}

// compareScreens compares the other screens: statistics in every window
// both ways, Home at several seeds, orders and times, the podcast cards and
// episode rows.
func compareScreens(t *testing.T, lib *views.Library, ref *reference.Library, now int64, r *rand.Rand, full bool) {
	t.Helper()
	if !reflect.DeepEqual(lib.Counters(), views.Counters(ref.Counters())) {
		t.Fatalf("counters %+v, the oracle %+v", lib.Counters(), ref.Counters())
	}
	marshal := func(v any) string {
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(false) // as the API writes
		if err := enc.Encode(v); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	rowsJSON := func(list []*views.EpisodeItem) string {
		var out []any
		for _, e := range list {
			m, err := views.EpisodeJSON(lib.V, e)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, m)
		}
		return marshal(out)
	}
	refRows := func(list []*reference.Episode) string {
		var out []any
		for _, e := range list {
			out = append(out, reference.EpisodeJSON(e))
		}
		return marshal(out)
	}
	windows := statisticsWindows(ref, now, r)
	if !full {
		windows = windows[:2]
	}
	for _, w := range windows {
		for _, marked := range []bool{false, true} {
			got, want := lib.Statistics(w[0], w[1], marked, now), ref.Statistics(w[0], w[1], marked, now)
			if len(got.Podcasts) != len(want.Podcasts) || got.OldestDate != want.OldestDate {
				t.Fatalf("statistics %v marked %v: %d rows, oldest %d; the oracle %d, %d", w, marked, len(got.Podcasts), got.OldestDate, len(want.Podcasts), want.OldestDate)
			}
			for i, row := range want.Podcasts {
				g := got.Podcasts[i]
				if g.Podcast.ID != row.Podcast.ID || g.Stats != row.Stats || g.OldestDate != row.OldestDate {
					t.Fatalf("statistics %v marked %v row %d: %s %+v %d; the oracle %s %+v %d", w, marked, i, g.Podcast.ID, g.Stats, g.OldestDate,
						row.Podcast.ID, row.Stats, row.OldestDate)
				}
			}
			if marshal(got.Months) != marshal(want.Months) {
				t.Fatalf("statistics months %v, the oracle %v", got.Months, want.Months)
			}
		}
	}
	// Home: fixed and random seeds, Inbox orders, times at a play's edges.
	seeds := []int64{0, 1, 42, r.Int64()}
	times := []int64{now}
	for _, e := range ref.Episodes {
		if e.LastPlayed > 0 && len(times) < 4 {
			times = append(times, e.LastPlayed+30_000, e.LastPlayed+30_001)
		}
	}
	orders := views.OrderNames[:]
	if !full {
		seeds, times, orders = seeds[3:], times[:1], []string{views.OrderNames[r.IntN(len(views.OrderNames))]}
	}
	for _, at := range times {
		for _, seed := range seeds {
			for _, order := range orders {
				for _, marked := range []bool{false, true} {
					h, w := lib.Home(at, seed, order, marked), ref.Home(at, seed, order, marked)
					if g, x := rowsJSON(h.Continue), refRows(w.Continue); g != x {
						t.Fatalf("Home at %d: continue\n%s\nthe oracle\n%s", at, g, x)
					}
					if g, x := rowsJSON(h.New), refRows(w.New); g != x || h.NewCount != w.NewCount {
						t.Fatalf("Home at %d, %s: new %d\n%s\nthe oracle %d\n%s", at, order, h.NewCount, g, w.NewCount, x)
					}
					if g, x := rowsJSON(h.Surprise), refRows(ref.SurpriseF5(at, seed)); g != x {
						t.Fatalf("Home at %d, seed %d: surprise\n%s\nthe specification\n%s", at, seed, g, x)
					}
					var gc, wc []string
					for _, p := range h.Classics {
						gc = append(gc, p.ID)
					}
					for _, p := range w.Classics {
						wc = append(wc, p.ID)
					}
					if !slices.Equal(gc, wc) {
						t.Fatalf("Home at %d marked %v: classics %v, the oracle %v", at, marked, gc, wc)
					}
				}
			}
		}
	}
	// Podcast cards and rows; episode rows.
	var pods []string
	for id := range ref.Podcasts {
		pods = append(pods, id)
	}
	slices.Sort(pods)
	all := ref.Statistics(0, stats.Forever, true, now)
	for i, id := range pods {
		if !full && i%7 != 0 {
			continue
		}
		p := lib.Podcast(id)
		pm, err := views.PodcastJSON(p)
		if err != nil {
			t.Fatal(err)
		}
		if g, w := marshal(pm), marshal(reference.PodcastJSON(ref.Podcasts[id])); g != w {
			t.Fatalf("podcast %s:\n%s\nthe oracle\n%s", id, g, w)
		}
		card, ok := lib.PodcastStatistics(p)
		var want *reference.PodcastStatistics
		for i := range all.Podcasts {
			if all.Podcasts[i].Podcast.ID == id {
				want = &all.Podcasts[i]
			}
		}
		if ok != (want != nil) || ok && (card.Stats != want.Stats || card.OldestDate != want.OldestDate) {
			t.Fatalf("podcast %s's card: %v %+v; the oracle %+v", id, ok, card, want)
		}
	}
	n := len(ref.Episodes)
	for i := 0; i < n; i += max(1, n/40) {
		w := ref.Episodes[i]
		e, p := lib.Episode(w.ID)
		if e == nil || p.ID != w.Podcast.ID {
			t.Fatalf("episode %s is not shown", w.ID)
		}
		if g, x := rowsJSON([]*views.EpisodeItem{e}), refRows([]*reference.Episode{w}); g != x {
			t.Fatalf("episode %s:\n%s\nthe oracle\n%s", w.ID, g, x)
		}
	}
}

// compareQueries compares the query matrix on one snapshot: the whole matrix, or (full false)
// count queries drawn from it, and the other screens.
func compareQueries(t *testing.T, lib *views.Library, ref *reference.Library, now int64, r *rand.Rand, full bool, count int) (lists, rows int) {
	t.Helper()
	matrix := queryMatrix(ref)
	if !full {
		r.Shuffle(len(matrix), func(i, j int) { matrix[i], matrix[j] = matrix[j], matrix[i] })
		matrix = matrix[:min(count, len(matrix))]
	}
	for _, m := range matrix {
		rows += compareList(t, lib, ref, m, r)
	}
	compareScreens(t, lib, ref, now, r, full)
	return len(matrix), rows
}

// The query matrix on the Android fixtures (each with six seeded states, imported for
// real) and generated libraries, the version made by the apply: the whole
// matrix.
func TestQueriesMatchTheReference(t *testing.T) {
	ctx := context.Background()
	check := func(t *testing.T, store *storage.Store, account string, seed uint64) {
		lib := throughApply(t, store, account)
		if err := store.Read(ctx, func(r *storage.ReadTx) error {
			ref, err := reference.Load(r, account)
			if err != nil {
				return err
			}
			lists, rows := compareQueries(t, lib, ref, viewstest.FixedNow, rand.New(rand.NewPCG(seed, 77)), true, 0)
			t.Logf("%d lists, %d rows", lists, rows)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, base := range []string{"emulator-parity-library.db", "authentication-preview-library.db"} {
		for seed := uint64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("%s/%d", base, seed), func(t *testing.T) {
				store, account := importAccount(t, mutatedFixture(t, base, seed))
				check(t, store, account, seed)
			})
		}
	}
	for seed := uint64(1); seed <= 2; seed++ {
		t.Run(fmt.Sprintf("generated/%d", seed), func(t *testing.T) {
			pods := make([]int, 20)
			for i := range pods {
				pods[i] = 40
			}
			store, account := openLibrary(t, viewstest.Spec{Seed: seed, Podcasts: pods, Queue: 80, Now: viewstest.FixedNow, Months: 24, Prefix: "q"}, false)
			check(t, store, account, seed)
		})
	}
}

// compareWindows compares count queries drawn from the matrix at two
// windows each: the first page, and a page at a random offset (read
// through its token), with the oracle's slices; and the other screens.
func compareWindows(t *testing.T, lib *views.Library, ref *reference.Library, now int64, r *rand.Rand, count int) int {
	t.Helper()
	matrix := queryMatrix(ref)
	r.Shuffle(len(matrix), func(i, j int) { matrix[i], matrix[j] = matrix[j], matrix[i] })
	matrix = matrix[:min(count, len(matrix))]
	for _, m := range matrix {
		want, counters, ok := ref.ListItems(strings.SplitN(m.path, "?", 2)[0], m.podcast, m.q)
		src, okNew := m.list(lib)
		if ok != okNew {
			t.Fatalf("%s: the oracle answers %v, the model %v", m, ok, okNew)
		}
		if !ok {
			continue
		}
		spool, _ := views.NewSpool("", func() time.Time { return time.UnixMilli(0) })
		id, err := spool.Put(lib.V.Account, 0, lib, m.String(), src, map[string]any{"counters": lib.Counters()})
		if err != nil {
			t.Fatal(err)
		}
		for _, offset := range []int{0, r.IntN(len(want) + 1)} {
			limit := 1 + r.IntN(500)
			page, err := spool.Read(lib.V.Account, id, m.String(), offset, limit, nil, lib.Identity())
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != len(want) || !bytes.Equal(page.Extra, counters) {
				t.Fatalf("%s: total %d, the oracle %d", m, page.Total, len(want))
			}
			end := min(len(want), offset+limit)
			if len(page.Items) != end-offset {
				t.Fatalf("%s at %d: %d rows, the oracle %d", m, offset, len(page.Items), end-offset)
			}
			for i, it := range page.Items {
				if !bytes.Equal(it, want[offset+i]) {
					t.Fatalf("%s: row %d:\n%s\nthe oracle:\n%s", m, offset+i, it, want[offset+i])
				}
			}
		}
	}
	compareScreens(t, lib, ref, now, r, false)
	return len(matrix)
}
