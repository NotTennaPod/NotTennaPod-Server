package views_test

// The incremental model's full build against the frozen oracle
// (internal/views/reference), and the class filters against Filter.Match.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// For every filter (all 2^15 property sets), every episode (each read
// state, with or without media, position, queue, favourite and history)
// and every podcast state, the oracle's Match is MatchClass of the
// episode's class and StateOK of the state.
func TestClassFiltersMatchFilterMatch(t *testing.T) {
	type attrs struct {
		read                                  int
		media, paused, queued, favorite, hist bool
	}
	var eps []attrs
	for _, read := range []int{-1, 0, 1} {
		for bits := range 32 {
			eps = append(eps, attrs{read, bits&1 != 0, bits&2 != 0, bits&4 != 0, bits&8 != 0, bits&16 != 0})
		}
	}
	checked := 0
	for mask := range 1 << 15 {
		b := func(i int) bool { return mask&(1<<i) != 0 }
		f := views.Filter{Played: b(0), Unplayed: b(1), New: b(2), Paused: b(3), NotPaused: b(4), Queued: b(5), NotQueued: b(6),
			HasMedia: b(7), NoMedia: b(8), Favorite: b(9), NotFavorite: b(10), InHistory: b(11), Subscribed: b(12), Archived: b(13), NotSubscribed: b(14)}
		for _, a := range eps {
			e := &reference.Episode{Read: a.read, HasMedia: a.media, Queued: a.queued, Favorite: a.favorite}
			if a.paused {
				e.Position = 1
			}
			if a.hist {
				e.History = 1
			}
			for _, state := range []string{"subscribed", "archived", "not_subscribed"} {
				e.Podcast = &reference.Podcast{State: state}
				class := views.ClassOf(e.Read, e.HasMedia, e.Position, e.Queued, e.Favorite, e.History)
				if got, want := f.MatchClass(class) && f.StateOK(state), reference.Filter(f).Match(e); got != want {
					t.Fatalf("filter %+v, episode %+v, %s: class says %v, Match %v", f, a, state, got, want)
				}
				checked++
			}
		}
	}
	t.Logf("%d combinations", checked)
}

// importAccount imports an Android backup into a new account, as
// importLibrary does, and returns the store and the account.
func importAccount(t *testing.T, path string) (*storage.Store, string) {
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
	svc := &protocol.Service{Store: store, Registry: reg, Box: box, Now: func() time.Time { return time.UnixMilli(viewstest.FixedNow) }}
	lib, err := androiddb.Read(ctx, path, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const importID = "6a1f6a0e-8a9b-4c55-9f6e-2b1d3c4e5f61"
	plan, err := importer.FromAndroid(lib, importID, viewstest.FixedNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Apply(ctx, svc, account, plan, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Complete(ctx, svc, account, importID, "", plan); err != nil {
		t.Fatal(err)
	}
	return store, account
}

// mutatedFixture copies an Android fixture and gives it seeded random
// library states (mutate).
func mutatedFixture(t *testing.T, base string, seed uint64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "AntennaPodBackup.db")
	copyFile(t, fixtures+base, path)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	mutate(t, db, rand.New(rand.NewPCG(seed, 99)))
	db.Close()
	return path
}

func provenanceList(raw json.RawMessage) []string {
	out := []string{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return out
}

// compareWithReference checks a version's shown projection against the
// oracle's library at the same snapshot: the account's records, every shown
// podcast and the stored episodes of shown podcasts field by field (with
// their records, statistics units, class and media source), the queue's
// order, the podcasts' class counts and undated statistics, their dated
// played time at windows around their units' dates, and the months.
func compareWithReference(t *testing.T, v *views.Version, ref *reference.Library) {
	t.Helper()
	sameRecords := func(what string, got map[string]views.RawRecord, want map[string]reference.Record) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: %d records, the reference %d", what, len(got), len(want))
		}
		for g, w := range want {
			r, ok := got[g]
			if !ok || string(r.Value) != string(w.Value) || !slices.Equal(provenanceList(r.Provenance), w.Provenance) {
				t.Fatalf("%s %s: %s %s, the reference %s %v", what, g, r.Value, r.Provenance, w.Value, w.Provenance)
			}
		}
	}
	sameRecords("account", v.AccountRecords, ref.Account)
	shown := map[string]*views.PodcastItem{}
	for id, p := range v.AllPodcasts() {
		shown[id] = p
	}
	if len(shown) != len(ref.Podcasts) {
		t.Fatalf("%d shown podcasts, the reference %d", len(shown), len(ref.Podcasts))
	}
	for id, w := range ref.Podcasts {
		p := shown[id]
		if p == nil || p.State != w.State || !reflect.DeepEqual(p.Title, w.Title) || p.DisplayTitle != w.DisplayTitle || p.ImageURL != w.ImageURL ||
			p.KeepUpdated != w.KeepUpdated || p.Rank != w.Rank {
			t.Fatalf("podcast %s: %+v, the reference %+v", id, p, w)
		}
		// The values the media and credential checks use, from the
		// oracle's own decoding of the records.
		generation, _ := reference.Fields(w.Records["subscription"])["generation_id"].(string)
		_, credentials := w.Records["credentials"]
		secret, _ := reference.Fields(w.Records["credentials"])["secret_id"].(string)
		if p.GenerationID != generation || p.HasCredentials != credentials || p.SecretID != secret {
			t.Fatalf("podcast %s: generation %q, credentials %v, secret %q; the reference %q, %v, %q", id, p.GenerationID, p.HasCredentials, p.SecretID,
				generation, credentials, secret)
		}
		sameRecords("podcast "+id, p.Records, w.Records)
	}
	var eps []*views.EpisodeItem
	for _, e := range v.AllEpisodes() {
		if shown[e.PodcastID] != nil {
			eps = append(eps, e)
		}
	}
	slices.SortFunc(eps, func(a, b *views.EpisodeItem) int {
		if a.Rank != b.Rank {
			return int(a.Rank - b.Rank)
		}
		return strings.Compare(a.ID, b.ID)
	})
	if len(eps) != len(ref.Episodes) {
		t.Fatalf("%d episodes of shown podcasts, the reference %d", len(eps), len(ref.Episodes))
	}
	parts := map[string][]stats.Part{}
	for i, w := range ref.Episodes {
		e := eps[i]
		source, declared := "", ""
		if m, ok := reference.Fields(w.Records["metadata"])["media"].(map[string]any); ok {
			source, _ = m["url"].(string)
			declared, _ = m["mime_type"].(string)
		}
		if source == "" {
			source, _ = w.Occurrence["enclosure_url"].(string)
		}
		meta := reference.Fields(w.Records["metadata"])
		image, _ := meta["image_url"].(string)
		chapters, _ := meta["chapter_url"].(string)
		transcript, _ := meta["transcript_url"].(string)
		transcriptType, _ := meta["transcript_type"].(string)
		wantParts := reference.Parts(ref, w)
		class := views.ClassOf(w.Read, w.HasMedia, w.Position, w.Queued, w.Favorite, w.History)
		if e.ID != w.ID || e.PodcastID != w.Podcast.ID || e.Placeholder != w.Placeholder || !reflect.DeepEqual(e.Title, w.Title) || e.PubDate != w.PubDate ||
			e.Read != w.Read || e.Position != w.Position || e.HasMedia != w.HasMedia || e.Duration != w.Duration || e.Size != w.Size ||
			!reflect.DeepEqual(e.Link, w.Link) || e.Queued != w.Queued || e.Favorite != w.Favorite || e.History != w.History ||
			e.LastPlayed != w.LastPlayed || e.Rank != w.Rank || e.SourceURL != source || e.DeclaredType != declared || e.ImageURL != image ||
			!reflect.DeepEqual(e.Parts, wantParts) || e.Class != class ||
			e.ChapterURL != chapters || e.TranscriptURL != transcript || e.TranscriptType != transcriptType ||
			e.Queued && contract.CompareQueueKeys(e.QueueKey, reference.QueueKey(w)) != 0 {
			t.Fatalf("episode %d %s:\n%+v\nthe reference:\n%+v", i, w.ID, *e, *w)
		}
		sameRecords("episode "+w.ID, e.Records, w.Records)
		parts[w.Podcast.ID] = append(parts[w.Podcast.ID], wantParts...)
	}
	// The queue in its order.
	var queue []*views.EpisodeItem
	for _, e := range eps {
		if e.Queued {
			queue = append(queue, e)
		}
	}
	slices.SortStableFunc(queue, func(a, b *views.EpisodeItem) int {
		if c := contract.CompareQueueKeys(a.QueueKey, b.QueueKey); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	if len(queue) != len(ref.Queue) {
		t.Fatalf("%d queued, the reference %d", len(queue), len(ref.Queue))
	}
	for i, w := range ref.Queue {
		if queue[i].ID != w.ID {
			t.Fatalf("queue %d: %s, the reference %s", i, queue[i].ID, w.ID)
		}
	}
	// The queue, Continue and Surprise trees, by the frozen rules.
	checkStructures(t, v)
	// Each sort order, range by range: a podcast's episodes of one class in
	// the oracle's order of them.
	type rangeKey struct {
		podcast string
		class   views.Class
	}
	refRanges := map[rangeKey][]*reference.Episode{}
	for _, w := range ref.Episodes {
		k := rangeKey{w.Podcast.ID, views.ClassOf(w.Read, w.HasMedia, w.Position, w.Queued, w.Favorite, w.History)}
		refRanges[k] = append(refRanges[k], w)
	}
	for i, name := range views.OrderNames {
		got := map[rangeKey][]string{}
		for e := range v.OrderEntries(i) {
			if shown[e.PodcastID] != nil {
				k := rangeKey{e.PodcastID, e.Class}
				got[k] = append(got[k], e.ID)
			}
		}
		if len(got) != len(refRanges) {
			t.Fatalf("order %s: %d ranges, the reference %d", name, len(got), len(refRanges))
		}
		for k, list := range refRanges {
			sorted := slices.Clone(list)
			reference.Sort(sorted, name)
			want := make([]string, len(sorted))
			for j, w := range sorted {
				want[j] = w.ID
			}
			if !slices.Equal(got[k], want) {
				t.Fatalf("order %s, podcast %s, class %d: %v, the reference %v", name, k.podcast, k.class, got[k], want)
			}
		}
	}
	// Each shown podcast's aggregates and dated statistics.
	for id := range shown {
		a := v.PodcastAgg(id)
		var hist [views.NumClasses]int32
		for _, e := range eps {
			if e.PodcastID == id {
				hist[e.Class]++
			}
		}
		all := stats.PerPodcast(parts[id], 0, stats.Forever)
		if a.Hist != hist || a.Parts != all.Episodes || a.TotalMS != all.TotalMS || a.MarkedAll != all.PlayedMarkedMS ||
			a.Started != all.EpisodesStarted || a.StartedMarked != all.EpisodesStartedMarked {
			t.Fatalf("podcast %s: aggregates %+v, the reference %+v", id, a, all)
		}
		var dates []int64
		var oldest int64
		for _, x := range parts[id] {
			dates = append(dates, x.LastPlayed)
			if x.LastPlayed > 0 && (oldest == 0 || x.LastPlayed < oldest) {
				oldest = x.LastPlayed
			}
		}
		if got := v.OldestPlayed(id); got != oldest {
			t.Fatalf("podcast %s: oldest played %d, the reference %d", id, got, oldest)
		}
		windows := [][2]int64{{0, stats.Forever}, {-1 << 62, stats.Forever}}
		for _, d := range dates {
			windows = append(windows, [2]int64{d, d}, [2]int64{d, d + 1}, [2]int64{d - 1, d}, [2]int64{d + 1, stats.Forever}, [2]int64{0, d})
		}
		for _, w := range windows {
			played, marked := v.PlayedBetween(id, w[0], w[1])
			if want := stats.PerPodcast(parts[id], w[0], w[1]).PlayedMS; played != want {
				t.Fatalf("podcast %s: played in [%d, %d) %d, the reference %d", id, w[0], w[1], played, want)
			}
			if want := stats.PlayedMarkedBetween(parts[id], w[0], w[1]); marked != want {
				t.Fatalf("podcast %s: marked in [%d, %d) %d, the reference %d", id, w[0], w[1], marked, want)
			}
		}
	}
	// The months, over the shown podcasts.
	var months []reference.MonthStatistics
	for k, ms := range v.Months() {
		y, m := views.MonthOf(k)
		months = append(months, reference.MonthStatistics{Year: y, Month: m, PlayedMS: ms})
	}
	if want := ref.Statistics(0, stats.Forever, false, viewstest.FixedNow).Months; !reflect.DeepEqual(months, want) && (len(months) > 0 || len(want) > 0) {
		t.Fatalf("months %v, the reference %v", months, want)
	}
}

// On the two Android fixtures, each given six seeded library states and
// imported for real, and on three generated libraries (with episodes
// without metadata, unregistered, merged, without a catalog item, and
// statistics units), the full build's shown projection equals the oracle's
// library field by field.
func TestBuilderMatchesTheReference(t *testing.T) {
	ctx := context.Background()
	check := func(t *testing.T, store *storage.Store, account string) {
		t.Helper()
		if err := store.Read(ctx, func(r *storage.ReadTx) error {
			v, err := views.BuildVersion(r.Account(account), account)
			if err != nil {
				return err
			}
			ref, err := reference.Load(r, account)
			if err != nil {
				return err
			}
			compareWithReference(t, v, ref)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, base := range []string{"emulator-parity-library.db", "authentication-preview-library.db"} {
		for seed := uint64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("%s/%d", base, seed), func(t *testing.T) {
				store, account := importAccount(t, mutatedFixture(t, base, seed))
				check(t, store, account)
			})
		}
	}
	for seed := uint64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("generated/%d", seed), func(t *testing.T) {
			pods := make([]int, 20)
			for i := range pods {
				pods[i] = 40
			}
			store, account := openLibrary(t, viewstest.Spec{Seed: seed, Podcasts: pods, Queue: 60, Now: viewstest.FixedNow, Months: 24, Prefix: "r"}, false)
			check(t, store, account)
		})
	}
}

// A seed replays exactly: the same seed gives the same mutated backup and
// the same imported library (the dates count from a fixed clock, never the
// time of the run).
func TestMutatedFixturesReplay(t *testing.T) {
	ctx := context.Background()
	dump := func(path string) string {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var b strings.Builder
		for _, q := range []string{"SELECT id, read, title, link, pubDate FROM FeedItems ORDER BY id",
			"SELECT id, duration, position, last_played_time, played_duration, playback_completion_date FROM FeedMedia ORDER BY id",
			"SELECT id, state FROM Feeds ORDER BY id"} {
			rows, err := db.Query(q)
			if err != nil {
				t.Fatal(err)
			}
			cols, _ := rows.Columns()
			for rows.Next() {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					t.Fatal(err)
				}
				fmt.Fprintln(&b, vals...)
			}
			rows.Close()
		}
		return b.String()
	}
	fields := func(path string) string {
		store, account := importAccount(t, path)
		var b strings.Builder
		// The import's own times are the fixed clock's: the metadata it
		// fetched says when.
		if err := store.Read(ctx, func(r *storage.ReadTx) error {
			ref, err := reference.Load(r, account)
			if err != nil {
				return err
			}
			n := 0
			for _, e := range ref.Episodes {
				if at, ok := reference.Fields(e.Records["metadata"])["fetch_time"]; ok {
					if v, _ := reference.Int(at); v != viewstest.FixedNow {
						t.Fatalf("episode %s was fetched at %v, not at the fixed clock", e.ID, at)
					}
					n++
				}
			}
			if n == 0 {
				t.Fatal("no imported metadata says when it was fetched")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.Read(ctx, func(r *storage.ReadTx) error {
			v, err := views.BuildVersion(r.Account(account), account)
			if err != nil {
				return err
			}
			// Each import has its own IDs: the values, sorted.
			var lines []string
			for _, e := range v.AllEpisodes() {
				lines = append(lines, fmt.Sprintln(e.PubDate, e.Read, e.Position, e.LastPlayed, e.History, e.Duration, e.Favorite, e.Queued, e.Parts))
			}
			slices.Sort(lines)
			b.WriteString(strings.Join(lines, ""))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	a, b := mutatedFixture(t, "emulator-parity-library.db", 3), mutatedFixture(t, "emulator-parity-library.db", 3)
	// The backup seed 3 makes, in every run (a wall clock would change it).
	const seed3 = "7458b4779bc4e3990d04de87f543b4cbf48ee926a2252dbeb4246c3450539f2a"
	sum := sha256.Sum256([]byte(dump(a)))
	if got := hex.EncodeToString(sum[:]); got != seed3 || dump(b) != dump(a) {
		t.Fatalf("seed 3 mutated the backup into %s, not %s", got, seed3)
	}
	if fa, fb := fields(a), fields(b); fa != fb {
		la, lb := strings.Split(fa, "\n"), strings.Split(fb, "\n")
		for i := range min(len(la), len(lb)) {
			if la[i] != lb[i] {
				t.Fatalf("the same seed's backup imported differently:\n%s\n%s", la[i], lb[i])
			}
		}
		t.Fatal("the same seed's backup imported differently")
	}
}
