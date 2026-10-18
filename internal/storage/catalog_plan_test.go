package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The catalog queries as they were before their join order was fixed: SQLite
// could start from every episode record of the account.
const (
	oldCatalogRevisionsQuery = `SELECT c.episode_id, json_extract(r.value, '$.revision') FROM catalog_items c
		JOIN records r ON r.account_id = c.account_id AND r.entity = 'episode' AND r.entity_id = c.episode_id AND r.field_group = 'metadata'
		WHERE c.account_id = ? AND c.podcast_id = ?`
	oldCatalogStampsQuery = `SELECT 'feed', json_extract(value, '$.revision'), json_extract(value, '$.fetch_time')
		FROM records WHERE account_id = ? AND entity = 'podcast' AND entity_id = ? AND field_group = 'metadata'
		UNION ALL
		SELECT c.episode_id, json_extract(r.value, '$.revision'), json_extract(r.value, '$.fetch_time') FROM catalog_items c
		JOIN records r ON r.account_id = c.account_id AND r.entity = 'episode' AND r.entity_id = c.episode_id AND r.field_group = 'metadata'
		WHERE c.account_id = ? AND c.podcast_id = ?`
)

// A server fetch reads the revisions of one podcast's catalog items under the
// single writer (PROTOCOL §9.4). The work scales with that podcast's items:
// the plan starts from catalog_items_podcast and looks up each item's
// metadata record by its full key, never walking the account's episode
// records. It holds on the writer without statistics, after the owner runs
// ANALYZE and after a restart on the analyzed database.
func TestCatalogRevisionsPlanStartsFromThePodcast(t *testing.T) {
	inStatisticsStates(t, 1, func(state string, s *Store, fx *catalogFixture) {
		acct, p := fx.accounts[0], fx.podcasts[0]
		plan := queryPlan(t, s, catalogRevisionsQuery, acct, p)
		if err := startsFromThePodcast(plan); err != nil {
			t.Errorf("%s: %v\nplan:\n%s", state, err, strings.Join(plan, "\n"))
		}
	})
}

// Re-parsing a stored response reads the stamps of one podcast's metadata
// record and its catalog items' records, on the same plan as the revisions,
// and the podcast's own record by its full key.
func TestCatalogStampsPlan(t *testing.T) {
	inStatisticsStates(t, 2, func(state string, s *Store, fx *catalogFixture) {
		acct, p := fx.accounts[0], fx.podcasts[0]
		plan := queryPlan(t, s, catalogStampsQuery, acct, p, acct, p)
		err := startsFromThePodcast(plan)
		if err == nil && !slices.ContainsFunc(plan, func(l string) bool {
			return strings.HasPrefix(l, "SEARCH records ") && strings.Contains(l, "entity_id=? AND field_group=?")
		}) {
			err = fmt.Errorf("the podcast's metadata record is not looked up by its key")
		}
		if err != nil {
			t.Errorf("%s: %v\nplan:\n%s", state, err, strings.Join(plan, "\n"))
		}
	})
}

// inStatisticsStates runs check on a small seeded library in each state of the
// planner's statistics the server's writer can be in: none, after ANALYZE ran
// on it, and reopened on the analyzed database.
func inStatisticsStates(t *testing.T, seed uint64, check func(state string, s *Store, fx *catalogFixture)) {
	t.Helper()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	fx := seedCatalog(t, s, 262, 6, seed)
	check("no statistics", s, fx)
	analyze(t, s)
	check("analyzed", s, fx)
	s.Close()
	check("reopened analyzed", openTest(t, config{dataDir: dir}), fx)
}

// The fixed queries return exactly the rows the old ones did, and the maps
// are the records' revisions and fetch times: on a small library and on one
// of 26,271 episodes in 70 podcasts, with items with and without metadata,
// null or missing revisions and fetch times, items of other podcasts, episode
// records without catalog items, the podcast's own metadata record present,
// null or missing, a podcast without items, an unknown podcast, and another
// account with the same IDs. The maps are checked for every podcast; on the
// large library's first account, whose old queries walk 26,271 episodes'
// records each, the rows are compared for the largest podcast, the ones of 317
// items, none and one, every tenth and the unknown one. The large library has
// no concurrency to check and takes minutes under the race detector, so make
// check runs it without it.
func TestCatalogRevisionsResultsUnchanged(t *testing.T) {
	for _, c := range []struct {
		episodes, podcasts int
		seed               uint64
	}{{262, 6, 3}, {26271, 70, 4}} {
		t.Run(fmt.Sprintf("%d episodes", c.episodes), func(t *testing.T) {
			if c.episodes > 10000 && (testing.Short() || raceEnabled) {
				t.Skip("load test (make check runs it without the race detector)")
			}
			s := openTest(t, config{dataDir: t.TempDir()})
			fx := seedCatalog(t, s, c.episodes, c.podcasts, c.seed)
			// The fixture holds every case, every item case in the largest podcast.
			kinds := map[string]bool{}
			for _, k := range fx.kinds[fx.accounts[0]][fx.podcasts[0]] {
				kinds[k] = true
			}
			for _, k := range episodeKinds {
				if !kinds[k.name] {
					t.Errorf("the largest podcast has no item with %s", k.name)
				}
			}
			feeds := map[string]bool{}
			for _, k := range fx.feedKinds[fx.accounts[0]] {
				feeds[k] = true
			}
			for _, k := range []string{"revision", "null revision", "no revision", "no metadata"} {
				if !feeds[k] {
					t.Errorf("no podcast has %s", k)
				}
			}
			podcasts := append(slices.Clone(fx.podcasts), "unknown")
			if err := s.Write(context.Background(), func(tx *WriteTx) error {
				for ai, acct := range fx.accounts {
					a := tx.Account(acct)
					for pi, p := range podcasts {
						if ai > 0 || c.episodes < 10000 || pi < 4 || pi%10 == 0 || p == "unknown" {
							oldRows := queryRows(t, tx.tx, oldCatalogRevisionsQuery, acct, p)
							if newRows := queryRows(t, tx.tx, catalogRevisionsQuery, acct, p); !slices.Equal(oldRows, newRows) {
								t.Errorf("revisions of %s in account %d: old rows %d and new rows %d differ", p, ai, len(oldRows), len(newRows))
							}
							oldRows = queryRows(t, tx.tx, oldCatalogStampsQuery, acct, p, acct, p)
							if newRows := queryRows(t, tx.tx, catalogStampsQuery, acct, p, acct, p); !slices.Equal(oldRows, newRows) {
								t.Errorf("stamps of %s in account %d: old rows %d and new rows %d differ", p, ai, len(oldRows), len(newRows))
							}
						}
						revs, err := a.CatalogRevisions(p)
						if err != nil {
							return err
						}
						if want := fx.revisions(acct, p); !maps.Equal(revs, want) {
							t.Errorf("CatalogRevisions(%s) in account %d: %s", p, ai, mapDiff(revs, want))
						}
						stamps, err := a.CatalogStamps(p)
						if err != nil {
							return err
						}
						if want := fx.stamps(acct, p); !maps.Equal(stamps, want) {
							t.Errorf("CatalogStamps(%s) in account %d: %s", p, ai, mapDiff(stamps, want))
						}
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// With the library's statistics on the writer too, the plans start
			// from the podcast.
			analyze(t, s)
			acct, p := fx.accounts[0], fx.podcasts[0]
			if plan := queryPlan(t, s, catalogRevisionsQuery, acct, p); startsFromThePodcast(plan) != nil {
				t.Errorf("analyzed: %v\nplan:\n%s", startsFromThePodcast(plan), strings.Join(plan, "\n"))
			}
			if plan := queryPlan(t, s, catalogStampsQuery, acct, p, acct, p); startsFromThePodcast(plan) != nil {
				t.Errorf("analyzed: %v\nplan:\n%s", startsFromThePodcast(plan), strings.Join(plan, "\n"))
			}
		})
	}
}

// startsFromThePodcast checks that a plan's join starts from the podcast's
// catalog items and looks up each item's record by its full key.
func startsFromThePodcast(plan []string) error {
	join := regexp.MustCompile(`^(SCAN|SEARCH) (c|r)\b`)
	var steps []string
	for _, l := range plan {
		if strings.HasPrefix(l, "SCAN ") {
			return fmt.Errorf("the plan scans: %s", l)
		}
		if join.MatchString(l) {
			steps = append(steps, l)
		}
	}
	if len(steps) != 2 || !strings.HasPrefix(steps[0], "SEARCH c USING ") ||
		!strings.Contains(steps[0], "INDEX catalog_items_podcast (account_id=? AND podcast_id=?)") {
		return fmt.Errorf("the join does not start from catalog_items_podcast: %q", steps)
	}
	if !strings.HasPrefix(steps[1], "SEARCH r USING ") || !strings.Contains(steps[1], "entity_id=? AND field_group=?") {
		return fmt.Errorf("an item's metadata record is not looked up by its key: %q", steps[1])
	}
	return nil
}

// mapDiff describes how two maps differ: their sizes and the first key, in
// order, whose entry differs.
func mapDiff[V comparable](got, want map[string]V) string {
	keys := append(slices.Collect(maps.Keys(got)), slices.Collect(maps.Keys(want))...)
	slices.Sort(keys)
	for _, k := range slices.Compact(keys) {
		g, gok := got[k]
		w, wok := want[k]
		if g != w || gok != wok {
			return fmt.Sprintf("%d entries, want %d; %q is %+v (%v), want %+v (%v)", len(got), len(want), k, g, gok, w, wok)
		}
	}
	return "equal"
}

// queryPlan returns a query's plan on the writer, in a write transaction as
// the production methods run, so it uses the writer's statistics.
func queryPlan(t *testing.T, s *Store, query string, args ...any) []string {
	t.Helper()
	var plan []string
	if err := s.Write(context.Background(), func(tx *WriteTx) error {
		rows, err := tx.tx.Query("EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, parent, unused int64
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				return err
			}
			plan = append(plan, detail)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return plan
}

// queryRows returns a query's rows, each column with its SQL type, sorted.
func queryRows(t *testing.T, tx *sql.Tx, query string, args ...any) []string {
	t.Helper()
	rows, err := tx.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%#v", vals))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

func analyze(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Write(context.Background(), func(tx *WriteTx) error {
		_, err := tx.tx.Exec("ANALYZE")
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// episodeKinds are the shapes of a catalog item's records, with their weights.
var episodeKinds = []struct {
	name   string
	weight int
}{
	{"revision", 70},     // a metadata record with a revision and a fetch time
	{"null revision", 4}, // "revision": null
	{"no revision", 4},   // no revision field
	{"no fetch time", 4}, // a revision, no fetch time
	{"no metadata", 10},  // other records only
	{"no records", 8},    // not even those
}

type catalogFixture struct {
	accounts []string
	podcasts []string // the largest first
	// By account, podcast and episode: what CatalogRevisions and
	// CatalogStamps return (the podcast's own stamp under "feed").
	revs      map[string]map[string]map[string]string
	stampsOf  map[string]map[string]map[string]CatalogStamp
	kinds     map[string]map[string][]string // the items' kinds
	feedKinds map[string][]string            // the podcasts' metadata record kinds
}

func (fx *catalogFixture) revisions(acct, podcast string) map[string]string {
	if m := fx.revs[acct][podcast]; m != nil {
		return m
	}
	return map[string]string{}
}

func (fx *catalogFixture) stamps(acct, podcast string) map[string]CatalogStamp {
	if m := fx.stampsOf[acct][podcast]; m != nil {
		return m
	}
	return map[string]CatalogStamp{}
}

// seedCatalog writes a library of the given number of catalog items in the
// given number of podcasts: the largest first (4,234 items on a large library,
// half of a small one), then one of 317 items (on a large library), an
// empty one and one of a single item; the rest share the remainder unevenly.
// Each item has an identity and a play state record beside its metadata, as
// synced episodes do, and some episode records have no catalog item. A second
// account holds the same podcasts and a tenth of the episode IDs, with other
// values.
func seedCatalog(t *testing.T, s *Store, episodes, podcasts int, seed uint64) *catalogFixture {
	t.Helper()
	r := rand.New(rand.NewPCG(seed, 0x6361))
	sizes := []int{episodes / 2}
	if episodes > 10000 {
		sizes = []int{4234, 317}
	}
	sizes = append(sizes, 0, 1)
	rest := episodes
	for _, n := range sizes {
		rest -= n
	}
	weights := make([]float64, podcasts-len(sizes))
	var sum float64
	for i := range weights {
		weights[i] = r.ExpFloat64() + 0.05
		sum += weights[i]
	}
	given := 0
	for i, w := range weights {
		n := int(float64(rest) * w / sum)
		if i == len(weights)-1 {
			n = rest - given
		}
		given += n
		sizes = append(sizes, n)
	}
	if slices.Max(sizes) != sizes[0] {
		t.Fatalf("seed %d: the first podcast is not the largest: %v", seed, sizes)
	}
	accounts := createAccounts(t, s, 2)
	fx := &catalogFixture{accounts: accounts,
		revs:      map[string]map[string]map[string]string{},
		stampsOf:  map[string]map[string]map[string]CatalogStamp{},
		kinds:     map[string]map[string][]string{},
		feedKinds: map[string][]string{}}
	for i := range sizes {
		fx.podcasts = append(fx.podcasts, fmt.Sprintf("p%03d", i))
	}
	pick := func() string {
		total := 0
		for _, k := range episodeKinds {
			total += k.weight
		}
		n := r.IntN(total)
		for _, k := range episodeKinds {
			if n < k.weight {
				return k.name
			}
			n -= k.weight
		}
		panic("unreachable")
	}
	metadata := func(kind, title string, rev int, fetch int64) (string, bool) {
		m := map[string]any{"title": title, "description": strings.Repeat("d", r.IntN(400)), "fetch_time": fetch,
			"revision": fmt.Sprintf("n-%06d", rev), "media": map[string]any{"url": "https://example.invalid/" + title + ".mp3"}}
		switch kind {
		case "null revision":
			m["revision"] = nil
		case "no revision":
			delete(m, "revision")
		case "no fetch time":
			delete(m, "fetch_time")
		case "no metadata", "no records":
			return "", false
		}
		v, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return string(v), true
	}
	err := s.Write(context.Background(), func(tx *WriteTx) error {
		ctx := context.Background()
		podcast, err := tx.tx.PrepareContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op')`)
		if err != nil {
			return err
		}
		item, err := tx.tx.PrepareContext(ctx, `INSERT INTO catalog_items (account_id, episode_id, podcast_id, first_node, pub_date)
			VALUES (?, ?, ?, 'n-0', ?)`)
		if err != nil {
			return err
		}
		rec, err := tx.tx.PrepareContext(ctx, `INSERT INTO records (account_id, entity, entity_id, field_group, value, provenance)
			VALUES (?, ?, ?, ?, ?, '[]')`)
		if err != nil {
			return err
		}
		node := 0
		for ai, acct := range accounts {
			fx.revs[acct], fx.stampsOf[acct], fx.kinds[acct] = map[string]map[string]string{}, map[string]map[string]CatalogStamp{}, map[string][]string{}
			episode := 0
			for pi, p := range fx.podcasts {
				if _, err := podcast.Exec(acct, p); err != nil {
					return err
				}
				revs, stamps := map[string]string{}, map[string]CatalogStamp{}
				fx.revs[acct][p], fx.stampsOf[acct][p] = revs, stamps
				// The podcast's own metadata record, and a record of another group.
				feedKind := []string{"revision", "null revision", "no revision", "no metadata"}[(pi+ai)%4]
				if pi == 0 {
					feedKind = "revision"
				}
				fx.feedKinds[acct] = append(fx.feedKinds[acct], feedKind)
				node++
				fetch := 1_700_000_000_000 + int64(node)
				if v, ok := metadata(feedKind, p, node, fetch); ok {
					if _, err := rec.Exec(acct, "podcast", p, "metadata", v); err != nil {
						return err
					}
					if feedKind == "revision" {
						stamps["feed"] = CatalogStamp{Revision: fmt.Sprintf("n-%06d", node), FetchTime: fetch}
					}
				}
				if _, err := rec.Exec(acct, "podcast", p, "subscription", `{"state":"subscribed"}`); err != nil {
					return err
				}
				for range sizes[pi] {
					episode++
					if ai == 1 && r.IntN(10) != 0 {
						continue // the other account holds a tenth of the IDs
					}
					e := fmt.Sprintf("e%05d", episode)
					kind := pick()
					fx.kinds[acct][p] = append(fx.kinds[acct][p], kind)
					if _, err := item.Exec(acct, e, p, int64(episode)); err != nil {
						return err
					}
					node++
					fetch := 1_700_000_000_000 + int64(node)
					if v, ok := metadata(kind, e, node, fetch); ok {
						if _, err := rec.Exec(acct, "episode", e, "metadata", v); err != nil {
							return err
						}
						rev := fmt.Sprintf("n-%06d", node)
						switch kind {
						case "revision":
							revs[e], stamps[e] = rev, CatalogStamp{Revision: rev, FetchTime: fetch}
						case "no fetch time":
							revs[e], stamps[e] = rev, CatalogStamp{Revision: rev}
						}
					}
					if kind != "no records" {
						for _, g := range []string{"identity", "play_state"} {
							if _, err := rec.Exec(acct, "episode", e, g, `{"generation_id":"g"}`); err != nil {
								return err
							}
						}
					}
					// Some episode records have no catalog item (a listening placeholder).
					if r.IntN(50) == 0 {
						node++
						if v, ok := metadata("revision", e+"x", node, fetch); ok {
							if _, err := rec.Exec(acct, "episode", e+"x", "metadata", v); err != nil {
								return err
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return fx
}
