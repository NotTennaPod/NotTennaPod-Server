package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// Migration 0026 keeps a podcast's quiet time with its schedule (PROTOCOL
// §9.6) and makes ServerNewest one seek.

// Upgrading a populated database keeps every schedule row and catalog item
// as it was; the rows start without a quiet time, and ServerNewest reads the
// same through the new index.
func TestMigration0026UpgradesAPopulatedDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:25]})
	accounts := createAccounts(t, s, 2)
	const columns = `account_id, podcast_id, next_at, failures, not_before, last_attempt, last_success, last_error,
		status_published_at, status_key, context`
	dump := func(s *Store, table, cols string) []string {
		t.Helper()
		rows, err := s.reader.QueryContext(ctx, `SELECT `+cols+` FROM `+table+` ORDER BY 1, 2`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		names, _ := rows.Columns()
		var out []string
		for rows.Next() {
			vals := make([]any, len(names))
			ptrs := make([]any, len(names))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprintf("%#v", vals))
		}
		return out
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for i, acct := range accounts {
			for p := range 3 {
				podcast := fmt.Sprintf("p1:%d", p)
				if _, err := tx.tx.ExecContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op')`, acct, podcast); err != nil {
					return err
				}
				if _, err := tx.tx.ExecContext(ctx, `INSERT INTO refresh_schedule (`+columns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					acct, podcast, 1000+p, i, 2000*i, nil, 900+p, `{"label":"io"}`, 800, "ok", "g\x00u\x00"); err != nil {
					return err
				}
				for e := range 5 {
					var seen any
					if e%2 == 0 {
						seen = fmt.Sprintf("n-%d", e)
					}
					if _, err := tx.tx.ExecContext(ctx, `INSERT INTO catalog_items (account_id, episode_id, podcast_id, first_node, pub_date, server_seen_node)
						VALUES (?, ?, ?, 'n-0', ?, ?)`, acct, fmt.Sprintf("e:%d:%d", p, e), podcast, 100*e+p+i, seen); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	schedules, items := dump(s, "refresh_schedule", columns), dump(s, "catalog_items", "*")
	s.Close()

	s = openTest(t, config{dataDir: dir})
	if v, _ := s.SchemaVersion(ctx); v != len(embeddedMigrations) || v < 26 {
		t.Fatalf("schema version %d", v)
	}
	if got := dump(s, "refresh_schedule", columns); strings.Join(got, "\n") != strings.Join(schedules, "\n") {
		t.Errorf("schedules after the upgrade:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(schedules, "\n"))
	}
	if got := dump(s, "catalog_items", "*"); strings.Join(got, "\n") != strings.Join(items, "\n") {
		t.Errorf("catalog items changed by the upgrade")
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for i, acct := range accounts {
			a := tx.Account(acct)
			for p := range 3 {
				podcast := fmt.Sprintf("p1:%d", p)
				row, ok, err := a.RefreshSchedule(podcast)
				if err != nil || !ok || row.QuietSince != 0 || row.NextAt != int64(1000+p) || row.LastSuccess != int64(900+p) {
					t.Errorf("%s after the upgrade: %+v %v %v", podcast, row, ok, err)
				}
				// Items 0, 2 and 4 are the server's; the newest is 4.
				if newest, ok, err := a.ServerNewest(podcast); err != nil || !ok || newest != int64(400+p+i) {
					t.Errorf("ServerNewest(%s): %d %v %v", podcast, newest, ok, err)
				}
				row.QuietSince = 12345
				if err := a.PutRefreshSchedule(row); err != nil {
					return err
				}
				if again, _, err := a.RefreshSchedule(podcast); err != nil || again != row {
					t.Errorf("%s with a quiet time: %+v %v", podcast, again, err)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// A staged import's schedules are published with their quiet times (a
// restart and a rebuild: internal/refresh, TestQuietSinceSurvivesRestartAndRebuild).
func TestQuietSinceSurvivesImportPublication(t *testing.T) {
	ctx := context.Background()
	live := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, live, 1)[0]
	const importID = "66666666-6666-4666-8666-666666666666"
	stagingPath, base, _ := stageImport(t, live, account, importID)
	want := RefreshRow{PodcastID: "p1:aa", NextAt: 5000, Failures: 2, NotBefore: 6000, LastAttempt: 4000, LastSuccess: 3000,
		LastError: `{"label":"io"}`, StatusPublishedAt: 3500, StatusKey: "error/io/0", Context: "g\x00u\x00", QuietSince: 2000}
	staging := openTest(t, config{dataDir: filepath.Dir(stagingPath)})
	if err := staging.Write(ctx, func(tx *WriteTx) error { return tx.Account(account).PutRefreshSchedule(want) }); err != nil {
		t.Fatal(err)
	}
	staging.Close()
	if err := live.PublishImport(ctx, stagingPath, account, importID, base, `{"imported":{}}`); err != nil {
		t.Fatal(err)
	}
	check := func(what string) {
		t.Helper()
		if err := live.Write(ctx, func(tx *WriteTx) error {
			got, ok, err := tx.Account(account).RefreshSchedule(want.PodcastID)
			if err != nil || !ok || got != want {
				t.Errorf("%s: %+v %v %v, want %+v", what, got, ok, err, want)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	check("after publication")
}

// serverNewestFixture is a podcast's catalog of 50,000 items from every
// source, as the model has them: by episode, its podcast, effective date and
// whether the server's fetches saw it.
type serverNewestFixture struct {
	account string
	items   map[string]CatalogItem
}

func (fx *serverNewestFixture) newest(podcast string) (int64, bool) {
	var newest int64
	found := false
	for _, it := range fx.items {
		if it.PodcastID == podcast && it.ServerSeenNode != "" && (!found || it.PubDate > newest) {
			newest, found = it.PubDate, true
		}
	}
	return newest, found
}

// seedServerNewest writes the fixture: podcast p1:big with 50,000 items, the
// newest 40,000 of them (whose dates the import gave, or devices observed)
// unseen by the server but every seventh; p1:merged with 2,000 items, the
// server's newer than any of p1:big's; and p1:other, whose server items are
// the newest of all. The rows are written in bulk, by the same rules as the
// model.
func seedServerNewest(t *testing.T, s *Store) *serverNewestFixture {
	t.Helper()
	ctx := context.Background()
	fx := &serverNewestFixture{account: createAccounts(t, s, 1)[0], items: map[string]CatalogItem{}}
	const base = int64(1_300_000_000_000)
	type spec struct {
		podcast             string
		n                   int
		seen                func(i int) bool
		date                func(i int) int64
		firstNode           func(i int) string
		serverNode          string
		seenSQL, dateSQL    string // the same rules in SQL, over n and the base ?2
		firstSQL, serverSQL string
	}
	specs := []spec{
		{"p1:big", 50_000, func(i int) bool { return i < 10_000 || i%7 == 0 && i < 30_000 }, func(i int) int64 { return base + int64(i)*60_000 },
			func(i int) string {
				if i%3 == 0 {
					return "import"
				}
				return "device"
			}, "fetch-1",
			"n < 10000 OR (n % 7 = 0 AND n < 30000)", "?2 + n * 60000", "CASE WHEN n % 3 = 0 THEN 'import' ELSE 'device' END", "'fetch-1'"},
		{"p1:merged", 2_000, func(i int) bool { return i%2 == 0 }, func(i int) int64 { return base + 40_000*60_000 + int64(i)*60_000 },
			func(int) string { return "device" }, "fetch-2",
			"n % 2 = 0", "?2 + 40000 * 60000 + n * 60000", "'device'", "'fetch-2'"},
		{"p1:other", 500, func(int) bool { return true }, func(i int) int64 { return base + 100_000*60_000 + int64(i) },
			func(int) string { return "fetch-3" }, "fetch-3",
			"1", "?2 + 100000 * 60000 + n", "'fetch-3'", "'fetch-3'"},
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		// The rows in bulk: the capture is suspended, as for an import.
		if err := tx.SuspendViewCapture(fx.account); err != nil {
			return err
		}
		for _, sp := range specs {
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op')`, fx.account, sp.podcast); err != nil {
				return err
			}
			if _, err := tx.tx.ExecContext(ctx, `WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?4)
				INSERT INTO catalog_items (account_id, episode_id, podcast_id, first_node, pub_date, server_seen_node)
				SELECT ?1, printf('%s:e%06d', ?3, n), ?3, `+sp.firstSQL+`, `+sp.dateSQL+`, CASE WHEN `+sp.seenSQL+` THEN `+sp.serverSQL+` END FROM i`,
				fx.account, base, sp.podcast, sp.n); err != nil {
				return err
			}
			for i := range sp.n {
				it := CatalogItem{EpisodeID: fmt.Sprintf("%s:e%06d", sp.podcast, i), PodcastID: sp.podcast, FirstNode: sp.firstNode(i), PubDate: sp.date(i)}
				if sp.seen(i) {
					it.ServerSeenNode = sp.serverNode
				}
				fx.items[it.EpisodeID] = it
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The rows are the model's.
	rows, err := s.reader.QueryContext(ctx, `SELECT episode_id, podcast_id, first_node, pub_date, COALESCE(server_seen_node, '')
		FROM catalog_items WHERE account_id = ?`, fx.account)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var it CatalogItem
		if err := rows.Scan(&it.EpisodeID, &it.PodcastID, &it.FirstNode, &it.PubDate, &it.ServerSeenNode); err != nil {
			t.Fatal(err)
		}
		if fx.items[it.EpisodeID] != it {
			t.Fatalf("row %+v, model %+v", it, fx.items[it.EpisodeID])
		}
		n++
	}
	if err := rows.Err(); err != nil || n != len(fx.items) {
		t.Fatalf("%d rows, model %d (%v)", n, len(fx.items), err)
	}
	return fx
}

// serverNewestWork returns ServerNewest's answer for a podcast and the
// SQLite virtual-machine instructions it took, on the writer as a server
// fetch runs it; without the index, the instructions the old plan takes.
func serverNewestWork(t *testing.T, s *Store, account, podcast string, withoutIndex bool) (int64, bool, int64) {
	t.Helper()
	ctx := context.Background()
	conn, err := s.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	steps := countSteps(t, conn)
	defer vmSteps.Range(func(k, v any) bool {
		if v == steps {
			vmSteps.Delete(k)
		}
		return true
	})
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if withoutIndex {
		if _, err := tx.ExecContext(ctx, `DROP INDEX catalog_items_server_newest`); err != nil {
			t.Fatal(err)
		}
	}
	w := &WriteTx{ctx: ctx, tx: tx, store: s, accounts: map[string]*AccountTx{}}
	before := steps.Load()
	newest, ok, err := w.Account(account).ServerNewest(podcast)
	if err != nil {
		t.Fatal(err)
	}
	return newest, ok, steps.Load() - before
}

// serverNewestBound is ServerNewest's budget in instructions: one seek,
// whatever the podcast's items.
const serverNewestBound = 200

// ServerNewest (the newest date the server's own fetches saw, read by every
// discovering fetch, PROTOCOL §9.5) is one seek of the partial index on a
// podcast of 50,000 items from every source, after podcast and episode
// merges, date corrections and items the server sees later: within a fixed
// budget of instructions, where the plan before the index walked every item,
// and always the model's answer. In each state of the planner's statistics.
func TestServerNewestIsOneSeek(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	fx := seedServerNewest(t, s)
	check := func(state string) {
		t.Helper()
		for _, p := range []string{"p1:big", "p1:merged", "p1:other", "p1:none"} {
			want, wantOK := fx.newest(p)
			got, ok, work := serverNewestWork(t, s, fx.account, p, false)
			if got != want || ok != wantOK {
				t.Errorf("%s: ServerNewest(%s) %d %v, want %d %v", state, p, got, ok, want, wantOK)
			}
			if work > serverNewestBound {
				t.Errorf("%s: ServerNewest(%s) took %d instructions, over its budget of %d", state, p, work, serverNewestBound)
			}
			if p == "p1:big" {
				t.Logf("%s: ServerNewest(%s): %d instructions", state, p, work)
			}
		}
		plan := queryPlan(t, s, `SELECT MAX(pub_date) FROM catalog_items WHERE account_id = ? AND podcast_id = ? AND server_seen_node IS NOT NULL`,
			fx.account, "p1:big")
		if len(plan) != 1 || !strings.Contains(plan[0], "INDEX catalog_items_server_newest (account_id=? AND podcast_id=?)") {
			t.Errorf("%s: plan %q", state, plan)
		}
	}
	check("seeded")
	// The plan before the index walks the podcast's items.
	if _, _, old := serverNewestWork(t, s, fx.account, "p1:big", true); old < 50_000 {
		t.Errorf("without the index: %d instructions; the budget does not tell the plans apart", old)
	} else {
		t.Logf("without the index: %d instructions", old)
	}
	// A podcast merge moves p1:merged's items, whose server items are newer.
	err := s.Write(ctx, func(tx *WriteTx) error {
		a := tx.Account(fx.account)
		if err := a.MoveCatalogItems("p1:merged", "p1:big"); err != nil {
			return err
		}
		for id, it := range fx.items {
			if it.PodcastID == "p1:merged" {
				it.PodcastID = "p1:big"
				fx.items[id] = it
			}
		}
		// Episode merges remove the newest server item and one other.
		newest, _ := fx.newest("p1:big")
		for id, it := range fx.items {
			if it.PodcastID == "p1:big" && it.ServerSeenNode != "" && (it.PubDate == newest || id == "p1:big:e000007") {
				if err := a.DeleteCatalogItem(id); err != nil {
					return err
				}
				delete(fx.items, id)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	check("after merges")
	// Date corrections: a device-only item becomes the newest of all (not
	// the server's), a server item moves back, and another forward.
	err = s.Write(ctx, func(tx *WriteTx) error {
		a := tx.Account(fx.account)
		correct := func(id string, date int64, seenBy string) error {
			it := fx.items[id]
			it.PubDate = date
			if seenBy != "" {
				it.ServerSeenNode = seenBy
			}
			fx.items[id] = it
			return a.PutCatalogItem(it)
		}
		newest, _ := fx.newest("p1:big")
		for id, it := range fx.items {
			if it.PodcastID == "p1:big" && it.ServerSeenNode != "" && it.PubDate == newest {
				if err := correct(id, 1, ""); err != nil {
					return err
				}
			}
		}
		if err := correct("p1:big:e049999", 2_000_000_000_000, ""); err != nil {
			return err
		}
		return correct("p1:big:e000123", newest-1, "")
	})
	if err != nil {
		t.Fatal(err)
	}
	check("after date corrections")
	// The server's fetch sees an item devices saw first, with a later date.
	err = s.Write(ctx, func(tx *WriteTx) error {
		it := fx.items["p1:big:e049998"]
		it.ServerSeenNode, it.PubDate = "fetch-4", 1_900_000_000_000
		fx.items[it.EpisodeID] = it
		return tx.Account(fx.account).PutCatalogItem(it)
	})
	if err != nil {
		t.Fatal(err)
	}
	check("after a server fetch")
	analyze(t, s)
	check("analyzed")
	s.Close()
	s = openTest(t, config{dataDir: dir})
	check("reopened analyzed")
}
