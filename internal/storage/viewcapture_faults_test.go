package storage

// The read model's change capture under faults, at open, across bulk
// rewrites (within a log lineage, (generation, tick) never decreases; a bulk
// rewrite raises the generation of its accounts only), at an account's
// deletion, and the audit that checks every test's writes.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	capA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	capB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

// newCaptureDir makes a closed data directory whose accounts A and B have
// the seeded library (the same contents every time).
func newCaptureDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s := openTest(t, config{dataDir: dir})
	if err := s.Write(context.Background(), func(tx *WriteTx) error {
		if _, err := tx.CreateAccountWithIDs(capA, "11111111-1111-4111-8111-111111111111"); err != nil {
			return err
		}
		_, err := tx.CreateAccountWithIDs(capB, "22222222-2222-4222-8222-222222222222")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	seedViews(t, s, capA)
	seedViews(t, s, capB)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// bulkRewrite rewrites the account's view inputs with capture suspended;
// pause runs at "inside", after the rewrite.
func bulkRewrite(tx *WriteTx, account string, pause func(string) error) error {
	if err := tx.SuspendViewCapture(account); err != nil {
		return err
	}
	for _, q := range []string{`UPDATE records SET provenance = '["bulk"]' WHERE account_id = ?`,
		`UPDATE catalog_items SET pub_date = pub_date + 1 WHERE account_id = ?`} {
		if _, err := tx.tx.ExecContext(tx.ctx, q, account); err != nil {
			return err
		}
	}
	return pause("inside")
}

// runCaptureChild is the "capture" child: it runs bulkRewrite on account
// NTP_CAPTURE_ACCOUNT and blocks at NTP_CAPTURE_POINT (a step of the
// suspension, "inside", "beforeCommit", or "committed" after the commit).
func runCaptureChild(ctx context.Context, cfg config) {
	s, err := open(ctx, cfg)
	if err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}
	point, account := os.Getenv("NTP_CAPTURE_POINT"), os.Getenv("NTP_CAPTURE_ACCOUNT")
	pause := func(at string) error {
		if at == point {
			return block("paused " + at)
		}
		return nil
	}
	s.hooks.viewCapture = pause
	s.hooks.beforeCommit = func() error { return pause("beforeCommit") }
	if err := s.Write(ctx, func(tx *WriteTx) error { return bulkRewrite(tx, account, pause) }); err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}
	pause("committed")
	fmt.Println("error: not paused")
	os.Exit(1)
}

// captureState is what a fault may or may not change.
type captureSnapshot struct {
	inputsA, inputsB map[entityKey]string
	stampsA, stampsB map[entityKey]int64
	clockA, clockB   ViewClock
}

func snapshotCapture(t *testing.T, s *Store) captureSnapshot {
	t.Helper()
	var c captureSnapshot
	c.inputsA, c.inputsB = viewInputs(t, s.reader, capA), viewInputs(t, s.reader, capB)
	c.stampsA, c.clockA = stampsOf(t, s.reader, capA)
	c.stampsB, c.clockB = stampsOf(t, s.reader, capB)
	return c
}

func (c captureSnapshot) diff(o captureSnapshot) string {
	var out []string
	if ch := changedInputs(c.inputsA, o.inputsA); len(ch) > 0 {
		out = append(out, "A's inputs of "+ch.String())
		for k := range ch {
			out = append(out, fmt.Sprintf("%v: %q against %q", k, c.inputsA[k], o.inputsA[k]))
		}
	}
	if ch := changedInputs(c.inputsB, o.inputsB); len(ch) > 0 {
		out = append(out, "B's inputs of "+ch.String())
	}
	if !maps.Equal(c.stampsA, o.stampsA) {
		out = append(out, fmt.Sprintf("A's %d stamps against %d", len(c.stampsA), len(o.stampsA)))
	}
	if !maps.Equal(c.stampsB, o.stampsB) {
		out = append(out, fmt.Sprintf("B's %d stamps against %d", len(c.stampsB), len(o.stampsB)))
	}
	if c.clockA != o.clockA || c.clockB != o.clockB {
		out = append(out, fmt.Sprintf("clocks %+v %+v against %+v %+v", c.clockA, c.clockB, o.clockA, o.clockB))
	}
	return strings.Join(out, "; ")
}

// checkCaptureInstalled: the schema is exact and every capture trigger has
// its canonical text.
func checkCaptureInstalled(t *testing.T, q rowQuerier) {
	t.Helper()
	ctx := context.Background()
	if err := checkSchema(ctx, q, embeddedMigrations); err != nil {
		t.Fatal(err)
	}
	have, err := readSchema(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range captureTriggers {
		if o, ok := have["trigger\x00"+c.name]; !ok || o.sql != c.sql {
			t.Fatalf("%s is missing or different", c.name)
		}
	}
}

// checkNextWriteCaptured: a change made now is stamped.
func checkNextWriteCaptured(t *testing.T, s *Store) {
	t.Helper()
	_, before := stampsOf(t, s.reader, capA)
	if err := s.Write(context.Background(), func(tx *WriteTx) error {
		return tx.Account(capA).PutRecord(RecordRow{"episode", "e:010", "state", fmt.Sprintf(`{"after":%d}`, before.Tick), `[]`})
	}); err != nil {
		t.Fatal(err)
	}
	stamps, after := stampsOf(t, s.reader, capA)
	if after.Tick != before.Tick+1 || after.Generation != before.Generation || stamps[ep("e:010")] != after.Tick {
		t.Fatalf("the next write: clock %+v → %+v, stamp %d", before, after, stamps[ep("e:010")])
	}
}

// An error or the process's death at each step of a suspended
// transaction (after the triggers are dropped, inside the bulk rewrite,
// after they are created again, after the clock is raised, just before
// COMMIT, just after it) leaves the triggers present and the schema exact
// with no repair at the next open, the data and the clock either both
// before or both after the transaction, and the next write captured.
func TestSuspendedCaptureSurvivesFaults(t *testing.T) {
	ctx := context.Background()
	var before, after captureSnapshot
	{
		s := openTest(t, config{dataDir: newCaptureDir(t)})
		before = snapshotCapture(t, s)
		if err := s.Write(ctx, func(tx *WriteTx) error { return bulkRewrite(tx, capA, func(string) error { return nil }) }); err != nil {
			t.Fatal(err)
		}
		after = snapshotCapture(t, s)
		s.Close()
		if after.clockA.Generation != before.clockA.Generation+1 || after.clockB != before.clockB || len(after.stampsA) != 0 {
			t.Fatalf("the rewrite: %s", before.diff(after))
		}
	}
	fault := errors.New("injected fault")
	for _, point := range []string{"dropped", "inside", "recreated", "clocked", "beforeCommit", "committed"} {
		for _, how := range []string{"error", "exit"} {
			if how == "error" && point == "committed" {
				continue
			}
			t.Run(point+" "+how, func(t *testing.T) {
				dir := newCaptureDir(t)
				if how == "error" {
					s := openTest(t, config{dataDir: dir})
					hook := func(at string) error {
						if at == point {
							return fault
						}
						return nil
					}
					s.hooks.viewCapture = hook
					s.hooks.beforeCommit = func() error { return hook("beforeCommit") }
					if err := s.Write(ctx, func(tx *WriteTx) error { return bulkRewrite(tx, capA, hook) }); !errors.Is(err, fault) {
						t.Fatalf("the write: %v", err)
					}
					s.hooks = hooks{}
					checkCaptureInstalled(t, s.writer)
					if d := snapshotCapture(t, s).diff(before); d != "" {
						t.Fatalf("after the error: %s", d)
					}
					checkNextWriteCaptured(t, s)
					s.Close()
				} else {
					c := startChild(t, "capture", dir, "NTP_CAPTURE_POINT="+point, "NTP_CAPTURE_ACCOUNT="+capA)
					c.waitFor(t, "paused "+point)
					c.kill()
				}
				s := openTest(t, config{dataDir: dir})
				checkCaptureInstalled(t, s.writer)
				got := snapshotCapture(t, s)
				want := before
				if point == "committed" {
					want = after
				}
				if how == "error" { // the next write's change is the only one since
					want.inputsA = maps.Clone(want.inputsA)
					delete(got.inputsA, ep("e:010"))
					delete(want.inputsA, ep("e:010"))
					got.stampsA, want.stampsA, got.clockA, want.clockA = nil, nil, ViewClock{}, ViewClock{}
				}
				if d := got.diff(want); d != "" {
					t.Fatalf("after reopening: %s", d)
				}
				checkNextWriteCaptured(t, s)
			})
		}
	}
}

// editDatabase runs statements on a closed data directory's database.
func editDatabase(t *testing.T, dir string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// rawState is a closed database's triggers and clocks.
func rawState(t *testing.T, dir string) string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var out []string
	queries := []string{`SELECT type || ' ' || name || ' ' || COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name`,
		`SELECT account_id || ' ' || tick || ' ' || generation FROM view_clock ORDER BY account_id`}
	if ok, err := hasTable(context.Background(), db, "view_changes"); err != nil {
		t.Fatal(err)
	} else if ok {
		queries = append(queries, `SELECT account_id || ' ' || entity || ' ' || entity_id || ' ' || tick FROM view_changes ORDER BY 1`)
	}
	for _, q := range queries {
		rows, err := queryStrings(context.Background(), db, q)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rows...)
	}
	return strings.Join(out, "\n")
}

// captureLog collects what the test logs through slog.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func captureSQL(name string) string {
	for _, c := range captureTriggers {
		if c.name == name {
			return c.sql
		}
	}
	panic(name)
}

// Opening a server's or a staging database that lacks capture
// triggers, and nothing else, creates them again and raises every account's
// generation and tick, with a warning; any other difference (a capture
// trigger with other text, a trigger of the database's own even with a
// capture name, a missing index) refuses to start and changes nothing; a
// copy never repairs.
func TestOpenRepairsOnlyAbsentCaptureTriggers(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name    string
		dropped []string
	}{
		{"one missing, and an account without a clock", []string{"view_capture_catalog_update"}},
		{"node ones missing", []string{"view_capture_node_order_insert", "view_capture_node_order_update", "view_capture_node_order_delete"}},
		{"all missing", func() (out []string) {
			for _, c := range captureTriggers {
				out = append(out, c.name)
			}
			return out
		}()},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := newCaptureDir(t)
			s := openTest(t, config{dataDir: dir})
			clockless := createAccounts(t, s, 1)[0] // no change yet: no clock
			before := snapshotCapture(t, s)
			if _, c := stampsOf(t, s.reader, clockless); c != (ViewClock{}) {
				t.Fatal("precondition: the new account has no clock")
			}
			s.Close()
			var stmts []string
			for _, n := range c.dropped {
				stmts = append(stmts, "DROP TRIGGER "+n)
			}
			editDatabase(t, dir, stmts...)
			log := captureLog(t)
			s = openTest(t, config{dataDir: dir})
			checkCaptureInstalled(t, s.writer)
			got := snapshotCapture(t, s)
			if changed := changedInputs(before.inputsA, got.inputsA); len(changed) > 0 {
				t.Fatalf("the repair changed %v", changed)
			}
			for _, pair := range [][2]ViewClock{{before.clockA, got.clockA}, {before.clockB, got.clockB}} {
				if pair[1].Generation != pair[0].Generation+1 || pair[1].Tick != pair[0].Tick+1 {
					t.Errorf("a clock went from %+v to %+v", pair[0], pair[1])
				}
			}
			if len(got.stampsA)+len(got.stampsB) != 0 {
				t.Errorf("stamps of the old generation are left")
			}
			if _, c := stampsOf(t, s.reader, clockless); c != (ViewClock{Tick: 1, Generation: 1}) {
				t.Errorf("the account without a clock: %+v after the repair", c)
			}
			if !strings.Contains(log.String(), "lacked read-model capture triggers") || !strings.Contains(log.String(), c.dropped[0]) {
				t.Errorf("the warning: %s", log)
			}
			checkNextWriteCaptured(t, s)
			// The restored triggers capture.
			if err := s.Write(ctx, func(tx *WriteTx) error {
				return tx.Account(capA).PutCatalogItem(CatalogItem{EpisodeID: "e:000", PodcastID: "p:0", FirstNode: "n:0", PubDate: 1})
			}); err != nil {
				t.Fatal(err)
			}
			if stamps, clock := stampsOf(t, s.reader, capA); stamps[ep("e:000")] != clock.Tick {
				t.Errorf("a catalog date change after the repair: %v", stamps)
			}
			// A second open finds nothing to repair.
			s.Close()
			again := rawState(t, dir)
			s = openTest(t, config{dataDir: dir})
			s.Close()
			if rawState(t, dir) != again {
				t.Errorf("a second open changed the clock or schema")
			}
		})
	}
	same := captureSQL("view_capture_records_update")
	for _, c := range []struct {
		name  string
		stmts []string
		want  string
	}{
		{"a weakened capture trigger", []string{"DROP TRIGGER view_capture_records_update",
			strings.Replace(same, "NEW.value IS NOT OLD.value OR NEW.provenance IS NOT OLD.provenance OR ", "NEW.value IS NOT OLD.value OR ", 1)},
			"has a different trigger view_capture_records_update"},
		{"a forged capture trigger", []string{"DROP TRIGGER view_capture_records_insert",
			"CREATE TRIGGER view_capture_records_insert AFTER INSERT ON records BEGIN SELECT 1; END"},
			"has a different trigger view_capture_records_insert"},
		{"an extra capture-named trigger", []string{"CREATE TRIGGER view_capture_records_extra AFTER INSERT ON records BEGIN SELECT 1; END"},
			"has trigger view_capture_records_extra of its own"},
		{"a missing index", []string{"DROP INDEX podcasts_by_creating_op"}, "lacks index podcasts_by_creating_op"},
		{"a missing capture trigger and index", []string{"DROP TRIGGER view_capture_records_delete", "DROP INDEX view_changes_by_tick"},
			"lacks index view_changes_by_tick"},
		{"a missing capture trigger and a trigger of its own", []string{"DROP TRIGGER view_capture_records_delete",
			"CREATE TRIGGER keep_tokens AFTER DELETE ON auth_tokens BEGIN SELECT 1; END"}, "has trigger keep_tokens of its own"},
		{"a missing capture table", []string{"DROP TABLE view_changes"}, "lacks table view_changes"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := newCaptureDir(t)
			editDatabase(t, dir, c.stmts...)
			edited := rawState(t, dir)
			for _, mode := range []storeMode{modeServer, modeStaging} {
				cfg := config{dataDir: dir, syncDir: noSync, mode: mode}
				if mode == modeStaging {
					cfg.serverID, cfg.keys = "33333333-3333-4333-8333-333333333333", []ReceiptKey{{ID: "k", Secret: make([]byte, 32)}}
				}
				s, err := open(ctx, cfg)
				if err == nil {
					s.Close()
					t.Fatalf("mode %d: the database opened", mode)
				}
				if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), c.want) {
					t.Fatalf("mode %d: %v", mode, err)
				}
			}
			if rawState(t, dir) != edited {
				t.Fatal("a refused open changed the database")
			}
		})
	}
	t.Run("a copy", func(t *testing.T) {
		dir := newCaptureDir(t)
		editDatabase(t, dir, "DROP TRIGGER view_capture_podcasts_insert")
		edited := rawState(t, dir)
		s, err := OpenCopy(ctx, dir, filepath.Join(t.TempDir(), "work"))
		if err == nil {
			s.Close()
			t.Fatal("a copy lacking a capture trigger opened")
		}
		if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "lacks trigger view_capture_podcasts_insert") {
			t.Fatal(err)
		}
		if rawState(t, dir) != edited {
			t.Fatal("opening a copy changed the original")
		}
	})
	t.Run("a staging database", func(t *testing.T) {
		live := openTest(t, config{dataDir: newCaptureDir(t)})
		id, err := live.ServerID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := live.ReceiptKeys(ctx)
		if err != nil {
			t.Fatal(err)
		}
		head, err := live.Head(ctx, capA)
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(t.TempDir(), "staging")
		st, err := OpenStaging(ctx, dir, id, keys)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Write(ctx, func(tx *WriteTx) error { return tx.MirrorAccount(capA, "11111111-1111-4111-8111-111111111111", head) }); err != nil {
			t.Fatal(err)
		}
		seedViews(t, st, capA)
		_, before := stampsOf(t, st.reader, capA)
		st.Close()
		editDatabase(t, dir, "DROP TRIGGER view_capture_episodes_insert")
		if st, err = OpenStaging(ctx, dir, id, keys); err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		checkCaptureInstalled(t, st.writer)
		if _, after := stampsOf(t, st.reader, capA); after.Generation != before.Generation+1 {
			t.Fatalf("the staging clock went from %+v to %+v", before, after)
		}
	})
}

// Each bulk rewrite (an import's
// publication, a staged rebuild's publication, a rebuild's clearing, an
// account's deletion, the repair at open) raises the generation and tick of
// the accounts it rewrites and clears their stamps, and leaves every other
// account's clock and stamps as they were; the account's read key (head,
// generation, tick) is never one it had before; and later changes go on
// from there.
func TestBulkRewritesRaiseOnlyTheirAccountsGeneration(t *testing.T) {
	ctx := context.Background()
	type key struct {
		head  string // log, epoch, seq and digest
		clock ViewClock
	}
	readKey := func(s *Store, account string) key {
		var k key
		if err := s.Read(ctx, func(r *ReadTx) error {
			h, err := r.Account(account).Head()
			if err != nil {
				return err
			}
			k.head = fmt.Sprintf("%s %d %d %x", h.LogID, h.Epoch, h.Seq, h.Digest)
			k.clock, err = r.Account(account).ViewClock()
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return k
	}
	for _, c := range []struct {
		name    string
		target  func(s *Store) string // prepares, returns the rewritten account
		rewrite func(s *Store, account string) *Store
	}{
		{"import publication", func(s *Store) string {
			var id string
			if err := s.Write(ctx, func(tx *WriteTx) error {
				a, err := tx.CreateAccount()
				id = a.ID
				if err != nil {
					return err
				}
				// An account without a library may have records (none today; the clock still moves).
				return tx.Account(id).PutRecord(RecordRow{"account", id, "queue_policy", `{}`, `[]`})
			}); err != nil {
				t.Fatal(err)
			}
			return id
		}, func(s *Store, account string) *Store {
			publishTestImport(t, s, account)
			return s
		}},
		{"staged publication", func(*Store) string { return capA }, func(s *Store, account string) *Store {
			dir := filepath.Join(t.TempDir(), "staged")
			st, err := s.StageCopy(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Write(ctx, func(tx *WriteTx) error {
				return tx.Account(account).PutRecord(RecordRow{"episode", "e:002", "state", `{"staged":true}`, `[]`})
			}); err != nil {
				t.Fatal(err)
			}
			base, err := st.Head(ctx, account)
			if err != nil {
				t.Fatal(err)
			}
			st.Close()
			if err := s.PublishStaged(ctx, filepath.Join(dir, databaseName), account, base, nil); err != nil {
				t.Fatal(err)
			}
			if got := viewInputs(t, s.reader, account)[ep("e:002")]; !strings.Contains(got, `{"staged":true}`) {
				t.Fatalf("the staged change was not published: %s", got)
			}
			return s
		}},
		{"rebuild clearing", func(*Store) string { return capA }, func(s *Store, account string) *Store {
			if err := s.Write(ctx, func(tx *WriteTx) error {
				a := tx.Account(account)
				if err := a.ClearDerived(); err != nil {
					return err
				}
				// Clearing twice in a transaction (a recovery replays twice) declares once.
				if err := a.ClearDerived(); err != nil {
					return err
				}
				return a.DropRebuildCopies()
			}); err != nil {
				t.Fatal(err)
			}
			return s
		}},
		{"repair at open", func(*Store) string { return capA }, func(s *Store, account string) *Store {
			dir := s.Dir()
			s.Close()
			editDatabase(t, dir, "DROP TRIGGER view_capture_records_insert")
			return openTest(t, config{dataDir: dir})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := openTest(t, config{dataDir: newCaptureDir(t)})
			account := c.target(s)
			// Both accounts have moved on before the rewrite.
			for _, id := range []string{account, capB} {
				if err := s.Write(ctx, func(tx *WriteTx) error {
					return tx.Account(id).PutRecord(RecordRow{"episode", "e:before", "state", `{}`, `[]`})
				}); err != nil {
					t.Fatal(err)
				}
			}
			before, otherBefore := readKey(s, account), readKey(s, capB)
			otherStamps, _ := stampsOf(t, s.reader, capB)
			s = c.rewrite(s, account)
			after, otherAfter := readKey(s, account), readKey(s, capB)
			if after.clock.Generation != before.clock.Generation+1 || after.clock.Tick <= before.clock.Tick {
				t.Errorf("the account's clock went from %+v to %+v", before.clock, after.clock)
			}
			if stamps, _ := stampsOf(t, s.reader, account); len(stamps) != 0 {
				t.Errorf("%d stamps of the older generation are left", len(stamps))
			}
			if after == before {
				t.Error("the account's read key did not change")
			}
			stamps, _ := stampsOf(t, s.reader, capB)
			switch {
			case c.name == "repair at open": // every account is rewritten
				if otherAfter.clock.Generation != otherBefore.clock.Generation+1 {
					t.Errorf("the other account's clock went from %+v to %+v", otherBefore.clock, otherAfter.clock)
				}
			case otherAfter != otherBefore || !maps.Equal(stamps, otherStamps):
				t.Errorf("the other account's key or stamps changed: %+v → %+v", otherBefore, otherAfter)
			}
			// Later changes go on from the new generation.
			if err := s.Write(ctx, func(tx *WriteTx) error {
				return tx.Account(account).PutRecord(RecordRow{"episode", "e:after", "state", `{}`, `[]`})
			}); err != nil {
				t.Fatal(err)
			}
			if later := readKey(s, account); later.clock.Generation != after.clock.Generation || later.clock.Tick != after.clock.Tick+1 {
				t.Errorf("after a later change: %+v (was %+v)", later.clock, after.clock)
			}
		})
	}
}

// publishTestImport publishes a staged import of one podcast into the empty
// account, as TestPublishImport does.
func publishTestImport(t *testing.T, live *Store, account string) {
	t.Helper()
	ctx := context.Background()
	const importID = "66666666-6666-4666-8666-666666666666"
	if _, _, err := reserve(t, live, account, importID, "ff"); err != nil {
		t.Fatal(err)
	}
	var base Head
	var namespace string
	if err := live.Write(ctx, func(tx *WriteTx) error {
		if err := tx.Account(account).SetImportStatus(importID, "processing", ""); err != nil {
			return err
		}
		var err error
		if base, err = tx.Account(account).Head(); err != nil {
			return err
		}
		namespace, err = tx.Account(account).Namespace()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stagingDir := t.TempDir()
	staging := openTest(t, config{dataDir: stagingDir})
	if err := staging.Write(ctx, func(tx *WriteTx) error {
		if err := tx.MirrorAccount(account, namespace, base); err != nil {
			return err
		}
		a := tx.Account(account)
		if err := a.InsertNode("77777777-7777-4777-8777-777777777777", "decision", 1, nil); err != nil {
			return err
		}
		if err := a.CreatePodcast("p1:aa", "77777777-7777-4777-8777-777777777777"); err != nil {
			return err
		}
		if err := a.PutRecord(RecordRow{"podcast", "p1:aa", "subscription", `{"state":"subscribed"}`, `[]`}); err != nil {
			return err
		}
		_, _, err := a.AppendCommit(record(account, 1))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	staging.Close()
	if err := live.PublishImport(ctx, filepath.Join(stagingDir, databaseName), account, importID, base, `{}`); err != nil {
		t.Fatal(err)
	}
	if got := viewInputs(t, live.reader, account)[pod("p1:aa")]; got == "" {
		t.Fatal("the import was not published")
	}
}

// The bulk side of the capture's work: an import's and a staged rebuild's publications write
// no stamp and one clock row (their account's).
func TestBulkPublicationsWriteNoStamps(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: newCaptureDir(t)})
	reset, read := countCaptureWrites(t, s)
	var fresh string
	if err := s.Write(ctx, func(tx *WriteTx) error {
		a, err := tx.CreateAccount()
		fresh = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reset()
	publishTestImport(t, s, fresh)
	// publishTestImport's own writes (the reservation) write no capture rows.
	if clock, stamps := read(); clock != 1 || stamps != 0 {
		t.Errorf("an import's publication wrote %d clock and %d stamp rows", clock, stamps)
	}
	dir := filepath.Join(t.TempDir(), "staged")
	st, err := s.StageCopy(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	base, err := st.Head(ctx, capA)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	reset()
	if err := s.PublishStaged(ctx, filepath.Join(dir, databaseName), capA, base, nil); err != nil {
		t.Fatal(err)
	}
	if clock, stamps := read(); clock != 1 || stamps != 0 {
		t.Errorf("a staged publication wrote %d clock and %d stamp rows", clock, stamps)
	}
	reset()
	if err := s.Write(ctx, func(tx *WriteTx) error { return tx.DeleteAccount(capA) }); err != nil {
		t.Fatal(err)
	}
	if clock, stamps := read(); clock != 0 || stamps != 0 {
		t.Errorf("an account's deletion wrote %d clock and %d stamp rows", clock, stamps)
	}
}

// Deleting an account leaves no clock or stamp of it, the other
// accounts' untouched; recreated with the same ID, it starts at (0, 0) under
// a new log, and its changes are captured from there.
func TestAccountDeletionLeavesNoViewState(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: newCaptureDir(t)})
	headBefore, err := s.Head(ctx, capA)
	if err != nil {
		t.Fatal(err)
	}
	otherStamps, otherClock := stampsOf(t, s.reader, capB)
	if stamps, clock := stampsOf(t, s.reader, capA); len(stamps) == 0 || clock.Tick == 0 {
		t.Fatal("precondition: the account has stamps")
	}
	if err := s.Write(ctx, func(tx *WriteTx) error { return tx.DeleteAccount(capA) }); err != nil {
		t.Fatal(err)
	}
	count := func(q string) int {
		var n int
		if err := s.reader.QueryRow(q, capA).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(`SELECT (SELECT COUNT(*) FROM view_clock WHERE account_id = ?1) + (SELECT COUNT(*) FROM view_changes WHERE account_id = ?1)`); n != 0 {
		t.Fatalf("%d view rows of the deleted account", n)
	}
	if stamps, clock := stampsOf(t, s.reader, capB); clock != otherClock || !maps.Equal(stamps, otherStamps) {
		t.Fatal("the other account's clock or stamps changed")
	}
	checkCaptureInstalled(t, s.writer)
	if err := s.Write(ctx, func(tx *WriteTx) error {
		_, err := tx.CreateAccountWithIDs(capA, "11111111-1111-4111-8111-111111111111")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var clock ViewClock
	var head Head
	if err := s.Read(ctx, func(r *ReadTx) error {
		var err error
		if clock, err = r.Account(capA).ViewClock(); err != nil {
			return err
		}
		head, err = r.Account(capA).Head()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if clock != (ViewClock{}) || head.LogID == headBefore.LogID {
		t.Fatalf("the recreated account: clock %+v, log %s (was %s)", clock, head.LogID, headBefore.LogID)
	}
	checkNextWriteCaptured(t, s)
	// Deleting it again inside a transaction that rolls back keeps it whole.
	if err := s.Write(ctx, func(tx *WriteTx) error {
		if err := tx.DeleteAccount(capA); err != nil {
			return err
		}
		return errRollback
	}); !errors.Is(err, errRollback) {
		t.Fatal(err)
	}
	if n := count(`SELECT COUNT(*) FROM view_clock WHERE account_id = ?`); n != 1 {
		t.Fatal("a rolled-back deletion removed the clock")
	}
	checkCaptureInstalled(t, s.writer)
}

// The audit runs in every test of the package, and refuses a transaction
// whose change of an undeclared account is not stamped, whose declared
// account's generation did not rise, or that moves another account's
// generation.
func TestCaptureAuditRefusesUncapturedChanges(t *testing.T) {
	if !viewAuditOn.Load() {
		t.Fatal("the capture audit is off")
	}
	ctx := context.Background()
	s := openTest(t, config{dataDir: newCaptureDir(t)})
	for _, c := range []struct {
		name  string
		write func(tx *WriteTx) error
		want  string
	}{
		{"a change with a trigger missing", func(tx *WriteTx) error {
			if _, err := tx.tx.ExecContext(ctx, `DROP TRIGGER view_capture_records_update`); err != nil {
				return err
			}
			return tx.Account(capA).PutRecord(RecordRow{"episode", "e:000", "metadata", `{"x":1}`, `[]`})
		}, "the change of episode e:000 of account " + capA + " is not stamped"},
		{"another account's change while suspended", func(tx *WriteTx) error {
			if err := tx.SuspendViewCapture(capA); err != nil {
				return err
			}
			return tx.Account(capB).PutRecord(RecordRow{"episode", "e:000", "metadata", `{"x":1}`, `[]`})
		}, "the change of episode e:000 of account " + capB + " is not stamped"},
		{"a declared account's generation kept", func(tx *WriteTx) error {
			if err := tx.SuspendViewCapture(capA); err != nil {
				return err
			}
			// The suspension's raise then only restores the generation.
			_, err := tx.tx.ExecContext(ctx, `UPDATE view_clock SET generation = generation - 1 WHERE account_id = ?`, capA)
			return err
		}, "account " + capA + " was declared but its clock went"},
		{"another account's generation moved", func(tx *WriteTx) error {
			_, err := tx.tx.ExecContext(ctx, `UPDATE view_clock SET generation = generation + 1 WHERE account_id = ?`, capB)
			return err
		}, "the generation of account " + capB + " moved"},
		{"a change under the clock it found", func(tx *WriteTx) error {
			if err := tx.Account(capA).PutRecord(RecordRow{"episode", "e:000", "state", `{"rewound":1}`, `[]`}); err != nil {
				return err
			}
			// The change is stamped above the clock, which goes back to
			// where it was: the old key would name the new data.
			_, err := tx.tx.ExecContext(ctx, `UPDATE view_clock SET tick = tick - 1 WHERE account_id = ?`, capA)
			return err
		}, "account " + capA + " changed but its tick stayed at"},
		{"a stamp above the clock", func(tx *WriteTx) error {
			if err := tx.Account(capA).PutRecord(RecordRow{"episode", "e:000", "state", `{"future":1}`, `[]`}); err != nil {
				return err
			}
			_, err := tx.tx.ExecContext(ctx, `UPDATE view_changes SET tick = tick + 100 WHERE account_id = ? AND entity_id = 'e:000'`, capA)
			return err
		}, "account " + capA + " has 1 stamps above its tick"},
		{"another account's tick back", func(tx *WriteTx) error {
			_, err := tx.tx.ExecContext(ctx, `UPDATE view_clock SET tick = tick - 1 WHERE account_id = ?`, capB)
			return err
		}, "the tick of account " + capB + " went back"},
	} {
		before := rawState(t, s.Dir())
		err := s.Write(ctx, c.write)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
		if rawState(t, s.Dir()) != before {
			t.Errorf("%s: a refused transaction changed the database", c.name)
		}
	}
}

// Migration 0025 on a database with a library: the capture's tables start
// empty (every account at (0, 0)), its triggers and indexes exist, nothing
// else changes, and the next change is captured.
func TestMigrationAddsTheCaptureToALibrary(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:24]})
	if err := s.Write(ctx, func(tx *WriteTx) error {
		if _, err := tx.CreateAccountWithIDs(capA, "11111111-1111-4111-8111-111111111111"); err != nil {
			return err
		}
		return tx.SuspendViewCapture(capA) // no capture yet: nothing to suspend
	}); err != nil {
		t.Fatal(err)
	}
	seedViews(t, s, capA)
	before := viewInputs(t, s.reader, capA)
	s.Close()
	s = openTest(t, config{dataDir: dir})
	checkCaptureInstalled(t, s.writer)
	if changed := changedInputs(before, viewInputs(t, s.reader, capA)); len(changed) != 0 {
		t.Fatalf("the migration changed %v", changed)
	}
	if stamps, clock := stampsOf(t, s.reader, capA); len(stamps) != 0 || clock != (ViewClock{}) {
		t.Fatalf("after the migration: %d stamps, clock %+v", len(stamps), clock)
	}
	checkNextWriteCaptured(t, s)
}
