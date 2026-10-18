package storage

// Work gates on the executing capture and readers: SQLite virtual-machine
// instructions counted on the connection that runs them (a progress
// handler), for a fixed small request against small and power-user
// libraries.

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// vmSteps holds each counting connection's counter, by its handle.
var vmSteps sync.Map

func progressCount(_ *libc.TLS, handle uintptr) int32 {
	if c, ok := vmSteps.Load(handle); ok {
		c.(*atomic.Int64).Add(1)
	}
	return 0
}

// cFunc is a Go function as the transpiled SQLite calls it.
func cFunc[T any](f T) uintptr { return *(*uintptr)(unsafe.Pointer(&struct{ f T }{f})) }

// countSteps makes the connection count the virtual-machine instructions
// it runs (SQLite's progress handler, called at every check).
func countSteps(t testing.TB, conn *sql.Conn) *atomic.Int64 {
	t.Helper()
	c := &atomic.Int64{}
	if err := conn.Raw(func(dc any) error {
		v := reflect.ValueOf(dc).Elem()
		handle := uintptr(v.FieldByName("db").Uint())
		tls := (*libc.TLS)(v.FieldByName("tls").UnsafePointer())
		vmSteps.Store(handle, c)
		sqlite3.Xsqlite3_progress_handler(tls, handle, 1, cFunc(progressCount), handle)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return c
}

// workLibrary writes a library of p podcasts (each created by its own
// ranked node, with as many ranked nodes that create nothing) and e
// episodes (registered, in the catalog, with identity, metadata and state
// records): the rows in bulk, the records through the capture, so every
// episode and podcast has a stamp.
func workLibrary(t *testing.T, s *Store, account string, p, e int) {
	t.Helper()
	ctx := context.Background()
	if err := s.Write(ctx, func(tx *WriteTx) error {
		if err := tx.SuspendViewCapture(account); err != nil {
			return err
		}
		for _, q := range []string{
			`WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?2)
				INSERT INTO causal_nodes (account_id, node_id, kind, normalized_time) SELECT ?1, printf('n:%04d', n), 'fact', n FROM i
				UNION ALL SELECT ?1, printf('u:%04d', n), 'fact', n FROM i`,
			`WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?2)
				INSERT INTO node_order (account_id, node_id, rank) SELECT ?1, printf('n:%04d', n), (2 * n + 1) * 1048576 FROM i
				UNION ALL SELECT ?1, printf('u:%04d', n), (2 * n + 2) * 1048576 FROM i`,
			`WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?2)
				INSERT INTO podcasts (account_id, podcast_id, creating_op) SELECT ?1, printf('p:%04d', n), printf('n:%04d', n) FROM i`,
			`WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?3)
				INSERT INTO episodes (account_id, episode_id, podcast_id, occurrence, registered_by)
				SELECT ?1, printf('e:%06d', n), printf('p:%04d', n % ?2), '{}', 'n:0000' FROM i`,
			`WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?3)
				INSERT INTO catalog_items (account_id, episode_id, podcast_id, first_node, pub_date)
				SELECT ?1, printf('e:%06d', n), printf('p:%04d', n % ?2), 'n:0000', n FROM i`,
		} {
			if _, err := tx.tx.ExecContext(ctx, q, account, p, e); err != nil {
				return fmt.Errorf("%s: %w", q, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The records through the capture: every entity has a stamp.
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, q := range []string{
			`WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?3)
				INSERT INTO records (account_id, entity, entity_id, field_group, value, provenance)
				SELECT ?1, 'episode', printf('e:%06d', n), g.column1, '{"podcast_id":"' || printf('p:%04d', n % ?2) || '"}', '[]'
				FROM i, (VALUES ('identity'), ('metadata'), ('play_state')) g`,
			`WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?2)
				INSERT INTO records (account_id, entity, entity_id, field_group, value, provenance)
				SELECT ?1, 'podcast', printf('p:%04d', n), 'subscription', '{"state":"subscribed"}', '[]' FROM i`,
		} {
			if _, err := tx.tx.ExecContext(ctx, q, account, p, e); err != nil {
				return fmt.Errorf("%s: %w", q, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// libraryWork measures, in instructions, each routine capture path and each
// delta reader for one entity of the library.
func libraryWork(t *testing.T, p, e int) map[string]int64 {
	t.Helper()
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	workLibrary(t, s, account, p, e)
	out := map[string]int64{}
	// The capture: real writers and statements in a transaction on the
	// writer connection, rolled back, with the test audit's triggers off
	// so that only the capture's own work counts.
	conn, err := s.writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	steps := countSteps(t, conn)
	names, err := queryStrings(ctx, conn, `SELECT name FROM temp.sqlite_schema WHERE type = 'trigger' AND name LIKE 'view\_audit\_%' ESCAPE '\'`)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if _, err := conn.ExecContext(ctx, `DROP TRIGGER temp.`+n); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name string
		run  func(a *AccountTx) error
	}{
		{"record update", func(a *AccountTx) error {
			return a.PutRecord(RecordRow{"episode", "e:000007", "play_state", `{"play_state":"played"}`, `[]`})
		}},
		{"record insert", func(a *AccountTx) error {
			return a.PutRecord(RecordRow{"episode", "e:000007", "favorite", `{"on":true}`, `[]`})
		}},
		{"catalog date", func(a *AccountTx) error {
			return a.PutCatalogItem(CatalogItem{EpisodeID: "e:000007", PodcastID: "p:0007", FirstNode: "n:0000", PubDate: -1})
		}},
		{"episode registered", func(a *AccountTx) error { return a.AddEpisode("e:new", "p:0003", `{}`, "n:0000") }},
		{"creating node moved", func(a *AccountTx) error {
			_, err := a.w.tx.ExecContext(ctx, `UPDATE node_order SET rank = rank + 1 WHERE account_id = ? AND node_id = 'n:0005'`, a.id)
			return err
		}},
		{"other node moved", func(a *AccountTx) error {
			_, err := a.w.tx.ExecContext(ctx, `UPDATE node_order SET rank = rank + 1 WHERE account_id = ? AND node_id = 'u:0005'`, a.id)
			return err
		}},
		{"other node placed", func(a *AccountTx) error {
			if _, err := a.w.tx.ExecContext(ctx, `INSERT INTO causal_nodes (account_id, node_id, kind, normalized_time) VALUES (?, 'u:new', 'fact', 1)`, a.id); err != nil {
				return err
			}
			before := steps.Load()
			_, err := a.w.tx.ExecContext(ctx, `INSERT INTO node_order (account_id, node_id, rank) VALUES (?, 'u:new', 3)`, a.id)
			out["other node placed (insert only)"] = steps.Load() - before
			return err
		}},
	} {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		w := &WriteTx{ctx: ctx, tx: tx, store: s, accounts: map[string]*AccountTx{}}
		before := steps.Load()
		if err := c.run(w.Account(account)); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		out[c.name] = steps.Load() - before
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close()
	// One change, then the readers of it, on one reader connection.
	if err := s.Write(ctx, func(tx *WriteTx) error {
		return tx.Account(account).PutRecord(RecordRow{"episode", "e:000011", "play_state", `{"play_state":"played"}`, `[]`})
	}); err != nil {
		t.Fatal(err)
	}
	rconn, err := s.reader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rconn.Close()
	rsteps := countSteps(t, rconn)
	rtx, err := rconn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()
	r := (&ReadTx{ctx: ctx, tx: rtx}).Account(account)
	clock, err := r.ViewClock()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		read func() (int, error)
	}{
		{"clock", func() (int, error) { _, err := r.ViewClock(); return 1, err }},
		{"changes", func() (int, error) {
			ch, _, _, err := r.ViewChangesSince(clock.Tick-1, ViewCursor{Tick: clock.Tick - 1})
			return len(ch), err
		}},
		{"episode records", func() (int, error) {
			rows, err := r.ViewEntityRecords("episode", []string{"e:000011"})
			return len(rows), err
		}},
		{"podcast records", func() (int, error) {
			rows, err := r.ViewEntityRecords("podcast", []string{"p:0011"})
			return len(rows), err
		}},
		{"registered", func() (int, error) { ids, err := r.RegisteredEpisodes([]string{"e:000011"}); return len(ids), err }},
		{"catalog", func() (int, error) { rows, err := r.CatalogRowsFor([]string{"e:000011"}); return len(rows), err }},
		{"ranks", func() (int, error) { ranks, err := r.PodcastRanksFor([]string{"p:0011"}); return len(ranks), err }},
	} {
		before := rsteps.Load()
		n, err := c.read()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if n == 0 {
			t.Fatalf("%s read nothing", c.name)
		}
		out["read "+c.name] = rsteps.Load() - before
	}
	return out
}

// The work of each routine capture path (a record, a catalog date, a
// registration, a creating node's rank, another node's rank or placement)
// and of each delta reader for one entity is the same, within a few
// instructions, in a 12-podcast, 50-episode library and in a 120-podcast,
// 50,400-episode one: none reads the library.
func TestCaptureAndReaderWorkIsIndependentOfTheLibrary(t *testing.T) {
	small := libraryWork(t, 12, 50)
	large := libraryWork(t, 120, 50_400)
	t.Logf("instructions, small library: %v", small)
	t.Logf("instructions, large library: %v", large)
	for name, n := range small {
		if n == 0 {
			t.Errorf("%s: no instructions counted", name)
		}
		if m := large[name]; m > n+16 {
			t.Errorf("%s: %d instructions in the large library, %d in the small one", name, m, n)
		}
	}
}

// A rebuild's copies are looked up by key: comparing the rebuilt records
// with the copy (RecordChanges) and reading an earlier record (PriorRecord)
// cost the same per record at 2,000 and 8,000 episodes, not a scan of the
// copy for each.
func TestRebuildCopiesAreLookedUpByKey(t *testing.T) {
	ctx := context.Background()
	work := func(e int) (changes, prior int64) {
		s := openTest(t, config{dataDir: t.TempDir()})
		account := createAccounts(t, s, 1)[0]
		workLibrary(t, s, account, 20, e)
		conn, err := s.writer.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		steps := countSteps(t, conn)
		// The test audit's triggers off: this transaction is rolled back.
		names, err := queryStrings(ctx, conn, `SELECT name FROM temp.sqlite_schema WHERE type = 'trigger' AND name LIKE 'view\_audit\_%' ESCAPE '\'`)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range names {
			if _, err := conn.ExecContext(ctx, `DROP TRIGGER temp.`+n); err != nil {
				t.Fatal(err)
			}
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		w := &WriteTx{ctx: ctx, tx: tx, store: s, accounts: map[string]*AccountTx{}}
		a := w.Account(account)
		if err := a.ClearDerived(); err != nil {
			t.Fatal(err)
		}
		// The rebuild derives the same records again.
		if _, err := tx.ExecContext(ctx, `INSERT INTO main.records SELECT * FROM temp.rebuild_records`); err != nil {
			t.Fatal(err)
		}
		before := steps.Load()
		changed, removed, err := a.RecordChanges()
		if err != nil || len(changed) != 0 || len(removed) != 0 {
			t.Fatalf("%d changed, %d removed: %v", len(changed), len(removed), err)
		}
		changes = steps.Load() - before
		if _, err := tx.ExecContext(ctx, `DELETE FROM main.records WHERE account_id = ?`, account); err != nil {
			t.Fatal(err)
		}
		before = steps.Load()
		for i := range 100 {
			if _, err := a.PriorRecord("episode", fmt.Sprintf("e:%06d", i*e/100), "metadata"); err != nil {
				t.Fatal(err)
			}
		}
		return changes, steps.Load() - before
	}
	sc, sp := work(2000)
	lc, lp := work(8000)
	t.Logf("instructions: the comparison %d / %d, 100 earlier records %d / %d", sc, lc, sp, lp)
	if lc > 6*sc {
		t.Errorf("comparing 4 times the records took %.1f times the instructions", float64(lc)/float64(sc))
	}
	if lp > 2*sp {
		t.Errorf("reading earlier records took %.1f times the instructions at 4 times the records", float64(lp)/float64(sp))
	}
}
