package storage

// The commit observer across every change of an account's view key and
// across a store's opening, the audit's guard against writes outside
// Store.Write, the list views' record projection, suspension of several
// accounts, its notifications, and failures at the commit itself.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// observeAll installs an observer that records each observation and checks
// that the store's newest snapshot holds each account exactly as observed.
func observeAll(t *testing.T) (calls func() []map[string]CommittedAccount) {
	t.Helper()
	var mu sync.Mutex
	var seen []map[string]CommittedAccount
	var problems []string
	observeAlso(t, func(st *Store, accounts map[string]CommittedAccount) {
		var found []string
		if st.reader == nil {
			found = append(found, "an observation before the store's readers exist")
		}
		for id, want := range accounts {
			var got CommittedAccount
			if err := st.Read(context.Background(), func(r *ReadTx) error {
				h, err := r.Account(id).Head()
				if errors.Is(err, ErrNoAccount) {
					return nil
				}
				if err != nil {
					return err
				}
				got.Exists, got.Head = true, h
				got.Clock, err = r.Account(id).ViewClock()
				return err
			}); err != nil {
				found = append(found, err.Error())
				continue
			}
			if got.Exists != want.Exists || got.Head.LogID != want.Head.LogID || got.Head.Epoch != want.Head.Epoch || got.Head.Seq != want.Head.Seq ||
				!bytes.Equal(got.Head.Digest, want.Head.Digest) || got.Clock != want.Clock {
				found = append(found, fmt.Sprintf("account %s: the snapshot has %+v, the observation %+v", id, got, want))
			}
		}
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, maps.Clone(accounts))
		problems = append(problems, found...)
	})
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		if len(problems) > 0 {
			t.Errorf("%d observer problems, first: %s", len(problems), problems[0])
		}
	})
	return func() []map[string]CommittedAccount {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(seen)
	}
}

func headsOf(t *testing.T, s *Store, ids ...string) map[string]Head {
	t.Helper()
	out := map[string]Head{}
	for _, id := range ids {
		h, err := s.Head(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		out[id] = h
	}
	return out
}

// Every account whose view key changes in a commit is observed, with its
// new key: a restore transition moves every account's epoch, an evidence
// transition in a store without restore metadata too, and the repair at
// open raises every clock (an account without one included).
func TestObserverSeesEveryViewKeyChange(t *testing.T) {
	ctx := context.Background()
	t.Run("restore transition", func(t *testing.T) {
		s := openTest(t, config{dataDir: newCaptureDir(t)})
		before := headsOf(t, s, capA, capB)
		calls := observeAll(t)
		if err := s.Write(ctx, func(tx *WriteTx) error { return tx.RestoreTransition("a test") }); err != nil {
			t.Fatal(err)
		}
		got := calls()
		if len(got) != 1 {
			t.Fatalf("%d observations", len(got))
		}
		for _, id := range []string{capA, capB} {
			if a, ok := got[0][id]; !ok || a.Head.Epoch != before[id].Epoch+1 {
				t.Errorf("account %s: observed %+v (epoch was %d)", id, a, before[id].Epoch)
			}
		}
	})
	t.Run("evidence transition in staging", func(t *testing.T) {
		live := openTest(t, config{dataDir: newCaptureDir(t)})
		id, err := live.ServerID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := live.ReceiptKeys(ctx)
		if err != nil {
			t.Fatal(err)
		}
		heads := headsOf(t, live, capA, capB)
		st, err := OpenStaging(ctx, filepath.Join(t.TempDir(), "staging"), id, keys)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if err := st.Write(ctx, func(tx *WriteTx) error {
			if err := tx.MirrorAccount(capA, "ns-a", heads[capA]); err != nil {
				return err
			}
			return tx.MirrorAccount(capB, "ns-b", heads[capB])
		}); err != nil {
			t.Fatal(err)
		}
		calls := observeAll(t)
		if err := st.Write(ctx, func(tx *WriteTx) error { return tx.EvidenceTransition(4) }); err != nil {
			t.Fatal(err)
		}
		got := calls()
		for _, id := range []string{capA, capB} {
			if a, ok := got[len(got)-1][id]; !ok || a.Head.Epoch != 5 {
				t.Errorf("account %s: observed %+v", id, a)
			}
		}
	})
	t.Run("repair at open", func(t *testing.T) {
		dir := newCaptureDir(t)
		s := openTest(t, config{dataDir: dir})
		clockless := createAccounts(t, s, 1)[0]
		s.Close()
		editDatabase(t, dir, "DROP TRIGGER view_capture_episodes_delete")
		calls := observeAll(t)
		s = openTest(t, config{dataDir: dir})
		got := calls()
		if len(got) != 1 {
			t.Fatalf("%d observations of the opening", len(got))
		}
		for _, id := range []string{capA, capB, clockless} {
			if a, ok := got[0][id]; !ok || a.Clock.Generation != 1 {
				t.Errorf("account %s: observed %+v", id, a)
			}
		}
	})
}

// An observer installed before a store opens is handed the opening's writes
// once the store's readers exist, as one observation, and each commit after
// it: a new store, an ordinary reopening, a staging database, a repair and a
// restore transition at startup.
func TestObserverInstalledBeforeOpen(t *testing.T) {
	ctx := context.Background()
	calls := observeAll(t)
	dir := filepath.Join(t.TempDir(), "data")
	s := openTest(t, config{dataDir: dir})
	if n := len(calls()); n != 1 {
		t.Fatalf("a new store: %d observations", n)
	}
	ids := createAccounts(t, s, 2)
	seedViews(t, s, ids[0])
	if got := calls(); len(got) != 3 || got[2][ids[0]].Clock.Tick == 0 {
		t.Fatalf("after two commits: %d observations", len(got))
	}
	heads := headsOf(t, s, ids...)
	s.Close()

	n := len(calls())
	s = openTest(t, config{dataDir: dir})
	if got := calls(); len(got) != n+1 {
		t.Fatalf("an ordinary reopening: %d observations", len(got)-n)
	}
	s.Close()

	// A restore transition at startup: the database file was replaced.
	db := filepath.Join(dir, databaseName)
	if _, err := os.Stat(db + "-wal"); err == nil {
		t.Fatal("precondition: the closed store left a write-ahead log")
	}
	data, err := os.ReadFile(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(db+".copy", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(db+".copy", db); err != nil {
		t.Fatal(err)
	}
	n = len(calls())
	s = openTest(t, config{dataDir: dir})
	got := calls()
	if len(got) != n+1 {
		t.Fatalf("a restore at startup: %d observations", len(got)-n)
	}
	for _, id := range ids {
		if a := got[n][id]; a.Head.Epoch != heads[id].Epoch+1 {
			t.Errorf("account %s after the restore: observed %+v (epoch was %d)", id, a, heads[id].Epoch)
		}
	}
	id, err := s.ServerID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := s.ReceiptKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	staging := filepath.Join(t.TempDir(), "staging")
	for range 2 { // created, then reopened
		n = len(calls())
		st, err := OpenStaging(ctx, staging, id, keys)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(calls()); got != n+1 {
			t.Fatalf("a staging database: %d observations", got-n)
		}
		st.Close()
	}

	editDatabase(t, dir, "DROP TRIGGER view_capture_records_insert")
	n = len(calls())
	s = openTest(t, config{dataDir: dir})
	got = calls()
	if len(got) != n+1 || got[n][ids[0]].Clock.Generation != 1 || got[n][ids[1]].Clock.Generation != 1 {
		t.Fatalf("a repair at open: %v", got[n:])
	}
}

// A view input or capture clock written outside Store.Write fails; a change
// of an account's log outside it leaves evidence that stops the next write
// until it is checked.
func TestAuditRefusesWritesOutsideStoreWrite(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: newCaptureDir(t)})
	before := rawState(t, s.Dir())
	for _, q := range []string{
		`UPDATE records SET provenance = '["outside"]' WHERE account_id = '` + capA + `' AND entity_id = 'e:000'`,
		`DELETE FROM catalog_items WHERE account_id = '` + capA + `' AND episode_id = 'e:000'`,
		`INSERT INTO episodes (account_id, episode_id, podcast_id, occurrence) VALUES ('` + capA + `', 'e:out', 'p:0', '{}')`,
		`UPDATE node_order SET rank = rank + 1 WHERE account_id = '` + capA + `' AND node_id = 'n:0'`,
		`UPDATE view_clock SET tick = tick + 1 WHERE account_id = '` + capA + `'`,
	} {
		if _, err := s.writer.ExecContext(ctx, q); err == nil || !strings.Contains(err.Error(), "outside Store.Write") {
			t.Errorf("%s: %v", q, err)
		}
	}
	if rawState(t, s.Dir()) != before {
		t.Fatal("a refused write changed the database")
	}
	if _, err := s.writer.ExecContext(ctx, `UPDATE account_logs SET head_seq = head_seq WHERE account_id = ?`, capA); err != nil {
		t.Fatal(err)
	}
	write := func() error {
		return s.Write(ctx, func(tx *WriteTx) error { return tx.SetMeta("after", "x") })
	}
	if err := write(); err == nil || !strings.Contains(err.Error(), "since the last write was checked") {
		t.Fatalf("a write over unchecked evidence: %v", err)
	}
	if _, err := s.writer.ExecContext(ctx, `DELETE FROM temp.view_audit_accounts`); err != nil {
		t.Fatal(err)
	}
	if err := write(); err != nil {
		t.Fatal(err)
	}
	checkNextWriteCaptured(t, s)
}

// The list views' projection of records, specified here independently of
// the query that makes it: episode metadata loses its top-level description
// and chapters, whether set or null, and keeps every other field (artwork,
// links, media with its own description, chapter and transcript addresses)
// byte for byte; other field groups, other entities and provenance are
// untouched.
func TestListViewsProjectRecordsAsSpecified(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	type rec struct{ entity, id, group, value, provenance, want string }
	recs := []rec{
		{"episode", "e:1", "metadata",
			`{"chapter_url":"https://example.invalid/c.json","chapters":[{"start":0,"title":"one"}],"description":"<p>long</p>","image_url":"https://example.invalid/e.jpg","link":"https://example.invalid/e","media":{"description":"kept","duration":5,"url":"https://example.invalid/e.mp3"},"pub_date":7,"title":"T","transcript_type":"text/vtt","transcript_url":"https://example.invalid/t.vtt"}`,
			`["aaaaaaaa-0000-4000-8000-000000000001","aaaaaaaa-0000-4000-8000-000000000002"]`,
			`{"chapter_url":"https://example.invalid/c.json","image_url":"https://example.invalid/e.jpg","link":"https://example.invalid/e","media":{"description":"kept","duration":5,"url":"https://example.invalid/e.mp3"},"pub_date":7,"title":"T","transcript_type":"text/vtt","transcript_url":"https://example.invalid/t.vtt"}`},
		{"episode", "e:2", "metadata", `{"chapters":null,"description":null,"image_url":null,"title":"N"}`, `[]`, `{"image_url":null,"title":"N"}`},
		{"episode", "e:3", "metadata", `{"image_url":"https://example.invalid/3.jpg","title":"no description"}`, `[]`,
			`{"image_url":"https://example.invalid/3.jpg","title":"no description"}`},
		{"episode", "e:4", "metadata", `{}`, `[]`, `{}`},
		{"episode", "e:1", "notes", `{"chapters":[1],"description":"another group keeps it"}`, `[]`, `{"chapters":[1],"description":"another group keeps it"}`},
		{"podcast", "p:1", "metadata", `{"description":"a podcast keeps it","image_url":"https://example.invalid/p.jpg","title":"P"}`, `["b"]`,
			`{"description":"a podcast keeps it","image_url":"https://example.invalid/p.jpg","title":"P"}`},
		{"account", account, "queue_policy", `{"description":"an account record keeps it"}`, `["c"]`, `{"description":"an account record keeps it"}`},
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, r := range recs {
			if err := tx.Account(account).PutRecord(RecordRow{r.entity, r.id, r.group, r.value, r.provenance}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var want []RecordRow
	for _, r := range recs {
		want = append(want, RecordRow{r.entity, r.id, r.group, r.want, r.provenance})
	}
	slices.SortFunc(want, func(a, b RecordRow) int {
		return strings.Compare(a.Entity+"\x00"+a.EntityID+"\x00"+a.FieldGroup, b.Entity+"\x00"+b.EntityID+"\x00"+b.FieldGroup)
	})
	if err := s.Read(ctx, func(r *ReadTx) error {
		a := r.Account(account)
		full, err := a.ViewRecords()
		if err != nil {
			return err
		}
		if !slices.Equal(full, want) {
			return fmt.Errorf("the full reader:\n%v\nwant\n%v", full, want)
		}
		for _, entity := range []string{"account", "episode", "podcast"} {
			var ids []string
			var wantEntity []RecordRow
			for _, w := range want {
				if w.Entity == entity {
					ids = append(ids, w.EntityID)
					wantEntity = append(wantEntity, w)
				}
			}
			got, err := a.ViewEntityRecords(entity, ids)
			if err != nil {
				return err
			}
			if !slices.Equal(got, wantEntity) {
				return fmt.Errorf("the %s reader:\n%v\nwant\n%v", entity, got, wantEntity)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// One transaction may suspend capture for several accounts: each declared
// account (declared twice or once) is one generation higher with its old
// stamps cleared, and an untouched account keeps its clock and stamps.
func TestSuspensionDeclaresEveryAccount(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: newCaptureDir(t)})
	third := createAccounts(t, s, 1)[0]
	seedViews(t, s, third)
	stampsA, a0 := stampsOf(t, s.reader, capA)
	_, b0 := stampsOf(t, s.reader, capB)
	thirdStamps, c0 := stampsOf(t, s.reader, third)
	if len(stampsA) == 0 {
		t.Fatal("precondition: stamps")
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, id := range []string{capA, capB, capA} {
			if err := tx.SuspendViewCapture(id); err != nil {
				return err
			}
		}
		for _, id := range []string{capA, capB} {
			if err := tx.Account(id).PutRecord(RecordRow{"episode", "e:000", "state", `{"bulk":1}`, `[]`}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for id, before := range map[string]ViewClock{capA: a0, capB: b0} {
		stamps, now := stampsOf(t, s.reader, id)
		if now.Generation != before.Generation+1 || now.Tick != before.Tick+1 || len(stamps) != 0 {
			t.Errorf("account %s: clock %+v → %+v, %d stamps", id, before, now, len(stamps))
		}
	}
	if stamps, now := stampsOf(t, s.reader, third); now != c0 || !maps.Equal(stamps, thirdStamps) {
		t.Errorf("the untouched account: clock %+v → %+v", c0, now)
	}
}

// A suspended transaction wakes its declared accounts' listeners (a
// generation change without a commit), not another account's, not the
// refresher's generation listeners, and nobody when it rolls back.
func TestSuspensionWakesOnlyItsAccounts(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: newCaptureDir(t)})
	chA, stopA := s.Subscribe(capA)
	defer stopA()
	chB, stopB := s.Subscribe(capB)
	defer stopB()
	gens, stopGens := s.SubscribeGenerations()
	defer stopGens()
	woken := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	if err := s.Write(ctx, func(tx *WriteTx) error { return bulkRewrite(tx, capA, func(string) error { return nil }) }); err != nil {
		t.Fatal(err)
	}
	if !woken(chA) || woken(chB) || woken(gens) {
		t.Fatal("a suspended rewrite of A: A must be woken, B and the generation listeners not")
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		if err := bulkRewrite(tx, capA, func(string) error { return nil }); err != nil {
			return err
		}
		return errRollback
	}); !errors.Is(err, errRollback) {
		t.Fatal(err)
	}
	if woken(chA) || woken(chB) || woken(gens) {
		t.Fatal("a rolled-back rewrite woke a listener")
	}
}

// Failures at the commit itself and part-way through creating the triggers
// again roll everything back: the data, the clocks and the schema; the next
// write is captured.
func TestSuspendedCaptureSurvivesCommitAndRecreationFailures(t *testing.T) {
	ctx := context.Background()
	t.Run("a commit that fails", func(t *testing.T) {
		s := openTest(t, config{dataDir: newCaptureDir(t)})
		before := snapshotCapture(t, s)
		err := s.Write(ctx, func(tx *WriteTx) error {
			if err := bulkRewrite(tx, capA, func(string) error { return nil }); err != nil {
				return err
			}
			// A deferred foreign key violation fails the COMMIT itself.
			if _, err := tx.tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
				return err
			}
			_, err := tx.tx.ExecContext(ctx, `INSERT INTO episodes (account_id, episode_id, podcast_id, occurrence) VALUES (?, 'e:orphan', 'p:none', '{}')`, capA)
			return err
		})
		if err == nil || !strings.Contains(err.Error(), "commit") {
			t.Fatalf("the write: %v", err)
		}
		checkCaptureInstalled(t, s.writer)
		if d := snapshotCapture(t, s).diff(before); d != "" {
			t.Fatalf("after a failed commit: %s", d)
		}
		checkNextWriteCaptured(t, s)
	})
	t.Run("a failure while creating the triggers again", func(t *testing.T) {
		s := openTest(t, config{dataDir: newCaptureDir(t)})
		before := snapshotCapture(t, s)
		fault := errors.New("injected fault")
		s.hooks.viewCapture = func(at string) error {
			if at == "recreated view_capture_episodes_insert" { // the seventh of fifteen
				return fault
			}
			return nil
		}
		if err := s.Write(ctx, func(tx *WriteTx) error { return bulkRewrite(tx, capA, func(string) error { return nil }) }); !errors.Is(err, fault) {
			t.Fatalf("the write: %v", err)
		}
		s.hooks = hooks{}
		checkCaptureInstalled(t, s.writer)
		if d := snapshotCapture(t, s).diff(before); d != "" {
			t.Fatalf("after the failure: %s", d)
		}
		checkNextWriteCaptured(t, s)
	})
}
