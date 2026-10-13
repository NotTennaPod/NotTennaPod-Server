package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// nextName names a test migration numbered after the embedded ones.
func nextName(what string) string { return fmt.Sprintf("%04d_%s.sql", len(embeddedMigrations)+1, what) }

// withExtra returns the embedded migrations plus m.
func withExtra(m migration) []migration {
	return append(append([]migration{}, embeddedMigrations...), m)
}

// assertMigrated checks, through a separate connection, that every migration is
// recorded in the database file.
func assertMigrated(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&v); err != nil || v != len(embeddedMigrations) {
		t.Errorf("sync before migrations completed: version %d (%v)", v, err)
	}
}

func TestCloseWaitsForWritesInProgress(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 1)
	inside, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.Write(ctx, func(tx *WriteTx) error {
			close(inside)
			<-release
			if _, err := tx.tx.ExecContext(ctx, "INSERT INTO server_meta (key, value) VALUES (?, 'x')", callerKey(ids[0], 1)); err != nil {
				return err
			}
			_, _, err := tx.Account(ids[0]).AppendCommit(record(ids[0], 1))
			return err
		})
	}()
	<-inside
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a write was in progress")
	case <-time.After(100 * time.Millisecond): // Close is still waiting, as it must
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("write in progress failed: %v", err)
	}
	<-closed
	if err := s.Write(ctx, func(*WriteTx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
	s2 := openTest(t, config{dataDir: s.Dir()})
	if heads := verifyStore(t, s2); heads[ids[0]] != 1 {
		t.Fatalf("the write that Close waited for is missing: %v", heads)
	}
}

func noSync(string) error { return nil }

func openTest(t *testing.T, cfg config) *Store {
	t.Helper()
	if cfg.syncDir == nil {
		cfg.syncDir = noSync
	}
	s, err := open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func createAccounts(t *testing.T, s *Store, n int) []string {
	t.Helper()
	var ids []string
	err := s.Write(context.Background(), func(tx *WriteTx) error {
		for i := 0; i < n; i++ {
			a, err := tx.CreateAccount()
			if err != nil {
				return err
			}
			ids = append(ids, a.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func record(account string, seq int64) CommitBody {
	return CommitBody{Records: []any{map[string]any{"entity": "probe", "id": account, "field_group": "n", "value": seq, "provenance": []any{}}}}
}

// appendWithCallerRow appends one commit and, in the same transaction, a
// caller-owned row keyed by the commit's revision.
func appendWithCallerRow(ctx context.Context, s *Store, account string) (int64, error) {
	var seq int64
	err := s.Write(ctx, func(tx *WriteTx) error {
		acct := tx.Account(account)
		next, err := acct.NextRevision()
		if err != nil {
			return err
		}
		if _, err := tx.tx.ExecContext(ctx, "INSERT INTO server_meta (key, value) VALUES (?, 'x')", callerKey(account, next.Seq)); err != nil {
			return err
		}
		rev, _, err := acct.AppendCommit(record(account, next.Seq))
		seq = rev.Seq
		return err
	})
	return seq, err
}

func callerKey(account string, seq int64) string { return fmt.Sprintf("caller/%s/%d", account, seq) }

// verifyStore checks every account's chain and that caller rows exist for exactly
// the committed revisions.
func verifyStore(t *testing.T, s *Store) map[string]int64 {
	t.Helper()
	ctx := context.Background()
	heads := map[string]int64{}
	err := s.Read(ctx, func(r *ReadTx) error {
		ids, err := r.AccountIDs()
		if err != nil {
			return err
		}
		for _, id := range ids {
			h, err := r.Account(id).Head()
			if err != nil {
				return err
			}
			heads[id] = h.Seq
		}
		keys, err := queryStrings(ctx, r.tx, "SELECT key FROM server_meta WHERE key LIKE 'caller/%'")
		if err != nil {
			return err
		}
		have := map[string]bool{}
		for _, k := range keys {
			have[k] = true
		}
		want := 0
		for id, seq := range heads {
			for i := int64(1); i <= seq; i++ {
				if !have[callerKey(id, i)] {
					return fmt.Errorf("caller row missing for %s seq %d", id, i)
				}
				want++
			}
		}
		if len(keys) != want {
			return fmt.Errorf("%d caller rows for %d commits: an uncommitted effect survived", len(keys), want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for id := range heads {
		if err := s.VerifyChain(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	return heads
}

func TestPragmas(t *testing.T) {
	s := openTest(t, config{dataDir: filepath.Join(t.TempDir(), "new")})
	for _, reader := range []bool{false, true} {
		p, err := s.Pragmas(context.Background(), reader)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{"journal_mode": "wal", "synchronous": "2", "fullfsync": "1", "checkpoint_fullfsync": "1",
			"foreign_keys": "1", "trusted_schema": "0", "query_only": map[bool]string{false: "0", true: "1"}[reader]}
		for k, v := range want {
			if p[k] != v {
				t.Errorf("reader=%v PRAGMA %s = %q, want %q", reader, k, p[k], v)
			}
		}
	}
	info, err := os.Stat(filepath.Join(s.Dir(), databaseName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("database mode %v (%v), want 0600", info.Mode().Perm(), err)
	}
	dirInfo, _ := os.Stat(s.Dir())
	if dirInfo.Mode().Perm() != 0o700 {
		t.Errorf("data directory mode %v, want 0700", dirInfo.Mode().Perm())
	}
}

func chainOf(dir string) []string {
	var out []string
	for {
		out = append(out, dir)
		parent := filepath.Dir(dir)
		if parent == dir {
			return out
		}
		dir = parent
	}
}

// expectedChain is the lexical chain of dir followed, when dir resolves through
// symlinks, by the physical chain.
func expectedChain(t *testing.T, dir string) []string {
	t.Helper()
	want := chainOf(dir)
	physical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if physical != dir {
		want = append(want, chainOf(physical)...)
	}
	return want
}

func TestProvisioningSyncsTheWholeChain(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b", "c")
	var synced []string
	record := func(d string) error {
		// Synchronization happens only after the lock file, the database and its
		// WAL exist and every migration has been applied.
		if len(synced) == 0 {
			for _, name := range []string{lockName, databaseName, databaseName + "-wal"} {
				if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
					t.Errorf("sync before %s exists: %v", name, err)
				}
			}
			assertMigrated(t, filepath.Join(dir, databaseName))
		}
		synced = append(synced, d)
		return nil
	}

	// Fresh start: every directory from the data directory to the root, deepest first.
	s := openTest(t, config{dataDir: dir, syncDir: record})
	if got, want := strings.Join(synced, "|"), strings.Join(expectedChain(t, dir), "|"); got != want {
		t.Fatalf("synced %v\nwant %v", synced, expectedChain(t, dir))
	}
	s.Close()

	// A start that died after creating directories but before syncing: the
	// directories already exist, and the restart still syncs the whole chain.
	dir2 := filepath.Join(root, "x", "y")
	if err := os.MkdirAll(dir2, 0o700); err != nil {
		t.Fatal(err)
	}
	var synced2 []string
	s = openTest(t, config{dataDir: dir2, syncDir: func(d string) error { synced2 = append(synced2, d); return nil }})
	if got, want := strings.Join(synced2, "|"), strings.Join(expectedChain(t, dir2), "|"); got != want {
		t.Fatalf("restart synced %v\nwant %v", synced2, expectedChain(t, dir2))
	}
	s.Close()

	// A data directory reached through a symlink: the physical parents are synced too.
	target := filepath.Join(root, "target", "parent", "data")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(filepath.Join(root, "target", "parent"), alias); err != nil {
		t.Fatal(err)
	}
	var synced3 []string
	s = openTest(t, config{dataDir: filepath.Join(alias, "data"), syncDir: func(d string) error { synced3 = append(synced3, d); return nil }})
	physicalTarget, _ := filepath.EvalSymlinks(target)
	for _, d := range []string{physicalTarget, filepath.Dir(physicalTarget), filepath.Dir(filepath.Dir(physicalTarget))} {
		if !slices.Contains(synced3, d) {
			t.Errorf("symlinked data directory: %s not synced (synced %v)", d, synced3)
		}
	}
	s.Close()

	// A failing sync stops startup, names the directory and releases the lock.
	failing := filepath.Join(root, "a")
	_, err := open(context.Background(), config{dataDir: dir, syncDir: func(d string) error {
		if d == failing {
			return errors.New("injected")
		}
		return nil
	}})
	if err == nil || !strings.Contains(err.Error(), failing) {
		t.Fatalf("open with failing sync: %v", err)
	}
	openTest(t, config{dataDir: dir}).Close()
}

func TestRealDirectorySync(t *testing.T) {
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "real"))
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func TestAppendReadAndVerify(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 2)
	for i := 1; i <= 5; i++ {
		seq, err := appendWithCallerRow(ctx, s, ids[0])
		if err != nil || seq != int64(i) {
			t.Fatalf("append %d: seq %d, %v", i, seq, err)
		}
	}
	verifyStore(t, s)
	err := s.Read(ctx, func(r *ReadTx) error {
		page, err := r.Account(ids[0]).Commits(1, 4, 2)
		if err != nil {
			return err
		}
		if len(page) != 2 || page[0].Seq != 2 || page[1].Seq != 3 {
			return fmt.Errorf("page %+v", page)
		}
		if !strings.HasPrefix(string(page[0].Canonical), `{"accepted":[],"decisions":[],"epoch":1,"records":[`) {
			return fmt.Errorf("canonical %s", page[0].Canonical)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// One commit per account per transaction, and NextRevision matches.
	err = s.Write(ctx, func(tx *WriteTx) error {
		a := tx.Account(ids[1])
		next, err := a.NextRevision()
		if err != nil {
			return err
		}
		rev, _, err := a.AppendCommit(record(ids[1], 1))
		if err != nil || rev != next {
			return fmt.Errorf("rev %v next %v err %v", rev, next, err)
		}
		if _, _, err := a.AppendCommit(record(ids[1], 2)); !errors.Is(err, ErrSecondCommit) {
			return fmt.Errorf("second commit: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTransactionFailureContract(t *testing.T) {
	type failure struct {
		name string
		fail func(cancel context.CancelFunc) error
	}
	failures := []failure{
		{"error", func(context.CancelFunc) error { return errors.New("injected") }},
		{"panic", func(context.CancelFunc) error { panic("injected") }},
		{"cancel", func(cancel context.CancelFunc) error { cancel(); return nil }},
	}
	for _, point := range []string{"afterLogInsert", "afterHeadUpdate", "beforeCommit"} {
		for _, f := range failures {
			t.Run(point+"/"+f.name, func(t *testing.T) {
				s := openTest(t, config{dataDir: t.TempDir()})
				ids := createAccounts(t, s, 1)
				if _, err := appendWithCallerRow(context.Background(), s, ids[0]); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				hook := func() error { return f.fail(cancel) }
				switch point {
				case "afterLogInsert":
					s.hooks.afterLogInsert = hook
				case "afterHeadUpdate":
					s.hooks.afterHeadUpdate = hook
				case "beforeCommit":
					s.hooks.beforeCommit = hook
				}
				var err error
				func() {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic: %v", r)
						}
					}()
					_, err = appendWithCallerRow(ctx, s, ids[0])
				}()
				if err == nil {
					t.Fatal("failed transaction reported success")
				}
				if f.name == "panic" && !strings.HasPrefix(err.Error(), "panic:") {
					t.Fatalf("panic not re-raised: %v", err)
				}
				s.hooks = hooks{}
				heads := verifyStore(t, s)
				if heads[ids[0]] != 1 {
					t.Fatalf("head %d after failed append, want 1", heads[ids[0]])
				}
				seq, err := appendWithCallerRow(context.Background(), s, ids[0])
				if err != nil || seq != 2 {
					t.Fatalf("next append: seq %d, %v (want 2: the failed seq is reused)", seq, err)
				}
				verifyStore(t, s)
			})
		}
	}
}

func TestAccountIsolation(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 2)
	a, b := ids[0], ids[1]
	for i := 0; i < 3; i++ {
		if _, err := appendWithCallerRow(ctx, s, a); err != nil {
			t.Fatal(err)
		}
	}
	err := s.Read(ctx, func(r *ReadTx) error {
		hb, err := r.Account(b).Head()
		if err != nil || hb.Seq != 0 {
			return fmt.Errorf("b head %+v %v", hb, err)
		}
		commits, err := r.Account(b).Commits(0, 1<<40, 0)
		if err != nil || len(commits) != 0 {
			return fmt.Errorf("b sees %d commits (%v)", len(commits), err)
		}
		if _, err := r.Account("00000000-0000-4000-8000-000000000000").Head(); !errors.Is(err, ErrNoAccount) {
			return fmt.Errorf("unknown account head: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Writing to b never touches a.
	if _, err := appendWithCallerRow(ctx, s, b); err != nil {
		t.Fatal(err)
	}
	heads := verifyStore(t, s)
	if heads[a] != 3 || heads[b] != 1 {
		t.Fatalf("heads %v", heads)
	}
	err = s.Write(ctx, func(tx *WriteTx) error {
		_, _, err := tx.Account("00000000-0000-4000-8000-000000000000").AppendCommit(record("x", 1))
		if !errors.Is(err, ErrNoAccount) {
			return fmt.Errorf("append to unknown account: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Deleting b removes exactly b's rows.
	if err := s.Write(ctx, func(tx *WriteTx) error { return tx.DeleteAccount(b) }); err != nil {
		t.Fatal(err)
	}
	err = s.Read(ctx, func(r *ReadTx) error {
		for table, want := range map[string][2]int{"accounts": {1, 0}, "account_logs": {1, 0}, "log_commits": {3, 0}} {
			for i, id := range []string{a, b} {
				var n int
				if err := r.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE account_id = ?", id).Scan(&n); err != nil {
					return err
				}
				if n != want[i] {
					return fmt.Errorf("%s rows for account %d: %d, want %d", table, i, n, want[i])
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAppendsAndReaders(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 8)
	const writers, perWriter = 32, 20
	var wg sync.WaitGroup
	errs := make(chan error, writers+8)
	stop := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := appendWithCallerRow(ctx, s, ids[(w+i)%len(ids)]); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := ids[r%len(ids)]
				err := s.Read(ctx, func(rt *ReadTx) error {
					h, err := rt.Account(id).Head()
					if err != nil {
						return err
					}
					commits, err := rt.Account(id).Commits(0, 1<<40, 0)
					if err != nil {
						return err
					}
					if int64(len(commits)) != h.Seq {
						return fmt.Errorf("snapshot has %d commits but head %d", len(commits), h.Seq)
					}
					return nil
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}(r)
	}
	wg.Wait()
	close(stop)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	heads := verifyStore(t, s)
	var total int64
	for _, h := range heads {
		total += h
	}
	if total != writers*perWriter {
		t.Fatalf("%d commits, want %d", total, writers*perWriter)
	}
}

func TestMigrations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	createAccounts(t, s, 1)
	s.Close()

	// A failing migration leaves the previous schema and data intact and refuses to start.
	bad := withExtra(newMigration(nextName("bad"), "CREATE TABLE extra (x INTEGER); SELEC nonsense;"))
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync, migrations: bad}); err == nil {
		t.Fatal("bad migration accepted")
	}
	s = openTest(t, config{dataDir: dir})
	if v, _ := s.SchemaVersion(ctx); v != len(embeddedMigrations) {
		t.Fatalf("schema version %d after failed migration", v)
	}
	var n int
	s.reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'extra'").Scan(&n)
	if n != 0 {
		t.Fatal("half-applied migration left a table")
	}
	s.Close()

	// A good migration applies once.
	good := withExtra(newMigration(nextName("good"), "CREATE TABLE extra (x INTEGER) STRICT;"))
	s = openTest(t, config{dataDir: dir, migrations: good})
	if v, _ := s.SchemaVersion(ctx); v != len(embeddedMigrations)+1 {
		t.Fatalf("schema version %d, want %d", v, len(embeddedMigrations)+1)
	}
	s.Close()
	s = openTest(t, config{dataDir: dir, migrations: good})
	s.Close()

	// A binary that does not know migration 2 refuses the newer database.
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync}); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("older binary: %v", err)
	}
	// An edited applied migration is refused.
	edited := withExtra(newMigration(nextName("good"), "CREATE TABLE extra (x INTEGER, y INTEGER) STRICT;"))
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync, migrations: edited}); !errors.Is(err, ErrMigrationEdited) {
		t.Fatalf("edited migration: %v", err)
	}
}

func TestStartupDetectsHeadMismatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	ids := createAccounts(t, s, 1)
	for i := 0; i < 2; i++ {
		if _, err := appendWithCallerRow(ctx, s, ids[0]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.writer.ExecContext(ctx, "UPDATE account_logs SET head_seq = 1"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync}); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("head mismatch not detected: %v", err)
	}
}

// Migration 0009 gives session effects stored before copied installations their session's author
// (a finalization also its target) and statistics cuts their author, in
// canonical key order, and leaves effects that already name one untouched.
func TestMigrationSessionEffectAuthors(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:8]})
	s.Close()
	db, err := sql.Open("sqlite", filepath.Join(dir, "nottennapod.db")) // foreign keys off: rows without their parents
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]string{
		"start":    `{"activity":true,"installation":"i","kind":"session_start","position":0,"session":"s","time":1}`,
		"fin":      `{"claim":"start","completed":true,"history":false,"installation":"i","kind":"session_finalize","ordinal":1,"session":"s","time":5}`,
		"cut":      `{"cuts":[{"ordinal":1,"session":"s"},{"ordinal":3,"session":"unknown"}],"kind":"statistics_reset"}`,
		"new":      `{"author":"b","contribution":5,"kind":"session_checkpoint","ordinal":2,"session":"s"}`,
		"newcut":   `{"cuts":[{"author":"b","ordinal":2,"session":"s"}],"kind":"statistics_reset"}`,
		"favorite": `{"kind":"favorite","on":true}`,
	}
	want := map[string]string{
		"start":    `{"activity":true,"author":"a","installation":"i","kind":"session_start","position":0,"session":"s","time":1}`,
		"fin":      `{"author":"a","claim":"start","completed":true,"history":false,"installation":"i","kind":"session_finalize","ordinal":1,"session":"s","target":"a","time":5}`,
		"cut":      `{"cuts":[{"author":"a","ordinal":1,"session":"s"},{"ordinal":3,"session":"unknown"}],"kind":"statistics_reset"}`,
		"new":      rows["new"],
		"newcut":   rows["newcut"],
		"favorite": rows["favorite"],
	}
	if _, err := db.Exec(`INSERT INTO listening_sessions (account_id, session_id, scope, author_id, start_node) VALUES ('acct', 's', 'episode:x', 'a', 'start')`); err != nil {
		t.Fatal(err)
	}
	for node, effect := range rows {
		if _, err := db.Exec(`INSERT INTO contributions (account_id, scope, node_id, effect) VALUES ('acct', 'episode:x', ?, ?)`, node, effect); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	s = openTest(t, config{dataDir: dir})
	for node, w := range want {
		var got string
		if err := s.reader.QueryRow(`SELECT effect FROM contributions WHERE node_id = ?`, node).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != w {
			t.Errorf("%s:\n got %s\nwant %s", node, got, w)
		}
	}
}

// Migration 0010 places archived facts an earlier version left unplaced (its rejected facts):
// a free position is held, a taken one roots a derived branch, and a later
// fact of that chain inherits the branch; admitted facts keep their placement.
func TestMigrationPlacesArchivedFacts(t *testing.T) {
	const (
		acct = "a0000000-0000-4000-8000-000000000001"
		ns   = "a0000000-0000-4000-8000-000000000002"
		u    = "a0000000-0000-4000-8000-0000000000aa"
	)
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:9]})
	if err := s.Write(context.Background(), func(tx *WriteTx) error {
		_, err := tx.CreateAccountWithIDs(acct, ns)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	db, err := sql.Open("sqlite", filepath.Join(dir, "nottennapod.db"))
	if err != nil {
		t.Fatal(err)
	}
	op := func(n int) string { return fmt.Sprintf("a0000000-0000-4000-8000-%012d", n) }
	fact := func(n, seq int, prev string) string {
		f := fmt.Sprintf(`{"author_id":"%s","author_seq":%d,"op_id":"%s"`, u, seq, op(n))
		if prev != "" {
			f += `,"previous_op_id":"` + prev + `"`
		}
		return f + "}"
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	add := func(n, seq int, prev, outcome string, epochSeq int) {
		exec(`INSERT INTO facts (account_id, op_id, content_hash, fact, receipt, type, type_version, outcome, frontier, epoch, seq)
			VALUES (?, ?, 'h', ?, x'00', 't', 1, ?, '[]', 1, ?)`, acct, op(n), []byte(fact(n, seq, prev)), outcome, epochSeq)
		if outcome != "rejected" { // admitted earlier: placed by migration 0007's backfill
			exec(`INSERT INTO fact_authors (account_id, op_id, raw_author, author_seq, effective_author) VALUES (?, ?, ?, ?, ?)`, acct, op(n), u, seq, u)
			exec(`INSERT INTO author_positions (account_id, author_id, author_seq, op_id) VALUES (?, ?, ?, ?)`, acct, u, seq, op(n))
		}
	}
	add(1, 1, "", "applied", 1)
	add(2, 2, op(1), "rejected", 2) // a rejected writing tip: (u, 2) is free
	add(3, 2, op(1), "rejected", 3) // a copy's fact at the taken (u, 2): a derived branch
	add(4, 3, op(3), "rejected", 4) // the copy's next fact inherits the branch
	add(5, 3, op(2), "applied", 5)  // admitted earlier at (u, 3): keeps its placement
	db.Close()
	s = openTest(t, config{dataDir: dir})
	want := map[int]string{1: u, 2: u, 3: contract.DerivedAuthorID(ns, op(3)), 4: contract.DerivedAuthorID(ns, op(3)), 5: u}
	err = s.Write(context.Background(), func(tx *WriteTx) error {
		a := tx.Account(acct)
		for n, w := range want {
			fa, ok, err := a.FactAuthorOf(op(n))
			if err != nil {
				return err
			}
			if !ok || fa.EffectiveAuthor != w {
				t.Errorf("fact %d: placed %v as %s, want %s", n, ok, fa.EffectiveAuthor, w)
			}
		}
		parent, tip, err := a.Lineage(contract.DerivedAuthorID(ns, op(3)))
		if err != nil || parent != u || tip != op(1) {
			t.Errorf("the branch's lineage: %s %s %v", parent, tip, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
