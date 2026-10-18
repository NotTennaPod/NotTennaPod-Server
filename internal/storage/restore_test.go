package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/keys"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

// PROTOCOL §11: the restore procedure, its refusals and its crash safety. The fixture
// is a server whose backup is followed by the changes PROTOCOL §11.4 is about: a
// password reset, a revocation, an account deleted and its username reused, and an
// account created.

type restoreFixture struct {
	dir    string // the stopped server's data directory
	backup string
	bundle []byte
	pass   []byte
	ids    map[string]string // username at backup time → account ID
	tokens map[string][]byte // username → a device token issued before the backup
}

const fixturePass = "correct horse battery staple"

func signed(t *testing.T, key ReceiptKey, account string, obj map[string]any) []byte {
	t.Helper()
	obj["key_id"] = key.ID
	mac, err := contract.SignedMAC(key.Secret, account, obj)
	if err != nil {
		t.Fatal(err)
	}
	obj["mac"] = mac
	b, err := contract.Canonical(obj)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// newRestoreFixture builds the fixture: four accounts with logins and tokens,
// alice's signed receipt and decision and sealed secret, feed response and push
// part; a backup; then, after it, alice's password reset, bob's installation
// revoked, carol deleted and her username given to bob, and dave created.
func newRestoreFixture(t *testing.T) *restoreFixture {
	t.Helper()
	ctx := context.Background()
	fx := &restoreFixture{dir: t.TempDir(), pass: []byte(fixturePass), ids: map[string]string{}, tokens: map[string][]byte{}}
	s := openTest(t, config{dataDir: fx.dir})
	box, err := sealed.LoadOrCreate(fx.dir)
	if err != nil {
		t.Fatal(err)
	}
	key := s.receiptKeys[0]
	for _, name := range []string{"root", "alice", "bob", "carol"} {
		if err := s.Write(ctx, func(w *WriteTx) error {
			a, err := w.CreateAccount()
			if err != nil {
				return err
			}
			fx.ids[name] = a.ID
			return w.CreateLogin(a.ID, name, name, "hash-"+name, name == "root")
		}); err != nil {
			t.Fatal(err)
		}
		_, fx.tokens[name] = newToken(t, s, fx.ids[name])
		appendWithCallerRow(ctx, s, fx.ids[name])
	}
	alice := fx.ids["alice"]
	install, _ := newToken(t, s, alice)
	if err := s.Write(ctx, func(w *WriteTx) error {
		a := w.Account(alice)
		factObj := map[string]any{"op_id": "op-1", "type": "test.note"}
		factBytes, err := contract.Canonical(factObj)
		if err != nil {
			return err
		}
		hash, err := contract.ContentHash(factObj)
		if err != nil {
			return err
		}
		receipt := signed(t, key, alice, map[string]any{"op_id": "op-1", "content_hash": hash, "outcome": "applied", "normalized_time": int64(5),
			"epoch": int64(1), "seq": int64(2), "index": int64(0)})
		if err := a.InsertFact(FactRow{OpID: "op-1", ContentHash: hash, Fact: factBytes, Receipt: receipt, Type: "test.note", TypeVersion: 1,
			Outcome: "applied", Frontier: "[]", Epoch: 1, Seq: 2}); err != nil {
			return err
		}
		if err := a.InsertNode("d-1", "decision", 5, nil); err != nil {
			return err
		}
		decision := signed(t, key, alice, map[string]any{"decision_id": "d-1", "decision_type": "test", "subject": "s", "epoch": int64(1), "seq": int64(2),
			"index": int64(1), "normalized_time": int64(5)})
		if err := a.InsertDecision(DecisionRow{DecisionID: "d-1", DecisionType: "test", Subject: "s", Epoch: 1, Seq: 2, NormalizedTime: 5, Decision: decision}); err != nil {
			return err
		}
		if err := a.PutSecret("sec-1", box.Seal([]byte(`{"password":"p"}`), []byte("secret\x00"+alice+"\x00sec-1"))); err != nil {
			return err
		}
		if _, err := w.tx.ExecContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, 'p1:x', 'op-1')`, alice); err != nil {
			return err
		}
		if err := a.PutFeedResponse("p1:x", box.Seal([]byte("feed"), []byte("feed-response\x00"+alice+"\x00p1:x"))); err != nil {
			return err
		}
		return a.StorePart(install, 7, PartRow{Index: 1, TransactionID: "tx", Count: 2, ContentHash: "h",
			Data: box.Seal([]byte("part"), []byte(fmt.Sprintf("part\x00%s\x00%s\x00%d\x00%d", alice, install, 7, 1)))})
	}); err != nil {
		t.Fatal(err)
	}
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fx.backup = info.Path
	// After the backup.
	bobInstall := tokenInstall(t, s, fx.tokens["bob"])
	for _, step := range []func(w *WriteTx) error{
		func(w *WriteTx) error { return w.Account(alice).SetPassword("hash-alice-reset", nil) },
		func(w *WriteTx) error { return w.Account(fx.ids["bob"]).RevokeInstallation(bobInstall) },
		func(w *WriteTx) error { return w.DeleteAccount(fx.ids["carol"]) },
		func(w *WriteTx) error { return w.Account(fx.ids["bob"]).Rename("carol", "carol") },
		func(w *WriteTx) error {
			a, err := w.CreateAccount()
			if err != nil {
				return err
			}
			return w.CreateLogin(a.ID, "dave", "dave", "hash-dave", false)
		},
	} {
		if err := s.Write(ctx, step); err != nil {
			t.Fatal(err)
		}
	}
	appendWithCallerRow(ctx, s, alice)
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := s.ExportKeys(ctx, path, fx.pass); err != nil {
		t.Fatal(err)
	}
	if fx.bundle, err = os.ReadFile(path); err != nil {
		t.Fatal(err)
	}
	s.Close()
	return fx
}

func tokenInstall(t *testing.T, s *Store, hash []byte) string {
	t.Helper()
	var install string
	if err := s.reader.QueryRow(`SELECT install_id FROM auth_tokens WHERE token_hash = ?`, hash).Scan(&install); err != nil {
		t.Fatal(err)
	}
	return install
}

// newRestoreFixtureIn is the fixture with its backup in the given format.
func newRestoreFixtureIn(t *testing.T, format int) *restoreFixture {
	t.Helper()
	fx := newRestoreFixture(t)
	if format == 1 {
		fx.backup = asFormat(t, fx.backup, fx.dir, 1)
	}
	return fx
}

// format is the fixture's backup's format.
func (fx *restoreFixture) format(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile(fx.backup)
	if err != nil {
		t.Fatal(err)
	}
	return backupFormat(bytes.NewReader(data), int64(len(data)))
}

// clone copies the fixture's data directory (and backup) for one run.
func (fx *restoreFixture) clone(t *testing.T) *restoreFixture {
	t.Helper()
	c := *fx
	c.dir = t.TempDir()
	entries, err := os.ReadDir(fx.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(fx.dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(c.dir, e.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := os.ReadFile(fx.backup)
	c.backup = filepath.Join(t.TempDir(), filepath.Base(fx.backup))
	os.WriteFile(c.backup, data, 0o600)
	return &c
}

func (fx *restoreFixture) restore(cfg config) (*RestoreReport, error) {
	cfg.dataDir = fx.dir
	if cfg.syncDir == nil {
		cfg.syncDir = noSync
	}
	return restore(context.Background(), cfg, RestoreOptions{DataDir: fx.dir, Backup: fx.backup, Bundle: fx.bundle, Passphrase: fx.pass})
}

// dump is the database's content, table by table, with the values that are
// random or clock-dependent per run replaced.
func dump(t *testing.T, path string) map[string][]string {
	t.Helper()
	db, err := openReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tables, err := queryStrings(context.Background(), db, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM ` + table)
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
			var parts []string
			for i, c := range cols {
				v := fmt.Sprintf("%v", vals[i])
				if b, ok := vals[i].([]byte); ok {
					v = hex.EncodeToString(b)
				}
				switch {
				case table == "server_meta" && c == "value" && vals[0] == metaIncarnation,
					table == "account_logins" && c == "credentials_changed_at",
					table == "schema_migrations" && c == "applied_at":
					v = "*"
				}
				parts = append(parts, c+"="+v)
			}
			out[table] = append(out[table], strings.Join(parts, " "))
		}
		rows.Close()
		slices.Sort(out[table])
	}
	return out
}

func loginOf(t *testing.T, path, account string) (username, hash, state string, version int64) {
	t.Helper()
	db, err := openReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.QueryRow(`SELECT l.username, l.password_hash, a.state, l.credentials_version FROM accounts a JOIN account_logins l USING (account_id)
		WHERE a.account_id = ?`, account).Scan(&username, &hash, &state, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return
}

// PROTOCOL §11.4 at the storage level.
func TestRestoreProcedure(t *testing.T) { forEachFormat(t, testRestoreProcedure) }

func testRestoreProcedure(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format).clone(t)
	live := filepath.Join(fx.dir, databaseName)
	before := readState(t, fx.dir)
	_, aliceHash, _, aliceVersion := loginOf(t, live, fx.ids["alice"])
	var out bytes.Buffer
	rep, err := restore(context.Background(), config{dataDir: fx.dir, syncDir: noSync},
		RestoreOptions{DataDir: fx.dir, Backup: fx.backup, Bundle: fx.bundle, Passphrase: fx.pass, Out: &out})
	if err != nil {
		t.Fatal(err)
	}
	// Every account is at epoch 1 (the high-water mark of a first start with no
	// accounts is 0): E is 2.
	want := &RestoreReport{Backup: fx.backup, Epoch: max(before.EpochHighWater, 1) + 1, PreRestore: rep.PreRestore,
		LoginsFrom:       "carried from the previous database where the account still exists",
		Restored:         []string{"alice", "carol", "carol-restored", "root"},
		NotRestored:      []string{"dave"},
		ReturnedDisabled: []string{"carol-restored"},
		Renamed:          []string{"carol → carol-restored"},
		LoginsCarried:    []string{"alice", "carol", "root"},
		LoginsKept:       []string{"carol-restored"},
	}
	if !reflect.DeepEqual(rep, want) {
		t.Fatalf("report\n%+v\nwant\n%+v", rep, want)
	}
	if !strings.HasPrefix(rep.PreRestore, preRestorePrefix) || !strings.Contains(out.String(), "carol-restored") {
		t.Fatalf("pre-restore %q, printed %q", rep.PreRestore, out.String())
	}
	// The previous database is kept, whole.
	if _, hash, _, _ := loginOf(t, filepath.Join(fx.dir, rep.PreRestore), fx.ids["alice"]); hash != aliceHash {
		t.Fatal("the pre-restore database is not the previous database")
	}
	// A password reset after the backup survives; credentials version + 1.
	if name, hash, state, version := loginOf(t, live, fx.ids["alice"]); hash != "hash-alice-reset" || version != aliceVersion+1 || state != "active" || name != "alice" {
		t.Fatalf("alice: %s %s %s %d", name, hash, state, version)
	}
	// A renamed account keeps its new name; the deleted one returns disabled.
	if name, _, state, _ := loginOf(t, live, fx.ids["bob"]); name != "carol" || state != "active" {
		t.Fatalf("bob: %s %s", name, state)
	}
	if name, hash, state, _ := loginOf(t, live, fx.ids["carol"]); name != "carol-restored" || hash != "hash-carol" || state != "disabled" {
		t.Fatalf("carol: %s %s %s", name, hash, state)
	}
	// The restored database opens without a detection, at the new epoch, with
	// no credential at all and the backup's data.
	s := openTest(t, config{dataDir: fx.dir})
	if s.restoredBecause != "" {
		t.Fatalf("detected after the procedure: %s", s.restoredBecause)
	}
	for id, e := range epochsOf(t, s) {
		if e != rep.Epoch {
			t.Errorf("account %s at epoch %d, want %d", id, e, rep.Epoch)
		}
	}
	for name, tok := range fx.tokens {
		if tokenWorks(t, s, tok) {
			t.Errorf("%s's token survived the restore", name)
		}
	}
	db := rawDB(t, fx.dir)
	for q, n := range map[string]int{
		`SELECT COUNT(*) FROM accounts`:                                   4,
		`SELECT COUNT(*) FROM auth_tokens`:                                0,
		`SELECT COUNT(*) FROM push_parts`:                                 0,
		`SELECT COUNT(*) FROM secrets`:                                    1,
		`SELECT COUNT(*) FROM server_meta WHERE key = 'backup'`:           0,
		`SELECT COUNT(*) FROM receipt_keys`:                               0,
		`SELECT COUNT(*) FROM installations WHERE revoked_at IS NOT NULL`: 0,
	} {
		if got := countRows(t, db, q); got != n {
			t.Errorf("%s: %d, want %d", q, got, n)
		}
	}
	st := readState(t, fx.dir)
	if st.Restore != nil || st.EpochHighWater != rep.Epoch || st.KeysExportedAt != before.KeysExportedAt {
		t.Fatalf("metadata %+v", st)
	}
	assertSettled(t, s)
}

// PROTOCOL §11.4: a fault after each recorded step and at each metadata write and file
// operation, then the procedure (or a server start) runs again: every run
// reaches the same database, metadata and report; the high-water mark never
// decreases and the new epoch is above every earlier one.
func TestRestoreResumesAfterACrashAtEveryPoint(t *testing.T) {
	forEachFormat(t, testRestoreResumesAfterACrashAtEveryPoint)
}

func testRestoreResumesAfterACrashAtEveryPoint(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	ref := fx.clone(t)
	refReport, err := ref.restore(config{})
	if err != nil {
		t.Fatal(err)
	}
	refDump := dump(t, filepath.Join(ref.dir, databaseName))
	refState := readState(t, ref.dir)
	hw := readState(t, fx.dir).EpochHighWater
	crash := errors.New("crash")
	type point struct{ kind, name string }
	var points []point
	for _, step := range []string{stepStage, stepTransition, stepSwap, stepDone, "clear"} {
		points = append(points, point{"before", "restore-" + step}, point{"after", "restore-" + step})
	}
	faults := []string{"staged-source-copied", "staged-copied", "staged-migrated", "staged-transitioned", "old-wal-moved", "old-moved", "staged-moved"}
	if format == 2 {
		faults = append(faults, "verify-copied", "verify-authenticated", "verify-decompressed", "staged-authenticated", "staged-decompressed",
			"staged-source-removed")
	}
	for _, f := range faults {
		points = append(points, point{"fault", f})
	}
	for i, p := range points {
		t.Run(p.kind+" "+p.name, func(t *testing.T) {
			c := fx.clone(t)
			fail := func(name string) error {
				if name == p.name {
					return crash
				}
				return nil
			}
			var h hooks
			switch p.kind {
			case "before":
				h.beforeStateWrite = fail
			case "after":
				h.afterStateWrite = fail
			default:
				h.restoreFault = fail
			}
			if _, err := c.restore(config{hooks: h}); !errors.Is(err, crash) {
				t.Fatalf("faulted run: %v", err)
			}
			mid := readState(t, c.dir)
			if mid.EpochHighWater < hw {
				t.Fatalf("high-water mark decreased: %d < %d", mid.EpochHighWater, hw)
			}
			rep := refReport
			switch {
			case p.kind == "after" && p.name == "restore-clear":
				// Finished; nothing to resume.
			case mid.Restore != nil && i%2 == 0:
				// A server start finishes it.
				s, err := open(context.Background(), config{dataDir: c.dir, syncDir: noSync})
				if err != nil {
					t.Fatal(err)
				}
				if s.restoredBecause != "" {
					t.Fatalf("the start after the restore detected: %s", s.restoredBecause)
				}
				rep = nil
				s.Close()
			default:
				if rep, err = c.restore(config{}); err != nil {
					t.Fatal(err)
				}
			}
			if rep != nil {
				got, want := *rep, *refReport
				got.PreRestore, want.PreRestore = "", ""
				got.Backup = filepath.Base(got.Backup)
				want.Backup = filepath.Base(want.Backup)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("report\n%+v\nwant\n%+v", got, want)
				}
			}
			if d := dump(t, filepath.Join(c.dir, databaseName)); !reflect.DeepEqual(d, refDump) {
				for table := range refDump {
					if !reflect.DeepEqual(d[table], refDump[table]) {
						t.Errorf("table %s\n%v\nwant\n%v", table, d[table], refDump[table])
					}
				}
				t.FailNow()
			}
			st := readState(t, c.dir)
			if st.Restore != nil || st.EpochHighWater != refState.EpochHighWater || st.AuthRevision != refState.AuthRevision ||
				st.PendingAuthRevision != 0 || st.NextIncarnation != "" || st.ServerID != refState.ServerID {
				t.Fatalf("metadata %+v, want like %+v", st, refState)
			}
			if st.EpochHighWater <= hw {
				t.Fatalf("new epoch %d not above %d", st.EpochHighWater, hw)
			}
			pre, _ := filepath.Glob(filepath.Join(c.dir, preRestorePrefix+"*"))
			if len(pre) != 1 {
				t.Fatalf("pre-restore files %v", pre)
			}
			if left := restoreLeftovers(t, c.dir); len(left) > 0 {
				t.Fatalf("left in the data directory: %v", left)
			}
			s := openTest(t, config{dataDir: c.dir})
			if s.restoredBecause != "" {
				t.Fatalf("detected after the restore: %s", s.restoredBecause)
			}
			assertSettled(t, s)
		})
	}
}

// restoreLeftovers lists what a restore may leave only while it runs: the
// staged database and its source, and the temporaries of its verification.
func restoreLeftovers(t *testing.T, dir string) []string {
	t.Helper()
	var left []string
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), stagedName) || strings.HasPrefix(e.Name(), restoreTempPrefix) {
			left = append(left, e.Name())
		}
	}
	return left
}

// snapshotDir hashes every file of a data directory.
func snapshotDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		out[e.Name()] = hex.EncodeToString(sum[:])
	}
	return out
}

// resigned changes a copy of the fixture's backup's database and signs it
// again, in the backup's format, with the server's key, as only a holder of
// the key bundle could: the checks behind the backup MAC see it.
func (fx *restoreFixture) resigned(t *testing.T, change func(path string)) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tampered.db")
	if err := os.WriteFile(path, rawDatabaseOf(t, fx.backup), 0o600); err != nil {
		t.Fatal(err)
	}
	change(path)
	db, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	serverID, ks, _, err := readReceiptKeysFile(fx.dir)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), filepath.Base(fx.backup))
	writeSigned(t, out, db, serverID, ks[0], fx.format(t))
	return out
}

// tamper changes a copy of the backup with SQL and signs it again.
func (fx *restoreFixture) tamper(t *testing.T, query string, args ...any) string {
	t.Helper()
	return fx.resigned(t, func(path string) {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if res, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		} else if n, _ := res.RowsAffected(); n == 0 && strings.HasPrefix(query, "UPDATE") {
			t.Fatalf("tamper changed nothing: %s", query)
		}
	})
}

// PROTOCOL §11.4: every refusal leaves the data directory exactly as it was.
func TestRestoreRefusalsChangeNothing(t *testing.T) {
	forEachFormat(t, testRestoreRefusalsChangeNothing)
}

func testRestoreRefusalsChangeNothing(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	other := t.TempDir()
	o := openTest(t, config{dataDir: other})
	otherKeys := filepath.Join(t.TempDir(), "other.json")
	if err := o.ExportKeys(context.Background(), otherKeys, fx.pass); err != nil {
		t.Fatal(err)
	}
	otherInfo, err := o.Backup(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.Close()
	otherBackup := asFormat(t, otherInfo.Path, other, format)
	otherBundle, _ := os.ReadFile(otherKeys)
	b, err := keys.Open(fx.bundle, fx.pass)
	if err != nil {
		t.Fatal(err)
	}
	b.ReceiptKeys[0].Key = bytes.Repeat([]byte{9}, 32)
	sameServerOtherKeys, _ := keys.Seal(b, fx.pass, 1)
	cases := []struct {
		name   string
		mutate func(t *testing.T, c *restoreFixture)
		want   string
	}{
		{"a wrong passphrase", func(t *testing.T, c *restoreFixture) { c.pass = []byte("wrong") }, "passphrase"},
		{"another server's bundle", func(t *testing.T, c *restoreFixture) { c.bundle = otherBundle }, "key bundle belongs to server"},
		{"another server's backup and bundle", func(t *testing.T, c *restoreFixture) { c.backup, c.bundle = otherBackup, otherBundle }, "the backup belongs to server"},
		{"keys that differ from the data directory's", func(t *testing.T, c *restoreFixture) { c.bundle = sameServerOtherKeys }, "differ from this data directory"},
		{"a backup secret that does not open", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `UPDATE secrets SET sealed = zeroblob(40)`)
		}, "sealed value in secrets"},
		{"a stored feed response that does not open", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `UPDATE feed_responses SET sealed = randomblob(64)`)
		}, "sealed value in feed_responses"},
		{"a push part that does not open", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `UPDATE push_parts SET part_index = 0`)
		}, "sealed value in push_parts"},
		{"a tampered receipt MAC", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `UPDATE facts SET receipt = CAST(replace(CAST(receipt AS TEXT), '"normalized_time":5', '"normalized_time":6') AS BLOB)`)
		}, "receipt of op-1"},
		{"a fact that does not match its signed receipt", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `UPDATE facts SET fact = CAST('{"op_id":"op-1","type":"test.other"}' AS BLOB)`)
		}, "does not match its signed receipt"},
		{"a tampered decision MAC", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `UPDATE decisions SET decision = CAST(replace(CAST(decision AS TEXT), '"subject":"s"', '"subject":"t"') AS BLOB)`)
		}, "decision of d-1"},
		{"a damaged backup", func(t *testing.T, c *restoreFixture) {
			c.backup = c.resigned(t, func(path string) {
				data, _ := os.ReadFile(path)
				for i := len(data) / 2; i < len(data)/2+4096 && i < len(data); i++ {
					data[i] = 0xA5
				}
				os.WriteFile(path, data, 0o600)
			})
		}, "does not verify"},
		// The MAC, and the exact
		// schema behind it.
		{"a changed backup", func(t *testing.T, c *restoreFixture) {
			data, _ := os.ReadFile(c.backup)
			data[len(data)/2] ^= 1
			c.backup = filepath.Join(t.TempDir(), "changed.db")
			os.WriteFile(c.backup, data, 0o600)
		}, "has been changed since it was made"},
		{"an unsigned backup", func(t *testing.T, c *restoreFixture) {
			db := rawDatabaseOf(t, c.backup)
			c.backup = filepath.Join(t.TempDir(), "unsigned.db")
			os.WriteFile(c.backup, db, 0o600)
		}, "is unsigned"},
		{"a backup without its trailer", func(t *testing.T, c *restoreFixture) {
			data, _ := os.ReadFile(c.backup)
			c.backup = filepath.Join(t.TempDir(), "cut.db")
			os.WriteFile(c.backup, data[:len(data)-40], 0o600)
		}, "is unsigned"},
		{"a backup signed with another server's key", func(t *testing.T, c *restoreFixture) {
			serverID, ks, _, _ := readReceiptKeysFile(c.dir)
			c.backup = filepath.Join(t.TempDir(), filepath.Base(c.backup))
			writeSigned(t, c.backup, rawDatabaseOf(t, fx.backup), serverID, ReceiptKey{ID: ks[0].ID, Secret: bytes.Repeat([]byte{3}, 32)}, format)
		}, "has been changed since it was made"},
		{"a trigger of its own", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `CREATE TRIGGER keep_tokens AFTER DELETE ON auth_tokens BEGIN SELECT 1; END`)
		}, "trigger keep_tokens of its own"},
		{"a view of its own", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `CREATE VIEW accounts_view AS SELECT * FROM accounts`)
		}, "view accounts_view of its own"},
		{"an index of its own", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `CREATE INDEX extra ON accounts (state)`)
		}, "index extra of its own"},
		{"a column of its own", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `ALTER TABLE auth_tokens ADD COLUMN extra TEXT`)
		}, "a different table auth_tokens"},
		{"a table of its own", func(t *testing.T, c *restoreFixture) {
			c.backup = c.tamper(t, `CREATE TABLE extra (x)`)
		}, "table extra of its own"},
		{"a missing index", func(t *testing.T, c *restoreFixture) {
			var index string
			plain := filepath.Join(t.TempDir(), "plain.db")
			os.WriteFile(plain, rawDatabaseOf(t, c.backup), 0o600)
			db, _ := openReadOnly(plain)
			db.QueryRow(`SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL ORDER BY name LIMIT 1`).Scan(&index)
			db.Close()
			c.backup = c.tamper(t, `DROP INDEX `+index)
		}, "lacks index"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fx.clone(t)
			tc.mutate(t, c)
			before := snapshotDir(t, c.dir)
			_, err := c.restore(config{})
			if !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want a refusal mentioning %q", err, tc.want)
			}
			if after := snapshotDir(t, c.dir); !reflect.DeepEqual(after, before) {
				t.Fatalf("the refusal changed the data directory:\n%v\n%v", before, after)
			}
		})
	}
}

// A new machine: restoring into an empty data directory installs the bundle's
// keys, keeps the backup's logins and serves the backup's sealed data.
func TestRestoreIntoAnEmptyDataDirectory(t *testing.T) {
	forEachFormat(t, testRestoreIntoAnEmptyDataDirectory)
}

func testRestoreIntoAnEmptyDataDirectory(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	c := *fx
	c.dir = filepath.Join(t.TempDir(), "new")
	rep, err := c.restore(config{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.PreRestore != "" || !strings.HasPrefix(rep.LoginsFrom, "kept from the backup") || len(rep.LoginsKept) != 4 || len(rep.NotRestored) != 0 {
		t.Fatalf("report %+v", rep)
	}
	for _, name := range []string{receiptKeysName, keysSecretName} {
		a, _ := os.ReadFile(filepath.Join(fx.dir, name))
		b, _ := os.ReadFile(filepath.Join(c.dir, name))
		if !bytes.Equal(a, b) {
			t.Errorf("%s differs from the original server's", name)
		}
	}
	s := openTest(t, config{dataDir: c.dir})
	box, _ := sealed.LoadOrCreate(c.dir)
	var secret []byte
	alice := fx.ids["alice"]
	s.reader.QueryRow(`SELECT sealed FROM secrets WHERE account_id = ?`, alice).Scan(&secret)
	if _, err := box.Open(secret, []byte("secret\x00"+alice+"\x00sec-1")); err != nil {
		t.Fatal("the restored secret does not open with the installed key")
	}
	if _, hash, state, _ := loginOf(t, filepath.Join(c.dir, databaseName), fx.ids["carol"]); hash != "hash-carol" || state != "active" {
		t.Fatal("a backup login was not kept")
	}
	assertSettled(t, s)
}

// A restore of another backup that has already replaced the database is
// finished first; one that has not is replaced.
func TestRestoreOfAnotherBackupWhileOneIsUnfinished(t *testing.T) {
	forEachFormat(t, testRestoreOfAnotherBackupWhileOneIsUnfinished)
}

func testRestoreOfAnotherBackupWhileOneIsUnfinished(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format).clone(t)
	crash := errors.New("crash")
	if _, err := fx.restore(config{hooks: hooks{restoreFault: func(p string) error {
		if p == "staged-moved" {
			return crash
		}
		return nil
	}}}); !errors.Is(err, crash) {
		t.Fatal(err)
	}
	second := fx.clone(t)
	second.dir = fx.dir
	data, _ := os.ReadFile(fx.backup)
	second.backup = filepath.Join(t.TempDir(), "other-name"+filepath.Ext(fx.backup))
	os.WriteFile(second.backup, data, 0o600)
	if _, err := second.restore(config{}); !errors.Is(err, ErrRestoreInProgress) {
		t.Fatalf("another backup during a swapped restore: %v", err)
	}
	if _, err := fx.restore(config{}); err != nil {
		t.Fatal(err)
	}
}

// The protocol is silent on a restore that would leave no active administrator:
// the backup's only administrator was deleted after a new one, created after
// the backup, took over. The returning administrator stays active (and the
// report says so), or nobody could administer the server.
func TestRestoreKeepsAnAdministrator(t *testing.T) { forEachFormat(t, testRestoreKeepsAnAdministrator) }

func testRestoreKeepsAnAdministrator(t *testing.T, format int) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir})
	login := func(w *WriteTx, name string, admin bool) (string, error) {
		a, err := w.CreateAccount()
		if err != nil {
			return "", err
		}
		return a.ID, w.CreateLogin(a.ID, name, name, "hash", admin)
	}
	var first, user string
	if err := s.Write(ctx, func(w *WriteTx) error {
		var err error
		if first, err = login(w, "first", true); err != nil {
			return err
		}
		user, err = login(w, "user", false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backup := asFormat(t, info.Path, dir, format)
	if err := s.Write(ctx, func(w *WriteTx) error {
		if _, err := login(w, "second", true); err != nil {
			return err
		}
		return w.DeleteAccount(first)
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := s.ExportKeys(ctx, path, []byte(fixturePass)); err != nil {
		t.Fatal(err)
	}
	bundle, _ := os.ReadFile(path)
	s.Close()
	rep, err := restore(ctx, config{dataDir: dir, syncDir: noSync}, RestoreOptions{DataDir: dir, Backup: backup, Bundle: bundle, Passphrase: []byte(fixturePass)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rep.KeptActive, []string{"first"}) || len(rep.ReturnedDisabled) != 0 || !slices.Equal(rep.NotRestored, []string{"second"}) {
		t.Fatalf("report %+v", rep)
	}
	live := filepath.Join(dir, databaseName)
	if _, _, state, _ := loginOf(t, live, first); state != "active" {
		t.Fatalf("the returning administrator is %s", state)
	}
	if _, _, state, _ := loginOf(t, live, user); state != "active" {
		t.Fatalf("user is %s", state)
	}
}

// A restore is bound to the backup bytes it verified: a backup changed or
// missing before a resumed staging is refused, at the stage step, at the
// staging again after an interrupted transition, and when a start resumes
// it; the database it would replace is kept, and the same backup put back
// lets the restore finish.
func TestResumedRestoreRefusesAChangedBackup(t *testing.T) {
	forEachFormat(t, testResumedRestoreRefusesAChangedBackup)
}

func testResumedRestoreRefusesAChangedBackup(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	original, _ := os.ReadFile(fx.backup)
	for _, at := range []struct{ point, step string }{{"staged-copied", stepStage}, {"staged-transitioned", stepTransition}} {
		for _, change := range []string{"changed", "missing"} {
			for _, resume := range []string{"restore", "start"} {
				t.Run(at.step+"/"+change+"/"+resume, func(t *testing.T) {
					c := fx.clone(t)
					if _, err := c.restore(config{hooks: hooks{restoreFault: func(p string) error {
						if p == at.point {
							return errors.New("crash")
						}
						return nil
					}}}); err == nil {
						t.Fatal("the injected crash did not stop the restore")
					}
					if st := readState(t, c.dir); st.Restore == nil || st.Restore.Step != at.step {
						t.Fatalf("the record is %+v", st.Restore)
					}
					live := fileSHA(t, filepath.Join(c.dir, databaseName))
					want := "not the file that was verified"
					if change == "changed" {
						data, _ := os.ReadFile(c.tamper(t, `UPDATE secrets SET sealed = zeroblob(40)`))
						os.WriteFile(c.backup, data, 0o600)
					} else {
						os.Remove(c.backup)
						want = "cannot read the backup"
					}
					var err error
					if resume == "restore" {
						_, err = c.restore(config{})
					} else {
						var s *Store
						if s, err = open(context.Background(), config{dataDir: c.dir, syncDir: noSync}); err == nil {
							s.Close()
						}
					}
					if !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), want) {
						t.Fatalf("got %v, want a refusal mentioning %q", err, want)
					}
					if got := fileSHA(t, filepath.Join(c.dir, databaseName)); got != live {
						t.Fatal("the database the restore would replace changed")
					}
					os.WriteFile(c.backup, original, 0o600)
					if _, err := c.restore(config{}); err != nil {
						t.Fatalf("with the backup put back: %v", err)
					}
					assertSettled(t, openTest(t, config{dataDir: c.dir}))
				})
			}
		}
	}
}

// A backup with a journal file beside it is refused,
// and a staged copy that does not verify cryptographically is never used.
func TestRestoreRefusesSidecarsAndUnverifiedStagedCopies(t *testing.T) {
	forEachFormat(t, testRestoreRefusesSidecarsAndUnverifiedStagedCopies)
}

func testRestoreRefusesSidecarsAndUnverifiedStagedCopies(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	c := fx.clone(t)
	if err := os.WriteFile(c.backup+"-wal", []byte("wal"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.restore(config{}); !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), "journal files") {
		t.Fatalf("a backup with a WAL beside it: %v", err)
	}
	os.Remove(c.backup + "-wal")
	// Keys that no longer verify the staged copy (as if the data directory's
	// keys changed after verification) stop the restore at staging.
	crash := config{hooks: hooks{restoreFault: func(p string) error {
		if p == "staged-copied" {
			return errors.New("crash")
		}
		return nil
	}}}
	if _, err := c.restore(crash); err == nil {
		t.Fatal("the injected crash did not stop the restore")
	}
	if err := writeReceiptKeysFile(c.dir, fx.ids["root"], []ReceiptKey{{ID: "k1", Secret: bytes.Repeat([]byte{7}, 32)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.restore(config{}); !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), "staged copy does not verify") {
		t.Fatalf("an unverified staged copy: %v", err)
	}
}

// A refused restore into a new data directory (missing, or an empty folder)
// creates nothing there: not the folder, its parents or its lock, and the
// empty folder keeps its mode, so the folder is still new for the next try
// (PROTOCOL §11.4). The same folder then takes the right backup and bundle.
func TestRefusedRestoreIntoANewDataDirectoryCreatesNothing(t *testing.T) {
	forEachFormat(t, testRefusedRestoreIntoANewDataDirectoryCreatesNothing)
}

func testRefusedRestoreIntoANewDataDirectoryCreatesNothing(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	o := openTest(t, config{dataDir: t.TempDir()})
	otherKeys := filepath.Join(t.TempDir(), "other.json")
	if err := o.ExportKeys(context.Background(), otherKeys, fx.pass); err != nil {
		t.Fatal(err)
	}
	o.Close()
	otherBundle, _ := os.ReadFile(otherKeys)
	refusals := []struct {
		name   string
		mutate func(t *testing.T, c *restoreFixture)
	}{
		{"a wrong passphrase", func(t *testing.T, c *restoreFixture) { c.pass = []byte("wrong") }},
		{"another server's bundle", func(t *testing.T, c *restoreFixture) { c.bundle = otherBundle }},
		{"a changed backup", func(t *testing.T, c *restoreFixture) {
			data, _ := os.ReadFile(c.backup)
			data[len(data)/2] ^= 1
			c.backup = filepath.Join(t.TempDir(), "changed.db")
			os.WriteFile(c.backup, data, 0o600)
		}},
	}
	for _, folder := range []string{"missing", "empty"} {
		t.Run(folder, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "config", "data")
			if folder == "empty" {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				os.Chmod(dir, 0o755)
			}
			for _, r := range refusals {
				c := *fx
				c.dir = dir
				r.mutate(t, &c)
				if _, err := c.restore(config{}); !errors.Is(err, ErrRestoreRefused) {
					t.Fatalf("%s: %v, want a refusal", r.name, err)
				}
				if folder == "missing" {
					if _, err := os.Lstat(filepath.Join(root, "config")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("%s: the refusal created %s (%v)", r.name, filepath.Join(root, "config"), err)
					}
					continue
				}
				entries, _ := os.ReadDir(dir)
				fi, _ := os.Stat(dir)
				if len(entries) != 0 || fi.Mode().Perm() != 0o755 {
					t.Fatalf("%s: the refusal left %v in the folder, mode %v", r.name, entries, fi.Mode().Perm())
				}
			}
			c := *fx
			c.dir = dir
			if _, err := c.restore(config{}); err != nil {
				t.Fatal(err)
			}
			s := openTest(t, config{dataDir: dir})
			assertSettled(t, s)
		})
	}
}

// A server started in the new folder while its restore was being verified
// (it holds the lock only while it runs) is seen once the restore holds the
// lock: the restore is verified again against that server and refused.
func TestRestoreIntoANewFolderVerifiesAgainWhenTheFolderChanged(t *testing.T) {
	fx := newRestoreFixture(t)
	dir := filepath.Join(t.TempDir(), "data")
	c := *fx
	c.dir = dir
	_, err := c.restore(config{hooks: hooks{restoreFault: func(point string) error {
		if point == "verified-before-lock" {
			s, err := open(context.Background(), config{dataDir: dir, syncDir: noSync})
			if err != nil {
				return err
			}
			return s.Close()
		}
		return nil
	}}})
	if !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), "the backup belongs to server") {
		t.Fatalf("got %v, want a refusal naming the other server", err)
	}
}

// A restore opens the key bundle, so the keys count as exported from then on
// (PROTOCOL §11.1): at the bundle's time on a new machine, and a later export
// already recorded stays.
func TestRestoreRecordsTheKeyExport(t *testing.T) {
	fx := newRestoreFixture(t)
	b, err := keys.Open(fx.bundle, fx.pass)
	if err != nil || b.CreatedAt <= 0 {
		t.Fatalf("bundle %v %v", b.CreatedAt, err)
	}
	fresh := *fx
	fresh.dir = filepath.Join(t.TempDir(), "new")
	if _, err := fresh.restore(config{}); err != nil {
		t.Fatal(err)
	}
	if got := readState(t, fresh.dir).KeysExportedAt; got != b.CreatedAt {
		t.Fatalf("keys exported at %d after a restore into a new folder, want the bundle's %d", got, b.CreatedAt)
	}
	s := openTest(t, config{dataDir: fresh.dir})
	if s.KeysExportedAt() != b.CreatedAt {
		t.Fatalf("the restored server reports keys exported at %d", s.KeysExportedAt())
	}
	s.Close()
	later := b.CreatedAt + 60_000
	inPlace := fx.clone(t)
	m, err := loadMeta(inPlace.dir, &hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.update("test", func(st *restoreState) { st.KeysExportedAt = later }); err != nil {
		t.Fatal(err)
	}
	if _, err := inPlace.restore(config{}); err != nil {
		t.Fatal(err)
	}
	if got := readState(t, inPlace.dir).KeysExportedAt; got != later {
		t.Fatalf("keys exported at %d after a restore in place, want the later export %d", got, later)
	}
}

// A restore stages exactly the bytes it verified (PROTOCOL §11.4): a backup
// changed or replaced after its verification is refused before anything is
// written for the restore, leaving a new data directory as it was (missing,
// or empty with its mode) and an existing one unchanged, and the same folder
// then takes the backup as verified. A change after the copy was made no
// longer matters: this run never reads the backup again.
func TestRestoreStagesTheBytesItVerified(t *testing.T) {
	forEachFormat(t, testRestoreStagesTheBytesItVerified)
}

func testRestoreStagesTheBytesItVerified(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	original, err := os.ReadFile(fx.backup)
	if err != nil {
		t.Fatal(err)
	}
	flip := func(path string) error {
		data := slices.Clone(original)
		data[len(data)/2] ^= 1
		return os.WriteFile(path, data, 0o600) // the same file, changed in place
	}
	replace := func(path string) error { // the same bytes in another file
		tmp := path + ".new"
		if err := os.WriteFile(tmp, original, 0o600); err != nil {
			return err
		}
		return os.Rename(tmp, path)
	}
	at := func(point string, change func(string) error, path string) config {
		return config{hooks: hooks{restoreFault: func(p string) error {
			if p == point {
				return change(path)
			}
			return nil
		}}}
	}
	for _, tc := range []struct {
		name, point string
		change      func(string) error
	}{
		{"changed before the lock", "verified-before-lock", flip},
		{"replaced before the lock", "verified-before-lock", replace},
		{"changed after the lock", "verified", flip},
	} {
		for _, folder := range []string{"missing", "empty"} {
			t.Run(tc.name+"/"+folder, func(t *testing.T) {
				c := fx.clone(t)
				root := t.TempDir()
				c.dir = filepath.Join(root, "config", "data")
				if folder == "empty" {
					os.MkdirAll(c.dir, 0o755)
					os.Chmod(c.dir, 0o755)
				}
				if _, err := c.restore(at(tc.point, tc.change, c.backup)); !errors.Is(err, ErrRestoreRefused) ||
					!strings.Contains(err.Error(), "not the file that was verified") {
					t.Fatalf("got %v, want a refusal", err)
				}
				if folder == "missing" {
					if _, err := os.Lstat(filepath.Join(root, "config")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("the refusal left %s (%v)", filepath.Join(root, "config"), err)
					}
				} else {
					entries, _ := os.ReadDir(c.dir)
					fi, _ := os.Stat(c.dir)
					if len(entries) != 0 || fi.Mode().Perm() != 0o755 {
						t.Fatalf("the refusal left %v in the folder, mode %v", entries, fi.Mode().Perm())
					}
				}
				if err := os.WriteFile(c.backup, original, 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := c.restore(config{}); err != nil {
					t.Fatalf("the folder no longer takes the backup: %v", err)
				}
				assertSettled(t, openTest(t, config{dataDir: c.dir}))
			})
		}
	}
	t.Run("changed in place, existing data directory", func(t *testing.T) {
		c := fx.clone(t)
		before := snapshotDir(t, c.dir)
		if _, err := c.restore(at("verified", flip, c.backup)); !errors.Is(err, ErrRestoreRefused) ||
			!strings.Contains(err.Error(), "not the file that was verified") {
			t.Fatalf("got %v, want a refusal", err)
		}
		if after := snapshotDir(t, c.dir); !reflect.DeepEqual(after, before) {
			t.Fatalf("the refusal changed the data directory:\n%v\n%v", before, after)
		}
	})
	t.Run("changed after the copy", func(t *testing.T) {
		c := fx.clone(t)
		c.dir = filepath.Join(t.TempDir(), "data")
		if _, err := c.restore(at("staged-copied", flip, c.backup)); err != nil {
			t.Fatalf("a change after the verified bytes were copied: %v", err)
		}
		assertSettled(t, openTest(t, config{dataDir: c.dir}))
	})
}

// A copy staged by a restore that stopped before recording itself is
// removed at the next start.
func TestAStagedCopyWithoutARestoreIsRemovedAtStart(t *testing.T) {
	forEachFormat(t, testAStagedCopyWithoutARestoreIsRemovedAtStart)
}

func testAStagedCopyWithoutARestoreIsRemovedAtStart(t *testing.T, format int) {
	fx := newRestoreFixtureIn(t, format)
	c := fx.clone(t)
	crash := errors.New("crash")
	if _, err := c.restore(config{hooks: hooks{beforeStateWrite: func(phase string) error {
		if phase == "restore-"+stepStage {
			return crash
		}
		return nil
	}}}); !errors.Is(err, crash) {
		t.Fatalf("the injected crash did not stop the restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(c.dir, stagedSourceName)); err != nil {
		t.Fatalf("no staged copy before the start: %v", err)
	}
	if st := readState(t, c.dir); st.Restore != nil {
		t.Fatalf("the restore was recorded: %+v", st.Restore)
	}
	openTest(t, config{dataDir: c.dir}).Close()
	if left := restoreLeftovers(t, c.dir); len(left) > 0 {
		t.Fatalf("the staged copy is left after the start: %v", left)
	}
}
