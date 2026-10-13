package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// PROTOCOL §11: startup detection of older or copied databases, the
// two-phase incarnation, the auth revision and the restore transition. Every
// "copy" here is made the way an operator or a backup tool would make one, and
// put back with the server stopped.

// ---- helpers

func readState(t *testing.T, dir string) *restoreState {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, stateName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var st restoreState
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatal(err)
	}
	return &st
}

func epochsOf(t *testing.T, s *Store) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	if err := s.Read(context.Background(), func(r *ReadTx) error {
		ids, err := r.AccountIDs()
		if err != nil {
			return err
		}
		for _, id := range ids {
			h, err := r.Account(id).Head()
			if err != nil {
				return err
			}
			out[id] = h.Epoch
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// newToken registers an installation in account and gives it a device token.
func newToken(t *testing.T, s *Store, account string) (install string, hash []byte) {
	t.Helper()
	install = newUUID()
	sum := sha256.Sum256([]byte(newUUID()))
	hash = sum[:]
	if err := s.Write(context.Background(), func(w *WriteTx) error {
		a := w.Account(account)
		if err := a.InsertInstallation(Installation{InstallID: install, InstanceNonce: newUUID(), Name: "phone", Platform: "ios", AppVersion: "1",
			Capabilities: "[]"}); err != nil {
			return err
		}
		return a.InsertCredential(hash, "device", install, nil)
	}); err != nil {
		t.Fatal(err)
	}
	return install, hash
}

func tokenWorks(t *testing.T, s *Store, hash []byte) bool {
	t.Helper()
	err := s.Read(context.Background(), func(r *ReadTx) error {
		_, err := r.CredentialByHash(hash)
		return err
	})
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	return err == nil
}

func revoke(s *Store, account, install string) error {
	return s.Write(context.Background(), func(w *WriteTx) error { return w.Account(account).RevokeInstallation(install) })
}

// rawCopy copies the live database the way a file-level backup tool would: a
// consistent snapshot of the file, with no backup marker.
func rawCopy(t *testing.T, s *Store) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "copy.db")
	if err := s.copyInto(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// writeInPlace overwrites the stopped server's database file, keeping its
// inode (cp over the file).
func writeInPlace(t *testing.T, dir string, data []byte) {
	t.Helper()
	path := filepath.Join(dir, databaseName)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

// replaceFile puts a new file in place of the stopped server's database (a new
// inode, as a restore from a file backup does).
func replaceFile(t *testing.T, dir string, data []byte) {
	t.Helper()
	path := filepath.Join(dir, databaseName)
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")
	if err := os.WriteFile(path+".new", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".new", path); err != nil {
		t.Fatal(err)
	}
}

// serverWithData opens a data directory with three accounts that have commits
// at epoch 1 and one device token.
func serverWithData(t *testing.T, dir string) (s *Store, ids []string, install string, token []byte) {
	t.Helper()
	s = openTest(t, config{dataDir: dir})
	ids = createAccounts(t, s, 3)
	for _, id := range ids {
		for i := 0; i < 2; i++ {
			if _, err := appendWithCallerRow(context.Background(), s, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	install, token = newToken(t, s, ids[0])
	return s, ids, install, token
}

// detected reports whether the open moved every account to a new epoch.
func detected(t *testing.T, before map[string]int64, s *Store) bool {
	t.Helper()
	after := epochsOf(t, s)
	moved := 0
	for id, e := range before {
		if after[id] != e {
			moved++
		}
	}
	if moved != 0 && moved != len(before) {
		t.Fatalf("only %d of %d accounts moved epoch", moved, len(before))
	}
	return moved > 0
}

// ---- PROTOCOL §14 detection matrix and PROTOCOL §14

func TestStartupDetectionMatrix(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name, signal string // signal: the reason startup must give, alone
		// prepare runs with the server open, then stops it and changes files;
		// it returns whether the next start must detect a restore.
		prepare func(t *testing.T, dir string, s *Store) bool
	}{
		{"a normal restart: no change", "", func(t *testing.T, dir string, s *Store) bool {
			s.Close()
			return false
		}},
		{"a backup copied into place: detected (marker)", "it is a backup copy", func(t *testing.T, dir string, s *Store) bool {
			info, err := s.Backup(ctx, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
			data, _ := os.ReadFile(info.Path)
			writeInPlace(t, dir, data) // same incarnation, same inode: only the marker tells
			return true
		}},
		{"an older raw copy replacing the file: detected (file identity)", "the database file was replaced", func(t *testing.T, dir string, s *Store) bool {
			data := rawCopy(t, s)
			appendWithCallerRow(ctx, s, firstAccount(t, s))
			s.Close()
			replaceFile(t, dir, data) // same incarnation, no marker: only the inode tells
			return true
		}},
		{"an earlier incarnation's copy written over the file in place: detected (token)", "its incarnation is not the current one", func(t *testing.T, dir string, s *Store) bool {
			data := rawCopy(t, s)
			s.Close()
			openTest(t, config{dataDir: dir}).Close() // a later incarnation
			writeInPlace(t, dir, data)
			return true
		}},
		{"metadata deleted: detected (flag)", "restore-state.json is missing although it existed", func(t *testing.T, dir string, s *Store) bool {
			s.Close()
			os.Remove(filepath.Join(dir, stateName))
			return true
		}},
		{"the same incarnation rolled back in place after a revocation: detected (auth revision)", "its auth revision is older than the server's", func(t *testing.T, dir string, s *Store) bool {
			data := rawCopy(t, s)
			account, install := tokenOf(t, s)
			if err := revoke(s, account, install); err != nil {
				t.Fatal(err)
			}
			s.Close()
			writeInPlace(t, dir, data) // same incarnation, same inode, no marker
			return true
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, _, _, token := serverWithData(t, dir)
			before := epochsOf(t, s)
			metaBefore := readState(t, dir)
			want := tc.prepare(t, dir, s)
			s2 := openTest(t, config{dataDir: dir})
			if got := detected(t, before, s2); got != want {
				t.Fatalf("detected %v, want %v", got, want)
			}
			if s2.restoredBecause != tc.signal {
				t.Errorf("reason %q, want exactly %q", s2.restoredBecause, tc.signal)
			}
			st := readState(t, dir)
			if want {
				for _, e := range epochsOf(t, s2) {
					if e <= metaBefore.EpochHighWater || e != st.EpochHighWater {
						t.Errorf("epoch %d, high-water mark before %d, now %d", e, metaBefore.EpochHighWater, st.EpochHighWater)
					}
				}
				if tokenWorks(t, s2, token) {
					t.Error("a token survived the transition")
				}
			} else if !tokenWorks(t, s2, token) {
				t.Error("a normal restart lost a token")
			}
			assertSettled(t, s2)
			// The start after that is normal.
			after := epochsOf(t, s2)
			s2.Close()
			if detected(t, after, openTest(t, config{dataDir: dir})) {
				t.Error("the next start detected again")
			}
		})
	}
}

func firstAccount(t *testing.T, s *Store) string {
	t.Helper()
	var ids []string
	if err := s.Read(context.Background(), func(r *ReadTx) error {
		var err error
		ids, err = r.AccountIDs()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return ids[0]
}

// tokenOf returns the account and installation of the one token serverWithData
// made.
func tokenOf(t *testing.T, s *Store) (account, install string) {
	t.Helper()
	if err := s.reader.QueryRow(`SELECT account_id, install_id FROM auth_tokens`).Scan(&account, &install); err != nil {
		t.Fatal(err)
	}
	return account, install
}

// assertSettled checks the metadata and the database agree after a start:
// one current incarnation, no pending phase, the same auth revision.
func assertSettled(t *testing.T, s *Store) {
	t.Helper()
	st := readState(t, s.dir)
	f, err := readDBFacts(context.Background(), s.reader)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := identityOf(filepath.Join(s.dir, databaseName))
	switch {
	case st.Incarnation != f.incarnation || st.NextIncarnation != "":
		t.Errorf("incarnation: metadata %q next %q, database %q", st.Incarnation, st.NextIncarnation, f.incarnation)
	case st.PendingAuthRevision != 0 || st.AuthRevision != f.authRevision:
		t.Errorf("auth revision: metadata %d pending %d, database %d", st.AuthRevision, st.PendingAuthRevision, f.authRevision)
	case f.marker || !f.flag:
		t.Errorf("marker %v, flag %v", f.marker, f.flag)
	case st.DBFile == nil || *st.DBFile != id:
		t.Errorf("file identity %v, want %v", st.DBFile, id)
	case st.EpochHighWater < f.maxEpoch:
		t.Errorf("high-water mark %d below epoch %d", st.EpochHighWater, f.maxEpoch)
	}
}

func TestAnotherServersDatabaseIsRefused(t *testing.T) {
	ctx := context.Background()
	dir, other := t.TempDir(), t.TempDir()
	openTest(t, config{dataDir: dir}).Close()
	o := openTest(t, config{dataDir: other})
	data := rawCopy(t, o)
	o.Close()
	writeInPlace(t, dir, data)
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync}); !errors.Is(err, ErrOtherServer) {
		t.Fatalf("another server's database with this server's keys: %v", err)
	}
	// Without receipt.keys, the metadata refuses it.
	os.Remove(filepath.Join(dir, receiptKeysName))
	if _, err := open(ctx, config{dataDir: dir, syncDir: noSync}); !errors.Is(err, ErrOtherServer) {
		t.Fatalf("another server's database with this server's metadata: %v", err)
	}
}

// PROTOCOL §14: a crash inside the two-phase incarnation write causes no false
// detection.
func TestCrashInsideTheIncarnationWrite(t *testing.T) {
	ctx := context.Background()
	crash := errors.New("crash")
	for _, point := range []struct{ when, phase string }{
		{"before", "incarnation-next"}, {"after", "incarnation-next"},
		{"before", "incarnation-final"}, {"after", "incarnation-final"},
	} {
		t.Run(point.when+" "+point.phase, func(t *testing.T) {
			dir := t.TempDir()
			s, _, _, token := serverWithData(t, dir)
			before := epochsOf(t, s)
			s.Close()
			h := hooks{}
			hook := func(phase string) error {
				if phase == point.phase {
					return crash
				}
				return nil
			}
			if point.when == "before" {
				h.beforeStateWrite = hook
			} else {
				h.afterStateWrite = hook
			}
			if _, err := open(ctx, config{dataDir: dir, syncDir: noSync, hooks: h}); !errors.Is(err, crash) {
				t.Fatalf("open: %v", err)
			}
			s2 := openTest(t, config{dataDir: dir})
			if detected(t, before, s2) || !tokenWorks(t, s2, token) {
				t.Fatal("a crash inside the incarnation write caused a detection")
			}
			assertSettled(t, s2)
		})
	}
}

// PROTOCOL §14: a crash at each phase of the auth revision write. A crash
// before the pending revision is written, or after the commit, causes no
// detection; a pending revision the database has not reached is a fence and
// runs the transition, which keeps the revoked token dead in every case.
func TestCrashAtEachPhaseOfTheAuthRevision(t *testing.T) {
	ctx := context.Background()
	crash := errors.New("crash")
	cases := []struct {
		name      string
		h         hooks
		committed bool // the revocation reached the database
		transit   bool // the next start runs the transition
	}{
		{"before the pending revision", hooks{beforeStateWrite: only("auth-pending", crash)}, false, false},
		{"after the pending revision, before the commit", hooks{afterStateWrite: only("auth-pending", crash)}, false, true},
		{"after the commit, before the final revision", hooks{beforeStateWrite: only("auth-final", crash)}, true, false},
		{"after the final revision", hooks{afterStateWrite: only("auth-final", crash)}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, ids, install, token := serverWithData(t, dir)
			_, other := newToken(t, s, ids[1])
			before := epochsOf(t, s)
			s.Close()
			s1, err := open(ctx, config{dataDir: dir, syncDir: noSync})
			if err != nil {
				t.Fatal(err)
			}
			s1.hooks = tc.h // installed after startup: only the revocation's writes
			if err := revoke(s1, ids[0], install); (err == nil) != tc.committed {
				t.Fatalf("revoke: %v", err)
			}
			s1.Close()
			s2 := openTest(t, config{dataDir: dir})
			if got := detected(t, before, s2); got != tc.transit {
				t.Fatalf("transition %v, want %v", got, tc.transit)
			}
			if tc.committed && tokenWorks(t, s2, token) {
				t.Error("the revoked token works again")
			}
			if !tc.transit && !tc.committed && !tokenWorks(t, s2, token) {
				t.Error("a revocation that never committed removed the token")
			}
			if !tc.transit && !tokenWorks(t, s2, other) {
				t.Error("another account's token was lost")
			}
			assertSettled(t, s2)
		})
	}
}

// only returns a hook that fails at one phase.
func only(phase string, err error) func(string) error {
	return func(p string) error {
		if p == phase {
			return err
		}
		return nil
	}
}

// PROTOCOL §14: the revocation committed, the metadata was not
// finalized, and then the database was rolled back in place to a copy from
// before it. The pending revision fences it: detected, the token stays dead.
func TestRollbackAfterAnUnfinalizedRevocationIsDetected(t *testing.T) {
	dir := t.TempDir()
	s, ids, install, token := serverWithData(t, dir)
	before := epochsOf(t, s)
	data := rawCopy(t, s)
	s.hooks.beforeStateWrite = only("auth-final", errors.New("crash"))
	if err := revoke(s, ids[0], install); err != nil {
		t.Fatal(err) // committed; the final write is lost
	}
	if st := readState(t, dir); st.PendingAuthRevision == 0 {
		t.Fatal("no pending revision after the simulated crash")
	}
	s.Close()
	writeInPlace(t, dir, data)
	s2 := openTest(t, config{dataDir: dir})
	if !detected(t, before, s2) || tokenWorks(t, s2, token) {
		t.Fatal("the rollback across the revocation was not detected")
	}
}

// PROTOCOL §14: the same rollback without any access-removing change is not detected
// at startup and resurrects nothing: tokens issued after the copy are unknown,
// earlier ones were never revoked.
func TestRollbackWithoutAccessRemovalIsNotDetected(t *testing.T) {
	dir := t.TempDir()
	s, ids, _, token := serverWithData(t, dir)
	before := epochsOf(t, s)
	data := rawCopy(t, s)
	_, later := newToken(t, s, ids[1])
	s.Close()
	writeInPlace(t, dir, data)
	s2 := openTest(t, config{dataDir: dir})
	if detected(t, before, s2) {
		t.Fatal("detected")
	}
	if !tokenWorks(t, s2, token) || tokenWorks(t, s2, later) {
		t.Fatal("the copy's tokens are not exactly those that existed when it was taken")
	}
}

// A transaction that fails after the pending revision was written (certainly
// not committed) withdraws it, so the next start sees nothing to fence.
func TestARolledBackRevocationLeavesNoFence(t *testing.T) {
	dir := t.TempDir()
	s, ids, install, token := serverWithData(t, dir)
	before := epochsOf(t, s)
	failed := errors.New("rolled back")
	s.hooks.beforeCommit = func() error { return failed }
	if err := revoke(s, ids[0], install); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	s.hooks.beforeCommit = nil
	if st := readState(t, dir); st.PendingAuthRevision != 0 {
		t.Fatalf("pending revision %d left", st.PendingAuthRevision)
	}
	s.Close()
	s2 := openTest(t, config{dataDir: dir})
	if detected(t, before, s2) || !tokenWorks(t, s2, token) {
		t.Fatal("a rolled-back revocation had an effect")
	}
}

// Every access-removing change raises the auth revision, mirrored in the
// metadata; expired-session cleanup and sign-in do not.
func TestAccessRemovingChangesRaiseTheAuthRevision(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 2)
	for _, id := range ids {
		if err := s.Write(ctx, func(w *WriteTx) error {
			return w.CreateLogin(id, "user-"+id[:8], "user-"+id[:8], "hash", true)
		}); err != nil {
			t.Fatal(err)
		}
	}
	install, token := newToken(t, s, ids[0])
	succ, _ := newToken(t, s, ids[0])
	revision := func() int64 {
		f, err := readDBFacts(ctx, s.reader)
		if err != nil {
			t.Fatal(err)
		}
		if st := readState(t, s.dir); st.AuthRevision != f.authRevision || st.PendingAuthRevision != 0 {
			t.Fatalf("metadata %d (pending %d), database %d", st.AuthRevision, st.PendingAuthRevision, f.authRevision)
		}
		return f.authRevision
	}
	steps := []struct {
		name  string
		raise bool
		fn    func(w *WriteTx) error
	}{
		{"expired-session cleanup", false, func(w *WriteTx) error { return w.DeleteExpiredSessions(w.nowMillis()) }},
		{"a new credential", false, func(w *WriteTx) error {
			return w.Account(ids[0]).InsertCredential(bytes.Repeat([]byte{1}, 32), "web", "", nil)
		}},
		{"granting administrator", false, func(w *WriteTx) error { return w.Account(ids[1]).SetAdmin(true) }},
		{"enabling", false, func(w *WriteTx) error { return w.Account(ids[1]).SetState("active") }},
		{"signing out", true, func(w *WriteTx) error { return w.Account(ids[0]).DeleteCredential(bytes.Repeat([]byte{1}, 32)) }},
		{"revoking a superseded installation's credentials", true, func(w *WriteTx) error {
			a := w.Account(ids[0])
			if err := a.Supersede(install, succ); err != nil {
				return err
			}
			return a.FinishSupersession(succ)
		}},
		{"revoking an installation", true, func(w *WriteTx) error { return w.Account(ids[0]).RevokeInstallation(succ) }},
		{"a password change", true, func(w *WriteTx) error { return w.Account(ids[0]).SetPassword("new", nil) }},
		{"disabling", true, func(w *WriteTx) error { return w.Account(ids[1]).SetState("disabled") }},
		{"removing administrator", true, func(w *WriteTx) error { return w.Account(ids[1]).SetAdmin(false) }},
		{"deleting an account", true, func(w *WriteTx) error { return w.DeleteAccount(ids[1]) }},
	}
	for _, step := range steps {
		was := revision()
		if err := s.Write(ctx, step.fn); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		if now := revision(); (now == was+1) != step.raise || now < was {
			t.Errorf("%s: revision %d → %d, raise %v", step.name, was, now, step.raise)
		}
	}
	_ = token
}

// PROTOCOL §14: the transition run inside a write transaction.
func TestRestoreTransition(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, ids, install, token := serverWithData(t, dir)
	if err := s.Write(ctx, func(w *WriteTx) error {
		return w.Account(ids[0]).StorePart(install, 1, PartRow{Index: 0, TransactionID: newUUID(), Count: 2, ContentHash: "h", Data: []byte("x")})
	}); err != nil {
		t.Fatal(err)
	}
	views := filepath.Join(dir, viewsDirName)
	os.MkdirAll(views, 0o700)
	for _, name := range []string{"v.json", "v.jsonl"} {
		os.WriteFile(filepath.Join(views, name), []byte("{}"), 0o600)
	}
	heads := map[string]Head{}
	s.Read(ctx, func(r *ReadTx) error {
		for _, id := range ids {
			heads[id], _ = r.Account(id).Head()
		}
		return nil
	})
	before := readState(t, dir)
	beforeFacts, _ := readDBFacts(ctx, s.reader)
	if err := s.Write(ctx, func(w *WriteTx) error { return w.RestoreTransition("test") }); err != nil {
		t.Fatal(err)
	}
	st := readState(t, dir)
	f, _ := readDBFacts(ctx, s.reader)
	if st.EpochHighWater <= max(before.EpochHighWater, beforeFacts.maxEpoch) {
		t.Fatalf("high-water mark %d, before %d", st.EpochHighWater, before.EpochHighWater)
	}
	if f.authRevision <= max(beforeFacts.authRevision, before.AuthRevision) || st.AuthRevision != f.authRevision || st.PendingAuthRevision != 0 {
		t.Fatalf("auth revision %d / %d", f.authRevision, st.AuthRevision)
	}
	if st.Incarnation == before.Incarnation || st.Incarnation != f.incarnation || st.NextIncarnation != "" {
		t.Fatal("no new incarnation")
	}
	s.Read(ctx, func(r *ReadTx) error {
		for _, id := range ids {
			h, _ := r.Account(id).Head()
			old := heads[id]
			if h.Epoch != st.EpochHighWater || h.Seq != old.Seq || !bytes.Equal(h.Digest, old.Digest) || h.LogID != old.LogID {
				t.Errorf("account %s head %+v, was %+v", id, h, old)
			}
		}
		return nil
	})
	if tokenWorks(t, s, token) || countRows(t, rawDB(t, dir), `SELECT COUNT(*) FROM push_parts`) != 0 {
		t.Fatal("tokens or push parts of the old timeline survived")
	}
	if entries, _ := os.ReadDir(views); len(entries) != 0 {
		t.Fatalf("views left: %v", entries)
	}
	// The first commit of the new epoch chains from the restored head.
	if _, err := appendWithCallerRow(ctx, s, ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(ctx, ids[0]); err != nil {
		t.Fatal(err)
	}
	var epoch int64
	s.reader.QueryRow(`SELECT epoch FROM log_commits WHERE account_id = ? ORDER BY seq DESC LIMIT 1`, ids[0]).Scan(&epoch)
	if epoch != st.EpochHighWater {
		t.Fatalf("new commit at epoch %d, want %d", epoch, st.EpochHighWater)
	}
	assertSettled(t, s)
	// A transition whose transaction rolls back leaves no fence.
	failed := errors.New("failed")
	before = readState(t, dir)
	if err := s.Write(ctx, func(w *WriteTx) error {
		if err := w.RestoreTransition("test"); err != nil {
			return err
		}
		return failed
	}); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	if st := readState(t, dir); st.PendingAuthRevision != 0 || st.NextIncarnation != "" || st.Incarnation != before.Incarnation ||
		st.EpochHighWater < before.EpochHighWater {
		t.Fatalf("a rolled-back transition left %+v", st)
	}
	s.Close()
	again := epochsOf(t, openTest(t, config{dataDir: dir}))
	for _, e := range again {
		if e != before.EpochHighWater {
			t.Fatalf("epoch %d after a rolled-back transition, want %d", e, before.EpochHighWater)
		}
	}
}
