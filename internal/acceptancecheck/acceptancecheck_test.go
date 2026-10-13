package acceptancecheck

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// TestWitness is the witness, on the data directory NTP_WITNESS_DATA names:
// `make acceptance-witness DATA=<dir>` runs it, and nothing else does.
func TestWitness(t *testing.T) {
	dir := os.Getenv("NTP_WITNESS_DATA")
	if dir == "" {
		t.Skip("the witness runs from `make acceptance-witness DATA=<dir>`")
	}
	Check(t, dir)
}

// ---- a fixture data directory: two accounts that subscribed (one podcast
// a private feed, with its login sealed), listened, favorited and marked
// played, as a stopped server leaves them

const minute = int64(60_000)

type fixture struct {
	t     *testing.T
	dir   string
	svc   *protocol.Service
	clock int64
	ids   int
	alice *member
	bob   *member
}

type member struct {
	id, namespace, username string
	inst                    storage.Installation
	seq, author             int64
	last                    string
	podcast, generation     string
	listened, marked        string // episode IDs
}

// commits is how many commits library makes in each account.
const commits = 6

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f, store := openFixture(t, filepath.Join(t.TempDir(), "data"))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return f
}

// openFixture builds the fixture in dir and leaves its server running (its
// store open, holding the lock).
func openFixture(t *testing.T, dir string) (*fixture, *storage.Store) {
	t.Helper()
	ctx := context.Background()
	f := &fixture{t: t, dir: dir, clock: 1_790_000_000_000}
	store, err := storage.Open(ctx, f.dir)
	if err != nil {
		t.Fatal(err)
	}
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewRegistry()
	ops.Register(reg)
	f.svc = &protocol.Service{Store: store, Registry: reg, Box: box, Now: func() time.Time { return time.UnixMilli(f.clock) }}
	f.alice, f.bob = f.member("alice"), f.member("bob")
	for _, m := range []*member{f.alice, f.bob} {
		f.library(m)
	}
	t.Cleanup(func() { store.Close() })
	return f, store
}

func (f *fixture) newID() string {
	f.ids++
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", f.ids)
}

func (f *fixture) member(username string) *member {
	m := &member{username: username}
	m.inst = storage.Installation{InstallID: f.newID(), InstanceNonce: f.newID(), Name: username + "'s phone", Platform: "ios", AppVersion: "1",
		Capabilities: `["favorites","history","inbox","queue","statistics"]`}
	err := f.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		if err != nil {
			return err
		}
		m.id, m.namespace = a.ID, a.Namespace
		if err := tx.CreateLogin(a.ID, username, username, "not a hash", false); err != nil {
			return err
		}
		return tx.Account(a.ID).InsertInstallation(m.inst)
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return m
}

// library subscribes to a private feed with its login, subscribes,
// listens to one episode for a minute (history and statistics), favorites it,
// and marks another played: one push each.
func (f *fixture) library(m *member) {
	login := protocol.Secret{ID: f.newID(), Username: m.username, Password: "a private feed's password"}
	f.pushWith(m, "podcast.subscribe", []any{}, map[string]any{"feed_url": "https://private.example.com/" + m.username + ".xml",
		"credentials_secret": login.ID}, login)
	sub := f.push(m, "podcast.subscribe", []any{}, map[string]any{"feed_url": "https://example.com/" + m.username + ".xml"})
	m.podcast = contract.PodcastID(m.namespace, sub)
	m.generation = contract.GenerationID(m.namespace, m.podcast, sub)
	one, two := f.episode(m, "one"), f.episode(m, "two")
	m.listened, m.marked = one["episode_id"].(string), two["episode_id"].(string)
	session := f.newID()
	f.clock += minute
	f.push(m, "session.start", target(one), map[string]any{"episode": one, "session_id": session, "start_position": int64(0), "activity": true})
	f.clock += minute
	f.push(m, "session.end", target(one), map[string]any{"episode": one, "session_id": session, "ordinal": int64(1), "position": minute,
		"contribution": minute, "activity": true, "reason": "ended", "completed": false, "threshold_ms": int64(30_000),
		"remove_from_queue": false, "history": true})
	f.push(m, "favorite.set", target(one), map[string]any{"episode": one, "on": true})
	f.push(m, "episode.mark_played", target(two), map[string]any{"episode": two, "reset_position": true})
}

func (f *fixture) episode(m *member, guid string) map[string]any {
	id, err := contract.EpisodeID(m.namespace, m.podcast, contract.KindRSSGUID, guid)
	if err != nil {
		f.t.Fatal(err)
	}
	return map[string]any{"episode_id": id, "podcast_id": m.podcast, "generation_id": m.generation,
		"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": guid, "guid": guid, "title": "Episode " + guid}}
}

func target(ref map[string]any) []any {
	return []any{map[string]any{"entity": "episode", "id": ref["episode_id"], "generation": ref["generation_id"]}}
}

// push sends one fact from m's phone, depending on its previous one, and
// expects it applied; it returns the fact's op_id.
func (f *fixture) push(m *member, typ string, targets []any, payload map[string]any) string {
	f.t.Helper()
	return f.pushWith(m, typ, targets, payload)
}

// pushWith is push with secrets beside the fact.
func (f *fixture) pushWith(m *member, typ string, targets []any, payload map[string]any, secrets ...protocol.Secret) string {
	f.t.Helper()
	m.author++
	op := f.newID()
	deps := []any{}
	fact := map[string]any{"op_id": op, "author_id": m.inst.InstallID, "author_seq": m.author, "type": typ, "type_version": int64(1),
		"client_time": f.clock, "targets": targets}
	if m.last != "" {
		fact["previous_op_id"] = m.last
		deps = append(deps, m.last)
	}
	fact["dependencies"] = deps
	b, err := contract.Canonical(payload)
	if err != nil {
		f.t.Fatal(err)
	}
	fact["payload"], _ = contract.DecodeJSON(b) // integers as the server decodes them
	m.last = op
	parsed, err := protocol.ParseFact(fact)
	if err != nil {
		f.t.Fatal(err)
	}
	ctx := context.Background()
	var cursor contract.Cursor
	if err := f.svc.Store.Read(ctx, func(r *storage.ReadTx) error {
		h, err := r.Account(m.id).Head()
		cursor = contract.Cursor{LogID: h.LogID, Epoch: h.Epoch, Seq: h.Seq, Digest: h.Digest}
		return err
	}); err != nil {
		f.t.Fatal(err)
	}
	m.seq++
	inst := m.inst
	resp, err := f.svc.Push(ctx, protocol.PushRequest{AccountID: m.id, Cursor: cursor, InstallID: inst.InstallID, AckedInstallSeq: m.seq - 1,
		Operations: []protocol.PushOp{{InstallSeq: m.seq, Fact: parsed}}, Secrets: secrets}, func(tx *storage.WriteTx) (*storage.Installation, error) {
		i, err := tx.Account(m.id).Installation(inst.InstallID)
		return &i, err
	})
	if err != nil {
		f.t.Fatal(err)
	}
	if o, _ := resp.Results[0].Receipt["outcome"].(string); o != "applied" {
		f.t.Fatalf("%s: %+v", typ, resp.Results[0])
	}
	return op
}

// exec changes the stopped fixture's database directly, as corruption or a
// bug would, without a commit.
func (f *fixture) exec(query string, args ...any) {
	f.t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(f.dir, "nottennapod.db"))
	if err != nil {
		f.t.Fatal(err)
	}
	defer db.Close()
	res, err := db.Exec(query, args...)
	if err != nil {
		f.t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		f.t.Fatalf("%s changed %d rows, want 1", query, n)
	}
}

// fingerprint is the data directory's files, their modes, times and contents.
func fingerprint(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		fmt.Fprintf(&b, "%s %v %v %x\n", rel, fi.Mode(), fi.ModTime().UnixNano(), sha256.Sum256(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func problems(res Result) string {
	var out []string
	for _, p := range res.Problems {
		out = append(out, p.String())
	}
	return strings.Join(out, "\n")
}

// ---- the tests

// A data directory a stopped server left passes, every account is checked,
// and the directory itself is not changed by the copy and the rebuilds.
func TestTheWitnessPassesAStoppedServersData(t *testing.T) {
	f := newFixture(t)
	before := fingerprint(t, f.dir)
	res := Verify(t, f.dir)
	if len(res.Problems) > 0 {
		t.Fatalf("problems in a sound data directory:\n%s", problems(res))
	}
	if len(res.Accounts) != 2 {
		t.Fatalf("accounts checked: %+v", res.Accounts)
	}
	for _, a := range res.Accounts {
		if a.Commits != commits || (a.Username != "alice" && a.Username != "bob") || len(a.Log) == 0 || !strings.Contains(a.Log[0], "rebuild invariant: ok") {
			t.Errorf("account %+v", a)
		}
	}
	if after := fingerprint(t, f.dir); after != before {
		t.Errorf("the witness changed the data directory:\nbefore\n%s\nafter\n%s", before, after)
	}
	Check(t, f.dir) // and passes as the make target runs it
}

// A tampered digest in the middle of one account's log is reported for that
// account, at its sequence number, by the archive's check and the chain's;
// the other account still passes.
func TestTheWitnessFindsATamperedDigest(t *testing.T) {
	f := newFixture(t)
	f.exec(`UPDATE log_commits SET digest = ? WHERE account_id = ? AND seq = 3`, make([]byte, 32), f.alice.id)
	res := Verify(t, f.dir)
	t.Logf("reported:\n%s", problems(res))
	if len(res.Problems) != 2 {
		t.Fatalf("problems:\n%s", problems(res))
	}
	for _, p := range res.Problems {
		if p.Account != f.alice.id || p.Username != "alice" {
			t.Errorf("problem %s, want alice's", p)
		}
	}
	if p := res.Problems[0]; p.Check != CheckArchive || !strings.Contains(p.Detail, "broken at seq 3") {
		t.Errorf("problem %s", p)
	}
	p := res.Problems[1]
	if p.Check != CheckChain || !strings.Contains(p.Detail, "digest at seq 3") {
		t.Errorf("problem %s", p)
	}
	if !strings.HasPrefix(p.String(), "account "+f.alice.id+" (alice): chain: ") {
		t.Errorf("the report %q does not name the account first", p)
	}
}

// A published record that drifted from what the archive derives is reported
// for its account, with the record the rebuild publishes instead.
func TestTheWitnessFindsADriftedPublishedRecord(t *testing.T) {
	f := newFixture(t)
	f.exec(`UPDATE records SET value = ? WHERE account_id = ? AND entity = 'episode' AND entity_id = ? AND field_group = 'play_state'`,
		`{"generation_id":"`+f.bob.generation+`","play_state":"unplayed"}`, f.bob.id, f.bob.marked)
	res := Verify(t, f.dir)
	if len(res.Problems) != 1 {
		t.Fatalf("problems:\n%s", problems(res))
	}
	p := res.Problems[0]
	t.Logf("reported: %s", p)
	if p.Account != f.bob.id || p.Check != CheckRebuild || !strings.Contains(p.Detail, "the rebuild changed published records") ||
		!strings.Contains(p.Detail, f.bob.marked) || !strings.Contains(p.Detail, `"play_state":"played"`) {
		t.Errorf("problem %s", p)
	}
}

// Derived state that drifted while the published records still agree (a
// contribution changed in place) is reported as the rows that differ.
func TestTheWitnessFindsDriftedDerivedState(t *testing.T) {
	f := newFixture(t)
	f.exec(`UPDATE contributions SET effect = effect || ' ' WHERE account_id = ?1 AND rowid = (SELECT MIN(rowid) FROM contributions WHERE account_id = ?1)`,
		f.alice.id)
	res := Verify(t, f.dir)
	if len(res.Problems) != 1 {
		t.Fatalf("problems:\n%s", problems(res))
	}
	p := res.Problems[0]
	t.Logf("reported: %s", p)
	if p.Account != f.alice.id || p.Check != CheckRebuild || !strings.Contains(p.Detail, "derived state differs after the rebuild") ||
		!strings.Contains(p.Detail, "contributions: ") {
		t.Errorf("problem %s", p)
	}
}

// A fact changed in the archive (its content no longer the one its receipt
// signs) is reported for its account, although the log and the rebuild
// still agree: recovery would refuse that evidence.
func TestTheWitnessFindsATamperedArchivedFact(t *testing.T) {
	f := newFixture(t)
	f.exec(`UPDATE facts SET fact = CAST(json_set(fact, '$.client_time', json_extract(fact, '$.client_time') + 1) AS BLOB)
		WHERE account_id = ?1 AND rowid = (SELECT MIN(rowid) FROM facts WHERE account_id = ?1)`, f.alice.id)
	res := Verify(t, f.dir)
	if len(res.Problems) != 1 {
		t.Fatalf("problems:\n%s", problems(res))
	}
	p := res.Problems[0]
	t.Logf("reported: %s", p)
	if p.Account != f.alice.id || p.Check != CheckArchive || !strings.Contains(p.Detail, "does not match its signed receipt") {
		t.Errorf("problem %s", p)
	}
}

// A receipt key replaced by another valid key, under which the archived
// receipts no longer verify, is reported.
func TestTheWitnessFindsAReplacedReceiptKey(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(f.dir, "receipt.keys")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var file map[string]any
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	file["keys"].([]any)[0].(map[string]any)["key"] = base64.StdEncoding.EncodeToString(make([]byte, 32))
	if data, err = json.Marshal(file); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	res := Verify(t, f.dir)
	t.Logf("reported:\n%s", problems(res))
	if len(res.Problems) != 1 || res.Problems[0].Check != CheckArchive || !strings.Contains(res.Problems[0].Detail, "does not verify") {
		t.Errorf("problems:\n%s", problems(res))
	}
}

// Private feeds' logins sealed under a secrets key that is missing or
// another key cannot be read: reported, not replaced by a new key.
func TestTheWitnessFindsUnreadableSecrets(t *testing.T) {
	for _, mode := range []string{"missing", "another"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			path := filepath.Join(f.dir, "secrets.key")
			if mode == "missing" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, make([]byte, 32), 0o600); err != nil {
				t.Fatal(err)
			}
			res := Verify(t, f.dir)
			if len(res.Problems) != 1 {
				t.Fatalf("problems:\n%s", problems(res))
			}
			p := res.Problems[0]
			t.Logf("reported: %s", p)
			if p.Check != CheckSecrets || (p.Account != f.alice.id && p.Account != f.bob.id) {
				t.Errorf("problem %s", p)
			}
			if _, err := os.Stat(path); mode == "missing" && !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the witness made a secrets key in the data directory: %v", err)
			}
		})
	}
}

// A database that is not a server's (here an empty file) is refused, not
// set up as a new, empty server that passes.
func TestTheWitnessRefusesAnUninitializedDatabase(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nottennapod.db", "server.lock"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := fingerprint(t, dir)
	res := Verify(t, dir)
	t.Logf("reported:\n%s", problems(res))
	if len(res.Problems) != 1 || res.Problems[0].Check != CheckOpen || !strings.Contains(res.Problems[0].Detail, dir+": its database is not a NotTennaPod server's") {
		t.Errorf("problems:\n%s", problems(res))
	}
	if after := fingerprint(t, dir); after != before {
		t.Errorf("the witness changed the directory:\nbefore\n%s\nafter\n%s", before, after)
	}
}

// The witness never writes the data directory, its lock included: a missing
// lock is refused, not made, and the lock's mode is left as it is.
func TestTheWitnessNeverWritesTheLock(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		f := newFixture(t)
		path := filepath.Join(f.dir, "server.lock")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		res := Verify(t, f.dir)
		t.Logf("reported:\n%s", problems(res))
		if len(res.Problems) != 1 || res.Problems[0].Check != CheckOpen || !strings.Contains(res.Problems[0].Detail, "server.lock") {
			t.Errorf("problems:\n%s", problems(res))
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the witness made the lock: %v", err)
		}
	})
	t.Run("readable by others", func(t *testing.T) {
		f := newFixture(t)
		path := filepath.Join(f.dir, "server.lock")
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		before := fingerprint(t, f.dir)
		if res := Verify(t, f.dir); len(res.Problems) > 0 {
			t.Fatalf("problems:\n%s", problems(res))
		}
		if after := fingerprint(t, f.dir); after != before {
			t.Errorf("the witness changed the data directory:\nbefore\n%s\nafter\n%s", before, after)
		}
	})
}

// TestKilledServerChild is the server TestTheWitnessReadsWhatAKilledServerLeft
// kills: it builds the fixture in NTP_WITNESS_CHILD_DATA, says so, and keeps
// its store open until it is killed.
func TestKilledServerChild(t *testing.T) {
	dir := os.Getenv("NTP_WITNESS_CHILD_DATA")
	if dir == "" {
		t.Skip("run by TestTheWitnessReadsWhatAKilledServerLeft")
	}
	openFixture(t, dir)
	fmt.Println("ready")
	var b [1]byte
	_, _ = os.Stdin.Read(b[:])
	t.Fatal("the server was not killed")
}

// What a server killed with SIGKILL leaves (committed transactions still in
// its write-ahead log, its lock released by the kernel) is what the witness
// checks: every commit is there, and the directory is left unchanged.
func TestTheWitnessReadsWhatAKilledServerLeft(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "data")
	cmd := exec.Command(exe, "-test.run=^TestKilledServerChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), "NTP_WITNESS_CHILD_DATA="+dir)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	lines := bufio.NewScanner(out)
	for lines.Scan() && lines.Text() != "ready" {
	}
	if lines.Text() != "ready" {
		t.Fatal("the server never became ready")
	}
	if fi, err := os.Stat(filepath.Join(dir, "nottennapod.db-wal")); err != nil || fi.Size() == 0 {
		t.Fatalf("nothing in the write-ahead log, so the test would not test it: %v", err)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("the server exited normally")
	}
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("the server did not die of SIGKILL: %v", cmd.ProcessState)
	}
	before := fingerprint(t, dir)
	res := Verify(t, dir)
	if len(res.Problems) > 0 || len(res.Accounts) != 2 || res.Accounts[0].Commits != commits || res.Accounts[1].Commits != commits {
		t.Fatalf("accounts %+v, problems:\n%s", res.Accounts, problems(res))
	}
	if after := fingerprint(t, dir); after != before {
		t.Errorf("the witness changed the data directory:\nbefore\n%s\nafter\n%s", before, after)
	}
}

// A data directory a server is using is refused, and nothing is copied.
func TestTheWitnessRefusesARunningServer(t *testing.T) {
	f, _ := openFixture(t, filepath.Join(t.TempDir(), "data"))
	res := Verify(t, f.dir)
	if len(res.Problems) != 1 || res.Problems[0].Check != CheckOpen || !strings.Contains(res.Problems[0].Detail, storage.ErrLocked.Error()) {
		t.Fatalf("problems:\n%s", problems(res))
	}
}
