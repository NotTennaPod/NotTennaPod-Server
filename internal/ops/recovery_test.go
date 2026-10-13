package ops

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// PROTOCOL §6.11 recovery (PROTOCOL §6.11): a real backup, the real restore procedure
// (PROTOCOL §11), devices holding what they pushed or pulled, and recovery uploads of
// what the restored server lacks.

// replicaCommit is one commit a device holds: its facts with their receipts
// and its decisions, in receipt order.
type replicaCommit struct {
	seq     int64
	entries []protocol.RecoveryEntry
	facts   []string // op IDs
}

// logCommits reads the account's commits after seq (what a device that
// pulled from there holds).
func (e *env) logCommits(t *testing.T, after int64) []replicaCommit {
	t.Helper()
	var out []replicaCommit
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		acct := r.Account(e.account)
		head, err := acct.Head()
		if err != nil {
			return err
		}
		commits, err := acct.Commits(after, head.Seq, int(head.Seq-after)+1)
		if err != nil {
			return err
		}
		for _, c := range commits {
			v, err := contract.DecodeJSON(c.Canonical)
			if err != nil {
				return err
			}
			cm := v.(map[string]any)
			type indexed struct {
				index int64
				id    string
				e     protocol.RecoveryEntry
				fact  bool
			}
			var list []indexed
			for _, x := range cm["accepted"].([]any) {
				a := x.(map[string]any)
				fact, receipt := a["fact"].(map[string]any), a["receipt"].(map[string]any)
				parsed, err := protocol.ParseFact(fact)
				if err != nil {
					return err
				}
				i, _ := integer(receipt["index"])
				list = append(list, indexed{i, parsed.OpID, protocol.RecoveryEntry{Fact: parsed, Receipt: receipt}, true})
			}
			for _, x := range cm["decisions"].([]any) {
				d := x.(map[string]any)
				i, _ := integer(d["index"])
				list = append(list, indexed{i, d["decision_id"].(string), protocol.RecoveryEntry{Decision: d}, false})
			}
			sort.Slice(list, func(a, b int) bool {
				if list[a].index != list[b].index {
					return list[a].index < list[b].index
				}
				return list[a].id < list[b].id
			})
			rc := replicaCommit{seq: c.Seq}
			for _, x := range list {
				rc.entries = append(rc.entries, x.e)
				if x.fact {
					rc.facts = append(rc.facts, x.id)
				}
			}
			out = append(out, rc)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// entries flattens commits into one upload (whole commits, in order).
func entries(commits ...[]replicaCommit) []protocol.RecoveryEntry {
	var out []protocol.RecoveryEntry
	for _, cs := range commits {
		for _, c := range cs {
			out = append(out, c.entries...)
		}
	}
	return out
}

// own is what a device holds of its own pushes it never pulled back: its
// facts with their receipts, without the decisions their commits made.
func own(commits []replicaCommit, d *device) []protocol.RecoveryEntry {
	var out []protocol.RecoveryEntry
	for _, c := range commits {
		for _, e := range c.entries {
			if e.Fact != nil && e.Fact.Fact != nil && (e.Fact.Fact.AuthorID == d.inst.InstallID || e.Fact.Fact.AuthorID == d.authorID) {
				out = append(out, e)
			}
		}
	}
	return out
}

// headSeq is the account's head seq.
func (e *env) headSeq(t *testing.T) int64 { return e.head().Seq }

// backup takes the administrator's backup now (PROTOCOL §11).
func (e *env) backup(t *testing.T) string {
	t.Helper()
	info, err := e.svc.Store.Backup(context.Background(), filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	return info.Path
}

// restore restores the backup with the real procedure (PROTOCOL §11): the server
// stops, the data directory takes the backup at a new epoch, and the server
// starts again. Installations created after the backup are gone; devices
// continue under successor installations (PROTOCOL §6).
func (e *env) restore(t *testing.T, backup string) {
	t.Helper()
	ctx := context.Background()
	dir := e.svc.Store.Dir()
	bundle := filepath.Join(t.TempDir(), "keys.json")
	pass := []byte("a long passphrase for the bundle")
	if err := e.svc.Store.ExportKeys(ctx, bundle, pass); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Restore(ctx, storage.RestoreOptions{DataDir: dir, Backup: backup, Bundle: data, Passphrase: pass}); err != nil {
		t.Fatal(err)
	}
	e.reopen(t)
}

// reopen opens the data directory again (a server restart).
func (e *env) reopen(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	dir := e.svc.Store.Dir()
	store, err := storage.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	e.t.Cleanup(func() { store.Close() })
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	views, err := protocol.NewViewRegistry(filepath.Join(store.Dir(), "views"), nil)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = &protocol.Service{Store: store, Registry: e.svc.Registry, Views: views, Box: box, Now: e.svc.Now, Random: e.svc.Random,
		StagingThreshold: e.svc.StagingThreshold}
}

// successor gives a device a fresh transport installation after a restore
// (PROTOCOL §6 step 1): it keeps writing as its author, continuing its chain.
func (e *env) successor(t *testing.T, d *device) *device {
	t.Helper()
	n := &device{inst: d.inst, author: d.author, authorID: d.authorID, last: d.last}
	if n.authorID == "" {
		n.authorID = d.inst.InstallID
	}
	n.inst.InstallID, n.inst.InstanceNonce = newID(), newID()
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(e.account).InsertInstallation(n.inst)
	}); err != nil {
		t.Fatal(err)
	}
	e.devices[d.inst.Name+"'"] = n
	return n
}

// recovery is one device's recovery session.
type recovery struct {
	e  *env
	d  *device
	id string
}

// startRecovery opens a recovery with a recovery snapshot (PROTOCOL §6.11); with a
// cursor the restored log still holds it answers the prefix shortcut.
func (e *env) startRecovery(t *testing.T, d *device, cursor *contract.Cursor) (*recovery, *protocol.SnapshotResponse) {
	t.Helper()
	rec := &recovery{e: e, d: d, id: newID()}
	resp, err := e.svc.Snapshot(context.Background(), protocol.SnapshotRequest{AccountID: e.account, Mode: "recovery", RecoveryID: rec.id,
		InstallID: d.inst.InstallID, Cursor: cursor})
	if err != nil {
		t.Fatal(err)
	}
	return rec, resp
}

func (r *recovery) authorize(tx *storage.WriteTx) (*storage.Installation, error) {
	inst, err := tx.Account(r.e.account).Installation(r.d.inst.InstallID)
	return &inst, err
}

// upload sends one upload request with every end-of-request step.
func (r *recovery) upload(t *testing.T, list []protocol.RecoveryEntry) *protocol.RecoveryUploadResponse {
	t.Helper()
	resp, err := r.e.svc.RecoveryUpload(context.Background(), protocol.RecoveryUploadRequest{AccountID: r.e.account, InstallID: r.d.inst.InstallID,
		RecoveryID: r.id, Entries: list, Steps: protocol.AllRecoveryEndSteps}, r.authorize)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func (r *recovery) finish(t *testing.T, complete bool) protocol.RecoverySummary {
	t.Helper()
	sum, err := r.e.svc.RecoveryFinish(context.Background(), r.e.account, r.d.inst.InstallID, r.id, complete, r.authorize)
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// statuses counts an upload's results by status.
func statuses(resp *protocol.RecoveryUploadResponse) map[string]int {
	out := map[string]int{}
	for _, r := range resp.Results {
		out[r.Status]++
	}
	return out
}

// sameRecords reports the records that differ (by key), for messages.
func sameRecords(t *testing.T, want, got map[string]string) {
	t.Helper()
	var diffs []string
	for k, v := range want {
		if got[k] != v {
			diffs = append(diffs, k+"\n    want "+v+"\n    got  "+got[k])
		}
	}
	for k, v := range got {
		if _, ok := want[k]; !ok {
			diffs = append(diffs, k+"\n    want (none)\n    got  "+v)
		}
	}
	slices.Sort(diffs)
	if len(diffs) > 0 {
		if len(diffs) > 12 {
			diffs = append(diffs[:12], "…")
		}
		t.Errorf("records differ from the run without a restore:\n  %s", strings.Join(diffs, "\n  "))
	}
}

// ---- scenario 20 (PROTOCOL §16): a week-old backup, three devices

// The server is restored from a week-old backup while three devices have
// newer changes, including one device's change that another device had only
// received: after recovery every device converges to the newest state and
// nothing is lost.
func TestScenario20WeekOldBackup(t *testing.T) {
	l := newListenEnv(t)
	a, b, c := l.device("A"), l.device("B"), l.device("C")
	two, three := l.episodeRef(l.p, l.g, "two"), l.episodeRef(l.p, l.g, "three")
	l.pushOK(t, a, l.lfact(a, "favorite.set", l.ref, map[string]any{"on": true}), "applied")
	backup := l.backup(t)
	atBackup := l.headSeq(t)

	// A week passes. A marks two played; B pulls it; A's phone is then lost.
	l.clock += 7 * 24 * 3600 * 1000
	l.pushOK(t, a, l.lfact(a, "episode.mark_played", two, map[string]any{"reset_position": true}), "applied")
	pulledB := l.headSeq(t)
	// B listens to one; C favorites three and unfavorites one (never pulled).
	s := newID()
	l.clock += minute
	l.pushOK(t, b, l.lfact(b, "session.start", l.ref, start(s, 0, true)), "applied")
	l.clock += 10 * minute
	l.pushOK(t, b, l.lfact(b, "session.end", l.ref, end(s, 1, 10*minute, 10*minute, "ended", false, true, true)), "applied")
	pulledB2 := l.headSeq(t)
	l.clock += minute
	l.pushOK(t, c, l.lfact(c, "favorite.set", three, map[string]any{"on": true}), "applied")
	l.pushOK(t, c, l.lfact(c, "favorite.set", l.ref, map[string]any{"on": false}), "applied")
	want := l.records()
	lost := l.logCommits(t, atBackup)
	if len(lost) != 5 {
		t.Fatalf("%d lost commits", len(lost))
	}
	var bHolds, cOwn []replicaCommit
	for _, cm := range lost {
		if cm.seq <= pulledB2 {
			bHolds = append(bHolds, cm)
		} else {
			cOwn = append(cOwn, cm)
		}
	}
	_ = pulledB

	l.restore(t, backup)
	if got := l.records(); got["episode/"+two["episode_id"].(string)+"/play_state"] != "" {
		t.Fatalf("the restored server already has the lost mark")
	}
	b2, c2 := l.successor(t, b), l.successor(t, c)
	// B recovers what it holds (A's mark it only received, and its own
	// listening); C its own receipted pushes, without decisions.
	rb, _ := l.startRecovery(t, b2, nil)
	if st := statuses(rb.upload(t, entries(bHolds))); st["joined"] != 3 || len(st) != 1 {
		t.Fatalf("B's upload: %v", st)
	}
	rc, _ := l.startRecovery(t, c2, nil)
	if st := statuses(rc.upload(t, own(cOwn, c))); st["joined"] != 2 || len(st) != 1 {
		t.Fatalf("C's upload: %v", st)
	}
	sameRecords(t, want, l.records())
	if sum := rb.finish(t, true); sum.Counts.Joined != 3 || sum.Complete == nil || !*sum.Complete {
		t.Fatalf("B's recovery: %+v", sum)
	}
	// Recovery converges: a second recovery of the same archives changes
	// nothing (scenario 20's devices retry) and no commit is appended.
	head := l.headSeq(t)
	if st := statuses(rb.upload(t, entries(bHolds))); st["known"] != 3 || len(st) != 1 {
		t.Fatalf("B's second upload: %v", st)
	}
	if l.headSeq(t) != head {
		t.Fatalf("a repeated upload appended a commit")
	}
	if err := l.svc.Store.VerifyChain(context.Background(), l.account); err != nil {
		t.Fatal(err)
	}
	_ = a
	_ = time.Second
}

// ---- provisional supersession (PROTOCOL §6.11)

// A provisional receipt and a provisional podcast creation are both
// superseded by recovered originals: one podcast survives, devices that used
// the provisional IDs are routed, and nothing is counted twice.
func TestRecoverySupersedesProvisionalReceiptsAndCreations(t *testing.T) {
	e := newEnv(t)
	a, b, d := e.device("A"), e.device("B"), e.device("D")
	e.pushOK(t, d, e.fact(d, "account.set_discovery_policy", nil, []any{map[string]any{"entity": "account", "id": e.account}}, map[string]any{"new_episodes_action": "inbox"}), "applied")
	backup := e.backup(t)
	atBackup := e.headSeq(t)

	// After the backup: A subscribes, listens to and marks an episode played.
	e.clock += minute
	sub := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
	e.pushOK(t, a, sub, "applied")
	p := contract.PodcastID(e.namespace, sub["op_id"].(string))
	g := contract.GenerationID(e.namespace, p, sub["op_id"].(string))
	ref := e.episodeRef(p, g, "one")
	s := newID()
	e.clock += minute
	e.pushOK(t, a, e.lfact(a, "session.start", ref, start(s, 0, true)), "applied")
	e.clock += 10 * minute
	e.pushOK(t, a, e.lfact(a, "session.end", ref, end(s, 1, 10*minute, 10*minute, "ended", false, true, true)), "applied")
	e.clock += minute
	mark := e.lfact(a, "episode.mark_played", ref, map[string]any{"reset_position": false})
	e.pushOK(t, a, mark, "applied")
	want := e.records()
	lost := e.logCommits(t, atBackup) // D pulled everything
	e.restore(t, backup)

	// After the restore: B (which never saw A's podcast) subscribes to the
	// same feed: a provisional podcast. A's phone lost its receipts and
	// pushes its subscription and mark again: provisional receipts, which
	// join B's provisional podcast.
	b2, a2 := e.successor(t, b), e.successor(t, a)
	e.clock += minute
	subB := e.fact(b2, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
	e.pushOK(t, b2, subB, "applied")
	p2 := contract.PodcastID(e.namespace, subB["op_id"].(string))
	g2 := contract.GenerationID(e.namespace, p2, subB["op_id"].(string))
	ref2 := e.episodeRef(p2, g2, "two")
	e.pushOK(t, b2, e.lfact(b2, "favorite.set", ref2, map[string]any{"on": true}), "applied")
	var outbox []map[string]any
	for _, cm := range lost {
		for _, x := range cm.entries {
			if x.Fact != nil {
				outbox = append(outbox, x.Fact.Object)
			}
		}
	}
	again := e.push(a2, outbox...)
	for _, x := range again {
		if x.Status != "accepted" {
			t.Fatalf("provisional re-push: %+v", again)
		}
	}

	// D recovers the originals.
	d2 := e.successor(t, d)
	r, _ := e.startRecovery(t, d2, nil)
	st := statuses(r.upload(t, entries(lost)))
	if st["replaced"] != len(outbox) || st["dependency_missing"] != 0 || st["conflict"] != 0 {
		t.Fatalf("D's upload: %v", st)
	}
	got := e.records()
	// One podcast survives: A's original, created first; B's provisional
	// podcast is merged into it, and B's favorite follows.
	if !strings.Contains(got["podcast/"+p2+"/subscription"], `"merged_into":"`+p+`"`) {
		t.Fatalf("the provisional podcast is not merged into the original: %s", got["podcast/"+p2+"/subscription"])
	}
	two, _ := contract.EpisodeID(e.namespace, p, contract.KindRSSGUID, "two")
	if !strings.Contains(got["episode/"+two+"/favorite"], `"on":true`) {
		t.Fatalf("B's favorite did not follow the merge: %v", got["episode/"+two+"/favorite"])
	}
	// Nothing counted twice: A's listening and mark as before the restore.
	for _, k := range []string{"/played_time", "/play_state", "/last_played", "/history"} {
		key := "episode/" + ref["episode_id"].(string) + k
		if got[key] != want[key] {
			t.Errorf("%s: %s, before the restore %s", key, got[key], want[key])
		}
	}
	// The originals' receipts replaced the provisional ones, which are kept.
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		acct := tx.Account(e.account)
		for _, op := range []string{sub["op_id"].(string), mark["op_id"].(string)} {
			f, err := acct.Fact(op)
			if err != nil {
				return err
			}
			if f.Epoch != 1 {
				t.Errorf("%s keeps a provisional receipt at epoch %d", op, f.Epoch)
			}
			sup, err := acct.SupersededReceipts(op)
			if err != nil {
				return err
			}
			if len(sup) != 1 {
				t.Errorf("%s: %d superseded receipts", op, len(sup))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Uploading the provisional receipts again is known (never new evidence).
	var provisional []protocol.RecoveryEntry
	for i, f := range outbox {
		parsed, err := protocol.ParseFact(f)
		if err != nil {
			t.Fatal(err)
		}
		provisional = append(provisional, protocol.RecoveryEntry{Fact: parsed, Receipt: again[i].Receipt})
	}
	head := e.headSeq(t)
	if st := statuses(r.upload(t, provisional)); st["known"] != len(outbox) {
		t.Fatalf("the provisional receipts uploaded again: %v", st)
	}
	if e.headSeq(t) != head {
		t.Fatalf("uploading superseded receipts appended a commit")
	}
}

// ---- replay twice (PROTOCOL §6.11)

// Replaying the same phone archive twice changes no statistics, queue or
// generations (0002): every entry is known and no commit is appended.
func TestRecoveryReplayTwiceChangesNothing(t *testing.T) {
	q := newQueueEnv(t, "One", "Two", "Three")
	a := q.device("A")
	backup := q.backup(t)
	atBackup := q.headSeq(t)
	q.seed(t, a, "One", "Two")
	s := newID()
	q.clock += minute
	q.pushOK(t, a, q.lfactRef(a, "session.start", q.refs["Three"], start(s, 0, true)), "applied")
	q.clock += 5 * minute
	q.pushOK(t, a, q.lfactRef(a, "session.end", q.refs["Three"], end(s, 1, 5*minute, 5*minute, "ended", true, true, true)), "applied")
	q.pushOK(t, a, q.fact(a, "podcast.unsubscribe", nil, podTarget(q.p, q.g), map[string]any{}), "applied")
	want := q.records()
	lost := q.logCommits(t, atBackup)
	q.restore(t, backup)
	a2 := q.successor(t, a)
	r, _ := q.startRecovery(t, a2, nil)
	if st := statuses(r.upload(t, entries(lost))); st["joined"] == 0 || len(st) != 1 {
		t.Fatalf("first upload: %v", st)
	}
	sameRecords(t, want, q.records())
	head := q.headSeq(t)
	state, err := q.svc.Store.DerivedState(context.Background(), q.account)
	if err != nil {
		t.Fatal(err)
	}
	r2, _ := q.startRecovery(t, a2, nil)
	if st := statuses(r2.upload(t, entries(lost))); st["known"] != len(entries(lost)) {
		t.Fatalf("second upload: %v", st)
	}
	if q.headSeq(t) != head {
		t.Fatalf("the second upload appended a commit")
	}
	again, err := q.svc.Store.DerivedState(context.Background(), q.account)
	if err != nil {
		t.Fatal(err)
	}
	for table, rows := range state {
		if !slices.Equal(rows, again[table]) {
			t.Errorf("%s changed on a replay", table)
		}
	}
}

// ---- epoch evidence (PROTOCOL §6.11)

// An undetected in-place copy: the data directory is copied while the server is stopped and both
// copies go on accepting facts (here the copy runs as a second server). Uploads from the copy's
// timeline into the original prove a timeline it never saw: before joining, the server runs the
// full transition to a new epoch (PROTOCOL §11.4), and the archive stays totally ordered.
func TestEpochEvidenceFromALostTimeline(t *testing.T) {
	e := newEnv(t)
	a, b := e.device("A"), e.device("B")
	e.pushOK(t, a, e.fact(a, "account.set_discovery_policy", nil, []any{map[string]any{"entity": "account", "id": e.account}}, map[string]any{"new_episodes_action": "inbox"}), "applied")
	dir := e.svc.Store.Dir()
	other := filepath.Join(t.TempDir(), "copy")
	if err := e.svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := copyDir(dir, other); err != nil {
		t.Fatal(err)
	}
	e.reopen(t)
	atCopy := e.headSeq(t)
	epoch := e.head().Epoch
	main := e.svc
	// The copy's timeline: A subscribes and favorites there.
	copyStore, err := storage.Open(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { copyStore.Close() })
	e.svc = &protocol.Service{Store: copyStore, Registry: main.Registry, Box: main.Box, Now: main.Now}
	p, g, _ := subscribeOne(t, e, a)
	e.pushOK(t, a, e.lfact(a, "favorite.set", e.episodeRef(p, g, "one"), map[string]any{"on": true}), "applied")
	lost := e.logCommits(t, atCopy)
	e.svc = main
	// The original, unaware, accepts B's fact at the same positions.
	e.clock += minute
	e.pushOK(t, b, e.fact(b, "account.set_discovery_policy", nil, []any{map[string]any{"entity": "account", "id": e.account}}, map[string]any{"new_episodes_action": "queue"}), "applied")
	a2 := e.successor(t, a)
	r, _ := e.startRecovery(t, a2, nil)
	if st := statuses(r.upload(t, entries(lost))); st["joined"] != len(entries(lost)) {
		t.Fatalf("upload: %v", st)
	}
	if h := e.head(); h.Epoch <= epoch {
		t.Fatalf("the account did not move to a new epoch: %+v", h)
	}
	// The archive stays totally ordered: (epoch, seq, index, op_id) unique,
	// though both timelines used the same positions.
	positions := map[[4]string]bool{}
	shared := 0
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		ps, err := tx.Account(e.account).FactPositions()
		seen := map[[3]int64]int{}
		for _, p := range ps {
			k := [4]string{itoa(int(p.Epoch)), itoa(int(p.Seq)), itoa(int(p.Index)), p.OpID}
			if positions[k] {
				t.Errorf("two facts at %v", k)
			}
			positions[k] = true
			seen[[3]int64{p.Epoch, p.Seq, p.Index}]++
		}
		for _, n := range seen {
			if n > 1 {
				shared++
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d receipt positions both timelines used", shared)
	if !strings.Contains(e.records()["episode/"+e.episodeRef(p, g, "one")["episode_id"].(string)+"/favorite"], `"on":true`) {
		t.Fatalf("the lost timeline's favorite was not joined")
	}
}

func copyDir(from, to string) error {
	return filepath.Walk(from, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		dst := filepath.Join(to, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, 0o700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, info.Mode().Perm())
	})
}

// ---- the frontier at a head (PROTOCOL §12)

// A recovered entry keeps its original receipt but becomes visible only in
// the commit that joins it: a past head's frontier never changes.
func TestRecoveryKeepsPastFrontiers(t *testing.T) {
	e := newEnv(t)
	a := e.device("A")
	e.pushOK(t, a, e.fact(a, "account.set_discovery_policy", nil, []any{map[string]any{"entity": "account", "id": e.account}}, map[string]any{"new_episodes_action": "inbox"}), "applied")
	backup := e.backup(t)
	atBackup := e.headSeq(t)
	e.pushOK(t, a, e.fact(a, "account.set_discovery_policy", nil, []any{map[string]any{"entity": "account", "id": e.account}}, map[string]any{"new_episodes_action": "queue"}), "applied")
	lost := e.logCommits(t, atBackup)
	e.restore(t, backup)
	b := e.successor(t, e.device("B"))
	e.clock += minute
	e.pushOK(t, b, e.fact(b, "account.set_discovery_policy", nil, []any{map[string]any{"entity": "account", "id": e.account}}, map[string]any{"new_episodes_action": "nothing"}), "applied")
	frontier := func(epoch, seq int64) []string {
		var out []string
		if err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
			var err error
			out, err = r.Account(e.account).FrontierAt(epoch, seq)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	h := e.head()
	before := frontier(h.Epoch, h.Seq)
	a2 := e.successor(t, a)
	r, _ := e.startRecovery(t, a2, nil)
	if st := statuses(r.upload(t, entries(lost))); st["joined"] != 1 {
		t.Fatalf("upload: %v", st)
	}
	if after := frontier(h.Epoch, h.Seq); !slices.Equal(before, after) {
		t.Fatalf("the frontier at %d changed when recovery joined an older entry: %v, then %v", h.Seq, before, after)
	}
	if now := e.head(); !slices.Contains(frontier(now.Epoch, now.Seq), lost[0].facts[0]) {
		t.Fatalf("the joined entry is not in the new head's frontier")
	}
}

// ---- scenario 30 (PROTOCOL §16)

// Restore before a manual played mark later hidden by a now-stale
// completion. Recover from a phone that merely received the mark, then
// deliver the completion's later replay. The manual mark survives because
// its source fact is retained.
func TestScenario30ManualMarkSurvives(t *testing.T) {
	l := newListenEnv(t)
	a, b, c := l.device("A"), l.device("B"), l.device("C")
	backup := l.backup(t)
	atBackup := l.headSeq(t)
	// B starts listening offline before A's mark (its completion is stale).
	s := newID()
	l.clock += minute
	startB := l.lfact(b, "session.start", l.ref, start(s, 0, true))
	l.clock += minute
	mark := l.lfact(a, "episode.mark_played", l.ref, map[string]any{"reset_position": true})
	l.pushOK(t, a, mark, "applied")
	received := l.logCommits(t, atBackup) // C pulled the mark
	l.clock += 30 * minute
	endB := l.lfact(b, "session.end", l.ref, end(s, 1, 30*minute, 30*minute, "ended", true, true, true))
	l.restore(t, backup)
	c2 := l.successor(t, c)
	r, _ := l.startRecovery(t, c2, nil)
	if st := statuses(r.upload(t, entries(received))); st["joined"] != 1 {
		t.Fatalf("C's upload: %v", st)
	}
	// B comes back online and delivers its session (the later replay).
	b2 := l.successor(t, b)
	for _, f := range []map[string]any{startB, endB} {
		if res := l.push(b2, f); res[0].Status != "accepted" {
			t.Fatalf("B's replay: %+v", res)
		}
	}
	got := l.records()
	if !strings.Contains(got["episode/"+l.ep+"/play_state"], `"played"`) {
		t.Fatalf("play state %s", got["episode/"+l.ep+"/play_state"])
	}
	// The mark's source fact is retained with its original receipt.
	if err := l.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		f, err := tx.Account(l.account).Fact(mark["op_id"].(string))
		if err != nil {
			return err
		}
		if f.Outcome != "applied" || f.Epoch != 1 {
			t.Errorf("the manual mark's receipt: %s at epoch %d", f.Outcome, f.Epoch)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = c
}

// ---- scenario 32 (PROTOCOL §16)

// frameEntry frames a recovered fact into parts of at most size bytes (PROTOCOL §6.11, as push's
// parts).
func frameEntry(t *testing.T, e protocol.RecoveryEntry, size int) []protocol.RecoveryEntry {
	t.Helper()
	data := e.Fact.Canonical
	tx := newID()
	var out []protocol.RecoveryEntry
	count := (len(data) + size - 1) / size
	for i := 0; i < count; i++ {
		chunk := data[i*size : min(len(data), (i+1)*size)]
		out = append(out, protocol.RecoveryEntry{Part: &protocol.Part{TransactionID: tx, Index: i, Count: count, ContentHash: e.Fact.ContentHash,
			Data: chunk}, Receipt: e.Receipt})
	}
	return out
}

// Restore before remove+Undo, a multi-field mark-played, a first admission,
// a podcast creation, an occurrence correction and a removed/live podcast
// merge. Recover from two replicas in both upload orders, with a crash
// between frames and between a commit and its response: every original
// effect and generation barrier survives exactly once.
func TestScenario32BothOrders(t *testing.T) {
	c := newCatalogEnv(t)
	a, d := c.device("A"), c.device("D")
	d1 := c.clock - 2*day
	c.serverFetch(t, idFeed(t, c.clock, idItem{"ep1", "Episode 1", "https://cdn.example.com/ep1.mp3", d1}), "")
	ref1 := c.idRef("ep1", "https://cdn.example.com/ep1.mp3", d1)
	ref1["occurrence"].(map[string]any)["title"] = "Episode 1"
	s := newID()
	c.clock += minute
	c.pushOK(t, a, c.lfact(a, "session.start", ref1, start(s, 0, true)), "applied")
	c.clock += 10 * minute
	c.pushOK(t, a, c.lfact(a, "session.end", ref1, end(s, 1, 10*minute, 10*minute, "ended", false, true, true)), "applied")
	backup := c.backup(t)
	atBackup := c.headSeq(t)

	// remove + Undo.
	c.clock += minute
	removal := c.lfact(a, "history.remove", ref1, map[string]any{})
	c.pushOK(t, a, removal, "applied")
	c.pushOK(t, a, c.lfact(a, "history.restore", ref1, map[string]any{"removal": removal["op_id"], "time": c.clock - 12*minute}), "applied")
	// A multi-field mark played.
	c.clock += minute
	c.pushOK(t, a, c.lfact(a, "episode.mark_played", ref1, map[string]any{"reset_position": true}), "applied")
	// A first admission: a new release found by the server.
	c.clock += minute
	c.serverFetch(t, idFeed(t, c.clock, idItem{"ep1", "Episode 1", "https://cdn.example.com/ep1.mp3", d1},
		idItem{"ep2", "Episode 2", "https://cdn.example.com/ep2.mp3", c.clock - minute}), "")
	// An occurrence correction: ep1's GUID changes (same title and date).
	c.clock += minute
	c.serverFetch(t, idFeed(t, c.clock, idItem{"ep1-new", "Episode 1", "https://cdn.example.com/ep1.mp3", d1},
		idItem{"ep2", "Episode 2", "https://cdn.example.com/ep2.mp3", c.clock - 2*minute}), "")
	// A podcast creation at a mirror URL, and a removed/live merge: the
	// original is unsubscribed, then the mirror redirects to it.
	c.clock += minute
	sub := c.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://mirror.example.com/feed"})
	c.pushOK(t, a, sub, "applied")
	p2 := contract.PodcastID(c.namespace, sub["op_id"].(string))
	g2 := contract.GenerationID(c.namespace, p2, sub["op_id"].(string))
	c.fetchFor(t, p2, g2, idFeed(t, c.clock, idItem{"ep2", "Episode 2", "https://cdn.example.com/ep2.mp3", c.clock - 3*minute}))
	c.clock += minute
	c.pushOK(t, a, c.fact(a, "podcast.unsubscribe", nil, podTarget(c.p, c.g), map[string]any{}), "applied")
	c.clock += minute
	c.fetchFor(t, p2, g2, idFeed(t, c.clock, idItem{"ep2", "Episode 2", "https://cdn.example.com/ep2.mp3", c.clock - 4*minute}),
		"https://example.com/feed")
	want := c.records()
	lost := c.logCommits(t, atBackup)
	if !strings.Contains(want["podcast/"+p2+"/subscription"], `"merged"`) {
		t.Fatalf("the scenario did not merge: %s", want["podcast/"+p2+"/subscription"])
	}
	half := lost[:len(lost)/2]
	for _, order := range []string{"short replica first", "long replica first"} {
		t.Run(order, func(t *testing.T) {
			c.restore(t, backup)
			d2 := c.successor(t, d)
			r, _ := c.startRecovery(t, d2, nil)
			first, second := half, lost
			if order == "long replica first" {
				first, second = lost, half
			}
			// The first upload frames its first fact and crashes after one part.
			list := entries(first)
			var framed []protocol.RecoveryEntry
			for i, e := range list {
				if e.Fact != nil {
					framed = frameEntry(t, e, 64)
					list = append(append(append([]protocol.RecoveryEntry{}, list[:i]...), framed...), list[i+1:]...)
					break
				}
			}
			if st := statuses(r.upload(t, framed[:1])); st["part_stored"] != 1 {
				t.Fatalf("the first part: %v", st)
			}
			res := r.upload(t, list)
			if st := statuses(res); st["dependency_missing"] != 0 || st["invalid_receipt"] != 0 || st["conflict"] != 0 {
				t.Fatalf("the first upload: %v", st)
			}
			// A crash between the commit and its response: the retry changes
			// nothing.
			head := c.headSeq(t)
			r.upload(t, list)
			if c.headSeq(t) != head {
				t.Fatalf("a retried upload appended a commit")
			}
			if st := statuses(r.upload(t, entries(second))); st["dependency_missing"] != 0 || st["conflict"] != 0 {
				t.Fatalf("the second upload: %v", st)
			}
			sameRecords(t, want, c.records())
			if sum := r.finish(t, true); sum.Counts.Joined != int64(len(entries(lost))) {
				t.Errorf("joined %d of %d entries", sum.Counts.Joined, len(entries(lost)))
			}
		})
	}
}

// ---- scenario 33 (PROTOCOL §16)

// A library copied while listening, restored before the copy: both the
// original and the copy recover what they hold, in either order; the shared
// prefix counts once and both continuations count.
func TestScenario33CopyThenRestore(t *testing.T) {
	for _, copyFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "original first", true: "copy first"}[copyFirst], func(t *testing.T) {
			l := newListenEnv(t)
			a := l.device("A")
			backup := l.backup(t)
			atBackup := l.headSeq(t)
			s, _, b0 := copyScenario(t, l, a)
			pulled := l.headSeq(t)
			b := l.localCopy(t, a, s, b0)
			s2 := newID()
			st := start(s2, 330*second, true)
			st["baseline"] = newID()
			l.clock += 10 * second
			l.pushOK(t, b, l.lfact(b, "session.start", l.ref, st), "applied")
			l.clock += 20 * second
			l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s2, 1, 350*second, 20*second),
				saveMeta("c1", 1, "c1", st["baseline"].(string), false))), "applied")
			l.clock += 10 * second
			l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 360*second, 360*second),
				saveMeta("sA6", 6, "s4", b0, false))), "applied")
			want := l.records()
			lost := l.logCommits(t, atBackup)
			var prefix []replicaCommit
			for _, cm := range lost {
				if cm.seq <= pulled {
					prefix = append(prefix, cm)
				}
			}
			// The copy holds the original's archive up to the copy, and its own.
			copyHolds := append(entries(prefix), own(lost, b)...)
			origHolds := own(lost, a)
			l.restore(t, backup)
			a2, b2 := l.successor(t, a), l.successor(t, b)
			ra, _ := l.startRecovery(t, a2, nil)
			rb, _ := l.startRecovery(t, b2, nil)
			if copyFirst {
				rb.upload(t, copyHolds)
				ra.upload(t, origHolds)
			} else {
				ra.upload(t, origHolds)
				rb.upload(t, copyHolds)
			}
			sameRecords(t, want, l.records())
			if got := l.playedMS(t); got != 380*second {
				t.Errorf("played time %d ms, want 380 s", got)
			}
		})
	}
}

// ---- scenario 42 (PROTOCOL §16)

// Missing dependencies, an unknown decision, a framed entry, a rejected
// fact and a repeated upload: none drops unrelated entries; recovery
// resumes without a sequence gap or a repeated side effect; and a recovery
// that could not supply every fact is not reported complete.
func TestScenario42NothingSilentlyDropped(t *testing.T) {
	l := newListenEnv(t)
	a, d := l.device("A"), l.device("D")
	backup := l.backup(t)
	atBackup := l.headSeq(t)
	two := l.episodeRef(l.p, l.g, "two")
	fav := l.lfact(a, "favorite.set", l.ref, map[string]any{"on": true})
	l.pushOK(t, a, fav, "applied")
	bad := l.lfact(a, "favorite.set", two, map[string]any{"on": "yes"})
	l.pushOK(t, a, bad, "rejected/invalid_payload")
	destroyed := l.lfact(a, "episode.mark_played", two, map[string]any{"reset_position": false})
	l.pushOK(t, a, destroyed, "applied")
	after := l.lfact(a, "favorite.set", two, map[string]any{"on": true}) // depends on the destroyed mark (its previous fact)
	l.pushOK(t, a, after, "applied")
	l.clock += minute
	e := l.device("E")
	l.pushOK(t, e, l.lfact(e, "episode.reset_position", l.ref, map[string]any{}), "applied")
	lost := l.logCommits(t, atBackup)
	l.restore(t, backup)
	d2 := l.successor(t, d)
	r, _ := l.startRecovery(t, d2, nil)
	var list []protocol.RecoveryEntry
	for _, e := range entries(lost) {
		if e.Fact != nil && e.Fact.OpID == destroyed["op_id"] {
			continue // every copy destroyed
		}
		list = append(list, e)
	}
	// A decision of a type this server does not know, and a tampered receipt.
	unknown := map[string]any{"decision_id": newID(), "decision_type": "test.unknown", "decision_version": int64(1), "dependencies": []any{},
		"normalized_time": l.clock, "epoch": int64(1), "seq": int64(99), "index": int64(0), "subject": "x", "inputs": map[string]any{},
		"result": map[string]any{}}
	keys, _ := l.svc.Store.ReceiptKeys(context.Background())
	unknown["key_id"] = keys[0].ID
	mac, _ := contract.SignedMAC(keys[0].Secret, l.account, normalizeJSON(unknown).(map[string]any))
	unknown = normalizeJSON(unknown).(map[string]any)
	unknown["mac"] = mac
	tampered := list[0]
	tr := map[string]any{}
	for k, v := range tampered.Receipt {
		tr[k] = v
	}
	tr["outcome"] = "superseded"
	list = append(list, protocol.RecoveryEntry{Decision: unknown}, protocol.RecoveryEntry{Fact: tampered.Fact, Receipt: tr})
	// The reset is framed; its first part arrives alone first.
	last := list[len(list)-3]
	framed := frameEntry(t, last, 100)
	list = append(list[:len(list)-3], append(framed, list[len(list)-2:]...)...)
	if st := statuses(r.upload(t, framed[:1])); st["part_stored"] != 1 {
		t.Fatalf("first part: %v", st)
	}
	resp := r.upload(t, list)
	st := statuses(resp)
	if st["dependency_missing"] != 1 || st["invalid"] != 1 || st["invalid_receipt"] != 1 || st["joined"] != 3 {
		t.Fatalf("upload: %v %+v", st, resp.Results)
	}
	if !strings.Contains(l.records()["episode/"+l.ep+"/favorite"], `"on":true`) {
		t.Fatalf("an unrelated favorite was dropped")
	}
	head := l.headSeq(t)
	r.upload(t, list)
	if l.headSeq(t) != head {
		t.Fatalf("a repeated upload appended a commit")
	}
	if err := l.svc.Store.VerifyChain(context.Background(), l.account); err != nil {
		t.Fatal(err)
	}
	sum := r.finish(t, false)
	if sum.Complete == nil || *sum.Complete || sum.Counts.DependencyMissing == 0 || len(sum.Problems) != 1 || sum.Problems[0].Status != "invalid_receipt" {
		t.Fatalf("summary %+v", sum)
	}
}

// ---- the staging fallback (PROTOCOL §6.11)

// With every rebuild staged (threshold 1), a recovery and the rebuild
// invariant run in a copy of the database and are published; the result is
// the in-place one.
func TestRecoveryStaged(t *testing.T) {
	l := newListenEnv(t)
	l.svc.StagingThreshold = 1
	a, d := l.device("A"), l.device("D")
	backup := l.backup(t)
	atBackup := l.headSeq(t)
	s := newID()
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, start(s, 0, true)), "applied")
	l.clock += 10 * minute
	l.pushOK(t, a, l.lfact(a, "session.end", l.ref, end(s, 1, 10*minute, 10*minute, "ended", true, true, true)), "applied")
	l.pushOK(t, a, l.lfact(a, "favorite.set", l.ref, map[string]any{"on": true}), "applied")
	want := l.records()
	lost := l.logCommits(t, atBackup)
	l.restore(t, backup)
	d2 := l.successor(t, d)
	r, _ := l.startRecovery(t, d2, nil)
	if st := statuses(r.upload(t, entries(lost))); st["joined"] != len(entries(lost)) {
		t.Fatalf("staged upload: %v", st)
	}
	sameRecords(t, want, l.records())
	if err := l.svc.Store.VerifyChain(context.Background(), l.account); err != nil {
		t.Fatal(err)
	}
	report, err := l.svc.Rebuild(context.Background(), l.account)
	if err != nil || !report.Staged || report.Committed {
		t.Fatalf("staged rebuild: %v %+v", err, report)
	}
	// A rebuild running elsewhere makes recovery requests and pushes wait.
	done, ok := l.svc.ClaimRebuildForTest(l.account)
	if !ok {
		t.Fatal("claim")
	}
	_, err = l.svc.RecoveryUpload(context.Background(), protocol.RecoveryUploadRequest{AccountID: l.account, InstallID: d2.inst.InstallID,
		RecoveryID: r.id, Entries: entries(lost)}, r.authorize)
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Code != "server_busy" || pe.RetryAfter <= 0 {
		t.Fatalf("during a rebuild: %v", err)
	}
	done()
}
