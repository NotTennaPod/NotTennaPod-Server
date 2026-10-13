package ops

// Staged rebuilds. The Python model's own-archive case is a rule vector in
// testdata/own_history.

import (
	"context"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestStagedPreservesNonLogState(t *testing.T) {
	e := newListenEnv(t)
	ctx := context.Background()
	if err := e.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		if err := a.PutFeedResponse(e.p, []byte("cached response")); err != nil {
			return err
		}
		return a.PutRefreshSchedule(storage.RefreshRow{PodcastID: e.p, NextAt: e.clock + day, NotBefore: e.clock + day, Failures: 3})
	}); err != nil {
		t.Fatal(err)
	}
	e.svc.StagingThreshold = 1
	report, err := e.svc.Rebuild(ctx, e.account)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("staged=%v", report.Staged)
	if err := e.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		body, err := a.FeedResponse(e.p)
		if err != nil || string(body) != "cached response" {
			t.Errorf("feed response lost: %q %v", body, err)
		}
		row, ok, err := a.RefreshSchedule(e.p)
		if err != nil || !ok || row.NotBefore != e.clock+day {
			t.Errorf("schedule lost: %+v exists=%v err=%v", row, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFramedEpochEvidence(t *testing.T) {
	e := newListenEnv(t)
	a := e.device("A")
	e.device("B")
	dir := e.svc.Store.Dir()
	other := filepath.Join(t.TempDir(), "copy")
	if err := e.svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := copyDir(dir, other); err != nil {
		t.Fatal(err)
	}
	e.reopen(t)
	at := e.headSeq(t)
	epoch := e.head().Epoch
	main := e.svc
	cs, err := storage.Open(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	e.svc = &protocol.Service{Store: cs, Registry: main.Registry, Box: main.Box, Now: main.Now}
	e.pushOK(t, a, e.lfact(a, "favorite.set", e.ref, map[string]any{"on": true}), "applied")
	lost := entries(e.logCommits(t, at))
	e.svc = main
	a2 := e.successor(t, a)
	r, _ := e.startRecovery(t, a2, nil)
	frames := frameEntry(t, lost[0], 100)
	for _, f := range frames {
		t.Logf("upload: %+v", r.upload(t, []protocol.RecoveryEntry{f}).Results)
	}
	if h := e.head(); h.Epoch <= epoch {
		t.Errorf("framed lost timeline accepted without transition: before epoch=%d after=%+v", epoch, h)
	}
}

func frontierOf(t *testing.T, e *env, h contract.Cursor) []string {
	t.Helper()
	var out []string
	if err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		out, err = r.Account(e.account).FrontierAt(h.Epoch, h.Seq)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestReplacementPastFrontier(t *testing.T) {
	e := newListenEnv(t)
	a := e.device("A")
	backup := e.backup(t)
	at := e.headSeq(t)
	fact := e.lfact(a, "favorite.set", e.ref, map[string]any{"on": true})
	e.pushOK(t, a, fact, "applied")
	lost := entries(e.logCommits(t, at))
	e.restore(t, backup)
	a2 := e.successor(t, a)
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(e.account).UpdateInstallationDevice(a2.inst.InstallID, "A", "1", `["history","queue","inbox","statistics"]`)
	}); err != nil {
		t.Fatal(err)
	}
	e.pushOK(t, a2, fact, "rejected/capability_not_declared")
	h := e.head()
	before := frontierOf(t, e.env, h)
	r, _ := e.startRecovery(t, a2, nil)
	t.Logf("upload: %+v", r.upload(t, lost).Results)
	after := frontierOf(t, e.env, h)
	if !slices.Equal(before, after) {
		t.Errorf("past frontier changed: before=%v after=%v", before, after)
	}
}

func TestInventoryConflictDiagnostics(t *testing.T) {
	e := newListenEnv(t)
	a := e.device("A")
	at := e.headSeq(t)
	e.pushOK(t, a, e.lfact(a, "favorite.set", e.ref, map[string]any{"on": true}), "applied")
	f := entries(e.logCommits(t, at))[0]
	r, _ := e.startRecovery(t, a, nil)
	result, err := e.svc.RecoveryInventory(context.Background(), e.account, a.inst.InstallID, r.id, []protocol.InventoryEntry{{Kind: "fact", ID: f.Fact.OpID, ContentHash: strings.Repeat("f", 64), Position: [3]int64{1, at + 1, 0}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("inventory: %+v", result)
	sum := r.finish(t, false)
	if sum.Counts.Conflict != 1 || len(sum.Problems) != 1 {
		t.Errorf("inventory conflict omitted from diagnostics: %+v", sum)
	}
}

func TestProvisionalGenerationKeepsHistory(t *testing.T) {
	e := newListenEnv(t)
	a, b := e.device("A"), e.device("B")
	unsub := e.fact(a, "podcast.unsubscribe", nil, []any{map[string]any{"entity": "podcast", "id": e.p, "generation": e.g}}, map[string]any{})
	e.pushOK(t, a, unsub, "applied")
	backup := e.backup(t)
	at := e.headSeq(t)
	payload := func() map[string]any {
		return map[string]any{"feed_url": "https://example.com/feed", "observed": map[string]any{"podcast_id": e.p, "generation_id": e.g, "state": "removed"}}
	}
	fa := e.fact(a, "podcast.subscribe", nil, []any{}, payload())
	fb := e.fact(b, "podcast.subscribe", nil, []any{}, payload())
	e.pushOK(t, a, fa, "applied")
	e.pushOK(t, b, fb, "applied")
	lost := entries(e.logCommits(t, at))
	ga := contract.GenerationID(e.namespace, e.p, fa["op_id"].(string))
	gb := contract.GenerationID(e.namespace, e.p, fb["op_id"].(string))
	e.restore(t, backup)
	b2 := e.successor(t, b)
	e.pushOK(t, b2, fb, "applied")
	ref := e.episodeRef(e.p, gb, "one")
	sid := newID()
	e.pushOK(t, b2, e.lfact(b2, "session.start", ref, start(sid, 0, true)), "applied")
	e.clock += minute
	e.pushOK(t, b2, e.lfact(b2, "session.end", ref, end(sid, 1, minute, minute, "ended", false, true, true)), "applied")
	e.pushOK(t, b2, e.lfact(b2, "favorite.set", ref, map[string]any{"on": true}), "applied")
	before := e.records()
	t.Logf("before favorite=%s history=%s", before["episode/"+e.ep+"/favorite"], before["episode/"+e.ep+"/history"])
	r, _ := e.startRecovery(t, b2, nil)
	t.Logf("upload: %+v", r.upload(t, lost).Results)
	after := e.records()
	t.Logf("effective generation=%s provisional=%s records=%v", ga, gb, after)
	if !strings.Contains(after["episode/"+e.ep+"/favorite"], `"on":true`) || after["episode/"+e.ep+"/history"] == "" {
		t.Errorf("favorite on provisional generation lost")
	}
}

func TestOwnArchiveOnlyRestoresDependentEffects(t *testing.T) {
	e := newEnv(t)
	a := e.device("A")
	backup := e.backup(t)
	at := e.headSeq(t)
	p, g, _ := subscribeOne(t, e, a)
	ref := e.episodeRef(p, g, "one")
	ep := ref["episode_id"].(string)
	sid := newID()
	e.pushOK(t, a, e.lfact(a, "session.start", ref, start(sid, 0, true)), "applied")
	e.clock += minute
	e.pushOK(t, a, e.lfact(a, "session.end", ref, end(sid, 1, minute, minute, "ended", false, true, true)), "applied")
	e.pushOK(t, a, e.lfact(a, "favorite.set", ref, map[string]any{"on": true}), "applied")
	lost := own(e.logCommits(t, at), a)
	e.restore(t, backup)
	a2 := e.successor(t, a)
	r, _ := e.startRecovery(t, a2, nil)
	t.Logf("first upload: %+v", r.upload(t, lost).Results)
	t.Logf("retry: %+v", r.upload(t, lost).Results)
	r.finish(t, true)
	got := e.records()
	t.Logf("after finish: %v", got)
	if !strings.Contains(got["episode/"+ep+"/favorite"], `"on":true`) || got["episode/"+ep+"/history"] == "" {
		t.Errorf("own receipted listening/favorite facts still have no effects after successful recovery and retry")
	}
}

func TestStagedRetiresCompletedParts(t *testing.T) {
	e := newListenEnv(t)
	a := e.device("A")
	backup := e.backup(t)
	at := e.headSeq(t)
	e.pushOK(t, a, e.lfact(a, "favorite.set", e.ref, map[string]any{"on": true}), "applied")
	lost := entries(e.logCommits(t, at))
	e.restore(t, backup)
	a2 := e.successor(t, a)
	r, _ := e.startRecovery(t, a2, nil)
	e.svc.StagingThreshold = 1
	frames := frameEntry(t, lost[0], 100)
	r.upload(t, frames[:1])
	t.Logf("completion: %+v", r.upload(t, frames[1:]).Results)
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		acct := tx.Account(e.account)
		if slot, err := acct.RecoveryFrameSlot(a2.inst.InstallID, frames[0].Part.TransactionID); err != nil || slot != 0 {
			if err == nil {
				t.Errorf("successful staged upload left its completed frame (slot %d)", slot)
			}
			return err
		}
		frames, bytes, err := acct.RecoveryFrameUsage(a2.inst.InstallID)
		if err != nil {
			return err
		}
		if frames != 0 || bytes != 0 {
			t.Errorf("successful staged upload left %d frames, %d bytes of parts", frames, bytes)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// A history concentrated in one episode is staged by its cost estimate, not
// only by the archive's size (9,018 facts in one episode once held the writer for 66 s in place).
func TestConcentratedHistoryIsStaged(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("load test")
	}
	l := newListenEnv(t)
	a := l.device("A")
	var batch []map[string]any
	for i := 0; i < 1100; i++ {
		l.clock += minute
		batch = append(batch, l.lfact(a, "favorite.set", l.ref, map[string]any{"on": i%2 == 0}))
		if len(batch) == 100 {
			for _, r := range l.push(a, batch...) {
				if r.Status != "accepted" {
					t.Fatalf("push: %+v", r)
				}
			}
			batch = nil
		}
	}
	size, concentration, err := l.svc.Store.RebuildCost(context.Background(), l.account)
	if err != nil {
		t.Fatal(err)
	}
	report, err := l.svc.Rebuild(context.Background(), l.account)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d entries, concentration %d: staged %v, write lock %v of %v", size, concentration, report.Staged, report.Locked, report.Duration)
	if size >= protocol.DefaultStagingThreshold || !report.Staged {
		t.Errorf("a concentrated history of %d entries was not staged", size)
	}
}
