package ops

import (
	"context"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Staged rebuilds and replacements. The two past-frontier variants are
// TestRealRepeatedReplacementPastFrontier and
// TestReplacementWithDescendantPastFrontier.

func TestStagedPreservesConcurrentFeedState(t *testing.T) {
	e := newListenEnv(t)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "stage")
	st, err := e.svc.Store.StageCopy(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	base, err := st.Head(ctx, e.account)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	err = e.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		if err := a.PutFeedResponse(e.p, []byte("concurrent cached response")); err != nil {
			return err
		}
		return a.PutRefreshSchedule(storage.RefreshRow{PodcastID: e.p, NextAt: e.clock + day, NotBefore: e.clock + 2*day, Failures: 7})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.svc.Store.PublishStaged(ctx, filepath.Join(dir, "nottennapod.db"), e.account, base, nil); err != nil {
		t.Fatal(err)
	}
	err = e.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		body, err := a.FeedResponse(e.p)
		if err != nil {
			return err
		}
		if string(body) != "concurrent cached response" {
			t.Errorf("cache overwritten: %q", body)
		}
		row, ok, err := a.RefreshSchedule(e.p)
		if err != nil {
			return err
		}
		if !ok || row.NotBefore != e.clock+2*day || row.Failures != 7 {
			t.Errorf("schedule overwritten: %+v", row)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOwnArchiveCommitMatchesStored(t *testing.T) {
	for _, order := range []string{"all", "reverse", "separate"} {
		t.Run(order, func(t *testing.T) {
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
			if order == "reverse" {
				slices.Reverse(lost)
			}
			if order == "separate" {
				for i := range lost {
					r.upload(t, lost[i:i+1])
				}
			} else {
				r.upload(t, lost)
			}
			got := e.records()
			if !strings.Contains(got["episode/"+ep+"/favorite"], `"on":true`) || got["episode/"+ep+"/history"] == "" {
				t.Fatal("dependent effects missing before any retry")
			}
			// Apply published records as a pulling client would, and compare the final
			// values and provenance with the database after both rebuilds settled.
			latest := map[string]string{}
			err := e.svc.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
				acct := tx.Account(e.account)
				h, err := acct.Head()
				if err != nil {
					return err
				}
				cms, err := acct.Commits(at, h.Seq, int(h.Seq-at)+1)
				if err != nil {
					return err
				}
				for _, cm := range cms {
					v, err := contract.DecodeJSON(cm.Canonical)
					if err != nil {
						return err
					}
					for _, raw := range v.(map[string]any)["records"].([]any) {
						rec := raw.(map[string]any)
						key := rec["entity"].(string) + "/" + rec["id"].(string) + "/" + rec["field_group"].(string)
						if rec["tombstone"] == true {
							delete(latest, key)
							continue
						}
						b, err := contract.Canonical(rec["value"])
						if err != nil {
							return err
						}
						latest[key] = string(b)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range latest {
				if got[key] != value {
					t.Errorf("published %s=%s; stored=%s", key, value, got[key])
				}
			}
		})
	}
}
func TestJoinedGenerationAcceptsLiveChanges(t *testing.T) {
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
	// Live references held by the provisional phone must continue routing.
	e.pushOK(t, b2, e.lfact(b2, "favorite.set", ref, map[string]any{"on": false}), "applied")
	if strings.Contains(e.records()["episode/"+e.ep+"/favorite"], `"on":true`) {
		t.Fatal("live favorite did not route out of joined generation")
	}
	leave := e.fact(b2, "podcast.unsubscribe", nil, []any{map[string]any{"entity": "podcast", "id": e.p, "generation": gb}}, map[string]any{})
	e.pushOK(t, b2, leave, "applied")
	var row storage.GenerationRow
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		var err error
		row, err = tx.Account(e.account).Generation(ga)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if row.ClosedBy != leave["op_id"] {
		t.Fatalf("joined generation unsubscribe did not close target: %+v", row)
	}
}

func TestRealRepeatedReplacementPastFrontier(t *testing.T) {
	e := newListenEnv(t)
	a := e.device("A")
	backup := e.backup(t)
	at := e.headSeq(t)
	fact := e.lfact(a, "favorite.set", e.ref, map[string]any{"on": true})
	e.pushOK(t, a, fact, "applied")
	original := entries(e.logCommits(t, at))
	e.restore(t, backup)
	a2 := e.successor(t, a)
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(e.account).UpdateInstallationDevice(a2.inst.InstallID, "A", "1", `["history","queue","inbox","statistics"]`)
	})
	if err != nil {
		t.Fatal(err)
	}
	e.pushOK(t, a2, fact, "rejected/capability_not_declared")
	middle := entries(e.logCommits(t, at))
	e.restore(t, backup)
	a3 := e.successor(t, a)
	e.pushOK(t, a3, fact, "applied")
	h := e.head()
	before := frontierOf(t, e.env, h)
	r, _ := e.startRecovery(t, a3, nil)
	r.upload(t, middle)
	between := frontierOf(t, e.env, h)
	r.upload(t, original)
	after := frontierOf(t, e.env, h)
	t.Logf("actual server receipts applied/rejected/applied: before=%v after rejection=%v after original=%v", before, between, after)
	if !slices.Equal(before, after) {
		t.Error("past frontier changed with real receipts from two restores")
	}
}

func TestReplacementWithDescendantPastFrontier(t *testing.T) {
	e := newListenEnv(t)
	a := e.device("A")
	b := e.device("B")
	baseFact := e.lfact(b, "favorite.set", e.ref, map[string]any{"on": false})
	e.pushOK(t, b, baseFact, "applied")
	backup := e.backup(t)
	at := e.headSeq(t)
	fact := e.lfact(a, "favorite.set", e.ref, map[string]any{"on": true}, baseFact["op_id"].(string))
	e.pushOK(t, a, fact, "applied")
	original := entries(e.logCommits(t, at))
	e.restore(t, backup)
	a2 := e.successor(t, a)
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(e.account).UpdateInstallationDevice(a2.inst.InstallID, "A", "1", `["history","queue","inbox","statistics"]`)
	})
	if err != nil {
		t.Fatal(err)
	}
	e.pushOK(t, a2, fact, "rejected/capability_not_declared")
	child := e.lfact(a2, "session.start", e.ref, start(newID(), 0, true), fact["op_id"].(string))
	e.pushOK(t, a2, child, "applied")
	h := e.head()
	before := frontierOf(t, e.env, h)
	r, _ := e.startRecovery(t, a2, nil)
	r.upload(t, original)
	after := frontierOf(t, e.env, h)
	t.Logf("past frontier with descendant: before=%v after=%v", before, after)
	if !slices.Equal(before, after) {
		t.Error("past frontier changed when descendant edges were rederived through later-published node")
	}
}
