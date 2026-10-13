package ops

// Deferred refolds publish exactly what immediate ones would, with commits
// replaying to the stored records and snapshots, also when a push stops early.

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"reflect"
	"strings"
	"testing"
)

func expectReplicaMatchesStore(t *testing.T, e *env, a *device) {
	t.Helper()
	replica := map[string]string{}
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		ac := r.Account(e.account)
		h, err := ac.Head()
		if err != nil {
			return err
		}
		commits, err := ac.Commits(0, h.Seq, 0)
		if err != nil {
			return err
		}
		for _, c := range commits {
			obj, err := contract.DecodeJSON(c.Canonical)
			if err != nil {
				return err
			}
			for _, raw := range obj.(map[string]any)["records"].([]any) {
				rec := raw.(map[string]any)
				key := rec["entity"].(string) + "/" + rec["id"].(string) + "/" + rec["field_group"].(string)
				if rec["tombstone"] == true {
					delete(replica, key)
				} else {
					b, err := contract.Canonical(rec["value"])
					if err != nil {
						return err
					}
					replica[key] = string(b)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stored := e.records()
	if !reflect.DeepEqual(replica, stored) {
		t.Fatalf("commit replay differs: replay=%v store=%v", replica, stored)
	}
	resp, err := e.svc.Snapshot(context.Background(), protocol.SnapshotRequest{AccountID: e.account, Mode: "attach", InstallID: a.inst.InstallID})
	if err != nil {
		t.Fatal(err)
	}
	snap := map[string]string{}
	for _, raw := range resp.Items {
		obj, err := contract.DecodeJSON(raw.(json.RawMessage))
		if err != nil {
			t.Fatal(err)
		}
		rec, ok := obj.(map[string]any)["record"].(map[string]any)
		if !ok {
			continue
		}
		key := rec["entity"].(string) + "/" + rec["id"].(string) + "/" + rec["field_group"].(string)
		b, err := contract.Canonical(rec["value"])
		if err != nil {
			t.Fatal(err)
		}
		snap[key] = string(b)
	}
	if !reflect.DeepEqual(snap, stored) {
		t.Fatalf("snapshot differs: snap=%v store=%v", snap, stored)
	}
	if err := e.svc.Store.VerifyChain(context.Background(), e.account); err != nil {
		t.Fatal(err)
	}
}

func TestDeferredRefoldWithClosure(t *testing.T) {
	for _, prior := range []bool{false, true} {
		for _, reopen := range []bool{false, true} {
			t.Run(fmt.Sprintf("prior=%v/reopen=%v", prior, reopen), func(t *testing.T) {
				l := newListenEnv(t)
				a := l.device("A")
				sid := newID()
				facts := []map[string]any{l.lfact(a, "session.start", l.ref, start(sid, 0, true))}
				if prior {
					l.push(a, facts...)
					facts = nil
				}
				cp := checkpoint(sid, 1, minute, minute)
				cp["duration"] = int64(2 * minute)
				facts = append(facts, l.lfact(a, "session.checkpoint", l.ref, cp))
				un := l.lfact(a, "podcast.unsubscribe", nil, map[string]any{})
				un["targets"] = podTarget(l.p, l.g)
				facts = append(facts, un, l.lfact(a, "session.checkpoint", l.ref, checkpoint(sid, 2, 2*minute, 2*minute)))
				closedIndex := len(facts) - 1
				g2 := ""
				if reopen {
					sub := l.lfact(a, "podcast.subscribe", nil, map[string]any{"feed_url": "https://example.com/feed", "observed": map[string]any{"podcast_id": l.p, "generation_id": l.g, "state": "removed"}})
					g2 = contract.GenerationID(l.namespace, l.p, sub["op_id"].(string))
					ref := l.episodeRef(l.p, g2, "one")
					s2 := newID()
					facts = append(facts, sub, l.lfact(a, "session.start", ref, start(s2, 123, true)), l.lfact(a, "session.checkpoint", ref, checkpoint(s2, 1, 456, 333)))
				}
				facts = append(facts, l.lfact(a, "history.clear", nil, map[string]any{}), l.lfact(a, "statistics.reset", nil, map[string]any{}))
				results := l.push(a, facts...)
				for i, r := range results {
					want := "applied"
					if i == closedIndex {
						want = "not_applied/generation_closed"
					}
					if outcome(r) != want {
						t.Fatalf("%d %s: %s want %s", i, facts[i]["type"], outcome(r), want)
					}
				}
				if reopen {
					l.expect(t, "position", `{"claim_id":"`+facts[closedIndex+2]["op_id"].(string)+`","generation_id":"`+g2+`","position":456}`)
					l.expect(t, "played_time", `{"generation_id":"`+g2+`","played_ms":0}`)
					l.expect(t, "history", `{"generation_id":"`+g2+`","time":null}`)
					if l.record("measured_duration") != "" {
						t.Fatal("old duration survived")
					}
				} else {
					for key := range l.records() {
						if strings.HasPrefix(key, "episode/"+l.ep+"/") && !strings.HasSuffix(key, "/identity") {
							t.Fatalf("closed record remains %s", key)
						}
					}
				}
				expectReplicaMatchesStore(t, l.env, a)
			})
		}
	}
}

func TestDeferredRefoldMixedBatch(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	sid := newID()
	st := l.lfact(a, "session.start", l.ref, start(sid, 0, true))
	cp := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sid, 1, minute, minute))
	reset := l.lfact(a, "episode.reset_position", l.ref, map[string]any{})
	cp2 := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sid, 2, 2*minute, 2*minute))
	fin := l.lfact(a, "session.finalize", l.ref, finalize(sid, 2, st["op_id"].(string), true))
	rem := l.lfact(a, "history.remove", l.ref, map[string]any{})
	restore := l.lfact(a, "history.restore", l.ref, map[string]any{"removal": rem["op_id"], "time": int64(0)})
	again := l.lfact(a, "history.restore", l.ref, map[string]any{"removal": rem["op_id"], "time": int64(0)})
	clear := l.lfact(a, "history.clear", nil, map[string]any{})
	stats := l.lfact(a, "statistics.reset", nil, map[string]any{})
	ending := l.lfact(a, "session.end", l.ref, end(sid, 3, 3*minute, 3*minute, "ended", true, true, true))
	facts := []map[string]any{st, cp, reset, cp2, fin, rem, restore, again, clear, stats, ending}
	for i, r := range l.push(a, facts...) {
		want := "applied"
		if i == 4 {
			want = "superseded"
		}
		if i == 7 {
			want = "not_applied/already_restored"
		}
		if outcome(r) != want {
			t.Fatalf("%d: %s want %s", i, outcome(r), want)
		}
	}
	l.expect(t, "position", `{"claim_id":"`+reset["op_id"].(string)+`","generation_id":"`+l.g+`","position":0}`)
	l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":60000}`)
	l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"unplayed"}`)
	expectReplicaMatchesStore(t, l.env, a)
}

func TestDeferredRefoldEarlyStop(t *testing.T) {
	for _, stop := range []string{"gap", "installation_conflict", "dependency_missing"} {
		t.Run(stop, func(t *testing.T) {
			l := newListenEnv(t)
			a := l.device("A")
			sid := newID()
			st := l.lfact(a, "session.start", l.ref, start(sid, 0, true))
			cp := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sid, 1, minute, minute))
			blocked := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sid, 2, 2*minute, 2*minute))
			if stop == "dependency_missing" {
				blocked["dependencies"] = []any{newID()}
			}
			facts := []map[string]any{st, cp, blocked}
			ops := []protocol.PushOp{}
			for i, f := range facts {
				af, err := protocol.ParseFact(f)
				if err != nil {
					t.Fatal(err)
				}
				ops = append(ops, protocol.PushOp{InstallSeq: a.seq + int64(i) + 1, Fact: af})
			}
			if stop == "gap" {
				ops[2].InstallSeq++
			}
			if stop == "installation_conflict" {
				ops[2].InstallSeq = ops[0].InstallSeq
			}
			resp, err := l.rawPush(a, ops, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Results) != 3 || resp.Results[2].Status != stop || resp.InstallWatermark != a.seq+2 {
				t.Fatalf("%+v", resp)
			}
			l.expect(t, "position", `{"claim_id":"`+st["op_id"].(string)+`","generation_id":"`+l.g+`","position":60000}`)
			l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":60000}`)
			expectReplicaMatchesStore(t, l.env, a)
		})
	}
}
