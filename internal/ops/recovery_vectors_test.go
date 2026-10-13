package ops

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// vectorRecovery joins a rule vector's recovery arrivals (docs/CONTRACT.md
// "Recovery") through the server's recovery upload: recovered facts and
// decisions are collected into one upload request, which ends at a
// recovery_end (every end-of-request step), at a recovered identity decision
// (the reference unifies right after one), or before a live arrival (no
// step). Receipts and decisions are signed with the server's key, as the
// original server signed them.
type vectorRecovery struct {
	t         *testing.T
	store     *storage.Store
	svc       *protocol.Service
	account   string
	random    *vectorRandom
	setNow    func(int64)
	install   string
	recovery  string
	batch     []protocol.RecoveryEntry
	last      int64 // the 1-based position of the batch's last arrival
	explicit  bool  // the batch holds explicit receipts (evidence of a lost timeline is possible)
	uploaded  map[string]bool
	key       storage.ReceiptKey
	sameEpoch bool
	// mid: the last upload ended no request (no recovery_end, no
	// unification): it published nothing, so the publication invariant is
	// not due (only at publication boundaries).
	mid bool
}

// unifiedBy are the decision types the reference unifies right after.
var unifiedBy = map[string]bool{"podcast.alias": true, "podcast.resolution": true, "podcast.merge": true, "episode.merge": true,
	"episode.guid_change": true, "episode.occurrence_mapping": true}

func newVectorRecovery(t *testing.T, store *storage.Store, svc *protocol.Service, account string, random *vectorRandom, setNow func(int64)) *vectorRecovery {
	keys, err := store.ReceiptKeys(context.Background())
	if err != nil || len(keys) == 0 {
		t.Fatalf("receipt keys: %v", err)
	}
	return &vectorRecovery{t: t, store: store, svc: svc, account: account, random: random, setNow: setNow, key: keys[0],
		install: "0000f00d-0000-4000-8000-00000000f00d", recovery: "0000f00d-0000-4000-8000-0000000ec0de", uploaded: map[string]bool{}}
}

func (v *vectorRecovery) open() {
	err := v.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		acct := tx.Account(v.account)
		if _, err := acct.Installation(v.install); err != nil {
			if err := acct.InsertInstallation(storage.Installation{InstallID: v.install, InstanceNonce: v.install, Name: "recovery",
				Platform: "ios", AppVersion: "1", Capabilities: "[]"}); err != nil {
				return err
			}
		}
		_, err := acct.OpenRecovery(v.recovery, v.install, 0)
		return err
	})
	if err != nil {
		v.t.Fatal(err)
	}
}

func num64(x any) (int64, bool) {
	n, ok := x.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := n.Int64()
	return i, err == nil
}

// take handles a recovery arrival (true), or reports a live one (false).
func (v *vectorRecovery) take(i int, arr map[string]any) bool {
	t := v.t
	switch {
	case arr["recovery_end"] == true:
		rt, _ := num64(arr["receive_time"])
		v.setNow(rt)
		v.flush(protocol.AllRecoveryEndSteps, arr["decisions"], int64(i+1))
		return true
	case arr["decision"] != nil:
		obj := arr["decision"].(map[string]any)
		d := map[string]any{}
		for k, x := range obj {
			d[k] = x
		}
		d["key_id"] = v.key.ID
		mac, err := contract.SignedMAC(v.key.Secret, v.account, d)
		if err != nil {
			t.Fatal(err)
		}
		d["mac"] = mac
		// A duplicate delivery of a recovered decision commits nothing, so it
		// is no unification point.
		dup := v.uploaded[d["decision_id"].(string)]
		v.uploaded[d["decision_id"].(string)] = true
		v.batch = append(v.batch, protocol.RecoveryEntry{Decision: d})
		v.last = int64(i + 1)
		if typ, _ := obj["decision_type"].(string); unifiedBy[typ] && !dup {
			rt, ok := num64(arr["receive_time"])
			if !ok {
				rt, _ = num64(obj["normalized_time"])
			}
			v.setNow(rt)
			v.flush(protocol.RecoveryEndSteps{Unify: true}, arr["decisions"], int64(i+1))
		}
		return true
	case arr["recovery"] == true:
		fact := arr["fact"].(map[string]any)
		a, err := protocol.ParseFact(fact)
		if err != nil {
			t.Fatalf("arrival %d: %v", i, err)
		}
		r := map[string]any{"op_id": a.OpID, "content_hash": a.ContentHash, "epoch": int64(1), "seq": int64(i + 1), "index": int64(0)}
		outcome, reason := "applied", ""
		if a.Violation != "" {
			outcome, reason = "rejected", a.Violation
		}
		if rc, ok := arr["receipt"].(map[string]any); ok {
			v.explicit = true
			for _, k := range []string{"epoch", "seq", "index"} {
				r[k], _ = num64(rc[k])
			}
			if o, ok := rc["outcome"].(string); ok {
				outcome, reason = o, ""
				reason, _ = rc["reason"].(string)
			}
			// The sort values the server recorded with a queue placement or
			// sort trigger (PROTOCOL §7.6) travel in its receipt.
			if sv, ok := rc["values"].([]any); ok {
				r["sort_values"] = sv
			}
		} else {
			r["epoch"] = v.epoch()
		}
		r["outcome"] = outcome
		if reason != "" {
			r["reason"] = reason
		}
		nt, _ := num64(arr["normalized_time"])
		r["normalized_time"] = nt
		if outcome == "rejected" {
			rt, _ := num64(arr["receive_time"])
			r["normalized_time"] = rt
			edges := []any{}
			for _, e := range arr["edges"].([]any) {
				edges = append(edges, e)
			}
			r["frontier"] = edges
		}
		r["key_id"] = v.key.ID
		mac, err := contract.SignedMAC(v.key.Secret, v.account, normalizeJSON(r).(map[string]any))
		if err != nil {
			t.Fatal(err)
		}
		r = normalizeJSON(r).(map[string]any)
		r["mac"] = mac
		var secrets []protocol.Secret
		_ = secrets
		v.batch = append(v.batch, protocol.RecoveryEntry{Fact: a, Receipt: r})
		v.last = int64(i + 1)
		return true
	}
	return false
}

func (v *vectorRecovery) epoch() int64 {
	var e int64
	v.store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(v.account).Head()
		e = h.Epoch
		return err
	})
	return e
}

// toEpoch moves the account to a live receipt's epoch (a restored server
// that already moved, as the vector's receipts say).
func (v *vectorRecovery) toEpoch(epoch int64) {
	for v.epoch() < epoch {
		if err := v.store.Write(context.Background(), func(tx *storage.WriteTx) error {
			return tx.RestoreTransition("rule vector: a restored server")
		}); err != nil {
			v.t.Fatal(err)
		}
	}
}

// flush uploads the batch (if any) as one request at position at (the
// batch's last arrival when 0) with the given end-of-request steps and the
// server's choices for the decisions they make.
func (v *vectorRecovery) flush(steps protocol.RecoveryEndSteps, choices any, at int64) {
	if len(v.batch) == 0 && steps == (protocol.RecoveryEndSteps{}) {
		return
	}
	if at == 0 {
		at = v.last
	}
	if v.install != "" {
		v.open()
	}
	padLog(v.t, v.store, v.account, at-1)
	if os.Getenv("NTP_DEBUG_RECOVERY") != "" {
		v.store.Read(context.Background(), func(r *storage.ReadTx) error {
			rows, _ := r.Account(v.account).Records(nil)
			for _, x := range rows {
				if strings.Contains(os.Getenv("NTP_DEBUG_RECOVERY"), x.FieldGroup) {
					v.t.Logf("before the upload at %d: %s/%s/%s %s", at, x.Entity, x.EntityID, x.FieldGroup, x.Value)
				}
			}
			return nil
		})
	}
	v.random.load(int(at-1), choices)
	ctx := context.Background()
	if len(v.batch) == 0 {
		if err := v.svc.RecoveryEnd(ctx, v.account, steps); err != nil {
			v.t.Fatalf("arrival %d: recovery end: %v", at-1, err)
		}
	} else {
		req := protocol.RecoveryUploadRequest{AccountID: v.account, InstallID: v.install, RecoveryID: v.recovery, Entries: v.batch,
			Steps: steps, SameTimeline: !v.explicit}
		for _, e := range v.batch {
			if e.Fact == nil {
				continue
			}
			if payload, ok := e.Fact.Object["payload"].(map[string]any); ok {
				for _, k := range []string{"credentials_secret", "secret_id"} {
					if id, ok := payload[k].(string); ok {
						req.Secrets = append(req.Secrets, protocol.Secret{ID: id, Username: "u", Password: "p"})
					}
				}
			}
		}
		resp, err := v.svc.RecoveryUpload(ctx, req, func(tx *storage.WriteTx) (*storage.Installation, error) {
			inst, err := tx.Account(v.account).Installation(v.install)
			return &inst, err
		})
		if err != nil {
			v.t.Fatalf("arrival %d: recovery upload: %v", at-1, err)
		}
		for _, r := range resp.Results {
			switch r.Status {
			case "joined", "known", "replaced":
			case "conflict":
				v.t.Logf("arrival %d: %s %s: conflict (changes nothing)", at-1, r.Kind, r.ID)
			default:
				v.t.Errorf("arrival %d: recovery upload of %s %s: %s %v", at-1, r.Kind, r.ID, r.Status, r.Missing)
			}
		}
	}
	v.random.done()
	v.batch, v.explicit = nil, false
	v.mid = steps == (protocol.RecoveryEndSteps{})
}
