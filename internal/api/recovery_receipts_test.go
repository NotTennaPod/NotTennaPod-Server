package api

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// A kept later receipt is published in a commit and its node's causal time does
// not depend on request order; after a staged upload's own epoch transition, a
// password change or reset still stops its publication.

func TestKeptReceiptPublication(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprint(staged), func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			r.raise()
			r.raise()
			f, original := r.fact(1)
			original["outcome"] = "rejected"
			original["reason"] = "capability_not_declared"
			original["frontier"] = []any{}
			r.sign(original, r.account)
			uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": original}))
			if staged {
				r.e.srv.cfg.Protocol.StagingThreshold = 1
			}
			body := map[string]any{"mode": "attach", "install_id": r.p.install, "acked_install_seq": 0}
			first := r.e.do("POST", "/v1/snapshot", body, r.p.cred).must(200)
			before, _ := r.e.store.Head(context.Background(), r.account)
			later := map[string]any{}
			for k, v := range original {
				later[k] = v
			}
			later["epoch"] = 2
			later["outcome"] = "applied"
			delete(later, "reason")
			delete(later, "frontier")
			r.sign(later, r.account)
			uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": later}))
			after, _ := r.e.store.Head(context.Background(), r.account)
			if after.Seq <= before.Seq {
				t.Errorf("new causal evidence invisible to pull: head unchanged at %d", after.Seq)
			}
			next := r.e.do("POST", "/v1/snapshot", body, r.p.cred).must(200)
			if !strings.Contains(string(next.raw), later["mac"].(string)) {
				t.Errorf("snapshot reused old archive and omitted kept receipt: before=%s after=%s", first.raw, next.raw)
			}
			var frontier []string
			r.e.store.Read(context.Background(), func(tx *storage.ReadTx) error {
				var err error
				frontier, err = tx.Account(r.account).FrontierAt(after.Epoch, after.Seq)
				return err
			})
			found := false
			for _, id := range frontier {
				found = found || id == f["op_id"]
			}
			if !found {
				t.Errorf("current head frontier omits newly admitted node: %v", frontier)
			}
		})
	}
}

func TestReceiptPermutations(t *testing.T) {
	for _, order := range [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
		for _, batched := range []bool{false, true} {
			t.Run(fmt.Sprint(order)+fmt.Sprint(batched), func(t *testing.T) {
				r := newRecoveryEnv(t)
				r.raise()
				r.raise()
				r.raise()
				f, original := r.fact(1)
				receipts := []map[string]any{}
				for i := 0; i < 3; i++ {
					rc := map[string]any{}
					for k, v := range original {
						rc[k] = v
					}
					rc["epoch"] = i + 1
					rc["normalized_time"] = (i + 1) * 100
					if i == 0 {
						rc["outcome"] = "rejected"
						rc["reason"] = "capability_not_declared"
						rc["frontier"] = []any{}
					}
					r.sign(rc, r.account)
					receipts = append(receipts, rc)
				}
				entries := []any{}
				for _, i := range order {
					entry := map[string]any{"fact": f, "receipt": receipts[i]}
					if batched {
						entries = append(entries, entry)
					} else {
						r.upload(entry).must(200)
					}
				}
				if batched {
					r.upload(entries...).must(200)
				}
				var nt int64
				// The minimum retained provisional normalized time is the documented Go replay rule.
				// Compare only orders where both provisional receipts were retained; other later
				// admissions while the primary admits are deliberately "known" by the contract.
				n := r.e.count("SELECT count(*) FROM superseded_receipts WHERE account_id=? AND op_id=?", r.account, f["op_id"])
				if n == 2 {
					nt = int64(r.e.count("SELECT normalized_time FROM causal_nodes WHERE account_id=? AND node_id=?", r.account, f["op_id"]))
					if nt != 200 {
						t.Errorf("same retained evidence, causal time=%d want 200", nt)
					}
				}
				d := r.decision(1)
				d["dependencies"] = []any{f["op_id"]}
				r.sign(d, r.account)
				if st := uploadStatus(t, r.upload(map[string]any{"decision": d})); st != "joined" {
					t.Fatal(st)
				}
				if r.finish().body["complete"] != true {
					t.Fatal("valid recovery did not complete")
				}
			})
		}
	}
}

// The epoch transition necessarily removes the old credential. A later reset
// must still fence publication of that old authenticated request.
func TestStagedTransitionSecurityRace(t *testing.T) {
	for _, change := range []string{"reset", "password", "disable", "revoke", "delete", "none"} {
		t.Run(change, func(t *testing.T) {
			r := newRecoveryEnv(t)
			pushed(t, r.p.push(op{seq: 1, fact: r.p.noteFact("baseline")}))
			r.e.srv.cfg.Protocol.StagingThreshold = 1
			ran := false
			r.e.srv.cfg.Protocol.Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error {
				if ran || r.epoch() <= 1 {
					return nil
				}
				ran = true
				root := r.e.session("root", "root-password-1")
				switch change {
				case "reset":
					r.e.resetPassword(root, r.account, "replacement-password").must(200)
				case "password":
					web := r.e.session("alice", "alice-password-1")
					r.e.do("POST", "/v1/account/password", map[string]any{"current": "alice-password-1", "new": "replacement-password"}, browser(web)).must(200)
				case "disable":
					r.e.setState(root, r.account, "disabled").must(200)
				case "revoke":
					web := r.e.session("alice", "alice-password-1")
					r.e.revoke(browser(web), r.p.install).must(200)
				case "delete":
					r.e.adminDo(root, "POST", "/v1/admin/accounts/"+r.account+"/delete", map[string]any{"confirm_username": "alice"}).must(200)
				}
				return nil
			})
			d := r.decision(1)
			got := r.upload(map[string]any{"decision": d})
			if !ran {
				t.Fatal("publication race hook not reached")
			}
			n := r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"])
			if change == "none" {
				got.must(200)
				if n != 1 {
					t.Fatal(n)
				}
				return
			}
			if got.status < 400 || n != 0 {
				t.Errorf("%s after epoch transition: status=%d published=%d body=%s", change, got.status, n, got.raw)
			}
		})
	}
}
