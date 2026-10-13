package api

import (
	"context"
	"encoding/json"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"net/http"
	"strings"
	"testing"
	"time"
)

type recoveryEnv struct {
	e            *env
	p            *phone
	account, rid string
}

func newRecoveryEnv(t *testing.T) *recoveryEnv {
	e := newEnv(t, withSyncBox(t))
	root, _ := e.setupAdmin("root", "root-password-1")
	account := e.createAccount(root, "alice", "alice-password-1", false)
	p := e.phoneOf("alice", "alice-password-1")
	r := &recoveryEnv{e, p, account, newID()}
	r.open()
	e.srv.cfg.Protocol.Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error { return nil })
	return r
}
func (r *recoveryEnv) open() {
	r.e.do("POST", "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": r.rid, "install_id": r.p.install, "acked_install_seq": 0}, r.p.cred).must(200)
}
func (r *recoveryEnv) sign(m map[string]any, account string) {
	keys, err := r.e.store.ReceiptKeys(context.Background())
	if err != nil {
		r.e.t.Fatal(err)
	}
	m["key_id"] = keys[0].ID
	mac, err := contract.SignedMAC(keys[0].Secret, account, m)
	if err != nil {
		r.e.t.Fatal(err)
	}
	m["mac"] = mac
}
func (r *recoveryEnv) fact(epoch int64) (map[string]any, map[string]any) {
	f := r.p.noteFact("lost fact")
	rc := map[string]any{"op_id": f["op_id"], "content_hash": contentHash(r.e.t, f), "outcome": "applied", "normalized_time": f["client_time"], "epoch": epoch, "seq": 1, "index": 0}
	r.sign(rc, r.account)
	return f, rc
}
func (r *recoveryEnv) decision(epoch int64) map[string]any {
	d := map[string]any{"decision_id": newID(), "decision_type": "test.decision", "decision_version": 1, "dependencies": []any{}, "normalized_time": time.Now().UnixMilli(), "epoch": epoch, "seq": 1, "index": 1, "subject": "x", "inputs": map[string]any{}, "result": map[string]any{}}
	r.sign(d, r.account)
	return d
}
func (r *recoveryEnv) upload(entries ...any) *response {
	return r.e.do("POST", "/v1/sync/recovery/upload", map[string]any{"recovery_id": r.rid, "entries": entries}, r.p.cred)
}
func uploadStatus(t *testing.T, x *response) string {
	t.Helper()
	x.must(200)
	return x.body["results"].([]any)[0].(map[string]any)["status"].(string)
}
func (r *recoveryEnv) finish() *response {
	return r.e.do("POST", "/v1/sync/recovery/finish", map[string]any{"recovery_id": r.rid, "complete": true}, r.p.cred).must(200)
}
func (r *recoveryEnv) epoch() int {
	return r.e.count("SELECT epoch FROM account_logs WHERE account_id=?", r.account)
}
func (r *recoveryEnv) raise() {
	r.e.t.Helper()
	err := r.e.store.Write(context.Background(), func(tx *storage.WriteTx) error { return tx.EvidenceTransition(1) })
	if err != nil {
		r.e.t.Fatal(err)
	}
	r.p.token = r.e.token("alice", "alice-password-1", r.p.install, r.p.nonce)
	r.p.cred = bearer(r.p.token)
}

func TestInvalidReceiptMatrix(t *testing.T) {
	for _, kind := range []string{"fact", "decision"} {
		for _, mutation := range []string{"mac", "key", "position", "outcome_or_result", "time", "payload_or_inputs", "account", "server"} {
			t.Run(kind+"/"+mutation, func(t *testing.T) {
				r := newRecoveryEnv(t)
				f, rc := r.fact(1)
				d := r.decision(1)
				signed := rc
				if kind == "decision" {
					signed = d
				}
				switch mutation {
				case "mac":
					signed["mac"] = strings.Repeat("0", 64)
				case "key":
					signed["key_id"] = "unknown"
				case "position":
					signed["seq"] = 99
				case "outcome_or_result":
					if kind == "fact" {
						signed["outcome"] = "rejected"
					} else {
						signed["result"] = map[string]any{"changed": true}
					}
				case "time":
					signed["normalized_time"] = 1
				case "payload_or_inputs":
					if kind == "fact" {
						f["payload"] = map[string]any{"text": "modified"}
					} else {
						signed["inputs"] = map[string]any{"modified": true}
					}
				case "account":
					r.sign(signed, newID())
				case "server":
					mac, _ := contract.SignedMAC([]byte("different server signing key"), r.account, signed)
					signed["mac"] = mac
				}
				var entry any = map[string]any{"fact": f, "receipt": rc}
				if kind == "decision" {
					entry = map[string]any{"decision": d}
				}
				if got := uploadStatus(t, r.upload(entry)); got != "invalid_receipt" {
					t.Fatalf("got %s", got)
				}
				if r.epoch() != 1 {
					t.Fatal("invalid receipt changed epoch")
				}
				if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=?", r.account) + r.e.count("SELECT count(*) FROM decisions WHERE account_id=?", r.account); n != 0 {
					t.Fatal("invalid evidence archived")
				}
				fin := r.finish()
				if len(fin.body["problems"].([]any)) != 1 {
					t.Fatal("problem not listed")
				}
				if fin.body["complete"] == true {
					t.Errorf("invalid receipt recorded but finish complete=true")
				}
			})
		}
	}
}

func TestFramedEvidence(t *testing.T) {
	for _, framed := range []bool{false, true} {
		name := "plain"
		if framed {
			name = "framed"
		}
		t.Run(name, func(t *testing.T) {
			r := newRecoveryEnv(t)
			f, rc := r.fact(1)
			before := r.e.count("SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM server_meta WHERE key='auth_revision'),0)")
			if !framed {
				if got := uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": rc})); got != "joined" {
					t.Fatal(got)
				}
			} else {
				parts := frame(t, f, 160)
				for _, p := range parts {
					r.upload(map[string]any{"part": p, "receipt": rc}).must(200)
				}
			}
			ep := r.epoch()
			tokens := r.e.count("SELECT count(*) FROM auth_tokens")
			after := r.e.count("SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM server_meta WHERE key='auth_revision'),0)")
			t.Logf("epoch=%d tokens=%d authrevision %d->%d archived=%d", ep, tokens, before, after, r.e.count("SELECT count(*) FROM facts WHERE account_id=?", r.account))
			if ep <= 1 || tokens != 0 || after <= before {
				t.Error("lost-timeline entry joined without full transition")
			}
		})
	}
}

func TestRecoveryRevocationRace(t *testing.T) {
	for _, endpoint := range []string{"snapshot", "inventory", "upload", "finish"} {
		t.Run(endpoint, func(t *testing.T) {
			r := newRecoveryEnv(t)
			f := r.p.noteFact("private recovered note")
			pushed(t, r.p.push(op{seq: 1, fact: f}))
			other := r.e.phoneOf("alice", "alice-password-1")
			path := "/v1/sync/recovery/" + endpoint
			body := map[string]any{"recovery_id": r.rid, "entries": []any{}}
			switch endpoint {
			case "inventory":
				body["entries"] = []any{map[string]any{"kind": "fact", "op_id": f["op_id"], "content_hash": contentHash(t, f), "position": []int{1, 1, 0}}}
			case "snapshot":
				path = "/v1/snapshot"
				body = map[string]any{"mode": "recovery", "recovery_id": newID(), "install_id": r.p.install, "acked_install_seq": 0}
			case "finish":
				body = map[string]any{"recovery_id": r.rid, "complete": true}
			}
			b, _ := json.Marshal(body)
			res := delayed(r.e, http.MethodPost, path, string(b), r.p.cred, func() { r.e.revoke(other.cred, r.p.install).must(200) })
			if res.status != 401 {
				t.Errorf("revoked credential got %d: %s", res.status, res.raw)
			}
		})
	}
}

// A recovery view's later pages are archived content too: a credential
// revoked after a page request was authenticated gets nothing.
func TestRecoverySnapshotLaterPageRevocationRace(t *testing.T) {
	r := newRecoveryEnv(t)
	for i := int64(1); i <= 3; i++ {
		pushed(t, r.p.push(op{seq: i, fact: r.p.noteFact("private recovered note")}))
	}
	other := r.e.phoneOf("alice", "alice-password-1")
	body := map[string]any{"mode": "recovery", "recovery_id": newID(), "install_id": r.p.install, "acked_install_seq": 0, "limit": 1}
	first := r.e.do(http.MethodPost, "/v1/snapshot", body, r.p.cred).must(200)
	next, _ := first.body["next"].(string)
	if first.body["has_more"] != true || next == "" {
		t.Fatalf("a one-commit page should have more: %s", first.raw)
	}
	body["page"] = next
	r.e.do(http.MethodPost, "/v1/snapshot", body, r.p.cred).must(200)
	b, _ := json.Marshal(body)
	res := delayed(r.e, http.MethodPost, "/v1/snapshot", string(b), r.p.cred, func() { r.e.revoke(other.cred, r.p.install).must(200) })
	if res.status != 401 {
		t.Errorf("revoked credential got a later page: %d: %s", res.status, res.raw)
	}
}

func TestInventoryConflictRecorded(t *testing.T) {
	r := newRecoveryEnv(t)
	f := r.p.noteFact("original")
	pushed(t, r.p.push(op{seq: 1, fact: f}))
	inv := r.e.do("POST", "/v1/sync/recovery/inventory", map[string]any{"recovery_id": r.rid, "entries": []any{map[string]any{"kind": "fact", "op_id": f["op_id"], "content_hash": strings.Repeat("0", 64), "position": []int{1, 1, 0}}}}, r.p.cred).must(200)
	if status := uploadStatus(t, inv); status != "conflict" {
		t.Fatal(status)
	}
	fin := r.finish()
	if len(fin.body["problems"].([]any)) == 0 {
		t.Errorf("inventory conflict lost: %s", fin.raw)
	}
}

func TestCrossKindConflictNonMutating(t *testing.T) {
	for _, incoming := range []string{"fact", "decision"} {
		t.Run(incoming, func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			f, rc := r.fact(1)
			d := r.decision(2)
			if incoming == "decision" {
				d["decision_id"] = f["op_id"]
				r.sign(d, r.account)
				if got := uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": rc})); got != "joined" {
					t.Fatal(got)
				}
			} else {
				d["epoch"] = 1
				r.sign(d, r.account)
				if got := uploadStatus(t, r.upload(map[string]any{"decision": d})); got != "joined" {
					t.Fatal(got)
				}
				f["op_id"] = d["decision_id"]
				rc["op_id"] = f["op_id"]
				rc["content_hash"] = contentHash(t, f)
				rc["epoch"] = 2
				r.sign(rc, r.account)
			}
			before := r.epoch()
			tokens := r.e.count("SELECT count(*) FROM auth_tokens")
			var entry any = map[string]any{"fact": f, "receipt": rc}
			if incoming == "decision" {
				entry = map[string]any{"decision": d}
			}
			if got := uploadStatus(t, r.upload(entry)); got != "conflict" {
				t.Fatal(got)
			}
			if r.epoch() != before || r.e.count("SELECT count(*) FROM auth_tokens") != tokens {
				t.Errorf("conflict mutated epoch %d->%d, tokens %d->%d", before, r.epoch(), tokens, r.e.count("SELECT count(*) FROM auth_tokens"))
			}
		})
	}
}

func TestStagedUploadRevocationRace(t *testing.T) {
	r := newRecoveryEnv(t)
	f := r.p.noteFact("baseline")
	pushed(t, r.p.push(op{seq: 1, fact: f}))
	r.raise()
	other := r.e.phoneOf("alice", "alice-password-1")
	r.e.srv.cfg.Protocol.StagingThreshold = 1
	entered, release := make(chan struct{}), make(chan struct{})
	r.e.srv.cfg.Protocol.Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error { close(entered); <-release; return nil })
	d := r.decision(1)
	done := make(chan *response, 1)
	go func() { done <- r.upload(map[string]any{"decision": d}) }()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("staged admit not reached")
	}
	r.e.revoke(other.cred, r.p.install).must(200)
	close(release)
	res := <-done
	n := r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"])
	if res.status != 401 || n != 0 {
		t.Errorf("staged upload committed after revocation: status=%d decision rows=%d response=%s", res.status, n, res.raw)
	}
}

func TestPartialFrameMAC(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	f, rc := r.fact(1)
	parts := frame(t, f, 160)
	bad := map[string]any{}
	for k, v := range rc {
		bad[k] = v
	}
	bad["mac"] = strings.Repeat("0", 64)
	got := uploadStatus(t, r.upload(map[string]any{"part": parts[0], "receipt": bad}))
	if got != "invalid_receipt" {
		t.Errorf("invalid receipt on first part returned %s", got)
	}
	for _, p := range parts[1:] {
		r.upload(map[string]any{"part": p, "receipt": rc}).must(200)
	}
	fin := r.finish()
	if fin.body["complete"] == true || len(fin.body["problems"].([]any)) == 0 {
		t.Errorf("bad MAC forgotten: %s", fin.raw)
	}
}

func TestSignedContentConflicts(t *testing.T) {
	for _, kind := range []string{"fact", "decision"} {
		t.Run(kind, func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			f, rc := r.fact(1)
			d := r.decision(1)
			var entry any = map[string]any{"fact": f, "receipt": rc}
			if kind == "decision" {
				entry = map[string]any{"decision": d}
			}
			if got := uploadStatus(t, r.upload(entry)); got != "joined" {
				t.Fatal(got)
			}
			query := "SELECT receipt FROM facts WHERE account_id=?"
			if kind == "decision" {
				query = "SELECT decision FROM decisions WHERE account_id=?"
			}
			before := r.e.bytesOf(query, r.account)
			if kind == "fact" {
				f["payload"] = map[string]any{"text": "different"}
				rc["content_hash"] = contentHash(t, f)
				r.sign(rc, r.account)
			} else {
				d["result"] = map[string]any{"different": true}
				r.sign(d, r.account)
			}
			if got := uploadStatus(t, r.upload(entry)); got != "conflict" {
				t.Fatal(got)
			}
			after := r.e.bytesOf(query, r.account)
			if string(before) != string(after) {
				t.Fatal("conflict replaced archived evidence")
			}
			if len(r.finish().body["problems"].([]any)) != 1 {
				t.Fatal("conflict not listed")
			}
		})
	}
}

func TestRecoveryAuthorizationMatrix(t *testing.T) {
	for _, scenario := range []string{"missing", "foreign_account", "foreign_install", "unbound_web", "revoked", "superseded"} {
		t.Run(scenario, func(t *testing.T) {
			r := newRecoveryEnv(t)
			var cred opt = r.p.cred
			switch scenario {
			case "missing":
				cred = func(*http.Request) {}
			case "foreign_account":
				cred = r.e.phoneOf("root", "root-password-1").cred
			case "foreign_install":
				cred = r.e.phoneOf("alice", "alice-password-1").cred
			case "unbound_web":
				cred = browser(r.e.session("alice", "alice-password-1"))
			case "revoked":
				r.e.revoke(r.p.cred, r.p.install).must(200)
			case "superseded":
				r.e.successor(r.p.cred, newID(), r.p.nonce).must(200)
			}
			for _, endpoint := range []string{"snapshot", "inventory", "upload", "finish"} {
				path := "/v1/sync/recovery/" + endpoint
				body := map[string]any{"recovery_id": r.rid, "install_id": r.p.install, "entries": []any{}}
				switch endpoint {
				case "snapshot":
					path = "/v1/snapshot"
					body = map[string]any{"mode": "recovery", "recovery_id": r.rid, "install_id": r.p.install, "acked_install_seq": 0}
				case "finish":
					body = map[string]any{"recovery_id": r.rid, "install_id": r.p.install, "complete": true}
				}
				res := r.e.do("POST", path, body, cred)
				if res.status == 200 {
					t.Errorf("%s %s unexpectedly allowed", scenario, endpoint)
				}
			}
		})
	}
}

func TestRecoveryIDIsolationAndRetry(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	f, rc := r.fact(1)
	entry := map[string]any{"fact": f, "receipt": rc}
	first := r.upload(entry).must(200)
	second := r.upload(entry).must(200)
	if first.str("head") != second.str("head") || uploadStatus(t, second) != "known" {
		t.Error("repeat upload changed the head")
	}
	a := r.finish()
	b := r.finish()
	if string(a.raw) != string(b.raw) {
		t.Error("identical immediate finishes differ")
	}
	same := r.e.phoneOf("alice", "alice-password-1")
	foreign := r.e.phoneOf("root", "root-password-1")
	for _, p := range []*phone{same, foreign} {
		for _, endpoint := range []string{"inventory", "upload", "finish"} {
			body := map[string]any{"recovery_id": r.rid, "entries": []any{}}
			if endpoint == "finish" {
				body = map[string]any{"recovery_id": r.rid, "complete": true}
			}
			res := r.e.do("POST", "/v1/sync/recovery/"+endpoint, body, p.cred)
			if res.status == 200 {
				t.Errorf("foreign recovery ID allowed: %s", endpoint)
			}
		}
	}
}
