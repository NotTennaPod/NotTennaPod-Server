package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Abandon one partial recovery frame, then finish: scratch state must not live forever.
func TestAbandonedRecoveryFramesAreCleanedUp(t *testing.T) {
	r := newRecoveryEnv(t)
	f := r.p.noteFact("genuine signed content")
	reply := pushed(t, r.p.push(op{seq: 1, fact: f}))
	var receipt map[string]any
	if err := json.Unmarshal(reply.Results[0].Receipt, &receipt); err != nil {
		t.Fatal(err)
	}
	// env.do marshals float64 integers as plain integer tokens.
	const n = 24
	for i := 0; i < n; i++ {
		part := map[string]any{"transaction_id": newID(), "part_index": 0, "part_count": 64, "content_hash": contentHash(t, f), "data": base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", protocol.MaxPartBytes)))}
		if got := uploadStatus(t, r.upload(map[string]any{"part": part, "receipt": receipt})); got != "part_stored" {
			t.Fatalf("part %d: %s", i, got)
		}
	}
	r.finish()
	if err := r.e.store.Close(); err != nil {
		t.Fatal(err)
	}
	r.e.reopen(withSyncBox(t))
	nrows := r.e.count("SELECT COUNT(*) FROM push_parts WHERE account_id=?", r.account)
	size := r.e.count("SELECT COALESCE(SUM(length(data)),0) FROM push_parts WHERE account_id=?", r.account)
	t.Logf("after finish and restart: retained parts=%d bytes=%d", nrows, size)
	if nrows == n {
		t.Errorf("all %d unrelated frames retained without a session/frame quota or finish cleanup", n)
	}
}

func TestEncryptedFrameAtAdvertisedLimit(t *testing.T) {
	r := newRecoveryEnv(t)
	f := r.p.noteFact("")
	// The empty-string fact's canonical length is its JSON marshaled length for this ASCII fixture.
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	f["payload"] = map[string]any{"text": strings.Repeat("a", protocol.MaxFramedFact-len(b))}
	b, err = json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != protocol.MaxFramedFact {
		t.Fatal(len(b))
	}
	parts := frame(t, f, protocol.MaxPartBytes)
	for i, part := range parts {
		res := pushed(t, r.p.push(op{seq: 1, part: part}))
		want := "part_stored"
		if i == len(parts)-1 {
			want = "accepted"
		}
		if res.Results[0].Status != want {
			t.Fatalf("valid %d-byte fact, part %d/%d: got %s want %s", len(b), i+1, len(parts), res.Results[0].Status, want)
		}
	}
}

type testInboxNote struct{ syncNote }

func (testInboxNote) Capability() string { return "inbox" }

// Everything signed in this scenario is produced by the actual push/decision
// paths. Two supported restores of an empty backup expose missing evidence in
// the server's full snapshot, rather than a forged or modified receipt.
func TestSnapshotPreservesProvisionalNodeEvidence(t *testing.T) {
	r := newRecoveryEnv(t)
	ctx := context.Background()
	configure := func() {
		r.e.srv.cfg.Protocol.Registry.Register("test.note", 1, testInboxNote{})
		r.e.srv.cfg.Protocol.Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error { return nil })
	}
	configure()
	backup, err := r.e.store.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	kp := filepath.Join(t.TempDir(), "keys.json")
	pass := []byte("test bundle passphrase")
	if err := r.e.store.ExportKeys(ctx, kp, pass); err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile(kp)
	if err != nil {
		t.Fatal(err)
	}
	restore := func() {
		if err := r.e.store.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := storage.Restore(ctx, storage.RestoreOptions{DataDir: r.e.dir, Backup: backup.Path, Bundle: bundle, Passphrase: pass}); err != nil {
			t.Fatal(err)
		}
		r.e.reopen(withSyncBox(t))
		configure()
		r.p.token = r.e.token("alice", "alice-password-1", r.p.install, r.p.nonce)
		r.p.cred = bearer(r.p.token)
		r.p.cursor = r.p.attach(0).head
		r.p.acked = 0
		r.open()
	}
	f := r.p.noteFact("same immutable fact")
	first := pushed(t, r.p.push(op{seq: 1, fact: f}))
	var original map[string]any
	if err := json.Unmarshal(first.Results[0].Receipt, &original); err != nil {
		t.Fatal(err)
	}
	if original["outcome"] != "rejected" {
		t.Fatal(original)
	}
	restore()
	r.e.do("PUT", "/v1/installations/self", map[string]any{"capabilities": []string{"inbox"}}, r.p.cred).must(200)
	provisional := pushed(t, r.p.push(op{seq: 1, fact: f}))
	var later map[string]any
	_ = json.Unmarshal(provisional.Results[0].Receipt, &later)
	if later["outcome"] != "applied" {
		t.Fatal(later)
	}
	var decision map[string]any
	err = r.e.srv.cfg.Protocol.ServerCommit(ctx, r.account, func(c *protocol.ApplyContext) error {
		var err error
		decision, err = c.Decide(protocol.DecisionInput{Type: "test.decision", Version: 1, Subject: "dependent", Dependencies: []string{f["op_id"].(string)}, Inputs: map[string]any{}, Result: map[string]any{}})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": original})); got != "replaced" {
		t.Fatal(got)
	}
	snap := r.e.do("POST", "/v1/snapshot", map[string]any{"mode": "attach", "install_id": r.p.install, "acked_install_seq": 0}, r.p.cred).must(200)
	if snap.body["has_more"] != false {
		t.Fatal("unexpected paging")
	}
	var entries []any
	for _, x := range snap.body["items"].([]any) {
		m := x.(map[string]any)
		if a, ok := m["accepted"]; ok {
			entries = append(entries, a)
		}
		if d, ok := m["decision"]; ok {
			entries = append(entries, map[string]any{"decision": d})
		}
	}
	restore()
	out := r.upload(entries...).must(200)
	for _, x := range out.body["results"].([]any) {
		m := x.(map[string]any)
		if m["id"] == decision["decision_id"] && m["status"] != "joined" {
			t.Errorf("server's complete snapshot cannot recover signed dependent decision: %v", m)
		}
	}
	if n := r.e.count("SELECT COUNT(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, decision["decision_id"]); n != 1 {
		t.Errorf("decision lost; snapshot includes %d entries, missing superseded node evidence", len(entries))
	}
	// The same signed set in original-first order: the later provisional
	// receipt is kept, so the decision joins (it was dropped as known).
	restore()
	if got := uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": original})); got != "joined" {
		t.Fatal(got)
	}
	got := uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": later}))
	retry := r.upload(map[string]any{"decision": decision}).must(200)
	if status := uploadStatus(t, retry); status != "joined" {
		t.Errorf("complete genuine receipt set in original-first order: later receipt=%s decision=%s", got, status)
	}
	// Control: the same signed set in provisional-first order does recover.
	restore()
	if got := uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": later})); got != "joined" {
		t.Fatal(got)
	}
	r.upload(map[string]any{"fact": f, "receipt": original}, map[string]any{"decision": decision}).must(200)
	if n := r.e.count("SELECT COUNT(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, decision["decision_id"]); n != 1 {
		t.Fatal("control failed: provisional-first order")
	}
	t.Log("control passed: identical genuine signed set recovers in provisional-first order")
}

// An installation's open recovery frames stay within
// MaxRecoveryFrames (the least recently used are discarded), and frames
// unused for a day expire. Recovery slots are found by the full transaction
// ID.
func TestRecoveryFramesBoundedAndExpire(t *testing.T) {
	r := newRecoveryEnv(t)
	f := r.p.noteFact("genuine signed content")
	reply := pushed(t, r.p.push(op{seq: 1, fact: f}))
	var receipt map[string]any
	if err := json.Unmarshal(reply.Results[0].Receipt, &receipt); err != nil {
		t.Fatal(err)
	}
	open := func() (int, int) {
		return r.e.count("SELECT COUNT(*) FROM recovery_frames WHERE account_id=?", r.account),
			r.e.count("SELECT COUNT(*) FROM push_parts WHERE account_id=? AND install_seq < 0", r.account)
	}
	partial := func(tx string) string {
		part := map[string]any{"transaction_id": tx, "part_index": 0, "part_count": 64, "content_hash": contentHash(t, f),
			"data": base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("x", protocol.MaxPartBytes)))}
		return uploadStatus(t, r.upload(map[string]any{"part": part, "receipt": receipt}))
	}
	first := newID()
	for i := 0; i < protocol.MaxRecoveryFrames+8; i++ {
		tx := newID()
		if i == 0 {
			tx = first
		}
		if got := partial(tx); got != "part_stored" {
			t.Fatalf("part %d: %s", i, got)
		}
		if frames, parts := open(); frames > protocol.MaxRecoveryFrames || parts != frames {
			t.Fatalf("after %d frames: %d open frames, %d stored parts", i+1, frames, parts)
		}
	}
	if n := r.e.count("SELECT COUNT(*) FROM recovery_frames WHERE account_id=? AND transaction_id=?", r.account, first); n != 0 {
		t.Error("the least recently used frame was kept past the bound")
	}
	r.e.clock.Advance(protocol.RecoveryFrameLifetime + time.Minute)
	if got := partial(newID()); got != "part_stored" {
		t.Fatal(got)
	}
	if frames, parts := open(); frames != 1 || parts != 1 {
		t.Errorf("frames unused for a day did not expire: %d frames, %d parts", frames, parts)
	}
	r.finish()
	if frames, parts := open(); frames != 0 || parts != 0 {
		t.Errorf("finish left %d frames and %d parts", frames, parts)
	}
}
