package protocol

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// storage.OpenStaging (PROTOCOL §11.5 import staging): decisions and receipts
// made in staging carry MACs under the main server's injected key, and the
// staging directory gets no restore metadata or key files of its own.
func TestStagingSignsWithTheInjectedKey(t *testing.T) {
	main := newHarness(t)
	keys, _ := main.store.ReceiptKeys(main.ctx)
	serverID, _ := main.store.ServerID(main.ctx)
	dir := filepath.Join(t.TempDir(), "staging")
	st, err := storage.OpenStaging(main.ctx, dir, serverID, keys)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if id, _ := st.ServerID(main.ctx); id != serverID {
		t.Fatalf("staging server ID %s, want %s", id, serverID)
	}
	h := &harness{t: t, ctx: main.ctx, dir: dir, store: st, clock: newTestClock(), vclock: newTestClock(), reg: acceptanceRegistry(), author: randomID()}
	h.account = h.newAccount()
	h.startService()
	in := h.addInstall()
	r := h.push(in, 0, fop(1, fs{op: randomID(), author: h.author, seq: 1, ct: 1, typ: "test.decide", payload: text("staged")}.object()))
	receipt, ok := r.Results[0].Receipt, r.Results[0].Status == "accepted"
	if !ok || receipt["key_id"] != keys[0].ID || !contract.VerifyMAC(keys[0].Secret, h.account, decoded(receipt).(map[string]any)) {
		t.Fatalf("staged receipt %v does not verify under the main server's key", receipt)
	}
	var decision []byte
	if err := h.db().QueryRow(`SELECT decision FROM decisions WHERE account_id = ?`, h.account).Scan(&decision); err != nil {
		t.Fatal(err)
	}
	obj, _ := contract.DecodeJSON(decision)
	if !contract.VerifyMAC(keys[0].Secret, h.account, obj.(map[string]any)) || !bytes.Contains(decision, []byte(`"key_id":"`+keys[0].ID+`"`)) {
		t.Fatal("the staged decision does not verify under the main server's key")
	}
	for _, name := range []string{"restore-state.json", "receipt.keys", "secrets.key"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("staging made %s", name)
		}
	}
}
