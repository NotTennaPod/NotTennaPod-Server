package api

// PROTOCOL §6.11 over HTTP (PROTOCOL §6.11): after a
// restore a device opens a recovery with a recovery snapshot (the prefix
// shortcut for a cursor the restored log still holds), compares its
// inventory, uploads what the server lacks, catches up by pulls and
// finishes; diagnostics list the recovery.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func TestRecoveryOverHTTP(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newEnv(t, withSyncBox(t))
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	alice := e.phoneOf("alice", "alice-password-1")
	first := alice.noteFact("before the backup")
	if got := alice.statuses(alice.push(op{seq: 1, fact: first})); !slices.Equal(got, []string{"accepted"}) {
		t.Fatalf("push: %v", got)
	}
	atBackup := pushed(t, alice.push(op{seq: 1, fact: first})).Head
	backup := e.adminDo(admin, http.MethodPost, "/v1/admin/backup", nil).must(http.StatusOK).str("file")
	alice.cursor = atBackup
	second, third := alice.noteFact("after the backup"), alice.noteFact("and again")
	if got := alice.statuses(alice.push(op{seq: 2, fact: second}, op{seq: 3, fact: third})); !slices.Equal(got, []string{"accepted", "accepted"}) {
		t.Fatalf("push: %v", got)
	}
	// What alice holds after the backup: the commits she pulled.
	commits, _ := alice.pullAll(atBackup, 0)
	var entries []any
	type held struct {
		Accepted []struct {
			Fact    map[string]any `json:"fact"`
			Receipt map[string]any `json:"receipt"`
		} `json:"accepted"`
	}
	inventory := []any{map[string]any{"kind": "fact", "op_id": first["op_id"], "content_hash": contentHash(t, first),
		"position": []any{1, 1, 0}}}
	for _, c := range commits {
		var h held
		if err := json.Unmarshal(c, &h); err != nil {
			t.Fatal(err)
		}
		for _, a := range h.Accepted {
			entries = append(entries, map[string]any{"fact": a.Fact, "receipt": a.Receipt})
			inventory = append(inventory, map[string]any{"kind": "fact", "op_id": a.Receipt["op_id"], "content_hash": a.Receipt["content_hash"],
				"position": []any{a.Receipt["epoch"], a.Receipt["seq"], a.Receipt["index"]}})
		}
	}
	if len(entries) != 2 {
		t.Fatalf("%d entries held", len(entries))
	}

	// The restore.
	bundle := filepath.Join(t.TempDir(), "keys.json")
	pass := []byte("a long passphrase")
	if err := e.store.ExportKeys(ctx, bundle, pass); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(bundle)
	e.store.Close()
	if _, err := storage.Restore(ctx, storage.RestoreOptions{DataDir: e.dir, Backup: filepath.Join(e.dir, "backups", backup), Bundle: data,
		Passphrase: pass}); err != nil {
		t.Fatal(err)
	}
	e.reopen(withSyncBox(t))
	if b := e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK).obj("bounds"); b["recovery_inventory"] != float64(2000) {
		t.Fatalf("bounds %v", b)
	}

	// Alice's phone: a successor installation, a recovery ID, once.
	install, nonce, recoveryID := newID(), newID(), newID()
	cred := bearer(e.token("alice", "alice-password-1", install, nonce))
	snap := e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": recoveryID, "install_id": install,
		"acked_install_seq": 0, "cursor": atBackup}, cred).must(http.StatusOK)
	if snap.body["prefix"] != true {
		t.Fatalf("the cursor the restored log holds is a prefix: %s", snap.raw)
	}
	reissued, err := contract.DecodeCursor(snap.str("cursor"))
	old, _ := contract.DecodeCursor(atBackup)
	if err != nil || reissued.Epoch <= old.Epoch || reissued.Seq != old.Seq {
		t.Fatalf("re-issued cursor %+v (old %+v): %v", reissued, old, err)
	}
	inv := e.do(http.MethodPost, "/v1/sync/recovery/inventory", map[string]any{"recovery_id": recoveryID, "entries": inventory}, cred).must(http.StatusOK)
	want := []string{"have", "need", "need"}
	for i, r := range inv.body["results"].([]any) {
		if got := r.(map[string]any)["status"]; got != want[i] {
			t.Fatalf("inventory %d: %v, want %s (%s)", i, got, want[i], inv.raw)
		}
	}
	up := e.do(http.MethodPost, "/v1/sync/recovery/upload", map[string]any{"recovery_id": recoveryID, "entries": entries}, cred).must(http.StatusOK)
	for _, r := range up.body["results"].([]any) {
		if r.(map[string]any)["status"] != "joined" {
			t.Fatalf("upload: %s", up.raw)
		}
	}
	// Repeating the request is harmless.
	up = e.do(http.MethodPost, "/v1/sync/recovery/upload", map[string]any{"recovery_id": recoveryID, "entries": entries}, cred).must(http.StatusOK)
	for _, r := range up.body["results"].([]any) {
		if r.(map[string]any)["status"] != "known" {
			t.Fatalf("repeated upload: %s", up.raw)
		}
	}
	inv = e.do(http.MethodPost, "/v1/sync/recovery/inventory", map[string]any{"recovery_id": recoveryID, "entries": inventory}, cred).must(http.StatusOK)
	for i, r := range inv.body["results"].([]any) {
		if got := r.(map[string]any)["status"]; got != "have" {
			t.Fatalf("inventory %d after the upload: %v", i, got)
		}
	}
	// Catching up by pulls from the re-issued cursor sees the joining commit
	// with the original receipts.
	p := &phone{e: e, cred: cred, install: install}
	pulled, _ := p.pullAll(snap.str("cursor"), 0)
	if len(pulled) != 1 {
		t.Fatalf("%d commits to catch up", len(pulled))
	}
	var joined held
	json.Unmarshal(pulled[0], &joined)
	if len(joined.Accepted) != 2 || joined.Accepted[0].Receipt["epoch"] != float64(old.Epoch) {
		t.Fatalf("the joining commit: %s", pulled[0])
	}
	// Errors: an unknown recovery, a too large inventory, a malformed entry.
	e.do(http.MethodPost, "/v1/sync/recovery/upload", map[string]any{"recovery_id": newID(), "entries": entries}, cred).
		expect(http.StatusBadRequest, "invalid_request")
	big := make([]any, 2001)
	for i := range big {
		big[i] = inventory[0]
	}
	e.do(http.MethodPost, "/v1/sync/recovery/inventory", map[string]any{"recovery_id": recoveryID, "entries": big}, cred).
		expect(http.StatusRequestEntityTooLarge, "payload_too_large")
	e.do(http.MethodPost, "/v1/sync/recovery/upload", map[string]any{"recovery_id": recoveryID, "entries": []any{map[string]any{"fact": first}}}, cred).
		expect(http.StatusBadRequest, "invalid_request")
	// A tampered receipt is refused, never joined, and listed.
	tampered := map[string]any{}
	for k, v := range entries[0].(map[string]any)["receipt"].(map[string]any) {
		tampered[k] = v
	}
	tampered["outcome"] = "superseded"
	up = e.do(http.MethodPost, "/v1/sync/recovery/upload", map[string]any{"recovery_id": recoveryID,
		"entries": []any{map[string]any{"fact": entries[0].(map[string]any)["fact"], "receipt": tampered}}}, cred).must(http.StatusOK)
	if up.body["results"].([]any)[0].(map[string]any)["status"] != "invalid_receipt" {
		t.Fatalf("tampered: %s", up.raw)
	}
	// A recovery with a recorded invalid receipt is never complete (PROTOCOL §6.11).
	fin := e.do(http.MethodPost, "/v1/sync/recovery/finish", map[string]any{"recovery_id": recoveryID, "complete": true}, cred).must(http.StatusOK)
	if fin.body["complete"] != false || fin.obj("counts")["joined"] != float64(2) || len(fin.body["problems"].([]any)) != 1 {
		t.Fatalf("finish: %s", fin.raw)
	}
	// Diagnostics list the recovery.
	diag := e.do(http.MethodGet, "/v1/diagnostics", nil, browser(e.session("alice", "alice-password-1"))).must(http.StatusOK)
	recs := diag.body["recoveries"].([]any)
	if len(recs) != 1 || recs[0].(map[string]any)["recovery_id"] != recoveryID {
		t.Fatalf("diagnostics recoveries: %v", recs)
	}
}

// PROTOCOL §6.11, §6.13: the inventory accepts the batch it advertises
// (bounds.recovery_inventory entries), which takes far more than the 64 KiB
// other bodies are limited to; one entry more, or a body above
// max_request_bytes, is refused before anything is read into the recovery.
func TestRecoveryInventoryAcceptsAdvertisedBatch(t *testing.T) {
	t.Parallel()
	r := newRecoveryEnv(t)
	hash := func(i int) string { return fmt.Sprintf("%064x", i+1) }
	entries := make([]any, 0, protocol.MaxRecoveryInventory+1)
	for i := range protocol.MaxRecoveryInventory {
		if i%4 == 3 {
			entries = append(entries, map[string]any{"kind": "decision", "decision_id": newID(), "rank": []any{1, 100000 + i}})
			continue
		}
		entries = append(entries, map[string]any{"kind": "fact", "op_id": newID(), "content_hash": hash(i), "position": []any{1, 100000 + i, i % 50}})
	}
	body := map[string]any{"recovery_id": r.rid, "install_id": r.p.install, "entries": entries}
	if raw, _ := json.Marshal(body); len(raw) <= maxBody {
		t.Fatalf("the advertised batch is %d bytes; the test needs more than the default limit", len(raw))
	}
	inv := r.e.do(http.MethodPost, "/v1/sync/recovery/inventory", body, r.p.cred).must(http.StatusOK)
	results := inv.body["results"].([]any)
	if len(results) != protocol.MaxRecoveryInventory {
		t.Fatalf("%d results for %d entries", len(results), protocol.MaxRecoveryInventory)
	}
	for i, res := range results {
		if got := res.(map[string]any)["status"]; got != "need" {
			t.Fatalf("entry %d: %v, want need", i, got)
		}
	}
	// One entry more than advertised.
	body["entries"] = append(entries, map[string]any{"kind": "fact", "op_id": newID(), "content_hash": hash(-1), "position": []any{1, 1, 0}})
	r.e.do(http.MethodPost, "/v1/sync/recovery/inventory", body, r.p.cred).expect(http.StatusRequestEntityTooLarge, "payload_too_large")
	// A body above max_request_bytes.
	small, _ := json.Marshal(map[string]any{"recovery_id": r.rid, "entries": entries[:1]})
	huge := rawBody(strings.Repeat(" ", protocol.MaxRequestBytes) + string(small))
	r.e.do(http.MethodPost, "/v1/sync/recovery/inventory", huge, r.p.cred).expect(http.StatusRequestEntityTooLarge, "payload_too_large")
	// Nothing was recorded: the recovery has no conflicts or problems.
	if fin := r.finish(); fin.body["complete"] != true || len(fin.body["problems"].([]any)) != 0 {
		t.Fatalf("finish: %s", fin.raw)
	}
}
