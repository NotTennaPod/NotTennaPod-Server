package protocol

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// deepFact returns a test.note fact whose own nesting depth is depth (the fact
// object counts as 1, its payload as 2).
func deepFact(op string, seq int64, depth int) map[string]any {
	inner := map[string]any{"text": "x"}
	for d := 2; d < depth; d++ {
		inner = map[string]any{"text": "x", "n": inner}
	}
	return map[string]any{"op_id": op, "author_id": "11111111-1111-4111-8111-111111111111", "author_seq": seq,
		"dependencies": []any{}, "type": "test.note", "type_version": int64(1), "client_time": int64(1_700_000_000_000),
		"targets": []any{}, "payload": inner}
}

// A fact at the depth bound is archived, and everything that wraps it (commit,
// pull page, snapshot page) still decodes within contract-v1's 64 levels.
func TestFactAtDepthBoundRoundTrips(t *testing.T) {
	e := newFlowEnv(t)
	obj := deepFact("cccccccc-0000-4000-8000-000000000001", 1, MaxFactDepth)
	if d := contract.Depth(obj); d != MaxFactDepth {
		t.Fatalf("test fact depth %d", d)
	}
	a, err := ParseFact(obj)
	if err != nil {
		t.Fatal(err)
	}
	if r := e.push(0, PushOp{InstallSeq: 1, Fact: a}); r.Results[0].Status != "accepted" {
		t.Fatalf("push: %+v", r)
	}
	if err := e.svc.Store.VerifyChain(context.Background(), e.account); err != nil {
		t.Fatal(err)
	}
	genesis := contract.Cursor{LogID: e.head().LogID, Epoch: 1, Seq: 0, Digest: contract.GenesisDigest}
	p, err := e.svc.Pull(context.Background(), PullRequest{AccountID: e.account, Cursor: genesis, InstallID: e.inst.InstallID, AckedInstallSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	body, err := p.Body()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := contract.DecodeJSON(body); err != nil {
		t.Fatalf("pull page does not decode: %v", err)
	}
	s, err := e.svc.Snapshot(context.Background(), SnapshotRequest{AccountID: e.account, Mode: "attach", InstallID: e.inst.InstallID})
	if err != nil {
		t.Fatal(err)
	}
	body, err = s.Body()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := contract.DecodeJSON(body); err != nil {
		t.Fatalf("snapshot page does not decode: %v", err)
	}
}

// A framed fact deeper than the bound is refused without consuming its slot.
func TestFramedFactBeyondDepthBoundIsNotConsumed(t *testing.T) {
	e := newFlowEnv(t)
	obj := deepFact("cccccccc-0000-4000-8000-000000000002", 1, MaxFactDepth+1)
	canonical, err := contract.Canonical(obj)
	if err != nil {
		t.Fatal(err)
	}
	hash, _ := contract.ContentHash(obj)
	r := e.push(0, PushOp{InstallSeq: 1, Part: &Part{TransactionID: "dddddddd-0000-4000-8000-000000000001", Index: 0, Count: 1,
		ContentHash: hash, Data: canonical}})
	if r.Results[0].Status != "invalid_part" || r.InstallWatermark != 0 {
		t.Fatalf("deep framed fact: %+v", r)
	}
}

// The aggregate size of a framed fact is bounded; going over consumes nothing.
func TestFramedFactBeyondAggregateBoundIsNotConsumed(t *testing.T) {
	e := newFlowEnv(t)
	big := strings.Repeat("x", MaxPartBytes-64)
	tx := "dddddddd-0000-4000-8000-000000000002"
	hash := strings.Repeat("ab", 32)
	count := MaxFramedFact/(MaxPartBytes-64) + 2
	if count > MaxPartCount {
		count = MaxPartCount
	}
	var last *PushResponse
	for i := 0; i < count; i++ {
		last = e.push(0, PushOp{InstallSeq: 1, Part: &Part{TransactionID: tx, Index: i, Count: count, ContentHash: hash, Data: []byte(big)}})
		if last.Results[0].Status == "invalid_part" {
			break
		}
	}
	if last.Results[0].Status != "invalid_part" || last.InstallWatermark != 0 {
		t.Fatalf("over the aggregate bound: %+v", last.Results[0])
	}
}

// The most secrets a push carries (500 of about 8 KB, 4 MB) are canonicalized
// and sealed before its write transaction, which only stores them: another
// writer is not held meanwhile (the condition-based form of the API test's timing).
func TestPushPreparesSecretsBeforeItsTransaction(t *testing.T) {
	h := newHarness(t)
	box, err := sealed.New(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	h.svc.Box = box
	in := h.addInstall()
	secrets := make([]Secret, MaxOperations)
	for i := range secrets {
		secrets[i] = Secret{ID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i), Username: strings.Repeat("u", 4096),
			Password: strings.Repeat("p", 3800)}
	}
	prepared := false
	h.svc.secretsPrepared = func() {
		prepared = true
		done := make(chan error, 1)
		go func() { done <- h.store.Write(h.ctx, func(*storage.WriteTx) error { return nil }) }()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Minute): // a held writer never completes: this only ends the wait
			t.Error("another writer waited while the push's secrets were prepared")
		}
	}
	if _, err := h.svc.Push(h.ctx, PushRequest{AccountID: h.account, Cursor: h.head(), InstallID: in, Secrets: secrets},
		authorizer(h.account, in)); err != nil {
		t.Fatal(err)
	}
	if !prepared {
		t.Fatal("the push prepared no secrets")
	}
	if err := h.store.Write(h.ctx, func(tx *storage.WriteTx) error {
		_, err := tx.Account(h.account).Secret(secrets[MaxOperations-1].ID)
		return err
	}); err != nil {
		t.Errorf("the last secret was not stored: %v", err)
	}
}
