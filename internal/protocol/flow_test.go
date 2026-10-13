package protocol

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// noteHandler is the test operation type `test.note`.
type noteHandler struct{ capability string }

func (h noteHandler) Capability() string { return h.capability }
func (noteHandler) Validate(f *Fact) error {
	if _, ok := f.Payload["text"].(string); !ok {
		return errors.New("text required")
	}
	return nil
}
func (noteHandler) Apply(ctx *ApplyContext, f *Fact) (Outcome, error) {
	ctx.Record(Record{Entity: "note", ID: f.OpID, FieldGroup: "text", Value: f.Payload["text"]})
	return Outcome{Result: "applied"}, nil
}

type flowEnv struct {
	t       *testing.T
	svc     *Service
	account string
	inst    *storage.Installation
}

func newFlowEnv(t *testing.T) *flowEnv {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	var account string
	inst := &storage.Installation{InstallID: "11111111-1111-4111-8111-111111111111", InstanceNonce: "22222222-2222-4222-8222-222222222222",
		Name: "test", Platform: "ios", AppVersion: "1", Capabilities: `["queue"]`}
	err = store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		if err != nil {
			return err
		}
		account = a.ID
		return tx.Account(a.ID).InsertInstallation(*inst)
	})
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry()
	reg.Register("test.note", 1, noteHandler{})
	reg.Register("test.queue_note", 1, noteHandler{capability: "queue"})
	reg.Register("test.inbox_note", 1, noteHandler{capability: "inbox"})
	views, err := NewViewRegistry(filepath.Join(store.Dir(), "views"), nil)
	if err != nil {
		t.Fatal(err)
	}
	e := &flowEnv{t: t, svc: &Service{Store: store, Registry: reg, Now: time.Now, Views: views}, account: account, inst: inst}
	t.Cleanup(func() { checkRebuild(t, e.svc, e.account) })
	return e
}

func (e *flowEnv) authorize(tx *storage.WriteTx) (*storage.Installation, error) {
	i, err := tx.Account(e.account).Installation(e.inst.InstallID)
	return &i, err
}

func (e *flowEnv) head() contract.Cursor {
	var c contract.Cursor
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(e.account).Head()
		c = contract.Cursor{LogID: h.LogID, Epoch: h.Epoch, Seq: h.Seq, Digest: h.Digest}
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

func fact(op string, seq int64, deps []any, text string) *Archived {
	obj := map[string]any{"op_id": op, "author_id": "11111111-1111-4111-8111-111111111111", "author_seq": seq,
		"dependencies": deps, "type": "test.note", "type_version": int64(1), "client_time": int64(1_700_000_000_000),
		"targets": []any{}, "payload": map[string]any{"text": text}}
	a, err := ParseFact(obj)
	if err != nil {
		panic(err)
	}
	return a
}

func (e *flowEnv) push(acked int64, ops ...PushOp) *PushResponse {
	e.t.Helper()
	resp, err := e.svc.Push(context.Background(), PushRequest{AccountID: e.account, Cursor: e.head(), InstallID: e.inst.InstallID,
		AckedInstallSeq: acked, Operations: ops}, e.authorize)
	if err != nil {
		e.t.Fatal(err)
	}
	return resp
}

func TestPushRetryPullAndSnapshot(t *testing.T) {
	e := newFlowEnv(t)
	a := "aaaaaaaa-0000-4000-8000-000000000001"
	b := "aaaaaaaa-0000-4000-8000-000000000002"
	r := e.push(0, PushOp{InstallSeq: 1, Fact: fact(a, 1, []any{}, "one")}, PushOp{InstallSeq: 2, Fact: fact(b, 2, []any{a}, "two")})
	if len(r.Results) != 2 || r.Results[0].Status != "accepted" || r.Results[1].Status != "accepted" || r.InstallWatermark != 2 {
		t.Fatalf("push: %+v", r)
	}
	// Lost response: the retry replays the original receipts and consumes nothing new.
	r2 := e.push(0, PushOp{InstallSeq: 1, Fact: fact(a, 1, []any{}, "one")}, PushOp{InstallSeq: 2, Fact: fact(b, 2, []any{a}, "two")})
	if r2.Results[0].Status != "duplicate" || r2.InstallWatermark != 2 || r2.Results[0].Receipt["mac"] != r.Results[0].Receipt["mac"] {
		t.Fatalf("retry: %+v", r2)
	}
	// Gap stops without consuming.
	r3 := e.push(2, PushOp{InstallSeq: 4, Fact: fact("aaaaaaaa-0000-4000-8000-000000000004", 4, []any{}, "x")})
	if r3.Results[0].Status != "gap" || r3.InstallWatermark != 2 {
		t.Fatalf("gap: %+v", r3)
	}
	// A second commit.
	c := "aaaaaaaa-0000-4000-8000-000000000003"
	if r4 := e.push(2, PushOp{InstallSeq: 3, Fact: fact(c, 3, []any{b}, "three")}); r4.Results[0].Status != "accepted" {
		t.Fatalf("third: %+v", r4)
	}
	// Pull everything from the genesis cursor and verify the chain.
	genesis := contract.Cursor{LogID: e.head().LogID, Epoch: 1, Seq: 0, Digest: contract.GenesisDigest}
	p, err := e.svc.Pull(context.Background(), PullRequest{AccountID: e.account, Cursor: genesis, InstallID: e.inst.InstallID, AckedInstallSeq: 3, Limit: 1})
	if err != nil || len(p.Items) != 1 || !p.HasMore || p.InstallWatermark != 3 {
		t.Fatalf("pull: %+v %v", p, err)
	}
	again, err := e.svc.Pull(context.Background(), PullRequest{AccountID: e.account, Cursor: genesis, InstallID: e.inst.InstallID, AckedInstallSeq: 3, Page: p.Next})
	if err != nil || again.HasMore || len(again.Items) == 0 {
		t.Fatalf("pull page 2: %+v %v", again, err)
	}
	// Snapshot: both facts in causal order, manifest present.
	sresp, err := e.svc.Snapshot(context.Background(), SnapshotRequest{AccountID: e.account, Mode: "attach", InstallID: e.inst.InstallID})
	if err != nil || sresp.HasMore || sresp.Manifest["item_count"] != int64(3) || len(sresp.Items) != 3 {
		t.Fatalf("snapshot: %+v %v", sresp, err)
	}
}
