package api

import (
	"context"
	"fmt"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRepeatedReplacementPastFrontier(t *testing.T) {
	r := newRecoveryEnv(t)
	for i := 0; i < 3; i++ {
		r.raise()
	}
	f, rc := r.fact(3)
	upload := func(epoch int, rejected bool) {
		rc["epoch"] = epoch
		rc["outcome"] = "applied"
		delete(rc, "frontier")
		delete(rc, "reason")
		if rejected {
			rc["outcome"] = "rejected"
			rc["reason"] = "capability_not_declared"
			rc["frontier"] = []any{}
		}
		r.sign(rc, r.account)
		st := uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": rc}))
		if st != "joined" && st != "replaced" {
			t.Fatal(st)
		}
	}
	upload(3, false)
	h, err := r.e.store.Head(context.Background(), r.account)
	if err != nil {
		t.Fatal(err)
	}
	frontier := func() []string {
		var out []string
		err := r.e.store.Read(context.Background(), func(tx *storage.ReadTx) error {
			var e error
			out, e = tx.Account(r.account).FrontierAt(h.Epoch, h.Seq)
			return e
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := frontier()
	upload(2, true)
	middle := frontier()
	upload(1, false)
	after := frontier()
	t.Logf("head=%+v before=%v middle=%v after=%v", h, before, middle, after)
	if !slices.Equal(before, after) {
		t.Errorf("already-published node disappeared from past frontier after applied/rejected/applied replacements")
	}
}

func TestProblemsAfterFinish(t *testing.T) {
	for _, kind := range []string{"invalid_receipt", "inventory_conflict", "upload_conflict"} {
		t.Run(kind, func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			f, rc := r.fact(1)
			uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": rc}))
			if r.finish().body["complete"] != true {
				t.Fatal("initial finish incomplete")
			}
			switch kind {
			case "invalid_receipt":
				rc["mac"] = strings.Repeat("0", 64)
				uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": rc}))
			case "upload_conflict":
				f["payload"] = map[string]any{"text": "changed"}
				rc["content_hash"] = contentHash(t, f)
				r.sign(rc, r.account)
				uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": rc}))
			case "inventory_conflict":
				uploadStatus(t, r.e.do("POST", "/v1/sync/recovery/inventory", map[string]any{"recovery_id": r.rid, "entries": []any{map[string]any{"kind": "decision", "decision_id": f["op_id"], "rank": []int{1, 1}}}}, r.p.cred))
			}
			if complete := r.e.count("SELECT complete FROM recoveries WHERE account_id=? AND recovery_id=?", r.account, r.rid); complete != 0 {
				t.Fatal("late problem did not clear completed flag")
			}
			if r.finish().body["complete"] != false {
				t.Fatal("finish overwrote late problem")
			}
		})
	}
}

func TestFrameRequestsAndDependencies(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprint(staged), func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			if staged {
				r.e.srv.cfg.Protocol.StagingThreshold = 1
				seed, rc := r.fact(1)
				uploadStatus(t, r.upload(map[string]any{"fact": seed, "receipt": rc}))
			}
			parent, pr := r.fact(1)
			child, cr := r.fact(1)
			child["dependencies"] = []any{parent["op_id"]}
			cr["content_hash"] = contentHash(t, child)
			r.sign(cr, r.account)
			parts := frame(t, child, 80)
			for i := 0; i < len(parts)-1; i++ {
				if st := uploadStatus(t, r.upload(map[string]any{"part": parts[i], "receipt": cr})); st != "part_stored" {
					t.Fatal(st)
				}
			}
			last := map[string]any{"part": parts[len(parts)-1], "receipt": cr}
			if st := uploadStatus(t, r.upload(last)); st != "dependency_missing" {
				t.Fatal(st)
			}
			uploadStatus(t, r.upload(map[string]any{"fact": parent, "receipt": pr}))
			if st := uploadStatus(t, r.upload(last)); st != "joined" {
				t.Fatal(st)
			}
			if n := r.e.count("SELECT count(*) FROM push_parts WHERE account_id=? AND install_seq<0", r.account); n != 0 {
				t.Errorf("%d completed parts left", n)
			}
		})
	}
}

func TestStagedHeadMoves(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	f, rc := r.fact(1)
	uploadStatus(t, r.upload(map[string]any{"fact": f, "receipt": rc}))
	r.e.srv.cfg.Protocol.StagingThreshold = 1
	calls := 0
	r.e.srv.cfg.Protocol.Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error {
		calls++
		if calls != 1 {
			return nil
		}
		return r.e.store.Write(context.Background(), func(tx *storage.WriteTx) error {
			_, _, err := tx.Account(r.account).AppendCommit(storage.CommitBody{})
			return err
		})
	})
	d := r.decision(1)
	if st := uploadStatus(t, r.upload(map[string]any{"decision": d})); st != "joined" {
		t.Fatal(st)
	}
	if calls < 2 {
		t.Fatalf("head moved without restaging: %d admits", calls)
	}
	if n := r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"]); n != 1 {
		t.Fatal(n)
	}
	t.Logf("admitted %d times across head retry; one publication", calls)
}

func TestDuplicateFinalPartWithinSizeBound(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	f, rc := r.fact(1)
	f["payload"] = map[string]any{"text": strings.Repeat("x", protocol.MaxFramedFact-1024)}
	rc["content_hash"] = contentHash(t, f)
	r.sign(rc, r.account)
	parts := frame(t, f, protocol.MaxPartBytes)
	for start := 0; start < len(parts)-1; start += 6 {
		var batch []any
		for i := start; i < min(start+6, len(parts)-1); i++ {
			batch = append(batch, map[string]any{"part": parts[i], "receipt": rc})
		}
		r.upload(batch...).must(200)
	}
	last := map[string]any{"part": parts[len(parts)-1], "receipt": rc}
	res := r.upload(last, last).must(200)
	joined := false
	for _, v := range res.body["results"].([]any) {
		if v.(map[string]any)["status"] == "joined" {
			joined = true
		}
	}
	if !joined {
		t.Errorf("valid frame below 8MiB rejected when final part repeated: %s", res.raw)
		// The rejection discarded the previously stored assembly. Replay the
		// identical frame without duplication as a size/receipt validity control.
		for start := 0; start < len(parts); start += 6 {
			var batch []any
			for i := start; i < min(start+6, len(parts)); i++ {
				batch = append(batch, map[string]any{"part": parts[i], "receipt": rc})
			}
			res = r.upload(batch...).must(200)
		}
		results := res.body["results"].([]any)
		st := results[len(results)-1].(map[string]any)["status"]
		t.Logf("identical frame without duplicate final part: %s", st)
		if st != "joined" {
			t.Fatal("size/receipt validity control failed")
		}
	}
}

func TestEpochEvidenceFallsBackToLiveWriter(t *testing.T) {
	r := newRecoveryEnv(t)
	pushed(t, r.p.push(op{seq: 1, fact: r.p.noteFact("baseline")}))
	r.e.srv.cfg.Protocol.StagingThreshold = 1
	var blocked []bool
	r.e.srv.cfg.Protocol.Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := r.e.store.Write(ctx, func(*storage.WriteTx) error { return nil })
		blocked = append(blocked, err != nil)
		return nil
	})
	d := r.decision(1)
	if st := uploadStatus(t, r.upload(map[string]any{"decision": d})); st != "joined" {
		t.Fatal(st)
	}
	t.Logf("live writer unavailable during each admission: %v", blocked)
	if slices.Contains(blocked, true) {
		t.Error("a staging-eligible epoch-evidence upload reran its rebuild while holding the live global writer")
	}
}

func TestBadReceiptsOnEveryFramePosition(t *testing.T) {
	for _, position := range []string{"first", "middle", "last", "other_content"} {
		t.Run(position, func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			f, rc := r.fact(1)
			parts := frame(t, f, 80)
			index := 0
			switch position {
			case "middle":
				index = len(parts) / 2
			case "last":
				index = len(parts) - 1
			}
			bad := map[string]any{}
			for k, v := range rc {
				bad[k] = v
			}
			bad["mac"] = strings.Repeat("0", 64)
			if position == "other_content" {
				_, bad = r.fact(1)
			}
			for i, p := range parts {
				receipt := rc
				if i == index {
					receipt = bad
				}
				st := uploadStatus(t, r.upload(map[string]any{"part": p, "receipt": receipt}))
				if i == index && st != "invalid_receipt" {
					t.Fatalf("bad part %d: %s", i, st)
				}
			}
			if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=?", r.account); n != 0 {
				t.Fatal("assembled a frame through an invalid receipt")
			}
			fin := r.finish()
			if fin.body["complete"] != false || len(fin.body["problems"].([]any)) != 1 {
				t.Fatalf("lost receipt failure: %s", fin.raw)
			}
		})
	}
}
