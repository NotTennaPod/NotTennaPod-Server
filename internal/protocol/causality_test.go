package protocol

// Causality through rejections (PROTOCOL §6.1, §6.9). Rejected facts are not causal
// nodes, but what they observed is still observed by their descendants.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func TestCausalityThroughRejections(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	base := h.clock.millis()
	author := func() string { return randomID() }
	set := func(op string, deps []string, ct int64, key, value string) map[string]any {
		return fs{op: op, author: author(), seq: 1, deps: deps, typ: "test.set", ct: ct, payload: map[string]any{"key": key, "text": value}}.object()
	}
	invalid := func(op string, deps []string, ct int64) map[string]any {
		return fs{op: op, author: author(), seq: 1, deps: deps, typ: "test.set", ct: ct, payload: map[string]any{"wrong": true}}.object()
	}
	seq := int64(0)
	push := func(f map[string]any, outcome string) map[string]any {
		t.Helper()
		seq++
		r := h.push(in, seq-1, fop(seq, f))
		h.expect(r, seq, want{seq: seq, status: "accepted", outcome: outcome})
		h.clock.Advance(time.Second)
		return r.Results[0].Receipt
	}
	nt := func(r map[string]any) int64 { return num(r["normalized_time"]) }
	frontier := func(r map[string]any) []byte { return canon(r["frontier"]) }

	// 1. Admitted A; R observes A and is rejected for its payload; B observes
	// only R, with a client clock behind A. B's op_id sorts before A's, so only
	// the causal edge through R can place B after A.
	a, r1, b := uid(0xe, 1), uid(0x5, 1), uid(0x1, 1)
	ra := push(set(a, nil, base-50_000, "x", "A"), "applied")
	rr := push(invalid(r1, []string{a}, base-40_000), "rejected")
	if !bytes.Equal(frontier(rr), canon([]any{a})) {
		t.Errorf("R's frontier %s, want [A]", frontier(rr))
	}
	if nt(rr) != base+1000 {
		t.Errorf("R's normalized time %d, want its receive time %d", nt(rr), base+1000)
	}
	rb := push(set(b, []string{r1}, base-90_000, "x", "B"), "applied")
	if nt(rb) < nt(ra) {
		t.Errorf("B's normalized time %d is below A's %d", nt(rb), nt(ra))
	}

	// 2. A chain of two rejected facts: R2's frontier is resolved through R1.
	a2, r21, r22, b2 := uid(0xe, 2), uid(0x5, 2), uid(0x6, 2), uid(0x1, 2)
	ra2 := push(set(a2, nil, base-30_000, "y", "A2"), "applied")
	rr21 := push(invalid(r21, []string{a2}, base-20_000), "rejected")
	rr22 := push(invalid(r22, []string{r21}, base-20_000), "rejected")
	if !bytes.Equal(frontier(rr21), canon([]any{a2})) || !bytes.Equal(frontier(rr22), canon([]any{a2})) {
		t.Errorf("chain frontiers %s and %s, want [A2] both", frontier(rr21), frontier(rr22))
	}
	rb2 := push(set(b2, []string{r22}, base-95_000, "y", "B2"), "applied")
	if nt(rb2) < nt(ra2) {
		t.Errorf("B2's normalized time %d is below A2's %d", nt(rb2), nt(ra2))
	}

	// 3. An envelope-rejected fact contributes an empty frontier, while other
	// branches are preserved: B3 observes D and E (which named C).
	c, d, e, b3 := uid(0xe, 3), uid(0xe, 4), uid(0x5, 3), uid(0x1, 3)
	rc := push(set(c, nil, base-10_000, "z", "C"), "applied")
	rd := push(set(d, nil, base-80_000, "z", "D"), "applied")
	bad := set(e, []string{c}, base-9_000, "z", "E")
	bad["type_version"] = 0
	re := push(bad, "rejected")
	if !bytes.Equal(frontier(re), canon([]any{})) {
		t.Errorf("envelope-rejected frontier %s, want []", frontier(re))
	}
	rb3 := push(set(b3, []string{d, e}, base-45_000, "z", "B3"), "applied")
	if nt(rb3) != base-45_000 {
		t.Errorf("B3's normalized time %d: C (named only by the envelope-rejected E) bounded it, or D did not (want %d)", nt(rb3), base-45_000)
	}
	if nt(rb3) < nt(rd) || nt(rb3) >= nt(rc) {
		t.Errorf("B3 %d, D %d, C %d", nt(rb3), nt(rd), nt(rc))
	}

	// 4. Fact → decision → rejected fact → fact: A4 makes decision D4; R4
	// observes D4 and is rejected; B4 observes only R4 with an old clock.
	a4, r4, b4 := uid(0xe, 5), uid(0x5, 5), uid(0x1, 5)
	push(fs{op: a4, author: author(), seq: 1, typ: "test.decide", ct: base - 70_000, payload: text("subject-" + randomID())}.object(), "applied")
	ds := h.lastCommit()["decisions"].([]any)
	if len(ds) != 1 {
		t.Fatalf("%d decisions", len(ds))
	}
	d4 := ds[0].(map[string]any)
	d4id := d4["decision_id"].(string)
	rr4 := push(invalid(r4, []string{d4id}, base-60_000), "rejected")
	if !bytes.Equal(frontier(rr4), canon([]any{d4id})) {
		t.Errorf("R4's frontier %s, want [D4]", frontier(rr4))
	}
	rb4 := push(set(b4, []string{r4}, base-99_000, "w", "B4"), "applied")
	if nt(rb4) < num(d4["normalized_time"]) {
		t.Errorf("B4's normalized time %d is below the decision's %v", nt(rb4), d4["normalized_time"])
	}

	// Every replica derives the same order from the wire data, and B's effect
	// is applied after A's.
	items := h.verifySnapshot(in)
	before := func(x, y string) {
		t.Helper()
		if i, j := indexOf(items, x), indexOf(items, y); i < 0 || j < 0 || i >= j {
			t.Errorf("%s (at %d) is not ordered before %s (at %d)", x, i, y, j)
		}
	}
	before(a, b)
	before(a2, b2)
	before(d, b3)
	before(b3, c)
	before(a4, d4id)
	before(d4id, b4)
	// Rejected facts come after the causal sequence.
	for _, id := range []string{r1, r21, r22, e, r4} {
		before(b4, id)
	}
	values := map[string]string{}
	for _, it := range items {
		if it.kind == "accepted" && it.receipt["outcome"] == "applied" && it.fact["type"] == "test.set" {
			p := it.fact["payload"].(map[string]any)
			values[p["key"].(string)] = p["text"].(string)
		}
	}
	if values["x"] != "B" || values["y"] != "B2" {
		t.Errorf("applying in snapshot order leaves x=%q y=%q, want B's effects to win", values["x"], values["y"])
	}
	// The same holds for a replica built from pulls.
	p, err := h.pullAll(h.account, in, h.genesis(), seq, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.commits) != int(seq) {
		t.Errorf("pulled %d commits, want %d", len(p.commits), seq)
	}
	h.verifyLog(h.account)
}

// decisionAfterHandler creates a decision depending on the fact itself, with the
// server clock behind the fact's normalized time.
type decisionAfterHandler struct{}

func (decisionAfterHandler) Capability() string   { return "" }
func (decisionAfterHandler) Validate(*Fact) error { return nil }
func (decisionAfterHandler) Apply(ctx *ApplyContext, f *Fact) (Outcome, error) {
	_, err := ctx.Decide(DecisionInput{Type: "test.decision", Version: 1, Subject: f.OpID, Dependencies: []string{f.OpID}})
	return Outcome{Result: "applied"}, err
}

// A decision made while the server clock is behind its dependency takes the
// dependency's normalized time (PROTOCOL §6.1: max(server_now, dependencies)).
func TestDecisionTimeNeverPrecedesItsDependencies(t *testing.T) {
	e := newFlowEnv(t)
	e.svc.Registry.Register("test.decide", 1, decisionAfterHandler{})
	future := time.Now().Add(time.Hour).UnixMilli()
	e.svc.Now = func() time.Time { return time.UnixMilli(future) }
	obj := map[string]any{"op_id": "bbbbbbbb-0000-4000-8000-000000000001", "author_id": e.inst.InstallID, "author_seq": int64(1),
		"dependencies": []any{}, "type": "test.decide", "type_version": int64(1), "client_time": future,
		"targets": []any{}, "payload": map[string]any{}}
	a, err := ParseFact(obj)
	if err != nil {
		t.Fatal(err)
	}
	r := e.push(0, PushOp{InstallSeq: 1, Fact: a})
	if r.Results[0].Status != "accepted" {
		t.Fatalf("push: %+v", r)
	}
	// Now the clock moves back by a day: a second fact depending on the first.
	e.svc.Now = func() time.Time { return time.UnixMilli(future - 24*3600*1000) }
	obj2 := map[string]any{"op_id": "bbbbbbbb-0000-4000-8000-000000000002", "author_id": e.inst.InstallID, "author_seq": int64(2),
		"dependencies": []any{"bbbbbbbb-0000-4000-8000-000000000001"}, "type": "test.decide", "type_version": int64(1),
		"client_time": future - 24*3600*1000, "targets": []any{}, "payload": map[string]any{}}
	b, err := ParseFact(obj2)
	if err != nil {
		t.Fatal(err)
	}
	if r := e.push(1, PushOp{InstallSeq: 2, Fact: b}); r.Results[0].Status != "accepted" {
		t.Fatalf("push 2: %+v", r)
	}
	err = e.svc.Store.Read(context.Background(), func(rt *storage.ReadTx) error {
		nodes, err := rt.Account(e.account).Nodes()
		if err != nil {
			return err
		}
		for _, n := range nodes {
			if n.Kind == "decision" && n.NormalizedTime < future {
				t.Errorf("decision %s has normalized time %d before its dependency %d", n.ID, n.NormalizedTime, future)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
