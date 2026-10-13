package protocol

// Sequencing of single installations (PROTOCOL §6.1, §6.4). After each case the tests check the
// exact responses, the consumed watermark, the slot entries of the commit (content_hash and
// result), that the whole log verifies and that a later snapshot containing the rejected facts
// orders and verifies.

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

func receiptBytes(r PushResult) []byte { return canon(r.Receipt) }

func TestSequencingInOrder(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall("queue")
	a, b, c := uid(1, 1), uid(1, 2), uid(1, 3)
	fa, fb, fc := h.note(a, 1, ""), h.note(b, 2, a, a), h.note(c, 3, b, a, b)
	r := h.push(in, 0, fop(1, fa), fop(2, fb), fop(3, fc))
	h.expect(r, 3, want{seq: 1, status: "accepted", outcome: "applied"}, want{seq: 2, status: "accepted", outcome: "applied"},
		want{seq: 3, status: "accepted", outcome: "applied"})
	for i, f := range []map[string]any{fa, fb, fc} {
		rc := r.Results[i].Receipt
		if rc["op_id"] != f["op_id"] || rc["content_hash"] != hashOf(f) || num(rc["epoch"]) != 1 || num(rc["seq"]) != 1 ||
			num(rc["normalized_time"]) != num(f["client_time"]) {
			t.Errorf("receipt %d: %v", i, rc)
		}
	}
	if head, _ := h.head().Encode(); r.Head != head {
		t.Errorf("push head %q is not the head cursor %q", r.Head, head)
	}
	h.expectLastCommit(1, []string{a, b, c}, slotWant{in, 1, a, hashOf(fa), "accepted"}, slotWant{in, 2, b, hashOf(fb), "accepted"},
		slotWant{in, 3, c, hashOf(fc), "accepted"})
	if recs := h.lastCommit()["records"].([]any); len(recs) != 3 {
		t.Errorf("commit has %d records, want one per applied note", len(recs))
	}
	if h.consumed(in) != 3 {
		t.Errorf("consumed_seq %d", h.consumed(in))
	}
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func TestSequencingRetryOfEveryPrefixReplaysOriginalReceipts(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	var facts []map[string]any
	var ops []PushOp
	prev := ""
	for i := 1; i <= 4; i++ {
		op := uid(1, i)
		f := h.note(op, int64(i), prev)
		facts, ops, prev = append(facts, f), append(ops, fop(int64(i), f)), op
	}
	original := map[int64][]byte{}
	for _, r := range []*PushResponse{h.push(in, 0, ops[0]), h.push(in, 1, ops[1], ops[2]), h.push(in, 3, ops[3])} {
		for _, res := range r.Results {
			if res.Status != "accepted" {
				t.Fatalf("first push: %+v", res)
			}
			original[res.InstallSeq] = wireJSON(t, res.Receipt)
		}
	}
	head := h.head()
	// Every response was lost: the client resends each prefix of its outbox.
	for k := 1; k <= 4; k++ {
		var wants []want
		for i := 1; i <= k; i++ {
			wants = append(wants, want{seq: int64(i), status: "duplicate", outcome: "applied"})
		}
		r := h.push(in, 0, ops[:k]...)
		h.expect(r, 4, wants...)
		for _, res := range r.Results {
			if got := wireJSON(t, res.Receipt); !bytes.Equal(got, original[res.InstallSeq]) {
				t.Errorf("prefix %d slot %d: receipt %s, want the original %s", k, res.InstallSeq, got, original[res.InstallSeq])
			}
		}
		if now := h.head(); now.Seq != head.Seq || !bytes.Equal(now.Digest, head.Digest) {
			t.Fatalf("a retry committed: head %d", now.Seq)
		}
	}
	// A retry that runs into new work consumes only the new slot.
	f5 := h.note(uid(1, 5), 5, uid(1, 4))
	r := h.push(in, 2, ops[2], ops[3], fop(5, f5))
	h.expect(r, 5, want{seq: 3, status: "duplicate", outcome: "applied"}, want{seq: 4, status: "duplicate", outcome: "applied"},
		want{seq: 5, status: "accepted", outcome: "applied"})
	h.expectLastCommit(head.Seq+1, []string{uid(1, 5)}, slotWant{in, 5, uid(1, 5), hashOf(f5), "accepted"})
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func TestSequencingDuplicateUnderFreshInstallationConsumesTheNewSlot(t *testing.T) {
	h := newHarness(t)
	original, copied := h.addInstall(), h.addInstall()
	a, b := uid(1, 1), uid(1, 2)
	fa, fb := h.note(a, 1, ""), h.note(b, 2, a, a)
	r := h.push(original, 0, fop(1, fa), fop(2, fb))
	ra, rb := receiptBytes(r.Results[0]), receiptBytes(r.Results[1])

	// A copied library resends b under its fresh transport installation.
	r = h.push(copied, 0, fop(1, fb))
	h.expect(r, 1, want{seq: 1, status: "duplicate", outcome: "applied"})
	if !bytes.Equal(receiptBytes(r.Results[0]), rb) {
		t.Errorf("fresh-installation duplicate returned %s, want the original receipt %s", receiptBytes(r.Results[0]), rb)
	}
	h.expectLastCommit(2, nil, slotWant{copied, 1, b, hashOf(fb), "duplicate"})
	c := h.lastCommit()
	if len(c["records"].([]any)) != 0 || len(c["decisions"].([]any)) != 0 {
		t.Errorf("a duplicate repeated effects: %s", canon(c))
	}
	// Its response was lost: the same slot replays without another commit.
	r = h.push(copied, 0, fop(1, fb))
	h.expect(r, 1, want{seq: 1, status: "duplicate", outcome: "applied"})
	if !bytes.Equal(receiptBytes(r.Results[0]), rb) || h.head().Seq != 2 {
		t.Error("replaying the fresh installation's slot changed something")
	}
	// The copy continues: another copied fact, then its own new fact.
	cc := uid(1, 3)
	fc := fs{op: cc, author: randomID(), seq: 1, deps: []string{b}, ct: h.clock.millis() - 1000}.object()
	r = h.push(copied, 1, fop(2, fa), fop(3, fc))
	h.expect(r, 3, want{seq: 2, status: "duplicate", outcome: "applied"}, want{seq: 3, status: "accepted", outcome: "applied"})
	if !bytes.Equal(receiptBytes(r.Results[0]), ra) {
		t.Error("second copied fact did not return its original receipt")
	}
	h.expectLastCommit(3, []string{cc}, slotWant{copied, 2, a, hashOf(fa), "duplicate"}, slotWant{copied, 3, cc, hashOf(fc), "accepted"})
	if h.consumed(original) != 2 || h.consumed(copied) != 3 {
		t.Errorf("watermarks %d and %d", h.consumed(original), h.consumed(copied))
	}
	h.verifyLog(h.account)
	h.verifySnapshot(copied)
}

func TestSequencingGapConsumesNothing(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	f := func(i int) map[string]any { return h.note(uid(1, i), int64(i), "") }
	r := h.push(in, 0, fop(2, f(2)), fop(3, f(3)))
	h.expect(r, 0, want{seq: 2, status: "gap"})
	if h.head().Seq != 0 || h.count(`SELECT COUNT(*) FROM facts`) != 0 {
		t.Fatal("a gap consumed or archived something")
	}
	h.expect(h.push(in, 0, fop(1, f(1)), fop(2, f(2))), 2, want{seq: 1, status: "accepted", outcome: "applied"},
		want{seq: 2, status: "accepted", outcome: "applied"})
	h.expect(h.push(in, 2, fop(4, f(4))), 2, want{seq: 4, status: "gap"})
	// A part beyond the next slot is not stored either.
	big := fs{op: uid(1, 4), author: h.author, seq: 4, ct: 1, payload: text(strings.Repeat("p", 300))}.object()
	parts := partOps(4, big, 200, randomID())
	h.expect(h.push(in, 2, parts[1]), 2, want{seq: 4, status: "gap"})
	if n := h.count(`SELECT COUNT(*) FROM push_parts`); n != 0 {
		t.Errorf("a part at a gap was stored (%d)", n)
	}
	r = h.push(in, 2, fop(3, f(3)), parts[0])
	h.expect(r, 3, want{seq: 3, status: "accepted", outcome: "applied"}, want{seq: 4, status: "part_stored", parts: []int{0}})
	h.verifyLog(h.account)
}

func TestSequencingConflictingCounter(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	a, b, c, d := uid(1, 1), uid(1, 2), uid(1, 3), uid(1, 4)
	fa, fb := h.note(a, 1, ""), h.note(b, 2, a)
	fc := h.note(c, 2, a)
	fd := fs{op: d, author: randomID(), seq: 1, ct: 1}.object() // independent: admissible on its own
	h.push(in, 0, fop(1, fa), fop(2, fb))
	// A different operation at a consumed counter: installation_conflict, stop.
	r := h.push(in, 2, fop(2, fc), fop(3, fd))
	h.expect(r, 2, want{seq: 2, status: "installation_conflict"})
	// The same op_id with different content at a consumed counter.
	fa2 := maps.Clone(fa)
	fa2["payload"] = text("changed")
	h.expect(h.push(in, 2, fop(1, fa2)), 2, want{seq: 1, status: "installation_conflict"})
	// Replays before the conflict still answer; nothing after it is processed.
	r = h.push(in, 0, fop(1, fa), fop(2, fc), fop(3, fd))
	h.expect(r, 2, want{seq: 1, status: "duplicate", outcome: "applied"}, want{seq: 2, status: "installation_conflict"})
	if h.head().Seq != 1 || h.count(`SELECT COUNT(*) FROM facts WHERE op_id IN (?, ?)`, c, d) != 0 {
		t.Error("a conflicting counter consumed or archived something")
	}
	h.verifyLog(h.account)
}

// Scenario 42 ("a copied transport counter ... none can silently drop unrelated
// facts"): an undetected copy of an installation diverges after slot 2. Its
// push of a different slot 2 followed by a slot 3 must stop at the conflict
// ("Different content → installation_conflict and stop"), otherwise the copy
// takes the original's next counter and the original's own slot 3 is refused
// forever.
func TestSequencingCopiedCounterCannotTakeTheOriginalsNextSlot(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	a, b := uid(1, 1), uid(1, 2)
	h.push(in, 0, fop(1, h.note(a, 1, "")), fop(2, h.note(b, 2, a)))
	copy2 := fs{op: uid(2, 2), author: randomID(), seq: 1, ct: 1}.object()
	copy3 := fs{op: uid(2, 3), author: randomID(), seq: 1, ct: 2}.object() // admissible on its own
	r := h.push(in, 1, fop(2, copy2), fop(3, copy3))
	if len(r.Results) != 1 || r.Results[0].Status != "installation_conflict" || r.InstallWatermark != 2 {
		t.Errorf("the copy's push answered %s; want only installation_conflict with nothing consumed", wireJSON(t, r))
	}
	if n := h.count(`SELECT COUNT(*) FROM facts WHERE op_id = ?`, uid(2, 3)); n != 0 {
		t.Error("the copy's slot 3 was consumed after its installation_conflict")
	}
	// The original device continues with its own slot 3.
	own3 := h.note(uid(1, 3), 3, b)
	r = h.push(in, 2, fop(3, own3))
	if len(r.Results) != 1 || r.Results[0].Status != "accepted" {
		t.Errorf("the original's own slot 3 answered %s: the copy took its transport counter", wireJSON(t, r))
	}
	h.verifyLog(h.account)
}

func TestSequencingOpIDConflictIsConsumedWithoutReceiptAndReplays(t *testing.T) {
	h := newHarness(t)
	in, other := h.addInstall(), h.addInstall()
	a, b := uid(1, 1), uid(1, 2)
	fa := h.note(a, 1, "")
	ra := receiptBytes(h.push(in, 0, fop(1, fa)).Results[0])
	fa2 := fs{op: a, author: randomID(), seq: 7, ct: 5, payload: text("another fact reusing the op_id")}.object()
	fb := h.note(b, 2, a, a)
	r := h.push(in, 1, fop(2, fa2), fop(3, fb))
	h.expect(r, 3, want{seq: 2, status: "op_id_conflict"}, want{seq: 3, status: "accepted", outcome: "applied"})
	h.expectLastCommit(2, []string{b}, slotWant{in, 2, a, hashOf(fa2), "op_id_conflict"}, slotWant{in, 3, b, hashOf(fb), "accepted"})
	if f, rc := h.archived(a); !bytes.Equal(f, canon(fa)) || !bytes.Equal(rc, ra) {
		t.Error("the original fact or its receipt changed after an op_id conflict")
	}
	rb := receiptBytes(r.Results[1])
	// A lost response: the retry replays op_id_conflict exactly.
	r = h.push(in, 1, fop(2, fa2), fop(3, fb))
	h.expect(r, 3, want{seq: 2, status: "op_id_conflict"}, want{seq: 3, status: "duplicate", outcome: "applied"})
	if !bytes.Equal(receiptBytes(r.Results[1]), rb) || h.head().Seq != 2 {
		t.Error("replay of an op_id_conflict slot changed something")
	}
	// The slot consumed the conflicting content: the original content there is a
	// different operation.
	h.expect(h.push(in, 3, fop(2, fa)), 3, want{seq: 2, status: "installation_conflict"})
	// Another installation's conflicting fact is consumed the same way.
	r = h.push(other, 0, fop(1, fa2))
	h.expect(r, 1, want{seq: 1, status: "op_id_conflict"})
	h.expectLastCommit(3, nil, slotWant{other, 1, a, hashOf(fa2), "op_id_conflict"})
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func TestSequencingVersionUnsupportedThenSupportedAfterUpgrade(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	a, b, c := uid(1, 1), uid(1, 2), uid(1, 3)
	ct := h.clock.millis() - 1000
	fl := fs{op: a, author: h.author, seq: 1, typ: "test.later", ct: ct}.object()
	fb := fs{op: b, author: h.author, seq: 2, prev: a, deps: []string{a}, ct: ct}.object()
	fv := fs{op: c, author: h.author, seq: 3, prev: b, ver: 2, ct: ct}.object()
	outbox := []PushOp{fop(1, fl), fop(2, fb), fop(3, fv)}
	// Unknown type: nothing consumed, later slots wait (PROTOCOL §10, scenario 26).
	h.expect(h.push(in, 0, outbox...), 0, want{seq: 1, status: "version_unsupported"})
	if h.head().Seq != 0 || h.count(`SELECT COUNT(*) FROM facts`) != 0 {
		t.Fatal("version_unsupported consumed or archived something")
	}
	// The server is upgraded with the type: the same slots go through.
	h.reg.Register("test.later", 1, acNote{})
	r := h.push(in, 0, outbox...)
	h.expect(r, 2, want{seq: 1, status: "accepted", outcome: "applied"}, want{seq: 2, status: "accepted", outcome: "applied"},
		want{seq: 3, status: "version_unsupported"})
	// Known type, unknown version; then that version too.
	h.expect(h.push(in, 2, outbox[2]), 2, want{seq: 3, status: "version_unsupported"})
	h.reg.Register("test.note", 2, acNote{})
	h.expect(h.push(in, 2, outbox[2]), 3, want{seq: 3, status: "accepted", outcome: "applied"})
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func TestSequencingDependencyMissingThenSupplied(t *testing.T) {
	h := newHarness(t)
	in, helper := h.addInstall(), h.addInstall()
	x, y, p, b, c := uid(5, 1), uid(3, 1), uid(4, 1), uid(1, 2), uid(1, 3)
	ct := h.clock.millis() - 50_000
	fb := fs{op: b, author: h.author, seq: 2, prev: p, deps: []string{x, y}, ct: ct}.object()
	fc := h.note(c, 3, b, b)
	r := h.push(in, 0, fop(1, fb), fop(2, fc))
	h.expect(r, 0, want{seq: 1, status: "dependency_missing", missing: []string{y, p, x}})
	if h.head().Seq != 0 {
		t.Fatal("dependency_missing consumed something")
	}
	other := randomID()
	fx := fs{op: x, author: other, seq: 1, ct: ct + 10_000}.object()
	fp := fs{op: p, author: h.author, seq: 1, ct: ct}.object()
	h.push(helper, 0, fop(1, fx), fop(2, fp))
	h.expect(h.push(in, 0, fop(1, fb), fop(2, fc)), 0, want{seq: 1, status: "dependency_missing", missing: []string{y}})
	fy := fs{op: y, author: other, seq: 2, prev: x, ct: ct + 20_000}.object()
	h.push(helper, 2, fop(3, fy))
	r = h.push(in, 0, fop(1, fb), fop(2, fc))
	h.expect(r, 2, want{seq: 1, status: "accepted", outcome: "applied"}, want{seq: 2, status: "accepted", outcome: "applied"})
	if nt := num(r.Results[0].Receipt["normalized_time"]); nt < ct+20_000 {
		t.Errorf("b's normalized time %d is below its dependency's %d", nt, ct+20_000)
	}
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

// Every kind of envelope violation is archived with a rejected receipt and
// consumes its slot, before anything that could defer it; its frontier is empty because its
// dependencies are untrusted.
func TestSequencingEnvelopeRejectionsAreConsumedAndNeverDeferred(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	admitted := uid(0xa, 1)
	h.push(in, 0, fop(1, fs{op: admitted, author: randomID(), seq: 1, ct: 1}.object()))
	unknown := uid(0xb, 99)
	cases := []struct {
		name string
		edit func(op string, f map[string]any)
	}{
		{"type_version 0 of an unknown type", func(_ string, f map[string]any) { f["type"], f["type_version"] = "test.unknown", 0 }},
		{"type_version 0", func(_ string, f map[string]any) { f["type_version"] = 0 }},
		{"negative type_version", func(_ string, f map[string]any) { f["type_version"] = -3 }},
		{"self-dependency", func(op string, f map[string]any) { f["dependencies"] = sortedList(admitted, op) }},
		{"self-dependency beside a missing one", func(op string, f map[string]any) { f["dependencies"] = sortedList(unknown, op) }},
		{"missing dependency and type_version 0", func(_ string, f map[string]any) {
			f["dependencies"], f["type_version"] = sortedList(unknown), 0
		}},
		{"author_seq 0", func(_ string, f map[string]any) { f["author_seq"] = 0 }},
		{"author_seq as a string", func(_ string, f map[string]any) { f["author_seq"] = "1" }},
		{"missing author_seq", func(_ string, f map[string]any) { delete(f, "author_seq") }},
		{"previous_op_id null", func(_ string, f map[string]any) { f["previous_op_id"] = nil }},
		{"previous_op_id equal to op_id", func(op string, f map[string]any) { f["previous_op_id"] = op }},
		{"previous_op_id not a UUID", func(_ string, f map[string]any) { f["previous_op_id"] = "previous" }},
		{"unsorted dependencies", func(_ string, f map[string]any) { f["dependencies"] = []any{unknown, admitted} }},
		{"duplicate dependencies", func(_ string, f map[string]any) { f["dependencies"] = []any{admitted, admitted} }},
		{"dependency not a UUID", func(_ string, f map[string]any) { f["dependencies"] = []any{"dep"} }},
		{"uppercase dependency", func(_ string, f map[string]any) { f["dependencies"] = []any{strings.ToUpper(admitted)} }},
		{"dependencies not an array", func(_ string, f map[string]any) { f["dependencies"] = admitted }},
		{"unsorted targets", func(_ string, f map[string]any) {
			f["targets"] = []any{map[string]any{"entity": "b", "id": "1"}, map[string]any{"entity": "a", "id": "1"}}
		}},
		{"duplicate targets", func(_ string, f map[string]any) {
			f["targets"] = []any{map[string]any{"entity": "a", "id": "1"}, map[string]any{"entity": "a", "id": "1"}}
		}},
		{"target with an unknown member", func(_ string, f map[string]any) {
			f["targets"] = []any{map[string]any{"entity": "a", "id": "1", "x": "y"}}
		}},
		{"missing payload", func(_ string, f map[string]any) { delete(f, "payload") }},
		{"payload not an object", func(_ string, f map[string]any) { f["payload"] = []any{} }},
		{"unknown member", func(_ string, f map[string]any) { f["extra"] = 1 }},
		{"type not a string", func(_ string, f map[string]any) { f["type"] = 5 }},
		{"client_time not an integer", func(_ string, f map[string]any) { f["client_time"] = "now" }},
		{"author_id not a UUID", func(_ string, f map[string]any) { f["author_id"] = "someone" }},
	}
	next := int64(2)
	for i, tc := range cases {
		op, good := uid(0xc, i), uid(0xd, i)
		bad := fs{op: op, author: randomID(), seq: 1, deps: []string{admitted}, ct: 7}.object()
		tc.edit(op, bad)
		fg := fs{op: good, author: randomID(), seq: 1, ct: 8}.object()
		receive := h.clock.millis()
		r := h.push(in, next-1, fop(next, bad), fop(next+1, fg))
		h.expect(r, next+1, want{seq: next, status: "accepted", outcome: "rejected"}, want{seq: next + 1, status: "accepted", outcome: "applied"})
		if len(r.Results) == 2 {
			rc := r.Results[0].Receipt
			if reason, _ := rc["reason"].(string); reason == "" {
				t.Errorf("%s: rejected without a reason", tc.name)
			}
			if fr, ok := rc["frontier"].([]any); !ok || len(fr) != 0 {
				t.Errorf("%s: envelope rejection frontier %v, want []", tc.name, rc["frontier"])
			}
			if num(rc["normalized_time"]) != receive {
				t.Errorf("%s: rejected normalized_time %v, want the receive time %d", tc.name, rc["normalized_time"], receive)
			}
			h.expectLastCommit(next/2+1, []string{op, good}, slotWant{in, next, op, hashOf(bad), "accepted"},
				slotWant{in, next + 1, good, hashOf(fg), "accepted"})
			if f, _ := h.archived(op); !bytes.Equal(f, canon(bad)) {
				t.Errorf("%s: the archive does not keep the original bytes", tc.name)
			}
			// Retrying the rejected slot replays its receipt.
			again := h.push(in, next+1, fop(next, bad))
			h.expect(again, next+1, want{seq: next, status: "duplicate", outcome: "rejected"})
			if !bytes.Equal(receiptBytes(again.Results[0]), receiptBytes(r.Results[0])) {
				t.Errorf("%s: replay changed the rejected receipt", tc.name)
			}
		}
		next += 2
		h.clock.Advance(time.Second)
	}
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func sortedList(ids ...string) []any {
	s := slices.Clone(ids)
	slices.Sort(s)
	out := make([]any, len(s))
	for i, x := range s {
		out[i] = x
	}
	return out
}

func TestSequencingDependencyOnRejectedFactIsSatisfiedThroughItsFrontier(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	a, rj, b, c := uid(1, 1), uid(1, 2), uid(1, 3), uid(1, 4)
	ct := h.clock.millis() - 30_000
	fa := fs{op: a, author: h.author, seq: 1, ct: ct}.object()
	frj := fs{op: rj, author: h.author, seq: 2, prev: a, deps: []string{a}, ct: ct + 1, payload: map[string]any{"bad": true}}.object()
	fb := fs{op: b, author: h.author, seq: 3, prev: rj, deps: []string{rj}, ct: ct - 5_000}.object()
	r := h.push(in, 0, fop(1, fa), fop(2, frj), fop(3, fb))
	h.expect(r, 3, want{seq: 1, status: "accepted", outcome: "applied"}, want{seq: 2, status: "accepted", outcome: "rejected"},
		want{seq: 3, status: "accepted", outcome: "applied"})
	if fr := r.Results[1].Receipt["frontier"]; !bytes.Equal(canon(fr), canon([]any{a})) {
		t.Errorf("rejected fact's frontier %v, want [a]", fr)
	}
	if num(r.Results[2].Receipt["normalized_time"]) < num(r.Results[0].Receipt["normalized_time"]) {
		t.Error("a fact observing a rejected fact that observed A is normalized before A")
	}
	// Separate pushes behave the same (the frontier is read from the archive).
	fc := fs{op: c, author: h.author, seq: 4, prev: b, deps: []string{rj}, ct: ct - 9_000}.object()
	r = h.push(in, 3, fop(4, fc))
	h.expect(r, 4, want{seq: 4, status: "accepted", outcome: "applied"})
	items := h.verifySnapshot(in)
	if indexOf(items, a) > indexOf(items, c) || indexOf(items, a) > indexOf(items, b) {
		t.Error("facts observing A through a rejected fact are ordered before A")
	}
	h.verifyLog(h.account)
}

func TestSequencingAuthorHistoryRejections(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	u, v, w := randomID(), randomID(), randomID()
	o := func(i int) string { return uid(1, i) }
	ct := h.clock.millis() - 10_000
	steps := []struct {
		f       map[string]any
		outcome string
	}{
		{fs{op: o(1), author: u, seq: 1, ct: ct}.object(), "applied"},
		{fs{op: o(2), author: u, seq: 1, deps: []string{o(1)}, ct: ct}.object(), "applied"},             // (u, 1) is held: a derived branch (PROTOCOL §6.10)
		{fs{op: o(3), author: v, seq: 1, ct: ct}.object(), "applied"},                                   //
		{fs{op: o(4), author: u, seq: 2, prev: o(3), ct: ct}.object(), "rejected"},                      // previous is another author's
		{fs{op: o(5), author: u, seq: 5, prev: o(1), ct: ct}.object(), "applied"},                       //
		{fs{op: o(6), author: u, seq: 4, prev: o(5), ct: ct}.object(), "rejected"},                      // previous has a higher author_seq
		{fs{op: o(7), author: u, seq: 2, prev: o(1), ct: ct}.object(), "applied"},                       // (u, 2) is held by rejected o(4): derived
		{fs{op: o(8), author: w, seq: 1, ct: ct, payload: map[string]any{"x": 1}}.object(), "rejected"}, // invalid payload, still holds (w, 1)
		{fs{op: o(9), author: w, seq: 1, ct: ct}.object(), "applied"},                                   // behind a rejected fact: a derived branch
		{fs{op: o(10), author: u, seq: 2, prev: o(2), ct: ct}.object(), "applied"},                      // follows its branch through previous_op_id
	}
	var ops []PushOp
	var wants []want
	for i, s := range steps {
		ops = append(ops, fop(int64(i+1), s.f))
		wants = append(wants, want{seq: int64(i + 1), status: "accepted", outcome: s.outcome})
	}
	r := h.push(in, 0, ops...)
	h.expect(r, int64(len(steps)), wants...)
	// Effective authors (PROTOCOL §6.10): the first fact at a position holds it; a later
	// one roots a derived branch, which its successors inherit.
	effective := func(op string) string {
		var a string
		h.db().QueryRow(`SELECT effective_author FROM fact_authors WHERE op_id = ?`, op).Scan(&a)
		return a
	}
	var namespace string
	h.db().QueryRow(`SELECT namespace FROM accounts WHERE account_id = ?`, h.account).Scan(&namespace)
	derived := func(op string) string { return contract.DerivedAuthorID(namespace, op) }
	for _, c := range []struct{ op, want string }{
		{o(1), u}, {o(2), derived(o(2))}, {o(4), u}, {o(5), u}, {o(7), derived(o(7))}, {o(8), w}, {o(9), derived(o(9))}, {o(10), derived(o(2))},
	} {
		if got := effective(c.op); got != c.want {
			t.Errorf("effective author of %s = %q, want %q", c.op, got, c.want)
		}
	}
	var parent string
	h.db().QueryRow(`SELECT parent FROM author_lineage WHERE author = ?`, derived(o(2))).Scan(&parent)
	if parent != u {
		t.Errorf("lineage of the derived branch = %q, want %q", parent, u)
	}
	// Receipts carry their position in the commit, in admission order.
	for i, res := range r.Results {
		if idx, _ := integer(res.Receipt["index"]); idx != int64(i) {
			t.Errorf("receipt %d has index %d", i, idx)
		}
	}
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func TestSequencingCapabilityOutsideTheCurrentDeclarationIsRejected(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall("queue")
	typed := func(i int, typ string) map[string]any {
		return fs{op: uid(1, i), author: h.author, seq: int64(i), typ: typ, ct: 1}.object()
	}
	r := h.push(in, 0, fop(1, typed(1, "test.queue_note")), fop(2, typed(2, "test.inbox_note")), fop(3, typed(3, "test.note")))
	h.expect(r, 3, want{seq: 1, status: "accepted", outcome: "applied"}, want{seq: 2, status: "accepted", outcome: "rejected"},
		want{seq: 3, status: "accepted", outcome: "applied"})
	// Admission uses the installation's current declaration.
	h.setCapabilities(in, "inbox")
	r = h.push(in, 3, fop(4, typed(4, "test.queue_note")), fop(5, typed(5, "test.inbox_note")))
	h.expect(r, 5, want{seq: 4, status: "accepted", outcome: "rejected"}, want{seq: 5, status: "accepted", outcome: "applied"})
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func bigNote(h *harness, op string, seq int64, size int) map[string]any {
	return fs{op: op, author: h.author, seq: seq, ct: 1, payload: text(strings.Repeat("x", size))}.object()
}

func TestSequencingPartsInEveryOrderWithResends(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	perms := [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	for k, perm := range perms {
		seq := int64(k + 1)
		op := uid(2, k)
		f := bigNote(h, op, seq, 1000+k)
		size := (len(canon(f)) + 2) / 3
		ops := partOps(seq, f, size, randomID())
		if len(ops) != 3 {
			t.Fatalf("framing produced %d parts", len(ops))
		}
		headBefore := h.head().Seq
		var present []int
		for j, idx := range perm {
			if j < 2 {
				present = append(present, idx)
				slices.Sort(present)
				h.expect(h.push(in, seq-1, ops[idx]), seq-1, want{seq: seq, status: "part_stored", parts: slices.Clone(present)})
				// Resends of any stored part are idempotent.
				h.expect(h.push(in, seq-1, ops[perm[0]]), seq-1, want{seq: seq, status: "part_stored", parts: slices.Clone(present)})
				if h.head().Seq != headBefore {
					t.Fatalf("permutation %v: a part consumed the slot early", perm)
				}
				continue
			}
			r := h.push(in, seq-1, ops[idx])
			h.expect(r, seq, want{seq: seq, status: "accepted", outcome: "applied"})
		}
		h.expectLastCommit(headBefore+1, []string{op}, slotWant{in, seq, op, hashOf(f), "accepted"})
		if fb, _ := h.archived(op); !bytes.Equal(fb, canon(f)) {
			t.Errorf("permutation %v: reassembled fact differs", perm)
		}
	}
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

func TestSequencingLostResponseToTheCompletingPartReplays(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	f := bigNote(h, uid(2, 1), 1, 700)
	ops := partOps(1, f, (len(canon(f))+1)/2, randomID())
	h.expect(h.push(in, 0, ops[0]), 0, want{seq: 1, status: "part_stored", parts: []int{0}})
	r := h.push(in, 0, ops[1])
	h.expect(r, 1, want{seq: 1, status: "accepted", outcome: "applied"})
	original := receiptBytes(r.Results[0])
	for name, op := range map[string]PushOp{"completing part": ops[1], "first part": ops[0], "whole fact": fop(1, f)} {
		r := h.push(in, 0, op)
		h.expect(r, 1, want{seq: 1, status: "duplicate", outcome: "applied"})
		if !bytes.Equal(receiptBytes(r.Results[0]), original) {
			t.Errorf("resending the %s did not replay the original receipt", name)
		}
	}
	if h.head().Seq != 1 {
		t.Fatal("a replayed part committed again")
	}
	// A part of other content at the consumed slot is a conflicting counter.
	g := bigNote(h, uid(2, 2), 1, 701)
	h.expect(h.push(in, 1, partOps(1, g, 400, randomID())[0]), 1, want{seq: 1, status: "installation_conflict"})
	// Replay and new work in one request.
	n := h.note(uid(2, 3), 2, "")
	h.expect(h.push(in, 0, ops[1], fop(2, n)), 2, want{seq: 1, status: "duplicate", outcome: "applied"},
		want{seq: 2, status: "accepted", outcome: "applied"})
	h.verifyLog(h.account)
}

func TestSequencingBadPartsAndReframing(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	f := bigNote(h, uid(2, 1), 1, 900)
	half := (len(canon(f)) + 1) / 2
	// A conflicting resend of a stored part is invalid_part.
	t1 := partObjects(f, half, randomID())
	h.expect(h.push(in, 0, wirePart(1, t1[0])), 0, want{seq: 1, status: "part_stored", parts: []int{0}})
	alt := maps.Clone(t1[0])
	alt["data"] = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte("y"), half))
	h.expect(h.push(in, 0, wirePart(1, alt)), 0, want{seq: 1, status: "invalid_part"})
	// A part of another transaction, whatever its content, is another library
	// writing this slot (PROTOCOL §6.10): installation_conflict, the assembly untouched.
	third := (len(canon(f)) + 2) / 3
	t3 := partObjects(f, third, randomID())
	h.expect(h.push(in, 0, wirePart(1, t3[1])), 0, want{seq: 1, status: "installation_conflict"})
	h.expect(h.push(in, 0, wirePart(1, t1[1])), 1, want{seq: 1, status: "accepted", outcome: "applied"})
	h.expectLastCommit(1, []string{uid(2, 1)}, slotWant{in, 1, uid(2, 1), hashOf(f), "accepted"})
	// A frame whose bytes do not hash to its content_hash is invalid_part when
	// complete; the invalid assembly is discarded, so the library can reframe.
	in2 := h.addInstall()
	f2 := bigNote(h, uid(2, 5), 1, 900)
	other := bigNote(h, uid(2, 9), 1, 10)
	t2 := partObjects(f2, half, randomID())
	for _, p := range t2 {
		p["content_hash"] = hashOf(other)
	}
	h.expect(h.push(in2, 0, wirePart(1, t2[0])), 0, want{seq: 1, status: "part_stored", parts: []int{0}})
	h.expect(h.push(in2, 0, wirePart(1, t2[1])), 0, want{seq: 1, status: "invalid_part"})
	if n := h.count(`SELECT COUNT(*) FROM push_parts WHERE install_id = ?`, in2); n != 0 {
		t.Fatalf("an invalid complete assembly kept %d parts", n)
	}
	t4 := partObjects(f2, third, randomID())
	h.expect(h.push(in2, 0, wirePart(1, t4[1])), 0, want{seq: 1, status: "part_stored", parts: []int{1}})
	h.expect(h.push(in2, 0, wirePart(1, t4[0])), 0, want{seq: 1, status: "part_stored", parts: []int{0, 1}})
	h.expect(h.push(in2, 0, wirePart(1, t4[2])), 1, want{seq: 1, status: "accepted", outcome: "applied"})
	// Bytes that are not the canonical encoding (here: indented) are invalid
	// even when they hash to the declared content_hash.
	g := bigNote(h, uid(2, 2), 2, 300)
	loose := []byte(strings.Replace(string(canon(g)), ",", ", ", -1))
	sum := sha256.Sum256(loose)
	tx := randomID()
	mk := func(i int, data []byte) map[string]any {
		return map[string]any{"transaction_id": tx, "part_index": int64(i), "part_count": int64(2), "content_hash": hex.EncodeToString(sum[:]),
			"data": base64.RawURLEncoding.EncodeToString(data)}
	}
	h.expect(h.push(in, 1, wirePart(2, mk(0, loose[:100]))), 1, want{seq: 2, status: "part_stored", parts: []int{0}})
	h.expect(h.push(in, 1, wirePart(2, mk(1, loose[100:]))), 1, want{seq: 2, status: "invalid_part"})
	ops := partOps(2, g, 200, randomID())
	for i, op := range ops {
		if i < len(ops)-1 {
			h.push(in, 1, op)
		} else {
			h.expect(h.push(in, 1, op), 2, want{seq: 2, status: "accepted", outcome: "applied"})
		}
	}
	// A framed fact with an envelope violation is rejected (consumed), not invalid.
	bad := bigNote(h, uid(2, 3), 3, 500)
	bad["type_version"] = 0
	ops = partOps(3, bad, (len(canon(bad))+1)/2, randomID())
	h.expect(h.push(in, 2, ops[0]), 2, want{seq: 3, status: "part_stored", parts: []int{0}})
	h.expect(h.push(in, 2, ops[1]), 3, want{seq: 3, status: "accepted", outcome: "rejected"})
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

// Op IDs and decision IDs share one ID space (PROTOCOL §6.1): a fact reusing a
// decision's ID is consumed, never admitted, and the decision is unchanged.
func TestSequencingFactReusingADecisionIDIsConsumedNotAdmitted(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	fd := fs{op: uid(1, 1), author: h.author, seq: 1, typ: "test.decide", ct: 1, payload: text("subject-" + randomID())}.object()
	h.push(in, 0, fop(1, fd))
	decisions := h.lastCommit()["decisions"].([]any)
	if len(decisions) != 1 {
		t.Fatalf("expected one decision, got %d", len(decisions))
	}
	x := decisions[0].(map[string]any)["decision_id"].(string)
	var before []byte
	h.db().QueryRow(`SELECT decision FROM decisions WHERE decision_id = ?`, x).Scan(&before)
	fx := fs{op: x, author: h.author, seq: 2, prev: uid(1, 1), ct: 2}.object()
	r := h.push(in, 1, fop(2, fx))
	if r.InstallWatermark != 2 || len(r.Results) != 1 {
		t.Fatalf("the slot was not consumed: %s", wireJSON(t, r))
	}
	status := r.Results[0].Status
	switch {
	case status == "op_id_conflict" && r.Results[0].Receipt == nil:
	case status == "accepted" && r.Results[0].Receipt["outcome"] == "rejected":
	default:
		t.Errorf("a fact reusing a decision ID answered %s", wireJSON(t, r.Results[0]))
	}
	var after []byte
	h.db().QueryRow(`SELECT decision FROM decisions WHERE decision_id = ?`, x).Scan(&after)
	if !bytes.Equal(before, after) || h.count(`SELECT COUNT(*) FROM causal_nodes WHERE node_id = ? AND kind = 'fact'`, x) != 0 {
		t.Error("the decision changed or the fact became a causal node")
	}
	again := h.push(in, 1, fop(2, fx))
	if again.Results[0].Status != status {
		t.Errorf("replay answered %s, first %s", again.Results[0].Status, status)
	}
	h.verifyLog(h.account)
	h.verifySnapshot(in)
}

// A handler failure other than validation aborts the push: nothing consumed,
// no partial commit (the HTTP layer answers 503 server_busy).
func TestHandlerFailureAbortsThePushAndConsumesNothing(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	ok := h.note(uid(1, 1), 1, "")
	bad := fs{op: uid(1, 2), author: h.author, seq: 2, typ: "test.fail", ct: 1}.object()
	_, err := h.tryPush(in, 0, fop(1, ok), fop(2, bad))
	if err == nil || protoCode(err) != "" {
		t.Fatalf("handler failure answered %v, want a transient (non-protocol) error", err)
	}
	if h.consumed(in) != 0 || h.head().Seq != 0 || h.count(`SELECT COUNT(*) FROM facts`) != 0 ||
		h.count(`SELECT COUNT(*) FROM causal_nodes`) != 0 || h.count(`SELECT COUNT(*) FROM author_positions`) != 0 {
		t.Fatal("a failed push left state behind")
	}
	h.expect(h.push(in, 0, fop(1, ok)), 1, want{seq: 1, status: "accepted", outcome: "applied"})
	h.verifyLog(h.account)
}

func TestRecordsWithinOnePushAreAccumulated(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	set := func(i int, key, value string) map[string]any {
		return fs{op: uid(9-i, i), author: h.author, seq: int64(i), typ: "test.set", ct: 1, payload: map[string]any{"key": key, "text": value}}.object()
	}
	r := h.push(in, 0, fop(1, set(1, "x", "first")), fop(2, set(2, "y", "only")), fop(3, set(3, "x", "final")))
	h.expect(r, 3, want{seq: 1, status: "accepted", outcome: "applied"}, want{seq: 2, status: "accepted", outcome: "applied"},
		want{seq: 3, status: "accepted", outcome: "applied"})
	wantRecords := []any{
		map[string]any{"entity": "reg", "id": "x", "field_group": "value", "value": "final", "provenance": sortedList(uid(8, 1), uid(6, 3))},
		map[string]any{"entity": "reg", "id": "y", "field_group": "value", "value": "only", "provenance": []any{uid(7, 2)}},
	}
	if got := h.lastCommit()["records"]; !bytes.Equal(canon(got), canon(wantRecords)) {
		t.Errorf("records %s, want %s", canon(got), canon(wantRecords))
	}
}

func TestDecisionsAreCausalNodesForLaterOperationsInTheSamePush(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	s1, s2 := "first-"+randomID(), "second-"+randomID()
	f1 := fs{op: uid(1, 1), author: h.author, seq: 1, typ: "test.decide", ct: 1, payload: text(s1)}.object()
	f2 := fs{op: uid(1, 2), author: h.author, seq: 2, typ: "test.decide_after", ct: 1, payload: map[string]any{"text": s2, "after": s1}}.object()
	r := h.push(in, 0, fop(1, f1), fop(2, f2))
	h.expect(r, 2, want{seq: 1, status: "accepted", outcome: "applied"}, want{seq: 2, status: "accepted", outcome: "applied"})
	c := h.lastCommit()
	ds := c["decisions"].([]any)
	if len(ds) != 2 {
		t.Fatalf("%d decisions", len(ds))
	}
	byS := map[string]map[string]any{}
	for _, d := range ds {
		m := d.(map[string]any)
		byS[m["subject"].(string)] = m
		if num(m["epoch"]) != num(c["epoch"]) || num(m["seq"]) != num(c["seq"]) {
			t.Errorf("decision rank is not the commit revision: %v", m)
		}
	}
	d1, d2 := byS[s1]["decision_id"].(string), byS[s2]
	if !slices.Contains(strList(d2["dependencies"]), d1) {
		t.Errorf("the later decision does not depend on the earlier one: %v", d2["dependencies"])
	}
	items := h.verifySnapshot(in)
	if !(indexOf(items, uid(1, 1)) < indexOf(items, d1) && indexOf(items, d1) < indexOf(items, d2["decision_id"].(string))) {
		t.Error("snapshot does not order fact → decision → decision")
	}
	h.verifyLog(h.account)
}
