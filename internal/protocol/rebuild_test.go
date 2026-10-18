package protocol

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// checkRebuild is the rebuild invariant (PROTOCOL §6.11): rebuilding an account from
// its archive reproduces its published records with their provenance and its
// whole derived state, and appends no commit.
func checkRebuild(t testing.TB, svc *Service, account string) {
	t.Helper()
	if t.Failed() || t.Skipped() {
		return
	}
	ctx := context.Background()
	before, err := svc.Store.DerivedState(ctx, account)
	if errors.Is(err, storage.ErrClosed) {
		return
	}
	if err != nil {
		t.Errorf("rebuild invariant: %v", err)
		return
	}
	headBefore := headSeq(t, svc.Store, account)
	report, err := svc.Rebuild(ctx, account)
	after, derr := svc.Store.DerivedState(ctx, account)
	if derr != nil {
		t.Errorf("rebuild invariant: %v", derr)
		return
	}
	headAfter := headSeq(t, svc.Store, account)
	switch {
	case err != nil:
		t.Errorf("rebuild invariant: %v\n%s", err, describeReport(report))
	case report.Committed || len(report.Changed) > 0:
		t.Errorf("rebuild invariant: the rebuild changed published records\n%s", describeReport(report))
	default:
		t.Logf("rebuild invariant: ok (%d facts, %d decisions, %v)", report.Facts, report.Decisions, report.Duration)
	}
	if headAfter != headBefore && !report.Committed {
		t.Errorf("rebuild invariant: the head moved from %d to %d without a commit", headBefore, headAfter)
	}
	if !report.Committed {
		if diff := diffDerived(before, after); diff != "" {
			t.Errorf("rebuild invariant: derived state differs after the rebuild:\n%s", diff)
		}
	}
}

func headSeq(t testing.TB, store *storage.Store, account string) int64 {
	var seq int64
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(account).Head()
		seq = h.Seq
		return err
	}); err != nil {
		t.Errorf("rebuild invariant: head: %v", err)
	}
	return seq
}

func unsupportedTypes(r RebuildReport) []string {
	var types []string
	for _, u := range r.Unsupported {
		types = append(types, strings.Fields(u)[0])
	}
	slices.Sort(types)
	return slices.Compact(types)
}

func describeReport(r RebuildReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  %d facts, %d decisions (%d made again), lost rows %d\n", r.Facts, r.Decisions, r.Consumed, r.LostRows)
	for _, m := range r.Mismatches {
		fmt.Fprintf(&b, "  mismatch: %s\n", m)
	}
	for _, u := range r.Unsupported {
		fmt.Fprintf(&b, "  unsupported: %s\n", u)
	}
	for i, rec := range r.Changed {
		if i == 20 {
			fmt.Fprintf(&b, "  … %d more changed records\n", len(r.Changed)-20)
			break
		}
		fmt.Fprintf(&b, "  changed: %s\n", canon(rec.Object()))
	}
	return b.String()
}

// diffDerived lists the rows that differ per table (at most a few each).
func diffDerived(before, after map[string][]string) string {
	var tables []string
	for t := range before {
		tables = append(tables, t)
	}
	slices.Sort(tables)
	var out []string
	for _, table := range tables {
		b, a := before[table], after[table]
		if slices.Equal(a, b) {
			continue
		}
		var gone, added []string
		for _, x := range b {
			if _, ok := slices.BinarySearch(a, x); !ok {
				gone = append(gone, x)
			}
		}
		for _, x := range a {
			if _, ok := slices.BinarySearch(b, x); !ok {
				added = append(added, x)
			}
		}
		out = append(out, fmt.Sprintf("  %s: %d rows before, %d after", table, len(b), len(a)))
		for i, x := range gone {
			if i == 3 {
				break
			}
			out = append(out, "    - "+x)
		}
		for i, x := range added {
			if i == 3 {
				break
			}
			out = append(out, "    + "+x)
		}
	}
	return strings.Join(out, "\n")
}

// A scenario with admitted, rejected and branching facts and decisions
// rebuilds to the same state, and its records survive.
func TestRebuildReproducesTheArchive(t *testing.T) {
	h := newHarness(t)
	inst := h.addInstall("queue")
	a, b, c, d, x := uid(1, 1), uid(1, 2), uid(1, 3), uid(1, 4), uid(1, 5)
	h.push(inst, 0,
		fop(1, fs{op: a, author: h.author, seq: 1, typ: "test.decide", payload: text("rebuild-s1")}.object()),
		fop(2, fs{op: b, author: h.author, seq: 2, prev: a, deps: []string{a}, typ: "test.decide_after",
			payload: map[string]any{"text": "rebuild-s2", "after": "rebuild-s1"}}.object()),
		fop(3, fs{op: c, author: h.author, seq: 3, prev: b, typ: "test.inbox_note"}.object()), // rejected: capability
	)
	// A copy writes the same position: a derived branch (PROTOCOL §6.10).
	h.push(inst, 3,
		fop(4, fs{op: d, author: h.author, seq: 3, prev: b, deps: []string{c}}.object()),
		fop(5, fs{op: x, author: h.author, seq: 4, prev: d, deps: []string{d}}.object()))
	report, err := h.svc.Rebuild(h.ctx, h.account)
	if err != nil {
		t.Fatalf("rebuild: %v\n%s", err, describeReport(report))
	}
	if report.Facts != 5 || report.Decisions != 2 || report.Consumed != 2 || report.Committed {
		t.Fatalf("report: %+v", report)
	}
}

// A rebuild publishes, in one commit, the records the archive determines
// where the published ones differ, and tombstones those it does not
// determine.
func TestRebuildRepublishesWhatDiffers(t *testing.T) {
	h := newHarness(t)
	h.reg.Register("test.register", 1, registerHandler{})
	inst := h.addInstall()
	a := uid(2, 1)
	h.push(inst, 0, fop(1, fs{op: a, author: h.author, seq: 1, typ: "test.register", payload: text("v1")}.object()))
	headBefore := headSeq(t, h.store, h.account)
	if _, err := h.db().Exec(`UPDATE records SET value = '{"text":"tampered"}' WHERE account_id = ?`, h.account); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db().Exec(`INSERT INTO records (account_id, entity, entity_id, field_group, value, provenance) VALUES (?, 'note', 'stray', 'text', '{}', '[]')`,
		h.account); err != nil {
		t.Fatal(err)
	}
	report, err := h.svc.Rebuild(h.ctx, h.account)
	if err != nil {
		t.Fatalf("rebuild: %v\n%s", err, describeReport(report))
	}
	if !report.Committed || len(report.Changed) != 2 || headSeq(t, h.store, h.account) != headBefore+1 {
		t.Fatalf("report: %s", describeReport(report))
	}
	commit := h.lastCommit()
	records := commit["records"].([]any)
	if len(records) != 2 {
		t.Fatalf("commit records: %v", records)
	}
	first, second := records[0].(map[string]any), records[1].(map[string]any)
	if first["id"] != a || first["value"].(map[string]any)["text"] != "v1" || second["id"] != "stray" || second["tombstone"] != true {
		t.Fatalf("commit records: %v", records)
	}
	if err := h.store.VerifyChain(h.ctx, h.account); err != nil {
		t.Fatal(err)
	}
	// A second rebuild finds nothing to change.
	if report, err := h.svc.Rebuild(h.ctx, h.account); err != nil || report.Committed {
		t.Fatalf("second rebuild: %v %s", err, describeReport(report))
	}
}

// registerHandler is test.register: it publishes a stored record (a
// projection), so a rebuild has records to compare.
type registerHandler struct{}

func (registerHandler) Capability() string { return "" }
func (registerHandler) Validate(f *Fact) error {
	if _, ok := f.Payload["text"].(string); !ok {
		return errors.New("text required")
	}
	return nil
}
func (registerHandler) Apply(ctx *ApplyContext, f *Fact) (Outcome, error) {
	return Outcome{Result: "applied"}, ctx.PublishRecord("note", f.OpID, "text", map[string]any{"text": f.Payload["text"]})
}

// A server decision whose type has no admit step cannot be replayed: the
// rebuild changes nothing and reports it.
func TestRebuildRefusesDecisionsWithoutAdmitStep(t *testing.T) {
	h := newHarness(t)
	h.noRebuild = "the test refuses its rebuild itself"
	inst := h.addInstall()
	a := uid(3, 1)
	h.push(inst, 0, fop(1, fs{op: a, author: h.author, seq: 1}.object()))
	if err := h.svc.ServerCommit(h.ctx, h.account, func(c *ApplyContext) error {
		_, err := c.Decide(DecisionInput{Type: "test.server", Version: 1, Subject: "s", Dependencies: []string{a}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before, err := h.store.DerivedState(h.ctx, h.account)
	if err != nil {
		t.Fatal(err)
	}
	report, err := h.svc.Rebuild(h.ctx, h.account)
	if !errors.Is(err, ErrRebuildUnsupported) || len(report.Unsupported) != 1 || !strings.HasPrefix(report.Unsupported[0], "test.server ") {
		t.Fatalf("rebuild: %v\n%s", err, describeReport(report))
	}
	after, err := h.store.DerivedState(h.ctx, h.account)
	if err != nil {
		t.Fatal(err)
	}
	if diff := diffDerived(before, after); diff != "" {
		t.Fatalf("a refused rebuild changed the account:\n%s", diff)
	}
}

// A replay that does not reproduce a receipt (here: the handler changed
// since) is reported, and the rebuild changes nothing.
func TestRebuildReportsAnOutcomeItCannotReproduce(t *testing.T) {
	h := newHarness(t)
	inst := h.addInstall()
	a := uid(4, 1)
	h.push(inst, 0, fop(1, fs{op: a, author: h.author, seq: 1}.object()))
	before, err := h.store.DerivedState(h.ctx, h.account)
	if err != nil {
		t.Fatal(err)
	}
	h.reg.Register("test.note", 1, refusingNote{})
	report, err := h.svc.Rebuild(h.ctx, h.account)
	if !errors.Is(err, ErrRebuildMismatch) || len(report.Mismatches) != 1 ||
		!strings.Contains(report.Mismatches[0], "receipt applied, replay not_applied/refused") {
		t.Fatalf("rebuild: %v\n%s", err, describeReport(report))
	}
	after, err := h.store.DerivedState(h.ctx, h.account)
	if err != nil {
		t.Fatal(err)
	}
	if diff := diffDerived(before, after); diff != "" {
		t.Fatalf("a refused rebuild changed the account:\n%s", diff)
	}
	h.reg.Register("test.note", 1, acNote{}) // the invariant at the end holds again
}

type refusingNote struct{ acNote }

func (refusingNote) Apply(*ApplyContext, *Fact) (Outcome, error) {
	return Outcome{Result: "not_applied", Reason: "refused"}, nil
}

// A replay takes the capability checks' answers from the receipts (PROTOCOL §6.11):
// a declaration changed since does not change what was admitted.
func TestRebuildKeepsCapabilityOutcomesAfterADeclarationChanges(t *testing.T) {
	h := newHarness(t)
	inst := h.addInstall()
	a, b := uid(6, 1), uid(6, 2)
	h.push(inst, 0, fop(1, fs{op: a, author: h.author, seq: 1, typ: "test.queue_note"}.object()))
	h.setCapabilities(inst, "queue")
	h.push(inst, 1, fop(2, fs{op: b, author: h.author, seq: 2, prev: a, typ: "test.queue_note"}.object()))
	h.setCapabilities(inst)
	if report, err := h.svc.Rebuild(h.ctx, h.account); err != nil || report.Committed {
		t.Fatalf("rebuild: %v\n%s", err, describeReport(report))
	}
}

// Competing decisions (equal type and subject): the effective one is the
// smallest rank (epoch, seq, decision_id), whatever the causal order (PROTOCOL §6.11).
func TestDecisionBySubjectOrdersByRank(t *testing.T) {
	h := newHarness(t)
	inst := h.addInstall()
	a := uid(5, 1)
	h.push(inst, 0, fop(1, fs{op: a, author: h.author, seq: 1}.object()))
	early, late := uid(0xf0, 1), uid(0x0f, 1)
	checked := errors.New("checked") // roll the hand-made decisions back
	err := h.store.Write(h.ctx, func(tx *storage.WriteTx) error {
		acct := tx.Account(h.account)
		// late is inserted first (the earlier causal rank) at a later revision.
		for _, d := range []struct {
			id  string
			seq int64
		}{{late, 9}, {early, 2}} {
			if err := acct.InsertNode(d.id, "decision", 1, []string{a}); err != nil {
				return err
			}
			if err := acct.InsertDecision(storage.DecisionRow{DecisionID: d.id, DecisionType: "test.compete", Subject: "s", Epoch: 1, Seq: d.seq,
				NormalizedTime: 1, Decision: []byte("{}")}); err != nil {
				return err
			}
		}
		got, err := acct.DecisionBySubject("test.compete", "s")
		if err == nil && got != early {
			return fmt.Errorf("effective decision %s, want %s", got, early)
		}
		if err == nil {
			err = checked
		}
		return err
	})
	if !errors.Is(err, checked) {
		t.Fatal(err)
	}
}

// A rebuild changes no head's frontier (PROTOCOL §12 FrontierAt): its replay makes the same causal
// nodes again and keeps the archive's rows whole, and a commit it publishes adds no node, so the
// new head's frontier is the previous head's.
func TestRebuildKeepsTheFrontierAtEveryHead(t *testing.T) {
	h := newHarness(t)
	h.reg.Register("test.register", 1, registerHandler{})
	inst := h.addInstall()
	a, b, c, d := uid(7, 1), uid(7, 2), uid(7, 3), uid(7, 4)
	h.push(inst, 0, fop(1, fs{op: a, author: h.author, seq: 1, typ: "test.decide", payload: text("frontier-s1")}.object()))
	h.push(inst, 1, fop(2, fs{op: b, author: h.author, seq: 2, prev: a, typ: "test.register", payload: text("v")}.object()),
		fop(3, fs{op: c, author: h.author, seq: 3, prev: b, deps: []string{a}, typ: "test.inbox_note"}.object())) // rejected
	h.push(inst, 3, fop(4, fs{op: d, author: h.author, seq: 4, prev: c, deps: []string{c}}.object()))
	frontiers := func() map[int64]string {
		out := map[int64]string{}
		if err := h.store.Read(h.ctx, func(r *storage.ReadTx) error {
			acct := r.Account(h.account)
			head, err := acct.Head()
			if err != nil {
				return err
			}
			for seq := int64(0); seq <= head.Seq; seq++ {
				f, err := acct.FrontierAt(head.Epoch, seq)
				if err != nil {
					return err
				}
				out[seq] = strings.Join(f, ",")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := frontiers()
	// A tampered record makes the rebuild publish a commit.
	if _, err := h.db().Exec(`UPDATE records SET value = '{"text":"tampered"}' WHERE account_id = ?`, h.account); err != nil {
		t.Fatal(err)
	}
	report, err := h.svc.Rebuild(h.ctx, h.account)
	if err != nil || !report.Committed {
		t.Fatalf("rebuild: %v\n%s", err, describeReport(report))
	}
	after := frontiers()
	head := int64(len(before) - 1)
	for seq := int64(0); seq <= head; seq++ {
		if after[seq] != before[seq] {
			t.Errorf("frontier at %d: %s before the rebuild, %s after", seq, before[seq], after[seq])
		}
	}
	if after[head+1] != before[head] {
		t.Errorf("the rebuild's commit changed the frontier: %s, then %s", before[head], after[head+1])
	}
}

// A server commit's decisions are admitted through their types' admit steps, in index order, in
// their commit.
func TestRebuildAdmitsServerDecisions(t *testing.T) {
	h := newHarness(t)
	var admitted []string
	h.reg.RegisterAdmit("test.server", func(c *ApplyContext, d map[string]any) error {
		admitted = append(admitted, d["subject"].(string))
		c.OpID = d["decision_id"].(string)
		defer func() { c.OpID = "" }()
		return c.PublishRecord("note", d["subject"].(string), "server", map[string]any{"by": d["decision_id"]})
	})
	inst := h.addInstall()
	a := uid(8, 1)
	h.push(inst, 0, fop(1, fs{op: a, author: h.author, seq: 1}.object()))
	if err := h.svc.ServerCommit(h.ctx, h.account, func(c *ApplyContext) error {
		for _, s := range []string{"s1", "s2"} {
			d, err := c.Decide(DecisionInput{Type: "test.server", Version: 1, Subject: s, Dependencies: []string{a}})
			if err != nil {
				return err
			}
			c.OpID = d["decision_id"].(string)
			err = c.PublishRecord("note", s, "server", map[string]any{"by": d["decision_id"]})
			c.OpID = ""
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	report, err := h.svc.Rebuild(h.ctx, h.account)
	if err != nil || report.Admitted != 2 || report.Committed || strings.Join(admitted, ",") != "s1,s2" {
		t.Fatalf("rebuild: %v %v\n%s", err, admitted, describeReport(report))
	}
}

// A rebuild checkpoints the write-ahead log after it commits, in place or
// staged (PROTOCOL §14), and only once the account is no longer busy: a push
// arriving while the housekeeping runs is accepted, never answered
// server_busy. A failed checkpoint never fails the rebuild, which is
// published once.
func TestRebuildCheckpointsTheWAL(t *testing.T) {
	h := newHarness(t)
	h.reg.Register("test.register", 1, registerHandler{})
	inst := h.addInstall()
	seq := 1
	fact := func(n int) PushOp {
		f := fs{op: uid(3, n), author: h.author, seq: int64(n), typ: "test.register", payload: text(fmt.Sprintf("v%d", n))}
		if n > 1 {
			f.prev = uid(3, n-1)
		}
		return fop(int64(n), f.object())
	}
	h.push(inst, 0, fact(1))
	var calls []string
	var during []string // the push made while each checkpoint ran: its status or error
	pushing := true
	if err := h.store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(h.ctx, h.dir, storage.WithWALCheckpointHook(func(after string, _ storage.WALCheckpoint, _ error) error {
		calls = append(calls, after)
		if pushing {
			seq++
			if resp, err := h.tryPush(inst, int64(seq-1), fact(seq)); err != nil {
				during = append(during, err.Error())
			} else {
				during = append(during, resp.Results[0].Status)
			}
		}
		return errors.New("simulated checkpoint failure")
	}))
	if err != nil {
		t.Fatal(err)
	}
	h.store = store
	h.startService()
	if _, err := h.db().Exec(`UPDATE records SET value = '{"text":"tampered"}' WHERE account_id = ? AND entity_id = ?`, h.account, uid(3, 1)); err != nil {
		t.Fatal(err)
	}
	before := headSeq(t, h.store, h.account)
	report, err := h.svc.Rebuild(h.ctx, h.account)
	if err != nil || !report.Committed || report.Staged {
		t.Fatalf("rebuild in place: %v %s", err, describeReport(report))
	}
	if !slices.Equal(calls, []string{"rebuild"}) || !slices.Equal(during, []string{"accepted"}) || headSeq(t, h.store, h.account) != before+2 {
		t.Fatalf("after a rebuild in place: checkpoints %q, pushes during them %q, head %d (was %d)", calls, during, headSeq(t, h.store, h.account), before)
	}
	calls, during = nil, nil
	if _, err := h.db().Exec(`UPDATE records SET value = '{"text":"tampered again"}' WHERE account_id = ? AND entity_id = ?`, h.account, uid(3, 1)); err != nil {
		t.Fatal(err)
	}
	h.svc.StagingThreshold = 1
	before = headSeq(t, h.store, h.account)
	report, err = h.svc.Rebuild(h.ctx, h.account)
	if err != nil || !report.Committed || !report.Staged {
		t.Fatalf("staged rebuild: %v %s", err, describeReport(report))
	}
	if !slices.Equal(calls, []string{"staged rebuild"}) || !slices.Equal(during, []string{"accepted"}) || headSeq(t, h.store, h.account) != before+2 {
		t.Fatalf("after a staged rebuild: checkpoints %q, pushes during them %q, head %d (was %d)", calls, during, headSeq(t, h.store, h.account), before)
	}
	pushing = false
	if err := h.store.VerifyChain(h.ctx, h.account); err != nil {
		t.Fatal(err)
	}
}
