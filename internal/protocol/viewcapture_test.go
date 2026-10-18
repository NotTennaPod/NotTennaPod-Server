package protocol

import (
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The read-model clock across rebuilds: an in-place rebuild
// (storage.ClearDerived) and a staged one (storage.PublishStaged) raise the rebuilt account's
// read-model generation and tick and clear its stamps; another account's
// clock is unchanged. (The capture audit, on in every test of the package,
// checks the same of every transaction.)
func TestRebuildsRaiseTheReadModelGenerationOfTheirAccountOnly(t *testing.T) {
	h := newHarness(t)
	other := h.newAccount()
	inst, otherInst := h.addInstall(), h.addInstallIn(other)
	h.push(inst, 0, fop(1, fs{op: uid(40, 1), author: h.author, seq: 1, typ: "test.decide", payload: text("i0-a")}.object()),
		fop(2, fs{op: uid(40, 2), author: h.author, seq: 2, prev: uid(40, 1)}.object()))
	if _, err := h.svc.Push(h.ctx, PushRequest{AccountID: other, Cursor: h.headOf(other), InstallID: otherInst,
		Operations: []PushOp{fop(1, fs{op: uid(41, 1), author: randomID(), seq: 1}.object())}}, authorizer(other, otherInst)); err != nil {
		t.Fatal(err)
	}
	// The test handlers publish no records: give both accounts a captured
	// change (the first rebuild drops the account's, which its archive does
	// not determine; the other's is removed at the end).
	for _, account := range []string{h.account, other} {
		if err := h.store.Write(h.ctx, func(tx *storage.WriteTx) error {
			return tx.Account(account).PutRecord(storage.RecordRow{Entity: "probe", EntityID: account, FieldGroup: "x", Value: `{}`, Provenance: `[]`})
		}); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		if err := h.store.Write(h.ctx, func(tx *storage.WriteTx) error {
			_, err := tx.Account(other).DeleteRecord("probe", other, "x")
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}()
	clock := func(account string) (c storage.ViewClock, stamps int) {
		t.Helper()
		if err := h.store.Read(h.ctx, func(r *storage.ReadTx) error {
			var err error
			if c, err = r.Account(account).ViewClock(); err != nil {
				return err
			}
			changes, _, _, err := r.Account(account).ViewChangesSince(0, storage.ViewCursor{})
			stamps = len(changes)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return c, stamps
	}
	for _, account := range []string{h.account, other} {
		if c, stamps := clock(account); c.Tick == 0 || stamps == 0 {
			t.Fatalf("precondition: the change was captured (%+v, %d stamps)", c, stamps)
		}
	}
	for _, staged := range []bool{false, true} {
		h.svc.StagingThreshold = -1
		if staged {
			h.svc.StagingThreshold = 1
		}
		before, _ := clock(h.account)
		otherBefore, otherStamps := clock(other)
		report, err := h.svc.Rebuild(h.ctx, h.account)
		if err != nil || report.Staged != staged {
			t.Fatalf("rebuild (staged %v): %v %+v", staged, err, report)
		}
		after, stamps := clock(h.account)
		if after.Generation != before.Generation+1 || after.Tick <= before.Tick || stamps != 0 {
			t.Errorf("staged %v: the account's clock went from %+v to %+v with %d stamps", staged, before, after, stamps)
		}
		if o, s := clock(other); o != otherBefore || s != otherStamps {
			t.Errorf("staged %v: the other account's clock went from %+v to %+v", staged, otherBefore, o)
		}
	}
	h.svc.StagingThreshold = 0
}
