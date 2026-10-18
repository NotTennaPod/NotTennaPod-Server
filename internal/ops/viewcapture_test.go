package ops

import (
	"context"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The read-model clock across recovery: a recovery upload that joins lost
// entries, in place (storage.ClearDerived) or staged (storage.PublishStaged), raises
// the account's read-model generation and tick and clears its stamps;
// another account's clock is unchanged. (The capture audit, on in every
// test of the package, checks the same of every transaction.)
func TestRecoveryRaisesTheReadModelGenerationOfItsAccountOnly(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "in place", true: "staged"}[staged], func(t *testing.T) {
			ctx := context.Background()
			l := newListenEnv(t)
			if staged {
				l.svc.StagingThreshold = 1
			}
			var other string
			if err := l.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
				a, err := tx.CreateAccount()
				other = a.ID
				if err != nil {
					return err
				}
				return tx.Account(other).PutRecord(storage.RecordRow{Entity: "account", EntityID: other, FieldGroup: "queue_policy", Value: `{}`, Provenance: `[]`})
			}); err != nil {
				t.Fatal(err)
			}
			a, d := l.device("A"), l.device("D")
			backup := l.backup(t)
			atBackup := l.headSeq(t)
			l.clock += minute
			l.pushOK(t, a, l.lfact(a, "favorite.set", l.ref, map[string]any{"on": true}), "applied")
			lost := l.logCommits(t, atBackup)
			l.restore(t, backup)
			d2 := l.successor(t, d)
			r, _ := l.startRecovery(t, d2, nil)
			clock := func(account string) (c storage.ViewClock, stamps int) {
				t.Helper()
				if err := l.svc.Store.Read(ctx, func(rd *storage.ReadTx) error {
					var err error
					if c, err = rd.Account(account).ViewClock(); err != nil {
						return err
					}
					changes, _, _, err := rd.Account(account).ViewChangesSince(0, storage.ViewCursor{})
					stamps = len(changes)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				return c, stamps
			}
			before, _ := clock(l.account)
			otherBefore, otherStamps := clock(other)
			if otherBefore.Tick == 0 {
				t.Fatal("precondition: the other account's change was captured")
			}
			if st := statuses(r.upload(t, entries(lost))); st["joined"] != len(entries(lost)) {
				t.Fatalf("upload: %v", st)
			}
			after, stamps := clock(l.account)
			if after.Generation <= before.Generation || after.Tick <= before.Tick || stamps != 0 {
				t.Errorf("the account's clock went from %+v to %+v, with %d stamps", before, after, stamps)
			}
			if o, s := clock(other); o != otherBefore || s != otherStamps {
				t.Errorf("the other account's clock went from %+v to %+v", otherBefore, o)
			}
			t.Logf("clock %+v → %+v, %d stamps since", before, after, stamps)
		})
	}
}
