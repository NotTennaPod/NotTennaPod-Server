package ops

import (
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// measureWarmup times the warm-up after a restart and the first command
// after it (TestQueueReplayMeasure).
func (q *queueEnv) measureWarmup(t *testing.T, one func() (string, time.Duration)) {
	for i := 0; i < 2; i++ {
		q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
			tx.Account(q.account).Memo().Delete(memoKey{q.account})
			return nil
		})
		t0 := time.Now()
		if _, err := WarmQueue(t.Context(), q.svc, q.account); err != nil {
			t.Fatal(err)
		}
		warm := time.Since(t0)
		full0, _ := replayCounts()
		kind, dur := one()
		full1, _ := replayCounts()
		t.Logf("after a restart and the warm-up (%s, holding the writer): first command (%s) %s (%d full replays)", warm, kind, dur, full1-full0)
	}
}
