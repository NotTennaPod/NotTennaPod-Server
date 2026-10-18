package ops

import (
	"runtime"
	"testing"
	"weak"
)

// Snapshots saved for reuse never keep the transaction they were replayed in:
// once a later command replaces the account's replay cache, the old cache
// (its snapshots, its commit's state and every older cache) is garbage. 40
// ordinary moves each supersede the cache the previous one left; after a
// collection none of the 40 is reachable.
func TestSupersededReplayCachesAreCollected(t *testing.T) {
	q := newQueueEnv(t, "A", "B")
	q.replayCheck = false
	a := q.device("A")
	q.seed(t, a, "A", "B")
	var old []weak.Pointer[replayCache]
	for i := range 40 {
		old = append(old, weakCache(t, q))
		place := atStart()
		if i%2 == 1 {
			place = atEnd()
		}
		q.clock += second
		q.pushOK(t, a, q.move(t, a, "A", place), "applied")
	}
	for range 3 {
		runtime.GC()
	}
	retained := 0
	for _, p := range old {
		if p.Value() != nil {
			retained++
		}
	}
	if retained > 0 {
		t.Fatalf("%d of %d superseded replay caches are still reachable", retained, len(old))
	}
}

// weakCache is a weak reference to the account's current replay cache (no
// strong reference stays on the caller's stack).
//
//go:noinline
func weakCache(t *testing.T, q *queueEnv) weak.Pointer[replayCache] {
	rc := q.cache(t)
	if rc == nil {
		t.Fatal("no replay cache")
	}
	return weak.Make(rc)
}
