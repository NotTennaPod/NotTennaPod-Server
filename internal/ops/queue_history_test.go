package ops

// A sorted add with a long queue history, under the queue replay
// (PROTOCOL §7.6).

import (
	"fmt"
	"testing"
	"time"
)

// A sorted add after a long history of sorts replays from its snapshot, not
// the whole history: 300 queued episodes, 200 earlier sort commands.
func TestSortedAddWithLongQueueHistory(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("load test (make check runs it without the race detector)")
	}
	const members = 300
	const sorts = 200
	titles := make([]string, members+1)
	for i := range titles {
		titles[i] = fmt.Sprintf("E%04d", i)
	}
	q := newQueueEnv(t, titles...)
	d := q.device("A")
	q.seed(t, d, titles[:members]...)
	q.pushOK(t, d, q.policy(d, titleSorting(true)), "applied")
	began := time.Now()
	for i := 0; i < sorts; i += 50 {
		var batch []map[string]any
		for j := 0; j < 50; j++ {
			q.clock += second
			order := "episode_title_z_a"
			if j%2 == 1 {
				order = "episode_title_a_z"
			}
			batch = append(batch, q.sortCmd(d, order, titles[:members], nil))
		}
		for _, r := range q.push(d, batch...) {
			if outcome(r) != "applied" {
				t.Fatalf("sort: %s", outcome(r))
			}
		}
	}
	t.Logf("%d sorts of %d members in %s", sorts, members, time.Since(began))
	q.clock += second
	f := q.add(t, d, atEnd(), "", titles[members])
	began = time.Now()
	q.pushOK(t, d, f, "applied")
	elapsed := time.Since(began)
	t.Logf("sorted add, %d queued, %d earlier sorts: %s", members, sorts, elapsed)
	if elapsed > 30*time.Second {
		t.Fatalf("sorted add took %s", elapsed)
	}
	if got := q.order(t); got[:len("E0000,E0001")] != "E0000,E0001" {
		t.Fatalf("queue starts %s", got[:20])
	}
}
