package ops

// An archive rebuild replays the account from its archive after clearing its
// derived state (PROTOCOL §6.11); the account's head does not move meanwhile,
// so the process-wide queue replay cache from before still validates. A
// handler replayed early in the rebuild must not resume from that cache: its
// later snapshots name nodes the rebuild has not derived again yet.

import (
	"context"
	"fmt"
	mrand "math/rand/v2"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// rebuildAfterRecovery: a queue-capable phone starts an episode that is not
// queued (no add: the phone saw it as it was) and then moves a queued
// one; both are lost in a restore and recovered. A recovered fact has no
// accepting transport slot, so its replay checks it against no capability:
// the start's replay resolves a queue placement (replayNow) before the later
// move's node is derived again. split recovers commit by commit (each upload
// rebuilds); cold restarts the server before the explicit rebuild.
func rebuildAfterRecovery(t *testing.T, split, cold bool) {
	q := newQueueEnv(t, "A", "B", "C")
	p := q.device("P")
	q.seed(t, p, "A", "B")
	backup := q.backup(t)
	at := q.headSeq(t)
	q.sees(p, p.last)
	q.clock += minute
	q.pushOK(t, p, q.lfact(p, "session.start", q.refs["C"], start(newID(), 0, true)), "applied")
	q.clock += minute
	q.pushOK(t, p, q.move(t, p, "A", atEnd()), "applied")
	q.expectOrder(t, "B,A")
	commits := q.logCommits(t, at)
	q.restore(t, backup)
	succ := q.successor(t, p)
	rec, _ := q.startRecovery(t, succ, nil)
	uploads := [][]protocol.RecoveryEntry{entries(commits)}
	if split {
		uploads = nil
		for _, c := range commits {
			uploads = append(uploads, c.entries)
		}
	}
	for i, list := range uploads {
		for _, r := range rec.upload(t, list).Results {
			if r.Status != "joined" && r.Status != "known" {
				t.Fatalf("upload %d: %s %s", i, r.ID, r.Status)
			}
		}
	}
	q.expectOrder(t, "B,A")
	if cold {
		q.svc.Store.Close()
		q.reopen(t)
	}
	if _, err := q.svc.Rebuild(context.Background(), q.account); err != nil {
		t.Fatalf("the rebuild after the recovery: %v", err)
	}
	checkReplay(t, q.svc, q.account)
	q.expectOrder(t, "B,A")
}

func TestRebuildAfterRecoveryWithAWarmQueueCache(t *testing.T) { rebuildAfterRecovery(t, false, false) }

func TestRebuildAfterRecoveryWithAColdQueueCache(t *testing.T) { rebuildAfterRecovery(t, false, true) }

func TestRebuildAfterARecoverySplitAcrossUploads(t *testing.T) { rebuildAfterRecovery(t, true, false) }

// Twelve seeded histories, as the read model's restore-and-recover workload
// produced the failure (3 of 12 seeds): after a backup, a queue-capable phone
// starts, ends, adds, moves and removes episodes and changes the sorting
// register, and a second phone acts too; the server is restored and every
// lost commit is recovered (in one request, or commit by commit for odd
// seeds); then the account is rebuilt with the replay cache the recovery left.
// Every rebuild succeeds, the published queue is the replay's order, and the
// rebuild does not change the order the recovery published.
func TestRebuildAfterRecoverySeeds(t *testing.T) {
	titles := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
	for seed := uint64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			r := mrand.New(mrand.NewPCG(seed, 8))
			q := newQueueEnv(t, titles...)
			p, o := q.device("P"), q.device("O")
			q.seed(t, p, "A", "B", "C")
			backup := q.backup(t)
			at := q.headSeq(t)
			q.sees(p, p.last)
			q.sees(o, p.last)
			view := &queueView{key: map[string]string{}, pid: map[string]string{}}
			sessions := map[string]string{}
			for range 10 {
				d := p
				if r.IntN(4) == 0 {
					d = o
				}
				view.sync(t, q)
				x := titles[r.IntN(len(titles))]
				q.clock += minute
				var f map[string]any
				switch k := r.IntN(7); {
				case k == 0 || k == 1:
					sessions[x] = newID()
					f = q.lfact(d, "session.start", q.refs[x], start(sessions[x], 0, true))
				case k == 2 && sessions[x] != "":
					f = q.lfact(d, "session.end", q.refs[x], end(sessions[x], 1, 59*minute, 58*minute, "ended", true, true, true))
					delete(sessions, x)
				case k == 3 && len(view.order) > 0:
					y := view.order[r.IntN(len(view.order))]
					m := view.move(t, q, y, atStart(), "")
					m["place"] = atStart()
					f = q.qfact(d, "queue.move", m, []string{y})
				case k == 4 && view.has(x):
					f = q.remove(d, x)
				case k == 5:
					f = q.policy(d, titleSorting(r.IntN(2) == 0))
				default:
					at := atEnd()
					f = q.qfact(d, "queue.add", map[string]any{"members": []any{view.place(t, q, x, at, "")}, "place": at}, []string{x})
				}
				q.push(d, f)
			}
			commits := q.logCommits(t, at)
			q.restore(t, backup)
			rec, _ := q.startRecovery(t, q.successor(t, p), nil)
			uploads := [][]protocol.RecoveryEntry{entries(commits)}
			if seed%2 == 1 {
				uploads = nil
				for _, c := range commits {
					uploads = append(uploads, c.entries)
				}
			}
			for i, list := range uploads {
				for _, res := range rec.upload(t, list).Results {
					if res.Status != "joined" && res.Status != "known" {
						t.Fatalf("upload %d: %s %s", i, res.ID, res.Status)
					}
				}
			}
			recovered := q.order(t)
			if _, err := q.svc.Rebuild(context.Background(), q.account); err != nil {
				t.Fatalf("the rebuild after the recovery: %v", err)
			}
			checkReplay(t, q.svc, q.account)
			if got := q.order(t); got != recovered {
				t.Fatalf("the rebuild changed the queue from %s to %s", recovered, got)
			}
		})
	}
}

// A rebuild that commits during the warm-up's replay keeps its cache: the
// warm-up's result, replayed from the state before the rebuild, never
// replaces a cache kept for the same head (a rebuild in which no record
// differs commits without moving the head).
func TestWarmupNeverReplacesARebuildsCache(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	p := q.device("P")
	q.seed(t, p, "A", "B", "C")
	q.svc.Store.Close()
	q.reopen(t)
	head := q.headSeq(t)
	paused, release := make(chan struct{}), make(chan struct{})
	warmupReplayed = func(string) {
		close(paused)
		<-release
	}
	defer func() { warmupReplayed = nil }()
	done := make(chan error, 1)
	go func() {
		_, err := WarmQueue(context.Background(), q.svc, q.account)
		done <- err
	}()
	<-paused
	if _, err := q.svc.Rebuild(context.Background(), q.account); err != nil {
		close(release)
		t.Fatal(err)
	}
	rebuilt := q.cache(t)
	if rebuilt == nil || q.headSeq(t) != head {
		close(release)
		t.Fatalf("precondition: the rebuild kept no cache (%v) or moved the head (%d → %d)", rebuilt != nil, head, q.headSeq(t))
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if q.cache(t) != rebuilt {
		t.Fatal("the warm-up replaced the rebuild's cache with one replayed before it")
	}
}
