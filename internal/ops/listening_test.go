package ops

// Listening, history and statistics (PROTOCOL §8).

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

const minute = int64(60_000)

// lfact builds a fact that depends on d's previous fact (the client
// obligation) and on deps.
func (e *env) lfact(d *device, typ string, ref map[string]any, payload map[string]any, deps ...string) map[string]any {
	if d.last != "" {
		deps = append(deps, d.last)
	}
	slices.Sort(deps)
	deps = slices.Compact(deps)
	targets := []any{}
	if ref != nil {
		payload["episode"] = ref
		targets = epTarget(ref)
	}
	// Decode as the server does (integers as JSON numbers).
	b, err := contract.Canonical(payload)
	if err != nil {
		e.t.Fatal(err)
	}
	v, err := contract.DecodeJSON(b)
	if err != nil {
		e.t.Fatal(err)
	}
	return e.fact(d, typ, deps, targets, v.(map[string]any))
}

func start(session string, pos int64, activity bool) map[string]any {
	return map[string]any{"session_id": session, "start_position": pos, "activity": activity}
}

func checkpoint(session string, ordinal, pos, contribution int64) map[string]any {
	return map[string]any{"session_id": session, "ordinal": ordinal, "position": pos, "contribution": contribution, "activity": true}
}

func end(session string, ordinal, pos, contribution int64, reason string, completed, activity, history bool) map[string]any {
	return map[string]any{"session_id": session, "ordinal": ordinal, "position": pos, "contribution": contribution, "activity": activity,
		"reason": reason, "completed": completed, "threshold_ms": int64(30_000), "remove_from_queue": false, "history": history}
}

func finalize(session string, ordinal int64, claim string, completed bool) map[string]any {
	return map[string]any{"session_id": session, "ordinal": ordinal, "claim_id": claim, "completed": completed,
		"threshold_ms": int64(30_000), "remove_from_queue": false, "history": true}
}

type listenEnv struct {
	*env
	p, g string
	ref  map[string]any
	ep   string
}

func newListenEnv(t *testing.T) *listenEnv {
	e := newEnv(t)
	p, g, _ := subscribeOne(t, e, e.device("A"))
	ref := e.episodeRef(p, g, "one")
	return &listenEnv{env: e, p: p, g: g, ref: ref, ep: ref["episode_id"].(string)}
}

func (l *listenEnv) record(group string) string { return l.records()["episode/"+l.ep+"/"+group] }

func (l *listenEnv) expect(t *testing.T, group, want string) {
	t.Helper()
	if got := l.record(group); got != want {
		t.Fatalf("%s = %s, want %s", group, got, want)
	}
}

// Scenario 8a: A pauses at 10, B continues to 30, A adopts 30 and resumes to
// 45. Everyone resumes at 45; statistics count 0→10 and 30→45 on A and 10→30
// on B, never the adopted jump.
func TestOwnershipHandoff(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	sa, sb, sa2 := newID(), newID(), newID()
	l.clock += minute
	startA := l.lfact(a, "session.start", l.ref, start(sa, 0, true))
	l.pushOK(t, a, startA, "applied")
	l.clock += 10 * minute
	pauseA := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 10*minute, 10*minute))
	l.pushOK(t, a, pauseA, "applied")
	l.clock += minute
	startB := l.lfact(b, "session.start", l.ref, map[string]any{"session_id": sb, "start_position": 10 * minute, "activity": true, "observed_claim": startA["op_id"]}, pauseA["op_id"].(string))
	l.pushOK(t, b, startB, "applied")
	l.clock += 20 * minute
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, 30*minute, 20*minute)), "applied")
	l.expect(t, "position", `{"claim_id":"`+startB["op_id"].(string)+`","generation_id":"`+l.g+`","position":1800000}`)
	// A adopts 30 while paused: seals its session at the pre-adoption position.
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.end", l.ref, end(sa, 2, 10*minute, 10*minute, "superseded", false, false, false), b.last), "applied")
	startA2 := l.lfact(a, "session.start", l.ref, start(sa2, 30*minute, true))
	l.pushOK(t, a, startA2, "applied")
	l.clock += 15 * minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa2, 1, 45*minute, 15*minute)), "applied")
	l.expect(t, "position", `{"claim_id":"`+startA2["op_id"].(string)+`","generation_id":"`+l.g+`","position":2700000}`)
	l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":2700000}`)
	l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"unplayed"}`)
}

// Scenario 8b: B takes over before A's lazy finalization in event order without
// observing it; in both delivery orders the episode stays unplayed at B's position.
func TestTakeoverBeforeLazyFinalization(t *testing.T) {
	for _, finalizationFirst := range []bool{true, false} {
		l := newListenEnv(t)
		a, b := l.device("A"), l.device("B")
		sa, sb := newID(), newID()
		l.clock += minute
		startA := l.lfact(a, "session.start", l.ref, start(sa, 0, true))
		l.pushOK(t, a, startA, "applied")
		l.clock += 59 * minute
		pause := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 59*minute+45_000, 59*minute+45_000))
		l.pushOK(t, a, pause, "applied")
		takeoverTime := l.clock + minute
		finalizeTime := l.clock + 2*minute
		l.clock = takeoverTime
		takeover := l.lfact(b, "session.start", l.ref, start(sb, 20*minute, true), pause["op_id"].(string))
		l.clock = finalizeTime
		fin := l.lfact(a, "session.finalize", l.ref, finalize(sa, 1, startA["op_id"].(string), true))
		if finalizationFirst {
			l.pushOK(t, a, fin, "applied") // the bundle fired at acceptance
			l.pushOK(t, b, takeover, "applied")
		} else {
			l.pushOK(t, b, takeover, "applied")
			l.pushOK(t, a, fin, "superseded") // its completion lost; its history mark counts
		}
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"unplayed"}`)
		l.expect(t, "position", `{"claim_id":"`+takeover["op_id"].(string)+`","generation_id":"`+l.g+`","position":1200000}`)
	}
}

// Scenario 31: a new session that observed a legitimate completion does not
// invalidate it; replaying a played episode stays played.
func TestReplayAfterCompletionStaysPlayed(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	sa, sb := newID(), newID()
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, start(sa, 0, true)), "applied")
	l.clock += 60 * minute
	done := l.lfact(a, "session.end", l.ref, end(sa, 1, 60*minute, 60*minute, "ended", true, true, true))
	l.pushOK(t, a, done, "applied")
	l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
	l.clock += minute
	l.pushOK(t, b, l.lfact(b, "session.start", l.ref, start(sb, 0, true), done["op_id"].(string)), "applied")
	l.clock += minute
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, minute, minute)), "applied")
	l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
	l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":3660000}`)
}

// An explicit reset bars later checkpoints of an older session; its listening
// still counts.
func TestResetPositionBarsOlderSession(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	sa := newID()
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, start(sa, 0, true)), "applied")
	l.clock += 5 * minute
	cp := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 5*minute, 5*minute))
	l.pushOK(t, a, cp, "applied")
	l.clock += minute
	reset := l.lfact(b, "episode.reset_position", l.ref, map[string]any{}, cp["op_id"].(string))
	l.pushOK(t, b, reset, "applied")
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 2, 7*minute, 7*minute)), "applied")
	l.expect(t, "position", `{"claim_id":"`+reset["op_id"].(string)+`","generation_id":"`+l.g+`","position":0}`)
	l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":420000}`)
}

// After adopting another device's position, a finalization
// completes under that claim, and the adopted owner's next checkpoint still owns.
func TestFinalizationUnderAdoptedClaim(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	sa, sb := newID(), newID()
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, start(sa, 0, true)), "applied")
	l.clock += 10 * minute
	pa := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 10*minute, 10*minute))
	l.pushOK(t, a, pa, "applied")
	l.clock += minute
	startB := l.lfact(b, "session.start", l.ref, start(sb, 10*minute, true), pa["op_id"].(string))
	l.pushOK(t, b, startB, "applied")
	l.clock += 48 * minute
	pb := l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, 58*minute+40_000, 48*minute+40_000))
	l.pushOK(t, b, pb, "applied")
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.end", l.ref, end(sa, 2, 10*minute, 10*minute, "superseded", false, false, false), pb["op_id"].(string)), "applied")
	l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(sa, 2, startB["op_id"].(string), true)), "applied")
	l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
	l.expect(t, "position", `{"claim_id":"`+startB["op_id"].(string)+`","generation_id":"`+l.g+`","position":0}`)
	l.clock += minute
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 2, 59*minute, 49*minute)), "applied")
	l.expect(t, "position", `{"claim_id":"`+startB["op_id"].(string)+`","generation_id":"`+l.g+`","position":3540000}`)
}

// Scenarios 36/37: reset at ordinal 2 (20 min), rewind to 5 min at ordinal 3,
// reset again, ordinal 4 at 15 min contributes 10 min.
func TestResetCutsByOrdinal(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	s := newID()
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, start(s, 0, true)), "applied")
	for i, c := range []int64{10, 20} {
		l.clock += 10 * minute
		l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, int64(i+1), c*minute, c*minute)), "applied")
	}
	l.pushOK(t, a, l.lfact(a, "statistics.reset", nil, map[string]any{}), "applied")
	l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":0}`)
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 3, 5*minute, 5*minute)), "applied")
	l.pushOK(t, a, l.lfact(a, "statistics.reset", nil, map[string]any{}), "applied")
	l.clock += 10 * minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 4, 15*minute, 15*minute)), "applied")
	l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":600000}`)
}

// Scenario 8c: two offline phones each reset after listening; neither's
// listening before its own reset counts, in both upload orders.
func TestConcurrentResets(t *testing.T) {
	for _, aFirst := range []bool{true, false} {
		l := newListenEnv(t)
		a, b := l.device("A"), l.device("B")
		sa, sb := newID(), newID()
		l.clock += minute
		var fa, fb []map[string]any
		fa = append(fa, l.lfact(a, "session.start", l.ref, start(sa, 0, true)))
		fb = append(fb, l.lfact(b, "session.start", l.ref, start(sb, 0, true)))
		l.clock += 10 * minute
		fa = append(fa, l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 10*minute, 10*minute)))
		fb = append(fb, l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, 20*minute, 20*minute)))
		fa = append(fa, l.lfact(a, "statistics.reset", nil, map[string]any{}))
		fb = append(fb, l.lfact(b, "statistics.reset", nil, map[string]any{}))
		if aFirst {
			l.push(a, fa...)
			l.push(b, fb...)
		} else {
			l.push(b, fb...)
			l.push(a, fa...)
		}
		l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":0}`)
	}
}

// History cuts suppress exactly what they observed: unobserved concurrent
// activity survives, a restore cancels only its removal, a clear never
// changes last played, and a late finalization moves history only.
func TestHistoryCuts(t *testing.T) {
	l := newListenEnv(t)
	a, b, c := l.device("A"), l.device("B"), l.device("C")
	sa, sc := newID(), newID()
	l.clock += minute
	t1 := l.clock
	startA := l.lfact(a, "session.start", l.ref, start(sa, 0, true))
	l.pushOK(t, a, startA, "applied")
	// C listens concurrently, unseen by B.
	l.clock += minute
	t2 := l.clock
	startC := l.lfact(c, "session.start", l.ref, start(sc, 0, true))
	l.clock += minute
	removal := l.lfact(b, "history.remove", l.ref, map[string]any{}, startA["op_id"].(string))
	l.pushOK(t, b, removal, "applied")
	l.expect(t, "history", `{"generation_id":"`+l.g+`","time":null}`)
	l.pushOK(t, c, startC, "applied")
	l.expect(t, "history", `{"generation_id":"`+l.g+`","time":`+msText(t2)+`}`)
	l.expect(t, "last_played", `{"generation_id":"`+l.g+`","time":`+msText(t2)+`}`)
	l.clock += minute
	restore := l.lfact(b, "history.restore", l.ref, map[string]any{"removal": removal["op_id"], "time": t1})
	l.pushOK(t, b, restore, "applied")
	l.pushOK(t, b, l.lfact(b, "history.restore", l.ref, map[string]any{"removal": removal["op_id"], "time": t1}), "not_applied/already_restored")
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "history.clear", nil, map[string]any{}, c.last, b.last), "applied")
	l.expect(t, "history", `{"generation_id":"`+l.g+`","time":null}`)
	l.expect(t, "last_played", `{"generation_id":"`+l.g+`","time":`+msText(t2)+`}`)
	// A finalization days later moves history, not last played.
	l.clock += 3 * 24 * 60 * minute
	late := l.clock
	l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(sa, 0+1, startA["op_id"].(string), false)), "rejected/invalid_payload") // no ordinal 1 yet
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, map[string]any{"session_id": sa, "ordinal": int64(1), "position": minute, "contribution": minute, "activity": false}), "applied")
	l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(sa, 1, startA["op_id"].(string), false)), "applied")
	l.expect(t, "history", `{"generation_id":"`+l.g+`","time":`+msText(late)+`}`)
	l.expect(t, "last_played", `{"generation_id":"`+l.g+`","time":`+msText(t2)+`}`)
}

func msText(n int64) string { return js(n) }

// Coverage is by causal past, so author-sequence gaps and
// rejected predecessors are exact.
func TestCutCoverageIsCausalPast(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	sa := newID()
	l.clock += minute
	first := l.lfact(a, "session.start", l.ref, start(sa, 0, true))
	l.pushOK(t, a, first, "applied")
	firstSeq := a.author
	// A's sequence firstSeq+4 arrives before its firstSeq+1 (both follow first).
	l.clock += minute
	a.author = firstSeq + 3
	five := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, minute, minute))
	l.pushOK(t, a, five, "applied")
	l.clock += minute
	l.pushOK(t, b, l.lfact(b, "history.remove", l.ref, map[string]any{}, five["op_id"].(string)), "applied")
	l.expect(t, "history", `{"generation_id":"`+l.g+`","time":null}`)
	a.author, a.last = firstSeq, first["op_id"].(string)
	l.clock += minute
	gapTime := l.clock
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, start(newID(), 0, true)), "applied")
	l.expect(t, "history", `{"generation_id":"`+l.g+`","time":`+msText(gapTime)+`}`)
	// A cut after a rejected predecessor observes that predecessor's frontier.
	l.clock += minute
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, map[string]any{"session_id": "not-a-uuid"}, a.last), "rejected/invalid_payload")
	l.pushOK(t, b, l.lfact(b, "history.remove", l.ref, map[string]any{}), "applied")
	l.expect(t, "history", `{"generation_id":"`+l.g+`","time":null}`)
}

// Admission faults (PROTOCOL §8).
func TestSessionAdmissionFaults(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	s := newID()
	l.clock += minute
	st := l.lfact(a, "session.start", l.ref, start(s, 0, true))
	l.pushOK(t, a, st, "applied")
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 1, minute, minute)), "applied")
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, checkpoint(s, 2, minute, minute)), "not_applied/foreign_session")
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 1, 2*minute, 2*minute)), "not_applied/session_ordinal_conflict")
	l.pushOK(t, a, l.lfact(a, "session.end", l.ref, end(s, 2, minute, minute, "ended", false, true, true)), "applied")
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 3, minute, minute)), "not_applied/session_sealed")
	l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(s, 2, st["op_id"].(string), false)), "applied")
	l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(s, 2, st["op_id"].(string), false)), "not_applied/session_finalized")
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(newID(), 1, minute, minute)), "rejected/invalid_payload")
	other := l.episodeRef(l.p, l.g, "two")
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", other, checkpoint(s, 4, minute, minute)), "rejected/invalid_payload")
	l.pushOK(t, a, l.lfact(a, "history.restore", l.ref, map[string]any{"removal": st["op_id"], "time": int64(0)}), "rejected/invalid_payload")
}

// Arrival order never changes listening projections: several offline devices'
// sessions, takeovers, completions, finalizations, resets, clears and removals
// arrive in different interleavings (each device's facts in order).
func TestListeningArrivalOrderDoesNotChangeProjections(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 22))
	for trial := 0; trial < 8; trial++ {
		type plan struct {
			device string
			facts  []map[string]any
		}
		base := newEnv(t)
		a := base.device("A")
		sub := base.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
		p := contract.PodcastID(base.namespace, sub["op_id"].(string))
		g := contract.GenerationID(base.namespace, p, sub["op_id"].(string))
		var plans []plan
		for _, name := range []string{"B", "C", "D"} {
			d := base.device(name)
			d.last = ""
			var facts []map[string]any
			for s := 0; s < 1+r.IntN(3); s++ {
				ref := base.episodeRef(p, g, fmt.Sprintf("ep-%d", r.IntN(2)))
				base.clock = 1_700_000_000_000 + r.Int64N(100*minute)
				session := newID()
				first := len(facts) == 0
				deps := []string{}
				if first {
					deps = append(deps, sub["op_id"].(string))
				}
				st := base.lfact(d, "session.start", ref, start(session, r.Int64N(10)*minute, true), deps...)
				facts = append(facts, st)
				pos, contribution, ordinal := int64(0), int64(0), int64(0)
				for c := 0; c < r.IntN(4); c++ {
					base.clock += minute
					ordinal++
					pos += (r.Int64N(5) - 1) * minute
					pos = max(pos, 0)
					contribution = max(contribution, pos)
					facts = append(facts, base.lfact(d, "session.checkpoint", ref, checkpoint(session, ordinal, pos, contribution)))
				}
				switch r.IntN(5) {
				case 0:
					ordinal++
					facts = append(facts, base.lfact(d, "session.end", ref, end(session, ordinal, pos, contribution, "ended", true, true, true)))
				case 1:
					if ordinal > 0 {
						facts = append(facts, base.lfact(d, "session.finalize", ref, finalize(session, ordinal, st["op_id"].(string), r.IntN(2) == 0)))
					}
				case 2:
					facts = append(facts, base.lfact(d, "history.remove", ref, map[string]any{}))
				case 3:
					facts = append(facts, base.lfact(d, "statistics.reset", nil, map[string]any{}))
				default:
					facts = append(facts, base.lfact(d, "episode.reset_position", ref, map[string]any{}))
				}
			}
			if r.IntN(3) == 0 {
				facts = append(facts, base.lfact(d, "history.clear", nil, map[string]any{}))
			}
			plans = append(plans, plan{name, facts})
		}
		var reference map[string]string
		for order := 0; order < 4; order++ {
			e := newEnvWithAccount(t, base)
			e.push(e.device("A"), sub)
			next := make([]int, len(plans))
			for {
				var ready []int
				for i, pl := range plans {
					if next[i] < len(pl.facts) {
						ready = append(ready, i)
					}
				}
				if len(ready) == 0 {
					break
				}
				i := ready[r.IntN(len(ready))]
				pl := plans[i]
				e.clock = 1_700_000_000_000 + 200*minute
				if res := e.push(e.device(pl.device), pl.facts[next[i]]); res[0].Status != "accepted" || outcome(res[0]) == "rejected" {
					t.Fatalf("trial %d: %+v", trial, res[0])
				}
				next[i]++
			}
			got := e.records()
			if reference == nil {
				reference = got
				continue
			}
			for k, v := range reference {
				if got[k] != v {
					t.Fatalf("trial %d order %d: %s = %s, first order %s", trial, order, k, got[k], v)
				}
			}
			if len(got) != len(reference) {
				t.Fatalf("trial %d order %d: %d records, first order %d", trial, order, len(got), len(reference))
			}
		}
	}
}

// Ordinals, not rank order, choose a session's latest save: a later save whose
// fact is ranked earlier (it does not depend on the previous save, against
// the client obligation, and its clock is behind) still determines the position.
func TestSessionOrdinalsChooseTheLatestSave(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	s := newID()
	l.clock += 10 * minute
	st := l.lfact(a, "session.start", l.ref, start(s, 0, true))
	l.pushOK(t, a, st, "applied")
	l.clock += 5 * minute
	one := l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 1, 5*minute, 5*minute))
	l.pushOK(t, a, one, "applied")
	l.clock -= 2 * minute // a clock step backwards
	two := l.fact(a, "session.checkpoint", []string{st["op_id"].(string)}, epTarget(l.ref), l.lfactPayload(checkpoint(s, 2, 7*minute, 7*minute)))
	l.clock += 10 * minute
	l.pushOK(t, a, two, "applied")
	l.expect(t, "position", `{"claim_id":"`+st["op_id"].(string)+`","generation_id":"`+l.g+`","position":420000}`)
	l.expect(t, "played_time", `{"generation_id":"`+l.g+`","played_ms":420000}`)
}

// lfactPayload decodes a payload with l.ref as the server does.
func (l *listenEnv) lfactPayload(payload map[string]any) map[string]any {
	payload["episode"] = l.ref
	b, err := contract.Canonical(payload)
	if err != nil {
		l.t.Fatal(err)
	}
	v, err := contract.DecodeJSON(b)
	if err != nil {
		l.t.Fatal(err)
	}
	return v.(map[string]any)
}

// A malformed restore naming an unknown removal is rejected, not waited for.
func TestMalformedRestoreRejected(t *testing.T) {
	for _, broken := range []string{"episode", "targets"} {
		l := newListenEnv(t)
		a := l.device("A")
		f := l.lfact(a, "history.restore", l.ref, map[string]any{"removal": newID(), "time": int64(0)})
		if broken == "episode" {
			f["payload"].(map[string]any)["episode"] = "not an episode reference"
		} else {
			f["targets"] = []any{}
		}
		if r := l.push(a, f); outcome(r[0]) != "rejected/invalid_payload" {
			t.Fatalf("malformed %s: status %s, outcome %s, missing %v", broken, r[0].Status, outcome(r[0]), r[0].Missing)
		}
	}
	// A valid restore of an unknown removal waits for it.
	l := newListenEnv(t)
	a := l.device("A")
	removal := newID()
	if r := l.push(a, l.lfact(a, "history.restore", l.ref, map[string]any{"removal": removal, "time": int64(0)})); r[0].Status != "dependency_missing" || len(r[0].Missing) != 1 || r[0].Missing[0] != removal {
		t.Fatalf("unknown removal: %+v", r[0])
	}
}
