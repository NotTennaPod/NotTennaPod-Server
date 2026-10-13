package ops

// PROTOCOL §8.1:
// where play was pressed last. An intentional resume of an open session is a
// session.checkpoint with claim: true, a position ownership claim on its own
// writing path; every checkpoint, end, finalization and leave resolves
// against the latest claim on its path.

import (
	"testing"
)

// resume is a claiming checkpoint: the save at READY after a play command,
// with its save evidence (a new save at the ordinal's index, so the save tree
// of a session whose other exports carry none keeps their order).
func resume(session string, ordinal, pos, contribution int64, observed string) map[string]any {
	p := checkpoint(session, ordinal, pos, contribution)
	p["claim"] = true
	if observed != "" {
		p["observed_claim"] = observed
	}
	save := session + "/" + itoa(int(ordinal))
	p["save"] = map[string]any{"id": sid(save), "index": ordinal, "first": sid(save), "baseline": sid(session + "/baseline"), "passive": false}
	return p
}

func (l *listenEnv) positionIs(t *testing.T, pos int64, claim string) {
	t.Helper()
	l.expect(t, "position", `{"claim_id":"`+claim+`","generation_id":"`+l.g+`","position":`+itoa(int(pos))+`}`)
}

// Scenario 5a and example 1 (PROTOCOL §8.1): A starts Monday and
// pauses with the app open; B plays to 60% on Tuesday; A, still offline,
// resumes on Wednesday to 30%. In both reconnection orders every device
// resumes at 30% (A's resume claim) and statistics count A's one session and
// B's one session.
func TestResumeClaimWherePlayWasPressedLast(t *testing.T) {
	for _, aFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "A reconnects first", false: "B reconnects first"}[aFirst], func(t *testing.T) {
			l := newListenEnv(t)
			a, b := l.device("A"), l.device("B")
			sa, sb := newID(), newID()
			day := 24 * 60 * minute
			l.clock += minute
			startA := l.lfact(a, "session.start", l.ref, start(sa, 0, true))
			l.pushOK(t, a, startA, "applied") // Monday, online
			l.clock += 10 * minute
			pauseA := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 10*minute, 10*minute))
			l.pushOK(t, a, pauseA, "applied")
			// Tuesday, B offline (it saw A's pause): plays 0 → 36 min.
			l.clock += day
			startB := l.lfact(b, "session.start", l.ref, map[string]any{"session_id": sb, "start_position": 0, "activity": true,
				"observed_claim": startA["op_id"]}, pauseA["op_id"].(string))
			l.clock += 36 * minute
			pauseB := l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, 36*minute, 36*minute))
			// Wednesday, A offline: resumes its open session at 10 → 18 min.
			l.clock += day
			resumeA := l.lfact(a, "session.checkpoint", l.ref, resume(sa, 2, 10*minute, 10*minute, startA["op_id"].(string)))
			l.clock += 8 * minute
			pause2A := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 3, 18*minute, 18*minute))
			l.clock += day // Thursday: both reconnect
			if aFirst {
				l.pushOK(t, a, resumeA, "applied")
				l.pushOK(t, a, pause2A, "applied")
				l.pushOK(t, b, startB, "applied")
				l.pushOK(t, b, pauseB, "applied")
			} else {
				l.pushOK(t, b, startB, "applied")
				l.pushOK(t, b, pauseB, "applied")
				l.pushOK(t, a, resumeA, "applied")
				l.pushOK(t, a, pause2A, "applied")
			}
			l.positionIs(t, 18*minute, resumeA["op_id"].(string))
			if got := l.playedMS(t); got != 54*minute {
				t.Errorf("played time %d ms, want 54 min (A 18 + B 36)", got)
			}
		})
	}
}

// Example 2 (PROTOCOL §8.1): 30% on A, then a later start on B to 60%: B's claim is later, so 60%, as
// without resume claims.
func TestLaterStartStillWins(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	sa, sb := newID(), newID()
	l.clock += minute
	startA := l.lfact(a, "session.start", l.ref, start(sa, 0, true))
	l.clock += 18 * minute
	pauseA := l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 18*minute, 18*minute))
	l.clock += minute
	startB := l.lfact(b, "session.start", l.ref, start(sb, 0, true))
	l.clock += 36 * minute
	pauseB := l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, 36*minute, 36*minute))
	l.pushOK(t, b, startB, "applied")
	l.pushOK(t, b, pauseB, "applied")
	l.pushOK(t, a, startA, "applied")
	l.pushOK(t, a, pauseA, "applied")
	l.positionIs(t, 36*minute, startB["op_id"].(string))
}

// Example 3 (PROTOCOL §8.1) and scenario 6: a phone left playing (no resume) never takes
// the position back from a later start by its ordinary checkpoints.
func TestPlayingPhoneDoesNotStealBack(t *testing.T) {
	l := newListenEnv(t)
	a, b := l.device("A"), l.device("B")
	sa, sb := newID(), newID()
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, start(sa, 0, true)), "applied")
	l.clock += minute
	startB := l.lfact(b, "session.start", l.ref, start(sb, 0, true))
	l.pushOK(t, b, startB, "applied")
	l.clock += 5 * minute
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, 5*minute, 5*minute)), "applied")
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(sa, 1, 8*minute, 8*minute)), "applied")
	l.positionIs(t, 5*minute, startB["op_id"].(string))
}

// Example 4 (PROTOCOL §8.1): A resets the position while B is paused offline; B then
// resumes and finishes. B's resume claim is later, so B's position stands and
// its completion is valid under it (played, position 0 under B's claim).
// Without the resume claim the reset would bar B's session.
func TestResumeAfterRemoteReset(t *testing.T) {
	for _, resetFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "reset first", false: "B first"}[resetFirst], func(t *testing.T) {
			l := newListenEnv(t)
			a, b := l.device("A"), l.device("B")
			sb := newID()
			l.clock += minute
			startB := l.lfact(b, "session.start", l.ref, start(sb, 0, true))
			l.pushOK(t, b, startB, "applied")
			l.clock += 50 * minute
			pauseB := l.lfact(b, "session.checkpoint", l.ref, checkpoint(sb, 1, 50*minute, 50*minute))
			l.pushOK(t, b, pauseB, "applied")
			// A (online) resets the position.
			l.clock += minute
			reset := l.lfact(a, "episode.reset_position", l.ref, map[string]any{}, pauseB["op_id"].(string))
			// B (offline, unaware) resumes at 50 and plays to the end.
			l.clock += minute
			resumeB := l.lfact(b, "session.checkpoint", l.ref, resume(sb, 2, 50*minute, 50*minute, startB["op_id"].(string)))
			l.clock += 10 * minute
			endB := l.lfact(b, "session.end", l.ref, end(sb, 3, 60*minute, 60*minute, "ended", true, true, true))
			if resetFirst {
				l.pushOK(t, a, reset, "applied")
				l.pushOK(t, b, resumeB, "applied")
				l.pushOK(t, b, endB, "applied")
			} else {
				l.pushOK(t, b, resumeB, "applied")
				l.pushOK(t, b, endB, "applied")
				l.pushOK(t, a, reset, "superseded") // ordered before B's resume and completion
			}
			l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
			l.positionIs(t, 0, resumeB["op_id"].(string))
		})
	}
}

// Example 5 (PROTOCOL §8.1): reset → resume → completion on one phone completes (the
// resume claims again on its path, so the reset no longer bars the session);
// a stale end resolves against its path's latest claim and loses to a later
// start elsewhere.
func TestResetResumeCompletionAndStaleEnd(t *testing.T) {
	t.Run("reset then resume then completion", func(t *testing.T) {
		l := newListenEnv(t)
		a := l.device("A")
		s := newID()
		l.clock += minute
		st := l.lfact(a, "session.start", l.ref, start(s, 0, true))
		l.pushOK(t, a, st, "applied")
		l.clock += 30 * minute
		l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 1, 30*minute, 30*minute)), "applied")
		l.clock += minute
		reset := l.lfact(a, "episode.reset_position", l.ref, map[string]any{})
		l.pushOK(t, a, reset, "applied")
		l.positionIs(t, 0, reset["op_id"].(string))
		// An ordinary checkpoint of the barred session writes nothing.
		l.clock += minute
		l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 2, 30*minute, 30*minute)), "applied")
		l.positionIs(t, 0, reset["op_id"].(string))
		// The person presses play: a resume claim, then the episode ends.
		l.clock += minute
		r := l.lfact(a, "session.checkpoint", l.ref, resume(s, 3, 0, 30*minute, reset["op_id"].(string)))
		l.pushOK(t, a, r, "applied")
		l.positionIs(t, 0, r["op_id"].(string))
		l.clock += 60 * minute
		l.pushOK(t, a, l.lfact(a, "session.end", l.ref, end(s, 4, 60*minute, 60*minute, "ended", true, true, true)), "applied")
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
		l.positionIs(t, 0, r["op_id"].(string))
	})
	t.Run("stale end after a later start", func(t *testing.T) {
		l := newListenEnv(t)
		a, b := l.device("A"), l.device("B")
		s, sb := newID(), newID()
		l.clock += minute
		st := l.lfact(a, "session.start", l.ref, start(s, 0, true))
		l.pushOK(t, a, st, "applied")
		l.clock += minute
		r := l.lfact(a, "session.checkpoint", l.ref, resume(s, 1, minute, minute, st["op_id"].(string)))
		l.pushOK(t, a, r, "applied")
		l.clock += minute
		startB := l.lfact(b, "session.start", l.ref, start(sb, 20*minute, true))
		l.clock += 58 * minute
		endA := l.lfact(a, "session.end", l.ref, end(s, 2, 59*minute+45*second, 59*minute, "ended", true, true, true))
		l.pushOK(t, b, startB, "applied")
		l.pushOK(t, a, endA, "applied")
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"unplayed"}`)
		l.positionIs(t, 20*minute, startB["op_id"].(string))
	})
}

// A lazy finalization and a leave name the path's latest claim (the resume):
// they complete while it owns the position, and lose to a later claim
// ordered before them (PROTOCOL §8.1).
func TestFinalizationAndLeaveUnderResumeClaim(t *testing.T) {
	setup := func(t *testing.T) (*listenEnv, *device, string, string, string) {
		l := newListenEnv(t)
		a := l.device("A")
		s := newID()
		l.clock += minute
		st := l.lfact(a, "session.start", l.ref, start(s, 0, true))
		l.pushOK(t, a, st, "applied")
		l.clock += minute
		l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 1, 30*minute, 30*minute)), "applied")
		l.clock += minute
		r := l.lfact(a, "session.checkpoint", l.ref, resume(s, 2, 30*minute, 30*minute, st["op_id"].(string)))
		l.pushOK(t, a, r, "applied")
		l.clock += 29 * minute
		l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 3, 59*minute+50*second, 59*minute)), "applied")
		return l, a, s, st["op_id"].(string), r["op_id"].(string)
	}
	t.Run("finalization naming the resume completes", func(t *testing.T) {
		l, a, s, _, r := setup(t)
		l.clock += minute
		l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(s, 3, r, true)), "applied")
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
	})
	t.Run("finalization naming the start means the resume (start-ID alias)", func(t *testing.T) {
		l, a, s, st, _ := setup(t)
		l.clock += minute
		l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(s, 3, st, true)), "applied")
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
	})
	t.Run("a later start elsewhere supersedes the finalization", func(t *testing.T) {
		l, a, s, _, r := setup(t)
		b := l.device("B")
		l.clock += minute
		startB := l.lfact(b, "session.start", l.ref, start(newID(), 10*minute, true))
		l.clock += minute
		fin := l.lfact(a, "session.finalize", l.ref, finalize(s, 3, r, true))
		l.pushOK(t, a, fin, "applied")
		l.pushOK(t, b, startB, "applied")
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"unplayed"}`)
		l.positionIs(t, 10*minute, startB["op_id"].(string))
	})
	t.Run("a leave observing the resume completes, a stale one does not", func(t *testing.T) {
		l, a, _, st, r := setup(t)
		c := l.device("C")
		l.clock += minute
		l.pushOK(t, c, l.lfact(c, "episode.leave", l.ref, leave(newID(), "switched", 59*minute+50*second, st, true)), "superseded")
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"unplayed"}`)
		l.clock += minute
		l.pushOK(t, a, l.lfact(a, "episode.leave", l.ref, leave(newID(), "switched", 59*minute+50*second, r, true)), "applied")
		l.expect(t, "play_state", `{"generation_id":"`+l.g+`","play_state":"played"}`)
	})
}

func leave(transition, reason string, pos int64, observed string, completed bool) map[string]any {
	p := map[string]any{"transition_id": transition, "reason": reason, "position": pos, "completed": completed,
		"threshold_ms": int64(30_000), "remove_from_queue": false, "history": true}
	if observed != "" {
		p["observed_claim"] = observed
	}
	return p
}

// PROTOCOL §8.1: claims belong to a writing path. Original A and an undetected copy B
// both continue session S (same baseline: no inferred claim); B resumes and
// claims; A's later ordinary checkpoint resolves against A's own latest claim
// (the start), so it cannot take B's position, in both arrival orders.
func TestResumeClaimBelongsToItsPath(t *testing.T) {
	for _, copyFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "original first", true: "copy first"}[copyFirst], func(t *testing.T) {
			l := newListenEnv(t)
			a := l.device("A")
			s, _, b0 := copyScenario(t, l, a)
			b := l.copyDevice(a, "B")
			l.clock += 10 * second
			bResume := l.lfact(b, "session.checkpoint", l.ref, withSave(resume(s, 2, 300*second, 300*second, ""), saveMeta("sB4", 4, "sB4", b0, false)))
			l.clock += 20 * second
			bPause := l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 3, 320*second, 320*second), saveMeta("sB5", 5, "sB5", b0, false)))
			l.clock += 30 * second
			aSave := l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 360*second, 360*second), saveMeta("sA6", 6, "sA4", b0, false)))
			if copyFirst {
				l.pushOK(t, b, bResume, "applied")
				l.pushOK(t, b, bPause, "applied")
				l.pushOK(t, a, aSave, "applied")
			} else {
				l.pushOK(t, a, aSave, "applied")
				l.pushOK(t, b, bResume, "applied")
				l.pushOK(t, b, bPause, "applied")
			}
			l.positionIs(t, 320*second, bResume["op_id"].(string))
		})
	}
}

// PROTOCOL §8.1: malformed claim fields are rejected; every other failed check keeps
// its outcome and grants no ownership.
func TestResumeClaimValidation(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	s := newID()
	l.clock += minute
	st := l.lfact(a, "session.start", l.ref, start(s, 0, true))
	l.pushOK(t, a, st, "applied")
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 1, minute, minute)), "applied")
	bad := []struct {
		name    string
		payload func() map[string]any
	}{
		{"not a boolean", func() map[string]any { p := checkpoint(s, 2, minute, minute); p["claim"] = "yes"; return p }},
		{"observed without claim", func() map[string]any {
			p := checkpoint(s, 2, minute, minute)
			p["observed_claim"] = st["op_id"]
			return p
		}},
		{"observed with claim false", func() map[string]any {
			p := checkpoint(s, 2, minute, minute)
			p["claim"], p["observed_claim"] = false, st["op_id"]
			return p
		}},
		{"observed not a UUID", func() map[string]any { return resume(s, 2, minute, minute, "not-a-uuid") }},
		{"no activity", func() map[string]any { p := resume(s, 2, minute, minute, ""); p["activity"] = false; return p }},
		{"passive save", func() map[string]any {
			return withSave(resume(s, 2, minute, minute, ""), saveMeta("v2", 2, "v2", newID(), true))
		}},
		{"no new save", func() map[string]any {
			return withSave(resume(s, 2, minute, minute, ""), saveMeta("v2", 2, "", newID(), false))
		}},
		{"no save evidence", func() map[string]any {
			p := resume(s, 2, minute, minute, "")
			delete(p, "save")
			return p
		}},
	}
	for _, c := range bad {
		l.clock += second
		l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, c.payload()), "rejected/invalid_payload")
	}
	// A valid claim false is an ordinary checkpoint.
	l.clock += second
	ord := checkpoint(s, 2, 2*minute, 2*minute)
	ord["claim"] = false
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, ord), "applied")
	l.positionIs(t, 2*minute, st["op_id"].(string))
	// An ordinal conflict keeps its outcome and claims nothing.
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, resume(s, 2, 9*minute, 2*minute, "")), "not_applied/session_ordinal_conflict")
	l.positionIs(t, 2*minute, st["op_id"].(string))
	// So does a sealed session.
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.end", l.ref, end(s, 3, 3*minute, 3*minute, "switched", false, true, true)), "applied")
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, resume(s, 4, 9*minute, 3*minute, "")), "not_applied/session_sealed")
	l.positionIs(t, 3*minute, st["op_id"].(string))
	// And a closed generation.
	s2 := newID()
	l.clock += second
	st2 := l.lfact(a, "session.start", l.ref, start(s2, 0, true))
	l.pushOK(t, a, st2, "applied")
	l.clock += second
	l.pushOK(t, a, l.fact(a, "podcast.unsubscribe", []string{a.last}, podTarget(l.p, l.g), map[string]any{}), "applied")
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, resume(s2, 1, minute, minute, "")), "not_applied/generation_closed")
}

// PROTOCOL §8.1, scenario 1a's literal timeline: the shared
// episode E (60 min) and episode F, rewind-on-resume off. After both phones
// reconnect, in both orders: E at 20:00 (P pressed play last), E's played
// time 20:00 (P1's 20:00 plus C1's residual 0 after C's reset), E's last
// played and history time Wednesday 09:10 (so 20:00 in October's
// statistics), and F's played time 0:50 (one session, Android's
// below-baseline rule).
func TestScenario1aSharedEpisodeTimeline(t *testing.T) {
	at := func(day, h, m, s int64) int64 {
		// Monday 2026-10-05 00:00 UTC is 1_791_158_400_000 ms.
		return 1_791_158_400_000 + day*24*3600_000 + h*3600_000 + m*60_000 + s*1000
	}
	for _, pFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "P reconnects first", false: "C reconnects first"}[pFirst], func(t *testing.T) {
			l := newListenEnv(t)
			l.replayCheck = true // every prefix of the 1a fixture (PROTOCOL §7.6)
			p, c := l.device("P"), l.device("C")
			eRef, fRef := l.ref, l.episodeRef(l.p, l.g, "F")
			p1, c1, f1 := newID(), newID(), newID()
			var pFacts, cFacts []map[string]any
			add := func(list *[]map[string]any, when int64, f func() map[string]any) map[string]any {
				l.clock = when
				x := f()
				*list = append(*list, x)
				return x
			}
			// Monday: P plays E from 0:00 to 10:00 and pauses (P1, claim c1).
			c1Start := add(&pFacts, at(0, 9, 0, 0), func() map[string]any { return l.lfact(p, "session.start", eRef, start(p1, 0, true)) })
			add(&pFacts, at(0, 9, 10, 0), func() map[string]any {
				return l.lfact(p, "session.checkpoint", eRef, checkpoint(p1, 1, 10*minute, 10*minute))
			})
			// Tuesday: C plays E from 0:00 to 30:00 (C1, claim c2), then resets
			// statistics, observing C1 at 30:00 but not P1.
			add(&cFacts, at(1, 9, 0, 0), func() map[string]any { return l.lfact(c, "session.start", eRef, start(c1, 0, true)) })
			add(&cFacts, at(1, 9, 30, 0), func() map[string]any {
				return l.lfact(c, "session.checkpoint", eRef, checkpoint(c1, 1, 30*minute, 30*minute))
			})
			add(&cFacts, at(1, 12, 0, 0), func() map[string]any {
				return l.fact(c, "statistics.reset", []string{c.last}, []any{}, map[string]any{})
			})
			// Wednesday: P resumes E at 10:00 (claim c3) and pauses at 20:00.
			c3 := add(&pFacts, at(2, 9, 0, 0), func() map[string]any {
				return l.lfact(p, "session.checkpoint", eRef, resume(p1, 2, 10*minute, 10*minute, c1Start["op_id"].(string)))
			})
			add(&pFacts, at(2, 9, 10, 0), func() map[string]any {
				return l.lfact(p, "session.checkpoint", eRef, checkpoint(p1, 3, 20*minute, 20*minute))
			})
			// Wednesday 10:00: F at 1×: start at 1:40, play to 3:20, pause,
			// resume, rewind to 0:50, play to 2:30, pause (200 s of playback).
			add(&pFacts, at(2, 10, 0, 0), func() map[string]any { return l.lfact(p, "session.start", fRef, start(f1, 100*second, true)) })
			add(&pFacts, at(2, 10, 1, 40), func() map[string]any {
				return l.lfact(p, "session.checkpoint", fRef, checkpoint(f1, 1, 200*second, 100*second))
			})
			add(&pFacts, at(2, 10, 2, 0), func() map[string]any {
				return l.lfact(p, "session.checkpoint", fRef, resume(f1, 2, 200*second, 100*second, ""))
			})
			add(&pFacts, at(2, 10, 2, 5), func() map[string]any {
				return l.lfact(p, "session.checkpoint", fRef, checkpoint(f1, 3, 55*second, 100*second)) // below the baseline: kept
			})
			add(&pFacts, at(2, 10, 3, 40), func() map[string]any {
				return l.lfact(p, "session.checkpoint", fRef, checkpoint(f1, 4, 150*second, 50*second))
			})
			l.clock = at(3, 9, 0, 0) // Thursday: both reconnect
			if pFirst {
				l.pushAll(t, p, pFacts...)
				l.pushAll(t, c, cFacts...)
			} else {
				l.pushAll(t, c, cFacts...)
				l.pushAll(t, p, pFacts...)
			}
			l.positionIs(t, 20*minute, c3["op_id"].(string))
			if got := l.playedMS(t); got != 20*minute {
				t.Errorf("E played %d ms, want 20:00", got)
			}
			wed := itoa(int(at(2, 9, 10, 0)))
			l.expect(t, "last_played", `{"generation_id":"`+l.g+`","time":`+wed+`}`)
			l.expect(t, "history", `{"generation_id":"`+l.g+`","time":`+wed+`}`)
			fEp := fRef["episode_id"].(string)
			if got := l.records()["episode/"+fEp+"/played_time"]; got != `{"generation_id":"`+l.g+`","played_ms":50000}` {
				t.Errorf("F played %s, want 0:50", got)
			}
		})
	}
}

// pushAll pushes facts from d one request each, all applied.
func (e *env) pushAll(t *testing.T, d *device, facts ...map[string]any) {
	t.Helper()
	for _, f := range facts {
		if r := e.push(d, f); outcome(r[0]) != "applied" {
			t.Fatalf("%s: %s", f["type"], outcome(r[0]))
		}
	}
}

// A claiming checkpoint's save must not be covered on its writing path: a
// repeated save, or one at or before a save the path already exported, is
// rejected invalid_payload. A non-claiming checkpoint keeps optional save
// metadata.
func TestResumeClaimNeedsAnUncoveredSave(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	s, b0 := newID(), newID()
	l.clock += minute
	st := start(s, 0, true)
	st["baseline"] = b0
	l.pushOK(t, a, l.lfact(a, "session.start", l.ref, st), "applied")
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 1, minute, minute), saveMeta("c5", 5, "c1", b0, false))), "applied")
	for name, save := range map[string]map[string]any{
		"the same save":   saveMeta("c5", 5, "c5", b0, false),
		"an earlier save": saveMeta("c4", 4, "c4", b0, false),
	} {
		l.clock += second
		r := l.lfact(a, "session.checkpoint", l.ref, withSave(resume(s, 2, minute, minute, ""), save))
		if res := l.push(a, r); outcome(res[0]) != "rejected/invalid_payload" {
			t.Errorf("%s: %s, want rejected/invalid_payload", name, outcome(res[0]))
		}
	}
	l.clock += second
	r := l.lfact(a, "session.checkpoint", l.ref, withSave(resume(s, 2, minute, minute, ""), saveMeta("c6", 6, "c6", b0, false)))
	l.pushOK(t, a, r, "applied")
	l.positionIs(t, minute, r["op_id"].(string))
	// A non-claiming checkpoint without save metadata is still valid.
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 3, 2*minute, 2*minute)), "applied")
	// A covered save on a session whose ordinal check fails keeps that
	// outcome.
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(resume(s, 3, minute, minute, ""), saveMeta("c5", 5, "c5", b0, false))),
		"not_applied/session_ordinal_conflict")
}
