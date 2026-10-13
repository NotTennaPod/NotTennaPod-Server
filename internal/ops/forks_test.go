package ops

// Copied installations (PROTOCOL §6.10):
// copied libraries. Scenario 33's numbers: session S is exported at 300 s; the
// copy is taken at a saved 330 s (the unexported tail); the original goes on
// to 360 s; the copy listens 20 s more.

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
)

// sid is a stable UUID for a named journal save in these scenarios.
func sid(name string) string {
	h := sha256.Sum256([]byte(name))
	x := hex.EncodeToString(h[:])
	return x[0:8] + "-" + x[8:12] + "-4" + x[13:16] + "-8" + x[17:20] + "-" + x[20:32]
}

const second = int64(1000)

// copyDevice is a library copied from src onto another device that also
// carried its nonce (the fallback): its own transport, but src's author, author
// counter and writing tip.
func (e *env) copyDevice(src *device, name string) *device {
	d := e.device(name)
	d.authorID = src.inst.InstallID
	if src.authorID != "" {
		d.authorID = src.authorID
	}
	d.author, d.last = src.author, src.last
	return d
}

func saveMeta(id string, index int64, first, baseline string, passive bool) map[string]any {
	m := map[string]any{"id": sid(id), "index": index, "baseline": baseline, "passive": passive}
	if first != "" {
		m["first"] = sid(first)
	}
	return m
}

func withSave(p map[string]any, save map[string]any) map[string]any {
	p["save"] = save
	return p
}

func (l *listenEnv) playedMS(t *testing.T) int64 {
	t.Helper()
	rec := l.record("played_time")
	const key = `"played_ms":`
	i := len(rec) - 1
	for j := 0; j+len(key) <= len(rec); j++ {
		if rec[j:j+len(key)] == key {
			i = j + len(key)
			break
		}
	}
	n := i
	for n < len(rec) && rec[n] >= '0' && rec[n] <= '9' {
		n++
	}
	v, err := strconv.ParseInt(rec[i:n], 10, 64)
	if err != nil {
		t.Fatalf("played_time record %s", rec)
	}
	return v
}

// copyScenario sets up S on A: start (baseline b0) and an export at 300 s
// covering saves 1–3. It returns the session, start fact and baseline.
func copyScenario(t *testing.T, l *listenEnv, a *device) (string, map[string]any, string) {
	t.Helper()
	s, b0 := newID(), newID()
	l.clock += second
	st := start(s, 0, true)
	st["baseline"] = b0
	startF := l.lfact(a, "session.start", l.ref, st)
	l.pushOK(t, a, startF, "applied")
	l.clock += 300 * second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 1, 300*second, 300*second), saveMeta("s3", 3, "s1", b0, false))), "applied")
	return s, startF, b0
}

// Scenario 33, fallback: both copies keep the author; whichever pushes first
// keeps it, the other becomes a derived branch. Statistics are 380 s either
// way; the copy's resumed listening takes over the position.
func TestCopyFallbackBothOrders(t *testing.T) {
	for _, copyFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "original first", true: "copy first"}[copyFirst], func(t *testing.T) {
			l := newListenEnv(t)
			a := l.device("A")
			s, _, b0 := copyScenario(t, l, a)
			b := l.copyDevice(a, "B")
			b1 := newID()
			// A keeps playing: saves 4 (shared, 330), 5 and 6, exported at 360.
			l.clock += 60 * second
			a2 := l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 360*second, 360*second), saveMeta("sA6", 6, "s4", b0, false)))
			// B reloads: its restart export of the copied save 4 (330), then
			// listens 20 s under a new baseline.
			l.clock += 30 * second
			bRestart := l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 330*second, 330*second), saveMeta("s4", 4, "s4", b0, true)))
			l.clock += 20 * second
			b3 := l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 3, 350*second, 350*second), saveMeta("sB6", 6, "sB5", b1, false)))
			if copyFirst {
				l.pushOK(t, b, bRestart, "applied")
				l.pushOK(t, b, b3, "applied")
				l.pushOK(t, a, a2, "applied")
			} else {
				l.pushOK(t, a, a2, "applied")
				l.pushOK(t, b, bRestart, "applied")
				l.pushOK(t, b, b3, "applied")
			}
			if got := l.playedMS(t); got != 380*second {
				t.Errorf("played time %d ms, want 380 s", got)
			}
			l.expect(t, "position", `{"claim_id":"`+b3["op_id"].(string)+`","generation_id":"`+l.g+`","position":350000}`)
			// The original's running session cannot steal the position back
			// with an ordinary checkpoint.
			l.clock += 60 * second
			l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 3, 420*second, 420*second), saveMeta("sA7", 7, "sA7", b0, false))), "applied")
			l.expect(t, "position", `{"claim_id":"`+b3["op_id"].(string)+`","generation_id":"`+l.g+`","position":350000}`)
			if got := l.playedMS(t); got != 440*second {
				t.Errorf("played time %d ms, want 440 s (420 + the copy's 20)", got)
			}
		})
	}
}

// Rewinds: the original rewinds to 100 s after the copy; the total is the same
// whichever library keeps the author.
func TestCopyRewindIsOrderIndependent(t *testing.T) {
	for _, copyFirst := range []bool{false, true} {
		l := newListenEnv(t)
		a := l.device("A")
		s, _, b0 := copyScenario(t, l, a)
		b := l.copyDevice(a, "B")
		b1 := newID()
		l.clock += 60 * second
		a2 := l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 100*second, 100*second), saveMeta("sA6", 6, "s4", b0, false)))
		l.clock += 30 * second
		bRestart := l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 330*second, 330*second), saveMeta("s4", 4, "s4", b0, true)))
		l.clock += 20 * second
		b3 := l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 3, 350*second, 350*second), saveMeta("sB6", 6, "sB5", b1, false)))
		if copyFirst {
			l.push(b, bRestart, b3)
			l.push(a, a2)
		} else {
			l.push(a, a2)
			l.push(b, bRestart, b3)
		}
		if got := l.playedMS(t); got != 120*second {
			t.Errorf("copy first %v: played time %d ms, want 120 s", copyFirst, got)
		}
	}
}

// localCopy is a copy detected locally: a new author whose first fact is its
// fork, then its tail export of the copied save 4 (330 s), passive.
func (l *listenEnv) localCopy(t *testing.T, a *device, s, b0 string) *device {
	t.Helper()
	b := l.device("B")
	l.clock += second
	fork := l.fact(b, "installation.fork", []string{a.last}, []any{}, map[string]any{"author": a.inst.InstallID, "head": a.last})
	l.pushOK(t, b, fork, "applied")
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 330*second, 330*second), saveMeta("s4", 4, "s4", b0, true))), "applied")
	return b
}

// Scenario 33, local detection: with the original alive the tail counts once
// (in the original's 360); with the original lost the copy's tail counts.
func TestCopyLocalDetection(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	s, _, b0 := copyScenario(t, l, a)
	b := l.localCopy(t, a, s, b0)
	// The copy's new listening is a new session: 20 s.
	s2 := newID()
	st := start(s2, 330*second, true)
	st["baseline"] = newID()
	l.clock += 10 * second
	l.pushOK(t, b, l.lfact(b, "session.start", l.ref, st), "applied")
	l.clock += 20 * second
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s2, 1, 350*second, 20*second), saveMeta("c1", 1, "c1", st["baseline"].(string), false))), "applied")
	if got := l.playedMS(t); got != 350*second {
		t.Errorf("original lost: played time %d ms, want 350 s (300 + the 30 s tail + 20)", got)
	}
	// The original turns up again and exports its 360 s: the tail now counts
	// in the original's continuation, once.
	l.clock += 10 * second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 360*second, 360*second), saveMeta("sA6", 6, "s4", b0, false))), "applied")
	if got := l.playedMS(t); got != 380*second {
		t.Errorf("original alive: played time %d ms, want 380 s", got)
	}
}

// A reset that observed the copy's tail export covers the shared tail on the
// original's path too: only the original's own 30 s count.
func TestResetThroughCopyTail(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	s, _, b0 := copyScenario(t, l, a)
	b := l.localCopy(t, a, s, b0)
	l.clock += second
	l.pushOK(t, b, l.lfact(b, "statistics.reset", nil, map[string]any{}), "applied")
	if got := l.playedMS(t); got != 0 {
		t.Fatalf("after the reset: %d ms, want 0", got)
	}
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 360*second, 360*second), saveMeta("sA6", 6, "s4", b0, false))), "applied")
	if got := l.playedMS(t); got != 30*second {
		t.Errorf("played time %d ms, want 30 s", got)
	}
}

// Copied finalizations (PROTOCOL §6.10): a copied pending lazy finalization completes the
// episode only if nobody went on listening past the save it names.
func TestCopiedFinalization(t *testing.T) {
	for _, originalContinued := range []bool{false, true} {
		l := newListenEnv(t)
		a := l.device("A")
		s, startF, b0 := copyScenario(t, l, a)
		b := l.device("B")
		l.clock += second
		l.pushOK(t, b, l.fact(b, "installation.fork", []string{a.last}, []any{}, map[string]any{"author": a.inst.InstallID, "head": a.last}), "applied")
		if originalContinued {
			l.clock += second
			l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 10*second, 300*second), saveMeta("sA5", 5, "s4", b0, false))), "applied")
		}
		l.clock += second
		want := "applied"
		if originalContinued {
			want = "superseded"
		}
		l.pushOK(t, b, l.lfact(b, "session.finalize", l.ref, finalize(s, 1, startF["op_id"].(string), true)), want)
		played := `"play_state":"played"`
		got := l.record("play_state")
		if (len(got) > 0 && contains(got, played)) != !originalContinued {
			t.Errorf("original continued %v: play state %s", originalContinued, got)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// A copy taken before the first export diverges at the start: the original's
// reloaded listening claims, and a later copy of the original that adds no new
// save cannot move that earlier divergence (the position once fell back to the first copy's 10 s).
func TestCopyBeforeFirstExportKeepsClaim(t *testing.T) {
	for _, copyFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "original first", true: "copy first"}[copyFirst], func(t *testing.T) {
			l := newListenEnv(t)
			a := l.device("A")
			s, b0 := newID(), newID()
			st := start(s, 0, true)
			st["baseline"] = b0
			l.pushOK(t, a, l.lfact(a, "session.start", l.ref, st), "applied")
			b := l.copyDevice(a, "B")
			// Both reload; another phone takes the position before either resumes.
			d := l.device("D")
			l.clock += second
			l.pushOK(t, d, l.lfact(d, "session.start", l.ref, start(newID(), 900*second, true)), "applied")
			l.clock += second
			bcp := l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 1, 10*second, 10*second), saveMeta("early-b", 1, "early-b", newID(), false)))
			l.clock += second
			b1 := newID()
			acp := l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 1, 20*second, 20*second), saveMeta("early-a", 1, "early-a", b1, false)))
			if copyFirst {
				l.pushOK(t, b, bcp, "applied")
				l.pushOK(t, a, acp, "applied")
			} else {
				l.pushOK(t, a, acp, "applied")
				l.pushOK(t, b, bcp, "applied")
			}
			l.expect(t, "position", `{"claim_id":"`+acp["op_id"].(string)+`","generation_id":"`+l.g+`","position":20000}`)
			// C copies A after its export and seals without a new save.
			c := l.copyDevice(a, "C")
			l.clock += second
			l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 30*second, 30*second), saveMeta("early-a2", 2, "early-a2", b1, false))), "applied")
			l.clock += second
			l.pushOK(t, c, l.lfact(c, "session.end", l.ref, withSave(end(s, 2, 20*second, 20*second, "suspended", false, false, false), saveMeta("early-a", 1, "", b1, true))), "applied")
			l.expect(t, "position", `{"claim_id":"`+acp["op_id"].(string)+`","generation_id":"`+l.g+`","position":30000}`)
		})
	}
}

// A copy of a derived branch carries its pending finalization naming the
// session's start: that means the claim its writing history holds, the
// branch's own.
func TestCopiedFinalizationResolvesInheritedClaim(t *testing.T) {
	l := newListenEnv(t)
	a := l.device("A")
	s, st, b0 := copyScenario(t, l, a)
	b := l.copyDevice(a, "B")
	l.clock += second
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 360*second, 360*second), saveMeta("claim-a", 6, "s4", b0, false))), "applied")
	l.clock += second
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 2, 330*second, 330*second), saveMeta("s4", 4, "s4", b0, true))), "applied")
	l.clock += second
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, withSave(checkpoint(s, 3, 350*second, 350*second), saveMeta("claim-b", 6, "b5", newID(), false))), "applied")
	// C, a locally detected copy of B, finalizes; offline B never learned its claim.
	c := l.device("C")
	l.clock += second
	l.pushOK(t, c, l.fact(c, "installation.fork", []string{b.last}, []any{}, map[string]any{"author": a.inst.InstallID, "head": b.last}), "applied")
	l.clock += second
	l.pushOK(t, c, l.lfact(c, "session.finalize", l.ref, finalize(s, 3, st["op_id"].(string), true)), "applied")
	if !contains(l.record("play_state"), `"play_state":"played"`) {
		t.Errorf("the copy's finalization: %s", l.record("play_state"))
	}
}
