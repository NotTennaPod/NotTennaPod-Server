#!/usr/bin/env python3
"""Hand-written cases of where play was pressed last (PROTOCOL §8.1): resume claims
(`session.checkpoint` with `claim`), written from PROTOCOL §8 and scenarios 5 and 5a (§16),
never from the Go code.

The cases are registered with selftest_rules.py (its B builder, its runner) and written
to contract/vectors/rules_offline_merges.json by `selftest_rules.py --groups DIR`; without
--groups the selftest checks that the committed file equals them.

Every expected value below is computed by hand in the comment beside it.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import selftest_rules as st  # noqa: E402
from selftest_rules import B, BLN, L, MIN, SEC, U1, check, eq, fixed_op, pg, posv, ref, sv  # noqa: E402

FILE = "rules_offline_merges.json"

# PROTOCOL §16: times are in October 2026 (UTC here).
SUN = 1791104400000 - 9 * 3600000  # Sun 2026-10-04 00:00
DAY = 86400000
HOUR = 3600000
E_LEN = 60 * MIN  # the shared episode E is 60 minutes long


def at(day, h, m=0, s=0):
    """Sun=0, Mon=1, ...: the UTC instant of that day and time."""
    return SUN + day * DAY + h * HOUR + m * MIN + s * SEC


MON, TUE, WED, THU = 1, 2, 3, 4

# Builder prefixes of their own, so that the committed vectors do not depend on how many
# cases the other modules build first.
_PREFIX = [0xa1000000]


def next_prefix():
    _PREFIX[0] += 1
    return _PREFIX[0]


def rcp(b, dev, r, sid, ordinal, pos, contribution, observed=None, save=None, **kw):
    """A claiming checkpoint: an intentional resume (PROTOCOL §8.1). It exports the save the
    player made at READY: a new, non-passive save, carried as `save` with `first` (the claim's evidence). Without an explicit `save` the
    session's other exports carry no save identities (they stay optional), so the resume
    save is new on its path whatever its index: here index = the ordinal, `first` itself."""
    if save is None:
        label = "resume/%s/%d" % (sid, ordinal)
        save = sv(label, ordinal, "resume/" + sid, first=label)
    x = {"session_id": sid, "ordinal": ordinal, "position": pos, "contribution": contribution, "activity": True,
         "claim": True, "save": save}
    if observed is not None:
        x["observed_claim"] = observed
    return b.ep(dev, "session.checkpoint", r, x, **kw)


def run_orders(name, plan, orders, check_each, devices=("A", "B", "C", "P", "Q")):
    """Builds one case per delivery order of the devices' batches: `plan(b, prefix)` returns
    (batches: {name: [thunk(rt)...]}, context); each batch is delivered as a whole, in the
    order given. Every order must give the same records."""
    prefix = next_prefix()
    finals = []
    for order in orders:
        b = B("%s; delivered: %s" % (name, ", ".join(order)), prefix, group=FILE)
        for dev in devices:
            b.dev(dev)  # device IDs in a fixed order, whatever the delivery order
        batches, ctx = plan(b, prefix)
        rt = at(THU, 8)
        for k in order:
            for thunk in batches[k]:
                rt += SEC
                thunk(rt)
        check_each(b, ctx)
        finals.append(b.engine.records())
    for i in range(1, len(finals)):
        eq(finals[i], finals[0], "%s: delivery order %d converges" % (name, i + 1))


def minute_saves(b, dev, e, sid, first_ord, t0, pos0, pos1, contrib_of, prefix, opbase):
    """Checkpoints at the wire cadence (one a minute while playing) from pos0 + 1 min to
    pos1, the last one at the pause, as delivery thunks with fixed op IDs."""
    out = []
    o = first_ord
    for k in range(1, (pos1 - pos0) // MIN + 1):
        p = pos0 + k * MIN
        out.append(lambda rt, o=o, p=p, k=k: b.cp(dev, e, sid, o, p, contrib_of(p), t=t0 + k * MIN, rt=rt,
                                                  op=fixed_op(prefix, opbase + k)))
        o += 1
    return out


# ---------------------------------------------------------------------------
# Scenario 5a: two phones on three days, and the earlier example (scenario 5)
# ---------------------------------------------------------------------------


def test_scenario_5a():
    """Scenario 5a (PROTOCOL §8.1). Mon 09:00 P (the iPhone) starts E at 0:00 and pauses at
    10:00 with the app open (claim c1 = its start). Tue 09:00 C (the Android phone), offline,
    starts E at 0:00 (claim c2) and plays to 36:00 (60%). Wed 09:00 P, still offline,
    resumes E at 10:00 (claim c3: a checkpoint with `claim`) and plays to 18:00 (30%).

    By hand: claims in order c1 (Mon) < c2 (Tue) < c3 (Wed); c3 owns, so the position is
    18:00 under c3 (without resume claims it would be 36:00). Played time: P1 = 18:00
    (one session, start 0) + C1 = 36:00 = 54:00. Last played and history: P's pause save,
    Wed 09:08."""
    def plan(b, prefix):
        s = b.subscribe("P", U1, op=fixed_op(prefix, 1), t=at(0, 20), rt=at(0, 20, 0, 1))
        P_, G_ = pg(s)
        e = st.ref(P_, G_, "ep-e", "E")
        sp, sc = fixed_op(prefix, 100), fixed_op(prefix, 101)
        ctx = {"G": G_, "e": e}
        p_mon = [lambda rt: ctx.__setitem__("c1", b.start("P", e, sp, 0, duration=E_LEN, t=at(MON, 9), rt=rt,
                                                          op=fixed_op(prefix, 2)))]
        p_mon += minute_saves(b, "P", e, sp, 1, at(MON, 9), 0, 10 * MIN, lambda p: p, prefix, 200)
        c_tue = [lambda rt: ctx.__setitem__("c2", b.start("C", e, sc, 0, after=[s], t=at(TUE, 9), rt=rt,
                                                          op=fixed_op(prefix, 3)))]
        c_tue += minute_saves(b, "C", e, sc, 1, at(TUE, 9), 0, 36 * MIN, lambda p: p, prefix, 300)
        p_wed = [lambda rt: ctx.__setitem__("c3", rcp(b, "P", e, sp, 11, 10 * MIN, 10 * MIN, observed=ctx["c1"],
                                                      t=at(WED, 9), rt=rt, op=fixed_op(prefix, 4)))]
        p_wed += minute_saves(b, "P", e, sp, 12, at(WED, 9), 10 * MIN, 18 * MIN, lambda p: p, prefix, 400)
        return {"P-Mon": p_mon, "C-Tue": c_tue, "P-Wed": p_wed}, ctx

    def check_each(b, x):
        e, G_ = x["e"], x["G"]
        eq(L(b, e, "position"), posv(G_, 18 * MIN, x["c3"]), "%s: 30%% (18:00) under P's Wednesday claim" % b.name)
        eq(L(b, e, "played_time"), {"generation_id": G_, "played_ms": 54 * MIN}, "%s: 18:00 + 36:00" % b.name)
        eq(L(b, e, "last_played"), {"generation_id": G_, "time": at(WED, 9, 8)}, "%s: last played Wed 09:08" % b.name)
        eq(L(b, e, "history"), {"generation_id": G_, "time": at(WED, 9, 8)}, "%s: history Wed 09:08" % b.name)
        eq(b.out(x["c3"]), ("applied", None), "%s: the claim is applied" % b.name)
    run_orders("5a: two phones on three days (Mon P starts, Tue C to 60%, Wed P resumes to 30%)", plan,
               [("P-Mon", "P-Wed", "C-Tue"), ("C-Tue", "P-Mon", "P-Wed"), ("P-Mon", "C-Tue", "P-Wed")], check_each)


def test_earlier_example():
    """Example 2 (PROTOCOL §8.1, scenario 5): Mon P plays E from 0:00 to 18:00 (30%) and pauses; Tue C
    starts E at 0:00 (its claim is later) and plays to 36:00 (60%). By hand: 36:00 under C's
    start; played 18:00 + 36:00 = 54:00; last played Tue 09:36."""
    def plan(b, prefix):
        s = b.subscribe("P", U1, op=fixed_op(prefix, 1), t=at(0, 20), rt=at(0, 20, 0, 1))
        P_, G_ = pg(s)
        e = st.ref(P_, G_, "ep-e", "E")
        sp, sc = fixed_op(prefix, 100), fixed_op(prefix, 101)
        ctx = {"G": G_, "e": e}
        p = [lambda rt: b.start("P", e, sp, 0, t=at(MON, 9), rt=rt, op=fixed_op(prefix, 2))]
        p += minute_saves(b, "P", e, sp, 1, at(MON, 9), 0, 18 * MIN, lambda q: q, prefix, 200)
        cc = [lambda rt: ctx.__setitem__("c2", b.start("C", e, sc, 0, after=[s], t=at(TUE, 9), rt=rt,
                                                       op=fixed_op(prefix, 3)))]
        cc += minute_saves(b, "C", e, sc, 1, at(TUE, 9), 0, 36 * MIN, lambda q: q, prefix, 300)
        return {"P": p, "C": cc}, ctx

    def check_each(b, x):
        e, G_ = x["e"], x["G"]
        eq(L(b, e, "position"), posv(G_, 36 * MIN, x["c2"]), "%s: 60%% under C's later start" % b.name)
        eq(L(b, e, "played_time"), {"generation_id": G_, "played_ms": 54 * MIN}, "%s: both sessions count" % b.name)
        eq(L(b, e, "last_played"), {"generation_id": G_, "time": at(TUE, 9, 36)}, "%s: last played" % b.name)
    run_orders("5: the earlier example (Mon P to 30%, Tue C starts and plays to 60%)", plan,
               [("P", "C"), ("C", "P")], check_each)


# ---------------------------------------------------------------------------
# PROTOCOL §8.1: scenario 1a's literal timeline (E and F)
# ---------------------------------------------------------------------------


def test_scenario_1a_timeline():
    """PROTOCOL §8.1 table, literally (rewind-on-resume off, 1x, threshold 30 s).

    | Mon 09:00-09:10 | P | play E from 0:00, pause      | P1 starts, c1   | 10:00 | P1 = 10:00 |
    | Tue 09:00-09:30 | C | play E from 0:00, pause      | C1 starts, c2   | 30:00 | C1 = 30:00 |
    | Tue 12:00       | C | reset statistics (C1 only)   | cut on C1       |       | C1 = 0     |
    | Wed 09:00-09:10 | P | resume E at 10:00, pause | P1 continues, c3| 20:00 | P1 = 20:00 |
    | Wed 10:00-10:05 | P | F: start 1:40, play to 3:20, pause, resume, rewind to 0:50, play to
                            2:30, pause                   | F1              | 2:30  | F1 = 0:50  |

    F's saves by hand (Android: contribution = position - start once beyond the start,
    else its preceding value): 10:01:00 at 2:40 -> 1:00; 10:01:40 at 3:20 -> 1:40 (pause);
    10:02:00 the resume claim at 3:20 -> 1:40; the rewind to 0:50 keeps 1:40 below the
    start; 10:03:00 at 1:50 -> 0:10; 10:03:40 at 2:30 -> 0:50 (pause).

    Expected (both reconnect orders): E at 20:00 under c3; E played 20:00 (20:00 + C1's
    residual 0); E last played and history Wed 09:10; F played 0:50, at 2:30 under F's
    resume claim, last played Wed 10:03:40."""
    def plan(b, prefix):
        s = b.subscribe("P", U1, op=fixed_op(prefix, 1), t=at(0, 20), rt=at(0, 20, 0, 1))
        P_, G_ = pg(s)
        e, f = st.ref(P_, G_, "ep-e", "E"), st.ref(P_, G_, "ep-f", "F")
        p1, c1s, f1 = fixed_op(prefix, 100), fixed_op(prefix, 101), fixed_op(prefix, 102)
        ctx = {"G": G_, "e": e, "f": f}
        pm = [lambda rt: ctx.__setitem__("c1", b.start("P", e, p1, 0, duration=E_LEN, t=at(MON, 9), rt=rt,
                                                       op=fixed_op(prefix, 2)))]
        pm += minute_saves(b, "P", e, p1, 1, at(MON, 9), 0, 10 * MIN, lambda p: p, prefix, 200)
        ct = [lambda rt: b.start("C", e, c1s, 0, after=[s], t=at(TUE, 9), rt=rt, op=fixed_op(prefix, 3))]
        ct += minute_saves(b, "C", e, c1s, 1, at(TUE, 9), 0, 30 * MIN, lambda p: p, prefix, 300)
        ct += [lambda rt: ctx.__setitem__("cut", b.sreset("C", t=at(TUE, 12), rt=rt, op=fixed_op(prefix, 4)))]
        pw = [lambda rt: ctx.__setitem__("c3", rcp(b, "P", e, p1, 11, 10 * MIN, 10 * MIN, observed=ctx["c1"],
                                                   t=at(WED, 9), rt=rt, op=fixed_op(prefix, 5)))]
        pw += minute_saves(b, "P", e, p1, 12, at(WED, 9), 10 * MIN, 20 * MIN, lambda p: p, prefix, 400)
        F0 = at(WED, 10)
        pw += [
            lambda rt: ctx.__setitem__("f1", b.start("P", f, f1, 100 * SEC, t=F0, rt=rt, op=fixed_op(prefix, 6))),
            lambda rt: b.cp("P", f, f1, 1, 160 * SEC, 60 * SEC, t=F0 + 60 * SEC, rt=rt, op=fixed_op(prefix, 8)),
            lambda rt: b.cp("P", f, f1, 2, 200 * SEC, 100 * SEC, t=F0 + 100 * SEC, rt=rt, op=fixed_op(prefix, 9)),
            lambda rt: ctx.__setitem__("fr", rcp(b, "P", f, f1, 3, 200 * SEC, 100 * SEC, observed=ctx["f1"],
                                                 t=F0 + 120 * SEC, rt=rt, op=fixed_op(prefix, 7))),
            lambda rt: b.cp("P", f, f1, 4, 110 * SEC, 10 * SEC, t=F0 + 180 * SEC, rt=rt, op=fixed_op(prefix, 10)),
            lambda rt: b.cp("P", f, f1, 5, 150 * SEC, 50 * SEC, t=F0 + 220 * SEC, rt=rt, op=fixed_op(prefix, 11)),
        ]
        return {"P": pm + pw, "C": ct}, ctx

    def check_each(b, x):
        e, f, G_ = x["e"], x["f"], x["G"]
        eq(L(b, e, "position"), posv(G_, 20 * MIN, x["c3"]), "%s: E at 20:00 (c3, P pressed play last)" % b.name)
        eq(L(b, e, "played_time"), {"generation_id": G_, "played_ms": 20 * MIN},
           "%s: E played 20:00 = P1 20:00 + C1's residual 0" % b.name)
        eq(L(b, e, "last_played"), {"generation_id": G_, "time": at(WED, 9, 10)}, "%s: E last played" % b.name)
        eq(L(b, e, "history"), {"generation_id": G_, "time": at(WED, 9, 10)}, "%s: E history" % b.name)
        eq(L(b, f, "played_time"), {"generation_id": G_, "played_ms": 50 * SEC}, "%s: F played 0:50" % b.name)
        eq(L(b, f, "position"), posv(G_, 150 * SEC, x["fr"]), "%s: F at 2:30 under its resume claim" % b.name)
        eq(L(b, f, "last_played"), {"generation_id": G_, "time": at(WED, 10, 3, 40)}, "%s: F last played" % b.name)
        eq(b.out(x["cut"]), ("applied", None), "%s: the reset" % b.name)
    run_orders("1a: the literal timeline (E and F, amendment §3)", plan, [("P", "C"), ("C", "P")], check_each)


# ---------------------------------------------------------------------------
# PROTOCOL §8.1: claims belong to a writing path (an undetected copy)
# ---------------------------------------------------------------------------


def test_copy_paths():
    """PROTOCOL §8.1 example with an undetected copy. A starts S at 0:00 (Mon 09:00, baseline A0) and keeps playing; at 09:10 (saves s1..s120 exported at 10:00) its
    library is copied to B, which reloads (baseline B1). B's person presses play at 09:20:
    a claiming checkpoint at 10:00 (b1); B plays to 20:00 (09:30). A, still playing, saves
    ordinary checkpoints at 09:21 (21:00) and 09:25 (25:00), pauses, and its person presses
    play at 09:40 (a claiming checkpoint at 25:00, a4) and plays to 30:00 (09:45).

    By hand, in claim order: a1 (A's start) < b1 (09:20) < a4 (09:40).
    * A's 09:21 and 09:25 checkpoints resolve against A's own path claim (a1), which is not
      current after b1: they never take B's position.
    * After a4, A's path claim owns: the final position is 30:00 under a4.
    * Played time (the save tree): shared 10:00; A's path +11 +4 +0 +5 = 20:00; B's path 0 + 10:00; total 40:00.
    Both delivery orders (the first at the author position keeps the author, the other
    becomes a derived branch)."""
    def plan(b, prefix):
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1), t=at(MON, 8), rt=at(MON, 8, 0, 1))
        P_, G_ = pg(s)
        e = st.ref(P_, G_, "ep-e", "E")
        S = fixed_op(prefix, 100)
        ctx = {"G": G_, "e": e}
        a1 = b.start("A", e, S, 0, baseline=BLN("A0"), duration=E_LEN, t=at(MON, 9), rt=at(MON, 9, 0, 1),
                     op=fixed_op(prefix, 2))
        b.cp("A", e, S, 1, 10 * MIN, 10 * MIN, save=sv("s120", 120, "A0", first="s1"), t=at(MON, 9, 10),
             rt=at(MON, 9, 10, 1), op=fixed_op(prefix, 3))
        ctx["a1"] = a1
        b.clone("A", "B")
        bb = [
            lambda rt: ctx.__setitem__("b1", rcp(b, "B", e, S, 2, 10 * MIN, 10 * MIN, observed=a1,
                                                 save=sv("b121", 121, "B1", first="b121"), t=at(MON, 9, 20), rt=rt,
                                                 op=fixed_op(prefix, 10))),
            lambda rt: ctx.__setitem__("b2", b.cp("B", e, S, 3, 20 * MIN, 20 * MIN,
                                                  save=sv("b241", 241, "B1", first="b122"), t=at(MON, 9, 30), rt=rt,
                                                  op=fixed_op(prefix, 11))),
        ]
        aa = [
            lambda rt: ctx.__setitem__("a2", b.cp("A", e, S, 2, 21 * MIN, 21 * MIN,
                                                  save=sv("a252", 252, "A0", first="a121"), t=at(MON, 9, 21), rt=rt,
                                                  op=fixed_op(prefix, 20))),
            lambda rt: ctx.__setitem__("a3", b.cp("A", e, S, 3, 25 * MIN, 25 * MIN,
                                                  save=sv("a300", 300, "A0", first="a253"), t=at(MON, 9, 25), rt=rt,
                                                  op=fixed_op(prefix, 21))),
            lambda rt: ctx.__setitem__("a4", rcp(b, "A", e, S, 4, 25 * MIN, 25 * MIN, observed=a1,
                                                 save=sv("a301", 301, "A0", first="a301"), t=at(MON, 9, 40), rt=rt,
                                                 op=fixed_op(prefix, 22))),
            lambda rt: ctx.__setitem__("a5", b.cp("A", e, S, 5, 30 * MIN, 30 * MIN,
                                                  save=sv("a361", 361, "A0", first="a302"), t=at(MON, 9, 45), rt=rt,
                                                  op=fixed_op(prefix, 23))),
        ]
        return {"A": aa, "B": bb}, ctx

    def check_each(b, x):
        e, G_ = x["e"], x["G"]
        eq(L(b, e, "position"), posv(G_, 30 * MIN, x["a4"]), "%s: A pressed play last (09:40)" % b.name)
        eq(L(b, e, "played_time"), {"generation_id": G_, "played_ms": 40 * MIN}, "%s: 10 + 20 + 10 minutes" % b.name)
        check(all(o["outcome"] == "applied" for o in b.engine.outcomes.values()), "%s: all applied" % b.name)
    run_orders("1.2: claims belong to their writing path (an undetected copy, A and B resume)", plan,
               [("A", "B"), ("B", "A")], check_each)
    # The prefix in the middle: before A's resume, B's 20:00 under b1 stands whatever A's
    # ordinary checkpoints say (both orders).

    def plan_mid(b, prefix):
        batches, ctx = plan(b, prefix)
        batches["A"] = batches["A"][:2]
        return batches, ctx

    def check_mid(b, x):
        eq(L(b, x["e"], "position"), posv(x["G"], 20 * MIN, x["b1"]),
           "%s: A's ordinary checkpoints after B's claim never take the position" % b.name)
    run_orders("1.2: an ordinary checkpoint of the original after the copy's claim", plan_mid,
               [("A", "B"), ("B", "A")], check_mid)


# ---------------------------------------------------------------------------
# Example 4 (PROTOCOL §8.1) and 5: reset -> resume -> completion; stale end, finalize, leave;
# adoption -> finalization
# ---------------------------------------------------------------------------


def test_reset_resume_completion():
    """Example 4 (PROTOCOL §8.1). Mon 09:50 B pauses E at 50:00 (session SB, claim b0 = its start) and
    goes offline. Tue 10:00 A (which synced B's facts) resets E's position. Wed 09:00 B,
    which never saw the reset, presses play (a claiming checkpoint b2 at 50:00), plays to
    the natural end and the end completes (Wed 09:10).

    By hand: the claims in order are b0 (Mon) < the reset (Tue) < b2 (Wed). b2 owns, so B's
    end (on b2's path) owns at its place: the completion bundle fires: played, 0:00 under
    b2 (which stays current). Without the claim (an old-style resume) the reset would bar
    B's later saves and the completion would be suppressed: see the second case."""
    for claimed in (True, False):
        def plan(b, prefix, claimed=claimed):
            s = b.subscribe("A", U1, op=fixed_op(prefix, 1), t=at(MON, 8), rt=at(MON, 8, 0, 1))
            P_, G_ = pg(s)
            e = st.ref(P_, G_, "ep-e", "E")
            SB = fixed_op(prefix, 100)
            ctx = {"G": G_, "e": e}
            b0 = b.start("B", e, SB, 0, after=[s], duration=E_LEN, t=at(MON, 9), rt=at(MON, 9, 0, 1),
                         op=fixed_op(prefix, 2))
            bp = b.cp("B", e, SB, 1, 50 * MIN, 50 * MIN, t=at(MON, 9, 50), rt=at(MON, 9, 50, 1), op=fixed_op(prefix, 3))
            ctx["b0"] = b0
            aa = [lambda rt: ctx.__setitem__("reset", b.ep("A", "episode.reset_position", e, after=[bp],
                                                           t=at(TUE, 10), rt=rt, op=fixed_op(prefix, 4)))]
            if claimed:
                first = lambda rt: ctx.__setitem__("b2", rcp(b, "B", e, SB, 2, 50 * MIN, 50 * MIN, observed=b0,  # noqa: E731
                                                             t=at(WED, 9), rt=rt, op=fixed_op(prefix, 5)))
            else:
                first = lambda rt: ctx.__setitem__("b2", b.cp("B", e, SB, 2, 50 * MIN, 50 * MIN, t=at(WED, 9), rt=rt,  # noqa: E731
                                                              op=fixed_op(prefix, 5)))
            bb = [first,
                  lambda rt: b.cp("B", e, SB, 3, 55 * MIN, 55 * MIN, t=at(WED, 9, 5), rt=rt, op=fixed_op(prefix, 6)),
                  lambda rt: ctx.__setitem__("end", b.end("B", e, SB, 4, E_LEN, E_LEN, completed=True, t=at(WED, 9, 10),
                                                          rt=rt, op=fixed_op(prefix, 7)))]
            return {"A": aa, "B": bb}, ctx

        def check_each(b, x, claimed=claimed):
            e, G_ = x["e"], x["G"]
            if claimed:
                eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "played"}, "%s: completed" % b.name)
                eq(L(b, e, "position"), posv(G_, 0, x["b2"]), "%s: 0:00 under B's resume claim" % b.name)
            else:
                eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "unplayed"},
                   "%s: the reset bars the old session's completion" % b.name)
                eq(L(b, e, "position"), posv(G_, 0, x["reset"]), "%s: the reset's 0:00" % b.name)
            eq(b.out(x["end"]), ("applied", None), "%s: an end is applied" % b.name)
        run_orders("1.8 example 4: reset, then a %s resume, then completion"
                   % ("claiming" if claimed else "non-claiming (pre-amendment)"), plan, [("A", "B"), ("B", "A")],
                   check_each)


def test_stale_end():
    """Example 5 (PROTOCOL §8.1), a stale end. Sun 09:00 B starts E at 30:00 (claim b0) and pauses at
    40:00 (09:10); A syncs. Mon 09:00 A starts E at 40:00 (claim a1) and plays to the
    natural end at 09:20, which completes. Mon 09:10 B, offline, presses play on its paused
    session (claim b2, at 40:00) and pauses at 45:00 (09:15).

    By hand: a1 (09:00) < b2 (09:10) < A's end (09:20): A's end resolves against its path
    claim a1, which is no longer current, so its completion bundle is suppressed. Final:
    unplayed, 45:00 under b2; played time A 20:00 + B 10:00 + 5:00 = 35:00."""
    def plan(b, prefix):
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1), t=at(0, 8), rt=at(0, 8, 0, 1))
        P_, G_ = pg(s)
        e = st.ref(P_, G_, "ep-e", "E")
        SA, SB = fixed_op(prefix, 100), fixed_op(prefix, 101)
        ctx = {"G": G_, "e": e}
        b0 = b.start("B", e, SB, 30 * MIN, after=[s], duration=E_LEN, t=at(0, 9), rt=at(0, 9, 0, 1),
                     op=fixed_op(prefix, 2))
        bp = b.cp("B", e, SB, 1, 40 * MIN, 10 * MIN, t=at(0, 9, 10), rt=at(0, 9, 10, 1), op=fixed_op(prefix, 3))
        aa = [lambda rt: ctx.__setitem__("a1", b.start("A", e, SA, 40 * MIN, after=[bp], t=at(MON, 9), rt=rt,
                                                       op=fixed_op(prefix, 4))),
              lambda rt: ctx.__setitem__("end", b.end("A", e, SA, 1, E_LEN, 20 * MIN, completed=True, t=at(MON, 9, 20),
                                                      rt=rt, op=fixed_op(prefix, 5)))]
        bb = [lambda rt: ctx.__setitem__("b2", rcp(b, "B", e, SB, 2, 40 * MIN, 10 * MIN, observed=b0, t=at(MON, 9, 10),
                                                   rt=rt, op=fixed_op(prefix, 6))),
              lambda rt: b.cp("B", e, SB, 3, 45 * MIN, 15 * MIN, t=at(MON, 9, 15), rt=rt, op=fixed_op(prefix, 7))]
        return {"A": aa, "B": bb}, ctx

    def check_each(b, x):
        e, G_ = x["e"], x["G"]
        eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "unplayed"}, "%s: the end is stale" % b.name)
        eq(L(b, e, "position"), posv(G_, 45 * MIN, x["b2"]), "%s: B's 45:00 under its resume claim" % b.name)
        eq(L(b, e, "played_time"), {"generation_id": G_, "played_ms": 35 * MIN},
           "%s: A 20:00 + B 15:00 (both count)" % b.name)
        eq(b.out(x["end"]), ("applied", None), "%s: an end is applied" % b.name)
    run_orders("1.8 example 5: a stale end (a later resume claim on another device)", plan,
               [("A", "B"), ("B", "A")], check_each)


def test_stale_finalize_and_alias():
    """Example 5 (PROTOCOL §8.1), a stale finalization, and the start-ID alias. Mon 09:00 A starts E
    (a1) and pauses at 59:50 (09:59:50: inside its 30 s threshold). Mon 10:30 B, offline,
    presses play on its own paused session (claim b2) and pauses at 59:55. Mon 11:00 A loads
    another episode: its lazy finalization of E names its pending claim a1 and completes.

    By hand: a1 < b2 < A's finalization: superseded, E stays unplayed at 59:55 under b2.

    The alias variant: A instead resumed E at 10:40 (a claiming checkpoint a3, after b2),
    paused at 59:58, and its finalization at 11:00 names the session's start a1 (an old
    client). CHOICE (the alias, with resume claims): the start means the latest claim on
    A's path ranked before the finalization, a3, which is current: the bundle fires
    (played, 0:00 under a3)."""
    for variant in ("stale", "alias"):
        def plan(b, prefix, variant=variant):
            s = b.subscribe("A", U1, op=fixed_op(prefix, 1), t=at(0, 8), rt=at(0, 8, 0, 1))
            P_, G_ = pg(s)
            e, other = st.ref(P_, G_, "ep-e", "E"), st.ref(P_, G_, "ep-o", "Other")
            SA, SB = fixed_op(prefix, 100), fixed_op(prefix, 101)
            ctx = {"G": G_, "e": e}
            b0 = b.start("B", e, SB, 50 * MIN, after=[s], duration=E_LEN, t=at(0, 9), rt=at(0, 9, 0, 1),
                         op=fixed_op(prefix, 2))
            bp = b.cp("B", e, SB, 1, 55 * MIN, 5 * MIN, t=at(0, 9, 5), rt=at(0, 9, 5, 1), op=fixed_op(prefix, 3))
            aa = [lambda rt: ctx.__setitem__("a1", b.start("A", e, SA, 55 * MIN, after=[bp], t=at(MON, 9), rt=rt,
                                                           op=fixed_op(prefix, 4))),
                  lambda rt: b.cp("A", e, SA, 1, E_LEN - 10 * SEC, 5 * MIN - 10 * SEC, t=at(MON, 9, 4, 50), rt=rt,
                                  op=fixed_op(prefix, 5))]
            if variant == "alias":
                aa += [lambda rt: ctx.__setitem__("a3", rcp(b, "A", e, SA, 2, E_LEN - 10 * SEC, 5 * MIN - 10 * SEC,
                                                            observed=ctx["a1"], t=at(MON, 10, 40), rt=rt,
                                                            op=fixed_op(prefix, 6))),
                       lambda rt: b.cp("A", e, SA, 3, E_LEN - 2 * SEC, 5 * MIN - 2 * SEC, t=at(MON, 10, 40, 8), rt=rt,
                                       op=fixed_op(prefix, 7))]
            aa += [lambda rt: ctx.__setitem__("fin", b.fin("A", e, SA, 3 if variant == "alias" else 1, ctx["a1"],
                                                           completed=True, t=at(MON, 11), rt=rt, op=fixed_op(prefix, 8))),
                   lambda rt: b.start("A", other, fixed_op(prefix, 102), 0, t=at(MON, 11, 0, 1), rt=rt,
                                      op=fixed_op(prefix, 9))]
            bb = [lambda rt: ctx.__setitem__("b2", rcp(b, "B", e, SB, 2, 55 * MIN, 5 * MIN, observed=b0,
                                                       t=at(MON, 10, 30), rt=rt, op=fixed_op(prefix, 10))),
                  lambda rt: ctx.__setitem__("b3", b.cp("B", e, SB, 3, E_LEN - 5 * SEC, 10 * MIN - 5 * SEC,
                                                        t=at(MON, 10, 34, 55), rt=rt, op=fixed_op(prefix, 11)))]
            return {"A": aa, "B": bb}, ctx

        def check_each(b, x, variant=variant):
            e, G_ = x["e"], x["G"]
            if variant == "stale":
                eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "unplayed"}, "%s: stale" % b.name)
                eq(L(b, e, "position"), posv(G_, E_LEN - 5 * SEC, x["b2"]), "%s: B's position" % b.name)
                # The receipt is decided at acceptance: applied when it arrived before B's
                # claim (its bundle fired then), superseded otherwise.
                want = ("applied", None) if b.engine.rank[x["fin"]] < 0 or b.arrivals.index(
                    next(a for a in b.arrivals if a.get("fact", {}).get("op_id") == x["fin"])) < b.arrivals.index(
                    next(a for a in b.arrivals if a.get("fact", {}).get("op_id") == x["b2"])) else ("superseded", None)
                eq(b.out(x["fin"]), want, "%s: the receipt at acceptance" % b.name)
            else:
                eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "played"}, "%s: the alias" % b.name)
                eq(L(b, e, "position"), posv(G_, 0, x["a3"]), "%s: 0:00 under A's resume claim" % b.name)
        run_orders("1.8 example 5: a %s finalization" % ("stale" if variant == "stale" else
                                                          "start-ID (6c alias) after a resume claim"),
                   plan, [("A", "B"), ("B", "A")], check_each)


def test_stale_leave():
    """Example 5 (PROTOCOL §8.1), a stale leave. Mon 09:00 B starts E (b1) and pauses at 59:50
    (09:04:50); A syncs, loads E paused (showing 59:50 under b1) and goes offline. Mon 10:00
    B presses play (claim b2) and pauses at 59:55. Mon 11:00 A switches away: episode.leave
    with observed_claim b1 and its own completion (59:50 within its threshold).

    By hand: b1 < b2 < the leave: the current claim at the leave is b2, not b1: no
    completion (superseded), its history mark stays. E unplayed at 59:55 under b2."""
    def plan(b, prefix):
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1), t=at(0, 8), rt=at(0, 8, 0, 1))
        P_, G_ = pg(s)
        e = st.ref(P_, G_, "ep-e", "E")
        SB = fixed_op(prefix, 100)
        ctx = {"G": G_, "e": e}
        b1 = b.start("B", e, SB, 55 * MIN, after=[s], duration=E_LEN, t=at(MON, 9), rt=at(MON, 9, 0, 1),
                     op=fixed_op(prefix, 2))
        bp = b.cp("B", e, SB, 1, E_LEN - 10 * SEC, 5 * MIN - 10 * SEC, t=at(MON, 9, 4, 50), rt=at(MON, 9, 4, 51),
                  op=fixed_op(prefix, 3))
        ctx["b1"] = b1
        aa = [lambda rt: ctx.__setitem__("leave", b.leave("A", e, st.det_uuid("leave/T1"), completed=True, observed=b1,
                                                          position=E_LEN - 10 * SEC, after=[bp], t=at(MON, 11), rt=rt,
                                                          op=fixed_op(prefix, 4)))]
        bb = [lambda rt: ctx.__setitem__("b2", rcp(b, "B", e, SB, 2, E_LEN - 10 * SEC, 5 * MIN - 10 * SEC, observed=b1,
                                                   t=at(MON, 10), rt=rt, op=fixed_op(prefix, 5))),
              lambda rt: b.cp("B", e, SB, 3, E_LEN - 5 * SEC, 5 * MIN - 5 * SEC, t=at(MON, 10, 0, 5), rt=rt,
                              op=fixed_op(prefix, 6))]
        return {"A": aa, "B": bb}, ctx

    def check_each(b, x):
        e, G_ = x["e"], x["G"]
        eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "unplayed"}, "%s: stale leave" % b.name)
        eq(L(b, e, "position"), posv(G_, E_LEN - 5 * SEC, x["b2"]), "%s: B's position" % b.name)
        eq(L(b, e, "history"), {"generation_id": G_, "time": at(MON, 11)}, "%s: its history mark stays" % b.name)
    run_orders("1.8 example 5: a stale leave (a resume claim after the claim it observed)", plan,
               [("A", "B"), ("B", "A")], check_each)


def test_adoption_finalization():
    """Example 5 (PROTOCOL §8.1), adoption -> finalization. Mon 09:00 A starts E (a1) and pauses at
    30:00. Mon 10:00 B presses play on its own open session SB (claim b2, at 40:00) and
    pauses at 59:50. A syncs and adopts 59:50: it seals SA at its pre-adoption position
    (superseded, no activity); its pending reference is now b2 (the position's claim).
    Mon 11:00 A loads another episode: its finalization names b2 and completes.

    By hand: b2 is current at the finalization: the bundle fires, played, 0:00 under b2.
    Variant: B presses play again at 10:50 (claim b4) before A's finalization (A never saw
    it): b4 is current, A's finalization (b2) is superseded; E stays at B's latest 59:52
    under b4."""
    for variant in ("valid", "stale"):
        def plan(b, prefix, variant=variant):
            s = b.subscribe("A", U1, op=fixed_op(prefix, 1), t=at(0, 8), rt=at(0, 8, 0, 1))
            P_, G_ = pg(s)
            e, other = st.ref(P_, G_, "ep-e", "E"), st.ref(P_, G_, "ep-o", "Other")
            SA, SB = fixed_op(prefix, 100), fixed_op(prefix, 101)
            ctx = {"G": G_, "e": e}
            b0 = b.start("B", e, SB, 30 * MIN, after=[s], duration=E_LEN, t=at(0, 9), rt=at(0, 9, 0, 1),
                         op=fixed_op(prefix, 2))
            bpa = b.cp("B", e, SB, 1, 40 * MIN, 10 * MIN, t=at(0, 9, 10), rt=at(0, 9, 10, 1), op=fixed_op(prefix, 3))
            a1 = b.start("A", e, SA, 0, after=[bpa], t=at(MON, 9), rt=at(MON, 9, 0, 1), op=fixed_op(prefix, 4))
            ap = b.cp("A", e, SA, 1, 30 * MIN, 30 * MIN, t=at(MON, 9, 30), rt=at(MON, 9, 30, 1), op=fixed_op(prefix, 5))
            b2 = rcp(b, "B", e, SB, 2, 40 * MIN, 10 * MIN, observed=b0, after=[ap], t=at(MON, 10), rt=at(MON, 10, 0, 1),
                     op=fixed_op(prefix, 6))
            b3 = b.cp("B", e, SB, 3, E_LEN - 10 * SEC, 30 * MIN - 10 * SEC, t=at(MON, 10, 19, 50),
                      rt=at(MON, 10, 19, 51), op=fixed_op(prefix, 7))
            ctx.update(a1=a1, b2=b2)
            aa = [lambda rt: b.end("A", e, SA, 2, 30 * MIN, 30 * MIN, reason="superseded", activity=False, history=False,
                                   after=[b3], t=at(MON, 10, 30), rt=rt, op=fixed_op(prefix, 8)),
                  lambda rt: ctx.__setitem__("fin", b.fin("A", e, SA, 2, b2, completed=True, t=at(MON, 11), rt=rt,
                                                          op=fixed_op(prefix, 9))),
                  lambda rt: b.start("A", other, fixed_op(prefix, 102), 0, t=at(MON, 11, 0, 1), rt=rt,
                                     op=fixed_op(prefix, 10))]
            bb = []
            if variant == "stale":
                bb = [lambda rt: ctx.__setitem__("b4", rcp(b, "B", e, SB, 4, E_LEN - 10 * SEC, 30 * MIN - 10 * SEC,
                                                           observed=b2, t=at(MON, 10, 50), rt=rt, op=fixed_op(prefix, 11))),
                      lambda rt: b.cp("B", e, SB, 5, E_LEN - 8 * SEC, 30 * MIN - 8 * SEC, t=at(MON, 10, 50, 2), rt=rt,
                                      op=fixed_op(prefix, 12))]
            return {"A": aa, "B": bb}, ctx

        def check_each(b, x, variant=variant):
            e, G_ = x["e"], x["G"]
            if variant == "valid":
                eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "played"}, "%s: completed" % b.name)
                eq(L(b, e, "position"), posv(G_, 0, x["b2"]), "%s: 0:00 under the adopted claim" % b.name)
                eq(b.out(x["fin"]), ("applied", None), "%s: applied" % b.name)
            else:
                eq(L(b, e, "play_state"), {"generation_id": G_, "play_state": "unplayed"}, "%s: stale" % b.name)
                eq(L(b, e, "position"), posv(G_, E_LEN - 8 * SEC, x["b4"]), "%s: B's latest claim" % b.name)
        orders = [("A", "B"), ("B", "A")] if variant == "stale" else [("A", "B")]
        run_orders("1.8 example 5: adoption of a resume claim, then a %s finalization" % variant, plan, orders,
                   check_each)


# ---------------------------------------------------------------------------
# PROTOCOL §8.1: validation and outcomes
# ---------------------------------------------------------------------------


def test_claim_validation():
    """PROTOCOL §8.1: malformed claim fields or combinations are rejected / invalid_payload; every
    other failed check keeps its existing outcome; no failed check grants ownership."""
    b = B("1.3: claim validation and outcomes", next_prefix(), group=FILE)
    s = b.subscribe("A", U1)
    P_, G_ = pg(s)
    e, e2 = st.ref(P_, G_, "ep-e", "E"), st.ref(P_, G_, "ep-2", "Two")
    S = b.uuid()
    a1 = b.start("A", e, S, 0, baseline=BLN("V0"))
    b.cp("A", e, S, 1, MIN, MIN, save=sv("v12", 12, "V0", first="v1"))
    et = [{"entity": "episode", "id": e["episode_id"], "generation": G_}]
    base = {"episode": e, "session_id": S, "ordinal": 2, "position": MIN, "contribution": MIN, "activity": True}
    bad = [
        dict(base, claim="true"),
        dict(base, claim=1),
        dict(base, claim=None),
        dict(base, claim=True, activity=False),
        dict(base, observed_claim=a1),
        dict(base, claim=False, observed_claim=a1),
        dict(base, claim=True, observed_claim="not-a-uuid"),
        dict(base, claim=True, observed_claim=a1 + "0"),
        dict(base, claim=True, save=sv("v13", 13, "V0", first="v13", passive=True)),
        dict(base, claim=True, save=sv("v12", 12, "V0")),
        dict(base, claim=True),  # no save: no evidence of a new save
    ]
    for p in bad:
        r = b.push("A", "session.checkpoint", p, et, reject=True)
        eq(b.out(r), ("rejected", "invalid_payload"), "rejected: %s" % sorted(set(p) - set(base) | {"activity"}))
    end_claim = {"episode": e, "session_id": S, "ordinal": 2, "position": MIN, "contribution": MIN, "activity": True,
                 "reason": "ended", "completed": False, "threshold_ms": 30000, "remove_from_queue": False,
                 "history": True, "claim": True}
    eq(b.out(b.push("A", "session.end", end_claim, et, reject=True)), ("rejected", "invalid_payload"),
       "an end carries no claim")
    eq(L(b, e, "position"), posv(G_, MIN, a1), "nothing rejected moved the position")
    # claim: false, explicitly: an ordinary checkpoint.
    ok = b.push("A", "session.checkpoint", dict(base, claim=False), et)
    eq((b.out(ok), L(b, e, "position")), (("applied", None), posv(G_, MIN, a1)), "claim false is ordinary")
    # Another device takes over; then failed claims keep their existing outcomes and grant
    # nothing.
    q = b.start("Q", e, b.uuid(), MIN, after=[ok])
    conflict = rcp(b, "A", e, S, 2, 5 * MIN, 5 * MIN)  # ordinal 2 is held with other content
    eq(b.out(conflict), ("not_applied", "session_ordinal_conflict"), "an ordinal conflict")
    # Existing checks come first: a claim re-exporting the covered save v12 that also
    # conflicts on its ordinal keeps session_ordinal_conflict.
    both = rcp(b, "A", e, S, 2, 5 * MIN, 5 * MIN, save=sv("v12", 12, "V0", first="v1"))
    eq(b.out(both), ("not_applied", "session_ordinal_conflict"), "the existing outcome wins over a covered save")
    foreign = rcp(b, "Q", e, S, 3, 5 * MIN, 5 * MIN, after=[ok])
    eq(b.out(foreign), ("not_applied", "foreign_session"), "another author's claim on A's session")
    eq(L(b, e, "position"), posv(G_, MIN, q), "neither grants ownership")
    claimed = rcp(b, "A", e, S, 3, 2 * MIN, 2 * MIN, observed=q, save=sv("v24", 24, "V0", first="v13"))
    eq((b.out(claimed), L(b, e, "position")), (("applied", None), posv(G_, 2 * MIN, claimed)), "a valid claim owns")
    b.end("A", e, S, 4, 3 * MIN, 3 * MIN, reason="switched")
    sealed = rcp(b, "A", e, S, 5, 4 * MIN, 4 * MIN, save=sv("v12", 12, "V0", first="v1"))
    eq(b.out(sealed), ("not_applied", "session_sealed"), "a claim after the session's end (its save covered too)")
    eq(L(b, e, "position"), posv(G_, 3 * MIN, claimed), "the end wrote under the claim; the sealed claim nothing")
    # A claim on a closed generation.
    S2 = b.uuid()
    b.start("A", e2, S2, 0)
    u = b.pod("Q", "unsubscribe", P_, G_)
    closed = rcp(b, "A", e2, S2, 1, MIN, MIN, after=[])
    eq((b.out(u), b.out(closed)), (("applied", None), ("not_applied", "generation_closed")), "generation_closed")


def test_claim_save_must_be_new():
    """A claiming checkpoint must cover a save that is new on its writing path. Its counterexample: A
    starts S (baseline V0) at 0:00 and exports save s1 (index 1, first s1) at 0:10; Q starts
    E at 0:20 and owns. A then sends ordinal 2, claiming, with exactly the same save s1
    (id, index, first, baseline) and position 0:10: no new save happened, so it is
    `rejected` / `invalid_payload` and Q keeps 0:20. The valid counterpart: A's ordinal 3
    claim with the fresh save s2 (index 2, first s2) owns: 0:10 under it."""
    b = B("1.3: a claim re-exporting its path's save is rejected; a fresh save claims", next_prefix(), group=FILE)
    for dev in ("A", "Q"):
        b.dev(dev)
    s = b.subscribe("A", U1)
    P_, G_ = pg(s)
    e = st.ref(P_, G_, "ep-e", "E")
    S = b.uuid()
    b.start("A", e, S, 0, baseline=BLN("V0"))
    a1 = b.cp("A", e, S, 1, 10 * SEC, 10 * SEC, save=sv("s1", 1, "V0", first="s1"))
    q = b.start("Q", e, b.uuid(), 20 * SEC, after=[a1])
    eq(L(b, e, "position"), posv(G_, 20 * SEC, q), "Q owns at 0:20")
    again = rcp(b, "A", e, S, 2, 10 * SEC, 10 * SEC, observed=q, save=sv("s1", 1, "V0", first="s1"), reject=True)
    eq(b.out(again), ("rejected", "invalid_payload"), "the same save again is no new save")
    eq(L(b, e, "position"), posv(G_, 20 * SEC, q), "the rejected claim grants nothing")
    fresh = rcp(b, "A", e, S, 3, 10 * SEC, 10 * SEC, observed=q, save=sv("s2", 2, "V0", first="s2"))
    eq((b.out(fresh), L(b, e, "position")), (("applied", None), posv(G_, 10 * SEC, fresh)),
       "a fresh save claims: 0:10 under it")


def test_claim_save_new_per_path():
    """Path scoping across an undetected copy, in both delivery orders. A
    starts T (baseline W0) and exports w120 (index 120) at 10:00; its library is copied to
    B. On B's writing path w120 is shared, so B claiming with w120 again, or with a new ID
    at the same index 120, is rejected; B's fresh b121 claims. The original's own claim
    with a121 (index 121 too, on its own path) is new as well.

    By hand: the rejected claims grant nothing; the later of the two valid claims owns:
    B's b121 delivered last (0:10:00 under b121), or A's a121. When A's claim arrives first
    it holds the author position, and B's facts form a derived branch (PROTOCOL §6.10) at their
    acceptance: the check still finds w120 on B's path."""
    for first in ("B", "A"):
        b = B("1.3: a claim's save is new per writing path (an undetected copy), %s delivered first" % first,
              next_prefix(), group=FILE)
        for dev in ("A",):
            b.dev(dev)
        s = b.subscribe("A", U1)
        P_, G_ = pg(s)
        e = st.ref(P_, G_, "ep-e", "E")
        T = b.uuid()
        b.start("A", e, T, 0, baseline=BLN("W0"))
        w = b.cp("A", e, T, 1, 10 * MIN, 10 * MIN, save=sv("w120", 120, "W0", first="w1"))
        b.clone("A", "B")
        out = {}

        def copy_side():
            out["rej1"] = rcp(b, "B", e, T, 2, 10 * MIN, 10 * MIN, observed=w, save=sv("w120", 120, "W0", first="w1"),
                              reject=True)
            out["rej2"] = rcp(b, "B", e, T, 3, 10 * MIN, 10 * MIN, observed=w,
                              save=sv("b120", 120, "B1", first="b120"), reject=True)
            out["b1"] = rcp(b, "B", e, T, 4, 10 * MIN, 10 * MIN, observed=w, save=sv("b121", 121, "B1", first="b121"))

        def original_side():
            out["a2"] = rcp(b, "A", e, T, 2, 10 * MIN, 10 * MIN, observed=w, save=sv("a121", 121, "W0", first="a121"))
        for side in ((copy_side, original_side) if first == "B" else (original_side, copy_side)):
            side()
        eq((b.out(out["rej1"]), b.out(out["rej2"])), (("rejected", "invalid_payload"), ("rejected", "invalid_payload")),
           "%s: on B's path the shared w120 is covered: the same save, or index 120 again" % b.name)
        eq((b.out(out["b1"]), b.out(out["a2"])), (("applied", None), ("applied", None)),
           "%s: B's b121 and A's a121 are new on their own paths" % b.name)
        last = out["a2"] if first == "B" else out["b1"]
        eq(L(b, e, "position"), posv(G_, 10 * MIN, last), "%s: the later claim owns" % b.name)


TESTS = [
    ("resume claims: scenario 5a", test_scenario_5a),
    ("resume claims: the earlier example (scenario 5)", test_earlier_example),
    ("resume claims: scenario 1a's literal timeline", test_scenario_1a_timeline),
    ("resume claims: claims belong to their writing path", test_copy_paths),
    ("resume claims: reset -> resume -> completion", test_reset_resume_completion),
    ("resume claims: a stale end", test_stale_end),
    ("resume claims: a stale finalization; the start-ID alias", test_stale_finalize_and_alias),
    ("resume claims: a stale leave", test_stale_leave),
    ("resume claims: adoption -> finalization", test_adoption_finalization),
    ("resume claims: claim validation and outcomes", test_claim_validation),
    ("resume claims: a claim's save must be new on its writing path", test_claim_save_must_be_new),
    ("resume claims: a claim's save is new per writing path", test_claim_save_new_per_path),
]
