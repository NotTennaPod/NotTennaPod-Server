#!/usr/bin/env python3
"""Hand-written cases of the queue replay (PROTOCOL §7.6; scenario 9a, §16), written from the
protocol and Android 3.12.1's queue commands, never from the Go code.

Each case states its exact expected order, computed by hand, in its docstring, and the
builder writes it beside the case (`expected_order`, `by_hand`): informative members that
implementations do not read. The cases are registered with selftest_rules.py and written
to contract/vectors/rules_queue_replay.json by `selftest_rules.py --groups DIR`.

Every case with concurrent devices is built in several delivery orders, each a vector case
of its own with the same expected order; check_queue_replay.py runs every case further in
every causal arrival order (bounded), as fresh admissions and as a recovery of one fixed
archive with request splits, and cross-checks the incremental replay after every prefix.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ntp_contract as c  # noqa: E402
import rules  # noqa: E402
import selftest_rules as st  # noqa: E402
from selftest_rules import (B, END, START, AFTER, K, T0, DAY, MIN, D, U1, U2, U3, NS, NOQ, check, eq, fixed_op, pg,  # noqa: E402
                            ref, qref, ifields, pfields, gocc, ofields, oref, keys_in_order, det_uuid, upload_with_deps,
                            rend, MEM, mem)

FILE = "rules_queue_replay.json"

_PREFIX = [0xb1000000]


def next_prefix():
    _PREFIX[0] += 1
    return _PREFIX[0]


def QB(name, prefix=None, devices=("A", "B", "C", "D", "E", "F", "W")):
    b = B(name, prefix if prefix is not None else next_prefix(), group=FILE)
    for d in devices:
        b.dev(d, caps=NOQ if d == "W" else None)  # device IDs in a fixed order
    return b


def note(b, order, by_hand, exception=None):
    """Asserts the order and writes it beside the case."""
    eq(b.queue(), order, "%s: the order" % b.name)
    b.notes = {"name": b.name, "expected_order": list(order), "by_hand": by_hand}
    if exception is not None:
        b.notes["arrival_order_exception"] = exception


def in_orders(name, setup, batches, orders, expected, by_hand, check_each=None):
    """One case per delivery order of the devices' offline batches (each a list of
    thunk(rt)); every order must give `expected`."""
    prefix = next_prefix()
    for order in orders:
        b = QB("%s; delivered: %s" % (name, ", ".join(order)), prefix)
        ctx = setup(b, prefix)
        rt = b.now + 30 * DAY  # the phones reconnect after every offline event
        for k in order:
            for thunk in batches(b, prefix, ctx)[k]:
                rt += 1000
                thunk(rt)
        note(b, expected, by_hand)
        if check_each is not None:
            check_each(b, ctx)


def resume_save(sid, ordinal):
    """The new, non-passive save a resume claim exports (PROTOCOL §8.1; its session's other
    exports carry no save identities, so it is new on its path)."""
    label = "resume/%s/%d" % (sid, ordinal)
    return st.sv(label, ordinal, "resume/" + sid, first=label)


def base_queue(b, prefix, titles, dev="A", url=U1, op0=1):
    """`dev` subscribes and adds the episodes `titles` at the end, in order (online).
    Returns (the last of these facts, which the other phones saw; P; G; refs)."""
    s = b.subscribe(dev, url, op=fixed_op(prefix, op0))
    P, G = pg(s)
    refs = dict((t, ref(P, G, t.lower(), t)) for t in titles)
    if titles:
        keys = keys_in_order(["%d/base/%s" % (prefix, t) for t in titles])
        s = b.qadd(dev, [(refs[t], k) for t, k in zip(titles, keys)], place=END, op=fixed_op(prefix, op0 + 1))
    return s, P, G, refs


# ---------------------------------------------------------------------------
# PROTOCOL §7.6: the six examples
# ---------------------------------------------------------------------------


def test_q0_back_adds():
    """A worked example of PROTOCOL §7.6 ("the order you added them"). The queue is [Z]. Monday the iPhone
    (A) adds X at the back; Tuesday the Android phone (B), offline, adds Y at the back. A
    back add is resolved to `end` (PROTOCOL §7.6). By hand: [Z] -> X at the end [Z, X] -> Y at the
    end [Z, X, Y], whichever phone reconnects first."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["Z"])
        refs.update(X=ref(P, G, "x", "X"), Y=ref(P, G, "y", "Y"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, x):
        r, t = x["refs"], x["t"]
        return {"A": [lambda rt: b.qadd("A", [(r["X"], K(None, None, "q0/x"))], place=END, t=t + DAY, rt=rt,
                                        op=fixed_op(prefix, 10))],
                "B": [lambda rt: b.qadd("B", [(r["Y"], K(None, None, "q0/y"))], place=END, after=[x["s"]],
                                        t=t + 2 * DAY, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("Monday X at the back, Tuesday Y at the back", setup, batches, [("A", "B"), ("B", "A")],
              ["Z", "X", "Y"], "[Z] -> [Z, X] -> [Z, X, Y]")


def test_q0_front_adds():
    """A worked example of PROTOCOL §7.6: the back adds above, made with "add to front"
    instead (resolved to `start`). By hand: [Z] -> X at the start [X, Z] -> Y at the start [Y, X, Z]."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["Z"])
        refs.update(X=ref(P, G, "x", "X"), Y=ref(P, G, "y", "Y"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, x):
        r, t = x["refs"], x["t"]
        return {"A": [lambda rt: b.qadd("A", [(r["X"], K(None, None, "q0f/x"))], place=START, t=t + DAY, rt=rt,
                                        op=fixed_op(prefix, 10))],
                "B": [lambda rt: b.qadd("B", [(r["Y"], K(None, None, "q0f/y"))], place=START, after=[x["s"]],
                                        t=t + 2 * DAY, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("Monday X to the front, Tuesday Y to the front", setup, batches, [("A", "B"), ("B", "A")],
              ["Y", "X", "Z"], "[Z] -> [X, Z] -> [Y, X, Z]")


def test_q0_three_back_adds():
    """A worked example of PROTOCOL §7.6: the queue is [Z]; the iPhone
    adds X (time 1); the Android phone adds Y (time 2), then W (time 3), all at the back.
    By hand: [Z, X] -> [Z, X, Y] -> [Z, X, Y, W] in all three delivery orders. Delivered Y, X,
    W, the late X is placed before Y; when the key X's commit writes does not fall before Y's,
    that commit also publishes a layout that re-keys Y. The case name seeds the server's
    choices, and it is chosen so that this corrective layout is needed (asserted below)."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["Z"])
        refs.update(X=ref(P, G, "x", "X"), Y=ref(P, G, "y", "Y"), W=ref(P, G, "w", "W"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, x):
        r, t = x["refs"], x["t"]
        return {"A": [lambda rt: b.qadd("A", [(r["X"], K(None, None, "q03/x"))], place=END, t=t + 1000, rt=rt,
                                        op=fixed_op(prefix, 10))],
                "B1": [lambda rt: b.qadd("B", [(r["Y"], K(None, None, "q03/y"))], place=END, after=[x["s"]],
                                         t=t + 2000, rt=rt, op=fixed_op(prefix, 11))],
                "B2": [lambda rt: b.qadd("B", [(r["W"], K(None, None, "q03/w"))], place=END, t=t + 3000, rt=rt,
                                         op=fixed_op(prefix, 12))]}

    def check_each(b, x):
        if b.name.endswith("delivered: B1, A, B2"):
            eq(sum(1 for d in b.engine.created if d["decision_type"] == rules.LAYOUT), 3,
               "%s: the late X's commit re-keys Y with a layout" % b.name)
    in_orders("X (time 1) on one phone, Y (2) then W (3) on the other, all at the back", setup, batches,
              [("A", "B1", "B2"), ("B1", "B2", "A"), ("B1", "A", "B2")], ["Z", "X", "Y", "W"],
              "[Z] -> [Z, X] -> [Z, X, Y] -> [Z, X, Y, W]", check_each)


def test_q0_keep_sorted_on_then_add():
    """A worked example of PROTOCOL §7.6: the queue is [Z, A]; the iPhone turns on keep-sorted
    by title (the dialog: the policy and its sort); later the Android phone, which never saw
    it, adds B at the back. By hand: policy on; sort [A, Z]; B at the end [A, Z, B], and
    keep-sorted is on at B's place: sorted [A, B, Z], in both delivery orders."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["Z", "A"])
        refs.update(B=ref(P, G, "b", "B"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, x):
        r, t = x["refs"], x["t"]
        return {"P": [lambda rt: b.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z", t=t + 1000, rt=rt,
                                           op=fixed_op(prefix, 10)),
                      lambda rt: b.qsort("A", "episode_title_a_z", observed=[r["Z"], r["A"]], t=t + 1001, rt=rt,
                                         op=fixed_op(prefix, 11))],
                "Q": [lambda rt: b.qadd("B", [(r["B"], K(None, None, "q04/b"))], place=END, after=[x["s"]],
                                        t=t + 2000, rt=rt, op=fixed_op(prefix, 12))]}
    in_orders("keep-sorted turned on, then another phone's add", setup, batches, [("P", "Q"), ("Q", "P")],
              ["A", "B", "Z"], "sort [A, Z]; B at the end [A, Z, B]; sorted at B's place [A, B, Z]")


def test_q0_two_drags():
    """A worked example of PROTOCOL §7.6: the queue is [P, A, E, F]; one phone drags E to just after P (time 1),
    the other drags F to just after P (time 2). By hand: [P, E, A, F] -> [P, F, E, A]."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["P", "A", "E", "F"])
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, x):
        r, t = x["refs"], x["t"]
        return {"1": [lambda rt: b.qmove("A", r["E"], K(None, None, "q05/e"), place=AFTER(r["P"]), t=t + 1000, rt=rt,
                                         op=fixed_op(prefix, 10))],
                "2": [lambda rt: b.qmove("B", r["F"], K(None, None, "q05/f"), place=AFTER(r["P"]), after=[x["s"]],
                                         t=t + 2000, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("two drags to just after P (E first, F later)", setup, batches, [("1", "2"), ("2", "1")],
              ["P", "F", "E", "A"], "[P, A, E, F] -> E after P [P, E, A, F] -> F after P [P, F, E, A]")


def test_q0_duplicate_add():
    """A worked example of PROTOCOL §7.6: the queue is [A]; the iPhone adds X (time 1); the Android
    phone, not having seen X, adds Y (time 2), then X (time 3). Android ignores an add of an
    episode already queued (`addQueueItem` skips it). By hand: [A, X] -> [A, X, Y] -> X is
    in: nothing placed, nothing sorted: [A, X, Y], in all three delivery orders (replaying
    every add as a reposition would give [A, Y, X])."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A"])
        refs.update(X=ref(P, G, "x", "X"), Y=ref(P, G, "y", "Y"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, x):
        r, t = x["refs"], x["t"]
        return {"I": [lambda rt: b.qadd("A", [(r["X"], K(None, None, "q06/x1"))], place=END, t=t + 1000, rt=rt,
                                        op=fixed_op(prefix, 10))],
                "A1": [lambda rt: b.qadd("B", [(r["Y"], K(None, None, "q06/y"))], place=END, after=[x["s"]],
                                         t=t + 2000, rt=rt, op=fixed_op(prefix, 11))],
                "A2": [lambda rt: b.qadd("B", [(r["X"], K(None, None, "q06/x2"))], place=END, t=t + 3000, rt=rt,
                                         op=fixed_op(prefix, 12))]}

    def check_each(b, x):
        eq(MEM(b, x["refs"]["X"])["in_queue"], True, "%s: X in" % b.name)
        eq(b.out(fixed_op(int(b.prefix), 12)), ("applied", None), "%s: the duplicate add is still a fact" % b.name)
    in_orders("a duplicate add is ignored", setup, batches, [("I", "A1", "A2"), ("A1", "A2", "I"), ("A1", "I", "A2")],
              ["A", "X", "Y"], "[A] -> [A, X] -> [A, X, Y] -> X already in: [A, X, Y]", check_each)
    # A partial bulk duplicate: only the absent member is placed, in its requested order.

    def setup2(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A", "X"])
        refs.update(Y=ref(P, G, "y", "Y"), W=ref(P, G, "w", "W"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches2(b, prefix, x):
        r, t = x["refs"], x["t"]
        return {"B": [lambda rt: b.qadd("B", [(r["Y"], K(None, None, "q06b/y")), (r["X"], K(None, None, "q06b/x")),
                                              (r["W"], K(None, None, "q06b/w"))], place=START, after=[x["s"]],
                                        t=t + 2000, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("§7.3: a partial bulk duplicate", setup2, batches2, [("B",)], ["Y", "W", "A", "X"],
              "[A, X]; the add [Y, X, W] at the start places only Y and W (X is in): [Y, W, A, X]")


# ---------------------------------------------------------------------------
# PROTOCOL §7.6: counterexamples
# ---------------------------------------------------------------------------


def _metadata_race(b, prefix, order):
    """The history: [Y] (title M); A adds X (time 10; X's known title Z); B, offline, sorts
    by title (time 20); X's metadata becomes A (time 30, depending on the add). Delivered in
    `order`."""
    s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    y, x = ref(P, G, "y"), ref(P, G, "x")
    b.observe("A", P, [(y, None, ifields("M", T0)), (x, None, ifields("Z", T0))], op=fixed_op(prefix, 2))
    add0 = b.qadd("A", [(y, K(None, None, "b1/y"))], place=END, op=fixed_op(prefix, 3))
    t = b.now
    steps = {
        "add": lambda rt: b.qadd("A", [(x, K(None, None, "b1/x"))], place=END, t=t + 10000, rt=rt, op=fixed_op(prefix, 10)),
        "sort": lambda rt: b.qsort("B", "episode_title_a_z", observed=[y], after=[add0], t=t + 20000, rt=rt,
                                   op=fixed_op(prefix, 11)),
        "meta": lambda rt: b.observe("A", P, [(x, b.revision(x["episode_id"]), ifields("A", T0))], t=t + 30000, rt=rt,
                                     op=fixed_op(prefix, 12), batch=fixed_op(prefix, 13)),
    }
    rt = t + 100000
    for k in order:
        rt += 1000
        steps[k](rt)
    return x, y


def test_metadata_racing_a_sort():
    """PROTOCOL §7.6 documented exception: a sort uses the canonical values the server recorded
    when it admitted the sort (PROTOCOL §7.6). The titles: Y = M; X = Z, then A (time 30).

    * add, sort, metadata: the sort's receipt recorded X = Z: [Y (M), X (Z)].
    * sort, add, metadata: the sort's receipt has no X (admitted before X's add): X's
      latest placement at or before the sort recorded Z: [Y, X].
    * add, metadata, sort: the sort's receipt recorded X = A: [X (A), Y (M)].
    Each result is a correct sort by the values recorded; one archive replays identically
    in every order (check_queue_replay.py)."""
    for order, want, why in ((("add", "sort", "meta"), ["Y", "X"], "the sort recorded X = Z: [Y, X]"),
                             (("sort", "add", "meta"), ["Y", "X"],
                              "the sort recorded only Y; X's placement recorded Z: [Y, X]"),
                             (("add", "meta", "sort"), ["X", "Y"], "the sort recorded X = A: [X, Y]")):
        b = QB("metadata racing a sort, admitted %s" % ", ".join(order))
        x, y = _metadata_race(b, int(b.prefix), order)
        names = {x["episode_id"]: "X", y["episode_id"]: "Y"}
        got = [names[m["episode_id"]] for m in b.engine._current_queue()]
        eq(got, want, b.name)
        b.notes = {"name": b.name, "expected_order": want, "by_hand": why,
                   "arrival_order_exception": "the documented exception: a metadata correction racing a sort"}


def test_smart_shuffle_with_a_removed_episode():
    """A1 (podcast PA), then B1 and B2 (podcast PB, B1 published before B2) are queued;
    B1 is removed; Smart Shuffle (old to new) sorts the in-members only. The roles are chosen
    so that PA's ID sorts before PB's (equal groups go by ascending podcast ID).
    By hand: in-members [A1, B2]: two groups of one, PA's first into slot 0: [A1, B2]
    (sorting every episode ever placed, [A1, B1, B2] -> [B1, B2, A1], then filtering, would
    give [B2, A1])."""
    prefix = next_prefix()
    b = QB("Smart Shuffle sorts the in-members only", prefix)
    s1 = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    s2 = b.subscribe("A", U2, op=fixed_op(prefix, 2))
    (P1, G1), (P2, G2) = pg(s1), pg(s2)
    (PA, GA), (PB, GB) = sorted([(P1, G1), (P2, G2)])
    a1, b1, b2 = qref(PA, GA, "a1", "A1", T0), qref(PB, GB, "b1", "B1", T0 + DAY), qref(PB, GB, "b2", "B2", T0 + 2 * DAY)
    keys = keys_in_order(["b2/a1", "b2/b1", "b2/b2"])
    add = b.qadd("A", [(a1, keys[0]), (b1, keys[1]), (b2, keys[2])], place=END)
    b.qremove("B", [b1], after=[add])
    b.qsort("B", "smart_shuffle_old_new", observed=[a1, b2])
    note(b, ["A1", "B2"], "in-members [A1, B2]; two groups of one; PA (smaller ID) first: [A1, B2]")


def test_anchor_of_a_suppressed_filing():
    """D is filed automatically at the back (queue action); phone C sees it and adds X
    after D; phone B removes D explicitly (it saw D too). An explicit decision bars the
    automatic add whatever the order: D is a ghost at its place (PROTOCOL §7.6), never a member; X
    lands after D's ghost. By hand: [A] -> D's ghost [A, (D)] -> X after D [A, (D), X] ->
    D out: [A, X], in both delivery orders of B and C."""
    qpol = {"new_episodes_action": "queue", "keep_updated": True}

    def setup(b, prefix):
        s = b.subscribe("A", U1, policy=qpol, op=fixed_op(prefix, 1))
        P, G = pg(s)
        old = ref(P, G, "old", "Old")
        b.fetch(P, [(old, ifields("Old", T0 - DAY))])
        a, d, x = ref(P, G, "a", "A"), ref(P, G, "d", "D"), ref(P, G, "x", "X")
        b.qadd("A", [(a, K(None, None, "b3/a"))], place=END, op=fixed_op(prefix, 2))
        b.fetch(P, [(old, ifields("Old", T0 - DAY)), (d, ifields("D", T0))])
        adm = st.ADM(b, G, d)
        return {"a": a, "d": d, "x": x, "adm": adm["decision_id"], "t": b.now}

    def batches(b, prefix, ctx):
        t = ctx["t"]
        return {"B": [lambda rt: b.qremove("B", [ctx["d"]], after=[ctx["adm"]], t=t + 1000, rt=rt,
                                           op=fixed_op(prefix, 10))],
                "C": [lambda rt: b.qadd("C", [(ctx["x"], K(None, None, "b3/x"))], place=AFTER(ctx["d"]),
                                        after=[ctx["adm"]], t=t + 2000, rt=rt, op=fixed_op(prefix, 11))]}

    def check_each(b, ctx):
        eq(MEM(b, ctx["d"])["in_queue"], False, "%s: D out" % b.name)
    in_orders("an add after an automatic filing that a concurrent removal suppresses", setup, batches,
              [("B", "C"), ("C", "B")], ["A", "X"], "[A] -> [A, (D)] -> [A, (D), X] -> [A, X]", check_each)


def test_after_current_and_undo():
    """After current past a downloading episode: the queue is [P, D, Z], the device's
    current episode P, D downloading: Android inserts X after D (`ItemEnqueuePositionCalculator`):
    `after D`. Concurrently another phone adds W at the back (earlier). By hand: [P, D, Z] ->
    W at the end [P, D, Z, W] -> X after D [P, D, X, Z, W].

    With the current episode not queued, the front scan from index 0 skips the downloading
    prefix: [D1 (downloading), A, B] -> `after D1` -> [D1, X, A, B].

    Undo after a front add on the same phone: [A, B, C]; B removed at index 1; a download
    request adds X at the front [X, A, C]; Undo restores B at its saved index 1 resolved at
    Undo time: after X: [X, B, A, C] (not after A, its predecessor at removal).

    Undo after a concurrent front add: phone B adds X at the front (time 2) while phone A,
    which never saw it, removes B (time 1) and undoes it (time 3) at index 1 of its own
    queue [A, C]: after A. By hand: [A, B, C] -> B out -> [X, A, (B), C]... the restore
    places B after A: [X, A, B, C]."""
    prefix = next_prefix()
    b = QB("after current past a downloading episode", prefix)
    s, P, G, r = base_queue(b, prefix, ["P", "D", "Z"])
    x, w = ref(P, G, "x", "X"), ref(P, G, "w", "W")
    t = b.now
    b.qadd("B", [(w, K(None, None, "b4/w"))], place=END, after=[s], t=t + 1000, rt=t + 5000)
    b.qadd("A", [(x, K(None, None, "b4/x"))], place=AFTER(r["D"]), t=t + 2000, rt=t + 6000)
    note(b, ["P", "D", "X", "Z", "W"], "[P, D, Z] -> [P, D, Z, W] -> X after D: [P, D, X, Z, W]")
    b = QB("after current, the current episode not queued: the front scan past the downloading prefix")
    s, P, G, r = base_queue(b, int(b.prefix), ["D1", "A", "B"])
    b.qadd("A", [(ref(P, G, "x", "X"), K(None, None, "b4c/x"))], place=AFTER(r["D1"]))
    note(b, ["D1", "X", "A", "B"], "[D1, A, B] -> after D1: [D1, X, A, B]")
    b = QB("Undo after a front add on the same phone (the saved index at Undo time)")
    s, P, G, r = base_queue(b, int(b.prefix), ["A", "B", "C"])
    x = ref(P, G, "x", "X")
    b.qremove("A", [r["B"]])
    b.qadd("A", [(x, K(None, None, "b4u/x"))], place=START)
    b.qrestore("A", [(r["B"], K(None, None, "b4u/b"))], place=AFTER(x))
    note(b, ["X", "B", "A", "C"], "[A, B, C] -> [A, C] -> [X, A, C] -> B at index 1 (after X): [X, B, A, C]")

    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A", "B", "C"])
        refs.update(X=ref(P, G, "x", "X"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, ctx):
        r, t = ctx["refs"], ctx["t"]
        return {"A": [lambda rt: b.qremove("A", [r["B"]], t=t + 1000, rt=rt, op=fixed_op(prefix, 10)),
                      lambda rt: b.qrestore("A", [(r["B"], K(None, None, "b4uc/b"))], place=AFTER(r["A"]), t=t + 3000,
                                            rt=rt, op=fixed_op(prefix, 11))],
                "B": [lambda rt: b.qadd("B", [(r["X"], K(None, None, "b4uc/x"))], place=START, after=[ctx["s"]],
                                        t=t + 2000, rt=rt, op=fixed_op(prefix, 12))]}
    in_orders("Undo after a concurrent front add (resolved against the undoing phone's queue)", setup, batches,
              [("A", "B"), ("B", "A")], ["X", "A", "B", "C"],
              "[A, B, C] -> B out -> X at the start [X, A, (B), C] -> B restored after A: [X, A, B, C]")


def test_random_sort():
    """The queue is [A, B, C]; phone B adds D at the back (time 1), unseen by phone A,
    which sorts randomly (time 2) and records its permutation [C, A, B] of the episodes it
    observed. By hand: [A, B, C, D] -> the observed in-members A, B, C take their slots
    (0, 1, 2) in the permutation's order; D keeps slot 3: [C, A, B, D]."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A", "B", "C"])
        refs.update(D=ref(P, G, "d", "D"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, ctx):
        r, t = ctx["refs"], ctx["t"]
        return {"A": [lambda rt: b.qpolicy("A", keep_sorted=False, sort_order="random", t=t + 2000, rt=rt,
                                           op=fixed_op(prefix, 10)),
                      lambda rt: b.qsort("A", "random", observed=[r["A"], r["B"], r["C"]],
                                         permutation=[r["C"], r["A"], r["B"]], t=t + 2001, rt=rt, op=fixed_op(prefix, 11))],
                "B": [lambda rt: b.qadd("B", [(r["D"], K(None, None, "b5/d"))], place=END, after=[ctx["s"]],
                                        t=t + 1000, rt=rt, op=fixed_op(prefix, 12))]}
    in_orders("a random sort replays its recorded permutation", setup, batches, [("A", "B"), ("B", "A")],
              ["C", "A", "B", "D"], "[A, B, C, D] -> observed A, B, C permuted in slots 0-2; D keeps slot 3")


def _merged_pair(b, prefix, titles):
    """Podcasts P (U1, the survivor: created first) and Q (U2) both carry GUID `shared`,
    titled titles[0] in P and titles[1] in Q; Q is later permanently redirected onto P's URL
    (PROTOCOL §9.7: a podcast merge; Q's `shared` merges into P's). Returns the refs and a thunk
    making the merge."""
    sp = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    sq = b.subscribe("A", U2, op=fixed_op(prefix, 2))
    (P, GP), (Q, GQ) = pg(sp), pg(sq)
    b.fetch(P, [(gocc("shared"), ofields(titles[0], T0, "https://cdn.example.org/s.mp3")),
                (gocc("mike"), ofields("Mike", T0 + DAY, "https://cdn.example.org/m.mp3"))])
    b.fetch(Q, [(gocc("shared"), ofields(titles[1], T0, "https://cdn.example.org/s.mp3"))])
    e1, e2, m = oref(P, GP, "shared"), oref(Q, GQ, "shared"), oref(P, GP, "mike")

    def merge():
        b.fetch(Q, [(gocc("shared"), ofields(titles[1], T0, "https://cdn.example.org/s.mp3"))], redirects=[(301, U1)],
                moved_to=U1)
        check(b.made(rules.PODCAST_MERGE), "merged")
    return P, GP, Q, GQ, e1, e2, m, merge


def test_merges():
    """The replay applies the final identity mapping from the start (PROTOCOL §7.6).

    * Conflicting recorded values: P's `shared` (E1, title Zulu), Q's `shared` (E2,
      title Alpha) and Mike are queued [Mike, E1, E2]; a title sort records Mike, Zulu and
      Alpha; then Q merges into P (E2 -> E1). By hand: under the final mapping E2's add is a
      duplicate: [Mike, E]; the sort's values for E are the survivor's own record (Zulu), not
      the alias's: a-z [Mike, E]. (The alias's Alpha would give [E, Mike].)
    * A random permutation naming both aliases: [Mike, E1, E2] sorted randomly with the
      permutation [E2, Mike, E1]; after the merge it maps to [E, Mike, E], first occurrence
      kept: [E, Mike] in the slots of the in-members [Mike, E]: [E, Mike] (the last
      occurrence would give [Mike, E]).
    * A run anchored to its own merged member: [Mike, E1] with E1 removed (out, its slot
      kept), then a phone adds [E2, X] after E1 (both new to it); after the merge E2 is E1:
      the run [E, X] is anchored to E, which is in the run: it goes after the nearest
      structural predecessor of E not in the run, Mike: [Mike, E, X]."""
    b = QB("aliases with conflicting recorded values")
    P, GP, Q, GQ, e1, e2, m, merge = _merged_pair(b, int(b.prefix), ("Zulu", "Alpha"))
    keys = keys_in_order(["n5/m", "n5/e1", "n5/e2"])
    add = b.qadd("A", [(m, keys[0]), (e1, keys[1]), (e2, keys[2])], place=END)
    b.qsort("B", "episode_title_a_z", observed=[m, e1, e2], after=[add])
    merge()
    E = b.engine._root(e1["episode_id"])
    eq([x["episode_id"] for x in b.engine._current_queue()], [m["episode_id"], E], "survivor's own value")
    b.notes = {"name": b.name, "expected_order": ["Mike", "E (Zulu)"],
               "by_hand": "[Mike, E1, E2] -> E2 maps to E (a duplicate add): [Mike, E]; sorted with E's own Zulu"}
    b = QB("a random permutation naming both aliases")
    P, GP, Q, GQ, e1, e2, m, merge = _merged_pair(b, int(b.prefix), ("Zulu", "Alpha"))
    keys = keys_in_order(["n5r/m", "n5r/e1", "n5r/e2"])
    add = b.qadd("A", [(m, keys[0]), (e1, keys[1]), (e2, keys[2])], place=END)
    b.qpolicy("B", keep_sorted=False, sort_order="random", after=[add])
    b.qsort("B", "random", observed=[m, e1, e2], permutation=[e2, m, e1])
    merge()
    E = b.engine._root(e1["episode_id"])
    eq([x["episode_id"] for x in b.engine._current_queue()], [E, m["episode_id"]], "first occurrence")
    b.notes = {"name": b.name, "expected_order": ["E", "Mike"],
               "by_hand": "permutation [E2, Mike, E1] -> [E, Mike, E] -> first occurrence [E, Mike]"}
    b = QB("a run anchored to its own merged member")
    P, GP, Q, GQ, e1, e2, m, merge = _merged_pair(b, int(b.prefix), ("Shared", "Shared"))
    x = oref(P, GP, "x-item")
    keys = keys_in_order(["b7/m", "b7/e1"])
    b.qadd("A", [(m, keys[0]), (e1, keys[1])], place=END)
    rm = b.qremove("A", [e1])
    b.qadd("B", [(e2, K(None, None, "b7/e2")), (x, K(None, None, "b7/x"))], place=AFTER(e1), after=[rm])
    merge()
    E = b.engine._root(e1["episode_id"])
    eq([y["episode_id"] for y in b.engine._current_queue()], [m["episode_id"], E, x["episode_id"]], "self-anchor")
    b.notes = {"name": b.name, "expected_order": ["Mike", "E", "X"],
               "by_hand": "[Mike, (E1)] -> [E2 = E, X] after E1 = E, in the run: after Mike: [Mike, E, X]"}


def test_smart_shuffle_across_a_merge():
    """R's episode A and P's and Q's `shared` (E1, E2; Q later merges into P) are queued
    [A, E1, E2] and Smart Shuffled (old to new) before the merge. Under the final mapping the
    queue is [A, E] (E2 a duplicate add) and the shuffle has two groups of one, R's and P's,
    placed in ascending podcast ID order (PROTOCOL §7.6). Merging two places after the fact would keep
    whatever the three-way shuffle gave."""
    b = QB("Smart Shuffle across a merge")
    prefix = int(b.prefix)
    P, GP, Q, GQ, e1, e2, m, merge = _merged_pair(b, prefix, ("Shared", "Shared"))
    sr = b.subscribe("A", U3, op=fixed_op(prefix, 3))
    R, GR = pg(sr)
    b.fetch(R, [(gocc("ra"), ofields("RA", T0 + 2 * DAY, "https://cdn.example.net/ra.mp3"))])
    a = oref(R, GR, "ra")
    keys = keys_in_order(["b7s/a", "b7s/e1", "b7s/e2"])
    add = b.qadd("A", [(a, keys[0]), (e1, keys[1]), (e2, keys[2])], place=END)
    b.qsort("B", "smart_shuffle_old_new", observed=[a, e1, e2], after=[add])
    merge()
    E = b.engine._root(e1["episode_id"])
    want = [E, a["episode_id"]] if P < R else [a["episode_id"], E]
    eq([x["episode_id"] for x in b.engine._current_queue()], want, "two groups of one by podcast ID")
    b.notes = {"name": b.name, "expected_order": ["E", "A"] if P < R else ["A", "E"],
               "by_hand": "final mapping: [A, E]; two groups of one; %s's podcast ID is smaller" % ("P" if P < R else "R")}


def import_decisions(prefix, import_id, podcasts, t0, seq0):
    """An Android database import (PROTOCOL §11.5) as its server decisions, as recovery uploads: per
    podcast one commit (`import.podcast`, its `podcast.alias`, its first fetch's
    `catalog.refresh` part 0 with the backup's items, an `import.episode` per item), then
    the `import.manifest`. `podcasts`: [(url, title, [(guid, title, pub, global queue index
    or None)])]. PROTOCOL §7.6's additions: each queued episode's `queue_index` (the
    backup's global Queue index) and `values` (what the import recorded for later sorts)."""
    out, seq = [], seq0
    queued = sorted((q, k, i) for k, (_, _, items) in enumerate(podcasts) for i, it in enumerate(items)
                    for q in [it[3]] if q is not None)
    keys, low = {}, None
    for q, k, i in queued:
        pid = det_uuid("%s/import/position/%d" % (import_id, q))
        low = c.queue_between(low, None, c.uuid_to_u(pid))
        keys[(k, i)] = (low, pid, q)
    n_episodes = 0
    for k, (url, ptitle, items) in enumerate(podcasts):
        seq += 1
        t = t0 + seq
        creation = det_uuid("%s/import/creation/%d" % (import_id, k))
        P = c.podcast_id(NS, creation)
        G = c.generation_id(NS, P, creation)
        norm = c.normalize_url(url)
        dp = det_uuid("%s/import/podcast/%d" % (import_id, k))

        def dec(did, dtype, idx, subject, inputs, result, deps):
            return {"decision": {"decision_id": did, "decision_type": dtype, "decision_version": 1,
                                 "dependencies": sorted(deps), "normalized_time": t, "epoch": 1, "seq": seq,
                                 "index": idx, "subject": subject, "inputs": inputs, "result": result}}
        out.append(dec(dp, rules.IMPORT_PODCAST, 0, "import:%s:podcast:%s" % (import_id, norm),
                       {"import_id": import_id, "source_feeds": [k + 1], "creation_id": creation},
                       {"podcast_id": P, "generation_id": G, "url": url, "title": ptitle, "state": "subscribed",
                        "keep_updated": True, "new_episodes_action": "global", "secret_id": None}, []))
        out.append(dec(det_uuid("%s/import/alias/%d" % (import_id, k)), rules.ALIAS, 1, "alias:" + norm, {},
                       {"podcast_id": P}, [dp]))
        R = det_uuid("%s/import/refresh/%d" % (import_id, k))
        ritems = []
        eps = []
        for i, (g, title, pub, q) in enumerate(items):
            E = c.episode_id(NS, P, "rss-guid", g)
            occ = {"k": "rss-guid", "v": g, "guid": g, "title": title, "publication_time": pub,
                   "enclosure_url": "https://media.example.org/%s.mp3" % g}
            ritems.append({"episode_id": E, "occurrence": dict(occ), "base": None,
                           "fields": ifields(title, pub, url="https://media.example.org/%s.mp3" % g)})
            res = {"episode_id": E, "generation_id": G, "occurrence": occ, "title": title,
                   "imported": {"play_state": "unplayed", "position": 0, "favorite": False, "last_played": 0,
                                "played_time": 0, "history": None, "units": []},
                   "baseline_id": "%s/%d" % (import_id, 1000 * (k + 1) + i)}
            if (k, i) in keys:
                key, pid, qi = keys[(k, i)]
                res.update(queue_key=key, position_id=pid, queue_index=qi,
                           values=[{"episode_id": E, "generation_id": G,
                                    "values": {"title": title, "podcast_id": P, "podcast_title": ptitle,
                                               "publication_time": pub, "duration": 0}}])
            eps.append(dec(det_uuid("%s/import/episode/%d/%d" % (import_id, k, i)), rules.IMPORT_EPISODE, 3 + i,
                           "import:%s:episode:%s" % (import_id, E),
                           {"import_id": import_id, "source_items": [1000 * (k + 1) + i]}, res, [dp]))
            n_episodes += 1
        out.append(dec(det_uuid("%s/import/part0/%d" % (import_id, k)), rules.CATALOG_REFRESH, 2,
                       "refresh:%s:%s:0" % (P, R),
                       {"podcast_id": P, "refresh_id": R, "part": 0, "outcome": "success", "generation_id": G,
                        "fetch_time": t},
                       {"population": True, "feed": {"base": None, "fields": pfields(ptitle)}, "items": ritems}, [dp]))
        out.extend(eps)
    seq += 1
    t = t0 + seq
    out.append({"decision": {"decision_id": det_uuid("%s/import/manifest" % import_id), "decision_type": rules.IMPORT_MANIFEST,
                             "decision_version": 1, "dependencies": [], "normalized_time": t, "epoch": 1, "seq": seq,
                             "index": 0, "subject": "import", "inputs": {"import_id": import_id},
                             "result": {"import_id": import_id, "kind": "android_db",
                                        "fingerprint": "%064x" % prefix,
                                        "result": {"imported": {"podcasts": len(podcasts), "episodes": n_episodes,
                                                                "queue": len(queued), "favorites": 0, "history": 0,
                                                                "inbox": 0},
                                                   "skipped": [], "ambiguous": [], "merged": []}}}})
    return out, seq


def _import_refs(dec_arrivals):
    """The imported episodes' EpisodeRefs by title (as a device that synced names them)."""
    gens = {}
    out = {}
    for a in dec_arrivals:
        d = a["decision"]
        if d["decision_type"] == rules.IMPORT_PODCAST:
            gens[d["result"]["generation_id"]] = d["result"]["podcast_id"]
        if d["decision_type"] == rules.IMPORT_EPISODE:
            r = d["result"]
            occ = dict(r["occurrence"])
            out[r["title"]] = {"episode_id": r["episode_id"], "podcast_id": gens[r["generation_id"]],
                               "generation_id": r["generation_id"], "occurrence": occ}
    return out


def test_imports():
    """An Android database import is one replay step for the whole account's
    imported queue, at its manifest's place, in the backup's global Queue order (PROTOCOL §7.6). * The backup's queue interleaves two podcasts, [A1, B1, A2]. Recovered with A's
      podcast decisions first, and with B's first: [A1, B1, A2] both times (appending each
      podcast's queue would give [A1, A2, B1] or [B1, A1, A2]).    * Then a phone adds X after A1, and another phone adds Y at the start:
      [Y, A1, X, B1, A2].
    The cases compare the queue field groups only (docs/CONTRACT.md: the import's other
    records are its own, outside this suite)."""
    for first in ("A", "B"):
        prefix = next_prefix()
        b = QB("an import interleaving two podcasts, %s's decisions recovered first; then placements" % first,
               prefix)
        b.field_groups = ["queue_membership", "queue_position"]
        imp = det_uuid("%d/import" % prefix)
        decs, seq = import_decisions(prefix, imp, [
            ("https://feeds.example.com/a.xml", "Show A", [("a1", "A1", T0, 0), ("a2", "A2", T0 + DAY, 2),
                                                         ("a3", "A3", T0 + 2 * DAY, None)]),
            ("https://feeds.example.com/b.xml", "Show B", [("b1", "B1", T0 + DAY, 1)])], T0, 0)
        refs = _import_refs(decs)
        pods = {}
        for a in decs[:-1]:
            d = a["decision"]
            pods.setdefault(d["seq"], []).append(a)
        groups = [pods[s] for s in sorted(pods)]
        if first == "B":
            groups = list(reversed(groups))
        for g in groups:
            for a in g:
                b.upload_decision(a["decision"])
        b.engine._ensure()
        eq(b.engine._current_queue(), [], "no step before the manifest")
        b.upload_decision(decs[-1]["decision"])
        b.now = T0 + 10 * DAY
        rend(b)
        eq(b.queue(), ["A1", "B1", "A2"], "%s: the backup's global order" % b.name)
        # The phones synced the import: their frontier is the import's decisions nothing
        # else depends on.
        held = set(d for a in decs for d in a["decision"]["dependencies"])
        sync = [a["decision"]["decision_id"] for a in decs if a["decision"]["decision_id"] not in held
                and a["decision"]["decision_type"] != rules.ALIAS]  # (a subscription decision is not a node)
        b.qadd("A", [(ref(refs["A1"]["podcast_id"], refs["A1"]["generation_id"], "x", "X"), K(None, None, "b8/x"))],
               place=AFTER(refs["A1"]), after=sync)
        b.qadd("B", [(ref(refs["B1"]["podcast_id"], refs["B1"]["generation_id"], "y", "Y"), K(None, None, "b8/y"))],
               place=START, after=sync)
        note(b, ["Y", "A1", "X", "B1", "A2"],
             "import [A1, B1, A2] (global order, whichever podcast recovers first) -> X after A1 -> Y at the start")


def test_bulk_move_to_top():
    """The queue is [A, B, C, D]; a web client selects [B, D] and moves them to the top:
    the complete selected run in its resulting order, at the start (not a sparse key
    changes, whose first-LIS form named [A, C]). Concurrently a phone adds E at the start
    (earlier). By hand: [E, A, B, C, D] -> the run [B, D] at the start: [B, D, E, A, C]."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A", "B", "C", "D"])
        refs.update(E=ref(P, G, "e", "E"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, ctx):
        r, t = ctx["refs"], ctx["t"]
        return {"W": [lambda rt: b.qrun("A", [r["B"], r["D"]], START, t=t + 2000, rt=rt, op=fixed_op(prefix, 10))],
                "P": [lambda rt: b.qadd("B", [(r["E"], K(None, None, "b9/e"))], place=START, after=[ctx["s"]],
                                        t=t + 1000, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("a bulk Move to top", setup, batches, [("W", "P"), ("P", "W")], ["B", "D", "E", "A", "C"],
              "[A, B, C, D] -> E at the start [E, A, B, C, D] -> [B, D] at the start [B, D, E, A, C]")


def test_recovery_of_layouts():
    """Recovery of layouts: the queue [A, B]; B is moved to the front: [B, A] (the move's
    commit publishes a layout L1). The server is restored to before the move. Request 1
    recovers the move; its end publishes a layout (the published keys [A, B] do not give
    [B, A]). Request 2 recovers L1: evidence only; the published keys give the order, so
    nothing more. Request 3 recovers nothing new. By hand: [B, A] after every request."""
    prefix = next_prefix()
    live = QB("recovery of layouts (live, not a vector)", prefix)
    live.vector_ok = False
    s, P, G, r = base_queue(live, prefix, ["A", "B"])
    k = len(live.arrivals)
    live.qmove("C", r["B"], K(None, None, "b10/b"), place=START, after=[s])
    eq(live.queue(), ["B", "A"], "live")
    ups = st.uploads(live, k)
    rs = st.restored(live, k, "recovery uploads a move, then its layout, then nothing new")
    rs.group, rs.prefix = FILE, prefix
    upload_entries = [x for cm in ups for x in cm]
    facts = [x for x in upload_entries if "fact" in x]
    decs = [x for x in upload_entries if "decision" in x]
    st.upload(rs, facts)
    n = len(rs.engine.created)
    rend(rs)
    eq([d["decision_type"] for d in rs.engine.created[n:]], [rules.LAYOUT], "request 1 publishes a layout")
    eq(rs.queue(), ["B", "A"], "after request 1")
    st.upload(rs, decs)
    n = len(rs.engine.created)
    rend(rs)
    eq(rs.engine.created[n:], [], "request 2: the recovered layout is evidence; nothing to publish")
    rend(rs)
    note(rs, ["B", "A"], "[A, B] -> B to the front: [B, A] (every request ends consistent)")


def test_late_suppression_of_an_anchor():
    """A late explicit decision suppresses an anchor's filing: D is filed automatically at the back
    of [A] (time 1); phone C adds X after D (2) and Y at the back (3). Weeks later an old
    explicit add of D at the back, made offline by phone B at time 4 without seeing the
    filing, arrives. An explicit decision bars the automatic add whatever the order: D's
    filing becomes a ghost from time 1, and B's add places D at the end at time 4.
    By hand: before: [A, D, X, Y]; after: [A, (D), X, Y] -> D at the end: [A, X, Y, D]."""
    qpol = {"new_episodes_action": "queue", "keep_updated": True}
    b = QB("a late explicit decision suppresses an anchor's filing")
    prefix = int(b.prefix)
    s = b.subscribe("A", U1, policy=qpol, op=fixed_op(prefix, 1))
    P, G = pg(s)
    old = ref(P, G, "old", "Old")
    b.fetch(P, [(old, ifields("Old", T0 - DAY))])
    a, d, x, y = (ref(P, G, t.lower(), t) for t in ("A", "D", "X", "Y"))
    b.qadd("A", [(a, K(None, None, "b11/a"))], place=END)
    t = b.now
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (d, ifields("D", T0))], rt=t + 1000)
    adm = st.ADM(b, G, d)["decision_id"]
    b.qadd("C", [(x, K(None, None, "b11/x"))], place=AFTER(d), after=[adm], t=t + 2000, rt=t + 2500)
    b.qadd("C", [(y, K(None, None, "b11/y"))], place=END, t=t + 3000, rt=t + 3500)
    eq(b.queue(), ["A", "D", "X", "Y"], "before the late fact")
    b.qadd("B", [(d, K(None, None, "b11/d"))], place=END, after=[s], t=t + 4000, rt=t + 30 * DAY)
    note(b, ["A", "X", "Y", "D"], "before: [A, D, X, Y]; the late explicit add makes D's filing a ghost: "
                                  "[A, (D), X, Y] -> D placed at the end at time 4: [A, X, Y, D]")


def test_mixed_local_queues():
    """A phone's queue mixes synced and local-folder episodes, [A, L, B]. It
    inserts synced X after L: the gap is projected onto the nearest synced episode before
    the inserted run: `after A` (local entries are never synced, PROTOCOL §7.6). Concurrently another
    phone adds Y after A (earlier). By hand: [A, B] -> Y after A [A, Y, B] -> X after A
    [A, X, Y, B]. (The device-side anchor rewrite keeps L before X on the first phone.)"""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A", "B"])
        refs.update(X=ref(P, G, "x", "X"), Y=ref(P, G, "y", "Y"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, ctx):
        r, t = ctx["refs"], ctx["t"]
        return {"M": [lambda rt: b.qadd("A", [(r["X"], K(None, None, "b12/x"))], place=AFTER(r["A"]), t=t + 2000, rt=rt,
                                        op=fixed_op(prefix, 10))],
                "O": [lambda rt: b.qadd("B", [(r["Y"], K(None, None, "b12/y"))], place=AFTER(r["A"]), after=[ctx["s"]],
                                        t=t + 1000, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("an insertion after a local episode, projected onto its synced neighbour", setup, batches,
              [("M", "O"), ("O", "M")], ["A", "X", "Y", "B"], "[A, B] -> [A, Y, B] -> X after A: [A, X, Y, B]")


# ---------------------------------------------------------------------------
# PROTOCOL §7.6: claims, cancelled removals and ghost anchors
# ---------------------------------------------------------------------------


def test_claim_cancels_a_completion():
    """PROTOCOL §7.6, a completion a later claim invalidates: [Z, A] (keys Z < A). Phone B
    starts Z (time 3, claim b0) and pauses; phone D starts Z (5, claim d1) and finishes it
    (30): the completion removes Z. Phone S, having seen that, sorts by title (40): it
    observed [A]. Then B's resume claim at 25 (an intentional resume of its open session, no
    queue bundle) arrives: D's completion at 30 is no longer under the current claim, so
    it does not fire: Z is in from the start. By hand: the sort at 40 sorts [Z, A] -> [A, Z].
    The published keys still say [Z, A]: the claim's commit must publish a layout [A, Z]."""
    b = QB("a resume claim cancels a completion; the unchanged keys must still publish [A, Z]")
    prefix = int(b.prefix)
    s, P, G, r = base_queue(b, prefix, ["Z", "A"])
    t = b.now
    SB, SD = fixed_op(prefix, 100), fixed_op(prefix, 101)
    b0 = b.start("B", r["Z"], SB, 0, duration=D, after=[s], t=t + 3000, rt=t + 3500)
    bp = b.cp("B", r["Z"], SB, 1, 5 * MIN, 5 * MIN, t=t + 4000, rt=t + 4500)
    b.start("D", r["Z"], SD, 5 * MIN, after=[bp], t=t + 5000, rt=t + 5500)
    end = b.end("D", r["Z"], SD, 1, D, D - 5 * MIN, completed=True, t=t + 30000, rt=t + 30500)
    b.qsort("E", "episode_title_a_z", observed=[r["A"]], after=[end], t=t + 40000, rt=t + 40500)
    eq(b.queue(), ["A"], "Z completed")
    n = len(b.engine.created)
    rc = b.ep("B", "session.checkpoint", r["Z"], {"session_id": SB, "ordinal": 2, "position": 5 * MIN,
                                                  "contribution": 5 * MIN, "activity": True, "claim": True,
                                                  "observed_claim": b0, "save": resume_save(SB, 2)},
                  t=t + 25000, rt=t + 50000)
    eq(b.out(rc), ("applied", None), "the claim")
    eq([d["decision_type"] for d in b.engine.created[n:]], [rules.LAYOUT], "the claim's commit publishes a layout")
    note(b, ["A", "Z"], "the completion at 30 is suppressed by the claim at 25: Z in; the sort at 40 sorts [Z, A]")


def test_cancelled_removal_reactivates_an_automatic_add():
    """D is filed automatically at the back of [Z] (time 10). Phone C, which had started
    D at 12 (claim c0) and paused, and phone D1, which starts it at 15 (claim d1, no bundle:
    it saw D queued) and finishes it at 30, the only explicit membership decision: then D's
    automatic add is barred from the start (a ghost), and the title sort at 20 (phone S)
    sorts [Z] alone. Then C's resume claim at 25 arrives: the completion at 30 does not
    fire, no explicit decision remains, the automatic add at 10 applies again, and D takes
    part in the sort at 20. By hand: before: [Z]; after: [Z, D] sorted at 20: [D, Z]."""
    qpol = {"new_episodes_action": "queue", "keep_updated": True}
    b = QB("a cancelled explicit removal reactivates an earlier automatic add before a sort")
    prefix = int(b.prefix)
    s = b.subscribe("A", U1, policy=qpol, op=fixed_op(prefix, 1))
    P, G = pg(s)
    old = ref(P, G, "old", "Old")
    b.fetch(P, [(old, ifields("Old", T0 - DAY))])
    z, d = ref(P, G, "z", "Z"), ref(P, G, "d", "D")
    b.qadd("A", [(z, K(None, None, "n4/z"))], place=END)
    t = b.now
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (d, ifields("D", T0))], rt=t + 10000)
    adm = st.ADM(b, G, d)["decision_id"]
    SC, SD = fixed_op(prefix, 100), fixed_op(prefix, 101)
    c0 = b.start("C", d, SC, 0, duration=D, after=[adm], t=t + 12000, rt=t + 12500)
    cp = b.cp("C", d, SC, 1, 2 * MIN, 2 * MIN, t=t + 13000, rt=t + 13500)
    b.start("D", d, SD, 2 * MIN, after=[cp], t=t + 15000, rt=t + 15500)
    b.qsort("E", "episode_title_a_z", observed=[z, d], after=[adm], t=t + 20000, rt=t + 20500)
    b.end("D", d, SD, 1, D, D - 2 * MIN, completed=True, t=t + 30000, rt=t + 30500)
    eq(b.queue(), ["Z"], "before the claim: D's filing is barred by the completion (a ghost)")
    b.ep("C", "session.checkpoint", d, {"session_id": SC, "ordinal": 2, "position": 2 * MIN, "contribution": 2 * MIN,
                                        "activity": True, "claim": True, "observed_claim": c0,
                                        "save": resume_save(SC, 2)},
         t=t + 25000, rt=t + 60000)
    note(b, ["D", "Z"], "the claim at 25 suppresses the completion at 30: no explicit decision, the filing at 10 "
                        "applies; the sort at 20 sorts [Z, D]: [D, Z]")


def test_ghost_anchor_after_a_sort():
    """Z is added; G is filed automatically at the back and removed explicitly by phone B
    (a ghost); A is added at the back: the structure [Z, (G), A]. A title sort permutes the
    in-members among their slots: [A, (G), Z] (the ghost keeps its slot, not its
    neighbours). Phone C, which saw G, adds X after G. By hand: [A, (G), X, Z]: [A, X, Z]."""
    qpol = {"new_episodes_action": "queue", "keep_updated": True}
    b = QB("a ghost anchor after a sort")
    prefix = int(b.prefix)
    s = b.subscribe("A", U1, policy=qpol, op=fixed_op(prefix, 1))
    P, G = pg(s)
    old = ref(P, G, "old", "Old")
    b.fetch(P, [(old, ifields("Old", T0 - DAY))])
    z, g, a, x = (ref(P, G, t.lower(), t) for t in ("Z", "G", "A", "X"))
    b.qadd("A", [(z, K(None, None, "n8/z"))], place=END)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (g, ifields("G", T0))])
    adm = st.ADM(b, G, g)["decision_id"]
    b.qremove("B", [g], after=[adm])
    b.qadd("A", [(a, K(None, None, "n8/a"))], place=END)
    b.qsort("A", "episode_title_a_z", observed=[z, a])
    b.qadd("C", [(x, K(None, None, "n8/x"))], place=AFTER(g), after=[adm])
    note(b, ["A", "X", "Z"], "[Z, (G), A] -> sorted in slots [A, (G), Z] -> X after G: [A, (G), X, Z]")


# ---------------------------------------------------------------------------
# PROTOCOL §7.6
# ---------------------------------------------------------------------------


def test_closed_members_and_generations():
    """PROTOCOL §7.6. A bulk add whose member's generation closed: the member is a ghost
    only. Old and new generations of one podcast: E queued in G1; G1 removed; a stale phone
    adds X after E@G1 (the anchor resolves to G1's ghost slot); the podcast re-subscribed
    into G2 and E@G2 added at the start. An anchor recorded against G1 never follows G2's
    placement. By hand: [A, E@G1] -> G1 closed: E@G1 a ghost from the start [A, (E1)] ->
    X after E1 [A, (E1), X] -> E@G2 at the start [E2, A, (E1), X]: [E, A, X]."""
    b = QB("§7.7: old and new generations of one podcast")
    prefix = int(b.prefix)
    so = b.subscribe("A", U2, op=fixed_op(prefix, 1))
    PO, GO = pg(so)
    a, x = ref(PO, GO, "a", "A"), ref(PO, GO, "x", "X")
    s1 = b.subscribe("A", U1, op=fixed_op(prefix, 2))
    P, G1 = pg(s1)
    e1 = ref(P, G1, "e", "E")
    b.qadd("A", [(a, K(None, None, "gen/a"))], place=END)
    add_e = b.qadd("A", [(e1, K(None, None, "gen/e1"))], place=END)
    u = b.pod("A", "unsubscribe", P, G1)
    b.qadd("C", [(x, K(None, None, "gen/x"))], place=AFTER(e1), after=[add_e])
    s2 = b.subscribe("A", U1, observed=st.obs(P, G1, "removed"), after=[u])
    G2 = b.res(s2)["generation_id"]
    e2 = ref(P, G2, "e", "E")
    b.qadd("A", [(e2, K(None, None, "gen/e2"))], place=START)
    eq([m["generation_id"] for m in b.engine._current_queue()][0], G2, "E@G2 first")
    note(b, ["E", "A", "X"], "[A, (E1)] -> X after E1's ghost slot -> E@G2 at the start: [E2, A, (E1), X]")
    b = QB("§7.3: a bulk add with a closed member")
    prefix = int(b.prefix)
    so = b.subscribe("A", U2, op=fixed_op(prefix, 1))
    PO, GO = pg(so)
    a, y = ref(PO, GO, "a", "A"), ref(PO, GO, "y", "Y")
    s1 = b.subscribe("A", U1, op=fixed_op(prefix, 2))
    P, G1 = pg(s1)
    w = ref(P, G1, "w", "W")
    b.qadd("A", [(a, K(None, None, "cl/a"))], place=END)
    u = b.pod("B", "unsubscribe", P, G1, after=[s1])
    r = b.qadd("C", [(w, K(None, None, "cl/w")), (y, K(None, None, "cl/y"))], place=START, after=[s1, so])
    eq(b.out(r), ("applied", None), "Y passes")
    note(b, ["Y", "A"], "[A] -> the run [W (closed: a ghost), Y] at the start: [(W), Y, A]: [Y, A]")


def test_empty_and_noop_sorts():
    """PROTOCOL §7.6. A no-op sort matters: phone S's queue [A, B] is sorted by title already; its
    sort (time 2) changes nothing locally but is sent; phone P's unseen add of "Ab" at the
    back (time 1) is sorted by it. By hand: [A, B, Ab] -> [A, Ab, B]. An empty sort: phone
    S sees an empty queue and sorts (time 2); phone P's unseen adds [Y, X] (time 1) are
    sorted by it: [X, Y]."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A", "B"])
        refs.update(Ab=ref(P, G, "ab", "Ab"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, ctx):
        r, t = ctx["refs"], ctx["t"]
        return {"S": [lambda rt: b.qsort("A", "episode_title_a_z", observed=[r["A"], r["B"]], t=t + 2000, rt=rt,
                                         op=fixed_op(prefix, 10))],
                "P": [lambda rt: b.qadd("B", [(r["Ab"], K(None, None, "noop/ab"))], place=END, after=[ctx["s"]],
                                        t=t + 1000, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("§7.4: a no-op sort sorts an unseen add", setup, batches, [("S", "P"), ("P", "S")], ["A", "Ab", "B"],
              "[A, B, Ab] -> title sort [A, Ab, B]")

    def setup2(b, prefix):
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        return {"s": s, "refs": {"X": ref(P, G, "x", "X"), "Y": ref(P, G, "y", "Y")}, "t": b.now}

    def batches2(b, prefix, ctx):
        r, t = ctx["refs"], ctx["t"]
        return {"S": [lambda rt: b.qsort("A", "episode_title_a_z", observed=[], t=t + 2000, rt=rt,
                                         op=fixed_op(prefix, 10))],
                "P": [lambda rt: b.qadd("B", [(r["Y"], K(None, None, "empty/y")), (r["X"], K(None, None, "empty/x"))],
                                        place=END, after=[ctx["s"]], t=t + 1000, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("§7.4: an empty sort sorts unseen adds", setup2, batches2, [("S", "P"), ("P", "S")], ["X", "Y"],
              "[Y, X] -> title sort [X, Y]")


SORT_ITEMS = [  # (name, title, podcast, publication, duration)
    ("e1", "Banana", 0, 3000, 20 * MIN),
    ("e2", "apple", 1, 1000, 10 * MIN),
    ("e3", "Cherry", 0, 2000, 10 * MIN),
    ("e4", "Apple", 0, 3000, 30 * MIN),
]
# By hand for the queue [e1, e2, e3, e4]; podcast 0 is "Zulu Show", podcast 1 "alpha show".
SORT_EXPECT = {
    "episode_title_a_z": (["e2", "e4", "e1", "e3"], "apple = Apple (stable: e2 before e4) < banana < cherry"),
    "episode_title_z_a": (["e3", "e1", "e2", "e4"], "cherry > banana > apple = Apple (ties keep the queue order)"),
    "feed_title_a_z": (["e2", "e1", "e3", "e4"], "alpha show < zulu show (e1, e3, e4 stable)"),
    "feed_title_z_a": (["e1", "e3", "e4", "e2"], "zulu show (stable) > alpha show"),
    "duration_short_long": (["e2", "e3", "e1", "e4"], "10 = 10 (stable) < 20 < 30 min"),
    "duration_long_short": (["e4", "e1", "e2", "e3"], "30 > 20 > 10 = 10 (stable)"),
    "date_old_new": (["e2", "e3", "e1", "e4"], "1000 < 2000 < 3000 = 3000 (stable)"),
    "date_new_old": (["e1", "e4", "e3", "e2"], "3000 = 3000 (stable) > 2000 > 1000"),
    "smart_shuffle_old_new": (["e3", "e1", "e4", "e2"],
                              "groups: podcast 0 [e3 2000, e1 3000, e4 3000] (3, placed first: spread 4/4 = 1, "
                              "slots 0, 1, 2), podcast 1 [e2] (slot 3)"),
    "smart_shuffle_new_old": (["e1", "e4", "e3", "e2"],
                              "podcast 0 new to old [e1, e4 (stable), e3] in slots 0-2, podcast 1 [e2] in slot 3"),
    "random": (["e4", "e2", "e1", "e3"], "the recorded permutation"),
}


def test_every_sort_order_with_ties():
    """PROTOCOL §7.6: every sort order the dialog offers, with ties, on one queue [e1, e2, e3, e4]
    (titles Banana, apple, Cherry, Apple; podcasts Zulu Show, alpha show, Zulu Show, Zulu
    Show; dates 3000, 1000, 2000, 3000; durations 20, 10, 10, 30 min). The expected orders
    are written in SORT_EXPECT with their reasons. A keep-sorted add of a tie (e5: "banana",
    podcast 0, 3000, 20 min) at the front, by title a-z: [e5, e1, e2, e3, e4] sorted stably:
    [e2, e4, e5, e1, e3] (e5 ties e1 and stays before it: the insertion comes first)."""
    for order, (want, why) in sorted(SORT_EXPECT.items()):
        b = QB("§7.5: sort order %s with ties" % order)
        prefix = int(b.prefix)
        s0 = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        s1 = b.subscribe("A", U2, op=fixed_op(prefix, 2))
        pods = [pg(s0), pg(s1)]
        b.observe("A", pods[0][0], [], feed=(None, pfields("Zulu Show")))
        b.observe("A", pods[1][0], [], feed=(None, pfields("alpha show")))
        refs = {}
        for name, title, k, pub, dur in SORT_ITEMS:
            P, G = pods[k]
            refs[name] = ref(P, G, name)
            b.observe("A", P, [(refs[name], None, ifields(title, pub, duration=dur))])
        keys = keys_in_order(["sorts/%s/%s" % (order, n) for n, _, _, _, _ in SORT_ITEMS])
        add = b.qadd("A", [(refs[n], k) for (n, _, _, _, _), k in zip(SORT_ITEMS, keys)], place=END)
        if order == "random":
            b.qsort("B", order, observed=[refs[n] for n, _, _, _, _ in SORT_ITEMS], permutation=[refs[n] for n in want],
                    after=[add])
        else:
            b.qsort("B", order, observed=[refs[n] for n, _, _, _, _ in SORT_ITEMS], after=[add])
        names = dict((r["episode_id"], n) for n, r in refs.items())
        eq([names[m["episode_id"]] for m in b.engine._current_queue()], want, "%s: %s" % (order, why))
        b.notes = {"name": b.name, "expected_order": want, "by_hand": why}
    b = QB("§7.5: a keep-sorted add of a tie inserts before it sorts")
    prefix = int(b.prefix)
    s0 = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    s1 = b.subscribe("A", U2, op=fixed_op(prefix, 2))
    pods = [pg(s0), pg(s1)]
    refs = {}
    for name, title, k, pub, dur in SORT_ITEMS + [("e5", "banana", 0, 3000, 20 * MIN)]:
        P, G = pods[k]
        refs[name] = ref(P, G, name)
        b.observe("A", P, [(refs[name], None, ifields(title, pub, duration=dur))])
    keys = keys_in_order(["sortsk/%s" % n for n, _, _, _, _ in SORT_ITEMS])
    add = b.qadd("A", [(refs[n], k) for (n, _, _, _, _), k in zip(SORT_ITEMS, keys)], place=END)
    b.qpolicy("B", keep_sorted=True, sort_order="episode_title_a_z", enqueue_location="front", after=[add])
    b.qadd("B", [(refs["e5"], K(None, None, "sortsk/e5"))], place=START)
    names = dict((r["episode_id"], n) for n, r in refs.items())
    want = ["e2", "e4", "e5", "e1", "e3"]
    eq([names[m["episode_id"]] for m in b.engine._current_queue()], want, "insert, then a stable sort")
    b.notes = {"name": b.name, "expected_order": want,
               "by_hand": "[e5, e1, e2, e3, e4] -> a-z stable: apple (e2), Apple (e4), banana (e5), Banana (e1), "
                          "cherry (e3)"}


def test_completion_invalidated_by_a_later_claim():
    """PROTOCOL §7.6: [X, Y]; phone B started X (claim b0) and paused; phone D starts X (d1) and
    finishes it (time 30): X out. Phone B's resume claim at 25 arrives later: the completion
    does not fire; X is in again, at its own place. By hand: [X, Y]."""
    b = QB("§7.6: a completion that a later claim invalidates")
    prefix = int(b.prefix)
    s, P, G, r = base_queue(b, prefix, ["X", "Y"])
    t = b.now
    SB, SD = fixed_op(prefix, 100), fixed_op(prefix, 101)
    b0 = b.start("B", r["X"], SB, 0, duration=D, after=[s], t=t + 3000, rt=t + 3500)
    bp = b.cp("B", r["X"], SB, 1, MIN, MIN, t=t + 4000, rt=t + 4500)
    b.start("D", r["X"], SD, MIN, after=[bp], t=t + 5000, rt=t + 5500)
    b.end("D", r["X"], SD, 1, D, D - MIN, completed=True, t=t + 30000, rt=t + 30500)
    eq(b.queue(), ["Y"], "completed")
    b.ep("B", "session.checkpoint", r["X"], {"session_id": SB, "ordinal": 2, "position": MIN, "contribution": MIN,
                                             "activity": True, "claim": True, "observed_claim": b0,
                                             "save": resume_save(SB, 2)},
         t=t + 25000, rt=t + 40000)
    note(b, ["X", "Y"], "the claim at 25 suppresses the completion at 30: X keeps its place: [X, Y]")


# ---------------------------------------------------------------------------
# Moves are placements, the import step waits for the whole import, run moves keep
# their sparse key predictions
# ---------------------------------------------------------------------------


def _moves_history(b, prefix, later):
    """[Y] (title M) is queued; B, offline, sorts by title (time 30), admitted while only Y
    is queued (its receipt records Y = M only); then A adds X at the end (time 10, X's title
    Z), X's title is corrected to A (time 15) and A moves X. With `later` false the move is
    at time 20, to the end; with `later` true C first adds W (title B) right after X (time
    35, having seen [Y, X]) and the move is at time 40, to the start."""
    s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    y, x, w = ref(P, G, "y"), ref(P, G, "x"), ref(P, G, "w")
    b.observe("A", P, [(y, None, ifields("M", T0)), (x, None, ifields("Z", T0)), (w, None, ifields("B", T0))],
              op=fixed_op(prefix, 2))
    add0 = b.qadd("A", [(y, K(None, None, "mv/y"))], place=END, op=fixed_op(prefix, 3))
    t = b.now
    rt = t + 100000
    b.qsort("B", "episode_title_a_z", observed=[y], after=[add0], t=t + 30000, rt=rt + 1000, op=fixed_op(prefix, 10))
    addx = b.qadd("A", [(x, K(None, None, "mv/x"))], place=END, t=t + 10000, rt=rt + 2000, op=fixed_op(prefix, 11))
    b.observe("A", P, [(x, b.revision(x["episode_id"]), ifields("A", T0))], t=t + 15000, rt=rt + 3000,
              op=fixed_op(prefix, 12), batch=fixed_op(prefix, 13))
    if not later:
        b.qmove("A", x, K(None, None, "mv/x2"), place=END, t=t + 20000, rt=rt + 4000, op=fixed_op(prefix, 14))
    else:
        b.qadd("C", [(w, K(None, None, "mv/w"))], place=AFTER(x), after=[addx], t=t + 35000, rt=rt + 4000,
               op=fixed_op(prefix, 15))
        b.qmove("A", x, K(None, None, "mv/x3"), place=START, t=t + 40000, rt=rt + 5000, op=fixed_op(prefix, 16))
    return {"X": x, "Y": y, "W": w}


def test_moves_are_placements():
    """PROTOCOL §7.6: a move is a placement; its receipt records
    its members' values, and a later sort without them in its receipt uses the latest
    placement at or before it, the move included; a placement after the sort never feeds it.

    * The metadata race extended: the order is add X (10), correction (15), move X to the end (20,
      records X = A), sort (30, recorded Y = M only). By hand: [Y, X] -> the move keeps [Y, X] ->
      the sort uses X = A from the move: [X (A), Y (M)]. (The add's Z would give [Y, X].)
    * The move after the sort: add X (10, Z), correction (15), sort (30), W after X (35),
      move X to the start (40, records A). By hand: [Y, X] -> the sort uses the add's Z:
      [Y, X] -> W after X: [Y, X, W] -> X to the start: [X, Y, W]. (Values flowing back
      from the later move would sort [X, Y], put W after X and give [X, W, Y].)
    Both depend on the correction being admitted before the move records X's values: PROTOCOL §7.6 documented exception (a metadata correction racing a placement's recorded values)."""
    for later, want, why in ((False, ["X", "Y"], "add X (Z), correction, move X to the end (records A), sort with X = A "
                                                 "from the move: [X, Y]"),
                             (True, ["X", "Y", "W"], "add X (Z), correction, sort with the add's Z: [Y, X]; W after X: "
                                                     "[Y, X, W]; X to the start (records A, after the sort): [X, Y, W]")):
        b = QB("Moves are placements: a %s" % ("later-than-sort move" if later else "move before the sort"))
        refs = _moves_history(b, int(b.prefix), later)
        names = dict((r["episode_id"], n) for n, r in refs.items())
        eq([names[m["episode_id"]] for m in b.engine._current_queue()], want, b.name)
        b.notes = {"name": b.name, "expected_order": want, "by_hand": why,
                   "arrival_order_exception": "the documented exception: a metadata correction racing a placement's "
                                              "recorded values"}


def test_import_request_boundaries():
    """PROTOCOL §7.6, §6.11: the import step waits for every decision its
    manifest counts (2 podcasts, 4 episodes, 3 queued); no part of the imported queue is
    ever placed. The backup's queue is [A1, B1, A2] (A3 not queued).

    * Request by request: (1) both podcasts' headers (import.podcast, its
      alias, its refresh part 0); (2) A1's episode decision; (3) A2's and A3's and the
      manifest: B1's is missing, so nothing is placed (counting podcast headers alone would
      publish [A1, A2]); (4) B1's: [A1, B1, A2].
    * The manifest first: (1) the manifest and Show A's decisions; (2) Show B's header: all
      podcast headers are present, B1's episode decision is not: nothing; (3) B1's: [A1, B1,
      A2].
    The cases compare the queue field groups only."""
    plans = {
        "probe": [lambda d: d["headers"], lambda d: [d["A1"]], lambda d: [d["A2"], d["A3"], d["manifest"]],
                  lambda d: [d["B1"]]],
        "manifest first": [lambda d: [d["manifest"]] + d["headerA"] + [d["A1"], d["A2"], d["A3"]],
                           lambda d: d["headerB"], lambda d: [d["B1"]]],
    }
    for name, plan in plans.items():
        prefix = next_prefix()
        b = QB("an import recovered over request boundaries (%s)" % name, prefix)
        b.field_groups = ["queue_membership", "queue_position"]
        imp = det_uuid("%d/import" % prefix)
        decs, _ = import_decisions(prefix, imp, [
            ("https://feeds.example.com/a.xml", "Show A", [("a1", "A1", T0, 0), ("a2", "A2", T0 + DAY, 2),
                                                         ("a3", "A3", T0 + 2 * DAY, None)]),
            ("https://feeds.example.com/b.xml", "Show B", [("b1", "B1", T0 + DAY, 1)])], T0, 0)
        parts = {"manifest": decs[-1]["decision"], "headerA": [], "headerB": []}
        for a in decs[:-1]:
            d = a["decision"]
            if d["decision_type"] == rules.IMPORT_EPISODE:
                parts[d["result"]["title"]] = d
            else:
                parts["headerA" if "a.xml" in json_dumps(d) else "headerB"].append(d)
        parts["headers"] = parts["headerA"] + parts["headerB"]
        b.now = T0 + 10 * DAY
        for i, req in enumerate(plan):
            for d in req(parts):
                b.upload_decision(d)
            rend(b)
            queued = [r for r in b.engine.records() if r["field_group"] == "queue_membership" and r["value"]["in_queue"]]
            if i < len(plan) - 1:
                eq((b.engine._current_queue(), queued), ([], []), "%s: request %d places nothing" % (b.name, i + 1))
        note(b, ["A1", "B1", "A2"], "nothing until all 2 podcasts, 4 episodes and 3 queued entries are held; "
                                    "then the backup's global order [A1, B1, A2]")


def json_dumps(x):
    import json
    return json.dumps(x, sort_keys=True)


def test_run_move_predictions():
    """PROTOCOL §7.6: a multi-item move may carry the device's sparse key
    predictions; `run` alone moves. The bulk Move to top's history with the web client predicting keys for B
    and D below A's: the same order, [B, D, E, A, C], in both delivery orders."""
    def setup(b, prefix):
        s, P, G, refs = base_queue(b, prefix, ["A", "B", "C", "D"])
        refs.update(E=ref(P, G, "e", "E"))
        return {"s": s, "refs": refs, "t": b.now}

    def batches(b, prefix, ctx):
        r, t = ctx["refs"], ctx["t"]
        low = b.engine._current_queue()[0]["key"]
        kb = K(None, low, "b9p/b")
        kd = K(kb[0], low, "b9p/d")
        return {"W": [lambda rt: b.qrun("A", [r["B"], r["D"]], START, members=[(r["B"], kb), (r["D"], kd)],
                                        after=[ctx["s"]], t=t + 2000, rt=rt, op=fixed_op(prefix, 10))],
                "P": [lambda rt: b.qadd("B", [(r["E"], K(None, None, "b9p/e"))], place=START, after=[ctx["s"]],
                                        t=t + 1000, rt=rt, op=fixed_op(prefix, 11))]}
    in_orders("a bulk Move to top with sparse key predictions on the run move", setup, batches, [("W", "P"), ("P", "W")],
              ["B", "D", "E", "A", "C"],
              "[A, B, C, D] -> E at the start [E, A, B, C, D] -> the run [B, D] at the start (its predictions only "
              "feed keys): [B, D, E, A, C]")


TESTS = [
    ("queue replay: back adds", test_q0_back_adds),
    ("queue replay: front adds", test_q0_front_adds),
    ("queue replay: three back adds", test_q0_three_back_adds),
    ("queue replay: keep-sorted on, then an add", test_q0_keep_sorted_on_then_add),
    ("queue replay: two drags after P", test_q0_two_drags),
    ("queue replay: a duplicate add; a partial bulk duplicate", test_q0_duplicate_add),
    ("queue replay: metadata racing a sort", test_metadata_racing_a_sort),
    ("queue replay: Smart Shuffle with a removed episode", test_smart_shuffle_with_a_removed_episode),
    ("queue replay: the anchor of a suppressed filing", test_anchor_of_a_suppressed_filing),
    ("queue replay: after current; Undo", test_after_current_and_undo),
    ("queue replay: random sort", test_random_sort),
    ("queue replay: merges", test_merges),
    ("queue replay: Smart Shuffle across a merge", test_smart_shuffle_across_a_merge),
    ("queue replay: imports", test_imports),
    ("queue replay: bulk Move to top", test_bulk_move_to_top),
    ("queue replay: recovery of layouts", test_recovery_of_layouts),
    ("queue replay: a late suppression of an anchor's filing", test_late_suppression_of_an_anchor),
    ("queue replay: mixed local queues", test_mixed_local_queues),
    ("queue replay: a claim cancels a completion", test_claim_cancels_a_completion),
    ("queue replay: a cancelled removal reactivates an automatic add", test_cancelled_removal_reactivates_an_automatic_add),
    ("queue replay: a ghost anchor after a sort", test_ghost_anchor_after_a_sort),
    ("queue replay: closed members; generations", test_closed_members_and_generations),
    ("queue replay: empty and no-op sorts", test_empty_and_noop_sorts),
    ("queue replay: every sort order with ties", test_every_sort_order_with_ties),
    ("queue replay: a completion a later claim invalidates", test_completion_invalidated_by_a_later_claim),
    ("queue replay: moves are placements", test_moves_are_placements),
    ("queue replay: an import over request boundaries", test_import_request_boundaries),
    ("queue replay: a run move's sparse key predictions", test_run_move_predictions),
]
