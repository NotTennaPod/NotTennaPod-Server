#!/usr/bin/env python3
"""Queue replay checks (PROTOCOL §7.6) over rule vectors (the independent reference).

Usage: python3 contract/reference/check_queue_replay.py [-v] [--orders N] [--splits N]
                                                        [--seed S] [FILE...]

(default FILE: contract/vectors/rules_queue_replay.json; any `rules` vector file works.)

For every case:

1. prefix: the case's arrivals are accepted one by one; after every arrival an
   incremental replay (PROTOCOL §7.6: snapshots of the replay state before every step, rerun from the
   earliest affected step) must equal the full replay. The earliest affected step is: a new or removed command (its own place), a step whose resolved content
   changed (an admission or identity decision, a merge, a generation close: the earliest
   command naming the affected entry), a step whose place in the order moved (receipt
   replacement or reranking), and a changed effective membership event (listening changes
   that make a completion or skip removal fire or not, explicit decisions that suppress or
   reactivate an automatic add: the earliest changed event).
2. fresh: every causal arrival order (all when there are at most N, else N sampled) of the
   case's facts between its server arrivals (fetches, recovered decisions and request
   ends stay in place: they are barriers), each fact admitted anew: layouts are made fresh,
   the other server choices are the vector's. The final queue (the order of the in-members
   and every membership record) must equal the case's. A case marked
   `arrival_order_exception` (a metadata correction racing a sort, the exception PROTOCOL §7.6 documents) is exempt from this check only.
3. fixed: the original run's facts (with their receipts and recorded values) and all its
   decisions, in every causal order (N sampled) to a replaying engine that decides nothing:
   identical records.
4. recovery: the original run's archive uploaded into a restored, empty server from two or
   three replicas in causal orders, with duplicates and request boundaries (N schedules):
   the same final queue after the last request. Half the schedules end requests between
   single uploads, inside a commit too (an import's podcast headers apart from its episode decisions), but never between a recovered
   fact and the decisions it caused, which the client uploads in the same request (PROTOCOL §6.11).

At every publication boundary it also checks the sparse layouts (class Sparse): no
layout when the keys fit, a layout lists only changed members, and every position record is
its scope's latest key write, changing only with it or its membership.

Every check also reads what a phone would show: at every publication boundary (a live
fact, a server fetch, a request end, a recovered identity decision's unification commit;
recovered uploads inside an unfinished request are not boundaries) the in-members of the
membership records, sorted by their published `queue_position` keys, must be exactly the
replay's order (PROTOCOL §7.6's transaction invariant).

A case marked `recovery_exception` (its reason: a hand-built history that no device
uploads, such as a fact naming a podcast, generation, session or removal outside its own
causal past, so other causal orders legitimately differ) skips checks 2, 3 and 4; checks 1
and the publication check still run. `--list-invalid` prints the cases of the given files
whose histories have such gaps (what the mark is for) and exits.

Prints the counts; exits 1 on any failure.
"""

import argparse
import copy
import functools
import itertools
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gen_rule_cases as gen  # noqa: E402
import ntp_contract as c  # noqa: E402
import rules  # noqa: E402
from check_vectors import load_vector_file  # noqa: E402

DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "vectors", "rules_queue_replay.json")


class Fail(Exception):
    pass


def qstate(eng):
    """The queue as a person sees it: the in-members' order and every membership record."""
    eng._ensure()
    order = [E for E, _ in eng._queue_order()]
    mems = sorted((E, recs["queue_membership"]) for (ent, E), recs in eng.proj.items()
                  if ent == "episode" and "queue_membership" in recs)
    return order, mems


def causal_gaps(case):
    """The facts of a case that name something whose creating fact is outside their causal
    past (dependencies, edges and previous_op_id, transitively): a podcast or generation
    (created or opened by a resolution of the case), a session (its start) or a removal (a
    history.restore's). No device writes such a fact (it can only name what it synced), so
    the history is no valid upload (PROTOCOL §6.11): its results may differ in other causal orders.
    Rejected facts are not considered."""
    facts, order = {}, []
    for a in case["arrivals"]:
        f = a.get("fact")
        if isinstance(f, dict) and isinstance(f.get("op_id"), str):
            facts[f["op_id"]] = (f, a)
            order.append(f["op_id"])
    deps = {}
    for op, (f, a) in facts.items():
        d = set(x for x in f.get("dependencies", []) if isinstance(x, str)) | set(a.get("edges", []))
        if isinstance(f.get("previous_op_id"), str):
            d.add(f["previous_op_id"])
        deps[op] = d

    def ancestors(op):
        out, stack = set(), list(deps.get(op, ()))
        while stack:
            x = stack.pop()
            if x not in out:
                out.add(x)
                stack.extend(deps.get(x, ()))
        return out
    creators = {}
    for op, r in case["expected"].get("resolutions", {}).items():
        if r.get("mode") == "created":
            creators.setdefault(("podcast", r["podcast_id"]), op)
        if r.get("mode") in ("created", "opened") and r.get("generation_id"):
            creators.setdefault(("generation", r["generation_id"]), op)
    starts = {}
    for op in order:
        f = facts[op][0]
        if f.get("type") == "session.start" and isinstance(f.get("payload"), dict):
            starts.setdefault(f["payload"].get("session_id"), op)
    outcomes = case["expected"].get("outcomes", {})
    gaps = []
    for op in order:
        f, a = facts[op]
        if outcomes.get(op, {}).get("outcome") == "rejected" or (a.get("receipt") or {}).get("outcome") == "rejected":
            continue
        p = f.get("payload")
        if not isinstance(p, dict):
            continue
        named = set()

        def walk(v):
            if isinstance(v, dict):
                for k, kind in (("podcast_id", "podcast"), ("generation_id", "generation")):
                    if isinstance(v.get(k), str):
                        named.add((kind, v[k]))
                for w in v.values():
                    walk(w)
            elif isinstance(v, list):
                for w in v:
                    walk(w)
        walk(p)
        for t in f.get("targets", []):
            if isinstance(t, dict) and t.get("entity") == "podcast" and isinstance(t.get("id"), str):
                named.add(("podcast", t["id"]))
        past = ancestors(op)
        for key in sorted(named):
            c = creators.get(key)
            if c is not None and c != op and c not in past:
                gaps.append("%s (%s) names %s %s outside its causal past (created by %s)"
                            % (op, f["type"], key[0], key[1], c))
        sid = p.get("session_id")
        if f["type"] != "session.start" and sid in starts and starts[sid] != op and starts[sid] not in past:
            gaps.append("%s (%s) names session %s, whose start %s is outside its causal past" % (op, f["type"], sid,
                                                                                               starts[sid]))
        if f["type"] == "history.restore" and p.get("removal") in facts and p["removal"] not in past:
            gaps.append("%s (history.restore) names removal %s outside its causal past" % (op, p["removal"]))
    return gaps


def ends_published(case):
    """Whether the case's final state is published: no recovered upload after its last
    publication boundary (a case may end inside an unfinished recovery request)."""
    seen, pending = set(), False
    for a in case["arrivals"]:
        if is_boundary(a, seen):
            pending = False
        elif a.get("recovery") or "decision" in a:
            pending = True
    return not pending


def is_boundary(a, seen=None):
    """Whether accepting this arrival ends a commit that runs the publication rule. `seen`
    (a set, updated) holds the decision IDs delivered before: a duplicate delivery of a
    recovered decision commits nothing."""
    if "fact" in a:
        return not a.get("recovery")
    if "fetch" in a or "recovery_end" in a:
        return True
    if "decision" not in a:
        return False
    did = a["decision"].get("decision_id")
    dup = seen is not None and did in seen
    if seen is not None:
        seen.add(did)
    return not dup and a["decision"].get("decision_type") in rules.UNIFIED_BY


def check_published(eng, where):
    """PROTOCOL §7.6: the in-members of the membership records, sorted by their published keys, are
    the replay's order; each has a position record in its own generation; keys differ."""
    eng._ensure()
    mem, pos = {}, {}
    for r in eng.records():
        if r["entity"] != "episode":
            continue
        if r["field_group"] == "queue_membership":
            mem[r["id"]] = r["value"]
        elif r["field_group"] == "queue_position":
            pos[r["id"]] = r["value"]
    ins = [E for E, v in mem.items() if v["in_queue"]]
    for E in ins:
        if E not in pos or pos[E]["generation_id"] != mem[E]["generation_id"]:
            raise Fail("%s: in-member %s has no position record in its generation" % (where, E[:12]))
    shown = sorted(ins, key=functools.cmp_to_key(lambda a, b: c.compare_queue_keys(pos[a]["key"], pos[b]["key"])))
    for a, b in zip(shown, shown[1:]):
        if c.compare_queue_keys(pos[a]["key"], pos[b]["key"]) == 0:
            raise Fail("%s: %s and %s share a published key" % (where, a[:12], b[:12]))
    order = [(E, G) for E, G in eng._queue_order()]
    if [(E, mem[E]["generation_id"]) for E in shown] != order:
        raise Fail("%s: the published keys show %s, the replay %s" % (where, [E[:12] for E in shown],
                                                                       [E[:12] for E, _ in order]))
    return 1


def keys_fit(writes, order):
    """Whether the keys (scope -> (rank, key, pid)) give the order: every member has one,
    strictly increasing (missing or equal keys are a mismatch)."""
    ks = [writes.get(k) for k in order]
    return all(w is not None for w in ks) and all(c.compare_queue_keys(a[1], z[1]) < 0 for a, z in zip(ks, ks[1:]))


class Sparse(object):
    """PROTOCOL §7.6, checked at every publication boundary of one run:

    * no layout when the keys fit: a commit records a layout only if the published keys
      before it (every key write but the layout's) did not give the replay's order;
    * a layout lists only changed members: each member it lists gets a key other than
      the one it had before (and after it the keys fit: check_published);
    * records follow the published keys: every `queue_position` record is its scope's
      latest key write, and a record changes between boundaries only when that write or
      its scope's membership changed (counted: the records republished)."""

    def __init__(self, counts):
        self.counts = counts
        self.ncreated = 0
        self.prev_recs, self.prev_pub, self.prev_mem = {}, {}, {}

    def check(self, eng, where):
        eng._ensure()
        made = eng.created[self.ncreated:]
        self.ncreated = len(eng.created)
        lays = [d for d in made if d["decision_type"] == rules.LAYOUT]
        if len(lays) > 1:
            raise Fail("%s: %d layouts in one commit" % (where, len(lays)))
        order = eng._queue_order()
        for lay in lays:
            before = eng._key_writes(exclude={lay["decision_id"]})
            if keys_fit(before, order):
                raise Fail("%s: a layout although the published keys gave the order" % where)
            for m in lay["result"]["members"]:
                b = before.get((m["episode_id"], m["generation_id"]))
                if b is not None and (b[1], b[2]) == (m["key"], m["position_id"]):
                    raise Fail("%s: the layout lists %s with its unchanged key" % (where, m["episode_id"][:12]))
            self.counts["layouts"] += 1
            self.counts["layout_members"] += len(lay["result"]["members"])
            self.counts["layout_queue"] += len(order)
        recs, mem = {}, {}
        for r in eng.records():
            if r["entity"] != "episode":
                continue
            if r["field_group"] == "queue_position":
                recs[(r["id"], r["value"]["generation_id"])] = (r["value"]["key"], r["value"]["position_id"])
            elif r["field_group"] == "queue_membership":
                mem[r["id"]] = r["value"]
        pub = dict((k, v[:2]) for k, v in eng.pubkeys.items())
        for k, v in recs.items():
            if pub.get(k) != v:
                raise Fail("%s: %s's position record %s is not its latest key write %s" % (where, k[0][:12], v[0],
                                                                                          pub.get(k)))
        for k in set(recs) | set(self.prev_recs):
            if recs.get(k) != self.prev_recs.get(k):
                self.counts["republished"] += 1
                if pub.get(k) == self.prev_pub.get(k) and mem.get(k[0]) == self.prev_mem.get(k[0]):
                    raise Fail("%s: %s's position record changed with neither its key write nor its membership"
                               % (where, k[0][:12]))
        self.prev_recs, self.prev_pub, self.prev_mem = recs, pub, mem
        return 1


def run(case):
    eng = rules.Engine(case["account_id"], case["namespace"])
    for a in case["arrivals"]:
        eng.accept(copy.deepcopy(a))
    eng._ensure()
    return eng


# ---------------------------------------------------------------------------
# 1. incremental vs full replay after every prefix (PROTOCOL §7.6)
# ---------------------------------------------------------------------------


class Incremental(object):
    """The snapshot replay (PROTOCOL §7.6): the previous prefix's prepared steps, membership timelines and
    a snapshot of the state before each of its steps; a new prefix reruns from the earliest
    affected step."""

    def __init__(self):
        self.steps = None
        self.tls = None
        self.snaps = None
        self.final = None
        self.reruns = []

    @staticmethod
    def _sig(step):
        r, x, kind, content = step

        def norm(v):
            if isinstance(v, dict):
                return sorted((repr(k), norm(w)) for k, w in v.items())
            if isinstance(v, (list, tuple)):
                return [norm(w) for w in v]
            return v
        return (x if kind != "policy" else ("policy", x), kind, repr(norm(content)))

    def earliest(self, eng, steps, tls):
        old, new = [self._sig(s) for s in self.steps], [self._sig(s) for s in steps]
        # A new, removed or moved command, or a changed resolved content (identity,
        # generation, admission effectiveness, recorded values): the first differing step.
        p = next((i for i, (a, b) in enumerate(zip(old, new)) if a != b), min(len(old), len(new)))
        # Changed effective membership events: the earliest changed event's place.
        changed = set()
        for k in set(self.tls) | set(tls):
            a = set((n, inq) for _, inq, n in self.tls.get(k, ()))
            b = set((n, inq) for _, inq, n in tls.get(k, ()))
            changed |= set(n for n, _ in a ^ b)
        if changed:
            e = min(eng.rank[n] for n in changed if n in eng.rank)
            p = min(p, next((i for i, s in enumerate(steps) if s[0] >= e), len(steps)))
        return p

    def step(self, eng):
        steps, tls = eng._replay_prepare()
        if self.steps is None:
            p = 0
            snaps = []
            state = eng._replay_run(steps, tls, 0, None, snaps)
        else:
            p = self.earliest(eng, steps, tls)
            base = self.snaps[p] if p < len(self.snaps) else self.final
            snaps = self.snaps[:p]
            state = eng._replay_run(steps, tls, p, base, snaps)
        self.reruns.append((p, len(steps)))
        self.steps, self.tls, self.snaps, self.final = steps, tls, snaps, copy.deepcopy(state)
        return [k for k in state["struct"] if k in tls and tls[k] and tls[k][-1][1]]


def check_prefixes(case, counts):
    eng = rules.Engine(case["account_id"], case["namespace"])
    inc = Incremental()
    n = 0
    seen = set()
    sparse = Sparse(counts)
    for i, a in enumerate(case["arrivals"]):
        eng.accept(copy.deepcopy(a))
        eng._ensure()
        full = eng._queue_order()
        got = inc.step(eng)
        if got != full:
            raise Fail("prefix %d: incremental %s, full %s (rerun from step %s)" % (i + 1, got, full, inc.reruns[-1]))
        if is_boundary(a, seen):
            counts["published"] += check_published(eng, "prefix %d" % (i + 1))
            sparse.check(eng, "prefix %d" % (i + 1))
        n += 1
    return n, inc.reruns


# ---------------------------------------------------------------------------
# 2. fresh admissions in every causal arrival order
# ---------------------------------------------------------------------------


class HybridChooser(object):
    """The server's choices for a fresh run: the vector's for that arrival (by type, in
    order) when they fit what this run decides (a layout needing the same number of fresh
    keys, a draw within the queue), else anew. Layouts are materializations: whether one
    is needed, and its keys, depend on the arrival order; their IDs stay the vector's when
    they can, so that later recovered decisions naming them still find them. Every choice
    takes this run's commit revision (the engine passes it)."""

    def __init__(self, rng, eng):
        self.rng, self.eng, self.supply, self.seq = rng, eng, [], 0

    def load(self, arrival, seq):
        self.supply = [copy.deepcopy(d) for d in arrival.get("decisions") or ()]
        self.seq = seq

    def __call__(self, dtype, info):
        for i, d in enumerate(self.supply):
            if d.get("decision_type") == dtype and d["decision_id"] not in self.eng.prio:
                ok = True
                if "draw" in d and not (0 <= d["draw"] <= info.get("size", -1)):
                    ok = False
                if dtype == rules.LAYOUT and len(d.get("position_ids", ())) != info["fresh"]:
                    ok = False
                if ok:
                    # The commit revision is this run's (a decision made later ranks later,
                    # so the newest layout is the published one).
                    return dict(self.supply.pop(i), epoch=info["commit"][0], seq=info["commit"][1])
        r = self.rng
        ep, sq = info["commit"]
        ch = {"decision_type": dtype, "decision_id": gen.rand_uuid(r), "epoch": ep, "seq": sq}
        if dtype == rules.LAYOUT:
            ch["position_ids"] = [gen.rand_uuid(r) for _ in range(info["fresh"])]
        elif dtype == rules.RESOLVED_ADD or (dtype == rules.ADMISSION and info.get("resolve")):
            ch["position_id"] = gen.rand_uuid(r)
            if info["random"]:
                ch["draw"] = r.randint(0, info["size"])
        elif dtype == rules.MAPPING and info.get("allocate"):
            ch["occurrence_id"] = gen.rand_uuid(r)
        return ch


def segments(case):
    """The case's arrivals as segments: runs of fact arrivals between barriers (any other
    arrival), each run with its facts' dependencies inside the run."""
    out, cur = [], []
    for a in case["arrivals"]:
        if "fact" in a and not a.get("recovery"):
            cur.append(a)
        else:
            if cur:
                out.append(("facts", cur))
                cur = []
            out.append(("barrier", a))
    if cur:
        out.append(("facts", cur))
    return out


def run_orders(facts, limit, rng):
    """Linear extensions of the facts' causal order (dependencies and author chains inside
    the run): all when there are at most `limit` (counted lazily), else `limit` sampled."""
    ids = [a["fact"]["op_id"] for a in facts]
    inside = set(ids)
    deps = {}
    for a in facts:
        f = a["fact"]
        d = set(f["dependencies"]) | ({f["previous_op_id"]} if "previous_op_id" in f else set())
        deps[f["op_id"]] = d & inside
    out = []

    def rec(prefix, placed):
        if len(out) > limit:
            return
        if len(prefix) == len(ids):
            out.append(list(prefix))
            return
        for x in ids:
            if x not in placed and deps[x] <= placed:
                prefix.append(x)
                placed.add(x)
                rec(prefix, placed)
                placed.discard(x)
                prefix.pop()
    rec([], set())
    by = dict((a["fact"]["op_id"], a) for a in facts)
    if len(out) <= limit:
        return [[by[x] for x in o] for o in out], True
    sampled = []
    for _ in range(limit):
        placed, order = set(), []
        while len(order) < len(ids):
            x = rng.choice([y for y in ids if y not in placed and deps[y] <= placed])
            placed.add(x)
            order.append(x)
        sampled.append([by[x] for x in order])
    return sampled, False


def check_fresh(case, want, limit, rng, counts):
    segs = segments(case)
    per = []
    complete = True
    for kind, x in segs:
        if kind == "facts":
            orders, full = run_orders(x, limit, rng)
            complete = complete and full
            per.append(orders)
        else:
            per.append([[x]])
    total = 1
    for p in per:
        total *= len(p)
    if total <= limit:
        combos = list(itertools.product(*per))
    else:
        complete = False
        combos = [tuple(rng.choice(p) for p in per) for _ in range(limit)]
    n = 0
    for combo in combos:
        eng = rules.Engine(case["account_id"], case["namespace"])
        chooser = HybridChooser(rng, eng)
        k = 0
        seen = set()
        sparse = Sparse(counts)
        for part in combo:
            for a in part:
                k += 1
                a = copy.deepcopy(a)
                chooser.load(a, k)
                a.pop("decisions", None)
                if a.get("recovery"):
                    eng.accept(a)
                else:
                    eng.accept(a, chooser=chooser)
                if is_boundary(a, seen):
                    counts["published"] += check_published(eng, "fresh order, arrival %d" % k)
                    sparse.check(eng, "fresh order, arrival %d" % k)
        got = qstate(eng)
        if got != want:
            raise Fail("arrival order %s: %s, expected %s" % ([a["fact"]["op_id"][-4:] for p in combo for a in p
                                                             if "fact" in a], got[0], want[0]))
        n += 1
    return n, complete


# ---------------------------------------------------------------------------
# 3. one fixed archive in every causal order (decisions held fixed)
# ---------------------------------------------------------------------------


def node_of(a, rejected=False):
    """(ID, what must precede it). A rejected fact is no node: it needs only its edges (its
    resolved frontier, empty for an envelope rejection), as its dependencies may be the
    malformed ones it was rejected for."""
    if "decision" in a:
        return a["decision"]["decision_id"], set(a["decision"]["dependencies"])
    f = a["fact"]
    if rejected:
        return f["op_id"], set(a["edges"])
    return f["op_id"], set(f["dependencies"]) | set(a["edges"]) | (
        {f["previous_op_id"]} if "previous_op_id" in f else set())


def check_fixed(case, eng, limit, rng, counts):
    want = eng.records()
    nodes = []
    for op, e in eng.ar_facts.items():
        a = copy.deepcopy(e["arrival"])
        a["receipt"] = dict(zip(("epoch", "seq", "index"), e["pos"]))
        a.pop("recovery", None)
        nodes.append(a)
    nodes += [{"decision": copy.deepcopy(d["obj"])} for d in eng.decisions.values()]
    outcomes = dict((op, e["outcome"]) for op, e in eng.ar_facts.items() if e["outcome"] is not None)
    values = dict((op, v) for op, v in eng.recorded.items() if v)
    known = set(node_of(a)[0] for a in nodes)
    rejected = set(op for op, o in outcomes.items() if o.get("outcome") == "rejected")
    n = 0
    for _ in range(limit):
        deps = dict((node_of(a)[0], node_of(a, "fact" in a and a["fact"]["op_id"] in rejected)[1] & known)
                    for a in nodes)
        pending, placed, order = list(nodes), set(), []
        while pending:
            ready = [a for a in pending if deps[node_of(a)[0]] <= placed]
            if not ready:
                raise Fail("fixed archive: the archive's dependencies admit no causal order")
            a = rng.choice(ready)
            pending.remove(a)
            placed.add(node_of(a)[0])
            order.append(a)
        rep = rules.Engine(case["account_id"], case["namespace"], eng.resolutions, eng.alias_decisions(),
                           eng.episode_decisions(), replay=True, fixed_outcomes=outcomes, fixed_values=values)
        for a in order:
            rep.accept(copy.deepcopy(a))
        if rep.records() != want:
            got = dict(((r["entity"], r["id"], r["field_group"]), r["value"]) for r in rep.records())
            exp = dict(((r["entity"], r["id"], r["field_group"]), r["value"]) for r in want)
            diff = sorted(k for k in set(got) | set(exp) if got.get(k) != exp.get(k))
            raise Fail("fixed archive order: records differ at %s" % diff[:3])
        if ends_published(case):
            counts["published"] += check_published(rep, "fixed archive, after the last decision")
            Sparse(counts).check(rep, "fixed archive, after the last decision")
        n += 1
    return n


# ---------------------------------------------------------------------------
# 4. recovery of one archive into an empty server, with request splits
# ---------------------------------------------------------------------------


def split_requests(rng, schedule, lost, p=0.35):
    """The schedule's uploads with request ends anywhere a client may end a request: after
    any upload, inside a commit too, with probability p, and at the end; never between a
    recovered fact and the decisions its acceptance made, which a client uploads after it in the same request (so a server commit's decisions, an import's or a
    fetch's, may be split)."""
    caused = set()
    for commit in lost:
        if any("fact" in x for x in commit):
            caused.update(x["decision"]["decision_id"] for x in commit if "decision" in x)
    ups = [x for x in schedule if "recovery_end" not in x]
    out = []
    for i, x in enumerate(ups):
        out.append(x)
        nxt = ups[i + 1] if i + 1 < len(ups) else None
        if nxt is not None and "decision" in nxt and nxt["decision"]["decision_id"] in caused:
            continue
        if rng.random() < p:
            out.append({"recovery_end": True, "receive_time": 0})
    if not out or "recovery_end" not in out[-1]:
        out.append({"recovery_end": True, "receive_time": 0})
    return out


def check_recovery(case, eng, want, limit, rng, counts):
    commits = gen.archive_commits(eng, rng)
    lost = [commits[k] for k in sorted(commits)]
    n = 0

    seen = set()
    sparse = [None]

    def after_each(e, a):
        if is_boundary(a, seen):
            counts["published"] += check_published(e, "recovery, after %s" % sorted(a)[0])
            sparse[0].check(e, "recovery, after %s" % sorted(a)[0])
    for i in range(limit):
        seen.clear()
        sparse[0] = Sparse(counts)
        replicas = gen.recovery_replicas(rng, len(lost))
        schedule = gen.recovery_schedule(rng, lost, replicas, set(), 0, dup=0.2, boundary=0.5)
        if i % 2 == 1:
            schedule = split_requests(rng, schedule, lost)
            counts["split"] += 1
        restored, _ = gen.run_recovery(case, 0, schedule, rng, after_each=after_each)
        got = qstate(restored)
        if got != want:
            raise Fail("recovery schedule: %s, expected %s" % (got[0], want[0]))
        n += 1
    return n


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("files", nargs="*")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--orders", type=int, default=400, help="arrival orders per case (all when fewer)")
    ap.add_argument("--splits", type=int, default=60, help="recovery schedules and fixed-archive orders per case")
    ap.add_argument("--seed", type=int, default=9101)
    ap.add_argument("--no-fresh", action="store_true",
                    help="skip check 2 (for random cases, whose server draws and metadata races may legitimately "
                         "differ by arrival order)")
    ap.add_argument("--prefix-only", action="store_true",
                    help="only check 1 and the published keys at its boundaries")
    ap.add_argument("--list-invalid", action="store_true",
                    help="print the cases whose histories name something outside a fact's causal past, and exit")
    args = ap.parse_args(argv)
    if args.list_invalid:
        for path in args.files or [DEFAULT]:
            for i, case in enumerate(load_vector_file(path)["cases"]):
                gaps = causal_gaps(case)
                if gaps:
                    print("%s: case %d%s: %s" % (os.path.basename(path), i, " (marked)" if case.get("recovery_exception")
                                                 else "", "; ".join(gaps)))
        return 0
    files = args.files or [DEFAULT]
    rng = random.Random(args.seed)
    totals = {"cases": 0, "prefixes": 0, "fresh": 0, "fresh_complete": 0, "fixed": 0, "recovery": 0, "exempt": 0,
              "published": 0, "split": 0, "invalid": 0, "layouts": 0, "layout_members": 0, "layout_queue": 0,
              "republished": 0}
    failures = 0
    for path in files:
        doc = load_vector_file(path)
        for i, case in enumerate(doc["cases"]):
            name = case.get("name", "case %d" % i)
            try:
                eng = run(case)
                want = qstate(eng)
                if "expected_order" in case and len(want[0]) != len(case["expected_order"]):
                    raise Fail("the case's own order has %d members, its note %d" % (len(want[0]),
                                                                                     len(case["expected_order"])))
                np_, reruns = check_prefixes(case, totals)
                totals["prefixes"] += np_
                invalid = bool(case.get("recovery_exception"))
                totals["invalid"] += 1 if invalid else 0
                if case.get("arrival_order_exception") or invalid or args.no_fresh or args.prefix_only:
                    totals["exempt"] += 1
                else:
                    nf, complete = check_fresh(case, want, args.orders, rng, totals)
                    totals["fresh"] += nf
                    totals["fresh_complete"] += 1 if complete else 0
                if not args.prefix_only and not invalid:
                    totals["fixed"] += check_fixed(case, eng, args.splits, rng, totals)
                    totals["recovery"] += check_recovery(case, eng, want, args.splits, rng, totals)
                totals["cases"] += 1
                if args.verbose:
                    print("ok   %s" % name)
            except (Fail, rules.RuleInputError, AssertionError) as e:
                failures += 1
                print("FAIL %s: %s: %s" % (os.path.basename(path), name, e))
    print("%s: %d cases; %d prefix checks (incremental = full); %d fresh arrival orders (%d cases exhaustive, %d "
          "exempt); %d fixed-archive orders; %d recovery schedules (%d split inside commits); %d publication "
          "boundaries (published keys = replay); sparse: %d layouts listing %d members of queues totalling %d, "
          "%d position records republished; %d cases marked recovery_exception (checks 2-4 skipped); %d failures"
          % ("OK" if failures == 0 else "FAILED", totals["cases"], totals["prefixes"], totals["fresh"],
             totals["fresh_complete"], totals["exempt"], totals["fixed"], totals["recovery"], totals["split"],
             totals["published"], totals["layouts"], totals["layout_members"], totals["layout_queue"],
             totals["republished"], totals["invalid"], failures))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
