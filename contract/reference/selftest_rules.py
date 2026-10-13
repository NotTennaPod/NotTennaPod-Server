#!/usr/bin/env python3
"""Self-test of the rule reference (rules.py, queue_sort.py).

Usage: python3 contract/reference/selftest_rules.py [--write DIR] [--groups DIR] [--quick]

Subscriptions and episodes (PROTOCOL §7.2, §7.3; scenarios 10-13 and 45 of §16): the
subscription transition table with every resolution mode and the stale cases,
explicit-vs-automatic barriers (the automatic `new` filings are real discovery admissions,
PROTOCOL §9.5), dismissal conditions, Undo/restore_state, partial set_policy with
concurrent edits, subscribe-from-preview turning keep_updated on, generation closure hiding
episode state, rejections and input errors.

Listening (PROTOCOL §8): ownership handoff A->B->A; takeover vs lazy finalization in both
delivery orders; stale completion vs replay and an explicit reset vs lazy completion (31);
a reset position barring an old session; adoption, finalization under the adopted claim
and a further checkpoint from the adopted owner; reset/rewind cuts (8c, 8d, 36, 37);
concurrent cuts in all upload orders; history remove/restore/clear with unobserved
concurrent activity surviving; last played unchanged by cuts and marks; a finalization
days later; author sequence gaps; cuts after a rejected predecessor; admission faults;
payload rejections; closed generations; scenarios 5-7; the measured duration register;
listening barriers; clear/reset fan-out.

Property tests: the incremental causal order equals contract-v1 Kahn order, every
projection after every arrival equals a from-scratch recomputation, and every arrival
permutation (with duplicate deliveries) of random multi-device fact sets (with listening
and queue activity, gen_rule_cases.py), with the persisted decisions held fixed (the queue
decisions replayed as decision arrivals), yields identical final records.

The queue (PROTOCOL §7.6): scenarios 8, 9, 25 and 35 (with a queue-less client and
metadata changes that do not sort); automatic vs explicit membership in both orders
(discovery filings, suppressed ones leaving ghosts); Undo of a removal under keep-sorted
(at the saved index, no sort); moves of out entries; completion and skip removals inside
the episode fold (a takeover suppressing the whole bundle in both delivery orders);
keep-sorted with ties and missing metadata; the offline add -> disable keep-sorted -> move
sequence live and in two recovery orders; a delayed older move before a sorting add; an
add under a concurrent disable of keep-sorted (the policy at the add's place decides);
concurrent policy writes (`sorting` as one register); queue-less play at every enqueue
location and under keep-sorted; queue-less play then completion while offline in every
causal delivery order of the resolved add; competing decisions in recovery; payload
rejections (`place` required, `moves` invalid) and capabilities; bulk members and closed
generations; malformed decision supplies; nested and concurrent keys; the canonical
comparators.

Copied installations (PROTOCOL §6.10): scenario 33's table (380, 120, 350, 360, 30 and
380 s without a tail) in both detection paths and every arrival order; ownership (a tail
before the first checkpoint, an export reusing its node, the copy's claim, the original's
stale completion, passive exports, the finalization's claim substitution); scenario 22 (a
restored backup beside the original); derived branches with and without a shared outbox,
behind a rejected fact, copies of copies, lineage-based foreign sessions and
per-continuation ordinals and sealing; a simulated restore (an earlier receipt recovered
after the copy's); copied finalizations (applied, stale in three ways and both orders,
first eligible in order, lineage through `head`); installation.fork admission;
regressions; determinism under permutations with fixed receipts; the CHOICE separating
"writing history" from "causal past"; the save tree against the generator's ground truth;
and the fallback symmetry of copied finalizations.

Catalog and discovery (PROTOCOL §9.4-§9.6): proposal outcomes and payload rejections;
admission in every arrival order of two devices' and the server's proposals (eligible vs
first population not sent, eligible vs eligible, gained media); an observation without its
proposal followed by a server fetch; Inbox filing by a device and by the server against
every barrier in both orders; queue filing against explicit membership, a device's
complete result under keep-sorted, queue-less resolution at every enqueue location,
several server admissions in one fetch in item order; the replacement rule (base,
concurrent and later or earlier fetch time, clamping, rank order, server vs device with
bases captured at the fetch's start, re-parse with the original base and time); closed
generations, preview-to-subscribe, failed and emptied first fetches; moves, aliases and a
move racing change_url; refresh_status publication; parts of 200 items; competing
admissions in recovery; arrival permutations of random catalog cases with the decisions
held fixed; the catalog generator.

Switching away (`episode.leave`, PROTOCOL §8.3, §8.4): completion under the observed
claim; a takeover ordered before it in both delivery orders; its history mark covered and
not covered by cuts; admissions before and after it; a later owning checkpoint; copied
pending leaves with and without a mark unplayed between them, in both orders; claimless
completion; a skip's removal once per transition; payload checks; the leave, a takeover
and a copy's leave recovered in every order from two replicas; the generator's leaves.

Identity reconciliation (PROTOCOL §9.7): scenarios 17 (bulletins on one enclosure; a
hosting move, server-first and phone-first), 18 (all GUIDs changed: twins, a missing date,
an empty title), 46 (the identity vectors) and 34 (two phones meeting two releases in both
sync orders; a removed survivor merged with a live source); the resolver for late facts,
undated occurrences, rule-3 IDs and arbitrary claims; rotating enclosures and allocated
IDs; merges during playback and compound effects; joined podcast merges and their routing;
lookup aliases only from decisions; arrival permutations of random identity cases; the
identity generator. Releases: a newest-first fetch reusing a GUID, signed enclosures added
to their release, the first dispute partitioning the evidence by release, a new GUID two
items share, repeated items in one fetch, and a podcast merge carrying an episode without
GUID and enclosure.

Resume claims and the queue replay's hand-computed cases are in selftest_claims.py
(PROTOCOL §8.1; rules_offline_merges.json) and selftest_queue_replay.py (PROTOCOL §7.6;
rules_queue_replay.json).

--write DIR writes every hand-written case marked as a vector to DIR/rules_selftest.json,
and the canonical comparator cases to DIR/queue_sort.json (for cross-checking another
implementation). Cases of their own vector file (rules_releases.json,
rules_episode_leave.json, rules_offline_merges.json, rules_queue_replay.json,
rules_identity_family.json) are not in rules_selftest.json: --groups DIR writes them, and
without it the selftest checks that the committed files equal them.
"""

import copy
import functools
import hashlib
import json
import os
import random
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import check_rules  # noqa: E402
import gen_rule_cases  # noqa: E402
import ntp_contract as c  # noqa: E402
import queue_sort as qs  # noqa: E402
import rules  # noqa: E402

T0 = 1760000000000
ACC = "a0000000-0000-4000-8000-000000000001"
NS = "b0000000-0000-4000-8000-000000000002"
ALL = ["favorites", "history", "inbox", "queue", "statistics"]
U1 = "https://feeds.example.com/one.xml"
U1B = "HTTPS://FEEDS.Example.com:443/one.xml#latest"  # normalizes to U1
U2 = "https://feeds.example.com/two.xml"
U2B = "https://Feeds.Example.com/two.xml"  # normalizes to U2
U3 = "https://mirror.example.net/feed"
U3B = "https://MIRROR.example.net:443/feed"  # normalizes to U3
DEFAULT_POLICY = {"new_episodes_action": "global", "keep_updated": True}
PREVIEW_POLICY = {"new_episodes_action": "global", "keep_updated": False}

BUILDERS = []


class Fail(AssertionError):
    pass


def check(cond, msg):
    if not cond:
        raise Fail(msg)


def eq(actual, expected, what):
    if not check_rules.same(actual, expected):
        if isinstance(actual, list) and isinstance(expected, list) and all(
                isinstance(r, dict) and "field_group" in r for r in actual + expected):
            empty = {"resolutions": {}, "outcomes": {}}
            diffs = check_rules.compare(dict(empty, records=expected), dict(empty, records=actual))
            raise Fail("%s:\n      %s" % (what, "\n      ".join(diffs[:12])))
        raise Fail("%s: expected %s, got %s" % (what, json.dumps(expected, ensure_ascii=False), json.dumps(actual, ensure_ascii=False)))


def fixed_op(prefix, k):
    """Explicit op IDs, so that one set of facts can be delivered in several orders."""
    return "%08x-0000-4000-9000-%012x" % (prefix, k)


def det_uuid(label):
    """A deterministic, well-spread version-4 UUID (server choices, position IDs)."""
    b = bytearray(hashlib.sha256(label.encode("utf-8")).digest()[:16])
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return "%s-%s-%s-%s-%s" % (h[:8], h[8:12], h[12:16], h[16:20], h[20:])


def pg(op):
    P = c.podcast_id(NS, op)
    return P, c.generation_id(NS, P, op)


def expect_input_error(fn, what):
    try:
        fn()
    except rules.RuleInputError:
        return
    raise Fail("%s: expected RuleInputError" % what)


class B(object):
    """Builds one case: facts from named devices, arrivals in acceptance order."""

    count = 0

    def __init__(self, name, prefix=None, group=None):
        B.count += 1
        self.prefix = B.count if prefix is None else prefix
        self.name = name
        # The vector file a hand-written case belongs to: None for rules_selftest.json, or
        # the name of its own file (written by --groups DIR).
        self.group = group
        self.engine = rules.Engine(ACC, NS)
        self.arrivals = []
        self.now = T0
        self.n = 0
        self.devs = {}
        self.vector_ok = True
        self.check_every = True  # compare the incremental projection with a recompute after every arrival
        # The server's choices for its decisions come from a separate deterministic
        # stream (so cases without decisions keep their op IDs), random draws from `draws`.
        self.nd = 0
        self.draws = []
        # The epoch of this server's own commits (a restored server that knows it has a newer
        # one): its first live fact states a receipt there, which moves the account to it, so
        # that the fact and the decisions of every later commit share one revision.
        self.epoch = 1
        self.server_label = name  # builders replaying one history in several orders share it
        BUILDERS.append(self)

    def server_uuid(self, kind):
        self.nd += 1
        return det_uuid("%s/%d/%s/%d" % (self.server_label, self.prefix, kind, self.nd))

    def choose(self, dtype, info):
        # A decision is part of its acceptance's commit: the commit's revision (the engine
        # passes it; docs/CONTRACT.md "Server decisions").
        ep, sq = info["commit"]
        ch = {"decision_type": dtype, "decision_id": self.server_uuid("decision"), "epoch": ep, "seq": sq}
        if dtype == rules.LAYOUT:
            ch["position_ids"] = [self.server_uuid("position") for _ in range(info["fresh"])]
        elif dtype == rules.RESOLVED_ADD or (dtype == rules.ADMISSION and info.get("resolve")):
            ch["position_id"] = self.server_uuid("position")
            if info["random"]:
                ch["draw"] = self.draws.pop(0) if self.draws else 0
        elif dtype == rules.MAPPING and info.get("allocate"):
            ch["occurrence_id"] = self.server_uuid("occurrence")
        return ch

    def uuid(self):
        self.n += 1
        return "%08x-0000-4000-8000-%012x" % (self.prefix, self.n)

    def dev(self, name, caps=None):
        if name not in self.devs:
            self.devs[name] = {"id": self.uuid(), "seq": 0, "prev": None, "caps": list(ALL if caps is None else caps)}
        return self.devs[name]

    def wait(self, ms):
        self.now += ms

    def push(self, dev, typ, payload, targets=(), t=None, rt=None, after=(), chain=True, caps=None, reject=None,
             patch=None, op=None, recovery=False, receipt=None):
        self.engine._ensure()
        d = self.dev(dev)
        rt = self.now + 1000 if rt is None else rt
        op = self.uuid() if op is None else op
        deps = set(after)
        if chain and d["prev"] is not None:
            deps.add(d["prev"])
        fact = {
            "op_id": op,
            "author_id": d["id"],
            "author_seq": d["seq"] + 1,
            "dependencies": sorted(deps),
            "type": typ,
            "type_version": 1,
            "client_time": rt if t is None else t,
            "targets": sorted(targets, key=lambda x: (x["entity"], x["id"], x.get("generation", ""))),
            "payload": payload,
        }
        if d["prev"] is not None:
            fact["previous_op_id"] = d["prev"]
        if patch is not None:
            patch(fact)
        frontier = set()
        for x in deps:
            info = self.engine.facts.get(x)
            if info is not None and not info["admitted"]:
                frontier.update(info["frontier"])
            else:
                frontier.add(x)
        edges = sorted(frontier)
        if reject == "envelope":
            edges, nt = [], rt
        elif reject:
            nt = rt
        else:
            nt = c.normalize_time(fact["client_time"], rt, [self.engine.node_time[e] for e in edges])
        arrival = {"fact": fact, "receive_time": rt, "normalized_time": nt, "edges": edges,
                   "capabilities": list(d["caps"] if caps is None else caps)}
        if recovery:
            arrival["recovery"] = True
        if receipt is None and not recovery and self.epoch > self.engine.epoch:
            receipt = {"epoch": self.epoch, "seq": len(self.arrivals) + 1, "index": 0}
        if receipt is not None:
            arrival["receipt"] = dict(receipt)
        self.accept(arrival)
        self.now = rt
        d["seq"] += 1
        d["prev"] = op
        return op

    def clone(self, src, dst):
        """A copied library that did not detect the copy (PROTOCOL §6.10): the same author, its
        sequence and writing tip, its capabilities."""
        d = self.dev(src)
        self.devs[dst] = {"id": d["id"], "seq": d["seq"], "prev": d["prev"], "caps": list(d["caps"])}
        return self.devs[dst]

    def fork(self, dev, author_dev, head, after=(), **kw):
        """PROTOCOL §6.10 local detection: `dev` (a new author) writes installation.fork as its
        first fact, depending on the copied frontier and the writing tip `head`."""
        return self.push(dev, "installation.fork", {"author": self.dev(author_dev)["id"], "head": head}, [],
                         after=set(after) | {head}, **kw)

    def eff(self, op):
        return self.engine.effective_author(op)

    def accept(self, arrival):
        if ("fetch" in arrival or "recovery_end" in arrival) and self.epoch > self.engine.epoch:
            raise Fail("%s: a server commit of a restored server at epoch %d before any of its live facts moved the "
                       "account there" % (self.name, self.epoch))
        if (("fact" in arrival and not arrival.get("recovery")) or "fetch" in arrival or "recovery_end" in arrival
                or ("decision" in arrival and "decisions" not in arrival and self.engine.archiving)):
            self.engine.accept(arrival, chooser=self.choose)
        else:
            self.engine.accept(arrival)
        self.arrivals.append(arrival)
        if self.check_every:
            eq(self.engine.records(), self.engine.full_records(), "%s: incremental projection vs full recompute" % self.name)

    def redeliver(self, op, patch=None, recovery=False):
        a = [x for x in self.arrivals if "fact" in x and x["fact"]["op_id"] == op][0]
        a = json.loads(json.dumps(a))
        a.pop("decisions", None)
        if recovery:
            a["recovery"] = True
        if patch is not None:
            patch(a["fact"])
        self.accept(a)

    def upload_decision(self, obj):
        """A recovered decision (a decision admitted in recovery)."""
        self.accept({"decision": json.loads(json.dumps(obj))})
        return obj["decision_id"]

    def decision(self, dtype, subject):
        """The decision of a type this builder's engine holds for a subject (the effective one)."""
        ds = [d["obj"] for d in self.engine.decisions.values()
              if d["obj"]["decision_type"] == dtype and d["obj"]["subject"] == subject]
        ds = [d for d in ds if self.engine._effective(d["decision_id"])]
        return ds[0] if ds else None

    # -- PROTOCOL §9.4 helpers ---------------------------------------------------------

    def revision(self, E=None, P=None):
        """The current canonical revision of an episode's (or a podcast's) metadata."""
        key = ("meta", "episode", E) if E is not None else ("meta", "podcast", P)
        return self.engine._fold_meta(key)[1]

    def observe(self, dev, P, items=(), feed=None, fetch_time=None, batch=None, **kw):
        """catalog.observe: items [(ref, base, fields)], feed (base, fields) or None."""
        p = {"podcast_id": P, "batch_id": batch or self.uuid(), "parser_version": "android-3.12.1",
             "fetch_time": (self.now + 500) if fetch_time is None else fetch_time,
             "items": [{"episode": r, "base": base, "fields": f} if base != "absent" else {"episode": r, "fields": f}
                       for r, base, f in items]}
        if feed is not None:
            p["feed"] = {"base": feed[0], "fields": feed[1]} if feed[0] != "absent" else {"fields": feed[1]}
        targets = [{"entity": "podcast", "id": P}] + [{"entity": "episode", "id": r["episode_id"]} for r, _, _ in items]
        return self.push(dev, "catalog.observe", p, targets, **kw)

    def propose(self, dev, r, action="inbox", pub=None, newest=None, gained=None, bundle=None, policy_node=None,
                batch=None, **kw):
        """discovery.propose; bundle = ((key, position_id), place or None)."""
        p = {"podcast_id": r["podcast_id"], "generation_id": r["generation_id"], "episode": r,
             "batch_id": batch or self.uuid(), "pub_date": pub, "newest_before_batch": newest, "action": action,
             "policy_node": policy_node}
        refs = [r]
        if gained is not None:
            p["gained_media"] = {"catalog_only_observation": gained}
        if bundle is not None:
            # bundle = ((key, position_id), place or None: where the key lies now).
            kp, place = bundle
            if place is None:
                place = self.place_for(kp[0], exclude=[r["episode_id"]])
            p["queue_bundle"] = {"key": kp[0], "position_id": kp[1], "place": place}
        return self.qpush(dev, "discovery.propose", p, refs, **kw)

    def discover(self, dev, r, action="inbox", fields=None, pub=None, newest=None, bundle=None, gained=None, t=None,
                 after=(), caps=None, rt=None, base="current"):
        """A device's refresh batch that discovers `r`: its observation (unless the
        canonical record already holds the same fields; `base` defaults to the canonical
        revision, as for a device that synced) and its proposal. Returns the proposal's
        op ID."""
        f = fields if fields is not None else ifields(r["occurrence"].get("title"), pub)
        cur = self.engine._fold_meta(("meta", "episode", r["episode_id"]))[0]
        if cur is None or rules._strip_meta(cur) != f:
            b = self.revision(r["episode_id"]) if base == "current" else base
            self.observe(dev, r["podcast_id"], [(r, b, f)], t=t, after=after, caps=caps, rt=rt)
            after = ()
        return self.propose(dev, r, action, pub=pub if pub is not None else f["pub_date"], newest=newest,
                            gained=gained, bundle=bundle, t=t, after=after, caps=caps, rt=rt)

    def fetch_start(self, P, R=None):
        R = R or self.server_uuid("refresh")
        self.accept({"fetch_start": {"refresh_id": R, "podcast_id": P}})
        return R

    def fetch(self, P, items=(), feed=None, R=None, outcome="success", fetch_time=None, rt=None, redirects=(),
              moved_to=None, new_feed_url=None, next_attempt=None, error=None, reparse_of=None, title="Show"):
        """A server fetch's result: items [(ref or occurrence, fields)]. Returns the refresh ID."""
        R = R or self.server_uuid("refresh")
        rt = self.now + 1000 if rt is None else rt
        fx = {"refresh_id": R, "podcast_id": P, "outcome": outcome}
        if outcome != "re-parse":
            fx["fetch_time"] = rt if fetch_time is None else fetch_time
        if outcome == "failure":
            fx["error"] = error or {"label": "I/O", "code": None, "detail": "connection reset"}
        else:
            fx["feed"] = feed if feed is not None else pfields(title)
            fx["items"] = [{"occurrence": x.get("occurrence", x), "fields": f} for x, f in items]
        if outcome == "success":
            fx.update(redirects=[{"status": s, "location": u} for s, u in redirects], moved_to=moved_to,
                      new_feed_url=new_feed_url)
        if outcome == "re-parse":
            fx["reparse_of"] = reparse_of
        elif next_attempt is not None:
            fx["next_attempt"] = next_attempt
        self.accept({"fetch": fx, "receive_time": rt})
        self.now = rt
        return R

    def made(self, dtype, after=0):
        """Decisions of a type this engine made (in creation order), from index `after`."""
        return [d for d in self.engine.created[after:] if d["decision_type"] == dtype]

    # -- operation helpers -----------------------------------------------------

    def subscribe(self, dev, url, observed=None, policy=None, secret=None, preview=False, target=True, **kw):
        p = {"feed_url": url}
        if observed is not None:
            p["observed"] = observed
        if policy is not None:
            p["policy"] = policy
        if secret is not None:
            p["credentials_secret"] = secret
        targets = [{"entity": "podcast", "id": observed["podcast_id"]}] if (observed and target) else []
        return self.push(dev, "podcast.register_preview" if preview else "podcast.subscribe", p, targets, **kw)

    def preview(self, dev, url, observed=None, **kw):
        return self.subscribe(dev, url, observed, preview=True, **kw)

    def pod(self, dev, typ, P, G, payload=None, **kw):
        return self.push(dev, "podcast." + typ, {} if payload is None else payload,
                         [{"entity": "podcast", "id": P, "generation": G}], **kw)

    def ep(self, dev, typ, ref, extra=None, **kw):
        p = {"episode": ref}
        p.update(extra or {})
        return self.push(dev, typ, p, [{"entity": "episode", "id": ref["episode_id"], "generation": ref["generation_id"]}], **kw)

    def mark_played(self, dev, ref, reset=True, **kw):
        return self.ep(dev, "episode.mark_played", ref, {"reset_position": reset}, **kw)

    def restore(self, dev, ref, play_state, position, **kw):
        return self.ep(dev, "episode.restore_state", ref, {"play_state": play_state, "position": position}, **kw)

    def fav(self, dev, ref, on, **kw):
        return self.ep(dev, "favorite.set", ref, {"on": on}, **kw)

    def dismiss(self, dev, refs, bulk_all=False, **kw):
        refs = sorted(refs, key=lambda r: r["episode_id"])
        targets = [{"entity": "episode", "id": r["episode_id"], "generation": r["generation_id"]} for r in refs]
        return self.push(dev, "inbox.dismiss_all" if bulk_all else "inbox.dismiss", {"episodes": refs}, targets, **kw)

    # -- PROTOCOL §8 helpers -------------------------------------------------------

    def start(self, dev, r, sid, pos=0, activity=True, observed=None, duration=None, baseline=None, **kw):
        x = {"session_id": sid, "start_position": pos, "activity": activity}
        if baseline is not None:
            x["baseline"] = baseline
        if observed is not None:
            x["observed_claim"] = observed
        if duration is not None:
            x["duration"] = duration
        return self.ep(dev, "session.start", r, x, **kw)

    def cp(self, dev, r, sid, ordinal, pos, contribution, activity=True, duration=None, save=None, **kw):
        x = {"session_id": sid, "ordinal": ordinal, "position": pos, "contribution": contribution, "activity": activity}
        if duration is not None:
            x["duration"] = duration
        if save is not None:
            x["save"] = save
        return self.ep(dev, "session.checkpoint", r, x, **kw)

    def end(self, dev, r, sid, ordinal, pos, contribution, reason="ended", completed=False, threshold=30000, rfq=False,
            history=True, activity=True, duration=None, save=None, **kw):
        x = {"session_id": sid, "ordinal": ordinal, "position": pos, "contribution": contribution, "activity": activity,
             "reason": reason, "completed": completed, "threshold_ms": threshold, "remove_from_queue": rfq,
             "history": history}
        if duration is not None:
            x["duration"] = duration
        if save is not None:
            x["save"] = save
        return self.ep(dev, "session.end", r, x, **kw)

    def fin(self, dev, r, sid, ordinal, claim, completed=True, threshold=30000, rfq=False, history=True, **kw):
        x = {"session_id": sid, "ordinal": ordinal, "claim_id": claim, "completed": completed, "threshold_ms": threshold,
             "remove_from_queue": rfq, "history": history}
        return self.ep(dev, "session.finalize", r, x, **kw)

    def leave(self, dev, r, transition, completed=False, observed=None, reason="switched", position=0, threshold=30000,
              rfq=False, history=True, **kw):
        """PROTOCOL §8: `episode.leave`."""
        x = {"transition_id": transition, "reason": reason, "position": position, "completed": completed,
             "threshold_ms": threshold, "remove_from_queue": rfq, "history": history}
        if observed is not None:
            x["observed_claim"] = observed
        return self.ep(dev, "episode.leave", r, x, **kw)

    def hremove(self, dev, r, **kw):
        return self.ep(dev, "history.remove", r, **kw)

    def hrestore(self, dev, r, removal, time, **kw):
        return self.ep(dev, "history.restore", r, {"removal": removal, "time": time}, **kw)

    def hclear(self, dev, **kw):
        return self.push(dev, "history.clear", {}, [], **kw)

    def sreset(self, dev, **kw):
        return self.push(dev, "statistics.reset", {}, [], **kw)

    # -- PROTOCOL §7.6 helpers (PROTOCOL §7.6: every placement carries `place`) ------

    def _placements(self, items):
        return [{"episode": r, "key": k, "position_id": pid} for r, (k, pid) in items]

    def qpush(self, dev, typ, payload, refs, **kw):
        targets = [{"entity": "episode", "id": r["episode_id"], "generation": r["generation_id"]} for r in refs]
        return self.push(dev, typ, payload, targets, **kw)

    def ref_of(self, E):
        """The EpisodeRef a device that synced uses for a queued episode."""
        recs = self.engine.proj[("episode", E)]
        ident = recs["identity"]
        G = (recs.get("queue_membership") or recs.get("position") or recs.get("play_state"))["generation_id"]
        return {"episode_id": E, "podcast_id": ident["podcast_id"], "generation_id": G,
                "occurrence": json.loads(json.dumps(ident["occurrence"]))}

    def place_for(self, key, exclude=()):
        """The gap a device whose view is the server's current queue resolves for a key it
        allocated: `start` below the first member, else after the last member below it."""
        prev = None
        for m in self.engine._current_queue():
            if m["episode_id"] in exclude:
                continue
            if m["key"] is not None and c.compare_queue_keys(m["key"], key) < 0:
                prev = m
        if prev is None:
            return {"kind": "start"}
        return {"kind": "after", "after": self.ref_of(prev["episode_id"])}

    @staticmethod
    def after(r):
        return {"kind": "after", "after": r}

    def qadd(self, dev, members, place=None, **kw):
        """members: [(ref, (key, position_id))...]; place: the resolved gap (default: where
        the first member's key lies in the server's current queue)."""
        if place is None:
            place = self.place_for(members[0][1][0], exclude=[r["episode_id"] for r, _ in members])
        return self.qpush(dev, "queue.add", {"members": self._placements(members), "place": place},
                          [r for r, _ in members], **kw)

    def qrestore(self, dev, members, place=None, **kw):
        if place is None:
            place = self.place_for(members[0][1][0], exclude=[r["episode_id"] for r, _ in members])
        return self.qpush(dev, "queue.restore", {"members": self._placements(members), "place": place},
                          [r for r, _ in members], **kw)

    def qremove(self, dev, refs, reason="user", **kw):
        refs = sorted(refs, key=lambda r: r["episode_id"])
        return self.qpush(dev, "queue.remove", {"episodes": refs, "reason": reason}, refs, **kw)

    def qmove(self, dev, r, kp, place=None, **kw):
        if place is None:
            place = self.place_for(kp[0], exclude=[r["episode_id"]])
        return self.qpush(dev, "queue.move", {"episode": r, "key": kp[0], "position_id": kp[1], "place": place}, [r],
                          **kw)

    def qrun(self, dev, run, place, members=None, **kw):
        """A multi-item move: the complete selected run in its resulting order, at `place`;
        `members` ([(ref, (key, position_id))...]): the device's sparse key predictions."""
        p = {"run": list(run), "place": place}
        if members is not None:
            p["members"] = self._placements(members)
        return self.qpush(dev, "queue.reorder", p, run, **kw)

    def qsort(self, dev, order, observed=None, permutation=None, **kw):
        """The sort dialog's reorder: `observed` (default: the server's current queue)."""
        if observed is None:
            observed = [self.ref_of(m["episode_id"]) for m in self.engine._current_queue()]
        srt = {"sort_order": order}
        if permutation is not None:
            srt["permutation"] = list(permutation)
        return self.push(dev, "queue.reorder", {"sort": srt, "observed": list(observed)},
                         [{"entity": "account", "id": ACC}], **kw)

    def qclear(self, dev, refs, **kw):
        refs = sorted(refs, key=lambda r: r["episode_id"])
        return self.qpush(dev, "queue.clear", {"observed": refs}, refs, **kw)

    def qpolicy(self, dev, enqueue_location=None, keep_sorted=None, sort_order=None, **kw):
        p = {}
        if enqueue_location is not None:
            p["enqueue_location"] = enqueue_location
        if keep_sorted is not None:
            p["sorting"] = {"keep_sorted": keep_sorted, "sort_order": sort_order}
        return self.push(dev, "account.set_queue_policy", p, [{"entity": "account", "id": ACC}], **kw)

    def qstart(self, dev, r, sid, kp, place=None, pos=0, **kw):
        """A play start with a queue bundle (the started episode's add at `place`)."""
        if place is None:
            place = self.place_for(kp[0], exclude=[r["episode_id"]])
        x = {"episode": r, "session_id": sid, "start_position": pos, "activity": True,
             "queue_bundle": {"key": kp[0], "position_id": kp[1], "place": place}}
        return self.qpush(dev, "session.start", x, [r], **kw)

    def queue(self):
        """The current queue as titles (the occurrence evidence's title)."""
        out = []
        for m in self.engine._current_queue():
            occ = self.engine.proj[("episode", m["episode_id"])]["identity"]["occurrence"]
            out.append(occ.get("title", m["episode_id"][:12]))
        return out

    def keyof(self, r):
        p = self.rec("episode", r["episode_id"], "queue_position")
        return (p["key"], p["position_id"]) if p is not None else None

    def nt(self, op):
        return self.engine.node_time[op]

    # -- inspection ----------------------------------------------------------------

    def res(self, op):
        return self.engine.resolutions.get(op)

    def out(self, op):
        o = self.engine.outcomes[op]
        return (o["outcome"], o.get("reason"))

    def rec(self, entity, eid, fg):
        return self.engine.proj.get((entity, eid), {}).get(fg)

    def case(self):
        exp = self.engine.result()
        if getattr(self, "field_groups", None) is not None:
            # A case comparing only some field groups (docs/CONTRACT.md).
            exp["field_groups"] = list(self.field_groups)
            exp["records"] = [r for r in exp["records"] if r["field_group"] in self.field_groups]
        out = {"account_id": ACC, "namespace": NS, "arrivals": self.arrivals, "expected": exp}
        if getattr(self, "notes", None):
            # PROTOCOL §16: the exact expected order, computed by hand, beside the case
            # (informative members: implementations read only arrivals and expected).
            out.update(self.notes)
        return out


def ref(P, G, guid, title=None):
    occ = {"k": "rss-guid", "v": guid, "guid": guid}
    if title is not None:
        occ["title"] = title
    return {"episode_id": c.episode_id(NS, P, "rss-guid", guid), "podcast_id": P, "generation_id": G, "occurrence": occ}


def obs(P, G, state):
    return {"podcast_id": P, "generation_id": G, "state": state}


def ifields(title=None, pub=None, media=True, url=None, duration=0, **extra):
    """An episode `metadata` value (PROTOCOL §9.4) without revision and fetch time."""
    f = dict((k, None) for k in rules.ITEM_FIELDS)
    f["title"], f["pub_date"] = title, pub
    if media:
        slug = "".join(ch if ch.isalnum() else "-" for ch in (title or "untitled")).lower()
        f["media"] = {"url": url or "https://cdn.example.org/%s.mp3" % slug, "size": "1000",
                      "mime_type": "audio/mpeg", "duration": duration}
    f.update(extra)
    return f


def pfields(title="Show", **extra):
    """A podcast `metadata` value (PROTOCOL §9.4) without revision and fetch time."""
    f = dict((k, None) for k in rules.FEED_FIELDS)
    f.update(title=title, funding=[], paged=False)
    f.update(extra)
    return f


# ---------------------------------------------------------------------------
# Subscriptions: transition table, resolution modes, stale cases
# ---------------------------------------------------------------------------


def setup_state(b, state):
    """Puts U1's podcast into `state`; returns (P, G, observation of that state) or Nones."""
    if state == "none":
        return None, None, None
    if state == "preview":
        r = b.preview("A", U1)
        P, G = pg(r)
        return P, G, obs(P, G, "not_subscribed")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    if state == "archived":
        b.pod("A", "archive", P, G)
    if state == "closed":
        b.pod("A", "unsubscribe", P, G)
        return P, G, obs(P, G, "removed")
    return P, G, obs(P, G, state)


def test_transition_table():
    states = ["none", "preview", "subscribed", "archived", "closed"]
    # (op, state) -> (mode or None, outcome, reason, final subscription state)
    table = {
        ("subscribe", "none"): ("created", "applied", None, "subscribed"),
        ("subscribe", "preview"): ("joined", "applied", None, "subscribed"),
        ("subscribe", "subscribed"): ("joined", "applied", None, "subscribed"),
        ("subscribe", "archived"): ("joined", "applied", None, "subscribed"),
        ("subscribe", "closed"): ("opened", "applied", None, "subscribed"),
        ("register_preview", "none"): ("created", "applied", None, "not_subscribed"),
        ("register_preview", "preview"): ("joined", "applied", None, "not_subscribed"),
        ("register_preview", "subscribed"): ("joined", "applied", None, "subscribed"),
        ("register_preview", "archived"): ("joined", "applied", None, "archived"),
        ("register_preview", "closed"): ("opened", "applied", None, "not_subscribed"),
        ("archive", "none"): (None, "not_applied", "unknown_podcast", None),
        ("archive", "preview"): (None, "not_applied", "not_subscribed", "not_subscribed"),
        ("archive", "subscribed"): (None, "applied", None, "archived"),
        ("archive", "archived"): (None, "applied", None, "archived"),
        ("archive", "closed"): (None, "not_applied", "generation_closed", "removed"),
        ("restore", "none"): (None, "not_applied", "unknown_podcast", None),
        ("restore", "preview"): (None, "not_applied", "not_subscribed", "not_subscribed"),
        ("restore", "subscribed"): (None, "applied", None, "subscribed"),
        ("restore", "archived"): (None, "applied", None, "subscribed"),
        ("restore", "closed"): (None, "not_applied", "generation_closed", "removed"),
        ("unsubscribe", "none"): (None, "not_applied", "unknown_podcast", None),
        ("unsubscribe", "preview"): (None, "applied", None, "removed"),
        ("unsubscribe", "subscribed"): (None, "applied", None, "removed"),
        ("unsubscribe", "archived"): (None, "applied", None, "removed"),
        ("unsubscribe", "closed"): (None, "not_applied", "generation_closed", "removed"),
    }
    for (op, state), (mode, outcome, reason, final) in sorted(table.items()):
        b = B("transition %s on %s" % (op, state))
        P, G, o = setup_state(b, state)
        if op in ("subscribe", "register_preview"):
            x = b.subscribe("B", U1B, observed=o, preview=(op == "register_preview"))
            r = b.res(x)
            eq(r["mode"], mode, b.name + " mode")
            if mode in ("created", "opened"):
                P2 = P if P is not None else c.podcast_id(NS, x)
                eq(r, {"podcast_id": P2, "generation_id": c.generation_id(NS, P2, x), "mode": mode}, b.name + " resolution")
                P, G = P2, r["generation_id"]
            else:
                eq(r, {"podcast_id": P, "generation_id": G, "mode": mode}, b.name + " resolution")
        else:
            if P is None:
                P = c.podcast_id(NS, b.uuid())
                G = c.generation_id(NS, P, b.uuid())
            x = b.pod("B", op, P, G)
            check(b.res(x) is None, b.name + ": only subscribe/register-preview have resolutions")
        eq(b.out(x), (outcome, reason), b.name + " outcome")
        sub = b.rec("podcast", P, "subscription")
        if final is None:
            check(sub is None, b.name + ": no podcast expected")
        else:
            eq(sub, {"state": final, "generation_id": G}, b.name + " subscription")
        if final == "removed":
            check(b.rec("podcast", P, "policy") is None, b.name + ": no policy for a removed podcast")
        elif final is not None:
            check(b.rec("podcast", P, "policy") is not None, b.name + ": policy record for the live generation")
        # The alias keeps the raw spelling of the fact that created the podcast.
        if final is not None:
            first_url = U1B if state == "none" else U1
            eq(b.rec("podcast", P, "identity")["aliases"], [{"url": first_url, "normalized": U1}], b.name + " aliases")


def test_created_defaults_policy_credentials():
    b = B("created with policy and credentials")
    secret = b.uuid()
    s = b.subscribe("A", U1B, policy={"new_episodes_action": "queue", "keep_updated": False}, secret=secret)
    P, G = pg(s)
    eq(b.rec("podcast", P, "identity"), {"aliases": [{"url": U1B, "normalized": U1}], "refresh_url": U1B}, "identity")
    eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "queue", "keep_updated": False}, "given policy")
    eq(b.rec("podcast", P, "credentials"), {"secret_id": secret}, "credentials")
    # A joined subscribe's policy is ignored; its credentials are an explicit write.
    s2 = b.uuid()
    j = b.subscribe("B", U1, observed=obs(P, G, "subscribed"), policy={"new_episodes_action": "inbox", "keep_updated": True},
                    secret=s2)
    eq(b.res(j)["mode"], "joined", "joined")
    eq(b.out(j), ("applied", None), "joined outcome")
    eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "queue", "keep_updated": False}, "joined keeps the policy")
    eq(b.rec("podcast", P, "credentials"), {"secret_id": s2}, "joined credentials")
    cl = b.pod("A", "set_credentials", P, G, {"clear": True})
    eq(b.out(cl), ("applied", None), "clear outcome")
    check(b.rec("podcast", P, "credentials") is None, "cleared credentials are a tombstone")
    s3 = b.uuid()
    b.pod("A", "set_credentials", P, G, {"secret_id": s3})
    eq(b.rec("podcast", P, "credentials"), {"secret_id": s3}, "set credentials")
    # Register-preview default policy (PROTOCOL §7.2): global, keep updated off (Android).
    r = b.preview("A", U2)
    eq(b.rec("podcast", pg(r)[0], "policy"), PREVIEW_POLICY, "preview default policy")
    eq(b.rec("podcast", pg(r)[0], "subscription"), {"state": "not_subscribed", "generation_id": pg(r)[1]}, "preview")


def test_observed_names_live_generation():
    b = B("observed names L itself")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    b.pod("A", "archive", P, G)
    # Any state of L itself joins (the device knows this lifecycle).
    for st in ("subscribed", "archived", "not_subscribed", "removed"):
        x = b.subscribe("B", U1, observed=obs(P, G, st))
        eq(b.res(x), {"podcast_id": P, "generation_id": G, "mode": "joined"}, "observed %s of L" % st)
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": G}, "subscribed again")
    # An observation naming another podcast is stale.
    other = c.podcast_id(NS, b.uuid())
    x = b.subscribe("C", U1, observed=obs(other, c.generation_id(NS, other, b.uuid()), "subscribed"))
    eq(b.res(x)["mode"], "stale", "observation of another podcast")
    eq(b.out(x), ("not_applied", "stale_subscribe"), "stale outcome")


def test_stale_absent_observed_both_orders():
    finals = []
    prefix = B.count + 1
    for order in ("subscribe_first", "unsubscribe_first"):
        b = B("absent observed vs removal, %s" % order, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        if order == "subscribe_first":
            x = b.subscribe("N", U1B, op=fixed_op(prefix, 2))  # a device that never knew the podcast
            eq(b.res(x), {"podcast_id": P, "generation_id": G, "mode": "joined"}, "joined before the removal")
            u = b.pod("A", "unsubscribe", P, G, op=fixed_op(prefix, 3))
        else:
            u = b.pod("A", "unsubscribe", P, G, op=fixed_op(prefix, 3))
            x = b.subscribe("N", U1B, op=fixed_op(prefix, 2))
            eq(b.res(x), {"podcast_id": P, "generation_id": G, "mode": "stale"}, "stale after the removal")
            eq(b.out(x), ("not_applied", "stale_subscribe"), "stale outcome")
        eq(b.out(u), ("applied", None), "unsubscribe")
        eq(b.rec("podcast", P, "subscription"), {"state": "removed", "generation_id": G}, "removed")
        finals.append(b.engine.records())
    eq(finals[0], finals[1], "both arrival orders converge")
    # register-preview without an observation after a removal is stale too.
    b = B("preview without observation after removal")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    b.pod("A", "unsubscribe", P, G)
    x = b.preview("N", U1)
    eq(b.res(x), {"podcast_id": P, "generation_id": G, "mode": "stale"}, "stale preview")
    # Observed the latest closed generation but not as removed: stale.
    x = b.subscribe("C", U1, observed=obs(P, G, "subscribed"))
    eq(b.res(x)["mode"], "stale", "live-looking observation of a closed generation")


def test_resubscribe_opened_fresh_state():
    b = B("re-subscribe after observed removal")
    secret = b.uuid()
    s = b.subscribe("A", U1, policy={"new_episodes_action": "nothing", "keep_updated": False}, secret=secret)
    P, G1 = pg(s)
    e1 = ref(P, G1, "ep-1", "One")
    m = b.mark_played("A", e1)
    f = b.fav("A", e1, True)
    eq(b.rec("episode", e1["episode_id"], "play_state"), {"generation_id": G1, "play_state": "played"}, "played in G1")
    u = b.pod("A", "unsubscribe", P, G1)
    for fg in ("play_state", "position", "favorite"):
        check(b.rec("episode", e1["episode_id"], fg) is None, "closure tombstones %s" % fg)
    eq(b.rec("episode", e1["episode_id"], "identity"), {"podcast_id": P, "occurrence": e1["occurrence"]}, "identity remains")
    check(b.rec("podcast", P, "credentials") is None and b.rec("podcast", P, "policy") is None, "per-generation state gone")
    r = b.subscribe("A", U1B, observed=obs(P, G1, "removed"))
    G2 = c.generation_id(NS, P, r)
    eq(b.res(r), {"podcast_id": P, "generation_id": G2, "mode": "opened"}, "opened")
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": G2}, "fresh subscription")
    eq(b.rec("podcast", P, "policy"), DEFAULT_POLICY, "fresh defaults")
    check(b.rec("podcast", P, "credentials") is None, "no credentials in the new generation")
    for fg in ("play_state", "position", "favorite"):
        check(b.rec("episode", e1["episode_id"], fg) is None, "G1 state stays hidden: %s" % fg)
    # A late fact for the closed generation is recorded but not applied.
    late = b.fav("B", e1, False, after=[f])
    eq(b.out(late), ("not_applied", "generation_closed"), "late fact for G1")
    e2 = ref(P, G2, "ep-1", "One")
    eq(e2["episode_id"], e1["episode_id"], "same episode ID across generations")
    b.ep("A", "episode.mark_unplayed", e2)
    eq(b.rec("episode", e1["episode_id"], "play_state"), {"generation_id": G2, "play_state": "unplayed"}, "G2 state visible")
    check(b.rec("episode", e1["episode_id"], "favorite") is None, "no favorite in G2")
    # The old generation's facts are untouched by the new lifecycle.
    eq(b.out(m), ("applied", None), "receipt of the old mark is unchanged")
    eq(b.out(u), ("applied", None), "unsubscribe receipt")


def test_concurrent_resubscriptions_share_generation():
    b = B("concurrent re-subscriptions")
    s = b.subscribe("A", U1)
    P, G1 = pg(s)
    u = b.pod("A", "unsubscribe", P, G1)
    x = b.subscribe("B", U1, observed=obs(P, G1, "removed"), after=[u])
    y = b.subscribe("C", U1B, observed=obs(P, G1, "removed"), after=[u], t=b.now - 500)
    G2 = c.generation_id(NS, P, x)
    eq(b.res(x), {"podcast_id": P, "generation_id": G2, "mode": "opened"}, "first opens")
    eq(b.res(y), {"podcast_id": P, "generation_id": G2, "mode": "joined"}, "second joins the same generation")
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": G2}, "one live generation")
    # Older lifecycles are stale: G1 removed, G2 removed, G3 opened; observing G1 removed.
    u2 = b.pod("B", "unsubscribe", P, G2)
    z = b.subscribe("B", U1, observed=obs(P, G2, "removed"))
    G3 = c.generation_id(NS, P, z)
    eq(b.res(z)["mode"], "opened", "G3 opened")
    w = b.subscribe("D", U1, observed=obs(P, G1, "removed"))
    eq(b.res(w), {"podcast_id": P, "generation_id": G3, "mode": "stale"}, "observation of an older closed generation")
    eq(b.out(u2), ("applied", None), "unsubscribe G2")


def test_delayed_subscribe_observed_archived_g1():
    b = B("delayed subscribe observed archived G1")
    s = b.subscribe("A", U1)
    P, G1 = pg(s)
    a = b.pod("A", "archive", P, G1)
    # X saw G1 archived and subscribes (restore by subscribe) offline, early client time.
    t_x = b.now + 100
    b.wait(1000)
    u = b.pod("A", "unsubscribe", P, G1)
    r = b.subscribe("A", U1, observed=obs(P, G1, "removed"))
    G2 = c.generation_id(NS, P, r)
    b.pod("A", "archive", P, G2)
    x = b.subscribe("X", U1, observed=obs(P, G1, "archived"), after=[a], t=t_x)
    eq(b.res(x), {"podcast_id": P, "generation_id": G2, "mode": "stale"}, "stale")
    eq(b.out(x), ("not_applied", "stale_subscribe"), "not applied")
    eq(b.rec("podcast", P, "subscription"), {"state": "archived", "generation_id": G2}, "G2 untouched")
    eq(b.out(u), ("applied", None), "unsubscribe")


def test_register_preview_never_downgrades():
    b = B("register-preview never downgrades")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    r = b.preview("B", U1B)
    eq(b.res(r), {"podcast_id": P, "generation_id": G, "mode": "joined"}, "joins")
    eq(b.out(r), ("applied", None), "applied")
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": G}, "still subscribed")
    # Delayed: a preview created the podcast, a subscribe joined, and a later-ordered
    # register-preview never undoes the subscription whatever the order.
    b = B("preview created, subscribe ordered before it")
    r = b.preview("A", U1, t=b.now + 5000)  # clamped to receive time
    P, G = pg(r)
    j = b.subscribe("B", U1, t=T0)  # never knew it; ordered first by its old clock
    eq(b.res(j), {"podcast_id": P, "generation_id": G, "mode": "joined"}, "subscribe joins the preview")
    check(b.engine.rank[j] < b.engine.rank[r], "subscribe ordered before the creating preview")
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": G}, "subscribed wins")
    eq(b.out(j), ("applied", None), "applied")


def test_preview_subscribe_keep_updated_and_concurrent_action():
    for order in ("subscribe_first", "edit_first"):
        b = B("subscribe from preview + concurrent action edit, %s" % order)
        r = b.preview("A", U1)
        P, G = pg(r)
        eq(b.rec("podcast", P, "policy"), PREVIEW_POLICY, "a preview starts with keep updated off")
        k = b.pod("A", "set_policy", P, G, {"keep_updated": False})
        eq(b.rec("podcast", P, "policy"), PREVIEW_POLICY, "preview policy")
        base = b.now
        if order == "subscribe_first":
            s = b.subscribe("A", U1, observed=obs(P, G, "not_subscribed"), t=base + 500)
            e = b.pod("B", "set_policy", P, G, {"new_episodes_action": "queue"}, after=[k], t=base + 400)
        else:
            e = b.pod("B", "set_policy", P, G, {"new_episodes_action": "queue"}, after=[k], t=base + 400)
            s = b.subscribe("A", U1, observed=obs(P, G, "not_subscribed"), t=base + 500)
        eq(b.res(s)["mode"], "joined", "joined from preview")
        eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "queue", "keep_updated": True}, "both survive")
        eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": G}, "subscribed")
        eq(b.out(s), ("applied", None), "subscribe applied")
        eq(b.out(e), ("applied", None), "edit applied")
    # A concurrent keep_updated=false ordered after the subscribe wins that register.
    b = B("subscribe from preview vs later keep_updated edit")
    r = b.preview("A", U1)
    P, G = pg(r)
    s = b.subscribe("A", U1, observed=obs(P, G, "not_subscribed"), t=b.now + 100)
    e = b.pod("B", "set_policy", P, G, {"keep_updated": False}, after=[r], t=b.now + 500)
    eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "global", "keep_updated": False}, "later edit wins")
    eq(b.out(e), ("applied", None), "edit applied")
    eq(b.out(s), ("applied", None), "subscribe still determines the subscription")


def test_joined_subscribe_superseded_and_archive_order():
    b = B("joined subscribe superseded by a later-ordered archive")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    base = b.now
    a = b.pod("A", "archive", P, G, t=base + 5000, rt=base + 6000)
    x = b.subscribe("B", U1, observed=obs(P, G, "subscribed"), after=[s], t=base + 4000)
    eq(b.res(x)["mode"], "joined", "joined")
    eq(b.out(x), ("superseded", None), "superseded by the archive ordered after it")
    eq(b.rec("podcast", P, "subscription"), {"state": "archived", "generation_id": G}, "archived")
    y = b.pod("C", "restore", P, G, after=[s], t=base + 4500)
    eq(b.out(y), ("superseded", None), "restore ordered before the archive")
    z = b.pod("C", "restore", P, G)
    eq(b.out(z), ("applied", None), "restore after the archive")
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": G}, "subscribed")
    eq(b.out(a), ("applied", None), "archive")


# ---------------------------------------------------------------------------
# Policy, URL aliases
# ---------------------------------------------------------------------------


def test_partial_set_policy_concurrent():
    b = B("partial set_policy with concurrent edits")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    base = b.now
    a = b.pod("A", "set_policy", P, G, {"new_episodes_action": "queue"}, t=base + 5000, rt=base + 6000)
    x = b.pod("B", "set_policy", P, G, {"new_episodes_action": "inbox", "keep_updated": False}, after=[s], t=base + 4000)
    eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "queue", "keep_updated": False}, "per-field registers")
    eq(b.out(a), ("applied", None), "a")
    eq(b.out(x), ("applied", None), "x determines keep_updated")
    y = b.pod("C", "set_policy", P, G, {"new_episodes_action": "nothing"}, after=[s], t=base + 3000)
    eq(b.out(y), ("superseded", None), "y superseded")
    eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "queue", "keep_updated": False}, "unchanged")
    z = b.pod("C", "set_policy", P, G, {"keep_updated": True})
    eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "queue", "keep_updated": True}, "later edit")
    eq(b.out(z), ("applied", None), "z")


def test_refresh_url_per_generation():
    b = B("refresh URL per generation")
    s = b.subscribe("A", U1)
    P, G1 = pg(s)
    b.pod("A", "change_url", P, G1, {"url": U3})
    eq(b.rec("podcast", P, "identity"),
       {"aliases": [{"url": U1, "normalized": U1}, {"url": U3, "normalized": U3}], "refresh_url": U3}, "changed")
    u = b.pod("A", "unsubscribe", P, G1)
    eq(b.rec("podcast", P, "identity"),
       {"aliases": [{"url": U1, "normalized": U1}, {"url": U3, "normalized": U3}], "refresh_url": U1},
       "the closed generation's write is hidden; aliases stay")
    r = b.subscribe("A", U3B, observed=obs(P, G1, "removed"), after=[u])
    G2 = c.generation_id(NS, P, r)
    eq(b.res(r)["mode"], "opened", "opened through the other alias")
    eq(b.rec("podcast", P, "identity")["refresh_url"], U3B, "the opening fact's URL")
    j = b.subscribe("N", U1)
    eq(b.res(j)["mode"], "joined", "joined")
    eq(b.rec("podcast", P, "identity")["refresh_url"], U3B, "a joined subscribe does not change it")
    b.pod("A", "unsubscribe", P, G2)
    eq(b.rec("podcast", P, "identity")["refresh_url"], U3B, "latest generation's URL when all are closed")
    # A preview-opened generation also starts from its own URL and keep updated off.
    r2 = b.preview("A", U1B, observed=obs(P, G2, "removed"))
    eq(b.res(r2)["mode"], "opened", "preview opened")
    eq(b.rec("podcast", P, "identity")["refresh_url"], U1B, "preview generation URL")
    eq(b.rec("podcast", P, "policy"), PREVIEW_POLICY, "preview generation policy")


def test_scenario45_alias_conflicts():
    b = B("scenario 45: URL alias conflicts")
    sp = b.subscribe("A", U1)
    sq = b.subscribe("A", U2)
    P, GP = pg(sp)
    Q, GQ = pg(sq)
    idq = b.rec("podcast", Q, "identity")
    x = b.pod("A", "change_url", P, GP, {"url": U2B})
    eq(b.out(x), ("not_applied", "alias_conflict"), "P to Q's alias")
    eq(b.rec("podcast", P, "identity"), {"aliases": [{"url": U1, "normalized": U1}], "refresh_url": U1}, "P intact")
    eq(b.rec("podcast", Q, "identity"), idq, "Q intact")
    # Race for an unowned URL: the first accepted claim wins even if ordered later.
    base = b.now
    c1 = b.pod("A", "change_url", P, GP, {"url": U3}, t=base + 5000, rt=base + 6000)
    c2 = b.pod("B", "change_url", Q, GQ, {"url": U3B}, after=[sq], t=base + 4000)
    eq(b.out(c1), ("applied", None), "first claim")
    eq(b.out(c2), ("not_applied", "alias_conflict"), "second claim")
    eq(b.rec("podcast", P, "identity"),
       {"aliases": [{"url": U1, "normalized": U1}, {"url": U3, "normalized": U3}], "refresh_url": U3}, "P claims U3")
    eq(b.rec("podcast", Q, "identity"), idq, "Q keeps its identity")
    eq(b.rec("podcast", Q, "subscription"), {"state": "subscribed", "generation_id": GQ}, "Q still subscribed")
    # Re-claiming an own alias in another spelling only changes the refresh URL.
    c3 = b.pod("A", "change_url", P, GP, {"url": U1B})
    eq(b.out(c3), ("applied", None), "own alias")
    eq(b.rec("podcast", P, "identity"),
       {"aliases": [{"url": U1, "normalized": U1}, {"url": U3, "normalized": U3}], "refresh_url": U1B}, "refresh URL")
    # Subscribing through the new alias returns P.
    j = b.subscribe("N", U3B)
    eq(b.res(j), {"podcast_id": P, "generation_id": GP, "mode": "joined"}, "alias resolves to P")


# ---------------------------------------------------------------------------
# Episodes
# ---------------------------------------------------------------------------


def test_scenario10_unsubscribe_vs_offline_listening():
    b = B("scenario 10: unsubscribe vs offline device")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "ep-1", "One")
    base = b.now
    # B (synced after s) marks played; accepted before the removal but ordered after it.
    f1 = b.mark_played("B", e, after=[s], t=base + 3000, rt=base + 4000)
    eq(b.out(f1), ("applied", None), "applied at acceptance")
    u = b.pod("A", "unsubscribe", P, G, t=base + 2000)
    check(b.engine.rank[u] < b.engine.rank[f1], "unsubscribe ordered before B's mark")
    check(b.rec("episode", e["episode_id"], "play_state") is None, "hidden whatever the order")
    f2 = b.fav("B", e, True)
    eq(b.out(f2), ("not_applied", "generation_closed"), "later offline fact")
    f3 = b.dismiss("B", [e])
    eq(b.out(f3), ("not_applied", "generation_closed"), "later dismissal")
    eq(b.rec("podcast", P, "subscription"), {"state": "removed", "generation_id": G}, "gone everywhere")
    eq(b.out(u), ("applied", None), "unsubscribe")


def test_scenario11_dismiss_all_vs_concurrent_filing():
    b = B("scenario 11: dismiss_all vs concurrent filing")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e1, e2, e3 = ref(P, G, "ep-1"), ref(P, G, "ep-2"), ref(P, G, "ep-3")
    # The filings are real discoveries (admitted proposals filing into the Inbox).
    n1 = b.discover("B", e1, t=b.now + 10, after=[s])
    n2 = b.discover("B", e2, t=b.now + 20, after=[s])
    for e in (e1, e2):
        eq(b.rec("episode", e["episode_id"], "play_state"), {"generation_id": G, "play_state": "new"}, "filed NEW")
    d = b.dismiss("A", [e1, e2], bulk_all=True, after=[n1, n2], t=b.now + 5000, rt=b.now + 6000)
    # C's discovery filed e3 concurrently; it is ordered before the dismissal but not named.
    n3 = b.discover("C", e3, t=b.now - 3000, after=[s])
    check(b.engine.rank[n3] < b.engine.rank[d], "the concurrent filing is ordered before the dismissal")
    eq(b.out(d), ("applied", None), "dismiss_all applied")
    eq(b.rec("episode", e1["episode_id"], "play_state"), {"generation_id": G, "play_state": "unplayed"}, "e1 dismissed")
    eq(b.rec("episode", e2["episode_id"], "play_state"), {"generation_id": G, "play_state": "unplayed"}, "e2 dismissed")
    eq(b.rec("episode", e3["episode_id"], "play_state"), {"generation_id": G, "play_state": "new"}, "e3 stays NEW")


def test_scenario12_favorite_converges_to_later():
    finals = []
    prefix = B.count + 1
    for order in ("on_first", "off_first"):
        b = B("scenario 12: favorite on/off, %s" % order, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e = ref(P, G, "ep-1")
        base = b.now
        if order == "on_first":
            on = b.fav("A", e, True, after=[s], t=base + 5000, rt=base + 9000, op=fixed_op(prefix, 2))
            off = b.fav("B", e, False, after=[s], t=base + 6000, rt=base + 10000, op=fixed_op(prefix, 3))
            eq(b.out(on), ("applied", None), "on")
            eq(b.out(off), ("applied", None), "off")
        else:
            off = b.fav("B", e, False, after=[s], t=base + 6000, rt=base + 9000, op=fixed_op(prefix, 3))
            on = b.fav("A", e, True, after=[s], t=base + 5000, rt=base + 10000, op=fixed_op(prefix, 2))
            eq(b.out(off), ("applied", None), "off")
            eq(b.out(on), ("superseded", None), "on is ordered before off")
        eq(b.rec("episode", e["episode_id"], "favorite"), {"generation_id": G, "on": False}, "later action wins")
        finals.append([r for r in b.engine.records() if r["field_group"] == "favorite"])
    eq(finals[0], finals[1], "converged")


def test_scenario13_undo_mark_played():
    b = B("scenario 13: Undo of mark played")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "ep-1")
    r0 = b.restore("A", e, "unplayed", 120000)  # stands in for listening to 2:00
    m = b.mark_played("A", e)
    eq(b.rec("episode", e["episode_id"], "position"), {"generation_id": G, "position": 0, "claim_id": m}, "reset")
    u = b.restore("A", e, "unplayed", 120000)
    eq(b.rec("episode", e["episode_id"], "play_state"), {"generation_id": G, "play_state": "unplayed"}, "restored state")
    eq(b.rec("episode", e["episode_id"], "position"), {"generation_id": G, "position": 120000, "claim_id": u},
       "restored position with the Undo's claim")
    eq([b.out(x) for x in (r0, m, u)], [("applied", None)] * 3, "outcomes")
    # A delayed Undo ordered before a concurrent mark from another device loses.
    base = b.now
    m2 = b.mark_played("B", e, after=[u], t=base + 5000, rt=base + 6000)
    u2 = b.restore("A", e, "unplayed", 120000, t=base + 4000)
    eq(b.out(u2), ("superseded", None), "delayed Undo superseded")
    eq(b.rec("episode", e["episode_id"], "position"), {"generation_id": G, "position": 0, "claim_id": m2}, "mark wins")
    # Without a position reset the delayed Undo still owns the position.
    base = b.now
    m3 = b.mark_played("B", e, reset=False, t=base + 5000, rt=base + 6000)
    u3 = b.restore("A", e, "new", 5000, t=base + 4000)
    eq(b.out(u3), ("applied", None), "Undo determines the position")
    eq(b.rec("episode", e["episode_id"], "play_state"), {"generation_id": G, "play_state": "played"}, "played")
    eq(b.rec("episode", e["episode_id"], "position"), {"generation_id": G, "position": 5000, "claim_id": u3}, "position")
    eq(b.out(m3), ("applied", None), "m3")


def test_dismiss_conditions():
    b = B("dismiss conditions")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e_new, e_played, e_unplayed, e_only = (ref(P, G, g) for g in ("n", "p", "u", "o"))
    b.restore("A", e_new, "new", 0)
    b.mark_played("A", e_played)
    b.ep("A", "episode.mark_unplayed", e_unplayed)
    d = b.dismiss("A", [e_new, e_played, e_unplayed, e_only])
    eq(b.out(d), ("applied", None), "dismiss applied")
    st = lambda e: b.rec("episode", e["episode_id"], "play_state")  # noqa: E731
    eq(st(e_new), {"generation_id": G, "play_state": "unplayed"}, "new -> unplayed")
    eq(st(e_played), {"generation_id": G, "play_state": "played"}, "played unchanged")
    eq(st(e_unplayed), {"generation_id": G, "play_state": "unplayed"}, "unplayed unchanged")
    eq(st(e_only), {"generation_id": G, "play_state": "unplayed"}, "a dismissal alone makes an unplayed record")
    check(b.rec("episode", e_only["episode_id"], "position") is None, "no position record")
    # At its place in order: a delayed restore(new) ordered before the dismissal is
    # dismissed by it; one ordered after it is not.
    for when, want in (("before", "unplayed"), ("after", "new")):
        b = B("dismiss at its place, restore %s" % when)
        s = b.subscribe("A", U1)
        P, G = pg(s)
        e = ref(P, G, "x")
        base = b.now
        d = b.dismiss("A", [e], t=base + 5000, rt=base + 6000)
        eq(st(e), {"generation_id": G, "play_state": "unplayed"}, "dismissal alone")
        r = b.restore("B", e, "new", 0, after=[s], t=base + (4000 if when == "before" else 7000), rt=base + 8000)
        eq(b.rec("episode", e["episode_id"], "play_state"), {"generation_id": G, "play_state": want}, "restore %s" % when)
        # Either way the restore determines the position, so it is applied.
        eq(b.out(r), ("applied", None), "restore outcome")
        eq(b.out(d), ("applied", None), "dismissal receipt")


def test_barriers_explicit_vs_automatic():
    st = lambda b, e: b.rec("episode", e["episode_id"], "play_state")["play_state"]  # noqa: E731
    b = B("admission alone and with a favorite")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "a")
    b.discover("D", e, after=[s])
    eq(st(b, e), "new", "admission files NEW")
    check(b.rec("episode", e["episode_id"], "position") is None, "no position")
    b.fav("A", e, True)
    eq(st(b, e), "new", "a favorite is not a barrier")
    eq(b.rec("episode", e["episode_id"], "favorite"), {"generation_id": G, "on": True}, "favorite")
    for kind in ("mark_unplayed", "mark_played", "reset_position", "dismiss", "restore_unplayed"):
        for when in ("before", "after"):
            b = B("barrier %s %s the admission" % (kind, when))
            s = b.subscribe("A", U1)
            P, G = pg(s)
            e = ref(P, G, "a")
            base = b.now
            if when == "after":
                b.discover("D", e, t=base + 100, after=[s])
            t = base + (50 if when == "before" else 5000)
            if kind == "dismiss":
                b.dismiss("B", [e], after=[s], t=t)
            elif kind == "restore_unplayed":
                b.restore("B", e, "unplayed", 0, after=[s], t=t)
            elif kind == "mark_played":
                b.mark_played("B", e, after=[s], t=t)
            else:
                b.ep("B", "episode." + kind, e, after=[s], t=t)
            if when == "before":
                b.discover("D", e, t=base + 9000, after=[s])
            want = "played" if kind == "mark_played" else "unplayed"
            eq(st(b, e), want, "%s %s the admission" % (kind, when))
    # Undo (explicit) can restore NEW even though it is a barrier.
    b = B("explicit restore of NEW")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "a")
    b.discover("D", e, after=[s])
    b.mark_played("A", e)
    b.restore("A", e, "new", 0)
    eq(st(b, e), "new", "Undo restores NEW")
    u = b.pod("A", "unsubscribe", P, G)
    late = b.discover("D", ref(P, G, "late"), after=[u])
    eq(b.out(late), ("not_applied", "generation_closed"), "a proposal into a closed generation")
    check(b.rec("episode", ref(P, G, "late")["episode_id"], "play_state") is None, "no state")


def test_episode_reasons_and_identity():
    b = B("episode reasons and identity")
    sp = b.subscribe("A", U1)
    sq = b.subscribe("A", U2)
    P, GP = pg(sp)
    Q, GQ = pg(sq)
    ghost = c.podcast_id(NS, b.uuid())
    e_ghost = ref(ghost, c.generation_id(NS, ghost, b.uuid()), "g")
    x = b.mark_played("A", e_ghost)
    eq(b.out(x), ("not_applied", "unknown_podcast"), "unknown podcast")
    check(b.rec("episode", e_ghost["episode_id"], "identity") is None, "no identity for an unknown podcast")
    e_badgen = ref(P, c.generation_id(NS, P, b.uuid()), "bg", "Bad generation")
    x = b.fav("A", e_badgen, True)
    eq(b.out(x), ("not_applied", "unknown_generation"), "unknown generation")
    check(b.rec("episode", e_badgen["episode_id"], "identity") is None, "a refused fact registers nothing")
    # The first fact that passes every check binds the episode (PROTOCOL §7.2).
    e1 = ref(P, GP, "ep-1", "First title")
    b.mark_played("A", e1)
    eq(b.rec("episode", e1["episode_id"], "identity"), {"podcast_id": P, "occurrence": e1["occurrence"]}, "bound")
    e1b = ref(P, GP, "ep-1", "Changed title")
    b.ep("A", "episode.mark_unplayed", e1b)
    eq(b.rec("episode", e1["episode_id"], "identity"), {"podcast_id": P, "occurrence": e1["occurrence"]},
       "placeholders never overwrite")
    mis = dict(e1, podcast_id=Q, generation_id=GQ)
    x = b.fav("A", mis, True)
    eq(b.out(x), ("not_applied", "episode_podcast_mismatch"), "episode keeps its first podcast")
    # A bulk with one valid member applies; with none, the first member's reason.
    x = b.dismiss("A", [e_ghost, ref(P, GP, "ep-2")])
    eq(b.out(x), ("applied", None), "mixed bulk")
    x = b.dismiss("A", [e_ghost, e_badgen])
    first = sorted([e_ghost, e_badgen], key=lambda r: r["episode_id"])[0]
    eq(b.out(x), ("not_applied", "unknown_podcast" if first is e_ghost else "unknown_generation"), "all members fail")
    # Discovery policy register.
    a1 = b.push("A", "account.set_discovery_policy", {"new_episodes_action": "queue"}, [{"entity": "account", "id": ACC}])
    eq(b.rec("account", ACC, "discovery_policy"), {"new_episodes_action": "queue"}, "discovery policy")
    a2 = b.push("B", "account.set_discovery_policy", {"new_episodes_action": "nothing"}, [{"entity": "account", "id": ACC}],
                t=T0)
    eq(b.out(a2), ("superseded", None), "older concurrent edit")
    eq(b.out(a1), ("applied", None), "a1")


# ---------------------------------------------------------------------------
# Rejections, transport and input errors
# ---------------------------------------------------------------------------


def test_rejections():
    b = B("rejections")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "ep")
    et = [{"entity": "episode", "id": e["episode_id"], "generation": G}]
    pt = [{"entity": "podcast", "id": P, "generation": G}]
    bad = [
        ("podcast.subscribe", {"feed_url": U1, "extra": 1}, []),
        ("podcast.subscribe", {"feed_url": U1, "observed": {"podcast_id": P, "generation_id": G}}, []),
        ("podcast.subscribe", {"feed_url": U1, "observed": obs(P, G, "gone")}, []),
        ("podcast.subscribe", {"feed_url": U1, "policy": {"new_episodes_action": "queue"}}, []),
        ("podcast.subscribe", {"feed_url": 5}, []),
        ("podcast.register_preview", {"feed_url": U1, "policy": DEFAULT_POLICY}, []),
        ("podcast.archive", {"x": True}, pt),
        ("podcast.archive", {}, [{"entity": "podcast", "id": P}]),
        ("podcast.set_policy", {}, pt),
        ("podcast.set_policy", {"new_episodes_action": "sometimes"}, pt),
        ("podcast.set_credentials", {"clear": False}, pt),
        ("podcast.set_credentials", {"secret_id": "not-a-uuid"}, pt),
        ("podcast.change_url", {"url": U2, "force": True}, pt),
        ("episode.mark_played", {"episode": e}, et),
        ("episode.mark_played", {"episode": e, "reset_position": True}, []),
        ("episode.mark_unplayed", {"episode": dict(e, occurrence={"k": "rss-guid"})}, et),
        ("episode.mark_unplayed", {"episode": dict(e, occurrence=dict(e["occurrence"], k="guid"))}, et),
        ("episode.mark_unplayed", {"episode": dict(e, extra=1)}, et),
        ("episode.restore_state", {"episode": e, "play_state": "new", "position": -1}, et),
        ("episode.restore_state", {"episode": e, "play_state": "listened", "position": 0}, et),
        ("favorite.set", {"episode": e, "on": 1}, et),
        ("inbox.dismiss", {"episodes": []}, []),
        ("inbox.dismiss", {"episodes": [e, e]}, et),
        ("inbox.dismiss", {"episodes": [e]}, []),
        ("account.set_discovery_policy", {"new_episodes_action": "global"}, [{"entity": "account", "id": ACC}]),
        ("account.set_discovery_policy", {"new_episodes_action": "inbox"}, [{"entity": "account", "id": NS}]),
    ]
    for typ, payload, targets in bad:
        x = b.push("A", typ, payload, targets, reject=True)
        eq(b.out(x), ("rejected", "invalid_payload"), "%s %s" % (typ, json.dumps(payload)[:80]))
        check(x not in b.engine.rank, "a rejected fact is not a causal node")
    # A fact depending on rejected facts sees their resolved frontier.
    y = b.mark_played("A", e)
    eq(b.engine.facts[y]["frontier"], [s], "frontier through rejected facts")
    eq(b.out(y), ("applied", None), "valid fact after rejections")
    # Envelope rejections with their reason codes (PROTOCOL §6.4).
    other = b.uuid()

    def setv(k, v):
        return lambda f: f.__setitem__(k, v)

    def delv(k):
        return lambda f: f.pop(k)
    envelope = [
        ("unknown_field", setv("extra", 1)),
        ("missing_field", delv("client_time")),
        ("unknown_field", lambda f: (f.pop("payload"), f.__setitem__("extra", 1))),  # first listed code wins
        ("invalid_author_id", setv("author_id", "A0000000-0000-4000-8000-000000000001")),
        ("invalid_author_seq", setv("author_seq", 0)),
        ("invalid_author_seq", setv("author_seq", "2")),
        ("invalid_previous_op_id", setv("previous_op_id", None)),
        ("invalid_previous_op_id", lambda f: f.__setitem__("previous_op_id", f["op_id"])),
        ("invalid_dependencies", lambda f: f.__setitem__("dependencies", [f["op_id"]])),
        ("invalid_dependencies", setv("dependencies", ["not-a-node"])),
        ("invalid_dependencies", setv("dependencies", {})),
        ("unsorted_dependencies", setv("dependencies", [s, s])),
        ("unsorted_dependencies", setv("dependencies", sorted([s, other], reverse=True))),
        ("invalid_type", setv("type", 7)),
        ("invalid_type_version", setv("type_version", 0)),
        ("invalid_type_version", setv("type_version", True)),
        ("invalid_client_time", setv("client_time", "now")),
        ("invalid_targets", setv("targets", {})),
        ("invalid_targets", setv("targets", [{"entity": "episode"}])),
        ("invalid_targets", setv("targets", [{"entity": "episode", "id": "x", "generation": 1}])),
        ("invalid_targets", setv("targets", [{"entity": "episode", "id": "x", "other": "y"}])),
        ("unsorted_targets", lambda f: f.__setitem__("targets", list(reversed(f["targets"])))),
        ("unsorted_targets", lambda f: f.__setitem__("targets", f["targets"] + f["targets"][:1])),
        ("invalid_payload", setv("payload", [])),
    ]
    for code, patch in envelope:
        x = b.dismiss("A", [e, ref(P, G, "zz")], patch=patch, reject="envelope")
        eq(b.out(x), ("rejected", code), "envelope %s" % code)
        check(x not in b.engine.rank, "an envelope rejection is not a causal node")
    x = b.fav("C", e, True, caps=["queue"], reject=True)
    eq(b.out(x), ("rejected", "capability_not_declared"), "favorites capability")
    x = b.dismiss("C", [e], caps=[], reject=True)
    eq(b.out(x), ("rejected", "capability_not_declared"), "inbox capability")
    x = b.ep("C", "episode.mark_unplayed", e, caps=[])
    eq(b.out(x), ("applied", None), "core needs no declaration")

    def reuse_seq(f):
        f["author_seq"] = 1
        f.pop("previous_op_id", None)
    # PROTOCOL §6.10 replaces PROTOCOL §6.4 author_position_taken: a fact at a held
    # author position is admitted under its derived author D(X).
    x = b.ep("A", "episode.mark_unplayed", e, patch=reuse_seq, chain=False)
    eq(b.out(x), ("applied", None), "author position already held: admitted under a derived author")
    eq(b.engine.effective_author(x), rules.derived_author(NS, x), "the derived author D(X)")
    eq(b.engine.parent_author(rules.derived_author(NS, x)), b.dev("A")["id"], "its parent is the author it left")

    def other_author(f):
        f["previous_op_id"] = y  # A's fact, but this fact is C's
    x = b.ep("C", "episode.mark_unplayed", e, patch=other_author, reject=True)
    eq(b.out(x), ("rejected", "impossible_author_history"), "previous fact of another author")


def test_transport_duplicates():
    b = B("duplicate deliveries")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "ep")
    m = b.mark_played("A", e)
    before = (b.engine.records(), dict(b.engine.outcomes), dict(b.engine.resolutions))
    b.redeliver(s)
    b.redeliver(m)

    def change(f):
        f["payload"]["reset_position"] = False
    b.redeliver(m, patch=change)  # same op_id, different content: op_id_conflict
    eq((b.engine.records(), dict(b.engine.outcomes), dict(b.engine.resolutions)), before, "duplicates change nothing")
    eq([r for _, r in b.engine.transport], ["duplicate", "duplicate", "op_id_conflict"], "transport results")


def test_input_errors():
    def run(arrivals):
        eng = rules.Engine(ACC, NS)
        for a in arrivals:
            eng.accept(a)
        return eng

    b = B("input errors (not a vector)")
    b.vector_ok = False
    s = b.subscribe("A", U1)
    good = b.arrivals[0]

    def variant(fn):
        a = json.loads(json.dumps(good))
        fn(a)
        return [a]
    for url in ("https://user:pw@feeds.example.com/x.xml", "HTTP://u@host/feed", "https://@host/feed"):
        expect_input_error(lambda: run(variant(lambda a: a["fact"]["payload"].__setitem__("feed_url", url))),
                           "userinfo in feed_url %s" % url)
    check(not rules.has_userinfo("https://host/a@b"), "an @ in the path is not userinfo")
    check(not rules.has_userinfo("mailto:user@example.com"), "no authority, no userinfo")
    P, G = pg(s)
    e = ref(P, G, "ep")
    e["occurrence"]["enclosure_url"] = "https://user:pw@cdn.example.com/a.mp3"
    expect_input_error(lambda: b.fav("A", e, True), "userinfo in an enclosure URL")
    expect_input_error(lambda: b.pod("A", "change_url", P, G, {"url": "https://u:p@x.example/feed"}), "userinfo in change_url")
    expect_input_error(lambda: run(variant(lambda a: a.__setitem__("normalized_time", a["normalized_time"] - 1))),
                       "wrong normalized time")
    expect_input_error(lambda: run(variant(lambda a: a.__setitem__("edges", [s]))), "edges not the frontier")
    expect_input_error(lambda: run(variant(lambda a: a["fact"].__setitem__("type", "podcast.subscribe_v9"))),
                       "unknown type")
    expect_input_error(lambda: run(variant(lambda a: a["fact"].__setitem__("type_version", 2))), "unknown version")
    expect_input_error(lambda: run(variant(lambda a: a["fact"].__setitem__("dependencies", [b.uuid()]))),
                       "missing dependency")
    expect_input_error(lambda: run(variant(lambda a: a["fact"].__setitem__("op_id", "NOT-A-UUID"))), "unarchivable")


# ---------------------------------------------------------------------------
# PROTOCOL §8: listening, ownership, completion, history and statistics
# ---------------------------------------------------------------------------

MIN = 60000
D = 60 * MIN  # the episode's measured duration in these cases


def L(b, r, fg):
    """An episode record value (None if absent)."""
    return b.rec("episode", r["episode_id"], fg)


def posv(G, position, claim):
    return {"generation_id": G, "position": position, "claim_id": claim}


def setup_listening(b, n=1):
    """A subscribes U1; returns (P, G, [EpisodeRef...])."""
    s = b.subscribe("A", U1)
    P, G = pg(s)
    return P, G, [ref(P, G, "ep-%d" % i, "Episode %d" % i) for i in range(1, n + 1)]


def test_8a_ownership_handoff():
    b = B("scenario 8a: ownership handoff A->B->A")
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, 10 * MIN, 10 * MIN)  # A pauses at minute 10
    eq(L(b, e, "position"), posv(G, 10 * MIN, a1), "A owns the position")
    sb = b.uuid()
    b1 = b.start("B", e, sb, 10 * MIN, observed=a1, after=[a2], duration=D + 400)
    b2 = b.cp("B", e, sb, 1, 30 * MIN, 20 * MIN)  # B continues to minute 30
    eq(L(b, e, "position"), posv(G, 30 * MIN, b1), "B's claim owns it")
    lp, hist = L(b, e, "last_played"), L(b, e, "history")
    # A adopts 30: it seals its session at the pre-adoption position, without activity.
    a3 = b.end("A", e, sa, 2, 10 * MIN, 10 * MIN, reason="superseded", activity=False, history=False, after=[b2])
    eq(L(b, e, "position"), posv(G, 30 * MIN, b1), "the sealing end does not own the position")
    eq((L(b, e, "last_played"), L(b, e, "history")), (lp, hist), "adoption adds no last played or history time")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 30 * MIN}, "adoption adds no contribution")
    # The user resumes on A: a new session at the adopted position, to minute 45.
    sa2 = b.uuid()
    a4 = b.start("A", e, sa2, 30 * MIN, observed=b1)
    a5 = b.cp("A", e, sa2, 1, 45 * MIN, 15 * MIN)
    eq(L(b, e, "position"), posv(G, 45 * MIN, a4), "every device resumes at 45")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 45 * MIN}, "0-10 on A, 10-30 on B, 30-45 on A")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "unplayed")
    eq(L(b, e, "last_played"), {"generation_id": G, "time": b.nt(a5)}, "last played")
    eq(L(b, e, "history"), {"generation_id": G, "time": b.nt(a5)}, "history")
    eq(L(b, e, "measured_duration"),
       {"generation_id": G, "duration_ms": D + 400, "installation_id": b.dev("B")["id"]}, "last reported duration")
    eq([b.out(x) for x in (a1, a2, b1, b2, a3, a4, a5)], [("applied", None)] * 7, "outcomes")


def takeover_vs_finalize(name, history, takeover):
    """Scenario 8b / 31: B takes over (a session start, or an explicit position reset) from
    A's paused session before A's lazy finalization in event order, without observing it.
    Delivered in both orders; returns the two builders."""
    out = []
    prefix = B.count + 1
    for first in ("finalization", "takeover"):
        b = B("%s, %s delivered first" % (name, first), prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e = ref(P, G, "ep-1", "Episode 1")
        sa, sb = fixed_op(prefix, 100), fixed_op(prefix, 101)
        a1 = b.start("A", e, sa, 50 * MIN, duration=D, op=fixed_op(prefix, 2))
        a2 = b.cp("A", e, sa, 1, D - 10000, D - 10000 - 50 * MIN, op=fixed_op(prefix, 3))  # paused 10 s before the end
        base = b.now

        def finalize(rt):
            return b.fin("A", e, sa, 1, a1, completed=True, history=history, t=base + 2000, rt=rt,
                         op=fixed_op(prefix, 4))

        def take(rt):
            if takeover == "session":
                x = b.start("B", e, sb, D - 10000, observed=a1, after=[a2], t=base + 1000, rt=rt, op=fixed_op(prefix, 5))
                b.cp("B", e, sb, 1, D - 5000, 5000, t=base + 1500, rt=rt + 1000, op=fixed_op(prefix, 6))
                return x
            return b.ep("B", "episode.reset_position", e, after=[a2], t=base + 1000, rt=rt, op=fixed_op(prefix, 5))
        if first == "finalization":
            f = finalize(base + 5000)
            eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "the bundle fires at first")
            eq(L(b, e, "position"), posv(G, 0, a1), "position 0 under A's claim")
            eq(b.out(f), ("applied", None), "the finalization's receipt: its bundle fired")
            x = take(base + 6000)
        else:
            x = take(base + 5000)
            f = finalize(base + 7000)
            eq(b.out(f), ("superseded", None), "finalization receipt: its completion lost to the takeover")
        check(b.engine.rank[x] < b.engine.rank[f], "the takeover is ordered before the finalization")
        eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "the stale completion is suppressed")
        if takeover == "session":
            eq(L(b, e, "position"), posv(G, D - 5000, x), "B's position under B's claim")
            eq(L(b, e, "last_played"), {"generation_id": G, "time": base + 1500}, "B's save is the latest activity")
        else:
            eq(L(b, e, "position"), posv(G, 0, x), "the reset's position under its claim")
            eq(L(b, e, "last_played"), {"generation_id": G, "time": b.nt(a2)}, "a reset is not listening")
        want = b.nt(f) if history else (base + 1500 if takeover == "session" else b.nt(a2))
        eq(L(b, e, "history"), {"generation_id": G, "time": want}, "a suppressed bundle keeps its history mark")
        out.append(b)
    eq(out[0].engine.records(), out[1].engine.records(), "%s: both delivery orders converge" % name)
    return out


def test_takeover_vs_lazy_finalization():
    takeover_vs_finalize("scenario 8b: takeover vs lazy finalization", True, "session")
    takeover_vs_finalize("scenario 8b without a history mark", False, "session")


def test_31_stale_completion_vs_replay():
    b = B("scenario 31: completion observed everywhere, then replay")
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    b.cp("A", e, sa, 1, 30 * MIN, 30 * MIN)
    a3 = b.end("A", e, sa, 2, D, D, reason="ended", completed=True)
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "natural end completes")
    eq(L(b, e, "position"), posv(G, 0, a1), "position 0 under the session's claim")
    sb = b.uuid()
    b1 = b.start("B", e, sb, 0, observed=a1, after=[a3])
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "a replay start never un-plays")
    b2 = b.cp("B", e, sb, 1, 5 * MIN, 5 * MIN)
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "PLAYED remains")
    eq(L(b, e, "position"), posv(G, 5 * MIN, b1), "the replay owns the position")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": D + 5 * MIN}, "replaying accumulates")
    eq([b.out(x) for x in (a3, b1, b2)], [("applied", None)] * 3, "outcomes")
    # A later replay end that completes again under its own (current) claim.
    b3 = b.end("B", e, sb, 2, D, D, completed=True)
    eq(L(b, e, "position"), posv(G, 0, b1), "second completion")
    eq(b.out(b3), ("applied", None), "b3")
    # Separately: a takeover / an explicit reset ordered before a lazy completion, in both
    # network orders, suppresses that stale completion.
    takeover_vs_finalize("scenario 31: explicit reset vs lazy finalization", True, "reset")


def test_reset_position_bars_old_session():
    b = B("reset position bars an old session")
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0)
    a2 = b.cp("A", e, sa, 1, 10 * MIN, 10 * MIN)
    r = b.ep("B", "episode.reset_position", e, after=[a2])
    eq(L(b, e, "position"), posv(G, 0, r), "reset")
    a3 = b.cp("A", e, sa, 2, 20 * MIN, 20 * MIN)  # A, still playing, never saw the reset
    eq(L(b, e, "position"), posv(G, 0, r), "the old session cannot write the position any more")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 20 * MIN}, "but its listening counts")
    eq(L(b, e, "last_played"), {"generation_id": G, "time": b.nt(a3)}, "and it is listening activity")
    eq(b.out(a3), ("applied", None), "a3 is applied (it records listening)")
    a4 = b.end("A", e, sa, 3, D - 1000, D - 1000, completed=True)
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "an invalidated completion does not fire")
    eq(L(b, e, "position"), posv(G, 0, r), "position still the reset's")
    eq(b.out(a4), ("applied", None), "a4")
    # A new intentional claim after the reset owns the position again.
    sc = b.uuid()
    c1 = b.start("A", e, sc, 0, observed=r)
    b.cp("A", e, sc, 1, 2 * MIN, 2 * MIN)
    eq(L(b, e, "position"), posv(G, 2 * MIN, c1), "a new session after the reset")
    # Mark played with reset (a claim) bars it too and keeps PLAYED.
    m = b.mark_played("B", e)
    c3 = b.cp("A", e, sc, 2, 3 * MIN, 3 * MIN, after=[])
    eq(L(b, e, "position"), posv(G, 0, m), "mark played with reset is a claim")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "listening keeps PLAYED")
    eq(b.out(m), ("applied", None), "m")
    eq(b.out(c3), ("applied", None), "c3")
    eq(b.out(a1), ("applied", None), "a1")


def test_adoption_finalize_then_adopted_owner():
    b = B("adoption, finalization under the adopted claim, a further checkpoint from the adopted owner")
    P, G, (e, e2) = setup_listening(b, 2)
    sa, sb = b.uuid(), b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, 50 * MIN, 50 * MIN)  # A pauses
    b1 = b.start("B", e, sb, 50 * MIN, observed=a1, after=[a2])
    b2 = b.cp("B", e, sb, 1, D - 15000, D - 15000 - 50 * MIN)  # B pauses 15 s before the end
    # A adopts B's position (sealing its session at the pre-adoption position), then loads
    # another episode: the lazy finalization evaluates the adopted position under B's claim.
    a3 = b.end("A", e, sa, 2, 50 * MIN, 50 * MIN, reason="superseded", activity=False, history=False, after=[b2])
    a4 = b.fin("A", e, sa, 2, b1, completed=True)
    eq(b.out(a4), ("applied", None), "the finalization fires under the adopted claim")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "played")
    eq(L(b, e, "position"), posv(G, 0, b1), "position 0 published under B's claim, which stays current")
    b.start("A", e2, b.uuid(), 0)
    # B never saw the finalization; its claim is still current, so its next save owns the position.
    b3 = b.cp("B", e, sb, 2, D - 10000, D - 10000 - 50 * MIN)
    eq(L(b, e, "position"), posv(G, D - 10000, b1), "the adopted owner's further checkpoint")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "still played")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 50 * MIN + (D - 10000 - 50 * MIN)}, "both sessions count")
    eq(L(b, e, "last_played"), {"generation_id": G, "time": b.nt(b3)}, "last played")
    eq([b.out(x) for x in (a1, a2, b1, b2, a3, b3)], [("applied", None)] * 6, "outcomes")
    # A finalization naming a claim that is no longer current does not fire.
    b4 = b.ep("C", "episode.reset_position", e, after=[b3])
    sc = b.uuid()
    c1 = b.start("C", e, sc, 0)
    c2 = b.cp("C", e, sc, 1, D - 1000, D - 1000)
    c3 = b.fin("C", e, sc, 1, b4, completed=True, history=False)
    eq(b.out(c3), ("superseded", None), "the named claim is not current: the completion lost")
    eq(L(b, e, "position"), posv(G, D - 1000, c1), "unchanged")
    eq(b.out(c2), ("applied", None), "c2")


def test_reset_and_rewind_cuts():
    # PROTOCOL §8: reset at ordinal 2 (20 min), rewind to 5 min at ordinal 3, reset
    # again, ordinal 4 at 15 min -> 10 min.
    b = B("statistics cuts: reset, rewind, reset, advance (PROTOCOL §16 scenario 36)")
    P, G, (e,) = setup_listening(b)
    s = b.uuid()
    b.start("A", e, s, 0)
    b.cp("A", e, s, 1, 10 * MIN, 10 * MIN)
    b.cp("A", e, s, 2, 20 * MIN, 20 * MIN)
    r1 = b.sreset("A")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 0}, "after the first reset")
    b.cp("A", e, s, 3, 5 * MIN, 5 * MIN)  # rewind: contribution 5
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 0}, "residuals are never negative")
    r2 = b.sreset("A")
    b.cp("A", e, s, 4, 15 * MIN, 15 * MIN)
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 10 * MIN}, "15 - 5 = 10, not 0")
    eq((b.out(r1), b.out(r2)), (("applied", None), ("applied", None)), "resets")
    eq(L(b, e, "last_played"), {"generation_id": G, "time": b.nt(b.arrivals[-1]["fact"]["op_id"])},
       "a reset leaves last played")
    # Scenario 36: reset at 20, rewind to 10, reset, advance to 15 -> 5.
    b = B("scenario 36: reset at 20, rewind to 10, reset, advance to 15")
    P, G, (e,) = setup_listening(b)
    s = b.uuid()
    b.start("A", e, s, 0)
    b.cp("A", e, s, 1, 20 * MIN, 20 * MIN)
    b.sreset("A")
    b.cp("A", e, s, 2, 10 * MIN, 10 * MIN)
    b.sreset("A")
    b.cp("A", e, s, 3, 15 * MIN, 15 * MIN)
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 5 * MIN}, "result 5")
    # An ended session observed by a reset contributes zero thereafter.
    s2 = b.uuid()
    b.start("A", e, s2, 15 * MIN)
    b.end("A", e, s2, 1, 25 * MIN, 10 * MIN, reason="switched")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 15 * MIN}, "5 + 10")
    b.sreset("B", after=[b.arrivals[-1]["fact"]["op_id"]])
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 0}, "everything observed")
    # Scenario 37: listen 0->10, restart the player (new session), rewind to 5 and save: 10.
    b = B("scenario 37: restart, rewind below the new baseline")
    P, G, (e, e2) = setup_listening(b, 2)
    s1, s2 = b.uuid(), b.uuid()
    b.start("A", e, s1, 0)
    a2 = b.cp("A", e, s1, 1, 10 * MIN, 10 * MIN)
    b.start("A", e, s2, 10 * MIN)  # after process death: a new session, new baseline
    b.cp("A", e, s2, 1, 5 * MIN, 0)  # below the start: the contribution stays 0 (PROTOCOL §8.8)
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 10 * MIN}, "total stays 10")
    # Pause near the end, terminate/relaunch and start another episode: lazy finalization
    # still completes once.
    s3 = b.uuid()
    c1 = b.start("A", e, s3, 5 * MIN)
    b.cp("A", e, s3, 1, D - 20000, D - 20000 - 5 * MIN)
    f = b.fin("A", e, s3, 1, c1)
    eq(b.out(f), ("applied", None), "completes")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "played once")
    f2 = b.fin("A", e, s3, 1, c1)
    eq(b.out(f2), ("not_applied", "session_finalized"), "never twice")
    b.start("A", e2, b.uuid(), 0)
    eq(b.out(a2), ("applied", None), "a2")
    # Scenario 8d: start at 10, forward to 15, rewind below the start, switch: Android's 5.
    b = B("scenario 8d: rewind below the session start, then switch")
    P, G, (e,) = setup_listening(b)
    s = b.uuid()
    b.start("A", e, s, 10 * MIN)
    b.cp("A", e, s, 1, 15 * MIN, 5 * MIN)
    b.cp("A", e, s, 2, 2 * MIN, 5 * MIN)
    b.end("A", e, s, 3, 3 * MIN, 5 * MIN, reason="switched")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 5 * MIN}, "frozen below the baseline")
    eq(L(b, e, "position"), {"generation_id": G, "position": 3 * MIN, "claim_id": b.arrivals[1]["fact"]["op_id"]},
       "position of the switch")


def interleavings(xs, ys):
    if not xs:
        return [list(ys)]
    if not ys:
        return [list(xs)]
    return [[xs[0]] + r for r in interleavings(xs[1:], ys)] + [[ys[0]] + r for r in interleavings(xs, ys[1:])]


def test_concurrent_cuts_all_orders():
    # Scenario 8c and 36: two offline phones each reset statistics (and B clears history)
    # after listening; every upload order of their offline facts gives the same records.
    prefix = B.count + 1
    finals = []
    chain_a = ["cpA2", "resetA", "cpA3"]
    chain_b = ["cpB2", "resetB", "clearB"]
    orders = interleavings(chain_a, chain_b)
    eq(len(orders), 20, "interleavings")
    for k, order in enumerate(orders):
        b = B("concurrent cuts, upload order %d: %s" % (k + 1, " ".join(order)), prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e1, e2 = ref(P, G, "ep-1", "One"), ref(P, G, "ep-2", "Two")
        sa, sb = fixed_op(prefix, 100), fixed_op(prefix, 101)
        b.start("A", e1, sa, 0, op=fixed_op(prefix, 2))
        a_cp1 = b.cp("A", e1, sa, 1, 10 * MIN, 10 * MIN, op=fixed_op(prefix, 3))
        b.start("B", e2, sb, 0, after=[a_cp1], op=fixed_op(prefix, 4))
        b_cp1 = b.cp("B", e2, sb, 1, 5 * MIN, 5 * MIN, op=fixed_op(prefix, 5))
        base = b.now
        facts = {
            "cpA2": lambda rt: b.cp("A", e1, sa, 2, 20 * MIN, 20 * MIN, after=[b_cp1], t=base + 1000, rt=rt,
                                    op=fixed_op(prefix, 6)),
            "resetA": lambda rt: b.sreset("A", t=base + 2000, rt=rt, op=fixed_op(prefix, 7)),
            "cpA3": lambda rt: b.cp("A", e1, sa, 3, 25 * MIN, 25 * MIN, t=base + 3000, rt=rt, op=fixed_op(prefix, 8)),
            "cpB2": lambda rt: b.cp("B", e2, sb, 2, 15 * MIN, 15 * MIN, t=base + 1500, rt=rt, op=fixed_op(prefix, 9)),
            "resetB": lambda rt: b.sreset("B", t=base + 2500, rt=rt, op=fixed_op(prefix, 10)),
            "clearB": lambda rt: b.hclear("B", t=base + 3500, rt=rt, op=fixed_op(prefix, 11)),
        }
        for i, name in enumerate(order):
            facts[name](base + 100000 + 1000 * i)
        # A's listening up to its reset and B's up to its reset are gone; each reset also
        # cut what it had seen of the other device.
        eq(L(b, e1, "played_time"), {"generation_id": G, "played_ms": 5 * MIN}, "e1: 25 - 20")
        eq(L(b, e2, "played_time"), {"generation_id": G, "played_ms": 0}, "e2: 15 - 15")
        eq(L(b, e1, "history"), {"generation_id": G, "time": base + 3000}, "e1: A's unobserved saves survive B's clear")
        eq(L(b, e2, "history"), {"generation_id": G, "time": None}, "e2: everything B observed is cleared")
        eq(L(b, e1, "last_played"), {"generation_id": G, "time": base + 3000}, "e1 last played")
        eq(L(b, e2, "last_played"), {"generation_id": G, "time": base + 1500}, "e2: a clear never zeroes last played")
        check(all(o["outcome"] == "applied" for o in b.engine.outcomes.values()), "all applied")
        finals.append(b.engine.records())
    for k in range(1, len(finals)):
        eq(finals[k], finals[0], "upload order %d converges with order 1" % (k + 1))


def test_history_cuts_and_unobserved_activity():
    b = B("history remove, restore and clear with unobserved concurrent activity")
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0)
    a2 = b.cp("A", e, sa, 1, 10 * MIN, 10 * MIN)  # pause
    H = lambda: L(b, e, "history")  # noqa: E731
    LP = lambda: L(b, e, "last_played")  # noqa: E731
    eq(H(), {"generation_id": G, "time": b.nt(a2)}, "in History")
    r1 = b.hremove("B", e, after=[a2])
    eq(H(), {"generation_id": G, "time": None}, "removed from History")
    eq(LP(), {"generation_id": G, "time": b.nt(a2)}, "last played unchanged by the removal")
    x1 = b.hrestore("B", e, r1, b.nt(a2))
    eq(H(), {"generation_id": G, "time": b.nt(a2)}, "Undo restores exactly the removal's entries")
    eq(b.out(x1), ("applied", None), "restore applied")
    x2 = b.hrestore("B", e, r1, b.nt(a2))
    eq(b.out(x2), ("not_applied", "already_restored"), "second restore of the same removal")
    r2 = b.hremove("B", e)
    eq(H(), {"generation_id": G, "time": None}, "removed again")
    # A resumes concurrently (it never saw r2), with a clock behind B's: its save is ordered
    # before r2 but r2 did not observe it, so it survives whatever the wall clock says.
    a3 = b.cp("A", e, sa, 2, 12 * MIN, 12 * MIN, t=b.nt(r2) - 500)
    check(b.engine.rank[a3] < b.engine.rank[r2], "the unobserved save is ordered before the removal")
    eq(H(), {"generation_id": G, "time": b.nt(a3)}, "unobserved activity survives the removal")
    eq(LP(), {"generation_id": G, "time": b.nt(a3)}, "last played")
    c1 = b.hclear("C", after=[a3, r2])
    eq(H(), {"generation_id": G, "time": None}, "a clear covering everything")
    eq(LP(), {"generation_id": G, "time": b.nt(a3)}, "a clear never changes last played")
    # Undo of B's removal cannot cancel C's clear.
    x3 = b.hrestore("B", e, r2, b.nt(a2), after=[c1])
    eq(b.out(x3), ("applied", None), "r2's cut cancelled")
    eq(H(), {"generation_id": G, "time": None}, "the clear still covers everything it observed")
    # A keeps listening without having seen the clear: a save ordered before it survives.
    a4 = b.cp("A", e, sa, 3, 15 * MIN, 15 * MIN, t=b.nt(c1) - 300)
    eq(H(), {"generation_id": G, "time": b.nt(a4)}, "unobserved activity survives the clear")
    eq(LP(), {"generation_id": G, "time": b.nt(a4)}, "last played follows activity")
    # A skips from the paused state: a history mark without listening activity.
    a5 = b.end("A", e, sa, 4, 15 * MIN, 15 * MIN, reason="skipped", activity=False, history=True, rfq=True)
    eq(H(), {"generation_id": G, "time": b.nt(a5)}, "a history mark moves History")
    eq(LP(), {"generation_id": G, "time": b.nt(a4)}, "but never last played")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 15 * MIN}, "history cuts never change played time")
    eq(L(b, e, "position"), posv(G, 15 * MIN, a1), "nor the position")
    # A removal on another device observing the mark hides it again.
    r3 = b.hremove("C", e, after=[a5])
    eq(H(), {"generation_id": G, "time": None}, "marks are history entries too")
    eq([b.out(x) for x in (r1, r2, r3, c1, a5)], [("applied", None)] * 5, "cuts are applied")
    # A removal of an episode without entries is applied but creates no history record.
    e2 = ref(P, G, "ep-2", "Two")
    r4 = b.hremove("C", e2)
    eq(b.out(r4), ("applied", None), "removal without entries")
    check(L(b, e2, "history") is None, "no history record without an entry")
    eq(L(b, e2, "identity"), {"podcast_id": P, "occurrence": e2["occurrence"]}, "the removal names (binds) the episode")


def test_finalization_days_later():
    b = B("a finalization days later moves History but not last played")
    P, G, (e, e2) = setup_listening(b, 2)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, D - 20000, D - 20000)  # paused 20 s before the end
    b.wait(3 * 24 * 3600 * 1000)
    f = b.fin("A", e, sa, 1, a1, completed=True, history=True)
    b.start("A", e2, b.uuid(), 0)
    eq(b.out(f), ("applied", None), "finalization")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "completed within the threshold")
    eq(L(b, e, "position"), posv(G, 0, a1), "position 0 under the session's claim")
    eq(L(b, e, "history"), {"generation_id": G, "time": b.nt(f)}, "History moves to the finalization time")
    eq(L(b, e, "last_played"), {"generation_id": G, "time": b.nt(a2)}, "last played (the statistics month) does not")
    check(b.nt(f) - b.nt(a2) > 3 * 24 * 3600 * 1000 - 5000, "days later")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": D - 20000}, "no second contribution")
    # Outside the threshold a finalization never completes, but it still marks History.
    sb = b.uuid()
    b1 = b.start("A", e2, sb, 0)
    b.cp("A", e2, sb, 1, 20 * MIN, 20 * MIN)
    f2 = b.fin("A", e2, sb, 1, b1, completed=False, history=True)
    eq(b.out(f2), ("applied", None), "a history mark is recorded")
    eq(L(b, e2, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "not completed")
    eq(L(b, e2, "history"), {"generation_id": G, "time": b.nt(f2)}, "History")


def test_author_sequence_gaps():
    # PROTOCOL §6.4 admits an author's sequence 5 before its sequence 2; a cut observing only
    # the first must not cover the second (coverage is by fact identity, not by author
    # coordinates).
    b = B("author sequence gaps: sequence 5 admitted before 2, a cut observing only the first")
    P, G, (e,) = setup_listening(b)
    s = b.arrivals[0]["fact"]["op_id"]
    dA = b.dev("A")
    seq = lambda n: (lambda f: f.__setitem__("author_seq", n))  # noqa: E731
    s5 = b.uuid()
    x5 = b.start("A", e, s5, 0, patch=seq(5))
    dA["seq"] = 5
    x6 = b.cp("A", e, s5, 1, 10 * MIN, 10 * MIN)
    eq(b.engine.facts[x6]["fact"]["author_seq"], 6, "sequence 6")
    c = b.hclear("C", after=[x6])
    r = b.sreset("C")
    eq(L(b, e, "history"), {"generation_id": G, "time": None}, "the clear covers sequences 5 and 6")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 0}, "the reset too")
    # A's sequence 2 arrives late (it observed only A's sequence 1), with an early clock.
    dA["seq"], dA["prev"] = 1, s
    s2 = b.uuid()
    y2 = b.start("A", e, s2, 0, t=b.nt(x5) - 200)
    y3 = b.cp("A", e, s2, 1, 7 * MIN, 7 * MIN, t=b.nt(x5) - 100)
    eq([b.engine.facts[y]["fact"]["author_seq"] for y in (y2, y3)], [2, 3], "sequences 2 and 3")
    check(b.engine.rank[y3] < b.engine.rank[c], "ordered before the clear")
    eq(L(b, e, "history"), {"generation_id": G, "time": b.nt(y3)}, "sequences 2 and 3 were not observed by the clear")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 7 * MIN}, "nor by the reset")
    eq(L(b, e, "position"), posv(G, 10 * MIN, x5), "x5's session started later in order and owns the position")
    eq([b.out(x) for x in (x5, x6, c, r, y2, y3)], [("applied", None)] * 6, "outcomes")


def test_cut_after_rejected_predecessor():
    b = B("cuts after a rejected predecessor observe what it observed")
    P, G, (e,) = setup_listening(b)
    s = b.uuid()
    b.start("A", e, s, 0)
    b.cp("A", e, s, 1, 10 * MIN, 10 * MIN)
    bad1 = b.cp("A", e, s, 2, 11 * MIN, -1, reject=True)  # an invalid contribution
    eq(b.out(bad1), ("rejected", "invalid_payload"), "rejected")
    c = b.hclear("A")  # its only dependency is the rejected fact
    eq(b.engine.facts[c]["frontier"], b.engine.facts[bad1]["frontier"], "the clear observes what the rejected fact did")
    eq(L(b, e, "history"), {"generation_id": G, "time": None}, "the clear covers the earlier saves")
    bad2 = b.push("A", "statistics.reset", {"observed": []}, [], reject=True)
    eq(b.out(bad2), ("rejected", "invalid_payload"), "rejected reset")
    r = b.sreset("A")
    eq(b.engine.facts[r]["frontier"], [c], "resolved through the rejected reset")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 0}, "the reset covers ordinal 1")
    eq((b.out(c), b.out(r)), (("applied", None), ("applied", None)), "cuts applied")


def test_listening_admission_faults():
    b = B("listening admission faults")
    P, G, (e, e2) = setup_listening(b, 2)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0)
    a2 = b.cp("A", e, sa, 1, 5 * MIN, 5 * MIN)
    O = b.out
    # 3. Only the session's author may append or finalize.
    eq(O(b.cp("B", e, sa, 2, 6 * MIN, 6 * MIN, after=[a2])), ("not_applied", "foreign_session"), "foreign checkpoint")
    eq(O(b.end("B", e, sa, 3, 6 * MIN, 6 * MIN)), ("not_applied", "foreign_session"), "foreign end")
    eq(O(b.fin("B", e, sa, 1, a1)), ("not_applied", "foreign_session"), "foreign finalization")
    # A second start of a registered session (CHOICE).
    eq(O(b.start("B", e, sa, 0)), ("not_applied", "foreign_session"), "another author restarts the session")
    eq(O(b.start("A", e, sa, 0)), ("not_applied", "session_ordinal_conflict"), "the author restarts its session")
    eq(O(b.start("A", e2, sa, 0)), ("rejected", "invalid_payload"), "a start of the session on another episode")
    # 4. Ordinals.
    eq(O(b.cp("A", e, sa, 1, 6 * MIN, 6 * MIN)), ("not_applied", "session_ordinal_conflict"), "existing ordinal")
    eq(O(b.cp("A", e, sa, 1, 5 * MIN, 5 * MIN)), ("not_applied", "session_ordinal_conflict"),
       "identical content in another fact")
    a3 = b.cp("A", e, sa, 3, 7 * MIN, 7 * MIN)
    eq(O(a3), ("applied", None), "ordinals may skip")
    eq(O(b.cp("A", e, sa, 2, 6 * MIN, 6 * MIN)), ("not_applied", "session_ordinal_conflict"), "below the latest")
    # 1. Unknown sessions and ordinals; 2. another episode.
    eq(O(b.cp("A", e, b.uuid(), 1, 0, 0)), ("rejected", "invalid_payload"), "unknown session")
    eq(O(b.fin("A", e, sa, 2, a1)), ("rejected", "invalid_payload"), "finalization naming an ordinal it does not have")
    eq(O(b.fin("A", e, sa, 9, a1)), ("rejected", "invalid_payload"), "finalization naming a future ordinal")
    eq(O(b.cp("A", e2, sa, 4, 8 * MIN, 8 * MIN)), ("rejected", "invalid_payload"), "checkpoint on another episode")
    eq(O(b.cp("B", e2, sa, 4, 8 * MIN, 8 * MIN, after=[a3])), ("rejected", "invalid_payload"),
       "check 2 comes before check 3")
    eq(O(b.cp("C", e, b.uuid(), 1, 0, 0, after=[a3])), ("rejected", "invalid_payload"), "check 1 before check 3")
    # Sealing.
    a4 = b.end("A", e, sa, 4, 8 * MIN, 8 * MIN, reason="switched")
    eq(O(a4), ("applied", None), "end")
    eq(O(b.cp("A", e, sa, 5, 9 * MIN, 9 * MIN)), ("not_applied", "session_sealed"), "checkpoint after the end")
    eq(O(b.end("A", e, sa, 6, 9 * MIN, 9 * MIN)), ("not_applied", "session_sealed"), "second end")
    eq(O(b.cp("A", e, sa, 4, 9 * MIN, 9 * MIN)), ("not_applied", "session_ordinal_conflict"),
       "an existing ordinal after the end (CHOICE: conflict before sealed)")
    # Finalization: at most once; it may follow the end; it does not seal (CHOICE).
    a5 = b.fin("A", e, sa, 4, a1, completed=False)
    eq(O(a5), ("applied", None), "finalization after the end")
    eq(O(b.fin("A", e, sa, 4, a1, completed=False)), ("not_applied", "session_finalized"), "double finalization")
    eq(O(b.fin("A", e, sa, 3, a1)), ("not_applied", "session_finalized"), "double finalization, other ordinal")
    sc = b.uuid()
    c1 = b.start("A", e, sc, 8 * MIN)
    b.cp("A", e, sc, 1, 9 * MIN, MIN)
    eq(O(b.fin("A", e, sc, 1, c1, completed=False, history=False)), ("applied", None),
       "a finalization that decides nothing records that decision")
    eq(O(b.fin("A", e, b.uuid(), 1, c1)), ("rejected", "invalid_payload"), "finalization of an unknown session")
    c4 = b.cp("A", e, sc, 2, 10 * MIN, 2 * MIN)
    eq(O(c4), ("applied", None), "a finalization does not seal")
    eq(L(b, e, "position"), posv(G, 10 * MIN, c1), "position")
    sd = b.uuid()
    d1 = b.start("A", e, sd, 0)
    b.cp("A", e, sd, 1, MIN, MIN)
    eq(O(b.fin("A", e, sd, 1, d1, completed=False, rfq=True, history=False)), ("applied", None),
       "a recorded queue removal makes it applied")
    # 5. Restore references.
    r1 = b.hremove("A", e)
    eq(O(b.hrestore("A", e2, r1, 0)), ("rejected", "invalid_payload"), "a removal of another episode")
    eq(O(b.hrestore("A", e, a2, 0)), ("rejected", "invalid_payload"), "not a removal")
    rr = b.push("A", "history.remove", {"episode": e, "extra": True},
                [{"entity": "episode", "id": e["episode_id"], "generation": G}], reject=True)
    eq(O(b.hrestore("A", e, rr, 0)), ("rejected", "invalid_payload"), "a rejected removal")
    eq(O(b.hrestore("A", e, r1, 0)), ("applied", None), "a valid restore")
    eq(O(b.hrestore("B", e, r1, 0)), ("not_applied", "already_restored"), "already restored")
    # Capabilities: history and statistics are optional; sessions are core.
    x = b.hremove("C", e, caps=["queue"], reject=True)
    eq(O(x), ("rejected", "capability_not_declared"), "history.remove needs history")
    eq(O(b.hclear("C", caps=["statistics"], reject=True)), ("rejected", "capability_not_declared"), "clear")
    eq(O(b.sreset("C", caps=["history"], reject=True)), ("rejected", "capability_not_declared"), "reset")
    eq(O(b.start("C", e, b.uuid(), 0, caps=[])), ("applied", None), "sessions are core")
    # Episode checks come first: unknown generation, closed generation.
    bad = ref(P, c.generation_id(NS, P, b.uuid()), "ep-1")
    eq(O(b.start("A", bad, b.uuid(), 0)), ("not_applied", "unknown_generation"), "unknown generation")
    eq(O(b.cp("A", bad, sa, 7, 0, 0)), ("not_applied", "unknown_generation"),
       "generation checks precede the session checks")
    eq(O(b.hremove("A", bad)), ("not_applied", "unknown_generation"), "removal on an unknown generation")


def test_listening_payload_rejections():
    b = B("listening payload rejections")
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0)
    et = [{"entity": "episode", "id": e["episode_id"], "generation": G}]
    st = {"episode": e, "session_id": sa, "start_position": 0, "activity": True}
    ck = {"episode": e, "session_id": sa, "ordinal": 1, "position": 0, "contribution": 0, "activity": True}
    en = dict(ck, reason="ended", completed=True, threshold_ms=30000, remove_from_queue=False, history=True)
    fi = {"episode": e, "session_id": sa, "ordinal": 1, "claim_id": a1, "completed": True, "threshold_ms": 0,
          "remove_from_queue": False, "history": True}

    def w(d, **kv):
        x = dict(d)
        for k, v in kv.items():
            if v is None:
                x.pop(k)
            else:
                x[k] = v
        return x
    bad = [
        ("session.start", w(st, activity=None), et),
        ("session.start", w(st, start_position=-1), et),
        ("session.start", w(st, session_id="S1"), et),
        ("session.start", w(st, observed_claim="claim"), et),
        ("session.start", w(st, queue_bundle=[]), et),
        ("session.start", w(st, duration=-5), et),
        ("session.start", w(st, ordinal=1), et),
        ("session.start", st, []),
        ("session.checkpoint", w(ck, ordinal=0), et),
        ("session.checkpoint", w(ck, contribution=-1), et),
        ("session.checkpoint", w(ck, position="0"), et),
        ("session.checkpoint", w(ck, activity=1), et),
        ("session.checkpoint", w(ck, reason="ended"), et),
        ("session.end", w(en, reason="paused"), et),
        ("session.end", w(en, threshold_ms=-1), et),
        ("session.end", w(en, history=None), et),
        ("session.end", w(en, completed="yes"), et),
        ("session.finalize", w(fi, claim_id=None), et),
        ("session.finalize", w(fi, claim_id="x"), et),
        ("session.finalize", w(fi, position=0), et),
        ("session.finalize", w(fi, duration=D), et),
        ("history.remove", {"episode": e, "time": 0}, et),
        ("history.remove", {"episode": e}, []),
        ("history.restore", {"episode": e, "removal": "r", "time": 0}, et),
        ("history.restore", {"episode": e, "removal": a1}, et),
        ("history.restore", {"episode": e, "removal": a1, "time": "now"}, et),
        ("history.clear", {"observed": [a1]}, []),
        ("history.clear", {}, et),
        ("statistics.reset", {}, [{"entity": "account", "id": ACC}]),
    ]
    for typ, payload, targets in bad:
        x = b.push("A", typ, payload, targets, reject=True)
        eq(b.out(x), ("rejected", "invalid_payload"), "%s %s" % (typ, json.dumps(payload)[:90]))
    ok = b.push("A", "session.checkpoint", w(ck, duration=0), et)
    eq(b.out(ok), ("applied", None), "duration 0 is a valid duration")
    # queue_bundle is {key, position_id, place} (PROTOCOL §7.6): "any object" is
    # invalid, and so are `moves`.
    x = b.push("A", "session.start", w(st, session_id=b.uuid(), queue_bundle={"add": {"anything": 1}}), et, reject=True)
    eq(b.out(x), ("rejected", "invalid_payload"), "queue_bundle has the bundle schema")
    pid = det_uuid("payload rejections bundle")
    key = c.queue_between(None, None, c.uuid_to_u(pid))
    retired = b.push("A", "session.start", w(st, session_id=b.uuid(),
                                             queue_bundle={"key": key, "position_id": pid, "moves": []}), et, reject=True)
    eq(b.out(retired), ("rejected", "invalid_payload"), "moves are invalid; place is required")
    ok2 = b.push("A", "session.start", w(st, session_id=b.uuid(),
                                         queue_bundle={"key": key, "position_id": pid, "place": {"kind": "end"}}), et)
    eq(b.out(ok2), ("applied", None), "a valid play-start bundle")


def test_listening_generation_closure():
    b = B("listening in a closed generation")
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, 10 * MIN, 10 * MIN)
    for fg in ("position", "last_played", "history", "played_time", "measured_duration"):
        check(L(b, e, fg) is not None, "%s exists" % fg)
    u = b.pod("B", "unsubscribe", P, G, after=[a2])
    for fg in ("play_state", "position", "last_played", "history", "played_time", "measured_duration"):
        check(L(b, e, fg) is None, "closure tombstones %s" % fg)
    x = b.cp("A", e, sa, 2, 20 * MIN, 20 * MIN)
    eq(b.out(x), ("not_applied", "generation_closed"), "listening after the removal is recorded, not applied")
    eq(b.out(b.hremove("A", e)), ("not_applied", "generation_closed"), "removal")
    eq(b.out(b.hclear("B")), ("applied", None), "an account-level clear is applied")
    eq(b.out(b.sreset("B")), ("applied", None), "an account-level reset is applied")
    r = b.subscribe("B", U1, observed=obs(P, G, "removed"))
    G2 = c.generation_id(NS, P, r)
    e2 = dict(e, generation_id=G2)
    s2 = b.uuid()
    y = b.start("B", e2, s2, 0)
    eq(L(b, e, "position"), posv(G2, 0, y), "fresh state in the new generation")
    eq(L(b, e, "played_time"), {"generation_id": G2, "played_ms": 0}, "removed generations never count")
    # A session of the closed generation cannot continue in the new one.
    eq(b.out(b.cp("A", e2, sa, 3, 0, 0)), ("rejected", "invalid_payload"), "another generation than its start")
    eq((b.out(a1), b.out(u)), (("applied", None), ("applied", None)), "receipts")


def test_scenarios_5_6_7():
    # Scenario 5: two offline phones listen to the same episode; the position is the session
    # that started last; statistics count both. Delivered in both orders.
    prefix = B.count + 1
    finals = []
    for first in ("A", "B"):
        b = B("scenario 5: two offline phones, %s uploads first" % first, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e = ref(P, G, "ep-1", "One")
        base = b.now
        sa, sb = fixed_op(prefix, 100), fixed_op(prefix, 101)
        ups = {
            "A": lambda rt: (b.start("A", e, sa, 0, t=base + 1000, rt=rt, op=fixed_op(prefix, 2)),
                             b.cp("A", e, sa, 1, 30 * MIN, 30 * MIN, t=base + 9000, rt=rt + 10, op=fixed_op(prefix, 3))),
            "B": lambda rt: (b.start("B", e, sb, 0, after=[s], t=base + 2000, rt=rt, op=fixed_op(prefix, 4)),
                             b.cp("B", e, sb, 1, 12 * MIN, 12 * MIN, t=base + 5000, rt=rt + 10, op=fixed_op(prefix, 5))),
        }
        ups[first](base + 50000)
        ups["B" if first == "A" else "A"](base + 60000)
        eq(L(b, e, "position"), posv(G, 12 * MIN, fixed_op(prefix, 4)), "the session that started last")
        eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 42 * MIN}, "statistics count both")
        eq(L(b, e, "last_played"), {"generation_id": G, "time": base + 9000}, "latest save")
        finals.append(b.engine.records())
    eq(finals[0], finals[1], "scenario 5 converges")
    # Scenario 6: an old session keeps sending checkpoints; the newer listening is never rewound.
    b = B("scenario 6: an old session keeps sending checkpoints")
    P, G, (e,) = setup_listening(b)
    so, sn = b.uuid(), b.uuid()
    o1 = b.start("W", e, so, 0)
    b.cp("W", e, so, 1, 5 * MIN, 5 * MIN)
    n1 = b.start("A", e, sn, 40 * MIN, observed=o1, after=[o1])
    b.cp("A", e, sn, 1, 41 * MIN, MIN)
    for k in range(2, 6):
        b.cp("W", e, so, k, (5 + k) * MIN, (5 + k) * MIN)
    eq(L(b, e, "position"), posv(G, 41 * MIN, n1), "never rewound")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": 11 * MIN}, "both count")
    # Scenario 7: paused 10 s before the end on A, replayed from the start on B; later A
    # starts another episode: not marked played.
    b = B("scenario 7: replay elsewhere before A's lazy finalization")
    P, G, (e, e2) = setup_listening(b, 2)
    sa, sb = b.uuid(), b.uuid()
    a1 = b.start("A", e, sa, 50 * MIN, duration=D)
    a2 = b.cp("A", e, sa, 1, D - 10000, D - 10000 - 50 * MIN)
    b1 = b.start("B", e, sb, 0, observed=a1, after=[a2])
    b.cp("B", e, sb, 1, 3 * MIN, 3 * MIN)
    f = b.fin("A", e, sa, 1, a1, completed=True)  # A never saw B's replay
    b.start("A", e2, b.uuid(), 0)
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "not marked played")
    eq(L(b, e, "position"), posv(G, 3 * MIN, b1), "B's position")
    eq(b.out(f), ("superseded", None), "the completion lost; its history mark still counts")


def test_measured_duration_and_barriers():
    b = B("measured duration register; listening barriers")
    P, G, (e, e2) = setup_listening(b, 2)
    sa, sb = b.uuid(), b.uuid()
    b.start("A", e, sa, 0, duration=D)
    eq(L(b, e, "measured_duration"), {"generation_id": G, "duration_ms": D, "installation_id": b.dev("A")["id"]}, "A")
    base = b.now
    b.start("B", e, sb, 0, duration=D + 700, t=base + 5000, rt=base + 6000)
    b.cp("A", e, sa, 1, MIN, MIN, duration=D - 300, t=base + 4000, rt=base + 7000)
    eq(L(b, e, "measured_duration"),
       {"generation_id": G, "duration_ms": D + 700, "installation_id": b.dev("B")["id"]}, "the last in order wins")
    b.cp("A", e, sa, 2, 2 * MIN, 2 * MIN, duration=D - 200)
    eq(L(b, e, "measured_duration"),
       {"generation_id": G, "duration_ms": D - 200, "installation_id": b.dev("A")["id"]}, "later")
    check(L(b, e2, "measured_duration") is None, "no record without a report")
    # A start dismisses NEW and is a barrier against automatic filing; never un-plays.
    b2 = B("listening barriers against automatic filing")
    P, G, (e,) = setup_listening(b2, 1)
    s = b2.arrivals[0]["fact"]["op_id"]
    b2.discover("D", e, after=[s])
    eq(L(b2, e, "play_state")["play_state"], "new", "filed NEW")
    x = b2.start("A", e, b2.uuid(), 0, activity=False)
    eq(L(b2, e, "play_state")["play_state"], "unplayed", "a start dismisses NEW")
    b2.restore("A", e, "new", 0)
    sc = b2.uuid()
    cx = b2.start("A", e, sc, 0, activity=False)
    eq(L(b2, e, "play_state")["play_state"], "unplayed", "again")
    b2.restore("A", e, "new", 0)
    b2.cp("A", e, sc, 1, MIN, MIN, activity=False)
    eq(L(b2, e, "play_state")["play_state"], "new", "a save without activity does not dismiss")
    b2.cp("A", e, sc, 2, MIN, MIN, activity=True)
    eq(L(b2, e, "play_state")["play_state"], "unplayed", "a listening save does")
    e3 = ref(P, G, "ep-3")
    b2.start("A", e3, b2.uuid(), 0, activity=False)
    b2.discover("E", e3, after=[s], t=b2.now + 5000)
    eq(L(b2, e3, "play_state")["play_state"], "unplayed", "a start is a barrier whatever the order")
    check(L(b2, e3, "last_played") is None and L(b2, e3, "history") is None, "no activity, no history")
    eq((b2.out(x), b2.out(cx)), (("applied", None), ("applied", None)), "starts")


def test_fanout(n_episodes, n_saves, vector):
    b = B("history clear and statistics reset fan-out: %d episodes x %d saves" % (n_episodes, n_saves))
    b.vector_ok = vector
    b.check_every = vector
    P, G, eps = setup_listening(b, n_episodes)
    devs = ["A", "B", "C"]
    for i, e in enumerate(eps):
        d = devs[i % 3]
        s = b.uuid()
        b.start(d, e, s, 0)
        for k in range(1, n_saves + 1):
            b.cp(d, e, s, k, k * MIN, k * MIN)
    last = [b.devs[d]["prev"] for d in devs]
    hc = b.hclear("D", after=last)
    sr = b.sreset("D")
    eq(b.engine.records(), b.engine.full_records(), "incremental vs full")
    for e in eps:
        eq(L(b, e, "history")["time"], None, "cleared")
        eq(L(b, e, "played_time")["played_ms"], 0, "reset")
        check(L(b, e, "last_played") is not None, "last played kept")
    fan = sum(1 for key, cts in b.engine.scopes.items() if any(ct.node == hc for ct in cts))
    eq(fan, n_episodes, "the clear fans out to every episode scope")
    # Concurrent activity the cuts did not observe survives in the episodes it touches.
    e = eps[0]
    s = b.uuid()
    b.devs["A"]["prev"] = last[0]
    y = b.start("A", e, s, 0, t=b.nt(hc) - 100)
    b.cp("A", e, s, 1, 3 * MIN, 3 * MIN)
    eq(L(b, e, "played_time")["played_ms"], 3 * MIN, "unobserved session counts")
    check(L(b, e, "history")["time"] is not None, "unobserved activity is in History")
    eq((b.out(hc), b.out(sr), b.out(y)), (("applied", None),) * 3, "outcomes")


def test_restore_unknown_removal_is_an_input_error():
    b = B("restore naming an unknown removal (not a vector)")
    b.vector_ok = False
    P, G, (e,) = setup_listening(b)
    expect_input_error(lambda: b.hrestore("A", e, b.uuid(), 0), "unknown removal: dependency_missing")
    # Even without the capability: the prerequisite is checked with the dependencies.
    expect_input_error(lambda: b.hrestore("C", e, b.uuid(), 0, caps=[]), "unknown removal before capabilities")


# ---------------------------------------------------------------------------
# PROTOCOL §7.6: the queue
# ---------------------------------------------------------------------------

NOQ = ["favorites", "history", "inbox", "statistics"]  # a client without the queue capability


def qref(P, G, guid, title=None, pub=None):
    r = ref(P, G, guid, title)
    if pub is not None:
        r["occurrence"]["publication_time"] = pub
    return r


def K(low, high, label):
    """A key strictly between `low` and `high` (keys or None) with a deterministic position ID."""
    pid = det_uuid(label)
    return c.queue_between(low, high, c.uuid_to_u(pid)), pid


def keys_in_order(labels):
    out, low = [], None
    for lab in labels:
        kp = K(low, None, lab)
        out.append(kp)
        low = kp[0]
    return out


def MEM(b, r):
    return b.rec("episode", r["episode_id"], "queue_membership")


def mem(G, in_queue, reason=None):
    v = {"generation_id": G, "in_queue": in_queue}
    if reason is not None:
        v["reason"] = reason
    return v


def ref_from_records(b, E):
    recs = b.engine.proj[("episode", E)]
    ident = recs["identity"]
    return {"episode_id": E, "podcast_id": ident["podcast_id"], "generation_id": recs["queue_membership"]["generation_id"],
            "occurrence": json.loads(json.dumps(ident["occurrence"]))}


END = {"kind": "end"}
START = {"kind": "start"}


def AFTER(r):
    return {"kind": "after", "after": r}


def upload_with_deps(b, live, obj):
    """Uploads a decision of `live` in recovery, after the decisions of `live` it depends on
    (a resolved add depends on the queue frontier, which holds the layouts) that `b` does
    not hold."""
    made = dict((d["decision_id"], d) for d in live.engine.created)
    need, stack = [], [obj["decision_id"]]
    while stack:
        did = stack.pop()
        if did in need or did in b.engine.ar_dec:
            continue
        need.append(did)
        stack.extend(x for x in made[did]["dependencies"] if x in made)
    for d in live.engine.created:
        if d["decision_id"] in need:
            b.upload_decision(d)


def qstate(b):
    """What a person sees of the queue: the order and each episode's membership."""
    mems = sorted((E, recs["queue_membership"]) for (ent, E), recs in b.engine.proj.items()
                  if ent == "episode" and "queue_membership" in recs)
    return b.queue(), mems


def layouts(b):
    return [d for d in b.engine.created if d["decision_type"] == rules.LAYOUT]


def test_scenario8_remove_vs_offline_move():
    """Scenario 8: A adds X and Y at the end, then removes X; offline B, which saw the add,
    moves X after Y. A move never adds: in both orders X is out and the queue is [Y]."""
    prefix = B.count + 1
    finals = []
    for first in ("remove", "move"):
        b = B("scenario 8: remove vs offline move, %s delivered first" % first, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        x, y = ref(P, G, "x", "X"), ref(P, G, "y", "Y")
        kx, ky = keys_in_order(["s8/x", "s8/y"])
        add = b.qadd("A", [(x, kx), (y, ky)], place=END, op=fixed_op(prefix, 2))
        base = b.now
        moved = K(ky[0], None, "s8/x-move")

        def rm(rt):
            return b.qremove("A", [x], t=base + 1000, rt=rt, op=fixed_op(prefix, 3))

        def mv(rt):
            return b.qmove("B", x, moved, place=AFTER(y), after=[add], t=base + 2000, rt=rt, op=fixed_op(prefix, 4))
        if first == "remove":
            r, m = rm(base + 5000), mv(base + 6000)
        else:
            m, r = mv(base + 5000), rm(base + 6000)
        eq(b.queue(), ["Y"], "the episode is not in the queue")
        eq(MEM(b, x), mem(G, False, "user"), "removed (a move never adds)")
        eq((b.out(add), b.out(r), b.out(m)), (("applied", None),) * 3, "outcomes")
        finals.append(qstate(b))
    eq(finals[0], finals[1], "scenario 8 converges")


def test_scenario9_clear_vs_offline_add():
    """Scenario 9: A clears [X, Y]; offline B adds Z (ordered before the clear, unseen by
    it). The clear removes exactly what it observed: [Z] in both orders."""
    prefix = B.count + 1
    finals = []
    for first in ("clear", "add"):
        b = B("scenario 9: clear vs offline add of another episode, %s delivered first" % first, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        x, y, z = ref(P, G, "x", "X"), ref(P, G, "y", "Y"), ref(P, G, "z", "Z")
        kx, ky = keys_in_order(["s9/x", "s9/y"])
        add = b.qadd("A", [(x, kx), (y, ky)], place=END, op=fixed_op(prefix, 2))
        base = b.now

        def clr(rt):
            return b.qclear("A", [x, y], t=base + 1000, rt=rt, op=fixed_op(prefix, 3))

        def addz(rt):  # ordered before the clear (earlier clock), never observed by it
            return b.qadd("B", [(z, K(ky[0], None, "s9/z"))], place=END, after=[add], t=base + 500, rt=rt,
                          op=fixed_op(prefix, 4))
        if first == "clear":
            cl, az = clr(base + 5000), addz(base + 6000)
        else:
            az, cl = addz(base + 5000), clr(base + 6000)
        check(b.engine.rank[az] < b.engine.rank[cl], "the add is ordered before the clear")
        eq(b.queue(), ["Z"], "B's episode remains")
        eq((MEM(b, x), MEM(b, y)), (mem(G, False, "cleared"), mem(G, False, "cleared")), "exactly the observed")
        eq((b.out(cl), b.out(az)), (("applied", None), ("applied", None)), "outcomes")
        finals.append(qstate(b))
    eq(finals[0], finals[1], "scenario 9 converges")


def test_scenario25_queueless_play():
    b = B("6b scenario 25: a client without the queue capability plays an episode")
    P, G, (x, e) = setup_listening(b, 2)
    b.qadd("A", [(x, K(None, None, "s25/x"))], place=END)
    s = b.uuid()
    st = b.start("W", e, s, 0, duration=D, caps=NOQ)
    eq(b.out(st), ("applied", None), "the start")
    ra = b.decision(rules.RESOLVED_ADD, st)
    check(ra is not None, "the server resolved the add once")
    eq(ra["inputs"], {"trigger": st, "enqueue_location": "back"}, "account default: back")
    eq(ra["result"]["place"], END, "back is the end (PROTOCOL §7.6)")
    eq(b.queue(), ["Episode 1", "Episode 2"], "added at the back")
    eq(MEM(b, e), mem(G, True), "in (explicit)")
    check(b.keyof(e) is not None, "a layout published its key")
    b.cp("W", e, s, 1, 30 * MIN, 30 * MIN, caps=NOQ)
    end = b.end("W", e, s, 2, D, D, completed=True, caps=NOQ)
    eq(MEM(b, e), mem(G, False, "completed"), "the completion removes it")
    eq(b.queue(), ["Episode 1"], "every phone shows it gone")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "played")
    eq(b.out(end), ("applied", None), "end")
    # A start of an episode already queued resolves nothing.
    s2 = b.uuid()
    st2 = b.start("W", x, s2, 0, caps=NOQ)
    check(b.decision(rules.RESOLVED_ADD, st2) is None, "nothing to resolve")


def _ordered_pair(low, high, l1, l2):
    """Two keys in (low, high) for labels derived from l1 and l2, the second one smaller."""
    for i in range(100):
        a, z = K(low, high, "%s/%d" % (l1, i)), K(low, high, "%s/%d" % (l2, i))
        if c.compare_queue_keys(z[0], a[0]) < 0:
            return a, z
    raise AssertionError("no pair")


def test_scenario35_concurrent_sorted_adds():
    """Scenario 35 by replay: [A, Z] sorted by title; offline B adds B at the end and C adds
    C at the end, each sorting at its own place: insert at the end, then sort: A, B, Z, then
    A, B, C, Z, in both delivery orders."""
    prefix = B.count + 1
    finals = []
    for first in ("B", "C"):
        b = B("scenario 35: concurrent sorted adds of B and C between A and Z, %s delivered first" % first, prefix)
        b.server_label = "scenario 35"
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        ea, eb, ec, ez = (ref(P, G, t.lower(), t) for t in ("A", "B", "C", "Z"))
        ka, kz = keys_in_order(["s35/a", "s35/z"])
        add = b.qadd("A", [(ea, ka), (ez, kz)], place=END, op=fixed_op(prefix, 2))
        pol = b.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z", op=fixed_op(prefix, 3))
        ro = b.qsort("A", "episode_title_a_z", op=fixed_op(prefix, 4))  # the dialog's immediate reorder
        kb, kc = _ordered_pair(ka[0], kz[0], "s35/b", "s35/c")
        base = b.now
        ups = {
            "B": lambda rt: b.qadd("B", [(eb, kb)], place=END, after=[ro], t=base + 1000, rt=rt, op=fixed_op(prefix, 5)),
            "C": lambda rt: b.qadd("C", [(ec, kc)], place=END, after=[ro], t=base + 1500, rt=rt, op=fixed_op(prefix, 6)),
        }
        t1 = ups[first](base + 5000)
        t2 = ups["C" if first == "B" else "B"](base + 6000)
        eq(b.queue(), ["A", "B", "C", "Z"], "each add sorts at its own place")
        eq((b.out(add), b.out(pol), b.out(ro), b.out(t1), b.out(t2)), (("applied", None),) * 5, "outcomes")
        finals.append(qstate(b))
    eq(finals[0], finals[1], "scenario 35 converges")
    # The same with a client without the queue capability (its resolved add is sorted at
    # its start's place).
    b = B("6b scenario 35: a queue-less play and a concurrent sorted add")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    ea, eb, ec, ez = (ref(P, G, t.lower(), t) for t in ("A", "B", "C", "Z"))
    ka, kz = keys_in_order(["s35q/a", "s35q/z"])
    b.qadd("A", [(ea, ka), (ez, kz)], place=END)
    b.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z")
    ro = b.qsort("A", "episode_title_a_z")
    base = b.now
    st = b.start("W", eb, b.uuid(), 0, after=[ro], caps=NOQ, t=base + 1000, rt=base + 5000)
    eq(b.queue(), ["A", "B", "Z"], "resolved at the back, then sorted (the start's place)")
    b.qadd("C", [(ec, K(ka[0], kz[0], "s35q/c"))], place=END, after=[ro], t=base + 1500, rt=base + 6000)
    eq(b.queue(), ["A", "B", "C", "Z"], "converged")
    check(b.decision(rules.RESOLVED_ADD, st) is not None, "the start's resolved add")
    # Metadata changes alone never sort; the next add sorts with the values its receipt
    # recorded (PROTOCOL §7.6).
    b = B("6b: metadata changes alone do not reorder")
    P, G, (x, y) = setup_listening(b, 2)
    kx, ky = keys_in_order(["meta/x", "meta/y"])
    b.qadd("A", [(x, kx), (y, ky)], place=END)
    b.start("A", x, b.uuid(), 0, duration=10 * MIN)
    sy = b.uuid()
    b.start("A", y, sy, 0, duration=20 * MIN)
    b.qpolicy("A", keep_sorted=True, sort_order="duration_short_long")
    b.qsort("A", "duration_short_long")
    n = len(b.engine.created)
    b.cp("A", y, sy, 1, MIN, MIN, duration=5 * MIN)  # y is now the shorter one
    eq(len(b.engine.created), n, "no decision")
    eq(b.queue(), ["Episode 1", "Episode 2"], "not re-sorted")
    z = ref(P, G, "ep-3", "Episode 3")
    t = b.qadd("A", [(z, K(ky[0], None, "meta/z"))], place=END)
    eq(b.queue(), ["Episode 3", "Episode 2", "Episode 1"], "the next add sorts with the current values (0, 5, 10 min)")
    eq(sorted(m["values"]["duration"] for m in b.engine.recorded[t]), [0, 5 * MIN, 10 * MIN],
       "its receipt recorded every member's values (measured durations, 0 when missing)")


def test_automatic_membership_hook():
    b = B("6b: automatic queue adds vs explicit membership (7b discovery filings)")
    P, G, eps = setup_listening(b, 6)
    s = b.arrivals[0]["fact"]["op_id"]
    e1, e2, e3, e4, e5, e6 = eps
    devs = iter("FGHIJKLMN")

    def auto(r, label, t=None, after=(s,)):
        # A queue filing at the proposal's place (PROTOCOL §9.4): each from its own device, so that
        # it depends only on the subscription, like the former test-only node.
        kp = K(None, None, label)
        nid = b.discover(next(devs), r, action="queue", bundle=(kp, END), t=t, after=after)
        eq(b.out(nid), ("applied", None), "the proposal is admitted")
        st = b.engine._fold_episode_scope(r["episode_id"], G).status.get(nid)
        reason = st.split(":", 1)[1] if st is not None and st.startswith("ineffective:") else None
        return nid, reason, kp
    _, r1, k1 = auto(e1, "auto/1")
    eq((r1, MEM(b, e1)), (None, mem(G, True)), "an automatic add alone applies")
    _, r2, _ = auto(e2, "auto/2")
    b.qremove("B", [e2])
    eq((r2, MEM(b, e2)), (None, mem(G, False, "user")), "an explicit removal after it wins")
    rm = b.qremove("B", [e3])
    eq(MEM(b, e3), mem(G, False, "user"), "an explicit decision for an episode never queued")
    _, r3, _ = auto(e3, "auto/3", t=b.nt(rm) - 500)
    eq((r3, MEM(b, e3)), ("explicit_state", mem(G, False, "user")),
       "an automatic add ordered before an explicit removal still loses (whatever the order)")
    k4 = K(None, None, "explicit/4")
    b.qadd("B", [(e4, k4)], place=END)
    _, r4, _ = auto(e4, "auto/4")
    eq((r4, MEM(b, e4)), ("explicit_state", mem(G, True)), "nor does it move an explicit entry")
    eq(b.queue(), ["Episode 1", "Episode 4"], "the suppressed filings left ghosts only")
    sx = b.uuid()
    b.start("B", e5, sx, 0)
    b.end("B", e5, sx, 1, D, D, completed=True)
    _, r5, _ = auto(e5, "auto/5")
    eq((r5, MEM(b, e5)), ("explicit_state", mem(G, False, "completed")), "a completion is an explicit decision")
    # A filing is a sort trigger under keep-sorted (scenario 35's discovery variant).
    b.qpolicy("B", keep_sorted=True, sort_order="episode_title_z_a")
    nid, r6, _ = auto(e6, "auto/6")
    check(r6 is None, "the filing applies")
    eq(b.queue(), ["Episode 6", "Episode 4", "Episode 1"], "z-a")


def test_undo_under_keep_sorted():
    """Undo of a removal is a restore at the saved index resolved at Undo time, and never
    sorts (Android's addQueueItemAt; PROTOCOL §7.6)."""
    b = B("6b: Undo of a removal under keep-sorted restores at the saved index and never sorts")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    al, be, ga, de = (ref(P, G, t.lower(), t) for t in ("Alpha", "Beta", "Gamma", "Delta"))
    ka, kb, kg = keys_in_order(["undo/a", "undo/b", "undo/g"])
    b.qadd("A", [(al, ka), (be, kb), (ga, kg)], place=END)
    b.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z")
    b.qsort("A", "episode_title_a_z")
    rm = b.qremove("A", [be])  # Android saves Beta's index: 1
    b.qpolicy("A", keep_sorted=True, sort_order="episode_title_z_a")
    b.qsort("A", "episode_title_z_a")
    eq(b.queue(), ["Gamma", "Alpha"], "z-a")
    u = b.qrestore("A", [(be, kb)], place=AFTER(ga))  # index 1 at Undo time: after Gamma
    eq(b.queue(), ["Gamma", "Beta", "Alpha"], "Beta back at index 1, unsorted")
    eq(MEM(b, be), mem(G, True), "explicit")
    eq((b.out(rm), b.out(u)), (("applied", None), ("applied", None)), "outcomes")
    t = b.qadd("A", [(de, K(None, None, "undo/d"))], place=END)
    eq(b.queue(), ["Gamma", "Delta", "Beta", "Alpha"], "the next add sorts everything")
    eq(b.out(t), ("applied", None), "add")


def test_moves_of_out_entries():
    b = B("moves of entries that are out of the queue")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    x, y, w = ref(P, G, "x", "X"), ref(P, G, "y", "Y"), ref(P, G, "w", "W")
    kx, ky = keys_in_order(["out/x", "out/y"])
    add = b.qadd("A", [(x, kx), (y, ky)], place=END)
    b.qremove("A", [x])
    m = b.qmove("B", x, K(ky[0], None, "out/x-move"), place=AFTER(y), after=[add])
    eq((MEM(b, x), b.queue()), (mem(G, False, "user"), ["Y"]), "the entry moves but stays out")
    eq(b.out(m), ("applied", None), "applied")
    mw = b.qmove("B", w, K(None, None, "out/w"), place=START)
    check(MEM(b, w) is None and b.keyof(w) is None, "a move of an episode never queued writes no record")
    eq(b.out(mw), ("applied", None), "applied")
    b.qadd("C", [(x, K(None, ky[0], "out/x-add"))], place=START)
    eq(b.queue(), ["X", "Y"], "a later add places it anew")
    eq(b.rec("episode", x["episode_id"], "play_state"), {"generation_id": G, "play_state": "unplayed"},
       "an add touches play state")


def test_completion_and_skip_removals():
    # A completion bundle suppressed by a takeover ordered before it keeps the episode
    # queued, in both delivery orders (the whole bundle is refolded).
    prefix = B.count + 1
    finals = []
    for first in ("finalization", "takeover"):
        b = B("completion removal vs takeover, %s delivered first" % first, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e = ref(P, G, "ep-1", "Episode 1")
        b.qadd("A", [(e, K(None, None, "cmp/e"))], place=END, op=fixed_op(prefix, 2))
        sa, sb = fixed_op(prefix, 100), fixed_op(prefix, 101)
        a1 = b.start("A", e, sa, 50 * MIN, duration=D, op=fixed_op(prefix, 3))
        a2 = b.cp("A", e, sa, 1, D - 10000, D - 10000 - 50 * MIN, op=fixed_op(prefix, 4))
        base = b.now

        def fin(rt):
            return b.fin("A", e, sa, 1, a1, t=base + 2000, rt=rt, op=fixed_op(prefix, 5))

        def take(rt):
            return b.start("B", e, sb, D - 10000, observed=a1, after=[a2], t=base + 1000, rt=rt, op=fixed_op(prefix, 6))
        if first == "finalization":
            fin(base + 5000)
            eq(MEM(b, e), mem(G, False, "completed"), "the bundle removes it at first")
            take(base + 6000)
        else:
            take(base + 5000)
            fin(base + 6000)
        eq(MEM(b, e), mem(G, True), "the suppressed bundle leaves it queued")
        eq(b.queue(), ["Episode 1"], "and in the queue's order")
        finals.append(qstate(b))
    eq(finals[0], finals[1], "both orders converge")
    b = B("6b: skip removal, keep skipped, mark played, re-add after completion")
    P, G, (e1, e2, e3, e4) = setup_listening(b, 4)
    keys = keys_in_order(["skip/1", "skip/2", "skip/3", "skip/4"])
    b.qadd("A", list(zip([e1, e2, e3, e4], keys)), place=END)
    s1 = b.uuid()
    b.start("A", e1, s1, 0)
    b.end("A", e1, s1, 1, 5 * MIN, 5 * MIN, reason="skipped", rfq=True)
    eq(MEM(b, e1), mem(G, False, "skipped"), "a skip with Keep skipped episodes off")
    s2 = b.uuid()
    b.start("A", e2, s2, 0)
    b.end("A", e2, s2, 1, 5 * MIN, 5 * MIN, reason="skipped", rfq=False)
    eq(MEM(b, e2), mem(G, True), "Keep skipped episodes on: stays")
    b.mark_played("A", e3)
    eq(MEM(b, e3), mem(G, True), "manual mark played never removes")
    s4 = b.uuid()
    b.start("A", e4, s4, 0)
    b.cp("A", e4, s4, 1, 5 * MIN, 5 * MIN)
    f = b.fin("A", e4, s4, 1, b.arrivals[-2]["fact"]["op_id"], completed=False, rfq=True, history=False)
    eq((MEM(b, e4), b.out(f)), (mem(G, False, "skipped"), ("applied", None)), "a finalization's skip removal")
    b.qadd("B", [(e4, K(None, None, "skip/readd"))], place=END)
    eq(MEM(b, e4), mem(G, True), "a later add re-adds")
    s5 = b.uuid()
    b.start("A", e2, s5, 0)
    b.end("A", e2, s5, 1, D, D, completed=True, rfq=True)
    eq(MEM(b, e2), mem(G, False, "completed"), "a completion's reason is completed")
    eq(b.queue(), ["Episode 3", "Episode 4"], "queue: Episode 4 re-added at the end")


def test_sort_results_ties_and_missing():
    # Queue-less plays under keep-sorted: the server resolves the add and the replay sorts
    # the in-members at the start's place with the values the server recorded, missing
    # values as Android's defaults, ties stable.
    def build(name, order, items, play, location="back"):
        b = B("6b keep-sorted with ties and missing values: " + name)
        s = b.subscribe("A", U1)
        P, G = pg(s)
        refs = [qref(P, G, "g%d" % i, t, pub) for i, (t, pub, _) in enumerate(items)]
        keys = keys_in_order(["ties/%s/%d" % (name, i) for i in range(len(items))])
        b.qadd("A", list(zip(refs, keys)), place=END)
        for r, (_, _, dur) in zip(refs, items):
            if dur is not None:
                b.start("A", r, b.uuid(), 0, duration=dur)
        if location != "back":
            b.qpolicy("A", enqueue_location=location)
        b.qpolicy("A", keep_sorted=True, sort_order=order)
        b.qsort("A", order)
        title, pub, dur = play
        p = qref(P, G, "played", title, pub)
        st = b.start("W", p, b.uuid(), 0, caps=NOQ, duration=dur)
        return b, st
    b, st = build("date_new_old", "date_new_old",
                  [("Tie one", 5000, None), ("No date", None, None), ("Tie two", 5000, None), ("Newer", 9000, None)],
                  ("Played tie", 5000, None))
    eq(b.queue(), ["Newer", "Tie one", "Tie two", "Played tie", "No date"], "ties keep the queue order; missing is 0")
    vals = b.decision(rules.RESOLVED_ADD, st)["result"]["values"]
    eq(sorted(v["values"]["publication_time"] for v in vals), [0, 5000, 5000, 5000, 9000],
       "the resolved add recorded every member's values, defaults filled in")
    b, st = build("titles", "episode_title_a_z",
                  [("Zoo", None, None), (None, None, None), ("émile", None, None), ("Apple", None, None)],
                  ("ÉMILE", None, None), location="front")
    untitled = qref(pg(b.arrivals[0]["fact"]["op_id"])[0], pg(b.arrivals[0]["fact"]["op_id"])[1], "g1")["episode_id"][:12]
    eq(b.queue(), [untitled, "Apple", "Zoo", "ÉMILE", "émile"],
       "missing title first; simple lowercase then code points (é after z); the front-resolved tie stays first")
    b, st = build("durations", "duration_long_short",
                  [("Short", None, 10 * MIN), ("Unknown", None, None), ("Long", None, 50 * MIN), ("Tie", None, 10 * MIN)],
                  ("Played", None, 10 * MIN))
    eq(b.queue(), ["Long", "Short", "Tie", "Played", "Unknown"], "long to short, ties stable, missing duration 0 last")
    b, st = build("smart shuffle", "smart_shuffle_new_old",
                  [("One a", 1000, None), ("One b", 3000, None), ("One c", 2000, None)],
                  ("One d", 1500, None))
    eq(b.queue(), ["One b", "One c", "One d", "One a"], "one podcast: by date, new to old")


def _offline_add_setup(b, prefix):
    """Keep-sorted (title z-a) queue Zulu, Alpha; an offline device adds Bravo at the end
    (its place sorts: Zulu, Bravo, Alpha), disables keep-sorted and moves Alpha to the
    front."""
    s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    zu, al, br = ref(P, G, "zulu", "Zulu"), ref(P, G, "alpha", "Alpha"), ref(P, G, "bravo", "Bravo")
    kz, ka = keys_in_order(["r1/z", "r1/a"])
    b.qadd("A", [(zu, kz), (al, ka)], place=END, op=fixed_op(prefix, 2))
    b.qpolicy("A", keep_sorted=True, sort_order="episode_title_z_a", op=fixed_op(prefix, 3))
    ro = b.qsort("A", "episode_title_z_a", op=fixed_op(prefix, 4))
    base = b.now
    plan = {
        "add": lambda rt, rec: b.qadd("B", [(br, K(kz[0], ka[0], "r1/b"))], place=END, after=[ro],
                                      t=base + 1000, rt=rt, op=fixed_op(prefix, 5), recovery=rec),
        "disable": lambda rt, rec: b.qpolicy("B", keep_sorted=False, sort_order="episode_title_z_a", t=base + 2000,
                                             rt=rt, op=fixed_op(prefix, 6), recovery=rec),
        "move": lambda rt, rec: b.qmove("B", al, K(None, kz[0], "r1/a-front"), place=START, t=base + 3000, rt=rt,
                                        op=fixed_op(prefix, 7), recovery=rec),
    }
    return base, plan, (zu, al, br)


def test_offline_add_disable_move():
    """The offline add, disable and move example under the replay (PROTOCOL §7.6): the add sorts at its place, the disable and the move replay after it: [Alpha, Zulu, Bravo]
    live and in two recovery upload orders."""
    prefix = B.count + 1
    live = B("offline add, disable keep-sorted, move (live)", prefix)
    base, plan, (zu, al, br) = _offline_add_setup(live, prefix)
    t = plan["add"](base + 100000, False)
    plan["disable"](base + 100001, False)
    m = plan["move"](base + 100002, False)
    eq(live.queue(), ["Alpha", "Zulu", "Bravo"], "the add sorted at its place; the move replays after it")
    eq((live.out(t), live.out(m)), (("applied", None), ("applied", None)), "outcomes")
    want = qstate(live)
    vals = live.engine.recorded[t]
    for name, order in (("move, disable, add", ["move", "disable", "add"]),
                        ("disable, add, move", ["disable", "add", "move"])):
        b = B("offline add, disable keep-sorted, move; recovery upload order: %s" % name, prefix)
        base, plan, _ = _offline_add_setup(b, prefix)
        n = len(b.engine.created)
        for i, step in enumerate(order):
            plan[step](base + 100000 + i, True)
        b.engine._ensure()
        eq(b.queue(), ["Alpha", "Zulu", "Bravo"], "recovery: %s" % name)
        eq(b.engine.created[n:], [], "recovered facts decide nothing (the publication runs at the request's end)")
        check(vals, "the live add recorded values")
        eq(qstate(b), want, "the same queue in recovery order (%s)" % name)


def test_delayed_older_move_vs_sort():
    """A delayed older move (made before a later keep-sorted add, unseen by it) is replayed
    at its own place, before the add's sort: the sorted order stands, whichever arrives
    first (the move is applied: its step happened)."""
    prefix = B.count + 1
    finals = []
    for first in ("move", "add"):
        b = B("6b: a delayed older move before a sorting add, %s delivered first" % first, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        al, be, ga = (ref(P, G, t.lower(), t) for t in ("Alpha", "Beta", "Gamma"))
        ka, kb = keys_in_order(["old/a", "old/b"])
        add = b.qadd("A", [(al, ka), (be, kb)], place=END, op=fixed_op(prefix, 2))
        b.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z", op=fixed_op(prefix, 3))
        ro = b.qsort("A", "episode_title_a_z", op=fixed_op(prefix, 4))
        base = b.now

        def move(rt):
            return b.qmove("E", be, K(None, ka[0], "old/b-front"), place=START, after=[add], t=base + 300, rt=rt,
                           op=fixed_op(prefix, 5))

        def gamma(rt):
            return b.qadd("F", [(ga, K(kb[0], None, "old/g"))], place=END, after=[ro], t=base + 1000, rt=rt,
                          op=fixed_op(prefix, 6))
        if first == "move":
            m, g = move(base + 5000), gamma(base + 6000)
        else:
            g, m = gamma(base + 5000), move(base + 6000)
        check(b.engine.rank[m] < b.engine.rank[g], "the move is ordered before the add")
        eq(b.queue(), ["Alpha", "Beta", "Gamma"], "the add's sort comes after the move")
        eq((b.out(m), b.out(g)), (("applied", None), ("applied", None)), "both applied")
        finals.append(qstate(b))
    eq(finals[0], finals[1], "converges")


def test_add_under_a_concurrent_disable():
    """An add's complete sorted result does not survive a concurrent disable of keep-sorted
    (PROTOCOL §7.6): the policy at the add's place in the order decides. [Alpha, Gamma] sorted by title; E moves Gamma to the front (time +300);
    C disables keep-sorted (+500); B, which still saw keep-sorted on, adds Beta at the end
    (+1000). By hand: [Gamma, Alpha], then Beta at the end without sorting: [Gamma, Alpha,
    Beta], in every delivery order."""
    prefix = B.count + 1
    finals = []
    for order in (("move", "disable", "add"), ("add", "move", "disable"), ("disable", "add", "move")):
        b = B("6b: an add under a concurrent disable of keep-sorted; delivered %s" % ", ".join(order), prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        al, be, ga = (ref(P, G, t.lower(), t) for t in ("Alpha", "Beta", "Gamma"))
        ka, kg = keys_in_order(["dis/a", "dis/g"])
        add0 = b.qadd("A", [(al, ka), (ga, kg)], place=END, op=fixed_op(prefix, 2))
        b.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z", op=fixed_op(prefix, 3))
        ro = b.qsort("A", "episode_title_a_z", op=fixed_op(prefix, 4))
        base = b.now
        ups = {
            "disable": lambda rt: b.qpolicy("C", keep_sorted=False, sort_order="episode_title_a_z", after=[ro],
                                            t=base + 500, rt=rt, op=fixed_op(prefix, 5)),
            "add": lambda rt: b.qadd("B", [(be, K(ka[0], kg[0], "dis/b"))], place=END, after=[ro], t=base + 1000,
                                     rt=rt, op=fixed_op(prefix, 6)),
            "move": lambda rt: b.qmove("E", ga, K(None, ka[0], "dis/g-front"), place=START, after=[add0], t=base + 300,
                                       rt=rt, op=fixed_op(prefix, 7)),
        }
        for i, k in enumerate(order):
            ups[k](base + 5000 + 1000 * i)
        eq(b.queue(), ["Gamma", "Alpha", "Beta"], "the policy at the add's place decides")
        finals.append(qstate(b))
    for f in finals[1:]:
        eq(f, finals[0], "converges")


def test_concurrent_policy_writes():
    prefix = B.count + 1
    for later in ("on", "off"):
        finals = []
        for first in ("A", "B"):
            b = B("concurrent policy writes (sorting is one register), %s later, %s delivered first"
                  % (later, first), prefix)
            s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
            base = b.now
            ta, tb = (base + 2000, base + 1000) if later == "on" else (base + 1000, base + 2000)
            ups = {
                "A": lambda rt: b.qpolicy("A", keep_sorted=True, sort_order="duration_short_long", after=[s], t=ta,
                                          rt=rt, op=fixed_op(prefix, 2)),
                "B": lambda rt: b.qpolicy("B", keep_sorted=False, sort_order="random", after=[s], t=tb, rt=rt,
                                          op=fixed_op(prefix, 3)),
            }
            ups[first](base + 5000)
            ups["B" if first == "A" else "A"](base + 6000)
            loc = b.qpolicy("C", enqueue_location="front", after=[s], t=base + 1500, rt=base + 7000,
                            op=fixed_op(prefix, 4))
            want = ({"enqueue_location": "front", "keep_sorted": True, "sort_order": "duration_short_long"}
                    if later == "on" else {"enqueue_location": "front", "keep_sorted": False, "sort_order": "random"})
            eq(b.rec("account", ACC, "queue_policy"), want, "the later sorting write wins whole; location independent")
            winner, loser = ("A", "B") if later == "on" else ("B", "A")
            # The later write is applied; the earlier one is applied if it arrived first
            # (it determined the register then), superseded if it arrived after the later one.
            wop, lop = fixed_op(prefix, 2 if winner == "A" else 3), fixed_op(prefix, 2 if loser == "A" else 3)
            eq(b.out(wop), ("applied", None), "the later write")
            eq(b.out(lop), ("applied", None) if first == loser else ("superseded", None), "the earlier write")
            eq(b.out(loc), ("applied", None), "location")
            finals.append(b.engine.records())
        eq(finals[0], finals[1], "converges (%s later)" % later)
    b = B("queue policy defaults and partial writes")
    b.qpolicy("A", enqueue_location="after_current")
    eq(b.rec("account", ACC, "queue_policy"),
       {"enqueue_location": "after_current", "keep_sorted": False, "sort_order": "date_new_old"}, "Android's defaults")


def test_queueless_play_locations():
    for loc, draw, want, place in (("back", None, ["X", "Y", "Z", "New"], END), ("front", None, ["New", "X", "Y", "Z"], START),
                                   ("after_current", None, ["X", "Y", "Z", "New"], END),
                                   ("random", 2, ["X", "Y", "New", "Z"], "Y"), ("random", 0, ["New", "X", "Y", "Z"], START),
                                   ("random", 3, ["X", "Y", "Z", "New"], "Z")):
        b = B("6b: queue-less play, enqueue location %s%s" % (loc, "" if draw is None else " (draw %d)" % draw))
        s = b.subscribe("A", U1)
        P, G = pg(s)
        xs = [ref(P, G, t.lower(), t) for t in ("X", "Y", "Z")]
        b.qadd("A", list(zip(xs, keys_in_order(["loc/%s/%d/%d" % (loc, draw or 0, i) for i in range(3)]))), place=END)
        b.qpolicy("A", enqueue_location=loc)
        if draw is not None:
            b.draws = [draw]
        st = b.start("W", ref(P, G, "new", "New"), b.uuid(), 0, caps=NOQ)
        eq(b.queue(), want, "resolved at %s" % loc)
        ra = b.decision(rules.RESOLVED_ADD, st)
        eq(ra["inputs"]["enqueue_location"], loc, "inputs")
        if isinstance(place, str):
            r = xs[("X", "Y", "Z").index(place)]
            place = {"kind": "after", "after": {"episode_id": r["episode_id"], "generation_id": G}}
        eq(ra["result"]["place"], place, "the resolved place (random's tail is after the last, never the end)")
        if draw is not None:
            eq(b.arrivals[-1]["decisions"][0]["draw"], draw, "the draw is the vector's")
    b = B("6b: queue-less play under keep-sorted, front")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    xs = [ref(P, G, t.lower(), t) for t in ("A", "C")]
    kk = keys_in_order(["locks/a", "locks/c"])
    b.qadd("A", list(zip(xs, kk)), place=END)
    b.qpolicy("A", enqueue_location="front", keep_sorted=True, sort_order="episode_title_a_z")
    b.qsort("A", "episode_title_a_z")
    st = b.start("W", ref(P, G, "b", "B"), b.uuid(), 0, caps=NOQ)
    ra = b.decision(rules.RESOLVED_ADD, st)
    eq(ra["result"]["place"], START, "resolved at the front")
    eq(b.queue(), ["A", "B", "C"], "then sorted at the start's place")
    lay = layouts(b)[-1]
    check(ra["decision_id"] in lay["dependencies"], "the layout depends on the resolved add (the account's frontier)")
    eq(MEM(b, ref(P, G, "b", "B")), mem(G, True), "in")


def _queueless_completion_setup(b, prefix):
    s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    x, e, y = ref(P, G, "x", "X"), ref(P, G, "e", "E"), ref(P, G, "y", "Y")
    kx = K(None, None, "qc/x")
    add = b.qadd("A", [(x, kx)], place=END, op=fixed_op(prefix, 2))
    base = b.now
    sid = fixed_op(prefix, 100)
    plan = {
        "start": lambda rt, rec: b.start("W", e, sid, 0, duration=D, after=[add], caps=NOQ, t=base + 1000, rt=rt,
                                         op=fixed_op(prefix, 3), recovery=rec),
        "cp": lambda rt, rec: b.cp("W", e, sid, 1, 30 * MIN, 30 * MIN, caps=NOQ, t=base + 2000, rt=rt,
                                   op=fixed_op(prefix, 4), recovery=rec),
        "end": lambda rt, rec: b.end("W", e, sid, 2, D, D, completed=True, caps=NOQ, t=base + 3000, rt=rt,
                                     op=fixed_op(prefix, 5), recovery=rec),
        # Another device adds Y meanwhile (intervening writes are preserved).
        "other": lambda rt, rec: b.qadd("A", [(y, K(kx[0], None, "qc/y"))], place=AFTER(x), t=base + 1500, rt=rt,
                                        op=fixed_op(prefix, 6), recovery=rec),
    }
    return base, plan, (x, e, y, G)


def test_queueless_play_then_completion_offline():
    prefix = B.count + 1
    live = B("queue-less play then completion while offline (live: one push)", prefix)
    live.server_label = "queue-less completion"
    base, plan, (x, e, y, G) = _queueless_completion_setup(live, prefix)
    st = plan["start"](base + 50000, False)
    plan["cp"](base + 50001, False)
    plan["end"](base + 50002, False)
    plan["other"](base + 50003, False)
    ra = live.decision(rules.RESOLVED_ADD, st)
    check(live.engine.rank[ra["decision_id"]] > live.engine.rank[fixed_op(prefix, 5)],
          "the resolved add decision is ranked after the completion")
    eq(MEM(live, e), mem(G, False, "completed"), "but it acts at its start: the episode ends out")
    eq(live.queue(), ["X", "Y"], "queue")
    want = qstate(live)
    facts = ["start", "cp", "end", "other"]
    orders = []
    for i in range(len(facts)):  # the decision uploaded at every causal place after its start
        orders.append(facts[:i + 1] + ["R"] + facts[i + 1:])
    orders.append(["other", "start", "cp", "R", "end"])
    for k, order in enumerate(orders):
        b = B("queue-less play then completion while offline, recovery order %d: %s" % (k + 1, " ".join(order)),
              prefix)
        b.server_label = "queue-less completion"  # the same server: the setup's layouts are the live run's
        base, plan, _ = _queueless_completion_setup(b, prefix)
        b.server_label = "queue-less completion/recovered"  # its own later choices
        for i, step in enumerate(order):
            if step == "R":
                b.upload_decision(ra)
            else:
                plan[step](base + 50000 + i, True)
        rend(b)
        eq(MEM(b, e), mem(G, False, "completed"), "out in recovery order %d" % (k + 1))
        eq(qstate(b), want, "recovery order %d converges" % (k + 1))


def test_competing_decisions():
    prefix = B.count + 1

    def setup(b):
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        al, be, ga = (ref(P, G, t.lower(), t) for t in ("Alpha", "Beta", "Gamma"))
        ka, kg = keys_in_order(["comp/a", "comp/g"])
        add = b.qadd("A", [(al, ka), (ga, kg)], place=END, op=fixed_op(prefix, 2))
        b.qpolicy("A", enqueue_location="random", op=fixed_op(prefix, 3))
        b.qmove("F", al, K(kg[0], None, "comp/a2"), place=AFTER(ga), after=[add], op=fixed_op(prefix, 5))
        st = b.start("W", be, fixed_op(prefix, 100), 0, caps=NOQ, op=fixed_op(prefix, 6))
        return st, (al, be, ga)
    orig = B("6b: competing decisions: the original server's resolved add (epoch 1, draw 0)", prefix)
    orig.draws = [0]
    st, (al, be, ga) = setup(orig)
    r1 = orig.decision(rules.RESOLVED_ADD, st)
    eq(orig.queue(), ["Beta", "Gamma", "Alpha"], "drawn at the front")
    b = B("competing decisions: a restored server decides again (epoch 2, draw 2), then the original is recovered",
          prefix)
    b.epoch, b.server_label = 2, "restored"
    b.draws = [2]
    st2, _ = setup(b)
    r2 = b.decision(rules.RESOLVED_ADD, st2)
    eq(b.queue(), ["Gamma", "Alpha", "Beta"], "drawn at the tail")
    check(r2["decision_id"] != r1["decision_id"], "different choices")
    upload_with_deps(b, orig, r1)
    rend(b)
    eq(b.decision(rules.RESOLVED_ADD, st)["decision_id"], r1["decision_id"], "the smallest rank (epoch 1) is effective")
    eq(b.queue(), ["Beta", "Gamma", "Alpha"], "the recovered decision decides; the competitor is evidence only")
    check(r2["decision_id"] in b.engine.decisions, "kept as evidence")


def test_payload_rejections_and_capabilities():
    b = B("queue payload rejections and capabilities (PROTOCOL §7.6)")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    x, y = ref(P, G, "x", "X"), ref(P, G, "y", "Y")
    kx, ky = keys_in_order(["rej/x", "rej/y"])
    pl = lambda r, kp: {"episode": r, "key": kp[0], "position_id": kp[1]}  # noqa: E731
    et = lambda *rs: [{"entity": "episode", "id": r["episode_id"], "generation": G} for r in  # noqa: E731
                      sorted(rs, key=lambda r: r["episode_id"])]
    other_pid = det_uuid("rej/other")
    n, m = c.parse_queue_key(kx[0])
    acct = [{"entity": "account", "id": ACC}]
    st = {"episode": x, "session_id": b.uuid(), "start_position": 0, "activity": True}
    bad = [
        ("queue.add", {"members": [], "place": END}, []),
        ("queue.add", {"members": [pl(x, kx)]}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "moves": [], "place": END}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "moves": []}, et(x)),
        ("queue.add", {"members": [pl(x, kx), pl(y, ky)], "place": END}, et(x)),
        ("queue.add", {"members": [dict(pl(x, kx), position_id=other_pid)], "place": END}, et(x)),
        ("queue.add", {"members": [dict(pl(x, kx), key="q1:%d:%x" % (n + 1, 2 * m))], "place": END}, et(x)),
        ("queue.add", {"members": [dict(pl(x, kx), key="q1:0:1")], "place": END}, et(x)),
        ("queue.add", {"members": [dict(pl(x, kx), key="q1:1:1")], "place": END}, et(x)),
        ("queue.add", {"members": [dict(pl(x, kx), key="q1:%d:%X" % (n, m))], "place": END}, et(x)),
        ("queue.add", {"members": [dict(pl(x, kx), position_id=other_pid.upper())], "place": END}, et(x)),
        ("queue.add", {"members": [dict(pl(x, kx), extra=1)], "place": END}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "place": {"kind": "middle"}}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "place": {"kind": "after"}}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "place": {"kind": "after", "after": {"episode_id": y["episode_id"]}}}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "place": {"kind": "end", "after": y}}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "place": "end"}, et(x)),
        ("queue.add", {"members": [pl(x, kx)], "place": AFTER(y)}, et(x, y)),
        ("queue.restore", {"members": [], "place": END}, []),
        ("queue.restore", {"members": [pl(x, kx)]}, et(x)),
        ("queue.remove", {"episodes": [x], "reason": "completed"}, et(x)),
        ("queue.remove", {"episodes": [x], "reason": "cleared"}, et(x)),
        ("queue.remove", {"episodes": [], "reason": "user"}, []),
        ("queue.remove", {"episodes": sorted([x, y], key=lambda r: r["episode_id"], reverse=True), "reason": "user"},
         et(x, y)),
        ("queue.move", {"episode": x, "key": kx[0], "position_id": kx[1]}, et(x)),
        ("queue.move", {"episode": x, "key": kx[0], "place": END}, et(x)),
        ("queue.move", {"episode": x, "key": kx[0], "position_id": kx[1], "place": END}, []),
        ("queue.reorder", {"members": [pl(x, kx)]}, et(x)),
        ("queue.reorder", {"run": [], "place": START}, []),
        ("queue.reorder", {"run": [x, x], "place": START}, et(x)),
        ("queue.reorder", {"run": [x]}, et(x)),
        ("queue.reorder", {"run": [x], "place": START}, []),
        ("queue.reorder", {"run": [x], "place": START, "sort": {"sort_order": "date_new_old"}, "observed": []}, et(x)),
        # A run move's optional sparse key predictions: non-empty, one per episode, each naming one
        # of the run's references.
        ("queue.reorder", {"run": [x], "place": START, "members": []}, et(x)),
        ("queue.reorder", {"run": [x], "place": START, "members": [pl(y, ky)]}, et(x)),
        ("queue.reorder", {"run": [x, y], "place": START, "members": [pl(x, kx), pl(x, kx)]}, et(x, y)),
        ("queue.reorder", {"run": [x], "place": START, "members": [dict(pl(x, kx), position_id=other_pid)]}, et(x)),
        ("queue.reorder", {"run": [x], "place": START, "members": [pl(dict(x, occurrence=dict(x["occurrence"],
                                                                                           title="Other")), kx)]},
         et(x)),
        ("queue.reorder", {"run": [x], "place": START, "members": {"episode": x}}, et(x)),
        ("queue.reorder", {"sort": {"sort_order": "size_small_large"}, "observed": []}, acct),
        ("queue.reorder", {"sort": {"sort_order": "random"}, "observed": [x]}, acct),
        ("queue.reorder", {"sort": {"sort_order": "random", "permutation": [y]}, "observed": [x]}, acct),
        ("queue.reorder", {"sort": {"sort_order": "random", "permutation": [x, x]}, "observed": [x]}, acct),
        ("queue.reorder", {"sort": {"sort_order": "date_new_old", "permutation": [x]}, "observed": [x]}, acct),
        ("queue.reorder", {"sort": {"sort_order": "date_new_old"}, "observed": [x, x]}, acct),
        ("queue.reorder", {"sort": {"sort_order": "date_new_old"}, "observed": [x]}, et(x)),
        ("queue.reorder", {"sort": {"sort_order": "date_new_old"}, "observed": [], "members": []}, acct),
        ("queue.reorder", {"sort": {"sort_order": "date_new_old"}}, acct),
        ("queue.clear", {"observed": []}, []),
        ("queue.clear", {}, []),
        ("account.set_queue_policy", {}, acct),
        ("account.set_queue_policy", {"sorting": {"keep_sorted": True, "sort_order": "random"}}, acct),
        ("account.set_queue_policy", {"sorting": {"keep_sorted": True}}, acct),
        ("account.set_queue_policy", {"sorting": {"keep_sorted": False, "sort_order": "size_small_large"}}, acct),
        ("account.set_queue_policy", {"enqueue_location": "end"}, acct),
        ("account.set_queue_policy", {"enqueue_location": "back"}, []),
        ("session.start", dict(st, queue_bundle={"key": kx[0], "position_id": kx[1]}), et(x)),
        ("session.start", dict(st, queue_bundle={"key": kx[0], "position_id": kx[1], "moves": []}), et(x)),
        ("session.start", dict(st, queue_bundle={"key": kx[0], "position_id": kx[1], "place": END, "moves": []}),
         et(x)),
        ("session.start", dict(st, queue_bundle={"key": kx[0], "position_id": other_pid, "place": END}), et(x)),
        ("session.start", dict(st, queue_bundle={"key": kx[0], "position_id": kx[1], "place": {"kind": "front"}}),
         et(x)),
        ("session.start", dict(st, queue_bundle={"key": kx[0], "position_id": kx[1], "place": AFTER(y)}), et(x, y)),
    ]
    for typ, payload, targets in bad:
        r = b.push("A", typ, payload, targets, reject=True)
        eq(b.out(r), ("rejected", "invalid_payload"), "%s %s" % (typ, json.dumps(payload)[:120]))
    caps_bad = [
        ("queue.add", {"members": [pl(x, kx)], "place": END}, et(x)),
        ("queue.move", dict(pl(x, kx), place=END), et(x)),
        ("queue.reorder", {"sort": {"sort_order": "date_new_old"}, "observed": []}, acct),
        ("account.set_queue_policy", {"enqueue_location": "front"}, acct),
        ("session.start", dict(st, session_id=b.uuid(),
                               queue_bundle={"key": kx[0], "position_id": kx[1], "place": END}), et(x)),
    ]
    for typ, payload, targets in caps_bad:
        r = b.push("W", typ, payload, targets, caps=NOQ, reject=True)
        eq(b.out(r), ("rejected", "capability_not_declared"), "%s without the queue capability" % typ)
    # Anchors are context, not targets: an add after an episode never placed is valid; the
    # replay places it at the end and logs the client fault (PROTOCOL §7.6).
    ok = b.qadd("A", [(x, kx)], place=AFTER(y))
    eq(b.out(ok), ("applied", None), "an anchor with no entry never fails the command")
    eq(b.queue(), ["X"], "placed")
    check(("anchor_without_entry", (y["episode_id"], G)) in b.engine.diagnostics, "logged")
    # A sort command is sent even when it changes nothing; an empty observation is valid.
    srt = b.qsort("A", "date_new_old", observed=[])
    eq(b.out(srt), ("applied", None), "an empty sort")
    eq(b.queue(), ["X"], "nothing to sort")
    # A run move with a sparse key prediction (PROTOCOL §7.6: the device's prediction stays only as
    # that): `run` moves, the prediction only feeds key retention (it fits: kept).
    ay = b.qadd("A", [(y, ky)], place=END)
    eq(b.queue(), ["X", "Y"], "Y at the end")
    kp = K(None, kx[0], "rej/run-y")
    mv = b.qrun("A", [y], START, members=[(y, kp)], after=[ay])
    eq((b.out(mv), b.queue()), (("applied", None), ["Y", "X"]), "the run moves; its prediction is valid")
    eq(L(b, y, "queue_position")["key"], kp[0], "the fitting prediction is the published key")


def test_bulk_members_and_closed_generations():
    b = B("bulk members, closed generations and tombstones")
    sp = b.subscribe("A", U1)
    sq = b.subscribe("A", U2)
    P, GP = pg(sp)
    Q, GQ = pg(sq)
    x, y, w = ref(P, GP, "x", "X"), ref(Q, GQ, "y", "Y"), ref(Q, GQ, "w", "W")
    kx, ky, kw = keys_in_order(["bulk/x", "bulk/y", "bulk/w"])
    b.qadd("A", [(x, kx), (y, ky)], place=END)
    u = b.pod("B", "unsubscribe", Q, GQ)
    eq(b.queue(), ["X"], "a closed generation hides its entries")
    check(MEM(b, y) is None and b.rec("episode", y["episode_id"], "queue_position") is None, "tombstoned")
    a2 = b.qadd("C", [(w, kw), (x, kx)], place=START, after=[sp])
    eq(b.out(a2), ("applied", None), "one effective member is enough")
    eq(b.queue(), ["X"], "X was already in: nothing moves; W is a ghost (its generation closed)")
    a3 = b.qadd("C", [(w, kw)], place=END)
    eq(b.out(a3), ("not_applied", "generation_closed"), "every member fails")
    ghost = c.podcast_id(NS, b.uuid())
    gref = ref(ghost, c.generation_id(NS, ghost, b.uuid()), "g", "Ghost")
    a4 = b.qremove("C", [gref, w])
    first = sorted([gref, w], key=lambda r: r["episode_id"])[0]
    eq(b.out(a4), ("not_applied", "unknown_podcast" if first is gref else "generation_closed"),
       "the first member's reason in payload order")
    # An archived podcast's episodes stay queued (Android has no feed-state filter).
    b.pod("A", "archive", P, GP)
    eq(b.queue(), ["X"], "archived episodes stay queued")
    eq(b.out(u), ("applied", None), "unsubscribe")


def test_queue_input_errors():
    b = B("6b: malformed decision supplies and decision arrivals (not a vector)")
    b.vector_ok = False
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "x", "X")
    kp = K(None, None, "ie/x")
    ok = b.qadd("A", [(e, kp)], place=END)
    b.qpolicy("A", keep_sorted=True, sort_order="episode_title_z_a")
    # Y's add sorts at its place (title z-a: Y before X): the layout re-keys one member.
    t = b.qadd("B", [(ref(P, G, "y", "Y"), K(kp[0], None, "ie/y"))], place=END)
    good = [a for a in b.arrivals if a["fact"]["op_id"] == t][0]
    check(good["decisions"] and good["decisions"][0]["decision_type"] == rules.LAYOUT, "a layout")
    check(len(good["decisions"][0]["position_ids"]) == 1, "one fresh key")

    def replay(mutate):
        eng = rules.Engine(ACC, NS)
        arrs = json.loads(json.dumps(b.arrivals))
        for a in arrs:
            if a["fact"]["op_id"] == t:
                mutate(a)
        for a in arrs:
            eng.accept(a)
    replay(lambda a: None)
    expect_input_error(lambda: replay(lambda a: a.pop("decisions")), "a missing supply")
    expect_input_error(lambda: replay(lambda a: a["decisions"].append(dict(a["decisions"][0]))), "an extra supply")
    expect_input_error(lambda: replay(lambda a: a["decisions"][0].__setitem__("position_ids", [])),
                       "a wrong number of position IDs")
    expect_input_error(lambda: replay(lambda a: a["decisions"][0].__setitem__("decision_id", ok)), "a known node ID")
    expect_input_error(lambda: replay(lambda a: a["decisions"][0].__setitem__("decision_type", rules.RESOLVED_ADD)),
                       "the wrong decision type")
    d = layouts(b)[-1]
    other = B("decision arrival errors (not a vector)")
    other.vector_ok = False
    expect_input_error(lambda: other.upload_decision(d), "a decision before its dependencies")
    before = b.engine.records()
    b.upload_decision(dict(d, normalized_time=d["normalized_time"] + 1))
    check(b.engine.records() == before and b.engine.transport[-1] == (d["decision_id"], "conflict"),
          "a known decision ID with other content is a conflict that changes nothing")
    # (A sparse layout may list one member: a second one with a lower key after it.)
    m0 = d["result"]["members"][0]
    lower = K(None, m0["key"], "ie/lower")
    other_e = e["episode_id"] if m0["episode_id"] != e["episode_id"] else ref(P, G, "y", "Y")["episode_id"]
    mem2 = [m0, dict(m0, episode_id=other_e, key=lower[0], position_id=lower[1])]
    expect_input_error(lambda: b.accept({"decision": dict(d, decision_id=det_uuid("ie/new"),
                                                           result={"members": mem2})}),
                       "a layout whose keys do not increase in its order")
    expect_input_error(lambda: b.accept({"decision": dict(d, decision_id=det_uuid("ie/new2"), subject="other")}),
                       "a layout's subject is the queue")
    expect_input_error(lambda: rules.run_case({"account_id": ACC, "namespace": NS, "arrivals": [
        {"decision": dict(d, decision_type="queue.sort_result")}]}), "queue.sort_result is retired")


def test_keys_property():
    """PROTOCOL §7.6: thousands of nested and concurrent keys, strictly inside their gaps and
    distinct (contract-v1 allocator)."""
    rng = random.Random(6262)
    keys = [K(None, None, "prop/0")[0]]
    seen = set(keys)
    for i in range(3000):
        j = rng.randrange(len(keys) + 1)
        low = keys[j - 1] if j > 0 else None
        high = keys[j] if j < len(keys) else None
        for k in range(1 + (i % 3)):  # concurrent writers allocate into the same gap
            kk = K(low, high, "prop/%d/%d" % (i, k))[0]
            check(low is None or c.compare_queue_keys(low, kk) < 0, "above its low")
            check(high is None or c.compare_queue_keys(kk, high) < 0, "below its high")
            check(kk not in seen, "distinct")
            seen.add(kk)
        keys.insert(j, kk)
    for a, z in zip(keys, keys[1:]):
        check(c.compare_queue_keys(a, z) < 0, "ordered")


# ---------------------------------------------------------------------------
# PROTOCOL §7.6: the canonical comparators (suite queue_sort)
# ---------------------------------------------------------------------------

PA = "p1:" + "a" * 64
PB = "p1:" + "b" * 64
PC = "p1:" + "c" * 64


def queue_sort_cases():
    """Hand-picked inputs for the shared `queue_sort` vectors (PROTOCOL §7.6); outputs from the
    reference, several of them asserted by hand in test_queue_sort."""
    t = lambda title, **kv: dict(kv, title=title, podcast_id=kv.pop("podcast_id", PA))  # noqa: E731
    cases = [
        ("empty queue", []),
        ("one item", [t("Only", publication_time=5, duration=7)]),
        ("all values equal: every order keeps the queue order (stability)",
         [{"podcast_id": PA, "title": "Same", "podcast_title": "Show", "publication_time": 9, "duration": 9}
          for _ in range(4)]),
        ("all values missing: Android's defaults, every order is stable",
         [{"podcast_id": PA}, {"podcast_id": PB}, {"podcast_id": PA}, {"podcast_id": PC}]),
        ("Unicode simple lowercase, then code points",
         [t("banana"), t("Apple"), t("ÄRGER"), t("äpfel"), t("Zebra"), t("zebra"), t("İstanbul"), t("istanbul"),
          t("ΟΔΟΣ"), t("οδος"), t("Straße"), t("STRASSE"), t("ǅemal"), t("ǆemal"), t("\U00010400x"), t("\U00010428w"),
          t("ﬁle"), t("\U0001f600"), t(""), t("apple")]),
        ("missing titles sort as the empty string", [t("b"), {"podcast_id": PA}, t("A"), t(""), {"podcast_id": PB}]),
        ("feed titles: lowercase, missing, ties",
         [{"podcast_id": PA, "title": "1", "podcast_title": "Zeta"}, {"podcast_id": PB, "title": "2"},
          {"podcast_id": PC, "title": "3", "podcast_title": "alpha"},
          {"podcast_id": PA, "title": "4", "podcast_title": "ZETA"},
          {"podcast_id": PB, "title": "5", "podcast_title": "Älpha"}]),
        ("dates: missing is 0, negative times, ties",
         [t("a", publication_time=1000), t("b"), t("c", publication_time=-5), t("d", publication_time=1000),
          t("e", publication_time=1760000000000), t("f", publication_time=0)]),
        ("durations: missing is 0, ties, large values",
         [t("a", duration=600000), t("b"), t("c", duration=600000), t("d", duration=9007199254740991),
          t("e", duration=1), t("f", duration=0)]),
        ("smart shuffle: two equally large groups (tie by podcast ID)",
         [t("b1", podcast_id=PB, publication_time=10), t("a1", podcast_id=PA, publication_time=20),
          t("b2", podcast_id=PB, publication_time=5), t("a2", podcast_id=PA, publication_time=1)]),
        ("smart shuffle: three podcasts, ties inside a group, missing dates",
         [t("a1", podcast_id=PA, publication_time=3), t("c1", podcast_id=PC), t("a2", podcast_id=PA, publication_time=3),
          t("b1", podcast_id=PB, publication_time=7), t("a3", podcast_id=PA, publication_time=1),
          t("c2", podcast_id=PC, publication_time=2), t("a4", podcast_id=PA), t("b2", podcast_id=PB, publication_time=7)]),
    ]
    rng = random.Random(6161)
    pods = ["p1:" + ("%x" % i) * 64 for i in range(1, 6)]
    for n in (17, 40):
        items = []
        for i in range(n):
            it = {"podcast_id": rng.choice(pods[:3] if n == 17 else pods)}
            if rng.random() < 0.9:
                it["title"] = rng.choice(["Ep %d" % rng.randint(1, 9), "ep %d" % rng.randint(1, 9), "Épisode", "Zz"])
            if rng.random() < 0.8:
                it["publication_time"] = rng.choice([0, 1000, 2000, rng.randint(-10, 10**12)])
            if rng.random() < 0.8:
                it["duration"] = rng.choice([0, 60000, rng.randint(0, 10**7)])
            if rng.random() < 0.5:
                it["podcast_title"] = rng.choice(["Show", "show", "Émission", "Zulu"])
            items.append(it)
        cases.append(("generated queue of %d items (seed 6161)" % n, items))
    out = []
    for desc, items in cases:
        out.append({"description": desc, "items": items,
                    "output": {o: qs.canonical_order(items, o) for o in qs.KEEP_SORTED_ORDERS}})
    return out


def test_queue_sort():
    # The simple lowercase mapping differs from Python's full one only at U+0130.
    for cp in range(0x110000):
        if 0xD800 <= cp <= 0xDFFF:
            continue
        ch = chr(cp)
        low = qs.simple_lower(ch)
        check(len(low) == 1, "U+%04X maps to one code point" % cp)
    eq(qs.simple_lower("İΣΑΣ ǅ STRASSE ẞ"), "iσασ ǆ strasse ß", "simple lowercase (no final sigma, no locale)")
    cases = {d["description"]: d for d in queue_sort_cases()}
    eq(cases["Unicode simple lowercase, then code points"]["output"]["episode_title_a_z"],
       [18, 1, 19, 0, 6, 7, 11, 10, 4, 5, 3, 2, 12, 13, 9, 8, 16, 15, 14, 17],
       "hand-checked: '' < Apple = apple (stable) < banana < İstanbul = istanbul (simple mapping) < strasse < straße"
       " < Zebra = zebra < äpfel < ärger < ǅemal = ǆemal < οδος < ΟΔΟΣ (no final sigma) < ﬁle (U+FB01) < 𐐨w < 𐐀x"
       " (U+10428) < 😀 (code points, not UTF-16 units)")
    z_a = cases["Unicode simple lowercase, then code points"]["output"]["episode_title_z_a"]
    eq(z_a[:3], [17, 14, 15], "z-a reverses the comparison, ties keep the queue order")
    eq(cases["smart shuffle: two equally large groups (tie by podcast ID)"]["output"]["smart_shuffle_old_new"],
       [2, 3, 1, 0], "hand-computed: PA first (smaller ID) into slots 1, 2 (spread 4/3); PB into 0, 3")
    eq(cases["smart shuffle: two equally large groups (tie by podcast ID)"]["output"]["smart_shuffle_new_old"],
       [0, 1, 3, 2], "new to old")
    eq(cases["dates: missing is 0, negative times, ties"]["output"]["date_old_new"], [2, 1, 5, 0, 3, 4], "dates")
    eq(cases["durations: missing is 0, ties, large values"]["output"]["duration_long_short"], [3, 0, 2, 4, 1, 5],
       "durations, ties stable in both directions")
    eq(cases["all values missing: Android's defaults, every order is stable"]["output"],
       {o: [1, 0, 2, 3] if o.startswith("smart") else [0, 1, 2, 3] for o in qs.KEEP_SORTED_ORDERS},
       "hand-computed: missing values; smart shuffle places PA (2 items, spread 4/3) in slots 1, 2, then PB and PC"
       " (1 each, by ID) in slots 0 and 3")
    # Smart shuffle reproduces Android's slot arithmetic (one large group spread out).
    items = [{"podcast_id": PA, "publication_time": i} for i in range(10)] + [{"podcast_id": PB, "publication_time": 0},
                                                                                {"podcast_id": PC, "publication_time": 0}]
    got = qs.canonical_order(items, "smart_shuffle_old_new")
    eq([items[i]["podcast_id"][3] for i in got], list("baaaaaaaaaac"),
       "hand-computed Android spread for 10 + 1 + 1: slot 0 is skipped (1 < 12/11), then 10 in a row")
    # LIS retention: the lexicographically first longest increasing subsequence.
    eq(sorted(qs.retained_positions([1, 0, 2])), [0, 2], "first by sorted position")
    eq(sorted(qs.retained_positions([2, 0, 1, 3])), [1, 2, 3], "the longest")
    eq(sorted(qs.retained_positions([3, 2, 1, 0])), [0], "decreasing: the first")
    eq(sorted(qs.retained_positions([])), [], "empty")
    for doc_case in queue_sort_cases():
        import check_vectors
        check_vectors.s_queue_sort(json.loads(json.dumps(doc_case, ensure_ascii=False)))


# ---------------------------------------------------------------------------
# PROTOCOL §6.10: copied installations
# ---------------------------------------------------------------------------

SEC = 1000


def SV(label):
    """A save identity (PROTOCOL §6.10)."""
    return det_uuid("save/" + label)


def BLN(label):
    """A baseline identity (PROTOCOL §6.10)."""
    return det_uuid("baseline/" + label)


def sv(label, index, base, first=None, passive=False):
    d = {"id": SV(label), "index": index, "baseline": BLN(base), "passive": passive}
    if first is not None:
        d["first"] = SV(first)
    return d


def PT(b, r):
    v = L(b, r, "played_time")
    return v["played_ms"] if v is not None else None


def merge_orders(lists):
    """Every interleaving of several step lists (each kept in its own order)."""
    lists = [x for x in lists if x]
    if not lists:
        return [[]]
    out = [list(lists[0])]
    for nxt in lists[1:]:
        out = [o for prev in out for o in interleavings(prev, nxt)]
    return out


# Scenario 33 (PROTOCOL §6.10): an export at 300 s; the library is copied at a saved 330 s.
S33_EXPECT = {"base": 380, "rewind": 120, "lost": 350, "two_copies": 360, "reset": 30, "no_tail": 380}


def s33_steps(detection, variant):
    steps = {}
    if variant not in ("lost", "two_copies"):
        steps["A"] = [("A", "E2")]
    copies = ["C", "C2"] if variant == "two_copies" else ["C"]
    for cn in copies:
        st = []
        if detection == "local":
            st.append((cn, "fork"))
        if variant != "no_tail":
            st.append((cn, "tail"))
        if variant == "reset":
            st.append((cn, "reset"))
        else:
            if detection == "local":
                st.append((cn, "new"))
            st += [(cn, "L1"), (cn, "L2")]
        steps[cn] = st
    return steps


def s33_case(name, prefix, detection, variant, order):
    b = B(name, prefix)
    b.server_label = "scenario 33 %s %s" % (detection, variant)
    s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e = ref(P, G, "ep-33", "Scenario 33")
    S = fixed_op(prefix, 100)
    a1 = b.start("A", e, S, 0, baseline=BLN("A0"), duration=D, op=fixed_op(prefix, 2))
    E1 = b.cp("A", e, S, 1, 300 * SEC, 300 * SEC, save=sv("s60", 60, "A0", first="s1"), op=fixed_op(prefix, 3))
    T = b.now
    copies = ["C", "C2"] if variant == "two_copies" else ["C"]
    for cn in copies:
        if detection == "fallback":
            b.clone("A", cn)
        else:
            b.dev(cn)
    tail = variant != "no_tail"
    o_pos = 100 * SEC if variant == "rewind" else 360 * SEC
    listen = {"C": (355 * SEC, 350 * SEC), "C2": (345 * SEC, 340 * SEC)} if tail else {"C": (325 * SEC, 320 * SEC)}
    from_pos = 330 * SEC if tail else 300 * SEC
    ops = {}

    def go(step, rt):
        lib, what = step
        k = copies.index(lib) if lib in copies else 0
        dt = 5 * SEC * k
        n = 10 + 20 * k
        op = fixed_op(prefix, n + {"E2": 0, "fork": 1, "tail": 2, "reset": 3, "new": 4, "L1": 5, "L2": 6}[what])
        if what == "E2":
            x = b.cp("A", e, S, 2, o_pos, o_pos, save=sv("o72", 72, "A0", first="s61"), t=T + 60 * SEC, rt=rt, op=op)
        elif what == "fork":
            x = b.fork(lib, "A", E1, t=T + 500 + dt, rt=rt, op=op)
        elif what == "tail":
            # The restart export (fallback) or tail export (local detection): the copied
            # saves s61..s66, passive.
            x = b.cp(lib, e, S, 2, 330 * SEC, 330 * SEC, save=sv("s66", 66, "A0", first="s61", passive=True),
                     t=T + SEC + dt, rt=rt, op=op)
        elif what == "reset":
            x = b.sreset(lib, t=T + 2 * SEC + dt, rt=rt, op=op)
        elif what == "new":
            # Local detection: new listening starts a new session (PROTOCOL §6.10).
            x = b.start(lib, e, fixed_op(prefix, 200 + k), from_pos, baseline=BLN(lib + "F0"), observed=a1,
                        t=T + 30 * SEC + dt, rt=rt, op=op)
        else:
            hi, lo = listen[lib]
            pos = hi if what == "L1" else lo
            if detection == "local":
                sid, ordinal = fixed_op(prefix, 200 + k), (1 if what == "L1" else 2)
                save = sv(lib + what, 5 if what == "L1" else 8, lib + "F0", first=lib + what + "first")
                x = b.cp(lib, e, sid, ordinal, pos, pos - from_pos, save=save, t=T + (40 if what == "L1" else 50) * SEC + dt,
                         rt=rt, op=op)
            else:
                # Fallback: the copy reloads (a new baseline) and resumes the same session.
                ordinal = (3 if tail else 2) + (0 if what == "L1" else 1)
                index = (71 if tail else 65) + (0 if what == "L1" else 3)
                save = sv(lib + what, index, lib + "B1", first=lib + what + "first")
                x = b.cp(lib, e, S, ordinal, pos, pos, save=save, t=T + (40 if what == "L1" else 50) * SEC + dt,
                         rt=rt, op=op)
        ops[step] = x
        return x
    for i, step in enumerate(order):
        go(step, T + 100 * SEC + i * SEC)
    return b, {"e": e, "G": G, "S": S, "a1": a1, "E1": E1, "ops": ops, "copies": copies}


def test_scenario33():
    """PROTOCOL §6.10: the played-time table, in both detection paths and every arrival order,
    with the ownership consequences."""
    for detection in ("fallback", "local"):
        for variant, want in sorted(S33_EXPECT.items()):
            steps = s33_steps(detection, variant)
            orders = merge_orders(list(steps.values()))
            prefix = B.count + 1
            finals = []
            for k, order in enumerate(orders):
                name = "scenario 33, %s, %s, order %d/%d: %s" % (
                    detection, variant, k + 1, len(orders), " ".join("%s:%s" % s for s in order))
                b, x = s33_case(name, prefix, detection, variant, order)
                # Every order is checked; the first and the last become vectors.
                b.vector_ok = k in (0, len(orders) - 1)
                e, G, ops, a1 = x["e"], x["G"], x["ops"], x["a1"]
                eq(PT(b, e), want * SEC, "%s: played time" % name)
                check(all(o["outcome"] == "applied" for o in b.engine.outcomes.values()), "%s: all applied" % name)
                if variant == "reset":
                    eq(L(b, e, "position"), posv(G, 360 * SEC, a1), "%s: the original's position" % name)
                elif variant in ("rewind", "base", "lost", "no_tail", "two_copies"):
                    last = x["copies"][-1]
                    if detection == "fallback":
                        # The copy's resumed listening takes over from its first save and
                        # the original's ordinary checkpoint never steals it back. With
                        # the original lost, the copy is the session's only continuation
                        # (no divergence), so the session's claim (the start) stands; the position
                        # is the copy's either way.
                        owner = a1 if variant == "lost" else ops[(last, "L1")]
                        eq(L(b, e, "position"), posv(G, b.engine.facts[ops[(last, "L2")]]["fact"]["payload"]["position"],
                                                     owner), "%s: the copy's claim owns the position" % name)
                    else:
                        eq(L(b, e, "position")["claim_id"], ops[(last, "new")], "%s: the copy's new session" % name)
                if detection == "fallback" and ("A", "E2") in ops and (x["copies"][0], "tail") in ops:
                    first = order.index(("A", "E2")) < order.index((x["copies"][0], "tail"))
                    keeper, other = (("A", "E2"), (x["copies"][0], "tail")) if first else (
                        (x["copies"][0], "tail"), ("A", "E2"))
                    eq(b.eff(ops[keeper]), b.dev("A")["id"], "%s: the first at the position keeps the author" % name)
                    eq(b.eff(ops[other]), rules.derived_author(NS, ops[other]), "%s: the other is a derived root" % name)
                finals.append(b.engine.records())
            for k in range(1, len(finals)):
                eq(finals[k], finals[0], "scenario 33 %s %s: order %d converges" % (detection, variant, k + 1))


def test_ownership():
    """PROTOCOL §6.10 ownership: a tail before the first checkpoint with another claim in
    between takes nothing; a seal covering no new save adds nothing; stale completions are
    suppressed; passive exports never claim (whatever their baseline)."""
    # A tail exported before the session's first checkpoint, another phone's claim between.
    for detection in ("fallback", "local"):
        b = B("ownership: a tail before the first checkpoint takes nothing (%s)" % detection)
        P, G, (e,) = setup_listening(b)
        S = b.uuid()
        a1 = b.start("A", e, S, 0, baseline=BLN("A0"))  # saves s1..s6 stay unexported
        if detection == "fallback":
            b.clone("A", "C")
        else:
            b.fork("C", "A", a1)
        SQ = b.uuid()
        q1 = b.start("Q", e, SQ, 0, after=[a1])
        q2 = b.cp("Q", e, SQ, 1, 10 * SEC, 10 * SEC)
        t = b.cp("C", e, S, 1, 30 * SEC, 30 * SEC, save=sv("s6", 6, "A0", first="s1", passive=True))
        eq(L(b, e, "position"), posv(G, 10 * SEC, q1), "the passive tail neither claims nor writes")
        eq(PT(b, e), 40 * SEC, "but counts")
        eq((b.out(t), b.out(q2)), (("applied", None), ("applied", None)), "outcomes")
    # A seal (or an end) covering no new save reuses the node: no node, no edge, no claim,
    # but its ordinal, sealing and completion effects remain.
    b = B("ownership: an export without `first` reuses its node")
    P, G, (e, e2) = setup_listening(b, 2)
    S = b.uuid()
    a1 = b.start("A", e, S, 0, baseline=BLN("A0"))
    b.cp("A", e, S, 1, D, D, save=sv("end", 720, "A0", first="s1"))
    # The natural end re-exports the save already exported (contribution as reported, but
    # the tree keeps the node's): no edge, completion fires, the session is sealed.
    x = b.end("A", e, S, 2, D, D + 7 * SEC, completed=True, save=sv("end", 720, "A0"))
    eq(PT(b, e), D, "no new node or edge: the node's contribution counts, not the end's")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "completion effects remain")
    eq(L(b, e, "position"), posv(G, 0, a1), "the bundle")
    eq(b.out(b.cp("A", e, S, 2, D, D)), ("not_applied", "session_ordinal_conflict"), "its ordinal remains")
    eq(b.out(b.cp("A", e, S, 3, D, D)), ("not_applied", "session_sealed"), "sealing remains")
    eq(b.out(x), ("applied", None), "x")
    # A seal at the pre-adoption position under a new baseline cannot claim.
    S2 = b.uuid()
    c1 = b.start("A", e2, S2, 0, baseline=BLN("B0"))
    b.cp("A", e2, S2, 1, MIN, MIN, save=sv("t12", 12, "B0", first="t1"))
    q = b.start("Q", e2, b.uuid(), 0, after=[c1])
    y = b.end("A", e2, S2, 2, MIN, MIN, reason="superseded", activity=False, history=False,
              save={"id": SV("t12"), "index": 12, "baseline": BLN("B9"), "passive": False})
    eq(L(b, e2, "position"), posv(G, 0, q), "an export of no new save never claims, whatever its baseline")
    eq(PT(b, e2), MIN, "and adds nothing")
    eq(b.out(y), ("applied", None), "y")
    # Stale completions are suppressed; the original's ordinary checkpoints never steal
    # the copy's claim back; passive exports never claim.
    b = B("ownership: the copy's claim, the original's stale completion, passive exports")
    P, G, (e,) = setup_listening(b)
    S = b.uuid()
    a1 = b.start("A", e, S, 0, baseline=BLN("A0"), duration=D)
    b.cp("A", e, S, 1, 50 * MIN, 50 * MIN, save=sv("s600", 600, "A0", first="s1"))
    b.clone("A", "C")
    r = b.cp("C", e, S, 2, 51 * MIN, 51 * MIN, save=sv("s612", 612, "A0", first="s601", passive=True))
    c1 = b.cp("C", e, S, 3, 53 * MIN, 53 * MIN, save=sv("c636", 636, "C1", first="c613"))
    # Until the original's own continuation arrives there is no divergence, so the copy writes
    # under the session's claim (PROTOCOL §8); it claims once the original's facts make the two
    # continuations (below).
    eq(L(b, e, "position"), posv(G, 53 * MIN, a1), "before any divergence the session's claim stands")
    eq(b.eff(r), b.dev("A")["id"], "the copy keeps the author (its facts came first)")
    o2 = b.cp("A", e, S, 2, 55 * MIN, 55 * MIN, save=sv("o660", 660, "A0", first="s601"))
    eq(b.eff(o2), rules.derived_author(NS, o2), "the original is the derived branch")
    eq(L(b, e, "position"), posv(G, 53 * MIN, c1), "the original's ordinary checkpoint never steals it back")
    o3 = b.end("A", e, S, 3, D, D, completed=True, save=sv("o720", 720, "A0", first="o661"))
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "a stale completion is suppressed")
    eq(L(b, e, "position"), posv(G, 53 * MIN, c1), "position unchanged")
    eq(PT(b, e), 51 * MIN + 2 * MIN + (D - 51 * MIN), "0-50-51 shared, the copy's 2, the original's 9")
    # Passive exports never claim, whatever their baseline: the copy reloads again (C2),
    # its process dies before exporting and the restart export is passive.
    q = b.start("Q", e, b.uuid(), 0, after=[c1])
    p = b.cp("C", e, S, 4, 54 * MIN, 54 * MIN, save=sv("c648", 648, "C2", first="c637", passive=True))
    eq(L(b, e, "position")["claim_id"], q, "a passive export under a new baseline does not claim")
    p2 = b.cp("C", e, S, 5, 55 * MIN, 55 * MIN, save=sv("c660", 660, "C2", first="c649"))
    eq(L(b, e, "position")["claim_id"], q, "nor does a later export under that baseline (its first was passive)")
    p3 = b.cp("C", e, S, 6, 56 * MIN, 56 * MIN, save=sv("c672", 672, "C3", first="c661"))
    eq(L(b, e, "position"), posv(G, 56 * MIN, p3), "a reload's first non-passive export claims")
    # A finalization naming the session's start, from a continuation that has claimed,
    # means that continuation's claim.
    e2 = ref(P, G, "ep-2", "Two")
    fz = b.fin("C", e, S, 6, a1, completed=False, history=True)
    eq(b.out(fz), ("applied", None), "C's finalization")
    b.start("C", e2, b.uuid(), 0)
    eq([b.out(z) for z in (r, c1, o2, o3, p, p2, p3)], [("applied", None)] * 7, "outcomes")
    b = B("ownership: a finalization naming the start means its continuation's claim")
    P, G, (e,) = setup_listening(b)
    S = b.uuid()
    a1 = b.start("A", e, S, 0, baseline=BLN("A0"), duration=D)
    b.cp("A", e, S, 1, 50 * MIN, 50 * MIN, save=sv("s600", 600, "A0", first="s1"))
    b.clone("A", "C")
    b.cp("A", e, S, 2, 51 * MIN, 51 * MIN, save=sv("o612", 612, "A0", first="s601"))
    b.cp("C", e, S, 2, 50 * MIN, 50 * MIN, save=sv("s600", 600, "A0", passive=True))  # the seal-like restart
    c1 = b.cp("C", e, S, 3, D - 10 * SEC, D - 10 * SEC, save=sv("c700", 700, "C1", first="c601"))
    f = b.fin("C", e, S, 3, a1, completed=True)
    eq(b.out(f), ("applied", None), "fires under the copy's claim")
    eq(L(b, e, "position"), posv(G, 0, c1), "published under the substituted claim")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "played")


def test_shared_exports_are_the_writing_history():
    """CHOICE (PROTOCOL §6.10 "the exports it shares with its ancestors (the facts in its causal
    past)"): a copy without a tail pulls the original's later export of the session before
    it resumes; that export is in the copy's causal past but not in its own writing
    history, so it is not on the copy's path. The copy branches at the last export it
    copied: 300 + 60 (the original) + 20 (the copy) = 380 s. Reading the shared exports as
    the whole causal past would put the copy after the original's 360 s (300 + 60 - 40 =
    320 s), so this vector separates the two readings."""
    b = B("CHOICE: shared exports are the copy's writing history, not everything it pulled")
    P, G, (e,) = setup_listening(b)
    S = b.uuid()
    a1 = b.start("A", e, S, 0, baseline=BLN("A0"), duration=D)
    E1 = b.cp("A", e, S, 1, 300 * SEC, 300 * SEC, save=sv("s60", 60, "A0", first="s1"))  # paused: no tail
    b.clone("A", "C")
    E2 = b.cp("A", e, S, 2, 360 * SEC, 360 * SEC, save=sv("o72", 72, "A0", first="s61"))  # the original resumes
    # The copy syncs (its frontier now holds E2), reloads and resumes from its own 300 s.
    c1 = b.cp("C", e, S, 2, 320 * SEC, 320 * SEC, save=sv("c64", 64, "C1", first="c61"), after=[E2])
    check(E2 in b.engine._ancestors(c1), "the original's later export is in the copy's causal past")
    eq(b.eff(c1), rules.derived_author(NS, c1), "the copy is the branch")
    eq(PT(b, e), 380 * SEC, "the copy's path is E1 then its own exports")
    eq(L(b, e, "position"), posv(G, 320 * SEC, c1), "its resumed listening claims")
    eq([b.out(x) for x in (a1, E1, E2, c1)], [("applied", None)] * 4, "outcomes")


def test_scenario22_restored_backup():
    """PROTOCOL §6.10 scenario 22: a restored backup running beside the original loses and
    duplicates nothing."""
    b = B("scenario 22: a restored backup beside the original")
    P, G, (e1, e2, e3) = setup_listening(b, 3)
    b.dev("A")
    S = b.uuid()
    a1 = b.start("A", e1, S, 0, baseline=BLN("A0"), duration=D)
    a2 = b.cp("A", e1, S, 1, 10 * MIN, 10 * MIN, save=sv("s120", 120, "A0", first="s1"))
    # The backup holds A's library up to a2, with two facts still in its outbox.
    pend = [b.fav("A", e2, True), b.mark_played("A", e3, reset=False)]
    backup = dict(b.dev("A"))
    a3 = b.cp("A", e1, S, 2, 20 * MIN, 20 * MIN, save=sv("s240", 240, "A0", first="s121"))
    b.devs["C"] = dict(backup, caps=list(backup["caps"]))
    for op in pend:
        b.redeliver(op)  # the restored library re-sends its outbox: duplicates
    eq([r for _, r in b.engine.transport], ["duplicate", "duplicate"], "no fact is duplicated")
    c1 = b.cp("C", e1, S, 2, 12 * MIN, 12 * MIN, save=sv("s144", 144, "A0", first="s121", passive=True))
    c2 = b.cp("C", e1, S, 3, 15 * MIN, 15 * MIN, save=sv("c180", 180, "C1", first="c145"))
    c3 = b.fav("C", e2, False)
    eq(b.eff(c1), rules.derived_author(NS, c1), "the backup diverged: a derived branch")
    eq(b.eff(c3), b.eff(c1), "its later facts inherit the branch")
    eq(PT(b, e1), 20 * MIN + 3 * MIN, "A's 20 minutes and the backup's own 3, the shared 12 once")
    eq(L(b, e1, "position"), posv(G, 15 * MIN, c2), "the backup's resumed listening claims")
    eq(L(b, e2, "favorite"), {"generation_id": G, "on": False}, "the backup's later edit counts")
    eq(L(b, e3, "play_state"), {"generation_id": G, "play_state": "played"}, "the shared outbox fact once")
    eq([b.out(x) for x in (a1, a2, a3, c1, c2, c3) + tuple(pend)], [("applied", None)] * 8, "nothing lost")


def test_derived_branches():
    """PROTOCOL §6.10: derived branches with and without a shared outbox, behind a rejected fact,
    copies of copies, every arrival order, the first ordinary fact after a divergence and
    lineage-based session admission."""
    # Shared and separate transports differ only below the rules (installation_conflict);
    # at the rule level the copy either re-sends the shared outbox first or not.
    for shared in (True, False):
        prefix = B.count + 1
        finals = []
        for first in ("A", "C"):
            b = B("derived branch, %s outbox, %s first" % ("shared" if shared else "no shared", first), prefix)
            s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
            P, G = pg(s)
            e1, e2 = ref(P, G, "ep-1", "One"), ref(P, G, "ep-2", "Two")
            pend = b.fav("A", e1, True, op=fixed_op(prefix, 2))
            b.clone("A", "C")
            base = b.now
            libs = {
                "A": lambda rt: (b.mark_played("A", e2, reset=False, t=base + 100, rt=rt, op=fixed_op(prefix, 3)),),
                "C": lambda rt: ((b.redeliver(pend),) if shared else ()) + (
                    b.fav("C", e1, False, t=base + 200, rt=rt, op=fixed_op(prefix, 4)),
                    b.fav("C", e2, True, t=base + 300, rt=rt + 10, op=fixed_op(prefix, 5))),
            }
            libs[first](base + 1000)
            libs["C" if first == "A" else "A"](base + 2000)
            a3, c4, c5 = fixed_op(prefix, 3), fixed_op(prefix, 4), fixed_op(prefix, 5)
            A = b.dev("A")["id"]
            if first == "A":
                eq((b.eff(a3), b.eff(c4), b.eff(c5)), (A, rules.derived_author(NS, c4), rules.derived_author(NS, c4)),
                   "the copy's first ordinary fact roots a branch; the next inherits it")
            else:
                eq((b.eff(c4), b.eff(c5), b.eff(a3)), (A, A, rules.derived_author(NS, a3)), "the original is the branch")
            eq([b.out(x) for x in (pend, a3, c4, c5)], [("applied", None)] * 4, "the first ordinary facts are admitted")
            eq(L(b, e1, "favorite"), {"generation_id": G, "on": False}, "e1")
            finals.append(b.engine.records())
        eq(finals[0], finals[1], "both arrival orders converge (shared=%s)" % shared)
    # Behind a rejected fact: the copy's first divergent fact is rejected; it still holds
    # its position and passes its author on, so the divergence is found behind it.
    prefix = B.count + 1
    finals = []
    for order in (("A", "C"), ("C", "A")):
        b = B("derived branch behind a rejected fact, %s first" % order[0], prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e = ref(P, G, "ep-1", "One")
        S = fixed_op(prefix, 100)
        a1 = b.start("A", e, S, 0, baseline=BLN("A0"), op=fixed_op(prefix, 2))
        b.cp("A", e, S, 1, MIN, MIN, save=sv("s12", 12, "A0", first="s1"), op=fixed_op(prefix, 3))
        b.clone("A", "C")
        base = b.now

        def lib_a(rt):
            b.cp("A", e, S, 2, 2 * MIN, 2 * MIN, save=sv("s24", 24, "A0", first="s13"), t=base + 100, rt=rt,
                 op=fixed_op(prefix, 4))
            b.cp("A", e, S, 3, 3 * MIN, 3 * MIN, save=sv("s36", 36, "A0", first="s25"), t=base + 200, rt=rt + 10,
                 op=fixed_op(prefix, 5))

        def lib_c(rt):
            b.cp("C", e, S, 2, MIN, -1, reject=True, t=base + 150, rt=rt, op=fixed_op(prefix, 6))
            b.cp("C", e, S, 2, MIN + 30 * SEC, MIN + 30 * SEC, save=sv("c18", 18, "C1", first="c13"), t=base + 250,
                 rt=rt + 10, op=fixed_op(prefix, 7))
        (lib_a if order[0] == "A" else lib_c)(base + 1000)
        (lib_c if order[0] == "A" else lib_a)(base + 2000)
        rj, c7, a4 = fixed_op(prefix, 6), fixed_op(prefix, 7), fixed_op(prefix, 4)
        eq(b.out(rj), ("rejected", "invalid_payload"), "the rejected fact")
        eq(b.out(c7), ("applied", None), "the fact behind it is admitted (no session_ordinal_conflict)")
        eq(b.eff(c7), b.eff(rj), "it inherits the rejected fact's author")
        if order[0] == "A":
            eq(b.eff(rj), rules.derived_author(NS, rj), "the rejected fact roots the branch")
        else:
            eq(b.eff(a4), rules.derived_author(NS, a4), "the rejected fact held the position")
        eq(PT(b, e), 3 * MIN + 30 * SEC, "both continuations count once above the shared minute")
        finals.append(b.engine.records())
    eq(finals[0], finals[1], "behind a rejected fact: both orders converge")
    # Copies of copies and lineage-based admission.
    b = B("derived branches: copies of copies, lineage and foreign sessions")
    P, G, (e, e2) = setup_listening(b, 2)
    A = b.dev("A")["id"]
    S = b.uuid()
    b.start("A", e, S, 0, baseline=BLN("A0"))
    b.cp("A", e, S, 1, MIN, MIN, save=sv("s12", 12, "A0", first="s1"))
    b.clone("A", "C1")
    b.clone("A", "K")  # a sibling copy of the same library
    a3 = b.cp("A", e, S, 2, 2 * MIN, 2 * MIN, save=sv("a24", 24, "A0", first="s13"))
    c3 = b.cp("C1", e, S, 2, 90 * SEC, 90 * SEC, save=sv("c18", 18, "C1", first="c13"))
    D1 = rules.derived_author(NS, c3)
    eq(b.eff(c3), D1, "C1 is a branch")
    b.clone("C1", "C2")  # a copy of the copy
    c4 = b.cp("C1", e, S, 3, 100 * SEC, 100 * SEC, save=sv("c20", 20, "C1", first="c19"))
    c4b = b.cp("C2", e, S, 3, 110 * SEC, 110 * SEC, save=sv("d22", 22, "C2", first="d19"))
    D2 = rules.derived_author(NS, c4b)
    eq((b.eff(c4), b.eff(c4b)), (D1, D2), "the copy's copy roots a branch of the branch")
    eq((b.engine.parent_author(D2), b.engine.parent_author(D1)), (D1, A), "lineage D2 -> D1 -> A")
    eq(b.out(c4b), ("applied", None), "a copy of a copy continues the session (it descends from its author)")
    k3 = b.cp("K", e, S, 2, 70 * SEC, 70 * SEC, save=sv("k14", 14, "K1", first="k13"))
    DK = rules.derived_author(NS, k3)
    eq(b.engine.parent_author(DK), A, "a sibling copy")
    # A session started on a branch: the sibling and the ancestor are foreign to it.
    S2 = b.uuid()
    b.start("C1", e2, S2, 0, baseline=BLN("C1x"))
    eq(b.out(b.cp("K", e2, S2, 1, MIN, MIN)), ("not_applied", "foreign_session"), "a sibling branch is foreign")
    eq(b.out(b.cp("A", e2, S2, 1, MIN, MIN)), ("not_applied", "foreign_session"), "an ancestor is foreign")
    eq(b.out(b.cp("C2", e2, S2, 1, MIN, MIN)), ("applied", None), "a descendant continues")
    # Ordinals and sealing per continuation.
    eq(b.out(b.cp("C2", e, S, 3, 111 * SEC, 111 * SEC)), ("not_applied", "session_ordinal_conflict"),
       "an ordinal conflict within the branch's continuation")
    b.end("A", e, S, 3, 2 * MIN, 2 * MIN, reason="switched")
    eq(b.out(b.cp("C1", e, S, 4, 2 * MIN, 2 * MIN, save=sv("c24", 24, "C1", first="c21"))), ("applied", None),
       "the original's end does not seal a branch")
    b.end("C1", e, S, 5, 2 * MIN, 2 * MIN, reason="switched", save=sv("c24", 24, "C1"))
    eq(b.out(b.cp("C1", e, S, 6, 3 * MIN, 3 * MIN)), ("not_applied", "session_sealed"), "the branch's own end seals it")
    eq(b.out(b.cp("C2", e, S, 4, 2 * MIN, 2 * MIN, save=sv("d24", 24, "C2", first="d23"))), ("applied", None),
       "but not its copy's")
    # The duration register names the effective author.
    b.cp("C2", e, S, 5, 2 * MIN + 5 * SEC, 2 * MIN + 5 * SEC, duration=D + 1, save=sv("d25", 25, "C2", first="d25"))
    eq(L(b, e, "measured_duration"), {"generation_id": G, "duration_ms": D + 1, "installation_id": D2},
       "measured_duration.installation_id is the effective author")
    eq(b.out(a3), ("applied", None), "a3")


def test_simulated_restore():
    """PROTOCOL §6.10: an uninformed copy uploads first to a restored server; the original
    holder's earlier receipt arrives later in recovery: the same records as live."""
    prefix = B.count + 1
    live = B("simulated restore: live reference", prefix)
    live.server_label = "6c restore"
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e = ref(P, G, "ep-1", "One")
    S = fixed_op(prefix, 100)
    live.start("A", e, S, 0, baseline=BLN("A0"), op=fixed_op(prefix, 2))
    E1 = live.cp("A", e, S, 1, 300 * SEC, 300 * SEC, save=sv("s60", 60, "A0", first="s1"), op=fixed_op(prefix, 3))
    live.clone("A", "C")
    base = live.now
    E2 = live.cp("A", e, S, 2, 360 * SEC, 360 * SEC, save=sv("o72", 72, "A0", first="s61"), t=base + 60 * SEC,
                 rt=base + 100 * SEC, op=fixed_op(prefix, 4))
    R = live.cp("C", e, S, 2, 330 * SEC, 330 * SEC, save=sv("s66", 66, "A0", first="s61", passive=True),
                t=base + SEC, rt=base + 101 * SEC, op=fixed_op(prefix, 5))
    C1 = live.cp("C", e, S, 3, 350 * SEC, 350 * SEC, save=sv("c70", 70, "C1", first="c67"), t=base + 40 * SEC,
                 rt=base + 102 * SEC, op=fixed_op(prefix, 6))
    eq(live.eff(R), rules.derived_author(NS, R), "live: the copy is the branch")
    eq(PT(live, e), 380 * SEC, "live played time")
    # The restore: the restored server holds s, the start and E1 (epoch 1); it lost E2.
    rs = B("simulated restore: copy first, original holder recovered later", prefix)
    rs.server_label = "6c restore"
    rs.devs = json.loads(json.dumps(live.devs))
    for a in live.arrivals[:3]:
        rs.accept(json.loads(json.dumps(a)))
    by = {a["fact"]["op_id"]: a for a in live.arrivals}
    for k, op in enumerate((R, C1)):
        a = json.loads(json.dumps(by[op]))
        a["receipt"] = {"epoch": 2, "seq": 1 + k, "index": 0}  # new commits of the restored epoch
        rs.accept(a)
    eq(rs.eff(R), rs.dev("A")["id"], "provisionally the copy holds the raw position")
    eq(PT(rs, e), 350 * SEC, "before recovery")
    a = json.loads(json.dumps(by[E2]))
    a["recovery"] = True
    a["receipt"] = {"epoch": 1, "seq": 4, "index": 0}  # its original receipt, earlier
    rs.accept(a)
    eq((rs.eff(E2), rs.eff(R), rs.eff(C1)), (rs.dev("A")["id"], rules.derived_author(NS, R),
                                             rules.derived_author(NS, R)),
       "the recovered earlier receipt holds the position; the copy becomes D(R), as live")
    eq(rs.engine.records(), live.engine.records(), "the same records as the live order")
    eq(rs.out(R), ("applied", None), "receipts never change")


def test_copied_finalizations():
    """PROTOCOL §6.10: copied finalizations (applied, stale, first eligible in order) and a
    library copied after a derived branch keeping its lineage."""
    def setup(b, prefix, detection, tail=False):
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e, e2 = ref(P, G, "ep-1", "One"), ref(P, G, "ep-2", "Two")
        S = fixed_op(prefix, 100)
        a1 = b.start("A", e, S, 50 * MIN, baseline=BLN("A0"), duration=D, op=fixed_op(prefix, 2))
        E1 = b.cp("A", e, S, 1, D - 20 * SEC, D - 20 * SEC - 50 * MIN, save=sv("s118", 118, "A0", first="s1"),
                  op=fixed_op(prefix, 3))  # paused 20 s before the end: a pending lazy finalization
        if detection == "local":
            b.fork("F", "A", E1, op=fixed_op(prefix, 4))
            if tail:
                b.cp("F", e, S, 2, D - 15 * SEC, D - 15 * SEC - 50 * MIN,
                     save=sv("s119", 119, "A0", first="s119", passive=True), op=fixed_op(prefix, 5))
        else:
            b.clone("A", "F")
        return P, G, e, e2, S, a1, E1

    for detection in ("local", "fallback"):
        # Applied when the original did not continue.
        b = B("copied finalization applied: the original did not continue (%s)" % detection)
        P, G, e, e2, S, a1, E1 = setup(b, B.count, detection)
        f = b.fin("F", e, S, 1, a1, completed=True)
        b.start("F", e2, b.uuid(), 0)
        eq(b.out(f), ("applied", None), "applied")
        eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "completed")
        if detection == "local":
            check(b.engine.descends(b.eff(f), b.dev("A")["id"]), "F descends from A")
        # Superseded when it did (both arrival orders), without using up a later eligible
        # finalization; also when the original rewound, or exported a later save at the
        # ordinal the copy names (its tail's).
        for how in ("continued", "rewound", "same_ordinal"):
            prefix = B.count + 1
            finals = []
            # Under fallback the copy's finalization is a copied one only if the original's
            # next fact holds the author position first (see test_fallback_asymmetry).
            for first in ("original", "copy") if detection == "local" else ("original",):
                b = B("copied finalization stale: the original %s, %s first (%s)" % (how, first, detection), prefix)
                P, G, e, e2, S, a1, E1 = setup(b, prefix, detection, tail=how == "same_ordinal")
                base = b.now
                named = 2 if how == "same_ordinal" else 1
                pos = {"continued": D - 10 * SEC, "rewound": 52 * MIN, "same_ordinal": D - 12 * SEC}[how]

                def original(rt):
                    return b.cp("A", e, S, 2, pos, pos - 50 * MIN,
                                save=sv("o" + how, 125, "A0", first="s119"), t=base + 5000, rt=rt, op=fixed_op(prefix, 10))

                def copy(rt):
                    return b.fin("F", e, S, named, a1, completed=True, t=base + 6000, rt=rt, op=fixed_op(prefix, 11))
                if first == "original":
                    o, f = original(base + 10000), copy(base + 11000)
                    eq(b.out(f), ("superseded", None), "stale: superseded")
                else:
                    f, o = copy(base + 10000), original(base + 11000)
                    eq(b.out(f), ("applied", None), "applied at acceptance; receipts never change")
                eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "the stale completion is void")
                check(L(b, e, "history")["time"] >= b.nt(f), "an ineligible finalization's history mark still counts")
                finals.append(b.engine.records())
                # The original's own later finalization is still eligible.
                if how != "rewound":
                    g = b.fin("A", e, S, 2, a1, completed=True, t=base + 20000, rt=base + 20000)
                    eq(b.out(g), ("applied", None), "a later eligible finalization takes effect")
                    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "completed by the original")
            if len(finals) == 2:
                eq(finals[0], finals[1], "stale (%s, %s): both orders converge" % (how, detection))
    # First in order between two copies of one backup, original lost.
    b = B("copied finalizations: first eligible in order between two copies")
    P, G, e, e2, S, a1, E1 = setup(b, B.count, "local")
    b.fork("F2", "A", E1)
    f1 = b.fin("F2", e, S, 1, a1, completed=False, history=True)
    f2 = b.fin("F", e, S, 1, a1, completed=True, history=True)
    eq((b.out(f1), b.out(f2)), (("applied", None), ("superseded", None)), "the first eligible one takes effect")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "unplayed"}, "the first decided: not completed")
    eq(L(b, e, "history"), {"generation_id": G, "time": max(b.nt(f1), b.nt(f2))}, "both history marks count")
    # A library copied after a derived branch keeps its lineage through `head`, also when
    # the copy was made offline: the copy cannot know head's effective author, the server
    # computes it whichever library's fact held the position first.
    for first in ("original", "branch"):
        b = B("lineage through head: a copy of a derived branch (the %s's fact first)" % first)
        P, G, (e, e2, e3) = setup_listening(b, 3)
        A = b.dev("A")["id"]
        b.clone("A", "C")
        if first == "original":
            b.fav("A", e3, True)  # the original's next fact holds the author position
        SC = b.uuid()
        c1 = b.start("C", e, SC, 50 * MIN, baseline=BLN("C0"), duration=D)
        c2 = b.cp("C", e, SC, 1, D - 20 * SEC, D - 20 * SEC - 50 * MIN, save=sv("cs1", 118, "C0", first="cs0"))
        if first == "branch":
            a_next = b.fav("A", e3, True)
            eq((b.eff(c1), b.eff(a_next)), (A, rules.derived_author(NS, a_next)), "C kept the author")
        else:
            eq(b.eff(c1), rules.derived_author(NS, c1), "C's session belongs to a derived branch")
        # C is copied (local detection) with its session paused near the end, saves
        # unexported: the fork names C's writing tip.
        fk = b.fork("Cx", "C", c2)
        tail = b.cp("Cx", e, SC, 2, D - 18 * SEC, D - 18 * SEC - 50 * MIN,
                    save=sv("cs2", 119, "C0", first="cs2", passive=True))
        f = b.fin("Cx", e, SC, 2, c1, completed=True)
        eq(b.engine.parent_author(b.dev("Cx")["id"]), b.eff(c2), "the fork's parent is head's effective author")
        eq([b.out(x) for x in (fk, tail, f)], [("applied", None)] * 3, "not foreign: tail and finalization apply")
        eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "the copied finalization completes")
        eq(PT(b, e), D - 18 * SEC - 50 * MIN, "the tail counts")
        eq(b.out(b.fin("Cx", e, SC, 2, c1, completed=True)), ("not_applied", "session_finalized"),
           "a second finalization by the same (descendant) author")
        eq(b.out(b.start("Cx", e, SC, 0)), ("not_applied", "session_ordinal_conflict"),
           "a second start of the session by a descendant (CHOICE)")


def test_fallback_asymmetry():
    """Under fallback, when the copy's pending finalization reaches the server before the
    original's next fact, the copy keeps the author and the original's further listening
    becomes the derived branch. In a family of several continuations every finalization is checked against the other
    continuations' exports, so the stale completion is void in both arrival orders."""
    prefix = B.count + 1
    finals = {}
    for first in ("original", "copy"):
        b = B("fallback symmetry of copied finalizations, %s first" % first, prefix)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e = ref(P, G, "ep-1", "One")
        S = fixed_op(prefix, 100)
        a1 = b.start("A", e, S, 50 * MIN, baseline=BLN("A0"), duration=D, op=fixed_op(prefix, 2))
        b.cp("A", e, S, 1, D - 20 * SEC, D - 20 * SEC - 50 * MIN, save=sv("s118", 118, "A0", first="s1"),
             op=fixed_op(prefix, 3))
        b.clone("A", "F")
        base = b.now
        steps = [lambda rt: b.cp("A", e, S, 2, D - 10 * SEC, D - 10 * SEC - 50 * MIN,
                                 save=sv("o125", 125, "A0", first="s119"), t=base + 5000, rt=rt, op=fixed_op(prefix, 10)),
                 lambda rt: b.fin("F", e, S, 1, a1, completed=True, t=base + 6000, rt=rt, op=fixed_op(prefix, 11))]
        if first == "copy":
            steps.reverse()
        for i, st in enumerate(steps):
            st(base + 10000 + 1000 * i)
        finals[first] = L(b, e, "play_state")["play_state"]
    eq(finals, {"original": "unplayed", "copy": "unplayed"},
       "the same in both arrival orders: the copy's stale completion is void")


def test_fork_admission():
    """PROTOCOL §6.10: installation.fork admission (applied, or rejected / invalid_payload)."""
    b = B("installation.fork admission")
    P, G, (e,) = setup_listening(b)
    a = b.fav("A", e, True)
    A = b.dev("A")["id"]
    ok = b.fork("F", "A", a)
    eq(b.out(ok), ("applied", None), "applied")
    eq(b.engine.parent_author(b.dev("F")["id"]), A, "lineage F -> head's effective author")
    eq(b.eff(ok), b.dev("F")["id"], "the fork is F's own first fact")
    bad = b.fav("B", e, False)
    cases = [
        ("author is the fork's own author", "G1", lambda d: {"author": d["id"], "head": a}, [a], None),
        ("head not among the dependencies", "G2", lambda d: {"author": A, "head": a}, [ok], None),
        ("head is not a fact of author", "G3", lambda d: {"author": A, "head": bad}, [bad], None),
        ("missing head", "G4", lambda d: {"author": A}, [a], None),
        ("extra member", "G5", lambda d: {"author": A, "head": a, "session": a}, [a], None),
        ("author not a UUID", "G6", lambda d: {"author": "A", "head": a}, [a], None),
        ("a target", "G7", lambda d: {"author": A, "head": a}, [a], [{"entity": "account", "id": ACC}]),
    ]
    for what, dev, payload, after, targets in cases:
        d = b.dev(dev)
        x = b.push(dev, "installation.fork", payload(d), targets or [], after=after, reject=True)
        eq(b.out(x), ("rejected", "invalid_payload"), what)
    # F has an earlier fact (its first fact is ordinary): the fork is invalid, and with a
    # previous_op_id it is not F's first fact either.
    b.fav("H", e, True)
    x = b.fork("H", "A", a, reject=True)
    eq(b.out(x), ("rejected", "invalid_payload"), "F has an earlier fact")
    # The other author-history rejections remain.
    x = b.push("G8", "installation.fork", {"author": A, "head": a}, [], after=[a],
               patch=lambda f: f.__setitem__("previous_op_id", a), reject=True)
    eq(b.out(x), ("rejected", "impossible_author_history"), "a predecessor of another author")
    # A rejected head counts (a pending or rejected writing tip).
    r = b.cp("A", e, b.uuid(), 1, 0, 0, reject=True)
    eq(b.out(r), ("rejected", "invalid_payload"), "a rejected tip")
    y = b.fork("J", "A", r)
    eq(b.out(y), ("applied", None), "a fork naming a rejected head")
    eq(b.engine.parent_author(b.dev("J")["id"]), A, "the rejected head's effective author")
    eq(b.engine.facts[y]["frontier"], b.engine.facts[r]["frontier"], "resolved through the rejected head")


def _replay(live, plan, name, prefix, lost=()):
    """Replays `live`'s arrivals in `plan` order: [(op, {"receipt": ..., "recovery": bool,
    "outcome": bool})...]; facts neither in the plan nor `lost` are delivered first, in
    live order."""
    rb = B(name, prefix)
    rb.server_label = live.server_label
    rb.devs = json.loads(json.dumps(live.devs))
    by = {a["fact"]["op_id"]: a for a in live.arrivals if "fact" in a}
    planned = [op for op, _ in plan]
    for a in live.arrivals:
        if a["fact"]["op_id"] not in planned and a["fact"]["op_id"] not in lost:
            rb.accept(json.loads(json.dumps(a)))
    for op, how in plan:
        a = json.loads(json.dumps(by[op]))
        a.pop("decisions", None)
        if "receipt" in how:
            a["receipt"] = dict(how["receipt"])
        if how.get("recovery"):
            a["recovery"] = True
            if how.get("outcome"):
                o = live.engine.outcomes[op]
                a["receipt"]["outcome"] = o["outcome"]
                if "reason" in o:
                    a["receipt"]["reason"] = o["reason"]
        rb.accept(a)
    return rb


def test_regressions():
    """PROTOCOL §6.10: a rejected writing tip met after its fork in recovery, and
    repeated copies preserving an ancestor's pending finalization."""
    prefix = B.count + 1
    live = B("regression: rejected writing tip, live reference", prefix)
    live.server_label = "6c rejected tip"
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e, e2 = ref(P, G, "ep-1", "One"), ref(P, G, "ep-2", "Two")
    S = fixed_op(prefix, 100)
    a1 = live.start("A", e, S, 50 * MIN, baseline=BLN("A0"), duration=D, op=fixed_op(prefix, 2))
    E1 = live.cp("A", e, S, 1, D - 20 * SEC, D - 20 * SEC - 50 * MIN, save=sv("s118", 118, "A0", first="s1"),
                 op=fixed_op(prefix, 3))
    live.clone("A", "C")
    a4 = live.fav("A", e2, True, op=fixed_op(prefix, 4))  # the original's next fact (later lost by a restore)
    r4 = live.fav("C", e2, "yes", reject=True, op=fixed_op(prefix, 5))  # the copy's rejected writing tip
    fk = live.fork("F", "C", r4, op=fixed_op(prefix, 6))  # the copy is copied again (local detection)
    f = live.fin("F", e, S, 1, a1, completed=True, op=fixed_op(prefix, 7))
    eq((live.out(r4), live.out(fk), live.out(f)), (("rejected", "invalid_payload"), ("applied", None),
                                                  ("applied", None)), "live outcomes")
    D4 = rules.derived_author(NS, r4)
    eq((live.eff(r4), live.engine.parent_author(live.dev("F")["id"])), (D4, D4),
       "the rejected tip roots a branch; the fork's parent is that branch")
    eq(L(live, e, "play_state"), {"generation_id": G, "play_state": "played"}, "the copied finalization applies")
    # Restore: the server lost a4..f. The copies upload first (epoch 2); the original
    # holder's a4 is recovered with its earlier receipt: the fork's parent moves to D(r4).
    rk = live.engine.facts
    key = lambda op: dict(zip(("epoch", "seq", "index"), rk[op]["rkey"]))  # noqa: E731
    rs = _replay(live, [(r4, {"receipt": {"epoch": 2, "seq": 1, "index": 0}}),
                        (fk, {"receipt": {"epoch": 2, "seq": 2, "index": 0}}),
                        (f, {"receipt": {"epoch": 2, "seq": 3, "index": 0}})], "regression: copies upload first",
                 prefix, lost=[a4])
    eq(rs.engine.parent_author(rs.dev("F")["id"]), rs.dev("A")["id"], "provisionally the tip holds the raw position")
    eq(rs.out(f), ("applied", None), "applied")
    am = [x for x in live.arrivals if x["fact"]["op_id"] == a4][0]
    am = json.loads(json.dumps(am))
    am["recovery"] = True
    am["receipt"] = key(a4)
    rs.accept(am)
    eq(rs.engine.parent_author(rs.dev("F")["id"]), D4, "recovery recomputes the fork's parent")
    eq(rs.engine.records(), live.engine.records(), "the same records as live")
    # Met after its fork in recovery: everything after E1 is recovered, the fork and the
    # finalization before their rejected head (their receipts stand).
    rm = _replay(live, [(fk, {"receipt": key(fk), "recovery": True, "outcome": True}),
                        (f, {"receipt": key(f), "recovery": True, "outcome": True}),
                        (a4, {"receipt": key(a4), "recovery": True, "outcome": True}),
                        (r4, {"receipt": key(r4), "recovery": True, "outcome": True})],
                 "regression: a rejected head met after its fork in recovery", prefix)
    rm.vector_ok = True
    eq(rm.engine.parent_author(rm.dev("F")["id"]), D4, "the fork's parent once its head is archived")
    eq(rm.engine.records(), live.engine.records(), "the same records as live")
    # Repeated copies preserving an ancestor's pending finalization.
    b = B("regression: repeated copies preserve an ancestor's pending finalization")
    P, G, (e, e2) = setup_listening(b, 2)
    S = b.uuid()
    a1 = b.start("A", e, S, 50 * MIN, baseline=BLN("A0"), duration=D)
    E1 = b.cp("A", e, S, 1, D - 20 * SEC, D - 20 * SEC - 50 * MIN, save=sv("s118", 118, "A0", first="s1"))
    k1 = b.fork("F1", "A", E1)
    k2 = b.fork("F2", "F1", k1)
    k3 = b.fork("F3", "F2", k2)
    f = b.fin("F3", e, S, 1, a1, completed=True)
    eq(b.out(f), ("applied", None), "the third-generation copy's finalization applies")
    check(b.engine.descends(b.dev("F3")["id"], b.dev("A")["id"]), "F3 -> F2 -> F1 -> A")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "completed")
    f0 = b.fin("A", e, S, 1, a1, completed=True)
    eq(b.out(f0), ("superseded", None), "the original's own finalization comes second (CHOICE)")


def test_copied_branches():
    """Copied branches, as vectors."""
    # A copy of a derived branch carries its pending finalization naming the
    # session's start; that means the claim its writing history holds.
    b = B("a copied branch's finalization resolves its inherited claim")
    P, G, (e,) = setup_listening(b, 1)
    s = b.uuid()
    st = b.start("A", e, s, 0, baseline=BLN("r1-base"))
    b.cp("A", e, s, 1, 300 * SEC, 300 * SEC, save=sv("r1-s3", 3, "r1-base", first="r1-s1"))
    b.clone("A", "B")
    b.cp("A", e, s, 2, 360 * SEC, 360 * SEC, save=sv("r1-a6", 6, "r1-base", first="r1-s4"))
    b.cp("B", e, s, 2, 330 * SEC, 330 * SEC, save=sv("r1-s4", 4, "r1-base", first="r1-s4", passive=True))
    bcp = b.cp("B", e, s, 3, 350 * SEC, 350 * SEC, save=sv("r1-b6", 6, "r1-b1", first="r1-b5"))
    b.fork("C", "B", bcp)
    fin = b.fin("C", e, s, 3, st)
    eq(b.out(fin), ("applied", None), "the copy's finalization")
    eq(L(b, e, "play_state"), {"generation_id": G, "play_state": "played"}, "completed")    # A copy taken before the first export diverges at the start; a later
    # copy that adds no save cannot move the original's earlier divergence.
    for copy_first in (False, True):
        b = B("a copy before the first export, %s first" % ("copy" if copy_first else "original"))
        P, G, (e,) = setup_listening(b, 1)
        s = b.uuid()
        b.start("A", e, s, 0, baseline=BLN("r1e-base"))
        b.clone("A", "B")
        b.start("D", e, b.uuid(), 900 * SEC)
        t = b.now
        mk_a = lambda rt: b.cp("A", e, s, 1, 20 * SEC, 20 * SEC, save=sv("r1e-a1", 1, "r1e-a", first="r1e-a1"),  # noqa: E731
                               t=t + 3000, rt=rt)
        mk_b = lambda rt: b.cp("B", e, s, 1, 10 * SEC, 10 * SEC, save=sv("r1e-b1", 1, "r1e-b", first="r1e-b1"),  # noqa: E731
                               t=t + 2000, rt=rt)
        if copy_first:
            mk_b(t + 5000)
            acp = mk_a(t + 6000)
        else:
            acp = mk_a(t + 5000)
            mk_b(t + 6000)
        eq(L(b, e, "position"), posv(G, 20 * SEC, acp), "the original's reloaded listening claims")
        b.clone("A", "C")
        b.cp("A", e, s, 2, 30 * SEC, 30 * SEC, save=sv("r1e-a2", 2, "r1e-a", first="r1e-a2"))
        b.end("C", e, s, 2, 20 * SEC, 20 * SEC, reason="suspended", activity=False, history=False,
              save=sv("r1e-a1", 1, "r1e-a", passive=True))
        eq(L(b, e, "position"), posv(G, 30 * SEC, acp), "the later copy changes nothing")    # A queue-less start ordered before a completion it supersedes needs no
    # resolved add, whichever arrives first.
    for start_first in (False, True):
        b = B("a queue-less start before a completion it supersedes, %s first"
              % ("start" if start_first else "completion"))
        P, G, (e, x) = setup_listening(b, 2)
        ke = K(None, None, "r1q/e")
        b.qadd("A", [(e, ke), (x, K(ke[0], None, "r1q/x"))])
        s = b.uuid()
        st = b.start("A", e, s, 0, caps=NOQ)
        b.cp("A", e, s, 1, 300 * SEC, 300 * SEC, caps=NOQ)
        b.clone("A", "B")
        t = b.now
        mk_s = lambda rt: b.start("B", e, b.uuid(), 300 * SEC, caps=NOQ, t=t + 1000, rt=rt)  # noqa: E731
        mk_f = lambda rt: b.fin("A", e, s, 1, st, caps=NOQ, t=t + 2000, rt=rt)  # noqa: E731
        if start_first:
            bs = mk_s(t + 5000)
            mk_f(t + 6000)
        else:
            mk_f(t + 5000)
            bs = mk_s(t + 6000)
        check(b.decision(rules.RESOLVED_ADD, bs) is None, "no add resolved")
        eq(b.queue(), ["Episode 1", "Episode 2"], "the episode stays where it was")


def test_determinism():
    """PROTOCOL §6.10: arrival permutations of facts with fixed receipts give identical records."""
    rng = random.Random(6006)
    prefix = B.count + 1
    for detection in ("fallback", "local"):
        steps = s33_steps(detection, "base")
        order = merge_orders(list(steps.values()))[0]
        live, _ = s33_case("determinism source (%s)" % detection, prefix, detection, "base", order)
        live.vector_ok = False
        want = live.engine.records()
        outcomes = dict(live.engine.outcomes)
        arrivals = []
        for a in live.arrivals:
            a = json.loads(json.dumps(a))
            a["receipt"] = dict(zip(("epoch", "seq", "index"), live.engine.facts[a["fact"]["op_id"]]["rkey"]))
            arrivals.append(a)
        for k in range(30):
            perm = linear_extension(rng, arrivals, rng.randint(0, 2))
            eng = rules.Engine(ACC, NS, live.engine.resolutions, live.engine.alias_decisions(),
                               live.engine.episode_decisions(), replay=True, fixed_outcomes=outcomes)
            for a in perm:
                eng.accept(a)
            eq(eng.records(), want, "copy permutation %d (%s)" % (k, detection))
            eq(eng.records(), eng.full_records(), "replay projection vs full recompute")


# ---------------------------------------------------------------------------
# PROTOCOL §9.4: catalog, discovery and server refresh
# ---------------------------------------------------------------------------

DAY = 86400000


def subject(G, r):
    return "admission:%s:%s" % (G, r["episode_id"])


def ADM(b, G, r):
    """The effective admission decision of an episode in a generation, or None."""
    return b.decision(rules.ADMISSION, subject(G, r))


def PS(b, r):
    v = b.rec("episode", r["episode_id"], "play_state")
    return v["play_state"] if v is not None else None


def setup_catalog(b, policy=None, old=True, url=U1, dev="A"):
    """`dev` subscribes; the server's first fetch (its first population) sees one older
    episode. Returns (P, G, subscribe op, the older episode's ref)."""
    s = b.subscribe(dev, url, policy=policy)
    P, G = pg(s)
    o = ref(P, G, "old", "Old")
    b.fetch(P, [(o, ifields("Old", T0 - DAY))] if old else [])
    return P, G, s, o


def test_payloads_and_outcomes():
    b = B("7b: proposal outcomes and payload rejections")
    P, G, s, old = setup_catalog(b)
    newest = T0 - DAY
    new, back, co = ref(P, G, "new", "New"), ref(P, G, "back", "Back"), ref(P, G, "co", "Co")
    x = b.discover("B", new, pub=T0, newest=newest)
    eq(b.out(x), ("applied", None), "an eligible playable discovery is admitted")
    adm = ADM(b, G, new)
    eq((adm["inputs"], adm["result"], adm["dependencies"]),
       ({"found_by": x}, {"episode_id": new["episode_id"], "generation_id": G, "action": "inbox", "proposal_id": x,
                          "source": b.dev("B")["id"]}, [x]), "the admission decision")
    eq(L(b, new, "admission"), {"generation_id": G, "admission_id": adm["decision_id"], "proposal_id": x,
                                "source": b.dev("B")["id"], "action": "inbox"}, "the admission record")
    eq(PS(b, new), "new", "filed into the Inbox at the proposal's place")
    eq(b.engine._fold_episode_scope(new["episode_id"], G).det["play_state"], x, "the proposal is the filing's origin")
    y = b.discover("C", new, action="queue", bundle=(K(None, None, "po/new"), END), pub=T0, newest=newest)
    eq(b.out(y), ("not_applied", "already_admitted"), "a later proposal")
    check(MEM(b, new) is None and len(b.made(rules.ADMISSION)) == 1, "it never re-files")
    z = b.discover("D", back, pub=T0 - 3 * DAY, newest=newest)
    eq(b.out(z), ("not_applied", "ineligible"), "an older playable item without evidence (backfill)")
    check(L(b, back, "admission") is None and PS(b, back) is None, "an ineligible discovery files nothing")
    eq(L(b, back, "metadata")["title"], "Back", "but its observation is catalog evidence")
    o = b.observe("E", P, [(co, None, ifields("Co", T0 - 9 * DAY, media=False))])
    w = b.propose("E", co, pub=T0 - 9 * DAY, newest=newest)
    eq(b.out(w), ("not_applied", "not_playable"), "a catalog-only item")
    g0 = b.discover("F", co, fields=ifields("Co", T0 - 9 * DAY), newest=newest)
    eq(b.out(g0), ("not_applied", "ineligible"), "the enclosure appeared, but no evidence")
    g1 = b.propose("F", co, pub=T0 - 9 * DAY, newest=newest, gained=x)
    eq(b.out(g1), ("not_applied", "ineligible"), "evidence that is not a catalog-only observation of it")
    g2 = b.propose("F", co, pub=T0 - 9 * DAY, newest=newest, gained=o)
    eq(b.out(g2), ("applied", None), "gained media with its catalog-only observation as evidence")
    eq(PS(b, co), "new", "the gained-media release is filed")
    nd = b.discover("F", ref(P, G, "nodate", "No date"), pub=None, newest=newest,
                    fields=ifields("No date", None))
    eq(b.out(nd), ("applied", None), "a missing publication date passes the date rule")
    nb = b.discover("F", ref(P, G, "nobase", "No newest"), pub=T0 - 30 * DAY, newest=None)
    eq(b.out(nb), ("applied", None), "CHOICE: a missing newest_before_batch passes the date rule")
    eq_ = b.discover("F", ref(P, G, "same", "Same"), pub=newest, newest=newest)
    eq(b.out(eq_), ("applied", None), "a date equal to the newest is not before it (Android)")
    # Not subscribed: archived, and a preview.
    b.pod("A", "archive", P, G)
    a1 = b.discover("B", ref(P, G, "arch", "Arch"), pub=T0 + DAY, newest=newest)
    eq(b.out(a1), ("not_applied", "not_subscribed"), "archived")
    b.pod("A", "restore", P, G)
    rv = b.preview("A", U2)
    Q, GQ = pg(rv)
    pv = b.discover("B", ref(Q, GQ, "pv", "Pv"), pub=T0, newest=None)
    eq(b.out(pv), ("not_applied", "not_subscribed"), "a preview")
    # PROTOCOL §7.2 checks come first.
    ghost = c.podcast_id(NS, b.uuid())
    gr = ref(ghost, c.generation_id(NS, ghost, b.uuid()), "g")
    eq(b.out(b.propose("B", gr)), ("not_applied", "unknown_podcast"), "unknown podcast")
    eq(b.out(b.propose("B", ref(P, c.generation_id(NS, P, b.uuid()), "ug"))), ("not_applied", "unknown_generation"),
       "unknown generation")
    mm = dict(ref(Q, GQ, "old"), episode_id=old["episode_id"])
    eq(b.out(b.propose("B", mm)), ("not_applied", "episode_podcast_mismatch"), "another podcast's episode")
    eq(b.out(b.observe("B", ghost, [(gr, None, ifields("G"))])), ("not_applied", "unknown_podcast"),
       "an observation of an unknown podcast")
    eq(b.out(b.observe("B", Q, [(mm, None, ifields("X"))])), ("not_applied", "episode_podcast_mismatch"),
       "an observation whose only item is bound to another podcast")
    mixed = b.observe("B", Q, [(mm, None, ifields("X")), (ref(Q, GQ, "q1", "Q1"), None, ifields("Q1"))])
    eq(b.out(mixed), ("applied", None), "a mismatched item is skipped like a bulk member")
    # Rejections (invalid_payload unless noted).
    rej = lambda op, what, reason="invalid_payload": eq(b.out(op), ("rejected", reason), what)  # noqa: E731
    rej(b.observe("H", P, [(new, None, ifields("New", T0))], reject=True,
                  patch=lambda f: f["targets"][0].__setitem__("generation", G)), "observe targets with a generation")
    rej(b.observe("H", P, [(ref(Q, GQ, "q2"), None, ifields("Q2"))], reject=True), "an item of another podcast")
    rej(b.observe("H", P, [(new, None, ifields("New", T0))], reject=True,
                  patch=lambda f: f["payload"]["items"][0]["fields"].__setitem__("pub_date", "yesterday")),
        "a malformed record value")
    rej(b.observe("H", P, [(new, None, dict(ifields("New", T0), extra=1))], reject=True), "an unknown record member")
    rej(b.observe("H", P, [], reject=True), "an observation without records")
    eq(b.out(b.observe("H", P, [(ref(P, G, "nb", "No base"), "absent", ifields("No base", T0))])), ("applied", None),
       "base is optional: absent means no revision held")
    eq(b.out(b.observe("H", P, [], feed=(None, pfields("Show")))), ("applied", None), "a feed record with no items")
    rej(b.observe("H", P, [(new, "not-a-node", ifields("New", T0))], reject=True), "a base that is not a node ID")
    rej(b.propose("H", ref(P, G, "b1"), action="inbox", bundle=(K(None, None, "rej/b1"), END), reject=True),
        "a bundle without the action queue")
    rej(b.propose("H", ref(P, G, "b2"), reject=True,
                  patch=lambda f: f["payload"].__setitem__("generation_id", GQ)), "generation_id not the EpisodeRef's")
    rej(b.propose("H", ref(P, G, "b3"), reject=True, patch=lambda f: f["payload"].__setitem__("action", "global")),
        "an unresolved action")
    rej(b.propose("H", ref(P, G, "b4"), reject=True,
                  patch=lambda f: f["payload"].__setitem__("gained_media", {"catalog_only_observation": "x"})),
        "malformed gained-media evidence")
    rej(b.propose("W", ref(P, G, "b5"), action="queue", bundle=(K(None, None, "rej/b5"), END), caps=NOQ, reject=True),
        "a bundle from a client without the queue capability", "capability_not_declared")
    expect_input_error(lambda: b.observe("H", P, [(new, None, ifields("New", T0, url="https://u:p@cdn.example.org/a"))]),
                       "userinfo in a record URL")
    # The server strips URL userinfo from a fetch's result, in URLs
    # and embedded in text.
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (new, ifields("New", T0, url="https://u:p@cdn.example.org/a",
                                                                description="See https://u:p@x.example/y"))])
    eq((L(b, new, "metadata")["media"]["url"], L(b, new, "metadata")["description"]),
       ("https://cdn.example.org/a", "See https://x.example/y"), "userinfo stripped from the parsed result")


def test_admission_orders():
    import itertools
    # (a) Eligible vs first population (not sent): B's first population observes X and
    # proposes nothing; C proposes X (queue); the server's fetch discovers it (inbox).
    for order in itertools.permutations(["first_pop", "device", "server"]):
        b = B("7b: eligible vs first population, order %s" % ", ".join(order))
        P, G, s, old = setup_catalog(b)
        xr = ref(P, G, "x", "X")
        base = b.now
        kx = K(None, None, "orda/x")
        steps = {
            "first_pop": lambda rt: b.observe("B", P, [(xr, None, ifields("X", T0))], t=base + 100, rt=rt, after=[s]),
            "device": lambda rt: b.discover("C", xr, action="queue", bundle=(kx, END), pub=T0, newest=T0 - DAY,
                                            t=base + 200, rt=rt, after=[s], base=None),
            "server": lambda rt: b.fetch(P, [(old, ifields("Old", T0 - DAY)), (xr, ifields("X", T0))], rt=rt),
        }
        ops = {}
        for i, step in enumerate(order):
            ops[step] = steps[step](base + 1000 * (i + 1))
        adms = b.made(rules.ADMISSION)
        eq(len(adms), 1, "%s: admitted exactly once" % b.name)
        device_first = order.index("device") < order.index("server")
        a = ADM(b, G, xr)
        if device_first:
            eq((a["result"]["source"], b.out(ops["device"])), (b.dev("C")["id"], ("applied", None)), b.name)
            eq((MEM(b, xr), b.keyof(xr), PS(b, xr)), (mem(G, True), kx, None), "%s: filed as C's proposal says" % b.name)
        else:
            eq(a["result"]["source"], "server", b.name)
            eq(b.out(ops["device"]), ("not_applied", "already_admitted"), b.name)
            eq((MEM(b, xr), PS(b, xr)), (None, "new"), "%s: filed as the server's discovery says" % b.name)
        check(b.out(ops["first_pop"])[0] in ("applied", "superseded"), "the first population's observation")
    # (b) Eligible vs eligible: A (inbox), B (queue) and the server (policy: nothing).
    for order in itertools.permutations(["A", "B", "server"]):
        b = B("7b: eligible vs eligible, order %s" % ", ".join(order))
        P, G, s, old = setup_catalog(b)
        pol = b.pod("A", "set_policy", P, G, {"new_episodes_action": "nothing"})
        xr = ref(P, G, "x", "X")
        base = b.now
        kb = K(None, None, "ordb/x")
        steps = {
            "A": lambda rt: b.discover("DA", xr, action="inbox", pub=T0, newest=T0 - DAY, t=base + 100, rt=rt,
                                       after=[pol], base=None),
            "B": lambda rt: b.discover("DB", xr, action="queue", bundle=(kb, END), pub=T0, newest=T0 - DAY,
                                       t=base + 200, rt=rt, after=[pol], base=None),
            "server": lambda rt: b.fetch(P, [(old, ifields("Old", T0 - DAY)), (xr, ifields("X", T0))], rt=rt),
        }
        ops = {}
        for i, step in enumerate(order):
            ops[step] = steps[step](base + 1000 * (i + 1))
        first = order[0]
        eq(len(b.made(rules.ADMISSION)), 1, "%s: admitted once" % b.name)
        a = ADM(b, G, xr)["result"]
        want_action = {"A": "inbox", "B": "queue", "server": "nothing"}[first]
        eq(a["action"], want_action, "%s: the first admitted proposal's action" % b.name)
        eq(L(b, xr, "admission")["action"], want_action, "the record")
        eq((PS(b, xr), MEM(b, xr)), {"A": ("new", None), "B": (None, mem(G, True)), "server": (None, None)}[first],
           "%s: its filing" % b.name)
        for dev in ("A", "B"):
            want = ("applied", None) if dev == first else ("not_applied", "already_admitted")
            eq(b.out(ops[dev]), want, "%s: %s's receipt" % (b.name, dev))
    # (c) Gained media: Co is catalog-only (old date); its enclosure appears. A proposes
    # with its earlier catalog-only observation as evidence, B without (ineligible, or
    # not_playable if nothing has observed the enclosure yet), the server's history knew
    # it as catalog-only.
    for order in itertools.permutations(["A", "B", "server"]):
        b = B("7b: gained media, order %s" % ", ".join(order))
        s = b.subscribe("Z", U1)
        P, G = pg(s)
        old, co = ref(P, G, "old", "Old"), ref(P, G, "co", "Co")
        o = b.observe("E", P, [(co, None, ifields("Co", T0 - 5 * DAY, media=False))])
        b.fetch(P, [(old, ifields("Old", T0 - DAY)), (co, ifields("Co", T0 - 5 * DAY, media=False))])
        base = b.now
        f = ifields("Co", T0 - 5 * DAY)
        steps = {
            "A": lambda rt: b.discover("DA", co, fields=f, newest=T0 - DAY, gained=o, t=base + 100, rt=rt,
                                       after=[o], base=o),
            "B": lambda rt: b.propose("DB", co, pub=T0 - 5 * DAY, newest=T0 - DAY, t=base + 200, rt=rt, after=[o]),
            "server": lambda rt: b.fetch(P, [(old, ifields("Old", T0 - DAY)), (co, f)], rt=rt),
        }
        ops = {}
        for i, step in enumerate(order):
            ops[step] = steps[step](base + 1000 * (i + 1))
        winner = [x for x in order if x != "B"][0]
        a = ADM(b, G, co)["result"]
        eq(a["source"], b.dev("DA")["id"] if winner == "A" else "server", "%s: the first eligible" % b.name)
        eq(PS(b, co), "new", "%s: filed once" % b.name)
        b_first = order[0] == "B"
        eq(b.out(ops["B"]), ("not_applied", "not_playable" if b_first else "ineligible"), "%s: B" % b.name)
        eq(b.out(ops["A"]), ("applied", None) if winner == "A" else ("not_applied", "already_admitted"),
           "%s: A" % b.name)


def test_observation_without_proposal_then_fetch():
    b = B("7b: an observation without its proposal, then the server's fetch")
    P, G, s, old = setup_catalog(b)
    xr = ref(P, G, "x", "X")
    ob = b.observe("B", P, [(xr, None, ifields("X", T0))])
    eq(b.out(ob), ("applied", None), "the observation")
    check(ADM(b, G, xr) is None and PS(b, xr) is None, "an observation alone files nothing")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (xr, ifields("X", T0))])
    parts, adms = b.made(rules.CATALOG_REFRESH, n), b.made(rules.ADMISSION, n)
    eq((len(parts), len(adms)), (1, 1), "part 0 (with the status) and the server's admission")
    eq(parts[0]["result"]["items"], [], "the item was unchanged: no record written")
    eq((adms[0]["inputs"], adms[0]["dependencies"]), ({"found_by": parts[0]["decision_id"]}, [parts[0]["decision_id"]]),
       "CHOICE: an unchanged item's admission is found by part 0")
    eq(adms[0]["result"]["source"], "server", "the server discovers against its own fetch history")
    eq(PS(b, xr), "new", "the release is not stranded")
    eq(b.engine._fold_episode_scope(xr["episode_id"], G).det["play_state"], adms[0]["decision_id"],
       "the server's admission is its own filing place")
    # A later device proposal of it is already admitted.
    eq(b.out(b.propose("C", xr, pub=T0, newest=T0 - DAY)), ("not_applied", "already_admitted"), "later")
    # An older item new to the server's history is backfill; Android's dates.
    n = len(b.engine.created)
    bk = ref(P, G, "bk", "Backfill")
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (xr, ifields("X", T0)), (bk, ifields("Backfill", T0 - 2 * DAY))])
    eq(b.made(rules.ADMISSION, n), [], "backfill against the newest effective date the server saw (X's)")


BARRIER_KINDS = ("mark_played", "mark_unplayed", "reset_position", "restore_unplayed", "dismiss", "queue_add",
                 "queue_restore", "start", "listening", "favorite")


def apply_barrier(b, kind, e, t, after):
    if kind == "mark_played":
        return b.mark_played("B", e, t=t, after=after)
    if kind in ("mark_unplayed", "reset_position"):
        return b.ep("B", "episode." + kind, e, t=t, after=after)
    if kind == "restore_unplayed":
        return b.restore("B", e, "unplayed", 0, t=t, after=after)
    if kind == "dismiss":
        return b.dismiss("B", [e], t=t, after=after)
    if kind == "queue_add":
        return b.qadd("B", [(e, K(None, None, "bar/add"))], t=t, after=after)
    if kind == "queue_restore":
        return b.qrestore("B", [(e, K(None, None, "bar/restore"))], t=t, after=after)
    if kind == "start":
        return b.start("B", e, b.uuid(), 0, activity=False, t=t, after=after)
    if kind == "listening":
        sid = b.uuid()
        b.start("B", e, sid, 0, activity=True, t=t, after=after)
        return b.cp("B", e, sid, 1, MIN, MIN, activity=True, t=t)
    return b.fav("B", e, True, t=t, after=after)


def test_inbox_filing_barriers():
    for source in ("device", "server"):
        for kind in BARRIER_KINDS:
            for when in ("before", "after"):
                b = B("7b: Inbox filing by the %s against %s, barrier %s" % (source, kind, when))
                P, G, s, old = setup_catalog(b)
                e = ref(P, G, "e", "E")
                base = b.now

                def file():
                    if source == "device":
                        return b.discover("D", e, pub=T0, newest=T0 - DAY, t=base + 1000, after=[s], base=None)
                    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (e, ifields("E", T0))])
                    return ADM(b, G, e)["decision_id"]
                if when == "after":
                    place = file()
                    apply_barrier(b, kind, e, base + 5000, [s])
                else:
                    apply_barrier(b, kind, e, base + 50, [s])
                    place = file()
                check(ADM(b, G, e) is not None, "%s: admitted (the filing is used up)" % b.name)
                want = {"favorite": "new", "mark_played": "played"}.get(kind, "unplayed")
                eq(PS(b, e), want, b.name)
                st = b.engine._fold_episode_scope(e["episode_id"], G).status.get(place)
                eq(st, "fired" if kind == "favorite" else "ineffective:barrier", "%s: the filing at its place" % b.name)


def test_queue_filing():
    qpol = {"new_episodes_action": "queue", "keep_updated": True}
    # Against explicit membership, by a device and by the server, in both orders, with
    # keep-sorted on: a filing that does not apply is not placed (a ghost at most, which
    # never moves an entry) and never sorts.
    for source in ("device", "server"):
        for kind in ("remove", "add"):
            for when in ("before", "after"):
                b = B("7b: queue filing by the %s vs an explicit %s %s it" % (source, kind, when))
                P, G, s, old = setup_catalog(b, policy=qpol)
                m = ref(P, G, "m", "M")
                b.qadd("B", [(m, K(None, None, "qf/m"))], place=END)
                b.qpolicy("B", keep_sorted=True, sort_order="episode_title_a_z")
                e = ref(P, G, "e", "E")
                kx, kd = K(None, None, "qf/explicit"), K(None, None, "qf/device")

                def explicit():
                    if kind == "remove":
                        return b.qremove("B", [e])
                    return b.qadd("B", [(e, kx)], place=END)

                def file():
                    if source == "device":
                        return b.discover("D", e, action="queue", bundle=(kd, END), pub=T0, newest=T0 - DAY, base=None)
                    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (e, ifields("E", T0))])
                    return ADM(b, G, e)["decision_id"]
                if when == "before":
                    explicit()
                    place = file()
                else:
                    place = file()
                    eq(b.queue(), ["E", "M"], "%s: the applied filing is sorted at its place" % b.name)
                    explicit()
                st = b.engine._fold_episode_scope(e["episode_id"], G).status.get(place)
                eq(st, "ineffective:explicit_state", "%s: the automatic add loses, whatever the order" % b.name)
                if kind == "remove":
                    eq((MEM(b, e), b.queue()), (mem(G, False, "user"), ["M"]), b.name)
                else:
                    eq((MEM(b, e), b.queue()), (mem(G, True), ["E", "M"]),
                       "%s: the explicit add places E (at the end), then sorts" % b.name)
    # A device's filing under keep-sorted: its bundle's place (the end), then the replay
    # sorts at the proposal's place (bundle moves are invalid).
    b = B("7b: a device's queue filing under keep-sorted")
    P, G, s, old = setup_catalog(b, policy=qpol)
    ea, ez = ref(P, G, "a", "A"), ref(P, G, "z", "Z")
    ka, kz = keys_in_order(["qs/a", "qs/z"])
    b.qadd("B", [(ea, ka), (ez, kz)], place=END)
    b.qpolicy("B", keep_sorted=True, sort_order="episode_title_a_z")
    b.qsort("B", "episode_title_a_z")
    eb = ref(P, G, "b", "B")
    kb = K(ka[0], kz[0], "qs/b")
    p = b.discover("D", eb, action="queue", bundle=(kb, END), pub=T0, newest=T0 - DAY)
    eq(b.out(p), ("applied", None), "admitted")
    eq(b.queue(), ["A", "B", "Z"], "inserted at its place, then sorted")
    a = ADM(b, G, eb)
    eq((a["result"]["key"], a["result"]["place"]), (kb[0], END), "the admission records the bundle's key and place")
    eq(sorted(v["values"]["title"] for v in a["result"]["values"]), ["A", "B", "Z"],
       "and the values of the queue then (it is a sort trigger at its place)")
    eq(b.out(b.propose("D2", eb, action="queue", bundle=(K(None, None, "qs/b2"), END), pub=T0, newest=T0 - DAY)),
       ("not_applied", "already_admitted"), "a second proposal's bundle never applies")
    # A client without the queue capability: the server resolves the placement once.
    for loc, draw, want, place in (("back", None, ["M1", "M2", "New"], END), ("front", None, ["New", "M1", "M2"], START),
                                   ("after_current", None, ["M1", "M2", "New"], END),
                                   ("random", 1, ["M1", "New", "M2"], "M1")):
        b = B("7b: a queue-less proposal, enqueue location %s" % loc)
        P, G, s, old = setup_catalog(b)
        m1, m2 = ref(P, G, "m1", "M1"), ref(P, G, "m2", "M2")
        b.qadd("B", list(zip([m1, m2], keys_in_order(["ql/%s/1" % loc, "ql/%s/2" % loc]))), place=END)
        b.qpolicy("B", enqueue_location=loc)
        if draw is not None:
            b.draws = [draw]
        nw = ref(P, G, "new", "New")
        p = b.discover("W", nw, action="queue", pub=T0, newest=T0 - DAY, caps=NOQ)
        a = ADM(b, G, nw)
        eq(a["inputs"], {"found_by": p, "enqueue_location": loc}, "the resolution is recorded")
        if place == "M1":
            place = {"kind": "after", "after": {"episode_id": m1["episode_id"], "generation_id": G}}
        eq(a["result"]["place"], place, "the resolved place")
        eq(b.queue(), want, "resolved at %s" % loc)
        eq(b.keyof(nw), (a["result"]["key"], a["result"]["position_id"]), "the resolved key fits: the layout keeps it")
    b = B("7b: a queue-less proposal under keep-sorted")
    P, G, s, old = setup_catalog(b)
    m1, m2 = ref(P, G, "m1", "Beta"), ref(P, G, "m2", "Delta")
    b.qadd("B", list(zip([m1, m2], keys_in_order(["qk/1", "qk/2"]))), place=END)
    b.qpolicy("B", keep_sorted=True, sort_order="episode_title_a_z")
    nw = ref(P, G, "new", "Alpha")
    p = b.discover("W", nw, action="queue", pub=T0, newest=T0 - DAY, caps=NOQ)
    eq(b.queue(), ["Alpha", "Beta", "Delta"], "the resolved add is sorted at the proposal's place")
    # Several server admissions in one fetch: each is its own place, in the fetch's item
    # order (CHOICE: each depends on the fetch's previous queue admission, so the replay
    # keeps that order), sorted under keep-sorted; one layout ends the commit.
    for keep in (True, False):
        b = B("7b: several server admissions in one fetch, keep-sorted %s" % ("on" if keep else "off"))
        P, G, s, old = setup_catalog(b, policy=qpol)
        mm = ref(P, G, "mike", "Mike")
        b.qadd("B", [(mm, K(None, None, "qm/m"))], place=END)
        if keep:
            b.qpolicy("B", keep_sorted=True, sort_order="episode_title_a_z")
        items = [(ref(P, G, g, t), ifields(t, T0 + i)) for i, (g, t) in enumerate([("zed", "Zed"), ("alpha", "Alpha"),
                                                                                     ("kilo", "Kilo")])]
        n = len(b.engine.created)
        b.fetch(P, [(old, ifields("Old", T0 - DAY))] + items)
        made = b.engine.created[n:]
        seq = [d["decision_type"] for d in sorted(made, key=lambda d: d["index"])]
        # PROTOCOL §7.6: a layout only when the published keys do not give
        # the replay's order: the resolved keys fit an unsorted queue; under keep-sorted the
        # sort at the filings' places moves them.
        eq(seq, [rules.CATALOG_REFRESH] + [rules.ADMISSION] * 3 + ([rules.LAYOUT] if keep else []),
           "part 0, the admissions, and a layout at the commit's end only when needed")
        adms = [d for d in sorted(made, key=lambda d: d["index"]) if d["decision_type"] == rules.ADMISSION]
        eq([a["result"]["episode_id"] for a in adms], [r["episode_id"] for r, _ in items], "in the fetch's item order")
        check(all(adms[i]["decision_id"] in adms[i + 1]["dependencies"] for i in range(2)), "chained")
        eq([d["index"] for d in sorted(made, key=lambda d: d["index"])], list(range(len(made))),
           "CHOICE: a server commit's decisions take indices 0, 1, ...")
        eq(b.queue(), ["Alpha", "Kilo", "Mike", "Zed"] if keep else ["Mike", "Zed", "Alpha", "Kilo"],
           "sorted" if keep else "at the end, in the fetch's item order")


def test_replacement_rule():
    b = B("7b: the canonical replacement rule")
    P, G, s, old = setup_catalog(b)
    x = ref(P, G, "x", "X")
    meta = lambda r: L(b, r, "metadata")  # noqa: E731
    a1 = b.observe("A", P, [(x, None, ifields("v1"))])
    fa = meta(x)["fetch_time"]
    eq((meta(x)["title"], meta(x)["revision"]), ("v1", a1), "no record: replaces")
    b1 = b.observe("B", P, [(x, a1, ifields("v2"))], fetch_time=fa - 10000)
    eq((meta(x)["title"], b.out(b1)), ("v2", ("applied", None)), "the base is the current revision: replaces, "
                                                                   "even with an earlier fetch time")
    c1 = b.observe("C", P, [(x, a1, ifields("v3"))], fetch_time=fa - 20000)
    eq((meta(x)["title"], b.out(c1)), ("v2", ("applied", None)),
       "concurrent and earlier: does not replace (the receipt is applied)")
    d1 = b.observe("D", P, [(x, None, ifields("v4"))], fetch_time=fa - 5000)
    eq((meta(x)["title"], meta(x)["revision"]), ("v4", d1), "concurrent and later: replaces")
    e1 = b.observe("E", P, [(x, d1, ifields("v5"))], fetch_time=b.now + DAY)
    eq((meta(x)["title"], meta(x)["fetch_time"]), ("v5", b.nt(e1)), "a future fetch time is clamped")
    # The same receive time: both fetch times clamp to it; F's larger op ID orders it last.
    f1 = b.observe("F", P, [(x, a1, ifields("v6"))], fetch_time=b.now + 2 * DAY, rt=b.nt(e1))
    check(b.nt(f1) == b.nt(e1) and b.engine.rank[f1] > b.engine.rank[e1], "ordered after, at the same time")
    eq(meta(x)["title"], "v5", "CHOICE: equal (clamped) fetch times do not replace")
    # A delayed observation ordered before the current one is folded at its place.
    g1 = b.observe("G", P, [(x, None, ifields("v7"))], fetch_time=fa - 1000, t=fa - 1000, after=[s])
    check(b.engine.rank[g1] < b.engine.rank[d1], "ordered before")
    eq(meta(x)["title"], "v5", "folded in rank order: v4 (later) replaces it, then v5")
    # Server vs device: a fetch captures its bases before its I/O.
    y, z = ref(P, G, "y", "Y"), ref(P, G, "z", "Z")
    b.observe("A", P, [(y, None, ifields("y0")), (z, None, ifields("z0"))])
    y0, z0 = b.revision(y["episode_id"]), b.revision(z["episode_id"])
    R = b.fetch_start(P)
    t0 = b.now
    dy = b.observe("H", P, [(y, y0, ifields("y-device")), (z, z0, ifields("z-device"))], fetch_time=t0 + 500)
    n = len(b.engine.created)
    # The server's response for y arrived before H's fetch, its whole fetch after.
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("v5")), (y, ifields("y-server")),
                (z, ifields("z-server"))], R=R, fetch_time=t0 + 400, rt=b.now + 1000)
    part = b.made(rules.CATALOG_REFRESH, n)[0]
    eq({i["episode_id"]: i["base"] for i in part["result"]["items"]}, {y["episode_id"]: y0, z["episode_id"]: z0},
       "the part names the revisions captured at the fetch's start")
    check(set([y0, z0]) <= set(part["dependencies"]) and dy not in part["dependencies"],
          "and depends on them, not on what it raced with")
    eq((meta(y)["title"], meta(z)["title"]), ("y-device", "z-device"),
       "the device fetched more recently: the server's concurrent fetch does not win")
    R = b.fetch_start(P)
    t1 = b.now
    b.observe("H", P, [(y, dy, ifields("y-device2"))], fetch_time=t1 + 100)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("v5")), (y, ifields("y-server2")),
                (z, ifields("z-device"))], R=R, fetch_time=t1 + 900, rt=b.now + 1000)
    eq(meta(y)["title"], "y-server2", "the server's fetch succeeded more recently: it wins")
    # A fresh server fetch names the current revision: it replaces whatever the times.
    dz = b.observe("H", P, [(z, b.revision(z["episode_id"]), ifields("z-device3"))])
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("v5")), (y, ifields("y-server2")),
                (z, ifields("z-server3"))], fetch_time=b.nt(dz) - 3000, rt=b.now + 5000)
    eq((meta(z)["title"], meta(z)["fetch_time"]), ("z-server3", b.nt(dz) - 3000), "base wins over fetch time")
    # An unchanged fetch writes nothing (its status is not due).
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("v5")), (y, ifields("y-server2")),
                (z, ifields("z-server3"))])
    eq(b.engine.created[n:], [], "an unchanged successful fetch writes nothing to the log")
    # Re-parse (PROTOCOL §9.6): the original base and fetch time.
    b = B("7b: re-parse with the original base and fetch time")
    P, G, s, old = setup_catalog(b)
    u, w = ref(P, G, "u", "U"), ref(P, G, "w", "W")
    R1 = b.fetch(P, [(old, ifields("Old", T0 - DAY)), (u, ifields("u1", T0)), (w, ifields("w1", T0))])
    p1 = b.revision(u["episode_id"])
    tf = L(b, u, "metadata")["fetch_time"]
    dw = b.observe("D", P, [(w, p1, ifields("w-device", T0))])
    status = b.rec("podcast", P, "refresh_status")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (u, ifields("u1-reparsed", T0)), (w, ifields("w1-reparsed", T0))],
            outcome="re-parse", reparse_of=R1)
    rp = b.made(rules.CATALOG_REFRESH, n)
    eq(len(rp), 1, "one re-parse part")
    eq((rp[0]["inputs"]["outcome"], rp[0]["inputs"]["fetch_time"], rp[0]["inputs"]["generation_id"]),
       ("re-parse", tf, None), "the original fetch time")
    eq(sorted(i["base"] for i in rp[0]["result"]["items"]), [p1, p1], "the revision the original fetch wrote")
    check("status" not in rp[0]["result"] and "population" not in rp[0]["result"], "no status, no marker")
    eq((L(b, u, "metadata")["title"], L(b, u, "metadata")["fetch_time"]), ("u1-reparsed", tf),
       "it replaces its own still-current observation, keeping the fetch time")
    eq((L(b, w, "metadata")["title"], L(b, w, "metadata")["revision"]), ("w-device", dw),
       "a newer device correction stands")
    eq(b.rec("podcast", P, "refresh_status"), status, "refresh_status unchanged")
    eq(b.made(rules.ADMISSION, n) + b.made(rules.MOVED, n), [], "a re-parse never admits or moves")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (u, ifields("u1-reparsed2", T0)), (w, ifields("w1-reparsed", T0))],
            outcome="re-parse", reparse_of=R1)
    eq(L(b, u, "metadata")["title"], "u1-reparsed2",
       "CHOICE: a second re-parse names the first re-parse's revision, and replaces it")
    eq(sorted(i["episode_id"] for p in b.made(rules.CATALOG_REFRESH, n) for i in p["result"]["items"]),
       sorted([u["episode_id"], w["episode_id"]]),
       "records that differ from canonical are written (w again, which a device corrected)")
    eq(L(b, w, "metadata")["title"], "w-device", "and the device correction still wins by fetch time")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (u, ifields("u1-reparsed2", T0)), (w, ifields("w-device", T0))],
            outcome="re-parse", reparse_of=R1)
    eq(b.engine.created[n:], [], "nothing differs from canonical: nothing written")
    R2 = b.fetch(P, [(old, ifields("Old", T0 - DAY)), (u, ifields("u1-reparsed2", T0)), (w, ifields("w-device", T0))])
    expect_input_error(lambda: b.fetch(P, [], outcome="re-parse", reparse_of=R1), "only the stored response")
    check(R2 != R1, "a newer stored response")


def test_closed_generations_and_preview():
    # Preview to subscribe: the preview's fetches were the generation's first population.
    b = B("7b: preview, its first population, then subscribe")
    r = b.preview("A", U1)
    P, G = pg(r)
    old, x = ref(P, G, "old", "Old"), ref(P, G, "x", "X")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY))])
    eq(b.made(rules.CATALOG_REFRESH, n)[0]["result"].get("population"), True, "the preview's first population")
    pv = b.discover("B", x, pub=T0, newest=T0 - DAY)
    eq(b.out(pv), ("not_applied", "not_subscribed"), "a preview files nothing")
    sub = b.subscribe("A", U1, observed=obs(P, G, "not_subscribed"))
    eq(b.res(sub)["generation_id"], G, "subscribing keeps the generation")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("X", T0))])
    check("population" not in b.made(rules.CATALOG_REFRESH, n)[0]["result"], "no second first population")
    eq((ADM(b, G, x)["result"]["source"], PS(b, x)), ("server", "new"), "the next fetch discovers")
    # Preview, then subscribe before any fetch: the first fetch is the first population.
    b = B("7b: preview then subscribe before any fetch")
    r = b.preview("A", U1)
    P, G = pg(r)
    b.subscribe("A", U1, observed=obs(P, G, "not_subscribed"))
    x = ref(P, G, "x", "X")
    b.fetch(P, [(x, ifields("X", T0))])
    check(ADM(b, G, x) is None and PS(b, x) is None, "first population admits nothing")
    # A retry after a failed first fetch is still first population; an emptied feed is not.
    b = B("7b: failed first fetch, retry; an emptied feed")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    b.fetch(P, outcome="failure")
    check(G not in b.engine.populated, "a failure records no population")
    e1 = ref(P, G, "e1", "E1")
    b.fetch(P, [(e1, ifields("E1", T0))])
    check(G in b.engine.populated and ADM(b, G, e1) is None, "the retry is the first population")
    b.fetch(P, [])
    e2 = ref(P, G, "e2", "E2")
    b.fetch(P, [(e2, ifields("E2", T0 + DAY))])
    eq(ADM(b, G, e2)["result"]["source"], "server", "a feed that became empty is not newly subscribed")
    # Closed generations.
    b = B("7b: closed generations")
    P, G, s, old = setup_catalog(b)
    x, late = ref(P, G, "x", "X"), ref(P, G, "late", "Late")
    b.discover("B", x, pub=T0, newest=T0 - DAY)
    check(L(b, x, "admission") is not None, "admitted")
    R = b.fetch_start(P)
    u = b.pod("A", "unsubscribe", P, G)
    check(L(b, x, "admission") is None and PS(b, x) is None, "closing the generation tombstones its admission")
    eq(L(b, x, "metadata")["title"], "X", "catalog metadata stays after an unsubscribe")
    lp = b.discover("B", late, pub=T0 + DAY, newest=T0, after=[u])
    eq(b.out(lp), ("not_applied", "generation_closed"), "a proposal naming a closed generation")
    eq(L(b, late, "metadata")["title"], "Late", "its observation still applies to the catalog")
    s2 = b.subscribe("A", U1, observed=obs(P, G, "removed"))
    G2 = b.res(s2)["generation_id"]
    check(G2 != G, "a fresh generation")
    n = len(b.engine.created)
    late2 = ref(P, G2, "late2", "Late 2")
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (late2, ifields("Late 2", T0 + 2 * DAY))], R=R)
    part = b.made(rules.CATALOG_REFRESH, n)[0]
    eq(part["inputs"]["generation_id"], G, "the fetch started under the closed generation")
    check("population" not in part["result"] and G2 not in b.engine.populated and b.made(rules.ADMISSION, n) == [],
          "its population and filing effects belong only to that generation, while live")
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (late2, ifields("Late 2", T0 + 2 * DAY))])
    check(G2 in b.engine.populated, "the new generation's first population")
    late3 = ref(P, G2, "late3", "Late 3")
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (late2, ifields("Late 2", T0 + 2 * DAY)),
                (late3, ifields("Late 3", T0 + 3 * DAY))])
    eq((ADM(b, G2, late3)["result"]["source"], PS(b, late3)), ("server", "new"), "then discoveries file again")
    check(ADM(b, G2, late2) is None, "late2 was the new generation's first population")
    # An explicit refresh of a removed podcast updates the catalog and files nothing.
    b = B("7b: explicit refresh of a removed podcast")
    P, G, s, old = setup_catalog(b)
    b.pod("A", "unsubscribe", P, G)
    x = ref(P, G, "x", "X")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("X", T0))])
    eq(b.made(rules.ADMISSION, n), [], "files nothing")
    eq(L(b, x, "metadata")["title"], "X", "updates the catalog")


def test_moves():
    b = B("7b: permanent and temporary redirects, new-feed-url, aliases")
    P, G, s, old = setup_catalog(b)
    items = [(old, ifields("Old", T0 - DAY))]
    ident = lambda: b.rec("podcast", P, "identity")  # noqa: E731
    n = len(b.engine.created)
    b.fetch(P, items, redirects=[(302, U2)])
    eq((b.made(rules.MOVED, n), ident()["refresh_url"]), ([], U1), "a temporary redirect changes nothing")
    n = len(b.engine.created)
    hop = "https://hop.example.com/feed"
    b.fetch(P, items, redirects=[(301, U3B), (302, hop), (308, U2B)], moved_to=U2B)
    mv = b.made(rules.MOVED, n)
    eq(len(mv), 1, "one move")
    eq(mv[0]["result"], {"refresh_url": U2B, "aliases": [{"url": U3B, "normalized": U3, "source": "redirect"},
                                                         {"url": U2B, "normalized": U2, "source": "redirect"}]},
       "every 301/308 hop's target becomes an alias; the refresh URL moves")
    eq(mv[0]["dependencies"], [b.made(rules.CATALOG_REFRESH, n)[0]["decision_id"]], "it depends on part 0")
    eq(ident(), {"aliases": sorted([{"url": U1, "normalized": U1}, {"url": U3B, "normalized": U3},
                                    {"url": U2B, "normalized": U2}], key=lambda a: a["normalized"]),
                 "refresh_url": U2B}, "the identity record")
    nf = "https://feeds.example.com/moved.xml"
    n = len(b.engine.created)
    b.fetch(P, items, new_feed_url=nf)
    eq(b.made(rules.MOVED, n)[0]["result"], {"refresh_url": nf, "aliases": [{"url": nf, "normalized": nf,
                                                                              "source": "new_feed_url"}]},
       "a fetched new-feed-url")
    # A move racing a user's change_url: the user's edit made while the fetch ran wins.
    R = b.fetch_start(P)
    mine = "https://mine.example.org/feed"
    cu = b.pod("A", "change_url", P, G, {"url": mine})
    n = len(b.engine.created)
    other = "https://other.example.org/feed"
    b.fetch(P, items, R=R, redirects=[(301, other)], moved_to=other)
    eq(b.made(rules.MOVED, n)[0]["result"]["refresh_url"], None, "the refresh URL write changed since the start")
    eq(ident()["refresh_url"], mine, "the user's URL stands")
    check(any(a["normalized"] == other for a in ident()["aliases"]), "the unclaimed alias is still added")
    eq(b.out(cu), ("applied", None), "change_url")
    # Without a race, the move wins until a user's later edit.
    third = "https://third.example.org/feed"
    b.fetch(P, items, redirects=[(308, third)], moved_to=third)
    eq(ident()["refresh_url"], third, "moved")
    later = "https://later.example.org/feed"
    b.pod("A", "change_url", P, G, {"url": later})
    eq(ident()["refresh_url"], later, "a user's newer URL edit wins")
    # A user's change_url to an alias a move claimed for this podcast conflicts.
    rq = b.subscribe("Q", "https://q.example.org/feed")
    Q, GQ = pg(rq)
    eq(b.out(b.pod("Q", "change_url", Q, GQ, {"url": third})), ("not_applied", "alias_conflict"),
       "a moved alias is a claim like any other")
    # A move onto another podcast's alias: the refresh URL moves, no alias is claimed, and the
    # server merges the two podcasts in the same fetch (P is older: it survives).
    n = len(b.engine.created)
    b.fetch(P, items, redirects=[(301, "https://q.example.org/feed")], moved_to="https://q.example.org/feed")
    eq(b.made(rules.MOVED, n)[0]["result"], {"refresh_url": "https://q.example.org/feed", "aliases": []},
       "the refresh URL moves; the claimed alias is not claimed again")
    pm = b.made(rules.PODCAST_MERGE, n)
    eq([(m["result"]["source"], m["result"]["into"]) for m in pm], [(Q, P)], "the podcasts merge")
    eq(ident()["refresh_url"], "https://q.example.org/feed", "moved")
    check(any(a["normalized"] == "https://q.example.org/feed" for a in ident()["aliases"]), "Q's alias moved to P")
    eq(b.rec("podcast", Q, "subscription"), {"state": "merged", "merged_into": P}, "Q is merged")


def test_refresh_status():
    b = B("7b: refresh_status publication")
    P, G, s, old = setup_catalog(b)
    items = [(old, ifields("Old", T0 - DAY))]
    st = lambda: b.rec("podcast", P, "refresh_status")  # noqa: E731
    first = st()
    eq((first["error"], first["last_success"], first["last_attempt"]), (None, first["last_attempt"],
                                                                         first["last_attempt"]), "first success")
    err = {"label": "http status", "code": 503, "detail": "Service Unavailable"}
    n = len(b.engine.created)
    b.fetch(P, outcome="failure", error=err, next_attempt=b.now + 900000 + 1000)
    parts = b.made(rules.CATALOG_REFRESH, n)
    eq((len(parts), parts[0]["result"]), (1, {"status": st(), "items": []}), "a failure publishes its status")
    eq((st()["error"], st()["last_success"]), (err, first["last_success"]), "the error, the last success kept")
    n = len(b.engine.created)
    b.fetch(P, outcome="failure", error=err)
    eq(b.engine.created[n:], [], "the same failure again: not due")
    b.wait(DAY)
    b.fetch(P, outcome="failure", error=err)
    eq(len(b.made(rules.CATALOG_REFRESH, n)), 1, "once a day otherwise")
    n = len(b.engine.created)
    b.fetch(P, outcome="failure", error={"label": "not found", "code": 404, "detail": None})
    eq(len(b.made(rules.CATALOG_REFRESH, n)), 1, "another error is due")
    n = len(b.engine.created)
    b.fetch(P, items)
    eq((len(b.made(rules.CATALOG_REFRESH, n)), st()["error"]), (1, None), "success after failure is due")
    n = len(b.engine.created)
    b.fetch(P, items)
    eq(b.engine.created[n:], [], "an unchanged success within a day writes nothing")
    b.wait(DAY)
    b.fetch(P, items)
    eq(len(b.made(rules.CATALOG_REFRESH, n)), 1, "and once a day")


def test_seen_history():
    """Part 0 lists as `seen` the items whose server fetch history
    changes without a written record, forcing part 0, so the history is recoverable from
    decisions alone."""
    b = B("7b: seen items in part 0")
    P, G, s, old = setup_catalog(b)
    x, y = ref(P, G, "x", "X"), ref(P, G, "y", "Y")
    b.observe("B", P, [(x, None, ifields("X", T0 - 3 * DAY)), (y, None, ifields("Y", T0 - 4 * DAY, media=False))])
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("X", T0 - 3 * DAY)), (y, ifields("Y", T0 - 4 * DAY, media=False))])
    parts = b.made(rules.CATALOG_REFRESH, n)
    eq((len(parts), parts[0]["result"]["items"], parts[0]["result"].get("seen")),
       (1, [], sorted([x["episode_id"], y["episode_id"]])), "unchanged records new to the history are seen")
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("X", T0 - 3 * DAY)), (y, ifields("Y", T0 - 4 * DAY, media=False))])
    eq(b.engine.created[n:], [], "an unchanged fetch of items the history holds writes nothing")
    b.observe("B", P, [(y, b.revision(y["episode_id"]), ifields("Y", T0 - 4 * DAY))])
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY)), (x, ifields("X", T0 - 3 * DAY)), (y, ifields("Y", T0 - 4 * DAY))])
    eq(b.made(rules.CATALOG_REFRESH, n)[0]["result"].get("seen"), [y["episode_id"]],
       "a catalog-only item that now has an enclosure (its record already canonical) is seen")
    check(ADM(b, G, y) is not None, "and discovered (gained media)")
    # Recovery from decisions alone restores the server's history.
    eng = rules.Engine(ACC, NS, b.engine.resolutions, b.engine.alias_decisions(), b.engine.episode_decisions(),
                       replay=True)
    for a in b.arrivals:
        if "fact" in a:
            a2 = json.loads(json.dumps(a))
            a2.pop("decisions", None)
            a2["receipt"] = dict(zip(("epoch", "seq", "index"), b.engine.facts[a["fact"]["op_id"]]["rkey"]))
            eng.accept(a2)
    for a in b.engine.decision_arrivals():
        if a["decision"]["decision_id"] not in eng.decisions:
            eng.accept(a)
    eq(eng.server_seen, b.engine.server_seen, "the history restored from the archived decisions")


def test_parts():
    b = B("7b: changed records in parts of at most 200 items")
    b.vector_ok = False  # large
    b.check_every = False
    P, G, s, old = setup_catalog(b)
    many = [(ref(P, G, "n%03d" % i, "N%03d" % i), ifields("N%03d" % i, T0 + i)) for i in range(201)]
    n = len(b.engine.created)
    b.fetch(P, [(old, ifields("Old", T0 - DAY))] + many)
    parts = b.made(rules.CATALOG_REFRESH, n)
    eq([(p["inputs"]["part"], len(p["result"]["items"]), "status" in p["result"]) for p in parts],
       [(0, 200, True), (1, 1, False)], "two parts")
    last = ADM(b, G, many[-1][0])
    eq(last["inputs"]["found_by"], parts[1]["decision_id"], "an admission depends on the part that found it")
    eq(len(b.made(rules.ADMISSION, n)), 201, "every new release admitted")
    eq(b.engine.records(), b.engine.full_records(), "incremental vs full")


def test_competing_admissions_in_recovery():
    """PROTOCOL §9.5: recovery restores the first admission of (G, E) by rank, whatever the upload
    order, and only the effective one's filing applies."""
    prefix = B.count + 1
    lives = {}

    def setup(b):
        for d in ("Z", "PA", "PB"):
            b.dev(d)  # the same author IDs in every builder
        s = b.subscribe("Z", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        x = ref(P, G, "x", "X")
        ob = b.observe("Z", P, [(x, None, ifields("X", T0))], op=fixed_op(prefix, 2), batch=fixed_op(prefix, 10))
        return s, G, x, ob
    for first, epoch in (("A", 1), ("B", 2)):
        b = B("live admission by %s (epoch %d)" % (first, epoch), prefix)
        b.server_label = "7b competing %s" % first
        b.epoch = epoch
        s, G, x, ob = setup(b)
        props = {
            "A": lambda: b.propose("PA", x, action="inbox", pub=T0, after=[ob], t=T0 + 50000, rt=T0 + 60000,
                                   op=fixed_op(prefix, 3), batch=fixed_op(prefix, 11)),
            "B": lambda: b.propose("PB", x, action="queue", bundle=(K(None, None, "rec/x"), END), pub=T0, after=[ob],
                                   t=T0 + 51000, rt=T0 + 61000, op=fixed_op(prefix, 4), batch=fixed_op(prefix, 12)),
        }
        props[first]()
        props["B" if first == "A" else "A"]()
        lives[first] = (b, ADM(b, G, x))
    want_state = None
    for order in (("A", "B"), ("B", "A")):
        b = B("competing admissions recovered, %s's first" % order[0], prefix)
        s, G, x, ob = setup(b)
        b.propose("PA", x, action="inbox", pub=T0, after=[ob], t=T0 + 50000, rt=T0 + 60000, op=fixed_op(prefix, 3),
                  batch=fixed_op(prefix, 11), recovery=True)
        b.propose("PB", x, action="queue", bundle=(K(None, None, "rec/x"), END), pub=T0, after=[ob], t=T0 + 51000,
                  rt=T0 + 61000, op=fixed_op(prefix, 4), batch=fixed_op(prefix, 12), recovery=True)
        for who in order:
            b.upload_decision(lives[who][1])
        eq(L(b, x, "admission")["admission_id"], lives["A"][1]["decision_id"], "the smallest rank (epoch 1) wins")
        eq((PS(b, x), MEM(b, x)), ("new", None), "only the effective admission files")
        eq(b.engine.created, [], "recovery decides nothing")
        if want_state is None:
            want_state = b.engine.records()
        eq(b.engine.records(), want_state, "both upload orders converge")
    expect_input_error(lambda: b.upload_decision(dict(lives["A"][1], decision_id=det_uuid("bad"),
                                                      dependencies=[s])), "an admission depends on its proposal")


def test_catalog_permutations(ncases, nperms):
    """Every causal delivery order of a random catalog case's facts and persisted decisions
    (refresh parts, admissions, moves, sort results) gives the same records."""
    rng = random.Random(7707)
    checked = 0
    stats = {"refresh": 0, "admission": 0, "moved": 0}
    for k in range(ncases):
        case, eng = gen_rule_cases.generate_case(rng, catalog=True)
        want = eng.records()
        res, aliases, episodes = eng.resolutions, eng.alias_decisions(), eng.episode_decisions()
        first = {}
        for a in case["arrivals"]:
            if "fact" not in a:
                continue
            op = a["fact"]["op_id"]
            if op not in first:
                a = json.loads(json.dumps(a))
                a.pop("decisions", None)
                a["receipt"] = dict(zip(("epoch", "seq", "index"), eng.facts[op]["rkey"]))
                first[op] = a
        nodes = list(first.values()) + eng.decision_arrivals()
        for d in eng.decisions.values():
            t = d["obj"]["decision_type"]
            for key, dt in (("refresh", rules.CATALOG_REFRESH), ("admission", rules.ADMISSION), ("moved", rules.MOVED)):
                stats[key] += t == dt
        for p in range(nperms):
            perm = linear_extension(rng, nodes, rng.randint(0, 2))
            replay = rules.Engine(case["account_id"], case["namespace"], res, aliases, episodes, replay=True,
                                  fixed_outcomes=eng.outcomes)
            for a in perm:
                replay.accept(a)
            eq(replay.records(), want, "catalog permutation %d of random case %d" % (p, k))
            eq(replay.records(), replay.full_records(), "replay projection vs full recompute")
        checked += 1
    check(checked == ncases and stats["refresh"] > 0 and stats["admission"] > 0,
          "the 7b permutation test met no refresh or admission: %s" % stats)


def test_catalog_generator(ncases):
    # The seed moved from 7708 to 7709 with the queue replay (the devices' queue draws
    # changed the random stream; 7708's first cases no longer cover three outcomes).
    rng = random.Random(7709)
    a = [gen_rule_cases.generate_case(rng, catalog=True)[0] for _ in range(ncases)]
    rng = random.Random(7709)
    b2 = [gen_rule_cases.generate_case(rng, catalog=True)[0] for _ in range(ncases)]
    eq(json.dumps(a, sort_keys=True), json.dumps(b2, sort_keys=True), "the catalog generator is deterministic per seed")
    seen = {"observe": 0, "propose": 0, "fetch": 0, "fetch_start": 0, "admission": 0, "reasons": set()}
    for i, case in enumerate(a):
        res = check_rules.check_case(json.loads(json.dumps(case, ensure_ascii=False)))
        check(res is None, "generated catalog case %d fails the checker: %s" % (i, res))
        for arr in case["arrivals"]:
            if "fact" in arr:
                t = arr["fact"]["type"]
                if t == "catalog.observe":
                    seen["observe"] += 1
                elif t == "discovery.propose":
                    seen["propose"] += 1
                    o = case["expected"]["outcomes"].get(arr["fact"]["op_id"])
                    if o is not None:
                        seen["reasons"].add(o.get("reason", o["outcome"]))
            elif "fetch" in arr:
                seen["fetch"] += 1
            elif "fetch_start" in arr:
                seen["fetch_start"] += 1
        seen["admission"] += sum(1 for d in case["expected"].get("decisions", [])
                                 if d["decision_type"] == rules.ADMISSION)
    check(all(seen[k] > 0 for k in ("observe", "propose", "fetch", "fetch_start", "admission")),
          "the catalog generator exercises observations, proposals, fetches and admissions: %s" % seen)
    check(len(seen["reasons"]) >= 3, "several proposal outcomes: %s" % sorted(seen["reasons"]))


# ---------------------------------------------------------------------------
# PROTOCOL §9.7: identity reconciliation
# ---------------------------------------------------------------------------

US = "\\u001f"


def gocc(g, k="rss-guid"):
    """A fetch item's occurrence (the server reads the kind and the GUID)."""
    return {"k": k, "v": g, "guid": g}


def eocc(enc):
    return {"k": "enclosure", "v": c.normalize_url(enc)}


def ofields(title, pub, enc=None, duration=0):
    return ifields(title, pub, media=enc is not None, url=enc, duration=duration)


def oref(P, G, g=None, enc=None, pub=None, title=None, k=None):
    """A device's EpisodeRef with full occurrence evidence, its ID derived by rules 2-4
    (`k` = rss-guid+enclosure for a device that knows the GUID is disputed)."""
    if g is not None:
        k = k or "rss-guid"
        v = g if k != "rss-guid+enclosure" else c.episode_v_guid_enclosure(g, c.normalize_url(enc))
        occ = {"k": k, "v": v, "guid": g}
    else:
        k, v = "enclosure", c.normalize_url(enc)
        occ = {"k": k, "v": v}
    if enc is not None:
        occ["enclosure_url"] = enc
    if title is not None:
        occ["title"] = title
    if pub is not None:
        occ["publication_time"] = pub
    return {"episode_id": c.episode_id(NS, P, k, v), "podcast_id": P, "generation_id": G, "occurrence": occ}


def eid(P, k, v):
    return c.episode_id(NS, P, k, v)


def rge(P, g, enc):
    return eid(P, "rss-guid+enclosure", c.episode_v_guid_enclosure(g, c.normalize_url(enc)))


def IR(b, P):
    return b.rec("podcast", P, "identity_rules")


def MERGED(b, E):
    v = b.rec("episode", E, "identity")
    return v.get("merged_into") if v is not None else None


def state_of(b, E):
    """The episode's state records (no identity, metadata or admission)."""
    recs = b.engine.proj.get(("episode", E), {})
    return dict((k, v) for k, v in recs.items() if k not in ("identity", "metadata", "admission"))


def test_scenario17():
    # (a) A bulletin reusing /latest.mp3 with new GUIDs and the same title every few hours.
    for first in ("server", "phone"):
        b = B("7c scenario 17a: bulletin with new GUIDs on /latest.mp3, %s first" % first)
        s = b.subscribe("A", U1)
        P, G = pg(s)
        latest = "https://cdn.example.org/news/latest.mp3"
        b1 = oref(P, G, "bulletin-1", latest, T0, "News")
        b.fetch(P, [(gocc("bulletin-1"), ofields("News", T0, latest))])
        b.mark_played("A", b1)
        b2 = oref(P, G, "bulletin-2", latest, T0 + 4 * 3600000, "News")
        if first == "phone":
            b.observe("B", P, [(b2, None, ofields("News", T0 + 4 * 3600000, latest))])
        b.fetch(P, [(gocc("bulletin-2"), ofields("News", T0 + 4 * 3600000, latest))])
        eq(b.made(rules.GUID_CHANGE), [], "%s: the dates differ: no GUID-change match" % b.name)
        eq(PS(b, b1), "played", "the earlier bulletin stays played")
        check(PS(b, b2) != "played" and MERGED(b, b2["episode_id"]) is None, "%s: a new episode, not played" % b.name)
        eq(IR(b, P), {"disputed_guids": [], "disputed_enclosures": [c.normalize_url(latest)], "aliases": []},
           "%s: the shared enclosure is disputed (different GUIDs never share identity)" % b.name)
    # (b) A hosting move changing every GUID and enclosure but keeping titles and dates.
    for first in ("server", "phone"):
        b = B("7c scenario 17b: hosting move keeps listening state, %s first" % first)
        s = b.subscribe("A", U1)
        P, G = pg(s)
        old = [(("old-%d" % i), "https://host1.example.org/%d.mp3" % i, "Episode %d" % i, T0 + i * DAY) for i in range(3)]
        new = [(("new-%d" % i), "https://host2.example.org/%d.mp3" % i, t, d) for i, (_, _, t, d) in enumerate(old)]
        b.fetch(P, [(gocc(g), ofields(t, d, u)) for g, u, t, d in old])
        o0 = oref(P, G, *old[0][:2], pub=old[0][3], title=old[0][2])
        sid = b.uuid()
        st = b.start("A", o0, sid, 0)
        b.cp("A", o0, sid, 1, 10 * MIN, 10 * MIN)
        n0 = oref(P, G, *new[0][:2], pub=new[0][3], title=new[0][2])
        if first == "phone":
            b.observe("B", P, [(n0, None, ofields(new[0][2], new[0][3], new[0][1]))])
            b.fav("B", n0, True)
        n = len(b.engine.created)
        b.fetch(P, [(gocc(g), ofields(t, d, u)) for g, u, t, d in new])
        gcs = b.made(rules.GUID_CHANGE, n)
        eq(sorted(x["result"]["guid"] for x in gcs), ["new-0", "new-1", "new-2"], "%s: one-to-one matches" % b.name)
        eq(gcs[0]["result"]["episode_id"], o0["episode_id"], "the new GUID is an alias of the old episode")
        eq([(m["result"]["from"], m["result"]["into"]) for m in b.made(rules.EPISODE_MERGE, n)],
           [(n0["episode_id"], o0["episode_id"])] if first == "phone" else [],
           "%s: the phone's episode for the new GUID merges into the old one" % b.name)
        eq(L(b, o0, "position"), posv(G, 10 * MIN, st), "%s: listening state kept" % b.name)
        eq(L(b, o0, "metadata")["media"]["url"], new[0][1], "the old ID now has the new enclosure")
        if first == "phone":
            eq(L(b, o0, "favorite"), {"generation_id": G, "on": True}, "the phone's favorite moved to the survivor")
            eq(MERGED(b, n0["episode_id"]), o0["episode_id"], "the phone's ID redirects")
        # A late fact naming the new GUID's plain ID is applied to the old episode.
        b.mark_played("C", n0)
        eq(PS(b, o0), "played", "%s: routed to the root at acceptance" % b.name)
        eq(sorted(a["value"] for a in IR(b, P)["aliases"]), ["new-0", "new-1", "new-2"],
           "the aliases devices cannot derive are published")
        check(all(a["kind"] == "guid" for a in IR(b, P)["aliases"]), "GUID aliases")


def test_scenario18():
    b = B("7c scenario 18: all GUIDs changed; merges only where the rule holds")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    old = [("a", "Twin", T0), ("b", "Twin", T0), ("c", "No date", None), ("d", "Dee", T0 + DAY), ("e", "", T0 + 2 * DAY),
           ("f", "Eff", T0 + 3 * DAY)]
    b.fetch(P, [(gocc("old-" + g), ofields(t, d, "https://h1.example.org/%s.mp3" % g)) for g, t, d in old])
    for g, _, _ in old:
        b.fav("A", oref(P, G, "old-" + g), True)
    # The new feed: f's date moved by one millisecond within the same second (a match).
    new = [(g, t, (d + 1 if g == "f" else d)) for g, t, d in old]
    b.fetch(P, [(gocc("new-" + g), ofields(t, d, "https://h2.example.org/%s.mp3" % g)) for g, t, d in new])
    eq(sorted(x["result"]["guid"] for x in b.made(rules.GUID_CHANGE)), ["new-d", "new-f"],
       "competing twins, a missing date and an empty title never merge; equal to the second does")
    for g, merged in (("a", False), ("b", False), ("c", False), ("d", True), ("e", False), ("f", True)):
        nE = eid(P, "rss-guid", "new-" + g)
        if merged:
            eq(b.rec("episode", nE, "identity"), None, "%s: never a separate episode" % g)
            eq(L(b, oref(P, G, "old-" + g), "favorite"), {"generation_id": G, "on": True}, "%s keeps its state" % g)
        else:
            check(L(b, oref(P, G, "new-" + g), "favorite") is None, "%s: a new, separate episode" % g)


def test_scenario46():
    # 1-3: GUID changes.
    b = B("7c scenario 46: hosting move, competing candidates, missing titles or dates")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    b.fetch(P, [(gocc("G1"), ofields("Move me", T0, "https://u1.example.org/a.mp3")),
                (gocc("X1"), ofields("Twin", T0 + DAY, "https://u1.example.org/x.mp3")),
                (gocc("Y1"), ofields(None, T0 + 2 * DAY, "https://u1.example.org/y.mp3")),
                (gocc("Z1"), ofields("Undated", None, "https://u1.example.org/z.mp3"))])
    n = len(b.engine.created)
    b.fetch(P, [(gocc("G2"), ofields("Move me", T0 + 999, "https://u2.example.org/a.mp3")),
                (gocc("X2"), ofields("Twin", T0 + DAY, "https://u2.example.org/x.mp3")),
                (gocc("X3"), ofields("Twin", T0 + DAY, "https://u2.example.org/x3.mp3")),
                (gocc("Y2"), ofields(None, T0 + 2 * DAY, "https://u2.example.org/y.mp3")),
                (gocc("Z2"), ofields("Undated", None, "https://u2.example.org/z.mp3"))])
    eq([(x["result"]["guid"], x["result"]["episode_id"]) for x in b.made(rules.GUID_CHANGE, n)],
       [("G2", eid(P, "rss-guid", "G1"))], "only the one-to-one hosting move (G1,U1) -> (G2,U2) matches")
    # 4: the same GUID and date at two signed URLs with 20 and 23 minute durations.
    b = B("7c scenario 46: one release at two signed URLs, duration-only differences")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    signed1 = "https://cdn.example.org/ep.mp3?sig=aaa"
    signed2 = "https://cdn.example.org/ep.mp3?sig=bbb"
    b.fetch(P, [(gocc("same"), ofields("Ep", T0, signed1, 20 * MIN))])
    r2 = oref(P, G, "same", signed2, T0, "Ep")
    b.observe("B", P, [(r2, None, ofields("Ep", T0, signed2, 23 * MIN))])
    b.fetch(P, [(gocc("same"), ofields("Ep", T0 + 3 * DAY, signed2, 23 * MIN))])
    eq((b.made(rules.MAPPING), IR(b, P)), ([], None), "within 7 days: one episode, nothing disputed")
    # 6: missing dates never split.
    b.fetch(P, [(gocc("same"), ofields("Ep", None, "https://cdn.example.org/other.mp3", 40 * MIN))])
    eq(b.made(rules.MAPPING), [], "a missing date is never dispute evidence")
    # 5: differing enclosures with two non-null dates more than 7 days apart.
    b = B("7c scenario 46: differing enclosures more than 7 days apart are distinct releases")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.org/r1.mp3", "https://cdn.example.org/r2.mp3"
    b.fetch(P, [(gocc("reused"), ofields("Release 1", T0, u1))])
    first = oref(P, G, "reused", u1, T0, "Release 1")
    b.mark_played("A", first)
    b.fetch(P, [(gocc("reused"), ofields("Release 1", T0 + 7 * DAY, "https://cdn.example.org/r1b.mp3"))])
    eq(b.made(rules.MAPPING), [], "exactly 7 days apart with another enclosure is the same release")
    b.fetch(P, [(gocc("reused"), ofields("Release 3", T0 + 20 * DAY, u2))])
    maps = b.made(rules.MAPPING)
    eq([(m["result"]["episode_id"], m["result"]["original"], m["result"]["publication_time"]) for m in maps],
       [(first["episode_id"], True, T0), (rge(P, "reused", u2), False, T0 + 20 * DAY)],
       "the earliest release keeps the original ID; the later one takes rss-guid+enclosure")
    eq(PS(b, first), "played", "the original's state stays")
    later = rge(P, "reused", u2)
    eq((PS(b, {"episode_id": later}), L(b, {"episode_id": later}, "metadata")["title"]), ("new", "Release 3"),
       "the later release is a new episode with its own metadata, discovered and filed as a new release")
    eq(IR(b, P)["disputed_guids"], [{"guid": "reused", "releases": [
        {"episode_id": first["episode_id"], "publication_time": T0,
         "enclosures": [c.normalize_url(u1), "https://cdn.example.org/r1b.mp3"]},
        {"episode_id": later, "publication_time": T0 + 20 * DAY, "enclosures": [c.normalize_url(u2)]}]}],
       "the resolver devices use")
    eq(b.rec("episode", later, "identity")["occurrence"]["k"], "rss-guid+enclosure", "registered by its mapping")


def scenario34_setup(b, prefix):
    s = b.subscribe("Z", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    for d in ("A", "B"):
        b.dev(d)
    g = "weekly-show"
    u1, u2 = "https://cdn.example.org/w1.mp3", "https://cdn.example.org/w2.mp3"
    d1, d2 = T0, T0 + 14 * DAY
    ra, rb = oref(P, G, g, u1, d1, "Week 1"), oref(P, G, g, u2, d2, "Week 3")
    check(ra["episode_id"] == rb["episode_id"], "both phones derive the same ID before the dispute is known")
    base = b.now
    sid = fixed_op(prefix, 99)
    ta, tb = base + 1000, base + 2000  # the phones' own clocks: the same in every delivery order

    def batch_a(rt):
        b.observe("A", P, [(ra, None, ofields("Week 1", d1, u1))], after=[s], t=ta, rt=rt, op=fixed_op(prefix, 10),
                  batch=fixed_op(prefix, 30), fetch_time=ta)
        b.fav("A", ra, True, t=ta + 1, rt=rt + 1, op=fixed_op(prefix, 11))
        b.start("A", ra, sid, 0, t=ta + 2, rt=rt + 2, op=fixed_op(prefix, 12))
        b.cp("A", ra, sid, 1, 12 * MIN, 12 * MIN, t=ta + 3, rt=rt + 3, op=fixed_op(prefix, 13))

    def batch_b(rt):
        b.observe("B", P, [(rb, None, ofields("Week 3", d2, u2))], after=[s], t=tb, rt=rt, op=fixed_op(prefix, 20),
                  batch=fixed_op(prefix, 31), fetch_time=tb)
        b.mark_played("B", rb, t=tb + 1, rt=rt + 1, op=fixed_op(prefix, 21))
        b.qadd("B", [(rb, K(None, None, "s34/b"))], t=tb + 2, rt=rt + 2, op=fixed_op(prefix, 22))
    return P, G, g, ra, rb, rge(P, g, u2), base, batch_a, batch_b, sid


def test_scenario34():
    prefix = B.count + 1
    finals = []
    for order in ("AB", "BA"):
        b = B("scenario 34: two offline phones meet two reused-GUID releases, %s synced first" % order[0], prefix)
        b.server_label = "7c s34 " + order
        P, G, g, ra, rb, later, base, batch_a, batch_b, sid = scenario34_setup(b, prefix)
        steps = {"A": batch_a, "B": batch_b}
        steps[order[0]](base + 10000)
        steps[order[1]](base + 20000)
        orig = ra["episode_id"]
        eq(sorted(m["result"]["episode_id"] for m in b.made(rules.MAPPING)), sorted([orig, later]),
           "%s: one mapping per release" % b.name)
        eq(state_of(b, orig), {"favorite": {"generation_id": G, "on": True},
                               "play_state": {"generation_id": G, "play_state": "unplayed"},
                               "position": posv(G, 12 * MIN, fixed_op(prefix, 12)),
                               "last_played": {"generation_id": G, "time": b.nt(fixed_op(prefix, 13))},
                               "history": {"generation_id": G, "time": b.nt(fixed_op(prefix, 13))},
                               "played_time": {"generation_id": G, "played_ms": 12 * MIN}},
           "%s: week 1 keeps A's listening and favorite" % b.name)
        eq(PS(b, {"episode_id": later}), "played", "%s: week 3 is played" % b.name)
        eq(MEM(b, {"episode_id": later}), mem(G, True), "%s: week 3 is queued" % b.name)
        check(MEM(b, ra) is None, "%s: week 1 is not queued" % b.name)
        finals.append(dict((E, state_of(b, E)) for E in (orig, later)))
        # The session continues under its release whatever ID its next save names.
        b.cp("A", ra, sid, 2, 15 * MIN, 15 * MIN)
        eq(L(b, ra, "position")["position"], 15 * MIN, "%s: A's next checkpoint" % b.name)
    eq(finals[0], finals[1], "both sync orders give each release the same state")


def test_late_facts_and_resolver():
    b = B("7c: the resolver for late facts, undated and arbitrary references, evidence")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.org/a.mp3", "https://cdn.example.org/b.mp3"
    b.fetch(P, [(gocc("g"), ofields("One", T0, u1))])
    b.fetch(P, [(gocc("g"), ofields("Two", T0 + 30 * DAY, u2))])
    orig, later = eid(P, "rss-guid", "g"), rge(P, "g", u2)
    # A late fact naming the original ID for the later release's occurrence (a phone that
    # does not know the dispute): routed by its date.
    f1 = b.fav("C", oref(P, G, "g", u2, T0 + 30 * DAY + DAY), True)
    eq((b.out(f1), L(b, {"episode_id": later}, "favorite")), (("applied", None), {"generation_id": G, "on": True}),
       "routed to the later release at acceptance")
    check(L(b, {"episode_id": orig}, "favorite") is None, "the original is untouched")
    # Undated: by its enclosure when exactly one release has it, else the original.
    b.mark_played("C", oref(P, G, "g", u2))
    eq(PS(b, {"episode_id": later}), "played", "an undated occurrence with the later release's enclosure")
    b.mark_played("C", oref(P, G, "g", "https://cdn.example.org/unknown.mp3"))
    eq(PS(b, {"episode_id": orig}), "played", "an undated occurrence with an unknown enclosure: the original")
    # A phone that knows the dispute names the rule-3 ID: the same release.
    b.ep("C", "episode.mark_unplayed", oref(P, G, "g", u2, T0 + 30 * DAY, k="rss-guid+enclosure"))
    eq(PS(b, {"episode_id": later}), "unplayed", "the rule-3 ID")
    # An arbitrary claimed ID authorizes nothing: a device naming another episode's ID with
    # this GUID's occurrence is applied to what it names, and records no evidence.
    other = oref(P, G, "other", "https://cdn.example.org/o.mp3", T0, "Other")
    b.fetch(P, [(gocc("g"), ofields("Two", T0 + 30 * DAY, u2)),
                (gocc("other"), ofields("Other", T0, "https://cdn.example.org/o.mp3"))])
    claim = dict(oref(P, G, "g", "https://cdn.example.org/far.mp3", T0 + 90 * DAY), episode_id=other["episode_id"])
    n = len(b.engine.created)
    b.observe("D", P, [(claim, None, ofields("Far", T0 + 90 * DAY, "https://cdn.example.org/far.mp3"))])
    eq(b.made(rules.MAPPING, n), [], "an observation for an arbitrary ID is no evidence: no new release")
    # A fact whose dated occurrence matches no release keeps its reference until the
    # release is known, then the mapping moves it.
    far = oref(P, G, "g", "https://cdn.example.org/far.mp3", T0 + 90 * DAY)
    b.fav("C", far, True)
    eq(L(b, {"episode_id": orig}, "favorite"), {"generation_id": G, "on": True}, "kept on the named original")
    n = len(b.engine.created)
    b.observe("E", P, [(far, None, ofields("Far", T0 + 90 * DAY, "https://cdn.example.org/far.mp3"))])
    third = rge(P, "g", "https://cdn.example.org/far.mp3")
    eq([m["result"]["episode_id"] for m in b.made(rules.MAPPING, n)], [third], "a device observation disputes")
    eq((L(b, {"episode_id": third}, "favorite"), L(b, {"episode_id": orig}, "favorite")),
       ({"generation_id": G, "on": True}, None), "the mapping partitioned the favorite to its release")


def test_rotating_enclosures():
    b = B("7c: rotating enclosures get allocated occurrence IDs")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.org/odd.mp3", "https://cdn.example.org/even.mp3"
    for i, u in enumerate([u1, u2, u1, u2]):
        b.fetch(P, [(gocc("bi"), ofields("Edition %d" % i, T0 + i * 14 * DAY, u))])
    maps = b.made(rules.MAPPING)
    ids = [m["result"]["episode_id"] for m in maps]
    eq(ids[:3], [eid(P, "rss-guid", "bi"), rge(P, "bi", u2), rge(P, "bi", u1)],
       "the original, then rss-guid+enclosure of each new pair")
    check(ids[3].startswith("e0:") and maps[3]["result"]["allocated"], "a pair already used: an allocated e0: ID")
    eq([r["publication_time"] for r in IR(b, P)["disputed_guids"][0]["releases"]],
       [T0 + i * 14 * DAY for i in range(4)], "four releases")
    eq(L(b, {"episode_id": ids[3]}, "metadata")["title"], "Edition 3", "the allocated release's records")


IDENTITY_FAMILY = "rules_identity_family.json"


def test_merge_family_reference():
    """PROTOCOL §9.7 (item 6: acceptance resolves an episode reference
    before any state validation; generated recovery seed 8004 case 31). P (A's
    subscription) and Q (B's) are merged by P's fetch moving onto Q's URL (P is older: it
    survives). B synced the merge (its start depends on it: a device can only name the
    survivor's episode after seeing it) but still names its own podcast Q and generation;
    its start names the survivor's episode E (P's derivation of the enclosure occurrence).
    By hand: Q's family is P, so the episode's podcast passes the check, the
    reference routes to (E, P, P's live class) and the start applies, whether E was
    registered under P first (A marked it played) or is first named by B's start. A
    reference naming an episode of a podcast outside the family (R's) is still
    episode_podcast_mismatch (not a vector)."""
    for k, registered in enumerate((True, False)):
        b = B("7c merge family: a merged-away podcast's reference naming the survivor's episode, %s"
              % ("which was registered under the survivor first" if registered else "first named after the merge"),
              0xc7f00001 + k, group=IDENTITY_FAMILY)
        for d in ("A", "B"):
            b.dev(d)
        sa = b.subscribe("A", U1)
        P, G = pg(sa)
        qurl = "https://q.example.org/feed"
        sb = b.subscribe("B", qurl)
        Q, GQ = pg(sb)
        u = "https://cdn.example.org/one.mp3"
        occ = {"k": "enclosure", "v": u, "enclosure_url": u, "title": "One", "publication_time": T0}
        EP = c.episode_id(NS, P, "enclosure", u)
        seen = [sb]
        if registered:
            seen.append(b.mark_played("A", {"episode_id": EP, "podcast_id": P, "generation_id": G,
                                            "occurrence": dict(occ)}))
        n = len(b.engine.created)
        b.fetch(P, [], redirects=[(301, qurl)], moved_to=qurl)
        eq(b.rec("podcast", Q, "subscription"), {"state": "merged", "merged_into": P}, "%s: Q merged into P" % b.name)
        seen += [d["decision_id"] for d in b.engine.created[n:] if d["decision_type"] == rules.PODCAST_MERGE]
        st = b.start("B", {"episode_id": EP, "podcast_id": Q, "generation_id": GQ, "occurrence": dict(occ)},
                     b.uuid(), 0, after=seen)
        eq(b.out(st), ("applied", None), "%s: the start applies" % b.name)
        eq(b.rec("episode", EP, "position")["claim_id"], st, "%s: under the survivor's episode" % b.name)
    # Outside the family: an episode of another podcast R named under Q is refused.
    o = B("7c merge family: an episode outside the family (not a vector)", 0xc7f00003)
    o.vector_ok = False
    P, G = pg(o.subscribe("A", U1))
    qurl = "https://q.example.org/feed"
    sb = o.subscribe("B", qurl)
    Q, GQ = pg(sb)
    R, GR = pg(o.subscribe("A", U3))
    u = "https://cdn.example.org/one.mp3"
    occ = {"k": "enclosure", "v": u, "enclosure_url": u, "title": "One", "publication_time": T0}
    ER = c.episode_id(NS, R, "enclosure", u)
    o.mark_played("A", {"episode_id": ER, "podcast_id": R, "generation_id": GR, "occurrence": dict(occ)})
    n = len(o.engine.created)
    o.fetch(P, [], redirects=[(301, qurl)], moved_to=qurl)
    merge = [d["decision_id"] for d in o.engine.created[n:] if d["decision_type"] == rules.PODCAST_MERGE]
    bad = o.start("B", {"episode_id": ER, "podcast_id": Q, "generation_id": GQ, "occurrence": dict(occ)}, o.uuid(), 0,
                  after=[sb] + merge)
    eq(o.out(bad), ("not_applied", "episode_podcast_mismatch"), "another family's episode is refused")


def test_merges_during_playback_and_compound():
    # PROTOCOL §9.7: a session started on the new GUID's ID before the GUID change continues under
    # the survivor; an add of the new GUID's episode, already queued as the survivor, is a
    # duplicate add once the merge maps it (PROTOCOL §7.6: the final mapping from the
    # start): one queue item survives, where it was.
    for when in ("before", "after"):
        b = B("7c: playback and a compound across a GUID-change merge, the fact %s the merge" % when)
        s = b.subscribe("A", U1)
        P, G = pg(s)
        b.fetch(P, [(gocc("old"), ofields("Same", T0, "https://h1.example.org/x.mp3")),
                    (gocc("keep"), ofields("Keep", T0 + DAY, "https://h1.example.org/k.mp3"))])
        o, k = oref(P, G, "old"), oref(P, G, "keep")
        nw = oref(P, G, "new", "https://h2.example.org/x.mp3", T0, "Same")
        b.observe("B", P, [(nw, None, ofields("Same", T0, "https://h2.example.org/x.mp3"))])
        sid = b.uuid()
        st = b.start("B", nw, sid, 0)
        b.cp("B", nw, sid, 1, 5 * MIN, 5 * MIN)
        ko, kn = keys_in_order(["cmp/old", "cmp/new"])
        b.qadd("A", [(o, ko), (k, K(kn[0], None, "cmp/keep"))], place=END)

        def compound():
            # B adds the new GUID's episode at the end
            return b.qadd("B", [(nw, kn)], place=END)
        if when == "before":
            compound()
        b.fetch(P, [(gocc("new"), ofields("Same", T0, "https://h2.example.org/x.mp3")),
                    (gocc("keep"), ofields("Keep", T0 + DAY, "https://h1.example.org/k.mp3"))])
        check(b.made(rules.EPISODE_MERGE) != [], "merged")
        if when == "after":
            compound()
        eq(L(b, o, "position"), posv(G, 5 * MIN, st), "%s: the session's position under its claim" % b.name)
        b.cp("B", nw, sid, 2, 7 * MIN, 7 * MIN)
        eq(L(b, o, "position"), posv(G, 7 * MIN, st), "%s: its next checkpoint continues it" % b.name)
        eq(b.queue(), [b.queue()[0], "Keep"], "%s: the survivor stays where it was" % b.name)
        eq(b.engine._current_queue()[0]["episode_id"], b.engine._root(o["episode_id"]), "%s: first" % b.name)
        eq(MEM(b, o), mem(G, True), "one queue item survives")


def test_merged_admissions():
    """PROTOCOL §9.7: when two admitted episodes merge, the earlier admission (by rank)
    keeps its filing and the later one's has no effect."""
    b = B("7c: two admitted episodes merge; the earlier admission files")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    b.fetch(P, [(gocc("first"), ofields("First", T0 - DAY, "https://h1.example.org/f.mp3"))])
    b.fetch(P, [(gocc("first"), ofields("First", T0 - DAY, "https://h1.example.org/f.mp3")),
                (gocc("old"), ofields("Same", T0, "https://h1.example.org/x.mp3"))])
    old = oref(P, G, "old")
    early = ADM(b, G, old)
    eq((early["result"]["action"], PS(b, old)), ("inbox", "new"), "the server admitted the old GUID's episode")
    nw = oref(P, G, "new", "https://h2.example.org/x.mp3", T0, "Same")
    p = b.discover("D", nw, action="queue", bundle=(K(None, None, "ma/new"), END), fields=ofields("Same", T0,
                   "https://h2.example.org/x.mp3"), newest=None)
    eq(b.out(p), ("applied", None), "a phone's proposal admits the new GUID's episode (a separate one so far)")
    eq(MEM(b, nw), mem(G, True), "and files it into the queue")
    b.fetch(P, [(gocc("first"), ofields("First", T0 - DAY, "https://h1.example.org/f.mp3")),
                (gocc("new"), ofields("Same", T0, "https://h2.example.org/x.mp3"))])
    check(b.made(rules.EPISODE_MERGE), "merged into the old episode")
    eq(L(b, old, "admission")["admission_id"], early["decision_id"], "the earlier admission is the episode's")
    eq((PS(b, old), MEM(b, old)), ("new", None), "only its filing counts: in the Inbox, not queued")


def test_podcast_merges():
    # Joined: both live; the newer podcast's fetch is permanently redirected to the older's URL.
    b = B("7c: podcast merge joining a live source into a live survivor")
    sp = b.subscribe("A", U1)
    P, GP = pg(sp)
    sq = b.subscribe("A", U2)
    Q, GQ = pg(sq)
    b.fetch(P, [(gocc("shared"), ofields("Shared", T0, "https://cdn.example.org/s.mp3"))])
    b.fetch(Q, [(gocc("shared"), ofields("Shared", T0, "https://cdn.example.org/s.mp3")),
                (gocc("only-q"), ofields("Only Q", T0 + DAY, "https://cdn.example.org/q.mp3"))])
    eq_s = oref(Q, GQ, "shared")
    b.fav("B", eq_s, True)
    oq = oref(Q, GQ, "only-q")
    b.mark_played("B", oq)
    n = len(b.engine.created)
    b.fetch(Q, [(gocc("shared"), ofields("Shared", T0, "https://cdn.example.org/s.mp3"))],
            redirects=[(301, U1B)], moved_to=U1B)
    pm = b.made(rules.PODCAST_MERGE, n)
    eq(len(pm), 1, "a podcast merge")
    r = pm[0]["result"]
    eq((r["source"], r["into"], r["generations"], r["properties"]),
       (Q, P, [{"generation_id": GQ, "mode": "joined", "into": GP}], "survivor"), "Q joins P's live generation")
    eq(sorted((m["from"], m["to"]) for m in r["episodes"]),
       sorted([(eq_s["episode_id"], eid(P, "rss-guid", "shared")), (oq["episode_id"], eid(P, "rss-guid", "only-q"))]),
       "explicit episode mappings by the survivor's rules")
    eq(b.rec("podcast", Q, "subscription"), {"state": "merged", "merged_into": P}, "Q merged")
    eq(L(b, oref(P, GP, "shared"), "favorite"), {"generation_id": GP, "on": True}, "Q's state on P's episode")
    eq(PS(b, oref(P, GP, "only-q")), "played", "and on a new survivor episode")
    eq(MERGED(b, oq["episode_id"]), eid(P, "rss-guid", "only-q"), "the source episode redirects")
    check(any(a["normalized"] == U2 for a in b.rec("podcast", P, "identity")["aliases"]), "Q's alias moved")
    # Routing of later facts naming the source.
    f = b.fav("C", oref(Q, GQ, "only-q"), True)
    eq((b.out(f), L(b, oref(P, GP, "only-q"), "favorite")), (("applied", None), {"generation_id": GP, "on": True}),
       "an episode fact naming the joined source generation applies to the survivor's live class")
    eq(b.out(b.pod("C", "archive", Q, GQ)), ("not_applied", "generation_merged"),
       "a lifecycle fact naming the joined source generation")
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": GP}, "the survivor's own")
    x = b.subscribe("D", U2, observed=obs(Q, GQ, "subscribed"))
    eq(b.res(x), {"podcast_id": P, "generation_id": GP, "mode": "joined"}, "a subscribe that observed Q joins P")
    expect_input_error(lambda: copy.deepcopy(b.engine).accept(
        {"fetch": {"refresh_id": det_uuid("q-refetch"), "podcast_id": Q, "outcome": "failure", "fetch_time": b.now,
                   "error": {"label": "I/O", "code": None, "detail": None}}, "receive_time": b.now}),
        "the server does not fetch a merged-away podcast")
    u = b.pod("A", "unsubscribe", P, GP)
    check(b.rec("episode", eid(P, "rss-guid", "only-q"), "play_state") is None,
          "an unsubscribe that observed the merge closes the joined class")
    eq(b.out(b.fav("C", oref(Q, GQ, "only-q"), False, after=[u])), ("not_applied", "generation_closed"),
       "then a fact naming the source generation is closed")


def test_podcast_merge_rehomed():
    # Scenario 34 (second part): a removed survivor merged with a live source; late facts
    # from closed generations are refused without suppressing the live source.
    b = B("7c scenario 34: a removed survivor merged with a live source")
    sp = b.subscribe("A", U1)
    P, GP = pg(sp)  # created first: the survivor
    b.fetch(P, [(gocc("p-old"), ofields("P old", T0, "https://cdn.example.org/p.mp3"))])
    po = oref(P, GP, "p-old")
    b.fav("A", po, True)
    b.pod("A", "unsubscribe", P, GP)
    sq = b.subscribe("B", U2, policy={"new_episodes_action": "nothing", "keep_updated": True})
    Q, GQ = pg(sq)
    b.fetch(Q, [(gocc("q-ep"), ofields("Q ep", T0 + DAY, "https://cdn.example.org/q.mp3"))])
    qe = oref(Q, GQ, "q-ep")
    b.mark_played("B", qe)
    n = len(b.engine.created)
    b.fetch(Q, [(gocc("q-ep"), ofields("Q ep", T0 + DAY, "https://cdn.example.org/q.mp3"))],
            redirects=[(308, U1)], moved_to=U1)
    r = b.made(rules.PODCAST_MERGE, n)[0]["result"]
    eq((r["source"], r["into"], r["generations"], r["properties"]),
       (Q, P, [{"generation_id": GQ, "mode": "rehomed", "into": GQ}], "source"),
       "the survivor's latest generation is removed: Q's live generation becomes P's")
    eq(b.rec("podcast", P, "subscription"), {"state": "subscribed", "generation_id": GQ}, "P is live again, as GQ")
    eq(b.rec("podcast", P, "policy"), {"new_episodes_action": "nothing", "keep_updated": True}, "with Q's policy")
    eq(b.rec("podcast", P, "identity")["refresh_url"], U1, "the moved refresh URL")
    qp = eid(P, "rss-guid", "q-ep")
    eq(PS(b, {"episode_id": qp}), "played", "the live source's state")
    late = b.fav("C", oref(P, GP, "p-old"), False)
    eq(b.out(late), ("not_applied", "generation_closed"), "a late fact from P's closed generation is refused")
    eq(PS(b, {"episode_id": qp}), "played", "without suppressing the live source")
    check(L(b, po, "favorite") is None, "the closed generation's state stays hidden")
    f = b.mark_played("C", oref(Q, GQ, "q-ep"), reset=False)
    eq(b.out(f), ("applied", None), "a fact naming the rehomed generation applies")
    eq(b.out(b.pod("C", "set_policy", Q, GQ, {"keep_updated": False})), ("applied", None),
       "a lifecycle fact naming the rehomed generation applies to the survivor")
    eq(b.rec("podcast", P, "policy")["keep_updated"], False, "on P")


def test_part_layout():
    """A fetch making identity decisions writes part 0
    (status, population, feed) before them; its items and `seen` follow in part 1, which is
    written with no item just to carry `seen`; only one part carries it."""
    for changed in (True, False):
        b = B("7c: part layout with identity evidence, %s" % ("a changed record" if changed else "no changed record"))
        s = b.subscribe("A", U1)
        P, G = pg(s)
        b.fetch(P, [(gocc("old"), ofields("Same", T0, "https://h1.example.org/x.mp3"))])
        x = oref(P, G, "x", "https://h1.example.org/y.mp3", T0 - 9 * DAY, "Seen")
        b.observe("B", P, [(x, None, ofields("Seen", T0 - 9 * DAY, "https://h1.example.org/y.mp3"))])
        enc = "https://h2.example.org/x.mp3" if changed else "https://h1.example.org/x.mp3"
        n = len(b.engine.created)
        b.fetch(P, [(gocc("new"), ofields("Same", T0, enc)),
                    (gocc("x"), ofields("Seen", T0 - 9 * DAY, "https://h1.example.org/y.mp3"))])
        made = sorted(b.engine.created[n:], key=lambda d: d["index"])
        eq([d["decision_type"] for d in made], [rules.CATALOG_REFRESH, rules.GUID_CHANGE, rules.CATALOG_REFRESH],
           "%s: part 0, the GUID change, then part 1" % b.name)
        p0, p1 = made[0], made[2]
        eq((p0["inputs"]["part"], sorted(p0["result"]), p0["result"]["items"]), (0, ["items", "status"], []),
           "%s: part 0 carries the status" % b.name)
        eq((p1["inputs"]["part"], [i["episode_id"] for i in p1["result"]["items"]], p1["result"].get("seen")),
           (1, [eid(P, "rss-guid", "old")] if changed else [], [x["episode_id"]]),
           "%s: part 1 carries the items and seen" % b.name)


def test_lookup_aliases_only_from_decisions():
    b = B("7c: device observations add evidence only; aliases come from server decisions")
    s = b.subscribe("A", U1)
    P, G = pg(s)
    b.fetch(P, [(gocc("one"), ofields("One", T0, "https://cdn.example.org/1.mp3"))])
    one = oref(P, G, "one")
    # A device claims the old episode for a new GUID (as a hosting move would look).
    claim = dict(oref(P, G, "two", "https://cdn.example.org/2.mp3", T0, "One"), episode_id=one["episode_id"])
    b.observe("B", P, [(claim, None, ofields("One", T0, "https://cdn.example.org/2.mp3"))])
    check(IR(b, P) is None, "no lookup alias from a device observation")
    eq(b.engine.lookup.get(P, {}), {}, "none")
    two = oref(P, G, "two")
    b.mark_played("C", two)
    check(PS(b, one) is None and PS(b, two) == "played", "the new GUID's ID stays its own episode")


# ---------------------------------------------------------------------------
# Releases: reference cases. Committed as contract/vectors/rules_releases.json.
# ---------------------------------------------------------------------------

RELEASES = "rules_releases.json"


def e0ref(P, G, label, title):
    """An EpisodeRef of an `e0:` episode without GUID or enclosure (PROTOCOL §7.2: k
    "none", v "")."""
    return {"episode_id": "e0:" + det_uuid(label), "podcast_id": P, "generation_id": G,
            "occurrence": {"k": "none", "v": "", "title": title}}


def test_releases_newest_first():
    """A newest-first feed reusing a GUID. All evidence and disputes of the
    fetch are settled before any item is derived, so its first fetch keeps both releases."""
    b = B("a newest-first first fetch reusing a GUID", 0x7c1a0004, RELEASES)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.com/one.mp3", "https://cdn.example.com/two.mp3"
    b.fetch(P, [(gocc("g"), ofields("Two", T0 + 30 * DAY, u2)), (gocc("g"), ofields("One", T0, u1))])
    orig, later = eid(P, "rss-guid", "g"), rge(P, "g", u2)
    eq([(m["result"]["episode_id"], m["result"]["original"], m["result"]["publication_time"])
        for m in b.made(rules.MAPPING)], [(orig, True, T0), (later, False, T0 + 30 * DAY)],
       "the earliest release keeps the original ID, the later one its own")
    eq((L(b, {"episode_id": orig}, "metadata")["title"], L(b, {"episode_id": later}, "metadata")["title"]),
       ("One", "Two"), "each release has its own item")
    # A phone that does not know the dispute names the plain ID: routed by its date.
    f = b.fav("B", oref(P, G, "g", u2, T0 + 30 * DAY, "Two"), True)
    eq((b.out(f), L(b, {"episode_id": later}, "favorite"), L(b, {"episode_id": orig}, "favorite")),
       (("applied", None), {"generation_id": G, "on": True}, None), "the later release's favorite")


def test_releases_signed_enclosure():
    """A newly observed (signed) enclosure of an existing release is added to
    that release, persisted as a decision and published, so an undated reference by that
    URL routes to the release. By a server fetch and by a phone's observation."""
    b = B("signed enclosure URLs added to their release", 0x7c1a0006, RELEASES)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.com/one.mp3", "https://cdn.example.com/two.mp3"
    u3, u4 = u2 + "?sig=new", u2 + "?sig=phone"
    orig, later = eid(P, "rss-guid", "g"), rge(P, "g", u2)
    b.fetch(P, [(gocc("g"), ofields("One", T0, u1))])
    b.fetch(P, [(gocc("g"), ofields("Two", T0 + 30 * DAY, u2))])
    n = len(b.engine.created)
    b.fetch(P, [(gocc("g"), ofields("Two", T0 + 30 * DAY, u3))])
    eq([(m["subject"], m["inputs"], m["result"]) for m in b.made(rules.MAPPING, n)],
       [("occurrence:%s:g:%s:%s" % (P, later, u3), {"podcast_id": P, "guid": "g", "found_by": None},
         {"episode_id": later, "enclosure": u3})], "the fetch's enclosure addition")
    f = b.fav("B", oref(P, G, "g", u3), True)
    eq((b.out(f), L(b, {"episode_id": later}, "favorite"), L(b, {"episode_id": orig}, "favorite")),
       (("applied", None), {"generation_id": G, "on": True}, None), "an undated reference by the signed URL")
    # A phone that synced the rules derives the release and observes another signed URL.
    n = len(b.engine.created)
    r4 = dict(oref(P, G, "g", u4, T0 + 30 * DAY + 3600000, "Two"), episode_id=later)
    o = b.observe("C", P, [(r4, None, ofields("Two", T0 + 30 * DAY + 3600000, u4))])
    eq([(m["inputs"]["found_by"], m["result"]) for m in b.made(rules.MAPPING, n)],
       [(o, {"episode_id": later, "enclosure": u4})], "the observation's enclosure addition")
    eq(IR(b, P)["disputed_guids"], [{"guid": "g", "releases": [
        {"episode_id": orig, "publication_time": T0, "enclosures": [u1]},
        {"episode_id": later, "publication_time": T0 + 30 * DAY, "enclosures": [u2, u3, u4]}]}],
       "published in identity_rules")
    b.mark_played("D", oref(P, G, "g", u4))
    eq((PS(b, {"episode_id": later}), PS(b, {"episode_id": orig})), ("played", None),
       "an undated reference by the phone's signed URL")


def test_releases_delayed_dispute():
    """A dispute first detected after the GUID's occurrences span several
    releases partitions the existing evidence by release (one enclosure reused 39 days apart, then a signed variant of the first release)."""
    b = B("the first dispute partitions the evidence by release", 0x7c1a0003, RELEASES)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.com/latest.mp3", "https://cdn.example.com/latest.mp3?sig=old"
    d1, d2 = T0 - 40 * DAY, T0 - DAY
    for title, u, d in (("One", u1, d1), ("Two", u1, d2), ("One", u2, d1), ("Two", u1, d2)):
        b.wait(60000)
        b.fetch(P, [(gocc("g"), ofields(title, d, u))])
    orig, later = eid(P, "rss-guid", "g"), rge(P, "g", u1)
    eq(IR(b, P)["disputed_guids"], [{"guid": "g", "releases": [
        {"episode_id": orig, "publication_time": d1, "enclosures": [u1, u2]},
        {"episode_id": later, "publication_time": d2, "enclosures": [u1]}]}],
       "two releases, 39 days apart, each with its own enclosures")
    b.fav("B", oref(P, G, "g", u1, d2), True)
    eq((L(b, {"episode_id": later}, "favorite"), L(b, {"episode_id": orig}, "favorite")),
       ({"generation_id": G, "on": True}, None), "a dated reference to the later release")
    eq((L(b, {"episode_id": orig}, "metadata")["title"], L(b, {"episode_id": later}, "metadata")["title"]),
       ("One", "Two"), "each release's item")


def test_releases_shared_new_guid():
    """A changed GUID now shared: two old episodes whose items now carry one
    new GUID. A new GUID several items carry is a reused GUID, not a changed one: no GUID
    change (competing evidence merges nothing); its items become its releases."""
    b = B("a new GUID two items share is no GUID change", 0x7c1a0007, RELEASES)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.com/one.mp3", "https://cdn.example.com/two.mp3"
    ra, rb = oref(P, G, "old-a", u1, T0), oref(P, G, "old-b", u2, T0 + 30 * DAY)
    b.fetch(P, [(gocc("old-b"), ofields("Two", T0 + 30 * DAY, u2)), (gocc("old-a"), ofields("One", T0, u1))])
    b.fav("A", rb, True)
    n = len(b.engine.created)
    b.fetch(P, [(gocc("g"), ofields("Two", T0 + 30 * DAY, u2)), (gocc("g"), ofields("One", T0, u1))])
    eq((b.made(rules.GUID_CHANGE, n), b.made(rules.EPISODE_MERGE, n)), ([], []), "no GUID change, no merge")
    eq([m["result"]["episode_id"] for m in b.made(rules.MAPPING, n)], [eid(P, "rss-guid", "g"), rge(P, "g", u2)],
       "the new GUID's two releases")
    eq(L(b, rb, "favorite"), {"generation_id": G, "on": True}, "the old episode keeps its state")


def test_releases_repeated_items():
    """Repeated items of one fetch (PROTOCOL §15 item 6): the
    server keeps the first item per derived episode."""
    b = B("repeated items in one fetch, the first kept", 0x7c1a0008, RELEASES)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    b.fetch(P, [(gocc("dup"), ofields("First", T0, "https://cdn.example.com/dup.mp3?ad=1")),
                (gocc("dup"), ofields("Second", T0 + DAY, "https://cdn.example.com/dup.mp3?ad=2"))])
    eq(L(b, {"episode_id": eid(P, "rss-guid", "dup")}, "metadata")["title"], "First", "the first item's record")


def test_releases_merge_carries_e0():
    """A podcast merge carries every episode of the source to the survivor,
    including one without GUID and enclosure (an `e0:` episode, k "none"), with its state."""
    b = B("a podcast merge carries an episode without GUID and enclosure", 0x7c1a0002, RELEASES)
    sp = b.subscribe("A", U1)
    P, GP = pg(sp)
    sq = b.subscribe("A", U2)
    Q, GQ = pg(sq)
    b.fetch(P, [(gocc("p-one"), ofields("P one", T0, "https://cdn.example.org/p1.mp3"))])
    b.fetch(Q, [(gocc("q-one"), ofields("Q one", T0, "https://cdn.example.org/q1.mp3"))])
    x = e0ref(Q, GQ, "podcast merge/anonymous", "Anonymous")
    eq(b.out(b.fav("B", x, True)), ("applied", None), "an e0: reference (k none) is accepted")
    b.mark_played("B", x, reset=False)
    n = len(b.engine.created)
    b.fetch(Q, [(gocc("q-one"), ofields("Q one", T0, "https://cdn.example.org/q1.mp3"))], redirects=[(301, U1)],
            moved_to=U1)
    pm = b.made(rules.PODCAST_MERGE, n)
    eq([(m["result"]["source"], m["result"]["into"], [e["from"] for e in m["result"]["episodes"]]) for m in pm],
       [(Q, P, [eid(Q, "rss-guid", "q-one")])], "the merge lists the derivable episodes")
    E = x["episode_id"]
    eq(b.rec("episode", E, "identity"), {"podcast_id": P, "occurrence": x["occurrence"]},
       "the e0: episode is the survivor's now")
    eq((L(b, x, "favorite"), PS(b, x)), ({"generation_id": GP, "on": True}, "played"), "with its state")
    f1 = b.fav("C", x, False)
    eq((b.out(f1), L(b, x, "favorite")), (("applied", None), {"generation_id": GP, "on": False}),
       "a later fact naming the source routes to it")
    f2 = b.ep("C", "episode.mark_unplayed", e0ref(P, GP, "podcast merge/anonymous", "Anonymous"))
    eq((b.out(f2), PS(b, x)), (("applied", None), "unplayed"), "and one naming the survivor applies")


def test_identity_generator(ncases):
    rng = random.Random(7709)
    seen = dict((t, 0) for t in rules.IDENTITY_DECISIONS)
    for k in range(ncases):
        case, eng = gen_rule_cases.generate_case(rng, identity=True)
        res = check_rules.check_case(json.loads(json.dumps(case, ensure_ascii=False)))
        check(res is None, "generated identity case %d fails the checker: %s" % (k, res))
        for d in case["expected"].get("decisions", []):
            if d["decision_type"] in seen:
                seen[d["decision_type"]] += 1
    check(all(v > 0 for v in seen.values()), "the identity generator makes every identity decision: %s" % seen)


def test_identity_permutations(ncases, nperms):
    """Arrival permutations of random identity cases (with the decisions held fixed) give the
    same records."""
    rng = random.Random(7710)
    for k in range(ncases):
        case, eng = gen_rule_cases.generate_case(rng, identity=True)
        want = eng.records()
        first = {}
        for a in case["arrivals"]:
            if "fact" not in a:
                continue
            op = a["fact"]["op_id"]
            if op not in first:
                a = json.loads(json.dumps(a))
                a.pop("decisions", None)
                a["receipt"] = dict(zip(("epoch", "seq", "index"), eng.facts[op]["rkey"]))
                first[op] = a
        nodes = list(first.values()) + eng.decision_arrivals()
        for p in range(nperms):
            perm = linear_extension(rng, nodes, rng.randint(0, 2))
            replay = rules.Engine(case["account_id"], case["namespace"], eng.resolutions, eng.alias_decisions(),
                                  eng.episode_decisions(), replay=True, fixed_outcomes=eng.outcomes)
            for a in perm:
                replay.accept(a)
            eq(replay.records(), want, "identity permutation %d of random identity case %d" % (p, k))
            eq(replay.records(), replay.full_records(), "replay projection vs full recompute")


# ---------------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------------


def test_incremental_order_equals_kahn(trials):
    rng = random.Random(20261005)
    for trial in range(trials):
        n = rng.randint(1, 14)
        nodes = []
        for i in range(n):
            nid = "%x" % rng.getrandbits(rng.choice([4, 8, 16]))
            while any(nd["id"] == nid for nd in nodes):
                nid += "%x" % rng.getrandbits(4)
            deps = rng.sample([nd["id"] for nd in nodes], rng.randint(0, min(3, len(nodes))))
            t = rng.randint(0, 6)
            if rng.random() < 0.5 and deps:  # normalized times respect dependencies
                t = max(t, max(nd["normalized_time"] for nd in nodes if nd["id"] in deps))
            nodes.append({"id": nid, "dependencies": deps, "normalized_time": t})
        want = c.causal_order(nodes)
        for _ in range(3):
            placed, order = set(), []
            pending = list(nodes)
            while pending:
                ready = [nd for nd in pending if set(nd["dependencies"]) <= placed]
                nd = rng.choice(ready)
                pending.remove(nd)
                placed.add(nd["id"])
                order.append(nd)
            eng = rules.Engine(ACC, NS)
            for nd in order:
                eng._insert_node(nd["id"], nd["normalized_time"], nd["dependencies"])
            eq(eng.order, want, "incremental order (trial %d)" % trial)


def _node_of(a):
    if "decision" in a:
        return a["decision"]["decision_id"], set(a["decision"]["dependencies"])
    f = a["fact"]
    return f["op_id"], set(f["dependencies"]) | ({f["previous_op_id"]} if "previous_op_id" in f else set())


def linear_extension(rng, arrivals, duplicates):
    """A random causal delivery order of fact and decision arrivals, with duplicates."""
    deps = dict(_node_of(a) for a in arrivals)
    pending = list(arrivals)
    placed = set()
    out = []
    while pending:
        ready = [a for a in pending if deps[_node_of(a)[0]] <= placed]
        a = rng.choice(ready)
        pending.remove(a)
        placed.add(_node_of(a)[0])
        out.append(a)
    for _ in range(duplicates):
        i = rng.randrange(len(out))
        out.insert(rng.randint(i + 1, len(out)), out[i])
    return out


def test_permutations(ncases, nperms):
    """PROTOCOL §7.6: every causal delivery order of the same facts *and the same persisted
    decisions* (recovered as decision arrivals; the replaying engine decides nothing) gives
    the same records."""
    # The seed moved from 4242 to 4243 with the queue replay (the devices' queue draws
    # changed the random stream; 4242's first cases hold no rejected fact any more).
    rng = random.Random(4243)
    checked = 0
    stats = {"decisions": 0, "derived": 0, "forks": 0, "rejected": 0}
    for k in range(ncases):
        # Copies (PROTOCOL §6.10): every second case copies a library; the receipts (and their outcomes)
        # are persisted, so chain positions and session admission replay identically.
        case, eng = gen_rule_cases.generate_case(rng, bogus=True, mismatch=True, copies=k % 2 == 0)
        if not case["arrivals"]:
            continue
        want = eng.records()
        res, aliases, episodes = eng.resolutions, eng.alias_decisions(), eng.episode_decisions()
        first = {}
        for a in case["arrivals"]:
            op = a["fact"]["op_id"]
            if op not in first:  # a transport duplicate replays its first acceptance
                a = json.loads(json.dumps(a))
                a.pop("decisions", None)
                a["receipt"] = dict(zip(("epoch", "seq", "index"), eng.facts[op]["rkey"]))
                first[op] = a
        nodes = list(first.values()) + eng.decision_arrivals()
        stats["decisions"] += len(eng.decisions)
        stats["derived"] += len(eng.derived_parent)
        stats["forks"] += len(eng.forks)
        stats["rejected"] += sum(1 for o in eng.outcomes.values() if o["outcome"] == "rejected")
        for p in range(nperms):
            perm = linear_extension(rng, nodes, rng.randint(0, 3))
            replay = rules.Engine(case["account_id"], case["namespace"], res, aliases, episodes, replay=True,
                                  fixed_outcomes=eng.outcomes)
            for a in perm:
                replay.accept(a)
            eq(replay.records(), want, "permutation %d of random case %d" % (p, k))
            eq(replay.records(), replay.full_records(), "replay projection vs full recompute")
        checked += 1
    check(checked > ncases // 2, "permutation test ran on too few cases")
    check(stats["decisions"] > 0, "the permutation test met no queue decision")
    check(stats["derived"] > 0 and stats["forks"] > 0 and stats["rejected"] > 0,
          "the permutation test met no derived branch, fork or rejected fact: %s" % stats)


def test_save_tree_ground_truth(ncases):
    """PROTOCOL §6.10 against the generator's ground truth: for every family, the played time the
    engine reconstructs from the exports' save identities (`first`, `index`) and the
    continuations' writing histories equals the sum over the full journals (every save of
    every library, exported or not) of the edges spanning the exported saves -- the shared
    part once, each continuation's own change, decreases included."""
    rng = random.Random(6150)
    families = branched = 0
    for k in range(ncases):
        sim = gen_rule_cases.generate_sim(rng, copies=True)
        eng, truth = sim.engine, sim.save_truth
        for key in [x for x in eng.scopes if x[0] == "episode"]:
            cts = eng._sorted(key)
            for sid, plan in eng._session_plans(cts).items():
                exports = [ct for ct in cts if ct.kind in ("checkpoint", "end") and ct.data["session_id"] == sid]
                if not exports or any("save" not in ct.data for ct in exports):
                    continue
                span = set()
                for ct in exports:
                    x = ct.data["save"]["id"]
                    while x is not None and x not in span:
                        span.add(x)
                        x = truth[x][0]
                want = max(0, sum(truth[x][1] - (truth[truth[x][0]][1] if truth[x][0] is not None else 0) for x in span))
                tree, nodes = plan["tree"], plan["nodes"]
                c = lambda n: 0 if n == ("root",) else nodes[n]["contribution"]  # noqa: E731
                got = max(0, sum(c(n) - c(p) for n, p in tree.items() if p is not None))
                eq(got, want, "case %d family %s: save tree vs ground truth" % (k, sid))
                families += 1
                branched += len(set(plan["cont"][ct.node] for ct in exports)) > 1
    check(branched >= ncases // 10, "the ground-truth test met too few branched families (%d of %d)" % (branched, families))


def test_generator(ncases):
    rng = random.Random(77)
    a = [gen_rule_cases.generate_case(rng)[0] for _ in range(ncases)]
    rng = random.Random(77)
    b2 = [gen_rule_cases.generate_case(rng)[0] for _ in range(ncases)]
    eq(json.dumps(a, sort_keys=True), json.dumps(b2, sort_keys=True), "generator is deterministic per seed")
    for i, case in enumerate(a):
        doc = json.loads(json.dumps({"suite": "rules", "cases": [case]}, ensure_ascii=False))
        res = check_rules.check_case(doc["cases"][0])
        check(res is None, "generated case %d fails the checker: %s" % (i, res))
        for arr in case["arrivals"]:
            check(c.canonical(arr["fact"]) is not None, "fact canonicalizes")


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# PROTOCOL §6.11: recovery
# ---------------------------------------------------------------------------


def uploads(b, after, seed=88):
    """The recovery uploads of builder b's commits after arrival `after` (whole commits, in
    position order): facts with their receipts, decisions as recorded (implicit
    subscription decisions with drawn IDs)."""
    commits = gen_rule_cases.archive_commits(b.engine, random.Random(seed))
    return [commits[k] for k in sorted(commits) if k[1] > after]


def restored(live, k, name):
    """A restored server holding the first k arrivals of `live` (its devices as they were)."""
    rs = B(name, live.prefix)
    rs.server_label = live.server_label + "/restored"
    rs.devs = json.loads(json.dumps(live.devs))
    for a in live.arrivals[:k]:
        a = json.loads(json.dumps(a))
        rs.engine.accept(a)  # with the original's recorded choices
        rs.arrivals.append(a)
    rs.now = live.now
    return rs


def rend(b):
    b.wait(1000)
    b.accept({"recovery_end": True, "receive_time": b.now})


def upload(b, entries):
    for x in entries:
        b.accept(json.loads(json.dumps(x)))


# ---------------------------------------------------------------------------
# episode.leave (PROTOCOL §8.3, §8.4). Committed as
# contract/vectors/rules_episode_leave.json.
# ---------------------------------------------------------------------------

LEAVE_FILE = "rules_episode_leave.json"


def statuses(b, r, ops):
    """The fold status of each fact in the episode's live scope ("fired", "removed",
    "void", "noop")."""
    st = b.engine._fold_episode_scope(r["episode_id"], r["generation_id"]).status
    return [st.get(x) for x in ops]


def test_leave_completes_under_observed_claim():
    b = B("leave: a leave completing under the observed claim", 0x6a1e0001, LEAVE_FILE)
    P, G, (e,) = setup_listening(b)
    [ke] = keys_in_order(["leave1/e"])
    b.qadd("A", [(e, ke)])
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, D - 10000, D - 10000)  # A pauses 10 s before the end
    lp = L(b, e, "last_played")
    # B loaded the episode paused at A's position and switches away: it is within B's
    # smart threshold, so B completes it under the claim it displayed.
    x = b.leave("B", e, b.uuid(), completed=True, observed=a1, position=D - 10000, after=[a2])
    eq(b.out(x), ("applied", None), "its bundle fired")
    eq((PS(b, e), L(b, e, "position"), MEM(b, e)), ("played", posv(G, 0, a1), mem(G, False, "completed")),
       "the completion bundle under the observed claim, which stays A's")
    eq((L(b, e, "history"), L(b, e, "last_played")), ({"generation_id": G, "time": b.nt(x)}, lp),
       "a history mark at its time; never last played")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": D - 10000}, "no contribution")


def test_leave_takeover_before_it():
    out = []
    prefix = 0x6a1e0002
    for first in ("leave", "takeover"):
        b = B("leave: a takeover ordered before it suppresses its completion, %s delivered first" % first, prefix,
              LEAVE_FILE)
        s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
        P, G = pg(s)
        e = ref(P, G, "ep-1", "Episode 1")
        sa, sc = fixed_op(prefix, 100), fixed_op(prefix, 101)
        a1 = b.start("A", e, sa, 50 * MIN, duration=D, op=fixed_op(prefix, 2))
        a2 = b.cp("A", e, sa, 1, D - 10000, D - 10000 - 50 * MIN, op=fixed_op(prefix, 3))
        base = b.now
        T = fixed_op(prefix, 200)

        def leave(rt):
            return b.leave("B", e, T, completed=True, observed=a1, position=D - 10000, after=[a2], t=base + 2000,
                           rt=rt, op=fixed_op(prefix, 4))

        def take(rt):
            return b.start("C", e, sc, D - 10000, observed=a1, after=[a2], t=base + 1000, rt=rt, op=fixed_op(prefix, 5))
        if first == "leave":
            x = leave(base + 5000)
            eq((b.out(x), PS(b, e)), (("applied", None), "played"), "the bundle fires at first")
            t = take(base + 6000)
        else:
            t = take(base + 5000)
            x = leave(base + 7000)
            eq(b.out(x), ("superseded", None), "its completion lost to the takeover")
        check(b.engine.rank[t] < b.engine.rank[x], "the takeover is ordered before the leave")
        eq((PS(b, e), L(b, e, "position")), ("unplayed", posv(G, D - 10000, t)), "%s: no completion" % b.name)
        eq(L(b, e, "history"), {"generation_id": G, "time": b.nt(x)}, "its history mark still counts")
        out.append(b)
    eq(out[0].engine.records(), out[1].engine.records(), "both delivery orders converge")


def test_leave_history_cuts():
    b = B("leave: its history mark covered and not covered by cuts", 0x6a1e0003, LEAVE_FILE)
    P, G, (e, f) = setup_listening(b, 2)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0)
    a2 = b.cp("A", e, sa, 1, 10 * MIN, 10 * MIN)
    lp = L(b, e, "last_played")
    # Two devices switch away from e outside their threshold: a history mark each.
    x1 = b.leave("B", e, b.uuid(), position=10 * MIN, observed=a1, after=[a2])
    x2 = b.leave("D", e, b.uuid(), position=10 * MIN, observed=a1, after=[a2])
    eq([b.out(x) for x in (x1, x2)], [("applied", None)] * 2, "a mark only")
    eq((PS(b, e), L(b, e, "history"), L(b, e, "last_played")),
       ("unplayed", {"generation_id": G, "time": b.nt(x2)}, lp), "history moves, last played does not")
    r = b.hremove("C", e, after=[x1])  # observes A's listening and B's mark, not D's
    eq(b.out(r), ("applied", None), "removal")
    eq(L(b, e, "history"), {"generation_id": G, "time": b.nt(x2)}, "D's unobserved mark survives the removal")
    hc = b.hclear("C", after=[x2])
    eq((b.out(hc), L(b, e, "history"), L(b, e, "last_played")), (("applied", None), {"generation_id": G, "time": None}, lp),
       "a clear observing D's mark covers it; last played stays")
    # A leave of an episode nobody played: its mark alone makes the history record.
    x3 = b.leave("B", f, b.uuid(), position=0)
    eq((L(b, f, "history"), L(b, f, "last_played"), PS(b, f), L(b, f, "position")),
       ({"generation_id": G, "time": b.nt(x3)}, None, None, None), "only a history record")


def test_leave_and_admissions():
    b = B("6a leave: not a barrier and no NEW dismissal; admissions after it file NEW", 0x6a1e0004, LEAVE_FILE)
    P, G, s, o = setup_catalog(b)
    e1, e2, e3 = (ref(P, G, "fresh-%d" % i, "Fresh %d" % i) for i in (1, 2, 3))
    # A phone loaded e1 and e3 paused from its own refresh and switched away before the
    # server discovered them: e1 outside the threshold, e3 within it (no claim exists and
    # the phone displayed none: a claimless completion).
    x1 = b.leave("B", e1, b.uuid(), position=0)
    x3 = b.leave("B", e3, b.uuid(), completed=True, position=D - 1000)
    eq([b.out(x) for x in (x1, x3)], [("applied", None)] * 2, "applied")
    b.fetch(P, [(o, ifields("Old", T0 - DAY))] + [(r, ifields(r["occurrence"]["title"], T0)) for r in (e1, e2, e3)])
    eq([ADM(b, G, r)["result"]["action"] for r in (e1, e2, e3)], ["inbox"] * 3, "the server admits all three")
    eq(PS(b, e1), "new", "a leave before the admission is no barrier: the admission files NEW")
    x2 = b.leave("B", e2, b.uuid(), position=0)
    eq((b.out(x2), PS(b, e2)), (("applied", None), "new"), "a leave after the admission does not dismiss NEW")
    eq((PS(b, e3), L(b, e3, "position")), ("played", {"generation_id": G, "position": 0}),
       "a completion is explicit: the automatic NEW does not undo it")


def test_leave_then_owning_checkpoint():
    b = B("leave: a later checkpoint of the owning session moves the position", 0x6a1e0005, LEAVE_FILE)
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, D - 20000, D - 20000)  # A is still playing
    x = b.leave("B", e, b.uuid(), completed=True, observed=a1, position=D - 20000, after=[a2])
    eq((PS(b, e), L(b, e, "position")), ("played", posv(G, 0, a1)), "B completes under A's claim")
    a3 = b.cp("A", e, sa, 2, D - 5000, D - 5000)  # A never saw it: its session still owns the position
    eq((PS(b, e), L(b, e, "position")), ("played", posv(G, D - 5000, a1)),
       "the owning session moves the position under the same claim; still played")
    a4 = b.end("A", e, sa, 3, D, D, completed=True)
    eq((PS(b, e), L(b, e, "position")), ("played", posv(G, 0, a1)), "A's natural end completes")
    eq([b.out(y) for y in (x, a3, a4)], [("applied", None)] * 3, "outcomes")
    eq(L(b, e, "played_time"), {"generation_id": G, "played_ms": D}, "A's listening counts, B's leave adds nothing")


def test_leave_copied_pending():
    """Two copies of B's library inherited one pending leave (one transition ID). With
    and without a mark unplayed between their leaves, in both orders, at most one
    completes."""
    finals = {}
    for mark in (False, True):
        for first in ("B", "B2", "late"):
            if first == "late" and not mark:
                continue
            prefix = 0x6a1e0006
            name = "leave: copied pending leaves, %s, %s" % (
                "a mark unplayed between them" if mark else "nothing between them",
                {"B": "the original's first", "B2": "the copy's first",
                 "late": "the copy's first, delivered after the original's"}[first])
            # The same history delivered in two orders shares its builder prefix (its devices).
            b = B(name, prefix + (2 if mark else 0) + (1 if first == "B2" else 0), LEAVE_FILE)
            b.server_label = "6a leave copied"
            s = b.subscribe("A", U1, op=fixed_op(prefix, 1))
            P, G = pg(s)
            e = ref(P, G, "ep-1", "Episode 1")
            sa = fixed_op(prefix, 100)
            a1 = b.start("A", e, sa, 0, duration=D, op=fixed_op(prefix, 2))
            a2 = b.cp("A", e, sa, 1, D - 10000, D - 10000, op=fixed_op(prefix, 3))
            b.dev("B")
            b.clone("B", "B2")  # B's library copied while it held the pending leave
            T = fixed_op(prefix, 200)
            base = b.now
            one, two = ("B", "B2") if first == "B" else ("B2", "B")

            def leave(dev, t, rt, k):
                return b.leave(dev, e, T, completed=True, observed=a1, position=D - 10000, after=[a2], t=t, rt=rt,
                               op=fixed_op(prefix, k))
            if first == "late":
                y = leave(one, base + 3000, base + 5000, 11)  # the second in order arrives first
                eq(b.out(y), ("applied", None), "alone, it completes")
                x = leave(two, base + 1000, base + 6000, 10)
                m = b.ep("C", "episode.mark_unplayed", e, after=[x], t=base + 2000, rt=base + 7000, op=fixed_op(prefix, 12))
                eq(b.out(x), ("applied", None), "ordered first, it completes; the other is void now")
            else:
                x = leave(one, base + 1000, base + 5000, 10)
                m = None
                if mark:
                    m = b.ep("C", "episode.mark_unplayed", e, after=[x], t=base + 2000, rt=base + 6000,
                             op=fixed_op(prefix, 12))
                    eq(PS(b, e), "unplayed", "the person marks it unplayed (the claim stays A's)")
                y = leave(two, base + 3000, base + 7000, 11)
                eq(b.out(y), ("superseded", None), "%s: the transition's effective leave came first" % name)
            eq(statuses(b, e, [x, y]), ["fired", "void"], "%s: one completion" % name)
            eq((PS(b, e), L(b, e, "position")), ("unplayed" if mark else "played", posv(G, 0, a1)),
               "%s: never completed twice" % name)
            eq(L(b, e, "history"), {"generation_id": G, "time": max(b.nt(x), b.nt(y))}, "both marks count")
            if first != "B2":
                finals.setdefault(mark, []).append(b.engine.records())
    eq(finals[True][0], finals[True][1], "delivery order does not matter")


def test_leave_first_effective():
    """The once-per-transition rule counts the first *effective* leave: the original's leave
    loses to a takeover; the copy adopted the takeover's claim and its leave of the same
    transition completes."""
    b = B("leave: copied pending leaves, the copy's leave is the first effective one", 0x6a1e000b, LEAVE_FILE)
    P, G, (e,) = setup_listening(b)
    sa, sc = b.uuid(), b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, D - 10000, D - 10000)
    b.dev("B")
    b.clone("B", "B2")
    T = b.uuid()
    c1 = b.start("C", e, sc, D - 10000, observed=a1, after=[a2])
    c2 = b.cp("C", e, sc, 1, D - 8000, 2000)  # C pauses 8 s before the end
    x = b.leave("B", e, T, completed=True, observed=a1, position=D - 10000, after=[a2])
    y = b.leave("B2", e, T, completed=True, observed=c1, position=D - 8000, after=[c2])
    eq([b.out(z) for z in (x, y)], [("superseded", None), ("applied", None)], "outcomes")
    eq(statuses(b, e, [x, y]), ["noop", "fired"], "the copy's leave is the transition's effective one")
    eq((PS(b, e), L(b, e, "position")), ("played", posv(G, 0, c1)), "completed under C's claim")


def test_leave_claimless():
    b = B("leave: claimless completion", 0x6a1e0007, LEAVE_FILE)
    P, G, (e, f, g) = setup_listening(b, 3)
    [ke] = keys_in_order(["claimless/e"])
    b.qadd("A", [(e, ke)])
    x = b.leave("B", e, b.uuid(), completed=True, position=D - 5000)
    eq((b.out(x), PS(b, e), L(b, e, "position"), MEM(b, e)),
       (("applied", None), "played", {"generation_id": G, "position": 0}, mem(G, False, "completed")),
       "no claim and no observed claim: completes, position 0 without a claim")
    sa = b.uuid()
    a1 = b.start("A", f, sa, 0, duration=D)
    a2 = b.cp("A", f, sa, 1, D - 5000, D - 5000)
    y = b.leave("B", f, b.uuid(), completed=True, position=D - 5000, after=[a2])
    eq((b.out(y), PS(b, f), L(b, f, "position")), (("superseded", None), "unplayed", posv(G, D - 5000, a1)),
       "no observed claim while a claim exists: no completion")
    z = b.leave("B", g, b.uuid(), completed=True, observed=a1, position=D - 5000)
    eq((b.out(z), PS(b, g), L(b, g, "position"), L(b, g, "history")),
       (("superseded", None), None, None, {"generation_id": G, "time": b.nt(z)}),
       "an observed claim while none exists: no completion, only the history mark")


def test_leave_skip_removal():
    b = B("leave: a skip's queue removal, once per transition", 0x6a1e0008, LEAVE_FILE)
    P, G, (e,) = setup_listening(b)
    [ke] = keys_in_order(["skip/e"])
    b.qadd("A", [(e, ke)])
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0, duration=D)
    a2 = b.cp("A", e, sa, 1, 10 * MIN, 10 * MIN)
    b.dev("B")
    b.clone("B", "B2")
    T = b.uuid()
    x = b.leave("B", e, T, reason="skipped", rfq=True, observed=a1, position=10 * MIN, after=[a2])
    eq((b.out(x), MEM(b, e), PS(b, e)), (("applied", None), mem(G, False, "skipped"), "unplayed"),
       "a skip outside the threshold with Keep skipped episodes off")
    r = b.qadd("C", [(e, K(ke[0], None, "skip/e-again"))], after=[x])
    eq(MEM(b, e), mem(G, True), "added again")
    y = b.leave("B2", e, T, reason="skipped", rfq=True, observed=a1, position=10 * MIN, after=[a2])
    eq((b.out(y), MEM(b, e)), (("superseded", None), mem(G, True)), "the copy's leave of the same transition")
    z = b.leave("B", e, b.uuid(), reason="skipped", rfq=True, observed=a1, position=10 * MIN, after=[r])
    eq((b.out(z), MEM(b, e)), (("applied", None), mem(G, False, "skipped")), "a new transition removes again")
    eq(statuses(b, e, [x, y, z]), ["removed", "void", "removed"], "statuses")


def test_leave_checks():
    b = B("leave: payload and episode checks", 0x6a1e0009, LEAVE_FILE)
    P, G, (e,) = setup_listening(b)
    sa = b.uuid()
    a1 = b.start("A", e, sa, 0)
    et = [{"entity": "episode", "id": e["episode_id"], "generation": G}]
    lv = {"episode": e, "transition_id": b.uuid(), "reason": "switched", "position": 0, "observed_claim": a1,
          "completed": False, "threshold_ms": 30000, "remove_from_queue": False, "history": True}

    def w(**kv):
        x = dict(lv)
        for k, v in kv.items():
            if v is None:
                x.pop(k)
            else:
                x[k] = v
        return x
    bad = [w(transition_id=None), w(transition_id="T1"), w(reason="paused"), w(reason=None), w(position=-1),
           w(position="0"), w(observed_claim="claim"), w(completed="yes"), w(threshold_ms=-1),
           w(remove_from_queue=None), w(history=None), w(session_id=sa), w(ordinal=1)]
    for p in bad:
        x = b.push("A", "episode.leave", p, et, reject=True)
        eq(b.out(x), ("rejected", "invalid_payload"), json.dumps(p)[:120])
    x = b.push("A", "episode.leave", lv, [], reject=True)
    eq(b.out(x), ("rejected", "invalid_payload"), "targets must be the episode and its generation")
    ok = b.push("A", "episode.leave", w(observed_claim=None, reason="ended", threshold_ms=0), et)
    eq(b.out(ok), ("applied", None), "observed_claim is optional; ended is a reason")
    Q = c.podcast_id(NS, det_uuid("6a leave unknown podcast"))
    u = dict(e, podcast_id=Q, generation_id=c.generation_id(NS, Q, det_uuid("6a leave unknown generation")))
    x = b.push("A", "episode.leave", w(episode=u), [{"entity": "episode", "id": u["episode_id"],
                                                    "generation": u["generation_id"]}])
    eq(b.out(x), ("not_applied", "unknown_podcast"), "the episode acceptance checks")
    b.pod("A", "unsubscribe", P, G)
    x = b.leave("B", e, b.uuid(), completed=True, observed=a1)
    eq(b.out(x), ("not_applied", "generation_closed"), "a closed generation")


def test_leave_recovery_orders():
    """The leave, a takeover and a copy's leave of the same transition, lost by a restore
    and uploaded in every order from two replicas (one holding the first two commits, the
    other all three, each upload of a commit by every replica holding it): the same
    records as the original server's."""
    prefix = 0x6a1e000a
    live = B("leave: recovery orders, the original server", prefix, LEAVE_FILE)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e = ref(P, G, "ep-1", "Episode 1")
    sa = fixed_op(prefix, 100)
    a1 = live.start("A", e, sa, 0, duration=D, op=fixed_op(prefix, 2))
    a2 = live.cp("A", e, sa, 1, D - 10000, D - 10000, op=fixed_op(prefix, 3))
    k = len(live.arrivals)
    live.dev("B")
    live.clone("B", "B2")
    T = fixed_op(prefix, 200)
    base = live.now
    x = live.leave("B", e, T, completed=True, observed=a1, position=D - 10000, after=[a2], t=base + 1000,
                   rt=base + 1000, op=fixed_op(prefix, 4))
    t = live.start("C", e, fixed_op(prefix, 101), D - 10000, observed=a1, after=[a2], t=base + 2000, rt=base + 2000,
                   op=fixed_op(prefix, 5))
    y = live.leave("B2", e, T, completed=True, observed=a1, position=D - 10000, after=[a2], t=base + 3000,
                   rt=base + 3000, op=fixed_op(prefix, 6))
    eq([live.out(z) for z in (x, t, y)], [("applied", None), ("applied", None), ("superseded", None)], "live outcomes")
    eq((PS(live, e), L(live, e, "position")), ("played", posv(G, D - 10000, t)),
       "the leave completed; the takeover claims the position (a start never un-plays)")
    want = live.engine.records()
    ups = uploads(live, k)
    eq(len(ups), 3, "three lost commits")
    holders = [("X", "Y"), ("X", "Y"), ("Y",)]  # X pulled the first two commits, Y all three
    import itertools
    for perm in itertools.permutations(range(3)):
        rs = restored(live, k, "leave: recovery orders, uploaded %s" % "".join("LTC"[i] for i in perm))
        rs.group = LEAVE_FILE
        for i in perm:
            for _ in holders[i]:
                upload(rs, ups[i])
        rend(rs)
        eq(rs.engine.records(), want, "%s: the original's records" % rs.name)


def test_leave_generator(ncases):
    """The random generator emits episode.leave (paused loads, near-end handoffs, copied
    pending leaves): leaves that complete, remove, are void (a copy's leave of an effective
    transition) or only mark history all occur, and every case passes the checker."""
    rng = random.Random(6106)
    seen = {"fired": 0, "removed": 0, "void": 0, "noop": 0, "shared": 0}
    for k in range(ncases):
        case, eng = gen_rule_cases.generate_case(rng, copies=k % 2 == 0)
        eq(check_rules.check_case(json.loads(json.dumps(case, ensure_ascii=False))), None, "leave case %d" % k)
        trans = {}
        for key, cts in eng.scopes.items():
            if key[0] != "episode":
                continue
            st = eng._fold_episode_scope(key[1], key[2]).status
            for ct in cts:
                if ct.kind == "leave":
                    seen[st.get(ct.node)] += 1
                    trans[ct.data["transition_id"]] = trans.get(ct.data["transition_id"], 0) + 1
        seen["shared"] += sum(1 for v in trans.values() if v > 1)
    check(all(v > 0 for v in seen.values()), "the generator's leaves cover every effect: %s" % seen)


def test_subscription_decisions():
    """PROTOCOL §6.11: podcast.alias and podcast.resolution are recoverable decision arrivals; a
    recovered registration waits for its resolution (recovered facts make no decision), a
    fact naming its podcast takes effect once it arrives, whatever the upload order."""
    prefix = B.count + 1
    live = B("the original server", prefix)
    s1 = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s1)
    e = ref(P, G, "ep-1", "One")
    live.fav("A", e, True, op=fixed_op(prefix, 2))
    s2 = live.subscribe("B", U1B, op=fixed_op(prefix, 3))
    eq(live.res(s2)["mode"], "joined", "the second registration joins")
    want = live.engine.records()
    ups = uploads(live, 0)
    flat = [x for cm in ups for x in cm]
    eq([x["decision"]["decision_type"] for x in flat if "decision" in x],
       [rules.ALIAS, rules.RESOLUTION, rules.RESOLUTION], "the live subscriptions' decisions are uploads")
    by = dict((gen_rule_cases.entry_id(x), x) for x in flat)
    al, r1 = [x for x in flat if "decision" in x][:2]
    r2 = [x for x in flat if "decision" in x][2]
    orders = {
        "commits": flat,
        "facts first": [by[s1], by[fixed_op(prefix, 2)], by[s2], r2, al, r1],
        "decisions late": [by[s1], by[fixed_op(prefix, 2)], by[s2], al, r2, r1],
    }
    for name, order in orders.items():
        rs = B("subscriptions recovered (%s)" % name, prefix)
        for i, x in enumerate(order):
            upload(rs, [x])
            if name == "facts first" and i == 2:
                eq(rs.engine.records(), [], "registrations wait for their resolutions")
                check(s1 not in rs.engine.resolutions, "no resolution yet")
        rend(rs)
        eq(rs.engine.records(), want, "%s: the original's records" % name)
        eq(rs.engine.resolutions, live.engine.resolutions, "%s: the recovered resolutions" % name)
        eq(rs.engine.created, [], "%s: nothing decided" % name)
    # A registration recovered without its resolution: the end of the request resolves it
    # as a live one would (a provisional decision); its original, uploaded later, agrees.
    rs = B("a registration resolved at the end of its request", prefix)
    upload(rs, [by[s1], by[fixed_op(prefix, 2)]])
    rend(rs)
    eq(rs.res(s1), live.res(s1), "resolved provisionally as live")
    eq(rs.rec("episode", e["episode_id"], "favorite")["on"], True, "the favorite now applies")
    upload(rs, [al, r1, by[s2], r2])
    rend(rs)
    eq(rs.engine.records(), want, "the originals agree; nothing changes")


def test_provisional_podcast_superseded():
    """PROTOCOL §6.11: a podcast created after the restore for the same URL meets the
    recovered original: the recovered podcast.alias wins by rank and the later podcast is
    merged into it (reason recovered); devices that used its IDs are routed."""
    prefix = B.count + 1
    live = B("the original timeline (A subscribes and favorites)", prefix)
    s1 = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P1, G1 = pg(s1)
    e1 = ref(P1, G1, "ep-1", "One")
    live.fav("A", e1, True, op=fixed_op(prefix, 2))
    rs = B("8b: B subscribes the same feed after a restore that lost A's facts", prefix)
    rs.epoch = 2
    s2 = rs.subscribe("B", U1B, op=fixed_op(prefix, 3), receipt={"epoch": 2, "seq": 1, "index": 0})
    P2, G2 = pg(s2)
    check(P2 != P1, "a new podcast")
    e2 = ref(P2, G2, "ep-1", "One")
    rs.mark_played("B", e2, op=fixed_op(prefix, 4), receipt={"epoch": 2, "seq": 2, "index": 0})
    upload(rs, [x for cm in uploads(live, 0) for x in cm])
    m = rs.made(rules.PODCAST_MERGE)
    eq([(x["inputs"]["cause"], x["result"]["source"], x["result"]["into"]) for x in m], [("recovered", P2, P1)],
       "one unification merge: the later podcast into the recovered original")
    eq(rs.rec("podcast", P2, "subscription"), {"state": "merged", "merged_into": P1}, "the provisional podcast is merged")
    eq(rs.rec("podcast", P1, "subscription")["state"], "subscribed", "one podcast survives")
    eq(rs.rec("episode", e1["episode_id"], "favorite")["on"], True, "A's favorite")
    eq(rs.rec("episode", e1["episode_id"], "play_state")["play_state"], "played", "B's mark, routed to the survivor")
    rend(rs)
    eq(len(rs.made(rules.PODCAST_MERGE)), 1, "nothing more to unify")


def test_provisional_receipt():
    """PROTOCOL §6.11: an original receipt replaces a provisional one: the fact is re-ranked at
    its original normalized time and re-applied with its original outcome; the provisional
    receipt is kept and recognised. The example: a favorite on at original time 100,
    re-accepted provisionally at 300, and a concurrent favorite off at 200: recovering the
    original makes the off win, visibly."""
    prefix = B.count + 1
    live = B("favorite on, original receipt at time 100", prefix)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e = ref(P, G, "ep-1", "One")
    on = live.fav("A", e, True, t=T0 + 900000, rt=T0 + 100000, op=fixed_op(prefix, 2))
    eq(live.nt(on), T0 + 100000, "the original's normalized time (a fast clock, clamped)")
    rs = restored(live, 1, "provisional re-push, then the original")
    off = rs.fav("B", e, False, t=T0 + 200000, rt=T0 + 200000, op=fixed_op(prefix, 3),
                 receipt={"epoch": 2, "seq": 2, "index": 0})
    a = json.loads(json.dumps([x for x in live.arrivals if x["fact"]["op_id"] == on][0]))
    a.update(receive_time=T0 + 300000, normalized_time=T0 + 300000, receipt={"epoch": 2, "seq": 3, "index": 0})
    rs.accept(a)
    eq(rs.rec("episode", e["episode_id"], "favorite")["on"], True, "provisionally the on (300) is later than the off")
    orig = [x for cm in uploads(live, 1) for x in cm if "fact" in x][0]
    upload(rs, [orig])
    eq(rs.engine.transport[-1], (on, "replaced"), "the original replaced the provisional receipt")
    eq(rs.nt(on), T0 + 100000, "re-ranked at its original time")
    eq(rs.rec("episode", e["episode_id"], "favorite")["on"], False, "the off (200) now wins, visibly")
    sup = json.loads(json.dumps(a))
    sup["recovery"] = True
    upload(rs, [sup])
    eq(rs.engine.transport[-1], (on, "known"), "a later upload of the provisional receipt is known")
    upload(rs, [orig])
    eq(rs.engine.transport[-1], (on, "known"), "the original again: known")
    eq(rs.out(on), ("applied", None), "the live answer is not rewritten")
    check(off in rs.engine.prio, "the concurrent fact is untouched")
    # An original rejected receipt over a provisional node with a descendant: the fact
    # stays a node and contributes nothing; its descendant's edges stay valid.
    prefix = B.count + 1
    live = B("a fact the original server rejected", prefix)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e, f = ref(P, G, "ep-1", "One"), ref(P, G, "ep-2", "Two")
    x = live.fav("N", e, True, caps=NOQ[1:], op=fixed_op(prefix, 2), reject=True)
    eq(live.out(x), ("rejected", "capability_not_declared"), "rejected originally")
    rs = restored(live, 1, "re-pushed and accepted, then its rejected original")
    a = json.loads(json.dumps([y for y in live.arrivals if y["fact"]["op_id"] == x][0]))
    a.update(capabilities=list(ALL), receipt={"epoch": 2, "seq": 2, "index": 0},
             normalized_time=c.normalize_time(a["fact"]["client_time"], a["receive_time"], []), edges=[])
    rs.accept(a)
    y = rs.fav("B", f, True, after=[x], op=fixed_op(prefix, 3), receipt={"epoch": 2, "seq": 3, "index": 0})
    eq(rs.rec("episode", e["episode_id"], "favorite")["on"], True, "provisionally applied")
    upload(rs, [z for cm in uploads(live, 1) for z in cm])
    check(x in rs.engine.prio and y in rs.engine.prio, "it stays a node; its descendant stays valid")
    eq(rs.rec("episode", e["episode_id"], "favorite"), None, "it contributes nothing")
    eq(rs.rec("episode", f["episode_id"], "favorite")["on"], True, "the descendant applies")


def test_recovery_end_reconciliation():
    """PROTOCOL §7.6 replaces PROTOCOL §6.11's sort reconciliation: the replay's order is a
    function of the recovered facts (no decision needed), and the end of a recovery request
    runs the publication rule: one layout when the published keys do not give that order,
    none when they do; recovered layouts are evidence only."""
    prefix = B.count + 1
    live = B("8b: sorted adds, then the restore loses them", prefix)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    al, be, ga = (ref(P, G, t.lower(), t) for t in ("Alpha", "Beta", "Gamma"))
    live.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z", op=fixed_op(prefix, 2))
    [kg] = keys_in_order(["rec/g"])
    live.qadd("A", [(ga, kg)], place=END, op=fixed_op(prefix, 3))
    live.qadd("B", [(al, K(kg[0], None, "rec/a-after"))], place=END, op=fixed_op(prefix, 4))
    eq(live.queue(), ["Alpha", "Gamma"], "the add sorts at its place")
    ups = uploads(live, 2)
    rs = restored(live, 2, "the adds recovered without their layouts")
    upload(rs, [cm[0] for cm in ups])  # a replica holding its own pushes only
    rs.engine._ensure()
    eq(rs.queue(), ["Alpha", "Gamma"], "as recovered: the replay sorts without any decision")
    n = len(rs.engine.created)
    rend(rs)
    eq([d["decision_type"] for d in rs.engine.created[n:]], [rules.LAYOUT], "the request's end publishes one layout")
    eq(rs.queue(), ["Alpha", "Gamma"], "sorted")
    upload(rs, [x for cm in ups for x in cm[1:]])
    n = len(rs.engine.created)
    rend(rs)
    eq(rs.engine.created[n:], [], "the recovered layouts are evidence; the published keys still give the order")
    eq(qstate(rs), qstate(live), "the same queue as the original")
    # An add accepted while keep-sorted was off: policy changes never re-sort.
    prefix = B.count + 1
    live = B("an add before keep-sorted", prefix)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    al, ga = ref(P, G, "alpha", "Alpha"), ref(P, G, "gamma", "Gamma")
    [kg] = keys_in_order(["off/g"])
    live.qadd("A", [(ga, kg)], place=END, op=fixed_op(prefix, 2))
    live.qadd("A", [(al, K(kg[0], None, "off/a"))], place=END, op=fixed_op(prefix, 3))
    live.qpolicy("A", keep_sorted=True, sort_order="episode_title_a_z", op=fixed_op(prefix, 4))
    eq(live.queue(), ["Gamma", "Alpha"], "not re-sorted by the policy change")
    rs = restored(live, 1, "adds made while keep-sorted was off, recovered")
    upload(rs, [x for cm in uploads(live, 1) for x in cm])
    n = len(rs.engine.created)
    rend(rs)
    eq(rs.engine.created[n:], [], "the recovered layouts already give the order")
    eq(rs.engine.records(), live.engine.records(), "the original's records")


def test_identity_unification():
    """PROTOCOL §6.11: rotating enclosures that get different e0: allocations before and after
    a restore end as one episode per release; release equivalence is one-to-one."""
    prefix = B.count + 1
    live = B("8b: rotating enclosures, the original allocations", prefix)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    u1, u2 = "https://cdn.example.org/odd.mp3", "https://cdn.example.org/even.mp3"
    for i, u in enumerate([u1, u2, u1, u2]):
        live.fetch(P, [(gocc("bi"), ofields("Edition %d" % i, T0 + i * 14 * DAY, u))])
    eA = live.made(rules.MAPPING)[3]["result"]["episode_id"]
    check(eA.startswith("e0:"), "the fourth release is allocated")
    rs = restored(live, 4, "the restored server fetches again and allocates another e0:")
    # The restored server knows it was restored: its commits are in epoch 2. A live fact
    # states that epoch first (a server commit carries no receipt), so the fetch's commit
    # and its decisions share one revision.
    rs.epoch = 2
    rs.qpolicy("A", enqueue_location="back")
    rs.fetch(P, [(gocc("bi"), ofields("Edition 3", T0 + 3 * 14 * DAY, u2))])
    eB = rs.made(rules.MAPPING)[-1]["result"]["episode_id"]
    check(eB.startswith("e0:") and eB != eA, "a different allocation")
    upload(rs, [x for cm in uploads(live, 4) for x in cm])
    m = [d for d in rs.made(rules.EPISODE_MERGE) if d["inputs"]["cause"] == "recovered"]
    eq([(d["result"]["from"], d["result"]["into"]) for d in m], [(eB, eA)], "the later allocation merges into the earlier")
    eq([r["publication_time"] for r in IR(rs, P)["disputed_guids"][0]["releases"]],
       [T0 + i * 14 * DAY for i in range(4)], "one episode per release")
    eq(rs.rec("episode", eB, "identity")["merged_into"], eA, "the provisional release is routed")
    rend(rs)
    eq(len(m), len([d for d in rs.made(rules.EPISODE_MERGE) if d["inputs"]["cause"] == "recovered"]), "stable")

    def mp(pt, encs, rank):
        return {"obj": {"result": {"publication_time": pt, "enclosures": encs}}, "rank": (1, rank, b"")}
    eqv = rules.Engine._equivalent_releases
    a, b_, c_ = mp(0, ["x"], 1), mp(6 * DAY, ["y"], 2), mp(12 * DAY, ["z"], 3)
    eq(eqv([a, b_, c_]), [], "days 0, 6 and 12: windows are not transitive, nothing merges")
    eq(eqv([a, b_]), [(a, b_)], "each the other's only release in the window")
    eq(eqv([a, mp(13 * DAY, ["x"], 4)]), [], "beyond the window and no shared occurrence")
    d = mp(0, ["x"], 5)
    eq(eqv([a, b_, d]), [(a, d)], "a shared occurrence is the same release whatever else matches")
    a2 = dict(a, rank=(1, 1, b"x"))
    eq(eqv([a, a2]), [], "releases of one commit are never the same")
    eq(eqv([a, a2, b_]), [], "a release's window neighbours are other commits'")


def test_epoch_evidence():
    """PROTOCOL §6.11: a recovered receipt at or above the account's epoch that the archive lacks
    proves a lost timeline: the account moves to max(high-water, epoch) + 1 before joining
    it; positions order by (epoch, seq, index, op_id)."""
    prefix = B.count + 1
    live = B("the lost timeline", prefix)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e = ref(P, G, "ep-1", "One")
    lost = live.fav("A", e, True, op=fixed_op(prefix, 2))
    rs = restored(live, 1, "an undetected copy continues in epoch 1")
    rs.devs = json.loads(json.dumps(live.devs))
    rs.devs["A"]["seq"], rs.devs["A"]["prev"] = 1, s  # the copied device does not know its lost fact
    other = rs.mark_played("A", e, op=fixed_op(prefix, 3))  # (1, 2, 0) again, A's author_seq 2 again
    eq(rs.engine.facts[other]["rkey"][:3], (1, 2, 0), "the same position as the lost fact")
    eq(rs.engine.epoch, 1, "nothing detected yet")
    upload(rs, [x for cm in uploads(live, 1) for x in cm])
    eq((rs.engine.epoch, rs.engine.high_water), (2, 2), "the account moves to epoch 2 before joining")
    first, second = sorted((lost, other), key=lambda op: op.encode())
    eq((rs.eff(first), rs.eff(second)), (rs.dev("A")["id"], rules.derived_author(NS, second)),
       "equal positions order by op_id: the smaller holds the author position")
    nxt = rs.fav("B", e, False)
    eq(rs.engine.facts[nxt]["rkey"][0], 2, "later commits are in the new epoch")
    expect_input_error(lambda: rs.engine.accept(dict(json.loads(json.dumps(rs.arrivals[-1])), receipt={
        "epoch": 1, "seq": 99, "index": 0}, fact=dict(rs.arrivals[-1]["fact"], op_id=det_uuid("8b/epoch1")))),
        "a live acceptance in an old epoch")


def test_conflicts():
    """PROTOCOL §6.11: joining checks content itself: a known op_id with other content, a known
    decision_id with another decision, an ID used as both a fact and a decision. A conflict
    changes nothing."""
    prefix = B.count + 1
    live = B("conflicts, the original", prefix)
    s = live.subscribe("A", U1, op=fixed_op(prefix, 1))
    P, G = pg(s)
    e = ref(P, G, "ep-1", "One")
    f = live.fav("A", e, True, op=fixed_op(prefix, 2))
    rs = restored(live, 2, "conflicts on the restored server")
    before = rs.engine.records()
    a = json.loads(json.dumps(live.arrivals[1]))
    a["recovery"] = True
    a["fact"]["payload"]["on"] = False
    upload(rs, [a])
    eq((rs.engine.transport[-1], rs.engine.records()), ((f, "conflict"), before), "other content: a conflict")
    d = [x for cm in uploads(live, 0) for x in cm if "decision" in x][0]["decision"]
    upload(rs, [{"decision": dict(d, decision_id=f)}])
    eq((rs.engine.transport[-1], rs.engine.records()), ((f, "conflict"), before), "a fact's ID as a decision")


def test_replay_session_fact_without_its_start():
    """Regression (frozen selftest case 95): with
    persisted receipts (a replay of one archive in another causal order), a session fact
    whose receipt admits it while its session has no admitted start in that order is an
    archive that does not hold its receipts: malformed input (RuleInputError), never a
    crash. Here B's start names A's podcast without depending on A's subscribe (a history no
    device writes), so an order may put it before the subscribe, where its reference fails
    and its session is never registered. The receipt order replays as the original."""
    b = B("replay: a session fact whose start is not admitted in the order (not a vector)")
    b.vector_ok = False
    s = b.subscribe("A", U1)
    P, G = pg(s)
    e = ref(P, G, "x", "X")
    S = b.uuid()
    b.start("B", e, S, 0)  # no dependency on the subscribe
    b.cp("B", e, S, 1, MIN, MIN)
    eng = b.engine
    arrivals = []
    for i, a in enumerate(b.arrivals):
        a = json.loads(json.dumps(a))
        a["receipt"] = {"epoch": 1, "seq": i + 1, "index": 0}
        arrivals.append(a)

    def replay(order):
        rep = rules.Engine(ACC, NS, eng.resolutions, eng.alias_decisions(), eng.episode_decisions(), replay=True,
                           fixed_outcomes=dict(eng.outcomes))
        for k in order:
            rep.accept(json.loads(json.dumps(arrivals[k])))
        return rep.records()
    eq(replay([0, 1, 2]), eng.records(), "the receipt order replays")
    try:
        replay([1, 0, 2])
        check(False, "an order without the session's start is refused")
    except rules.RuleInputError as e:
        check("whose start is not admitted" in str(e), "malformed input, not a crash: %s" % e)


def test_committed_recovery_cases():
    """PROTOCOL §6.11: the rule-selftest cases with recovery uploads still
    pass."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "vectors", "rules_selftest.json")
    with open(path, encoding="utf-8") as fh:
        cases = json.load(fh)["cases"]
    n = 0
    for cs in cases:
        if any("decision" in a or a.get("recovery") for a in cs["arrivals"]):
            n += 1
            eq(check_rules.check_case(cs), None, "committed recovery case %d" % n)
    check(n >= 10, "the committed recovery cases (%d)" % n)


def keys_as_order(records):
    """Records with the queue's keys replaced by the order they give (PROTOCOL §7.6: different request partitions may record different corrective layouts, so
    recovery tests compare orders, and keys only between phones on one server head)."""
    out, pos = [], {}
    for r in records:
        if r["field_group"] == "queue_position":
            pos[(r["id"], r["value"]["generation_id"])] = r["value"]["key"]
        else:
            out.append(r)
    shown = sorted(pos, key=functools.cmp_to_key(lambda a, b: c.compare_queue_keys(pos[a], pos[b])))
    return out, shown


def test_recovery_generator(ncases, nsched):
    """PROTOCOL §6.11 --recovery: a random case restored at a random commit; the lost commits uploaded
    from two or three replicas in random orders with duplicates and request boundaries.
    Complete recovery gives the records of the run without a restore; every order of the
    same (complete or partial) recovered set gives the same records."""
    rng = random.Random(8808)
    for i in range(ncases):
        kind = ("base", "catalog", "identity")[i % 3]
        case, eng = gen_rule_cases.generate_case(rng, catalog=kind != "base", identity=kind == "identity")
        n = len(case["arrivals"])
        commits = gen_rule_cases.archive_commits(eng, rng)
        k = rng.randint(max(1, n // 3), max(1, n - 1))
        lost = [commits[key] for key in sorted(commits) if key[1] > k]
        known = set(gen_rule_cases.entry_id(x) for key in commits if key[1] <= k for x in commits[key])
        if not lost:
            continue
        for partial in (False, True):
            reps = gen_rule_cases.recovery_replicas(rng, len(lost), partial)
            first = None
            for _ in range(nsched):
                sched = gen_rule_cases.recovery_schedule(rng, lost, reps, known, 0)
                rs, arrs = gen_rule_cases.run_recovery(case, k, sched, rng)
                got = keys_as_order(rs.records())
                if not partial:
                    eq(got, keys_as_order(eng.records()), "case %d (%s): complete recovery = no restore" % (i, kind))
                if first is None:
                    first = got
                    out = {"account_id": case["account_id"], "namespace": case["namespace"], "arrivals": arrs,
                           "expected": rs.result()}
                    eq(check_rules.check_case(json.loads(json.dumps(out))), None, "case %d: a valid vector" % i)
                eq(got, first, "case %d (%s, %s): order independence" % (i, kind, "partial" if partial else "complete"))


# ---------------------------------------------------------------------------
# URL credentials in GUIDs and in HTML text. Committed as
# contract/vectors/rules_userinfo.json, and the embedded-URL rule's own cases as
# contract/vectors/strip_userinfo.json.
# ---------------------------------------------------------------------------

USERINFO = "rules_userinfo.json"
STRIP_USERINFO = "strip_userinfo.json"

# (text, the text with every embedded URL's userinfo removed)
STRIP_USERINFO_CASES = [
    ("see https://user:pw@host/x and http://a@b", "see https://host/x and http://b"),
    ("<a href='https://us:pw@host/'>", "<a href='https://host/'>"),
    ("<a href=\"https://us'er:pw@host/\">", "<a href=\"https://host/\">"),
    ("https://us'er:pw@host", "https://host"),
    ("'https://example.com','a@b.com'", "'https://example.com','a@b.com'"),
    ("<a href=\"https://user:pw&#64;host/\">", "<a href=\"https://host/\">"),
    ("https://user:pw&#x40host/", "https://host/"),
    ("https://user:pw&#X40;host/", "https://host/"),
    ("https://user:pw&#00064;host/", "https://host/"),
    ("https://user:pw&commat;host", "https://host"),
    ("https://user:pw&Commat;host", "https://user:pw&Commat;host"),
    ("https://user:pw&#6412;host", "https://user:pw&#6412;host"),
    ("x //u:p@h y", "x //h y"),
    ("(//u:p@h)", "(//h)"),
    ("a//u:p@h", "a//u:p@h"),
    ("no urls here a@b", "no urls here a@b"),
    ("mailto:a@b.com", "mailto:a@b.com"),
    ("https://h/p@q", "https://h/p@q"),
    ("https://h?x=a@b", "https://h?x=a@b"),
    ("https://h#a@b", "https://h#a@b"),
    ("&#0;https://u@h", "&#0;https://h"),
    ("https:&#47;&#47;u:p@h", "https:&#47;&#47;h"),
    ("https&colon;//u:p@h", "https&colon;//h"),
    ("https://a@b@c/", "https://c/"),
    ("\u00e9 https://\u00fc:p@h", "\u00e9 https://h"),
    ("https://u:p@h\u00a0x@y", "https://y"),
    ("https://u:p\t@h", "https://u:p\t@h"),
    ("<https://u:p@h>", "<https://h>"),
    ("https://u:p@h<b>@c</b>", "https://h<b>@c</b>"),
    ("ftp://u:p@h https://v:q@i", "ftp://h https://i"),
    ("", ""),
    # More digits than a code point needs: text, so its # ends the authority.
    ("https://u&#" + "1" * 5000 + ";@h", "https://u&#" + "1" * 5000 + ";@h"),
    ("https://u:p&#0000000000064;h", "https://h"),
    # A control character before a scheme-relative URL is a boundary, so a GUID's trim
    # (which removes it) cannot uncover a URL that kept its userinfo.
    ("\u0001//u@h", "\u0001//h"),
    ("a\u001f//u:p@h/x", "a\u001f//h/x"),
    ("\u0000//u@h", "\u0000//h"),
    # Thousands of leading zeros are still the code point after them.
    ("https://u&#" + "0" * 5000 + "64;h", "https://h"),
    ("https://u&#x" + "0" * 5000 + "40;h", "https://h"),
]


CATALOG_ROUTING = "rules_catalog_routing.json"


def test_catalog_routing():
    """When the first dispute splits a GUID's earlier
    evidence into releases, each earlier fetch's item metadata moves to its own release
    at once (a fetch's observation follows its occurrence, as a device's does); before,
    the later release had no metadata until it was fetched again."""
    b = B("catalog metadata follows the releases a later dispute finds", 0x7c1f0002, CATALOG_ROUTING)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    u1, u2 = "https://cdn.example.com/latest.mp3", "https://cdn.example.com/latest.mp3?sig=old"
    d1, d2 = T0 - 40 * DAY, T0 - DAY
    for title, u, d in (("One", u1, d1), ("Two", u1, d2), ("One", u2, d1)):
        b.wait(60000)
        b.fetch(P, [(gocc("g"), ofields(title, d, u))])
    orig, later = eid(P, "rss-guid", "g"), rge(P, "g", u1)
    eq([r["episode_id"] for r in IR(b, P)["disputed_guids"][0]["releases"]], [orig, later], "two releases")
    eq((L(b, {"episode_id": orig}, "metadata")["title"], L(b, {"episode_id": later}, "metadata")["title"]),
       ("One", "Two"), "each release has its own fetch's metadata before any new fetch")
    eq(L(b, {"episode_id": later}, "metadata")["pub_date"], d2, "the later release's date")


def test_strip_userinfo_cases():
    """The embedded-URL rule (rules.strip_userinfo) on its own cases."""
    for text, out in STRIP_USERINFO_CASES:
        eq(rules.strip_userinfo(text), out, repr(text))
        eq(rules.strip_userinfo(out), out, "stripping is idempotent: %r" % out)


def strip_userinfo_vectors():
    return {"suite": "strip_userinfo", "cases": [{"input": t, "output": o} for t, o in STRIP_USERINFO_CASES]}


def test_security_guid_userinfo():
    """A GUID carrying URL credentials derives its episode without them
    (PROTOCOL §9.4, as a device must), and no record or decision carries them."""
    b = B("security: a GUID with URL credentials", 0x5ec00003, USERINFO)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    g = "https://alice:secret@example.org/episode"
    b.fetch(P, [(gocc(g), ofields("One", T0, "https://cdn.example.com/one.mp3"))])
    E = eid(P, "rss-guid", "https://example.org/episode")
    eq(L(b, {"episode_id": E}, "metadata")["title"], "One", "the episode derived without the credentials")
    eq("secret" in repr(b.engine.proj) or "secret" in repr(b.engine.created), False, "no record or decision carries them")
    # A phone that strips the GUID, as the client obligations require, names the same episode.
    f = b.fav("B", oref(P, G, "https://example.org/episode", "https://cdn.example.com/one.mp3", T0, "One"), True)
    eq((b.out(f), L(b, {"episode_id": E}, "favorite")), (("applied", None), {"generation_id": G, "on": True}),
       "the phone's reference")


def test_security_text_userinfo():
    """An `@` written as a character reference, and an apostrophe in the user name, do
    not keep URL credentials in a fetch's text."""
    b = B("security: URL credentials in HTML text", 0x5ec00004, USERINFO)
    s = b.subscribe("A", U1)
    P, G = pg(s)
    desc = ("<p><a href=\"https://user:pw&#64;host.example/x\">x</a> https://us'er:pw@host.example/y "
            "<a href='https://u:p@h.example/'>z</a> mail a@b.example</p>")
    want = ("<p><a href=\"https://host.example/x\">x</a> https://host.example/y "
            "<a href='https://h.example/'>z</a> mail a@b.example</p>")
    b.fetch(P, [(gocc("g"), ifields("One", T0, url="https://cdn.example.com/one.mp3", description=desc,
                                     link="https://us'er:pw@site.example/ep"))])
    m = L(b, {"episode_id": eid(P, "rss-guid", "g")}, "metadata")
    eq((m["description"], m["link"]), (want, "https://site.example/ep"), "the stored text")


def main(argv):
    write_dir = None
    groups_dir = None
    quick = False
    i = 0
    while i < len(argv):
        if argv[i] == "--write" and i + 1 < len(argv):
            write_dir = argv[i + 1]
            i += 2
        elif argv[i] == "--groups" and i + 1 < len(argv):
            groups_dir = argv[i + 1]
            i += 2
        elif argv[i] == "--quick":
            quick = True
            i += 1
        else:
            print(__doc__.strip(), file=sys.stderr)
            return 2
    tests = [
        ("transition table", test_transition_table),
        ("created defaults, policy, credentials", test_created_defaults_policy_credentials),
        ("observed names L itself", test_observed_names_live_generation),
        ("stale: absent observed, both orders", test_stale_absent_observed_both_orders),
        ("re-subscribe opens a fresh generation", test_resubscribe_opened_fresh_state),
        ("concurrent re-subscriptions share one generation", test_concurrent_resubscriptions_share_generation),
        ("delayed subscribe observed archived G1", test_delayed_subscribe_observed_archived_g1),
        ("register-preview never downgrades", test_register_preview_never_downgrades),
        ("subscribe from preview + concurrent action edit", test_preview_subscribe_keep_updated_and_concurrent_action),
        ("joined subscribe superseded", test_joined_subscribe_superseded_and_archive_order),
        ("partial set_policy", test_partial_set_policy_concurrent),
        ("refresh URL per generation", test_refresh_url_per_generation),
        ("scenario 45 alias conflicts", test_scenario45_alias_conflicts),
        ("scenario 10", test_scenario10_unsubscribe_vs_offline_listening),
        ("scenario 11", test_scenario11_dismiss_all_vs_concurrent_filing),
        ("scenario 12", test_scenario12_favorite_converges_to_later),
        ("scenario 13", test_scenario13_undo_mark_played),
        ("dismiss conditions", test_dismiss_conditions),
        ("explicit vs automatic barriers", test_barriers_explicit_vs_automatic),
        ("episode reasons and identity", test_episode_reasons_and_identity),
        ("rejections", test_rejections),
        ("duplicate deliveries", test_transport_duplicates),
        ("input errors", test_input_errors),
        ("listening: scenario 8a ownership handoff", test_8a_ownership_handoff),
        ("listening: scenario 8b takeover vs lazy finalization", test_takeover_vs_lazy_finalization),
        ("listening: scenario 31 stale completion vs replay", test_31_stale_completion_vs_replay),
        ("listening: reset position bars an old session", test_reset_position_bars_old_session),
        ("listening: adoption, finalization, adopted owner", test_adoption_finalize_then_adopted_owner),
        ("listening: reset and rewind cuts (8c, 8d, 36, 37)", test_reset_and_rewind_cuts),
        ("listening: concurrent cuts in all upload orders", test_concurrent_cuts_all_orders),
        ("listening: history cuts and unobserved activity", test_history_cuts_and_unobserved_activity),
        ("listening: finalization days later", test_finalization_days_later),
        ("listening: author sequence gaps", test_author_sequence_gaps),
        ("listening: cut after a rejected predecessor", test_cut_after_rejected_predecessor),
        ("listening: admission faults", test_listening_admission_faults),
        ("listening: payload rejections", test_listening_payload_rejections),
        ("listening: generation closure", test_listening_generation_closure),
        ("listening: scenarios 5, 6, 7", test_scenarios_5_6_7),
        ("listening: measured duration and barriers", test_measured_duration_and_barriers),
        ("listening: fan-out (vector size)", lambda: test_fanout(6, 3, True)),
        ("listening: fan-out (thousands of entries)", lambda: test_fanout(40 if quick else 150, 20, False)),
        ("listening: restore of an unknown removal", test_restore_unknown_removal_is_an_input_error),
        ("queue: scenario 8 remove vs offline move", test_scenario8_remove_vs_offline_move),
        ("queue: scenario 9 clear vs offline add", test_scenario9_clear_vs_offline_add),
        ("queue: scenario 25 queue-less play", test_scenario25_queueless_play),
        ("queue: scenario 35 concurrent sorted adds; metadata", test_scenario35_concurrent_sorted_adds),
        ("queue: automatic vs explicit membership (hook)", test_automatic_membership_hook),
        ("queue: Undo under keep-sorted", test_undo_under_keep_sorted),
        ("queue: moves of out entries", test_moves_of_out_entries),
        ("queue: completion and skip removals in the fold", test_completion_and_skip_removals),
        ("queue: keep-sorted with ties and missing values", test_sort_results_ties_and_missing),
        ("queue: offline add, disable, move (live and recovery)", test_offline_add_disable_move),
        ("queue: a delayed older move before a sorting add", test_delayed_older_move_vs_sort),
        ("queue: an add under a concurrent disable (the policy at the add's place decides)", test_add_under_a_concurrent_disable),
        ("queue: concurrent policy writes", test_concurrent_policy_writes),
        ("queue: queue-less play, enqueue locations", test_queueless_play_locations),
        ("queue: queue-less play then completion offline", test_queueless_play_then_completion_offline),
        ("queue: competing decisions in recovery", test_competing_decisions),
        ("queue: payload rejections and capabilities", test_payload_rejections_and_capabilities),
        ("queue: bulk members and closed generations", test_bulk_members_and_closed_generations),
        ("queue: malformed supplies and decision arrivals", test_queue_input_errors),
        ("queue: nested and concurrent keys", test_keys_property),
        ("queue: canonical comparators (queue_sort)", test_queue_sort),
        ("copies: scenario 33 in both detection paths and every order", test_scenario33),
        ("copies: ownership", test_ownership),
        ("copies: shared exports are the writing history (CHOICE)", test_shared_exports_are_the_writing_history),
        ("copies: scenario 22 restored backup", test_scenario22_restored_backup),
        ("copies: derived branches", test_derived_branches),
        ("copies: simulated restore", test_simulated_restore),
        ("copies: copied finalizations", test_copied_finalizations),
        ("copies: fallback symmetry of copied finalizations", test_fallback_asymmetry),
        ("copies: installation.fork admission", test_fork_admission),
        ("copies: regressions", test_regressions),
        ("copies: copied branches", test_copied_branches),
        ("copies: determinism with fixed receipts", test_determinism),
        ("catalog: proposal outcomes and payload rejections", test_payloads_and_outcomes),
        ("catalog: admission in every arrival order", test_admission_orders),
        ("catalog: an observation without its proposal, then a fetch", test_observation_without_proposal_then_fetch),
        ("catalog: Inbox filing against every barrier", test_inbox_filing_barriers),
        ("catalog: queue filing", test_queue_filing),
        ("catalog: the replacement rule and re-parse", test_replacement_rule),
        ("catalog: closed generations and preview-to-subscribe", test_closed_generations_and_preview),
        ("catalog: moves and a move racing change_url", test_moves),
        ("catalog: refresh_status publication", test_refresh_status),
        ("catalog: parts of at most 200 items", test_parts),
        ("catalog: seen items restore the server's history", test_seen_history),
        ("catalog: competing admissions in recovery", test_competing_admissions_in_recovery),
        ("identity: scenario 17 (bulletins; a hosting move)", test_scenario17),
        ("identity: scenario 18 (all GUIDs changed)", test_scenario18),
        ("identity: scenario 46 (identity vectors)", test_scenario46),
        ("identity: scenario 34 (two phones, two releases, both orders)", test_scenario34),
        ("identity: the resolver for late facts; evidence", test_late_facts_and_resolver),
        ("identity: rotating enclosures", test_rotating_enclosures),
        ("identity: merges during playback; compound effects", test_merges_during_playback_and_compound),
        ("identity: merged admissions (the earlier files)", test_merged_admissions),
        ("identity: podcast merge, joined", test_podcast_merges),
        ("identity: podcast merge, rehomed (scenario 34)", test_podcast_merge_rehomed),
        ("identity: lookup aliases only from decisions", test_lookup_aliases_only_from_decisions),
        ("identity: part layout with identity evidence", test_part_layout),
        ("releases: a newest-first fetch reusing a GUID", test_releases_newest_first),
        ("releases: signed enclosures added to their release", test_releases_signed_enclosure),
        ("releases: the first dispute partitions by release", test_releases_delayed_dispute),
        ("releases: a new GUID two items share", test_releases_shared_new_guid),
        ("releases: repeated items in one fetch", test_releases_repeated_items),
        ("releases: a merge carries an e0: episode", test_releases_merge_carries_e0),
        ("catalog metadata follows the releases", test_catalog_routing),
        ("security: the embedded-URL rule's cases", test_strip_userinfo_cases),
        ("security: a GUID with URL credentials", test_security_guid_userinfo),
        ("security: URL credentials in HTML text", test_security_text_userinfo),
        ("incremental order = Kahn", lambda: test_incremental_order_equals_kahn(300 if quick else 3000)),
        ("arrival permutations", lambda: test_permutations(15 if quick else 80, 4 if quick else 8)),
        ("copies: save tree vs the generator's ground truth", lambda: test_save_tree_ground_truth(100 if quick else 300)),
        ("generator", lambda: test_generator(10 if quick else 40)),
        ("catalog: arrival permutations of catalog cases", lambda: test_catalog_permutations(20 if quick else 80,
                                                                                   4 if quick else 8)),
        ("catalog: the catalog generator", lambda: test_catalog_generator(20 if quick else 60)),
        ("identity: arrival permutations of identity cases", lambda: test_identity_permutations(20 if quick else 80,
                                                                                    4 if quick else 8)),
        ("identity: the identity generator", lambda: test_identity_generator(80 if quick else 200)),
        ("recovery: subscription decisions recovered in any order", test_subscription_decisions),
        ("recovery: a provisional podcast superseded by the recovered original", test_provisional_podcast_superseded),
        ("recovery: provisional receipt replacement", test_provisional_receipt),
        ("recovery: recovery_end and the sort reconciliation", test_recovery_end_reconciliation),
        ("recovery: identity unification", test_identity_unification),
        ("recovery: epoch evidence and the op_id tie-break", test_epoch_evidence),
        ("recovery: conflicts change nothing", test_conflicts),
        ("recovery: the eleven committed recovery cases", test_committed_recovery_cases),
        ("replay: a session fact without its admitted start (regression)", test_replay_session_fact_without_its_start),
        ("recovery: the recovery generator", lambda: test_recovery_generator(15 if quick else 60, 2 if quick else 4)),
        ("leave: completing under the observed claim", test_leave_completes_under_observed_claim),
        ("leave: a takeover ordered before it", test_leave_takeover_before_it),
        ("leave: history mark and cuts", test_leave_history_cuts),
        ("leave: admissions and NEW", test_leave_and_admissions),
        ("leave: a later owning checkpoint", test_leave_then_owning_checkpoint),
        ("leave: copied pending leaves", test_leave_copied_pending),
        ("leave: the first effective leave of a transition", test_leave_first_effective),
        ("leave: claimless completion", test_leave_claimless),
        ("leave: a skip's removal once per transition", test_leave_skip_removal),
        ("leave: payload and episode checks", test_leave_checks),
        ("leave: recovery orders", test_leave_recovery_orders),
        ("leave: the random generator", lambda: test_leave_generator(80)),
        # Last, so that the builders of the cases before keep their prefixes.
        ("identity: a merged-away podcast's reference naming the survivor's episode", test_merge_family_reference),
    ]
    # The 2026-10-07 PROTOCOL §8.1 cases live in their own modules, which build on this one
    # (run as a script, this module is __main__: they must share its builders).
    sys.modules.setdefault("selftest_rules", sys.modules[__name__])
    import selftest_claims  # noqa: E402
    import selftest_queue_replay  # noqa: E402
    tests += selftest_claims.TESTS + selftest_queue_replay.TESTS
    failed = 0
    for name, fn in tests:
        try:
            fn()
        except Exception as e:  # noqa: BLE001 - report every failure
            failed += 1
            print("FAIL %s: %s" % (name, e))
            if not isinstance(e, Fail):
                traceback.print_exc()
        else:
            print("ok   %s" % name)
    # Every hand-written case also passes the vector checker as a round trip.
    vectors = [bb.case() for bb in BUILDERS if bb.vector_ok and bb.group is None]
    for bb in BUILDERS:
        if bb.vector_ok:
            res = check_rules.check_case(json.loads(json.dumps(bb.case(), ensure_ascii=False)))
            if res is not None:
                failed += 1
                print("FAIL vector round trip %s: %s" % (bb.name, res))
    groups = {}
    for bb in BUILDERS:
        if bb.vector_ok and bb.group is not None:
            groups.setdefault(bb.group, []).append(bb.case())
    if groups_dir is not None:
        os.makedirs(groups_dir, exist_ok=True)
        path = os.path.join(groups_dir, STRIP_USERINFO)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(strip_userinfo_vectors(), f, ensure_ascii=False, sort_keys=True, indent=1)
            f.write("\n")
        print("wrote %d cases to %s" % (len(STRIP_USERINFO_CASES), path))
        for name in sorted(groups):
            path = os.path.join(groups_dir, name)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"suite": "rules", "cases": groups[name]}, f, ensure_ascii=False, sort_keys=True, indent=1)
                f.write("\n")
            print("wrote %d cases to %s" % (len(groups[name]), path))
    else:
        # The committed group files are what the hand-written cases give now.
        vdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "vectors")
        spath = os.path.join(vdir, STRIP_USERINFO)
        if os.path.exists(spath):
            with open(spath, encoding="utf-8") as fh:
                if json.load(fh) != json.loads(json.dumps(strip_userinfo_vectors(), ensure_ascii=False)):
                    failed += 1
                    print("FAIL contract/vectors/%s differs from its cases (regenerate with --groups)" % STRIP_USERINFO)
        for name in sorted(groups):
            path = os.path.join(vdir, name)
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as fh:
                committed = json.load(fh)["cases"]
            if not check_rules.same(committed, json.loads(json.dumps(groups[name], ensure_ascii=False))):
                failed += 1
                print("FAIL contract/vectors/%s differs from its hand-written cases (regenerate with --groups)" % name)
    if write_dir is not None:
        os.makedirs(write_dir, exist_ok=True)
        path = os.path.join(write_dir, "rules_selftest.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"suite": "rules", "cases": vectors}, f, ensure_ascii=False, sort_keys=True, indent=1)
            f.write("\n")
        print("wrote %d cases to %s" % (len(vectors), path))
        qpath = os.path.join(write_dir, "queue_sort.json")
        qcases = queue_sort_cases()
        with open(qpath, "w", encoding="utf-8") as f:
            json.dump({"suite": "queue_sort", "cases": qcases}, f, ensure_ascii=False, sort_keys=True, indent=1)
            f.write("\n")
        print("wrote %d cases to %s" % (len(qcases), qpath))
    print("%s: %d tests, %d hand-written cases (%d as vectors), %d failures"
          % ("OK" if failed == 0 else "FAILED", len(tests), len(BUILDERS), len(vectors), failed))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
