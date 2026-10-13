#!/usr/bin/env python3
"""Generate random but valid `rules` cases with expected results from the Python reference.

Usage: python3 contract/reference/gen_rule_cases.py OUT_DIR [--cases N] [--seed S]
                                                    [--per-file M] [--no-bogus]
                                                    [--no-listening] [--no-queue]
                                                    [--no-forks] [--no-leave] [--copies]
                                                    [--catalog] [--identity] [--recovery]
                                                    [--file NAME]

Each case simulates one account with several podcasts (feed URLs in several spellings
that normalize alike, alternate and contested URLs for change_url) and several devices
(installations with their own clocks, capabilities, offline periods and local views).
Devices subscribe, preview, archive, restore, unsubscribe and re-subscribe with the
`observed` field their last synced view (plus their own pending actions) gives, edit
policies, credentials and feed URLs, and act on episodes through EpisodeRefs whose
episode_id is the contract's derivation (PROTOCOL §3.3), including favorites and dismissals.
Facts reach the server when a device is online or syncs; the server computes edges and
normalized times as admission does (PROTOCOL §6.4) and the reference engine computes the
expected resolutions, outcomes and records. With bogus refs enabled (the default) a few
EpisodeRefs name unknown podcasts or generations, or reuse another podcast's episode ID,
to exercise those not_applied reasons; every payload still follows PROTOCOL §7 exactly.

PROTOCOL §8 (unless --no-listening): each device has a player. It starts sessions
(`session.start` at the newest position it knows, naming the claim it saw), plays with
five-second-save contributions computed by Android's rule (forward displacement from the
start, frozen below it, PROTOCOL §8.8), seeks, pauses, resumes, skips, switches, is suspended or
restarted (abandoning its open session), reaches the natural end, and lazily finalizes a
paused previous episode when it loads another one, deciding completion from its own
measured duration and smart threshold. After a sync a paused player adopts another
claim's position: it seals its session (`superseded`, no activity) and later resumes in a
new session or finalizes under the adopted claim. Devices with those capabilities remove
episodes from History (and Undo it, occasionally twice), clear History and reset
statistics, exporting their latest save first and depending on their whole known frontier.
Concurrent offline devices, takeovers, late arrivals and days-long gaps follow from the
same simulation. --no-listening reproduces the subscription-only generator exactly (same random
stream for the same seed); it implies --no-queue.

PROTOCOL §7.6 (unless --no-queue): devices with the queue capability keep a local queue
view (records after a sync plus their own changes) and the account queue policy. They add
episodes at the enqueue location they resolve (back; front after "downloading" items;
after their current episode; a random index) into the `place` every placement carries
(PROTOCOL §7.6), insert there and then stable-sort their local queue under
keep-sorted with the canonical comparators (as Android does),
start unqueued episodes with a play-start bundle, remove episodes (and Undo it with
`queue.restore` at the saved index resolved at Undo time), move single episodes and move
several to the top or bottom as a run (`queue.reorder` with `run` and `place`), use the
sort dialog (`account.set_queue_policy` sorting and a `queue.reorder` with `sort` and the
episodes they observed, a permutation for random), change the enqueue location and clear
the queue with the members they saw. A run move sometimes carries the device's sparse key
predictions (`members`). Devices without the queue capability start
episodes without a bundle (the server resolves the add). Completion and skip removals come
from the players. The server's choices for its decisions (decision IDs, position IDs, the
random enqueue draw) come from a separate stream derived from the account ID, so
--no-queue reproduces the listening-only generator's facts exactly for the same seed
(devices without the queue never put decisions into their dependency frontier).

PROTOCOL §6.10 (unless --no-forks): players keep a journal of five-second saves with save
identities (a random ID per library, the session's save counter, the current baseline);
exports carry `save` ({id, index, first?, baseline, passive}) and starts their `baseline`.
Some saves stay unexported between exports. A process restart exports the latest
unexported save passively (the restart export), reloads the player under a new baseline
and either resumes the same session (whose first export under the new baseline claims)
or starts a new one. A play command on an open session saves at READY with `claim`
(PROTOCOL §8.1, `observed_claim` the claim the device saw). Devices take backups and libraries are copied, from the current
state or an older backup, sometimes losing the original: by local detection (a new
author, `installation.fork` naming the writing tip, a passive tail export of the open
session's unexported saves, the session sealed locally, new sessions afterwards) or not
detected (the same author and writing tip: a restart and derived branches on the
server). Copies of copies and sibling copies of one backup occur, copied outboxes are
re-sent by every holder (transport duplicates), copied pending finalizations are
finalized by the copy, and some facts are deliberately invalid (rejected) in a device's
chain. Concurrent copies, resets observing tails and every arrival order follow from the
simulation. --no-forks reproduces the generator without copies exactly; --copies makes
every case copy at least one library.

PROTOCOL §9.4 (--catalog): each show has a publisher feed over time (a backlog, new
releases dated when they appear, backfilled old items, items that start catalog-only and
gain their enclosure, corrected titles). Devices refresh a podcast they know: they send the
records that are new or differ from the canonical ones they hold (base = that revision),
then one proposal per eligible playable discovery new to their replica (or catalog-only
there and now playable, with their evidence), with their resolved action and, for the
queue, their complete result; never in their first population of a generation or for a
podcast not subscribed in their view, except a sloppy device now and then (so the server's
checks are exercised). The server fetches any registered podcast: at once, or started at
one step and applied at a later one (racing what devices push meanwhile), sometimes
failing, following permanent or temporary redirects or a new-feed-url, or re-parsing its
stored response. Each catalog show uses one GUID kind (rss-guid or atom-id) with plain
derivations. Without --catalog the generator's output is unchanged.

PROTOCOL §9.7 (--identity, implies --catalog): some shows publish weekly releases that
reuse one GUID with rotating enclosures and dates 3 to 20 days apart, news bulletins with
new GUIDs on one /latest.mp3, move host (every GUID and enclosure changes, titles and
dates kept) or are permanently redirected onto another show's URL; devices derive episode
IDs through the identity_rules they synced (aliases, the release resolver, rule 3 for an
unknown release), and the server stops fetching a merged-away podcast.

Paused loads and `episode.leave`, PROTOCOL §8 (unless --no-leave; it needs the copy-aware
players, so --no-forks implies it): a device sometimes loads an episode paused instead of playing it (a paused load,
continuous playback off), often one another device listened to, displaying its position
and claim (adopted again after a sync). Switching away or skipping then records
`episode.leave` with the transition ID it gave the load, the claim it displayed, its own
completion decision (its measured duration and smart threshold) and, for a skip outside
the threshold with Keep skipped episodes off, a queue removal; playing it instead starts a
session at that position. Libraries are sometimes copied while they hold such a pending
leave, so two copies leave one transition. --no-leave reproduces the generator without paused loads exactly (every flag
combination).

PROTOCOL §6.11 (--recovery): each case is a random case of any mode restored at a random
commit. The vector holds the original's arrivals up to that commit, then the lost commits
uploaded as recovery arrivals (facts with their original receipts, decisions as recorded,
a live subscription's podcast.alias and podcast.resolution with drawn IDs) from two or
three replicas' archives (each a prefix of the lost commits, whole commits; sometimes none
holds them all), in random orders their dependencies allow, with duplicates and
`recovery_end` request boundaries; sometimes lost facts are first pushed again live
(provisional receipts, in epoch 2 or, an undetected copy, in epoch 1), or one device first
uploads its own receipted pushes without the decisions their commits made (provisional
resolutions, sort reconciliations and unification merges follow).

Files are written as OUT_DIR/rules_random_NNNN.json ({"suite": "rules", "cases": [...]}),
or as one file OUT_DIR/NAME with --file NAME.
"""

import argparse
import copy
import functools
import hashlib
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ntp_contract as c  # noqa: E402
import queue_sort as qs  # noqa: E402
import rules  # noqa: E402

BASE_TIME = 1760000000000
ALL_CAPS = ["favorites", "history", "inbox", "queue", "statistics"]
TITLES = ["Episode %d", "Épisode %d", "第%d集", "Folge %d: Über alles", "Ep. %d — \"quoted\""]


def rand_uuid(rng):
    b = bytearray(rng.getrandbits(128).to_bytes(16, "big"))
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return "%s-%s-%s-%s-%s" % (h[:8], h[8:12], h[12:16], h[16:20], h[20:])


def hash_uuid(label):
    """A deterministic version-4 UUID (save and baseline identities: no draw)."""
    b = bytearray(hashlib.sha256(label.encode("utf-8")).digest()[:16])
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return "%s-%s-%s-%s-%s" % (h[:8], h[8:12], h[12:16], h[16:20], h[20:])


def make_shows(rng, n, catalog=False, identity=False):
    shows = []
    for i in range(n):
        host = "feeds%d.example.com" % i
        path = "/show%d/feed.xml" % i
        variants = [
            "https://%s%s" % (host, path),
            "HTTPS://Feeds%d.Example.COM%s" % (i, path),
            "https://%s:443%s#latest" % (host, path),
        ]
        alternates = [
            "https://mirror%d.example.net/show%d.rss" % (i, i),
            "http://%s%s" % (host, path),  # http and https are different URLs
            "https://%s/show%d/feed.xml?format=mp3" % (host, i),
        ]
        items = []
        for j in range(rng.randint(3, 6) + (rng.randint(2, 4) if catalog else 0)):
            kind = rng.choice(["rss-guid"] * 6 + ["enclosure"] * 2 + ["atom-id", "rss-guid+enclosure"])
            enclosure = "https://cdn%d.example.org/audio/s%d-e%d.mp3?t=%d" % (i, i, j, rng.randint(0, 99))
            if rng.random() < 0.2:
                enclosure = enclosure.replace("https://cdn", "HTTPS://CDN")
            occ = {}
            if kind == "rss-guid":
                guid = rng.choice(["guid-%d-%d" % (i, j), "tag:example.com,2026:show %d ep %d" % (i, j), "%d" % (1000 * i + j)])
                occ = {"k": kind, "v": guid, "guid": guid}
                if rng.random() < 0.9:
                    occ["enclosure_url"] = enclosure
            elif kind == "atom-id":
                guid = "urn:uuid:" + rand_uuid(rng)
                occ = {"k": kind, "v": guid, "guid": guid, "enclosure_url": enclosure}
            elif kind == "enclosure":
                occ = {"k": kind, "v": c.normalize_url(enclosure), "enclosure_url": enclosure}
            else:
                guid = "reused-%d" % j
                occ = {"k": kind, "v": c.episode_v_guid_enclosure(guid, c.normalize_url(enclosure)), "guid": guid,
                       "enclosure_url": enclosure}
            if rng.random() < 0.9:
                occ["title"] = rng.choice(TITLES) % j
            if rng.random() < 0.9:
                occ["publication_time"] = BASE_TIME - rng.randint(0, 10**10)
            items.append(occ)
        show = {"variants": variants, "alternates": alternates, "items": items}
        if catalog:
            # One GUID kind per show, as in real feeds (an Atom show's IDs are atom-id,
            # an RSS show's rss-guid), and plain derivations only: rss-guid+enclosure
            # belongs to a disputed GUID's later release.
            show["guid_kind"] = "atom-id" if rng.random() < 0.25 else "rss-guid"
            for occ in items:
                if "guid" in occ:
                    occ["k"], occ["v"] = show["guid_kind"], occ["guid"]
        if catalog:
            # PROTOCOL §9.4: the publisher's feed over time (draws only in catalog mode).
            # Each item is in the backlog, a new release (dated when it appears) or a
            # backfilled old item appearing later; some playable items start catalog-only
            # and gain their enclosure; some titles are corrected later.
            show["feed"] = []
            for occ in items:
                r = rng.random()
                appear = BASE_TIME - 1
                if r >= 0.4:
                    # Soon, or after the hours and days that pass in a case.
                    appear = BASE_TIME + (rng.randint(5000, 200000) if rng.random() < 0.4
                                          else rng.randint(3600000, 3 * 86400000))
                    if r < 0.85 and "publication_time" in occ:
                        occ["publication_time"] = appear  # a new release
                media_at = None
                if "enclosure_url" in occ and occ["k"] != "enclosure" and rng.random() < 0.25:
                    media_at = appear + rng.randint(1000, 90000)
                fix_at = appear + rng.randint(1000, 150000) if rng.random() < 0.15 else None
                show["feed"].append({"occ": occ, "appear": appear, "media_at": media_at, "fix_at": fix_at})
            show["title"] = "Show %d" % i
            show["title_fix_at"] = BASE_TIME + rng.randint(1000, 200000) if rng.random() < 0.3 else None
            if identity:
                add_identity_behaviour(rng, show, i, n)
        shows.append(show)
    return shows


def add_identity_behaviour(rng, show, i, n):
    """PROTOCOL §9.7 (draws only in identity mode): a show may publish weekly releases that
    reuse one GUID with rotating enclosures and dates 3 to 20 days apart (a dispute when more
    than 7 days apart with another enclosure), news bulletins with new GUIDs on one
    /latest.mp3 and one title hours apart (a disputed enclosure, never a merge), move host
    (every GUID and enclosure change, titles and dates kept: GUID-change merges), or be
    permanently redirected onto another show's URL (a podcast merge when that URL is a
    podcast's alias)."""
    kind = rng.choice(["plain", "weekly", "weekly", "news", "move", "move", "move", "redirect", "redirect"])
    show["kind"] = kind
    day = 86400000

    def series(occs):
        t = BASE_TIME - 1
        entries = []
        for occ in occs:
            entries.append({"occ": occ, "appear": t, "media_at": None, "fix_at": None})
            t = (BASE_TIME + rng.randint(5000, 150000)) if t < BASE_TIME else t + rng.randint(5000, 150000)
        for a, b in zip(entries, entries[1:]):
            a["until"] = b["appear"]
        show["feed"] += entries
    if kind == "weekly":
        g = "weekly-%d" % i
        encs = ["https://cdn%d.example.org/weekly-a.mp3" % i, "https://cdn%d.example.org/weekly-b.mp3" % i]
        pub, occs = BASE_TIME - 60 * day, []
        for j in range(rng.randint(2, 4)):
            occs.append({"k": show["guid_kind"], "v": g, "guid": g, "enclosure_url": encs[0] if j == 0 else rng.choice(encs),
                         "title": "Weekly %d-%d" % (i, j), "publication_time": pub})
            pub += rng.choice([3, 10, 14, 20]) * day
        series(occs)
    elif kind == "news":
        latest = "https://cdn%d.example.org/news/latest.mp3" % i
        series([{"k": show["guid_kind"], "v": "news-%d-%d" % (i, j), "guid": "news-%d-%d" % (i, j), "enclosure_url": latest,
                 "title": "News", "publication_time": BASE_TIME + j * 4 * 3600000} for j in range(rng.randint(2, 4))])
    elif kind == "move":
        show["move_at"] = BASE_TIME + rng.randint(20000, 90000)
    elif kind == "redirect":
        show["redirect_at"] = BASE_TIME + rng.randint(5000, 150000)
        show["redirect_to"] = rng.randrange(n)


def occ_at(show, entry, t):
    """An item's occurrence at time t: after a hosting move its GUID and enclosure change."""
    occ = entry["occ"]
    if show.get("move_at") is None or t < show["move_at"]:
        return occ
    occ = dict(occ)
    if "enclosure_url" in occ:
        occ["enclosure_url"] = occ["enclosure_url"].replace("cdn", "host2-cdn").replace("CDN", "host2-cdn")
    if "guid" in occ:
        occ["guid"] = "moved:" + occ["guid"]
    if occ["k"] in ("rss-guid", "atom-id"):
        occ["v"] = occ["guid"]
    elif occ["k"] == "rss-guid+enclosure":
        occ["v"] = c.episode_v_guid_enclosure(occ["guid"], c.normalize_url(occ["enclosure_url"]))
    else:
        occ["v"] = c.normalize_url(occ["enclosure_url"])
    return occ


def feed_fields_at(show, t):
    """The publisher's feed record at time t (PROTOCOL §9.4 podcast `metadata` fields)."""
    f = dict((k, None) for k in rules.FEED_FIELDS)
    title = show["title"]
    if show["title_fix_at"] is not None and t >= show["title_fix_at"]:
        title += " (renamed)"
    f.update(title=title, funding=[], paged=False, link="https://example.com/show")
    return f


def item_fields_at(entry, t, show=None):
    """An item's record at time t (PROTOCOL §9.4 episode `metadata` fields)."""
    occ = occ_at(show, entry, t) if show is not None else entry["occ"]
    f = dict((k, None) for k in rules.ITEM_FIELDS)
    title = occ.get("title")
    if entry["fix_at"] is not None and t >= entry["fix_at"]:
        title = (title or "Untitled") + " (corrected)"
    f["title"] = title
    f["pub_date"] = occ.get("publication_time")
    if "enclosure_url" in occ and (entry["media_at"] is None or t >= entry["media_at"]):
        f["media"] = {"url": occ["enclosure_url"], "size": "1000", "mime_type": "audio/mpeg",
                      "duration": item_duration(occ)}
    return f


def feed_at(show, t):
    """The items the feed lists at time t: [(occurrence, fields)], in document order."""
    return [(occ_at(show, e, t), item_fields_at(e, t, show)) for e in show["feed"]
            if e["appear"] <= t and ("until" not in e or t < e["until"])]


def item_duration(occ):
    """The media duration of an item, 8 to 108 minutes, derived from its evidence (no draw
    from the case's random stream)."""
    h = hashlib.sha256(json.dumps(occ, sort_keys=True, ensure_ascii=False).encode("utf-8")).digest()
    return 8 * 60000 + int.from_bytes(h[:4], "big") % (100 * 60000)


def episode_targets(ref):
    return [{"entity": "episode", "id": ref["episode_id"], "generation": ref["generation_id"]}]


class Device(object):
    def __init__(self, rng, listening=False):
        self.id = rand_uuid(rng)
        self.caps = list(ALL_CAPS) if rng.random() < 0.7 else sorted(rng.sample(ALL_CAPS, rng.randint(0, 4)))
        self.offset = rng.choice([0, 0, rng.randint(-30000, 30000), rng.randint(-600000, 5000)])
        self.seq = 0
        self.prev = None
        self.frontier = set()
        self.pending = []
        self.online = rng.random() < 0.6
        self.alias = {}  # normalized URL -> P
        self.subs = {}  # P -> {"state", "generation_id"}
        self.episodes = {}  # E -> {"play_state", "position", "on", "claim", "generation_id", "history", ...}
        self.pending_pods = set()  # P re-subscribed after removal, not synced yet
        self.pending_new = set()  # normalized URLs subscribed while unknown
        # PROTOCOL §8. The player: the current episode's session, or None. Draws happen
        # only with listening on, so PROTOCOL §7.2 random stream is unchanged without it.
        self.player = None
        self.local = {}  # E -> {"position", "claim", "generation_id"}: own saves since the last sync
        self.removals = []  # own history removals that could be undone: (op, ref, shown time)
        self.restored = []  # removals already undone (an occasional duplicate Undo)
        self.threshold = 30000
        self.keep_skipped = True
        if listening:
            self.threshold = rng.choice([30000, 30000, 30000, 0, 15000, 60000])  # smart mark as played
            self.keep_skipped = rng.random() < 0.7
        # PROTOCOL §7.6: the local queue view (no draws here).
        self.qin = set()  # episodes the device sees in its queue
        self.qpos = {}  # E -> (key, position_id)
        self.qref = {}  # E -> EpisodeRef the device uses for it
        self.qpolicy = dict(rules.DEFAULT_QUEUE_POLICY)
        self.qremoved = []  # own removals that can be undone: (ref, its index when removed)
        # PROTOCOL §6.10: the library instance (salts save and baseline IDs; a copy gets a
        # new one). Derived from the device ID: no draw.
        self.lib = self.id
        # PROTOCOL §9.4 (no draws): the device's replica per podcast (items it knows, with
        # whether it saw media and their effective dates, and the generations whose first
        # population it did), the canonical records it holds, records it sent and not yet
        # saw back, its catalog-only evidence per episode, and the policies it knows.
        self.cat = {}
        self.meta = {}
        self.sent = {}
        self.co_ev = {}
        self.pol = {}
        self.disc = None
        self.rules = {}  # P -> the identity_rules record it synced


def _qcmp(a, b):
    return c.compare_queue_keys(a[1], b[1])


class Sim(object):
    def __init__(self, rng, bogus=True, mismatch=True, listening=True, queue=True, forks=True, copies=False,
                 catalog=False, identity=False, leave=True):
        self.rng = rng
        self.bogus = bogus
        self.mismatch = mismatch
        self.listening = listening
        self.queue = queue and listening
        self.forks = forks and self.queue
        self.force_copies = copies and self.forks
        # PROTOCOL §8: paused loads and episode.leave (draws only when on).
        self.leaves = leave and self.forks
        # PROTOCOL §9.4: catalog observations, discovery proposals and server fetches.
        self.catalog = catalog or identity
        # PROTOCOL §9.7: identity reconciliation (implies the catalog).
        self.identity = identity
        self.pending_fetch = {}  # refresh ID -> podcast (fetches started, not applied yet)
        self.parse_time = {}  # refresh ID -> the publisher time its successful fetch parsed
        self.backups = []  # (source device, snapshot)
        self.expect_reject = set()  # op IDs emitted invalid on purpose (rejected facts in a chain)
        self.ncopies = 0
        self.focus, self.focus_steps = [], 0  # after a copy: the original and its copies act more
        # Ground truth for tests: every journal save ever recorded, by save ID: (the save
        # before it in its library's journal, or None, and its contribution).
        self.save_truth = {}
        self.account_id = rand_uuid(rng)
        self.namespace = rand_uuid(rng)
        self.engine = rules.Engine(self.account_id, self.namespace)
        # The server's random choices: a stream of their own (no draw from `rng`).
        seed = int.from_bytes(hashlib.sha256(("server/" + self.account_id).encode("utf-8")).digest()[:8], "big")
        self.server_rng = random.Random(seed)
        self.ncreated = 0
        self.arrivals = []
        self.now = BASE_TIME
        self.server_now = BASE_TIME
        self.shows = make_shows(rng, rng.randint(2, 4), self.catalog, identity)
        self.contested = ["https://shared.example.org/feed%d.xml" % k for k in range(2)]
        self.url_show = {}
        for i, s in enumerate(self.shows):
            for u in s["variants"] + s["alternates"]:
                self.url_show.setdefault(c.normalize_url(u), i)
        self.pod_show = {}
        self.has_child = set()
        self.devices = [Device(rng, listening) for _ in range(rng.randint(2, 4))]

    # -- transport -------------------------------------------------------------

    def emit(self, dev, typ, payload, targets, reject=False):
        op = rand_uuid(self.rng)
        if reject:
            self.expect_reject.add(op)
        deps = set(dev.frontier)
        if self.forks and dev.prev is not None:
            # A fact lists its author's previous fact among its dependencies (after
            # a sync the frontier may omit it, for example a rejected writing tip).
            deps.add(dev.prev)
        fact = {
            "op_id": op,
            "author_id": dev.id,
            "author_seq": dev.seq + 1,
            "dependencies": sorted(deps),
            "type": typ,
            "type_version": 1,
            "client_time": self.now + dev.offset,
            "targets": sorted(targets, key=lambda t: (t["entity"], t["id"], t.get("generation", ""))),
            "payload": payload,
        }
        if dev.prev is not None:
            fact["previous_op_id"] = dev.prev
        dev.seq += 1
        dev.prev = op
        dev.frontier = {op}
        dev.pending.append(fact)
        if dev.online:
            self.push(dev)
        return op

    def push(self, dev):
        for fact in dev.pending:
            self.server_now = max(self.server_now + self.rng.randint(1, 50), self.now)
            edges = set()
            for d in fact["dependencies"]:
                info = self.engine.facts.get(d)
                if info is not None and not info["admitted"]:
                    edges.update(info["frontier"])  # PROTOCOL §6.4: through a rejected fact
                else:
                    edges.add(d)
            edges = sorted(edges)
            if fact["op_id"] in self.expect_reject and fact["op_id"] not in self.engine.facts:
                nt = self.server_now  # a rejected fact's normalized time is its receive time
            else:
                nt = c.normalize_time(fact["client_time"], self.server_now, [self.engine.node_time[e] for e in edges])
            arrival = {
                "fact": fact,
                "receive_time": self.server_now,
                "normalized_time": nt,
                "edges": edges,
                "capabilities": list(dev.caps),
            }
            self.engine.accept(arrival, chooser=self.choose)
            rejected = self.engine.outcomes[fact["op_id"]]["outcome"] == "rejected"
            if rejected != (fact["op_id"] in self.expect_reject):
                raise AssertionError("generator produced an unexpected outcome: %s for %s"
                                     % (self.engine.outcomes[fact["op_id"]], fact["type"]))
            self.arrivals.append(arrival)
            if not (rejected and self.catalog):
                # A rejected fact is not a causal node, so its edges must not hide its
                # parents from a later sync's frontier (CHOICE: corrected only in the
                # catalog and identity modes, so that the earlier modes reproduce their
                # committed vectors exactly).
                self.has_child.update(edges)
            for d in self.engine.created[self.ncreated:]:
                if self.queue:
                    # Devices see decisions in their commits and depend on them (PROTOCOL §7.6); the
                    # Devices of the listening-only generator (--no-queue) know facts only.
                    self.has_child.update(d["dependencies"])
            self.ncreated = len(self.engine.created)
            res = self.engine.resolutions.get(fact["op_id"])
            if res is not None and res["mode"] == "created":
                self.pod_show[res["podcast_id"]] = self.url_show[c.normalize_url(fact["payload"]["feed_url"])]
        dev.pending = []

    def choose(self, dtype, info):
        """The server's random choices for a decision it makes now."""
        r = self.server_rng
        ep, sq = info["commit"]  # the commit's revision (a commit has one)
        ch = {"decision_type": dtype, "decision_id": rand_uuid(r), "epoch": ep, "seq": sq}
        if dtype == rules.LAYOUT:
            ch["position_ids"] = [rand_uuid(r) for _ in range(info["fresh"])]
        elif dtype == rules.RESOLVED_ADD or (dtype == rules.ADMISSION and info.get("resolve")):
            ch["position_id"] = rand_uuid(r)
            if info["random"]:
                ch["draw"] = r.randint(0, info["size"])
        elif dtype == rules.MAPPING and info.get("allocate"):
            ch["occurrence_id"] = rand_uuid(r)
        return ch

    def sync(self, dev):
        self.push(dev)
        dev.frontier = set(n for n in self.engine.order
                           if n not in self.has_child and (self.queue or n not in self.engine.decisions))
        dev.alias = {n: a["podcast_id"] for n, a in self.engine.aliases.items()}
        dev.subs = {}
        dev.episodes = {}
        dev.qin, dev.qpos, dev.qref = set(), {}, {}
        dev.qpolicy = dict(rules.DEFAULT_QUEUE_POLICY)
        for (entity, eid), recs in self.engine.proj.items():
            if entity == "podcast":
                dev.subs[eid] = dict(recs["subscription"])
            elif entity == "episode":
                dev.episodes[eid] = {
                    "play_state": recs.get("play_state", {}).get("play_state", "unplayed"),
                    "position": recs.get("position", {}).get("position", 0),
                    "on": recs.get("favorite", {}).get("on", False),
                    "claim": recs.get("position", {}).get("claim_id"),
                    "generation_id": recs.get("position", {}).get("generation_id"),
                    "history": recs.get("history", {}).get("time"),
                    "history_gen": recs.get("history", {}).get("generation_id"),
                    "identity": recs.get("identity"),
                    "duration": recs.get("measured_duration", {}).get("duration_ms"),
                }
                qp = recs.get("queue_position")
                if qp is not None:
                    dev.qpos[eid] = (qp["key"], qp["position_id"])
                    ident = recs["identity"]
                    dev.qref[eid] = {"episode_id": eid, "podcast_id": ident["podcast_id"],
                                     "generation_id": qp["generation_id"],
                                     "occurrence": json.loads(json.dumps(ident["occurrence"]))}
                    if recs.get("queue_membership", {}).get("in_queue"):
                        dev.qin.add(eid)
            elif entity == "account" and "queue_policy" in recs:
                dev.qpolicy = dict(recs["queue_policy"])
        dev.pending_pods = set()
        dev.pending_new = set()
        dev.local = {}
        if self.catalog:
            self.sync_catalog(dev)
        if self.listening:
            self.adopt(dev)

    def sync_catalog(self, dev):
        """The canonical records and policies a sync delivers; the synced items join
        the device's replica (an initial snapshot never files)."""
        dev.meta, dev.pol, dev.sent, dev.disc, dev.rules = {}, {}, {}, None, {}
        for (entity, eid), recs in self.engine.proj.items():
            if "metadata" in recs:
                f = {k: v for k, v in recs["metadata"].items() if k not in ("revision", "fetch_time")}
                dev.meta[(entity, eid)] = {"revision": recs["metadata"]["revision"], "fields": f}
                if entity == "episode":
                    rep = dev.cat.setdefault(recs["identity"]["podcast_id"], {"known": {}, "gens": set()})
                    prev = rep["known"].get(eid)
                    date = f["pub_date"] if f["pub_date"] is not None else recs["metadata"]["fetch_time"]
                    rep["known"][eid] = {"media": f["media"] is not None or (prev is not None and prev["media"]),
                                         "date": date}
                    if f["media"] is None:
                        dev.co_ev.setdefault(eid, recs["metadata"]["revision"])
            if entity == "podcast" and "policy" in recs:
                dev.pol[eid] = recs["policy"]["new_episodes_action"]
            if entity == "podcast" and "identity_rules" in recs:
                dev.rules[eid] = recs["identity_rules"]
            if entity == "account" and "discovery_policy" in recs:
                dev.disc = recs["discovery_policy"]["new_episodes_action"]

    # -- device actions --------------------------------------------------------

    def live_known(self, dev):
        # A merged-away podcast is not live (its subscription names its survivor).
        return sorted(P for P, s in dev.subs.items() if s["state"] not in ("removed", "merged")
                      and P not in dev.pending_pods)

    def pick_url(self):
        r = self.rng.random()
        if r < 0.93:
            return self.rng.choice(self.rng.choice(self.shows)["variants"])
        return self.rng.choice(self.rng.choice(self.shows)["alternates"])

    def a_subscribe(self, dev, preview=False):
        url = self.pick_url()
        norm = c.normalize_url(url)
        P = dev.alias.get(norm)
        payload = {"feed_url": url}
        targets = []
        if P is None:
            if norm in dev.pending_new:
                return False
            dev.pending_new.add(norm)
        else:
            if P in dev.pending_pods:
                return False
            s = dev.subs[P]
            if preview and s["state"] != "removed" and self.rng.random() < 0.7:
                return False
            payload["observed"] = {"podcast_id": P, "generation_id": s["generation_id"], "state": s["state"]}
            targets = [{"entity": "podcast", "id": P}]
            if s["state"] == "removed":
                dev.pending_pods.add(P)
            elif not preview:
                s["state"] = "subscribed"
        if not preview:
            if self.rng.random() < 0.3:
                payload["policy"] = {
                    "new_episodes_action": self.rng.choice(rules.POLICY_ACTIONS),
                    "keep_updated": self.rng.random() < 0.5,
                }
            if self.rng.random() < 0.15:
                payload["credentials_secret"] = rand_uuid(self.rng)
        self.emit(dev, "podcast.register_preview" if preview else "podcast.subscribe", payload, targets)
        return True

    def a_podcast_op(self, dev, typ):
        live = self.live_known(dev)
        if not live:
            return False
        P = self.rng.choice(live)
        s = dev.subs[P]
        sloppy = self.rng.random() < 0.15  # a stale UI or a concurrent change
        if typ == "podcast.archive" and s["state"] != "subscribed" and not sloppy:
            return False
        if typ == "podcast.restore" and s["state"] != "archived" and not sloppy:
            return False
        payload = {}
        if typ == "podcast.set_policy":
            fields = self.rng.choice([["new_episodes_action"], ["keep_updated"], ["new_episodes_action", "keep_updated"]])
            if "new_episodes_action" in fields:
                payload["new_episodes_action"] = self.rng.choice(rules.POLICY_ACTIONS)
            if "keep_updated" in fields:
                payload["keep_updated"] = self.rng.random() < 0.5
        elif typ == "podcast.set_credentials":
            payload = {"clear": True} if self.rng.random() < 0.3 else {"secret_id": rand_uuid(self.rng)}
        elif typ == "podcast.change_url":
            show = self.pod_show.get(P, 0)
            r = self.rng.random()
            if r < 0.5:
                url = self.rng.choice(self.shows[show]["alternates"])
            elif r < 0.75:
                url = self.rng.choice(self.contested)
            else:
                url = self.rng.choice(self.rng.choice(self.shows)["variants"])
            payload = {"url": url}
        self.emit(dev, typ, payload, [{"entity": "podcast", "id": P, "generation": s["generation_id"]}])
        if typ == "podcast.set_policy" and "new_episodes_action" in payload:
            dev.pol[P] = payload["new_episodes_action"]
        if typ == "podcast.archive":
            s["state"] = "archived"
        elif typ == "podcast.restore" and s["state"] == "archived":
            s["state"] = "subscribed"
        elif typ == "podcast.unsubscribe":
            s["state"] = "removed"
        return True

    def device_ref(self, dev, P, occ):
        """The episode a device derives for an occurrence with the identity_rules it
        synced (PROTOCOL §3.3 rules 1-4, the published resolver), and the occurrence it sends
        (rule 3 names its GUID+enclosure kind and value)."""
        rules_rec = dev.rules.get(P) or {}
        g = occ.get("guid") if occ["k"] != "enclosure" else None
        u = c.normalize_url(occ["enclosure_url"]) if "enclosure_url" in occ else None
        d = occ.get("publication_time")
        kfam = "atom-id" if occ["k"] == "atom-id" else "rss-guid"
        aliases = dict(((a["kind"], a["value"]), a["episode_id"]) for a in rules_rec.get("aliases", []))
        dg = dict((x["guid"], x["releases"]) for x in rules_rec.get("disputed_guids", []))
        occ = dict(occ)
        if g is not None:
            occ["k"], occ["v"] = kfam, g
            if g in dg:
                rels = dg[g]
                if d is not None:
                    near = [r for r in rels if r["publication_time"] is not None
                            and abs(d - r["publication_time"]) <= rules.RELEASE_WINDOW_MS]
                    if near:
                        r = min(near, key=lambda r: (abs(d - r["publication_time"]), r["publication_time"],
                                                     r["episode_id"].encode("utf-8")))
                        return r["episode_id"], occ
                elif u is not None and sum(1 for r in rels if u in r["enclosures"]) == 1:
                    return [r for r in rels if u in r["enclosures"]][0]["episode_id"], occ
                if d is not None and u is not None:
                    occ["k"], occ["v"] = "rss-guid+enclosure", c.episode_v_guid_enclosure(g, u)
                    return c.episode_id(self.namespace, P, occ["k"], occ["v"]), occ
                return c.episode_id(self.namespace, P, kfam, g), occ
            return aliases.get(("guid", g)) or c.episode_id(self.namespace, P, kfam, g), occ
        occ["k"], occ["v"] = "enclosure", u
        if u not in set(rules_rec.get("disputed_enclosures", [])) and ("enclosure", u) in aliases:
            return aliases[("enclosure", u)], occ
        return c.episode_id(self.namespace, P, "enclosure", u), occ

    def make_ref(self, dev, P):
        G = dev.subs[P]["generation_id"]
        show = self.pod_show.get(P, 0)
        if self.identity:
            sh = self.shows[show]
            occ = dict(occ_at(sh, self.rng.choice(sh["feed"]), self.now))
        else:
            occ = dict(self.rng.choice(self.shows[show]["items"]))
        Pref, Gref, Pid = P, G, P
        if self.bogus and self.rng.random() < 0.06:
            r = self.rng.random()
            if r < 0.35:
                Pref = Pid = c.podcast_id(self.namespace, rand_uuid(self.rng))
                Gref = c.generation_id(self.namespace, Pref, rand_uuid(self.rng))
            elif r < 0.7 or not self.mismatch:
                Gref = c.generation_id(self.namespace, P, rand_uuid(self.rng))
            else:
                others = [Q for Q in dev.subs if Q != P]
                if others:
                    Pid = self.rng.choice(sorted(others))
        E = c.episode_id(self.namespace, Pid, occ["k"], occ["v"])
        if self.identity and Pid == P and Pref == P:
            E, occ = self.device_ref(dev, P, occ)
        return {
            "episode_id": E,
            "podcast_id": Pref,
            "generation_id": Gref,
            "occurrence": occ,
        }

    def a_episode_op(self, dev, typ):
        live = self.live_known(dev)
        if not live:
            return False
        if typ in ("inbox.dismiss", "inbox.dismiss_all") and "inbox" not in dev.caps:
            return False
        if typ == "favorite.set" and "favorites" not in dev.caps:
            return False
        if typ == "inbox.dismiss_all":
            refs = [self.make_ref(dev, self.rng.choice(live)) for _ in range(self.rng.randint(1, 5))]
        elif typ == "inbox.dismiss":
            P = self.rng.choice(live)
            refs = [self.make_ref(dev, P) for _ in range(self.rng.randint(1, 3))]
        else:
            refs = [self.make_ref(dev, self.rng.choice(live))]
        uniq = {}
        for r in refs:
            uniq.setdefault(r["episode_id"], r)
        refs = [uniq[k] for k in sorted(uniq)]
        targets = [{"entity": "episode", "id": r["episode_id"], "generation": r["generation_id"]} for r in refs]
        ref = refs[0]
        seen = dev.episodes.get(ref["episode_id"], {"play_state": "unplayed", "position": 0, "on": False})
        if typ in ("inbox.dismiss", "inbox.dismiss_all"):
            payload = {"episodes": refs}
        elif typ == "episode.mark_played":
            payload = {"episode": ref, "reset_position": self.rng.random() < 0.7}
        elif typ == "episode.restore_state":
            payload = {
                "episode": ref,
                "play_state": self.rng.choice(rules.PLAY_STATES),
                "position": self.rng.choice([0, seen["position"], self.rng.randint(0, 3600000)]),
            }
        elif typ == "favorite.set":
            payload = {"episode": ref, "on": (not seen["on"]) if self.rng.random() < 0.8 else seen["on"]}
            seen["on"] = payload["on"]
            dev.episodes[ref["episode_id"]] = seen
        else:
            payload = {"episode": ref}
        op = self.emit(dev, typ, payload, targets)
        if self.listening:
            claims = typ in ("episode.reset_position", "episode.restore_state") or (
                typ == "episode.mark_played" and payload["reset_position"])
            if claims:
                self.adopt_own_claim(dev, ref, op, payload.get("position", 0))
        return True

    def a_discovery(self, dev):
        a = self.rng.choice(rules.DISCOVERY_ACTIONS)
        self.emit(dev, "account.set_discovery_policy", {"new_episodes_action": a},
                  [{"entity": "account", "id": self.account_id}])
        dev.disc = a
        return True

    # -- PROTOCOL §8: the player -------------------------------------------------

    def measured(self, dev, ref):
        """The device's measured duration: the item's, give or take a quarter second per
        device (deterministic, no draw)."""
        h = hashlib.sha256((dev.id + ref["episode_id"]).encode("utf-8")).digest()
        return item_duration(ref["occurrence"]) + (h[0] % 3 - 1) * 250

    def session_emit(self, dev, typ, extra):
        pl = dev.player
        payload = {"episode": pl["ref"], "session_id": pl["sid"]}
        payload.update(extra)
        return self.emit(dev, typ, payload, episode_targets(pl["ref"]))

    def note_local(self, dev):
        pl = dev.player
        dev.local[pl["ref"]["episode_id"]] = {"position": pl["pos"], "claim": pl["claim"],
                                              "generation_id": pl["ref"]["generation_id"]}

    def within_threshold(self, dev, pl):
        """Android's almostEnded with the device's own measured duration and threshold."""
        return pl["dur"] > 0 and pl["pos"] >= pl["dur"] - dev.threshold

    def advance(self, pl, delta):
        """Moves the media position; the contribution follows Android's five-second saves:
        forward displacement from the session's start, frozen below it (PROTOCOL §8.8)."""
        pl["pos"] = max(0, min(pl["dur"], pl["pos"] + delta))
        if pl["pos"] > pl["start_pos"]:
            # PROTOCOL §8.8, with the baseline restarting after a reload (`base` is the
            # contribution when the player was reloaded; 0 without forks).
            pl["contrib"] = pl.get("base", 0) + pl["pos"] - pl["start_pos"]
        self.now += max(0, delta) // 10

    def save(self, dev, activity=True, claim=False):
        pl = dev.player
        # Exported checkpoints carry the ordinal of their local save; at most one per
        # minute is exported, so ordinals may skip.
        pl["ord"] += 1 if self.rng.random() < 0.85 else 2
        extra = {"ordinal": pl["ord"], "position": pl["pos"], "contribution": pl["contrib"], "activity": activity}
        # A claim covers a new save and carries it (PROTOCOL §8.1 as corrected): only a
        # device with save identities (PROTOCOL §6.10) claims.
        claim = claim and self.forks
        if claim:
            extra["claim"] = True
            if pl.get("claim") is not None:
                extra["observed_claim"] = pl["claim"]
        if self.rng.random() < 0.15:
            extra["duration"] = pl["dur"]
        if self.forks:
            self.record(dev)
            extra["save"] = self.save_identity(dev)
        op = self.session_emit(dev, "session.checkpoint", extra)
        if self.forks:
            pl["mine"].add(op)
        if claim:
            pl["claim"] = op
        self.note_local(dev)
        return op

    # -- PROTOCOL §6.10: save identities, restart and tail exports --------------------

    def record(self, dev):
        """A five-second save in the local journal (not exported by itself)."""
        pl = dev.player
        pl["sidx"] += 1
        sid = hash_uuid("save/%s/%s/%d" % (dev.lib, pl["sid"], pl["sidx"]))
        self.save_truth[sid] = (pl["journal"][-1]["id"] if pl["journal"] else None, pl["contrib"])
        pl["journal"].append({"id": sid,
                              "index": pl["sidx"], "baseline": pl["baseline"], "pos": pl["pos"],
                              "contrib": pl["contrib"], "activity": pl["playing"]})

    def save_identity(self, dev, passive=False):
        """PROTOCOL §6.10 `save` of an export of the latest journal save: `first` is the first save
        after the previous export, absent when it covers no new save."""
        pl = dev.player
        last = pl["journal"][-1]
        sv = {"id": last["id"], "index": last["index"], "baseline": last["baseline"], "passive": passive}
        if pl["sidx"] > pl["exported"]:
            sv["first"] = pl["journal"][pl["exported"]]["id"]
        pl["exported"] = pl["sidx"]
        return sv

    def passive_export(self, dev):
        """The restart export (or, under local detection, the tail export): the latest
        unexported save, passive (PROTOCOL §6.10)."""
        pl = dev.player
        if pl is None or not pl["journal"] or pl["sidx"] <= pl["exported"]:
            return None
        last = pl["journal"][-1]
        pl["ord"] += 1
        extra = {"ordinal": pl["ord"], "position": last["pos"], "contribution": last["contrib"],
                 "activity": last["activity"], "save": self.save_identity(dev, passive=True)}
        op = self.session_emit(dev, "session.checkpoint", extra)
        pl["mine"].add(op)
        return op

    def reload(self, dev):
        """The player loads the episode again (process restart, a restored copy): a new
        baseline; Android's played duration restarts from the current position (PROTOCOL §7.1)."""
        pl = dev.player
        pl["nbase"] += 1
        pl["baseline"] = hash_uuid("baseline/%s/%s/%d" % (dev.lib, pl["sid"], pl["nbase"]))
        pl["base"], pl["start_pos"] = pl["contrib"], pl["pos"]

    def start_session(self, dev, ref, pos=None, observed=None):
        E, G = ref["episode_id"], ref["generation_id"]
        dur = self.measured(dev, ref)
        if pos is None:
            # The newest locally available position, including local unsent saves.
            view = dev.local.get(E) or dev.episodes.get(E)
            if view is not None and view.get("generation_id") == G:
                pos, observed = view["position"], view.get("claim")
            else:
                pos, observed = 0, None
        if pos >= dur - 1000:
            pos = 0  # a finished position: play from the start
        extra = {"start_position": pos, "activity": True}
        if observed is not None:
            extra["observed_claim"] = observed
        if self.rng.random() < 0.8:
            extra["duration"] = dur
        if self.queue and "queue" in dev.caps and E not in dev.qin:
            # Playing an unqueued episode enqueues it at the enqueue location
            # (PROTOCOL §7.6: the gap the device resolved, and its key prediction).
            members, place = self.resolve_add(dev, [ref], current=False)
            extra["queue_bundle"] = {"key": members[0]["key"], "position_id": members[0]["position_id"],
                                     "place": place}
        dev.player = {"sid": rand_uuid(self.rng), "ref": ref, "start_pos": pos, "pos": pos, "contrib": 0, "ord": 0,
                      "playing": True, "sealed": False, "abandoned": False, "claim": None, "dur": dur}
        if self.forks:
            pl = dev.player
            pl.update(journal=[], sidx=0, exported=0, nbase=0, base=0, mine=set())
            pl["baseline"] = hash_uuid("baseline/%s/%s/0" % (dev.lib, pl["sid"]))
            extra["baseline"] = pl["baseline"]
        op = self.session_emit(dev, "session.start", extra)
        dev.player["claim"] = op
        if self.forks:
            dev.player["mine"].add(op)
        self.note_local(dev)
        return op

    def end_session(self, dev, reason, activity=True):
        pl = dev.player
        completed = reason == "ended" or (reason in ("skipped", "switched") and self.within_threshold(dev, pl))
        pl["ord"] += 1
        extra = {
            "ordinal": pl["ord"],
            "position": pl["pos"],
            "contribution": pl["contrib"],
            "activity": activity,
            "reason": reason,
            "completed": completed,
            "threshold_ms": dev.threshold,
            # Keep skipped episodes off: a skip outside the threshold removes from the queue.
            "remove_from_queue": reason == "skipped" and not completed and not dev.keep_skipped,
            # Android writes the history time for end, skip and switch.
            "history": reason in ("ended", "skipped", "switched"),
        }
        if reason != "superseded" and self.rng.random() < 0.2:
            extra["duration"] = pl["dur"]
        if self.forks:
            last = pl["journal"][-1] if pl["journal"] else None
            if reason != "superseded" or last is None or (last["pos"], last["contrib"]) != (pl["pos"], pl["contrib"]):
                self.record(dev)
            # A seal at the pre-adoption position covers no new save unless saves were
            # still unexported: then `first` is absent and the export reuses its node.
            extra["save"] = self.save_identity(dev)
        op = self.session_emit(dev, "session.end", extra)
        if self.forks:
            pl["mine"].add(op)
        pl["sealed"], pl["playing"] = True, False
        if completed or extra["remove_from_queue"]:
            dev.qin.discard(pl["ref"]["episode_id"])  # the completion bundle / skip removal
        return op

    def finalize(self, dev):
        """Lazy completion of the paused previous episode, from the position the device
        holds (its own, or the adopted one under the adopted claim)."""
        pl = dev.player
        if pl["ord"] < 1:
            return None
        completed = self.within_threshold(dev, pl)
        if completed:
            dev.qin.discard(pl["ref"]["episode_id"])
        return self.session_emit(dev, "session.finalize", {
            "ordinal": pl["ord"],
            "claim_id": pl["claim"],
            "completed": completed,
            "threshold_ms": dev.threshold,
            "remove_from_queue": False,
            "history": True,
        })

    def a_leave_handoff(self, dev):
        """PROTOCOL §8: another device pauses just before the end and syncs; this
        device syncs, loads that episode paused (displaying the other's claim) and leaves
        it at once or later, within or outside its own threshold; sometimes its library is
        copied first, so two copies hold the pending leave."""
        srcs = [d for d in self.devices if d is not dev and self.live_known(d)]
        if not srcs:
            return False
        src = self.rng.choice(srcs)
        if src.player is None or not src.player["playing"]:
            self.a_play(src)
        pl = src.player
        if pl is None or not pl["playing"]:
            return True
        self.advance(pl, pl["dur"] - self.rng.randint(1000, 45000) - pl["pos"])
        pl["playing"] = False
        self.save(src)
        self.push(src)
        dev.online = True
        self.sync(dev)
        # The device names the episode with the same feed evidence (so it measures about
        # the same duration); it loads it only if its view shows it with a claim.
        ref = json.loads(json.dumps(pl["ref"]))
        v = dev.episodes.get(ref["episode_id"])
        if (v is None or v.get("claim") is None or v.get("generation_id") != ref["generation_id"]
                or ref["podcast_id"] not in self.live_known(dev)):
            return True
        cur = dev.player
        if cur is not None and (cur["ref"]["episode_id"], cur["ref"]["generation_id"]) == (
                ref["episode_id"], ref["generation_id"]):
            return True
        self.leave_current(dev)
        self.load_paused(dev, ref)
        r = self.rng.random()
        if r < 0.3:
            # Copied while it holds the pending leave; each copy may leave it soon.
            for d in [dev] + self.copy_pending(dev):
                if d.player is not None and d.player.get("pending") and self.rng.random() < 0.6:
                    self.leave_pending(d, "switched" if self.rng.random() < 0.6 else "skipped")
                    d.player = None
        elif r < 0.7:
            self.leave_pending(dev, "switched" if self.rng.random() < 0.5 else "skipped")
            dev.player = None
        return True

    def near_end(self, dev, ref):
        view = dev.local.get(ref["episode_id"]) or dev.episodes.get(ref["episode_id"])
        if view is None or view.get("generation_id") != ref["generation_id"]:
            return False
        dur = self.measured(dev, ref)
        return dur > 0 and view["position"] >= dur - dev.threshold

    def load_paused(self, dev, ref):
        """PROTOCOL §8: the device loads an episode paused and plays nothing in this
        load (a paused load, continuous playback off): a pending leave transition with its
        own ID, the newest position the device holds and the claim it displays. Copies of
        the library keep it. Playing it starts a session (resume); switching away or
        skipping records episode.leave."""
        E, G = ref["episode_id"], ref["generation_id"]
        view = dev.local.get(E) or dev.episodes.get(E)
        if view is not None and view.get("generation_id") == G:
            pos, claim = view["position"], view.get("claim")
        else:
            pos, claim = 0, None
        dev.player = {"pending": True, "ref": ref, "pos": pos, "claim": claim, "dur": self.measured(dev, ref),
                      "transition": rand_uuid(self.rng), "playing": False, "sealed": True, "abandoned": False, "ord": 0}

    def leave_pending(self, dev, reason):
        """episode.leave from the pending transition: Android's history mark and the
        device's own completion decision (its measured duration and smart threshold) under
        the claim it displayed; a skip outside the threshold with Keep skipped episodes off
        removes from the queue."""
        pl = dev.player
        completed = self.within_threshold(dev, pl)
        if reason == "switched" and completed and self.rng.random() < 0.2:
            reason = "ended"
        payload = {"episode": pl["ref"], "transition_id": pl["transition"], "reason": reason, "position": pl["pos"],
                   "completed": completed, "threshold_ms": dev.threshold,
                   "remove_from_queue": reason == "skipped" and not completed and not dev.keep_skipped,
                   "history": True}
        if pl["claim"] is not None:
            payload["observed_claim"] = pl["claim"]
        self.emit(dev, "episode.leave", payload, episode_targets(pl["ref"]))
        if completed or payload["remove_from_queue"]:
            dev.qin.discard(pl["ref"]["episode_id"])

    def leave_current(self, dev):
        pl = dev.player
        if pl is None:
            return
        if pl.get("pending"):
            self.leave_pending(dev, "switched")
        elif pl["playing"]:
            self.advance(pl, self.rng.randint(0, 60000))
            self.end_session(dev, "switched")
        else:
            self.finalize(dev)
        dev.player = None

    def resume(self, dev):
        pl = dev.player
        if pl["sealed"] or pl["abandoned"]:
            # A new session at the newest position (the adopted one, if any); the pending
            # finalization is consumed without completion.
            ref, pos, claim = pl["ref"], pl["pos"], pl["claim"]
            dev.player = None
            self.start_session(dev, ref, pos, claim)
        else:
            pl["playing"] = True
            # Android saves when playback reaches READY; PROTOCOL §8.1: the
            # person's play command on an open session is a claim of its writing path
            # (recorded at READY), which the device's pending references then name.
            self.save(dev, claim=True)
        return True

    def adopt(self, dev):
        """After a sync, a paused player adopts a newer claim's position: an open session
        is sealed at its pre-adoption position without listening activity."""
        pl = dev.player
        if pl is None or pl["playing"]:
            return
        view = dev.episodes.get(pl["ref"]["episode_id"])
        if view is None or view.get("generation_id") != pl["ref"]["generation_id"]:
            return
        claim = view.get("claim")
        if claim is None or claim == pl["claim"] or claim in pl.get("mine", ()):
            # PROTOCOL §6.10: a position whose claim is one of the device's own continuation's
            # facts is its own.
            return
        if not pl["sealed"] and not pl["abandoned"]:
            self.end_session(dev, "superseded", activity=False)
        pl["claim"], pl["pos"] = claim, view["position"]

    def adopt_own_claim(self, dev, ref, op, position):
        """The device's own explicit reset / restore of its loaded, paused episode."""
        pl = dev.player
        if pl is None or pl["playing"] or (pl["ref"]["episode_id"], pl["ref"]["generation_id"]) != (
                ref["episode_id"], ref["generation_id"]):
            return
        if not pl["sealed"] and not pl["abandoned"]:
            self.end_session(dev, "superseded", activity=False)
        pl["claim"], pl["pos"] = op, position

    def export_save(self, dev):
        """Before a cut the device exports its unexported local save."""
        pl = dev.player
        if pl is not None and pl["playing"] and not pl["sealed"]:
            self.advance(pl, self.rng.randint(1000, 60000))
            if pl["pos"] >= pl["dur"]:
                self.end_session(dev, "ended")
                dev.player = None
            else:
                self.save(dev)

    # -- PROTOCOL §8: actions ------------------------------------------------------

    def listened_refs(self, dev):
        """Episodes of live podcasts the device's view shows with an owned position (some
        device listened to them): playing one is a takeover."""
        live = {dev.subs[P]["generation_id"] for P in self.live_known(dev)}
        out = []
        for E in sorted(dev.episodes):
            v = dev.episodes[E]
            if v.get("claim") is not None and v.get("identity") and v.get("generation_id") in live:
                out.append({"episode_id": E, "podcast_id": v["identity"]["podcast_id"],
                            "generation_id": v["generation_id"],
                            "occurrence": json.loads(json.dumps(v["identity"]["occurrence"]))})
        return out

    def a_play(self, dev):
        pl = dev.player
        if pl is not None and not pl["playing"] and self.rng.random() < 0.3:
            return self.resume(dev)
        live = self.live_known(dev)
        if not live:
            return False
        listened = self.listened_refs(dev)
        if listened and self.rng.random() < 0.4:
            ref = self.rng.choice(listened)
        else:
            ref = self.make_ref(dev, self.rng.choice(live))
        if pl is not None and (pl["ref"]["episode_id"], pl["ref"]["generation_id"]) == (
                ref["episode_id"], ref["generation_id"]):
            return False if pl["playing"] else self.resume(dev)
        self.leave_current(dev)
        self.start_session(dev, ref)
        return True

    def a_switch(self, dev, paused=None):
        """Loads another episode (a paused one is lazily finalized first, a pending leave
        recorded). PROTOCOL §8: sometimes it is loaded paused, often an episode
        another device listened to."""
        live = self.live_known(dev)
        if not live:
            return False
        if paused is None:
            paused = self.leaves and self.rng.random() < 0.35
        if paused:
            # Often an episode another device listened to, and then often one paused within
            # this device's threshold of its end (a lazy completion by leave).
            listened = self.listened_refs(dev)
            near = [r for r in listened if self.near_end(dev, r)]
            r = self.rng.random()
            if near and r < 0.4:
                ref = self.rng.choice(near)
            elif listened and r < 0.75:
                ref = self.rng.choice(listened)
            else:
                ref = self.make_ref(dev, self.rng.choice(live))
        else:
            ref = self.make_ref(dev, self.rng.choice(live))
        pl = dev.player
        if pl is not None and (pl["ref"]["episode_id"], pl["ref"]["generation_id"]) == (
                ref["episode_id"], ref["generation_id"]):
            return False
        self.leave_current(dev)
        if paused:
            self.load_paused(dev, ref)
        else:
            self.start_session(dev, ref)
        return True

    def a_listen(self, dev):
        pl = dev.player
        if pl is None or not pl["playing"]:
            return False
        for _ in range(self.rng.randint(1, 3)):
            r = self.rng.random()
            if r < 0.65:
                self.advance(pl, self.rng.randint(20000, 15 * 60000))  # playing
            elif r < 0.75:
                self.advance(pl, pl["dur"] - self.rng.randint(0, 90000) - pl["pos"])  # seek near the end
            elif r < 0.85:
                self.advance(pl, self.rng.randint(30000, 10 * 60000))  # seek forward
            else:
                self.advance(pl, -self.rng.randint(10000, 20 * 60000))  # seek back, maybe below the start
            if pl["pos"] >= pl["dur"]:
                self.end_session(dev, "ended")
                dev.player = None
                return True
            if self.forks and self.rng.random() < 0.4:
                self.record(dev)  # a five-second save that is not exported yet
            else:
                self.save(dev)
        return True

    def a_pause(self, dev):
        pl = dev.player
        if pl is None or not pl["playing"]:
            return False
        if self.rng.random() < 0.35:
            # Paused just before the end (inside or outside the smart threshold): the lazy
            # finalization decides completion later (scenarios 7, 8b, 31).
            self.advance(pl, pl["dur"] - self.rng.randint(1000, 45000) - pl["pos"])
        else:
            self.advance(pl, self.rng.randint(0, 30000))
        if pl["pos"] >= pl["dur"]:
            self.end_session(dev, "ended")
            dev.player = None
            return True
        pl["playing"] = False
        self.save(dev)
        return True

    def a_skip(self, dev):
        pl = dev.player
        if pl is not None and pl.get("pending"):
            self.leave_pending(dev, "skipped")
            dev.player = None
            return True
        if pl is None or pl["sealed"] or pl["abandoned"]:
            return False
        self.end_session(dev, "skipped", activity=pl["playing"])
        dev.player = None
        return True

    def a_stop(self, dev):
        """Suspended (the player was stopped; the pending finalization stays) or removed."""
        pl = dev.player
        if pl is None or pl["sealed"] or pl["abandoned"]:
            return False
        if self.rng.random() < 0.7:
            self.end_session(dev, "suspended", activity=pl["playing"])
        else:
            self.end_session(dev, "removed", activity=pl["playing"])
            dev.player = None
        return True

    def a_restart(self, dev):
        """Process death: the last durable save is exported, the open session is abandoned
        (it stands at its latest checkpoint); the paused player is restored passively."""
        pl = dev.player
        if pl is None or pl["sealed"] or pl["abandoned"]:
            return False
        if self.forks:
            # The last durable save may be unexported; the restart exports it first
            # (passive), the player reloads under a new baseline, and the next playback
            # either resumes the same session (claiming at its first export) or starts a
            # new one.
            if pl["playing"]:
                self.advance(pl, self.rng.randint(0, 20000))
                if pl["pos"] >= pl["dur"]:
                    self.end_session(dev, "ended")
                    dev.player = None
                    return True
                self.record(dev)
                pl["playing"] = False
            self.passive_export(dev)
            self.reload(dev)
            pl["abandoned"] = self.rng.random() < 0.5
            return True
        if pl["playing"]:
            self.advance(pl, self.rng.randint(0, 20000))
            if pl["pos"] >= pl["dur"]:
                self.end_session(dev, "ended")
                dev.player = None
                return True
            pl["playing"] = False
            self.save(dev)
        pl["abandoned"] = True
        return True

    def history_ref(self, dev, E):
        v = dev.episodes[E]
        return {"episode_id": E, "podcast_id": v["identity"]["podcast_id"], "generation_id": v["history_gen"],
                "occurrence": json.loads(json.dumps(v["identity"]["occurrence"]))}

    def a_history_remove(self, dev):
        if "history" not in dev.caps:
            return False
        cands = [(E, None) for E, v in sorted(dev.episodes.items()) if v.get("history") is not None and v.get("identity")]
        pl = dev.player
        if pl is not None and not pl.get("pending") and pl["ref"]["episode_id"] not in dict(cands):
            cands.append((pl["ref"]["episode_id"], pl["ref"]))  # its own listening is in History
        if not cands:
            return False
        E, ref = self.rng.choice(cands)
        if ref is None:
            ref, shown = self.history_ref(dev, E), dev.episodes[E]["history"]
        else:
            shown = self.now + dev.offset
        self.export_save(dev)
        op = self.emit(dev, "history.remove", {"episode": ref}, episode_targets(ref))
        dev.removals.append((op, ref, shown))
        if E in dev.episodes:
            dev.episodes[E]["history"] = None
        return True

    def a_history_restore(self, dev):
        if "history" not in dev.caps:
            return False
        if dev.removals and self.rng.random() < 0.85:
            op, ref, shown = dev.removals.pop()  # Undo of the latest removal
            dev.restored.append((op, ref, shown))
        elif dev.restored:
            op, ref, shown = self.rng.choice(dev.restored)  # a duplicate Undo: already_restored
        else:
            return False
        self.emit(dev, "history.restore", {"episode": ref, "removal": op, "time": shown}, episode_targets(ref))
        return True

    def a_history_clear(self, dev):
        if "history" not in dev.caps:
            return False
        self.export_save(dev)
        self.emit(dev, "history.clear", {}, [])
        for v in dev.episodes.values():
            v["history"] = None
        return True

    def a_statistics_reset(self, dev):
        if "statistics" not in dev.caps:
            return False
        self.export_save(dev)
        self.emit(dev, "statistics.reset", {}, [])
        return True

    # -- PROTOCOL §7.6: the queue ------------------------------------------------------

    def local_queue(self, dev):
        """The device's queue view, in key order."""
        items = [(E, dev.qpos[E][0]) for E in dev.qin if E in dev.qpos]
        items.sort(key=functools.cmp_to_key(_qcmp))
        return [E for E, _ in items]

    def device_values(self, dev, E):
        """The canonical sort values as the device sees them (PROTOCOL §7.6: occurrence evidence,
        measured duration, empty podcast titles)."""
        ref = dev.qref[E]
        occ = ref["occurrence"]
        v = {"title": occ.get("title"), "podcast_id": ref["podcast_id"], "podcast_title": "",
             "publication_time": occ.get("publication_time"),
             "duration": dev.episodes.get(E, {}).get("duration")}
        if self.catalog:
            # PROTOCOL §9.4: the canonical metadata it holds, each value falling back to the
            # occurrence; the measured duration wins over the feed's.
            m = (dev.meta.get(("episode", E)) or {}).get("fields") or {}
            pm = (dev.meta.get(("podcast", ref["podcast_id"])) or {}).get("fields") or {}
            if m.get("title") is not None:
                v["title"] = m["title"]
            if m.get("pub_date") is not None:
                v["publication_time"] = m["pub_date"]
            if v["duration"] is None and m.get("media") is not None:
                v["duration"] = m["media"]["duration"]
            v["podcast_title"] = pm.get("title") or ""
        return v

    def placement(self, dev, E):
        k, pid = dev.qpos[E]
        return {"episode": dev.qref[E], "key": k, "position_id": pid}

    def new_pid(self):
        return rand_uuid(self.rng)

    def place_at(self, dev, q, i):
        """PROTOCOL §7.6: the gap before index i of the local queue q: `start` at 0,
        else after the episode at i - 1 (the tail too, never `end`)."""
        return {"kind": "start"} if i == 0 else {"kind": "after", "after": dev.qref[q[i - 1]]}

    def resort(self, dev, order, q):
        """The local stable sort of the whole queue q (canonical, or a shuffle for random);
        returns the new order and re-keys it locally."""
        if order == "random":
            idx = list(range(len(q)))
            self.rng.shuffle(idx)
        else:
            idx = qs.canonical_order([self.device_values(dev, E) for E in q], order)
        out = [q[i] for i in idx]
        keyed = qs.rekey([dev.qpos[E] for E in q], idx, self.new_pid)
        for i, (k, pid, _) in zip(idx, keyed):
            dev.qpos[q[i]] = (k, pid)
        return out

    def insert_keys(self, dev, q, i, refs):
        """Keys strictly between the neighbours at index i of q, for refs in order."""
        low = dev.qpos[q[i - 1]][0] if i > 0 else None
        high = dev.qpos[q[i]][0] if i < len(q) else None
        for ref in refs:
            E = ref["episode_id"]
            pid = self.new_pid()
            low = c.queue_between(low, high, c.uuid_to_u(pid))
            dev.qpos[E], dev.qref[E] = (low, pid), ref

    def resolve_add(self, dev, refs, current=True):
        """The adding device (PROTOCOL §7.6): it resolves its enqueue location into a
        gap of its queue (back: the end; front: after the consecutive run it is downloading
        at the head; after current: after its current episode, else the front scan; random:
        a drawn index), inserts there, then stable-sorts its queue under keep-sorted (the
        replay's rule), and sends the gap with its members' predicted keys. Returns
        (members, place)."""
        q = self.local_queue(dev)
        loc = dev.qpolicy["enqueue_location"]
        if loc == "back":
            i, place = len(q), {"kind": "end"}
        else:
            if loc == "front":
                i = 0
                if q and self.rng.random() < 0.3:
                    i = min(len(q), self.rng.randint(1, 2))  # skipping items it is downloading
            elif loc == "after_current":
                pl = dev.player
                cur = pl["ref"]["episode_id"] if (pl is not None and current) else None
                i = q.index(cur) + 1 if cur in q else 0
            else:
                i = self.rng.randint(0, len(q))
            place = self.place_at(dev, q, i)
        self.insert_keys(dev, q, i, refs)
        new = [r["episode_id"] for r in refs]
        for E in new:
            dev.qin.add(E)
        local = q[:i] + new + q[i:]
        if dev.qpolicy["keep_sorted"] and dev.qpolicy["sort_order"] != "random":
            self.resort(dev, dev.qpolicy["sort_order"], local)
        return [self.placement(dev, E) for E in new], place

    @staticmethod
    def placement_targets(placements):
        return [episode_targets(p["episode"])[0] for p in placements]

    def a_q_add(self, dev):
        live = self.live_known(dev)
        if not live:
            return False
        refs = []
        for _ in range(self.rng.randint(1, 3)):
            r = self.make_ref(dev, self.rng.choice(live))
            if r["episode_id"] not in dev.qin and all(x["episode_id"] != r["episode_id"] for x in refs):
                refs.append(r)
        if not refs:
            return False
        members, place = self.resolve_add(dev, refs)
        self.emit(dev, "queue.add", {"members": members, "place": place}, self.placement_targets(members))
        return True

    def a_q_remove(self, dev):
        q = self.local_queue(dev)
        if not q:
            return False
        picked = sorted(self.rng.sample(q, min(len(q), self.rng.randint(1, 2))))
        reason = "user" if self.rng.random() < 0.85 else "download_deleted"
        refs = [dev.qref[E] for E in picked]
        self.emit(dev, "queue.remove", {"episodes": refs, "reason": reason}, [episode_targets(r)[0] for r in refs])
        for E in picked:
            dev.qremoved.append((dev.qref[E], q.index(E)))
        for E in picked:
            dev.qin.discard(E)
        return True

    def a_q_restore(self, dev):
        """Undo of the device's latest removal (Android's addQueueItemAt): its saved index,
        resolved against the queue at Undo time and clamped to its length
        (PROTOCOL §7.6); never sorted."""
        while dev.qremoved:
            ref, idx = dev.qremoved.pop()
            E = ref["episode_id"]
            if E in dev.qin:
                continue
            q = self.local_queue(dev)
            i = min(idx, len(q))
            place = self.place_at(dev, q, i)
            self.insert_keys(dev, q, i, [ref])
            self.emit(dev, "queue.restore", {"members": [self.placement(dev, E)], "place": place}, episode_targets(ref))
            dev.qin.add(E)
            return True
        return False

    def a_q_move(self, dev):
        """A drag (disabled under keep-sorted, Android); rarely an entry another device has
        removed (a stale view)."""
        if dev.qpolicy["keep_sorted"]:
            return False
        q = self.local_queue(dev)
        outs = sorted(E for E in dev.qpos if E not in dev.qin)
        if outs and self.rng.random() < 0.08:
            E, rest = self.rng.choice(outs), q
        elif len(q) >= 2:
            E = self.rng.choice(q)
            rest = [x for x in q if x != E]
        else:
            return False
        i = self.rng.randint(0, len(rest))
        place = self.place_at(dev, rest, i)
        self.insert_keys(dev, rest, i, [dev.qref[E]])
        p = self.placement(dev, E)
        p["place"] = place
        self.emit(dev, "queue.move", p, episode_targets(dev.qref[E]))
        return True

    def a_q_reorder(self, dev):
        """Move several selected episodes to the top or the bottom (not under keep-sorted):
        the complete selected run in its resulting order (PROTOCOL §7.6), at the
        start or at the end."""
        q = self.local_queue(dev)
        if dev.qpolicy["keep_sorted"] or len(q) < 2:
            return False
        sel = sorted(self.rng.sample(q, self.rng.randint(1, min(3, len(q) - 1))), key=q.index)
        rest = [x for x in q if x not in sel]
        top = self.rng.random() < 0.5
        if top:
            self.insert_keys(dev, rest, 0, [dev.qref[E] for E in sel])
        else:
            self.insert_keys(dev, rest, len(rest), [dev.qref[E] for E in sel])
        payload = {"run": [dev.qref[E] for E in sel], "place": {"kind": "start" if top else "end"}}
        if self.rng.random() < 0.5:
            # The device's sparse key predictions for some of the moved episodes (PROTOCOL §7.6).
            k = self.rng.randint(1, len(sel))
            payload["members"] = [self.placement(dev, E) for E in sorted(self.rng.sample(sel, k), key=sel.index)]
        self.emit(dev, "queue.reorder", payload, [episode_targets(dev.qref[E])[0] for E in sel])
        return True

    def a_q_sort_dialog(self, dev):
        """The sort dialog: the policy (`sorting` as one register; random turns keep-sorted
        off) and the sort command over the queue it sees, sent even when it changes nothing
        (PROTOCOL §7.6): a random sort carries its permutation."""
        order = self.rng.choice(qs.SORT_ORDERS)
        keep = order != "random" and self.rng.random() < 0.75
        self.emit(dev, "account.set_queue_policy", {"sorting": {"keep_sorted": keep, "sort_order": order}},
                  [{"entity": "account", "id": self.account_id}])
        dev.qpolicy["keep_sorted"], dev.qpolicy["sort_order"] = keep, order
        q = self.local_queue(dev)
        observed = [dev.qref[E] for E in q]
        out = self.resort(dev, order, q) if q else []
        srt = {"sort_order": order}
        if order == "random":
            srt["permutation"] = [dev.qref[E] for E in out]
        self.emit(dev, "queue.reorder", {"sort": srt, "observed": observed}, [{"entity": "account", "id": self.account_id}])
        return True

    def a_q_location(self, dev):
        loc = self.rng.choice(rules.ENQUEUE_LOCATIONS)
        self.emit(dev, "account.set_queue_policy", {"enqueue_location": loc},
                  [{"entity": "account", "id": self.account_id}])
        dev.qpolicy["enqueue_location"] = loc
        return True

    def a_q_clear(self, dev):
        q = self.local_queue(dev)
        if not q:
            return False
        refs = [dev.qref[E] for E in sorted(q)]
        self.emit(dev, "queue.clear", {"observed": refs}, [episode_targets(r)[0] for r in refs])
        dev.qin = set()
        return True

    # -- PROTOCOL §9.4: refreshes on devices and on the server ------------------------------

    def a_refresh(self, dev):
        """A device refreshes one podcast it knows (keep-updated or a manual refresh): it
        parses the publisher's feed as it is now, sends the records that are new or differ
        from the canonical ones it holds (base = their revision), then one proposal per
        eligible playable discovery new to its replica (or catalog-only there and now with
        an enclosure, with its evidence), unless this is its first population of the
        generation or the podcast is not subscribed in its view. Its filing applies locally
        at once (a queue add with its complete result). A sloppy device occasionally
        proposes what is not eligible (a backfilled item, a catalog-only one, an
        unsubscribed podcast), so the server's checks are exercised."""
        live = self.live_known(dev)
        if not live:
            return False
        P = self.rng.choice(live)
        if P not in self.pod_show:
            return False
        show = self.shows[self.pod_show[P]]
        s = dev.subs[P]
        G, state = s["generation_id"], s["state"]
        t = self.now + dev.offset
        items = feed_at(show, self.now)
        rep = dev.cat.setdefault(P, {"known": {}, "gens": set()})
        first_pop = G not in rep["gens"] and not rep["known"]
        refs, obs_items = {}, []
        idents = {}
        for occ, f in items:
            if self.identity:
                E, occ = self.device_ref(dev, P, occ)
            else:
                E = c.episode_id(self.namespace, P, occ["k"], occ["v"])
            idents[id(f)] = E
            refs[E] = {"episode_id": E, "podcast_id": P, "generation_id": G, "occurrence": dict(occ)}
            held = dev.meta.get(("episode", E))
            if (held is not None and held["fields"] == f) or dev.sent.get(("episode", E)) == f:
                continue
            obs_items.append({"episode": refs[E], "base": held["revision"] if held is not None else None, "fields": f})
            dev.sent[("episode", E)] = f
        payload = {"podcast_id": P, "batch_id": rand_uuid(self.rng), "parser_version": "android-3.12.1",
                   "fetch_time": t, "items": obs_items}
        ff = feed_fields_at(show, self.now)
        held = dev.meta.get(("podcast", P))
        if not ((held is not None and held["fields"] == ff) or dev.sent.get(("podcast", P)) == ff):
            payload["feed"] = {"base": held["revision"] if held is not None else None, "fields": ff}
            dev.sent[("podcast", P)] = ff
        if obs_items or "feed" in payload:
            targets = [{"entity": "podcast", "id": P}] + [{"entity": "episode", "id": x["episode"]["episode_id"]}
                                                          for x in obs_items]
            op = self.emit(dev, "catalog.observe", payload, targets)
            for x in obs_items:
                if x["fields"]["media"] is None:
                    dev.co_ev[x["episode"]["episode_id"]] = op
        newest = max((v["date"] for v in rep["known"].values()), default=None)
        sloppy = self.rng.random() < 0.2
        if (not first_pop and state == "subscribed") or sloppy:
            for occ, f in items:
                E = idents[id(f)]
                prev = rep["known"].get(E)
                media = f["media"] is not None
                gained = None
                if prev is None:
                    ok = media and (f["pub_date"] is None or newest is None or f["pub_date"] >= newest)
                elif not prev["media"] and media:
                    gained = dev.co_ev.get(E)
                    ok = gained is not None
                else:
                    continue
                if not ok and not (sloppy and self.rng.random() < 0.3):
                    continue
                self.propose(dev, P, refs[E], f, newest, gained)
        for occ, f in items:
            E = idents[id(f)]
            prev = rep["known"].get(E)
            date = f["pub_date"] if f["pub_date"] is not None else (prev["date"] if prev is not None else t)
            rep["known"][E] = {"media": f["media"] is not None or (prev is not None and prev["media"]), "date": date}
        if state != "removed":
            rep["gens"].add(G)
        return True

    def propose(self, dev, P, ref, f, newest, gained):
        a = dev.pol.get(P, "global")
        if a == "global":
            a = dev.disc or rules.DEFAULT_DISCOVERY_ACTION
        payload = {"podcast_id": P, "generation_id": ref["generation_id"], "episode": ref,
                   "batch_id": rand_uuid(self.rng), "pub_date": f["pub_date"], "newest_before_batch": newest,
                   "action": a, "policy_node": None}
        if gained is not None:
            payload["gained_media"] = {"catalog_only_observation": gained}
        targets = episode_targets(ref)
        if a == "queue" and self.queue and "queue" in dev.caps and ref["episode_id"] not in dev.qin:
            members, place = self.resolve_add(dev, [ref])
            payload["queue_bundle"] = {"key": members[0]["key"], "position_id": members[0]["position_id"],
                                       "place": place}
        self.emit(dev, "discovery.propose", payload, targets)

    def server_step(self):
        """The server's refresh: completes a fetch it started earlier, starts one (its
        result is applied at a later step, racing what devices push meanwhile), or fetches
        at once; any registered podcast (scheduled or an explicit refresh)."""
        for R in [R for R, P in self.pending_fetch.items() if P in self.engine.pmerged]:
            del self.pending_fetch[R]  # the scheduler stops refreshing a merged-away podcast
        if self.pending_fetch and self.rng.random() < 0.6:
            R = self.rng.choice(sorted(self.pending_fetch))
            self.server_fetch(self.pending_fetch.pop(R), R)
            return
        pods = sorted(P for P in self.pod_show if self.engine._known_podcast(P) and P not in self.engine.pmerged)
        if not pods:
            return
        P = self.rng.choice(pods)
        if P in self.pending_fetch.values():
            return
        if self.rng.random() < 0.3:
            R = rand_uuid(self.rng)
            arrival = {"fetch_start": {"refresh_id": R, "podcast_id": P}}
            self.engine.accept(arrival)
            self.arrivals.append(arrival)
            self.pending_fetch[R] = P
        else:
            self.server_fetch(P, None)

    def server_fetch(self, P, R):
        rng = self.rng
        self.server_now = max(self.server_now + rng.randint(1, 50), self.now)
        rt = self.server_now
        started = R is not None  # a started fetch completes as a success or a failure
        R = R or rand_uuid(rng)
        show = self.shows[self.pod_show[P]]
        fx = {"refresh_id": R, "podcast_id": P}
        r = rng.random()
        if r < 0.1:
            label, code = rng.choice([("I/O", None), ("http status", 503), ("not found", 404), ("unauthorized", 401),
                                      ("parser exception", None)])
            fx.update(outcome="failure", fetch_time=rt - rng.randint(0, 2000),
                      error={"label": label, "code": code, "detail": None if code is None else "HTTP %d" % code},
                      next_attempt=rt + 900000)
        elif r < 0.16 and not started and P in self.engine.stored:
            # A newer parser re-parses the stored response: some descriptions change.
            Rp = self.engine.stored[P]["refresh_id"]
            tp = self.parse_time[Rp]
            items = []
            for occ, f in feed_at(show, tp):
                if rng.random() < 0.5:
                    f = dict(f, description="re-parsed")
                items.append({"occurrence": occ, "fields": f})
            fx.update(outcome="re-parse", reparse_of=Rp, feed=feed_fields_at(show, tp), items=items)
        else:
            moved_to, new_feed_url, redirects = None, None, []
            q = rng.random()
            if q < 0.06:
                moved_to = rng.choice(show["alternates"] + ["https://moved%d.example.org/feed" % rng.randint(0, 3)])
                redirects = [{"status": rng.choice([301, 308]), "location": moved_to}]
            elif q < 0.1:
                redirects = [{"status": rng.choice([302, 307]), "location": rng.choice(show["alternates"])}]
            elif q < 0.12:
                new_feed_url = rng.choice(show["alternates"])
            if self.identity and show.get("redirect_at") is not None and self.now >= show["redirect_at"]:
                # The feed now answers with a permanent redirect onto another show's URL.
                target = self.shows[show["redirect_to"]]["variants"][0]
                moved_to, redirects = target, [{"status": rng.choice([301, 308]), "location": target}]
            fx.update(outcome="success", fetch_time=rt - rng.randint(0, 2000), feed=feed_fields_at(show, self.now),
                      items=[{"occurrence": occ, "fields": f} for occ, f in feed_at(show, self.now)],
                      redirects=redirects, moved_to=moved_to, new_feed_url=new_feed_url, next_attempt=rt + 3600000)
            self.parse_time[R] = self.now
        arrival = {"fetch": fx, "receive_time": rt}
        self.engine.accept(arrival, chooser=self.choose)
        self.arrivals.append(arrival)
        for d in self.engine.created[self.ncreated:]:
            self.has_child.update(d["dependencies"])
        self.ncreated = len(self.engine.created)

    # -- PROTOCOL §6.10: backups and copies ----------------------------------------------

    def a_backup(self, dev):
        """A backup of the library (restored later, possibly several times)."""
        self.backups.append((dev, copy.deepcopy(dev)))
        return True

    def a_copy(self, dev, force=False):
        """A library copied onto another device: from its current state (sometimes the
        original is lost: a migration) or from an older backup; detected locally or not."""
        if len(self.devices) >= 7 and not force:
            return False
        if self.backups and self.rng.random() < 0.4:
            src, snap = self.rng.choice(self.backups)
            lose = False
        else:
            src, snap = dev, dev
            lose = len(self.devices) > 1 and self.rng.random() < 0.15
        return self.copy_from(src, snap, lose)

    def copy_from(self, src, snap, lose):
        new = copy.deepcopy(snap)
        new.lib = rand_uuid(self.rng)
        new.online = self.rng.random() < 0.6
        if self.rng.random() < 0.3:
            new.offset = self.rng.choice([0, self.rng.randint(-30000, 30000)])
        if lose and src in self.devices:
            self.devices.remove(src)
            src.pending = []  # the old phone is gone with its unsent outbox
        self.ncopies += 1
        self.devices.append(new)
        if self.rng.random() < 0.55:
            self.detect_locally(new)
        else:
            self.restart_copy(new)
        return new

    def copy_scenario(self, dev):
        """A copy of a library whose player is busy with a session: playing with
        unexported saves (a tail), or paused (a pending lazy finalization), then one or two
        restores of that backup (sibling copies); the following steps favour the original
        and its copies (continuing, resuming, finalizing, resetting). PROTOCOL §8:
        sometimes the library holds a pending leave instead (an episode loaded paused), so
        the copies leave one transition."""
        if self.leaves and self.rng.random() < 0.3 and self.a_switch(dev, paused=True):
            self.copy_pending(dev)
            return True
        pl = dev.player
        if pl is None or pl["sealed"] or pl["abandoned"]:
            self.a_play(dev)
            pl = dev.player
        if pl is not None and not pl["playing"] and not pl["sealed"] and not pl["abandoned"]:
            self.resume(dev)
            pl = dev.player
        if pl is not None and pl["playing"]:
            for _ in range(self.rng.randint(0, 3)):
                self.advance(pl, self.rng.randint(5000, 90000))
                if pl["pos"] >= pl["dur"]:
                    self.end_session(dev, "ended")
                    dev.player = None
                    break
                self.record(dev)
            if dev.player is not None and self.rng.random() < 0.45:
                self.a_pause(dev)
        if self.rng.random() < 0.25:
            self.a_bad(dev)  # a rejected writing tip: the copy forks from (or diverges behind) it
        snap = copy.deepcopy(dev)
        self.backups.append((dev, snap))
        lose = len(self.devices) > 1 and self.rng.random() < 0.2
        copies = [self.copy_from(dev, snap, lose and k == 0) for k in range(1 if self.rng.random() < 0.7 else 2)]
        self.focus = [d for d in [dev] + copies if d in self.devices]
        self.focus_steps = self.rng.randint(8, 25)
        return True

    def copy_pending(self, dev):
        """Copies of a library holding a pending leave (each keeps its transition ID);
        returns the copies."""
        snap = copy.deepcopy(dev)
        self.backups.append((dev, snap))
        copies = [self.copy_from(dev, snap, False) for _ in range(1 if self.rng.random() < 0.7 else 2)]
        self.focus = [d for d in [dev] + copies if d in self.devices]
        self.focus_steps = self.rng.randint(8, 25)
        return copies

    def detect_locally(self, dev):
        """A copy detected locally (PROTOCOL §6.10): a new transport and author F; installation.fork naming the writing tip
        (unless the library never wrote as its author); the open session's unexported saves
        exported passively by F (the tail export) and the session sealed locally; the
        player state and any pending lazy finalization kept."""
        old, tip = dev.id, dev.prev
        dev.id = rand_uuid(self.rng)
        dev.seq, dev.prev = 0, None
        if tip is not None:
            dev.frontier = set(dev.frontier) | {tip}
            self.emit(dev, "installation.fork", {"author": old, "head": tip}, [])
        pl = dev.player
        if pl is not None:
            if not pl["sealed"]:
                self.passive_export(dev)
                pl["sealed"] = True
            pl["playing"] = False

    def restart_copy(self, dev):
        """A copy not detected locally (PROTOCOL §6.10): it starts as a restarted process:
        the restart export, a reloaded player (a new baseline), the same author and tip."""
        pl = dev.player
        if pl is None:
            return
        if not pl["sealed"] and not pl["abandoned"]:
            pl["playing"] = False
            self.passive_export(dev)
            self.reload(dev)
            pl["abandoned"] = self.rng.random() < 0.4
        pl["playing"] = False

    def a_bad(self, dev):
        """A fact rejected for its payload in the device's chain: it holds its author
        position and passes its author on (PROTOCOL §6.10)."""
        live = self.live_known(dev)
        if not live:
            return False
        ref = self.make_ref(dev, self.rng.choice(live))
        self.emit(dev, "favorite.set", {"episode": ref, "on": "yes"}, episode_targets(ref), reject=True)
        return True

    def act(self, dev):
        knows = bool(self.live_known(dev))
        choices = [
            (1.5 if knows else 4, lambda: self.a_subscribe(dev)),
            (0.7 if knows else 1.5, lambda: self.a_subscribe(dev, preview=True)),
            (1.2, lambda: self.a_podcast_op(dev, "podcast.archive")),
            (1.2, lambda: self.a_podcast_op(dev, "podcast.restore")),
            (1.2, lambda: self.a_podcast_op(dev, "podcast.unsubscribe")),
            (1, lambda: self.a_podcast_op(dev, "podcast.set_policy")),
            (0.6, lambda: self.a_podcast_op(dev, "podcast.set_credentials")),
            (0.6, lambda: self.a_podcast_op(dev, "podcast.change_url")),
            (2, lambda: self.a_episode_op(dev, "episode.mark_played")),
            (1.2, lambda: self.a_episode_op(dev, "episode.mark_unplayed")),
            (1, lambda: self.a_episode_op(dev, "episode.reset_position")),
            (1.2, lambda: self.a_episode_op(dev, "episode.restore_state")),
            (1.5, lambda: self.a_episode_op(dev, "favorite.set")),
            (1.2, lambda: self.a_episode_op(dev, "inbox.dismiss")),
            (0.6, lambda: self.a_episode_op(dev, "inbox.dismiss_all")),
            (0.2, lambda: self.a_discovery(dev)),
        ]
        if self.listening:
            playing = dev.player is not None and dev.player["playing"]
            choices += [
                (4 if knows else 0.5, lambda: self.a_play(dev)),
                (4 if playing else 0.1, lambda: self.a_listen(dev)),
                (4 if playing else 0.1, lambda: self.a_pause(dev)),
                (0.6, lambda: self.a_skip(dev)),
                (0.3, lambda: self.a_stop(dev)),
                (0.3, lambda: self.a_restart(dev)),
                (0.8, lambda: self.a_history_remove(dev)),
                (1.5 if dev.removals else (0.15 if dev.restored else 0), lambda: self.a_history_restore(dev)),
                (0.15, lambda: self.a_history_clear(dev)),
                (0.2, lambda: self.a_statistics_reset(dev)),
            ]
        if self.leaves:
            # PROTOCOL §8: a paused load (continuous playback off, a paused load),
            # and a paused load of another device's episode near its end.
            choices += [(2.5 if knows else 0, lambda: self.a_switch(dev, paused=True)),
                        (1.0 if knows else 0, lambda: self.a_leave_handoff(dev))]
        if self.queue and "queue" in dev.caps:
            queued = bool(dev.qin)
            choices += [
                (3 if knows else 0.2, lambda: self.a_q_add(dev)),
                (1.2 if queued else 0.05, lambda: self.a_q_remove(dev)),
                (1 if dev.qremoved else 0, lambda: self.a_q_restore(dev)),
                (1.5 if queued else 0.05, lambda: self.a_q_move(dev)),
                (0.6 if queued else 0.05, lambda: self.a_q_reorder(dev)),
                (0.5 if queued else 0.3, lambda: self.a_q_sort_dialog(dev)),
                (0.12, lambda: self.a_q_location(dev)),
                (0.12 if queued else 0.02, lambda: self.a_q_clear(dev)),
            ]
        if self.catalog:
            choices += [(7 if knows else 0.2, lambda: self.a_refresh(dev))]
        if self.forks:
            choices += [
                (0.35, lambda: self.a_backup(dev)),
                (0.3 if len(self.devices) < 7 else 0, lambda: self.a_copy(dev)),
                (0.15, lambda: self.a_bad(dev)),
            ]
        weights = [w for w, _ in choices]
        for _ in range(8):
            _, fn = self.rng.choices(choices, weights)[0]
            if fn():
                return

    def run(self, steps):
        forced = []
        if self.forks and (self.force_copies or self.rng.random() < 0.3):
            forced.append(self.rng.randint(steps // 4, max(steps // 4, steps // 2)))
            if self.rng.random() < 0.35:
                forced.append(self.rng.randint(forced[0] + 1, max(forced[0] + 1, (3 * steps) // 4)))
        for step in range(steps):
            self.now += self.rng.randint(1, 4000)
            if self.listening and self.rng.random() < 0.03:
                self.now += self.rng.randint(3600000, 4 * 86400000)  # hours or days pass
            if self.catalog and self.rng.random() < (0.18 if self.identity else 0.12):
                self.server_step()
                continue
            dev = self.rng.choice(self.devices)
            if step in forced:
                # Copy a library busy with a session (the second one is usually a copy
                # of a copy, from the current focus).
                cands = [d for d in self.focus if d in self.devices and d is not self.focus[0]] if step != forced[0] else []
                if not cands:
                    players = [d for d in self.devices if d.player is not None]
                    cands = players or [dev]
                self.copy_scenario(self.rng.choice(cands))
                continue
            if self.focus_steps > 0:
                self.focus_steps -= 1
                live = [d for d in self.focus if d in self.devices]
                if live and self.rng.random() < 0.7:
                    dev = self.rng.choice(live)
                    if self.rng.random() < 0.12:
                        # A reset (or a clear) after syncing, observing whatever tails arrived.
                        dev.online = True
                        self.sync(dev)
                        if "statistics" in dev.caps:
                            self.a_statistics_reset(dev)
                        continue
                    pl = dev.player
                    if pl is not None and not pl["playing"]:
                        r2 = self.rng.random()
                        if r2 < 0.3:
                            self.resume(dev)  # resuming the copied (or adopted) session
                            continue
                        if r2 < 0.55 and self.a_switch(dev):
                            continue  # loading another episode finalizes the paused one
            r = self.rng.random()
            if r < 0.12:
                dev.online = not dev.online
                if dev.online:
                    self.sync(dev)
            elif r < 0.27:
                if dev.online:
                    self.sync(dev)
            else:
                self.act(dev)
        for dev in self.devices:
            self.now += self.rng.randint(1, 4000)
            self.push(dev)
        for R in sorted(self.pending_fetch):
            P = self.pending_fetch.pop(R)
            if P not in self.engine.pmerged:
                self.server_fetch(P, R)

    def case(self):
        return {
            "account_id": self.account_id,
            "namespace": self.namespace,
            "arrivals": self.arrivals,
            "expected": self.engine.result(),
        }


def generate_sim(rng, bogus=True, mismatch=True, steps=None, listening=True, queue=True, forks=True, copies=False,
                 catalog=False, identity=False, leave=True):
    """Runs one random account history; returns the simulation (its engine, its arrivals
    and, with forks, the ground truth of every journal save)."""
    sim = Sim(rng, bogus=bogus, mismatch=mismatch, listening=listening, queue=queue, forks=forks, copies=copies,
              catalog=catalog, identity=identity, leave=leave)
    sim.run(steps if steps is not None else rng.randint(15, 70))
    return sim


def generate_case(rng, bogus=True, mismatch=True, steps=None, listening=True, queue=True, forks=True, copies=False,
                  catalog=False, identity=False, leave=True):
    """Returns (case, engine) for one random account history."""
    sim = generate_sim(rng, bogus, mismatch, steps, listening, queue, forks, copies, catalog, identity, leave)
    return sim.case(), sim.engine


# ---------------------------------------------------------------------------
# PROTOCOL §6.11: recovery after a restore (--recovery)
# ---------------------------------------------------------------------------


def archive_commits(eng, rng):
    """The original run's archive as recovery uploads, grouped by commit (epoch, seq) in
    position order: each fact with its receipt (position and outcome), each decision as
    recorded (an implicit podcast.alias or podcast.resolution gets a drawn ID)."""
    commits = {}
    for op, e in eng.ar_facts.items():
        a = copy.deepcopy(e["arrival"])
        a["recovery"] = True
        a["receipt"] = dict(zip(("epoch", "seq", "index"), e["pos"]))
        if e["outcome"] is not None:
            a["receipt"].update(e["outcome"])
        if e.get("values"):
            a["receipt"]["values"] = copy.deepcopy(e["values"])  # PROTOCOL §7.6
        commits.setdefault(e["pos"][:2], []).append((e["pos"] + (op.encode(),), a))
    for e in eng.ar_dec.values():
        obj = eng.decision_object(e, rand_uuid(rng) if e["id"] is None else None)
        commits.setdefault(e["pos"][:2], []).append((e["pos"] + (obj["decision_id"].encode(),), {"decision": obj}))
    return dict((k, [x[1] for x in sorted(v, key=lambda x: x[0])]) for k, v in commits.items())


def entry_id(x):
    return x["fact"]["op_id"] if "fact" in x else x["decision"]["decision_id"]


def entry_deps(x):
    if "fact" in x:
        if (x.get("receipt") or {}).get("outcome") == "rejected":
            # A rejected fact joins on its edges (its resolved frontier, empty for an
            # envelope rejection); its dependencies may be the malformed ones it was
            # rejected for.
            return set(x["edges"])
        return set(x["fact"]["dependencies"]) | set(x["edges"])
    return set(x["decision"]["dependencies"])


def recovery_replicas(rng, nlost, partial=False):
    """Two or three replicas, each holding a prefix of the lost commits (a device that
    pulled up to some commit; whole commits). The longest holds them all unless
    `partial`."""
    n = rng.randint(2, 3)
    top = nlost if not partial else rng.randint(1, max(1, nlost - 1))
    cuts = [rng.randint(1, top) for _ in range(n - 1)] + [top]
    rng.shuffle(cuts)
    return [list(range(cut)) for cut in cuts]


def recovery_schedule(rng, lost, replicas, known, start, dup=0.15, boundary=0.35):
    """Uploads of the replicas' commits, interleaved, each replica in a random order its
    dependencies allow, with duplicates and request boundaries (`recovery_end`), ending
    with one. `known`: the IDs the restored server holds."""
    known = set(known)
    remaining = [set(r) for r in replicas]
    done, out, now = [], [], start

    def ready(ix):
        ids = set(entry_id(x) for x in lost[ix])
        return all(d in known or d in ids for x in lost[ix] for d in entry_deps(x))

    while any(remaining):
        if not any(ready(ix) for r in remaining for ix in r):
            raise ValueError("the lost commits admit no causal upload order")
        i = rng.choice([j for j, r in enumerate(remaining) if r])
        for _ in range(rng.randint(1, 4)):
            cands = sorted(ix for ix in remaining[i] if ready(ix))
            if not cands:
                break
            ix = rng.choice(cands)
            remaining[i].discard(ix)
            out.extend(copy.deepcopy(lost[ix]))
            known |= set(entry_id(x) for x in lost[ix])
            done.append(ix)
            if rng.random() < dup:
                out.append(copy.deepcopy(rng.choice(lost[rng.choice(done)])))
        if rng.random() < boundary:
            out.append({"recovery_end": True, "receive_time": now})
            now += 1000
    out.append({"recovery_end": True, "receive_time": now})
    return out


class RecoveryChooser(object):
    """The restored server's choices for the decisions recovery makes (unification merges,
    sort reconciliations) and live re-pushes make."""

    def __init__(self, rng, engine, arrivals):
        self.rng, self.engine, self.arrivals = rng, engine, arrivals

    def __call__(self, dtype, info):
        r = self.rng
        ep, sq = info["commit"]  # the commit's revision (a commit has one)
        ch = {"decision_type": dtype, "decision_id": rand_uuid(r), "epoch": ep, "seq": sq}
        if dtype == rules.LAYOUT:
            ch["position_ids"] = [rand_uuid(r) for _ in range(info["fresh"])]
        elif dtype == rules.RESOLVED_ADD or (dtype == rules.ADMISSION and info.get("resolve")):
            ch["position_id"] = rand_uuid(r)
            if info["random"]:
                ch["draw"] = r.randint(0, info["size"])
        elif dtype == rules.MAPPING and info.get("allocate"):
            ch["occurrence_id"] = rand_uuid(r)
        return ch


def repush(engine, arrival, now, epoch=None):
    """A lost fact pushed again live by a device that never stored its receipt: a new
    acceptance (a provisional receipt) at `now`, with the frontier and normalized time
    admission computes from what the restored server holds; None if a dependency is
    missing there."""
    f = arrival["fact"]
    frontier = set()
    for d in f["dependencies"]:
        if d in engine.ar_nodes:
            frontier.add(d)
        elif d in engine.ar_rejected:
            frontier.update(engine.ar_rejected[d])
        else:
            return None
    prev = f.get("previous_op_id")
    if prev is not None and prev not in engine.ar_facts:
        return None
    edges = sorted(frontier, key=lambda x: x.encode())
    a = {"fact": copy.deepcopy(f), "receive_time": now,
         "normalized_time": c.normalize_time(f["client_time"], now, [engine.ar_nodes[e] for e in edges]),
         "edges": edges, "capabilities": list(arrival["capabilities"])}
    if epoch is not None:
        a["receipt"] = {"epoch": epoch, "seq": 0, "index": 0}  # seq filled in by the caller
    return a


def run_recovery(case, k, schedule, rng, repushes=(), epoch2=False, after_each=None):
    """Runs the restored server: the original's first k arrivals, then `schedule`; each
    `repushes` entry (position in the schedule, lost fact arrival) is pushed live before
    that position when its dependencies are held; `after_each(engine, arrival)` is called
    after every scheduled or pushed arrival. Returns (engine, arrivals)."""
    eng = rules.Engine(case["account_id"], case["namespace"])
    arrs = []
    chooser = RecoveryChooser(rng, eng, arrs)
    for a in case["arrivals"][:k]:
        a = copy.deepcopy(a)
        eng.accept(a)
        arrs.append(a)
    now = max([a.get("receive_time", 0) for a in case["arrivals"]] + [0]) + 60000
    pushes = sorted(repushes, key=lambda x: x[0])
    for i, a in enumerate(schedule + [None]):
        while pushes and pushes[0][0] <= i:
            _, lost = pushes.pop(0)
            if lost["fact"]["op_id"] in eng.ar_facts:
                continue
            now += 1000
            p = repush(eng, lost, now, epoch=max(eng.epoch, 2) if epoch2 else None)
            if p is None:
                continue
            if "receipt" in p:
                p["receipt"]["seq"] = len(arrs) + 1
            eng.accept(p, chooser=chooser)
            arrs.append(p)
            if after_each is not None:
                after_each(eng, p)
        if a is None:
            break
        a = copy.deepcopy(a)
        if "recovery_end" in a:
            now += 1000
            a["receive_time"] = now
        if "recovery_end" in a or "decision" in a:
            eng.accept(a, chooser=chooser)
        else:
            eng.accept(a)
        arrs.append(a)
        if after_each is not None:
            after_each(eng, a)
    return eng, arrs


def own_pushes(rng, lost, known):
    """A device's own receipted pushes it never pulled back: one author's lost facts
    (with their receipts) without the decisions their commits made, as far as their
    dependencies are held."""
    facts = [x for cm in lost for x in cm if "fact" in x]
    authors = sorted(set(x["fact"]["author_id"] for x in facts))
    if not authors:
        return []
    who = rng.choice(authors)
    out, have = [], set(known)
    for x in facts:
        if x["fact"]["author_id"] == who and entry_deps(x) <= have:
            out.append(copy.deepcopy(x))
            have.add(entry_id(x))
    return out


def generate_recovery_case(rng, partial=False, repush_lost=False, own=False, leave=True):
    """A random case restored at a random commit; the lost commits re-delivered from
    two or three replicas' archives in a random order, with duplicates and request
    boundaries, optionally with some lost facts pushed again live first, or with one
    device's own receipted pushes (no decisions) uploaded first. Returns (case, info) with
    info = {original, k, lost, replicas, known} for property checks."""
    kind = rng.choice(["base", "catalog", "identity"])
    case, eng = generate_case(rng, catalog=kind != "base", identity=kind == "identity", leave=leave)
    n = len(case["arrivals"])
    commits = archive_commits(eng, rng)
    k = rng.randint(max(1, n // 3), max(1, n - 1))
    lost = [commits[key] for key in sorted(commits) if key[1] > k]
    known = set(entry_id(x) for key in commits if key[1] <= k for x in commits[key])
    replicas = recovery_replicas(rng, len(lost), partial) if lost else []
    schedule = recovery_schedule(rng, lost, replicas, known, 0) if lost else [{"recovery_end": True, "receive_time": 0}]
    if own and lost:
        mine = own_pushes(rng, lost, known)
        if mine:
            schedule = mine + [{"recovery_end": True, "receive_time": 0}] + schedule
    repushes = []
    if repush_lost and lost:
        facts = [x for cm in lost for x in cm if "fact" in x and x["receipt"].get("outcome") != rules.REJECTED]
        # A live push comes between upload requests: a request (its entries up to its
        # recovery_end) is one transaction that nothing interleaves (the recovery seed 6100611).
        bounds = [0] + [i + 1 for i, a in enumerate(schedule) if "recovery_end" in a]
        for x in rng.sample(facts, min(len(facts), rng.randint(1, 4))):
            at = rng.randint(0, len(schedule) // 2)
            repushes.append((min(b for b in bounds if b >= at), x))
    restored, arrs = run_recovery(case, k, schedule, rng, repushes, epoch2=rng.random() < 0.5)
    out = {"account_id": case["account_id"], "namespace": case["namespace"], "arrivals": arrs,
           "expected": restored.result()}
    return out, {"original": eng, "case": case, "k": k, "lost": lost, "replicas": replicas, "known": known,
                 "restored": restored}


def main(argv):
    ap = argparse.ArgumentParser(description="Generate random rules vector cases.")
    ap.add_argument("out_dir")
    ap.add_argument("--cases", type=int, default=500)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--per-file", type=int, default=100)
    ap.add_argument("--no-bogus", action="store_true", help="no EpisodeRefs naming unknown podcasts/generations")
    ap.add_argument("--no-listening", action="store_true",
                    help="subscription and episode operations only (that mode's exact random stream)")
    ap.add_argument("--no-queue", action="store_true",
                    help="no queue activity (the listening mode's exact facts); implies --no-forks")
    ap.add_argument("--no-forks", action="store_true",
                    help="no copies or save identities (the queue mode's exact facts)")
    ap.add_argument("--no-leave", action="store_true",
                    help="no paused loads or episode.leave (the facts without them, exactly)")
    ap.add_argument("--copies", action="store_true", help="every case copies at least one library")
    ap.add_argument("--catalog", action="store_true",
                    help="catalog observations, discovery proposals and server fetches")
    ap.add_argument("--identity", action="store_true",
                    help="identity reconciliation (reused GUIDs, GUID changes, podcast merges); "
                         "implies --catalog")
    ap.add_argument("--recovery", action="store_true",
                    help="recovery: random cases (any mode) restored at a random commit, the lost "
                         "commits re-delivered from replicas' archives with duplicates and request boundaries, "
                         "sometimes after live re-pushes")
    ap.add_argument("--file", default=None, help="write every case to OUT_DIR/FILE instead of rules_random_NNNN.json")
    args = ap.parse_args(argv)
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**31)
    rng = random.Random(seed)
    os.makedirs(args.out_dir, exist_ok=True)
    if args.recovery:
        cases = [generate_recovery_case(rng, partial=rng.random() < 0.3, repush_lost=rng.random() < 0.3,
                                        own=rng.random() < 0.3, leave=not args.no_leave)[0]
                 for _ in range(args.cases)]
    else:
        cases = [generate_case(rng, bogus=not args.no_bogus, listening=not args.no_listening, queue=not args.no_queue,
                               forks=not args.no_forks, copies=args.copies, catalog=args.catalog,
                               identity=args.identity, leave=not args.no_leave)[0]
                 for _ in range(args.cases)]
    if args.file is not None:
        path = os.path.join(args.out_dir, args.file)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"suite": "rules", "cases": cases}, f, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            f.write("\n")
        print("wrote %d cases to %s (seed %d)" % (len(cases), path, seed))
        return 0
    nfiles = 0
    for start in range(0, len(cases), max(1, args.per_file)):
        path = os.path.join(args.out_dir, "rules_random_%04d.json" % nfiles)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"suite": "rules", "cases": cases[start:start + args.per_file]}, f,
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            f.write("\n")
        nfiles += 1
    print("wrote %d cases in %d files to %s (seed %d)" % (len(cases), nfiles, args.out_dir, seed))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
