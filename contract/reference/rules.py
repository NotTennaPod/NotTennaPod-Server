"""Independent Python 3 reference of the projection rules (rule vectors).

Written only from docs/PROTOCOL.md and docs/CONTRACT.md ("Rule vectors"), never from the
Go code: subscriptions, generations and episode state (§7.1–§7.3), listening sessions,
position ownership, completion, last played, history and statistics (§8), the queue's
membership, operations, policy and `queue.resolved_add` decisions (§7.6), and copied
installations: receipt order, chain positions and derived authors, `installation.fork`,
session continuations, save identities, the save tree, cuts and claims (§6.10, §8.5).
Standard library only; reuses ntp_contract for canonical JSON, IDs, URL normalization,
queue keys, time normalization and envelope validation, and queue_sort for the canonical
queue comparators.

Server decisions (PROTOCOL §7.6). The server's own random choices (decision IDs, the
position IDs of re-keyed members, the random enqueue draw) and the decisions' commit
revisions are supplied by the vector: a fact arrival lists them in `decisions`, in the
order the server creates its decisions at that acceptance; `expected.decisions` holds the
decisions themselves. A recovered fact (`"recovery": true`) creates no decision and has
no outcome in the vector (its original receipt stands); a recovered decision arrives as
`{"decision": {...}}`. See docs/CONTRACT.md "Rule vectors".

The engine processes a `rules` case's arrivals in acceptance order, exactly as the server
would at acceptance:

* envelope validation (PROTOCOL §6.1, §6.4), author history, capabilities and the payload
  schemas (anything invalid is `rejected` with the reason codes of PROTOCOL §7.8, archived,
  not a node);
* subscribe / register-preview resolution (PROTOCOL §7.2: created, joined, opened, stale);
* the global causal order, maintained incrementally (PROTOCOL §6.9); contributions per
  scope ((podcast, P), (episode, E, G), account), and whole-scope folds in rank order
  after every arrival, for the affected scopes only;
* each fact's outcome at acceptance (applied / superseded / not_applied + reason);
* the live records (PROTOCOL §4).

Input that the server would never have accepted (a credential-bearing URL, an
unarchivable fact, an unknown type or version, a missing dependency, edges or normalized
times inconsistent with admission) raises RuleInputError: such a vector is malformed.

Copied installations (PROTOCOL §6.10). Every archived fact has a receipt position
`(epoch, seq, index)`; a fact arrival may state it in `receipt` (default: epoch 1, seq =
the arrival's 1-based position in the case, index 0). Live acceptances must come in
receipt order; recovered facts keep their original (possibly earlier) receipt, and may
state their original outcome (`receipt.outcome`/`reason`). Chain positions and effective
authors are a function of the archived facts in receipt order, recomputed when an earlier
receipt arrives; scopes are then refolded, outcomes never change. Session facts belong to
continuations `(session_id, effective author)`; a session's exported saves form a tree
from which played time, statistics cuts, export claims and the eligibility of copied
finalizations are derived. Transport framing (PROTOCOL §6.5) is not part of rule vectors:
the engine sees whole facts only.

Catalog, discovery and server refresh (PROTOCOL §9.4–§9.6): the device operations
`catalog.observe` and `discovery.propose` (v1), catalog items, the canonical replacement
rule per record folded in rank order, the `metadata`, `admission` and `refresh_status`
records, `discovery.admission` decisions whose filing acts at the proposal's place with
the proposal as origin (the server's own admissions are their own place), the queue
resolution of automatic adds, and the server's own fetches as an input stream:
`{"fetch_start": ...}` and `{"fetch": ...}` arrivals, applied as `catalog.refresh` parts,
admissions against the server's own fetch history and `podcast.moved` decisions (see
docs/CONTRACT.md "Catalog, discovery and server fetches").

Identity reconciliation (PROTOCOL §9.7): identity evidence (occurrences; a device
observation counts only for the plain derivation of its occurrence or a published rule),
lookup aliases only from server decisions, disputed enclosures and GUIDs, release groups
(7 days) with `episode.occurrence_mapping` decisions and the partition of facts by their
occurrence, the resolver for late facts, GUID-change matches on complete server fetches,
episode merges to the root survivor (contributions, listening indexes and evidence moved;
compound effects; references routed before validation), podcast merges (joined or rehomed
live generation, episode mappings by the survivor's rules, `generation_merged`), and the
`identity_rules` record and `merged` subscription state (see docs/CONTRACT.md "Identity
reconciliation").

`episode.leave` (PROTOCOL §8.3, §8.4; type_version 1, core): a non-claiming switch-away
fact, a history mark and a completion conditional on the observed claim, at most once per
transition, folded at its place.

Where play was pressed last (PROTOCOL §8.1): `session.checkpoint` with `claim` (an
intentional resume) is a position ownership claim of its own writing path, joining the
family's claim set; every later checkpoint, end, finalization and leave resolves against
the latest claim on its own path.

The queue replay (PROTOCOL §7.6): every placement carries the `place` its device resolved
(start, after an episode, end); the queue's order is the replay of every queue command in
the deterministic order over a structure of entries per (episode, generation class), out
entries and ghosts included, with membership at each step from the episode fold; sorts use
the canonical values the server recorded in receipts and decisions
(`expected.sort_values`); keys are only a materialization: each in-member's published key
is its latest key write in rank order, and the publication rule ends every commit with one
sparse `queue.layout`, listing only the members whose key changes, when those keys do not
give the replay's order. An Android import is one replay step (its decisions are modelled
as recovered decision arrivals). The reference implements only the full replay;
check_queue_replay.py compares an incremental one with it after every prefix.

Choices made where the protocol is silent are marked "CHOICE:" below.
"""

import copy
import functools
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ntp_contract as c  # noqa: E402
import queue_sort as qs  # noqa: E402

__all__ = [
    "RuleInputError",
    "Engine",
    "run_case",
    "has_userinfo",
    "OPERATION_CAPABILITY",
    "DEFAULT_POLICY",
    "PREVIEW_POLICY",
    "envelope_reason",
    "sort_records",
]

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

POLICY_ACTIONS = ("global", "inbox", "queue", "nothing")
DISCOVERY_ACTIONS = ("inbox", "queue", "nothing")
SUBSCRIPTION_STATES = ("not_subscribed", "subscribed", "archived", "removed")
PLAY_STATES = ("new", "unplayed", "played")

# PROTOCOL §7.2: an opened generation's base policy is the opening fact's
# `policy`, else global with keep updated on for a subscribe and off for a register-preview
# (Android creates previews with keep updated off).
DEFAULT_POLICY = {"new_episodes_action": "global", "keep_updated": True}
PREVIEW_POLICY = {"new_episodes_action": "global", "keep_updated": False}

# PROTOCOL §10.1: inbox.* need `inbox`, favorite.set needs `favorites`; everything else is core.
OPERATION_CAPABILITY = {
    "podcast.subscribe": "core",
    "podcast.register_preview": "core",
    "podcast.archive": "core",
    "podcast.restore": "core",
    "podcast.unsubscribe": "core",
    "podcast.set_policy": "core",
    "podcast.set_credentials": "core",
    "podcast.change_url": "core",
    "episode.mark_played": "core",
    "episode.mark_unplayed": "core",
    "episode.reset_position": "core",
    "episode.restore_state": "core",
    "inbox.dismiss": "inbox",
    "inbox.dismiss_all": "inbox",
    "favorite.set": "favorites",
    "account.set_discovery_policy": "core",
    # PROTOCOL §8.
    "session.start": "core",
    "session.checkpoint": "core",
    "session.end": "core",
    "session.finalize": "core",
    "history.remove": "history",
    "history.restore": "history",
    "history.clear": "history",
    "statistics.reset": "statistics",
    # PROTOCOL §7.6 (§10.1: the queue and the account's queue policy).
    "queue.add": "queue",
    "queue.restore": "queue",
    "queue.remove": "queue",
    "queue.move": "queue",
    "queue.reorder": "queue",
    "queue.clear": "queue",
    "account.set_queue_policy": "queue",
    # PROTOCOL §6.10.
    "installation.fork": "core",
    # PROTOCOL §9.4 (core capability).
    "catalog.observe": "core",
    "discovery.propose": "core",
    # PROTOCOL §8 (2026-10-05): a non-claiming switch-away fact.
    "episode.leave": "core",
}

FORK = "installation.fork"
SESSION_TYPES = ("session.start", "session.checkpoint", "session.end", "session.finalize")
LISTENING_TYPES = SESSION_TYPES + ("history.remove", "history.restore", "history.clear", "statistics.reset")
SESSION_END_REASONS = ("ended", "skipped", "switched", "removed", "suspended", "superseded")
# PROTOCOL §8: episode.leave.
LEAVE = "episode.leave"
LEAVE_REASONS = ("switched", "skipped", "ended")
# PROTOCOL §7.6.
QUEUE_TYPES = ("queue.add", "queue.restore", "queue.remove", "queue.move", "queue.reorder", "queue.clear")
ENQUEUE_LOCATIONS = ("back", "front", "after_current", "random")
QUEUE_REMOVE_REASONS = ("user", "download_deleted")
SORT_ORDERS = qs.SORT_ORDERS
DEFAULT_QUEUE_POLICY = {"enqueue_location": "back", "keep_sorted": False, "sort_order": "date_new_old"}
RESOLVED_ADD = "queue.resolved_add"
# PROTOCOL §7.6: the keys the server publishes for the replay's order (it replaces
# `queue.sort_result`, which is not a decision type).
LAYOUT = "queue.layout"
LAYOUT_SUBJECT = "queue"
# PROTOCOL §7.6: the gap a device resolved.
PLACE_KINDS = ("start", "after", "end")
# PROTOCOL §9.4: server decisions of the catalog.
CATALOG_REFRESH = "catalog.refresh"
ADMISSION = "discovery.admission"
MOVED = "podcast.moved"
# PROTOCOL §9.7: identity decisions.
GUID_CHANGE = "episode.guid_change"
MAPPING = "episode.occurrence_mapping"
EPISODE_MERGE = "episode.merge"
PODCAST_MERGE = "podcast.merge"
IDENTITY_DECISIONS = (GUID_CHANGE, MAPPING, EPISODE_MERGE, PODCAST_MERGE)
CATALOG_DECISIONS = (CATALOG_REFRESH, ADMISSION, MOVED) + IDENTITY_DECISIONS
# Subscription decisions (PROTOCOL §7.2), recoverable as decision arrivals (PROTOCOL §6.11).
ALIAS = "podcast.alias"
RESOLUTION = "podcast.resolution"
SUBSCRIPTION_DECISIONS = (ALIAS, RESOLUTION)
RESOLUTION_MODES = ("created", "joined", "opened", "stale")
# PROTOCOL §11.5 import decisions (PROTOCOL §7.6: the import is one replay step,
# at its completing decision, the manifest). The reference models what the replay needs
# (the podcasts, their episodes and the imported queue; play state, position and favorite
# for realism), not the statistics baselines (import_android.py is their oracle).
IMPORT_PODCAST = "import.podcast"
IMPORT_EPISODE = "import.episode"
IMPORT_MANIFEST = "import.manifest"
IMPORT_DECISIONS = (IMPORT_PODCAST, IMPORT_EPISODE, IMPORT_MANIFEST)
IMPORT_STATES = ("subscribed", "not_subscribed", "archived")
DECISION_TYPES = (RESOLVED_ADD, LAYOUT) + CATALOG_DECISIONS + SUBSCRIPTION_DECISIONS + IMPORT_DECISIONS
# PROTOCOL §6.11: identity decisions never split anything; equivalents are unified by a merge.
UNIFIED_BY = (ALIAS, RESOLUTION) + IDENTITY_DECISIONS
# PROTOCOL §9.7: two dates "more than 7 days apart" are different releases.
RELEASE_WINDOW_MS = 7 * 86400000
# PROTOCOL §9.7: within one node and scope, membership effects before position-only effects.
_POSITION_ONLY = frozenset(["q_move"])
# PROTOCOL §9.4 operations.
OBSERVE = "catalog.observe"
PROPOSE = "discovery.propose"
# PROTOCOL §9.4: the changed catalog records of a fetch, in parts of at most 200 items.
PART_ITEMS = 200
# PROTOCOL §9.4: refresh_status is published when its outcome class or error changes,
# and at most once a day otherwise.
STATUS_INTERVAL_MS = 86400000
FETCH_OUTCOMES = ("success", "failure", "re-parse")
PERMANENT_REDIRECTS = (301, 308)
# PROTOCOL §9.4 record values (the parser's Feed and Item fields), without `revision` and
# `fetch_time`.
FEED_FIELDS = ("title", "description", "author", "language", "link", "image_url", "type", "funding", "paged",
               "next_page_url")
ITEM_FIELDS = ("title", "description", "link", "pub_date", "image_url", "payment_link", "media", "chapters",
               "chapter_url", "transcript_url", "transcript_type", "social_interact_url")
# Android's default new-episodes action (PROTOCOL §9.4: `global` resolves through the account
# default, whose default is `inbox`).
DEFAULT_DISCOVERY_ACTION = "inbox"

APPLIED = "applied"
SUPERSEDED = "superseded"
NOT_APPLIED = "not_applied"
REJECTED = "rejected"

# Reason codes (PROTOCOL §7.8, §10.4).
R_INVALID_PAYLOAD = "invalid_payload"
R_STALE = "stale_subscribe"
R_CLOSED = "generation_closed"
R_UNKNOWN_PODCAST = "unknown_podcast"
R_UNKNOWN_GENERATION = "unknown_generation"
R_MISMATCH = "episode_podcast_mismatch"
R_ALIAS_CONFLICT = "alias_conflict"
R_CAPABILITY = "capability_not_declared"
# PROTOCOL §6.4 author-position rejection; PROTOCOL §6.10 replaces it by a derived
# author ("No refusal"), so the engine never reports it any more.
R_POSITION_TAKEN = "author_position_taken"
R_IMPOSSIBLE_HISTORY = "impossible_author_history"
R_NOT_SUBSCRIBED = "not_subscribed"  # archive/restore of a live preview
# PROTOCOL §8.
R_FOREIGN_SESSION = "foreign_session"
R_ORDINAL_CONFLICT = "session_ordinal_conflict"
R_SEALED = "session_sealed"
R_FINALIZED = "session_finalized"
R_ALREADY_RESTORED = "already_restored"
# The PROTOCOL §8 check-3/4 reasons: the fact is admitted but contributes nothing.
SESSION_REASONS = frozenset([R_FOREIGN_SESSION, R_ORDINAL_CONFLICT, R_SEALED, R_FINALIZED])
# PROTOCOL §7.6: an automatic queue add against an explicit membership decision.
R_EXPLICIT_STATE = "explicit_state"
# PROTOCOL §9.4: a proposal that is not admitted.
R_NOT_PLAYABLE = "not_playable"
R_INELIGIBLE = "ineligible"
R_ALREADY_ADMITTED = "already_admitted"
# PROTOCOL §9.7: a lifecycle fact naming a source generation
# joined into a survivor's live generation.
R_GENERATION_MERGED = "generation_merged"
OUTCOMES = (APPLIED, SUPERSEDED, NOT_APPLIED, REJECTED)

# PROTOCOL §6.10: D(X) = "a1:" + hex(SHA-256(L("author-v1", namespace, X.op_id))).
DERIVED_AUTHOR_PREFIX = "a1:"


def derived_author(namespace, op_id):
    return DERIVED_AUTHOR_PREFIX + c._sha_hex(c.length_prefixed(["author-v1", namespace, op_id]))


def author_coordinates(fact):
    """PROTOCOL §6.10 "well-formed author coordinates": (author_id, author_seq, previous_op_id or
    None), or None. CHOICE: an archived fact has them when its author_id is a UUID, its
    author_seq a safe integer >= 1 and its previous_op_id absent or a UUID other than its
    own op_id, whatever its other envelope violations (an envelope-rejected fact with
    sound coordinates holds a chain position too)."""
    if not isinstance(fact, dict):
        return None
    a, s = fact.get("author_id"), fact.get("author_seq")
    if not c.is_uuid(a) or not _is_plain_int(s) or not 1 <= s <= c.MAX_SAFE_INTEGER:
        return None
    if "previous_op_id" not in fact:
        return (a, s, None)
    p = fact["previous_op_id"]
    if not c.is_uuid(p) or p == fact.get("op_id"):
        return None
    return (a, s, p)

# Play-state contribution kinds (the register rule of PROTOCOL §7.1, with the kinds that make a
# play_state record exist). CHOICE (PROTOCOL §7.6): a queue add or restore touches
# play_state like a dismissal does (its `new` -> `unplayed` is conditional, fired or not).
_PLAY_KINDS = frozenset(["mark_played", "mark_unplayed", "reset_position", "restore_state", "dismiss", "admission",
                         "q_add", "q_restore"])
# Barriers against an automatic `new` (the listening barriers are in _is_barrier).
# Explicit queue adds (`queue.add` members and a play-start bundle's add, which come
# with a start anyway); CHOICE: a restore (Undo of a removal, an explicit re-add) too.
_BARRIER_KINDS = frozenset(["mark_played", "mark_unplayed", "reset_position", "restore_state", "dismiss",
                            "q_add", "q_restore"])
# Contribution kinds that make their node part of the queue membership/position
# frontier a decision observes (end and finalize count when they carry a queue removal).
_QUEUE_KINDS = frozenset(["q_add", "q_restore", "q_move", "q_remove", "q_clear", "q_auto", "q_import"])
# Contribution kinds of session facts (PROTOCOL §8).
_SESSION_KINDS = frozenset(["start", "checkpoint", "end", "finalize"])
_SESSION_KIND = {"session.start": "start", "session.checkpoint": "checkpoint", "session.end": "end",
                 "session.finalize": "finalize"}


def _is_barrier(ct):
    """PROTOCOL §8: a session start is always a barrier against automatic filing; a checkpoint or
    an end is one if it is a listening activity.

    CHOICE: a finalization and a history mark alone are not barriers (the table lists
    none), nor are history cuts. An `episode.leave` is never one (PROTOCOL §8)."""
    if ct.kind in _BARRIER_KINDS or ct.kind == "start":
        return True
    if ct.kind in ("checkpoint", "end"):
        return ct.data["activity"]
    return False


def _is_history_entry(ct):
    """PROTOCOL §8: listening activities (start, checkpoint and end saves with `activity`) and
    history marks (end, finalize and, PROTOCOL §8, leave with `history`) are the history
    entries."""
    k = ct.kind
    if k in ("start", "checkpoint"):
        return ct.data["activity"]
    if k == "end":
        return ct.data["activity"] or ct.data["history"]
    if k in ("finalize", "leave"):
        return ct.data["history"]
    return False


class RuleInputError(ValueError):
    """The arrival sequence is not something the server would have accepted."""


class _Invalid(Exception):
    """Payload (or type-specific target) schema violation: rejected / invalid_payload."""


# ---------------------------------------------------------------------------
# URL userinfo (PROTOCOL §6.1, "No credentials in any string")
# ---------------------------------------------------------------------------


def has_userinfo(url):
    """True if `url` has an authority with userinfo (an `@` before the end of the authority).

    Uses the authority rule of URL normalization v1 (PROTOCOL §3.3): after `scheme:` and
    `//`, the authority runs to the first `/`, `?` or `#`; userinfo is everything up to its
    last `@`.
    """
    if not isinstance(url, str):
        return False
    m = c._SCHEME_RE.match(url)
    if m is None:
        return False
    rest = url[m.end():]
    if not rest.startswith("//"):
        return False
    body = rest[2:]
    end = len(body)
    for ch in "/?#":
        k = body.find(ch, 0, end)
        if k != -1:
            end = k
    return "@" in body[:end]


# Embedded URLs in text. Text may be HTML, so the characters that shape a URL may be
# written as character references: the rule reads a decoded view of the text and
# removes the matching spans of the original.
_URL_START = re.compile("[A-Za-z][A-Za-z0-9+.\\-]*://|(?:^|(?<=[\x00-\x20\"'=(<>]))//")
_CHAR_REF = re.compile("&(?:#[xX]([0-9A-Fa-f]+);?|#([0-9]+);?|(commat|colon|sol);)")
_NAMED_REFS = {"commat": "@", "colon": ":", "sol": "/"}
_AUTHORITY_ENDS = frozenset("/?#<>\"")


def _decoded_view(text):
    """(view, spans): `text` with its numeric character references (`&#64;`, `&#x40;`,
    with or without `;`) and `&commat;`, `&colon;` and `&sol;` decoded, in one pass; and
    for each character of the view, the [start, end) of the text it comes from. A
    numeric reference that is not a Unicode scalar value, or is zero, stays as written."""
    view, spans, i = [], [], 0
    for m in _CHAR_REF.finditer(text):
        for k in range(i, m.start()):
            view.append(text[k])
            spans.append((k, k + 1))
        if m.group(3) is not None:
            ch = _NAMED_REFS[m.group(3)]
        else:
            digits = m.group(1) if m.group(1) is not None else m.group(2)
            # More digits than any code point needs (leading zeros aside) is no
            # scalar value; Python's int() also refuses thousands of digits.
            significant = digits.lstrip("0") or "0"
            if len(significant) > 8:
                ch = None
            else:
                cp = int(significant, 16) if m.group(1) is not None else int(significant)
                ch = chr(cp) if 0 < cp <= 0x10FFFF and not 0xD800 <= cp <= 0xDFFF else None
        if ch is None:
            for k in range(m.start(), m.end()):
                view.append(text[k])
                spans.append((k, k + 1))
        else:
            view.append(ch)
            spans.append((m.start(), m.end()))
        i = m.end()
    for k in range(i, len(text)):
        view.append(text[k])
        spans.append((k, k + 1))
    return "".join(view), spans


def strip_userinfo(text):
    """The server removes the userinfo of every URL embedded anywhere in a text value:
    `scheme://user:pass@host` becomes `scheme://host`.

    - A URL starts at `scheme://`, or at a scheme-relative `//` at the start of the text
      or after U+0000-U+0020, a quotation mark, an apostrophe, `=`, `(`, `<` or `>`.
      (Control characters count, so trimming a GUID afterwards cannot uncover a URL.)
    - Its authority runs to the first U+0000-U+0020, `/`, `?`, `#`, `<`, `>` or quotation
      mark, and also to the first apostrophe when the URL starts right after an
      apostrophe (a single-quoted attribute). Otherwise an apostrophe belongs to the
      authority, as RFC 3986 allows in userinfo.
    - Its userinfo is everything up to its last `@`.
    - The rule reads the text with character references decoded (`_decoded_view`), so
      `&#64;` is an `@`; the removed span covers the references it spans.    An apostrophe in the user name, and an `@` written as a character reference, do not
    escape the rule."""
    view, spans = _decoded_view(text)
    cuts = []
    pos = 0
    while True:
        m = _URL_START.search(view, pos)
        if m is None:
            break
        start = m.end()
        quoted = m.start() > 0 and view[m.start() - 1] == "'"
        end, at = start, -1
        while end < len(view):
            ch = view[end]
            if ord(ch) <= 0x20 or ch in _AUTHORITY_ENDS or (quoted and ch == "'"):
                break
            if ch == "@":
                at = end
            end += 1
        if at >= 0:
            cuts.append((spans[start][0], spans[at][1]))
        pos = max(end, m.end())
    if not cuts:
        return text
    out, i = [], 0
    for a, b in cuts:
        out.append(text[i:a])
        i = b
    out.append(text[i:])
    return "".join(out)


def _strip_all(v):
    if isinstance(v, str):
        return strip_userinfo(v)
    if isinstance(v, dict):
        return dict((k, _strip_all(x)) for k, x in v.items())
    if isinstance(v, list):
        return [_strip_all(x) for x in v]
    return v


def _all_strings(v):
    if isinstance(v, str):
        yield v
    elif isinstance(v, dict):
        for k, x in v.items():
            yield k
            for s in _all_strings(x):
                yield s
    elif isinstance(v, list):
        for x in v:
            for s in _all_strings(x):
                yield s


def _url_fields(fact):
    """URL-typed payload members of the known operation types (as far as they are present).

    CHOICE: "URL fields" are feed_url (subscribe, register_preview), url (change_url) and
    every EpisodeRef occurrence.enclosure_url.
    """
    p = fact.get("payload")
    if not isinstance(p, dict):
        return []
    t = fact.get("type")
    if t in (OBSERVE, PROPOSE):
        # PROTOCOL §9.4 "Every URL has its userinfo removed": the catalog payloads carry many URL
        # fields (links, images, media, chapters, funding, occurrences). CHOICE: every
        # string of the payload is checked as a URL (has_userinfo only fires for a string
        # that is itself a URL with userinfo).
        return list(_all_strings(p))
    out = []
    if t in ("podcast.subscribe", "podcast.register_preview"):
        out.append(p.get("feed_url"))
    elif t == "podcast.change_url":
        out.append(p.get("url"))
    refs = []
    if isinstance(p.get("episode"), dict):
        refs.append(p["episode"])
    if t in ("inbox.dismiss", "inbox.dismiss_all", "queue.remove") and isinstance(p.get("episodes"), list):
        refs.extend(r for r in p["episodes"] if isinstance(r, dict))
    if t == "queue.clear" and isinstance(p.get("observed"), list):
        refs.extend(r for r in p["observed"] if isinstance(r, dict))
    # Placements: queue.add and restore members (queue.move's payload is itself a
    # placement, above). PROTOCOL §7.6: every `place.after` anchor (a command's, a
    # play-start or discovery bundle's), a run move's `run` and a sort's `observed` and
    # `permutation` are EpisodeRefs too.
    if t in QUEUE_TYPES or t in ("session.start", PROPOSE):
        g = p.get("members")
        if isinstance(g, list):
            refs.extend(x["episode"] for x in g if isinstance(x, dict) and isinstance(x.get("episode"), dict))
        places = [p.get("place")]
        if isinstance(p.get("queue_bundle"), dict):
            places.append(p["queue_bundle"].get("place"))
        for pl in places:
            if isinstance(pl, dict) and isinstance(pl.get("after"), dict):
                refs.append(pl["after"])
        lists = [p.get("run")]
        if t == "queue.reorder":
            lists.append(p.get("observed"))
            if isinstance(p.get("sort"), dict):
                lists.append(p["sort"].get("permutation"))
        for g in lists:
            if isinstance(g, list):
                refs.extend(r for r in g if isinstance(r, dict))
    for r in refs:
        occ = r.get("occurrence")
        if isinstance(occ, dict):
            out.append(occ.get("enclosure_url"))
    return [u for u in out if isinstance(u, str)]


# ---------------------------------------------------------------------------
# Envelope (PROTOCOL §6.1 level, PROTOCOL §6.4), with PROTOCOL §6.4 codes
# ---------------------------------------------------------------------------

_FACT_REQUIRED = ("op_id", "author_id", "author_seq", "dependencies", "type", "type_version", "client_time",
                  "targets", "payload")
_FACT_OPTIONAL = ("previous_op_id",)
MAX_FACT_DEPTH = 32


def _depth(v):
    if isinstance(v, dict):
        return 1 + max((_depth(x) for x in v.values()), default=0)
    if isinstance(v, list):
        return 1 + max((_depth(x) for x in v), default=0)
    return 0


def _is_plain_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def unarchivable_reason(fact):
    """Why a fact cannot be archived at all (invalid_request, nothing consumed), or None."""
    if not isinstance(fact, dict):
        return "fact is not an object"
    try:
        c.canonical(fact)
    except c.ContractError as e:
        return "not strictly decodable protocol JSON: %s" % e
    if _depth(fact) > MAX_FACT_DEPTH:
        return "fact nests deeper than %d levels" % MAX_FACT_DEPTH
    if not c.is_uuid(fact.get("op_id")):
        return "op_id missing or not a UUID"
    return None


def envelope_reason(fact):
    """PROTOCOL §6.4 code for the first envelope violation, or None.

    CHOICE: when several apply, the first in the order the protocol lists the codes."""
    keys = set(fact)
    if keys - set(_FACT_REQUIRED) - set(_FACT_OPTIONAL):
        return "unknown_field"
    if set(_FACT_REQUIRED) - keys:
        return "missing_field"
    op = fact["op_id"]
    if not c.is_uuid(fact["author_id"]):
        return "invalid_author_id"
    if not _is_plain_int(fact["author_seq"]) or fact["author_seq"] < 1:
        return "invalid_author_seq"
    if "previous_op_id" in fact and (not c.is_uuid(fact["previous_op_id"]) or fact["previous_op_id"] == op):
        return "invalid_previous_op_id"
    deps = fact["dependencies"]
    if not isinstance(deps, list) or not all(c.is_uuid(d) and d != op for d in deps):
        return "invalid_dependencies"
    if any(_bkey(a) >= _bkey(b) for a, b in zip(deps, deps[1:])):
        return "unsorted_dependencies"
    if not isinstance(fact["type"], str):
        return "invalid_type"
    if not _is_plain_int(fact["type_version"]) or fact["type_version"] < 1:
        return "invalid_type_version"
    if not _is_plain_int(fact["client_time"]):
        return "invalid_client_time"
    targets = fact["targets"]
    if not isinstance(targets, list):
        return "invalid_targets"
    for t in targets:
        if (not isinstance(t, dict) or "entity" not in t or "id" not in t
                or set(t) - {"entity", "id", "generation"} or not all(isinstance(x, str) for x in t.values())):
            return "invalid_targets"
    tkeys = [(_bkey(t["entity"]), _bkey(t["id"]), _bkey(t.get("generation", ""))) for t in targets]
    if any(a >= b for a, b in zip(tkeys, tkeys[1:])):
        return "unsorted_targets"
    if not isinstance(fact["payload"], dict):
        return "invalid_payload"
    return None


# ---------------------------------------------------------------------------
# Payload schemas (PROTOCOL §7, type_version 1)
# ---------------------------------------------------------------------------


def _members(v, required, optional=(), what="payload"):
    if not isinstance(v, dict):
        raise _Invalid("%s is not an object" % what)
    missing = [k for k in required if k not in v]
    if missing:
        raise _Invalid("%s lacks %s" % (what, missing))
    extra = set(v) - set(required) - set(optional)
    if extra:
        raise _Invalid("%s has unknown members %s" % (what, sorted(extra)))


def _str(v, what):
    if not isinstance(v, str):
        raise _Invalid("%s is not a string" % what)


def _bool(v, what):
    if not isinstance(v, bool):
        raise _Invalid("%s is not a boolean" % what)


def _int(v, what, lo=None):
    if not isinstance(v, int) or isinstance(v, bool) or abs(v) > c.MAX_SAFE_INTEGER:
        raise _Invalid("%s is not a safe integer" % what)
    if lo is not None and v < lo:
        raise _Invalid("%s is below %d" % (what, lo))


def _enum(v, allowed, what):
    if not isinstance(v, str) or v not in allowed:
        raise _Invalid("%s is not one of %s" % (what, "|".join(allowed)))


def _uuid(v, what):
    if not c.is_uuid(v):
        raise _Invalid("%s is not a lowercase UUID" % what)


def _podcast_id(v, what):
    if not c.is_podcast_id(v):
        raise _Invalid("%s is not a podcast ID" % what)


def _generation_id(v, what):
    if not c.is_generation_id(v):
        raise _Invalid("%s is not a generation ID" % what)


def _observed(v):
    _members(v, ("podcast_id", "generation_id", "state"), (), "observed")
    _podcast_id(v["podcast_id"], "observed.podcast_id")
    _generation_id(v["generation_id"], "observed.generation_id")
    _enum(v["state"], SUBSCRIPTION_STATES, "observed.state")
    return dict(v)


def _episode_ref(v, what):
    _members(v, ("episode_id", "podcast_id", "generation_id", "occurrence"), (), what)
    if not c.is_episode_id(v["episode_id"]):
        raise _Invalid("%s.episode_id is not an episode ID" % what)
    _podcast_id(v["podcast_id"], what + ".podcast_id")
    _generation_id(v["generation_id"], what + ".generation_id")
    occ = v["occurrence"]
    w = what + ".occurrence"
    _members(occ, ("k", "v"), ("guid", "enclosure_url", "title", "publication_time"), w)
    # PROTOCOL §7.2: "for an e0: episode without GUID or enclosure, k is "none" and v is """ (an
    # episode without GUID and enclosure is a real Android item). CHOICE: as for the other kinds,
    # (k, v) is not checked against the episode ID.
    _enum(occ["k"], c.EPISODE_KINDS + ("none",), w + ".k")
    _str(occ["v"], w + ".v")
    for f in ("guid", "enclosure_url", "title"):
        if f in occ:
            _str(occ[f], "%s.%s" % (w, f))
    if "publication_time" in occ:
        _int(occ["publication_time"], w + ".publication_time")
    # CHOICE: the episode ID is not re-derived from (k, v): the protocol says it "is the
    # device's derivation" but gives no rejection rule, and PROTOCOL §3.3 rule 1 (known alias)
    # can legitimately give an ID that is not derived from this occurrence's (k, v).
    return {
        "episode_id": v["episode_id"],
        "podcast_id": v["podcast_id"],
        "generation_id": v["generation_id"],
        "occurrence": copy.deepcopy(occ),
    }


def _one_podcast_target(targets):
    if len(targets) != 1:
        raise _Invalid("needs exactly one podcast + generation target")
    t = targets[0]
    if t["entity"] != "podcast" or "generation" not in t:
        raise _Invalid("target must be {entity: podcast, id, generation}")
    _podcast_id(t["id"], "target id")
    _generation_id(t["generation"], "target generation")
    return t["id"], t["generation"]


def _episode_targets(targets, refs):
    want = sorted(
        ({"entity": "episode", "id": r["episode_id"], "generation": r["generation_id"]} for r in refs),
        key=lambda t: (t["id"].encode("utf-8"), t["generation"].encode("utf-8")),
    )
    if targets != want:
        raise _Invalid("targets must be exactly the episode + generation of every EpisodeRef")


def parse_payload(fact, account_id):
    """Validates the type-specific payload and targets; returns a parsed dict or raises _Invalid."""
    t = fact["type"]
    p = fact["payload"]
    targets = fact["targets"]
    if t in ("podcast.subscribe", "podcast.register_preview"):
        if t == "podcast.subscribe":
            _members(p, ("feed_url",), ("observed", "policy", "credentials_secret"))
        else:
            _members(p, ("feed_url",), ("observed",))
        _str(p["feed_url"], "feed_url")
        out = {"feed_url": p["feed_url"]}
        if "observed" in p:
            out["observed"] = _observed(p["observed"])
        if "policy" in p:
            _members(p["policy"], ("new_episodes_action", "keep_updated"), (), "policy")
            _enum(p["policy"]["new_episodes_action"], POLICY_ACTIONS, "policy.new_episodes_action")
            _bool(p["policy"]["keep_updated"], "policy.keep_updated")
            out["policy"] = dict(p["policy"])
        if "credentials_secret" in p:
            _uuid(p["credentials_secret"], "credentials_secret")
            out["credentials_secret"] = p["credentials_secret"]
        # "none, or the podcast the device knows". CHOICE: a generation member is allowed;
        # the target is not cross-checked against `observed` or the resolution (a replayed
        # creating operation may name a podcast the server maps elsewhere, PROTOCOL §3.3).
        if len(targets) > 1:
            raise _Invalid("subscribe names at most one podcast target")
        if targets:
            tg = targets[0]
            if tg["entity"] != "podcast":
                raise _Invalid("subscribe target must be a podcast")
            _podcast_id(tg["id"], "target id")
            if "generation" in tg:
                _generation_id(tg["generation"], "target generation")
        return out
    if t in ("podcast.archive", "podcast.restore", "podcast.unsubscribe"):
        _members(p, ())
        pid, gid = _one_podcast_target(targets)
        return {"podcast_id": pid, "generation_id": gid}
    if t == "podcast.set_policy":
        _members(p, (), ("new_episodes_action", "keep_updated"))
        if not p:
            raise _Invalid("set_policy needs at least one field")
        if "new_episodes_action" in p:
            _enum(p["new_episodes_action"], POLICY_ACTIONS, "new_episodes_action")
        if "keep_updated" in p:
            _bool(p["keep_updated"], "keep_updated")
        pid, gid = _one_podcast_target(targets)
        return {"podcast_id": pid, "generation_id": gid, "fields": dict(p)}
    if t == "podcast.set_credentials":
        if "secret_id" in p:
            _members(p, ("secret_id",))
            _uuid(p["secret_id"], "secret_id")
            secret = p["secret_id"]
        else:
            _members(p, ("clear",))
            if p["clear"] is not True:
                raise _Invalid("clear must be true")
            secret = None
        pid, gid = _one_podcast_target(targets)
        return {"podcast_id": pid, "generation_id": gid, "secret": secret}
    if t == "podcast.change_url":
        _members(p, ("url",))
        _str(p["url"], "url")
        pid, gid = _one_podcast_target(targets)
        return {"podcast_id": pid, "generation_id": gid, "url": p["url"]}
    if t in ("episode.mark_played", "episode.mark_unplayed", "episode.reset_position", "episode.restore_state", "favorite.set"):
        extra = {
            "episode.mark_played": ("reset_position",),
            "episode.mark_unplayed": (),
            "episode.reset_position": (),
            "episode.restore_state": ("play_state", "position"),
            "favorite.set": ("on",),
        }[t]
        _members(p, ("episode",) + extra)
        ref = _episode_ref(p["episode"], "episode")
        out = {"refs": [ref]}
        if t == "episode.mark_played":
            _bool(p["reset_position"], "reset_position")
            out["reset"] = p["reset_position"]
        elif t == "episode.restore_state":
            _enum(p["play_state"], PLAY_STATES, "play_state")
            _int(p["position"], "position", 0)
            out["play_state"] = p["play_state"]
            out["position"] = p["position"]
        elif t == "favorite.set":
            _bool(p["on"], "on")
            out["on"] = p["on"]
        _episode_targets(targets, [ref])
        return out
    if t in ("inbox.dismiss", "inbox.dismiss_all"):
        _members(p, ("episodes",))
        if not isinstance(p["episodes"], list):
            raise _Invalid("episodes is not an array")
        # CHOICE: an empty bulk is invalid (it names no entity and could have no effect).
        if not p["episodes"]:
            raise _Invalid("episodes is empty")
        refs = [_episode_ref(r, "episodes[%d]" % i) for i, r in enumerate(p["episodes"])]
        # "sorted by episode ID": strictly ascending bytewise, so one member per episode.
        for a, b in zip(refs, refs[1:]):
            if a["episode_id"].encode("utf-8") >= b["episode_id"].encode("utf-8"):
                raise _Invalid("episodes are not sorted by episode ID without duplicates")
        _episode_targets(targets, refs)
        return {"refs": refs}
    if t == "account.set_discovery_policy":
        _members(p, ("new_episodes_action",))
        _enum(p["new_episodes_action"], DISCOVERY_ACTIONS, "new_episodes_action")
        if targets != [{"entity": "account", "id": account_id}]:
            raise _Invalid("target must be exactly {entity: account, id: <account>}")
        return {"action": p["new_episodes_action"]}
    if t in SESSION_TYPES:
        return _parse_session(t, p, targets)
    if t in QUEUE_TYPES:
        return _parse_queue(t, p, targets, account_id)
    if t == OBSERVE:
        return _parse_observe(p, targets)
    if t == PROPOSE:
        return _parse_propose(p, targets)
    if t == "account.set_queue_policy":
        _members(p, (), ("enqueue_location", "sorting"))
        if not p:
            raise _Invalid("set_queue_policy needs at least one field")
        out = {}
        if "enqueue_location" in p:
            _enum(p["enqueue_location"], ENQUEUE_LOCATIONS, "enqueue_location")
            out["enqueue_location"] = p["enqueue_location"]
        if "sorting" in p:
            s = p["sorting"]
            _members(s, ("keep_sorted", "sort_order"), (), "sorting")
            _bool(s["keep_sorted"], "sorting.keep_sorted")
            _enum(s["sort_order"], SORT_ORDERS, "sorting.sort_order")
            if s["keep_sorted"] and s["sort_order"] == "random":
                raise _Invalid("keep_sorted with sort_order random (Android turns keep-sorted off for random)")
            out["sorting"] = {"keep_sorted": s["keep_sorted"], "sort_order": s["sort_order"]}
        if targets != [{"entity": "account", "id": account_id}]:
            raise _Invalid("target must be exactly {entity: account, id: <account>}")
        return out
    if t == LEAVE:
        return _parse_leave(p, targets)
    if t == "history.remove":
        _members(p, ("episode",))
        ref = _episode_ref(p["episode"], "episode")
        _episode_targets(targets, [ref])
        return {"refs": [ref]}
    if t == "history.restore":
        _members(p, ("episode", "removal", "time"))
        ref = _episode_ref(p["episode"], "episode")
        _uuid(p["removal"], "removal")
        # CHOICE: `time` (evidence only, PROTOCOL §8) is any safe integer instant.
        _int(p["time"], "time")
        _episode_targets(targets, [ref])
        return {"refs": [ref], "removal": p["removal"], "time": p["time"]}
    if t in ("history.clear", "statistics.reset"):
        _members(p, ())
        # CHOICE: "account-level ... and have no targets" (PROTOCOL §8): any target is invalid.
        if targets:
            raise _Invalid("%s has no targets" % t)
        return {}
    if t == FORK:
        # PROTOCOL §6.10: {author, head}. `author` is the wire author (an installation UUID) and
        # `head` a fact (an op_id). CHOICE: the fork names no target (it changes no
        # record; its effect is the lineage), so any target is invalid.
        _members(p, ("author", "head"))
        _uuid(p["author"], "author")
        _uuid(p["head"], "head")
        if targets:
            raise _Invalid("installation.fork has no targets")
        return {"author": p["author"], "head": p["head"]}
    raise AssertionError("unhandled type %s" % t)


def _parse_leave(p, targets):
    """PROTOCOL §8, `episode.leave` (type_version 1): {episode, transition_id,
    reason: switched | skipped | ended, position, observed_claim?, completed, threshold_ms,
    remove_from_queue, history}; targets: the episode and its generation. `transition_id`
    is a UUID; `observed_claim`, when present, a node ID (a UUID). CHOICE: `position` and
    `threshold_ms` are contract-v1 durations (integer ms >= 0), as a session end's; no
    consistency is required between `reason`, `position`, `completed`, `threshold_ms` and
    `remove_from_queue` (the device decides); `position` and `reason` are evidence only."""
    _members(p, ("episode", "transition_id", "reason", "position", "completed", "threshold_ms", "remove_from_queue",
                 "history"), ("observed_claim",))
    ref = _episode_ref(p["episode"], "episode")
    _uuid(p["transition_id"], "transition_id")
    _enum(p["reason"], LEAVE_REASONS, "reason")
    _int(p["position"], "position", 0)
    if "observed_claim" in p:
        _uuid(p["observed_claim"], "observed_claim")
    _bool(p["completed"], "completed")
    _int(p["threshold_ms"], "threshold_ms", 0)
    _bool(p["remove_from_queue"], "remove_from_queue")
    _bool(p["history"], "history")
    _episode_targets(targets, [ref])
    return {"refs": [ref], "transition_id": p["transition_id"], "reason": p["reason"], "position": p["position"],
            "observed_claim": p.get("observed_claim"), "completed": p["completed"], "threshold_ms": p["threshold_ms"],
            "remove_from_queue": p["remove_from_queue"], "history": p["history"]}


# PROTOCOL §8 payload members per session type: (required, optional); PROTOCOL §6.10 adds the start's
# `baseline` and the exports' `save`.
_SESSION_MEMBERS = {
    "session.start": (("episode", "session_id", "start_position", "activity"),
                      ("observed_claim", "duration", "queue_bundle", "baseline")),
    "session.checkpoint": (("episode", "session_id", "ordinal", "position", "contribution", "activity"),
                           ("duration", "save", "claim", "observed_claim")),
    "session.end": (("episode", "session_id", "ordinal", "position", "contribution", "activity", "reason",
                     "completed", "threshold_ms", "remove_from_queue", "history"), ("duration", "save")),
    "session.finalize": (("episode", "session_id", "ordinal", "claim_id", "completed", "threshold_ms",
                          "remove_from_queue", "history"), ()),
}


def _save_identity(v):
    """PROTOCOL §6.10: `save: {id, index, first?, baseline, passive}`.

    CHOICE: `id`, `first` and `baseline` are UUIDs (save and baseline identities are random
    IDs), `index` (the session's save counter) is a safe integer >= 1; no relation between
    the members is required (`first` may equal `id`: an export covering exactly one new
    save). CHOICE: `save` (and the start's `baseline`) are optional, so that facts without save identities
    stay valid; how a session without them folds is described in Engine._session_plan."""
    _members(v, ("id", "index", "baseline", "passive"), ("first",), "save")
    _uuid(v["id"], "save.id")
    _int(v["index"], "save.index", 1)
    _uuid(v["baseline"], "save.baseline")
    _bool(v["passive"], "save.passive")
    if "first" in v:
        _uuid(v["first"], "save.first")
    return dict(v)


def _parse_session(t, p, targets):
    """PROTOCOL §8. Positions, contributions, thresholds and durations are contract-v1 durations
    (integer ms >= 0); ordinals are >= 1.

    CHOICE: `observed_claim` must be a UUID (a claim is a node: an op_id) and has no effect;
    `duration` >= 0 is recorded as reported; no consistency is required between `reason`,
    `completed`, `threshold_ms` and positions (completion is the originating device's
    decision).

    PROTOCOL §7.6: `queue_bundle` is `{key, position_id,
    place}` (all three required): the started episode's add at the gap the device resolved
    and its key prediction; keep-sorted `moves` are invalid. The target is the started
    episode (the place's anchor is context, not a target)."""
    required, optional = _SESSION_MEMBERS[t]
    _members(p, required, optional)
    ref = _episode_ref(p["episode"], "episode")
    _uuid(p["session_id"], "session_id")
    out = {"refs": [ref], "session_id": p["session_id"]}
    target_refs = [ref]
    if t == "session.start":
        _int(p["start_position"], "start_position", 0)
        _bool(p["activity"], "activity")
        out["position"] = p["start_position"]
        out["activity"] = p["activity"]
        if "baseline" in p:
            _uuid(p["baseline"], "baseline")
            out["baseline"] = p["baseline"]
        if "observed_claim" in p:
            _uuid(p["observed_claim"], "observed_claim")
            out["observed_claim"] = p["observed_claim"]
        if "queue_bundle" in p:
            qb = p["queue_bundle"]
            _members(qb, ("key", "position_id", "place"), (), "queue_bundle")
            _key_pid(qb["key"], qb["position_id"], "queue_bundle")
            out["bundle"] = {"key": qb["key"], "pid": qb["position_id"], "place": _place(qb["place"], "queue_bundle.place")}
    else:
        _int(p["ordinal"], "ordinal", 1)
        out["ordinal"] = p["ordinal"]
    if t in ("session.checkpoint", "session.end"):
        _int(p["position"], "position", 0)
        _int(p["contribution"], "contribution", 0)
        _bool(p["activity"], "activity")
        out.update(position=p["position"], contribution=p["contribution"], activity=p["activity"])
        if "save" in p:
            out["save"] = _save_identity(p["save"])
    if t == "session.checkpoint":
        # PROTOCOL §8.1: `claim` (a strict boolean, default false) marks an intentional resume;
        # `observed_claim` (UUID evidence, never a precondition) is allowed only with it. A
        # claiming checkpoint is listening activity and covers a new, non-passive save, so it
        # carries `save` with `first` and `passive: false` (the save is the claim's evidence;
        # non-claiming exports keep `save` optional). Whether that save is new on its writing
        # path is checked at acceptance (Engine._claim_rejection).
        claim = p.get("claim", False)
        _bool(claim, "claim")
        if "observed_claim" in p:
            if claim is not True:
                raise _Invalid("observed_claim is allowed only with claim: true")
            _uuid(p["observed_claim"], "observed_claim")
        if claim:
            if p["activity"] is not True:
                raise _Invalid("a claiming checkpoint is listening activity")
            sv = out.get("save")
            if sv is None or sv["passive"] or "first" not in sv:
                raise _Invalid("a claiming checkpoint covers a new, non-passive save (save with first, not passive)")
        out["claim"] = claim
    if t == "session.end":
        _enum(p["reason"], SESSION_END_REASONS, "reason")
        out["reason"] = p["reason"]
    if t == "session.finalize":
        _uuid(p["claim_id"], "claim_id")
        out["claim_id"] = p["claim_id"]
    if t in ("session.end", "session.finalize"):
        _bool(p["completed"], "completed")
        _int(p["threshold_ms"], "threshold_ms", 0)
        _bool(p["remove_from_queue"], "remove_from_queue")
        _bool(p["history"], "history")
        out.update(completed=p["completed"], threshold_ms=p["threshold_ms"],
                   remove_from_queue=p["remove_from_queue"], history=p["history"])
    if "duration" in p:
        _int(p["duration"], "duration", 0)
        out["duration"] = p["duration"]
    _episode_targets(targets, target_refs)
    return out


# ---------------------------------------------------------------------------
# PROTOCOL §7.6 payloads
# ---------------------------------------------------------------------------


def _key_pid(key, pid, what):
    """PROTOCOL §7.6: a key parses under contract-v1 (strictly between the sentinels, canonical
    wire form) and carries the residue of its position ID: m mod 2^129 = 2U + 1.

    CHOICE: any lowercase UUID is a position ID (no version check)."""
    _uuid(pid, what + ".position_id")
    if not isinstance(key, str):
        raise _Invalid("%s.key is not a string" % what)
    try:
        _, m = c.parse_queue_key(key)
    except c.ContractError as e:
        raise _Invalid("%s.key: %s" % (what, e))
    u = int.from_bytes(c.uuid_to_u(pid), "big")
    if m % (1 << 129) != 2 * u + 1:
        raise _Invalid("%s.key does not carry the residue of its position_id" % what)


def _placement(v, what):
    _members(v, ("episode", "key", "position_id"), (), what)
    ref = _episode_ref(v["episode"], what + ".episode")
    _key_pid(v["key"], v["position_id"], what)
    return {"ref": ref, "key": v["key"], "pid": v["position_id"]}


def _placements(v, what, nonempty):
    if not isinstance(v, list):
        raise _Invalid("%s is not an array" % what)
    if nonempty and not v:
        raise _Invalid("%s is empty" % what)
    return [_placement(x, "%s[%d]" % (what, i)) for i, x in enumerate(v)]


def _unique_episodes(refs):
    ids = [r["episode_id"] for r in refs]
    if len(set(ids)) != len(ids):
        raise _Invalid("an episode may appear once per fact")


def _sorted_refs(v, what):
    """A non-empty array of EpisodeRefs sorted by episode ID without duplicates (as
    PROTOCOL §7.2 bulk dismissals). CHOICE: queue.remove's `episodes` and queue.clear's
    `observed` follow this rule, and neither may be empty."""
    if not isinstance(v, list):
        raise _Invalid("%s is not an array" % what)
    if not v:
        raise _Invalid("%s is empty" % what)
    refs = [_episode_ref(r, "%s[%d]" % (what, i)) for i, r in enumerate(v)]
    for a, b in zip(refs, refs[1:]):
        if a["episode_id"].encode("utf-8") >= b["episode_id"].encode("utf-8"):
            raise _Invalid("%s are not sorted by episode ID without duplicates" % what)
    return refs


def _decision_member(m, what, values):
    want = ("episode_id", "generation_id", "key", "position_id") + (("values",) if values else ())
    _members(m, want, (), what)
    if not c.is_episode_id(m["episode_id"]):
        raise _Invalid("%s.episode_id is not an episode ID" % what)
    _generation_id(m["generation_id"], what + ".generation_id")
    _key_pid(m["key"], m["position_id"], what)
    if values:
        v = m["values"]
        _members(v, qs.VALUE_FIELDS, (), what + ".values")
        for f in ("title", "podcast_id", "podcast_title"):
            _str(v[f], "%s.values.%s" % (what, f))
        for f in ("publication_time", "duration"):
            _int(v[f], "%s.values.%s" % (what, f))


def _status_value(v, what):
    _members(v, ("last_attempt", "last_success", "error", "next_attempt"), (), what)
    for f in ("last_attempt", "last_success", "next_attempt"):
        if v[f] is not None:
            _int(v[f], "%s.%s" % (what, f))
    if v["error"] is not None:
        _members(v["error"], ("label", "code", "detail"), (), what + ".error")
        _str(v["error"]["label"], what + ".error.label")
        if v["error"]["code"] is not None:
            _int(v["error"]["code"], what + ".error.code")
        _opt_str(v["error"]["detail"], what + ".error.detail")


def _check_catalog_decision_body(obj):
    """Version-1 schemas of the catalog decisions, as this reference writes them (CHOICE: the protocol names their subjects, dependencies and roles, not their members):

    * `catalog.refresh`, subject `refresh:<P>:<refresh id>:<part>`: inputs {podcast_id,
      refresh_id, part, outcome (success|failure|re-parse), generation_id (captured at
      the fetch's start; null for a re-parse), fetch_time}; result {status? (part 0 of a
      success or failure), population? (true, part 0), feed? ({base, fields}, part 0),
      seen? (the sorted episodes whose server history changed without a written record,
      in the first part written after the items are derived: part 0, or part 1 when part 0
      went first for identity evidence; absent when empty), items: [{episode_id, occurrence,
      base, fields}]}.
    * `discovery.admission`, subject `admission:<G>:<E>`: inputs {found_by (the proposal,
      or the refresh part that found the episode), enqueue_location? (present exactly when
      the server resolved the queue placement)}; result {episode_id, generation_id,
      action, proposal_id (null for the server's own), source (the proposing installation,
      or "server"), key?, position_id?, place?, values? (present exactly for the action
      queue; PROTOCOL §7.6: the filing's resolved gap and the canonical sort
      values the server recorded)}.
    * `podcast.moved`, subject `moved:<G>:<refresh id>`: inputs {podcast_id,
      generation_id, refresh_id}; result {refresh_url (or null), aliases: [{url,
      normalized, source (redirect|new_feed_url)}]}."""
    inp, res, t, subj = obj["inputs"], obj["result"], obj["decision_type"], obj["subject"]
    if t == CATALOG_REFRESH:
        _members(inp, ("podcast_id", "refresh_id", "part", "outcome", "generation_id", "fetch_time"), (), "inputs")
        _podcast_id(inp["podcast_id"], "inputs.podcast_id")
        _uuid(inp["refresh_id"], "inputs.refresh_id")
        _int(inp["part"], "inputs.part", 0)
        _enum(inp["outcome"], FETCH_OUTCOMES, "inputs.outcome")
        if inp["generation_id"] is not None:
            _generation_id(inp["generation_id"], "inputs.generation_id")
        _int(inp["fetch_time"], "inputs.fetch_time")
        if subj != "refresh:%s:%s:%d" % (inp["podcast_id"], inp["refresh_id"], inp["part"]):
            raise _Invalid("subject must be refresh:<podcast>:<refresh id>:<part>")
        _members(res, ("items",), ("status", "population", "feed", "seen"), "result")
        if ("status" in res or "population" in res or "feed" in res) and inp["part"] != 0:
            raise _Invalid("status, population and feed belong to part 0")
        if "seen" in res:
            if (not isinstance(res["seen"], list) or not res["seen"] or not all(c.is_episode_id(e) for e in res["seen"])
                    or [_bkey(e) for e in res["seen"]] != sorted(set(_bkey(e) for e in res["seen"]))):
                raise _Invalid("result.seen is a non-empty sorted array of distinct episode IDs")
        if "status" in res:
            _status_value(res["status"], "result.status")
        if "population" in res and res["population"] is not True:
            raise _Invalid("result.population is true when present")
        if "feed" in res:
            _members(res["feed"], ("base", "fields"), (), "result.feed")
            _base(res["feed"]["base"], "result.feed.base")
            feed_fields(res["feed"]["fields"], "result.feed.fields")
        if not isinstance(res["items"], list):
            raise _Invalid("result.items is not an array")
        for i, x in enumerate(res["items"]):
            w = "result.items[%d]" % i
            _members(x, ("episode_id", "occurrence", "base", "fields"), (), w)
            if not c.is_episode_id(x["episode_id"]):
                raise _Invalid("%s.episode_id is not an episode ID" % w)
            _members(x["occurrence"], ("k", "v"), ("guid", "enclosure_url", "title", "publication_time"), w + ".occurrence")
            _base(x["base"], w + ".base")
            item_fields(x["fields"], w + ".fields")
        if len(set(x["episode_id"] for x in res["items"])) != len(res["items"]):
            raise _Invalid("an episode appears once per part")
    elif t == ADMISSION:
        _members(inp, ("found_by",), ("enqueue_location",), "inputs")
        _uuid(inp["found_by"], "inputs.found_by")
        _members(res, ("episode_id", "generation_id", "action", "proposal_id", "source"),
                 ("key", "position_id", "place", "values"), "result")
        if not c.is_episode_id(res["episode_id"]):
            raise _Invalid("result.episode_id is not an episode ID")
        _generation_id(res["generation_id"], "result.generation_id")
        _enum(res["action"], DISCOVERY_ACTIONS, "result.action")
        if res["proposal_id"] is not None:
            _uuid(res["proposal_id"], "result.proposal_id")
            if res["proposal_id"] != inp["found_by"]:
                raise _Invalid("a device admission is found by its proposal")
            _uuid(res["source"], "result.source")
        elif res["source"] != "server":
            raise _Invalid("an admission without proposal is the server's")
        if any((f in res) != (res["action"] == "queue") for f in ("key", "position_id", "place", "values")):
            raise _Invalid("key, position_id, place and values are present exactly for the action queue")
        if "key" in res:
            _key_pid(res["key"], res["position_id"], "result")
            _decision_place(res["place"], "result.place")
            _check_values(res["values"], "result.values")
        if "enqueue_location" in inp:
            _enum(inp["enqueue_location"], ENQUEUE_LOCATIONS, "inputs.enqueue_location")
            if res["action"] != "queue":
                raise _Invalid("a resolved placement needs the action queue")
        if subj != "admission:%s:%s" % (res["generation_id"], res["episode_id"]):
            raise _Invalid("subject must be admission:<generation>:<episode>")
    else:
        _members(inp, ("podcast_id", "generation_id", "refresh_id"), (), "inputs")
        _podcast_id(inp["podcast_id"], "inputs.podcast_id")
        _generation_id(inp["generation_id"], "inputs.generation_id")
        _uuid(inp["refresh_id"], "inputs.refresh_id")
        if subj != "moved:%s:%s" % (inp["generation_id"], inp["refresh_id"]):
            raise _Invalid("subject must be moved:<generation>:<refresh id>")
        _members(res, ("refresh_url", "aliases"), (), "result")
        _opt_str(res["refresh_url"], "result.refresh_url")
        if not isinstance(res["aliases"], list):
            raise _Invalid("result.aliases is not an array")
        for i, a in enumerate(res["aliases"]):
            w = "result.aliases[%d]" % i
            _members(a, ("url", "normalized", "source"), (), w)
            _str(a["url"], w + ".url")
            _enum(a["source"], ("redirect", "new_feed_url"), w + ".source")
            try:
                norm = c.normalize_url(a["url"])
            except c.ContractError as e:
                raise _Invalid("%s.url: %s" % (w, e))
            if a["normalized"] != norm:
                raise _Invalid("%s.normalized is not the URL's normalization" % w)


def _check_identity_decision_body(obj):
    """Version-1 schemas of the identity decisions as this reference writes them (CHOICE: the
    lead named their types and subjects; the bodies are the reference's):

    * `episode.guid_change`, subject `guid:<P>:<guid>`: inputs {podcast_id, refresh_id,
      kind (rss-guid|atom-id: the new GUID's identity kind)}; result {guid, episode_id
      (the old root)}. Dependencies: the podcast's creating fact.
    * `episode.occurrence_mapping`, subject `occurrence:<P>:<guid>:<episode>`: inputs
      {podcast_id, guid, found_by (the observing fact, or null for a server fetch)};
      result {episode_id, original (the release keeping the original ID), publication_time,
      enclosures (normalized, first first), allocated (an `e0:` ID)}. Dependencies: the
      observing fact, or the podcast's creating fact. An enclosure addition to an
      existing release has subject
      `occurrence:<P>:<guid>:<episode>:<enclosure>`, the same inputs and dependencies,
      and result {episode_id (the release), enclosure (normalized)}.
    * `episode.merge`, subject `merge:<from>`: inputs {podcast_id (the survivor's), cause
      (guid_change, or recovered), by (the guid_change decision; for a unification,
      the effective decision it unifies with)}; result {from, into}. Dependencies: the
      guid_change decision (a unification: both competing decisions).
    * `podcast.merge`, subject `podcast-merge:<source>`: inputs {source, into, refresh_id,
      url (the normalized URL the fetch landed on)}; result {source, into, aliases
      (normalized, moved), generations: [{generation_id, mode (joined|rehomed|closed),
      into (the survivor generation, or null)}], episodes: [{from, to}], properties
      (survivor|source)}. Dependencies: the triggering fetch's part 0, both podcasts'
      creating facts and the opening facts of the joined or rehomed live generations. Recovery's
      unification merge has inputs {source, into, cause: "recovered"} and no part 0."""
    inp, res, t, subj = obj["inputs"], obj["result"], obj["decision_type"], obj["subject"]
    if t == GUID_CHANGE:
        _members(inp, ("podcast_id", "refresh_id", "kind"), (), "inputs")
        _podcast_id(inp["podcast_id"], "inputs.podcast_id")
        _uuid(inp["refresh_id"], "inputs.refresh_id")
        _enum(inp["kind"], ("rss-guid", "atom-id"), "inputs.kind")
        _members(res, ("guid", "episode_id"), (), "result")
        _str(res["guid"], "result.guid")
        if not c.is_episode_id(res["episode_id"]):
            raise _Invalid("result.episode_id is not an episode ID")
        if subj != "guid:%s:%s" % (inp["podcast_id"], res["guid"]):
            raise _Invalid("subject must be guid:<podcast>:<guid>")
    elif t == MAPPING:
        _members(inp, ("podcast_id", "guid", "found_by"), (), "inputs")
        _podcast_id(inp["podcast_id"], "inputs.podcast_id")
        _str(inp["guid"], "inputs.guid")
        if inp["found_by"] is not None:
            _uuid(inp["found_by"], "inputs.found_by")
        if "enclosure" in res:
            # An enclosure addition to an existing release.
            _members(res, ("episode_id", "enclosure"), (), "result")
            if not c.is_episode_id(res["episode_id"]):
                raise _Invalid("result.episode_id is not an episode ID")
            _str(res["enclosure"], "result.enclosure")
            if subj != "occurrence:%s:%s:%s:%s" % (inp["podcast_id"], inp["guid"], res["episode_id"], res["enclosure"]):
                raise _Invalid("subject must be occurrence:<podcast>:<guid>:<episode>:<enclosure>")
            return
        _members(res, ("episode_id", "original", "publication_time", "enclosures", "allocated"), (), "result")
        if not c.is_episode_id(res["episode_id"]):
            raise _Invalid("result.episode_id is not an episode ID")
        _bool(res["original"], "result.original")
        _bool(res["allocated"], "result.allocated")
        if res["publication_time"] is not None:
            _int(res["publication_time"], "result.publication_time")
        if not isinstance(res["enclosures"], list) or not all(isinstance(u, str) for u in res["enclosures"]):
            raise _Invalid("result.enclosures is not an array of strings")
        if not res["original"] and not res["enclosures"]:
            raise _Invalid("a later release has an enclosure")
        if subj != "occurrence:%s:%s:%s" % (inp["podcast_id"], inp["guid"], res["episode_id"]):
            raise _Invalid("subject must be occurrence:<podcast>:<guid>:<episode>")
    elif t == EPISODE_MERGE:
        _members(inp, ("podcast_id", "cause", "by"), (), "inputs")
        _podcast_id(inp["podcast_id"], "inputs.podcast_id")
        _enum(inp["cause"], ("guid_change", "recovered"), "inputs.cause")
        _uuid(inp["by"], "inputs.by")
        _members(res, ("from", "into"), (), "result")
        for f in ("from", "into"):
            if not c.is_episode_id(res[f]):
                raise _Invalid("result.%s is not an episode ID" % f)
        if subj != "merge:%s" % res["from"] or res["from"] == res["into"]:
            raise _Invalid("subject must be merge:<from>, into another episode")
    else:
        if "cause" in inp:
            # PROTOCOL §6.11 (CHOICE): a unification merge names its cause instead of a fetch.
            _members(inp, ("source", "into", "cause"), (), "inputs")
            _enum(inp["cause"], ("recovered",), "inputs.cause")
        else:
            _members(inp, ("source", "into", "refresh_id", "url"), (), "inputs")
            _uuid(inp["refresh_id"], "inputs.refresh_id")
            _str(inp["url"], "inputs.url")
        _members(res, ("source", "into", "aliases", "generations", "episodes", "properties"), (), "result")
        for part in (inp, res):
            _podcast_id(part["source"], "source")
            _podcast_id(part["into"], "into")
        if (inp["source"], inp["into"]) != (res["source"], res["into"]) or res["source"] == res["into"]:
            raise _Invalid("inputs and result name the same two podcasts")
        _enum(res["properties"], ("survivor", "source"), "result.properties")
        if not isinstance(res["aliases"], list) or not all(isinstance(a, str) for a in res["aliases"]):
            raise _Invalid("result.aliases is not an array of URLs")
        if not isinstance(res["generations"], list) or not isinstance(res["episodes"], list):
            raise _Invalid("result.generations and result.episodes are arrays")
        for i, g in enumerate(res["generations"]):
            _members(g, ("generation_id", "mode", "into"), (), "result.generations[%d]" % i)
            _generation_id(g["generation_id"], "generation_id")
            _enum(g["mode"], ("joined", "rehomed", "closed"), "mode")
            if (g["into"] is None) != (g["mode"] == "closed"):
                raise _Invalid("a joined or rehomed generation names where it goes")
            if g["into"] is not None:
                _generation_id(g["into"], "into")
        for i, m in enumerate(res["episodes"]):
            _members(m, ("from", "to"), (), "result.episodes[%d]" % i)
            if not c.is_episode_id(m["from"]) or not c.is_episode_id(m["to"]):
                raise _Invalid("episode mappings name episode IDs")
        if subj != "podcast-merge:%s" % res["source"]:
            raise _Invalid("subject must be podcast-merge:<source>")


def _check_subscription_decision_body(obj):
    """PROTOCOL §7.2 subscription decisions as the server makes them (recoverable):

    * `podcast.alias`, subject `alias:<normalized feed URL>`: inputs {}; result
      {podcast_id}. Dependencies: the subscribe or preview registration that created the
      podcast.
    * `podcast.resolution`, subject the fact's op_id: inputs {}; result {podcast_id, mode
      (created|joined|opened|stale), generation_id (absent only for a stale resolution of
      a podcast without generations)}. Dependencies: the fact."""
    inp, res, t, subj, deps = obj["inputs"], obj["result"], obj["decision_type"], obj["subject"], obj["dependencies"]
    if inp != {}:
        raise _Invalid("inputs is {}")
    if t == ALIAS:
        _members(res, ("podcast_id",), (), "result")
        _podcast_id(res["podcast_id"], "result.podcast_id")
        if not isinstance(subj, str) or not subj.startswith("alias:") or len(deps) != 1:
            raise _Invalid("subject must be alias:<normalized URL>, with the creating fact as dependency")
        return
    _members(res, ("podcast_id", "mode"), ("generation_id",), "result")
    _podcast_id(res["podcast_id"], "result.podcast_id")
    _enum(res["mode"], RESOLUTION_MODES, "result.mode")
    if "generation_id" in res:
        _generation_id(res["generation_id"], "result.generation_id")
    elif res["mode"] != "stale":
        raise _Invalid("result.generation_id is present unless the resolution is stale")
    if deps != [subj]:
        raise _Invalid("a resolution's subject is its fact, its only dependency")


def _decision_place(v, what):
    """A server-resolved `place` (PROTOCOL §7.6: server-resolved adds and server
    filings carry it in the decision). CHOICE: an `after` anchor is the server's own
    reference to a queued episode, `{episode_id, generation_id}`."""
    if not isinstance(v, dict) or "kind" not in v:
        raise _Invalid("%s is not a place" % what)
    _enum(v["kind"], PLACE_KINDS, what + ".kind")
    if v["kind"] == "after":
        _members(v, ("kind", "after"), (), what)
        _members(v["after"], ("episode_id", "generation_id"), (), what + ".after")
        if not c.is_episode_id(v["after"]["episode_id"]):
            raise _Invalid("%s.after.episode_id is not an episode ID" % what)
        _generation_id(v["after"]["generation_id"], what + ".after.generation_id")
    else:
        _members(v, ("kind",), (), what)


def _check_values(v, what):
    """PROTOCOL §7.6: canonical sort values the server recorded, as
    [{episode_id, generation_id, values: {title, podcast_id, podcast_title, publication_time,
    duration}}] sorted by (episode ID, generation ID), once each."""
    if not isinstance(v, list):
        raise _Invalid("%s is not an array" % what)
    keys = []
    for i, m in enumerate(v):
        w = "%s[%d]" % (what, i)
        _members(m, ("episode_id", "generation_id", "values"), (), w)
        if not c.is_episode_id(m["episode_id"]):
            raise _Invalid("%s.episode_id is not an episode ID" % w)
        _generation_id(m["generation_id"], w + ".generation_id")
        _members(m["values"], qs.VALUE_FIELDS, (), w + ".values")
        for f in ("title", "podcast_id", "podcast_title"):
            _str(m["values"][f], "%s.values.%s" % (w, f))
        for f in ("publication_time", "duration"):
            _int(m["values"][f], "%s.values.%s" % (w, f))
        keys.append((_bkey(m["episode_id"]), _bkey(m["generation_id"])))
    if keys != sorted(set(keys)):
        raise _Invalid("%s is sorted by (episode ID, generation ID), once each" % what)


def _check_import_decision_body(obj):
    """PROTOCOL §11.5 import decisions as the server writes them (the subset the reference
    models; CHOICE for the queue replay's additions, marked new):

    * `import.podcast`, subject `import:<import id>:podcast:<normalized URL>`: inputs
      {import_id, source_feeds, creation_id}; result {podcast_id, generation_id, url, title,
      state (subscribed|not_subscribed|archived), keep_updated, new_episodes_action,
      secret_id (or null)}.
    * `import.episode`, subject `import:<import id>:episode:<episode>`: inputs {import_id,
      source_items}; result {episode_id, generation_id, occurrence, title, imported:
      {play_state, position, favorite, last_played, played_time, history, units},
      baseline_id, queue_key?, position_id?, queue_index? (new: the backup's global Queue
      index), values? (new: the canonical sort values the import recorded, as a list of one
      entry), duration?}. The reference models no statistics baseline: a non-zero
      last_played or played_time, a history time, units, `rows` or `catalog` are input it
      does not model (RuleInputError).
    * `import.manifest`, subject `import`: inputs {import_id}; result {import_id, kind,
      fingerprint, result: {imported: {podcasts, episodes, queue, ...}, ...}} (the import's counts:
      podcasts, distinct episodes, distinct queued episodes). It completes the import: the
      queue replay's import step is at its place once the import's decisions its counts
      call for are all present (PROTOCOL §11.5; the counts are the explicit completeness evidence, so no subset of an imported queue is
      ever placed)."""
    inp, res, t, subj = obj["inputs"], obj["result"], obj["decision_type"], obj["subject"]
    if t == IMPORT_PODCAST:
        _members(inp, ("import_id", "source_feeds", "creation_id"), (), "inputs")
        _uuid(inp["import_id"], "inputs.import_id")
        _uuid(inp["creation_id"], "inputs.creation_id")
        _members(res, ("podcast_id", "generation_id", "url", "title", "state", "keep_updated", "new_episodes_action",
                       "secret_id"), (), "result")
        _podcast_id(res["podcast_id"], "result.podcast_id")
        _generation_id(res["generation_id"], "result.generation_id")
        _str(res["url"], "result.url")
        _enum(res["state"], IMPORT_STATES, "result.state")
        _bool(res["keep_updated"], "result.keep_updated")
        _enum(res["new_episodes_action"], POLICY_ACTIONS, "result.new_episodes_action")
        if res["secret_id"] is not None:
            _uuid(res["secret_id"], "result.secret_id")
        try:
            norm = c.normalize_url(res["url"])
        except c.ContractError as e:
            raise _Invalid("result.url: %s" % e)
        if subj != "import:%s:podcast:%s" % (inp["import_id"], norm):
            raise _Invalid("subject must be import:<import id>:podcast:<normalized URL>")
    elif t == IMPORT_EPISODE:
        _members(inp, ("import_id", "source_items"), (), "inputs")
        _uuid(inp["import_id"], "inputs.import_id")
        _members(res, ("episode_id", "generation_id", "occurrence", "title", "imported", "baseline_id"),
                 ("queue_key", "position_id", "queue_index", "values", "duration", "rows", "catalog"), "result")
        if not c.is_episode_id(res["episode_id"]):
            raise _Invalid("result.episode_id is not an episode ID")
        _generation_id(res["generation_id"], "result.generation_id")
        _members(res["occurrence"], ("k", "v"), ("guid", "enclosure_url", "title", "publication_time"), "result.occurrence")
        im = res["imported"]
        _members(im, ("play_state", "position", "favorite", "last_played", "played_time", "history", "units"), (),
                 "result.imported")
        _enum(im["play_state"], PLAY_STATES, "result.imported.play_state")
        _int(im["position"], "result.imported.position", 0)
        _bool(im["favorite"], "result.imported.favorite")
        if ("queue_key" in res) != ("position_id" in res) or ("queue_key" in res) != ("queue_index" in res) or (
                "queue_key" in res) != ("values" in res):
            raise _Invalid("queue_key, position_id, queue_index and values come together")
        if "queue_key" in res:
            _key_pid(res["queue_key"], res["position_id"], "result")
            _int(res["queue_index"], "result.queue_index", 0)
            _check_values(res["values"], "result.values")
            if [(m["episode_id"], m["generation_id"]) for m in res["values"]] != [(res["episode_id"], res["generation_id"])]:
                raise _Invalid("an imported episode records its own values")
        if subj != "import:%s:episode:%s" % (inp["import_id"], res["episode_id"]):
            raise _Invalid("subject must be import:<import id>:episode:<episode>")
    else:
        _members(inp, ("import_id",), (), "inputs")
        _uuid(inp["import_id"], "inputs.import_id")
        _members(res, ("import_id", "kind", "fingerprint", "result"), (), "result")
        if res["import_id"] != inp["import_id"] or subj != "import":
            raise _Invalid("a manifest's subject is `import`, for its own import ID")
        r = res["result"]
        if not isinstance(r, dict) or not isinstance(r.get("imported"), dict):
            raise _Invalid("result.result.imported is an object")
        for k in ("podcasts", "episodes", "queue"):
            _int(r["imported"].get(k), "result.result.imported." + k, 0)
        if r["imported"]["queue"] > r["imported"]["episodes"]:
            raise _Invalid("an import queues at most its episodes")


def _unmodelled_import(obj):
    """An import.episode the reference does not model (see _check_import_decision_body)."""
    res = obj["result"]
    im = res["imported"]
    return (im["last_played"] not in (0, None) or im["played_time"] not in (0, None) or im["history"] is not None
            or im["units"] or "rows" in res or "catalog" in res)


def _check_decision_body(obj):
    """Version-1 schemas of the queue decisions, `queue.layout` included (PROTOCOL §7.6), and of
    the catalog, identity, subscription and import decisions."""
    inp, res = obj["inputs"], obj["result"]
    if not isinstance(inp, dict) or not isinstance(res, dict):
        raise _Invalid("inputs and result are objects")
    if obj["decision_type"] in SUBSCRIPTION_DECISIONS:
        _check_subscription_decision_body(obj)
        return
    if obj["decision_type"] in (CATALOG_REFRESH, ADMISSION, MOVED):
        _check_catalog_decision_body(obj)
        return
    if obj["decision_type"] in IDENTITY_DECISIONS:
        _check_identity_decision_body(obj)
        return
    if obj["decision_type"] in IMPORT_DECISIONS:
        _check_import_decision_body(obj)
        return
    if obj["decision_type"] == LAYOUT:
        # PROTOCOL §7.6: subject the account's queue; inputs {frontier, mapping_version,
        # policy}; result the in-members whose key changes, in replay order, each
        # with its key and position ID.
        _members(inp, ("frontier", "mapping_version", "policy"), (), "inputs")
        if not isinstance(inp["frontier"], list) or not all(c.is_uuid(x) for x in inp["frontier"]):
            raise _Invalid("inputs.frontier is an array of node IDs")
        _int(inp["mapping_version"], "inputs.mapping_version", 0)
        if inp["policy"] is not None:
            _uuid(inp["policy"], "inputs.policy")
        _members(res, ("members",), (), "result")
        if not isinstance(res["members"], list):
            raise _Invalid("result.members is not an array")
        for i, m in enumerate(res["members"]):
            _decision_member(m, "result.members[%d]" % i, False)
        ids = [(m["episode_id"], m["generation_id"]) for m in res["members"]]
        if len(set(ids)) != len(ids):
            raise _Invalid("a layout lists each member once")
        keys = [m["key"] for m in res["members"]]
        if any(c.compare_queue_keys(a, z) >= 0 for a, z in zip(keys, keys[1:])):
            raise _Invalid("a layout's keys increase in its order")
        if obj["subject"] != LAYOUT_SUBJECT:
            raise _Invalid("a layout's subject is %r" % LAYOUT_SUBJECT)
        return
    _members(inp, ("trigger", "enqueue_location"), (), "inputs")
    _enum(inp["enqueue_location"], ENQUEUE_LOCATIONS, "inputs.enqueue_location")
    _members(res, ("episode_id", "generation_id", "key", "position_id", "place", "values"), (), "result")
    _decision_member({k: res[k] for k in ("episode_id", "generation_id", "key", "position_id")}, "result", False)
    _decision_place(res["place"], "result.place")
    _check_values(res["values"], "result.values")
    if inp["trigger"] != obj["subject"]:
        raise _Invalid("inputs.trigger must be the subject")


def _place(v, what):
    """PROTOCOL §7.6: the gap the device resolved when it executed the command:
    `{kind: start}`, `{kind: after, after: EpisodeRef}` or `{kind: end}`. The anchor is
    context (validated as a well-formed EpisodeRef), never a mutation target."""
    if not isinstance(v, dict) or "kind" not in v:
        raise _Invalid("%s is not a place" % what)
    _enum(v["kind"], PLACE_KINDS, what + ".kind")
    if v["kind"] == "after":
        _members(v, ("kind", "after"), (), what)
        return {"kind": "after", "after": _episode_ref(v["after"], what + ".after")}
    _members(v, ("kind",), (), what)
    return {"kind": v["kind"]}


def _ref_list(v, what, nonempty):
    """An array of EpisodeRefs, one per episode, in the given order."""
    if not isinstance(v, list):
        raise _Invalid("%s is not an array" % what)
    if nonempty and not v:
        raise _Invalid("%s is empty" % what)
    refs = [_episode_ref(r, "%s[%d]" % (what, i)) for i, r in enumerate(v)]
    _unique_episodes(refs)
    return refs


def _parse_queue(t, p, targets, account_id):
    """PROTOCOL §7.6 (type_version 1, amended in place: no real
    data, PROTOCOL §7.6). Returns {"form", "entries": [(ref, kind, data)...] in payload order, and
    the form's `place`, `run`, `sort` and `observed`}.

    * `queue.add {members: [placement...] (non-empty, in requested order), place}`;
      `moves` are invalid (an unknown member: invalid).
    * `queue.restore {members: [placement...], place}` (place resolved at Undo time).
    * `queue.move {episode, key, position_id, place}`.
    * `queue.reorder`, a multi-item move: `{run: [EpisodeRef...] (the complete selected
      run in its resulting order), place, members?}`: `run` alone defines the movement;
      the optional `members` are the device's sparse key predictions (PROTOCOL §7.6: "today's
      sparse members ... stay only as that"),
      which only feed key retention (PROTOCOL §7.6). CHOICE: when present they are non-empty
      placements, one per episode, each naming one of the run's EpisodeRefs exactly (as
      the run names it). The sort dialog: `{sort: {sort_order, permutation?}, observed:
      [EpisodeRef...]}`, without key predictions. CHOICE: a sort's `observed` (possibly
      empty) is context, so its target is the account, as the sort policy's;
      `permutation` is present exactly for `random`, and names exactly the observed
      episodes, once each.
    * `queue.remove` and `queue.clear` are unchanged.

    The members' keys (add, restore, move) are the device's predictions: a layout keeps one
    that fits (PROTOCOL §7.6). Every key parses and carries its position ID's residue (PROTOCOL §7.6)."""
    entries = []
    out = {"form": None}
    if t in ("queue.add", "queue.restore"):
        _members(p, ("members", "place"))
        kind = "q_add" if t == "queue.add" else "q_restore"
        entries = [(m["ref"], kind, {"key": m["key"], "pid": m["pid"]}) for m in _placements(p["members"], "members", True)]
        out.update(form="add" if t == "queue.add" else "restore", place=_place(p["place"], "place"))
    elif t == "queue.remove":
        _members(p, ("episodes", "reason"))
        refs = _sorted_refs(p["episodes"], "episodes")
        _enum(p["reason"], QUEUE_REMOVE_REASONS, "reason")
        entries = [(r, "q_remove", {"reason": p["reason"]}) for r in refs]
        out["form"] = "remove"
    elif t == "queue.move":
        _members(p, ("episode", "key", "position_id", "place"))
        m = _placement({k: p[k] for k in ("episode", "key", "position_id")}, "payload")
        entries = [(m["ref"], "q_move", {"key": m["key"], "pid": m["pid"]})]
        out.update(form="move", place=_place(p["place"], "place"))
    elif t == "queue.reorder":
        if "run" in p:
            _members(p, ("run", "place"), ("members",))
            run = _ref_list(p["run"], "run", True)
            preds = {}
            if "members" in p:
                ms = _placements(p["members"], "members", True)
                _unique_episodes([m["ref"] for m in ms])
                for i, m in enumerate(ms):
                    j = next((j for j, r in enumerate(p["run"]) if r == p["members"][i]["episode"]), None)
                    if j is None:
                        raise _Invalid("members[%d] names an episode the run does not name" % i)
                    preds[j] = {"key": m["key"], "pid": m["pid"]}
            entries = [(r, "q_move", preds.get(j, {})) for j, r in enumerate(run)]
            out.update(form="run", place=_place(p["place"], "place"))
        else:
            _members(p, ("sort", "observed"))
            srt = p["sort"]
            _members(srt, ("sort_order",), ("permutation",), "sort")
            _enum(srt["sort_order"], SORT_ORDERS, "sort.sort_order")
            observed = _ref_list(p["observed"], "observed", False)
            perm = None
            if srt["sort_order"] == "random":
                if "permutation" not in srt:
                    raise _Invalid("a random sort carries its permutation")
                perm = _ref_list(srt["permutation"], "sort.permutation", False)
                if sorted(r["episode_id"] for r in perm) != sorted(r["episode_id"] for r in observed):
                    raise _Invalid("the permutation orders exactly the observed episodes")
            elif "permutation" in srt:
                raise _Invalid("only a random sort carries a permutation")
            if targets != [{"entity": "account", "id": account_id}]:
                raise _Invalid("a sort's target is exactly {entity: account, id: <account>}")
            out.update(form="sort", sort_order=srt["sort_order"], observed=observed, permutation=perm, entries=[])
            return out
    elif t == "queue.clear":
        _members(p, ("observed",))
        entries = [(r, "q_clear", {}) for r in _sorted_refs(p["observed"], "observed")]
        out["form"] = "clear"
    refs = [e[0] for e in entries]
    _unique_episodes(refs)
    _episode_targets(targets, refs)
    out["entries"] = entries
    return out


# ---------------------------------------------------------------------------
# PROTOCOL §9.4 payloads (PROTOCOL §4 record values)
# ---------------------------------------------------------------------------


def _opt_str(v, what):
    if v is not None and not isinstance(v, str):
        raise _Invalid("%s is not a string or null" % what)


def _decimal(v, what):
    """The parser's canonical Feed form writes Java longs a feed can make arbitrarily large
    (enclosure size, Podlove chapter start) as decimal strings."""
    s = v[1:] if isinstance(v, str) and v.startswith("-") else v
    if not isinstance(s, str) or not s or not all("0" <= ch <= "9" for ch in s):
        raise _Invalid("%s is not a decimal string" % what)


def feed_fields(v, what="fields"):
    """PROTOCOL §9.4 podcast `metadata` value without `revision` and `fetch_time`: the parser's Feed
    fields minus identity (GUIDs, occurrences) and moves (alternate feed URLs,
    new-feed-url). CHOICE: exactly these members; text and URLs are strings or null,
    `paged` a boolean, `funding` an array of {url, content} (strings or null), as the parser's
    canonical Feed form has them."""
    _members(v, FEED_FIELDS, (), what)
    for f in ("title", "description", "author", "language", "link", "image_url", "type", "next_page_url"):
        _opt_str(v[f], "%s.%s" % (what, f))
    _bool(v["paged"], what + ".paged")
    if not isinstance(v["funding"], list):
        raise _Invalid("%s.funding is not an array" % what)
    for i, x in enumerate(v["funding"]):
        w = "%s.funding[%d]" % (what, i)
        _members(x, ("url", "content"), (), w)
        _opt_str(x["url"], w + ".url")
        _opt_str(x["content"], w + ".content")
    return copy.deepcopy(v)


def item_fields(v, what="fields"):
    """PROTOCOL §9.4 episode `metadata` value without `revision` and `fetch_time`: the parser's Item
    fields minus the GUID. CHOICE: exactly these members; `pub_date` a safe integer (ms)
    or null; `media` null (no enclosure: a catalog-only item) or {url (string), size
    (decimal string or null), mime_type (string or null), duration (integer ms >= 0)};
    `chapters` null or an array of {start (decimal string), title, link, image_url}."""
    _members(v, ITEM_FIELDS, (), what)
    for f in ("title", "description", "link", "image_url", "payment_link", "chapter_url", "transcript_url",
              "transcript_type", "social_interact_url"):
        _opt_str(v[f], "%s.%s" % (what, f))
    if v["pub_date"] is not None:
        _int(v["pub_date"], what + ".pub_date")
    m = v["media"]
    if m is not None:
        w = what + ".media"
        _members(m, ("url", "size", "mime_type", "duration"), (), w)
        _str(m["url"], w + ".url")
        if m["size"] is not None:
            _decimal(m["size"], w + ".size")
        _opt_str(m["mime_type"], w + ".mime_type")
        _int(m["duration"], w + ".duration", 0)
    ch = v["chapters"]
    if ch is not None:
        if not isinstance(ch, list):
            raise _Invalid("%s.chapters is not an array or null" % what)
        for i, x in enumerate(ch):
            w = "%s.chapters[%d]" % (what, i)
            _members(x, ("start", "title", "link", "image_url"), (), w)
            _decimal(x["start"], w + ".start")
            for f in ("title", "link", "image_url"):
                _opt_str(x[f], "%s.%s" % (w, f))
    return copy.deepcopy(v)


def _base(v, what):
    """A record observation's `base`: the canonical revision (a node) the device last
    received for that record. Optional in an observation (absent
    means no revision held; CHOICE: null is accepted as the same); a UUID that need not be
    a known node (an unknown base never names the current revision, so it is
    concurrent). Decisions always carry it (null for none)."""
    if v is not None:
        _uuid(v, what)


def _parse_observe(p, targets):
    """PROTOCOL §9.4 `catalog.observe`: {podcast_id, batch_id, parser_version, fetch_time, feed?:
    {base?, fields}, items: [{episode, base?, fields}]}.

    CHOICE: `parser_version` is a non-empty string; `items` may be empty only when `feed`
    is present; each item's EpisodeRef names the payload's podcast (else invalid_payload)
    and an episode appears once; its generation_id is well-formed but not checked
    (observations are catalog evidence, accepted whatever the subscription state, PROTOCOL §9).
    Targets: the podcast and every episode, with no generation, sorted."""
    _members(p, ("podcast_id", "batch_id", "parser_version", "fetch_time", "items"), ("feed",))
    P = p["podcast_id"]
    _podcast_id(P, "podcast_id")
    _uuid(p["batch_id"], "batch_id")
    if not isinstance(p["parser_version"], str) or not p["parser_version"]:
        raise _Invalid("parser_version is not a non-empty string")
    _int(p["fetch_time"], "fetch_time")
    out = {"podcast_id": P, "batch_id": p["batch_id"], "fetch_time": p["fetch_time"], "feed": None, "items": []}
    if "feed" in p:
        _members(p["feed"], ("fields",), ("base",), "feed")
        _base(p["feed"].get("base"), "feed.base")
        out["feed"] = {"base": p["feed"].get("base"), "fields": feed_fields(p["feed"]["fields"], "feed.fields")}
    if not isinstance(p["items"], list):
        raise _Invalid("items is not an array")
    for i, x in enumerate(p["items"]):
        w = "items[%d]" % i
        _members(x, ("episode", "fields"), ("base",), w)
        ref = _episode_ref(x["episode"], w + ".episode")
        if ref["podcast_id"] != P:
            raise _Invalid("%s names another podcast" % w)
        _base(x.get("base"), w + ".base")
        out["items"].append({"ref": ref, "base": x.get("base"), "fields": item_fields(x["fields"], w + ".fields")})
    if out["feed"] is None and not out["items"]:
        raise _Invalid("an observation names at least one record")
    _unique_episodes([x["ref"] for x in out["items"]])
    want = [{"entity": "podcast", "id": P}] + [{"entity": "episode", "id": x["ref"]["episode_id"]} for x in out["items"]]
    want.sort(key=lambda t: (_bkey(t["entity"]), _bkey(t["id"])))
    if targets != want:
        raise _Invalid("targets must be the podcast and every observed episode, without generation")
    return out


def _parse_propose(p, targets):
    """PROTOCOL §9.4 `discovery.propose`: {podcast_id, generation_id, episode, batch_id, pub_date,
    newest_before_batch, gained_media?: {catalog_only_observation}, action, policy_node,
    queue_bundle?}.

    CHOICE: `podcast_id` and `generation_id` must equal the EpisodeRef's; `pub_date` and
    `newest_before_batch` are safe integers or null (both members required);
    `policy_node` is a UUID or null and is evidence only (never checked against the
    policy); `catalog_only_observation` is a UUID (a fact or a server decision); a
    `queue_bundle` ({key, position_id, place}, as the play-start bundle, PROTOCOL §7.6) is allowed only with action `queue`. Target: the episode with its generation."""
    _members(p, ("podcast_id", "generation_id", "episode", "batch_id", "pub_date", "newest_before_batch", "action",
                 "policy_node"), ("gained_media", "queue_bundle"))
    ref = _episode_ref(p["episode"], "episode")
    _podcast_id(p["podcast_id"], "podcast_id")
    _generation_id(p["generation_id"], "generation_id")
    if (p["podcast_id"], p["generation_id"]) != (ref["podcast_id"], ref["generation_id"]):
        raise _Invalid("podcast_id and generation_id must be the EpisodeRef's")
    _uuid(p["batch_id"], "batch_id")
    for f in ("pub_date", "newest_before_batch"):
        if p[f] is not None:
            _int(p[f], f)
    _enum(p["action"], DISCOVERY_ACTIONS, "action")
    if p["policy_node"] is not None:
        _uuid(p["policy_node"], "policy_node")
    out = {"refs": [ref], "pub_date": p["pub_date"], "newest": p["newest_before_batch"], "action": p["action"],
           "gained": None, "bundle": None}
    if "gained_media" in p:
        _members(p["gained_media"], ("catalog_only_observation",), (), "gained_media")
        _uuid(p["gained_media"]["catalog_only_observation"], "gained_media.catalog_only_observation")
        out["gained"] = p["gained_media"]["catalog_only_observation"]
    target_refs = [ref]
    if "queue_bundle" in p:
        if p["action"] != "queue":
            raise _Invalid("a queue_bundle needs the action queue")
        qb = p["queue_bundle"]
        _members(qb, ("key", "position_id", "place"), (), "queue_bundle")
        _key_pid(qb["key"], qb["position_id"], "queue_bundle")
        out["bundle"] = {"key": qb["key"], "pid": qb["position_id"], "place": _place(qb["place"], "queue_bundle.place")}
    _episode_targets(targets, target_refs)
    return out


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class _Contribution(object):
    """A node's effect on one scope. `orig` is the episode the fact or decision named
    (before routing; orders a compound, PROTOCOL §9.7) and `occ` the (GUID, normalized enclosure,
    publication time) of a fact's episode reference (None for decisions), by which a
    reused GUID's facts are partitioned (PROTOCOL §9.7)."""
    __slots__ = ("node", "kind", "gen", "data", "orig", "occ", "ref", "ngen")

    def __init__(self, node, kind, gen=None, data=None, orig=None, occ=None, ref=None):
        self.node = node
        self.kind = kind
        self.gen = gen
        self.data = data or {}
        self.orig = orig
        self.occ = occ
        # A fact's named reference (episode, podcast, generation, GUID kind), from which
        # its place is recomputed whenever identity changes; ngen the generation it named.
        self.ref = ref
        self.ngen = gen


class _Fold(object):
    """Result of folding one scope: records by field group, determining node per register,
    and the per-contribution status ("wrote", "fired", "noop", "ineffective:<reason>")."""

    __slots__ = ("records", "det", "status", "mem_timeline", "auto_applied")

    def __init__(self):
        self.records = {}
        self.det = {}
        self.status = {}
        # PROTOCOL §7.6: the scope's effective membership events [(rank, in_queue)]
        # in order, and the admissions whose automatic add applies.
        self.mem_timeline = []
        self.auto_applied = set()


def _strip_meta(value):
    """A metadata record value without `revision` and `fetch_time`: the observed fields."""
    return {k: v for k, v in value.items() if k not in ("revision", "fetch_time")}


def _outcome(o, reason=None):
    d = {"outcome": o}
    if reason is not None:
        d["reason"] = reason
    return d


def _bkey(s):
    return s.encode("utf-8")


def sort_records(records):
    return sorted(records, key=lambda r: (_bkey(r["entity"]), _bkey(r["id"]), _bkey(r["field_group"])))


class Engine(object):
    """Server-side rule engine for one account.

    `fixed_resolutions` ({op_id: {podcast_id, generation_id, mode}}), `fixed_aliases`
    ({normalized_url: {podcast_id, op_id}}) and `fixed_episodes` ({episode_id: {podcast_id,
    occurrence, op_id}}) replay persisted acceptance-time allocations instead of deciding
    them (PROTOCOL §9.5, §7.2); used by the permutation test. With `replay`
    the engine makes no queue decisions at all: they arrive as `{"decision": ...}`
    arrivals (the persisted decisions of the permutation test, PROTOCOL §7.6), and live
    arrivals need not come in receipt order (they carry their persisted `receipt`).
    `fixed_outcomes` ({op_id: {outcome, reason?}}) replays persisted receipts (PROTOCOL §6.10:
    receipts never change): a fact whose receipt is rejected is archived as such, and the
    session checks are not re-run (a fact refused by them contributes nothing).
    `fixed_values` ({op_id: [values...]}) replays the sort values the server recorded in
    persisted receipts (PROTOCOL §7.6) in that mode.
    """

    def __init__(self, account_id, namespace, fixed_resolutions=None, fixed_aliases=None, fixed_episodes=None,
                 replay=False, fixed_outcomes=None, rebuild=False, fixed_values=None):
        if not c.is_uuid(account_id) or not c.is_uuid(namespace):
            raise RuleInputError("account_id and namespace must be lowercase UUIDs")
        self.account_id = account_id
        self.namespace = namespace
        self.fixed_resolutions = fixed_resolutions
        self.fixed_aliases = fixed_aliases
        self.fixed_episodes = fixed_episodes
        self.replay = replay
        self.fixed_outcomes = fixed_outcomes
        # PROTOCOL §6.10. Receipt positions of archived facts and the chain state derived
        # from them in receipt order: author coordinates, effective and inherited authors,
        # held positions, derived authors' parents, applied forks (op -> {"author": F,
        # "wire": author, "head": head}) and each fork author's binding fork.
        self.narrivals = 0
        self.rkeys = set()
        self.max_rkey = None
        self.coords = {}
        self.eff = {}
        self.inherited = {}
        self.held = {}
        self.derived_parent = {}
        self.forks = {}
        self.fork_heads = {}
        self.fork_of = {}
        # PROTOCOL §7.6: server decisions (decision_id -> {"obj", "rank", "live"}), the
        # decisions per (decision_type, subject) (competitors, PROTOCOL §9.5: the smallest rank is
        # effective), the decisions created here (expected.decisions), the queue nodes and
        # the nodes having a queue node as descendant (for the frontier a decision
        # observes), a memo of trigger ancestry, and the current arrival's context.
        self.decisions = {}
        self.by_subject = {}
        self.created = []
        self.qnodes = set()
        self.qflag = set()
        self._desc_memo = {}
        self._ctx = None
        # Causal order (PROTOCOL §6.9): node IDs in rank order.
        self.order = []
        self.rank = {}
        self.prio = {}
        self.node_time = {}
        # Archive: op_id -> {"hash", "fact", "admitted", "frontier"}.
        self.facts = {}
        self.author_positions = {}
        self.transport = []  # (op_id, "duplicate" | "op_id_conflict"), diagnostics only
        self.resolutions = {}
        self.outcomes = {}
        # Catalog skeleton (PROTOCOL §9.4).
        self.aliases = {}  # normalized -> {"podcast_id", "url", "op_id"}
        self.podcasts = {}  # P -> {"creating_op", "generations": [G...]}
        self.generations = {}  # G -> {"podcast_id", "op_id", "kind", "predecessor", "policy", "url", "closed_by"}
        self.episodes = {}  # E -> {"podcast_id", "occurrence", "op_id"} (acceptance-time binding)
        self.episode_gens = {}  # E -> set of G with a scope
        # Contributions per scope.
        self.scopes = {}
        # Projection: (entity, id) -> {field_group: value}.
        self.proj = {}
        # PROTOCOL §8: resolved edges of every causal node (for the causal past of cuts)
        # and the session registry used by the admission checks: session_id ->
        # {"episode_id", "generation_id", "start", "facts": [the session's checkpoints,
        # ends and finalizations that passed the session checks, in acceptance order]}.
        # Continuations are derived from it with the current effective authors.
        self.node_edges = {}
        self.sessions = {}
        # PROTOCOL §9.4. Catalog items, acceptance-time: E -> {"podcast_id", "first"
        # (first observing node), "first_time" (its clamped fetch time), "playable" (first
        # observing node with an enclosure, or None), "date" (effective)}; which observations saw which
        # episode without media (gained-media evidence): (node, E) -> has media; the
        # admissions per (G, E) (decision IDs, in admission order); the generations whose
        # first population the server recorded (from catalog.refresh part 0); the server's
        # own fetch history (its replica, PROTOCOL §9.6): P -> {E: {"playable"}}; pending
        # fetches (fetch_start) and every refresh ID used; the live and the last published
        # refresh status per podcast; the stored response of the last successful fetch
        # (PROTOCOL §9.6: its refresh ID, fetch time and the revision each of its records names).
        self.catalog_items = {}
        self.obs_media = {}
        self.admitted = {}
        self.populated = set()
        self.server_seen = {}
        self.fetch_pending = {}
        self.refresh_ids = set()
        self.status_live = {}
        self.status_logged = {}
        self.stored = {}
        # PROTOCOL §9.7. Merge redirects to the root survivor (one step: E -> root);
        # identity evidence per podcast ((GUID, normalized enclosure, publication time) ->
        # {"episode", "node"}, in recording order); lookup aliases per podcast, only from
        # server decisions ((kind, value) -> {"episode", "node"}); the occurrence-mapping
        # decisions per (podcast, disputed GUID); podcast merges (source -> {"into",
        # "decision", "gens": {G: (mode, target G)}, "episodes": {from: to}}).
        self.root = {}
        self.evidence = {}
        self.lookup = {}
        self.mappings = {}
        self.additions = {}  # (P, GUID) -> enclosure-addition mapping decisions
        self.pmerged = {}
        # Merged episodes whose GUID becomes a lookup alias of the survivor (src -> [(P_dst,
        # deciding node)]): the alias is added whenever src's occurrence is known, at the
        # merge or when src registers later (recovery may admit the merge first), and
        # contributions are re-placed at the end of that arrival.
        self.merge_alias = {}
        self._alias_dirty = False
        # PROTOCOL §6.11. The account's current epoch and the high-water mark of every epoch
        # it used. The archive, kept beside the derived state by an engine taking
        # recovery uploads (not by a replay): every archived fact (op -> {"arrival", "pos"
        # (epoch, seq, index), "outcome" (its receipt's, or None when a recovered arrival
        # does not state it), "node" (a causal node), "hash", "recovered", "superseded"
        # (provisional receipts it replaced)}), every decision (key -> {"obj", "type",
        # "subject", "result", "pos", "id" (None for an implicit one: a live subscription's
        # podcast.alias and podcast.resolution, whose IDs vectors do not record)}), the
        # archive's node times and rejected frontiers, and whether derived state must be
        # rebuilt from it. In a rebuild (`rebuild`): pending effects and joins.
        self.epoch = 1
        self.high_water = 1
        self.archiving = not replay
        self.ar_facts = {}
        self.ar_dec = {}
        self.ar_nodes = {}
        self.ar_rejected = {}
        self.dirty = False
        self.rebuild_mode = rebuild
        self.gen_join = {}
        self.parked = set()
        self.pre_admit = {}  # rebuild: op -> the identity decisions its own acceptance made
        # PROTOCOL §7.6. The placement and sort commands of admitted facts (op -> their
        # parsed payload as named, the status each named member had at acceptance: "ok",
        # "closed" or "fail"), the canonical sort values each fact's receipt recorded (PROTOCOL
        # §7.6;
        # op -> [{episode_id, generation_id, values}]), receipts' values replayed from a
        # vector (`fixed_values`, op -> list), the nodes with a child (for the account's
        # causal frontier a layout depends on), the published keys (see below), the
        # import decisions (import ID -> {"podcasts": [...], "episodes": [...],
        # "manifest": decision ID}) and the PROTOCOL §7.6 diagnostics (anchors with no entry).
        self.qsteps = {}
        self.recorded = {}
        self.fixed_values = fixed_values
        self.has_child = set()
        # PROTOCOL §7.6: the published key of each scope, its latest key write
        # in rank order ({(E, G): (key, position_id, rank)}), kept in step with the records
        # by _sync_pub whenever an arrival may have changed it (pub_dirty).
        self.pubkeys = {}
        self.pub_dirty = False
        self._syncing = False
        self.replay_placed = set()
        self.imports = {}
        self.import_queued = set()
        self.diagnostics = []

    # -- causal order --------------------------------------------------------

    def _insert_node(self, nid, t, edges):
        """The insertion rule (PROTOCOL §6.9): immediately before the first node after the largest
        edge rank whose priority (normalized_time, id bytewise) is greater; else last."""
        prio = (t, _bkey(nid))
        p = max((self.rank[e] for e in edges), default=-1)
        i = p + 1
        n = len(self.order)
        while i < n and self.prio[self.order[i]] < prio:
            i += 1
        self.order.insert(i, nid)
        for j in range(i, len(self.order)):
            self.rank[self.order[j]] = j
        self.prio[nid] = prio
        self.node_time[nid] = t
        self.node_edges[nid] = list(edges)
        self.has_child.update(edges)

    def _ancestors(self, nid):
        """The causal past of a node: its resolved edges and their ancestors (PROTOCOL §8 "A cut
        observes its causal past"; PROTOCOL §6.4: edges are the resolved frontier, so a
        rejected predecessor stands for what it observed)."""
        seen = set()
        stack = list(self.node_edges[nid])
        while stack:
            x = stack.pop()
            if x in seen:
                continue
            seen.add(x)
            stack.extend(self.node_edges.get(x, ()))
        return seen

    # -- acceptance ------------------------------------------------------------

    def accept(self, arrival, chooser=None):
        """Accepts one arrival. `chooser(decision_type, info)`, when given, makes the
        server's random choices for the decisions this acceptance creates (and records
        them in the arrival's `decisions`); without it they are read from `decisions`."""
        if not isinstance(arrival, dict):
            raise RuleInputError("arrival is not an object")
        self.narrivals += 1
        self.pub_dirty = True
        try:
            self._accept_one(arrival, chooser)
        except RuleInputError:
            # Input the server would never have accepted is no arrival of the case (a
            # vector never holds it): it takes no position, so later default receipts and
            # commit revisions stay those of the case's own arrivals.
            self.narrivals -= 1
            raise
        finally:
            # Whatever the arrival changed, the published keys follow at the next read
            # (an _ensure inside the acceptance may have synchronized them half-way).
            self.pub_dirty = True
            if self._alias_dirty:
                self._alias_dirty = False
                self._place_all()

    def _accept_one(self, arrival, chooser):
        kinds = [k for k in ("fact", "decision", "fetch_start", "fetch", "recovery_end") if k in arrival]
        if len(kinds) != 1:
            raise RuleInputError("an arrival is exactly one of a fact, a decision, a fetch start, a fetch or a "
                                 "recovery end: %s" % sorted(arrival))
        if "recovery_end" in arrival:
            self._accept_recovery_end(arrival, chooser)
            return
        if "decision" in arrival:
            if self.archiving:
                self._accept_recovered_decision(arrival, chooser)
            else:
                self._accept_decision_arrival(arrival)
            return
        if "fetch_start" in arrival:
            self._ensure()
            self._accept_fetch_start(arrival)
            return
        if "fetch" in arrival:
            self._ensure()
            self._accept_fetch(arrival, chooser)
            return
        for k in ("fact", "receive_time", "normalized_time", "edges", "capabilities"):
            if k not in arrival:
                raise RuleInputError("arrival lacks %r" % k)
        recovery = arrival.get("recovery", False)
        if not isinstance(recovery, bool):
            raise RuleInputError("recovery must be a boolean")
        supply = arrival.get("decisions")
        if supply is not None and (recovery or chooser is not None):
            raise RuleInputError("decisions are supplied only for live acceptances replayed from a vector")
        if supply is not None and (not isinstance(supply, list) or not all(isinstance(x, dict) for x in supply)):
            raise RuleInputError("decisions must be an array of objects")
        if recovery and self.archiving:
            self._accept_recovered_fact(arrival)
            return
        self._ensure()
        self._ctx = {
            "arrival": arrival,
            "live": not recovery and not self.replay,
            "receive_time": arrival["receive_time"],
            "chooser": chooser,
            "supply": list(supply or []),
            "caps": arrival["capabilities"],
            "fixed": None,
        }
        try:
            self._accept_fact(arrival, recovery)
            if self._ctx["live"] and not self._ctx.get("duplicate"):
                # PROTOCOL §7.6: the publication rule ends every commit.
                self._publish_queue()
            if self._ctx["supply"] and self._ctx["live"]:
                raise RuleInputError("the arrival supplies %d unused decision choice(s)" % len(self._ctx["supply"]))
            if self.archiving:
                self._archive_live(arrival)
        finally:
            self._ctx = None

    def _accept_fact(self, arrival, recovery):
        fact = arrival["fact"]
        rt, nt, edges, caps = arrival["receive_time"], arrival["normalized_time"], arrival["edges"], arrival["capabilities"]
        for v, what in ((rt, "receive_time"), (nt, "normalized_time")):
            if not isinstance(v, int) or isinstance(v, bool) or abs(v) > c.MAX_SAFE_INTEGER:
                raise RuleInputError("%s is not a safe integer" % what)
        if not isinstance(edges, list) or not all(isinstance(e, str) for e in edges):
            raise RuleInputError("edges must be an array of node IDs")
        if not isinstance(caps, list) or not all(isinstance(x, str) for x in caps):
            raise RuleInputError("capabilities must be an array of strings")
        if not isinstance(fact, dict):
            raise RuleInputError("fact is not an object (unarchivable: invalid_request)")
        if isinstance(fact.get("type"), str) and fact["type"] in OPERATION_CAPABILITY:
            for u in _url_fields(fact):
                if has_userinfo(u):
                    raise RuleInputError(
                        "URL with userinfo %r: the server refuses the request with invalid_request before archiving" % u
                    )
        why = unarchivable_reason(fact)
        if why is not None:
            raise RuleInputError("unarchivable fact (invalid_request, nothing consumed): %s" % why)
        op = fact["op_id"]
        env_err = envelope_reason(fact)
        h = c.content_hash(fact)
        if op in self.facts or op in self.prio:
            # A transport duplicate or conflict consumes no decision choice (and commits
            # nothing, so no layout either).
            self._ctx["supply"] = []
            self._ctx["duplicate"] = True
            if op in self.facts:
                self.transport.append((op, "duplicate" if self.facts[op]["hash"] == h else "op_id_conflict"))
            else:
                self.transport.append((op, "op_id_conflict"))
            return
        rkey, fixed = self._receipt(arrival, recovery, op)
        self._ctx["fixed"] = fixed
        self._ctx["rkey"] = rkey
        if arrival.get("node_only") and self.rebuild_mode:
            # PROTOCOL §6.11: an original rejected receipt replaced a provisional acceptance that
            # made the fact a node with descendants: it stays a node (at its provisional time
            # and edges), holds its chain position at the original receipt and contributes
            # nothing.
            self.facts[op] = {"hash": h, "fact": fact, "admitted": True, "frontier": sorted(set(edges), key=_bkey),
                              "rkey": rkey}
            moved = self._place(op, rkey)
            self._insert_node(op, nt, sorted(set(edges), key=_bkey))
            self._after_placement(op, moved)
            return
        if fixed is not None and fixed["outcome"] == REJECTED:
            # An original rejected receipt (a recovered fact, or a replay of persisted
            # receipts): archived as recorded, with its receipt's frontier as edges.
            if env_err is None and fact.get("type") not in OPERATION_CAPABILITY:
                raise RuleInputError("%s is unknown: version_unsupported, never accepted" % fact.get("type"))
            if sorted(set(edges), key=_bkey) != edges or not all(e in self.prio for e in edges):
                raise RuleInputError("a recovered rejected fact's edges are its receipt's frontier of known nodes")
            if nt != rt:
                raise RuleInputError("a rejected fact's normalized_time is its receive_time")
            self._archive_rejected(op, h, fact, fixed.get("reason"), list(edges), rkey, record=not recovery)
            return
        if env_err is not None:
            if fixed is not None:
                raise RuleInputError("the receipt of %s says it was admitted, but its envelope is invalid" % op)
            self._reject(op, h, fact, env_err, [], arrival, rkey)
            return
        typ = fact["type"]
        if typ not in OPERATION_CAPABILITY or fact["type_version"] != 1:
            raise RuleInputError("%s v%s is unknown: version_unsupported, never accepted" % (typ, fact["type_version"]))
        frontier = set()
        complete = True
        for d in fact["dependencies"]:
            if d in self.prio:
                frontier.add(d)
            elif d in self.facts and not self.facts[d]["admitted"]:
                frontier.update(self.facts[d]["frontier"])
            elif recovery:
                # CHOICE (PROTOCOL §6.10): recovery may upload a fact before an archived
                # rejected dependency (for example a fork before its rejected head); its
                # recorded edges stand and rule effects naming the missing fact wait for it.
                complete = False
            else:
                raise RuleInputError("dependency %s is unknown: dependency_missing, never accepted" % d)
        prev = fact.get("previous_op_id")
        if prev is not None and prev not in self.facts and prev not in self.prio:
            if not recovery:
                raise RuleInputError("previous_op_id %s is unknown: dependency_missing" % prev)
            complete = False
        if typ == "history.restore":
            # PROTOCOL §8 check 5: an unknown removal ID is dependency_missing, as the restore
            # depends on it. CHOICE: like PROTOCOL §7.2 prerequisites it is checked with
            # the dependencies, before the author and capability checks, and only for an
            # otherwise valid payload (a malformed reference is invalid_payload).
            try:
                rp = parse_payload(fact, self.account_id)
            except _Invalid:
                rp = None
            if rp is not None and rp["removal"] not in self.facts and rp["removal"] not in self.prio:
                raise RuleInputError("removal %s is unknown: dependency_missing, never accepted" % rp["removal"])
        frontier = sorted(frontier, key=_bkey)
        # PROTOCOL §6.10: an author-position collision is no longer a rejection (the fact is admitted
        # under its derived author); the other author-history rejections remain. CHOICE:
        # impossible_author_history keeps PROTOCOL §6.4 wording exactly (an *admitted*
        # predecessor of another author or with a higher author_seq).
        reason = None
        if prev is not None and prev in self.facts and self.facts[prev]["admitted"]:
            pf = self.facts[prev]["fact"]
            if pf["author_id"] != fact["author_id"] or pf["author_seq"] > fact["author_seq"]:
                reason = R_IMPOSSIBLE_HISTORY
        cap = OPERATION_CAPABILITY[typ]
        # Recovery (PROTOCOL §6.11) may retransmit previously accepted facts of any
        # capability (CHOICE: a recovered arrival skips the capability checks).
        if reason is None and cap != "core" and cap not in caps and not recovery:
            reason = R_CAPABILITY
        parsed = None
        if reason is None:
            try:
                parsed = parse_payload(fact, self.account_id)
            except _Invalid:
                reason = R_INVALID_PAYLOAD
        if reason is None and parsed.get("bundle") is not None and "queue" not in caps and not recovery:
            # CHOICE: a play-start queue bundle is a queue write; a client without the
            # queue capability may not author one (PROTOCOL §10.1 rule 2). Checked once the
            # payload is valid, before the session checks.
            reason = R_CAPABILITY
        if reason is None:
            # PROTOCOL §9.7: references are resolved (merge redirects, then the resolver) before
            # any state validation.
            for r in self._refs_of(typ, parsed):
                self._route_inplace(r)
        if reason is None and typ in LISTENING_TYPES and fixed is None:
            # CHOICE (PROTOCOL §9.7): with a persisted receipt the session checks are not re-run (their
            # episode comparison follows merges a recovery order may admit later).
            reason = self._listening_rejection(fact, typ, parsed)
        if reason is None and typ == "session.checkpoint" and parsed["claim"] and fixed is None and not recovery:
            reason = self._claim_rejection(op, fact, parsed, rkey)
        if reason is None and typ == FORK:
            reason = self._fork_rejection(op, fact, parsed, rkey, lenient=recovery or fixed is not None)
        if reason is not None:
            if recovery or fixed is not None:
                raise RuleInputError("fact %s would be rejected (%s), but its receipt says it was admitted" % (op, reason))
            self._reject(op, h, fact, reason, frontier, arrival, rkey)
            return
        if complete:
            if sorted(set(edges), key=_bkey) != frontier or len(edges) != len(set(edges)):
                raise RuleInputError("edges %s differ from the resolved frontier %s" % (edges, frontier))
        else:
            if sorted(set(edges), key=_bkey) != edges or not all(e in self.prio for e in edges):
                raise RuleInputError("a recovered fact's edges must be sorted known causal nodes")
            frontier = list(edges)
        try:
            want_nt = c.normalize_time(fact["client_time"], rt, [self.node_time[e] for e in frontier])
        except c.ContractError as e:
            raise RuleInputError("time normalization: %s" % e)
        if nt != want_nt:
            raise RuleInputError("normalized_time %d differs from admission's %d" % (nt, want_nt))
        self.facts[op] = {"hash": h, "fact": fact, "admitted": True, "frontier": frontier, "rkey": rkey}
        self.author_positions[(fact["author_id"], fact["author_seq"])] = op
        moved = self._place(op, rkey)
        self._insert_node(op, nt, frontier)
        for obj in self.pre_admit.pop(op, ()):
            # Rebuild: the occurrence mappings this fact's acceptance made, before its
            # effects (as the live acceptance had them; identity decisions before dependents).
            self._accept_decision_arrival({"decision": copy.deepcopy(obj)})
        outcome = self._apply(op, typ, parsed)
        if not recovery:
            # A recovered fact keeps its original receipt (PROTOCOL §6.1), which is not
            # part of the vector.
            self.outcomes[op] = outcome
        if op in self.qsteps:
            # PROTOCOL §7.6: the receipt's recorded sort values: made now when live,
            # else the original receipt's (a recovered fact's, or a replay's).
            if self._ctx["live"]:
                self._record_fact_values(op)
            else:
                given = (arrival.get("receipt") or {}).get("values")
                if given is None and self.fixed_values is not None:
                    given = self.fixed_values.get(op)
                if given:
                    self.recorded[op] = copy.deepcopy(given)
        self._after_placement(op, moved)

    def _receipt(self, arrival, recovery, op):
        """PROTOCOL §6.10 receipt order: the arrival's receipt position (epoch, seq, index, op_id)
        and, for a recovered fact or a replay of persisted receipts, its original outcome.

        CHOICE (vector format): without `receipt` the position is (epoch, n, 0), n the
        arrival's 1-based position in the case and epoch the account's current one (1 until
        a live receipt or epoch evidence moves it, PROTOCOL §6.11); a live acceptance comes after
        every archived receipt (a new commit), in the current epoch; a recovered one keeps
        its original position."""
        r = arrival.get("receipt")
        fixed = None
        if r is None:
            key = (self.epoch, self.narrivals, 0)
        else:
            allowed = {"epoch", "seq", "index"} | ({"outcome", "reason", "values"} if recovery else set())
            if not isinstance(r, dict) or set(r) - allowed or not {"epoch", "seq", "index"} <= set(r):
                raise RuleInputError("receipt has members %s (want epoch, seq, index%s)"
                                     % (sorted(r) if isinstance(r, dict) else r,
                                        "[, outcome, reason, values]" if recovery else ""))
            if "values" in r:
                # PROTOCOL §7.6: the canonical sort values the original receipt recorded.
                try:
                    _check_values(r["values"], "receipt.values")
                except _Invalid as e:
                    raise RuleInputError(str(e))
            for k in ("epoch", "seq", "index"):
                if not _is_plain_int(r[k]) or not 0 <= r[k] <= c.MAX_SAFE_INTEGER:
                    raise RuleInputError("receipt.%s is not a non-negative safe integer" % k)
            key = (r["epoch"], r["seq"], r["index"])
            if "outcome" in r:
                o = r["outcome"]
                if o not in OUTCOMES:
                    raise RuleInputError("receipt.outcome %r is not an outcome" % (o,))
                if (o in (NOT_APPLIED, REJECTED)) != ("reason" in r) or not isinstance(r.get("reason", ""), str):
                    raise RuleInputError("receipt.reason is present exactly for not_applied and rejected")
                fixed = _outcome(o, r.get("reason"))
            elif "reason" in r:
                raise RuleInputError("receipt.reason without an outcome")
        # PROTOCOL §6.11: positions are ordered by (epoch, seq, index, op_id), so two timelines'
        # equal positions still order totally (CHOICE: two archived facts may therefore
        # share (epoch, seq, index)).
        live = not recovery and not self.replay
        if live and r is not None and key[0] < self.epoch:
            raise RuleInputError("a live acceptance's receipt %s is before the account's epoch %d" % (key, self.epoch))
        key = key + (_bkey(op),)
        if live and self.max_rkey is not None and key < self.max_rkey:
            raise RuleInputError("a live acceptance's receipt %s precedes an archived receipt %s" % (key[:3], self.max_rkey[:3]))
        if live:
            self.epoch = max(self.epoch, key[0])
            self.high_water = max(self.high_water, key[0])
        if fixed is None and self.fixed_outcomes is not None:
            fixed = self.fixed_outcomes.get(op)
            if fixed is not None:
                fixed = dict(fixed)
        return key, fixed

    def _reject(self, op, h, fact, reason, frontier, arrival, rkey):
        # PROTOCOL §6.4: a rejected fact is archived, is not a causal node, its receipt's
        # normalized_time is the receive time and it carries its resolved frontier (empty
        # for an envelope rejection).
        if sorted(set(arrival["edges"]), key=_bkey) != list(frontier):
            raise RuleInputError("edges of a rejected fact must be its resolved frontier %s" % (frontier,))
        if arrival["normalized_time"] != arrival["receive_time"]:
            raise RuleInputError("a rejected fact's normalized_time is its receive_time")
        self._archive_rejected(op, h, fact, reason, list(frontier), rkey, record=True)

    def _archive_rejected(self, op, h, fact, reason, frontier, rkey, record):
        self.facts[op] = {"hash": h, "fact": fact, "admitted": False, "frontier": list(frontier), "rkey": rkey}
        if record:
            self.outcomes[op] = _outcome(REJECTED, reason)
        # PROTOCOL §6.10: a rejected fact holds its chain position and passes its author on.
        moved = self._place(op, rkey)
        self._after_placement(op, moved)

    # -- PROTOCOL §6.10: chain positions, derived authors, lineage -------------------

    def _place(self, op, rkey):
        """Places a newly archived fact in receipt order (PROTOCOL §6.10). Returns True when an
        earlier receipt arrived, so that every chain was recomputed."""
        self.rkeys.add(rkey)
        co = author_coordinates(self.facts[op]["fact"])
        if co is not None:
            self.coords[op] = co
        if self.max_rkey is None or rkey > self.max_rkey:
            self.max_rkey = rkey
            self._place_one(op)
            return False
        self._recompute_chains()
        return True

    def _place_one(self, op):
        co = self.coords.get(op)
        if co is None:
            return
        a, s, prev = co
        E = a
        # 1. The inherited author: through previous_op_id from an archived fact of the
        # same author_id placed before this one (in receipt order).
        if prev is not None and prev in self.eff and self.coords[prev][0] == a:
            E = self.eff[prev]
        self.inherited[op] = E
        if (E, s) not in self.held:
            # 2. The position is free: the fact holds it.
            self.held[(E, s)] = op
            self.eff[op] = E
        else:
            # 3. A later, different fact at a held position roots a derived branch whose
            # parent is the inherited author. CHOICE: for a root without a same-author
            # predecessor (for example two first facts of one author) the parent is the
            # raw author itself, which is its inherited author.
            D = derived_author(self.namespace, op)
            self.derived_parent[D] = E
            self.held[(D, s)] = op
            self.eff[op] = D

    def _recompute_chains(self):
        self.eff, self.inherited, self.held, self.derived_parent = {}, {}, {}, {}
        for op in sorted(self.coords, key=lambda x: self.facts[x]["rkey"]):
            self._place_one(op)
        self._recompute_forks()

    def _recompute_forks(self):
        """Each fork author's binding fork: its applied fork first in receipt order
        (CHOICE; admission allows only one, so several exist only after recovery)."""
        self.fork_of = {}
        for op in sorted(self.forks, key=lambda x: self.facts[x]["rkey"]):
            self.fork_of.setdefault(self.forks[op]["author"], op)

    def _after_placement(self, op, moved):
        """Refolds every episode when effective authors or lineage may have changed: an
        earlier receipt reinterpreted the chains (PROTOCOL §6.10 "Recovery is the same function"),
        or a fork's head arrived after its fork (recovery)."""
        if moved or op in self.fork_heads:
            for E in sorted(self.episodes):
                self._refresh_episode(E)

    def parent_author(self, x):
        """PROTOCOL §6.10 lineage: a derived author's parent is its root's inherited author; a
        local-detection author's parent is the effective author of its fork's head."""
        if x in self.derived_parent:
            return self.derived_parent[x]
        fk = self.fork_of.get(x)
        if fk is not None:
            return self.eff.get(self.forks[fk]["head"])
        return None

    def descends(self, z, a):
        """True if author `z` strictly descends from author `a` in the lineage."""
        seen = set()
        x = self.parent_author(z)
        while x is not None and x not in seen:
            if x == a:
                return True
            seen.add(x)
            x = self.parent_author(x)
        return False

    def in_lineage(self, z, a):
        return z == a or self.descends(z, a)

    def effective_author(self, op):
        return self.eff.get(op)

    def _fork_rejection(self, op, fact, parsed, rkey, lenient):
        """PROTOCOL §6.10 admission of `installation.fork`: `author` is not the fork's own author F,
        F has no earlier fact, and `head` is a fact of `author` among the dependencies.

        CHOICE: "no earlier fact" counts every archived fact of F (admitted or rejected)
        ranked before the fork in receipt order; a fork carrying a previous_op_id is invalid
        too (it must be F's first fact). With `lenient` (a recovered fact or a persisted
        receipt) a head not archived yet is accepted: the lineage waits for it."""
        F = fact["author_id"]
        if "previous_op_id" in fact or parsed["author"] == F:
            return R_INVALID_PAYLOAD
        for x, info in self.facts.items():
            if x != op and isinstance(info["fact"], dict) and info["fact"].get("author_id") == F and info["rkey"] < rkey:
                return R_INVALID_PAYLOAD
        head = parsed["head"]
        if head not in fact["dependencies"]:
            return R_INVALID_PAYLOAD
        hf = self.facts.get(head)
        if hf is None:
            return None if lenient else R_INVALID_PAYLOAD
        if hf["fact"].get("author_id") != parsed["author"]:
            return R_INVALID_PAYLOAD
        return None

    def _apply_fork(self, op, parsed):
        """An applied fork records the lineage F -> the effective author of `head`; it
        contributes to no scope (no records, PROTOCOL §6.10)."""
        F = self.facts[op]["fact"]["author_id"]
        self.forks[op] = {"author": F, "wire": parsed["author"], "head": parsed["head"]}
        self.fork_heads.setdefault(parsed["head"], []).append(op)
        self._recompute_forks()
        for E in sorted(self.episodes):
            self._refresh_episode(E)
        return _outcome(APPLIED)

    def _chain_prev(self, op):
        """The previous fact of `op` in its library's own writing history: its
        previous_op_id, or for an applied fork its head (PROTOCOL §6.10: the copy's writing tip)."""
        if op in self.forks:
            h = self.forks[op]["head"]
            return h if h in self.facts else None
        f = self.facts[op]["fact"]
        p = f.get("previous_op_id") if isinstance(f, dict) else None
        return p if isinstance(p, str) and p in self.facts else None

    def _chain_walk(self, op, limit=None):
        """The writing history before `op`, newest first."""
        seen = {op}
        x = self._chain_prev(op)
        while x is not None and x not in seen:
            seen.add(x)
            yield x
            if limit is not None and x == limit:
                return
            x = self._chain_prev(x)

    # -- handlers ---------------------------------------------------------------

    def _apply(self, op, typ, parsed):
        if typ in ("podcast.subscribe", "podcast.register_preview"):
            return self._apply_subscribe(op, parsed, typ == "podcast.register_preview")
        if typ.startswith("podcast."):
            return self._apply_podcast_op(op, typ, parsed)
        if typ == "account.set_discovery_policy":
            self._add(("account",), _Contribution(op, "discovery", None, {"action": parsed["action"]}))
            fold = self._refresh_account()
            return self._outcome_from(op, [fold])
        if typ == "account.set_queue_policy":
            self._add(("account",), _Contribution(op, "queue_policy", None, dict(parsed)))
            fold = self._refresh_account()
            return self._outcome_from(op, [fold])
        if typ in LISTENING_TYPES:
            return self._apply_listening(op, typ, parsed)
        if typ in QUEUE_TYPES:
            return self._apply_queue(op, typ, parsed)
        if typ == FORK:
            return self._apply_fork(op, parsed)
        if typ == OBSERVE:
            return self._apply_observe(op, parsed)
        if typ == PROPOSE:
            return self._apply_propose(op, parsed)
        if typ == LEAVE:
            return self._apply_leave(op, parsed)
        return self._apply_episode_op(op, typ, parsed)

    # -- PROTOCOL §9.4: observations and proposals ------------------------------------

    def _apply_observe(self, op, parsed):
        """PROTOCOL §9.4 `catalog.observe`: catalog evidence, accepted whatever the subscription
        state. Each record is an observation at the fact's place, with its stated base and
        the fetch time clamped to the fact's normalized time (PROTOCOL §9.4).

        CHOICE: the podcast must be known (else not_applied / unknown_podcast); an item
        whose episode is bound to another podcast is skipped like a bulk member
        (episode_podcast_mismatch), and the fact is not_applied only if no record remains;
        a passing item registers its episode. An accepted
        observation's receipt is `applied`, whether or not it became a current revision."""
        P = parsed["podcast_id"]
        if not self._known_podcast(P):
            return _outcome(NOT_APPLIED, R_UNKNOWN_PODCAST)
        ft = min(parsed["fetch_time"], self.node_time[op])
        # CHOICE: an observation naming a merged-away podcast applies its items to the
        # survivor's episodes (by the merge's mappings, or the survivor's rules), but its
        # feed record stays with the podcast it names (hidden once merged), so the result
        # does not depend on whether the merge was admitted before it.
        Pr = self._podcast_root(P)
        keys, first_reason = [], None
        if parsed["feed"] is not None:
            f = parsed["feed"]
            keys.append(("meta", "podcast", P))
            self._add(keys[-1], _Contribution(op, "obs", None, {"base": f["base"], "fields": f["fields"], "fetch_time": ft}))
        # Two passes (CHOICE): first every item's identity evidence and the occurrence
        # mappings it makes, then the items' effects, so that the effects see every mapping
        # the observation made, as a rebuild admitting them before its effects does
        # ("identity decisions before dependents").
        passing = []
        for x in parsed["items"]:
            ref = x["ref"]
            E = ref["episode_id"]
            b = self._binding(E)
            if b is not None and b["podcast_id"] != P:
                first_reason = first_reason or R_MISMATCH
                continue
            # PROTOCOL §9.7: the item's identity evidence is the observed record (CHOICE: its
            # EpisodeRef's GUID with the observed enclosure and publication date, the
            # values a server fetch would record); it counts only for the plain derivation
            # of that occurrence or the episode the server's rules give it.
            media = x["fields"]["media"]
            occ3 = (self._occ3(ref["occurrence"])[0][0], self._norm(media["url"]) if media is not None else None,
                    x["fields"]["pub_date"])
            kfam = self._occ3(ref["occurrence"])[1]
            if Pr != P:
                E0 = self.root.get(E) or self.pmerged[P]["episodes"].get(E) or self._derive(Pr, occ3, kfam) or E
            else:
                E0 = self._root(E)
            counts = E0 in self._plain_ids(Pr, occ3, kfam) or E0 == self._derive(Pr, occ3, kfam)
            if counts and self._record(Pr, occ3, op, kfam):
                self._detect(Pr, occ3, kfam, op, [op])
            passing.append((x, counts, occ3, kfam))
        for x, counts, occ3, kfam in passing:
            ref = x["ref"]
            E = ref["episode_id"]
            if Pr != P:
                E0 = self.root.get(E) or self.pmerged[P]["episodes"].get(E) or self._derive(Pr, occ3, kfam) or E
            else:
                E0 = self._root(E)
            Er = self._route_identity(Pr, self._root(E0), occ3, kfam)
            if counts:
                self._set_evidence_episode(Pr, occ3, Er)
            self._register(E, P, ref["occurrence"], op)
            self._register(Er, Pr, ref["occurrence"], op)
            if Pr != P:
                # As _check_ref: a source episode routed through a podcast merge is merged.
                self._walk_merges(E, P, ref["occurrence"], occ3, kfam, op)
            keys.append(("meta", "episode", Er))
            self._add(keys[-1], _Contribution(op, "obs", None, {"base": x["base"], "fields": x["fields"], "fetch_time": ft},
                                              orig=E, occ=occ3, ref=(E, P, None, kfam)))
            self._note_observation(op, Pr, Er, ft, x["fields"])
        if not keys:
            return _outcome(NOT_APPLIED, first_reason)
        self._refresh_podcast(Pr)
        for k in keys:
            if k[1] == "episode" and k[2] in self.episodes:
                self._refresh_episode(k[2])
        for x in parsed["items"]:
            if x["ref"]["episode_id"] in self.episodes:
                self._refresh_episode(x["ref"]["episode_id"])
        return _outcome(APPLIED)

    def _apply_propose(self, op, parsed):
        """PROTOCOL §9.4 `discovery.propose`: PROTOCOL §7.2 checks, then not_subscribed,
        not_playable, ineligible, already_admitted; the first eligible proposal for
        (G, E) is admitted by a `discovery.admission` decision whose filing acts at this
        fact's place. The bundle's moved episodes are checked like a play start's (PROTOCOL §7.6): right after the discovered episode passes, a failing one is
        skipped.

        Eligibility (PROTOCOL §9.5): the live generation is subscribed (state now, at acceptance);
        the episode is playable (an accepted observation of it with an enclosure, any
        source); and the date rule holds on the proposal's own evidence (pub_date or
        newest_before_batch null, or pub_date >= newest_before_batch: CHOICE, Android's
        "not before") or the gained-media evidence is valid. CHOICE: valid evidence is an
        accepted observation (a catalog.observe fact or a catalog.refresh part) that saw
        this episode without media; no order between it and the playable observation is
        required."""
        ref = parsed["refs"][0]
        P, E, G = ref["podcast_id"], ref["episode_id"], ref["generation_id"]
        reason = self._check_ref(ref, op)
        bundle = parsed["bundle"]
        if E in self.episodes:
            self._refresh_episode(E)
        if reason is not None:
            return _outcome(NOT_APPLIED, reason)
        if self._fold_podcast(P).records["subscription"]["state"] != "subscribed":
            return _outcome(NOT_APPLIED, R_NOT_SUBSCRIBED)
        item = self.catalog_items.get(E)
        if item is None or item["playable"] is None:
            return _outcome(NOT_APPLIED, R_NOT_PLAYABLE)
        pub, newest = parsed["pub_date"], parsed["newest"]
        date_ok = pub is None or newest is None or pub >= newest
        gained_ok = parsed["gained"] is not None and self.obs_media.get((parsed["gained"], E)) is False
        if not date_ok and not gained_ok:
            return _outcome(NOT_APPLIED, R_INELIGIBLE)
        if self.admitted.get((G, E)):
            return _outcome(NOT_APPLIED, R_ALREADY_ADMITTED)
        if self._ctx is None or not self._ctx["live"]:
            # Recovery and replays decide nothing: the admission arrives as a decision.
            return _outcome(APPLIED)
        action = parsed["action"]
        # CHOICE: the server resolves the placement whenever a queue proposal has no
        # bundle (a client without the queue capability; a queue client's proposal
        # without one is accepted too, not rejected). CHOICE: `source` is the proposal's
        # wire author_id (immutable; the effective author may be reinterpreted later).
        resolve = action == "queue" and bundle is None
        result = {"episode_id": E, "generation_id": G, "action": action, "proposal_id": op,
                  "source": self.facts[op]["fact"]["author_id"]}
        inputs = {"found_by": op}
        self._make_admission(E, G, action, resolve, bundle, inputs, result, [op])
        return _outcome(APPLIED)

    def _make_admission(self, E, G, action, resolve, bundle, inputs, result, deps):
        """Creates the admission, with, for a queue filing, its placement: the bundle's
        (key, position ID and the place the device resolved), or, when the source sent no
        bundle (a device without the queue capability, or the server's own discovery), the
        server's resolution from the account's enqueue location (PROTOCOL §9.5: `place` in the decision), and the canonical sort values it recorded (PROTOCOL §7).
        The filing is a step of the queue replay at its place (the proposal, or the
        server's admission itself); under keep-sorted the replay sorts there (the
        `queue.sort_result` is retired).

        CHOICE: the result records `key`, `position_id`, `place` and `values` for every
        queue admission, and inputs.enqueue_location exactly when resolved. CHOICE: the
        resolution happens whether or not the add will apply. CHOICE: the admission's
        dependencies are only what found the episode."""
        info = {"resolve": resolve}
        if resolve:
            loc, cur = self._policy_now()["enqueue_location"], self._current_queue()
            info.update(random=loc == "random", size=len(cur))
        ch = self._choice(ADMISSION, info)
        if action == "queue":
            if resolve:
                inputs["enqueue_location"] = loc
                result["place"], result["key"] = self._resolve_place(loc, cur, ch)
                result["position_id"] = ch["position_id"]
            else:
                result["key"], result["position_id"] = bundle["key"], bundle["pid"]
                result["place"] = self._place_value(bundle["place"])
            place_node = result["proposal_id"] if result["proposal_id"] is not None else None
            result["values"] = self._values_list([(E, G)], place_node)
        return self._new_decision(ADMISSION, "admission:%s:%s" % (G, E), ch, inputs, result, deps)

    # -- PROTOCOL §9.4: the server's own fetches (an input stream) ----------------------

    def _new_refresh_id(self, R):
        if not c.is_uuid(R):
            raise RuleInputError("refresh_id is not a UUID")
        if R in self.refresh_ids or R in self.prio or R in self.facts:
            raise RuleInputError("refresh_id %s is already used" % R)
        self.refresh_ids.add(R)

    def _capture(self, P):
        """What a fetch captures when it starts, before its I/O (PROTOCOL §9.6): the
        generation it starts under (the live one; CHOICE: else the latest, which only
        names a move's subject), the node that determines that generation's refresh URL,
        and the canonical revision of every catalog record of the podcast (its bases)."""
        live = self._live_generation(P)
        G = live if live is not None else self._latest_generation(P)
        url_det = self._fold_podcast(P).det["identity.refresh_url"] if live is not None else None
        bases = {}
        keys = [("meta", "podcast", P)] + [("meta", "episode", E) for E, ep in self.episodes.items()
                                           if ep["podcast_id"] == P]
        for k in keys:
            rev = self._fold_meta(k)[1]
            if rev is not None:
                bases[k] = rev
        return {"podcast_id": P, "generation": G, "live": live is not None, "url_det": url_det, "bases": bases}

    def _accept_fetch_start(self, arrival):
        """`{"fetch_start": {refresh_id, podcast_id}}`: a server fetch starts (CHOICE: its
        own arrival, so that what it captures is the state at that point of the stream).
        One fetch per account and podcast at a time (PROTOCOL §9.6 coalescing)."""
        if self.replay:
            raise RuleInputError("a replay receives a fetch's decisions, not the fetch")
        if set(arrival) != {"fetch_start"}:
            raise RuleInputError("a fetch_start arrival has only `fetch_start`")
        fs = arrival["fetch_start"]
        if not isinstance(fs, dict) or set(fs) != {"refresh_id", "podcast_id"}:
            raise RuleInputError("fetch_start has exactly refresh_id and podcast_id")
        P = fs["podcast_id"]
        if not isinstance(P, str) or not self._known_podcast(P) or P in self.pmerged:
            raise RuleInputError("the server fetches only registered podcasts that are not merged away")
        if any(x["podcast_id"] == P for x in self.fetch_pending.values()):
            raise RuleInputError("one fetch per account and podcast at a time")
        self._new_refresh_id(fs["refresh_id"])
        self.fetch_pending[fs["refresh_id"]] = self._capture(P)

    def _check_fetch(self, fx, rt):
        """The shape of a fetch arrival's `fetch` (docs/CONTRACT.md "Server fetches").
        The server strips URL userinfo from every string of the
        parsed result, URLs and text alike (CHOICE: GUIDs and the trace's URLs too)."""
        if not isinstance(fx, dict):
            raise RuleInputError("fetch is not an object")
        fx = _strip_all(fx)
        o = fx.get("outcome")
        common = ("refresh_id", "podcast_id", "outcome")
        try:
            if o == "success":
                _members(fx, common + ("fetch_time", "feed", "items"),
                         ("redirects", "moved_to", "new_feed_url", "next_attempt"), "fetch")
            elif o == "failure":
                _members(fx, common + ("fetch_time", "error"), ("next_attempt",), "fetch")
            elif o == "re-parse":
                _members(fx, common + ("reparse_of", "feed", "items"), (), "fetch")
            else:
                raise _Invalid("fetch.outcome is not one of %s" % "|".join(FETCH_OUTCOMES))
            out = {"refresh_id": fx["refresh_id"], "podcast_id": fx["podcast_id"], "outcome": o}
            if o != "re-parse":
                _int(fx["fetch_time"], "fetch.fetch_time")
                if fx["fetch_time"] > rt:
                    raise _Invalid("a fetch's fetch_time is at most its receive_time")
                out["fetch_time"] = fx["fetch_time"]
                out["next_attempt"] = fx.get("next_attempt")
                if out["next_attempt"] is not None:
                    _int(out["next_attempt"], "fetch.next_attempt")
            if o == "failure":
                e = fx["error"]
                _members(e, ("label", "code", "detail"), (), "fetch.error")
                _str(e["label"], "fetch.error.label")
                if e["code"] is not None:
                    _int(e["code"], "fetch.error.code")
                _opt_str(e["detail"], "fetch.error.detail")
                out["error"] = copy.deepcopy(e)
                return out
            if o == "re-parse":
                _uuid(fx["reparse_of"], "fetch.reparse_of")
                out["reparse_of"] = fx["reparse_of"]
            P = fx["podcast_id"]
            _podcast_id(P, "fetch.podcast_id")
            out["feed"] = feed_fields(fx["feed"], "fetch.feed")
            if not isinstance(fx["items"], list):
                raise _Invalid("fetch.items is not an array")
            out["items"] = []
            for i, it in enumerate(fx["items"]):
                w = "fetch.items[%d]" % i
                _members(it, ("occurrence", "fields"), (), w)
                occ = it["occurrence"]
                _members(occ, ("k", "v"), ("guid", "enclosure_url", "title", "publication_time"), w + ".occurrence")
                _enum(occ["k"], c.EPISODE_KINDS, w + ".occurrence.k")
                _str(occ["v"], w + ".occurrence.v")
                fields = item_fields(it["fields"], w + ".fields")
                # CHOICE: the server derives an item's episode from what it parsed
                # (PROTOCOL §3.3, §9.7): `k` gives the GUID kind (atom-id, else rss-guid;
                # rss-guid+enclosure is read as rss-guid: disputes are the server's), the
                # occurrence's `guid` the GUID, the record's media URL and pub_date the
                # enclosure and date; `v` and the other occurrence members are not used.
                # An item without GUID and enclosure has nothing to play and is not in a fetch.
                g = c.guid_trim(occ["guid"]) if (occ["k"] != "enclosure" and isinstance(occ.get("guid"), str)) else None
                if occ["k"] != "enclosure" and g is None:
                    raise _Invalid("%s: a GUID kind without a GUID" % w)
                media = fields["media"]
                if g is None and media is None:
                    raise _Invalid("%s has neither GUID nor enclosure" % w)
                kfam = "atom-id" if occ["k"] == "atom-id" else "rss-guid"
                built = {"k": kfam if g is not None else "enclosure",
                         "v": g if g is not None else c.normalize_url(media["url"])}
                if g is not None:
                    built["guid"] = g
                if media is not None:
                    built["enclosure_url"] = media["url"]
                if fields["title"] is not None:
                    built["title"] = fields["title"]
                if fields["pub_date"] is not None:
                    built["publication_time"] = fields["pub_date"]
                out["items"].append({"occurrence": built, "kfam": kfam, "fields": fields,
                                     "occ3": (g, self._norm(media["url"]) if media is not None else None,
                                              fields["pub_date"])})
            if o == "success":
                rds = fx.get("redirects", [])
                if not isinstance(rds, list):
                    raise _Invalid("fetch.redirects is not an array")
                for i, rd in enumerate(rds):
                    _members(rd, ("status", "location"), (), "fetch.redirects[%d]" % i)
                    _int(rd["status"], "fetch.redirects[%d].status" % i, 300)
                    _str(rd["location"], "fetch.redirects[%d].location" % i)
                out["redirects"] = copy.deepcopy(rds)
                for f in ("moved_to", "new_feed_url"):
                    _opt_str(fx.get(f), "fetch." + f)
                    out[f] = fx.get(f)
        except _Invalid as e:
            raise RuleInputError("malformed fetch: %s" % e)
        except c.ContractError as e:
            raise RuleInputError("malformed fetch: %s" % e)
        return out

    def _accept_fetch(self, arrival, chooser):
        """`{"fetch": {...}, "receive_time", "decisions"?}`: a server fetch's result applied
        in one server-originated commit (PROTOCOL §9.6): its `catalog.refresh` parts, admissions
        (in item order) and move, then the commit's layout, made at `receive_time`
        (the server's now). Without an earlier fetch_start of its refresh ID the fetch
        starts at its own arrival. The server's choices come from `decisions` (or the
        chooser), in creation order."""
        if self.replay:
            raise RuleInputError("a replay receives a fetch's decisions, not the fetch")
        if set(arrival) - {"fetch", "receive_time", "decisions"} or "receive_time" not in arrival:
            raise RuleInputError("a fetch arrival has fetch, receive_time and optionally decisions")
        rt = arrival["receive_time"]
        if not _is_plain_int(rt) or abs(rt) > c.MAX_SAFE_INTEGER:
            raise RuleInputError("receive_time is not a safe integer")
        supply = arrival.get("decisions")
        if supply is not None and chooser is not None:
            raise RuleInputError("decisions are supplied only for acceptances replayed from a vector")
        if supply is not None and (not isinstance(supply, list) or not all(isinstance(x, dict) for x in supply)):
            raise RuleInputError("decisions must be an array of objects")
        fx = self._check_fetch(arrival["fetch"], rt)
        R, P = fx["refresh_id"], fx["podcast_id"]
        if not isinstance(P, str) or not self._known_podcast(P):
            raise RuleInputError("the server fetches only registered podcasts")
        if P in self.pmerged:
            raise RuleInputError("the server does not fetch a merged-away podcast (PROTOCOL §9.7)")
        cap = self.fetch_pending.pop(R, None) if isinstance(R, str) else None
        if cap is not None:
            if cap["podcast_id"] != P or fx["outcome"] == "re-parse":
                raise RuleInputError("a fetch completes the fetch_start of its podcast (a re-parse has none)")
        else:
            self._new_refresh_id(R)
            if any(x["podcast_id"] == P for x in self.fetch_pending.values()):
                raise RuleInputError("one fetch per account and podcast at a time")
            cap = self._capture(P)
        self._ctx = {"arrival": arrival, "live": True, "receive_time": rt, "chooser": chooser,
                     "supply": list(supply or []), "caps": [], "fixed": None, "server": True}
        try:
            if fx["outcome"] == "success":
                self._fetch_success(R, P, fx, cap)
            elif fx["outcome"] == "failure":
                self._fetch_failure(R, P, fx, cap)
            else:
                self._fetch_reparse(R, P, fx)
            # PROTOCOL §7.6: the publication rule ends the fetch's commit too.
            self._publish_queue()
            if self._ctx["supply"]:
                raise RuleInputError("the arrival supplies %d unused decision choice(s)" % len(self._ctx["supply"]))
        finally:
            self._ctx = None

    def _status_due(self, P, cls, status):
        """The refresh status (PROTOCOL §9.6). CHOICE: due when nothing was published yet, the outcome class
        (success or failure) or the error object differs from the last published status,
        or that status's last attempt is at least a day older than this attempt."""
        lg = self.status_logged.get(P)
        return (lg is None or lg["class"] != cls or lg["error"] != status["error"]
                or status["last_attempt"] - lg["last_attempt"] >= STATUS_INTERVAL_MS)

    def _changes(self, P, fx):
        """The parse's records against the canonical ones now (CHOICE: a record "differs
        from canonical" at the fetch's application, values compared without revision and
        fetch time). Returns (feed changed, kept items, changed items, the current revision
        of each record). CHOICE: an item whose episode is bound to another podcast is
        skipped (no record, no discovery)."""
        cur_revs = {}
        pkey = ("meta", "podcast", P)
        cur, rev = self._fold_meta(pkey)
        cur_revs[pkey] = rev
        feed_changed = cur is None or _strip_meta(cur) != fx["feed"]
        kept, changed = [], []
        for it in fx["items"]:
            E = it["episode_id"]
            b = self._binding(E)
            if b is not None and b["podcast_id"] != P:
                continue
            kept.append(it)
            k = ("meta", "episode", E)
            cur, rev = self._fold_meta(k)
            cur_revs[k] = rev
            if cur is None or _strip_meta(cur) != it["fields"]:
                changed.append(it)
        return feed_changed, kept, changed, cur_revs

    def _write_parts(self, P, R, outcome, G, ft, feed, changed, bases, part0_extra, start=0):
        """Writes the catalog.refresh parts of one fetch: the first part written holds
        `part0_extra` (part 0: status, population, seen; a later first part: seen) and, as
        part 0, the feed record; the changed items follow in parts of at most PART_ITEMS
        (CHOICE: the first part carries the first ones). A part depends on the revisions its
        records replace and the podcast's identity node (its creating fact). Returns (the
        first part's ID, {episode: its part's ID}, {record key: part ID})."""
        creating = self.podcasts[P]["creating_op"]
        chunks = [changed[i:i + PART_ITEMS] for i in range(0, len(changed), PART_ITEMS)] or [[]]
        part0, part_of, written = None, {}, {}
        for n, chunk in enumerate(chunks, start):
            deps = {creating}
            result = {}
            if n == start:
                result.update(copy.deepcopy(part0_extra))
            if n == 0:
                if feed is not None:
                    b = bases.get(("meta", "podcast", P))
                    result["feed"] = {"base": b, "fields": copy.deepcopy(feed)}
                    if b is not None:
                        deps.add(b)
            result["items"] = []
            for it in chunk:
                b = bases.get(("meta", "episode", it["episode_id"]))
                result["items"].append({"episode_id": it["episode_id"], "occurrence": copy.deepcopy(it["occurrence"]),
                                        "base": b, "fields": copy.deepcopy(it["fields"])})
                if b is not None:
                    deps.add(b)
            inputs = {"podcast_id": P, "refresh_id": R, "part": n, "outcome": outcome, "generation_id": G,
                      "fetch_time": ft}
            ch = self._choice(CATALOG_REFRESH, {})
            obj = self._new_decision(CATALOG_REFRESH, "refresh:%s:%s:%d" % (P, R, n), ch, inputs, result, deps)
            did = obj["decision_id"]
            if n == start:
                part0 = did
            if n == 0 and feed is not None:
                written[("meta", "podcast", P)] = did
            for it in chunk:
                part_of[it["episode_id"]] = did
                written[("meta", "episode", it["episode_id"])] = did
        return part0, part_of, written

    def _history_changes(self, P, kept, changed):
        """The episodes of kept items whose
        server fetch history this fetch changes (new to it, or catalog-only there and now
        with an enclosure) without a changed record in any part. Listed in part 0 as `seen`
        (sorted), which they force, so the history is recoverable from decisions alone."""
        seen = self.server_seen.get(P, {})
        written = set(it["episode_id"] for it in changed)
        out = []
        for it in kept:
            E, prev = it["episode_id"], seen.get(it["episode_id"])
            if E in written:
                continue
            if prev is None or (not prev["playable"] and it["fields"]["media"] is not None):
                out.append(E)
        return sorted(set(out), key=_bkey)

    def _update_history(self, P, items, ft):
        """The server's own fetch history (PROTOCOL §9.6: catalog items record which items the
        server's fetches have seen). CHOICE: `playable` records whether any server fetch
        saw the item with an enclosure (a regained enclosure after a loss is not a
        catalog-only transition). A server sighting is a dated observation of the catalog
        item too (its effective date)."""
        seen = self.server_seen.setdefault(P, {})
        for it in items:
            E, f = it["episode_id"], it["fields"]
            prev = seen.get(E)
            seen[E] = {"playable": f["media"] is not None or (prev is not None and prev["playable"])}
            ci = self.catalog_items.get(E)
            if ci is not None and f["pub_date"] is not None:
                ci["date"] = f["pub_date"]

    def _server_action(self, P):
        """The podcast's new-episodes action as the server knows it at application, with
        `global` resolved through the account default (Android's default `inbox`)."""
        a = (self._fold_podcast(P).records.get("policy") or {}).get("new_episodes_action", "global")
        if a == "global":
            d = self._fold_account().records.get("discovery_policy")
            a = d["new_episodes_action"] if d is not None else DEFAULT_DISCOVERY_ACTION
        return a

    def _fetch_success(self, R, P, fx, cap):
        """A successful fetch (PROTOCOL §9.6).

        * Effects on the generation it started under, only while it is live: the
          population marker on its first successful fetch (whatever its subscription
          state: a preview's fetches are its first population), admissions only while
          it is subscribed and its first population is done.
        * Discoveries against the server's own fetch history (PROTOCOL §9.5): an item new to it, or
          that it knew as catalog-only and that now has an enclosure (the gained-media
          exception: no date rule); the date rule against the newest effective date of the
          items the history held before this fetch (each item's latest dated observation,
          any source). CHOICE: with an empty history that date is the fetch time (Android's
          `new Date()` when the feed has no item yet).
        * The move and the refresh status.
        Part 0 is written when anything is: a changed record, the population marker, an
        admission (CHOICE: an admission of an unchanged item depends on part 0), a move,
        or a due status; part 0 always carries the current status."""
        ft, G = fx["fetch_time"], cap["generation"]
        live_now = self._live_generation(P)
        g_live = cap["live"] and G == live_now
        subscribed = g_live and self._fold_podcast(P).records["subscription"]["state"] == "subscribed"
        first_pop = g_live and G not in self.populated
        status = {"last_attempt": ft, "last_success": ft, "error": None, "next_attempt": fx["next_attempt"]}
        pcur = self._fold_meta(("meta", "podcast", P))[0]
        early_feed = pcur is None or _strip_meta(pcur) != fx["feed"]
        early = {}

        def part0_for_identity():
            # When the fetch makes identity decisions (a GUID-change match, a new release),
            # part 0 (status, population, the feed) is written before them, and the fetch's
            # items follow in the next parts.
            if "part0" in early:
                return
            extra = {"status": status}
            if first_pop:
                extra["population"] = True
            p0, _, w = self._write_parts(P, R, "success", G, ft, fx["feed"] if early_feed else None, [], cap["bases"],
                                         extra)
            early.update(part0=p0, written=w)
            self.status_logged[P] = {"class": "success", "error": None, "last_attempt": ft}
        # GUID-change matching on this complete fetch, then each item's evidence (and
        # disputes), then its derivation.
        self._ctx["before_identity"] = part0_for_identity
        try:
            self._identify_items(R, P, fx["items"], detect=True)
        finally:
            self._ctx["before_identity"] = None
        feed_changed, kept, changed, cur_revs = self._changes(P, fx)
        seen = self.server_seen.get(P, {})
        # The newest effective date (any source's latest dated
        # observation) over the items the server's fetches have seen.
        newest = max((self.catalog_items[E]["date"] for E in seen if E in self.catalog_items), default=ft)
        cands = []
        if subscribed and not first_pop:
            for it in kept:
                E, f = it["episode_id"], it["fields"]
                if f["media"] is None:
                    continue
                prev = seen.get(E)
                if prev is None:
                    ok = f["pub_date"] is None or f["pub_date"] >= newest
                else:
                    ok = not prev["playable"]
                if ok and not self.admitted.get((G, E)):
                    cands.append(E)
        aliases, norms = [], set()
        hops = [(rd["location"], "redirect") for rd in fx["redirects"] if rd["status"] in PERMANENT_REDIRECTS]
        if fx["new_feed_url"] is not None:
            hops.append((fx["new_feed_url"], "new_feed_url"))
        for url, src in hops:
            norm = c.normalize_url(url)
            if norm in norms:
                continue
            norms.add(norm)
            if norm not in self.aliases:
                aliases.append({"url": url, "normalized": norm, "source": src})
        refresh_url = None
        target = fx["new_feed_url"] if fx["new_feed_url"] is not None else fx["moved_to"]
        if target is not None and g_live:
            pf = self._fold_podcast(P)
            # The refresh URL moves even onto another podcast's alias (identity reconciliation
            # then merges the two in this fetch). CHOICE: not when it equals the current one,
            # normalized.
            if (pf.det["identity.refresh_url"] == cap["url_det"]
                    and c.normalize_url(target) != c.normalize_url(pf.records["identity"]["refresh_url"])):
                refresh_url = target
        move = refresh_url is not None or bool(aliases)
        # PROTOCOL §9.7: a 301/308 hop, the move or a fetched new-feed-url landing on another
        # podcast's alias merges the two podcasts in this commit (CHOICE: the first such URL
        # in trace order, then moved_to, then new_feed_url; it forces part 0).
        other, landing = None, None
        for url in ([rd["location"] for rd in fx["redirects"] if rd["status"] in PERMANENT_REDIRECTS]
                    + [x for x in (fx["moved_to"], fx["new_feed_url"]) if x is not None]):
            a = self.aliases.get(c.normalize_url(url))
            if a is not None and self._podcast_root(a["podcast_id"]) != P:
                other, landing = self._podcast_root(a["podcast_id"]), c.normalize_url(url)
                break
        due = self._status_due(P, "success", status)
        seen_ids = self._history_changes(P, kept, changed)
        part0, part_of, written = None, {}, {}
        if "part0" in early:
            # Part 0 went first; the items and `seen` follow in parts 1.. (a part with no item
            # is written just to carry `seen`; only one part carries it).
            part0, written = early["part0"], dict(early["written"])
            if changed or seen_ids:
                _, part_of, w = self._write_parts(P, R, "success", G, ft, None, changed, cap["bases"],
                                                  {"seen": seen_ids} if seen_ids else {}, start=1)
                written.update(w)
        elif feed_changed or changed or first_pop or cands or move or due or other is not None or seen_ids:
            extra = {"status": status}
            if first_pop:
                extra["population"] = True
            if seen_ids:
                extra["seen"] = seen_ids
            part0, part_of, written = self._write_parts(P, R, "success", G, ft, fx["feed"] if feed_changed else None,
                                                        changed, cap["bases"], extra)
            self.status_logged[P] = {"class": "success", "error": None, "last_attempt": ft}
        self._update_history(P, kept, ft)
        prev_queue = None
        for E in cands:
            # Each server admission is its own filing place, in the fetch's item order; its
            # action is the policy as the server knows it now. CHOICE (PROTOCOL §7.6: the
            # replay orders places by rank, and admissions of one fetch are concurrent at one
            # time): a queue filing depends on the fetch's previous queue filing, so the
            # replay keeps the fetch's item order, as one device adding them would.
            action = self._server_action(P)
            found = part_of.get(E, part0)
            result = {"episode_id": E, "generation_id": G, "action": action, "proposal_id": None, "source": "server"}
            deps = [found] + ([prev_queue] if action == "queue" and prev_queue is not None else [])
            obj = self._make_admission(E, G, action, action == "queue", None, {"found_by": found}, result, deps)
            if action == "queue":
                prev_queue = obj["decision_id"]
        if move:
            ch = self._choice(MOVED, {})
            self._new_decision(MOVED, "moved:%s:%s" % (G, R), ch, {"podcast_id": P, "generation_id": G, "refresh_id": R},
                               {"refresh_url": refresh_url, "aliases": aliases}, [part0])
        revs = dict(cur_revs)
        revs.update(written)
        self.stored[P] = {"refresh_id": R, "fetch_time": ft, "revs": revs}
        self.status_live[P] = status
        if other is not None:
            self._make_podcast_merge(R, P, other, landing, part0)

    def _identify_items(self, R, P, items, detect):
        """Identity reconciliation on a fetch's items: with `detect` (a successful fetch), GUID-change matching
        (PROTOCOL §9.7), then every item's evidence, then the disputes, release groups and
        enclosure additions that evidence makes (PROTOCOL §9.7; in item order), and only then
        every item's episode by the server's derivation (PROTOCOL §9.7). All evidence and disputes of a fetch are settled
        before any item is derived (the two-pass observation, extended to disputes), so a
        newest-first feed reusing a GUID derives each release to its own episode. A
        re-parse only derives (CHOICE: it records no evidence of its own here, matches
        nothing and disputes nothing; its parts' items are evidence when admitted, like any
        part's).

        Repeated items (PROTOCOL §15 item 6): the server keeps
        the first item per derived episode; the later ones write no record and are not
        discovered. CHOICE: their evidence still counts (it was observed)."""
        creating = self.podcasts[P]["creating_op"]
        if detect:
            self._guid_changes(R, P, items, creating)
            new = [it for it in items if self._record(P, it["occ3"], None, it["kfam"])]
            for it in new:
                self._detect(P, it["occ3"], it["kfam"], None, [creating])
        kept, seen = [], set()
        for it in items:
            occ3, kfam = it["occ3"], it["kfam"]
            E = self._derive(P, occ3, kfam)
            if detect:
                self._set_evidence_episode(P, occ3, E)
            it["episode_id"] = E
            if E not in seen:
                seen.add(E)
                kept.append(it)
        items[:] = kept

    def _guid_changes(self, R, P, items, creating):
        """PROTOCOL §9.7: old candidates are the podcast's roots the server's earlier
        fetches saw that no item of this complete fetch resolves to; new candidates are this
        fetch's items with a GUID that resolve to an episode new to that history (a phone's
        included). A pair matches on identical non-empty titles and identical non-null
        publication dates to the second (CHOICE: an old root's title and date are its
        canonical metadata's); only a one-to-one match across both whole sets counts. Each
        match is an `episode.guid_change` (the new GUID becomes a lookup alias of the old
        root), followed by an `episode.merge` of the new item's episode when it exists
        (is registered)."""
        seen_roots = set(self._root(E) for E in self.server_seen.get(P, {}))
        pre = [self._derive(P, it["occ3"], it["kfam"]) for it in items]
        resolved = set(pre) - {None}
        old = sorted((E for E in seen_roots if E not in resolved), key=_bkey)
        # CHOICE (a changed GUID now shared): a new GUID that several items of this fetch
        # carry is a reused GUID, not a changed one, so it is no candidate (one GUID
        # cannot become the alias of several old roots; competing evidence merges
        # nothing). Its items become its releases.
        gcount = {}
        for it in items:
            gcount[it["occ3"][0]] = gcount.get(it["occ3"][0], 0) + 1
        new = [i for i, it in enumerate(items)
               if it["occ3"][0] is not None and (pre[i] is None or pre[i] not in seen_roots)
               and gcount[it["occ3"][0]] == 1]

        def okey(E):
            m = self._fold_meta(("meta", "episode", E))[0]
            return (m["title"], m["pub_date"]) if m is not None else (None, None)

        def same(a, b):
            return bool(a[0]) and a[0] == b[0] and a[1] is not None and b[1] is not None and a[1] // 1000 == b[1] // 1000
        ok = dict((E, okey(E)) for E in old)
        nk = dict((i, (items[i]["fields"]["title"], items[i]["fields"]["pub_date"])) for i in new)
        mn = dict((i, [E for E in old if same(nk[i], ok[E])]) for i in new)
        mo = dict((E, [i for i in new if same(nk[i], ok[E])]) for E in old)
        pairs = [(i, mn[i][0]) for i in new if len(mn[i]) == 1 and len(mo[mn[i][0]]) == 1]
        for i, E_old in pairs:
            g, kfam = items[i]["occ3"][0], items[i]["kfam"]
            E_new = pre[i]
            exists = E_new is not None and E_new != E_old and E_new in self.episodes
            hook = self._ctx.get("before_identity")
            if hook is not None:
                hook()
            ch = self._choice(GUID_CHANGE, {})
            gc = self._new_decision(GUID_CHANGE, "guid:%s:%s" % (P, g), ch, {"podcast_id": P, "refresh_id": R, "kind": kfam},
                                    {"guid": g, "episode_id": E_old}, [creating])
            if exists:
                ch = self._choice(EPISODE_MERGE, {})
                self._new_decision(EPISODE_MERGE, "merge:%s" % E_new, ch,
                                   {"podcast_id": P, "cause": "guid_change", "by": gc["decision_id"]},
                                   {"from": E_new, "into": E_old}, [gc["decision_id"]])

    def _make_podcast_merge(self, R, P, Q, url, part0, survivor=None):
        """PROTOCOL §9.7: the survivor is the podcast created first (its creating fact's
        rank). Generations: the source's live one is joined into the survivor's live
        generation, or rehomed when the survivor has none (PROTOCOL §3.3: the source's state, policy
        and credentials then); every other source generation stays closed. Episodes: every
        source root, by its identity occurrence resolved through the survivor's rules.
        After the decision the source's evidence joins the survivor's by the same rules
        (CHOICE: disputes found then are occurrence mappings of this commit)."""
        rP, rQ = self.rank[self.podcasts[P]["creating_op"]], self.rank[self.podcasts[Q]["creating_op"]]
        dst, src = (P, Q) if rP < rQ else (Q, P)
        if survivor is not None:
            dst, src = (P, Q) if survivor == P else (Q, P)
        Gs, Gd = self._live_generation(src), self._live_generation(dst)
        gens = []
        for G in self.podcasts[src]["generations"]:
            if G == Gs:
                gens.append({"generation_id": G, "mode": "joined" if Gd else "rehomed", "into": Gd if Gd else G})
            else:
                gens.append({"generation_id": G, "mode": "closed", "into": None})
        episodes = []
        for E in sorted((E for E, ep in self.episodes.items() if ep["podcast_id"] == src and E not in self.root),
                        key=_bkey):
            occ3, kfam = self._occ3(self.episodes[E]["occurrence"])
            to = self._derive(dst, occ3, kfam)
            if to is not None:
                episodes.append({"from": E, "to": to})
        aliases = sorted((n for n, a in self.aliases.items() if a["podcast_id"] == src), key=_bkey)
        joining = [(k, e.get("kfam", "rss-guid")) for k, e in self.evidence.get(src, {}).items()
                   if k not in self.evidence.get(dst, {})]
        # CHOICE: it depends on the fetch's part 0, both podcasts' creating facts and the
        # opening facts of the live generations it joins or rehomes (what it names); a recovery
        # unification (R None) has no part 0 and names its cause.
        deps = ([part0] if part0 is not None else []) + [self.podcasts[src]["creating_op"],
                                                          self.podcasts[dst]["creating_op"]]
        deps += [self.generations[G]["op_id"] for G in (Gs, Gd) if G is not None]
        ch = self._choice(PODCAST_MERGE, {})
        inputs = ({"source": src, "into": dst, "refresh_id": R, "url": url} if R is not None
                  else {"source": src, "into": dst, "cause": "recovered"})
        obj = self._new_decision(PODCAST_MERGE, "podcast-merge:%s" % src, ch, inputs,
                                 {"source": src, "into": dst, "aliases": aliases, "generations": gens,
                                  "episodes": episodes, "properties": "source" if (Gs and not Gd) else "survivor"},
                                 deps)
        for k, kf in joining:
            self._detect(dst, k, kf, obj["decision_id"], [obj["decision_id"]])

    def _fetch_failure(self, R, P, fx, cap):
        """A failed fetch or parse writes no observation and keeps the catalog; it updates
        the live status, published in a part 0 holding only the status when due."""
        prev = self.status_live.get(P)
        status = {"last_attempt": fx["fetch_time"], "last_success": prev["last_success"] if prev is not None else None,
                  "error": fx["error"], "next_attempt": fx["next_attempt"]}
        if self._status_due(P, "failure", status):
            self._write_parts(P, R, "failure", cap["generation"], fx["fetch_time"], None, [], {}, {"status": status})
            self.status_logged[P] = {"class": "failure", "error": copy.deepcopy(fx["error"]),
                                     "last_attempt": fx["fetch_time"]}
        self.status_live[P] = status

    def _fetch_reparse(self, R, P, fx):
        """PROTOCOL §9.6: a re-parse of the podcast's stored response (the last successful fetch's).
        Each changed record names as its base the revision stored for it with the response
        and keeps the original fetch time; it never proposes, admits, moves or changes
        refresh_status.

        A record is written when it differs from the current
        canonical record (so an intervening device correction makes it write a record that
        loses by fetch time). CHOICE (open): the stored revisions become the
        re-parse's own for the records it writes, so a later re-parse names them and
        replaces them (naming the original fetch's revision would make it concurrent with
        an equal fetch time, so it could never replace the first re-parse). A record the
        original parse lacked has no stored revision (base null). The re-parse updates the
        server's fetch history like a fetch (so the items it reveals are never discovered
        later); inputs.generation_id is null."""
        st = self.stored.get(P)
        if st is None or fx["reparse_of"] != st["refresh_id"]:
            raise RuleInputError("a re-parse names the podcast's stored response (its last successful fetch)")
        ft = st["fetch_time"]
        self._identify_items(R, P, fx["items"], detect=False)
        feed_changed, kept, changed, _ = self._changes(P, fx)
        seen_ids = self._history_changes(P, kept, changed)
        if feed_changed or changed or seen_ids:
            # CHOICE: a re-parse's history changes are listed and force part 0 as a fetch's.
            _, _, written = self._write_parts(P, R, "re-parse", None, ft, fx["feed"] if feed_changed else None, changed,
                                              st["revs"], {"seen": seen_ids} if seen_ids else {})
            st["revs"].update(written)
        self._update_history(P, kept, ft)

    # -- PROTOCOL §7.6: the queue ---------------------------------------------------

    def _apply_queue(self, op, typ, parsed):
        """PROTOCOL §7.6: per named episode a bulk member
        (PROTOCOL §7.2): a failing member is skipped; the fact is not_applied only if every
        member fails, with the first failing member's reason in payload order. A placement
        command (add, restore, move, a run move) and a sort command are steps of the queue
        replay (PROTOCOL §7.6), recorded with the status each named member had here. CHOICE: they
        are applied whenever one member passes (a sort always): their effect is their
        place in the replay's order. A removal or a clear is a membership register write:
        applied or superseded."""
        form = parsed["form"]
        if form == "sort":
            self.qsteps[op] = {"form": "sort", "parsed": parsed, "status": []}
            self._mark_queue_node(op)
            return _outcome(APPLIED)
        effective = []
        first_reason = None
        status = []
        for ref, kind, data in parsed["entries"]:
            r = self._check_ref(ref, op)
            status.append("ok" if r is None else ("closed" if r == R_CLOSED else "fail"))
            if r is None:
                effective.append((ref, kind, data))
            elif first_reason is None:
                first_reason = r
        for ref, kind, data in effective:
            self._add(("episode", ref["episode_id"], ref["generation_id"]), self._ctb(op, kind, ref, data))
        if form in ("add", "restore", "move", "run"):
            self.qsteps[op] = {"form": form, "parsed": parsed, "status": status}
            self._mark_queue_node(op)
        for ref, _, _ in parsed["entries"]:
            if ref["episode_id"] in self.episodes:
                self._refresh_episode(ref["episode_id"])
        if not effective:
            return _outcome(NOT_APPLIED, first_reason)
        if form in ("add", "restore", "move", "run"):
            return _outcome(APPLIED)
        folds = [self._fold_episode_scope(r["episode_id"], r["generation_id"]) for r, _, _ in effective]
        return self._outcome_from(op, folds)

    def _commit_revision(self):
        """The revision (epoch, seq) of the commit this acceptance makes: a live fact's
        receipt's; a server-originated commit's (a fetch, a request end, a recovered identity
        decision's unification) is at the arrival's position in the account's current epoch,
        after any epoch evidence its entry carried (PROTOCOL §6.11)."""
        rk = self._ctx.get("rkey") if self._ctx is not None else None
        if rk is not None and not self._ctx.get("server"):
            return (rk[0], rk[1])
        return (self.epoch, self.narrivals)

    def _choice(self, dtype, info):
        """The server's random choices for the next decision of this acceptance. A decision
        is part of its acceptance's commit, so its `epoch` and `seq` are that commit's
        revision (docs/CONTRACT.md "Server decisions": a commit has one revision; it is
        passed to a chooser as info["commit"])."""
        ctx = self._ctx
        commit = self._commit_revision()
        info = dict(info, commit=commit)
        if ctx["chooser"] is not None:
            ch = ctx["chooser"](dtype, info)
            ctx["arrival"].setdefault("decisions", []).append(ch)
        else:
            if not ctx["supply"]:
                raise RuleInputError("the arrival does not supply the server's choices for its %s decision" % dtype)
            ch = ctx["supply"].pop(0)
        if not isinstance(ch, dict) or ch.get("decision_type") != dtype:
            raise RuleInputError("the next supplied decision choice is not a %s: %s" % (dtype, ch))
        # An admission supplies a position ID (and a draw for `random`) exactly when
        # the server resolves its queue placement; refresh parts and moves supply nothing.
        resolves = dtype == RESOLVED_ADD or (dtype == ADMISSION and info.get("resolve"))
        allowed = {"decision_type", "decision_id", "epoch", "seq"}
        if dtype == LAYOUT:
            allowed |= {"position_ids"}
        elif resolves:
            allowed |= {"position_id", "draw"}
        elif dtype == MAPPING and info.get("allocate"):
            # PROTOCOL §9.7: a release whose rss-guid+enclosure ID is taken gets `e0:` + this UUID.
            allowed |= {"occurrence_id"}
            if not c.is_uuid(ch.get("occurrence_id")):
                raise RuleInputError("an allocated occurrence mapping supplies its occurrence_id")
        if set(ch) - allowed or not {"decision_id", "epoch", "seq"} <= set(ch):
            raise RuleInputError("decision choice %s has members %s (want %s)" % (dtype, sorted(ch), sorted(allowed)))
        if not c.is_uuid(ch["decision_id"]):
            raise RuleInputError("decision_id is not a UUID")
        if (ch["decision_id"] in self.prio or ch["decision_id"] in self.facts or ch["decision_id"] in self.ar_dec
                or ch["decision_id"] in self.ar_facts):
            raise RuleInputError("decision_id %s is already a node ID" % ch["decision_id"])
        for k in ("epoch", "seq"):
            if not _is_plain_int(ch[k]) or not 0 <= ch[k] <= c.MAX_SAFE_INTEGER:
                raise RuleInputError("%s is not a non-negative safe integer" % k)
        if (ch["epoch"], ch["seq"]) != commit:
            raise RuleInputError("decision choice %s is at revision %s, its commit at %s (a commit has one revision)"
                                 % (dtype, (ch["epoch"], ch["seq"]), commit))
        if dtype == LAYOUT:
            pids = ch.get("position_ids")
            if not isinstance(pids, list) or not all(c.is_uuid(p) for p in pids) or len(pids) != info["fresh"]:
                raise RuleInputError("a layout needing %d fresh keys must supply exactly that many position_ids"
                                     % info["fresh"])
        elif resolves:
            if not c.is_uuid(ch.get("position_id")):
                raise RuleInputError("a resolved add supplies its position_id")
            if info["random"]:
                d = ch.get("draw")
                if not _is_plain_int(d) or not 0 <= d <= info["size"]:
                    raise RuleInputError("a random enqueue supplies a draw in [0, %d]" % info["size"])
            elif "draw" in ch:
                raise RuleInputError("a draw is supplied only for the random enqueue location")
        return ch

    def _current_policy(self):
        return dict(self._fold_account().records.get("queue_policy", DEFAULT_QUEUE_POLICY))

    def _policy_now(self):
        return self._current_policy()

    def _current_queue(self):
        """The account's queue now: the in-members in the replay's order (PROTOCOL §7.6), each with its published key (its latest key write in rank order, PROTOCOL §7.6; None
        without one) and its current canonical sort values."""
        writes = self._key_writes()
        out = []
        for E, G in self._queue_order():
            w = writes.get((E, G))
            out.append({"episode_id": E, "generation_id": G, "key": w[1] if w else None,
                        "position_id": w[2] if w else None, "values": self._episode_values(E)})
        return out

    def _episode_values(self, E):
        recs = self.proj.get(("episode", E))
        if recs is not None and "identity" in recs:
            return self.sort_values(recs)
        ep = self.episodes.get(E, {})
        occ = ep.get("occurrence") or {}
        return qs.sort_values({"title": occ.get("title"), "podcast_id": self._podcast_root(ep.get("podcast_id") or ""),
                               "publication_time": occ.get("publication_time")})

    def sort_values(self, recs):
        """PROTOCOL §7.6's one canonical accessor (PROTOCOL §9.4 "Sort values"), from an episode's current
        records: title, publication date and duration from its catalog `metadata` record
        (the feed duration when there is no measured one), the podcast title from the
        podcast's `metadata`; the occurrence evidence for an episode without metadata (as
        the earlier rules had it), podcast titles empty without podcast metadata. Each value falls back to the occurrence evidence when there is no
        metadata record or its field is null (the metadata record always has every
        member); a measured duration wins over the feed's `media.duration`. The inputs
        carry all five values with Android's defaults filled in."""
        ident = recs["identity"]
        occ = ident["occurrence"]
        dur = recs.get("measured_duration")
        meta = recs.get("metadata") or {}
        title = meta.get("title") if meta.get("title") is not None else occ.get("title")
        pub = meta.get("pub_date") if meta.get("pub_date") is not None else occ.get("publication_time")
        feed_dur = meta["media"]["duration"] if meta.get("media") is not None else None
        pmeta = self.proj.get(("podcast", ident["podcast_id"]), {}).get("metadata") or {}
        return qs.sort_values({
            "title": title,
            "podcast_id": ident["podcast_id"],
            "podcast_title": pmeta.get("title"),
            "publication_time": pub,
            "duration": dur["duration_ms"] if dur is not None else feed_dur,
        })

    def _mark_queue_node(self, n):
        """Records `n` as a queue membership/position node and flags its ancestors (they
        have a queue descendant, so they are not on the queue frontier)."""
        if n in self.qnodes:
            return
        self.qnodes.add(n)
        stack = list(self.node_edges[n])
        while stack:
            x = stack.pop()
            if x in self.qflag:
                continue  # its ancestors are flagged already
            self.qflag.add(x)
            stack.extend(self.node_edges.get(x, ()))

    def _decision_deps(self, trigger):
        """PROTOCOL §7.6: the trigger and the queue membership/position frontier the server
        observed. CHOICE: that frontier is the set of admitted nodes holding a queue
        membership or position contribution in any episode scope of the account (queue
        facts, play starts with a bundle, ends, finalizations and leaves carrying a
        completion or skip removal, resolved adds), minus those having such a node as
        descendant; the trigger is always listed."""
        return sorted(set(q for q in self.qnodes if q not in self.qflag) | {trigger}, key=_bkey)

    def _next_decision_index(self):
        """PROTOCOL §6.10: a decision's receipt `index` is its position in its commit. The
        accepted fact comes first (its own index), then the decisions its acceptance
        makes, in the order they are made."""
        r = (self._ctx.get("arrival") or {}).get("receipt") or {}
        base = r.get("index", 0) if isinstance(r, dict) else 0
        n = self._ctx.get("decisions_made", 0)
        self._ctx["decisions_made"] = n + 1
        if self._ctx.get("server"):
            # PROTOCOL §9.4: a server-originated commit holds only decisions. CHOICE: they take
            # the indices 0, 1, ... in the order they are made.
            return n
        return base + 1 + n

    def _new_decision(self, dtype, subject, ch, inputs, result, deps=None):
        """Makes a decision. Without `deps`, a queue decision: its trigger (the subject)
        and the queue frontier it observed; catalog decisions name their own."""
        if deps is None:
            deps = self._decision_deps(subject)
        else:
            deps = sorted(set(deps), key=_bkey)
        # PROTOCOL §6.1: max(server_now, dependencies). CHOICE: server_now is the
        # receive time of the arrival whose acceptance makes the decision.
        nt = max([self._ctx["receive_time"]] + [self.node_time[d] for d in deps])
        obj = {
            "decision_id": ch["decision_id"],
            "decision_type": dtype,
            "decision_version": 1,
            "dependencies": deps,
            "normalized_time": nt,
            "epoch": ch["epoch"],
            "seq": ch["seq"],
            "index": self._next_decision_index(),
            "subject": subject,
            "inputs": inputs,
            "result": result,
        }
        self.created.append(copy.deepcopy(obj))
        self._admit_decision(obj)
        return obj

    def _make_resolved_add(self, start, ref):
        """PROTOCOL §7.6: the start of a client without the queue
        capability, for an episode not in the queue at acceptance: the server resolves the
        add once from the account's enqueue location, as a phone without current episode or
        download state would (`back` and `after_current` are the end, `front` the start,
        `random` a drawn index i in [0, size]: 0 is the start, else after the queue's
        (i-1)th episode, the tail too), records the `place`, a key prediction between its
        neighbours' keys and the canonical sort values (PROTOCOL §7.6). The add is the start's own
        queue effect, a replay step at the start's place (under keep-sorted the replay
        sorts there; no sort result is recorded)."""
        loc, cur = self._policy_now()["enqueue_location"], self._current_queue()
        ch = self._choice(RESOLVED_ADD, {"random": loc == "random", "size": len(cur)})
        place, key = self._resolve_place(loc, cur, ch)
        E, G = ref["episode_id"], ref["generation_id"]
        self._new_decision(RESOLVED_ADD, start, ch, {"trigger": start, "enqueue_location": loc},
                           {"episode_id": E, "generation_id": G, "key": key, "position_id": ch["position_id"],
                            "place": place, "values": self._values_list([(E, G)], start)})

    @staticmethod
    def _resolve_place(loc, cur, ch):
        """A server resolution (PROTOCOL §7.6): (place, key prediction).
        CHOICE: the key lies between the neighbours' current keys; when they are not in
        order (a commit not yet published), just above the lower one."""
        if loc in ("back", "after_current"):
            place, i = {"kind": "end"}, len(cur)
        elif loc == "front":
            place, i = {"kind": "start"}, 0
        else:
            i = ch["draw"]
            place = {"kind": "start"} if i == 0 else {
                "kind": "after", "after": {"episode_id": cur[i - 1]["episode_id"], "generation_id": cur[i - 1]["generation_id"]}}
        low = cur[i - 1]["key"] if i > 0 else None
        high = cur[i]["key"] if i < len(cur) else None
        if low is not None and high is not None and c.compare_queue_keys(low, high) >= 0:
            high = None
        return place, c.queue_between(low, high, c.uuid_to_u(ch["position_id"]))

    @staticmethod
    def _place_value(place):
        """A fact's place as a decision records it (anchors as {episode_id, generation_id})."""
        if place["kind"] != "after":
            return {"kind": place["kind"]}
        a = place["after"]
        return {"kind": "after", "after": {"episode_id": a["episode_id"], "generation_id": a["generation_id"]}}

    # -- PROTOCOL §7.6: sort values recorded by the server ------------------------

    def _keep_sorted_at(self, node):
        """The account's `sorting` register at a node's place in the order (the last write
        ranked before it; with no node, now)."""
        keep = DEFAULT_QUEUE_POLICY["keep_sorted"]
        for ct in self._sorted(("account",)):
            if ct.kind != "queue_policy" or "sorting" not in ct.data:
                continue
            if node is not None and node in self.rank and self.rank[ct.node] >= self.rank[node]:
                break
            keep = ct.data["sorting"]["keep_sorted"]
        return keep

    def _values_list(self, keys, place_node, force=False, trigger=True):
        """PROTOCOL §7.6: the canonical values a placement records for its own episodes, and, when it is
        a sort trigger at admission (keep-sorted at its place, or `force` for a sort
        command), for every member then in the queue. CHOICE: "then in the queue" is the
        queue at admission, the commit's own effect included; values come from the one
        canonical accessor (PROTOCOL §7.6, §9.4) at admission. A move is a placement but never a
        sort trigger (`trigger` false)."""
        vals = {}
        for E, G in keys:
            vals[(E, G)] = self._episode_values(E)
        if force or (trigger and self._keep_sorted_at(place_node)):
            for m in self._current_queue():
                vals.setdefault((m["episode_id"], m["generation_id"]), m["values"])
        return [{"episode_id": E, "generation_id": G, "values": dict(v)}
                for (E, G), v in sorted(vals.items(), key=lambda kv: (_bkey(kv[0][0]), _bkey(kv[0][1])))]

    def _record_fact_values(self, op):
        """A live placement's or sort's receipt values (PROTOCOL §7.6). Every placement command (an add,
        a restore, a play-start bundle, a move, a run move: PROTOCOL §7.6) records its own
        members' values (those passing the bulk checks); an add, restore or bundle records
        the queue too when keep-sorted is on at its place; a move never sorts, so records
        only its own; a
        sort records the queue."""
        q = self.qsteps[op]
        if q["form"] == "sort":
            self.recorded[op] = self._values_list([], op, force=True)
            return
        refs = [q["parsed"]["refs"][0]] if q["form"] == "bundle" else [e[0] for e in q["parsed"]["entries"]]
        keys = [(r["episode_id"], r["generation_id"]) for r, st in zip(refs, q["status"]) if st == "ok"]
        self.recorded[op] = self._values_list(keys, op, trigger=q["form"] not in ("move", "run"))

    # -- PROTOCOL §7.6: the replay ---------------------------------------------

    def _scope_closed(self, key):
        """Whether an entry's generation class can hold no member (PROTOCOL §7.6): closed, or not
        its podcast's live generation now. CHOICE: the second case arises only while a
        recovery has joined a later generation's opening but not yet the fact that closes
        the earlier one; the records then show the live generation's state only
        (_episode_records), so the replay counts that generation's entries as structure only
        too, and the published keys always show what the records show."""
        g = self.generations.get(key[1])
        if g is None:
            return False
        if g["closed_by"] is not None:
            return True
        return self._live_generation(self._podcast_root(g["podcast_id"])) != key[1]

    def _ref_key(self, ref):
        """PROTOCOL §7.6: an EpisodeRef's replay entry, (episode, generation class), where it routes
        now under the final identity and generation mapping. A reference into a closed
        source generation of a podcast merge keeps a structural entry of its own (a ghost,
        PROTOCOL §7.6). None when it names nothing (an unknown podcast or generation)."""
        if "named" in ref:
            E, P, G = ref["named"]
            occ3, kfam = ref["occ3"], ref["kfam"]
        else:
            E, P, G = ref["episode_id"], ref["podcast_id"], ref["generation_id"]
            occ3, kfam = self._occ3(ref["occurrence"])
        if not self._known_podcast(P) or (G not in self.podcasts[P]["generations"] and not self._joined_into(P, G)):
            return None
        routed, reason = self._route(P, G, E, occ3, kfam)
        if routed is None:
            return (self._root(E), G) if reason == R_CLOSED else None
        return (routed[0], self.gen_join.get(routed[2], routed[2]))

    def _decision_key(self, E, G):
        return self._route_scope(E, G)

    def _membership_timelines(self):
        """PROTOCOL §7.6: membership at each step comes from the episode fold's effect of each
        membership fact at its place: per live scope, its effective membership events in
        order [(rank, in_queue)] (explicit decisions, automatic adds that apply, fired
        completion and skip removals, restores, the import). A closed generation's entries
        are never members (PROTOCOL §7.6). Also returns, per scope, the admissions whose
        automatic add applies."""
        tls, auto = {}, {}
        for key, cts in self.scopes.items():
            if key[0] != "episode" or not cts or key[1] in self.root:
                continue
            k = (key[1], key[2])
            if self._scope_closed(k):
                continue
            f = self._fold_episode_scope(key[1], key[2])
            if f.mem_timeline:
                tls[k] = f.mem_timeline
            if f.auto_applied:
                auto[k] = f.auto_applied
        return tls, auto

    @staticmethod
    def _member_at(tl, r, inclusive):
        v = False
        for rk, inq, _ in tl:
            if rk < r or (inclusive and rk == r):
                v = inq
            else:
                break
        return v

    def _import_counts(self, manifest):
        """(held, wanted): the import's podcast, episode and queued-episode decisions held,
        and the manifest's counts of them."""
        o = self.decisions[manifest]["obj"]
        imp = self.imports.get(o["inputs"]["import_id"], {})
        eps = imp.get("episodes", ())
        held = (len(imp.get("podcasts", ())), len(eps),
                sum(1 for d in eps if "queue_key" in self.decisions[d]["obj"]["result"]))
        im = o["result"]["result"]["imported"]
        return held, (im["podcasts"], im["episodes"], im["queue"])

    def _import_complete(self, manifest):
        """PROTOCOL §7.6: the import step happens only once every decision the manifest's counts
        call for is present: all `imported.podcasts` import.podcast decisions, all
        `imported.episodes` import.episode decisions, `imported.queue` of them queued. A
        recovery that holds only part of them (request boundaries after the headers or
        between episode decisions) places none of the imported queue."""
        held, wanted = self._import_counts(manifest)
        return held == wanted

    def _replay_steps(self):
        """The queue commands in the deterministic order (PROTOCOL §7.6): facts at their own place;
        a filing at its proposal's place (the server's own admission at its own); a resolved
        add at its start's; the import at its manifest's; the account's `sorting` writes.
        CHOICE: several steps at one place (two admissions of one proposal) keep their
        decisions' rank order, after the place's own fact."""
        steps = []
        for op in self.qsteps:
            if op in self.rank:
                steps.append((self.rank[op], 0, (), "fact", op))
        for did, d in self.decisions.items():
            o = d["obj"]
            t = o["decision_type"]
            if t == ADMISSION and o["result"]["action"] == "queue":
                place = o["result"]["proposal_id"] or did
                if place in self.rank:
                    steps.append((self.rank[place], 1, d["rank"], "admission", did))
            elif t == RESOLVED_ADD and self._effective(did) and o["subject"] in self.rank:
                steps.append((self.rank[o["subject"]], 1, d["rank"], "resolved", did))
            elif t == IMPORT_MANIFEST and self._effective(did) and self._import_complete(did):
                steps.append((self.rank[did], 1, d["rank"], "import", did))
        for ct in self._sorted(("account",)):
            if ct.kind == "queue_policy" and "sorting" in ct.data:
                steps.append((self.rank[ct.node], 0, (), "policy", ct))
        steps.sort(key=lambda x: (x[0], x[1], x[2]))
        return steps

    def _map_values(self, lst):
        """PROTOCOL §7.6: recorded values by replay entry: mapped through the final identity map;
        of two aliases of one survivor, the survivor's own record, else the alias registered
        first (CHOICE: by its registering node's rank, then episode ID); Smart Shuffle's
        podcast ID through podcast merges."""
        out, prio = {}, {}
        for m in lst or ():
            k = self._decision_key(m["episode_id"], m["generation_id"])
            reg = self.episodes.get(m["episode_id"], {}).get("op_id")
            rk = (0 if m["episode_id"] == k[0] else 1, self.rank.get(reg, len(self.order) + 1), _bkey(m["episode_id"]))
            if k not in out or rk < prio[k]:
                v = dict(m["values"])
                v["podcast_id"] = self._podcast_root(v["podcast_id"])
                out[k], prio[k] = v, rk
        return out

    def _step_values(self, kind, x):
        if kind == "fact":
            return self._map_values(self.recorded.get(x))
        o = self.decisions[x]["obj"]
        if kind in ("admission", "resolved"):
            return self._map_values(o["result"].get("values"))
        lst = []
        for did in self.imports.get(o["inputs"]["import_id"], {}).get("episodes", ()):
            lst.extend(self.decisions[did]["obj"]["result"].get("values") or ())
        return self._map_values(lst)

    def _replay_prepare(self):
        """The replay's inputs (PROTOCOL §7.6): the membership timelines and, per step in the
        deterministic order, its resolved content under the final identity and generation
        mapping (PROTOCOL §7.6): (rank, node, kind, content). A step's content: for a placement, its
        members [(entry, applies)] (deduplicated, first occurrence kept), place, anchor
        entry, whether it may sort, and the values its receipt or decision recorded; for a
        move, its run's members [(entry, live)], place and anchor; for a sort, its order and
        values, or its observed and permutation entries; for a policy write, the register."""
        tls, auto = self._membership_timelines()
        out = []

        def dedup(pairs):
            seen, res = set(), []
            for k, v in pairs:
                if k is not None and k not in seen:
                    seen.add(k)
                    res.append((k, v))
            return res
        for r, _, _, kind, x in self._replay_steps():
            if kind == "policy":
                out.append((r, x.node, "policy", {"sorting": dict(x.data["sorting"])}))
                continue
            if kind == "fact":
                q = self.qsteps[x]
                form, parsed = q["form"], q["parsed"]
                if form == "sort":
                    if parsed["sort_order"] == "random":
                        content = {"form": "sort", "order": "random",
                                   "observed": [k for k, _ in dedup((self._ref_key(y), None) for y in parsed["observed"])],
                                   "permutation": [k for k, _ in dedup((self._ref_key(y), None)
                                                                       for y in parsed["permutation"])]}
                    else:
                        content = {"form": "sort", "order": parsed["sort_order"], "values": self._step_values("fact", x)}
                    out.append((r, x, "fact", content))
                    continue
                refs = [parsed["refs"][0]] if form == "bundle" else [e[0] for e in parsed["entries"]]
                place = parsed["bundle"]["place"] if form == "bundle" else parsed["place"]
                anchor = self._ref_key(place["after"]) if place["kind"] == "after" else None
                members = []
                for ref, stt in zip(refs, q["status"]):
                    if stt == "fail":
                        continue
                    k = self._ref_key(ref)
                    if k is not None:
                        members.append((k, stt == "ok" and not self._scope_closed(k)))
                content = {"form": "move" if form in ("move", "run") else "place", "members": dedup(members),
                           "place": place["kind"], "anchor": anchor, "sorts": form != "restore",
                           "values": self._step_values("fact", x), "writer": x}
                out.append((r, x, "fact", content))
                continue
            o = self.decisions[x]["obj"]
            if kind == "import":
                eps = sorted((self.decisions[d]["obj"] for d in self.imports.get(o["inputs"]["import_id"], {})
                              .get("episodes", ()) if "queue_key" in self.decisions[d]["obj"]["result"]),
                             key=lambda e: (e["result"]["queue_index"], _bkey(e["decision_id"])))
                members = dedup((self._decision_key(e["result"]["episode_id"], e["result"]["generation_id"]),
                                 e["decision_id"]) for e in eps)
                content = {"form": "place", "members": [(k, not self._scope_closed(k)) for k, _ in members],
                           "place": "end", "anchor": None, "sorts": False, "values": self._step_values(kind, x),
                           "writers": dict(members)}
            else:
                res = o["result"]
                k = self._decision_key(res["episode_id"], res["generation_id"])
                if kind == "admission":
                    app = self._effective(x) and not self._scope_closed(k) and x in auto.get(k, ())
                else:
                    app = not self._scope_closed(k)
                place = res["place"]
                anchor = (self._decision_key(place["after"]["episode_id"], place["after"]["generation_id"])
                          if place["kind"] == "after" else None)
                content = {"form": "place", "members": [(k, app)], "place": place["kind"], "anchor": anchor,
                           "sorts": True, "values": self._step_values(kind, x), "writer": x}
            out.append((r, x, kind, content))
        return out, tls

    @staticmethod
    def _replay_state():
        return {"struct": [], "sorting": {"keep_sorted": DEFAULT_QUEUE_POLICY["keep_sorted"],
                                          "sort_order": DEFAULT_QUEUE_POLICY["sort_order"]},
                "last_vals": {}, "diag": [], "placed": set()}

    def _replay_run(self, steps, tls, start=0, state=None, snapshots=None):
        """Runs the prepared steps from index `start` on a state (the empty one at 0), as
        PROTOCOL §7.6 executes them; with `snapshots` (a list) it appends a copy of
        the state before each step it runs (snapshots).

        * add, restore, play bundle, filing, resolved add, import: only the members not in
          the queue immediately before the step are placed (taken out, inserted at `place`
          in their requested order); a member that does not apply (an automatic add an
          explicit decision suppresses, a losing admission, a closed generation) leaves a
          ghost at `place` if it has no entry yet; an existing entry never moves for it. If
          anything was placed and keep-sorted is on there, the in-members are sorted (a
          restore and the import never sort).
        * move, run move: the run is taken out and inserted at `place`, out entries too; a
          closed generation's member is a ghost only. A move is a placement for sort
          evidence: its members' recorded values become their latest placement values
          (no membership change, no sort).
        * sort dialog: the in-members at the step are permuted among their slots by the
          canonical order with the recorded values (or the recorded permutation of the
          observed ones); out entries and ghosts keep their slots.
        * policy: the `sorting` register."""
        state = copy.deepcopy(state) if state is not None else self._replay_state()
        struct, last_vals, diag = state["struct"], state["last_vals"], state["diag"]

        def in_at(k, r, inclusive):
            tl = tls.get(k)
            return tl is not None and self._member_at(tl, r, inclusive)

        def insert(run, place, anchor):
            run_set = set(run)
            rest = [k for k in struct if k not in run_set]
            if place == "start":
                i = 0
            elif place == "end":
                i = len(rest)
            elif anchor is None or anchor not in struct:
                # PROTOCOL §7.6: an anchor with no entry at all (a client fault): the end, logged.
                diag.append(("anchor_without_entry", anchor))
                i = len(rest)
            elif anchor in run_set:
                # The anchor is in the run itself (after mapping): after its nearest structural
                # predecessor not in the run, or at the start.
                j = struct.index(anchor) - 1
                while j >= 0 and struct[j] in run_set:
                    j -= 1
                i = rest.index(struct[j]) + 1 if j >= 0 else 0
            else:
                i = rest.index(anchor) + 1
            struct[:] = rest[:i] + list(run) + rest[i:]

        def sort_in(r, order, vals, observed=None, permutation=None):
            slots = [i for i, k in enumerate(struct) if in_at(k, r, True)]
            members = [struct[i] for i in slots]
            if order == "random":
                mset, obs = set(members), set(observed)
                perm = [k for k in permutation if k in mset and k in obs]
                pset = set(perm)
                for i, k in zip([i for i in slots if struct[i] in pset], perm):
                    struct[i] = k
                return
            values = []
            for k in members:
                v = vals.get(k) or last_vals.get(k)
                if v is None:
                    diag.append(("values_missing", k))
                    v = self._episode_values(k[0])
                values.append(v)
            for i, j in zip(slots, qs.canonical_order(values, order)):
                struct[i] = members[j]

        for idx in range(start, len(steps)):
            if snapshots is not None:
                snapshots.append(copy.deepcopy(state))
            r, x, kind, content = steps[idx]
            if kind == "policy":
                state["sorting"] = dict(content["sorting"])
                continue
            form = content["form"]
            if form == "sort":
                if content["order"] == "random":
                    sort_in(r, "random", None, content["observed"], content["permutation"])
                else:
                    sort_in(r, content["order"], content["values"])
                continue
            if form == "move":
                run = [k for k, live in content["members"] if live or k not in struct]
                if run:
                    insert(run, content["place"], content["anchor"])
                for k, live in content["members"]:
                    if live and k in content["values"]:
                        last_vals[k] = content["values"][k]
                continue
            placed, run = [], []
            for k, app in content["members"]:
                if app:
                    if not in_at(k, r, False):
                        run.append(k)
                        placed.append(k)
                elif k not in struct:
                    run.append(k)
            if run:
                insert(run, content["place"], content["anchor"])
            for k in placed:
                # PROTOCOL §7.6: the members a placement step actually places are its key
                # writes (with the writer that carries each key).
                state["placed"].add((content["writers"][k] if "writers" in content else content["writer"], k))
            if placed:
                vals = content["values"]
                for k in placed:
                    if k in vals:
                        last_vals[k] = vals[k]
                if state["sorting"]["keep_sorted"] and content["sorts"]:
                    sort_in(r, state["sorting"]["sort_order"], vals)
        return state

    def _replay(self):
        """PROTOCOL §7.6: the structure (every entry: members, out entries, ghosts) after
        replaying every queue command in the deterministic order, each executed as Android
        3.12.1 executes it at the place its device resolved (_replay_run); and the
        membership timelines. The reference implements only this full replay."""
        steps, tls = self._replay_prepare()
        state = self._replay_run(steps, tls)
        self.diagnostics = state["diag"]
        self.replay_placed = state["placed"]
        return state["struct"], tls

    def _queue_order(self):
        """The replay's order of the in-members (PROTOCOL §7.6)."""
        struct, tls = self._replay()
        return [k for k in struct if k in tls and tls[k] and tls[k][-1][1]]

    # -- PROTOCOL §7.6: keys (queue.layout) ------------------------------------------

    def _key_writes(self, exclude=()):
        """The key-write selector (PROTOCOL §7.6): each scope's latest key write in rank order,
        {(E, G): (rank, key, position_id)}; a scope's published key.

        * Rank is the writer's original receipt rank (epoch, seq, index, ID): a fact's
          receipt position, a decision's (epoch, seq, index) with its ID (a recorded decision
          without `index` after every indexed entry of its revision). Within one commit the
          fact comes first, then its decisions, then the layout. Neither the replay's
          command order nor the arrival order is the rank.
        * Which members write: a placement step (an add, a restore, a play bundle, a
          filing, a resolved add, the import) writes only for the members it actually
          places, absent from the queue immediately before the step as the replay decides
          (an add or restore member already in the queue, or an automatic filing that does
          not apply, writes nothing); a move or run move writes for every member that
          carries a key (its own, or a run move's prediction), out entries included.
        * Which writes count: only facts that were applied (rejected, malformed or
          session-refused facts make no replay step); a member that fails its own checks
          writes nothing; a replaced receipt's writes are the original receipt's (the
          archive holds the fact at its original position); a recorded layout is an
          independent write.
        * Scopes: device keys and resolved-add, filing and import keys route through their
          references and decision scopes (_ref_key, _decision_key), through merges; a
          layout's keys stay on their exact (episode, generation); generation classes stay
          separate; an import writes only when its single step places its members (PROTOCOL §7.6).

        `exclude`: writer IDs left out (the harness compares a commit's layout against the
        keys before it)."""
        self._replay()
        placed = self.replay_placed
        out = {}

        def put(k, pos, key, pid):
            if k is not None and key is not None and (k not in out or pos > out[k][0]):
                out[k] = (pos, key, pid)
        for op, q in self.qsteps.items():
            if q["form"] == "sort" or op not in self.facts or op in exclude:
                continue
            pos = self.facts[op]["rkey"]
            parsed = q["parsed"]
            if q["form"] == "bundle":
                items = [(parsed["refs"][0], parsed["bundle"]["key"], parsed["bundle"]["pid"])]
            else:
                # A run move's sparse predictions (members without one write no key).
                items = [(e[0], e[2].get("key"), e[2].get("pid")) for e in parsed["entries"]]
            mover = q["form"] in ("move", "run")
            seen = set()
            for (ref, key, pid), stt in zip(items, q["status"]):
                if stt == "fail":
                    continue
                k = self._ref_key(ref)
                if k is None or k in seen:
                    continue  # (the replay's first-occurrence deduplication)
                seen.add(k)
                if stt == "ok" and (mover or (op, k) in placed):
                    put(k, pos, key, pid)
        for did, d in self.decisions.items():
            if did in exclude:
                continue
            o = d["obj"]
            t = o["decision_type"]
            pos = (o["epoch"], o["seq"], o.get("index", self._BIG), _bkey(did))
            res = o["result"]
            if t == LAYOUT:
                for m in res["members"]:
                    put((m["episode_id"], m["generation_id"]), pos, m["key"], m["position_id"])
            elif (t == ADMISSION and res["action"] == "queue") or t == RESOLVED_ADD:
                k = self._decision_key(res["episode_id"], res["generation_id"])
                if (did, k) in placed:
                    put(k, pos, res["key"], res["position_id"])
            elif t == IMPORT_EPISODE and "queue_key" in res:
                k = self._decision_key(res["episode_id"], res["generation_id"])
                if (did, k) in placed:
                    put(k, pos, res["queue_key"], res["position_id"])
        return out

    def _account_frontier(self):
        """Every admitted causal node no other node depends on."""
        return sorted((n for n in self.order if n not in self.has_child), key=_bkey)

    def _publish_queue(self):
        """The publication rule (PROTOCOL §7.6, a transaction invariant): at the end
        of every commit, derive the replay's order of in-members; each in-member's published
        key is its latest key write in rank order (_key_writes). If those keys give the
        order (every in-member has one, strictly increasing), nothing is recorded. Otherwise
        one sparse `queue.layout` lists only the members whose key changes (PROTOCOL §7.6): the in-members along the first longest increasing run of the
        current published keys keep theirs (a member without a key write, or one of two
        equal keys, is off the run), and the others get fresh keys left to right, between
        their neighbours (position IDs from the server's choice, in that order).

        CHOICE: its dependencies are the account's whole causal frontier (a superset of
        every causal input the replay read, earlier layout writes included), and its inputs
        {frontier (the same nodes), mapping_version (the number of identity-mapping
        decisions held: episode merges, podcast merges, occurrence mappings, GUID changes),
        policy (the node that decided the `sorting` register, or null)}."""
        self._sync_pub()
        order = self._queue_order()
        cands = [self.pubkeys.get(k) for k in order]
        if all(p is not None for p in cands) and all(c.compare_queue_keys(a[0], z[0]) < 0
                                                    for a, z in zip(cands, cands[1:])):
            return None
        known = [i for i, w in enumerate(cands) if w is not None]
        # The retained run: the first longest increasing run of the published keys; equal
        # keys can never both be kept (later ones rank lower).
        srt = sorted(known, key=functools.cmp_to_key(
            lambda i, j: c.compare_queue_keys(cands[i][0], cands[j][0]) or (j - i)))
        rank_of = {i: n for n, i in enumerate(srt)}
        seq = [rank_of[i] for i in known]
        keep = set(known[n] for n in qs.retained_positions(seq))
        fresh = len(order) - len(keep)
        ch = self._choice(LAYOUT, {"fresh": fresh, "size": len(order)})
        pids = iter(ch["position_ids"])
        members, low = [], None
        for i, (E, G) in enumerate(order):
            if i in keep:
                low = cands[i][0]
                continue
            high = next((cands[j][0] for j in range(i + 1, len(order)) if j in keep), None)
            pid = next(pids)
            key = c.queue_between(low, high, c.uuid_to_u(pid))
            members.append({"episode_id": E, "generation_id": G, "key": key, "position_id": pid})
            low = key
        deps = self._account_frontier()
        mv = sum(1 for d in self.decisions.values()
                 if d["obj"]["decision_type"] in (EPISODE_MERGE, PODCAST_MERGE, MAPPING, GUID_CHANGE))
        inputs = {"frontier": list(deps), "mapping_version": mv,
                  "policy": self._fold_account().det.get("queue_policy.sorting")}
        obj = self._new_decision(LAYOUT, LAYOUT_SUBJECT, ch, inputs, {"members": members}, deps)
        self._sync_pub()
        return obj

    def _sync_pub(self):
        """Brings the published keys (PROTOCOL §7.6: each scope's latest key write in rank order) and the
        `queue_position` records in step: recomputes them and refreshes exactly the episodes
        whose published key changed, so a record is republished only when it changes."""
        if self._syncing:
            return
        self._syncing = True
        try:
            self.pub_dirty = False
            new = dict((k, (w[1], w[2], w[0])) for k, w in self._key_writes().items())
            old = self.pubkeys
            self.pubkeys = new
            changed = set(k[0] for k in set(old) | set(new)
                          if (old.get(k) or (None, None))[:2] != (new.get(k) or (None, None))[:2])
            for E in sorted(changed):
                if E in self.episodes:
                    self._refresh_episode(E)
        finally:
            self._syncing = False

    def _admit_decision(self, obj):
        """Admits a decision (created here, or recovered): a causal node with its
        dependencies as edges, its contributions (a resolved add: the start's own queue
        effect, contributed under the start's node in the started episode's scope; a layout:
        the published keys of the scopes it lists; the import's decisions), and the refolds
        of every scope a competitor of the same subject touches (only the smallest rank is
        effective; layouts never compete: they are materializations, PROTOCOL §7.6)."""
        did = obj["decision_id"]
        dtype = obj["decision_type"]
        self._insert_node(did, obj["normalized_time"], obj["dependencies"])
        # Queue membership/position nodes (PROTOCOL §7.6). CHOICE (PROTOCOL §9.4): an admission
        # whose filing is a queue add counts, as a resolved add does; refresh parts and
        # moves do not.
        if dtype in (LAYOUT, RESOLVED_ADD) or (dtype == ADMISSION and obj["result"]["action"] == "queue") or (
                dtype == IMPORT_EPISODE and "queue_key" in obj["result"]):
            self._mark_queue_node(did)
        self.decisions[did] = {"obj": obj, "rank": (obj["epoch"], obj["seq"], _bkey(did))}
        skey = (dtype, obj["subject"])
        self.by_subject.setdefault(skey, []).append(did)
        if self.archiving and not self.rebuild_mode and did not in self.ar_dec:
            self._archive_decision(obj)
        if (dtype in (GUID_CHANGE, EPISODE_MERGE, PODCAST_MERGE) and len(self.by_subject[skey]) > 1
                and not self._effective(did)):
            # PROTOCOL §6.11: a later identity decision of the same subject changes nothing; the
            # unification merge makes what it named one with the effective one's result.
            return
        if dtype == LAYOUT:
            self.pub_dirty = True  # its keys are key writes (PROTOCOL §7.6)
        elif dtype in IMPORT_DECISIONS:
            self._admit_import(obj)
        elif dtype == RESOLVED_ADD:
            r = obj["result"]
            E, G = self._route_scope(r["episode_id"], r["generation_id"])
            self._add(("episode", E, G),
                      _Contribution(obj["subject"], "q_resolved", G,
                                    {"key": r["key"], "pid": r["position_id"], "decision": did},
                                    orig=r["episode_id"]))
        elif dtype == CATALOG_REFRESH:
            self._admit_refresh(obj)
        elif dtype == ADMISSION:
            self._admit_admission(obj)
        elif dtype == MOVED:
            self._admit_moved(obj)
        elif dtype == GUID_CHANGE:
            self._admit_guid_change(obj)
        elif dtype == MAPPING:
            self._admit_mapping(obj)
        elif dtype == EPISODE_MERGE:
            self._admit_episode_merge(obj)
        else:
            self._admit_podcast_merge(obj)
        # Refold every scope a competitor of the same subject touches (only the smallest
        # rank is effective).
        touched, pods = set(), set()
        for other in self.by_subject[skey]:
            e, p = self._decision_scope(self.decisions[other]["obj"])
            touched |= e
            pods |= p
        for P in sorted(pods):
            self._refresh_podcast(P)
        # A decision's contribution lives at the root of the episode it names.
        for E in sorted(touched | set(self._root(x) for x in touched)):
            if E in self.episodes:
                self._refresh_episode(E)

    def _decision_scope(self, o):
        """The episodes and podcasts whose records a decision can change."""
        t, r = o["decision_type"], o["result"]
        if t == LAYOUT:
            return set(m["episode_id"] for m in r["members"]), set()
        if t == RESOLVED_ADD:
            return {r["episode_id"]}, set()
        if t == IMPORT_PODCAST:
            return set(), {r["podcast_id"]}
        if t == IMPORT_EPISODE:
            return {r["episode_id"]}, set()
        if t == IMPORT_MANIFEST:
            imp = self.imports.get(o["inputs"]["import_id"], {})
            return set(self.decisions[d]["obj"]["result"]["episode_id"] for d in imp.get("episodes", ())), set()
        if t == CATALOG_REFRESH:
            P = o["inputs"]["podcast_id"]
            return set(x["episode_id"] for x in r["items"]), {P, self._podcast_root(P)}
        if t == ADMISSION:
            return {self._root(r["episode_id"])}, set()
        if t in IDENTITY_DECISIONS:
            # Identity decisions can change any episode of the podcasts they name.
            pods = set()
            if t == PODCAST_MERGE:
                # Every merged-away podcast: a chain's records name its final survivor.
                pods = {r["source"], r["into"], self._podcast_root(r["into"])} | set(self.pmerged)
            else:
                pods = {o["inputs"]["podcast_id"], self._podcast_root(o["inputs"]["podcast_id"])}
            return set(E for E, ep in self.episodes.items() if ep["podcast_id"] in pods) | set(self.root), pods
        P, G = o["inputs"]["podcast_id"], o["inputs"]["generation_id"]
        return set(), {P, self._podcast_root(P)} | ({self.generations[G]["podcast_id"]} if G in self.generations else set())

    # -- PROTOCOL §11.5: the import's decisions (PROTOCOL §7.6) -----------------------

    def _check_import_decision_refs(self, obj):
        """What an import decision (or a layout) refers to must exist when it is admitted."""
        t, res = obj["decision_type"], obj["result"]
        if t == IMPORT_PODCAST:
            if self._known_podcast(res["podcast_id"]):
                raise RuleInputError("an imported podcast is created by its import")
        elif t == IMPORT_EPISODE:
            if res["generation_id"] not in self.generations:
                raise RuleInputError("an imported episode names its import's podcast generation")
            if _unmodelled_import(obj):
                raise RuleInputError("an imported statistics baseline (last played, played time, history, units, "
                                     "rows, catalog) is not modelled by this reference (import_android.py is its oracle)")

    def _admit_import(self, obj):
        """The import's decisions, the subset the queue replay needs. `import.podcast` creates the
        podcast (its creation ID is the decision; its alias, its generation with the
        backup's state and policy, its credentials secret); `import.episode` registers the
        episode and contributes its imported state (play state, position as a claim,
        favorite); `import.manifest` completes the import: once all of the import's
        podcast decisions are present, every queued imported episode gets its queue entry
        at the manifest's place, the replay's one import step (PROTOCOL §7.6)."""
        did, t, inp, res = obj["decision_id"], obj["decision_type"], obj["inputs"], obj["result"]
        imp = self.imports.setdefault(inp["import_id"], {"podcasts": [], "episodes": [], "manifest": None})
        if t == IMPORT_PODCAST:
            P, G = res["podcast_id"], res["generation_id"]
            pod = self.podcasts.setdefault(P, {"creating_op": None, "generations": []})
            if pod["creating_op"] is None:
                pod["creating_op"] = did
            self._claim_alias(c.normalize_url(res["url"]), P, res["url"], did)
            if G not in self.generations:
                self.generations[G] = {
                    "podcast_id": P, "op_id": did, "kind": "preview" if res["state"] == "not_subscribed" else "subscribe",
                    "predecessor": None, "policy": {"new_episodes_action": res["new_episodes_action"],
                                                    "keep_updated": res["keep_updated"]},
                    "url": res["url"], "closed_by": None}
                pod["generations"].append(G)
            self._add(("podcast", P), _Contribution(did, "open", G, {"secret": res["secret_id"]}))
            if res["state"] == "archived":
                self._add(("podcast", P), _Contribution(did, "archive", G, {}))
            imp["podcasts"].append(did)
            self._refresh_podcast(P, all_episodes=True)
        elif t == IMPORT_EPISODE:
            if _unmodelled_import(obj):
                raise RuleInputError("an imported statistics baseline is not modelled by this reference")
            E, G = res["episode_id"], res["generation_id"]
            self._register(E, self.generations[G]["podcast_id"], res["occurrence"], did)
            E2, G2 = self._route_scope(E, G)
            self._add(("episode", E2, G2), _Contribution(did, "import", G2, {"imported": res["imported"], "decision": did},
                                                         orig=E))
            imp["episodes"].append(did)
            self._refresh_episode(E2)
        else:
            imp["manifest"] = did
        self._settle_imports()

    def _settle_imports(self):
        for imp in self.imports.values():
            m = imp.get("manifest")
            if m is None:
                continue
            held, wanted = self._import_counts(m)
            if any(h > w for h, w in zip(held, wanted)):
                raise RuleInputError("the import holds more decisions %s than its manifest counts %s" % (held, wanted))
            if held != wanted:
                continue
            for did in imp["episodes"]:
                res = self.decisions[did]["obj"]["result"]
                if "queue_key" not in res or (m, did) in self.import_queued:
                    continue
                self.import_queued.add((m, did))
                E, G = self._route_scope(res["episode_id"], res["generation_id"])
                self._add(("episode", E, G), _Contribution(m, "q_import", G, {"decision": did, "index": res["queue_index"]},
                                                           orig=res["episode_id"]))
                self._refresh_episode(E)

    # -- PROTOCOL §9.4: admitting catalog decisions ------------------------------------

    def _register(self, E, P, occ, node):
        """Registers an episode named by an accepted catalog record (PROTOCOL §9.4: "an item never
        registered before is registered with its occurrence"), like _check_ref does for an
        EpisodeRef; with persisted allocations, only by its deciding node (or when it
        comes to exist through a merge or mapping after its deciding node, which routed
        elsewhere in this order, was already admitted)."""
        if self.fixed_episodes is not None:
            d = self.fixed_episodes.get(E)
            if d is not None and (d["op_id"] == node or d["op_id"] in self.facts or d["op_id"] in self.decisions) \
                    and E not in self.episodes:
                self.episodes[E] = {"podcast_id": d["podcast_id"], "occurrence": copy.deepcopy(d["occurrence"]),
                                    "op_id": node}
                self._registered(E)
        elif E not in self.episodes:
            self.episodes[E] = {"podcast_id": P, "occurrence": copy.deepcopy(occ), "op_id": node}
            self._registered(E)

    def _note_observation(self, node, P, E, fetch_time, fields):
        """Catalog items (PROTOCOL §9.4): the first observing node and its time, the first one with
        an enclosure, which observation saw the episode without media, and the effective
        date (the latest dated observation from any source, device or
        server, in acceptance order; without any, the first observation's time, as Android
        persists a null date as "now")."""
        has = fields["media"] is not None
        self.obs_media[(node, E)] = has
        it = self.catalog_items.get(E)
        if it is None:
            it = self.catalog_items[E] = {"podcast_id": P, "first": node, "first_time": fetch_time, "playable": None,
                                          "date": fetch_time}
        if has and it["playable"] is None:
            it["playable"] = node
        if fields["pub_date"] is not None:
            it["date"] = fields["pub_date"]

    def _admit_refresh(self, obj):
        """A catalog.refresh part: its records are observations at the part's own place
        (its revision is the part), with the fetch's time; part 0 may carry the
        population marker of the generation and the refresh status."""
        did, inp, res = obj["decision_id"], obj["inputs"], obj["result"]
        P, ft = inp["podcast_id"], inp["fetch_time"]
        if "feed" in res:
            self._add(("meta", "podcast", P), _Contribution(did, "obs", None, {
                "base": res["feed"]["base"], "fields": res["feed"]["fields"], "fetch_time": ft, "dec": did}))
        for x in res["items"]:
            E = x["episode_id"]
            self._register(E, P, x["occurrence"], did)
            b = self._binding(E)
            if b is not None and b["podcast_id"] != P:
                continue  # never written live (the fetch skips such items); a malformed recovery
            # A part's items are identity evidence (recorded earlier live, so only a
            # replay or recovery adds them here); their records go to the root.
            media = x["fields"]["media"]
            g = x["occurrence"].get("guid")
            occ3 = (c.guid_trim(g) if isinstance(g, str) else None, self._norm(media["url"]) if media is not None else None,
                    x["fields"]["pub_date"])
            Er = self._root(E)
            # Evidence of the merge chain's survivor (a merge may be admitted first).
            kf = "atom-id" if x["occurrence"].get("k") == "atom-id" else "rss-guid"
            if self._record(self._podcast_root(P), occ3, did, kf):
                self._set_evidence_episode(self._podcast_root(P), occ3, Er)
            # Like a device's observation, the item's record follows its occurrence (where
            # it routes now), so a dispute found later that splits the GUID's evidence into
            # releases moves each item's metadata to its own release.
            self._add(("meta", "episode", Er), _Contribution(did, "obs", None, {
                "base": x["base"], "fields": x["fields"], "fetch_time": ft, "dec": did}, orig=E, occ=occ3,
                ref=(E, P, None, kf)))
            self._note_observation(did, P, Er, ft, x["fields"])
        if res.get("population"):
            self.populated.add(inp["generation_id"])
        if "status" in res:
            self._add(("status", P), _Contribution(did, "status", None, {"value": res["status"], "dec": did}))
        # A refresh part restores the server's fetch history of its
        # items and of its `seen` (live, the fetch updated it already; recovered, this is
        # its only source). CHOICE: a `seen` episode is playable as its canonical metadata.
        hist = self.server_seen.setdefault(self._podcast_root(P), {})
        for x in res["items"]:
            E = self._root(x["episode_id"])
            prev = hist.get(E)
            hist[E] = {"playable": x["fields"]["media"] is not None or (prev is not None and prev["playable"])}
        for E in res.get("seen", ()):
            E = self._root(E)
            meta = self._fold_meta(("meta", "episode", E))[0]
            prev = hist.get(E)
            hist[E] = {"playable": (meta is not None and meta["media"] is not None) or (prev is not None and prev["playable"])}

    def _admit_admission(self, obj):
        """PROTOCOL §9.4: the one filing of an episode in a generation. It acts at its place with
        that place as its writes' origin: a device proposal's own node (as a resolved add
        acts at its start), or, for the server's own discovery, the
        admission itself. Every contribution names the admission, so only the effective
        one (the smallest rank of its subject, PROTOCOL §9.5) takes effect.

        * the `admission` record (every action);
        * `inbox`: an automatic NEW, applied only without any barrier in the scope;
        * `queue`: an automatic add (PROTOCOL §7.6), a step of the queue replay at its place
          (PROTOCOL §7.6; the proposal bundle's `moves` are invalid);
        * `nothing`: no effect.

        The record and the filing are two contributions of the same node in the same scope
        (the same rank), which folds exactly as Go's single effect per node and scope.
        CHOICE: only an `inbox` filing touches play_state (its record then exists even
        when a barrier blocks the NEW, as PROTOCOL §7.2 admissions); a `queue` or
        `nothing` admission creates no play_state record."""
        did, r = obj["decision_id"], obj["result"]
        action = r["action"]
        E, G = self._route_scope(r["episode_id"], r["generation_id"])
        place = r["proposal_id"] if r["proposal_id"] is not None else did
        key = ("episode", E, G)
        value = {"generation_id": G, "admission_id": did, "proposal_id": r["proposal_id"], "source": r["source"],
                 "action": action}
        o = r["episode_id"]
        self._add(key, _Contribution(place, "admitted", G, {"adm": did, "value": value}, orig=o))
        if action == "inbox":
            self._add(key, _Contribution(place, "admission", G, {"adm": did}, orig=o))
        elif action == "queue":
            self._add(key, _Contribution(place, "q_auto", G, {"key": r["key"], "pid": r["position_id"], "adm": did},
                                         orig=o))
        self.admitted.setdefault((G, E), []).append(did)

    def _admit_moved(self, obj):
        """PROTOCOL §9.4: a permanent move. Its aliases are claimed when unclaimed (first claim
        wins; with persisted allocations, exactly the claims it made); its refresh URL is
        a write of the generation's refresh URL at its place, like change_url."""
        did, inp, res = obj["decision_id"], obj["inputs"], obj["result"]
        P, G = inp["podcast_id"], inp["generation_id"]
        for a in res["aliases"]:
            norm = a["normalized"]
            if self.fixed_aliases is not None:
                d = self.fixed_aliases.get(norm)
                if d is not None and d["op_id"] == did:
                    self._claim_alias(norm, self._podcast_root(d["podcast_id"]), a["url"], did)
            else:
                # For the end of the merge chain (recovery may admit a merge first).
                self._claim_alias(norm, self._podcast_root(P), a["url"], did)
        if res["refresh_url"] is not None:
            # The generation's current podcast (a generation rehomed by a merge admitted
            # first in recovery moved there).
            Pg = self.generations[G]["podcast_id"] if G in self.generations else P
            self._add(("podcast", Pg), _Contribution(did, "moved", G, {"url": res["refresh_url"], "dec": did}))
            if Pg != P and self._known_podcast(Pg):
                self._refresh_podcast(Pg)

    def _effective(self, did):
        """Decisions of one (type, subject) compete: the smallest rank is effective (PROTOCOL §9.5, §6.11). Layouts never compete (PROTOCOL §7.6: materializations; every one is
        evidence of what was shown, and the highest-ranked listing a scope publishes its
        key)."""
        d = self.decisions[did]
        if d["obj"]["decision_type"] == LAYOUT:
            return True
        comp = self.by_subject[(d["obj"]["decision_type"], d["obj"]["subject"])]
        return min(comp, key=lambda x: self.decisions[x]["rank"]) == did

    def _descends(self, node, ancestor):
        """True if `ancestor` is a strict causal ancestor of `node`: a backward walk over
        the edges, pruned at nodes ranked before `ancestor` (ranks are a topological
        order). Ancestry never changes, so results are memoized."""
        if node == ancestor:
            return False
        k = (node, ancestor)
        hit = self._desc_memo.get(k)
        if hit is not None:
            return hit
        bound = self.rank[ancestor]
        found = False
        if self.rank[node] > bound:
            seen = set()
            stack = list(self.node_edges[node])
            while stack:
                x = stack.pop()
                if x == ancestor:
                    found = True
                    break
                if x in seen or self.rank[x] < bound:
                    continue
                seen.add(x)
                stack.extend(self.node_edges[x])
        self._desc_memo[k] = found
        return found

    def _accept_decision_arrival(self, arrival):
        """A recovered decision `{"decision": {...}}` (PROTOCOL §7.6: decisions admitted in
        recovery, including competitors of the same subject). It is not re-decided: its
        content is validated for form and taken as recorded."""
        if set(arrival) - {"decision"}:
            raise RuleInputError("a decision arrival has only `decision`")
        obj = arrival["decision"]
        want = {"decision_id", "decision_type", "decision_version", "dependencies", "normalized_time", "epoch", "seq",
                "subject", "inputs", "result"}
        if not isinstance(obj, dict) or set(obj) - {"index"} != want:
            raise RuleInputError("a decision has exactly the members %s" % sorted(want))
        did = obj["decision_id"]
        if not c.is_uuid(did):
            raise RuleInputError("decision_id is not a UUID")
        if did in self.decisions:
            if c.canonical(self.decisions[did]["obj"]) != c.canonical(obj):
                raise RuleInputError("decision %s arrives with different content" % did)
            return  # a duplicate delivery
        if did in self.prio or did in self.facts:
            raise RuleInputError("decision_id %s is already a node ID" % did)
        if obj["decision_type"] not in DECISION_TYPES or obj["decision_version"] != 1:
            raise RuleInputError("unknown decision %s v%s" % (obj["decision_type"], obj["decision_version"]))
        deps = obj["dependencies"]
        if (not isinstance(deps, list) or not all(isinstance(d, str) for d in deps)
                or any(_bkey(a) >= _bkey(b) for a, b in zip(deps, deps[1:]))):
            raise RuleInputError("dependencies must be sorted node IDs without duplicates")
        for d in deps:
            if d not in self.prio:
                raise RuleInputError("dependency %s is unknown: dependency_missing" % d)
        for k in ("normalized_time", "epoch", "seq"):
            if not _is_plain_int(obj[k]) or abs(obj[k]) > c.MAX_SAFE_INTEGER:
                raise RuleInputError("%s is not a safe integer" % k)
        # A rebuild replays the archive, whose dependency times may be later than the
        # decision's (see _accept_recovered_decision).
        if not self.rebuild_mode and obj["normalized_time"] < max([self.node_time[d] for d in deps],
                                                                   default=obj["normalized_time"]):
            raise RuleInputError("a decision's normalized_time is at least its dependencies'")
        subj = obj["subject"]
        sf = self.facts.get(subj) if isinstance(subj, str) else None
        try:
            _check_decision_body(obj)
        except _Invalid as e:
            raise RuleInputError("decision %s: %s" % (did, e))
        if obj["decision_type"] in CATALOG_DECISIONS:
            self._check_catalog_decision_refs(obj)
            self._admit_decision(copy.deepcopy(obj))
            return
        if obj["decision_type"] in IMPORT_DECISIONS + (LAYOUT,):
            self._check_import_decision_refs(obj)
            self._admit_decision(copy.deepcopy(obj))
            return
        if subj not in deps or ((sf is None or not sf["admitted"]) and subj not in self.prio):
            raise RuleInputError("the subject must be an admitted trigger among the dependencies")
        if obj["decision_type"] == RESOLVED_ADD:
            st = sf["fact"] if sf is not None else None
            r = obj["result"]
            if st is None or st["type"] != "session.start":
                raise RuleInputError("a resolved add's subject is the start of the episode it adds")
            # CHOICE: the episode is not compared with the start's: it is where the
            # start routed when the add was resolved, which recovery may reach only later.
        self._admit_decision(copy.deepcopy(obj))

    # -- PROTOCOL §6.11: recovery ------------------------------------------------------
    #
    # An engine taking recovery uploads keeps the archive beside its derived state. A
    # recovered fact or decision is archived (validated against the archive) and derived
    # state is rebuilt from the archive before it is next read: every archived fact in
    # receipt order (epoch, seq, index, op_id) with its receipt's outcome, every decision as
    # recorded at its own position, each subscription by its effective resolution (PROTOCOL §6.10,
    # PROTOCOL §6.11: derived state is a function of the archive in receipt order, so the order of
    # uploads does not matter). Live acceptances apply to that state incrementally.

    _BIG = c.MAX_SAFE_INTEGER + 1
    _IMPLICIT = b"\xff"  # an implicit decision's rank tie-break: after any decision ID

    def _ensure(self):
        if self.dirty and self.archiving:
            self._rebuild()
        if self.pub_dirty:
            self._sync_pub()

    @staticmethod
    def _strip_arrival(arrival):
        return {k: copy.deepcopy(arrival[k])
                for k in ("fact", "receive_time", "normalized_time", "edges", "capabilities") if k in arrival}

    def _archive_live(self, arrival):
        """Archives what a live acceptance archived (a fact, admitted or rejected), with the
        receipt it answered."""
        op = arrival["fact"]["op_id"] if isinstance(arrival.get("fact"), dict) else None
        info = self.facts.get(op)
        if info is None or op in self.ar_facts:
            return  # refused before archiving, or a transport duplicate or conflict
        node = op in self.prio
        self.ar_facts[op] = {"arrival": self._strip_arrival(arrival), "pos": info["rkey"][:3],
                             "outcome": copy.deepcopy(self.outcomes.get(op)), "node": node, "hash": info["hash"],
                             "recovered": False, "superseded": set(), "values": copy.deepcopy(self.recorded.get(op))}
        if node:
            self.ar_nodes[op] = self.node_time[op]
        else:
            self.ar_rejected[op] = list(info["frontier"])

    def _archive_decision(self, obj):
        did = obj["decision_id"]
        self.ar_dec[did] = {"obj": copy.deepcopy(obj), "type": obj["decision_type"], "subject": obj["subject"],
                            "result": copy.deepcopy(obj["result"]),
                            "pos": (obj["epoch"], obj["seq"], obj.get("index", self._BIG)), "id": did}
        if obj["decision_type"] not in SUBSCRIPTION_DECISIONS:
            self.ar_nodes[did] = obj["normalized_time"]

    def _implicit(self, dtype, subject, result, pos, op):
        """A podcast.alias or podcast.resolution decision the server made (live, or at the
        end of a recovery request) for the registration `op`. Vectors do not record its ID
        (Go draws it); it ranks at its commit, after any decision of the same commit
        (CHOICE). `decision_object` gives it an ID for an upload."""
        nt = max(self._ctx["receive_time"], self.node_time[op])
        self.ar_dec[("implicit", dtype, subject, pos)] = {
            "obj": None, "type": dtype, "subject": subject, "result": dict(result), "pos": pos, "id": None,
            "op": op, "nt": nt}

    def decision_object(self, e, decision_id):
        """An archived decision as a contract-v1 decision (without key_id and mac); an
        implicit one with the given ID."""
        if e["id"] is not None:
            return copy.deepcopy(e["obj"])
        return {"decision_id": decision_id, "decision_type": e["type"], "decision_version": 1,
                "dependencies": [e["op"]], "normalized_time": e["nt"], "epoch": e["pos"][0], "seq": e["pos"][1],
                "index": e["pos"][2], "subject": e["subject"], "inputs": {}, "result": copy.deepcopy(e["result"])}

    def _ar_rank(self, e):
        return (e["pos"][0], e["pos"][1], _bkey(e["id"]) if e["id"] is not None else self._IMPLICIT)

    def _epoch_evidence(self, epoch):
        """PROTOCOL §6.11: a verified receipt or decision at or above the account's epoch, for an
        entry the archive does not hold, proves a lost timeline: before it is joined the
        account moves to max(high-water, its epoch) + 1."""
        if epoch >= self.epoch:
            self.epoch = self.high_water = max(self.high_water, epoch) + 1

    def _conflict(self, ident, why):
        # PROTOCOL §6.11: a conflict changes nothing; it is listed (diagnostics, not records).
        self.transport.append((ident, why))

    def _accept_recovered_fact(self, arrival):
        """PROTOCOL §6.11: a recovered fact. Known (the same receipt, an earlier one, or a known
        superseded one), a conflict (other content, or the ID of a decision), the
        replacement of a later provisional receipt (PROTOCOL §6.11), or new."""
        fact = arrival["fact"]
        rt, nt, edges = arrival["receive_time"], arrival["normalized_time"], arrival["edges"]
        for v, what in ((rt, "receive_time"), (nt, "normalized_time")):
            if not _is_plain_int(v) or abs(v) > c.MAX_SAFE_INTEGER:
                raise RuleInputError("%s is not a safe integer" % what)
        if not isinstance(edges, list) or not all(isinstance(e, str) for e in edges):
            raise RuleInputError("edges must be an array of node IDs")
        if not isinstance(arrival["capabilities"], list) or not all(isinstance(x, str) for x in arrival["capabilities"]):
            raise RuleInputError("capabilities must be an array of strings")
        if not isinstance(fact, dict):
            raise RuleInputError("fact is not an object (unarchivable: invalid_request)")
        why = unarchivable_reason(fact)
        if why is not None:
            raise RuleInputError("unarchivable fact (invalid_request, nothing consumed): %s" % why)
        op = fact["op_id"]
        h = c.content_hash(fact)
        if op in self.ar_dec:
            self._conflict(op, "conflict")
            return
        pos, fixed = self._receipt(arrival, True, op)
        explicit = "receipt" in arrival
        e = self.ar_facts.get(op)
        if e is not None:
            if e["hash"] != h:
                self._conflict(op, "conflict")
                return
            if pos[:3] == e["pos"] or pos[:3] in e["superseded"]:
                self.transport.append((op, "known"))
                return
            if pos > e["pos"] + (_bkey(op),):
                self._keep_later_receipt(e, op, arrival, pos, fixed, explicit)
                return
        fixed = self._check_recovered_fact(arrival, fixed)
        if explicit:
            self._epoch_evidence(pos[0])
        rejected = fixed is not None and fixed["outcome"] == REJECTED
        if e is not None:
            # PROTOCOL §6.11: the original replaces the provisional receipt, which is kept.
            e["superseded"].add(e["pos"])
            e["pos"], e["outcome"], e["recovered"] = pos[:3], fixed, True
            e["values"] = copy.deepcopy((arrival.get("receipt") or {}).get("values"))
            if rejected and e["node"]:
                e["node_only"] = True
            else:
                e["arrival"], e["node"] = self._strip_arrival(arrival), not rejected
                e.pop("node_only", None)
            self.transport.append((op, "replaced"))
        else:
            e = self.ar_facts[op] = {"arrival": self._strip_arrival(arrival), "pos": pos[:3], "outcome": fixed,
                                     "node": not rejected, "hash": h, "recovered": True, "superseded": set(),
                                     "values": copy.deepcopy((arrival.get("receipt") or {}).get("values"))}
        if e["node"]:
            if not e.get("node_only"):
                self.ar_nodes[op] = nt
            self.ar_rejected.pop(op, None)
        else:
            self.ar_nodes.pop(op, None)
            self.ar_rejected[op] = sorted(set(edges), key=_bkey)
        self.dirty = True

    def _keep_later_receipt(self, e, op, arrival, pos, fixed, explicit):
        """A later receipt of an archived fact (recovery is a union, whatever the upload order). It is known, unless the archived receipt
        is a rejection and this later one admitted the fact (a provisional acceptance
        after a restore): then it is kept as a superseded receipt and the fact is a node
        that contributes nothing, exactly as when the provisional receipt came first
        and the original rejection replaced it (PROTOCOL §6.11)."""
        if e["node"]:
            self.transport.append((op, "known"))
            return
        fixed = self._check_recovered_fact(arrival, fixed)
        if fixed is not None and fixed["outcome"] == REJECTED:
            self.transport.append((op, "known"))
            return
        if explicit:
            self._epoch_evidence(pos[0])
        e["superseded"].add(pos[:3])
        e["arrival"], e["node"], e["node_only"] = self._strip_arrival(arrival), True, True
        self.ar_nodes[op] = arrival["normalized_time"]
        self.ar_rejected.pop(op, None)
        self.transport.append((op, "joined"))
        self.dirty = True

    def _check_recovered_fact(self, arrival, fixed):
        """The checks a recovered fact passes against the archive (the rebuild applies the
        rest): a rejected one's edges are known nodes and its normalized time its receive
        time; an admitted one's edges are its resolved frontier (or, before a dependency
        that is not a node yet, known nodes) and its normalized time admission's. Returns
        the receipt's outcome (an invalid envelope is a rejection)."""
        fact = arrival["fact"]
        rt, nt, edges = arrival["receive_time"], arrival["normalized_time"], arrival["edges"]
        env_err = envelope_reason(fact)
        if env_err is not None and fixed is None:
            fixed = _outcome(REJECTED, env_err)
        if fixed is not None and fixed["outcome"] == REJECTED:
            if sorted(set(edges), key=_bkey) != edges or not all(x in self.ar_nodes for x in edges):
                raise RuleInputError("a recovered rejected fact's edges are its receipt's frontier of known nodes")
            if nt != rt:
                raise RuleInputError("a rejected fact's normalized_time is its receive_time")
            return fixed
        if env_err is not None:
            raise RuleInputError("the receipt of %s says it was admitted, but its envelope is invalid" % fact["op_id"])
        if fact["type"] not in OPERATION_CAPABILITY or fact["type_version"] != 1:
            raise RuleInputError("%s v%s is unknown: version_unsupported, never accepted"
                                 % (fact["type"], fact["type_version"]))
        frontier, complete = set(), True
        for d in fact["dependencies"]:
            if d in self.ar_nodes:
                frontier.add(d)
            elif d in self.ar_rejected:
                frontier.update(self.ar_rejected[d])
            else:
                complete = False
        prev = fact.get("previous_op_id")
        if prev is not None and prev not in self.ar_facts:
            complete = False
        if complete:
            if sorted(set(edges), key=_bkey) != sorted(frontier, key=_bkey) or len(edges) != len(set(edges)):
                raise RuleInputError("edges %s differ from the resolved frontier %s" % (edges, sorted(frontier)))
        elif sorted(set(edges), key=_bkey) != edges or not all(x in self.ar_nodes for x in edges):
            raise RuleInputError("a recovered fact's edges must be sorted known causal nodes")
        try:
            want = c.normalize_time(fact["client_time"], rt, [self.ar_nodes[x] for x in edges])
        except c.ContractError as err:
            raise RuleInputError("time normalization: %s" % err)
        if nt != want:
            raise RuleInputError("normalized_time %d differs from admission's %d" % (nt, want))
        return fixed

    def _accept_recovered_decision(self, arrival, chooser):
        """PROTOCOL §6.11: a recovered decision, archived at its rank as recorded (its dependencies
        must be nodes), never re-decided. An identity decision is then checked against the
        effective decisions of its evidence key and equivalents are unified (PROTOCOL §6.11),
        with the server's choices for those merges in the arrival's `decisions`."""
        if set(arrival) - {"decision", "decisions", "receive_time"}:
            raise RuleInputError("a decision arrival has decision and optionally decisions and receive_time")
        obj = arrival["decision"]
        want = {"decision_id", "decision_type", "decision_version", "dependencies", "normalized_time", "epoch", "seq",
                "subject", "inputs", "result"}
        if not isinstance(obj, dict) or set(obj) - {"index"} != want:
            raise RuleInputError("a decision has exactly the members %s" % sorted(want))
        did = obj["decision_id"]
        if not c.is_uuid(did):
            raise RuleInputError("decision_id is not a UUID")
        if did in self.ar_facts:
            self._conflict(did, "conflict")
            return
        if did in self.ar_dec:
            if c.canonical(self.ar_dec[did]["obj"]) != c.canonical(obj):
                self._conflict(did, "conflict")
            else:
                self.transport.append((did, "known"))
            return
        if obj["decision_type"] not in DECISION_TYPES or obj["decision_version"] != 1:
            raise RuleInputError("unknown decision %s v%s" % (obj["decision_type"], obj["decision_version"]))
        deps = obj["dependencies"]
        if (not isinstance(deps, list) or not all(isinstance(d, str) for d in deps)
                or any(_bkey(a) >= _bkey(b) for a, b in zip(deps, deps[1:]))):
            raise RuleInputError("dependencies must be sorted node IDs without duplicates")
        for d in deps:
            if d not in self.ar_nodes:
                raise RuleInputError("dependency %s is unknown: dependency_missing" % d)
        for k in ("normalized_time", "epoch", "seq") + (("index",) if "index" in obj else ()):
            if not _is_plain_int(obj[k]) or abs(obj[k]) > c.MAX_SAFE_INTEGER:
                raise RuleInputError("%s is not a safe integer" % k)
        # No check of its normalized time against its dependencies': a dependency may
        # hold a receipt the decision's server never saw, at a later time (re-pushed live
        # on a restored timeline whose positions precede the original's: an undetected
        # restore, PROTOCOL §6.11). Times are never reclamped; causal order still places the
        # decision after it (PROTOCOL §6.9; the recovery seed 6100611).
        try:
            _check_decision_body(obj)
        except _Invalid as err:
            raise RuleInputError("decision %s: %s" % (did, err))
        t, subj = obj["decision_type"], obj["subject"]
        sf = self.ar_facts.get(subj) if isinstance(subj, str) else None
        if t == IMPORT_EPISODE and _unmodelled_import(obj):
            raise RuleInputError("an imported statistics baseline is not modelled by this reference")
        if t == RESOLUTION:
            if sf is None or not sf["node"] or sf["arrival"]["fact"]["type"] not in ("podcast.subscribe",
                                                                                     "podcast.register_preview"):
                raise RuleInputError("a resolution's subject is an admitted subscribe or preview registration")
        elif t == ALIAS:
            f = self.ar_facts.get(deps[0])
            f = f["arrival"]["fact"] if f is not None else None
            imp = self.ar_dec.get(deps[0])
            if imp is not None and imp["type"] == IMPORT_PODCAST:
                # An imported podcast's alias depends on its import.podcast decision.
                if "alias:" + c.normalize_url(imp["result"]["url"]) != subj:
                    raise RuleInputError("an imported podcast's alias is its import's feed URL")
            elif (f is None or f["type"] not in ("podcast.subscribe", "podcast.register_preview")
                    or not isinstance(f.get("payload"), dict)
                    or "alias:" + c.normalize_url(f["payload"].get("feed_url", "")) != subj):
                raise RuleInputError("an alias depends on the registration that created it, for its feed URL")
        elif t == ADMISSION:
            # What can be checked against the archive (the rebuild checks the rest).
            if obj["inputs"]["found_by"] not in deps:
                raise RuleInputError("an admission depends on what found the episode")
            pid = obj["result"]["proposal_id"]
            if pid is not None:
                pf = self.ar_facts.get(pid)
                f = pf["arrival"]["fact"] if pf is not None and pf["node"] else None
                if f is None or f["type"] != PROPOSE or f["author_id"] != obj["result"]["source"]:
                    raise RuleInputError("a device admission names an admitted proposal, by its source")
        elif t not in CATALOG_DECISIONS + IMPORT_DECISIONS + (LAYOUT,):
            if subj not in deps or ((sf is None or not sf["node"]) and subj not in self.ar_nodes):
                raise RuleInputError("the subject must be an admitted trigger among the dependencies")
            if t == RESOLVED_ADD and (sf is None or sf["arrival"]["fact"]["type"] != "session.start"):
                raise RuleInputError("a resolved add's subject is the start of the episode it adds")
        rt = arrival.get("receive_time", obj["normalized_time"])
        if not _is_plain_int(rt) or abs(rt) > c.MAX_SAFE_INTEGER:
            raise RuleInputError("receive_time is not a safe integer")
        supply = arrival.get("decisions")
        if supply is not None and (chooser is not None or not isinstance(supply, list)
                                   or not all(isinstance(x, dict) for x in supply)):
            raise RuleInputError("decisions must be an array of objects (and only without a chooser)")
        self._epoch_evidence(obj["epoch"])
        self._archive_decision(obj)
        self.dirty = True
        if t in UNIFIED_BY:
            self._ensure()
            self._ctx = {"arrival": arrival, "live": True, "receive_time": rt, "chooser": chooser,
                         "supply": list(supply or []), "caps": [], "fixed": None, "server": True}
            try:
                self._unify()
                # PROTOCOL §7.6: the unification commit ends with the publication rule.
                self._publish_queue()
                if self._ctx["supply"]:
                    raise RuleInputError("the arrival supplies %d unused decision choice(s)" % len(self._ctx["supply"]))
            finally:
                self._ctx = None
        elif supply:
            raise RuleInputError("the arrival supplies %d unused decision choice(s)" % len(supply))

    def _accept_recovery_end(self, arrival, chooser):
        """`{"recovery_end": true, "receive_time", "decisions"?}`: the end of an upload
        request (PROTOCOL §6.11), a server commit at the arrival's position in the current epoch:
        every admitted subscribe or preview registration still without a podcast.resolution
        is resolved as a live one would be (a provisional decision, in receipt order);
        equivalent identity decisions are unified; and the publication rule records one
        queue.layout when the published keys do not give the replay's order
        (PROTOCOL §7.6; it replaces PROTOCOL §6.11's sort reconciliation)."""
        if set(arrival) - {"recovery_end", "receive_time", "decisions"} or arrival.get("recovery_end") is not True:
            raise RuleInputError("a recovery_end arrival has recovery_end: true, receive_time and optionally decisions")
        rt = arrival.get("receive_time")
        if not _is_plain_int(rt) or abs(rt) > c.MAX_SAFE_INTEGER:
            raise RuleInputError("receive_time is not a safe integer")
        supply = arrival.get("decisions")
        if supply is not None and (chooser is not None or not isinstance(supply, list)
                                   or not all(isinstance(x, dict) for x in supply)):
            raise RuleInputError("decisions must be an array of objects (and only without a chooser)")
        if not self.archiving:
            raise RuleInputError("a replay receives decisions, not the end of a recovery request")
        self._ensure()
        commit = (self.epoch, self.narrivals)
        self._ctx = {"arrival": arrival, "live": True, "receive_time": rt, "chooser": chooser,
                     "supply": list(supply or []), "caps": [], "fixed": None, "server": True}
        try:
            resolved = set(e["subject"] for e in self.ar_dec.values() if e["type"] == RESOLUTION)
            pending = sorted((op for op, e in self.ar_facts.items()
                              if e["node"] and not e.get("node_only") and op not in resolved
                              and e["arrival"]["fact"]["type"] in ("podcast.subscribe", "podcast.register_preview")),
                             key=lambda op: self.ar_facts[op]["pos"] + (_bkey(op),))
            for op in pending:
                parsed = parse_payload(self.ar_facts[op]["arrival"]["fact"], self.account_id)
                res = self._resolve(op, parsed)
                if res["mode"] == "created":
                    self._implicit(ALIAS, "alias:" + c.normalize_url(parsed["feed_url"]), {"podcast_id": res["podcast_id"]},
                                   commit + (self._next_decision_index(),), op)
                self._implicit(RESOLUTION, op, res, commit + (self._next_decision_index(),), op)
                self.dirty = True
                self._ensure()
            self._unify()
            # PROTOCOL §7.6: the publication rule ends the request: one layout when the published
            # keys do not give the replay's order, recovery-only requests included.
            self._publish_queue()
            if self._ctx["supply"]:
                raise RuleInputError("the arrival supplies %d unused decision choice(s)" % len(self._ctx["supply"]))
        finally:
            self._ctx = None

    def _rebuild(self):
        """Derived state from the archive (see above). A non-effective resolution that
        created or opened something still creates it (identity: never split, PROTOCOL §6.11), without
        its subscription's effect: in the effective resolution's own podcast, its
        generation joins the effective one's; elsewhere a unification merge joins it."""
        groups = {}
        for e in self.ar_dec.values():
            if e["type"] == RESOLUTION:
                groups.setdefault(e["subject"], []).append(e)
        eff, extra = {}, []
        for op, es in groups.items():
            es.sort(key=self._ar_rank)
            eff[op] = dict(es[0]["result"])
            for o in es[1:]:
                r = o["result"]
                if r["mode"] in ("created", "opened") and (r["podcast_id"], r.get("generation_id")) != (
                        eff[op]["podcast_id"], eff[op].get("generation_id")):
                    extra.append(o)
        outcomes = dict((op, e["outcome"]) for op, e in self.ar_facts.items() if e["outcome"] is not None)
        inner = Engine(self.account_id, self.namespace, fixed_resolutions=eff, replay=True, fixed_outcomes=outcomes,
                       rebuild=True)
        items = []
        for op, e in self.ar_facts.items():
            items.append((e["pos"] + (_bkey(op),), "fact", op))
        for k, e in self.ar_dec.items():
            if e["type"] in SUBSCRIPTION_DECISIONS:
                continue
            deps = e["obj"]["dependencies"]
            f = self.ar_facts.get(deps[0]) if len(deps) == 1 else None
            if e["type"] == MAPPING and f is not None and f["pos"][:2] == e["pos"][:2] and f["node"]:
                inner.pre_admit.setdefault(deps[0], []).append(e["obj"])
                continue
            items.append((e["pos"] + (_bkey(e["id"]),), "decision", k))
        for objs in inner.pre_admit.values():
            objs.sort(key=lambda o: (o.get("index", self._BIG), _bkey(o["decision_id"])))
        for o in extra:
            items.append((o["pos"] + (self._IMPLICIT if o["id"] is None else _bkey(o["id"]),), "identity", o))
        items.sort(key=lambda x: x[0])
        waiting = []
        for it in items:
            waiting.append(it)
            progress = True
            while progress:
                progress = False
                for w in waiting:
                    if self._rebuild_ready(inner, w):
                        waiting.remove(w)
                        self._rebuild_run(inner, w)
                        progress = True
                        break
        for w in waiting:
            # CHOICE: a fact whose prerequisites never arrived is joined anyway (a
            # subscription naming a podcast not known contributes nothing); a decision
            # stays pending until what it names arrives.
            if w[1] == "fact":
                self._rebuild_run(inner, w)
        keep = ("ar_facts", "ar_dec", "ar_nodes", "ar_rejected", "outcomes", "created", "transport", "narrivals",
                "epoch", "high_water", "status_live", "status_logged", "stored", "fetch_pending", "_ctx", "archiving")
        saved = dict((k, self.__dict__[k]) for k in keep)
        rids = self.refresh_ids
        self.__dict__.clear()
        self.__dict__.update(inner.__dict__)
        self.__dict__.update(saved)
        self.refresh_ids = rids | inner.refresh_ids
        self.replay = False
        self.fixed_resolutions = self.fixed_aliases = self.fixed_episodes = self.fixed_outcomes = None
        self.rebuild_mode = False
        self.pre_admit = {}
        self.dirty = False
        self.pub_dirty = True

    def _rebuild_ready(self, inner, item):
        kind = item[1]
        if kind == "fact":
            e = self.ar_facts[item[2]]
            if not all(x in inner.prio for x in e["arrival"]["edges"]):
                return False
            f = e["arrival"]["fact"]
            if e["node"] and not e.get("node_only") and f["type"] in ("podcast.subscribe", "podcast.register_preview"):
                r = inner.fixed_resolutions.get(item[2])
                if r is not None and r["mode"] != "created":
                    if not inner._known_podcast(r["podcast_id"]):
                        return False
                    if r["mode"] == "joined" and r.get("generation_id") not in inner.generations:
                        return False
            return True
        if kind == "identity":
            o = item[2]
            return o["subject"] in inner.facts and (o["result"]["mode"] == "created"
                                                    or inner._known_podcast(o["result"]["podcast_id"]))
        e = self.ar_dec[item[2]]
        obj = e["obj"]
        if not all(d in inner.prio for d in obj["dependencies"]):
            return False
        if obj["decision_type"] in CATALOG_DECISIONS:
            try:
                inner._check_catalog_decision_refs(obj)
            except RuleInputError:
                return False
        if obj["decision_type"] in IMPORT_DECISIONS:
            try:
                inner._check_import_decision_refs(obj)
            except RuleInputError:
                return False
        return True

    def _rebuild_run(self, inner, item):
        inner.narrivals += 1
        kind = item[1]
        if kind == "fact":
            e = self.ar_facts[item[2]]
            a = copy.deepcopy(e["arrival"])
            a["recovery"] = True
            a["receipt"] = dict(zip(("epoch", "seq", "index"), e["pos"]))
            if e["outcome"] is not None:
                a["receipt"].update(e["outcome"])
            if e.get("values"):
                a["receipt"]["values"] = copy.deepcopy(e["values"])
            if e.get("node_only"):
                a["node_only"] = True
            inner.accept(a)
        elif kind == "decision":
            inner.accept({"decision": copy.deepcopy(self.ar_dec[item[2]]["obj"])})
        else:
            inner._provisional_identity(item[2]["subject"], item[2]["result"])

    def _provisional_identity(self, op, res):
        """A non-effective resolution's podcast or generation (see _rebuild)."""
        P, G = res["podcast_id"], res.get("generation_id")
        f = self.facts[op]["fact"]
        parsed = parse_payload(f, self.account_id)
        preview = f["type"] == "podcast.register_preview"
        pod = self.podcasts.setdefault(P, {"creating_op": None, "generations": []})
        if res["mode"] == "created" and pod["creating_op"] is None:
            pod["creating_op"] = op
        if G is None or G in self.generations or G in self.gen_join:
            return
        e = self.fixed_resolutions.get(op)
        if e is not None and e.get("generation_id") is not None and self._podcast_root(e["podcast_id"]) == self._podcast_root(P):
            self.gen_join[G] = e["generation_id"]
            return
        self.generations[G] = {
            "podcast_id": P, "op_id": op, "kind": "preview" if preview else "subscribe",
            "predecessor": parsed["observed"]["generation_id"] if res["mode"] == "opened" else None,
            "policy": dict(parsed["policy"]) if "policy" in parsed else dict(PREVIEW_POLICY if preview else DEFAULT_POLICY),
            "url": parsed["feed_url"], "closed_by": None}
        pod["generations"].append(G)

    # -- PROTOCOL §6.11: unification of equivalent identity decisions -----------------

    def _unify(self):
        """Makes merges until no two effective identity decisions with the same evidence
        key disagree. Never splits: a later or provisional result is merged into the
        effective one (the survivor rules for podcasts: the earliest created, PROTOCOL §9.7)."""
        for _ in range(10000):
            if not self._unify_step():
                return
        raise RuntimeError("unification does not converge")

    def _unify_step(self):
        groups = {}
        for e in self.ar_dec.values():
            if e["type"] in (ALIAS, RESOLUTION, PODCAST_MERGE, EPISODE_MERGE, GUID_CHANGE):
                groups.setdefault((e["type"], e["subject"]), []).append(e)
        for key in sorted(groups, key=lambda k: (_bkey(k[0]), _bkey(k[1]))):
            es = sorted(groups[key], key=self._ar_rank)
            t, eff = key[0], es[0]
            for o in es[1:]:
                re_, ro = eff["result"], o["result"]
                if t == ALIAS:
                    if self._unify_podcasts(re_["podcast_id"], ro["podcast_id"]):
                        return True
                elif t == RESOLUTION:
                    if ro["mode"] in ("created", "opened") and self._unify_podcasts(re_["podcast_id"], ro["podcast_id"]):
                        return True
                elif t == PODCAST_MERGE:
                    if self._unify_podcasts(re_["into"], ro["into"]):
                        return True
                elif t == EPISODE_MERGE:
                    if self._unify_episodes(re_["into"], ro["into"], eff, o):
                        return True
                elif self._unify_episodes(re_["episode_id"], ro["episode_id"], eff, o):
                    return True
        for (P, g) in sorted(self.mappings, key=lambda k: (_bkey(k[0]), _bkey(k[1]))):
            ms = [self.decisions[d] for d in self.mappings[(P, g)] if self._effective(d)]
            for a, b in self._equivalent_releases(ms):
                if self._unify_episodes(a["obj"]["result"]["episode_id"], b["obj"]["result"]["episode_id"],
                                        self.ar_dec[a["obj"]["decision_id"]], self.ar_dec[b["obj"]["decision_id"]]):
                    return True
        return False

    @staticmethod
    def _equivalent_releases(ms):
        """PROTOCOL §9.7: two releases (occurrence mappings) of one podcast and GUID are the same
        when they share an occurrence (publication time and an enclosure), or when each is
        the other's only release within the 7-day window. Pairs (earlier rank, later).

        CHOICE: releases decided in one commit are one side and never the same (a GUID's
        first dispute groups its earlier occurrences into one release, which may lie within
        7 days of the new one); a release's window neighbours are those of other commits.
        Within one timeline a release made in a later commit lies beyond every existing
        release's window, so only another timeline's releases can match."""
        def dated(m):
            return m["obj"]["result"]["publication_time"]

        def commit(m):
            return m["rank"][:2]

        def near(x):
            return [m for m in ms if commit(m) != commit(x) and dated(m) is not None
                    and abs(dated(m) - dated(x)) <= RELEASE_WINDOW_MS]
        out = []
        for i, a in enumerate(ms):
            for b in ms[i + 1:]:
                if commit(a) == commit(b):
                    continue
                ra, rb = a["obj"]["result"], b["obj"]["result"]
                shared = ra["publication_time"] == rb["publication_time"] and set(ra["enclosures"]) & set(rb["enclosures"])
                window = False
                if dated(a) is not None and dated(b) is not None and abs(dated(a) - dated(b)) <= RELEASE_WINDOW_MS:
                    window = near(a) == [b] and near(b) == [a]
                if shared or window:
                    out.append((a, b) if a["rank"] < b["rank"] else (b, a))
        return sorted(out, key=lambda p: (p[0]["rank"], p[1]["rank"]))

    def _unify_podcasts(self, Pa, Pb):
        """PROTOCOL §6.11 (CHOICE): the survivor is the podcast created first, by the rank of the
        decision that created it (its podcast.alias or creating podcast.resolution: the
        recovered original wins over a podcast created after the restore), else by its
        creating fact's rank (PROTOCOL §9.7)."""
        ra, rb = self._podcast_root(Pa), self._podcast_root(Pb)
        if ra == rb or not self._known_podcast(ra) or not self._known_podcast(rb):
            return False
        ka, kb = self._creation_rank(ra), self._creation_rank(rb)
        self._make_podcast_merge(None, ra, rb, None, None, survivor=ra if ka <= kb else rb)
        return True

    def _creation_rank(self, P):
        ranks = [self._ar_rank(e) for e in self.ar_dec.values()
                 if (e["type"] == ALIAS and e["result"]["podcast_id"] == P)
                 or (e["type"] == RESOLUTION and e["result"]["podcast_id"] == P and e["result"]["mode"] == "created")]
        if ranks:
            return (0,) + min(ranks)
        return (1, self.rank[self.podcasts[P]["creating_op"]])

    def _unify_episodes(self, Ea, Eb, ea, eb):
        """Merges what the later decision `eb` named into what the effective `ea` named."""
        ra, rb = self._root(Ea), self._root(Eb)
        if ra == rb or ea["id"] is None or eb["id"] is None:
            return False
        P = (ea["obj"]["inputs"].get("podcast_id") or self.episodes.get(ra, {}).get("podcast_id"))
        P = self._podcast_root(P) if P is not None else P
        ch = self._choice(EPISODE_MERGE, {})
        self._new_decision(EPISODE_MERGE, "merge:%s" % rb, ch, {"podcast_id": P, "cause": "recovered", "by": ea["id"]},
                           {"from": rb, "into": ra}, [ea["id"], eb["id"]])
        return True

    # -- PROTOCOL §9.7: identity evidence, derivation and routing ------------------------

    def _root(self, E):
        return self.root.get(E, E)

    def _podcast_root(self, P):
        seen = set()
        while P in self.pmerged and P not in seen:
            seen.add(P)
            P = self.pmerged[P]["into"]
        return P

    @staticmethod
    def _norm(u):
        if u is None:
            return None
        try:
            return c.normalize_url(u)
        except c.ContractError:
            return u

    def _occ3(self, occ):
        """A fact's occurrence (PROTOCOL §9.7): its EpisodeRef occurrence's GUID,
        normalized enclosure and publication time, and its GUID kind (atom-id, else
        rss-guid)."""
        g = occ.get("guid")
        g = c.guid_trim(g) if isinstance(g, str) else None
        pt = occ.get("publication_time")
        return (g, self._norm(occ.get("enclosure_url")), pt), ("atom-id" if occ.get("k") == "atom-id" else "rss-guid")

    def _eid(self, P, k, v):
        return c.episode_id(self.namespace, P, k, v)

    def _plain_ids(self, P, occ3, kfam):
        """The plain derivations (PROTOCOL §3.3 rules 2-4) of an occurrence, as roots: its GUID's
        (rule 2) and GUID+enclosure (rule 3) IDs, or its enclosure's without a GUID."""
        g, u, _ = occ3
        out = []
        if g is not None:
            out.append(self._eid(P, kfam, g))
            if u is not None:
                out.append(self._eid(P, "rss-guid+enclosure", c.episode_v_guid_enclosure(g, u)))
        elif u is not None:
            out.append(self._eid(P, "enclosure", u))
        return set(self._root(e) for e in out)

    def _releases(self, P, g):
        """The release groups of a disputed GUID (PROTOCOL §9.7): the results of its
        effective occurrence-mapping decisions. PROTOCOL §6.11 (CHOICE): of releases whose
        episodes were unified (one root), the earliest-ranked one. Each release lists, after its own enclosures, those its effective enclosure
        additions add (in rank order)."""
        out, roots = [], {}
        ds = sorted((d for d in self.mappings.get((P, g), ()) if self._effective(d)),
                    key=lambda d: self.decisions[d]["rank"])
        for d in ds:
            r = self.decisions[d]["obj"]["result"]
            root = self._root(r["episode_id"])
            if root not in roots:
                roots[root] = dict(r, enclosures=list(r["enclosures"]))
                out.append(roots[root])
        adds = sorted((d for d in self.additions.get((P, g), ()) if self._effective(d)),
                      key=lambda d: self.decisions[d]["rank"])
        for d in adds:
            a = self.decisions[d]["obj"]["result"]
            r = roots.get(self._root(a["episode_id"]))
            if r is not None and a["enclosure"] not in r["enclosures"]:
                r["enclosures"].append(a["enclosure"])
        return out

    def _disputed(self, P, g):
        return bool(self.mappings.get((P, g))) and bool(self._releases(P, g))

    def _original(self, P, g):
        for r in self._releases(P, g):
            if r["original"]:
                return self._root(r["episode_id"])
        return None

    def _family(self, P, g):
        return set(self._root(r["episode_id"]) for r in self._releases(P, g))

    def _release_for(self, P, g, u, d):
        """PROTOCOL §9.7 resolver: a dated item goes to the release whose publication time lies
        within 7 days of its own (CHOICE: the nearest; ties to the earlier release, then
        the smaller episode ID); an undated one to the release holding its enclosure if
        exactly one does, else to the original. Returns None for a dated item no release
        is within 7 days of (a release the server does not know yet)."""
        rels = self._releases(P, g)
        if d is not None:
            near = [r for r in rels if r["publication_time"] is not None
                    and abs(d - r["publication_time"]) <= RELEASE_WINDOW_MS]
            if near:
                r = min(near, key=lambda r: (abs(d - r["publication_time"]), r["publication_time"],
                                             _bkey(r["episode_id"])))
                return self._root(r["episode_id"])
            return None
        if u is not None:
            withu = [r for r in rels if u in r["enclosures"]]
            if len(withu) == 1:
                return self._root(withu[0]["episode_id"])
        return self._original(P, g)

    def _lookup_alias(self, P, kind, value):
        a = self.lookup.get(P, {}).get((kind, value))
        return self._root(a["episode"]) if a is not None else None

    def _disputed_enclosures(self, P):
        """PROTOCOL §9.7: an enclosure observed for two episodes (roots) with different GUIDs (an
        absent GUID differs from any). CHOICE: computed from the evidence, not a decision."""
        by_u = {}
        for (g, u, _), e in self.evidence.get(P, {}).items():
            if u is not None and e["episode"] is not None:
                by_u.setdefault(u, set()).add((self._root(e["episode"]), g))
        out = set()
        for u, pairs in by_u.items():
            pl = sorted(pairs, key=lambda x: (_bkey(x[0]), _bkey(x[1] or "")))
            if any(a[0] != b[0] and a[1] != b[1] for a in pl for b in pl):
                out.add(u)
        return out

    def _derive(self, P, occ3, kfam, rooted=True):
        """PROTOCOL §9.7 derivation with the evidence (PROTOCOL §3.3 rules 1-4), as the server derives its
        own items and resolves a source podcast's episodes: an undisputed lookup alias (by
        GUID if the item has one, by enclosure only without one), the GUID's ID if it is
        not disputed, the release a disputed GUID resolves to (CHOICE: rule 3 when no
        release matches a dated item, the original for an undated one without enclosure),
        the enclosure's ID without a GUID. None without GUID and enclosure."""
        g, u, d = occ3
        rt = self._root if rooted else (lambda x: x)
        if g is not None:
            if self._disputed(P, g):
                r = self._release_for(P, g, u, d)
                if r is not None:
                    return r
                if u is not None:
                    return rt(self._eid(P, "rss-guid+enclosure", c.episode_v_guid_enclosure(g, u)))
                return self._original(P, g)
            a = self._lookup_alias(P, "guid", g)
            return a if a is not None else rt(self._eid(P, kfam, g))
        if u is not None:
            if u not in self._disputed_enclosures(P):
                a = self._lookup_alias(P, "enclosure", u)
                if a is not None:
                    return a
            return rt(self._eid(P, "enclosure", u))
        return None

    def _route_identity(self, P, E0, occ3, kfam):
        """PROTOCOL §9.7: a reference to a plain derivation of its occurrence, or to its
        disputed GUID's release family, goes where the server's rules put the occurrence
        (a lookup alias; the release its date or enclosure resolves to); a reference to
        anything else keeps what it names (an arbitrary claimed ID authorizes nothing).
        CHOICE: a dated occurrence of a disputed GUID that matches no release keeps its
        reference (the release is created by evidence, whose mapping then partitions)."""
        g, u, d = occ3
        if g is not None and self._disputed(P, g):
            if E0 in self._family(P, g) or E0 in self._plain_ids(P, occ3, kfam):
                r = self._release_for(P, g, u, d)
                if r is not None:
                    return r
            return E0
        if g is not None:
            if E0 == self._root(self._eid(P, kfam, g)):
                a = self._lookup_alias(P, "guid", g)
                if a is not None:
                    return a
            return E0
        if u is not None and E0 == self._root(self._eid(P, "enclosure", u)) and u not in self._disputed_enclosures(P):
            a = self._lookup_alias(P, "enclosure", u)
            if a is not None:
                return a
        return E0

    def _route_scope(self, E, G):  # E may be None: route a generation only
        """Where a decision's contribution to (E, G) lives now (PROTOCOL §9.7): the episode's root, and
        for a source generation joined by a podcast merge, the survivor's live class. So a
        decision admitted after a merge (in recovery) lands where the merge would have
        moved it."""
        G = self.gen_join.get(G, G)  # a provisional generation joined into the effective one
        seen = set()
        while G in self.generations and G not in seen:
            seen.add(G)
            pm = self.pmerged.get(self.generations[G]["podcast_id"])
            if pm is None:
                break
            mode, G2 = pm["gens"].get(G, ("closed", None))
            if mode != "joined":
                break
            G = G2
        return (self._root(E) if E is not None else None), G

    def _route(self, P, G, E, occ3, kfam):
        """Routes an episode reference at acceptance (PROTOCOL §9.7): through podcast
        merges by its original podcast and generation (a closed source generation answers
        generation_closed; a joined one goes to the survivor's live class, a rehomed one
        keeps its ID), the source episode to its mapping (CHOICE: an episode the merge did
        not list goes where the survivor's rules derive its occurrence), then merge
        redirects, then the release resolver. Returns ((E, P, G), None) or (None, reason)."""
        G = self.gen_join.get(G, G)  # a provisional generation joined into the effective one
        seen = set()
        while P in self.pmerged and P not in seen:
            seen.add(P)
            pm = self.pmerged[P]
            if G is not None:
                mode, G2 = pm["gens"].get(G, ("closed", None))
                if mode == "closed":
                    return None, R_CLOSED
            else:
                G2 = None  # a catalog record: no generation to route
            dest = self.root.get(E) or pm["episodes"].get(E) or self._derive(pm["into"], occ3, kfam) or E
            E, P, G = dest, pm["into"], G2
        E = self._root(E)
        return (self._route_identity(P, E, occ3, kfam), P, G), None

    def _walk_merges(self, E, P, occ, occ3, kfam, node):
        """Through each podcast merge of a chain, the episode a reference reaches at each
        survivor is registered (if new) and the previous one redirected to it, so the
        identity records do not depend on how much of the chain recovery had admitted when
        the fact arrived (CHOICE)."""
        seen, touched = set(), []
        while P in self.pmerged and P not in seen:
            seen.add(P)
            pm = self.pmerged[P]
            nxt = pm["episodes"].get(E) or self._derive(pm["into"], occ3, kfam, rooted=False)
            if nxt is None or nxt == E:
                break
            self._register(nxt, pm["into"], occ, node)
            if E not in self.root:
                self._merge_episode(E, nxt, self._gen_of(pm), P_dst=pm["into"], node=node)
                self._reindex_sessions()
            touched += [E, nxt]
            E, P = nxt, pm["into"]
        for x in touched + [self._root(x) for x in touched]:
            if x in self.episodes:
                self._refresh_episode(x)

    def _route_inplace(self, ref):
        """Routes a parsed EpisodeRef once, keeping what it named in ref["named"]: PROTOCOL §7.2 checks on what it names (unknown podcast, unknown generation), the episode's
        podcast against the podcast the reference resolves to (its merge family), then
        _route. The generation closure is checked on the routed reference (_check_ref)."""
        if "named" in ref:
            return
        E, P, G = ref["episode_id"], ref["podcast_id"], ref["generation_id"]
        occ3, kfam = self._occ3(ref["occurrence"])
        ref["named"], ref["occ3"], ref["kfam"] = (E, P, G), occ3, kfam
        reason = None
        if not self._known_podcast(P):
            reason = R_UNKNOWN_PODCAST
        elif G not in self.podcasts[P]["generations"]:
            reason = R_UNKNOWN_GENERATION
        else:
            # PROTOCOL §9.7 item 6: the episode's podcast is checked against the
            # podcast the reference resolves to, the named podcast's merge-family survivor,
            # in PROTOCOL §7.2 check order. A reference under a merged-away podcast naming
            # the survivor's episode is accepted whether that episode was registered first
            # or is named first; an episode of a podcast outside the family is still
            # episode_podcast_mismatch.
            b = self._binding(E)
            if b is not None and self._podcast_root(b["podcast_id"]) != self._podcast_root(P):
                reason = R_MISMATCH
        if reason is None:
            routed, reason = self._route(P, G, E, occ3, kfam)
            if routed is not None:
                ref["episode_id"], ref["podcast_id"], ref["generation_id"] = routed
        ref["route_reason"] = reason

    def _refs_of(self, typ, parsed):
        """Every EpisodeRef of a parsed payload (catalog observations route themselves)."""
        if parsed is None or typ == OBSERVE:
            return []
        # PROTOCOL §7.6: anchors, runs' context and a sort's observations are context,
        # routed by the replay itself, never here.
        out = list(parsed.get("refs", ()))
        for e in parsed.get("entries", ()):
            out.append(e[0])
        return out

    def _ctb(self, node, kind, ref, data=None):
        """A fact's contribution through one of its (routed) EpisodeRefs."""
        named = ref.get("named")
        ct = _Contribution(node, kind, ref["generation_id"], data, orig=named[0] if named else ref["episode_id"],
                           occ=ref.get("occ3"), ref=(named + (ref.get("kfam", "rss-guid"),)) if named else None)
        if named:
            ct.ngen = named[2]
        return ct

    # -- PROTOCOL §9.7: evidence and release groups --------------------------------------

    def _record(self, P, key, node, kfam="rss-guid"):
        """Records an occurrence as evidence (PROTOCOL §9.7), with its GUID kind; True if it
        was new."""
        ev = self.evidence.setdefault(P, {})
        if key in ev:
            return False
        ev[key] = {"episode": None, "node": node, "kfam": kfam}
        return True

    def _set_evidence_episode(self, P, key, E):
        e = self.evidence.get(P, {}).get(key)
        if e is not None and e["episode"] is None:
            e["episode"] = E

    def _detect(self, P, key, kfam, found_by, deps):
        """The reused-GUID rule (PROTOCOL §9.7), live only: an occurrence (g, u, d) of a GUID
        observed with another non-empty enclosure at a date more than 7 days away disputes
        it. The earliest release keeps the original episode (CHOICE: the episode the GUID
        derived to before the dispute, a lookup alias or its plain ID). A later release's
        ID is rss-guid+enclosure of its first enclosure unless an existing release has that
        ID (rotating enclosures): then a server-allocated `e0:` ID. Each release is an
        `episode.occurrence_mapping` decision (CHOICE: the original release too, written
        first, then the others by publication time), never recomputed.

        The first dispute partitions the GUID's evidence by release (see _partition), so occurrences observed before
        the dispute keep their own releases. Once disputed, an occurrence no release lies
        within 7 days of starts a new release by the same rule; a dated occurrence that
        resolves to a release not listing its enclosure adds the enclosure to it (enclosure additions are persisted and published), by an `episode.occurrence_mapping`
        decision of its own (CHOICE: subject `occurrence:<P>:<guid>:<episode>:<enclosure>`,
        result {episode_id, enclosure}; see _check_identity_decision_body)."""
        g, u, d = key
        if g is None or u is None or d is None or self._ctx is None or not self._ctx.get("live"):
            return
        occs = [k for k in self.evidence.get(P, {}) if k[0] == g]

        def far(x):
            return any(k[1] is not None and k[1] != x[1] and k[2] is not None and abs(x[2] - k[2]) > RELEASE_WINDOW_MS
                       for k in occs if k != x)
        hook = self._ctx.get("before_identity")
        if not self._disputed(P, g):
            if not far(key):
                return
            a = self._lookup_alias(P, "guid", g)
            orig = a if a is not None else self._root(self._eid(P, kfam, g))
            groups = self._partition(occs, far)
        else:
            rel = self._release_record(P, g, self._release_for(P, g, u, d))
            if rel is not None:
                if u in rel["enclosures"]:
                    return
                if hook is not None:
                    hook()
                ch = self._choice(MAPPING, {})
                E = rel["episode_id"]
                self._new_decision(MAPPING, "occurrence:%s:%s:%s:%s" % (P, g, E, u), ch,
                                   {"podcast_id": P, "guid": g, "found_by": found_by}, {"episode_id": E, "enclosure": u},
                                   deps)
                return
            if not far(key):
                return
            orig = None
            groups = [(d, [u], False)]
        if hook is not None:
            hook()
        for pt, encs, original in groups:
            allocated = False
            if original:
                E = orig
            else:
                E = self._eid(P, "rss-guid+enclosure", c.episode_v_guid_enclosure(g, encs[0]))
                taken = set(r["episode_id"] for r in self._releases(P, g)) | ({orig} if orig else set())
                allocated = E in taken
            ch = self._choice(MAPPING, {"allocate": allocated})
            if allocated:
                E = "e0:" + ch["occurrence_id"]
            self._new_decision(MAPPING, "occurrence:%s:%s:%s" % (P, g, E), ch,
                               {"podcast_id": P, "guid": g, "found_by": found_by},
                               {"episode_id": E, "original": original, "publication_time": pt,
                                "enclosures": list(encs), "allocated": allocated}, deps)

    @staticmethod
    def _partition(occs, far):
        """The release groups of a GUID's evidence at its first dispute (the first dispute partitions the existing evidence by
        release). Returns [(publication time, enclosures, original)], the
        original first, then by publication time. CHOICE, in two passes so that every
        occurrence lands where the resolver (PROTOCOL §9.7) puts it:

        1. Releases: the dated occurrences with an enclosure, in (date, enclosure) order.
           The first creates the original release; a later one creates a release when it
           is more than 7 days from every release so far and the dispute rule holds for it
           (another occurrence of the GUID, earlier or later, has a different enclosure and
           a date more than 7 days away), as a live observation arriving after the dispute
           would. Its date is the release's publication time and its enclosure the first.
        2. Enclosures: every such occurrence's enclosure is listed in the release nearest
           within 7 days (ties: the earlier release), after the creating one, in that order.
        Undated occurrences and occurrences without an enclosure are not grouped (they are
        never dispute evidence; the resolver places them when they are referenced)."""
        W = RELEASE_WINDOW_MS
        dated = sorted((k for k in occs if k[1] is not None and k[2] is not None), key=lambda k: (k[2], _bkey(k[1])))
        rels = []
        for k in dated:
            if not rels or (all(abs(k[2] - pt) > W for pt, _ in rels) and far(k)):
                rels.append((k[2], [k[1]]))
        for k in dated:
            near = [(abs(k[2] - pt), pt, i) for i, (pt, _) in enumerate(rels) if abs(k[2] - pt) <= W]
            if near:
                encs = rels[min(near)[2]][1]
                if k[1] not in encs:
                    encs.append(k[1])
        return [(pt, encs, i == 0) for i, (pt, encs) in enumerate(rels)]

    def _release_record(self, P, g, E):
        """The release of a disputed GUID whose episode (root) is E, with its enclosure
        additions, or None."""
        if E is None:
            return None
        for r in self._releases(P, g):
            if self._root(r["episode_id"]) == E:
                return dict(r, episode_id=E)
        return None

    def _admit_mapping(self, obj):
        """An occurrence mapping: the release joins the GUID's resolver, its episode is
        registered (by this decision) if new, and the GUID's family is repartitioned. An
        enclosure addition adds its enclosure to its release, which
        the resolver and identity_rules then list (CHOICE: it is kept apart from the
        releases, so it never competes with them or counts as a release in recovery's
        unification; it applies to the release whose episode has its episode's root,
        whenever both are admitted)."""
        inp, r = obj["inputs"], obj["result"]
        P, g = inp["podcast_id"], inp["guid"]
        if "enclosure" in r:
            self.additions.setdefault((P, g), []).append(obj["decision_id"])
            if P in self.pmerged:
                self._settle_merged()
            self._place_all()
            return
        self.mappings.setdefault((P, g), []).append(obj["decision_id"])
        occ = {"k": "rss-guid", "v": g, "guid": g}
        if not r["original"]:
            occ = {"k": "rss-guid+enclosure", "v": c.episode_v_guid_enclosure(g, r["enclosures"][0]), "guid": g,
                   "enclosure_url": r["enclosures"][0]}
        if r["publication_time"] is not None:
            occ["publication_time"] = r["publication_time"]
        self._register(r["episode_id"], P, occ, obj["decision_id"])
        if P in self.pmerged:
            self._settle_merged()  # a release of a merged-away podcast follows its merge
        self._place_all()

    def _target(self, ct, key):
        """Where a contribution lives now (PROTOCOL §9.7): a fact's contribution
        through an EpisodeRef with occurrence evidence lives where that reference routes
        (podcast merges by its original podcast and generation, merge redirects, then the
        resolver for a disputed GUID or an undisputed lookup alias); a decision's
        contribution, an admission's filing, and a fact's without a reference live at the
        root of the episode they name (in the survivor's live class for a joined
        generation). A pure function of the identity state, so the order in which
        identity decisions and facts arrive does not matter (CHOICE)."""
        meta = key[0] == "meta"
        decision = any(k in ct.data for k in ("adm", "dec", "decision"))
        # A catalog observation follows its occurrence, a device's or a fetch's.
        if ct.ref is not None and (not decision or ct.kind == "obs"):
            nE, nP, nG, kfam = ct.ref
            routed, _ = self._route(nP, None if meta else nG, nE, ct.occ or (None, None, None), kfam)
            if routed is None:
                return key
            return ("meta", "episode", routed[0]) if meta else ("episode", routed[0], routed[2])
        if meta:
            return ("meta", "episode", self._root(ct.orig or key[2]))
        E, G = self._route_scope(ct.orig or key[1], ct.ngen or key[2])
        return ("episode", E, G)

    def _place_all(self):
        """Recomputes every episode and catalog contribution's place after an identity change
        (a mapping, a GUID change, an episode or podcast merge), moving those whose place
        changed; account-level cuts then follow the entries they cover and sessions follow
        their starts. Replaces moving by partition or by alias: nothing is merged by an
        alias, so a GUID's dispute and its aliases can arrive in either order."""
        moves = []
        for key in sorted((k for k in self.scopes if (k[0] == "episode" or k[:2] == ("meta", "episode"))
                           and self.scopes[k]), key=lambda k: tuple(_bkey(x) for x in k)):
            keep = []
            for ct in self.scopes[key]:
                tgt = self._target(ct, key)
                if tgt != key:
                    moves.append((key, tgt, ct))
                else:
                    keep.append(ct)
            self.scopes[key] = keep
        if not moves:
            return
        touched = set()
        for key, tgt, ct in moves:
            if tgt[0] == "episode":
                ct.gen = tgt[2]
            self._add(tgt, ct)
            touched.add(key[1] if key[0] == "episode" else key[2])
            touched.add(tgt[1] if tgt[0] == "episode" else tgt[2])
        self._refan_cuts(touched)
        self._reindex_sessions()
        for E in sorted(touched):
            if E in self.episodes:
                self._refresh_episode(E)

    def _refan_cuts(self, episodes):
        gens = set()
        for E in episodes:
            gens |= self.episode_gens.get(E, set())
        for G in gens:
            keys = [("episode", E, G) for E in sorted(episodes, key=_bkey) if ("episode", E, G) in self.scopes]
            cuts = {}
            for k in keys:
                rest = []
                for ct in self.scopes[k]:
                    if ct.kind in ("history_clear", "statistics_reset"):
                        u = cuts.setdefault(ct.node, (ct.kind, {} if ct.kind == "statistics_reset" else set()))
                        if ct.kind == "history_clear":
                            u[1].update(ct.data["covered"])
                        else:
                            for sid, ops in ct.data["covered"].items():
                                u[1].setdefault(sid, set()).update(ops)
                    else:
                        rest.append(ct)
                self.scopes[k] = rest
            for node, (kind, cov) in sorted(cuts.items(), key=lambda x: self.rank[x[0]]):
                for k in keys:
                    cts = self.scopes[k]
                    if kind == "history_clear":
                        mine = frozenset(x for x in cov if any(ct.node == x for ct in cts))
                        data = {"covered": mine}
                    else:
                        sids = set(ct.data["session_id"] for ct in cts if ct.kind in _SESSION_KINDS)
                        mine = {sid: frozenset(ops) for sid, ops in cov.items() if sid in sids}
                        data = {"covered": mine}
                    if mine:
                        self._add(k, _Contribution(node, kind, G, data, orig=k[1]))

    def _reindex_sessions(self):
        """PROTOCOL §9.7: a session continues where its start's contribution now lives."""
        for key, cts in self.scopes.items():
            if key[0] != "episode":
                continue
            for ct in cts:
                if ct.kind == "start":
                    s = self.sessions.get(ct.data["session_id"])
                    if s is not None and s["start"] == ct.node:
                        s["episode_id"], s["generation_id"] = key[1], key[2]

    # -- PROTOCOL §9.7: merges --------------------------------------------------------

    def _move_scope(self, src, dst, gen):
        cts = self.scopes.pop(src, None)
        for ct in cts or ():
            if gen is not None:
                ct.gen = gen
            self._add(dst, ct)

    def _merge_episode(self, E1, E2, gen_of=None, P_dst=None, node=None):
        """PROTOCOL §9.7: merges root(E1) into root(E2): one-step redirects; every scope's
        contributions (episode scopes, mapped through `gen_of` for a podcast merge, and the
        catalog scope) move to the survivor, which is folded from the union; admissions,
        the catalog item, the server's fetch history, evidence and lookup aliases move too,
        and the merged episode's GUIDs (its enclosures without a GUID) become lookup
        aliases of the survivor (CHOICE) in the survivor's podcast. Returns the episodes
        touched."""
        src, dst = self._root(E1), self._root(E2)
        if src == dst:
            return set()
        P_dst = P_dst or (self.episodes[dst]["podcast_id"] if dst in self.episodes else None)
        for x, r in list(self.root.items()):
            if r == src:
                self.root[x] = dst
        self.root[src] = dst
        gof = gen_of or (lambda G: G)
        for G in sorted(self.episode_gens.pop(src, set())):
            self._move_scope(("episode", src, G), ("episode", dst, gof(G)), gof(G))
        self._move_scope(("meta", "episode", src), ("meta", "episode", dst), None)
        for (G, E), lst in list(self.admitted.items()):
            if E == src:
                self.admitted.setdefault((gof(G), dst), []).extend(lst)
                del self.admitted[(G, E)]
        ci = self.catalog_items.pop(src, None)
        if ci is not None:
            d = self.catalog_items.get(dst)
            if d is None:
                self.catalog_items[dst] = dict(ci, podcast_id=P_dst or ci["podcast_id"])
            elif d["playable"] is None:
                d["playable"] = ci["playable"]
        for k in [k for k in self.obs_media if k[1] == src]:
            v = self.obs_media.pop(k)
            self.obs_media.setdefault((k[0], dst), v)
        for seen in self.server_seen.values():
            if src in seen:
                v = seen.pop(src)
                seen[dst] = {"playable": v["playable"] or seen.get(dst, {}).get("playable", False)}
        keys = []
        occ = self.episodes.get(src, {}).get("occurrence")
        if occ is not None:
            keys.append(self._occ3(occ)[0])
        for ev in self.evidence.values():
            for k, e in ev.items():
                if e["episode"] == src:
                    keys.append(k)
                    e["episode"] = dst
        for lk in self.lookup.values():
            for a in lk.values():
                if a["episode"] == src:
                    a["episode"] = dst
        if P_dst is not None:
            self.merge_alias.setdefault(src, []).append((P_dst, node))
            for g, u, _ in keys:
                ak = ("guid", g) if g is not None else (("enclosure", u) if u is not None else None)
                if ak is not None:
                    self._put_alias(P_dst, ak, dst, node, src)
        return {src, dst}

    def _alias_prec(self, node, src):
        if node in self.decisions:
            k = (0,) + self.decisions[node]["rank"]
        elif node in self.facts:
            k = (1,) + self.facts[node]["rkey"]
        else:
            k = (2,)
        return k + (_bkey(src or ""),)

    def _put_alias(self, P, ak, E, node, src=None):
        """Adds a lookup alias. CHOICE (so that recovery order does not matter): two
        claims of one key keep the one made by the earlier-ranked decision (a fact's after
        any decision's, by receipt), then the smaller merged episode ID."""
        lk = self.lookup.setdefault(P, {})
        cur = lk.get(ak)
        if cur is not None and self._alias_prec(node, src) >= self._alias_prec(cur["node"], cur.get("src")):
            return False
        lk[ak] = {"episode": E, "node": node, "src": src}
        if cur is not None:
            self._alias_dirty = True
        return True

    def _registered(self, x):
        """An episode was registered: if a merge already merged it away, its GUID (its
        enclosure without a GUID) becomes the lookup alias the merge would have added had
        the episode been known then (CHOICE, so that the aliases do not depend on whether
        recovery admitted the merge before the facts naming the episode)."""
        for P_dst, node in self.merge_alias.get(x, ()):
            g, u, _ = self._occ3(self.episodes[x]["occurrence"])[0]
            ak = ("guid", g) if g is not None else (("enclosure", u) if u is not None else None)
            if ak is not None and self._put_alias(self._podcast_root(P_dst), ak, self._root(x), node, x):
                self._alias_dirty = True

    def _admit_guid_change(self, obj):
        """A GUID-change match (PROTOCOL §9.7): the new GUID becomes a lookup alias of the old
        root. An alias routes (a fact naming the new GUID's plain
        derivation with that GUID's occurrence goes to the old root while the GUID is
        undisputed), it merges nothing; the registered new episode is merged by its own
        `episode.merge`."""
        inp, r = obj["inputs"], obj["result"]
        P, g = inp["podcast_id"], r["guid"]
        top = self._podcast_root(P)  # a merge admitted first moved the podcast's aliases on
        self._put_alias(top, ("guid", g), r["episode_id"], obj["decision_id"])
        self._settle_merged()
        self._place_all()

    def _admit_episode_merge(self, obj):
        r = obj["result"]
        self._merge_episode(r["from"], r["into"], P_dst=obj["inputs"]["podcast_id"], node=obj["decision_id"])
        self._place_all()
        self._reindex_sessions()

    def _admit_podcast_merge(self, obj):
        """PROTOCOL §9.7: the source's aliases move to the survivor; its live generation is
        joined into the survivor's live class, or rehomed as the survivor's live generation
        (with its state, policy and credentials) when the survivor's latest generation is
        removed; closed source generations stay closed; every listed episode mapping merges
        the source episode into its survivor episode (registered by this decision if new),
        and (CHOICE) any other source episode present goes where the survivor's rules
        derive it. The source's evidence and lookup aliases join the survivor's."""
        did, r = obj["decision_id"], obj["result"]
        src, dst = r["source"], r["into"]
        pm = {"into": dst, "decision": did, "gens": {}, "episodes": {}}
        for gm in r["generations"]:
            G = gm["generation_id"]
            if gm["mode"] == "joined":
                pm["gens"][G] = ("joined", gm["into"])
            elif gm["mode"] == "rehomed":
                pm["gens"][G] = ("rehomed", G)
                self.generations[G]["podcast_id"] = dst
                if G not in self.podcasts[dst]["generations"]:
                    self.podcasts[dst]["generations"].append(G)
                rest = []
                for ct in self.scopes.get(("podcast", src), ()):
                    if ct.gen == G:
                        self._add(("podcast", dst), ct)
                    else:
                        rest.append(ct)
                self.scopes[("podcast", src)] = rest
            else:
                pm["gens"][G] = ("closed", None)
        self.pmerged[src] = pm
        gen_of = self._gen_of(pm)
        # Scopes this source already held for a generation rehomed into it (recovery may
        # admit the merges of a chain in any order) follow too.
        # CHOICE: aliases, evidence and lookup aliases go to the end of the merge chain (the
        # survivor itself, unless recovery admits this merge after the survivor's own).
        top = self._podcast_root(dst)
        for a in self.aliases.values():
            if a["podcast_id"] == src:
                a["podcast_id"] = top
        for m in r["episodes"]:
            pm["episodes"][m["from"]] = m["to"]
        for m in r["episodes"]:
            occ = (self.episodes.get(m["from"]) or {}).get("occurrence") or {"k": "rss-guid", "v": ""}
            self._register(m["to"], dst, occ, did)
            self._merge_episode(m["from"], m["to"], gen_of, P_dst=dst, node=did)
        self._settle_merged()
        dst = top
        for k, a in self.lookup.get(src, {}).items():
            self._put_alias(dst, k, self._root(a["episode"]), a["node"], a.get("src"))
        ev = self.evidence.setdefault(dst, {})
        for k, e in self.evidence.get(src, {}).items():
            if k not in ev:
                ev[k] = {"episode": self._root(e["episode"]) if e["episode"] else None, "node": e["node"],
                         "kfam": e.get("kfam", "rss-guid")}
        for E, v in self.server_seen.pop(src, {}).items():
            seen = self.server_seen.setdefault(dst, {})
            seen[self._root(E)] = {"playable": v["playable"] or seen.get(self._root(E), {}).get("playable", False)}
        for E, ci in self.catalog_items.items():
            if ci["podcast_id"] == src:
                ci["podcast_id"] = dst
        self._place_all()
        self._reindex_sessions()

    def _gen_of(self, pm):
        """A merge's generation mapping, followed through later merges of the chain (so a
        generation rehomed into a survivor that recovery had already merged on is routed
        on, as the survivor's own merge would have)."""
        def gen_of(G):
            mode, G2 = pm["gens"].get(G, ("closed", None))
            return self._route_scope(None, G2 if mode == "joined" else G)[1]
        return gen_of

    def _settle_merged(self):
        """Every registered episode of a merged-away podcast that is not merged yet goes
        where its merge lists it, else where the survivor's rules derive its occurrence
        (CHOICE), registered there if new and merged into it, through every merge of a
        chain. So the result does not depend on the order in which recovery admits the
        merges and the facts that registered the episodes."""
        changed = True
        while changed:
            changed = False
            cands = []
            for E, ep in sorted(self.episodes.items()):
                pm = self.pmerged.get(ep["podcast_id"])
                if pm is not None and E not in self.root:
                    occ3, kfam = self._occ3(ep["occurrence"])
                    cands.append((E, pm, occ3, kfam, ep["occurrence"]))
            # An episode never registered whose scope lies under a merged-away podcast's
            # joined generation (a fact routed through part of a chain in recovery).
            for key in sorted(k for k in self.scopes if k[0] == "episode"):
                E, G = key[1], key[2]
                if E in self.episodes or E in self.root or not self.scopes[key] or G not in self.generations:
                    continue
                pm = self.pmerged.get(self.generations[G]["podcast_id"])
                if pm is None or pm["gens"].get(G, ("closed",))[0] != "joined":
                    continue
                src = next((ct for ct in self.scopes[key] if ct.occ is not None), None)
                occ3 = src.occ if src is not None else (None, None, None)
                kfam = src.ref[3] if src is not None and src.ref is not None else "rss-guid"
                occ = {"k": kfam, "v": occ3[0] or ""}
                cands.append((E, pm, occ3, kfam, occ))
            for E, pm, occ3, kfam, occ in cands:
                if E in self.root:
                    continue
                to = pm["episodes"].get(E) or self._derive(pm["into"], occ3, kfam, rooted=False)
                if to is None or to == E:
                    continue
                self._register(to, pm["into"], occ, pm["decision"])
                self._merge_episode(E, to, self._gen_of(pm), P_dst=pm["into"], node=pm["decision"])
                changed = True
        # Every episode scope whose episode or generation now routes elsewhere moves (a
        # generation rehomed into a survivor whose own merge, admitted first in recovery,
        # joined it on).
        for key in sorted(k for k in self.scopes if k[0] == "episode"):
            E2, G2 = self._route_scope(key[1], key[2])
            if (E2, G2) != (key[1], key[2]) and self.scopes.get(key):
                self._move_scope(key, ("episode", E2, G2), G2)
                self.episode_gens.get(key[1], set()).discard(key[2])
        for (G, E), lst in list(self.admitted.items()):
            E2, G2 = self._route_scope(E, G)
            if (E2, G2) != (E, G):
                self.admitted.setdefault((G2, E2), []).extend(lst)
                del self.admitted[(G, E)]

    def _identity_rules(self, P):
        """PROTOCOL §9.7: the podcast's `identity_rules` record, or None when empty: its
        disputed GUIDs with their releases, its disputed enclosures, and the lookup aliases
        whose episode differs from rules 2-4 (CHOICE: neither rss-guid nor atom-id of the
        GUID, or not enclosure of the URL; aliases of disputed GUIDs or enclosures are not
        used, so not published). CHOICE: sorted by GUID; releases by publication time
        (null last), then episode ID; enclosures in the decision's order; aliases by (kind,
        value)."""
        dg = []
        for (P2, g) in sorted(self.mappings, key=lambda x: (_bkey(x[0]), _bkey(x[1]))):
            if P2 != P:
                continue
            rels = self._releases(P, g)
            if rels:
                dg.append({"guid": g, "releases": sorted(
                    ({"episode_id": self._root(x["episode_id"]), "publication_time": x["publication_time"],
                      "enclosures": list(x["enclosures"])} for x in rels),
                    key=lambda x: (x["publication_time"] is None, x["publication_time"] or 0, _bkey(x["episode_id"])))})
        de = sorted(self._disputed_enclosures(P), key=_bkey)
        al = []
        for (kind, value), a in self.lookup.get(P, {}).items():
            E = self._root(a["episode"])
            if kind == "guid":
                if self._disputed(P, value) or E in (self._eid(P, "rss-guid", value), self._eid(P, "atom-id", value)):
                    continue
            elif value in de or E == self._eid(P, "enclosure", value):
                continue
            al.append({"kind": kind, "value": value, "episode_id": E})
        al.sort(key=lambda x: (_bkey(x["kind"]), _bkey(x["value"])))
        if not (dg or de or al):
            return None
        return {"disputed_guids": dg, "disputed_enclosures": de, "aliases": al}

    def _check_catalog_decision_refs(self, obj):
        """What a recovered catalog decision refers to must exist: its podcast (and generation),
        and for a device admission, its proposal among the dependencies, admitted, naming
        the same episode and generation, by the recorded source."""
        t, inp, res, deps = obj["decision_type"], obj["inputs"], obj["result"], obj["dependencies"]
        if t == PODCAST_MERGE:
            for P in (res["source"], res["into"]):
                if not self._known_podcast(P):
                    raise RuleInputError("a podcast merge names an unknown podcast")
            for g in res["generations"]:
                if g["generation_id"] not in self.generations or (g["into"] is not None and g["into"] not in self.generations):
                    raise RuleInputError("a podcast merge names a generation not opened yet")
            return
        if t in IDENTITY_DECISIONS:
            if not self._known_podcast(inp["podcast_id"]):
                raise RuleInputError("a %s decision names an unknown podcast" % t)
            return
        if t in (CATALOG_REFRESH, MOVED):
            P = inp["podcast_id"]
            if not self._known_podcast(P):
                raise RuleInputError("a %s decision names an unknown podcast" % t)
            # CHOICE: a move's generation need not be open yet in recovery (its opening fact,
            # a re-subscription, need not be in the move's causal past); its write applies
            # only while that generation is the podcast's live one.
            return
        if inp["found_by"] not in deps:
            raise RuleInputError("an admission depends on what found the episode")
        if res["proposal_id"] is not None:
            info = self.facts.get(res["proposal_id"])
            f = info["fact"] if info is not None and info["admitted"] else None
            if f is None or f["type"] != PROPOSE or f["author_id"] != res["source"]:
                raise RuleInputError("a device admission names an admitted proposal, by its source")
            # CHOICE: the episode is not compared with the proposal's: it is where the
            # proposal routed when admitted, which recovery may reach only later.

    # -- PROTOCOL §8: listening, history, statistics -------------------------------

    def _ref_reason(self, ref):
        """PROTOCOL §7.2 EpisodeRef acceptance checks, in order, without
        registering anything (_check_ref registers the binding when they pass)."""
        P, E, G = ref["podcast_id"], ref["episode_id"], ref["generation_id"]
        if not self._known_podcast(P):
            return R_UNKNOWN_PODCAST
        if G not in self.podcasts[P]["generations"] and not self._joined_into(P, G):
            return R_UNKNOWN_GENERATION
        b = self._binding(E)
        if b is not None and b["podcast_id"] != P and not self._carried(E, b, P):
            return R_MISMATCH
        if self._closed_for(G):
            return R_CLOSED
        return None

    def _carried(self, E, b, P):
        """A podcast merge carries
        every episode of the source to the survivor, including those it has no mapping for
        (an episode without GUID and enclosure, which the survivor's rules cannot derive).
        Such an episode keeps its ID and is the survivor's from then on: it is bound to a
        merged-away podcast, merged into no other episode, and a reference naming any
        podcast of its merge family is its own podcast's (CHOICE)."""
        return (b["podcast_id"] in self.pmerged and E not in self.root
                and self._podcast_root(b["podcast_id"]) == self._podcast_root(P))

    def _joined_into(self, P, G):
        """CHOICE: G is the generation a podcast merge into P joined a source
        generation into. Recovery may admit that merge before the one that rehomes G into
        P; a reference routed there is then P's, as it is once both are admitted."""
        return any(pm["into"] == P and ("joined", G) in pm["gens"].values() for pm in self.pmerged.values())

    def _closed_for(self, G):
        """Whether generation G is closed for the fact being accepted: closed by a fact
        with an earlier receipt (PROTOCOL §6.10, §6.11: acceptance follows receipt order, so a
        replay or recovery in another order decides as the original acceptance did); for
        anything else, closed at all."""
        cb = self.generations[G]["closed_by"]
        if cb is None:
            return False
        rk = self._ctx.get("rkey") if self._ctx is not None else None
        return rk is None or cb not in self.facts or self.facts[cb]["rkey"] < rk

    def _listening_rejection(self, fact, typ, parsed):
        """PROTOCOL §8 checks 1, 2 and 5, which reject (`invalid_payload`). They come after
        PROTOCOL §7.2 generation checks, which make a fact `not_applied` instead: a fact
        failing those is admitted and reported with their reason (_apply_listening).

        CHOICE: a fact rejected here registers no episode binding (a refused fact registers nothing, PROTOCOL §7.2)."""
        if "refs" not in parsed:
            return None  # history.clear, statistics.reset: account-level, nothing to check
        ref = parsed["refs"][0]
        if ref.get("route_reason") is not None or self._ref_reason(ref) is not None:
            return None
        E, G = ref["episode_id"], ref["generation_id"]
        if typ in SESSION_TYPES:
            s = self.sessions.get(parsed["session_id"])
            if typ != "session.start":
                # 1. Session existence (and, for a finalization, the named ordinal). PROTOCOL §6.10:
                # the start may be an ancestor's. CHOICE: the named ordinal may be that of
                # any admitted checkpoint or end of the session's family (any continuation);
                # which save it names on the finalizer's path is resolved in the fold.
                if s is None:
                    return R_INVALID_PAYLOAD
                if typ == "session.finalize" and parsed["ordinal"] not in self._family_ordinals(s):
                    return R_INVALID_PAYLOAD
            # 2. Same episode and generation as the start. CHOICE: a second start naming a
            # registered session goes through the same checks as the session's other facts.
            if s is not None and (s["episode_id"], s["generation_id"]) != (E, G):
                return R_INVALID_PAYLOAD
            return None
        if typ == "history.restore":
            # 5. The removal must be a history.remove of the same episode. CHOICE: "same
            # episode" means the same episode ID and generation (the cut lives in that scope),
            # and the removal must be admitted (a rejected removal recorded no cut).
            rf = self.facts.get(parsed["removal"])
            if rf is None or not rf["admitted"] or rf["fact"]["type"] != "history.remove":
                return R_INVALID_PAYLOAD
            rref = _episode_ref(rf["fact"]["payload"]["episode"], "removal")
            self._route_inplace(rref)  # the removal's episode as it routes now
            if (rref["episode_id"], rref["generation_id"]) != (E, G):
                return R_INVALID_PAYLOAD
        return None

    def _apply_listening(self, op, typ, parsed):
        if typ in ("history.clear", "statistics.reset"):
            return self._apply_account_cut(op, typ)
        fact = self.facts[op]["fact"]
        ref = parsed["refs"][0]
        E, G = ref["episode_id"], ref["generation_id"]
        r = self._check_ref(ref, op)
        bundle = parsed.get("bundle") if typ == "session.start" else None
        if bundle is not None:
            # PROTOCOL §7.6: a play bundle is a replay step at the start's place; a
            # start refused on a closed generation leaves its episode's ghost (PROTOCOL §7.6).
            self.qsteps[op] = {"form": "bundle", "parsed": parsed,
                               "status": ["ok" if r is None else ("closed" if r == R_CLOSED else "fail")]}
            self._mark_queue_node(op)
        if r is not None:
            if E in self.episodes:
                self._refresh_episode(E)
            return _outcome(NOT_APPLIED, r)
        key = ("episode", E, G)
        if typ in SESSION_TYPES:
            reason = self._session_admission(op, typ, parsed)
            if reason is not None:
                if bundle is not None:
                    self.qsteps[op]["status"] = ["fail"]  # a refused start contributes nothing
                self._refresh_episode(E)  # publishes a binding this fact may have made
                return _outcome(NOT_APPLIED, reason)
            data = dict(parsed)
            del data["refs"]
            data.pop("bundle", None)
            self._add(key, self._ctb(op, _SESSION_KIND[typ], ref, data))
            if bundle is not None:
                # The started episode's add (membership in, its key prediction) under the
                # start's node (PROTOCOL §7.6); its place is the replay's (PROTOCOL §7.6).
                self._add(key, self._ctb(op, "q_add", ref, {"key": bundle["key"], "pid": bundle["pid"]}))
            self._refresh_episode(E)
            if (typ == "session.start" and self._ctx is not None and self._ctx["live"] and bundle is None
                    and "queue" not in self._ctx["caps"]):
                # CHOICE: only a client without the queue capability gets a resolved add; a
                # queue client's start without a bundle saw the episode queued.
                mem = self.proj.get(("episode", E), {}).get("queue_membership")
                if mem is None or not mem["in_queue"]:
                    self._make_resolved_add(op, ref)
            if typ != "session.finalize":
                # PROTOCOL §8: start, checkpoint and end are applied whenever admitted.
                return _outcome(APPLIED)
            fold = self._fold_episode_scope(E, G)
            # PROTOCOL §6.10: a finalization that does not take effect (an ineligible copied
            # finalization, or one after the session's effective finalization: CHOICE)
            # is superseded.
            if fold.status.get(op) == "void":
                return _outcome(SUPERSEDED)
            # Superseded if its completion lost to a takeover
            # ordered before it (its history mark still counts); otherwise applied
            # (it recorded the device's decision).
            if parsed["completed"] and fold.status.get(op) != "fired":
                return _outcome(SUPERSEDED)
            return _outcome(APPLIED)
        if typ == "history.remove":
            # A cut suppressing exactly this episode's covered history entries: the entries
            # of this scope in the removal's causal past. Later-admitted facts are never
            # ancestors, so the covered set is final at acceptance.
            anc = self._ancestors(op)
            covered = frozenset(ct.node for ct in self.scopes.get(key, ()) if _is_history_entry(ct) and ct.node in anc)
            self._add(key, self._ctb(op, "history_remove", ref, {"covered": covered}))
            self._refresh_episode(E)
            return _outcome(APPLIED)
        # history.restore: cancels exactly the named removal's cut.
        # CHOICE: cancellation is not positional (a restore cancels the cut wherever it is
        # ordered); the receipt is already_restored iff a restore of the same removal was
        # accepted before.
        already = any(ct.kind == "history_restore" and ct.data["removal"] == parsed["removal"]
                      for ct in self.scopes.get(key, ()))
        self._add(key, self._ctb(op, "history_restore", ref, {"removal": parsed["removal"]}))
        self._refresh_episode(E)
        if already:
            return _outcome(NOT_APPLIED, R_ALREADY_RESTORED)
        return _outcome(APPLIED)

    def _session_admission(self, op, typ, parsed):
        """PROTOCOL §8 checks 3 and 4 (`not_applied`), per continuation (PROTOCOL §6.10); on success the
        fact joins the session registry.

        PROTOCOL §6.10: session facts belong to the continuation (session_id, effective author). The
        session's own author and every author descending from it in the lineage may append
        (a branch continuation) or finalize (a copied finalization); another author is
        foreign_session. Ordinals and sealing are checked within the continuation.
        CHOICE: the lineage alone decides; there is no check that a branch continuation's
        session was started before the divergence, nor that its ordinals lie above the
        ordinals it shares ("a branch continues the device's numbering above the shared
        ordinals" is a client obligation, not an admission check).

        CHOICE: a session exists only once a start passing every check was accepted. A
        second start naming a registered session (check 2 already passed) is
        session_ordinal_conflict from an author in the session's lineage, else
        foreign_session. Within check 4 an ordinal conflict is reported before sealing. A
        finalization does not seal: checkpoints and an end may follow it. CHOICE: the single-path
        "second finalization" rule (session_finalized) applies per continuation, that is
        to a second finalization by the same effective author, whether or not it is the
        session's own author.

        With a persisted receipt (a recovered fact's or a replay's), the checks are not
        re-run: a fact the receipt says they refused contributes nothing."""
        sid = parsed["session_id"]
        s = self.sessions.get(sid)
        fixed = self._ctx.get("fixed") if self._ctx is not None else None
        if fixed is not None:
            if fixed["outcome"] == NOT_APPLIED and fixed.get("reason") in SESSION_REASONS:
                return fixed["reason"]
            if typ == "session.start":
                if s is None:
                    ref = parsed["refs"][0]
                    self.sessions[sid] = {"episode_id": ref["episode_id"], "generation_id": ref["generation_id"],
                                          "start": op, "facts": []}
                return None
            if s is None:
                # The receipt says the session's checks passed, so its start was admitted
                # first; an order without it (its start refused here, or not yet joined
                # although the fact depends on it) is an archive whose causal order does
                # not hold its receipts.
                raise RuleInputError("%s's receipt admits it to session %s, whose start is not admitted in this "
                                     "order (an archive that does not hold its receipts)" % (op, sid))
            s["facts"].append(op)
            return None
        Z = self.eff.get(op)
        if typ == "session.start":
            if s is not None:
                return R_ORDINAL_CONFLICT if self.in_lineage(Z, self.eff.get(s["start"])) else R_FOREIGN_SESSION
            ref = parsed["refs"][0]
            self.sessions[sid] = {"episode_id": ref["episode_id"], "generation_id": ref["generation_id"],
                                  "start": op, "facts": []}
            return None
        reason = self._continuation_reason(Z, s, typ, parsed)
        if reason is None:
            s["facts"].append(op)
        return reason

    def _continuation_reason(self, Z, s, typ, parsed):
        """PROTOCOL §8 check 4 within the continuation of effective author Z (PROTOCOL §6.10), for a
        checkpoint, end or finalization of the registered session `s`; no side effect."""
        if Z is None or not self.in_lineage(Z, self.eff.get(s["start"])):
            return R_FOREIGN_SESSION
        cont = [x for x in s["facts"] if self.eff.get(x) == Z]
        if typ == "session.finalize":
            if any(self.facts[x]["fact"]["type"] == "session.finalize" for x in cont):
                return R_FINALIZED
            return None
        o = parsed["ordinal"]
        ords = [self.facts[x]["fact"]["payload"]["ordinal"] for x in cont
                if self.facts[x]["fact"]["type"] != "session.finalize"]
        if o in ords or o < max(ords, default=0):
            return R_ORDINAL_CONFLICT
        if any(self.facts[x]["fact"]["type"] == "session.end" for x in cont):
            return R_SEALED
        return None

    def _claim_rejection(self, op, fact, parsed, rkey):
        """PROTOCOL §8.1: a claiming checkpoint covers a new save, so its
        save must not be already covered on its writing path; such a claim is `rejected` /
        `invalid_payload` and never joins the claim set. CHOICE: checked after every existing
        check (PROTOCOL §7.2 generation checks, PROTOCOL §8 checks 1, 2 and 5, and the continuation, ordinal and sealing checks of check 4, PROTOCOL §6.10), so a fact failing
        one keeps that outcome; this needs the fact's effective author before it is placed,
        which a live fact appended in receipt order has (a recovered fact keeps its receipt:
        the fold's guard in _session_plan applies the same rule)."""
        ref = parsed["refs"][0]
        if ref.get("route_reason") is not None or self._ref_reason(ref) is not None:
            return None
        s = self.sessions.get(parsed["session_id"])
        if s is None or (self.max_rkey is not None and rkey < self.max_rkey):
            return None
        Z, parent = self._live_author(op, fact)
        fresh = parent is not None and Z not in self.derived_parent
        if fresh:
            self.derived_parent[Z] = parent  # the lineage placement will record (PROTOCOL §6.10)
        try:
            if self._continuation_reason(Z, s, "session.checkpoint", parsed) is not None:
                return None
        finally:
            if fresh:
                del self.derived_parent[Z]
        prev = fact.get("previous_op_id")
        chain = [prev] + list(self._chain_walk(prev, limit=s["start"])) if prev in self.facts else []
        if self._save_covered(parsed["save"], chain, set(s["facts"])):
            return R_INVALID_PAYLOAD
        return None

    def _live_author(self, op, fact):
        """(effective author, derived parent or None): what a fact appended in receipt order
        will get (PROTOCOL §6.10, _place_one), without placing it."""
        co = author_coordinates(fact)
        if co is None:
            return None, None
        a, sq, prev = co
        E = a
        if prev is not None and prev in self.eff and self.coords[prev][0] == a:
            E = self.eff[prev]
        return (derived_author(self.namespace, op), E) if (E, sq) in self.held else (E, None)

    def _save_covered(self, sv, chain, members):
        """Whether an export's save is already covered on its writing path: `chain` is the
        writing history before it (newest first), `members` the session's admitted facts.
        CHOICE: covered when an earlier export on the path (a checkpoint or end with save
        metadata) exported the same save ID, or a save with the same or a higher index (the
        session's save counter only grows along one path, PROTOCOL §6.10), or the save this one
        names as its `first`."""
        for x in chain:
            if x not in members:
                continue
            f = self.facts[x]["fact"]
            xs = f["payload"].get("save") if f["type"] in ("session.checkpoint", "session.end") else None
            if not isinstance(xs, dict):
                continue
            if xs.get("id") == sv["id"] or xs.get("index", 0) >= sv["index"] or xs.get("id") == sv.get("first"):
                return True
        return False

    def _family_ordinals(self, s):
        return set(self.facts[x]["fact"]["payload"]["ordinal"] for x in s["facts"]
                   if self.facts[x]["fact"]["type"] in ("session.checkpoint", "session.end"))

    def _apply_account_cut(self, op, typ):
        """history.clear and statistics.reset (PROTOCOL §8): computed from the cut's causal past
        and contributed to every episode scope holding a covered entry or session (the
        fan-out at acceptance is complete because later facts are never ancestors).

        A statistics cut per session (PROTOCOL §6.10: per family) is the set of covered exports; the
        fold closes it over the save tree (every node before a covered one on every path),
        which for a single path is the greatest covered ordinal. CHOICE: only
        contributing checkpoints and ends are covered exports; a covered start or
        finalization alone covers nothing (no residual change); facts that were not
        applied are traversed but never count as covered session facts."""
        anc = self._ancestors(op)
        touched = []
        for key, cts in list(self.scopes.items()):
            if key[0] != "episode":
                continue
            if typ == "history.clear":
                covered = frozenset(ct.node for ct in cts if _is_history_entry(ct) and ct.node in anc)
                if not covered:
                    continue
                data = {"covered": covered}
                kind = "history_clear"
            else:
                cuts = {}
                for ct in cts:
                    if ct.kind in _SESSION_KINDS and ct.node in anc:
                        got = cuts.setdefault(ct.data["session_id"], set())
                        if ct.kind in ("checkpoint", "end"):
                            got.add(ct.node)
                if not cuts:
                    continue
                data = {"covered": {sid: frozenset(x) for sid, x in cuts.items()}}
                kind = "statistics_reset"
            self._add(key, _Contribution(op, kind, key[2], data))
            touched.append(key[1])
        for E in sorted(set(touched)):
            if E in self.episodes:
                self._refresh_episode(E)
        # PROTOCOL §8: a cut is applied even when another cut already covers it.
        return _outcome(APPLIED)

    def _add(self, key, contribution):
        if contribution.orig is None:
            if key[0] == "episode":
                contribution.orig = key[1]
            elif key[:2] == ("meta", "episode"):
                contribution.orig = key[2]
        self.scopes.setdefault(key, []).append(contribution)
        if key[0] == "episode":
            self.episode_gens.setdefault(key[1], set()).add(key[2])
            k, d = contribution.kind, contribution.data
            if k in _QUEUE_KINDS or (k in ("end", "finalize", "leave") and (d["completed"] or d["remove_from_queue"])):
                # CHOICE (PROTOCOL §8): a leave carrying a completion or a removal is a
                # queue membership node, as an end or a finalization carrying one is.
                self._mark_queue_node(contribution.node)

    def _resolve(self, op, parsed):
        if self.fixed_resolutions is not None:
            r = self.fixed_resolutions.get(op)
            if r is None:
                if self.rebuild_mode:
                    return None  # a recovered registration waits for its resolution
                raise RuleInputError("no persisted resolution for %s" % op)
            return dict(r)
        norm = c.normalize_url(parsed["feed_url"])
        alias = self.aliases.get(norm)
        obs = self._route_observed(parsed.get("observed"))
        if alias is None:
            P = c.podcast_id(self.namespace, op)
            return {"podcast_id": P, "generation_id": c.generation_id(self.namespace, P, op), "mode": "created"}
        P = alias["podcast_id"]
        names = lambda g: obs is not None and obs["podcast_id"] == P and obs["generation_id"] == g  # noqa: E731
        live = self._live_generation(P)
        if live is not None:
            pred = self.generations[live]["predecessor"]
            if obs is None or names(live) or (pred is not None and names(pred) and obs["state"] == "removed"):
                return {"podcast_id": P, "generation_id": live, "mode": "joined"}
            # CHOICE: a stale resolution names the podcast's live generation.
            return {"podcast_id": P, "generation_id": live, "mode": "stale"}
        latest = self._latest_generation(P)
        if names(latest) and obs["state"] == "removed":
            return {"podcast_id": P, "generation_id": c.generation_id(self.namespace, P, op), "mode": "opened"}
        # CHOICE: a stale resolution with no live generation names the latest closed one.
        return {"podcast_id": P, "generation_id": latest, "mode": "stale"}

    def _route_observed(self, obs):
        """CHOICE: a subscribe's `observed` naming a merged-away podcast is read as the
        survivor's, by the same generation routing (a joined generation is the survivor's
        live class; a closed one stays a closed generation, of the source)."""
        if obs is None:
            return None
        P, G = obs["podcast_id"], obs["generation_id"]
        seen = set()
        while P in self.pmerged and P not in seen:
            seen.add(P)
            mode, G2 = self.pmerged[P]["gens"].get(G, ("closed", None))
            if mode == "closed":
                return obs
            P, G = self.pmerged[P]["into"], G2
        return dict(obs, podcast_id=P, generation_id=G)

    def _claim_alias(self, norm, P, url, op):
        if norm not in self.aliases:
            self.aliases[norm] = {"podcast_id": P, "url": url, "op_id": op}

    def _apply_subscribe(self, op, parsed, preview):
        res = self._resolve(op, parsed)
        if res is None:
            # PROTOCOL §6.11: recovered facts make no decision; the registration's effect waits for
            # its podcast.resolution (uploaded after it, or made at the request's end).
            self.parked.add(op)
            fx = self._ctx.get("fixed") if self._ctx is not None else None
            return dict(fx) if fx is not None else _outcome(APPLIED)
        self.resolutions[op] = dict(res)
        if self.archiving and self._ctx is not None and self._ctx["live"]:
            # The decisions a live registration makes (recoverable; implicit in vectors).
            rk = self._ctx["rkey"]
            i = rk[2] + 1
            if res["mode"] == "created":
                self._implicit(ALIAS, "alias:" + c.normalize_url(parsed["feed_url"]), {"podcast_id": res["podcast_id"]},
                               (rk[0], rk[1], i), op)
                i += 1
            self._implicit(RESOLUTION, op, res, (rk[0], rk[1], i), op)
        mode, P, G = res["mode"], res["podcast_id"], res.get("generation_id")
        if mode == "stale":
            return _outcome(NOT_APPLIED, R_STALE)
        pod = self.podcasts.setdefault(P, {"creating_op": None, "generations": []})
        secret = parsed.get("credentials_secret")
        if mode in ("created", "opened"):
            if mode == "created":
                pod["creating_op"] = op
                self._claim_alias(c.normalize_url(parsed["feed_url"]), P, parsed["feed_url"], op)
                predecessor = None
            else:
                predecessor = parsed["observed"]["generation_id"]
            if "policy" in parsed:
                policy = dict(parsed["policy"])
            else:
                policy = dict(PREVIEW_POLICY if preview else DEFAULT_POLICY)
            self.generations[G] = {
                "podcast_id": P,
                "op_id": op,
                "kind": "preview" if preview else "subscribe",
                "predecessor": predecessor,
                "policy": policy,
                "url": parsed["feed_url"],  # the generation's base refresh URL
                "closed_by": None,
            }
            pod["generations"].append(G)
            self._add(("podcast", P), _Contribution(op, "open", G, {"secret": secret}))
            self._refresh_podcast(P, all_episodes=True)
            return _outcome(APPLIED)
        if mode != "joined":
            raise RuleInputError("unknown resolution mode %r" % mode)
        # CHOICE: a joined subscribe's payload `policy` is ignored (only a created or opened
        # generation "gets the given policy").
        # The generation's current podcast (a merge admitted first in recovery may have
        # rehomed it).
        P = self.generations[G]["podcast_id"] if G in self.generations else P
        self._add(("podcast", P), _Contribution(op, "join_preview" if preview else "join_subscribe", G, {"secret": secret}))
        fold = self._refresh_podcast(P)
        if preview:
            return _outcome(APPLIED)
        return self._outcome_from(op, [fold])

    def _known_podcast(self, P):
        pod = self.podcasts.get(P)
        return pod is not None and pod["creating_op"] is not None

    def _apply_podcast_op(self, op, typ, parsed):
        P, G = parsed["podcast_id"], parsed["generation_id"]
        G = self.gen_join.get(G, G)  # a provisional generation joined into the effective one
        fixed = self._ctx.get("fixed") if self._ctx is not None else None
        if (fixed is not None and fixed["outcome"] == NOT_APPLIED
                and fixed.get("reason") in (R_GENERATION_MERGED, R_ALIAS_CONFLICT)):
            # CHOICE, as the session refusals: with a persisted generation_merged or
            # alias_conflict receipt the fact contributes nothing, even where recovery admits
            # it on the other side of a podcast merge (which moves generations and aliases;
            # receipts never change).
            return _outcome(NOT_APPLIED, fixed["reason"])
        if typ == "podcast.change_url" and self.fixed_aliases is not None:
            # Replaying persisted decisions: the alias decision this fact made stands
            # whatever this arrival order would decide now (PROTOCOL §9.5).
            d = self.fixed_aliases.get(c.normalize_url(parsed["url"]))
            if d is not None and d["op_id"] == op:
                # For the end of the merge chain (a merge may be admitted first).
                owner = self._podcast_root(d["podcast_id"]) if d["podcast_id"] in self.podcasts else d["podcast_id"]
                self._claim_alias(c.normalize_url(parsed["url"]), owner, parsed["url"], op)
                if self._known_podcast(owner):
                    self._refresh_podcast(owner)
        if not self._known_podcast(P):
            return _outcome(NOT_APPLIED, R_UNKNOWN_PODCAST)
        if G not in self.podcasts[P]["generations"]:
            return _outcome(NOT_APPLIED, R_UNKNOWN_GENERATION)
        # PROTOCOL §9.7: a lifecycle fact naming a merged-away podcast is routed by its generation:
        # a closed source generation is closed; a joined one answers generation_merged;
        # a rehomed one is the survivor's live generation.
        seen = set()
        while P in self.pmerged and P not in seen:
            seen.add(P)
            mode, _ = self.pmerged[P]["gens"].get(G, ("closed", None))
            if mode == "closed":
                return _outcome(NOT_APPLIED, R_CLOSED)
            if mode == "joined":
                return _outcome(NOT_APPLIED, R_GENERATION_MERGED)
            P = self.pmerged[P]["into"]
        gen = self.generations[G]
        closed = self._closed_for(G)
        if fixed is not None and fixed["outcome"] == NOT_APPLIED and fixed.get("reason") == R_CLOSED:
            closed = True  # PROTOCOL §6.11 (CHOICE): as for episode references
        if closed:
            return _outcome(NOT_APPLIED, R_CLOSED)
        if typ == "podcast.unsubscribe":
            # The earliest closing receipt closes (several admitted ones exist only when
            # recovery reinterprets a provisional acceptance, PROTOCOL §6.11).
            cb = gen["closed_by"]
            if cb is None or self.facts[op]["rkey"] < self.facts[cb]["rkey"]:
                gen["closed_by"] = op
            self._add(("podcast", P), _Contribution(op, "unsubscribe", G))
            self._refresh_podcast(P, all_episodes=True)
            return _outcome(APPLIED)
        if typ == "podcast.change_url":
            url = parsed["url"]
            norm = c.normalize_url(url)
            claimed = False
            if self.fixed_aliases is not None:
                d = self.fixed_aliases.get(norm)
                admitted = fixed is not None and fixed["outcome"] in (APPLIED, SUPERSEDED)
                if d is None or (d["op_id"] != op and self._podcast_root(d["podcast_id"]) != self._podcast_root(P)
                                 and not admitted):
                    # No decision: the original acceptance never reached the claim (its
                    # generation was closed), so the generation ends closed and this fact's
                    # contribution would be hidden anyway. (this fact's own claim, or one
                    # of the podcast a merge admitted later joins, is no conflict.)
                    return _outcome(NOT_APPLIED, R_ALIAS_CONFLICT)
                claimed = d["op_id"] == op  # claimed above
            else:
                a = self.aliases.get(norm)
                if a is not None and a["podcast_id"] != P and not (
                        fixed is not None and fixed["outcome"] in (APPLIED, SUPERSEDED)):
                    # (CHOICE: a persisted receipt that admitted the change decides, as for
                    # the generation checks: recovery may admit the podcast merge that made
                    # the URL this podcast's only after the fact.)
                    return _outcome(NOT_APPLIED, R_ALIAS_CONFLICT)
                if a is None:
                    self._claim_alias(norm, P, url, op)
                    claimed = True
            self._add(("podcast", P), _Contribution(op, "change_url", G, {"url": url}))
            fold = self._refresh_podcast(P)
            if claimed:
                return _outcome(APPLIED)  # the alias claim is a permanent effect
            return self._outcome_from(op, [fold])
        kind = {
            "podcast.archive": "archive",
            "podcast.restore": "restore",
            "podcast.set_policy": "set_policy",
            "podcast.set_credentials": "set_credentials",
        }[typ]
        self._add(("podcast", P), _Contribution(op, kind, G, parsed))
        fold = self._refresh_podcast(P)
        return self._outcome_from(op, [fold])

    def _binding(self, E):
        if self.fixed_episodes is not None:
            return self.fixed_episodes.get(E)
        return self.episodes.get(E)

    def _check_ref(self, ref, node):
        """Acceptance rules for one EpisodeRef, in the order of PROTOCOL §7.2.
        The first fact naming an episode that passes them all registers it (its podcast binding and occurrence evidence); a refused fact registers nothing.
        When replaying persisted allocations the binding is the persisted one and is
        registered when its deciding fact arrives, whatever this order would decide.

        The reference is routed first (_route_inplace: podcast merges, merge redirects,
        the release resolver) and rewritten in place; the checks run on what it names, the
        generation closure on where it goes. CHOICE: a passing fact registers both the
        episode it names (under the podcast it names; a redirect when rerouted) and the
        episode it is applied to, if new, so registration does not depend on whether a
        merge or mapping was admitted before it."""
        self._route_inplace(ref)
        nE, nP, _ = ref["named"]
        P, E = ref["podcast_id"], ref["episode_id"]
        if self.fixed_episodes is not None:
            for x in dict.fromkeys((nE, E)):
                d = self.fixed_episodes.get(x)
                if d is not None and d["op_id"] == node and x not in self.episodes:
                    self.episodes[x] = {"podcast_id": d["podcast_id"], "occurrence": copy.deepcopy(d["occurrence"]),
                                        "op_id": node}
                    self._registered(x)
        reason = ref["route_reason"] or self._ref_reason(ref)
        fx = self._ctx.get("fixed") if self._ctx is not None else None
        if fx is not None and fx["outcome"] == NOT_APPLIED and fx.get("reason") == R_CLOSED:
            # PROTOCOL §6.11 (CHOICE): a fact whose persisted receipt refused it as
            # generation_closed contributes nothing, whatever this arrival order says.
            reason = reason or R_CLOSED
        if reason is not None:
            return reason
        if ref["route_reason"] is None and nP in self.pmerged:
            # CHOICE: a source episode the podcast merge did not list, routed to where the
            # survivor's rules put it, is merged there (a redirect), as the merge would have
            # done had the episode existed when it was admitted. Only for a fact that passes
            # (a refused fact registers nothing, so that what registers does not depend on
            # whether the merge was admitted before the fact).
            self._walk_merges(nE, nP, ref["occurrence"], ref["occ3"], ref["kfam"], node)
        if self.fixed_episodes is None:
            for x, xp in ((nE, nP), (E, P)):
                if x not in self.episodes:
                    self.episodes[x] = {"podcast_id": xp, "occurrence": copy.deepcopy(ref["occurrence"]), "op_id": node}
                    self._registered(x)
        if nE != E and nE in self.episodes:
            self._refresh_episode(nE)
        return None

    def _apply_leave(self, op, parsed):
        """PROTOCOL §8, `episode.leave`: PROTOCOL §7.2 episode and generation checks
        (no session checks), then one contribution to the episode scope, folded at its
        place (see _fold_episode_scope). Outcome: `applied`, or `superseded` when its
        reported completion (or, without one, its reported removal) did not apply because
        the claim changed before it or an earlier leave of the same transition was
        effective, like a finalization. CHOICE: a leave reporting a completion whose bundle
        did not fire is superseded even if its removal applied instead."""
        ref = parsed["refs"][0]
        r = self._check_ref(ref, op)
        E, G = ref["episode_id"], ref["generation_id"]
        if r is not None:
            if E in self.episodes:
                self._refresh_episode(E)
            return _outcome(NOT_APPLIED, r)
        data = dict(parsed)
        del data["refs"]
        self._add(("episode", E, G), self._ctb(op, "leave", ref, data))
        self._refresh_episode(E)
        st = self._fold_episode_scope(E, G).status.get(op)
        if parsed["completed"]:
            return _outcome(APPLIED if st == "fired" else SUPERSEDED)
        if parsed["remove_from_queue"]:
            return _outcome(APPLIED if st == "removed" else SUPERSEDED)
        return _outcome(APPLIED)

    def _apply_episode_op(self, op, typ, parsed):
        refs = parsed["refs"]
        effective = []
        first_reason = None
        for ref in refs:
            r = self._check_ref(ref, op)
            if r is None:
                effective.append(ref)
            elif first_reason is None:
                first_reason = r
        kind = {
            "episode.mark_played": "mark_played",
            "episode.mark_unplayed": "mark_unplayed",
            "episode.reset_position": "reset_position",
            "episode.restore_state": "restore_state",
            "inbox.dismiss": "dismiss",
            "inbox.dismiss_all": "dismiss",
            "favorite.set": "favorite",
        }[typ]
        for ref in effective:
            self._add(("episode", ref["episode_id"], ref["generation_id"]), self._ctb(op, kind, ref, parsed))
        for ref in refs:
            if ref["episode_id"] in self.episodes:
                self._refresh_episode(ref["episode_id"])
        if not effective:
            # CHOICE: a bulk fact is not_applied only if no member passes; its reason is the
            # first failing member's (in payload order).
            return _outcome(NOT_APPLIED, first_reason)
        if kind == "dismiss":
            # CHOICE: a dismissal is a barrier even when it changes nothing visible (PROTOCOL §7.3),
            # so it is applied whenever at least one member is effective.
            return _outcome(APPLIED)
        folds = [self._fold_episode_scope(r["episode_id"], r["generation_id"]) for r in effective]
        return self._outcome_from(op, folds)

    def _outcome_from(self, op, folds):
        """Outcome at acceptance from the scope folds that include the new fact:
        applied if it determines a value or a conditional effect fired; superseded if it
        wrote but a fact ordered after it determines every value; not_applied with the
        rule's reason if its effect was ineffective at its place in order."""
        for f in folds:
            if op in f.det.values():
                return _outcome(APPLIED)
        statuses = [f.status[op] for f in folds if op in f.status]
        if "fired" in statuses:
            return _outcome(APPLIED)
        if "wrote" in statuses:
            return _outcome(SUPERSEDED)
        for st in statuses:
            if st.startswith("ineffective:"):
                return _outcome(NOT_APPLIED, st.split(":", 1)[1])
        return _outcome(APPLIED)

    # -- generations -------------------------------------------------------------

    def _latest_of(self, gens):
        """The generation no other listed generation names as predecessor (the end of the
        lifecycle chain); with several (only transiently, while replaying persisted
        decisions out of order) the last opened."""
        if not gens:
            return None
        preds = set(self.generations[g]["predecessor"] for g in gens)
        cands = [g for g in gens if g not in preds]
        return cands[-1] if cands else gens[-1]

    def _latest_generation(self, P):
        return self._latest_of(self.podcasts[P]["generations"])

    def _live_generation(self, P):
        pod = self.podcasts.get(P)
        if pod is None or P in self.pmerged:
            return None
        return self._latest_of([g for g in pod["generations"] if self.generations[g]["closed_by"] is None])

    # -- folds -------------------------------------------------------------------

    def _sorted(self, key):
        # PROTOCOL §7.6: a resolved add is folded at its start's place, right after the start's
        # own effects (CHOICE: the same rank, after every other contribution of that node).
        # PROTOCOL §9.7: several effects of one node in one scope (a compound, after a merge or
        # mapping) apply membership effects before position-only effects, then by original
        # episode ID; otherwise in the order they were made.
        return sorted(self.scopes.get(key, ()), key=lambda ct: (self.rank[ct.node], ct.kind == "q_resolved",
                                                                ct.kind in _POSITION_ONLY, _bkey(ct.orig or "")))

    def _live_contribution(self, ct):
        """False for a contribution of a catalog decision that is not the effective one of its
        subject (a competitor admitted in recovery)."""
        d = ct.data
        if "dec" in d:
            return self._effective(d["dec"])
        if "adm" in d:
            return self._effective(d["adm"])
        return True

    def _fold_meta(self, key):
        """PROTOCOL §9.4 replacement rule, folded per record in rank order over its observations:
        an observation replaces the current record if there is none, if it names the
        current revision as its base, or if it is concurrent (another base) and its fetch
        time is later. Returns (value, revision node) or (None, None). CHOICE: equal fetch
        times do not replace ("later" is strict)."""
        cur = None
        for ct in self._sorted(key):
            if not self._live_contribution(ct):
                continue
            d = ct.data
            if cur is None or d["base"] == cur.node or d["fetch_time"] > cur.data["fetch_time"]:
                cur = ct
        if cur is None:
            return None, None
        v = copy.deepcopy(cur.data["fields"])
        v["revision"] = cur.node
        v["fetch_time"] = cur.data["fetch_time"]
        return v, cur.node

    def _fold_status(self, P):
        """The podcast's `refresh_status`: the status of the last effective part 0 carrying
        one, in rank order. CHOICE: a podcast-level record kept whatever the subscription
        state (like catalog metadata)."""
        v = None
        for ct in self._sorted(("status", P)):
            if self._live_contribution(ct):
                v = ct.data["value"]
        return copy.deepcopy(v)

    def _fold_podcast(self, P):
        pod = self.podcasts.get(P)
        if pod is None or pod["creating_op"] is None:
            return None
        f = _Fold()
        if P in self.pmerged:
            # PROTOCOL §9.7: a merged-away podcast keeps only its subscription record (CHOICE).
            f.records["subscription"] = {"state": "merged", "merged_into": self._podcast_root(P)}
            return f
        contribs = self._sorted(("podcast", P))
        rules_rec = self._identity_rules(P)
        if rules_rec is not None:
            f.records["identity_rules"] = rules_rec
        # PROTOCOL §9.4: catalog metadata is not per generation and stays after an unsubscribe.
        meta = self._fold_meta(("meta", "podcast", P))[0]
        if meta is not None:
            f.records["metadata"] = meta
        status = self._fold_status(P)
        if status is not None:
            f.records["refresh_status"] = status
        aliases = sorted(
            ({"url": a["url"], "normalized": n} for n, a in self.aliases.items() if a["podcast_id"] == P),
            key=lambda a: (_bkey(a["normalized"]), _bkey(a["url"])),
        )
        # Aliases are permanent `podcast.alias` decisions with the raw spelling of their
        # first claim. The refresh URL belongs to a generation: the opening
        # fact's feed_url, replaced by that generation's change_url writes in rank order;
        # a closed generation's writes are hidden. The record shows the live generation's,
        # else the latest generation's (whose writes are hidden: its opening URL).
        live = self._live_generation(P)
        shown = live if live is not None else self._latest_generation(P)
        refresh, det_refresh = self.generations[shown]["url"], self.generations[shown]["op_id"]
        if live is not None:
            for ct in contribs:
                # PROTOCOL §9.4: a move's refresh URL is a write at its place like change_url.
                if ct.kind in ("change_url", "moved") and ct.gen == live and self._live_contribution(ct):
                    refresh, det_refresh = ct.data["url"], ct.node
                    f.status[ct.node] = "wrote"
        f.records["identity"] = {"aliases": aliases, "refresh_url": refresh}
        f.det["identity.refresh_url"] = det_refresh
        if live is None:
            f.records["subscription"] = {"state": "removed", "generation_id": self._latest_generation(P)}
            return f
        g = self.generations[live]
        state = "subscribed" if g["kind"] == "subscribe" else "not_subscribed"
        det_sub = g["op_id"]
        policy = dict(g["policy"])
        det_action = det_keep = g["op_id"]
        secret, det_cred = None, None
        for ct in contribs:
            if ct.gen != live:
                continue
            k = ct.kind
            if k == "open":
                if ct.data.get("secret") is not None:
                    secret, det_cred = ct.data["secret"], ct.node
                    f.status[ct.node] = "wrote"
                else:
                    f.status[ct.node] = "noop"
            elif k == "join_subscribe":
                if state == "not_subscribed":
                    # Subscribing from a preview sets keep updated on (Android) and keeps
                    # the new-episodes action.
                    policy["keep_updated"], det_keep = True, ct.node
                state, det_sub = "subscribed", ct.node
                if ct.data.get("secret") is not None:
                    secret, det_cred = ct.data["secret"], ct.node
                f.status[ct.node] = "wrote"
            elif k == "join_preview":
                f.status[ct.node] = "noop"  # register-preview never downgrades
            elif k in ("archive", "restore"):
                if state in ("subscribed", "archived"):
                    state, det_sub = ("archived" if k == "archive" else "subscribed"), ct.node
                    f.status[ct.node] = "wrote"
                else:
                    f.status[ct.node] = "ineffective:" + R_NOT_SUBSCRIBED
            elif k == "set_policy":
                fields = ct.data["fields"]
                if "new_episodes_action" in fields:
                    policy["new_episodes_action"], det_action = fields["new_episodes_action"], ct.node
                if "keep_updated" in fields:
                    policy["keep_updated"], det_keep = fields["keep_updated"], ct.node
                f.status[ct.node] = "wrote"
            elif k == "set_credentials":
                secret, det_cred = ct.data["secret"], ct.node
                f.status[ct.node] = "wrote"
        f.records["subscription"] = {"state": state, "generation_id": live}
        f.records["policy"] = policy
        if secret is not None:
            f.records["credentials"] = {"secret_id": secret}
        f.det["subscription"] = det_sub
        f.det["policy.new_episodes_action"] = det_action
        f.det["policy.keep_updated"] = det_keep
        if det_cred is not None:
            f.det["credentials"] = det_cred
        return f

    # -- PROTOCOL §6.10: session families and their save tree --------------------------

    def _session_plans(self, contribs):
        """Per session of the scope (its family), the plan of PROTOCOL §6.10, from the session
        contributions and the statistics cuts of the scope."""
        by_sid, covered = {}, {}
        for ct in contribs:
            if ct.kind in _SESSION_KINDS:
                by_sid.setdefault(ct.data["session_id"], []).append(ct)
            elif ct.kind == "statistics_reset":
                for sid, ops in ct.data["covered"].items():
                    covered.setdefault(sid, set()).update(ops)
        return {sid: self._session_plan(cts, covered.get(sid, ())) for sid, cts in by_sid.items()}

    def _session_plan(self, cts, covered):
        """One family: its continuations and their paths, the save tree, the claiming
        exports, the claim each export writes under, the effect of each finalization and
        the family's played time. `cts` are the session's contributions in rank order.

        Continuations (PROTOCOL §6.10) group the exports (checkpoints and ends) by effective author.
        A continuation's path is its own exports in ordinal order, preceded by the exports
        it shares with its ancestors. CHOICE: the shared exports are those of the session
        in the continuation's own writing history: the chain from its first export (lowest
        ordinal) back through previous_op_id links and, at an applied fork, its head (the protocol says "the facts in its causal past"; the writing history is the part of the
        causal past the copied library wrote itself, so ancestor exports the copy only
        pulled after the divergence are not on its path).

        Exports without save identities (CHOICE): an export without `save` is a tree node of its own whose
        `first` equals no other export's, and never claims; a start without `baseline`
        witnesses no baseline. A session without save metadata is therefore a single path
        whose played time and cuts are exactly the single path's."""
        start = next((ct for ct in cts if ct.kind == "start"), None)
        snode = start.node if start is not None else None
        A = self.eff.get(snode) if snode is not None else None
        exports = [ct for ct in cts if ct.kind in ("checkpoint", "end")]
        fins = [ct for ct in cts if ct.kind == "finalize"]
        cont = {ct.node: self.eff.get(ct.node, self.facts[ct.node]["fact"]["author_id"]) for ct in exports + fins}
        by_op = {ct.node: ct for ct in exports}

        def lineage(Z):
            return A is not None and self.in_lineage(Z, A)

        own = {}
        for ct in exports:
            own.setdefault(cont[ct.node], []).append(ct)
        for Z in own:
            own[Z].sort(key=lambda ct: (ct.data["ordinal"], self.rank[ct.node]))

        def prefix_of(anchor, exclude):
            out = [x for x in self._chain_walk(anchor, limit=snode) if x in by_op and x not in exclude]
            return [by_op[x] for x in reversed(out)]

        paths = {}
        for Z, mine in own.items():
            if Z == A or not lineage(Z):
                paths[Z] = list(mine)
            else:
                paths[Z] = prefix_of(mine[0].node, set(ct.node for ct in mine)) + mine

        # Tree nodes: one per exported save (by save ID); an export without `first`
        # reuses its path's previous node (PROTOCOL §6.10) and adds no node or edge.
        ROOT = ("root",)
        info = {}
        for ct in exports:  # rank order: the first export of a save defines its node
            sv = ct.data.get("save")
            if sv is None:
                k, first, index = ("f", ct.node), ("f", ct.node), ct.data["ordinal"]
            elif "first" in sv:
                k, first, index = ("s", sv["id"]), ("s", sv["first"]), sv["index"]
            else:
                continue
            if k not in info:
                info[k] = {"first": first, "index": index, "contribution": ct.data["contribution"],
                           "baseline": sv["baseline"] if sv is not None else None}
        node_of = {}
        seqs = {}
        for Z, path in paths.items():
            seq, prev = [], ROOT
            for ct in path:
                sv = ct.data.get("save")
                if sv is None:
                    k = ("f", ct.node)
                elif "first" in sv:
                    k = ("s", sv["id"])
                else:
                    k = prev
                if cont[ct.node] == Z:
                    node_of[ct.node] = k
                if k != prev:
                    seq.append(k)
                prev = k
            seqs[Z] = seq
        parent, kids = self._save_tree(seqs, info, own, A, ROOT)

        def ancestors(k):
            out = []
            while k is not None:
                out.append(k)
                k = parent.get(k)
            return out

        # Played time: the uncovered nodes' edges (child minus parent), at least zero. A
        # covered export covers its node and every node before it on every path.
        cov = set()
        for op in covered:
            if op in node_of:
                cov.update(ancestors(node_of[op]))
        contrib = lambda k: 0 if k == ROOT else info[k]["contribution"]  # noqa: E731
        total = sum(contrib(k) - contrib(parent[k]) for k in parent if k != ROOT and k not in cov)
        played = max(0, total)

        # Claims (PROTOCOL §6.10).
        def last_node(Z):
            if Z in paths and paths[Z]:
                return node_of.get(paths[Z][-1].node) or ROOT
            return ROOT

        def lca(a, b):
            anc = set(ancestors(a))
            for k in ancestors(b):
                if k in anc:
                    return k
            return ROOT

        start_bl = start.data.get("baseline") if start is not None else None
        claims = set()
        def nearest_holder(Z):
            P = self.parent_author(Z)
            seen_p = set()
            while P is not None and P != A and P not in own and P not in seen_p:
                seen_p.add(P)
                P = self.parent_author(P)
            return P if P in own else A

        for Z, mine in own.items():
            used = set([start_bl]) - {None}
            if Z == A:
                # Claims are for listening past a divergence (PROTOCOL §6.10); the
                # session's own continuation claims only after its earliest branch point
                # with a continuation that holds exports, and a session without one
                # keeps its own claim (its start).
                children = [Y for Y in own if Y != A and lineage(Y) and nearest_holder(Y) == A]
                if not children:
                    continue
                # The earliest branch point (PROTOCOL §6.10.5): every branch point lies on the
                # session's own path, so the earliest's ancestors are the intersection
                # (the union witnessed baselines past the earliest divergence).
                at_or_before = None
                for Y in children:
                    anc = set(ancestors(lca(last_node(Y), last_node(A))))
                    at_or_before = anc if at_or_before is None else at_or_before & anc
                for ct in exports:
                    sv = ct.data.get("save")
                    k = node_of.get(ct.node)
                    if sv is not None and k in at_or_before:
                        used.add(sv["baseline"])
            elif lineage(Z):
                # The branch point: where the continuation's path leaves its parent
                # continuation's (CHOICE: the nearest lineage ancestor holding exports of
                # the session, else the session's own author), in the tree.
                P = self.parent_author(Z)
                seen_p = set()
                while P is not None and P != A and P not in own and P not in seen_p:
                    seen_p.add(P)
                    P = self.parent_author(P)
                bp = lca(last_node(Z), last_node(P if P in own else A))
                at_or_before = set(ancestors(bp))
                for ct in exports:
                    sv = ct.data.get("save")
                    k = node_of.get(ct.node)
                    if sv is not None and k in at_or_before:
                        used.add(sv["baseline"])
            # CHOICE: "the continuation's first export under a baseline" counts every own
            # export in ordinal order (a passive first export under a baseline means that
            # baseline never claims).
            seen_b = set()
            for ct in mine:
                sv = ct.data.get("save")
                if sv is None or sv["baseline"] in seen_b:
                    continue
                seen_b.add(sv["baseline"])
                if not sv["passive"] and "first" in sv and sv["baseline"] not in used:
                    claims.add(ct.node)
        # PROTOCOL §8.1: an intentional resume (a checkpoint with `claim`) is a claim of
        # its own writing path, with or without a divergence; it joins the family's
        # claim set, so every later export on that path (and only on that path) writes
        # under it, and a finalization naming the session's start from that path means
        # it (the start-ID alias, CHOICE: the latest claim of either kind on the
        # finalizer's path). Only a claim whose save is new on its writing path
        # (acceptance rejects the others; this guards recovered receipts).
        for ct in exports:
            if ct.kind == "checkpoint" and ct.data.get("claim") and not self._save_covered(
                    ct.data["save"], list(self._chain_walk(ct.node, limit=snode)), set(by_op)):
                claims.add(ct.node)
        claim_of = {}
        for Z, path in paths.items():
            cur = snode
            for ct in path:
                if ct.node in claims:
                    cur = ct.node
                elif cont[ct.node] == Z:
                    claim_of[ct.node] = cur

        # Finalizations (PROTOCOL §6.10): a copied finalization (by an author strictly descending
        # from the session's author) is eligible only if no export descends in the tree
        # from the save exported at the ordinal it names; the session's first eligible
        # finalization in order takes effect.
        # CHOICE: every finalization by a strict descendant is a copied one, even if that
        # author also exported saves of the session (a tail export, a derived branch's
        # listening); the session's own author's finalizations are always eligible
        # (literal PROTOCOL §6.10, which makes the outcome depend on which library keeps the author
        # under fallback). CHOICE: eligibility looks at every export of the family,
        # whatever its place in order (an export of the original that arrives later makes
        # an earlier-accepted copied finalization void in the projection; its receipt
        # stays).
        fin = {}
        used_slot = False
        # In a family of several continuations every finalization (the session author's
        # own included) stands only if no export of another continuation descends from
        # the save it names, so the outcome does not depend on which library kept the
        # author; a single continuation keeps the single-path rule.
        exporters = {}
        for ct in exports:
            k = node_of.get(ct.node)
            if k is not None:
                exporters.setdefault(k, set()).add(cont[ct.node])

        def other_descends(k, Z):
            stack = list(kids.get(k, ()))
            while stack:
                x = stack.pop()
                if exporters.get(x, set()) - {Z}:
                    return True
                stack.extend(kids.get(x, ()))
            return False

        several = len(own) >= 2
        for ct in fins:
            Z = cont[ct.node]
            eligible = True
            if Z != A and lineage(Z):
                # CHOICE: the save it names is the export with that ordinal in its own
                # writing history (the nearest); an unresolvable ordinal is ineligible.
                ref_op = next((x for x in self._chain_walk(ct.node, limit=snode)
                               if x in by_op and by_op[x].data["ordinal"] == ct.data["ordinal"]), None)
                k = node_of.get(ref_op) if ref_op is not None else None
                eligible = k is not None and k != ROOT and not other_descends(k, Z)
            elif several:
                ref_op = next((x for x in self._chain_walk(ct.node, limit=snode)
                               if x in by_op and by_op[x].data["ordinal"] == ct.data["ordinal"]), None)
                k = node_of.get(ref_op) if ref_op is not None else None
                eligible = k is None or not other_descends(k, Z)
            if not eligible or used_slot:
                fin[ct.node] = (False, None)
                continue
            used_slot = True
            fclaim = ct.data["claim_id"]
            if snode is not None and fclaim == snode:
                # PROTOCOL §6.10: naming the session's start, from a continuation that has since
                # claimed, means that continuation's claim. CHOICE: the latest claiming
                # export on the finalizer's path ranked before the finalization, its
                # shared part included (the claim its continuation writes under).
                path = paths.get(Z)
                if path is None:
                    path = prefix_of(ct.node, set())
                for x in path:
                    if x.node in claims and self.rank[x.node] < self.rank[ct.node]:
                        fclaim = x.node
            fin[ct.node] = (True, fclaim)
        return {"start": snode, "author": A, "cont": cont, "claims": claims, "claim_of": claim_of, "fin": fin,
                "played": played, "tree": parent, "nodes": info, "node_of": node_of}

    def _save_tree(self, seqs, info, own, A, ROOT):
        """PROTOCOL §6.10: attaches every path (a sequence of save nodes) to one tree. Two paths
        share their common nodes; after the last common export, if their next exports
        cover the same first save, the one with the smaller save index is the branch point
        and lies on both paths. So the nodes with equal `first` under one node form a chain
        in index order. CHOICE: two such nodes with the same index but different save IDs
        (impossible when clients keep the restart/tail export obligations) are siblings.
        Paths are attached in a fixed order (the session's own author first, then by the
        rank of each continuation's first export); for well-formed input the result does
        not depend on it."""
        parent = {ROOT: None}
        kids = {ROOT: []}

        def chain_child(p, first):
            cands = [k for k in kids[p] if info[k]["first"] == first]
            if not cands:
                return None
            return min(cands, key=lambda k: (info[k]["index"], k))

        def attach(k, p):
            parent[k] = p
            kids.setdefault(p, []).append(k)
            kids.setdefault(k, [])

        order = sorted(seqs, key=lambda Z: (Z != A, min((self.rank[ct.node] for ct in own.get(Z, ())), default=-1), Z))
        for Z in order:
            cur = ROOT
            for k in seqs[Z]:
                if k in parent:
                    cur = k
                    continue
                first, index = info[k]["first"], info[k]["index"]
                p = cur
                nxt = chain_child(p, first)
                while nxt is not None and info[nxt]["index"] < index:
                    p = nxt
                    nxt = chain_child(p, first)
                attach(k, p)
                if nxt is not None and info[nxt]["index"] > index:
                    kids[p].remove(nxt)
                    attach(nxt, k)
                cur = k
        return parent, kids

    def _fold_episode_scope(self, E, G):
        """The episode scope's single state machine (PROTOCOL §7.2, extended by PROTOCOL §8):
        play state, position with its ownership claim, favorite, and the listening state
        (claims, sessions, history entries, history and statistics cuts), folded together
        in rank order."""
        f = _Fold()
        # Contributions of an admission that is not the effective one of its subject
        # take no effect.
        contribs = [ct for ct in self._sorted(("episode", E, G)) if self._live_contribution(ct)]
        # PROTOCOL §9.7, §7: in a merged episode only the earlier admission (by rank) files.
        # A proposal bundle's move of another member carries its proposer's admission
        # (it counts only while that admission is effective) but is not this episode's
        # filing (PROTOCOL §9.4: an explicit move), so it neither counts as an admission here
        # nor is dropped.
        adms = set(ct.data["adm"] for ct in contribs if "adm" in ct.data and ct.kind != "q_move")
        if len(adms) > 1:
            keep = min(adms, key=lambda d: self.decisions[d]["rank"])
            contribs = [ct for ct in contribs if "adm" not in ct.data or ct.kind == "q_move" or ct.data["adm"] == keep]
        barrier = any(_is_barrier(ct) for ct in contribs)
        # An imported NEW is the Inbox as an admission's filing is (explicit
        # interactions stop it).
        admissions = [ct for ct in contribs if ct.kind == "admission"
                      or (ct.kind == "import" and ct.data["imported"]["play_state"] == "new")]
        admitted, det_adm = None, None
        play, det_play = "unplayed", None
        if admissions and not barrier:
            play, det_play = "new", admissions[0].node
        pos, claim, det_pos = 0, None, None
        fav, det_fav = False, None
        touch_play = touch_pos = touch_fav = False
        # Listening state. A session's claim is its start node. CHOICE: taken from the scope's
        # start contributions, so that a (non-compliant) checkpoint ranked before its own
        # start simply does not own the position there. Per session (family), the
        # plan of its continuations, save tree, claims and finalizations.
        starts = {ct.data["session_id"]: ct.node for ct in contribs if ct.kind == "start"}
        plans = self._session_plans(contribs)
        if starts:
            # Every session fact has its start in this scope (PROTOCOL §8 check 2), and a start
            # always writes the position and conditionally the play state.
            touch_play = touch_pos = True
        current = None  # the current claim: the node that owns the position
        cont_ords = {}  # (session_id, effective author) -> ordinals folded so far
        entries = []  # history entries: (node, normalized time, is listening activity)
        removals = {}  # history.remove node -> covered entry nodes
        restored = set()  # removal nodes whose cut a restore cancelled
        clears = []  # covered entry nodes of each history.clear contributed here
        duration, det_dur = None, None
        left = set()  # PROTOCOL §8: transitions whose effective leave was folded
        # Membership decisions (node, explicit, in_queue, reason, admission) in fold
        # order; resolved afterwards because an automatic add depends on whether any
        # explicit decision exists at all. Positions are the queue replay's
        # (PROTOCOL §7.6: published by queue.layout decisions).
        mem_events = []
        for ct in contribs:
            k, n = ct.kind, ct.node
            t = self.node_time[n]
            if k in _PLAY_KINDS:
                touch_play = True
            if k in ("q_add", "q_restore"):
                # Membership in (explicit), `new` -> `unplayed`.
                if play == "new":
                    play, det_play = "unplayed", n
                    f.status[n] = "fired"
                else:
                    f.status[n] = "wrote"
                mem_events.append((n, True, True, None, None))
            elif k == "q_resolved":
                # The start's own queue effect (PROTOCOL §7.6), if its decision is the effective one.
                d = ct.data
                if self._effective(d["decision"]):
                    if play == "new":
                        play, det_play = "unplayed", n
                    mem_events.append((n, True, True, None, None))
            elif k == "q_move":
                f.status.setdefault(n, "wrote")  # a replay step only (it never writes membership)
            elif k == "q_remove":
                mem_events.append((n, True, False, ct.data["reason"], None))
                f.status[n] = "wrote"
            elif k == "q_clear":
                mem_events.append((n, True, False, "cleared", None))
                f.status[n] = "wrote"
            elif k == "q_auto":
                mem_events.append((n, False, True, None, ct.data["adm"]))
            elif k == "q_import":
                # PROTOCOL §7.6: the imported queue entry, explicit, at the import's
                # completing decision (no NEW dismissal, no barrier: the backup's state).
                mem_events.append((n, True, True, None, None))
                f.status[n] = "wrote"
            elif k == "import":
                # An imported episode's state, written like a restore but no barrier (its
                # NEW is the Inbox, above); its position is a claim.
                im = ct.data["imported"]
                touch_play = touch_pos = True
                if im["play_state"] != "new":
                    play, det_play = im["play_state"], n
                pos, claim, det_pos = im["position"], n, n
                current = n
                if im["favorite"]:
                    touch_fav = True
                    fav, det_fav = True, n
                f.status[n] = "wrote"
            elif k == "admission":
                f.status[n] = "fired" if not barrier else "ineffective:barrier"
            elif k == "admitted":
                # Shown in the scope's generation (a joined source's moves with it).
                admitted, det_adm = dict(ct.data["value"], generation_id=G), n
            elif k == "mark_played":
                play, det_play = "played", n
                if ct.data["reset"]:
                    touch_pos = True
                    pos, claim, det_pos = 0, n, n
                    current = n  # mark played with reset is a claim (PROTOCOL §8)
                f.status[n] = "wrote"
            elif k == "mark_unplayed":
                play, det_play = "unplayed", n
                f.status[n] = "wrote"
            elif k == "reset_position":
                touch_pos = True
                play, det_play = "unplayed", n
                pos, claim, det_pos = 0, n, n
                current = n
                f.status[n] = "wrote"
            elif k == "restore_state":
                touch_pos = True
                play, det_play = ct.data["play_state"], n
                pos, claim, det_pos = ct.data["position"], n, n
                current = n
                f.status[n] = "wrote"
            elif k == "dismiss":
                if play == "new":
                    play, det_play = "unplayed", n
                    f.status[n] = "fired"
                else:
                    f.status[n] = "noop"
            elif k == "favorite":
                touch_fav = True
                fav, det_fav = ct.data["on"], n
                f.status[n] = "wrote"
            elif k == "start":
                d = ct.data
                current = n
                pos, claim, det_pos = d["position"], n, n
                if play == "new":  # never played -> unplayed
                    play, det_play = "unplayed", n
                if d["activity"]:
                    entries.append((n, t, True))
                f.status[n] = "wrote"
            elif k in ("checkpoint", "end"):
                d = ct.data
                plan = plans[d["session_id"]]
                # Ordinals, not rank order, choose a session's latest save, so
                # only a save newer than every save of its session ranked before it may write the
                # position. PROTOCOL §6.10: within its continuation.
                seen = cont_ords.setdefault((d["session_id"], plan["cont"][n]), [])
                newest = all(d["ordinal"] > o for o in seen)
                seen.append(d["ordinal"])
                if n in plan["claims"]:
                    # PROTOCOL §6.10: an export that claims takes the position at its own place in
                    # order, like a start (CHOICE: whatever older save of its continuation
                    # was ranked before it).
                    current = n
                    sclaim, owns = n, True
                else:
                    # Without a claim of its own, the continuation writes under the claim
                    # its path holds: the latest claiming export before it on its path,
                    # else the session's start. A passive export (a restart or tail
                    # export) of a save that some export of the family descends from in
                    # the tree is stale and writes nothing, so a copy's tail ordered
                    # after the original's later export cannot move the position back,
                    # while a passive export nobody went past still does.
                    sclaim = plan["claim_of"][n]
                    owns = sclaim is not None and current == sclaim and newest
                    sv = d.get("save")
                    if owns and sv is not None and sv.get("passive"):
                        k0 = plan["node_of"].get(n)
                        if k0 is not None and any(pk == k0 for kk, pk in plan["tree"].items() if kk != k0):
                            owns = False
                if owns:
                    pos, claim, det_pos = d["position"], sclaim, n
                if d["activity"]:
                    # CHOICE: the activity effects apply whether or not the session owns
                    # the position (non-owning sessions are real listening, PROTOCOL §8).
                    if play == "new":
                        play, det_play = "unplayed", n
                    entries.append((n, t, True))
                f.status[n] = "wrote"
                if k == "end":
                    if d["completed"] and owns:
                        # The completion bundle under the validated claim, which stays
                        # current, with queue membership out (reason completed).
                        play, det_play = "played", n
                        pos, claim, det_pos = 0, sclaim, n
                        f.status[n] = "fired"
                        mem_events.append((n, True, False, "completed", None))
                    elif d["remove_from_queue"]:
                        # Otherwise the device's skip removal (reason skipped). CHOICE:
                        # unconditional at its place (no ownership condition): it is the
                        # device's explicit queue decision.
                        mem_events.append((n, True, False, "skipped", None))
                    if d["history"]:
                        entries.append((n, t, False))
            elif k == "finalize":
                d = ct.data
                effect, fclaim = plans[d["session_id"]]["fin"][n]
                if not effect:
                    # PROTOCOL §6.10: an ineligible copied finalization, or one after the session's
                    # effective finalization, takes no completion or queue effect. As
                    # with a superseded finalization, its history mark still counts: it is a real
                    # event on its device.
                    f.status[n] = "void"
                    if d["history"]:
                        entries.append((n, t, False))
                    continue
                if d["completed"] and current is not None and fclaim == current:
                    play, det_play = "played", n
                    pos, claim, det_pos = 0, fclaim, n
                    f.status[n] = "fired"
                    mem_events.append((n, True, False, "completed", None))
                else:
                    f.status[n] = "noop"
                    if d["remove_from_queue"]:
                        mem_events.append((n, True, False, "skipped", None))
                if d["history"]:
                    entries.append((n, t, False))
            elif k == "leave":
                # PROTOCOL §8. Not a claim, not a barrier, no NEW dismissal, no
                # contribution, cut or duration, not listening activity. Only the first
                # effective leave of a transition completes or removes (CHOICE: per episode
                # scope, the scope the leave applies to; a transition names one episode).
                # The completion bundle fires if the current claim here is the observed one
                # (both absent counts as equal), under that claim, which stays current.
                # CHOICE: a leave touches play_state and position only when its bundle
                # fires, and queue membership when its bundle or removal applies.
                d = ct.data
                T = d["transition_id"]
                if T in left:
                    f.status[n] = "void"
                elif d["completed"] and current == d["observed_claim"]:
                    touch_play = touch_pos = True
                    play, det_play = "played", n
                    pos, claim, det_pos = 0, current, n
                    f.status[n] = "fired"
                    mem_events.append((n, True, False, "completed", None))
                    left.add(T)
                elif d["remove_from_queue"]:
                    f.status[n] = "removed"
                    mem_events.append((n, True, False, "skipped", None))
                    left.add(T)
                else:
                    f.status[n] = "noop"
                if d["history"]:
                    # A history mark at the fact's normalized time, as an end's or a
                    # finalization's (never last_played).
                    entries.append((n, t, False))
            elif k == "history_remove":
                removals[n] = ct.data["covered"]
            elif k == "history_restore":
                restored.add(ct.data["removal"])
            elif k == "history_clear":
                clears.append(ct.data["covered"])
            if k in ("start", "checkpoint", "end") and "duration" in ct.data:
                # PROTOCOL §6.10: the installation is the reporting fact's effective author.
                duration, det_dur = (ct.data["duration"], self.eff.get(n, self.facts[n]["fact"]["author_id"])), n
        if touch_play:
            f.records["play_state"] = {"generation_id": G, "play_state": play}
            if det_play is not None:
                f.det["play_state"] = det_play
        if touch_pos:
            v = {"generation_id": G, "position": pos}
            if claim is not None:
                v["claim_id"] = claim
            f.records["position"] = v
            if det_pos is not None:
                f.det["position"] = det_pos
        if touch_fav:
            f.records["favorite"] = {"generation_id": G, "on": fav}
            if det_fav is not None:
                f.det["favorite"] = det_fav
        if admitted is not None:
            # PROTOCOL §9.4: the episode's `admission` record, per generation (tombstoned when it
            # closes, like the other episode state groups).
            f.records["admission"] = copy.deepcopy(admitted)
            f.det["admission"] = det_adm
        # PROTOCOL §7.6 membership: explicit decisions are a register (the last in order wins); an
        # automatic add applies only if the scope has no explicit decision at all.
        # CHOICE: the record exists once the register has a value -- an explicit decision
        # (a fired completion bundle or a skip removal counts, even for an episode never
        # queued) or an applied automatic add; `reason` is present only when out.
        explicit = any(e[1] for e in mem_events)
        mem, det_mem = None, None
        for n, expl, in_queue, reason, adm in mem_events:
            if expl or not explicit:
                mem, det_mem = (in_queue, reason), n
                # PROTOCOL §7.6: the effective membership events, in order, are the
                # scope's membership at each replay step.
                f.mem_timeline.append((self.rank[n], in_queue, n))
                if adm is not None:
                    f.auto_applied.add(adm)
            if not expl:
                f.status[n] = "ineffective:" + R_EXPLICIT_STATE if explicit else "fired"
        if mem is not None:
            v = {"generation_id": G, "in_queue": mem[0]}
            if mem[1] is not None:
                v["reason"] = mem[1]
            f.records["queue_membership"] = v
            f.det["queue_membership"] = det_mem
        # PROTOCOL §7.6: the position record is the scope's published key,
        # its latest key write in rank order (absent without one; _sync_pub keeps it in step).
        # CHOICE: only for a scope with a membership record (an identity partition can move
        # the membership a key write saw to another episode; the stale key is evidence only).
        qp = self.pubkeys.get((E, G))
        if qp is not None and mem is not None:
            f.records["queue_position"] = {"generation_id": G, "key": qp[0], "position_id": qp[1]}
        # Derived listening values. last_played: the greatest time among listening activities
        # (history marks and cuts never change it).
        activity_times = [t for _, t, act in entries if act]
        if activity_times:
            f.records["last_played"] = {"generation_id": G, "time": max(activity_times)}
        # history: the greatest time among entries not suppressed by a cut (a removal's cut
        # unless a restore cancelled it; every clear's cut), else null. CHOICE: the record
        # exists once an entry exists (a cut alone creates none).
        if entries:
            suppressed = set()
            for rn, covered in removals.items():
                if rn not in restored:
                    suppressed |= covered
            for covered in clears:
                suppressed |= covered
            visible = [t for n, t, _ in entries if n not in suppressed]
            f.records["history"] = {"generation_id": G, "time": max(visible) if visible else None}
        # played_time: PROTOCOL §6.10, per session family the sum of the uncovered save-tree edges,
        # at least zero (for a single path max(0, C_latest - C_cut)).
        if starts:
            total = sum(plan["played"] for plan in plans.values())
            f.records["played_time"] = {"generation_id": G, "played_ms": total}
        # measured_duration: the last `duration` in order, with its installation (CHOICE:
        # the reporting fact's author_id, its immutable source installation).
        if duration is not None:
            f.records["measured_duration"] = {"generation_id": G, "duration_ms": duration[0],
                                              "installation_id": duration[1]}
            f.det["measured_duration"] = det_dur
        return f

    def _fold_account(self):
        f = _Fold()
        action, det = None, None
        # PROTOCOL §7.6: `enqueue_location` and `sorting` ({keep_sorted, sort_order}) are two
        # independent registers; the record exists once a queue policy fact contributed
        # (CHOICE, as discovery_policy), with Android's defaults for an unwritten register.
        qpol, det_loc, det_sorting, touched = dict(DEFAULT_QUEUE_POLICY), None, None, False
        for ct in self._sorted(("account",)):
            if ct.kind == "discovery":
                action, det = ct.data["action"], ct.node
            else:
                touched = True
                if "enqueue_location" in ct.data:
                    qpol["enqueue_location"], det_loc = ct.data["enqueue_location"], ct.node
                if "sorting" in ct.data:
                    qpol["keep_sorted"] = ct.data["sorting"]["keep_sorted"]
                    qpol["sort_order"] = ct.data["sorting"]["sort_order"]
                    det_sorting = ct.node
            f.status[ct.node] = "wrote"
        if action is not None:
            f.records["discovery_policy"] = {"new_episodes_action": action}
            f.det["discovery_policy"] = det
        if touched:
            f.records["queue_policy"] = qpol
            if det_loc is not None:
                f.det["queue_policy.enqueue_location"] = det_loc
            if det_sorting is not None:
                f.det["queue_policy.sorting"] = det_sorting
        return f

    # -- projections ---------------------------------------------------------------

    def _set_entity(self, key, recs):
        if recs:
            self.proj[key] = copy.deepcopy(recs)
        else:
            self.proj.pop(key, None)

    def _episode_records(self, E):
        ep = self.episodes.get(E)
        if ep is None:
            return {}
        recs = {"identity": {"podcast_id": ep["podcast_id"], "occurrence": copy.deepcopy(ep["occurrence"])}}
        if E in self.root:
            # PROTOCOL §9.7: a merged episode keeps its identity, naming its survivor, and loses its
            # state and catalog records.
            recs["identity"]["merged_into"] = self.root[E]
            return recs
        # An episode a podcast merge carried to the survivor without a
        # mapping is the survivor's (CHOICE: its identity names the survivor).
        P = self._podcast_root(ep["podcast_id"])
        recs["identity"]["podcast_id"] = P
        # PROTOCOL §9.4: catalog metadata is not per generation and stays after an unsubscribe.
        meta = self._fold_meta(("meta", "episode", E))[0]
        if meta is not None:
            recs["metadata"] = meta
        live = self._live_generation(P)
        if live is not None and live in self.episode_gens.get(E, ()):
            recs.update(self._fold_episode_scope(E, live).records)
        return recs

    def _refresh_episode(self, E):
        self._set_entity(("episode", E), self._episode_records(E))

    def _refresh_podcast(self, P, all_episodes=False):
        fold = self._fold_podcast(P)
        self._set_entity(("podcast", P), fold.records if fold is not None else {})
        if all_episodes:
            for E, ep in self.episodes.items():
                if ep["podcast_id"] == P or self._podcast_root(ep["podcast_id"]) == P:
                    self._refresh_episode(E)
        return fold if fold is not None else _Fold()

    def _refresh_account(self):
        fold = self._fold_account()
        self._set_entity(("account", self.account_id), fold.records)
        return fold

    @staticmethod
    def _flatten(proj):
        out = []
        for (entity, eid), recs in proj.items():
            for fg, v in recs.items():
                out.append({"entity": entity, "id": eid, "field_group": fg, "value": copy.deepcopy(v)})
        return sort_records(out)

    def records(self):
        """The current live records, sorted by (entity, id, field_group)."""
        self._ensure()
        return self._flatten(self.proj)

    def full_records(self):
        """Recomputes every scope from scratch (to check the incremental projection)."""
        self._ensure()
        proj = {}
        for P in self.podcasts:
            fold = self._fold_podcast(P)
            if fold is not None and fold.records:
                proj[("podcast", P)] = fold.records
        for E in self.episodes:
            recs = self._episode_records(E)
            if recs:
                proj[("episode", E)] = recs
        acc = self._fold_account().records
        if acc:
            proj[("account", self.account_id)] = acc
        return self._flatten(proj)

    def alias_decisions(self):
        return {n: {"podcast_id": a["podcast_id"], "op_id": a["op_id"]} for n, a in self.aliases.items()}

    def episode_decisions(self):
        return {E: {"podcast_id": e["podcast_id"], "occurrence": copy.deepcopy(e["occurrence"]), "op_id": e["op_id"]}
                for E, e in self.episodes.items()}

    def created_decisions(self):
        """The decisions this engine made (PROTOCOL §7.6), sorted by decision ID."""
        return sorted(copy.deepcopy(self.created), key=lambda d: _bkey(d["decision_id"]))

    def decision_arrivals(self):
        """Every decision this engine holds, as recovery arrivals (for replays)."""
        return [{"decision": copy.deepcopy(d["obj"])} for d in self.decisions.values()]

    def result(self):
        self._ensure()
        out = {
            "resolutions": copy.deepcopy(self.resolutions),
            "outcomes": copy.deepcopy(self.outcomes),
            "records": self.records(),
        }
        if self.created:
            # Absent when the case makes no decision (older vectors have no member).
            out["decisions"] = self.created_decisions()
        # PROTOCOL §7.6: the canonical sort values the live receipts recorded (absent
        # when none did).
        sv = dict((op, copy.deepcopy(v)) for op, v in self.recorded.items() if v and op in self.outcomes)
        if sv:
            out["sort_values"] = sv
        return out


def run_case(case, fixed_resolutions=None, fixed_aliases=None, fixed_episodes=None):
    """Runs a `rules` case's arrivals; returns {resolutions, outcomes, decisions, records}."""
    if not isinstance(case, dict):
        raise RuleInputError("case is not an object")
    for k in ("account_id", "namespace", "arrivals"):
        if k not in case:
            raise RuleInputError("case lacks %r" % k)
    if not isinstance(case["arrivals"], list):
        raise RuleInputError("arrivals is not an array")
    eng = Engine(case["account_id"], case["namespace"], fixed_resolutions, fixed_aliases, fixed_episodes)
    for a in case["arrivals"]:
        eng.accept(a)
    return eng.result()
