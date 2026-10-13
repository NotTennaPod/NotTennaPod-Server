"""Canonical queue sort (PROTOCOL §7.6), written from the protocol and Android 3.12.1's
FeedItemPermutors (its code, not its doc-comment
diagrams) -- never from the Go code. Standard library only.

* `canonical_order(items, order)`: the comparators of the sort orders Android's queue sort
  dialog offers, on canonical values, stable over the current queue order (Java's
  `Collections.sort` is a stable merge sort). Titles compare by their Unicode simple
  lowercase mapping, then by code point. Missing values are Android's defaults (title "",
  publication time 0, duration 0). Smart shuffle is Android's algorithm with its double
  arithmetic; equally large podcast groups (Android: HashMap order) are placed in
  ascending podcast ID order.
* `retained_positions(seq)`: the members that keep their keys (a longest increasing
  subsequence of current queue positions).
* `rekey(current, order, new_position_id)`: the keys of the sorted result.

Choices made where the protocol is silent are marked "CHOICE:".
"""

import os
import sys
from bisect import bisect_left

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ntp_contract as c  # noqa: E402

__all__ = [
    "SORT_ORDERS",
    "KEEP_SORTED_ORDERS",
    "simple_lower",
    "sort_values",
    "canonical_order",
    "retained_positions",
    "rekey",
]

# PROTOCOL §7.6: the orders Android 3.12.1's queue sort dialog offers (size and file name are
# not offered for the queue).
SORT_ORDERS = (
    "episode_title_a_z",
    "episode_title_z_a",
    "feed_title_a_z",
    "feed_title_z_a",
    "duration_short_long",
    "duration_long_short",
    "date_old_new",
    "date_new_old",
    "random",
    "smart_shuffle_old_new",
    "smart_shuffle_new_old",
)
# `random` turns keep-sorted off (Android) and is never a server sort.
KEEP_SORTED_ORDERS = tuple(o for o in SORT_ORDERS if o != "random")

VALUE_FIELDS = ("title", "podcast_id", "podcast_title", "publication_time", "duration")


def _simple_lower_char(ch):
    # Python's str.lower() is the full lowercase mapping. Applied to one code point it
    # has no context (so no Final_Sigma rule) and differs from the simple mapping
    # (UnicodeData.txt field 13) only for U+0130, whose full mapping is "i" + U+0307 and
    # whose simple mapping is "i" (checked over every code point by selftest_rules.py).
    low = ch.lower()
    if len(low) == 1:
        return low
    if ch == "İ":
        return "i"
    raise AssertionError("no simple lowercase mapping known for U+%04X" % ord(ch))


def simple_lower(s):
    """The Unicode simple lowercase mapping of every code point (no locale, no context)."""
    return "".join(_simple_lower_char(ch) for ch in s)


def sort_values(v):
    """A member's canonical sort values with Android's defaults for missing ones."""
    return {
        "title": v.get("title") if v.get("title") is not None else "",
        "podcast_id": v.get("podcast_id") if v.get("podcast_id") is not None else "",
        "podcast_title": v.get("podcast_title") if v.get("podcast_title") is not None else "",
        "publication_time": v.get("publication_time") if v.get("publication_time") is not None else 0,
        "duration": v.get("duration") if v.get("duration") is not None else 0,
    }


def _smart_shuffle(vals, ascending):
    """FeedItemPermutors.smartShuffle on indices 0..n-1 (current queue order)."""
    groups = {}
    for i, v in enumerate(vals):
        groups.setdefault(v["podcast_id"], []).append(i)
    feeds = []
    for pid, idx in groups.items():
        # Each group by publication time, stable over the queue order (descending keeps
        # equal times in queue order too: Java's comparator f2 vs f1 in a stable sort).
        idx = sorted(idx, key=lambda i: vals[i]["publication_time"], reverse=not ascending)
        feeds.append((pid, idx))
    # Largest group first. Android's ties keep HashMap iteration order; CHOICE (the protocol says
    # "by podcast ID"): ascending podcast ID, bytewise.
    feeds.sort(key=lambda f: (-len(f[1]), f[0].encode("utf-8")))
    n = len(vals)
    queue = [None] * n
    empty = list(range(n))
    for _, items in feeds:
        spread = len(empty) / (len(items) + 1)  # Java: (double) size / (items + 1)
        skipped = 0
        placed = 0
        remaining = []
        k = 0
        while k < len(empty):
            slot = empty[k]
            k += 1
            skipped += 1
            # Java compares the int with the double product exactly; so does Python.
            if skipped >= spread * (placed + 1):
                queue[slot] = items[placed]
                placed += 1
                if placed == len(items):
                    remaining.extend(empty[k:])
                    break
                continue
            remaining.append(slot)
        if placed != len(items):
            raise AssertionError("smart shuffle left items unplaced")
        empty = remaining
    if empty or any(q is None for q in queue):
        raise AssertionError("smart shuffle left empty slots")
    return queue


def canonical_order(items, order):
    """`items` are sort-value objects in current queue order; returns their indices in
    sorted order for one of KEEP_SORTED_ORDERS."""
    vals = [sort_values(v) for v in items]
    idx = list(range(len(vals)))
    if order in ("episode_title_a_z", "episode_title_z_a"):
        keys = [simple_lower(v["title"]) for v in vals]
        return sorted(idx, key=lambda i: keys[i], reverse=order.endswith("z_a"))
    if order in ("feed_title_a_z", "feed_title_z_a"):
        keys = [simple_lower(v["podcast_title"]) for v in vals]
        return sorted(idx, key=lambda i: keys[i], reverse=order.endswith("z_a"))
    if order in ("duration_short_long", "duration_long_short"):
        # CHOICE: exact integer comparison (Android's int milliseconds would overflow only
        # beyond 24.8 days).
        return sorted(idx, key=lambda i: vals[i]["duration"], reverse=order == "duration_long_short")
    if order in ("date_old_new", "date_new_old"):
        return sorted(idx, key=lambda i: vals[i]["publication_time"], reverse=order == "date_new_old")
    if order in ("smart_shuffle_old_new", "smart_shuffle_new_old"):
        return _smart_shuffle(vals, order == "smart_shuffle_old_new")
    raise ValueError("no canonical order %r" % (order,))


def retained_positions(seq):
    """`seq[i]` is the current queue position of the i-th member of the sorted result
    (distinct integers). Returns the set of result positions along a longest increasing
    subsequence of `seq`.

    CHOICE (the protocol says only "a longest increasing subsequence"): the
    lexicographically first one by result position: the earliest result position that
    starts a longest one, then repeatedly the earliest later position that continues one.
    """
    n = len(seq)
    if n == 0:
        return set()
    longest = [0] * n  # longest[i]: length of the longest increasing run starting at i
    tails = []  # tails[k] = -(largest first value of an increasing run of length k + 1)
    for i in range(n - 1, -1, -1):
        v = -seq[i]
        k = bisect_left(tails, v)
        longest[i] = k + 1
        if k == len(tails):
            tails.append(v)
        else:
            tails[k] = v
    best = max(longest)
    out = set()
    need, last = best, None
    for j in range(n):
        if need == 0:
            break
        if longest[j] == need and (last is None or seq[j] > last):
            out.add(j)
            last = seq[j]
            need -= 1
    return out


def rekey(current, order, new_position_id):
    """`current`: [(key, position_id)] in current queue order; `order`: current indices in
    sorted order. Returns [(key, position_id, fresh)] in sorted order: members along the
    retained subsequence keep theirs; every other one gets a fresh key from the contract-v1
    allocator with `new_position_id()`.

    CHOICE: fresh keys are allocated in sorted order, each strictly between the key just
    assigned to its predecessor (or sentinel 0) and the next retained key (or sentinel 1),
    consuming position IDs in that order."""
    keep = retained_positions(order)
    out = []
    n = len(order)
    for i in range(n):
        if i in keep:
            k, pid = current[order[i]]
            out.append((k, pid, False))
            continue
        low = out[i - 1][0] if i > 0 else None
        high = None
        for j in range(i + 1, n):
            if j in keep:
                high = current[order[j]][0]
                break
        pid = new_position_id()
        out.append((c.queue_between(low, high, c.uuid_to_u(pid)), pid, True))
    return out
