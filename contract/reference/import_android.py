#!/usr/bin/env python3
"""Independent reference for PROTOCOL §11.5: the server-side import of an Android
AntennaPod 3.12.1 database backup (`Antennapod.db`, `user_version` 3110000).

Written from docs/PROTOCOL.md (§11.5 the import, §3.3 identity, §9.7 release groups, §8.8
statistics), plus Android AntennaPod 3.12.1's own source (tag 3.12.1). It never reads the Go implementation. Standard library only; the URL
normalization and GUID trimming come from `ntp_contract.py` (contract-v1).

    python3 contract/reference/import_android.py <Antennapod.db>      # expected import as JSON

What it produces (the JSON format is specified in docs/CONTRACT.md, "Import vectors"):

1. **The expected import**, expressed in source IDs (`Feeds.id`, `FeedItems.id`,
   `FeedMedia.id`): podcasts with lifecycle state, keep-updated, new-episodes action and
   credential presence; their episodes, each a set of source items with its identity rule,
   its state (play state, Inbox, position, favorite, queue, duration), the state row's
   baseline and the other rows' separate statistical units; the queue order; the result's
   imported counts and skipped, ambiguous and merged lists.
2. **Android's numbers**: Android 3.12.1's own statistics SQL run on the file
   (`PodDBAdapter.getFeedStatisticsCursor` and `getMonthlyStatisticsCursor`, see
   `ANDROID_FEED_STATISTICS_SQL` and `ANDROID_MONTHLY_SQL`).
3. **The projection**: the same numbers computed only from the expected episodes and units
   with PROTOCOL §8 client projection rules, and every mismatch with Android's numbers.

Which Android code each number follows (all at tag 3.12.1):

- Per podcast, "Subscriptions" statistics screen (`SubscriptionStatisticsFragment` ->
  `DBReader.getStatistics(includeMarkedAsPlayed, from, to)` ->
  `PodDBAdapter.getFeedStatisticsCursor`): one row per `Feeds.id` with `state != 1`
  (subscribed and archived), over `FeedItems LEFT JOIN FeedMedia INNER JOIN Feeds`:
  - `played_time` = SUM over rows with `last_played_time >= from AND < to` of
    `played_duration` (with "include marked as played": `played_duration` if `!= 0`, else
    `duration` if `read = 1`, else 0). `DBReader` divides by 1000 per feed for display;
    this reference keeps milliseconds. The default filter is `[0, Long.MAX_VALUE)`
    (`StatisticsFragment` preferences); "include marked as played" forces it
    (`StatisticsFilterDialog`), so marked-played totals are all-time only.
  - `episodes_started` = COUNT of rows with `playback_completion_date > 0 AND
    played_duration > 0` (with "include marked as played": `OR read = 1 OR position > 0`),
    never date-filtered.
  - `num_episodes` = COUNT(*) of joined rows; `total_time` = SUM(`duration`).
- Per podcast, the per-feed statistics dialog (`FeedStatisticsFragment`) calls
  `DBReader.getStatistics(true, 0, Long.MAX_VALUE)`: the "include marked as played" numbers
  (episodes started, episodes total, time played, total duration).
- Per month, "Years" screen (`YearsStatisticsFragment` -> `DBReader.getMonthlyTimeStatistics`
  -> `PodDBAdapter.getMonthlyStatisticsCursor`): SUM(`played_duration`) of every
  `FeedMedia` row with `last_played_time > 0 AND played_duration > 0`, grouped by the UTC
  year and month of `last_played_time` (`strftime` on `datetime(ms/1000, 'unixepoch')`).
  Every media row counts, whatever its feed's state (previews too). Years are the sums of
  their months (`YearStatisticsListAdapter`).

Design points, applied here:

1. A unit is made for every other row of an episode that has a media row, even an all-zero
   one; rows without a media row make none. A unit carries its row's played time, last
   played, history, duration, PLAYED flag and position, and counts in "episodes", "total
   duration" and "started" (with "include marked as played": also PLAYED or position > 0).
2. A merged podcast takes the properties of its lowest-ID non-preview feed (the lowest-ID
   feed when all are previews). Its credentials are that feed's, else those of the first
   feed in ID order that has any. A preview feed's rows joining a listed podcast change its
   numbers: an expected difference of the merge.
3. An Android preview feed (state 1) is imported only if one of its rows holds user state:
   played, NEW or a position; history, last played or played time; favorite or queue.
   Otherwise it is skipped with reason `unused_preview`.
4. A merged episode's history is its state row's own (playback_completion_date > 0 of the
   state row); units keep their own. (Not "the latest among its rows", so that "episodes started" stays exactly Android's; the
   History list may then miss a merged episode whose only history is in another row.)
5. Release groups as below (two passes, publication-date order).
6. Userinfo in a feed URL is stripped and becomes the secret.
7. A missing last-played (counted as 0 by the client's date filter) and local-folder
   listening (skipped) are client rules and an inherent skip: expected differences, not
   mismatches.

Every difference between Android's numbers and the projection is attributed: each detected
cause names the source rows involved and an adjustment of a scratch copy of the file (delete
the skipped feeds' rows, list the joined preview feed, ...). A difference that disappears
when Android's SQL runs on the copy with its causes adjusted is an expected difference,
listed with the causes that change it; anything left is a mismatch. No fixture has one.

Judgement calls (each one also noted where it is applied):

- CHOICE: Only user_version 3110000 is read (older files are out of scope for this
  reference); anything else raises UnsupportedFile, as do files that fail SQLite's
  integrity_check or lack a table.
- CHOICE: A feed whose download_url is NULL or empty cannot be imported: it is skipped
  with reason 'no_feed_url' (Android itself could not refresh it).
- CHOICE: Feed URLs are grouped by contract-v1 normalize_url after removing the URL's
  userinfo; the podcast URL is the creating feed's URL without it.
- CHOICE: A feed has credentials when its username or password is non-empty, or its URL has
  userinfo (BasicAuthorizationInterceptor: !isEmpty(username) || !isEmpty(password)).
- CHOICE: The unused-preview rule applies per feed, before URL grouping: an unused preview
  never joins another feed's podcast. User state means read 1 or -1; position > 0;
  playback_completion_date, last_played_time > 0; played_duration != 0; a Favorites or
  Queue row.
- CHOICE: Feeds.state other than 0, 1, 2 is 'subscribed' (Android's statistics list it,
  state != 1).
- CHOICE: new_episodes_action other than 0-3 is 'inbox' (NewEpisodesAction.fromCode's
  default); NULL is 0, 'global'.
- CHOICE: keep_updated is true when > 0, as FeedPreferencesCursor reads it (getInt(...)
  > 0, so NULL is false).
- CHOICE: The podcast's GUID kind K is 'atom-id' when the creating (lowest-ID) feed's
  type is 'atom', else 'rss-guid' (type NULL or 'rss').
- CHOICE: Each item is joined with its lowest-id FeedMedia row (FeedMedia.feeditem =
  item id); the legacy FeedItems.media column is ignored. Further media rows of one
  item, media rows of missing items and items of missing feeds are not imported and are
  listed under 'ignored'.
- CHOICE: The enclosure is the media row's download_url with userinfo removed, then
  normalize_url; NULL or empty means no enclosure.
- CHOICE: pubDate NULL is an undated occurrence; every integer, 0 included, is a date.
- CHOICE: Release groups of one GUID are built in two passes. First, releases are
  created in publication-date order (ties by item ID): the first dated row creates one;
  a later row creates one when it is more than 7 days from every release's date, has an
  enclosure, and an earlier row has a different enclosure more than 7 days away (PROTOCOL §9.7's
  dispute rule); its date is the release's date. Then every row is placed as PROTOCOL §9.7's
  resolver places it: a dated row in the release nearest within 7 days (ties: the
  earlier); any other row in the release holding its enclosure if exactly one does, else
  in the earliest release. A GUID with no dated row is one undated release.
- CHOICE: The earliest release keeps the plain ID (K, GUID); a later release is
  rss-guid+enclosure of its first row's enclosure, or an allocated e0 ID when an earlier
  release already has that ID (identity {k: 'e0', reason: 'release_id_taken',
  source_item_id: its first row}).
- CHOICE: Items without GUID and without enclosure each become their own episode with an
  e0 identity (reason 'no_guid_no_enclosure') and keep their state and statistics.
- CHOICE: The state row is the row with the greatest last_played_time (NULL is 0), ties
  broken by the highest FeedItems.id.
- CHOICE: Play state comes from the state row's read: 1 played, -1 new (Inbox), anything
  else unplayed. Position and duration come from the state row's media (NULL is 0; no
  media: position 0, duration null). Positions (the episode's and the units') are read as
  FeedMediaCursor does (getInt, 32-bit truncation: where Android's player resumes);
  durations and played times keep their 64-bit values, as Android's statistics SQL sums
  them.
- CHOICE: The queue keeps Android's order (Queue.id); an episode with several queued
  rows appears once, at its earliest row. Queue and Favorites rows naming items that are
  not imported (missing, local folder, unused preview, missing feed) are dropped and listed
  under 'ignored'.
- CHOICE: A baseline exists for the state row's media when played_duration,
  last_played_time or playback_completion_date is non-zero; last_played and history are
  null unless > 0. The episode's history (ruling 4) is its baseline's, the state row's
  own.
- CHOICE: Projection: an absent last_played is 0 for date filters (the client rule of
  ruling 7: Android's default filter [0, Long.MAX_VALUE) counts rows whose
  last_played_time is 0); 'episodes' counts episodes plus units (Android counts rows); an
  episode's duration is its state row's; an episode is started when its history (ruling 4)
  is present and its played time > 0.
- CHOICE: Titles in the result lists are custom_title when non-empty, else title; a
  merged episode's title is its state row's.
- CHOICE: Result counts: 'history' counts episodes with a history time (ruling 4);
  'favorites', 'queue' and 'inbox' count episodes.
- CHOICE: Date-filter windows checked: [0, MAX); [start of the UTC month of DBReader's
  oldestDate, MAX) (Android's 'All time' button, which uses the device's time zone);
  every UTC calendar month holding a positive last_played_time. Any bounds test the same
  filter.
"""

import calendar
import hashlib
import json
import os
import pathlib
import re
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ntp_contract as nc  # noqa: E402  (contract-v1: normalize_url, guid_trim)
from rules import strip_userinfo  # noqa: E402 (PROTOCOL §9.4: identity values without URL userinfo)

FORMAT = "nottennapod-import-android-v1"
SUPPORTED_USER_VERSION = 3110000
LOCAL_FOLDER_PREFIX = "antennapod_local:"  # Feed.PREFIX_LOCAL_FOLDER, Feed.isLocalFeed()
RELEASE_WINDOW_MS = 7 * 86400000  # PROTOCOL §9.7: "within 7 days" / "more than 7 days"
JAVA_LONG_MAX = 9223372036854775807  # Long.MAX_VALUE, the default "to" of Android's filter

# Feed.STATE_*, FeedItem.NEW/UNPLAYED/PLAYED, FeedPreferences.NewEpisodesAction codes.
STATE_SUBSCRIBED, STATE_NOT_SUBSCRIBED, STATE_ARCHIVED = 0, 1, 2
READ_NEW, READ_UNPLAYED, READ_PLAYED = -1, 0, 1
STATE_NAMES = {STATE_SUBSCRIBED: "subscribed", STATE_NOT_SUBSCRIBED: "not_subscribed",
               STATE_ARCHIVED: "archived"}
ACTION_NAMES = {0: "global", 1: "inbox", 2: "nothing", 3: "queue"}

# Every "- CHOICE:" paragraph of the module docstring, one string each.
CHOICES = [" ".join(p.split())[len("- CHOICE: "):]
           for p in re.split(r"\n(?=- )", __doc__[__doc__.index("- CHOICE:"):].strip())]


class UnsupportedFile(Exception):
    """The file is not an Android AntennaPod 3.12.1 database this reference reads."""


# ---------------------------------------------------------------------------
# Small value helpers
# ---------------------------------------------------------------------------

def _int(v, default=0):
    """An SQLite integer column as Android's Cursor.getInt/getLong reads it (NULL is 0)."""
    if v is None:
        return default
    if isinstance(v, (bytes, bytearray)):
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default


def _java_int(v):
    """Cursor.getInt: the column's 64-bit value truncated to a signed 32-bit int."""
    return ((_int(v) + 2**31) % 2**32) - 2**31


def _positive(v):
    """A time column that means "set" only when > 0 (Android's `> 0` tests); else None."""
    n = _int(v)
    return n if n > 0 else None


_AUTHORITY_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://([^/?#]*)")


def split_userinfo(url):
    """(url without its userinfo, userinfo or None). Only an authority's `user[:pw]@` is
    removed; everything else stays byte for byte."""
    m = _AUTHORITY_RE.match(url)
    if m is None:
        return url, None
    auth = m.group(1)
    at = auth.rfind("@")
    if at < 0:
        return url, None
    return url[: m.start(1)] + auth[at + 1:] + url[m.end(1):], auth[:at]


def normalized_enclosure(raw):
    """CHOICE: userinfo removed by the fetch's one rule (`strip_userinfo`, PROTOCOL §9.4, §9.7),
    so an imported episode derives the enclosure its feed's fetch derives; then
    contract-v1 normalization. None when the media row has no URL."""
    if not isinstance(raw, str) or raw == "":
        return None
    return nc.normalize_url(strip_userinfo(raw))


def feed_title(feed):
    ct = feed.get("custom_title")
    return ct if isinstance(ct, str) and ct != "" else (feed.get("title") or "")


def is_local_feed(feed):
    url = feed.get("download_url")
    return isinstance(url, str) and url.startswith(LOCAL_FOLDER_PREFIX)


def month_of(ms):
    """UTC (year, month) as SQLite's strftime('%Y'/'%m', datetime(ms/1000, 'unixepoch'))."""
    secs = ms // 1000 if ms >= 0 else -((-ms) // 1000)  # SQLite integer division truncates
    y, mo = time.gmtime(secs)[:2]
    return y, mo


def month_start_ms(year, month):
    return calendar.timegm((year, month, 1, 0, 0, 0)) * 1000


def next_month(year, month):
    return (year + 1, 1) if month == 12 else (year, month + 1)


# ---------------------------------------------------------------------------
# Reading the file
# ---------------------------------------------------------------------------

def open_readonly(path):
    uri = pathlib.Path(os.path.abspath(path)).as_uri() + "?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table(conn, name):
    """All rows of a table, columns by name, in id order."""
    return [dict(r) for r in conn.execute("SELECT * FROM %s ORDER BY id" % name)]


def read_library(path):
    """Reads a 3.12.1 backup into plain rows. Raises UnsupportedFile for another version."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        head = f.read(16)
        digest.update(head)
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    if head != b"SQLite format 3\x00":
        raise UnsupportedFile("not an SQLite database")
    conn = open_readonly(path)
    try:
        uv = conn.execute("PRAGMA user_version").fetchone()[0]
        if uv != SUPPORTED_USER_VERSION:
            raise UnsupportedFile("user_version %r is not 3110000" % (uv,))
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise UnsupportedFile("integrity_check failed")
        lib = {
            "user_version": uv,
            "sha256": digest.hexdigest(),
            "feeds": _table(conn, "Feeds"),
            "items": _table(conn, "FeedItems"),
            "media": _table(conn, "FeedMedia"),
            "queue": _table(conn, "Queue"),
            "favorites": _table(conn, "Favorites"),
        }
    except sqlite3.DatabaseError as e:
        raise UnsupportedFile("unreadable: %s" % e)
    finally:
        conn.close()
    return lib


# ---------------------------------------------------------------------------
# The expected import
# ---------------------------------------------------------------------------

def _state_code(feed):
    return _int(feed.get("state"), STATE_SUBSCRIBED)


def state_name(feed):
    """Feeds.state 0, 1, 2 -> subscribed, not_subscribed (preview), archived (PROTOCOL §11.5). NULL is 0,
    as FeedCursor's getInt reads it. CHOICE: any other value is subscribed."""
    return STATE_NAMES.get(_state_code(feed), "subscribed")


def new_episodes_action(feed):
    """PROTOCOL §11.5. CHOICE: NULL is 0 (global); an unknown code is inbox (NewEpisodesAction.fromCode)."""
    return ACTION_NAMES.get(_int(feed.get("new_episodes_action"), 0), "inbox")


def has_credentials(feed):
    """CHOICE: Android's BasicAuthorizationInterceptor sends credentials when the username
    or the password is non-empty, or when the feed URL carries userinfo."""
    if feed.get("username") or feed.get("password"):
        return True
    url = feed.get("download_url")
    return isinstance(url, str) and bool(split_userinfo(url)[1])


def play_state(item):
    r = _int(item.get("read"), READ_UNPLAYED)
    if r == READ_PLAYED:
        return "played"
    if r == READ_NEW:
        return "new"
    return "unplayed"


def release_groups(occurrences):
    """Release groups (PROTOCOL §9.7) for the import's rows of one GUID in one podcast.

    `occurrences`: [{"id": item id, "u": normalized enclosure or None, "d": pubDate or None}].
    Returns releases in creation order (the first is the earliest and keeps the plain ID):
    [{"pt": publication time or None, "encs": [enclosures in order], "members": [item ids],
      "first": item id of the row that created it}].

    CHOICE (see CHOICES), in two passes so that every row lands where PROTOCOL §9.7's resolver
    (which devices use from then on) puts it:
    1. Releases are created exactly as a fetch's first dispute partitions the same
       evidence (rules._partition): the dated rows with an enclosure, in (date, enclosure, item ID) order;
       the first creates the first release; a later one creates a release when it is more
       than 7 days from every release's publication time and the dispute rule holds for it
       against any other row of the GUID, earlier or later (another row has a different
       enclosure and a date more than 7 days away). Its date is the release's publication
       time and its enclosure the release's first. Without a dated row with an enclosure,
       the first dated row creates the only release; without a dated row, the first row by
       ID creates it, undated.
    2. Every dated row joins the release whose publication time is nearest within 7 days
       (ties: the earlier release). The rest (dated rows no release is within 7 days of, and
       undated rows) join the release holding their enclosure if exactly one does, else the
       earliest release."""
    W = RELEASE_WINDOW_MS
    dated = sorted((o for o in occurrences if o["d"] is not None), key=lambda o: (o["d"], o["u"] or "", o["id"]))
    rels = []

    def new(o, pt):
        rels.append({"pt": pt, "encs": [o["u"]] if o["u"] is not None else [], "members": [],
                     "first": o["id"]})

    for o in (o for o in dated if o["u"] is not None):
        if not rels:
            new(o, o["d"])
        elif (all(abs(o["d"] - r["pt"]) > W for r in rels)
              and any(s["u"] is not None and s["u"] != o["u"] and abs(o["d"] - s["d"]) > W for s in dated)):
            new(o, o["d"])
    if not rels and dated:
        new(dated[0], dated[0]["d"])
    if not rels:
        new(min(occurrences, key=lambda o: o["id"]), None)

    def add(r, o):
        r["members"].append(o["id"])
        if o["u"] is not None and o["u"] not in r["encs"]:
            r["encs"].append(o["u"])

    rest = []
    for o in dated:
        near = [(abs(o["d"] - r["pt"]), i) for i, r in enumerate(rels)
                if r["pt"] is not None and abs(o["d"] - r["pt"]) <= W]
        if near:
            add(rels[min(near)[1]], o)
        else:
            rest.append(o)
    rest += sorted((o for o in occurrences if o["d"] is None), key=lambda o: o["id"])
    for o in rest:
        holding = [r for r in rels if o["u"] is not None and o["u"] in r["encs"]]
        add(holding[0] if len(holding) == 1 else rels[0], o)
    for r in rels:
        r["members"].sort()
    return rels


def _has_user_state(item, media, favorite_items, queue_items):
    """Ruling 3: a row holds user state when it is played, NEW, favorite or queued, or its
    media has a position, history, last played or played time."""
    if _int(item.get("read"), READ_UNPLAYED) in (READ_PLAYED, READ_NEW):
        return True
    if item["id"] in favorite_items or item["id"] in queue_items:
        return True
    if media is None:
        return False
    return (_int(media.get("position")) > 0 or _positive(media.get("playback_completion_date")) is not None
            or _positive(media.get("last_played_time")) is not None or _int(media.get("played_duration")) != 0)


def plan_import(lib):
    """The expected import of a library read by `read_library` (or built by hand).

    Podcasts (PROTOCOL §11.5): feeds grouped by normalized URL; local folders and unused
    previews (ruling 3) skipped. CHOICE: URL-less feeds skipped; the unused-preview rule
    applies per feed before grouping; userinfo removed before grouping; K from the creating
    feed's type. Ruling 2: properties from the lowest-ID non-preview feed, credentials from
    it or else the first feed that has any. Episodes (PROTOCOL §3.3): GUID, then release
    groups; else the normalized enclosure; else alone with an e0 identity (CHOICE: keeping
    its state). CHOICE: each item uses its lowest-id media row; rows that cannot be placed,
    and queue and favorite rows naming them, are listed under 'ignored'; the queue keeps
    Android's order with one entry per episode at its earliest row."""
    feeds = {f["id"]: f for f in lib["feeds"]}
    items = {it["id"]: it for it in lib["items"]}

    # Each item's media: the lowest-id FeedMedia row naming it (Android's LEFT JOIN).
    media_by_item, extra_media, orphan_media = {}, [], []
    for m in sorted(lib["media"], key=lambda m: m["id"]):
        fi = m.get("feeditem")
        if fi not in items:
            orphan_media.append(m["id"])
        elif fi in media_by_item:
            extra_media.append(m["id"])
        else:
            media_by_item[fi] = m

    favorite_items = set(row.get("feeditem") for row in lib["favorites"])
    queue_rows = sorted(lib["queue"], key=lambda q: q["id"])
    queue_rank = {}
    for q in queue_rows:
        queue_rank.setdefault(q.get("feeditem"), q["id"])

    skipped, ambiguous, merged = [], [], []
    ignored = {"orphan_items": [], "orphan_media": orphan_media, "extra_media": extra_media,
               "dropped_queue_rows": [], "dropped_favorite_rows": []}

    items_by_feed = {}
    for iid in sorted(items):
        fd = items[iid].get("feed")
        if fd not in feeds:
            ignored["orphan_items"].append(iid)
            continue
        items_by_feed.setdefault(fd, []).append(iid)

    # Podcasts: feeds grouped by normalized URL; local folders, URL-less feeds and unused
    # previews skipped.
    groups, order = {}, []
    for fid in sorted(feeds):
        f = feeds[fid]
        url = f.get("download_url")
        reason = None
        if is_local_feed(f):
            reason = "local_folder"
        elif not isinstance(url, str) or url == "":
            reason = "no_feed_url"
        elif _state_code(f) == STATE_NOT_SUBSCRIBED and not any(
                _has_user_state(items[i], media_by_item.get(i), favorite_items, queue_rank)
                for i in items_by_feed.get(fid, ())):
            reason = "unused_preview"
        if reason is not None:
            skipped.append({"reason": reason, "title": feed_title(f), "source_feed_id": fid})
            continue
        key = nc.normalize_url(strip_userinfo(url))
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(fid)

    podcasts = []
    item_episode = {}
    for key in order:
        fids = groups[key]
        creating = feeds[fids[0]]
        listed = [fid for fid in fids if _state_code(feeds[fid]) != STATE_NOT_SUBSCRIBED]
        props_id = listed[0] if listed else fids[0]
        props = feeds[props_id]
        with_credentials = [fid for fid in fids if has_credentials(feeds[fid])]
        cred_id = props_id if props_id in with_credentials else (with_credentials[0] if with_credentials else None)
        kfam = "atom-id" if creating.get("type") == "atom" else "rss-guid"
        podcast = {
            "creating_feed_id": fids[0],
            "source_feed_ids": list(fids),
            "properties_feed_id": props_id,
            "title": feed_title(creating),
            "url": strip_userinfo(creating["download_url"]),
            "normalized_url": key,
            "k": kfam,
            "state": state_name(props),
            "keep_updated": _int(props.get("keep_updated")) > 0,
            "new_episodes_action": new_episodes_action(props),
            "credentials": cred_id is not None,
            "credentials_feed_id": cred_id,
            "episodes": [],
        }
        if len(fids) > 1:
            merged.append({"kind": "podcast", "title": podcast["title"], "source_feed_ids": list(fids)})

        pitems = sorted(i for fid in fids for i in items_by_feed.get(fid, ()))
        by_guid, by_enc, alone = {}, {}, []
        for iid in pitems:
            it = items[iid]
            # PROTOCOL §9.4: identity values lose URL userinfo before derivation, as a fetch's do.
            g = nc.guid_trim(strip_userinfo(it["item_identifier"])) if isinstance(it.get("item_identifier"), str) else None
            m = media_by_item.get(iid)
            u = normalized_enclosure(m.get("download_url")) if m is not None else None
            if g is not None:
                d = it.get("pubDate")
                by_guid.setdefault(g, []).append({"id": iid, "u": u, "d": None if d is None else _int(d)})
            elif u is not None:
                by_enc.setdefault(u, []).append(iid)
            else:
                alone.append(iid)

        groups_out = []  # (members, identity)
        for g, occs in by_guid.items():
            rels = release_groups(occs)
            taken = []
            for idx, r in enumerate(rels):
                if idx == 0:
                    ident = {"k": kfam, "v": g}
                else:
                    ident = {"k": "rss-guid+enclosure", "v": nc.episode_v_guid_enclosure(g, r["encs"][0])}
                    if ident in taken:
                        ident = {"k": "e0", "reason": "release_id_taken", "source_item_id": r["first"]}
                taken.append(ident)
                groups_out.append((r["members"], ident))
        for u, ids in by_enc.items():
            groups_out.append((sorted(ids), {"k": "enclosure", "v": u}))
        for iid in alone:
            groups_out.append(([iid], {"k": "e0", "reason": "no_guid_no_enclosure", "source_item_id": iid}))
            ambiguous.append({"reason": "no_guid_no_enclosure", "podcast": podcast["title"],
                              "title": items[iid].get("title") or "", "source_item_id": iid})
        groups_out.sort(key=lambda t: t[0][0])

        for members, ident in groups_out:
            ep = _episode(members, ident, items, media_by_item, favorite_items, queue_rank)
            podcast["episodes"].append(ep)
            for iid in members:
                item_episode[iid] = ep
            if len(members) > 1:
                merged.append({"kind": "episode", "podcast": podcast["title"], "title": ep["title"],
                               "source_item_ids": list(members)})
        podcast["populated"] = bool(podcast["episodes"])
        podcasts.append(podcast)

    # Queue: Android's order (Queue.id), one entry per episode at its earliest row.
    queue, queued_eps = [], set()
    for q in queue_rows:
        iid = q.get("feeditem")
        ep = item_episode.get(iid)
        if ep is None:
            ignored["dropped_queue_rows"].append(q["id"])
            continue
        if id(ep) in queued_eps:
            continue
        queued_eps.add(id(ep))
        queue.append(iid)
    for row in sorted(lib["favorites"], key=lambda r: r["id"]):
        if row.get("feeditem") not in item_episode:
            ignored["dropped_favorite_rows"].append(row["id"])

    episodes = [ep for p in podcasts for ep in p["episodes"]]
    result = {
        "imported": {
            "podcasts": len(podcasts),
            "episodes": len(episodes),
            "queue": len(queue),
            "favorites": sum(1 for ep in episodes if ep["favorite"]),
            "history": sum(1 for ep in episodes if ep["history"] is not None),
            "inbox": sum(1 for ep in episodes if ep["inbox"]),
        },
        "skipped": skipped,
        "ambiguous": ambiguous,
        "merged": merged,
    }
    return {"podcasts": podcasts, "queue": queue, "result": result, "ignored": ignored}


def _episode(members, ident, items, media_by_item, favorite_items, queue_rank):
    """One episode from its source rows (PROTOCOL §11.5, rulings 1 and 4). CHOICE: the state
    row is the greatest last_played_time (NULL 0), then the highest item ID; play state,
    position, duration and history are its own; its baseline exists only when one of its
    three values is non-zero. Every other row with media is a unit carrying its own values,
    PLAYED flag and position."""
    def lpt(iid):
        m = media_by_item.get(iid)
        return _int(m.get("last_played_time")) if m is not None else 0

    state_id = max(members, key=lambda iid: (lpt(iid), iid))
    st = items[state_id]
    sm = media_by_item.get(state_id)
    baseline = None
    if sm is not None:
        b = {"source_item_id": state_id, "source_media_id": sm["id"],
             "played_ms": _int(sm.get("played_duration")),
             "last_played": _positive(sm.get("last_played_time")),
             "history": _positive(sm.get("playback_completion_date"))}
        if b["played_ms"] != 0 or b["last_played"] is not None or b["history"] is not None:
            baseline = b
    units = []
    for iid in members:
        m = media_by_item.get(iid)
        if iid == state_id or m is None:
            continue
        units.append({"source_item_id": iid, "source_media_id": m["id"],
                      "played_ms": _int(m.get("played_duration")),
                      "last_played": _positive(m.get("last_played_time")),
                      "history": _positive(m.get("playback_completion_date")),
                      "duration_ms": _int(m.get("duration")),
                      "played": _int(items[iid].get("read"), READ_UNPLAYED) == READ_PLAYED,
                      "position_ms": _java_int(m.get("position"))})
    ps = play_state(st)
    return {
        "source_item_ids": list(members),
        "identity": ident,
        "title": st.get("title") or "",
        "state_item_id": state_id,
        "play_state": ps,
        "inbox": ps == "new",
        "position_ms": _java_int(sm.get("position")) if sm is not None else 0,
        "favorite": any(i in favorite_items for i in members),
        "queued": any(i in queue_rank for i in members),
        "duration_ms": _int(sm.get("duration")) if sm is not None else None,
        "history": baseline["history"] if baseline else None,
        "baseline": baseline,
        "units": units,
    }


# ---------------------------------------------------------------------------
# Android's numbers: its own statistics SQL (PodDBAdapter, tag 3.12.1)
# ---------------------------------------------------------------------------

def android_feed_statistics_sql(include_marked, t_from, t_to, six_months_ago=0):
    """`PodDBAdapter.getFeedStatisticsCursor`, transcribed term by term. The only change is
    the selected feed columns: `KEYS_FEED` (every Feeds column) becomes `Feeds.id AS
    feed_id`, which leaves the aggregates untouched."""
    last_played = "FeedMedia.last_played_time"
    was_started = "FeedMedia.playback_completion_date > 0 AND FeedMedia.played_duration > 0"
    if include_marked:
        was_started = "(" + was_started + ") OR FeedItems.read=1 OR FeedMedia.position> 0"
    time_filter = last_played + ">=" + str(t_from) + " AND " + last_played + "<" + str(t_to)
    played_time = "FeedMedia.played_duration"
    if include_marked:
        played_time = ("(CASE WHEN " + played_time + " != 0 THEN " + played_time + " ELSE ("
                       "CASE WHEN FeedItems.read=1 THEN FeedMedia.duration ELSE 0 END) END)")
    return ("SELECT Feeds.id AS feed_id, "
            "COUNT(*) AS num_episodes, "
            "MIN(CASE WHEN " + last_played + " > 0 THEN " + last_played
            + " ELSE " + str(JAVA_LONG_MAX) + " END) AS oldest_date, "
            "SUM(CASE WHEN (" + was_started + ") THEN 1 ELSE 0 END) AS episodes_started, "
            "IFNULL(SUM(CASE WHEN (" + time_filter + ") THEN (" + played_time + ") ELSE 0 END), 0) AS played_time, "
            "IFNULL(SUM(FeedMedia.duration), 0) AS total_time, "
            "SUM(CASE WHEN FeedMedia.downloaded > 0 THEN 1 ELSE 0 END) AS num_downloaded, "
            "SUM(CASE WHEN FeedMedia.downloaded > 0 THEN FeedMedia.filesize ELSE 0 END) AS download_size, "
            "SUM(CASE WHEN FeedItems.read != 1 AND FeedItems.pubDate >= " + str(six_months_ago)
            + " THEN 1 ELSE 0 END) AS num_recent_unplayed "
            " FROM FeedItems"
            " LEFT JOIN FeedMedia ON FeedItems.id=FeedMedia.feeditem "
            " INNER JOIN Feeds ON FeedItems.feed=Feeds.id"
            " WHERE Feeds.state!=1"
            " GROUP BY Feeds.id")


ANDROID_MONTHLY_SQL = (
    "SELECT SUM(played_duration) AS total_duration"
    ", strftime('%m', datetime(last_played_time/1000, 'unixepoch')) AS month"
    ", strftime('%Y', datetime(last_played_time/1000, 'unixepoch')) AS year"
    " FROM FeedMedia"
    " WHERE last_played_time > 0 AND played_duration > 0"
    " GROUP BY year, month"
    " ORDER BY year, month")

ANDROID_FEED_STATISTICS_SQL = android_feed_statistics_sql(False, 0, JAVA_LONG_MAX)


def _windows(oldest_date, months):
    """CHOICE: [0, MAX); [start of the UTC month of DBReader's oldestDate, MAX), what Android's
    "All time" button selects (in the device's time zone); every listed UTC month."""
    wins = [(0, JAVA_LONG_MAX)]
    if oldest_date is not None:
        y, m = month_of(oldest_date)
        wins.append((month_start_ms(y, m), JAVA_LONG_MAX))
    for y, m in months:
        ny, nm = next_month(y, m)
        wins.append((month_start_ms(y, m), month_start_ms(ny, nm)))
    out = []
    for w in wins:
        if w not in out:
            out.append(w)
    return out


def _json_to(t):
    return None if t == JAVA_LONG_MAX else t


def android_numbers(path, adjust=(), windows=None):
    """Runs Android's statistics SQL on the file. Per feed (state != 1): the all-time
    numbers without and with "include marked as played", and the played time per window;
    per month: Android's monthly cursor, parsed as DBReader.getMonthlyTimeStatistics does.

    `adjust` (SQL statements) runs first on an in-memory copy, for attributing differences;
    `windows` ([(from, to or None)]) replaces the windows derived from the file."""
    conn = open_readonly(path)
    if adjust:
        mem = sqlite3.connect(":memory:")
        conn.backup(mem)
        conn.close()
        conn = mem
        conn.row_factory = sqlite3.Row
        for stmt in adjust:
            conn.execute(stmt)
    try:
        def feed_rows(include_marked, t_from, t_to):
            out = {}
            for r in conn.execute(android_feed_statistics_sql(include_marked, t_from, t_to)):
                out[r["feed_id"]] = {"oldest_date": _int(r["oldest_date"]),
                                     "num_episodes": _int(r["num_episodes"]),
                                     "episodes_started": _int(r["episodes_started"]),
                                     "played_time": _int(r["played_time"]),
                                     "total_time": _int(r["total_time"])}
            return out

        plain = feed_rows(False, 0, JAVA_LONG_MAX)
        marked = feed_rows(True, 0, JAVA_LONG_MAX)
        per_feed = []
        for fid in sorted(plain):
            p, k = plain[fid], marked[fid]
            per_feed.append({"feed_id": fid,
                             "played_ms": p["played_time"],
                             "played_marked_ms": k["played_time"],
                             "episodes_started": p["episodes_started"],
                             "episodes_started_marked": k["episodes_started"],
                             "episodes": p["num_episodes"],
                             "total_ms": p["total_time"]})
        monthly = []
        for r in conn.execute(ANDROID_MONTHLY_SQL):
            monthly.append({"year": int(r["year"]), "month": int(r["month"]),
                            "played_ms": _int(r["total_duration"])})
        if windows is None:
            # DBReader.getStatistics: oldestDate is the least oldest_date of a feed with episodes.
            oldest = [r["oldest_date"] for r in plain.values()
                      if r["num_episodes"] > 0 and r["oldest_date"] < JAVA_LONG_MAX]
            lp_months = sorted(set(month_of(_int(r[0])) for r in conn.execute(
                "SELECT last_played_time FROM FeedMedia WHERE last_played_time > 0")))
            windows = [(f, _json_to(t)) for f, t in _windows(min(oldest) if oldest else None, lp_months)]
        out_windows = []
        for t_from, t_to in windows:
            rows = feed_rows(False, t_from, JAVA_LONG_MAX if t_to is None else t_to)
            out_windows.append({"from": t_from, "to": t_to,
                                "per_feed": [{"feed_id": fid, "played_ms": rows[fid]["played_time"]}
                                             for fid in sorted(rows)]})
    finally:
        conn.close()
    return {"per_feed": per_feed, "windows": out_windows, "monthly": monthly}


# ---------------------------------------------------------------------------
# The projection (PROTOCOL §8) from the expected episodes and units
# ---------------------------------------------------------------------------

KEYS = ("played_ms", "played_marked_ms", "episodes_started", "episodes_started_marked", "episodes", "total_ms")


def _in_window(last_played, t_from, t_to):
    lp = 0 if last_played is None else last_played  # CHOICE (ruling 7's client rule): absent is 0
    return t_from <= lp < t_to


def _stat_parts(ep):
    """An episode's statistical parts: the episode itself (its baseline's played time and
    last played, its history, duration, play state and position) and each unit."""
    b = ep["baseline"] or {"played_ms": 0, "last_played": None}
    parts = [{"played_ms": b["played_ms"], "last_played": b["last_played"], "history": ep["history"],
              "duration_ms": ep["duration_ms"] or 0, "played": ep["play_state"] == "played",
              "position_ms": ep["position_ms"]}]
    return parts + ep["units"]


def project(plan, windows):
    """The client projection (PROTOCOL §8.8) over the expected import, per podcast for subscribed and
    archived podcasts and per month for all: each episode counts with its baseline's played
    time by its last_played, its history (ruling 4), duration, play state and position; each
    unit with its own values (ruling 1). CHOICE: an absent last_played is 0 in date filters;
    'episodes' counts episodes plus units."""
    per_podcast = []
    listed = [p for p in plan["podcasts"] if p["state"] in ("subscribed", "archived")]
    for p in listed:
        n = dict({"creating_feed_id": p["creating_feed_id"]}, **{k: 0 for k in KEYS})
        for ep in p["episodes"]:
            for x in _stat_parts(ep):
                n["episodes"] += 1
                n["total_ms"] += x["duration_ms"]
                if _in_window(x["last_played"], 0, JAVA_LONG_MAX):
                    n["played_ms"] += x["played_ms"]
                    n["played_marked_ms"] += (x["played_ms"] if x["played_ms"] != 0
                                              else (x["duration_ms"] if x["played"] else 0))
                started = x["history"] is not None and x["played_ms"] > 0
                n["episodes_started"] += 1 if started else 0
                n["episodes_started_marked"] += 1 if (started or x["played"] or x["position_ms"] > 0) else 0
        per_podcast.append(n)

    out_windows = []
    for w in windows:
        t_from = w["from"]
        t_to = JAVA_LONG_MAX if w["to"] is None else w["to"]
        rows = []
        for p in listed:
            s = sum(x["played_ms"] for ep in p["episodes"] for x in _stat_parts(ep)
                    if _in_window(x["last_played"], t_from, t_to))
            rows.append({"creating_feed_id": p["creating_feed_id"], "played_ms": s})
        out_windows.append({"from": t_from, "to": w["to"], "per_podcast": rows})

    months = {}
    for p in plan["podcasts"]:
        for ep in p["episodes"]:
            for x in _stat_parts(ep):
                if x["last_played"] is not None and x["last_played"] > 0 and x["played_ms"] > 0:
                    ym = month_of(x["last_played"])
                    months[ym] = months.get(ym, 0) + x["played_ms"]
    monthly = [{"year": y, "month": m, "played_ms": months[(y, m)]} for (y, m) in sorted(months)]
    return {"per_podcast": per_podcast, "windows": out_windows, "monthly": monthly}


def android_per_podcast(plan, android):
    """Android's per-feed rows summed per imported podcast (its source feeds' rows; preview
    feeds have none). Rows of feeds that are not imported stay per feed."""
    feed_to_podcast = {}
    for p in plan["podcasts"]:
        for fid in p["source_feed_ids"]:
            feed_to_podcast[fid] = p["creating_feed_id"]
    per_podcast, not_imported = {}, []
    for row in android["per_feed"]:
        pid = feed_to_podcast.get(row["feed_id"])
        if pid is None:
            not_imported.append(dict(row))
            continue
        acc = per_podcast.setdefault(pid, dict({"creating_feed_id": pid}, **{k: 0 for k in KEYS}))
        for k in KEYS:
            acc[k] += row[k]
    windows = []
    for w in android["windows"]:
        acc = {}
        for r in w["per_feed"]:
            pid = feed_to_podcast.get(r["feed_id"])
            if pid is not None:
                acc[pid] = acc.get(pid, 0) + r["played_ms"]
        windows.append({"from": w["from"], "to": w["to"],
                        "per_podcast": [{"creating_feed_id": pid, "played_ms": acc[pid]} for pid in sorted(acc)]})
    return [per_podcast[k] for k in sorted(per_podcast)], not_imported, windows


def entry_key(e):
    """The identity of a difference: its metric and where (podcast, feed, window, month)."""
    m = e["metric"]
    if m == "per_feed_not_imported":
        return (m, e["feed_id"])
    if m == "monthly.played_ms":
        return (m, e["month"])
    if m == "window.played_ms":
        return (m, e["from"], e["to"], e["podcast"])
    return (m, e["podcast"])


def metric_values(plan, android):
    """Android's value of every metric, keyed as `entry_key` keys differences."""
    a_pp, not_imported, a_windows = android_per_podcast(plan, android)
    v = {}
    for r in a_pp:
        for k in KEYS:
            v[("per_podcast." + k, r["creating_feed_id"])] = r[k]
    for row in not_imported:
        v[("per_feed_not_imported", row["feed_id"])] = json.dumps({k: row[k] for k in KEYS if row[k]}, sort_keys=True)
    for w in a_windows:
        for r in w["per_podcast"]:
            v[("window.played_ms", w["from"], w["to"], r["creating_feed_id"])] = r["played_ms"]
    for r in android["monthly"]:
        v[("monthly.played_ms", "%04d-%02d" % (r["year"], r["month"]))] = r["played_ms"]
    return v


def compare(plan, android, projection):
    """Every difference between Android's numbers and the projection."""
    a_pp, not_imported, a_windows = android_per_podcast(plan, android)
    diffs = []
    zero = {k: 0 for k in KEYS}
    a_by = {r["creating_feed_id"]: r for r in a_pp}
    p_by = {r["creating_feed_id"]: r for r in projection["per_podcast"]}
    for pid in sorted(set(a_by) | set(p_by)):
        a, p = a_by.get(pid, zero), p_by.get(pid, zero)
        for k in KEYS:
            if a[k] != p[k]:
                diffs.append({"metric": "per_podcast." + k, "podcast": pid, "android": a[k], "projection": p[k]})
    for row in not_imported:
        nonzero = {k: row[k] for k in KEYS if row[k] != 0}
        diffs.append({"metric": "per_feed_not_imported", "feed_id": row["feed_id"], "android": nonzero,
                      "projection": None})
    for aw, pw in zip(a_windows, projection["windows"]):
        a = {r["creating_feed_id"]: r["played_ms"] for r in aw["per_podcast"]}
        p = {r["creating_feed_id"]: r["played_ms"] for r in pw["per_podcast"]}
        for pid in sorted(set(a) | set(p)):
            if a.get(pid, 0) != p.get(pid, 0):
                diffs.append({"metric": "window.played_ms", "from": aw["from"], "to": aw["to"], "podcast": pid,
                              "android": a.get(pid, 0), "projection": p.get(pid, 0)})
    am = {(r["year"], r["month"]): r["played_ms"] for r in android["monthly"]}
    pm = {(r["year"], r["month"]): r["played_ms"] for r in projection["monthly"]}
    for ym in sorted(set(am) | set(pm)):
        if am.get(ym, 0) != pm.get(ym, 0):
            diffs.append({"metric": "monthly.played_ms", "month": "%04d-%02d" % ym,
                          "android": am.get(ym, 0), "projection": pm.get(ym, 0)})
    return a_pp, a_windows, diffs


def _ids(xs):
    return ",".join(str(int(x)) for x in sorted(xs))


def detect_causes(lib, plan):
    """The conditions in the file that make Android's numbers differ from the projection,
    all accepted by the rulings. Each names its source rows and carries `_adjust`: SQL that
    removes its effect from a scratch copy of the file (absent for a note that causes no
    difference)."""
    feeds = {f["id"]: f for f in lib["feeds"]}
    items = {it["id"]: it for it in lib["items"]}
    media_rows = {m["id"]: m for m in lib["media"]}
    media_by_item = {}
    for m in sorted(lib["media"], key=lambda m: m["id"]):
        if m.get("feeditem") in items:
            media_by_item.setdefault(m.get("feeditem"), m)
    causes = []

    by_reason = {}
    for sk in plan["result"]["skipped"]:
        by_reason.setdefault(sk["reason"], []).append(sk["source_feed_id"])
    for reason in sorted(by_reason):
        fids = by_reason[reason]
        rows = sorted(iid for iid, it in items.items() if it.get("feed") in fids)
        if rows:
            causes.append({"cause": "skipped_" + reason,
                           "detail": "a skipped feed's rows count in Android's statistics (per feed when its state "
                                     "is not 1, per month always) and nowhere after the import",
                           "source_feed_ids": sorted(fids), "source_item_ids": rows,
                           "_adjust": ["DELETE FROM FeedMedia WHERE feeditem IN (%s)" % _ids(rows),
                                       "DELETE FROM FeedItems WHERE id IN (%s)" % _ids(rows)]})

    joined = sorted(fid for p in plan["podcasts"] if p["state"] in ("subscribed", "archived")
                    for fid in p["source_feed_ids"] if _state_code(feeds[fid]) == STATE_NOT_SUBSCRIBED)
    if joined:
        causes.append({"cause": "preview_feed_joins_listed_podcast",
                       "detail": "Android's per-feed statistics leave out preview feeds; after the merge their rows "
                                 "count in the subscribed or archived podcast (ruling 2)",
                       "source_feed_ids": joined,
                       "_adjust": ["UPDATE Feeds SET state=0 WHERE id IN (%s)" % _ids(joined)]})

    no_media, absent_lp = [], []
    for p in plan["podcasts"]:
        for ep in p["episodes"]:
            no_media += [iid for iid in ep["source_item_ids"]
                         if iid != ep["state_item_id"] and iid not in media_by_item]
            b = ep["baseline"]
            for x in ([b] if b else []) + ep["units"]:
                if x["played_ms"] != 0 and x["last_played"] is None:
                    absent_lp.append((x["source_media_id"], x["played_ms"]))
    if no_media:
        causes.append({"cause": "merged_row_without_media",
                       "detail": "a merged row without a media row makes no unit (ruling 1); Android counts it as an "
                                 "episode, and as started with 'include marked as played' when it is PLAYED",
                       "source_item_ids": sorted(no_media),
                       "_adjust": ["DELETE FROM FeedItems WHERE id IN (%s)" % _ids(no_media)]})
    if absent_lp:
        causes.append({"cause": "absent_last_played_client_rule",
                       "detail": "played time without a last-played date (Android's last_played_time 0, e.g. from "
                                 "before 1.4) counts in date filters starting at 0 only through the client rule that "
                                 "an absent last_played is 0 (ruling 7); never in 'All time' or per month",
                       "source_media_ids": sorted(m for m, _ in absent_lp),
                       "played_ms": sum(v for _, v in absent_lp)})
    if plan["ignored"]["orphan_media"] or plan["ignored"]["extra_media"]:
        mids = plan["ignored"]["orphan_media"] + plan["ignored"]["extra_media"]
        causes.append({"cause": "media_not_imported",
                       "detail": "media rows of missing items (Android's monthly statistics count them) or second "
                                 "media rows of one item (Android's LEFT JOIN counts each)",
                       "source_media_ids": sorted(mids),
                       "_adjust": ["DELETE FROM FeedMedia WHERE id IN (%s)" % _ids(mids)]})
    if plan["ignored"]["orphan_items"]:
        oi = plan["ignored"]["orphan_items"]
        causes.append({"cause": "items_without_feed",
                       "detail": "items of a missing feed are not imported; Android's monthly statistics count "
                                 "their media",
                       "source_item_ids": sorted(oi),
                       "_adjust": ["DELETE FROM FeedMedia WHERE feeditem IN (%s)" % _ids(oi)]})
    null_lp = [mid for mid, m in media_rows.items()
               if m.get("last_played_time") is None and _int(m.get("played_duration")) != 0]
    if null_lp:
        causes.append({"cause": "null_last_played_time",
                       "detail": "Android's date filter never matches NULL; the import reads it as 0",
                       "source_media_ids": sorted(null_lp),
                       "_adjust": ["UPDATE FeedMedia SET last_played_time=0 WHERE id IN (%s)" % _ids(null_lp)]})
    return causes


def attribute(path, plan, android, projection, diffs, causes):
    """Splits the differences: those that vanish when Android's SQL runs on a copy with
    every cause adjusted are expected (each listing the causes whose own adjustment changes
    it); the rest, and any the adjustments create, are mismatches."""
    windows = [(w["from"], w["to"]) for w in android["windows"]]
    adjusting = [c for c in causes if c.get("_adjust")]
    everything = [stmt for c in adjusting for stmt in c["_adjust"]]
    residual = compare(plan, android_numbers(path, everything, windows), projection)[2] if everything else diffs
    residual_keys = set(entry_key(e) for e in residual)
    base = metric_values(plan, android)
    by_cause = {c["cause"]: metric_values(plan, android_numbers(path, c["_adjust"], windows)) for c in adjusting}
    expected, mismatches = [], []
    for e in diffs:
        k = entry_key(e)
        e = dict(e, causes=sorted(name for name, vals in by_cause.items() if vals.get(k) != base.get(k)))
        (mismatches if k in residual_keys else expected).append(e)
    seen = set(entry_key(e) for e in diffs)
    mismatches += [dict(e, causes=["introduced_by_adjustment"]) for e in residual if entry_key(e) not in seen]
    return expected, mismatches


# ---------------------------------------------------------------------------
# Everything for one file
# ---------------------------------------------------------------------------

def analyze(path, name=None):
    """The complete expected import and statistics comparison for one backup file."""
    lib = read_library(path)
    plan = plan_import(lib)
    android = android_numbers(path)
    projection = project(plan, android["windows"])
    a_pp, a_windows, diffs = compare(plan, android, projection)
    causes = detect_causes(lib, plan)
    expected, mismatches = attribute(path, plan, android, projection, diffs, causes)
    return {
        "format": FORMAT,
        "fixture": name or os.path.basename(path),
        "sha256": lib["sha256"],
        "user_version": lib["user_version"],
        "result": plan["result"],
        "podcasts": plan["podcasts"],
        "queue": plan["queue"],
        "ignored": plan["ignored"],
        "statistics": {
            "android": {"per_feed": android["per_feed"], "per_podcast": a_pp, "windows": a_windows,
                        "monthly": android["monthly"]},
            "projection": projection,
            "mismatches": mismatches,
            "expected_differences": expected,
            "causes": [{k: v for k, v in c.items() if k != "_adjust"} for c in causes],
        },
    }


def dumps(obj):
    return json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


# ---------------------------------------------------------------------------
# Writing a 3.12.1 file (for generated fixtures)
# ---------------------------------------------------------------------------

# The exact DDL of an AntennaPod 3.12.1 database (sqlite_master of the emulator libraries
# in internal/importer/androiddb/testdata/android-3.12.1, in creation order; sqlite_sequence
# is created by SQLite itself with the first AUTOINCREMENT table).
SCHEMA_3_12_1 = [
    "CREATE TABLE android_metadata (locale TEXT)",
    "CREATE TABLE Feeds (id INTEGER PRIMARY KEY AUTOINCREMENT ,title TEXT,custom_title TEXT,file_url TEXT,"
    "download_url TEXT,downloaded INTEGER,link TEXT,description TEXT,payment_link TEXT,last_update TEXT,"
    "language TEXT,author TEXT,image_url TEXT,type TEXT,feed_identifier TEXT,auto_download INTEGER DEFAULT 1,"
    "username TEXT,password TEXT,include_filter TEXT DEFAULT '',exclude_filter TEXT DEFAULT '',"
    "minimal_duration_filter INTEGER DEFAULT -1,keep_updated INTEGER DEFAULT 1,is_paged INTEGER DEFAULT 0,"
    "next_page_link TEXT,hide TEXT,sort_order TEXT,last_update_failed INTEGER DEFAULT 0,"
    "auto_delete_action INTEGER DEFAULT 0,feed_playback_speed REAL DEFAULT -1.0,"
    "feed_skip_silence INTEGER DEFAULT 1,feed_volume_adaption INTEGER DEFAULT 0,tags TEXT,"
    "feed_skip_intro INTEGER DEFAULT 0,feed_skip_ending INTEGER DEFAULT 0,episode_notification INTEGER DEFAULT 0,"
    "state INTEGER DEFAULT 0,new_episodes_action INTEGER DEFAULT 0)",
    "CREATE TABLE FeedItems (id INTEGER PRIMARY KEY AUTOINCREMENT ,title TEXT,pubDate INTEGER,read INTEGER,"
    "link TEXT,description TEXT,payment_link TEXT,media INTEGER,feed INTEGER,has_simple_chapters INTEGER,"
    "item_identifier TEXT,image_url TEXT,auto_download INTEGER,podcastindex_chapter_url TEXT,"
    "podcastindex_transcript_type TEXT,podcastindex_transcript_url TEXT,social_interact_url TEXT)",
    "CREATE TABLE FeedMedia (id INTEGER PRIMARY KEY AUTOINCREMENT ,duration INTEGER,file_url TEXT,"
    "download_url TEXT,downloaded INTEGER,position INTEGER,filesize INTEGER,mime_type TEXT,"
    "playback_completion_date INTEGER,feeditem INTEGER,played_duration INTEGER,has_embedded_picture INTEGER,"
    "last_played_time INTEGER)",
    "CREATE TABLE DownloadLog (id INTEGER PRIMARY KEY AUTOINCREMENT ,feedfile INTEGER,feedfile_type INTEGER,"
    "reason INTEGER,successful INTEGER,completion_date INTEGER,reason_detailed TEXT,title TEXT)",
    "CREATE TABLE Queue(id INTEGER PRIMARY KEY,feeditem INTEGER,feed INTEGER)",
    "CREATE TABLE SimpleChapters (id INTEGER PRIMARY KEY AUTOINCREMENT ,title TEXT,start INTEGER,"
    "feeditem INTEGER,link TEXT,image_url TEXT)",
    "CREATE TABLE Favorites(id INTEGER PRIMARY KEY,feeditem INTEGER,feed INTEGER)",
    "CREATE INDEX FeedItems_feed ON FeedItems (feed)",
    "CREATE INDEX FeedItems_pubDate ON FeedItems (pubDate)",
    "CREATE INDEX FeedItems_read ON FeedItems (read)",
    "CREATE INDEX FeedMedia_feeditem ON FeedMedia (feeditem)",
    "CREATE INDEX Queue_feeditem ON Queue (feeditem)",
    "CREATE INDEX SimpleChapters_feeditem ON SimpleChapters (feeditem)",
]


def write_library(path, feeds=(), items=(), media=(), queue=(), favorites=()):
    """Writes a small AntennaPod 3.12.1 database: the exact DDL, `user_version` 3110000 and
    the given rows (dicts of column -> value; omitted columns take their DDL default)."""
    for suffix in ("", "-journal", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        for ddl in SCHEMA_3_12_1:
            conn.execute(ddl)
        conn.execute("INSERT INTO android_metadata (locale) VALUES ('en_US')")
        for table, rows in (("Feeds", feeds), ("FeedItems", items), ("FeedMedia", media),
                            ("Queue", queue), ("Favorites", favorites)):
            for row in rows:
                cols = sorted(row)
                conn.execute("INSERT INTO %s (%s) VALUES (%s)" % (table, ",".join(cols), ",".join("?" * len(cols))),
                             [row[c] for c in cols])
        conn.execute("PRAGMA user_version = %d" % SUPPORTED_USER_VERSION)
        conn.commit()
    finally:
        conn.close()


def main(argv):
    if len(argv) != 2:
        sys.stderr.write("usage: import_android.py <Antennapod.db>\n")
        return 2
    try:
        out = analyze(argv[1])
    except UnsupportedFile as e:
        sys.stderr.write("unsupported file: %s\n" % e)
        return 1
    sys.stdout.write(dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
