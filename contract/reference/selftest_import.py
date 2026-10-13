#!/usr/bin/env python3
"""Self-tests for import_android.py (PROTOCOL §11.5: Android AntennaPod 3.12.1 backup import).

    python3 contract/reference/selftest_import.py            # run every check, exit 0 on success
    python3 contract/reference/selftest_import.py --write    # regenerate contract/vectors/import

Checks:
- the DDL the generator writes is exactly the 3.12.1 emulator libraries' DDL;
- release groups and episode grouping on hand-built cases (PROTOCOL §3.3, §9.7);
- the merged-row rules of PROTOCOL §11.5 (state row, unions, units);
- podcast grouping and lifecycle, policies and credentials on hand-built feeds;
- Android's own statistics SQL equals the projection on the four real 3.12.1 libraries;
- each generated fixture produces exactly its documented mismatches, and the four-value unit (no PLAYED flag) produces the documented extra one;
- the vectors on disk equal a fresh computation (and the generated .db files hold exactly
  the rows defined here).

The generated fixtures are defined in FIXTURES below; `--write` rebuilds their .db files with
the exact 3.12.1 schema and rewrites every .json (generated and real fixtures).
"""

import calendar
import json
import os
import shutil
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import import_android as ia  # noqa: E402

REPO = os.path.dirname(os.path.dirname(HERE))
VECTORS = os.path.join(REPO, "contract", "vectors", "import")
REAL_DIR = os.path.join(REPO, "internal", "importer", "androiddb", "testdata", "android-3.12.1")
REAL_FIXTURES = ["archived-credentials-library", "authentication-preview-library",
                 "emulator-parity-library", "first-journey-downloaded"]
# The one older-versions file already at 3110000 (DDL-generated, edge values; not upgraded).
EDGES = ("v3110000-edges", os.path.join(REPO, "internal", "importer", "androiddb", "testdata", "older-versions",
                                        "v3110000-edges.db"),
         "internal/importer/androiddb/testdata/older-versions/v3110000-edges.db")
# Android's LEFT JOIN counts item 3's second media row.
EDGES_DIFFERENCES = {("per_podcast.episodes", 1, ("media_not_imported",))}
SCRATCH = os.path.join(REPO, ".work", "import-selftest")

FAILURES = []


def check(cond, what):
    if not cond:
        FAILURES.append(what)
        print("FAIL: " + what)


def eq(got, want, what):
    if got != want:
        FAILURES.append(what)
        print("FAIL: %s\n  got:  %r\n  want: %r" % (what, got, want))


def ms(y, mo, d, h=12, mi=0):
    return calendar.timegm((y, mo, d, h, mi, 0)) * 1000


DAY = 86400000


# ---------------------------------------------------------------------------
# Generated fixtures (small libraries with the exact 3.12.1 schema)
# ---------------------------------------------------------------------------

def _media(mid, item, url, duration=0, position=0, played=0, last_played=0, history=0, mime="audio/mpeg"):
    return {"id": mid, "feeditem": item, "download_url": url, "duration": duration, "position": position,
            "played_duration": played, "last_played_time": last_played, "playback_completion_date": history,
            "downloaded": 0, "filesize": 0, "mime_type": mime, "has_embedded_picture": -1}


def _item(iid, feed, title, guid, pub, read=0, media=None):
    return {"id": iid, "feed": feed, "title": title, "item_identifier": guid, "pubDate": pub, "read": read,
            "media": media, "link": None, "has_simple_chapters": 0, "auto_download": 1}


def _feed(fid, title, url, state=0, keep_updated=1, action=0, ftype="rss", username=None, password=None,
          custom_title=None):
    return {"id": fid, "title": title, "custom_title": custom_title, "download_url": url, "type": ftype,
            "state": state, "keep_updated": keep_updated, "new_episodes_action": action,
            "username": username, "password": password, "downloaded": 0, "auto_download": 1}


CDN = "https://cdn.example.invalid"

FIXTURES = {
    # Duplicate GUIDs, reused GUIDs, enclosure-only and ambiguous items, every play state,
    # favorites and a queue across two feeds, history, a pre-1.4 legacy row and a future date.
    "merged-rows": {
        "description": "duplicate-GUID rows in one release (played in different months), a reused GUID across "
                       "two releases, a rotating enclosure (allocated release ID), a row re-dated 13 days later with the same "
                       "enclosure that joins the later release 7 days away (PROTOCOL §9.7's resolver), enclosure-only rows that "
                       "normalize equal, items without GUID or enclosure, a text-only item with a dangling legacy "
                       "media link, GUIDs 'null' and blank, NEW / played / unplayed-with-position, favorites, a "
                       "queue across feeds, history, a legacy row with last_played_time 0 and a future last-played "
                       "date; an Atom feed with credentials",
        "feeds": [
            _feed(1, "Merged Rows Show", "https://feeds.example.invalid/merged.xml"),
            _feed(2, "Atom Show", "https://private.example.invalid/feed.atom", keep_updated=0, action=3,
                  ftype="atom", username="listener", password="secret", custom_title="My Atom Show"),
        ],
        "items": [
            _item(1, 1, "Duplicate one (first copy)", "dup-1", ms(2026, 2, 1)),
            _item(2, 1, "Duplicate one", "  dup-1\n", ms(2026, 2, 3)),
            _item(3, 1, "Reused GUID (2025)", "reused", ms(2025, 1, 6), read=1),
            _item(4, 1, "Reused GUID (rerun)", "reused", ms(2025, 6, 2), read=-1),
            _item(5, 1, "Reused GUID (rerun, new ad)", "reused", ms(2025, 6, 4), read=-1),
            _item(6, 1, "No GUID", None, ms(2026, 1, 10), read=1),
            _item(7, 1, "No GUID (copy)", "", ms(2026, 1, 10)),
            _item(8, 1, "Announcement", None, ms(2026, 1, 20)),
            _item(9, 1, "Announcement", None, ms(2026, 1, 21), read=1),
            _item(10, 1, "Text only", "text-only", ms(2026, 1, 22), media=999),
            _item(11, 1, "From the future", "future", ms(2026, 9, 1)),
            _item(12, 1, "GUID null", "null", ms(2026, 9, 2), read=-1),
            _item(13, 1, "Blank GUID", " \t ", ms(2026, 9, 3)),
            _item(14, 1, "Legacy listening", "legacy", ms(2014, 5, 1), read=1),
            _item(15, 2, "Atom one", "atom-1", ms(2026, 4, 1)),
            _item(16, 2, "Atom two", "atom-2", ms(2026, 4, 8), read=-1),
            _item(17, 2, "Atom two (other case)", "ATOM-2", ms(2026, 4, 8)),
            _item(18, 1, "Rotating (January)", "rotating", ms(2025, 1, 1)),
            _item(19, 1, "Rotating (February)", "rotating", ms(2025, 2, 1)),
            _item(20, 1, "Rotating (March)", "rotating", ms(2025, 3, 1), read=1),
            _item(21, 1, "Late dispute (first)", "late", ms(2024, 1, 1), read=1),
            _item(22, 1, "Late dispute (re-dated)", "late", ms(2024, 1, 14)),
            _item(23, 1, "Late dispute (new audio)", "late", ms(2024, 1, 21), read=-1),
        ],
        "media": [
            _media(1, 1, CDN + "/dup-1.mp3?sig=a", 1800000, 0, 600000, ms(2026, 3, 10, 20), ms(2026, 3, 10, 20, 5)),
            _media(2, 2, CDN + "/dup-1.mp3?sig=b", 1800000, 420000, 300000, ms(2026, 5, 2, 8), ms(2026, 5, 2, 8)),
            _media(3, 3, CDN + "/2025/reused.mp3", 2400000, 0, 2400000, ms(2025, 1, 7), ms(2025, 1, 7, 13)),
            _media(4, 4, CDN + "/2025/reused-rerun.mp3", 2400000),
            _media(5, 5, CDN + "/2025/reused-rerun.mp3?ad=2", 2460000),
            _media(6, 6, "HTTPS://CDN.Example.invalid:443/no-guid.mp3#t=0", 900000, 0, 900000,
                   ms(2026, 1, 15), ms(2026, 1, 15)),
            _media(7, 7, CDN + "/no-guid.mp3", 900000),
            _media(8, 9, None, 300000),
            _media(9, 11, CDN + "/future.mp3", 3600000, 60000, 60000, ms(2030, 6, 15), ms(2030, 6, 15)),
            _media(10, 12, CDN + "/guid-null.mp3", 1200000),
            _media(11, 14, CDN + "/legacy.mp3", 1200000, 0, 1200000, 0, ms(2014, 6, 1)),
            _media(12, 15, "https://private.example.invalid/a1.m4a", 1500000, 30000, 30000,
                   ms(2026, 4, 20), ms(2026, 4, 20), mime="audio/mp4"),
            _media(13, 16, "https://private.example.invalid/a2.m4a", 1500000, mime="audio/mp4"),
            _media(14, 17, "https://private.example.invalid/a2-upper.m4a", 1500000, mime="audio/mp4"),
            _media(15, 18, CDN + "/rotating-x.mp3", 600000),
            _media(16, 19, CDN + "/rotating-y.mp3", 600000),
            _media(17, 20, CDN + "/rotating-y.mp3", 600000),
            _media(18, 21, CDN + "/late-a.mp3", 1000000, 0, 1000000, ms(2024, 1, 2), ms(2024, 1, 2)),
            _media(19, 22, CDN + "/late-a.mp3", 1000000, 500000, 500000, ms(2024, 2, 1), ms(2024, 2, 1)),
            _media(20, 23, CDN + "/late-b.mp3", 1100000),
        ],
        "queue": [{"id": 0, "feeditem": 15, "feed": 2}, {"id": 1, "feeditem": 11, "feed": 1},
                  {"id": 2, "feeditem": 2, "feed": 1}, {"id": 3, "feeditem": 16, "feed": 2},
                  {"id": 4, "feeditem": 1, "feed": 1}, {"id": 5, "feeditem": 777, "feed": 1}],
        "favorites": [{"id": 1, "feeditem": 1, "feed": 1}, {"id": 2, "feeditem": 10, "feed": 1},
                      {"id": 3, "feeditem": 15, "feed": 2}, {"id": 4, "feeditem": 4242, "feed": 1}],
        "expected_differences": [],
    },
    # Podcast-level mapping: same URL in two spellings, a local folder, archived and preview
    # feeds, credentials (columns and URL userinfo), new-episodes actions, keep-updated.
    "feeds-lifecycle": {
        "description": "two feeds whose URLs normalize equal (merged, with a GUID shared across them; credentials "
                       "only on the second), a local folder with listening (skipped), archived and preview feeds "
                       "with listening, two unused previews (skipped, one with the merged feeds' URL), credentials "
                       "in columns and in URL userinfo, every new-episodes action, keep_updated NULL, a feed "
                       "without items, a queue across feeds with a local-folder row",
        "feeds": [
            _feed(1, "Duplicate URL", "HTTP://Example.invalid:80/dup-feed.xml#top", action=1),
            _feed(2, "Local folder",
                  "antennapod_local:content://com.android.externalstorage.documents/tree/primary%3APodcasts"),
            _feed(3, "Archived Show", "https://archive.example.invalid/feed.xml", state=2, keep_updated=0, action=2),
            _feed(4, "Preview Show", "https://preview.example.invalid/feed.xml", state=1, keep_updated=0),
            _feed(5, "Duplicate URL (copy)", "http://example.invalid/dup-feed.xml", keep_updated=0, action=3,
                  username="late-user", password="pw"),
            _feed(6, "Private Show", "https://secure.example.invalid/feed.xml", username="listener",
                  password="p@ss word"),
            _feed(7, "Userinfo Show", "https://member:token@members.example.invalid/feed.xml"),
            _feed(8, "Odd action", "https://odd.example.invalid/feed.xml", keep_updated=None, action=7, state=None),
            _feed(9, "Unused preview", "https://unused.example.invalid/feed.xml", state=1, keep_updated=0),
            _feed(10, "Duplicate URL (unused preview)", "https://EXAMPLE.invalid/dup-feed.xml", state=1,
                  keep_updated=0, username="preview-user", password="pw"),
        ],
        "items": [
            _item(1, 1, "D1", "d1", ms(2026, 6, 1), read=1),
            _item(2, 1, "D2", "d2", ms(2026, 6, 8)),
            _item(3, 5, "D1 (copy feed)", "d1", ms(2026, 6, 1)),
            _item(4, 5, "D3", "d3", ms(2026, 6, 15), read=-1),
            _item(5, 2, "Local track", "local-1", ms(2026, 7, 9), read=1),
            _item(6, 3, "Archived one", "arch-1", ms(2025, 11, 1), read=1),
            _item(7, 3, "Archived two", "arch-2", ms(2025, 11, 8)),
            _item(8, 4, "Preview one", "prev-1", ms(2026, 7, 1)),
            _item(9, 6, "Private one", "priv-1", ms(2026, 8, 1)),
            _item(10, 7, "Member one", "member-1", ms(2026, 8, 2)),
            _item(11, 9, "Unused one", "u1", ms(2026, 8, 3)),
            _item(12, 10, "D1 (unused preview)", "d1", ms(2026, 6, 1)),
        ],
        "media": [
            _media(1, 1, "https://example.invalid/d1.mp3", 1000000, 0, 1000000, ms(2026, 6, 2), ms(2026, 6, 2)),
            _media(2, 2, "https://example.invalid/d2.mp3", 1000000),
            _media(3, 3, "https://example.invalid/d1.mp3?src=copy", 1000000, 250000, 250000,
                   ms(2026, 7, 3), ms(2026, 7, 3)),
            _media(4, 4, "https://example.invalid/d3.mp3", 1000000),
            _media(5, 5, "content://com.android.externalstorage.documents/tree/primary%3APodcasts/track1.mp3",
                   120000, 0, 120000, ms(2026, 7, 10), ms(2026, 7, 10)),
            _media(6, 6, "https://archive.example.invalid/a1.mp3", 2000000, 0, 2000000,
                   ms(2025, 11, 2), ms(2025, 11, 2)),
            _media(7, 7, "https://archive.example.invalid/a2.mp3", 2000000),
            _media(8, 8, "https://preview.example.invalid/p1.mp3", 1800000, 90000, 90000,
                   ms(2026, 7, 4), ms(2026, 7, 4)),
            _media(9, 9, "https://secure.example.invalid/e1.mp3", 600000),
            _media(10, 10, "https://member:token@members.example.invalid/ep1.mp3", 600000),
            _media(11, 11, "https://unused.example.invalid/u1.mp3", 500000),
            _media(12, 12, "https://example.invalid/d1.mp3", 1000000),
        ],
        "queue": [{"id": 0, "feeditem": 7, "feed": 3}, {"id": 1, "feeditem": 3, "feed": 5},
                  {"id": 2, "feeditem": 5, "feed": 2}, {"id": 3, "feeditem": 1, "feed": 1},
                  {"id": 4, "feeditem": 9, "feed": 6}],
        "favorites": [{"id": 1, "feeditem": 6, "feed": 3}, {"id": 2, "feeditem": 5, "feed": 2}],
        "expected_differences": [
            ("per_feed_not_imported", 2, ("skipped_local_folder",)),       # its row in Android's per-feed list
            ("monthly.played_ms", "2026-07", ("skipped_local_folder",)),   # its listening in July
        ],
    },
    # Merged rows Android counts through units, and the rulings' expected differences.
    "parity-gaps": {
        "description": "merged rows Android counts only through units carrying their own PLAYED flag and position "
                       "(an all-zero row, a PLAYED row with duration 0, a row with only a position, a PLAYED row never "
                       "listened to); a state row removed from history while another row keeps its history (the "
                       "episode takes the state row's: no history, not started, as in Android); expected "
                       "differences: a merged row without a media row and a used preview feed whose URL equals a "
                       "subscribed feed's",
        "feeds": [
            _feed(1, "Gaps Show", "https://gaps.example.invalid/feed.xml"),
            _feed(2, "Joined Show", "https://joined.example.invalid/feed.xml"),
            _feed(3, "Joined Show (preview)", "https://JOINED.example.invalid/feed.xml", state=1, keep_updated=0),
        ],
        "items": [
            _item(1, 1, "Zero copy", "zero-dup", ms(2026, 3, 1)),
            _item(2, 1, "Zero", "zero-dup", ms(2026, 3, 1), read=1),
            _item(3, 1, "Marked copy", "marked-dup", ms(2026, 4, 1), read=1),
            _item(4, 1, "Marked", "marked-dup", ms(2026, 4, 2)),
            _item(5, 1, "Position copy", "pos-dup", ms(2026, 5, 1)),
            _item(6, 1, "Position", "pos-dup", ms(2026, 5, 1)),
            _item(7, 1, "Played copy", "played-dup", ms(2026, 6, 1), read=1),
            _item(8, 1, "Played", "played-dup", ms(2026, 6, 2)),
            _item(9, 2, "Joined one", "j1", ms(2026, 7, 1), read=1),
            _item(10, 3, "Joined two (preview)", "j2", ms(2026, 7, 1)),
            _item(11, 1, "Zero copy (no media)", "zero-dup", ms(2026, 3, 1), read=1),
            _item(12, 1, "History kept", "hist-dup", ms(2026, 8, 1)),
            _item(13, 1, "History removed", "hist-dup", ms(2026, 8, 1)),
        ],
        "media": [
            _media(1, 1, "https://gaps.example.invalid/zero-a.mp3", 0),
            _media(2, 2, "https://gaps.example.invalid/zero-b.mp3", 1200000, 0, 1200000,
                   ms(2026, 3, 2), ms(2026, 3, 2)),
            _media(3, 3, "https://gaps.example.invalid/marked-a.mp3", 0),
            _media(4, 4, "https://gaps.example.invalid/marked-b.mp3", 900000, 100000, 100000,
                   ms(2026, 4, 3), ms(2026, 4, 3)),
            _media(5, 5, "https://gaps.example.invalid/pos-a.mp3", 600000, 200000),
            _media(6, 6, "https://gaps.example.invalid/pos-b.mp3", 600000, 60000, 60000,
                   ms(2026, 5, 5), ms(2026, 5, 5)),
            _media(7, 7, "https://gaps.example.invalid/played-a.mp3", 1500000),
            _media(8, 8, "https://gaps.example.invalid/played-b.mp3", 1500000, 300000, 300000,
                   ms(2026, 6, 3), ms(2026, 6, 3)),
            _media(9, 9, "https://joined.example.invalid/j1.mp3", 1000000, 0, 1000000,
                   ms(2026, 7, 1, 20), ms(2026, 7, 1, 20)),
            _media(10, 10, "https://joined.example.invalid/j2.mp3", 800000, 400000, 400000,
                   ms(2026, 7, 2), ms(2026, 7, 2)),
            _media(11, 12, "https://gaps.example.invalid/hist-a.mp3", 700000, 0, 200000,
                   ms(2026, 8, 2), ms(2026, 8, 2)),
            _media(12, 13, "https://gaps.example.invalid/hist-b.mp3", 700000, 300000, 300000, ms(2026, 8, 5), 0),
        ],
        "queue": [],
        "favorites": [],
        "expected_differences": [
            ("per_podcast.episodes", 1, ("merged_row_without_media",)),                 # row 11
            ("per_podcast.episodes_started_marked", 1, ("merged_row_without_media",)),  # row 11 is PLAYED
        ] + [(m, 2, ("preview_feed_joins_listed_podcast",)) for m in (                  # feed 3 joins podcast 2
            "per_podcast.played_ms", "per_podcast.played_marked_ms", "per_podcast.episodes_started",
            "per_podcast.episodes_started_marked", "per_podcast.episodes", "per_podcast.total_ms",
            "window.played_ms")],
    },
}


def write_fixture(name, path):
    fx = FIXTURES[name]
    ia.write_library(path, fx["feeds"], fx["items"], fx["media"], fx["queue"], fx["favorites"])


def diff_keys(entries):
    """(metric, where, causes) of differences; a metric's windows collapse into one key."""
    out = set()
    for m in entries:
        where = m.get("feed_id", m.get("month", m.get("podcast")))
        out.add((m["metric"], where, tuple(m.get("causes", ()))))
    return out


# ---------------------------------------------------------------------------
# Hand-built cases
# ---------------------------------------------------------------------------

def occ(iid, u, d):
    return {"id": iid, "u": u, "d": d}


def members(rels):
    return [r["members"] for r in rels]


def test_release_groups():
    W = ia.RELEASE_WINDOW_MS
    t0 = ms(2025, 1, 1)
    # One release: within 7 days, whatever the enclosures (signed URLs, dynamic ads).
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "b", t0 + 2 * DAY), occ(3, "c", t0 + W)])),
       [[1, 2, 3]], "within 7 days (7 days exactly included) is one release")
    # Two releases: different enclosure and more than 7 days.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "b", t0 + W + 1)])),
       [[1], [2]], "different enclosures more than 7 days apart are two releases")
    # Same enclosure far apart: no dispute evidence, one release.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "a", t0 + 40 * DAY)])),
       [[1, 2]], "same enclosure far apart stays one release")
    # The earliest release is first even when its row has the higher ID.
    rels = ia.release_groups([occ(5, "late", t0 + 30 * DAY), occ(9, "early", t0)])
    eq(members(rels), [[9], [5]], "the earliest release comes first whatever the row IDs")
    eq(rels[1]["encs"], ["late"], "a later release's first enclosure")
    # A row without enclosure cannot start a release; it joins the earliest one.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, None, t0 + 30 * DAY)])),
       [[1, 2]], "a dated row without enclosure joins the earliest release")
    # Distances are measured to a release's date, not to its last row; and every row is then
    # placed where PROTOCOL §9.7's resolver puts it (the nearest release date within 7 days), even
    # when it shares the other release's enclosure.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "a", t0 + 6 * DAY), occ(3, "b", t0 + 8 * DAY)])),
       [[1], [2, 3]], "8 days from the first release's date starts a release; day 6 is nearer to it")
    # A row assigned before any dispute still follows the resolver once the dispute exists.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "b", t0 + 20 * DAY), occ(3, "a", t0 + 13 * DAY)])),
       [[1], [2, 3]], "a row 7 days from a later release joins it, whatever its enclosure")
    # The dispute rule holds against any other row, earlier or later, as a fetch's first
    # dispute partitions the same evidence: a far row with the first release's enclosure
    # becomes a release when a later row has another.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "a", t0 + 20 * DAY), occ(3, "b", t0 + 40 * DAY)])),
       [[1], [2], [3]], "a later row's different enclosure more than 7 days away counts")
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "a", t0 + 10 * DAY), occ(3, "b", t0 + 20 * DAY)])),
       [[1], [2], [3]], "three releases, as a fetch makes")
    # A far row with the first release's enclosure and no dispute evidence at all stays with it.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "a", t0 + 20 * DAY)])),
       [[1, 2]], "no new release without a different enclosure more than 7 days away")
    # A far row with the same enclosure as an existing later release joins that release.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "b", t0 + 20 * DAY), occ(3, "b", t0 + 60 * DAY),
                                  occ(4, "c", t0 + 61 * DAY)])),
       [[1], [2], [3, 4]], "a far row disputed by an older different enclosure starts a release; a near one joins")
    # Undated rows: the release holding their enclosure if exactly one does, else the earliest.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "b", t0 + 20 * DAY), occ(3, "b", None),
                                  occ(4, "z", None)])),
       [[1, 4], [2, 3]], "undated rows go by enclosure, else to the earliest release")
    eq(members(ia.release_groups([occ(1, "a", None), occ(2, "b", None)])), [[1, 2]],
       "undated rows alone are one release")
    # Nearest release wins; a tie goes to the earlier release.
    eq(members(ia.release_groups([occ(1, "a", t0), occ(2, "b", t0 + 10 * DAY), occ(3, "c", t0 + 5 * DAY)])),
       [[1, 3], [2]], "a row 5 days from both releases joins the earlier")


def lib(feeds, items, media=(), queue=(), favorites=()):
    return {"feeds": list(feeds), "items": list(items), "media": list(media), "queue": list(queue),
            "favorites": list(favorites)}


def episodes_of(plan, feed_id):
    for p in plan["podcasts"]:
        if feed_id in p["source_feed_ids"]:
            return p["episodes"]
    raise KeyError(feed_id)


def by_items(plan):
    return {tuple(ep["source_item_ids"]): ep for p in plan["podcasts"] for ep in p["episodes"]}


def test_identity_and_grouping():
    F = [_feed(1, "Show", "https://show.example.invalid/feed.xml")]
    t = ms(2026, 1, 1)
    plan = ia.plan_import(lib(F, [
        _item(1, 1, "a", " g1 ", t), _item(2, 1, "b", "g1", t + DAY),           # one GUID, one release
        _item(3, 1, "c", "G1", t),                                               # GUIDs keep case
        _item(4, 1, "d", "null", t), _item(5, 1, "e", None, t),                  # 'null' is absent
        _item(6, 1, "f", None, t), _item(7, 1, "g", "", t),                       # neither: ambiguous
        _item(8, 1, "h", "r", t), _item(9, 1, "i", "r", t + 30 * DAY),            # reused GUID
        _item(10, 1, "j", "r", t + 60 * DAY),                                    # rotating back to /r2
        _item(11, 1, "k", "r", t + 90 * DAY),
    ], [
        _media(1, 1, "https://x.example.invalid/1.mp3"), _media(2, 2, "https://x.example.invalid/2.mp3"),
        _media(3, 3, "https://x.example.invalid/3.mp3"),
        _media(4, 4, "https://X.example.invalid/shared.mp3"), _media(5, 5, "https://x.example.invalid/shared.mp3"),
        _media(6, 7, ""),
        _media(8, 8, "https://x.example.invalid/r1.mp3"), _media(9, 9, "https://x.example.invalid/r2.mp3"),
        _media(10, 10, "https://x.example.invalid/r1.mp3"), _media(11, 11, "https://x.example.invalid/r2.mp3"),
    ]))
    eps = by_items(plan)
    eq(sorted(eps), [(1, 2), (3,), (4, 5), (6,), (7,), (8,), (9,), (10,), (11,)], "episode grouping")
    eq(eps[(1, 2)]["identity"], {"k": "rss-guid", "v": "g1"}, "trimmed GUID identity")
    eq(eps[(3,)]["identity"], {"k": "rss-guid", "v": "G1"}, "GUID case kept")
    eq(eps[(4, 5)]["identity"], {"k": "enclosure", "v": "https://x.example.invalid/shared.mp3"},
       "'null' GUID and no GUID group by normalized enclosure")
    eq(eps[(6,)]["identity"], {"k": "e0", "reason": "no_guid_no_enclosure", "source_item_id": 6}, "ambiguous row")
    eq(eps[(7,)]["identity"]["k"], "e0", "empty GUID and empty enclosure is ambiguous")
    eq(eps[(8,)]["identity"], {"k": "rss-guid", "v": "r"}, "the earliest release keeps the plain ID")
    eq(eps[(9,)]["identity"], {"k": "rss-guid+enclosure", "v": "r\x1fhttps://x.example.invalid/r2.mp3"},
       "a later release is rss-guid+enclosure of its first enclosure")
    eq(eps[(10,)]["identity"], {"k": "rss-guid+enclosure", "v": "r\x1fhttps://x.example.invalid/r1.mp3"},
       "a third release returning to the first enclosure is rss-guid+enclosure (the original holds the plain ID)")
    eq(eps[(11,)]["identity"], {"k": "e0", "reason": "release_id_taken", "source_item_id": 11},
       "a release whose rss-guid+enclosure ID is taken is allocated")
    eq([a["source_item_id"] for a in plan["result"]["ambiguous"]], [6, 7], "ambiguous list")

    # Atom feeds use atom-id for the plain ID only.
    plan = ia.plan_import(lib([_feed(1, "A", "https://a.example.invalid/f", ftype="atom")],
                              [_item(1, 1, "a", "x", t), _item(2, 1, "b", "x", t + 30 * DAY)],
                              [_media(1, 1, "https://a.example.invalid/1"), _media(2, 2, "https://a.example.invalid/2")]))
    eq([ep["identity"]["k"] for ep in episodes_of(plan, 1)], ["atom-id", "rss-guid+enclosure"], "atom K")

    # Enclosures with userinfo compare without it; GUID-less items never join a GUID episode.
    plan = ia.plan_import(lib(F, [_item(1, 1, "a", None, t), _item(2, 1, "b", None, t), _item(3, 1, "c", "g", t)],
                              [_media(1, 1, "https://u:p@x.example.invalid/e.mp3"),
                               _media(2, 2, "https://x.example.invalid/e.mp3"),
                               _media(3, 3, "https://x.example.invalid/e.mp3")]))
    eq(sorted(by_items(plan)), [(1, 2), (3,)], "userinfo-free enclosure grouping; no cross-match with a GUID")
    # The fetch's one rule (strip_userinfo): userinfo spelled with character references goes too,
    # so an imported enclosure derives the episode its feed's fetch derives.
    plan = ia.plan_import(lib(F, [_item(1, 1, "a", None, t), _item(2, 1, "b", None, t)],
                              [_media(1, 1, "https://u:p&#64;x.example.invalid/e.mp3"),
                               _media(2, 2, "https://x.example.invalid/e.mp3")]))
    eq([(k, ep["identity"]) for k, ep in sorted(by_items(plan).items())],
       [((1, 2), {"k": "enclosure", "v": "https://x.example.invalid/e.mp3"})],
       "an enclosure's userinfo spelled with a character reference is removed, as in a fetch")


def test_merged_state():
    F = [_feed(1, "Show", "https://show.example.invalid/feed.xml")]
    t = ms(2026, 1, 1)
    items = [_item(1, 1, "first", "g", t, read=1), _item(2, 1, "second", "g", t + DAY, read=0),
             _item(3, 1, "third", "g", t + 2 * DAY, read=-1), _item(4, 1, "no media", "g", t)]
    media = [_media(1, 1, "https://x.example.invalid/1", 1000, 0, 500, ms(2026, 2, 1), ms(2026, 2, 1)),
             _media(2, 2, "https://x.example.invalid/2", 2000, 700, 700, ms(2026, 3, 1), ms(2026, 2, 15)),
             _media(3, 3, "https://x.example.invalid/3", 3000)]
    queue = [{"id": 0, "feeditem": 9}, {"id": 1, "feeditem": 3}, {"id": 2, "feeditem": 1}]
    favorites = [{"id": 1, "feeditem": 1}]
    F2 = F + [_feed(2, "Other", "https://other.example.invalid/f")]
    items2 = items + [_item(9, 2, "other", "o", t)]
    plan = ia.plan_import(lib(F2, items2, media, queue, favorites))
    ep = by_items(plan)[(1, 2, 3, 4)]
    eq(ep["state_item_id"], 2, "the state row is the most recently played")
    eq((ep["play_state"], ep["inbox"], ep["position_ms"], ep["duration_ms"]), ("unplayed", False, 700, 2000),
       "state values from the state row")
    eq((ep["favorite"], ep["queued"]), (True, True), "favorite and queue are unions")
    eq(plan["queue"], [9, 3], "queue order with one entry per episode at its earliest row")
    eq(ep["baseline"], {"source_item_id": 2, "source_media_id": 2, "played_ms": 700,
                        "last_played": ms(2026, 3, 1), "history": ms(2026, 2, 15)}, "baseline of the state row")
    eq(ep["units"], [
        {"source_item_id": 1, "source_media_id": 1, "played_ms": 500, "last_played": ms(2026, 2, 1),
         "history": ms(2026, 2, 1), "duration_ms": 1000, "played": True, "position_ms": 0},
        {"source_item_id": 3, "source_media_id": 3, "played_ms": 0, "last_played": None, "history": None,
         "duration_ms": 3000, "played": False, "position_ms": 0}], "units of the other rows (row 4 has no media)")
    eq(ep["history"], ms(2026, 2, 15), "the episode's history is the state row's own (ruling 4)")
    eq([m["source_item_ids"] for m in plan["result"]["merged"]], [[1, 2, 3, 4]], "merged list")
    # A later completion on another row is the episode's history; the baseline keeps its own.
    plan = ia.plan_import(lib(F, [_item(1, 1, "a", "g", t), _item(2, 1, "b", "g", t)],
                              [_media(1, 1, "https://x.example.invalid/1", 10, 0, 5, ms(2026, 1, 2), ms(2026, 1, 9)),
                               _media(2, 2, "https://x.example.invalid/2", 10, 4, 4, ms(2026, 1, 5), 0)]))
    ep = episodes_of(plan, 1)[0]
    eq((ep["state_item_id"], ep["baseline"]["history"], ep["history"], ep["units"][0]["history"]),
       (2, None, None, ms(2026, 1, 9)), "another row's later history stays with its unit (ruling 4)")

    # Ties on last_played go to the highest item ID; NEW there is the Inbox.
    plan = ia.plan_import(lib(F, [_item(1, 1, "a", "g", t, read=1), _item(2, 1, "b", "g", t, read=-1)],
                              [_media(1, 1, "https://x.example.invalid/1"), _media(2, 2, "https://x.example.invalid/2")]))
    ep = episodes_of(plan, 1)[0]
    eq((ep["state_item_id"], ep["play_state"], ep["inbox"], ep["baseline"], ep["history"]),
       (2, "new", True, None, None), "tie to the highest ID; an all-zero state row gives no baseline")
    eq([(u["source_item_id"], u["played"], u["duration_ms"]) for u in ep["units"]], [(1, True, 0)],
       "an all-zero other row is still a unit (ruling 1)")

    # Only the lowest-id media row of an item is read; the others are listed.
    plan = ia.plan_import(lib(F, [_item(1, 1, "a", "g", t)],
                              [_media(5, 1, "https://x.example.invalid/b", 10), _media(3, 1, "https://x.example.invalid/a", 20),
                               _media(7, 99, "https://x.example.invalid/c")]))
    eq(episodes_of(plan, 1)[0]["duration_ms"], 20, "lowest-id media row")
    eq((plan["ignored"]["extra_media"], plan["ignored"]["orphan_media"]), ([5], [7]), "ignored media rows")


def test_podcasts():
    feeds = [
        _feed(1, "Preview first", "HTTPS://Show.example.invalid:443/feed.xml#x", state=1, keep_updated=0),
        _feed(2, "Subscribed second", "https://show.example.invalid/feed.xml", state=0, action=3,
              username="", password="pw"),
        _feed(3, "Local", "antennapod_local:content://x"),
        _feed(4, "No URL", None),
        _feed(5, "Archived", "https://arch.example.invalid/f", state=2, keep_updated=5, action=2, ftype="atom"),
        _feed(6, "Odd", "https://odd.example.invalid/f", state=9, keep_updated=None, action=11),
        _feed(7, "Trailing slash differs", "https://show.example.invalid/feed.xml/"),
        _feed(8, "Userinfo", "https://me:pw@show.example.invalid/feed.xml"),
    ]
    feeds += [
        _feed(9, "Unused preview", "https://unused.example.invalid/f", state=1),
        _feed(10, "Unused preview, same URL", "https://show.example.invalid/feed.xml", state=1, username="x"),
        _feed(11, "Plain", "https://plain.example.invalid/f"),
        _feed(12, "Plain copy with credentials", "https://PLAIN.example.invalid/f", password="p"),
        _feed(13, "Plain copy, other credentials", "https://plain.example.invalid:443/f", username="u"),
    ]
    items = [_item(1, 3, "local item", "l", 0), _item(2, 1, "preview item, favorite", "p", 0),
             _item(3, 9, "unused preview item", "u", 0), _item(4, 10, "unused", "u", 0)]
    plan = ia.plan_import(lib(feeds, items, [_media(1, 3, "https://unused.example.invalid/1")],
                              favorites=[{"id": 1, "feeditem": 2}]))
    ps = {p["creating_feed_id"]: p for p in plan["podcasts"]}
    eq(sorted(ps), [1, 5, 6, 7, 11], "podcasts by normalized URL (userinfo removed; trailing slash kept)")
    eq(ps[1]["source_feed_ids"], [1, 2, 8], "merged podcast's feeds (the unused preview is not one)")
    eq((ps[1]["properties_feed_id"], ps[1]["state"], ps[1]["new_episodes_action"], ps[1]["keep_updated"],
        ps[1]["credentials"], ps[1]["credentials_feed_id"]), (2, "subscribed", "queue", True, True, 2),
       "a preview creating feed yields to the first non-preview feed's properties")
    eq((ps[11]["source_feed_ids"], ps[11]["credentials"], ps[11]["credentials_feed_id"]), ([11, 12, 13], True, 12),
       "credentials from the first feed that has any when the properties feed has none")
    eq(ps[1]["url"], "HTTPS://Show.example.invalid:443/feed.xml#x", "the podcast URL is the creating feed's raw URL")
    eq((ps[5]["state"], ps[5]["keep_updated"], ps[5]["new_episodes_action"], ps[5]["k"], ps[5]["credentials"]),
       ("archived", True, "nothing", "atom-id", False), "archived feed")
    eq((ps[6]["state"], ps[6]["keep_updated"], ps[6]["new_episodes_action"]), ("subscribed", False, "inbox"),
       "unknown state, NULL keep_updated and unknown action")
    eq([(s["reason"], s["source_feed_id"]) for s in plan["result"]["skipped"]],
       [("local_folder", 3), ("no_feed_url", 4), ("unused_preview", 9), ("unused_preview", 10)], "skipped feeds")
    eq(plan["result"]["imported"]["favorites"], 1, "a favorite makes a preview used")
    eq(plan["result"]["imported"]["episodes"], 1, "local and unused-preview items are not imported")
    for state in ({"read": 1}, {"read": -1}, {"position": 5}, {"playback_completion_date": 5},
                  {"last_played_time": 5}, {"played_duration": 5}, {"queue": True}):
        it = _item(1, 1, "a", "g", 0, read=state.get("read", 0))
        md = _media(1, 1, "https://p.example.invalid/1")
        md.update({k: v for k, v in state.items() if k not in ("read", "queue")})
        plan = ia.plan_import(lib([_feed(1, "P", "https://p.example.invalid/f", state=1)], [it], [md],
                                  queue=[{"id": 0, "feeditem": 1}] if state.get("queue") else []))
        eq((len(plan["podcasts"]), plan["result"]["skipped"]), (1, []), "a preview with %r is used" % state)
    plan = ia.plan_import(lib([_feed(1, "U", "https://me:pw@u.example.invalid/f")], []))
    eq((plan["podcasts"][0]["credentials"], plan["podcasts"][0]["url"]),
       (True, "https://u.example.invalid/f"), "URL userinfo is credentials and leaves the URL")
    plan = ia.plan_import(lib([_feed(1, "U", "https://me:pw&#64;u.example.invalid/f")], []))
    eq((plan["podcasts"][0]["credentials"], plan["podcasts"][0]["url"], plan["podcasts"][0]["normalized_url"]),
       (False, "https://u.example.invalid/f", "https://u.example.invalid/f"),
       "userinfo spelled with a character reference leaves the stored URL; "
       "it is not credentials, since Android's client sends only a literal user:password@")


def test_unsupported():
    os.makedirs(SCRATCH, exist_ok=True)
    path = os.path.join(SCRATCH, "old.db")
    ia.write_library(path)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = 3100000")
    conn.commit()
    conn.close()
    try:
        ia.read_library(path)
        check(False, "an older user_version must be refused")
    except ia.UnsupportedFile:
        pass


# ---------------------------------------------------------------------------
# Real and generated fixtures
# ---------------------------------------------------------------------------

def schema_of(path):
    conn = ia.open_readonly(path)
    try:
        return [r[0] for r in conn.execute("SELECT sql FROM sqlite_master WHERE name != 'sqlite_sequence' "
                                           "ORDER BY rowid")]
    finally:
        conn.close()


def test_schema():
    for name in REAL_FIXTURES:
        eq(schema_of(os.path.join(REAL_DIR, name + ".db")), ia.SCHEMA_3_12_1, "3.12.1 DDL of " + name)


def test_real_fixtures():
    for name in REAL_FIXTURES:
        out = ia.analyze(os.path.join(REAL_DIR, name + ".db"))
        eq((out["statistics"]["mismatches"], out["statistics"]["expected_differences"]), ([], []),
           "Android's SQL equals the projection on " + name)
    for name in ("corrupt-feeditems-page", "missing-feedmedia-3110000", "newer-3110001", "text-file", "zero-bytes"):
        try:
            ia.read_library(os.path.join(REAL_DIR, "..", "invalid", name + ".db"))
            check(False, "invalid/%s.db must be refused" % name)
        except ia.UnsupportedFile:
            pass
    out = ia.analyze(os.path.join(REAL_DIR, "archived-credentials-library.db"))
    ps = {p["creating_feed_id"]: p for p in out["podcasts"]}
    eq(ps[2]["source_feed_ids"], [2, 5], "archived-credentials: identical URLs merge")
    eq([(p["state"], p["credentials"]) for k, p in sorted(ps.items()) if k in (1, 3)],
       [("subscribed", True), ("archived", True)], "archived-credentials: lifecycle")
    eq(out["result"]["skipped"], [{"reason": "unused_preview", "title": "Preview imported", "source_feed_id": 4}],
       "archived-credentials: the preview without items is unused")
    out = ia.analyze(os.path.join(REAL_DIR, "authentication-preview-library.db"))
    eq([(s["reason"], s["source_feed_id"]) for s in out["result"]["skipped"]],
       [("unused_preview", 6), ("unused_preview", 9)], "authentication-preview: unused previews")
    eq([p["state"] for p in out["podcasts"] if p["creating_feed_id"] == 8], ["not_subscribed"],
       "authentication-preview: a preview with a played episode is imported")
    out = ia.analyze(os.path.join(REAL_DIR, "archived-credentials-library.db"))
    ep = ps[1]["episodes"][0]
    eq((ep["play_state"], ep["position_ms"], ep["favorite"], ep["queued"], ep["baseline"]),
       ("played", 23456, True, True, None), "archived-credentials: played item with a position, no listening")
    out = ia.analyze(os.path.join(REAL_DIR, "emulator-parity-library.db"))
    eq(out["statistics"]["android"]["monthly"], [{"year": 2026, "month": 9, "played_ms": 179493}],
       "emulator-parity: Android's monthly total")
    ps = {p["creating_feed_id"]: p for p in out["podcasts"]}
    eq(ps[12]["new_episodes_action"], "queue", "emulator-parity: the queue action")
    eq(out["queue"], [20, 21, 22, 23, 24, 25, 26, 27, 90], "emulator-parity: queue order")
    out = ia.analyze(EDGES[1])
    eq((out["statistics"]["mismatches"], diff_keys(out["statistics"]["expected_differences"])),
       ([], EDGES_DIFFERENCES), "edges: the expected difference")
    ep = {tuple(e["source_item_ids"]): e for p in out["podcasts"] for e in p["episodes"]}[(3,)]
    eq((ep["position_ms"], ep["play_state"], ep["queued"]), (1, "new", True),
       "edges: lowest-id media row, position truncated as getInt, NEW, duplicate queue rows")
    eq((ep["identity"], out["queue"]), ({"k": "enclosure", "v": "https://e.test/a.mp3"}, [3]), "edges: identity, queue")
    eq(out["ignored"], {"orphan_items": [2], "orphan_media": [11], "extra_media": [10], "dropped_queue_rows": [2, 3],
                        "dropped_favorite_rows": [1]}, "edges: ignored rows")


def four_value_mismatches(out):
    """Per-podcast differences when units carry only the four documented values (no PLAYED flag)."""
    plan = {"podcasts": json.loads(json.dumps(out["podcasts"]))}
    for p in plan["podcasts"]:
        for ep in p["episodes"]:
            for u in ep["units"]:
                u["played"] = False
    proj = ia.project(plan, out["statistics"]["android"]["windows"])
    android = {r["creating_feed_id"]: r for r in out["statistics"]["android"]["per_podcast"]}
    keys = set()
    for r in proj["per_podcast"]:
        a = android.get(r["creating_feed_id"], {})
        for k, v in r.items():
            if k != "creating_feed_id" and a.get(k, 0) != v:
                keys.add(("per_podcast." + k, r["creating_feed_id"]))
    return keys


def test_generated():
    os.makedirs(SCRATCH, exist_ok=True)
    for name, fx in sorted(FIXTURES.items()):
        path = os.path.join(SCRATCH, name + ".db")
        write_fixture(name, path)
        eq(schema_of(path), ia.SCHEMA_3_12_1, "generated DDL of " + name)
        out = ia.analyze(path)
        eq(out["statistics"]["mismatches"], [], "no mismatch in " + name)
        eq(diff_keys(out["statistics"]["expected_differences"]), set(fx["expected_differences"]),
           "expected differences of " + name)

    out = ia.analyze(os.path.join(SCRATCH, "merged-rows.db"))
    eps = by_items(out)
    e = eps[(1, 2)]
    eq((e["state_item_id"], e["play_state"], e["position_ms"], e["favorite"], e["queued"]),
       (2, "unplayed", 420000, True, True), "merged-rows: duplicate GUID rows are one episode")
    eq([(u["source_item_id"], u["played_ms"], u["last_played"]) for u in e["units"]],
       [(1, 600000, ms(2026, 3, 10, 20))], "merged-rows: the other row is a unit in its own month")
    eq(e["history"], ms(2026, 5, 2, 8), "merged-rows: the state row's history is the episode's")
    eq([c["cause"] for c in out["statistics"]["causes"]], ["absent_last_played_client_rule"],
       "merged-rows: the legacy row counts through the client rule")
    eq((eps[(3,)]["identity"]["k"], eps[(4, 5)]["identity"]["k"], eps[(4, 5)]["inbox"]),
       ("rss-guid", "rss-guid+enclosure", True), "merged-rows: reused GUID releases")
    eq([eps[(i,)]["identity"]["k"] for i in (18, 19, 20)], ["rss-guid", "rss-guid+enclosure", "e0"],
       "merged-rows: rotating enclosure")
    eq(eps[(6, 7)]["identity"]["k"], "enclosure", "merged-rows: GUID-less rows by normalized enclosure")
    eq((eps[(21,)]["identity"], eps[(22, 23)]["identity"], eps[(22, 23)]["state_item_id"]),
       ({"k": "rss-guid", "v": "late"}, {"k": "rss-guid+enclosure", "v": "late\x1f" + CDN + "/late-b.mp3"}, 22),
       "merged-rows: rows are placed by the release resolver")
    eq(eps[(11,)]["baseline"]["last_played"], ms(2030, 6, 15), "merged-rows: a future date is kept")
    eq((eps[(10,)]["favorite"], eps[(10,)]["baseline"], eps[(10,)]["duration_ms"]), (True, None, None),
       "merged-rows: text-only favorite")
    eq(out["queue"], [15, 11, 2, 16], "merged-rows: queue across feeds")
    eq(out["ignored"]["dropped_queue_rows"] + out["ignored"]["dropped_favorite_rows"], [5, 4],
       "merged-rows: rows naming missing items")
    eq([a["source_item_id"] for a in out["result"]["ambiguous"]], [8, 9, 13], "merged-rows: ambiguous items")

    out = ia.analyze(os.path.join(SCRATCH, "feeds-lifecycle.db"))
    ps = {p["creating_feed_id"]: p for p in out["podcasts"]}
    eq(sorted(ps), [1, 3, 4, 6, 7, 8], "feeds-lifecycle: podcasts")
    eq((ps[1]["source_feed_ids"], ps[1]["state"], ps[1]["new_episodes_action"], ps[1]["keep_updated"],
        ps[1]["credentials_feed_id"]), ([1, 5], "subscribed", "inbox", True, 5),
       "feeds-lifecycle: same URL in two spellings; credentials from the second feed")
    eq([(ps[k]["state"], ps[k]["credentials"]) for k in (3, 4, 6, 7)],
       [("archived", False), ("not_subscribed", False), ("subscribed", True), ("subscribed", True)],
       "feeds-lifecycle: lifecycle and credentials")
    eq((ps[8]["new_episodes_action"], ps[8]["keep_updated"], ps[8]["populated"]), ("inbox", False, False),
       "feeds-lifecycle: odd action, NULL keep_updated, no items")
    eq(out["queue"], [7, 3, 9], "feeds-lifecycle: queue")
    eq([(s["reason"], s["source_feed_id"]) for s in out["result"]["skipped"]],
       [("local_folder", 2), ("unused_preview", 9), ("unused_preview", 10)],
       "feeds-lifecycle: local folder and unused previews skipped")

    # Without the unit's PLAYED flag, a merged PLAYED row's duration is missed (ruling 1's reason).
    out = ia.analyze(os.path.join(SCRATCH, "parity-gaps.db"))
    lit = four_value_mismatches(out)
    check(("per_podcast.played_marked_ms", 1) in lit,
          "parity-gaps: without the unit's PLAYED flag the marked-played total differs")
    eps = by_items(out)
    eq([(u["source_item_id"], u["played"], u["position_ms"]) for u in eps[(1, 2, 11)]["units"]], [(1, False, 0)],
       "parity-gaps: the all-zero row is a unit; the row without media is not")
    eq([(u["source_item_id"], u["position_ms"]) for u in eps[(5, 6)]["units"]], [(5, 200000)],
       "parity-gaps: a unit carries its position")
    eq((eps[(12, 13)]["state_item_id"], eps[(12, 13)]["history"], eps[(12, 13)]["units"][0]["history"]),
       (13, None, ms(2026, 8, 2)), "parity-gaps: a state row removed from history leaves the episode without one")


def table_rows(path):
    conn = ia.open_readonly(path)
    try:
        out = {"user_version": conn.execute("PRAGMA user_version").fetchone()[0]}
        for t in ("android_metadata", "Feeds", "FeedItems", "FeedMedia", "DownloadLog", "Queue",
                  "SimpleChapters", "Favorites"):
            out[t] = [tuple(r) for r in conn.execute("SELECT * FROM %s" % t)]
        return out
    finally:
        conn.close()


def vector_targets():
    """(json name, db path, fixture label) for every vector."""
    out = []
    for name in sorted(FIXTURES):
        out.append((name, os.path.join(VECTORS, name + ".db"), "contract/vectors/import/%s.db" % name))
    for name in REAL_FIXTURES:
        out.append((name, os.path.join(REAL_DIR, name + ".db"),
                    "internal/importer/androiddb/testdata/android-3.12.1/%s.db" % name))
    out.append(EDGES)
    return out


def test_vectors_on_disk():
    os.makedirs(SCRATCH, exist_ok=True)
    for name in sorted(FIXTURES):
        on_disk = os.path.join(VECTORS, name + ".db")
        if not os.path.exists(on_disk):
            check(False, "missing generated fixture %s (run --write)" % on_disk)
            continue
        fresh = os.path.join(SCRATCH, name + ".fresh.db")
        write_fixture(name, fresh)
        eq(table_rows(on_disk), table_rows(fresh), "contents of %s.db" % name)
    for name, db, label in vector_targets():
        jp = os.path.join(VECTORS, name + ".json")
        if not os.path.exists(jp) or not os.path.exists(db):
            check(False, "missing vector %s" % jp)
            continue
        with open(jp) as f:
            on_disk = json.load(f)
        fresh = json.loads(ia.dumps(vector(name, db, label)))
        check(on_disk == fresh, "vector %s equals a fresh computation" % jp)
        st = on_disk["cases"][0]["statistics"]
        check(st["mismatches"] == [], "vector %s has no mismatch" % name)
        if name in FIXTURES:
            eq(diff_keys(st["expected_differences"]), set(FIXTURES[name]["expected_differences"]),
               "vector %s: expected differences" % name)


def vector(name, db, label):
    """A contract vector file (`{suite, cases}`, docs/CONTRACT.md) with one case."""
    out = ia.analyze(db, label)
    if name in FIXTURES:
        out["description"] = FIXTURES[name]["description"]
    return {"suite": "import_android", "cases": [out]}


def write_vectors():
    os.makedirs(VECTORS, exist_ok=True)
    for name in sorted(FIXTURES):
        write_fixture(name, os.path.join(VECTORS, name + ".db"))
    for name, db, label in vector_targets():
        out = vector(name, db, label)
        with open(os.path.join(VECTORS, name + ".json"), "w") as f:
            f.write(ia.dumps(out))
        st = out["cases"][0]["statistics"]
        print("wrote %s.json (%d mismatches, %d expected differences)"
              % (name, len(st["mismatches"]), len(st["expected_differences"])))


def main(argv):
    if "--write" in argv:
        write_vectors()
    tests = [test_schema, test_release_groups, test_identity_and_grouping, test_merged_state, test_podcasts,
             test_unsupported, test_real_fixtures, test_generated, test_vectors_on_disk]
    for t in tests:
        n = len(FAILURES)
        try:
            t()
        except Exception as e:  # a crash is a failure, reported with its test
            import traceback
            traceback.print_exc()
            FAILURES.append("%s raised %r" % (t.__name__, e))
        print("%s %s" % ("ok  " if len(FAILURES) == n else "FAIL", t.__name__))
    shutil.rmtree(SCRATCH, ignore_errors=True)
    if FAILURES:
        print("%d failure(s)" % len(FAILURES))
        return 1
    print("selftest_import: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
