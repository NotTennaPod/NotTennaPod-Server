#!/usr/bin/env python3
"""Feed parser vectors (PROTOCOL §9.2).

The expected outputs come from Android AntennaPod 3.12.1's own parser classes,
run unchanged on the Android 3.12.1 emulator by the instrumented exporter
GoldenDeviceExportTest in the NotTennaPod-Android checkout. This script only
builds the inputs and checks and files the outputs; it never decides a result.

  generate  writes the feed corpus to contract/vectors/feeds/ (Android's parser
            test feeds, NotTennaPod-iOS's FeedParserTests scenarios, the hostile
            corpus and seeded generated feeds) with SHA256SUMS, and the value
            inputs (dates, durations, chapter times, HTML, MIME) as JSON for the
            exporter.
  assemble  checks the exporter's output (one row per input, in order; no date
            inside the clock window) and writes expected.jsonl,
            feed_values.json, feed_date_symbols.json and PROVENANCE.md.
  compare   compares a JVM (Robolectric) export of the same corpus with the
            emulator's rows.

Standard library only. The same seed always gives the same corpus and inputs.
"""

import argparse
import datetime
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import sys
from pathlib import Path

DAY_MS = 86400000
YEAR_2101_MS = 4133980800000  # 2101-01-01T00:00:00Z: "after the year 2100"

# ---------------------------------------------------------------------------
# Namespaces (SyndHandler.startPrefixMapping)

NSURI = {
    "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "media": "http://search.yahoo.com/mrss/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "podcast": "https://podcastindex.org/namespace/1.0",
    "podcast_github": "https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md",
    "psc": "http://podlove.org/simple-chapters",
    "atom": "http://www.w3.org/2005/Atom",
}
ALT_PREFIX = {
    "itunes": ["it", "itunesu", "i"],
    "content": ["c", "cont"],
    "media": ["m", "mrss"],
    "dc": ["dcterms", "d"],
    "podcast": ["pi", "p2"],
    "podcast_github": ["pi", "p2", "podcastindex"],
    "psc": ["chapters", "pc"],
    "atom": ["a10", "at"],
}

# ---------------------------------------------------------------------------
# Small Java emulations used only to register derived inputs (never results)

JAVA_WS = " \t\n\x0b\f\r"


def java_trim_all(s):
    """SyndStringUtils.trimAllWhitespace for ordinary strings."""
    return s.strip(JAVA_WS)


def java_trim(s):
    """String.trim()."""
    i, j = 0, len(s)
    while i < j and ord(s[i]) <= 0x20:
        i += 1
    while j > i and ord(s[j - 1]) <= 0x20:
        j -= 1
    return s[i:j]


def filename_extension(url):
    """commons-io 2.5 FilenameUtils.getExtension."""
    dot = url.rfind(".")
    sep = max(url.rfind("/"), url.rfind("\\"))
    if sep > dot or dot == -1:
        return ""
    return url[dot + 1:]


# ---------------------------------------------------------------------------
# Value pools. Every date is either more than a day before the generation time
# or after 2100 (PROTOCOL §9.2): past years end in 2024 (field overflow is bounded
# to a few months), future years start in 2102, and two-digit years are 00-25
# or 48-99, so any two-digit prefix or SimpleDateFormat century window lands
# in the past. assemble() verifies this against the device's own results.

PAST_YEARS = list(range(1970, 2025)) + [1, 99, 999, 1066, 1601, 1752, 1899, 1900, 1950, 1969]
FUTURE_YEARS = list(range(2102, 2600)) + [9999, 10000, 99999, 123456, 200000]
YY = list(range(0, 26)) + list(range(48, 100))

SHORT_WD = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
LONG_WD = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
SHORT_MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
LONG_MON = ["January", "February", "March", "April", "May", "June", "July", "August",
            "September", "October", "November", "December"]

PATTERNS = [
    "dd MMM yy HH:mm:ss Z",
    "dd MMM yy HH:mm Z",
    "EEE, dd MMM yyyy HH:mm:ss Z",
    "EEE, dd MMM yyyy HH:mm:ss",
    "EEE, dd MMMM yyyy HH:mm:ss Z",
    "EEE, dd MMMM yyyy HH:mm:ss",
    "EEEE, dd MMM yyyy HH:mm:ss Z",
    "EEEE, dd MMM yy HH:mm:ss Z",
    "EEEE, dd MMM yyyy HH:mm:ss",
    "EEEE, dd MMM yy HH:mm:ss",
    "EEE MMM d HH:mm:ss yyyy",
    "EEE, dd MMM yyyy HH:mm Z",
    "EEE, dd MMM yyyy HH:mm",
    "EEE, dd MMMM yyyy HH:mm Z",
    "EEE, dd MMMM yyyy HH:mm",
    "EEEE, dd MMM yyyy HH:mm Z",
    "EEEE, dd MMM yy HH:mm Z",
    "EEEE, dd MMM yyyy HH:mm",
    "EEEE, dd MMM yy HH:mm",
    "EEE MMM d HH:mm yyyy",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS Z",
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ssZ",
    "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    "yyyy-MM-ddZ",
    "yyyy-MM-dd",
    "EEE d MMM yyyy HH:mm:ss 'GMT'Z (z)",
]

NUMERIC_ZONES = ["+0000", "-0000", "-0500", "+0530", "+1400", "-1200", "+0100", "+0200", "-0800", "+0945"]
ZONE_FORMS = [
    "+0000", "-0500", "+0530", "+1400", "-1200", "+00:00", "-05:00", "+05:30", "+01:00", "+01", "-05", "+1",
    "GMT", "GMT+01:00", "GMT-05:00", "GMT+1", "GMT+0100", "GMT+01", "GMT-0800", "UTC", "UTC+01:00", "UT", "Z",
    "z", "EST", "EDT", "CST", "CDT", "MST", "MDT", "PST", "PDT", "BST", "CET", "CEST", "IST", "JST", "AEST",
    "MSK", "HST", "AKST", "NZST", "WET", "EET", "EEST", "GMT+00:00", "Eastern Standard Time",
    "Pacific Daylight Time", "Central European Standard Time", "Central European Summer Time",
    "Greenwich Mean Time", "Coordinated Universal Time", "British Summer Time", "India Standard Time",
    "Japan Standard Time", "America/New_York", "Europe/Berlin", "XYZ", "+25:00", "+2400", "+0", "GMT+",
    "+12345", "--0500", "+-0100", "0000", "+00:0", "est", "pst", "gmt", "Gmt", "utc",
]
ABBREV_ZONES = ["EST", "EDT", "CST", "CDT", "MST", "MDT", "PST", "PDT", "BST", "CET", "CEST", "IST", "JST",
                "AEST", "GMT", "UTC", "UT", "MSK", "HST", "Eastern Standard Time", "Central European Standard Time",
                "Central European Summer Time", "Greenwich Mean Time", "Pacific Standard Time",
                "Coordinated Universal Time", "GMT+01:00"]
GERMAN_WD = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
GERMAN_MON = ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]
DATE_GARBAGE = [
    "", " ", "\t", "now", "Today", "yesterday", "0", "-1", "1700000000", "1700000000000", "null", "NaN", "2024",
    "Jan 2024", "Thu", ",", ", 01 Jan 2024 10:00:00 +0000", "Thu,01 Jan 2024 10:00:00 +0000", "T", "Z",
    "--", "2024-", "-01-01", "::", "Thu, ", "Thu, Jan", "Mon, 99 Foo 99999 99:99:99 +9999", "Thu, 01 Foo 2024 10:00:00 +0000",
    "Thu, 01 Jan 2024 10:00:00 +0000 +0000", "Thu, 01 Jan 2024 10:00:00 GMT GMT", "Thu, 01 Jan 2024 10::00 +0000",
    "Thu, 01 Jan 2024 10:00:00 +", "01/02/2003", "1/2/2003", "2003/01/02", "02.01.2003", "2003.01.02",
    "Jan. 15, 2024", "January 15, 2024", "Jan 15, 2024 10:00 AM", "15 January 2024", "Monday, January 15, 2024",
    "2024-01-15 10:00:00", "2024-01-15 10:00:00 +0000", "20240115T100000Z", "20240115", "2024-W03-1",
    "2024-015", "2024-01-15T10:00", "2024-01-15T10", "2024-01-15T10:00:00+01:00:00", "Thu., 01 Jan 2024 10:00:00 +0000",
    "Thu, 01. Jan 2024 10:00:00 +0000", "Thu, 01 Jan. 2024 10:00:00 +0000", "Thu 01 Jan 2024 10:00:00 +0000",
    "Thu, 1 Jan 2024 10:00:00 +0000", "Thu, 01 Jan 2024 1:00:00 +0000", "Thu, 01 Jan 2024 10:0:00 +0000",
    "Thu, 01 Jan 2024 10:00:0 +0000", "Thu, 01 Jan 2024 10:00:00+0000", "Thu, 01 Jan 2024T10:00:00 +0000",
    "Thu, 01-Jan-2024 10:00:00 GMT", "Thursday, 01-Jan-24 10:00:00 GMT", "Thu Jan  1 10:00:00 2024",
    "Thu Jan 01 2024 10:00:00 GMT+0100 (Central European Standard Time)", "Thu 1 Jan 2024 10:00:00 GMT+0100",
    "Thu 1 Jan 2024 10:00:00 GMT+0100 ()", "Thu 1 Jan 2024 10:00:00 GMT+0100 (XYZ)",
    "\u0662\u0660\u0662\u0664-\u0660\u0661-\u0661\u0665", "\uff12\uff10\uff12\uff14-\uff10\uff11-\uff11\uff15",
    "Thu, \u0660\u0661 Jan 2024 10:00:00 +0000", "Thu, 01 Jan \uff12\uff10\uff12\uff14 10:00:00 +0000",
    "Thu,\u00a001 Jan 2024 10:00:00 +0000", "Thu, 01 Jan 2024 10:00:00 \u2212" + "0500",
    "Sept", "Sept 2024", "Mon, 15 Sept 2024", "Sun, 15 Sept 2024 10:00:00 +0000x", "Thu, 01 Jan 2024 10:00:00 +0000\u0085",
    "Foo, Bar, 01 Jan 2024 10:00:00 +0000", "Foo, Thu, 01 Jan 2024 10:00:00 +0000", "Foo, 2024-01-15T10:00:00Z",
    "Foo,2024-01-15", "Foo, , 2024-01-15", "Foo, Bar", "Foo, ", "_, 2024-01-15", "Thu-Fri, 2024-01-15",
    "\u00e9t\u00e9, 2024-01-15", "Thu, 01 Jan 2024 25:61:61 +0000",
]
DATE_SUFFIXES = [" garbage", "x", " (CET)", " (Central European Standard Time)", "Z", " +0000", ";", ".",
                 " GMT GMT", " 2024", "\u00a0", " UTC", " -", " ()", "0", ":00", ".5", " PM", " AM"]
WHITESPACE = [" ", "  ", "\t", "\n", "\r\n", "\u00a0", "\u2003", " \t ", "\n  ", "\f", "\x0b"]


def date_pool_builder(rng):
    pool = []

    def add(s):
        pool.append(s)

    def weekday(y, mo, d):
        try:
            return datetime.date(2000 + y % 400, mo, d).weekday()
        except ValueError:
            return rng.randrange(7)

    def fields(future=None, out_of_range=False):
        if future is None:
            future = rng.random() < 0.2
        y = rng.choice(FUTURE_YEARS if future else PAST_YEARS)
        mo = rng.randint(1, 12)
        dmax = [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30,
                31, 30, 31][mo - 1]
        d = rng.randint(1, dmax)
        f = {"y": y, "mo": mo, "d": d, "h": rng.randint(0, 23), "mi": rng.randint(0, 59), "s": rng.randint(0, 59),
             "ms": rng.randint(0, 999), "yy": rng.choice(YY)}
        if out_of_range:
            k = rng.choice(["d", "d", "h", "mi", "s", "mo", "feb"])
            if k == "d":
                f["d"] = rng.choice([0, 32, 33, 99, dmax + 1])
            elif k == "h":
                f["h"] = rng.choice([24, 25, 99])
            elif k == "mi":
                f["mi"] = rng.choice([60, 61, 99])
            elif k == "s":
                f["s"] = rng.choice([60, 61, 99])
            elif k == "mo":
                f["mo_raw"] = rng.choice([0, 13, 99])
            else:
                f["mo"] = 2
                f["d"] = rng.choice([29, 30, 31])
        f["wd"] = weekday(f["y"], f["mo"], min(f["d"], 28) if f["d"] > 28 else max(f["d"], 1))
        return f

    def fmt(pattern, f, zone=None, wd=None, mon_case=None, yy=False):
        out = []
        i = 0
        while i < len(pattern):
            c = pattern[i]
            if c == "'":
                j = pattern.index("'", i + 1)
                out.append(pattern[i + 1:j])
                i = j + 1
                continue
            j = i
            while j < len(pattern) and pattern[j] == c:
                j += 1
            n = j - i
            if c == "E":
                out.append(wd if wd is not None else (LONG_WD if n >= 4 else SHORT_WD)[f["wd"]])
            elif c == "M":
                mo = f.get("mo_raw", f["mo"])
                if n >= 3:
                    names = LONG_MON if n >= 4 else SHORT_MON
                    name = names[(f["mo"] - 1) % 12]
                    if mon_case == "lower":
                        name = name.lower()
                    elif mon_case == "upper":
                        name = name.upper()
                    out.append(name)
                else:
                    out.append("%02d" % mo if n == 2 else str(mo))
            elif c == "d":
                out.append("%02d" % f["d"] if n == 2 else str(f["d"]))
            elif c == "y":
                if n == 2 and yy:
                    out.append("%02d" % f["yy"])
                else:
                    out.append(str(f["y"]).zfill(4))
            elif c == "H":
                out.append("%02d" % f["h"] if n == 2 else str(f["h"]))
            elif c == "m":
                out.append("%02d" % f["mi"])
            elif c == "s":
                out.append("%02d" % f["s"])
            elif c == "S":
                out.append("%03d" % f["ms"])
            elif c == "Z":
                out.append(zone if zone is not None else rng.choice(NUMERIC_ZONES))
            elif c == "z":
                out.append(zone if zone is not None else rng.choice(ABBREV_ZONES))
            else:
                out.append(c * n)
            i = j
        return "".join(out)

    rfc = "EEE, dd MMM yyyy HH:mm:ss Z"
    iso = "yyyy-MM-dd'T'HH:mm:ss"

    # every strict pattern, with four- and two-digit years where it has "yy"
    for pattern in PATTERNS:
        for k in range(48):
            yy = "yy " in pattern and k % 2 == 0
            add(fmt(pattern, fields(), yy=yy))
    # every zone form with several layouts
    for zone in ZONE_FORMS:
        for layout in [rfc, "dd MMM yyyy HH:mm:ss Z", "EEE, dd MMM yyyy HH:mm Z", "EEEE, dd MMM yyyy HH:mm:ss Z",
                       "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "EEE d MMM yyyy HH:mm:ss 'GMT'Z (z)"]:
            f = fields()
            if layout.endswith("(z)"):
                add(fmt(layout, f, zone=None).rsplit(" (", 1)[0] + " (" + zone + ")")
            else:
                add(fmt(layout, f, zone=zone))
        add(fmt(iso, fields()) + zone)
        add(fmt(iso, fields()) + "." + "1234567"[:rng.randint(0, 7)] + zone)
        add(fmt("yyyy-MM-dd", fields()) + zone)
        add(fmt("EEE MMM d HH:mm:ss yyyy", fields()) + " " + zone)
    # weekday forms: wrong, long, case, foreign, abbreviated, missing comma
    for k in range(400):
        f = fields()
        choice = k % 10
        if choice == 0:
            wd = SHORT_WD[(f["wd"] + rng.randint(1, 6)) % 7]
        elif choice == 1:
            wd = LONG_WD[f["wd"]]
        elif choice == 2:
            wd = SHORT_WD[f["wd"]].lower()
        elif choice == 3:
            wd = SHORT_WD[f["wd"]].upper()
        elif choice == 4:
            wd = GERMAN_WD[f["wd"]]
        elif choice == 5:
            wd = rng.choice(["Thurs", "Tues", "Thu.", "Weds", "Sat.", "Mo", "Su", "X", "Tuesday's", "Lundi"])
        elif choice == 6:
            wd = LONG_WD[(f["wd"] + rng.randint(1, 6)) % 7]
        elif choice == 7:
            wd = LONG_WD[f["wd"]].upper()
        elif choice == 8:
            wd = SHORT_WD[f["wd"]]
            add(fmt("EEE dd MMM yyyy HH:mm:ss Z", f, wd=wd))
            continue
        else:
            wd = SHORT_WD[f["wd"]]
            add(fmt("EEE,dd MMM yyyy HH:mm:ss Z", f, wd=wd))
            continue
        add(fmt(rng.choice([rfc, "EEEE, dd MMM yyyy HH:mm:ss Z", "EEE, dd MMM yyyy HH:mm Z", "EEE, dd MMMM yyyy HH:mm:ss",
                            "EEE MMM d HH:mm:ss yyyy", "EEE d MMM yyyy HH:mm:ss 'GMT'Z (z)"]), f, wd=wd))
    # month forms: lower/upper case, long, Sept, foreign
    for k in range(360):
        f = fields()
        layout = rng.choice([rfc, "dd MMM yyyy HH:mm:ss Z", "EEE, dd MMMM yyyy HH:mm:ss Z", "EEE MMM d HH:mm:ss yyyy",
                             "dd MMM yy HH:mm Z"])
        case = k % 6
        if case == 0:
            add(fmt(layout, f, mon_case="lower", yy="yy " in layout))
        elif case == 1:
            add(fmt(layout, f, mon_case="upper", yy="yy " in layout))
        elif case == 2:
            f["mo"] = 9
            s = fmt(layout, f, yy="yy " in layout)
            add(re.sub(r"\bSep(tember)?\b", rng.choice(["Sept", "sept", "SEPT", "Sept.", "Septem"]), s))
        elif case == 3:
            s = fmt(rfc, f)
            add(s.replace(SHORT_MON[f["mo"] - 1], GERMAN_MON[f["mo"] - 1]))
        elif case == 4:
            add(fmt(layout.replace("MMM", "MMMM") if "MMMM" not in layout else layout, f, yy="yy " in layout))
        else:
            add(fmt(layout, f, mon_case=rng.choice(["lower", "upper"]), yy="yy " in layout))
    # two-digit years
    for yy in YY:
        for layout in ["dd MMM yy HH:mm:ss Z", "EEEE, dd MMM yy HH:mm Z", "EEE, dd MMM yy HH:mm:ss Z", "dd MMM yy HH:mm Z"]:
            f = fields(future=False)
            f["yy"] = yy
            add(fmt(layout, f, yy=True))
    # out-of-range fields: lenient first pass versus strict patterns
    for k in range(480):
        f = fields(out_of_range=True)
        layout = rng.choice([rfc, rfc, "EEE, dd MMM yyyy HH:mm Z", "dd MMM yyyy HH:mm:ss Z", "EEEE, dd MMM yyyy HH:mm:ss",
                             iso, "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd",
                             "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "EEE MMM d HH:mm:ss yyyy"])
        add(fmt(layout, f))
    # fractional seconds of 0-9 digits
    for digits in range(10):
        for tail in ["", "Z", "z", "+00:00", "-0500", " +0000", "+05:30", "+01", " Z"]:
            f = fields()
            frac = "".join(str(rng.randint(0, 9)) for _ in range(digits))
            add(fmt(iso, f) + "." + frac + tail)
        for layout in [rfc, "dd MMM yyyy HH:mm:ss Z"]:
            f = fields()
            s = fmt(layout, f)
            head, zone = s.rsplit(" ", 1)
            add(head + "." + "".join(str(rng.randint(0, 9)) for _ in range(digits)) + " " + zone)
    # normalizations: '/' separators, repeated spaces, ±hh:mm, CEST/CET, '.'
    for k in range(360):
        f = fields()
        c = k % 6
        if c == 0:
            add(fmt(rng.choice([iso, "yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm:ssZ"]), f).replace("-", "/"))
        elif c == 1:
            add(fmt(rfc, f).replace(" ", rng.choice(["  ", "   ", " \t"]), rng.randint(1, 4)))
        elif c == 2:
            add(fmt(rfc, f, zone=rng.choice(["+01:00", "-05:30", "+00:00", "+14:00", "-12:00"])))
        elif c == 3:
            add(fmt(rng.choice([rfc, "dd MMM yyyy HH:mm:ss Z", "yyyy-MM-dd'T'HH:mm:ss"]), f, zone=rng.choice(["CEST", "CET"]))
                + ("" if rng.random() < 0.8 else rng.choice(["CET", "CEST"])))
        elif c == 4:
            add(fmt(iso, f) + rng.choice(["CET", "CEST", " CET", "+02:00", "-01:00"]))
        else:
            s = fmt(rfc, f)
            add(s.replace(":", ".", rng.randint(1, 2)) if rng.random() < 0.5 else s.replace(",", ".,", 1))
    # leading and trailing whitespace
    for k in range(240):
        s = fmt(rng.choice([rfc, iso, "yyyy-MM-dd'T'HH:mm:ssZ", "dd MMM yyyy HH:mm:ss Z"]), fields())
        add(rng.choice(["", rng.choice(WHITESPACE)]) + s + rng.choice(WHITESPACE))
    # trailing garbage after a valid prefix
    for k in range(320):
        s = fmt(rng.choice([rfc, iso, "yyyy-MM-dd'T'HH:mm:ssZ", "EEE, dd MMM yyyy HH:mm Z", "yyyy-MM-dd"]), fields())
        add(s + rng.choice(DATE_SUFFIXES))
    # truncations of valid strings
    for k in range(240):
        s = fmt(rng.choice([rfc, iso, "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "EEE d MMM yyyy HH:mm:ss 'GMT'Z (z)"]), fields())
        add(s[:rng.randint(1, len(s) - 1)])
    # the weekday-stripping retry: "<word>, <rest>"
    for k in range(200):
        f = fields()
        rest = fmt(rng.choice([iso, "yyyy-MM-dd", "dd MMM yyyy HH:mm:ss Z", "yyyy-MM-dd'T'HH:mm:ss'Z'", rfc,
                               "EEE MMM d HH:mm:ss yyyy", "dd MMM yy HH:mm Z"]), f, yy=True)
        word = rng.choice(["Foo", "Thu", "Donnerstag", "x", "Day1", "_", "Thursday", "\u00e9t\u00e9"])
        add(word + rng.choice([", ", ",  ", ", \t"]) + rest)
    # fixed garbage and junk without digits
    for s in DATE_GARBAGE:
        add(s)
    letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ,.:+-/()TZ\u00e9"
    for k in range(200):
        add("".join(rng.choice(letters) for _ in range(rng.randint(1, 30))))
    # dates far in the past and future in the commonest forms
    for k in range(200):
        add(fmt(rng.choice([rfc, iso + "'Z'", "yyyy-MM-dd'T'HH:mm:ssZ"]), fields(future=k % 2 == 0)))
    return pool


DURATION_FORMS = [
    "1:02:03", "62:03", "3723", "3723.5", "0", "00:00:00", "", " 12:34 ", "\n45:00\n", "1:2:3:4", "abc", "1e3",
    "-5", "+7", "99999999999", "2147483.648", "2147483.647", "596:31:23.647", "596:31:23.648", "0x1.8p2", "1.2f",
    "NaN", "Infinity", "12:34:56.789", "1:60:60", "01:02", ":30", "30:", "\u0661:\u0662\u0663", "1.5.5", "1:02:03.5",
    "1.5:00", "::", " ", "\t1:00\n", "1:00:00:00", "35791:23:47", "35791:23:48", "2562047788015215:30:07",
    "9223372036854775807", "-9223372036854775808", "00:45", "45", "4500", "1:30", "01:30:00", "1h30m", "PT1H30M",
    "90 min", "1:30 ", " 1:30", "1 :30", "１:３０", "0.5", ".5", "5.", "-0", "-0.5", "1:-1", "-1:00:00", "1e2",
    "1.2e2", "1.0e99", "3.4028235e38", "1E3:00", "00:00:01.999999", "100:00:00",
]
TIME_FORMS = [
    "00:01:02.5", "1:02", "62", "bad:time", "1:2:3:4", "00:00:00", "0", "-1:00", "1.5e3", " 1:00", "1:00 ",
    "01:02:03.123456", "2147483647:00:00", "2147483648:00:00", "99:59:59.999", "1:02.5x", "NaN", "Infinity:00",
    "0:0:0.0001", "12:34:56", "\u0661:\u0660\u0662", "+1:00", "0x10:00", "1:1e2", "1:00:NaN", "1:00:Infinity",
    "00:00:59.9999", "1::", ":", "::", "1:2:", "abc", "", " ", "00:00:00.000", "35791:23:47.647",
    "596523:14:07.999", "-2147483648:00:00", "1:0x1p3", "1:1.5f", "1:2d", "0:0:3.4028235e38", "00:01:02,5",
    "\uff11:\uff10\uff10", "01:02:03:04:05", "1:00:00.5", "00:30", "1:30:00", "10:00:00.000001",
]
HTML_ATOMS = [
    "Tom & Jerry <3", "a < b", "a > b", "1 <2", "x<y", "<3 <3", "R&D", "AT&T", "&amp;", "&amp;amp;", "&lt;b&gt;",
    "&#169;", "&#xA9;", "&#x1F600;", "&#128512;", "&copy;", "&nbsp;", "&hellip;", "&mdash;", "&euro;", "&unknown;",
    "&#0;", "&#xD800;", "&#xFFFF;", "&#65;&#66;", "&quot;q&quot;", "&apos;a&apos;", "&", "&;", "&#;", "&#x;",
    "&amp", "&lt", "&AMP;", "&Lt;", "caf\u00e9", "\u8336", "\U0001F600", "\u05e9\u05dc\u05d5\u05dd",
    "\u0645\u0631\u062d\u0628\u0627", "e\u0301", "\u00a0", "\u200b", "\ufeff", "plain words", "Episode 12",
    "  spaced  ", "multi\nline", "tab\there", "cr\rlf", "a\r\nb", "trailing ", " leading", "\u2028sep", "[x]",
    "100%", "\"quoted\"", "'single'", "back\\slash", "{json: 1}", "#hash", "a=b&c=d", "http://example.com/?a=1&b=2",
]
HTML_WRAPS = [
    "<b>%s</b>", "<i>%s</i>", "<u>%s</u>", "<strong>%s</strong>", "<em>%s</em>", "<p>%s</p>", "<div>%s</div>",
    "<h1>%s</h1>", "<h3>%s</h3>", "<h6>%s</h6>", "<blockquote>%s</blockquote>", "<ul><li>%s</li><li>two</li></ul>",
    "<ol><li>%s</li></ol>", "<li>%s</li>", "<a href=\"https://example.com/x?a=1&amp;b=2\">%s</a>",
    "<font color=\"red\">%s</font>", "<span style=\"color:red\">%s</span>", "<sup>%s</sup>", "<sub>%s</sub>",
    "<big>%s</big>", "<small>%s</small>", "<tt>%s</tt>", "<del>%s</del>", "<s>%s</s>", "<strike>%s</strike>",
    "<cite>%s</cite>", "<dfn>%s</dfn>", "<code>%s</code>", "<pre>%s\n  pre</pre>", "<P>%s</P>", "<B>%s</b>",
    "<p style=\"text-align:center\">%s</p>", "<div dir=\"rtl\">%s</div>", "<unknown>%s</unknown>",
    "<table><tr><td>%s</td><td>b</td></tr></table>", "<center>%s</center>", "<dl><dt>%s</dt><dd>b</dd></dl>",
    "<b>%s", "%s</p>", "<p>%s", "<b><i>%s</b></i>", "<p><p>%s</p></p>", "<div><p>%s</p></div>",
    "<span>%s</span>", "<label>%s</label>", "<section>%s</section>", "<article>%s</article>",
]
HTML_SINGLES = [
    "<br>", "<br/>", "<br />", "<BR>", "<br></br>", "<img src=\"x.png\">", "<img src=\"x\" alt=\"y\"/>",
    "<img>", "<hr>", "<hr/>", "<p></p>", "<p/>", "</p>", "<p", "<<>>", "<>", "< p>", "</>", "<!-- comment -->",
    "<!-- unterminated", "<![CDATA[x]]>", "<?php echo 1; ?>", "<!DOCTYPE html>", "<script>alert(1)</script>",
    "<style>p{color:red}</style>", "<title>t</title>", "<head><title>t</title></head><body>b</body>",
    "<html><body>x</body></html>", "<meta charset=\"utf-8\">", "<input value=\"v\">", "<textarea>t</textarea>",
    "<iframe src=\"x\"></iframe>", "<object>o</object>", "<svg><text>s</text></svg>", "<math>m</math>",
    "<a name=\"x\"></a>", "<wbr>", "<nobr>n</nobr>", "<xmp><b>x</b></xmp>", "<plaintext>", "<listing>l</listing>",
]


def html_pool_builder(rng):
    pool = []
    for s in HTML_ATOMS + HTML_SINGLES:
        pool.append(s)
    for w in HTML_WRAPS:
        pool.append(w % rng.choice(HTML_ATOMS))
        pool.append(w % "text")
    for ws in WHITESPACE + ["\n\n", " \n ", "\t\t", "\r"]:
        pool.append("a" + ws + "b")
        pool.append(ws + "<p>x</p>" + ws)
        pool.append("<p>" + ws + "x" + ws + "</p>" + ws + "<p>y</p>")

    def piece(depth):
        r = rng.random()
        if r < 0.35 or depth > 2:
            return rng.choice(HTML_ATOMS)
        if r < 0.5:
            return rng.choice(HTML_SINGLES)
        if r < 0.62:
            return rng.choice(WHITESPACE + ["\n\n", " "])
        inner = "".join(piece(depth + 1) for _ in range(rng.randint(1, 3)))
        return rng.choice(HTML_WRAPS) % inner

    for k in range(2600):
        pool.append("".join(piece(0) for _ in range(rng.randint(1, 6))))
    pool += ["\ud800", "a\udc00b", "\ud83dx", "<b>\udfff</b>", "\U0001F600\ud800"]
    return pool


ENC_TYPES = ["audio/mpeg", "audio/mp3", "audio/x-m4a", "audio/mp4", "audio/aac", "audio/ogg", "audio/opus",
             "audio/wav", "audio/x-wav", "audio/flac", "audio/*", "video/mp4", "video/webm", "video/quicktime",
             "video/x-matroska", "application/ogg", "application/octet-stream", "image/jpeg", "image/png", "image/*",
             "text/html", "text/plain", "application/pdf", "unknown", "", None, "Audio/MPEG", "AUDIO/MPEG", "audio",
             "audio/", " audio/mpeg", "audio/mpeg; charset=binary", "application/x-bittorrent", "application/json",
             "video", "x-audio/mpeg"]
URL_EXTS = ["mp3", "m4a", "m4b", "ogg", "oga", "opus", "mp4", "m4v", "webm", "mkv", "flac", "wav", "aac", "3gp",
            "mov", "avi", "wma", "jpg", "png", "pdf", "txt", "html", "torrent", "MP3", "Mp4", "OGG", "xyz", "",
            "mp3.zip", "php", "aspx", "mpga", "ogx", "amr", "mid", "ts", "m3u8", "caf", "aif"]
SIZES = [None, "", "0", "1", "12", "16383", "16384", "16385", "123456", "52428800", "2147483647", "2147483648",
         "4294967296", "9007199254740991", "9007199254740992", "9007199254740993", "123456789012345678",
         "9223372036854775807", "9223372036854775808", "-1", "-16384", "-9223372036854775808", " 20000", "20000 ",
         "+20000", "2e5", "20000.0", "0x4E20", "abc", "\u0662\u0660\u0660\u0660\u0660",
         "\uff12\uff10\uff10\uff10\uff10\uff10", "00020000", "1,000,000", "-0", "18446744073709551616"]
MEDIA_DURATIONS = [None, "", "0", "1", "59", "3600", "-5", "2147483", "2147484", "2147483648", "9223372036854775807",
                   "1.5", "abc", "01:00", " 60", "+60", "0060"]


def url_pool(rng, n):
    hosts = ["https://cdn.example.com", "http://example.org", "https://user:pass@media.example.net",
             "https://example.com:8443", "HTTPS://EXAMPLE.COM", "https://xn--caf-dma.example", "//example.com",
             "ftp://files.example.com", "https://example.com/a.b"]
    out = []
    for i in range(n):
        ext = rng.choice(URL_EXTS)
        path = rng.choice(["ep%d" % i, "dir.with.dots/ep%d" % i, "e p%d" % i, "caf\u00e9/%d" % i, "%d" % i, "a/b/c%d" % i])
        u = rng.choice(hosts) + "/" + path + ("." + ext if ext else "")
        r = rng.random()
        if r < 0.15:
            u += "?" + rng.choice(["auth=yes", "a=1&b=2", "file=x.mp3", "dl=1.0"])
        elif r < 0.2:
            u += "#t=10"
        elif r < 0.23:
            u = " " + u + " "
        out.append(u)
    return out


EXTENSION_CANDIDATES = """
123 1km 3dml 3ds 3g2 3gp 3gpp 3gpp2 3mf 7z aab aac aam aas abw ac acc ace acu acutc adp aep afm afp ahead ai aif aifc
aiff air ait ami amr apk appcache application apr arc arj asc asf asm aso asx atc atom atomcat atomsvc atx au avi avif
aw azf azs azw bat bcpio bdf bdm bed bh2 bin blb blorb bmi bmp book box boz bpk btif bz bz2 c c11amc c11amz c4d c4f c4g
c4p c4u cab caf cap car cat cb7 cba cbr cbt cbz cc cct ccxml cdbcmsg cdf cdkey cdmia cdmic cdmid cdmio cdmiq cdx cdxml
cdy cer cfs cgm chat chm chrt cif cii cil cla class clkk clkp clkt clkw clkx clp cmc cmdf cml cmp cmx cod com conf cpio
cpp cpt crd crl crt cryptonote csh csml csp css cst csv cu curl cww cxt cxx dae daf dart dataless davmount dbk dcr dcurl
dd2 ddd deb def deploy der dfac dgc dic dir dis dist distz djv djvu dll dmg dmp dms dna doc docm docx dot dotm dotx dp
dpg dra dsc dssc dtb dtd dts dtshd dump dvb dvi dwf dwg dxf dxp dxr ecelp4800 ecelp7470 ecelp9600 ecma edm edx efif ei6
elc emf eml emma emz eol eot eps epub es es3 esa esf et3 etx eva evy exe exi ext ez ez2 ez3 f f4a f4b f4v f77 f90 fbs
fcdt fcs fdf fe_launch fg5 fgd fh fh4 fh5 fh7 fhc fig flac fli flo flv flw flx fly fm fnc for fpx frame fsc fst ftc fti
fvt fxp fxpl fzs g2w g3 g3w gac gam gbr gca gdl geo gex ggb ggt ghf gif gim gml gmx gnumeric gph gpx gqf gqs gram gramps
gre grv grxml gsf gtar gtm gtw gv gxf gxt h h261 h263 h264 hal hbci hdf heic heif hh hlp hpgl hpid hps hqx htke htm html
hvd hvp hvs i2g icc ice icm ico ics ief ifb ifm iges igl igm igs igx iif imp ims imy in ink inkml install iota ipfix ipk
irm irp iso itp ivp ivu jad jam jar java jisp jlt jnlp joda jpe jpeg jpg jpgm jpgv jpm js json jsonml jxl kar karbon kfo
kia kml kmz kne knp kon kpr kpt kpxx ksp ktr ktx ktz kwd kwt lasxml latex lbd lbe les lha link66 list list3820 listafp
lnk log lostxml lrf lrm ltf lvp lwp lzh m13 m14 m1v m21 m2a m2v m3a m3u m3u8 m4a m4b m4u m4v ma mads mag maker man mar
mathml mb mbk mbox mc1 mcd mcurl mdb mdi me mesh meta4 metalink mets mfm mft mgp mgz mid midi mie mif mime mj2 mjp2 mjs
mk3d mka mks mkv mlp mmd mmf mmr mng mny mobi mods mov movie mp2 mp21 mp2a mp3 mp4 mp4a mp4s mp4v mpc mpe mpeg mpg mpg4
mpga mpkg mpm mpn mpp mpt mpy mqy mrc mrcx ms mscml mseed mseq msf msh msi msl msty mts mus musicxml mvb mwf mxf mxl
mxmf mxml mxs mxu n-gage n3 nb nbp nc ncx nfo ngdat nitf nlu nml nnd nns nnw npx nsc nsf ntf nzb oa2 oa3 oas obd obj oda
odb odc odf odft odg odi odm odp ods odt oga ogg ogv ogx omdoc onepkg onetmp onetoc onetoc2 opf opml oprc opus org osf
osfpvg ota otc otf otg oth oti otp ots ott oxps oxt p p10 p12 p7b p7c p7m p7r p7s p8 pas paw pbd pbm pcap pcf pcl pclxl
pct pcurl pcx pdb pdf pfa pfb pfm pfr pfx pgm pgn pgp pic pkg pki pkipath plb plc plf pls pml png pnm portpkg pot potm
potx ppam ppd ppm pps ppsm ppsx ppt pptm pptx pqa prc pre prf ps psb psd psf pskcxml ptid pub pvb pwn pya pyv qam qbo
qfx qps qt qwd qwt qxb qxd qxl qxt ra ram rar ras rcprofile rdf rdz rep res rgb rif rip ris rl rlc rld rm rmi rmp rms
rmvb rnc roa roff rp9 rpss rpst rq rs rsd rss rtf rtttl rtx s s3m saf sbml sc scd scm scq scs scurl sda sdc sdd sdkd sdkm
sdp sdw see seed sema semd semf ser setpay setreg sfd-hdstx sfs sfv sgi sgl sgm sgml sh shar shf sid sig sil silo sis
sisx sit sitx skd skm skp skt sldm sldx slt sm smf smi smil smv smzip snd snf so spc spf spl spot spp spq spx sql src srt
sru srx ssdl sse ssf ssml st stc std stf sti stk stl str stw sub sus susp sv4cpio sv4crc svc svd svg svgz swa swf swi
sxc sxd sxg sxi sxm sxw t t3 taglet tao tar tcap tcl teacher tei teicorpus tex texi texinfo text tfi tfm tga thmx tif
tiff tmo toml torrent tpl tpt tr tra trm ts tsd tsv ttc ttf ttl ttml twd twds txd txf txt u32 udeb ufd ufdl ulx umj
unityweb uoml uri uris urls ustar utz uu uva uvd uvf uvg uvh uvi uvm uvp uvs uvt uvu uvv uvva uvvd uvvf uvvg uvvh uvvi
uvvm uvvp uvvs uvvt uvvu uvvv uvvx uvvz uvx uvz vcard vcd vcf vcg vcs vcx vis viv vob vor vox vrml vsd vsf vss vst vsw
vtt vtu vxml w3d wad wasm wav wax wbmp wbs wbxml wcm wdb wdp weba webm webmanifest webp wg wgt wks wm wma wmd wmf wml
wmlc wmls wmlsc wmv wmx wmz woff woff2 wpd wpl wps wqd wri wrl wsdl wspolicy wtb wvx x32 x3d x3db x3dbz x3dv x3dvz x3dz
xaml xap xar xbap xbd xbm xdf xdm xdp xdssc xdw xenc xer xfdf xfdl xht xhtml xhvml xif xla xlam xlc xlf xlm xls xlsb
xlsm xlsx xlt xltm xltx xlw xm xml xo xop xpi xpl xpm xpr xps xpw xpx xsl xslt xsm xspf xul xvm xvml xwd xyz xz yang yin
z1 z2 z3 z4 z5 z6 z7 z8 zaz zip zir zirz zmm
""".split()
EXTENSION_EXTRA = ["", ".", "MP3", "Mp3", "OPUS", "M4A", "JPG", "PDF", "mp3?x=1", "mp3#t", "mp3 ", " mp3", "mp3.",
                   "tar.gz", "\u00e9", "MP4", "WebM", "FLAC", "Ogg", "aac ", "m4a?auth=yes"]


# ---------------------------------------------------------------------------
# XML serialization helpers


def esc_text(rng, s, refs=True):
    out = []
    for i, ch in enumerate(s):
        if ch == "&":
            out.append(rng.choice(["&amp;", "&amp;", "&#38;", "&#x26;"]) if refs else "&amp;")
        elif ch == "<":
            out.append(rng.choice(["&lt;", "&lt;", "&#60;", "&#x3C;"]) if refs else "&lt;")
        elif ch == ">":
            out.append("&gt;" if (i >= 2 and s[i - 2:i] == "]]") or rng.random() < 0.6 else ">")
        elif ch == "\r":
            out.append("&#13;")
        elif refs and ord(ch) > 0x7f and rng.random() < 0.15:
            out.append(rng.choice(["&#%d;" % ord(ch), "&#x%X;" % ord(ch)]))
        elif refs and ch in "\"'" and rng.random() < 0.3:
            out.append({"\"": "&quot;", "'": "&apos;"}[ch])
        else:
            out.append(ch)
    return "".join(out)


def ser_text(rng, s):
    """Serializes text content: escaped, CDATA or a mix of both."""
    if "\r" in s or "]]>" in s:
        return esc_text(rng, s)
    r = rng.random()
    if r < 0.55 or not s:
        return esc_text(rng, s)
    if r < 0.85:
        return "<![CDATA[" + s + "]]>"
    cut = rng.randint(0, len(s))
    if rng.random() < 0.5:
        return esc_text(rng, s[:cut]) + "<![CDATA[" + s[cut:] + "]]>"
    return "<![CDATA[" + s[:cut] + "]]>" + esc_text(rng, s[cut:])


def ser_attr(rng, s):
    q = "\"" if rng.random() < 0.85 else "'"
    out = []
    for ch in s:
        if ch == "&":
            out.append("&amp;")
        elif ch == "<":
            out.append("&lt;")
        elif ch == q:
            out.append("&quot;" if q == "\"" else "&apos;")
        elif ch in "\n\t\r":
            out.append("&#%d;" % ord(ch) if rng.random() < 0.7 else ch)
        else:
            out.append(ch)
    return q + "".join(out) + q


# ---------------------------------------------------------------------------
# Generated feeds

TITLE_ATOMS = HTML_ATOMS + ["Episode %d", "Folge %d: \u00dcber alles", "\u7b2c%d\u96c6", "#%d \u2013 Interview",
                            "S01E%d", "<b>Bold</b> title", "<p>Para</p>", "Line<br>break", "<img src=\"x.png\"> cover",
                            "Tom &amp; Jerry &lt;3", "&lt;b&gt;escaped&lt;/b&gt;", "a\n  b", "  padded  "]


class Ctx:
    def __init__(self, rng, generator, root_kind):
        self.rng = rng
        self.gen = generator
        self.root_kind = root_kind
        self.modes = {}
        self.pref = {}
        self.uri = {}
        self.root_decls = []
        self.channel_decls = []
        self.atom_ns_mode = None

    def setup_namespaces(self):
        rng = self.rng
        for key in ["itunes", "content", "media", "dc", "podcast", "psc", "atom"]:
            if key == "atom" and self.root_kind == "atom":
                continue
            r = rng.random()
            mode = ("root" if r < 0.62 else "absent" if r < 0.73 else "alt" if r < 0.81 else "both" if r < 0.87
                    else "channel" if r < 0.92 else "local" if r < 0.996 else "undeclared")
            if mode == "channel" and self.root_kind != "rss":
                mode = "root"
            uri_key = key
            canonical = key
            if key == "podcast" and rng.random() < 0.25:
                uri_key = "podcast_github"
                canonical = rng.choice(["podcast", "pi", "podcastindex", "p"])
            if key == "psc":
                canonical = rng.choice(["psc", "psc", "sc"])
            alt = rng.choice(ALT_PREFIX[uri_key])
            uri = NSURI[uri_key]
            self.modes[key] = mode
            self.uri[key] = uri
            if mode in ("root", "channel", "local", "undeclared"):
                self.pref[key] = [canonical]
            elif mode == "alt":
                self.pref[key] = [alt]
            elif mode == "both":
                self.pref[key] = [canonical, alt]
            decls = []
            if mode in ("root", "channel"):
                decls = [(canonical, uri)]
            elif mode == "alt":
                decls = [(alt, uri)]
            elif mode == "both":
                decls = [(canonical, uri), (alt, uri)]
                rng.shuffle(decls)
            (self.channel_decls if mode == "channel" else self.root_decls).extend(decls)

    def has(self, key):
        return self.modes.get(key, "absent") != "absent"

    def el(self, key, name, attrs=(), content=None):
        rng = self.rng
        decl = ""
        if key is None:
            qn = name
        elif key == "atomfeed":
            prefix = rng.choice(self.atom_feed_prefixes)
            qn = prefix + ":" + name if prefix else name
        else:
            prefix = rng.choice(self.pref[key])
            qn = prefix + ":" + name
            if self.modes[key] == "local":
                decl = " xmlns:%s=\"%s\"" % (prefix, self.uri[key])
        a = "".join(" %s=%s" % (k, ser_attr(rng, v)) for k, v in attrs if v is not None)
        if content is None:
            return "<%s%s%s/>" % (qn, decl, a) if rng.random() < 0.9 else "<%s%s%s></%s>" % (qn, decl, a, qn)
        return "<%s%s%s>%s</%s>" % (qn, decl, a, content, qn)

    def text(self, kind):
        s = self.gen.text(kind)
        return s, ser_text(self.rng, s)


class FeedGenerator:
    def __init__(self, rng, pools):
        self.rng = rng
        self.pools = pools  # dict: dates (list), registers

    # -- text -----------------------------------------------------------------
    def text(self, kind):
        rng = self.rng
        if kind == "title":
            n = rng.randint(1, 3)
            parts = []
            for _ in range(n):
                a = rng.choice(TITLE_ATOMS)
                if "%d" in a:
                    a = a % rng.randint(0, 999)
                if rng.random() < 0.15:
                    a = rng.choice(HTML_WRAPS) % a
                parts.append(a)
            s = rng.choice(["", " ", "  ", "\n"]).join(parts)
            if rng.random() < 0.1:
                s = rng.choice(WHITESPACE[:5]) + s + rng.choice(WHITESPACE[:5])
            s = s.replace("\ufeff", "").replace("\u2028", "")
        elif kind == "desc":
            n = rng.choice([0, 1, 1, 2, 3, 5, 8])
            parts = []
            for _ in range(n):
                r = rng.random()
                if r < 0.4:
                    parts.append(rng.choice(["<p>%s</p>", "%s", "<div>%s</div>", "%s<br>", "<li>%s</li>"]) %
                                 rng.choice(TITLE_ATOMS).replace("%d", "7"))
                elif r < 0.6:
                    parts.append(rng.choice(HTML_SINGLES[:12]))
                elif r < 0.75:
                    parts.append(rng.choice(["\U0001F600" * rng.randint(1, 4), "a" * rng.randint(1, 9), "\u00e9" * 3]))
                else:
                    parts.append(rng.choice(WHITESPACE[:6]) + rng.choice(HTML_ATOMS))
            s = "".join(parts)
            s = s.replace("\ufeff", "").replace("\u2028", "").replace("\r", "\n" if rng.random() < 0.5 else "\r")
        elif kind == "author":
            s = rng.choice(["Jane Doe", "Jane &amp; John", "<b>Studio</b>", "Caf\u00e9 Radio", "  Spaced  ", "A, B",
                            "&copy; 2020 Org", "\u7b2c\u4e00", "Tom & Jerry <3", "\U0001F600 Pod", ""])
        else:
            raise ValueError(kind)
        self.pools["html"].append(s)
        self.pools["html"].append(java_trim_all(s))
        return s

    def date(self):
        rng = self.rng
        s = rng.choice(self.pools["feed_dates"])
        if rng.random() < 0.08:
            s = rng.choice(WHITESPACE[:5]) + s + rng.choice(WHITESPACE[:5])
        self.pools["dates"].append(s)
        self.pools["dates"].append(java_trim_all(s))
        return s

    def duration(self):
        rng = self.rng
        s = rng.choice(DURATION_FORMS) if rng.random() < 0.4 else rng.choice([
            "%d:%02d:%02d" % (rng.randint(0, 5), rng.randint(0, 59), rng.randint(0, 59)),
            "%02d:%02d" % (rng.randint(0, 200), rng.randint(0, 59)),
            "%d" % rng.randint(0, 20000),
            "%d.%d" % (rng.randint(0, 9999), rng.randint(0, 999)),
        ])
        self.pools["durations"].append(s)
        return s

    def chapter_time(self):
        rng = self.rng
        s = rng.choice(TIME_FORMS) if rng.random() < 0.35 else rng.choice([
            "%02d:%02d:%02d.%03d" % (rng.randint(0, 3), rng.randint(0, 59), rng.randint(0, 59), rng.randint(0, 999)),
            "%02d:%02d:%02d" % (rng.randint(0, 3), rng.randint(0, 59), rng.randint(0, 59)),
            "%d:%02d" % (rng.randint(0, 120), rng.randint(0, 59)),
            "%d" % rng.randint(0, 9999),
        ])
        self.pools["times"].append(s)
        return s

    def media_url(self):
        u = self.rng.choice(self.pools["urls"])
        return u

    def mime(self, t, u):
        self.pools["mime"].append([t, u])
        if u is not None:
            self.pools["extensions"].append(filename_extension(u))

    # -- elements -----------------------------------------------------------------
    def enclosure(self, c):
        rng = self.rng
        u = rng.choice([self.media_url(), self.media_url(), self.media_url(), None, ""])
        t = rng.choice(ENC_TYPES)
        self.mime(t, u)
        return c.el(None, "enclosure", [("url", u), ("length", rng.choice(SIZES)), ("type", t)])

    def media_content(self, c):
        rng = self.rng
        u = rng.choice([self.media_url(), self.media_url(), None])
        t = rng.choice(ENC_TYPES)
        self.mime(t, u)
        attrs = [("url", u), ("type", t), ("medium", rng.choice([None, None, "audio", "video", "image", "document",
                                                                  "Audio"])),
                 ("isDefault", rng.choice([None, None, "true", "false", "TRUE", "1"])),
                 ("fileSize", rng.choice(SIZES)), ("duration", rng.choice(MEDIA_DURATIONS))]
        rng.shuffle(attrs)
        return c.el("media", "content", attrs)

    def chapters(self, c):
        rng = self.rng
        out = []
        for _ in range(rng.randint(0, 6)):
            attrs = [("start", self.chapter_time() if rng.random() < 0.93 else rng.choice([None, ""])),
                     ("title", rng.choice([None, "Intro", "Caf\u00e9 &amp; more", "<b>x</b>", "", "\U0001F600"])),
                     ("href", rng.choice([None, "https://example.com/ch", ""])),
                     ("image", rng.choice([None, "https://example.com/ch.png"]))]
            rng.shuffle(attrs)
            out.append(c.el("psc", "chapter", attrs))
        attrs = [("version", rng.choice([None, "1.2"]))]
        return c.el("psc", "chapters", attrs, "".join(out) if out else (None if rng.random() < 0.5 else ""))

    def transcripts(self, c):
        rng = self.rng
        out = []
        for _ in range(rng.randint(1, 3)):
            out.append(c.el("podcast", "transcript", [
                ("url", rng.choice(["https://example.com/t.json", "https://example.com/t.vtt", "", None])),
                ("type", rng.choice(["application/json", "text/vtt", "application/srt", "application/srr",
                                     "application/x-subrip", "text/html", "text/plain", "", None, "Application/JSON"])),
                ("language", rng.choice([None, "en"]))]))
        return "".join(out)

    def unknown(self, c, level):
        rng = self.rng
        choices = [
            lambda: c.el(None, "category", [], ser_text(rng, "Technology")),
            lambda: c.el(None, "comments", [], "https://example.com/c"),
            lambda: c.el(None, "source", [("url", "https://example.com/s")], c.el(None, "title", [], "Source title")),
            lambda: "<x:foo xmlns:x=\"urn:x\">" + c.el(None, "title", [], ser_text(rng, "Nested title")) + "</x:foo>",
            lambda: "<x:title xmlns:x=\"urn:x\">Other namespace title</x:title>",
            lambda: "<foo>" + c.el(None, "description", [], "Nested description") + "</foo>",
            lambda: "<googleplay:description xmlns:googleplay=\"http://www.google.com/schemas/play-podcasts/1.0\">gp"
                    "</googleplay:description>",
            lambda: "<wrapper><inner><deeper>text</deeper></inner></wrapper>",
            lambda: "<x xmlns=\"urn:other\"><title>Default namespace title</title></x>",
            lambda: "<!-- comment --><?pi data?>",
        ]
        if c.has("itunes"):
            choices.append(lambda: c.el("itunes", "explicit", [], rng.choice(["false", "yes", "clean"])))
            choices.append(lambda: c.el("itunes", "episodeType", [], "full"))
            choices.append(lambda: c.el("itunes", "category", [("text", "Arts")],
                                        c.el("itunes", "category", [("text", "Design")])))
        if c.has("podcast"):
            choices.append(lambda: c.el("podcast", "person", [("role", "host")], "Jane"))
            choices.append(lambda: c.el("podcast", "season", [], "2"))
            choices.append(lambda: c.el("podcast", "locked", [], "yes"))
        return rng.choice(choices)()

    def atom_link(self, c, scope):
        rng = self.rng
        rel = rng.choice([None, "alternate", "self", "next", "payment", "archives", "enclosure", "related", "hub",
                          "first", "last", "Next", ""])
        t = rng.choice([None, None, "text/html", "application/xml+xhtml", "application/atom+xml", "application/rss+xml",
                        "application/xhtml+xml", "audio/mpeg", ""])
        href = rng.choice(["https://example.com/feed?page=2", "https://example.com/alt.xml", "https://example.com/",
                           "https://example.com/pay", "", None, self.media_url()])
        attrs = [("rel", rel), ("type", t), ("href", href),
                 ("title", rng.choice([None, "Alternate", "", "MP3 feed", "Caf\u00e9"])),
                 ("length", rng.choice([None, None, "12", "20000", "abc", "9223372036854775808"]))]
        if rel == "enclosure" or (t or "").startswith("audio"):
            self.mime(t, href)
        rng.shuffle(attrs)
        if c.root_kind == "rss" and rng.random() < 0.12:
            a = "".join(" %s=%s" % (k, ser_attr(rng, v)) for k, v in attrs if v is not None)
            return "<link xmlns=\"%s\"%s/>" % (NSURI["atom"], a)
        key = "atom" if c.root_kind == "rss" else "atomfeed"
        return c.el(key, "link", attrs)

    # -- RSS ------------------------------------------------------------------
    def rss_item(self, c, untitled_ok):
        rng = self.rng
        els = []
        title_r = rng.random()
        has_desc = rng.random() < 0.7
        if title_r < 0.93 or not (untitled_ok or has_desc):
            if rng.random() < 0.02:
                els.append(c.el(None, "title", [], None))
            else:
                els.append(c.el(None, "title", [], c.text("title")[1]))
        if rng.random() < 0.85:
            g = rng.choice(["https://example.com/item/%d" % rng.randint(0, 99999), "urn:uuid:%08x" % rng.getrandbits(32),
                            "  spaced guid  ", "", " ", "\n", "dup", "Caf\u00e9-guid", "a&b"])
            els.append(c.el(None, "guid", [("isPermaLink", rng.choice([None, "false", "true"]))], esc_text(rng, g)))
        if rng.random() < 0.7:
            els.append(c.el(None, "link", [], esc_text(rng, rng.choice(["https://example.com/ep", " https://e.com/x ",
                                                                         ""]))))
        if rng.random() < 0.85:
            els.append(c.el(None, "pubDate", [], esc_text(rng, self.date())))
        if has_desc:
            els.append(c.el(None, "description", [], c.text("desc")[1]))
        if c.has("dc") and rng.random() < 0.12:
            els.append(c.el("dc", "date", [], esc_text(rng, self.date())))
        if c.has("content") and rng.random() < 0.35:
            els.append(c.el("content", "encoded", [], c.text("desc")[1]))
        if c.has("itunes"):
            if rng.random() < 0.3:
                els.append(c.el("itunes", "summary", [], c.text("desc")[1]))
            if rng.random() < 0.25:
                els.append(c.el("itunes", "subtitle", [], c.text("title")[1]))
            if rng.random() < 0.35:
                els.append(c.el("itunes", "image", [("href", rng.choice(["https://example.com/ep.jpg", "", " ", None,
                                                                         " https://e.com/i.png "]))]))
            if rng.random() < 0.6:
                els.append(c.el("itunes", "duration", [], esc_text(rng, self.duration())))
            if rng.random() < 0.05:
                els.append(c.el("itunes", "author", [], "Item author"))
        for _ in range(rng.choice([0, 1, 1, 1, 1, 1, 1, 1, 2, 3])):
            els.append(self.enclosure(c))
        if c.has("media"):
            n = rng.choice([0, 0, 0, 0, 0, 1, 1, 2, 3])
            contents = [self.media_content(c) for _ in range(n)]
            if contents and rng.random() < 0.3:
                els.append(c.el("media", "group", [], "".join(contents)))
            else:
                els.extend(contents)
            if rng.random() < 0.15:
                els.append(c.el("media", "thumbnail", [("url", rng.choice(["https://example.com/th.jpg", "", None]))]))
            if rng.random() < 0.1:
                els.append(c.el("media", "description", [("type", rng.choice([None, "plain", "html"]))],
                                c.text("desc")[1]))
        if c.has("psc"):
            if rng.random() < 0.12:
                els.append(self.chapters(c))
        if c.has("podcast"):
            if rng.random() < 0.1:
                els.append(c.el("podcast", "chapters", [("url", rng.choice(["https://example.com/ch.json", "", None])),
                                                        ("type", "application/json+chapters")]))
            if rng.random() < 0.12:
                els.append(self.transcripts(c))
            if rng.random() < 0.06:
                els.append(c.el("podcast", "socialInteract", [("uri", rng.choice(["https://social.example/@a/1", "",
                                                                                   None])),
                                                              ("protocol", "activitypub")]))
        if c.has("atom") and rng.random() < 0.08:
            els.append(self.atom_link(c, "item"))
        if rng.random() < 0.1:
            els.append(self.unknown(c, "item"))
        if rng.random() < 0.7:
            rng.shuffle(els)
        return c.el(None, "item", [], self.sep().join(els))

    def sep(self):
        return self.rng.choice(["", "\n", "\n  ", " ", "\r\n"])

    def rss(self, ctx, special):
        rng = self.rng
        els = []
        untitled_ok = rng.random() < 0.03
        title_r = rng.random()
        if title_r >= 0.025:
            els.append(ctx.el(None, "title", [], ctx.text("title")[1]))
            if title_r > 0.95:
                els.append(ctx.el(None, "title", [], ctx.text("title")[1]))
        if rng.random() < 0.85:
            els.append(ctx.el(None, "link", [], esc_text(rng, rng.choice(["https://example.com", " https://example.com/ ",
                                                                           ""]))))
        if rng.random() < 0.8:
            els.append(ctx.el(None, "description", [], ctx.text("desc")[1]))
        if rng.random() < 0.6:
            els.append(ctx.el(None, "language", [], esc_text(rng, rng.choice(["en", " EN-US ", "de-DE", "", "fr\n",
                                                                               "TR", "pt-BR"]))))
        if rng.random() < 0.45:
            inner = [ctx.el(None, "url", [], esc_text(rng, rng.choice(["https://example.com/img.png",
                                                                       "  https://example.com/ws.png\n", ""])))]
            if rng.random() < 0.5:
                inner.append(ctx.el(None, "title", [], "Image title"))
            if rng.random() < 0.5:
                inner.append(ctx.el(None, "link", [], "https://example.com/img-link"))
            rng.shuffle(inner)
            els.append(ctx.el(None, "image", [], "".join(inner)))
        if ctx.has("itunes"):
            if rng.random() < 0.6:
                els.append(ctx.el("itunes", "image", [("href", rng.choice(["https://example.com/it.jpg", "", " ", None,
                                                                           " https://example.com/sp.jpg "]))]))
            if rng.random() < 0.5:
                els.append(ctx.el("itunes", "author", [], ser_text(rng, self.text("author"))))
            if rng.random() < 0.35:
                els.append(ctx.el("itunes", "summary", [], ctx.text("desc")[1]))
            if rng.random() < 0.3:
                els.append(ctx.el("itunes", "subtitle", [], ctx.text("title")[1]))
            if rng.random() < 0.1:
                u = rng.choice(["https://new.example.com/feed", " https://new.example.com/sp ", "ftp://x", "", "http",
                                "HTTP://upper.example", "\nhttps://nl.example\n"])
                els.append(ctx.el("itunes", "new-feed-url", [], esc_text(rng, u)))
            if rng.random() < 0.25:
                els.append(ctx.el("itunes", "owner", [], ctx.el("itunes", "name", [], "Owner") +
                                  ctx.el("itunes", "email", [], "o@example.com")))
        if ctx.has("media") and rng.random() < 0.2:
            els.append(ctx.el("media", "thumbnail", [("url", rng.choice(["https://example.com/mt.jpg", None, ""]))]))
        if ctx.has("podcast"):
            for _ in range(rng.choice([0, 0, 0, 1, 1, 2, 3])):
                body = rng.choice([None, "", "Support us", "  spaced  ", "<b>x</b>"])
                els.append(ctx.el("podcast", "funding", [("url", rng.choice(["https://example.com/donate", None, ""]))],
                                  None if body is None else esc_text(rng, body)))
            if rng.random() < 0.004:
                els.append(ctx.el("podcast", "chapters", [("url", "https://example.com/feed-chapters.json")]))
            if rng.random() < 0.004:
                els.append(ctx.el("podcast", "transcript", [("url", "https://example.com/t.json"),
                                                            ("type", "application/json")]))
        if ctx.has("atom"):
            for _ in range(rng.choice([0, 0, 1, 1, 2, 3, 4])):
                els.append(self.atom_link(ctx, "channel"))
        if ctx.has("dc") and rng.random() < 0.1:
            els.append(ctx.el("dc", "creator", [], "DC Creator"))
        if ctx.has("psc") and rng.random() < 0.005:
            els.append(ctx.el("psc", "chapter", [("start", "00:00:01")]))
        if rng.random() < 0.15:
            els.append(self.unknown(ctx, "channel"))
        if rng.random() < 0.08:
            els.append("<textInput><title>Search</title><link>https://example.com/search</link></textInput>")
        if rng.random() < 0.1:
            els.append(ctx.el(None, "lastBuildDate", [], esc_text(rng, rng.choice(self.pools["feed_dates"]))))
        rng.shuffle(els)
        n = rng.choice([0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 8, 12, 20]) if rng.random() < 0.98 else rng.randint(30, 60)
        items = [self.rss_item(ctx, untitled_ok) for _ in range(n)]
        if rng.random() < 0.8:
            body = els + items
        else:
            cut = rng.randint(0, len(els))
            body = els[:cut] + items + els[cut:]
        channel_decl = "".join(" xmlns:%s=\"%s\"" % (p, u) for p, u in ctx.channel_decls)
        channel = "<channel%s>%s</channel>" % (channel_decl, self.sep().join(body))
        if special == "two_channels":
            channel += "<channel><title>Second channel</title>" + self.rss_item(ctx, False) + "</channel>"
        elif special == "item_outside":
            channel += "<item><title>Outside</title><enclosure url=\"https://example.com/o.mp3\" type=\"audio/mpeg\"/></item>"
        version = ctx.version
        root_attrs = "" if version is None else " version=\"%s\"" % version
        decls = "".join(" xmlns:%s=\"%s\"" % (p, u) for p, u in ctx.root_decls)
        default = ""
        if rng.random() < 0.03:
            default = rng.choice([" xmlns=\"http://backend.userland.com/rss2\"", " xmlns=\"\""])
        return "<rss%s%s%s>%s</rss>" % (root_attrs, default, decls, self.sep() + channel + self.sep())

    # -- Atom -----------------------------------------------------------------
    def atom_text(self, ctx, name, kind):
        rng = self.rng
        t = rng.choice([None, None, "text", "html", "html", "xhtml", "HTML", "plain"])
        if t == "xhtml" and rng.random() < 0.6:
            body = "<div xmlns=\"http://www.w3.org/1999/xhtml\">" + esc_text(rng, self.text(kind)) + "</div>"
        else:
            body = ctx.text(kind)[1]
        return ctx.el("atomfeed", name, [("type", t)], body)

    def atom_entry(self, ctx):
        rng = self.rng
        els = []
        if rng.random() < 0.95:
            els.append(self.atom_text(ctx, "title", "title"))
        if rng.random() < 0.85:
            els.append(ctx.el("atomfeed", "id", [], esc_text(rng, rng.choice(["urn:uuid:1", " raw ", "", "tag:x,2020:1",
                                                                               "\nid\n"]))))
        for _ in range(rng.choice([0, 1, 1, 2, 3])):
            els.append(self.atom_link(ctx, "entry"))
        if rng.random() < 0.5:
            els.append(self.atom_text(ctx, "content", "desc"))
        if rng.random() < 0.5:
            els.append(self.atom_text(ctx, "summary", "desc"))
        dates = []
        if rng.random() < 0.7:
            dates.append(ctx.el("atomfeed", "updated", [], esc_text(rng, self.date())))
        if rng.random() < 0.6:
            dates.append(ctx.el("atomfeed", "published", [], esc_text(rng, self.date())))
        if rng.random() < 0.1:
            dates.append(ctx.el("atomfeed", "updated", [], esc_text(rng, self.date())))
        els.extend(dates)
        if rng.random() < 0.2:
            els.append(ctx.el("atomfeed", "author", [], ctx.el("atomfeed", "name", [], "Entry author")))
        if ctx.has("itunes"):
            if rng.random() < 0.4:
                els.append(ctx.el("itunes", "duration", [], esc_text(rng, self.duration())))
            if rng.random() < 0.2:
                els.append(ctx.el("itunes", "image", [("href", rng.choice(["https://example.com/e.jpg", ""]))]))
        if ctx.has("media") and rng.random() < 0.2:
            els.append(self.media_content(ctx))
        if ctx.has("psc") and rng.random() < 0.08:
            els.append(self.chapters(ctx))
        if ctx.has("podcast") and rng.random() < 0.08:
            els.append(self.transcripts(ctx))
        if ctx.has("dc") and rng.random() < 0.05:
            els.append(ctx.el("dc", "date", [], esc_text(rng, self.date())))
        if rng.random() < 0.06:
            els.append(self.unknown(ctx, "entry"))
        rng.shuffle(els)
        return ctx.el("atomfeed", "entry", [], self.sep().join(els))

    def atom(self, ctx, special):
        rng = self.rng
        els = []
        if rng.random() > 0.025:
            els.append(self.atom_text(ctx, "title", "title"))
        if rng.random() < 0.6:
            els.append(self.atom_text(ctx, "subtitle", "desc"))
        if rng.random() < 0.8:
            els.append(ctx.el("atomfeed", "id", [], esc_text(rng, rng.choice(["https://example.com/feed", " raw ",
                                                                              "urn:x", ""]))))
        for _ in range(rng.choice([0, 1, 1, 2, 3, 4])):
            els.append(self.atom_link(ctx, "feed"))
        for _ in range(rng.choice([0, 0, 1, 1, 2, 3])):
            inner = ctx.el("atomfeed", "name", [], esc_text(rng, rng.choice(["Ann", "  Bob  ", "", "C&D", "\u00c9mile"])))
            if rng.random() < 0.3:
                inner += ctx.el("atomfeed", "email", [], "a@example.com")
            els.append(ctx.el("atomfeed", "author", [], inner))
        if rng.random() < 0.3:
            els.append(ctx.el("atomfeed", "logo", [], esc_text(rng, rng.choice(["https://example.com/logo.png",
                                                                                " https://e.com/l.png\n", ""]))))
        if rng.random() < 0.2:
            els.append(ctx.el("atomfeed", "icon", [], esc_text(rng, rng.choice(["https://example.com/icon.png", ""]))))
        if rng.random() < 0.3:
            els.append(ctx.el("atomfeed", "updated", [], esc_text(rng, rng.choice(self.pools["feed_dates"]))))
        if ctx.has("itunes"):
            if rng.random() < 0.4:
                els.append(ctx.el("itunes", "image", [("href", rng.choice(["https://example.com/ai.jpg", ""]))]))
            if rng.random() < 0.3:
                els.append(ctx.el("itunes", "author", [], ser_text(rng, self.text("author"))))
            if rng.random() < 0.2:
                els.append(ctx.el("itunes", "summary", [], ctx.text("desc")[1]))
            if rng.random() < 0.05:
                els.append(ctx.el("itunes", "new-feed-url", [], "https://moved.example.com/atom"))
        if ctx.has("media") and rng.random() < 0.15:
            els.append(ctx.el("media", "thumbnail", [("url", "https://example.com/amt.jpg")]))
        if ctx.has("podcast") and rng.random() < 0.15:
            els.append(ctx.el("podcast", "funding", [("url", "https://example.com/give")], "Give"))
        if rng.random() < 0.08:
            els.append(self.unknown(ctx, "feed"))
        rng.shuffle(els)
        n = rng.choice([0, 1, 1, 2, 2, 3, 4, 5, 8, 12])
        body = els + [self.atom_entry(ctx) for _ in range(n)]
        decls = "".join(" xmlns:%s=\"%s\"" % (p, u) for p, u in ctx.root_decls)
        lang = rng.choice(["", "", " xml:lang=\"en\"", " xml:lang=\"EN-us\"", " xml:lang=\"\""])
        mode = ctx.atom_ns_mode
        if mode == "default":
            root, nsdecl = "feed", " xmlns=\"%s\"" % NSURI["atom"]
        elif mode == "prefixed":
            root, nsdecl = "atom:feed", " xmlns:atom=\"%s\"" % NSURI["atom"]
        elif mode == "both":
            root, nsdecl = "feed", (" xmlns=\"%s\" xmlns:atom=\"%s\"" if rng.random() < 0.5 else
                                    " xmlns:atom=\"%s\" xmlns=\"%s\"") % (NSURI["atom"], NSURI["atom"])
        else:
            root, nsdecl = "feed", ""
        return "<%s%s%s%s>%s</%s>" % (root, nsdecl, decls, lang, self.sep() + self.sep().join(body) + self.sep(), root)

    # -- documents ------------------------------------------------------------
    def document(self):
        rng = self.rng
        r = rng.random()
        kind = "rss" if r < 0.66 else "atom" if r < 0.94 else "special"
        if kind == "special":
            return self.special_document()
        ctx = Ctx(rng, self, kind)
        ctx.setup_namespaces()
        special = None
        sr = rng.random()
        if kind == "rss":
            vr = rng.random()
            ctx.version = ("2.0" if vr < 0.82 else None if vr < 0.9 else "0.91" if vr < 0.945 else "0.92" if vr < 0.99
                           else rng.choice(["1.0", "3.0", "", "2"]))
            if sr < 0.01:
                special = "two_channels"
            elif sr < 0.02:
                special = "item_outside"
            body = self.rss(ctx, special)
        else:
            ar = rng.random()
            ctx.atom_ns_mode = "default" if ar < 0.85 else "prefixed" if ar < 0.95 else "both" if ar < 0.98 else "none"
            ctx.atom_feed_prefixes = {"default": [""], "prefixed": ["atom"], "both": ["", "atom"], "none": [""]}[
                ctx.atom_ns_mode]
            body = self.atom(ctx, special)
        if rng.random() < 0.015:
            body = self.break_document(body)
        prolog = ""
        if rng.random() < 0.04:
            prolog += "<!-- generated -->\n"
        if rng.random() < 0.02:
            root = "rss" if kind == "rss" else "feed"
            prolog += "<!DOCTYPE %s [<!ENTITY show \"Entity Show\"><!ENTITY amp2 \"&#38;#38;\">]>\n" % root
            body = body.replace("Nested title", "&show; title", 1).replace("Episode", "&show;", 1)
        return prolog + body

    def break_document(self, body):
        rng = self.rng
        c = rng.randrange(7)
        if c == 0:
            return body[:rng.randint(1, max(1, len(body) - 1))]
        if c == 1:
            return body.replace("</item>", "</itme>", 1).replace("</entry>", "</entyr>", 1)
        if c == 2:
            return body.replace("Episode", "Episode & more", 1).replace("<title>", "<title>R&D ", 1)
        if c == 3:
            return body.replace("<title>", "<title>&nbsp;", 1)
        if c == 4:
            return body.replace("<title>", "<title>&#1;", 1)
        if c == 5:
            return body + "<rss/>"
        return body.replace("<title>", "<title><undeclared:x/>", 1)

    def special_document(self):
        rng = self.rng
        c = rng.randrange(8)
        if c == 0:
            return ("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>%s</title>"
                    "<link rel=\"alternate\" type=\"application/rss+xml\" href=\"/feed\"></head><body>x</body></html>"
                    % esc_text(rng, rng.choice(["My Podcast", "Caf\u00e9 &amp; Radio", "", "  spaced  "])))
        if c == 1:
            return "<html><body><p>No title here</p></body></html>"
        if c == 2:
            return ("<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns=\"http://purl.org/rss/1.0/\">"
                    "<channel><title>RDF</title></channel><item><title>i</title></item></rdf:RDF>")
        if c == 3:
            return "<opml version=\"2.0\"><head><title>Subs</title></head><body/></opml>"
        if c == 4:
            return "<?xml version=\"1.0\"?>\n<!-- only a comment -->"
        if c == 5:
            return "<HTML><HEAD><TITLE>Upper</TITLE></HEAD></HTML>"
        if c == 6:
            return "<feed><title>No namespace</title><entry><title>e</title></entry></feed>"
        return "<rss version=\"2.0\"><channel><title>Minimal</title></channel></rss>"


ENCODINGS = [
    # (weight, codec, bom, declared encoding or None)
    (35, "utf-8", False, None), (30, "utf-8", False, "UTF-8"), (4, "utf-8", False, "utf-8"),
    (3, "utf-8", True, "UTF-8"), (2, "utf-8", True, None), (3, "utf-16-le", True, "UTF-16"),
    (2, "utf-16-be", True, "UTF-16"), (1.5, "utf-16-be", False, "UTF-16"), (1.5, "utf-16-le", False, "UTF-16LE"),
    (1, "utf-16-le", True, None), (1, "utf-16-be", False, "UTF-16BE"), (6, "latin-1", False, "ISO-8859-1"),
    (5, "cp1252", False, "windows-1252"), (1.5, "ascii", False, "US-ASCII"), (1, "latin-1", False, "UTF-8"),
    (1, "utf-8", False, "ISO-8859-1"), (0.5, "utf-16-le", True, "UTF-8"), (0.5, "utf-8", False, "UTF-16"),
    (0.3, "utf-8", False, "x-unknown-charset"), (0.5, "latin-1", False, "latin1"), (0.5, "latin-1", False, "ISO-8859-15"),
    (0.5, "utf-8", False, "UTF8"), (0.4, "cp1252", False, "Cp1252"), (0.3, "utf-16-be", True, "UTF-16LE"),
]
BOMS = {"utf-8": b"\xef\xbb\xbf", "utf-16-le": b"\xff\xfe", "utf-16-be": b"\xfe\xff"}


def encode_document(rng, text):
    total = sum(w for w, *_ in ENCODINGS)
    r = rng.random() * total
    for w, codec, bom, declared in ENCODINGS:
        r -= w
        if r <= 0:
            break
    decl = ""
    if declared is not None:
        decl = "<?xml version=\"1.0\" encoding=\"%s\"%s?>" % (declared, rng.choice(["", "", " standalone=\"yes\""]))
        decl += rng.choice(["\n", "", "\r\n"])
    elif rng.random() < 0.3:
        decl = "<?xml version=\"1.0\"?>\n"
    if text.startswith("<?xml"):
        decl = ""
    if rng.random() < 0.05:
        text = text.replace("\n", "\r\n")
    data = (decl + text).encode(codec, "xmlcharrefreplace")
    if bom:
        data = BOMS[codec] + data
    return data, "%s%s/%s" % (codec, "+bom" if bom else "", declared)


# ---------------------------------------------------------------------------
# NotTennaPod-iOS FeedParserTests scenarios, transcribed with Swift's string
# rules; verify_ios_scenarios() checks every literal against the Swift source.

IOS_NAMESPACES = "\n".join([
    "xmlns:itunes=\"http://www.itunes.com/dtds/podcast-1.0.dtd\"",
    "xmlns:content=\"http://purl.org/rss/1.0/modules/content/\"",
    "xmlns:media=\"http://search.yahoo.com/mrss/\"",
    "xmlns:podcast=\"https://podcastindex.org/namespace/1.0\"",
    "xmlns:psc=\"http://podlove.org/simple-chapters\"",
    "xmlns:atom=\"http://www.w3.org/2005/Atom\"",
    "xmlns:dc=\"http://purl.org/dc/elements/1.1/\"",
])
EXTERNAL_SENTINEL = "/data/local/tmp/ntp-7a/external-entity-sentinel"


def ios_rss(channel):
    return "<rss version=\"2.0\" " + IOS_NAMESPACES + "><channel>" + channel + "</channel></rss>"


def ios_scenarios():
    s = []
    s.append(("description-selection", ios_rss(
        "<item><description>aaa</description><content:encoded>\U0001F600\U0001F600</content:encoded>\n"
        "<itunes:summary>bbbb</itunes:summary></item>\n"
        "<item><description><![CDATA[<p>Raw shownotes</p>]]></description><itunes:summary>short</itunes:summary></item>\n"
        "<item><title/><description>fallback only for absent title</description></item>")))
    s.append(("html-metadata-rss", ios_rss(
        "<title><![CDATA[A &amp; B <b>bold</b>]]></title>\n"
        "<description><![CDATA[<p>First</p><p>Second<br>line</p>]]></description>\n"
        "<itunes:author><![CDATA[Author &copy;]]></itunes:author>\n"
        "<item><description><![CDATA[one\n"
        "two &amp; three]]></description></item>")))
    s.append(("html-metadata-atom",
              "<feed xmlns=\"http://www.w3.org/2005/Atom\"><title type=\"html\">&lt;p&gt;First&lt;/p&gt;&lt;p&gt;Second&lt;/p&gt;</title>\n"
              "<entry><summary type=\"text\">&lt;b&gt;Literal&lt;/b&gt;</summary></entry></feed>"))
    items = ("<item><media:thumbnail url=\"episode\"/><itunes:image href=\"\"/></item>\n"
             "<item><media:thumbnail url=\"episode\"/><itunes:image/></item>\n"
             "<item/>")
    for i, channel in enumerate([
            "<image><url>rss</url></image><media:thumbnail url=\"media\"/><itunes:image href=\" itunes \"/>",
            "<itunes:image href=\" itunes \"/><image><url>rss</url></image><media:thumbnail url=\"media\"/>"]):
        s.append(("artwork-%d" % (i + 1), ios_rss(channel + items)))
    s.append(("enclosure-rss", ios_rss(
        "<item><enclosure url=\"cover\" type=\"image/png\"/><enclosure url=\"first\" type=\"audio/mpeg\" length=\"16383\"/>\n"
        "<enclosure url=\"second\" type=\"audio/mpeg\" length=\"40000\"/></item>\n"
        "<item><enclosure url=\"file\" type=\"unknown\" length=\"16384\"/></item>\n"
        "<item><enclosure url=\"\" type=\"audio/mpeg\"/></item>\n"
        "<item><enclosure url=\"file.mp3?auth=yes\" length=\"-9\"/></item>\n"
        "<item><enclosure url=\"file.mp3\" type=\"image/jpeg\"/></item>")))
    s.append(("enclosure-atom",
              "<feed xmlns=\"http://www.w3.org/2005/Atom\"><entry><link rel=\"enclosure\" length=\"12\" type=\"audio/mpeg\"/></entry></feed>"))
    s.append(("media-default", ios_rss(
        "<item><itunes:duration>02:15:45</itunes:duration><enclosure url=\"original\" type=\"audio/mpeg\"/>\n"
        "<media:content url=\"other\" type=\"audio/aac\"/>\n"
        "<media:content url=\"chosen\" type=\"audio/flac\" isDefault=\"true\" duration=\"99\" fileSize=\"12\"/>\n"
        "<itunes:duration>invalid</itunes:duration></item>\n"
        "<item><media:content url=\"next\" medium=\"audio\" duration=\"2\"/></item>\n"
        "<item><itunes:duration>5</itunes:duration></item>\n"
        "<item><enclosure url=\"last\" type=\"audio/mpeg\"/></item>")))
    formats = ["application/x-subrip", "text/vtt", "application/json"]
    for perm in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]]:
        tags = "".join("<podcast:transcript url=\"url%d\" type=\"%s\"/>" % (i, formats[i]) for i in perm)
        s.append(("transcript-%s" % "".join(map(str, perm)), ios_rss(
            "<item>" + tags + "<podcast:transcript url=\"later\" type=\"application/json\"/></item>")))
    s.append(("transcript-aliases", ios_rss(
        "<item><podcast:transcript url=\"unsupported\" type=\"text/plain\"/>\n"
        "<podcast:transcript url=\"srt\" type=\"application/srr\"/>\n"
        "<podcast:transcript url=\"\" type=\"application/json\"/></item>")))
    s.append(("identifiers-rss", ios_rss(
        "<language> EN-US </language><item><guid>  identity  </guid></item><item><guid> </guid></item>\n"
        "<item><guid/></item><item xmlns:other=\"http://www.itunes.com/dtds/podcast-1.0.dtd\"><other:image href=\"recognized-by-uri\"/></item>")))
    s.append(("identifiers-unregistered",
              "<rss xmlns:other=\"http://www.itunes.com/dtds/podcast-1.0.dtd\"><channel><other:image href=\"ignored\"/></channel></rss>"))
    s.append(("identifiers-atom", "<feed xmlns=\"http://www.w3.org/2005/Atom\" xml:lang=\"EN-US\"><id> raw </id></feed>"))
    s.append(("chapters-funding", ios_rss(
        "<podcast:funding url=\"donate\">  support  </podcast:funding><itunes:new-feed-url> https://new.example </itunes:new-feed-url>\n"
        "<atom:link rel=\"next\" href=\"next\"/><atom:link rel=\"alternate\" type=\"application/rss+xml\" href=\"alternate\" title=\"Old\"/>\n"
        "<atom:link rel=\"archives\" type=\"application/rss+xml\" href=\"alternate\" title=\"New\"/>\n"
        "<item><podcast:chapters url=\"chapters.json\"/><podcast:socialInteract uri=\"social\"/>\n"
        "<psc:chapters><psc:chapter start=\"00:01:02.5\" title=\"Start\" href=\"link\" image=\"image\"/>\n"
        "<psc:chapter start=\"bad:time\"/><psc:chapter start=\"12\" title=\"Single value\"/></psc:chapters></item>")))
    s.append(("atom-dates",
              "<feed xmlns=\"http://www.w3.org/2005/Atom\">\n"
              "<entry><updated>2020-01-01</updated><published>2021-01-01</published><updated>2022-01-01</updated></entry>\n"
              "<entry><published>2099-01-01</published><updated>2020-01-01</updated></entry>\n"
              "<entry><updated>2020-01-01</updated><published>2099-01-01</published></entry>\n"
              "</feed>"))
    s.append(("reject-html", "<html/>"))
    s.append(("reject-rss-3.0", "<rss version=\"3.0\"/>"))
    s.append(("reject-empty", ""))
    s.append(("reject-not-xml", "not xml"))
    s.append(("reject-mismatched", "<rss><channel><item></channel></rss>"))
    s.append(("reject-undefined-entity", "<rss><channel>&missing;</channel></rss>"))
    for version in ["2.0", "0.91", "0.92"]:
        s.append(("rss-version-%s" % version, "<rss version=\"%s\"><channel/></rss>" % version))
    xml = ("<?xml version=\"1.0\" encoding=\"UTF-16\"?><rss><channel><title>Caf\u00e9<![CDATA[ &amp; ]]>\u8336"
           "</title></channel></rss>")
    s.append(("declared-utf16", b"\xff\xfe" + xml.encode("utf-16-le")))
    s.append(("nested-markup", ios_rss(
        "<item><description>before<unknown>inside</unknown>after</description></item>\n"
        "<item><description><![CDATA[before<unknown>inside</unknown>after]]></description></item>")))
    for index in range(32):
        s.append(("independent-%02d" % index,
                  "<rss><channel><title>feed-%d</title><item><guid>item-%d</guid></item></channel></rss>" % (index, index)))
    s.append(("large-feed", ios_rss("".join("<item><guid>duplicate</guid><title>episode-%d</title></item>" % i
                                            for i in range(1000)))))
    s.append(("external-entity",
              "<!DOCTYPE rss [<!ENTITY external SYSTEM \"file://%s\">]>\n"
              "<rss><channel><title>&external;</title></channel></rss>" % EXTERNAL_SENTINEL))
    return s


def verify_ios_scenarios(swift_path, scenarios):
    source = Path(swift_path).read_text(encoding="utf-8")
    texts = [t if isinstance(t, str) else t[2:].decode("utf-16-le") for _, t in scenarios] + [IOS_NAMESPACES]
    blob = "\x00".join(texts)
    lines = source.split("\n")
    checked = 0
    i = 0
    while i < len(lines):
        if lines[i].rstrip().endswith("\"\"\"") and lines[i].count("\"\"\"") == 1:
            j = i + 1
            while lines[j].strip() != "\"\"\"" and not lines[j].strip().startswith("\"\"\")"):
                j += 1
            indent = len(lines[j]) - len(lines[j].lstrip())
            block = "\n".join(line[indent:] for line in lines[i + 1:j])
            for piece in re.split(r"\\\([^)]*\)", block):
                if piece and piece not in blob:
                    raise SystemExit("iOS scenario literal not transcribed: %r" % piece[:80])
            checked += 1
            i = j + 1
        else:
            i += 1
    for m in re.finditer(r"(?:parse|rss)\(\"((?:[^\"\\]|\\.)*)\"\)", source):
        lit = m.group(1)
        for piece in re.split(r"\\\([^)]*\)", lit):
            piece = piece.replace("\\\"", "\"")
            if piece and piece not in blob:
                raise SystemExit("iOS scenario literal not transcribed: %r" % piece[:80])
        checked += 1
    return checked


# ---------------------------------------------------------------------------


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def dedupe(seq):
    seen = set()
    out = []
    for x in seq:
        key = json.dumps(x)
        if key not in seen:
            seen.add(key)
            out.append(x)
    return out


def extract_dates(text):
    out = []
    for m in re.finditer(r"<((?:\w+:)?(?:pubDate|updated|published|date))(?:\s[^>]*)?>(.*?)</\1>", text, re.S):
        out.append(m.group(2))
    return out


def generate(args):
    seed = args.seed
    rng = random.Random(seed)
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for p in out.glob("*.xml"):
        p.unlink()
    files = []

    android_res = Path(args.android) / "parser/feed/src/test/resources"
    for p in sorted(android_res.glob("*.xml")):
        files.append(("android-" + p.name, p.read_bytes(), "android"))
    scenarios = ios_scenarios()
    checked = verify_ios_scenarios(Path(args.ios) / "Tests/NotTennaPodCoreTests/FeedParserTests.swift", scenarios)
    for name, text in scenarios:
        files.append(("ios-%s.xml" % name, text if isinstance(text, bytes) else text.encode("utf-8"), "ios"))
    hostile = Path(args.ios) / "Tests/NotTennaPodCoreTests/Fixtures/HostileFeeds"
    for p in sorted(hostile.glob("*.xml")):
        files.append(("hostile-" + p.name, p.read_bytes(), "hostile"))

    date_rng = random.Random(seed * 1000003 + 1)
    date_pool = date_pool_builder(date_rng)
    feed_dates = [d for d in date_pool if "\x00" not in d and "\x0b" not in d and "\f" not in d and "\x85" not in d
                  and not any(0xd800 <= ord(c) <= 0xdfff for c in d)]
    valid_like = [d for d in feed_dates if re.search(r"\d{4}|\d\d:\d\d", d)]
    pools = {"dates": [], "durations": [], "times": [], "html": [], "mime": [], "extensions": [],
             "feed_dates": valid_like * 3 + feed_dates, "urls": url_pool(random.Random(seed * 1000003 + 2), 400)}
    gen = FeedGenerator(rng, pools)
    encodings = {}
    for i in range(args.feeds):
        text = gen.document()
        data, enc = encode_document(rng, text)
        encodings[enc] = encodings.get(enc, 0) + 1
        files.append(("gen-%04d.xml" % i, data, "generated"))

    names = [f[0] for f in files]
    assert len(set(names)) == len(names)
    for name, data, _ in files:
        (out / name).write_bytes(data)
    sums = "".join("%s  %s\n" % (sha256(data), name) for name, data, _ in sorted(files))
    (out / "SHA256SUMS").write_text(sums, encoding="utf-8")

    extracted = []
    for name, data, group in files:
        if group == "generated":
            continue
        if data.startswith(b"\xff\xfe"):
            text = data[2:].decode("utf-16-le", "replace")
        else:
            text = data.decode("utf-8", "replace")
        for d in extract_dates(text):
            extracted += [d, java_trim_all(d)]

    vrng = random.Random(seed * 1000003 + 3)
    ios_durations = [c["input"] for c in json.loads(
        (Path(args.ios) / "Tests/NotTennaPodCoreTests/Fixtures/duration-java.json").read_text(encoding="utf-8"))]
    gen_durations = []
    for k in range(900):
        r = vrng.random()
        if r < 0.3:
            gen_durations.append("%d:%02d:%02d" % (vrng.randint(0, 99), vrng.randint(0, 99), vrng.randint(0, 99)))
        elif r < 0.5:
            gen_durations.append("%d:%02d" % (vrng.randint(0, 9999), vrng.randint(0, 99)))
        elif r < 0.65:
            gen_durations.append("%d.%0*d" % (vrng.randint(0, 99999), vrng.randint(1, 9), vrng.randint(0, 999)))
        elif r < 0.75:
            gen_durations.append(str(vrng.randint(-10 ** vrng.randint(1, 19), 10 ** vrng.randint(1, 19))))
        elif r < 0.85:
            gen_durations.append(vrng.choice(WHITESPACE) + vrng.choice(DURATION_FORMS) + vrng.choice(WHITESPACE))
        else:
            gen_durations.append(":".join(vrng.choice(["0", "1", "01", "-1", "+2", "1.5", "", " 3", "1e1", "x",
                                                       "2147483648", "0x10", "\u0663"])
                                          for _ in range(vrng.randint(1, 4))))
    gen_times = []
    for k in range(700):
        r = vrng.random()
        if r < 0.35:
            gen_times.append("%02d:%02d:%02d.%0*d" % (vrng.randint(0, 99), vrng.randint(0, 99), vrng.randint(0, 99),
                                                      vrng.randint(1, 7), vrng.randint(0, 9999)))
        elif r < 0.55:
            gen_times.append("%d:%02d" % (vrng.randint(0, 9999), vrng.randint(0, 99)))
        elif r < 0.65:
            gen_times.append(str(vrng.randint(0, 100000)))
        else:
            gen_times.append(":".join(vrng.choice(["0", "1", "01", "-1", "+2", "1.5", "", " 3", "3 ", "1e1", "x",
                                                   "2147483648", "0x10", "NaN", "Infinity", "\u0663", "1.5f"])
                                      for _ in range(vrng.randint(1, 5))))
    mime_types = ENC_TYPES + ["application/x-mpegurl", "audio/x-mpegurl", "video/mp2t"]
    mime_urls = [None, ""] + ["https://example.com/a." + e for e in URL_EXTS] + [
        "https://example.com/a.mp3?auth=1", "https://example.com/a.b/file", "https://example.com/file",
        "file.MP3", "a.mp3#frag", "https://example.com/a.ogg?x=y.mp3", "a.mp3/", "C:\\dir\\a.mp3", "a.\u00e9"]
    mimes = [[t, u] for t in mime_types for u in mime_urls]

    values = {
        "date": dedupe(date_pool + pools["dates"] + extracted),
        "duration": dedupe(ios_durations + DURATION_FORMS + gen_durations + pools["durations"]),
        "time": dedupe(TIME_FORMS + gen_times + pools["times"]),
        "html": dedupe(html_pool_builder(vrng) + pools["html"])[:6000],
        "mime": dedupe(mimes + pools["mime"]),
        "extension": dedupe(EXTENSION_CANDIDATES + EXTENSION_EXTRA + [filename_extension(u) for u in mime_urls if u]
                            + pools["extensions"]),
    }
    info = {"generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "generated_at_ms": int(now.timestamp() * 1000),
            "seed": seed, "feeds": args.feeds,
            "groups": {g: sum(1 for f in files if f[2] == g) for g in ["android", "ios", "hostile", "generated"]},
            "encodings": dict(sorted(encodings.items())), "ios_literals_checked": checked}
    doc = dict(values)
    doc["info"] = info
    Path(args.values_in).parent.mkdir(parents=True, exist_ok=True)
    Path(args.values_in).write_text(json.dumps(doc, ensure_ascii=True), encoding="ascii")
    print(json.dumps(info, indent=1))
    print({k: len(v) for k, v in values.items()})


# ---------------------------------------------------------------------------


def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def assemble(args):
    dev = Path(args.device_out)
    feeds = Path(args.feeds)
    values_in = json.loads(Path(args.values_in).read_text(encoding="ascii"))
    info = values_in["info"]
    sums = [line.split("  ", 1) for line in (feeds / "SHA256SUMS").read_text(encoding="utf-8").splitlines() if line]
    names = [n for _, n in sums]
    for digest, name in sums:
        if sha256((feeds / name).read_bytes()) != digest:
            raise SystemExit("corpus file changed since generation: " + name)
    on_disk = sorted(p.name for p in feeds.glob("*.xml"))
    if on_disk != sorted(names):
        raise SystemExit("corpus directory does not match SHA256SUMS")

    rows = read_jsonl(dev / "feeds.jsonl")
    if [r["file"] for r in rows] != sorted(names):
        missing = sorted(set(names) - set(r["file"] for r in rows))
        raise SystemExit("feed rows incomplete or out of order; missing: %s" % missing[:10])
    feed_device = json.loads((dev / "feeds-device.json").read_text(encoding="utf-8"))
    value_device = json.loads((dev / "values-device.json").read_text(encoding="utf-8"))
    if feed_device["rows"] != len(names):
        raise SystemExit("device reported %d feed rows" % feed_device["rows"])

    expected_cases = []
    for d in values_in["date"]:
        expected_cases += [("date_parse", d), ("date_parse_or_null_if_future", d)]
    expected_cases += [("duration_in_millis", d) for d in values_in["duration"]]
    expected_cases += [("parse_time_string", d) for d in values_in["time"]]
    for h in values_in["html"]:
        expected_cases += [("html_compact", h), ("html_legacy", h)]
    expected_cases += [("mime_type", m) for m in values_in["mime"]]
    expected_cases += [("mime_type_from_extension", e) for e in values_in["extension"]]
    cases = read_jsonl(dev / "values.jsonl")
    if len(cases) != len(expected_cases):
        raise SystemExit("value rows: %d, inputs: %d" % (len(cases), len(expected_cases)))
    for case, (kind, inp) in zip(cases, expected_cases):
        if case["kind"] != kind or case["input"] != inp or "output" not in case:
            raise SystemExit("value row mismatch: %r vs %r" % (case, (kind, inp)))

    gen_ms = info["generated_at_ms"]
    run_ms = min(feed_device["started_ms"], value_device["started_ms"])
    limit = min(gen_ms, run_ms) - DAY_MS
    allowed_future = {"2099-01-01"}  # NotTennaPod-iOS atom-dates scenario, kept verbatim: null before 2099
    violations = []
    for case in cases:
        if case["kind"] == "date_parse" and isinstance(case["output"], int):
            ms = case["output"]
            if limit <= ms < YEAR_2101_MS and java_trim_all(case["input"]) not in allowed_future:
                violations.append(case)
            if abs(ms) > 2 ** 53 - 1:
                violations.append(case)
    for row in rows:
        for item in row["result"].get("items", []):
            if item["pub_date"] is not None and item["pub_date"] >= limit:
                violations.append({"file": row["file"], "pub_date": item["pub_date"]})
    if violations:
        raise SystemExit("dates inside the clock window: %s" % violations[:10])

    shutil.copyfile(dev / "feeds.jsonl", feeds / "expected.jsonl")
    vpath = Path(args.values_out)
    with open(vpath, "w", encoding="ascii") as f:
        header = {"suite": "feed_values", "generated_at": info["generated_at"], "seed": info["seed"],
                  "device": {k: value_device[k] for k in ["fingerprint", "sdk", "release", "started_ms",
                                                          "finished_ms"]}}
        f.write(json.dumps(header, ensure_ascii=True)[:-1] + ",\n\"cases\": [\n")
        f.write(",\n".join(json.dumps(c, ensure_ascii=True, separators=(",", ":")) for c in cases))
        f.write("\n]}\n")
    json.loads(vpath.read_text(encoding="ascii"))
    symbols = json.loads((dev / "date-symbols.json").read_text(encoding="utf-8"))
    Path(args.symbols_out).write_text(json.dumps(symbols, ensure_ascii=True, indent=1) + "\n", encoding="ascii")

    labels = {}
    for row in rows:
        label = row["result"].get("error", "success")
        labels[label] = labels.get(label, 0) + 1
    groups = {}
    for row in rows:
        g = row["file"].split("-", 1)[0]
        label = row["result"].get("error", "success")
        groups.setdefault(g, {}).setdefault(label, 0)
        groups[g][label] += 1
    kinds = {}
    for c in cases:
        kinds[c["kind"]] = kinds.get(c["kind"], 0) + 1
    android_commit = subprocess.run(["git", "-C", args.android, "rev-parse", "HEAD"], capture_output=True, text=True,
                                    check=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", args.android, "status", "--porcelain", "--", "parser/feed"],
                           capture_output=True, text=True, check=True).stdout.strip()
    if dirty:
        raise SystemExit("the exporter is not committed in the Android checkout:\n" + dirty)
    exceptions = sum(1 for c in cases if isinstance(c["output"], dict))
    write_provenance(args, info, feed_device, value_device, symbols, android_commit, len(names), labels, groups,
                     kinds, feeds, vpath, exceptions)
    print(json.dumps({"feeds": len(rows), "labels": labels, "groups": groups, "values": kinds}, indent=1))


def write_provenance(args, info, feed_device, value_device, symbols, commit, nfeeds, labels, groups, kinds, feeds,
                     vpath, exceptions):
    def ms(t):
        return datetime.datetime.fromtimestamp(t / 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"

    def digest(p):
        return sha256(Path(p).read_bytes())

    lines = [
        "# Feed parser vectors: provenance",
        "",
        "Expected outputs of Android AntennaPod 3.12.1's own feed parser classes, run unchanged on the Android",
        "3.12.1 emulator (PROTOCOL §9.2). Generated by `contract/reference/gen_feed_corpus.py`.",
        "",
        "## Generation",
        "",
        "- Generation time: %s (%d ms)." % (info["generated_at"], info["generated_at_ms"]),
        "- Seed: %d; generated feeds: %d." % (info["seed"], info["feeds"]),
        "- Corpus: %d files: %s." % (nfeeds, ", ".join("%s %d" % (k, v) for k, v in info["groups"].items())),
        "  - `android-*`: `parser/feed/src/test/resources` of the Android checkout;",
        "  - `ios-*`: the inline scenarios of NotTennaPod-iOS's `FeedParserTests.swift` (%d literals checked against the"
        % info["ios_literals_checked"],
        "    Swift source); `ios-external-entity.xml` names `file://%s`, which the run pushes" % EXTERNAL_SENTINEL,
        "    with the content `external-entity-sentinel`;",
        "  - `hostile-*`: NotTennaPod-iOS's `Fixtures/HostileFeeds/*.xml` (compare type and error label only);",
        "  - `gen-*`: seeded generated feeds. Encodings (codec[+bom]/declared): %s." % ", ".join(
            "%s %d" % (k, v) for k, v in info["encodings"].items()),
        "- Clock: `DateUtils.parseOrNullIfFuture` reads the device clock, which was not changed. Every date in the",
        "  corpus and the value inputs parses either to more than one day before the generation time and the run, or to",
        "  2101 or later; `assemble` checks this on the device's own `DateUtils.parse` results. The one exception is",
        "  `2099-01-01` in `ios-atom-dates.xml` (kept verbatim): null on any clock before 2099.",
        "  Two-digit years use only 00-25 and 48-99, so SimpleDateFormat's century window (80 years back, 20",
        "  ahead) gives the same years on any clock from 2006 through 2027.",
        "",
        "## Emulator",
        "",
        "- AVD `AntennaPod-3.12.1-api36`; API level %d (Android %s)." % (feed_device["sdk"], feed_device["release"]),
        "- Build fingerprint: `%s`." % feed_device["fingerprint"],
        "- Default time zone and locale as found on the emulator (not changed).",
        "- Feed run: %s to %s; value run: %s to %s (device clock)." % (
            ms(feed_device["started_ms"]), ms(feed_device["finished_ms"]), ms(value_device["started_ms"]),
            ms(value_device["finished_ms"])),
        "",
        "## Exporter",
        "",
        "- NotTennaPod-Android, branch `nottennapod-sync`, commit `%s` (tag 3.12.1 plus test-only commits)." % commit,
        "- `parser/feed/src/androidTest/java/de/danoeh/antennapod/parser/feed/GoldenDeviceExportTest.java`:",
        "  - `exportFeeds` runs `FeedHandler` on every corpus file with `FeedParserTask`'s title checks and generative",
        "    cover, source `https://feeds.example.invalid/<file name>`, and writes one row per file in the schema of",
        "    the JVM `GoldenFeedExportTest`, except that `media.size` and chapter `start` are decimal strings, and",
        "    that an alternate-feed link without `href` (a null key in Android's map, which a JSON object cannot",
        "    hold) is the member `alternate_feed_url_null` with its title, present only then. It verifies every",
        "    file's SHA-256 against `SHA256SUMS` and fails unless every file produced a row.",
        "  - `exportValues` records `DateUtils.parse`, `DateUtils.parseOrNullIfFuture`, `DurationParser.inMillis`,",
        "    `DateUtils.parseTimeString`, `HtmlCompat.fromHtml(…, FROM_HTML_MODE_COMPACT|LEGACY).toString()`,",
        "    `MimeTypeUtils.getMimeType(type, url)` and `MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext)`,",
        "    and Locale.US `DateFormatSymbols`, zone strings and time zone IDs; it fails unless every input produced",
        "    a row.",
        "",
        "## Files",
        "",
        "| File | SHA-256 |",
        "| --- | --- |",
        "| `feeds/SHA256SUMS` (per-file SHA-256 of the corpus; `shasum -a 256 -c SHA256SUMS`) | `%s` |" % digest(
            feeds / "SHA256SUMS"),
        "| `feeds/expected.jsonl` | `%s` |" % digest(feeds / "expected.jsonl"),
        "| `feed_values.json` | `%s` |" % digest(vpath),
        "| `feed_date_symbols.json` | `%s` |" % digest(args.symbols_out),
        "",
        "## Results",
        "",
        "- `expected.jsonl`: %d rows (%s)." % (nfeeds, ", ".join("%s %d" % (k, v) for k, v in sorted(labels.items()))),
    ]
    for g, counts in sorted(groups.items()):
        lines.append("  - `%s-*`: %s." % (g, ", ".join("%s %d" % (k, v) for k, v in sorted(counts.items()))))
    lines += [
        "- `feed_values.json` (suite `feed_values`): %d cases: %s." % (sum(kinds.values()), ", ".join(
            "`%s` %d" % (k, v) for k, v in kinds.items())),
        "  - A case is `{\"kind\", \"input\", \"output\"}`. Dates are milliseconds since the epoch or null;",
        "    `duration_in_millis` and `parse_time_string` outputs are the Java `long` as a decimal string, or",
        "    `\"error\"` for `NumberFormatException`; HTML and MIME outputs are strings or null. `mime_type` inputs",
        "    are `[type, url]`. An unexpected exception is recorded as `{\"exception\": \"<class>\"}` (%d cases)."
        % exceptions,
        "  - Strings are exact UTF-16: unpaired surrogates are `\\u` escapes.",
        "- `feed_date_symbols.json`: Locale.US weekdays, short weekdays, months, short months, AM/PM, eras, %d zone"
        % len(symbols["zone_strings"]),
        "  string rows, %d time zone IDs with raw offsets, and `TimeZone.getTimeZone(\"GMT\").getID()`." % len(
            symbols["available_ids"]),
        "",
        "## Commands",
        "",
        "From the NotTennaPod-Server checkout, with `ANDROID_HOME` the Android SDK, `JAVA_HOME` a JDK 21,",
        "`ADB=$ANDROID_HOME/platform-tools/adb` and the AVD `AntennaPod-3.12.1-api36` running:",
        "",
        "```sh",
    ]
    lines += COMMANDS.format(seed=info["seed"], feeds=info["feeds"]).strip("\n").split("\n")
    lines += ["```", ""]
    Path(feeds / "PROVENANCE.md").write_text("\n".join(lines), encoding="utf-8")


COMMANDS = """
python3 contract/reference/gen_feed_corpus.py generate --seed {seed} --feeds {feeds} \\
    --android ../NotTennaPod-Android --ios ../NotTennaPod-iOS \\
    --out contract/vectors/feeds --values-in .work/feeds7a/values_in.json
$ADB shell rm -rf /data/local/tmp/ntp-7a
$ADB shell mkdir -p /data/local/tmp/ntp-7a
$ADB push contract/vectors/feeds /data/local/tmp/ntp-7a/
$ADB push .work/feeds7a/values_in.json /data/local/tmp/ntp-7a/values_in.json
printf external-entity-sentinel > .work/feeds7a/external-entity-sentinel
$ADB push .work/feeds7a/external-entity-sentinel /data/local/tmp/ntp-7a/external-entity-sentinel
(cd ../NotTennaPod-Android && ./gradlew --console=plain :parser:feed:connectedDebugAndroidTest \\
    -Pandroid.testInstrumentationRunnerArguments.ntpFeedsIn=/data/local/tmp/ntp-7a/feeds \\
    -Pandroid.testInstrumentationRunnerArguments.ntpValuesIn=/data/local/tmp/ntp-7a/values_in.json)
python3 contract/reference/gen_feed_corpus.py assemble --android ../NotTennaPod-Android \\
    --device-out "../NotTennaPod-Android/parser/feed/build/outputs/connected_android_test_additional_output/debugAndroidTest/connected/AntennaPod-3.12.1-api36(AVD) - 16" \\
    --feeds contract/vectors/feeds --values-in .work/feeds7a/values_in.json \\
    --values-out contract/vectors/feed_values.json --symbols-out contract/vectors/feed_date_symbols.json
$ADB shell rm -rf /data/local/tmp/ntp-7a
"""


# ---------------------------------------------------------------------------


def normalize_jvm(result):
    for item in result.get("items", []):
        if item.get("media") is not None and isinstance(item["media"].get("size"), int):
            item["media"]["size"] = str(item["media"]["size"])
        for ch in item.get("chapters") or []:
            if isinstance(ch.get("start"), int):
                ch["start"] = str(ch["start"])
    return result


def diff_paths(a, b, path=""):
    if type(a) is not type(b):
        return [path or "/"]
    if isinstance(a, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                out.append(path + "/" + k)
            else:
                out += diff_paths(a[k], b[k], path + "/" + k)
        return out
    if isinstance(a, list):
        if len(a) != len(b):
            return [path + "[len]"]
        out = []
        for i, (x, y) in enumerate(zip(a, b)):
            out += diff_paths(x, y, path + "[]")
        return out
    return [] if a == b else [path]


def compare(args):
    emu = {r["file"]: r["result"] for r in read_jsonl(args.emulator)}
    jvm = {r["file"]: normalize_jvm(r["result"]) for r in read_jsonl(args.jvm)}
    if set(emu) != set(jvm):
        print("files only in one export:", sorted(set(emu) ^ set(jvm))[:20])
    differing = [f for f in sorted(emu) if f in jvm and emu[f] != jvm[f]]
    by_field = {}
    by_group = {}
    for f in differing:
        a, b = emu[f], jvm[f]
        if "error" in a or "error" in b:
            key = "outcome: emulator %s / jvm %s" % (a.get("error", "success"), b.get("error", "success"))
            if "error" in a and "error" in b and a["error"] == b["error"]:
                key = "error detail (%s)" % a["error"]
            paths = [key]
        else:
            paths = sorted(set(diff_paths(a, b)))
        for p in paths:
            by_field.setdefault(p, []).append(f)
        g = f.split("-", 1)[0]
        by_group[g] = by_group.get(g, 0) + 1
    print("rows: %d; differing: %d; by group: %s" % (len(emu), len(differing), by_group))
    for p, fs in sorted(by_field.items(), key=lambda kv: -len(kv[1])):
        print("  %5d  %s  e.g. %s" % (len(fs), p, ", ".join(fs[:3])))
    shown = 0
    for p, fs in sorted(by_field.items(), key=lambda kv: -len(kv[1])):
        if shown >= args.examples:
            break
        f = fs[0]
        print("\n== %s (%s)" % (f, p))
        for label, r, o in [("emulator", emu[f], jvm[f]), ("jvm", jvm[f], emu[f])]:
            print("  %s: %s" % (label, json.dumps(pick(r, o, p), ensure_ascii=False)[:600]))
        shown += 1


def pick(result, other, path):
    if "error" in result or path.startswith("outcome") or path.startswith("error"):
        return {k: result.get(k) for k in ["error", "detail", "title", "type"] if k in result}
    parts = [p for p in path.split("/") if p]
    if parts and parts[0].startswith("items") and len(parts) > 1:
        field = parts[1].split("[")[0]
        for i, (x, y) in enumerate(zip(result.get("items", []), other.get("items", []))):
            if x.get(field) != y.get(field):
                return {"item": i, field: x.get(field)}
        return None
    return result.get(parts[0].split("[")[0]) if parts else result


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--feeds", type=int, default=2000)
    g.add_argument("--android", default="../NotTennaPod-Android")
    g.add_argument("--ios", default="../NotTennaPod-iOS")
    g.add_argument("--out", default="contract/vectors/feeds")
    g.add_argument("--values-in", default=".work/feeds7a/values_in.json")
    a = sub.add_parser("assemble")
    a.add_argument("--android", default="../NotTennaPod-Android")
    a.add_argument("--device-out", required=True)
    a.add_argument("--feeds", default="contract/vectors/feeds")
    a.add_argument("--values-in", default=".work/feeds7a/values_in.json")
    a.add_argument("--values-out", default="contract/vectors/feed_values.json")
    a.add_argument("--symbols-out", default="contract/vectors/feed_date_symbols.json")
    c = sub.add_parser("compare")
    c.add_argument("--emulator", default="contract/vectors/feeds/expected.jsonl")
    c.add_argument("--jvm", required=True)
    c.add_argument("--examples", type=int, default=8)
    args = ap.parse_args()
    {"generate": generate, "assemble": assemble, "compare": compare}[args.cmd](args)


if __name__ == "__main__":
    main()
