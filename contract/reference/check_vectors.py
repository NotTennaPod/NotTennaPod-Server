#!/usr/bin/env python3
"""Check contract-v1 vector files against the independent Python reference.

Usage: python3 contract/reference/check_vectors.py FILE_OR_DIR...

Directories are searched recursively for *.json; files of suite "rules" are left to
check_rules.py, suite "feed_values" to the Go feed tests; suite "queue_sort" (PROTOCOL §7.6 canonical comparators) is checked
against queue_sort.py. Prints one summary line per file and
the first few mismatches in detail; exits 1 on any mismatch, unknown suite or unreadable
file, 2 on usage errors.
"""

import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ntp_contract as c  # noqa: E402

MAX_DETAILS = 5
_HEX_RE = re.compile(r"(?:[0-9a-f]{2})*")


class CaseError(Exception):
    """The vector case itself is malformed."""


class Mismatch(Exception):
    pass


def _no_dupes(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise ValueError("duplicate member %r in vector file" % k)
        d[k] = v
    return d


def _no_float(text):
    raise ValueError("non-integer number %s in vector file" % text)


def load_vector_file(path):
    with open(path, "rb") as f:
        data = f.read()
    return json.loads(
        data.decode("utf-8"),
        object_pairs_hook=_no_dupes,
        parse_float=_no_float,
        parse_constant=_no_float,
    )


def same(a, b):
    """Deep equality that distinguishes bool from int."""
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


def hexbytes(s, what):
    if not isinstance(s, str) or _HEX_RE.fullmatch(s) is None:
        raise CaseError("%s is not lowercase hex" % what)
    return bytes.fromhex(s)


def need(case, *fields):
    for f in fields:
        if f not in case:
            raise CaseError("missing field %r" % f)


def expects_error(case):
    e = case.get("error")
    if e is None or e is False:
        return False
    return True


def run(case, compute, expected):
    """compute() -> actual. If the case expects an error, compute must raise ContractError."""
    if expects_error(case):
        try:
            actual = compute()
        except c.ContractError:
            return
        raise Mismatch("expected rejection, got %s" % short(actual))
    try:
        actual = compute()
    except c.ContractError as e:
        raise Mismatch("expected %s, got error %s" % (short(expected), e))
    if not same(actual, expected):
        raise Mismatch("expected %s, got %s" % (short(expected), short(actual)))


def short(v, limit=300):
    try:
        s = json.dumps(v, ensure_ascii=True)
    except (TypeError, ValueError):
        s = repr(v)
    return s if len(s) <= limit else s[:limit] + "...(%d chars)" % len(s)


# --- suites -----------------------------------------------------------------


def s_canonical_json(case):
    if "input_hex" in case:
        raw = hexbytes(case["input_hex"], "input_hex")
    else:
        need(case, "input")
        raw = case["input"]
        if not isinstance(raw, str):
            raise CaseError("input is not a string")
    if not expects_error(case):
        need(case, "output")
    run(case, lambda: c.canonical(c.strict_decode(raw)).decode("utf-8"), case.get("output"))


def s_url(case):
    need(case, "input", "output")
    run(case, lambda: c.normalize_url(case["input"]), case["output"])


def s_guid(case):
    need(case, "input", "output")
    run(case, lambda: c.guid_trim(case["input"]), case["output"])


def s_length_prefixed(case):
    need(case, "fields", "output")
    run(case, lambda: c.length_prefixed(case["fields"]).hex(), case["output"])


def s_ids(case):
    need(case, "kind")
    kind = case["kind"]
    if not expects_error(case):
        need(case, "output")
    if kind == "podcast":
        need(case, "namespace", "op_id")
        f = lambda: c.podcast_id(case["namespace"], case["op_id"])  # noqa: E731
    elif kind == "generation":
        need(case, "namespace", "podcast_id", "op_id")
        f = lambda: c.generation_id(case["namespace"], case["podcast_id"], case["op_id"])  # noqa: E731
    elif kind == "episode":
        need(case, "namespace", "podcast_id", "k", "v")
        f = lambda: c.episode_id(case["namespace"], case["podcast_id"], case["k"], case["v"])  # noqa: E731
    else:
        raise CaseError("unknown kind %r" % (kind,))
    run(case, f, case.get("output"))


def s_queue_parse(case):
    need(case, "input", "valid")
    if not isinstance(case["valid"], bool):
        raise CaseError("valid is not a boolean")
    ok = c.is_valid_queue_key(case["input"])
    if ok != case["valid"]:
        raise Mismatch("expected valid=%s, got valid=%s" % (case["valid"], ok))
    if ok:
        n, m = c.parse_queue_key(case["input"])
        if c.format_queue_key(n, m) != case["input"]:
            raise Mismatch("format(parse(x)) != x")


def s_queue_compare(case):
    need(case, "a", "b")
    if not expects_error(case):
        need(case, "output")
    run(case, lambda: c.compare_queue_keys(case["a"], case["b"]), case.get("output"))
    if not expects_error(case):
        back = c.compare_queue_keys(case["b"], case["a"])
        if back != -case["output"]:
            raise Mismatch("compare(b, a) = %d is not the negation" % back)


def s_queue_between(case):
    need(case, "low", "high", "u")
    u = hexbytes(case["u"], "u")
    if len(u) != 16:
        raise CaseError("u is not 16 bytes")
    if not expects_error(case):
        need(case, "output")
    run(case, lambda: c.queue_between(case["low"], case["high"], u), case.get("output"))
    if not expects_error(case):
        out = case["output"]
        if case["low"] is not None and c.compare_queue_keys(case["low"], out) >= 0:
            raise Mismatch("output is not above low")
        if case["high"] is not None and c.compare_queue_keys(out, case["high"]) >= 0:
            raise Mismatch("output is not below high")


def s_causal_order(case):
    need(case, "nodes")
    err = case.get("error")
    if err is not None:
        if not isinstance(err, str):
            raise CaseError("error must be a code string")
        try:
            got = c.causal_order(case["nodes"])
        except c.ContractError as e:
            if e.code != err:
                raise Mismatch("expected error %s, got error %s (%s)" % (err, e.code, e))
            return
        raise Mismatch("expected error %s, got %s" % (err, short(got)))
    need(case, "output")
    run(case, lambda: c.causal_order(case["nodes"]), case["output"])


def s_time_normalization(case):
    need(case, "client_time", "receive_time", "dependency_times", "output")
    run(
        case,
        lambda: c.normalize_time(case["client_time"], case["receive_time"], case["dependency_times"]),
        case["output"],
    )


def s_content_hash(case):
    need(case, "fact")
    if not expects_error(case):
        need(case, "output")
    run(case, lambda: c.content_hash(case["fact"]), case.get("output"))


def s_mac(case):
    need(case, "key", "account_id", "object")
    key = hexbytes(case["key"], "key")
    if not expects_error(case):
        need(case, "output")
    run(case, lambda: c.mac(key, case["account_id"], case["object"]), case.get("output"))


def s_digest_chain(case):
    need(case, "previous", "object")
    prev = hexbytes(case["previous"], "previous")
    if not expects_error(case):
        need(case, "output")
    run(case, lambda: c.next_digest(prev, case["object"]).hex(), case.get("output"))


def s_cursor(case):
    need(case, "encoded")
    if expects_error(case):
        run(case, lambda: c.decode_cursor(case["encoded"]), None)
        return
    need(case, "cursor")
    run(case, lambda: c.encode_cursor(case["cursor"]), case["encoded"])
    run(case, lambda: c.decode_cursor(case["encoded"]), case["cursor"])


def s_parts(case):
    need(case, "parts", "content_hash")
    if not expects_error(case):
        need(case, "fact")
    run(case, lambda: c.reassemble_parts(case["parts"], case["content_hash"]), case.get("fact"))


def s_queue_sort(case):
    """PROTOCOL §7.6 canonical comparators: `items` (sort values in current queue order,
    missing members = missing values) and `output` {order: [item indices in sorted order]}
    for every keep-sorted order."""
    import queue_sort as qs  # the canonical sort lives beside the rule reference

    need(case, "items", "output")
    items, output = case["items"], case["output"]
    if not isinstance(items, list) or not isinstance(output, dict):
        raise CaseError("items must be an array and output an object")
    for i, it in enumerate(items):
        if not isinstance(it, dict) or set(it) - set(qs.VALUE_FIELDS) or not isinstance(it.get("podcast_id"), str):
            raise CaseError("item %d must be an object of %s with a podcast_id" % (i, ", ".join(qs.VALUE_FIELDS)))
        for f in ("title", "podcast_title"):
            if f in it and not isinstance(it[f], str):
                raise CaseError("item %d %s is not a string" % (i, f))
        for f in ("publication_time", "duration"):
            if f in it and (not isinstance(it[f], int) or isinstance(it[f], bool)):
                raise CaseError("item %d %s is not an integer" % (i, f))
    if set(output) != set(qs.KEEP_SORTED_ORDERS):
        raise CaseError("output must cover exactly the keep-sorted orders %s" % ", ".join(qs.KEEP_SORTED_ORDERS))
    for order in qs.KEEP_SORTED_ORDERS:
        got = qs.canonical_order(items, order)
        if not same(got, output[order]):
            raise Mismatch("%s: expected %s, got %s" % (order, short(output[order]), short(got)))


def s_strip_userinfo(case):
    """The embedded-URL userinfo rule (rules.strip_userinfo; docs/CONTRACT.md)."""
    import rules
    need(case, "input", "output")
    run(case, lambda: rules.strip_userinfo(case["input"]), case["output"])


SUITES = {
    "canonical_json": s_canonical_json,
    "url": s_url,
    "guid": s_guid,
    "length_prefixed": s_length_prefixed,
    "ids": s_ids,
    "queue_parse": s_queue_parse,
    "queue_compare": s_queue_compare,
    "queue_between": s_queue_between,
    "causal_order": s_causal_order,
    "time_normalization": s_time_normalization,
    "content_hash": s_content_hash,
    "mac": s_mac,
    "digest_chain": s_digest_chain,
    "cursor": s_cursor,
    "parts": s_parts,
    "queue_sort": s_queue_sort,
    "strip_userinfo": s_strip_userinfo,
}


# Suites checked elsewhere, and reference data that is no vector file.
CHECKED_ELSEWHERE = {
    "rules": "check_rules.py",
    "feed_values": "the Go feed tests (Android's own output on the emulator)",
    "import_android": "selftest_import.py",
}
REFERENCE_DATA = {
    "feed_date_symbols.json": "Android's Locale.US DateFormatSymbols, data for the feed date parser",
}


def check_file(path):
    """Returns (ok, summary_line, detail_lines)."""
    if os.path.basename(path) in REFERENCE_DATA:
        return True, "%s: reference data (%s)" % (path, REFERENCE_DATA[os.path.basename(path)]), []
    try:
        doc = load_vector_file(path)
    except (OSError, UnicodeDecodeError, ValueError) as e:
        return False, "%s: unreadable vector file: %s" % (path, e), []
    if not isinstance(doc, dict) or not isinstance(doc.get("cases"), list) or "suite" not in doc:
        return False, "%s: not a vector file ({suite, cases})" % path, []
    suite = doc["suite"]
    if suite in CHECKED_ELSEWHERE:
        return True, "%s: suite=%s skipped (checked by %s)" % (path, suite, CHECKED_ELSEWHERE[suite]), []
    fn = SUITES.get(suite)
    cases = doc["cases"]
    if fn is None:
        return False, "%s: suite=%s cases=%d UNKNOWN SUITE" % (path, suite, len(cases)), []
    failures = 0
    details = []
    for idx, case in enumerate(cases):
        problem = None
        try:
            if not isinstance(case, dict):
                raise CaseError("case is not an object")
            fn(case)
        except Mismatch as e:
            problem = "mismatch: %s" % e
        except CaseError as e:
            problem = "malformed case: %s" % e
        except Exception as e:  # an implementation crash is also a failure
            problem = "crash: %s: %s" % (type(e).__name__, e)
        if problem is not None:
            failures += 1
            if len(details) < MAX_DETAILS:
                details.append("  case %d: %s\n    case: %s" % (idx, problem, short(case, 600)))
    line = "%s: suite=%s cases=%d failures=%d" % (path, suite, len(cases), failures)
    return failures == 0, line, details


def collect(args):
    files = []
    for a in args:
        if os.path.isdir(a):
            for root, dirs, names in os.walk(a):
                dirs.sort()
                for name in sorted(names):
                    if name.endswith(".json"):
                        files.append(os.path.join(root, name))
        else:
            files.append(a)
    return files


def main(argv):
    if not argv:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    files = collect(argv)
    if not files:
        print("no vector files found", file=sys.stderr)
        return 1
    start = time.monotonic()
    all_ok = True
    total = 0
    for path in files:
        ok, line, details = check_file(path)
        print(line)
        for d in details:
            print(d)
        all_ok = all_ok and ok
        m = re.search(r"cases=(\d+)", line)
        total += int(m.group(1)) if m else 0
    print(
        "%s: %d files, %d cases, %.1fs"
        % ("OK" if all_ok else "FAILED", len(files), total, time.monotonic() - start)
    )
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
