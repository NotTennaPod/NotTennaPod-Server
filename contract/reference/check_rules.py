#!/usr/bin/env python3
"""Check `rules` suite vector files against the independent Python rule reference.

Usage: python3 contract/reference/check_rules.py [-v] FILE_OR_DIR...

Directories are searched recursively for *.json; files of other suites are skipped. Each
case's arrivals are run through rules.Engine and its resolutions, outcomes and records are
compared with `expected`. Prints one summary line per file and the first few failing cases
in detail (all of them with -v); exits 1 on any mismatch, malformed case or unreadable
file, 2 on usage errors.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import rules  # noqa: E402
from check_vectors import collect, load_vector_file, same, short  # noqa: E402

MAX_DETAILS = 5
MAX_LINES_PER_CASE = 12


def _rkey(r):
    return (r.get("entity"), r.get("id"), r.get("field_group"))


def compare(expected, actual):
    """Returns a list of human-readable differences (empty when equal)."""
    diffs = []
    for part in ("resolutions", "outcomes"):
        exp, act = expected.get(part), actual[part]
        if not isinstance(exp, dict):
            diffs.append("expected.%s is not an object" % part)
            continue
        for op in sorted(set(exp) | set(act)):
            if op not in act:
                diffs.append("%s %s: expected %s, got none" % (part[:-1], op, short(exp[op])))
            elif op not in exp:
                diffs.append("%s %s: expected none, got %s" % (part[:-1], op, short(act[op])))
            elif not same(exp[op], act[op]):
                diffs.append("%s %s: expected %s, got %s" % (part[:-1], op, short(exp[op]), short(act[op])))
    # PROTOCOL §7.6: the server decisions made at acceptance (absent = none).
    exp_dec, act_dec = expected.get("decisions", []), actual.get("decisions", [])
    if not isinstance(exp_dec, list) or not all(isinstance(d, dict) and "decision_id" in d for d in exp_dec):
        diffs.append("expected.decisions is not an array of decisions")
    else:
        em = {d["decision_id"]: d for d in exp_dec}
        am = {d["decision_id"]: d for d in act_dec}
        for did in sorted(set(em) | set(am)):
            if did not in am:
                diffs.append("decision %s: expected %s, got none" % (did, short(em[did])))
            elif did not in em:
                diffs.append("decision %s: expected none, got %s" % (did, short(am[did])))
            elif not same(em[did], am[did]):
                keys = sorted(k for k in set(em[did]) | set(am[did]) if not same(em[did].get(k), am[did].get(k)))
                for k in keys:
                    diffs.append("decision %s %s: expected %s, got %s"
                                 % (did, k, short(em[did].get(k)), short(am[did].get(k))))
        if not diffs and [d["decision_id"] for d in exp_dec] != [d["decision_id"] for d in act_dec]:
            diffs.append("expected.decisions is not sorted by decision_id")
    # PROTOCOL §7.6: the canonical sort values the live receipts recorded (absent =
    # none).
    exp_sv, act_sv = expected.get("sort_values", {}), actual.get("sort_values", {})
    if not isinstance(exp_sv, dict):
        diffs.append("expected.sort_values is not an object")
    else:
        for op in sorted(set(exp_sv) | set(act_sv)):
            if not same(exp_sv.get(op), act_sv.get(op)):
                diffs.append("sort values of %s: expected %s, got %s" % (op, short(exp_sv.get(op)), short(act_sv.get(op))))
    exp_recs, act_recs = expected.get("records"), actual["records"]
    if not isinstance(exp_recs, list) or not all(isinstance(r, dict) for r in exp_recs):
        diffs.append("expected.records is not an array of objects")
        return diffs
    groups = expected.get("field_groups")
    if groups is not None:
        # A case may compare only some field groups (docs/CONTRACT.md: the import cases,
        # whose other records are the import's own, outside this suite).
        if not isinstance(groups, list) or not all(isinstance(g, str) for g in groups):
            diffs.append("expected.field_groups is not an array of field groups")
            return diffs
        act_recs = [r for r in act_recs if r["field_group"] in groups]
        if any(r.get("field_group") not in groups for r in exp_recs):
            diffs.append("expected.records lists a field group outside expected.field_groups")
    exp_map = {}
    for r in exp_recs:
        if _rkey(r) in exp_map:
            diffs.append("expected.records lists %s twice" % (_rkey(r),))
        exp_map[_rkey(r)] = r
    act_map = {_rkey(r): r for r in act_recs}
    for k in sorted(set(exp_map) | set(act_map), key=lambda k: tuple(str(x) for x in k)):
        e, a = exp_map.get(k), act_map.get(k)
        name = "record %s/%s/%s" % k
        if a is None:
            diffs.append("%s: expected %s, got none" % (name, short(e.get("value", e))))
        elif e is None:
            diffs.append("%s: expected none, got %s" % (name, short(a["value"])))
        elif set(e) != {"entity", "id", "field_group", "value"}:
            diffs.append("%s: expected record has members %s (want entity, id, field_group, value)" % (name, sorted(e)))
        elif not same(e["value"], a["value"]):
            diffs.append("%s: expected %s, got %s" % (name, short(e["value"]), short(a["value"])))
    if not diffs and not same(exp_recs, act_recs):
        diffs.append("expected.records is not sorted by (entity, id, field_group)")
    return diffs


def check_case(case):
    """Returns (problem_kind, lines) or None when the case passes."""
    if not isinstance(case, dict):
        return "malformed", ["case is not an object"]
    exp = case.get("expected")
    if not isinstance(exp, dict) or not all(k in exp for k in ("resolutions", "outcomes", "records")):
        return "malformed", ["case lacks expected {resolutions, outcomes, records}"]
    try:
        actual = rules.run_case(case)
    except rules.RuleInputError as e:
        return "malformed", ["input the server would not have accepted: %s" % e]
    except Exception as e:  # a reference crash is a failure too
        return "crash", ["%s: %s" % (type(e).__name__, e)]
    diffs = compare(exp, actual)
    if diffs:
        return "mismatch", diffs
    return None


def check_file(path, verbose):
    try:
        doc = load_vector_file(path)
    except (OSError, UnicodeDecodeError, ValueError) as e:
        return False, None, "%s: unreadable vector file: %s" % (path, e), []
    if isinstance(doc, dict) and "suite" not in doc and "cases" not in doc:
        return True, None, "%s: skipped (data, not a vector file)" % path, []
    if not isinstance(doc, dict) or not isinstance(doc.get("cases"), list) or "suite" not in doc:
        return False, None, "%s: not a vector file ({suite, cases})" % path, []
    if doc["suite"] != "rules":
        return True, None, "%s: suite=%s skipped (not a rules file)" % (path, doc["suite"]), []
    cases = doc["cases"]
    failures = 0
    details = []
    for idx, case in enumerate(cases):
        res = check_case(case)
        if res is None:
            continue
        failures += 1
        if verbose or len(details) < MAX_DETAILS:
            kind, lines = res
            shown = lines if verbose else lines[:MAX_LINES_PER_CASE]
            more = "" if len(shown) == len(lines) else "\n      ... %d more" % (len(lines) - len(shown))
            details.append("  case %d: %s\n      %s%s" % (idx, kind, "\n      ".join(shown), more))
    line = "%s: suite=rules cases=%d failures=%d" % (path, len(cases), failures)
    return failures == 0, len(cases), line, details


def main(argv):
    verbose = False
    args = []
    for a in argv:
        if a in ("-v", "--verbose"):
            verbose = True
        else:
            args.append(a)
    if not args:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    files = collect(args)
    if not files:
        print("no vector files found", file=sys.stderr)
        return 1
    start = time.monotonic()
    all_ok = True
    total = 0
    checked = 0
    for path in files:
        ok, n, line, details = check_file(path, verbose)
        print(line)
        for d in details:
            print(d)
        all_ok = all_ok and ok
        if n is not None:
            total += n
            checked += 1
    if checked == 0:
        print("no rules vector files found", file=sys.stderr)
        all_ok = False
    print("%s: %d rules files, %d cases, %.1fs" % ("OK" if all_ok else "FAILED", checked, total, time.monotonic() - start))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
