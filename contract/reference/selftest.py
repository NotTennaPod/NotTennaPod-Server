#!/usr/bin/env python3
"""Self-test of the Python contract-v1 reference.

Hand-written cases from the protocol's stated examples, RFC 8785 examples,
hand-derived expectations of the literal algorithms, and property checks (queue key
ordering and uniqueness, causal order invariance under permutation, URL normalization
idempotence, canonical JSON round trips). Exits non-zero on any failure.
"""

import base64
import hashlib
import hmac
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ntp_contract as c  # noqa: E402

FAILURES = []
COUNT = [0]


def check(name, cond, detail=""):
    COUNT[0] += 1
    if not cond:
        FAILURES.append("%s %s" % (name, detail))


def eq(name, got, want):
    check(name, type(got) is type(want) and got == want, "\n    got:  %r\n    want: %r" % (got, want))


def raises(name, fn, code=None):
    COUNT[0] += 1
    try:
        got = fn()
    except c.ContractError as e:
        if code is not None and e.code != code:
            FAILURES.append("%s: expected error %s, got %s (%s)" % (name, code, e.code, e))
        return
    except Exception as e:  # wrong exception type is a bug too
        FAILURES.append("%s: expected ContractError, got %s: %s" % (name, type(e).__name__, e))
        return
    FAILURES.append("%s: expected rejection, got %r" % (name, got))


def canon_text(text):
    return c.canonical(c.strict_decode(text)).decode("utf-8")


# ---------------------------------------------------------------------------
# Canonical JSON
# ---------------------------------------------------------------------------


def test_canonical_json():
    # RFC 8785 section 3.2.3 sorting example (UTF-16 code unit order).
    rfc_sort_in = (
        '{\n  "\\u20ac": "Euro Sign",\n  "\\r": "Carriage Return",\n'
        '  "\\ufb33": "Hebrew Letter Dalet With Dagesh",\n  "1": "One",\n'
        '  "\\ud83d\\ude00": "Emoji: Grinning Face",\n  "\\u0080": "Control",\n'
        '  "\\u00f6": "Latin Small Letter O With Diaeresis"\n}'
    )
    rfc_sort_out = (
        '{"\\r":"Carriage Return","1":"One","\u0080":"Control",'
        '"\u00f6":"Latin Small Letter O With Diaeresis","\u20ac":"Euro Sign",'
        '"\U0001f600":"Emoji: Grinning Face","\ufb33":"Hebrew Letter Dalet With Dagesh"}'
    )
    eq("rfc8785 sort", canon_text(rfc_sort_in), rfc_sort_out)
    # RFC 8785 example (numbers removed: floats are outside the protocol domain).
    rfc_str_in = (
        '{\n  "string": "\\u20ac$\\u000F\\u000aA\'\\u0042\\u0022\\u005c\\\\\\"\\/",\n'
        '  "literals": [null, true, false]\n}'
    )
    rfc_str_out = '{"literals":[null,true,false],"string":"\u20ac$\\u000f\\nA\'B\\"\\\\\\\\\\"/"}'
    eq("rfc8785 strings", canon_text(rfc_str_in), rfc_str_out)
    raises(
        "rfc8785 numbers rejected",
        lambda: c.strict_decode("[333333333.33333329, 1E30, 4.50, 2e-3, 0.000000000000000000000000001]"),
    )

    ok = {
        '{"b":1,"a":[true,false,null,{}],"c":""}': '{"a":[true,false,null,{}],"b":1,"c":""}',
        ' \t\n\r[ 1 ,\t2 ]\n': "[1,2]",
        '"\\u00e9"': '"\u00e9"',
        '"\\u001f\\u007f"': '"\\u001f\u007f"',
        '"\\/"': '"/"',
        '"\\u2028\\u2029"': '"\u2028\u2029"',
        '"\\uD83D\\uDE00"': '"\U0001f600"',
        '"\\u0000"': '"\\u0000"',
        '"\\b\\f\\n\\r\\t"': '"\\b\\f\\n\\r\\t"',
        '"\\u0008\\u000C\\u000b\\u001F"': '"\\b\\f\\u000b\\u001f"',
        '"\u00e9\U0001f600\u007f"': '"\u00e9\U0001f600\u007f"',
        "[0,-1,9007199254740991,-9007199254740991]": "[0,-1,9007199254740991,-9007199254740991]",
        '"x"': '"x"',
        "1": "1",
        "true": "true",
        "null": "null",
        "{}": "{}",
        "[]": "[]",
        '{"\\uff61":1,"\\ud83d\\ude00":2}': '{"\U0001f600":2,"\uff61":1}',
        '{"a":{"z":1,"y":[{"b":2,"a":1}]}}': '{"a":{"y":[{"a":1,"b":2}],"z":1}}',
        '{"":1,"\\u0000":2}': '{"":1,"\\u0000":2}',
        '{"aa":1,"a":2,"b":3}': '{"a":2,"aa":1,"b":3}',
    }
    for text, want in ok.items():
        try:
            eq("canonical %r" % text, canon_text(text), want)
        except c.ContractError as e:
            FAILURES.append("canonical %r: unexpected error %s" % (text, e))
    eq("bytes input", c.canonical(c.strict_decode(b'{"b":[],"a":"\xc3\xa9"}')), b'{"a":"\xc3\xa9","b":[]}')

    eq("depth 64 arrays", canon_text("[" * 64 + "]" * 64), "[" * 64 + "]" * 64)
    raises("depth 65 arrays", lambda: c.strict_decode("[" * 65 + "]" * 65))
    deep_obj = '{"a":' * 64 + "1" + "}" * 64
    eq("depth 64 objects", canon_text(deep_obj), deep_obj)
    raises("depth 65 objects", lambda: c.strict_decode('{"a":' * 65 + "1" + "}" * 65))
    raises("depth 65 mixed", lambda: c.strict_decode('[{"a":' * 32 + "[1]" + "}]" * 32))

    bad = [
        "[1.0]", "[1.5]", "[1e2]", "[1E2]", "[1e-2]", "[-0]", "-0", "[9007199254740992]",
        "[-9007199254740992]", "[12345678901234567]", "[01]", "[00]", "[+1]", "[.5]", "[1.]",
        "[-]", "[- 1]", "NaN", "Infinity", "[1,]", '{"a":1,}', "{a:1}", "'x'", '"\\ud800"',
        '"\\udc00"', '"\\ud800\\u0041"', '"\\ud800x"', '"\\udc00\\ud800"', '"\\ud800\\ud800"',
        '"a\tb"', '"\x00"', '"\x1f"', '"\n"', '"\\x"', '"\\u12"', '"\\u12G4"', '"\\U0041"',
        '"\\\'"', "[1] x", "[1][2]", "", "   ", "\ufeff[]", '{"a" 1}', "[1 2]", "tru", "nul",
        "True", '"abc', "[", "{", '{"a":1,"\\u0061":2}', '{"a":1,"a":1}', '{"x":{"a":1,"a":2}}',
        '"\ud800"', '"\udc00"', "[1]\x00", "\x0b[]", "[1]\u00a0", "[1]/",
    ]
    for text in bad:
        raises("reject %r" % text, lambda t=text: c.strict_decode(t), "invalid_json")
    for raw in [b'"\xff"', b'"\xc3"', b'"\xc0\xaf"', b'"\xed\xa0\x80"', b'"\xf4\x90\x80\x80"', b"[1]\x80"]:
        raises("reject bytes %r" % raw, lambda r=raw: c.strict_decode(r), "invalid_json")

    raises("canonical float", lambda: c.canonical([1.5]))
    raises("canonical 2^53", lambda: c.canonical(2**53))
    raises("canonical lone surrogate", lambda: c.canonical("\ud800"))
    raises("canonical non-string key", lambda: c.canonical({1: 2}))
    eq("canonical bool vs int", c.canonical([True, 1, False, 0]), b"[true,1,false,0]")


def random_value(rng, depth=0):
    kind = rng.randrange(7 if depth < 4 else 4)
    if kind == 0:
        return rng.choice([None, True, False])
    if kind == 1:
        return rng.choice([0, 1, -1, c.MAX_SAFE_INTEGER, -c.MAX_SAFE_INTEGER, rng.randrange(-(10**12), 10**12)])
    if kind in (2, 3):
        return random_string(rng)
    if kind == 4:
        return [random_value(rng, depth + 1) for _ in range(rng.randrange(4))]
    return {random_string(rng): random_value(rng, depth + 1) for _ in range(rng.randrange(5))}


_CHARS = ["a", "B", "\"", "\\", "/", "\x00", "\x1f", "\x7f", "\n", "\u00e9", "\u2028", "\uff61", "\ufb33",
          "\U0001f600", "\U00010000", "\ue000", " ", "\t", "\u0080"]


def random_string(rng):
    return "".join(rng.choice(_CHARS) for _ in range(rng.randrange(6)))


def test_canonical_properties():
    rng = random.Random(8785)
    for _ in range(2000):
        v = random_value(rng)
        b = c.canonical(v)
        back = c.strict_decode(b)
        if c.canonical(back) != b:
            FAILURES.append("canonical round trip %r" % (v,))
            break
        if c.strict_decode(b.decode("utf-8")) != back:
            FAILURES.append("str/bytes decode differ %r" % (v,))
            break
    COUNT[0] += 1


# ---------------------------------------------------------------------------
# Identity primitives
# ---------------------------------------------------------------------------

A = "6f1c2c4e-6a1b-4f0e-9d3a-2b7c8e9f0a1b"
OP = "0b1e2d3c-4f5a-4b6c-8d7e-9fa0b1c2d3e4"


def test_length_prefixed_and_ids():
    eq("L()", c.length_prefixed([]).hex(), "")
    eq("L('')", c.length_prefixed([""]).hex(), "00000000")
    eq("L(ab)", c.length_prefixed(["ab"]).hex(), "000000026162")
    eq("L(e-acute)", c.length_prefixed(["\u00e9"]).hex(), "00000002c3a9")
    eq("L(emoji, a)", c.length_prefixed(["\U0001f600", "a"]).hex(), "00000004f09f9880" + "0000000161")
    eq("L(podcast-v1)", c.length_prefixed(["podcast-v1"]).hex(), "0000000a706f64636173742d7631")
    eq("L no NFC", c.length_prefixed(["e\u0301"]).hex(), "0000000365cc81")
    raises("L lone surrogate", lambda: c.length_prefixed(["\ud800"]))
    raises("L non-string", lambda: c.length_prefixed([1]))

    def lp(*fields):
        out = b""
        for f in fields:
            b = f.encode("utf-8")
            out += bytes([0, 0, 0, len(b)]) + b
        return out

    p = c.podcast_id(A, OP)
    eq("podcast id", p, "p1:" + hashlib.sha256(lp("podcast-v1", A, OP)).hexdigest())
    check("podcast id format", c.is_podcast_id(p))
    g = c.generation_id(A, p, OP)
    eq("generation id", g, "g1:" + hashlib.sha256(lp("generation-v1", A, p, OP)).hexdigest())
    check("generation id format", c.is_generation_id(g))
    for k in ("rss-guid", "atom-id", "enclosure"):
        e = c.episode_id(A, p, k, "v")
        eq("episode id " + k, e, "e1:" + hashlib.sha256(lp("episode-v1", A, p, k, "v")).hexdigest())
        check("episode id format", c.is_episode_id(e))
    v = c.episode_v_guid_enclosure("guid-1", "https://example.com/a.mp3")
    eq("guid+enclosure V", v, "guid-1\x1fhttps://example.com/a.mp3")
    eq(
        "episode id guid+enclosure",
        c.episode_id(A, p, "rss-guid+enclosure", v),
        "e1:" + hashlib.sha256(lp("episode-v1", A, p, "rss-guid+enclosure", v)).hexdigest(),
    )
    check("distinct kinds give distinct ids", c.episode_id(A, p, "rss-guid", "x") != c.episode_id(A, p, "atom-id", "x"))
    check("e0 form", c.is_episode_id("e0:" + OP) and not c.is_episode_id("e0:" + OP.upper()))
    raises("episode unknown kind", lambda: c.episode_id(A, p, "guid", "v"), "invalid_kind")
    raises("episode kind case", lambda: c.episode_id(A, p, "RSS-GUID", "v"), "invalid_kind")
    raises("uppercase namespace", lambda: c.podcast_id(A.upper(), OP), "invalid_uuid")
    raises("braced op", lambda: c.podcast_id(A, "{" + OP + "}"), "invalid_uuid")
    raises("unhyphenated op", lambda: c.podcast_id(A, OP.replace("-", "")), "invalid_uuid")
    raises("bad podcast id", lambda: c.generation_id(A, "p1:" + "A" * 64, OP))


def test_guid():
    cases = [
        ("  abc  ", "abc"),
        ("\t\n abc\r\x00", "abc"),
        ("\x01\x1f\x20abc\x20", "abc"),
        ("\u00a0abc\u00a0", "\u00a0abc\u00a0"),
        ("\u2003x\u3000", "\u2003x\u3000"),
        ("\x7fabc", "\x7fabc"),
        ("a b\tc", "a b\tc"),
        ("NULL", "NULL"),
        ("Null", "Null"),
        ("nul l", "nul l"),
        ("null!", "null!"),
        ("http://example.com/?p=1", "http://example.com/?p=1"),
        ("", None),
        ("   ", None),
        ("\x00\x1f", None),
        ("null", None),
        (" null\n", None),
        (None, None),
    ]
    for raw, want in cases:
        eq("guid %r" % (raw,), c.guid_trim(raw), want)


# ---------------------------------------------------------------------------
# URL normalization v1
# ---------------------------------------------------------------------------

URL_CASES = [
    # Design examples.
    ("https://%4A.EXAMPLE/audio", "https://%4A.example/audio"),
    ("https://[2001:DB8::1]:443/audio", "https://[2001:db8::1]/audio"),
    # Mixed-case scheme and host.
    ("HTTPS://Example.COM/Path/File.MP3", "https://example.com/Path/File.MP3"),
    ("hTtP://WWW.Example.com", "http://www.example.com"),
    # Default, non-default and empty ports.
    ("http://example.com:80/a", "http://example.com/a"),
    ("https://example.com:443/a", "https://example.com/a"),
    ("http://example.com:443/a", "http://example.com:443/a"),
    ("https://example.com:80/a", "https://example.com:80/a"),
    ("HTTP://example.com:80/a", "http://example.com/a"),
    ("http://example.com:/a", "http://example.com/a"),
    ("http://example.com:", "http://example.com"),
    ("http://example.com:080/a", "http://example.com:080/a"),
    ("http://example.com:8080?x", "http://example.com:8080?x"),
    ("ftp://example.com:21/a", "ftp://example.com:21/a"),
    ("http://:80/", "http:///"),
    # Userinfo with @ and :.
    ("http://User:Pa@ss@Example.COM:80/a", "http://User:Pa@ss@example.com/a"),
    ("https://u:p@HOST:443", "https://u:p@host"),
    ("http://USER@", "http://USER@"),
    ("http://a@[::1]:80/", "http://a@[::1]/"),
    ("http://[::1]@Host/", "http://[::1]@host/"),
    ("http://a:b@c:d@E:8080/", "http://a:b@c:d@e:8080/"),
    # Percent triplets in the host.
    ("http://%4a%ZZ.EX%41MPLE/", "http://%4a%zz.ex%41mple/"),
    ("http://%%4A%4/", "http://%%4A%4/"),
    ("http://H%3A:80", "http://h%3A"),
    ("http://%e2%82%AC.EXAMPLE/", "http://%e2%82%AC.example/"),
    # IPv6 literals with default, other and empty ports.
    ("https://[2001:DB8::1]:8443/audio", "https://[2001:db8::1]:8443/audio"),
    ("https://[2001:DB8::1]:/audio", "https://[2001:db8::1]/audio"),
    ("https://[2001:DB8::1]/audio", "https://[2001:db8::1]/audio"),
    ("http://[2001:DB8::1]:80", "http://[2001:db8::1]"),
    ("http://[::1]:80:80/", "http://[::1]:80:80/"),
    # Malformed brackets.
    ("http://[::1", "http://[::1"),
    ("http://[::A/x", "http://[::a/x"),
    ("http://[::1]x:80/a", "http://[::1]x:80/a"),
    ("http://[::A]X:80/", "http://[::a]x:80/"),
    ("http://[::1]]:80/", "http://[::1]]:80/"),
    # Fragments with and without an authority, fragment containing '?'.
    ("https://example.com/a#Frag", "https://example.com/a"),
    ("https://Example.com#frag", "https://example.com"),
    ("https://example.com/a#frag?x=1", "https://example.com/a"),
    ("https://example.com/a?x=1#f?y", "https://example.com/a?x=1"),
    ("https://ex.com#a?b", "https://ex.com"),
    ("https://EX.com?q#f", "https://ex.com?q"),
    ("https://ex.com/a#b#c", "https://ex.com/a"),
    ("https://ex.com/#", "https://ex.com/"),
    ("mailto:Someone@Example.com#x", "mailto:Someone@Example.com"),
    ("URN:ISBN:0451450523#Frag?x", "urn:ISBN:0451450523"),
    ("HTTP:Example#x", "http:Example"),
    ("http:/X#y", "http:/X"),
    ("http:", "http:"),
    # Trailing slash, path case, percent-escape case in paths, dot segments, query order.
    ("https://example.com/a/", "https://example.com/a/"),
    ("https://example.com/%7e%7E/%2F", "https://example.com/%7e%7E/%2F"),
    ("https://example.com/a/./b/../c", "https://example.com/a/./b/../c"),
    ("https://example.com/?b=2&a=1&utm_source=X", "https://example.com/?b=2&a=1&utm_source=X"),
    ("https://example.com", "https://example.com"),
    ("http:///Path", "http:///Path"),
    # Non-ASCII host: ASCII letters lowercased, others unchanged.
    ("https://B\u00dcCHER.Example/", "https://b\u00dccher.example/"),
    ("https://\u0130STANBUL.example/", "https://\u0130stanbul.example/"),
    # Scheme-less strings are unchanged (including any fragment).
    ("example.com/feed#x", "example.com/feed#x"),
    ("//Example.com/x#y", "//Example.com/x#y"),
    ("1http://X#y", "1http://X#y"),
    ("+a:b#c", "+a:b#c"),
    ("", ""),
    ("\u00e9http://X", "\u00e9http://X"),
    # Anything matching the scheme pattern is a scheme, even a host:port.
    ("Example.COM:80/a#x", "example.com:80/a"),
    # feed: and itpc: stay their own schemes.
    ("FEED://Example.com/rss#x", "feed://example.com/rss"),
    ("itpc://Example.com:80/rss", "itpc://example.com:80/rss"),
    ("feed:https://Example.com:443/rss#x", "feed:https://Example.com:443/rss"),
    ("ITPC:http://Example.com/rss", "itpc:http://Example.com/rss"),
    # Unbracketed host-and-port with more than one ':' is all host, no port.
    ("http://H:80:80/", "http://h:80:80/"),
    ("http://h::/", "http://h::/"),
    ("https://2001:DB8::1/audio", "https://2001:db8::1/audio"),
    ("http://u:p@H:80:80/x", "http://u:p@h:80:80/x"),
    ("http://h:/", "http://h/"),
    (":80", ":80"),
]


def test_url():
    for raw, want in URL_CASES:
        eq("url %r" % raw, c.normalize_url(raw), want)


def test_url_properties():
    rng = random.Random(3986)
    schemes = ["http", "HTTP", "https", "HtTpS", "feed", "itpc", "ftp", "x+y.z-1", "1x", ""]
    alphabet = list("aAzZ09.-_~:@[]%4fFgG/?#") + ["\u00e9", "\u00dc", " ", "%4A", "%zz", ":80", ":443", ":"]
    for _ in range(20000):
        scheme = rng.choice(schemes)
        sep = rng.choice(["://", ":", ":/", "", "://"])
        tail = "".join(rng.choice(alphabet) for _ in range(rng.randrange(14)))
        url = scheme + sep + tail
        once = c.normalize_url(url)
        twice = c.normalize_url(once)
        COUNT[0] += 1
        if twice != once:
            FAILURES.append("url not idempotent: %r -> %r -> %r" % (url, once, twice))
        if c._SCHEME_RE.match(url) is None and once != url:
            FAILURES.append("scheme-less url changed: %r" % url)
        if "#" in once and c._SCHEME_RE.match(url) is not None:
            FAILURES.append("fragment kept: %r -> %r" % (url, once))


# ---------------------------------------------------------------------------
# Queue keys
# ---------------------------------------------------------------------------


def test_queue_keys():
    valid = ["q1:1:1", "q1:2:1", "q1:2:3", "q1:4:b", "q1:131:1", "q1:3:7", "q1:200:1", "q1:8:ff",
             "q1:131:1" + "f" * 32, "q1:1048576:1", "q1:1048576:" + "f" * 262144]
    invalid = ["q1:0:1", "q1:1:2", "q1:01:1", "q1:2:01", "q1:2:5", "q1:2:4", "q1:3:A", "q1:4:B",
               "Q1:1:1", "q2:1:1", "q1:1:1 ", " q1:1:1", "q1:+1:1", "q1:1:", "q1::1", "q1:1:1:1",
               "q1:-1:1", "q1:1:0", "q1:0:0", "q1:1:-1", "q1:1:0x1", "q1:1", "q1:", "", "q1:1:1\n",
               "q1:\u0661:1", "q1:8:1_1", "q1:8:100", "q1:1.0:1", "q1:2:3/4", "q1:8:g", "q1:1048577:1",
               "q1:10000000:1", "q1:" + "9" * 5000 + ":1", "q1:8:" + "f" * 5000]
    for k in valid:
        check("valid key %s" % k, c.is_valid_queue_key(k))
        n, m = c.parse_queue_key(k)
        eq("format(parse(%s))" % k, c.format_queue_key(n, m), k)
    for k in invalid:
        check("invalid key %r" % k, not c.is_valid_queue_key(k))
    raises("format even", lambda: c.format_queue_key(3, 2))
    raises("format >= 1", lambda: c.format_queue_key(1, 3))

    cmp = [
        ("q1:1:1", "q1:2:3", -1),
        ("q1:2:3", "q1:1:1", 1),
        ("q1:2:1", "q1:1:1", -1),
        ("q1:1:1", "q1:1:1", 0),
        ("q1:3:5", "q1:2:3", -1),
        ("q1:200:1", "q1:1:1", -1),
        ("q1:131:1", "q1:262:1", 1),
        # Bytewise order would say the opposite here.
        ("q1:3:7", "q1:10:3ff", -1),
        ("q1:10:3ff", "q1:3:7", 1),
        ("q1:2:3", "q1:10:1", 1),
    ]
    for a, b, want in cmp:
        eq("compare %s %s" % (a, b), c.compare_queue_keys(a, b), want)
    raises("compare invalid", lambda: c.compare_queue_keys("q1:1:2", "q1:1:1"))

    u0 = bytes(16)
    umax = b"\xff" * 16
    eq("between sentinels U=0", c.queue_between(None, None, u0), "q1:131:1")
    eq("between sentinels U=max", c.queue_between(None, None, umax), "q1:131:1" + "f" * 32)
    eq("between sentinels U=5", c.queue_between(None, None, (5).to_bytes(16, "big")), "q1:131:b")
    eq("after 1/2 U=0", c.queue_between("q1:1:1", None, u0), "q1:132:8" + "0" * 31 + "1")
    eq("before 2^-131 U=0", c.queue_between(None, "q1:131:1", u0), "q1:262:1")
    eq("before 2^-131 U=max", c.queue_between(None, "q1:131:1", umax), "q1:262:1" + "f" * 32)
    # Between 1/4 and 3/4: D = 1/2, smallest n with 2^(n-1) > 2^130 is n = 132;
    # m > 2^130, m = 1 (mod 2^129) -> m = 2^130 + 1.
    eq("between 1/4 and 3/4 U=0", c.queue_between("q1:2:1", "q1:2:3", u0), "q1:132:4" + "0" * 31 + "1")
    eq("uuid_to_u", c.uuid_to_u(OP).hex(), OP.replace("-", ""))
    eq("between with int U", c.queue_between(None, None, 5), "q1:131:b")
    raises("between equal", lambda: c.queue_between("q1:1:1", "q1:1:1", u0), "invalid_range")
    raises("between reversed", lambda: c.queue_between("q1:2:3", "q1:1:1", u0), "invalid_range")
    raises("between bad key", lambda: c.queue_between("q1:1:2", None, u0))
    raises("between short U", lambda: c.queue_between(None, None, b"\x00" * 15))
    raises("between beyond n bound", lambda: c.queue_between(None, "q1:1048576:1", u0), "key_too_long")
    raises("format beyond n bound", lambda: c.format_queue_key(1048577, 1))
    eq("between near n bound", c.queue_between(None, "q1:1048445:1", u0), "q1:1048576:1")
    k1 = c.queue_between("q1:2:1", "q1:2:3", OP.encode()[:16])
    eq("equal-U determinism", c.queue_between("q1:2:1", "q1:2:3", OP.encode()[:16]), k1)


def _check_sorted_unique(name, keys):
    for a, b in zip(keys, keys[1:]):
        if c.compare_queue_keys(a, b) != -1:
            FAILURES.append("%s: %s !< %s" % (name, a, b))
            return
    if len(set(keys)) != len(keys):
        FAILURES.append("%s: duplicate keys" % name)
    COUNT[0] += 1


def test_queue_properties():
    rng = random.Random(129)
    # Random insertions with distinct U.
    keys = []
    used_u = set()
    for _ in range(3000):
        u = rng.getrandbits(128)
        while u in used_u:
            u = rng.getrandbits(128)
        used_u.add(u)
        pos = rng.randrange(len(keys) + 1)
        low = keys[pos - 1] if pos > 0 else None
        high = keys[pos] if pos < len(keys) else None
        keys.insert(pos, c.queue_between(low, high, u.to_bytes(16, "big")))
    _check_sorted_unique("random insertions", keys)
    top = max(c.parse_queue_key(k)[0] for k in keys)
    resorted = sorted(keys, key=lambda k: (lambda n, m: m << (top - n))(*c.parse_queue_key(k)))
    check("numeric sort agrees", resorted == keys)

    # 100 insertions into the smallest gap (sentinels included).
    keys = []
    for i in range(100):
        bounds = [(0, 0)] + [c.parse_queue_key(k) for k in keys] + [(0, 1)]
        top = max(n for n, _ in bounds)
        vals = [m << (top - n) for n, m in bounds]
        gaps = [vals[j + 1] - vals[j] for j in range(len(vals) - 1)]
        j = gaps.index(min(gaps))
        low = keys[j - 1] if j > 0 else None
        high = keys[j] if j < len(keys) else None
        keys.insert(j, c.queue_between(low, high, rng.getrandbits(128).to_bytes(16, "big")))
    _check_sorted_unique("smallest gap", keys)

    # Repeated insertion at the front and at the back, with the same U each time.
    u = rng.getrandbits(128).to_bytes(16, "big")
    front, back = [], []
    for _ in range(100):
        front.insert(0, c.queue_between(None, front[0] if front else None, u))
        back.append(c.queue_between(back[-1] if back else None, None, u))
    _check_sorted_unique("front insertion", front)
    _check_sorted_unique("back insertion", back)

    # Distinct U between the same bounds never collide; equal U is deterministic.
    for low, high in [(None, None), ("q1:2:1", "q1:2:3"), (None, "q1:262:1"), ("q1:8:ff", None)]:
        seen = set()
        for _ in range(500):
            k = c.queue_between(low, high, rng.getrandbits(128).to_bytes(16, "big"))
            seen.add(k)
            if (low is not None and c.compare_queue_keys(low, k) != -1) or (
                high is not None and c.compare_queue_keys(k, high) != -1
            ):
                FAILURES.append("between out of bounds %s %s %s" % (low, high, k))
        check("distinct U distinct keys %s %s" % (low, high), len(seen) == 500)


# ---------------------------------------------------------------------------
# Causal order and time normalization
# ---------------------------------------------------------------------------


def node(i, deps, t):
    return {"id": i, "dependencies": list(deps), "normalized_time": t}


def test_causal_order():
    eq("empty", c.causal_order([]), [])
    eq("chain", c.causal_order([node("c", ["b"], 1), node("b", ["a"], 2), node("a", [], 3)]), ["a", "b", "c"])
    eq("ties by id", c.causal_order([node("b", [], 5), node("a", [], 5), node("c", [], 4)]), ["c", "a", "b"])
    # A pairwise comparator is not transitive here: y < x by time, x < z by time, z < y by
    # dependency. Kahn emits x (smallest ready), then z, then y.
    eq("non-transitive", c.causal_order([node("x", [], 3), node("y", ["z"], 1), node("z", [], 5)]), ["x", "z", "y"])
    # A comparator falling back to time for unrelated nodes would put w (t=2) before z.
    eq(
        "ready set only",
        c.causal_order([node("w", ["z"], 2), node("z", ["v"], 9), node("v", [], 1), node("u", [], 4)]),
        ["v", "u", "z", "w"],
    )
    # Id order is bytewise UTF-8 (U+FF61 < U+1F600), not UTF-16 (where U+1F600 sorts first).
    eq("utf-8 id order", c.causal_order([node("\U0001f600", [], 1), node("\uff61", [], 1)]), ["\uff61", "\U0001f600"])
    eq("negative times", c.causal_order([node("a", [], 0), node("b", [], -5)]), ["b", "a"])
    eq("repeated dependency", c.causal_order([node("b", ["a", "a"], 0), node("a", [], 1)]), ["a", "b"])

    # Fact -> decision -> fact with the server clock moving backward.
    f1_time = c.normalize_time(1000, 1000, [])
    d_time = c.normalize_time(900, 900, [f1_time])  # decision: client = receive = server_now
    f2_time = c.normalize_time(950, 920, [d_time])
    eq("backward clock decision", d_time, 1000)
    eq("backward clock fact", f2_time, 1000)
    f1, d, f2 = "ffff-fact-1", "eeee-decision", "aaaa-fact-2"
    eq(
        "fact -> decision -> fact",
        c.causal_order([node(f2, [d], f2_time), node(d, [f1], d_time), node(f1, [], f1_time)]),
        [f1, d, f2],
    )

    raises("missing", lambda: c.causal_order([node("a", ["zz"], 1)]), "dependency_missing")
    try:
        c.causal_order([node("a", ["zz", "yy"], 1), node("b", ["yy"], 1)])
    except c.ContractError as e:
        eq("missing list", e.missing, ["yy", "zz"])
    raises("cycle", lambda: c.causal_order([node("a", ["b"], 1), node("b", ["a"], 1)]), "cycle")
    raises("self loop", lambda: c.causal_order([node("a", ["a"], 1)]), "cycle")
    raises(
        "cycle behind ready nodes",
        lambda: c.causal_order([node("r", [], 0), node("a", ["c", "r"], 1), node("b", ["a"], 1), node("c", ["b"], 1)]),
        "cycle",
    )
    raises("duplicate", lambda: c.causal_order([node("a", [], 1), node("a", [], 2)]), "duplicate")
    raises("duplicate before missing", lambda: c.causal_order([node("a", ["q"], 1), node("a", [], 2)]), "duplicate")
    raises("missing before cycle", lambda: c.causal_order([node("a", ["b", "q"], 1), node("b", ["a"], 1)]),
           "dependency_missing")


def naive_order(nodes):
    """Independent O(n^2) restatement: repeatedly emit the smallest node whose deps are emitted."""
    done, out = set(), []
    remaining = list(nodes)
    while remaining:
        ready = [n for n in remaining if all(d in done for d in n["dependencies"])]
        best = min(ready, key=lambda n: (n["normalized_time"], n["id"].encode("utf-8")))
        out.append(best["id"])
        done.add(best["id"])
        remaining.remove(best)
    return out


def test_causal_properties():
    rng = random.Random(4)
    pool = ["a", "b", "\u00e9", "\uff61", "\U0001f600", "Z", "0", "-"]
    for trial in range(300):
        count = rng.randrange(1, 30)
        ids = []
        while len(ids) < count:
            candidate = "".join(rng.choice(pool) for _ in range(rng.randrange(1, 4)))
            if candidate not in ids:
                ids.append(candidate)
        nodes = []
        for i, nid in enumerate(ids):
            deps = rng.sample(ids[:i], rng.randrange(min(i, 3) + 1)) if i else []
            nodes.append(node(nid, deps, rng.randrange(4)))
        expected = c.causal_order(nodes)
        if expected != naive_order(nodes):
            FAILURES.append("causal order differs from naive restatement (trial %d)" % trial)
            break
        position = {nid: k for k, nid in enumerate(expected)}
        if any(position[d] >= position[n["id"]] for n in nodes for d in n["dependencies"]):
            FAILURES.append("causal order violates a dependency (trial %d)" % trial)
            break
        for _ in range(5):
            shuffled = nodes[:]
            rng.shuffle(shuffled)
            for n in shuffled:
                rng.shuffle(n["dependencies"])
            if c.causal_order(shuffled) != expected:
                FAILURES.append("causal order depends on input order (trial %d)" % trial)
                break
    COUNT[0] += 1


def test_time_normalization():
    cases = [
        (50, 100, [], 50),
        (150, 100, [], 100),
        (100, 100, [], 100),
        (50, 100, [70], 70),
        (80, 100, [70], 80),
        (120, 100, [70, 10], 100),
        (50, 60, [70], 70),
        (80, 60, [70], 70),
        (65, 60, [10, 70, 30], 70),
        (-5, 10, [], -5),
        (100, 100, [100], 100),
    ]
    for client, receive, deps, want in cases:
        eq("normalize %r" % ((client, receive, deps),), c.normalize_time(client, receive, deps), want)
    raises("normalize float", lambda: c.normalize_time(1.5, 2, []))


# ---------------------------------------------------------------------------
# Facts, hashes, MACs, digests, cursors, parts
# ---------------------------------------------------------------------------

AUTHOR = "1a2b3c4d-0000-4000-8000-000000000001"
DEP1 = "00000000-0000-4000-8000-000000000001"
DEP2 = "0000000a-0000-4000-8000-000000000001"
FACT = {
    "type": "episode.mark_played",
    "op_id": OP,
    "author_id": AUTHOR,
    "author_seq": 2,
    "previous_op_id": DEP1,
    "dependencies": [DEP1, DEP2],
    "type_version": 1,
    "client_time": 1759600000123,
    "targets": [
        {"entity": "episode", "id": "e1:" + "ab" * 32, "generation": "g1:" + "cd" * 32},
        {"entity": "podcast", "id": "p1:" + "01" * 32},
    ],
    "payload": {"reset_position": True, "note": "caf\u00e9"},
}
FACT_CANON = (
    '{"author_id":"%s","author_seq":2,"client_time":1759600000123,"dependencies":["%s","%s"],'
    '"op_id":"%s","payload":{"note":"caf\u00e9","reset_position":true},"previous_op_id":"%s",'
    '"targets":[{"entity":"episode","generation":"g1:%s","id":"e1:%s"},{"entity":"podcast","id":"p1:%s"}],'
    '"type":"episode.mark_played","type_version":1}'
    % (AUTHOR, DEP1, DEP2, OP, DEP1, "cd" * 32, "ab" * 32, "01" * 32)
).encode("utf-8")


def test_fact_hash():
    eq("fact canonical", c.canonical(FACT), FACT_CANON)
    eq("content hash", c.content_hash(FACT), hashlib.sha256(FACT_CANON).hexdigest())
    first = dict(FACT)
    del first["previous_op_id"]
    check("first fact without previous_op_id", c.content_hash(first) != c.content_hash(FACT))

    def variant(**changes):
        f = dict(FACT)
        for k, v in changes.items():
            if v is _DELETE:
                del f[k]
            else:
                f[k] = v
        return f

    rejected = {
        "extra field": variant(extra=1),
        "missing payload": variant(payload=_DELETE),
        "missing author_id": variant(author_id=_DELETE),
        "null previous": variant(previous_op_id=None),
        "uppercase author_id": variant(author_id=AUTHOR.upper()),
        "unsorted deps": variant(dependencies=[DEP2, DEP1]),
        "duplicate deps": variant(dependencies=[DEP1, DEP1]),
        "non-uuid dep": variant(dependencies=["x"]),
        "author_seq 0": variant(author_seq=0),
        "type_version 0": variant(type_version=0),
        "type_version bool": variant(type_version=True),
        "client_time string": variant(client_time="1"),
        "unsorted targets": variant(targets=list(reversed(FACT["targets"]))),
        "duplicate targets": variant(targets=[FACT["targets"][1], FACT["targets"][1]]),
        "target extra": variant(targets=[{"entity": "podcast", "id": "p", "x": "y"}]),
        "target null generation": variant(targets=[{"entity": "podcast", "id": "p", "generation": None}]),
        "payload array": variant(payload=[]),
        "type number": variant(type=1),
    }
    for name, f in rejected.items():
        raises("fact rejected: " + name, lambda f=f: c.validate_fact(f), "rejected")
        # The content hash is a function of the canonical bytes only.
        eq("hash without validation: " + name, c.content_hash(f), hashlib.sha256(c.canonical(f)).hexdigest())
    unarchivable = {
        "not object": [1],
        "missing op_id": variant(op_id=_DELETE),
        "uppercase op_id": variant(op_id=OP.upper()),
        "numeric op_id": variant(op_id=7),
        "float": variant(client_time=1.5),
        "huge integer": variant(client_time=2**53),
    }
    for name, f in unarchivable.items():
        raises("fact unarchivable: " + name, lambda f=f: c.validate_fact(f), "unarchivable")
    raises("hash of float", lambda: c.content_hash(variant(client_time=1.5)))
    c.validate_fact(FACT)
    c.validate_fact(first)
    # Targets compare generation-or-empty: a missing generation sorts before any generation.
    c.validate_fact(variant(targets=[{"entity": "e", "id": "x"}, {"entity": "e", "id": "x", "generation": "g"}]))
    COUNT[0] += 1


_DELETE = object()


def test_mac_and_digest():
    key = bytes(range(32))
    account = "9b2f3a4c-5d6e-4f70-8a9b-0c1d2e3f4a5b"
    receipt = {
        "op_id": OP,
        "content_hash": "00" * 32,
        "outcome": "applied",
        "normalized_time": 1759600000123,
        "epoch": 1,
        "seq": 7,
        "key_id": "k1",
        "mac": "ignored",
    }
    signed = (
        '{"account_id":"%s","content_hash":"%s","epoch":1,"key_id":"k1","normalized_time":1759600000123,'
        '"op_id":"%s","outcome":"applied","seq":7}' % (account, "00" * 32, OP)
    ).encode()
    want = hmac.new(key, signed, hashlib.sha256).hexdigest()
    eq("receipt mac", c.mac(key, account, receipt), want)
    no_mac = dict(receipt)
    del no_mac["mac"]
    eq("mac member ignored", c.mac(key, account, no_mac), want)
    check("verify mac", c.verify_mac(key, account, dict(receipt, mac=want)))
    check("verify mac other account", not c.verify_mac(key, "9b2f3a4c-5d6e-4f70-8a9b-0c1d2e3f4a5c", dict(receipt, mac=want)))
    raises("mac with account_id member", lambda: c.mac(key, account, dict(receipt, account_id=account)))
    raises("mac bad account", lambda: c.mac(key, account.upper(), receipt))

    commit = {"epoch": 1, "seq": 1, "accepted": [], "decisions": [], "slots": [], "records": []}
    d1 = c.next_digest(c.ZERO_DIGEST, commit)
    eq(
        "digest 1",
        d1.hex(),
        hashlib.sha256(bytes(32) + b'{"accepted":[],"decisions":[],"epoch":1,"records":[],"seq":1,"slots":[]}').hexdigest(),
    )
    commit2 = dict(commit, seq=2)
    eq("chain", c.chain_digest([commit, commit2]), c.next_digest(d1, commit2))
    raises("digest short previous", lambda: c.next_digest(b"\x00" * 31, commit))


def b64(b):
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def test_cursor():
    cur = {"log_id": "6f1c2c4e-0000-4000-8000-00000000000a", "epoch": 3, "seq": 42, "digest": "0f" * 32}
    text = '{"digest":"%s","epoch":3,"log_id":"%s","seq":42}' % ("0f" * 32, cur["log_id"])
    enc = c.encode_cursor(cur)
    eq("cursor encode", enc, b64(text.encode()))
    check("cursor alphabet", "=" not in enc and "+" not in enc and "/" not in enc)
    eq("cursor decode", c.decode_cursor(enc), cur)
    eq("cursor seq 0", c.decode_cursor(c.encode_cursor(dict(cur, seq=0, digest="00" * 32)))["seq"], 0)

    def enc_text(t):
        return b64(t.encode())

    bad = {
        "padding": enc + "=" * (-len(enc) % 4 or 4),
        "unsorted": enc_text('{"log_id":"%s","epoch":3,"seq":42,"digest":"%s"}' % (cur["log_id"], "0f" * 32)),
        "whitespace": enc_text(text.replace(",", ", ")),
        "uppercase digest": enc_text(text.replace("0f" * 32, "0F" * 32)),
        "short digest": enc_text(text.replace("0f" * 32, "0f" * 31)),
        "extra field": enc_text(text[:-1] + ',"x":1}'),
        "missing field": enc_text('{"digest":"%s","epoch":3,"log_id":"%s"}' % ("0f" * 32, cur["log_id"])),
        "negative epoch": enc_text(text.replace('"epoch":3', '"epoch":-3')),
        "string seq": enc_text(text.replace('"seq":42', '"seq":"42"')),
        "plus": "+" + enc[1:],
        "slash": "/" + enc[1:],
        "not json": b64(b"hello"),
        "not utf-8": b64(b'{"x":"\xff"}'),
        "array": enc_text("[1]"),
        "empty": "",
        "length 1 mod 4": "A" * 5,
        "space": enc[:4] + " " + enc[4:],
    }
    for name, value in bad.items():
        raises("cursor reject " + name, lambda v=value: c.decode_cursor(v), "invalid_cursor")
    for name, log_id in (("short", "a"), ("uppercase", cur["log_id"].upper()), ("number", 5)):
        raises("cursor log_id " + name, lambda v=log_id: c.encode_cursor(dict(cur, log_id=v)), "invalid_cursor")
        if isinstance(log_id, str):
            raises("cursor decode log_id " + name, lambda v=log_id: c.decode_cursor(enc_text(text.replace(cur["log_id"], v))),
                   "invalid_cursor")
    # Non-zero trailing bits: same bytes, different (non-canonical) last character.
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    tested = 0
    for seq in (1, 12, 123, 1234):
        e = c.encode_cursor(dict(cur, seq=seq))
        if len(e) % 4:
            tested += 1
            tweaked = e[:-1] + alphabet[alphabet.index(e[-1]) | 1]
            raises("cursor reject trailing bits", lambda t=tweaked: c.decode_cursor(t), "invalid_cursor")
    check("trailing-bit cursor cases exist", tested > 0)
    for raw in (b"a", b"ab", b"abc", b"\xff\x00"):
        eq("b64url round trip %r" % raw, c.b64url_decode(c.b64url_encode(raw)), raw)
    raises("b64url trailing bits", lambda: c.b64url_decode("YR"), "invalid_base64")
    raises("b64url padding", lambda: c.b64url_decode("YQ=="), "invalid_base64")
    raises("encode bad cursor", lambda: c.encode_cursor(dict(cur, digest="0F" * 32)))


def test_parts():
    data = FACT_CANON
    h = hashlib.sha256(data).hexdigest()
    slices = [data[:1], data[1:11], data[11:200], data[200:]]
    eq("parts reassembly", c.reassemble_parts([b64(s) for s in slices], h), c.strict_decode(data))
    eq("single part", c.reassemble_parts([b64(data)], h), c.strict_decode(data))
    # Slices may split inside a multi-byte UTF-8 character.
    k = data.index("\u00e9".encode()) + 1
    eq("split inside utf-8", c.reassemble_parts([b64(data[:k]), b64(data[k:])], h), c.strict_decode(data))
    raises("wrong hash", lambda: c.reassemble_parts([b64(data)], "00" * 32), "invalid_part")
    raises("uppercase hash", lambda: c.reassemble_parts([b64(data)], h.upper()), "invalid_part")
    raises("out of order", lambda: c.reassemble_parts([b64(s) for s in reversed(slices)], h), "invalid_part")
    raises("missing slice", lambda: c.reassemble_parts([b64(s) for s in slices[:-1]], h), "invalid_part")
    loose = data.replace(b",", b", ")
    raises("non-canonical bytes", lambda: c.reassemble_parts([b64(loose)], hashlib.sha256(loose).hexdigest()),
           "invalid_part")
    padded = base64.urlsafe_b64encode(data).decode()
    if padded.endswith("="):
        raises("padded part", lambda: c.reassemble_parts([padded], h), "invalid_part")
    raises("empty slice", lambda: c.reassemble_parts([b64(data[:5]), "", b64(data[5:])], h), "invalid_part")
    raises("no parts", lambda: c.reassemble_parts([], h), "invalid_part")
    arr = b"[1,2]"
    raises("not an object", lambda: c.reassemble_parts([b64(arr)], hashlib.sha256(arr).hexdigest()), "invalid_part")
    # Fact validation is a later, separate step: an invalid envelope still reassembles.
    bad_fact = dict(FACT, extra=1)
    bf = c.canonical(bad_fact)
    eq("invalid fact reassembles", c.reassemble_parts([b64(bf)], hashlib.sha256(bf).hexdigest()), bad_fact)


def main():
    tests = [
        test_canonical_json,
        test_canonical_properties,
        test_length_prefixed_and_ids,
        test_guid,
        test_url,
        test_url_properties,
        test_queue_keys,
        test_queue_properties,
        test_causal_order,
        test_causal_properties,
        test_time_normalization,
        test_fact_hash,
        test_mac_and_digest,
        test_cursor,
        test_parts,
    ]
    for t in tests:
        try:
            t()
        except Exception as e:  # a crash in a test is a failure, keep going
            FAILURES.append("%s crashed: %s: %s" % (t.__name__, type(e).__name__, e))
    for f in FAILURES:
        print("FAIL", f)
    print("%s: %d checks, %d failures" % ("OK" if not FAILURES else "FAILED", COUNT[0], len(FAILURES)))
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
