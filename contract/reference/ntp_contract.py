"""Independent Python 3 reference implementation of the NotTennaPod wire contract (contract-v1).

Written only from docs/PROTOCOL.md (§3, §6) and docs/CONTRACT.md, never from the Go implementation. Standard library only; the strict
protocol JSON decoder and the RFC 8785 (JCS) serializer are implemented here by hand.

Every function raises ContractError for input the contract says must be rejected.
"""

import base64
import hashlib
import heapq
import hmac
import re

__all__ = [
    "ContractError",
    "MAX_SAFE_INTEGER",
    "MAX_DEPTH",
    "ZERO_DIGEST",
    "strict_decode",
    "canonical",
    "length_prefixed",
    "is_uuid",
    "podcast_id",
    "generation_id",
    "episode_id",
    "episode_v_guid_enclosure",
    "is_podcast_id",
    "is_generation_id",
    "is_episode_id",
    "guid_trim",
    "normalize_url",
    "MAX_QUEUE_N",
    "parse_queue_key",
    "format_queue_key",
    "is_valid_queue_key",
    "compare_queue_keys",
    "queue_between",
    "uuid_to_u",
    "causal_order",
    "normalize_time",
    "validate_fact",
    "content_hash",
    "mac",
    "verify_mac",
    "next_digest",
    "chain_digest",
    "b64url_encode",
    "b64url_decode",
    "encode_cursor",
    "decode_cursor",
    "reassemble_parts",
]

MAX_SAFE_INTEGER = 2**53 - 1
MAX_DEPTH = 64
ZERO_DIGEST = bytes(32)


class ContractError(ValueError):
    """Input rejected by the contract. `code` is a short machine-readable reason."""

    def __init__(self, code, message=None, missing=None):
        super().__init__(code if message is None else "%s: %s" % (code, message))
        self.code = code
        self.missing = missing


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def _check_safe_int(v, what="integer"):
    if not _is_int(v):
        raise ContractError("invalid_value", "%s is not an integer" % what)
    if v > MAX_SAFE_INTEGER or v < -MAX_SAFE_INTEGER:
        raise ContractError("invalid_value", "%s outside +/-(2^53-1)" % what)


# ---------------------------------------------------------------------------
# 1. Strict protocol JSON decoding
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"[ \t\n\r]*")
_NUM_RE = re.compile(r"-?(?:0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?")
_STR_RUN_RE = re.compile(r'[^"\\\x00-\x1f]*')
_HEX4_RE = re.compile(r"[0-9a-fA-F]{4}")
_SURROGATE_RE = re.compile("[\ud800-\udfff]")
_SIMPLE_ESCAPES = {
    '"': '"',
    "\\": "\\",
    "/": "/",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
}


def _jerr(message):
    return ContractError("invalid_json", message)


class _Decoder(object):
    __slots__ = ("s", "n")

    def __init__(self, s):
        self.s = s
        self.n = len(s)

    def ws(self, i):
        return _WS_RE.match(self.s, i).end()

    def value(self, i, depth):
        s = self.s
        if i >= self.n:
            raise _jerr("unexpected end of input")
        c = s[i]
        if c == "{":
            return self.obj(i + 1, depth + 1)
        if c == "[":
            return self.arr(i + 1, depth + 1)
        if c == '"':
            return self.string(i + 1)
        if c == "t" and s.startswith("true", i):
            return True, i + 4
        if c == "f" and s.startswith("false", i):
            return False, i + 5
        if c == "n" and s.startswith("null", i):
            return None, i + 4
        if c == "-" or "0" <= c <= "9":
            return self.number(i)
        raise _jerr("unexpected character %r at offset %d" % (c, i))

    def obj(self, i, depth):
        if depth > MAX_DEPTH:
            raise _jerr("nesting deeper than %d levels" % MAX_DEPTH)
        s, n = self.s, self.n
        result = {}
        i = self.ws(i)
        if i < n and s[i] == "}":
            return result, i + 1
        while True:
            if i >= n or s[i] != '"':
                raise _jerr("expected member name at offset %d" % i)
            key, i = self.string(i + 1)
            if key in result:
                raise _jerr("duplicate member name %r" % key)
            i = self.ws(i)
            if i >= n or s[i] != ":":
                raise _jerr("expected ':' at offset %d" % i)
            i = self.ws(i + 1)
            val, i = self.value(i, depth)
            result[key] = val
            i = self.ws(i)
            if i < n and s[i] == ",":
                i = self.ws(i + 1)
                continue
            if i < n and s[i] == "}":
                return result, i + 1
            raise _jerr("expected ',' or '}' at offset %d" % i)

    def arr(self, i, depth):
        if depth > MAX_DEPTH:
            raise _jerr("nesting deeper than %d levels" % MAX_DEPTH)
        s, n = self.s, self.n
        result = []
        i = self.ws(i)
        if i < n and s[i] == "]":
            return result, i + 1
        while True:
            val, i = self.value(i, depth)
            result.append(val)
            i = self.ws(i)
            if i < n and s[i] == ",":
                i = self.ws(i + 1)
                continue
            if i < n and s[i] == "]":
                return result, i + 1
            raise _jerr("expected ',' or ']' at offset %d" % i)

    def string(self, i):
        s, n = self.s, self.n
        chunks = []
        while True:
            j = _STR_RUN_RE.match(s, i).end()
            if j > i:
                chunks.append(s[i:j])
            if j >= n:
                raise _jerr("unterminated string")
            c = s[j]
            if c == '"':
                return "".join(chunks), j + 1
            if c != "\\":
                raise _jerr("unescaped control character U+%04X in string" % ord(c))
            if j + 1 >= n:
                raise _jerr("unterminated escape")
            e = s[j + 1]
            simple = _SIMPLE_ESCAPES.get(e)
            if simple is not None:
                chunks.append(simple)
                i = j + 2
                continue
            if e != "u":
                raise _jerr("invalid escape \\%s" % e)
            if not _HEX4_RE.fullmatch(s, j + 2, j + 6):
                raise _jerr("invalid \\u escape")
            cp = int(s[j + 2 : j + 6], 16)
            i = j + 6
            if 0xD800 <= cp <= 0xDBFF:
                if s.startswith("\\u", i) and _HEX4_RE.fullmatch(s, i + 2, i + 6):
                    lo = int(s[i + 2 : i + 6], 16)
                    if 0xDC00 <= lo <= 0xDFFF:
                        chunks.append(chr(0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00)))
                        i += 6
                        continue
                raise _jerr("lone surrogate escape \\u%04x" % cp)
            if 0xDC00 <= cp <= 0xDFFF:
                raise _jerr("lone surrogate escape \\u%04x" % cp)
            chunks.append(chr(cp))

    def number(self, i):
        m = _NUM_RE.match(self.s, i)
        if m is None:
            raise _jerr("invalid number at offset %d" % i)
        if m.group(1) is not None:
            raise _jerr("non-integer number")
        if m.group(2) is not None:
            raise _jerr("exponent form")
        text = m.group(0)
        if text == "-0":
            raise _jerr("-0")
        if len(text.lstrip("-")) > 16:
            raise _jerr("integer outside +/-(2^53-1)")
        v = int(text)
        if v > MAX_SAFE_INTEGER or v < -MAX_SAFE_INTEGER:
            raise _jerr("integer outside +/-(2^53-1)")
        return v, m.end()


def strict_decode(text):
    """Decode protocol JSON strictly (PROTOCOL §3.1).

    `text` may be bytes (must be valid UTF-8) or str. A str containing a surrogate code
    point cannot be the decoding of valid UTF-8 and is rejected as invalid UTF-8.
    Objects -> dict, arrays -> list, integers -> int, strings -> str, literals -> bool/None.
    """
    if isinstance(text, (bytes, bytearray, memoryview)):
        try:
            s = bytes(text).decode("utf-8", "strict")
        except UnicodeDecodeError:
            raise _jerr("invalid UTF-8") from None
    elif isinstance(text, str):
        s = text
        if _SURROGATE_RE.search(s):
            raise _jerr("invalid UTF-8 (surrogate code point in text)")
    else:
        raise TypeError("strict_decode expects str or bytes")
    d = _Decoder(s)
    value, i = d.value(d.ws(0), 0)
    i = d.ws(i)
    if i != d.n:
        raise _jerr("trailing data at offset %d" % i)
    return value


# ---------------------------------------------------------------------------
# 1. RFC 8785 canonical serialization (integer-only value domain)
# ---------------------------------------------------------------------------

_ESC_RE = re.compile(r'["\\\x00-\x1f]')
_ESC_MAP = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r", "\t": "\\t"}
for _c in range(0x20):
    _ESC_MAP.setdefault(chr(_c), "\\u%04x" % _c)
del _c


def _esc_sub(m):
    return _ESC_MAP[m.group(0)]


def _utf16_key(name):
    return name.encode("utf-16-be", "surrogatepass")


def _canon_into(v, out):
    if v is None:
        out.append("null")
    elif v is True:
        out.append("true")
    elif v is False:
        out.append("false")
    elif isinstance(v, int):
        _check_safe_int(v)
        out.append(str(int(v)))
    elif isinstance(v, str):
        out.append('"')
        out.append(_ESC_RE.sub(_esc_sub, v))
        out.append('"')
    elif isinstance(v, (list, tuple)):
        out.append("[")
        first = True
        for item in v:
            if not first:
                out.append(",")
            first = False
            _canon_into(item, out)
        out.append("]")
    elif isinstance(v, dict):
        for k in v:
            if not isinstance(k, str):
                raise ContractError("invalid_value", "object member name is not a string")
        out.append("{")
        first = True
        for k in sorted(v, key=_utf16_key):
            if not first:
                out.append(",")
            first = False
            out.append('"')
            out.append(_ESC_RE.sub(_esc_sub, k))
            out.append('":')
            _canon_into(v[k], out)
        out.append("}")
    else:
        raise ContractError("invalid_value", "value of type %s is outside the protocol domain" % type(v).__name__)


def canonical(value):
    """UTF-8(JCS(value)) as bytes."""
    out = []
    _canon_into(value, out)
    try:
        return "".join(out).encode("utf-8", "strict")
    except UnicodeEncodeError:
        raise ContractError("invalid_value", "string contains a lone surrogate") from None


# ---------------------------------------------------------------------------
# 3. Identity primitives
# ---------------------------------------------------------------------------

_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_P_RE = re.compile(r"p1:[0-9a-f]{64}")
_G_RE = re.compile(r"g1:[0-9a-f]{64}")
_E1_RE = re.compile(r"e1:[0-9a-f]{64}")
_HEX_RE = re.compile(r"(?:[0-9a-f]{2})*")
_HEX64_RE = re.compile(r"[0-9a-f]{64}")
EPISODE_KINDS = ("rss-guid", "atom-id", "rss-guid+enclosure", "enclosure")


def is_uuid(s):
    return isinstance(s, str) and _UUID_RE.fullmatch(s) is not None


def _need_uuid(s, what):
    if not is_uuid(s):
        raise ContractError("invalid_uuid", "%s is not a lowercase hyphenated UUID: %r" % (what, s))


def _utf8(s, what="field"):
    if not isinstance(s, str):
        raise ContractError("invalid_value", "%s is not a string" % what)
    try:
        return s.encode("utf-8", "strict")
    except UnicodeEncodeError:
        raise ContractError("invalid_value", "%s contains a lone surrogate" % what) from None


def length_prefixed(fields):
    """L(f1, ..., fk): each field's UTF-8 bytes preceded by a 4-byte big-endian length."""
    out = bytearray()
    for f in fields:
        b = _utf8(f)
        if len(b) > 0xFFFFFFFF:
            raise ContractError("invalid_value", "field longer than 2^32-1 bytes")
        out += len(b).to_bytes(4, "big")
        out += b
    return bytes(out)


def _sha_hex(b):
    return hashlib.sha256(b).hexdigest()


def is_podcast_id(s):
    return isinstance(s, str) and _P_RE.fullmatch(s) is not None


def is_generation_id(s):
    return isinstance(s, str) and _G_RE.fullmatch(s) is not None


def is_episode_id(s):
    if not isinstance(s, str):
        return False
    if _E1_RE.fullmatch(s):
        return True
    return s.startswith("e0:") and is_uuid(s[3:])


def podcast_id(namespace, op_id):
    _need_uuid(namespace, "namespace")
    _need_uuid(op_id, "op_id")
    return "p1:" + _sha_hex(length_prefixed(["podcast-v1", namespace, op_id]))


def generation_id(namespace, podcast, op_id):
    _need_uuid(namespace, "namespace")
    if not is_podcast_id(podcast):
        raise ContractError("invalid_id", "not a podcast ID: %r" % (podcast,))
    _need_uuid(op_id, "op_id")
    return "g1:" + _sha_hex(length_prefixed(["generation-v1", namespace, podcast, op_id]))


def episode_id(namespace, podcast, k, v):
    """E = "e1:" + hex(SHA-256(L("episode-v1", A, P, K, V))). V is taken as given."""
    _need_uuid(namespace, "namespace")
    if not is_podcast_id(podcast):
        raise ContractError("invalid_id", "not a podcast ID: %r" % (podcast,))
    if k not in EPISODE_KINDS:
        raise ContractError("invalid_kind", "unknown episode identity kind %r" % (k,))
    return "e1:" + _sha_hex(length_prefixed(["episode-v1", namespace, podcast, k, v]))


def episode_v_guid_enclosure(guid, normalized_enclosure):
    """V for K = "rss-guid+enclosure": guid, U+001F, normalized enclosure URL."""
    return guid + "\u001f" + normalized_enclosure


def guid_trim(raw):
    """Java String.trim() then GuidValidator: None when the GUID is absent."""
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ContractError("invalid_value", "GUID is not a string")
    start, end = 0, len(raw)
    while start < end and ord(raw[start]) <= 0x20:
        start += 1
    while end > start and ord(raw[end - 1]) <= 0x20:
        end -= 1
    v = raw[start:end]
    if v == "" or v == "null":
        return None
    return v


# URL normalization v1 (PROTOCOL §3.5), a string algorithm.

_SCHEME_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:")
_ASCII_LOWER = {c: c + 32 for c in range(ord("A"), ord("Z") + 1)}
_HEXDIGITS = frozenset("0123456789abcdefABCDEF")


def _lower_host(host):
    if "%" not in host:
        return host.translate(_ASCII_LOWER)
    out = []
    i, n = 0, len(host)
    while i < n:
        c = host[i]
        if c == "%" and i + 2 < n and host[i + 1] in _HEXDIGITS and host[i + 2] in _HEXDIGITS:
            out.append(host[i : i + 3])
            i += 3
        else:
            out.append(c.translate(_ASCII_LOWER))
            i += 1
    return "".join(out)


def normalize_url(url):
    if not isinstance(url, str):
        raise ContractError("invalid_value", "URL is not a string")
    m = _SCHEME_RE.match(url)
    if m is None:
        return url
    scheme = url[: m.end() - 1].translate(_ASCII_LOWER)
    rest = url[m.end() :]
    if not rest.startswith("//"):
        h = rest.find("#")
        if h != -1:
            rest = rest[:h]
        return scheme + ":" + rest
    body = rest[2:]
    end = len(body)
    for ch in "/?#":
        k = body.find(ch, 0, end)
        if k != -1:
            end = k
    authority, after = body[:end], body[end:]
    at = authority.rfind("@")
    userinfo, hostport = authority[: at + 1], authority[at + 1 :]
    if hostport.startswith("["):
        close = hostport.find("]")
        if close == -1:
            host, port = hostport, None
        else:
            tail = hostport[close + 1 :]
            if tail == "":
                host, port = hostport, None
            elif tail[0] == ":":
                host, port = hostport[: close + 1], tail[1:]
            else:
                host, port = hostport, None
    elif hostport.count(":") == 1:
        colon = hostport.find(":")
        host, port = hostport[:colon], hostport[colon + 1 :]
    else:
        # No ':' or more than one (malformed, e.g. "h:80:80"): the whole is the host.
        host, port = hostport, None
    host = _lower_host(host)
    if port is not None and (
        port == "" or (port == "80" and scheme == "http") or (port == "443" and scheme == "https")
    ):
        port = None
    h = after.find("#")
    if h != -1:
        after = after[:h]
    return scheme + "://" + userinfo + host + ("" if port is None else ":" + port) + after


# ---------------------------------------------------------------------------
# 5. Queue keys
# ---------------------------------------------------------------------------

_QKEY_RE = re.compile(r"q1:(0|[1-9][0-9]*):([1-9a-f][0-9a-f]*)")
_MOD129 = 1 << 129
MAX_QUEUE_N = 1048576


def _qerr(message):
    return ContractError("invalid_queue_key", message)


def parse_queue_key(s):
    """Wire form -> (n, m) with m odd, 0 < m/2^n < 1 and n <= 1,048,576."""
    if not isinstance(s, str):
        raise _qerr("not a string")
    mt = _QKEY_RE.fullmatch(s)
    if mt is None:
        raise _qerr("malformed queue key %r" % (s,))
    if len(mt.group(1)) > 7:
        raise _qerr("n above %d" % MAX_QUEUE_N)
    n = int(mt.group(1))
    if n > MAX_QUEUE_N:
        raise _qerr("n above %d" % MAX_QUEUE_N)
    if len(mt.group(2)) > (n + 3) // 4:
        raise _qerr("value not below 1 in %r" % (s,))
    m = int(mt.group(2), 16)
    if m & 1 == 0:
        raise _qerr("m is even in %r" % (s,))
    if m.bit_length() > n:
        raise _qerr("value not below 1 in %r" % (s,))
    return (n, m)


def is_valid_queue_key(s):
    try:
        parse_queue_key(s)
        return True
    except ContractError:
        return False


def format_queue_key(n, m):
    if not (_is_int(n) and _is_int(m)) or n < 0 or m <= 0 or m & 1 == 0 or m.bit_length() > n:
        raise _qerr("(n=%r, m=%r) is not a reduced key in (0, 1)" % (n, m))
    if n > MAX_QUEUE_N:
        raise _qerr("n above %d" % MAX_QUEUE_N)
    return "q1:%d:%x" % (n, m)


def _key(k):
    if isinstance(k, str):
        return parse_queue_key(k)
    if isinstance(k, tuple) and len(k) == 2:
        format_queue_key(k[0], k[1])
        return k
    raise _qerr("not a queue key: %r" % (k,))


def compare_queue_keys(a, b):
    """-1, 0 or 1 comparing m1/2^n1 with m2/2^n2 exactly."""
    n1, m1 = _key(a)
    n2, m2 = _key(b)
    # floor(log2(value)) = bit_length(m) - 1 - n; differing magnitudes decide immediately,
    # equal magnitudes have |n1 - n2| bounded by the bit lengths so the shift is cheap.
    e1 = m1.bit_length() - n1
    e2 = m2.bit_length() - n2
    if e1 != e2:
        return -1 if e1 < e2 else 1
    if n1 >= n2:
        x, y = m1, m2 << (n1 - n2)
    else:
        x, y = m1 << (n2 - n1), m2
    return (x > y) - (x < y)


def uuid_to_u(uuid_str):
    _need_uuid(uuid_str, "position write ID")
    return bytes.fromhex(uuid_str.replace("-", ""))


def queue_between(low, high, u):
    """Allocate the key strictly between low and high (None = sentinel 0 / 1) for write ID u.

    `u` is the 16-byte position-write ID (big-endian) or an int in [0, 2^128).
    """
    if isinstance(u, (bytes, bytearray)):
        if len(u) != 16:
            raise ContractError("invalid_value", "position write ID must be 16 bytes")
        U = int.from_bytes(bytes(u), "big")
    elif _is_int(u) and 0 <= u < (1 << 128):
        U = u
    else:
        raise ContractError("invalid_value", "position write ID must be 16 bytes")
    nL, mL = (0, 0) if low is None else _key(low)
    nR, mR = (0, 1) if high is None else _key(high)
    n0 = max(nL, nR)
    lnum = mL << (n0 - nL)  # L = lnum / 2^n0
    rnum = mR << (n0 - nR)  # R = rnum / 2^n0
    d = rnum - lnum  # D = d / 2^n0
    if d <= 0:
        raise ContractError("invalid_range", "low >= high")
    # Smallest n >= 1 with D * 2^n > 2^130, i.e. d * 2^n > 2^(130 + n0).
    target = 1 << (130 + n0)
    n = max(1, 130 + n0 - d.bit_length())
    while n > 1 and (d << (n - 1)) > target:
        n -= 1
    while (d << n) <= target:
        n += 1
    # Smallest m with m > L * 2^n (= (lnum << n) / 2^n0) and m = r (mod 2^129).
    r = 2 * U + 1
    m_min = ((lnum << n) >> n0) + 1
    m = m_min + ((r - m_min) % _MOD129)
    if not ((m << n0) < (rnum << n)):  # guaranteed by the choice of n
        raise AssertionError("allocator invariant violated")
    if n > MAX_QUEUE_N:
        raise ContractError("key_too_long", "allocation would need n above %d" % MAX_QUEUE_N)
    return format_queue_key(n, m)


# ---------------------------------------------------------------------------
# 6. Causal order and time normalization
# ---------------------------------------------------------------------------


def _node_fields(node):
    if isinstance(node, dict):
        nid, deps, t = node.get("id"), node.get("dependencies"), node.get("normalized_time")
    else:
        nid, deps, t = node
    if not isinstance(nid, str):
        raise ContractError("invalid_node", "node id is not a string")
    if not isinstance(deps, (list, tuple)) or not all(isinstance(x, str) for x in deps):
        raise ContractError("invalid_node", "dependencies must be a list of strings")
    _check_safe_int(t, "normalized_time")
    return nid, deps, t


def causal_order(nodes):
    """Kahn's algorithm with ready-set priority (normalized_time, id bytewise UTF-8).

    Raises ContractError with code "duplicate", "dependency_missing" (with .missing) or
    "cycle", checked in that order.
    """
    parsed = []
    seen = set()
    for node in nodes:
        nid, deps, t = _node_fields(node)
        if nid in seen:
            raise ContractError("duplicate", "node %r appears more than once" % nid)
        seen.add(nid)
        parsed.append((nid, deps, t))
    missing = sorted({d for _, deps, _ in parsed for d in deps if d not in seen}, key=lambda x: _utf8(x))
    if missing:
        raise ContractError("dependency_missing", "missing %r" % missing, missing=missing)
    indeg = {}
    children = {nid: [] for nid, _, _ in parsed}
    heap = []
    for nid, deps, t in parsed:
        uniq = set(deps)
        indeg[nid] = len(uniq)
        for d in uniq:
            children[d].append(nid)
        if not uniq:
            heap.append((t, _utf8(nid, "node id"), nid))
    times = {nid: t for nid, _, t in parsed}
    heapq.heapify(heap)
    out = []
    while heap:
        _, _, nid = heapq.heappop(heap)
        out.append(nid)
        for c in children[nid]:
            indeg[c] -= 1
            if indeg[c] == 0:
                heapq.heappush(heap, (times[c], _utf8(c, "node id"), c))
    if len(out) != len(parsed):
        raise ContractError("cycle", "dependency cycle")
    return out


def normalize_time(client_time, receive_time, dependency_times=()):
    _check_safe_int(client_time, "client_time")
    _check_safe_int(receive_time, "receive_time")
    for t in dependency_times:
        _check_safe_int(t, "dependency time")
    if not dependency_times:
        return min(client_time, receive_time)
    lower = max(dependency_times)
    if receive_time < lower:
        return lower
    return min(max(client_time, lower), receive_time)


# ---------------------------------------------------------------------------
# 4. Facts, MACs, digests, cursors, parts
# ---------------------------------------------------------------------------

_FACT_REQUIRED = frozenset(
    ["op_id", "author_id", "author_seq", "dependencies", "type", "type_version", "client_time", "targets", "payload"]
)
_FACT_OPTIONAL = frozenset(["previous_op_id"])
_TARGET_KEYS = frozenset(["entity", "id", "generation"])


def _rejected(message):
    return ContractError("rejected", message)


def validate_fact(fact):
    """Fact validation (PROTOCOL §6.1), separate from hashing.

    Raises ContractError with code "unarchivable" (not a strictly decodable JSON object, or
    op_id missing or not a UUID: invalid_request, nothing consumed) or "rejected" (any other
    envelope rule broken: archived with a rejected receipt). Payload schemas per type are
    defined by the operations (PROTOCOL §7, §8) and are not checked here.
    """
    if not isinstance(fact, dict):
        raise ContractError("unarchivable", "fact is not an object")
    try:
        canonical(fact)
    except ContractError as e:
        raise ContractError("unarchivable", "not strictly decodable JSON: %s" % e) from None
    if not is_uuid(fact.get("op_id")):
        raise ContractError("unarchivable", "op_id missing or not a UUID")
    keys = set(fact)
    missing = _FACT_REQUIRED - keys
    if missing:
        raise _rejected("missing fields %s" % sorted(missing))
    extra = keys - _FACT_REQUIRED - _FACT_OPTIONAL
    if extra:
        raise _rejected("unknown fields %s" % sorted(extra))
    if not is_uuid(fact["author_id"]):
        raise _rejected("author_id is not a UUID")
    if "previous_op_id" in fact and not is_uuid(fact["previous_op_id"]):
        raise _rejected("previous_op_id is not a UUID")
    for f, lo in (("author_seq", 1), ("type_version", 1), ("client_time", None)):
        v = fact[f]
        if not _is_int(v):
            raise _rejected("%s is not an integer" % f)
        if lo is not None and v < lo:
            raise _rejected("%s below %d" % (f, lo))
    deps = fact["dependencies"]
    if not isinstance(deps, list):
        raise _rejected("dependencies is not an array")
    prev = None
    for d in deps:
        if not is_uuid(d):
            raise _rejected("dependency %r is not a UUID node ID" % (d,))
        b = d.encode("utf-8")
        if prev is not None and b <= prev:
            raise _rejected("dependencies not sorted ascending without duplicates")
        prev = b
    if not isinstance(fact["type"], str):
        raise _rejected("type is not a string")
    targets = fact["targets"]
    if not isinstance(targets, list):
        raise _rejected("targets is not an array")
    prev = None
    for t in targets:
        if not isinstance(t, dict):
            raise _rejected("target is not an object")
        if "entity" not in t or "id" not in t or set(t) - _TARGET_KEYS:
            raise _rejected("target must be {entity, id, generation?}")
        if not all(isinstance(t[k], str) for k in t):
            raise _rejected("target members must be strings")
        key = (_utf8(t["entity"]), _utf8(t["id"]), _utf8(t.get("generation", "")))
        if prev is not None and key <= prev:
            raise _rejected("targets not sorted by (entity, id, generation-or-empty) without duplicates")
        prev = key
    if not isinstance(fact["payload"], dict):
        raise _rejected("payload is not an object")


def content_hash(fact):
    """hex(SHA-256(UTF-8(JCS(fact)))) over the object as received; does not validate."""
    return _sha_hex(canonical(fact))



def _mac_input(account_id, obj):
    if not isinstance(obj, dict):
        raise ContractError("invalid_value", "MAC object is not an object")
    _need_uuid(account_id, "account_id")
    body = {k: v for k, v in obj.items() if k != "mac"}
    if "account_id" in body:
        raise ContractError("invalid_value", "MAC object already has an account_id member")
    body["account_id"] = account_id
    return canonical(body)


def mac(key, account_id, obj):
    """hex(HMAC-SHA256(key, UTF-8(JCS(obj without "mac", plus member account_id))))."""
    return hmac.new(bytes(key), _mac_input(account_id, obj), hashlib.sha256).hexdigest()


def verify_mac(key, account_id, obj):
    given = obj.get("mac") if isinstance(obj, dict) else None
    if not isinstance(given, str):
        return False
    return hmac.compare_digest(given, mac(key, account_id, obj))


def next_digest(previous, obj):
    """d = SHA-256(previous || UTF-8(JCS(obj))); returns 32 bytes."""
    previous = bytes(previous)
    if len(previous) != 32:
        raise ContractError("invalid_value", "previous digest must be 32 bytes")
    return hashlib.sha256(previous + canonical(obj)).digest()


def chain_digest(objs, start=ZERO_DIGEST):
    d = start
    for o in objs:
        d = next_digest(d, o)
    return d


_B64URL_RE = re.compile(r"[A-Za-z0-9_-]*")


def b64url_encode(b):
    return base64.urlsafe_b64encode(bytes(b)).rstrip(b"=").decode("ascii")


def b64url_decode(s):
    """Strict base64url without padding; non-canonical (non-zero trailing bits) rejected."""
    if not isinstance(s, str) or _B64URL_RE.fullmatch(s) is None:
        raise ContractError("invalid_base64", "not unpadded base64url: %r" % (s,))
    if len(s) % 4 == 1:
        raise ContractError("invalid_base64", "impossible base64 length")
    b = base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
    if b64url_encode(b) != s:
        raise ContractError("invalid_base64", "non-canonical base64url (trailing bits)")
    return b


_CURSOR_KEYS = frozenset(["log_id", "epoch", "seq", "digest"])


def _check_cursor(c):
    if not isinstance(c, dict) or set(c) != _CURSOR_KEYS:
        raise ContractError("invalid_cursor", "cursor must be exactly {log_id, epoch, seq, digest}")
    if not is_uuid(c["log_id"]):
        raise ContractError("invalid_cursor", "log_id is not a UUID")
    for f in ("epoch", "seq"):
        v = c[f]
        if not _is_int(v) or v < 0 or v > MAX_SAFE_INTEGER:
            raise ContractError("invalid_cursor", "%s is not a non-negative integer" % f)
    if not isinstance(c["digest"], str) or _HEX64_RE.fullmatch(c["digest"]) is None:
        raise ContractError("invalid_cursor", "digest is not 64 lowercase hex characters")


def encode_cursor(cursor):
    _check_cursor(cursor)
    return b64url_encode(canonical(cursor))


def decode_cursor(encoded):
    try:
        raw = b64url_decode(encoded)
        value = strict_decode(raw)
    except ContractError as e:
        raise ContractError("invalid_cursor", str(e)) from None
    _check_cursor(value)
    if canonical(value) != raw:
        raise ContractError("invalid_cursor", "cursor JSON is not canonical")
    return value


def reassemble_parts(parts, expected_hash):
    """Concatenate non-empty base64url slices; the bytes must be exactly the canonical JSON of
    an object whose content hash is expected_hash. Returns the object. Fact validation is a
    separate, later step (validate_fact)."""
    if not isinstance(expected_hash, str) or _HEX64_RE.fullmatch(expected_hash) is None:
        raise ContractError("invalid_part", "content_hash is not 64 lowercase hex characters")
    if not isinstance(parts, (list, tuple)) or not parts:
        raise ContractError("invalid_part", "parts must be a non-empty list")
    try:
        slices = [b64url_decode(p) for p in parts]
        if not all(slices):
            raise ContractError("invalid_part", "empty slice")
        data = b"".join(slices)
        obj = strict_decode(data)
    except ContractError as e:
        raise ContractError("invalid_part", str(e)) from None
    if not isinstance(obj, dict):
        raise ContractError("invalid_part", "reassembled JSON is not an object")
    if canonical(obj) != data:
        raise ContractError("invalid_part", "reassembled bytes are not canonical JSON")
    if _sha_hex(data) != expected_hash:
        raise ContractError("invalid_part", "content hash mismatch")
    return obj
