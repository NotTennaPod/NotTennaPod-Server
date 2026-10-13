// Package contract implements the NotTennaPod wire contract (contract-v1): canonical
// encodings, identity derivations, URL normalization, queue keys, causal order and
// the hashes, digests and MACs that every implementation must reproduce exactly.
// See PROTOCOL §3 and docs/CONTRACT.md.
package contract

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
)

// LengthPrefixed returns L(f1, …, fk): each field's UTF-8 bytes preceded by their
// length as a 4-byte unsigned big-endian integer. Strings are used exactly as given;
// no Unicode normalization is applied.
func LengthPrefixed(fields ...string) []byte {
	size := 0
	for _, f := range fields {
		size += 4 + len(f)
	}
	out := make([]byte, 0, size)
	var prefix [4]byte
	for _, f := range fields {
		binary.BigEndian.PutUint32(prefix[:], uint32(len(f)))
		out = append(out, prefix[:]...)
		out = append(out, f...)
	}
	return out
}

// ValidUUID reports whether s is a UUID in contract form: lowercase hyphenated,
// 36 characters (PROTOCOL §3).
func ValidUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return false
			}
		}
	}
	return true
}

func derived(prefix string, fields ...string) string {
	sum := sha256.Sum256(LengthPrefixed(fields...))
	return prefix + hex.EncodeToString(sum[:])
}

// PodcastID derives P from the account namespace and the operation that created
// the podcast (PROTOCOL §3.3).
func PodcastID(namespace, creatingOpID string) string {
	return derived("p1:", "podcast-v1", namespace, creatingOpID)
}

// DerivedAuthorID derives the effective author of a divergent author branch from
// its root fact (PROTOCOL §6.10).
func DerivedAuthorID(namespace, rootOpID string) string {
	return derived("a1:", "author-v1", namespace, rootOpID)
}

// GenerationID derives a subscription generation from the fact that opened it
// (PROTOCOL §3.3, generation boundaries).
func GenerationID(namespace, podcastID, creatingOpID string) string {
	return derived("g1:", "generation-v1", namespace, podcastID, creatingOpID)
}

// Episode identity kinds (PROTOCOL §3.3).
const (
	KindRSSGUID          = "rss-guid"
	KindAtomID           = "atom-id"
	KindGUIDAndEnclosure = "rss-guid+enclosure"
	KindEnclosure        = "enclosure"
)

// ErrUnknownKind reports an episode identity kind outside contract-v1.
var ErrUnknownKind = errors.New("contract: unknown episode identity kind")

// EpisodeID derives E for an already-chosen kind and value (PROTOCOL §3.3 rules 2–4).
// Choosing the kind from alias and dispute state is the catalog's job.
func EpisodeID(namespace, podcastID, kind, value string) (string, error) {
	switch kind {
	case KindRSSGUID, KindAtomID, KindGUIDAndEnclosure, KindEnclosure:
		return derived("e1:", "episode-v1", namespace, podcastID, kind, value), nil
	}
	return "", ErrUnknownKind
}

// GUIDAndEnclosureValue builds V for the disputed-GUID identity: GUID, U+001F,
// normalized enclosure URL.
func GUIDAndEnclosureValue(guid, normalizedEnclosure string) string {
	return guid + "\u001f" + normalizedEnclosure
}

// TrimGUID applies AntennaPod's GuidValidator rule (Java String.trim): remove
// leading and trailing code points <= U+0020. It reports false when the GUID is
// absent: empty after trimming, or exactly "null".
func TrimGUID(raw string) (string, bool) {
	trimmed := strings.TrimFunc(raw, func(r rune) bool { return r <= 0x20 })
	if trimmed == "" || trimmed == "null" {
		return "", false
	}
	return trimmed, true
}

var schemePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.\-]*:`)

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// lowerHost lowercases ASCII letters but copies percent triplets verbatim.
func lowerHost(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c == '%' && i+2 < len(b) && isHex(b[i+1]) && isHex(b[i+2]) {
			i += 2
			continue
		}
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// NormalizeURL applies URL normalization v1 (PROTOCOL §3). It is a string
// algorithm, not a URL parser, so every implementation produces identical output.
func NormalizeURL(raw string) string {
	match := schemePattern.FindString(raw)
	if match == "" {
		return raw
	}
	scheme := lowerASCII(match[:len(match)-1])
	rest := raw[len(match):]
	if !strings.HasPrefix(rest, "//") {
		if i := strings.IndexByte(rest, '#'); i >= 0 {
			rest = rest[:i]
		}
		return scheme + ":" + rest
	}
	afterSlashes := rest[2:]
	end := strings.IndexAny(afterSlashes, "/?#")
	if end < 0 {
		end = len(afterSlashes)
	}
	authority, tail := afterSlashes[:end], afterSlashes[end:]
	if i := strings.IndexByte(tail, '#'); i >= 0 {
		tail = tail[:i]
	}
	return scheme + "://" + normalizeAuthority(scheme, authority) + tail
}

func normalizeAuthority(scheme, authority string) string {
	userinfo, hostport := "", authority
	if i := strings.LastIndexByte(authority, '@'); i >= 0 {
		userinfo, hostport = authority[:i+1], authority[i+1:]
	}
	host, port, hasPort := hostport, "", false
	if strings.HasPrefix(hostport, "[") {
		if j := strings.IndexByte(hostport, ']'); j >= 0 {
			host = hostport[:j+1]
			if after := hostport[j+1:]; strings.HasPrefix(after, ":") {
				port, hasPort = after[1:], true
			} else if after != "" {
				// Malformed: keep the remainder attached to the host unchanged.
				host = hostport
			}
		}
	} else if strings.Count(hostport, ":") == 1 {
		j := strings.IndexByte(hostport, ':')
		host, port, hasPort = hostport[:j], hostport[j+1:], true
	}
	host = lowerHost(host)
	if hasPort && (port == "" || (scheme == "http" && port == "80") || (scheme == "https" && port == "443")) {
		hasPort = false
	}
	if hasPort {
		return userinfo + host + ":" + port
	}
	return userinfo + host
}
