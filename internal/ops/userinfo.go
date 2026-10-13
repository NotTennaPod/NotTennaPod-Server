package ops

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// URL userinfo in text (PROTOCOL §9.4, §7): the server keeps no URL
// credentials outside sealed secrets, in URLs, GUIDs or text. StripUserinfo
// is the one rule, rules.strip_userinfo of the reference; its own cases are
// contract/vectors/strip_userinfo.json.

// The rule reads the text with its character references decoded (numeric,
// with or without ";", and &commat;, &colon; and &sol;), one view character
// at a time: nothing is built for text that holds no URL (a decoded copy of the whole text cost
// about 20 times its size).

var namedRefs = []struct {
	name string
	r    rune
}{{"commat;", '@'}, {"colon;", ':'}, {"sol;", '/'}}

// viewAt is the view character at byte i of s and the bytes it spans: a
// character reference decodes to its character; one whose number is not a
// Unicode scalar value, or is zero, stays as written.
func viewAt(s string, i int) (rune, int) {
	c := s[i]
	if c == '&' {
		if r, n, ok := refAt(s, i); ok {
			return r, n
		}
		return '&', 1
	}
	if c < utf8.RuneSelf {
		return rune(c), 1
	}
	return utf8.DecodeRuneInString(s[i:])
}

// refAt reads the character reference at s[i] ('&'), as the reference's
// expression &(?:#[xX]([0-9A-Fa-f]+);?|#([0-9]+);?|(commat|colon|sol);)
// matches there: its character and length, ok false without a match or a
// scalar value.
func refAt(s string, i int) (rune, int, bool) {
	j := i + 1
	if j < len(s) && s[j] == '#' {
		j++
		base, digit := 10, isDecimal
		if j < len(s) && (s[j] == 'x' || s[j] == 'X') {
			j++
			base, digit = 16, isHex
		}
		k := j
		for k < len(s) && digit(s[k]) {
			k++
		}
		if k == j {
			return 0, 0, false // "&#x" without a hex digit is no decimal reference either
		}
		r, ok := scalar(s[j:k], base)
		if k < len(s) && s[k] == ';' {
			k++
		}
		return r, k - i, ok
	}
	for _, x := range namedRefs {
		if strings.HasPrefix(s[j:], x.name) {
			return x.r, 1 + len(x.name), true
		}
	}
	return 0, 0, false
}

func isDecimal(c byte) bool { return c >= '0' && c <= '9' }

func isHex(c byte) bool { return isDecimal(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

// scalar reads a numeric reference's digits: a Unicode scalar value other
// than zero, else false.
func scalar(digits string, base int) (rune, bool) {
	digits = strings.TrimLeft(digits, "0")
	if digits == "" || len(digits) > 8 {
		return 0, false
	}
	n, err := strconv.ParseInt(digits, base, 64)
	if err != nil || n > 0x10FFFF || (n >= 0xD800 && n <= 0xDFFF) {
		return 0, false
	}
	return rune(n), true
}

func schemeChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '.' || c == '-'
}

// follows reports whether the view characters at byte i of s are want, and
// the byte after them.
func follows(s string, i int, want string) (int, bool) {
	for _, w := range want {
		if i >= len(s) {
			return 0, false
		}
		r, n := viewAt(s, i)
		if r != w {
			return 0, false
		}
		i += n
	}
	return i, true
}

// urlMatch is one URL the rule finds, in byte offsets of the text: where it
// starts (its scheme, or its //), where its scheme ends (-1 for a
// scheme-relative URL), where its authority starts and ends, the end of its
// userinfo's last @ (-1 without userinfo), and whether it starts right after
// an apostrophe.
type urlMatch struct {
	start, schemeEnd, authority, end, atEnd int
	quoted                                  bool
}

// scanURLs calls f with each URL of s in order, reading the text's decoded
// view one character at a time (viewAt):
//   - A URL starts at scheme:// (ASCII), or at a scheme-relative // at the
//     start of the text or after U+0000–U+0020, a quotation mark, an
//     apostrophe, =, (, < or >.
//   - Its authority runs to the first U+0000–U+0020, /, ?, #, <, > or
//     quotation mark, and also to the first apostrophe when the URL starts
//     right after an apostrophe (a single-quoted attribute). Otherwise an
//     apostrophe belongs to the authority, as RFC 3986 allows in userinfo.
//   - Its userinfo is everything up to its last @.
//
// The next URL is looked for from the end of the authority.
func scanURLs(s string, f func(urlMatch)) {
	prev := rune(-1) // the view character before i (none at the start)
	for i := 0; i < len(s); {
		r, n := viewAt(s, i)
		m := urlMatch{start: i, schemeEnd: -1, atEnd: -1, quoted: prev == '\''}
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z':
			// A scheme: the letter's run, then ://. A later start in the
			// same run ends at the same place.
			j, last := i+n, r
			for j < len(s) {
				c, k := viewAt(s, j)
				if !schemeChar(c) {
					break
				}
				j, last = j+k, c
			}
			k, ok := follows(s, j, "://")
			if !ok {
				i, prev = j, last
				continue
			}
			m.schemeEnd, m.authority = j, k
		case r == '/' && (prev <= 0x20 || strings.ContainsRune("\"'=(<>", prev)): // prev < 0 at the start
			k, ok := follows(s, i+n, "/")
			if !ok {
				i, prev = i+n, r
				continue
			}
			m.authority = k
		default:
			i, prev = i+n, r
			continue
		}
		end, last := m.authority, '/'
		for end < len(s) {
			c, k := viewAt(s, end)
			if c <= 0x20 || strings.ContainsRune("/?#<>\"", c) || (m.quoted && c == '\'') {
				break
			}
			if c == '@' {
				m.atEnd = end + k
			}
			end, last = end+k, c
		}
		m.end = end
		f(m)
		i, prev = end, last
	}
}

// StripUserinfo removes the userinfo of every URL in a text value
// (scanURLs): scheme://user:pass@host becomes scheme://host. The rule reads
// the text with character references decoded, so &#64; is an @; the removed
// span covers the references it spans.
func StripUserinfo(s string) string {
	if !strings.Contains(s, "&") && (!strings.Contains(s, "//") || !strings.Contains(s, "@")) {
		return s // nothing a decoded view could cut
	}
	var b strings.Builder
	kept := 0 // s is copied to b up to here
	scanURLs(s, func(m urlMatch) {
		if m.atEnd < 0 {
			return
		}
		if kept == 0 {
			b.Grow(len(s))
		}
		b.WriteString(s[kept:m.authority])
		kept = m.atEnd
	})
	if kept == 0 {
		return s
	}
	b.WriteString(s[kept:])
	return b.String()
}

// URL is a URL FindURLs found in a text: its byte span, and its scheme (""
// for a scheme-relative URL) and host[:port] as decoded. HostEnd is where
// its authority ends: a path, query or fragment follows when HostEnd < End.
type URL struct {
	Start, End, HostEnd int
	Scheme, Host        string
}

// FindURLs finds the URLs of a text as StripUserinfo does (scanURLs), for
// those who cut them: a URL here runs on past its authority, through /, ?
// and #, to the first U+0000–U+0020, <, > or quotation mark, or to the first
// apostrophe when it starts right after one. A URL that starts inside an
// earlier one is part of it.
func FindURLs(s string) []URL {
	var out []URL
	scanURLs(s, func(m urlMatch) {
		if len(out) > 0 && m.start < out[len(out)-1].End {
			return
		}
		end := m.end
		for end < len(s) {
			c, k := viewAt(s, end)
			if c <= 0x20 || c == '<' || c == '>' || c == '"' || (m.quoted && c == '\'') {
				break
			}
			end += k
		}
		u := URL{Start: m.start, End: end, HostEnd: m.end}
		if m.schemeEnd >= 0 {
			u.Scheme = decoded(s[m.start:m.schemeEnd])
		}
		host := m.authority
		if m.atEnd >= 0 {
			host = m.atEnd
		}
		u.Host = decoded(s[host:m.end])
		out = append(out, u)
	})
	return out
}

// decoded is a span of text with its character references decoded.
func decoded(s string) string {
	if !strings.Contains(s, "&") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		r, n := viewAt(s, i)
		b.WriteRune(r)
		i += n
	}
	return b.String()
}

// stripAll applies StripUserinfo to every string of a JSON value, in place
// (the reference's _strip_all; object keys stay).
func stripAll(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, y := range x {
			x[k] = stripAll(y)
		}
	case []any:
		for i, y := range x {
			x[i] = stripAll(y)
		}
	case string:
		return StripUserinfo(x)
	}
	return v
}
