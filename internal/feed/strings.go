package feed

import (
	"strings"
	"unicode"
	"unicode/utf16"
)

// utf16Len is Java's String.length().
func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n += utf16.RuneLen(r)
	}
	return n
}

// javaTrim is String.trim(): it removes code units up to U+0020 at both ends.
func javaTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return r <= ' ' })
}

// icuSpace is \s in Android's regular expressions (ICU): [\t\n\f\r\p{Z}].
func icuSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\f', '\r':
		return true
	}
	return unicode.Is(unicode.Z, r)
}

// trimAllWhitespace is SyndStringUtils.trimAllWhitespace:
// replaceAll("(^\\s*)|(\\s*$)", "") under ICU.
func trimAllWhitespace(s string) string {
	return strings.TrimFunc(s, icuSpace)
}

func ptr(s string) *string { return &s }

func empty(p *string) bool { return p == nil || *p == "" }
