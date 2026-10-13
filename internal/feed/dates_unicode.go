package feed

import (
	"sort"
	"unicode"
	"unicode/utf16"
)

// Character classes of the date parser, on Go's Unicode tables. Text is
// UTF-16: a high surrogate followed by a low one is one code point, any other
// surrogate stands for itself.

// dateRuneAt is the code point at t[i] and its length in units.
func dateRuneAt(t []uint16, i int) (rune, int) {
	c := rune(t[i])
	if c >= 0xD800 && c < 0xDC00 && i+1 < len(t) {
		if d := rune(t[i+1]); d >= 0xDC00 && d < 0xE000 {
			return utf16.DecodeRune(c, d), 2
		}
	}
	return c, 1
}

// dateRuneBefore is the code point ending at t[i-1] and its length in units
// (0 at the start).
func dateRuneBefore(t []uint16, i int) (rune, int) {
	if i <= 0 {
		return 0, 0
	}
	c := rune(t[i-1])
	if c >= 0xDC00 && c < 0xE000 && i >= 2 {
		if h := rune(t[i-2]); h >= 0xD800 && h < 0xDC00 {
			return utf16.DecodeRune(h, c), 2
		}
	}
	return c, 1
}

// dateIsDigit reports a decimal digit (Nd) code point, any plane.
func dateIsDigit(r rune) bool { return unicode.Is(unicode.Nd, r) }

// dateIsUnitDigit reports a unit that is a decimal digit by itself: not a
// surrogate.
func dateIsUnitDigit(c uint16) bool {
	return (c < 0xD800 || c >= 0xE000) && unicode.Is(unicode.Nd, rune(c))
}

// dateDigitValue is the value of a decimal digit code point. Unicode encodes
// every set of decimal digits as ten consecutive code points from 0 to 9, so
// a run of Nd code points in Go's table starts at a zero.
func dateDigitValue(r rune) int {
	if r < 0x80 {
		return int(r - '0')
	}
	if r <= 0xFFFF {
		rs := unicode.Nd.R16
		i := sort.Search(len(rs), func(i int) bool { return rune(rs[i].Hi) >= r })
		return int(r-rune(rs[i].Lo)) / int(rs[i].Stride) % 10
	}
	rs := unicode.Nd.R32
	i := sort.Search(len(rs), func(i int) bool { return rune(rs[i].Hi) >= r })
	return int(r-rune(rs[i].Lo)) / int(rs[i].Stride) % 10
}

// dateIsWord reports a word character of ICU regular expressions (\w).
func dateIsWord(r rune) bool {
	if r < 0x80 {
		return r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
	}
	return unicode.IsLetter(r) || unicode.IsMark(r) || unicode.Is(unicode.Nd, r) || unicode.Is(unicode.Nl, r) ||
		unicode.Is(unicode.Pc, r) || unicode.Is(unicode.Other_Alphabetic, r) || r == 0x200C || r == 0x200D
}

// dateIsBoundaryTransparent reports a code point that word boundaries look
// through: Mn, Me, Cf or Other_Grapheme_Extend.
func dateIsBoundaryTransparent(r rune) bool {
	if r < 0x80 {
		return false
	}
	return unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf, unicode.Other_Grapheme_Extend)
}

// dateIsLineTerminator reports a line terminator of ICU regular expressions.
func dateIsLineTerminator(c uint16) bool {
	return c >= 0x0A && c <= 0x0D || c == 0x85 || c == 0x2028 || c == 0x2029
}

// dateIsBlank reports the blanks a field skips.
func dateIsBlank(c uint16) bool { return c == ' ' || c == '\t' }

// dateUnitsMatchIgnoringCase compares two units as Java's case-insensitive
// region match does: equal, equal upper cases, or equal lower cases of the
// upper cases.
func dateUnitsMatchIgnoringCase(a, b uint16) bool {
	if a == b {
		return true
	}
	ua, ub := unicode.ToUpper(rune(a)), unicode.ToUpper(rune(b))
	return ua == ub || unicode.ToLower(ua) == unicode.ToLower(ub)
}

// dateFoldKey is the smallest code point of r's simple case-fold orbit: two
// code points fold to each other when their keys are equal.
func dateFoldKey(r rune) rune {
	key := r
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		key = min(key, f)
	}
	return key
}

var dateMinusSigns = []rune{0x2D, 0xFF0D, 0xFE63, 0x2010, 0x2011, 0x2012, 0x2013, 0x2212, 0x207B, 0x208B, 0x2796}

var datePlusSigns = []rune{0x2B, 0xFF0B, 0xFB29, 0xFE62, 0x207A, 0x208A, 0x2795}

func dateIsMinus(r rune) bool {
	for _, m := range dateMinusSigns {
		if r == m {
			return true
		}
	}
	return false
}

func dateIsPlus(r rune) bool {
	for _, m := range datePlusSigns {
		if r == m {
			return true
		}
	}
	return false
}
