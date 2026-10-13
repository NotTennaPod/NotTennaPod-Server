package feed

import (
	"strings"
	"unicode"
)

// Java's Character and String semantics on UTF-16 code units, as TagSoup and
// Html use them. A surrogate is never a letter, digit or space.

func tagsoupIsSurrogate(c uint16) bool { return c >= 0xD800 && c <= 0xDFFF }

// tagsoupIsLetter is Character.isLetter(char).
func tagsoupIsLetter(c uint16) bool {
	return !tagsoupIsSurrogate(c) && unicode.IsLetter(rune(c))
}

// tagsoupIsDigit is Character.isDigit(char): general category Nd.
func tagsoupIsDigit(c uint16) bool {
	return !tagsoupIsSurrogate(c) && unicode.Is(unicode.Nd, rune(c))
}

// tagsoupIsWhitespace is Character.isWhitespace(char): a Unicode space,
// line or paragraph separator other than a no-break space, or one of
// U+0009-U+000D and U+001C-U+001F.
func tagsoupIsWhitespace(c uint16) bool {
	switch {
	case c >= 0x09 && c <= 0x0D, c >= 0x1C && c <= 0x20:
		return true
	case c < 0x80, c == 0xA0, c == 0x2007, c == 0x202F, tagsoupIsSurrogate(c):
		return false
	}
	return unicode.Is(unicode.Z, rune(c))
}

// tagsoupToLower is Character.toLowerCase(char), the simple mapping.
func tagsoupToLower(c uint16) uint16 {
	if tagsoupIsSurrogate(c) {
		return c
	}
	if l := unicode.ToLower(rune(c)); l <= 0xFFFF {
		return uint16(l)
	}
	return c
}

// tagsoupToUpper is Character.toUpperCase(char), the simple mapping.
func tagsoupToUpper(c uint16) uint16 {
	if tagsoupIsSurrogate(c) {
		return c
	}
	if u := unicode.ToUpper(rune(c)); u <= 0xFFFF {
		return uint16(u)
	}
	return c
}

// tagsoupDigit is Character.digit(char, radix), or -1.
func tagsoupDigit(c uint16, radix int) int {
	d := -1
	switch {
	case c >= '0' && c <= '9':
		d = int(c - '0')
	case c >= 'a' && c <= 'z':
		d = int(c-'a') + 10
	case c >= 'A' && c <= 'Z':
		d = int(c-'A') + 10
	case c >= 0xFF21 && c <= 0xFF3A: // fullwidth A-Z
		d = int(c-0xFF21) + 10
	case c >= 0xFF41 && c <= 0xFF5A: // fullwidth a-z
		d = int(c-0xFF41) + 10
	case tagsoupIsDigit(c):
		// Nd characters come in runs of ten, 0 to 9, from the start of each
		// range of the table.
		for _, r := range unicode.Nd.R16 {
			if c >= r.Lo && c <= r.Hi {
				d = int(c-r.Lo) % 10
				break
			}
		}
	}
	if d >= radix {
		return -1
	}
	return d
}

// tagsoupParseInt is Integer.parseInt(s, radix); ok is false where Java
// throws NumberFormatException.
func tagsoupParseInt(s []uint16, radix int) (v int32, ok bool) {
	if len(s) == 0 {
		return 0, false
	}
	i, negative := 0, false
	limit := -int64(1<<31 - 1)
	if s[0] < '0' {
		switch s[0] {
		case '-':
			negative, limit = true, -int64(1<<31)
		case '+':
		default:
			return 0, false
		}
		if len(s) == 1 {
			return 0, false
		}
		i++
	}
	multmin := limit / int64(radix)
	var result int64
	for ; i < len(s); i++ {
		d := tagsoupDigit(s[i], radix)
		if d < 0 || result < multmin {
			return 0, false
		}
		result *= int64(radix)
		if result < limit+int64(d) {
			return 0, false
		}
		result -= int64(d)
	}
	if negative {
		return int32(result), true
	}
	return int32(-result), true
}

// tagsoupLowerRoot is String.toLowerCase(Locale.ROOT) as Android's
// libcore CaseMapper does it: the simple mapping, U+0130 to "i̇", and a
// final sigma between a cased letter and a non-cased one. step counts each
// code point.
func tagsoupLowerRoot(s string, step func()) string {
	rs := []rune(s)
	cased := func(r rune) bool {
		return unicode.IsUpper(r) || unicode.IsLower(r) || unicode.IsTitle(r) || unicode.Is(unicode.Other_Lowercase, r) ||
			unicode.Is(unicode.Other_Uppercase, r)
	}
	var b strings.Builder
	for i, r := range rs {
		step()
		switch {
		case r == 0x130:
			b.WriteString("i̇")
		case r == 0x3A3 && i > 0 && cased(rs[i-1]) && (i+1 == len(rs) || !cased(rs[i+1])):
			b.WriteRune(0x3C2)
		default:
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// htmlEqualsIgnoreCase is String.equalsIgnoreCase(s, t) for an ASCII t:
// per code unit, equal upper cases or equal lower cases of the upper cases.
func htmlEqualsIgnoreCase(s []uint16, t string) bool {
	if len(s) != len(t) {
		return false
	}
	for i, c := range s {
		d := uint16(t[i])
		if c == d {
			continue
		}
		u1, u2 := tagsoupToUpper(c), tagsoupToUpper(d)
		if u1 != u2 && tagsoupToLower(u1) != tagsoupToLower(u2) {
			return false
		}
	}
	return true
}
