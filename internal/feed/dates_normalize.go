package feed

// DateUtils' clean-up of a date before its fallback patterns, and the test
// for a leading weekday-like word. Each step returns its input unchanged, or
// a new slice; the input is never written to.

// dateNormalizeSteps are the clean-up steps, in DateUtils' order.
var dateNormalizeSteps = [...]struct {
	name  string
	apply func(*dateParser, []uint16) []uint16
}{
	{"trim", (*dateParser).trim},
	{"slash", (*dateParser).slashToHyphen},
	{"spaces", (*dateParser).collapseSpaces},
	{"zone colon", (*dateParser).dropZoneColon},
	{"CEST", func(p *dateParser, t []uint16) []uint16 { return p.replaceAtEnd(t, "CEST", "+0200") }},
	{"CET", func(p *dateParser, t []uint16) []uint16 { return p.replaceAtEnd(t, "CET", "+0100") }},
	{"Sept", (*dateParser).septToSep},
	{"fraction", (*dateParser).fractionToMillis},
}

// normalize applies every clean-up step.
func (p *dateParser) normalize(t []uint16) []uint16 {
	for _, s := range dateNormalizeSteps {
		t = s.apply(p, t)
	}
	return t
}

// trim drops leading and trailing units up to U+0020, as String.trim.
func (p *dateParser) trim(t []uint16) []uint16 {
	i, j := 0, len(t)
	for i < j && t[i] <= ' ' {
		p.w.step()
		i++
	}
	for j > i && t[j-1] <= ' ' {
		p.w.step()
		j--
	}
	return t[i:j]
}

// slashToHyphen replaces every '/' with '-'.
func (p *dateParser) slashToHyphen(t []uint16) []uint16 {
	var out []uint16
	for i, c := range t {
		p.w.step()
		if c == '/' {
			if out == nil {
				out = append([]uint16(nil), t...)
			}
			out[i] = '-'
		}
	}
	if out == nil {
		return t
	}
	return out
}

// collapseSpaces replaces each run of U+0020 with one.
func (p *dateParser) collapseSpaces(t []uint16) []uint16 {
	var out []uint16
	for i, c := range t {
		p.w.step()
		if c == ' ' && i > 0 && t[i-1] == ' ' {
			if out == nil {
				out = append(make([]uint16, 0, len(t)), t[:i]...)
			}
			continue
		}
		if out != nil {
			out = append(out, c)
		}
	}
	if out == nil {
		return t
	}
	return out
}

// dateTextEnd is where a pattern anchored at the end ($) may end: the end, or
// before a final line terminator.
func dateTextEnd(t []uint16) int {
	if n := len(t); n > 0 && dateIsLineTerminator(t[n-1]) {
		return n - 1
	}
	return len(t)
}

// dropZoneColon removes the colon of a final [+-]DD:DD, D any decimal digit.
// Read backwards from the end, as only one such match can end there.
func (p *dateParser) dropZoneColon(t []uint16) []uint16 {
	i := dateTextEnd(t)
	digits := func() bool {
		for range 2 {
			p.w.step()
			r, n := dateRuneBefore(t, i)
			if n == 0 || !dateIsDigit(r) {
				return false
			}
			i -= n
		}
		return true
	}
	if !digits() || i == 0 || t[i-1] != ':' {
		return t
	}
	i--
	colon := i
	if !digits() || i == 0 || t[i-1] != '+' && t[i-1] != '-' {
		return t
	}
	out := append(make([]uint16, 0, len(t)-1), t[:colon]...)
	return append(out, t[colon+1:]...)
}

// replaceAtEnd replaces a final old (before a final line terminator, if any)
// with new.
func (p *dateParser) replaceAtEnd(t []uint16, old, new string) []uint16 {
	end := dateTextEnd(t)
	at := end - len(old)
	if at < 0 {
		return t
	}
	for i := range len(old) {
		if t[at+i] != uint16(old[i]) {
			return t
		}
	}
	out := append(make([]uint16, 0, len(t)+len(new)-len(old)), t[:at]...)
	for i := range len(new) {
		out = append(out, uint16(new[i]))
	}
	return append(out, t[end:]...)
}

// septToSep replaces each word "Sept" with "Sep" (\bSept\b), left to right.
func (p *dateParser) septToSep(t []uint16) []uint16 {
	var out []uint16
	changed, last := false, 0
	for i := 0; i+4 <= len(t); i++ {
		p.w.step()
		if t[i] != 'S' || t[i+1] != 'e' || t[i+2] != 'p' || t[i+3] != 't' ||
			!p.wordBoundary(t, i) || !p.wordBoundary(t, i+4) {
			continue
		}
		out = append(append(out, t[last:i]...), 'S', 'e', 'p')
		changed, last = true, i+4
		i += 3
	}
	if !changed {
		return t
	}
	return append(out, t[last:]...)
}

// wordBoundary reports an ICU word boundary (\b) at t[i]: none before a
// boundary-transparent code point; otherwise one where a word character
// meets a non-word character, looking back through transparent ones.
func (p *dateParser) wordBoundary(t []uint16, i int) bool {
	after := false
	if i < len(t) {
		r, _ := dateRuneAt(t, i)
		if dateIsBoundaryTransparent(r) {
			return false
		}
		after = dateIsWord(r)
	}
	before := false
	for j := i; j > 0; {
		p.w.step()
		r, n := dateRuneBefore(t, j)
		j -= n
		if !dateIsBoundaryTransparent(r) {
			before = dateIsWord(r)
			break
		}
	}
	return after != before
}

// fractionToMillis makes the digits after the first '.' three: more are cut,
// fewer padded with '0'. As in DateUtils, the text after the digits is kept
// only when it is longer than one unit.
func (p *dateParser) fractionToMillis(t []uint16) []uint16 {
	dot := -1
	for i, c := range t {
		p.w.step()
		if c == '.' {
			dot = i
			break
		}
	}
	if dot < 0 {
		return t
	}
	end := dot + 1
	for end < len(t) && dateIsUnitDigit(t[end]) {
		p.w.step()
		end++
	}
	n := end - dot - 1
	if n == 3 {
		return t
	}
	var out []uint16
	if n > 3 {
		out = append(make([]uint16, 0, len(t)), t[:dot+4]...)
	} else {
		out = append(make([]uint16, 0, len(t)+3), t[:end]...)
		for range 3 - n {
			out = append(out, '0')
		}
	}
	if end < len(t)-1 {
		out = append(out, t[end:]...)
	}
	return out
}

// lastLineTerminator is the index of the last line terminator, or -1.
func (p *dateParser) lastLineTerminator(t []uint16) int {
	for i := len(t) - 1; i >= 0; i-- {
		p.w.step()
		if dateIsLineTerminator(t[i]) {
			return i
		}
	}
	return -1
}

// weekdayWord reports whether t starts with one or more word characters and
// ", " (DateUtils' "^\w+, .*$" without its line-terminator condition), and
// the index of that comma.
func (p *dateParser) weekdayWord(t []uint16) (int, bool) {
	i := 0
	for i < len(t) {
		r, n := dateRuneAt(t, i)
		if !dateIsWord(r) {
			break
		}
		p.w.step()
		i += n
	}
	if i == 0 || i+1 >= len(t) || t[i] != ',' || t[i+1] != ' ' {
		return 0, false
	}
	return i, true
}
