package feed

// DateUtils' date patterns and how one is matched against a text.
//
// A pattern is a list of tokens: a run of one ASCII letter is a field, text
// in single quotes and any other character a literal. Matching runs left to
// right from the start of the text. A literal must be there unit for unit. A
// field first skips blanks (space, tab) and fails at the end of the text;
// numbers are read where the blanks end, names (weekday, month) only where
// they began. No field has a width: each reads as far as its number goes.

// datePatternToken is a field (letter, count) or a literal (letter 0).
type datePatternToken struct {
	letter byte
	count  int
	lit    []uint16
}

// datePattern is a compiled pattern.
type datePattern struct {
	text   string
	tokens []datePatternToken
}

func compileDatePattern(s string) datePattern {
	p := datePattern{text: s}
	literal := func(c byte) {
		if n := len(p.tokens); n > 0 && p.tokens[n-1].letter == 0 {
			p.tokens[n-1].lit = append(p.tokens[n-1].lit, uint16(c))
			return
		}
		p.tokens = append(p.tokens, datePatternToken{lit: []uint16{uint16(c)}})
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i
			for j < len(s) && s[j] == c {
				j++
			}
			p.tokens = append(p.tokens, datePatternToken{letter: c, count: j - i})
			i = j
		case c == '\'':
			j := i + 1
			for j < len(s) && s[j] != '\'' {
				literal(s[j])
				j++
			}
			i = j + 1
		default:
			literal(c)
			i++
		}
	}
	return p
}

// dateRFC822 is tried first, leniently.
var dateRFC822 = compileDatePattern("EEE, dd MMM yyyy HH:mm:ss Z")

// dateFallback is one of DateUtils' fallback patterns. A shadowed pattern
// reads exactly what an earlier one reads (EEE and EEEE, MMM and MMMM parse
// alike), so it can never answer and is skipped.
type dateFallback struct {
	pattern  datePattern
	shadowed bool
}

// dateFallbacks are tried in order, strictly.
var dateFallbacks = [...]dateFallback{
	{compileDatePattern("dd MMM yy HH:mm:ss Z"), false},               // F0
	{compileDatePattern("dd MMM yy HH:mm Z"), false},                  // F1
	{compileDatePattern("EEE, dd MMM yyyy HH:mm:ss Z"), false},        // F2
	{compileDatePattern("EEE, dd MMM yyyy HH:mm:ss"), false},          // F3
	{compileDatePattern("EEE, dd MMMM yyyy HH:mm:ss Z"), true},        // F4, as F2
	{compileDatePattern("EEE, dd MMMM yyyy HH:mm:ss"), true},          // F5, as F3
	{compileDatePattern("EEEE, dd MMM yyyy HH:mm:ss Z"), true},        // F6, as F2
	{compileDatePattern("EEEE, dd MMM yy HH:mm:ss Z"), false},         // F7
	{compileDatePattern("EEEE, dd MMM yyyy HH:mm:ss"), true},          // F8, as F3
	{compileDatePattern("EEEE, dd MMM yy HH:mm:ss"), false},           // F9
	{compileDatePattern("EEE MMM d HH:mm:ss yyyy"), false},            // F10
	{compileDatePattern("EEE, dd MMM yyyy HH:mm Z"), false},           // F11
	{compileDatePattern("EEE, dd MMM yyyy HH:mm"), false},             // F12
	{compileDatePattern("EEE, dd MMMM yyyy HH:mm Z"), true},           // F13, as F11
	{compileDatePattern("EEE, dd MMMM yyyy HH:mm"), true},             // F14, as F12
	{compileDatePattern("EEEE, dd MMM yyyy HH:mm Z"), true},           // F15, as F11
	{compileDatePattern("EEEE, dd MMM yy HH:mm Z"), false},            // F16
	{compileDatePattern("EEEE, dd MMM yyyy HH:mm"), true},             // F17, as F12
	{compileDatePattern("EEEE, dd MMM yy HH:mm"), false},              // F18
	{compileDatePattern("EEE MMM d HH:mm yyyy"), false},               // F19
	{compileDatePattern("yyyy-MM-dd'T'HH:mm:ss"), false},              // F20
	{compileDatePattern("yyyy-MM-dd'T'HH:mm:ss.SSS Z"), false},        // F21
	{compileDatePattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), false},       // F22
	{compileDatePattern("yyyy-MM-dd'T'HH:mm:ss.SSS"), false},          // F23
	{compileDatePattern("yyyy-MM-dd'T'HH:mm:ssZ"), false},             // F24
	{compileDatePattern("yyyy-MM-dd'T'HH:mm:ss'Z'"), false},           // F25
	{compileDatePattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), false},         // F26
	{compileDatePattern("yyyy-MM-ddZ"), false},                        // F27
	{compileDatePattern("yyyy-MM-dd"), false},                         // F28
	{compileDatePattern("EEE d MMM yyyy HH:mm:ss 'GMT'Z (z)"), false}, // F29
}

// rfc822 tries the RFC 822 pattern leniently on the date as given; text after
// it is ignored.
func (p *dateParser) rfc822(t []uint16) (int64, bool) {
	ms, _, ok := p.match(&dateRFC822, t, true)
	return ms, ok
}

// fallback tries the fallback patterns strictly; the first that reads all of
// t answers. It also returns that pattern's index.
func (p *dateParser) fallback(t []uint16) (int64, int, bool) {
	for i := range dateFallbacks {
		f := &dateFallbacks[i]
		if f.shadowed {
			continue
		}
		if ms, end, ok := p.match(&f.pattern, t, false); ok && end == len(t) {
			return ms, i, true
		}
	}
	return 0, 0, false
}

// match matches a pattern from the start of t and resolves its fields: the
// instant, where the pattern ended, and whether it matched.
func (p *dateParser) match(pat *datePattern, t []uint16, lenient bool) (int64, int, bool) {
	var f dateFields
	pos := 0
	for _, tok := range pat.tokens {
		if tok.letter == 0 {
			if len(t)-pos < len(tok.lit) {
				return 0, 0, false
			}
			for i, c := range tok.lit {
				if t[pos+i] != c {
					return 0, 0, false
				}
			}
			pos += len(tok.lit)
			continue
		}
		start := pos
		for pos < len(t) && dateIsBlank(t[pos]) {
			p.w.step()
			pos++
		}
		if pos == len(t) {
			return 0, 0, false
		}
		var ok bool
		switch tok.letter {
		case 'E':
			var day int
			if day, pos, ok = dateMatchName(t, start, pos, dateWeekdays[1:], dateShortWeekdays[1:]); ok {
				f.record(dateWeekday, int64(day+1))
			}
		case 'M':
			if tok.count >= 3 {
				var month int
				if month, pos, ok = dateMatchName(t, start, pos, dateMonths, dateShortMonths); ok {
					f.record(dateMonth, int64(month))
				}
				break
			}
			fallthrough
		case 'y', 'd', 'H', 'm', 's', 'S':
			pos, ok = p.numericField(t, pos, tok, &f)
		case 'Z', 'z':
			pos, ok = p.zoneField(t, start, pos, &f)
		default:
			panic("feed: unsupported date pattern letter " + string(tok.letter))
		}
		if !ok {
			return 0, 0, false
		}
	}
	ms, ok := p.resolve(&f, lenient)
	if ok && f.ambiguousYear && ms < p.yearWindow().start {
		// The year of a two-digit value equal to the window's start year is
		// a century later when the date falls before the window.
		f.val[dateYear] += 100
		ms, ok = p.resolve(&f, lenient)
	}
	return ms, pos, ok
}

// numericField reads a numeric field's number at pos and records it.
func (p *dateParser) numericField(t []uint16, pos int, tok datePatternToken, f *dateFields) (int, bool) {
	n, end, ok := p.readNumber(t, pos)
	if !ok {
		return 0, false
	}
	switch tok.letter {
	case 'y':
		year := n.int32()
		if tok.count <= 2 && end-pos == 2 && dateIsUnitDigit(t[pos]) && dateIsUnitDigit(t[pos+1]) {
			w := p.yearWindow()
			v := int64(year)
			y := w.base + v
			if v < w.ambiguous {
				y += 100
			}
			f.record(dateYear, int64(int32(y)))
			f.ambiguousYear = v == w.ambiguous
		} else {
			f.record(dateYear, int64(year))
		}
	case 'M':
		f.record(dateMonth, int64(n.int32()-1))
	case 'd':
		f.record(dateDay, int64(n.int32()))
	case 'H':
		f.record(dateHour, int64(n.int32()))
	case 'm':
		f.record(dateMinute, int64(n.int32()))
	case 's':
		f.record(dateSecond, int64(n.int32()))
	case 'S':
		f.record(dateMillis, int64(n.fraction(end-pos)))
	}
	return end, true
}

// dateMatchName matches a weekday or month name at start (where the field's
// blanks began; a skipped blank makes it fail): the longest of the long
// names, else the longest of the short ones, unit by unit ignoring case. It
// returns the name's index and the end.
func dateMatchName(t []uint16, start, pos int, long, short []string) (int, int, bool) {
	if pos != start {
		return 0, 0, false
	}
	for _, names := range [2][]string{long, short} {
		best, bestLen := -1, 0
		for i, name := range names {
			if len(name) > bestLen && len(t)-start >= len(name) && dateNameAt(t, start, name) {
				best, bestLen = i, len(name)
			}
		}
		if best >= 0 {
			return best, start + bestLen, true
		}
	}
	return 0, 0, false
}

// dateNameAt reports whether an ASCII name is at t[at:], ignoring case.
func dateNameAt(t []uint16, at int, name string) bool {
	for i := range len(name) {
		if !dateUnitsMatchIgnoringCase(t[at+i], uint16(name[i])) {
			return false
		}
	}
	return true
}

// zoneField reads a zone (Z or z) at pos: a numeric offset, GMT with or
// without one, or a zone name.
func (p *dateParser) zoneField(t []uint16, start, pos int, f *dateFields) (int, bool) {
	switch c := t[pos]; {
	case c == '+' || c == '-':
		return dateReadOffset(t, pos, f)
	case (c == 'G' || c == 'g') && len(t)-start >= 3 && dateNameAt(t, start, "GMT"):
		// GMT is looked for where the blanks began: after a blank, "GMT" is
		// read as a zone name instead.
		q := start + 3
		if q < len(t) && (t[q] == '+' || t[q] == '-') {
			return dateReadOffset(t, q, f)
		}
		f.record(dateZoneOffset, 0)
		f.record(dateDSTOffset, 0)
		return q, true
	}
	return p.zoneName(t, pos, f)
}

// dateReadOffset reads [+-]H[H][:]MM after the sign at t[sign], ASCII digits
// only, hours up to 23 and minutes up to 59.
func dateReadOffset(t []uint16, sign int, f *dateFields) (int, bool) {
	digit := func(i int) (int64, bool) {
		if i < len(t) && t[i] >= '0' && t[i] <= '9' {
			return int64(t[i] - '0'), true
		}
		return 0, false
	}
	i := sign + 1
	hours, ok := digit(i)
	if !ok {
		return 0, false
	}
	i++
	if d, ok := digit(i); ok {
		hours = hours*10 + d
		i++
	}
	if hours > 23 {
		return 0, false
	}
	if i < len(t) && t[i] == ':' {
		i++
	}
	tens, ok1 := digit(i)
	ones, ok2 := digit(i + 1)
	if !ok1 || !ok2 || tens*10+ones > 59 {
		return 0, false
	}
	ms := (hours*60 + tens*10 + ones) * 60_000
	if t[sign] == '-' {
		ms = -ms
	}
	f.record(dateZoneOffset, ms)
	f.record(dateDSTOffset, 0)
	return i + 2, true
}

// zoneName reads the longest zone name at pos, code point by code point under
// simple case folding, and resolves it to a zone: its own, or for a metazone
// name the zone an earlier name chose if it has used that metazone, else the
// metazone's reference zone.
func (p *dateParser) zoneName(t []uint16, pos int, f *dateFields) (int, bool) {
	names := dateZoneNameKeys()
	var keys [dateZoneNameMaxRunes]rune
	var ends [dateZoneNameMaxRunes]int
	n := 0
	for i := pos; i < len(t) && n < len(keys); n++ {
		r, size := dateRuneAt(t, i)
		i += size
		keys[n], ends[n] = dateFoldKey(r), i
	}
	best, bestEnd := -1, 0
	for i, name := range names {
		if len(name) > n {
			continue
		}
		matched := true
		for j, k := range name {
			if keys[j] != k {
				matched = false
				break
			}
		}
		if matched && ends[len(name)-1] > bestEnd {
			best, bestEnd = i, ends[len(name)-1]
		}
	}
	if best < 0 {
		return 0, false
	}
	name := &dateZoneNames[best]
	id := name.tz
	if id == "" {
		current := "Etc/GMT"
		if f.zone != nil {
			current = f.zone.id
		}
		id = dateMetaZoneRefs[name.mz]
		for _, mz := range dateZoneMetaZones[current] {
			if mz == name.mz {
				id = current
				break
			}
		}
	}
	zone := dateZoneByID(id)
	if zone == nil {
		return 0, false
	}
	f.zone = zone
	switch {
	case !name.dst:
		f.forget(dateZoneOffset)
		f.record(dateDSTOffset, 0)
	default:
		if s := zone.savings(p.now); s != 0 {
			f.forget(dateZoneOffset)
			f.record(dateDSTOffset, s)
		}
	}
	return bestEnd, true
}
