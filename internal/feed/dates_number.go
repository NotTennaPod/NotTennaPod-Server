package feed

import (
	"math"
	"strconv"
	"unicode"
)

// Numbers in date fields, read as ICU reads an integer: an optional minus
// sign (any of Unicode's), then decimal digits of any script, or NaN, with ∞
// allowed once, and at most one exponent. A plus sign does not start a
// number.

// dateNumberKind tells a finite number from NaN and the infinities.
type dateNumberKind uint8

const (
	dateFinite dateNumberKind = iota
	dateNaN
	datePosInf
	dateNegInf
)

// dateNumber is a number read from a date: for a finite one, its significant
// digits (ASCII, no leading or trailing zeros; empty for zero) times 10^exp.
type dateNumber struct {
	kind   dateNumberKind
	neg    bool
	digits []byte
	exp    int64
}

// readNumber reads a number at t[at:]: the number, where it ends, and whether
// one was there.
func (p *dateParser) readNumber(t []uint16, at int) (dateNumber, int, bool) {
	var (
		neg, seen, nan, inf, hasExp bool
		expNeg, expHuge             bool
		expVal                      int64
	)
	digits := p.digits[:0]
	i := at
loop:
	for i < len(t) {
		r, size := dateRuneAt(t, i)
		switch {
		case !seen && !neg && dateIsMinus(r):
			neg = true
			i += size
		case !seen && dateFoldsTo(t, i, "NaN"):
			nan, seen = true, true
			i += 3
		case !seen && dateIsDigit(r):
			for i < len(t) {
				r, size := dateRuneAt(t, i)
				if !dateIsDigit(r) {
					break
				}
				p.w.step()
				digits = append(digits, byte('0'+dateDigitValue(r)))
				i += size
			}
			seen = true
		case !inf && r == '∞':
			inf, seen = true, true
			i += size
		case seen && !hasExp && (r == 'E' || r == 'e'):
			end, eNeg, eVal, eHuge, ok := p.readExponent(t, i+size)
			if !ok {
				break loop
			}
			hasExp, expNeg, expVal, expHuge = true, eNeg, eVal, eHuge
			i = end
		default:
			break loop
		}
	}
	p.digits = digits
	if !seen {
		return dateNumber{}, 0, false
	}
	n := dateNumber{neg: neg}
	switch {
	case nan:
		n.kind = dateNaN
		return n, i, true
	case inf:
		n.kind = datePosInf
		if neg {
			n.kind = dateNegInf
		}
		return n, i, true
	}
	lead := 0
	for lead < len(digits) && digits[lead] == '0' {
		lead++
	}
	trail := len(digits)
	for trail > lead && digits[trail-1] == '0' {
		trail--
	}
	n.digits, n.exp = digits[lead:trail], int64(len(digits)-trail)
	if !hasExp {
		return n, i, true
	}
	overflow := expHuge
	if !overflow && len(n.digits) > 0 {
		k := n.exp + expVal
		if expNeg {
			k = n.exp - expVal
		}
		overflow = k < math.MinInt32 || k > math.MaxInt32 || k+int64(len(n.digits)) > math.MaxInt32
		n.exp = k
	}
	if overflow {
		// Too large an exponent: zero when negative, else an infinity.
		n.digits, n.exp = nil, 0
		if !expNeg {
			n.kind = datePosInf
			if neg {
				n.kind = dateNegInf
			}
		}
	}
	return n, i, true
}

// readExponent reads an exponent's rest after its E: an optional bidi
// control, an optional sign, an optional bidi control and at least one
// decimal digit. huge reports a value of more than ten digits or above
// 2,147,483,647.
func (p *dateParser) readExponent(t []uint16, i int) (end int, neg bool, val int64, huge, ok bool) {
	optional := func(is func(rune) bool) rune {
		if i < len(t) {
			if r, size := dateRuneAt(t, i); is(r) {
				i += size
				return r
			}
		}
		return 0
	}
	isBidi := func(r rune) bool { return unicode.Is(unicode.Bidi_Control, r) }
	optional(isBidi)
	if s := optional(func(r rune) bool { return dateIsMinus(r) || dateIsPlus(r) }); s != 0 {
		neg = dateIsMinus(s)
	}
	optional(isBidi)
	n, sig := 0, 0
	for i < len(t) {
		r, size := dateRuneAt(t, i)
		if !dateIsDigit(r) {
			break
		}
		p.w.step()
		d := int64(dateDigitValue(r))
		if sig > 0 || d != 0 {
			sig++
			if sig <= 10 {
				val = val*10 + d
			}
		}
		n++
		i += size
	}
	if n == 0 {
		return 0, false, 0, false, false
	}
	return i, neg, val, sig > 10 || val > math.MaxInt32, true
}

// int32 is the number as a 32-bit field value: an exact integer that fits in
// 64 bits gives its low 32 bits; anything else is truncated toward zero and
// saturates; NaN is 0.
func (n *dateNumber) int32() int32 {
	switch n.kind {
	case dateNaN:
		return 0
	case datePosInf:
		return math.MaxInt32
	case dateNegInf:
		return math.MinInt32
	}
	if len(n.digits) == 0 {
		return 0
	}
	if n.exp >= 0 && int64(len(n.digits))+n.exp <= 19 {
		var v uint64
		for _, d := range n.digits {
			v = v*10 + uint64(d-'0')
		}
		for range n.exp {
			v *= 10
		}
		if !n.neg && v <= math.MaxInt64 {
			return int32(v)
		}
		if n.neg && v <= 1<<63 {
			return int32(-v)
		}
	}
	return dateSaturate(n.float64())
}

// float64 is the number's double value. The digits and exponent go through
// strconv.ParseFloat, which reads exponents only up to 10,000.
func (n *dateNumber) float64() float64 {
	var v float64
	switch n.kind {
	case dateNaN:
		return math.NaN()
	case datePosInf:
		return math.Inf(1)
	case dateNegInf:
		return math.Inf(-1)
	}
	if len(n.digits) > 0 {
		switch m := int64(len(n.digits)) + n.exp; {
		case m > 400:
			v = math.Inf(1)
		case m < -400:
			v = 0
		default:
			v, _ = strconv.ParseFloat(string(n.digits)+"e"+strconv.FormatInt(n.exp, 10), 64)
		}
	}
	if n.neg {
		v = -v
	}
	return v
}

// fraction is the milliseconds of an S field whose number is width units
// long: the value over 10^width, times 1000.
func (n *dateNumber) fraction(width int) int32 {
	return dateSaturate(n.float64() / math.Pow10(width) * 1000)
}

// dateSaturate truncates toward zero into 32 bits, saturating; NaN is 0.
func dateSaturate(x float64) int32 {
	switch {
	case x != x:
		return 0
	case x >= math.MaxInt32:
		return math.MaxInt32
	case x <= math.MinInt32:
		return math.MinInt32
	}
	return int32(x)
}

// dateFoldsTo reports whether the ASCII word is at t[at:], each unit
// fold-equal to its letter.
func dateFoldsTo(t []uint16, at int, word string) bool {
	if len(t)-at < len(word) {
		return false
	}
	for i := range len(word) {
		if dateFoldKey(rune(t[at+i])) != dateFoldKey(rune(word[i])) {
			return false
		}
	}
	return true
}
