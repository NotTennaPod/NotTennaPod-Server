package contract

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// MaxDepth bounds nesting of protocol JSON (PROTOCOL §3).
const MaxDepth = 64

// DecodeJSON strictly decodes protocol JSON (PROTOCOL §3): it rejects invalid UTF-8,
// duplicate member names, lone surrogate escapes, unescaped control characters,
// trailing data, nesting deeper than MaxDepth, and numbers outside the integer
// grammar and range. Numbers are returned as json.Number, objects as map[string]any
// and arrays as []any.
func DecodeJSON(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("%w: invalid UTF-8", ErrNotCanonicalizable)
	}
	d := decoder{s: string(data)}
	d.space()
	v, err := d.value(0)
	if err != nil {
		return nil, err
	}
	d.space()
	if d.i != len(d.s) {
		return nil, d.fail("trailing data")
	}
	return v, nil
}

type decoder struct {
	s string
	i int
}

func (d *decoder) fail(what string) error {
	return fmt.Errorf("%w: %s at byte %d", ErrNotCanonicalizable, what, d.i)
}

func (d *decoder) space() {
	for d.i < len(d.s) {
		switch d.s[d.i] {
		case ' ', '\t', '\n', '\r':
			d.i++
		default:
			return
		}
	}
}

func (d *decoder) value(depth int) (any, error) {
	if d.i >= len(d.s) {
		return nil, d.fail("unexpected end")
	}
	switch c := d.s[d.i]; {
	case c == '{':
		return d.object(depth + 1)
	case c == '[':
		return d.array(depth + 1)
	case c == '"':
		return d.str()
	case c == '-' || c >= '0' && c <= '9':
		return d.number()
	case strings.HasPrefix(d.s[d.i:], "true"):
		d.i += 4
		return true, nil
	case strings.HasPrefix(d.s[d.i:], "false"):
		d.i += 5
		return false, nil
	case strings.HasPrefix(d.s[d.i:], "null"):
		d.i += 4
		return nil, nil
	}
	return nil, d.fail("unexpected character")
}

func (d *decoder) object(depth int) (any, error) {
	if depth > MaxDepth {
		return nil, d.fail("nesting too deep")
	}
	d.i++ // {
	obj := map[string]any{}
	d.space()
	if d.i < len(d.s) && d.s[d.i] == '}' {
		d.i++
		return obj, nil
	}
	for {
		d.space()
		if d.i >= len(d.s) || d.s[d.i] != '"' {
			return nil, d.fail("expected member name")
		}
		key, err := d.str()
		if err != nil {
			return nil, err
		}
		if _, dup := obj[key]; dup {
			return nil, d.fail("duplicate member name")
		}
		d.space()
		if d.i >= len(d.s) || d.s[d.i] != ':' {
			return nil, d.fail("expected ':'")
		}
		d.i++
		d.space()
		v, err := d.value(depth)
		if err != nil {
			return nil, err
		}
		obj[key] = v
		d.space()
		if d.i >= len(d.s) {
			return nil, d.fail("unexpected end")
		}
		if d.s[d.i] == ',' {
			d.i++
			continue
		}
		if d.s[d.i] == '}' {
			d.i++
			return obj, nil
		}
		return nil, d.fail("expected ',' or '}'")
	}
}

func (d *decoder) array(depth int) (any, error) {
	if depth > MaxDepth {
		return nil, d.fail("nesting too deep")
	}
	d.i++ // [
	arr := []any{}
	d.space()
	if d.i < len(d.s) && d.s[d.i] == ']' {
		d.i++
		return arr, nil
	}
	for {
		d.space()
		v, err := d.value(depth)
		if err != nil {
			return nil, err
		}
		arr = append(arr, v)
		d.space()
		if d.i >= len(d.s) {
			return nil, d.fail("unexpected end")
		}
		if d.s[d.i] == ',' {
			d.i++
			continue
		}
		if d.s[d.i] == ']' {
			d.i++
			return arr, nil
		}
		return nil, d.fail("expected ',' or ']'")
	}
}

func (d *decoder) hex4() (rune, bool) {
	if d.i+4 > len(d.s) {
		return 0, false
	}
	n, err := strconv.ParseUint(d.s[d.i:d.i+4], 16, 16)
	if err != nil {
		return 0, false
	}
	d.i += 4
	return rune(n), true
}

func (d *decoder) str() (string, error) {
	d.i++ // opening quote
	var b strings.Builder
	for {
		if d.i >= len(d.s) {
			return "", d.fail("unterminated string")
		}
		c := d.s[d.i]
		switch {
		case c == '"':
			d.i++
			return b.String(), nil
		case c < 0x20:
			return "", d.fail("unescaped control character")
		case c != '\\':
			b.WriteByte(c)
			d.i++
			continue
		}
		d.i++ // backslash
		if d.i >= len(d.s) {
			return "", d.fail("unterminated escape")
		}
		e := d.s[d.i]
		d.i++
		switch e {
		case '"', '\\', '/':
			b.WriteByte(e)
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'u':
			r, ok := d.hex4()
			if !ok {
				return "", d.fail("invalid \\u escape")
			}
			if utf16.IsSurrogate(r) {
				if r >= 0xdc00 || !strings.HasPrefix(d.s[d.i:], `\u`) {
					return "", d.fail("lone surrogate")
				}
				d.i += 2
				r2, ok := d.hex4()
				if !ok || r2 < 0xdc00 || r2 > 0xdfff {
					return "", d.fail("lone surrogate")
				}
				r = utf16.DecodeRune(r, r2)
			}
			b.WriteRune(r)
		default:
			return "", d.fail("invalid escape")
		}
	}
}

func (d *decoder) number() (any, error) {
	start := d.i
	if d.s[d.i] == '-' {
		d.i++
	}
	for d.i < len(d.s) {
		c := d.s[d.i]
		if c >= '0' && c <= '9' || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-' {
			d.i++
			continue
		}
		break
	}
	text := d.s[start:d.i]
	if _, err := canonicalInteger(text); err != nil {
		return nil, err
	}
	return json.Number(text), nil
}

// Depth returns the nesting depth of a decoded value: 1 for an object or array
// with only scalar members, 0 for a scalar (the counting rule of DecodeJSON).
func Depth(v any) int {
	switch x := v.(type) {
	case map[string]any:
		d := 0
		for _, e := range x {
			d = max(d, Depth(e))
		}
		return d + 1
	case []any:
		d := 0
		for _, e := range x {
			d = max(d, Depth(e))
		}
		return d + 1
	}
	return 0
}
