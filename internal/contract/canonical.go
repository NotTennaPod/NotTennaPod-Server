package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// MaxSafeInteger bounds every integer in protocol data (PROTOCOL §3).
const MaxSafeInteger = 1<<53 - 1

// ErrNotCanonicalizable reports a value outside the contract's JSON subset.
var ErrNotCanonicalizable = errors.New("contract: value not representable in canonical JSON")

// Canonical returns the RFC 8785 (JCS) serialization of a decoded protocol value:
// map[string]any, []any, string, json.Number (integers only), int/int64, bool or nil.
func Canonical(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if x {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		if !utf8.ValidString(x) {
			return fmt.Errorf("%w: invalid UTF-8 string", ErrNotCanonicalizable)
		}
		writeCanonicalString(buf, x)
	case json.Number:
		n, err := canonicalInteger(string(x))
		if err != nil {
			return err
		}
		buf.WriteString(strconv.FormatInt(n, 10))
	case int:
		return writeCanonical(buf, int64(x))
	case int64:
		if x > MaxSafeInteger || x < -MaxSafeInteger {
			return fmt.Errorf("%w: integer out of range", ErrNotCanonicalizable)
		}
		buf.WriteString(strconv.FormatInt(x, 10))
	case []any:
		buf.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			if !utf8.ValidString(k) {
				return fmt.Errorf("%w: invalid UTF-8 key", ErrNotCanonicalizable)
			}
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonicalString(buf, k)
			buf.WriteByte(':')
			if err := writeCanonical(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("%w: unsupported type %T", ErrNotCanonicalizable, v)
	}
	return nil
}

// canonicalInteger accepts only the JSON integer grammar within ±(2^53-1): no
// fraction, exponent or "-0".
func canonicalInteger(s string) (int64, error) {
	if s == "" || s == "-0" {
		return 0, fmt.Errorf("%w: number %q", ErrNotCanonicalizable, s)
	}
	digits := s
	if digits[0] == '-' {
		digits = digits[1:]
	}
	if digits == "" || (len(digits) > 1 && digits[0] == '0') {
		return 0, fmt.Errorf("%w: number %q", ErrNotCanonicalizable, s)
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: number %q", ErrNotCanonicalizable, s)
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n > MaxSafeInteger || n < -MaxSafeInteger {
		return 0, fmt.Errorf("%w: number %q out of range", ErrNotCanonicalizable, s)
	}
	return n, nil
}

func lessUTF16(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

// writeCanonicalString follows ECMAScript JSON.stringify as required by JCS.
func writeCanonicalString(buf *bytes.Buffer, s string) {
	const hexDigits = "0123456789abcdef"
	buf.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if r < 0x20 {
				buf.WriteString(`\u00`)
				buf.WriteByte(hexDigits[r>>4])
				buf.WriteByte(hexDigits[r&0xf])
			} else {
				buf.WriteRune(r)
			}
		}
	}
	buf.WriteByte('"')
}
