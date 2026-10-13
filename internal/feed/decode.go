package feed

import (
	"bytes"
	"errors"
	"regexp"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/encoding/ianaindex"
)

// How Android turns a downloaded feed file into characters: FeedHandler and
// TypeGetter both read through commons-io XmlStreamReader (lenient), which picks
// the encoding from the byte-order mark, the first bytes and the XML
// declaration, and decodes with InputStreamReader (malformed input becomes
// U+FFFD).

// errEncoding is XmlStreamReader failing: an unknown declared charset or a
// declaration it cannot find the end of. Android then classifies the document
// through TypeGetter's error path.
var errEncoding = errors.New("feed: unreadable encoding")

var byteOrderMarks = []struct {
	mark     []byte
	encoding string
}{
	// BOMInputStream tries the longest marks first.
	{[]byte{0x00, 0x00, 0xFE, 0xFF}, "UTF-32BE"}, {[]byte{0xFF, 0xFE, 0x00, 0x00}, "UTF-32LE"},
	{[]byte{0xEF, 0xBB, 0xBF}, "UTF-8"}, {[]byte{0xFE, 0xFF}, "UTF-16BE"}, {[]byte{0xFF, 0xFE}, "UTF-16LE"},
}

var guesses = []struct {
	start    []byte
	encoding string
}{
	{[]byte{0x3C, 0x3F, 0x78, 0x6D}, "UTF-8"}, {[]byte{0x00, 0x3C, 0x00, 0x3F}, "UTF-16BE"}, {[]byte{0x3C, 0x00, 0x3F, 0x00}, "UTF-16LE"},
	{[]byte{0, 0, 0, 0x3C, 0, 0, 0, 0x3F, 0, 0, 0, 0x78, 0, 0, 0, 0x6D}, "UTF-32BE"},
	{[]byte{0x3C, 0, 0, 0, 0x3F, 0, 0, 0, 0x78, 0, 0, 0, 0x6D, 0, 0, 0}, "UTF-32LE"},
	{[]byte{0x4C, 0x6F, 0xA7, 0x94}, "CP1047"},
}

var prologEncoding = regexp.MustCompile(`<\?xml.*encoding[\s]*=[\s]*((?:".[^"]*")|(?:'.[^']*'))`)

// decodeDocument is XmlStreamReader's detection and decoding.
func decodeDocument(data []byte) (string, error) {
	bom := ""
	body := data
	for _, b := range byteOrderMarks {
		if bytes.HasPrefix(data, b.mark) {
			bom, body = b.encoding, data[len(b.mark):]
			break
		}
	}
	guess := ""
	for _, g := range guesses {
		if bytes.HasPrefix(body, g.start) {
			guess = g.encoding
			break
		}
	}
	declared, err := declaredEncoding(body, guess)
	if err != nil {
		return "", err
	}
	var enc string
	switch {
	case bom == "":
		switch {
		case guess == "" || declared == "":
			enc = "UTF-8"
		case declared == "UTF-16" && (guess == "UTF-16BE" || guess == "UTF-16LE"):
			enc = guess
		default:
			enc = declared
		}
	case bom == "UTF-8":
		// A conflicting guess or declaration throws; lenient detection falls back
		// to the declared encoding (or UTF-8).
		if (guess != "" && guess != "UTF-8") || (declared != "" && declared != "UTF-8") {
			enc = orUTF8(declared)
		} else {
			enc = "UTF-8"
		}
	case strings.HasPrefix(bom, "UTF-16"):
		if (guess != "" && guess != bom) || (declared != "" && declared != "UTF-16" && declared != bom) {
			enc = orUTF8(declared)
		} else {
			enc = bom
		}
	default:
		enc = orUTF8(declared)
	}
	return decodeCharset(body, enc)
}

func orUTF8(s string) string {
	if s == "" {
		return "UTF-8"
	}
	return s
}

// declaredEncoding is XmlStreamReader.getXmlProlog: the declaration up to the
// first '>' within 4096 bytes read with the guessed encoding, its encoding
// attribute upper-cased.
func declaredEncoding(body []byte, guess string) (string, error) {
	if guess == "" {
		return "", nil
	}
	head := body
	if len(head) > 4096 {
		head = head[:4096]
	}
	text, err := decodeCharset(head, guess)
	end := strings.IndexByte(text, '>')
	if err != nil || end < 0 {
		return "", errEncoding
	}
	prolog := strings.NewReplacer("\r\n", "", "\n", "", "\r", "").Replace(text[:end+1])
	m := prologEncoding.FindStringSubmatch(prolog)
	if m == nil {
		return "", nil
	}
	v := m[1]
	return strings.ToUpper(v[1 : len(v)-1]), nil
}

// decodeCharset is InputStreamReader with a Java charset name: malformed input
// becomes U+FFFD; an unknown charset fails.
func decodeCharset(b []byte, name string) (string, error) {
	switch strings.ToUpper(name) {
	case "UTF-8", "UTF8":
		return decodeUTF8(b), nil
	case "UTF-16BE", "UTF-16LE", "UTF-16":
		little := strings.ToUpper(name) == "UTF-16LE"
		if strings.ToUpper(name) == "UTF-16" && len(b) >= 2 {
			// Java's UTF-16 honours a leading mark and otherwise reads big-endian.
			if b[0] == 0xFF && b[1] == 0xFE {
				little, b = true, b[2:]
			} else if b[0] == 0xFE && b[1] == 0xFF {
				b = b[2:]
			}
		}
		units := make([]uint16, 0, len(b)/2)
		for i := 0; i+1 < len(b); i += 2 {
			if little {
				units = append(units, uint16(b[i])|uint16(b[i+1])<<8)
			} else {
				units = append(units, uint16(b[i])<<8|uint16(b[i+1]))
			}
		}
		s := string(utf16.Decode(units))
		if len(b)%2 == 1 {
			s += "�"
		}
		return s, nil
	case "UTF-32BE", "UTF-32LE", "UTF-32":
		little := strings.ToUpper(name) == "UTF-32LE"
		var sb strings.Builder
		for i := 0; i+3 < len(b); i += 4 {
			var r rune
			if little {
				r = rune(b[i]) | rune(b[i+1])<<8 | rune(b[i+2])<<16 | rune(b[i+3])<<24
			} else {
				r = rune(b[i])<<24 | rune(b[i+1])<<16 | rune(b[i+2])<<8 | rune(b[i+3])
			}
			if !utf8.ValidRune(r) {
				r = utf8.RuneError
			}
			sb.WriteRune(r)
		}
		if len(b)%4 != 0 {
			sb.WriteRune(utf8.RuneError)
		}
		return sb.String(), nil
	}
	enc, err := charset(name)
	if err != nil {
		return "", errEncoding
	}
	out, err := enc.NewDecoder().Bytes(b)
	if err != nil {
		return "", errEncoding
	}
	return strings.ToValidUTF8(string(out), "�"), nil
}

// charset resolves a Java charset name.
func charset(name string) (encoding.Encoding, error) {
	if e, err := ianaindex.IANA.Encoding(name); err == nil && e != nil {
		return e, nil
	}
	if e, err := htmlindex.Get(name); err == nil {
		return e, nil
	}
	return nil, errEncoding
}

// DecodeUTF8 decodes bytes as UTF-8 the way Android's (ICU) decoder does (an
// OPML import reads its file so, PROTOCOL §11.5).
func DecodeUTF8(b []byte) string { return decodeUTF8(b) }

// decodeUTF8 decodes UTF-8 as Android's (ICU) decoder does: each maximal
// subpart of an ill-formed sequence (Unicode 3.9, U+FFFD substitution of
// maximal subparts) becomes one U+FFFD.
func decodeUTF8(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	var sb strings.Builder
	for i := 0; i < len(b); {
		c := b[i]
		if c < 0x80 {
			sb.WriteByte(c)
			i++
			continue
		}
		// The sequence's length and its second byte's range (Unicode Table 3-7).
		n, lo, hi := 0, byte(0x80), byte(0xBF)
		switch {
		case c >= 0xC2 && c <= 0xDF:
			n = 2
		case c == 0xE0:
			n, lo = 3, 0xA0
		case c >= 0xE1 && c <= 0xEC, c == 0xEE, c == 0xEF:
			n = 3
		case c == 0xED:
			n, hi = 3, 0x9F
		case c == 0xF0:
			n, lo = 4, 0x90
		case c >= 0xF1 && c <= 0xF3:
			n = 4
		case c == 0xF4:
			n, hi = 4, 0x8F
		}
		k := 1
		for n > 0 && k < n && i+k < len(b) {
			x := b[i+k]
			if k == 1 && (x < lo || x > hi) || k > 1 && (x < 0x80 || x > 0xBF) {
				break
			}
			k++
		}
		if n > 0 && k == n {
			sb.Write(b[i : i+n])
		} else {
			sb.WriteRune(utf8.RuneError)
		}
		i += k
	}
	return sb.String()
}
