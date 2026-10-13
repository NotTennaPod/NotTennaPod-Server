package feed

import (
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ---- Java number parsing

// javaDigit is Character.digit(char, 10): Unicode decimal digits count, but
// only in the Basic Multilingual Plane (Java looks at UTF-16 units, and a
// surrogate is no digit).
func javaDigit(r rune) (int, bool) {
	if r >= '0' && r <= '9' {
		return int(r - '0'), true
	}
	if r > 0xFFFF || !unicode.IsDigit(r) {
		return 0, false
	}
	v := 0
	for d := r - 1; v < 9 && unicode.IsDigit(d); d-- {
		v++
	}
	return v, true
}

// javaParseLong is Long.parseLong (radix 10).
func javaParseLong(s string) (int64, bool) {
	return javaParseInteger(s, math.MinInt64, math.MaxInt64)
}

// javaParseInt is Integer.parseInt (radix 10).
func javaParseInt(s string) (int64, bool) {
	return javaParseInteger(s, math.MinInt32, math.MaxInt32)
}

func javaParseInteger(s string, min, max int64) (int64, bool) {
	rs := []rune(s)
	if len(rs) == 0 {
		return 0, false
	}
	neg, i := false, 0
	limit := -max
	if rs[0] < '0' {
		switch rs[0] {
		case '-':
			neg, limit = true, min
		case '+':
		default:
			return 0, false
		}
		if len(rs) == 1 {
			return 0, false
		}
		i = 1
	}
	// Integer.parseInt's algorithm: accumulate negatively against the limit.
	multmin := limit / 10
	var result int64
	for ; i < len(rs); i++ {
		d, ok := javaDigit(rs[i])
		if !ok || result < multmin {
			return 0, false
		}
		result *= 10
		if result < limit+int64(d) {
			return 0, false
		}
		result -= int64(d)
	}
	if neg {
		return result, true
	}
	return -result, true
}

// javaParseFloat is Float.parseFloat: surrounding whitespace is trimmed, an
// optional f/F/d/D suffix, decimal or hexadecimal significands, "NaN" and
// "Infinity"; rounded once to a float.
func javaParseFloat(s string) (float32, bool) {
	s = javaTrim(s)
	if s == "" {
		return 0, false
	}
	body := s
	sign := ""
	if body[0] == '+' || body[0] == '-' {
		sign, body = body[:1], body[1:]
	}
	switch body {
	case "NaN":
		return float32(math.NaN()), true
	case "Infinity":
		if sign == "-" {
			return float32(math.Inf(-1)), true
		}
		return float32(math.Inf(1)), true
	}
	if n := len(body); n > 0 && strings.ContainsRune("fFdD", rune(body[n-1])) {
		body = body[:n-1]
	}
	if body == "" {
		return 0, false
	}
	hex := strings.HasPrefix(body, "0x") || strings.HasPrefix(body, "0X")
	for _, r := range body {
		switch {
		case r >= '0' && r <= '9', r == '.':
		case hex && (r == 'x' || r == 'X' || r == 'p' || r == 'P' || r == '+' || r == '-' || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')):
		case !hex && (r == 'e' || r == 'E' || r == '+' || r == '-'):
		default:
			return 0, false
		}
	}
	if hex && !strings.ContainsAny(body, "pP") {
		return 0, false // Java requires the binary exponent
	}
	f, err := strconv.ParseFloat(sign+body, 32)
	if err != nil {
		if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
			return float32(f), true // overflow to infinity, underflow to zero, as Java
		}
		return 0, false
	}
	return float32(f), true
}

// floatToLong is Java's (long) narrowing of a float: NaN is 0, saturating.
func floatToLong(f float32) int64 {
	switch {
	case f != f:
		return 0
	case float64(f) >= 9.223372036854775807e18:
		return math.MaxInt64
	case float64(f) <= -9.223372036854775808e18:
		return math.MinInt64
	}
	return int64(f)
}

// saturatingMul is TimeUnit's toMillis: saturating at the long range.
func saturatingMul(v, by int64) int64 {
	if v > math.MaxInt64/by {
		return math.MaxInt64
	}
	if v < math.MinInt64/by {
		return math.MinInt64
	}
	return v * by
}

// javaSplit is String.split(regex) for a single literal separator: trailing
// empty strings are removed.
func javaSplit(s, sep string) []string {
	if !strings.Contains(s, sep) {
		return []string{s} // no match: the input itself, even when empty
	}
	parts := strings.Split(s, sep)
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// ---- DurationParser.inMillis

func durationMillis(s string) (int64, bool) {
	parts := javaSplit(javaTrim(s), ":")
	switch len(parts) {
	case 1:
		return secondsMillis(parts[0])
	case 2:
		return hmsMillis("0", parts[0], parts[1])
	case 3:
		return hmsMillis(parts[0], parts[1], parts[2])
	}
	return 0, false
}

func hmsMillis(h, m, s string) (int64, bool) {
	hv, ok := javaParseLong(h)
	if !ok {
		return 0, false
	}
	mv, ok := javaParseLong(m)
	if !ok {
		return 0, false
	}
	sv, ok := secondsMillis(s)
	if !ok {
		return 0, false
	}
	return saturatingMul(hv, 3_600_000) + saturatingMul(mv, 60_000) + sv, true
}

func secondsMillis(s string) (int64, bool) {
	if strings.Contains(s, ".") {
		v, ok := javaParseFloat(s)
		if !ok {
			return 0, false
		}
		frac := float32(math.Mod(float64(v), 1)) // float % 1
		return saturatingMul(floatToLong(v), 1000) + floatToLong(frac*1000), true
	}
	v, ok := javaParseLong(s)
	if !ok {
		return 0, false
	}
	return saturatingMul(v, 1000), true
}

// ---- DateUtils.parseTimeString ([HH:]MM:SS[.mmm]; a single value gives 0)

func parseTimeString(s string) (int64, bool) {
	parts := javaSplit(s, ":")
	var result int64
	idx := 0
	if len(parts) == 3 {
		h, ok := javaParseInt(parts[idx])
		if !ok {
			return 0, false
		}
		result += h * 3_600_000
		idx++
	}
	if len(parts) >= 2 {
		m, ok := javaParseInt(parts[idx])
		if !ok {
			return 0, false
		}
		result += m * 60_000
		idx++
		sec, ok := javaParseFloat(parts[idx])
		if !ok {
			return 0, false
		}
		result += floatToLong(sec * 1000)
	}
	return result, true
}

// ---- MimeTypeUtils

const octetStream = "application/octet-stream"

var (
	audioExtensions = map[string]bool{"3gp": true, "aac": true, "amr": true, "flac": true, "imy": true, "m4a": true, "m4b": true,
		"mid": true, "mkv": true, "mp3": true, "mp4": true, "mxmf": true, "oga": true, "ogg": true, "ogx": true, "opus": true,
		"ota": true, "rtttl": true, "rtx": true, "wav": true, "xmf": true}
	videoExtensions = map[string]bool{"3gp": true, "mkv": true, "mp4": true, "ogg": true, "ogv": true, "ogx": true, "webm": true}
)

func isMediaFile(t *string) bool {
	if t == nil {
		return false
	}
	return strings.HasPrefix(*t, "audio/") || strings.HasPrefix(*t, "video/") || *t == "application/ogg" || *t == octetStream
}

func isImageFile(t *string) bool { return t != nil && strings.HasPrefix(*t, "image/") }

// mimeType is MimeTypeUtils.getMimeType.
func mimeType(t, filename *string) *string {
	if isMediaFile(t) && *t != octetStream {
		return t
	}
	if ft := mimeTypeFromURL(filename); isMediaFile(ft) {
		return ft
	}
	return t
}

func mimeTypeFromURL(url *string) *string {
	if url == nil {
		return nil
	}
	ext := extension(*url)
	if m := mimeTypeFromExtension(ext); m != nil {
		return m
	}
	switch {
	case audioExtensions[ext]:
		return ptr("audio/*")
	case videoExtensions[ext]:
		return ptr("video/*")
	}
	return nil
}

// mimeTypeFromExtension is MimeTypeMap.getMimeTypeFromExtension (Android's
// MimeMap: the extension lower-cased).
func mimeTypeFromExtension(ext string) *string {
	if m, ok := androidMIMEMap[strings.ToLower(ext)]; ok && ext != "" {
		return &m
	}
	return nil
}

// extension is commons-io FilenameUtils.getExtension.
func extension(name string) string {
	dot := strings.LastIndexByte(name, '.')
	sep := max(strings.LastIndexByte(name, '/'), strings.LastIndexByte(name, '\\'))
	if dot < 0 || sep > dot {
		return ""
	}
	return name[dot+1:]
}

// javaLowerUS is String.toLowerCase(Locale.US).
func javaLowerUS(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "İ", "i̇"))
}
