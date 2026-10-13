// Package vectorgen produces the shared contract-v1 test vectors (docs/CONTRACT.md)
// from the Go implementation: a fixed, checked-in set and reproducible randomized
// sets for differential checks against independent implementations.
package vectorgen

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// Suite is one vector file.
type Suite struct {
	Name  string
	Cases []map[string]any
}

// Encode returns the file bytes: indented JSON with sorted keys and a final newline.
func (s Suite) Encode() ([]byte, error) {
	cases := make([]any, len(s.Cases))
	for i, c := range s.Cases {
		cases[i] = c
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(map[string]any{"suite": s.Name, "cases": cases}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

const (
	namespaceA = "6f1c2d3e-4a5b-4c6d-8e7f-90a1b2c3d4e5"
	namespaceB = "0a0b0c0d-0e0f-4011-8213-141516171819"
	accountA   = "11111111-2222-4333-8444-555555555555"
	opA        = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	opB        = "01234567-89ab-4cde-8f01-23456789abcd"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// Fixed returns the checked-in vector suites.
func Fixed() []Suite {
	r := rand.New(rand.NewPCG(1, 2))
	return []Suite{
		canonicalFixed(),
		urlSuite(urlFixedInputs()),
		guidSuite(guidFixedInputs()),
		lengthPrefixedFixed(),
		idsFixed(),
		queueParseFixed(),
		queueCompareFixed(),
		queueBetweenFixed(r),
		causalFixed(),
		timeFixed(),
		contentHashFixed(),
		macFixed(),
		digestFixed(),
		cursorFixed(),
		partsFixed(),
	}
}

// Random returns n randomized cases spread over the suites where random inputs
// exercise the most logic, reproducible from seed.
func Random(seed uint64, n int) []Suite {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	share := func(pct int) int { return n * pct / 100 }
	urls := make([]string, share(25))
	for i := range urls {
		urls[i] = randomURL(r)
	}
	guids := make([]string, share(5))
	for i := range guids {
		guids[i] = randomGUID(r)
	}
	return []Suite{
		urlSuite(urls),
		guidSuite(guids),
		canonicalRandom(r, share(15)),
		idsRandom(r, share(15)),
		lengthPrefixedRandom(r, share(5)),
		queueBetweenRandom(r, share(25)),
		causalRandom(r, share(5)),
		timeRandom(r, n-share(25)-share(5)-share(15)-share(15)-share(5)-share(25)-share(5)),
	}
}

// ---- canonical JSON

func canonicalCase(input []byte) map[string]any {
	c := map[string]any{}
	if !utf8.Valid(input) {
		c["input_hex"] = hex.EncodeToString(input)
	} else {
		c["input"] = string(input)
	}
	v, err := contract.DecodeJSON(input)
	if err == nil {
		var out []byte
		out, err = contract.Canonical(v)
		if err == nil {
			c["output"] = string(out)
			return c
		}
	}
	c["error"] = true
	return c
}

func canonicalFixed() Suite {
	inputs := []string{
		`{"b":1,"a":[true,null,"x"]}`,
		`{"€":1,"😀":2,"z":3,"":0}`,
		`{"\ud83d\ude00":1,"\ufb33":2}`,
		` { "a" : [ 1 , 2 ] , "b" : { } } `,
		`"a\u0001\b\"\\\/ \u007f\u00e9\u2028"`,
		`"\u0000\u001f\u0020"`,
		`9007199254740991`, `-9007199254740991`, `0`, `-5`, `[]`, `{}`, `null`, `true`, `false`,
		`[[[[[[[[[[1]]]]]]]]]]`,
		`{"a":{"b":{"c":[{"d":"e"}]}}}`,
		// rejected
		`1.5`, `1e3`, `1E3`, `-0`, `9007199254740992`, `-9007199254740992`, `01`, `+1`, `.5`,
		`{"a":1} x`, `{"a":1,"a":2}`, `{"a":1,"b":{"c":1,"c":2}}`,
		`"\ud800"`, `"\udc00"`, `"\udc00\ud800"`, `"\ud800\u0041"`, "\"a\x01\"", "\"\t\"",
		`"\x"`, `"\u12"`, `[1,]`, `{"a":1,}`, `{a:1}`, `tru`, `nul`, ``, ` `, `"abc`, `[`, `{"a"}`,
		`'a'`, `NaN`, `Infinity`, `1.0`, `-1.5e2`, "\ufeff{}", `"\uFFFF"`, `{"a":"\uD83D\uDE00"}`,
		strings.Repeat("[", contract.MaxDepth) + strings.Repeat("]", contract.MaxDepth),
		strings.Repeat("[", contract.MaxDepth+1) + strings.Repeat("]", contract.MaxDepth+1),
	}
	var cases []map[string]any
	for _, in := range inputs {
		cases = append(cases, canonicalCase([]byte(in)))
	}
	for _, raw := range [][]byte{{'"', 0xff, '"'}, {'"', 0xc3, '"'}, {'"', 0xed, 0xa0, 0x80, '"'}} {
		cases = append(cases, canonicalCase(raw))
	}
	return Suite{"canonical_json", cases}
}

func randomValue(r *rand.Rand, depth int) any {
	k := r.IntN(8)
	if depth > 4 {
		k = r.IntN(5)
	}
	switch k {
	case 0:
		return nil
	case 1:
		return r.IntN(2) == 0
	case 2:
		n := r.Int64N(2*contract.MaxSafeInteger+1) - contract.MaxSafeInteger
		if r.IntN(2) == 0 {
			n = r.Int64N(2001) - 1000
		}
		return n
	case 3, 4:
		return randomString(r, 12)
	case 5, 6:
		m := map[string]any{}
		for i := r.IntN(5); i > 0; i-- {
			m[randomString(r, 6)] = randomValue(r, depth+1)
		}
		return m
	default:
		a := []any{}
		for i := r.IntN(5); i > 0; i-- {
			a = append(a, randomValue(r, depth+1))
		}
		return a
	}
}

var stringAlphabet = []rune("aAzZ09 _-\"\\/\b\f\n\r\t\x00\x01\x1f\x7fé€\u2028\U0001F600\uFB33\u00a0")

func randomString(r *rand.Rand, max int) string {
	var b strings.Builder
	for i := r.IntN(max + 1); i > 0; i-- {
		b.WriteRune(stringAlphabet[r.IntN(len(stringAlphabet))])
	}
	return b.String()
}

// writeLoose serializes v as valid but non-canonical JSON: random member order,
// random whitespace and random (valid) escapes.
func writeLoose(r *rand.Rand, b *strings.Builder, v any) {
	ws := func() {
		for i := r.IntN(3); i > 0; i-- {
			b.WriteByte(" \t\n\r"[r.IntN(4)])
		}
	}
	ws()
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case int64:
		b.WriteString(strconv.FormatInt(x, 10))
	case string:
		writeLooseString(r, b, x)
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			ws()
			writeLooseString(r, b, k)
			ws()
			b.WriteByte(':')
			writeLoose(r, b, x[k])
		}
		ws()
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			writeLoose(r, b, e)
		}
		ws()
		b.WriteByte(']')
	}
	ws()
}

func writeLooseString(r *rand.Rand, b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, c := range s {
		switch {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteRune(c)
		case c < 0x20 || r.IntN(4) == 0:
			if c > 0xffff {
				hi, lo := surrogates(c)
				fmt.Fprintf(b, `\u%04X\u%04x`, hi, lo)
			} else if r.IntN(2) == 0 {
				fmt.Fprintf(b, `\u%04x`, c)
			} else {
				fmt.Fprintf(b, `\u%04X`, c)
			}
		case c == '/' && r.IntN(2) == 0:
			b.WriteString(`\/`)
		default:
			b.WriteRune(c)
		}
	}
	b.WriteByte('"')
}

func surrogates(c rune) (rune, rune) {
	c -= 0x10000
	return 0xd800 + (c>>10)&0x3ff, 0xdc00 + c&0x3ff
}

func canonicalRandom(r *rand.Rand, n int) Suite {
	cases := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		var b strings.Builder
		writeLoose(r, &b, randomValue(r, 0))
		in := b.String()
		if r.IntN(10) == 0 { // corrupt it
			pos := r.IntN(len(in) + 1)
			in = in[:pos] + []string{",", "}", "\"", "\\u", "1.0", "\x01", `{"a":1,"a":2}`}[r.IntN(7)] + in[pos:]
		}
		cases = append(cases, canonicalCase([]byte(in)))
	}
	return Suite{"canonical_json", cases}
}

// ---- URL and GUID

func urlFixedInputs() []string {
	return []string{
		"HTTPS://Example.COM/Feed.xml", "http://example.com:80/a", "https://example.com:443/a",
		"http://example.com:443/a", "https://example.com:8443/a", "https://example.com:/a",
		"https://example.com:0443/a", "https://User:Pa@ss@Host.Example/x", "http://[2001:DB8::1]:80/x",
		"http://[2001:DB8::1]:8080/x", "https://example.com/a?b=1#frag?x", "https://example.com#top",
		"https://example.com/a/%7e/../b/?Q=%2F&q=1", "https://Exämple.COM/x", "feed://Example.com/rss",
		"itpc:Example.com/rss#x", "example.com/feed.xml", "", "https://example.com", "https://example.com?x",
		"https://%4A.EXAMPLE/audio", "https://%4a.EXAMPLE/%4A", "https://A%4.EXAMPLE/x", "https://A%.B/x",
		"https://[2001:DB8::1]:443/audio", "https://[2001:DB8::1]:/audio", "https://[2001:DB8::1]/audio",
		"https://[::1/x", "https://[::A]x:80/x", "mailto:Someone@Example.com#x", "HTTP://EXAMPLE.COM:80",
		"http://example.com:80", "http://example.com:80?q#f", "https://EXAMPLE.com:443#f",
		"https://a@b@C.com:443/", "https://[::1]:443", "HtTp://Ex.Com/Path/With/CAPS/", "https:///path",
		"https://", "https:", "1http://Example.com", "h-t.t+p://Example.COM:80/x", "http:Example.COM",
		"http://example.com/%E2%82%ac?x=%e2%82%ac#%20", "  https://Example.com/ ", "https://ex ample.COM/a b",
		"http://h:80:80/", "http://H::/", "http://h:/", "Example.COM:80/a#x", "https://%%4A.COM/", "https://u:p@[::1]:443/",
	}
}

func urlSuite(inputs []string) Suite {
	cases := make([]map[string]any, len(inputs))
	for i, in := range inputs {
		cases[i] = map[string]any{"input": in, "output": contract.NormalizeURL(in)}
	}
	return Suite{"url", cases}
}

func pick(r *rand.Rand, options ...string) string { return options[r.IntN(len(options))] }

func randomCase(r *rand.Rand, s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' && r.IntN(3) == 0 {
			b[i] = c - 32
		}
	}
	return string(b)
}

func randomChars(r *rand.Rand, alphabet string, max int) string {
	var b strings.Builder
	runes := []rune(alphabet)
	for i := r.IntN(max + 1); i > 0; i-- {
		b.WriteRune(runes[r.IntN(len(runes))])
	}
	return b.String()
}

func randomURL(r *rand.Rand) string {
	if r.IntN(20) == 0 {
		return randomChars(r, "aZ:/?#@[]%4a.-+19 é", 30)
	}
	var b strings.Builder
	if r.IntN(15) != 0 {
		b.WriteString(randomCase(r, pick(r, "http", "https", "feed", "itpc", "ftp", "x-y.z+1", "pcast")))
		b.WriteByte(':')
	}
	if r.IntN(8) != 0 {
		b.WriteString("//")
		if r.IntN(5) == 0 {
			b.WriteString(randomChars(r, "uS:@p%4", 6) + "@")
		}
		switch r.IntN(6) {
		case 0:
			b.WriteString("[" + randomChars(r, "0123456789abcdefABCDEF:", 12))
			if r.IntN(6) != 0 {
				b.WriteString("]")
			}
			if r.IntN(4) == 0 {
				b.WriteString(randomChars(r, "x:", 2))
			}
		default:
			b.WriteString(randomCase(r, randomChars(r, pick(r, "abcxyz.-0%4AéÄ", "ab:8%0"), 12)))
		}
		if r.IntN(3) == 0 {
			b.WriteString(":" + pick(r, "80", "443", "", "8080", "080", "0443", "x", "8443"))
		}
	}
	for i := r.IntN(4); i > 0; i-- {
		b.WriteString("/" + randomChars(r, "aB%2Fe~.:@ ", 6))
	}
	if r.IntN(3) == 0 {
		b.WriteString("?" + randomChars(r, "a=&B%2f:/?@", 8))
	}
	if r.IntN(3) == 0 {
		b.WriteString("#" + randomChars(r, "a?#/B:", 6))
	}
	return b.String()
}

func guidFixedInputs() []string {
	return []string{"  abc  ", "\t\n\x00abc def\x1f ", "\u00a0abc\u00a0", "null", " null ", "NULL", "Null",
		"   ", "", "ABC", "urn:uuid:1234", " a\u2003", "\x7fabc", "\x20\x21"}
}

func randomGUID(r *rand.Rand) string {
	return randomChars(r, " \t\n\x00\x1f\x20\x7f\u00a0\u2003anulNUL-é😀", 8)
}

func guidSuite(inputs []string) Suite {
	cases := make([]map[string]any, len(inputs))
	for i, in := range inputs {
		out, ok := contract.TrimGUID(in)
		c := map[string]any{"input": in, "output": nil}
		if ok {
			c["output"] = out
		}
		cases[i] = c
	}
	return Suite{"guid", cases}
}

// ---- identities

func lpCase(fields []string) map[string]any {
	anyFields := make([]any, len(fields))
	for i, f := range fields {
		anyFields[i] = f
	}
	return map[string]any{"fields": anyFields, "output": hex.EncodeToString(contract.LengthPrefixed(fields...))}
}

func lengthPrefixedFixed() Suite {
	return Suite{"length_prefixed", []map[string]any{
		lpCase([]string{}), lpCase([]string{""}), lpCase([]string{"", ""}), lpCase([]string{"ab", "c"}),
		lpCase([]string{"a", "bc"}), lpCase([]string{"é", "😀"}), lpCase([]string{strings.Repeat("x", 300)}),
	}}
}

func lengthPrefixedRandom(r *rand.Rand, n int) Suite {
	cases := make([]map[string]any, n)
	for i := range cases {
		fields := make([]string, r.IntN(5))
		for j := range fields {
			fields[j] = randomString(r, 10)
		}
		cases[i] = lpCase(fields)
	}
	return Suite{"length_prefixed", cases}
}

func randomUUID(r *rand.Rand) string {
	var b [16]byte
	for i := range b {
		b[i] = byte(r.UintN(256))
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func episodeCase(ns, p, k, v string) map[string]any {
	c := map[string]any{"kind": "episode", "namespace": ns, "podcast_id": p, "k": k, "v": v}
	if id, err := contract.EpisodeID(ns, p, k, v); err == nil {
		c["output"] = id
	} else {
		c["error"] = true
	}
	return c
}

func idsFixed() Suite {
	pA := contract.PodcastID(namespaceA, opA)
	pB := contract.PodcastID(namespaceB, opA)
	enc := contract.NormalizeURL("HTTPS://Cdn.Example.COM:443/ep1.mp3#t=10")
	return Suite{"ids", []map[string]any{
		{"kind": "podcast", "namespace": namespaceA, "op_id": opA, "output": pA},
		{"kind": "podcast", "namespace": namespaceB, "op_id": opA, "output": pB},
		{"kind": "podcast", "namespace": namespaceA, "op_id": opB, "output": contract.PodcastID(namespaceA, opB)},
		{"kind": "generation", "namespace": namespaceA, "podcast_id": pA, "op_id": opA, "output": contract.GenerationID(namespaceA, pA, opA)},
		{"kind": "generation", "namespace": namespaceA, "podcast_id": pA, "op_id": opB, "output": contract.GenerationID(namespaceA, pA, opB)},
		episodeCase(namespaceA, pA, contract.KindRSSGUID, "https://example.com/?p=123"),
		episodeCase(namespaceA, pA, contract.KindAtomID, "https://example.com/?p=123"),
		episodeCase(namespaceA, pB, contract.KindRSSGUID, "https://example.com/?p=123"),
		episodeCase(namespaceA, pA, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue("ep-1", enc)),
		episodeCase(namespaceA, pA, contract.KindEnclosure, enc),
		episodeCase(namespaceA, pA, contract.KindRSSGUID, "Épisode 😀 \u0065\u0301"),
		episodeCase(namespaceA, pA, contract.KindRSSGUID, "Épisode 😀 \u00e9"),
		episodeCase(namespaceA, pA, contract.KindRSSGUID, ""),
		episodeCase(namespaceA, pA, "guid", "x"),
		episodeCase(namespaceA, pA, "RSS-GUID", "x"),
	}}
}

func idsRandom(r *rand.Rand, n int) Suite {
	kinds := []string{contract.KindRSSGUID, contract.KindAtomID, contract.KindGUIDAndEnclosure, contract.KindEnclosure}
	cases := make([]map[string]any, n)
	for i := range cases {
		ns, op := randomUUID(r), randomUUID(r)
		switch r.IntN(3) {
		case 0:
			cases[i] = map[string]any{"kind": "podcast", "namespace": ns, "op_id": op, "output": contract.PodcastID(ns, op)}
		case 1:
			p := contract.PodcastID(ns, randomUUID(r))
			cases[i] = map[string]any{"kind": "generation", "namespace": ns, "podcast_id": p, "op_id": op, "output": contract.GenerationID(ns, p, op)}
		default:
			p := contract.PodcastID(ns, op)
			k := kinds[r.IntN(len(kinds))]
			v := randomString(r, 20)
			if k == contract.KindGUIDAndEnclosure {
				v = contract.GUIDAndEnclosureValue(randomString(r, 8), contract.NormalizeURL(randomURL(r)))
			} else if k == contract.KindEnclosure {
				v = contract.NormalizeURL(randomURL(r))
			}
			cases[i] = episodeCase(ns, p, k, v)
		}
	}
	return Suite{"ids", cases}
}

// ---- queue keys

func queueParseFixed() Suite {
	valid := []string{"q1:1:1", "q1:2:1", "q1:2:3", "q1:3:7", "q1:131:1", "q1:200:" + strings.Repeat("f", 50), "q1:1048576:1"}
	invalid := []string{"", "q1:1", "q2:1:1", "q1:01:1", "q1:1:01", "q1:1:2", "q1:1:A", "q1:0:1", "q1:1:3",
		"q1:-1:1", "q1:1:1:1", "Q1:1:1", "q1: 1:1", "q1:1:0", "q1:2:5", "q1:+1:1", "q1:1:-1", "q1:1:", "q1::1",
		"q1:4294967296:1", "q1:3:0x1", "q1:1048577:1"}
	var cases []map[string]any
	for _, s := range valid {
		if _, err := contract.ParseQueueKey(s); err != nil {
			panic("valid key rejected: " + s)
		}
		cases = append(cases, map[string]any{"input": s, "valid": true})
	}
	for _, s := range invalid {
		_, err := contract.ParseQueueKey(s)
		cases = append(cases, map[string]any{"input": s, "valid": err == nil})
	}
	return Suite{"queue_parse", cases}
}

func queueCompareFixed() Suite {
	pairs := [][2]string{{"q1:1:1", "q1:1:1"}, {"q1:1:1", "q1:2:3"}, {"q1:2:1", "q1:1:1"}, {"q1:3:5", "q1:2:3"},
		{"q1:130:1", "q1:131:3"}, {"q1:200:" + strings.Repeat("f", 50), "q1:1:1"}, {"q1:9:ff", "q1:8:7f"}}
	var cases []map[string]any
	for _, p := range pairs {
		a, b := must(contract.ParseQueueKey(p[0])), must(contract.ParseQueueKey(p[1]))
		cases = append(cases, map[string]any{"a": p[0], "b": p[1], "output": int64(contract.CompareQueueKeys(a, b))})
	}
	return Suite{"queue_compare", cases}
}

func randomU(r *rand.Rand) [16]byte {
	var u [16]byte
	for i := range u {
		u[i] = byte(r.UintN(256))
	}
	return u
}

func keyString(k *contract.QueueKey) any {
	if k == nil {
		return nil
	}
	return k.String()
}

func betweenCase(low, high *contract.QueueKey, u [16]byte) (map[string]any, *contract.QueueKey) {
	c := map[string]any{"low": keyString(low), "high": keyString(high), "u": hex.EncodeToString(u[:])}
	k, err := contract.QueueKeyBetween(low, high, u)
	if err != nil {
		c["error"] = true
		return c, nil
	}
	c["output"] = k.String()
	return c, &k
}

func queueBetweenFixed(r *rand.Rand) Suite {
	var cases []map[string]any
	add := func(low, high *contract.QueueKey, u [16]byte) *contract.QueueKey {
		c, k := betweenCase(low, high, u)
		cases = append(cases, c)
		return k
	}
	var zero, ones [16]byte
	for i := range ones {
		ones[i] = 0xff
	}
	first := add(nil, nil, zero)
	add(nil, nil, ones)
	add(nil, nil, zero) // equal U, equal key
	// front and back, 50 each
	front, back := first, first
	for i := 0; i < 50; i++ {
		front = add(nil, front, randomU(r))
		back = add(back, nil, randomU(r))
	}
	// 100 insertions into the smallest gap
	low := first
	high := add(low, nil, randomU(r))
	for i := 0; i < 100; i++ {
		high = add(low, high, randomU(r))
	}
	// exponent bound: exactly at the bound, above it after the exponent loop, and
	// above it with the computed starting exponent already sufficient
	for _, high := range []string{"q1:1048445:1", "q1:1048446:1", "q1:1048576:3", "q1:1048576:1"} {
		k := must(contract.ParseQueueKey(high))
		add(nil, &k, zero)
	}
	// extremes and errors
	add(first, first, zero)
	add(back, front, zero)
	add(nil, front, ones)
	add(back, nil, ones)
	return Suite{"queue_between", cases}
}

func queueBetweenRandom(r *rand.Rand, n int) Suite {
	var cases []map[string]any
	for len(cases) < n {
		keys := []*contract.QueueKey{nil} // nil at index 0 = sentinel 0; implicit sentinel 1 at the end
		for i := 0; i < 200 && len(cases) < n; i++ {
			pos := r.IntN(len(keys)) // insert between keys[pos] and keys[pos+1] (or 1)
			if r.IntN(3) == 0 {
				pos = len(keys) - 1 // back
			} else if r.IntN(3) == 0 {
				pos = 0 // front
			}
			var high *contract.QueueKey
			if pos+1 < len(keys) {
				high = keys[pos+1]
			}
			c, k := betweenCase(keys[pos], high, randomU(r))
			cases = append(cases, c)
			if k == nil {
				panic("allocation failed")
			}
			keys = append(keys[:pos+1], append([]*contract.QueueKey{k}, keys[pos+1:]...)...)
		}
	}
	return Suite{"queue_between", cases}
}

// ---- causal order and time

func causalCase(nodes []contract.OrderedFact) map[string]any {
	list := make([]any, len(nodes))
	for i, n := range nodes {
		deps := make([]any, len(n.Dependencies))
		for j, d := range n.Dependencies {
			deps[j] = d
		}
		list[i] = map[string]any{"id": n.ID, "dependencies": deps, "normalized_time": n.NormalizedTime}
	}
	c := map[string]any{"nodes": list}
	order, err := contract.CausalOrder(append([]contract.OrderedFact(nil), nodes...))
	switch {
	case err == nil:
		out := make([]any, len(order))
		for i, id := range order {
			out[i] = id
		}
		c["output"] = out
	case errors.Is(err, contract.ErrDependencyMissing):
		c["error"] = "dependency_missing"
	case errors.Is(err, contract.ErrDependencyCycle):
		c["error"] = "cycle"
	case errors.Is(err, contract.ErrDuplicateFact):
		c["error"] = "duplicate"
	default:
		panic(err)
	}
	return c
}

type node = contract.OrderedFact

func causalFixed() Suite {
	// fact -> decision -> fact with a backward server clock: the decision and the
	// dependent fact take the dependency lower bound (time normalization).
	f1 := int64(1_700_000_010_000)
	dec := contract.NormalizeTime(1_700_000_005_000, 1_700_000_005_000, []int64{f1})
	f2 := contract.NormalizeTime(1_700_000_001_000, 1_700_000_003_000, []int64{dec})
	cases := []map[string]any{
		causalCase(nil),
		causalCase([]node{{ID: "a", NormalizedTime: 5}}),
		causalCase([]node{{ID: "a", NormalizedTime: 10}, {ID: "b", NormalizedTime: 1}, {ID: "c", Dependencies: []string{"a"}, NormalizedTime: 5}}),
		causalCase([]node{{ID: "x", Dependencies: []string{"z"}, NormalizedTime: 1}, {ID: "y", NormalizedTime: 2}, {ID: "z", NormalizedTime: 3}}),
		causalCase([]node{{ID: "b", NormalizedTime: 7}, {ID: "a", NormalizedTime: 7}, {ID: "B", NormalizedTime: 7}, {ID: "é", NormalizedTime: 7}, {ID: "z", NormalizedTime: 7}}),
		causalCase([]node{
			{ID: "11111111-0000-4000-8000-000000000001", NormalizedTime: f1},
			{ID: "dddddddd-0000-4000-8000-000000000002", Dependencies: []string{"11111111-0000-4000-8000-000000000001"}, NormalizedTime: dec},
			{ID: "00000000-0000-4000-8000-000000000003", Dependencies: []string{"dddddddd-0000-4000-8000-000000000002"}, NormalizedTime: f2},
			{ID: "00000000-0000-4000-8000-000000000004", NormalizedTime: f1 - 1},
		}),
		// pairwise "earlier time first unless dependent" is not transitive here
		causalCase([]node{{ID: "p", NormalizedTime: 3}, {ID: "q", Dependencies: []string{"r"}, NormalizedTime: 1}, {ID: "r", NormalizedTime: 5}}),
		causalCase([]node{{ID: "x", Dependencies: []string{"missing"}}}),
		causalCase([]node{{ID: "x", Dependencies: []string{"y"}}, {ID: "y", Dependencies: []string{"x"}}}),
		causalCase([]node{{ID: "x", Dependencies: []string{"x"}}}),
		causalCase([]node{{ID: "x"}, {ID: "x"}}),
		causalCase([]node{{ID: "a", Dependencies: []string{"a"}}, {ID: "b", Dependencies: []string{"missing"}}}),
		causalCase([]node{{ID: "a", Dependencies: []string{"zz"}}, {ID: "a"}}),
		causalCase([]node{{ID: "a", Dependencies: []string{"b", "b"}, NormalizedTime: 1}, {ID: "b", NormalizedTime: 2}}),
	}
	return Suite{"causal_order", cases}
}

func causalRandom(r *rand.Rand, n int) Suite {
	cases := make([]map[string]any, n)
	for i := range cases {
		count := 1 + r.IntN(25)
		nodes := make([]node, count)
		for j := range nodes {
			id := randomUUID(r)
			if r.IntN(2) == 0 {
				id = randomChars(r, "abAB09é", 3) + strconv.Itoa(j)
			}
			nodes[j] = node{ID: id, NormalizedTime: r.Int64N(6)}
			seen := map[string]bool{}
			for d := r.IntN(min(j, 4) + 1); d > 0 && j > 0; d-- {
				if dep := nodes[r.IntN(j)].ID; !seen[dep] {
					seen[dep] = true
					nodes[j].Dependencies = append(nodes[j].Dependencies, dep)
				}
			}
		}
		switch r.IntN(20) {
		case 0:
			nodes[0].Dependencies = append(nodes[0].Dependencies, "missing")
		case 1:
			nodes[0].Dependencies = append(nodes[0].Dependencies, nodes[count-1].ID)
		}
		r.Shuffle(count, func(a, b int) { nodes[a], nodes[b] = nodes[b], nodes[a] })
		cases[i] = causalCase(nodes)
	}
	return Suite{"causal_order", cases}
}

func timeCase(client, receive int64, deps []int64) map[string]any {
	list := make([]any, len(deps))
	for i, d := range deps {
		list[i] = d
	}
	return map[string]any{"client_time": client, "receive_time": receive, "dependency_times": list,
		"output": contract.NormalizeTime(client, receive, deps)}
}

func timeFixed() Suite {
	return Suite{"time_normalization", []map[string]any{
		timeCase(100, 200, nil), timeCase(300, 200, nil), timeCase(100, 200, []int64{150}),
		timeCase(100, 200, []int64{150, 120}), timeCase(100, 200, []int64{250}), timeCase(300, 200, []int64{250}),
		timeCase(-5, 200, nil), timeCase(200, 200, []int64{200}), timeCase(0, 0, []int64{}),
		timeCase(1_700_000_005_000, 1_700_000_005_000, []int64{1_700_000_010_000}),
	}}
}

func timeRandom(r *rand.Rand, n int) Suite {
	cases := make([]map[string]any, n)
	base := int64(1_700_000_000_000)
	for i := range cases {
		deps := make([]int64, r.IntN(4))
		for j := range deps {
			deps[j] = base + r.Int64N(2000) - 1000
		}
		cases[i] = timeCase(base+r.Int64N(4000)-2000, base+r.Int64N(2000)-1000, deps)
	}
	return Suite{"time_normalization", cases}
}

// ---- hashes, MACs, digests, cursors, parts

func sampleFact(op string, seq int64, deps []any) map[string]any {
	return map[string]any{
		"op_id": op, "author_id": "22222222-3333-4444-8555-666666666666", "author_seq": seq,
		"dependencies": deps, "type": "episode.mark_played", "type_version": int64(1),
		"client_time": int64(1_700_000_000_123),
		"targets":     []any{map[string]any{"entity": "episode", "id": "e1:" + strings.Repeat("ab", 32), "generation": "g1:" + strings.Repeat("cd", 32)}},
		"payload":     map[string]any{"reset_position": true, "title": "Épisode \"1\"\n😀"},
	}
}

func contentHashFixed() Suite {
	facts := []map[string]any{
		sampleFact(opA, 1, []any{}),
		sampleFact(opB, 2, []any{opA}),
		{"op_id": opA, "payload": map[string]any{}},
	}
	f := sampleFact(opB, 3, []any{opA})
	f["previous_op_id"] = opA
	facts = append(facts, f)
	var cases []map[string]any
	for _, fact := range facts {
		cases = append(cases, map[string]any{"fact": fact, "output": must(contract.ContentHash(fact))})
	}
	return Suite{"content_hash", cases}
}

func sampleReceipt() map[string]any {
	return map[string]any{"op_id": opA, "content_hash": must(contract.ContentHash(sampleFact(opA, 1, []any{}))),
		"outcome": "applied", "normalized_time": int64(1_700_000_000_123), "epoch": int64(1), "seq": int64(42), "key_id": "k1"}
}

// sampleLayout is a queue.layout decision (PROTOCOL §7.6):
// every in-member in the replay's order with its key.
func sampleLayout(epoch, seq int64) map[string]any {
	return map[string]any{"decision_id": opB, "decision_type": "queue.layout", "decision_version": int64(1),
		"dependencies": []any{opA}, "normalized_time": int64(1_700_000_010_000), "epoch": epoch, "seq": seq, "index": int64(1),
		"subject": "queue", "inputs": map[string]any{"frontier": []any{opA}, "mapping_version": int64(0),
			"policy": map[string]any{"keep_sorted": false, "sort_order": "date_new_old"}},
		"result": map[string]any{"members": []any{map[string]any{"episode_id": "e1:" + strings.Repeat("ab", 32), "generation_id": "g1:" + strings.Repeat("cd", 32),
			"key": "q1:1:1", "position_id": "00000000-0000-4000-8000-000000000000"}}}, "key_id": "k1"}
}

func macFixed() Suite {
	key := bytes.Repeat([]byte{0x0b}, 32)
	decision := sampleLayout(1, 43)
	rejected := sampleReceipt()
	rejected["outcome"], rejected["reason"], rejected["frontier"] = "rejected", "invalid_payload", []any{opB}
	// A queue placement's receipt records the server's sort values
	// (PROTOCOL §7.6).
	valued := sampleReceipt()
	valued["sort_values"] = []any{map[string]any{"episode_id": "e1:" + strings.Repeat("ab", 32), "generation_id": "g1:" + strings.Repeat("cd", 32),
		"title": "Épisode \"1\"", "podcast_id": "p1:" + strings.Repeat("ef", 32), "podcast_title": "Show", "publication_time": int64(1_699_999_000_000),
		"duration": int64(3_600_000)}}
	var cases []map[string]any
	for _, obj := range []map[string]any{sampleReceipt(), rejected, valued, decision} {
		for _, account := range []string{accountA, namespaceB} {
			cases = append(cases, map[string]any{"key": hex.EncodeToString(key), "account_id": account, "object": obj,
				"output": must(contract.SignedMAC(key, account, obj))})
		}
	}
	return Suite{"mac", cases}
}

func digestFixed() Suite {
	receipt := sampleReceipt()
	receipt["mac"] = must(contract.SignedMAC(bytes.Repeat([]byte{0x0b}, 32), accountA, receipt))
	commits := []map[string]any{
		{"epoch": int64(1), "seq": int64(1), "accepted": []any{map[string]any{"fact": sampleFact(opA, 1, []any{}), "receipt": receipt}},
			"decisions": []any{}, "slots": []any{map[string]any{"install_id": namespaceB, "install_seq": int64(1), "op_id": opA, "content_hash": must(contract.ContentHash(sampleFact(opA, 1, []any{}))), "result": "accepted"}},
			"records": []any{map[string]any{"entity": "episode", "id": "e1:" + strings.Repeat("ab", 32), "field_group": "play_state",
				"value": map[string]any{"play_state": "played"}, "provenance": []any{opA}}}},
		{"epoch": int64(1), "seq": int64(2), "accepted": []any{}, "decisions": []any{},
			"slots": []any{map[string]any{"install_id": namespaceB, "install_seq": int64(2), "op_id": opA, "content_hash": must(contract.ContentHash(sampleFact(opA, 1, []any{}))), "result": "duplicate"}}, "records": []any{}},
		{"epoch": int64(2), "seq": int64(3), "accepted": []any{}, "decisions": []any{}, "slots": []any{},
			"records": []any{map[string]any{"entity": "episode", "id": "e1:" + strings.Repeat("ab", 32), "field_group": "favorite", "tombstone": true, "provenance": []any{opB}}}},
	}
	decision := sampleLayout(2, 4)
	decision["mac"] = must(contract.SignedMAC(bytes.Repeat([]byte{0x0b}, 32), accountA, decision))
	commits = append(commits, map[string]any{"epoch": int64(2), "seq": int64(4), "accepted": []any{}, "decisions": []any{decision},
		"slots": []any{}, "records": []any{map[string]any{"entity": "queue", "id": "e1:" + strings.Repeat("ab", 32), "field_group": "queue_position",
			"value": map[string]any{"key": "q1:1:1"}, "provenance": []any{opB}}}})
	var cases []map[string]any
	prev := contract.GenesisDigest
	for _, c := range commits {
		next := must(contract.NextDigest(prev, c))
		cases = append(cases, map[string]any{"previous": hex.EncodeToString(prev), "object": c, "output": hex.EncodeToString(next)})
		prev = next
	}
	// Snapshot items chain from 32 zero bytes with the same rule (PROTOCOL §3).
	prev = contract.GenesisDigest
	for _, item := range []map[string]any{
		{"accepted": map[string]any{"fact": sampleFact(opA, 1, []any{}), "receipt": receipt}},
		{"decision": decision},
		{"record": map[string]any{"entity": "episode", "id": "e1:" + strings.Repeat("ab", 32), "field_group": "play_state",
			"value": map[string]any{"play_state": "played"}, "provenance": []any{opA}}},
	} {
		next := must(contract.NextDigest(prev, item))
		cases = append(cases, map[string]any{"previous": hex.EncodeToString(prev), "object": item, "output": hex.EncodeToString(next)})
		prev = next
	}
	return Suite{"digest_chain", cases}
}

func cursorFixed() Suite {
	var cases []map[string]any
	for _, c := range []contract.Cursor{
		{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 1, Seq: 0, Digest: make([]byte, 32)},
		{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 2, Seq: 41, Digest: bytes.Repeat([]byte{0xab}, 32)},
		{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 7, Seq: contract.MaxSafeInteger, Digest: bytes.Repeat([]byte{0x01}, 32)},
	} {
		enc := must(c.Encode())
		cases = append(cases, map[string]any{"cursor": map[string]any{"log_id": c.LogID, "epoch": c.Epoch, "seq": c.Seq,
			"digest": hex.EncodeToString(c.Digest)}, "encoded": enc})
	}
	bad := []string{"", "!!", "e30", // {}
		b64(`{"digest":"` + strings.Repeat("00", 32) + `","epoch":1,"log_id":"6a1f0e2c-1111-4222-8333-944455556666","seq":-1}`),
		b64(`{"digest":"` + strings.Repeat("00", 31) + `","epoch":1,"log_id":"6a1f0e2c-1111-4222-8333-944455556666","seq":1}`),
		b64(`{"epoch":1,"digest":"` + strings.Repeat("00", 32) + `","log_id":"6a1f0e2c-1111-4222-8333-944455556666","seq":1}`),
		b64(` {"digest":"` + strings.Repeat("00", 32) + `","epoch":1,"log_id":"6a1f0e2c-1111-4222-8333-944455556666","seq":1}`),
		b64(`{"digest":"` + strings.Repeat("00", 32) + `","epoch":1,"log_id":"6a1f0e2c-1111-4222-8333-944455556666","seq":1,"z":1}`),
		b64(`{"digest":"` + strings.Repeat("AB", 32) + `","epoch":1,"log_id":"6a1f0e2c-1111-4222-8333-944455556666","seq":1}`),
		must(contract.Cursor{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 1, Seq: 1, Digest: make([]byte, 32)}.Encode()) + "=",
		b64(`{"digest":"` + strings.Repeat("00", 32) + `","epoch":1,"log_id":"6A1F0E2C-1111-4222-8333-944455556666","seq":1}`),
		b64(`{"digest":"` + strings.Repeat("00", 32) + `","epoch":1,"log_id":"x","seq":1}`),
		insertAt(must(contract.Cursor{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 1, Seq: 1, Digest: make([]byte, 32)}.Encode()), 10, "\n"),
		insertAt(must(contract.Cursor{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 1, Seq: 1, Digest: make([]byte, 32)}.Encode()), 10, "\r\n"),
		flipTrailingBit(must(contract.Cursor{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 1, Seq: 1, Digest: make([]byte, 32)}.Encode())),
	}
	for _, s := range bad {
		if _, err := contract.DecodeCursor(s); err == nil {
			panic("bad cursor accepted: " + s)
		}
		cases = append(cases, map[string]any{"encoded": s, "error": true})
	}
	return Suite{"cursor", cases}
}

func b64(s string) string {
	return strings.TrimRight(strings.NewReplacer("+", "-", "/", "_").Replace(encodeStd([]byte(s))), "=")
}

func partsFixed() Suite {
	fact := sampleFact(opA, 1, []any{})
	hash := must(contract.ContentHash(fact))
	var cases []map[string]any
	for _, size := range []int{1000000, 64, 7, 1} {
		parts := must(contract.SplitCanonical(fact, size))
		cases = append(cases, partsCase(parts, hash, fact))
	}
	parts := must(contract.SplitCanonical(fact, 64))
	other := must(contract.ContentHash(sampleFact(opB, 1, []any{})))
	cases = append(cases,
		partsCase(parts, other, nil),                                             // wrong hash
		partsCase(parts[1:], hash, nil),                                          // missing part
		partsCase(append([]string{parts[1], parts[0]}, parts[2:]...), hash, nil), // reordered
		partsCase([]string{b64(` {"op_id":"x"}`)}, must(contract.ContentHash(map[string]any{"op_id": "x"})), nil),                 // not canonical
		partsCase([]string{b64(`["x"]`)}, hash, nil),                                                                              // not an object
		partsCase([]string{parts[0] + "="}, hash, nil),                                                                            // padding
		partsCase(append([]string{""}, parts...), hash, nil),                                                                      // empty part
		partsCase(append([]string{insertAt(parts[0], 4, "\r")}, parts[1:]...), hash, nil),                                         // CR inside a part
		partsCase(append([]string{parts[0] + "\n"}, parts[1:]...), hash, nil),                                                     // LF after a part
		partsCase([]string{flipTrailingBit(b64(`{"op_id":"x"}`))}, must(contract.ContentHash(map[string]any{"op_id": "x"})), nil), // non-zero trailing bits
	)
	return Suite{"parts", cases}
}

func partsCase(parts []string, hash string, want map[string]any) map[string]any {
	list := make([]any, len(parts))
	for i, p := range parts {
		list[i] = p
	}
	c := map[string]any{"parts": list, "content_hash": hash}
	fact, err := contract.ReassembleFact(parts, hash)
	if err != nil {
		if want != nil {
			panic(err)
		}
		c["error"] = true
		return c
	}
	if want == nil {
		panic("invalid parts accepted")
	}
	c["fact"] = fact
	return c
}

func encodeStd(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// flipTrailingBit returns s with its last character's unused low bit set, so it
// decodes to the same bytes under a lenient decoder but is not strict base64url.
func flipTrailingBit(s string) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	if len(s)%4 == 0 {
		panic("no unused bits")
	}
	i := strings.IndexByte(alphabet, s[len(s)-1])
	return s[:len(s)-1] + string(alphabet[i|1])
}

func insertAt(s string, i int, insert string) string { return s[:i] + insert + s[i:] }
