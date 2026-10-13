package feed

import (
	"context"
	"html"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// What Android reads from an HTML page with jsoup 1.15.1: TypeGetter's website
// title, and FeedDiscoverer's feed links. Both look only at document.head(), so
// this follows jsoup's tokeniser and tree builder until the body starts.

// websiteError is TypeGetter.throwExceptionIfWebsite: a title in the head names
// the website; otherwise the root is always html ("Server returned html").
func websiteError(ctx context.Context, text string) *ParseError {
	if h := jsoupParseHead(ctx, text, nil); h.title != nil {
		return &ParseError{Label: "unsupported_type_html", Detail: "Website title: \"" + *h.title + "\""}
	}
	return &ParseError{Label: "unsupported_type_html", Detail: "Server returned html"}
}

// jsoupHead is what document.head() holds that Android reads: the text() of
// its first title. Its link elements go to jsoupParseHead's link function.
type jsoupHead struct {
	title *string
}

// jsoup's tree-construction modes before the body (HtmlTreeBuilderState).
const (
	jsoupBeforeHead     = iota // Initial, BeforeHtml and BeforeHead alike
	jsoupInHead                //
	jsoupInHeadNoscript        // jsoup keeps anything unexpected in it as text
	jsoupAfterHead             //
)

// jsoupParseHead follows jsoup's tree builder until the body starts, keeping
// what it puts in the head: title and link start tags in the head modes (after
// the head too, where jsoup moves them into it), a head noscript's links, and
// everything inside a template in the head. Each link's attributes go to
// link, if any, in document order as they are read; the scan stops when link
// returns false.
//
// It stops early when ctx is done, between tokens and within one (a tag or a
// text can be as long as the page: jsoupWork); the caller checks ctx.
func jsoupParseHead(ctx context.Context, s string, link func(attrs map[string]string) bool) jsoupHead {
	var h jsoupHead
	s = strings.TrimPrefix(s, "\xef\xbb\xbf")
	mode, templates := jsoupBeforeHead, 0
	w := &jsoupWork{ctx: ctx}
	for i, n := 0, 0; i < len(s); n++ {
		if n%checkEvery == 0 && ctx.Err() != nil || w.stopped {
			return h
		}
		if s[i] != '<' {
			j := strings.IndexByte(s[i:], '<')
			if j < 0 {
				j = len(s) - i
			}
			if w.done(j) {
				return h
			}
			if templates == 0 && mode != jsoupInHeadNoscript && !jsoupBlank(jsoupDecode(s[i:i+j], 0, false, w), w) && !w.stopped {
				return h // text starts the body
			}
			i += j
			continue
		}
		switch {
		case strings.HasPrefix(s[i+1:], "!"):
			i = jsoupSkipMarkup(s, i, w)
		case strings.HasPrefix(s[i+1:], "?"):
			i = jsoupSkipPast(s, i, ">", w) // a bogus comment
		case strings.HasPrefix(s[i+1:], "/"):
			switch {
			case jsoupLetterAt(s, i+2):
				tag, ok := jsoupReadTag(s, i+2, w)
				if !ok {
					return h
				}
				i = tag.end
				switch name := tag.name; {
				case name == "template" && templates > 0:
					templates--
				case templates > 0:
				case mode == jsoupInHeadNoscript:
					if name == "noscript" {
						mode = jsoupInHead
					}
				case name == "head" && mode != jsoupAfterHead:
					mode = jsoupAfterHead
				case name == "body" || name == "html" || name == "br":
					return h // anything else: it starts the body
				}
			case i+2 >= len(s):
				return h // "</" at the end is text
			case s[i+2] == '>':
				i += 3
			default:
				i = jsoupSkipPast(s, i, ">", w) // a bogus comment
			}
		case jsoupLetterAt(s, i+1):
			tag, ok := jsoupReadTag(s, i+1, w)
			if !ok {
				return h // a tag cut off by the end of input emits nothing
			}
			i = tag.end
			if mode == jsoupInHeadNoscript && templates == 0 {
				switch tag.name {
				case "link":
					if link != nil && !link(tag.attrs) {
						return h
					}
				case "noframes", "style":
					i = jsoupSkipRawText(s, i, tag.name, w)
				}
				continue // the head's own elements, or text
			}
			switch tag.name {
			case "html":
			case "head":
				if mode == jsoupBeforeHead {
					mode = jsoupInHead
				}
			case "title":
				if h.title == nil {
					// text(): jsoup preserves a title's whitespace, then trims.
					t := javaTrim(jsoupDecode(s[i:jsoupEndTag(s, i, "title", w)], 0, false, w))
					if w.stopped {
						return h
					}
					h.title = &t
				}
				i = jsoupSkipRawText(s, i, "title", w)
				mode = max(mode, jsoupInHead)
			case "template":
				templates++
				mode = max(mode, jsoupInHead)
			case "link":
				if link != nil && !link(tag.attrs) {
					return h
				}
				mode = max(mode, jsoupInHead)
			case "base", "basefont", "bgsound", "command", "meta":
				mode = max(mode, jsoupInHead)
			case "noframes", "style", "script":
				i = jsoupSkipRawText(s, i, tag.name, w)
				mode = max(mode, jsoupInHead)
			case "noscript":
				if templates > 0 {
					continue
				}
				if mode == jsoupAfterHead {
					return h // only the head's own noscript; after it, the body starts
				}
				mode = jsoupInHeadNoscript
			default:
				if templates == 0 {
					return h // body, frameset or any other element starts the body
				}
			}
		default:
			if templates == 0 && mode != jsoupInHeadNoscript {
				return h // a lone "<" is text
			}
			i++
		}
	}
	return h
}

// jsoupTag is a tag token: its lower-cased name, its attributes (names
// lower-cased, the first of duplicates kept, values decoded) and the index
// after it.
type jsoupTag struct {
	name  string
	attrs map[string]string
	end   int
}

const jsoupSpace = "\t\n\f\r "

// jsoupWork counts a head scan's work in bytes and checks ctx every
// jsoupCheckBytes of them. Once ctx is done the scan stops: every helper
// returns early with stopped set, and nothing it returns is used.
type jsoupWork struct {
	ctx     context.Context
	n       int
	stopped bool
}

const jsoupCheckBytes = 1 << 16

// done counts bytes of work and reports whether the scan must stop.
func (w *jsoupWork) done(bytes int) bool {
	if w.n += bytes; w.n >= jsoupCheckBytes && !w.stopped {
		w.n = 0
		w.stopped = w.ctx.Err() != nil
	}
	return w.stopped
}

// jsoupReadTag reads a tag from its name on, as jsoup's tokeniser (TagName and
// the attribute states); ok is false when the input ends inside it, or when
// w stops the scan.
func jsoupReadTag(s string, i int, w *jsoupWork) (jsoupTag, bool) {
	t := jsoupTag{attrs: map[string]string{}}
	j := i
	for j < len(s) && !strings.ContainsRune(jsoupSpace+"/><", rune(s[j])) {
		if w.done(1) {
			return t, false
		}
		j++
	}
	t.name = strings.ToLower(s[i:j])
	if j < len(s) && s[j] == '<' {
		t.end = j // jsoup ends the tag at a "<" and rereads it
		return t, true
	}
	add := func(name, value string) {
		if name = strings.ToLower(name); name != "" {
			if _, dup := t.attrs[name]; !dup {
				t.attrs[name] = value
			}
		}
	}
	for j < len(s) {
		if w.done(1) {
			return t, false
		}
		switch c := s[j]; {
		case strings.IndexByte(jsoupSpace, c) >= 0 || c == '/':
			j++
		case c == '>':
			t.end = j + 1
			return t, true
		default:
			// An attribute name: up to whitespace, "/", "=" or ">" (a leading
			// quote, "<" or "=" belongs to it).
			k := j + 1
			for k < len(s) && !strings.ContainsRune(jsoupSpace+"/=>", rune(s[k])) {
				k++
			}
			name := s[j:k]
			for k < len(s) && strings.IndexByte(jsoupSpace, s[k]) >= 0 {
				k++
			}
			if w.done(k - j) {
				return t, false
			}
			if k >= len(s) || s[k] != '=' {
				add(name, "")
				j = k
				continue
			}
			k++
			v := k
			for k < len(s) && strings.IndexByte(jsoupSpace, s[k]) >= 0 {
				k++
			}
			if w.done(k-v) || k >= len(s) {
				return t, false
			}
			switch s[k] {
			case '"', '\'':
				e := strings.IndexByte(s[k+1:], s[k])
				if e < 0 || w.done(e) {
					return t, false
				}
				add(name, jsoupDecode(s[k+1:k+1+e], s[k], true, w))
				j = k + e + 2
			case '>':
				add(name, "") // "=" then ">": an empty value, and the tag ends
				t.end = k + 1
				return t, true
			default:
				e := k
				for e < len(s) && !strings.ContainsRune(jsoupSpace+">", rune(s[e])) {
					e++
				}
				if w.done(e - k) {
					return t, false
				}
				add(name, jsoupDecode(s[k:e], '>', true, w))
				j = e
			}
			if w.stopped {
				return t, false
			}
		}
	}
	return t, false
}

func jsoupLetterAt(s string, i int) bool {
	return i < len(s) && (s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z')
}

// jsoupSkipMarkup skips "<!": a comment, a doctype or a bogus comment.
func jsoupSkipMarkup(s string, i int, w *jsoupWork) int {
	rest := s[i+2:]
	if !strings.HasPrefix(rest, "--") {
		return jsoupSkipPast(s, i, ">", w)
	}
	body := rest[2:]
	switch {
	case strings.HasPrefix(body, ">"):
		return i + 5
	case strings.HasPrefix(body, "->"):
		return i + 6
	}
	// A comment ends at "--" followed by ">" or "!>", found in one pass.
	for p := 0; ; {
		k := strings.Index(body[p:], "--")
		if k < 0 || w.done(k+1) {
			return len(s)
		}
		q := p + k + 2
		switch {
		case q < len(body) && body[q] == '>':
			return i + 4 + q + 1
		case q+1 < len(body) && body[q] == '!' && body[q+1] == '>':
			return i + 4 + q + 2
		}
		p += k + 1
	}
}

func jsoupSkipPast(s string, i int, closer string, w *jsoupWork) int {
	if k := strings.Index(s[i:], closer); k >= 0 && !w.done(k) {
		return i + k + len(closer)
	}
	return len(s)
}

// jsoupSkipRawText skips a raw-text or RCDATA element's content and end tag.
func jsoupSkipRawText(s string, i int, name string, w *jsoupWork) int {
	end := jsoupEndTag(s, i, name, w)
	if end == len(s) {
		return end
	}
	if tag, ok := jsoupReadTag(s, end+2, w); ok {
		return tag.end
	}
	return len(s)
}

// jsoupEndTag finds the element's appropriate end tag: "</name" (ASCII
// letters in any case) then whitespace, "/" or ">".
func jsoupEndTag(s string, i int, name string, w *jsoupWork) int {
	for {
		k := strings.Index(s[i:], "</")
		if k < 0 || w.done(k+2) {
			return len(s)
		}
		j := i + k + 2
		if j+len(name) <= len(s) && asciiEqualFold(s[j:j+len(name)], name) &&
			(j+len(name) == len(s) || strings.ContainsRune(jsoupSpace+"/>", rune(s[j+len(name)]))) {
			return i + k
		}
		i = j
	}
}

// asciiEqualFold compares ASCII case-insensitively; other bytes must match.
func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if x >= 'A' && x <= 'Z' {
			x += 'a' - 'A'
		}
		if y >= 'A' && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}

// jsoupBlank is StringUtil.isBlank: only HTML whitespace.
func jsoupBlank(s string, w *jsoupWork) bool {
	for _, r := range s {
		if w.done(1) {
			return true
		}
		if !strings.ContainsRune(jsoupSpace, r) {
			return false
		}
	}
	return true
}

// jsoupWin1252 is Tokeniser.win1252Extensions: numeric references 0x80–0x9F.
// jsoupLongestEntity is one more than the longest name in the HTML5 entity
// table without its semicolon: CounterClockwiseContourIntegral (31 bytes).
// A longer name decodes, at best, a prefix and keeps the rest, which neither
// a legacy entity (decoding the whole name to one character) nor a named one
// (ending without the semicolon) does.
const jsoupLongestEntity = 32

var jsoupWin1252 = [...]rune{
	0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
	0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
}

// jsoupDecode decodes character references as jsoup's tokeniser does
// (Tokeniser.consumeCharacterReference). In an attribute value, allowed is its
// closing quote (or ">" when unquoted), which is never a reference; a null
// becomes U+FFFD outside text.
func jsoupDecode(s string, allowed byte, inAttribute bool, w *jsoupWork) string {
	if !strings.ContainsAny(s, "&\x00") || w.done(len(s)) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if w.done(1) {
			return b.String()
		}
		c := s[i]
		switch {
		case c == 0:
			b.WriteRune(utf8.RuneError)
			i++
		case c != '&':
			b.WriteByte(c)
			i++
		default:
			rest := s[i+1:]
			if rest != "" && allowed != 0 && rest[0] == allowed {
				b.WriteByte('&')
				i++
				continue
			}
			decoded, n := jsoupReference(rest, inAttribute, w)
			if n == 0 {
				b.WriteByte('&')
				i++
				continue
			}
			b.WriteString(decoded)
			i += 1 + n
		}
	}
	return b.String()
}

// jsoupReference decodes the reference after an "&", returning the text and
// how many bytes it consumed (0: no reference, the "&" is literal).
func jsoupReference(s string, inAttribute bool, w *jsoupWork) (string, int) {
	if s == "" || strings.ContainsRune(jsoupSpace+"<&", rune(s[0])) {
		return "", 0
	}
	if s[0] == '#' {
		j, hex := 1, false
		if j < len(s) && (s[j] == 'x' || s[j] == 'X') {
			j, hex = 2, true
		}
		k := j
		for k < len(s) && (s[k] >= '0' && s[k] <= '9' || hex && (s[k] >= 'a' && s[k] <= 'f' || s[k] >= 'A' && s[k] <= 'F')) {
			k++
		}
		if k == j || w.done(k) {
			return "", 0
		}
		base := 10
		if hex {
			base = 16
		}
		v, err := strconv.ParseInt(s[j:k], base, 32)
		if k < len(s) && s[k] == ';' {
			k++
		}
		switch {
		case err != nil || v >= 0xD800 && v <= 0xDFFF || v > 0x10FFFF:
			return string(utf8.RuneError), k
		case v >= 0x80 && v < 0x80+int64(len(jsoupWin1252)):
			v = int64(jsoupWin1252[v-0x80])
		}
		return string(rune(v)), k
	}
	// A letter-then-digit sequence names it: a base entity with or without the
	// semicolon, any other entity only with it.
	k := 0
	for k < len(s) {
		r, n := utf8.DecodeRuneInString(s[k:])
		if r > 0xFFFF || !unicode.IsLetter(r) { // Character.isLetter(char): BMP only
			break
		}
		k += n
	}
	for k < len(s) && s[k] >= '0' && s[k] <= '9' {
		k++
	}
	if w.done(k) {
		return "", 0
	}
	name := s[:k]
	semicolon := k < len(s) && s[k] == ';'
	if name == "" {
		return "", 0
	}
	v, ok := jsoupEntity(name, semicolon)
	if !ok || inAttribute && !semicolon && k < len(s) && strings.IndexByte("=-_", s[k]) >= 0 {
		return "", 0
	}
	if semicolon {
		k++
	}
	return v, k
}

// jsoupEntity looks a name up in the HTML5 entity table (Go's html package has
// the same one): with the semicolon any entity, without it only the legacy
// (jsoup "base") entities. No name of jsoupLongestEntity bytes or more is one
// (TestJsoupEntityLongNames), and such a name is not handed to the table's
// scan, which is as long as the name.
func jsoupEntity(name string, semicolon bool) (string, bool) {
	if len(name) >= jsoupLongestEntity {
		return "", false
	}
	if v := html.UnescapeString("&" + name); v != "&"+name && utf8.RuneCountInString(v) == 1 {
		return v, true // a legacy entity matched whole
	}
	if !semicolon {
		return "", false
	}
	in := "&" + name + ";"
	if v := html.UnescapeString(in); v != in && (!strings.HasSuffix(v, ";") || v == ";") {
		return v, true
	}
	return "", false
}
