package feed

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"math/bits"
	"math/rand/v2"
	"strconv"
	"strings"
)

// Namespace URIs and their canonical prefixes (SyndHandler.startPrefixMapping).
const (
	nsAtom          = "http://www.w3.org/2005/Atom"
	nsContent       = "http://purl.org/rss/1.0/modules/content/"
	nsItunes        = "http://www.itunes.com/dtds/podcast-1.0.dtd"
	nsChapters      = "http://podlove.org/simple-chapters"
	nsMedia         = "http://search.yahoo.com/mrss/"
	nsDublinCore    = "http://purl.org/dc/elements/1.1/"
	nsPodcastIndex  = "https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md"
	nsPodcastIndex2 = "https://podcastindex.org/namespace/1.0"
	nsXML           = "http://www.w3.org/XML/1998/namespace"
	nsXMLNS         = "http://www.w3.org/2000/xmlns/"
)

// Parsing budgets (PROTOCOL §9.2; Android has none): a feed over one fails like a
// parser exception and the previous catalog stays.
const (
	maxEntityText = 16 << 20 // expanded entity text in total
	maxText       = 64 << 20 // decoded text and attribute values in total
	maxItems      = 100_000
	maxDepth      = 256
	checkEvery    = 1024 // tokens between cancellation checks

	// Expat's billion-laughs protection (Android has it): past the activation
	// threshold, input plus expansions may be at most this factor of the input.
	expatAmplification = 100
	expatActivation    = 8 << 20
)

var errBudget = errors.New("feed: over a parsing budget")

// attributes are SAX Attributes, looked up by qualified name.
type attributes map[string]string

func (a attributes) get(name string) *string {
	if v, ok := a[name]; ok {
		return &v
	}
	return nil
}

// syndElement is SyndElement (with AtomText's type and content).
type syndElement struct {
	name     string
	ns       namespace
	atomType *string // AtomText
	atom     bool
	content  *string
}

// namespace is a namespace handler (parser/feed/namespace).
type namespace interface {
	start(local string, st *handlerState, attrs attributes) *syndElement
	end(local string, st *handlerState)
}

// handlerState is HandlerState.
type handlerState struct {
	feed             *Feed
	items            []*Item
	current          *Item
	currentFunding   *Funding
	tagstack         []*syndElement
	pushed           []bool               // per open element: whether its start pushed onto tagstack
	namespaces       map[string]namespace // by URI
	defaults         []namespace
	contentBuf       *strings.Builder // nil is Java's null
	duration         *int32           // tempObjects[Itunes.DURATION]
	alternateURLs    map[string]string
	alternateNull    *string // the null key's title (see Feed.AlternateNull)
	hasAlternateNull bool
	redirectURL      *string
	authors          *strings.Builder // feed.Author as Atom's names built it (addFeedAuthor)
	authorsOf        *string          // the feed.Author authors built
	now              int64
	ctx              context.Context // the parse\'s, for long value conversions
}

func newHandlerState(kind string, now int64) *handlerState {
	st := &handlerState{feed: &Feed{}, namespaces: map[string]namespace{}, alternateURLs: map[string]string{}, now: now}
	if kind == "rss20" || kind == "rss091" {
		st.defaults = append(st.defaults, rss20{})
	}
	return st
}

// top is tagstack.peek(): Java throws on an empty stack.
func (st *handlerState) top() *syndElement {
	if len(st.tagstack) == 0 {
		panic(errStack)
	}
	return st.tagstack[len(st.tagstack)-1]
}

// secondTag is getSecondTag (the element under the top).
func (st *handlerState) secondTag() *syndElement {
	if len(st.tagstack) < 2 {
		panic(errStack)
	}
	return st.tagstack[len(st.tagstack)-2]
}

func (st *handlerState) thirdTag() *syndElement {
	if len(st.tagstack) < 3 {
		panic(errStack)
	}
	return st.tagstack[len(st.tagstack)-3]
}

// content is getContentBuf().toString(), or nil for a null buffer.
func (st *handlerState) content() *string {
	if st.contentBuf == nil {
		return nil
	}
	s := st.contentBuf.String()
	return &s
}

func (st *handlerState) addItem() {
	it := &Item{}
	st.current = it
	st.items = append(st.items, it)
	if len(st.items) > maxItems {
		panic(errBudget)
	}
}

// errStack and errNull are the Java exceptions a handler can throw (an empty
// stack, a null dereference); Android's parse fails.
var (
	errStack = errors.New("feed: empty stack")
	errNull  = errors.New("feed: null dereference")
)

// ---- SyndHandler

func (st *handlerState) startPrefixMapping(prefix, uri string) {
	if _, known := st.namespaces[uri]; known {
		return
	}
	switch {
	case uri == nsAtom:
		if prefix == "" {
			st.defaults = append(st.defaults, atom{})
		} else if prefix == "atom" {
			st.namespaces[uri] = atom{}
		}
	case uri == nsContent && prefix == "content":
		st.namespaces[uri] = content{}
	case uri == nsItunes && prefix == "itunes":
		st.namespaces[uri] = itunes{}
	case uri == nsChapters && (prefix == "psc" || prefix == "sc"):
		st.namespaces[uri] = simpleChapters{}
	case uri == nsMedia && prefix == "media":
		st.namespaces[uri] = media{}
	case uri == nsDublinCore && prefix == "dc":
		st.namespaces[uri] = dublinCore{}
	case uri == nsPodcastIndex || (uri == nsPodcastIndex2 && prefix == "podcast"):
		// Java precedence: NSURI || (NSURI2 && prefix == "podcast").
		st.namespaces[uri] = podcastIndex{}
	}
}

func (st *handlerState) endPrefixMapping(prefix string) {
	if len(st.defaults) > 1 && prefix == "" {
		st.defaults = st.defaults[:len(st.defaults)-1]
	}
}

func (st *handlerState) handling(uri, qName string) namespace {
	if h := st.namespaces[uri]; h != nil {
		return h
	}
	if len(st.defaults) > 0 && !strings.Contains(qName, ":") {
		return st.defaults[len(st.defaults)-1]
	}
	return nil
}

func (st *handlerState) startElement(uri, local, qName string, attrs attributes) {
	st.contentBuf = &strings.Builder{}
	h := st.handling(uri, qName)
	if h != nil {
		st.tagstack = append(st.tagstack, h.start(local, st, attrs))
	}
	st.pushed = append(st.pushed, h != nil)
}

func (st *handlerState) characters(s string) {
	if len(st.tagstack) >= 2 && st.contentBuf != nil {
		st.contentBuf.WriteString(s)
	}
}

// endElement chooses the handler again, as Android does. A namespace that a
// child declared can make an element handled at only one of its ends; its
// start's push and its end's pop would then no longer pair up (Android pops
// another element's entry, or an empty stack), so that is a parse error.
func (st *handlerState) endElement(uri, local, qName string) {
	h := st.handling(uri, qName)
	pushed := len(st.pushed) > 0 && st.pushed[len(st.pushed)-1]
	if len(st.pushed) > 0 {
		st.pushed = st.pushed[:len(st.pushed)-1]
	}
	if (h != nil) != pushed {
		panic(errStack)
	}
	if h != nil {
		h.end(local, st)
		st.tagstack = st.tagstack[:len(st.tagstack)-1]
	}
	st.contentBuf = nil
}

// ---- the XML reader (Expat with namespace processing, as Android's SAX parser)

type scope struct {
	prefixes map[string]string // declared on this element
	order    []string
}

// binding is a prefix's namespace: its URI and the URI's number (saxParse).
type binding struct {
	uri string
	id  int
}

// expandedName is an attribute's namespace, by number, and local name.
type expandedName struct {
	ns    int
	local string
}

func saxParse(ctx context.Context, doc string, st *handlerState) (err error) {
	st.ctx = ctx
	defer func() {
		if r := recover(); r != nil {
			if c, ok := r.(parseCanceled); ok {
				err = c.err
				return
			}
			if e, ok := r.(error); ok && (errors.Is(e, errStack) || errors.Is(e, errNull) || errors.Is(e, errBudget)) {
				err = &ParseError{Label: "parser_exception"}
				return
			}
			panic(r)
		}
	}()
	// Declared general entities are substituted into the text first, so their
	// replacement text is parsed as content, as Expat does; every use counts
	// against the budgets before anything is built.
	doc, err = expandDocument(ctx, doc)
	if err != nil {
		return err
	}
	d := xml.NewDecoder(&ctxReader{ctx: ctx, r: strings.NewReader(doc)})
	d.Strict = true
	d.CharsetReader = func(_ string, r io.Reader) (io.Reader, error) { return r, nil } // already decoded
	d.Entity = map[string]string{}
	var raw []xml.Name // the open elements, for well-formedness
	var scopes []scope // namespace declarations per open element
	// Each namespace URI is numbered by value when it is declared, where its
	// bytes are read anyway; attributes' expanded names compare the numbers
	// (0 for no namespace). Comparing the URIs instead hashed a long one once
	// per attribute: quadratic time.
	ids := map[string]int{"": 0, nsXML: 1}
	bound := map[string][]binding{"xml": {{nsXML, 1}}}
	resolve := func(prefix string) (binding, bool) {
		if prefix == "xmlns" {
			return binding{}, false
		}
		if v := bound[prefix]; len(v) > 0 {
			return v[len(v)-1], true
		}
		return binding{}, prefix == ""
	}
	rooted, closed := false, false
	decoded := 0 // text and attribute bytes so far
	// step counts the work within one token, an attribute or a namespace
	// binding at a time, and checks ctx every checkEvery: one start tag can
	// hold millions of attributes, which the checks between tokens and while
	// reading (ctxReader) do not split.
	steps := 0
	step := func() error {
		if steps++; steps%checkEvery != 0 {
			return nil
		}
		return ctx.Err()
	}
	for n := 0; ; n++ {
		if n%checkEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		tok, err := d.RawToken()
		if err == io.EOF {
			break
		}
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr // ctxReader stopped the token
			}
			return &ParseError{Label: "parser_exception"}
		}
		switch t := tok.(type) {
		case xml.Directive:
			if rooted {
				return &ParseError{Label: "parser_exception"}
			}
		case xml.StartElement:
			if closed {
				return &ParseError{Label: "parser_exception"} // junk after the document element
			}
			rooted = true
			if len(raw) >= maxDepth {
				return &ParseError{Label: "parser_exception"}
			}
			sc := scope{prefixes: map[string]string{}}
			seen := map[string]bool{}
			for _, a := range t.Attr {
				if err := step(); err != nil {
					return err
				}
				q := qname(a.Name)
				if seen[q] {
					return &ParseError{Label: "parser_exception"} // duplicate attribute
				}
				seen[q] = true
				switch {
				case a.Name.Space == "xmlns":
					// Expat's namespace constraints: xml is bound only to its
					// namespace and nothing else is; xmlns is never declared;
					// a prefix cannot be undeclared.
					p, v := a.Name.Local, a.Value
					if p == "xmlns" || (p == "xml") != (v == nsXML) || v == nsXMLNS || v == "" {
						return &ParseError{Label: "parser_exception"}
					}
					sc.prefixes[p] = v
					sc.order = append(sc.order, p)
				case a.Name.Space == "" && a.Name.Local == "xmlns":
					if a.Value == nsXML || a.Value == nsXMLNS {
						return &ParseError{Label: "parser_exception"}
					}
					sc.prefixes[""] = a.Value
					sc.order = append(sc.order, "")
				}
			}
			for _, p := range sc.order {
				if err := step(); err != nil {
					return err
				}
				uri := sc.prefixes[p]
				id, ok := ids[uri]
				if !ok {
					id = len(ids)
					ids[uri] = id
				}
				bound[p] = append(bound[p], binding{uri, id})
				st.startPrefixMapping(p, uri)
			}
			el, ok := resolve(t.Name.Space)
			if !ok {
				return &ParseError{Label: "parser_exception"} // unbound prefix
			}
			attrs := attributes{}
			expanded := map[expandedName]bool{} // (namespace, local): unique, as in Expat
			for _, a := range t.Attr {
				if err := step(); err != nil {
					return err
				}
				if a.Name.Space == "xmlns" || (a.Name.Space == "" && a.Name.Local == "xmlns") {
					continue
				}
				ns := 0
				if a.Name.Space != "" {
					b, ok := resolve(a.Name.Space)
					if !ok {
						return &ParseError{Label: "parser_exception"}
					}
					ns = b.id
				}
				if k := (expandedName{ns, a.Name.Local}); expanded[k] {
					return &ParseError{Label: "parser_exception"}
				} else {
					expanded[k] = true
				}
				attrs[qname(a.Name)] = a.Value
				decoded += len(a.Value)
			}
			if decoded > maxText {
				return &ParseError{Label: "parser_exception"}
			}
			raw = append(raw, t.Name)
			scopes = append(scopes, sc)
			st.startElement(el.uri, t.Name.Local, qname(t.Name), attrs)
		case xml.EndElement:
			if len(raw) == 0 || raw[len(raw)-1] != t.Name {
				return &ParseError{Label: "parser_exception"} // mismatched tag
			}
			el, _ := resolve(t.Name.Space)
			st.endElement(el.uri, t.Name.Local, qname(t.Name))
			sc := scopes[len(scopes)-1]
			raw, scopes = raw[:len(raw)-1], scopes[:len(scopes)-1]
			for i := len(sc.order) - 1; i >= 0; i-- {
				if err := step(); err != nil {
					return err
				}
				p := sc.order[i]
				bound[p] = bound[p][:len(bound[p])-1]
				st.endPrefixMapping(p)
			}
			if len(raw) == 0 {
				closed = true
			}
		case xml.CharData:
			if len(raw) == 0 {
				if strings.TrimSpace(string(t)) != "" {
					return &ParseError{Label: "parser_exception"}
				}
				continue
			}
			if decoded += len(t); decoded > maxText {
				return &ParseError{Label: "parser_exception"}
			}
			st.characters(string(t))
		}
	}
	if !rooted || len(raw) > 0 {
		return &ParseError{Label: "parser_exception"}
	}
	return nil
}

// ctxReader is a document for encoding/xml that checks ctx every
// ctxReadEvery bytes read: the decoder reads a token (a text of many
// megabytes) in one call that the parse's checks between tokens cannot split.
// It is no io.ByteReader, so the decoder buffers it and reads it in chunks.
type ctxReader struct {
	ctx context.Context
	r   *strings.Reader
	n   int
}

const ctxReadEvery = 1 << 16

func (c *ctxReader) Read(p []byte) (int, error) {
	if c.n += len(p); c.n >= ctxReadEvery {
		c.n = 0
		if err := c.ctx.Err(); err != nil {
			return 0, err
		}
	}
	return c.r.Read(p)
}

func qname(n xml.Name) string {
	if n.Space == "" {
		return n.Local
	}
	return n.Space + ":" + n.Local
}

// expandDocument reads the prolog's internal-subset entity declarations and,
// if there are any, returns the document with every use of a declared entity
// after the prolog replaced by its replacement text, recursively, as Expat
// parses it at use: character references are replaced when a declaration is
// read; the predefined entities stay for the decoder. Uses in comments, CDATA
// sections and processing instructions are not uses. Each use counts, in
// document order and before its text is built, against Expat's amplification
// limit and the server's own budget. Undefined, recursive or malformed
// references are malformed, but only when used.
func expandDocument(ctx context.Context, doc string) (string, error) {
	w := &entityWork{ctx: ctx}
	declared, root, err := prologEntities(doc, w)
	if err != nil || len(declared) == 0 {
		return doc, err
	}
	x := &entityExpander{lits: map[string]string{}, lens: map[string]int{}, vals: map[string]string{}, declared: declared, w: w}
	for name, raw := range declared {
		// Character references are replaced when the declaration is read: a
		// malformed one is malformed whether or not the entity is used.
		lit, err := replaceReferences(raw, func(string) (string, bool, error) { return "", false, nil }, w)
		if err != nil {
			return "", err
		}
		x.lits[name] = lit
	}
	// A '&' starts a use only when the next ';' ends a declared name. The next
	// ';' is found once for a run of '&', and each '&' of the run has its own
	// candidate, as long as the rest of the run: looking each up hashes it
	// whole (11 names and an 8 MiB run took minutes). The candidates of a run
	// are filtered in one pass instead (entityFilter), and only those it
	// passes are looked up, their bytes counted.
	filter, err := newEntityFilter(declared, w)
	if err != nil {
		return "", err
	}
	semicolon := -1 // the next ';' at or after i, or len(doc) when there is none
	var b strings.Builder
	b.WriteString(doc[:root])
	indirect := 0
	for i, steps := root, 1; i < len(doc); steps++ {
		if steps%checkEvery == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		switch {
		case strings.HasPrefix(doc[i:], "<!--"):
			j := skipPast(doc, i, "-->")
			b.WriteString(doc[i:j])
			i = j
		case strings.HasPrefix(doc[i:], "<![CDATA["):
			j := skipPast(doc, i, "]]>")
			b.WriteString(doc[i:j])
			i = j
		case strings.HasPrefix(doc[i:], "<?"):
			j := skipPast(doc, i, "?>")
			b.WriteString(doc[i:j])
			i = j
		case doc[i] == '&':
			if semicolon < i {
				semicolon = len(doc)
				if k := strings.IndexByte(doc[i:], ';'); k >= 0 {
					semicolon = i + k
					if err := filter.window(doc, i, semicolon, w); err != nil {
						return "", err
					}
				}
			}
			name := ""
			if j := semicolon - i; semicolon < len(doc) && filter.hits[i+1] {
				if err := w.add(j - 1); err != nil {
					return "", err
				}
				name = doc[i+1 : i+j]
			}
			if _, ok := x.lits[name]; !ok {
				b.WriteByte('&') // a character reference, a predefined or an undefined entity: the decoder's
				i++
				continue
			}
			j := semicolon - i
			n, err := x.length(name, 0)
			if err != nil {
				return "", err
			}
			indirect += n
			x.charged += n
			direct := i + j + 1
			if x.charged > maxEntityText || direct+indirect >= expatActivation && direct+indirect > expatAmplification*direct {
				return "", &ParseError{Label: "parser_exception"}
			}
			v, err := x.value(name, 0)
			if err != nil {
				return "", err
			}
			b.WriteString(v)
			i = direct
		default:
			j := strings.IndexAny(doc[i:], "<&")
			if j < 0 {
				j = len(doc) - i
			} else if j == 0 {
				j = 1
			}
			b.WriteString(doc[i : i+j])
			i += j
		}
	}
	return b.String(), nil
}

// entityFilter tells which candidate names of a run of '&' may be declared,
// without hashing each: a polynomial hash modulo 2^61-1 with a random base,
// computed backward from the run's ';' in one pass, is compared with the
// declared names' hashes of the same length. Equal strings hash alike, so
// every declared candidate passes; one that passes is looked up, which
// decides, so the result is the lookups'. With a random base, a candidate of
// n bytes that is no declared name passes with a probability of at most
// n/2^61, however the document is made.
type entityFilter struct {
	base    uint64
	longest int
	lengths map[int]bool
	hashes  map[entityHash]bool
	hits    map[int]bool // in the current run: the starts of candidates that passed
}

type entityHash struct {
	n int
	h uint64
}

const mersenne61 = 1<<61 - 1

func mulmod61(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	r := (hi<<3 | lo>>61) + (lo & mersenne61)
	r = (r & mersenne61) + (r >> 61)
	if r >= mersenne61 {
		r -= mersenne61
	}
	return r
}

func addmod61(a, b uint64) uint64 {
	r := a + b
	if r >= mersenne61 {
		r -= mersenne61
	}
	return r
}

func newEntityFilter(declared map[string]string, w *entityWork) (*entityFilter, error) {
	f := &entityFilter{base: 256 + rand.Uint64N(mersenne61-256), lengths: map[int]bool{}, hashes: map[entityHash]bool{}, hits: map[int]bool{}}
	for name := range declared {
		if err := w.add(len(name)); err != nil {
			return nil, err
		}
		var h uint64
		for i := 0; i < len(name); i++ {
			h = addmod61(mulmod61(h, f.base), uint64(name[i])+1)
		}
		f.longest = max(f.longest, len(name))
		f.lengths[len(name)] = true
		f.hashes[entityHash{len(name), h}] = true
	}
	return f, nil
}

// window finds the candidates of the run of '&' from amp to the ';' at semi:
// doc[p:semi] for each '&' at p-1 in [amp, semi). It reads at most the
// longest name's length back from semi, once per run; runs do not overlap.
func (f *entityFilter) window(doc string, amp, semi int, w *entityWork) error {
	clear(f.hits)
	lo := max(amp+1, semi-f.longest)
	if err := w.add(semi - lo); err != nil {
		return err
	}
	var h uint64
	pw := uint64(1)
	for p := semi - 1; p >= lo; p-- {
		h = addmod61(h, mulmod61(uint64(doc[p])+1, pw))
		pw = mulmod61(pw, f.base)
		if n := semi - p; doc[p-1] == '&' && f.lengths[n] && f.hashes[entityHash{n, h}] {
			f.hits[p] = true
		}
	}
	return nil
}

// entityWork counts the steps of entity processing (reading declarations,
// replacing character references, expanding at use) and checks ctx every
// entityCheckSteps of them: one declaration can be as long as the document.
type entityWork struct {
	ctx context.Context
	n   int
}

const entityCheckSteps = 1 << 16

// add counts steps, and every entityCheckSteps returns ctx's error when ctx
// is done.
func (w *entityWork) add(steps int) error {
	if w.n += steps; w.n < entityCheckSteps {
		return nil
	}
	w.n = 0
	return w.ctx.Err()
}

// prologEntities reads the prolog up to the document element: the internal
// subset's general entity declarations (the first declaration of a name binds;
// external ones are empty, never fetched) and the document element's offset.
func prologEntities(doc string, w *entityWork) (map[string]string, int, error) {
	if !strings.Contains(doc, "<!ENTITY") {
		return nil, 0, nil
	}
	d := xml.NewDecoder(&ctxReader{ctx: w.ctx, r: strings.NewReader(doc)})
	d.Strict = true
	d.CharsetReader = func(_ string, r io.Reader) (io.Reader, error) { return r, nil }
	declared := map[string]string{}
	for {
		offset := int(d.InputOffset())
		tok, err := d.RawToken()
		if err != nil {
			if cerr := w.ctx.Err(); cerr != nil {
				return nil, 0, cerr // ctxReader stopped the token
			}
			return nil, 0, &ParseError{Label: "parser_exception"}
		}
		switch t := tok.(type) {
		case xml.StartElement:
			return declared, offset, nil
		case xml.Directive:
			err := entityDeclarations(string(t), w, func(name, value string) {
				if _, seen := declared[name]; !seen {
					declared[name] = value // empty for SYSTEM and PUBLIC
				}
			})
			if err != nil {
				return nil, 0, err
			}
		}
	}
}

// entityDeclarations reports a directive's general entity declarations, its
// matches of
//
//	<!ENTITY\s+([^\s%]+)\s+(?:"([^"]*)"|'([^']*)'|(SYSTEM|PUBLIC)\b[^>]*)\s*>
//
// as regexp's FindAllStringSubmatch finds them (TestEntityDeclarationsMatchTheExpression):
// the leftmost matches in order, each searched for from the end of the last,
// a "<!ENTITY" the rest does not match passed over; value is the quoted text,
// empty for SYSTEM and PUBLIC. Every part is determined: \s is [\t\n\f\r ],
// the name is the longest run before the whitespace that must follow it, a
// quoted value ends at its quote, and [^>]* at the next '>'. The next quote
// and '>' are found once for all attempts after them, and a candidate is
// rejected as soon as a part is missing, so the scan is linear; its steps are
// counted: a declaration can be as long as the document.
func entityDeclarations(dir string, w *entityWork, found func(name, value string)) error {
	space := func(c byte) bool { return c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == ' ' }
	// The next quote or '>' at or after a position, or len(dir): a search
	// from `from` found it `at`, which stands for every position between.
	type hit struct{ from, at int }
	next := map[byte]hit{}
	nextAt := func(c byte, i int) int {
		if f, ok := next[c]; ok && f.from <= i && i <= f.at {
			return f.at
		}
		f := hit{i, len(dir)}
		if k := strings.IndexByte(dir[i:], c); k >= 0 {
			f.at = i + k
		}
		next[c] = f
		return f.at
	}
	for pos := 0; ; {
		k := strings.Index(dir[pos:], "<!ENTITY")
		if k < 0 {
			return w.add(len(dir) - pos)
		}
		if err := w.add(k + 1); err != nil {
			return err
		}
		start := pos + k
		pos = start + 1 // where the search goes on if this one does not match
		// Each part is rejected as soon as it is missing, before the next is
		// scanned: a candidate whose name would run into the next "<!ENTITY"
		// stops at its missing whitespace, so no scan covers another
		// candidate's name and the search stays linear.
		i := start + len("<!ENTITY")
		j := i
		for j < len(dir) && space(dir[j]) {
			j++
		}
		if j == i {
			continue // \s+
		}
		n := j
		for n < len(dir) && !space(dir[n]) && dir[n] != '%' {
			n++
		}
		if n == j {
			continue // a name
		}
		v := n
		for v < len(dir) && space(dir[v]) {
			v++
		}
		if err := w.add(v - i); err != nil {
			return err
		}
		if v == n || v == len(dir) {
			continue // \s+, then a value
		}
		name, value, end := dir[j:n], "", -1
		switch c := dir[v]; c {
		case '"', '\'':
			q := nextAt(c, v+1)
			if q == len(dir) {
				continue
			}
			e := q + 1
			for e < len(dir) && space(dir[e]) {
				e++
			}
			if err := w.add(e - v); err != nil {
				return err
			}
			if e < len(dir) && dir[e] == '>' {
				value, end = dir[v+1:q], e+1
			}
		case 'S', 'P':
			kw := dir[v:min(v+6, len(dir))]
			if kw != "SYSTEM" && kw != "PUBLIC" {
				continue
			}
			if b := v + 6; b < len(dir) && (dir[b] == '_' || dir[b] >= '0' && dir[b] <= '9' || dir[b] >= 'A' && dir[b] <= 'Z' || dir[b] >= 'a' && dir[b] <= 'z') {
				continue // \b
			}
			if g := nextAt('>', v+6); g < len(dir) {
				end = g + 1
			}
		}
		if end < 0 {
			continue
		}
		if err := w.add(end - v); err != nil {
			return err
		}
		found(name, value)
		pos = end
	}
}

// entityExpander expands declared entities at use, lazily: an entity that is
// never used is never expanded, and lengths are known before any text is built.
// Every use, and every expansion built along the way (an entity inside
// another is built and kept too), is charged to the entity budget before it
// is allocated.
type entityExpander struct {
	declared map[string]string
	lits     map[string]string // replacement texts after character references
	lens     map[string]int    // expanded lengths, at most maxEntityText+1
	vals     map[string]string // expanded texts of used entities
	charged  int               // entity text charged so far: uses and built expansions
	w        *entityWork
}

const maxEntityDepth = 64

// length is the entity's fully expanded length, saturated just past the
// budget; a recursive or undefined reference inside is malformed.
func (x *entityExpander) length(name string, depth int) (int, error) {
	if n, ok := x.lens[name]; ok {
		if n < 0 {
			return 0, &ParseError{Label: "parser_exception"} // recursion
		}
		return n, nil
	}
	if depth > maxEntityDepth {
		return 0, &ParseError{Label: "parser_exception"}
	}
	x.lens[name] = -1
	total := 0
	err := forReferences(x.lits[name], x.w, func(text string) { total += len(text) }, func(ref string) error {
		if _, ok := x.lits[ref]; !ok {
			return &ParseError{Label: "parser_exception"} // undefined
		}
		n, err := x.length(ref, depth+1)
		total += n
		return err
	})
	if err != nil {
		return 0, err
	}
	x.lens[name] = min(total, maxEntityText+1)
	return x.lens[name], nil
}

// value is the entity's expanded text; length has measured it. Building it
// is charged first.
func (x *entityExpander) value(name string, depth int) (string, error) {
	if v, ok := x.vals[name]; ok {
		return v, nil
	}
	if x.charged += x.lens[name]; x.charged > maxEntityText {
		return "", &ParseError{Label: "parser_exception"}
	}
	var b strings.Builder
	b.Grow(x.lens[name])
	err := forReferences(x.lits[name], x.w, func(text string) { b.WriteString(text) }, func(ref string) error {
		v, err := x.value(ref, depth+1)
		b.WriteString(v)
		return err
	})
	if err != nil {
		return "", err
	}
	x.vals[name] = b.String()
	return x.vals[name], nil
}

// forReferences splits a replacement text into its text and its references to
// declared or undefined general entities; character references and the
// predefined entities stay text, for the decoder. Its steps are counted.
func forReferences(lit string, w *entityWork, text func(string), ref func(string) error) error {
	for {
		i := strings.IndexByte(lit, '&')
		if i < 0 {
			text(lit)
			return w.add(len(lit))
		}
		j := strings.IndexByte(lit[i:], ';')
		if j < 2 {
			return &ParseError{Label: "parser_exception"} // a "&" that starts no reference
		}
		if err := w.add(i + j + 1); err != nil {
			return err
		}
		name := lit[i+1 : i+j]
		if _, pre := predefinedEntities[name]; pre || name[0] == '#' {
			text(lit[:i+j+1])
		} else {
			text(lit[:i])
			if err := ref(name); err != nil {
				return err
			}
		}
		lit = lit[i+j+1:]
	}
}

var predefinedEntities = map[string]string{"amp": "&", "lt": "<", "gt": ">", "quot": `"`, "apos": "'"}

// replaceReferences replaces "&#…;" character references and, through named,
// "&name;" references (named reports whether it replaced one; unreplaced ones
// stay as they are). A "&" that starts no reference is malformed. Its steps
// are counted.
func replaceReferences(s string, named func(string) (string, bool, error), w *entityWork) (string, error) {
	if !strings.Contains(s, "&") {
		return s, w.add(len(s))
	}
	var b strings.Builder
	for {
		i := strings.IndexByte(s, '&')
		if i < 0 {
			b.WriteString(s)
			return b.String(), w.add(len(s))
		}
		b.WriteString(s[:i])
		j := strings.IndexByte(s[i:], ';')
		if j < 2 {
			return "", &ParseError{Label: "parser_exception"}
		}
		if err := w.add(i + j + 1); err != nil {
			return "", err
		}
		ref := s[i+1 : i+j]
		if ref[0] == '#' {
			r, ok := charReference(ref[1:])
			if !ok {
				return "", &ParseError{Label: "parser_exception"}
			}
			b.WriteRune(r)
		} else if v, done, err := named(ref); err != nil {
			return "", err
		} else if done {
			b.WriteString(v)
		} else {
			b.WriteString(s[i : i+j+1])
		}
		s = s[i+j+1:]
	}
}

// charReference is an XML character reference's value ("65" or "x41").
func charReference(ref string) (rune, bool) {
	base := 10
	if strings.HasPrefix(ref, "x") {
		ref, base = ref[1:], 16
	}
	v, err := strconv.ParseUint(ref, base, 32)
	if err != nil || ref == "" || ref[0] == '+' || !isXMLChar(rune(v)) {
		return 0, false
	}
	return rune(v), true
}

func isXMLChar(r rune) bool {
	return r == 0x9 || r == 0xA || r == 0xD || r >= 0x20 && r <= 0xD7FF || r >= 0xE000 && r <= 0xFFFD || r >= 0x10000 && r <= 0x10FFFF
}

func skipPast(s string, i int, end string) int {
	if j := strings.Index(s[i:], end); j >= 0 {
		return i + j + len(end)
	}
	return len(s)
}
