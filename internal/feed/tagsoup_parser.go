package feed

import (
	"context"
	"strings"
	"sync"
	"unicode/utf16"
)

//go:generate go run ./internal/gentagsoup -out tagsoup_schema.go

// The port of TagSoup's Parser, Schema, ElementType and Element
// (platform/external/tagsoup, Android 16), with the features Html.fromHtml
// leaves at their defaults: namespaces, unknown elements kept with an ANY
// content model and allowed at the root, restartable elements restarted,
// script and style read as CDATA, ignorable whitespace dropped.

// Schema's content models and flags.
const (
	tagsoupMAny   uint32 = 0xFFFFFFFF
	tagsoupMEmpty uint32 = 0

	tagsoupFRestart = 1
	tagsoupFCdata   = 2
	tagsoupFNoforce = 4
)

// tagsoupElementDef is one HTMLSchema element type.
type tagsoupElementDef struct {
	name            string
	model, memberOf uint32
	flags           int
	parent          string
}

// tagsoupElementType is ElementType.
type tagsoupElementType struct {
	name, localName string
	model, memberOf uint32
	flags           int
	parent          *tagsoupElementType
	html            htmlTag // what Html's converter does with the local name
}

func (t *tagsoupElementType) canContain(o *tagsoupElementType) bool { return t.model&o.memberOf != 0 }

// tagsoupSchemaTypes is HTMLSchema's element types by lower-case name.
var tagsoupSchemaTypes = sync.OnceValue(func() map[string]*tagsoupElementType {
	types := make(map[string]*tagsoupElementType, len(tagsoupElementDefs))
	for _, d := range tagsoupElementDefs {
		types[d.name] = tagsoupNewType(d.name, d.model, d.memberOf, d.flags)
	}
	for _, d := range tagsoupElementDefs {
		if d.parent != "" {
			types[d.name].parent = types[d.parent]
		}
	}
	return types
})

func tagsoupNewType(name string, model, memberOf uint32, flags int) *tagsoupElementType {
	local := name
	if i := strings.IndexByte(name, ':'); i >= 0 {
		local = name[i+1:]
	}
	return &tagsoupElementType{name: name, localName: local, model: model, memberOf: memberOf, flags: flags,
		html: htmlTagOf(local)}
}

// tagsoupAttr is an attribute of an element: its qualified name and value.
type tagsoupAttr struct {
	name  string
	value []uint16
}

// tagsoupElement is Element: an element on the stack or the restart queue.
type tagsoupElement struct {
	typ       *tagsoupElementType
	attrs     []tagsoupAttr
	index     map[string]int // attrs by name, once there are many
	next      *tagsoupElement
	preclosed bool
}

// tagsoupIndexAttrs is the attribute count from which an element indexes its
// attributes by name: a tag with very many is set in linear time.
const tagsoupIndexAttrs = 16

func (e *tagsoupElement) canContain(o *tagsoupElement) bool { return e.typ.canContain(o.typ) }

// setAttribute is ElementType.setAttribute for an element: namespace
// declarations are ignored, a repeated name replaces the value.
func (e *tagsoupElement) setAttribute(name string, value []uint16) {
	if name == "xmlns" || strings.HasPrefix(name, "xmlns:") {
		return
	}
	if i, ok := e.attributeIndex(name); ok {
		e.attrs[i].value = value
		return
	}
	if e.index != nil {
		e.index[name] = len(e.attrs)
	} else if len(e.attrs) >= tagsoupIndexAttrs {
		e.index = make(map[string]int, 2*len(e.attrs))
		for i, a := range e.attrs {
			e.index[a.name] = i
		}
		e.index[name] = len(e.attrs)
	}
	e.attrs = append(e.attrs, tagsoupAttr{name, value})
}

// attributeIndex is the index of the attribute with a name (names are unique).
func (e *tagsoupElement) attributeIndex(name string) (int, bool) {
	if e.index != nil {
		i, ok := e.index[name]
		return i, ok
	}
	for i := range e.attrs {
		if e.attrs[i].name == name {
			return i, true
		}
	}
	return 0, false
}

// attribute is Attributes.getValue("", local) for an unprefixed name.
func (e *tagsoupElement) attribute(local string) []uint16 {
	if i, ok := e.attributeIndex(local); ok {
		return e.attrs[i].value
	}
	return nil
}

// tagsoupParser is Parser, reporting elements and text to Html's converter.
// Unknown elements are defined for this parse only: Android keeps them in its
// shared schema, with the same content model, so later parses see the same.
type tagsoupParser struct {
	scanner          *tagsoupScanner
	conv             *htmlConverter
	bogons           map[string]*tagsoupElementType
	newElement       *tagsoupElement
	attributeName    *string
	stack            *tagsoupElement
	saved            *tagsoupElement
	pcdataElement    *tagsoupElement
	virginStack      bool
	doctypeIsPresent bool
	ctx              context.Context // checked while scanning (htmlText)
	work             int             // stack steps since ctx was last checked
}

// tagsoupCheckWork is how many steps the parser takes between checks of
// ctx: elements passed over in a walk down the stack, popped or pushed, and
// code units of a token's name, value, text or declaration. Some inputs make
// TagSoup walk a deep stack for every tag, quadratic time on Android too, and
// one token can be as long as the text; the scanner's own check counts only
// characters as they are read.
const tagsoupCheckWork = 1 << 16

// step counts a step and checks ctx every tagsoupCheckWork of them,
// panicking with parseCanceled when it is done.
func (p *tagsoupParser) step() { p.steps(1) }

// steps counts n steps.
func (p *tagsoupParser) steps(n int) {
	if p.work += n; p.work < tagsoupCheckWork || p.ctx == nil {
		return
	}
	p.work = 0
	if err := p.ctx.Err(); err != nil {
		panic(parseCanceled{err})
	}
}

// parse is Parser.parse.
func (p *tagsoupParser) parse(src []uint16) {
	p.stack = &tagsoupElement{typ: p.elementType("<root>")}
	p.pcdataElement = &tagsoupElement{typ: p.elementType("<pcdata>")}
	p.virginStack = true
	p.scanner = &tagsoupScanner{src: src}
	p.scanner.scan(p)
}

// elementType is Schema.getElementType.
func (p *tagsoupParser) elementType(name string) *tagsoupElementType {
	t, _ := p.elementTypeKey(name)
	return t
}

// elementTypeKey is elementType and the lower-cased name it looked up.
func (p *tagsoupParser) elementTypeKey(name string) (*tagsoupElementType, string) {
	key := tagsoupLowerRoot(name, p.step)
	if t := tagsoupSchemaTypes()[key]; t != nil {
		return t, key
	}
	return p.bogons[key], key
}

func tagsoupString(b []uint16) string { return string(utf16.Decode(b)) }

// adup is Parser.adup: an attribute without a value takes its name.
func (p *tagsoupParser) adup() {
	if p.newElement == nil || p.attributeName == nil {
		return
	}
	p.steps(len(*p.attributeName))
	p.newElement.setAttribute(*p.attributeName, utf16.Encode([]rune(*p.attributeName)))
	p.attributeName = nil
}

// aname is Parser.aname.
func (p *tagsoupParser) aname(buf []uint16) {
	if p.newElement == nil {
		return
	}
	name := tagsoupLowerRoot(tagsoupMakeName(buf, p.step), p.step)
	p.attributeName = &name
}

// aval is Parser.aval.
func (p *tagsoupParser) aval(buf []uint16) {
	if p.newElement == nil || p.attributeName == nil {
		return
	}
	p.newElement.setAttribute(*p.attributeName, p.expandEntities(buf))
	p.attributeName = nil
}

// expandEntities is Parser.expandEntities: in an attribute value, a reference
// is expanded only when a semicolon ends it.
func (p *tagsoupParser) expandEntities(src []uint16) []uint16 {
	refStart := -1
	dst := make([]uint16, len(src))
	n := 0
	for _, ch := range src {
		p.step()
		dst[n] = ch
		n++
		switch {
		case ch == '&' && refStart == -1:
			refStart = n // the start of a reference, after the &
		case refStart == -1:
		case tagsoupIsLetter(ch) || tagsoupIsDigit(ch) || ch == '#':
		case ch == ';':
			ent := p.lookupEntity(dst[refStart : n-1])
			if ent > 0xFFFF {
				ent -= 0x10000
				dst[refStart-1] = uint16((ent >> 10) + 0xD800)
				dst[refStart] = uint16((ent & 0x3FF) + 0xDC00)
				n = refStart + 1
			} else if ent != 0 {
				dst[refStart-1] = uint16(ent)
				n = refStart
			}
			refStart = -1
		default:
			refStart = -1
		}
	}
	return dst[:n]
}

// lookupEntity is Parser.lookupEntity: a numeric reference, or a named one
// from the schema; 0 when there is none.
func (p *tagsoupParser) lookupEntity(name []uint16) int32 {
	if len(name) < 1 {
		return 0
	}
	p.steps(len(name))
	if name[0] == '#' {
		if len(name) > 1 && (name[1] == 'x' || name[1] == 'X') {
			v, _ := tagsoupParseInt(name[2:], 16)
			return v
		}
		v, _ := tagsoupParseInt(name[1:], 10)
		return v
	}
	if len(name) > tagsoupLongestEntity() {
		return 0 // longer than any name in the table
	}
	return tagsoupEntities[tagsoupString(name)]
}

// tagsoupLongestEntity is the length of the longest name in tagsoupEntities.
var tagsoupLongestEntity = sync.OnceValue(func() int {
	n := 0
	for name := range tagsoupEntities {
		n = max(n, len(name))
	}
	return n
})

// eof is Parser.eof.
func (p *tagsoupParser) eof() {
	if p.virginStack {
		p.rectify(p.pcdataElement)
	}
	for p.stack.next != nil {
		p.pop()
	}
}

// etag is Parser.etag.
func (p *tagsoupParser) etag(buf []uint16) {
	if p.etagCdata(buf) {
		return
	}
	p.etagBasic(buf)
}

var tagsoupEtagChars = []uint16{'<', '/', '>'}

// etagCdata is Parser.etag_cdata: in script or style, an end tag for another
// element is text, and the CDATA content goes on.
func (p *tagsoupParser) etagCdata(buf []uint16) bool {
	if p.stack.typ.flags&tagsoupFCdata == 0 {
		return false
	}
	current := utf16.Encode([]rune(p.stack.typ.name))
	realTag := len(buf) == len(current)
	if realTag {
		for i := range buf {
			if tagsoupToLower(buf[i]) != tagsoupToLower(current[i]) {
				realTag = false
				break
			}
		}
	}
	if realTag {
		return false
	}
	p.conv.characters(tagsoupEtagChars[:2])
	p.characters(buf)
	p.conv.characters(tagsoupEtagChars[2:])
	p.scanner.startCDATA()
	return true
}

// etagBasic is Parser.etag_basic.
func (p *tagsoupParser) etagBasic(buf []uint16) {
	p.newElement = nil
	var name string
	if len(buf) != 0 {
		t := p.elementType(tagsoupMakeName(buf, p.step))
		if t == nil {
			return // a mysterious end tag
		}
		name = t.name
	} else {
		name = p.stack.typ.name
	}
	var sp *tagsoupElement
	inNoforce := false
	for sp = p.stack; sp != nil; sp = sp.next {
		p.step()
		if sp.typ.name == name {
			break
		}
		if sp.typ.flags&tagsoupFNoforce != 0 {
			inNoforce = true
		}
	}
	if sp == nil || sp.next == nil || sp.next.next == nil {
		return
	}
	if inNoforce {
		sp.preclosed = true
	} else {
		for p.stack != sp {
			p.restartablyPop()
		}
		p.pop()
	}
	for p.stack.preclosed {
		p.pop()
	}
	p.restart(nil)
}

// restart is Parser.restart: it pushes saved restartable elements back
// where they fit; e is the next element to be started, if known.
func (p *tagsoupParser) restart(e *tagsoupElement) {
	for p.saved != nil && p.stack.canContain(p.saved) && (e == nil || p.saved.canContain(e)) {
		next := p.saved.next
		p.push(p.saved)
		p.saved = next
	}
}

// pop is Parser.pop.
func (p *tagsoupParser) pop() {
	if p.stack == nil {
		return
	}
	p.step()
	p.conv.endElement(p.stack)
	p.stack = p.stack.next
}

// restartablyPop is Parser.restartablyPop: a restartable element is saved to
// be pushed again later. (Element.anonymize drops its id and name
// attributes, which no restartable element's text depends on.)
func (p *tagsoupParser) restartablyPop() {
	popped := p.stack
	p.pop()
	if popped.typ.flags&tagsoupFRestart != 0 {
		popped.next = p.saved
		p.saved = popped
	}
}

// push is Parser.push.
func (p *tagsoupParser) push(e *tagsoupElement) {
	p.step()
	p.conv.startElement(e)
	e.next = p.stack
	p.stack = e
	p.virginStack = false
	if e.typ.flags&tagsoupFCdata != 0 {
		p.scanner.startCDATA()
	}
}

// decl is Parser.decl. A declaration adds no text, but it can throw
// StringIndexOutOfBoundsException: in split, when the trimmed declaration
// ends in whitespace that String.trim keeps (U+3000, say), outside quotes;
// in trimquotes, when the first DOCTYPE's system or public identifier is a
// lone quote.
func (p *tagsoupParser) decl(buf []uint16) {
	v, ok := tagsoupSplit(buf, p.step)
	if !ok {
		panic(htmlErrException)
	}
	if len(v) == 0 || !htmlEqualsIgnoreCase(v[0], "DOCTYPE") {
		return
	}
	if p.doctypeIsPresent {
		return // one doctype only
	}
	p.doctypeIsPresent = true
	var ids [][]uint16
	if len(v) > 3 {
		switch tagsoupString(v[2]) {
		case "SYSTEM":
			ids = v[3:4]
		case "PUBLIC":
			ids = v[3:min(len(v), 5)]
		}
	}
	for _, id := range ids {
		if len(id) == 1 && (id[0] == '\'' || id[0] == '"') {
			panic(htmlErrException)
		}
	}
}

// tagsoupSplit is Parser.split: the trimmed value in words, a quoted phrase
// being one word. It is not ok where Java throws.
func tagsoupSplit(val []uint16, step func()) (words [][]uint16, ok bool) {
	for len(val) > 0 && val[0] <= ' ' {
		step()
		val = val[1:]
	}
	for len(val) > 0 && val[len(val)-1] <= ' ' {
		step()
		val = val[:len(val)-1]
	}
	if len(val) == 0 {
		return nil, true
	}
	s := 0
	sq, dq := false, false
	var last uint16
	for e, c := range val {
		step()
		switch {
		case !dq && c == '\'' && last != '\\':
			sq = !sq
			if s < 0 {
				s = e
			}
		case !sq && c == '"' && last != '\\':
			dq = !dq
			if s < 0 {
				s = e
			}
		case !sq && !dq:
			if tagsoupIsWhitespace(c) {
				if s >= 0 {
					words = append(words, val[s:e])
				}
				s = -1
			} else if s < 0 && c != ' ' {
				s = e
			}
		}
		last = c
	}
	if s < 0 {
		return nil, false // substring(-1, length)
	}
	return append(words, val[s:]), true
}

// gi is Parser.gi: the name of a start tag. An unknown element is defined
// with an ANY content model, allowed anywhere.
func (p *tagsoupParser) gi(buf []uint16) {
	if p.newElement != nil {
		return
	}
	name := tagsoupMakeName(buf, p.step)
	t, key := p.elementTypeKey(name)
	if t == nil {
		t = tagsoupNewType(name, tagsoupMAny, tagsoupMAny, 0)
		if p.bogons == nil {
			p.bogons = map[string]*tagsoupElementType{}
		}
		p.bogons[key] = t
	}
	p.newElement = &tagsoupElement{typ: t}
}

// pcdata is Parser.pcdata: whitespace where text is not allowed is dropped.
func (p *tagsoupParser) pcdata(buf []uint16) {
	if len(buf) == 0 {
		return
	}
	allWhite := true
	for _, c := range buf {
		p.step()
		if !tagsoupIsWhitespace(c) {
			allWhite = false
		}
	}
	if allWhite && !p.stack.canContain(p.pcdataElement) {
		return
	}
	p.rectify(p.pcdataElement)
	p.characters(buf)
}

// characters reports text to the converter in parts, counting its steps:
// the scanner can hand over a text as long as its buffer has grown. The
// converter's result does not depend on where the text is cut.
func (p *tagsoupParser) characters(buf []uint16) {
	for len(buf) > 0 {
		n := min(len(buf), tagsoupCheckWork)
		p.steps(n)
		p.conv.characters(buf[:n])
		buf = buf[n:]
	}
}

// stagc is Parser.stagc: the end of a start tag.
func (p *tagsoupParser) stagc(buf []uint16) {
	if p.newElement == nil {
		return
	}
	p.rectify(p.newElement)
	if p.stack.typ.model == tagsoupMEmpty {
		p.etagBasic(buf) // an immediate end tag
	}
}

// stage is Parser.stage: the end of an empty-element tag.
func (p *tagsoupParser) stage(buf []uint16) {
	if p.newElement == nil {
		return
	}
	p.rectify(p.newElement)
	p.etagBasic(buf)
}

// rectify is Parser.rectify: it pops and pushes elements, implied parents
// included, so that e can be pushed.
func (p *tagsoupParser) rectify(e *tagsoupElement) {
	var sp *tagsoupElement
	for {
		for sp = p.stack; sp != nil; sp = sp.next {
			p.step()
			if sp.canContain(e) {
				break
			}
		}
		if sp != nil {
			break
		}
		parentType := e.typ.parent
		if parentType == nil {
			break
		}
		e = &tagsoupElement{typ: parentType, next: e}
	}
	if sp == nil {
		return
	}
	for p.stack != sp {
		if p.stack == nil || p.stack.next == nil || p.stack.next.next == nil {
			break
		}
		p.restartablyPop()
	}
	for e != nil {
		next := e.next
		if e.typ.name != "<pcdata>" {
			p.push(e)
		}
		e = next
		p.restart(e)
	}
	p.newElement = nil
}

// tagsoupMakeName is Parser.makeName: letters, digits, '-', '.', '_' and
// one colon, with '_' where a name or local name would not start with a
// letter. step counts each code unit.
func tagsoupMakeName(buf []uint16, step func()) string {
	dst := make([]uint16, 0, len(buf)+2)
	seenColon, start := false, true
	for _, ch := range buf {
		step()
		switch {
		case tagsoupIsLetter(ch) || ch == '_':
			start = false
			dst = append(dst, ch)
		case tagsoupIsDigit(ch) || ch == '-' || ch == '.':
			if start {
				dst = append(dst, '_')
			}
			start = false
			dst = append(dst, ch)
		case ch == ':' && !seenColon:
			seenColon = true
			if start {
				dst = append(dst, '_')
			}
			start = true
			dst = append(dst, ch)
		}
	}
	if len(dst) == 0 || dst[len(dst)-1] == ':' {
		dst = append(dst, '_')
	}
	return tagsoupString(dst)
}
