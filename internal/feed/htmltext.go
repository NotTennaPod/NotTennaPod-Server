package feed

import (
	"context"
	"fmt"
	"unicode/utf16"
)

// htmlText is HtmlCompat.fromHtml(s, compact ? FROM_HTML_MODE_COMPACT :
// FROM_HTML_MODE_LEGACY).toString() on Android 16, where HtmlCompat calls
// android.text.Html.fromHtml(s, flags): TagSoup's parser (tagsoup_*.go)
// feeding Html's HtmlToSpannedConverter, of which only the text is kept.
//
// The work is on UTF-16 code units, as in Java; an unpaired surrogate in the
// result becomes U+FFFD, as Go strings hold no surrogates. Where fromHtml
// throws, htmlText panics with htmlErrException.
//
// Some inputs take TagSoup quadratic time, so the conversion checks ctx (the
// fetch deadline also stops parsing, PROTOCOL §9.2) and panics with
// parseCanceled when it is done.
func htmlText(ctx context.Context, s string, compact bool) string {
	c := &htmlConverter{margin: 2}
	if compact {
		c.margin = 1
	}
	p := &tagsoupParser{conv: c, ctx: ctx}
	p.parse(utf16.Encode([]rune(s)))
	return string(utf16.Decode(c.text))
}

// parseCanceled carries the context's error out of a value conversion (HTML
// text, dates) to saxParse.
type parseCanceled struct{ err error }

// htmlErrException is the StringIndexOutOfBoundsException that Html.fromHtml
// throws for a few malformed inputs: some declarations (tagsoupParser.decl)
// and <font color="-"> (htmlConverter.startFont). It wraps errNull so that the
// feed parse fails, as Android's does.
var htmlErrException = fmt.Errorf("feed: Html.fromHtml threw StringIndexOutOfBoundsException: %w", errNull)

// htmlConverter is the text side of Html's HtmlToSpannedConverter, a
// ContentHandler of the TagSoup parser. Spans do not change the text and are
// left out.
type htmlConverter struct {
	// margin is getMargin: the newlines around a block element, 1 for every
	// block element with FROM_HTML_MODE_COMPACT (all six separator flags), 2
	// with FROM_HTML_MODE_LEGACY.
	margin int
	text   []uint16 // the SpannableStringBuilder
}

// htmlTag is what Html's converter does with an element, by its local name.
type htmlTag uint8

const (
	htmlTagOther htmlTag = iota // spans only, or nothing
	htmlTagBlock                // startBlockElement and endBlockElement: p, ul, li, div, blockquote, h1-h6
	htmlTagBr                   // handleBr at the end
	htmlTagImg                  // startImg
	htmlTagFont                 // startFont
)

// htmlTagOf is handleStartTag's and handleEndTag's choice by local name, with
// String.equalsIgnoreCase. A name of more than 40 bytes has more than 10
// UTF-16 units (at most 3 bytes each, 4 for 2), longer than any it is
// compared with ("blockquote"), and is not converted.
func htmlTagOf(localName string) htmlTag {
	if len(localName) > 40 {
		return htmlTagOther
	}
	tag := utf16.Encode([]rune(localName))
	if len(tag) == 2 && tagsoupToLower(tag[0]) == 'h' && tag[1] >= '1' && tag[1] <= '6' {
		return htmlTagBlock
	}
	for _, t := range [...]struct {
		name string
		tag  htmlTag
	}{{"br", htmlTagBr}, {"p", htmlTagBlock}, {"ul", htmlTagBlock}, {"li", htmlTagBlock}, {"div", htmlTagBlock},
		{"blockquote", htmlTagBlock}, {"font", htmlTagFont}, {"img", htmlTagImg}} {
		if htmlEqualsIgnoreCase(tag, t.name) {
			return t.tag
		}
	}
	return htmlTagOther
}

// startElement is handleStartTag: a block element starts on a new line
// (startBlockElement's appendNewlines), an image is U+FFFC (startImg), a font
// checks its color (startFont).
func (c *htmlConverter) startElement(e *tagsoupElement) {
	switch e.typ.html {
	case htmlTagBlock:
		c.appendNewlines(c.margin)
	case htmlTagFont:
		c.startFont(e.attribute("color"))
	case htmlTagImg:
		c.text = append(c.text, 0xFFFC)
	}
}

// endElement is handleEndTag: <br> is a newline (handleBr) and a block element
// ends with its newlines (endBlockElement). The Newline mark it reads is the
// latest one, from the innermost open block element: TagSoup closes every
// element it opens, in order, and all block margins are equal.
func (c *htmlConverter) endElement(e *tagsoupElement) {
	switch e.typ.html {
	case htmlTagBr:
		c.text = append(c.text, '\n')
	case htmlTagBlock:
		c.appendNewlines(c.margin)
	}
}

// appendNewlines is Html's appendNewlines: at least min newlines at the end of
// text that is not empty. Html counts all the newlines at the end; counting up
// to min decides the same, without quadratic time for many blocks after many
// line breaks.
func (c *htmlConverter) appendNewlines(min int) {
	if len(c.text) == 0 {
		return
	}
	n := 0
	for i := len(c.text) - 1; i >= 0 && c.text[i] == '\n' && n < min; i-- {
		n++
	}
	for ; n < min; n++ {
		c.text = append(c.text, '\n')
	}
}

// characters is HtmlToSpannedConverter.characters: a space or newline after
// a space or newline, or at the start, is dropped; any other is a space.
func (c *htmlConverter) characters(buf []uint16) {
	for _, ch := range buf {
		if ch == ' ' || ch == '\n' {
			pred := uint16('\n')
			if n := len(c.text); n > 0 {
				pred = c.text[n-1]
			}
			if pred != ' ' && pred != '\n' {
				c.text = append(c.text, ' ')
			}
		} else {
			c.text = append(c.text, ch)
		}
	}
}

// startFont is the part of startFont that can throw: getHtmlColor hands a
// color that does not start with a letter to XmlUtils.convertValueToInt,
// which reads past the end of "-".
func (c *htmlConverter) startFont(color []uint16) {
	if len(color) == 1 && color[0] == '-' {
		panic(htmlErrException)
	}
}
