package feed

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"time"
)

// ParseError is a feed Android refuses: its download-error label and detail.
type ParseError struct {
	Label  string // "parser_exception", "unsupported_type" or "unsupported_type_html"
	Detail string
}

func (e *ParseError) Error() string { return "feed: " + e.Label + ": " + e.Detail }

// Generative cover marker for a feed without an image (a saved-data prefix).
const generativeCover = "antennapod_generative_cover:"

// Parse parses a feed document as Android's FeedParserTask: TypeGetter picks the
// type, FeedHandler's SAX handlers parse, then the title checks and the
// generative cover. source is the feed's download URL; now is the evaluation
// time of DateUtils.parseOrNullIfFuture. A feed never stops the server: a
// panic while parsing is a parser exception.
//
// A parse whose context ended is neither a feed nor a parse error, however
// close to its end the context ended: its callers report the deadline, and a
// re-parse tries again (PROTOCOL §9.2).
func Parse(ctx context.Context, raw []byte, source string, now time.Time) (*Feed, error) {
	f, err := parse(ctx, raw, source, now)
	if cerr := ctx.Err(); cerr != nil {
		return nil, cerr
	}
	return f, err
}

func parse(ctx context.Context, raw []byte, source string, now time.Time) (f *Feed, err error) {
	defer func() {
		if r := recover(); r != nil {
			f, err = nil, &ParseError{Label: "parser_exception"}
		}
	}()
	text, err := decodeDocument(raw)
	if err != nil {
		// XmlStreamReader failed: TypeGetter's reader is unusable, and the
		// document is classified as a website.
		if pe := websiteError(ctx, string(raw)); ctx.Err() == nil {
			return nil, pe
		}
		return nil, ctx.Err()
	}
	kind, lang, perr := typeOf(ctx, text)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if perr != nil {
		return nil, perr
	}
	st := newHandlerState(kind, now.UnixMilli())
	switch kind { // TypeGetter sets no type for RSS 0.91 and 0.92
	case "atom":
		st.feed.Type = "atom"
		if lang != nil {
			st.feed.Language = lang
		}
	case "rss20":
		st.feed.Type = "rss"
	}
	if err := saxParse(ctx, text, st); err != nil {
		var pe *ParseError
		if errors.As(err, &pe) {
			return nil, pe
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, &ParseError{Label: "parser_exception"}
	}
	f = st.feed
	f.Items = st.items
	f.AlternateURLs = st.alternateURLs
	f.AlternateNull, f.HasAlternateNull = st.alternateNull, st.hasAlternateNull
	f.RedirectURL = st.redirectURL
	if f.Title == nil {
		return nil, &ParseError{Label: "parser_exception", Detail: "Feed has no title"}
	}
	for _, it := range f.Items {
		if it.Title == nil {
			return nil, &ParseError{Label: "parser_exception", Detail: "Item has no title"}
		}
	}
	if empty(f.ImageURL) {
		f.ImageURL = ptr(generativeCover + source)
	}
	return f, nil
}

// ---- TypeGetter

// typeOf is TypeGetter.getType: the first start tag decides.
func typeOf(ctx context.Context, text string) (kind string, lang *string, err *ParseError) {
	d := xml.NewDecoder(&ctxReader{ctx: ctx, r: strings.NewReader(text)})
	d.Strict = true
	d.CharsetReader = func(_ string, r io.Reader) (io.Reader, error) { return r, nil }
	d.Entity = xml.HTMLEntity
	for n := 1; ; n++ {
		if n%checkEvery == 0 && ctx.Err() != nil {
			return "", nil, nil // the caller reports the cancellation
		}
		tok, terr := d.RawToken()
		if terr != nil && ctx.Err() != nil {
			return "", nil, nil // ctxReader stopped the token; the caller reports it
		}
		if terr != nil {
			if terr == io.EOF {
				return "", nil, &ParseError{Label: "unsupported_type", Detail: "Unknown problem when trying to determine feed type"}
			}
			return "", nil, websiteError(ctx, text)
		}
		switch t := tok.(type) {
		case xml.CharData:
			if strings.TrimSpace(string(t)) != "" {
				return "", nil, websiteError(ctx, text)
			}
		case xml.StartElement:
			switch t.Name.Local {
			case "feed":
				for _, a := range t.Attr {
					if a.Name.Space == "xml" && a.Name.Local == "lang" {
						v := a.Value
						lang = &v
					}
				}
				return "atom", lang, nil
			case "rss":
				version, has := "", false
				for _, a := range t.Attr {
					if a.Name.Space == "" && a.Name.Local == "version" {
						version, has = a.Value, true
					}
				}
				if !has || version == "2.0" {
					return "rss20", nil, nil
				}
				if version == "0.91" || version == "0.92" {
					return "rss091", nil, nil
				}
				return "", nil, &ParseError{Label: "unsupported_type", Detail: "Unsupported rss version"}
			default:
				return "", nil, websiteError(ctx, text)
			}
		}
	}
}
