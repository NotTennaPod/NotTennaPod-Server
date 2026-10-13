package feed

import "strings"

// Atom (namespace/Atom.java and element/AtomText.java).

type atom struct{}

func isAtomText(name string) bool {
	return name == "title" || name == "content" || name == "subtitle" || name == "summary"
}

func isFeed(name string) bool     { return name == "feed" || name == "channel" }
func isFeedItem(name string) bool { return name == "entry" || name == "item" }

func (atom) start(local string, st *handlerState, a attributes) *syndElement {
	switch {
	case local == "entry":
		st.addItem()
	case isAtomText(local):
		return &syndElement{name: local, ns: atom{}, atom: true, atomType: a.get("type")}
	case local == "link":
		if len(st.tagstack) == 0 {
			panic(errStack)
		}
		href, rel := a.get("href"), a.get("rel")
		parent := st.top().name
		if isFeedItem(parent) && st.current != nil {
			switch {
			case rel == nil || *rel == "alternate":
				st.current.Link = href
			case *rel == "enclosure":
				var size int64
				if s := a.get("length"); !empty(s) {
					if v, ok := javaParseLong(*s); ok {
						size = v
					}
				}
				mime := mimeType(a.get("type"), href)
				valid := isMediaFile(mime)
				if !valid && st.current.Media == nil && !isImageFile(mime) {
					valid = true
					mime = ptr("audio/*")
				}
				if valid && st.current.Media == nil {
					st.current.Media = newMedia(href, size, mime)
				}
			case *rel == "payment":
				st.current.PaymentLink = href
			}
		} else if isFeed(parent) {
			typ := a.get("type")
			switch {
			case rel == nil || *rel == "alternate":
				// The link, unless typed as another feed (a podlove alternate feed).
				if (typ == nil && st.feed.Link == nil) || (typ != nil && (*typ == "text/html" || *typ == "application/xml+xhtml")) {
					st.feed.Link = href
				} else if typ != nil && (*typ == "application/atom+xml" || *typ == "application/rss+xml") {
					st.addAlternate(a.get("title"), href)
				}
			case *rel == "archives":
				if typ != nil && (*typ == "application/atom+xml" || *typ == "application/rss+xml") {
					st.addAlternate(a.get("title"), href)
				}
			case *rel == "payment":
				st.feed.Funding = append(st.feed.Funding, Funding{URL: href, Content: ptr("")})
			case *rel == "next":
				st.feed.Paged = true
				st.feed.NextPageURL = href
			}
		}
	}
	return &syndElement{name: local, ns: atom{}}
}

// addAlternate is Atom's podlove alternate feed: the title, or the href when
// the title is empty, put into HandlerState's HashMap by href (a repeated href
// keeps the later title; a null href is the null key).
func (st *handlerState) addAlternate(title, href *string) {
	if empty(title) {
		title = href
	}
	if href == nil {
		st.alternateNull, st.hasAlternateNull = title, true
		return
	}
	t := ""
	if title != nil {
		t = *title
	}
	st.alternateURLs[*href] = t
}

// processed is AtomText.getProcessedContent.
func (e *syndElement) processed(st *handlerState) *string {
	if e.content == nil {
		return nil
	}
	if e.atomType != nil && *e.atomType == "html" {
		return ptr(htmlText(st.ctx, *e.content, false))
	}
	return e.content
}

func (atom) end(local string, st *handlerState) {
	if local == "entry" {
		if it := st.current; it != nil && st.duration != nil {
			if it.Media != nil {
				it.Media.Duration = *st.duration
			}
			st.duration = nil
		}
		st.current = nil
	}
	if len(st.tagstack) < 2 {
		return
	}
	raw := ""
	if st.contentBuf != nil {
		raw = st.contentBuf.String()
	}
	text := trimAllWhitespace(raw)
	top := st.top()
	second := st.secondTag().name
	var textEl *syndElement
	if isAtomText(top.name) {
		if top.atom {
			textEl = top
			textEl.content = ptr(text)
		} else {
			panic(errNull) // a ClassCastException in Java: only Atom's own elements are AtomText
		}
	}
	switch {
	case top.name == "id":
		if second == "feed" {
			st.feed.Identifier = ptr(raw)
		} else if second == "entry" && st.current != nil {
			st.current.GUID = ptr(raw)
		}
	case top.name == "title" && textEl != nil:
		if second == "feed" {
			st.feed.Title = textEl.processed(st)
		} else if second == "entry" && st.current != nil {
			st.current.Title = textEl.processed(st)
		}
	case top.name == "subtitle" && second == "feed" && textEl != nil:
		st.feed.Description = textEl.processed(st)
	case top.name == "content" && second == "entry" && textEl != nil && st.current != nil:
		st.current.setDescriptionIfLonger(textEl.processed(st))
	case top.name == "summary" && second == "entry" && textEl != nil && st.current != nil:
		st.current.setDescriptionIfLonger(textEl.processed(st))
	case top.name == "updated" && second == "entry" && st.current != nil && st.current.PubDate == nil:
		st.current.PubDate = parseDateOrNullIfFuture(st.ctx, text, st.now)
	case top.name == "published" && second == "entry" && st.current != nil:
		st.current.PubDate = parseDateOrNullIfFuture(st.ctx, text, st.now)
	case top.name == "logo" && st.feed.ImageURL == nil:
		st.feed.ImageURL = ptr(text)
	case top.name == "icon":
		st.feed.ImageURL = ptr(text)
	case top.name == "name" && second == "author" && st.current == nil:
		st.addFeedAuthor(text)
	}
}

// addFeedAuthor is Atom's feed author name: the author, or the author so far,
// ", " and the name. Java concatenates anew each time, quadratic time for very
// many names; here they are appended in place while the author is the one
// built so (another handler may replace it).
func (st *handlerState) addFeedAuthor(name string) {
	if st.feed.Author == nil {
		st.feed.Author = ptr(name)
		return
	}
	if st.authors == nil || st.authorsOf != st.feed.Author {
		st.authors = &strings.Builder{}
		st.authors.WriteString(*st.feed.Author)
	}
	st.authors.WriteString(", ")
	st.authors.WriteString(name)
	st.feed.Author = ptr(st.authors.String())
	st.authorsOf = st.feed.Author
}
