package feed

import "regexp"

// The namespace handlers of parser/feed/namespace, rule for rule. Where the
// Java code would throw (a null item, a missing chapter list, an empty stack),
// the port panics with errNull or errStack and the parse fails as Android's does.

func (st *handlerState) item() *Item {
	if st.current == nil {
		panic(errNull)
	}
	return st.current
}

func newMedia(url *string, size int64, mime *string) *Media {
	return &Media{URL: url, Size: size, MIMEType: mime}
}

// ---- RSS 2.0 (Rss20)

type rss20 struct{}

func (rss20) start(local string, st *handlerState, a attributes) *syndElement {
	if len(st.tagstack) == 0 && (local == "item" || local == "enclosure") {
		panic(errStack) // Stack.lastElement() / peek() on an empty stack
	}
	if local == "item" && st.top().name == "channel" {
		st.addItem()
	} else if local == "enclosure" && st.top().name == "item" && st.current != nil {
		url := a.get("url")
		mime := mimeType(a.get("type"), url)
		valid := isMediaFile(mime)
		if !valid && !isImageFile(mime) && st.current.Media == nil {
			valid = true
			mime = ptr("audio/*")
		}
		if st.current.Media == nil && valid && !empty(url) {
			var size int64
			if s := a.get("length"); !empty(s) {
				if v, ok := javaParseLong(*s); ok {
					size = v
				}
			}
			if size < 16384 {
				size = 0 // less than 16 KiB is suspicious
			}
			st.current.Media = newMedia(url, size, mime)
		}
	}
	return &syndElement{name: local, ns: rss20{}}
}

func (rss20) end(local string, st *handlerState) {
	if local == "item" {
		if it := st.current; it != nil {
			// The title is optional in RSS 2.0; the description stands in.
			if it.Title == nil {
				it.Title = it.Description
			}
			if st.duration != nil {
				if it.Media != nil {
					it.Media.Duration = *st.duration
				}
				st.duration = nil
			}
		}
		st.current = nil
		return
	}
	if len(st.tagstack) < 2 || st.contentBuf == nil {
		return
	}
	raw := st.contentBuf.String()
	text := trimAllWhitespace(raw)
	top := st.top().name
	second := st.secondTag().name
	third := ""
	if len(st.tagstack) >= 3 {
		third = st.thirdTag().name
	}
	switch {
	case top == "guid" && second == "item":
		// An empty or non-standard guid is ignored.
		if raw != "" && st.current != nil {
			st.current.GUID = ptr(raw)
		}
	case top == "title":
		t := htmlText(st.ctx, text, true)
		if second == "item" && st.current != nil {
			st.current.Title = &t
		} else if second == "channel" {
			st.feed.Title = &t
		}
	case top == "link":
		if second == "channel" {
			st.feed.Link = ptr(text)
		} else if second == "item" && st.current != nil {
			st.current.Link = ptr(text)
		}
	case top == "pubDate" && second == "item" && st.current != nil:
		st.current.PubDate = parseDateOrNullIfFuture(st.ctx, text, st.now)
	case top == "url" && second == "image" && third == "channel":
		if st.feed.ImageURL == nil { // prefer itunes:image
			st.feed.ImageURL = ptr(text)
		}
	case local == "description":
		if second == "channel" {
			st.feed.Description = ptr(htmlText(st.ctx, text, true))
		} else if second == "item" && st.current != nil {
			st.current.setDescriptionIfLonger(ptr(text))
		}
	case local == "language":
		st.feed.Language = ptr(javaLowerUS(text))
	}
}

// ---- iTunes (Itunes)

type itunes struct{}

func (itunes) start(local string, st *handlerState, a attributes) *syndElement {
	if local == "image" {
		url := a.get("href")
		if st.current != nil {
			st.current.ImageURL = url
		} else if !empty(url) {
			st.feed.ImageURL = url // prefer to all other images
		}
	}
	return &syndElement{name: local, ns: itunes{}}
}

var httpPrefix = regexp.MustCompile(`^http`)

func (itunes) end(local string, st *handlerState) {
	if st.contentBuf == nil {
		return
	}
	c := st.contentBuf.String()
	if c == "" {
		return
	}
	switch {
	case local == "author" && len(st.tagstack) <= 3:
		st.feed.Author = ptr(htmlText(st.ctx, c, true))
	case local == "duration":
		if ms, ok := durationMillis(c); ok {
			d := int32(ms) // (int) durationMs
			st.duration = &d
		}
	case local == "subtitle":
		if st.current != nil && empty(st.current.Description) {
			st.current.setDescriptionIfLonger(ptr(c))
		} else if empty(st.feed.Description) {
			st.feed.Description = ptr(c)
		}
	case local == "summary":
		if st.current != nil {
			st.current.setDescriptionIfLonger(ptr(c))
		} else if st.secondTag().name == "channel" {
			st.feed.Description = ptr(c)
		}
	case local == "new-feed-url" && httpPrefix.MatchString(javaTrim(c)):
		st.redirectURL = ptr(javaTrim(c))
	}
}

// ---- content:encoded (Content)

type content struct{}

func (content) start(local string, _ *handlerState, _ attributes) *syndElement {
	return &syndElement{name: local, ns: content{}}
}

func (content) end(local string, st *handlerState) {
	if local == "encoded" && st.current != nil && st.contentBuf != nil {
		st.current.setDescriptionIfLonger(st.content())
	}
}

// ---- Dublin Core (DublinCore)

type dublinCore struct{}

func (dublinCore) start(local string, _ *handlerState, _ attributes) *syndElement {
	return &syndElement{name: local, ns: dublinCore{}}
}

func (dublinCore) end(_ string, st *handlerState) {
	if st.current != nil && st.contentBuf != nil && len(st.tagstack) >= 2 {
		if st.top().name == "date" && st.secondTag().name == "item" {
			st.current.PubDate = parseDateOrNullIfFuture(st.ctx, st.contentBuf.String(), st.now)
		}
	}
}

// ---- Media RSS (Media)

type media struct{}

func (media) start(local string, st *handlerState, a attributes) *syndElement {
	switch local {
	case "content":
		if st.current == nil {
			break
		}
		url := a.get("url")
		isDefault := a.get("isDefault") != nil && *a.get("isDefault") == "true"
		medium := a.get("medium")
		mime := mimeType(a.get("type"), url)
		validMedia, validImage := false, false
		switch {
		case medium != nil && *medium == "audio":
			validMedia, mime = true, ptr("audio/*")
		case medium != nil && *medium == "video":
			validMedia, mime = true, ptr("video/*")
		case medium != nil && *medium == "image" && (mime == nil || (!hasPrefix(*mime, "audio/") && !hasPrefix(*mime, "video/"))):
			// Some publishers explicitly declare the audio file as an image.
			validImage, mime = true, ptr("image/*")
		case isMediaFile(mime):
			validMedia = true
		case isImageFile(mime):
			validImage = true
		default:
			// Workaround for broken feeds.
			validMedia, mime = st.current.Media == nil, ptr("audio/*")
		}
		if (st.current.Media == nil || isDefault) && url != nil && validMedia {
			var size int64
			if s := a.get("fileSize"); !empty(s) {
				if v, ok := javaParseLong(*s); ok {
					size = v
				}
			}
			var durationMs int32
			if s := a.get("duration"); !empty(s) {
				if v, ok := javaParseLong(*s); ok {
					durationMs = int32(v * 1000) // (int) TimeUnit.MILLISECONDS.convert(duration, SECONDS)
					if v > 9223372036854775807/1000 || v < -9223372036854775808/1000 {
						durationMs = int32(saturatedMillis(v))
					}
				}
			}
			m := newMedia(url, size, mime)
			if durationMs > 0 {
				m.Duration = durationMs
			}
			st.current.Media = m
		} else if url != nil && validImage {
			st.current.ImageURL = url
		}
	case "thumbnail":
		if url := a.get("url"); url != nil {
			if st.current != nil {
				st.current.ImageURL = url
			} else if st.feed.ImageURL == nil {
				st.feed.ImageURL = url
			}
		}
	case "description":
		return &syndElement{name: local, ns: media{}, atom: true, atomType: a.get("type")}
	}
	return &syndElement{name: local, ns: media{}}
}

// saturatedMillis is TimeUnit.SECONDS.toMillis's overflow saturation.
func saturatedMillis(v int64) int64 {
	if v > 0 {
		return 9223372036854775807
	}
	return -9223372036854775808
}

func (media) end(local string, st *handlerState) {
	if local == "description" {
		if st.contentBuf == nil {
			panic(errNull) // getContentBuf().toString() on null
		}
		c := st.contentBuf.String()
		if st.current != nil {
			st.current.setDescriptionIfLonger(&c)
		}
	}
}

// ---- Podcasting 2.0 (PodcastIndex)

type podcastIndex struct{}

func (podcastIndex) start(local string, st *handlerState, a attributes) *syndElement {
	switch local {
	case "funding":
		st.feed.Funding = append(st.feed.Funding, Funding{URL: a.get("url"), Content: ptr("")})
		st.currentFunding = &st.feed.Funding[len(st.feed.Funding)-1]
	case "chapters":
		if href := a.get("url"); !empty(href) {
			st.item().ChapterURL = href
		}
	case "socialInteract":
		if href := a.get("uri"); !empty(href) && st.current != nil {
			st.current.SocialInteractURL = href
		}
	case "transcript":
		href, typ := a.get("url"), a.get("type")
		if !empty(href) && !empty(typ) {
			st.item().setTranscriptURL(typ, href)
		}
	}
	return &syndElement{name: local, ns: podcastIndex{}}
}

func (podcastIndex) end(local string, st *handlerState) {
	if st.contentBuf == nil {
		return
	}
	c := st.contentBuf.String()
	if local == "funding" && st.currentFunding != nil && c != "" {
		st.currentFunding.Content = &c
	}
}

// ---- Podlove simple chapters (SimpleChapters)

type simpleChapters struct{}

func (simpleChapters) start(local string, st *handlerState, a attributes) *syndElement {
	if it := st.current; it != nil {
		if local == "chapters" {
			it.Chapters = []Chapter{}
		} else if local == "chapter" && !empty(a.get("start")) {
			if start, ok := parseTimeString(*a.get("start")); ok {
				if it.Chapters == nil {
					panic(errNull) // getChapters().add on a missing list
				}
				it.Chapters = append(it.Chapters, Chapter{Start: start, Title: a.get("title"), Link: a.get("href"), ImageURL: a.get("image")})
			}
		}
	}
	return &syndElement{name: local, ns: simpleChapters{}}
}

func (simpleChapters) end(string, *handlerState) {}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }
