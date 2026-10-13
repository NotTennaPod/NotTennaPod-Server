// Package feed fetches and parses podcast feeds exactly as Android AntennaPod
// 3.12.1 does (PROTOCOL §9.2): its parser classes
// (parser/feed: SyndHandler, HandlerState, the namespace handlers, DateUtils,
// DurationParser, MimeTypeUtils) and FeedParserTask's checks, ported rule for
// rule and verified against Android's own output on the emulator.
package feed

import (
	"encoding/json"
	"strconv"
)

// Feed is a parsed feed: Android's Feed and FeedHandlerResult fields.
type Feed struct {
	Type          string // "rss" or "atom"; "" (null) for RSS 0.91 and 0.92
	Identifier    *string
	Title         *string
	Description   *string
	Author        *string
	Language      *string
	Link          *string
	ImageURL      *string
	Funding       []Funding
	Paged         bool
	NextPageURL   *string
	RedirectURL   *string           // itunes:new-feed-url
	AlternateURLs map[string]string // podlove alternate feeds: URL → title
	// An alternate link without href: Android's HashMap holds it under the
	// null key, with its title (null too when the title is empty).
	AlternateNull    *string
	HasAlternateNull bool
	Items            []*Item
}

// Funding is a feed's payment link (FeedFunding).
type Funding struct {
	URL     *string
	Content *string
}

// Item is a parsed episode (FeedItem).
type Item struct {
	GUID              *string
	Title             *string
	Description       *string
	Link              *string
	PubDate           *int64 // ms since the epoch
	ImageURL          *string
	PaymentLink       *string
	Media             *Media
	Chapters          []Chapter // nil when the item has no psc:chapters
	ChapterURL        *string
	TranscriptURL     *string
	TranscriptType    *string
	SocialInteractURL *string

	// The UTF-16 length of the description, while descriptionOf is it: Java
	// keeps a string's length, so comparing many candidates to a long
	// description stays linear (setDescriptionIfLonger).
	descriptionOf  *string
	descriptionLen int
}

// Media is an item's enclosure (FeedMedia).
type Media struct {
	URL      *string
	Size     int64
	MIMEType *string
	Duration int32 // ms, Android keeps an int
}

// Chapter is a Podlove simple chapter.
type Chapter struct {
	Start    int64 // ms
	Title    *string
	Link     *string
	ImageURL *string
}

// setDescriptionIfLonger keeps the longer description by UTF-16 length.
func (it *Item) setDescriptionIfLonger(d *string) {
	if d == nil {
		return
	}
	if it.Description != nil && it.descriptionOf != it.Description {
		it.descriptionOf, it.descriptionLen = it.Description, utf16Len(*it.Description)
	}
	if n := utf16Len(*d); it.Description == nil || it.descriptionLen < n {
		it.Description, it.descriptionOf, it.descriptionLen = d, d, n
	}
}

// transcript types and priorities (TranscriptType).
func transcriptType(mime string) (int, string) {
	switch mime {
	case "application/json":
		return 4, "application/json"
	case "text/vtt":
		return 3, "text/vtt"
	case "application/srt", "application/srr", "application/x-subrip":
		return 2, "application/srt"
	}
	return 0, ""
}

// setTranscriptURL is FeedItem.updateTranscriptPreferredFormat.
func (it *Item) setTranscriptURL(typ, url *string) {
	if typ == nil || *typ == "" || url == nil || *url == "" {
		return
	}
	p, canonical := transcriptType(*typ)
	prev := 0
	if it.TranscriptType != nil {
		prev, _ = transcriptType(*it.TranscriptType)
	}
	if p > prev {
		u, c := *url, canonical
		it.TranscriptURL, it.TranscriptType = &u, &c
	}
}

// ---- canonical JSON (the vectors' schema: GoldenFeedExportTest)

func str(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// Object returns the feed in the vectors' JSON schema. Java longs a feed can
// make arbitrarily large are decimal strings (PROTOCOL §9.2).
func (f *Feed) Object() map[string]any {
	funding := []any{}
	for _, x := range f.Funding {
		funding = append(funding, map[string]any{"url": str(x.URL), "content": str(x.Content)})
	}
	alternates := map[string]any{}
	for u, t := range f.AlternateURLs {
		alternates[u] = t
	}
	items := []any{}
	for _, it := range f.Items {
		items = append(items, it.Object())
	}
	var typ any
	if f.Type != "" {
		typ = f.Type
	}
	obj := map[string]any{
		"type": typ, "identifier": str(f.Identifier), "title": str(f.Title), "description": str(f.Description),
		"author": str(f.Author), "language": str(f.Language), "link": str(f.Link), "image_url": str(f.ImageURL),
		"funding": funding, "paged": f.Paged, "next_page_url": str(f.NextPageURL), "redirect_url": str(f.RedirectURL),
		"alternate_feed_urls": alternates, "items": items,
	}
	if f.HasAlternateNull {
		obj["alternate_feed_url_null"] = str(f.AlternateNull)
	}
	return obj
}

// Object returns the item in the vectors' JSON schema.
func (it *Item) Object() map[string]any {
	var pub any
	if it.PubDate != nil {
		pub = *it.PubDate
	}
	var media any
	if it.Media != nil {
		media = map[string]any{"url": str(it.Media.URL), "size": strconv.FormatInt(it.Media.Size, 10),
			"mime_type": str(it.Media.MIMEType), "duration": int64(it.Media.Duration)}
	}
	var chapters any
	if it.Chapters != nil {
		list := []any{}
		for _, c := range it.Chapters {
			list = append(list, map[string]any{"start": strconv.FormatInt(c.Start, 10), "title": str(c.Title),
				"link": str(c.Link), "image_url": str(c.ImageURL)})
		}
		chapters = list
	}
	return map[string]any{
		"guid": str(it.GUID), "title": str(it.Title), "description": str(it.Description), "link": str(it.Link),
		"pub_date": pub, "image_url": str(it.ImageURL), "payment_link": str(it.PaymentLink), "media": media,
		"chapters": chapters, "chapter_url": str(it.ChapterURL), "transcript_url": str(it.TranscriptURL),
		"transcript_type": str(it.TranscriptType), "social_interact_url": str(it.SocialInteractURL),
	}
}

// MarshalJSON encodes the feed in the vectors' schema.
func (f *Feed) MarshalJSON() ([]byte, error) { return json.Marshal(f.Object()) }
