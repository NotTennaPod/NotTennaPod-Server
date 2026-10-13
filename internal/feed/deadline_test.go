package feed

// Inputs that held a parse past its deadline in one long linear pass, not a
// quadratic one. Outputs are unchanged; these tests check the cancellation.

import (
	"context"
	"errors"
	"fmt"
	"html"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// A date as long as a feed's text that takes no weekday retry ran through
// every pass and format unchecked (a numeric pubDate of 16 MiB parsed 2 s
// past a 100 ms deadline, and its feed was a success). Every field's date
// counts its steps now; the date is still null (Android keeps the feed).
func TestLongDateStopsAtCancellation(t *testing.T) {
	date := strings.Repeat("1", 1<<20)
	const rss = `<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/"><channel><title>T</title><item><title>I</title>`
	for name, doc := range map[string]string{
		"pubDate":   rss + `<pubDate>DATE</pubDate></item></channel></rss>`,
		"dc:date":   rss + `<dc:date>DATE</dc:date></item></channel></rss>`,
		"published": `<feed xmlns="http://www.w3.org/2005/Atom"><title>T</title><entry><title>I</title><published>DATE</published></entry></feed>`,
	} {
		long := strings.Replace(doc, "DATE", date, 1)
		f, err := budgetParse(context.Background(), long)
		if err != nil || len(f.Items) != 1 || f.Items[0].PubDate != nil {
			t.Errorf("%s: %v %+v", name, err, f)
		}
		// Every check a parse of a short date makes passes; the long
		// date's steps stop it.
		ctx := &budgetCancelAfter{Context: context.Background(), checks: quadraticChecks(strings.Replace(doc, "DATE", "1", 1))}
		if _, err := budgetParse(ctx, long); !errors.Is(err, context.Canceled) {
			t.Errorf("%s cancelled during the date: %v", name, err)
		}
	}
}

// A parse whose context ends after its last check is no success: Parse
// checks once more before it returns. This document's parse checks after
// typeOf, at the SAX loop's start and at the title's conversion; the fourth
// check is the last.
func TestAnExpiredParseIsNoSuccess(t *testing.T) {
	const doc = `<rss version="2.0"><channel><title>T</title></channel></rss>`
	if n := quadraticChecks(doc); n != 4 {
		t.Fatalf("the parse checks its context %d times, want 4: update this test", n)
	}
	ctx := &budgetCancelAfter{Context: context.Background(), checks: 3}
	if f, err := budgetParse(ctx, doc); !errors.Is(err, context.Canceled) {
		t.Errorf("a parse that expired at its end: %v %+v", err, f)
	}
}

// encoding/xml reads a token in one call, so a text of many megabytes was
// read unchecked between the parse's checks (16 MiB of pubDate took 0.15 s
// past the deadline once the date itself was checked). The decoder's input
// checks the context as it is read.
func TestLongTextStopsAtCancellation(t *testing.T) {
	const doc = `<rss version="2.0"><channel><title>T</title><x>TEXT</x></channel></rss>`
	long := strings.Replace(doc, "TEXT", strings.Repeat("a", 4<<20), 1)
	if _, err := budgetParse(context.Background(), long); err != nil {
		t.Fatal(err)
	}
	ctx := &budgetCancelAfter{Context: context.Background(), checks: quadraticChecks(strings.Replace(doc, "TEXT", "a", 1))}
	if _, err := budgetParse(ctx, long); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled while reading a long text: %v", err)
	}
}

// Entity declarations were read and their character references replaced
// without a check: a 10 MiB declaration ran 0.7 s past a 300 ms deadline,
// and with a malformed tail the expired parse reported parser_exception,
// which a re-parse takes as final. Reading the prolog, the declarations'
// scan and their references now check the context.
func TestLongEntityDeclarationStopsAtCancellation(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	value := strings.Repeat("&#65;", 1<<20)
	for name, tail := range map[string]string{"valid": "", "invalid tail": "&#invalid;"} {
		doc := `<!DOCTYPE rss [<!ENTITY e "` + value + tail + `">]><rss><channel><title>T</title></channel></rss>`
		if _, err := budgetParse(context.Background(), doc); (tail == "") != (err == nil) || tail != "" && !budgetIsParserException(err) {
			t.Errorf("%s, not cancelled: %v", name, err)
		}
		if _, err := expandDocument(cancelled, doc); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: expanding: %v", name, err)
		}
		// The parse's first check, reading the prolog for the type, passes.
		ctx := &budgetCancelAfter{Context: context.Background(), checks: 1}
		if _, err := budgetParse(ctx, doc); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: parsing: %v", name, err)
		}
	}
	// One declaration's own work.
	err := entityDeclarations(`DOCTYPE rss [<!ENTITY e "`+strings.Repeat("x", 1<<20)+`">]`, &entityWork{ctx: cancelled}, func(string, string) {})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("scanning a long declaration: %v", err)
	}
	if _, err := replaceReferences(value, func(string) (string, bool, error) { return "", false, nil }, &entityWork{ctx: cancelled}); !errors.Is(err, context.Canceled) {
		t.Errorf("replacing a long declaration's references: %v", err)
	}
}

// A parse whose context ends after its last check is no parse error either.
// This document's parse checks after typeOf and at the SAX loop's start; the
// third check is the last, before the missing title would be reported.
func TestAnExpiredParseIsNoParseError(t *testing.T) {
	const doc = `<rss version="2.0"><channel></channel></rss>`
	if _, err := budgetParse(context.Background(), doc); !budgetIsParserException(err) {
		t.Fatalf("a feed without a title: %v", err)
	}
	if n := quadraticChecks(doc); n != 3 {
		t.Fatalf("the parse checks its context %d times, want 3: update this test", n)
	}
	ctx := &budgetCancelAfter{Context: context.Background(), checks: 2}
	if _, err := budgetParse(ctx, doc); !errors.Is(err, context.Canceled) {
		t.Errorf("a parse that expired at its end: %v", err)
	}
}

// One large tag (a link with 3.6 million attributes) was read whole, its
// attribute, name, value and decoding scans unchecked: discovery ran 1.4 s
// past a 100 ms deadline. The head scan counts its work within a tag, a
// value or a title, and stops there.
func TestOneLargeTagStopsAtCancellation(t *testing.T) {
	const link = `<link rel="alternate" type="application/rss+xml" href=`
	var attrs strings.Builder
	for i := range 200_000 {
		fmt.Fprintf(&attrs, " a%d=v", i)
	}
	for name, page := range map[string]string{
		"attributes": `<html><head>` + link + `"/feed"` + attrs.String() + `></head></html>`,
		"value":      `<html><head>` + link + `"/feed?` + strings.Repeat("&amp;", 400_000) + `"></head></html>`,
		"title":      `<html><head><title>` + strings.Repeat("&amp;", 400_000) + `</title>` + link + `"/feed"></head></html>`,
	} {
		links := 0
		count := func(map[string]string) bool { links++; return true }
		if h := jsoupParseHead(context.Background(), page, count); links != 1 || (name == "title") != (h.title != nil) {
			t.Fatalf("%s: %d links, title %v", name, links, h.title != nil)
		}
		// The scan's first check passes; the large token's own work stops it.
		links = 0
		if h := jsoupParseHead(&budgetCancelAfter{Context: context.Background(), checks: 1}, page, count); links != 0 || h.title != nil {
			t.Errorf("%s: read whole after cancellation (%d links, title %v)", name, links, h.title != nil)
		}
	}
}

// jsoupEntity hands no name of jsoupLongestEntity bytes or more to the
// entity table: none is an entity, with or without its semicolon.
func TestJsoupEntityLongNames(t *testing.T) {
	unguarded := func(name string, semicolon bool) (string, bool) {
		if v := html.UnescapeString("&" + name); v != "&"+name && utf8.RuneCountInString(v) == 1 {
			return v, true
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
	prefixes := []string{"", "CounterClockwiseContourIntegral", "ClockwiseContourIntegral", "amp", "AMP", "copy", "not", "notin", "nbsp",
		"AElig", "acE", "NotNestedGreaterGreater", "DoubleLongLeftRightArrow"}
	tails := []string{"", "x", "1", "é", "a1", "ä"}
	rng := rand.New(rand.NewPCG(20261009, 9))
	for range 50_000 {
		name := prefixes[rng.IntN(len(prefixes))]
		for len(name) < 24+rng.IntN(16) {
			name += tails[rng.IntN(len(tails))]
		}
		for _, semicolon := range []bool{false, true} {
			v, ok := jsoupEntity(name, semicolon)
			w, wok := unguarded(name, semicolon)
			if v != w || ok != wok {
				t.Fatalf("%q (semicolon %v): %q %v, the table says %q %v", name, semicolon, v, ok, w, wok)
			}
		}
	}
	if v, ok := jsoupEntity("CounterClockwiseContourIntegral", true); !ok || v != "∳" {
		t.Errorf("the longest entity: %q %v", v, ok)
	}
}

// One start tag's attributes were checked for duplicates, bound and
// collected without a check once the decoder had read the tag (3 million
// attributes ran 1.9 s past the deadline). The work within a token counts
// now. The test compares a tag of many attributes with a text of the same
// length, which the reading checks alike.
func TestManyAttributesStopAtCancellation(t *testing.T) {
	const doc = `<rss version="2.0"><channel><title>T</title><x BODY</x></channel></rss>`
	var attrs strings.Builder
	for i := range 100_000 {
		fmt.Fprintf(&attrs, ` a%d="v" xmlns:p%d="u"`, i, i)
	}
	tag := strings.Replace(doc, "BODY", attrs.String()+">", 1)
	text := strings.Replace(doc, "BODY", ">"+strings.Repeat("a", attrs.Len()), 1)
	if len(tag) != len(text) {
		t.Fatalf("lengths %d and %d", len(tag), len(text))
	}
	if _, err := budgetParse(context.Background(), tag); err != nil {
		t.Fatal(err)
	}
	// Every check a parse of the text makes passes; the attributes' own
	// work stops the tag's.
	ctx := &budgetCancelAfter{Context: context.Background(), checks: quadraticChecks(text)}
	if _, err := budgetParse(ctx, tag); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled after the tag was read: %v", err)
	}
}

// TagSoup read one tag name of 16 Mi characters with its scanner's checks,
// then made it a name, lower-cased it and looked it up, twice, without one
// (2.8 s past the deadline). Its names, attribute values, entity names,
// declarations and texts count their steps now. Each case is compared with a
// comment of the same length in bytes and in code units, which the reading
// and the scanner check alike and TagSoup drops.
func TestLongTagNameStopsAtCancellation(t *testing.T) {
	const doc = `<rss version="2.0"><channel><title><![CDATA[HTML]]></title></channel></rss>`
	const n = 1 << 20
	for name, html := range map[string]string{
		"tag name":        "<" + strings.Repeat("Σ", n) + ">",
		"end tag name":    "</" + strings.Repeat("Σ", n) + ">",
		"attribute name":  "<b " + strings.Repeat("Σ", n) + ">",
		"attribute value": "<b a=" + strings.Repeat("Σ", n) + ">",
		"entity name":     "&" + strings.Repeat("Σ", n) + ";",
		"declaration":     "<!DOCTYPE " + strings.Repeat("Σ", n) + ">",
	} {
		html += "<!---->"
		long := strings.Replace(doc, "HTML", html, 1)
		if _, err := budgetParse(context.Background(), long); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		ascii := len(html) - 2*n // every other character is "Σ": two bytes, one unit
		comment := strings.Replace(doc, "HTML", "<!--"+strings.Repeat("Σ", n)+strings.Repeat("x", ascii-7)+"-->", 1)
		if len(comment) != len(long) || utf16Len(comment) != utf16Len(long) {
			t.Fatalf("%s: lengths %d/%d and %d/%d", name, len(long), utf16Len(long), len(comment), utf16Len(comment))
		}
		// Every check a parse of the comment makes passes; the token's own
		// passes stop it.
		ctx := &budgetCancelAfter{Context: context.Background(), checks: quadraticChecks(comment)}
		if _, err := budgetParse(ctx, long); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: cancelled after it was read: %v", name, err)
		}
	}
}

// findLinksChecks is how many times findLinks checks its context on page.
func findLinksChecks(page string) int {
	ctx := &budgetCancelAfter{Context: context.Background(), checks: math.MaxInt}
	findLinks(ctx, page, "https://example.org/page", "https://example.org/page")
	return math.MaxInt - ctx.checks
}

// One discovery link's href was escaped and resolved without a check: a
// 32 MiB href took 4 s against a 100 ms deadline. Escaping counts its work,
// and a long reference is taken apart as url.Parse does, so that only a short
// one reaches net/url; a long href that dot segments shorten is still kept.
func TestOneLongHrefStopsAtCancellation(t *testing.T) {
	const link = `<link rel="alternate" type="application/rss+xml" `
	long := "/" + strings.Repeat("^", 2<<20)
	hostile := `<html><head>` + link + `href="` + long + `" data-x="/"></head></html>`
	baseline := `<html><head>` + link + `href="/" data-x="` + long + `"></head></html>`
	if len(hostile) != len(baseline) {
		t.Fatal("lengths")
	}
	if a, b := findLinksChecks(hostile), findLinksChecks(baseline); a < b+(6<<20)/jsoupCheckBytes {
		t.Errorf("the long href's escaping and resolution checked %d times, the page's reading %d", a-b, b)
	}
	// Dot segments shorten a reference of 7 MiB to /feed.
	dots := "/" + strings.Repeat("x/", 1<<20) + strings.Repeat("../", 1<<20) + "feed"
	links := findLinks(context.Background(), `<html><head>`+link+`href="`+dots+`"></head></html>`, "https://example.org/page", "https://example.org/page")
	if len(links) != 1 || links[0].URL != "https://example.org/feed" {
		t.Errorf("a long href that resolves short: %d links", len(links))
	}
	// Through discovery, with a 100 ms budget.
	page := `<html><head>` + link + `href="/` + strings.Repeat("^", 32<<20) + `"></head></html>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(page)) }))
	defer srv.Close()
	f := testFetcher()
	f.total = 100 * time.Millisecond
	start := time.Now()
	_, err := f.Discover(context.Background(), srv.URL, nil, time.Now())
	if fe := fetchErr(t, err); fe.Label != "time_limit" {
		t.Errorf("discovery: %+v", fe)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("discovery with a 100 ms budget took %s", d)
	}
}
