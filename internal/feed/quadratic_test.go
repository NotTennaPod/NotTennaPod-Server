package feed

// Hostile inputs that took quadratic time, some without stopping at the
// parse's deadline (in the date parse and elsewhere in internal/feed).
// Outputs are unchanged: the
// vectors and the corpus decide that; these tests check the time and the
// cancellation.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// quadraticWithin fails when f takes longer than limit. The limits are far
// above the linear time even under the race detector on a loaded machine, and
// far below the quadratic time the same input took before.
func quadraticWithin(t *testing.T, limit time.Duration, what string, f func()) {
	t.Helper()
	start := time.Now()
	f()
	if d := time.Since(start); d > limit {
		t.Errorf("%s took %s", what, d)
	}
}

// quadraticChecks is how many times a parse of doc checks its context.
func quadraticChecks(doc string) int {
	ctx := &budgetCancelAfter{Context: context.Background(), checks: math.MaxInt}
	budgetParse(ctx, doc)
	return math.MaxInt - ctx.checks
}

// A date of many "word, " prefixes took DateUtils' weekday retry quadratic
// time (Android's too: 120 KB took 22 s here; linear now, see
// TestDateWeekdayRetryIsLinear). Every date field stops at the parse's
// cancellation; before, a refresh went on past its deadline and kept its
// worker. The retry's result is unchanged.
func TestQuadraticDateStopsAtCancellation(t *testing.T) {
	const plain = "Sat, 09 Oct 2021 10:00:00 +0000"
	const want = 1633773600000
	date := strings.Repeat("a, ", 1000) + plain
	if ms, ok := parseDate(context.Background(), date, corpusNow); !ok || ms != want {
		t.Fatalf("parseDate after 1,000 prefixes = %d, %v", ms, ok)
	}
	if _, ok := parseDate(context.Background(), strings.Repeat("Sat, ", 50)+"nothing", corpusNow); ok {
		t.Fatal("a date of weekdays only parsed")
	}
	const rss = `<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/"><channel><title>T</title><item><title>I</title>`
	for name, doc := range map[string]string{
		"pubDate":   rss + `<pubDate>DATE</pubDate></item></channel></rss>`,
		"dc:date":   rss + `<dc:date>DATE</dc:date></item></channel></rss>`,
		"published": `<feed xmlns="http://www.w3.org/2005/Atom"><title>T</title><entry><title>I</title><published>DATE</published></entry></feed>`,
		"updated":   `<feed xmlns="http://www.w3.org/2005/Atom"><title>T</title><entry><title>I</title><updated>DATE</updated></entry></feed>`,
	} {
		hostile := strings.Replace(doc, "DATE", date, 1)
		f, err := budgetParse(context.Background(), hostile)
		if err != nil || len(f.Items) != 1 || f.Items[0].PubDate == nil || *f.Items[0].PubDate != want {
			t.Errorf("%s: %v %+v", name, err, f)
		}
		// Every check a parse of the plain date makes passes; the hostile
		// date's first retry stops it.
		ctx := &budgetCancelAfter{Context: context.Background(), checks: quadraticChecks(strings.Replace(doc, "DATE", plain, 1))}
		if _, err := budgetParse(ctx, hostile); !errors.Is(err, context.Canceled) {
			t.Errorf("%s cancelled during the date: %v", name, err)
		}
	}
}

// Stray '&'s in a document that declares an entity each looked for the next
// ';' to the end of the document (2 MB took 53 s, 60 MB would take hours) and
// never checked the context. The scan is linear and stops at cancellation;
// the result is the same parser exception.
func TestQuadraticStrayAmpersands(t *testing.T) {
	const prolog = `<?xml version="1.0"?><!DOCTYPE rss [<!ENTITY e "x">]><rss version="2.0"><channel><title>`
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, title := range map[string]string{
		"no semicolon":         strings.Repeat("&", 2<<20),
		"one semicolon at end": strings.Repeat("&", 2<<20) + ";",
	} {
		doc := prolog + title + `</title></channel></rss>`
		quadraticWithin(t, 10*time.Second, name, func() {
			if _, err := budgetParse(context.Background(), doc); !budgetIsParserException(err) {
				t.Errorf("%s: %v", name, err)
			}
		})
		if _, err := expandDocument(cancelled, doc); !errors.Is(err, context.Canceled) {
			t.Errorf("%s cancelled: %v", name, err)
		}
	}
	// Uses among stray '&'s still expand.
	f, err := budgetParse(context.Background(), prolog+`&e;&#65;&e;&amp;&e;</title></channel></rss>`)
	if err != nil || *f.Title != "xAx&x" {
		t.Fatalf("uses: %v %v", err, f)
	}
}

// TagSoup walks the whole stack for an end tag it cannot find and for each
// block start under many restartable elements: quadratic time, on Android
// too. Its context check counted only characters, so a title could hold the
// parse long past its deadline (8 s past it for 27 MB, 15 s for 15 MB, more
// for deeper stacks). Stack steps are counted and checked now.
func TestQuadraticHTMLStopsAtCancellation(t *testing.T) {
	const rss = `<rss version="2.0"><channel><title><![CDATA[TITLE]]></title></channel></rss>`
	for name, html := range map[string]string{
		// Both under 4,096 characters: the scanner checks only at its start.
		"end tags not open":    strings.Repeat("<div>", 600) + strings.Repeat("</span>", 150),
		"blocks in formatting": strings.Repeat("<b>", 300) + strings.Repeat("<p>", 300),
	} {
		doc := strings.Replace(rss, "TITLE", html, 1)
		if _, err := budgetParse(context.Background(), doc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// Every check a parse of a one-letter title makes passes; the stack
		// steps' first check stops it.
		ctx := &budgetCancelAfter{Context: context.Background(), checks: quadraticChecks(strings.Replace(rss, "TITLE", "x", 1))}
		if _, err := budgetParse(ctx, doc); !errors.Is(err, context.Canceled) {
			t.Errorf("%s cancelled in the stack walks: %v", name, err)
		}
	}
}

// Html's appendNewlines counted every newline at the end for each block, so
// many blocks after many line breaks took quadratic time (440 KB of title
// took 2 s). It counts up to the margin, which decides the same.
func TestQuadraticBlocksAfterLineBreaks(t *testing.T) {
	const n = 200_000
	c := &htmlConverter{margin: 2, text: []uint16{'x'}}
	for range n {
		c.text = append(c.text, '\n')
	}
	quadraticWithin(t, 5*time.Second, "200,000 blocks after 200,000 line breaks", func() {
		for range n {
			c.appendNewlines(c.margin)
		}
	})
	if len(c.text) != n+1 {
		t.Fatalf("newlines added after %d: %d", n, len(c.text)-1-n)
	}
	for _, tc := range []struct{ in, compact, legacy string }{
		{"x<br><p>y</p>", "x\ny\n", "x\n\ny\n\n"},
		{"x<br><br><br><p>y</p><p>z</p>", "x\n\n\ny\nz\n", "x\n\n\ny\n\nz\n\n"},
	} {
		if got := htmlText(context.Background(), tc.in, true); got != tc.compact {
			t.Errorf("compact %q = %q, want %q", tc.in, got, tc.compact)
		}
		if got := htmlText(context.Background(), tc.in, false); got != tc.legacy {
			t.Errorf("legacy %q = %q, want %q", tc.in, got, tc.legacy)
		}
	}
}

// TagSoup looked every attribute up among the tag's earlier ones: one tag of
// 40,000 attributes took 2.3 s. Many are indexed now; a repeated name still
// replaces the value in its place.
func TestQuadraticManyAttributes(t *testing.T) {
	names := make([]string, 200_000)
	for i := range names {
		names[i] = fmt.Sprintf("a%d", i)
	}
	e := &tagsoupElement{}
	quadraticWithin(t, 5*time.Second, "200,000 attributes", func() {
		for _, name := range names {
			e.setAttribute(name, []uint16{'1'})
		}
	})
	e.setAttribute("a3", []uint16{'2'})
	e.setAttribute("a199999", []uint16{'3'})
	e.setAttribute("xmlns:x", []uint16{'4'})
	if len(e.attrs) != len(names) || e.attrs[3].name != "a3" || e.attrs[3].value[0] != '2' ||
		e.attribute("a199999")[0] != '3' || e.attribute("xmlns:x") != nil || e.attribute("b") != nil {
		t.Errorf("attributes: %d, %+v", len(e.attrs), e.attrs[:4])
	}
	// Through Html: <font color="-"> throws (htmlErrException), the color
	// found after 20,000 other attributes.
	var b strings.Builder
	b.WriteString("<font")
	for _, name := range names[:20_000] {
		b.WriteString(" " + name + "=v")
	}
	b.WriteString(` color="-">x</font>`)
	defer func() {
		if r := recover(); r != htmlErrException {
			t.Errorf("color after 20,000 attributes: %v", r)
		}
	}()
	htmlText(context.Background(), b.String(), true)
}

// Each competing description measured the current one's UTF-16 length again
// (Java keeps a string's length): 40,000 short descriptions after a long one
// took 40 s. The length is kept now; the comparison is the same.
func TestQuadraticDescriptionCandidates(t *testing.T) {
	long := strings.Repeat("d", 1<<20)
	it := &Item{}
	it.setDescriptionIfLonger(&long)
	quadraticWithin(t, 5*time.Second, "20,000 descriptions after 1 MiB", func() {
		for range 20_000 {
			it.setDescriptionIfLonger(ptr("short"))
		}
	})
	if it.Description != &long {
		t.Fatal("a shorter description replaced the long one")
	}
	// UTF-16 lengths decide: "😀" is two units, as long as "ab".
	it = &Item{}
	for _, s := range []string{"😀", "ab", "abc", "ab😀", "abcd"} {
		it.setDescriptionIfLonger(ptr(s))
	}
	if *it.Description != "ab😀" {
		t.Errorf("by UTF-16 length: %q", *it.Description)
	}
	// A description set elsewhere is measured anew.
	it.Description = ptr("abcdefgh")
	it.setDescriptionIfLonger(ptr("abcdefg"))
	if *it.Description != "abcdefgh" {
		t.Errorf("after another set: %q", *it.Description)
	}
}

// Atom's feed author is every author name joined by ", ", which Java (and
// the port) concatenated anew for each name: 40,000 authors took 1.3 s. The
// names are appended in place; another handler's author is still extended.
func TestQuadraticFeedAuthors(t *testing.T) {
	st := newHandlerState("atom", 0)
	quadraticWithin(t, 5*time.Second, "200,000 authors", func() {
		for range 200_000 {
			st.addFeedAuthor("a")
		}
	})
	if got := *st.feed.Author; len(got) != 3*200_000-2 || !strings.HasPrefix(got, "a, a, ") || strings.HasSuffix(got, ", ") {
		t.Fatalf("200,000 authors: %d bytes", len(got))
	}
	f, err := budgetParse(context.Background(), `<feed xmlns="http://www.w3.org/2005/Atom" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">`+
		`<title>T</title><author><name>A</name></author><author><name>B</name></author><itunes:author>I</itunes:author>`+
		`<author><name>C</name></author><author><name> D </name></author></feed>`)
	if err != nil || *f.Author != "I, C, D" {
		t.Fatalf("authors: %v %q", err, *f.Author)
	}
}

// A run of '&' before one ';' gives each '&' a candidate entity name as long
// as the rest of the run. Once more than a few entities are declared, every
// candidate up to the longest name was hashed for its lookup: 11 entities
// and a run of 1 MiB took 24 s, 4 MiB 327 s.
// A candidate is looked up only when a declared name has its length.
func TestQuadraticEntityCandidates(t *testing.T) {
	const n = 1 << 20
	var decls strings.Builder
	decls.WriteString(`<!ENTITY ` + strings.Repeat("e", n) + ` "v">`)
	for i := range 10 {
		fmt.Fprintf(&decls, `<!ENTITY a%d "w">`, i)
	}
	doc := `<!DOCTYPE rss [` + decls.String() + `]><rss version="2.0"><channel><title>t</title><description>` +
		strings.Repeat("&", n) + `;</description></channel></rss>`
	quadraticWithin(t, 8*time.Second, "a 1 MiB run of '&' with 11 entities", func() {
		if _, err := budgetParse(context.Background(), doc); !budgetIsParserException(err) {
			t.Errorf("the run: %v", err)
		}
	})
	// Uses still expand, at every declared length.
	ok := `<!DOCTYPE rss [` + decls.String() + `]><rss version="2.0"><channel><title>&a3;&&a7;` +
		`</title></channel></rss>`
	if _, err := budgetParse(context.Background(), ok); !budgetIsParserException(err) {
		t.Errorf("a stray '&' among uses: %v", err)
	}
	f, err := budgetParse(context.Background(), strings.Replace(ok, "&&a7;", "&a7;&"+strings.Repeat("e", n)+";", 1))
	if err != nil || *f.Title != "wwv" {
		t.Fatalf("uses: %v", err)
	}
}

// expandDocumentChecks is how many times expandDocument checks its context
// on doc.
func expandDocumentChecks(doc string) int {
	ctx := &budgetCancelAfter{Context: context.Background(), checks: math.MaxInt}
	expandDocument(ctx, doc)
	return math.MaxInt - ctx.checks
}

// The candidates of a run of '&' are filtered in one pass back from its
// ';', as far as the longest declared name, and that pass is counted: a name
// of 600,000 bytes and a run's '&' as far from its ';' read 600 KB. The
// baseline is the same document with the '&' next to the ';'; both stay
// under the 1,024 steps at which the scan itself checks.
func TestEntityCandidateFilterIsCounted(t *testing.T) {
	const k = 600_000
	doc := `<!DOCTYPE rss [<!ENTITY ` + strings.Repeat("n", k) + ` "v">]><rss version="2.0"><channel><title>t</title>` +
		`<description>RUN</description></channel></rss>`
	hostile := strings.Replace(doc, "RUN", "&"+strings.Repeat("y", k)+";", 1)
	baseline := strings.Replace(doc, "RUN", strings.Repeat("y", k)+"&;", 1)
	ctx := &budgetCancelAfter{Context: context.Background(), checks: expandDocumentChecks(baseline)}
	if _, err := expandDocument(ctx, hostile); !errors.Is(err, context.Canceled) {
		t.Errorf("the filter's pass was not counted: %v", err)
	}
}

// Attributes' expanded names (namespace, local) must be unique. The namespace was the URI itself,
// hashed for every attribute: a 1 MiB URI and 12,000 prefixed attributes took 1.6 s, and the time
// grew with their product. Each URI is numbered once, when declared.
func TestQuadraticNamespacedAttributes(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<rss version="2.0" xmlns:p="` + strings.Repeat("u", 2<<20) + `"><channel><title>t</title><x`)
	for i := range 40_000 {
		fmt.Fprintf(&b, ` p:a%d=""`, i)
	}
	b.WriteString(`/></channel></rss>`)
	doc := b.String()
	quadraticWithin(t, 5*time.Second, "40,000 attributes in a namespace of 2 MiB", func() {
		if _, err := budgetParse(context.Background(), doc); err != nil {
			t.Errorf("the attributes: %v", err)
		}
	})
	// The same URI under two prefixes is one namespace; other URIs, no
	// namespace and the default namespace are others.
	for doc, dup := range map[string]bool{
		`<x xmlns:p="u" xmlns:q="u" p:a="1" q:a="2"/>`:                            true,
		`<x xmlns:p="u" p:a="1"><y xmlns:q="u" p:a="1" q:a="2"/></x>`:             true,
		`<x xmlns:p="u" xmlns:q="v" p:a="1" q:a="2"/>`:                            false,
		`<x xmlns:p="u" a="1" p:a="2"/>`:                                          false,
		`<x xmlns="u" xmlns:p="u" a="1" p:a="2"/>`:                                false,
		`<x xmlns:p="u" p:a="1"><y xmlns:p="v" p:a="1" xmlns:q="u" q:a="2"/></x>`: false,
		`<x xmlns:p="u"><y xmlns:p="v" xmlns:q="v" p:a="1" q:a="2"/></x>`:         true,
		`<x xml:lang="en" lang="en"/>`:                                            false,
	} {
		feed := `<rss version="2.0"><channel><title>t</title>` + doc + `</channel></rss>`
		_, err := budgetParse(context.Background(), feed)
		if dup != budgetIsParserException(err) {
			t.Errorf("%s: %v", doc, err)
		}
	}
}

// The length guard leaves one family super-linear: names of many lengths,
// each looked up once per run of '&' as long as the longest. Declaring every
// length in [w/2, w) and repeating runs of w '&' makes each run look up as
// many bytes as the declarations hold: (n/2)^1.5 bytes for a document of n.
// The candidates of a run are filtered in one pass now, so the work counted
// grows with the document: here the checks stay within twice those of one
// check per 1,024 bytes.
func TestEntityCandidateWorkIsLinear(t *testing.T) {
	const n = 4 << 20
	w := 2
	for 3*w*w/8 < n/2 {
		w++
	}
	var b strings.Builder
	b.WriteString(`<!DOCTYPE rss [`)
	for l := w / 2; l < w; l++ {
		b.WriteString(`<!ENTITY ` + strings.Repeat("n", l) + ` "v">`)
	}
	b.WriteString(`]><rss version="2.0"><channel><title>t</title><description>`)
	for b.Len() < n-w {
		b.WriteString(strings.Repeat("&", w) + ";")
	}
	b.WriteString(`</description></channel></rss>`)
	doc := b.String()
	if checks, limit := expandDocumentChecks(doc), 2*len(doc)/checkEvery; checks > limit {
		t.Errorf("%d checks for %d bytes, more than %d", checks, len(doc), limit)
	}
	// A declared name among the runs is still found, and expands.
	use := strings.Replace(doc, strings.Repeat("&", w)+";", "&&"+strings.Repeat("n", w/2)+";", 1)
	if out, err := expandDocument(context.Background(), use); err != nil || !strings.Contains(out, "<description>&v") {
		t.Errorf("a use among the runs: %v", err)
	}
}
