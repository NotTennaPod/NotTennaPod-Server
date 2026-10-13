package feed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

// Hostile inputs.

func hostileParse(ctx context.Context, doc string) (*Feed, error) {
	return Parse(ctx, []byte(doc), "https://feeds.example.invalid/hostile.xml", time.UnixMilli(corpusNow))
}

// An unused entity is never expanded, however large it would be (Expat
// expands only at use), and declaring it costs no more than its text.
func TestHostileUnusedEntityIsNotExpanded(t *testing.T) {
	doc := `<!DOCTYPE rss [<!ENTITY a "` + strings.Repeat("x", 64<<10) + `"><!ENTITY b "` + strings.Repeat("&a;", 1024) +
		`"><!ENTITY u "&undefined;">]><rss><channel><title>ok</title></channel></rss>`
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f, err := hostileParse(context.Background(), doc)
	runtime.ReadMemStats(&after)
	if err != nil || f.Title == nil || *f.Title != "ok" {
		t.Fatalf("unused entities: %v %v", f, err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 16<<20 {
		t.Errorf("parsing a %d-byte document allocated %d bytes", len(doc), alloc)
	}
	// Used, the same entity is over the budget before any text is built.
	used := strings.Replace(doc, "<title>ok</title>", "<title>&b;</title>", 1)
	if _, err := hostileParse(context.Background(), used); !budgetIsParserException(err) {
		t.Errorf("a used 64 MiB entity: %v", err)
	}
}

// An entity's replacement text is content: its markup makes elements, as in
// Expat, so Android's handlers see the item.
func TestHostileEntityMarkupIsContent(t *testing.T) {
	doc := `<!DOCTYPE rss [<!ENTITY e "<item><title>Episode</title></item>">]><rss><channel><title>Feed</title>&e;</channel></rss>`
	f, err := hostileParse(context.Background(), doc)
	if err != nil || len(f.Items) != 1 || *f.Items[0].Title != "Episode" {
		t.Fatalf("entity markup: %+v %v", f, err)
	}
	// Markup in an attribute value is malformed (Android's type check or its
	// parser refuses it).
	doc = `<!DOCTYPE rss [<!ENTITY e "<b>">]><rss x="&e;"><channel><title>Feed</title></channel></rss>`
	if _, err := hostileParse(context.Background(), doc); err == nil {
		t.Error("markup in an attribute was accepted")
	}
}

// A non-ASCII letter whose lower case is longer must not move offsets in the
// website scan (it used to panic).
func TestHostileWebsiteUnicodeCase(t *testing.T) {
	doc := `<html><head><script data-x="` + strings.Repeat("K", 100) + `"></script><title>T</title></head></html>`
	_, err := hostileParse(context.Background(), doc)
	var pe *ParseError
	if !errors.As(err, &pe) || pe.Detail != `Website title: "T"` {
		t.Errorf("website with Kelvin signs: %v", err)
	}
}

// Thousands of raw-text elements are scanned in linear time, and a cancelled
// context stops the scan.
func TestHostileWebsiteScanIsLinearAndCancellable(t *testing.T) {
	doc := `<html><head>` + strings.Repeat(`<SCRIPT></SCRIPT>`, 12_000) + `</head></html>`
	start := time.Now()
	if _, err := hostileParse(context.Background(), doc); err == nil {
		t.Fatal("a website parsed as a feed")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("12,000 script elements took %s", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hostileParse(ctx, doc); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
}

// Discovery has one deadline for the fetch and the parse; a server that never
// answers is cut off by it. (The parse's own cancellation is tested above.)
func TestHostileDiscoverDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	f := testFetcher()
	f.total = 100 * time.Millisecond
	start := time.Now()
	_, err := f.Discover(context.Background(), srv.URL, nil, time.UnixMilli(corpusNow))
	if fe := fetchErr(t, err); fe.Label != "time_limit" {
		t.Errorf("discovery past its deadline: %s", fe.Label)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("discovery returned after %s", d)
	}
}

// A malformed response quoting credentials never reaches the error's text.
func TestHostileTransportErrorsAreSanitized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprintf(conn, "HTTP/1.1 https://user:password@example.org/feed?token=secret nope\r\n\r\n")
	}))
	defer srv.Close()
	_, err := testFetcher().Fetch(context.Background(), FetchRequest{URL: srv.URL + "/feed?token=secret"})
	if err == nil || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "secret") {
		t.Errorf("error text: %v", err)
	}
}

// Validators go to the hop they belong to, after a temporary redirect too.
func TestHostileValidatorsAfterRedirect(t *testing.T) {
	var sent []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/feed", http.StatusFound)
			return
		}
		sent = append(sent, r.Header.Get("If-None-Match"))
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fmt.Fprint(w, `<rss/>`)
	}))
	defer srv.Close()
	f := testFetcher()
	first, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/start"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/start", Validators: &Validators{Key: first.RepresentationKey, ETag: first.ETag}})
	if err != nil || !second.NotModified || strings.Join(sent, ",") != `,"v1"` {
		t.Errorf("conditional fetch after a redirect: %v %v, sent %q", err, second, sent)
	}
}

// A stored response is never returned truncated: at the body limit with large
// validators it opens whole, and an oversized header cannot be sealed.
func TestHostileStoredResponseBounds(t *testing.T) {
	box, err := sealed.New(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	r := StoredResponse{Body: bytes.Repeat([]byte("x"), maxBody), ETag: strings.Repeat("e", 128<<10)}
	b, err := r.Seal(box, "account", "podcast")
	if err != nil {
		t.Fatal(err)
	}
	got, err := OpenStoredResponse(box, "account", "podcast", b)
	if err != nil || len(got.Body) != maxBody || got.ETag != r.ETag {
		t.Errorf("opened %d bytes, %v", len(got.Body), err)
	}
	r.ETag = strings.Repeat("e", maxStoredHeader)
	if _, err := r.Seal(box, "account", "podcast"); err == nil {
		t.Error("an oversized header was sealed")
	}
}

// Expat's namespace constraints.
func TestHostileNamespaceConstraints(t *testing.T) {
	for _, doc := range []string{
		`<rss xmlns:a="urn:x" xmlns:b="urn:x" a:x="1" b:x="2"><channel><title>Feed</title></channel></rss>`,
		`<rss xmlns:xml="urn:wrong"><channel><title>Feed</title></channel></rss>`,
		`<rss xmlns:p="http://www.w3.org/XML/1998/namespace"><channel><title>Feed</title></channel></rss>`,
		`<rss xmlns:p=""><channel><title>Feed</title></channel></rss>`,
	} {
		if _, err := hostileParse(context.Background(), doc); !budgetIsParserException(err) {
			t.Errorf("%s: %v", doc, err)
		}
	}
}
