package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testdata/discovery: NotTennaPod-iOS's discovery pages and their outcomes,
// checked there against Android 3.12.1's own classes (expected.json says how).
type discoverExpected struct {
	Base  string `json:"base"`
	Pages []struct {
		Page        string `json:"page"`
		Requested   string `json:"requested_url"`
		Android     discoverOutcome
		Recommended *discoverOutcome `json:"recommended_rfc3986"`
		Defect      string           `json:"android_defect"`
	} `json:"pages"`
	URLInputs []struct{ Input, Prepared string } `json:"url_inputs"`
}

type discoverOutcome struct {
	Outcome string `json:"outcome"`
	URL     string `json:"url"`
	Type    string `json:"type"`
	Items   []struct{ URL, Label string }
	Dialog  struct{ Message string }
}

func discoverLoad(t *testing.T) discoverExpected {
	t.Helper()
	b, err := os.ReadFile("testdata/discovery/expected.json")
	if err != nil {
		t.Fatal(err)
	}
	var x discoverExpected
	if err := json.Unmarshal(b, &x); err != nil {
		t.Fatal(err)
	}
	return x
}

func TestPrepareURL(t *testing.T) {
	for _, c := range discoverLoad(t).URLInputs {
		if got, err := PrepareURL(c.Input); err != nil || got != c.Prepared {
			t.Errorf("PrepareURL(%q) = %q, %v; want %q", c.Input, got, err, c.Prepared)
		}
	}
	for in, want := range map[string]string{
		"antennapod-subscribe://example.com/feed":                                     "http://example.com/feed",
		"https://antennapod.org/deeplink/subscribe?url=https%3A%2F%2Fexample.com%2Ff": "https://example.com/f",
		"https://www.subscribeonandroid.com/example.com/feed.xml":                     "https://example.com/feed.xml",
		"pcast:example.com/x": "http://example.com/x",
	} {
		if got, err := PrepareURL(in); err != nil || got != want {
			t.Errorf("PrepareURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	// Android crashes on these.
	for _, in := range []string{"itpc", "https://antennapod.org/deeplink/subscribe?x=1", "https://antennapod.org/deeplink/subscribe?url=%zz"} {
		if _, err := PrepareURL(in); !errors.Is(err, ErrAddress) {
			t.Errorf("PrepareURL(%q): %v, want ErrAddress", in, err)
		}
	}
}

func TestDiscoverPages(t *testing.T) {
	x := discoverLoad(t)
	routes := map[string]string{} // request URI → page
	for _, p := range x.Pages {
		u, _ := url.Parse(p.Requested)
		routes[u.RequestURI()] = p.Page
	}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, ok := routes[r.URL.RequestURI()]
		if !ok {
			http.NotFound(w, r)
			return
		}
		b, err := os.ReadFile(filepath.Join("testdata/discovery", page))
		if err != nil {
			t.Error(err)
		}
		w.Write([]byte(strings.ReplaceAll(string(b), "{BASE}", srv.URL)))
	}))
	defer srv.Close()
	base := func(s string) string { return strings.ReplaceAll(s, x.Base, srv.URL) }
	f := testFetcher()
	// Android's order (by hash code) depends on the port: TestFindLinksOrder
	// checks it at the fixtures' own base; here the links are compared as sets.
	sorted := func(xs []string) string { slices.Sort(xs); return strings.Join(xs, "\n  ") }
	for _, p := range x.Pages {
		res, err := f.Discover(context.Background(), base(p.Requested), nil, time.UnixMilli(corpusNow))
		if err != nil {
			t.Errorf("%s: %v", p.Page, err)
			continue
		}
		want := p.Android
		if p.Recommended != nil {
			want = *p.Recommended // PROTOCOL §9.2: RFC 3986 resolution
		}
		if p.Defect != "" {
			want = discoverOutcome{Outcome: "error_dialog"} // PROTOCOL §9.2: no link back to the page itself
		}
		var links []string
		for _, l := range res.Links {
			links = append(links, l.URL+" | "+l.Title)
		}
		switch want.Outcome {
		case "open_single":
			if len(res.Links) != 1 || res.Links[0].URL != base(want.URL) {
				t.Errorf("%s: links %q, want only %s", p.Page, links, base(want.URL))
			}
		case "chooser":
			var w []string
			for _, it := range want.Items {
				w = append(w, base(it.URL)+" | "+base(it.Label))
			}
			if g, w := sorted(links), sorted(w); g != w {
				t.Errorf("%s: links\n  %s\nwant\n  %s", p.Page, g, w)
			}
		case "preview":
			typ := map[string]string{"rss20": "rss", "atom": "atom"}[want.Type]
			if res.Feed == nil || res.Feed.Type != typ {
				t.Errorf("%s: feed %v (%v), want type %s", p.Page, res.Feed, res.Err, typ)
			}
		case "error_dialog":
			_, reason, _ := strings.Cut(want.Dialog.Message, "Technical reason: \n")
			reason, _, _ = strings.Cut(reason, " \n\nFile URL")
			if p.Page == "plain.txt" || p.Page == "data.json" {
				// The fixture's JVM kxml2 calls these an unknown type; Android 16's
				// own parser calls them websites (the emulator corpus:
				// hostile-plain-text.xml, hostile-json.xml), and the server follows it.
				reason = "Server returned html"
			}
			switch {
			case res.Err == nil || len(res.Links) > 0:
				t.Errorf("%s: %v %q, want an error", p.Page, res.Err, links)
			case p.Defect != "":
			case reason == "<SAX parser message>":
				if res.Err.Label != "parser_exception" {
					t.Errorf("%s: %v, want a parser exception", p.Page, res.Err)
				}
			case res.Err.Detail != reason:
				t.Errorf("%s: %q, want %q", p.Page, res.Err.Detail, reason)
			}
		default:
			t.Errorf("%s: unknown outcome %q", p.Page, want.Outcome)
		}
	}
}

// A protected page on A redirects to B, which also asks: credentials approved
// for A never reach B; B is reported, and once approved, discovery succeeds.
func TestDiscoverCredentials(t *testing.T) {
	const page = `<html><head><title>Private</title><link rel="alternate" type="application/rss+xml" href="/feed.xml"></head></html>`
	authed := func(w http.ResponseWriter, r *http.Request) bool {
		if u, p, ok := r.BasicAuth(); ok && u == "me" && p == "secret" {
			return true
		}
		w.WriteHeader(401)
		return false
	}
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authed(w, r) {
			w.Write([]byte(page))
		}
	}))
	defer b.Close()
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authed(w, r) {
			http.Redirect(w, r, b.URL+"/page", http.StatusFound)
		}
	}))
	defer a.Close()
	au, _ := url.Parse(a.URL)
	bu, _ := url.Parse(b.URL)
	f := testFetcher()
	discover := func(creds *Credentials) (*DiscoverResult, *FetchError) {
		res, err := f.Discover(context.Background(), a.URL+"/page", creds, time.UnixMilli(corpusNow))
		var fe *FetchError
		errors.As(err, &fe)
		return res, fe
	}
	if _, fe := discover(nil); fe == nil || fe.Label != "unauthorized" || fe.Origin != Origin(au) {
		t.Fatalf("no credentials: %+v, want A asking", fe)
	}
	creds := &Credentials{Username: "me", Password: "secret", ApprovedOrigins: []string{Origin(au)}}
	if _, fe := discover(creds); fe == nil || fe.Origin != Origin(bu) {
		t.Fatalf("approved for A: %+v, want B asking", fe)
	}
	creds.ApprovedOrigins = append(creds.ApprovedOrigins, Origin(bu))
	res, fe := discover(creds)
	if fe != nil || len(res.Links) != 1 || res.Links[0].URL != b.URL+"/feed.xml" {
		t.Fatalf("approved for both: %+v %+v", res, fe)
	}
	// Userinfo in the address is credentials for its own origin only.
	withUser := strings.Replace(a.URL, "http://", "http://me:secret@", 1) + "/page"
	_, err := f.Discover(context.Background(), withUser, nil, time.UnixMilli(corpusNow))
	var ufe *FetchError
	if !errors.As(err, &ufe) || ufe.Origin != Origin(bu) {
		t.Fatalf("userinfo: %v, want B asking", err)
	}
}

// TestFindLinksOrder checks FeedDiscoverer's order (Android's ArrayMap, by the
// URLs' hash codes) at the fixtures' own base.
func TestFindLinksOrder(t *testing.T) {
	x := discoverLoad(t)
	for _, p := range x.Pages {
		want := p.Android
		if p.Recommended != nil {
			want = *p.Recommended
		}
		if want.Outcome != "chooser" {
			continue
		}
		b, err := os.ReadFile(filepath.Join("testdata/discovery", p.Page))
		if err != nil {
			t.Fatal(err)
		}
		var got, w []string
		for _, l := range findLinks(context.Background(), strings.ReplaceAll(string(b), "{BASE}", x.Base), p.Requested, p.Requested) {
			got = append(got, l.URL+" | "+l.Title)
		}
		for _, it := range want.Items {
			w = append(w, it.URL+" | "+it.Label)
		}
		if strings.Join(got, "\n") != strings.Join(w, "\n") {
			t.Errorf("%s:\n  %s\nwant\n  %s", p.Page, strings.Join(got, "\n  "), strings.Join(w, "\n  "))
		}
	}
}

// A URL fetched is at most 16 KiB, the final one included (PROTOCOL §9.2): a
// page's relative links resolve against it, so a redirect to a huge URL can
// no longer make every link huge. A longer redirect target fails as an
// unusable redirect, a longer address as an invalid feed URL.
func TestFetchURLLengthBound(t *testing.T) {
	const feedDoc = `<rss version="2.0"><channel><title>T</title></channel></rss>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/to/")); err == nil {
			w.Header().Set("Location", "/"+strings.Repeat("p", n))
			w.WriteHeader(http.StatusFound)
			return
		}
		fmt.Fprint(w, feedDoc)
	}))
	defer srv.Close()
	f := testFetcher()
	fits := maxURLBytes - len(srv.URL+"/")
	res, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/to/" + strconv.Itoa(fits)})
	if err != nil || len(res.FinalURL) != maxURLBytes {
		t.Fatalf("a redirect to %d bytes: %v", maxURLBytes, err)
	}
	_, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/to/" + strconv.Itoa(fits+1)})
	if fe := fetchErr(t, err); fe.Label != "http_status" || fe.Code != http.StatusFound {
		t.Errorf("a redirect to %d bytes: %+v", maxURLBytes+1, fe)
	}
	_, err = f.Discover(context.Background(), srv.URL+"/to/"+strconv.Itoa(fits+1), nil, time.UnixMilli(corpusNow))
	if fe := fetchErr(t, err); fe.Label != "http_status" {
		t.Errorf("discovery through a redirect to %d bytes: %+v", maxURLBytes+1, fe)
	}
	_, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/" + strings.Repeat("p", fits+1)})
	if fe := fetchErr(t, err); fe.Label != "io" {
		t.Errorf("an address of %d bytes: %+v", maxURLBytes+1, fe)
	}
}

// Discovery offers at most 1,000 links and resolves at most 4 MiB of link
// URLs (PROTOCOL §9.2). Every relative link is as long as the page's URL: under a
// 16 KiB URL, 20,000 of them kept 330 MB of URLs and allocated gigabytes. A
// page over the bounds gives the links collected until then, in Android's
// order.
func TestDiscoveryBoundsItsLinks(t *testing.T) {
	const page = "https://example.org/page"
	link := func(href string) string {
		return `<link rel="alternate" type="application/rss+xml" href="` + href + `">`
	}
	var many, first strings.Builder
	for i := range 1500 {
		l := link(fmt.Sprintf("https://example.invalid/%04d", i))
		many.WriteString(l)
		if i < 1000 {
			first.WriteString(l)
		}
	}
	got := findLinks(context.Background(), "<html><head>"+many.String()+"</head></html>", page, page)
	want := findLinks(context.Background(), "<html><head>"+first.String()+"</head></html>", page, page)
	if len(want) != 1000 || !slices.Equal(got, want) {
		t.Errorf("1,500 links: %d offered, want the first 1,000 (%d)", len(got), len(want))
	}

	long := "https://example.org/" + strings.Repeat("p", maxURLBytes-len("https://example.org//page")) + "/page"
	var hostile strings.Builder
	hostile.WriteString("<html><head>")
	for i := range 20_000 {
		hostile.WriteString(link(fmt.Sprintf("?%d", i)))
	}
	hostile.WriteString("</head></html>")
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	links := findLinks(context.Background(), hostile.String(), long, long)
	runtime.ReadMemStats(&after)
	total := 0
	for _, l := range links {
		total += len(l.URL)
	}
	if len(links) == 0 || total > maxDiscoveredBytes {
		t.Errorf("under a %d-byte URL: %d links, %d bytes", len(long), len(links), total)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 128<<20 {
		t.Errorf("20,000 links under a %d-byte URL allocated %d MB", len(long), alloc>>20)
	}
}
