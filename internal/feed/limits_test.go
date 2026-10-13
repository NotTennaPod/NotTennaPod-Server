package feed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// A namespace declared in a child can make its parent handled at only one of
// its ends. The pop then no longer pairs with a push; that is a parse error,
// never a crash (a 124-byte feed stopped the server).
func TestUnpairedEndTagIsAParseError(t *testing.T) {
	for _, doc := range []string{
		`<feed><x:outer xmlns:x="http://www.w3.org/2005/Atom"><atom:inner xmlns:atom="http://www.w3.org/2005/Atom"/></x:outer></feed>`,
		`<rss version="2.0"><channel><title>T</title><i:x xmlns:i="http://www.itunes.com/dtds/podcast-1.0.dtd"><itunes:y xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"/></i:x></channel></rss>`,
	} {
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			kind := "atom"
			if doc[1] == 'r' {
				kind = "rss20"
			}
			return saxParse(context.Background(), doc, newHandlerState(kind, corpusNow))
		}()
		var pe *ParseError
		if !errors.As(err, &pe) || pe.Label != "parser_exception" {
			t.Errorf("%s: %v", doc, err)
		}
		if _, err := Parse(context.Background(), []byte(doc), "https://example.org/feed", time.UnixMilli(corpusNow)); !budgetIsParserException(err) {
			t.Errorf("Parse %s: %v", doc, err)
		}
	}
	// An element handled at both ends by different handlers keeps Android's
	// result: here an Atom default namespace inside an RSS item, whose XHTML
	// child ends the Atom default before its parent's end tag.
	f, err := budgetParse(context.Background(), `<rss version="2.0"><channel><title>T</title><item><title>I</title>`+
		`<content xmlns="http://www.w3.org/2005/Atom" type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml">x</div></content>`+
		`</item></channel></rss>`)
	if err != nil || len(f.Items) != 1 || *f.Items[0].Title != "I" {
		t.Fatalf("both ends handled: %v %+v", err, f)
	}
}

// Discovery's link ordering is bounded work and stops at the deadline (100,000
// links took about 4.6 s under a 200 ms deadline). Links keep Android's order:
// by hash, equal hashes in page order.
func TestDiscoveryKeepsItsDeadline(t *testing.T) {
	var b strings.Builder
	b.WriteString("<html><head>")
	for i := 0; i < 100000; i++ {
		fmt.Fprintf(&b, `<link rel="alternate" type="application/rss+xml" href="https://example.invalid/%08d">`, i)
	}
	b.WriteString("</head></html>")
	page := b.String()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page) }))
	defer srv.Close()
	f := testFetcher()
	f.total = 200 * time.Millisecond
	start := time.Now()
	f.Discover(context.Background(), srv.URL, nil, time.Now())
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("discovery with a 200 ms deadline took %v", elapsed)
	}

	// "Aa" and "BB" have the same Java hash code.
	links := findLinks(context.Background(), `<html><head>`+
		`<link rel="alternate" type="application/rss+xml" href="/BB">`+
		`<link rel="alternate" type="application/rss+xml" href="/zz">`+
		`<link rel="alternate" type="application/rss+xml" href="/Aa">`+
		`<link rel="alternate" type="application/rss+xml" href="/BB" title="again">`+
		`</head></html>`, "https://example.org/page", "https://example.org/page")
	var got []string
	for _, l := range links {
		got = append(got, l.URL+" "+l.Title)
	}
	if want := "https://example.org/BB again|https://example.org/Aa /Aa|https://example.org/zz /zz"; strings.Join(got, "|") != want {
		t.Fatalf("order %q", got)
	}
}

// Entity expansions built along the way are charged to the 16 MiB budget too (a
// 525 KB chain allocated about 65 MB).
func TestNestedEntityExpansionsAreCharged(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<!DOCTYPE rss [<!ENTITY e0 "`)
	b.WriteString(strings.Repeat("x", 512<<10))
	b.WriteString(`">`)
	for i := 1; i <= 63; i++ {
		fmt.Fprintf(&b, `<!ENTITY e%d "x&e%d;">`, i, i-1)
	}
	b.WriteString(`]><rss version="2.0"><channel><title>Feed</title><description>&e63;</description></channel></rss>`)
	if _, err := budgetParse(context.Background(), b.String()); !budgetIsParserException(err) {
		t.Fatalf("a chain of 64 expansions of 512 KiB: %v", err)
	}
	// A short chain within the budget still expands.
	f, err := budgetParse(context.Background(), `<!DOCTYPE rss [<!ENTITY a "x"><!ENTITY b "&a;&a;"><!ENTITY c "&b;&b;">]>`+
		`<rss version="2.0"><channel><title>&c;</title></channel></rss>`)
	if err != nil || *f.Title != "xxxx" {
		t.Fatalf("a short chain: %v", err)
	}
}

// A huge Retry-After is the longest wait, not
// an overflow to a negative duration that the schedule ignores.
func TestRetryAfterIsClamped(t *testing.T) {
	now := time.Now()
	for v, want := range map[string]time.Duration{
		"9223372037":           MaxRetryAfter,
		"99999999999999999999": MaxRetryAfter,
		"2592001":              MaxRetryAfter,
		"120":                  2 * time.Minute,
		"0":                    0,
		"-5":                   0,
		now.Add(90 * 24 * time.Hour).UTC().Format(http.TimeFormat): MaxRetryAfter,
	} {
		r := &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": {v}}}
		if got := retryAfter(r, now); got != want {
			t.Errorf("Retry-After %q: %v, want %v", v, got, want)
		}
	}
}

// An origin stored without its port (an import before the fix) still approves
// its own origin, and no other.
func TestPortlessApprovedOriginsStillMatch(t *testing.T) {
	c := &Credentials{ApprovedOrigins: []string{"https://example.org"}}
	for raw, want := range map[string]bool{"https://example.org/feed": true, "https://EXAMPLE.org:443/x": true,
		"https://example.org:8443/feed": false, "http://example.org/feed": false, "https://other.org/feed": false} {
		u, _ := url.Parse(raw)
		if got := approved(c, u); got != want {
			t.Errorf("%s: %v", raw, got)
		}
	}
}

// fixtureDNS answers this process's DNS lookups from a local fixture until
// the test ends (no system setting changes): answers gives the addresses for
// a query type (1 A, 28 AAAA).
func fixtureDNS(t *testing.T, answers func(qtype uint16) []net.IP) {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	old := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", conn.LocalAddr().String())
	}}
	t.Cleanup(func() { net.DefaultResolver = old; conn.Close() })
	go func() {
		buf := make([]byte, 4096)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			end := 12
			for end < n && buf[end] != 0 {
				end += 1 + int(buf[end])
			}
			if end += 5; n < 17 || end > n {
				continue
			}
			qtype := binary.BigEndian.Uint16(buf[end-4 : end-2])
			out := append([]byte{buf[0], buf[1], 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0}, buf[12:end]...)
			count := uint16(0)
			for _, ip := range answers(qtype) {
				data := ip.To4()
				if qtype == 28 {
					data = nil
					if ip.To4() == nil {
						data = ip.To16()
					}
				}
				if data == nil {
					continue
				}
				out = append(out, 0xc0, 0x0c, byte(qtype>>8), byte(qtype), 0, 1, 0, 0, 0, 0, 0, byte(len(data)))
				out = append(out, data...)
				count++
			}
			binary.BigEndian.PutUint16(out[6:8], count)
			conn.WriteTo(out, addr)
		}
	}()
}

// Each checked DNS address is tried in turn, pinned; an
// unreachable first answer falls back to the next.
func TestEachCheckedAddressIsTried(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	fixtureDNS(t, func(q uint16) []net.IP {
		if q == 28 {
			return []net.IP{net.ParseIP("::1")} // nothing listens there
		}
		return []net.IP{net.ParseIP("127.0.0.1")}
	})
	ips, err := net.DefaultResolver.LookupIPAddr(context.Background(), "dual.fixture.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(ips) != 2 || ips[0].IP.To4() != nil {
		t.Skipf("the resolver ordered the answers %v; the test needs IPv6 first", ips)
	}
	f := NewFetcher()
	f.AllowLoopback(func(h string) bool { return h == "dual.fixture.test" })
	defer f.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if res, err := f.Fetch(ctx, FetchRequest{URL: "http://dual.fixture.test:" + port + "/"}); err != nil || string(res.Body) != "ok" {
		t.Fatalf("the second address was not tried: %v", err)
	}
}

// The server's own listening address and port are refused; a wildcard listener
// stands for every address of the machine; another port of the machine stays
// allowed.
func TestOwnListenerIsRefused(t *testing.T) {
	own := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "own") }))
	defer own.Close()
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "other") }))
	defer other.Close()
	_, port, _ := net.SplitHostPort(own.Listener.Addr().String())
	wildcard, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+port)
	if err != nil {
		t.Fatal(err)
	}
	for _, self := range []net.Addr{own.Listener.Addr(), wildcard} {
		f := testFetcher()
		f.RefuseListener(self)
		_, err := f.Fetch(context.Background(), FetchRequest{URL: own.URL + "/healthz"})
		var fe *FetchError
		if !errors.As(err, &fe) || fe.Label != "refused_destination" {
			t.Errorf("listener %v: fetching it gave %v", self, err)
		}
		if res, err := f.Fetch(context.Background(), FetchRequest{URL: other.URL}); err != nil || string(res.Body) != "other" {
			t.Errorf("listener %v: another port: %v", self, err)
		}
	}
}
