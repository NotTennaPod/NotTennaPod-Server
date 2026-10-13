package feed

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// testFetcher reaches the tests' servers on 127.0.0.1; any other loopback
// name (localhost) stays refused, so refusals through DNS can be tested.
func testFetcher() *Fetcher {
	f := NewFetcher()
	f.allowHost = func(host string) bool { return host == "127.0.0.1" }
	return f
}

func fetchErr(t *testing.T, err error) *FetchError {
	t.Helper()
	var fe *FetchError
	if !errors.As(err, &fe) {
		t.Fatalf("not a FetchError: %v", err)
	}
	return fe
}

func TestFetchRedirects(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/perm", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/temp", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/temp", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/feed", http.StatusFound) })
	mux.HandleFunc("/feed", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("<rss/>")) })
	mux.HandleFunc("/loop", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/loop", http.StatusFound) })
	f := testFetcher()
	res, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/perm"})
	if err != nil {
		t.Fatal(err)
	}
	if res.PermanentMove != srv.URL+"/temp" || res.FinalURL != srv.URL+"/feed" || len(res.Trace) != 3 {
		t.Errorf("permanent move %q, final %q, trace %v", res.PermanentMove, res.FinalURL, res.Trace)
	}
	// Only the first hop's 301/308 counts (Android); a temporary redirect adds nothing.
	res, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/temp"})
	if err != nil || res.PermanentMove != "" {
		t.Errorf("temporary: %v %q", err, res.PermanentMove)
	}
	_, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/loop"})
	if fe := fetchErr(t, err); fe.Label != "too_many_redirects" {
		t.Errorf("loop: %s", fe.Label)
	}
	a, _ := url.Parse("http://example.com/feed?x=1")
	b, _ := url.Parse("https://example.com/feed?x=1")
	if !upgraded(a, b) || upgraded(b, a) {
		t.Error("an http→https upgrade of the same URL is a permanent move")
	}
}

func TestFetchConditional(t *testing.T) {
	var gotINM []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotINM = append(gotINM, r.Header.Get("If-None-Match"))
		if r.Header.Get("If-None-Match") == `"1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"1"`)
		w.Write([]byte("<rss/>"))
	}))
	defer srv.Close()
	f := testFetcher()
	res, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/a"})
	if err != nil || res.ETag != `"1"` {
		t.Fatalf("%v %v", err, res)
	}
	v := &Validators{Key: res.RepresentationKey, ETag: res.ETag}
	if res, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/a", Validators: v}); err != nil || !res.NotModified {
		t.Fatalf("same representation: %v %+v", err, res)
	}
	// Another URL (or credential revision) never gets these validators.
	if res, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/b", Validators: v}); err != nil || res.NotModified {
		t.Fatalf("another URL: %v %+v", err, res)
	}
	if res, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/a", Validators: v, Force: true}); err != nil || res.NotModified {
		t.Fatalf("forced: %v %+v", err, res)
	}
	if want := []string{"", `"1"`, "", ""}; strings.Join(gotINM, ",") != strings.Join(want, ",") {
		t.Errorf("If-None-Match sent %q, want %q", gotINM, want)
	}
}

func TestFetchCredentials(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][]string{}
	auth := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen[name] = append(seen[name], r.Header.Get("Authorization"))
			mu.Unlock()
			if r.Header.Get("Authorization") == "Basic "+base64.StdEncoding.EncodeToString([]byte("ü:pä")) {
				w.Write([]byte("<rss/>"))
				return
			}
			w.WriteHeader(401)
		}
	}
	feedSrv := httptest.NewServer(auth("feed"))
	defer feedSrv.Close()
	other := httptest.NewServer(auth("other"))
	defer other.Close()
	mover := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/private", http.StatusFound)
	}))
	defer mover.Close()
	u, _ := url.Parse(feedSrv.URL)
	m, _ := url.Parse(mover.URL)
	creds := &Credentials{Username: "ü", Password: "pä", Revision: "s1", ApprovedOrigins: []string{Origin(u), Origin(m)}}
	f := testFetcher()
	// Same origin: a 401, then ISO-8859-1 (also a 401 here: ü encodes as one byte), then UTF-8.
	if _, err := f.Fetch(context.Background(), FetchRequest{URL: feedSrv.URL + "/feed", Credentials: creds}); err != nil {
		t.Fatal(err)
	}
	if got := seen["feed"]; len(got) != 3 || got[0] != "" || got[1] != basic("ü", "pä", true) || got[2] != basic("ü", "pä", false) {
		t.Errorf("same-origin attempts %q", got)
	}
	// A redirect to an unapproved origin answering 401: the credentials never go there.
	_, err := f.Fetch(context.Background(), FetchRequest{URL: mover.URL + "/feed", Credentials: creds})
	if fe := fetchErr(t, err); fe.Label != "unauthorized" {
		t.Errorf("unapproved origin: %s", fe.Label)
	}
	for _, a := range seen["other"] {
		if a != "" {
			t.Errorf("credentials reached an unapproved origin: %q", a)
		}
	}
	if strings.Contains(err.Error(), "pä") || strings.Contains(err.Error(), "ü") {
		t.Error("credentials in an error")
	}
	// Once approved, they do.
	o, _ := url.Parse(other.URL)
	creds.ApprovedOrigins = append(creds.ApprovedOrigins, Origin(o))
	if _, err := f.Fetch(context.Background(), FetchRequest{URL: mover.URL + "/feed", Credentials: creds}); err != nil {
		t.Errorf("explicitly approved origin: %v", err)
	}
}

func TestFetchBoundsAndErrors(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	stall := make(chan struct{})
	defer close(stall) // before srv.Close, which waits for the stalled handler
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) { w.Write(bytes.Repeat([]byte("x"), 2048)) })
	mux.HandleFunc("/gzip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		gz.Write([]byte("<rss>compressed</rss>"))
		gz.Close()
	})
	mux.HandleFunc("/short", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Write([]byte("<rss/>"))
	})
	mux.HandleFunc("/stall", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("<rss>"))
		w.(http.Flusher).Flush()
		<-stall
	})
	mux.HandleFunc("/busy", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(503)
	})
	mux.HandleFunc("/gone", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(410) })
	f := testFetcher()
	f.bodyLimit, f.total = 1024, 500*time.Millisecond
	cases := []struct{ path, label string }{
		{"/big", "size_limit"}, {"/short", "io"}, {"/stall", "time_limit"}, {"/busy", "http_status"}, {"/gone", "not_found"},
	}
	for _, c := range cases {
		_, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + c.path})
		fe := fetchErr(t, err)
		if fe.Label != c.label {
			t.Errorf("%s: %s, want %s", c.path, fe.Label, c.label)
		}
		if c.path == "/busy" && (fe.Code != 503 || fe.RetryAfter != 120*time.Second) {
			t.Errorf("busy: %+v", fe)
		}
	}
	res, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/gzip"})
	if err != nil || string(res.Body) != "<rss>compressed</rss>" {
		t.Errorf("gzip: %v %q", err, res.Body)
	}
	// Loopback destinations are refused, also through DNS.
	_, err = NewFetcher().Fetch(context.Background(), FetchRequest{URL: srv.URL + "/gzip"})
	if fe := fetchErr(t, err); fe.Label != "refused_destination" {
		t.Errorf("loopback: %s", fe.Label)
	}
	_, err = NewFetcher().Fetch(context.Background(), FetchRequest{URL: "http://localhost:1/feed"})
	if fe := fetchErr(t, err); fe.Label != "refused_destination" {
		t.Errorf("localhost: %s", fe.Label)
	}
}

// Twenty redirects are followed; the 21st is too many (PROTOCOL §9.2).
func TestFetchRedirectLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var n int
		fmt.Sscanf(r.URL.Path, "/hop/%d", &n)
		if n > 0 {
			http.Redirect(w, r, fmt.Sprintf("/hop/%d", n-1), http.StatusFound)
			return
		}
		w.Write([]byte("<rss/>"))
	}))
	defer srv.Close()
	f := testFetcher()
	res, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/hop/20"})
	if err != nil || len(res.Trace) != 21 {
		t.Fatalf("20 redirects: %v, %d hops", err, len(res.Trace))
	}
	_, err = f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/hop/21"})
	if fe := fetchErr(t, err); fe.Label != "too_many_redirects" {
		t.Errorf("21 redirects: %s", fe.Label)
	}
}

// Loopback is refused on every hop, also when a name resolves to it.
func TestFetchRefusedThroughDNSAndRedirects(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("<rss/>")) }))
	defer target.Close()
	u, _ := url.Parse(target.URL)
	viaName := "http://localhost:" + u.Port() + "/feed"
	mover := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, viaName, http.StatusFound)
	}))
	defer mover.Close()
	f := testFetcher()
	if _, err := f.Fetch(context.Background(), FetchRequest{URL: viaName}); fetchErr(t, err).Label != "refused_destination" {
		t.Errorf("a name resolving to loopback: %v", err)
	}
	if _, err := f.Fetch(context.Background(), FetchRequest{URL: mover.URL + "/feed"}); fetchErr(t, err).Label != "refused_destination" {
		t.Errorf("a redirect to loopback: %v", err)
	}
}

// Validators belong to the credential revision too: new credentials send none.
func TestFetchValidatorsFollowCredentials(t *testing.T) {
	var sent []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent = append(sent, r.Header.Get("If-None-Match"))
		w.Header().Set("ETag", `"1"`)
		w.Write([]byte("<rss/>"))
	}))
	defer srv.Close()
	f := testFetcher()
	u, _ := url.Parse(srv.URL)
	c1 := &Credentials{Username: "a", Password: "b", Revision: "s1", ApprovedOrigins: []string{Origin(u)}}
	res, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/f", Credentials: c1})
	if err != nil {
		t.Fatal(err)
	}
	v := &Validators{Key: res.RepresentationKey, ETag: res.ETag}
	c2 := *c1
	c2.Revision = "s2"
	f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/f", Credentials: c1, Validators: v})
	f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/f", Credentials: &c2, Validators: v})
	if want := []string{"", `"1"`, ""}; strings.Join(sent, ",") != strings.Join(want, ",") {
		t.Errorf("If-None-Match %q, want %q", sent, want)
	}
}
