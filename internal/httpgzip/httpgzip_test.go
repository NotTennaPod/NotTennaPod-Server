package httpgzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// Accept-Encoding as RFC 9110 §12.5.3 reads it: an explicit refusal (q=0)
// wins over a wildcard, a wildcard covers gzip when gzip is not listed, the
// quality's forms and case are parsed, and no header means identity.
func TestAccepts(t *testing.T) {
	for _, tc := range []struct {
		values []string
		want   bool
	}{
		{nil, false},
		{[]string{""}, false},
		{[]string{"identity"}, false},
		{[]string{"br, deflate"}, false},
		{[]string{"gzip"}, true},
		{[]string{"GZip"}, true},
		{[]string{"x-gzip"}, true},
		{[]string{"gzip, deflate, br"}, true},
		{[]string{"br", "gzip"}, true}, // several header lines
		{[]string{"gzip;q=0"}, false},
		{[]string{"gzip; q=0"}, false},
		{[]string{"gzip;Q=0"}, false},
		{[]string{"gzip;q=0.0"}, false},
		{[]string{"gzip;q=0.000"}, false},
		{[]string{"gzip;q=0.001"}, true},
		{[]string{"gzip;q=0.5"}, true},
		{[]string{"gzip;q=1"}, true},
		{[]string{"gzip;q=1.000"}, true},
		{[]string{"gzip;q=1.5"}, false},    // malformed
		{[]string{"gzip;q=0.0001"}, false}, // malformed
		{[]string{"gzip;q=abc"}, false},    // malformed
		{[]string{"gzip;q="}, false},       // malformed
		{[]string{"gzip;level=1"}, true},   // another parameter
		{[]string{"*"}, true},
		{[]string{"*;q=0.1"}, true},
		{[]string{"*;q=0"}, false},
		{[]string{"br, *"}, true},
		{[]string{"gzip;q=0, *"}, false},
		{[]string{"*", "gzip;q=0"}, false},
		{[]string{"gzip;q=0.5, *;q=0"}, true},
		{[]string{"gzip, gzip;q=0"}, false}, // listed twice: the lower weight
		{[]string{" , ,gzip ,"}, true},
	} {
		if got := Accepts(tc.values); got != tc.want {
			t.Errorf("Accepts(%q) = %v, want %v", tc.values, got, tc.want)
		}
	}
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Negotiate compresses a body of at least MinSize for a client that accepts
// gzip, sets Vary on every answer and Content-Length to what is sent, and
// the compressed body decodes to exactly the original bytes.
func TestNegotiate(t *testing.T) {
	big := []byte(strings.Repeat(`{"op_id":"x","payload":{"text":"some text"}}`, 100) + "\n")
	small := big[:MinSize-1]
	req := func(ae ...string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		for _, v := range ae {
			r.Header.Add("Accept-Encoding", v)
		}
		return r
	}
	for _, tc := range []struct {
		name string
		r    *http.Request
		body []byte
		gzip bool
	}{
		{"no request", nil, big, false},
		{"no header", req(), big, false},
		{"gzip", req("gzip, deflate"), big, true},
		{"refused", req("gzip;q=0"), big, false},
		{"small", req("gzip"), small, false},
		{"exactly the minimum", req("gzip"), big[:MinSize], true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			sent := Negotiate(h, tc.r, tc.body)
			if h.Get("Vary") != "Accept-Encoding" || h.Get("Content-Length") != strconv.Itoa(len(sent)) {
				t.Fatalf("headers %v for %d bytes", h, len(sent))
			}
			if got := h.Get("Content-Encoding") == "gzip"; got != tc.gzip {
				t.Fatalf("Content-Encoding %q, want gzip %v", h.Get("Content-Encoding"), tc.gzip)
			}
			if !tc.gzip {
				if !bytes.Equal(sent, tc.body) {
					t.Fatal("the identity body changed")
				}
				return
			}
			if len(sent) >= len(tc.body) && len(tc.body) > MinSize {
				t.Errorf("compressed %d bytes to %d", len(tc.body), len(sent))
			}
			if !bytes.Equal(gunzip(t, sent), tc.body) {
				t.Fatal("the decoded body differs from the original")
			}
		})
	}
	// Vary is not repeated.
	h := http.Header{"Vary": {"Accept-Encoding"}}
	Negotiate(h, req("gzip"), big)
	if len(h.Values("Vary")) != 1 {
		t.Errorf("Vary %v", h.Values("Vary"))
	}
	// Pooled writers never mix bodies.
	other := bytes.Repeat([]byte("other body "), 500)
	a, b := Compress(big), Compress(other)
	if !bytes.Equal(gunzip(t, a), big) || !bytes.Equal(gunzip(t, b), other) {
		t.Fatal("pooled writers mixed two bodies")
	}
}
