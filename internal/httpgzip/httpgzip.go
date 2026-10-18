// Package httpgzip compresses whole responses for clients that accept gzip
// (PROTOCOL §6): the API's JSON and the embedded web app. Streamed responses
// (events, media) never come here. The decoded bytes are exactly the
// identity response's, so canonical bytes, digests and bounds are unchanged.
package httpgzip

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// MinSize is the smallest body worth compressing.
const MinSize = 1 << 10

// level trades a little size for speed: a 1 MiB sync page takes a few
// milliseconds.
const level = 5

var writers = sync.Pool{New: func() any {
	w, _ := gzip.NewWriterLevel(nil, level)
	return w
}}

// Compress returns b gzip-compressed.
func Compress(b []byte) []byte {
	var out bytes.Buffer
	out.Grow(len(b)/4 + 64)
	w := writers.Get().(*gzip.Writer)
	w.Reset(&out)
	_, _ = w.Write(b) // a bytes.Buffer never fails
	_ = w.Close()
	w.Reset(nil)
	writers.Put(w)
	return out.Bytes()
}

// Negotiate decides a response body's encoding for r (nil: identity). With
// gzip accepted and a body of at least MinSize, it sets Content-Encoding and
// returns the compressed body; otherwise it returns b. Either way it sets
// Vary and the body's Content-Length, so call it before WriteHeader.
func Negotiate(h http.Header, r *http.Request, b []byte) []byte {
	if !strings.Contains(strings.ToLower(strings.Join(h.Values("Vary"), ",")), "accept-encoding") {
		h.Add("Vary", "Accept-Encoding")
	}
	if r != nil && len(b) >= MinSize && Accepts(r.Header.Values("Accept-Encoding")) {
		b = Compress(b)
		h.Set("Content-Encoding", "gzip")
	}
	h.Set("Content-Length", strconv.Itoa(len(b)))
	return b
}

// Accepts reports whether Accept-Encoding values allow gzip (RFC 9110
// §12.5.3): gzip or x-gzip with a quality above 0, or else a wildcard with
// one. An explicit gzip;q=0 refuses it whatever the wildcard says; no header
// means identity. A malformed quality makes its element unacceptable.
func Accepts(values []string) bool {
	gzipQ, starQ := -1, -1 // thousandths; -1: not listed
	for _, v := range values {
		for _, elem := range strings.Split(v, ",") {
			coding, params, _ := strings.Cut(elem, ";")
			coding = strings.ToLower(strings.TrimSpace(coding))
			if coding == "" {
				continue
			}
			q := quality(params)
			switch coding {
			case "gzip", "x-gzip":
				if gzipQ < 0 || q < gzipQ {
					gzipQ = q
				}
			case "*":
				if starQ < 0 || q < starQ {
					starQ = q
				}
			}
		}
	}
	if gzipQ >= 0 {
		return gzipQ > 0
	}
	return starQ > 0
}

// quality parses an element's parameters for its weight, in thousandths
// (1000 without a q parameter, 0 when it is malformed).
func quality(params string) int {
	q := 1000
	for _, p := range strings.Split(params, ";") {
		name, value, ok := strings.Cut(p, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "q") {
			continue
		}
		q = parseQ(strings.TrimSpace(value))
	}
	return q
}

// parseQ parses a qvalue: "0" or "1", optionally followed by "." and up to
// three digits ("1" allows only zeros).
func parseQ(s string) int {
	if s == "" || (s[0] != '0' && s[0] != '1') {
		return 0
	}
	whole, frac, dot := strings.Cut(s, ".")
	if len(whole) != 1 || (dot && len(frac) > 3) {
		return 0
	}
	n := 0
	for i := range 3 {
		d := 0
		if i < len(frac) {
			if frac[i] < '0' || frac[i] > '9' {
				return 0
			}
			d = int(frac[i] - '0')
		}
		n = n*10 + d
	}
	if whole == "1" {
		if n != 0 {
			return 0
		}
		return 1000
	}
	return n
}
