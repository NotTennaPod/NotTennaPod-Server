package media

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

// rep is a pinned representation of one source URL with one credential
// revision (PROTOCOL §13): the facts the first answer gave, which every later
// answer must repeat.
type rep struct {
	source       string // the episode's enclosure URL (requests start there; redirects are followed every time)
	resource     string // the resource the redirects led to: its origin and path (resourceOf)
	creds        *feed.Credentials
	length       int64  // the total length; -1 unknown
	etag         string // a strong ETag; "" without
	lastModified string
	ctype        string // the upstream's media type, when it named one
	ranges       bool   // the upstream answered a range request with 206
	encoding     string // a content coding the upstream applied despite asking for identity
}

// strong reports a representation with a strong validator; any other is
// weak and never spliced (PROTOCOL §13).
func (r *rep) strong() bool { return r.etag != "" }

// key identifies the representation for the probe cache and the spools
// (PROTOCOL §13: account, source URL, credential revision and validators).
func (r *rep) key(account string) string {
	rev := ""
	if r.creds != nil {
		rev = r.creds.Revision
	}
	return strings.Join([]string{account, r.source, r.resource, rev, r.etag, r.lastModified, strconv.FormatInt(r.length, 10), r.encoding}, "\x00")
}

// resourceOf is the identity of the resource a request's redirects led to:
// its origin, path and query. Validators belong to one resource, so an
// answer from another is another representation, whatever its ETag says.
// Only well-known volatile query parameters are left out (stableQuery):
// signatures, expiries and tracking that change on every redirect while the
// file stays the same.
func resourceOf(u *url.URL) string {
	return feed.Origin(u) + u.EscapedPath() + "?" + stableQuery(u.RawQuery)
}

// Volatile query parameters, matched without regard to case:
//   - signatures and expiries: X-Amz-* (S3), X-Goog-* (Cloud Storage),
//     Signature, Expires, Key-Pair-Id and Policy (CloudFront), sig,
//     signature, token, __token__ and hdnts (Akamai), hmac, exp, expires;
//     st with e or se (a start and an end: Akamai and Azure tokens);
//   - tracking: utm_*, _gl, awCollectionId and awEpisodeId (Adswizz),
//     request_event_id and timestamp (Megaphone's per-request IDs).
var volatileParams = map[string]bool{
	"signature": true, "expires": true, "key-pair-id": true, "policy": true, "sig": true, "token": true,
	"__token__": true, "hdnts": true, "hmac": true, "exp": true,
	"_gl": true, "awcollectionid": true, "awepisodeid": true, "request_event_id": true, "timestamp": true,
}

func volatileParam(name string) bool {
	return volatileParams[name] || strings.HasPrefix(name, "x-amz-") || strings.HasPrefix(name, "x-goog-") || strings.HasPrefix(name, "utm_")
}

// stableQuery is a query without its volatile parameters, the rest as a
// sorted multiset (order and encoding do not matter; repeats do).
func stableQuery(raw string) string {
	type param struct{ name, k, v string }
	var params []param
	names := map[string]bool{}
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		if u, err := url.QueryUnescape(k); err == nil {
			k = u
		}
		if u, err := url.QueryUnescape(v); err == nil {
			v = u
		}
		name := strings.ToLower(k)
		names[name] = true
		params = append(params, param{name, k, v})
	}
	paired := names["st"] && (names["e"] || names["se"])
	var kept []string
	for _, p := range params {
		if volatileParam(p.name) || paired && (p.name == "st" || p.name == "e" || p.name == "se") {
			continue
		}
		kept = append(kept, url.QueryEscape(p.k)+"="+url.QueryEscape(p.v))
	}
	sort.Strings(kept)
	return strings.Join(kept, "&")
}

// upstream fetches a pinned representation's bytes with the fetch rules.
type upstream struct {
	fetcher *feed.Fetcher
	stall   time.Duration
	rep     *rep

	mu      sync.Mutex
	ignored bool // the upstream answered a range request with the whole representation
}

func strongETag(h http.Header) string {
	e := strings.TrimSpace(h.Get("ETag"))
	if e == "" || strings.HasPrefix(e, "W/") {
		return ""
	}
	return e
}

func encodingOf(h http.Header) string {
	e := strings.ToLower(strings.TrimSpace(h.Get("Content-Encoding")))
	if e == "identity" {
		return ""
	}
	return e
}

// pin asks for the first two bytes and records the representation: a 206
// shows ranges work and gives the total; a 200 shows they do not (its body
// is not read). Two bytes are the request podcast statistics do not count.
func pin(ctx context.Context, f *feed.Fetcher, source string, creds *feed.Credentials) (*rep, error) {
	resp, final, err := f.OpenMedia(ctx, feed.MediaRequest{URL: source, Range: "bytes=0-1", Credentials: creds})
	if err != nil {
		return nil, fetchFailure(err)
	}
	defer resp.Body.Close()
	r := &rep{source: source, resource: resourceOf(final), creds: creds, length: -1, etag: strongETag(resp.Header),
		lastModified: resp.Header.Get("Last-Modified"), ctype: mediaType(resp.Header.Get("Content-Type")),
		encoding: encodingOf(resp.Header)}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if r.encoding != "" {
			return r, nil // an encoded range: only a whole, decoded copy serves it (PROTOCOL §13)
		}
		cr, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || cr.start != 0 {
			return nil, errBadRange
		}
		r.ranges, r.length = true, cr.total
	case http.StatusOK:
		if r.encoding == "" {
			r.length = resp.ContentLength
		}
	case http.StatusRequestedRangeNotSatisfiable:
		// Only an empty file cannot satisfy bytes=0-1.
		return nil, newError(http.StatusBadGateway, "media_unavailable", "wrong_size")
	default:
		return nil, statusFailure(resp.StatusCode)
	}
	return r, nil
}

type contentRange struct {
	start, end, total int64 // total -1 for "*"
	unsatisfied       bool  // bytes */total
}

func parseContentRange(v string) (contentRange, bool) {
	cr := contentRange{total: -1}
	rest, ok := strings.CutPrefix(strings.TrimSpace(v), "bytes ")
	if !ok {
		return cr, false
	}
	span, total, ok := strings.Cut(strings.TrimSpace(rest), "/")
	if !ok {
		return cr, false
	}
	if total != "*" {
		n, err := strconv.ParseInt(total, 10, 64)
		if err != nil || n < 0 {
			return cr, false
		}
		cr.total = n
	}
	if span == "*" {
		cr.unsatisfied = true
		return cr, cr.total >= 0
	}
	a, b, ok := strings.Cut(span, "-")
	if !ok {
		return cr, false
	}
	var err1, err2 error
	cr.start, err1 = strconv.ParseInt(a, 10, 64)
	cr.end, err2 = strconv.ParseInt(b, 10, 64)
	if err1 != nil || err2 != nil || cr.start < 0 || cr.end < cr.start || (cr.total >= 0 && cr.end >= cr.total) {
		return cr, false
	}
	return cr, true
}

// opened is an upstream answer the decision table accepted.
type opened struct {
	body     io.ReadCloser
	status   int   // 200, 206 or 416
	start    int64 // the offset of the body's first byte
	length   int64 // the body's length; -1 unknown
	total    int64
	unranged bool // a range was asked for and the whole representation came
}

func (o *opened) Close() error {
	if o.body != nil {
		return o.body.Close()
	}
	return nil
}

// open asks the upstream for bytes [start, end] (end -1: to the end) of the
// pinned representation when ranged is set and the host gives ranges, else for
// all of it, and checks the answer against the representation (the decision
// table of PROTOCOL §13). Only a strong ETag is sent as If-Range. The body it
// returns is exact: it fails rather than end early, and it ends the stream when
// the upstream sends nothing for the stall time.
func (u *upstream) open(ctx context.Context, start, end int64, ranged bool) (*opened, error) {
	r := u.rep
	req := feed.MediaRequest{URL: r.source, Credentials: r.creds}
	ranged = ranged && r.ranges
	if ranged {
		if end >= 0 {
			req.Range = fmt.Sprintf("bytes=%d-%d", start, end)
		} else {
			req.Range = fmt.Sprintf("bytes=%d-", start)
		}
		req.IfRange = r.etag
	}
	ctx, cancel := context.WithCancel(ctx)
	resp, final, err := u.fetcher.OpenMedia(ctx, req)
	if err != nil {
		cancel()
		return nil, fetchFailure(err)
	}
	fail := func(e *Error) (*opened, error) {
		resp.Body.Close()
		cancel()
		return nil, e
	}
	switch resp.StatusCode {
	case http.StatusPartialContent, http.StatusOK, http.StatusRequestedRangeNotSatisfiable:
		if resourceOf(final) != r.resource || !sameValidators(r, resp, req.IfRange != "") {
			return fail(errChanged) // another resource, or another representation of it
		}
	}
	o := &opened{status: resp.StatusCode, total: r.length}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if !ranged {
			return fail(errBadRange)
		}
		if encodingOf(resp.Header) != "" {
			return fail(newError(http.StatusBadGateway, "media_unavailable", "io")) // an encoded 206 (PROTOCOL §13)
		}
		cr, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || cr.unsatisfied {
			return fail(errBadRange)
		}
		if cr.total != r.length {
			return fail(errChanged)
		}
		if cr.start != start || (end >= 0 && cr.end > end) {
			return fail(errBadRange)
		}
		if resp.ContentLength >= 0 && resp.ContentLength != cr.end-cr.start+1 {
			return fail(errBadRange)
		}
		o.start, o.length = cr.start, cr.end-cr.start+1
	case http.StatusOK:
		if encodingOf(resp.Header) != r.encoding {
			return fail(errChanged)
		}
		if r.encoding == "" && r.length >= 0 && resp.ContentLength >= 0 && resp.ContentLength != r.length {
			return fail(errChanged)
		}
		o.start, o.length = 0, r.length
		if r.encoding != "" {
			o.length = -1
		}
		if ranged {
			o.unranged = true
			u.mu.Lock()
			u.ignored = true
			u.mu.Unlock()
		}
	case http.StatusRequestedRangeNotSatisfiable:
		cr, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || !cr.unsatisfied || cr.total != r.length {
			return fail(errChanged)
		}
		resp.Body.Close()
		cancel()
		return o, nil
	default:
		return fail(statusFailure(resp.StatusCode))
	}
	excess := errChanged // a whole representation longer than pinned
	if o.status == http.StatusPartialContent {
		excess = errBadRange // a range longer than its Content-Range
	}
	o.body = &exactBody{r: &stallReader{r: resp.Body, timeout: u.stall, stalled: cancel}, left: o.length, excess: excess,
		closer: func() error { cancel(); return resp.Body.Close() }}
	return o, nil
}

// sameValidators compares an answer's validators with the pinned ones (PROTOCOL §13).
// A strong representation's answers must carry its ETag; only a 206 to our
// If-Range, which the server sent because the ETag matched, may leave it
// out, and a 416 carries no representation. A weak one's must carry its
// Last-Modified when it had one (its length is checked apart).
func sameValidators(r *rep, resp *http.Response, conditional bool) bool {
	partial := resp.StatusCode == http.StatusPartialContent
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return true
	}
	if r.etag != "" {
		got := strings.TrimSpace(resp.Header.Get("ETag"))
		if got == "" {
			return partial && conditional
		}
		return got == r.etag
	}
	if r.lastModified != "" {
		return resp.Header.Get("Last-Modified") == r.lastModified
	}
	return true
}

// unranged reports whether the upstream has answered a range request with
// the whole representation since the pin.
func (u *upstream) unranged() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.ignored
}

// stallReader ends a read the upstream answers nothing to for the stall
// time (PROTOCOL §13): stalled cancels the request, and the read fails.
type stallReader struct {
	r       io.Reader
	timeout time.Duration
	stalled func()

	mu  sync.Mutex
	hit bool
}

func (s *stallReader) Read(p []byte) (int, error) {
	if s.timeout <= 0 {
		return s.r.Read(p)
	}
	t := time.AfterFunc(s.timeout, func() {
		s.mu.Lock()
		s.hit = true
		s.mu.Unlock()
		s.stalled()
	})
	n, err := s.r.Read(p)
	t.Stop()
	s.mu.Lock()
	hit := s.hit
	s.mu.Unlock()
	if hit && err != nil {
		return n, errStalled
	}
	return n, err
}

// exactBody delivers exactly left bytes (or, when left is -1, all there
// are): an early end is errTruncated, an upstream failure a media error,
// and an answer that goes on past its length the excess error. The end is
// the upstream's own: the last bytes are released only once the upstream
// has ended there, so a longer answer never passes for a complete one.
type exactBody struct {
	r      io.Reader
	left   int64
	excess *Error
	closer func() error
	ended  bool
}

func (b *exactBody) Read(p []byte) (int, error) {
	if b.ended || b.left == 0 {
		return 0, io.EOF
	}
	if b.left > 0 && int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.r.Read(p)
	if b.left > 0 {
		b.left -= int64(n)
	}
	switch {
	case err == io.EOF && b.left > 0:
		return n, errTruncated
	case err == io.EOF:
		b.ended = true
		return n, io.EOF
	case err == nil && b.left == 0:
		if e := b.atEnd(); e != nil {
			return 0, e // withhold the last bytes: the response must not end clean
		}
		b.ended = true
		return n, io.EOF
	case err == nil:
		return n, nil
	}
	return n, mediaReadError(err)
}

// atEnd checks that the upstream ends where the length says.
func (b *exactBody) atEnd() *Error {
	var one [1]byte
	for {
		m, err := b.r.Read(one[:])
		switch {
		case m > 0:
			return b.excess
		case err == io.EOF:
			return nil
		case err != nil:
			return mediaReadError(err)
		}
	}
}

func mediaReadError(err error) *Error {
	var me *Error
	if errors.As(err, &me) {
		return me
	}
	return errTruncated
}

func (b *exactBody) Close() error { return b.closer() }
