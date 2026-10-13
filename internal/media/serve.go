package media

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// writeMediaHeaders sets a media or asset response's type and the headers
// that keep relayed bytes from ever becoming a page of the app's origin
// (PROTOCOL §13): the type is always the server's choice (audio/* or video/*
// for media), never sniffed, rendered inline in a sandbox with no script or
// subresource, and readable only by the app's own origin.
func writeMediaHeaders(h http.Header, ctype string) {
	h.Set("Content-Type", ctype)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Disposition", "inline")
	h.Set("Content-Security-Policy", "sandbox; default-src 'none'")
	h.Set("Cross-Origin-Resource-Policy", "same-origin")
}

// clientRange reads the first range of a browser's Range header against a
// known total (PROTOCOL §13: several ranges are served as the first). ok is false
// for no or an unreadable header (the whole representation is served);
// unsat for a range that starts at or past the end.
func clientRange(v string, total int64) (start, end int64, ok, unsat bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(v), "bytes=")
	if !found || total < 0 {
		return 0, 0, false, false
	}
	first := strings.TrimSpace(strings.Split(spec, ",")[0])
	a, b, found := strings.Cut(first, "-")
	if !found {
		return 0, 0, false, false
	}
	if a == "" { // the last n bytes
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, false, false
		}
		if n == 0 || total == 0 {
			return 0, 0, true, true
		}
		return max(0, total-n), total - 1, true, false
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, false
	}
	end = total - 1
	if b != "" {
		e, err := strconv.ParseInt(b, 10, 64)
		if err != nil || e < start {
			return 0, 0, false, false
		}
		end = min(e, total-1)
	}
	if start >= total {
		return 0, 0, true, true
	}
	return start, end, true, false
}

// seekable reports whether the relay serves byte ranges for the token:
// strong representations from hosts that honour ranges (PROTOCOL §13).
func (t *token) seekable() bool {
	r := t.up.rep
	return r.strong() && r.ranges && r.encoding == "" && !t.up.unranged()
}

// headRelay answers HEAD from the pinned representation, without a body or
// a request to the publisher (PROTOCOL §13).
func (s *Service) headRelay(w http.ResponseWriter, t *token) {
	h := w.Header()
	writeMediaHeaders(h, t.ctype)
	h.Set("Cache-Control", "private, no-transform")
	if t.spool != nil {
		h.Set("Accept-Ranges", "bytes")
		if n := t.spool.knownTotal(t.up.rep.length, t.up.rep.encoding != ""); n >= 0 {
			h.Set("Content-Length", strconv.FormatInt(n, 10))
		}
		t.spool.mu.Lock()
		etag := t.spool.etag
		t.spool.mu.Unlock()
		if etag != "" {
			h.Set("ETag", etag)
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	r := t.up.rep
	if t.seekable() {
		h.Set("Accept-Ranges", "bytes")
		h.Set("ETag", r.etag)
	} else {
		h.Set("Accept-Ranges", "none")
	}
	if r.lastModified != "" {
		h.Set("Last-Modified", r.lastModified)
	}
	if r.length >= 0 {
		h.Set("Content-Length", strconv.FormatInt(r.length, 10))
	}
	w.WriteHeader(http.StatusOK)
}

// serveUpstream relays one upstream response (PROTOCOL §13): a byte
// range of a seekable representation, else the whole representation with
// Accept-Ranges: none. Every answer is checked against the pinned
// representation before a byte is sent; a response never combines bytes of
// two upstream responses.
func (s *Service) serveUpstream(w http.ResponseWriter, r *http.Request, t *token) *Error {
	id := t.begin(s.now())
	outcome, failure := outCancelled, (*Error)(nil)
	defer func() { t.finish(id, outcome, failure) }()
	rep := t.up.rep
	seekable := t.seekable()
	start, end, ranged, unsat := clientRange(r.Header.Get("Range"), rep.length)
	if ir := r.Header.Get("If-Range"); ir != "" && ir != rep.etag {
		ranged = false // validators the browser holds are not this representation's
	}
	if !seekable {
		ranged, unsat = false, false
	}
	h := w.Header()
	writeMediaHeaders(h, t.ctype)
	h.Set("Cache-Control", "private, no-transform")
	if unsat {
		h.Set("Content-Range", "bytes */"+strconv.FormatInt(rep.length, 10))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		outcome = outPartial
		return nil
	}
	if !ranged {
		start, end = 0, -1
	}
	o, err := t.up.open(r.Context(), start, end, ranged)
	if err != nil {
		var e *Error
		if !errors.As(err, &e) {
			e = fetchFailure(err)
		}
		if r.Context().Err() != nil {
			return nil
		}
		outcome, failure = outFailed, e
		for _, k := range []string{"Content-Disposition", "Cache-Control"} {
			h.Del(k)
		}
		return e
	}
	defer o.Close()
	if seekable && !o.unranged {
		h.Set("ETag", rep.etag)
		h.Set("Accept-Ranges", "bytes")
	} else {
		h.Set("Accept-Ranges", "none")
	}
	if rep.lastModified != "" {
		h.Set("Last-Modified", rep.lastModified)
	}
	switch o.status {
	case http.StatusRequestedRangeNotSatisfiable:
		h.Set("Content-Range", "bytes */"+strconv.FormatInt(rep.length, 10))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		outcome = outPartial
		return nil
	case http.StatusPartialContent:
		h.Set("Content-Range", "bytes "+strconv.FormatInt(o.start, 10)+"-"+strconv.FormatInt(o.start+o.length-1, 10)+"/"+strconv.FormatInt(rep.length, 10))
		h.Set("Content-Length", strconv.FormatInt(o.length, 10))
		w.WriteHeader(http.StatusPartialContent)
	default:
		if o.length >= 0 {
			h.Set("Content-Length", strconv.FormatInt(o.length, 10))
		}
		w.WriteHeader(http.StatusOK)
	}
	outcome, failure = s.copyOut(w, r, o.body, o.length)
	if outcome == outComplete && o.start+o.length != rep.length {
		outcome = outPartial
	}
	abortIfFailed(outcome)
	return nil
}

// serveSpool serves a spool copy with real ranges and the copy's own
// validators (PROTOCOL §13), waiting for bytes still being copied.
func (s *Service) serveSpool(w http.ResponseWriter, r *http.Request, t *token) *Error {
	id := t.begin(s.now())
	outcome, failure := outCancelled, (*Error)(nil)
	defer func() { t.finish(id, outcome, failure) }()
	f := t.spool
	defer f.lease(s.now)() // the whole stream, waits included, keeps the copy
	total, err := f.waitTotal(r.Context(), t.up.rep.length, t.up.rep.encoding != "")
	if err != nil {
		if r.Context().Err() != nil {
			return nil
		}
		var e *Error
		if !errors.As(err, &e) {
			e = fetchFailure(err)
		}
		outcome, failure = outFailed, e
		return e
	}
	f.mu.Lock()
	etag := f.etag
	f.mu.Unlock()
	start, end, ranged, unsat := clientRange(r.Header.Get("Range"), total)
	if ir := r.Header.Get("If-Range"); ir != "" && ir != etag {
		ranged, unsat = false, false
	}
	h := w.Header()
	writeMediaHeaders(h, t.ctype)
	h.Set("Cache-Control", "private, no-transform")
	h.Set("Accept-Ranges", "bytes")
	if etag != "" {
		h.Set("ETag", etag)
	}
	if unsat {
		h.Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		outcome = outPartial
		return nil
	}
	if !ranged {
		start, end = 0, total-1
	}
	body, err := f.reader(r.Context(), start, end, s.now)
	if err != nil {
		e := fetchFailure(err)
		outcome, failure = outFailed, e
		return e
	}
	defer body.Close()
	length := end - start + 1
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	if ranged {
		h.Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.FormatInt(total, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	outcome, failure = s.copyOut(w, r, body, length)
	if outcome == outComplete && end != total-1 {
		outcome = outPartial
	}
	abortIfFailed(outcome)
	return nil
}

// copyOut streams a body to the browser with a fixed buffer. A browser
// that goes away cancels it; a source that fails or ends early fails the
// stream, and the caller then aborts the connection (abortIfFailed) so the
// browser never takes a short response for the end of the media.
func (s *Service) copyOut(w http.ResponseWriter, r *http.Request, body io.Reader, length int64) (int, *Error) {
	buf := make([]byte, 64<<10)
	rc := http.NewResponseController(w)
	var sent int64
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			_ = rc.SetWriteDeadline(s.streamWrite())
			if _, err := w.Write(buf[:n]); err != nil {
				return outCancelled, nil
			}
			_ = rc.Flush() // what arrived goes on: a slow publisher's first bytes start playback
			sent += int64(n)
		}
		if rerr == io.EOF {
			if length >= 0 && sent != length {
				return outFailed, errTruncated
			}
			return outComplete, nil
		}
		if rerr != nil {
			if r.Context().Err() != nil {
				return outCancelled, nil
			}
			var e *Error
			if !errors.As(rerr, &e) {
				e = errTruncated
			}
			return outFailed, e
		}
	}
}

// abortIfFailed ends a response whose stream failed after its headers were
// sent: the deferred finish has the outcome to record, and the browser sees
// a failed load rather than a short, clean one.
func abortIfFailed(outcome int) {
	if outcome == outFailed {
		panic(http.ErrAbortHandler)
	}
}
