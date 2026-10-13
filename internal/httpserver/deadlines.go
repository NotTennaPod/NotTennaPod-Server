package httpserver

import (
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Request and response deadlines. The header timeout stays 5 s.
const (
	// A request body must arrive at an average of at least bodyRate bytes a
	// second, after a grace of bodyGrace; a body of unknown length is
	// allowed as long as the largest the API accepts.
	bodyRate    = 32 << 10
	bodyUnknown = 8 << 20
	// WriteTimeout bounds an ordinary response from its first byte. Streams
	// (events, media) set their own deadline before every write instead, so
	// a healthy long stream is never cut.
	WriteTimeout = 2 * time.Minute
)

var bodyGrace = 30 * time.Second // a variable for tests

// bodyDeadline is the read deadline of a request body of length n (-1:
// unknown).
func bodyDeadline(now time.Time, n int64) time.Time {
	if n < 0 || n > bodyUnknown {
		n = bodyUnknown
	}
	return now.Add(bodyGrace + time.Duration(n/bodyRate)*time.Second)
}

// deadlineBody clears the body's read deadline once the body has been read
// (or failed, or was closed): net/http then reads the connection in the
// background to notice a client going away, and a deadline left set would
// cancel a long handler's context.
type deadlineBody struct {
	io.ReadCloser
	rc    *http.ResponseController
	clear atomic.Bool
}

func (b *deadlineBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.done()
	}
	return n, err
}

func (b *deadlineBody) Close() error {
	b.done()
	return b.ReadCloser.Close()
}

func (b *deadlineBody) done() {
	if b.clear.CompareAndSwap(false, true) {
		_ = b.rc.SetReadDeadline(time.Time{})
	}
}

// deadlineWriter gives an ordinary response a write deadline from its first
// byte. A handler that sets its own write deadline (a stream renewing it
// before every write) keeps its own.
type deadlineWriter struct {
	http.ResponseWriter
	rc      *http.ResponseController
	own     atomic.Bool
	started atomic.Bool
}

func (w *deadlineWriter) start() {
	if w.started.CompareAndSwap(false, true) && !w.own.Load() {
		_ = w.rc.SetWriteDeadline(time.Now().Add(WriteTimeout))
	}
}

func (w *deadlineWriter) WriteHeader(code int) {
	w.start()
	w.ResponseWriter.WriteHeader(code)
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	w.start()
	return w.ResponseWriter.Write(p)
}

// SetWriteDeadline is the handler's own deadline (http.ResponseController
// finds it here first).
func (w *deadlineWriter) SetWriteDeadline(t time.Time) error {
	w.own.Store(true)
	return w.rc.SetWriteDeadline(t)
}

func (w *deadlineWriter) SetReadDeadline(t time.Time) error { return w.rc.SetReadDeadline(t) }

// Unwrap lets http.ResponseController reach the connection's writer (Flush).
func (w *deadlineWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// withDeadlines applies the deadlines to one request.
func withDeadlines(w http.ResponseWriter, r *http.Request) (http.ResponseWriter, *http.Request) {
	rc := http.NewResponseController(w)
	if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 {
		_ = rc.SetReadDeadline(bodyDeadline(time.Now(), r.ContentLength))
		r.Body = &deadlineBody{ReadCloser: r.Body, rc: rc}
	}
	return &deadlineWriter{ResponseWriter: w, rc: rc}, r
}
