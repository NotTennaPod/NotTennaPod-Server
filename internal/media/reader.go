package media

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// source is what a loopback reader serves: a pinned representation through
// the relay's own fetch rules, a spool copy, or (for the startup check) a
// local sample.
type source interface {
	// size is the total length, waiting for a spool copy's when needed.
	size(ctx context.Context) (int64, error)
	// seekable reports random access.
	seekable() bool
	// read returns bytes [start, end] (end -1: to the end); a source that is
	// not seekable is read from its start and the bytes before start are
	// skipped by the caller.
	read(ctx context.Context, start, end int64) (io.ReadCloser, error)
}

type upstreamSource struct{ up *upstream }

func (s upstreamSource) size(context.Context) (int64, error) { return s.up.rep.length, nil }

func (s upstreamSource) seekable() bool {
	return s.up.rep.ranges && s.up.rep.encoding == "" && !s.up.unranged()
}

func (s upstreamSource) read(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if !s.seekable() {
		start, end = 0, -1
	}
	o, err := s.up.open(ctx, start, end, start > 0 || end >= 0)
	if err != nil {
		return nil, err
	}
	if o.status == http.StatusRequestedRangeNotSatisfiable {
		return nil, errBadRange
	}
	if o.unranged && start > 0 {
		return nil, errBadRange // the reader asked for random access the host stopped giving
	}
	return o.body, nil
}

type spoolSource struct {
	f        *spoolFile
	declared int64
	encoded  bool
	now      func() time.Time
}

func (s spoolSource) size(ctx context.Context) (int64, error) {
	return s.f.waitTotal(ctx, s.declared, s.encoded)
}

func (s spoolSource) seekable() bool { return true }

func (s spoolSource) read(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	total, err := s.size(ctx)
	if err != nil {
		return nil, err
	}
	if end < 0 || end >= total {
		end = total - 1
	}
	return s.f.reader(ctx, start, end, s.now)
}

// fileSource serves a local file (the startup check's samples).
type fileSource struct{ path string }

func (s fileSource) size(context.Context) (int64, error) {
	st, err := os.Stat(s.path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (s fileSource) seekable() bool { return true }

func (s fileSource) read(_ context.Context, start, end int64) (io.ReadCloser, error) {
	fh, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	if _, err := fh.Seek(start, io.SeekStart); err != nil {
		fh.Close()
		return nil, err
	}
	if end < 0 {
		return fh, nil
	}
	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(fh, end-start+1), fh}, nil
}

// loopback is the readers' listener (PROTOCOL §13): an ephemeral 127.0.0.1
// port, with one random 128-bit capability per probe or conversion, bound to
// one source and revoked when the process exits. ffprobe and ffmpeg read
// only through it, so they never see a publisher's URL or a redirect, and
// every byte they read comes through the relay's destination and credential
// rules.
//
// The capability never appears in a command line, which other users can
// read (ps, /proc): the tool reads a URL without any secret through the
// reader as its HTTP proxy, and the capability travels as the proxy's
// credentials, from the tool's environment (PROTOCOL §13).
type loopback struct {
	mu     sync.Mutex
	ln     net.Listener
	srv    *http.Server
	grants map[string]*grant
}

// grant is one reader capability.
type grant struct {
	id     string
	src    source
	budget int64 // the most bytes it reads from its source; 0 unlimited
	ctx    context.Context

	mu      sync.Mutex
	used    int64
	over    bool   // a read went beyond the budget
	failure *Error // what the source failed with
	sawEnd  bool   // it delivered the source's final byte
}

// outcome is what the source failed with, and whether its final byte was
// delivered.
func (g *grant) outcome() (*Error, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failure, g.sawEnd
}

func (g *grant) note(f func(g *grant)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	f(g)
}

// open starts the listener on first use.
func (l *loopback) open() error {
	if l.ln != nil {
		return nil
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	l.ln = ln
	l.srv = &http.Server{Handler: http.HandlerFunc(l.serve), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = l.srv.Serve(ln) }()
	return nil
}

// input is what a tool is given to read a grant: a URL without any secret,
// and the environment that names the reader as its HTTP proxy with the
// capability as the proxy's credentials.
type input struct {
	url string
	env []string
}

// readerURL is every grant's URL: the reader, as the proxy, serves it.
const readerURL = "http://reader.invalid/c"

// grant opens a capability over src; revoke ends it.
func (l *loopback) grant(ctx context.Context, src source, budget int64) (*grant, input, func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.open(); err != nil {
		return nil, input{}, nil, err
	}
	g := &grant{id: newTokenID(), src: src, budget: budget, ctx: ctx}
	if l.grants == nil {
		l.grants = map[string]*grant{}
	}
	l.grants[g.id] = g
	in := input{url: readerURL, env: []string{"http_proxy=http://g:" + g.id + "@" + l.ln.Addr().String()}}
	revoke := func() {
		l.mu.Lock()
		delete(l.grants, g.id)
		l.mu.Unlock()
	}
	return g, in, revoke, nil
}

// capability is the grant a request names in its proxy credentials ("" for
// none).
func capability(r *http.Request) string {
	v, ok := strings.CutPrefix(r.Header.Get("Proxy-Authorization"), "Basic ")
	if !ok {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(v))
	if err != nil {
		return ""
	}
	id, ok := strings.CutPrefix(string(b), "g:")
	if !ok {
		return ""
	}
	return id
}

func (l *loopback) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.srv != nil {
		_ = l.srv.Close()
	}
	l.grants = nil
}

// serve answers ffprobe's and ffmpeg's requests: GET and HEAD, one byte
// range at a time.
func (l *loopback) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/c" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	id := capability(r)
	l.mu.Lock()
	g := l.grants[id]
	l.mu.Unlock()
	if id == "" || g == nil {
		// The tool sends its credentials once challenged.
		w.Header().Set("Proxy-Authenticate", `Basic realm="reader"`)
		http.Error(w, "proxy credentials required", http.StatusProxyAuthRequired)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	total, err := g.src.size(ctx)
	if err != nil {
		g.note(func(g *grant) { g.failure = fetchFailure(err) })
		http.Error(w, "unavailable", http.StatusBadGateway)
		return
	}
	seekable := g.src.seekable()
	start, end, ranged := int64(0), int64(-1), false
	if v := r.Header.Get("Range"); v != "" {
		if a, b, ok := parseOneRange(v); ok {
			start, end, ranged = a, b, true
		}
	}
	if total >= 0 && start > 0 && start >= total {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if total >= 0 && (end < 0 || end >= total) {
		end = total - 1
	}
	// Without random access, a later offset is reached by skipping bytes:
	// only within a budget (the probe's).
	skip := int64(0)
	if !seekable && start > 0 {
		if g.budget == 0 || start >= g.budget {
			g.note(func(g *grant) { g.over = true })
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		skip = start
	}
	// A source without random access answers 200 from its start, so ffmpeg
	// treats it as a stream; only the probe's skip within its budget answers
	// a later offset, as a 206.
	partial := ranged && (seekable || start > 0)
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	if seekable {
		h.Set("Accept-Ranges", "bytes")
	}
	if total >= 0 {
		h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		if partial {
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		}
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	body, err := g.src.read(ctx, start, end)
	if err != nil {
		if ctx.Err() == nil {
			g.note(func(g *grant) { g.failure = fetchFailure(err) })
		}
		http.Error(w, "unavailable", http.StatusBadGateway)
		return
	}
	defer body.Close()
	if partial && total >= 0 {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	buf := make([]byte, 64<<10)
	pos := int64(0) // offset in the source of the next byte read
	if seekable {
		pos = start
	}
	for {
		want := int64(len(buf))
		if g.budget > 0 {
			g.mu.Lock()
			left := g.budget - g.used
			if left <= 0 {
				g.over = true
			}
			g.mu.Unlock()
			if left <= 0 {
				return // the probe's reads are bounded: ffprobe sees a short answer
			}
			want = min(want, left)
		}
		n, rerr := body.Read(buf[:want])
		if n > 0 {
			g.note(func(g *grant) { g.used += int64(n) })
			chunk := buf[:n]
			if skip > 0 {
				d := min(skip, int64(n))
				chunk, skip = chunk[d:], skip-d
			}
			pos += int64(n)
			if len(chunk) > 0 {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
			if total >= 0 && pos >= total {
				g.note(func(g *grant) { g.sawEnd = true })
			}
		}
		if rerr == io.EOF {
			if total < 0 {
				g.note(func(g *grant) { g.sawEnd = true })
			}
			return
		}
		if rerr != nil {
			if ctx.Err() == nil || errors.Is(rerr, errStalled) {
				g.note(func(g *grant) { g.failure = fetchFailure(rerr) })
			}
			return
		}
	}
}

// parseOneRange reads a single "bytes=a-b" or "bytes=a-" range.
func parseOneRange(v string) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(strings.TrimSpace(v), "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	a, b, ok := strings.Cut(spec, "-")
	if !ok || a == "" {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	end := int64(-1)
	if b != "" {
		if end, err = strconv.ParseInt(b, 10, 64); err != nil || end < start {
			return 0, 0, false
		}
	}
	return start, end, true
}
