package media

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// spoolFile is a private copy of a representation (PROTOCOL §13) for what
// needs backward access the upstream cannot give: an index beyond the probe
// budget on a host without ranges, a content coding, or an unknown length.
// It is served with real ranges and its own validators (its length and a
// hash of its first MiB), and deleted when no token holds it.
type spoolFile struct {
	key  string
	path string

	mu       sync.Mutex
	grew     chan struct{} // closed and replaced as bytes arrive
	written  int64
	total    int64 // the final length; -1 until known
	done     bool
	err      *Error
	etag     string
	refs     int       // tokens holding it
	active   int       // eviction leases: streams, conversions and probes using it
	lastUsed time.Time // when it was last acquired or a lease ended
	cancel   context.CancelFunc
	removed  bool
	owner    *spools // deletes it once refs and active are both zero
}

// spools holds the spool files: at most a few per server, 1 GiB each.
type spools struct {
	dir      string
	max      int
	maxBytes int64
	headroom func() int64 // free space to keep (PROTOCOL §14 rule)
	free     func(dir string) (int64, bool)
	playing  func(f *spoolFile, now time.Time) bool // the copy serves an account's live playback

	mu    sync.Mutex
	files map[string]*spoolFile
}

var errTooLarge = errors.New("media: the file is larger than the server copies")

// acquire returns the spool for a representation, starting a copy when
// there is none: one more token holds it. It fails with errServerBusy when
// every spool is in use, errNoSpace when the disk lacks room, and
// errTooLarge for a representation longer than the cap.
func (s *spools) acquire(ctx context.Context, key string, up *upstream, now time.Time) (*spoolFile, error) {
	if up.rep.encoding == "" && up.rep.length > s.maxBytes {
		return nil, errTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.files[key]; f != nil {
		f.mu.Lock()
		ok := f.err == nil
		if ok {
			f.refs++
			f.lastUsed = now
		}
		f.mu.Unlock()
		if ok {
			return f, nil
		}
		s.removeLocked(f)
	}
	if len(s.files) >= s.max {
		// Evict the least recently used copy that holds no lease (no
		// stream, conversion or probe is using it) and serves no live
		// playback (a paused one included).
		var victim *spoolFile
		var victimUsed time.Time
		for _, f := range s.files {
			f.mu.Lock()
			leased, used := f.active > 0, f.lastUsed
			f.mu.Unlock()
			if !leased && !s.playing(f, now) && (victim == nil || used.Before(victimUsed)) {
				victim, victimUsed = f, used
			}
		}
		if victim == nil {
			return nil, errServerBusy
		}
		s.removeLocked(victim)
	}
	need := s.maxBytes
	if up.rep.encoding == "" && up.rep.length >= 0 {
		need = up.rep.length
	}
	if err := private.RealDir(s.dir); err != nil {
		return nil, err
	}
	if free, ok := s.free(s.dir); ok && free-need < s.headroom() {
		return nil, errNoSpace
	}
	fh, err := os.CreateTemp(s.dir, "spool-*")
	if err != nil {
		return nil, err
	}
	fctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	f := &spoolFile{key: key, path: fh.Name(), grew: make(chan struct{}), total: -1, refs: 1, lastUsed: now, cancel: cancel, owner: s}
	s.files[key] = f
	go s.fill(fctx, f, fh, up)
	return f, nil
}

// release drops a token's hold. The copy is deleted once no token holds it
// and no lease uses it: a conversion reading it outlives its token's
// eviction.
func (s *spools) release(f *spoolFile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f.mu.Lock()
	f.refs--
	unused := f.refs <= 0 && f.active <= 0
	f.mu.Unlock()
	if unused {
		s.removeLocked(f)
	}
}

// leaseEnded deletes a copy whose last lease ended after its last token
// released it.
func (s *spools) leaseEnded(f *spoolFile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f.mu.Lock()
	unused := f.refs <= 0 && f.active <= 0
	f.mu.Unlock()
	if unused {
		s.removeLocked(f)
	}
}

func (s *spools) removeLocked(f *spoolFile) {
	if s.files[f.key] == f {
		delete(s.files, f.key)
	}
	f.mu.Lock()
	if !f.removed {
		f.removed = true
		f.cancel()
		if f.err == nil && !f.done {
			f.err = newError(http.StatusBadGateway, "media_unavailable", "io")
		}
		close(f.grew)
		f.grew = make(chan struct{})
	}
	f.mu.Unlock()
	_ = os.Remove(f.path)
}

// closeAll deletes every copy (shutdown).
func (s *spools) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.files {
		s.removeLocked(f)
	}
}

// fill copies the representation, decoding a content coding, checking the
// cap and the free space as it goes.
func (s *spools) fill(ctx context.Context, f *spoolFile, fh *os.File, up *upstream) {
	defer fh.Close()
	finish := func(e *Error) {
		f.settle(e)
		if e != nil {
			// A failed copy can serve nothing: its partial file goes and its
			// place is free at once. Its tokens keep the failure to report.
			s.mu.Lock()
			s.removeLocked(f)
			s.mu.Unlock()
		}
	}
	o, err := up.open(ctx, 0, -1, false)
	if err != nil {
		finish(fetchFailure(err))
		return
	}
	defer o.Close()
	body, derr := decoded(o.body, up.rep.encoding)
	if derr != nil {
		finish(derr)
		return
	}
	hash := sha256.New()
	buf := make([]byte, 64<<10)
	var sinceCheck int64
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			if f.written+int64(n) > s.maxBytes {
				finish(newError(http.StatusBadGateway, "media_unavailable", "size_limit"))
				return
			}
			if _, err := fh.Write(buf[:n]); err != nil {
				finish(errNoSpace)
				return
			}
			if f.written < 1<<20 {
				hash.Write(buf[:min(int64(n), 1<<20-f.written)])
			}
			sinceCheck += int64(n)
			if sinceCheck >= 16<<20 {
				sinceCheck = 0
				if free, ok := s.free(s.dir); ok && free < s.headroom() {
					finish(errNoSpace)
					return
				}
			}
			f.mu.Lock()
			f.written += int64(n)
			if f.written >= 1<<20 && f.etag == "" && up.rep.encoding == "" && up.rep.length >= 0 {
				f.etag = `"ntp-` + hex.EncodeToString(hash.Sum(nil))[:32] + "-" + strconv.FormatInt(up.rep.length, 10) + `"`
			}
			close(f.grew)
			f.grew = make(chan struct{})
			f.mu.Unlock()
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			finish(fetchFailure(rerr))
			return
		}
	}
	if up.rep.encoding == "" && up.rep.length >= 0 && f.written != up.rep.length {
		finish(errTruncated)
		return
	}
	f.mu.Lock()
	if f.etag == "" {
		f.etag = `"ntp-` + hex.EncodeToString(hash.Sum(nil))[:32] + "-" + strconv.FormatInt(f.written, 10) + `"`
	}
	f.mu.Unlock()
	finish(nil)
}

// settle records the copy's end, its failure or its completion and length,
// and wakes its waiters. A removed copy keeps its own end.
func (f *spoolFile) settle(e *Error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removed {
		return
	}
	if e != nil {
		f.err = e
	} else {
		f.done, f.total = true, f.written
	}
	close(f.grew)
	f.grew = make(chan struct{})
}

// knownTotal is the copy's final length as soon as it is known: the
// upstream's declared length for an unencoded copy, else the copy's own
// when it is done.
func (f *spoolFile) knownTotal(declared int64, encoded bool) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.knownTotalLocked(declared, encoded)
}

func (f *spoolFile) knownTotalLocked(declared int64, encoded bool) int64 {
	if f.done {
		return f.total
	}
	if !encoded && declared >= 0 {
		return declared
	}
	return -1
}

// waitTotal waits until the copy's length is known. The length, a failure
// and the wake channel are read together: a completion between two reads
// would replace the channel and never close the one waited on (server bug
// hunt 2026-10, SB-2).
func (f *spoolFile) waitTotal(ctx context.Context, declared int64, encoded bool) (int64, error) {
	for {
		f.mu.Lock()
		t, err, ch := f.knownTotalLocked(declared, encoded), f.err, f.grew
		f.mu.Unlock()
		if t >= 0 {
			return t, nil
		}
		if err != nil {
			return 0, err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// waitDone waits until the copy is complete.
func (f *spoolFile) waitDone(ctx context.Context) error {
	for {
		f.mu.Lock()
		done, err, ch := f.done, f.err, f.grew
		f.mu.Unlock()
		switch {
		case err != nil:
			return err
		case done:
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// lease keeps the copy from eviction until the returned release is called
// (PROTOCOL §13: a copy in use is never evicted).
func (f *spoolFile) lease(now func() time.Time) (release func()) {
	f.mu.Lock()
	f.active++
	f.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			f.active--
			f.lastUsed = now()
			f.mu.Unlock()
			if f.owner != nil {
				f.owner.leaseEnded(f)
			}
		})
	}
}

// reader reads [start, end] of the copy, waiting for bytes still coming. It
// holds a lease until closed.
func (f *spoolFile) reader(ctx context.Context, start, end int64, now func() time.Time) (io.ReadCloser, error) {
	fh, err := os.Open(f.path)
	if err != nil {
		return nil, newError(http.StatusBadGateway, "media_unavailable", "io")
	}
	return &spoolReader{f: f, fh: fh, ctx: ctx, pos: start, end: end, release: f.lease(now)}, nil
}

type spoolReader struct {
	f       *spoolFile
	fh      *os.File
	ctx     context.Context
	pos     int64
	end     int64 // inclusive
	release func()
}

func (r *spoolReader) Read(p []byte) (int, error) {
	if r.pos > r.end {
		return 0, io.EOF
	}
	for {
		r.f.mu.Lock()
		written, done, ferr, ch := r.f.written, r.f.done, r.f.err, r.f.grew
		r.f.mu.Unlock()
		if ferr != nil {
			return 0, ferr
		}
		if r.pos < written {
			want := min(int64(len(p)), written-r.pos, r.end-r.pos+1)
			n, err := r.fh.ReadAt(p[:want], r.pos)
			r.pos += int64(n)
			if err != nil && err != io.EOF {
				return n, newError(http.StatusBadGateway, "media_unavailable", "io")
			}
			return n, nil
		}
		if done {
			return 0, errTruncated
		}
		select {
		case <-ch:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
}

func (r *spoolReader) Close() error {
	r.release()
	return r.fh.Close()
}

// decoded undoes an HTTP content coding: gzip, or deflate, which HTTP
// defines as zlib-wrapped and some servers send raw.
func decoded(r io.Reader, coding string) (io.Reader, *Error) {
	switch coding {
	case "":
		return r, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, newError(http.StatusBadGateway, "media_unavailable", "io")
		}
		return zr, nil
	case "deflate":
		br := bufio.NewReader(r)
		if h, err := br.Peek(2); err == nil && h[0]&0x0f == 8 && (uint16(h[0])<<8|uint16(h[1]))%31 == 0 {
			zr, err := zlib.NewReader(br)
			if err != nil {
				return nil, newError(http.StatusBadGateway, "media_unavailable", "io")
			}
			return zr, nil
		}
		return flate.NewReader(br), nil
	}
	return nil, newError(http.StatusBadGateway, "media_unavailable", "http_status")
}

// clearDir empties the spool directory (at start: copies never outlive the process).
func clearDir(dir string) {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(dir, e.Name()))
	}
}
