package media

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// conversions admits ffmpeg processes (PROTOCOL §13): at most a few per
// server and one per account. A new conversion for an account ends the
// account's older one: a seek reloads, and the reload must not wait for or
// be refused by the stream it replaces.
type conversions struct {
	mu        sync.Mutex
	max       int
	running   int
	byAccount map[string]*slot
}

type slot struct {
	c         *conversions
	account   string
	cancel    context.CancelFunc
	done      chan struct{}
	preempted atomic.Bool
	once      sync.Once
}

// acquire takes a slot whose context ends with ctx, with parent (the
// service's), or when a newer conversion of the account preempts it.
func (c *conversions) acquire(ctx, parent context.Context, account string) (*slot, context.Context, error) {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		if old := c.byAccount[account]; old != nil {
			old.preempted.Store(true)
			old.cancel()
			c.mu.Unlock()
			select {
			case <-old.done:
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-deadline.C:
				return nil, nil, errServerBusy
			}
		}
		if c.running >= c.max {
			c.mu.Unlock()
			return nil, nil, errServerBusy
		}
		rctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(parent, cancel)
		s := &slot{c: c, account: account, done: make(chan struct{})}
		s.cancel = func() { stop(); cancel() }
		c.running++
		c.byAccount[account] = s
		c.mu.Unlock()
		return s, rctx, nil
	}
}

func (s *slot) release() {
	s.once.Do(func() {
		s.cancel()
		s.c.mu.Lock()
		s.c.running--
		if s.c.byAccount[s.account] == s {
			delete(s.c.byAccount, s.account)
		}
		s.c.mu.Unlock()
		close(s.done)
	})
}

func formatSeconds(ms int64) string {
	return strconv.FormatInt(ms/1000, 10) + "." + strconv.FormatInt(1000+ms%1000, 10)[1:]
}

// conversionArgs are ffmpeg's arguments for a compatibility stream (PROTOCOL §13): the probed
// demuxer named, never detected; only http and tcp; the format whitelist; mov's external
// references off; always transcoded. With random access the start is an input seek with
// decoding, which starts exactly at the time; without it, ffmpeg reads from the start and
// discards to the time (an output seek). Output timestamps start at 0, so the media time is
// start_ms plus the element's time.
func conversionArgs(f *Facts, p *Profile, startMS int64, seekable bool, in string) []string {
	a := []string{"-nostdin", "-hide_banner", "-nostats", "-loglevel", "error", "-xerror",
		"-threads", "1", "-protocol_whitelist", "http,tcp", "-format_whitelist", formatWhitelist}
	if !seekable {
		a = append(a, "-seekable", "0")
	}
	if f.Demuxer == "mov" {
		a = append(a, "-enable_drefs", "0")
	}
	a = append(a, "-f", f.Demuxer)
	if startMS > 0 && seekable {
		a = append(a, "-ss", formatSeconds(startMS))
	}
	a = append(a, "-i", in, "-map", "0:a:0", "-vn", "-sn", "-dn", "-threads", "1")
	if startMS > 0 && !seekable {
		a = append(a, "-ss", formatSeconds(startMS))
	}
	if f.Channels > 2 {
		a = append(a, "-ac", "2")
	}
	a = append(a, "-output_ts_offset", "0", "-avoid_negative_ts", "make_zero", "-max_muxing_queue_size", "256")
	a = append(a, p.args...)
	return append(a, "-progress", "pipe:3", "pipe:1")
}

// conversion is one ffmpeg process feeding a writer.
type conversion struct {
	cmd      *exec.Cmd
	stdout   io.ReadCloser
	stderr   *limitedBuffer
	progress chan progress
	stalled  atomic.Bool
	kill     context.CancelFunc
}

type progress struct {
	outMS int64
	ended bool
}

// readProgress follows ffmpeg's -progress output.
func readProgress(r io.ReadCloser, out chan<- progress) {
	defer r.Close()
	var p progress
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), "=")
		if !ok {
			continue
		}
		switch k {
		case "out_time_us":
			if us, err := strconv.ParseInt(v, 10, 64); err == nil && us >= 0 {
				p.outMS = us / 1000
			}
		case "progress":
			p.ended = v == "end"
		}
	}
	out <- p
}

// startConversion starts ffmpeg with args; ctx kills it.
func (s *Service) startConversion(ctx context.Context, env []string, args []string) (*conversion, error) {
	t := s.tools()
	if t == nil {
		return nil, errNoTools
	}
	ctx, kill := context.WithCancel(ctx)
	cmd := confined(ctx, env, t.ffmpeg, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		kill()
		return nil, err
	}
	stderr := &limitedBuffer{max: 64 << 10}
	cmd.Stderr = stderr
	pr, pw, err := os.Pipe()
	if err != nil {
		kill()
		return nil, err
	}
	cmd.ExtraFiles = []*os.File{pw} // fd 3: -progress pipe:3
	if err := cmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		kill()
		return nil, err
	}
	pw.Close()
	c := &conversion{cmd: cmd, stdout: stdout, stderr: stderr, progress: make(chan progress, 1), kill: kill}
	go readProgress(pr, c.progress)
	return c, nil
}

// read reads ffmpeg's output; a read that waits longer than the watchdog
// (the process produces nothing while its reader is served) kills it.
func (c *conversion) read(p []byte, watchdog time.Duration) (int, error) {
	t := time.AfterFunc(watchdog, func() {
		c.stalled.Store(true)
		c.kill()
	})
	defer t.Stop()
	return c.stdout.Read(p)
}

// wait reaps the process and reports whether it ran clean to the end:
// exit 0, nothing on stderr (it logs errors only), and its progress ended.
func (c *conversion) wait() (clean bool, outMS int64) {
	err := c.cmd.Wait()
	c.kill()
	p := <-c.progress
	return err == nil && strings.TrimSpace(c.stderr.String()) == "" && p.ended && !c.stalled.Load(), p.outMS
}

// completed is PROTOCOL §13 conservative completion: the run was clean, its reader
// saw no failure, and its output reached the original duration within 2 s
// (or, for a duration ffprobe only estimated, the reader delivered the
// source's final byte). Without a known duration a clean run completes.
func completed(clean bool, startMS, outMS, durationMS int64, estimated, sawEnd bool) bool {
	if !clean {
		return false
	}
	if durationMS <= 0 {
		return true
	}
	if d := startMS + outMS - durationMS; d >= -2000 && d <= 2000 {
		return true
	}
	return estimated && sawEnd
}

// serveCompat streams a conversion of the token's source from startMS.
func (s *Service) serveCompat(w http.ResponseWriter, r *http.Request, t *token, startMS int64) *Error {
	id := t.begin(s.now())
	outcome, failure := outCancelled, (*Error)(nil)
	defer func() { t.finish(id, outcome, failure) }()
	fail := func(e *Error) *Error {
		outcome, failure = outFailed, e
		return e
	}
	sl, ctx, err := s.conv.acquire(r.Context(), s.ctx, t.account)
	if err != nil {
		if r.Context().Err() != nil {
			return nil
		}
		return fail(errServerBusy)
	}
	defer sl.release()
	// A replaced, cancelled or shut-down stream stops writing at once, even
	// to a browser that no longer reads it.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(s.streamWrite())
	stopDeadline := context.AfterFunc(ctx, func() { _ = rc.SetWriteDeadline(time.Now()) })
	defer stopDeadline()
	if t.spool != nil {
		// The conversion keeps the copy for its whole run, the gaps between
		// ffmpeg's requests included.
		defer t.spool.lease(s.now)()
		// Every copied representation needs its whole copy (an index at its
		// end, or a length known only at the end): wait for it, so the
		// watchdog below times ffmpeg, not the download.
		if err := t.spool.waitDone(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fail(fetchFailure(err))
		}
	}
	src := t.source()
	g, in, revoke, err := s.reader.grant(ctx, src, 0)
	if err != nil {
		return fail(errServerBusy)
	}
	defer revoke()
	c, err := s.startConversion(ctx, in.env, conversionArgs(t.facts, t.profile, startMS, src.seekable(), in.url))
	if err != nil {
		return fail(errServerBusy)
	}
	buf := make([]byte, 64<<10)
	watchdog := 2 * s.cfg.Limits.Stall
	n, rerr := c.read(buf, watchdog)
	if n == 0 {
		clean, outMS := c.wait()
		gf, sawEnd := g.outcome()
		switch {
		case r.Context().Err() != nil || sl.preempted.Load():
			panic(http.ErrAbortHandler) // replaced or gone before any output
		case gf != nil:
			return fail(gf)
		case c.stalled.Load():
			return fail(errStalled)
		case completed(clean, startMS, outMS, t.durationMS, t.facts.Estimated, sawEnd):
			// Nothing left after the start (a start at the very end).
			outcome = outComplete
			writeMediaHeaders(w.Header(), t.ctype)
			w.Header().Set("Accept-Ranges", "none")
			w.Header().Set("Cache-Control", "no-store, no-transform")
			w.WriteHeader(http.StatusOK)
			return nil
		}
		return fail(errUnplayable)
	}
	h := w.Header()
	writeMediaHeaders(h, t.ctype)
	h.Set("Accept-Ranges", "none")
	h.Set("Cache-Control", "no-store, no-transform")
	w.WriteHeader(http.StatusOK)
	writeErr := false
	for {
		if n > 0 {
			_ = rc.SetWriteDeadline(s.streamWrite())
			if _, err := w.Write(buf[:n]); err != nil {
				writeErr = true
				c.kill()
				break
			}
			_ = rc.Flush()
		}
		if rerr != nil {
			break
		}
		n, rerr = c.read(buf, watchdog)
	}
	clean, outMS := c.wait()
	gf, sawEnd := g.outcome()
	switch {
	case writeErr || r.Context().Err() != nil || sl.preempted.Load():
		// The browser went away or the player started another stream.
	case gf != nil:
		fail(gf)
	case c.stalled.Load():
		fail(errStalled)
	case completed(clean, startMS, outMS, t.durationMS, t.facts.Estimated, sawEnd):
		outcome = outComplete
	case clean:
		fail(errTruncated) // ended clean but short of the duration: never complete
	default:
		fail(errUnplayable)
	}
	if outcome != outComplete {
		// Only a complete conversion ends its response cleanly: the browser
		// must not take any other end for the episode's.
		panic(http.ErrAbortHandler)
	}
	return nil
}
