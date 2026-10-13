package media

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// limitedBuffer keeps the first max bytes written to it.
type limitedBuffer struct {
	mu  sync.Mutex
	b   bytes.Buffer
	max int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if room := l.max - l.b.Len(); room > 0 {
		l.b.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

func (l *limitedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// confined builds a process with an empty environment and no input (PROTOCOL §13); the context
// kills it, and Wait never hangs on its pipes. env is the only environment it gets (a reader's
// proxy, or nothing).
func confined(ctx context.Context, env []string, path string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = append([]string{}, env...)
	cmd.WaitDelay = 2 * time.Second
	return cmd
}

// probeEntry is one probe key's result, shared by every waiter (PROTOCOL §13).
type probeEntry struct {
	done    chan struct{}
	facts   *Facts
	over    bool // a host without ranges whose index lies beyond the budget
	expires time.Time
}

type probeCache struct {
	mu      sync.Mutex
	entries map[string]*probeEntry
	sem     chan struct{}
}

// probe learns a representation's facts (PROTOCOL §13): ffprobe reads
// through a loopback reader with bounded total reads, and its answer counts
// only with decoded frames. A probe that fails is inconclusive (nil facts),
// never "unsupported". Results are cached by representation for a day;
// inconclusive ones for five minutes.
func (s *Service) probe(ctx context.Context, key string, src source) (*Facts, bool) {
	if s.tools() == nil {
		return nil, false
	}
	now := s.now()
	s.probes.mu.Lock()
	e := s.probes.entries[key]
	if e != nil {
		select {
		case <-e.done:
			if now.After(e.expires) {
				e = nil
			}
		default:
		}
	}
	owner := e == nil
	if owner {
		e = &probeEntry{done: make(chan struct{})}
		s.probes.entries[key] = e
		for k, x := range s.probes.entries {
			select {
			case <-x.done:
				if now.After(x.expires) {
					delete(s.probes.entries, k)
				}
			default:
			}
		}
	}
	s.probes.mu.Unlock()
	if owner {
		// The probe runs to its own deadline whoever asked first: its waiters
		// share its answer.
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Limits.ProbeDeadline)
		go func() {
			defer cancel()
			e.facts, e.over = s.runProbe(pctx, src)
			ttl := 24 * time.Hour
			if e.facts == nil && !e.over {
				ttl = 5 * time.Minute
			}
			e.expires = s.now().Add(ttl)
			close(e.done)
		}()
	}
	select {
	case <-e.done:
		return e.facts, e.over
	case <-ctx.Done():
		return nil, false
	}
}

func (s *Service) runProbe(ctx context.Context, src source) (*Facts, bool) {
	select {
	case s.probes.sem <- struct{}{}:
		defer func() { <-s.probes.sem }()
	case <-ctx.Done():
		return nil, false
	case <-s.ctx.Done():
		return nil, false
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	// The budget bounds what a probe reads from a publisher; a local copy
	// costs the publisher nothing.
	budget := s.cfg.Limits.ProbeBudget
	if _, local := src.(spoolSource); local {
		budget = 0
	}
	g, in, revoke, err := s.reader.grant(ctx, src, budget)
	if err != nil {
		return nil, false
	}
	defer revoke()
	facts := s.ffprobe(ctx, in, src.seekable())
	g.mu.Lock()
	over := g.over && !src.seekable()
	g.mu.Unlock()
	if facts != nil {
		over = false
	}
	return facts, over
}

// ffprobe runs ffprobe on a reader's input.
func (s *Service) ffprobe(ctx context.Context, in input, seekable bool) *Facts {
	t := s.tools()
	if t == nil {
		return nil
	}
	args := []string{"-hide_banner", "-v", "warning", "-protocol_whitelist", "http,tcp", "-format_whitelist", formatWhitelist}
	if !seekable {
		args = append(args, "-seekable", "0")
	}
	args = append(args,
		"-show_entries", "format=format_name,duration:stream=index,codec_type,codec_name,profile,sample_rate,channels,nb_read_frames:stream_disposition=attached_pic",
		"-count_frames", "-read_intervals", "%+5", "-of", "json", in.url)
	cmd := confined(ctx, in.env, t.ffprobe, args...)
	stdout := &limitedBuffer{max: 1 << 20}
	stderr := &limitedBuffer{max: 64 << 10}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		// A fixed category, never ffprobe's own words: they can quote the
		// media's content, a URL with credentials included.
		s.cfg.Logger.Debug("media: a probe was inconclusive", "exit", exitCategory(err))
		return nil
	}
	f := parseProbe([]byte(stdout.String()), stderr.String())
	if f == nil {
		s.cfg.Logger.Debug("media: a probe found no decodable audio")
	}
	return f
}

// parseProbe reads ffprobe's JSON. The answer counts only with decoded audio
// frames and a demuxer conversions can name.
func parseProbe(out []byte, warnings string) *Facts {
	var p struct {
		Streams []struct {
			CodecType    string `json:"codec_type"`
			CodecName    string `json:"codec_name"`
			Profile      string `json:"profile"`
			SampleRate   string `json:"sample_rate"`
			Channels     int    `json:"channels"`
			NbReadFrames string `json:"nb_read_frames"`
			Disposition  struct {
				AttachedPic int `json:"attached_pic"`
			} `json:"disposition"`
		} `json:"streams"`
		Format struct {
			FormatName string `json:"format_name"`
			Duration   string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &p); err != nil {
		return nil
	}
	f := &Facts{Demuxer: demuxerOf(p.Format.FormatName)}
	if f.Demuxer == "" {
		return nil
	}
	audio := false
	for _, st := range p.Streams {
		switch {
		case st.CodecType == "audio" && !audio:
			frames, _ := strconv.ParseInt(st.NbReadFrames, 10, 64)
			if frames <= 0 {
				return nil
			}
			audio = true
			f.Audio, f.Profile, f.Channels = st.CodecName, st.Profile, st.Channels
			f.SampleRate, _ = strconv.Atoi(st.SampleRate)
		case st.CodecType == "video" && st.Disposition.AttachedPic == 0 && f.Video == "":
			f.Video, f.VideoProf = st.CodecName, st.Profile
		}
	}
	if !audio {
		return nil
	}
	if d, err := strconv.ParseFloat(p.Format.Duration, 64); err == nil && d > 0 && !math.IsInf(d, 0) {
		f.DurationMS = int64(math.Round(d * 1000))
	}
	f.Estimated = strings.Contains(warnings, "Estimating duration from bitrate")
	return f
}

// exitCategory names how a tool ended, for the log, without its output.
func exitCategory(err error) string {
	var exit *exec.ExitError
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "stopped"
	case errors.As(err, &exit) && exit.ExitCode() >= 0:
		return "exit status " + strconv.Itoa(exit.ExitCode())
	case errors.As(err, &exit):
		return "killed"
	}
	return "did not start"
}
