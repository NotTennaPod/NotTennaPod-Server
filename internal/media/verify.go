package media

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// toolset is the ffmpeg and ffprobe the server found.
type toolset struct {
	ffmpeg, ffprobe string
	version         string
}

// tools are ffmpeg and ffprobe once found (probes use them even when no
// conversion passed the check); nil without them.
func (s *Service) tools() *toolset {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tl
}

// Problem is a coverage matrix entry the server cannot serve, with why.
type Problem struct {
	Source  string `json:"source"`
	Profile string `json:"profile,omitempty"`
	Reason  string `json:"reason"`
}

// Matrix is the result of the startup check (PROTOCOL §13): which
// source formats convert to which output profiles on this server.
type Matrix struct {
	State     string              `json:"state"` // checking, ready, degraded or unavailable
	Reason    string              `json:"reason,omitempty"`
	FFmpeg    string              `json:"ffmpeg,omitempty"`
	FFprobe   string              `json:"ffprobe,omitempty"`
	Version   string              `json:"version,omitempty"`
	Verified  map[string][]string `json:"verified"` // source → profiles
	Problems  []Problem           `json:"problems"`
	CheckedAt int64               `json:"checked_at,omitempty"`
	Checking  bool                `json:"checking"` // a check is running
	Usable    bool                `json:"-"`        // some conversion works
	ok        map[string]map[string]bool
}

// allows reports whether a probed source converts to a profile: its matrix
// row when it has one, else any verified row of its demuxer (the probe's
// decoded frames show its codec decodes).
func (m *Matrix) allows(f *Facts, profile string) bool {
	if m == nil {
		return false
	}
	if k := f.sourceKey(); k != "" {
		return m.ok[k][profile]
	}
	for k, row := range m.ok {
		if strings.HasPrefix(k, f.Demuxer+"/") && row[profile] {
			return true
		}
	}
	return false
}

// matrixSource is a coverage matrix row: a format the phones play (PROTOCOL §13).
type matrixSource struct {
	key  string   // demuxer/codec, as Facts.sourceKey names it
	name string   // for the service page
	gen  []string // ffmpeg arguments that encode the sample
}

var matrixSources = []matrixSource{
	{"mp3/mp3", "MP3", []string{"-c:a", "libmp3lame", "-b:a", "96k", "-f", "mp3"}},
	{"mov/aac", "AAC (M4A)", []string{"-c:a", "aac", "-b:a", "96k", "-f", "mp4"}},
	{"mov/alac", "ALAC (M4A)", []string{"-c:a", "alac", "-f", "mp4"}},
	{"aac/aac", "AAC (ADTS)", []string{"-c:a", "aac", "-b:a", "96k", "-f", "adts"}},
	{"flac/flac", "FLAC", []string{"-c:a", "flac", "-f", "flac"}},
	{"ogg/vorbis", "Ogg Vorbis", []string{"-c:a", "vorbis", "-strict", "experimental", "-ac", "2", "-f", "ogg"}},
	{"ogg/opus", "Ogg Opus", []string{"-c:a", "libopus", "-b:a", "64k", "-f", "ogg"}},
	{"ogg/flac", "Ogg FLAC", []string{"-c:a", "flac", "-f", "ogg"}},
	{"matroska/opus", "WebM Opus", []string{"-c:a", "libopus", "-b:a", "64k", "-f", "webm"}},
	{"matroska/vorbis", "WebM Vorbis", []string{"-c:a", "vorbis", "-strict", "experimental", "-ac", "2", "-f", "webm"}},
	{"wav/pcm_s16le", "WAV", []string{"-c:a", "pcm_s16le", "-f", "wav"}},
	{"aiff/pcm_s16be", "AIFF", []string{"-c:a", "pcm_s16be", "-f", "aiff"}},
	{"mov/video+aac", "MP4 video", []string{"-c:v", "mpeg4", "-c:a", "aac", "-b:a", "96k", "-f", "mp4"}},
}

// Required tools for acceptance (PROTOCOL §13).
var (
	requiredEncoders = []string{"aac", "libopus"}
	requiredDemuxers = []string{"ogg", "matroska", "mov", "flac", "wav"}
)

// MatrixStatus is the latest check, for the service page.
func (s *Service) MatrixStatus() Matrix {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.matrix == nil {
		return Matrix{State: "checking", Checking: true, Verified: map[string][]string{}, Problems: []Problem{}}
	}
	m := *s.matrix
	m.Checking = s.checking.Load()
	return m
}

// Recheck runs the check again in the background (the service page asks);
// conversions keep the previous result until it ends.
func (s *Service) Recheck() {
	if !s.checking.CompareAndSwap(false, true) {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.Verify(s.ctx)
	}()
}

// awaitMatrix waits for the first check (it runs at start, in seconds).
func (s *Service) awaitMatrix(ctx context.Context) *Matrix {
	select {
	case <-s.verified:
	case <-ctx.Done():
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.matrix == nil || !s.matrix.Usable {
		return nil
	}
	return s.matrix
}

// Verify finds ffmpeg and ffprobe and checks the whole matrix: for every
// source format it generates a two-second sample with lavfi's sine source,
// probes it, converts it from 0.5 s through a loopback reader with the
// conversion's own arguments to every output profile, and decodes the
// result. Only conversions that pass are advertised (PROTOCOL §13).
func (s *Service) Verify(ctx context.Context) {
	s.checking.Store(true)
	defer s.checking.Store(false)
	m := s.check(ctx)
	s.mu.Lock()
	s.matrix = m
	s.mu.Unlock()
	select {
	case <-s.verified:
	default:
		close(s.verified)
	}
	switch m.State {
	case "ready":
		s.cfg.Logger.Info("media: the compatibility path is ready", "ffmpeg", m.Version)
	case "degraded":
		s.cfg.Logger.Warn("media: the compatibility path is degraded", "reason", m.Reason, "problems", len(m.Problems))
	default:
		s.cfg.Logger.Warn("media: the compatibility path is unavailable; the relay works", "reason", m.Reason)
	}
}

func (s *Service) check(ctx context.Context) *Matrix {
	m := &Matrix{State: "unavailable", Verified: map[string][]string{}, Problems: []Problem{}, ok: map[string]map[string]bool{},
		CheckedAt: s.now().UnixMilli()}
	if s.cfg.NoTools {
		m.Reason = "ffmpeg is not used (configuration)"
		return m
	}
	tl, reason := locate(s.cfg.FFmpeg, s.cfg.FFprobe)
	if tl == nil {
		m.Reason = reason
		return m
	}
	m.FFmpeg, m.FFprobe, m.Version = tl.ffmpeg, tl.ffprobe, tl.version
	s.mu.Lock()
	s.tl = tl
	s.mu.Unlock()
	var missing []string
	if major := majorVersion(tl.version); major != 0 && major < 6 {
		missing = append(missing, "ffmpeg 6 or later (found "+tl.version+")")
	}
	encoders := listNames(ctx, tl.ffmpeg, "-encoders")
	demuxers := listNames(ctx, tl.ffmpeg, "-demuxers")
	for _, e := range requiredEncoders {
		if !encoders[e] {
			missing = append(missing, "the "+e+" encoder")
		}
	}
	for _, d := range requiredDemuxers {
		if !demuxers[d] {
			missing = append(missing, "the "+d+" demuxer")
		}
	}
	// A new server's spool folder may not exist yet: create it and try again
	// (an inner err here once hid the retry's success, so a fresh data
	// directory never converted until the next start).
	dir, err := os.MkdirTemp(s.cfg.SpoolDir, "check-")
	if err != nil && private.RealDir(s.cfg.SpoolDir) == nil {
		dir, err = os.MkdirTemp(s.cfg.SpoolDir, "check-")
	}
	if err != nil {
		m.Reason = "cannot write samples: " + err.Error()
		return m
	}
	defer os.RemoveAll(dir)
	for _, src := range matrixSources {
		if ctx.Err() != nil {
			m.Reason = "stopped"
			return m
		}
		for _, p := range s.checkSource(ctx, tl, dir, src, m) {
			m.Problems = append(m.Problems, p)
		}
	}
	for k, row := range m.ok {
		for p, ok := range row {
			if ok {
				m.Verified[k] = append(m.Verified[k], p)
				m.Usable = true
			}
		}
		sort.Strings(m.Verified[k])
	}
	switch {
	case !m.Usable:
		m.Reason = "no conversion works"
		if len(missing) > 0 {
			m.Reason = "missing " + strings.Join(missing, ", ")
		}
	case len(missing) > 0:
		m.State, m.Reason = "degraded", "missing "+strings.Join(missing, ", ")
	case len(m.Problems) > 0:
		m.State, m.Reason = "degraded", "some conversions failed"
	default:
		m.State = "ready"
	}
	return m
}

// checkSource generates, probes and converts one matrix row.
func (s *Service) checkSource(ctx context.Context, tl *toolset, dir string, src matrixSource, m *Matrix) []Problem {
	sample := filepath.Join(dir, strings.ReplaceAll(src.key, "/", "_")+".sample")
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=2"}
	if strings.Contains(src.key, "video+") {
		args = append(args, "-f", "lavfi", "-i", "color=c=black:s=64x64:r=10:d=2")
	}
	args = append(append(args, src.gen...), "-y", sample)
	if out, err := s.runChecked(ctx, tl.ffmpeg, args...); err != nil {
		return []Problem{{Source: src.name, Reason: "cannot make a sample: " + firstLine(out, err)}}
	}
	facts, _ := s.runProbe(ctx, fileSource{sample})
	if facts == nil {
		return []Problem{{Source: src.name, Reason: "the probe could not read the sample"}}
	}
	if facts.sourceKey() != src.key {
		return []Problem{{Source: src.name, Reason: "the probe read the sample as " + facts.Demuxer + "/" + facts.Audio}}
	}
	m.ok[src.key] = map[string]bool{}
	var problems []Problem
	for i := range Profiles {
		p := &Profiles[i]
		if reason := s.checkConversion(ctx, tl, dir, facts, p, sample); reason != "" {
			problems = append(problems, Problem{Source: src.name, Profile: p.Name, Reason: reason})
			continue
		}
		m.ok[src.key][p.Name] = true
	}
	return problems
}

// checkConversion converts a sample from 0.5 s exactly as a stream would
// and decodes the result: about 1.5 s of audio must come back.
func (s *Service) checkConversion(ctx context.Context, tl *toolset, dir string, facts *Facts, p *Profile, sample string) string {
	sl, cctx, err := s.conv.acquire(ctx, s.ctx, "\x00check")
	if err != nil {
		return "the server was busy"
	}
	defer sl.release()
	g, in, revoke, err := s.reader.grant(cctx, fileSource{sample}, 0)
	if err != nil {
		return err.Error()
	}
	defer revoke()
	c, err := s.startConversion(cctx, in.env, conversionArgs(facts, p, 500, true, in.url))
	if err != nil {
		return "ffmpeg did not start"
	}
	out := filepath.Join(dir, "out-"+p.Name)
	fh, err := os.Create(out)
	if err != nil {
		c.kill()
		c.wait()
		return err.Error()
	}
	_, copyErr := io.Copy(fh, c.stdout)
	fh.Close()
	clean, outMS := c.wait()
	if copyErr != nil || !clean {
		return "the conversion failed: " + firstLine(c.stderr.String(), nil)
	}
	if _, sawEnd := g.outcome(); !completed(clean, 500, outMS, facts.DurationMS, facts.Estimated, sawEnd) {
		return fmt.Sprintf("the conversion ended at %d ms of %d", 500+outMS, facts.DurationMS)
	}
	// Decode the result back: mono 8 kHz 16-bit is 16000 bytes a second.
	cmd := confined(cctx, nil, tl.ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-f", p.demx, "-i", out,
		"-f", "s16le", "-ac", "1", "-ar", "8000", "pipe:1")
	pcm := &countingWriter{}
	stderr := &limitedBuffer{max: 4 << 10}
	cmd.Stdout, cmd.Stderr = pcm, stderr
	if err := cmd.Run(); err != nil {
		return "the result does not decode: " + firstLine(stderr.String(), err)
	}
	secs := float64(pcm.n) / 16000
	if secs < 1.25 || secs > 1.8 {
		return fmt.Sprintf("the result decodes to %.2f s, not 1.5 s", secs)
	}
	return ""
}

type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

func (s *Service) runChecked(ctx context.Context, path string, args ...string) (string, error) {
	sl, cctx, err := s.conv.acquire(ctx, s.ctx, "\x00check")
	if err != nil {
		return "", err
	}
	defer sl.release()
	cmd := confined(cctx, nil, path, args...)
	out := &limitedBuffer{max: 4 << 10}
	cmd.Stdout, cmd.Stderr = out, out
	err = cmd.Run()
	return out.String(), err
}

func firstLine(out string, err error) string {
	out = strings.TrimSpace(out)
	if i := strings.IndexByte(out, '\n'); i >= 0 {
		out = out[:i]
	}
	if out == "" && err != nil {
		out = err.Error()
	}
	if len(out) > 200 {
		out = out[:200]
	}
	return out
}

// locate finds ffmpeg (--ffmpeg, else PATH) and ffprobe (--ffprobe, else
// next to ffmpeg, else PATH) and reads ffmpeg's version.
func locate(ffmpeg, ffprobe string) (*toolset, string) {
	name := ffmpeg
	if name == "" {
		name = "ffmpeg"
	}
	fp, err := exec.LookPath(name)
	if err != nil {
		return nil, "ffmpeg is not installed (or not on PATH; see --ffmpeg)"
	}
	pp := ""
	switch {
	case ffprobe != "":
		pp, err = exec.LookPath(ffprobe)
	case ffmpeg != "":
		pp, err = exec.LookPath(filepath.Join(filepath.Dir(fp), "ffprobe"))
		if err != nil {
			pp, err = exec.LookPath("ffprobe")
		}
	default:
		pp, err = exec.LookPath("ffprobe")
	}
	if err != nil {
		return nil, "ffprobe is not installed (or not on PATH; see --ffprobe)"
	}
	fp, _ = filepath.Abs(fp)
	pp, _ = filepath.Abs(pp)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := confined(ctx, nil, fp, "-hide_banner", "-version").Output()
	if err != nil {
		return nil, "ffmpeg does not run"
	}
	version := ""
	if m := regexp.MustCompile(`ffmpeg version (\S+)`).FindStringSubmatch(string(out)); m != nil {
		version = m[1]
	}
	return &toolset{ffmpeg: fp, ffprobe: pp, version: version}, ""
}

// majorVersion reads "8.1.2", "n6.1" or "6.1.1-3ubuntu5"; 0 for a build
// without a release number (a git snapshot).
func majorVersion(v string) int {
	m := regexp.MustCompile(`^n?(\d+)\.`).FindStringSubmatch(v)
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// listNames reads ffmpeg's -encoders or -demuxers list.
func listNames(ctx context.Context, ffmpeg, flag string) map[string]bool {
	out, err := confined(ctx, nil, ffmpeg, "-hide_banner", flag).Output()
	names := map[string]bool{}
	if err != nil {
		return names
	}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	started := false
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "--" || strings.HasPrefix(strings.TrimSpace(line), "---") {
			started = true
			continue
		}
		f := strings.Fields(line)
		if !started || len(f) < 2 {
			continue
		}
		for _, n := range strings.Split(f[1], ",") {
			names[n] = true
		}
	}
	return names
}
