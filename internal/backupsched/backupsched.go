// Package backupsched takes the server's daily backups (PROTOCOL §11.2, §14): one
// verified copy a day at a quiet hour, missed days coalesced into one catch-up after a wake or
// start, the last 30 kept by a durable publication counter, failures backed
// off and never counted, and the disk never filled.
package backupsched

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	randv2 "math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

const (
	// Keep is how many daily backups are kept (PROTOCOL §11.2).
	Keep = 30
	// DailyDir is the daily backups' folder inside the backup directory;
	// on-demand backups stay in the backup directory itself, never pruned.
	DailyDir = "daily"
	// StateFile is the persisted schedule, in the data directory.
	StateFile   = "backup-schedule.json"
	jitter      = 15 * time.Minute
	maxRun      = 30 * time.Minute
	minBack     = 5 * time.Minute
	maxBack     = 6 * time.Hour
	minHeadroom = 1 << 30 // 1 GiB
)

// Backuper makes one verified backup with a chosen name (storage.Store).
type Backuper interface {
	BackupAs(ctx context.Context, dir string, name func(taken time.Time) (string, error)) (storage.BackupInfo, error)
	Dir() string
}

// Clock is the scheduler's time source; tests move it by hand.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config configures a Scheduler; zero fields take the defaults.
type Config struct {
	BackupDir string        // default <data-dir>/backups
	At        time.Duration // time of day, local (default 03:30 unless AtSet)
	AtSet     bool          // At is set: zero is midnight, not the default
	Clock     Clock
	Random    func() float64
	// FreeBytes reports the free space of the file system holding dir (default statfs).
	FreeBytes func(dir string) (int64, error)
	// Verify checks a published backup found at start that the schedule had not recorded (default storage.VerifyDatabaseFile).
	Verify func(ctx context.Context, path string) error
	Logger *slog.Logger
}

// State is the persisted schedule.
type State struct {
	NextDue      int64  `json:"next_due"`      // ms: the next backup is taken at or after this time
	LastSuccess  int64  `json:"last_success"`  // ms
	LastFile     string `json:"last_file"`     // base name
	LastSize     int64  `json:"last_size"`     // bytes
	LastAttempt  int64  `json:"last_attempt"`  // ms
	LastError    string `json:"last_error"`    // the last failure, cleared by a success
	Failures     int    `json:"failures"`      // consecutive
	RetryAt      int64  `json:"retry_at"`      // ms: not before this after a failure
	WriteCheck   string `json:"write_check"`   // "ok" or why the folder cannot be written
	WriteChecked int64  `json:"write_checked"` // ms
}

// Status is what the service page shows.
type Status struct {
	State
	Dir     string
	Kept    int
	Used    int64
	Running bool
}

// Scheduler runs the daily backups.
type Scheduler struct {
	store Backuper
	cfg   Config
	log   *slog.Logger

	mu      sync.Mutex
	state   State
	running bool
	wake    chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
	// afterCheck is called after each check of the schedule (tests).
	afterCheck func()
}

// New returns a scheduler for store; Start runs it.
func New(store Backuper, cfg Config) *Scheduler {
	if cfg.BackupDir == "" {
		cfg.BackupDir = filepath.Join(store.Dir(), "backups")
	}
	if cfg.At == 0 && !cfg.AtSet {
		cfg.At = 3*time.Hour + 30*time.Minute
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.Random == nil {
		cfg.Random = randv2.Float64
	}
	if cfg.FreeBytes == nil {
		cfg.FreeBytes = freeBytes
	}
	if cfg.Verify == nil {
		cfg.Verify = func(ctx context.Context, path string) error { return storage.VerifyDatabaseFile(ctx, path, -1) }
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{store: store, cfg: cfg, log: log, wake: make(chan struct{}, 1)}
}

func (s *Scheduler) dailyDir() string { return filepath.Join(s.cfg.BackupDir, DailyDir) }

// makeDirs makes the backup folder and its daily folder: both must belong to
// the server's user, and are made 0700.
func (s *Scheduler) makeDirs() error {
	for _, d := range []string{s.cfg.BackupDir, s.dailyDir()} {
		if err := private.Dir(d); err != nil {
			return fmt.Errorf("backups: %w", err)
		}
	}
	// Backups made while an inheritable ACL entry was there keep none, the
	// administrators' ones beside the daily folder included.
	if err := private.Tree(s.cfg.BackupDir); err != nil {
		return fmt.Errorf("backups: %w", err)
	}
	return nil
}
func (s *Scheduler) statePath() string { return filepath.Join(s.store.Dir(), StateFile) }

// Start loads the schedule, checks the backup folder can be written, and
// checks what is due every minute until ctx ends (a wall-clock check, so a
// backup missed while the Mac slept runs within minutes of waking).
func (s *Scheduler) Start(ctx context.Context) error {
	if err := s.load(); err != nil {
		return err
	}
	s.checkWritable()
	ctx, s.cancel = context.WithCancel(ctx)
	s.done = make(chan struct{})
	go s.loop(ctx)
	return nil
}

// Stop stops the scheduler, cancelling a running backup.
func (s *Scheduler) Stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	<-s.done
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	for {
		s.check(ctx)
		if s.afterCheck != nil {
			s.afterCheck()
		}
		select {
		case <-ctx.Done():
			return
		case <-s.cfg.Clock.After(time.Minute):
		case <-s.wake:
		}
	}
}

// load reads the persisted schedule; a first start schedules the next quiet hour.
func (s *Scheduler) load() error {
	b, err := os.ReadFile(s.statePath())
	switch {
	case errors.Is(err, os.ErrNotExist):
		s.state = State{}
	case err != nil:
		return err
	default:
		if err := json.Unmarshal(b, &s.state); err != nil {
			s.log.Warn("backups: the schedule file is unreadable; starting a new one", "error", err)
			s.state = State{}
		}
	}
	now := s.cfg.Clock.Now()
	s.reconcile(now)
	s.recoverPublished(now)
	if err := s.prune(); err != nil {
		s.log.Warn("backups: pruning old backups failed", "error", err)
	}
	return s.save()
}

// reconcile brings back deadlines a clock change left far ahead: a deadline
// beyond the next day's quiet hour and its jitter was written while the clock
// ran ahead (or the clock was set back since), so it is recomputed. A clock
// change therefore never suppresses a backup (PROTOCOL §11.2). It runs at start
// and at every check. It reports whether it changed anything.
func (s *Scheduler) reconcile(now time.Time) bool {
	changed := false
	if limit := s.at(dayOf(s.served(now), 1)).Add(jitter); s.state.NextDue == 0 || s.state.NextDue > limit.UnixMilli() {
		s.state.NextDue = s.nextAfter(now).UnixMilli()
		changed = true
	}
	if s.state.RetryAt > now.Add(maxBack).UnixMilli() {
		s.state.RetryAt = now.UnixMilli()
		changed = true
	}
	return changed
}

// recoverPublished records a backup published just before a crash, before
// its success was recorded (PROTOCOL §11.2: "found and recorded at start"): the newest
// daily backup, when it is newer than the recorded success, is verified and
// recorded. One that does not verify is left alone (never deleted here) and
// not recorded; retention removes it in time.
func (s *Scheduler) recoverPublished(now time.Time) {
	list, err := s.list()
	if err != nil || len(list) == 0 {
		return
	}
	newest := list[len(list)-1]
	m := dailyName.FindStringSubmatch(newest.name)
	taken, err := time.ParseInLocation("20060102-150405", m[2], time.UTC)
	if err != nil || taken.UnixMilli() <= s.state.LastSuccess {
		return
	}
	path := filepath.Join(s.dailyDir(), newest.name)
	if err := s.cfg.Verify(context.Background(), path); err != nil {
		s.log.Warn("backups: an unrecorded backup does not verify; it is not counted as a success", "file", newest.name, "error", err)
		return
	}
	s.state.LastSuccess, s.state.LastFile, s.state.LastSize = taken.UnixMilli(), newest.name, newest.size
	s.state.NextDue = s.nextDay(taken.In(now.Location())).UnixMilli()
	s.state.Failures, s.state.RetryAt, s.state.LastError = 0, 0, ""
	s.log.Info("backups: recorded a backup published before the server stopped", "file", newest.name)
}

// Each day has one backup, at its quiet hour (At, local) with jitter: the
// day's slot. A slot's window, At ± jitter, can cross midnight (--backup-time
// near it), so a backup belongs to the slot it served, not to its date.

// dayOf is the day n days after t's (local), at noon.
func dayOf(t time.Time, n int) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d+n, 12, 0, 0, 0, t.Location())
}

// at is day's quiet hour, before jitter: the local wall-clock time, also on
// a daylight-saving change day, whose hours from midnight differ (server bug
// hunt 2026-10, SB-5).
func (s *Scheduler) at(day time.Time) time.Time {
	y, m, d := day.Date()
	a := s.cfg.At
	return time.Date(y, m, d, int(a/time.Hour), int(a%time.Hour/time.Minute), int(a%time.Minute/time.Second), int(a%time.Second), day.Location())
}

func (s *Scheduler) jitter() time.Duration {
	return time.Duration((s.cfg.Random()*2 - 1) * float64(jitter))
}

// served is the day whose backup one taken at t is: the latest day whose
// window had opened by t. A catch-up serves the latest missed day, so missed
// days coalesce into it.
func (s *Scheduler) served(t time.Time) time.Time {
	for n := 1; ; n-- {
		if day := dayOf(t, n); !s.at(day).Add(-jitter).After(t) {
			return day
		}
	}
}

// nextAfter is the first scheduled time after t, for a new schedule or one
// a clock change left far ahead.
func (s *Scheduler) nextAfter(t time.Time) time.Time {
	j := s.jitter()
	for n := -1; ; n++ {
		if next := s.at(dayOf(t, n)).Add(j); next.After(t) {
			return next
		}
	}
}

// nextDay is the scheduled time after a backup taken at t: the next day's
// slot, with its own jitter. Its window opens after t, so a day never has a
// second backup however the jitter falls.
func (s *Scheduler) nextDay(t time.Time) time.Time {
	return s.at(dayOf(s.served(t), 1)).Add(s.jitter())
}

func (s *Scheduler) save() error {
	b, _ := json.MarshalIndent(s.state, "", "  ")
	tmp := s.statePath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.statePath())
}

// checkWritable writes and removes a test file in the backup folder, as the
// service will, so the service page can say whether backups can be written
// there (a protected folder needs the owner's permission, PROTOCOL §11.2).
func (s *Scheduler) checkWritable() {
	dir := s.dailyDir()
	result := "ok"
	if err := s.makeDirs(); err != nil {
		result = err.Error()
	} else if f, err := os.CreateTemp(dir, ".nottennapod-write-check-*"); err != nil {
		// A new, randomly named file: nothing planted in the folder is followed.
		result = err.Error()
	} else {
		_, err := f.Write([]byte("ok\n"))
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			result = err.Error()
		}
		os.Remove(f.Name())
	}
	s.mu.Lock()
	s.state.WriteCheck, s.state.WriteChecked = result, s.cfg.Clock.Now().UnixMilli()
	_ = s.save()
	s.mu.Unlock()
	if result != "ok" {
		s.log.Warn("backups: the backup folder cannot be written", "dir", dir, "error", result)
	}
}

// check takes the day's backup when it is due (once, however many days were missed).
func (s *Scheduler) check(ctx context.Context) {
	now := s.cfg.Clock.Now()
	s.mu.Lock()
	if s.reconcile(now) {
		_ = s.save()
	}
	due := now.UnixMilli() >= s.state.NextDue && now.UnixMilli() >= s.state.RetryAt && !s.running
	if due {
		s.running = true
		s.state.LastAttempt = now.UnixMilli()
	}
	s.mu.Unlock()
	if !due {
		return
	}
	info, err := s.backup(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	end := s.cfg.Clock.Now()
	if err != nil {
		if ctx.Err() != nil {
			return // stopping: the backup is retried at the next start
		}
		s.state.Failures++
		back := minBack << min(s.state.Failures-1, 10)
		s.state.RetryAt = end.Add(min(back, maxBack)).UnixMilli()
		s.state.LastError = err.Error()
		s.log.Error("backups: the daily backup failed; older backups are kept", "error", err, "retry_in", min(back, maxBack))
		_ = s.save()
		return
	}
	s.state.Failures, s.state.RetryAt, s.state.LastError = 0, 0, ""
	s.state.LastSuccess, s.state.LastFile, s.state.LastSize = info.TakenAt, filepath.Base(info.Path), info.Size
	// The next is the following day's: missed days coalesce into this one. A
	// backup serves the slot open when it was taken, as when it is recovered
	// after a crash.
	s.state.NextDue = s.nextDay(time.UnixMilli(info.TakenAt).In(end.Location())).UnixMilli()
	if err := s.save(); err != nil {
		s.log.Error("backups: cannot record the backup", "error", err)
	}
	s.log.Info("backups: daily backup published", "file", info.Path, "bytes", info.Size)
	if err := s.prune(); err != nil {
		s.log.Warn("backups: pruning old backups failed", "error", err)
	}
}

var dailyName = regexp.MustCompile(`^nottennapod-daily-(\d{6,})-(\d{8}-\d{6})\.db$`)

type published struct {
	name    string
	counter int64
	size    int64
}

// list returns the published daily backups in publication order.
func (s *Scheduler) list() ([]published, error) {
	entries, err := os.ReadDir(s.dailyDir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []published
	for _, e := range entries {
		m := dailyName.FindStringSubmatch(e.Name())
		if m == nil || !e.Type().IsRegular() {
			continue
		}
		n, _ := strconv.ParseInt(m[1], 10, 64)
		var size int64
		if fi, err := e.Info(); err == nil {
			size = fi.Size()
		}
		out = append(out, published{name: e.Name(), counter: n, size: size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].counter < out[j].counter })
	return out, nil
}

// backup publishes the next daily backup: its name carries the next
// publication counter, so retention never depends on names' dates, mtimes or
// the clock; a crash after publishing leaves at worst one extra file (PROTOCOL §11.2).
func (s *Scheduler) backup(ctx context.Context) (storage.BackupInfo, error) {
	dir := s.dailyDir()
	if err := s.makeDirs(); err != nil {
		return storage.BackupInfo{}, err
	}
	list, err := s.list()
	if err != nil {
		return storage.BackupInfo{}, err
	}
	next := int64(1)
	if len(list) > 0 {
		next = list[len(list)-1].counter + 1
	}
	// A copy abandoned by a crash is removed first (only this scheduler writes here).
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".nottennapod-backup-") {
				os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
	need, err := s.headroom()
	if err != nil {
		return storage.BackupInfo{}, err
	}
	// Free space is checked on the backup's disk and on the live database's,
	// whose WAL grows while the copy holds its snapshot (PROTOCOL §11.2). Unknown
	// space refuses the backup rather than risk either disk.
	space := func() error {
		dest, err := s.cfg.FreeBytes(dir)
		if err != nil {
			return fmt.Errorf("cannot tell the free space for backups: %w", err)
		}
		live, err := s.cfg.FreeBytes(s.store.Dir())
		if err != nil {
			return fmt.Errorf("cannot tell the free space of the data folder: %w", err)
		}
		if live < need.headroom {
			return fmt.Errorf("not enough free space beside the database: %d MB free, %d MB kept free", live>>20, need.headroom>>20)
		}
		if dest < need.headroom {
			return fmt.Errorf("the backup was stopped: free space fell to %d MB", dest>>20)
		}
		return nil
	}
	if free, err := s.cfg.FreeBytes(dir); err != nil {
		return storage.BackupInfo{}, fmt.Errorf("cannot tell the free space for backups: %w", err)
	} else if free < need.dbSize+need.headroom {
		return storage.BackupInfo{}, fmt.Errorf("not enough free space for a backup: %d MB free, %d MB needed", free>>20, (need.dbSize+need.headroom)>>20)
	}
	if err := space(); err != nil {
		return storage.BackupInfo{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, maxRun)
	defer cancel()
	// Watch both disks while copying; a copy that would cross the headroom
	// is cancelled before it does.
	var low error
	watch := make(chan struct{})
	go func() {
		defer close(watch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			if err := space(); err != nil {
				low = err
				cancel()
				return
			}
		}
	}()
	info, err := s.store.BackupAs(ctx, dir, func(taken time.Time) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("nottennapod-daily-%06d-%s.db", next, taken.UTC().Format("20060102-150405"))), nil
	})
	cancel()
	<-watch
	if low != nil {
		return info, low
	}
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		return info, fmt.Errorf("the backup took longer than %v and was stopped", maxRun)
	}
	return info, err
}

type need struct{ dbSize, headroom int64 }

// headroom is the larger of 1 GiB and twice the live database with its WAL.
func (s *Scheduler) headroom() (need, error) {
	size := databaseSize(s.store.Dir())
	return need{dbSize: size, headroom: Headroom(s.store.Dir())}, nil
}

// Headroom is the free space kept beside the database in dataDir (PROTOCOL §11.2):
// the larger of 1 GiB and twice the database with its WAL. Other writers to
// the data directory (imports) keep it too.
func Headroom(dataDir string) int64 { return max(minHeadroom, 2*databaseSize(dataDir)) }

// FreeBytes is the space available to this user on dir's file system.
func FreeBytes(dir string) (int64, error) { return freeBytes(dir) }

func databaseSize(dataDir string) int64 {
	var size int64
	for _, f := range []string{"nottennapod.db", "nottennapod.db-wal"} {
		if fi, err := os.Stat(filepath.Join(dataDir, f)); err == nil {
			size += fi.Size()
		}
	}
	return size
}

// prune deletes the oldest daily backups beyond Keep, by publication counter.
func (s *Scheduler) prune() error {
	list, err := s.list()
	if err != nil {
		return err
	}
	for len(list) > Keep {
		if err := os.Remove(filepath.Join(s.dailyDir(), list[0].name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		list = list[1:]
	}
	return nil
}

// Status reports the schedule for the service page.
func (s *Scheduler) Status() Status {
	s.mu.Lock()
	st := Status{State: s.state, Dir: s.cfg.BackupDir, Running: s.running}
	s.mu.Unlock()
	list, _ := s.list()
	st.Kept = len(list)
	for _, p := range list {
		st.Used += p.size
	}
	return st
}
