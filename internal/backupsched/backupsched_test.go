package backupsched

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	_ "time/tzdata" // explicit zones on any system

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// ---- a clock tests move by hand

type testClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.waiters = append(c.waiters, waiter{c.now.Add(d), ch})
	return ch
}

// waiting tells whether a timer is set that an advance of a minute fires
// (the loop is waiting for its next check). A timer set before the clock
// went back is far off and does not count.
func (c *testClock) waiting() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.waiters {
		if !w.at.After(c.now.Add(time.Minute)) {
			return true
		}
	}
	return false
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(c.now) {
			w.ch <- c.now
		} else {
			kept = append(kept, w)
		}
	}
	c.waiters = kept
}

// ---- a store whose backups are small files

type fakeStore struct {
	dir   string
	clock *testClock // its backups are taken at the scheduler's time
	mu    sync.Mutex
	fail  error
	block bool // wait for cancellation
	made  int
}

func (f *fakeStore) Dir() string { return f.dir }

// VerifyBackup refuses every file: none of the fake's is a backup.
func (f *fakeStore) VerifyBackup(context.Context, string, func(int64) error) error {
	return errors.New("not a backup")
}

func (f *fakeStore) BackupAs(ctx context.Context, dir string, name func(time.Time) (string, error), opts storage.BackupOptions) (storage.BackupInfo, error) {
	f.mu.Lock()
	fail, block := f.fail, f.block
	f.mu.Unlock()
	if opts.Start != nil {
		if err := opts.Start(); err != nil {
			return storage.BackupInfo{}, err
		}
	}
	if fail != nil {
		return storage.BackupInfo{}, fail
	}
	if block {
		<-ctx.Done()
		return storage.BackupInfo{}, ctx.Err()
	}
	taken := f.clock.Now()
	p, err := name(taken)
	if err != nil {
		return storage.BackupInfo{}, err
	}
	if err := os.WriteFile(p, []byte("backup"), 0o600); err != nil {
		return storage.BackupInfo{}, err
	}
	f.mu.Lock()
	f.made++
	f.mu.Unlock()
	return storage.BackupInfo{Path: p, Size: 6, TakenAt: taken.UnixMilli()}, nil
}

type env struct {
	t      *testing.T
	clock  *testClock
	store  *fakeStore
	sched  *Scheduler
	checks chan struct{}
}

// newEnv starts at 10:00 local on a day; backups are due at 03:30 ± 0
// unless cfg.Random draws jitter.
func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	data := t.TempDir()
	clock := &testClock{now: time.Date(2026, 10, 5, 10, 0, 0, 0, time.Local)}
	e := &env{t: t, clock: clock, store: &fakeStore{dir: data, clock: clock}, checks: make(chan struct{}, 1000)}
	cfg.Clock = e.clock
	if cfg.Random == nil {
		cfg.Random = func() float64 { return 0.5 } // no jitter
	}
	if cfg.FreeBytes == nil {
		cfg.FreeBytes = func(string) (int64, error) { return 100 << 30, nil }
	}
	e.sched = New(e.store, cfg)
	e.sched.afterCheck = func() { e.checks <- struct{}{} }
	return e
}

func (e *env) start() {
	e.t.Helper()
	if err := e.sched.Start(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(e.sched.Stop)
	<-e.checks
}

// minutes advances the clock minute by minute, letting the loop check each time.
func (e *env) minutes(n int) {
	for i := 0; i < n; i++ {
		e.settle()
		e.clock.Advance(time.Minute)
		<-e.checks
	}
}

// settle waits until the loop has set its timer, so an advance always fires it.
func (e *env) settle() {
	deadline := time.Now().Add(10 * time.Second)
	for !e.clock.waiting() {
		if time.Now().After(deadline) {
			e.t.Fatal("the scheduler never waited for its next check")
		}
		time.Sleep(time.Millisecond)
	}
}

func (e *env) daily() []published {
	list, err := e.sched.list()
	if err != nil {
		e.t.Fatal(err)
	}
	return list
}

func TestOneBackupADayAndAWeekAsleepMakesOne(t *testing.T) {
	e := newEnv(t, Config{})
	// Thirty backups already kept.
	dir := filepath.Join(e.store.dir, "backups", DailyDir)
	os.MkdirAll(dir, 0o700)
	for i := 1; i <= Keep; i++ {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("nottennapod-daily-%06d-20260901-033000.db", i)), []byte("old"), 0o600)
	}
	e.start()
	if e.store.made != 0 {
		t.Fatalf("a backup at start, before the quiet hour: %d", e.store.made)
	}
	// The next 03:30 comes: one backup, the oldest pruned.
	e.minutes(18 * 60)
	if e.store.made != 1 {
		t.Fatalf("%d backups on the first night", e.store.made)
	}
	list := e.daily()
	if len(list) != Keep || list[0].counter != 2 || list[len(list)-1].counter != Keep+1 {
		t.Fatalf("after the first night: %d kept, %d..%d", len(list), list[0].counter, list[len(list)-1].counter)
	}
	// The Mac sleeps a week: the wall clock jumps; on waking exactly one backup, one pruned.
	e.settle()
	e.clock.Advance(7 * 24 * time.Hour)
	<-e.checks
	e.minutes(10)
	if e.store.made != 2 {
		t.Fatalf("%d backups after a week asleep, want one more", e.store.made)
	}
	list = e.daily()
	if len(list) != Keep || list[0].counter != 3 {
		t.Fatalf("after the week: %d kept from %d", len(list), list[0].counter)
	}
	// Nothing more until the next quiet hour.
	e.minutes(60)
	if e.store.made != 2 {
		t.Errorf("a second backup the same day: %d", e.store.made)
	}
	st := e.sched.Status()
	if st.Kept != Keep || st.LastFile == "" || st.LastError != "" || st.WriteCheck != "ok" {
		t.Errorf("status %+v", st)
	}
}

func TestFailuresBackOffAndDoNotCount(t *testing.T) {
	e := newEnv(t, Config{})
	e.store.fail = errors.New("disk on fire")
	e.start()
	e.minutes(17*60 + 31) // 03:31: one failed attempt at 03:30
	st := e.sched.Status()
	if st.Failures != 1 || st.LastError == "" || st.Kept != 0 {
		t.Fatalf("after a failure %+v", st)
	}
	first := st.LastAttempt
	e.minutes(3) // 03:34
	if e.sched.Status().LastAttempt != first {
		t.Fatal("retried before the backoff")
	}
	e.minutes(1) // 03:35, five minutes after: the second attempt fails too
	if st := e.sched.Status(); st.Failures != 2 {
		t.Fatalf("second attempt %+v", st)
	}
	e.store.mu.Lock()
	e.store.fail = nil
	e.store.mu.Unlock()
	e.minutes(10) // 03:45, ten minutes after the second
	st = e.sched.Status()
	if st.Failures != 0 || st.Kept != 1 || st.LastError != "" {
		t.Fatalf("after recovering %+v", st)
	}
}

func TestADeadlineFromAClockThatRanAheadIsRecomputed(t *testing.T) {
	e := newEnv(t, Config{})
	ahead := State{NextDue: e.clock.Now().Add(30 * 24 * time.Hour).UnixMilli()}
	b := []byte(fmt.Sprintf(`{"next_due": %d}`, ahead.NextDue))
	os.WriteFile(filepath.Join(e.store.dir, StateFile), b, 0o600)
	e.start()
	if due := time.UnixMilli(e.sched.Status().NextDue); due.Sub(e.clock.Now()) > 24*time.Hour+jitter {
		t.Fatalf("a deadline a month ahead was kept: %v", due)
	}
}

func TestACrashAfterPublishingLeavesAtMostOneExtraFile(t *testing.T) {
	e := newEnv(t, Config{})
	dir := filepath.Join(e.store.dir, "backups", DailyDir)
	os.MkdirAll(dir, 0o700)
	for i := 1; i <= Keep; i++ {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("nottennapod-daily-%06d-20260901-033000.db", i)), []byte("old"), 0o600)
	}
	// Published just before a crash, never recorded in the schedule.
	os.WriteFile(filepath.Join(dir, fmt.Sprintf("nottennapod-daily-%06d-20260905-033000.db", Keep+1)), []byte("new"), 0o600)
	e.start()
	e.minutes(18 * 60)
	list := e.daily()
	if len(list) != Keep || list[len(list)-1].counter != Keep+2 {
		t.Fatalf("%d kept, newest %d", len(list), list[len(list)-1].counter)
	}
}

func TestDiskSafety(t *testing.T) {
	free := int64(512 << 20) // under the 1 GiB headroom
	var mu sync.Mutex
	e := newEnv(t, Config{FreeBytes: func(string) (int64, error) { mu.Lock(); defer mu.Unlock(); return free, nil }})
	e.start()
	e.minutes(17*60 + 31) // 03:31
	st := e.sched.Status()
	if e.store.made != 0 || st.LastError == "" || st.Failures != 1 {
		t.Fatalf("a backup without headroom: made %d, %+v", e.store.made, st)
	}
	// Space is fine before the copy but runs out during it: the copy is stopped.
	mu.Lock()
	free = 100 << 30
	mu.Unlock()
	e.store.mu.Lock()
	e.store.block = true
	e.store.mu.Unlock()
	go func() {
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		free = 100 << 20
		mu.Unlock()
	}()
	e.minutes(4) // 03:35: the retry starts, then runs out of space
	if st := e.sched.Status(); st.Failures != 2 || st.Kept != 0 {
		t.Fatalf("a copy that ran out of space: %+v", st)
	}
}

func TestAnUnwritableFolderIsReported(t *testing.T) {
	data := t.TempDir()
	locked := filepath.Join(data, "locked")
	os.Mkdir(locked, 0o500)
	t.Cleanup(func() { os.Chmod(locked, 0o700) })
	e := newEnv(t, Config{BackupDir: filepath.Join(locked, "backups")})
	e.start()
	if st := e.sched.Status(); st.WriteCheck == "ok" {
		t.Fatalf("an unwritable folder reported writable: %+v", st)
	}
}

// One real backup through the store: verified, private, in the daily folder.
func TestARealDailyBackupIsVerified(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clock := &testClock{now: time.Date(2026, 10, 5, 3, 29, 0, 0, time.Local)}
	s := New(store, Config{Clock: clock, Random: func() float64 { return 0.5 }})
	checks := make(chan struct{}, 100)
	s.afterCheck = func() { checks <- struct{}{} }
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	<-checks
	for !clock.waiting() {
		time.Sleep(time.Millisecond)
	}
	clock.Advance(time.Minute)
	<-checks
	st := s.Status()
	if st.Kept != 1 || st.LastError != "" {
		t.Fatalf("status %+v", st)
	}
	path := filepath.Join(store.Dir(), "backups", DailyDir, st.LastFile)
	if !strings.HasSuffix(path, ".db.gz") {
		t.Errorf("the daily backup is %s, not compressed", st.LastFile)
	}
	if err := store.VerifyBackup(ctx, path, nil); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("backup mode %v", fi.Mode().Perm())
	}
}

// ---- clocks, slots and failures

func TestAClockSetBackWhileRunningDoesNotSuppressBackups(t *testing.T) {
	e := newEnv(t, Config{})
	e.start()
	e.settle()
	e.clock.Advance(-30 * 24 * time.Hour) // the clock is corrected a month back
	e.sched.wake <- struct{}{}            // a timer set before the change would fire late; wake the loop
	<-e.checks
	if due := time.UnixMilli(e.sched.Status().NextDue).Sub(e.clock.Now()); due > 24*time.Hour+jitter {
		t.Fatalf("the next backup is %v away after the clock went back", due)
	}
	e.minutes(24 * 60)
	if e.store.made != 1 {
		t.Fatalf("%d backups in the day after the clock went back", e.store.made)
	}
}

func TestBackupsPublishedBeforeACrashAreRecordedAndPruned(t *testing.T) {
	verified := 0
	e := newEnv(t, Config{Verify: func(context.Context, string) error { verified++; return nil }})
	dir := filepath.Join(e.store.dir, "backups", DailyDir)
	os.MkdirAll(dir, 0o700)
	for i := 1; i <= Keep; i++ {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("nottennapod-daily-%06d-20260901-033000.db", i)), []byte("old"), 0o600)
	}
	// Five crashes, each just after publishing and before recording.
	for i := 0; i < 5; i++ {
		if err := e.sched.load(); err != nil {
			t.Fatal(err)
		}
		if _, err := e.sched.backup(context.Background()); err != nil {
			t.Fatal(err)
		}
		e.sched = New(e.store, e.sched.cfg)
	}
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	st := e.sched.Status()
	if st.Kept != Keep || st.LastSuccess == 0 || verified == 0 {
		t.Fatalf("after five unrecorded publications: kept %d, last success %d, verified %d", st.Kept, st.LastSuccess, verified)
	}
	// The recovered backup counts as today's: none is taken again at once.
	if time.UnixMilli(st.NextDue).Before(e.clock.Now()) {
		t.Errorf("a recovered backup left the next one due now")
	}
}

func TestAnAbandonedCopyIsRemovedBeforeTheSpaceCheck(t *testing.T) {
	var temp string
	e := newEnv(t, Config{FreeBytes: func(string) (int64, error) {
		if _, err := os.Stat(temp); err == nil {
			return 512 << 20, nil // the abandoned copy fills the disk
		}
		return 100 << 30, nil
	}})
	e.sched.load()
	os.MkdirAll(e.sched.dailyDir(), 0o700)
	temp = filepath.Join(e.sched.dailyDir(), ".nottennapod-backup-123.tmp")
	os.WriteFile(temp, []byte("abandoned"), 0o600)
	if _, err := e.sched.backup(context.Background()); err != nil {
		t.Fatalf("the backup after an abandoned copy: %v", err)
	}
	if _, err := os.Stat(temp); err == nil {
		t.Error("the abandoned copy remains")
	}
}

func TestUnknownOrLowSpaceOnEitherDiskRefusesTheBackup(t *testing.T) {
	unknown := newEnv(t, Config{FreeBytes: func(string) (int64, error) { return 0, errors.New("statfs failed") }})
	unknown.sched.load()
	if _, err := unknown.sched.backup(context.Background()); err == nil || unknown.store.made != 0 {
		t.Errorf("a backup with unknown free space: %v (made %d)", err, unknown.store.made)
	}
	var live string
	low := newEnv(t, Config{BackupDir: filepath.Join(t.TempDir(), "external"), FreeBytes: func(dir string) (int64, error) {
		if dir == live {
			return 0, nil
		}
		return 100 << 30, nil
	}})
	live = low.store.Dir()
	low.sched.load()
	if _, err := low.sched.backup(context.Background()); err == nil || low.store.made != 0 {
		t.Errorf("a backup with a full data disk: %v (made %d)", err, low.store.made)
	}
}

// ---- regression tests

// draws returns the values in turn, then the last one again.
func draws(values ...float64) func() float64 {
	var mu sync.Mutex
	i := 0
	return func() float64 {
		mu.Lock()
		defer mu.Unlock()
		v := values[min(i, len(values)-1)]
		i++
		return v
	}
}

// takenDays are the days (local) the daily backups were taken, from their names.
func (e *env) takenDays() []string {
	var days []string
	for _, p := range e.daily() {
		taken, err := time.ParseInLocation("20060102-150405", dailyName.FindStringSubmatch(p.name)[2], time.UTC)
		if err != nil {
			e.t.Fatal(err)
		}
		days = append(days, taken.In(e.clock.Now().Location()).Format("2006-01-02 15:04"))
	}
	return days
}

// A backup early in its window, followed by a later jitter draw,
// makes no second backup that day: the next is the next day's.
func TestJitterNeverMakesASecondBackupADay(t *testing.T) {
	e := newEnv(t, Config{Random: draws(0, 0.999, 0, 0.999, 0.5)})
	e.clock.now = time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	for range 4 * 24 * 60 {
		e.sched.check(context.Background())
		e.clock.Advance(time.Minute)
	}
	want := []string{"2026-10-06 03:15", "2026-10-07 03:45", "2026-10-08 03:15", "2026-10-09 03:45"}
	if got := e.takenDays(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("backups taken at %v, want %v", got, want)
	}
}

// A backup recovered after a crash counts as its day's; a restart
// days later still makes one catch-up, then one a day.
func TestARecoveredBackupIsItsDays(t *testing.T) {
	e := newEnv(t, Config{Random: draws(0, 0.999, 0.5), Verify: func(context.Context, string) error { return nil }})
	e.clock.now = time.Date(2026, 10, 6, 3, 15, 0, 0, time.UTC)
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.sched.backup(context.Background()); err != nil { // published, then a crash
		t.Fatal(err)
	}
	e.clock.Advance(5 * time.Minute)
	e.sched = New(e.store, e.sched.cfg)
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	if next := time.UnixMilli(e.sched.Status().NextDue).UTC(); next.Day() != 7 {
		t.Fatalf("after a recovered backup the next is %v, want October 7's", next)
	}
	// Down for three days: one catch-up at the restart, then the next day's.
	e.clock.now = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	e.sched = New(e.store, e.sched.cfg)
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	for range 24 * 60 {
		e.sched.check(context.Background())
		e.clock.Advance(time.Minute)
	}
	want := []string{"2026-10-06 03:15", "2026-10-09 12:00", "2026-10-10 03:30"}
	if got := e.takenDays(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("backups taken at %v, want %v", got, want)
	}
}

// The quiet hour is the local time on a daylight-saving change day
// too, in both directions, for a first schedule and the day after a backup.
func TestTheQuietHourOnDaylightSavingDays(t *testing.T) {
	for _, c := range []struct {
		zone string
		y    int
		m    time.Month
		d    int
	}{
		{"America/Los_Angeles", 2026, time.March, 8},    // 02:00 becomes 03:00
		{"America/Los_Angeles", 2026, time.November, 1}, // 02:00 becomes 01:00
		{"Europe/London", 2026, time.March, 29},         // 01:00 becomes 02:00
		{"Europe/London", 2026, time.October, 25},       // 02:00 becomes 01:00
	} {
		loc, err := time.LoadLocation(c.zone)
		if err != nil {
			t.Fatal(err)
		}
		e := newEnv(t, Config{})
		if next := e.sched.nextAfter(time.Date(c.y, c.m, c.d, 0, 0, 0, 0, loc)); next.Hour() != 3 || next.Minute() != 30 {
			t.Errorf("%s %d-%02d-%02d: a first schedule at %v", c.zone, c.y, c.m, c.d, next)
		}
		// Backups the day before, the day itself and the day after.
		e.clock.now = time.Date(c.y, c.m, c.d-1, 0, 0, 0, 0, loc)
		if err := e.sched.load(); err != nil {
			t.Fatal(err)
		}
		for range 3 * 24 * 60 {
			e.sched.check(context.Background())
			e.clock.Advance(time.Minute)
		}
		var want []string
		for n := -1; n <= 1; n++ {
			want = append(want, time.Date(c.y, c.m, c.d+n, 3, 30, 0, 0, loc).Format("2006-01-02 15:04"))
		}
		if got := e.takenDays(); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: backups taken at %v, want %v", c.zone, got, want)
		}
	}
}

// slowStore's backups take a while on the scheduler's clock.
type slowStore struct {
	*fakeStore
	takes time.Duration
}

func (s slowStore) BackupAs(ctx context.Context, dir string, name func(time.Time) (string, error), opts storage.BackupOptions) (storage.BackupInfo, error) {
	info, err := s.fakeStore.BackupAs(ctx, dir, name, opts)
	s.clock.Advance(s.takes)
	return info, err
}

// A backup belongs to the slot whose window had opened when it
// was taken, whether its success is recorded at once or recovered after a
// crash. A catch-up taken just before a window, finishing inside it, leaves
// that window's backup to do.
func TestABackupServesTheSlotOfItsStart(t *testing.T) {
	e := newEnv(t, Config{Verify: func(context.Context, string) error { return nil }})
	e.clock.now = time.Date(2026, 10, 6, 3, 14, 0, 0, time.UTC) // October 6's window opens at 03:15
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	e.sched.state.NextDue = e.clock.Now().Add(-24 * time.Hour).UnixMilli() // a missed day
	if err := e.sched.save(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(e.sched.statePath())
	if err != nil {
		t.Fatal(err)
	}
	e.sched.store = slowStore{e.store, 2 * time.Minute}
	e.sched.check(context.Background())
	recorded := time.UnixMilli(e.sched.Status().NextDue).UTC()
	// The same backup, published just before a crash that lost its record.
	if err := os.WriteFile(e.sched.statePath(), before, 0o600); err != nil {
		t.Fatal(err)
	}
	e.sched = New(e.store, e.sched.cfg)
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	recovered := time.UnixMilli(e.sched.Status().NextDue).UTC()
	want := time.Date(2026, 10, 6, 3, 30, 0, 0, time.UTC)
	if !recorded.Equal(want) || !recovered.Equal(want) {
		t.Fatalf("after a backup from 03:14 to 03:16 the next is %v (recorded) and %v (recovered), want %v", recorded, recovered, want)
	}
}

// --backup-time 00:00 is midnight:
// a set At of zero is not the default, and its window crosses midnight with
// one backup per slot. Config{} keeps the default 03:30.
func TestBackupsAtMidnight(t *testing.T) {
	if s := New(&fakeStore{dir: t.TempDir()}, Config{}); s.cfg.At != 3*time.Hour+30*time.Minute {
		t.Fatalf("the default quiet hour is %v", s.cfg.At)
	}
	e := newEnv(t, Config{At: 0, AtSet: true, Random: draws(0, 0.999, 0.5)})
	e.clock.now = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	if err := e.sched.load(); err != nil {
		t.Fatal(err)
	}
	for range 3 * 24 * 60 {
		e.sched.check(context.Background())
		e.clock.Advance(time.Minute)
	}
	// October 7's slot at 23:45 the evening before, October 8's at 00:15, October 9's at
	// midnight.
	want := []string{"2026-10-06 23:45", "2026-10-08 00:15", "2026-10-09 00:00"}
	if got := e.takenDays(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("backups taken at %v, want %v", got, want)
	}
}

// ---- compressed backups, the guarded runner and the usage lines

// Retention counts daily backups of both formats by their publication
// counter: older .db ones go first, and the 30 newest stay whatever their
// format.
func TestRetentionCountsBothFormats(t *testing.T) {
	e := newEnv(t, Config{})
	dir := filepath.Join(e.store.dir, "backups", DailyDir)
	os.MkdirAll(dir, 0o700)
	for i := 1; i <= 35; i++ {
		ext := ".db"
		if i > 20 {
			ext = ".db.gz"
		}
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("nottennapod-daily-%06d-20260901-033000%s", i, ext)), []byte("old"), 0o600)
	}
	e.start()
	list := e.daily()
	if len(list) != Keep || list[0].counter != 6 || list[len(list)-1].counter != 35 {
		t.Fatalf("kept %d, %d..%d", len(list), list[0].counter, list[len(list)-1].counter)
	}
	e.minutes(18 * 60) // the next quiet hour: one more, compressed
	list = e.daily()
	if len(list) != Keep || list[0].counter != 7 || list[len(list)-1].name != fmt.Sprintf("nottennapod-daily-%06d-%s.db.gz", 36,
		strings.TrimSuffix(strings.TrimPrefix(list[len(list)-1].name, "nottennapod-daily-000036-"), ".db.gz")) {
		t.Fatalf("after the next backup: %d kept, %d..%s", len(list), list[0].counter, list[len(list)-1].name)
	}
	if st := e.sched.Status(); st.Kept != Keep || st.Used != int64(3*29+6) {
		t.Fatalf("status %d kept, %d bytes", st.Kept, st.Used)
	}
}

// A compressed daily backup published just before a crash is verified with
// the server's keys and recorded at start; a tampered one is not recorded,
// and nothing of it is decoded.
func TestRecoverPublishedCompressed(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clock := &testClock{now: time.Date(2026, 10, 5, 10, 0, 0, 0, time.Local)}
	s := New(store, Config{Clock: clock, Random: func() float64 { return 0.5 }})
	if err := s.load(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.backup(ctx); err != nil { // published, then a crash before it was recorded
		t.Fatal(err)
	}
	first := s.daily()
	os.Remove(s.statePath())
	s = New(store, Config{Clock: clock, Random: func() float64 { return 0.5 }})
	if err := s.load(); err != nil {
		t.Fatal(err)
	}
	if st := s.Status(); st.LastFile != first[0].name || st.LastSuccess == 0 {
		t.Fatalf("a published compressed backup was not recorded: %+v", st.State)
	}
	// A newer one, changed after it was signed.
	clock.Advance(24 * time.Hour)
	if _, err := s.backup(ctx); err != nil {
		t.Fatal(err)
	}
	list := s.daily()
	newest := filepath.Join(s.dailyDir(), list[len(list)-1].name)
	data, _ := os.ReadFile(newest)
	data[len(data)/3] ^= 1
	os.WriteFile(newest, data, 0o600)
	os.Remove(s.statePath())
	decoded := storage.BackupsDecoded()
	s = New(store, Config{Clock: clock, Random: func() float64 { return 0.5 }})
	if err := s.load(); err != nil {
		t.Fatal(err)
	}
	if st := s.Status(); st.LastFile == list[len(list)-1].name || st.LastSuccess != 0 {
		t.Fatalf("a tampered backup was recorded: %+v", st.State)
	}
	if storage.BackupsDecoded() != decoded {
		t.Fatal("a tampered backup was decoded")
	}
	if left, _ := filepath.Glob(filepath.Join(s.dailyDir(), ".nottennapod-backup-*")); len(left) > 0 {
		t.Fatalf("the check left %v", left)
	}
}

func (s *Scheduler) daily() []published {
	list, _ := s.list()
	return list
}

// An on-demand backup (the admin page's and the command's) keeps the disks
// safe as the daily ones do: refused at the start without room, stopped by
// the watch when space runs out while it copies, by the guard when it runs
// out while it compresses, and by the cap when it takes too long; never
// published then. A copy left by a crash never stops the retry that removes
// it.
func TestManualBackupUsesHeadroomWatchAndCap(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	plenty := func(string) (int64, error) { return 100 << 30, nil }
	empty := func(t *testing.T, dir string) {
		t.Helper()
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".nottennapod-backup-") || e.Name() == ".nottennapod-backup-abandoned.tmp" {
				continue
			}
			t.Fatalf("left %s", e.Name())
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".db.gz") {
				t.Fatalf("published %s", e.Name())
			}
		}
	}
	t.Run("low at the start", func(t *testing.T) {
		dir := t.TempDir()
		_, err := Manual(ctx, store, dir, Guard{Free: func(string) (int64, error) { return 512 << 20, nil }})
		if !errors.Is(err, storage.ErrNoSpace) || !strings.Contains(err.Error(), "not enough free space for a backup") {
			t.Fatalf("got %v", err)
		}
		empty(t, dir)
	})
	t.Run("low while it compresses", func(t *testing.T) {
		dir := t.TempDir()
		// The raw copy and the compressed one both exist only while it compresses.
		_, err := Manual(ctx, store, dir, Guard{Free: func(d string) (int64, error) {
			if temps, _ := filepath.Glob(filepath.Join(dir, ".nottennapod-backup-*.tmp")); len(temps) >= 2 {
				return 1 << 30, nil
			}
			return 100 << 30, nil
		}})
		if !errors.Is(err, storage.ErrNoSpace) || !strings.Contains(err.Error(), "to compress the backup") {
			t.Fatalf("got %v", err)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Fatalf("left %v", entries)
		}
	})
	t.Run("low while it copies", func(t *testing.T) {
		dir := t.TempDir()
		var mu sync.Mutex
		free := int64(100 << 30)
		blocked := &fakeStore{dir: store.Dir(), clock: &testClock{now: time.Now()}, block: true}
		go func() {
			time.Sleep(200 * time.Millisecond)
			mu.Lock()
			free = 100 << 20
			mu.Unlock()
		}()
		start := time.Now()
		_, err := Manual(ctx, blocked, dir, Guard{Free: func(d string) (int64, error) {
			mu.Lock()
			defer mu.Unlock()
			if d != dir {
				return 100 << 30, nil
			}
			return free, nil
		}})
		if !errors.Is(err, storage.ErrNoSpace) || !strings.Contains(err.Error(), "free space fell") || time.Since(start) > 5*time.Second {
			t.Fatalf("got %v after %v", err, time.Since(start))
		}
	})
	t.Run("the cap", func(t *testing.T) {
		blocked := &fakeStore{dir: store.Dir(), clock: &testClock{now: time.Now()}, block: true}
		_, err := Manual(ctx, blocked, t.TempDir(), Guard{Free: plenty, MaxRun: 100 * time.Millisecond})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "took longer than") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a crash's copy never stops the retry", func(t *testing.T) {
		dir := t.TempDir()
		abandoned := filepath.Join(dir, ".nottennapod-backup-abandoned.tmp")
		os.WriteFile(abandoned, []byte("a copy left by a crash"), 0o600)
		info, err := Manual(ctx, store, dir, Guard{Free: func(string) (int64, error) {
			if _, err := os.Stat(abandoned); err == nil {
				return 512 << 20, nil // the abandoned copy fills the disk
			}
			return 100 << 30, nil
		}})
		if err != nil {
			t.Fatalf("the retry after a crash: %v", err)
		}
		if _, err := os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("the abandoned copy remains")
		}
		if !strings.HasSuffix(info.Path, ".db.gz") || store.VerifyBackup(ctx, info.Path, nil) != nil {
			t.Fatalf("the backup %+v", info)
		}
	})
}

// The usage lines list daily, on-demand and pre-restore copies and their
// bytes, with nothing else counted, whether daily backups are on or off.
func TestUsageListsDailyManualPreRestore(t *testing.T) {
	backups, data := t.TempDir(), t.TempDir()
	os.MkdirAll(filepath.Join(backups, DailyDir), 0o700)
	write := func(path string, n int) {
		if err := os.WriteFile(path, make([]byte, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(backups, DailyDir, "nottennapod-daily-000001-20260901-033000.db"), 10)
	write(filepath.Join(backups, DailyDir, "nottennapod-daily-000002-20260902-033000.db.gz"), 20)
	write(filepath.Join(backups, DailyDir, ".nottennapod-backup-1.tmp"), 1000)
	write(filepath.Join(backups, "nottennapod-20261001-120000-1.db"), 100)
	write(filepath.Join(backups, "nottennapod-20261002-120000-1.db.gz"), 200)
	write(filepath.Join(backups, "nottennapod-20261002-120000-2.db.gz"), 300)
	write(filepath.Join(backups, ".nottennapod-backup-2.tmp"), 1000)
	write(filepath.Join(backups, "notes.txt"), 1000)
	write(filepath.Join(data, "nottennapod.db"), 1000)
	write(filepath.Join(data, "nottennapod.db-wal"), 1000)
	write(filepath.Join(data, "nottennapod.db.pre-restore-20261003-080000"), 4000)
	write(filepath.Join(data, "nottennapod.db.pre-restore-20261003-080000-wal"), 50)
	write(filepath.Join(data, "nottennapod.db.pre-restore-20261003-080000-shm"), 5)
	write(filepath.Join(data, "nottennapod.db.pre-restore-20261004-080000-2"), 3000)
	write(filepath.Join(data, "nottennapod.db.restoring"), 1000)
	got := GetUsage(backups, data)
	want := Usage{Daily: Use{2, 30}, Manual: Use{3, 600}, PreRestore: Use{2, 7055}}
	if got != want {
		t.Fatalf("usage %+v, want %+v", got, want)
	}
	if got := GetUsage(filepath.Join(t.TempDir(), "none"), t.TempDir()); got != (Usage{}) {
		t.Fatalf("empty folders: %+v", got)
	}
}
