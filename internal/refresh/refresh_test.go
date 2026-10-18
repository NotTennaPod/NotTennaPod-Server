package refresh

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// 1. A subscription's first fetch populates the catalog and files nothing; a
// release in a later scheduled fetch is admitted and new.
func TestFirstPopulationThenRelease(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	t0 := e.clock.ms()
	fs.set(item{"One", t0 - 3*day, true}, item{"Two", t0 - 2*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	if got := paths(fs.seen()); got != "/feed" {
		t.Fatalf("first fetch requests %q", got)
	}
	if got := e.podcastRecord(p, "metadata"); !strings.Contains(got, `"title":"Show"`) {
		t.Errorf("podcast metadata %s", got)
	}
	if got := e.episodeRecord(p, "One", "metadata"); !strings.Contains(got, `"title":"One"`) {
		t.Errorf("episode metadata %s", got)
	}
	for _, title := range []string{"One", "Two"} {
		if got := e.episodeRecord(p, title, "admission"); got != "" {
			t.Errorf("the first population filed %s: %s", title, got)
		}
	}
	st := decode(t, e.podcastRecord(p, "refresh_status"))
	if num(st, "last_attempt") != t0 || num(st, "last_success") != t0 || st["error"] != nil || num(st, "next_attempt") != t0+hour {
		t.Errorf("refresh status %v", st)
	}
	// Nothing is due before the hour.
	e.clock.Advance(59 * time.Minute)
	runDue(t, r)
	if got := fs.seen(); len(got) != 0 {
		t.Fatalf("fetched early: %v", got)
	}
	e.clock.Advance(time.Minute)
	release := e.clock.ms() - minute
	fs.set(item{"One", t0 - 3*day, true}, item{"Two", t0 - 2*day, true}, item{"Three", release, true})
	runDue(t, r)
	if got := paths(fs.seen()); got != "/feed" {
		t.Fatalf("scheduled fetch requests %q", got)
	}
	if got := e.episodeRecord(p, "Three", "admission"); !strings.Contains(got, `"source":"server"`) || !strings.Contains(got, `"action":"inbox"`) {
		t.Errorf("the release was not admitted: %s", got)
	}
	if got := e.episodeRecord(p, "Three", "play_state"); !strings.Contains(got, `"play_state":"new"`) {
		t.Errorf("play state %s", got)
	}
	if got := e.episodeRecord(p, "Two", "admission"); got != "" {
		t.Errorf("an old episode was filed: %s", got)
	}
}

// 2. Scheduled fetches are conditional; a fetch after a failure, and after a
// change of credentials or URL, sends no validators.
func TestConditionalFetches(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	p, g := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	expect := func(what string, conditional bool) {
		t.Helper()
		rs := fs.seen()
		if len(rs) != 1 || (rs[0].ifNoneMatch != "") != conditional {
			t.Fatalf("%s: requests %+v, want one, conditional %v", what, rs, conditional)
		}
	}
	runDue(t, r)
	expect("first population", false)
	e.clock.Advance(time.Hour)
	head := e.head()
	runDue(t, r)
	expect("scheduled", true)
	if e.head() != head {
		t.Errorf("a 304 whose status was not due wrote to the log")
	}
	// A failure; the next fetch is unconditional, the one after it conditional.
	fs.with(func(fs *feedServer) { fs.status = 500 })
	e.clock.Advance(time.Hour)
	runDue(t, r)
	expect("failing", true)
	fs.with(func(fs *feedServer) { fs.status = 0 })
	e.clock.Advance(15 * time.Minute)
	runDue(t, r)
	expect("after a failure", false)
	e.clock.Advance(time.Hour)
	runDue(t, r)
	expect("after a success", true)
	// New credentials: fetched now, unconditionally.
	secret := protocol.Secret{ID: newID(), Username: "u", Password: "p"}
	e.push(e.fact("podcast.set_credentials", podTarget(p, g), map[string]any{"secret_id": secret.ID}), secret)
	runDue(t, r)
	expect("new credentials", false)
	e.clock.Advance(time.Hour)
	runDue(t, r)
	expect("with the credentials", true)
	// A new URL: fetched now, unconditionally.
	e.push(e.fact("podcast.change_url", podTarget(p, g), map[string]any{"url": fs.url("/feed?edition=2")}))
	runDue(t, r)
	expect("new URL", false)
	if row := e.row(p); row.Failures != 0 || row.LastError != "" {
		t.Errorf("row %+v", row)
	}
}

// 3. A failing feed backs off, doubling; its error is published once, not on
// every retry, and cleared by the next success.
func TestFailureBackoffAndStatus(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fs.seen()
	fs.with(func(fs *feedServer) { fs.status = 500 })
	e.clock.Advance(time.Hour)
	head := e.head()
	runDue(t, r)
	if e.head() != head+1 {
		t.Fatalf("the first failure did not publish its status")
	}
	failedAt := e.clock.ms()
	st := decode(t, e.podcastRecord(p, "refresh_status"))
	errObj, _ := st["error"].(map[string]any)
	if errObj["label"] != "http_status" || num(errObj, "code") != 500 || num(st, "next_attempt") != failedAt+15*minute ||
		num(st, "last_success") != failedAt-hour {
		t.Fatalf("status after a failure %v", st)
	}
	published := e.podcastRecord(p, "refresh_status")
	fs.seen()
	for i, wait := range []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour} {
		if row := e.row(p); row.Failures != int64(i+1) || row.NextAt != e.clock.ms()+wait.Milliseconds() {
			t.Fatalf("after failure %d: row %+v, want next in %v", i+1, row, wait)
		}
		e.clock.Advance(wait - time.Second)
		runDue(t, r)
		if got := fs.seen(); len(got) != 0 {
			t.Fatalf("retry %d came early", i+1)
		}
		e.clock.Advance(time.Second)
		head = e.head()
		runDue(t, r)
		if got := fs.seen(); len(got) != 1 {
			t.Fatalf("retry %d: %d requests", i+1, len(got))
		}
		if e.head() != head || e.podcastRecord(p, "refresh_status") != published {
			t.Fatalf("retry %d republished an unchanged error", i+1)
		}
	}
	fs.with(func(fs *feedServer) { fs.status = 0 })
	e.clock.Advance(4 * time.Hour)
	runDue(t, r)
	st = decode(t, e.podcastRecord(p, "refresh_status"))
	if st["error"] != nil || num(st, "last_success") != e.clock.ms() || num(st, "next_attempt") != e.clock.ms()+hour {
		t.Errorf("status after recovering %v", st)
	}
	if row := e.row(p); row.Failures != 0 || row.LastError != "" {
		t.Errorf("row after recovering %+v", row)
	}
}

// Backoff doubles from 15 minutes to 24 hours; jitter stays within ±10 %.
func TestBackoffAndJitter(t *testing.T) {
	e := newEnv(t)
	r := e.refresher(nil, Config{})
	want := []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour, 8 * time.Hour,
		16 * time.Hour, 24 * time.Hour, 24 * time.Hour, 24 * time.Hour}
	for i, w := range want {
		if got := r.backoff(int64(i + 1)); got != w {
			t.Errorf("backoff %d: %v, want %v", i+1, got, w)
		}
	}
	if got := r.backoff(1000); got != 24*time.Hour {
		t.Errorf("backoff 1000: %v", got)
	}
	low, high := e.refresher(nil, Config{Random: fixedRandom(0)}), e.refresher(nil, Config{Random: fixedRandom(0.999999)})
	if got := low.jitter(time.Hour); got != 54*time.Minute {
		t.Errorf("low jitter %v", got)
	}
	if got := high.jitter(time.Hour); got < 65*time.Minute || got > 66*time.Minute {
		t.Errorf("high jitter %v", got)
	}
	if got := high.backoff(9); got != 24*time.Hour {
		t.Errorf("jitter took the backoff past its cap: %v", got)
	}
}

// 4. Retry-After is a lower bound on the next attempt, capped at 30 days.
func TestRetryAfter(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fs.seen()
	fs.with(func(fs *feedServer) { fs.status, fs.retryAfter = 503, "7200" })
	e.clock.Advance(time.Hour)
	runDue(t, r)
	if row := e.row(p); row.NotBefore != e.clock.ms()+2*hour || row.NextAt != row.NotBefore {
		t.Fatalf("row after Retry-After %+v", row)
	}
	fs.seen()
	e.clock.Advance(2*time.Hour - time.Second)
	runDue(t, r)
	if got := fs.seen(); len(got) != 0 {
		t.Fatalf("fetched before Retry-After: %v", got)
	}
	// A Retry-After shorter than the backoff does not bring it forward.
	fs.with(func(fs *feedServer) { fs.retryAfter = "60" })
	e.clock.Advance(time.Second)
	runDue(t, r)
	if got := fs.seen(); len(got) != 1 {
		t.Fatalf("not fetched after Retry-After: %v", got)
	}
	if row := e.row(p); row.NextAt != e.clock.ms()+30*minute || row.NotBefore != e.clock.ms()+minute {
		t.Errorf("row after a short Retry-After %+v", row)
	}
	// Sixty days are capped at 30.
	fs.with(func(fs *feedServer) { fs.retryAfter = "5184000" })
	e.clock.Advance(30 * time.Minute)
	runDue(t, r)
	if row := e.row(p); row.NextAt != e.clock.ms()+30*day || row.NotBefore != row.NextAt {
		t.Errorf("row after a 60-day Retry-After %+v", row)
	}
}

// 6b. Sleep and clock changes (PROTOCOL §14): after an
// eight-hour sleep every refresh that came due runs within the spread window,
// none twice; a deadline written while the clock ran a week ahead is brought
// back; a three-day Retry-After survives a restart unchanged.
func TestSleepAndClockChanges(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	var podcasts []string
	for _, path := range []string{"/a", "/b", "/c", "/d"} {
		p, _ := e.subscribe(fs.url(path), nil)
		podcasts = append(podcasts, p)
	}
	r := e.refresher(testFetcher(), Config{SyncEvery: time.Minute, Random: fixedRandom(0.1, 0.4, 0.7, 0.9, 0.5)})
	// Each pass reports the clock time it ran at (afterPass runs on the
	// loop's goroutine). The test moves the clock only once the loop has
	// done what it does at the current time, never on a real-time delay.
	passes := make(chan int64, 1000)
	r.afterPass = func() { passes <- r.lastPassMs }
	// passAt waits for the loop's pass at the clock's current time (it is
	// woken: a wake is never lost, and it reads the time when it runs).
	passAt := func() {
		now := e.clock.ms()
		r.signal()
		for at := <-passes; at < now; at = <-passes {
		}
	}
	// catchUp moves the clock in 30-second steps, each under SyncEvery and its
	// slack so none looks like another sleep, and each once the loop has run
	// its pass at the current time and its fetches have ended, until n
	// fetches arrived: within the 10-minute spread after from.
	catchUp := func(n int, from int64) {
		t.Helper()
		for fs.count() < n {
			if late := time.Duration(e.clock.ms()-from) * time.Millisecond; late >= 10*time.Minute+time.Minute {
				t.Fatalf("%v after the wake only %d were fetched, beyond the 10-minute spread", late, fs.count())
			}
			e.clock.Advance(30 * time.Second)
			passAt()
			for !idle(r) {
				<-passes
			}
		}
		for !idle(r) {
			<-passes
		}
	}
	r.Start(context.Background())
	for fs.count() < 4 || !idle(r) {
		<-passes
	}
	fs.seen()
	// The Mac sleeps for eight hours: the wall clock moves, the monotonic one does not.
	e.clock.Sleep(8 * time.Hour)
	woke := e.clock.ms()
	// The first pass after the wake spreads the overdue work over the next
	// ten minutes.
	passAt()
	for _, p := range podcasts {
		if next := e.row(p).NextAt; next < woke || next > woke+(10*time.Minute).Milliseconds() {
			t.Errorf("overdue work rescheduled %v after the wake, beyond the 10-minute spread", time.Duration(next-woke)*time.Millisecond)
		}
	}
	catchUp(4, woke)
	fetched := map[string]int{}
	for _, q := range fs.seen() {
		fetched[q.path]++
	}
	if len(fetched) != 4 {
		t.Errorf("fetched in the catch-up: %v", fetched)
	}
	for path, n := range fetched {
		if n != 1 {
			t.Errorf("%s fetched %d times in the catch-up", path, n)
		}
	}
	// The service page shows the catch-up.
	if st, err := r.ScheduleStatus(context.Background()); err != nil || st.LastWake < woke || st.Running != 0 {
		t.Errorf("schedule status after the wake %+v %v", st, err)
	}
	r.Stop()

	// The clock ran a week ahead when the next deadlines were written, then was corrected.
	for _, p := range podcasts[:1] {
		row := e.row(p)
		row.NextAt = e.clock.ms() + 7*day
		e.putRow(row)
	}
	// One podcast's publisher asked for three days of quiet.
	quiet := e.row(podcasts[1])
	quiet.NotBefore = e.clock.ms() + 3*day
	quiet.NextAt = quiet.NotBefore
	e.putRow(quiet)
	r = e.refresher(testFetcher(), Config{SyncEvery: time.Minute})
	passes = make(chan int64, 1000)
	r.afterPass = func() { passes <- r.lastPassMs }
	r.Start(context.Background())
	defer r.Stop()
	<-passes
	now := e.clock.ms()
	if row := e.row(podcasts[0]); row.NextAt > now+(66*time.Minute).Milliseconds() {
		t.Errorf("a deadline a week ahead was kept: %v from now", time.Duration(row.NextAt-now)*time.Millisecond)
	}
	if row := e.row(podcasts[1]); row.NotBefore != quiet.NotBefore {
		t.Errorf("Retry-After changed across the restart: %d, want %d", row.NotBefore, quiet.NotBefore)
	}
	// Two hours on (spread again: the loop did not run meanwhile), the other
	// three run; the quiet one does not.
	e.clock.Advance(2 * time.Hour)
	later := e.clock.ms()
	passAt()
	catchUp(3, later)
	for _, u := range fs.seen() {
		if strings.HasSuffix(u.path, "/b") {
			t.Errorf("fetched within the three days the publisher asked for")
		}
	}
}

// 5. One fetch per podcast at a time: requests during a fetch join it; a
// request afterwards runs the next fetch now, without validators.
func TestManualRequestsCoalesce(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fs.seen()
	passes := make(chan struct{}, 100)
	r.afterPass = func() { passes <- struct{}{} }
	gate, arrived := make(chan struct{}), make(chan string, 10)
	fs.with(func(fs *feedServer) { fs.gate, fs.arrived = gate, arrived })
	ctx := context.Background()
	r.Start(ctx)
	defer r.Stop()
	sts, err := r.Request(ctx, e.account, p)
	if err != nil || len(sts) != 1 || !sts[0].InProgress || sts[0].LastSuccess != e.clock.ms() {
		t.Fatalf("request: %+v %v", sts, err)
	}
	<-arrived // the fetch is running
	for _, podcast := range []string{p, ""} {
		sts, err := r.Request(ctx, e.account, podcast)
		if err != nil || len(sts) != 1 || !sts[0].InProgress {
			t.Fatalf("request during the fetch: %+v %v", sts, err)
		}
	}
	close(gate)
	waitIdle(r, passes)
	rs := fs.seen()
	if len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("coalesced requests: %+v", rs)
	}
	if row := e.row(p); row.NextAt != e.clock.ms()+hour {
		t.Errorf("a joined request left the podcast due: %+v", row)
	}
	// Idle: a request runs the next fetch now.
	fs.with(func(fs *feedServer) { fs.gate = nil })
	if _, err := r.Request(ctx, e.account, p); err != nil {
		t.Fatal(err)
	}
	<-arrived
	waitIdle(r, passes)
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("a manual refresh: %+v", rs)
	}
	if _, err := r.Request(ctx, e.account, "p1:"+strings.Repeat("0", 64)); err != ErrUnknownPodcast {
		t.Errorf("unknown podcast: %v", err)
	}
}

// 5b. A new subscription is fetched at once (PROTOCOL §9.4, its first population),
// not at the refresher's next scan of the subscriptions: the commit that
// opens its generation wakes the refresher.
func TestANewSubscriptionIsFetchedAtOnce(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	arrived := make(chan string, 10)
	fs.with(func(fs *feedServer) { fs.arrived = arrived })
	// The next scan is an hour away on a clock that does not move.
	r := e.refresher(testFetcher(), Config{SyncEvery: time.Hour})
	passes := make(chan struct{}, 100)
	r.afterPass = func() { passes <- struct{}{} }
	r.Start(context.Background())
	defer r.Stop()
	<-passes // nothing to fetch yet
	p, _ := e.subscribe(fs.url("/new"), nil)
	select {
	case path := <-arrived:
		if path != "/new" {
			t.Fatalf("fetched %s", path)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a new subscription waits for the next scan of the subscriptions")
	}
	waitIdle(r, passes)
	if row := e.row(p); row.LastSuccess == 0 {
		t.Errorf("the first fetch did not apply: %+v", row)
	}
}

// waitIdle waits for a pass of the loop that finds nothing running or queued.
func waitIdle(r *Refresher, passes chan struct{}) {
	for {
		if idle(r) {
			return
		}
		<-passes
	}
}

// 6. The schedule survives a restart: overdue work is spread over the spread
// window, as after a wake from sleep.
func TestScheduleSurvivesRestart(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	var podcasts []string
	for _, path := range []string{"/a", "/b", "/c"} {
		p, _ := e.subscribe(fs.url(path), nil)
		podcasts = append(podcasts, p)
	}
	first := e.refresher(testFetcher(), Config{})
	runDue(t, first)
	if n := len(fs.seen()); n != 3 {
		t.Fatalf("%d first fetches", n)
	}
	for _, p := range podcasts {
		if row := e.row(p); row.NextAt != e.clock.ms()+hour {
			t.Fatalf("row %+v", row)
		}
	}
	// The server is down for three hours.
	e.clock.Advance(3 * time.Hour)
	r := e.refresher(testFetcher(), Config{SyncEvery: time.Hour, Random: fixedRandom(0.2, 0.5, 0.8, 0.5)})
	passes := make(chan struct{}, 100)
	r.afterPass = func() { passes <- struct{}{} }
	r.Start(context.Background())
	defer r.Stop()
	<-passes
	spread := (10 * time.Minute).Milliseconds() // min(10 min, interval/6)
	now := e.clock.ms()
	seen := map[int64]bool{}
	for _, p := range podcasts {
		row := e.row(p)
		if row.NextAt <= now || row.NextAt > now+spread || seen[row.NextAt] {
			t.Fatalf("overdue work not spread: next %d, now %d", row.NextAt, now)
		}
		seen[row.NextAt] = true
	}
	if n := len(fs.seen()); n != 0 {
		t.Fatalf("%d fetches before their spread times", n)
	}
	e.clock.Advance(10 * time.Minute)
	r.signal()
	for fs.count() < 3 || !idle(r) {
		<-passes
	}
	if n := len(fs.seen()); n != 3 {
		t.Fatalf("%d fetches after the spread", n)
	}
	// A wake from sleep: the overdue work is spread again, not run at once.
	e.clock.Advance(3 * time.Hour)
	r.signal()
	now = e.clock.ms()
	spreadAgain := func() bool {
		for _, p := range podcasts {
			if row := e.row(p); row.NextAt <= now || row.NextAt > now+spread {
				return false
			}
		}
		return true
	}
	for !spreadAgain() {
		<-passes
	}
	if n := len(fs.seen()); n != 0 {
		t.Errorf("%d fetches at once after a wake", n)
	}
}

// idle: nothing runs, and no request waits for an attempt.
func idle(r *Refresher) bool {
	if requestsWaiting(r) {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.running) == 0
}

// requestsWaiting reports a pending request (PROTOCOL §9.6) the loop would
// start an attempt for.
func requestsWaiting(r *Refresher) bool {
	var cands []storage.RefreshCandidate
	if err := r.svc.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
		var err error
		cands, err = tx.RefreshCandidates("")
		return err
	}); err != nil {
		panic(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range cands {
		h, held := r.held[key{c.AccountID, c.PodcastID}]
		if s := c.Schedule; s != nil && s.RequestPending() && c.State != "merged" && (!held || r.nowMs() >= h.until || s.RequestSeq > h.seq) {
			return true
		}
	}
	return false
}

// 7. A permanent redirect moves the refresh URL and adds the target as an
// alias; a temporary one changes nothing; a fetched new-feed-url moves too.
func TestMoves(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	fs.with(func(fs *feedServer) {
		fs.redirects["/old"] = redirect{301, "/new"}
		fs.redirects["/temp"] = redirect{302, "/elsewhere"}
	})
	moved, _ := e.subscribe(fs.url("/old"), nil)
	temp, _ := e.subscribe(fs.url("/temp"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fs.seen()
	id := decode(t, e.podcastRecord(moved, "identity"))
	if id["refresh_url"] != fs.url("/new") || !strings.Contains(e.podcastRecord(moved, "identity"), fs.url("/new")) {
		t.Fatalf("after a 301: %s", e.podcastRecord(moved, "identity"))
	}
	aliases, _ := id["aliases"].([]any)
	if len(aliases) != 2 {
		t.Errorf("aliases after a 301: %v", aliases)
	}
	if id := decode(t, e.podcastRecord(temp, "identity")); id["refresh_url"] != fs.url("/temp") {
		t.Fatalf("after a 302: %v", id)
	}
	if got := e.podcastRecord(temp, "identity"); strings.Contains(got, "/elsewhere") {
		t.Errorf("a temporary redirect added an alias: %s", got)
	}
	e.clock.Advance(time.Hour)
	runDue(t, r)
	rs := fs.seen()
	if got := paths(rs); got != "/elsewhere,/new,/temp" {
		t.Fatalf("second fetches %q", got)
	}
	for _, x := range rs {
		if x.path == "/new" && x.ifNoneMatch != "" {
			t.Errorf("the moved URL was fetched with the old URL's validators")
		}
	}
	// itunes:new-feed-url, fetched successfully, moves the refresh URL.
	nf, _ := e.subscribe(fs.url("/announcing"), nil)
	fs.with(func(fs *feedServer) { fs.extra = `<itunes:new-feed-url>` + fs.url("/home") + `</itunes:new-feed-url>` })
	runDue(t, r)
	if id := decode(t, e.podcastRecord(nf, "identity")); id["refresh_url"] != fs.url("/home") {
		t.Fatalf("after new-feed-url: %v", id)
	}
	if got := e.podcastRecord(nf, "identity"); !strings.Contains(got, fs.url("/home")) {
		t.Errorf("new-feed-url not an alias: %s", got)
	}
}

// 7b. A permanent redirect to a feed that names a new-feed-url: the
// new-feed-url wins, and both the redirect target and the new-feed-url become
// aliases.
func TestNewFeedURLWinsOverRedirect(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	fs.with(func(fs *feedServer) {
		fs.redirects["/a"] = redirect{301, "/b"}
		fs.extra = `<itunes:new-feed-url>` + fs.url("/c") + `</itunes:new-feed-url>`
	})
	p, _ := e.subscribe(fs.url("/a"), nil)
	runDue(t, e.refresher(testFetcher(), Config{}))
	got := e.podcastRecord(p, "identity")
	if id := decode(t, got); id["refresh_url"] != fs.url("/c") {
		t.Fatalf("refresh URL after a redirect and a new-feed-url: %s", got)
	}
	for _, u := range []string{fs.url("/b"), fs.url("/c")} {
		if !strings.Contains(got, u) {
			t.Errorf("%s is not an alias: %s", u, got)
		}
	}
}

// 7c. A podcast merged into another (a mirror that moves permanently to the
// original, PROTOCOL §9.7) is only a redirect: the schedule never fetches it again,
// and a request for it refreshes its survivor.
func TestMergedPodcastIsNotRefreshed(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	original, _ := e.subscribe(fs.url("/original"), nil)
	mirror, _ := e.subscribe(fs.url("/mirror"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fs.with(func(fs *feedServer) { fs.redirects["/mirror"] = redirect{301, "/original"} })
	if _, err := r.refresh(context.Background(), key{e.account, mirror}, true); err != nil {
		t.Fatal(err)
	}
	if got := e.podcastRecord(mirror, "subscription"); !strings.Contains(got, `"state":"merged"`) {
		t.Fatalf("the mirror was not merged: %s", got)
	}
	fs.seen()
	// At an unchanged time nothing is due: the merged podcast is not
	// "awaiting its first population" (each pass restarted its fetch).
	for i := 0; i < 3; i++ {
		res, err := r.pass(context.Background(), false, false)
		if err != nil {
			t.Fatal(err)
		}
		r.jobs.Wait()
		if res.started != 0 {
			t.Fatalf("pass %d started %d fetches (%s)", i, res.started, paths(fs.seen()))
		}
	}
	if st, err := r.ScheduleStatus(context.Background()); err != nil || st.DueNow != 0 {
		t.Errorf("schedule status: %+v %v", st, err)
	}
	// A request for the merged podcast refreshes its survivor.
	sts, err := r.Request(context.Background(), e.account, mirror)
	if err != nil || len(sts) != 1 || sts[0].PodcastID != original || !sts[0].InProgress {
		t.Fatalf("request for the merged podcast: %+v %v", sts, err)
	}
	runDue(t, r)
	if got := paths(fs.seen()); got != "/original" {
		t.Errorf("a request for the merged podcast fetched %q", got)
	}
	// Later only the survivor is fetched, once an interval.
	for i := 0; i < 3; i++ {
		e.clock.Advance(time.Hour + 10*time.Minute)
		runDue(t, r)
		if got := paths(fs.seen()); got != "/original" {
			t.Fatalf("interval %d fetched %q", i, got)
		}
	}
}

// 8. Credentials go only to approved origins: the secret's, or the refresh
// URL's for a secret without them; never to a redirect target elsewhere.
func TestCredentialsOnlyToApprovedOrigins(t *testing.T) {
	e := newEnv(t)
	a, b := newFeedServer(t), newFeedServer(t)
	for _, fs := range []*feedServer{a, b} {
		fs.set(item{"One", e.clock.ms() - day, true})
		fs.with(func(fs *feedServer) { fs.user, fs.pass = "listener", "pa55-word" })
	}
	a.with(func(fs *feedServer) { fs.redirects["/hop"] = redirect{302, b.url("/private")} })
	originOf := func(fs *feedServer) string {
		u, _ := url.Parse(fs.srv.URL)
		return feed.Origin(u)
	}
	approved, _ := e.subscribe(a.url("/private"), &protocol.Secret{ID: newID(), Username: "listener", Password: "pa55-word",
		ApprovedOrigins: []string{originOf(a)}})
	hop, _ := e.subscribe(a.url("/hop"), &protocol.Secret{ID: newID(), Username: "listener", Password: "pa55-word",
		ApprovedOrigins: []string{originOf(a)}})
	legacy, _ := e.subscribe(a.url("/private2"), &protocol.Secret{ID: newID(), Username: "listener", Password: "pa55-word"})
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	for _, p := range []string{approved, legacy} {
		st := decode(t, e.podcastRecord(p, "refresh_status"))
		if st["error"] != nil || !strings.Contains(e.podcastRecord(p, "metadata"), `"title":"Show"`) {
			t.Errorf("an approved origin's fetch: %v", st)
		}
	}
	st := decode(t, e.podcastRecord(hop, "refresh_status"))
	if e, _ := st["error"].(map[string]any); e["label"] != "unauthorized" {
		t.Errorf("a redirect to an unapproved origin: %v", st)
	}
	sent := 0
	for _, x := range a.seen() {
		if x.authorization != "" {
			sent++
		}
	}
	if sent != 2 {
		t.Errorf("credentials sent %d times to the approved origin, want 2", sent)
	}
	for _, x := range b.seen() {
		if x.authorization != "" {
			t.Fatalf("credentials reached an unapproved origin")
		}
	}
}

// A secret stored without approved origins approves only the origin of the
// URL the user set: after a permanent move to another origin, the
// credentials stay behind (PROTOCOL §9.4).
func TestLegacyCredentialsDoNotFollowAMove(t *testing.T) {
	e := newEnv(t)
	a, b := newFeedServer(t), newFeedServer(t)
	a.set(item{"One", e.clock.ms() - day, true})
	a.with(func(fs *feedServer) { fs.user, fs.pass = "listener", "pa55-word" })
	b.set(item{"One", e.clock.ms() - day, true}) // open at first
	p, _ := e.subscribe(a.url("/private"), &protocol.Secret{ID: newID(), Username: "listener", Password: "pa55-word"})
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	if st := decode(t, e.podcastRecord(p, "refresh_status")); st["error"] != nil {
		t.Fatalf("the user's own origin: %v", st)
	}
	// The publisher moves the feed permanently to another origin.
	a.with(func(fs *feedServer) { fs.redirects["/private"] = redirect{301, b.url("/private")} })
	e.clock.Advance(time.Hour)
	runDue(t, r)
	if id := decode(t, e.podcastRecord(p, "identity")); id["refresh_url"] != b.url("/private") {
		t.Fatalf("the move did not happen: %v", id)
	}
	// The new origin asks for a password: the user approved only the first.
	b.with(func(fs *feedServer) { fs.user, fs.pass = "listener", "pa55-word" })
	b.seen()
	e.clock.Advance(time.Hour)
	runDue(t, r)
	seen := b.seen()
	if len(seen) == 0 {
		t.Fatal("the moved URL was never fetched")
	}
	for _, x := range seen {
		if x.authorization != "" {
			t.Fatalf("legacy credentials followed an automatic move to another origin")
		}
	}
	if st := decode(t, e.podcastRecord(p, "refresh_status")); st["error"] == nil {
		t.Errorf("an unapproved origin asking for credentials: %v", st)
	}
}

// 9. After a parser upgrade a stored response is re-parsed with its own
// source and time: its records are corrected, a newer device observation
// stands, and nothing is filed or published as a status.
func TestReparseAfterParserUpgrade(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	t0 := e.clock.ms()
	fs.set(item{"One", t0 - 2*day, true}, item{"Two", t0 - day, true})
	p, g := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	// A device corrects Two after the server's fetch.
	e.clock.Advance(time.Minute)
	meta := decode(t, e.episodeRecord(p, "Two", "metadata"))
	ep := e.episode(p, "Two")
	// An observation sends the whole record (PROTOCOL §9.4): the canonical one, corrected.
	fields := map[string]any{}
	for k, v := range meta {
		if k != "revision" && k != "fetch_time" {
			fields[k] = v
		}
	}
	fields["title"] = "Two (device)"
	e.push(e.fact("catalog.observe", []any{map[string]any{"entity": "episode", "id": ep}, map[string]any{"entity": "podcast", "id": p}},
		map[string]any{"podcast_id": p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": e.clock.ms(),
			"items": []any{map[string]any{"base": meta["revision"], "fields": fields,
				"episode": map[string]any{"episode_id": ep, "podcast_id": p, "generation_id": g,
					"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": "guid-Two", "guid": "guid-Two", "title": "Two"}}}}}))
	if got := e.episodeRecord(p, "Two", "metadata"); !strings.Contains(got, `"title":"Two (device)"`) {
		t.Fatalf("device correction %s", got)
	}
	// The stored response, as an older parser left it: today's parser reads
	// more from it (fake: its body now says more).
	fs.set(item{"One (fixed)", t0 - 2*day, true}, item{"Two (fixed)", t0 - day, true}, item{"Three", t0 - minute, true})
	setStored(t, e, p, func(s *feed.StoredResponse) {
		s.ParserVersion = "ntp-go/0"
		fs.with(func(fs *feedServer) { s.Body = fs.body() })
	})
	status := e.podcastRecord(p, "refresh_status")
	fs.seen()
	fresh := e.refresher(testFetcher(), Config{})
	fresh.ReparseAll(context.Background())
	if got := e.episodeRecord(p, "One", "metadata"); !strings.Contains(got, `"title":"One (fixed)"`) || !strings.Contains(got, `"fetch_time":`+strconv.FormatInt(t0, 10)) {
		t.Errorf("re-parsed record %s", got)
	}
	if got := e.episodeRecord(p, "Two", "metadata"); !strings.Contains(got, `"title":"Two (device)"`) {
		t.Errorf("the re-parse overrode a newer observation: %s", got)
	}
	if got := e.episodeRecord(p, "Three", "metadata"); !strings.Contains(got, `"title":"Three"`) {
		t.Errorf("a newly parsed item %s", got)
	}
	if got := e.episodeRecord(p, "Three", "admission"); got != "" {
		t.Errorf("a re-parse filed: %s", got)
	}
	if got := e.podcastRecord(p, "refresh_status"); got != status {
		t.Errorf("a re-parse changed the status: %s", got)
	}
	if s := stored(t, e, p); s.ParserVersion != feed.ParserVersion {
		t.Errorf("stored parser version %q", s.ParserVersion)
	}
	head := e.head()
	fresh.ReparseAll(context.Background())
	if e.head() != head {
		t.Errorf("a second re-parse wrote again")
	}
	if rs := fs.seen(); len(rs) != 0 {
		t.Errorf("the re-parse fetched: %v", rs)
	}
}

func stored(t *testing.T, e *env, podcast string) feed.StoredResponse {
	t.Helper()
	var s feed.StoredResponse
	err := e.svc.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
		b, err := tx.Account(e.account).FeedResponse(podcast)
		if err != nil {
			return err
		}
		s, err = feed.OpenStoredResponse(e.svc.Box, e.account, podcast, b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func setStored(t *testing.T, e *env, podcast string, change func(*feed.StoredResponse)) {
	t.Helper()
	s := stored(t, e, podcast)
	change(&s)
	b, err := s.Seal(e.svc.Box, e.account, podcast)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(e.account).PutFeedResponse(podcast, b)
	}); err != nil {
		t.Fatal(err)
	}
}

// Fetches run 4 at a time overall and 1 at a time per host.
func TestConcurrencyLimits(t *testing.T) {
	e := newEnv(t)
	for _, u := range []string{"http://a.example/1", "http://a.example/2", "http://a.example/3", "http://b.example/f",
		"http://c.example/f", "http://d.example/f", "http://e.example/f"} {
		e.subscribe(u, nil)
	}
	g := &gateFetcher{active: map[string]int{}, started: make(chan string, 20), release: make(chan struct{})}
	r := e.refresher(g, Config{})
	res, err := r.pass(context.Background(), false, false)
	if err != nil || res.started != 4 {
		t.Fatalf("started %d, %v", res.started, err)
	}
	for range 4 {
		<-g.started
	}
	g.mu.Lock()
	if g.maxTotal != 4 || g.maxPerHost != 1 {
		t.Errorf("concurrency %d overall, %d per host", g.maxTotal, g.maxPerHost)
	}
	g.mu.Unlock()
	close(g.release)
	runDue(t, r)
	if n := len(g.started); n != 3 {
		t.Errorf("%d further fetches, want 3", n)
	}
	if g.maxTotal > 4 || g.maxPerHost != 1 {
		t.Errorf("concurrency %d overall, %d per host", g.maxTotal, g.maxPerHost)
	}
}

type gateFetcher struct {
	mu                   sync.Mutex
	active               map[string]int
	total                int
	maxTotal, maxPerHost int
	started              chan string
	release              chan struct{}
}

func (g *gateFetcher) Fetch(ctx context.Context, req feed.FetchRequest) (*feed.FetchResult, error) {
	host := hostOf(req.URL)
	g.mu.Lock()
	g.active[host]++
	g.total++
	g.maxTotal, g.maxPerHost = max(g.maxTotal, g.total), max(g.maxPerHost, g.active[host])
	g.mu.Unlock()
	g.started <- req.URL
	select {
	case <-g.release:
	case <-ctx.Done():
	}
	g.mu.Lock()
	g.active[host]--
	g.total--
	g.mu.Unlock()
	return nil, &feed.FetchError{Label: "io", Detail: "test"}
}

// A secret stored without approved origins approves only the origin of the URL
// the user had set when the secret was set; a later URL change to another
// origin does not take the credentials along.
func TestLegacyCredentialsDoNotFollowAURLChange(t *testing.T) {
	e := newEnv(t)
	a, b := newFeedServer(t), newFeedServer(t)
	for _, fs := range []*feedServer{a, b} {
		fs.set(item{"One", e.clock.ms() - day, true})
		fs.with(func(fs *feedServer) { fs.user, fs.pass = "listener", "pa55-word" })
	}
	p, g := e.subscribe(a.url("/private"), &protocol.Secret{ID: newID(), Username: "listener", Password: "pa55-word"})
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	if st := decode(t, e.podcastRecord(p, "refresh_status")); st["error"] != nil {
		t.Fatalf("the origin the secret was set for: %v", st)
	}
	e.clock.Advance(time.Minute)
	e.push(e.fact("podcast.change_url", podTarget(p, g), map[string]any{"url": b.url("/private")}))
	e.clock.Advance(time.Hour)
	runDue(t, r)
	if len(b.seen()) == 0 {
		t.Fatal("the new URL was never fetched")
	}
	for _, x := range b.seen() {
		if x.authorization != "" {
			t.Fatalf("legacy credentials followed a URL change to another origin")
		}
	}
}

// A re-parse names the revisions its response stood for as bases, so a device
// correction that replaced the server's record (at an equal fetch time) stands.
func TestReparseKeepsACorrectionAtTheSameFetchTime(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	t0 := e.clock.ms()
	fs.set(item{"One", t0 - 2*day, true}, item{"Two", t0 - day, true})
	p, g := e.subscribe(fs.url("/feed"), nil)
	runDue(t, e.refresher(testFetcher(), Config{}))
	meta := decode(t, e.episodeRecord(p, "Two", "metadata"))
	fetchTime := meta["fetch_time"]
	fields := map[string]any{}
	for k, v := range meta {
		if k != "revision" && k != "fetch_time" {
			fields[k] = v
		}
	}
	fields["title"] = "Two (device)"
	ep := e.episode(p, "Two")
	e.clock.Advance(time.Minute)
	// The device's correction names the server's revision and the same fetch time.
	e.push(e.fact("catalog.observe", []any{map[string]any{"entity": "episode", "id": ep}, map[string]any{"entity": "podcast", "id": p}},
		map[string]any{"podcast_id": p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": fetchTime,
			"items": []any{map[string]any{"base": meta["revision"], "fields": fields,
				"episode": map[string]any{"episode_id": ep, "podcast_id": p, "generation_id": g,
					"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": "guid-Two", "guid": "guid-Two", "title": "Two"}}}}}))
	if got := e.episodeRecord(p, "Two", "metadata"); !strings.Contains(got, `"title":"Two (device)"`) {
		t.Fatalf("the correction did not replace the server's record: %s", got)
	}
	setStored(t, e, p, func(s *feed.StoredResponse) { s.ParserVersion = "ntp-go/0" })
	e.refresher(testFetcher(), Config{}).ReparseAll(context.Background())
	if got := e.episodeRecord(p, "Two", "metadata"); !strings.Contains(got, `"title":"Two (device)"`) {
		t.Errorf("the re-parse overwrote a correction made at its own fetch time: %s", got)
	}
}

// The schedule's fetch context is persisted, so after a restart a changed URL
// (or a new generation) is fetched at once rather than after the old context's
// backoff.
func TestNewGenerationAfterRestartIgnoresOldBackoff(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	fs.with(func(fs *feedServer) { fs.status = 500 })
	p, g := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	for i := 0; i < 5; i++ {
		runDue(t, r)
		e.clock.Advance(time.Duration(e.row(p).NextAt-e.clock.ms()) * time.Millisecond)
	}
	if e.row(p).Failures < 3 {
		t.Fatalf("no backoff built up: %+v", e.row(p))
	}
	// The user changes the feed's URL; the server restarts before its
	// scheduler sees the change.
	e.push(e.fact("podcast.change_url", podTarget(p, g), map[string]any{"url": fs.url("/feed?moved=1")}))
	fs.with(func(fs *feedServer) { fs.status = 0 })
	fs.seen()
	restarted := e.refresher(testFetcher(), Config{})
	runDue(t, restarted)
	if len(fs.seen()) == 0 {
		t.Fatalf("the new generation waited for the old backoff: %+v", e.row(p))
	}
}
