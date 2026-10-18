package refresh

// Refresh requests (PROTOCOL §9.6) are kept with the schedule: each gets a
// sequence, an attempt's outcome records the newest one it answered, and a
// single podcast's request stays forced until a forced attempt answers it.
// The status read schedules nothing.

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// populated subscribes to a feed and runs its first fetch, so its stored
// response gives later ordinary fetches validators.
func populated(t *testing.T, e *env, fs *feedServer, path string) string {
	t.Helper()
	fs.set(item{"One", e.clock.ms() - day, true})
	p, _ := e.subscribe(fs.url(path), nil)
	runDue(t, e.refresher(testFetcher(), Config{}))
	fs.seen()
	return p
}

// ask asks for a refresh and returns the sequence the podcast's request got.
func ask(t *testing.T, r *Refresher, account, podcast, of string) int64 {
	t.Helper()
	sts, err := r.Request(context.Background(), account, podcast)
	if err != nil {
		t.Fatal(err)
	}
	return statusOf(t, sts, of).RequestSeq
}

// waitArrived waits for a request to reach the feed server.
func waitArrived(t *testing.T, arrived chan string) {
	t.Helper()
	select {
	case <-arrived:
	case <-time.After(30 * time.Second):
		t.Fatal("no fetch reached the feed")
	}
}

// answered fails unless the podcast's requests up to seq are answered and
// none is pending.
func answered(t *testing.T, e *env, podcast string, seq int64) {
	t.Helper()
	row := e.row(podcast)
	if row.DoneSeq < seq || row.RequestPending() {
		t.Fatalf("request %d not answered: request_seq %d, force %d, done %d", seq, row.RequestSeq, row.RequestForceSeq, row.DoneSeq)
	}
}

// An attempt a restart cuts short answers nothing: its request, and
// one that joined it, are still pending after the restart (the podcast is
// populated and not kept updated, so nothing else would fetch it), run once,
// and are answered. Before, both lived in memory only and were lost.
func TestRequestsSurviveARestartThatCutsTheirAttemptShort(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := populated(t, e, fs, "/feed")
	pod := decode(t, e.podcastRecord(p, "subscription"))
	e.push(e.fact("podcast.set_policy", podTarget(p, pod["generation_id"].(string)), map[string]any{"keep_updated": false}))
	gate := make(chan struct{})
	arrived := make(chan string, 4)
	fs.with(func(fs *feedServer) { fs.gate, fs.arrived = gate, arrived })
	r := e.refresher(testFetcher(), Config{})
	first := ask(t, r, e.account, p, p)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := r.pass(ctx, false, false); err != nil {
		t.Fatal(err)
	}
	waitArrived(t, arrived)
	joined := ask(t, r, e.account, p, p) // joins the attempt in progress
	if first != 1 || joined != 2 {
		t.Fatalf("request sequences %d, %d", first, joined)
	}
	cancel() // the server stops while the fetch runs
	r.jobs.Wait()
	close(gate)
	fs.with(func(fs *feedServer) { fs.gate, fs.arrived = nil, nil })
	fs.seen()
	if row := e.row(p); !row.RequestPending() || row.DoneSeq != 0 {
		t.Fatalf("a cut-short attempt answered its requests: %+v", row)
	}
	restarted := e.refresher(testFetcher(), Config{})
	runDue(t, restarted)
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("after the restart: %+v", rs)
	}
	answered(t, e, p, joined)
}

// A request made after an attempt's outcome was written, while the
// attempt is still listed as running, joins nothing that answers it: it
// stays pending and runs. Before, it joined the finished attempt and was lost.
func TestARequestJoiningAFinishedAttemptRuns(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := populated(t, e, fs, "/feed")
	r := e.refresher(testFetcher(), Config{})
	var once sync.Once
	var late int64
	r.beforeFinish = func(k key) {
		once.Do(func() { late = ask(t, r, e.account, p, p) })
	}
	first := ask(t, r, e.account, p, p)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 2 {
		t.Fatalf("%d fetches for a request made after the first attempt's outcome", len(rs))
	}
	if late != first+1 {
		t.Fatalf("sequences %d, %d", first, late)
	}
	answered(t, e, p, late)
}

// Every way an attempt ends answers the requests it covers, and none
// runs again for them: a success, a 304, a failed fetch, an outcome that could
// not be written (its fallback was), a panic in the outcome; a podcast merged
// into another is answered without an attempt, and a removed podcast's
// explicit refresh runs (PROTOCOL §9.6) and is answered. An attempt that wrote
// nothing answers nothing (TestARequestOutlivesStorageThatCannotWrite,
// TestAContextThatCannotBeReadAnswersNothing).
func TestEveryOutcomeAnswersTheRequest(t *testing.T) {
	for _, c := range []struct {
		name    string
		single  bool // a single podcast's request (else a refresh of all)
		prepare func(t *testing.T, e *env, fs *feedServer, r *Refresher, p string)
		fetches int
	}{
		{"success", true, nil, 1},
		{"not modified", false, nil, 1},
		{"failed fetch", true, func(t *testing.T, e *env, fs *feedServer, r *Refresher, p string) {
			fs.with(func(fs *feedServer) { fs.status = 500 })
		}, 1},
		{"outcome not written", true, func(t *testing.T, e *env, fs *feedServer, r *Refresher, p string) {
			var once sync.Once
			r.fault = func(stage string) (err error) {
				if stage == "outcome" {
					once.Do(func() { err = errors.New("storage failed") })
				}
				return err
			}
		}, 1},
		{"panic in the outcome", true, func(t *testing.T, e *env, fs *feedServer, r *Refresher, p string) {
			var once sync.Once
			r.fault = func(stage string) error {
				if stage == "outcome" {
					once.Do(func() { panic("outcome exploded") })
				}
				return nil
			}
		}, 1},
		{"removed", true, func(t *testing.T, e *env, fs *feedServer, r *Refresher, p string) {
			pod := decode(t, e.podcastRecord(p, "subscription"))
			e.push(e.fact("podcast.unsubscribe", podTarget(p, pod["generation_id"].(string)), map[string]any{}))
		}, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			fs := newFeedServer(t)
			p := populated(t, e, fs, "/feed")
			e.clock.Advance(10 * time.Minute)
			r := e.refresher(testFetcher(), Config{})
			target := ""
			if c.single {
				target = p
			}
			seq := ask(t, r, e.account, target, p)
			if c.prepare != nil {
				c.prepare(t, e, fs, r, p)
			}
			runDue(t, r)
			if rs := fs.seen(); len(rs) != c.fetches {
				t.Fatalf("%d fetches: %+v", len(rs), rs)
			}
			answered(t, e, p, seq)
			runDue(t, r)
			if rs := fs.seen(); len(rs) != 0 {
				t.Fatalf("ran again for an answered request: %+v", rs)
			}
		})
	}
}

// A request for a podcast that is merged into another before it runs
// is answered without a fetch; the status names the survivor.
func TestAMergedPodcastsRequestIsAnswered(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	original, _ := e.subscribe(fs.url("/original"), nil)
	mirror, _ := e.subscribe(fs.url("/mirror"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	seq := ask(t, r, e.account, mirror, mirror)
	// Before it runs, a fetch of the original moves permanently to the
	// mirror's URL: the same feed, so the mirror (created later) is merged
	// into the original (PROTOCOL §9.7).
	fs.with(func(fs *feedServer) { fs.redirects["/original"] = redirect{301, "/mirror"} })
	if _, err := r.refresh(context.Background(), key{e.account, original}, true); err != nil {
		t.Fatal(err)
	}
	if got := e.podcastRecord(mirror, "subscription"); !strings.Contains(got, `"state":"merged"`) {
		t.Fatalf("the mirror was not merged: %s", got)
	}
	fs.seen()
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 0 {
		t.Fatalf("a merged podcast was fetched: %+v", rs)
	}
	answered(t, e, mirror, seq)
	sts, err := r.Status(context.Background(), e.account, []string{mirror}, nil)
	if err != nil || len(sts) != 1 || sts[0].MergedInto != original || sts[0].InProgress {
		t.Fatalf("status %+v %v", sts, err)
	}
}

// A refresh of every podcast records no request for one it leaves to
// its Retry-After (its answer has no sequence) and one for the others; an
// ordinary request a Retry-After recorded later holds back is answered, not
// left pending.
func TestRefreshAllLeavesRetryAfterUnrequested(t *testing.T) {
	e := newEnv(t)
	r := e.refresher(testFetcher(), Config{})
	limited, other, _, _ := retryAfterFeeds(t, e, r, "7200")
	e.clock.Advance(30 * time.Second)
	sts, err := r.Request(context.Background(), e.account, "")
	if err != nil {
		t.Fatal(err)
	}
	if st := statusOf(t, sts, limited); st.RequestSeq != 0 || st.InProgress {
		t.Errorf("the limited podcast's answer %+v", st)
	}
	if st := statusOf(t, sts, other); st.RequestSeq == 0 || !st.InProgress {
		t.Errorf("the other podcast's answer %+v", st)
	}
	runDue(t, r)
	answered(t, e, other, statusOf(t, sts, other).RequestSeq)
	if row := e.row(limited); row.RequestSeq != 0 {
		t.Errorf("a request was recorded for the limited podcast: %+v", row)
	}
	// An ordinary request pending when a Retry-After is recorded.
	e.clock.Advance(3 * time.Hour)
	runDue(t, r)
	e.clock.Advance(10 * time.Minute)
	seq := ask(t, r, e.account, "", other)
	row := e.row(other)
	row.Failures, row.NotBefore, row.NextAt = 1, e.clock.ms()+2*hour, e.clock.ms()+2*hour
	e.putRow(row)
	runDue(t, r)
	answered(t, e, other, seq)
}

// A single podcast's request, then a refresh of all before it starts:
// one forced attempt (no validators) answers both. Before, force was kept, but
// the second request had no way to know it was answered.
func TestSingleThenAllIsOneForcedAttempt(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := populated(t, e, fs, "/feed")
	e.clock.Advance(10 * time.Minute)
	r := e.refresher(testFetcher(), Config{})
	ask(t, r, e.account, p, p)
	all := ask(t, r, e.account, "", p)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("fetches %+v", rs)
	}
	answered(t, e, p, all)
}

// A refresh of all, then a single podcast's request before it starts:
// one forced attempt. A single podcast's request made while an ordinary
// attempt runs is not answered by it: a forced attempt follows (Android forces
// a single feed's refresh).
func TestAForcedRequestIsNeverDowngraded(t *testing.T) {
	t.Run("before the start", func(t *testing.T) {
		e := newEnv(t)
		fs := newFeedServer(t)
		p := populated(t, e, fs, "/feed")
		e.clock.Advance(10 * time.Minute)
		r := e.refresher(testFetcher(), Config{})
		ask(t, r, e.account, "", p)
		single := ask(t, r, e.account, p, p)
		runDue(t, r)
		if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
			t.Fatalf("fetches %+v", rs)
		}
		answered(t, e, p, single)
	})
	t.Run("during an ordinary attempt", func(t *testing.T) {
		e := newEnv(t)
		fs := newFeedServer(t)
		p := populated(t, e, fs, "/feed")
		e.clock.Advance(10 * time.Minute)
		r := e.refresher(testFetcher(), Config{})
		ask(t, r, e.account, "", p)
		gate := make(chan struct{})
		arrived := make(chan string, 4)
		fs.with(func(fs *feedServer) { fs.gate, fs.arrived = gate, arrived })
		done := make(chan struct{})
		go func() {
			defer close(done)
			runDue(t, r)
		}()
		waitArrived(t, arrived) // the ordinary fetch, with validators, is under way
		single := ask(t, r, e.account, p, p)
		fs.with(func(fs *feedServer) { fs.gate, fs.arrived = nil, nil })
		close(gate)
		<-done
		rs := fs.seen()
		if len(rs) != 2 || rs[0].ifNoneMatch == "" || rs[1].ifNoneMatch != "" {
			t.Fatalf("fetches %+v", rs)
		}
		answered(t, e, p, single)
	})
}

// The status read schedules nothing: the schedule row is unchanged and no
// fetch follows it.
func TestRefreshStatusReadSchedulesNothing(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := populated(t, e, fs, "/feed")
	e.clock.Advance(10 * time.Minute)
	r := e.refresher(testFetcher(), Config{})
	before := e.row(p)
	sts, err := r.Status(context.Background(), e.account, []string{p, "p1:missing"}, nil)
	if err != nil || len(sts) != 1 || sts[0].PodcastID != p || sts[0].InProgress || sts[0].NextAttempt != before.NextAt {
		t.Fatalf("status %+v %v", sts, err)
	}
	if after := e.row(p); after != before {
		t.Fatalf("the status read changed the schedule: %+v → %+v", before, after)
	}
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 0 {
		t.Fatalf("the status read caused fetches: %+v", rs)
	}
}

// A refresh that finds the feed unchanged (304) commits nothing to the log
// (no event reaches a client), yet the status read shows the request
// answered: in progress until the attempt ends, then not.
func TestRefreshStatusAfterUnchangedFetch(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := populated(t, e, fs, "/feed")
	e.clock.Advance(10 * time.Minute)
	r := e.refresher(testFetcher(), Config{})
	head := e.head()
	seq := ask(t, r, e.account, "", p)
	sts, err := r.Status(context.Background(), e.account, []string{p}, nil)
	if err != nil || len(sts) != 1 || !sts[0].InProgress || sts[0].RequestSeq != seq || sts[0].DoneSeq >= seq {
		t.Fatalf("before the fetch: %+v %v", sts, err)
	}
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch == "" {
		t.Fatalf("fetches %+v", rs)
	}
	sts, err = r.Status(context.Background(), e.account, []string{p}, nil)
	if err != nil || len(sts) != 1 || sts[0].InProgress || sts[0].DoneSeq < seq {
		t.Fatalf("after a 304: %+v %v", sts, err)
	}
	if h := e.head(); h != head {
		t.Errorf("the unchanged fetch committed: head %d → %d", head, h)
	}
}

// keptUpdatedOff subscribes, fetches the feed once and turns Keep updated
// off: nothing but a request fetches it again.
func keptUpdatedOff(t *testing.T, e *env, fs *feedServer) string {
	t.Helper()
	p := populated(t, e, fs, "/feed")
	pod := decode(t, e.podcastRecord(p, "subscription"))
	e.push(e.fact("podcast.set_policy", podTarget(p, pod["generation_id"].(string)), map[string]any{"keep_updated": false}))
	return p
}

// Single-podcast requests made while an ordinary attempt (a refresh of
// all, with validators) runs are not answered by it, the first of several no
// more than the last, nor the ordinary requests between them: one waits for
// sequence 2 (a browser), another for 4 (a phone). The ordinary attempt answers
// what it started with; the forced follow-up answers the rest. Before, the
// ordinary 304 answered every forced request but the newest.
func TestAnOrdinaryAttemptAnswersNoForcedRequestMadeDuringIt(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := populated(t, e, fs, "/feed")
	e.clock.Advance(10 * time.Minute)
	r := e.refresher(testFetcher(), Config{})
	all := ask(t, r, e.account, "", p)
	gate := make(chan struct{})
	arrived := make(chan string, 4)
	fs.with(func(fs *feedServer) { fs.gate, fs.arrived = gate, arrived })
	if _, err := r.pass(context.Background(), false, false); err != nil {
		t.Fatal(err)
	}
	waitArrived(t, arrived) // the ordinary fetch, with validators, is under way
	browser := ask(t, r, e.account, p, p)
	between := ask(t, r, e.account, "", p)
	phone := ask(t, r, e.account, p, p)
	if all != 1 || browser != 2 || between != 3 || phone != 4 {
		t.Fatalf("sequences %d %d %d %d", all, browser, between, phone)
	}
	fs.with(func(fs *feedServer) { fs.gate, fs.arrived = nil, nil })
	close(gate)
	r.jobs.Wait() // that attempt alone: no pass dispatches the follow-up yet
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch == "" {
		t.Fatalf("fetches %+v", rs)
	}
	sts, err := r.Status(context.Background(), e.account, []string{p}, nil)
	if err != nil || len(sts) != 1 {
		t.Fatal(sts, err)
	}
	if st := sts[0]; st.DoneSeq != all || st.DoneSeq >= browser || st.DoneSeq >= phone || !st.InProgress {
		t.Fatalf("after the ordinary attempt: %+v (the browser waits for %d, the phone for %d)", st, browser, phone)
	}
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("follow-up fetches %+v", rs)
	}
	answered(t, e, p, phone)
}

// A request whose attempt could write neither its outcome nor the
// fallback (storage refusing writes to the schedule) stays pending, and once
// storage works again it runs and is answered after a short hold, without a
// restart or a new request, though Keep updated is off. Before, it was held
// for as long as the process ran.
func TestARequestOutlivesStorageThatCannotWrite(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := keptUpdatedOff(t, e, fs)
	r := e.refresher(testFetcher(), Config{})
	seq := ask(t, r, e.account, p, p)
	db, err := sql.Open("sqlite", filepath.Join(e.svc.Store.Dir(), "nottennapod.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TRIGGER test_schedule_fails BEFORE UPDATE ON refresh_schedule BEGIN SELECT RAISE(ABORT, 'storage failing'); END`); err != nil {
		t.Fatal(err)
	}
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 {
		t.Fatalf("fetches while storage fails: %+v", rs)
	}
	if row := e.row(p); row.DoneSeq != 0 || !row.RequestPending() {
		t.Fatalf("an attempt that wrote nothing answered: %+v", row)
	}
	runDue(t, r) // held: no tight loop
	if rs := fs.seen(); len(rs) != 0 {
		t.Fatalf("ran again at once: %+v", rs)
	}
	if _, err := db.Exec(`DROP TRIGGER test_schedule_fails`); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(retryHold)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("after storage recovered: %+v", rs)
	}
	answered(t, e, p, seq)
}

// A request whose attempt cannot read its context (no fetch, nothing
// written) is not reported answered while the old success is all a person
// can see: the status read keeps it in progress, and after a short hold it
// runs and is answered by a real fetch, though Keep updated is off. Before, it
// was answered at once with no fetch and no failure.
func TestAContextThatCannotBeReadAnswersNothing(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := keptUpdatedOff(t, e, fs)
	before := e.row(p)
	status := e.podcastRecord(p, "refresh_status")
	e.clock.Advance(10 * time.Minute)
	r := e.refresher(testFetcher(), Config{})
	seq := ask(t, r, e.account, p, p)
	var once sync.Once
	r.fault = func(stage string) (err error) {
		if stage == "capture" {
			once.Do(func() { err = errors.New("storage failed") })
		}
		return err
	}
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 0 {
		t.Fatalf("fetches %+v", rs)
	}
	sts, err := r.Status(context.Background(), e.account, []string{p}, nil)
	if err != nil || len(sts) != 1 || !sts[0].InProgress || sts[0].DoneSeq >= seq || sts[0].LastAttempt != before.LastAttempt {
		t.Fatalf("status after a context that could not be read: %+v %v", sts, err)
	}
	if got := e.podcastRecord(p, "refresh_status"); got != status {
		t.Fatalf("the visible status changed: %s → %s", status, got)
	}
	e.clock.Advance(retryHold)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("after the hold: %+v", rs)
	}
	answered(t, e, p, seq)
	if row := e.row(p); row.LastAttempt != e.clock.ms() || row.LastSuccess != e.clock.ms() {
		t.Fatalf("the answering attempt is not the visible one: %+v", row)
	}
}

// openRaw opens the store's database beside it, for triggers that make
// storage refuse some writes (the refresher's own code never does this).
func openRaw(t *testing.T, e *env) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(e.svc.Store.Dir(), "nottennapod.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
}

// pendingAndUnchanged fails unless the request is still pending, the status
// read says so, and what a person sees (the published status) is unchanged.
func pendingAndUnchanged(t *testing.T, e *env, r *Refresher, p string, seq int64, status string) {
	t.Helper()
	sts, err := r.Status(context.Background(), e.account, []string{p}, nil)
	if err != nil || len(sts) != 1 || !sts[0].InProgress || sts[0].DoneSeq >= seq {
		t.Fatalf("a request answered with nothing to show for it: %+v %v", sts, err)
	}
	if got := e.podcastRecord(p, "refresh_status"); got != status {
		t.Fatalf("the visible status changed: %s → %s", status, got)
	}
}

// An outcome that cannot be written, nor the failure recorded in its
// place, while the backoff alone can be: the request stays pending (nothing a
// person can see changed), and once storage works again it runs and the new
// episode arrives. Before, the backoff write answered it with the old success
// shown, and nothing ran again.
func TestARequestIsAnsweredOnlyWithAnOutcomeToShow(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := keptUpdatedOff(t, e, fs)
	status := e.podcastRecord(p, "refresh_status")
	e.clock.Advance(10 * time.Minute)
	fs.set(item{"One", e.clock.ms() - day, true}, item{"Two", e.clock.ms() - hour, true})
	r := e.refresher(testFetcher(), Config{})
	seq := ask(t, r, e.account, p, p)
	db := openRaw(t, e)
	exec(t, db, `CREATE TRIGGER test_all_fail BEFORE UPDATE ON refresh_schedule BEGIN SELECT RAISE(ABORT, 'storage failing'); END`)
	exec(t, db, `CREATE TRIGGER test_outcome_fails BEFORE UPDATE ON refresh_schedule WHEN NEW.last_attempt IS NOT OLD.last_attempt
		BEGIN SELECT RAISE(ABORT, 'storage failing'); END`)
	runDue(t, r)
	pendingAndUnchanged(t, e, r, p, seq, status)
	// Storage recovers in part: the backoff can be written, an outcome cannot.
	exec(t, db, `DROP TRIGGER test_all_fail`)
	e.clock.Advance(holdAfter(1))
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 2 {
		t.Fatalf("fetches %+v", rs)
	}
	pendingAndUnchanged(t, e, r, p, seq, status)
	if got := e.episodeRecord(p, "Two", "metadata"); got != "" {
		t.Fatalf("the new episode arrived without an outcome: %s", got)
	}
	// It recovers in full: the held request runs again and the new episode arrives.
	exec(t, db, `DROP TRIGGER test_outcome_fails`)
	e.clock.Advance(holdAfter(2))
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 {
		t.Fatalf("after recovery: %+v", rs)
	}
	answered(t, e, p, seq)
	if got := e.episodeRecord(p, "Two", "metadata"); got == "" {
		t.Fatal("the new episode never arrived")
	}
}

// The panic's fallback of an unwritable outcome: a panic in the outcome, and again in recording
// the failure, leaves only the backoff written; the request stays pending
// with nothing changed for a person to see, and runs again after the hold.
// Before, the fallback answered it with the old success shown.
func TestAPanicsFallbackAnswersNothing(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := keptUpdatedOff(t, e, fs)
	status := e.podcastRecord(p, "refresh_status")
	e.clock.Advance(10 * time.Minute)
	r := e.refresher(testFetcher(), Config{})
	seq := ask(t, r, e.account, p, p)
	var mu sync.Mutex
	failing := true
	r.fault = func(stage string) error {
		mu.Lock()
		defer mu.Unlock()
		if stage == "outcome" && failing {
			panic("outcome exploded")
		}
		return nil
	}
	runDue(t, r)
	if row := e.row(p); row.Failures != 1 {
		t.Fatalf("the fallback did not back off: %+v", row)
	}
	pendingAndUnchanged(t, e, r, p, seq, status)
	mu.Lock()
	failing = false
	mu.Unlock()
	e.clock.Advance(holdAfter(1))
	runDue(t, r)
	answered(t, e, p, seq)
	if row := e.row(p); row.LastSuccess != e.clock.ms() {
		t.Fatalf("not answered by a success: %+v", row)
	}
}

// An outcome that cannot be written once is recorded as an I/O failure in its
// place (the catalog untouched): the request is answered with an error a
// person can see and a new attempt time. Before, the backoff alone answered it
// with the old success shown.
func TestAnOutcomeThatCannotBeWrittenIsAVisibleFailure(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	p := keptUpdatedOff(t, e, fs)
	e.clock.Advance(10 * time.Minute)
	r := e.refresher(testFetcher(), Config{})
	seq := ask(t, r, e.account, p, p)
	var once sync.Once
	r.fault = func(stage string) (err error) {
		if stage == "outcome" {
			once.Do(func() { err = errors.New("storage failed") })
		}
		return err
	}
	runDue(t, r)
	answered(t, e, p, seq)
	if row := e.row(p); row.LastAttempt != e.clock.ms() || !strings.Contains(row.LastError, `"label":"io"`) {
		t.Fatalf("schedule after an outcome that could not be written: %+v", row)
	}
	if got := e.podcastRecord(p, "refresh_status"); !strings.Contains(got, `"label":"io"`) {
		t.Fatalf("the visible status shows no failure: %s", got)
	}
}
