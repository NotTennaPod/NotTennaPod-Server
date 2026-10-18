package refresh

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// PROTOCOL §9.3: a host whose ETag does not match its unchanged body is sent
// If-Modified-Since alone, and fetched without validators once a day after its
// last full response. §9.6: a refresh of every podcast does not bring one
// forward past its Retry-After.

const stableLastModified = "Mon, 02 Jan 2023 15:04:05 GMT"

// unreliableHost serves a feed whose ETag changes with every response and
// whose Last-Modified stays: it answers 304 only to If-Modified-Since alone.
func unreliableHost(t *testing.T, e *env) *feedServer {
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	fs.with(func(fs *feedServer) { fs.varyETag, fs.lastModified = true, stableLastModified })
	return fs
}

// learnUnreliable subscribes to the host and runs the fetches that show its
// ETag is unreliable: the first population, then a scheduled fetch whose
// If-None-Match gets the same body again. It returns the podcast and the
// start of that fetch, the last full response.
func learnUnreliable(t *testing.T, e *env, fs *feedServer, r *Refresher) (podcast, generation string, full int64) {
	t.Helper()
	podcast, generation = e.subscribe(fs.url("/feed"), nil)
	runDue(t, r)
	e.clock.Advance(time.Hour)
	full = e.clock.ms()
	runDue(t, r)
	rs := fs.seen()
	if len(rs) != 2 || rs[0].ifNoneMatch != "" || rs[1].ifNoneMatch == "" || rs[1].ifModifiedSince != stableLastModified {
		t.Fatalf("learning: requests %+v", rs)
	}
	if s := stored(t, e, podcast); !s.ETagUnreliable || s.Now != full {
		t.Fatalf("the stored response after the same body again: unreliable %v, now %d (want %d)", s.ETagUnreliable, s.Now, full)
	}
	return podcast, generation, full
}

// expectRequest checks the one request a fetch made.
func expectRequest(t *testing.T, fs *feedServer, what string, ifNoneMatch, ifModifiedSince bool) {
	t.Helper()
	rs := fs.seen()
	if len(rs) != 1 || (rs[0].ifNoneMatch != "") != ifNoneMatch || (rs[0].ifModifiedSince != "") != ifModifiedSince {
		t.Fatalf("%s: requests %+v, want one with If-None-Match %v and If-Modified-Since %v", what, rs, ifNoneMatch, ifModifiedSince)
	}
}

// After its ETag failed to match the unchanged body, the host gets
// If-Modified-Since alone and answers 304.
func TestUnreliableETagFallsBackToLastModified(t *testing.T) {
	e := newEnv(t)
	fs := unreliableHost(t, e)
	r := e.refresher(testFetcher(), Config{})
	p, _, full := learnUnreliable(t, e, fs, r)
	for range 3 {
		e.clock.Advance(time.Hour)
		head := e.head()
		runDue(t, r)
		expectRequest(t, fs, "flagged", false, true)
		if row := e.row(p); row.LastSuccess != e.clock.ms() || row.Failures != 0 {
			t.Fatalf("the 304 was not a success: %+v", row)
		}
		if s := stored(t, e, p); s.Now != full || !s.ETagUnreliable {
			t.Fatalf("a 304 replaced the stored response: now %d, unreliable %v", s.Now, s.ETagUnreliable)
		}
		if e.head() != head {
			t.Fatalf("a 304 wrote to the log")
		}
	}
}

// A host that answers 304 to any conditional request, even after its feed
// changed: at the first fetch a day after the last full response, no
// validators are sent and the change is applied.
func TestUnreliableETagSafetyFetchIsUnconditional(t *testing.T) {
	e := newEnv(t)
	fs := unreliableHost(t, e)
	r := e.refresher(testFetcher(), Config{})
	p, _, full := learnUnreliable(t, e, fs, r)
	fs.with(func(fs *feedServer) {
		fs.any304 = true
		fs.items = append(fs.items, item{"Two", e.clock.ms(), true})
	})
	for e.clock.ms()+hour < full+day {
		e.clock.Advance(time.Hour)
		runDue(t, r)
		expectRequest(t, fs, "before the day", false, true)
		if got := e.episodeRecord(p, "Two", "metadata"); got != "" {
			t.Fatalf("applied before the safety fetch: %s", got)
		}
	}
	e.clock.Advance(time.Hour)
	if e.clock.ms() != full+day {
		t.Fatalf("the safety fetch's time %d, want %d", e.clock.ms(), full+day)
	}
	runDue(t, r)
	expectRequest(t, fs, "a day after the last full response", false, false)
	if got := e.episodeRecord(p, "Two", "metadata"); !strings.Contains(got, `"title":"Two"`) {
		t.Fatalf("the change was not applied: %q", got)
	}
	if s := stored(t, e, p); s.Now != e.clock.ms() || !s.ETagUnreliable {
		t.Fatalf("after the safety fetch: now %d, unreliable %v", s.Now, s.ETagUnreliable)
	}
	// The day counts again from this full response.
	e.clock.Advance(time.Hour)
	runDue(t, r)
	expectRequest(t, fs, "after the safety fetch", false, true)
}

// The flag is kept with the stored response, so a restarted server still
// sends If-Modified-Since alone and still makes its safety fetch a day after
// the last full response: the host's 304s to any conditional request do not
// hide a change for longer.
func TestUnreliableETagSurvivesRestart(t *testing.T) {
	e := newEnv(t)
	fs := unreliableHost(t, e)
	p, _, full := learnUnreliable(t, e, fs, e.refresher(testFetcher(), Config{}))
	fs.with(func(fs *feedServer) {
		fs.any304 = true
		fs.items = append(fs.items, item{"Two", e.clock.ms(), true})
	})
	restarted := e.refresher(testFetcher(), Config{})
	e.clock.Advance(time.Hour)
	runDue(t, restarted)
	expectRequest(t, fs, "after the restart", false, true)
	e.clock.Advance(time.Duration(full+day-e.clock.ms()) * time.Millisecond)
	runDue(t, restarted)
	expectRequest(t, fs, "a day after the last full response, after a restart", false, false)
	if got := e.episodeRecord(p, "Two", "metadata"); !strings.Contains(got, `"title":"Two"`) {
		t.Fatalf("a restart hid the change: %q", got)
	}
}

// A change of URL, credentials or representation (another final URL) forgets
// the flag: the next conditional fetch sends If-None-Match again.
func TestUnreliableETagForgottenOnSourceRepresentationOrCredentialChange(t *testing.T) {
	for _, change := range []string{"url", "credentials", "representation"} {
		t.Run(change, func(t *testing.T) {
			e := newEnv(t)
			fs := unreliableHost(t, e)
			r := e.refresher(testFetcher(), Config{})
			p, g, _ := learnUnreliable(t, e, fs, r)
			e.clock.Advance(time.Minute)
			switch change {
			case "url":
				e.push(e.fact("podcast.change_url", podTarget(p, g), map[string]any{"url": fs.url("/feed?edition=2")}))
			case "credentials":
				secret := protocol.Secret{ID: newID(), Username: "u", Password: "p"}
				e.push(e.fact("podcast.set_credentials", podTarget(p, g), map[string]any{"secret_id": secret.ID}), secret)
			case "representation":
				fs.with(func(fs *feedServer) { fs.redirects["/feed"] = redirect{302, "/elsewhere"} })
				e.clock.Advance(time.Hour)
			}
			runDue(t, r)
			fs.seen()
			if s := stored(t, e, p); s.ETagUnreliable {
				t.Fatalf("the flag survived the change")
			}
			e.clock.Advance(time.Hour)
			runDue(t, r)
			rs := fs.seen()
			last := rs[len(rs)-1]
			if last.ifNoneMatch == "" {
				t.Fatalf("after the change: requests %+v, want If-None-Match again", rs)
			}
		})
	}
}

// A host whose Last-Modified changes with every response is never flagged:
// every scheduled fetch sends If-None-Match.
func TestChangedLastModifiedIsNeverFlagged(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	fs.with(func(fs *feedServer) { fs.varyETag, fs.changingLM = true, true })
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fs.seen()
	for i := range 4 {
		e.clock.Advance(time.Hour)
		runDue(t, r)
		expectRequest(t, fs, "scheduled", true, true)
		if s := stored(t, e, p); s.ETagUnreliable {
			t.Fatalf("flagged after fetch %d", i+1)
		}
	}
}

// An honest host keeps both validators and gets no daily fetch without them:
// two days of hourly 304s, and the stored response is the first one.
func TestHonestHostKeepsBothValidatorsAndNoDailyFullFetch(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	fs.with(func(fs *feedServer) { fs.lastModified = stableLastModified })
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	first := e.clock.ms()
	fs.seen()
	for range 49 {
		e.clock.Advance(time.Hour)
		runDue(t, r)
		expectRequest(t, fs, "scheduled", true, true)
	}
	if s := stored(t, e, p); s.Now != first || s.ETagUnreliable {
		t.Fatalf("an honest host's stored response: now %d (first %d), unreliable %v", s.Now, first, s.ETagUnreliable)
	}
}

// A flagged host's safety fetch keeps the failure backoff and Retry-After:
// a 503 with Retry-After at the safety fetch delays the next attempt to it,
// and nothing is fetched before.
func TestFlaggedHostRespectsBackoffAndRetryAfter(t *testing.T) {
	e := newEnv(t)
	fs := unreliableHost(t, e)
	r := e.refresher(testFetcher(), Config{})
	p, _, full := learnUnreliable(t, e, fs, r)
	e.clock.Advance(time.Duration(full+day-e.clock.ms()) * time.Millisecond)
	fs.with(func(fs *feedServer) { fs.status, fs.retryAfter = 503, "7200" })
	runDue(t, r)
	expectRequest(t, fs, "the safety fetch", false, false)
	row := e.row(p)
	if row.NotBefore != e.clock.ms()+2*hour || row.NextAt != row.NotBefore {
		t.Fatalf("after a Retry-After at the safety fetch: %+v", row)
	}
	fs.with(func(fs *feedServer) { fs.status, fs.retryAfter = 500, "" })
	e.clock.Advance(2*time.Hour - time.Second)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 0 {
		t.Fatalf("fetched before the Retry-After: %+v", rs)
	}
	// A failure then backs off from 15 minutes, as for any host.
	e.clock.Advance(time.Second)
	runDue(t, r)
	expectRequest(t, fs, "after the Retry-After", false, false)
	if row := e.row(p); row.NextAt != e.clock.ms()+30*minute || row.Failures != 2 {
		t.Fatalf("backoff after the second failure: %+v", row)
	}
	fs.with(func(fs *feedServer) { fs.status = 0 })
	e.clock.Advance(30*time.Minute - time.Second)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 0 {
		t.Fatalf("fetched before the backoff: %+v", rs)
	}
	e.clock.Advance(time.Second)
	runDue(t, r)
	expectRequest(t, fs, "after the backoff", false, false)
	if s := stored(t, e, p); s.Now != e.clock.ms() || !s.ETagUnreliable {
		t.Fatalf("after recovering: now %d, unreliable %v", s.Now, s.ETagUnreliable)
	}
	e.clock.Advance(time.Hour)
	runDue(t, r)
	expectRequest(t, fs, "after recovering", false, true)
}

// retryAfterFeeds subscribes to two feeds and fetches them; the first then
// answers 503 with Retry-After seconds at its next scheduled fetch.
func retryAfterFeeds(t *testing.T, e *env, r *Refresher, seconds string) (limited, other string, limitedFS, otherFS *feedServer) {
	t.Helper()
	limitedFS, otherFS = newFeedServer(t), newFeedServer(t)
	for _, fs := range []*feedServer{limitedFS, otherFS} {
		fs.set(item{"One", e.clock.ms() - day, true})
	}
	limited, _ = e.subscribe(limitedFS.url("/limited"), nil)
	other, _ = e.subscribe(otherFS.url("/other"), nil)
	runDue(t, r)
	limitedFS.with(func(fs *feedServer) { fs.status, fs.retryAfter = 503, seconds })
	e.clock.Advance(time.Hour)
	runDue(t, r)
	if row := e.row(limited); row.NotBefore == 0 {
		t.Fatalf("no Retry-After recorded: %+v", row)
	}
	limitedFS.with(func(fs *feedServer) { fs.status, fs.retryAfter = 0, "" })
	limitedFS.seen()
	otherFS.seen()
	return
}

func statusOf(t *testing.T, sts []Status, podcast string) Status {
	t.Helper()
	for _, s := range sts {
		if s.PodcastID == podcast {
			return s
		}
	}
	t.Fatalf("no status for %s in %+v", podcast, sts)
	return Status{}
}

// A refresh of every podcast fetches the others at once and leaves one whose
// Retry-After has not passed to it: its status shows that time, and it is
// fetched then. A Retry-After shorter than the backoff brings it forward to
// the Retry-After.
func TestRefreshAllRespectsRetryAfter(t *testing.T) {
	for _, c := range []struct {
		name, seconds string
	}{{"longer than the backoff", "7200"}, {"shorter than the backoff", "60"}} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			r := e.refresher(testFetcher(), Config{})
			limited, other, limitedFS, otherFS := retryAfterFeeds(t, e, r, c.seconds)
			notBefore := e.row(limited).NotBefore
			e.clock.Advance(30 * time.Second)
			sts, err := r.Request(context.Background(), e.account, "")
			if err != nil {
				t.Fatal(err)
			}
			if st := statusOf(t, sts, limited); st.InProgress || st.NextAttempt != notBefore {
				t.Errorf("the limited podcast's status %+v, want next attempt %d", st, notBefore)
			}
			if st := statusOf(t, sts, other); !st.InProgress {
				t.Errorf("the other podcast's status %+v", st)
			}
			if row := e.row(limited); row.NextAt != notBefore {
				t.Errorf("the limited podcast's next attempt %d, want its Retry-After %d", row.NextAt, notBefore)
			}
			runDue(t, r)
			if rs := limitedFS.seen(); len(rs) != 0 {
				t.Fatalf("a refresh of every podcast fetched one before its Retry-After: %+v", rs)
			}
			if rs := otherFS.seen(); len(rs) != 1 {
				t.Fatalf("the other podcast: %+v", rs)
			}
			e.clock.Advance(time.Duration(notBefore-e.clock.ms()) * time.Millisecond)
			runDue(t, r)
			if rs := limitedFS.seen(); len(rs) != 1 {
				t.Fatalf("not fetched when its Retry-After passed: %+v", rs)
			}
		})
	}
}

// A Retry-After recorded between the request's read of its candidates and its
// write wins: the podcast is not queued.
func TestRefreshAllRaceWithNewRetryAfter(t *testing.T) {
	e := newEnv(t)
	fsA, fsB := newFeedServer(t), newFeedServer(t)
	for _, fs := range []*feedServer{fsA, fsB} {
		fs.set(item{"One", e.clock.ms() - day, true})
	}
	a, _ := e.subscribe(fsA.url("/a"), nil)
	b, _ := e.subscribe(fsB.url("/b"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fsA.seen()
	fsB.seen()
	e.clock.Advance(10 * time.Minute)
	notBefore := e.clock.ms() + 2*hour
	r.beforeRequestWrite = func() {
		// A fetch of a records a 429 meanwhile.
		row := e.row(a)
		row.Failures, row.NotBefore, row.NextAt = 1, notBefore, notBefore
		e.putRow(row)
	}
	sts, err := r.Request(context.Background(), e.account, "")
	if err != nil {
		t.Fatal(err)
	}
	if st := statusOf(t, sts, a); st.InProgress || st.NextAttempt != notBefore {
		t.Errorf("status %+v, want next attempt %d", st, notBefore)
	}
	if queued := e.row(a).RequestPending(); queued {
		t.Errorf("queued despite the Retry-After")
	}
	if row := e.row(a); row.NextAt != notBefore {
		t.Errorf("next attempt %d, want %d", row.NextAt, notBefore)
	}
	runDue(t, r)
	if rs := fsA.seen(); len(rs) != 0 {
		t.Fatalf("fetched before its Retry-After: %+v", rs)
	}
	if rs := fsB.seen(); len(rs) != 1 {
		t.Fatalf("the other podcast: %+v (%v)", rs, b)
	}
}

// A refresh of every podcast already queued when a Retry-After is recorded
// is not dispatched before it: the request is dropped and the podcast waits
// for its schedule.
func TestQueuedRefreshAllNotDispatchedBeforeNotBefore(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	fs.seen()
	e.clock.Advance(10 * time.Minute)
	if _, err := r.Request(context.Background(), e.account, ""); err != nil {
		t.Fatal(err)
	}
	// Before the next pass, a fetch that was running records a Retry-After.
	notBefore := e.clock.ms() + 2*hour
	row := e.row(p)
	row.Failures, row.NotBefore, row.NextAt = 1, notBefore, notBefore
	e.putRow(row)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 0 {
		t.Fatalf("dispatched before its Retry-After: %+v", rs)
	}
	if queued := e.row(p).RequestPending(); queued {
		t.Errorf("the request is still queued")
	}
	e.clock.Advance(2 * time.Hour)
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 1 {
		t.Fatalf("not fetched after its Retry-After: %+v", rs)
	}
}

// A single podcast's refresh is the override: fetched at once, without
// validators, whatever its Retry-After.
func TestSingleRefreshOverridesRetryAfter(t *testing.T) {
	e := newEnv(t)
	r := e.refresher(testFetcher(), Config{})
	limited, _, limitedFS, _ := retryAfterFeeds(t, e, r, "7200")
	e.clock.Advance(time.Minute)
	sts, err := r.Request(context.Background(), e.account, limited)
	if err != nil || len(sts) != 1 || !sts[0].InProgress {
		t.Fatalf("request: %+v %v", sts, err)
	}
	runDue(t, r)
	if rs := limitedFS.seen(); len(rs) != 1 || rs[0].ifNoneMatch != "" {
		t.Fatalf("a single podcast's refresh: %+v", rs)
	}
	if row := e.row(limited); row.Failures != 0 || row.LastSuccess != e.clock.ms() {
		t.Errorf("row %+v", row)
	}
}
