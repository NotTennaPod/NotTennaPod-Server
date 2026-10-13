package refresh

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The 124-byte feed that crashed the parser fails its refresh like any parser
// exception, recorded in the podcast's refresh status.
func TestTheCrashingFeedFailsItsRefresh(t *testing.T) {
	e := newEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<feed><x:outer xmlns:x="http://www.w3.org/2005/Atom"><atom:inner xmlns:atom="http://www.w3.org/2005/Atom"/></x:outer></feed>`))
	}))
	defer srv.Close()
	p, _ := e.subscribe(srv.URL, nil)
	r := e.refresher(testFetcher(), Config{})
	if err := r.refresh(context.Background(), key{e.account, p}, false); err != nil {
		t.Fatal(err)
	}
	if status := e.podcastRecord(p, "refresh_status"); !strings.Contains(status, `"label":"parser_exception"`) {
		t.Fatalf("refresh status %s", status)
	}
}

type panickingFetcher struct{}

func (panickingFetcher) Fetch(context.Context, feed.FetchRequest) (*feed.FetchResult, error) {
	panic("fetcher exploded")
}

// A panic in a refresh worker fails that
// refresh; the server goes on.
func TestAPanicFailsTheRefresh(t *testing.T) {
	e := newEnv(t)
	p, _ := e.subscribe("https://example.invalid/feed", nil)
	r := e.refresher(panickingFetcher{}, Config{})
	r.mu.Lock()
	r.startLocked(context.Background(), dueItem{k: key{e.account, p}})
	r.mu.Unlock()
	r.jobs.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.running) != 0 || r.finished[key{e.account, p}] == 0 {
		t.Fatalf("the panicked refresh did not finish: running %v", r.running)
	}
}

// bodyFetcher answers every fetch with body at once, whatever its context.
type bodyFetcher []byte

func (b bodyFetcher) Fetch(context.Context, feed.FetchRequest) (*feed.FetchResult, error) {
	return &feed.FetchResult{Body: b}, nil
}

// Parsing runs under the fetch's deadline; a feed
// still parsing when it passes fails with time_limit.
func TestParsingRunsUnderTheFetchDeadline(t *testing.T) {
	e := newEnv(t)
	p, _ := e.subscribe("https://example.invalid/feed", nil)
	var b strings.Builder
	b.WriteString(`<rss version="2.0"><channel><title>Show</title>`)
	for range 20000 {
		b.WriteString(`<item><title>Episode</title></item>`)
	}
	b.WriteString(`</channel></rss>`)
	r := e.refresher(bodyFetcher(b.String()), Config{FetchLimit: time.Nanosecond})
	if err := r.refresh(context.Background(), key{e.account, p}, false); err != nil {
		t.Fatal(err)
	}
	if status := e.podcastRecord(p, "refresh_status"); !strings.Contains(status, `"label":"time_limit"`) {
		t.Fatalf("a parse past the deadline: %s", status)
	}
}

// A date of many "word, " prefixes took DateUtils quadratic time (60 KB took
// 5 s, 1 MB would have taken half an hour). It takes linear time now, but a
// long one still takes long: 1.5 MB, 2.5 s. Its parse stops at the fetch's
// deadline and fails with time_limit; before, the refresh went on past it,
// keeping its worker, and then succeeded.
func TestAHostileDateStopsAtTheFetchDeadline(t *testing.T) {
	e := newEnv(t)
	p, _ := e.subscribe("https://example.invalid/feed", nil)
	body := `<rss version="2.0"><channel><title>Show</title><item><title>Episode</title><pubDate>` +
		strings.Repeat("a, ", 1<<19) + `Sat, 09 Oct 2021 10:00:00 +0000</pubDate></item></channel></rss>`
	r := e.refresher(bodyFetcher(body), Config{FetchLimit: 100 * time.Millisecond})
	if err := r.refresh(context.Background(), key{e.account, p}, false); err != nil {
		t.Fatal(err)
	}
	if status := e.podcastRecord(p, "refresh_status"); !strings.Contains(status, `"label":"time_limit"`) {
		t.Fatalf("a date past the deadline: %s", status)
	}
}

// A long date that takes no weekday retry (a numeric pubDate of 4 MiB) ran
// past the deadline and its parse was a success: a refresh recorded it, a
// moved feed verified, a re-parse was marked done. Now the refresh fails with
// time_limit, the move does not verify, and the re-parse stays pending.
func TestALongDateStopsAtTheFetchDeadline(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 2*day, true})
	stored200, _ := e.subscribe(fs.url("/feed"), nil)
	runDue(t, e.refresher(testFetcher(), Config{}))
	p, _ := e.subscribe("https://example.invalid/feed", nil)
	long := `<rss version="2.0"><channel><title>Show</title><item><title>Episode</title><pubDate>` +
		strings.Repeat("1", 4<<20) + `</pubDate></item></channel></rss>`
	r := e.refresher(bodyFetcher(long), Config{FetchLimit: 100 * time.Millisecond})
	if err := r.refresh(context.Background(), key{e.account, p}, false); err != nil {
		t.Fatal(err)
	}
	if status := e.podcastRecord(p, "refresh_status"); !strings.Contains(status, `"label":"time_limit"`) {
		t.Errorf("a long date past the deadline: %s", status)
	}
	if r.verified(context.Background(), key{e.account, p}, "https://example.invalid/moved", nil) {
		t.Error("a moved feed verified past the deadline")
	}
	p = stored200
	setStored(t, e, p, func(s *feed.StoredResponse) { s.ParserVersion, s.Body = "ntp-go/0", []byte(long) })
	r.ReparseAll(context.Background())
	var o storage.ReparseOutcome
	if err := e.svc.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
		var err error
		o, err = tx.Account(e.account).ReparseOutcome(p)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if s := stored(t, e, p); s.ParserVersion != "ntp-go/0" || o != (storage.ReparseOutcome{Timeouts: 1, Parser: feed.ParserVersion}) {
		t.Errorf("a re-parse past the deadline: %q %+v", s.ParserVersion, o)
	}
}

// A re-parse cut short by its deadline is not done: the stored response keeps
// its parser version and is tried again at the next pass (before, it was
// marked re-parsed and never tried again). After three timeouts for one
// parser version it counts as unparseable, so a hostile one cannot take every
// start's whole deadline. The outcome is kept in the clear beside the
// response, written with it, for diagnostics; a new response clears it. A
// response that does not parse is given up at once, as before.
func TestReparseAfterATimeout(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 2*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	runDue(t, e.refresher(testFetcher(), Config{}))
	outcome := func() storage.ReparseOutcome {
		t.Helper()
		var o storage.ReparseOutcome
		if err := e.svc.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
			var err error
			o, err = tx.Account(e.account).ReparseOutcome(p)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return o
	}
	setOutcome := func(o storage.ReparseOutcome) {
		t.Helper()
		if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
			b, err := tx.Account(e.account).FeedResponse(p)
			if err != nil {
				return err
			}
			return tx.Account(e.account).PutReparsedFeedResponse(p, b, o)
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A date of 1.5 MB that takes 2.5 s to parse.
	hostile := `<rss version="2.0"><channel><title>Show</title><item><title>Episode</title><pubDate>` +
		strings.Repeat("a, ", 1<<19) + `Sat, 09 Oct 2021 10:00:00 +0000</pubDate></item></channel></rss>`
	setStored(t, e, p, func(s *feed.StoredResponse) { s.ParserVersion, s.Body = "ntp-go/0", []byte(hostile) })
	var log bytes.Buffer
	r := e.refresher(testFetcher(), Config{FetchLimit: 100 * time.Millisecond})
	r.log = slog.New(slog.NewTextHandler(&log, nil))
	for pass := 1; pass <= 3; pass++ {
		r.ReparseAll(context.Background())
		s, o := stored(t, e, p), outcome()
		switch {
		case pass < 3 && (s.ParserVersion != "ntp-go/0" || o != storage.ReparseOutcome{Timeouts: pass, Parser: feed.ParserVersion}):
			t.Fatalf("after timeout %d: %q, %+v", pass, s.ParserVersion, o)
		case pass == 3 && (s.ParserVersion != feed.ParserVersion || o != storage.ReparseOutcome{Failure: "timeout", Timeouts: 3, Parser: feed.ParserVersion}):
			t.Fatalf("after timeout 3: %q, %+v", s.ParserVersion, o)
		}
	}
	if !strings.Contains(log.String(), "cannot be re-parsed in time") || !strings.Contains(log.String(), "deadline 3 times") {
		t.Errorf("log: %s", log.String())
	}
	if got := e.episodeRecord(p, "One", "metadata"); !strings.Contains(got, `"title":"One"`) {
		t.Errorf("the catalog after the timeouts: %s", got)
	}
	r.ReparseAll(context.Background())
	if s, o := stored(t, e, p), outcome(); s.ParserVersion != feed.ParserVersion || o.Failure != "timeout" {
		t.Errorf("a response given up on was re-parsed again: %q %+v", s.ParserVersion, o)
	}
	// A new response has no outcome.
	if err := e.refresher(testFetcher(), Config{}).refresh(context.Background(), key{e.account, p}, true); err != nil {
		t.Fatal(err)
	}
	if s, o := stored(t, e, p), outcome(); s.ParserVersion != feed.ParserVersion || o != (storage.ReparseOutcome{}) {
		t.Errorf("after a new fetch: %q %+v", s.ParserVersion, o)
	}
	// Timeouts for an older parser version do not count for this one.
	setStored(t, e, p, func(s *feed.StoredResponse) { s.ParserVersion, s.Body = "ntp-go/0", []byte(hostile) })
	setOutcome(storage.ReparseOutcome{Timeouts: 2, Parser: "ntp-go/0"})
	r.ReparseAll(context.Background())
	if s, o := stored(t, e, p), outcome(); s.ParserVersion != "ntp-go/0" || o != (storage.ReparseOutcome{Timeouts: 1, Parser: feed.ParserVersion}) {
		t.Errorf("after older timeouts: %q, %+v", s.ParserVersion, o)
	}
	// A response that does not parse is given up at once, as before.
	setStored(t, e, p, func(s *feed.StoredResponse) {
		s.ParserVersion, s.Body = "ntp-go/0", []byte(`<rss version="2.0"><channel>`)
	})
	setOutcome(storage.ReparseOutcome{Timeouts: 2, Parser: feed.ParserVersion})
	r.ReparseAll(context.Background())
	if s, o := stored(t, e, p), outcome(); s.ParserVersion != feed.ParserVersion || o != (storage.ReparseOutcome{}) {
		t.Errorf("an unparseable response: %q %+v", s.ParserVersion, o)
	}
}

// A failure's detail keeps no URL userinfo (the feed
// fields' rule) in the replicated refresh_status.
func TestFailureDetailsKeepNoCredentials(t *testing.T) {
	e := newEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html><head><title>Login failed https://alice:PRIVATE_PASSWORD@private.invalid/feed</title></head></html>`))
	}))
	defer srv.Close()
	p, _ := e.subscribe(srv.URL, nil)
	r := e.refresher(testFetcher(), Config{})
	if err := r.refresh(context.Background(), key{e.account, p}, false); err != nil {
		t.Fatal(err)
	}
	status := e.podcastRecord(p, "refresh_status")
	if strings.Contains(status, "PRIVATE_PASSWORD") || strings.Contains(status, "alice") ||
		!strings.Contains(status, `Login failed https://private.invalid/feed`) {
		t.Fatalf("refresh status %s", status)
	}
}

// credentialPanicFetcher panics with a message quoting a URL with
// credentials.
type credentialPanicFetcher struct{}

func (credentialPanicFetcher) Fetch(context.Context, feed.FetchRequest) (*feed.FetchResult, error) {
	panic("bad https://alice:secret_pw@example.org/feed")
}

// A panic in a refresh is a failed refresh like any other: its status is
// parser_exception, the podcast backs off (the next pass does not start it
// again), and the log has a fixed message and the stack, never the panic's
// text.
func TestAPanicIsAFailedRefresh(t *testing.T) {
	e := newEnv(t)
	p, _ := e.subscribe("https://example.org/feed", nil)
	r := e.refresher(credentialPanicFetcher{}, Config{})
	var log bytes.Buffer
	r.log = slog.New(slog.NewTextHandler(&log, nil))
	r.mu.Lock()
	r.startLocked(context.Background(), dueItem{k: key{e.account, p}})
	r.mu.Unlock()
	r.jobs.Wait()
	if status := e.podcastRecord(p, "refresh_status"); !strings.Contains(status, `"label":"parser_exception"`) {
		t.Errorf("refresh status %q", status)
	}
	if strings.Contains(log.String(), "secret_pw") || !strings.Contains(log.String(), "a panic failed the refresh") {
		t.Errorf("log: %s", log.String())
	}
	res, err := r.pass(context.Background(), false, false)
	r.jobs.Wait()
	if err != nil || res.started != 0 {
		t.Errorf("the next pass started the failed refresh again: %+v %v", res, err)
	}
}

// newFeedPanicFetcher answers the feed, which names a new feed URL, and
// panics when that URL is verified.
type newFeedPanicFetcher struct{}

func (newFeedPanicFetcher) Fetch(_ context.Context, req feed.FetchRequest) (*feed.FetchResult, error) {
	if strings.Contains(req.URL, "new.example.org") {
		panic("bad https://alice:secret_pw@new.example.org/feed")
	}
	body := `<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" version="2.0"><channel><title>Show</title>` +
		`<itunes:new-feed-url>https://new.example.org/feed</itunes:new-feed-url></channel></rss>`
	return &feed.FetchResult{Body: []byte(body), FinalURL: req.URL}, nil
}

// A panic verifying a new feed URL is no answer: the refresh succeeds
// without the move.
func TestAPanicVerifyingANewFeedURLIsNoMove(t *testing.T) {
	e := newEnv(t)
	p, _ := e.subscribe("https://example.org/feed", nil)
	r := e.refresher(newFeedPanicFetcher{}, Config{})
	var log bytes.Buffer
	r.log = slog.New(slog.NewTextHandler(&log, nil))
	if err := r.refresh(context.Background(), key{e.account, p}, false); err != nil {
		t.Fatal(err)
	}
	if status := e.podcastRecord(p, "refresh_status"); strings.Contains(status, `"label"`) {
		t.Errorf("refresh status %s", status)
	}
	if id := e.podcastRecord(p, "identity"); strings.Contains(id, "new.example.org") {
		t.Errorf("an unverified new feed URL moved the podcast: %s", id)
	}
	if strings.Contains(log.String(), "secret_pw") {
		t.Errorf("log: %s", log.String())
	}
}

// panicOnFirstDecision panics at the server's first decision ID.
type panicOnFirstDecision struct{ n int }

func (r *panicOnFirstDecision) DecisionID(string) string {
	if r.n++; r.n == 1 {
		panic("bad https://alice:secret_pw@example.org/feed")
	}
	return newID()
}
func (*panicOnFirstDecision) PositionID() [16]byte { return [16]byte{1} }
func (*panicOnFirstDecision) Index(int) int        { return 0 }

// A panic at any stage of a refresh (reading its context, fetching, parsing,
// applying its fetch, inside its commit, or preparing it) is a failed refresh:
// its status parser_exception, the normal backoff, and nothing of the panic's
// text in the log.
func TestAPanicAtAnyStageIsAFailedRefresh(t *testing.T) {
	for _, stage := range []string{"capture", "fetch", "parse", "apply commit", "inside the commit", "apply preparation"} {
		t.Run(stage, func(t *testing.T) {
			e := newEnv(t)
			p, _ := e.subscribe("https://example.org/feed", nil)
			r := e.refresher(bodyFetcher(`<rss version="2.0"><channel><title>Show</title></channel></rss>`), Config{})
			var log bytes.Buffer
			r.log = slog.New(slog.NewTextHandler(&log, nil))
			calls := 0
			panicAt := func(n int) {
				if calls++; calls == n {
					panic("bad https://alice:secret_pw@example.org/feed")
				}
			}
			switch stage {
			case "capture", "apply commit":
				at := map[string]int{"capture": 1, "apply commit": 2}[stage]
				e.svc.Now = func() time.Time { panicAt(at); return e.clock.Now() }
			case "fetch":
				r.cfg.Fetcher = credentialPanicFetcher{}
			case "parse":
				r.cfg.Fetcher = bodyFetcher(`<feed><x:outer xmlns:x="http://www.w3.org/2005/Atom"><atom:inner xmlns:atom="http://www.w3.org/2005/Atom"/></x:outer></feed>`)
			case "inside the commit":
				e.svc.Random = &panicOnFirstDecision{}
			case "apply preparation":
				r.cfg.Random = func() float64 { panicAt(1); return .5 }
			}
			r.mu.Lock()
			r.startLocked(context.Background(), dueItem{k: key{e.account, p}})
			r.mu.Unlock()
			r.jobs.Wait()
			if status := e.podcastRecord(p, "refresh_status"); !strings.Contains(status, `"label":"parser_exception"`) {
				t.Errorf("refresh status %q", status)
			}
			var row storage.RefreshRow
			if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
				var err error
				row, _, err = tx.Account(e.account).RefreshSchedule(p)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if row.Failures != 1 || row.NextAt <= e.clock.ms() || row.LastAttempt == 0 || !strings.Contains(row.LastError, "parser_exception") {
				t.Errorf("schedule %+v at %d", row, e.clock.ms())
			}
			if strings.Contains(log.String(), "secret_pw") {
				t.Errorf("log: %s", log.String())
			}
			res, err := r.pass(context.Background(), false, false)
			r.jobs.Wait()
			if err != nil || res.started != 0 {
				t.Errorf("the next pass started the failed refresh again: %+v %v", res, err)
			}
		})
	}
}

// When no failed refresh can be recorded (here every server commit
// panics), the podcast still backs off from its schedule, once, and the
// worker ends.
func TestAPanicThatKeepsPanickingStillBacksOff(t *testing.T) {
	e := newEnv(t)
	p, _ := e.subscribe("https://example.org/feed", nil)
	r := e.refresher(bodyFetcher(`<rss version="2.0"><channel><title>Show</title></channel></rss>`), Config{})
	var log bytes.Buffer
	r.log = slog.New(slog.NewTextHandler(&log, nil))
	e.svc.Now = func() time.Time { panic("bad https://alice:secret_pw@example.org/feed") }
	r.mu.Lock()
	r.startLocked(context.Background(), dueItem{k: key{e.account, p}})
	r.mu.Unlock()
	r.jobs.Wait()
	var row storage.RefreshRow
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		var err error
		row, _, err = tx.Account(e.account).RefreshSchedule(p)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if row.Failures != 1 || row.NextAt <= e.clock.ms() {
		t.Errorf("schedule %+v at %d", row, e.clock.ms())
	}
	if strings.Contains(log.String(), "secret_pw") || !strings.Contains(log.String(), "a panic stopped recording a failed refresh") {
		t.Errorf("log: %s", log.String())
	}
}
