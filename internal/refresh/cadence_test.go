package refresh

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// PROTOCOL §9.6: a podcast whose catalog the server's fetches have found
// unchanged for 30 days is fetched every 6 hours, after 90 days every 24
// hours; anything new or changed makes it hourly again at once.

// alwaysFull serves the feed with a new ETag on every response, so every
// fetch is a 200 with the whole body and is parsed and applied.
func alwaysFull(t *testing.T, e *env, items ...item) *feedServer {
	fs := newFeedServer(t)
	fs.set(items...)
	fs.with(func(fs *feedServer) { fs.varyETag = true })
	return fs
}

// quietFor makes a fetched podcast's catalog quiet since d ago, as a stopped
// server would have left it.
func (e *env) quietFor(podcast string, d time.Duration) int64 {
	e.t.Helper()
	row := e.row(podcast)
	row.QuietSince = e.clock.ms() - d.Milliseconds()
	e.putRow(row)
	return row.QuietSince
}

// fetchNext advances the clock to the podcast's next attempt and runs it.
func (e *env) fetchNext(t *testing.T, r *Refresher, podcast string) {
	t.Helper()
	if d := e.row(podcast).NextAt - e.clock.ms(); d > 0 {
		e.clock.Advance(time.Duration(d) * time.Millisecond)
	}
	runDue(t, r)
}

// expectNext checks a podcast's quiet time and that its next attempt is one
// interval from now.
func (e *env) expectNext(t *testing.T, podcast string, quietSince int64, interval time.Duration) {
	t.Helper()
	row := e.row(podcast)
	if row.QuietSince != quietSince || row.NextAt != e.clock.ms()+interval.Milliseconds() {
		t.Fatalf("quiet since %d (want %d), next in %v (want %v)", row.QuietSince, quietSince,
			time.Duration(row.NextAt-e.clock.ms())*time.Millisecond, interval)
	}
}

const months = 30 * 24 * time.Hour

func TestQuietSinceStartsAtFirstSuccess(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - day, true})
	fs.with(func(fs *feedServer) { fs.status = 500 })
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	if row := e.row(p); row.QuietSince != 0 {
		t.Fatalf("a failure started the quiet time: %+v", row)
	}
	fs.with(func(fs *feedServer) { fs.status = 0 })
	e.fetchNext(t, r, p)
	first := e.clock.ms()
	e.expectNext(t, p, first, time.Hour)
	// Unchanged (a 304): the quiet time stays.
	e.fetchNext(t, r, p)
	e.expectNext(t, p, first, time.Hour)
	// A podcast without one (fetched before the server kept it) starts at
	// its next success, a 304 included.
	row := e.row(p)
	row.QuietSince = 0
	e.putRow(row)
	e.fetchNext(t, r, p)
	e.expectNext(t, p, e.clock.ms(), time.Hour)
}

// The interval by quiet time and --refresh-interval; jitter stays within ±10 %.
func TestDormantIntervals(t *testing.T) {
	e := newEnv(t)
	now := e.clock.ms()
	for _, c := range []struct {
		interval time.Duration
		want     [4]time.Duration // unknown, 10, 40 and 100 days quiet
	}{
		{time.Hour, [4]time.Duration{time.Hour, time.Hour, 6 * time.Hour, 24 * time.Hour}},
		{8 * time.Hour, [4]time.Duration{8 * time.Hour, 8 * time.Hour, 8 * time.Hour, 24 * time.Hour}},
		{48 * time.Hour, [4]time.Duration{48 * time.Hour, 48 * time.Hour, 48 * time.Hour, 48 * time.Hour}},
	} {
		r := e.refresher(nil, Config{Interval: c.interval})
		for i, quiet := range []int64{0, now - 10*day, now - 40*day, now - 100*day} {
			if got := r.interval(quiet, now); got != c.want[i] {
				t.Errorf("interval %v, quiet since %d: %v, want %v", c.interval, quiet, got, c.want[i])
			}
		}
		// The thresholds: 30 and 90 days exactly.
		if got := r.interval(now-30*day, now); got != max(c.interval, 6*time.Hour) {
			t.Errorf("interval %v at 30 days: %v", c.interval, got)
		}
		if got := r.interval(now-30*day+1, now); got != c.interval {
			t.Errorf("interval %v just under 30 days: %v", c.interval, got)
		}
		if got := r.interval(now-90*day, now); got != max(c.interval, 24*time.Hour) {
			t.Errorf("interval %v at 90 days: %v", c.interval, got)
		}
	}
	// Applied by a fetch, with jitter at both ends.
	for _, c := range []struct {
		random   float64
		low, top time.Duration
	}{{0, 6 * time.Hour * 9 / 10, 6 * time.Hour * 9 / 10}, {0.999999, 6*time.Hour*11/10 - time.Second, 6 * time.Hour * 11 / 10}} {
		e := newEnv(t)
		fs := alwaysFull(t, e, item{"One", e.clock.ms() - day, true})
		p, _ := e.subscribe(fs.url("/feed"), nil)
		r := e.refresher(testFetcher(), Config{Random: fixedRandom(c.random)})
		runDue(t, r)
		quiet := e.quietFor(p, 40*24*time.Hour)
		e.fetchNext(t, r, p)
		row := e.row(p)
		if next := time.Duration(row.NextAt-e.clock.ms()) * time.Millisecond; next < c.low || next > c.top || row.QuietSince != quiet {
			t.Errorf("random %v: next in %v, want within [%v, %v]; quiet since %d, want %d", c.random, next, c.low, c.top, row.QuietSince, quiet)
		}
	}
}

// A new item makes a quiet podcast hourly again at once.
func TestNewItemRevivesAtOnce(t *testing.T) {
	e := newEnv(t)
	fs := alwaysFull(t, e, item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	quiet := e.quietFor(p, 100*24*time.Hour)
	e.fetchNext(t, r, p)
	e.expectNext(t, p, quiet, 24*time.Hour)
	fs.with(func(fs *feedServer) { fs.items = append(fs.items, item{"Two", e.clock.ms(), true}) })
	e.fetchNext(t, r, p)
	e.expectNext(t, p, e.clock.ms(), time.Hour)
	if got := e.episodeRecord(p, "Two", "admission"); !strings.Contains(got, `"source":"server"`) {
		t.Errorf("the release was not admitted: %s", got)
	}
}

// Back-catalog additions and corrections are activity, whatever their date.
func TestOldDatedNewItemIsActivity(t *testing.T) {
	e := newEnv(t)
	fs := alwaysFull(t, e, item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	e.quietFor(p, 100*24*time.Hour)
	fs.with(func(fs *feedServer) { fs.items = append(fs.items, item{"Archive", e.clock.ms() - 5*365*day, true}) })
	e.fetchNext(t, r, p)
	e.expectNext(t, p, e.clock.ms(), time.Hour)
	if got := e.episodeRecord(p, "Archive", "admission"); got != "" {
		t.Errorf("a back-catalog item was filed: %s", got)
	}
}

func TestChangedItemIsActivity(t *testing.T) {
	e := newEnv(t)
	fs := alwaysFull(t, e, item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	e.quietFor(p, 40*24*time.Hour)
	fs.with(func(fs *feedServer) { fs.items[0].title = "One (corrected)" })
	e.fetchNext(t, r, p)
	e.expectNext(t, p, e.clock.ms(), time.Hour)
	if got := e.episodeRecord(p, "One", "metadata"); !strings.Contains(got, "One (corrected)") {
		t.Errorf("the correction was not applied: %s", got)
	}
}

// A change to the podcast's own metadata alone (channel churn) is applied but
// is not activity.
func TestPodcastMetadataOnlyChangeIsNotActivity(t *testing.T) {
	e := newEnv(t)
	fs := alwaysFull(t, e, item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	quiet := e.quietFor(p, 100*24*time.Hour)
	fs.with(func(fs *feedServer) { fs.extra = `<description>Updated every build</description>` })
	e.fetchNext(t, r, p)
	e.expectNext(t, p, quiet, 24*time.Hour)
	if got := e.podcastRecord(p, "metadata"); !strings.Contains(got, "Updated every build") {
		t.Errorf("the podcast's metadata was not applied: %s", got)
	}
}

// An item a phone observed before the server's fetch is new to the server's
// fetch history: activity, though its record does not change.
func TestItemFirstSeenByPhoneIsActivity(t *testing.T) {
	e := newEnv(t)
	fs := alwaysFull(t, e, item{"One", e.clock.ms() - 400*day, true})
	p, g := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	quiet := e.quietFor(p, 100*24*time.Hour)
	// The publisher adds Two, an old episode (so the server does not admit
	// it: only its fetch history tells it is new). The server's parse of it,
	// as a phone that reached the publisher first sends it: the same feed
	// under another subscription gives the record's fields.
	fs.with(func(fs *feedServer) { fs.items = append(fs.items, item{"Two", e.clock.ms() - 500*day, true}) })
	twin, _ := e.subscribe(fs.url("/twin"), nil)
	runDue(t, r)
	if e.row(p).QuietSince != quiet {
		t.Fatalf("the twin's fetch fetched the podcast")
	}
	meta := decode(t, e.episodeRecord(twin, "Two", "metadata"))
	fields := map[string]any{}
	for k, v := range meta {
		if k != "revision" && k != "fetch_time" {
			fields[k] = v
		}
	}
	ep := e.episode(p, "Two")
	e.clock.Advance(time.Minute)
	e.push(e.fact("catalog.observe", []any{map[string]any{"entity": "episode", "id": ep}, map[string]any{"entity": "podcast", "id": p}},
		map[string]any{"podcast_id": p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": e.clock.ms(),
			"items": []any{map[string]any{"fields": fields,
				"episode": map[string]any{"episode_id": ep, "podcast_id": p, "generation_id": g,
					"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": "guid-Two", "guid": "guid-Two", "title": "Two"}}}}}))
	observed := e.episodeRecord(p, "Two", "metadata")
	if observed == "" {
		t.Fatal("the phone's observation was not applied")
	}
	e.fetchNext(t, r, p)
	if got := e.episodeRecord(p, "Two", "metadata"); got != observed {
		t.Fatalf("the server's fetch changed the phone's record (so this is not the case under test):\n%s\n%s", observed, got)
	}
	e.expectNext(t, p, e.clock.ms(), time.Hour)
	if got := e.episodeRecord(p, "Two", "admission"); got != "" {
		t.Errorf("the old episode was admitted (so this is not the case under test): %s", got)
	}
}

// A 304, a failure and a re-parse keep the quiet time.
func TestNotModifiedFailureAndReparseKeepQuietSince(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	t0 := e.clock.ms()
	fs.set(item{"One", t0 - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	quiet := e.quietFor(p, 40*24*time.Hour)
	e.fetchNext(t, r, p)
	if got := fs.seen(); len(got) != 2 || got[1].ifNoneMatch == "" {
		t.Fatalf("requests %+v", got)
	}
	e.expectNext(t, p, quiet, 6*time.Hour)
	fs.with(func(fs *feedServer) { fs.status = 500 })
	e.fetchNext(t, r, p)
	if row := e.row(p); row.QuietSince != quiet || row.Failures != 1 {
		t.Fatalf("after a failure: %+v", row)
	}
	fs.with(func(fs *feedServer) { fs.status = 0 })
	e.fetchNext(t, r, p) // unconditional after a failure: a 200, unchanged
	e.expectNext(t, p, quiet, 6*time.Hour)
	// A re-parse that changes records is not a fetch.
	before := e.row(p)
	fs.set(item{"One (fixed)", t0 - 400*day, true}, item{"Two", t0 - minute, true})
	setStored(t, e, p, func(s *feed.StoredResponse) {
		s.ParserVersion = "ntp-go/0"
		fs.with(func(fs *feedServer) { s.Body = fs.body() })
	})
	e.refresher(testFetcher(), Config{}).ReparseAll(context.Background())
	if got := e.episodeRecord(p, "Two", "metadata"); got == "" {
		t.Fatal("the re-parse applied nothing")
	}
	if row := e.row(p); row != before {
		t.Fatalf("a re-parse changed the schedule: %+v, was %+v", row, before)
	}
}

// Empty and undated feeds become quiet like any other.
func TestEmptyAndUndatedFeedsBecomeDormant(t *testing.T) {
	for _, c := range []struct {
		name  string
		items []item
	}{{"empty", nil}, {"undated", []item{{"One", 0, true}, {"Two", 0, false}}}} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			fs := alwaysFull(t, e, c.items...)
			p, _ := e.subscribe(fs.url("/feed"), nil)
			r := e.refresher(testFetcher(), Config{})
			runDue(t, r)
			first := e.clock.ms()
			e.expectNext(t, p, first, time.Hour)
			for range 3 {
				e.fetchNext(t, r, p)
				e.expectNext(t, p, first, time.Hour)
			}
			if n := len(fs.seen()); n != 4 {
				t.Fatalf("%d fetches, want 4 full ones", n)
			}
			quiet := e.quietFor(p, 31*24*time.Hour)
			e.fetchNext(t, r, p)
			e.expectNext(t, p, quiet, 6*time.Hour)
		})
	}
}

// Another URL, other credentials or a new subscription of the podcast starts
// its observation again.
func TestContextChangeRestartsObservation(t *testing.T) {
	for _, change := range []string{"url", "credentials", "resubscribe"} {
		t.Run(change, func(t *testing.T) {
			e := newEnv(t)
			fs := newFeedServer(t)
			fs.set(item{"One", e.clock.ms() - 400*day, true})
			p, g := e.subscribe(fs.url("/feed"), nil)
			r := e.refresher(testFetcher(), Config{})
			runDue(t, r)
			e.quietFor(p, 100*24*time.Hour)
			e.fetchNext(t, r, p)
			e.clock.Advance(time.Minute)
			switch change {
			case "url":
				e.push(e.fact("podcast.change_url", podTarget(p, g), map[string]any{"url": fs.url("/feed?edition=2")}))
			case "credentials":
				secret := protocol.Secret{ID: newID(), Username: "u", Password: "p"}
				e.push(e.fact("podcast.set_credentials", podTarget(p, g), map[string]any{"secret_id": secret.ID}), secret)
			case "resubscribe":
				e.push(e.fact("podcast.unsubscribe", podTarget(p, g), map[string]any{}))
				e.push(e.fact("podcast.subscribe", []any{}, map[string]any{"feed_url": fs.url("/feed"),
					"observed": map[string]any{"podcast_id": p, "generation_id": g, "state": "removed"}}))
			}
			// The scheduler sees the change and fetches now: the new
			// context's first success starts its quiet time.
			res, err := r.pass(context.Background(), false, false)
			if err != nil {
				t.Fatal(err)
			}
			if res.started != 1 {
				t.Fatalf("the change was not fetched at once: %+v", res)
			}
			runDue(t, r)
			e.expectNext(t, p, e.clock.ms(), time.Hour)
		})
	}
}

// A podcast the scheduler meets without a quiet time (imported, attached
// with an old library, or fetched before the server kept one) is observed
// from its first success: hourly for its first 30 days, whatever the age of
// its episodes.
func TestImportedPodcastObservationPeriod(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 3*365*day, true}, item{"Two", e.clock.ms() - 2*365*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	first := e.clock.ms()
	e.expectNext(t, p, first, time.Hour)
	// The server was down for 29 days.
	e.clock.Advance(29 * 24 * time.Hour)
	runDue(t, r)
	e.expectNext(t, p, first, time.Hour)
	e.clock.Advance(24*time.Hour + time.Hour)
	runDue(t, r)
	e.expectNext(t, p, first, 6*time.Hour)
}

// A request fetches a quiet podcast at once, a single one or every one.
func TestManualAndRefreshAllFetchDormantAtOnce(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	quiet := e.quietFor(p, 100*24*time.Hour)
	e.fetchNext(t, r, p)
	fs.seen()
	for _, podcast := range []string{p, ""} {
		e.clock.Advance(time.Hour)
		if _, err := r.Request(context.Background(), e.account, podcast); err != nil {
			t.Fatal(err)
		}
		runDue(t, r)
		if got := fs.seen(); len(got) != 1 {
			t.Fatalf("request %q: %d fetches", podcast, len(got))
		}
		e.expectNext(t, p, quiet, 24*time.Hour)
	}
}

// After a clock jump, each podcast's deadline is checked against its own
// interval: a quiet podcast's 24-hour deadline stays, an active podcast's
// is brought back, and a quiet one's beyond its interval too.
func TestReconcileUsesPodcastInterval(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 400*day, true})
	quietP, _ := e.subscribe(fs.url("/quiet"), nil)
	activeP, _ := e.subscribe(fs.url("/active"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	now := e.clock.ms()
	set := func(p string, quiet, next int64) {
		row := e.row(p)
		row.QuietSince, row.NextAt = quiet, next
		e.putRow(row)
	}
	set(quietP, now-100*day, now+20*hour)
	set(activeP, now-day, now+20*hour)
	if _, err := r.pass(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if row := e.row(quietP); row.NextAt != now+20*hour {
		t.Errorf("a quiet podcast's deadline was brought back: %v", time.Duration(row.NextAt-now)*time.Millisecond)
	}
	if row := e.row(activeP); row.NextAt != now+hour {
		t.Errorf("an active podcast's deadline: %v", time.Duration(row.NextAt-now)*time.Millisecond)
	}
	set(quietP, now-100*day, now+7*day)
	if _, err := r.pass(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if row := e.row(quietP); row.NextAt != now+day {
		t.Errorf("a quiet podcast's deadline a week ahead: %v", time.Duration(row.NextAt-now)*time.Millisecond)
	}
}

// A quiet time in the future (the clock went back) counts as active, and the
// next success starts it again.
func TestClockBackwardsCountsAsActive(t *testing.T) {
	e := newEnv(t)
	fs := alwaysFull(t, e, item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	row := e.row(p)
	row.QuietSince = e.clock.ms() + 200*day // written while the clock ran far ahead
	e.putRow(row)
	if got := r.interval(row.QuietSince, e.clock.ms()); got != time.Hour {
		t.Errorf("a future quiet time: %v", got)
	}
	e.fetchNext(t, r, p)
	e.expectNext(t, p, e.clock.ms(), time.Hour)
}

// The quiet time is kept across a restart and a rebuild of the account,
// in place or staged.
func TestQuietSinceSurvivesRestartAndRebuild(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	runDue(t, e.refresher(testFetcher(), Config{}))
	quiet := e.quietFor(p, 100*24*time.Hour)
	restarted := e.refresher(testFetcher(), Config{})
	e.fetchNext(t, restarted, p)
	e.expectNext(t, p, quiet, 24*time.Hour)
	want := e.row(p)
	for _, threshold := range []int{-1, 1} {
		e.svc.StagingThreshold = threshold
		if _, err := e.svc.Rebuild(context.Background(), e.account); err != nil {
			t.Fatal(err)
		}
		if row := e.row(p); row != want {
			t.Fatalf("staging threshold %d: the rebuild changed the schedule: %+v, was %+v", threshold, row, want)
		}
	}
	e.fetchNext(t, e.refresher(testFetcher(), Config{}), p)
	e.expectNext(t, p, quiet, 24*time.Hour)
}

// heldPush pushes facts in one push whose write transaction is held open
// once it has begun: held receives when the push holds the writer, closing
// release lets it go on, and done reports its outcome (every fact applied).
func (e *env) heldPush(facts []map[string]any, secrets ...protocol.Secret) (held <-chan struct{}, release chan<- struct{}, done <-chan error) {
	e.t.Helper()
	acked := e.seq
	var ops []protocol.PushOp
	for _, f := range facts {
		a, err := protocol.ParseFact(f)
		if err != nil {
			e.t.Fatal(err)
		}
		e.seq++
		ops = append(ops, protocol.PushOp{InstallSeq: e.seq, Fact: a})
	}
	var h contract.Cursor
	if err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		hd, err := r.Account(e.account).Head()
		h = contract.Cursor{LogID: hd.LogID, Epoch: hd.Epoch, Seq: hd.Seq, Digest: hd.Digest}
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	inTx, rel, out := make(chan struct{}, 1), make(chan struct{}), make(chan error, 1)
	svc, account, inst := e.svc, e.account, e.inst
	go func() {
		resp, err := svc.Push(context.Background(), protocol.PushRequest{AccountID: account, Cursor: h, InstallID: inst.InstallID,
			AckedInstallSeq: acked, Operations: ops, Secrets: secrets},
			func(tx *storage.WriteTx) (*storage.Installation, error) {
				select {
				case inTx <- struct{}{}:
				default:
				}
				<-rel
				i, err := tx.Account(account).Installation(inst.InstallID)
				return &i, err
			})
		if err == nil {
			for i, res := range resp.Results {
				if o, _ := res.Receipt["outcome"].(string); o != "applied" {
					err = fmt.Errorf("fact %d: %+v", i, res)
				}
			}
		}
		out <- err
	}()
	return inTx, rel, out
}

// waitFetches waits until no fetch runs.
func waitFetches(r *Refresher) {
	for {
		r.mu.Lock()
		n := len(r.running)
		r.mu.Unlock()
		if n == 0 {
			return
		}
		<-r.wake
	}
}

// An edit of the feed's context (its URL, credentials or generation) that
// commits while a fetch is being dispatched, after the scheduler read the old
// context and before the fetch captured the new one, starts a new
// observation: the scheduler never saw the change, so the fetch resets the
// quiet time itself. A failed first fetch keeps the reset pending.
func TestContextEditRacingDispatchRestartsObservation(t *testing.T) {
	for _, change := range []string{"url", "credentials", "generation", "url, failing first"} {
		t.Run(change, func(t *testing.T) {
			e := newEnv(t)
			fs, moved := newFeedServer(t), newFeedServer(t)
			for _, s := range []*feedServer{fs, moved} {
				s.set(item{"One", e.clock.ms() - 400*day, true})
			}
			p, g := e.subscribe(fs.url("/feed"), nil)
			r := e.refresher(testFetcher(), Config{})
			runDue(t, r)
			quiet := e.quietFor(p, 100*24*time.Hour)
			e.fetchNext(t, r, p)
			e.expectNext(t, p, quiet, 24*time.Hour)
			fs.seen()
			// Due again; the edit's push holds the writer as the scheduler
			// reads the old context and dispatches the fetch.
			e.clock.Advance(24 * time.Hour)
			e.clock.Advance(time.Minute)
			var facts []map[string]any
			var secrets []protocol.Secret
			switch change {
			case "url", "url, failing first":
				facts = append(facts, e.fact("podcast.change_url", podTarget(p, g), map[string]any{"url": moved.url("/moved")}))
				if change == "url, failing first" {
					moved.with(func(fs *feedServer) { fs.status = 500 })
				}
			case "credentials":
				secret := protocol.Secret{ID: newID(), Username: "u", Password: "p"}
				secrets = append(secrets, secret)
				facts = append(facts, e.fact("podcast.set_credentials", podTarget(p, g), map[string]any{"secret_id": secret.ID}))
			case "generation":
				facts = append(facts, e.fact("podcast.unsubscribe", podTarget(p, g), map[string]any{}),
					e.fact("podcast.subscribe", []any{}, map[string]any{"feed_url": fs.url("/feed"),
						"observed": map[string]any{"podcast_id": p, "generation_id": g, "state": "removed"}}))
			}
			before := e.row(p).Context
			held, release, done := e.heldPush(facts, secrets...)
			<-held
			res, err := r.pass(context.Background(), false, false)
			if err != nil || res.started != 1 {
				t.Fatalf("dispatch under the held push: %+v %v", res, err)
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatalf("the edit: %v", err)
			}
			waitFetches(r)
			runDue(t, r)
			row := e.row(p)
			if row.Context == before {
				t.Fatalf("the fetch did not capture the edit: context %q", row.Context)
			}
			fetched := append(fs.seen(), moved.seen()...)
			if len(fetched) != 1 {
				t.Fatalf("fetches after the edit: %+v", fetched)
			}
			if change == "url, failing first" {
				if fetched[0].path != "/moved" || row.QuietSince != 0 || row.Failures != 1 {
					t.Fatalf("after the new URL's failed first fetch: request %+v, row %+v", fetched[0], row)
				}
				moved.with(func(fs *feedServer) { fs.status = 0 })
				e.fetchNext(t, r, p)
			} else if change == "url" && fetched[0].path != "/moved" {
				t.Fatalf("fetched %+v, not the new URL", fetched[0])
			}
			// The new context's first success starts its quiet time: hourly.
			e.expectNext(t, p, e.clock.ms(), time.Hour)
		})
	}
}

// An edit after the fetch captured its context is the scheduler's to see: the
// fetch's schedule keeps the context it fetched, and the next pass resets the
// quiet time and fetches the new URL at once.
func TestEditDuringFetchIsSeenByTheNextPass(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 400*day, true})
	p, g := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	quiet := e.quietFor(p, 100*24*time.Hour)
	e.fetchNext(t, r, p)
	fs.seen()
	e.clock.Advance(24*time.Hour + time.Minute)
	gate, arrived := make(chan struct{}), make(chan string, 1)
	fs.with(func(fs *feedServer) { fs.gate, fs.arrived = gate, arrived })
	if res, err := r.pass(context.Background(), false, false); err != nil || res.started != 1 {
		t.Fatalf("dispatch: %+v %v", res, err)
	}
	<-arrived // the fetch captured the old URL and is under way
	e.push(e.fact("podcast.change_url", podTarget(p, g), map[string]any{"url": fs.url("/moved")}))
	fs.with(func(fs *feedServer) { fs.gate, fs.arrived = nil, nil })
	close(gate)
	waitFetches(r)
	if row := e.row(p); row.QuietSince != quiet || strings.Contains(row.Context, "/moved") {
		t.Fatalf("the fetch of the old URL: %+v", row)
	}
	runDue(t, r)
	if rs := fs.seen(); len(rs) != 2 || rs[1].path != "/moved" {
		t.Fatalf("requests %+v, want the old URL, then the new one at once", rs)
	}
	e.expectNext(t, p, e.clock.ms(), time.Hour)
}

// A move the server's own fetch applies (a permanent redirect) keeps the
// quiet time: the feed is the same, at another URL.
func TestOwnMoveKeepsQuietTime(t *testing.T) {
	e := newEnv(t)
	fs := newFeedServer(t)
	fs.set(item{"One", e.clock.ms() - 400*day, true})
	p, _ := e.subscribe(fs.url("/feed"), nil)
	r := e.refresher(testFetcher(), Config{})
	runDue(t, r)
	quiet := e.quietFor(p, 100*24*time.Hour)
	e.fetchNext(t, r, p)
	fs.with(func(fs *feedServer) { fs.redirects["/feed"] = redirect{301, "/new"} })
	e.fetchNext(t, r, p)
	if row := e.row(p); !strings.Contains(row.Context, "/new") {
		t.Fatalf("the move was not applied: %+v", row)
	}
	e.expectNext(t, p, quiet, 24*time.Hour)
	fs.seen()
	e.fetchNext(t, r, p)
	if rs := fs.seen(); len(rs) != 1 || rs[0].path != "/new" {
		t.Fatalf("requests after the move %+v", rs)
	}
	e.expectNext(t, p, quiet, 24*time.Hour)
}
