package refresh

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/clock"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// The refresh schedule runs on the server's one wall clock (PROTOCOL §16, --dev-clock-offset). With
// the clock three days ahead or behind, a fetch's time, its published status and its next due time
// are the moved clock's, and the decisions it publishes are received at the moved clock's time. A
// restart with the clock corrected is a jump the schedule follows: a deadline the clock left far
// ahead comes back within an interval, and one it left behind is overdue work, spread as at any
// start. Nothing is fetched at the restart in either case.
func TestTheScheduleRunsOnTheServersClock(t *testing.T) {
	for _, offset := range []time.Duration{72 * time.Hour, -72 * time.Hour} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			e := newEnv(t)
			wall := clock.Offset(offset)
			e.svc.Now = wall.Now
			fs := newFeedServer(t)
			fs.set(item{"One", time.Now().Add(-30 * 24 * time.Hour).UnixMilli(), true})
			p, _ := e.subscribe(fs.url("/feed"), nil)
			r := New(e.svc, Config{Fetcher: testFetcher(), Clock: wall, Random: fixedRandom(0.5)})
			before := wall.Now().UnixMilli()
			runDue(t, r)
			after := wall.Now().UnixMilli()
			if n := len(fs.seen()); n != 1 {
				t.Fatalf("%d fetches for the first population", n)
			}
			row := e.row(p)
			// The next is an hour after the fetch ended (no jitter).
			if row.LastAttempt < before || row.LastAttempt > after || row.LastSuccess != row.LastAttempt ||
				row.NextAt < row.LastAttempt+hour || row.NextAt > after+hour {
				t.Fatalf("schedule row %+v, want the fetch within [%d, %d] and the next an hour later", row, before, after)
			}
			st := decode(t, e.podcastRecord(p, "refresh_status"))
			if num(st, "last_attempt") != row.LastAttempt || num(st, "next_attempt") != row.NextAt {
				t.Errorf("published status %v, row %+v", st, row)
			}
			fetches := 0
			if err := e.svc.ServerCommit(context.Background(), e.account, func(c *protocol.ApplyContext) error {
				decisions, err := c.Account.Decisions()
				for _, d := range decisions {
					if d.DecisionType != "catalog.refresh" {
						continue
					}
					fetches++
					if d.NormalizedTime < before || d.NormalizedTime > after {
						t.Errorf("the fetch's decision was received at %d, want within [%d, %d]", d.NormalizedTime, before, after)
					}
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if fetches != 1 {
				t.Errorf("%d fetch decisions", fetches)
			}

			// The server restarts with its clock corrected.
			e.svc.Now = clock.System().Now
			restarted := New(e.svc, Config{Fetcher: testFetcher(), Clock: clock.System(), Random: fixedRandom(0.5)})
			passes := make(chan struct{}, 10)
			restarted.afterPass = func() { passes <- struct{}{} }
			start := time.Now().UnixMilli()
			restarted.Start(context.Background())
			<-passes
			restarted.Stop()
			end := time.Now().UnixMilli()
			want := hour // ahead: brought back to one interval (no jitter)
			if offset < 0 {
				want = (5 * time.Minute).Milliseconds() // behind: overdue, spread over min(10 min, interval/6)
			}
			if row := e.row(p); row.NextAt < start+want || row.NextAt > end+want {
				t.Errorf("after the restart the next fetch is at %d, want within [%d, %d]", row.NextAt, start+want, end+want)
			}
			if n := len(fs.seen()); n != 0 {
				t.Errorf("%d fetches at the restart", n)
			}
		})
	}
}
