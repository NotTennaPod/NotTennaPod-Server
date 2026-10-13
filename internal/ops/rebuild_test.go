package ops

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/rebuildtest"
)

// checkRebuild is the rebuild invariant (PROTOCOL §6.11), run at the end of every
// test environment and rule vector (rebuildtest.Check).
func checkRebuild(t testing.TB, svc *protocol.Service, account string) {
	t.Helper()
	rebuildtest.Check(t, svc, account, nil)
}

// effectRef is the episode reference in stored contribution effects. Data in
// the upgrade fixtures has none, and a rebuild writes it, so their derived
// state is compared without it; no stored data needs a backfill.
var effectRef = regexp.MustCompile(`"ref":\{[^{}]*\},?`)

// registeredBy is the episode's registering node the queue replay added
// (migration 0023, no backfill); a rebuild writes it.
var registeredBy = regexp.MustCompile(` registered_by=\S*`)

// checkRebuildUpgraded is the rebuild invariant for the upgrade fixtures.
func checkRebuildUpgraded(t testing.TB, svc *protocol.Service, account string) {
	t.Helper()
	rebuildtest.Check(t, svc, account, func(row string) string {
		return registeredBy.ReplaceAllString(effectRef.ReplaceAllString(row, ""), "")
	})
}

func describeRebuild(r protocol.RebuildReport) string { return rebuildtest.Describe(r) }

// The rebuild holds the account's write transaction (PROTOCOL §6.11), so its cost
// matters: an account of listening sessions, favorites and queue changes,
// pushed in small batches as phones push them, with resolved adds at random
// places and keep-sorted sort results. NTP_REBUILD_LOAD sets the number of
// facts (default 2,000), NTP_REBUILD_EPISODES the episodes they are spread
// over (default 100), NTP_REBUILD_STAGING the staging threshold (-1: in
// place); the test logs the push and rebuild times and how long the rebuild
// held the write lock.
func TestRebuildLoad(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("load test (make check runs it without the race detector)")
	}
	n := 2000
	if s := os.Getenv("NTP_REBUILD_LOAD"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	staging := 0 // the default threshold
	if s := os.Getenv("NTP_REBUILD_STAGING"); s != "" {
		staging, _ = strconv.Atoi(s) // -1: always in place
	}
	episodes := 100
	if s := os.Getenv("NTP_REBUILD_EPISODES"); s != "" { // 1: a concentrated history
		episodes, _ = strconv.Atoi(s)
	}
	titles := make([]string, episodes)
	for i := range titles {
		titles[i] = fmt.Sprintf("E%03d", i)
	}
	q := newQueueEnv(t, titles...)
	q.noRebuild = "the test checks its rebuild itself"
	q.svc.StagingThreshold = staging
	a, b := q.device("A"), q.device("B")
	q.seed(t, a, titles[:min(10, episodes)]...)
	q.pushOK(t, a, q.policy(a, map[string]any{"enqueue_location": "random"}), "applied")
	// B plays without the queue capability: its starts get resolved adds.
	q.setCapabilities(t, b, `["favorites","history","inbox","statistics"]`)
	facts, pushes := 0, 0
	var pushTime time.Duration
	push := func(d *device, batch []map[string]any) {
		begin := time.Now()
		r := q.push(d, batch...)
		pushTime += time.Since(begin)
		pushes++
		facts += len(batch)
		for i, x := range r {
			if x.Status != "accepted" {
				t.Fatalf("push %d fact %d: %+v", pushes, i, x)
			}
		}
	}
	for i := 0; facts < n; i++ {
		title := titles[i%episodes]
		ref := q.refs[title]
		d := a
		if i%3 == 1 {
			d = b
		}
		if i == episodes/2 {
			push(a, []map[string]any{q.policy(a, map[string]any{"sorting": map[string]any{"keep_sorted": true, "sort_order": "episode_title_a_z"}})})
		}
		s := newID()
		q.clock += minute
		batch := []map[string]any{q.lfactRef(d, "session.start", ref, start(s, 0, true))}
		for o := int64(1); o <= 20; o++ {
			q.clock += minute
			batch = append(batch, q.lfactRef(d, "session.checkpoint", ref, checkpoint(s, o, o*minute, minute)))
			if len(batch) == 8 {
				push(d, batch)
				batch = nil
			}
		}
		q.clock += minute
		batch = append(batch, q.lfactRef(d, "session.end", ref, end(s, 21, 21*minute, minute, "ended", i%2 == 0, true, true)))
		if i%5 == 0 {
			batch = append(batch, q.lfactRef(d, "favorite.set", ref, map[string]any{"on": i%10 == 0}))
		}
		push(d, batch)
	}
	report, err := q.svc.Rebuild(t.Context(), q.account)
	if err != nil || report.Committed {
		t.Fatalf("rebuild: %v\n%s", err, describeRebuild(report))
	}
	per10k := time.Duration(float64(report.Duration) / float64(report.Facts) * 10_000)
	t.Logf("%d facts in %d pushes took %s; the rebuild replayed %d facts and %d decisions in %s (%s per 10,000 facts); staged %v, write lock held %s",
		facts, pushes, pushTime, report.Facts, report.Decisions, report.Duration, per10k, report.Staged, report.Locked)
	checkRebuild(t, q.svc, q.account)
}
