package ops

// The queue publication's incremental keys against the full computation
// after every commit, its fallbacks to the full path, the warm-up after a
// restart, and the work of one queue command as the history grows.

import (
	"context"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/queue"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// publicationsVerified counts the incremental publications checked against
// the full computation; skipPublicationCheck turns the check off (a
// measurement of a command's own work).
var (
	publicationsVerified atomic.Int64
	skipPublicationCheck atomic.Bool
)

// Every incremental publication of every test of the package is checked: its
// keys are the full computation's, and every episode whose published key
// differs from the cache's is among those it republishes. A difference
// panics in the committing goroutine, so the test that made the commit fails
// with its stack.
func init() {
	checkPublication = func(c *protocol.ApplyContext, st *queue.State, cache *replayCache, pub queue.KeyMap, dirty map[string]bool) {
		if skipPublicationCheck.Load() {
			return
		}
		full, _, err := publishedKeys(c, st)
		if err != nil {
			panic(err)
		}
		if pub.Len() != len(full) {
			panic(fmt.Sprintf("incremental publication: %d keys, the full computation %d", pub.Len(), len(full)))
		}
		for e, k := range full {
			if got, ok := pub.Get(e); !ok || got != k {
				panic(fmt.Sprintf("incremental publication: %s has %v, the full computation %v", e, got, k))
			}
		}
		republished := func(e queue.Entry) {
			if ep, _ := scopeEpisode(string(e)); !dirty[ep] {
				panic(fmt.Sprintf("incremental publication: %s's key changed but its record is not republished", e))
			}
		}
		for e, k := range full {
			if p, ok := cache.pub.Get(e); !ok || p != k {
				republished(e)
			}
		}
		for e := range cache.pub.All() {
			if _, ok := full[e]; !ok {
				republished(e)
			}
		}
		publicationsVerified.Add(1)
	}
}

// A long random history of three phones, two of them often offline
// (their predictions conflict, so layouts are recorded), with sorts,
// keep-sorted periods, months-late uploads ordered near the start, commits
// rolled back after their publication, and a restore whose lost commits are
// recovered (their layouts and receipts rank before what the server saw).
// Every incremental publication equals the full computation (the package's
// check), the incremental replay equals a full one and the published queue is
// the replay's order after every push (replayCheck), and the rebuild
// invariant holds at the end.
func TestIncrementalPublicationEqualsFull(t *testing.T) {
	titles := make([]string, 40)
	for i := range titles {
		titles[i] = fmt.Sprintf("T%02d", i)
	}
	q := newQueueEnv(t, titles...)
	r := mrand.New(mrand.NewPCG(5, 8))
	devs := []*device{q.device("A"), q.device("B"), q.device("C")}
	views := make([]*queueView, len(devs))
	for i := range views {
		views[i] = &queueView{key: map[string]string{}, pid: map[string]string{}}
	}
	inc0, full0 := publicationCounts()
	ver0 := publicationsVerified.Load()
	pubStats.Lock()
	rewound0 := pubStats.rewound
	pubStats.Unlock()
	layouts0 := q.layouts(t)
	var backup string
	var atBackup int64
	var lastStep string
	steps := 160
	for i := range steps {
		di := r.IntN(len(devs))
		d, v := devs[di], views[di]
		if di == 0 || r.IntN(4) == 0 {
			v.sync(t, q) // A is always in sync; B and C are often offline
		}
		var batch []map[string]any
		for range 1 + r.IntN(3) {
			q.clock += second
			pick := func() string { return v.order[r.IntN(len(v.order))] }
			place := func() (map[string]any, string) {
				switch k := r.IntN(3); {
				case k == 0 || len(v.order) == 0:
					return atStart(), ""
				case k == 1:
					return atEnd(), ""
				}
				a := pick()
				return q.after(a), a
			}
			switch k := r.IntN(100); {
			case k < 3:
				batch = append(batch, q.sortCmd(d, "episode_title_a_z", nil, nil))
				sort.Strings(v.order)
			case k < 5:
				batch = append(batch, q.policy(d, titleSorting(r.IntN(2) == 0)))
			case k < 50 || len(v.order) < 5:
				x := titles[r.IntN(len(titles))]
				at, anchor := place()
				batch = append(batch, q.qfact(d, "queue.add", map[string]any{"members": []any{v.place(t, q, x, at, anchor)}, "place": at}, []string{x}))
			case k < 75:
				x := pick()
				at, anchor := place()
				if anchor == x {
					at, anchor = atEnd(), ""
				}
				m := v.move(t, q, x, at, anchor)
				m["place"] = at
				batch = append(batch, q.qfact(d, "queue.move", m, []string{x}))
			default:
				x := pick()
				v.remove(x)
				batch = append(batch, q.remove(d, x))
			}
		}
		if i%37 == 36 {
			// A months-late upload: ordered near the start of the history.
			saved := q.clock
			q.clock = 1_700_000_000_000 + int64(i)*second
			late := q.device(fmt.Sprintf("Late%d", i))
			lv := &queueView{key: map[string]string{}, pid: map[string]string{}}
			lv.sync(t, q)
			if len(lv.order) > 0 {
				x := lv.order[r.IntN(len(lv.order))]
				q.push(late, q.move(t, late, x, atStart()))
			}
			q.clock = saved
		}
		for _, res := range q.push(d, batch...) {
			if o := outcome(res); o == "rejected/invalid_payload" {
				t.Fatalf("%v", res)
			}
		}
		if i%23 == 22 {
			// A commit rolled back after its publication: it brought an
			// episode into the queue at the last step, and nothing of it is
			// kept (the next commit's replay and publication are checked).
			q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
				s, err := tx.Account(q.account).QueueStepsAfter(-1)
				lastStep = s[len(s)-1].Node
				return err
			})
			ref := q.refs[titles[r.IntN(len(titles))]]
			scope := protocol.EpisodeScope(ref["generation_id"].(string), ref["episode_id"].(string))
			if err := q.svc.Probe(t.Context(), q.account, func(c *protocol.ApplyContext) error {
				events, err := c.Account.ScopeQueueEvents(scope)
				if err != nil {
					return err
				}
				events = append(events, storage.QueueEventRow{Scope: scope, Node: lastStep, In: true})
				if err := c.Account.SetScopeQueueEvents(scope, events); err != nil {
					return err
				}
				c.QueueTouch(lastStep)
				return publishQueue(c, false, true)
			}); err != nil {
				t.Fatal(err)
			}
		}
		if i == steps/2 {
			backup = q.backup(t)
			atBackup = q.headSeq(t)
			for _, d := range devs {
				q.sees(d, d.last)
			}
		}
	}
	inc1, _ := publicationCounts()
	if q.layouts(t) == layouts0 {
		t.Fatal("no layout was recorded: the conflicting predictions went untested")
	}
	_, fullRec := publicationCounts()
	// The restore and the recovery of what it lost (every commit's entries
	// in one request per commit, layouts included).
	want := q.order(t)
	lost := q.logCommits(t, atBackup)
	q.restore(t, backup)
	succ := q.successor(t, devs[0])
	rec, _ := q.startRecovery(t, succ, nil)
	for _, c := range lost {
		for _, res := range rec.upload(t, c.entries).Results {
			if res.Status != "joined" && res.Status != "known" {
				t.Fatalf("recovery: %s %s", res.ID, res.Status)
			}
		}
	}
	checkReplay(t, q.svc, q.account)
	if got := q.order(t); got != want {
		t.Fatalf("after the recovery: %s, want %s", got, want)
	}
	// Commands after the recovery publish incrementally again.
	a := q.successor(t, devs[1])
	v := views[1]
	for range 10 {
		v.sync(t, q)
		q.clock += second
		x := titles[r.IntN(len(titles))]
		q.push(a, q.qfact(a, "queue.add", map[string]any{"members": []any{v.place(t, q, x, atStart(), "")}, "place": atStart()}, []string{x}))
	}
	inc2, full2 := publicationCounts()
	verified := publicationsVerified.Load() - ver0
	t.Logf("%d incremental publications (%d after the recovery), %d checked against the full computation; full ones: %v (before: %v); %d layouts",
		inc2-inc0, inc2-inc1, verified, full2, full0, q.layouts(t)-layouts0)
	pubStats.Lock()
	t.Logf("%d incremental publications resumed before the cache's last node", pubStats.rewound-rewound0)
	pubStats.Unlock()
	if inc1-inc0 < steps || inc2-inc1 < 10 || verified < int64(inc2-inc0)/2 {
		t.Fatalf("too few incremental publications checked: %d incremental, %d after the recovery, %d checked", inc2-inc0, inc2-inc1, verified)
	}
	pubStats.Lock()
	rewound := pubStats.rewound - rewound0
	pubStats.Unlock()
	if rewound == 0 {
		t.Fatal("no incremental publication resumed before the cache's last node: the late uploads went untested")
	}
	if full2["full"] == fullRec["full"] {
		t.Fatal("the recovery's uploads (earlier-ranked receipts and layouts) did not take the full path")
	}
}

// Each fallback takes the full path: a missing cache (a restart without the
// warm-up), a changed identity mapping (TestReplayAppliesTheFinalMappingFromTheStart
// checks its reason), a rebuild, and layout writes the replay did not make
// (recovered layouts, ranked before what the cache saw;
// TestIncrementalPublicationEqualsFull).
func TestPublicationFallbacksTakeTheFullPath(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C", "D")
	a := q.device("A")
	q.seed(t, a, "A", "B")
	q.clock += minute
	q.pushOK(t, a, q.add(t, a, atEnd(), "", "C"), "applied")
	expect := func(why string, step func()) {
		t.Helper()
		inc0, full0 := publicationCounts()
		step()
		inc1, full1 := publicationCounts()
		if full1[why] == full0[why] {
			t.Fatalf("%s: no full publication (%d incremental; full %v)", why, inc1-inc0, full1)
		}
	}
	// A warm cache publishes incrementally.
	inc0, _ := publicationCounts()
	q.clock += minute
	q.pushOK(t, a, q.move(t, a, "C", atStart()), "applied")
	if inc1, _ := publicationCounts(); inc1 == inc0 {
		t.Fatal("a queue command with a warm cache did not publish incrementally")
	}
	expect("no trusted cache", func() {
		q.svc.Store.Close()
		q.reopen(t)
		q.clock += minute
		q.pushOK(t, a, q.add(t, a, atEnd(), "", "D"), "applied")
	})
	expect("full", func() { checkRebuild(t, q.svc, q.account) })
	expect("layout writes the replay did not make", func() {
		if err := q.svc.Probe(t.Context(), q.account, func(c *protocol.ApplyContext) error {
			c.QueueKeysChanged()
			return publishQueue(c, false, true)
		}); err != nil {
			t.Fatal(err)
		}
	})
	expect("replayed from the start", func() {
		if err := q.svc.Probe(t.Context(), q.account, func(c *protocol.ApplyContext) error {
			c.QueueInvalidateAll()
			return publishQueue(c, false, true)
		}); err != nil {
			t.Fatal(err)
		}
	})
}

// After a restart, the warm-up replays the account once without a
// commit, keeps the cache for the head it read, and the first queue command
// resumes from it: no full replay, an incremental publication.
func TestQueueWarmupInstallsCache(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C", "D")
	a := q.device("A")
	q.seed(t, a, "A", "B", "C")
	q.clock += minute
	q.pushOK(t, a, q.move(t, a, "C", atStart()), "applied")
	q.svc.Store.Close()
	q.reopen(t)
	before := q.headSeq(t)
	warmed, err := WarmQueue(t.Context(), q.svc, q.account)
	if err != nil || !warmed {
		t.Fatalf("warm-up: %v %v", warmed, err)
	}
	if after := q.headSeq(t); after != before {
		t.Fatalf("the warm-up made a commit: head %d → %d", before, after)
	}
	if rc := q.cache(t); rc == nil || !rc.pubOK {
		t.Fatal("the warm-up kept no trusted cache for the head")
	}
	if again, err := WarmQueue(t.Context(), q.svc, q.account); err != nil || again {
		t.Fatalf("a second warm-up replayed again: %v %v", again, err)
	}
	q.replayCheck = false // its full replays would count
	full0, inc0 := replayCounts()
	pinc0, _ := publicationCounts()
	q.clock += minute
	q.pushOK(t, a, q.add(t, a, q.after("A"), "", "D"), "applied")
	full1, inc1 := replayCounts()
	pinc1, _ := publicationCounts()
	if full1 != full0 || inc1 == inc0 || pinc1 == pinc0 {
		t.Fatalf("the first command after the warm-up: %d full replays, %d incremental, %d incremental publications", full1-full0, inc1-inc0, pinc1-pinc0)
	}
	checkReplay(t, q.svc, q.account)
	q.expectOrder(t, "C,A,D,B")
}

// WarmQueues warms every account, one at a time, and stops when its context
// ends, also during an account's replay: that replay is not kept and no
// later account is warmed.
func TestWarmQueuesRecentlyActiveAccounts(t *testing.T) {
	q := newQueueEnv(t, "A", "B")
	a := q.device("A")
	q.seed(t, a, "A", "B")
	accounts := []string{q.account}
	for i := range 3 {
		if err := q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
			acct, err := tx.CreateAccount()
			if err != nil || i == 2 {
				return err // the last one has no installation: not active, not warmed
			}
			accounts = append(accounts, acct.ID)
			return tx.Account(acct.ID).InsertInstallation(storage.Installation{InstallID: fmt.Sprintf("install-%d", i), InstanceNonce: "n",
				Name: "phone", Platform: "ios", AppVersion: "1", Capabilities: "[]"})
		}); err != nil {
			t.Fatal(err)
		}
	}
	q.svc.Store.Close()
	q.reopen(t)
	var order []string
	warmupReplayed = func(account string) { order = append(order, account) }
	defer func() { warmupReplayed = nil }()
	WarmQueues(t.Context(), q.svc)
	sort.Strings(accounts)
	if fmt.Sprint(order) != fmt.Sprint(accounts) {
		t.Fatalf("warmed %v, want every active account in turn: %v", order, accounts)
	}
	for _, account := range accounts {
		if q.cacheOf(t, account) == nil {
			t.Fatalf("account %s kept no cache", account)
		}
	}
	// Cancelled while the first account's replay is done but not yet kept.
	q.svc.Store.Close()
	q.reopen(t)
	ctx, cancel := context.WithCancel(t.Context())
	order = nil
	warmupReplayed = func(account string) {
		order = append(order, account)
		cancel()
	}
	WarmQueues(ctx, q.svc)
	if len(order) != 1 {
		t.Fatalf("replayed %v after the cancellation", order)
	}
	for _, account := range accounts {
		if q.cacheOf(t, account) != nil {
			t.Fatalf("account %s kept a cache after the cancellation", account)
		}
	}
}

// A live write during the warm-up's replay never waits for it: the replay
// reads a snapshot without the store's writer. Its result, overtaken by the
// write, is discarded (the write's own cache stays).
func TestLiveWriteDuringWarmup(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	a := q.device("A")
	q.seed(t, a, "A", "B", "C")
	q.svc.Store.Close()
	q.reopen(t)
	paused, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	replays := 0
	warmupReplayed = func(string) {
		replays++
		once.Do(func() {
			close(paused)
			<-release
		})
	}
	defer func() { warmupReplayed = nil }()
	free := func() { releaseOnce.Do(func() { close(release) }) }
	defer free()
	type result struct {
		warmed bool
		err    error
	}
	done := make(chan result, 1)
	go func() {
		warmed, err := WarmQueue(context.Background(), q.svc, q.account)
		done <- result{warmed, err}
	}()
	<-paused
	// A warm-up still holding the writer would hold this write until the
	// watchdog frees it.
	watchdog := time.AfterFunc(30*time.Second, free)
	defer watchdog.Stop()
	started := time.Now()
	q.clock += minute
	q.pushOK(t, a, q.move(t, a, "C", atStart()), "applied")
	if waited := time.Since(started); waited > 10*time.Second {
		t.Fatalf("a live write waited %s for the warm-up", waited)
	}
	written := q.cache(t) // the write's own replay left the cache for its head
	if written == nil {
		t.Fatal("the write kept no cache")
	}
	free()
	// The overtaken replay is discarded; the next attempt finds the write's
	// cache valid and stops.
	r := <-done
	if r.err != nil || r.warmed || replays != 1 {
		t.Fatalf("warm-up: kept %v, %v, after %d replays", r.warmed, r.err, replays)
	}
	if q.cache(t) != written {
		t.Fatal("the overtaken replay replaced the write's cache")
	}
	q.expectOrder(t, "C,A,B")
}

// A live write while the warm-up's replay is still in progress (inside its
// snapshot) completes before the replay goes on: the replay holds no
// writer. Held there, the warm-up is cancelled: it keeps nothing.
func TestLiveWriteDuringWarmupReplay(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	a := q.device("A")
	q.seed(t, a, "A", "B", "C")
	q.svc.Store.Close()
	q.reopen(t)
	held, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	free := func() { releaseOnce.Do(func() { close(release) }) }
	defer free()
	warmupReplaying = func(string) {
		once.Do(func() {
			close(held)
			<-release
		})
	}
	defer func() { warmupReplaying = nil }()
	done := make(chan error, 1)
	go func() {
		_, err := WarmQueue(context.Background(), q.svc, q.account)
		done <- err
	}()
	<-held
	// A replay holding the writer would hold this write until the watchdog
	// releases the replay.
	watchdog := time.AfterFunc(30*time.Second, free)
	defer watchdog.Stop()
	started := time.Now()
	q.clock += minute
	q.pushOK(t, a, q.move(t, a, "C", atStart()), "applied")
	waited := time.Since(started)
	select {
	case <-release:
		t.Fatalf("the live write finished only after the replay was released (%s)", waited)
	default:
	}
	free()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	q.expectOrder(t, "C,A,B")

	// Cancelled while the replay is in progress: nothing is kept.
	q.svc.Store.Close()
	q.reopen(t)
	ctx, cancel := context.WithCancel(t.Context())
	warmupReplaying = func(string) { cancel() }
	warmed, err := WarmQueue(ctx, q.svc, q.account)
	if warmed || err == nil {
		t.Fatalf("a warm-up cancelled during its replay: kept %v, error %v", warmed, err)
	}
	if q.cache(t) != nil {
		t.Fatal("a warm-up cancelled during its replay kept a cache")
	}
}

// cache is the account's replay cache if it is valid for the head.
func (q *queueEnv) cache(t *testing.T) *replayCache { return q.cacheOf(t, q.account) }

// cacheOf is an account's replay cache if it is valid for its head.
func (q *queueEnv) cacheOf(t *testing.T, account string) *replayCache {
	t.Helper()
	var rc *replayCache
	if err := q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
		a := tx.Account(account)
		h, err := a.Head()
		if err != nil {
			return err
		}
		if v, ok := a.Memo().Load(memoKey{account}); ok && v.(*replayCache).valid(h) {
			rc = v.(*replayCache)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return rc
}

// The server's part of a queue edit's work: with a queue of 10 members and a history of
// 1,000, 10,000 and 50,000 queue commands, a queue edit (an add after a
// member, a move to the start, a removal; one push each) loads, reads and
// visits in proportion to what it touches: replay nodes loaded, layout key
// rows read, entries recomputed, records checked and tree nodes visited and
// copied stay within a small factor of the 1,000-command history's.
// NTP_QUEUE_BUDGET_MAX bounds the largest history (default 50000).
func TestQueueEditWorkBudget(t *testing.T) {
	if testing.Short() || raceEnabled {
		// Building the histories takes minutes; under the race detector,
		// half an hour (a single goroutine's counters: make check runs it
		// without).
		t.Skip("load test (make check runs it without the race detector)")
	}
	sizes := []int{1000, 10000, 50000}
	if s := os.Getenv("NTP_QUEUE_BUDGET_MAX"); s != "" {
		limit, _ := strconv.Atoi(s)
		sizes = sizes[:0]
		for _, n := range []int{1000, 10000, 50000} {
			if n <= limit {
				sizes = append(sizes, n)
			}
		}
	}
	type work struct {
		nodes, keyRows, entries, positions int64
		visited, allocated                 int64
		fullReplays                        int
	}
	// The package's check of every incremental publication against the full
	// computation reads the whole history: not part of a command's work.
	skipPublicationCheck.Store(true)
	defer skipPublicationCheck.Store(false)
	measure := func(history int) work {
		episodes := history / 2
		titles := make([]string, episodes+40)
		for i := range titles {
			titles[i] = fmt.Sprintf("E%06d", i)
		}
		q := newQueueEnv(t, titles...)
		q.replayCheck = false
		q.noRebuild = "a measurement of a long history (the rebuild invariant is checked by the queue tests)"
		viewstest.Skip(q.svc.Store) // the read model's own check would count
		a := q.device("A")
		// The history: every episode added, then removed, 250 commands a
		// push; after each push two of them go back to the start with keys
		// out of order (a phone that had not pulled), so a layout gives one
		// a fresh key, and they leave again with the next push: the layout
		// key writes grow with the history too.
		var batch []map[string]any
		var sticky []string
		flush := func(last int) {
			q.push(a, batch...)
			batch = batch[:0]
			x, y := titles[last-1], titles[last]
			k, id := keyBetween(t, "", "")
			k2, id2 := keyBetween(t, k, "")
			q.clock += second
			q.push(a, q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement(x, k, id)}, "place": atStart()}, []string{x}),
				q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement(y, k2, id2)}, "place": atStart()}, []string{y}))
			sticky = []string{x, y}
		}
		for i := 0; i < episodes; i++ {
			if len(sticky) > 0 {
				q.clock += second
				batch = append(batch, q.remove(a, sticky...))
				sticky = nil
			}
			q.clock += second
			batch = append(batch, q.add(t, a, atEnd(), "", titles[i]))
			q.clock += second
			batch = append(batch, q.remove(a, titles[i]))
			if len(batch) >= 250 || i == episodes-1 {
				flush(i)
			}
		}
		q.clock += second
		q.pushOK(t, a, q.remove(a, sticky...), "applied")
		var layoutRows int
		q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
			rows, err := tx.Account(q.account).KeyWrites()
			layoutRows = len(rows)
			return err
		})
		live := titles[episodes : episodes+10]
		q.clock += second
		q.pushOK(t, a, q.add(t, a, atEnd(), "", live...), "applied")
		// The phone predicts keys between its neighbours, as a device does.
		v := &queueView{key: map[string]string{}, pid: map[string]string{}}
		edit := func(typ string, x string, at map[string]any, anchor string) {
			t.Helper()
			v.sync(t, q)
			q.clock += second
			var f map[string]any
			switch typ {
			case "add":
				f = q.qfact(a, "queue.add", map[string]any{"members": []any{v.place(t, q, x, at, anchor)}, "place": at}, []string{x})
			case "move":
				m := v.move(t, q, x, at, anchor)
				m["place"] = at
				f = q.qfact(a, "queue.move", m, []string{x})
			default:
				f = q.remove(a, x)
			}
			q.pushOK(t, a, f, "applied")
		}
		// Warm: one command first.
		edit("move", live[0], atEnd(), "")
		f0, _ := replayCounts()
		replayStats.Lock()
		n0 := replayStats.nodes
		replayStats.Unlock()
		pubStats.Lock()
		e0, p0 := pubStats.entries, pubStats.positions
		pubStats.Unlock()
		k0 := storage.KeyWriteRowsRead()
		v0, a0 := ptree.Counts()
		const edits = 9
		for i := range edits / 3 {
			x := titles[episodes+10+i]
			edit("add", x, q.after(live[i]), live[i])
			edit("move", live[i+3], atStart(), "")
			edit("remove", x, nil, "")
		}
		v1, a1 := ptree.Counts()
		f1, _ := replayCounts()
		replayStats.Lock()
		n1 := replayStats.nodes
		replayStats.Unlock()
		pubStats.Lock()
		e1, p1 := pubStats.entries, pubStats.positions
		pubStats.Unlock()
		w := work{nodes: int64(n1 - n0), keyRows: storage.KeyWriteRowsRead() - k0, entries: int64(e1 - e0),
			positions: int64(p1 - p0), visited: (v1 - v0) / edits, allocated: (a1 - a0) / edits, fullReplays: f1 - f0}
		t.Logf("history %d (%d layout key rows), %d edits: %d replay nodes loaded, %d layout key rows read, %d entries recomputed, %d records checked in all; "+
			"per edit %d tree nodes visited, %d copied", history, layoutRows, edits, w.nodes, w.keyRows, w.entries, w.positions, w.visited, w.allocated)
		return w
	}
	var base work
	for i, n := range sizes {
		w := measure(n)
		if w.fullReplays != 0 {
			t.Fatalf("history %d: %d full replays", n, w.fullReplays)
		}
		if i == 0 {
			base = w
			continue
		}
		bound := func(what string, got, at1000 int64, slack int64) {
			if got > 2*at1000+slack {
				t.Errorf("history %d: %s %d, at 1,000 commands %d: it grows with the history", n, what, got, at1000)
			}
		}
		bound("replay nodes loaded", w.nodes, base.nodes, 4)
		bound("layout key rows read", w.keyRows, base.keyRows, 4)
		bound("entries recomputed", w.entries, base.entries, 4)
		bound("records checked", w.positions, base.positions, 4)
		bound("tree nodes visited per edit", w.visited, base.visited, 200)
		bound("tree nodes copied per edit", w.allocated, base.allocated, 100)
	}
	// Nine edits: each loads its own node or two, recomputes the keys it
	// writes and checks their records.
	if base.nodes > 30 || base.entries > 30 || base.positions > 30 || base.keyRows > 30 {
		t.Errorf("nine queue edits load %d nodes, read %d layout rows, recompute %d entries and check %d records", base.nodes, base.keyRows,
			base.entries, base.positions)
	}
}

// A late listening fact can take a key write away from a later command
// without writing one itself: B's resume, ordered before P's completion of
// Z, cancels the completion's removal, so P's later re-add of Z finds it
// queued and places nothing. The replay resumes before the resume; the
// re-add's key write is only in the cache's past, and Z's published key goes
// back to its first add's (the incremental publication recomputes the
// entries written on either side of the shared past).
func TestLateClaimUndoesALaterKeyWrite(t *testing.T) {
	q := newQueueEnv(t, "Z", "A")
	p, b := q.device("P"), q.device("B")
	q.seed(t, p, "Z", "A")
	first := q.positions(t)
	sp, sb := newID(), newID()
	q.clock += minute
	startB := q.lfact(b, "session.start", q.refs["Z"], start(sb, 0, true), p.last)
	q.pushOK(t, b, startB, "applied")
	q.clock += minute
	q.pushOK(t, b, q.lfact(b, "session.checkpoint", q.refs["Z"], checkpoint(sb, 1, minute, minute)), "applied")
	q.clock += minute
	q.pushOK(t, p, q.lfact(p, "session.start", q.refs["Z"], start(sp, minute, true), b.last), "applied")
	q.clock += minute
	resumeB := q.lfact(b, "session.checkpoint", q.refs["Z"], resume(sb, 2, minute, minute, startB["op_id"].(string)))
	q.clock += minute
	q.pushOK(t, p, q.lfact(p, "session.end", q.refs["Z"], end(sp, 1, 59*minute, 58*minute, "ended", true, true, true)), "applied")
	q.expectOrder(t, "A")
	// P adds Z back at the start, with a key before A's.
	q.clock += minute
	k, id := keyBetween(t, "", first["A"][0])
	q.pushOK(t, p, q.qfact(p, "queue.add", map[string]any{"members": []any{q.placement("Z", k, id)}, "place": atStart()}, []string{"Z"}), "applied")
	q.expectOrder(t, "Z,A")
	if got := q.positions(t)["Z"][0]; got != k {
		t.Fatalf("Z's key after the re-add: %s, want %s", got, k)
	}
	inc0, _ := publicationCounts()
	pubStats.Lock()
	rewound0 := pubStats.rewound
	pubStats.Unlock()
	q.pushOK(t, b, resumeB, "applied")
	q.expectOrder(t, "Z,A")
	if got := q.positions(t)["Z"]; got != first["Z"] {
		t.Fatalf("Z's published key %v, want its first add's %v", got, first["Z"])
	}
	inc1, _ := publicationCounts()
	pubStats.Lock()
	rewound := pubStats.rewound - rewound0
	pubStats.Unlock()
	if inc1 == inc0 || rewound == 0 {
		t.Fatalf("the resume's commit did not publish incrementally from before the cache's last node (%d, %d)", inc1-inc0, rewound)
	}
}
