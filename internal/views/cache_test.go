package views_test

// The read model's cache: one builder per account, elected without
// blocking, publication fenced on identity, the purge and suspect mode,
// the warmers and the idle release, and the structural memory bound.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// cacheEnv is a store with a small generated library, a views registry and
// a cache over them, wired as the read API wires them.
type cacheEnv struct {
	t        *testing.T
	store    *storage.Store
	account  string
	episodes []string
	spool    *views.Spool
	cache    *views.Cache
	n        atomic.Int64 // makes each commit's value new

	clockMu sync.Mutex
	now     time.Time

	mon *monitor
}

func newCacheEnv(t *testing.T) *cacheEnv {
	t.Helper()
	return newCacheEnvWith(t, viewstest.Spec{Seed: 5, Podcasts: []int{8, 8, 4}, Queue: 4, Now: viewstest.FixedNow, Months: 6, Prefix: "k"})
}

func newCacheEnvWith(t *testing.T, spec viewstest.Spec) *cacheEnv {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := &cacheEnv{t: t, store: store, now: time.Unix(1_700_000_000, 0)}
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		e.account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	lib, err := viewstest.Generate(ctx, store, e.account, spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, eps := range lib.Episodes {
		e.episodes = append(e.episodes, eps...)
	}
	viewstest.Skip(store) // every read here is checked against a full build in its own snapshot
	if e.spool, err = views.NewSpool("", e.clock); err != nil {
		t.Fatal(err)
	}
	e.cache = views.NewCache(store, e.spool, e.clock)
	t.Cleanup(e.cache.Close) // before the store closes
	store.OnIdentityChange(func(accounts []string, all bool) {
		if all {
			e.spool.RetireAll()
		}
		for _, a := range accounts {
			e.spool.Retire(a)
		}
	})
	e.mon = &monitor{last: map[uint64]views.Library{}}
	e.cache.HookForTesting(e.mon.hooks(nil))
	return e
}

func (e *cacheEnv) clock() time.Time {
	e.clockMu.Lock()
	defer e.clockMu.Unlock()
	return e.now
}

func (e *cacheEnv) advance(d time.Duration) {
	e.clockMu.Lock()
	e.now = e.now.Add(d)
	e.clockMu.Unlock()
}

// commit changes one episode's play state record (its provenance at least,
// so the change is always captured), with an optional padding of n bytes,
// and appends a commit (the account's listeners are woken).
func (e *cacheEnv) commit(pad int) string {
	e.t.Helper()
	return e.commitTo(e.episodes[int(e.n.Load()+1)%len(e.episodes)], pad)
}

// commitTo is commit on a given episode.
func (e *cacheEnv) commitTo(ep string, pad int) string {
	e.t.Helper()
	i := e.n.Add(1)
	state := []string{"played", "unplayed"}[i%2]
	value := fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, state)
	if pad > 0 {
		value = fmt.Sprintf(`{"generation_id":"g","pad":%q,"play_state":%q}`, strings.Repeat("x", pad), state)
	}
	if err := e.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: ep, FieldGroup: "play_state", Value: value,
			Provenance: fmt.Sprintf(`["t%d"]`, i)}); err != nil {
			return err
		}
		_, _, err := a.AppendCommit(storage.CommitBody{})
		return err
	}); err != nil {
		e.t.Error(err)
	}
	return ep
}

// commitAll is a broad change: every episode's play state record, all
// played or all unplayed by turns, in one commit (a whole library marked
// played, or that undone).
func (e *cacheEnv) commitAll() {
	e.t.Helper()
	i := e.n.Add(1)
	value := fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, []string{"played", "unplayed"}[i%2])
	if err := e.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		for _, ep := range e.episodes {
			if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: ep, FieldGroup: "play_state", Value: value,
				Provenance: fmt.Sprintf(`["t%d"]`, i)}); err != nil {
				return err
			}
		}
		_, _, err := a.AppendCommit(storage.CommitBody{})
		return err
	}); err != nil {
		e.t.Error(err)
	}
}

// raiseGeneration rewrites a record with capture suspended (a bulk rewrite).
func (e *cacheEnv) raiseGeneration() {
	e.t.Helper()
	if err := e.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		if err := tx.SuspendViewCapture(e.account); err != nil {
			return err
		}
		return tx.Account(e.account).PutRecord(storage.RecordRow{Entity: "episode", EntityID: e.episodes[0], FieldGroup: "play_state",
			Value: `{"generation_id":"g","play_state":"played"}`, Provenance: fmt.Sprintf(`["bulk%d"]`, e.n.Add(1))})
	}); err != nil {
		e.t.Fatal(err)
	}
}

// correct checks a served library against its own snapshot: its key, and
// its content against a full build there.
func (e *cacheEnv) correct(r *storage.ReadTx, lib *views.Library) error {
	acct := r.Account(e.account)
	head, err := acct.Head()
	if err != nil {
		return err
	}
	clock, err := acct.ViewClock()
	if err != nil {
		return err
	}
	if lib.Head.Seq != head.Seq || lib.Head.Epoch != head.Epoch || lib.V.Clock != clock || lib.V.Head.Seq != head.Seq {
		return fmt.Errorf("served %+v %+v at a snapshot of %+v %+v", lib.Head, lib.V.Clock, head, clock)
	}
	full, err := views.BuildVersion(acct, e.account)
	if err != nil {
		return err
	}
	if err := views.EqualVersions(lib.V, full); err != nil {
		return fmt.Errorf("served a library that differs from its snapshot's: %w", err)
	}
	return nil
}

// get reads the account's library as a request does, checked.
func (e *cacheEnv) get(ctx context.Context) (*views.Library, error) {
	return e.cache.Get(ctx, e.account, nil, e.correct)
}

func (e *cacheEnv) mustGet() *views.Library {
	e.t.Helper()
	lib, err := e.get(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return lib
}

// waitFor waits for cond, failing after 30 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// monitor checks the election's invariants on every build and publication:
// at most one builder at a time, the published version never goes back
// within an incarnation, and it counts the builds by path.
type monitor struct {
	building atomic.Int32
	builds   atomic.Int64
	waits    atomic.Int64
	served   atomic.Int64

	mu    sync.Mutex
	paths map[views.Path]int
	fails int
	pubs  int
	last  map[uint64]views.Library // by incarnation: the last published
	errs  []string
	base  *views.Version // the last published version
}

func (m *monitor) fail(format string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errs = append(m.errs, fmt.Sprintf(format, args...))
}

// hooks are the monitor's hooks, with extra's added.
func (m *monitor) hooks(extra *views.CacheHooks) views.CacheHooks {
	var x views.CacheHooks
	if extra != nil {
		x = *extra
	}
	return views.CacheHooks{
		Keyed: func(a string) {
			if x.Keyed != nil {
				x.Keyed(a)
			}
		},
		Build: func(a string) {
			if n := m.building.Add(1); n > 1 {
				m.fail("%d builders at once", n)
			}
			m.builds.Add(1)
			if x.Build != nil {
				x.Build(a)
			}
		},
		Built: func(a string, p views.Path, v *views.Version, err error) *views.Version {
			m.building.Add(-1)
			m.mu.Lock()
			if m.paths == nil {
				m.paths = map[views.Path]int{}
			}
			if err != nil {
				m.fails++
			} else {
				m.paths[p]++
			}
			m.mu.Unlock()
			if x.Built != nil {
				return x.Built(a, p, v, err)
			}
			return v
		},
		Waiting: func(a string) {
			m.waits.Add(1)
			if x.Waiting != nil {
				x.Waiting(a)
			}
		},
		Published: func(a string, lib *views.Library) {
			m.mu.Lock()
			inc := views.LibraryIncarnation(lib)
			if prev, ok := m.last[inc]; ok && !newerKey(lib, &prev) {
				m.errs = append(m.errs, fmt.Sprintf("published %+v %+v after %+v %+v", lib.Head, lib.V.Clock, prev.Head, prev.V.Clock))
			}
			m.last[inc] = *lib
			m.pubs++
			m.base = lib.V
			m.mu.Unlock()
			if x.Published != nil {
				x.Published(a, lib)
			}
		},
		Served: func(a string, lib *views.Library) {
			m.served.Add(1)
			if x.Served != nil {
				x.Served(a, lib)
			}
		},
		Compared: x.Compared,
	}
}

func newerKey(a, b *views.Library) bool {
	ka := []int64{a.Head.Epoch, a.V.Clock.Generation, a.V.Clock.Tick, a.Head.Seq}
	kb := []int64{b.Head.Epoch, b.V.Clock.Generation, b.V.Clock.Tick, b.Head.Seq}
	for i := range ka {
		if ka[i] != kb[i] {
			return ka[i] > kb[i]
		}
	}
	return false
}

func (m *monitor) path(p views.Path) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.paths[p]
}

func (m *monitor) pubCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pubs
}

func (m *monitor) lastBase() *views.Version {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.base
}

// check fails the test with the invariants' violations.
func (m *monitor) check(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.errs {
		t.Error(e)
	}
}

// omitted is an apply that read none of its changes: the base's content at
// the snapshot's identity (a corruption).
func omitted(base, v *views.Version) *views.Version {
	o := *base
	o.Head, o.Clock = v.Head, v.Clock
	return &o
}

// Cold readers join one build; the others release their readers and
// wait outside any transaction (so the pool's other readers are free), then
// are served the one build.
func TestCacheColdReadersShareOneBuild(t *testing.T) {
	e := newCacheEnv(t)
	release := make(chan struct{})
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Build: func(string) { <-release }}))
	const readers = 8
	libs := make([]*views.Library, readers)
	errs := make([]error, readers)
	var wg sync.WaitGroup
	for i := range readers {
		wg.Go(func() { libs[i], errs[i] = e.get(context.Background()) })
	}
	waitFor(t, "7 readers waiting for the build", func() bool { return e.mon.waits.Load() >= readers-1 })
	// The builder holds one of the 4 readers; the 7 waiting hold none.
	held, done := make(chan struct{}), make(chan struct{})
	for range 3 {
		go e.store.Read(context.Background(), func(*storage.ReadTx) error {
			held <- struct{}{}
			<-done
			return nil
		})
	}
	for i := range 3 {
		select {
		case <-held:
		case <-time.After(20 * time.Second):
			t.Fatalf("only %d of the 3 other readers free while readers wait: a waiter holds one", i)
		}
	}
	close(done)
	close(release)
	wg.Wait()
	for i := range readers {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if libs[i] != libs[0] {
			t.Errorf("reader %d served another library", i)
		}
	}
	if n := e.mon.builds.Load(); n != 1 || e.mon.path(views.PathFirst) != 1 {
		t.Errorf("%d builds for %d cold readers, want 1", n, readers)
	}
	e.mon.check(t)
}

// A request whose snapshot is older than the version published
// meanwhile starts again: it never builds in full, and the published
// version never goes back.
func TestCacheOlderSnapshotStartsAgain(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	e.commit(0)
	e.mustGet() // the startup self-check's verified apply
	paused, resume := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Keyed: func(string) {
		if once.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}}))
	before := e.mon.builds.Load()
	type result struct {
		lib *views.Library
		err error
	}
	slow := make(chan result, 1)
	go func() {
		lib, err := e.get(context.Background())
		slow <- result{lib, err}
	}()
	<-paused // its snapshot's key is read
	e.commit(0)
	fresh := e.mustGet() // publishes the newer version
	close(resume)
	r := <-slow
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.lib != fresh {
		t.Errorf("the slow request served %+v %+v, not the published %+v %+v", r.lib.Head, r.lib.V.Clock, fresh.Head, fresh.V.Clock)
	}
	if n := e.mon.builds.Load() - before; n != 1 {
		t.Errorf("%d builds, want the fresh request's one apply", n)
	}
	if n := e.mon.path(views.PathFirst) + e.mon.path(views.PathGeneration); n != 1 {
		t.Errorf("%d full builds, want only the first", n)
	}
	e.mon.check(t)
}

// A builder cancelled mid-build publishes nothing and counts no
// failure; its waiters start again and are served. A panicking build
// publishes nothing either: its own request and its waiters start again;
// two in a row purge the account.
func TestCacheBuilderCancelledOrFailing(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	e.commit(0)
	e.mustGet()
	inc := e.spool.Incarnation(e.account)

	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Build: func(string) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}}))
	e.commit(0)
	ctx, cancel := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() {
		_, err := e.get(ctx)
		leader <- err
	}()
	<-entered
	waits := e.mon.waits.Load()
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range 3 {
		wg.Go(func() { _, errs[i] = e.get(context.Background()) })
	}
	waitFor(t, "3 waiters", func() bool { return e.mon.waits.Load() >= waits+3 })
	cancel()
	close(release)
	if err := <-leader; !errors.Is(err, context.Canceled) {
		t.Errorf("the cancelled builder: %v", err)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Errorf("a waiter of a cancelled builder: %v", err)
		}
	}
	if e.spool.Incarnation(e.account) != inc {
		t.Fatal("a cancellation purged the account")
	}

	// One panic: started again, served, not purged.
	var panics atomic.Int32
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Built: func(_ string, _ views.Path, v *views.Version, _ error) *views.Version {
		if panics.Add(-1) >= 0 {
			panic("injected")
		}
		return v
	}}))
	panics.Store(1)
	e.commit(0)
	e.mustGet()
	if e.spool.Incarnation(e.account) != inc {
		t.Fatal("one failed build purged the account")
	}
	// Two in a row: purged, then built in full.
	panics.Store(2)
	e.commit(0)
	full := e.mon.path(views.PathFirst)
	lib := e.mustGet()
	if e.spool.Incarnation(e.account) != inc+1 || views.LibraryIncarnation(lib) != inc+1 {
		t.Errorf("two failed builds: incarnation %d (served under %d), want %d", e.spool.Incarnation(e.account), views.LibraryIncarnation(lib), inc+1)
	}
	if e.mon.path(views.PathFirst) != full+1 || views.Suspect(e.cache, e.account) != 3 {
		t.Errorf("after the purge: %d full builds, suspect %d", e.mon.path(views.PathFirst)-full, views.Suspect(e.cache, e.account))
	}
	e.mon.check(t)
}

// Requests missing together while commits land
// every millisecond, with the warmer on. Every response is its own
// snapshot's library (or busy), never stale; one builder at a time; the
// published version never goes back; no full build but the first.
func TestCacheElectionUnderACommitStorm(t *testing.T) {
	e := newCacheEnv(t)
	e.cache.StartWarmers()
	e.mustGet()
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var served, busy atomic.Int64
	for range 4 {
		wg.Go(func() {
			for ctx.Err() == nil {
				_, err := e.get(ctx)
				switch {
				case err == nil:
					served.Add(1)
				case errors.Is(err, views.ErrBusy):
					busy.Add(1)
				case ctx.Err() != nil:
				default:
					e.t.Error(err)
					return
				}
			}
		})
	}
	// A commit every millisecond for 200 ms, and at least 40 of them (each
	// takes longer than that here).
	deadline := time.Now().Add(200 * time.Millisecond)
	commits := 0
	for time.Now().Before(deadline) || commits < 40 {
		e.commit(0)
		commits++
		time.Sleep(time.Millisecond)
	}
	stop()
	wg.Wait()
	// The warmer catches up with the last commit without a reader.
	head := e.mustGetHead()
	waitFor(t, "the warmer's last publication", func() bool { return e.publishedSeq() >= head })
	t.Logf("%d commits: %d responses, %d busy; builds %d (%d applies, %d exact), %d waits", commits, served.Load(), busy.Load(),
		e.mon.builds.Load(), e.mon.path(views.PathApply), e.mon.path(views.PathExact), e.mon.waits.Load())
	if served.Load() == 0 {
		t.Error("no request was served")
	}
	if n := e.mon.path(views.PathFirst) + e.mon.path(views.PathGeneration); n != 1 {
		t.Errorf("%d full builds, want only the first", n)
	}
	e.mon.check(t)
}

func (e *cacheEnv) mustGetHead() int64 {
	var seq int64
	if err := e.store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(e.account).Head()
		seq = h.Seq
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	return seq
}

func (e *cacheEnv) publishedSeq() int64 {
	e.mon.mu.Lock()
	defer e.mon.mu.Unlock()
	if e.mon.base == nil {
		return 0
	}
	return e.mon.base.Head.Seq
}

// The warmer leads while requests miss (they wait for it and
// are served its version), and advances the version after a commit nobody
// reads.
func TestCacheWarmerLeads(t *testing.T) {
	e := newCacheEnv(t)
	e.cache.StartWarmers()
	e.mustGet()
	e.commit(0)
	waitFor(t, "the warmer's verified apply", func() bool { return e.publishedSeq() == e.mustGetHead() })
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Build: func(string) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}}))
	builds := e.mon.builds.Load()
	e.commit(0)
	<-entered // the warmer builds
	waits := e.mon.waits.Load()
	var wg sync.WaitGroup
	libs := make([]*views.Library, 3)
	for i := range 3 {
		wg.Go(func() {
			var err error
			if libs[i], err = e.get(context.Background()); err != nil {
				e.t.Error(err)
			}
		})
	}
	waitFor(t, "3 requests waiting for the warmer", func() bool { return e.mon.waits.Load() >= waits+3 })
	close(release)
	wg.Wait()
	if n := e.mon.builds.Load() - builds; n != 1 {
		t.Errorf("%d builds for one commit, want the warmer's", n)
	}
	for _, lib := range libs {
		if lib != libs[0] {
			t.Error("the requests were served different libraries")
		}
	}
	// A commit nobody reads: the warmer publishes it.
	e.commit(0)
	head := e.mustGetHead()
	waitFor(t, "the warmer's publication", func() bool { return e.publishedSeq() == head })
	e.mon.check(t)
}

// An account unread for 10 minutes is released (its version and
// warmer go); a use of its views keeps it; its next read builds in full.
func TestCacheIdleRelease(t *testing.T) {
	e := newCacheEnv(t)
	e.cache.StartWarmers()
	e.mustGet()
	e.advance(views.IdleRelease - time.Second)
	e.cache.Touch(e.account)
	e.advance(views.IdleRelease - time.Second)
	e.cache.Sweep()
	if len(e.cache.Accounts()) != 1 {
		t.Fatal("an account whose views were used was released")
	}
	e.advance(views.IdleRelease)
	e.cache.Sweep()
	if len(e.cache.Accounts()) != 0 || views.Reachable(e.cache, e.account) != 0 {
		t.Fatalf("an idle account kept: %v", e.cache.Accounts())
	}
	pubs := e.mon.pubCount()
	e.commit(0) // no warmer any more: nothing is built
	time.Sleep(50 * time.Millisecond)
	if e.mon.pubCount() != pubs {
		t.Error("a released account's commit was built")
	}
	first := e.mon.path(views.PathFirst)
	e.mustGet()
	if e.mon.path(views.PathFirst) != first+1 {
		t.Error("the read after the release did not build in full")
	}
	e.mon.check(t)
}

// The startup self-check: each account's first apply is verified against a
// full build; a difference purges the account, and the request is served
// the full build of a new incarnation.
func TestCacheStartupSelfCheck(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	inc := e.spool.Incarnation(e.account)
	var corrupt atomic.Bool
	corrupt.Store(true)
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Built: func(_ string, p views.Path, v *views.Version, err error) *views.Version {
		if err == nil && p == views.PathApply && corrupt.Swap(false) {
			return omitted(e.mon.lastBase(), v)
		}
		return v
	}}))
	e.commit(0)
	lib := e.mustGet() // served only if right for its snapshot (correct)
	if e.spool.Incarnation(e.account) != inc+1 || views.LibraryIncarnation(lib) != inc+1 || views.Suspect(e.cache, e.account) != 3 {
		t.Fatalf("a wrong first apply: incarnation %d, served under %d, suspect %d", e.spool.Incarnation(e.account), views.LibraryIncarnation(lib),
			views.Suspect(e.cache, e.account))
	}
	// Later applies are not verified once the check passed (and suspect
	// mode cleared): a corruption then stays until Verify finds it.
	for range 3 {
		e.commit(0)
		e.mustGet()
	}
	if s := views.Suspect(e.cache, e.account); s != 0 {
		t.Errorf("suspect mode after 3 verified applies: %d", s)
	}
	e.mon.check(t)
}

// A wrong version V10 is published (an unverified apply), a
// view pins it and V11 is applied from it. Verification (through the
// election) finds the difference: the incarnation rises, the V10 view
// expires and is gone, V11 is dropped, a view of a library taken before the
// purge is refused, and a request is served a full build of the new
// incarnation. A purge while a build is in progress keeps that build from
// publishing. Suspect mode verifies the next 3 applies (a wrong one among
// them purges again).
func TestCachePurgeFencesEverythingDerived(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	e.commit(0)
	e.mustGet() // the startup self-check
	inc := e.spool.Incarnation(e.account)

	var corrupt atomic.Bool
	hold := make(chan struct{})
	var holding atomic.Bool
	entered := make(chan struct{}, 1)
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{
		Build: func(string) {
			if holding.Load() {
				entered <- struct{}{}
				<-hold
			}
		},
		Built: func(_ string, p views.Path, v *views.Version, err error) *views.Version {
			if err == nil && p == views.PathApply && corrupt.Swap(false) {
				return omitted(e.mon.lastBase(), v)
			}
			return v
		},
	}))
	// V10: wrong (the commit's change omitted), published unverified.
	corrupt.Store(true)
	e.commit(0)
	v10, err := e.cache.Get(context.Background(), e.account, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.Read(context.Background(), func(r *storage.ReadTx) error { return e.correct(r, v10) }); err == nil {
		t.Fatal("the corruption did not take")
	}
	id10, err := e.spool.Put(e.account, inc, v10, "q", v10.Queue(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// V11, applied from V10: wrong too.
	e.commit(0)
	if _, err := e.cache.Get(context.Background(), e.account, nil, nil); err != nil {
		t.Fatal(err)
	}
	result, err := e.cache.Verify(context.Background(), e.account)
	if err != nil || result != views.VerifyMismatch {
		t.Fatalf("verification: %q %v", result, err)
	}
	if e.spool.Incarnation(e.account) != inc+1 {
		t.Fatal("the mismatch did not purge the account")
	}
	if n := views.Reachable(e.cache, e.account); n != 0 { // V10's view went with V11
		t.Errorf("%d versions reachable after the purge", n)
	}
	if _, err := e.spool.Read(e.account, id10, "q", 0, 10, nil, v10.Identity()); !errors.Is(err, views.ErrExpired) {
		t.Errorf("a page of the purged V10's view: %v", err)
	}
	// A view of a library taken before the purge is refused.
	if _, err := e.spool.Put(e.account, inc, v10, "q2", v10.Queue(), nil); !errors.Is(err, views.ErrRetired) {
		t.Errorf("a view registered after the purge under the old incarnation: %v", err)
	}
	full := e.mon.path(views.PathFirst)
	if lib := e.mustGet(); views.LibraryIncarnation(lib) != inc+1 || e.mon.path(views.PathFirst) != full+1 {
		t.Fatal("the read after the purge was not a full build of the new incarnation")
	}
	if views.Suspect(e.cache, e.account) != 3 {
		t.Fatalf("suspect %d after the purge", views.Suspect(e.cache, e.account))
	}

	// A purge while a build is in progress: that build is not published.
	holding.Store(true)
	e.commit(0)
	pubs := e.mon.pubCount()
	held := make(chan *views.Library, 1)
	go func() {
		lib, err := e.get(context.Background())
		if err != nil {
			e.t.Error(err)
		}
		held <- lib
	}()
	<-entered
	holding.Store(false)
	e.cache.Purge(e.account)
	full = e.mon.path(views.PathFirst)
	close(hold)
	lib := <-held // correct for its snapshot, or the test failed in get
	if lib == nil || views.LibraryIncarnation(lib) != inc+2 {
		t.Fatal("the held request was not served from the new incarnation")
	}
	if e.mon.path(views.PathFirst) != full+1 || e.mon.pubCount() != pubs+1 {
		t.Errorf("after the purge: %d full builds, %d publications (want 1, the new incarnation's; the held build not published)", e.mon.path(views.PathFirst)-full,
			e.mon.pubCount()-pubs)
	}
	// Suspect mode: the next applies are verified; a wrong one purges again.
	if views.Suspect(e.cache, e.account) != 3 {
		t.Fatalf("suspect %d after the purge", views.Suspect(e.cache, e.account))
	}
	e.commit(0)
	e.mustGet()
	corrupt.Store(true)
	e.commit(0)
	e.mustGet()
	if e.spool.Incarnation(e.account) != inc+3 {
		t.Error("a wrong apply in suspect mode did not purge the account")
	}
	for range 3 {
		e.commit(0)
		e.mustGet()
	}
	if views.Suspect(e.cache, e.account) != 0 {
		t.Errorf("suspect %d after 3 verified applies", views.Suspect(e.cache, e.account))
	}
	if result, err := e.cache.Verify(context.Background(), e.account); err != nil || result != views.VerifyOK {
		t.Errorf("verification of a healthy account: %q %v", result, err)
	}
	e.mon.check(t)
}

// Verification is a build of the election: while another build is in
// progress it waits (never two builds of an account at once, so the
// verification's full build is the flight's slot), then verifies.
func TestCacheVerifyWaitsForTheBuildInProgress(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	e.commit(0)
	e.mustGet()
	entered, release := make(chan struct{}), make(chan struct{})
	var released sync.Once
	t.Cleanup(func() { released.Do(func() { close(release) }) }) // a failure never leaves the build held
	var once atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Build: func(string) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}}))
	e.commit(0)
	go e.mustGet()
	<-entered
	waits := e.mon.waits.Load()
	verified := make(chan string, 1)
	go func() {
		r, err := e.cache.Verify(context.Background(), e.account)
		if err != nil {
			e.t.Error(err)
		}
		verified <- r
	}()
	waitFor(t, "the verification waiting for the build", func() bool { return e.mon.waits.Load() > waits })
	if n := views.Reachable(e.cache, e.account); n > 2 { // the published version and the build's slot
		t.Errorf("%d versions reachable during a build and a verification", n)
	}
	released.Do(func() { close(release) })
	if r := <-verified; r != views.VerifyOK {
		t.Errorf("verification after the build: %q", r)
	}
	e.mon.check(t)
}

// Whatever the schedule (views pinned at every age, large payloads
// that shrink, generation changes), the versions an account keeps reachable
// stay within the structural bound (MaxVersions), and so does the heap.
func TestCacheMemoryIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates hundreds of megabytes")
	}
	e := newCacheEnv(t)
	// The large payload is always the same episode's: the library holds one.
	const big = 1 << 20
	heavy := e.episodes[1]
	e.commitTo(heavy, big)
	lib := e.mustGet()
	ranges := 0
	for range lib.V.AllPodcasts() {
		ranges += views.NumClasses
	}
	libBytes, base := e.measureOne()

	var tokens []string
	maxReachable := 0
	for step := range 30 {
		switch {
		case step == 20:
			e.raiseGeneration()
		case step%5 == 4:
			e.commitTo(heavy, 16) // the payload shrinks
		default:
			e.commitTo(heavy, big)
		}
		lib := e.mustGet()
		id, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), lib, fmt.Sprintf("q%d", step), lib.Queue(), nil)
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, id)
		if n := views.Reachable(e.cache, e.account); n > views.MaxVersions {
			t.Fatalf("step %d: %d versions reachable", step, n)
		} else {
			maxReachable = max(maxReachable, n)
		}
		// Every tenth step (a read of a dead view drops it, so the others
		// leave every view in place): the view of the version before is
		// still read; one beyond the published, the previous and the two
		// leased versions has expired.
		if step >= 4 && step%10 == 9 {
			if _, err := e.spool.Read(e.account, tokens[step-4], fmt.Sprintf("q%d", step-4), 0, 5, nil, lib.Identity()); !errors.Is(err, views.ErrExpired) {
				t.Fatalf("step %d: a view four versions old: %v", step, err)
			}
			if _, err := e.spool.Read(e.account, tokens[step-1], fmt.Sprintf("q%d", step-1), 0, 5, nil, lib.Identity()); err != nil {
				t.Fatalf("step %d: the view of the version before: %v", step, err)
			}
		}
	}
	e.checkHeap(base, libBytes, ranges, maxReachable)
	e.mon.check(t)
}

// measureOne measures one library's heap (a full build of the account now)
// and the heap without any cached library (the account purged).
func (e *cacheEnv) measureOne() (libBytes, base uint64) {
	e.t.Helper()
	var one *views.Version
	h0 := heapNow()
	if err := e.store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		one, err = views.BuildVersion(r.Account(e.account), e.account)
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	libBytes = heapNow() - h0
	runtime.KeepAlive(one)
	one = nil
	e.cache.Purge(e.account) // the measurement starts without a cached library
	return libBytes, heapNow()
}

// checkHeap is the hard heap gate: what the cache, the registry and the
// admitted responses hold stays within 1.2 × (MaxVersions libraries + the
// views' plans).
func (e *cacheEnv) checkHeap(base, libBytes uint64, ranges, maxReachable int) {
	e.t.Helper()
	used := heapNow() - base
	bound := uint64(1.2 * float64(uint64(views.MaxVersions)*libBytes+uint64(64*(40*ranges+1024))))
	e.t.Logf("one library %d bytes; %d bytes held (bound %d); at most %d versions reachable", libBytes, used, bound, maxReachable)
	if used > bound {
		e.t.Errorf("the cache, views and responses hold %d bytes, over the bound %d", used, bound)
	}
}

func heapNow() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// pauseKeyed has the next snapshot's election wait at the Keyed hook until
// the returned function is called; paused is closed when it waits.
func (e *cacheEnv) pauseKeyed(extra *views.CacheHooks) (paused <-chan struct{}, resume func()) {
	p, r := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	x := views.CacheHooks{}
	if extra != nil {
		x = *extra
	}
	x.Keyed = func(string) {
		if once.CompareAndSwap(false, true) {
			close(p)
			<-r
		}
	}
	e.cache.HookForTesting(e.mon.hooks(&x))
	return p, func() { close(r) }
}

// A snapshot pinned before a generation change (whose write returned,
// so the account was retired) never takes on the new incarnation: it starts
// again and is served the new generation.
func TestCacheSnapshotBeforeAGenerationChangeStartsAgain(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	paused, resume := e.pauseKeyed(nil)
	got := make(chan *views.Library, 1)
	go func() {
		lib, err := e.get(context.Background())
		if err != nil {
			e.t.Error(err)
		}
		got <- lib
	}()
	<-paused
	before := e.spool.Incarnation(e.account)
	e.raiseGeneration()
	inc := e.spool.Incarnation(e.account)
	if inc == before {
		t.Fatal("the generation change did not retire the account")
	}
	resume()
	lib := <-got
	if lib == nil {
		t.Fatal("no library")
	}
	var gen int64
	if err := e.store.Read(context.Background(), func(r *storage.ReadTx) error {
		c, err := r.Account(e.account).ViewClock()
		gen = c.Generation
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if lib.V.Clock.Generation != gen || views.LibraryIncarnation(lib) != inc || !e.cache.Valid(e.account, lib) {
		t.Errorf("served generation %d under incarnation %d; the account is at generation %d, incarnation %d", lib.V.Clock.Generation,
			views.LibraryIncarnation(lib), gen, inc)
	}
	e.mon.check(t)
}

// A snapshot pinned before the account's deletion is never published:
// the warm-up's attempt starts again and finds no account.
func TestCacheSnapshotBeforeTheAccountsDeletionIsNotPublished(t *testing.T) {
	e := newCacheEnv(t)
	paused, resume := e.pauseKeyed(nil)
	warmed := make(chan error, 1)
	go func() { warmed <- e.cache.Warm(context.Background(), e.account) }()
	<-paused
	if err := e.store.Write(context.Background(), func(tx *storage.WriteTx) error { return tx.DeleteAccount(e.account) }); err != nil {
		t.Fatal(err)
	}
	resume()
	err := <-warmed
	if got := e.cache.Accounts(); len(got) != 0 || e.mon.pubCount() != 0 {
		t.Errorf("a deleted account's library was published (%v, %d publications; warm-up: %v)", got, e.mon.pubCount(), err)
	}
}

// Work rejected by a purge (the startup check's verified apply, purged
// while it was compared) moves no counter: suspect mode still needs 3.
func TestCacheRejectedApplyDoesNotDischargeSuspectMode(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	var once atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Built: func(_ string, p views.Path, v *views.Version, err error) *views.Version {
		if err == nil && p == views.PathApply && once.CompareAndSwap(false, true) {
			e.cache.Purge(e.account)
		}
		return v
	}}))
	e.commit(0)
	e.mustGet() // its apply is rejected; it is served a full build of the new incarnation
	if s := views.Suspect(e.cache, e.account); s != 3 {
		t.Errorf("suspect %d after a rejected verified apply, want 3", s)
	}
	e.mon.check(t)
}

// headOnly appends a commit that changes no view input.
func (e *cacheEnv) headOnly() {
	e.t.Helper()
	if err := e.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		_, _, err := tx.Account(e.account).AppendCommit(storage.CommitBody{})
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
}

// A head-only commit verifies nothing: after a purge, three of them
// leave suspect mode at 3, and the next real apply is verified (a wrong one
// is found); the startup check survives a head-only first commit.
func TestCacheHeadOnlyCommitsVerifyNothing(t *testing.T) {
	e := newCacheEnv(t)
	e.mustGet()
	var corrupt atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Built: func(_ string, p views.Path, v *views.Version, err error) *views.Version {
		if err == nil && p == views.PathApply && corrupt.Swap(false) {
			return omitted(e.mon.lastBase(), v)
		}
		return v
	}}))
	// The startup check, after a head-only first commit.
	e.headOnly()
	e.mustGet()
	inc := e.spool.Incarnation(e.account)
	corrupt.Store(true)
	e.commit(0)
	e.mustGet()
	if e.spool.Incarnation(e.account) != inc+1 {
		t.Fatal("a head-only first commit used up the startup check: a wrong first apply was not found")
	}
	// Suspect mode, after three head-only commits.
	for range 3 {
		e.headOnly()
		e.mustGet()
	}
	if s := views.Suspect(e.cache, e.account); s != 3 {
		t.Fatalf("suspect %d after three head-only commits, want 3", s)
	}
	corrupt.Store(true)
	e.commit(0)
	e.mustGet()
	if e.spool.Incarnation(e.account) != inc+2 {
		t.Error("the first real apply after head-only commits was not verified")
	}
	e.mon.check(t)
}

// Close owns the startup warm-up: it cancels it and waits for it, and
// nothing is published after it returns.
func TestCacheCloseWaitsForWarm(t *testing.T) {
	e := newCacheEnv(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Build: func(string) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}}))
	warmed := make(chan error, 1)
	go func() { warmed <- e.cache.Warm(context.Background(), e.account) }()
	<-entered
	closed := make(chan struct{})
	go func() {
		e.cache.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while the warm-up was building")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-closed
	pubs := e.mon.pubCount()
	if err := <-warmed; err == nil {
		t.Error("a warm-up cut short by Close reported no error")
	}
	if pubs != 0 {
		t.Errorf("%d publications by a warm-up Close cut short", pubs)
	}
	if _, err := e.get(context.Background()); !errors.Is(err, views.ErrClosed) {
		t.Errorf("a read after Close: %v", err)
	}
}

// Pages being encoded are admitted (at most 2 per account, before the
// view is looked up), so successive slow pages, each of a version since
// replaced, cannot keep more versions reachable than the bound: the others
// wait holding nothing. The heap gate holds with the payload shrunk.
func TestCachePausedPagesStayWithinTheBound(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of megabytes")
	}
	e := newCacheEnv(t)
	views.SetHoldWaitForTesting(e.spool, 5*time.Minute)
	const big = 1 << 20
	heavy := e.episodes[1]
	e.commitTo(heavy, big)
	lib := e.mustGet()
	ranges := 0
	for range lib.V.AllPodcasts() {
		ranges += views.NumClasses
	}
	libBytes, base := e.measureOne()
	release := make(chan struct{})
	var encoding atomic.Int32
	e.spool.HookPagesForTesting(func() {
		encoding.Add(1)
		<-release
	})
	var wg sync.WaitGroup
	maxReachable := 0
	for i := range 16 {
		e.commitTo(heavy, big)
		lib := e.mustGet()
		id, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), lib, fmt.Sprintf("p%d", i), lib.Queue(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ident := lib.Identity() // the reader holds the view's ID and identity only
		wg.Go(func() { _, _ = e.spool.Read(e.account, id, fmt.Sprintf("p%d", i), 0, 5, nil, ident) })
		if i < 2 {
			waitFor(t, "a page being encoded", func() bool { return encoding.Load() == int32(i+1) })
		}
		if n := views.Reachable(e.cache, e.account); n > views.MaxVersions {
			t.Fatalf("page %d: %d versions reachable", i, n)
		} else {
			maxReachable = max(maxReachable, n)
		}
	}
	lib = nil
	e.commitTo(heavy, 16) // the payload shrinks
	e.mustGet()
	if n := encoding.Load(); n != 2 {
		t.Errorf("%d pages encoded at once, want 2", n)
	}
	e.checkHeap(base, libBytes, ranges, maxReachable)
	close(release)
	wg.Wait()
	e.mon.check(t)
}

// A list held open survives the commits of a playing phone: its version
// is leased while its view is read in the last 10 minutes, beyond the
// published version and the one before it; at most 2 leased versions, the
// most recently read first; the lease ends after 10 minutes unread.
func TestCacheLeasedViewsSurvivePlayback(t *testing.T) {
	e := newCacheEnv(t)
	put := func(name string) (string, *views.Library) {
		lib := e.mustGet()
		id, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), lib, name, lib.Queue(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return id, lib
	}
	read := func(id, name string) error {
		_, err := e.spool.Read(e.account, id, name, 0, 5, nil, e.mustGet().Identity())
		return err
	}
	a, _ := put("a")
	for i := range 5 { // five playback checkpoints, the held list read between them
		e.commit(0)
		e.mustGet()
		e.advance(time.Minute)
		if err := read(a, "a"); err != nil {
			t.Fatalf("the held list after %d checkpoints: %v", i+1, err)
		}
		if n := views.Reachable(e.cache, e.account); n > views.MaxVersions {
			t.Fatalf("%d versions reachable", n)
		}
	}
	// Two more held lists of later versions, read after a: a is the least
	// recently read of three leased versions, and goes.
	e.commit(0)
	b, _ := put("b")
	e.commit(0)
	c, _ := put("c")
	e.advance(time.Second)
	if err := read(b, "b"); err != nil {
		t.Fatal(err)
	}
	if err := read(c, "c"); err != nil {
		t.Fatal(err)
	}
	e.commit(0)
	e.mustGet()
	e.commit(0)
	e.mustGet()
	if err := read(a, "a"); !errors.Is(err, views.ErrExpired) {
		t.Errorf("a third leased version: %v", err)
	}
	if err := read(b, "b"); err != nil {
		t.Errorf("a leased version: %v", err)
	}
	if err := read(c, "c"); err != nil {
		t.Errorf("a leased version: %v", err)
	}
	// The lease ends 10 minutes after the last read.
	e.advance(views.IdleExpiry + time.Second)
	e.commit(0)
	e.mustGet()
	e.commit(0)
	e.mustGet()
	if err := read(b, "b"); !errors.Is(err, views.ErrExpired) {
		t.Errorf("a view unread for 10 minutes: %v", err)
	}
	e.mon.check(t)
}

// An account released to make room for others (more than 8) takes its
// views with it: its old token is 410 and it keeps no version reachable.
func TestCacheEvictionReleasesViews(t *testing.T) {
	e := newCacheEnv(t)
	lib := e.mustGet()
	id, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), lib, "q", lib.Queue(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 8 {
		e.advance(time.Second)
		var other string
		if err := e.store.Write(ctx, func(tx *storage.WriteTx) error {
			a, err := tx.CreateAccount()
			other = a.ID
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := viewstest.Generate(ctx, e.store, other, viewstest.Spec{Seed: uint64(10 + i), Podcasts: []int{2}, Now: viewstest.FixedNow, Months: 2,
			Prefix: fmt.Sprintf("x%d", i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.cache.Get(ctx, other, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if slices := e.cache.Accounts(); len(slices) != 8 || contains(slices, e.account) {
		t.Fatalf("cached %v", slices)
	}
	if _, err := e.spool.Read(e.account, id, "q", 0, 5, nil, lib.Identity()); !errors.Is(err, views.ErrExpired) {
		t.Errorf("a released account's token: %v", err)
	}
	if n := views.Reachable(e.cache, e.account); n != 0 {
		t.Errorf("a released account keeps %d versions reachable", n)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// The power user's library (NTP_READMODEL_SCALE=1): 100 podcasts
// of 500 episodes over 20 years. Four readers and the warmer during 100
// position commits: no busy answer, no full build after the first verified
// apply, every response its own snapshot's version, a p95 read budget, and
// the heap of the cached versions within 3 libraries (they share their
// structure).
func TestCacheElectionAtScale(t *testing.T) {
	if os.Getenv("NTP_READMODEL_SCALE") == "" {
		t.Skip("set NTP_READMODEL_SCALE=1 for the power user's library")
	}
	pods := make([]int, 100)
	for i := range pods {
		pods[i] = 500
	}
	e := newCacheEnvWith(t, viewstest.Spec{Seed: 7, Podcasts: pods, Queue: 100, Now: viewstest.FixedNow, Months: 240, Prefix: "s"})
	libBytes, base := e.measureOne()
	ctx := context.Background()
	// Each response is checked against its own snapshot's key (a full build
	// per read would be the test's own cost, not the server's).
	keyed := func(r *storage.ReadTx, lib *views.Library) error {
		acct := r.Account(e.account)
		head, err := acct.Head()
		if err != nil {
			return err
		}
		clock, err := acct.ViewClock()
		if err != nil {
			return err
		}
		if lib.Head.Seq != head.Seq || lib.V.Clock != clock {
			return fmt.Errorf("served %+v %+v at a snapshot of %d %+v", lib.Head.Seq, lib.V.Clock, head.Seq, clock)
		}
		return nil
	}
	if _, err := e.cache.Get(ctx, e.account, nil, keyed); err != nil {
		t.Fatal(err)
	}
	e.commit(0)
	if _, err := e.cache.Get(ctx, e.account, nil, keyed); err != nil { // the verified first apply
		t.Fatal(err)
	}
	e.cache.StartWarmers()
	before := views.Work()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var busy atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				_, err := e.cache.Get(ctx, e.account, nil, keyed)
				d := time.Since(start)
				switch {
				case errors.Is(err, views.ErrBusy):
					busy.Add(1)
				case err != nil:
					e.t.Error(err)
					return
				}
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		})
	}
	for range 100 {
		e.commit(0)
	}
	close(stop)
	wg.Wait()
	head := e.mustGetHead()
	waitFor(t, "the warmer's last publication", func() bool { return e.publishedSeq() >= head })
	w := views.Work().Minus(before)
	slices.Sort(lat)
	p95 := lat[len(lat)*95/100]
	t.Logf("%d responses, %d busy; p50 %v, p95 %v, max %v; %+v", len(lat), busy.Load(), lat[len(lat)/2], p95, lat[len(lat)-1], w)
	if busy.Load() != 0 {
		t.Errorf("%d busy answers", busy.Load())
	}
	if w.FullBuilds != 0 || e.mon.path(views.PathGeneration) != 0 {
		t.Errorf("%d full builds after the first verified apply", w.FullBuilds)
	}
	if p95 > 250*time.Millisecond {
		t.Errorf("p95 read %v, over the 250 ms budget", p95)
	}
	used := heapNow() - base
	bound := uint64(1.2 * float64(3*libBytes))
	t.Logf("one library %d bytes; the cache holds %d (bound %d)", libBytes, used, bound)
	if used > bound {
		t.Errorf("the cached versions hold %d bytes, over 3 libraries' bound %d", used, bound)
	}
	// The last version equals a full build of its snapshot.
	lib, err := e.cache.Get(ctx, e.account, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.Read(ctx, func(r *storage.ReadTx) error { return e.correct(r, lib) }); err != nil {
		t.Error(err)
	}
	e.mon.check(t)
	worstHeldState(t, e, base, libBytes, func() { e.commit(0) }, 2.5)
}

// worstHeldState measures the worst state an account's library can reach:
// two admitted page encoders paused on old versions, two leased
// snapshots, the version before the published one, the published one, and a
// verified build in progress whose comparison is a full, unshared build,
// each version one commit from the next. The live heap at that peak stays
// within budget libraries: with one realistic (one-episode) commit between
// versions, 2.5 (the published version, the comparison build and the
// versions' shared deltas); with broad commits, the structural bound.
func worstHeldState(t *testing.T, e *cacheEnv, base, libBytes uint64, commit func(), budget float64) {
	ctx := context.Background()
	views.SetHoldWaitForTesting(e.spool, time.Hour)
	step := make(chan struct{})
	var encoding atomic.Int32
	e.spool.HookPagesForTesting(func() {
		encoding.Add(1)
		<-step
	})
	var peak uint64
	reachable := 0
	var sampling atomic.Bool
	e.cache.HookForTesting(e.mon.hooks(&views.CacheHooks{Compared: func(string) {
		if sampling.Load() {
			peak, reachable = heapNow(), views.Reachable(e.cache, e.account)
		}
	}}))
	pin := func(name string) (string, views.Identity) {
		commit()
		lib, err := e.cache.Get(ctx, e.account, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		id, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), lib, name, lib.Queue(), nil)
		if err != nil {
			t.Fatal(err)
		}
		e.advance(time.Second)
		return id, lib.Identity()
	}
	var wg sync.WaitGroup
	for i := range 2 { // two encoders paused on the oldest versions
		id, ident := pin(fmt.Sprintf("old%d", i))
		wg.Go(func() { _, _ = e.spool.Read(e.account, id, fmt.Sprintf("old%d", i), 0, 100, nil, ident) })
		waitFor(t, "a page being made", func() bool { return encoding.Load() == int32(i+1) })
	}
	pin("leased0") // two leased snapshots
	pin("leased1")
	pin("before") // the version before the published one
	pin("published")
	commit() // a verified build of it, through the election
	sampling.Store(true)
	result, err := e.cache.Verify(ctx, e.account)
	if err != nil || result != views.VerifyOK {
		t.Fatalf("verification: %q %v", result, err)
	}
	close(step)
	wg.Wait()
	used := peak - base
	t.Logf("worst held state: %d versions reachable; one library %d bytes; %d bytes held at the peak, %.2f libraries (budget %.1f)", reachable, libBytes,
		used, float64(used)/float64(libBytes), budget)
	if reachable < 7 {
		t.Errorf("the peak was taken with %d versions reachable, not the worst state", reachable)
	}
	if float64(used) > budget*float64(libBytes) {
		t.Errorf("the worst held state holds %.2f libraries, over %.1f", float64(used)/float64(libBytes), budget)
	}
}

// What an account's library holds after broad changes (PROTOCOL §14): when
// commits each change the whole library's played state, versions share
// little but the catalog, so the account holds about one library per
// version it keeps. With two leased lists open (the published version, the
// one before and the two leased versions) that is about 4 libraries; in the
// worst held state (worstHeldState) about 8, its structural bound
// (MaxVersions). NTP_READMODEL_SCALE=1 runs it at 100,000 episodes.
func TestCacheBroadChangesHeldState(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates several libraries")
	}
	per := 20
	if os.Getenv("NTP_READMODEL_SCALE") != "" {
		per = 1000
	}
	pods := make([]int, 100)
	for i := range pods {
		pods[i] = per
	}
	e := newCacheEnvWith(t, viewstest.Spec{Seed: 7, Podcasts: pods, Queue: 100, Now: viewstest.FixedNow, Months: 240, Prefix: "b"})
	libBytes, base := e.measureOne()
	ctx := context.Background()
	if _, err := e.cache.Get(ctx, e.account, nil, nil); err != nil {
		t.Fatal(err)
	}
	for i := range 4 { // two leased lists, the version before the published one, the published one
		e.commitAll()
		lib, err := e.cache.Get(ctx, e.account, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), lib, fmt.Sprintf("list%d", i), lib.Queue(), nil); err != nil {
			t.Fatal(err)
		}
		e.advance(time.Second)
	}
	used, reachable := heapNow()-base, views.Reachable(e.cache, e.account)
	t.Logf("two leased lists after broad changes: %d versions reachable; one library %d bytes; %d bytes held, %.2f libraries (budget 4.5)",
		reachable, libBytes, used, float64(used)/float64(libBytes))
	if reachable != 4 {
		t.Errorf("%d versions reachable, not the published, the one before and two leased", reachable)
	}
	if float64(used) > 4.5*float64(libBytes) {
		t.Errorf("two leased lists after broad changes hold %.2f libraries, over 4.5", float64(used)/float64(libBytes))
	}
	worstHeldState(t, e, base, libBytes, e.commitAll, float64(views.MaxVersions)+0.5)
	e.mon.check(t)
}

// The bound on accounts is global: an account released while one of its
// responses is still being made still holds its library and counts. With
// eight accounts' responses being made, a ninth account's read waits until
// one ends; no account's source is freed or invalidated while in use.
func TestCacheHeldAccountsStayWithinTheBound(t *testing.T) {
	e := newCacheEnv(t)
	views.SetHoldWaitForTesting(e.spool, 5*time.Minute)
	ctx := context.Background()
	accounts := []string{e.account}
	for i := range 8 {
		var id string
		if err := e.store.Write(ctx, func(tx *storage.WriteTx) error {
			a, err := tx.CreateAccount()
			id = a.ID
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := viewstest.Generate(ctx, e.store, id, viewstest.Spec{Seed: uint64(20 + i), Podcasts: []int{2}, Now: viewstest.FixedNow, Months: 2,
			Prefix: fmt.Sprintf("h%d", i)}); err != nil {
			t.Fatal(err)
		}
		accounts = append(accounts, id)
	}
	step := make(chan struct{})
	var encoding atomic.Int32
	e.spool.HookPagesForTesting(func() {
		encoding.Add(1)
		<-step
	})
	var wg sync.WaitGroup
	pages := make([]error, 8)
	for i, a := range accounts[:8] {
		e.advance(time.Second)
		lib, err := e.cache.Get(ctx, a, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		id, err := e.spool.Put(a, e.spool.Incarnation(a), lib, "q", lib.Queue(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ident := lib.Identity()
		wg.Go(func() { _, pages[i] = e.spool.Read(a, id, "q", 0, 5, nil, ident) })
		waitFor(t, "a page being made", func() bool { return encoding.Load() == int32(i+1) })
	}
	if n := views.HeldAccounts(e.cache); n != 8 {
		t.Fatalf("%d accounts hold a library", n)
	}
	got := make(chan error, 1)
	go func() {
		_, err := e.cache.Get(ctx, accounts[8], nil, nil)
		got <- err
	}()
	select {
	case err := <-got:
		t.Fatalf("a ninth account was read while eight accounts' responses were being made (%v): %d accounts hold a library", err, views.HeldAccounts(e.cache))
	case <-time.After(300 * time.Millisecond):
	}
	step <- struct{}{} // one response ends: its account can go
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	if n := views.HeldAccounts(e.cache); n > 8 {
		t.Errorf("%d accounts hold a library", n)
	}
	close(step)
	wg.Wait()
	for i, err := range pages {
		if err != nil {
			t.Errorf("account %d's page, made while its account was cached: %v", i, err)
		}
	}
}

// A held list whose view is read once a minute (the client renews its
// lease while a selection is active) survives a second window registering a
// fresh view at every checkpoint, for 12 checkpoints.
func TestCacheHeldViewReadEveryMinuteSurvivesAnotherWindow(t *testing.T) {
	e := newCacheEnv(t)
	lib := e.mustGet()
	held, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), lib, "held", lib.Queue(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ident := lib.Identity()
	read := func() error {
		_, err := e.spool.Read(e.account, held, "held", 0, 5, nil, ident)
		return err
	}
	for i := range 12 {
		e.commit(0)
		cur := e.mustGet()
		window := func() {
			if _, err := e.spool.Put(e.account, e.spool.Incarnation(e.account), cur, fmt.Sprintf("w%d", i), cur.Queue(), nil); err != nil {
				t.Fatal(err)
			}
		}
		if i%2 == 0 { // the other window refreshes before the held list is read, or after
			window()
		}
		if err := read(); err != nil {
			t.Fatalf("checkpoint %d: the held list: %v", i+1, err)
		}
		if i%2 == 1 {
			window()
		}
		if n := views.Reachable(e.cache, e.account); n > views.MaxVersions {
			t.Fatalf("%d versions reachable", n)
		}
		e.advance(time.Minute)
	}
	e.mon.check(t)
}

// With a restore and without change capture: a snapshot pinned before a
// retirement (whose write returned) is never served under the new
// incarnation. The pause is in the caller's check, after the snapshot is
// pinned (its first read).
func TestCacheSnapshotBeforeARetirementInBothCaptureModes(t *testing.T) {
	for _, capture := range []bool{true, false} {
		for _, change := range []string{"generation", "restore"} {
			t.Run(fmt.Sprintf("capture=%v/%s", capture, change), func(t *testing.T) {
				e := newCacheEnv(t)
				views.SetCaptureForTesting(e.cache, capture)
				e.mustGet()
				paused, resume := make(chan struct{}), make(chan struct{})
				var once atomic.Bool
				check := func(r *storage.ReadTx) error {
					if _, err := r.Account(e.account).Head(); err != nil { // pins the snapshot
						return err
					}
					if once.CompareAndSwap(false, true) {
						close(paused)
						<-resume
					}
					return nil
				}
				got := make(chan *views.Library, 1)
				go func() {
					lib, err := e.cache.Get(context.Background(), e.account, check, nil)
					if err != nil {
						e.t.Error(err)
					}
					got <- lib
				}()
				<-paused
				switch change {
				case "generation":
					e.raiseGeneration()
				case "restore":
					if err := e.store.Write(context.Background(), func(tx *storage.WriteTx) error { return tx.RestoreTransition("a test") }); err != nil {
						t.Fatal(err)
					}
				}
				close(resume)
				lib := <-got
				if lib == nil {
					t.Fatal("no library")
				}
				var head storage.Head
				var clock storage.ViewClock
				if err := e.store.Read(context.Background(), func(r *storage.ReadTx) error {
					var err error
					if head, err = r.Account(e.account).Head(); err != nil {
						return err
					}
					clock, err = r.Account(e.account).ViewClock()
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if lib.Head.Epoch != head.Epoch || lib.V.Clock.Generation != clock.Generation || !e.cache.Valid(e.account, lib) {
					t.Errorf("served epoch %d generation %d (valid %v); the account is at epoch %d generation %d", lib.Head.Epoch, lib.V.Clock.Generation,
						e.cache.Valid(e.account, lib), head.Epoch, clock.Generation)
				}
			})
		}
	}
}
