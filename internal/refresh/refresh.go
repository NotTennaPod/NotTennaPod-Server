// Package refresh is the server's own feed refresh (PROTOCOL §9.6): which
// podcasts it fetches and when, bounded concurrency, failure backoff and Retry-After,
// coalescing of manual requests, validators, refresh_status, and the re-parse of stored
// responses after a parser upgrade. The catalog effects of a fetch are ops.ApplyServerFetch's.
package refresh

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	randv2 "math/rand/v2"
	"net/url"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Fetcher fetches feeds; *feed.Fetcher is the production one.
type Fetcher interface {
	Fetch(ctx context.Context, req feed.FetchRequest) (*feed.FetchResult, error)
}

// Clock is the refresher's time source. Due times are compared as wall-clock
// milliseconds, so a wake from sleep shows as overdue work.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// Monotonic is a clock whose monotonic time can be read apart from its wall
// time. While a Mac sleeps the wall clock moves on and the monotonic one does
// not, and a manual clock change moves only the wall clock: a difference
// between the two is a clock jump (PROTOCOL §14).
type Monotonic interface {
	Elapsed() time.Duration
}

type realClock struct{}

var processStart = time.Now()

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) Elapsed() time.Duration                 { return time.Since(processStart) }

// Config configures a Refresher; zero fields take the defaults.
type Config struct {
	Interval    time.Duration  // between scheduled fetches of a podcast, ±10 % jitter (default 1 h)
	Concurrency int            // fetches at a time (default 4)
	PerHost     int            // fetches at a time per host (default 1)
	PerAccount  int            // fetches at a time per account while another account's work waits (default Concurrency/2, at least 1)
	SyncEvery   time.Duration  // how often subscriptions are rescanned (default 30 s)
	Spread      time.Duration  // overdue work after a restart or wake is spread over this (default min(10 min, Interval/6))
	Fetcher     Fetcher        // default feed.NewFetcher()
	FetchLimit  time.Duration  // one fetch and its parse, together (default feed.FetchLimit)
	Clock       Clock          // default the system clock
	Random      func() float64 // [0, 1), safe for concurrent use (default math/rand/v2)
	Logger      *slog.Logger
}

// Schedule constants (PROTOCOL §9.4).
const (
	jitterFraction = 0.1
	backoffBase    = 15 * time.Minute
	backoffMax     = 24 * time.Hour
	retryAfterMax  = 30 * 24 * time.Hour // the only limit on Retry-After (PROTOCOL §14)
	wakeSlack      = time.Minute         // a pass later than SyncEvery by more than this follows a sleep
)

type key struct{ account, podcast string }

type job struct{ host string }

// Refresher runs the server's feed refreshes.
type Refresher struct {
	svc *protocol.Service
	cfg Config
	log *slog.Logger

	mu         sync.Mutex
	running    map[key]*job
	hosts      map[string]int
	manual     map[key]bool   // requested through the API; true: forced (no validators)
	contexts   map[key]string // the fetch context (generation, URL, credentials) last seen
	finished   map[key]int64  // finishSeq when the key's last fetch ended
	finishSeq  int64
	served     map[string]int64 // account: startSeq when its last fetch started (accounts take turns)
	startSeq   int64
	wake       chan struct{}
	jobs       sync.WaitGroup
	cancel     context.CancelFunc
	done       chan struct{}
	afterPass  func() // tests: after each pass of the loop
	lastPassMs int64
	lastMono   time.Duration // the clock's monotonic time at the last pass, when it has one
	lastWake   int64         // ms: the last pass after a wake or clock jump (the service page)
}

// New returns a refresher for the service's accounts. Start runs it.
func New(svc *protocol.Service, cfg Config) *Refresher {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.PerHost <= 0 {
		cfg.PerHost = 1
	}
	if cfg.PerAccount <= 0 {
		cfg.PerAccount = max(1, cfg.Concurrency/2)
	}
	if cfg.SyncEvery <= 0 {
		cfg.SyncEvery = 30 * time.Second
	}
	if cfg.Spread <= 0 {
		cfg.Spread = min(10*time.Minute, cfg.Interval/6)
	}
	if cfg.Fetcher == nil {
		cfg.Fetcher = feed.NewFetcher()
	}
	if cfg.FetchLimit <= 0 {
		cfg.FetchLimit = feed.FetchLimit
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.Random == nil {
		cfg.Random = randv2.Float64
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Refresher{svc: svc, cfg: cfg, log: log, running: map[key]*job{}, hosts: map[string]int{}, manual: map[key]bool{},
		contexts: map[key]string{}, finished: map[key]int64{}, served: map[string]int64{}, wake: make(chan struct{}, 1)}
}

// Start re-parses stored responses whose parser version is outdated, then runs
// the schedule until ctx ends or Stop is called.
func (r *Refresher) Start(ctx context.Context) {
	ctx, r.cancel = context.WithCancel(ctx)
	r.done = make(chan struct{})
	go r.run(ctx)
}

// Stop stops the refresher and waits for its fetches to end. A fetch cut short
// records nothing.
func (r *Refresher) Stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	<-r.done
}

func (r *Refresher) run(ctx context.Context) {
	defer close(r.done)
	defer r.jobs.Wait()
	// A commit that opens a generation wakes the loop: its first population
	// is fetched at once, not at the next scan (PROTOCOL §9.4).
	opened, stopOpened := r.svc.Store.SubscribeGenerations()
	defer stopOpened()
	r.ReparseAll(ctx)
	for ctx.Err() == nil {
		now := r.nowMs()
		// The first pass, or one long after the last (a wake from sleep, a
		// stalled process), spreads the overdue work (PROTOCOL §14).
		first := r.lastPassMs == 0
		spread := first || now-r.lastPassMs > (r.cfg.SyncEvery+wakeSlack).Milliseconds()
		// A wall clock that moved apart from the monotonic one (a sleep, a
		// manual change) is logged, and deadlines it left far ahead are
		// recomputed, as at a start (PROTOCOL §14).
		jumped := false
		if m, ok := r.cfg.Clock.(Monotonic); ok {
			mono := m.Elapsed()
			if !first {
				wall := time.Duration(now-r.lastPassMs) * time.Millisecond
				if d := wall - (mono - r.lastMono); d > wakeSlack || d < -wakeSlack {
					jumped = true
					r.log.Info("refresh: the clock jumped (a sleep or a clock change)", "wall", wall.Round(time.Second), "monotonic", (mono - r.lastMono).Round(time.Second))
				}
			}
			r.lastMono = mono
		}
		if !first && (spread || jumped) {
			r.mu.Lock()
			r.lastWake = now
			r.mu.Unlock()
		}
		r.lastPassMs = now
		res, err := r.pass(ctx, spread, first || jumped)
		wait := res.wait
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.log.Error("refresh: schedule pass failed", "error", err)
			wait = r.cfg.SyncEvery
		}
		if r.afterPass != nil {
			r.afterPass()
		}
		select {
		case <-ctx.Done():
		case <-r.cfg.Clock.After(wait):
		case <-r.wake:
		case <-opened:
		}
	}
}

func (r *Refresher) nowMs() int64 { return r.cfg.Clock.Now().UnixMilli() }

// signal wakes the loop (a fetch ended, or a request arrived).
func (r *Refresher) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Refresher) jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (1 - jitterFraction + 2*jitterFraction*r.cfg.Random()))
}

// backoff is the delay after the n-th consecutive failure: 15 minutes,
// doubling up to 24 hours, with jitter.
func (r *Refresher) backoff(n int64) time.Duration {
	d := backoffBase
	for i := int64(1); i < n && d < backoffMax; i++ {
		d *= 2
	}
	return min(r.jitter(min(d, backoffMax)), backoffMax)
}

// fetchContext identifies what a podcast's fetches use: a change schedules it
// now and clears its backoff (validators are dropped by the stored response's
// source and representation key).
func fetchContext(c storage.RefreshCandidate) string {
	return c.GenerationID + "\x00" + c.RefreshURL + "\x00" + c.SecretID
}

func contextURL(fc string) string {
	parts := strings.Split(fc, "\x00")
	if len(parts) != 3 {
		return ""
	}
	return parts[1]
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// isLive: the podcast has a live generation (subscribed, archived or a
// preview). A removed podcast has none, and a merged one (PROTOCOL §9.7) is only a
// redirect to its survivor, which is fetched instead.
func isLive(c storage.RefreshCandidate) bool {
	switch c.State {
	case "subscribed", "archived", "not_subscribed":
		return c.GenerationID != ""
	}
	return false
}

// eligibility: scheduled (subscribed, keep-updated) or awaiting the live
// generation's first population.
func eligibility(c storage.RefreshCandidate) (scheduled, needsPopulation bool) {
	return c.State == "subscribed" && c.KeepUpdated, isLive(c) && !c.Populated
}

type passResult struct {
	started, skipped int
	wait             time.Duration // until the next due time (at most SyncEvery)
}

type dueItem struct {
	k      key
	host   string
	manual bool
	force  bool
	at     int64
}

// pass reconciles the schedule with the subscriptions and starts the due
// fetches within the concurrency limits. spread moves overdue scheduled work
// to random times within Spread. reconcile (at a start, after a clock jump)
// brings back an ordinary deadline further ahead than one interval with its
// jitter, as one written while the clock ran ahead; a Retry-After lower
// bound (NotBefore) is never changed (PROTOCOL §14).
func (r *Refresher) pass(ctx context.Context, spread, reconcile bool) (passResult, error) {
	res := passResult{wait: r.cfg.SyncEvery}
	r.mu.Lock()
	seq := r.finishSeq
	r.mu.Unlock()
	var cands []storage.RefreshCandidate
	err := r.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		cands, err = tx.RefreshCandidates("")
		return err
	})
	if err != nil {
		return res, err
	}
	now := r.nowMs()
	type update struct {
		k      key
		before *storage.RefreshRow
		after  storage.RefreshRow
		prev   *string // the fetch context it replaced, restored if the write is left out
	}
	var updates []update
	var due []dueItem
	present := make(map[key]bool, len(cands))
	accounts := map[string]bool{}
	r.mu.Lock()
	for _, c := range cands {
		k := key{c.AccountID, c.PodcastID}
		present[k] = true
		accounts[c.AccountID] = true
		_, running := r.running[k]
		if running || r.finished[k] > seq {
			res.skipped++ // its fetch writes the row; the next pass sees it
			continue
		}
		if c.State == "merged" {
			delete(r.manual, k) // a request is its survivor's (AuthorizedRequest)
			continue
		}
		scheduled, needsPop := eligibility(c)
		eligible := scheduled || needsPop
		manual, isManual := r.manual[k]
		var row storage.RefreshRow
		if c.Schedule != nil {
			row = *c.Schedule
		} else {
			row = storage.RefreshRow{PodcastID: c.PodcastID, NextAt: now}
		}
		before := row
		var changedFrom *string
		if isLive(c) {
			fc := fetchContext(c)
			// The persisted context wins, so a restart still notices a
			// replaced generation or a changed URL.
			prev, seen := row.Context, row.Context != ""
			if !seen {
				prev, seen = r.contexts[k]
			}
			r.contexts[k] = fc
			row.Context = fc
			if seen && prev != fc {
				changedFrom = &prev
				row.Failures = 0
				if contextURL(prev) != c.RefreshURL {
					row.NotBefore = 0
				}
				if eligible {
					row.NextAt = now
				}
			}
		}
		if reconcile && c.Schedule != nil && row.NextAt > now+int64(float64(r.cfg.Interval.Milliseconds())*(1+jitterFraction)) {
			row.NextAt = now + r.jitter(r.cfg.Interval).Milliseconds()
		}
		if spread && eligible && !isManual && c.Schedule != nil && row.NextAt <= now {
			row.NextAt = max(row.NotBefore, now+int64(r.cfg.Random()*float64(r.cfg.Spread.Milliseconds())))
		}
		// A new generation is fetched at once (its first population), also
		// when it opened just before a restart.
		if needsPop && row.Failures == 0 && row.NextAt > now {
			row.NextAt = now
		}
		if c.Schedule == nil {
			if eligible {
				updates = append(updates, update{k, nil, row, changedFrom})
			}
		} else if row != before {
			b := before
			updates = append(updates, update{k, &b, row, changedFrom})
		}
		switch {
		case isManual:
			due = append(due, dueItem{k: k, host: hostOf(c.RefreshURL), manual: true, force: manual, at: row.NextAt})
		case eligible && row.NextAt <= now && now >= row.NotBefore:
			due = append(due, dueItem{k: k, host: hostOf(c.RefreshURL), at: row.NextAt})
		case eligible:
			res.wait = min(res.wait, time.Duration(max(row.NextAt, row.NotBefore)-now)*time.Millisecond)
		}
	}
	// Forget podcasts that are gone (a deleted account).
	for k := range r.manual {
		if !present[k] {
			delete(r.manual, k)
		}
	}
	for k := range r.contexts {
		if !present[k] {
			delete(r.contexts, k)
		}
	}
	for k := range r.finished {
		if !present[k] {
			delete(r.finished, k)
		}
	}
	for a := range r.served {
		if !accounts[a] {
			delete(r.served, a)
		}
	}
	r.mu.Unlock()
	if len(updates) > 0 {
		var left []update
		err := r.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
			left = left[:0]
			for _, u := range updates {
				acct := tx.Account(u.k.account)
				if exists, err := acct.PodcastExists(u.after.PodcastID); err != nil || !exists {
					if err != nil {
						return err
					}
					continue // its account was deleted meanwhile
				}
				cur, ok, err := acct.RefreshSchedule(u.after.PodcastID)
				if err != nil {
					return err
				}
				// Something else (a request, a fetch) wrote the row since it
				// was read: leave it to the next pass.
				if ok != (u.before != nil) || (ok && cur != *u.before) {
					left = append(left, u)
					continue
				}
				if err := acct.PutRefreshSchedule(u.after); err != nil {
					return fmt.Errorf("refresh: schedule %s: %w", u.after.PodcastID, err)
				}
			}
			return nil
		})
		if err != nil {
			left = updates
		}
		// The next pass sees a context change whose write was left out again.
		r.mu.Lock()
		for _, u := range left {
			if u.prev != nil {
				r.contexts[u.k] = *u.prev
			}
		}
		r.mu.Unlock()
		if err != nil {
			return res, err
		}
	}
	// An account's own order: manual requests first, then the longest due.
	sort.Slice(due, func(i, j int) bool {
		if due[i].manual != due[j].manual {
			return due[i].manual
		}
		if due[i].at != due[j].at {
			return due[i].at < due[j].at
		}
		if due[i].k.account != due[j].k.account {
			return due[i].k.account < due[j].k.account
		}
		return due[i].k.podcast < due[j].k.podcast
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	res.started = r.admitLocked(ctx, due)
	res.wait = max(res.wait, 0)
	return res, nil
}

// admitLocked starts due work within the limits and returns how many fetches it started (r.mu
// held). Accounts take turns: the one with the fewest fetches running goes first, then the one
// whose last fetch started longest ago, so no account waits behind another's backlog, manual or
// not. A turn starts the account's first item, in its own order, whose host has room. While an
// account below PerAccount has work waiting, an account at PerAccount starts nothing more;
// otherwise one account may use every slot, so a server with one active account is no slower.
// Work left waiting starts when a fetch ends: a finished fetch wakes the loop.
func (r *Refresher) admitLocked(ctx context.Context, due []dueItem) int {
	type turn struct {
		account string
		items   []dueItem // waiting, in the account's order
		running int
	}
	var turns []*turn
	byAccount := map[string]*turn{}
	for _, d := range due {
		if _, running := r.running[d.k]; running {
			continue
		}
		t := byAccount[d.k.account]
		if t == nil {
			t = &turn{account: d.k.account}
			byAccount[d.k.account] = t
			turns = append(turns, t)
		}
		t.items = append(t.items, d)
	}
	for k := range r.running {
		if t := byAccount[k.account]; t != nil {
			t.running++
		}
	}
	first := func(a, b *turn) bool {
		if a.running != b.running {
			return a.running < b.running
		}
		if sa, sb := r.served[a.account], r.served[b.account]; sa != sb {
			return sa < sb
		}
		return a.account < b.account
	}
	started := 0
	for len(r.running) < r.cfg.Concurrency {
		held := false // an account below the limit has work waiting
		for _, t := range turns {
			if len(t.items) > 0 && t.running < r.cfg.PerAccount {
				held = true
				break
			}
		}
		var pick *turn
		at := -1
		for _, t := range turns {
			if (held && t.running >= r.cfg.PerAccount) || (pick != nil && !first(t, pick)) {
				continue
			}
			for i, d := range t.items {
				if r.hosts[d.host] < r.cfg.PerHost {
					pick, at = t, i
					break
				}
			}
		}
		if pick == nil {
			break
		}
		d := pick.items[at]
		pick.items = append(pick.items[:at], pick.items[at+1:]...)
		pick.running++
		r.startLocked(ctx, d)
		started++
	}
	return started
}

// startLocked starts one fetch (r.mu held).
func (r *Refresher) startLocked(ctx context.Context, d dueItem) {
	j := &job{host: d.host}
	r.running[d.k] = j
	r.hosts[d.host]++
	r.startSeq++
	r.served[d.k.account] = r.startSeq
	delete(r.manual, d.k)
	r.jobs.Add(1)
	go func() {
		defer r.jobs.Done()
		err := safely(func() error { return r.refresh(ctx, d.k, d.force) })
		var p *panicked
		switch {
		case errors.As(err, &p):
			// A panic the refresh did not turn into a failed refresh itself
			// (reading its context, or applying its fetch).
			r.logPanic(d.k, p)
			r.failAfterPanic(ctx, d.k)
		case err != nil && ctx.Err() == nil:
			r.log.Error("refresh: fetch failed to apply", "account", d.k.account, "podcast", d.k.podcast, "error", err)
		}
		r.mu.Lock()
		delete(r.running, d.k)
		if r.hosts[j.host]--; r.hosts[j.host] <= 0 {
			delete(r.hosts, j.host)
		}
		r.finishSeq++
		r.finished[d.k] = r.finishSeq
		r.mu.Unlock()
		r.signal()
	}()
}

// panicked is a recovered panic: its stack only. The panic's own text is
// never kept or logged, as it can quote a URL with credentials.
type panicked struct{ stack string }

func (p *panicked) Error() string { return "refresh: panic\n" + p.stack }

// safely runs f, turning a panic into a *panicked error: a feed or a refresh
// never stops the server.
func safely(f func() error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &panicked{stack: string(debug.Stack())}
		}
	}()
	return f()
}

func newRefreshID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ScheduleStatus is what the service page shows about the schedule.
type ScheduleStatus struct {
	DueNow   int   `json:"due_now"`   // podcasts whose refresh is due
	Running  int   `json:"running"`   // fetches running
	LastWake int64 `json:"last_wake"` // ms: the last catch-up after a sleep or clock change, 0 if none
}

// ScheduleStatus counts the podcasts due now and reports the last wake catch-up.
func (r *Refresher) ScheduleStatus(ctx context.Context) (ScheduleStatus, error) {
	var cands []storage.RefreshCandidate
	err := r.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		cands, err = tx.RefreshCandidates("")
		return err
	})
	if err != nil {
		return ScheduleStatus{}, err
	}
	now := r.nowMs()
	var st ScheduleStatus
	for _, c := range cands {
		scheduled, needsPop := eligibility(c)
		if !(scheduled || needsPop) {
			continue
		}
		if c.Schedule == nil || (c.Schedule.NextAt <= now && now >= c.Schedule.NotBefore) {
			st.DueNow++
		}
	}
	r.mu.Lock()
	st.Running, st.LastWake = len(r.running), r.lastWake
	r.mu.Unlock()
	return st, nil
}
