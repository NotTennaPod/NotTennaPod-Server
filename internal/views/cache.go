package views

// The read model's cache: each account's library version, built once and
// shared by every request (PROTOCOL §12).
//
// One builder per account, elected without blocking. Every caller (a
// request, an account's warmer, the startup warm-up) runs the same loop
// (Cache.get). It opens a read transaction, has the caller checked again
// (authorization is fresh in every attempt), reads the snapshot's key (its
// head and capture clock) and, under the cache's lock:
//
//   - the published version is at that key, under the account's current
//     incarnation: it is served, in this transaction;
//   - a build is running: the transaction is rolled back (no reader is held
//     while waiting), the caller waits for the build outside any
//     transaction, then starts again;
//   - the published version is newer than the snapshot: the caller starts
//     again (a newer snapshot is at least the published one's); never a
//     full build;
//   - else the caller becomes the account's builder (its flight), with the
//     published version as its base. In its own transaction it applies the
//     changes captured since the base (Advance), or builds in full when
//     there is no base (an account's first build, after a purge or an idle
//     release) or the lineage or generation changed. It publishes the
//     result only if the account's incarnation is still the one it was
//     elected under and the result is newer than the published version,
//     then serves it from its own snapshot; it never waited on anyone. A
//     result it did not publish is never served: it starts again.
//
// The incarnation a snapshot may serve under is taken before it is pinned:
// a retirement after that (a generation change, a restore, the account's
// deletion, a purge) discards the snapshot at the election or the
// publication. The published version therefore never goes back within an
// incarnation, an earlier snapshot never takes on a later incarnation, and
// a routine change never causes a full build. After 6 attempts the caller
// gives up (ErrBusy).
//
// Faults. A build that panics or fails publishes nothing, and its waiters
// start again. An apply that finds the version inconsistent, a verified
// apply that differs from a full build at the same snapshot, or two
// consecutive failures purge the account: a new incarnation (Spool.Retire:
// its views expire, and a view or build of the old one is refused), the
// published version dropped, and suspect mode, in which the next
// suspectApplies applies are each verified against a full build. The first
// apply of each account after the server starts is verified too (the
// startup self-check), and Verify checks an account on demand. Only a
// completed, matching verification of an apply in the still-current
// incarnation counts: a head-only commit verifies nothing, and work rejected
// by a purge moves no counter.
//
// Memory, a structural bound on what is held, per account: the published
// version, the one before it and at most maxLeased leased versions while
// views pin them (the views of any other version expire at publication:
// Spool.retain); one build in progress, with its verification's full build
// (Verify is a build of the election too); and one library per admitted
// response, at most maxHolds (Spool.Admit: a page being encoded, a screen
// being made). At most maxAccounts accounts hold a library, counting those
// released while a response of theirs is still being made (enter); an
// account unread for IdleRelease, or released to make room, goes with its
// views (Spool.Retire).

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

const (
	// maxAccounts is how many accounts' libraries the cache keeps; the
	// least recently read is released first.
	maxAccounts = 8
	// IdleRelease is how long an unread account's library is kept.
	IdleRelease = 10 * time.Minute
	// attempts bounds one caller's elections.
	attempts = 6
	// suspectApplies is how many verified applies clear suspect mode.
	suspectApplies = 3
)

// ErrClosed reports a call after Close.
var ErrClosed = errors.New("views: the read model is closed")

var (
	errWait    = errors.New("views: wait for the account's build")
	errRestart = errors.New("views: start again")
	errGone    = errors.New("views: the account was released")
)

// Cache keeps each account's published library version.
type Cache struct {
	store   *storage.Store
	spool   *Spool // the incarnations, and the views that pin versions
	capture bool   // the store captures view-input changes; without, every read builds in full
	now     func() time.Time

	mu       sync.Mutex
	accounts map[string]*cached
	checked  map[string]bool // accounts whose first apply was verified (the startup self-check)
	serial   uint64          // publications
	hooks    CacheHooks
	warm     bool // accounts' warmers run (StartWarmers)
	closed   bool

	ctx    context.Context // the warmers'; cancelled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type cached struct {
	current  *Library // the published version
	flight   *flight  // the build in progress
	suspect  int      // verified applies still needed
	failures int      // consecutive failed builds
	lastRead time.Time
	quit     chan struct{} // ends the account's warmer; nil without one
}

type flight struct {
	done   chan struct{}
	base   *Library
	inc    uint64
	verify bool // compared with a full build: two libraries in the flight's slot
}

// CacheHooks are test hooks (HookForTesting). Each is called without the
// cache's lock unless it says otherwise.
type CacheHooks struct {
	Keyed     func(account string)                                         // a snapshot's key read, before the election
	Build     func(account string)                                         // the elected builder, in its transaction, before it builds
	Built     func(account string, p Path, v *Version, err error) *Version // after it built, whatever the outcome: its result (a replacement corrupts it)
	Waiting   func(account string)                                         // a caller about to wait for another's build, its reader released
	Published func(account string, lib *Library)                           // under the cache's lock
	Served    func(account string, lib *Library)                           // a library served, its transaction done
	Compared  func(account string)                                         // a verified build, with its comparison's full build still held
}

// NewCache returns an empty cache of the store's accounts. spool is the read
// API's views registry: it holds the incarnations, and the cache expires
// the views of versions it no longer retains; nil for none. now is the
// clock of the idle release (nil: time.Now).
func NewCache(store *storage.Store, spool *Spool, now func() time.Time) *Cache {
	if now == nil {
		now = time.Now
	}
	if spool == nil {
		spool, _ = NewSpool("", now)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Cache{store: store, spool: spool, capture: store.ViewCapture(), now: now, accounts: map[string]*cached{}, checked: map[string]bool{},
		ctx: ctx, cancel: cancel}
}

// HookForTesting sets the test hooks.
func (c *Cache) HookForTesting(h CacheHooks) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hooks = h
}

// Get runs fn in a read transaction with the account's library at that
// snapshot, and returns the library. check, if not nil, runs first in every
// transaction (the caller's authorization, checked again). A library is
// served only from a transaction that checked the caller at its start and
// waited for nothing. The caller sends a response made from the library
// only if it is still Valid once made.
func (c *Cache) Get(ctx context.Context, account string, check func(*storage.ReadTx) error, fn func(*storage.ReadTx, *Library) error) (*Library, error) {
	for range attempts {
		// Its warmer subscribes before the snapshot: no commit is missed.
		if err := c.enter(ctx, account); err != nil {
			return nil, err
		}
		lib, _, err := c.get(ctx, account, check, fn, modeRead)
		if !errors.Is(err, errGone) {
			return lib, err
		}
	}
	return nil, ErrBusy
}

// Warm builds the account's library if it has none at the latest snapshot
// (the startup warm-up). Its idle time starts now.
func (c *Cache) Warm(ctx context.Context, account string) error {
	if err := c.enter(ctx, account); err != nil {
		return err
	}
	_, _, err := c.get(ctx, account, nil, nil, modeWarm)
	if errors.Is(err, errGone) {
		return nil // released meanwhile
	}
	return err
}

// enter makes the account's state, read now, once the global bound allows
// it: at most maxAccounts accounts hold a library, those cached and those
// released whose responses are still being made (Spool.Admit) alike. It
// releases the least recently read cached accounts that hold nothing beyond
// their state (no build in progress, no response being made); while none
// can go, it waits, outside any transaction, for a response to end.
func (c *Cache) enter(ctx context.Context, account string) error {
	for {
		c.mu.Lock()
		if c.accounts[account] != nil || c.makeRoom(account) {
			c.state(account, true)
			c.mu.Unlock()
			return nil
		}
		freed := c.spool.released()
		c.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// makeRoom releases cached accounts until the account fits within the
// global bound, and reports whether it does. c.mu is held.
func (c *Cache) makeRoom(account string) bool {
	held := c.spool.heldAccounts()
	count := func() int {
		n := 0
		for id := range held {
			if c.accounts[id] == nil && id != account {
				n++ // released, a response still being made
			}
		}
		return n + len(c.accounts) + 1
	}
	for count() > maxAccounts {
		var oldest string
		for id, a := range c.accounts {
			if a.flight == nil && !held[id] && (oldest == "" || a.lastRead.Before(c.accounts[oldest].lastRead)) {
				oldest = id
			}
		}
		if oldest == "" {
			return false
		}
		c.release(oldest)
	}
	return true
}

// mode is what a caller of get wants.
type mode int

const (
	modeRead   mode = iota // a request: fn is served the library
	modeWarm               // the warmer, the warm-up: the library is built, nothing is served
	modeVerify             // Verify: the published version, applied to a new snapshot, against a full build
)

func (c *Cache) get(ctx context.Context, account string, check func(*storage.ReadTx) error, fn func(*storage.ReadTx, *Library) error,
	m mode) (*Library, string, error) {
	// Close owns every operation: it cancels it, waits for it, and nothing
	// publishes once it has begun.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, "", ErrClosed
	}
	c.wg.Add(1)
	hooks := c.hooks
	c.mu.Unlock()
	defer c.wg.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(c.ctx, cancel)()
	serve := func(r *storage.ReadTx, lib *Library) error {
		if fn != nil {
			return fn(r, lib)
		}
		return nil
	}
	for range attempts {
		var wait <-chan struct{}
		var lib *Library
		result := ""
		// The incarnation the snapshot may serve under is taken before the
		// snapshot is pinned: a retirement after it (a generation change, a
		// restore, the account's deletion, a purge) is seen at the election
		// or the publication, and the snapshot is discarded. A later
		// incarnation is never given to an earlier snapshot.
		inc := c.spool.Incarnation(account)
		err := c.store.Read(ctx, func(r *storage.ReadTx) error {
			if check != nil {
				if err := check(r); err != nil {
					return err
				}
			}
			acct := r.Account(account)
			head, err := acct.Head()
			if err != nil {
				return err
			}
			clock, err := acct.ViewClock()
			if err != nil {
				return err
			}
			if !c.capture {
				if m == modeVerify {
					result = VerifyNotCached
					return nil
				}
				v, err := BuildVersion(acct, account)
				if err != nil {
					return err
				}
				if c.spool.Incarnation(account) != inc {
					return errRestart
				}
				lib = &Library{Head: head, V: v, inc: inc}
				return serve(r, lib)
			}
			if hooks.Keyed != nil {
				hooks.Keyed(account)
			}
			c.mu.Lock()
			if c.spool.Incarnation(account) != inc {
				c.mu.Unlock()
				return errRestart // retired since the snapshot's incarnation was taken
			}
			a := c.accounts[account]
			if a == nil {
				c.mu.Unlock()
				return errGone // released meanwhile: only a read brings it back, through the global bound (enter)
			}
			cur := a.current
			if cur != nil && cur.inc != inc {
				cur = nil // of an older incarnation: never served, never a base
			}
			exact := cur != nil && cmpKey(cur.Head, cur.V.Clock, head, clock) == 0 && string(cur.Head.Digest) == string(head.Digest)
			switch {
			case exact && m != modeVerify:
				lib = cur
				c.mu.Unlock()
				return serve(r, lib)
			case a.flight != nil:
				wait = a.flight.done
				c.mu.Unlock()
				return errWait
			case cur == nil && m == modeVerify:
				c.mu.Unlock()
				result = VerifyNotCached
				return nil
			case cur != nil && cur.Head.LogID == head.LogID && cmpKey(cur.Head, cur.V.Clock, head, clock) > 0:
				c.mu.Unlock()
				return errRestart // an older snapshot than the published version's
			}
			f := &flight{done: make(chan struct{}), base: cur, inc: inc, verify: cur != nil && (m == modeVerify || a.suspect > 0 || !c.checked[account])}
			a.flight = f
			c.mu.Unlock()
			out, err := c.lead(ctx, acct, account, f, head, clock, m == modeVerify, hooks)
			switch {
			case err != nil:
				return err
			case out.stale:
				return errRestart
			case m == modeVerify:
				result = VerifyOK
				if out.mismatch {
					result = VerifyMismatch
				}
				return nil
			case out.lib == nil:
				return errRestart // not published: never served
			}
			lib = out.lib
			return serve(r, lib)
		})
		switch {
		case errors.Is(err, errWait):
			if hooks.Waiting != nil {
				hooks.Waiting(account)
			}
			select {
			case <-wait:
			case <-ctx.Done():
				return nil, "", ctx.Err()
			}
		case errors.Is(err, errRestart):
		case err != nil:
			return nil, "", err
		case m == modeVerify:
			return nil, result, nil
		default:
			if hooks.Served != nil {
				hooks.Served(account, lib)
			}
			return lib, "", nil
		}
	}
	return nil, "", ErrBusy
}

// outcome is what a build came to.
type outcome struct {
	lib      *Library // published
	stale    bool     // the incarnation changed meanwhile: nothing moved, start again
	mismatch bool     // the account was purged (a verified build differs, an inconsistent apply)
}

// lead is the elected builder's work: it builds the account's version at
// its snapshot from the flight's base and publishes it under the rules
// above. force (Verify) compares an unchanged version too. An error is the
// caller's (its context ended, the cache closed, a full build or a
// verification failed).
func (c *Cache) lead(ctx context.Context, acct *storage.AccountReader, account string, f *flight, head storage.Head, clock storage.ViewClock,
	force bool, hooks CacheHooks) (out outcome, err error) {
	var v *Version
	var path Path
	compared, mismatch := false, false
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("views: the build panicked: %v", p)
			}
		}()
		if hooks.Build != nil {
			hooks.Build(account)
		}
		var base *Version
		if f.base != nil {
			base = f.base.V
		}
		v, path, err = Advance(base, acct, account)
		if hooks.Built != nil {
			v = hooks.Built(account, path, v, err)
		}
		if err != nil {
			return
		}
		// The comparison is the flight's own work (its slot in the memory
		// bound), never a build beside it.
		if f.verify && (path == PathApply || force && path == PathExact) {
			full, ferr := BuildVersion(acct, account)
			if ferr != nil {
				err = ferr
				return
			}
			compared, mismatch = true, EqualVersions(v, full) != nil
			if hooks.Compared != nil {
				hooks.Compared(account)
			}
			runtime.KeepAlive(full)
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.accounts[account] // an account with a flight is never released
	defer func() {
		a.flight = nil
		close(f.done)
	}()
	switch {
	case c.closed:
		return outcome{}, ErrClosed
	case err != nil && ctx.Err() != nil:
		return outcome{}, err // cancelled: not a failure
	case f.inc != c.spool.Incarnation(account):
		// Purged or retired meanwhile: rejected work moves nothing (no
		// failure, no verification credit, no purge of the new incarnation).
		return outcome{stale: true}, nil
	case mismatch:
		c.purge(account, a, "a verified apply differs from a full build")
		return outcome{mismatch: true}, nil
	case errors.Is(err, ErrInconsistent):
		c.purge(account, a, "an apply found the version inconsistent")
		return outcome{mismatch: true}, nil
	case err != nil:
		if a.failures++; a.failures >= 2 {
			c.purge(account, a, "two builds failed")
		}
		if f.base != nil && !force {
			return outcome{}, nil // an apply's failure: start again
		}
		return outcome{}, err
	}
	a.failures = 0
	// Credit only for a completed, matching verification of an apply: an
	// unchanged version (a head-only commit) verifies nothing.
	if compared && path == PathApply {
		c.checked[account] = true
		a.suspect = max(a.suspect-1, 0)
	}
	if cur := a.current; cur != nil && cur.inc == f.inc && cur.Head.LogID == head.LogID && cmpKey(head, clock, cur.Head, cur.V.Clock) <= 0 {
		return outcome{}, nil // never older than the published version
	}
	c.serial++
	lib := &Library{Head: head, V: v, inc: f.inc, pub: c.serial}
	prev := a.current
	a.current = lib
	c.spool.retain(account, lib, prev)
	if hooks.Published != nil {
		hooks.Published(account, lib)
	}
	return outcome{lib: lib}, nil
}

// cmpKey orders two snapshots' keys of one log: by epoch, generation, tick,
// then head sequence, all of which never go back within it. Keys of
// different logs compare as 0 only if equal (and are otherwise unordered;
// an account's log never changes while the server runs).
func cmpKey(h1 storage.Head, c1 storage.ViewClock, h2 storage.Head, c2 storage.ViewClock) int {
	if h1.LogID != h2.LogID {
		return cmp.Compare(h1.LogID, h2.LogID)
	}
	return cmp.Or(cmp.Compare(h1.Epoch, h2.Epoch), cmp.Compare(c1.Generation, c2.Generation), cmp.Compare(c1.Tick, c2.Tick), cmp.Compare(h1.Seq, h2.Seq))
}

// state returns the account's state, made (with its warmer) if new; read
// records a read. c.mu is held.
func (c *Cache) state(account string, read bool) *cached {
	a := c.accounts[account]
	if a == nil {
		a = &cached{lastRead: c.now()}
		c.accounts[account] = a
		c.startWarmer(account, a)
	}
	if read {
		a.lastRead = c.now()
	}
	return a
}

// release drops an account's state, ends its warmer and retires its views
// (Spool.Retire): a released account keeps no library reachable through
// them, so the bound on cached accounts holds. c.mu is held.
func (c *Cache) release(account string) {
	a := c.accounts[account]
	delete(c.accounts, account)
	if a != nil && a.quit != nil {
		close(a.quit)
	}
	c.spool.Retire(account)
}

// purge starts a new incarnation of the account: its views expire and are
// refused (Spool.Retire), builds and responses of the old one are never
// published or sent, its version is dropped and its next applies are
// verified. c.mu is held.
func (c *Cache) purge(account string, a *cached, reason string) {
	a.current, a.suspect, a.failures = nil, suspectApplies, 0
	c.spool.Retire(account)
	slog.Warn("read model: an account's library was dropped and is built again", "account", account, "reason", reason)
}

// Purge purges the account (purge).
func (c *Cache) Purge(account string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if a := c.accounts[account]; a != nil {
		c.purge(account, a, "purged")
		return
	}
	c.spool.Retire(account)
}

// Valid reports whether a response made from lib may be sent: its
// incarnation is still the account's (a purge, or an identity change that
// retired the account, since it was served discards the response; the
// caller makes it again). O(1).
func (c *Cache) Valid(account string, lib *Library) bool {
	return lib.inc == c.spool.Incarnation(account)
}

// Admit admits a response of the account made from one of its libraries,
// waiting at most the registry's admission time (Spool.Admit); TryAdmit
// admits it only if a slot is free at once. A caller reads its library
// first and is then admitted without waiting, or, when every slot is taken,
// drops it, waits for admission and reads it again: no caller waits while
// it pins a library.
func (c *Cache) Admit(ctx context.Context, account string) (*Hold, error) {
	ctx, cancel := context.WithTimeout(ctx, c.spool.holdWait)
	defer cancel()
	return c.spool.Admit(ctx, account)
}

// TryAdmit is Spool.TryAdmit.
func (c *Cache) TryAdmit(account string) (*Hold, bool) { return c.spool.TryAdmit(account) }

// Touch records a use of the account's library that did not read it through
// the cache (a list view's page).
func (c *Cache) Touch(account string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if a := c.accounts[account]; a != nil {
		a.lastRead = c.now()
	}
}

// Accounts lists the accounts the cache holds a library of.
func (c *Cache) Accounts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for id, a := range c.accounts {
		if a.current != nil {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// Verification results.
const (
	VerifyOK        = "ok"
	VerifyMismatch  = "mismatch"
	VerifyNotCached = "not_cached"
)

// Verify checks the account's published version through the election, as
// the account's one build (never beside another): in one snapshot, the
// version with the changes since applied (or unchanged) against a full
// build. A mismatch purges the account. An account without a published
// version is not_cached.
func (c *Cache) Verify(ctx context.Context, account string) (string, error) {
	_, result, err := c.get(ctx, account, nil, nil, modeVerify)
	if errors.Is(err, errGone) || err == nil && result == "" {
		return VerifyNotCached, nil // released meanwhile
	}
	return result, err
}

// ---- warmers

// StartWarmers starts each account's warmer: from an account's first read
// until its idle release, every commit of the account advances its
// published version in the background, so the next read is an exact hit.
// A read never depends on it: a miss builds on demand.
func (c *Cache) StartWarmers() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.warm || c.closed {
		return
	}
	c.warm = true
	for id, a := range c.accounts {
		c.startWarmer(id, a)
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-t.C:
				c.Sweep()
			}
		}
	}()
}

// startWarmer subscribes the account's warmer to its commits. c.mu is held.
func (c *Cache) startWarmer(account string, a *cached) {
	if !c.warm || c.closed || a.quit != nil {
		return
	}
	commits, unsubscribe := c.store.Subscribe(account)
	quit := make(chan struct{})
	a.quit = quit
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer unsubscribe()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-quit:
				return
			case <-commits:
				// A failure is the next request's to see: it builds on its own.
				_, _, _ = c.get(c.ctx, account, nil, nil, modeWarm)
			}
		}
	}()
}

// Sweep releases the accounts unread for IdleRelease (none with a build in
// progress): their versions and warmers go, and their next read builds in
// full.
func (c *Cache) Sweep() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range slices.Sorted(maps.Keys(c.accounts)) {
		if a := c.accounts[id]; a.flight == nil && c.now().Sub(a.lastRead) >= IdleRelease {
			c.release(id)
		}
	}
}

// Close cancels every operation of the cache (warmers, the warm-up,
// requests' and verifications' builds) and waits for them: nothing
// publishes once it has begun, and nothing reads the store after it
// returns (call it before the store closes). Later calls answer ErrClosed.
func (c *Cache) Close() {
	c.mu.Lock()
	c.closed, c.warm = true, false
	c.mu.Unlock()
	c.cancel()
	c.wg.Wait()
}
