package refresh

// Refresh slots are shared
// fairly between accounts.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// otherAccount is another account, with one device, on e's server.
func (e *env) otherAccount() *env {
	e.t.Helper()
	o := &env{t: e.t, svc: e.svc, clock: e.clock, inst: storage.Installation{InstallID: newID(), InstanceNonce: newID(),
		Name: "phone", Platform: "ios", AppVersion: "1", Capabilities: `[]`}}
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		if err != nil {
			return err
		}
		o.account, o.namespace = a.ID, a.Namespace
		return tx.Account(o.account).InsertInstallation(o.inst)
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return o
}

// heldFetcher holds each fetch until the test releases its URL, then fails
// it (a failed fetch backs off, so it is not due again).
type heldFetcher struct {
	started chan string
	mu      sync.Mutex
	gates   map[string]chan struct{}
}

func newHeldFetcher() *heldFetcher {
	return &heldFetcher{started: make(chan string, 100), gates: map[string]chan struct{}{}}
}

func (f *heldFetcher) gate(url string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.gates[url]
	if !ok {
		g = make(chan struct{})
		f.gates[url] = g
	}
	return g
}

func (f *heldFetcher) Fetch(ctx context.Context, req feed.FetchRequest) (*feed.FetchResult, error) {
	g := f.gate(req.URL)
	f.started <- req.URL
	select {
	case <-g:
		return nil, &feed.FetchError{Label: "io", Detail: "test"}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// take returns the URLs of the next n fetches to start.
func (f *heldFetcher) take(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = <-f.started
	}
	return out
}

// release ends the fetch of url and waits until the refresher has finished it.
func (f *heldFetcher) release(r *Refresher, url string) {
	r.mu.Lock()
	n := len(r.running)
	r.mu.Unlock()
	close(f.gate(url))
	for {
		r.mu.Lock()
		left := len(r.running)
		r.mu.Unlock()
		if left < n {
			return
		}
		<-r.wake // a finished fetch signals
	}
}

// startPass runs one pass, expects it to start want fetches, and returns
// their URLs.
func startPass(t *testing.T, ctx context.Context, r *Refresher, f *heldFetcher, want int) []string {
	t.Helper()
	res, err := r.pass(ctx, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.started != want {
		t.Fatalf("started %d fetches, want %d", res.started, want)
	}
	urls := f.take(want)
	sort.Strings(urls)
	return urls
}

// owners counts URLs by the prefix before "-" in their host.
func owners(urls []string) map[string]int {
	out := map[string]int{}
	for _, u := range urls {
		host := strings.TrimPrefix(u, "https://")
		owner, _, _ := strings.Cut(host, "-")
		out[owner]++
	}
	return out
}

func refreshAll(t *testing.T, ctx context.Context, r *Refresher, e *env) {
	t.Helper()
	if _, err := r.Request(ctx, e.account, ""); err != nil {
		t.Fatal(err)
	}
}

// A has 16 feeds on distinct hosts and asks for Refresh all; B has one feed
// due. B's feed starts in the first round, and A's take the other slots.
func TestARefreshAllDoesNotStarveAnotherAccount(t *testing.T) {
	a := newEnv(t)
	b := a.otherAccount()
	for i := range 16 {
		a.subscribe(fmt.Sprintf("https://a-%d.test/feed", i), nil)
	}
	b.subscribe("https://b-0.test/feed", nil)
	f := newHeldFetcher()
	r := a.refresher(f, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.jobs.Wait() }()
	refreshAll(t, ctx, r, a)
	fetched := map[string]int{}
	for round := range 4 {
		urls := startPass(t, ctx, r, f, 4)
		got := owners(urls)
		if round == 0 && (got["a"] != 3 || got["b"] != 1) {
			t.Fatalf("first round %v: B's feed waits for A's Refresh all", urls)
		}
		for o, n := range got {
			fetched[o] += n
		}
		if res, err := r.pass(ctx, false, false); err != nil || res.started != 0 {
			t.Fatalf("a pass with every slot busy: %+v %v", res, err)
		}
		for _, u := range urls {
			f.release(r, u)
		}
	}
	if fetched["a"] != 15 || fetched["b"] != 1 {
		t.Fatalf("fetched %v in four rounds", fetched)
	}
}

// An account that holds every slot gives the next free one to another
// account's waiting refresh, however often it asks for Refresh all again.
func TestTheNextFreeSlotGoesToAnotherAccount(t *testing.T) {
	a := newEnv(t)
	for i := range 8 {
		a.subscribe(fmt.Sprintf("https://a-%d.test/feed", i), nil)
	}
	f := newHeldFetcher()
	r := a.refresher(f, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.jobs.Wait() }()
	refreshAll(t, ctx, r, a)
	held := startPass(t, ctx, r, f, 4) // alone, A takes every slot
	b := a.otherAccount()
	b.subscribe("https://b-0.test/feed", nil)
	b.subscribe("https://b-1.test/feed", nil)
	refreshAll(t, ctx, r, a)
	startPass(t, ctx, r, f, 0)
	for i, want := range []string{"b", "b", "a"} {
		f.release(r, held[i])
		urls := startPass(t, ctx, r, f, 1)
		if owners(urls)[want] != 1 {
			t.Fatalf("slot %d went to %v, want account %s's", i+1, urls, want)
		}
	}
}

// The per-account limit holds only while another account has work waiting:
// beside an account whose work is all running, and then alone, one account
// uses every slot. One-person servers are no slower.
func TestAnAccountAloneUsesEverySlot(t *testing.T) {
	a := newEnv(t)
	b := a.otherAccount()
	b.subscribe("https://b-0.test/feed", nil)
	f := newHeldFetcher()
	r := a.refresher(f, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.jobs.Wait() }()
	bURL := startPass(t, ctx, r, f, 1)[0]
	for i := range 8 {
		a.subscribe(fmt.Sprintf("https://a-%d.test/feed", i), nil)
	}
	refreshAll(t, ctx, r, a)
	if got := owners(startPass(t, ctx, r, f, 3)); got["a"] != 3 {
		t.Fatalf("beside B's running fetch, A started %v", got)
	}
	f.release(r, bURL)
	if got := owners(startPass(t, ctx, r, f, 1)); got["a"] != 1 {
		t.Fatalf("alone, A started %v", got)
	}
	r.mu.Lock()
	n := len(r.running)
	r.mu.Unlock()
	if n != 4 {
		t.Fatalf("%d fetches running, want every slot", n)
	}
}

// While another account has work waiting, an account starts no more than
// half the slots, even when that work waits for its host.
func TestAnAccountHoldsHalfTheSlotsWhileAnotherWaits(t *testing.T) {
	a := newEnv(t)
	b := a.otherAccount()
	for i := range 8 {
		a.subscribe(fmt.Sprintf("https://a-%d.test/feed", i), nil)
	}
	for i := range 3 {
		b.subscribe(fmt.Sprintf("https://b-shared.test/%d", i), nil)
	}
	f := newHeldFetcher()
	r := a.refresher(f, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.jobs.Wait() }()
	refreshAll(t, ctx, r, a)
	first := startPass(t, ctx, r, f, 3)
	if got := owners(first); got["a"] != 2 || got["b"] != 1 {
		t.Fatalf("first round %v", first)
	}
	bURL := first[len(first)-1] // b-shared sorts last
	f.release(r, bURL)
	next := startPass(t, ctx, r, f, 1)
	if owners(next)["b"] != 1 {
		t.Fatalf("B's second feed: %v", next)
	}
	// B's last feed starts; nothing of B's waits, so A takes the last slot.
	f.release(r, next[0])
	if got := owners(startPass(t, ctx, r, f, 2)); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("after B's last feed started: %v", got)
	}
}

// Accounts take turns: five accounts with backlogs share four slots, one of
// them asking for Refresh all. Every two rounds include every account, and
// after five each has had all four of its feeds fetched.
func TestAccountsTakeTurns(t *testing.T) {
	first := newEnv(t)
	accounts := []*env{first}
	for range 4 {
		accounts = append(accounts, first.otherAccount())
	}
	for ai, e := range accounts {
		for fi := range 4 {
			e.subscribe(fmt.Sprintf("https://%c-%d.test/feed", 'a'+ai, fi), nil)
		}
	}
	f := newHeldFetcher()
	r := first.refresher(f, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.jobs.Wait() }()
	refreshAll(t, ctx, r, first)
	var previous map[string]int
	total := map[string]int{}
	for round := 1; round <= 5; round++ {
		urls := startPass(t, ctx, r, f, 4)
		got := owners(urls)
		if len(got) != 4 {
			t.Fatalf("round %d: %v, want four accounts", round, urls)
		}
		for _, o := range "abcde" {
			if round > 1 && previous[string(o)]+got[string(o)] == 0 {
				t.Fatalf("rounds %d and %d left out account %c", round-1, round, o)
			}
		}
		for o, n := range got {
			total[o] += n
		}
		previous = got
		for _, u := range urls {
			f.release(r, u)
		}
	}
	for _, o := range "abcde" {
		if total[string(o)] != 4 {
			t.Fatalf("after five rounds: %v", total)
		}
	}
}

// Manual refreshes come first within their account, not across accounts.
// With two slots (one per account while the other waits), A's single-podcast
// refresh starts before A's longer-due scheduled feeds, and B's Refresh all,
// due longer still, does not take A's slot.
func TestManualRefreshesGoFirstWithinTheirAccount(t *testing.T) {
	a := newEnv(t)
	b := a.otherAccount()
	now := a.clock.ms()
	var asked string
	for i, due := range []int64{now - 2*hour, now - 2*hour, now - hour} {
		p, _ := a.subscribe(fmt.Sprintf("https://a-%d.test/feed", i), nil)
		a.putRow(storage.RefreshRow{PodcastID: p, NextAt: due})
		asked = p // the last, due latest
	}
	for i := range 2 {
		p, _ := b.subscribe(fmt.Sprintf("https://b-%d.test/feed", i), nil)
		b.putRow(storage.RefreshRow{PodcastID: p, NextAt: now - 3*hour})
	}
	f := newHeldFetcher()
	r := a.refresher(f, Config{Concurrency: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.jobs.Wait() }()
	if _, err := r.Request(ctx, a.account, asked); err != nil {
		t.Fatal(err)
	}
	refreshAll(t, ctx, r, b)
	urls := startPass(t, ctx, r, f, 2)
	if urls[0] != "https://a-2.test/feed" || owners(urls)["b"] != 1 {
		t.Fatalf("first round %v: want A's asked podcast and one of B's", urls)
	}
	for _, u := range urls {
		f.release(r, u)
	}
	// A's scheduled feeds follow, still one slot each.
	if got := owners(startPass(t, ctx, r, f, 2)); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("second round %v", got)
	}
}
