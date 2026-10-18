package api

// The read model's cache through the read API: a request waiting for
// another's build checks the caller again when it starts again, a
// response made from a library purged meanwhile is never sent,
// on-demand verification, and the startup warm-up of recently active
// accounts.

import (
	"context"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
)

// A request that finds another request building its
// account's library releases its reader (the pool's other readers stay
// free), waits, and starts again with the caller checked again: a session
// signed out meanwhile gets 401, on the summary and on an episode's detail.
// The builder serves its own snapshot.
func TestWaitersCheckTheCallerAgain(t *testing.T) {
	for _, path := range []string{"/v1/read/summary", "/v1/read/episodes/e1:none"} {
		t.Run(path, func(t *testing.T) {
			r := newReadEnv(t)
			other := r.env.session("owner", "correct horse battery")
			r.registerWeb(other, newID(), newID()).must(http.StatusOK)
			entered, release := make(chan struct{}), make(chan struct{})
			waiting := make(chan struct{}, 8)
			var once atomic.Bool
			r.srv.libraries.HookForTesting(views.CacheHooks{
				Published: r.countPublication,
				Build: func(string) {
					if once.CompareAndSwap(false, true) {
						close(entered)
						<-release
					}
				},
				Waiting: func(string) { waiting <- struct{}{} },
			})
			leader := make(chan *response, 1)
			go func() { leader <- r.do(http.MethodGet, "/v1/read/summary", nil, r.web) }()
			<-entered
			waiter := make(chan *response, 1)
			go func() { waiter <- r.do(http.MethodGet, path, nil, browser(other)) }()
			select {
			case <-waiting:
			case <-time.After(30 * time.Second):
				t.Fatal("the second request did not wait for the build")
			}
			// The builder holds one of the 4 readers; the waiter holds none.
			held, done := make(chan struct{}), make(chan struct{})
			for range 3 {
				go r.store.Read(context.Background(), func(*storage.ReadTx) error {
					held <- struct{}{}
					<-done
					return nil
				})
			}
			for i := range 3 {
				select {
				case <-held:
				case <-time.After(30 * time.Second):
					t.Fatalf("only %d of the 3 other readers free while a request waits", i)
				}
			}
			close(done)
			r.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(other)).must(http.StatusOK)
			close(release)
			if res := <-leader; res.status != http.StatusOK {
				t.Errorf("the builder: %d %s", res.status, res.raw)
			}
			if res := <-waiter; res.status != http.StatusUnauthorized {
				t.Errorf("a request that waited across its session's sign-out: %d %s", res.status, res.raw)
			}
		})
	}
}

// purgeEnv is an imported library, a phone that favorites episodes, and
// hooks that corrupt the next apply (the commit's change omitted) and purge
// the account right after the next library is served.
type purgeEnv struct {
	*readEnv
	s                 *commitRun
	corrupt, purgeNow atomic.Bool
	base              atomic.Pointer[views.Version]
	served            atomic.Int32
}

func newPurgeEnv(t *testing.T) *purgeEnv {
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	p := &purgeEnv{readEnv: r, s: &commitRun{readEnv: r, t: t, p: r.readPhone(), bins: map[string]int{}, outcomes: map[string]int{}}}
	r.srv.libraries.HookForTesting(views.CacheHooks{
		Published: func(a string, lib *views.Library) {
			r.countPublication(a, lib)
			p.base.Store(lib.V)
		},
		Built: func(_ string, path views.Path, v *views.Version, err error) *views.Version {
			if err == nil && path == views.PathApply && p.corrupt.Swap(false) {
				o := *p.base.Load()
				o.Head, o.Clock = v.Head, v.Clock
				return &o
			}
			return v
		},
		Served: func(a string, _ *views.Library) {
			p.served.Add(1)
			if p.purgeNow.Swap(false) {
				r.srv.libraries.Purge(a)
			}
		},
	})
	r.get("/v1/read/summary").must(http.StatusOK)
	p.favorite(p.unfavorited())
	r.get("/v1/read/summary").must(http.StatusOK) // the startup self-check's verified apply
	return p
}

// unfavorited is an episode the library shows that is not a favorite.
func (p *purgeEnv) unfavorited() *reference.Episode {
	ref := p.oracle()
	for _, e := range ref.Episodes {
		if !e.Favorite {
			return e
		}
	}
	p.t.Fatal("every episode is a favorite")
	return nil
}

func (p *purgeEnv) favorite(e *reference.Episode) {
	p.t.Helper()
	re := p.s.epRefFrom(p.oracle(), e)
	if out := p.s.push(p.s.fact("favorite.set", target(re), map[string]any{"episode": re, "on": true})); out != "applied" {
		p.t.Fatalf("favorite.set: %s", out)
	}
}

// A response made from a library the account was purged of after it
// was served (here, a wrong one) is discarded and made again.
func TestPurgedResponsesAreNotSent(t *testing.T) {
	p := newPurgeEnv(t)
	e := p.unfavorited()
	p.corrupt.Store(true)
	p.favorite(e)
	p.purgeNow.Store(true)
	p.served.Store(0)
	res := p.get("/v1/read/episodes/" + e.ID).must(http.StatusOK)
	if fav, _ := res.obj("episode")["favorite"].(bool); !fav || p.served.Load() != 2 {
		t.Errorf("an episode made from a purged library: favorite %v after %d libraries served", fav, p.served.Load())
	}
	// A list too (its view is refused under the purged incarnation).
	e = p.unfavorited()
	p.corrupt.Store(true)
	p.favorite(e)
	p.purgeNow.Store(true)
	res = p.get("/v1/read/favorites?limit=500").must(http.StatusOK)
	if !slices.ContainsFunc(items(t, res), func(m map[string]any) bool { return m["episode_id"] == e.ID }) {
		t.Error("a list made from a purged library was sent")
	}
	// A page of a view made before a purge is 410.
	first := p.get("/v1/read/favorites?limit=1").must(http.StatusOK)
	tok := first.str("next_page_token")
	if tok == "" {
		t.Fatal("one favorite only")
	}
	p.srv.libraries.Purge(p.account)
	p.get("/v1/read/favorites?limit=1&page_token=" + tok).must(http.StatusGone)
}

// Verification on demand (an administrator): every cached library against
// a full build; a wrong one is purged, and the next read is right.
func TestVerifyReadModel(t *testing.T) {
	p := newPurgeEnv(t)
	got := p.adminDo(p.session, http.MethodPost, "/v1/admin/read-model/verify", map[string]any{}).must(http.StatusOK)
	if got.obj("accounts")[p.account] != "ok" {
		t.Fatalf("a healthy library: %s", got.raw)
	}
	e := p.unfavorited()
	p.corrupt.Store(true)
	p.favorite(e)
	if res := p.get("/v1/read/episodes/" + e.ID).must(http.StatusOK); res.obj("episode")["favorite"] == true {
		t.Fatal("the corruption did not take")
	}
	got = p.adminDo(p.session, http.MethodPost, "/v1/admin/read-model/verify", map[string]any{}).must(http.StatusOK)
	if got.obj("accounts")[p.account] != "mismatch" {
		t.Fatalf("a wrong library: %s", got.raw)
	}
	if res := p.get("/v1/read/episodes/" + e.ID).must(http.StatusOK); res.obj("episode")["favorite"] != true {
		t.Error("the read after a mismatch is still wrong")
	}
}

// The startup warm-up builds the libraries of the accounts with an
// installation seen in the last 30 days, and the warmers then follow their
// commits without a read.
func TestWarmLibrariesWarmsRecentlyActiveAccounts(t *testing.T) {
	r := newReadEnv(t)
	t.Cleanup(r.srv.Close)
	r.createAccount(r.session, "idle", "idle-password-1", false) // no installation
	r.srv.WarmLibraries(context.Background())
	if got := r.srv.libraries.Accounts(); !slices.Equal(got, []string{r.account}) {
		t.Fatalf("warmed %v, want the active account only", got)
	}
	pubs := r.pubs.Load()
	r.importInto(r.account, "emulator-parity-library.db") // a commit
	deadline := time.Now().Add(30 * time.Second)
	for r.pubs.Load() == pubs {
		if time.Now().After(deadline) {
			t.Fatal("the warmer did not follow a commit nobody read")
		}
		time.Sleep(time.Millisecond)
	}
	var later []string
	if err := r.store.Read(context.Background(), func(x *storage.ReadTx) error {
		var err error
		later, err = x.RecentlyActiveAccounts(time.Now().Add(storage.RecentlyActive + time.Hour))
		return err
	}); err != nil || len(later) != 0 {
		t.Errorf("accounts active 30 days later: %v %v", later, err)
	}
}

// Over HTTP: a summary whose snapshot was pinned before a generation
// change (here, a bulk rewrite of the discovery policy, whose write returned)
// is never sent as the new incarnation's: it starts again and shows the new
// policy.
func TestSummaryAcrossAGenerationChange(t *testing.T) {
	r := newReadEnv(t)
	r.get("/v1/read/summary").must(http.StatusOK)
	paused, resume := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	r.srv.libraries.HookForTesting(views.CacheHooks{
		Published: r.countPublication,
		Keyed: func(string) {
			if once.CompareAndSwap(false, true) {
				close(paused)
				<-resume
			}
		},
	})
	got := make(chan *response, 1)
	go func() { got <- r.do(http.MethodGet, "/v1/read/summary", nil, r.web) }()
	<-paused
	if err := r.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		if err := tx.SuspendViewCapture(r.account); err != nil {
			return err
		}
		return tx.Account(r.account).PutRecord(storage.RecordRow{Entity: "account", EntityID: r.account, FieldGroup: "discovery_policy",
			Value: `{"new_episodes_action":"nothing"}`, Provenance: `["bulk"]`})
	}); err != nil {
		t.Fatal(err)
	}
	close(resume)
	res := <-got
	if res.status != http.StatusOK {
		t.Fatalf("summary: %d %s", res.status, res.raw)
	}
	if p := res.obj("policies")["new_episodes_action"]; p != "nothing" {
		t.Errorf("a summary across a generation change shows the replaced policy %v", p)
	}
}
