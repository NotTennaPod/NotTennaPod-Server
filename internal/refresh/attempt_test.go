package refresh

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

// attempts records Config.AfterAttempt's calls.
type attempts struct {
	mu    sync.Mutex
	calls []string
}

func (a *attempts) hook(account, podcast string, success bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	outcome := "failed"
	if success {
		outcome = "succeeded"
	}
	a.calls = append(a.calls, podcast+" "+outcome)
}

func (a *attempts) got() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
}

// closingFetcher closes the store while it fetches: the outcome of its fetch
// can be written nowhere.
type closingFetcher struct {
	e    *env
	body []byte
}

func (f closingFetcher) Fetch(context.Context, feed.FetchRequest) (*feed.FetchResult, error) {
	f.e.svc.Store.Close()
	return &feed.FetchResult{Body: f.body}, nil
}

// blockingFetcher answers only when its fetch is cancelled.
type blockingFetcher struct{ started chan struct{} }

func (f blockingFetcher) Fetch(ctx context.Context, _ feed.FetchRequest) (*feed.FetchResult, error) {
	close(f.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

type failingFetcher struct{}

func (failingFetcher) Fetch(context.Context, feed.FetchRequest) (*feed.FetchResult, error) {
	return nil, &feed.FetchError{Label: "io", Detail: "test"}
}

// AfterAttempt is called once for every fetch that ended, outside every lock:
// a success; a failed fetch, a panic and a fetch whose outcome could not be
// written are failures. A stopped fetch and a podcast that no longer exists are
// no attempt.
func TestAfterAttemptCalledOncePerEndedAttempt(t *testing.T) {
	const show = `<rss version="2.0"><channel><title>Show</title></channel></rss>`
	for _, c := range []struct {
		name  string
		fetch func(e *env) Fetcher
		want  string // "" for no call
	}{
		{"success", func(*env) Fetcher { return bodyFetcher(show) }, "succeeded"},
		{"failed fetch", func(*env) Fetcher { return failingFetcher{} }, "failed"},
		{"panic", func(*env) Fetcher { return credentialPanicFetcher{} }, "failed"},
		{"outcome not written", func(e *env) Fetcher { return closingFetcher{e, []byte(show)} }, "failed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			p, _ := e.subscribe("https://example.org/feed", nil)
			var a attempts
			r := e.refresher(c.fetch(e), Config{})
			r.cfg.AfterAttempt = func(account, podcast string, success bool) {
				// Outside every lock: the refresher's own can be taken.
				r.mu.Lock()
				r.mu.Unlock()
				if account != e.account {
					t.Errorf("account %s", account)
				}
				a.hook(account, podcast, success)
			}
			r.mu.Lock()
			r.startLocked(context.Background(), dueItem{k: key{e.account, p}})
			r.mu.Unlock()
			r.jobs.Wait()
			if got := a.got(); !slices.Equal(got, []string{p + " " + c.want}) {
				t.Errorf("calls %v", got)
			}
		})
	}
	t.Run("gone and stopped", func(t *testing.T) {
		e := newEnv(t)
		p, _ := e.subscribe("https://example.org/feed", nil)
		var a attempts
		started := make(chan struct{})
		r := e.refresher(blockingFetcher{started}, Config{AfterAttempt: a.hook})
		ctx, cancel := context.WithCancel(context.Background())
		r.mu.Lock()
		r.startLocked(ctx, dueItem{k: key{e.account, "no-such-podcast"}})
		r.startLocked(ctx, dueItem{k: key{e.account, p}})
		r.mu.Unlock()
		<-started
		cancel()
		r.jobs.Wait()
		if got := a.got(); len(got) != 0 {
			t.Errorf("calls %v", got)
		}
	})
}

// Cohort lists the podcasts the schedule fetches now: scheduled ones and
// those awaiting their first population. One that is only awaiting its first
// population leaves the list once populated, and one whose keep-updated is
// turned off leaves it too: a list made later is not the same list, which is
// why an import counts its first refresh from the list made at publication.
func TestCohortListsScheduledAndUnpopulated(t *testing.T) {
	e := newEnv(t)
	scheduled, _ := e.subscribe("https://example.org/a", nil)
	once, g := e.subscribe("https://example.org/b", nil)
	e.push(e.fact("podcast.set_policy", podTarget(once, g), map[string]any{"keep_updated": false}))
	r := e.refresher(bodyFetcher(`<rss version="2.0"><channel><title>Show</title><item><title>One</title><guid>one</guid></item></channel></rss>`), Config{})
	list := func() []string {
		got, err := r.Cohort(context.Background(), e.account)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		return got
	}
	want := []string{scheduled, once}
	slices.Sort(want)
	if got := list(); !slices.Equal(got, want) {
		t.Fatalf("before the first fetch %v, want %v", got, want)
	}
	runDue(t, r)
	if got := list(); !slices.Equal(got, []string{scheduled}) {
		t.Errorf("after the first population %v", got)
	}
}
