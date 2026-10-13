package api

// The refresh API (PROTOCOL §9.4): POST /v1/refresh with sync
// authentication answers 202 at once with each podcast's live status, and a
// request runs the podcast's next fetch now.

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// withCatalog serves the real operation types (subscriptions, catalog).
func withCatalog(t *testing.T) func(*Config) {
	return func(c *Config) {
		reg := protocol.NewRegistry()
		ops.Register(reg)
		views, err := protocol.NewViewRegistry(filepath.Join(c.Store.Dir(), "views"), c.Now)
		if err != nil {
			t.Fatal(err)
		}
		box, err := sealed.LoadOrCreate(c.Store.Dir())
		if err != nil {
			t.Fatal(err)
		}
		c.Protocol = &protocol.Service{Store: c.Store, Registry: reg, Now: c.Now, Views: views, Box: box}
	}
}

// gatedFetcher reports each fetch and holds it until released; every fetch
// fails as not found.
type gatedFetcher struct {
	calls   chan string
	release chan struct{}
}

func (f gatedFetcher) Fetch(ctx context.Context, req feed.FetchRequest) (*feed.FetchResult, error) {
	f.calls <- req.URL
	select {
	case <-f.release:
	case <-ctx.Done():
	}
	return nil, &feed.FetchError{Label: "not_found"}
}

func TestRefreshAPI(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withCatalog(t))
	_, accountID := e.setupAdmin("root", "root-password-1")
	p := e.phoneOf("root", "root-password-1")
	fact := func(seq int64, typ string, targets []any, payload map[string]any) map[string]any {
		return map[string]any{"op_id": newID(), "author_id": p.author, "author_seq": seq, "dependencies": []any{}, "type": typ,
			"type_version": 1, "client_time": time.Now().UnixMilli(), "targets": targets, "payload": payload}
	}
	sub := fact(1, "podcast.subscribe", []any{}, map[string]any{"feed_url": "https://feeds.example.com/show"})
	old := fact(2, "podcast.subscribe", []any{}, map[string]any{"feed_url": "https://feeds.example.com/old"})
	var namespace string
	e.store.Read(context.Background(), func(r *storage.ReadTx) error {
		a, err := r.GetAccount(accountID)
		namespace = a.Namespace
		return err
	})
	podcast := contract.PodcastID(namespace, sub["op_id"].(string))
	removed := contract.PodcastID(namespace, old["op_id"].(string))
	unsub := fact(3, "podcast.unsubscribe", []any{map[string]any{"entity": "podcast", "id": removed,
		"generation": contract.GenerationID(namespace, removed, old["op_id"].(string))}}, map[string]any{})
	r := pushed(t, p.push(op{1, sub, nil}, op{2, old, nil}, op{3, unsub, nil}))
	for _, x := range r.Results {
		if x.Status != "accepted" || !strings.Contains(string(x.Receipt), `"outcome":"applied"`) {
			t.Fatalf("setup: %+v", r)
		}
	}

	// Without a refresher there is no endpoint.
	e.do(http.MethodPost, "/v1/refresh", nil, p.cred).expect(http.StatusNotFound, "not_found")

	f := gatedFetcher{calls: make(chan string, 10), release: make(chan struct{})}
	refresher := refresh.New(e.srv.cfg.Protocol, refresh.Config{Fetcher: f})
	e.srv.cfg.Refresher = refresher
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refresher.Start(ctx)
	defer refresher.Stop()
	if got := <-f.calls; got != "https://feeds.example.com/show" { // the new subscription's first fetch, held
		t.Fatalf("first fetch %q", got)
	}

	// During it, a refresh of all (the podcasts kept updated) joins it and
	// answers at once.
	res := e.do(http.MethodPost, "/v1/refresh", nil, p.cred).must(http.StatusAccepted)
	list, _ := res.body["podcasts"].([]any)
	if len(list) != 1 {
		t.Fatalf("statuses %s", res.raw)
	}
	st := list[0].(map[string]any)
	if st["podcast_id"] != podcast || st["in_progress"] != true || len(st) != 6 {
		t.Errorf("status %v", st)
	}
	for _, k := range []string{"last_attempt", "last_success", "error"} {
		if v, present := st[k]; !present || v != nil {
			t.Errorf("%s = %v before any fetch", k, v)
		}
	}
	close(f.release)

	// A named podcast is fetched now, whatever its state.
	res = e.do(http.MethodPost, "/v1/refresh", map[string]any{"podcast_id": removed}, p.cred).must(http.StatusAccepted)
	if list, _ := res.body["podcasts"].([]any); len(list) != 1 || list[0].(map[string]any)["podcast_id"] != removed ||
		list[0].(map[string]any)["in_progress"] != true {
		t.Errorf("statuses %s", res.raw)
	}
	if got := <-f.calls; got != "https://feeds.example.com/old" {
		t.Errorf("fetched %q", got)
	}
	e.do(http.MethodPost, "/v1/refresh", rawBody(""), p.cred).must(http.StatusAccepted)

	// Refusals.
	e.do(http.MethodPost, "/v1/refresh", map[string]any{"podcast_id": "p1:" + strings.Repeat("0", 64)}, p.cred).expect(http.StatusNotFound, "not_found")
	e.do(http.MethodPost, "/v1/refresh", map[string]any{"podcast_id": 7}, p.cred).expect(http.StatusBadRequest, "invalid_request")
	e.do(http.MethodPost, "/v1/refresh", map[string]any{"podcast": podcast}, p.cred).expect(http.StatusBadRequest, "invalid_request")
	e.do(http.MethodPost, "/v1/refresh", nil).expect(http.StatusUnauthorized, "unauthorized")
	e.do(http.MethodGet, "/v1/refresh", nil, p.cred).expect(http.StatusMethodNotAllowed, "method_not_allowed")
}
