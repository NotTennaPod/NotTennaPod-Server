package api

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
)

func init() {
	errorStatus["account_changed"] = []int{409}
}

// withReads serves the read API over the real operation types (and test.note,
// so a phone can commit cheaply).
func withReads(t *testing.T) func(*Config) {
	return func(c *Config) {
		reg := protocol.NewRegistry()
		ops.Register(reg)
		reg.Register("test.note", 1, syncNote{})
		snapshots, err := protocol.NewViewRegistry(filepath.Join(c.Store.Dir(), "views"), c.Now)
		if err != nil {
			t.Fatal(err)
		}
		box, err := sealed.LoadOrCreate(c.Store.Dir())
		if err != nil {
			t.Fatal(err)
		}
		c.Protocol = &protocol.Service{Store: c.Store, Registry: reg, Now: c.Now, Views: snapshots, Box: box}
		if c.Views, err = views.NewSpool(filepath.Join(c.Store.Dir(), "read-views"), c.Now); err != nil {
			t.Fatal(err)
		}
	}
}

// importInto imports an Android backup into the account directly (PROTOCOL §11.5).
func (e *env) importInto(account, fixture string) {
	e.t.Helper()
	ctx := context.Background()
	lib, err := androiddb.Read(ctx, "../importer/androiddb/testdata/android-3.12.1/"+fixture, e.t.TempDir())
	if err != nil {
		e.t.Fatal(err)
	}
	importID := newID()
	plan, err := importer.FromAndroid(lib, importID, e.clock.Now().UnixMilli())
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := importer.Apply(ctx, e.srv.cfg.Protocol, account, plan, 0); err != nil {
		e.t.Fatal(err)
	}
	if _, err := importer.Complete(ctx, e.srv.cfg.Protocol, account, importID, "", plan); err != nil {
		e.t.Fatal(err)
	}
}

type readEnv struct {
	*env
	session, account string
	web              opt
}

func newReadEnv(t *testing.T) *readEnv {
	e := newEnv(t, withReads(t))
	session, account := e.setupAdmin("owner", "correct horse battery")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	return &readEnv{env: e, session: session, account: account, web: browser(session)}
}

func (r *readEnv) get(path string) *response {
	r.t.Helper()
	return r.do(http.MethodGet, path, nil, r.web)
}

func items(t *testing.T, r *response) []map[string]any {
	t.Helper()
	list, _ := r.body["items"].([]any)
	out := make([]map[string]any, len(list))
	for i, x := range list {
		out[i], _ = x.(map[string]any)
	}
	return out
}

func head(r *response) map[string]any { return r.obj("head") }

// Every view answers at one head with the cursor a push names; lists page
// at the head of their first page whatever commits follow; page tokens bind
// their parameters and expire after ten idle minutes.
func TestReadViews(t *testing.T) {
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	sum := r.get("/v1/read/summary").must(http.StatusOK)
	if c := sum.obj("counters"); c["queue"] == nil || c["inbox"] == nil {
		t.Fatalf("summary %s", sum.raw)
	}
	if head(sum)["cursor"] == "" || sum.obj("policies")["new_episodes_action"] != "inbox" {
		t.Fatalf("summary %s", sum.raw)
	}
	all := r.get("/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed").must(http.StatusOK)
	total := int(all.body["total"].(float64))
	if total < 10 || all.body["next_page_token"] != nil {
		t.Fatalf("episodes: total %d, next %v", total, all.body["next_page_token"])
	}
	first := r.get("/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed&limit=4").must(http.StatusOK)
	// A commit lands between pages.
	p := r.phoneOf("owner", "correct horse battery")
	pushed(t, p.push(op{seq: 1, fact: p.noteFact("between pages")}))
	var paged []map[string]any
	paged = append(paged, items(t, first)...)
	page := first
	for page.body["next_page_token"] != nil {
		token := page.body["next_page_token"].(string)
		page = r.get("/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed&limit=4&page_token=" + token).must(http.StatusOK)
		if head(page)["seq"] != head(first)["seq"] {
			t.Fatalf("a later page has head %v, the first %v", head(page)["seq"], head(first)["seq"])
		}
		paged = append(paged, items(t, page)...)
	}
	if len(paged) != total {
		t.Fatalf("paged %d of %d", len(paged), total)
	}
	for i, it := range items(t, all) {
		if paged[i]["episode_id"] != it["episode_id"] {
			t.Fatalf("page order differs at %d", i)
		}
	}
	token := first.body["next_page_token"].(string)
	r.get("/v1/read/episodes?filter=played&page_token="+token).expect(http.StatusBadRequest, "invalid_request")
	r.clock.Advance(11 * time.Minute)
	r.get("/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed&limit=4&page_token="+token).
		expect(http.StatusGone, "view_expired")
	// The other screens answer.
	for _, path := range []string{"/v1/read/subscriptions", "/v1/read/subscriptions?order=alphabetical&counter=unplayed",
		"/v1/read/queue", "/v1/read/inbox", "/v1/read/favorites", "/v1/read/history", "/v1/read/statistics",
		"/v1/read/statistics?include_marked=true", "/v1/read/home?seed=7"} {
		if res := r.get(path).must(http.StatusOK); head(res)["cursor"] == "" {
			t.Errorf("%s: no head", path)
		}
	}
	r.get("/v1/read/episodes?filter=downloaded").expect(http.StatusBadRequest, "invalid_request")
	r.get("/v1/read/episodes?sort=nonsense").expect(http.StatusBadRequest, "invalid_request")
	r.get("/v1/read/episodes?bogus=1").expect(http.StatusBadRequest, "invalid_request")
	// A podcast and an episode.
	ep := items(t, all)[0]
	id := ep["episode_id"].(string)
	one := r.get("/v1/read/episodes/" + id).must(http.StatusOK)
	if one.obj("episode")["episode_id"] != id || one.obj("episode")["metadata"] == nil {
		t.Fatalf("episode %s", one.raw)
	}
	pod := r.get("/v1/read/podcasts/" + ep["podcast_id"].(string)).must(http.StatusOK)
	if pod.obj("podcast")["statistics"] == nil {
		t.Fatalf("podcast %s", pod.raw)
	}
	eps := r.get("/v1/read/podcasts/" + ep["podcast_id"].(string) + "/episodes").must(http.StatusOK)
	if len(items(t, eps)) == 0 {
		t.Fatal("a podcast without episodes")
	}
	r.get("/v1/read/episodes/e1:nothing").expect(http.StatusNotFound, "not_found")
	r.get("/v1/read/podcasts/p1:nothing").expect(http.StatusNotFound, "not_found")
}

// Credentials show as set with their username, never the password.
func TestReadCredentials(t *testing.T) {
	r := newReadEnv(t)
	r.importInto(r.account, "archived-credentials-library.db")
	subs := r.get("/v1/read/subscriptions?state=archived").must(http.StatusOK)
	list := items(t, subs)
	if len(list) != 1 || list[0]["credentials"] != true {
		t.Fatalf("archived %s", subs.raw)
	}
	pod := r.get("/v1/read/podcasts/" + list[0]["podcast_id"].(string)).must(http.StatusOK)
	creds, _ := pod.obj("podcast")["credentials"].(map[string]any)
	if creds["set"] != true || creds["username"] == nil || creds["username"] == "" {
		t.Fatalf("credentials %v", creds)
	}
	if strings.Contains(string(pod.raw), "password") {
		t.Fatalf("a password reached the browser: %s", pod.raw)
	}
}

// Only a browser bound to its web installation reads; a tab naming another
// account is told to reload.
func TestReadAuthorization(t *testing.T) {
	e := newEnv(t, withReads(t))
	session, account := e.setupAdmin("owner", "correct horse battery")
	e.do(http.MethodGet, "/v1/read/summary", nil, browser(session)).expect(http.StatusForbidden, "forbidden")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	e.do(http.MethodGet, "/v1/read/summary", nil, browser(session)).must(http.StatusOK)
	tok := e.token("owner", "correct horse battery", newID(), newID())
	e.do(http.MethodGet, "/v1/read/summary", nil, bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")
	// The event stream also serves phones (TestEventStream).
	e.do(http.MethodGet, "/v1/read/summary", nil, browser(session), hdr("X-NotTennaPod-Account", newID())).
		expect(http.StatusConflict, "account_changed")
	e.do(http.MethodGet, "/v1/read/summary", nil, browser(session), hdr("X-NotTennaPod-Account", account)).must(http.StatusOK)
	// The fence applies to every request of a tab, writes included.
	e.do(http.MethodGet, "/v1/installations", nil, browser(session), hdr("X-NotTennaPod-Account", newID())).
		expect(http.StatusConflict, "account_changed")
}

// The frontier at a view's head is exactly that head's causal past: later
// commits do not change it, and every node the head held is in the past of
// one of its nodes.
func TestReadFrontier(t *testing.T) {
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	cursor := head(r.get("/v1/read/summary").must(http.StatusOK))["cursor"].(string)
	before := r.get("/v1/read/frontier?cursor=" + cursor).must(http.StatusOK)
	p := r.phoneOf("owner", "correct horse battery")
	fact := p.noteFact("after the view")
	pushed(t, p.push(op{seq: 1, fact: fact}))
	again := r.get("/v1/read/frontier?cursor=" + cursor).must(http.StatusOK)
	if string(mustJSON(t, before.body["frontier"])) != string(mustJSON(t, again.body["frontier"])) {
		t.Fatalf("the frontier of a past head changed")
	}
	latest := head(r.get("/v1/read/summary").must(http.StatusOK))["cursor"].(string)
	now := r.get("/v1/read/frontier?cursor=" + latest).must(http.StatusOK)
	list, _ := now.body["frontier"].([]any)
	found := false
	for _, x := range list {
		found = found || x == fact["op_id"]
	}
	if !found {
		t.Fatalf("the new fact is not in the frontier %v", list)
	}
	// Coverage: every node of the first head is an ancestor of (or in) its frontier.
	err := r.store.Read(context.Background(), func(tx *storage.ReadTx) error {
		nodes, err := tx.Account(r.account).Nodes()
		if err != nil {
			return err
		}
		deps := map[string][]string{}
		for _, n := range nodes {
			deps[n.ID] = n.Dependencies
		}
		seen := map[string]bool{}
		var walk func(string)
		walk = func(id string) {
			if seen[id] {
				return
			}
			seen[id] = true
			for _, d := range deps[id] {
				walk(d)
			}
		}
		for _, x := range before.body["frontier"].([]any) {
			walk(x.(string))
		}
		for _, n := range nodes {
			if !seen[n.ID] && n.ID != fact["op_id"] {
				t.Errorf("node %s is not covered by the frontier", n.ID)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r.get("/v1/read/frontier?cursor=nonsense").expect(http.StatusBadRequest, "invalid_request")
}

func mustJSON(t *testing.T, v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A commit of the account wakes its listeners; another account's does not.
func TestCommitNotifications(t *testing.T) {
	e := newEnv(t, withReads(t))
	admin, account := e.setupAdmin("owner", "correct horse battery")
	other := e.createAccount(admin, "other", "correct horse battery", false)
	mine, cancel := e.store.Subscribe(account)
	defer cancel()
	p := e.phoneOf("other", "correct horse battery")
	pushed(t, p.push(op{seq: 1, fact: p.noteFact("elsewhere")}))
	select {
	case <-mine:
		t.Fatal("another account's commit woke this account's listener")
	default:
	}
	_ = other
	q := e.phoneOf("owner", "correct horse battery")
	pushed(t, q.push(op{seq: 1, fact: q.noteFact("here")}))
	select {
	case <-mine:
	default:
		t.Fatal("the account's commit did not wake its listener")
	}
}

// The event stream: a hint with the head at once and after each commit, and
// the stream closes when the browser's installation is revoked or the server
// shuts down.
func TestEventStream(t *testing.T) {
	old := credentialInterval
	credentialInterval = 20 * time.Millisecond
	t.Cleanup(func() { credentialInterval = old })
	r := newReadEnv(t)
	srv := httptest.NewServer(r.srv)
	defer srv.Close()
	open := func() (*bufio.Reader, func()) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/events", nil)
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: r.session})
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
			t.Fatalf("events: %d %s", res.StatusCode, res.Header.Get("Content-Type"))
		}
		return bufio.NewReader(res.Body), func() { res.Body.Close() }
	}
	next := func(rd *bufio.Reader) (string, string) {
		var event, data string
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return "eof", ""
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case line == "":
				if event != "" {
					return event, data
				}
			case strings.HasPrefix(line, "event: "):
				event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			}
		}
	}
	rd, done := open()
	defer done()
	if ev, data := next(rd); ev != "changed" || !strings.Contains(data, `"seq":`) {
		t.Fatalf("first event %q %q", ev, data)
	}
	p := r.phoneOf("owner", "correct horse battery")
	// A phone's device token opens a stream too.
	preq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/events", nil)
	preq.Header.Set("Authorization", "Bearer "+p.token)
	pres, err := http.DefaultClient.Do(preq)
	if err != nil || pres.StatusCode != http.StatusOK {
		t.Fatalf("a phone's stream: %v %v", err, pres)
	}
	defer pres.Body.Close()
	prd := bufio.NewReader(pres.Body)
	next(prd)
	pushed(t, p.push(op{seq: 1, fact: p.noteFact("hint")}))
	ev, data := next(rd)
	var h struct{ Seq int64 }
	if err := json.Unmarshal([]byte(data), &h); ev != "changed" || err != nil || h.Seq < 1 {
		t.Fatalf("hint %q %q", ev, data)
	}
	if ev, _ := next(prd); ev != "changed" {
		t.Fatalf("the phone's hint %q", ev)
	}
	// Revoking the browser's installation closes its stream.
	list := r.get("/v1/installations").must(http.StatusOK)
	for _, x := range list.body["installations"].([]any) {
		inst := x.(map[string]any)
		if inst["platform"] == "web" {
			r.do(http.MethodPost, "/v1/installations/"+inst["install_id"].(string)+"/revoke", map[string]any{}, r.web).must(http.StatusOK)
		}
	}
	for {
		ev, _ := next(rd)
		if ev == "closed" || ev == "eof" {
			break
		}
	}
	// Shutdown ends streams.
	r2 := newReadEnv(t)
	srv2 := httptest.NewServer(r2.srv)
	defer srv2.Close()
	r, srv = r2, srv2
	rd2, done2 := open()
	defer done2()
	next(rd2)
	r2.srv.Shutdown()
	if ev, _ := next(rd2); ev != "eof" {
		t.Fatalf("after shutdown: %q", ev)
	}
}

// Diagnostics: devices, rejections with their reason, refresh errors without
// URL credentials, and a fact's author, save identities and covering cuts;
// another account's only for an administrator.
func TestDiagnostics(t *testing.T) {
	r := newReadEnv(t)
	install, nonce := newID(), newID()
	tok := r.signInAs("owner", "correct horse battery", install, nonce, device("iPhone", "history")).must(http.StatusOK).str("token")
	p := &phone{e: r.env, cred: bearer(tok), token: tok, install: install, nonce: nonce, author: newID()}
	p.cursor = p.attach(0).head
	note := p.noteFact("listened")
	bad := p.noteFact("")
	delete(bad["payload"].(map[string]any), "text")
	p.seq++
	deps := []string{note["op_id"].(string), bad["op_id"].(string)}
	sort.Strings(deps)
	clear := map[string]any{"op_id": newID(), "author_id": p.author, "author_seq": p.seq, "dependencies": []any{deps[0], deps[1]},
		"type": "history.clear", "type_version": 1, "client_time": time.Now().UnixMilli(), "targets": []any{}, "payload": map[string]any{}}
	pushed(t, p.push(op{seq: 1, fact: note}, op{seq: 2, fact: bad}, op{seq: 3, fact: clear}))
	// A refresh error whose detail names a URL with credentials.
	var podcast string
	err := r.srv.cfg.Protocol.ServerCommit(context.Background(), r.account, func(c *protocol.ApplyContext) error {
		podcast = "p1:" + strings.Repeat("ab", 32)
		return c.PublishRecord("podcast", podcast, "refresh_status", map[string]any{"last_attempt": 1, "last_success": nil,
			"next_attempt": 2, "error": map[string]any{"label": "Error", "code": "http_401", "detail": "https://alice:hunter2@feeds.example/x"}})
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.get("/v1/diagnostics?fact=" + note["op_id"].(string)).must(http.StatusOK)
	if strings.Contains(string(d.raw), "hunter2") || strings.Contains(string(d.raw), "alice") {
		t.Fatalf("credentials in diagnostics: %s", d.raw)
	}
	if devices, _ := d.body["devices"].([]any); len(devices) != 2 {
		t.Fatalf("devices %v", d.body["devices"])
	}
	rejections, _ := d.body["rejections"].([]any)
	if len(rejections) != 1 || rejections[0].(map[string]any)["op_id"] != bad["op_id"] || rejections[0].(map[string]any)["reason"] == nil {
		t.Fatalf("rejections %v", rejections)
	}
	if errs, _ := d.body["refresh_errors"].([]any); len(errs) != 1 {
		t.Fatalf("refresh errors %v", d.body["refresh_errors"])
	}
	fact := d.obj("fact")
	author, _ := fact["author"].(map[string]any)
	if author["raw"] != p.author || author["effective"] != p.author {
		t.Fatalf("author %v", fact["author"])
	}
	covered, _ := fact["covered_by"].([]any)
	if len(covered) != 1 || covered[0].(map[string]any)["op_id"] != clear["op_id"] {
		t.Fatalf("covered by %v", fact["covered_by"])
	}
	r.get("/v1/diagnostics?fact="+newID()).expect(http.StatusNotFound, "not_found")
	// Another account: administrators only.
	other := r.createAccount(r.session, "member", "correct horse battery", false)
	r.get("/v1/diagnostics?account=" + other).must(http.StatusOK)
	member := r.env.session("member", "correct horse battery")
	r.do(http.MethodGet, "/v1/diagnostics?account="+r.account, nil, browser(member)).expect(http.StatusForbidden, "forbidden")
	r.do(http.MethodGet, "/v1/diagnostics", nil, browser(member)).must(http.StatusOK)
}

// Diagnostics list the stored responses a re-parse gave up on (PROTOCOL §9.4) from
// the outcome kept in the clear beside each: they never open a stored
// response (each view used to decrypt the account's whole library), so even
// one that does not open is listed.
func TestDiagnosticsListReparseFailures(t *testing.T) {
	r := newReadEnv(t)
	p := r.phoneOf("owner", "correct horse battery")
	var ns string
	if err := r.store.Read(context.Background(), func(tx *storage.ReadTx) error {
		a, err := tx.GetAccount(r.account)
		ns = a.Namespace
		return err
	}); err != nil {
		t.Fatal(err)
	}
	svc := r.srv.cfg.Protocol
	given := storage.ReparseOutcome{Failure: "timeout", Timeouts: 3, Parser: "ntp-go/1"}
	var podcasts []string
	for i, o := range []storage.ReparseOutcome{given, {Timeouts: 1, Parser: "ntp-go/1"}} {
		fact := map[string]any{"op_id": newID(), "author_id": p.author, "author_seq": int64(i + 1), "dependencies": []any{}, "type": "podcast.subscribe",
			"type_version": 1, "client_time": time.Now().UnixMilli(), "targets": []any{}, "payload": map[string]any{"feed_url": "https://feeds.example/" + strconv.Itoa(i)}}
		if got := pushed(t, p.push(op{seq: int64(i + 1), fact: fact})); len(got.Results) != 1 || got.Results[0].Status != "accepted" {
			t.Fatalf("subscribe: %+v", got)
		}
		podcast := contract.PodcastID(ns, fact["op_id"].(string))
		podcasts = append(podcasts, podcast)
		b, err := feed.StoredResponse{Source: "https://feeds.example/" + strconv.Itoa(i), ParserVersion: feed.ParserVersion,
			Body: []byte("<rss/>")}.Seal(svc.Box, r.account, podcast)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			b = []byte("not a sealed response")
		}
		if err := svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
			return tx.Account(r.account).PutReparsedFeedResponse(podcast, b, o)
		}); err != nil {
			t.Fatal(err)
		}
	}
	d := r.get("/v1/diagnostics").must(http.StatusOK)
	list, _ := d.body["reparse_failures"].([]any)
	if len(list) != 1 {
		t.Fatalf("re-parse failures %v", d.body["reparse_failures"])
	}
	if f := list[0].(map[string]any); f["podcast_id"] != podcasts[0] || f["reason"] != "timeout" || f["timeouts"] != float64(3) ||
		f["parser_version"] != "ntp-go/1" {
		t.Errorf("re-parse failure %v", f)
	}
}

// The feed preview fetches as Android does: a feed with its episodes, a
// page's feed links, a 401 naming its origin, then the feed with
// credentials; and it says when the account already has the podcast.
func TestFeedPreview(t *testing.T) {
	rss := `<?xml version="1.0"?><rss version="2.0"><channel><title>Preview Lab</title><link>https://example.com/</link>
<item><title>One</title><guid>one</guid><pubDate>Mon, 05 Oct 2026 10:00:00 +0000</pubDate><enclosure url="https://example.com/1.mp3" length="10" type="audio/mpeg"/></item>
</channel></rss>`
	feeds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/feed.xml":
			w.Header().Set("Content-Type", "application/rss+xml")
			_, _ = w.Write([]byte(rss))
		case "/page":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<html><head><link rel="alternate" type="application/rss+xml" title="The feed" href="/feed.xml"></head></html>`))
		case "/private.xml":
			if u, p, ok := r.BasicAuth(); !ok || u != "alice" || p != "s3cret" {
				w.Header().Set("WWW-Authenticate", `Basic realm="x"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(rss))
		default:
			http.NotFound(w, r)
		}
	}))
	defer feeds.Close()
	fetcher := feed.NewFetcher()
	fetcher.AllowLoopback(func(string) bool { return true })
	e := newEnv(t, withReads(t), func(c *Config) { c.Fetcher = fetcher })
	session, account := e.setupAdmin("owner", "correct horse battery")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	post := func(body map[string]any) *response {
		return e.do(http.MethodPost, "/v1/read/preview", body, browser(session)).must(http.StatusOK)
	}
	got := post(map[string]any{"url": feeds.URL + "/feed.xml"})
	f := got.obj("feed")
	if f["title"] != "Preview Lab" || len(f["items"].([]any)) != 1 || got.body["podcast_id"] != nil {
		t.Fatalf("preview %s", got.raw)
	}
	page := post(map[string]any{"url": feeds.URL + "/page"})
	if links, _ := page.body["links"].([]any); len(links) != 1 || links[0].(map[string]any)["title"] != "The feed" {
		t.Fatalf("page %s", page.raw)
	}
	locked := post(map[string]any{"url": feeds.URL + "/private.xml"})
	if locked.obj("error")["label"] != "unauthorized" || locked.obj("error")["origin"] == nil {
		t.Fatalf("private %s", locked.raw)
	}
	open := post(map[string]any{"url": feeds.URL + "/private.xml", "username": "alice", "password": "s3cret"})
	if open.obj("feed")["title"] != "Preview Lab" || strings.Contains(string(open.raw), "s3cret") {
		t.Fatalf("with credentials %s", open.raw)
	}
	if blank := post(map[string]any{"url": "  "}); blank.obj("error")["label"] == nil {
		t.Fatalf("blank %s", blank.raw)
	}
	// Once the account has the podcast, the preview says which.
	e.importInto(account, "emulator-parity-library.db")
	var owned string
	_ = e.store.Read(context.Background(), func(r *storage.ReadTx) error {
		lib, err := views.Load(r.Account(account))
		for _, p := range lib.Podcasts {
			if aliases, ok := p.Records["identity"].Field("aliases").([]any); ok && len(aliases) > 0 {
				owned = aliases[0].(map[string]any)["url"].(string)
			}
		}
		return err
	})
	if owned == "" {
		t.Fatal("no podcast URL")
	}
	if again := post(map[string]any{"url": owned}); again.body["podcast_id"] == nil {
		t.Fatalf("the account's own podcast: %s", again.raw)
	}
}

// A page load learns its account and whether its session is bound.
func TestSessionEndpoint(t *testing.T) {
	e := newEnv(t, withReads(t))
	session, account := e.setupAdmin("owner", "correct horse battery")
	got := e.do(http.MethodGet, "/v1/session", nil, browser(session)).must(http.StatusOK)
	if got.obj("account")["account_id"] != account || got.body["install_id"] != nil {
		t.Fatalf("session %s", got.raw)
	}
	id := newID()
	e.registerWeb(session, id, newID()).must(http.StatusOK)
	if got := e.do(http.MethodGet, "/v1/session", nil, browser(session)).must(http.StatusOK); got.body["install_id"] != id {
		t.Fatalf("bound session %s", got.raw)
	}
	e.do(http.MethodGet, "/v1/session", nil).expect(http.StatusUnauthorized, "unauthorized")
}

// Credentials go only to the origin the user approved: after a redirect, the
// one whose 401 asked; the address's own origin never receives them. A page's
// feed link named by its URL never shows URL credentials.
func TestFeedPreviewCredentialOrigins(t *testing.T) {
	rss := `<?xml version="1.0"?><rss version="2.0"><channel><title>Private</title></channel></rss>`
	var leaked atomic.Bool
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); ok && u == "alice" && p == "s3cret" {
			_, _ = w.Write([]byte(rss))
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="x"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer private.Close()
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
		switch r.URL.Path {
		case "/feed":
			http.Redirect(w, r, private.URL+"/feed.xml", http.StatusFound)
		case "/page":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<html><head><link rel="alternate" type="application/rss+xml" href="https://bob:hunter2@feeds.example/x.xml"></head></html>`))
		}
	}))
	defer front.Close()
	fetcher := feed.NewFetcher()
	fetcher.AllowLoopback(func(string) bool { return true })
	e := newEnv(t, withReads(t), func(c *Config) { c.Fetcher = fetcher })
	session, _ := e.setupAdmin("owner", "correct horse battery")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	post := func(body map[string]any) *response {
		return e.do(http.MethodPost, "/v1/read/preview", body, browser(session)).must(http.StatusOK)
	}
	asked := post(map[string]any{"url": front.URL + "/feed"})
	origin, _ := asked.obj("error")["origin"].(string)
	if asked.obj("error")["label"] != "unauthorized" || origin == "" || !strings.HasPrefix(private.URL, origin[:strings.LastIndex(origin, ":")]) {
		t.Fatalf("challenge %s", asked.raw)
	}
	// Without the asking origin, credentials stay with the address's origin: the private host never gets them.
	if again := post(map[string]any{"url": front.URL + "/feed", "username": "alice", "password": "s3cret"}); again.obj("error")["label"] != "unauthorized" {
		t.Fatalf("credentials crossed origins: %s", again.raw)
	}
	if leaked.Load() {
		// The front origin was approved here, so it may receive them; reset for the next case.
		leaked.Store(false)
	}
	ok := post(map[string]any{"url": front.URL + "/feed", "username": "alice", "password": "s3cret", "origin": origin})
	if ok.obj("feed")["title"] != "Private" {
		t.Fatalf("approved origin %s", ok.raw)
	}
	if leaked.Load() {
		t.Fatal("the address's own origin received credentials approved for another")
	}
	e.do(http.MethodPost, "/v1/read/preview", map[string]any{"url": front.URL + "/feed", "username": "a", "password": "b", "origin": "https://x.example/path"},
		browser(session)).expect(http.StatusBadRequest, "invalid_request")
	// Credentials a push would refuse are refused here too: 4096 bytes each, counted in UTF-8.
	long := strings.Repeat("é", 2049) // 4098 bytes in 2049 characters
	for _, c := range [][2]string{{"alice", long}, {long, "s3cret"}} {
		e.do(http.MethodPost, "/v1/read/preview", map[string]any{"url": front.URL + "/feed", "username": c[0], "password": c[1], "origin": origin},
			browser(session)).expect(http.StatusBadRequest, "invalid_request")
	}
	if at := post(map[string]any{"url": front.URL + "/feed", "username": "alice", "password": strings.Repeat("x", 4096), "origin": origin}); at.obj("error")["label"] != "unauthorized" {
		t.Fatalf("a password of exactly 4096 bytes is refused: %s", at.raw)
	}
	page := post(map[string]any{"url": front.URL + "/page"})
	if strings.Contains(string(page.raw), "hunter2") || strings.Contains(string(page.raw), "bob:") {
		t.Fatalf("a link title shows credentials: %s", page.raw)
	}
}

// Publishing an import wakes the account's listeners, like any commit.
func TestImportPublicationNotifies(t *testing.T) {
	e := newEnv(t, withImports(t), withReads(t))
	session, account := e.setupAdmin("owner", "correct horse battery")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	changes, cancel := e.store.Subscribe(account)
	defer cancel()
	data := []byte(`<?xml version="1.0"?><opml version="2.0"><body><outline type="rss" text="A" xmlUrl="https://example.com/a.xml"/></body></opml>`)
	sum := sha256.Sum256(data)
	r := e.do(http.MethodPost, "/v1/import", map[string]any{"kind": "opml", "fingerprint": hex.EncodeToString(sum[:]), "size": len(data),
		"file_name": "subs.opml"}, browser(session)).must(http.StatusCreated)
	id := r.str("import_id")
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString(data)}, browser(session)).must(http.StatusOK)
	e.do(http.MethodPost, "/v1/import/"+id+"/finish", nil, browser(session)).must(http.StatusOK)
	select {
	case <-changes:
	case <-time.After(30 * time.Second):
		t.Fatal("no notification after the import was published")
	}
}
