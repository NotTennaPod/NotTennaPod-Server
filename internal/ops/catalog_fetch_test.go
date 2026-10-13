package ops

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// PROTOCOL §9.4: what a server fetch writes (point 7).

// head is the account log's head seq.
func (c *catalogEnv) head(t *testing.T) int64 {
	t.Helper()
	var seq int64
	err := c.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(c.account).Head()
		seq = h.Seq
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// decisionsOf lists the account's decisions of a type, in rank order.
func (c *catalogEnv) decisionsOf(t *testing.T, decisionType string) []map[string]any {
	t.Helper()
	var out []map[string]any
	err := c.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		acct := r.Account(c.account)
		ids, err := acct.OrderedNodeIDs()
		if err != nil {
			return err
		}
		for _, id := range ids {
			b, err := acct.DecisionBytes(id)
			if err != nil {
				continue // a fact
			}
			v, err := contract.DecodeJSON(b)
			if err != nil {
				return err
			}
			if d := v.(map[string]any); d["decision_type"] == decisionType {
				out = append(out, d)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// apply applies a server fetch for the live generation as the scheduler
// does: the context captured first, then the result in its own commit.
func (c *catalogEnv) apply(t *testing.T, x ServerFetch) {
	t.Helper()
	x.PodcastID, x.GenerationID, x.Namespace = c.p, c.g, c.namespace
	if x.RefreshID == "" {
		x.RefreshID = newID()
	}
	err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error {
		rc, err := CaptureRefresh(ctx, c.p)
		x.RefreshBy, x.Bases = rc.RefreshBy, rc.Bases
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error { return ApplyServerFetch(ctx, x) }); err != nil {
		t.Fatal(err)
	}
}

// An unchanged successful fetch of a populated, subscribed generation writes
// nothing unless its status is due (PROTOCOL §9.4): the log head stays.
func TestUnchangedFetchWritesNothing(t *testing.T) {
	c := newCatalogEnv(t, "One", "Two")
	day := int64(86_400_000)
	items := []feedItem{{"One", c.clock - 2*day, true}, {"Two", c.clock - day, true}}
	c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), items...), "")
	c.clock += 3_600_000
	before := c.head(t)
	c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), items...), "")
	if after := c.head(t); after != before {
		t.Fatalf("an unchanged fetch wrote %d commits", after-before)
	}
	// A due status alone is a part 0 with the status and nothing else.
	status := map[string]any{"last_attempt": c.clock, "last_success": c.clock, "error": nil, "next_attempt": nil}
	c.apply(t, ServerFetch{FetchTime: c.clock, Feed: serverFeed(t, time.UnixMilli(c.clock), items...), Status: status})
	if after := c.head(t); after != before+1 {
		t.Fatalf("a due status wrote %d commits, want 1", after-before)
	}
	parts := c.decisionsOf(t, "catalog.refresh")
	last := parts[len(parts)-1]
	if res := last["result"].(map[string]any); len(res) != 2 || res["status"] == nil || len(res["items"].([]any)) != 0 {
		t.Errorf("status-only part 0: %s", js(res))
	}
}

// A 304 confirms the stored response: a success without records, evidence
// or discovery. It writes a part 0 only when its status is due, never a
// population marker.
func TestNotModifiedFetch(t *testing.T) {
	c := newCatalogEnv(t, "One")
	day := int64(86_400_000)
	// A 304 before any 200: not the generation's first population.
	status := map[string]any{"last_attempt": c.clock, "last_success": c.clock, "error": nil, "next_attempt": nil}
	before := c.head(t)
	c.apply(t, ServerFetch{FetchTime: c.clock, NotModified: true, CurrentStatus: status})
	if after := c.head(t); after != before {
		t.Fatalf("a 304 without a due status wrote %d commits", after-before)
	}
	c.apply(t, ServerFetch{FetchTime: c.clock, NotModified: true, Status: status})
	parts := c.decisionsOf(t, "catalog.refresh")
	if len(parts) != 1 {
		t.Fatalf("%d parts, want 1", len(parts))
	}
	in, res := parts[0]["inputs"].(map[string]any), parts[0]["result"].(map[string]any)
	if in["outcome"] != "success" || res["population"] != nil || res["feed"] != nil || len(res["items"].([]any)) != 0 {
		t.Errorf("a 304's part 0: inputs %s, result %s", js(in), js(res))
	}
	if got := c.records()["podcast/"+c.p+"/refresh_status"]; got != js(status) {
		t.Errorf("refresh_status %s, want %s", got, js(status))
	}
	// The first 200 is still the generation's first population: it files nothing.
	c.clock += day
	c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"One", c.clock - minute, true}), "")
	if got := c.record("One", "admission"); got != "" {
		t.Errorf("the first 200 after a 304 filed: %s", got)
	}
}

// A fetch arrival's records (record-level input): part 0 holds the status,
// the population marker, the feed record and the first 200 changed items;
// later items follow in parts of 200; each server admission depends on the
// part holding its record and resolves nothing for the Inbox.
func TestRefreshPartLayout(t *testing.T) {
	c := newCatalogEnv(t)
	feedFields := map[string]any{"title": "Show", "description": nil, "author": nil, "language": nil, "link": nil, "image_url": nil, "type": nil,
		"funding": []any{}, "paged": false, "next_page_url": nil}
	item := func(i int, pub int64) ServerItem {
		guid := fmt.Sprintf("item-%03d", i)
		return ServerItem{
			Occurrence: map[string]any{"guid": guid, "k": contract.KindRSSGUID, "v": guid, "title": guid, "publication_time": pub},
			Fields: map[string]any{"title": guid, "description": nil, "link": nil, "pub_date": pub, "image_url": nil, "payment_link": nil,
				"media":    map[string]any{"url": "https://cdn.example.com/" + guid + ".mp3", "size": "1000", "mime_type": "audio/mpeg", "duration": int64(0)},
				"chapters": nil, "chapter_url": nil, "transcript_url": nil, "transcript_type": nil, "social_interact_url": nil},
		}
	}
	status := func() map[string]any {
		return map[string]any{"last_attempt": c.clock, "last_success": c.clock, "error": nil, "next_attempt": nil}
	}
	c.apply(t, ServerFetch{FetchTime: c.clock, FeedFields: feedFields, Items: []ServerItem{item(0, c.clock-minute)}, Status: status()})
	first := c.decisionsOf(t, "catalog.refresh")
	if len(first) != 1 {
		t.Fatalf("first population: %d parts", len(first))
	}
	if res := first[0]["result"].(map[string]any); res["population"] != true || res["feed"] == nil || len(res["items"].([]any)) != 1 {
		t.Fatalf("first population's part 0: %s", js(res))
	}
	c.clock += minute
	items := []ServerItem{item(0, c.clock-2*minute)}
	for i := 1; i <= 205; i++ {
		items = append(items, item(i, c.clock))
	}
	refresh := newID()
	c.apply(t, ServerFetch{RefreshID: refresh, FetchTime: c.clock, FeedFields: feedFields, Items: items, CurrentStatus: status()})
	var part0, part1 map[string]any
	for _, d := range c.decisionsOf(t, "catalog.refresh") {
		switch d["subject"] {
		case "refresh:" + c.p + ":" + refresh + ":0":
			part0 = d
		case "refresh:" + c.p + ":" + refresh + ":1":
			part1 = d
		}
	}
	if part0 == nil || part1 == nil {
		t.Fatal("missing parts")
	}
	r0, r1 := part0["result"].(map[string]any), part1["result"].(map[string]any)
	// item-000 is unchanged; items 1–205 are new.
	if len(r0["items"].([]any)) != 200 || len(r1["items"].([]any)) != 5 || r0["status"] == nil || r1["status"] != nil || r0["population"] != nil {
		t.Fatalf("parts: %d and %d items, part 0 %s", len(r0["items"].([]any)), len(r1["items"].([]any)), js(map[string]any{"status": r0["status"], "population": r0["population"]}))
	}
	inPart := map[string]string{}
	for _, part := range []map[string]any{part0, part1} {
		for _, x := range part["result"].(map[string]any)["items"].([]any) {
			inPart[x.(map[string]any)["episode_id"].(string)] = part["decision_id"].(string)
		}
	}
	admissions := c.decisionsOf(t, "discovery.admission")
	if len(admissions) != 205 {
		t.Fatalf("%d admissions, want 205", len(admissions))
	}
	for _, a := range admissions {
		res, in := a["result"].(map[string]any), a["inputs"].(map[string]any)
		e := res["episode_id"].(string)
		if in["found_by"] != inPart[e] || js(a["dependencies"]) != js([]any{inPart[e]}) || res["proposal_id"] != nil || res["source"] != "server" {
			t.Fatalf("admission %s", js(a))
		}
		if _, resolved := in["enqueue_location"]; resolved {
			t.Fatalf("an Inbox admission resolved a queue placement: %s", js(a))
		}
	}
}

// The refresh status is due when nothing was published, when the outcome
// class or the error changes, and once a day otherwise (PROTOCOL §9.4); a
// failure keeps the last success.
func TestRefreshStatusRule(t *testing.T) {
	hour := int64(3_600_000)
	ok := func(at int64) FetchOutcome { return FetchOutcome{FetchTime: at} }
	fail := func(at int64, label string) FetchOutcome {
		success := int64(1000)
		return FetchOutcome{FetchTime: at, Error: map[string]any{"label": label, "code": nil, "detail": nil}, LastSuccess: &success}
	}
	published := func(o FetchOutcome) map[string]any {
		v, _ := RefreshStatus(nil, o)
		return normalizeJSON(v).(map[string]any)
	}
	cases := []struct {
		name    string
		current map[string]any
		o       FetchOutcome
		due     bool
	}{
		{"first", nil, ok(1000), true},
		{"same class within a day", published(ok(1000)), ok(1000 + 23*hour), false},
		{"a day later", published(ok(1000)), ok(1000 + 24*hour), true},
		{"success to failure", published(ok(1000)), fail(1000+hour, "network"), true},
		{"same error", published(fail(1000, "network")), fail(1000+hour, "network"), false},
		{"another error", published(fail(1000, "network")), fail(1000+hour, "parse"), true},
		{"failure to success", published(fail(1000, "network")), ok(1000 + hour), true},
	}
	for _, tc := range cases {
		if _, due := RefreshStatus(tc.current, tc.o); due != tc.due {
			t.Errorf("%s: due %v, want %v", tc.name, due, tc.due)
		}
	}
	v, _ := RefreshStatus(nil, fail(5000, "network"))
	if v["last_success"] != int64(1000) || v["last_attempt"] != int64(5000) {
		t.Errorf("a failure's status %s", js(v))
	}
}

// A URL with credentials inside feed text (a description, a title) never
// reaches the catalog records or the log.
func TestEmbeddedCredentialsNeverReachTheCatalog(t *testing.T) {
	c := newCatalogEnv(t, "One")
	now := time.UnixMilli(c.clock)
	f := serverFeed(t, now, feedItem{"One", c.clock - 86_400_000, true})
	desc := `Download https://alice:s3cret@cdn.example.com/e.mp3 or <a href="//carol:pw9xyz@x.example/">here</a>, mail a@b.example`
	title := "One (see ftp://bob:hunter2@files.example.com/x)"
	f.Items[0].Description, f.Items[0].Title = &desc, &title
	c.serverFetch(t, f, "")
	for k, v := range c.records() {
		if strings.Contains(v, "s3cret") || strings.Contains(v, "hunter2") || strings.Contains(v, "pw9xyz") {
			t.Errorf("%s holds credentials: %s", k, v)
		}
	}
	err := c.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		commits, err := r.Account(c.account).Commits(0, 1<<40, 0)
		for _, cm := range commits {
			if strings.Contains(string(cm.Canonical), "s3cret") || strings.Contains(string(cm.Canonical), "hunter2") ||
				strings.Contains(string(cm.Canonical), "pw9xyz") {
				t.Errorf("commit %d holds credentials", cm.Seq)
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.record("One", "metadata"); !strings.Contains(got, "https://cdn.example.com/e.mp3") || !strings.Contains(got, "a@b.example") {
		t.Errorf("the text lost more than credentials: %s", got)
	}
}

// An item a device observed first, which the server's first fetch sees
// unchanged, enters the server's history with archived evidence: part 0 lists
// it as seen.
func TestUnchangedItemsEnterHistoryWithEvidence(t *testing.T) {
	c := newCatalogEnv(t, "E")
	a := c.device("A")
	day := int64(86_400_000)
	c.pushOK(t, a, c.observe(a, map[string]int64{"E": c.clock - day}, "E"), "applied")
	c.clock += minute
	f := serverFeed(t, time.UnixMilli(c.clock), feedItem{"E", c.clock - minute - day, true})
	// Match the device's record exactly, so the server's record is unchanged.
	f.Items[0].Media.Size = 1000
	c.serverFetch(t, f, "")
	ep := c.refs["E"]["episode_id"].(string)
	found := false
	err := c.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		commits, err := r.Account(c.account).Commits(0, 1<<40, 0)
		for _, cm := range commits {
			if strings.Contains(string(cm.Canonical), `"seen":["`+ep+`"]`) {
				found = true
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("no refresh part records that the server saw %s", ep)
	}
}

// When part 0 is written early for identity evidence (here a GUID change), the
// newly seen unchanged item is recorded by the next part instead.
func TestSeenAfterAnEarlyPart0(t *testing.T) {
	c := newCatalogEnv(t, "E")
	a := c.device("A")
	day := int64(86_400_000)
	c.serverFetch(t, serverFeed(t, time.UnixMilli(c.clock), feedItem{"X", c.clock - 3*day, true}), "")
	c.clock += minute
	c.pushOK(t, a, c.observe(a, map[string]int64{"E": c.clock - day}, "E"), "applied")
	c.clock += minute
	f := serverFeed(t, time.UnixMilli(c.clock), feedItem{"X", c.clock - 2*minute - 3*day, true}, feedItem{"E", c.clock - minute - day, true})
	renamed := "guid-X-renamed"
	f.Items[0].GUID = &renamed // the same episode under a new GUID
	f.Items[1].Media.Size = 1000
	c.serverFetch(t, f, "")
	ep := c.refs["E"]["episode_id"].(string)
	found := false
	err := c.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		commits, err := r.Account(c.account).Commits(0, 1<<40, 0)
		for _, cm := range commits {
			body := string(cm.Canonical)
			if strings.Contains(body, `"episode.guid_change"`) && strings.Contains(body, `"seen":["`+ep+`"]`) {
				found = true
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("the GUID-change fetch does not record that the server saw %s", ep)
	}
}
