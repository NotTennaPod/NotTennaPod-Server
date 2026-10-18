package ops

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// PROTOCOL §9.6: what a server fetch reports as catalog activity, the
// refresher's measure of a quiet feed.

// activityFeed is an RSS feed of the items, with extra channel XML.
func activityFeed(t *testing.T, now int64, extra string, items ...feedItem) *feed.Feed {
	t.Helper()
	var b strings.Builder
	b.WriteString(`<rss version="2.0"><channel><title>Show</title>` + extra)
	for _, it := range items {
		fmt.Fprintf(&b, `<item><title>%s</title><guid>guid-%s</guid><pubDate>%s</pubDate>`, it.title, it.title,
			time.UnixMilli(it.pub).UTC().Format(time.RFC1123Z))
		if it.playable {
			fmt.Fprintf(&b, `<enclosure url="https://cdn.example.com/%s.mp3" length="20000" type="audio/mpeg"/>`, it.title)
		}
		b.WriteString(`</item>`)
	}
	b.WriteString(`</channel></rss>`)
	f, err := feed.Parse(context.Background(), []byte(b.String()), "https://example.com/feed", time.UnixMilli(now))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// fetchActivity applies a server fetch as the refresher does and returns
// each activity it reported. With rollback, the commit fails after the
// fetch is applied (and is rolled back), as a commit that is retried.
func (c *catalogEnv) fetchActivity(t *testing.T, x ServerFetch, rollback bool) []FetchActivity {
	t.Helper()
	x.PodcastID, x.GenerationID, x.Namespace = c.p, c.g, c.namespace
	if x.RefreshID == "" {
		x.RefreshID = newID()
	}
	if err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error {
		rc, err := CaptureRefresh(ctx, c.p)
		x.RefreshBy, x.Bases = rc.RefreshBy, rc.Bases
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var got []FetchActivity
	x.Activity = func(a FetchActivity) { got = append(got, a) }
	errRollback := errors.New("rolled back")
	err := c.svc.ServerCommit(context.Background(), c.account, func(ctx *protocol.ApplyContext) error {
		if err := ApplyServerFetch(ctx, x); err != nil {
			return err
		}
		if rollback {
			return errRollback
		}
		return nil
	})
	if err != nil && !(rollback && errors.Is(err, errRollback)) {
		t.Fatal(err)
	}
	return got
}

// observeExact has a device observe items with exactly the fields the
// server's parse of f gives them, as a phone that reached the publisher
// before the server does.
func (c *catalogEnv) observeExact(t *testing.T, d *device, f *feed.Feed, titles ...string) {
	t.Helper()
	var items []any
	targets := []any{}
	for _, it := range f.Items {
		for _, title := range titles {
			if it.Title != nil && *it.Title == title {
				items = append(items, map[string]any{"episode": c.itemRef(title), "fields": itemFields(it)})
				targets = append(targets, map[string]any{"entity": "episode", "id": c.refs[title]["episode_id"]})
			}
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].(map[string]any)["id"].(string) < targets[j].(map[string]any)["id"].(string)
	})
	targets = append(targets, map[string]any{"entity": "podcast", "id": c.p})
	payload := map[string]any{"podcast_id": c.p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": c.clock, "items": items}
	b, _ := contract.Canonical(payload)
	v, _ := contract.DecodeJSON(b)
	deps := []string{}
	if d.last != "" {
		deps = append(deps, d.last)
	}
	c.pushOK(t, d, c.fact(d, "catalog.observe", deps, targets, v.(map[string]any)), "applied")
}

func TestApplyServerFetchReportsActivity(t *testing.T) {
	c := newCatalogEnv(t, "One", "Two", "Three", "Archive", "Phone", "Retry")
	day := int64(86_400_000)
	t0 := c.clock
	items := []feedItem{{"One", t0 - 3*day, true}, {"Two", t0 - 2*day, true}}
	phone := c.device("phone")
	for _, step := range []struct {
		name     string
		before   func()
		fetch    func() ServerFetch
		rollback bool
		want     []FetchActivity // nil: no report
	}{
		{name: "first population", fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, want: []FetchActivity{{ItemRecords: 2}}},
		{name: "unchanged", fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, want: []FetchActivity{{}}},
		{name: "the podcast's own metadata only", fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "<description>Built at "+fmt.Sprint(c.clock)+"</description>", items...)}
		}, want: []FetchActivity{{}}},
		{name: "a release", before: func() { items = append(items, feedItem{"Three", c.clock - minute, true}) }, fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, want: []FetchActivity{{ItemRecords: 1, Admitted: 1}}},
		{name: "a back-catalog item", before: func() { items = append(items, feedItem{"Archive", t0 - 1000*day, true}) }, fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, want: []FetchActivity{{ItemRecords: 1}}},
		{name: "a date correction", before: func() { items[0].pub -= day }, fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, want: []FetchActivity{{ItemRecords: 1}}},
		{name: "an item a phone saw first", before: func() {
			items = append(items, feedItem{"Phone", c.clock - minute, true})
			c.observeExact(t, phone, activityFeed(t, c.clock, "", items...), "Phone")
			c.clock += minute
		}, fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, want: []FetchActivity{{NewToServer: 1, Admitted: 1}}},
		{name: "a 304", fetch: func() ServerFetch { return ServerFetch{FetchTime: c.clock, NotModified: true} }},
		{name: "a failure", fetch: func() ServerFetch { return ServerFetch{FetchTime: c.clock} }},
		{name: "a re-parse", before: func() { items[1].pub -= day }, fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...), Reparse: true}
		}},
		{name: "a commit rolled back", before: func() { items = append(items, feedItem{"Retry", c.clock - minute, true}) }, fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, rollback: true, want: []FetchActivity{{ItemRecords: 1, Admitted: 1}}},
		{name: "the same fetch again", fetch: func() ServerFetch {
			return ServerFetch{FetchTime: c.clock, Feed: activityFeed(t, c.clock, "", items...)}
		}, want: []FetchActivity{{ItemRecords: 1, Admitted: 1}}},
	} {
		c.clock += 3_600_000
		if step.before != nil {
			step.before()
		}
		got := c.fetchActivity(t, step.fetch(), step.rollback)
		if fmt.Sprint(got) != fmt.Sprint(step.want) {
			t.Errorf("%s: reported %+v, want %+v", step.name, got, step.want)
		}
		for _, a := range got {
			if active := a.ItemRecords+a.NewToServer+a.Admitted > 0; a.Active() != active {
				t.Errorf("%s: Active() %v for %+v", step.name, a.Active(), a)
			}
		}
	}
	if got := c.record("Phone", "metadata"); !strings.Contains(got, `"title":"Phone"`) {
		t.Errorf("the phone's observation %s", got)
	}
}
