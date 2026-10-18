package importer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The KXmlParser oracle (NotTennaPod-iOS Tests/.../OPMLFixtures, made with
// Android's own parser): every supported case reads to the same entries, or
// fails.
func TestOPMLOracle(t *testing.T) {
	data, err := os.ReadFile("testdata/opml/oracle.json")
	if err != nil {
		t.Fatal(err)
	}
	var oracle struct {
		Provider string `json:"provider"`
		Cases    []struct {
			Name, Scope, Input, Outcome string
			Entries                     []struct {
				Title  string `json:"title"`
				XMLURL string `json:"xmlURL"`
			}
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &oracle); err != nil {
		t.Fatal(err)
	}
	supported := 0
	for _, c := range oracle.Cases {
		if c.Scope != "supported" {
			continue
		}
		supported++
		t.Run(c.Name, func(t *testing.T) {
			in, err := os.ReadFile(filepath.Join("testdata/opml", c.Input))
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadOPML(context.Background(), in)
			if c.Outcome == "error" {
				if err == nil {
					t.Fatalf("read %v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(c.Entries) {
				t.Fatalf("%d entries, want %d: %+v", len(got), len(c.Entries), got)
			}
			for i, e := range c.Entries {
				if got[i].Title != e.Title || got[i].URL != e.XMLURL {
					t.Errorf("entry %d: %q %q, want %q %q", i, got[i].Title, got[i].URL, e.Title, e.XMLURL)
				}
			}
		})
	}
	if supported != 29 {
		t.Fatalf("%d supported cases", supported)
	}
}

// OPML gives subscriptions only: one per normalized URL, no episodes, and an
// address that is not a web feed is skipped.
func TestFromOPML(t *testing.T) {
	plan := FromOPML([]OPMLEntry{{"A", "https://e.test/a.xml"}, {"A again", "HTTPS://E.TEST/a.xml"}, {"Local", "file:///x.xml"},
		{"Private", "https://user:pw@p.test/feed"}}, "1b4e28ba-2fa1-41d2-883f-0016d3cca427", 1)
	if len(plan.Podcasts) != 2 || len(plan.Merged) != 1 || len(plan.Skipped) != 1 {
		t.Fatalf("plan %+v", plan)
	}
	p := plan.Podcasts[1]
	if p.URL != "https://p.test/feed" || p.SecretID == "" || plan.Secrets[p.SecretID].Username != "user" ||
		plan.Secrets[p.SecretID].ApprovedOrigins[0] != "https://p.test:443" {
		t.Errorf("credentials in an address: %+v %+v", p, plan.Secrets)
	}
	for _, p := range plan.Podcasts {
		if p.State != "subscribed" || !p.KeepUpdated || p.NewEpisodes != "global" || len(p.Rows) != 0 {
			t.Errorf("OPML podcast %+v", p)
		}
	}
}

// Credentials in an address never become a title.
func TestOPMLTitlesCarryNoCredentials(t *testing.T) {
	entries, err := ReadOPML(context.Background(), []byte(`<opml><body><outline xmlUrl="https://alice:secret@example.com/feed"/></body></opml>`))
	if err != nil || len(entries) != 1 {
		t.Fatalf("%v %v", entries, err)
	}
	if strings.Contains(entries[0].Title, "secret") || entries[0].Title != "https://example.com/feed" {
		t.Fatalf("title %q", entries[0].Title)
	}
	plan := FromOPML(entries, "1b4e28ba-2fa1-41d2-883f-0016d3cca427", 1)
	if p := plan.Podcasts[0]; strings.Contains(p.Title, "secret") || strings.Contains(p.URL, "secret") || p.SecretID == "" {
		t.Fatalf("podcast %+v", p)
	}
}

// Android's OpmlImportActivity adds each feed titled by its outline (new
// Feed(xmlUrl, null, text)), so a feed it cannot fetch keeps that name: the
// imported podcast's record holds the entry's title until its first fetch,
// which is still a first population that files nothing (PROTOCOL §11.5).
func TestOPMLImportKeepsOutlineTitles(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	entries, err := ReadOPML(ctx, []byte(`<opml version="2.0"><body>
<outline type="rss" text="Text name" title="Title name" xmlUrl="https://titled.example.invalid/feed.xml"/>
<outline type="rss" text="Only text" xmlUrl="https://text.example.invalid/feed.xml"/>
<outline type="rss" xmlUrl="https://bob:pw@bare.example.invalid/feed.xml"/>
</body></opml>`))
	if err != nil {
		t.Fatal(err)
	}
	const importID = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"
	plan := FromOPML(entries, importID, e.clock)
	pods, err := Apply(ctx, e.svc, e.account, plan, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Complete(ctx, e.svc, e.account, importID, "", plan); err != nil {
		t.Fatal(err)
	}
	lib := e.library()
	for i, want := range []string{"Title name", "Only text", "https://bare.example.invalid/feed.xml"} {
		p := pods[i]
		pod := lib.Podcasts[p.PodcastID]
		if pod == nil || pod.Title == nil || *pod.Title != want || pod.DisplayTitle != want {
			t.Errorf("podcast %d reads as %+v, want %q", i, pod, want)
		}
		if node, seen := e.populated(p.PodcastID, p.GenerationID); node != "" || len(seen) != 0 {
			t.Errorf("podcast %d is populated by %q with seen history %v", i, node, seen)
		}
	}
	// The first fetch is a first population: it files nothing.
	e.clock += 86_400_000
	refresh := "3f0c2a8e-4b1d-4e6f-9a2c-5d7e8f901234"
	e.fetch(pods[0].PodcastID, refresh, "Fetched name", rssItem{"New", e.clock - 60_000})
	if r := e.part0(pods[0].PodcastID, refresh)["result"].(map[string]any); r["population"] != true {
		t.Errorf("the first fetch's part 0: %v", r)
	}
	if got := e.decisions("discovery.admission"); len(got) != 0 {
		t.Errorf("the first population filed %d", len(got))
	}
	if pod := e.library().Podcasts[pods[0].PodcastID]; pod.Title == nil || *pod.Title != "Fetched name" {
		t.Errorf("after the first fetch the podcast reads as %+v", pod)
	}
}
