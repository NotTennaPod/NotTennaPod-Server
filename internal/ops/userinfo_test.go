package ops

// URL userinfo leaves GUIDs and text whatever its spelling.

import (
	"context"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The embedded-URL rule's own cases (suite strip_userinfo), from the
// reference's rules.strip_userinfo.
func TestStripUserinfoVectors(t *testing.T) {
	data, err := os.ReadFile("../../contract/vectors/strip_userinfo.json")
	if err != nil {
		t.Fatal(err)
	}
	v, err := contract.DecodeJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	suite := v.(map[string]any)
	if suite["suite"] != "strip_userinfo" {
		t.Fatalf("suite %v", suite["suite"])
	}
	cases, _ := suite["cases"].([]any)
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range cases {
		m := c.(map[string]any)
		in, want := m["input"].(string), m["output"].(string)
		if got := StripUserinfo(in); got != want {
			t.Errorf("StripUserinfo(%q) = %q, want %q", in, got, want)
		}
	}
}

// A feed's GUID with credentials, and credentials in text written with a
// character reference or an apostrophe, never reach records or the log.
func TestFeedCredentialsStayOutOfRecordsAndLog(t *testing.T) {
	for _, tc := range []struct{ name, field, value string }{
		{"guid", "guid", "https://alice:secret_pw@example.org/episode"},
		{"character reference", "description", `<a href="https://alice:secret_pw&#64;example.org/audio">audio</a>`},
		{"apostrophe", "description", `See https://o'brien:secret_pw@example.org/audio`},
		{"several @", "description", `See https://alice:secret_pw@extra@example.org/audio`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCatalogEnv(t, "One")
			raw := `<rss><channel><title>Feed</title><item><title>One</title><guid>ordinary</guid><description>ordinary</description>` +
				`<enclosure url="https://example.org/audio" type="audio/mpeg"/></item></channel></rss>`
			raw = strings.Replace(raw, "<"+tc.field+">ordinary</"+tc.field+">", "<"+tc.field+"><![CDATA["+tc.value+"]]></"+tc.field+">", 1)
			f, err := feed.Parse(context.Background(), []byte(raw), "https://example.org/feed", time.UnixMilli(c.clock))
			if err != nil {
				t.Fatal(err)
			}
			c.serverFetch(t, f, "")
			for k, v := range c.records() {
				if strings.Contains(v, "secret_pw") {
					t.Errorf("record %s keeps the credentials: %s", k, v)
				}
			}
			if tc.field == "guid" {
				// The GUID without its userinfo derives the episode.
				e, _ := contract.EpisodeID(c.namespace, c.p, contract.KindRSSGUID, "https://example.org/episode")
				if c.records()["episode/"+e+"/identity"] == "" {
					t.Errorf("no episode derived from the GUID without its userinfo: %v", c.records())
				}
			}
			err = c.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
				cms, err := r.Account(c.account).Commits(0, 1<<40, 0)
				for _, cm := range cms {
					if strings.Contains(string(cm.Canonical), "secret_pw") {
						t.Errorf("commit %d keeps the credentials", cm.Seq)
					}
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The scrub reads the text's decoded view one character at a time, so a large
// text without a URL costs about nothing beyond itself, and one with a URL
// about one copy (a decoded copy of the whole text used to cost about 20 times
// its size).
func TestStripUserinfoCostsAboutTheTextItself(t *testing.T) {
	prose := strings.Repeat("ordinary prose ", 600000) + " & company"
	allocated := func(s string) uint64 {
		runtime.GC()
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		StripUserinfo(s)
		runtime.ReadMemStats(&b)
		return b.TotalAlloc - a.TotalAlloc
	}
	if got := allocated(prose); got > uint64(len(prose))/10 {
		t.Errorf("%d bytes of prose without a URL allocated %d bytes", len(prose), got)
	}
	withURL := prose + " https://alice:secret_pw&#64;example.org/x"
	if got := StripUserinfo(withURL); strings.Contains(got, "secret_pw") || len(got) != len(withURL)-len("alice:secret_pw&#64;") {
		t.Fatalf("the URL at the end kept its userinfo: %q", got[len(got)-60:])
	}
	if got := allocated(withURL); got > 3*uint64(len(withURL)) {
		t.Errorf("%d bytes of prose with one URL allocated %d bytes", len(withURL), got)
	}
}

// FindURLs finds URLs as the strip does, for diagnostics to cut them to
// scheme://host[:port]: each runs on through its path, query and fragment.
func TestFindURLs(t *testing.T) {
	for _, tc := range []struct {
		text string
		want []URL
	}{
		// An @ written as a character reference; the URL ends at a space.
		{"see https://u:pw&#64;h.example:8443/p?x=1 end", []URL{{Start: 4, End: 41, HostEnd: 35, Scheme: "https", Host: "h.example:8443"}}},
		// An apostrophe in a path belongs to the URL, and in userinfo too.
		{"https://o'brien:pw@h.example/o'neil/rss?token=Q more", []URL{{Start: 0, End: 47, HostEnd: 28, Scheme: "https", Host: "h.example"}}},
		// A URL right after an apostrophe ends at the next one.
		{"href='https://h.example/p?t=1' x", []URL{{Start: 6, End: 29, HostEnd: 23, Scheme: "https", Host: "h.example"}}},
		// Scheme-relative, at the start and after a control character.
		{"//host.example/p?token=SECRET", []URL{{Start: 0, End: 29, HostEnd: 14, Host: "host.example"}}},
		{"a\x01//u@h.example/p", []URL{{Start: 2, End: 17, HostEnd: 15, Host: "h.example"}}},
		// A scheme and :// written as character references.
		{"&#x68;ttps&colon;&sol;&sol;h.example/p", []URL{{Start: 0, End: 38, HostEnd: 36, Scheme: "https", Host: "h.example"}}},
		// A URL inside another's query is part of it; quotes end a URL.
		{`<a href="https://a.example/x?u=https://b.example/y">`, []URL{{Start: 9, End: 50, HostEnd: 26, Scheme: "https", Host: "a.example"}}},
		{"no url here, a@b and mailto:c@d", nil},
	} {
		got := FindURLs(tc.text)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("FindURLs(%q) = %+v, want %+v", tc.text, got, tc.want)
		}
		for _, u := range got {
			if u.Start < 0 || u.End > len(tc.text) || u.Start >= u.End || u.HostEnd <= u.Start || u.HostEnd > u.End {
				t.Errorf("FindURLs(%q): span %d..%d", tc.text, u.Start, u.End)
			}
		}
	}
}
