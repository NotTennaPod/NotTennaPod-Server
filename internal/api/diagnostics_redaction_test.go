// Diagnostics redaction through the HTTP response: every spelling of a URL is cut to its
// host, through the real parser, refresher and import paths.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

type redactionCase struct{ name, in, want string }

func readRedactedErrors(t *testing.T, cases []redactionCase, parser bool) {
	t.Helper()
	r := newReadEnv(t)
	for i, c := range cases {
		detail := c.in
		if parser {
			_, err := feed.Parse(context.Background(), []byte(`<html><head><title>`+c.in+`</title></head></html>`), "https://feeds.example/rss", time.Now())
			var pe *feed.ParseError
			if !errors.As(err, &pe) {
				t.Fatal(err)
			}
			detail = pe.Detail
		}
		// Same userinfo transformation as refresh.parseFailure, before persistence.
		detail = ops.StripUserinfo(detail)
		if err := r.srv.cfg.Protocol.ServerCommit(context.Background(), r.account, func(a *protocol.ApplyContext) error {
			return a.PublishRecord("podcast", fmt.Sprintf("p1:%064x", i+1), "refresh_status", map[string]any{"error": map[string]any{"detail": detail, "case": c.name}})
		}); err != nil {
			t.Fatal(err)
		}
	}
	d := r.get("/v1/diagnostics").must(http.StatusOK)
	var body struct {
		Refresh []struct {
			Status struct{ Error struct{ Detail, Case string } }
		} `json:"refresh_errors"`
	}
	if err := json.Unmarshal(d.raw, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Refresh) != len(cases) {
		t.Fatalf("got %d records", len(body.Refresh))
	}
	byName := map[string]string{}
	for _, v := range body.Refresh {
		byName[v.Status.Error.Case] = v.Status.Error.Detail
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want := c.want
			if parser {
				want = `Website title: "` + want + `"`
			}
			if got := byName[c.name]; got != want {
				t.Errorf("HTTP detail=%q, want=%q", got, want)
			}
		})
	}
}

func TestDiagnosticsRedactURLsAndKeepProse(t *testing.T) {
	readRedactedErrors(t, []redactionCase{
		{"apostrophe path", `See https://feeds.example/o'neil/PATH_SECRET?x=QUERY_SECRET#FRAGMENT_SECRET then retry.`, `See https://feeds.example/… then retry.`},
		{"apostrophe user", `https://o'neil:USER_SECRET@feeds.example/PATH_SECRET`, `https://feeds.example/…`},
		{"quoted", `href='https://feeds.example/PATH_SECRET' useful prose`, `href='https://feeds.example/…' useful prose`},
		{"named scheme", `https&colon;&sol;&sol;u:USER_SECRET&commat;feeds.example/PATH_SECRET`, `https://feeds.example/…`},
		{"numeric scheme", `&#104;&#x74;tps:&#47;&#x2f;feeds.example/PATH_SECRET`, `https://feeds.example/…`},
		{"relative", `see //feeds.example/PATH_SECRET?x=QUERY_SECRET#FRAGMENT_SECRET next`, `see //feeds.example/… next`},
		{"ipv6 relative", `//u:USER_SECRET@[2001:db8::7]:8443/PATH_SECRET`, `//[2001:db8::7]:8443/…`},
		{"ipv6", `https://[2001:db8::7]:8443?token=QUERY_SECRET`, `https://[2001:db8::7]:8443/…`},
		{"nested", `https://outer.example/path?next=https://u:USER_SECRET@inner.example/PATH_SECRET`, `https://outer.example/…`},
		{"nested fragment", `https://outer.example/#https://u:USER_SECRET@inner.example/PATH_SECRET`, `https://outer.example/…`},
		{"percent encoding", `https://feeds.example/PATH%20SECRET?next=https%3A%2F%2Finner.example%2FSECRET`, `https://feeds.example/…`},
		{"encoded apostrophe", `https://feeds.example/o&#39;neil/PATH_SECRET`, `https://feeds.example/…`},
		{"bare hosts", `feeds.example and [2001:db8::7]:8443 remain readable.`, `feeds.example and [2001:db8::7]:8443 remain readable.`},
		{"ordinary prose", `Don't lose O'Neil's message. 10/20, a//b, café 東京.`, `Don't lose O'Neil's message. 10/20, a//b, café 東京.`},
		{"quoted prose", `See "https://feeds.example/PATH_SECRET". Retry feeds.example later.`, `See "https://feeds.example/…". Retry feeds.example later.`},
		{"multiple", `https://one.example/PATH_SECRET and https://two.example/PATH_SECRET`, `https://one.example/… and https://two.example/…`},
		{"host only", `https://feeds.example:8443`, `https://feeds.example:8443`},
	}, false)
}

func TestDiagnosticsRedactParserDecodedSpellings(t *testing.T) {
	readRedactedErrors(t, []redactionCase{
		{"named", `https&colon;&sol;&sol;u:USER_SECRET&commat;feeds.example/PATH_SECRET`, `https://feeds.example/…`},
		{"numeric", `&#104;ttps://o&#39;neil:USER_SECRET&#64;feeds.example/PATH_SECRET`, `https://feeds.example/…`},
		{"quoted prose", `Read &quot;https://feeds.example/PATH_SECRET&quot; then retry.`, `Read "https://feeds.example/…" then retry.`},
		{"double escaped scheme", `https&amp;colon;&amp;sol;&amp;sol;feeds.example/PATH_SECRET`, `https://feeds.example/…`},
		{"literal named in path", `https://feeds.example/path&amp;quot;PATH_SECRET`, `https://feeds.example/…`},
		{"literal amp", `https://feeds.example/path?x=QUERY_SECRET&amp;amp;y=SECRET`, `https://feeds.example/…`},
		{"decoded space prose", `Read https://feeds.example/PATH_SECRET&#32;then retry.`, `Read https://feeds.example/… then retry.`},
	}, true)
}

func TestDiagnosticsLiteralReferencesDoNotEndACut(t *testing.T) {
	var cases []redactionCase
	for _, ref := range []string{"&#32;", "&#x20;", "&#9;", "&#10;", "&#13;", "&#34;", "&#x22;", "&#60;", "&#62;", "&#32", "&#x20"} {
		raw := "https://feeds.example/path" + ref + "FRAGMENT_SECRET?token=QUERY_SECRET"
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		// '#' is an ordinary literal fragment delimiter in this URL, not an HTML decode request.
		if u.Fragment == "" {
			t.Fatal("expected a literal URL fragment")
		}
		t.Logf("literal URL %q, path=%q fragment=%q", raw, u.Path, u.Fragment)
		cases = append(cases, redactionCase{ref, html.EscapeString(raw), "https://feeds.example/…"})
	}
	readRedactedErrors(t, cases, true)
}

func TestDiagnosticsRedactStoredImports(t *testing.T) {
	r := newReadEnv(t)
	id := newID()
	err := r.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		_, _, err := tx.Account(r.account).ReserveImport(storage.Import{ImportID: id, Kind: "opml", Fingerprint: strings.Repeat("a", 64), Size: 123, FileName: `archive https://feeds.example/o'neil/PATH_SECRET`})
		if err != nil {
			return err
		}
		return tx.Account(r.account).SetImportStatus(id, "failed", ops.StripUserinfo(`Cannot read https&colon;&sol;&sol;u:USER_SECRET&commat;feeds.example:8443/PATH_SECRET?token=QUERY_SECRET#FRAGMENT_SECRET; retry later`))
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.get("/v1/diagnostics").must(http.StatusOK)
	var b struct {
		Import struct {
			Reason   string
			FileName string `json:"file_name"`
		}
	}
	if err := json.Unmarshal(d.raw, &b); err != nil {
		t.Fatal(err)
	}
	if b.Import.Reason != `Cannot read https://feeds.example:8443/… retry later` || b.Import.FileName != `archive https://feeds.example/…` {
		t.Fatalf("HTTP import %+v", b.Import)
	}
	t.Logf("HTTP import %+v", b.Import)
	err = r.store.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(r.account).SetImportStatus(id, "failed", `Cannot read https://feeds.example/path&#32;FRAGMENT_SECRET`)
	})
	if err != nil {
		t.Fatal(err)
	}
	d = r.get("/v1/diagnostics").must(http.StatusOK)
	if strings.Contains(string(d.raw), "FRAGMENT_SECRET") {
		t.Errorf("stored import reason leaks through HTTP: %s", d.raw)
	}
}

func TestDiagnosticsRedactARealRefreshError(t *testing.T) {
	r := newReadEnv(t)
	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>https://feeds.example/path&amp;#32;FRAGMENT_SECRET?token=QUERY_SECRET</title></head></html>`)
	}))
	defer pub.Close()
	p := r.phoneOf("owner", "correct horse battery")
	fact := map[string]any{"op_id": newID(), "author_id": p.author, "author_seq": 1, "dependencies": []any{}, "type": "podcast.subscribe", "type_version": 1, "client_time": time.Now().UnixMilli(), "targets": []any{}, "payload": map[string]any{"feed_url": pub.URL + "/feed"}}
	got := pushed(t, p.push(op{1, fact, nil}))
	if len(got.Results) != 1 || got.Results[0].Status != "accepted" {
		t.Fatalf("subscribe: %+v", got)
	}
	fetcher := feed.NewFetcher()
	fetcher.AllowLoopback(func(string) bool { return true })
	refresher := refresh.New(r.srv.cfg.Protocol, refresh.Config{Fetcher: fetcher})
	if _, err := refresher.Request(context.Background(), r.account, ""); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	refresher.Start(ctx)
	defer func() { cancel(); refresher.Stop() }()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("refresh did not publish error")
		case <-ticker.C:
		}
		d := r.get("/v1/diagnostics").must(http.StatusOK)
		if !strings.Contains(string(d.raw), "unsupported_type_html") {
			continue
		}
		t.Logf("real fetch -> parse -> refresh persistence -> HTTP=%s", d.raw)
		if strings.Contains(string(d.raw), "FRAGMENT_SECRET") || strings.Contains(string(d.raw), "QUERY_SECRET") {
			t.Error("real refresh leaks literal URL fragment in diagnostics")
		}
		return
	}
}
