package api

// The list views' validity over HTTP (PROTOCOL §12): a token of a view
// whose account has since changed its library generation (a rebuild, an
// import) or lineage expires at once, whatever other request came first,
// and never serves what the change replaced; a page being made when its
// account is purged or its generation changes is not served; queue anchors
// that have no place in the queue fall back to their index.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
)

// importOverHTTP imports an Android backup as a browser does, and waits
// for its publication (PROTOCOL §11.5): the staged library is published in
// one transaction that raises the account's generation.
func (r *readEnv) importOverHTTP(path string) {
	r.t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		r.t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	reserve := map[string]any{"kind": "android_db", "fingerprint": hex.EncodeToString(sum[:]), "size": len(data), "file_name": "AntennaPodBackup.db"}
	id := r.do(http.MethodPost, "/v1/import", reserve, r.web).must(http.StatusCreated).str("import_id")
	const chunk = 4 << 20
	for n := 0; n*chunk < len(data); n++ {
		part := data[n*chunk : min(len(data), (n+1)*chunk)]
		r.do(http.MethodPut, "/v1/import/"+id+"/chunks/"+itoa(n), map[string]any{"data": base64.StdEncoding.EncodeToString(part)}, r.web).must(http.StatusOK)
	}
	r.do(http.MethodPost, "/v1/import/"+id+"/finish", nil, r.web).must(http.StatusOK)
	deadline := time.Now().Add(90 * time.Minute) // a power user's library
	for {
		got := r.do(http.MethodGet, "/v1/import/"+id, nil, r.web).must(http.StatusOK)
		switch got.str("status") {
		case "published":
			return
		case "failed":
			r.t.Fatalf("import %s", got.raw)
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("import still %s", got.str("status"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// readPhone signs a phone in with the capabilities the facts need.
func (r *readEnv) readPhone() *phone {
	r.t.Helper()
	id, nonce := newID(), newID()
	res := r.signInAs("owner", "correct horse battery", id, nonce, device("phone", "favorites", "history", "inbox", "queue", "statistics")).must(http.StatusOK)
	ph := &phone{e: r.env, cred: bearer(res.str("token")), token: res.str("token"), install: id, nonce: nonce, author: newID()}
	ph.cursor = ph.attachHead()
	return ph
}

// attachHead attaches the phone and returns the snapshot's head: the pages
// are followed, their items (framed ones too, in a power user's library)
// not kept.
func (p *phone) attachHead() string {
	p.e.t.Helper()
	body := map[string]any{"mode": "attach", "install_id": p.install, "acked_install_seq": 0}
	for {
		pg := decodePage(p.e.t, p.e.do(http.MethodPost, "/v1/snapshot", body, p.cred))
		if !pg.HasMore {
			return pg.Head
		}
		body["page"] = pg.Next
	}
}

func (r *readEnv) oracle() *reference.Library {
	r.t.Helper()
	var ref *reference.Library
	if err := r.store.Read(context.Background(), func(x *storage.ReadTx) error {
		var err error
		ref, err = reference.Load(x, r.account)
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
	return ref
}

func (r *readEnv) currentIdentity() views.Identity {
	r.t.Helper()
	var id views.Identity
	if err := r.store.Read(context.Background(), func(x *storage.ReadTx) error {
		var err error
		id, err = views.CurrentIdentity(x.Account(r.account))
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
	return id
}

// pageItems are a list response's rows, as served.
func pageItems(t *testing.T, res *response) []json.RawMessage {
	var items []json.RawMessage
	json.Unmarshal(bodyValues(t, res)["items"], &items)
	return items
}

// rowID is a list row's episode ID.
func rowID(raw json.RawMessage) string {
	var row struct {
		ID string `json:"episode_id"`
	}
	json.Unmarshal(raw, &row)
	return row.ID
}

func sameRows(t *testing.T, what string, got, want []json.RawMessage) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, the oracle %d", what, len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("%s: row %d\n%s\nthe oracle\n%s", what, i, got[i], want[i])
		}
	}
}

var allStates = "/v1/read/episodes?filter=" + url.QueryEscape("include_subscribed,include_archived,include_not_subscribed")

// A token from before a rebuild, read before any other request, is 410 at
// once; so are the start pages of an empty account's lists after an
// import's publication. A view whose next page holds a value a rebuild
// repairs never serves that value again: its token is 410, and the list
// loaded again is the repaired one.
func TestTokensExpireWithTheGeneration(t *testing.T) {
	ctx := context.Background()
	t.Run("rebuild", func(t *testing.T) {
		r := newReadEnv(t)
		r.importInto(r.account, "emulator-parity-library.db")
		tok := r.get(allStates + "&limit=2").must(http.StatusOK).str("next_page_token")
		before := r.currentIdentity()
		if _, err := r.srv.cfg.Protocol.Rebuild(ctx, r.account); err != nil {
			t.Fatal(err)
		}
		if after := r.currentIdentity(); after.Generation <= before.Generation {
			t.Fatalf("the rebuild did not raise the generation: %+v, %+v", before, after)
		}
		r.get(allStates+"&limit=2&page_token="+tok).expect(http.StatusGone, "view_expired")
	})
	t.Run("repaired", func(t *testing.T) {
		r := newReadEnv(t)
		r.importInto(r.account, "emulator-parity-library.db")
		ref := r.oracle()
		want, _, _ := ref.ListItems("/v1/read/episodes", "", url.Values{"filter": {"include_subscribed,include_archived,include_not_subscribed"}})
		second := rowID(want[1])
		const wrong = "A title the archive never had"
		if err := r.store.Write(ctx, func(tx *storage.WriteTx) error {
			a := tx.Account(r.account)
			rec, err := a.Record("episode", second, "metadata")
			if err != nil {
				return err
			}
			var m map[string]any
			dec := json.NewDecoder(strings.NewReader(rec.Value))
			dec.UseNumber()
			if err := dec.Decode(&m); err != nil {
				return err
			}
			m["title"] = wrong
			b, _ := json.Marshal(m)
			rec.Value = string(b)
			return a.PutRecord(rec)
		}); err != nil {
			t.Fatal(err)
		}
		first := r.get(allStates + "&limit=1").must(http.StatusOK)
		tok := first.str("next_page_token")
		// The view holds the wrong title on its next page.
		stale, _, _ := r.oracle().ListItems("/v1/read/episodes", "", url.Values{"filter": {"include_subscribed,include_archived,include_not_subscribed"}})
		if !bytes.Contains(stale[1], []byte(wrong)) {
			t.Fatal("the edited title is not on the view's second page")
		}
		if _, err := r.srv.cfg.Protocol.Rebuild(ctx, r.account); err != nil {
			t.Fatal(err)
		}
		repaired, _, _ := r.oracle().ListItems("/v1/read/episodes", "", url.Values{"filter": {"include_subscribed,include_archived,include_not_subscribed"}})
		for _, row := range repaired {
			if bytes.Contains(row, []byte(wrong)) {
				t.Fatal("the rebuild did not repair the title")
			}
		}
		res := r.get(allStates + "&limit=1&page_token=" + tok)
		if bytes.Contains(res.raw, []byte(wrong)) {
			t.Fatalf("the old token served the repaired-away title: %d", res.status)
		}
		res.expect(http.StatusGone, "view_expired")
		again := r.get(allStates + "&limit=500").must(http.StatusOK)
		sameRows(t, "the list loaded again", pageItems(t, again), repaired)
	})
	t.Run("import", func(t *testing.T) {
		r := newReadEnv(t, withImports(t))
		lists := []string{allStates, "/v1/read/queue?", "/v1/read/history?", "/v1/read/inbox?"}
		var toks []string
		for _, l := range lists {
			res := r.get(l + "&anchor_index=0&limit=1").must(http.StatusOK)
			if res.body["total"] != float64(0) {
				t.Fatalf("%s: an empty account lists %v", l, res.body["total"])
			}
			toks = append(toks, res.str("start_page_token"))
		}
		before := r.currentIdentity()
		r.importOverHTTP("../importer/androiddb/testdata/android-3.12.1/emulator-parity-library.db")
		if after := r.currentIdentity(); after.Generation <= before.Generation {
			t.Fatalf("the import's publication did not raise the generation: %+v, %+v", before, after)
		}
		ref := r.oracle()
		for i, l := range lists {
			r.get(l+"&limit=1&page_token="+toks[i]).expect(http.StatusGone, "view_expired")
			u, _ := url.Parse(l)
			want, _, _ := ref.ListItems(u.Path, "", u.Query())
			sameRows(t, l+" after the import", pageItems(t, r.get(l+"&limit=500").must(http.StatusOK)), want)
		}
	})
}

// A page being made when its account is purged, or when its library's
// generation changes (a rebuild commits), is not served: a token's read
// is 410, and a first request starts again and serves the new state.
func TestPagesAreNotServedAfterInvalidation(t *testing.T) {
	ctx := context.Background()
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	spool := r.srv.cfg.Views
	defer spool.HookPagesForTesting(nil)
	rebuild := func() {
		if _, err := r.srv.cfg.Protocol.Rebuild(ctx, r.account); err != nil {
			t.Error(err)
		}
	}
	for _, c := range []struct {
		name   string
		during func()
	}{{"a purge", func() { spool.Retire(r.account) }}, {"a rebuild", rebuild}} {
		// A token's page.
		tok := r.get(allStates + "&limit=3").must(http.StatusOK).str("next_page_token")
		calls := 0
		spool.HookPagesForTesting(func() {
			if calls++; calls == 1 {
				c.during()
			}
		})
		r.get(allStates+"&limit=3&page_token="+tok).expect(http.StatusGone, "view_expired")
		if calls != 1 {
			t.Fatalf("%s during a token's page: %d pages made", c.name, calls)
		}
		// A first request: made again from the new state.
		calls = 0
		res := r.get(allStates + "&limit=3").must(http.StatusOK)
		spool.HookPagesForTesting(nil)
		if calls != 2 {
			t.Fatalf("%s during a first request: %d pages made", c.name, calls)
		}
		ref := r.oracle()
		want, _, _ := ref.ListItems("/v1/read/episodes", "", url.Values{"filter": {"include_subscribed,include_archived,include_not_subscribed"}})
		sameRows(t, c.name+": the first page made again", pageItems(t, res), want[:3])
		if h, _ := headJSON(ref.Head); fmt.Sprint(h["cursor"]) != fmt.Sprint(res.obj("head")["cursor"]) {
			t.Fatalf("%s: the page is not at the new head", c.name)
		}
	}
}

// The pages' last check is ordered against a purge and a rebuild that
// complete during the final revalidation itself: after the page was made
// and checked by the registry, the next reading of the clock is the
// credential's recheck in the final read transaction (whose snapshot is
// then fixed); a purge, or a real rebuild, completed there is seen, and the
// page is not served.
func TestPagesAreNotServedAfterAFinalInvalidation(t *testing.T) {
	ctx := context.Background()
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	spool := r.srv.cfg.Views
	defer spool.HookPagesForTesting(nil)
	for _, c := range []struct {
		name   string
		during func()
	}{
		{"a purge", func() { spool.Retire(r.account) }},
		{"a rebuild", func() {
			if _, err := r.srv.cfg.Protocol.Rebuild(ctx, r.account); err != nil {
				t.Error(err)
			}
		}},
	} {
		for _, first := range []bool{false, true} {
			tok := r.get(allStates + "&limit=3").must(http.StatusOK).str("next_page_token")
			done := false
			spool.HookPagesForTesting(func() {
				spool.HookPagesForTesting(nil)
				// The page is about to be made: act at the clock's next
				// reading, inside the final recheck.
				r.clock.next.set(func() { c.during(); done = true })
			})
			before := r.currentIdentity()
			if first {
				res := r.get(allStates + "&limit=3").must(http.StatusOK)
				if !done {
					t.Fatalf("%s: the first request's final check did not run it", c.name)
				}
				ref := r.oracle()
				want, _, _ := ref.ListItems("/v1/read/episodes", "", url.Values{"filter": {"include_subscribed,include_archived,include_not_subscribed"}})
				sameRows(t, c.name+" during a first request's final check: the page made again", pageItems(t, res), want[:3])
				if h, _ := headJSON(ref.Head); fmt.Sprint(h["cursor"]) != fmt.Sprint(res.obj("head")["cursor"]) {
					t.Fatalf("%s during a first request's final check: the page is not at the new head", c.name)
				}
				continue
			}
			res := r.get(allStates + "&limit=3&page_token=" + tok)
			if !done {
				t.Fatalf("%s: the token's final check did not run it", c.name)
			}
			if c.name == "a rebuild" && r.currentIdentity().Generation <= before.Generation {
				t.Fatal("the rebuild did not raise the generation")
			}
			res.expect(http.StatusGone, "view_expired")
		}
	}
}

// Queue anchors (PROTOCOL §12): a queued episode's place; an episode that
// just left the queue (a real queue.remove), one never queued and one not
// stored, at their index; an empty queue: no rows, no page before, a start
// token; every window the oracle's, with the page before and the start.
func TestQueueAnchors(t *testing.T) {
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	s := &commitRun{readEnv: r, t: t, p: r.readPhone(), bins: map[string]int{}, outcomes: map[string]int{}}
	queue := func() []json.RawMessage {
		want, _, _ := r.oracle().ListItems("/v1/read/queue", "", nil)
		return want
	}
	window := func(what, q string, want []json.RawMessage, offset, limit int) *response {
		t.Helper()
		res := r.get("/v1/read/queue?" + q).must(http.StatusOK)
		off, _ := res.body["offset"].(float64)
		end := min(len(want), offset+limit)
		if int(off) != offset {
			t.Fatalf("%s: offset %v, want %d", what, off, offset)
		}
		sameRows(t, what, pageItems(t, res), want[offset:end])
		prev, _ := res.body["prev_page_token"].(string)
		if (offset == 0) != (prev == "") {
			t.Fatalf("%s: prev %q at %d", what, prev, offset)
		}
		if prev != "" {
			from := max(0, offset-limit)
			sameRows(t, what+": the page before", pageItems(t, r.get(fmt.Sprintf("/v1/read/queue?limit=%d&page_token=%s", limit, prev)).must(http.StatusOK)),
				want[from:min(len(want), from+limit)])
		}
		start := r.get(fmt.Sprintf("/v1/read/queue?limit=%d&page_token=%s", limit, res.str("start_page_token"))).must(http.StatusOK)
		sameRows(t, what+": the start", pageItems(t, start), want[:min(limit, len(want))])
		return res
	}
	want := queue()
	if len(want) < 5 {
		t.Fatalf("a queue of %d", len(want))
	}
	for i := range want {
		window(fmt.Sprintf("anchored at %d", i), fmt.Sprintf("anchor=%s&before=1&limit=3", rowID(want[i])), want, max(0, i-1), 3)
	}
	// An episode that just left the queue.
	gone := rowID(want[2])
	var row map[string]any
	for _, e := range s.rows() {
		if e["episode_id"] == gone {
			row = e
		}
	}
	re := ref(row)
	if out := s.push(s.fact("queue.remove", target(re), map[string]any{"episodes": []any{re}, "reason": "user"})); out != "applied" {
		t.Fatalf("queue.remove: %s", out)
	}
	want = queue()
	for _, w := range want {
		if rowID(w) == gone {
			t.Fatal("the removed episode is still queued")
		}
	}
	window("an anchor that left the queue", "anchor="+gone+"&anchor_index=2&before=1&limit=2", want, 1, 2)
	// One never queued, one not stored, an index past the end.
	queued := map[string]bool{}
	for _, w := range want {
		queued[rowID(w)] = true
	}
	var never string
	for _, e := range r.oracle().Episodes {
		if !queued[e.ID] && e.ID != gone {
			never = e.ID
			break
		}
	}
	window("an anchor never queued", "anchor="+never+"&anchor_index=3&before=0&limit=2", want, 3, 2)
	window("an anchor not stored", "anchor=e1:"+strings.Repeat("0", 64)+"&anchor_index=1&before=1&limit=2", want, 0, 2)
	window("an index past the end", "anchor_index=500&before=2&limit=2", want, len(want)-2, 2)
	// The empty queue.
	var refs []any
	for _, e := range s.rows() {
		if queued[e["episode_id"].(string)] {
			refs = append(refs, ref(e))
		}
	}
	for _, x := range refs {
		re := x.(map[string]any)
		if out := s.push(s.fact("queue.remove", target(re), map[string]any{"episodes": []any{re}, "reason": "user"})); out != "applied" {
			t.Fatalf("queue.remove: %s", out)
		}
	}
	if want = queue(); len(want) != 0 {
		t.Fatalf("%d left in the queue", len(want))
	}
	res := window("an empty queue", "anchor="+gone+"&anchor_index=4&before=3&limit=2", want, 0, 2)
	if res.body["total"] != float64(0) || res.body["next_page_token"] != nil {
		t.Fatalf("an empty queue: %s", res.raw)
	}
}

// A request's whole read-model work, at the request's boundary (the cache
// load included): after a commit, a screen applies that commit's changes to
// the cached version (no full build, the changed entity's records only); a
// page token reads no version at all; a first list request after another
// commit applies again.
func TestRequestWorkAfterACommit(t *testing.T) {
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	s := &commitRun{readEnv: r, t: t, p: r.readPhone(), bins: map[string]int{}, outcomes: map[string]int{}}
	r.get("/v1/read/summary").must(http.StatusOK) // the cache holds the account's version
	commit := func() {
		t.Helper()
		ref := r.oracle()
		e := ref.Episodes[len(ref.Episodes)/2]
		re := s.epRefFrom(ref, e)
		if out := s.push(s.fact("favorite.set", target(re), map[string]any{"episode": re, "on": true})); out != "applied" {
			t.Fatalf("favorite.set: %s", out)
		}
	}
	measure := func(f func()) views.WorkCounts {
		before := views.Work()
		f()
		return views.Work().Minus(before)
	}
	// The startup self-check: the account's first apply is also built in
	// full and compared, once per account and server run.
	commit()
	r.get("/v1/read/summary").must(http.StatusOK)
	tok := r.get(allStates + "&limit=5").must(http.StatusOK).str("next_page_token")
	commit()
	summary := measure(func() { r.get("/v1/read/summary").must(http.StatusOK) })
	page := measure(func() { r.get(allStates + "&limit=5&page_token=" + tok).must(http.StatusOK) })
	commit()
	list := measure(func() { r.get(allStates + "&limit=5").must(http.StatusOK) })
	t.Logf("summary %+v; token page %+v; first page %+v", summary, page, list)
	for name, w := range map[string]views.WorkCounts{"summary": summary, "first page": list} {
		if w.FullBuilds != 0 || w.Applies != 1 || w.EntitiesBuilt > 2 || w.RecordsDecoded > 20 {
			t.Errorf("%s after a commit: %+v", name, w)
		}
	}
	if page.FullBuilds != 0 || page.Applies != 0 || page.RecordsDecoded != 0 || page.RowsEncoded != 5 {
		t.Errorf("a token's page: %+v", page)
	}
}
