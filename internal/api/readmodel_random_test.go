package api

// The random commits' helpers: a phone's facts and their receipts, and the comparisons of
// the served responses with the read API's composition before the read
// model (the frozen oracle, internal/views/reference). The sequences are in
// readmodel_commits_test.go.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	mrand "math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
)

// commitRun is a sequence of real commits and checks.
type commitRun struct {
	*readEnv
	t        *testing.T
	r        *mrand.Rand
	p        *phone
	last     string // the phone's last fact
	bins     map[string]int
	outcomes map[string]int

	applied  map[string]int     // each kind of step that changed the library, or passed its effect check
	ref      *reference.Library // the oracle at the last check
	ns       string             // the account's namespace
	held     []heldPage         // pages of earlier views, read again after each step
	recorded *[]any             // when set, the phone's pushed facts with their receipts (a recovery's entries)
	frontier []any              // when set, what the facts depend on besides the phone's own (the backup's frontier)
	secrets  []any              // the next push's secrets
	removals []map[string]any   // history removals (op ID and episode) a restore can name
	mirrors  int                // podcasts subscribed at mirror URLs
	scale    bool               // the power user's library: windowed lists
	ident    views.Identity     // the account's identity at the last check
	queries  func() []readQuery // the full matrix
}

// heldPage is a page of a view made at an earlier step: while the account's
// identity is unchanged and the read model retains its version (the
// published one, the one before it, or a leased one) it is the same rows;
// after a change it is 410, and once two newer versions were published it
// may be 410.
type heldPage struct {
	url   string
	ident views.Identity
	pub   int64 // the account's publications when its view was made
	want  []json.RawMessage
}

// readQuery is a request the check compares with the oracle.
type readQuery struct {
	path    string
	podcast string // a podcast's list or view
	q       url.Values
}

func (rq readQuery) url() string {
	if len(rq.q) == 0 {
		return rq.path
	}
	return rq.path + "?" + rq.q.Encode()
}

// rows are the library's rows as the read API lists them (every state).
func (s *commitRun) rows() []map[string]any {
	res := s.get("/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed&limit=500")
	var out []map[string]any
	for {
		res.must(http.StatusOK)
		// With their numbers as written (the facts' payloads carry them).
		var body struct {
			Items []map[string]any `json:"items"`
		}
		dec := json.NewDecoder(bytes.NewReader(res.raw))
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil {
			s.t.Fatal(err)
		}
		out = append(out, body.Items...)
		next, _ := res.body["next_page_token"].(string)
		if next == "" {
			return out
		}
		res = s.get("/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed&limit=500&page_token=" + next)
	}
}

// ref is an episode row's EpisodeRef, as the browser makes it.
func ref(row map[string]any) map[string]any {
	recs, _ := row["records"].(map[string]any)
	identity, _ := recs["identity"].(map[string]any)
	value, _ := identity["value"].(map[string]any)
	pod, _ := row["podcast"].(map[string]any)
	out := map[string]any{"episode_id": row["episode_id"], "podcast_id": row["podcast_id"], "generation_id": pod["generation_id"]}
	if occ, ok := value["occurrence"]; ok {
		out["occurrence"] = occ
	}
	return out
}

func target(refs ...map[string]any) []any {
	var ts []map[string]any
	for _, r := range refs {
		ts = append(ts, map[string]any{"entity": "episode", "id": r["episode_id"], "generation": r["generation_id"]})
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i]["id"].(string) < ts[j]["id"].(string) })
	out := []any{}
	for _, x := range ts {
		out = append(out, x)
	}
	return out
}

// fact is a fact of the phone's author on the current frontier (or, while
// a backup is held, on the backup's frontier: the phone's facts then depend
// only on what the backup holds and on each other, as a recovery needs).
func (s *commitRun) fact(typ string, targets []any, payload map[string]any) map[string]any {
	frontier := s.frontier
	if frontier == nil {
		head := s.get("/v1/read/summary").must(http.StatusOK)
		fr := s.get("/v1/read/frontier?cursor=" + url.QueryEscape(head.obj("head")["cursor"].(string))).must(http.StatusOK)
		frontier = fr.body["frontier"].([]any)
	}
	deps := []string{}
	for _, d := range frontier {
		deps = append(deps, d.(string))
	}
	// A power user's frontier after an import can be thousands of nodes: a
	// fact names at most 64 of them (it need not name all it follows), so
	// it stays within the advertised fact size.
	if len(deps) > 64 {
		slices.Sort(deps)
		deps = deps[len(deps)-64:]
	}
	if s.last != "" {
		deps = append(deps, s.last)
	}
	slices.Sort(deps)
	deps = slices.Compact(deps)
	list := []any{}
	for _, d := range deps {
		list = append(list, d)
	}
	b, _ := contract.Canonical(payload)
	v, _ := contract.DecodeJSON(b)
	s.p.seq++
	f := map[string]any{"op_id": newID(), "author_id": s.p.author, "author_seq": s.p.seq, "dependencies": list, "type": typ,
		"type_version": 1, "client_time": s.clock.Now().UnixMilli(), "targets": targets, "payload": v}
	if s.last != "" {
		f["previous_op_id"] = s.last
	}
	s.last = f["op_id"].(string)
	return f
}

func (s *commitRun) push(f map[string]any) string {
	s.t.Helper()
	seq := s.p.acked + 1
	body := s.p.pushBody(op{seq: seq, fact: f})
	if s.secrets != nil {
		body["secrets"], s.secrets = s.secrets, nil
	}
	res := s.do(http.MethodPost, "/v1/push", body, s.p.cred).must(http.StatusOK)
	s.p.acked = seq
	var reply struct {
		Results []pushResult `json:"results"`
	}
	json.Unmarshal(res.raw, &reply)
	results := reply.Results
	if len(results) != 1 {
		s.t.Fatalf("push: %s", res.raw)
	}
	var receipt struct {
		Outcome string `json:"outcome"`
		Reason  string `json:"reason"`
	}
	json.Unmarshal(results[0].Receipt, &receipt)
	s.outcomes[f["type"].(string)+" "+receipt.Outcome+map[bool]string{true: "/" + receipt.Reason, false: ""}[receipt.Reason != ""]]++
	if s.recorded != nil && results[0].Status == "accepted" {
		var rec map[string]any
		dec := json.NewDecoder(bytes.NewReader(results[0].Receipt))
		dec.UseNumber()
		dec.Decode(&rec)
		*s.recorded = append(*s.recorded, map[string]any{"fact": f, "receipt": rec})
	}
	if cur, ok := res.body["head"].(map[string]any); ok {
		if c, ok := cur["cursor"].(string); ok {
			s.p.cursor = c
		}
	}
	return receipt.Outcome
}

func newQueueKey(t *testing.T, low string) (string, string) {
	var u [16]byte
	rand.Read(u[:])
	var lo *contract.QueueKey
	if low != "" {
		k, _ := contract.ParseQueueKey(low)
		lo = &k
	}
	k, err := contract.QueueKeyBetween(lo, nil, u)
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
	return k.String(), id
}

// ---- comparisons with the oracle

// snapshotRef is the oracle's library at the store's current snapshot.
func (s *commitRun) snapshotRef() *reference.Library {
	var ref *reference.Library
	if err := s.store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		ref, err = reference.Load(r, s.account)
		return err
	}); err != nil {
		s.t.Fatal(err)
	}
	return ref
}

func asAPI(t *testing.T, v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}

func bodyValues(t *testing.T, res *response) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(res.raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// sameBytes compares a response's values with the oracle's.
func sameBytes(t *testing.T, what string, got map[string]json.RawMessage, want map[string]any) {
	t.Helper()
	for k, v := range want {
		if w := asAPI(t, v); !bytes.Equal(got[k], w) {
			t.Fatalf("%s: %q\n%s\nthe oracle\n%s", what, k, got[k], w)
		}
	}
}

// checkHead checks a response's head is the snapshot's.
func (s *commitRun) checkHead(what string, got json.RawMessage, ref *reference.Library) {
	h, err := headJSON(ref.Head)
	if err != nil {
		s.t.Fatal(err)
	}
	if w := asAPI(s.t, h); !bytes.Equal(got, w) {
		s.t.Fatalf("%s: head %s, the snapshot %s", what, got, w)
	}
}

// compareList walks a list in pages of random sizes and compares its rows,
// total, view and head with the oracle's.
func (s *commitRun) compareList(rq readQuery, ref *reference.Library) {
	path := rq.path
	if rq.podcast != "" {
		path = "/v1/read/podcasts/{id}/episodes"
	}
	want, counters, ok := ref.ListItems(path, rq.podcast, rq.q)
	q := url.Values{}
	for k, v := range rq.q {
		q[k] = v
	}
	q.Set("limit", strconv.Itoa(1+s.r.IntN(500)))
	res := s.get(rq.path + "?" + q.Encode())
	if !ok {
		if res.status == http.StatusOK {
			s.t.Fatalf("%s: answered where the old server refused", rq.url())
		}
		return
	}
	res.must(http.StatusOK)
	var got []json.RawMessage
	for {
		body := bodyValues(s.t, res)
		s.checkHead(rq.url(), body["head"], ref)
		if !bytes.Equal(body["view"], counters) {
			s.t.Fatalf("%s: view %s, the oracle %s", rq.url(), body["view"], counters)
		}
		if total := string(body["total"]); total != strconv.Itoa(len(want)) {
			s.t.Fatalf("%s: total %s, the oracle %d", rq.url(), total, len(want))
		}
		var page []json.RawMessage
		json.Unmarshal(body["items"], &page)
		got = append(got, page...)
		next, _ := res.body["next_page_token"].(string)
		if next == "" {
			break
		}
		q.Set("page_token", next)
		q.Set("limit", strconv.Itoa(1+s.r.IntN(500)))
		res = s.get(rq.path + "?" + q.Encode()).must(http.StatusOK)
	}
	if len(got) != len(want) {
		s.t.Fatalf("%s: %d rows, the oracle %d", rq.url(), len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			s.t.Fatalf("%s: row %d\n%s\nthe oracle\n%s", rq.url(), i, got[i], want[i])
		}
	}
}

// compareScreens compares the other endpoints' responses.
func (s *commitRun) compareScreens(ref *reference.Library, full bool) {
	now := s.clock.Now().UnixMilli()
	sum := s.get("/v1/read/summary").must(http.StatusOK)
	body := bodyValues(s.t, sum)
	s.checkHead("summary", body["head"], ref)
	sameBytes(s.t, "summary", body, ref.SummaryBody(projection.DefaultEnqueueLocation, projection.DefaultSortOrder))
	for _, q := range []string{"", "?include_marked=true", fmt.Sprintf("?from=%d&to=%d", now-100*24*3600*1000, now), "?from=5&to=5"} {
		res := s.get("/v1/read/statistics" + q).must(http.StatusOK)
		u, _ := url.Parse("/x" + q)
		from, to := int64(0), stats.Forever
		if v := u.Query().Get("from"); v != "" {
			from, _ = strconv.ParseInt(v, 10, 64)
			to, _ = strconv.ParseInt(u.Query().Get("to"), 10, 64)
		}
		b := bodyValues(s.t, res)
		s.checkHead("statistics", b["head"], ref)
		sameBytes(s.t, "statistics"+q, b, ref.StatisticsBody(from, to, u.Query().Get("include_marked") == "true", now))
	}
	seeds := []int64{s.r.Int64()}
	if full {
		seeds = append(seeds, 0, 1, 42)
	}
	for _, seed := range seeds {
		for _, marked := range []bool{false, true} {
			order := []string{"date_new_old", "date_old_new", "episode_title_a_z"}[s.r.IntN(3)]
			res := s.get(fmt.Sprintf("/v1/read/home?seed=%d&inbox_sort=%s&include_marked=%v", seed, order, marked)).must(http.StatusOK)
			b := bodyValues(s.t, res)
			s.checkHead("home", b["head"], ref)
			sameBytes(s.t, fmt.Sprintf("home seed %d", seed), b, ref.HomeBody(now, seed, order, marked))
		}
	}
	var pods []string
	for id := range ref.Podcasts {
		pods = append(pods, id)
	}
	slices.Sort(pods)
	for i, id := range pods {
		if !full && i%5 != s.r.IntN(5) {
			continue
		}
		res := s.get("/v1/read/podcasts/" + id).must(http.StatusOK)
		b := bodyValues(s.t, res)
		s.checkHead("podcast", b["head"], ref)
		var served struct {
			Credentials map[string]any `json:"credentials"`
		}
		json.Unmarshal(b["podcast"], &served)
		want, _ := ref.PodcastBody(id, served.Credentials, now)
		sameBytes(s.t, "podcast "+id, b, map[string]any{"podcast": want, "counters": ref.Counters()})
	}
	for i := 0; i < len(ref.Episodes); i += max(1, len(ref.Episodes)/map[bool]int{true: 60, false: 6}[full]) {
		e := ref.Episodes[i]
		res := s.get("/v1/read/episodes/" + e.ID).must(http.StatusOK)
		b := bodyValues(s.t, res)
		var meta json.RawMessage
		if err := s.store.Read(context.Background(), func(r *storage.ReadTx) error {
			rec, err := r.Account(s.account).Record("episode", e.ID, "metadata")
			if err == nil {
				meta = json.RawMessage(rec.Value)
			}
			return nil
		}); err != nil {
			s.t.Fatal(err)
		}
		ep, pod, _ := ref.EpisodeBody(e.ID, meta)
		s.checkHead("episode", b["head"], ref)
		sameBytes(s.t, "episode "+e.ID, b, map[string]any{"episode": ep, "podcast": pod, "counters": ref.Counters()})
	}
}

// An anchored first request (PROTOCOL §12) answers the window around its
// anchor with where it is and the tokens before it and to the start; its
// parameters are checked; a first request without them answers exactly as
// before.
func TestReadAnchoredWindows(t *testing.T) {
	r := newReadEnv(t)
	r.importInto(r.account, "emulator-parity-library.db")
	var ref *reference.Library
	if err := r.store.Read(context.Background(), func(x *storage.ReadTx) error {
		var err error
		ref, err = reference.Load(x, r.account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	all := url.Values{"filter": {"include_subscribed,include_archived,include_not_subscribed"}}
	want, _, _ := ref.ListItems("/v1/read/episodes", "", all)
	if len(want) < 20 {
		t.Fatalf("%d rows", len(want))
	}
	page := func(res *response) (int, []json.RawMessage) {
		var items []json.RawMessage
		json.Unmarshal(bodyValues(t, res)["items"], &items)
		off, _ := res.body["offset"].(float64)
		return int(off), items
	}
	same := func(what string, items []json.RawMessage, from int) {
		t.Helper()
		for i, it := range items {
			if !bytes.Equal(it, want[from+i]) {
				t.Fatalf("%s: row %d differs from the oracle's", what, from+i)
			}
		}
	}
	base := "/v1/read/episodes?filter=" + url.QueryEscape(all.Get("filter"))
	for _, at := range []int{0, 3, len(want) / 2, len(want) - 1} {
		res := r.get(fmt.Sprintf("%s&anchor=%s&before=3&limit=5", base, url.QueryEscape(rowID(want[at])))).must(http.StatusOK)
		start := max(0, at-3)
		off, items := page(res)
		if off != start || len(items) != min(5, len(want)-start) {
			t.Fatalf("anchored at %d: offset %d, %d rows", at, off, len(items))
		}
		same(fmt.Sprintf("the window at %d", at), items, start)
		prev, _ := res.body["prev_page_token"].(string)
		if (start == 0) != (prev == "") {
			t.Fatalf("anchored at %d: prev %q", at, prev)
		}
		if prev != "" {
			p := r.get(base + "&limit=5&page_token=" + prev).must(http.StatusOK)
			_, items := page(p)
			same("the page before", items, max(0, start-5))
		}
		first := r.get(base + "&limit=5&page_token=" + res.body["start_page_token"].(string)).must(http.StatusOK)
		_, items = page(first)
		same("the start", items, 0)
	}
	// An anchor that is gone: its index.
	res := r.get(base + "&anchor=e1:gone&anchor_index=7&limit=4").must(http.StatusOK)
	if off, items := page(res); off != 7 || len(items) != 4 {
		t.Fatalf("a gone anchor: offset %d, %d rows", off, len(items))
	}
	// The queue too.
	r.get("/v1/read/queue?anchor_index=0&limit=2").must(http.StatusOK)
	// Parameters.
	r.get(base+"&before=201&anchor=e1:x").expect(http.StatusBadRequest, "invalid_request")
	r.get(base+"&anchor_index=-1").expect(http.StatusBadRequest, "invalid_request")
	tok := r.get(base + "&limit=2").must(http.StatusOK).body["next_page_token"].(string)
	r.get(base+"&anchor=e1:x&page_token="+tok).expect(http.StatusBadRequest, "invalid_request")
	r.get("/v1/read/subscriptions?anchor=e1:x").expect(http.StatusBadRequest, "invalid_request")
	// Without a window: no window fields.
	plain := r.get(base + "&limit=5").must(http.StatusOK)
	for _, k := range []string{"offset", "prev_page_token", "start_page_token"} {
		if _, ok := plain.body[k]; ok {
			t.Fatalf("a first request without a window answered %q", k)
		}
	}
}
