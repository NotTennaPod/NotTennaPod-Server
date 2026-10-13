package api

// PROTOCOL §6.4 acceptance tests over HTTP (push admission and bounds,
// PROTOCOL §6.4, §6.13, §10). The phone is played through /v1/push, /v1/pull and
// /v1/snapshot exactly as an app would call them; stored state is checked
// through a separate database connection.

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

func init() {
	// The codes the sync endpoints add to the PROTOCOL §5 table.
	for code, status := range map[string]int{"lineage_unknown": 409, "lineage_mismatch": 409, "view_expired": 410,
		"version_unsupported": 426, "dependency_missing": 409} {
		if _, ok := errorStatus[code]; !ok {
			errorStatus[code] = []int{status}
		}
	}
}

// syncNote is the test operation type test.note: payload {"text": string}.
type syncNote struct{}

func (syncNote) Capability() string { return "" }
func (syncNote) Validate(f *protocol.Fact) error {
	if _, ok := f.Payload["text"].(string); !ok {
		return errors.New("text required")
	}
	return nil
}
func (syncNote) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	ctx.Record(protocol.Record{Entity: "note", ID: f.OpID, FieldGroup: "text", Value: f.Payload["text"]})
	return protocol.Outcome{Result: "applied"}, nil
}

func withSync(t *testing.T) func(*Config) {
	return func(c *Config) {
		reg := protocol.NewRegistry()
		reg.Register("test.note", 1, syncNote{})
		views, err := protocol.NewViewRegistry(filepath.Join(c.Store.Dir(), "views"), c.Now)
		if err != nil {
			t.Fatal(err)
		}
		c.Protocol = &protocol.Service{Store: c.Store, Registry: reg, Now: c.Now, Views: views}
	}
}

// ---- a phone

type phone struct {
	e       *env
	cred    opt
	token   string
	install string
	nonce   string
	cursor  string
	acked   int64
	author  string
	seq     int64
}

func (e *env) phoneOf(username, password string) *phone {
	e.t.Helper()
	id, nonce := newID(), newID()
	tok := e.token(username, password, id, nonce)
	p := &phone{e: e, cred: bearer(tok), token: tok, install: id, nonce: nonce, author: newID()}
	p.cursor = p.attach(0).head
	return p
}

func (p *phone) noteFact(text string, deps ...string) map[string]any {
	p.seq++
	list := []any{}
	for _, d := range deps {
		list = append(list, d)
	}
	return map[string]any{"op_id": newID(), "author_id": p.author, "author_seq": p.seq, "dependencies": list, "type": "test.note",
		"type_version": 1, "client_time": time.Now().UnixMilli(), "targets": []any{}, "payload": map[string]any{"text": text}}
}

func contentHash(t testing.TB, fact map[string]any) string {
	raw, _ := json.Marshal(fact)
	v, err := contract.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	h, err := contract.ContentHash(v.(map[string]any))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

type op struct {
	seq  int64
	fact map[string]any
	part map[string]any
}

func (p *phone) pushBody(ops ...op) map[string]any {
	list := []any{}
	for _, o := range ops {
		m := map[string]any{"install_seq": o.seq}
		if o.part != nil {
			m["part"] = o.part
		} else {
			m["fact"] = o.fact
		}
		list = append(list, m)
	}
	return map[string]any{"cursor": p.cursor, "install_id": p.install, "acked_install_seq": p.acked, "operations": list}
}

func (p *phone) push(ops ...op) *response {
	p.e.t.Helper()
	return p.e.do(http.MethodPost, "/v1/push", p.pushBody(ops...), p.cred)
}

type pushResult struct {
	InstallSeq   int64           `json:"install_seq"`
	Status       string          `json:"status"`
	Receipt      json.RawMessage `json:"receipt"`
	Missing      []string        `json:"missing"`
	PartsPresent []int           `json:"parts_present"`
}

type pushReply struct {
	Head             string       `json:"head"`
	InstallWatermark int64        `json:"install_watermark"`
	Results          []pushResult `json:"results"`
}

func pushed(t testing.TB, r *response) pushReply {
	t.Helper()
	var out pushReply
	if r.status != http.StatusOK {
		t.Fatalf("push: %d %s", r.status, r.raw)
	}
	if err := json.Unmarshal(r.raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func (p *phone) statuses(r *response) []string {
	p.e.t.Helper()
	var out []string
	reply := pushed(p.e.t, r)
	for _, res := range reply.Results {
		out = append(out, res.Status)
	}
	p.acked = max(p.acked, reply.InstallWatermark)
	return out
}

type page struct {
	Head             string            `json:"head"`
	From             string            `json:"from"`
	HasMore          bool              `json:"has_more"`
	Next             string            `json:"next"`
	InstallWatermark int64             `json:"install_watermark"`
	Items            []json.RawMessage `json:"items"`
	Manifest         *struct {
		ItemCount      int64  `json:"item_count"`
		SnapshotDigest string `json:"snapshot_digest"`
	} `json:"manifest"`
}

func decodePage(t testing.TB, r *response) page {
	t.Helper()
	var p page
	if r.status != http.StatusOK {
		t.Fatalf("%s: %d %s", r.what, r.status, r.raw)
	}
	if err := json.Unmarshal(r.raw, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

type snapshotResult struct {
	head  string
	items [][]byte
}

// attach runs a whole first-attachment snapshot (limit items per page) and
// verifies its manifest over the items exactly as received.
func (p *phone) attach(limit int) snapshotResult {
	p.e.t.Helper()
	body := map[string]any{"mode": "attach", "install_id": p.install, "acked_install_seq": 0}
	if limit > 0 {
		body["limit"] = limit
	}
	var out snapshotResult
	digest := contract.GenesisDigest
	for {
		pg := decodePage(p.e.t, p.e.do(http.MethodPost, "/v1/snapshot", body, p.cred))
		out.head = pg.Head
		for _, it := range pg.Items {
			if bytes.HasPrefix(it, []byte(`{"item_part"`)) {
				p.e.t.Fatal("unexpected framed item in a small snapshot")
			}
			out.items = append(out.items, it)
			digest, _ = contract.ChainDigest(digest, it)
		}
		if !pg.HasMore {
			if pg.Manifest == nil || pg.Manifest.ItemCount != int64(len(out.items)) || pg.Manifest.SnapshotDigest != hex.EncodeToString(digest) {
				p.e.t.Fatalf("snapshot manifest does not verify over the received items")
			}
			return out
		}
		body["page"] = pg.Next
	}
}

func genesisOf(t testing.TB, cursor string) string {
	c, err := contract.DecodeCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	g, _ := contract.Cursor{LogID: c.LogID, Epoch: c.Epoch, Seq: 0, Digest: contract.GenesisDigest}.Encode()
	return g
}

// pullAll pulls every page of one view and verifies the commits, as received,
// against the digest chain from `from` to the head.
func (p *phone) pullAll(from string, limit int) (commits [][]byte, pages [][]byte) {
	p.e.t.Helper()
	body := map[string]any{"cursor": from, "install_id": p.install, "acked_install_seq": p.acked, "limit": limit}
	var head string
	for {
		r := p.e.do(http.MethodPost, "/v1/pull", body, p.cred)
		pg := decodePage(p.e.t, r)
		pages = append(pages, r.raw)
		head = pg.Head
		for _, it := range pg.Items {
			var m map[string]json.RawMessage
			json.Unmarshal(it, &m)
			c, ok := m["commit"]
			if !ok {
				p.e.t.Fatalf("unexpected pull item %s", it)
			}
			commits = append(commits, c)
		}
		if !pg.HasMore {
			break
		}
		body["page"] = pg.Next
	}
	fc, _ := contract.DecodeCursor(from)
	hc, err := contract.DecodeCursor(head)
	if err != nil {
		p.e.t.Fatal(err)
	}
	d := fc.Digest
	for _, c := range commits {
		d, _ = contract.ChainDigest(d, c)
	}
	if !bytes.Equal(d, hc.Digest) || int64(len(commits)) != hc.Seq-fc.Seq {
		p.e.t.Fatalf("pulled commits, as received over HTTP, do not verify against the chain")
	}
	return commits, pages
}

// ---- stored state

func (e *env) stored(accountID string) string {
	e.t.Helper()
	return fmt.Sprintf("commits %d facts %d slots %d parts %d consumed %d",
		e.count(`SELECT COUNT(*) FROM log_commits WHERE account_id = ?`, accountID),
		e.count(`SELECT COUNT(*) FROM facts WHERE account_id = ?`, accountID),
		e.count(`SELECT COUNT(*) FROM transport_slots WHERE account_id = ?`, accountID),
		e.count(`SELECT COUNT(*) FROM push_parts WHERE account_id = ?`, accountID),
		e.count(`SELECT COALESCE(SUM(consumed_seq), 0) FROM installations WHERE account_id = ?`, accountID))
}

// ---- tests

func TestSyncPushOverHTTP(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withSync(t))
	_, rootID := e.setupAdmin("root", "root-password-1")
	p := e.phoneOf("root", "root-password-1")
	f1, f2 := p.noteFact("one"), p.noteFact("two")
	f2["dependencies"] = []any{f1["op_id"]}
	body := p.pushBody(op{1, f1, nil}, op{2, f2, nil})
	first := pushed(t, e.do(http.MethodPost, "/v1/push", body, p.cred))
	if len(first.Results) != 2 || first.Results[0].Status != "accepted" || first.Results[1].Status != "accepted" || first.InstallWatermark != 2 {
		t.Fatalf("push: %+v", first)
	}
	// The response was lost: the same request replays the receipts byte for byte.
	again := pushed(t, e.do(http.MethodPost, "/v1/push", body, p.cred))
	for i := range again.Results {
		if again.Results[i].Status != "duplicate" || !bytes.Equal(again.Results[i].Receipt, first.Results[i].Receipt) {
			t.Errorf("retry result %d: %s %s, want duplicate with %s", i, again.Results[i].Status, again.Results[i].Receipt, first.Results[i].Receipt)
		}
	}
	p.acked = 2
	// Another fact under a known op_id: consumed, no receipt.
	conflict := p.noteFact("other content")
	conflict["op_id"] = f1["op_id"]
	r := pushed(t, p.push(op{3, conflict, nil}))
	if len(r.Results) != 1 || r.Results[0].Status != "op_id_conflict" || r.Results[0].Receipt != nil || r.InstallWatermark != 3 {
		t.Errorf("op_id conflict: %+v", r)
	}
	p.acked = 3
	// A missing dependency names it and consumes nothing.
	missing := newID()
	dep := p.noteFact("waits", missing)
	r = pushed(t, p.push(op{4, dep, nil}))
	if len(r.Results) != 1 || r.Results[0].Status != "dependency_missing" || len(r.Results[0].Missing) != 1 || r.Results[0].Missing[0] != missing {
		t.Errorf("dependency_missing: %+v", r)
	}
	before := e.stored(rootID)
	// An unarchivable fact fails the whole request; nothing is consumed.
	bad := p.noteFact("no id")
	delete(bad, "op_id")
	p.push(op{4, p.noteFact("valid"), nil}, op{5, bad, nil}).expect(400, "invalid_request")
	// Malformed sequencing is refused.
	p.push(op{4, p.noteFact("a"), nil}, op{6, p.noteFact("b"), nil}).expect(400, "invalid_request")
	p.push(op{0, p.noteFact("zero"), nil}).expect(400, "invalid_request")
	// Another installation's ID with this credential.
	other := e.phoneOf("root", "root-password-1")
	body = p.pushBody(op{4, p.noteFact("x"), nil})
	body["install_id"] = other.install
	e.do(http.MethodPost, "/v1/push", body, p.cred).expect(403, "forbidden")
	if after := e.stored(rootID); after != before {
		t.Errorf("refused requests changed the store:\n before %s\n after  %s", before, after)
	}
	// The pulled log carries the commits exactly as stored.
	commits, _ := p.pullAll(genesisOf(t, p.cursor), 1)
	if len(commits) != 2 {
		t.Errorf("%d commits", len(commits))
	}
	var slots []any
	for _, c := range commits {
		v, _ := contract.DecodeJSON(c)
		slots = append(slots, v.(map[string]any)["slots"].([]any)...)
	}
	want := []map[string]any{
		{"install_id": p.install, "install_seq": 1, "op_id": f1["op_id"], "content_hash": contentHash(t, f1), "result": "accepted"},
		{"install_id": p.install, "install_seq": 2, "op_id": f2["op_id"], "content_hash": contentHash(t, f2), "result": "accepted"},
		{"install_id": p.install, "install_seq": 3, "op_id": f1["op_id"], "content_hash": contentHash(t, conflict), "result": "op_id_conflict"},
	}
	if len(slots) != len(want) {
		t.Fatalf("slot entries %v", slots)
	}
	for i := range want {
		got, _ := contract.Canonical(slots[i])
		w, _ := json.Marshal(want[i])
		wv, _ := contract.DecodeJSON(w)
		wc, _ := contract.Canonical(wv)
		if !bytes.Equal(got, wc) {
			t.Errorf("slot entry %d: %s, want %s", i, got, wc)
		}
	}
}

// A push authenticated before its credential is revoked, its
// installation superseded or its account disabled consumes nothing.
func TestSyncAuthorizationRaceConsumesNothing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		status int
		code   string
		race   func(e *env, root, aliceID string, p, other *phone)
	}{
		{"control", 200, "", func(*env, string, string, *phone, *phone) {}},
		{"revoked", 401, "unauthorized", func(e *env, _, _ string, p, other *phone) {
			e.revoke(other.cred, p.install).must(http.StatusOK)
		}},
		{"superseded", 409, "installation_superseded", func(e *env, _, _ string, p, _ *phone) {
			e.successor(p.cred, newID(), p.nonce).must(http.StatusOK)
		}},
		{"disabled", 403, "forbidden", func(e *env, root, aliceID string, _, _ *phone) {
			e.setState(root, aliceID, "disabled").must(http.StatusOK)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, withSync(t))
			root, _ := e.setupAdmin("root", "root-password-1")
			aliceID := e.createAccount(root, "alice", "alice-password-1", false)
			p := e.phoneOf("alice", "alice-password-1")
			other := e.phoneOf("alice", "alice-password-1")
			body, _ := json.Marshal(p.pushBody(op{1, p.noteFact("delayed"), nil}))
			r := delayed(e, http.MethodPost, "/v1/push", string(body), p.cred, func() { tc.race(e, root, aliceID, p, other) })
			if tc.status == 200 {
				if got := p.statuses(r.must(200)); len(got) != 1 || got[0] != "accepted" {
					t.Fatalf("undisturbed delayed push: %v", got)
				}
				return
			}
			r.expect(tc.status, tc.code)
			if got := e.stored(aliceID); got != "commits 0 facts 0 slots 0 parts 0 consumed 0" {
				t.Errorf("the raced push consumed something: %s", got)
			}
		})
	}
	// Pull and snapshot apply the same credential rule.
	e := newEnv(t, withSync(t))
	e.setupAdmin("root", "root-password-1")
	p := e.phoneOf("root", "root-password-1")
	e.successor(p.cred, newID(), p.nonce).must(http.StatusOK)
	e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": p.cursor, "install_id": p.install, "acked_install_seq": 0}, p.cred).
		expect(409, "installation_superseded")
	e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": p.install, "acked_install_seq": 0}, p.cred).
		expect(409, "installation_superseded")
}

// The same race for a framed part: a part authenticated before the account was
// disabled is not stored.
func TestSyncAuthorizationRaceStoresNoPart(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withSync(t))
	root, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(root, "alice", "alice-password-1", false)
	p := e.phoneOf("alice", "alice-password-1")
	f := p.noteFact(strings.Repeat("p", 2000))
	parts := frame(t, f, 1000)
	body, _ := json.Marshal(p.pushBody(op{1, nil, parts[0]}))
	delayed(e, http.MethodPost, "/v1/push", string(body), p.cred, func() {
		e.setState(root, aliceID, "disabled").must(http.StatusOK)
	}).expect(403, "forbidden")
	if got := e.stored(aliceID); got != "commits 0 facts 0 slots 0 parts 0 consumed 0" {
		t.Errorf("the raced part was stored: %s", got)
	}
}

func frame(t testing.TB, fact map[string]any, size int) []map[string]any {
	raw, _ := json.Marshal(fact)
	v, err := contract.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	slices, err := contract.SplitCanonical(v, size)
	if err != nil {
		t.Fatal(err)
	}
	tx, hash := newID(), contentHash(t, fact)
	var out []map[string]any
	for i, s := range slices {
		out = append(out, map[string]any{"transaction_id": tx, "part_index": i, "part_count": len(slices), "content_hash": hash, "data": s})
	}
	return out
}

// Tokens and installation IDs of another account cannot push
// into, pull from or snapshot an account; page tokens are bound to their
// account and installation.
func TestSyncIsolationBetweenAccounts(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withSync(t))
	root, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(root, "alice", "alice-password-1", false)
	bobID := e.createAccount(root, "bob", "bob-password-1", false)
	bob := e.phoneOf("bob", "bob-password-1")
	var bobOps []string
	for i := 1; i <= 2; i++ {
		f := bob.noteFact(fmt.Sprintf("bob %d", i))
		bobOps = append(bobOps, f["op_id"].(string))
		if got := bob.statuses(bob.push(op{int64(i), f, nil})); got[0] != "accepted" {
			t.Fatalf("bob's push: %v", got)
		}
	}
	bobState := e.stored(bobID)
	alice := e.phoneOf("alice", "alice-password-1")
	reveals := func(r *response) {
		t.Helper()
		for _, op := range bobOps {
			if bytes.Contains(r.raw, []byte(op)) {
				t.Errorf("%s revealed bob's fact %s", r.what, op)
			}
		}
	}
	bobGenesis := genesisOf(t, bob.cursor)
	// Alice's credential with bob's installation ID.
	body := alice.pushBody(op{1, alice.noteFact("into bob"), nil})
	body["install_id"] = bob.install
	e.do(http.MethodPost, "/v1/push", body, alice.cred).expect(403, "forbidden")
	reveals(e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": bobGenesis, "install_id": bob.install, "acked_install_seq": 0},
		alice.cred).expect(403, "forbidden"))
	reveals(e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": bob.install, "acked_install_seq": 0},
		alice.cred).expect(403, "forbidden"))
	// Alice's own installation under bob's installation ID (IDs are per account).
	twin := &phone{e: e, install: bob.install, author: newID()}
	twin.token = e.token("alice", "alice-password-1", bob.install, newID())
	twin.cred = bearer(twin.token)
	twin.cursor = twin.attach(0).head
	// Bob's lineage is unknown in alice's account.
	body = twin.pushBody(op{1, twin.noteFact("with bob's cursor"), nil})
	body["cursor"] = bob.cursor
	e.do(http.MethodPost, "/v1/push", body, twin.cred).expect(409, "lineage_unknown")
	reveals(e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": bobGenesis, "install_id": twin.install, "acked_install_seq": 0},
		twin.cred).expect(409, "lineage_unknown"))
	if got := twin.statuses(twin.push(op{1, twin.noteFact("alice's own"), nil})); got[0] != "accepted" {
		t.Fatalf("alice's push under the shared installation ID: %v", got)
	}
	if e.stored(bobID) != bobState {
		t.Errorf("alice's pushes changed bob's account: %s, was %s", e.stored(bobID), bobState)
	}
	commits, pages := twin.pullAll(genesisOf(t, twin.cursor), 0)
	if len(commits) != 1 {
		t.Errorf("alice's log has %d commits", len(commits))
	}
	for _, pg := range pages {
		reveals(&response{what: "alice's pull", raw: pg})
	}
	for _, it := range alice.attach(0).items {
		reveals(&response{what: "alice's snapshot", raw: it})
	}
	// Bob's page tokens are refused in alice's account and in bob's other
	// installation.
	pp := decodePage(t, e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": bobGenesis, "install_id": bob.install,
		"acked_install_seq": 2, "limit": 1}, bob.cred))
	sp := decodePage(t, e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": bob.install,
		"acked_install_seq": 0, "limit": 1}, bob.cred))
	if pp.Next == "" || sp.Next == "" {
		t.Fatal("expected further pages")
	}
	bob2 := e.phoneOf("bob", "bob-password-1")
	aliceGenesis := genesisOf(t, alice.cursor)
	for _, attempt := range []*response{
		e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": aliceGenesis, "install_id": alice.install, "acked_install_seq": 0, "page": pp.Next}, alice.cred),
		e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": bobGenesis, "install_id": bob2.install, "acked_install_seq": 0, "page": pp.Next}, bob2.cred),
		e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": alice.install, "acked_install_seq": 0, "page": sp.Next}, alice.cred),
		e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": bob2.install, "acked_install_seq": 0, "page": sp.Next}, bob2.cred),
	} {
		attempt.refused()
		if attempt.status >= 500 {
			t.Errorf("%s: %d", attempt.what, attempt.status)
		}
		if attempt.str("error") != "" {
			reveals(attempt)
		}
	}
	if e.stored(aliceID) != "commits 1 facts 1 slots 1 parts 0 consumed 1" {
		t.Errorf("alice's account: %s", e.stored(aliceID))
	}
}

// Oversized requests are refused before consuming anything; a
// fact over the unframed bound must be framed, and framing works.
func TestSyncBounds(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withSync(t))
	_, rootID := e.setupAdmin("root", "root-password-1")
	info := e.do(http.MethodGet, "/v1/info", nil).must(200)
	var advertised struct {
		Bounds map[string]json.Number `json:"bounds"`
	}
	dec := json.NewDecoder(bytes.NewReader(info.raw))
	dec.UseNumber()
	if err := dec.Decode(&advertised); err != nil {
		t.Fatal(err)
	}
	values := fmt.Sprint(advertised.Bounds)
	for _, v := range []string{"4194304", "500", "262144", "1048576"} {
		if !strings.Contains(values, ":"+v) {
			t.Errorf("GET /v1/info bounds %s do not advertise %s", values, v)
		}
	}
	p := e.phoneOf("root", "root-password-1")
	// A fact whose canonical bytes are exactly the unframed bound.
	sized := func(n int) map[string]any {
		f := p.noteFact("")
		raw, _ := json.Marshal(f)
		v, _ := contract.DecodeJSON(raw)
		c, _ := contract.Canonical(v)
		f["payload"] = map[string]any{"text": strings.Repeat("b", n-len(c))}
		return f
	}
	small := p.noteFact("small")
	over := sized(protocol.MaxUnframedFact + 1)
	// The whole request is refused; the small fact before it is not consumed.
	p.push(op{1, small, nil}, op{2, over, nil}).expect(413, "payload_too_large")
	// More than 4 MiB of request body.
	var many []op
	for i := 1; i <= 17; i++ {
		many = append(many, op{int64(i), sized(250 << 10), nil})
	}
	p.push(many...).expect(413, "payload_too_large")
	// More than 500 operations.
	many = nil
	for i := 1; i <= protocol.MaxOperations+1; i++ {
		many = append(many, op{int64(i), p.noteFact("n"), nil})
	}
	if r := p.push(many...).refused(); r.status >= 500 {
		t.Errorf("501 operations: %d", r.status)
	}
	// A part above the part bound.
	parts := frame(t, sized(protocol.MaxPartBytes+2048), protocol.MaxPartBytes+1024)
	if r := p.push(op{1, nil, parts[0]}).refused(); r.status >= 500 {
		t.Errorf("oversized part: %d", r.status)
	}
	if got := e.stored(rootID); got != "commits 0 facts 0 slots 0 parts 0 consumed 0" {
		t.Fatalf("refused requests consumed something: %s", got)
	}
	// Exactly at the bound is accepted unframed; above it, framed.
	exact := sized(protocol.MaxUnframedFact)
	if got := p.statuses(p.push(op{1, small, nil}, op{2, exact, nil})); len(got) != 2 || got[1] != "accepted" {
		t.Errorf("a fact of exactly 256 KiB: %v", got)
	}
	parts = frame(t, over, protocol.MaxPartBytes)
	if got := p.statuses(p.push(op{3, nil, parts[0]})); got[0] != "part_stored" {
		t.Errorf("first part: %v", got)
	}
	if got := p.statuses(p.push(op{3, nil, parts[1]})); got[0] != "accepted" {
		t.Errorf("completing part: %v", got)
	}
	var found bool
	commits, _ := p.pullAll(genesisOf(t, p.cursor), 0)
	for _, c := range commits {
		found = found || bytes.Contains(c, []byte(over["op_id"].(string)))
	}
	if !found {
		t.Error("the framed fact is not in the pulled log")
	}
}

// Pull and snapshot over HTTP: repeated tokens, lineage checks, and an expired view.
func TestSyncPagesOverHTTP(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withSync(t))
	e.setupAdmin("root", "root-password-1")
	p := e.phoneOf("root", "root-password-1")
	for i := 1; i <= 4; i++ {
		p.statuses(p.push(op{int64(i), p.noteFact(fmt.Sprintf("<n%d & >", i)), nil}))
	}
	genesis := genesisOf(t, p.cursor)
	body := map[string]any{"cursor": genesis, "install_id": p.install, "acked_install_seq": 4, "limit": 1}
	first := decodePage(t, e.do(http.MethodPost, "/v1/pull", body, p.cred))
	body["page"] = first.Next
	a := e.do(http.MethodPost, "/v1/pull", body, p.cred).must(200)
	p.statuses(p.push(op{5, p.noteFact("later"), nil}))
	b := e.do(http.MethodPost, "/v1/pull", body, p.cred).must(200)
	if !bytes.Equal(a.raw, b.raw) {
		t.Errorf("the same page token returned a different page:\n%s\n%s", a.raw, b.raw)
	}
	p.acked = 5
	p.pullAll(genesis, 2) // commits as received verify (including <, > and & in payloads)
	// Lineage checks.
	body = map[string]any{"cursor": genesis, "install_id": p.install, "acked_install_seq": 6}
	e.do(http.MethodPost, "/v1/pull", body, p.cred).expect(409, "lineage_mismatch")
	c, _ := contract.DecodeCursor(p.cursor)
	c.Digest = bytes.Repeat([]byte{9}, 32)
	wrong, _ := c.Encode()
	body = map[string]any{"cursor": wrong, "install_id": p.install, "acked_install_seq": 0}
	e.do(http.MethodPost, "/v1/pull", body, p.cred).expect(409, "lineage_mismatch")
	e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "upgrade", "cursor": wrong, "install_id": p.install, "acked_install_seq": 0},
		p.cred).expect(409, "lineage_mismatch")
	// A snapshot view unused for more than a day answers 410 view_expired.
	sp := decodePage(t, e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": p.install,
		"acked_install_seq": 0, "limit": 2}, p.cred))
	e.clock.Advance(25 * time.Hour)
	e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": p.install, "acked_install_seq": 0,
		"page": sp.Next}, p.cred).expect(410, "view_expired")
}

// Design ("Pull"): "A commit is sent as its stored canonical bytes (embedded as
// raw JSON)"; PROTOCOL §6.4: the client verifies commits against the digest
// chain and the snapshot manifest over the items. Text such as podcast titles
// and show notes can contain U+2028/U+2029, which JCS does not escape.
func TestSyncServesCommitsAndItemsAsTheirCanonicalBytes(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withSync(t))
	_, rootID := e.setupAdmin("root", "root-password-1")
	p := e.phoneOf("root", "root-password-1")
	p.statuses(p.push(op{1, p.noteFact("Line\u2028separator and paragraph\u2029separator <&>"), nil}))
	stored := e.bytesOf(`SELECT commit_json FROM log_commits WHERE account_id = ? AND seq = 1`, rootID)
	r := e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": genesisOf(t, p.cursor), "install_id": p.install,
		"acked_install_seq": 1}, p.cred)
	pg := decodePage(t, r)
	var m map[string]json.RawMessage
	json.Unmarshal(pg.Items[0], &m)
	if !bytes.Equal(m["commit"], stored) {
		t.Errorf("the pulled commit is not its stored canonical bytes:\n got  %q\n want %q", m["commit"], stored)
	}
	sp := decodePage(t, e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": p.install,
		"acked_install_seq": 0}, p.cred))
	d := contract.GenesisDigest
	for _, it := range sp.Items {
		d, _ = contract.ChainDigest(d, it)
	}
	if sp.Manifest == nil || sp.Manifest.SnapshotDigest != hex.EncodeToString(d) {
		t.Errorf("the snapshot manifest does not verify over the items as received")
	}
}

// bytesOf runs a one-value query on the database file through a separate
// read-only connection.
func (e *env) bytesOf(query string, args ...any) []byte {
	e.t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(e.dir, "nottennapod.db")+"?_pragma=query_only(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		e.t.Fatal(err)
	}
	defer db.Close()
	var b []byte
	if err := db.QueryRow(query, args...).Scan(&b); err != nil {
		e.t.Fatalf("%s: %v", query, err)
	}
	return b
}
