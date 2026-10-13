package protocol

// Shared harness for PROTOCOL §6.4 acceptance tests (acceptance_test.go,
// causality_test.go, views_test.go, convergence_test.go, load_test.go,
// crash_test.go). Every expectation in those tests is derived from
// PROTOCOL §6 and contract-v1; the helpers only drive the Service the way the HTTP
// layer does and verify what a replica can verify from the wire data alone.

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// ---- clocks

// testClock is a settable clock. hold arms a one-shot pause inside the next
// Now call, which gives tests a deterministic interruption point.
type testClock struct {
	mu   sync.Mutex
	now  time.Time
	gate atomic.Pointer[clockGate]
}

type clockGate struct{ entered, release chan struct{} }

func newTestClock() *testClock { return &testClock{now: time.Now().Truncate(time.Millisecond)} }

func (c *testClock) Now() time.Time {
	if g := c.gate.Swap(nil); g != nil {
		close(g.entered)
		<-g.release
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *testClock) millis() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.UnixMilli()
}

func (c *testClock) hold() *clockGate {
	g := &clockGate{entered: make(chan struct{}), release: make(chan struct{})}
	c.gate.Store(g)
	return g
}

// ---- test operation types

// acNote is test.note and its capability variants: payload {"text": string}.
type acNote struct{ capability string }

func (h acNote) Capability() string { return h.capability }
func (acNote) Validate(f *Fact) error {
	if _, ok := f.Payload["text"].(string); !ok || len(f.Payload) != 1 {
		return errors.New("payload must be {text}")
	}
	return nil
}
func (acNote) Apply(ctx *ApplyContext, f *Fact) (Outcome, error) {
	ctx.Record(Record{Entity: "note", ID: f.OpID, FieldGroup: "text", Value: f.Payload["text"]})
	return Outcome{Result: "applied"}, nil
}

// acSet is test.set: a register write {"key": string, "text": string}.
type acSet struct{}

func (acSet) Capability() string { return "" }
func (acSet) Validate(f *Fact) error {
	_, k := f.Payload["key"].(string)
	_, v := f.Payload["text"].(string)
	if !k || !v {
		return errors.New("payload must be {key, text}")
	}
	return nil
}
func (acSet) Apply(ctx *ApplyContext, f *Fact) (Outcome, error) {
	ctx.Record(Record{Entity: "reg", ID: f.Payload["key"].(string), FieldGroup: "value", Value: f.Payload["text"]})
	return Outcome{Result: "applied"}, nil
}

// decisionsBySubject lets test.decide_after find a decision made earlier in the
// same push (subjects are unique per test).
var decisionsBySubject sync.Map

// acDecide is test.decide: {"text": subject}; it records and makes a server
// decision that depends on the fact (fact → decision).
type acDecide struct{}

func (acDecide) Capability() string { return "" }
func (acDecide) Validate(f *Fact) error {
	if _, ok := f.Payload["text"].(string); !ok {
		return errors.New("text required")
	}
	return nil
}
func (acDecide) Apply(ctx *ApplyContext, f *Fact) (Outcome, error) {
	subject := f.Payload["text"].(string)
	d, err := ctx.Decide(DecisionInput{Type: "test.decision", Version: 1, Subject: subject, Dependencies: []string{f.OpID},
		Inputs: map[string]any{"op": f.OpID}, Result: map[string]any{"ok": true}})
	if err != nil {
		return Outcome{}, err
	}
	decisionsBySubject.Store(subject, d["decision_id"].(string))
	ctx.Record(Record{Entity: "note", ID: f.OpID, FieldGroup: "text", Value: subject})
	return Outcome{Result: "applied"}, nil
}

// acDecideAfter is test.decide_after: {"text": subject, "after": earlier subject};
// its decision depends on the earlier decision.
type acDecideAfter struct{}

func (acDecideAfter) Capability() string { return "" }
func (acDecideAfter) Validate(f *Fact) error {
	_, a := f.Payload["text"].(string)
	_, b := f.Payload["after"].(string)
	if !a || !b {
		return errors.New("text and after required")
	}
	return nil
}
func (acDecideAfter) Apply(ctx *ApplyContext, f *Fact) (Outcome, error) {
	earlier, ok := decisionsBySubject.Load(f.Payload["after"].(string))
	if !ok {
		return Outcome{}, errors.New("earlier decision unknown")
	}
	_, err := ctx.Decide(DecisionInput{Type: "test.decision", Version: 1, Subject: f.Payload["text"].(string),
		Dependencies: []string{f.OpID, earlier.(string)}})
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Result: "applied"}, nil
}

// acFail is test.fail: its Apply fails like a storage error would.
type acFail struct{}

func (acFail) Capability() string   { return "" }
func (acFail) Validate(*Fact) error { return nil }
func (acFail) Apply(*ApplyContext, *Fact) (Outcome, error) {
	return Outcome{}, errors.New("simulated handler failure")
}

func acceptanceRegistry() *Registry {
	reg := NewRegistry()
	reg.Register("test.note", 1, acNote{})
	reg.Register("test.queue_note", 1, acNote{capability: "queue"})
	reg.Register("test.inbox_note", 1, acNote{capability: "inbox"})
	reg.Register("test.set", 1, acSet{})
	reg.Register("test.decide", 1, acDecide{})
	reg.Register("test.decide_after", 1, acDecideAfter{})
	reg.Register("test.fail", 1, acFail{})
	return reg
}

// ---- IDs

// uid returns a deterministic UUID whose leading group controls bytewise order.
func uid(group, n int) string { return fmt.Sprintf("%08x-0000-4000-8000-%012x", group, n) }

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// ---- facts and parts

// fs describes a fact; object() builds its wire object.
type fs struct {
	op, author string
	seq        int64
	prev       string
	deps       []string
	typ        string
	ver        int64
	ct         int64
	targets    []any
	payload    map[string]any
}

func (s fs) object() map[string]any {
	deps := slices.Clone(s.deps)
	sort.Strings(deps)
	list := make([]any, len(deps))
	for i, d := range deps {
		list[i] = d
	}
	typ, ver := s.typ, s.ver
	if typ == "" {
		typ = "test.note"
	}
	if ver == 0 {
		ver = 1
	}
	targets := s.targets
	if targets == nil {
		targets = []any{}
	}
	payload := s.payload
	if payload == nil {
		payload = map[string]any{"text": "note " + s.op}
	}
	obj := map[string]any{"op_id": s.op, "author_id": s.author, "author_seq": s.seq, "dependencies": list,
		"type": typ, "type_version": ver, "client_time": s.ct, "targets": targets, "payload": payload}
	if s.prev != "" {
		obj["previous_op_id"] = s.prev
	}
	return obj
}

func text(t string) map[string]any { return map[string]any{"text": t} }

// canon returns the canonical bytes of a protocol value.
func canon(v any) []byte {
	b, err := contract.Canonical(v)
	if err != nil {
		panic(err)
	}
	return b
}

// decoded returns v as the strict decoder returns it from the wire.
func decoded(v any) any {
	d, err := contract.DecodeJSON(canon(v))
	if err != nil {
		panic(err)
	}
	return d
}

func hashOf(obj map[string]any) string {
	h, err := contract.ContentHash(decoded(obj).(map[string]any))
	if err != nil {
		panic(err)
	}
	return h
}

// wireFact parses a fact exactly as the HTTP layer does.
func wireFact(obj map[string]any) *Archived {
	a, err := ParseFact(decoded(obj))
	if err != nil {
		panic(err)
	}
	return a
}

func fop(seq int64, obj map[string]any) PushOp { return PushOp{InstallSeq: seq, Fact: wireFact(obj)} }

// partObjects frames a fact's canonical bytes into wire part objects of size bytes.
func partObjects(obj map[string]any, size int, tx string) []map[string]any {
	data := canon(obj)
	hash := hashOf(obj)
	var out []map[string]any
	count := (len(data) + size - 1) / size
	for i := 0; i < count; i++ {
		end := min((i+1)*size, len(data))
		out = append(out, map[string]any{"transaction_id": tx, "part_index": int64(i), "part_count": int64(count),
			"content_hash": hash, "data": base64.RawURLEncoding.EncodeToString(data[i*size : end])})
	}
	return out
}

// wirePart decodes a part object as the HTTP layer does.
func wirePart(seq int64, obj map[string]any) PushOp {
	p, err := DecodePart(decoded(obj).(map[string]any))
	if err != nil {
		panic(err)
	}
	return PushOp{InstallSeq: seq, Part: p}
}

func partOps(seq int64, obj map[string]any, size int, tx string) []PushOp {
	var ops []PushOp
	for _, p := range partObjects(obj, size, tx) {
		ops = append(ops, wirePart(seq, p))
	}
	return ops
}

// ---- harness

type harness struct {
	t       testing.TB
	ctx     context.Context
	dir     string
	store   *storage.Store
	svc     *Service
	reg     *Registry
	clock   *testClock // receive times (Service.Now)
	vclock  *testClock // view registry clock
	account string
	author  string // default author for facts
	dbOnce  sync.Once
	rawDB   *sql.DB

	accounts  []string // every account newAccount created (the rebuild invariant checks each)
	noRebuild string   // why the rebuild invariant does not apply ("" = it does)
}

func newHarness(t testing.TB) *harness {
	t.Helper()
	h := &harness{t: t, ctx: context.Background(), dir: filepath.Join(t.TempDir(), "data"), clock: newTestClock(),
		vclock: newTestClock(), reg: acceptanceRegistry(), author: randomID()}
	store, err := storage.Open(h.ctx, h.dir)
	if err != nil {
		t.Fatal(err)
	}
	h.store = store
	t.Cleanup(func() {
		if h.rawDB != nil {
			h.rawDB.Close()
		}
		h.store.Close()
	})
	h.account = h.newAccount()
	h.startService()
	// Every scenario ends with the rebuild invariant (PROTOCOL §6.11): rebuilding each
	// account from its archive reproduces it (before the store closes).
	t.Cleanup(func() {
		if h.noRebuild != "" {
			t.Logf("rebuild invariant: not applicable: %s", h.noRebuild)
			return
		}
		for _, account := range h.accounts {
			checkRebuild(t, h.svc, account)
		}
	})
	return h
}

func (h *harness) newAccount() string {
	var id string
	if err := h.store.Write(h.ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		id = a.ID
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
	h.accounts = append(h.accounts, id)
	return id
}

func (h *harness) startService() {
	views, err := NewViewRegistry(filepath.Join(h.store.Dir(), "views"), h.vclock.Now)
	if err != nil {
		h.t.Fatal(err)
	}
	h.svc = &Service{Store: h.store, Registry: h.reg, Now: h.clock.Now, Views: views}
}

// restart closes and reopens the data directory and starts a new Service with a
// new view registry, as a server restart does.
func (h *harness) restart() {
	h.t.Helper()
	if err := h.store.Close(); err != nil {
		h.t.Fatal(err)
	}
	store, err := storage.Open(h.ctx, h.dir)
	if err != nil {
		h.t.Fatal(err)
	}
	h.store = store
	h.startService()
}

func capsJSON(caps []string) string {
	list := make([]any, len(caps))
	for i, c := range caps {
		list[i] = c
	}
	return string(canon(list))
}

// addInstall registers an installation in account with the declared capabilities.
func (h *harness) addInstallIn(account string, caps ...string) string {
	h.t.Helper()
	id := randomID()
	if err := h.store.Write(h.ctx, func(tx *storage.WriteTx) error {
		return tx.Account(account).InsertInstallation(storage.Installation{InstallID: id, InstanceNonce: randomID(), Name: "test",
			Platform: "ios", AppVersion: "1", Capabilities: capsJSON(caps)})
	}); err != nil {
		h.t.Fatal(err)
	}
	return id
}

func (h *harness) addInstall(caps ...string) string { return h.addInstallIn(h.account, caps...) }

func (h *harness) setCapabilities(install string, caps ...string) {
	h.t.Helper()
	if err := h.store.Write(h.ctx, func(tx *storage.WriteTx) error {
		return tx.Account(h.account).UpdateInstallationDevice(install, "test", "1", capsJSON(caps))
	}); err != nil {
		h.t.Fatal(err)
	}
}

func authorizer(account, install string) Authorize {
	return func(tx *storage.WriteTx) (*storage.Installation, error) {
		i, err := tx.Account(account).Installation(install)
		if err != nil {
			return nil, err
		}
		return &i, nil
	}
}

func (h *harness) headOf(account string) contract.Cursor {
	h.t.Helper()
	var c contract.Cursor
	if err := h.store.Read(h.ctx, func(r *storage.ReadTx) error {
		hd, err := r.Account(account).Head()
		c = contract.Cursor{LogID: hd.LogID, Epoch: hd.Epoch, Seq: hd.Seq, Digest: hd.Digest}
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
	return c
}

func (h *harness) head() contract.Cursor { return h.headOf(h.account) }

func (h *harness) genesis() contract.Cursor {
	c := h.head()
	return contract.Cursor{LogID: c.LogID, Epoch: c.Epoch, Seq: 0, Digest: contract.GenesisDigest}
}

func (h *harness) tryPush(install string, acked int64, ops ...PushOp) (*PushResponse, error) {
	return h.svc.Push(h.ctx, PushRequest{AccountID: h.account, Cursor: h.head(), InstallID: install, AckedInstallSeq: acked,
		Operations: ops}, authorizer(h.account, install))
}

func (h *harness) push(install string, acked int64, ops ...PushOp) *PushResponse {
	h.t.Helper()
	r, err := h.tryPush(install, acked, ops...)
	if err != nil {
		h.t.Fatalf("push: %v", err)
	}
	return r
}

func (h *harness) note(op string, seq int64, prev string, deps ...string) map[string]any {
	return fs{op: op, author: h.author, seq: seq, prev: prev, deps: deps, ct: h.clock.millis() - 60_000}.object()
}

// ---- push result expectations

type want struct {
	seq     int64
	status  string
	outcome string // the receipt's outcome; "" = no receipt expected
	missing []string
	parts   []int
}

func (h *harness) keys() map[string][]byte {
	h.t.Helper()
	ks, err := h.store.ReceiptKeys(h.ctx)
	if err != nil || len(ks) == 0 {
		h.t.Fatalf("receipt keys: %v", err)
	}
	out := map[string][]byte{}
	for _, k := range ks {
		out[k.ID] = k.Secret
	}
	return out
}

var receiptFields = map[string]bool{"op_id": true, "content_hash": true, "outcome": true, "reason": true, "normalized_time": true,
	"epoch": true, "seq": true, "index": true, "key_id": true, "mac": true, "frontier": true}

// checkReceipt verifies a receipt's form (PROTOCOL §6.1 and its PROTOCOL §8.1) and MAC.
func checkReceipt(t testing.TB, keys map[string][]byte, account string, r map[string]any) {
	t.Helper()
	for k := range r {
		if !receiptFields[k] {
			t.Errorf("receipt has unexpected field %q: %v", k, r)
		}
	}
	outcome, _ := r["outcome"].(string)
	if !slices.Contains([]string{"applied", "superseded", "not_applied", "rejected"}, outcome) {
		t.Errorf("receipt outcome %q", outcome)
	}
	_, hasReason := r["reason"]
	if hasReason != (outcome == "not_applied" || outcome == "rejected") {
		t.Errorf("receipt reason presence wrong for %s: %v", outcome, r)
	}
	frontier, hasFrontier := r["frontier"].([]any)
	if hasFrontier != (outcome == "rejected") {
		t.Errorf("receipt frontier presence wrong for %s: %v", outcome, r)
	}
	for i := 1; i < len(frontier); i++ {
		if frontier[i-1].(string) >= frontier[i].(string) {
			t.Errorf("receipt frontier not sorted/unique: %v", frontier)
		}
	}
	keyID, _ := r["key_id"].(string)
	key, ok := keys[keyID]
	if !ok || !contract.VerifyMAC(key, account, decoded(r).(map[string]any)) {
		t.Errorf("receipt MAC does not verify: %v", r)
	}
}

func (h *harness) expect(resp *PushResponse, watermark int64, wants ...want) {
	h.t.Helper()
	keys := h.keys()
	if resp.InstallWatermark != watermark {
		h.t.Errorf("install_watermark %d, want %d", resp.InstallWatermark, watermark)
	}
	if len(resp.Results) != len(wants) {
		h.t.Fatalf("got %d results %s, want %d", len(resp.Results), wireJSON(h.t, resp.Results), len(wants))
	}
	for i, w := range wants {
		r := resp.Results[i]
		if r.InstallSeq != w.seq || r.Status != w.status {
			h.t.Errorf("result %d: seq %d status %q, want seq %d status %q (%s)", i, r.InstallSeq, r.Status, w.seq, w.status, wireJSON(h.t, r))
		}
		if (r.Receipt != nil) != (w.outcome != "") {
			h.t.Errorf("result %d (%s): receipt presence %v, want %v", i, w.status, r.Receipt != nil, w.outcome != "")
		}
		if r.Receipt != nil {
			checkReceipt(h.t, keys, h.account, r.Receipt)
			if got := r.Receipt["outcome"]; w.outcome != "" && got != w.outcome {
				h.t.Errorf("result %d: receipt outcome %v, want %s (%v)", i, got, w.outcome, r.Receipt)
			}
		}
		if !slices.Equal(r.Missing, w.missing) {
			h.t.Errorf("result %d: missing %v, want %v", i, r.Missing, w.missing)
		}
		if !slices.Equal(r.PartsPresent, w.parts) {
			h.t.Errorf("result %d: parts_present %v, want %v", i, r.PartsPresent, w.parts)
		}
	}
}

func num(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			panic(err)
		}
		return i
	case float64:
		return int64(n)
	}
	panic(fmt.Sprintf("not a number: %T %v", v, v))
}

// ---- log inspection

func (h *harness) commitsOf(account string) [][]byte {
	h.t.Helper()
	var out [][]byte
	if err := h.store.Read(h.ctx, func(r *storage.ReadTx) error {
		cs, err := r.Account(account).Commits(0, 1<<53, 0)
		for _, c := range cs {
			out = append(out, c.Canonical)
		}
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
	return out
}

func (h *harness) lastCommit() map[string]any {
	h.t.Helper()
	cs := h.commitsOf(h.account)
	if len(cs) == 0 {
		h.t.Fatal("no commit")
	}
	v, err := contract.DecodeJSON(cs[len(cs)-1])
	if err != nil {
		h.t.Fatal(err)
	}
	return v.(map[string]any)
}

type slotWant struct {
	install string
	seq     int64
	op      string
	hash    string
	result  string
}

// expectLastCommit checks the newest commit's slot entries (PROTOCOL §6.1, §8.1: {install_id,
// install_seq, op_id, content_hash, result}) and the op IDs of its accepted entries.
func (h *harness) expectLastCommit(headSeq int64, accepted []string, slots ...slotWant) {
	h.t.Helper()
	if got := h.head().Seq; got != headSeq {
		h.t.Fatalf("head seq %d, want %d", got, headSeq)
	}
	c := h.lastCommit()
	if num(c["seq"]) != headSeq {
		h.t.Fatalf("last commit seq %v", c["seq"])
	}
	list, _ := c["slots"].([]any)
	if len(list) != len(slots) {
		h.t.Fatalf("commit %d has %d slot entries %s, want %d", headSeq, len(list), canon(list), len(slots))
	}
	for i, s := range slots {
		want := map[string]any{"install_id": s.install, "install_seq": s.seq, "op_id": s.op, "content_hash": s.hash, "result": s.result}
		if !bytes.Equal(canon(list[i]), canon(want)) {
			h.t.Errorf("commit %d slot %d: %s, want %s", headSeq, i, canon(list[i]), canon(want))
		}
	}
	var ops []string
	for _, e := range c["accepted"].([]any) {
		ops = append(ops, e.(map[string]any)["fact"].(map[string]any)["op_id"].(string))
	}
	want := slices.Clone(accepted)
	sort.Strings(want)
	if !slices.Equal(ops, want) {
		h.t.Errorf("commit %d accepted %v, want %v", headSeq, ops, want)
	}
}

func (h *harness) consumed(install string) int64 {
	h.t.Helper()
	var n int64
	if err := h.store.Read(h.ctx, func(r *storage.ReadTx) error {
		var err error
		n, err = r.Account(h.account).ConsumedSeq(install)
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
	return n
}

// db is an independent connection to the database file (evidence of what is
// stored, and a way to simulate a lineage change).
func (h *harness) db() *sql.DB {
	h.dbOnce.Do(func() {
		db, err := sql.Open("sqlite", filepath.Join(h.dir, "nottennapod.db")+"?_pragma=busy_timeout(10000)")
		if err != nil {
			h.t.Fatal(err)
		}
		h.rawDB = db
	})
	return h.rawDB
}

func (h *harness) count(query string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.db().QueryRow(query, args...).Scan(&n); err != nil {
		h.t.Fatalf("%s: %v", query, err)
	}
	return n
}

// bumpEpoch runs the real restore transition (PROTOCOL §11): every
// account's log, the given one included, moves to a new epoch, credentials and
// the old timeline's views and push parts are dropped.
func (h *harness) bumpEpoch(account string) {
	h.t.Helper()
	if err := h.store.Write(h.ctx, func(w *storage.WriteTx) error { return w.RestoreTransition("test: " + account) }); err != nil {
		h.t.Fatal(err)
	}
}

// archived returns the stored fact and receipt bytes of op.
func (h *harness) archived(op string) (fact, receipt []byte) {
	h.t.Helper()
	if err := h.db().QueryRow(`SELECT fact, receipt FROM facts WHERE account_id = ? AND op_id = ?`, h.account, op).Scan(&fact, &receipt); err != nil {
		h.t.Fatalf("archived %s: %v", op, err)
	}
	return fact, receipt
}

// verifyLog checks what any replica relies on: the digest chain from genesis to
// the head, one slot entry per consumed slot (and exactly the stored ones), the
// watermark of every installation, every slot entry consistent with the archive,
// every fact and decision first committed exactly once and equal to the archive.
func (h *harness) verifyLog(account string) {
	h.t.Helper()
	if err := h.store.VerifyChain(h.ctx, account); err != nil {
		h.t.Fatalf("VerifyChain: %v", err)
	}
	head := h.headOf(account)
	digest := contract.GenesisDigest
	type slotKey struct {
		install string
		seq     int64
	}
	slots := map[slotKey]map[string]any{}
	slotCommit := map[slotKey]int64{}
	facts := map[string][2][]byte{}
	factCommit := map[string]int64{}
	decisions := map[string][]byte{}
	for i, c := range h.commitsOf(account) {
		var err error
		if digest, err = contract.ChainDigest(digest, c); err != nil {
			h.t.Fatal(err)
		}
		v, err := contract.DecodeJSON(c)
		if err != nil {
			h.t.Fatal(err)
		}
		commit := v.(map[string]any)
		seq := num(commit["seq"])
		if seq != int64(i+1) || num(commit["epoch"]) < 1 {
			h.t.Errorf("commit %d has seq %d", i+1, seq)
		}
		for _, s := range commit["slots"].([]any) {
			m := s.(map[string]any)
			if len(m) != 5 {
				h.t.Errorf("slot entry %s does not have exactly the five amended fields", canon(m))
			}
			k := slotKey{m["install_id"].(string), num(m["install_seq"])}
			if _, dup := slots[k]; dup {
				h.t.Errorf("slot %v committed twice", k)
			}
			if r := m["result"]; r != "accepted" && r != "duplicate" && r != "op_id_conflict" {
				h.t.Errorf("slot %v result %v", k, r)
			}
			slots[k], slotCommit[k] = m, seq
		}
		for _, e := range commit["accepted"].([]any) {
			entry := e.(map[string]any)
			f := entry["fact"].(map[string]any)
			op := f["op_id"].(string)
			if _, dup := facts[op]; dup {
				h.t.Errorf("fact %s first committed twice", op)
			}
			r := entry["receipt"].(map[string]any)
			if num(r["seq"]) != seq {
				h.t.Errorf("fact %s receipt seq %v in commit %d", op, r["seq"], seq)
			}
			facts[op], factCommit[op] = [2][]byte{canon(f), canon(r)}, seq
		}
		for _, d := range commit["decisions"].([]any) {
			m := d.(map[string]any)
			id := m["decision_id"].(string)
			if _, dup := decisions[id]; dup {
				h.t.Errorf("decision %s committed twice", id)
			}
			decisions[id] = canon(m)
		}
	}
	if !bytes.Equal(digest, head.Digest) || int64(len(h.commitsOf(account))) != head.Seq {
		h.t.Fatalf("chain from genesis does not reach the head")
	}
	// Stored transport slots equal the slot entries in the log.
	rows, err := h.db().Query(`SELECT install_id, install_seq, op_id, content_hash, result, seq FROM transport_slots WHERE account_id = ?`, account)
	if err != nil {
		h.t.Fatal(err)
	}
	stored := 0
	maxSeq := map[string]int64{}
	countBy := map[string]int64{}
	for rows.Next() {
		var k slotKey
		var op, hash, result string
		var seq int64
		if err := rows.Scan(&k.install, &k.seq, &op, &hash, &result, &seq); err != nil {
			h.t.Fatal(err)
		}
		stored++
		m := slots[k]
		want := map[string]any{"install_id": k.install, "install_seq": k.seq, "op_id": op, "content_hash": hash, "result": result}
		if m == nil || !bytes.Equal(canon(m), canon(want)) || slotCommit[k] != seq {
			h.t.Errorf("stored slot %v (%s %s %s at %d) differs from the log %s at %d", k, op, hash, result, seq, canon(m), slotCommit[k])
		}
		countBy[k.install]++
		maxSeq[k.install] = max(maxSeq[k.install], k.seq)
		// The slot entry agrees with the archive.
		arch, ok := facts[op]
		var archHash string
		if ok {
			var r map[string]any
			v, _ := contract.DecodeJSON(arch[1])
			r = v.(map[string]any)
			archHash = r["content_hash"].(string)
		}
		switch result {
		case "accepted":
			if !ok || archHash != hash || factCommit[op] != seq {
				h.t.Errorf("accepted slot %v does not match its first commit", k)
			}
		case "duplicate":
			if !ok || archHash != hash || factCommit[op] > seq {
				h.t.Errorf("duplicate slot %v does not match an earlier commit", k)
			}
		case "op_id_conflict":
			if ok && archHash == hash {
				h.t.Errorf("op_id_conflict slot %v has the archived content", k)
			}
		}
	}
	rows.Close()
	if stored != len(slots) {
		h.t.Errorf("%d stored slots, %d slot entries in the log", stored, len(slots))
	}
	for install, n := range countBy {
		if maxSeq[install] != n {
			h.t.Errorf("installation %s: slots are not contiguous from 1 (%d slots, max %d)", install, n, maxSeq[install])
		}
		var consumed int64
		if err := h.db().QueryRow(`SELECT consumed_seq FROM installations WHERE account_id = ? AND install_id = ?`, account, install).Scan(&consumed); err != nil {
			h.t.Fatal(err)
		}
		if consumed != n {
			h.t.Errorf("installation %s: consumed_seq %d, %d slots", install, consumed, n)
		}
	}
	// The archive is exactly what the log first committed.
	frows, err := h.db().Query(`SELECT op_id, fact, receipt FROM facts WHERE account_id = ?`, account)
	if err != nil {
		h.t.Fatal(err)
	}
	n := 0
	for frows.Next() {
		var op string
		var f, r []byte
		if err := frows.Scan(&op, &f, &r); err != nil {
			h.t.Fatal(err)
		}
		n++
		if got, ok := facts[op]; !ok || !bytes.Equal(got[0], f) || !bytes.Equal(got[1], r) {
			h.t.Errorf("archived fact %s differs from its commit", op)
		}
	}
	frows.Close()
	if n != len(facts) {
		h.t.Errorf("%d archived facts, %d in the log", n, len(facts))
	}
	drows, err := h.db().Query(`SELECT decision_id, decision FROM decisions WHERE account_id = ?`, account)
	if err != nil {
		h.t.Fatal(err)
	}
	n = 0
	for drows.Next() {
		var id string
		var d []byte
		if err := drows.Scan(&id, &d); err != nil {
			h.t.Fatal(err)
		}
		n++
		if !bytes.Equal(decisions[id], d) {
			h.t.Errorf("stored decision %s differs from its commit", id)
		}
	}
	drows.Close()
	if n != len(decisions) {
		h.t.Errorf("%d stored decisions, %d in the log", n, len(decisions))
	}
}

// ---- wire pages

func wireJSON(t testing.TB, v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // as the API encodes responses
	if err := enc.Encode(v); err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

type wirePage struct {
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

type framePart struct {
	Seq       int64  `json:"seq"`
	Index     int64  `json:"index"`
	PartIndex int64  `json:"part_index"`
	PartCount int64  `json:"part_count"`
	Data      string `json:"data"`
}

// frames reassembles framed items (commit_part or item_part) in page order.
type frames struct {
	kind  string
	key   int64
	count int64
	next  int64
	buf   []byte
}

// add consumes one item (kind is "commit_part" for pulls, "item_part" for
// snapshots). It reports whether the item was a frame and returns the whole item
// bytes when one completes.
func (f *frames) add(kind string, raw json.RawMessage) (whole []byte, done, framed bool, err error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, false, false, err
	}
	partRaw, framed := m[kind]
	if !framed {
		if f.buf != nil {
			return nil, false, false, errors.New("a whole item interrupts a framed item")
		}
		if kind == "commit_part" {
			c, ok := m["commit"]
			if !ok || len(m) != 1 {
				return nil, false, false, fmt.Errorf("pull item is neither commit nor commit_part: %s", raw)
			}
			return c, true, false, nil
		}
		return raw, true, false, nil
	}
	if len(m) != 1 {
		return nil, false, true, fmt.Errorf("framed item has extra members: %s", raw)
	}
	var p framePart
	if err := json.Unmarshal(partRaw, &p); err != nil {
		return nil, false, true, err
	}
	key := p.Seq
	if kind == "item_part" {
		key = p.Index
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(p.Data)
	if err != nil || len(data) == 0 || len(data) > PageBytes {
		return nil, false, true, fmt.Errorf("bad part data (%d bytes): %v", len(data), err)
	}
	if f.buf == nil {
		if p.PartIndex != 0 || p.PartCount < 2 {
			return nil, false, true, fmt.Errorf("framed item starts at part %d of %d", p.PartIndex, p.PartCount)
		}
		f.key, f.count, f.next, f.buf = key, p.PartCount, 0, []byte{}
	}
	if key != f.key || p.PartCount != f.count || p.PartIndex != f.next {
		return nil, false, true, fmt.Errorf("part %d/%d of %d out of order (expected %d/%d of %d)", p.PartIndex, p.PartCount, key, f.next, f.count, f.key)
	}
	f.buf = append(f.buf, data...)
	f.next++
	if f.next == f.count {
		out := f.buf
		f.buf = nil
		return out, true, true, nil
	}
	return nil, false, true, nil
}

type pulled struct {
	head      contract.Cursor
	headStr   string
	commits   [][]byte
	pages     [][]byte
	framed    int // commit_part items seen
	watermark int64
}

// pullAll pulls every page of one view from `from`, checking every page rule of
// PROTOCOL §6.4, and verifies the staged commits against the digest chain.
// Each page is requested twice (a lost response); the repeat must be identical.
func (h *harness) pullAll(account, install string, from contract.Cursor, acked int64, limit int) (*pulled, error) {
	h.t.Helper()
	req := PullRequest{AccountID: account, Cursor: from, InstallID: install, AckedInstallSeq: acked, Limit: limit}
	out := &pulled{}
	fr := &frames{kind: "commit_part"}
	fromStr, _ := from.Encode()
	for first := true; ; first = false {
		resp, err := h.svc.Pull(h.ctx, req)
		if err != nil {
			return nil, err
		}
		page := wireJSON(h.t, resp)
		if !first {
			again, err := h.svc.Pull(h.ctx, req)
			if err != nil {
				return nil, fmt.Errorf("repeating a page token: %w", err)
			}
			if !bytes.Equal(wireJSON(h.t, again), page) {
				h.t.Errorf("repeating a page token returned a different page:\n%s\n%s", page, wireJSON(h.t, again))
			}
		}
		out.pages = append(out.pages, page)
		var p wirePage
		if err := json.Unmarshal(page, &p); err != nil {
			h.t.Fatal(err)
		}
		if first {
			out.headStr, out.watermark = p.Head, p.InstallWatermark
		} else if p.Head != out.headStr || p.InstallWatermark != out.watermark {
			h.t.Errorf("head/watermark changed within one view: %s %d vs %s %d", p.Head, p.InstallWatermark, out.headStr, out.watermark)
		}
		if p.From != fromStr {
			h.t.Errorf("page from %q, want the request cursor", p.From)
		}
		if p.HasMore != (p.Next != "") {
			h.t.Errorf("has_more %v with next %q", p.HasMore, p.Next)
		}
		budget := 0
		for _, item := range p.Items {
			c, done, framed, err := fr.add("commit_part", item)
			if err != nil {
				h.t.Fatalf("pull item: %v", err)
			}
			if framed {
				out.framed++
				if len(p.Items) != 1 {
					h.t.Errorf("a commit_part shares its page with %d other items", len(p.Items)-1)
				}
			} else {
				budget += len(c)
			}
			if done {
				out.commits = append(out.commits, c)
			}
		}
		if budget > PageBytes {
			h.t.Errorf("a pull page carries %d bytes of commits, above the 1 MiB page bound", budget)
		}
		if !p.HasMore {
			break
		}
		req.Page = p.Next
	}
	if fr.buf != nil {
		h.t.Fatal("pull ended inside a framed commit")
	}
	head, err := contract.DecodeCursor(out.headStr)
	if err != nil {
		h.t.Fatalf("head cursor: %v", err)
	}
	out.head = head
	digest := from.Digest
	for i, c := range out.commits {
		v, err := contract.DecodeJSON(c)
		if err != nil {
			h.t.Fatalf("commit does not decode: %v", err)
		}
		if !bytes.Equal(canon(v), c) {
			h.t.Errorf("served commit bytes are not canonical")
		}
		if got := num(v.(map[string]any)["seq"]); got != from.Seq+int64(i)+1 {
			h.t.Errorf("commit seq %d, want %d", got, from.Seq+int64(i)+1)
		}
		digest, _ = contract.ChainDigest(digest, c)
	}
	if int64(len(out.commits)) != head.Seq-from.Seq || !bytes.Equal(digest, head.Digest) {
		h.t.Errorf("pulled commits (%d) do not verify from %d to head %d", len(out.commits), from.Seq, head.Seq)
	}
	return out, nil
}

type snapped struct {
	headStr   string
	items     [][]byte
	pages     [][]byte
	framed    int
	watermark int64
	count     int64
	digest    string
}

// snapshotAll fetches every page of a snapshot (repeating each token once, which
// must reproduce the page) and verifies the manifest over the reassembled items.
// When req.Page continues a view, prior holds the items already received.
func (h *harness) snapshotAll(req SnapshotRequest, prior ...[]byte) (*snapped, error) {
	h.t.Helper()
	out := &snapped{items: prior}
	fr := &frames{kind: "item_part"}
	for first := true; ; first = false {
		resp, err := h.svc.Snapshot(h.ctx, req)
		if err != nil {
			return out, err
		}
		page := wireJSON(h.t, resp)
		if req.Page != "" {
			again, err := h.svc.Snapshot(h.ctx, req)
			if err != nil {
				return out, fmt.Errorf("repeating a snapshot token: %w", err)
			}
			if !bytes.Equal(wireJSON(h.t, again), page) {
				h.t.Errorf("repeating a snapshot token returned a different page")
			}
		}
		out.pages = append(out.pages, page)
		var p wirePage
		if err := json.Unmarshal(page, &p); err != nil {
			h.t.Fatal(err)
		}
		if first {
			out.headStr, out.watermark = p.Head, p.InstallWatermark
		} else if p.Head != out.headStr || p.InstallWatermark != out.watermark {
			h.t.Errorf("snapshot head/watermark changed between pages")
		}
		budget := 0
		for _, item := range p.Items {
			whole, done, framed, err := fr.add("item_part", item)
			if err != nil {
				h.t.Fatalf("snapshot item: %v", err)
			}
			if framed {
				out.framed++
			} else {
				budget += len(whole)
			}
			if done {
				out.items = append(out.items, whole)
			}
		}
		if budget > PageBytes {
			h.t.Errorf("a snapshot page carries %d bytes of items, above the 1 MiB page bound", budget)
		}
		if p.HasMore != (p.Next != "") || (p.Manifest != nil) == p.HasMore {
			h.t.Errorf("snapshot page: has_more %v, next %q, manifest %v", p.HasMore, p.Next, p.Manifest != nil)
		}
		if !p.HasMore {
			if p.Manifest == nil {
				h.t.Fatal("last snapshot page has no manifest")
			}
			out.count, out.digest = p.Manifest.ItemCount, p.Manifest.SnapshotDigest
			break
		}
		req.Page = p.Next
	}
	if fr.buf != nil {
		h.t.Fatal("snapshot ended inside a framed item")
	}
	d := contract.GenesisDigest
	for _, it := range out.items {
		v, err := contract.DecodeJSON(it)
		if err != nil || !bytes.Equal(canon(v), it) {
			h.t.Errorf("snapshot item is not canonical JSON: %v", err)
		}
		d, _ = contract.ChainDigest(d, it)
	}
	if out.count != int64(len(out.items)) || hex.EncodeToString(d) != out.digest {
		h.t.Errorf("manifest (%d, %s) does not verify over %d items (%x)", out.count, out.digest, len(out.items), d)
	}
	return out, nil
}

// ---- independent replica checks of snapshot items

type snapItem struct {
	kind     string // accepted, decision, record
	id       string
	fact     map[string]any
	receipt  map[string]any
	decision map[string]any
}

func parseItems(t testing.TB, items [][]byte) []snapItem {
	t.Helper()
	var out []snapItem
	for _, raw := range items {
		v, err := contract.DecodeJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		m := v.(map[string]any)
		if len(m) != 1 {
			t.Fatalf("snapshot item has %d members: %s", len(m), raw)
		}
		switch {
		case m["accepted"] != nil:
			e := m["accepted"].(map[string]any)
			f, r := e["fact"].(map[string]any), e["receipt"].(map[string]any)
			out = append(out, snapItem{kind: "accepted", id: f["op_id"].(string), fact: f, receipt: r})
		case m["decision"] != nil:
			d := m["decision"].(map[string]any)
			out = append(out, snapItem{kind: "decision", id: d["decision_id"].(string), decision: d})
		case m["record"] != nil:
			out = append(out, snapItem{kind: "record"})
		default:
			t.Fatalf("unknown snapshot item %s", raw)
		}
	}
	return out
}

func strList(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

// replicaOrder computes, from wire data only, the causal order a replica derives
// (PROTOCOL §6.9 with the PROTOCOL §6, §8.1): admitted facts and decisions are
// nodes; a dependency on a rejected fact stands for that fact's signed frontier.
// It returns the order and each admitted fact's resolved frontier.
func replicaOrder(t testing.TB, items []snapItem) ([]string, map[string][]string) {
	t.Helper()
	rejected := map[string][]string{}
	for _, it := range items {
		if it.kind == "accepted" && it.receipt["outcome"] == "rejected" {
			rejected[it.id] = strList(it.receipt["frontier"])
		}
	}
	var nodes []contract.OrderedFact
	resolved := map[string][]string{}
	for _, it := range items {
		switch {
		case it.kind == "accepted" && it.receipt["outcome"] != "rejected":
			set := map[string]bool{}
			for _, d := range strList(it.fact["dependencies"]) {
				if fr, ok := rejected[d]; ok {
					for _, x := range fr {
						set[x] = true
					}
				} else {
					set[d] = true
				}
			}
			var deps []string
			for d := range set {
				deps = append(deps, d)
			}
			sort.Strings(deps)
			resolved[it.id] = deps
			nodes = append(nodes, contract.OrderedFact{ID: it.id, Dependencies: deps, NormalizedTime: num(it.receipt["normalized_time"])})
		case it.kind == "decision":
			nodes = append(nodes, contract.OrderedFact{ID: it.id, Dependencies: strList(it.decision["dependencies"]),
				NormalizedTime: num(it.decision["normalized_time"])})
		}
	}
	order, err := contract.CausalOrder(nodes)
	if err != nil {
		t.Fatalf("a replica cannot order the snapshot: %v", err)
	}
	return order, resolved
}

// checkSnapshotItems verifies what a receiving replica checks: every receipt and
// decision MAC, every content hash, causal order of admitted facts and decisions
// (recomputed independently), then rejected entries by (epoch, seq, op_id), then
// records; and that normalized times never go below a resolved dependency's.
func checkSnapshotItems(t testing.TB, keys map[string][]byte, account string, raw [][]byte) []snapItem {
	t.Helper()
	items := parseItems(t, raw)
	times := map[string]int64{}
	for _, it := range items {
		switch it.kind {
		case "accepted":
			checkReceipt(t, keys, account, it.receipt)
			h, _ := contract.ContentHash(it.fact)
			if it.receipt["content_hash"] != h || it.receipt["op_id"] != it.id {
				t.Errorf("snapshot receipt for %s does not match its fact", it.id)
			}
			if it.receipt["outcome"] != "rejected" {
				times[it.id] = num(it.receipt["normalized_time"])
			}
		case "decision":
			key := keys[it.decision["key_id"].(string)]
			if !contract.VerifyMAC(key, account, it.decision) {
				t.Errorf("decision %s MAC does not verify", it.id)
			}
			times[it.id] = num(it.decision["normalized_time"])
		}
	}
	// Sections: causal items, then rejected entries, then records.
	section := func(it snapItem) int {
		switch {
		case it.kind == "record":
			return 2
		case it.kind == "accepted" && it.receipt["outcome"] == "rejected":
			return 1
		}
		return 0
	}
	for i := 1; i < len(items); i++ {
		if section(items[i]) < section(items[i-1]) {
			t.Errorf("snapshot item %d (%s %s) comes after a later section", i, items[i].kind, items[i].id)
		}
	}
	order, resolved := replicaOrder(t, items)
	var causal []string
	for _, it := range items {
		if section(it) == 0 {
			causal = append(causal, it.id)
		}
	}
	if !slices.Equal(causal, order) {
		t.Errorf("snapshot causal sequence differs from the replica's causal order:\n got  %v\n want %v", causal, order)
	}
	for id, deps := range resolved {
		for _, d := range deps {
			if times[id] < times[d] {
				t.Errorf("%s normalized time %d below its resolved dependency %s (%d)", id, times[id], d, times[d])
			}
		}
	}
	var rej []snapItem
	for _, it := range items {
		if section(it) == 1 {
			rej = append(rej, it)
		}
	}
	sorted := slices.IsSortedFunc(rej, func(a, b snapItem) int {
		for _, f := range []string{"epoch", "seq"} {
			if x, y := num(a.receipt[f]), num(b.receipt[f]); x != y {
				if x < y {
					return -1
				}
				return 1
			}
		}
		return strings.Compare(a.id, b.id)
	})
	if !sorted {
		t.Errorf("rejected entries are not ordered by (epoch, seq, op_id)")
	}
	return items
}

// verifySnapshot takes an attach snapshot (small pages, so tokens are exercised)
// and checks its order, MACs and manifest, and that it holds exactly the archive
// and the decisions.
func (h *harness) verifySnapshot(install string) []snapItem {
	h.t.Helper()
	s, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: install, Limit: 3})
	if err != nil {
		h.t.Fatalf("snapshot: %v", err)
	}
	items := checkSnapshotItems(h.t, h.keys(), h.account, s.items)
	facts, decisions := 0, 0
	for _, it := range items {
		switch it.kind {
		case "accepted":
			facts++
			f, r := h.archived(it.id)
			if !bytes.Equal(f, canon(it.fact)) || !bytes.Equal(r, canon(it.receipt)) {
				h.t.Errorf("snapshot entry %s differs from the archive", it.id)
			}
		case "decision":
			decisions++
		}
	}
	if n := h.count(`SELECT COUNT(*) FROM facts WHERE account_id = ?`, h.account); n != facts {
		h.t.Errorf("snapshot has %d facts, archive %d", facts, n)
	}
	if n := h.count(`SELECT COUNT(*) FROM decisions WHERE account_id = ?`, h.account); n != decisions {
		h.t.Errorf("snapshot has %d decisions, store %d", decisions, n)
	}
	return items
}

func indexOf(items []snapItem, id string) int {
	return slices.IndexFunc(items, func(it snapItem) bool { return it.id == id })
}

// protoCode returns the protocol error code of err ("" if none).
func protoCode(err error) string {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func protoStatus(err error) int {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Status
	}
	return 0
}
