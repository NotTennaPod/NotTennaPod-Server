package protocol

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Bounds advertised by GET /v1/info (PROTOCOL §6.13).
const (
	MaxRequestBytes    = 4 << 20
	MaxOperations      = 500
	MaxUnframedFact    = 256 << 10
	MaxPartBytes       = 256 << 10
	MaxPartCount       = 64
	MaxFramedFact      = 8 << 20 // aggregate bytes of one framed fact (measured: well under a second to complete)
	MaxFactDepth       = 32      // nesting of a fact; commits and pages add at most 6 levels within contract-v1's 64
	PageBytes          = 1 << 20
	DefaultPageCommits = 1000
)

// Bounds returns the advertised bounds.
func Bounds() map[string]any {
	return map[string]any{"max_request_bytes": MaxRequestBytes, "max_operations": MaxOperations,
		"max_unframed_fact_bytes": MaxUnframedFact, "max_part_bytes": MaxPartBytes, "max_part_count": MaxPartCount,
		"max_framed_fact_bytes": MaxFramedFact, "max_fact_depth": MaxFactDepth,
		"page_bytes": PageBytes, "recovery_inventory": MaxRecoveryInventory}
}

// Error is a protocol error answered as a PROTOCOL §10 error body.
type Error struct {
	Status     int
	Code       string
	Message    string
	Missing    []string
	RetryAfter time.Duration // for server_busy
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func protoErr(status int, code, msg string) *Error {
	return &Error{Status: status, Code: code, Message: msg}
}

var (
	errLineageUnknown  = protoErr(409, "lineage_unknown", "This server does not know this library's history.")
	errLineageMismatch = protoErr(409, "lineage_mismatch", "This server's history differs from this library's; recovery is needed.")
)

// Service executes the sync protocol for one store.
type Service struct {
	Store    *storage.Store
	Registry *Registry
	Now      func() time.Time
	Views    *ViewRegistry
	Box      *sealed.Box // secrets and staged parts at rest
	// Random supplies the server's choices in decisions (nil: crypto/rand);
	// rule vectors replay recorded choices through it.
	Random Randomness

	// beforeCommit is a test hook inside the push transaction.
	beforeCommit func() error
	// secretsPrepared is a test hook once a push's secrets are prepared,
	// before its transaction.
	secretsPrepared func()
	// StagingThreshold is the archive size (facts and decisions) from which
	// a rebuild or a recovery request runs in a staging copy (PROTOCOL §6.11): zero
	// means DefaultStagingThreshold, a negative value never.
	StagingThreshold int
}

// Authorize re-validates the caller inside the write transaction and returns
// the caller's current installation (PROTOCOL §5 `revalidate`).
type Authorize func(tx *storage.WriteTx) (*storage.Installation, error)

// AuthorizeRead is Authorize inside a read transaction (a handler serving
// archived content checks the caller in the snapshot it reads).
type AuthorizeRead func(tx *storage.ReadTx) (*storage.Installation, error)

// PushOp is one operation of a push request.
type PushOp struct {
	InstallSeq int64
	Fact       *Archived // whole fact, already parsed
	Part       *Part
}

// Part is one slice of a framed fact.
type Part struct {
	TransactionID string
	Index         int
	Count         int
	ContentHash   string
	Data          []byte
}

// Secret is a credential carried beside the operations (PROTOCOL §7.2).
type Secret struct {
	ID       string
	Username string
	Password string
	// ApprovedOrigins are the origins the server may send the credentials to
	// (PROTOCOL §9.2, §9.4); nil when the client sent none.
	ApprovedOrigins []string
}

// PushRequest is a decoded push.
type PushRequest struct {
	AccountID       string
	Cursor          contract.Cursor
	InstallID       string
	AckedInstallSeq int64
	Operations      []PushOp
	Secrets         []Secret
}

// PushResult is one operation's result.
type PushResult struct {
	InstallSeq   int64          `json:"install_seq"`
	Status       string         `json:"status"`
	Receipt      map[string]any `json:"receipt,omitempty"`
	Missing      []string       `json:"missing,omitempty"`
	PartsPresent []int          `json:"parts_present,omitempty"`
}

// PushResponse is the push response body.
type PushResponse struct {
	Head             string       `json:"head"`
	InstallWatermark int64        `json:"install_watermark"`
	Results          []PushResult `json:"results"`
}

// commitBuilder accumulates one push's commit.
type commitBuilder struct {
	key       storage.ReceiptKey
	accepted  []map[string]any
	decisions []map[string]any
	slots     []Slot
	records   map[[3]string]Record
	ids       map[string]bool
	dirty     map[string]string // scope → last fact that deferred its refold
	random    Randomness
	index     int64 // next receipt or decision index in this commit (PROTOCOL §6.10)
	// deferred is work a server commit batches until its next flush (Defer),
	// by key, in first-request order.
	deferred    []string
	deferredFns map[string]func(*ApplyContext) error
	// The queue replay (PROTOCOL §7.6): nodes whose replay inputs
	// changed, whether the whole replay is invalid, the publisher's state
	// and what runs once the commit is durable.
	queueTouched map[string]bool
	queueAll     bool
	queueKeys    bool // key writes the replay does not derive (a recovered layout)
	queueState   any
	after        []func(storage.Head)
}

// runAfter runs the commit's after-commit work with the account's head.
func (b *commitBuilder) runAfter(head storage.Head) {
	for _, fn := range b.after {
		fn(head)
	}
	b.after = nil
}

// publishQueue runs the registered queue publisher at the end of a commit.
func (s *Service) publishQueue(c *ApplyContext, full, layout bool) error {
	if s.Registry == nil || s.Registry.publisher == nil {
		return nil
	}
	return s.Registry.publisher(c, full, layout)
}

// nextIndex reserves the next position in the commit's admission order.
func (b *commitBuilder) nextIndex() int64 {
	i := b.index
	b.index++
	return i
}

func (b *commitBuilder) record(r Record) {
	if b.records == nil {
		b.records = map[[3]string]Record{}
	}
	if old, ok := b.records[r.Key()]; ok {
		r.Provenance = append(slices.Clone(old.Provenance), r.Provenance...)
	}
	b.records[r.Key()] = r
}

// flushDirty refolds the scopes whose refold was deferred (listening facts,
// whose outcome does not depend on the fold) once per push.
func (p *pusher) flushDirty() error {
	return flushDirty(p.b, ApplyContext{Account: p.acct, AccountID: p.accountID, Namespace: p.namespace, ReceiveTime: p.receive,
		Revision: p.rev})
}

func (s *Service) random() Randomness {
	if s.Random != nil {
		return s.Random
	}
	return cryptoRandom{}
}

// Randomness is the source of a server's choices in decisions.
type Randomness interface {
	DecisionID(decisionType string) string // a fresh UUID (checked unused by the caller)
	PositionID() [16]byte                  // a fresh position-write ID
	Index(n int) int                       // uniform in [0, n)
}

type cryptoRandom struct{}

func (cryptoRandom) DecisionID(string) string { return randomUUID() }
func (cryptoRandom) PositionID() [16]byte {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		panic(err)
	}
	return u
}
func (cryptoRandom) Index(n int) int {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	// Rejection sampling for an unbiased draw.
	limit := ^uint64(0) - ^uint64(0)%uint64(n)
	for {
		x := uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 | uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
		if x < limit {
			return int(x % uint64(n))
		}
		if _, err := rand.Read(b[:]); err != nil {
			panic(err)
		}
	}
}

func randomUUID() string {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		panic(err)
	}
	u[6] = u[6]&0x0f | 0x40
	u[8] = u[8]&0x3f | 0x80
	h := hex.EncodeToString(u[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// newNodeID returns a random UUID not used by any node or fact.
func (b *commitBuilder) newNodeID(acct *storage.AccountTx, decisionType string) (string, error) {
	for {
		id := b.random.DecisionID(decisionType)
		used, err := acct.IsNodeOrFact(id)
		if err != nil {
			return "", err
		}
		if !used && !b.ids[id] {
			b.ids[id] = true
			return id, nil
		}
	}
}

func (b *commitBuilder) body() storage.CommitBody {
	var body storage.CommitBody
	// By fact, then receipt position: a recovery commit may carry one fact's
	// original receipt and a later one it keeps.
	position := func(a map[string]any) [3]int64 {
		r, _ := a["receipt"].(map[string]any)
		e, _ := integer(r["epoch"])
		s, _ := integer(r["seq"])
		x, _ := integer(r["index"])
		return [3]int64{e, s, x}
	}
	sort.Slice(b.accepted, func(i, j int) bool {
		oi, oj := b.accepted[i]["fact"].(map[string]any)["op_id"].(string), b.accepted[j]["fact"].(map[string]any)["op_id"].(string)
		if oi != oj {
			return oi < oj
		}
		pi, pj := position(b.accepted[i]), position(b.accepted[j])
		return slices.Compare(pi[:], pj[:]) < 0
	})
	for _, a := range b.accepted {
		body.Accepted = append(body.Accepted, a)
	}
	sort.Slice(b.decisions, func(i, j int) bool {
		return b.decisions[i]["decision_id"].(string) < b.decisions[j]["decision_id"].(string)
	})
	for _, d := range b.decisions {
		body.Decisions = append(body.Decisions, d)
	}
	sort.Slice(b.slots, func(i, j int) bool {
		if b.slots[i].InstallID != b.slots[j].InstallID {
			return b.slots[i].InstallID < b.slots[j].InstallID
		}
		return b.slots[i].InstallSeq < b.slots[j].InstallSeq
	})
	for _, s := range b.slots {
		body.Slots = append(body.Slots, s.Object())
	}
	keys := make([][3]string, 0, len(b.records))
	for k := range b.records {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		for x := 0; x < 3; x++ {
			if keys[i][x] != keys[j][x] {
				return keys[i][x] < keys[j][x]
			}
		}
		return false
	})
	for _, k := range keys {
		body.Records = append(body.Records, b.records[k].Object())
	}
	return body
}

// Push runs one push in one write transaction (PROTOCOL §6.4).
func (s *Service) Push(ctx context.Context, req PushRequest, authorize Authorize) (*PushResponse, error) {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return nil, fmt.Errorf("protocol: no receipt key: %v", err)
	}
	if err := s.busy(req.AccountID); err != nil {
		return nil, err
	}
	receive := s.Now().UnixMilli()
	// Content that must never be archived is refused before anything is stored,
	// whatever the fact's type or envelope validity (PROTOCOL §7.2).
	for _, op := range req.Operations {
		if op.Fact != nil && ContainsCredentialURL(op.Fact.Object) {
			return nil, protoErr(400, "invalid_request", "A fact contains a URL with credentials; send credentials as secrets.")
		}
	}
	secrets, err := s.prepareSecrets(req.AccountID, req.Secrets)
	if err != nil {
		return nil, err
	}
	if s.secretsPrepared != nil {
		s.secretsPrepared()
	}
	var resp *PushResponse
	var finalHead storage.Head
	var after *commitBuilder
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		after = nil
		inst, err := authorize(tx)
		if err != nil {
			return err
		}
		if inst.InstallID != req.InstallID {
			return protoErr(403, "forbidden", "install_id is not this credential's installation.")
		}
		acct := tx.Account(req.AccountID)
		consumed, err := acct.ConsumedSeq(req.InstallID)
		if err != nil {
			return err
		}
		if err := checkLineage(acct, req.Cursor, req.AckedInstallSeq, consumed); err != nil {
			return err
		}
		// An import in progress owns the empty account until it publishes or
		// fails (PROTOCOL §11.5): writes wait, consuming nothing.
		if reserved, err := acct.ImportReserved(); err != nil {
			return err
		} else if reserved {
			return &Error{Status: 503, Code: "server_busy", Message: "This account's library is being imported; sync continues when it finishes.",
				RetryAfter: 30 * time.Second}
		}
		if err := s.putSecrets(acct, req.AccountID, secrets); err != nil {
			return err
		}
		namespace, err := acct.Namespace()
		if err != nil {
			return err
		}
		rev, err := acct.NextRevision()
		if err != nil {
			return err
		}
		p := &pusher{s: s, acct: acct, accountID: req.AccountID, namespace: namespace, inst: inst, rev: rev, receive: receive,
			b: &commitBuilder{key: keys[0], ids: map[string]bool{}, random: s.random()}}
		results := p.run(req, consumed)
		if p.err != nil {
			return p.err
		}
		if err := p.flushDirty(); err != nil {
			return err
		}
		if len(p.b.slots) > 0 {
			if err := s.publishQueue(&ApplyContext{Account: acct, AccountID: req.AccountID, Namespace: namespace, NormalizedTime: receive,
				ReceiveTime: receive, Revision: rev, commit: p.b, flush: p.flushDirty}, false, true); err != nil {
				return err
			}
			if _, _, err := acct.AppendCommit(p.b.body()); err != nil {
				return err
			}
		}
		if s.beforeCommit != nil {
			if err := s.beforeCommit(); err != nil {
				return err
			}
		}
		head, err := acct.Head()
		if err != nil {
			return err
		}
		cursor, err := contract.Cursor{LogID: head.LogID, Epoch: head.Epoch, Seq: head.Seq, Digest: head.Digest}.Encode()
		if err != nil {
			return err
		}
		watermark, err := acct.ConsumedSeq(req.InstallID)
		if err != nil {
			return err
		}
		resp = &PushResponse{Head: cursor, InstallWatermark: watermark, Results: results}
		finalHead, after = head, p.b
		return nil
	})
	if err != nil {
		return nil, err
	}
	if after != nil {
		after.runAfter(finalHead)
	}
	return resp, nil
}

// checkLineage applies PROTOCOL §6.4 lineage checks to a cursor and watermark.
func checkLineage(acct interface {
	Head() (storage.Head, error)
	CommitDigest(int64) ([]byte, error)
}, c contract.Cursor, acked, consumed int64) error {
	head, err := acct.Head()
	if err != nil {
		return err
	}
	if c.LogID != head.LogID {
		return errLineageUnknown
	}
	if c.Epoch != head.Epoch || c.Seq > head.Seq {
		return errLineageMismatch
	}
	d, err := acct.CommitDigest(c.Seq)
	if err != nil || !bytes.Equal(d, c.Digest) {
		return errLineageMismatch
	}
	if acked > consumed {
		return errLineageMismatch
	}
	return nil
}

type pusher struct {
	s         *Service
	acct      *storage.AccountTx
	accountID string
	namespace string
	inst      *storage.Installation
	rev       storage.Revision
	receive   int64
	b         *commitBuilder
	err       error

	// In a rebuild's replay (PROTOCOL §6.11): the archived receipt of the fact being
	// replayed, whose normalized time it keeps, and the decision oracle its
	// handler's decisions come from. Both nil in a live push.
	original *Receipt
	oracle   *rebuilder
	// recovery: the replay follows the receipts: a fact its
	// receipt admitted is admitted again whatever the checks say now.
	recovery bool
}

// follows reports whether this replay takes the fact's admission from its
// receipt rather than from the checks (recovery mode, an admitted receipt).
func (p *pusher) follows() bool {
	return p.recovery && p.original != nil && p.original.Outcome != "rejected"
}

// nodeOnly makes a fact a causal node at normalized time t, with the
// frontier its dependencies resolve to, placed in its chain, without
// effects: an original rejected receipt that replaced a provisional
// admission (PROTOCOL §6.11).
func (p *pusher) nodeOnly(a *Archived, t int64) error {
	author, seq, prev, ok := coordinates(a)
	if ok {
		if _, err := p.place(a.OpID, author, seq, prev); err != nil {
			return err
		}
	}
	var deps []string
	if a.Fact != nil {
		deps = a.Fact.Dependencies
	}
	edges, err := p.frontier(deps)
	if err != nil {
		return err
	}
	return p.acct.InsertNode(a.OpID, "fact", t, edges)
}

// frontier resolves dependencies to causal nodes: a node stands for
// itself, an archived rejected fact for its frontier; others are skipped.
func (p *pusher) frontier(deps []string) ([]string, error) {
	set := map[string]bool{}
	for _, d := range deps {
		if _, err := p.acct.NodeTime(d); err == nil {
			set[d] = true
			continue
		} else if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		f, err := p.acct.Fact(d)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		if f.Outcome != "rejected" {
			continue
		}
		ids, err := decodeIDList(f.Frontier)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if _, err := p.acct.NodeTime(id); err == nil {
				set[id] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func (p *pusher) run(req PushRequest, consumed int64) []PushResult {
	var results []PushResult
	next := consumed + 1
	for _, op := range req.Operations {
		r, consumedSlot, stop := p.one(op, next)
		if p.err != nil {
			return nil
		}
		results = append(results, r)
		if consumedSlot {
			next++
		}
		if stop {
			break
		}
	}
	return results
}

// one processes one operation; it reports whether a slot was consumed and
// whether processing must stop.
func (p *pusher) one(op PushOp, next int64) (PushResult, bool, bool) {
	res := PushResult{InstallSeq: op.InstallSeq}
	switch {
	case op.InstallSeq < next:
		r := p.replay(op, res)
		return r, false, r.Status == "installation_conflict"
	case op.InstallSeq > next:
		res.Status = "gap"
		return res, false, true
	}
	archived := op.Fact
	if op.Part != nil {
		var done bool
		archived, done = p.part(op, &res)
		if p.err != nil || !done {
			return res, false, true
		}
	}
	return p.admit(op.InstallSeq, archived, res)
}

// replay answers a slot that was already consumed with its original result.
func (p *pusher) replay(op PushOp, res PushResult) PushResult {
	slot, err := p.acct.Slot(p.inst.InstallID, op.InstallSeq)
	if err != nil {
		p.fail(err)
		return res
	}
	same := false
	if op.Part != nil {
		same = op.Part.ContentHash == slot.ContentHash
	} else {
		same = op.Fact.OpID == slot.OpID && op.Fact.ContentHash == slot.ContentHash
	}
	if !same {
		res.Status = "installation_conflict"
		return res
	}
	if slot.Result == "op_id_conflict" {
		res.Status = "op_id_conflict"
		return res
	}
	res.Status = "duplicate"
	res.Receipt = p.receiptOf(slot.OpID)
	return res
}

func (p *pusher) fail(err error) {
	if p.err == nil {
		p.err = err
	}
}

func (p *pusher) receiptOf(opID string) map[string]any {
	f, err := p.acct.Fact(opID)
	if err != nil {
		p.fail(err)
		return nil
	}
	v, err := contract.DecodeJSON(f.Receipt)
	if err != nil {
		p.fail(err)
		return nil
	}
	return v.(map[string]any)
}

// part stores one part and, when the slot is complete, reassembles the fact.
func (p *pusher) part(op PushOp, res *PushResult) (*Archived, bool) {
	part := op.Part
	// Metadata only: data is read for one conflicting index and at completion.
	stored, err := p.acct.PartsMeta(p.inst.InstallID, op.InstallSeq)
	if err != nil {
		p.fail(err)
		return nil, false
	}
	if len(stored) > 0 && stored[0].TransactionID != part.TransactionID {
		// A slot's assembly belongs to its first framing transaction (PROTOCOL §6.10): a
		// library never reframes a slot, so another transaction is another library
		// writing this slot (a copy).
		res.Status = "installation_conflict"
		return nil, false
	}
	total, present := len(part.Data), false
	for _, s := range stored {
		if s.Count != part.Count || s.ContentHash != part.ContentHash {
			res.Status = "invalid_part"
			return nil, false
		}
		if s.Index == part.Index {
			data, err := p.acct.PartData(p.inst.InstallID, op.InstallSeq, s.Index)
			if err == nil {
				data, err = p.openPart(op.InstallSeq, s.Index, data)
			}
			if err != nil {
				p.fail(err)
				return nil, false
			}
			if !bytes.Equal(data, part.Data) {
				res.Status = "invalid_part"
				return nil, false
			}
			present = true
			continue
		}
		total += s.Size
	}
	if total > MaxFramedFact {
		res.Status = "invalid_part" // over the advertised aggregate bound; nothing consumed
		return nil, false
	}
	if !present {
		if err := p.acct.StorePart(p.inst.InstallID, op.InstallSeq, storage.PartRow{Index: part.Index, TransactionID: part.TransactionID,
			Count: part.Count, ContentHash: part.ContentHash, Data: p.sealPart(op.InstallSeq, part.Index, part.Data), Size: len(part.Data)}); err != nil {
			p.fail(err)
			return nil, false
		}
		stored = append(stored, storage.PartMeta{Index: part.Index})
	}
	if len(stored) < part.Count {
		res.Status = "part_stored"
		indexes := make([]int, 0, len(stored))
		for _, s := range stored {
			indexes = append(indexes, s.Index)
		}
		slices.Sort(indexes)
		res.PartsPresent = indexes
		return nil, false
	}
	parts, err := p.acct.Parts(p.inst.InstallID, op.InstallSeq)
	if err != nil {
		p.fail(err)
		return nil, false
	}
	var buf bytes.Buffer
	buf.Grow(total)
	for _, s := range parts {
		data, err := p.openPart(op.InstallSeq, s.Index, s.Data)
		if err != nil {
			p.fail(err)
			return nil, false
		}
		buf.Write(data)
	}
	archived, ok := reassemble(buf.Bytes(), part.ContentHash)
	if ok && ContainsCredentialURL(archived.Object) {
		ok = false
	}
	if !ok {
		// A complete assembly that is invalid can never complete: discard it, so
		// its library can frame the fact again (the slot belongs to the first
		// transaction only while its assembly can still complete, PROTOCOL §6.10).
		if err := p.acct.DeleteParts(p.inst.InstallID, op.InstallSeq); err != nil {
			p.fail(err)
			return nil, false
		}
		res.Status = "invalid_part"
		return nil, false
	}
	return archived, true
}

func reassemble(data []byte, contentHash string) (*Archived, bool) {
	v, err := contract.DecodeJSON(data)
	if err != nil || contract.Depth(v) > MaxFactDepth {
		return nil, false
	}
	canonical, err := contract.Canonical(v)
	if err != nil || !bytes.Equal(canonical, data) {
		return nil, false
	}
	a, err := ParseFact(v)
	if err != nil || a.ContentHash != contentHash {
		return nil, false
	}
	return a, true
}

// admit handles a fact at the next slot: the transport checks (a known op_id
// or a collision with a decision ID), then acceptance, archiving and the slot.
func (p *pusher) admit(installSeq int64, a *Archived, res PushResult) (PushResult, bool, bool) {
	consume := func(result string) {
		p.b.slots = append(p.b.slots, Slot{InstallID: p.inst.InstallID, InstallSeq: installSeq, OpID: a.OpID, ContentHash: a.ContentHash, Result: result})
		if err := p.acct.ConsumeSlot(storage.SlotRow{InstallID: p.inst.InstallID, InstallSeq: installSeq, OpID: a.OpID,
			ContentHash: a.ContentHash, Result: result, Seq: p.rev.Seq}); err != nil {
			p.fail(err)
		}
		if err := p.acct.DeleteParts(p.inst.InstallID, installSeq); err != nil {
			p.fail(err)
		}
	}
	// Known op_id (or a collision with a decision ID).
	known, err := p.acct.Fact(a.OpID)
	switch {
	case err == nil:
		if known.ContentHash == a.ContentHash {
			consume("duplicate")
			res.Status = "duplicate"
			res.Receipt = p.receiptOf(a.OpID)
		} else {
			consume("op_id_conflict")
			res.Status = "op_id_conflict"
		}
		return res, true, false
	case !errors.Is(err, storage.ErrNotFound):
		p.fail(err)
		return res, false, true
	}
	if isNode, err := p.acct.IsNodeOrFact(a.OpID); err != nil {
		p.fail(err)
		return res, false, true
	} else if isNode {
		consume("op_id_conflict")
		res.Status = "op_id_conflict"
		return res, true, false
	}
	v := p.accept(a)
	if p.err != nil {
		return res, false, true
	}
	if v.status != "" {
		res.Status, res.Missing = v.status, v.missing
		return res, false, true
	}
	p.archive(a, v.receipt, v.admitted)
	consume("accepted")
	return p.accepted(res, a.OpID), true, false
}

// verdict is a fact's acceptance: a status that stops the push without
// consuming its slot (version_unsupported, dependency_missing), or the
// receipt to archive.
type verdict struct {
	status   string // "" when the fact is archived
	missing  []string
	receipt  Receipt // outcome, reason and normalized time; frontier when rejected
	admitted bool    // a causal node, applied by its handler (receipt.Index is its position)
}

// accept runs a fact through acceptance (PROTOCOL §6.4, from the envelope check on): it places the
// fact in its author chain (PROTOCOL §6.10), resolves its frontier, checks it, and when it is
// admitted makes it a causal node and applies its handler. It archives nothing and consumes no
// slot, so a live push (admit) and a rebuild's replay (PROTOCOL §6.11) share it; a replay keeps
// the archived receipt's normalized time and takes its decisions from the archive (p.original,
// p.oracle). On a storage or handler error it sets p.err.
func (p *pusher) accept(a *Archived) verdict {
	// Envelope rejection comes before anything that could defer.
	if a.Violation != "" {
		if author, seq, prev, ok := coordinates(a); ok {
			if _, err := p.place(a.OpID, author, seq, prev); err != nil {
				p.fail(err)
				return verdict{}
			}
		}
		return verdict{receipt: Receipt{Outcome: "rejected", Reason: a.Violation, NormalizedTime: p.receive, Frontier: []string{}}}
	}
	f := a.Fact
	handler := p.s.Registry.lookup(f.Type, f.TypeVersion)
	if handler == nil {
		return verdict{status: "version_unsupported"}
	}
	// Dependencies: causal nodes, or rejected facts standing for their frontier.
	frontier := map[string]bool{}
	var missing []string
	required := slices.Clone(f.Dependencies)
	if f.PreviousOpID != "" {
		required = append(required, f.PreviousOpID)
	}
	for i, d := range required {
		isDependency := i < len(f.Dependencies)
		if _, err := p.acct.NodeTime(d); err == nil {
			if isDependency {
				frontier[d] = true
			}
			continue
		} else if !errors.Is(err, storage.ErrNotFound) {
			p.fail(err)
			return verdict{}
		}
		rejected, err := p.acct.Fact(d)
		if errors.Is(err, storage.ErrNotFound) {
			missing = append(missing, d)
			continue
		}
		if err != nil {
			p.fail(err)
			return verdict{}
		}
		if isDependency {
			ids, err := decodeIDList(rejected.Frontier)
			if err != nil {
				p.fail(err)
				return verdict{}
			}
			for _, id := range ids {
				frontier[id] = true
			}
		}
	}
	// Prerequisites named in a payload count only once the payload is valid:
	// a malformed fact is rejected below rather than waited for forever.
	payloadValid := handler.Validate(f) == nil
	if av, ok := handler.(AccountValidator); ok && payloadValid {
		payloadValid = av.ValidateInAccount(AccountInfo{ID: p.accountID, Namespace: p.namespace}, f) == nil
	}
	if payloadValid && !p.follows() {
		if pr, ok := handler.(Prerequisiter); ok {
			for _, id := range pr.Prerequisites(f) {
				if _, err := p.acct.Secret(id); errors.Is(err, storage.ErrNotFound) {
					missing = append(missing, id)
				} else if err != nil {
					p.fail(err)
					return verdict{}
				}
			}
		}
		if np, ok := handler.(NodePrerequisiter); ok {
			for _, id := range np.NodePrerequisites(f) {
				if known, err := p.acct.IsNodeOrFact(id); err != nil {
					p.fail(err)
					return verdict{}
				} else if !known {
					missing = append(missing, id)
				}
			}
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return verdict{status: "dependency_missing", missing: slices.Compact(missing)}
	}
	edges := make([]string, 0, len(frontier))
	var times []int64
	for id := range frontier {
		edges = append(edges, id)
		t, err := p.acct.NodeTime(id)
		if err != nil {
			p.fail(err)
			return verdict{}
		}
		times = append(times, t)
	}
	sort.Strings(edges)
	normalized := contract.NormalizeTime(f.ClientTime, p.receive, times)
	if p.original != nil {
		normalized = p.original.NormalizedTime // never reclamped (PROTOCOL §6)
	}
	reject := func(reason string) verdict {
		return verdict{receipt: Receipt{Outcome: "rejected", Reason: reason, NormalizedTime: p.receive, Frontier: edges}}
	}
	// Every archived fact holds a chain position (PROTOCOL §6.10), rejected ones included.
	author, err := p.place(f.OpID, f.AuthorID, f.AuthorSeq, f.PreviousOpID)
	if err != nil {
		p.fail(err)
		return verdict{}
	}
	if p.follows() {
		// Recovery (PROTOCOL §6.11): the receipt proves the admission; the checks are
		// not run again. A payload this server cannot apply contributes
		// nothing.
		if !payloadValid {
			if err := p.acct.InsertNode(f.OpID, "fact", normalized, edges); err != nil {
				p.fail(err)
				return verdict{}
			}
			return verdict{receipt: Receipt{Outcome: p.original.Outcome, Reason: p.original.Reason, NormalizedTime: normalized,
				Index: p.b.nextIndex()}, admitted: true}
		}
		return p.apply(handler, f, normalized, edges, author)
	}
	if reason, err := p.authorHistory(f); err != nil {
		p.fail(err)
		return verdict{}
	} else if reason != "" {
		return reject(reason)
	}
	if c := handler.Capability(); c != "" && !p.declares(c) {
		return reject("capability_not_declared")
	}
	if err := handler.Validate(f); err != nil {
		return reject("invalid_payload")
	}
	if av, ok := handler.(AccountValidator); ok {
		if err := av.ValidateInAccount(AccountInfo{ID: p.accountID, Namespace: p.namespace}, f); err != nil {
			return reject("invalid_payload")
		}
	}
	if cn, ok := handler.(CapabilityNeeder); ok {
		for _, c := range cn.NeededCapabilities(f) {
			if !p.declares(c) {
				return reject("capability_not_declared")
			}
		}
	}
	if sv, ok := handler.(StateValidator); ok {
		if err := sv.ValidateState(p.acct, AccountInfo{ID: p.accountID, Namespace: p.namespace}, f); errors.Is(err, ErrInvalidPayload) {
			return reject("invalid_payload")
		} else if err != nil {
			p.fail(err)
			return verdict{}
		}
	}
	return p.apply(handler, f, normalized, edges, author)
}

// apply makes an admitted fact a causal node and applies its handler.
func (p *pusher) apply(handler Handler, f *Fact, normalized int64, edges []string, author string) verdict {
	actx := &ApplyContext{Account: p.acct, AccountID: p.accountID, Namespace: p.namespace, NormalizedTime: normalized, ReceiveTime: p.receive,
		Revision: p.rev, OpID: f.OpID, Capabilities: p.capabilities(), Author: author, commit: p.b, flush: p.flushDirty, oracle: p.oracle}
	if p.follows() {
		actx.Fixed = &Outcome{Result: p.original.Outcome, Reason: p.original.Reason}
	}
	// The fact is a causal node before its effects run, so decisions may depend on it.
	if err := p.acct.InsertNode(f.OpID, "fact", normalized, edges); err != nil {
		p.fail(err)
		return verdict{}
	}
	p.b.ids[f.OpID] = true
	index := p.b.nextIndex() // the fact precedes the decisions it makes
	actx.Index = index
	outcome, err := handler.Apply(actx, f)
	if err != nil {
		p.fail(err)
		return verdict{}
	}
	r := Receipt{Outcome: outcome.Result, Reason: outcome.Reason, NormalizedTime: normalized, Index: index}
	if len(outcome.Values) > 0 {
		r.SortValues = normalizeJSON(outcome.Values).([]any)
	}
	return verdict{receipt: r, admitted: true}
}

func (p *pusher) accepted(res PushResult, opID string) PushResult {
	res.Status = "accepted"
	res.Receipt = p.receiptOf(opID)
	return res
}

func (p *pusher) declares(capability string) bool {
	if p.original != nil {
		// A replay does not re-run the capability checks (PROTOCOL §6.11): the
		// installation's declaration may have changed since, and the receipt
		// records whether it declared what the fact needs.
		return p.original.Outcome != "rejected" || p.original.Reason != "capability_not_declared"
	}
	return slices.Contains(p.capabilities(), capability)
}

func (p *pusher) capabilities() []string {
	var caps []string
	if v, err := contract.DecodeJSON([]byte(p.inst.Capabilities)); err == nil {
		for _, c := range v.([]any) {
			caps = append(caps, c.(string))
		}
	}
	return caps
}

// authorHistory returns a rejection reason for an impossible author history: a
// previous fact of another author_id or with a sequence not below the fact's.
// A held position is no longer a rejection: the fact roots a derived branch
// (PROTOCOL §6.10, place).
func (p *pusher) authorHistory(f *Fact) (string, error) {
	if f.PreviousOpID == "" {
		return "", nil
	}
	prev, placed, err := p.acct.FactAuthorOf(f.PreviousOpID)
	if err != nil {
		return "", err
	}
	if placed {
		if prev.RawAuthor != f.AuthorID || prev.AuthorSeq >= f.AuthorSeq {
			return "impossible_author_history", nil
		}
		return "", nil
	}
	// An archived fact without well-formed coordinates is acceptable; otherwise it
	// is a decision, which is no previous fact.
	if _, err := p.acct.Fact(f.PreviousOpID); err == nil {
		return "", nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return "", err
	}
	return "invalid_previous_op_id", nil
}

// coordinates returns an archived fact's author coordinates when they are
// well-formed (PROTOCOL §6.10: rejected facts are placed too).
func coordinates(a *Archived) (author string, seq int64, prev string, ok bool) {
	if a.Fact != nil {
		return a.Fact.AuthorID, a.Fact.AuthorSeq, a.Fact.PreviousOpID, true
	}
	author, _ = a.Object["author_id"].(string)
	seq, isInt := integer(a.Object["author_seq"])
	if !contract.ValidUUID(author) || !isInt || seq < 1 {
		return "", 0, "", false
	}
	if s, isStr := a.Object["previous_op_id"].(string); isStr && contract.ValidUUID(s) {
		prev = s
	}
	return author, seq, prev, true
}

// place records a fact's chain position and returns its effective author (PROTOCOL §6.10):
// the author inherited through previous_op_id from a placed fact of the same
// author_id holds (author, seq) if it is free; otherwise the fact roots a
// derived branch whose parent is the inherited author.
func (p *pusher) place(opID, author string, seq int64, prev string) (string, error) {
	inherited := author
	if prev != "" {
		fa, ok, err := p.acct.FactAuthorOf(prev)
		if err != nil {
			return "", err
		}
		if ok && fa.RawAuthor == author {
			inherited = fa.EffectiveAuthor
		}
	}
	effective := inherited
	holder, err := p.acct.AuthorPosition(inherited, seq)
	if err != nil {
		return "", err
	}
	if holder != "" {
		effective = contract.DerivedAuthorID(p.namespace, opID)
		if err := p.acct.SetLineage(effective, inherited, opID, prev); err != nil {
			return "", err
		}
	}
	return effective, p.acct.PlaceFact(opID, storage.FactAuthor{RawAuthor: author, AuthorSeq: seq, EffectiveAuthor: effective})
}

// archive signs the receipt and stores the fact (an admitted fact's causal
// node and chain position were recorded by accept; a rejected one takes its
// index in the commit here).
func (p *pusher) archive(a *Archived, r Receipt, admitted bool) {
	r.OpID, r.ContentHash, r.Epoch, r.Seq = a.OpID, a.ContentHash, p.rev.Epoch, p.rev.Seq
	if !admitted {
		r.Index = p.b.nextIndex()
	}
	if err := r.Sign(p.b.key.ID, p.b.key.Secret, p.accountID); err != nil {
		p.fail(err)
		return
	}
	receipt, err := contract.Canonical(r.Object())
	if err != nil {
		p.fail(err)
		return
	}
	frontier := "[]"
	if r.Outcome == "rejected" {
		list := make([]any, len(r.Frontier))
		for i, id := range r.Frontier {
			list[i] = id
		}
		b, err := contract.Canonical(list)
		if err != nil {
			p.fail(err)
			return
		}
		frontier = string(b)
	}
	typ, version := "", int64(0)
	if v, ok := a.Object["type"].(string); ok {
		typ = v
	}
	if n, ok := integer(a.Object["type_version"]); ok {
		version = n
	}
	if err := p.acct.InsertFact(storage.FactRow{OpID: a.OpID, ContentHash: a.ContentHash, Fact: a.Canonical, Receipt: receipt,
		Type: typ, TypeVersion: version, Outcome: r.Outcome, Frontier: frontier, Epoch: p.rev.Epoch, Seq: p.rev.Seq, ReceiptIndex: r.Index}); err != nil {
		p.fail(err)
		return
	}
	p.b.accepted = append(p.b.accepted, map[string]any{"fact": a.Object, "receipt": r.Object()})
}

func decodeIDList(s string) ([]string, error) {
	v, err := contract.DecodeJSON([]byte(s))
	if err != nil {
		return nil, err
	}
	list, ok := v.([]any)
	if !ok {
		return nil, errors.New("protocol: frontier is not a list")
	}
	out := make([]string, len(list))
	for i, x := range list {
		out[i], _ = x.(string)
	}
	return out, nil
}

// DecodePart validates a wire part object.
func DecodePart(obj map[string]any) (*Part, error) {
	bad := protoErr(400, "invalid_request", "Malformed part.")
	for k := range obj {
		switch k {
		case "transaction_id", "part_index", "part_count", "content_hash", "data":
		default:
			return nil, bad
		}
	}
	tx, _ := obj["transaction_id"].(string)
	hash, _ := obj["content_hash"].(string)
	data, _ := obj["data"].(string)
	index, ok1 := integer(obj["part_index"])
	count, ok2 := integer(obj["part_count"])
	if !contract.ValidUUID(tx) || !ok1 || !ok2 || count < 1 || count > MaxPartCount || index < 0 || index >= count || len(hash) != 64 {
		return nil, bad
	}
	if _, err := hex.DecodeString(hash); err != nil || hex.EncodeToString(mustHex(hash)) != hash {
		return nil, bad
	}
	raw, err := decodeBase64URL(data)
	if err != nil || len(raw) == 0 {
		return nil, bad
	}
	if len(raw) > MaxPartBytes {
		return nil, protoErr(413, "payload_too_large", "A part is larger than the advertised bound; reframe with smaller parts.")
	}
	return &Part{TransactionID: tx, Index: int(index), Count: int(count), ContentHash: hash, Data: raw}, nil
}

func mustHex(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

func decodeBase64URL(s string) ([]byte, error) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return nil, errors.New("invalid base64url")
		}
	}
	return base64.RawURLEncoding.Strict().DecodeString(s)
}

// sealPart encrypts a staged part (PROTOCOL §7.2) when a key is configured.
func (p *pusher) sealPart(installSeq int64, index int, data []byte) []byte {
	if p.s.Box == nil {
		return data
	}
	return p.s.Box.Seal(data, partAD(p.accountID, p.inst.InstallID, installSeq, index))
}

func (p *pusher) openPart(installSeq int64, index int, data []byte) ([]byte, error) {
	if p.s.Box == nil {
		return data, nil
	}
	return p.s.Box.Open(data, partAD(p.accountID, p.inst.InstallID, installSeq, index))
}

func partAD(account, install string, installSeq int64, index int) []byte {
	return []byte(fmt.Sprintf("part\x00%s\x00%s\x00%d\x00%d", account, install, installSeq, index))
}

// StoreSecrets seals secrets made by the server itself (an import's feed
// credentials, PROTOCOL §11.5), as a push stores its secrets.
func (s *Service) StoreSecrets(acct *storage.AccountTx, accountID string, secrets []Secret) error {
	return s.storeSecrets(acct, accountID, secrets)
}

// preparedSecret is a request's secret canonicalized and sealed before the
// write transaction, which only stores it (500 secrets once held the writer for seconds).
type preparedSecret struct {
	id            string
	plain, sealed []byte
}

// prepareSecrets canonicalizes and seals a request's secrets.
func (s *Service) prepareSecrets(accountID string, secrets []Secret) ([]preparedSecret, error) {
	if len(secrets) == 0 {
		return nil, nil
	}
	if s.Box == nil {
		return nil, protoErr(400, "invalid_request", "This server cannot store credentials.")
	}
	out := make([]preparedSecret, 0, len(secrets))
	for _, sec := range secrets {
		value := map[string]any{"username": sec.Username, "password": sec.Password}
		if sec.ApprovedOrigins != nil {
			origins := slices.Clone(sec.ApprovedOrigins)
			slices.Sort(origins)
			list := []any{}
			for _, o := range slices.Compact(origins) {
				list = append(list, o)
			}
			value["approved_origins"] = list
		}
		plain, err := contract.Canonical(value)
		if err != nil {
			return nil, err
		}
		out = append(out, preparedSecret{id: sec.ID, plain: plain, sealed: s.Box.Seal(plain, secretAD(accountID, sec.ID))})
	}
	return out, nil
}

func secretAD(accountID, id string) []byte { return []byte("secret\x00" + accountID + "\x00" + id) }

// putSecrets stores prepared secrets, idempotently by ID; a different value
// under a known ID is refused.
func (s *Service) putSecrets(acct *storage.AccountTx, accountID string, prepared []preparedSecret) error {
	for _, p := range prepared {
		existing, err := acct.Secret(p.id)
		if err == nil {
			old, err := s.Box.Open(existing, secretAD(accountID, p.id))
			if err != nil {
				return err
			}
			if !bytes.Equal(old, p.plain) {
				return protoErr(400, "invalid_request", "A different secret already has this ID.")
			}
			continue
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if err := acct.PutSecret(p.id, p.sealed); err != nil {
			return err
		}
	}
	return nil
}

// storeSecrets prepares and stores secrets in one step (callers already in
// a write transaction for other reasons: an import's own secrets).
func (s *Service) storeSecrets(acct *storage.AccountTx, accountID string, secrets []Secret) error {
	prepared, err := s.prepareSecrets(accountID, secrets)
	if err != nil {
		return err
	}
	return s.putSecrets(acct, accountID, prepared)
}

// SecretValue opens a stored secret of the account (GET /v1/secrets/{id}).
func (s *Service) SecretValue(ctx context.Context, accountID, id string) (map[string]any, error) {
	return s.AuthorizedSecretValue(ctx, accountID, id, nil)
}

// ContainsCredentialURL reports whether any string in v is a URL carrying HTTP
// userinfo ("scheme://user[:password]@host…"). Such content must never be
// archived: facts are immutable, so credentials travel only as secrets.
func ContainsCredentialURL(v any) bool {
	switch x := v.(type) {
	case string:
		return credentialURL(x)
	case map[string]any:
		for k, e := range x {
			if credentialURL(k) || ContainsCredentialURL(e) {
				return true
			}
		}
	case []any:
		for _, e := range x {
			if ContainsCredentialURL(e) {
				return true
			}
		}
	}
	return false
}

// credentialURL reports a URL with userinfo in s. A string that is one URL
// (it starts with "scheme://" and holds no whitespace or control characters)
// has its authority up to the first "/", "?" or "#", whatever else it holds.
// In other text an embedded URL's authority also ends at whitespace, control
// characters, "<", ">" and the quotation mark that opened it, so a handle or
// markup after a URL is not mistaken for userinfo.
func credentialURL(s string) bool {
	oneString := !strings.ContainsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
	for i := 0; i+3 <= len(s); i++ {
		if s[i:i+3] != "://" {
			continue
		}
		j := i // a scheme must precede "://"
		for j > 0 && isSchemeByte(s[j-1]) {
			j--
		}
		if j == i {
			continue
		}
		oneURL := oneString && j == 0
		var quote rune
		if j > 0 && (s[j-1] == '"' || s[j-1] == '\'' || s[j-1] == '`') {
			quote = rune(s[j-1])
		}
		for _, r := range s[i+3:] {
			if r == '/' || r == '?' || r == '#' {
				break
			}
			if !oneURL && (unicode.IsSpace(r) || unicode.IsControl(r) || r == '<' || r == '>' || r == quote) {
				break
			}
			if r == '@' {
				return true
			}
		}
	}
	return false
}

func isSchemeByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '.' || c == '-'
}
