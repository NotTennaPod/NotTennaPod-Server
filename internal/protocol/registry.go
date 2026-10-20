package protocol

import (
	"encoding/json"
	"errors"
	"slices"
	"sort"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// ErrInvalidPayload marks a StateValidator rejection (other errors abort the push).
var ErrInvalidPayload = errors.New("protocol: invalid payload")

// Outcome is a handler's result for an admitted fact.
type Outcome struct {
	Result string // applied, superseded, not_applied
	Reason string // for not_applied
	// Values are the canonical sort values the receipt records (a queue
	// placement's or sort trigger's, PROTOCOL §7.6); nil for none.
	Values []any
}

// Handler implements one operation type and version.
type Handler interface {
	// Capability is the optional capability the operation belongs to ("" = core).
	Capability() string
	// Validate checks the payload schema; an error rejects the fact (reason
	// invalid_payload).
	Validate(f *Fact) error
	// Apply applies the fact's effects through ctx inside the push transaction.
	// An error aborts the whole push (nothing is consumed).
	Apply(ctx *ApplyContext, f *Fact) (Outcome, error)
}

// AccountValidator is an optional Handler extension: payload checks that need
// the account (identity derivations from the namespace).
type AccountValidator interface {
	ValidateInAccount(account AccountInfo, f *Fact) error
}

// AccountInfo identifies the account a fact is admitted into.
type AccountInfo struct {
	ID        string
	Namespace string
}

// CapabilityNeeder is an optional Handler extension: capabilities a particular
// fact needs beyond its type's (a play start carrying a queue bundle needs the
// queue capability); checked once the payload is valid.
type CapabilityNeeder interface {
	NeededCapabilities(f *Fact) []string
}

// StateValidator is an optional Handler extension: payload checks against the
// account's admitted state (a listening session's start, a restore's removal).
// An error rejects the fact with invalid_payload before it becomes a causal node.
type StateValidator interface {
	ValidateState(acct *storage.AccountTx, account AccountInfo, f *Fact) error
}

// Prerequisiter is an optional Handler extension: IDs (secrets) that must be
// held before the fact is admitted; missing ones answer dependency_missing.
type Prerequisiter interface {
	Prerequisites(f *Fact) []string
}

// NodePrerequisiter is an optional Handler extension: facts a fact names in its
// payload that must be archived before it is admitted (a restore's removal);
// missing ones answer dependency_missing like missing dependencies.
type NodePrerequisiter interface {
	NodePrerequisites(f *Fact) []string
}

type typeKey struct {
	Type    string
	Version int64
}

// Registry maps (type, type_version) to handlers, and decision types to
// their admit steps.
type Registry struct {
	handlers    map[typeKey]Handler
	admits      map[string]AdmitFunc
	recoveryEnd RecoveryEndFunc
	publisher   QueuePublisher
}

// QueuePublisher runs at the end of every commit, before it is appended
// (PROTOCOL §7.6: no commit publishes keys that differ from
// the replay's order): it derives the replay's order and, if the published
// keys do not give it and layout is set, records a queue.layout; it
// publishes the queue_position records. full derives every episode's (after
// a rebuild, PROTOCOL §6.11).
type QueuePublisher func(c *ApplyContext, full, layout bool) error

// RegisterQueuePublisher sets the queue's publication step.
func (r *Registry) RegisterQueuePublisher(fn QueuePublisher) { r.publisher = fn }

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: map[typeKey]Handler{}, admits: map[string]AdmitFunc{}}
}

// Register adds a handler.
func (r *Registry) Register(opType string, version int64, h Handler) {
	r.handlers[typeKey{opType, version}] = h
}

func (r *Registry) lookup(opType string, version int64) Handler {
	if r == nil {
		return nil
	}
	return r.handlers[typeKey{opType, version}]
}

// AdmitFunc is a decision type's admit step (PROTOCOL §6.11 "A decision", PROTOCOL §6 step
// 3): it applies an archived decision's recorded result to the derived
// state, as the code that made it applied it, with no new randomness and no
// new decision. The decision is a causal node with its recorded edges, and
// its archived row is restored, before it runs. d is the decision object as
// recorded.
type AdmitFunc func(c *ApplyContext, d map[string]any) error

// RegisterAdmit adds a decision type's admit step (version 1).
func (r *Registry) RegisterAdmit(decisionType string, fn AdmitFunc) {
	if r.admits == nil {
		r.admits = map[string]AdmitFunc{}
	}
	r.admits[decisionType] = fn
}

func (r *Registry) admit(decisionType string) AdmitFunc {
	if r == nil {
		return nil
	}
	return r.admits[decisionType]
}

// ApplyContext is a handler's view of the push transaction.
type ApplyContext struct {
	Account        *storage.AccountTx
	AccountID      string
	Namespace      string
	NormalizedTime int64
	ReceiveTime    int64
	Revision       storage.Revision
	OpID           string
	// Capabilities are the pushing installation's declared capabilities.
	Capabilities []string
	// Author is the fact's effective author (PROTOCOL §6.10): its author_id, or the
	// derived author of the divergent branch it belongs to.
	Author string
	// Resolved is set when an episode reference of this fact was resolved to
	// another episode (PROTOCOL §9.7): its effects in one scope then compose.
	Resolved bool
	// Index is the fact's position in its commit (its receipt's index):
	// (Revision, Index) is its receipt position.
	Index int64
	// Fixed is the outcome of the fact's receipt when a recovery rebuild
	// replays it (PROTOCOL §6.11): handlers honour it instead of re-running the
	// session and generation checks (a fact the receipt refused contributes
	// nothing). Nil in a live push and a strict rebuild.
	Fixed *Outcome

	commit   *commitBuilder
	batch    bool            // a server commit: Defer batches work until the next flush
	flush    func() error    // settles refolds deferred in this push
	scopes   []string        // scopes this fact contributed to (Settle recomputes them)
	scopeSet map[string]bool // the same, for constant-time deduplication
	oracle   *rebuilder      // in a rebuild's replay: Decide answers from the archive (PROTOCOL §6.11)
}

// Rebuilding reports a rebuild's replay (PROTOCOL §6.11): decisions come from the
// archive.
func (c *ApplyContext) Rebuilding() bool { return c.oracle != nil }

// RecoveryReplay reports a recovery rebuild's replay, which follows the
// receipts and the archive's effective decisions (PROTOCOL §6.11).
func (c *ApplyContext) RecoveryReplay() bool { return c.oracle != nil && c.oracle.recovery }

// RecordedDecision returns, in a rebuild's replay, the archive's effective
// decision of a type and subject (its smallest rank, PROTOCOL §6.11) without making
// it.
func (c *ApplyContext) RecordedDecision(decisionType, subject string) (map[string]any, bool) {
	if c.oracle == nil {
		return nil, false
	}
	list := c.oracle.bySubject[[2]string{decisionType, subject}]
	if len(list) == 0 {
		return nil, false
	}
	return list[0].obj, true
}

// Record publishes a changed projection record in this push's commit. A later
// record for the same key in the same push replaces it (provenance is merged).
func (c *ApplyContext) Record(r Record) {
	if !slices.Contains(r.Provenance, c.OpID) {
		r.Provenance = append(r.Provenance, c.OpID)
	}
	c.commit.record(r)
}

// DecisionInput describes a server decision a handler makes.
type DecisionInput struct {
	Type         string
	Version      int64
	Subject      string
	Dependencies []string // causal nodes the decision observed
	Inputs       map[string]any
	Result       map[string]any
}

// ErrDecisionDependency reports a decision dependency that is not a causal node.
var ErrDecisionDependency = errors.New("protocol: decision depends on an unknown node")

// Decide creates, signs and persists a decision now, so later operations in the
// same push see it (decisions are causal nodes as soon as they exist).
// In a rebuild's replay it makes no decision: the archive's effective
// decision of the same type and subject becomes a causal node again and is
// returned as recorded (PROTOCOL §6.11, the decision oracle); the caller applies the
// returned decision's result, not what it asked for. In a recovery rebuild a
// decision of another commit, or one whose dependencies are not derived yet,
// is admitted at its own position instead: Decide then returns nil, and the
// caller goes on without it.
func (c *ApplyContext) Decide(in DecisionInput) (map[string]any, error) {
	if c.oracle != nil {
		return c.oracle.decide(c, in)
	}
	deps := slices.Clone(in.Dependencies)
	sort.Strings(deps)
	deps = slices.Compact(deps)
	var times []int64
	for _, d := range deps {
		t, err := c.Account.NodeTime(d)
		if err != nil {
			return nil, ErrDecisionDependency
		}
		times = append(times, t)
	}
	id, err := c.commit.newNodeID(c.Account, in.Type)
	if err != nil {
		return nil, err
	}
	normalized := contract.NormalizeTime(c.ReceiveTime, c.ReceiveTime, times)
	if in.Type == "podcast.resolution" && len(times) > 0 {
		// PROTOCOL §6.1: a resolution takes its registration's normalized time
		// (its only dependency), not the time the server resolved it, so what a
		// device recorded while its registration waited keeps the time the person
		// acted (§6.9, §15.2). Stored decisions keep the times they were made with.
		normalized = slices.Max(times)
	}
	depList := make([]any, len(deps))
	for i, d := range deps {
		depList[i] = d
	}
	inputs, result := in.Inputs, in.Result
	if inputs == nil {
		inputs = map[string]any{}
	}
	if result == nil {
		result = map[string]any{}
	}
	obj := map[string]any{"decision_id": id, "decision_type": in.Type, "decision_version": in.Version,
		"dependencies": depList, "normalized_time": normalized, "epoch": c.Revision.Epoch, "seq": c.Revision.Seq, "index": c.commit.nextIndex(),
		"subject": in.Subject, "inputs": inputs, "result": result, "key_id": c.commit.key.ID}
	mac, err := contract.SignedMAC(c.commit.key.Secret, c.AccountID, obj)
	if err != nil {
		return nil, err
	}
	obj["mac"] = mac
	canonical, err := contract.Canonical(obj)
	if err != nil {
		return nil, err
	}
	if err := insertDecisionNode(c.Account, id, in.Type, normalized, deps); err != nil {
		return nil, err
	}
	if err := c.Account.InsertDecision(storage.DecisionRow{DecisionID: id, DecisionType: in.Type, Subject: in.Subject,
		Epoch: c.Revision.Epoch, Seq: c.Revision.Seq, NormalizedTime: normalized, Decision: canonical}); err != nil {
		return nil, err
	}
	c.commit.decisions = append(c.commit.decisions, obj)
	return obj, nil
}

// ---- the queue replay's bookkeeping (PROTOCOL §7.6)

// QueueStep records a node as a replay step (with the decision resolving it,
// if any) and notes that the replay changes from its place on.
func (c *ApplyContext) QueueStep(node, decision string) error {
	if err := c.Account.PutQueueStep(node, decision); err != nil {
		return err
	}
	c.QueueTouch(node)
	return nil
}

// QueueStepWrites records a fact as a replay step with its key writers: the indexes of its
// key-carrying members whose own checks passed (PROTOCOL §7.6, §6 selector; none for a refused or
// session-ineligible fact); and refused, the members a move leaves out: those whose own checks
// failed for another reason than a closed generation; and closed, the members admitted as of a
// closed generation (live, or as a recovered receipt decided), which a move only ghosts.
func (c *ApplyContext) QueueStepWrites(node string, writers, refused, closed []int) error {
	enc := func(list []int) (string, error) {
		b, err := json.Marshal(append([]int{}, list...))
		return string(b), err
	}
	w, err := enc(writers)
	if err != nil {
		return err
	}
	r, err := enc(refused)
	if err != nil {
		return err
	}
	cl, err := enc(closed)
	if err != nil {
		return err
	}
	if err := c.Account.PutQueueStepWrites(node, w, r, cl); err != nil {
		return err
	}
	c.QueueTouch(node)
	return nil
}

// QueueKeysChanged notes key writes the replay does not derive (a recovered
// layout's): the publication rule checks the published keys again.
func (c *ApplyContext) QueueKeysChanged() { c.commit.queueKeys = true }

// QueueKeysChangedSince reports QueueKeysChanged; take clears it.
func (c *ApplyContext) QueueKeysChangedSince(take bool) bool {
	k := c.commit.queueKeys
	if take {
		c.commit.queueKeys = false
	}
	return k
}

// QueueTouch notes nodes whose replay inputs changed in this commit (a new
// step, a membership effect that changed): the replay reruns from the
// earliest of them.
func (c *ApplyContext) QueueTouch(nodes ...string) {
	if c.commit.queueTouched == nil {
		c.commit.queueTouched = map[string]bool{}
	}
	for _, n := range nodes {
		c.commit.queueTouched[n] = true
	}
}

// QueueInvalidateAll notes a change of the identity or generation mapping:
// the replay reruns from the start (its steps are routed again).
func (c *ApplyContext) QueueInvalidateAll() { c.commit.queueAll = true }

// QueueChanges are the nodes QueueTouch noted since the replay last took
// them, and whether the whole replay is invalid; take empties them.
func (c *ApplyContext) QueueChanges(take bool) ([]string, bool) {
	nodes := make([]string, 0, len(c.commit.queueTouched))
	for n := range c.commit.queueTouched {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	all := c.commit.queueAll
	if take {
		c.commit.queueTouched, c.commit.queueAll = nil, false
	}
	return nodes, all
}

// QueueMemo is the queue publisher's state for this commit (opaque here).
func (c *ApplyContext) QueueMemo() *any { return &c.commit.queueState }

// AfterCommit runs fn with the account's head once the commit's transaction
// has committed (never when it rolls back).
func (c *ApplyContext) AfterCommit(fn func(storage.Head)) {
	c.commit.after = append(c.commit.after, fn)
}

// Publishing reports whether the commit's queue publication runs at its end
// (not inside a rebuild's replay, which publishes once at its end).
func (c *ApplyContext) Publishing() bool { return c.oracle == nil }

// CommitNodes are the causal nodes this commit has made so far (its admitted
// facts and its decisions), sorted.
func (c *ApplyContext) CommitNodes() []string {
	out := make([]string, 0, len(c.commit.ids))
	for id := range c.commit.ids {
		if _, err := c.Account.NodeTime(id); err == nil {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// CommitRecordIDs are the IDs of an entity's records of a field group this
// commit changed or tombstoned so far.
func (c *ApplyContext) CommitRecordIDs(entity, group string) []string {
	var out []string
	for k := range c.commit.records {
		if k[0] == entity && k[2] == group {
			out = append(out, k[1])
		}
	}
	sort.Strings(out)
	return out
}

// insertDecisionNode makes a decision a causal node; a registration's own
// resolution and alias stay off the causal frontier (InsertImplicitNode).
func insertDecisionNode(acct *storage.AccountTx, id, decisionType string, normalized int64, deps []string) error {
	if decisionType == "podcast.resolution" || decisionType == "podcast.alias" {
		return acct.InsertImplicitNode(id, "decision", normalized, deps)
	}
	return acct.InsertNode(id, "decision", normalized, deps)
}
