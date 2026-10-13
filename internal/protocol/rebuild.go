package protocol

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Account rebuild (PROTOCOL §6.11).
//
// Derived state is a function of the archive in receipt order (PROTOCOL §6.10), so an
// account can be cleared and derived again by replaying its archive: every
// fact goes through the acceptance code of a live push (pusher.accept) at its
// receipt's position, normalized time and revision, consuming no transport
// slot and archiving nothing, and every decision its handler asks for comes
// from the archive (the decision oracle). The rebuilt records are compared
// with the published ones and the differences published in one commit.
//
// Decisions no fact replay makes (a server fetch's, an import's, identity
// decisions) are admitted through their types' admit steps (Registry.
// RegisterAdmit), in their commits, in index order: each applies its
// recorded result. A rebuild that meets a decision of a type without an
// admit step changes nothing and reports it.

// RebuildReport describes one account rebuild.
type RebuildReport struct {
	Facts     int // archived facts replayed
	Decisions int // archived decisions
	Consumed  int // decisions made again by the replay of the fact that made them
	Admitted  int // decisions no fact made, admitted through their types' admit steps
	// Mismatches are replay results that differ from the archive: an outcome
	// other than the receipt's, a decision its fact's replay did not make or
	// asked for differently, a server choice without a recorded one.
	Mismatches []string
	// Unsupported are the archived decisions no fact replay made whose type
	// has no admit step.
	Unsupported []string
	// Changed are the records the rebuild derives differently from the
	// published ones (tombstones for those it no longer has), by key.
	Changed []Record
	// LostRows counts stored feed responses and refresh schedules whose
	// podcast the rebuild did not create again.
	LostRows  int
	Committed bool // a commit with the changed records was appended
	Duration  time.Duration
	// Recovery: the archive holds recovered entries, so the replay followed
	// the receipts (rebuildAccount's recovery mode).
	Recovery bool
	// Staged: the rebuild ran in a staging copy (PROTOCOL §6.11's fallback); Locked
	// is how long it held the server's write lock (all of Duration in place,
	// the publication when staged).
	Staged bool
	Locked time.Duration
}

// Errors from Rebuild; the account is unchanged and the report says why.
var (
	ErrRebuildUnsupported = errors.New("protocol: the archive holds decisions of a type without an admit step")
	ErrRebuildMismatch    = errors.New("protocol: replaying the archive does not reproduce its receipts and decisions")
)

// Rebuild derives the account again from its archive in one write
// transaction (PROTOCOL §6.11): it clears the derived state (storage.ClearDerived),
// replays every archived entry in receipt order (epoch, seq, index, id),
// refolds every scope, and publishes the records that differ from the
// published ones in one commit (none when nothing differs). A rebuild that
// meets a decision without an admit step (ErrRebuildUnsupported) or does not
// reproduce the archive (ErrRebuildMismatch) is rolled back.
func (s *Service) Rebuild(ctx context.Context, accountID string) (RebuildReport, error) {
	started := time.Now()
	var report RebuildReport
	if staged, locked, err := s.staged(ctx, accountID, nil, func(ss *Service) error {
		var err error
		report, err = ss.Rebuild(ctx, accountID)
		return err
	}); staged {
		report.Duration, report.Locked, report.Staged = time.Since(started), locked, true
		return report, err
	} else if err != nil {
		return report, err
	}
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return report, fmt.Errorf("protocol: no receipt key: %v", err)
	}
	var finished *commitBuilder
	var head storage.Head
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		report = RebuildReport{}
		finished = nil
		acct := tx.Account(accountID)
		if reserved, err := acct.ImportReserved(); err != nil {
			return err
		} else if reserved {
			return &Error{Status: 503, Code: "server_busy", Message: "This account's library is being imported.", RetryAfter: 30 * time.Second}
		}
		done := s.beginRebuild(accountID)
		defer done()
		// An archive recovery joined entries to is rebuilt as recovery does:
		// its receipts decide (PROTOCOL §6.11).
		recovered, err := acct.HasRecoveries()
		if err != nil {
			return err
		}
		report.Recovery = recovered
		// The queue's publication after the rebuild may record a layout
		// (PROTOCOL §7.6): a decision of the rebuild's commit.
		b := &commitBuilder{key: keys[0], ids: map[string]bool{}, random: s.random()}
		complete, err := s.rebuildAccount(acct, accountID, recovered, &report, b)
		if err != nil {
			return err
		}
		if recovered {
			// The receipts decided; what differs from them is reported.
			report.Mismatches = nil
		}
		switch {
		case len(report.Unsupported) > 0:
			return ErrRebuildUnsupported
		case !complete || len(report.Mismatches) > 0 || report.LostRows > 0:
			return ErrRebuildMismatch
		}
		// The publication rule: a layout if the rebuilt replay's order is not
		// what the published keys give (PROTOCOL §7.6: "after a rebuild,
		// the publication rule runs").
		namespace, err := acct.Namespace()
		if err != nil {
			return err
		}
		rev, err := acct.NextRevision()
		if err != nil {
			return err
		}
		if err := s.publishQueue(&ApplyContext{Account: acct, AccountID: accountID, Namespace: namespace, Revision: rev, commit: b}, false, true); err != nil {
			return err
		}
		if len(b.records) > 0 {
			merged := map[[3]string]Record{}
			for _, rec := range report.Changed {
				merged[rec.Key()] = rec
			}
			for k, rec := range b.records {
				merged[k] = rec
			}
			report.Changed = report.Changed[:0]
			for _, rec := range merged {
				report.Changed = append(report.Changed, rec)
			}
			sort.Slice(report.Changed, func(i, j int) bool {
				a, b := report.Changed[i].Key(), report.Changed[j].Key()
				return slices.Compare(a[:], b[:]) < 0
			})
		}
		if len(report.Changed) > 0 || len(b.decisions) > 0 {
			var body storage.CommitBody
			for _, d := range b.body().Decisions {
				body.Decisions = append(body.Decisions, d)
			}
			for _, rec := range report.Changed {
				body.Records = append(body.Records, rec.Object())
			}
			if _, _, err := acct.AppendCommit(body); err != nil {
				return err
			}
			report.Committed = true
		}
		if head, err = acct.Head(); err != nil {
			return err
		}
		finished = b
		return nil
	})
	if err == nil && finished != nil {
		finished.runAfter(head)
	}
	report.Duration = time.Since(started)
	report.Locked = report.Duration
	return report, err
}

// rebuildAccount clears the account's derived state and replays its archive
// inside the caller's write transaction, and sets report.Changed to the
// records that differ from the published ones. In recovery mode (PROTOCOL §6.11)
// the receipts decide: a replay never rejects a fact its receipt admitted,
// handlers honour the receipt's outcome, a decision the archive holds for a
// later commit is admitted at its own position, and only the effective
// decision of each (type, subject) applies; differences from the receipts
// are reported, not refused. It reports false when a strict replay stopped
// early.
//
// After the replay the queue's publication runs once (PROTOCOL §7.6):
// the published keys are the copied records'; a layout it records is a
// decision of b, the commit that publishes the rebuild.
func (s *Service) rebuildAccount(acct *storage.AccountTx, accountID string, recovery bool, report *RebuildReport, b *commitBuilder) (bool, error) {
	acct.Uninterruptible()
	namespace, err := acct.Namespace()
	if err != nil {
		return false, err
	}
	r := &rebuilder{s: s, acct: acct, accountID: accountID, namespace: namespace, report: report, recovery: recovery}
	if err := r.load(); err != nil {
		return false, err
	}
	if err := acct.ClearDerived(); err != nil {
		return false, err
	}
	complete, err := r.replay()
	if err != nil {
		return false, err
	}
	if complete || recovery {
		if err := r.refoldAll(); err != nil {
			return false, err
		}
		// The queue_position records: every scope's published key (from the
		// archived layouts), no new layout (the caller's commit records it).
		// The replay state carries over to the caller's commit.
		rev, err := acct.NextRevision()
		if err != nil {
			return false, err
		}
		scratch := &commitBuilder{ids: map[string]bool{}, random: r, queueAll: true}
		pc := &ApplyContext{Account: acct, AccountID: accountID, Namespace: namespace, Revision: rev, commit: scratch}
		if err := s.publishQueue(pc, true, false); err != nil {
			return false, err
		}
		b.queueTouched, b.queueAll, b.queueState = nil, false, scratch.queueState
	}
	lost, err := acct.RestoreSaved()
	if err != nil {
		return false, err
	}
	report.LostRows = lost
	changed, removed, err := acct.RecordChanges()
	if err != nil {
		return false, err
	}
	report.Changed = recordChanges(changed, removed)
	if err := acct.DropRebuildCopies(); err != nil {
		return false, err
	}
	return complete, nil
}

// beginRebuild marks a rebuild of the account as running until the returned
// function is called: recovery requests for the account answer server_busy
// meanwhile (PROTOCOL §6.11). Inside the account's write transaction (serialized) a
// staged rebuild of the account may already hold the mark; it stays.
func (s *Service) beginRebuild(accountID string) func() {
	key := rebuildKey{s.Store, accountID}
	if _, loaded := rebuilding.LoadOrStore(key, true); loaded {
		return func() {}
	}
	return func() { rebuilding.Delete(key) }
}

// claimRebuild marks a staged rebuild of the account as running, unless one
// already is.
func (s *Service) claimRebuild(accountID string) (func(), bool) {
	key := rebuildKey{s.Store, accountID}
	if _, loaded := rebuilding.LoadOrStore(key, true); loaded {
		return nil, false
	}
	return func() { rebuilding.Delete(key) }, true
}

// Rebuilding reports whether a rebuild of the account is running.
func (s *Service) Rebuilding(accountID string) bool {
	_, ok := rebuilding.Load(rebuildKey{s.Store, accountID})
	return ok
}

// rebuilding holds the accounts whose rebuild is running (PROTOCOL §6.11), by store,
// so every Service of one store (they are copied) sees the same marks.
var rebuilding sync.Map

type rebuildKey struct {
	store   *storage.Store
	account string
}

// recordChanges turns the rebuilt records that differ into commit records,
// and the published records the rebuild no longer has into tombstones.
func recordChanges(changed, removed []storage.RecordRow) []Record {
	var out []Record
	for _, r := range changed {
		value, _ := contract.DecodeJSON([]byte(r.Value))
		out = append(out, Record{Entity: r.Entity, ID: r.EntityID, FieldGroup: r.FieldGroup, Value: value, Provenance: provenanceList(r.Provenance)})
	}
	for _, r := range removed {
		out = append(out, Record{Entity: r.Entity, ID: r.EntityID, FieldGroup: r.FieldGroup, Tombstone: true, Provenance: []string{}})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Key(), out[j].Key()
		return slices.Compare(a[:], b[:]) < 0
	})
	return out
}

func provenanceList(s string) []string {
	v, _ := contract.DecodeJSON([]byte(s))
	list, _ := v.([]any)
	out := []string{}
	for _, x := range list {
		if id, ok := x.(string); ok {
			out = append(out, id)
		}
	}
	return out
}

// rebuilder replays one account's archive.
type rebuilder struct {
	s         *Service
	acct      *storage.AccountTx
	accountID string
	namespace string
	report    *RebuildReport

	entries   []*archiveEntry
	bySubject map[[2]string][]*archivedDecision // competitors by rank (epoch, seq, decision_id)
	installs  map[string]string                 // op_id → the installation whose slot accepted it
	insts     map[string]storage.Installation
	last      storage.Revision

	caused    []*archivedDecision // the decisions the fact being replayed made, in creation order
	pos       int                 // position IDs the current decision has served
	admitting *archivedDecision   // the decision whose admit step runs (it makes no decision)

	// Recovery mode (rebuildAccount): the receipts decide.
	recovery bool
	p        *pusher          // the commit being replayed (entries may run out of order)
	current  *archiveEntry    // the fact being replayed
	provNode map[string]int64 // facts whose original rejected receipt replaced a provisional admission: nodes without effects, at their provisional time
}

// archiveEntry is one archived fact or decision at its receipt position.
type archiveEntry struct {
	epoch, seq, index int64
	id                string
	decision          *archivedDecision // nil for a fact
	caused            []*archivedDecision
	row               *storage.FactRow // a fact's archived row, once read
	done              bool
}

func (e *archiveEntry) commit() storage.Revision { return storage.Revision{Epoch: e.epoch, Seq: e.seq} }

// archivedDecision is an archived decision as recorded.
type archivedDecision struct {
	row       storage.DecisionRow
	obj       map[string]any
	deps      []string
	owner     string // the fact whose acceptance made it ("" in a server commit)
	consumed  bool
	effective bool // the smallest rank of its (type, subject) (PROTOCOL §6.11)
}

func (d *archivedDecision) String() string {
	return d.row.DecisionID + " (" + d.row.DecisionType + " " + d.row.Subject + ")"
}

// load reads the archive's positions and decisions before clearing.
func (r *rebuilder) load() error {
	positions, err := r.acct.FactPositions()
	if err != nil {
		return err
	}
	decisions, err := r.acct.Decisions()
	if err != nil {
		return err
	}
	if r.installs, err = r.acct.AcceptingInstallations(); err != nil {
		return err
	}
	insts, err := r.acct.Installations()
	if err != nil {
		return err
	}
	r.insts = map[string]storage.Installation{}
	for _, i := range insts {
		r.insts[i.InstallID] = i
	}
	for _, p := range positions {
		r.entries = append(r.entries, &archiveEntry{epoch: p.Epoch, seq: p.Seq, index: p.Index, id: p.OpID})
	}
	r.bySubject = map[[2]string][]*archivedDecision{}
	for _, row := range decisions {
		v, err := contract.DecodeJSON(row.Decision)
		if err != nil {
			return fmt.Errorf("protocol: rebuild: decision %s: %w", row.DecisionID, err)
		}
		obj, _ := v.(map[string]any)
		index, ok := integer(obj["index"])
		if !ok {
			index = -1
		}
		d := &archivedDecision{row: row, obj: obj}
		deps, _ := obj["dependencies"].([]any)
		for _, x := range deps {
			if id, ok := x.(string); ok {
				d.deps = append(d.deps, id)
			}
		}
		r.entries = append(r.entries, &archiveEntry{epoch: row.Epoch, seq: row.Seq, index: index, id: row.DecisionID, decision: d})
		key := [2]string{row.DecisionType, row.Subject}
		r.bySubject[key] = append(r.bySubject[key], d)
	}
	sort.Slice(r.entries, func(i, j int) bool {
		a, b := r.entries[i], r.entries[j]
		if a.epoch != b.epoch {
			return a.epoch < b.epoch
		}
		if a.seq != b.seq {
			return a.seq < b.seq
		}
		if a.index != b.index {
			return a.index < b.index
		}
		return a.id < b.id
	})
	for key, list := range r.bySubject {
		sort.Slice(list, func(i, j int) bool {
			a, b := list[i].row, list[j].row
			if a.Epoch != b.Epoch {
				return a.Epoch < b.Epoch
			}
			if a.Seq != b.Seq {
				return a.Seq < b.Seq
			}
			return a.DecisionID < b.DecisionID
		})
		list[0].effective = true
		if key[0] == queueLayout {
			// Layouts are materializations, never competitors (PROTOCOL §7.6, §6): every one is admitted, as
			// evidence only.
			for _, d := range list {
				d.effective = true
			}
		}
	}
	if r.recovery {
		// A fact whose original receipt is rejected but whose superseded
		// provisional receipt admitted it stays a node, without effects (PROTOCOL §6.11).
		var err error
		if r.provNode, err = r.acct.ProvisionallyAdmitted(); err != nil {
			return err
		}
	}
	// A fact precedes the decisions its acceptance makes in its commit (PROTOCOL §6.10: receipt and
	// decision indexes are admission order).
	var owner *archiveEntry
	for _, e := range r.entries {
		if owner != nil && owner.commit() != e.commit() {
			owner = nil
		}
		switch {
		case e.decision == nil:
			owner = e
			r.report.Facts++
		case owner != nil:
			e.decision.owner = owner.id
			owner.caused = append(owner.caused, e.decision)
			r.report.Decisions++
		default:
			r.report.Decisions++
		}
	}
	return nil
}

// mismatch notes a replay result that differs from the archive.
func (r *rebuilder) mismatch(format string, args ...any) {
	r.report.Mismatches = append(r.report.Mismatches, fmt.Sprintf(format, args...))
}

// replay replays every entry in receipt order, one commit at a time: a
// commit's deferred refolds settle at its end, as its push settled them. It
// reports false when it stopped early (a decision the archive does not hold).
//
// In recovery mode an entry whose causal past is not yet derived (a
// dependency that is not a node, which a provisional receipt or a replaced
// one can cause) waits, and runs as soon as it is ready (the reference's waiting list).
func (r *rebuilder) replay() (bool, error) {
	var waiting []*archiveEntry
	for _, e := range r.entries {
		if r.recovery {
			ready, err := r.ready(e)
			if err != nil {
				return false, err
			}
			if !ready {
				waiting = append(waiting, e)
				continue
			}
		}
		if complete, err := r.run(e); err != nil || !complete {
			return false, err
		}
		for progress := true; progress && len(waiting) > 0; {
			progress = false
			for i, w := range waiting {
				ready, err := r.ready(w)
				if err != nil {
					return false, err
				}
				if !ready {
					continue
				}
				waiting = append(waiting[:i:i], waiting[i+1:]...)
				if _, err := r.run(w); err != nil {
					return false, err
				}
				progress = true
				break
			}
		}
	}
	// Facts whose causal past never completes are joined anyway (a
	// registration naming a podcast the archive does not create contributes
	// nothing); a decision waits until what it depends on is derived.
	for len(waiting) > 0 {
		next := -1
		for i, w := range waiting {
			if w.decision == nil {
				next = i
				break
			}
		}
		if next < 0 {
			break
		}
		w := waiting[next]
		waiting = append(waiting[:next:next], waiting[next+1:]...)
		r.mismatch("fact %s: its causal past never became complete", w.id)
		if _, err := r.run(w); err != nil {
			return false, err
		}
		for progress := true; progress && len(waiting) > 0; {
			progress = false
			for i, x := range waiting {
				ready, err := r.ready(x)
				if err != nil {
					return false, err
				}
				if !ready {
					continue
				}
				waiting = append(waiting[:i:i], waiting[i+1:]...)
				if _, err := r.run(x); err != nil {
					return false, err
				}
				progress = true
				break
			}
		}
	}
	for _, w := range waiting {
		if !w.decision.consumed {
			// Its dependencies were causal nodes when it was archived, and the
			// archive's nodes are all derived again: it cannot stay pending.
			return false, fmt.Errorf("protocol: rebuild: decision %s never became ready", w.decision)
		}
	}
	if r.p != nil {
		if err := r.p.flushDirty(); err != nil {
			return false, err
		}
	}
	return true, nil
}

// run replays one entry in its commit's pusher (a new commit settles the
// previous one's deferred refolds first).
func (r *rebuilder) run(e *archiveEntry) (bool, error) {
	if e.done {
		return true, nil
	}
	e.done = true
	rev := e.commit()
	if r.p == nil || r.p.rev != rev {
		if r.p != nil {
			if err := r.p.flushDirty(); err != nil {
				return false, err
			}
		}
		r.p = &pusher{s: r.s, acct: r.acct, accountID: r.accountID, namespace: r.namespace, rev: rev,
			b: &commitBuilder{ids: map[string]bool{}, random: r}, oracle: r, recovery: r.recovery}
	}
	if rev.Epoch > r.last.Epoch || (rev.Epoch == r.last.Epoch && rev.Seq > r.last.Seq) {
		r.last = rev
	}
	if e.decision != nil {
		return true, r.unmade(r.p, e.decision)
	}
	return r.fact(r.p, e)
}

// ready reports whether an entry's causal past is derived (recovery mode): a
// decision's dependencies are nodes; a fact's dependencies are nodes or
// archived rejected facts whose frontiers are nodes; a registration whose
// effective resolution joins or opens finds its podcast and generation.
func (r *rebuilder) ready(e *archiveEntry) (bool, error) {
	if e.decision != nil {
		if e.decision.consumed {
			return true, nil
		}
		return r.nodes(e.decision.deps)
	}
	row, err := r.factRow(e)
	if err != nil {
		return false, err
	}
	if row.Outcome == "rejected" {
		if _, prov := r.provNode[e.id]; !prov {
			return true, nil
		}
	}
	v, err := contract.DecodeJSON(row.Fact)
	if err != nil {
		return true, nil
	}
	obj, _ := v.(map[string]any)
	deps, _ := obj["dependencies"].([]any)
	for _, x := range deps {
		d, _ := x.(string)
		if _, err := r.acct.NodeTime(d); err == nil {
			continue
		} else if !errors.Is(err, storage.ErrNotFound) {
			return false, err
		}
		dep, err := r.acct.Fact(d)
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		if dep.Outcome != "rejected" {
			return false, nil // an admitted fact not derived yet
		}
		ids, err := decodeIDList(dep.Frontier)
		if err != nil {
			return false, err
		}
		if ok, err := r.nodes(ids); err != nil || !ok {
			return false, err
		}
	}
	if typ, _ := obj["type"].(string); typ == "podcast.subscribe" || typ == "podcast.register_preview" {
		res, ok := r.recordedResult("podcast.resolution", e.id)
		if ok {
			mode, _ := res["mode"].(string)
			podcast, _ := res["podcast_id"].(string)
			if mode != "created" {
				exists, err := r.acct.PodcastExists(podcast)
				if err != nil || !exists {
					return false, err
				}
			}
			if g, _ := res["generation_id"].(string); mode == "joined" && g != "" {
				if _, err := r.acct.Generation(g); errors.Is(err, storage.ErrNotFound) {
					return false, nil
				} else if err != nil {
					return false, err
				}
			}
		}
	}
	return true, nil
}

func (r *rebuilder) nodes(ids []string) (bool, error) {
	for _, id := range ids {
		if _, err := r.acct.NodeTime(id); errors.Is(err, storage.ErrNotFound) {
			return false, nil
		} else if err != nil {
			return false, err
		}
	}
	return true, nil
}

// factRow reads (once) an entry's archived fact row.
func (r *rebuilder) factRow(e *archiveEntry) (*storage.FactRow, error) {
	if e.row == nil {
		row, err := r.acct.Fact(e.id)
		if err != nil {
			return nil, err
		}
		e.row = &row
	}
	return e.row, nil
}

// recordedResult is the result of the archive's effective decision of a
// type and subject.
func (r *rebuilder) recordedResult(typ, subject string) (map[string]any, bool) {
	list := r.bySubject[[2]string{typ, subject}]
	if len(list) == 0 {
		return nil, false
	}
	res, _ := list[0].obj["result"].(map[string]any)
	return res, true
}

// fact replays one archived fact through acceptance and compares the result
// with its receipt.
func (r *rebuilder) fact(p *pusher, e *archiveEntry) (bool, error) {
	row, err := r.factRow(e)
	if err != nil {
		return false, err
	}
	v, err := contract.DecodeJSON(row.Fact)
	if err != nil {
		return false, fmt.Errorf("protocol: rebuild: fact %s: %w", e.id, err)
	}
	a, err := ParseFact(v)
	if err != nil {
		return false, fmt.Errorf("protocol: rebuild: fact %s: %w", e.id, err)
	}
	original, err := parseReceipt(row.Receipt)
	if err != nil {
		return false, fmt.Errorf("protocol: rebuild: receipt of %s: %w", e.id, err)
	}
	p.inst = r.installation(e.id)
	p.original, p.receive = &original, original.NormalizedTime
	p.b.index = original.Index
	if r.recovery && original.Outcome == "rejected" {
		if t, prov := r.provNode[e.id]; prov {
			// PROTOCOL §6.11: an original rejected receipt over a provisional
			// admission with descendants: a node without effects.
			return true, p.nodeOnly(a, t)
		}
		// Archived as rejected: it holds its chain position (PROTOCOL §6.10).
		if author, seq, prev, ok := coordinates(a); ok {
			if _, err := p.place(a.OpID, author, seq, prev); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	r.caused, r.pos, r.current = e.caused, 0, e
	got := p.accept(a)
	r.caused, r.current = nil, nil
	if p.err != nil {
		var miss *oracleMiss
		if errors.As(p.err, &miss) {
			r.mismatch("%s", miss.Error())
			return false, nil
		}
		return false, fmt.Errorf("protocol: rebuild: replaying %s: %w", e.id, p.err)
	}
	typ, _ := a.Object["type"].(string)
	want := original.Outcome
	if original.Reason != "" {
		want += "/" + original.Reason
	}
	switch {
	case got.status != "":
		r.mismatch("fact %s (%s): receipt %s, replay answers %s %v", e.id, typ, want, got.status, got.missing)
	case got.receipt.Outcome != original.Outcome || got.receipt.Reason != original.Reason:
		have := got.receipt.Outcome
		if got.receipt.Reason != "" {
			have += "/" + got.receipt.Reason
		}
		r.mismatch("fact %s (%s): receipt %s, replay %s", e.id, typ, want, have)
	case original.Outcome == "rejected" && !slices.Equal(got.receipt.Frontier, original.Frontier):
		r.mismatch("fact %s (%s): rejected with frontier %v, replay resolves %v", e.id, typ, original.Frontier, got.receipt.Frontier)
	}
	return true, nil
}

// parseReceipt reads the fields of an archived receipt a replay uses.
func parseReceipt(b []byte) (Receipt, error) {
	v, err := contract.DecodeJSON(b)
	if err != nil {
		return Receipt{}, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return Receipt{}, errors.New("not an object")
	}
	var r Receipt
	r.OpID, _ = m["op_id"].(string)
	r.Outcome, _ = m["outcome"].(string)
	r.Reason, _ = m["reason"].(string)
	r.NormalizedTime, _ = integer(m["normalized_time"])
	r.Epoch, _ = integer(m["epoch"])
	r.Seq, _ = integer(m["seq"])
	if r.Index, ok = integer(m["index"]); !ok {
		r.Index = -1
	}
	if list, ok := m["frontier"].([]any); ok {
		r.Frontier = []string{}
		for _, x := range list {
			if id, ok := x.(string); ok {
				r.Frontier = append(r.Frontier, id)
			}
		}
	}
	return r, nil
}

// installation is the installation whose push accepted a fact: its declared
// capabilities are the ones a live acceptance checked (a rebuild checks them as live, against the
// installation's current declaration).
func (r *rebuilder) installation(op string) *storage.Installation {
	if id, ok := r.installs[op]; ok {
		if inst, ok := r.insts[id]; ok {
			return &inst
		}
	}
	return &storage.Installation{Capabilities: "[]"}
}

// unmade handles an archived decision the walk reaches: made again by its fact's replay, or
// not. One no fact made (a server commit's: a fetch's, an import's, an identity decision) is
// admitted through its type's admit step, in its commit, in index order. A decision of a fact
// whose replay did not make it, or of a type without an admit step, cannot be replayed; it
// still becomes a causal node with its recorded edges (when they are nodes), so the rest of
// the archive replays and the report lists everything.
func (r *rebuilder) unmade(p *pusher, d *archivedDecision) error {
	if d.consumed {
		return nil
	}
	admit := r.s.Registry.admit(d.row.DecisionType)
	switch {
	case r.recovery && !d.effective && d.row.DecisionType != "podcast.resolution":
		// A later competitor contributes nothing (PROTOCOL §6.11); a later
		// resolution still creates what it created (its admit step).
		admit = nil
	case r.recovery && admit == nil:
		r.report.Unsupported = append(r.report.Unsupported, d.row.DecisionType+" "+d.row.DecisionID)
	case r.recovery:
	case d.row.DecisionType == queueLayout:
		// A layout follows the commit's facts, made by the commit's queue
		// publication, not by a fact (PROTOCOL §7.6): admitted as
		// evidence whatever its place in the commit.
	case d.owner != "":
		r.mismatch("decision %s of fact %s: its replay did not make it", d, d.owner)
		admit = nil
	case admit == nil:
		r.report.Unsupported = append(r.report.Unsupported, d.row.DecisionType+" "+d.row.DecisionID)
	}
	d.consumed = true
	for _, dep := range d.deps {
		if _, err := r.acct.NodeTime(dep); errors.Is(err, storage.ErrNotFound) {
			if admit != nil {
				r.mismatch("decision %s: its recorded dependency %s is not a node", d, dep)
			}
			return nil
		} else if err != nil {
			return err
		}
	}
	if err := insertDecisionNode(r.acct, d.row.DecisionID, d.row.DecisionType, d.row.NormalizedTime, d.deps); err != nil {
		return err
	}
	if err := r.acct.RestoreDecision(d.row.DecisionID); err != nil {
		return err
	}
	if admit == nil {
		return nil
	}
	r.report.Admitted++
	c := &ApplyContext{Account: r.acct, AccountID: r.accountID, Namespace: r.namespace, NormalizedTime: d.row.NormalizedTime,
		ReceiveTime: d.row.NormalizedTime, Revision: p.rev, commit: p.b, flush: p.flushDirty, oracle: r, batch: d.owner == ""}
	if r.recovery {
		// A recovery's archive may lack what a decision names (a podcast its
		// registration's resolution has not created yet): such a decision
		// applies nothing now, atomically, and the next rebuild applies it
		// once the archive holds the rest.
		if err := r.acct.Savepoint("admit"); err != nil {
			return err
		}
	}
	r.admitting = d
	err := admit(c, d.obj)
	r.admitting = nil
	if r.recovery {
		if errors.Is(err, storage.ErrNotFound) {
			r.mismatch("decision %s: what it names is not derived (%v); it applies nothing", d, err)
			return r.acct.RollbackTo("admit")
		}
		if rerr := r.acct.Release("admit"); rerr != nil && err == nil {
			err = rerr
		}
	}
	var miss *oracleMiss
	if errors.As(err, &miss) {
		r.mismatch("%s", miss.Error())
		return nil
	}
	if err != nil {
		return fmt.Errorf("protocol: rebuild: admitting %s: %w", d, err)
	}
	return nil
}

// refoldAll refolds every scope once the archive is replayed, so every record
// is derived from the final contributions.
func (r *rebuilder) refoldAll() error {
	scopes, err := r.acct.ScopesWithPrefix("")
	if err != nil {
		return err
	}
	c := ApplyContext{Account: r.acct, AccountID: r.accountID, Namespace: r.namespace, Revision: r.last,
		commit: &commitBuilder{ids: map[string]bool{}, random: r}}
	for _, scope := range scopes {
		if _, err := c.recompute(scope); err != nil {
			return err
		}
	}
	return nil
}

// ---- the decision oracle

// oracleMiss reports a decision a replayed handler asks for that the archive
// cannot supply; the replay stops there.
type oracleMiss struct {
	op, typ, subject, why string
}

func (e *oracleMiss) Error() string {
	return fmt.Sprintf("fact %s asks for a %s decision on %q: %s", e.op, e.typ, e.subject, e.why)
}

// decide answers a replayed handler's Decide with the archive's effective
// decision of the same type and subject not yet made again (the smallest rank
// (epoch, seq, decision_id), PROTOCOL §6.11): it becomes a causal node again with
// its recorded edges and normalized time, its archived row is restored whole, and
// it is returned as recorded. The handler's request is compared with the recorded
// decision.
func (r *rebuilder) decide(c *ApplyContext, in DecisionInput) (map[string]any, error) {
	if r.admitting != nil {
		return nil, &oracleMiss{op: r.admitting.row.DecisionID, typ: in.Type, subject: in.Subject, why: "an admit step makes no decision"}
	}
	if r.recovery {
		// Only the effective decision, made in the fact's own commit, is made
		// again here; anything else is admitted at its own position (or not at
		// all: a later competitor contributes nothing).
		list := r.bySubject[[2]string{in.Type, in.Subject}]
		if len(list) == 0 {
			return nil, nil
		}
		d := list[0]
		// It depends on the fact (every decision a fact's acceptance makes
		// does), and it is of the fact's commit, except a registration's
		// resolution and alias, which apply whenever they were made (a
		// provisional one at the end of a recovery request, PROTOCOL §6.11).
		subscription := in.Type == "podcast.resolution" || in.Type == "podcast.alias"
		if d.consumed || !slices.Contains(d.deps, c.OpID) ||
			(!subscription && (d.row.Epoch != c.Revision.Epoch || d.row.Seq != c.Revision.Seq)) {
			return nil, nil
		}
		if ok, err := r.nodes(d.deps); err != nil || !ok {
			return nil, err
		}
		return r.make(c, d)
	}
	var d *archivedDecision
	for _, x := range r.bySubject[[2]string{in.Type, in.Subject}] {
		if !x.consumed {
			d = x
			break
		}
	}
	if d == nil {
		return nil, &oracleMiss{op: c.OpID, typ: in.Type, subject: in.Subject, why: "the archive holds none"}
	}
	if next := r.next(); next != d {
		made := "none"
		if next != nil {
			made = next.String()
		}
		r.mismatch("fact %s: decision %s made where the fact's acceptance made %s", c.OpID, d, made)
	}
	if diff := d.differs(in); diff != "" {
		r.mismatch("fact %s: decision %s: the replay asks for a different %s", c.OpID, d, diff)
	}
	for _, dep := range d.deps {
		if _, err := c.Account.NodeTime(dep); errors.Is(err, storage.ErrNotFound) {
			return nil, &oracleMiss{op: c.OpID, typ: in.Type, subject: in.Subject, why: "its recorded dependency " + dep + " is not a node"}
		} else if err != nil {
			return nil, err
		}
	}
	return r.make(c, d)
}

// make makes an archived decision again for the fact being replayed: a
// causal node with its recorded edges and normalized time, its archived row
// restored whole.
func (r *rebuilder) make(c *ApplyContext, d *archivedDecision) (map[string]any, error) {
	if err := insertDecisionNode(c.Account, d.row.DecisionID, d.row.DecisionType, d.row.NormalizedTime, d.deps); err != nil {
		return nil, err
	}
	if err := c.Account.RestoreDecision(d.row.DecisionID); err != nil {
		return nil, err
	}
	d.consumed, r.pos = true, 0
	r.report.Consumed++
	c.commit.nextIndex()
	return d.obj, nil
}

// differs names what a replayed request differs in from the recorded
// decision ("" when it asks for exactly what was recorded).
func (d *archivedDecision) differs(in DecisionInput) string {
	deps := slices.Clone(in.Dependencies)
	sort.Strings(deps)
	deps = slices.Compact(deps)
	if !slices.Equal(deps, d.deps) && !(len(deps) == 0 && len(d.deps) == 0) {
		return fmt.Sprintf("dependencies (%v, recorded %v)", deps, d.deps)
	}
	if v, _ := integer(d.obj["decision_version"]); v != in.Version {
		return "version"
	}
	for _, part := range []struct {
		name  string
		asked map[string]any
	}{{"inputs", in.Inputs}, {"result", in.Result}} {
		asked := part.asked
		if asked == nil {
			asked = map[string]any{}
		}
		a, err := contract.Canonical(normalizeJSON(asked))
		if err != nil {
			return part.name + " (" + err.Error() + ")"
		}
		b, err := contract.Canonical(d.obj[part.name])
		if err != nil || string(a) != string(b) {
			return fmt.Sprintf("%s (%s, recorded %s)", part.name, a, b)
		}
	}
	return ""
}

// normalizeJSON gives a handler's value the shapes contract.Canonical
// accepts, as Decide's signing does.
func normalizeJSON(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	out, err := contract.DecodeJSON(b)
	if err != nil {
		return v
	}
	return out
}

// next is the fact's first decision not yet made again: the one its handler
// is choosing for (docs/CONTRACT.md "Server decisions": a decision's choices
// precede it, in creation order).
func (r *rebuilder) next() *archivedDecision {
	for _, d := range r.caused {
		if !d.consumed {
			return d
		}
	}
	return nil
}

// ---- the server's recorded choices (docs/CONTRACT.md "Server decisions")
//
// A replayed handler draws its random choices before it asks for the decision
// they belong to. The rebuilder serves them from that decision's recorded
// result: a resolved queue placement's position ID and random draw, a sort
// result's fresh position IDs, an allocated release's occurrence ID.

// DecisionID serves an allocated release's ID ("e0:" + the choice).
func (r *rebuilder) DecisionID(kind string) string {
	d := r.next()
	if kind == "occurrence" && d != nil && d.row.DecisionType == "episode.occurrence_mapping" {
		res, _ := d.obj["result"].(map[string]any)
		if e, _ := res["episode_id"].(string); strings.HasPrefix(e, "e0:") {
			return strings.TrimPrefix(e, "e0:")
		}
	}
	if !r.recovery {
		r.mismatch("a %s ID drawn for %s without a recorded choice", kind, describe(d))
	}
	return randomUUID()
}

// PositionID serves the current decision's next fresh position ID.
func (r *rebuilder) PositionID() [16]byte {
	d := r.next()
	ids := freshPositionIDs(d)
	if r.pos < len(ids) {
		s := ids[r.pos]
		r.pos++
		if b, err := hex.DecodeString(strings.ReplaceAll(s, "-", "")); err == nil && len(b) == 16 {
			var u [16]byte
			copy(u[:], b)
			return u
		}
	}
	if !r.recovery {
		r.mismatch("a position ID drawn for %s without a recorded choice", describe(d))
	}
	return cryptoRandom{}.PositionID()
}

// Index serves a random enqueue location's draw. A replayed handler takes a
// server placement's recorded decision whole (its place and key), so it
// never draws; a draw asked for anyway has no recorded choice.
func (r *rebuilder) Index(n int) int {
	if !r.recovery {
		r.mismatch("a random enqueue draw for %s without a recorded choice", describe(r.next()))
	}
	return 0
}

func describe(d *archivedDecision) string {
	if d == nil {
		return "no recorded decision"
	}
	return d.String()
}

// queueLayout is the queue publication's decision type (PROTOCOL §7.6).
const queueLayout = "queue.layout"

// freshPositionIDs are the position IDs a decision's making drew, in order: a
// resolved placement's (its inputs name the enqueue location it resolved).
func freshPositionIDs(d *archivedDecision) []string {
	if d == nil {
		return nil
	}
	inputs, _ := d.obj["inputs"].(map[string]any)
	result, _ := d.obj["result"].(map[string]any)
	if _, resolved := inputs["enqueue_location"]; resolved {
		if p, ok := result["position_id"].(string); ok {
			return []string{p}
		}
	}
	return nil
}
