package ops

// Listening, history and statistics operations of PROTOCOL §8.

import (
	"errors"
	"slices"
	"sort"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// RegisterListening adds PROTOCOL §8 operation types (version 1).
func RegisterListening(r *protocol.Registry) {
	r.Register("session.start", 1, sessionOp{kind: projection.KindSessionStart})
	r.Register("session.checkpoint", 1, sessionOp{kind: projection.KindCheckpoint})
	r.Register("session.end", 1, sessionOp{kind: projection.KindSessionEnd})
	r.Register("session.finalize", 1, sessionOp{kind: projection.KindFinalize})
	r.Register("episode.leave", 1, leaveOp{})
	r.Register("history.remove", 1, historyOp{kind: projection.KindHistoryRemove})
	r.Register("history.restore", 1, historyOp{kind: projection.KindHistoryRestore})
	r.Register("history.clear", 1, accountCutOp{kind: projection.KindHistoryClear})
	r.Register("statistics.reset", 1, accountCutOp{kind: projection.KindStatsReset})
}

var endReasons = map[string]bool{"ended": true, "skipped": true, "switched": true, "removed": true, "suspended": true, "superseded": true}

// startRefs validates a session start's episode and its queue bundle (PROTOCOL §7.6 by
// PROTOCOL §7.6): {key, position_id, place}, the add the device resolved when it did not see the
// episode queued; the target is the started episode (the place's anchor is context).
func startRefs(namespace string, f *protocol.Fact) (EpisodeRef, *placement, place, error) {
	r, err := parseEpisodeRef(f.Payload["episode"], namespace)
	if err != nil {
		return r, nil, place{}, err
	}
	add, at, err := parseBundle(f.Payload, r, namespace)
	if err != nil {
		return r, nil, place{}, err
	}
	return r, add, at, episodeTargets(f, []EpisodeRef{r})
}

// parseBundle reads a payload's queue_bundle (a play's or a filing's add
// the device resolved): nil without one.
func parseBundle(payload map[string]any, r EpisodeRef, namespace string) (*placement, place, error) {
	b, present := payload["queue_bundle"]
	if !present {
		return nil, place{}, nil
	}
	obj, ok := object(b)
	if !ok {
		return nil, place{}, invalidf("queue_bundle must be an object")
	}
	if err := fields(obj, []string{"key", "position_id", "place"}); err != nil {
		return nil, place{}, err
	}
	k, u, err := parseKeyAndID(obj)
	if err != nil {
		return nil, place{}, err
	}
	at, err := parsePlace(obj["place"], namespace)
	if err != nil {
		return nil, place{}, err
	}
	return &placement{ref: r, key: k, positionID: u}, at, nil
}

// singleRef validates a payload's one episode reference against the targets.
func singleRef(namespace string, f *protocol.Fact) (EpisodeRef, error) {
	r, err := parseEpisodeRef(f.Payload["episode"], namespace)
	if err != nil {
		return r, err
	}
	return r, checkTargets(f, []protocol.Target{{Entity: "episode", ID: r.EpisodeID, Generation: r.GenerationID}})
}

func nonNegative(obj map[string]any, k string) (int64, error) {
	n, ok := integer(obj[k])
	if !ok || n < 0 {
		return 0, invalidf("%s must be a non-negative integer", k)
	}
	return n, nil
}

func flag(obj map[string]any, k string) (bool, error) {
	b, ok := boolean(obj[k])
	if !ok {
		return false, invalidf("%s must be a boolean", k)
	}
	return b, nil
}

func uuidField(obj map[string]any, k string) (string, error) {
	s, ok := str(obj, k)
	if !ok || !contract.ValidUUID(s) {
		return "", invalidf("%s must be a UUID", k)
	}
	return s, nil
}

// parseSave validates an export's save identity (PROTOCOL §6.10): {id, index, first?,
// baseline, passive}; first is absent when the export covers no new save.
func parseSave(raw any) (*projection.SaveMeta, error) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, invalidf("save must be an object")
	}
	if err := fields(obj, []string{"id", "index", "baseline", "passive"}, "first"); err != nil {
		return nil, err
	}
	var m projection.SaveMeta
	var err error
	if m.ID, err = uuidField(obj, "id"); err != nil {
		return nil, err
	}
	if m.Index, err = nonNegative(obj, "index"); err != nil {
		return nil, err
	}
	if m.Index < 1 {
		return nil, invalidf("save index must be at least 1")
	}
	if m.Baseline, err = uuidField(obj, "baseline"); err != nil {
		return nil, err
	}
	if m.Passive, err = flag(obj, "passive"); err != nil {
		return nil, err
	}
	if _, present := obj["first"]; present {
		if m.First, err = uuidField(obj, "first"); err != nil {
			return nil, err
		}
	}
	return &m, nil
}

// ---- sessions

type sessionOp struct{ kind string }

func (sessionOp) Capability() string { return "" }

type sessionPayload struct {
	session                                       string
	ordinal, position, contribution               int64
	activity, completed, removeFromQueue, history bool
	claim                                         string
	claiming                                      bool // a checkpoint's intentional resume (PROTOCOL §8.1)
	duration                                      *int64
	baseline                                      string               // a start's (PROTOCOL §6.10)
	save                                          *projection.SaveMeta // an export's (PROTOCOL §6.10)
}

func (o sessionOp) parse(f *protocol.Fact) (sessionPayload, error) {
	var p sessionPayload
	var err error
	switch o.kind {
	case projection.KindSessionStart:
		err = fields(f.Payload, []string{"episode", "session_id", "start_position", "activity"}, "observed_claim", "duration", "queue_bundle", "baseline")
	case projection.KindCheckpoint:
		err = fields(f.Payload, []string{"episode", "session_id", "ordinal", "position", "contribution", "activity"}, "duration", "save",
			"claim", "observed_claim")
	case projection.KindSessionEnd:
		err = fields(f.Payload, []string{"episode", "session_id", "ordinal", "position", "contribution", "activity", "reason", "completed",
			"threshold_ms", "remove_from_queue", "history"}, "duration", "save")
	case projection.KindFinalize:
		err = fields(f.Payload, []string{"episode", "session_id", "ordinal", "claim_id", "completed", "threshold_ms", "remove_from_queue", "history"})
	}
	if err != nil {
		return p, err
	}
	if p.session, err = uuidField(f.Payload, "session_id"); err != nil {
		return p, err
	}
	if o.kind == projection.KindSessionStart {
		if p.position, err = nonNegative(f.Payload, "start_position"); err != nil {
			return p, err
		}
		if _, present := f.Payload["observed_claim"]; present {
			if _, err = uuidField(f.Payload, "observed_claim"); err != nil {
				return p, err
			}
		}
		if _, present := f.Payload["baseline"]; present {
			if p.baseline, err = uuidField(f.Payload, "baseline"); err != nil {
				return p, err
			}
		}
	} else {
		if p.ordinal, err = nonNegative(f.Payload, "ordinal"); err != nil {
			return p, err
		}
		if p.ordinal < 1 {
			return p, invalidf("ordinal must be at least 1")
		}
	}
	if o.kind == projection.KindCheckpoint || o.kind == projection.KindSessionEnd {
		if p.position, err = nonNegative(f.Payload, "position"); err != nil {
			return p, err
		}
		if p.contribution, err = nonNegative(f.Payload, "contribution"); err != nil {
			return p, err
		}
		if raw, present := f.Payload["save"]; present {
			if p.save, err = parseSave(raw); err != nil {
				return p, err
			}
		}
	}
	if o.kind != projection.KindFinalize {
		if p.activity, err = flag(f.Payload, "activity"); err != nil {
			return p, err
		}
		if _, present := f.Payload["duration"]; present {
			d, err := nonNegative(f.Payload, "duration")
			if err != nil {
				return p, err
			}
			p.duration = &d
		}
	}
	if o.kind == projection.KindSessionEnd {
		if r, ok := str(f.Payload, "reason"); !ok || !endReasons[r] {
			return p, invalidf("bad reason")
		}
	}
	if o.kind == projection.KindSessionEnd || o.kind == projection.KindFinalize {
		if p.completed, err = flag(f.Payload, "completed"); err != nil {
			return p, err
		}
		if _, err = nonNegative(f.Payload, "threshold_ms"); err != nil {
			return p, err
		}
		if p.removeFromQueue, err = flag(f.Payload, "remove_from_queue"); err != nil {
			return p, err
		}
		if p.history, err = flag(f.Payload, "history"); err != nil {
			return p, err
		}
	}
	if o.kind == projection.KindFinalize {
		if p.claim, err = uuidField(f.Payload, "claim_id"); err != nil {
			return p, err
		}
	}
	if o.kind == projection.KindCheckpoint {
		if err := p.parseClaim(f.Payload); err != nil {
			return p, err
		}
	}
	return p, nil
}

// parseClaim validates a checkpoint's resume claim (PROTOCOL §8): claim is a strict boolean
// (absent: false); observed_claim, evidence only, is a UUID allowed only with claim; a
// claim is listening activity and covers a new save that is not passive (a resume save,
// never a restart or tail export). Malformed claim fields or combinations are invalid; the
// session, ordinal, sealing and generation checks keep their outcomes.
func (p *sessionPayload) parseClaim(payload map[string]any) error {
	if raw, present := payload["claim"]; present {
		b, ok := boolean(raw)
		if !ok {
			return invalidf("claim must be a boolean")
		}
		p.claiming = b
	}
	if _, present := payload["observed_claim"]; present {
		if !p.claiming {
			return invalidf("observed_claim is allowed only with claim")
		}
		if _, err := uuidField(payload, "observed_claim"); err != nil {
			return err
		}
	}
	if !p.claiming {
		return nil
	}
	if !p.activity {
		return invalidf("a claim is listening activity")
	}
	// A claim needs its save evidence: a new save, not passive (ValidateState checks that its path
	// has not covered it).
	if p.save == nil || p.save.Passive || p.save.First == "" {
		return invalidf("a claim covers a new save that is not passive")
	}
	return nil
}

// claimCovered reports, for a claiming checkpoint whose session checks pass, whether its save is
// already covered on its writing path: an export of the path (the continuation's own, and its
// parents' shared part) exported the same save, the save the claim names as its first, or one
// with the same or a higher index (a repeated or covered save is rejected invalid_payload). A
// claim whose session checks fail keeps their outcome (foreign_session,
// session_ordinal_conflict, session_sealed), given in Apply.
func claimCovered(acct *storage.AccountTx, f *protocol.Fact, p sessionPayload, s storage.SessionRow) (bool, error) {
	fa, _, err := acct.FactAuthorOf(f.OpID)
	if err != nil {
		return false, err
	}
	author := fa.EffectiveAuthor
	if author == "" {
		author = f.AuthorID
	}
	own, err := continuationFor(acct, p.session, author, s.AuthorID)
	if err != nil || own == "" {
		return false, err
	}
	type segment struct {
		author string
		limit  int64 // greatest ordinal on the path (-1: every one)
	}
	var path []segment
	var parent string
	var base int64
	if own == author {
		c, err := acct.Continuation(p.session, author)
		if err != nil {
			return false, err
		}
		if p.ordinal <= c.Latest || c.SealedBy != "" {
			return false, nil // its existing outcome, in Apply
		}
		path = append(path, segment{author, -1})
		parent, base = c.Parent, c.Base
	} else {
		if parent, base, err = branchOf(acct, p.session, author); err != nil {
			return false, err
		}
		if p.ordinal <= base {
			return false, nil
		}
	}
	for n := 0; parent != "" && n < 1024; n++ {
		path = append(path, segment{parent, base})
		c, err := acct.Continuation(p.session, parent)
		if err != nil {
			return false, err
		}
		parent, base = c.Parent, min(base, c.Base)
	}
	for _, seg := range path {
		ordinals, nodes, err := acct.SessionFactsOf(p.session, seg.author)
		if err != nil {
			return false, err
		}
		for i, o := range ordinals {
			if o == 0 || (seg.limit >= 0 && o > seg.limit) {
				continue
			}
			raw, ok, err := acct.Contribution(s.Scope, nodes[i])
			if err != nil {
				return false, err
			}
			if !ok {
				continue
			}
			e, err := decodeEffect(raw)
			if err != nil {
				return false, err
			}
			for _, x := range parts(e) {
				if x.Save != nil && (x.Save.ID == p.save.ID || x.Save.ID == p.save.First || x.Save.Index >= p.save.Index) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (o sessionOp) Validate(f *protocol.Fact) error {
	if _, err := o.parse(f); err != nil {
		return err
	}
	_, err := o.refs("", f)
	return err
}

func (o sessionOp) ValidateInAccount(account protocol.AccountInfo, f *protocol.Fact) error {
	_, err := o.refs(account.Namespace, f)
	return err
}

// refs validates the episode reference(s) and targets.
func (o sessionOp) refs(namespace string, f *protocol.Fact) (EpisodeRef, error) {
	if o.kind == projection.KindSessionStart {
		r, _, _, err := startRefs(namespace, f)
		return r, err
	}
	return singleRef(namespace, f)
}

// NeededCapabilities: a queue bundle is a queue write (PROTOCOL §10.1 rule 2), so a
// client without the queue capability may not author one.
func (o sessionOp) NeededCapabilities(f *protocol.Fact) []string {
	if _, present := f.Payload["queue_bundle"]; o.kind == projection.KindSessionStart && present {
		return []string{"queue"}
	}
	return nil
}

// ValidateState rejects session facts that cannot belong to their session
// (PROTOCOL §8 rules 1 and 2), after PROTOCOL §7.2 episode checks, which answer
// not_applied in Apply.
func (o sessionOp) ValidateState(acct *storage.AccountTx, account protocol.AccountInfo, f *protocol.Fact) error {
	ref, _ := o.refs(account.Namespace, f)
	p, _ := o.parse(f)
	if reason, _, err := episodeReason(acct, account.Namespace, &ref); err != nil || reason != "" {
		return err
	}
	s, err := acct.Session(p.session)
	if errors.Is(err, storage.ErrNotFound) {
		if o.kind == projection.KindSessionStart {
			return nil
		}
		return protocol.ErrInvalidPayload
	} else if err != nil {
		return err
	}
	if s.Scope != protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID) {
		return protocol.ErrInvalidPayload
	}
	if o.kind == projection.KindCheckpoint && p.claiming {
		if covered, err := claimCovered(acct, f, p, s); err != nil {
			return err
		} else if covered {
			return protocol.ErrInvalidPayload
		}
	}
	if o.kind == projection.KindFinalize {
		fa, _, err := acct.FactAuthorOf(f.OpID)
		if err != nil {
			return err
		}
		author := fa.EffectiveAuthor
		if author == "" {
			author = f.AuthorID
		}
		target, err := continuationFor(acct, p.session, author, s.AuthorID)
		if err != nil || target == "" {
			return err // another author's session: not_applied in Apply
		}
		if _, _, err := ordinalOwner(acct, p.session, target, p.ordinal); errors.Is(err, storage.ErrNotFound) {
			return protocol.ErrInvalidPayload
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (o sessionOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	outcome, err := o.apply(ctx, f)
	if err != nil || outcome.Result != "not_applied" {
		return outcome, err
	}
	return o.refused(ctx, f, outcome)
}

// refused completes a session start that applies nothing but carries a queue
// bundle: the bundle is still a placement command, so the start is a replay
// step that leaves its ghost (PROTOCOL §7.6) and a queue
// membership/position node (PROTOCOL §7.6), and under keep-sorted at its place its
// receipt records the queue. An end or a finalization that
// applies nothing contributes no removal, so it is no queue node.
func (o sessionOp) refused(ctx *protocol.ApplyContext, f *protocol.Fact, outcome protocol.Outcome) (protocol.Outcome, error) {
	if o.kind == projection.KindSessionStart {
		_, add, _, err := startRefs(ctx.Namespace, f)
		if err != nil || add == nil {
			return outcome, err
		}
		// No key write: the start failed its own or its session's checks.
		if err := ctx.QueueStepWrites(f.OpID, nil, nil, nil); err != nil {
			return outcome, err
		}
		if err := ctx.Account.AddQueueNode(f.OpID); err != nil {
			return outcome, err
		}
		if ctx.Rebuilding() {
			return outcome, nil
		}
		sorted, err := keepSortedAt(ctx, f.OpID)
		if err != nil || !sorted {
			return outcome, err
		}
		outcome.Values, err = recordValues(ctx, nil, true)
		return outcome, err
	}
	return outcome, nil
}

func (o sessionOp) apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	ref, err := o.refs(ctx.Namespace, f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	p, err := o.parse(f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if reason, err := checkEpisode(ctx, &ref); err != nil || reason != "" {
		return protocol.Outcome{Result: "not_applied", Reason: reason}, err
	}
	if fx := ctx.Fixed; fx != nil && fx.Result == "not_applied" && sessionReasons[fx.Reason] {
		// Recovery (reference CHOICE): the session checks are not run
		// again; a fact its receipt refused contributes nothing.
		return *fx, nil
	}
	acct := ctx.Account
	scope := protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID)
	notApplied := func(reason string) (protocol.Outcome, error) {
		return protocol.Outcome{Result: "not_applied", Reason: reason}, nil
	}
	// The fact's continuation is (session, effective author) (PROTOCOL §6.10).
	author := ctx.Author
	if author == "" {
		author = f.AuthorID
	}
	index := storage.ListeningFact{Node: f.OpID, Scope: scope, HistoryEntry: p.activity, Session: p.session, Author: author, Ordinal: p.ordinal}
	var cont storage.ContinuationRow
	var copied bool   // a finalization by a descendant author (PROTOCOL §6.10)
	var target string // the continuation a finalization names its ordinal in
	switch o.kind {
	case projection.KindSessionStart:
		// A second start of a session (same episode, ValidateState) is a
		// foreign append or a different fact at its ordinal 0.
		if s, err := acct.Session(p.session); err == nil {
			// As in the independent reference: an author of the session's
			// lineage (a copy) repeats its start; anyone else appends to a foreign one.
			if own, err := continuationFor(acct, p.session, author, s.AuthorID); err != nil {
				return protocol.Outcome{}, err
			} else if own == "" {
				return notApplied("foreign_session")
			}
			return notApplied("session_ordinal_conflict")
		} else if !errors.Is(err, storage.ErrNotFound) {
			return protocol.Outcome{}, err
		}
		if err := acct.StartSession(p.session, scope, author, f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
		cont = storage.ContinuationRow{Session: p.session, Author: author}
	default:
		s, err := acct.Session(p.session)
		if err != nil {
			return protocol.Outcome{}, err
		}
		own, err := continuationFor(acct, p.session, author, s.AuthorID)
		if err != nil {
			return protocol.Outcome{}, err
		}
		if own == "" {
			return notApplied("foreign_session") // not the session's author nor a descendant
		}
		copied = author != s.AuthorID
		if o.kind == projection.KindFinalize {
			target = own
			if own == author {
				c, err := acct.Continuation(p.session, author)
				if err != nil {
					return protocol.Outcome{}, err
				}
				if c.FinalizedBy != "" {
					return notApplied("session_finalized")
				}
				if err := acct.FinalizeContinuation(p.session, author, f.OpID); err != nil {
					return protocol.Outcome{}, err
				}
			}
			// A copied finalization of an ancestor's continuation consumes nothing
			// here: the fold decides whether it is eligible (PROTOCOL §6.10). It can name
			// only an ordinal in its own writing history (Base below).
			if own != author {
				if cont.Base, err = sharedOrdinal(acct, p.session, author, own); err != nil {
					return protocol.Outcome{}, err
				}
			}
			// A finalization adds no contribution; only its history mark is an entry.
			index.HistoryEntry, index.Session = p.history, ""
			break
		}
		if own != author {
			if cont, err = newContinuation(acct, p.session, author, s.AuthorID, s.StartNode); err != nil {
				return protocol.Outcome{}, err
			}
		} else if cont, err = acct.Continuation(p.session, author); err != nil {
			return protocol.Outcome{}, err
		}
		if p.ordinal <= cont.Latest {
			return notApplied("session_ordinal_conflict")
		}
		if cont.SealedBy != "" {
			return notApplied("session_sealed")
		}
		if err := acct.AddSessionFact(p.session, author, p.ordinal, f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
		if o.kind == projection.KindSessionEnd {
			if err := acct.SealContinuation(p.session, author, f.OpID); err != nil {
				return protocol.Outcome{}, err
			}
			index.HistoryEntry = p.activity || p.history
		}
	}
	if index.HistoryEntry || index.Session != "" {
		if err := acct.AddListeningFact(index); err != nil {
			return protocol.Outcome{}, err
		}
	}
	pos := p.position
	e := projection.Effect{Kind: o.kind, Session: p.session, Ordinal: p.ordinal, Position: &pos, Contribution: p.contribution,
		Activity: p.activity, Completed: p.completed, RemoveFromQueue: p.removeFromQueue, History: p.history, Claim: p.claim,
		Time: ctx.NormalizedTime, Duration: p.duration, Installation: author,
		Author: author, Parent: cont.Parent, Base: cont.Base, Baseline: p.baseline, Save: p.save, Copied: copied, Target: target,
		Claiming: p.claiming}
	if o.kind == projection.KindFinalize {
		e.Position = nil
	}
	e = withRef(e, ref)
	var resolved *projection.Effect
	if o.kind == projection.KindSessionStart {
		if resolved, err = startQueueEffect(ctx, f, ref, &e); err != nil {
			return protocol.Outcome{}, err
		}
	}
	contribution := e
	if resolved != nil {
		contribution = projection.ComposeAt(ref.EpisodeID, e, *resolved)
	}
	if err := ctx.Contribute(scope, contribution); err != nil {
		return protocol.Outcome{}, err
	}
	adds := e.QueueKey != "" || resolved != nil
	// Queue membership/position nodes (PROTOCOL §7.6): a start's add, an end's or a
	// finalization's queue removal.
	if adds || ((o.kind == projection.KindSessionEnd || o.kind == projection.KindFinalize) && (p.completed || p.removeFromQueue)) {
		if err := ctx.Account.AddQueueNode(f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
	}
	if e.QueueKey != "" {
		// The device's bundle: the start is a replay step at its place, and
		// its receipt records the values of what it places and, under
		// keep-sorted at its place, of every member then in the queue
		// (PROTOCOL §7.6).
		outcome := ctx.SettleLater()
		if ctx.Rebuilding() {
			return outcome, nil // a replay keeps its receipt
		}
		trigger, err := keepSortedAt(ctx, f.OpID)
		if err != nil {
			return outcome, err
		}
		outcome.Values, err = recordValues(ctx, []string{scope}, trigger)
		return outcome, err
	}
	if adds {
		return ctx.SettleLater(), nil
	}
	if o.kind == projection.KindFinalize {
		return ctx.Settle() // its outcome depends on the fold
	}
	// Start, checkpoint and end are applied whenever admitted (PROTOCOL §8).
	return ctx.SettleLater(), nil
}

// sessionReasons are the session checks' refusals (PROTOCOL §8 checks 3 and 4).
var sessionReasons = map[string]bool{"foreign_session": true, "session_ordinal_conflict": true, "session_sealed": true, "session_finalized": true}

// ---- leaving a paused load (PROTOCOL §8, iOS PROTOCOL §15)

// leaveOp is episode.leave: a device switched away from, skipped or ended an
// episode it loaded but did not play in that load. Its history mark is a
// history entry; its completion (under the claim it observed) or skip
// removal takes effect only for the first effective leave of its pending
// transition, in the fold. It claims nothing, dismisses no NEW, is no
// filing barrier, adds no contribution or duration and is no listening
// activity.
type leaveOp struct{}

func (leaveOp) Capability() string { return "" }

var leaveReasons = map[string]bool{"switched": true, "skipped": true, "ended": true}

type leavePayload struct {
	transition, claim                   string
	completed, removeFromQueue, history bool
}

func (leaveOp) parse(f *protocol.Fact) (leavePayload, error) {
	var p leavePayload
	err := fields(f.Payload, []string{"episode", "transition_id", "reason", "position", "completed", "threshold_ms", "remove_from_queue",
		"history"}, "observed_claim")
	if err != nil {
		return p, err
	}
	if p.transition, err = uuidField(f.Payload, "transition_id"); err != nil {
		return p, err
	}
	if r, ok := str(f.Payload, "reason"); !ok || !leaveReasons[r] {
		return p, invalidf("bad reason")
	}
	// position and threshold_ms are evidence (durations); nothing checks
	// them against the decisions (reference CHOICE).
	if _, err = nonNegative(f.Payload, "position"); err != nil {
		return p, err
	}
	if _, err = nonNegative(f.Payload, "threshold_ms"); err != nil {
		return p, err
	}
	if _, present := f.Payload["observed_claim"]; present {
		if p.claim, err = uuidField(f.Payload, "observed_claim"); err != nil {
			return p, err
		}
	}
	if p.completed, err = flag(f.Payload, "completed"); err != nil {
		return p, err
	}
	if p.removeFromQueue, err = flag(f.Payload, "remove_from_queue"); err != nil {
		return p, err
	}
	if p.history, err = flag(f.Payload, "history"); err != nil {
		return p, err
	}
	return p, nil
}

func (o leaveOp) Validate(f *protocol.Fact) error {
	if _, err := o.parse(f); err != nil {
		return err
	}
	_, err := singleRef("", f)
	return err
}

func (leaveOp) ValidateInAccount(account protocol.AccountInfo, f *protocol.Fact) error {
	_, err := singleRef(account.Namespace, f)
	return err
}

func (o leaveOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	ref, err := singleRef(ctx.Namespace, f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	p, err := o.parse(f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if reason, err := checkEpisode(ctx, &ref); err != nil || reason != "" {
		return protocol.Outcome{Result: "not_applied", Reason: reason}, err
	}
	scope := protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID)
	if p.history {
		// Its history mark is a history entry, covered by cuts whose causal
		// past holds it.
		if err := ctx.Account.AddListeningFact(storage.ListeningFact{Node: f.OpID, Scope: scope, HistoryEntry: true}); err != nil {
			return protocol.Outcome{}, err
		}
	}
	e := projection.Effect{Kind: projection.KindLeave, Transition: p.transition, Claim: p.claim, Completed: p.completed,
		RemoveFromQueue: p.removeFromQueue, History: p.history, Time: ctx.NormalizedTime}
	if err := ctx.Contribute(scope, withRef(e, ref)); err != nil {
		return protocol.Outcome{}, err
	}
	// A leave carrying a completion or a removal is a queue membership node
	// for the queue's decision frontier, as an end or a finalization carrying one is
	// (reference CHOICE).
	if p.completed || p.removeFromQueue {
		if err := ctx.Account.AddQueueNode(f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
	}
	return ctx.Settle() // its outcome depends on the fold
}

// ---- history remove and restore

type historyOp struct{ kind string }

func (historyOp) Capability() string { return "history" }

func (o historyOp) Validate(f *protocol.Fact) error {
	if o.kind == projection.KindHistoryRemove {
		return fields(f.Payload, []string{"episode"})
	}
	if err := fields(f.Payload, []string{"episode", "removal", "time"}); err != nil {
		return err
	}
	if _, err := uuidField(f.Payload, "removal"); err != nil {
		return err
	}
	if _, ok := integer(f.Payload["time"]); !ok {
		return invalidf("time must be an integer")
	}
	return nil
}

// NodePrerequisites makes a restore wait for the removal it names (admission
// asks only for payloads that pass Validate and ValidateInAccount).
func (o historyOp) NodePrerequisites(f *protocol.Fact) []string {
	if o.kind == projection.KindHistoryRestore {
		removal, _ := str(f.Payload, "removal")
		return []string{removal}
	}
	return nil
}

func (o historyOp) ValidateInAccount(account protocol.AccountInfo, f *protocol.Fact) error {
	_, err := singleRef(account.Namespace, f)
	return err
}

// ValidateState requires a restore to name a removal of its own episode.
func (o historyOp) ValidateState(acct *storage.AccountTx, account protocol.AccountInfo, f *protocol.Fact) error {
	if o.kind != projection.KindHistoryRestore {
		return nil
	}
	ref, _ := singleRef(account.Namespace, f)
	if reason, _, err := episodeReason(acct, account.Namespace, &ref); err != nil || reason != "" {
		return err
	}
	removal, _ := str(f.Payload, "removal")
	lf, err := acct.ListeningFactOf(removal)
	if errors.Is(err, storage.ErrNotFound) || (err == nil && (!lf.Removal || lf.Scope != protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID))) {
		return protocol.ErrInvalidPayload
	}
	return err
}

func (o historyOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	ref, err := singleRef(ctx.Namespace, f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if reason, err := checkEpisode(ctx, &ref); err != nil || reason != "" {
		return protocol.Outcome{Result: "not_applied", Reason: reason}, err
	}
	acct := ctx.Account
	scope := protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID)
	e := projection.Effect{Kind: o.kind}
	if o.kind == projection.KindHistoryRestore {
		e.Removal, _ = str(f.Payload, "removal")
		// Only the first-accepted restore of a removal takes effect.
		if done, err := acct.Restored(e.Removal); err != nil || done {
			return protocol.Outcome{Result: "not_applied", Reason: "already_restored"}, err
		}
		if err := acct.AddListeningFact(storage.ListeningFact{Node: f.OpID, Scope: scope, Restores: e.Removal}); err != nil {
			return protocol.Outcome{}, err
		}
	} else {
		entries, err := acct.HistoryEntries(scope)
		if err != nil {
			return protocol.Outcome{}, err
		}
		covered, err := coveredBy(acct, f.OpID, entries)
		if err != nil {
			return protocol.Outcome{}, err
		}
		e.Covered = covered[scope]
		if err := acct.AddListeningFact(storage.ListeningFact{Node: f.OpID, Scope: scope, Removal: true}); err != nil {
			return protocol.Outcome{}, err
		}
	}
	if err := ctx.Contribute(scope, withRef(e, ref)); err != nil {
		return protocol.Outcome{}, err
	}
	// Removals and first restores are applied whenever admitted.
	return ctx.SettleLater(), nil
}

// ---- history clear and statistics reset (account-level, no targets)

type accountCutOp struct{ kind string }

func (o accountCutOp) Capability() string {
	if o.kind == projection.KindStatsReset {
		return "statistics"
	}
	return "history"
}

func (o accountCutOp) Validate(f *protocol.Fact) error {
	if err := fields(f.Payload, nil); err != nil {
		return err
	}
	if len(f.Targets) != 0 {
		return invalidf("an account-level cut has no targets")
	}
	return nil
}

func (o accountCutOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	acct := ctx.Account
	var candidates []storage.ListeningFact
	var err error
	if o.kind == projection.KindHistoryClear {
		candidates, err = acct.HistoryEntries("")
	} else {
		candidates, err = acct.SessionFacts()
	}
	if err != nil {
		return protocol.Outcome{}, err
	}
	covered, err := coveredBy(acct, f.OpID, candidates)
	if err != nil {
		return protocol.Outcome{}, err
	}
	scopes := make([]string, 0, len(covered))
	for scope := range covered {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	if o.kind == projection.KindStatsReset {
		cuts := statisticsCuts(candidates, covered)
		for _, scope := range scopes {
			if err := ctx.Contribute(scope, projection.Effect{Kind: o.kind, Cuts: cuts[scope]}); err != nil {
				return protocol.Outcome{}, err
			}
		}
	} else {
		for _, scope := range scopes {
			if err := ctx.Contribute(scope, projection.Effect{Kind: o.kind, Covered: covered[scope]}); err != nil {
				return protocol.Outcome{}, err
			}
		}
	}
	// A cut is applied whatever the folds report (PROTOCOL §8), including in
	// scopes of closed generations.
	return ctx.SettleLater(), nil
}

// coveredBy returns the candidates in the cut's causal past, by scope, each
// list sorted (PROTOCOL §8: a cut observes its causal past).
func coveredBy(acct *storage.AccountTx, cut string, candidates []storage.ListeningFact) (map[string][]string, error) {
	out := map[string][]string{}
	if len(candidates) == 0 {
		return out, nil
	}
	minRank := candidates[0].Rank
	for _, c := range candidates {
		minRank = min(minRank, c.Rank)
	}
	past, err := acct.Ancestors(cut, minRank)
	if err != nil {
		return nil, err
	}
	for _, c := range candidates {
		if past[c.Node] {
			out[c.Scope] = append(out[c.Scope], c.Node)
		}
	}
	for scope := range out {
		sort.Strings(out[scope])
	}
	return out, nil
}

// statisticsCuts is, per scope, each covered session's greatest covered ordinal.
func statisticsCuts(candidates []storage.ListeningFact, covered map[string][]string) map[string][]projection.SessionCut {
	isCovered := map[string]bool{}
	for _, list := range covered {
		for _, n := range list {
			isCovered[n] = true
		}
	}
	// The greatest covered ordinal per scope and continuation (PROTOCOL §6.10).
	type key struct{ session, author string }
	best := map[string]map[key]int64{}
	for _, c := range candidates {
		if !isCovered[c.Node] {
			continue
		}
		if best[c.Scope] == nil {
			best[c.Scope] = map[key]int64{}
		}
		k := key{c.Session, c.Author}
		if o, seen := best[c.Scope][k]; !seen || c.Ordinal > o {
			best[c.Scope][k] = c.Ordinal
		}
	}
	out := map[string][]projection.SessionCut{}
	for scope, conts := range best {
		for k, o := range conts {
			out[scope] = append(out[scope], projection.SessionCut{Session: k.session, Author: k.author, Ordinal: o})
		}
		sort.Slice(out[scope], func(i, j int) bool {
			a, b := out[scope][i], out[scope][j]
			if a.Session != b.Session {
				return a.Session < b.Session
			}
			return a.Author < b.Author
		})
	}
	return out
}

// startQueueEffect sets a session start's queue add (PROTOCOL §7.6): the device's
// bundle, or for a client without the queue capability that sent none, the
// server's resolution when the episode is not queued, recorded as a
// queue.resolved_add decision and folded as the start's own effect. Either
// makes the start a replay step at its place (PROTOCOL §7.6).
//
// The device's bundle is part of the start's own effect. The server's
// resolution is an effect of its own (KindResolvedAdd), contributed by the
// start beside its effect and returned: it lives where the decision's
// episode lives, not where the start's occurrence routes.
func startQueueEffect(ctx *protocol.ApplyContext, f *protocol.Fact, ref EpisodeRef, e *projection.Effect) (*projection.Effect, error) {
	_, add, _, err := startRefs(ctx.Namespace, f)
	if err != nil {
		return nil, err
	}
	if add != nil {
		// The bundle's key can be a key write (its one member passed its
		// checks); the replay decides whether the start places it.
		e.QueueKey, e.PositionID = add.key.String(), formatUUID(add.positionID)
		return nil, ctx.QueueStepWrites(f.OpID, []int{0}, nil, nil)
	}
	if slices.Contains(ctx.Capabilities, "queue") {
		return nil, nil // the device saw it queued
	}
	// The start's own effects count: a start ranked before a completion it now
	// supersedes keeps the episode queued, so it needs no add, whichever arrived
	// first.
	scope := protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID)
	if err := ctx.Contribute(scope, *e); err != nil {
		return nil, err
	}
	if st, err := ctx.FoldEpisodeScope(scope); err != nil || st.InQueue {
		return nil, err
	}
	// Dependencies: the start and the queue frontier the server observed.
	deps, err := ctx.Account.QueueFrontier()
	if err != nil {
		return nil, err
	}
	deps = append(deps, f.OpID)
	slices.Sort(deps)
	deps = slices.Compact(deps)
	var in protocol.DecisionInput
	if rec, ok := ctx.RecordedDecision("queue.resolved_add", f.OpID); ok && ctx.Rebuilding() {
		// A rebuild takes the recorded resolution (its place and key were
		// resolved against the queue as published then).
		in = recordedInput(rec)
	} else {
		at, key, u, location, err := resolvedPlacement(ctx)
		if err != nil {
			return nil, err
		}
		// Under keep-sorted at the start's place, the resolved add is a
		// sort trigger: it records every member then in the queue (PROTOCOL §8).
		trigger, err := keepSortedAt(ctx, f.OpID)
		if err != nil {
			return nil, err
		}
		values, err := recordValues(ctx, []string{scope}, trigger)
		if err != nil {
			return nil, err
		}
		in = protocol.DecisionInput{Type: "queue.resolved_add", Version: 1, Subject: f.OpID, Dependencies: deps,
			Inputs: map[string]any{"trigger": f.OpID, "enqueue_location": location},
			Result: map[string]any{"episode_id": ref.EpisodeID, "generation_id": ref.GenerationID, "key": key.String(), "position_id": formatUUID(u),
				"place": at.value(), "values": values}}
	}
	decision, err := ctx.Decide(in)
	if err != nil || decision == nil {
		return nil, err // a recovery rebuild admits the decision at its own position
	}
	id := decision["decision_id"].(string)
	if err := ctx.Account.AddQueueNode(id); err != nil {
		return nil, err
	}
	if err := ctx.QueueStep(f.OpID, id); err != nil {
		return nil, err
	}
	// The decision as made (a rebuild's is the recorded one); its key is a
	// key write when the replay finds it places the episode.
	add2 := resolvedAddEffect(decision)
	return &add2, nil
}

// recordedInput is a recorded decision as a request for it (a rebuild asks
// for exactly what was recorded).
func recordedInput(d map[string]any) protocol.DecisionInput {
	typ, _ := str(d, "decision_type")
	subject, _ := str(d, "subject")
	version, _ := integer(d["decision_version"])
	inputs, _ := object(d["inputs"])
	result, _ := object(d["result"])
	var deps []string
	list, _ := d["dependencies"].([]any)
	for _, x := range list {
		if id, ok := x.(string); ok {
			deps = append(deps, id)
		}
	}
	return protocol.DecisionInput{Type: typ, Version: version, Subject: subject, Dependencies: deps, Inputs: inputs, Result: result}
}

// resolvedAddEffect is a queue.resolved_add decision's effect: the start's
// add at the recorded key, for the decision's episode.
func resolvedAddEffect(decision map[string]any) projection.Effect {
	result, _ := object(decision["result"])
	e := projection.Effect{Kind: projection.KindResolvedAdd}
	e.QueueKey, _ = str(result, "key")
	e.PositionID, _ = str(result, "position_id")
	e.Member, _ = str(result, "episode_id")
	return e
}

// admitResolvedAdd admits a queue.resolved_add that its start did not make
// again in a recovery rebuild (PROTOCOL §6.11): the start contributes the add beside
// its effect where the decision's episode lives, as startQueueEffect does,
// and the start becomes the replay step the decision resolves.
func admitResolvedAdd(c *protocol.ApplyContext, d map[string]any) error {
	id, _, result := decisionParts(d)
	start, _ := str(d, "subject")
	episode, _ := str(result, "episode_id")
	generation, _ := str(result, "generation_id")
	if _, err := c.Account.NodeTime(start); errors.Is(err, storage.ErrNotFound) {
		return nil // the start contributes nothing (its receipt refused it)
	} else if err != nil {
		return err
	}
	v := newView(c)
	scope := protocol.EpisodeScope(v.routeScope(generation), v.root(episode))
	if v.err != nil {
		return v.err
	}
	add := resolvedAddEffect(d)
	e := add
	if raw, ok, err := c.Account.Contribution(scope, start); err != nil {
		return err
	} else if ok {
		x, err := decodeEffect(raw)
		if err != nil {
			return err
		}
		for _, p := range parts(x) {
			if p.Kind == projection.KindResolvedAdd {
				return c.QueueStep(start, id) // made already
			}
		}
		ep, _ := scopeEpisode(scope)
		e = projection.ComposeAt(ep, x, add)
	}
	if err := c.ContributeAs(start, scope, e); err != nil {
		return err
	}
	if err := c.Account.AddQueueNode(id); err != nil {
		return err
	}
	if err := c.Account.AddQueueNode(start); err != nil {
		return err
	}
	return c.QueueStep(start, id)
}
