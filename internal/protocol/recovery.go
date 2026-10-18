package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Recovery after a restore (PROTOCOL §6.11): devices upload what the restored archive lacks; the
// server verifies every receipt and decision MAC, inserts the entries at their original receipt
// positions, rebuilds the account from its archive in receipt order, and publishes what changed
// in one commit, with the end-of-request steps (provisional resolutions, identity unification,
// one sort reconciliation) made in the same commit.

// Recovery bounds advertised by GET /v1/info (bounds.recovery_inventory).
const (
	MaxRecoveryInventory = 2000
	MaxRecoveryEntries   = MaxOperations
)

var errRecoveryNotOpen = protoErr(400, "invalid_request", "This recovery is not open: start it with a recovery snapshot.")

// RecoveryEntry is one uploaded entry: a fact (or one part of a framed
// fact) with its original receipt, or a decision as recorded.
type RecoveryEntry struct {
	Fact     *Archived
	Part     *Part
	Receipt  map[string]any
	Decision map[string]any
}

// RecoveryUploadRequest is a decoded recovery upload.
type RecoveryUploadRequest struct {
	AccountID  string
	InstallID  string
	RecoveryID string
	Entries    []RecoveryEntry
	Secrets    []Secret
	// Steps are the end-of-request steps (PROTOCOL §6.11); the API runs all of
	// them. The rule vectors run them where the reference does.
	Steps RecoveryEndSteps
	// SameTimeline says the facts' receipts are of the account's current
	// timeline, so they are no evidence of a lost one (PROTOCOL §6.11): the rule
	// vectors' shorthand positions (docs/CONTRACT.md). Never set by the API.
	SameTimeline bool
}

// RecoveryEndSteps selects the end-of-request steps (PROTOCOL §6.11).
type RecoveryEndSteps struct {
	Resolutions bool // registrations without an effective resolution are resolved (provisional)
	Unify       bool // equivalent identity decisions are unified
}

// AllRecoveryEndSteps is what every upload request ends with. (The queue
// needs no step of its own: every commit ends with its publication,
// PROTOCOL §7.6.)
var AllRecoveryEndSteps = RecoveryEndSteps{Resolutions: true, Unify: true}

// RecoveryEndFunc runs the end-of-request steps in a recovery commit:
// decisions are made as a server commit's are, after the rebuild.
type RecoveryEndFunc func(c *ApplyContext, steps RecoveryEndSteps) error

// RegisterRecoveryEnd sets the end-of-request steps.
func (r *Registry) RegisterRecoveryEnd(fn RecoveryEndFunc) { r.recoveryEnd = fn }

// RecoveryResult is one entry's result.
type RecoveryResult struct {
	Kind         string   `json:"kind"` // fact, decision or part
	ID           string   `json:"id"`
	Status       string   `json:"status"` // joined, known, replaced, part_stored, dependency_missing, invalid_receipt, invalid, conflict
	Missing      []string `json:"missing,omitempty"`
	PartsPresent []int    `json:"parts_present,omitempty"`
}

// RecoveryUploadResponse is the upload response body.
type RecoveryUploadResponse struct {
	Head    string           `json:"head"`
	Results []RecoveryResult `json:"results"`
}

// RecoveryCounts are a recovery session's cumulative counts (diagnostics).
type RecoveryCounts struct {
	Requests          int64 `json:"requests"`
	Joined            int64 `json:"joined"`
	Known             int64 `json:"known"`
	Replaced          int64 `json:"replaced"`
	DependencyMissing int64 `json:"dependency_missing"`
	InvalidReceipt    int64 `json:"invalid_receipt"`
	Invalid           int64 `json:"invalid"`
	Conflict          int64 `json:"conflict"`
	// Mismatches counts replay results that differ from the receipts (the
	// receipts decide; the details are logged).
	Mismatches int64 `json:"mismatches,omitempty"`
}

// RecoveryProblem is an entry a recovery could not join: an invalid receipt
// or a conflict (PROTOCOL §6.11), listed in diagnostics.
type RecoveryProblem struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (s *Service) busy(accountID string) error {
	if s.Rebuilding(accountID) {
		return errBusy
	}
	return nil
}

// ---- upload

// RecoveryUpload joins one upload request (PROTOCOL §6.11) in one write
// transaction: it verifies and inserts the entries, rebuilds the account in
// recovery mode, runs the requested end-of-request steps and publishes
// everything in one commit. A request is retried safely: every result is
// idempotent by ID.
func (s *Service) RecoveryUpload(ctx context.Context, req RecoveryUploadRequest, authorize Authorize) (*RecoveryUploadResponse, error) {
	return s.recoveryUpload(ctx, req, authorize, nil)
}

// recoveryUpload is RecoveryUpload, with a recovery job's hooks when it runs
// one (nil: a request run synchronously).
func (s *Service) recoveryUpload(ctx context.Context, req RecoveryUploadRequest, authorize Authorize, job *uploadHooks) (*RecoveryUploadResponse, error) {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return nil, fmt.Errorf("protocol: no receipt key: %v", err)
	}
	if err := s.busy(req.AccountID); err != nil {
		return nil, err
	}
	var resp *RecoveryUploadResponse
	stage := func(authorize Authorize) (bool, error) {
		check := func(tx *storage.WriteTx) error {
			if err := authorized(tx, authorize, req.InstallID); err != nil {
				return err
			}
			return job.publish(tx)
		}
		staged, _, err := s.staged(ctx, req.AccountID, check, func(ss *Service) error {
			var err error
			resp, err = ss.recoveryUpload(ctx, req, authorize, nil)
			return err
		})
		return staged, err
	}
	// A job that ran its own transition before (a restart since) is
	// authorized as right after it, by the fence it stored with it: only on
	// the timeline that transition made.
	transitioned := job != nil && job.fence != nil
	if transitioned {
		authorize = installationAuthorize(req.AccountID, req.InstallID, *job.fence)
	}
	staged, err := stage(authorize)
	if errors.Is(err, errStagedEvidence) && transitioned {
		return nil, errBusy // a second lost timeline since: try again
	}
	if errors.Is(err, errStagedEvidence) {
		// The request proves a lost timeline (PROTOCOL §6.11): only the epoch
		// transition runs live, which is short; the upload is then staged again
		// against the new epoch. The transition signs every device out, so from
		// then on the uploader is authorized by its installation (still the
		// account's, not revoked, not superseded) and by the account's
		// credentials as they stood right after the transition: a revocation,
		// password change or reset meanwhile still wins. The exception holds
		// only on the timeline the transition made (its epoch): a later
		// transition, which signs every device out again, voids it. A job
		// stores that fence in the transition's transaction.
		var fence timelineFence
		if fence, err = s.recoveryTransition(ctx, req, authorize, keys, job); err != nil {
			return nil, err
		}
		if err := job.afterTransition(fence); err != nil {
			return nil, err
		}
		authorize = installationAuthorize(req.AccountID, req.InstallID, fence)
		if staged, err = stage(authorize); errors.Is(err, errStagedEvidence) {
			return nil, errBusy // a second lost timeline since: try again
		}
	}
	if staged || err != nil {
		return resp, err
	}
	return s.recoveryUploadInPlace(ctx, req, authorize, keys, job, false)
}

// errWouldJoin rolls back a cheap evaluation (recoveryUploadCheap) of an
// upload that joins something: it runs as a job.
var errWouldJoin = errors.New("protocol: the upload joins entries")

// recoveryUploadCheap answers, in place and at once, an upload that joins
// nothing (PROTOCOL §6.11): the archive as it is now answers it (joined
// entries known, stored parts part_stored, dependency_missing and frame
// expiry evaluated afresh), in the transaction the synchronous upload would
// make in place, with its end steps, counts, secrets and stored parts. An
// upload that would join or replace an entry, prove a lost timeline, or
// whose end steps decide anything that needs the account derived again is
// rolled back with errWouldJoin, before it changes anything.
func (s *Service) recoveryUploadCheap(ctx context.Context, req RecoveryUploadRequest, authorize Authorize) (*RecoveryUploadResponse, error) {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return nil, fmt.Errorf("protocol: no receipt key: %v", err)
	}
	if err := s.busy(req.AccountID); err != nil {
		return nil, err
	}
	return s.recoveryUploadInPlace(ctx, req, authorize, keys, nil, true)
}

// recoveryUploadInPlace joins an upload in one live write transaction: the
// whole account is derived again there when anything joined. cheap stops
// short of that (recoveryUploadCheap).
func (s *Service) recoveryUploadInPlace(ctx context.Context, req RecoveryUploadRequest, authorize Authorize, keys []storage.ReceiptKey,
	job *uploadHooks, cheap bool) (*RecoveryUploadResponse, error) {
	var resp *RecoveryUploadResponse
	now := s.Now().UnixMilli()
	secrets, err := s.prepareSecrets(req.AccountID, req.Secrets)
	if err != nil {
		return nil, err
	}
	// The upload holds the account's rebuild claim across its live
	// transaction (PROTOCOL §6.11): no staged rebuild (another device's job)
	// copies the account while it writes, so no publication of such a copy
	// replaces what it acknowledged (stored parts, frames, counts); while one
	// runs, the upload is busy.
	release, ok := s.claimRebuild(req.AccountID)
	if !ok {
		return nil, errRebuildClaimed
	}
	defer release()
	if cheap {
		if err := s.jobPoint("cheap"); err != nil {
			return nil, err
		}
	}
	var report RebuildReport
	var finished *commitBuilder
	var finalHead storage.Head
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		report = RebuildReport{}
		finished = nil
		rec, err := s.uploadable(tx, req, authorize)
		if err != nil {
			return err
		}
		if err := s.expireFrames(tx); err != nil {
			return err
		}
		acct := tx.Account(req.AccountID)
		if err := acct.DeferForeignKeys(); err != nil {
			return err
		}
		if err := s.putSecrets(acct, req.AccountID, secrets); err != nil {
			return err
		}
		j := &joiner{s: s, tx: tx, acct: acct, accountID: req.AccountID, installID: req.InstallID, recoveryID: req.RecoveryID, keys: keys, now: now,
			cheap: cheap}
		// Framed facts first: every part's receipt is verified, and a frame this request
		// completes is assembled in memory, so its fact takes part in the evidence check like any
		// other entry (PROTOCOL §6.11). Parts of frames still incomplete are stored after the
		// check (a transition discards stored parts).
		entries, results, final, frames := j.frames(req.Entries)
		if j.err != nil {
			return j.err
		}
		if err := j.evidence(entries, final, req.SameTimeline); err != nil {
			return err
		}
		j.storeParts(req.Entries, results, final, frames)
		if j.err != nil {
			return j.err
		}
		if j.rev, err = acct.NextRevision(); err != nil {
			return err
		}
		// Entries join in the order their dependencies allow: an entry whose
		// dependency another entry of the request supplies waits for it
		// (clients upload in causal order; this keeps a request whole when
		// they do not).
		var pending []int
		for i := range entries {
			if !final[i] {
				pending = append(pending, i)
			}
		}
		for progress := true; progress && len(pending) > 0; {
			progress = false
			var again []int
			for _, i := range pending {
				res := j.join(entries[i])
				if j.err != nil {
					return j.err
				}
				if res.Status == "dependency_missing" {
					again = append(again, i)
				} else {
					progress = true
				}
				results[i] = res
			}
			pending = again
		}
		if cheap && j.changed {
			return errWouldJoin
		}
		for _, i := range pending {
			j.count(results[i])
		}
		// An assembled frame is done with once its fact is joined (or known
		// or refused); one waiting for a dependency keeps its parts, so a
		// later request completes it.
		j.settleFrames(req.Entries, results, frames)
		if j.err != nil {
			return j.err
		}
		b := &commitBuilder{key: keys[0], ids: map[string]bool{}, random: s.random()}
		if j.changed {
			if _, err := s.rebuildAccount(acct, req.AccountID, true, &report, b); err != nil {
				return err
			}
			for _, rec := range report.Changed {
				b.record(rec)
			}
		}
		before := len(b.decisions)
		if err := s.endSteps(acct, req.AccountID, j.rev, now, b, req.Steps); err != nil {
			return err
		}
		made := slices.Clone(b.decisions[before:]) // the end steps' decisions
		add := j.counts
		if cheap && resolved(made) {
			return errWouldJoin
		}
		if resolved(made) {
			// A provisional resolution made now lets the registration's
			// dependents (its own archive's listening and favorites, replayed
			// before it existed) take effect: the account is derived again
			// with it before anything is published.
			again := RebuildReport{}
			if _, err := s.rebuildAccount(acct, req.AccountID, true, &again, b); err != nil {
				return err
			}
			for _, r := range again.Changed {
				b.record(r)
			}
			report.Mismatches = append(report.Mismatches, again.Mismatches...)
		}
		// No commit publishes keys that differ from the replay's order, a
		// commit of recovery uploads only included (PROTOCOL §7.6).
		namespace, err := acct.Namespace()
		if err != nil {
			return err
		}
		// A request publishes keys at its end steps (the reference's
		// recovery_end; every API request carries them): recovered entries
		// inside a request publish no layout.
		if err := s.publishQueue(&ApplyContext{Account: acct, AccountID: req.AccountID, Namespace: namespace, NormalizedTime: now,
			ReceiveTime: now, Revision: j.rev, commit: b}, j.changed, req.Steps != RecoveryEndSteps{}); err != nil {
			return err
		}
		// The commit carries each changed record as it is stored now.
		if err := settleRecords(acct, b); err != nil {
			return err
		}
		b.accepted = append(b.accepted, j.accepted...)
		b.decisions = append(b.decisions, j.decisions...)
		if len(b.accepted) > 0 || len(b.decisions) > 0 || len(b.records) > 0 {
			if _, _, err := acct.AppendCommit(b.body()); err != nil {
				return err
			}
		}
		add.Mismatches = int64(len(report.Mismatches))
		if err := recordProblems(acct, rec, j.problems, add, j.now, true); err != nil {
			return err
		}
		head, err := acct.Head()
		if err != nil {
			return err
		}
		cursor, err := contract.Cursor{LogID: head.LogID, Epoch: head.Epoch, Seq: head.Seq, Digest: head.Digest}.Encode()
		if err != nil {
			return err
		}
		if err := job.publish(tx); err != nil {
			return err
		}
		resp = &RecoveryUploadResponse{Head: cursor, Results: results}
		finished, finalHead = b, head
		return nil
	})
	if err != nil {
		return nil, err
	}
	if finished != nil {
		finished.runAfter(finalHead)
	}
	if n := len(report.Mismatches); n > 0 {
		slog.Info("recovery: replay results differ from receipts; the receipts decide", "account", req.AccountID, "count", n,
			"first", report.Mismatches[:min(n, 3)])
	}
	return resp, nil
}

// uploadable authorizes an upload request in its transaction and returns its
// recovery: open, the uploader's, and no import reserving the account.
func (s *Service) uploadable(tx *storage.WriteTx, req RecoveryUploadRequest, authorize Authorize) (storage.Recovery, error) {
	inst, err := authorize(tx)
	if err != nil {
		return storage.Recovery{}, err
	}
	if inst.InstallID != req.InstallID {
		return storage.Recovery{}, protoErr(403, "forbidden", "install_id is not this credential's installation.")
	}
	acct := tx.Account(req.AccountID)
	rec, err := acct.Recovery(req.RecoveryID)
	if errors.Is(err, storage.ErrNotFound) {
		return rec, errRecoveryNotOpen
	} else if err != nil {
		return rec, err
	}
	if rec.InstallID != req.InstallID {
		return rec, protoErr(403, "forbidden", "This recovery belongs to another installation.")
	}
	if reserved, err := acct.ImportReserved(); err != nil {
		return rec, err
	} else if reserved {
		return rec, &Error{Status: 503, Code: "server_busy", Message: "This account's library is being imported.", RetryAfter: 30 * time.Second}
	}
	return rec, nil
}

// recoveryTransition runs a staged upload's epoch transition live: in one short transaction, the
// request's entries are verified and its completing frames assembled in memory, as the upload
// does, and the evidence check runs the transition. The transition discards stored parts; the
// stored parts of the frames this request names are kept again, so the staged upload that
// follows completes them.
func (s *Service) recoveryTransition(ctx context.Context, req RecoveryUploadRequest, authorize Authorize,
	keys []storage.ReceiptKey, job *uploadHooks) (timelineFence, error) {
	var fence timelineFence
	err := s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if _, err := s.uploadable(tx, req, authorize); err != nil {
			return err
		}
		acct := tx.Account(req.AccountID)
		j := &joiner{s: s, tx: tx, acct: acct, accountID: req.AccountID, installID: req.InstallID, recoveryID: req.RecoveryID, keys: keys,
			now: s.Now().UnixMilli()}
		entries, _, final, frames := j.frames(req.Entries)
		if j.err != nil {
			return j.err
		}
		type keptFrame struct {
			slot  int64
			parts []storage.PartRow
		}
		kept := map[string]keptFrame{}
		for id := range frames {
			slot, err := acct.RecoveryFrameSlot(req.InstallID, id)
			if err != nil {
				return err
			}
			if slot == 0 {
				continue
			}
			parts, err := acct.Parts(req.InstallID, slot)
			if err != nil {
				return err
			}
			kept[id] = keptFrame{slot, parts}
		}
		before, err := acct.Head()
		if err != nil {
			return err
		}
		if err := j.evidence(entries, final, req.SameTimeline); err != nil {
			return err
		}
		after, err := acct.Head()
		if err != nil {
			return err
		}
		if fence.version, err = acct.CredentialsVersion(); err != nil {
			return err
		}
		fence.epoch = after.Epoch
		if err := job.transitioned(acct, fence); err != nil {
			return err
		}
		if after.Epoch == before.Epoch {
			return nil
		}
		for id, f := range kept {
			if err := acct.RestoreRecoveryFrame(req.InstallID, id, req.RecoveryID, f.slot, j.now, f.parts); err != nil {
				return err
			}
		}
		return nil
	})
	return fence, err
}

// timelineFence is what an uploader's own epoch transition leaves as its
// authority: the account's credentials version right after it, and the
// timeline it made (the account's epoch then; epochs are never reused).
type timelineFence struct {
	version, epoch int64
}

// installationAuthorize authorizes an uploader after the epoch transition
// its own request ran, which signed every device out: the account is still
// on the timeline that transition made (no later transition, which signed
// every device out again), its installation is still the account's, not
// revoked and not superseded, the account is active, and its credentials
// version is still the fence's (no password change or reset since).
func installationAuthorize(accountID, installID string, fence timelineFence) Authorize {
	return func(tx *storage.WriteTx) (*storage.Installation, error) {
		acct := tx.Account(accountID)
		if head, err := acct.Head(); err != nil {
			return nil, err
		} else if head.Epoch != fence.epoch {
			return nil, protoErr(401, "unauthorized", "Sign in again.")
		}
		inst, err := acct.Installation(installID)
		if errors.Is(err, storage.ErrNotFound) {
			return nil, protoErr(401, "unauthorized", "Sign in again.")
		} else if err != nil {
			return nil, err
		}
		if inst.RevokedAt != nil {
			return nil, protoErr(401, "unauthorized", "Sign in again.")
		}
		if inst.SupersededBy != "" {
			return nil, protoErr(409, "installation_superseded", "This installation was replaced; use its successor.")
		}
		if state, err := acct.State(); err != nil {
			return nil, err
		} else if state != "active" {
			return nil, protoErr(403, "forbidden", "This account is not active.")
		}
		if v, err := acct.CredentialsVersion(); err != nil {
			return nil, err
		} else if v != fence.version {
			return nil, protoErr(401, "unauthorized", "Sign in again.")
		}
		return &inst, nil
	}
}

// endSteps runs the end-of-request steps in the request's commit, after
// the rebuild, as a server commit's decisions are made.
func (s *Service) endSteps(acct *storage.AccountTx, accountID string, rev storage.Revision, now int64, b *commitBuilder,
	steps RecoveryEndSteps) error {
	fn := s.Registry.recoveryEnd
	if fn == nil || steps == (RecoveryEndSteps{}) {
		return nil
	}
	namespace, err := acct.Namespace()
	if err != nil {
		return err
	}
	template := ApplyContext{Account: acct, AccountID: accountID, Namespace: namespace, NormalizedTime: now, ReceiveTime: now,
		Revision: rev, commit: b, batch: true}
	c := template
	c.flush = func() error { return flushDirty(b, template) }
	if err := fn(&c, steps); err != nil {
		return err
	}
	return flushDirty(b, template)
}

// RecoveryEnd runs end-of-request steps on their own (a request boundary
// with nothing joined) as a server commit.
func (s *Service) RecoveryEnd(ctx context.Context, accountID string, steps RecoveryEndSteps) error {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return fmt.Errorf("protocol: no receipt key: %v", err)
	}
	now := s.Now().UnixMilli()
	var finished *commitBuilder
	var head storage.Head
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		finished = nil
		acct := tx.Account(accountID)
		rev, err := acct.NextRevision()
		if err != nil {
			return err
		}
		b := &commitBuilder{key: keys[0], ids: map[string]bool{}, random: s.random()}
		if err := s.endSteps(acct, accountID, rev, now, b, steps); err != nil {
			return err
		}
		namespace, err := acct.Namespace()
		if err != nil {
			return err
		}
		if err := s.publishQueue(&ApplyContext{Account: acct, AccountID: accountID, Namespace: namespace, NormalizedTime: now, ReceiveTime: now,
			Revision: rev, commit: b}, false, true); err != nil {
			return err
		}
		if len(b.decisions) > 0 || len(b.records) > 0 {
			if _, _, err = acct.AppendCommit(b.body()); err != nil {
				return err
			}
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
	return err
}

// joiner verifies and inserts one request's entries.
type joiner struct {
	s         *Service
	tx        *storage.WriteTx
	acct      *storage.AccountTx
	accountID string
	installID string
	// recoveryID is the recovery the request belongs to: its frames are
	// discarded when it finishes.
	recoveryID string
	keys       []storage.ReceiptKey
	now        int64
	rev        storage.Revision
	err        error

	changed bool // an entry was joined or a receipt replaced
	// cheap refuses lost-timeline evidence with errWouldJoin rather than
	// running the transition (recoveryUploadCheap).
	cheap     bool
	accepted  []map[string]any // the commit's recovered facts with their original receipts
	decisions []map[string]any // the commit's recovered decisions as recorded
	counts    RecoveryCounts
	problems  []RecoveryProblem
}

func (j *joiner) fail(err error) {
	if j.err == nil {
		j.err = err
	}
}

// verified is a parsed, MAC-verified entry.
type verified struct {
	kind     string
	id       string
	fact     *Archived
	receipt  Receipt
	rawRcpt  map[string]any
	decision map[string]any
	drow     storage.DecisionRow
	status   string // a final status found while verifying ("" to join)
}

// verify parses an entry and checks its MAC (PROTOCOL §6.11).
func (j *joiner) verify(e RecoveryEntry) verified {
	switch {
	case e.Decision != nil:
		return j.verifyDecision(e.Decision)
	case e.Fact != nil:
		return j.verifyFact(e.Fact, e.Receipt)
	}
	return verified{kind: "fact", status: "invalid"}
}

func (j *joiner) key(id string) []byte {
	for _, k := range j.keys {
		if k.ID == id {
			return k.Secret
		}
	}
	return nil
}

var receiptMembers = map[string]bool{"op_id": true, "content_hash": true, "outcome": true, "reason": true, "normalized_time": true,
	"epoch": true, "seq": true, "index": true, "frontier": true, "sort_values": true, "key_id": true, "mac": true}

func (j *joiner) verifyFact(a *Archived, raw map[string]any) verified {
	v := verified{kind: "fact", id: a.OpID, fact: a, rawRcpt: raw}
	if raw == nil || ContainsCredentialURL(a.Object) {
		v.status = "invalid"
		return v
	}
	for k := range raw {
		if !receiptMembers[k] {
			v.status = "invalid"
			return v
		}
	}
	keyID, _ := raw["key_id"].(string)
	secret := j.key(keyID)
	if secret == nil || !contract.VerifyMAC(secret, j.accountID, raw) {
		v.status = "invalid_receipt"
		return v
	}
	r, err := parseReceipt(mustCanonical(raw))
	if err != nil {
		v.status = "invalid"
		return v
	}
	r.ContentHash, _ = raw["content_hash"].(string)
	r.KeyID = keyID
	if r.OpID != a.OpID || r.ContentHash != a.ContentHash {
		v.status = "invalid_receipt" // a receipt of another fact or content
		return v
	}
	switch r.Outcome {
	case "applied", "superseded", "not_applied", "rejected":
	default:
		v.status = "invalid"
		return v
	}
	if r.Epoch < 1 || r.Seq < 1 || r.Index < 0 || (r.Outcome == "rejected" && r.Frontier == nil) ||
		(a.Fact == nil && r.Outcome != "rejected") {
		v.status = "invalid"
		return v
	}
	v.receipt = r
	return v
}

var decisionMembers = map[string]bool{"decision_id": true, "decision_type": true, "decision_version": true, "dependencies": true,
	"normalized_time": true, "epoch": true, "seq": true, "index": true, "subject": true, "inputs": true, "result": true,
	"key_id": true, "mac": true}

func (j *joiner) verifyDecision(d map[string]any) verified {
	id, _ := d["decision_id"].(string)
	v := verified{kind: "decision", id: id, decision: d}
	for k := range d {
		if !decisionMembers[k] {
			v.status = "invalid"
			return v
		}
	}
	keyID, _ := d["key_id"].(string)
	secret := j.key(keyID)
	if !contract.ValidUUID(id) {
		v.status = "invalid"
		return v
	}
	if secret == nil || !contract.VerifyMAC(secret, j.accountID, d) {
		v.status = "invalid_receipt"
		return v
	}
	typ, _ := d["decision_type"].(string)
	subject, _ := d["subject"].(string)
	epoch, ok1 := integer(d["epoch"])
	seq, ok2 := integer(d["seq"])
	nt, ok3 := integer(d["normalized_time"])
	_, okInputs := d["inputs"].(map[string]any)
	_, okResult := d["result"].(map[string]any)
	deps, okDeps := d["dependencies"].([]any)
	if !ok1 || !ok2 || !ok3 || !okInputs || !okResult || !okDeps || epoch < 1 || seq < 1 || j.s.Registry.admit(typ) == nil {
		v.status = "invalid"
		return v
	}
	for _, x := range deps {
		if _, ok := x.(string); !ok {
			v.status = "invalid"
			return v
		}
	}
	canonical, err := contract.Canonical(d)
	if err != nil {
		v.status = "invalid"
		return v
	}
	v.drow = storage.DecisionRow{DecisionID: id, DecisionType: typ, Subject: subject, Epoch: epoch, Seq: seq, NormalizedTime: nt,
		Decision: canonical}
	return v
}

func mustCanonical(v any) []byte {
	b, _ := contract.Canonical(v)
	return b
}

// evidence runs the restore transition before anything is joined when a
// verified entry the archive does not hold names an epoch at or above the
// account's current one (PROTOCOL §6.11). Conflicts, cross-kind included,
// are no evidence: a conflict changes nothing (PROTOCOL §6.11).
func (j *joiner) evidence(entries []RecoveryEntry, final []bool, sameTimeline bool) error {
	head, err := j.acct.Head()
	if err != nil {
		return err
	}
	top := int64(0)
	for i, e := range entries {
		if final[i] || e.Part != nil {
			continue
		}
		v := j.verify(e)
		if v.status != "" || (sameTimeline && v.kind == "fact") {
			continue
		}
		epoch := v.receipt.Epoch
		if v.kind == "decision" {
			epoch = v.drow.Epoch
			if _, err := j.acct.ArchivedDecision(v.id); err == nil {
				continue // known, or a conflict
			} else if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			if _, err := j.acct.Fact(v.id); err == nil {
				continue // the ID of a fact: a conflict
			} else if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		} else {
			if _, err := j.acct.ArchivedDecision(v.id); err == nil {
				continue // the ID of a decision: a conflict
			} else if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			if known, err := j.acct.Fact(v.id); err == nil {
				r := v.receipt
				if known.ContentHash != v.fact.ContentHash || (r.Epoch == known.Epoch && r.Seq == known.Seq && r.Index == known.ReceiptIndex) {
					continue // a conflict, or the archived receipt
				}
				// An earlier receipt replaces the archived one and a later one
				// is kept: either is evidence unless the archive already holds
				// it.
				if sup, err := j.acct.SupersededReceipt(v.id, r.Epoch, r.Seq, r.Index); err != nil {
					return err
				} else if sup {
					continue
				}
			} else if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
		if epoch >= head.Epoch && epoch > top {
			top = epoch
		}
	}
	if top == 0 {
		return nil
	}
	if j.cheap {
		return errWouldJoin
	}
	return j.tx.EvidenceTransition(top)
}

// positionLess reports whether a receipt's position is before an archived
// fact's: (epoch, seq, index) (the op_id is the same).
func positionLess(r Receipt, f storage.FactRow) bool {
	if r.Epoch != f.Epoch {
		return r.Epoch < f.Epoch
	}
	if r.Seq != f.Seq {
		return r.Seq < f.Seq
	}
	return r.Index < f.ReceiptIndex
}

// join verifies and joins one entry.
func (j *joiner) join(e RecoveryEntry) RecoveryResult {
	v := j.verify(e)
	res := RecoveryResult{Kind: v.kind, ID: v.id, Status: v.status}
	if res.Status == "" {
		if v.kind == "decision" {
			res.Status, res.Missing = j.joinDecision(v)
		} else {
			res.Status, res.Missing = j.joinFact(v)
		}
	}
	if res.Status != "dependency_missing" {
		j.count(res)
	}
	return res
}

func (j *joiner) count(res RecoveryResult) {
	c := &j.counts
	switch res.Status {
	case "joined":
		c.Joined++
	case "known":
		c.Known++
	case "replaced":
		c.Replaced++
	case "dependency_missing":
		c.DependencyMissing++
	case "invalid_receipt":
		c.InvalidReceipt++
	case "invalid":
		c.Invalid++
	case "conflict":
		c.Conflict++
	}
	if res.Status == "invalid_receipt" || res.Status == "conflict" {
		j.problems = append(j.problems, RecoveryProblem{Kind: res.Kind, ID: res.ID, Status: res.Status})
	}
}

// available reports whether an ID is, or will be after the rebuild, a
// causal node or an archived fact (a dependency the joined entry may name).
func (j *joiner) available(id string, rejectedToo bool) (bool, error) {
	if _, err := j.acct.NodeTime(id); err == nil {
		return true, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return false, err
	}
	if f, err := j.acct.Fact(id); err == nil {
		if rejectedToo || f.Outcome != "rejected" {
			return true, nil
		}
		// A fact a kept provisional receipt admitted is a node once the
		// request's rebuild runs.
		return j.acct.AdmittedByReceipts(id)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return false, err
	}
	if _, err := j.acct.ArchivedDecision(id); err == nil {
		return true, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return false, err
	}
	return false, nil
}

func (j *joiner) joinFact(v verified) (string, []string) {
	acct, a, r := j.acct, v.fact, v.receipt
	if _, err := acct.ArchivedDecision(a.OpID); err == nil {
		return "conflict", nil // the ID of a decision (PROTOCOL §6.11)
	} else if !errors.Is(err, storage.ErrNotFound) {
		j.fail(err)
		return "", nil
	}
	receipt := mustCanonical(v.rawRcpt)
	known, err := acct.Fact(a.OpID)
	switch {
	case err == nil:
		if known.ContentHash != a.ContentHash {
			return "conflict", nil // PROTOCOL §6.11: never replaces a receipt
		}
		if known.Epoch == r.Epoch && known.Seq == r.Seq && known.ReceiptIndex == r.Index {
			if !bytes.Equal(known.Receipt, receipt) {
				return "conflict", nil
			}
			return "known", nil
		}
		if sup, err := acct.SupersededReceipt(a.OpID, r.Epoch, r.Seq, r.Index); err != nil {
			j.fail(err)
			return "", nil
		} else if sup {
			return "known", nil // a superseded receipt
		}
		if !positionLess(r, known) {
			// A later receipt that admitted a fact the archive's receipt
			// rejects (a provisional acceptance of a lost timeline) is kept as
			// superseded, as if it had been archived first and the original
			// had replaced it: the fact is a node without effects (PROTOCOL §6.11)
			// from this commit, whatever the upload order. Any other later receipt is known.
			// A receipt kept now is published in the request's commit like any
			// recovered entry, so pull and snapshots carry it, and the account
			// is derived again: a node's causal time is the earliest of its
			// admitting receipts, a function of the archive whatever the
			// request order.
			kept, _, err := acct.KeepReceipt(a.OpID, r.Epoch, r.Seq, r.Index, receipt, r.Outcome != "rejected", [2]int64{j.rev.Epoch, j.rev.Seq}, j.now)
			if err != nil {
				j.fail(err)
				return "", nil
			}
			if kept {
				j.changed = true
				j.accepted = append(j.accepted, map[string]any{"fact": a.Object, "receipt": v.rawRcpt})
			}
			return "known", nil
		}
		// PROTOCOL §6.11: the original replaces the provisional receipt.
		row := j.factRow(a, r, receipt)
		row.NodePubEpoch, row.NodePubSeq = j.rev.Epoch, j.rev.Seq
		if err := acct.ReplaceReceipt(row, j.now); err != nil {
			j.fail(err)
			return "", nil
		}
		j.changed = true
		j.accepted = append(j.accepted, map[string]any{"fact": a.Object, "receipt": v.rawRcpt})
		return "replaced", nil
	case !errors.Is(err, storage.ErrNotFound):
		j.fail(err)
		return "", nil
	}
	// Not archived: its dependencies (as push: nodes or archived rejected
	// facts) and previous fact must be held; a rejected receipt's frontier
	// must be nodes.
	var missing []string
	need := func(id string, rejectedToo bool) {
		ok, err := j.available(id, rejectedToo)
		if err != nil {
			j.fail(err)
		}
		if !ok {
			missing = append(missing, id)
		}
	}
	if r.Outcome == "rejected" {
		for _, id := range r.Frontier {
			need(id, false)
		}
	} else if a.Fact != nil {
		for _, d := range a.Fact.Dependencies {
			need(d, true)
		}
	}
	if a.Fact != nil && a.Fact.PreviousOpID != "" {
		need(a.Fact.PreviousOpID, true)
	}
	if j.err != nil {
		return "", nil
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return "dependency_missing", slices.Compact(missing)
	}
	row := j.factRow(a, r, receipt)
	row.PubEpoch, row.PubSeq = j.rev.Epoch, j.rev.Seq
	row.NodePubEpoch, row.NodePubSeq = j.rev.Epoch, j.rev.Seq
	if err := acct.InsertFact(row); err != nil {
		j.fail(err)
		return "", nil
	}
	j.changed = true
	j.accepted = append(j.accepted, map[string]any{"fact": a.Object, "receipt": v.rawRcpt})
	return "joined", nil
}

func (j *joiner) factRow(a *Archived, r Receipt, receipt []byte) storage.FactRow {
	frontier := "[]"
	if r.Outcome == "rejected" {
		list := make([]any, len(r.Frontier))
		for i, id := range r.Frontier {
			list[i] = id
		}
		frontier = string(mustCanonical(list))
	}
	typ, version := "", int64(0)
	if v, ok := a.Object["type"].(string); ok {
		typ = v
	}
	if n, ok := integer(a.Object["type_version"]); ok {
		version = n
	}
	return storage.FactRow{OpID: a.OpID, ContentHash: a.ContentHash, Fact: a.Canonical, Receipt: receipt, Type: typ, TypeVersion: version,
		Outcome: r.Outcome, Frontier: frontier, Epoch: r.Epoch, Seq: r.Seq, ReceiptIndex: r.Index}
}

func (j *joiner) joinDecision(v verified) (string, []string) {
	acct := j.acct
	if known, err := acct.Fact(v.id); err == nil && known.OpID != "" {
		return "conflict", nil
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		j.fail(err)
		return "", nil
	}
	if known, err := acct.ArchivedDecision(v.id); err == nil {
		if !bytes.Equal(known.Decision, v.drow.Decision) {
			return "conflict", nil
		}
		return "known", nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		j.fail(err)
		return "", nil
	}
	var missing []string
	deps, _ := v.decision["dependencies"].([]any)
	for _, x := range deps {
		id, _ := x.(string)
		ok, err := j.available(id, false)
		if err != nil {
			j.fail(err)
			return "", nil
		}
		if !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return "dependency_missing", slices.Compact(missing)
	}
	row := v.drow
	row.PubEpoch, row.PubSeq = j.rev.Epoch, j.rev.Seq
	if err := acct.InsertDecision(row); err != nil {
		j.fail(err)
		return "", nil
	}
	j.changed = true
	j.decisions = append(j.decisions, v.decision)
	return "joined", nil
}

// frame is a framing transaction of one request.
type frame struct {
	tx      string
	entries []int // the request's part entries, in order
	done    int   // the entry that carries the assembled fact (-1: incomplete)
	slot    int64 // its stored parts' slot when the request began (0: none)
}

// MaxRecoveryFrames and MaxRecoveryFrameBytes bound an installation's open
// recovery frames (incomplete, or complete and waiting for a dependency) and
// the plaintext bytes of their stored parts.
// A client uploads a frame's parts in one request or a few consecutive ones,
// so a correct client holds one or two.
const (
	MaxRecoveryFrames     = 16
	MaxRecoveryFrameBytes = 4 * MaxFramedFact
)

// RecoveryFrameLifetime is how long an unused recovery frame is kept: an
// abandoned upload's parts expire.
const RecoveryFrameLifetime = 24 * time.Hour

// verifyPart checks a part's receipt (PROTOCOL §6.11): its MAC
// and that it names the frame's content.
func (j *joiner) verifyPart(e RecoveryEntry) string {
	raw := e.Receipt
	if raw == nil {
		return "invalid"
	}
	for k := range raw {
		if !receiptMembers[k] {
			return "invalid"
		}
	}
	keyID, _ := raw["key_id"].(string)
	secret := j.key(keyID)
	if secret == nil || !contract.VerifyMAC(secret, j.accountID, raw) {
		return "invalid_receipt"
	}
	if h, _ := raw["content_hash"].(string); h != e.Part.ContentHash {
		return "invalid_receipt"
	}
	return ""
}

// frames verifies every part entry's receipt and assembles in memory each
// frame this request completes with the parts already stored: its last part
// entry carries the fact (with that part's receipt), its other part entries
// answer part_stored. It returns the entries to check and join, the results
// so far, which entries are final, and the frames by transaction.
func (j *joiner) frames(in []RecoveryEntry) ([]RecoveryEntry, []RecoveryResult, []bool, map[string]*frame) {
	entries := slices.Clone(in)
	results := make([]RecoveryResult, len(in))
	final := make([]bool, len(in))
	frames := map[string]*frame{}
	var order []string
	for i, e := range in {
		if e.Part == nil {
			continue
		}
		res := RecoveryResult{Kind: "part", ID: e.Part.TransactionID}
		if st := j.verifyPart(e); st != "" {
			res.Status = st
			results[i], final[i] = res, true
			j.count(res)
			continue
		}
		f := frames[e.Part.TransactionID]
		if f == nil {
			f = &frame{tx: e.Part.TransactionID, done: -1}
			frames[f.tx] = f
			order = append(order, f.tx)
		}
		f.entries = append(f.entries, i)
	}
	for _, tx := range order {
		f := frames[tx]
		seq, err := j.acct.RecoveryFrameSlot(j.installID, tx)
		if err != nil {
			j.fail(err)
			return nil, nil, nil, nil
		}
		var stored []storage.PartMeta
		if seq != 0 {
			if stored, err = j.acct.PartsMeta(j.installID, seq); err != nil {
				j.fail(err)
				return nil, nil, nil, nil
			}
		}
		f.slot = seq
		first := in[f.entries[0]].Part
		isStored := map[int]bool{}
		bad := false
		for _, m := range stored {
			if m.TransactionID != tx || m.Count != first.Count || m.ContentHash != first.ContentHash {
				bad = true
			}
			isStored[m.Index] = true
		}
		// Each index counts once; a part repeated in the request must be
		// identical, as one repeating a stored part must
		// (checked when assembling).
		sent := map[int][]byte{}
		for _, i := range f.entries {
			p := in[i].Part
			if p.Count != first.Count || p.ContentHash != first.ContentHash {
				bad = true
			}
			if d, dup := sent[p.Index]; dup {
				if !bytes.Equal(d, p.Data) {
					bad = true
				}
				continue
			}
			sent[p.Index] = p.Data
		}
		have := len(sent)
		for idx := range isStored {
			if _, ok := sent[idx]; !ok {
				have++
			}
		}
		if bad {
			for _, i := range f.entries {
				results[i], final[i] = RecoveryResult{Kind: "part", ID: tx, Status: "invalid_part"}, true
			}
			continue
		}
		if have < first.Count {
			continue // stored after the evidence check
		}
		// Complete: assemble the stored parts and this request's.
		var buf bytes.Buffer
		p := &pusher{s: j.s, acct: j.acct, accountID: j.accountID, inst: &storage.Installation{InstallID: j.installID}}
		differs := false
		for idx := 0; idx < first.Count; idx++ {
			data, ok := sent[idx]
			if isStored[idx] {
				raw, err := j.acct.PartData(j.installID, seq, idx)
				var kept []byte
				if err == nil {
					kept, err = p.openPart(seq, idx, raw)
				}
				if err != nil {
					j.fail(err)
					return nil, nil, nil, nil
				}
				if ok && !bytes.Equal(kept, data) {
					differs = true
				}
				data = kept
			}
			buf.Write(data)
		}
		last := f.entries[len(f.entries)-1]
		archived, ok := reassemble(buf.Bytes(), first.ContentHash)
		if differs || !ok || buf.Len() > MaxFramedFact || ContainsCredentialURL(archived.Object) {
			// It can never complete: discard it, so it can be framed again.
			if err := j.acct.DropRecoveryFrame(j.installID, tx); err != nil {
				j.fail(err)
			}
			for _, i := range f.entries {
				results[i], final[i] = RecoveryResult{Kind: "part", ID: tx, Status: "invalid_part"}, true
			}
			continue
		}
		f.done = last
		entries[last] = RecoveryEntry{Fact: archived, Receipt: in[last].Receipt}
		present := make([]int, first.Count)
		for k := range present {
			present[k] = k
		}
		for _, i := range f.entries {
			if i != last {
				results[i], final[i] = RecoveryResult{Kind: "part", ID: tx, Status: "part_stored", PartsPresent: present}, true
			}
		}
	}
	return entries, results, final, frames
}

// storeParts stores the parts of frames this request leaves incomplete (as
// push's parts; PROTOCOL §6.10 ownership by the first framing transaction).
func (j *joiner) storeParts(in []RecoveryEntry, results []RecoveryResult, final []bool, frames map[string]*frame) {
	for _, f := range frames {
		if f.done >= 0 {
			continue
		}
		var parts []*Part
		var idx []int
		for _, i := range f.entries {
			if !final[i] {
				parts, idx = append(parts, in[i].Part), append(idx, i)
			}
		}
		if len(parts) == 0 {
			continue
		}
		slot, ok := j.makeRoom(f, parts, frames)
		if j.err != nil {
			return
		}
		if !ok {
			for _, i := range idx {
				results[i], final[i] = RecoveryResult{Kind: "part", ID: f.tx, Status: "invalid_part"}, true
			}
			continue
		}
		for k, part := range parts {
			i := idx[k]
			res := RecoveryResult{Kind: "part", ID: f.tx}
			p := &pusher{s: j.s, acct: j.acct, accountID: j.accountID, inst: &storage.Installation{InstallID: j.installID}}
			var pr PushResult
			if _, done := p.part(PushOp{InstallSeq: slot, Part: part}, &pr); p.err != nil {
				j.fail(p.err)
				return
			} else if done {
				pr.Status = "part_stored" // completed by a concurrent request: joined by the next one
			}
			res.Status, res.PartsPresent = pr.Status, pr.PartsPresent
			if res.Status == "installation_conflict" {
				res.Status = "invalid_part"
			}
			results[i], final[i] = res, true
		}
		if err := j.dropEmpty(f.tx, slot); err != nil {
			j.fail(err)
			return
		}
	}
}

// makeRoom returns the slot a frame's parts are stored in, opening the frame if
// it is new, within the installation's bounds: the least recently used other
// frames are discarded until the frame and these parts fit (a client whose
// frame was discarded sees fewer parts present and sends them again). False
// when they cannot fit beside this request's own frames.
func (j *joiner) makeRoom(f *frame, parts []*Part, frames map[string]*frame) (int64, bool) {
	stored := map[int]bool{}
	if f.slot != 0 {
		meta, err := j.acct.PartsMeta(j.installID, f.slot)
		if err != nil {
			j.fail(err)
			return 0, false
		}
		for _, m := range meta {
			stored[m.Index] = true
		}
	}
	size := 0
	for _, p := range parts {
		if !stored[p.Index] {
			stored[p.Index] = true
			size += len(p.Data)
		}
	}
	except := map[string]bool{}
	for tx := range frames {
		except[tx] = true
	}
	for {
		open, bytes, err := j.acct.RecoveryFrameUsage(j.installID)
		if err != nil {
			j.fail(err)
			return 0, false
		}
		if f.slot == 0 {
			open++
		}
		if open <= MaxRecoveryFrames && bytes+int64(size) <= MaxRecoveryFrameBytes {
			break
		}
		old, err := j.acct.OldestRecoveryFrame(j.installID, except)
		if err != nil {
			j.fail(err)
			return 0, false
		}
		if old == "" {
			return 0, false
		}
		if err := j.acct.DropRecoveryFrame(j.installID, old); err != nil {
			j.fail(err)
			return 0, false
		}
	}
	slot, err := j.acct.UseRecoveryFrame(j.installID, f.tx, j.recoveryID, j.now)
	if err != nil {
		j.fail(err)
		return 0, false
	}
	f.slot = slot
	return slot, true
}

// dropEmpty discards a frame that holds no part (all its parts were
// refused).
func (j *joiner) dropEmpty(tx string, slot int64) error {
	meta, err := j.acct.PartsMeta(j.installID, slot)
	if err != nil || len(meta) > 0 {
		return err
	}
	return j.acct.DropRecoveryFrame(j.installID, tx)
}

// settleFrames discards the stored parts of every frame whose fact this
// request joined or refused; a fact waiting for a dependency keeps its frame
// (its parts are stored now, so a later request completes it).
func (j *joiner) settleFrames(in []RecoveryEntry, results []RecoveryResult, frames map[string]*frame) {
	for _, f := range frames {
		if f.done < 0 {
			continue
		}
		if results[f.done].Status != "dependency_missing" {
			if err := j.acct.DropRecoveryFrame(j.installID, f.tx); err != nil {
				j.fail(err)
			}
			continue
		}
		var parts []*Part
		for _, i := range f.entries {
			parts = append(parts, in[i].Part)
		}
		slot, ok := j.makeRoom(f, parts, frames)
		if j.err != nil || !ok {
			continue // no room: the client uploads the frame again with its dependency
		}
		p := &pusher{s: j.s, acct: j.acct, accountID: j.accountID, inst: &storage.Installation{InstallID: j.installID}}
		for _, part := range parts {
			var pr PushResult
			p.part(PushOp{InstallSeq: slot, Part: part}, &pr)
			if p.err != nil {
				j.fail(p.err)
				return
			}
		}
		if err := j.dropEmpty(f.tx, slot); err != nil {
			j.fail(err)
			return
		}
	}
}

// expireFrames discards the recovery frames of every account unused for
// RecoveryFrameLifetime (abandoned uploads). Recovery
// requests run it in their transactions; it reads only the few stored
// recovery parts.
func (s *Service) expireFrames(tx *storage.WriteTx) error {
	_, err := tx.ExpireRecoveryFrames(s.Now().Add(-RecoveryFrameLifetime).UnixMilli())
	return err
}

func orEmptyProblems(p []RecoveryProblem) []RecoveryProblem {
	if p == nil {
		return []RecoveryProblem{}
	}
	return p
}

// ---- snapshot, inventory and finish

// openRecovery opens or resumes a recovery (PROTOCOL §6.11; idempotent by
// recovery_id) and answers the prefix shortcut (PROTOCOL §6.11) when the device's
// cursor names a commit the restored log still has with the same digest:
// the cursor re-issued in the current epoch (nil otherwise: a full
// snapshot follows).
func (s *Service) openRecovery(ctx context.Context, req SnapshotRequest) (*SnapshotResponse, error) {
	if !contract.ValidUUID(req.RecoveryID) {
		return nil, protoErr(400, "invalid_request", "recovery_id must be a UUID.")
	}
	release, err := s.holdLive(req.AccountID)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.jobPoint("open"); err != nil {
		return nil, err
	}
	var out *SnapshotResponse
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		out = nil
		if err := authorized(tx, req.Authorize, req.InstallID); err != nil {
			return err
		}
		acct := tx.Account(req.AccountID)
		if rec, err := acct.Recovery(req.RecoveryID); err == nil && rec.InstallID != req.InstallID {
			return protoErr(403, "forbidden", "This recovery belongs to another installation.")
		} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if _, err := acct.OpenRecovery(req.RecoveryID, req.InstallID, s.Now().UnixMilli()); err != nil {
			return err
		}
		if req.Cursor == nil {
			return nil
		}
		head, err := acct.Head()
		if err != nil {
			return err
		}
		c := *req.Cursor
		if c.LogID != head.LogID || c.Seq > head.Seq {
			return nil
		}
		d, err := acct.CommitDigest(c.Seq)
		if err != nil || !bytes.Equal(d, c.Digest) {
			return nil
		}
		reissued, err := contract.Cursor{LogID: head.LogID, Epoch: head.Epoch, Seq: c.Seq, Digest: c.Digest}.Encode()
		if err != nil {
			return err
		}
		headCursor, err := contract.Cursor{LogID: head.LogID, Epoch: head.Epoch, Seq: head.Seq, Digest: head.Digest}.Encode()
		if err != nil {
			return err
		}
		watermark, err := acct.ConsumedSeq(req.InstallID)
		if err != nil {
			return err
		}
		out = &SnapshotResponse{Prefix: true, Cursor: reissued, Head: headCursor, InstallWatermark: watermark}
		return nil
	})
	return out, err
}

// InventoryEntry is one entry of a recovery inventory (PROTOCOL §6.11).
type InventoryEntry struct {
	Kind        string // fact or decision
	ID          string
	ContentHash string   // a fact's
	Position    [3]int64 // a fact's receipt (epoch, seq, index)
	Rank        [2]int64 // a decision's (epoch, seq)
}

// InventoryResult is one inventory answer: have, need or conflict.
type InventoryResult struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Status string `json:"status"`
}

// RecoveryInventory compares a device's entries with the archive (PROTOCOL §6.11): have (held with
// this receipt, an earlier one, or as a known superseded receipt), need (absent, or held under a
// later provisional receipt) or conflict (same ID, different content; never uploaded).
func (s *Service) RecoveryInventory(ctx context.Context, accountID, installID, recoveryID string, entries []InventoryEntry,
	authorize Authorize) ([]InventoryResult, error) {
	release, err := s.holdLive(accountID)
	if err != nil {
		return nil, err
	}
	defer release()
	out := make([]InventoryResult, len(entries))
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if err := authorized(tx, authorize, installID); err != nil {
			return err
		}
		acct := tx.Account(accountID)
		rec, err := acct.Recovery(recoveryID)
		if errors.Is(err, storage.ErrNotFound) {
			return errRecoveryNotOpen
		} else if err != nil {
			return err
		}
		if rec.InstallID != installID {
			return protoErr(403, "forbidden", "This recovery belongs to another installation.")
		}
		var conflicts []RecoveryProblem
		for i, e := range entries {
			res := InventoryResult{Kind: e.Kind, ID: e.ID}
			if e.Kind == "decision" {
				res.Status, err = inventoryDecision(acct, e)
			} else {
				res.Status, err = inventoryFact(acct, e)
			}
			if err != nil {
				return err
			}
			if res.Status == "conflict" {
				conflicts = append(conflicts, RecoveryProblem{Kind: e.Kind, ID: e.ID, Status: "conflict"})
			}
			out[i] = res
		}
		// Conflicts are recorded, so finish and diagnostics show them.
		return recordProblems(acct, rec, conflicts, RecoveryCounts{Conflict: int64(len(conflicts))}, s.Now().UnixMilli(), false)
	})
	return out, err
}

// authorized runs a caller's transactional authorization (nil: none) and
// checks the installation.
func authorized(tx *storage.WriteTx, authorize Authorize, installID string) error {
	if authorize == nil {
		return nil
	}
	inst, err := authorize(tx)
	if err != nil {
		return err
	}
	if inst.InstallID != installID {
		return protoErr(403, "forbidden", "install_id is not this credential's installation.")
	}
	return nil
}

// recordProblems adds counts and problems to a recovery session. A recovery
// with recorded problems is never complete (PROTOCOL §6.11: the client keeps such
// entries and does not claim full recovery), whenever they are recorded.
func recordProblems(acct *storage.AccountTx, rec storage.Recovery, problems []RecoveryProblem, add RecoveryCounts, now int64,
	request bool) error {
	var counts RecoveryCounts
	_ = json.Unmarshal([]byte(rec.Counts), &counts)
	if request {
		counts.Requests++
	}
	counts.Joined += add.Joined
	counts.Known += add.Known
	counts.Replaced += add.Replaced
	counts.DependencyMissing += add.DependencyMissing
	counts.InvalidReceipt += add.InvalidReceipt
	counts.Invalid += add.Invalid
	counts.Conflict += add.Conflict
	counts.Mismatches += add.Mismatches
	var all []RecoveryProblem
	_ = json.Unmarshal([]byte(rec.Problems), &all)
	for _, p := range problems {
		if !slices.Contains(all, p) {
			all = append(all, p)
		}
	}
	sort.Slice(all, func(a, b int) bool {
		if all[a].ID != all[b].ID {
			return all[a].ID < all[b].ID
		}
		return all[a].Kind < all[b].Kind
	})
	c, err := canonicalString(counts)
	if err != nil {
		return err
	}
	pr, err := canonicalString(orEmptyProblems(all))
	if err != nil {
		return err
	}
	return acct.UpdateRecovery(rec.RecoveryID, c, pr, now, nil)
}

func inventoryFact(acct *storage.AccountTx, e InventoryEntry) (string, error) {
	if _, err := acct.ArchivedDecision(e.ID); err == nil {
		return "conflict", nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return "", err
	}
	f, err := acct.Fact(e.ID)
	if errors.Is(err, storage.ErrNotFound) {
		return "need", nil
	} else if err != nil {
		return "", err
	}
	if f.ContentHash != e.ContentHash {
		return "conflict", nil
	}
	r := Receipt{Epoch: e.Position[0], Seq: e.Position[1], Index: e.Position[2]}
	if sup, err := acct.SupersededReceipt(e.ID, r.Epoch, r.Seq, r.Index); err != nil {
		return "", err
	} else if sup || !positionLess(r, f) {
		return "have", nil
	}
	return "need", nil // held under a later provisional receipt
}

func inventoryDecision(acct *storage.AccountTx, e InventoryEntry) (string, error) {
	if _, err := acct.Fact(e.ID); err == nil {
		return "conflict", nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return "", err
	}
	d, err := acct.ArchivedDecision(e.ID)
	if errors.Is(err, storage.ErrNotFound) {
		return "need", nil
	} else if err != nil {
		return "", err
	}
	if d.Epoch != e.Rank[0] || d.Seq != e.Rank[1] {
		return "conflict", nil
	}
	return "have", nil
}

// RecoverySummary is a recovery session as diagnostics and finish show it.
type RecoverySummary struct {
	RecoveryID string            `json:"recovery_id"`
	InstallID  string            `json:"install_id"`
	StartedAt  int64             `json:"started_at"`
	LastSeenAt int64             `json:"last_seen_at"`
	FinishedAt *int64            `json:"finished_at"`
	Complete   *bool             `json:"complete"`
	Counts     RecoveryCounts    `json:"counts"`
	Problems   []RecoveryProblem `json:"problems"`
}

// Summary reads a stored recovery session.
func Summary(r storage.Recovery) RecoverySummary {
	out := RecoverySummary{RecoveryID: r.RecoveryID, InstallID: r.InstallID, StartedAt: r.StartedAt, LastSeenAt: r.LastSeenAt,
		FinishedAt: r.FinishedAt, Complete: r.Complete, Problems: []RecoveryProblem{}}
	_ = json.Unmarshal([]byte(r.Counts), &out.Counts)
	_ = json.Unmarshal([]byte(r.Problems), &out.Problems)
	if out.Problems == nil {
		out.Problems = []RecoveryProblem{}
	}
	return out
}

// RecoveryFinish records a recovery's outcome (PROTOCOL §6.11): whether the client
// believes it complete, with the counts and problems; the end-of-request
// steps run once more (a crash may have cut the last request's). Repeating
// it is harmless.
func (s *Service) RecoveryFinish(ctx context.Context, accountID, installID, recoveryID string, complete bool,
	authorize Authorize) (RecoverySummary, error) {
	release, err := s.holdLive(accountID)
	if err != nil {
		return RecoverySummary{}, err
	}
	var out RecoverySummary
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if err := authorized(tx, authorize, installID); err != nil {
			return err
		}
		acct := tx.Account(accountID)
		rec, err := acct.Recovery(recoveryID)
		if errors.Is(err, storage.ErrNotFound) {
			return errRecoveryNotOpen
		} else if err != nil {
			return err
		}
		if rec.InstallID != installID {
			return protoErr(403, "forbidden", "This recovery belongs to another installation.")
		}
		if err := acct.UpdateRecovery(recoveryID, rec.Counts, rec.Problems, s.Now().UnixMilli(), &complete); err != nil {
			return err
		}
		// Finishing leaves none of its frames behind.
		if err := acct.DropRecoveryFramesOf(recoveryID); err != nil {
			return err
		}
		if err := s.expireFrames(tx); err != nil {
			return err
		}
		if rec, err = acct.Recovery(recoveryID); err != nil {
			return err
		}
		out = Summary(rec)
		return nil
	})
	release()
	if err != nil {
		return out, err
	}
	return out, s.RecoveryEnd(ctx, accountID, RecoveryEndSteps{Resolutions: true, Unify: true})
}

// resolved reports whether decisions include a podcast.resolution.
func resolved(made []map[string]any) bool {
	for _, d := range made {
		if d["decision_type"] == "podcast.resolution" {
			return true
		}
	}
	return false
}

// settleRecords makes each record of a commit what is stored now (a
// recovery request publishes its rebuild's, its end steps' and a second
// rebuild's changes in one commit): its stored value and provenance, or a
// tombstone.
func settleRecords(acct *storage.AccountTx, b *commitBuilder) error {
	for k, r := range b.records {
		row, err := acct.Record(r.Entity, r.ID, r.FieldGroup)
		if errors.Is(err, storage.ErrNotFound) {
			b.records[k] = Record{Entity: r.Entity, ID: r.ID, FieldGroup: r.FieldGroup, Tombstone: true, Provenance: []string{}}
			continue
		} else if err != nil {
			return err
		}
		value, err := contract.DecodeJSON([]byte(row.Value))
		if err != nil {
			return err
		}
		b.records[k] = Record{Entity: r.Entity, ID: r.ID, FieldGroup: r.FieldGroup, Value: value, Provenance: provenanceList(row.Provenance)}
	}
	return nil
}
