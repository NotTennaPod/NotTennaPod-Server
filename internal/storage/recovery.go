package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Recovery after a restore (PROTOCOL §6.11): recovery sessions, the original receipts that replace
// provisional ones, and the publication position of recovered entries.

// Recovery is one recovery session of an account.
type Recovery struct {
	RecoveryID string
	InstallID  string
	StartedAt  int64
	LastSeenAt int64
	FinishedAt *int64
	Complete   *bool
	Counts     string // canonical JSON object
	Problems   string // canonical JSON array
}

const recoveryColumns = `recovery_id, install_id, started_at, last_seen_at, finished_at, complete, counts, problems`

func scanRecovery(row interface{ Scan(...any) error }) (Recovery, error) {
	var r Recovery
	var finished sql.NullInt64
	var complete sql.NullInt64
	err := row.Scan(&r.RecoveryID, &r.InstallID, &r.StartedAt, &r.LastSeenAt, &finished, &complete, &r.Counts, &r.Problems)
	if errors.Is(err, sql.ErrNoRows) {
		return Recovery{}, ErrNotFound
	}
	if finished.Valid {
		t := finished.Int64
		r.FinishedAt = &t
	}
	if complete.Valid {
		c := complete.Int64 == 1
		r.Complete = &c
	}
	return r, err
}

// Recovery returns a recovery session (ErrNotFound if none).
func (a *AccountTx) Recovery(recoveryID string) (Recovery, error) {
	return scanRecovery(a.w.tx.QueryRowContext(a.w.ctx, `SELECT `+recoveryColumns+` FROM recoveries WHERE account_id = ? AND recovery_id = ?`,
		a.id, recoveryID))
}

// OpenRecovery opens a recovery session, or touches the one with this ID
// (idempotent by recovery_id). It reports whether the session is new.
func (a *AccountTx) OpenRecovery(recoveryID, installID string, now int64) (bool, error) {
	res, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO recoveries (account_id, recovery_id, install_id, started_at, last_seen_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT (account_id, recovery_id) DO UPDATE SET last_seen_at = excluded.last_seen_at`,
		a.id, recoveryID, installID, now, now)
	if err != nil {
		return false, err
	}
	r, err := a.Recovery(recoveryID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1 && r.StartedAt == now, nil
}

// UpdateRecovery stores a session's counts and problems (canonical JSON)
// and when it was last seen; finished, when not nil, records the client's
// finish with whether it believes the recovery complete.
func (a *AccountTx) UpdateRecovery(recoveryID, counts, problems string, now int64, finished *bool) error {
	var complete any
	var finishedAt any
	if finished != nil {
		finishedAt = now
		if *finished {
			complete = 1
		} else {
			complete = 0
		}
	}
	// A recovery with recorded problems (invalid receipts, conflicts) is
	// never complete, whenever they are recorded (PROTOCOL §6.11).
	return a.execOne(`UPDATE recoveries SET counts = ?1, problems = ?2, last_seen_at = ?3,
		finished_at = COALESCE(?4, finished_at),
		complete = CASE WHEN ?2 != '[]' AND COALESCE(?5, complete) IS NOT NULL THEN 0 ELSE COALESCE(?5, complete) END
		WHERE account_id = ?6 AND recovery_id = ?7`,
		counts, problems, now, finishedAt, complete, a.id, recoveryID)
}

// RecentRecoveries returns the account's latest recovery sessions, newest
// first (diagnostics).
func (a *AccountReader) RecentRecoveries(limit int) ([]Recovery, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT `+recoveryColumns+` FROM recoveries WHERE account_id = ?
		ORDER BY last_seen_at DESC, recovery_id LIMIT ?`, a.id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Recovery
	for rows.Next() {
		r, err := scanRecovery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SupersededReceipt reports whether (epoch, seq, index) is a receipt of the
// fact that an original replaced (PROTOCOL §6.11).
func (a *AccountTx) SupersededReceipt(opID string, epoch, seq, index int64) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM superseded_receipts WHERE account_id = ? AND op_id = ? AND epoch = ? AND seq = ?
		AND receipt_index = ?`, a.id, opID, epoch, seq, index).Scan(&n)
	return n > 0, err
}

// SupersededByFact returns, per archived fact, the receipts kept beside its
// own (receipts an original replaced, and later receipts that admitted it
// over a rejected original), each in receipt order. A snapshot carries those
// of causal nodes.
func (a *AccountReader) SupersededByFact() (map[string][][]byte, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT op_id, receipt FROM superseded_receipts WHERE account_id = ?
		ORDER BY op_id, epoch, seq, receipt_index`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][][]byte{}
	for rows.Next() {
		var op string
		var b []byte
		if err := rows.Scan(&op, &b); err != nil {
			return nil, err
		}
		out[op] = append(out[op], b)
	}
	return out, rows.Err()
}

// SupersededReceipts returns the receipts originals replaced for a fact, in
// receipt order.
func (a *AccountTx) SupersededReceipts(opID string) ([][]byte, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT receipt FROM superseded_receipts WHERE account_id = ? AND op_id = ?
		ORDER BY epoch, seq, receipt_index`, a.id, opID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ReplaceReceipt replaces an archived fact's receipt with its original
// (PROTOCOL §6.11): the archived receipt moves to superseded_receipts and the fact
// takes the original's receipt, position, outcome and frontier. Its
// publication position stays (it was visible since then); f.NodePubEpoch
// and f.NodePubSeq are the replacing commit's.
func (a *AccountTx) ReplaceReceipt(f FactRow, now int64) error {
	old, err := a.Fact(f.OpID)
	if err != nil {
		return err
	}
	wasNode, err := a.nodeByReceipts(old)
	if err != nil {
		return err
	}
	if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO superseded_receipts (account_id, op_id, epoch, seq, receipt_index, receipt, replaced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, a.id, old.OpID, old.Epoch, old.Seq, old.ReceiptIndex, old.Receipt, now); err != nil {
		return err
	}
	// A fact no receipt admitted before becomes a causal node now: its node
	// is published by this commit (f.NodePub*), not by an earlier one, so a
	// past head's frontier never changes. A node keeps its first
	// actual publication, whatever receipts replace one another later.
	nodeEpoch, nodeSeq := old.NodePubEpoch, old.NodePubSeq
	if !wasNode && f.Outcome != "rejected" {
		nodeEpoch, nodeSeq = f.NodePubEpoch, f.NodePubSeq
	}
	return a.execOne(`UPDATE facts SET receipt = ?, outcome = ?, frontier = ?, epoch = ?, seq = ?, receipt_index = ?, node_pub_epoch = ?, node_pub_seq = ?
		WHERE account_id = ? AND op_id = ?`, f.Receipt, f.Outcome, f.Frontier, f.Epoch, f.Seq, f.ReceiptIndex, nodeEpoch, nodeSeq, a.id, f.OpID)
}

// KeepReceipt keeps a verified receipt of an archived fact whose position is later than the
// archived one's, when the archived receipt rejected the fact and this one admitted it (a
// provisional acceptance after a restore): it is kept as superseded, as if it had been archived
// first and the original had replaced it, so the fact is a node that contributes nothing (PROTOCOL
// §6.11) whatever the upload order. Any other later receipt is known and not kept
// (docs/CONTRACT.md). kept reports a receipt the archive did not hold (its commit publishes it); became, a
// fact that becomes a causal node by it: its node is published by that commit (nodePub).
func (a *AccountTx) KeepReceipt(opID string, epoch, seq, index int64, receipt []byte, admitted bool, nodePub [2]int64,
	now int64) (kept, became bool, err error) {
	f, err := a.Fact(opID)
	if err != nil {
		return false, false, err
	}
	if f.Outcome != "rejected" || !admitted {
		return false, false, nil
	}
	wasNode, err := a.nodeByReceipts(f)
	if err != nil {
		return false, false, err
	}
	res, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO superseded_receipts (account_id, op_id, epoch, seq, receipt_index, receipt, replaced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, a.id, opID, epoch, seq, index, receipt, now)
	if err != nil {
		return false, false, err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return false, false, err
	}
	if wasNode {
		return true, false, nil
	}
	return true, true, a.execOne(`UPDATE facts SET node_pub_epoch = ?, node_pub_seq = ? WHERE account_id = ? AND op_id = ?`, nodePub[0], nodePub[1],
		a.id, opID)
}

// CredentialsVersion returns the account's sign-in credentials version (0
// without a login): a password change or reset increases it.
func (a *AccountTx) CredentialsVersion() (int64, error) {
	var v int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT credentials_version FROM account_logins WHERE account_id = ?`, a.id).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

// AdmittedByReceipts reports whether an archived fact is a causal node by
// its receipts: its own admitted it, or a superseded one did (PROTOCOL §6.11).
func (a *AccountTx) AdmittedByReceipts(opID string) (bool, error) {
	f, err := a.Fact(opID)
	if err != nil {
		return false, err
	}
	return a.nodeByReceipts(f)
}

func (a *AccountTx) nodeByReceipts(f FactRow) (bool, error) {
	if f.Outcome != "rejected" {
		return true, nil
	}
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM superseded_receipts WHERE account_id = ? AND op_id = ?
		AND json_extract(CAST(receipt AS TEXT), '$.outcome') != 'rejected'`, a.id, f.OpID).Scan(&n)
	return n > 0, err
}

// State returns the account's state ("active" or another).
func (a *AccountTx) State() (string, error) {
	var state string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT state FROM accounts WHERE account_id = ?`, a.id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return state, err
}

// DeferForeignKeys defers this transaction's foreign-key checks to its
// commit: a recovery upload archives decisions before the rebuild in the
// same transaction makes them causal nodes again.
func (a *AccountTx) DeferForeignKeys() error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `PRAGMA defer_foreign_keys = ON`)
	return err
}

// ArchivedDecision returns an archived decision's row (ErrNotFound if none).
func (a *AccountTx) ArchivedDecision(decisionID string) (DecisionRow, error) {
	var d DecisionRow
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT decision_id, decision_type, subject, epoch, seq, normalized_time, decision, pub_epoch, pub_seq
		FROM decisions WHERE account_id = ? AND decision_id = ?`, a.id, decisionID).
		Scan(&d.DecisionID, &d.DecisionType, &d.Subject, &d.Epoch, &d.Seq, &d.NormalizedTime, &d.Decision, &d.PubEpoch, &d.PubSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return DecisionRow{}, ErrNotFound
	}
	return d, err
}

// EvidenceTransition runs the full restore transition (PROTOCOL §11) because a verified receipt
// or decision at epoch, for an entry the archive does not hold, proves a timeline the server lost
// without detecting it (PROTOCOL §6.11): the server moves to max(high-water, epoch) + 1 before
// the entry is joined, in this transaction.
func (w *WriteTx) EvidenceTransition(epoch int64) error {
	if w.transition != nil {
		if w.transition.epoch > epoch {
			return nil
		}
		return fmt.Errorf("storage: a second lost timeline in one transaction (epoch %d)", epoch)
	}
	if w.store.meta == nil {
		// A staging or test store without restore metadata moves its accounts
		// only (no credentials or incarnation to change).
		_, err := w.tx.ExecContext(w.ctx, `UPDATE account_logs SET epoch = MAX(epoch, ?) + 1`, epoch)
		return err
	}
	w.evidenceEpoch = epoch
	return w.RestoreTransition("receipt evidence from a lost timeline")
}

// ProvisionallyAdmitted returns the facts whose original receipt is rejected
// but a superseded provisional receipt admitted (PROTOCOL §6.11), with that
// receipt's normalized time: they stay causal nodes without effects.
func (a *AccountTx) ProvisionallyAdmitted() (map[string]int64, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT f.op_id, MIN(json_extract(CAST(s.receipt AS TEXT), '$.normalized_time')) FROM facts f
		JOIN superseded_receipts s ON s.account_id = f.account_id AND s.op_id = f.op_id
		WHERE f.account_id = ? AND f.outcome = 'rejected' AND json_extract(CAST(s.receipt AS TEXT), '$.outcome') != 'rejected'
		GROUP BY f.op_id`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var op string
		var t int64
		if err := rows.Scan(&op, &t); err != nil {
			return nil, err
		}
		out[op] = t
	}
	return out, rows.Err()
}

// NodeScopes returns the scopes a node contributes to, sorted.
func (a *AccountTx) NodeScopes(nodeID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT scope FROM contributions WHERE account_id = ? AND node_id = ? ORDER BY scope`, a.id, nodeID)
}

// UnresolvedRegistrations returns the admitted subscribe and preview
// registrations (causal nodes) without any podcast.resolution decision, in
// receipt order (PROTOCOL §6.11).
func (a *AccountTx) UnresolvedRegistrations() ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT f.op_id FROM facts f
		JOIN causal_nodes c ON c.account_id = f.account_id AND c.node_id = f.op_id
		WHERE f.account_id = ?1 AND f.type IN ('podcast.subscribe', 'podcast.register_preview') AND f.outcome != 'rejected'
		AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.account_id = ?1 AND d.decision_type = 'podcast.resolution' AND d.subject = f.op_id)
		ORDER BY f.epoch, f.seq, f.receipt_index, f.op_id`, a.id)
}

// DecisionsOf returns the account's admitted decisions (causal nodes) of the
// given types in rank order (epoch, seq, decision_id).
func (a *AccountTx) DecisionsOf(types ...string) ([]DecisionRow, error) {
	if len(types) == 0 {
		return nil, nil
	}
	q := `SELECT d.decision_id, d.decision_type, d.subject, d.epoch, d.seq, d.normalized_time, d.decision, d.pub_epoch, d.pub_seq
		FROM decisions d JOIN causal_nodes c ON c.account_id = d.account_id AND c.node_id = d.decision_id
		WHERE d.account_id = ? AND d.decision_type IN (?` + strings.Repeat(", ?", len(types)-1) + `)
		ORDER BY d.epoch, d.seq, d.decision_id`
	args := []any{a.id}
	for _, t := range types {
		args = append(args, t)
	}
	rows, err := a.w.tx.QueryContext(a.w.ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DecisionRow
	for rows.Next() {
		var d DecisionRow
		if err := rows.Scan(&d.DecisionID, &d.DecisionType, &d.Subject, &d.Epoch, &d.Seq, &d.NormalizedTime, &d.Decision, &d.PubEpoch,
			&d.PubSeq); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Recovered reports whether an archived fact was joined by recovery: its
// publication position is not its receipt's, or an original replaced its
// receipt.
func (a *AccountTx) Recovered(opID string) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT (SELECT COUNT(*) FROM facts WHERE account_id = ?1 AND op_id = ?2
		AND (pub_epoch != epoch OR pub_seq != seq)) + (SELECT COUNT(*) FROM superseded_receipts WHERE account_id = ?1 AND op_id = ?2)`,
		a.id, opID).Scan(&n)
	return n > 0, err
}

// HasRecoveries reports whether recovery ever ran for the account (its
// archive may hold recovered entries).
func (a *AccountTx) HasRecoveries() (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT EXISTS (SELECT 1 FROM recoveries WHERE account_id = ?1)
		OR EXISTS (SELECT 1 FROM superseded_receipts WHERE account_id = ?1)`, a.id).Scan(&n)
	return n > 0, err
}

// UpdateRecoverySeen records when a recovery session was last seen.
func (a *AccountTx) UpdateRecoverySeen(recoveryID string, now int64) error {
	return a.execOne(`UPDATE recoveries SET last_seen_at = ? WHERE account_id = ? AND recovery_id = ?`, now, a.id, recoveryID)
}

// Savepoint, RollbackTo and Release scope part of a write transaction: a
// recovery rebuild applies a decision atomically or not at all.
func (a *AccountTx) Savepoint(name string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `SAVEPOINT `+name)
	return err
}

// RollbackTo undoes everything since the savepoint and releases it.
func (a *AccountTx) RollbackTo(name string) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `ROLLBACK TO `+name); err != nil {
		return err
	}
	return a.Release(name)
}

// Release ends a savepoint, keeping what it scoped.
func (a *AccountTx) Release(name string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `RELEASE `+name)
	return err
}

// Uninterruptible runs the rest of this write transaction's statements
// without the request's cancellation: a rebuild (PROTOCOL §6.11) replays thousands of
// statements, and a cancellable context costs each one a watcher goroutine.
// A cancelled request still rolls the transaction back at its end.
func (w *WriteTx) Uninterruptible() { w.ctx = context.WithoutCancel(w.ctx) }

// Uninterruptible is WriteTx.Uninterruptible for the account's transaction.
func (a *AccountTx) Uninterruptible() { a.w.Uninterruptible() }
