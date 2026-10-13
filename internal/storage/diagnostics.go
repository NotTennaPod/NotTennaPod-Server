package storage

import (
	"database/sql"
	"errors"
)

// Reads for the diagnostics page (PROTOCOL §12).

// RecentRejections returns the account's latest rejected facts, newest first.
func (a *AccountReader) RecentRejections(limit int) ([]FactRow, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT `+factColumns+` FROM facts WHERE account_id = ? AND outcome = 'rejected'
		ORDER BY epoch DESC, seq DESC, op_id LIMIT ?`, a.id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FactRow
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// ArchivedFact returns an archived fact (ErrNotFound if none).
func (a *AccountReader) ArchivedFact(opID string) (FactRow, error) {
	f, err := scanFact(a.r.tx.QueryRowContext(a.r.ctx, `SELECT `+factColumns+` FROM facts WHERE account_id = ? AND op_id = ?`, a.id, opID))
	if errors.Is(err, sql.ErrNoRows) {
		return f, ErrNotFound
	}
	return f, err
}

// FactPlacement is a fact's place in its author chain and, for a derived
// branch, where the branch began (PROTOCOL §6.10).
type FactPlacement struct {
	FactAuthor
	Placed       bool
	BranchParent string // the effective author's parent; "" for an original author
	BranchRoot   string // the fact that began the derived branch
}

// Placement returns a fact's author placement.
func (a *AccountReader) Placement(opID string) (FactPlacement, error) {
	var p FactPlacement
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT raw_author, author_seq, effective_author FROM fact_authors WHERE account_id = ? AND op_id = ?`,
		a.id, opID).Scan(&p.RawAuthor, &p.AuthorSeq, &p.EffectiveAuthor)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	p.Placed = true
	err = a.r.tx.QueryRowContext(a.r.ctx, `SELECT parent, via FROM author_lineage WHERE account_id = ? AND author = ?`,
		a.id, p.EffectiveAuthor).Scan(&p.BranchParent, &p.BranchRoot)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return p, err
}

// SaveIdentity is a listening fact's save identity (PROTOCOL §6.10): its session, the
// continuation that wrote it and its ordinal.
type SaveIdentity struct {
	SessionID, Continuation string
	Ordinal                 int64
}

// SaveIdentities returns the save identities a node holds.
func (a *AccountReader) SaveIdentities(nodeID string) ([]SaveIdentity, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT session_id, author, ordinal FROM session_facts WHERE account_id = ? AND node_id = ?
		ORDER BY session_id, author, ordinal`, a.id, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SaveIdentity
	for rows.Next() {
		var s SaveIdentity
		if err := rows.Scan(&s.SessionID, &s.Continuation, &s.Ordinal); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CutsCovering returns the admitted cuts (history removals and clears,
// statistics resets) that have the node in their causal past (PROTOCOL §8).
func (a *AccountReader) CutsCovering(nodeID string) ([]FactRow, error) {
	edges, err := a.r.tx.QueryContext(a.r.ctx, `SELECT node_id, dependency_id FROM causal_edges WHERE account_id = ?`, a.id)
	if err != nil {
		return nil, err
	}
	dependents := map[string][]string{}
	for edges.Next() {
		var n, d string
		if err := edges.Scan(&n, &d); err != nil {
			edges.Close()
			return nil, err
		}
		dependents[d] = append(dependents[d], n)
	}
	edges.Close()
	if err := edges.Err(); err != nil {
		return nil, err
	}
	later := map[string]bool{}
	queue := []string{nodeID}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, d := range dependents[n] {
			if !later[d] {
				later[d] = true
				queue = append(queue, d)
			}
		}
	}
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT `+factColumns+` FROM facts WHERE account_id = ? AND outcome != 'rejected'
		AND type IN ('history.clear', 'history.remove', 'statistics.reset') ORDER BY epoch, seq, op_id`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FactRow
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		if later[f.OpID] {
			out = append(out, f)
		}
	}
	return out, rows.Err()
}
