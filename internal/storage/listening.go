package storage

import (
	"database/sql"
	"errors"
)

// SessionRow is a listening session's start (PROTOCOL §8).
type SessionRow struct {
	ID, Scope, AuthorID, StartNode string
}

// ContinuationRow is one author's continuation of a session (PROTOCOL §6.10): the start's
// author holds the root continuation; a descendant author's continuation follows
// its parent's up to Base.
type ContinuationRow struct {
	Session, Author, Parent string
	Base                    int64
	SealedBy, FinalizedBy   string // "" when not
	Latest                  int64  // greatest admitted ordinal (at least Base)
}

// Session returns a listening session's start.
func (a *AccountTx) Session(id string) (SessionRow, error) {
	r := SessionRow{ID: id}
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT scope, author_id, start_node FROM listening_sessions WHERE account_id = ? AND session_id = ?`,
		a.id, id).Scan(&r.Scope, &r.AuthorID, &r.StartNode)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// StartSession records a session's start as its root continuation's ordinal 0.
func (a *AccountTx) StartSession(id, scope, authorID, startNode string) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO listening_sessions (account_id, session_id, scope, author_id, start_node)
		VALUES (?, ?, ?, ?, ?)`, a.id, id, scope, authorID, startNode); err != nil {
		return err
	}
	if err := a.AddContinuation(id, authorID, "", 0); err != nil {
		return err
	}
	return a.AddSessionFact(id, authorID, 0, startNode)
}

// AddContinuation records an author's continuation of a session.
func (a *AccountTx) AddContinuation(session, author, parent string, base int64) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO session_continuations (account_id, session_id, author, parent, base) VALUES (?, ?, ?, ?, ?)`,
		a.id, session, author, parent, base)
	return err
}

// Continuation returns an author's continuation of a session, with its greatest ordinal.
func (a *AccountTx) Continuation(session, author string) (ContinuationRow, error) {
	r := ContinuationRow{Session: session, Author: author}
	var sealed, finalized sql.NullString
	var latest sql.NullInt64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT c.parent, c.base, c.sealed_by, c.finalized_by,
		(SELECT MAX(ordinal) FROM session_facts f WHERE f.account_id = c.account_id AND f.session_id = c.session_id AND f.author = c.author)
		FROM session_continuations c WHERE c.account_id = ? AND c.session_id = ? AND c.author = ?`, a.id, session, author).
		Scan(&r.Parent, &r.Base, &sealed, &finalized, &latest)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	r.SealedBy, r.FinalizedBy = sealed.String, finalized.String
	r.Latest = max(r.Base, latest.Int64)
	return r, err
}

// AddSessionFact records a checkpoint or end at its ordinal in a continuation.
func (a *AccountTx) AddSessionFact(session, author string, ordinal int64, node string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO session_facts (account_id, session_id, author, ordinal, node_id) VALUES (?, ?, ?, ?, ?)`,
		a.id, session, author, ordinal, node)
	return err
}

// SessionFact returns the node at an ordinal of a continuation.
func (a *AccountTx) SessionFact(session, author string, ordinal int64) (string, error) {
	var node string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT node_id FROM session_facts WHERE account_id = ? AND session_id = ? AND author = ? AND ordinal = ?`,
		a.id, session, author, ordinal).Scan(&node)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return node, err
}

// SessionFactsOf returns a continuation's (ordinal, node) pairs in ordinal order.
func (a *AccountTx) SessionFactsOf(session, author string) ([]int64, []string, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT ordinal, node_id FROM session_facts WHERE account_id = ? AND session_id = ? AND author = ? ORDER BY ordinal`,
		a.id, session, author)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var ordinals []int64
	var nodes []string
	for rows.Next() {
		var o int64
		var n string
		if err := rows.Scan(&o, &n); err != nil {
			return nil, nil, err
		}
		ordinals, nodes = append(ordinals, o), append(nodes, n)
	}
	return ordinals, nodes, rows.Err()
}

// SealContinuation records a continuation's end.
func (a *AccountTx) SealContinuation(session, author, node string) error {
	return a.execOne(`UPDATE session_continuations SET sealed_by = ? WHERE account_id = ? AND session_id = ? AND author = ?`, node, a.id, session, author)
}

// FinalizeContinuation records a continuation's own finalization.
func (a *AccountTx) FinalizeContinuation(session, author, node string) error {
	return a.execOne(`UPDATE session_continuations SET finalized_by = ? WHERE account_id = ? AND session_id = ? AND author = ?`, node, a.id, session, author)
}

// ListeningFact is a fact a cut can cover, or a history removal.
type ListeningFact struct {
	Node, Scope  string
	Rank         int64
	HistoryEntry bool
	Session      string // with Ordinal ≥ 0: a session fact
	Author       string // its continuation's author (PROTOCOL §6.10)
	Ordinal      int64
	Removal      bool
	Restores     string // a history restore's removal
}

// AddListeningFact indexes a listening fact (session "" for none).
func (a *AccountTx) AddListeningFact(f ListeningFact) error {
	var session, author, ordinal, restores any
	if f.Session != "" {
		session, author, ordinal = f.Session, f.Author, f.Ordinal
	}
	if f.Restores != "" {
		restores = f.Restores
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO listening_facts (account_id, node_id, scope, history_entry, session_id, author, ordinal, removal, restores)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, a.id, f.Node, f.Scope, f.HistoryEntry, session, author, ordinal, f.Removal, restores)
	return err
}

// PutListeningFact indexes an import decision's listening fact in one scope,
// replacing the one it had there: the rows of a grouped import that a
// dispute splits between releases are one node in several scopes, each
// covered by cuts there.
func (a *AccountTx) PutListeningFact(f ListeningFact) error {
	if err := a.RemoveListeningFact(f.Node, f.Scope); err != nil {
		return err
	}
	return a.AddListeningFact(f)
}

// RemoveListeningFact removes a node's listening fact from one scope.
func (a *AccountTx) RemoveListeningFact(node, scope string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM listening_facts WHERE account_id = ? AND node_id = ? AND scope = ?`, a.id, node, scope)
	return err
}

// Restored reports whether an admitted restore already cancels the removal.
func (a *AccountTx) Restored(removal string) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM listening_facts WHERE account_id = ? AND restores = ?`, a.id, removal).Scan(&n)
	return n > 0, err
}

// ListeningFactOf returns an indexed listening fact.
func (a *AccountTx) ListeningFactOf(node string) (ListeningFact, error) {
	fs, err := a.listeningFacts(`l.node_id = ?`, node)
	if err != nil {
		return ListeningFact{}, err
	}
	if len(fs) == 0 {
		return ListeningFact{}, ErrNotFound
	}
	return fs[0], nil
}

// HistoryEntries returns the history entries of one scope, or of the whole
// account when scope is "".
func (a *AccountTx) HistoryEntries(scope string) ([]ListeningFact, error) {
	if scope == "" {
		return a.listeningFacts(`l.history_entry = 1`)
	}
	return a.listeningFacts(`l.scope = ? AND l.history_entry = 1`, scope)
}

// SessionFacts returns every session fact with an ordinal in the account.
func (a *AccountTx) SessionFacts() ([]ListeningFact, error) {
	return a.listeningFacts(`l.ordinal IS NOT NULL`)
}

func (a *AccountTx) listeningFacts(where string, args ...any) ([]ListeningFact, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT l.node_id, l.scope, o.rank, l.history_entry, COALESCE(l.session_id, ''), COALESCE(l.author, ''), COALESCE(l.ordinal, -1), l.removal
		FROM listening_facts l JOIN node_order o ON o.account_id = l.account_id AND o.node_id = l.node_id
		WHERE l.account_id = ? AND `+where, append([]any{a.id}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ListeningFact
	for rows.Next() {
		var f ListeningFact
		if err := rows.Scan(&f.Node, &f.Scope, &f.Rank, &f.HistoryEntry, &f.Session, &f.Author, &f.Ordinal, &f.Removal); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// NodeRank returns a node's rank in the account's causal order.
func (a *AccountTx) NodeRank(node string) (int64, error) {
	var rank int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT rank FROM node_order WHERE account_id = ? AND node_id = ?`, a.id, node).Scan(&rank)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return rank, err
}

// Ancestors returns the node's causal past restricted to nodes ranked at or
// above minRank (ranks are a topological order, so no path from the node to
// such an ancestor leaves that range). The edges in the range are read in one
// indexed scan and walked in memory.
func (a *AccountTx) Ancestors(node string, minRank int64) (map[string]bool, error) {
	top, err := a.NodeRank(node)
	if err != nil {
		return nil, err
	}
	// The rank range first (CROSS JOIN keeps SQLite's join order): left to
	// itself the planner scans every edge of the account, which made each call linear in the
	// archive.
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT e.node_id, e.dependency_id, d.rank FROM node_order o INDEXED BY node_order_rank
		CROSS JOIN causal_edges e ON e.account_id = o.account_id AND e.node_id = o.node_id
		CROSS JOIN node_order d ON d.account_id = e.account_id AND d.node_id = e.dependency_id
		WHERE o.account_id = ? AND o.rank >= ? AND o.rank <= ?`, a.id, minRank, top)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deps := map[string][]string{}
	for rows.Next() {
		var from, to string
		var rank int64
		if err := rows.Scan(&from, &to, &rank); err != nil {
			return nil, err
		}
		if rank >= minRank {
			deps[from] = append(deps[from], to)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	stack := append([]string(nil), deps[node]...)
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[n] {
			continue
		}
		seen[n] = true
		stack = append(stack, deps[n]...)
	}
	return seen, nil
}
