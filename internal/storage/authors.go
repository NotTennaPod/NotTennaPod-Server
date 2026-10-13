package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// FactAuthor is an archived fact's placement in its author chain (PROTOCOL §6.10).
type FactAuthor struct {
	RawAuthor       string
	AuthorSeq       int64
	EffectiveAuthor string
}

// FactAuthorOf returns a placed fact's raw and effective author, if it was placed.
func (a *AccountTx) FactAuthorOf(opID string) (FactAuthor, bool, error) {
	var fa FactAuthor
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT raw_author, author_seq, effective_author FROM fact_authors WHERE account_id = ? AND op_id = ?`,
		a.id, opID).Scan(&fa.RawAuthor, &fa.AuthorSeq, &fa.EffectiveAuthor)
	if errors.Is(err, sql.ErrNoRows) {
		return FactAuthor{}, false, nil
	}
	return fa, err == nil, err
}

// PlaceFact records a fact's placement and the chain position it holds.
func (a *AccountTx) PlaceFact(opID string, fa FactAuthor) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO fact_authors (account_id, op_id, raw_author, author_seq, effective_author) VALUES (?, ?, ?, ?, ?)`,
		a.id, opID, fa.RawAuthor, fa.AuthorSeq, fa.EffectiveAuthor); err != nil {
		return err
	}
	return a.InsertAuthorPosition(fa.EffectiveAuthor, fa.AuthorSeq, opID)
}

// SetLineage records an effective author's parent (PROTOCOL §6.10), the fact that made it
// and the last shared fact of the parent's writing history; the first record stands.
func (a *AccountTx) SetLineage(author, parent, via, tip string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO author_lineage (account_id, author, parent, via, tip) VALUES (?, ?, ?, ?, ?)`,
		a.id, author, parent, via, tip)
	return err
}

// Parent returns an author's parent, or "" for an author without lineage.
func (a *AccountTx) Parent(author string) (string, error) {
	var parent string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT parent FROM author_lineage WHERE account_id = ? AND author = ?`, a.id, author).Scan(&parent)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return parent, err
}

// DescendsFrom reports whether author is ancestor or descends from it through
// lineage (an author descends from itself).
func (a *AccountTx) DescendsFrom(author, ancestor string) (bool, error) {
	for seen := 0; author != "" && seen < 1024; seen++ {
		if author == ancestor {
			return true, nil
		}
		parent, err := a.Parent(author)
		if err != nil {
			return false, err
		}
		author = parent
	}
	return false, nil
}

// Lineage returns an author's parent and the last shared fact of the parent's
// writing history ("" for none), or ErrNotFound.
func (a *AccountTx) Lineage(author string) (parent, tip string, err error) {
	err = a.w.tx.QueryRowContext(a.w.ctx, `SELECT parent, tip FROM author_lineage WHERE account_id = ? AND author = ?`, a.id, author).Scan(&parent, &tip)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	return parent, tip, err
}

// MaxOrdinalUpTo returns a continuation's greatest ordinal among its facts at or
// below an author sequence (its writing history up to a shared tip).
func (a *AccountTx) MaxOrdinalUpTo(session, author string, maxSeq int64) (int64, bool, error) {
	var o sql.NullInt64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT MAX(f.ordinal) FROM session_facts f
		JOIN fact_authors fa ON fa.account_id = f.account_id AND fa.op_id = f.node_id
		WHERE f.account_id = ? AND f.session_id = ? AND f.author = ? AND f.ordinal > 0 AND fa.author_seq <= ?`,
		a.id, session, author, maxSeq).Scan(&o)
	return o.Int64, o.Valid, err
}

// OtherFactsOf counts the placed facts written as an author, other than one.
func (a *AccountTx) OtherFactsOf(rawAuthor, except string) (int, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM fact_authors WHERE account_id = ? AND raw_author = ? AND op_id != ?`,
		a.id, rawAuthor, except).Scan(&n)
	return n, err
}

// placeArchivedFacts places every archived fact that has well-formed author
// coordinates but no placement (rejected facts of an earlier server version) as a push places a
// fact (protocol.pusher.place), in receipt order: the author inherited
// through previous_op_id from a placed fact of the same author_id holds the
// position if it is free; otherwise the fact roots a derived branch. Admitted
// facts were all placed by migration 0007 and keep their placement.
func placeArchivedFacts(w *WriteTx) error {
	type unplaced struct {
		account, namespace, op string
		fact                   []byte
	}
	rows, err := w.tx.QueryContext(w.ctx, `SELECT f.account_id, a.namespace, f.op_id, f.fact FROM facts f
		JOIN accounts a ON a.account_id = f.account_id
		WHERE NOT EXISTS (SELECT 1 FROM fact_authors x WHERE x.account_id = f.account_id AND x.op_id = f.op_id)
		ORDER BY f.account_id, f.epoch, f.seq, f.rowid`)
	if err != nil {
		return err
	}
	var all []unplaced
	for rows.Next() {
		var u unplaced
		if err := rows.Scan(&u.account, &u.namespace, &u.op, &u.fact); err != nil {
			rows.Close()
			return err
		}
		all = append(all, u)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, u := range all {
		v, err := contract.DecodeJSON(u.fact)
		if err != nil {
			continue // not an object: no coordinates
		}
		obj, _ := v.(map[string]any)
		author, _ := obj["author_id"].(string)
		n, isNum := obj["author_seq"].(json.Number)
		seq, err := strconv.ParseInt(string(n), 10, 64)
		if !contract.ValidUUID(author) || !isNum || err != nil || seq < 1 {
			continue // malformed coordinates are never placed (PROTOCOL §14.10)
		}
		prev, _ := obj["previous_op_id"].(string)
		if !contract.ValidUUID(prev) {
			prev = ""
		}
		acct := w.Account(u.account)
		inherited := author
		if prev != "" {
			fa, ok, err := acct.FactAuthorOf(prev)
			if err != nil {
				return err
			}
			if ok && fa.RawAuthor == author {
				inherited = fa.EffectiveAuthor
			}
		}
		effective := inherited
		holder, err := acct.AuthorPosition(inherited, seq)
		if err != nil {
			return err
		}
		if holder != "" {
			effective = contract.DerivedAuthorID(u.namespace, u.op)
			if err := acct.SetLineage(effective, inherited, u.op, prev); err != nil {
				return err
			}
		}
		if err := acct.PlaceFact(u.op, FactAuthor{RawAuthor: author, AuthorSeq: seq, EffectiveAuthor: effective}); err != nil {
			return err
		}
	}
	return nil
}
