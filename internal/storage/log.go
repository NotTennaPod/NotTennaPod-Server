package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// Revision is a position in an account's change log.
type Revision struct {
	Epoch int64
	Seq   int64
}

// Head is the current end of an account's change log.
type Head struct {
	LogID  string
	Epoch  int64
	Seq    int64
	Digest []byte
}

// CommitBody is the content of a commit apart from its revision (PROTOCOL §6.1).
// PROTOCOL §6.4 replaces these generic values with typed, validated envelopes.
type CommitBody struct {
	Accepted  []any
	Decisions []any
	Slots     []any
	Records   []any
}

// StoredCommit is one committed log entry: its canonical bytes are served to
// clients unchanged.
type StoredCommit struct {
	Seq       int64
	Epoch     int64
	Digest    []byte
	Canonical []byte
}

// Errors from the change log.
var (
	ErrSecondCommit     = errors.New("storage: one commit per account per transaction")
	ErrRevisionMismatch = errors.New("storage: commit revision differs from NextRevision")
	ErrChainBroken      = errors.New("storage: change log chain does not verify")
)

// AccountTx is an account's view of a write transaction.
type AccountTx struct {
	w        *WriteTx
	id       string
	next     *Revision
	appended bool
}

// AccountReader is an account's view of a read transaction.
type AccountReader struct {
	r  *ReadTx
	id string
}

func readHead(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, accountID string) (Head, error) {
	var h Head
	err := q.QueryRowContext(ctx, "SELECT log_id, epoch, head_seq, head_digest FROM account_logs WHERE account_id = ?", accountID).
		Scan(&h.LogID, &h.Epoch, &h.Seq, &h.Digest)
	if errors.Is(err, sql.ErrNoRows) {
		return Head{}, ErrNoAccount
	}
	return h, err
}

// Head returns the account's log head as seen inside this transaction.
func (a *AccountTx) Head() (Head, error) { return readHead(a.w.ctx, a.w.tx, a.id) }

// NextRevision returns the revision this transaction's commit will take, so the
// caller can put it into receipts and decisions before signing them.
func (a *AccountTx) NextRevision() (Revision, error) {
	if a.appended {
		return Revision{}, ErrSecondCommit
	}
	h, err := a.Head()
	if err != nil {
		return Revision{}, err
	}
	r := Revision{Epoch: h.Epoch, Seq: h.Seq + 1}
	a.next = &r
	return r, nil
}

// AppendCommit appends the account's commit for this transaction: it takes the
// next seq, chains the digest and advances the head, all inside the caller's
// transaction, so the commit and the caller's other writes become visible together
// or not at all.
func (a *AccountTx) AppendCommit(body CommitBody) (Revision, []byte, error) {
	if a.appended {
		return Revision{}, nil, ErrSecondCommit
	}
	ctx, tx := a.w.ctx, a.w.tx
	h, err := a.Head()
	if err != nil {
		return Revision{}, nil, err
	}
	rev := Revision{Epoch: h.Epoch, Seq: h.Seq + 1}
	if a.next != nil && *a.next != rev {
		return Revision{}, nil, ErrRevisionMismatch
	}
	commit := map[string]any{
		"epoch": rev.Epoch, "seq": rev.Seq,
		"accepted": orEmpty(body.Accepted), "decisions": orEmpty(body.Decisions),
		"slots": orEmpty(body.Slots), "records": orEmpty(body.Records),
	}
	canonical, err := contract.Canonical(commit)
	if err != nil {
		return Revision{}, nil, err
	}
	digest, err := contract.ChainDigest(h.Digest, canonical)
	if err != nil {
		return Revision{}, nil, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO log_commits (account_id, seq, epoch, digest, commit_json, committed_at) VALUES (?, ?, ?, ?, ?, ?)",
		a.id, rev.Seq, rev.Epoch, digest, canonical, a.w.nowMillis()); err != nil {
		return Revision{}, nil, err
	}
	if err := call(a.w.store.hooks.afterLogInsert); err != nil {
		return Revision{}, nil, err
	}
	res, err := tx.ExecContext(ctx, "UPDATE account_logs SET head_seq = ?, head_digest = ? WHERE account_id = ? AND head_seq = ?",
		rev.Seq, digest, a.id, h.Seq)
	if err != nil {
		return Revision{}, nil, err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return Revision{}, nil, fmt.Errorf("storage: head update affected %d rows: %v", n, err)
	}
	if err := call(a.w.store.hooks.afterHeadUpdate); err != nil {
		return Revision{}, nil, err
	}
	a.appended = true
	return rev, digest, nil
}

func orEmpty(v []any) []any {
	if v == nil {
		return []any{}
	}
	return v
}

// Head returns the account's log head in this snapshot.
func (a *AccountReader) Head() (Head, error) { return readHead(a.r.ctx, a.r.tx, a.id) }

// Commits returns commits with afterSeq < seq <= upToSeq in seq order, at most
// limit of them (limit <= 0 means no limit).
func (a *AccountReader) Commits(afterSeq, upToSeq int64, limit int) ([]StoredCommit, error) {
	if limit <= 0 {
		limit = -1
	}
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT seq, epoch, digest, commit_json FROM log_commits
		WHERE account_id = ? AND seq > ? AND seq <= ? ORDER BY seq LIMIT ?`, a.id, afterSeq, upToSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredCommit
	for rows.Next() {
		var c StoredCommit
		if err := rows.Scan(&c.Seq, &c.Epoch, &c.Digest, &c.Canonical); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// VerifyChain recomputes the account's whole digest chain in one snapshot: seq
// contiguous from 1, each digest = SHA-256(previous || canonical bytes), the stored
// bytes canonical, and the head equal to the last commit.
func (s *Store) VerifyChain(ctx context.Context, accountID string) error {
	return s.Read(ctx, func(r *ReadTx) error {
		acct := r.Account(accountID)
		h, err := acct.Head()
		if err != nil {
			return err
		}
		prev := contract.GenesisDigest
		var seq int64
		for {
			page, err := acct.Commits(seq, h.Seq, 500)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			for _, c := range page {
				if c.Seq != seq+1 {
					return fmt.Errorf("%w: seq %d follows %d", ErrChainBroken, c.Seq, seq)
				}
				if err := checkCanonical(c.Canonical); err != nil {
					return fmt.Errorf("%w: seq %d: %v", ErrChainBroken, c.Seq, err)
				}
				d, err := contract.ChainDigest(prev, c.Canonical)
				if err != nil {
					return err
				}
				if !bytes.Equal(d, c.Digest) {
					return fmt.Errorf("%w: digest at seq %d", ErrChainBroken, c.Seq)
				}
				prev, seq = d, c.Seq
			}
		}
		if seq != h.Seq || !bytes.Equal(prev, h.Digest) {
			return fmt.Errorf("%w: head (%d) does not match the last commit (%d)", ErrChainBroken, h.Seq, seq)
		}
		return nil
	})
}

func checkCanonical(data []byte) error {
	v, err := contract.DecodeJSON(data)
	if err != nil {
		return err
	}
	c, err := contract.Canonical(v)
	if err != nil {
		return err
	}
	if !bytes.Equal(c, data) {
		return errors.New("stored commit is not canonical")
	}
	return nil
}

// checkHeads verifies at startup that every account's head matches its last
// commit: the commit count equals head_seq, seqs run 1..head_seq, and the last
// commit's digest is the head digest.
func (s *Store) checkHeads(ctx context.Context) error {
	return s.Read(ctx, func(r *ReadTx) error {
		ids, err := r.AccountIDs()
		if err != nil {
			return err
		}
		for _, id := range ids {
			h, err := r.Account(id).Head()
			if err != nil {
				return fmt.Errorf("%w: account %s has no log: %v", ErrIntegrity, id, err)
			}
			var count, minSeq, maxSeq int64
			if err := r.tx.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) FROM log_commits WHERE account_id = ?", id).
				Scan(&count, &minSeq, &maxSeq); err != nil {
				return err
			}
			if count != h.Seq || maxSeq != h.Seq || (count > 0 && minSeq != 1) {
				return fmt.Errorf("%w: account %s head %d, commits %d (%d..%d)", ErrIntegrity, id, h.Seq, count, minSeq, maxSeq)
			}
			want := contract.GenesisDigest
			if count > 0 {
				if err := r.tx.QueryRowContext(ctx, "SELECT digest FROM log_commits WHERE account_id = ? AND seq = ?", id, maxSeq).Scan(&want); err != nil {
					return err
				}
			}
			if !bytes.Equal(want, h.Digest) {
				return fmt.Errorf("%w: account %s head digest does not match its last commit", ErrIntegrity, id)
			}
		}
		return nil
	})
}
