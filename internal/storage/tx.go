package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// WriteTx is the one write transaction in progress (BEGIN IMMEDIATE on the single
// writer connection).
type WriteTx struct {
	ctx      context.Context
	tx       *sql.Tx
	store    *Store
	accounts map[string]*AccountTx

	authChange bool        // an access-removing change was made (PROTOCOL §11)
	raised     int64       // the auth revision this transaction writes, once raised
	transition *transition // a restore transition made in this transaction (PROTOCOL §11)
	// evidenceEpoch is the epoch a recovered receipt or decision evidenced
	// (PROTOCOL §6.11): a transition in this transaction moves above it.
	evidenceEpoch int64
	changed       map[string]bool // accounts whose log advanced without AppendCommit (notify.go)
	opened        bool            // a podcast generation was opened (notify.go)
}

// ReadTx is a read transaction on one WAL snapshot.
type ReadTx struct {
	ctx context.Context
	tx  *sql.Tx
}

// Write runs fn in a serialized write transaction and commits if fn returns nil.
// An error, a panic (re-raised after rollback) or a cancelled context rolls back
// every effect of fn. Keep fn short: no network I/O.
//
// A transaction that removes access (marked by the auth methods) raises the
// auth revision in two phases around its commit (PROTOCOL §11): the restore metadata
// records the pending revision, the transaction commits it, and the metadata then
// records it as current. A restore transition finishes the same way.
func (s *Store) Write(ctx context.Context, fn func(*WriteTx) error) error {
	s.life.RLock()
	defer s.life.RUnlock()
	if s.closed.Load() {
		return ErrClosed
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: begin: %w", err)
	}
	w := &WriteTx{ctx: ctx, tx: tx, store: s, accounts: map[string]*AccountTx{}}
	committed, commitTried := false, false
	defer func() {
		if !committed {
			// While holding the writer, so no other transaction sees the
			// metadata half-reverted; also during a panic, which then continues.
			if !commitTried {
				w.revert()
			}
			_ = tx.Rollback()
		}
	}()
	if err := fn(w); err != nil {
		return err
	}
	if err := w.raiseAuthRevision(); err != nil {
		return err
	}
	if err := call(s.hooks.beforeCommit); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A failed commit may or may not have reached the disk: a pending revision
	// stays, and the next start treats it as a fence (PROTOCOL §11).
	commitTried = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit: %w", err)
	}
	committed = true
	w.finish()
	s.notify(w.committedAccounts(), w.transition != nil, w.opened || len(w.changed) > 0)
	return nil
}

// Read runs fn in a read transaction; everything it reads comes from one snapshot.
func (s *Store) Read(ctx context.Context, fn func(*ReadTx) error) error {
	if s.closed.Load() {
		return ErrClosed
	}
	tx, err := s.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("storage: begin read: %w", err)
	}
	defer tx.Rollback()
	return fn(&ReadTx{ctx: ctx, tx: tx})
}

// ErrNoAccount reports an account ID that does not exist.
var ErrNoAccount = errors.New("storage: no such account")

// Account is an account's identity.
type Account struct {
	ID        string
	Namespace string
	State     string
	CreatedAt int64
}

// CreateAccount creates an active account with fresh random IDs and an empty log
// at epoch 1.
func (w *WriteTx) CreateAccount() (Account, error) {
	a := Account{ID: newUUID(), Namespace: newUUID(), State: "active", CreatedAt: w.nowMillis()}
	if _, err := w.tx.ExecContext(w.ctx, "INSERT INTO accounts (account_id, namespace, state, created_at) VALUES (?, ?, ?, ?)",
		a.ID, a.Namespace, a.State, a.CreatedAt); err != nil {
		return Account{}, err
	}
	if _, err := w.tx.ExecContext(w.ctx, "INSERT INTO account_logs (account_id, log_id, epoch, head_seq, head_digest) VALUES (?, ?, 1, 0, ?)",
		a.ID, newUUID(), make([]byte, 32)); err != nil {
		return Account{}, err
	}
	return a, nil
}

// DeleteAccount removes the account and, by cascade, everything it owns (PROTOCOL §5).
// It removes access, so it raises the auth revision (PROTOCOL §11).
func (w *WriteTx) DeleteAccount(id string) error {
	res, err := w.tx.ExecContext(w.ctx, "DELETE FROM accounts WHERE account_id = ?", id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNoAccount
	}
	w.markAuthChange()
	return nil
}

// Account returns the account-scoped view of this transaction. Every method on it
// binds the account ID; an unknown account fails with ErrNoAccount on first use.
func (w *WriteTx) Account(id string) *AccountTx {
	if a, ok := w.accounts[id]; ok {
		return a
	}
	a := &AccountTx{w: w, id: id}
	w.accounts[id] = a
	return a
}

// Account returns the account-scoped read view.
func (r *ReadTx) Account(id string) *AccountReader {
	return &AccountReader{r: r, id: id}
}

// AccountIDs lists every account, for server-level tasks (startup checks,
// administration).
func (r *ReadTx) AccountIDs() ([]string, error) {
	return queryStrings(r.ctx, r.tx, "SELECT account_id FROM accounts ORDER BY account_id")
}

// GetAccount returns an account's identity.
func (r *ReadTx) GetAccount(id string) (Account, error) {
	var a Account
	err := r.tx.QueryRowContext(r.ctx, "SELECT account_id, namespace, state, created_at FROM accounts WHERE account_id = ?", id).
		Scan(&a.ID, &a.Namespace, &a.State, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, ErrNoAccount
	}
	return a, err
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func queryStrings(ctx context.Context, q queryer, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CreateAccountWithIDs creates an active account with given IDs (rule-vector
// tests; PROTOCOL §11 import reuses it).
func (w *WriteTx) CreateAccountWithIDs(id, namespace string) (Account, error) {
	a := Account{ID: id, Namespace: namespace, State: "active", CreatedAt: w.nowMillis()}
	if _, err := w.tx.ExecContext(w.ctx, "INSERT INTO accounts (account_id, namespace, state, created_at) VALUES (?, ?, ?, ?)",
		a.ID, a.Namespace, a.State, a.CreatedAt); err != nil {
		return Account{}, err
	}
	if _, err := w.tx.ExecContext(w.ctx, "INSERT INTO account_logs (account_id, log_id, epoch, head_seq, head_digest) VALUES (?, ?, 1, 0, ?)",
		a.ID, newUUID(), make([]byte, 32)); err != nil {
		return Account{}, err
	}
	return a, nil
}
