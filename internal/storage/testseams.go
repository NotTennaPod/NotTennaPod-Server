package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// Seams for the read model's tests, which no server code calls (a source
// test in internal/views checks).

// RawReadForTesting gives the read model's test oracle
// (internal/views/reference) the read transaction itself, so that it runs
// its own frozen copies of the readers' queries and never shares SQL with
// the code it checks.
func RawReadForTesting(r *ReadTx) (context.Context, *sql.Tx) { return r.ctx, r.tx }

// UnregisterEpisodeForTesting removes an episode's registration, a change
// to the read model's inputs that only bulk rewrites make in the server;
// the read model's randomized tests make it a routine one. It reports
// whether the episode was registered.
func (a *AccountTx) UnregisterEpisodeForTesting(episodeID string) (bool, error) {
	res, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM episodes WHERE account_id = ? AND episode_id = ?`, a.id, episodeID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// VerifyReadForTesting runs fn in a read transaction on a connection of its
// own, outside the readers' pool: the read model's commit observer checks a
// commit while the store's write gate is held, and must not wait for a pool
// slot that a reader waiting on that write holds.
func (s *Store) VerifyReadForTesting(ctx context.Context, fn func(*ReadTx) error) error {
	if s.closed.Load() {
		return ErrClosed
	}
	s.verifyMu.Lock()
	if s.verify == nil {
		db, err := openDB(s.dbPath, true)
		if err != nil {
			s.verifyMu.Unlock()
			return err
		}
		db.SetMaxOpenConns(1)
		s.verify = db
	}
	db := s.verify
	s.verifyMu.Unlock()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("storage: begin verification read: %w", err)
	}
	defer tx.Rollback()
	return fn(&ReadTx{ctx: ctx, tx: tx})
}
