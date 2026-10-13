package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Rebuilds in a staging database (PROTOCOL §6.11, the staging fallback): a large account is
// rebuilt, and a recovery request joined, in a copy of the database, which takes no write
// lock; publication then replaces the account's derived state and archive with the copy's
// in one short write transaction, if the account's head is still the one the copy began
// from (PROTOCOL §11.5's rule).

// rebuildStagingDir is the data directory's folder of staged rebuilds; a
// start removes what a stopped one left.
const rebuildStagingDir = "rebuild-staging"

// RebuildStagingDir is an account's staged-rebuild folder.
func (s *Store) RebuildStagingDir(accountID string) string {
	return filepath.Join(s.dir, rebuildStagingDir, accountID)
}

// StageCopy copies one read snapshot of the database into dir and opens it
// as a staging database that signs with this server's identity and keys
// (OpenStaging). The caller closes it and removes dir.
func (s *Store) StageCopy(ctx context.Context, dir string) (*Store, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := private.RealDir(dir); err != nil {
		return nil, err
	}
	if err := s.copyInto(ctx, filepath.Join(dir, databaseName)); err != nil {
		return nil, err
	}
	id, err := s.ServerID(ctx)
	if err != nil {
		return nil, err
	}
	return OpenStaging(ctx, dir, id, s.receiptKeys, WithClock(s.now))
}

// rebuildPublished are the account tables a staged rebuild or recovery
// replaces wholesale: its derived state and its archive. Feed responses and
// refresh schedules, which change without a commit, stay as the live
// database has them; secrets and recovery parts are merged.
var rebuildPublished = publishedTables()

// publishedTables lists rebuildPublished children before parents (the order
// of deletion; insertion runs the other way, parents first, so no deferred
// foreign-key check ever waits for a parent: SQLite would look for the
// children of each parent inserted later by a scan).
func publishedTables() []string {
	var out []string
	for _, t := range rebuildCleared {
		if t != "causal_nodes" {
			out = append(out, t)
		}
	}
	return append(out, "records", "decisions", "causal_nodes", "facts", "superseded_receipts", "recoveries")
}

// PublishStaged makes a staged rebuild the account's state in one write
// transaction: it requires the account's head to be base, replaces the
// account's rows of every rebuildPublished table with the staging database's,
// adds its secrets and push parts, appends its commits after base, and moves
// the head. ErrImportChanged reports a head that moved (the caller stages
// again).
func (s *Store) PublishStaged(ctx context.Context, staging, accountID string, base Head, check func(*WriteTx) error) error {
	publishMu.Lock()
	defer publishMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if _, err := s.writer.ExecContext(ctx, "ATTACH DATABASE ? AS staging", staging); err != nil {
		return fmt.Errorf("storage: attaching the staged rebuild: %w", err)
	}
	defer s.writer.ExecContext(context.Background(), "DETACH DATABASE staging")
	return s.Write(ctx, func(tx *WriteTx) error {
		if check != nil {
			if err := check(tx); err != nil {
				return err
			}
		}
		if _, err := tx.tx.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON"); err != nil {
			return err
		}
		acct := tx.Account(accountID)
		head, err := acct.Head()
		if err != nil {
			return err
		}
		if head.LogID != base.LogID || head.Epoch != base.Epoch || head.Seq != base.Seq || !bytes.Equal(head.Digest, base.Digest) {
			return ErrImportChanged
		}
		var staged Head
		if err := tx.tx.QueryRowContext(ctx, `SELECT log_id, epoch, head_seq, head_digest FROM staging.account_logs WHERE account_id = ?`, accountID).
			Scan(&staged.LogID, &staged.Epoch, &staged.Seq, &staged.Digest); err != nil {
			return fmt.Errorf("storage: the staged rebuild has no account: %w", err)
		}
		if staged.LogID != base.LogID || staged.Seq < base.Seq {
			return ErrImportChanged
		}
		if staged.Epoch != head.Epoch {
			// Epoch evidence moved the staged copy (PROTOCOL §6.11): the live server
			// runs that transition in place instead.
			return ErrStagedEvidence
		}
		tx.markChanged(accountID)
		// Feed responses and refresh schedules change without a commit (the
		// refresher writes them during staging too): the live rows stay,
		// though deleting the account's podcasts cascades to them.
		for _, table := range []string{"feed_responses", "refresh_schedule"} {
			if _, err := tx.tx.ExecContext(ctx, `DROP TABLE IF EXISTS temp.staged_`+table); err != nil {
				return err
			}
			if _, err := tx.tx.ExecContext(ctx, `CREATE TEMP TABLE staged_`+table+` AS SELECT * FROM main.`+table+` WHERE account_id = ?`,
				accountID); err != nil {
				return err
			}
		}
		for _, table := range rebuildPublished {
			if _, err := tx.tx.ExecContext(ctx, `DELETE FROM main.`+table+` WHERE account_id = ?`, accountID); err != nil {
				return fmt.Errorf("storage: publishing %s: %w", table, err)
			}
		}
		for i := len(rebuildPublished) - 1; i >= 0; i-- {
			table := rebuildPublished[i]
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO main.`+table+` SELECT * FROM staging.`+table+` WHERE account_id = ?`,
				accountID); err != nil {
				return fmt.Errorf("storage: publishing %s: %w", table, err)
			}
		}
		for _, table := range []string{"feed_responses", "refresh_schedule"} {
			if _, err := tx.tx.ExecContext(ctx, `INSERT OR REPLACE INTO main.`+table+` SELECT * FROM temp.staged_`+table+` t
				WHERE EXISTS (SELECT 1 FROM main.podcasts p WHERE p.account_id = t.account_id AND p.podcast_id = t.podcast_id)`); err != nil {
				return fmt.Errorf("storage: publishing %s: %w", table, err)
			}
			if _, err := tx.tx.ExecContext(ctx, `DROP TABLE temp.staged_`+table); err != nil {
				return err
			}
		}
		if _, err := tx.tx.ExecContext(ctx, `INSERT OR REPLACE INTO main.secrets SELECT * FROM staging.secrets WHERE account_id = ?`, accountID); err != nil {
			return fmt.Errorf("storage: publishing secrets: %w", err)
		}
		// A recovery's frames and their parts (negative slots) are the staged
		// request's result: completed frames are gone, incomplete ones stored.
		// Push parts (positive slots) stay as the live database has them.
		if _, err := tx.tx.ExecContext(ctx, `DELETE FROM main.push_parts WHERE account_id = ? AND install_seq < 0`, accountID); err != nil {
			return err
		}
		if _, err := tx.tx.ExecContext(ctx, `DELETE FROM main.recovery_frames WHERE account_id = ?`, accountID); err != nil {
			return err
		}
		if _, err := tx.tx.ExecContext(ctx, `INSERT INTO main.push_parts SELECT * FROM staging.push_parts WHERE account_id = ? AND install_seq < 0`,
			accountID); err != nil {
			return fmt.Errorf("storage: publishing parts: %w", err)
		}
		if _, err := tx.tx.ExecContext(ctx, `INSERT INTO main.recovery_frames SELECT * FROM staging.recovery_frames WHERE account_id = ?`,
			accountID); err != nil {
			return fmt.Errorf("storage: publishing recovery frames: %w", err)
		}
		if _, err := tx.tx.ExecContext(ctx, `INSERT INTO main.log_commits SELECT * FROM staging.log_commits WHERE account_id = ? AND seq > ?`,
			accountID, base.Seq); err != nil {
			return fmt.Errorf("storage: publishing the log: %w", err)
		}
		return acct.execOne(`UPDATE account_logs SET head_seq = ?, head_digest = ? WHERE account_id = ?`, staged.Seq, staged.Digest, accountID)
	})
}

// ErrStagedEvidence reports a staged recovery whose entries moved the copy to
// a new epoch: the live server joins them in place, running the transition.
var ErrStagedEvidence = errors.New("storage: a staged recovery proved a lost timeline")

// ArchiveSize counts the account's archived facts and decisions.
func (s *Store) ArchiveSize(ctx context.Context, accountID string) (int64, error) {
	var n int64
	err := s.reader.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM facts WHERE account_id = ?1) + (SELECT COUNT(*) FROM decisions WHERE account_id = ?1)`,
		accountID).Scan(&n)
	return n, err
}

// Head reads an account's log head.
func (s *Store) Head(ctx context.Context, accountID string) (Head, error) {
	var h Head
	err := s.Read(ctx, func(r *ReadTx) error {
		var err error
		h, err = r.Account(accountID).Head()
		return err
	})
	return h, err
}

// RebuildCost estimates a rebuild of the account: its archived
// entries, and the concentration of its scopes (the sum over scopes of their
// contributions squared: a fact's refold reads its whole scope, so a long
// history in one episode costs quadratically).
func (s *Store) RebuildCost(ctx context.Context, accountID string) (entries, concentration int64, err error) {
	if entries, err = s.ArchiveSize(ctx, accountID); err != nil {
		return 0, 0, err
	}
	err = s.reader.QueryRowContext(ctx, `SELECT COALESCE(SUM(n * n), 0) FROM (SELECT COUNT(*) AS n FROM contributions WHERE account_id = ? GROUP BY scope)`,
		accountID).Scan(&concentration)
	return entries, concentration, err
}
