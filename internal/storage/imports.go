package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

// Library imports (PROTOCOL §11.5).

// Import is an account's library import.
type Import struct {
	AccountID, ImportID string
	Kind                string // android_db or opml
	Fingerprint         string // SHA-256 of the file, lowercase hex
	Size                int64
	FileName            string
	Status              string // uploading, processing, published or failed
	Reason              string // a failure's reason
	Epoch               int64  // the account's epoch when reserved
	Incarnation         string // the server's incarnation when reserved
	Progress            int    // podcasts applied in staging
	Result              string // the published result (canonical JSON)
	CreatedAt           int64
	UpdatedAt           int64
}

// Errors of ReserveImport.
var (
	ErrImportExists    = errors.New("storage: another import is reserved or published in this account")
	ErrAccountNotEmpty = errors.New("storage: the account already has a library")
)

const importColumns = `account_id, import_id, kind, fingerprint, size, file_name, status, COALESCE(reason, ''), epoch, incarnation, progress,
	COALESCE(result, ''), created_at, updated_at`

func scanImport(row interface{ Scan(...any) error }) (Import, error) {
	var im Import
	err := row.Scan(&im.AccountID, &im.ImportID, &im.Kind, &im.Fingerprint, &im.Size, &im.FileName, &im.Status, &im.Reason, &im.Epoch,
		&im.Incarnation, &im.Progress, &im.Result, &im.CreatedAt, &im.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Import{}, ErrNotFound
	}
	return im, err
}

// Empty reports whether the account has no library: no facts and no
// decisions (PROTOCOL §5 note: logins, installations and sessions don't
// count).
func (a *AccountTx) Empty() (bool, error) {
	var has bool
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT EXISTS (SELECT 1 FROM facts WHERE account_id = ?) OR EXISTS (SELECT 1 FROM decisions WHERE account_id = ?)`,
		a.id, a.id).Scan(&has)
	return !has, err
}

// ReserveImport claims the empty account for an import in one transaction
// (PROTOCOL §5: atomicity). The same fingerprint as the account's reserved or
// published import returns that import (created false); any other live import
// is ErrImportExists; an account with a library is ErrAccountNotEmpty.
func (a *AccountTx) ReserveImport(im Import) (Import, bool, error) {
	live, err := scanImport(a.w.tx.QueryRowContext(a.w.ctx, `SELECT `+importColumns+` FROM imports
		WHERE account_id = ? AND status IN ('uploading', 'processing', 'published')`, a.id))
	switch {
	case err == nil && live.Fingerprint == im.Fingerprint && live.Kind == im.Kind:
		return live, false, nil
	case err == nil:
		return live, false, ErrImportExists
	case !errors.Is(err, ErrNotFound):
		return Import{}, false, err
	}
	empty, err := a.Empty()
	if err != nil {
		return Import{}, false, err
	}
	if !empty {
		return Import{}, false, ErrAccountNotEmpty
	}
	head, err := a.Head()
	if err != nil {
		return Import{}, false, err
	}
	var incarnation string
	if err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COALESCE((SELECT value FROM server_meta WHERE key = 'incarnation'), '')`).Scan(&incarnation); err != nil {
		return Import{}, false, err
	}
	now := a.w.nowMillis()
	im.AccountID, im.Status, im.Epoch, im.Incarnation, im.CreatedAt, im.UpdatedAt = a.id, "uploading", head.Epoch, incarnation, now, now
	_, err = a.w.tx.ExecContext(a.w.ctx, `INSERT INTO imports (account_id, import_id, kind, fingerprint, size, file_name, status, epoch, incarnation,
		created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.id, im.ImportID, im.Kind, im.Fingerprint, im.Size, im.FileName, im.Status, im.Epoch, im.Incarnation, now, now)
	return im, err == nil, err
}

// Import returns one of the account's imports.
func (a *AccountTx) Import(importID string) (Import, error) {
	return scanImport(a.w.tx.QueryRowContext(a.w.ctx, `SELECT `+importColumns+` FROM imports WHERE account_id = ? AND import_id = ?`, a.id, importID))
}

// ImportIDs lists the account's imports, whatever their status: deleting
// the account removes their files.
func (a *AccountTx) ImportIDs() ([]string, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT import_id FROM imports WHERE account_id = ? ORDER BY import_id`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Import returns one of the account's imports.
func (a *AccountReader) Import(importID string) (Import, error) {
	return scanImport(a.r.tx.QueryRowContext(a.r.ctx, `SELECT `+importColumns+` FROM imports WHERE account_id = ? AND import_id = ?`, a.id, importID))
}

// CurrentImport returns the account's live import, else its latest failed
// one (ErrNotFound for none).
func (a *AccountReader) CurrentImport() (Import, error) {
	return scanImport(a.r.tx.QueryRowContext(a.r.ctx, `SELECT `+importColumns+` FROM imports WHERE account_id = ?
		ORDER BY status = 'failed', updated_at DESC, import_id LIMIT 1`, a.id))
}

// ImportReserved reports an import in progress: sync writes into the account
// wait until it is published or fails (PROTOCOL §11.5).
func (a *AccountTx) ImportReserved() (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM imports WHERE account_id = ? AND status IN ('uploading', 'processing')`, a.id).Scan(&n)
	return n > 0, err
}

// SetImportStatus moves an import on; a failure releases the reservation.
func (a *AccountTx) SetImportStatus(importID, status, reason string) error {
	var r any
	if reason != "" {
		r = reason
	}
	return a.execOne(`UPDATE imports SET status = ?, reason = ?, updated_at = ? WHERE account_id = ? AND import_id = ?`,
		status, r, a.w.nowMillis(), a.id, importID)
}

// SetImportProgress records how many podcasts staging has applied.
func (a *AccountTx) SetImportProgress(importID string, n int) error {
	return a.execOne(`UPDATE imports SET progress = ?, updated_at = ? WHERE account_id = ? AND import_id = ?`, n, a.w.nowMillis(), a.id, importID)
}

// UnfinishedImports lists every import still uploading or processing, in
// every account (startup resumes or fails them, PROTOCOL §11.5).
func (s *Store) UnfinishedImports(ctx context.Context) ([]Import, error) {
	rows, err := s.reader.QueryContext(ctx, `SELECT `+importColumns+` FROM imports WHERE status IN ('uploading', 'processing') ORDER BY created_at, import_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Import
	for rows.Next() {
		im, err := scanImport(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, im)
	}
	return out, rows.Err()
}

// FailUnfinishedImports marks every import still uploading or processing as
// failed (interrupted_by_restore) and releases its reservation; a restore
// transition calls it (PROTOCOL §11).
func (w *WriteTx) FailUnfinishedImports() error {
	_, err := w.tx.ExecContext(w.ctx, `UPDATE imports SET status = 'failed', reason = 'interrupted_by_restore', updated_at = ?
		WHERE status IN ('uploading', 'processing')`, w.nowMillis())
	return err
}

// MirrorAccount creates, in a staging database, the account an import
// targets with its IDs and its log's head, so staged commits chain from that
// head (PROTOCOL §11.5).
func (w *WriteTx) MirrorAccount(accountID, namespace string, head Head) error {
	if _, err := w.tx.ExecContext(w.ctx, "INSERT INTO accounts (account_id, namespace, state, created_at) VALUES (?, ?, 'active', ?)",
		accountID, namespace, w.nowMillis()); err != nil {
		return err
	}
	_, err := w.tx.ExecContext(w.ctx, "INSERT INTO account_logs (account_id, log_id, epoch, head_seq, head_digest) VALUES (?, ?, ?, ?, ?)",
		accountID, head.LogID, head.Epoch, head.Seq, head.Digest)
	return err
}

// importTables are the library tables publication copies from the staging
// database, parents first (PROTOCOL §11.5). Every account-scoped table is either
// here or in importExcluded (TestImportTablesCoverEveryAccountTable).
var importTables = []string{
	"causal_nodes", "causal_edges", "node_order", "facts", "superseded_receipts", "author_positions", "decisions", "log_commits",
	"podcasts", "generations", "podcast_aliases", "episodes", "contributions", "records", "secrets",
	"listening_sessions", "listening_facts", "queue_frontier", "queue_steps", "queue_events", "queue_keys", "causal_frontier",
	"fact_authors", "author_lineage", "session_continuations", "session_facts",
	"feed_responses", "catalog_items", "generation_population", "generation_joins", "refresh_schedule",
	"identity_occurrences", "identity_aliases", "identity_disputes", "identity_releases", "identity_release_additions", "episode_merges",
	"podcast_merges",
}

// importExcluded are the account tables publication never copies: the
// account's identity, administrative and authentication state, transport
// state and the imports themselves stay as the live database has them; only
// the log head moves.
var importExcluded = []string{"accounts", "account_logs", "account_logins", "installations", "auth_tokens", "transport_slots", "push_parts", "imports",
	"recoveries", "recovery_frames"}

// ErrImportChanged reports that the target account changed since staging
// began (its head moved, it gained a library, or the import is no longer
// processing): staging restarts from the new head.
var ErrImportChanged = errors.New("storage: the account changed since the import was staged")

var publishMu sync.Mutex

// PublishImport makes a staged import the account's library in one
// transaction (PROTOCOL §5 atomicity; §11.5): it requires the account's head to
// be base (the head staging began from), the account to be empty and the
// import to be processing; it copies the staged account's rows of every
// importTables table and its commits after base, moves the head to the staged
// head, and marks the import published with its result.
func (s *Store) PublishImport(ctx context.Context, staging, accountID, importID string, base Head, result string) error {
	publishMu.Lock()
	defer publishMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	// ATTACH is refused inside a transaction; the writer pool has one
	// connection, so the attachment holds for the write below.
	if _, err := s.writer.ExecContext(ctx, "ATTACH DATABASE ? AS staging", staging); err != nil {
		return fmt.Errorf("storage: attaching the staged import: %w", err)
	}
	defer s.writer.ExecContext(context.Background(), "DETACH DATABASE staging")
	return s.Write(ctx, func(tx *WriteTx) error {
		if _, err := tx.tx.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON"); err != nil {
			return err
		}
		var mainVersion, stagedVersion int
		if err := tx.tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM main.schema_migrations").Scan(&mainVersion); err != nil {
			return err
		}
		if err := tx.tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM staging.schema_migrations").Scan(&stagedVersion); err != nil {
			return err
		}
		if mainVersion != stagedVersion {
			return fmt.Errorf("%w: staged with schema %d, live %d", ErrImportChanged, stagedVersion, mainVersion)
		}
		tx.markChanged(accountID) // the account's log advances: wake its listeners (PROTOCOL §12)
		acct := tx.Account(accountID)
		head, err := acct.Head()
		if err != nil {
			return err
		}
		if head.LogID != base.LogID || head.Epoch != base.Epoch || head.Seq != base.Seq || !bytes.Equal(head.Digest, base.Digest) {
			return ErrImportChanged
		}
		if empty, err := acct.Empty(); err != nil {
			return err
		} else if !empty {
			return ErrImportChanged
		}
		im, err := acct.Import(importID)
		if err != nil {
			return err
		}
		if im.Status != "processing" {
			return ErrImportChanged
		}
		var staged Head
		if err := tx.tx.QueryRowContext(ctx, `SELECT log_id, epoch, head_seq, head_digest FROM staging.account_logs WHERE account_id = ?`, accountID).
			Scan(&staged.LogID, &staged.Epoch, &staged.Seq, &staged.Digest); err != nil {
			return fmt.Errorf("storage: the staged import has no account: %w", err)
		}
		if staged.LogID != base.LogID || staged.Epoch != base.Epoch || staged.Seq < base.Seq {
			return ErrImportChanged
		}
		for _, table := range importTables {
			q := `INSERT INTO main.` + table + ` SELECT * FROM staging.` + table + ` WHERE account_id = ?`
			args := []any{accountID}
			if table == "log_commits" {
				q += ` AND seq > ?`
				args = append(args, base.Seq)
			}
			if _, err := tx.tx.ExecContext(ctx, q, args...); err != nil {
				return fmt.Errorf("storage: publishing %s: %w", table, err)
			}
		}
		if err := acct.execOne(`UPDATE account_logs SET head_seq = ?, head_digest = ? WHERE account_id = ?`, staged.Seq, staged.Digest, accountID); err != nil {
			return err
		}
		return acct.execOne(`UPDATE imports SET status = 'published', result = ?, updated_at = ? WHERE account_id = ? AND import_id = ?`,
			result, s.nowMillis(), accountID, importID)
	})
}

// SetMeta writes a server_meta value (a staging database records the head
// its import began from).
func (w *WriteTx) SetMeta(key, value string) error {
	_, err := w.tx.ExecContext(w.ctx, `INSERT INTO server_meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// Meta reads a server_meta value ("" when absent).
func (s *Store) Meta(ctx context.Context, key string) (string, error) {
	var v string
	err := s.reader.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM server_meta WHERE key = ?), '')`, key).Scan(&v)
	return v, err
}

// DecisionsOfType returns the account's decisions of one type, canonical
// bytes in rank order.
func (a *AccountReader) DecisionsOfType(decisionType string) ([][]byte, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT decision FROM decisions WHERE account_id = ? AND decision_type = ? ORDER BY epoch, seq, decision_id`,
		a.id, decisionType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var d []byte
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Namespace returns the account's namespace.
func (a *AccountReader) Namespace() (string, error) {
	var ns string
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT namespace FROM accounts WHERE account_id = ?`, a.id).Scan(&ns)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNoAccount
	}
	return ns, err
}
