package storage

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
)

// The test-only audit of the read model's change capture (viewchanges.go).
// When it is on, TEMP triggers of the audit's own on the writer connection
// (outside the schema check, which reads main only) record the entity of
// every change to a view input, and the account of every change to an
// account's log identity or capture clock, and every write transaction
// checks before it commits that:
//   - each change of an account the transaction did not declare
//     (SuspendViewCapture) is stamped above the account's tick at its start
//     and at or below its tick now, and that tick moved forward;
//   - no account has a stamp above its tick;
//   - each declared account that still exists is one generation higher, its
//     tick higher and its old stamps gone, and each declared account deleted
//     has neither clock nor stamps;
//   - no other account's generation moved, nor its tick went back, nor its
//     clock went away.
//
// The audit's triggers also refuse a write to a view input or a capture
// clock made while no Store.Write is under way, and a write refuses to
// start over evidence of changes no write checked (an account's log changed
// outside Store.Write).
//
// A failed check fails the transaction. The audit is written apart from the
// capture triggers, so that a mistake in one is not repeated in the other.

var viewAuditOn atomic.Bool

// AuditViewCaptureForTesting turns the capture audit on for every store of
// this process. Tests call it from TestMain; a server never does.
func AuditViewCaptureForTesting() { viewAuditOn.Store(true) }

// viewAudit is an audited write transaction's state.
type viewAudit struct {
	all bool // every account's clock was raised (the repair at open)
}

// auditGuard opens every audit trigger: a write made while no Store.Write is
// under way fails.
const auditGuard = `SELECT RAISE(ABORT, 'storage: a view input or capture clock was written outside Store.Write')
			WHERE NOT EXISTS (SELECT 1 FROM temp.view_audit_active);
		`

// viewAuditTables are the audit's TEMP tables: the entities a transaction
// changed, the accounts whose identity (log or clock) it changed, the clocks
// at its start, and the mark of a Store.Write under way.
var viewAuditTables = []string{
	`CREATE TEMP TABLE IF NOT EXISTS view_audit (account_id TEXT, entity TEXT, entity_id TEXT)`,
	`CREATE TEMP TABLE IF NOT EXISTS view_audit_accounts (account_id TEXT)`,
	`CREATE TEMP TABLE IF NOT EXISTS view_audit_clock (account_id TEXT PRIMARY KEY, tick INTEGER, generation INTEGER)`,
	`CREATE TEMP TABLE IF NOT EXISTS view_audit_active (on_write INTEGER)`,
}

// viewAuditTriggers record, in temp.view_audit, the entity of each row
// change of a view input (an update's new and old entity), and in
// temp.view_audit_accounts each account created, deleted, or whose log or
// capture clock changed.
var viewAuditTriggers = []string{
	`CREATE TEMP TRIGGER view_audit_records_insert AFTER INSERT ON main.records BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, NEW.entity, NEW.entity_id); END`,
	`CREATE TEMP TRIGGER view_audit_records_update AFTER UPDATE ON main.records
		WHEN NOT (NEW.value IS OLD.value AND NEW.provenance IS OLD.provenance AND NEW.account_id IS OLD.account_id
			AND NEW.entity IS OLD.entity AND NEW.entity_id IS OLD.entity_id AND NEW.field_group IS OLD.field_group) BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, NEW.entity, NEW.entity_id), (OLD.account_id, OLD.entity, OLD.entity_id); END`,
	`CREATE TEMP TRIGGER view_audit_records_delete AFTER DELETE ON main.records BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (OLD.account_id, OLD.entity, OLD.entity_id); END`,
	`CREATE TEMP TRIGGER view_audit_catalog_insert AFTER INSERT ON main.catalog_items BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, 'episode', NEW.episode_id); END`,
	`CREATE TEMP TRIGGER view_audit_catalog_update AFTER UPDATE ON main.catalog_items
		WHEN NOT (NEW.pub_date IS OLD.pub_date AND NEW.rowid IS OLD.rowid AND NEW.account_id IS OLD.account_id AND NEW.episode_id IS OLD.episode_id) BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, 'episode', NEW.episode_id), (OLD.account_id, 'episode', OLD.episode_id); END`,
	`CREATE TEMP TRIGGER view_audit_catalog_delete AFTER DELETE ON main.catalog_items BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (OLD.account_id, 'episode', OLD.episode_id); END`,
	`CREATE TEMP TRIGGER view_audit_episodes_insert AFTER INSERT ON main.episodes BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, 'episode', NEW.episode_id); END`,
	`CREATE TEMP TRIGGER view_audit_episodes_update AFTER UPDATE ON main.episodes
		WHEN NOT (NEW.account_id IS OLD.account_id AND NEW.episode_id IS OLD.episode_id) BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, 'episode', NEW.episode_id), (OLD.account_id, 'episode', OLD.episode_id); END`,
	`CREATE TEMP TRIGGER view_audit_episodes_delete AFTER DELETE ON main.episodes BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (OLD.account_id, 'episode', OLD.episode_id); END`,
	`CREATE TEMP TRIGGER view_audit_podcasts_insert AFTER INSERT ON main.podcasts BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, 'podcast', NEW.podcast_id); END`,
	`CREATE TEMP TRIGGER view_audit_podcasts_update AFTER UPDATE ON main.podcasts
		WHEN NOT (NEW.account_id IS OLD.account_id AND NEW.podcast_id IS OLD.podcast_id AND NEW.creating_op IS OLD.creating_op) BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (NEW.account_id, 'podcast', NEW.podcast_id), (OLD.account_id, 'podcast', OLD.podcast_id); END`,
	`CREATE TEMP TRIGGER view_audit_podcasts_delete AFTER DELETE ON main.podcasts BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit VALUES (OLD.account_id, 'podcast', OLD.podcast_id); END`,
	`CREATE TEMP TRIGGER view_audit_node_order_insert AFTER INSERT ON main.node_order BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit SELECT account_id, 'podcast', podcast_id FROM main.podcasts
			WHERE account_id = NEW.account_id AND creating_op = NEW.node_id; END`,
	`CREATE TEMP TRIGGER view_audit_node_order_update AFTER UPDATE ON main.node_order
		WHEN NOT (NEW.rank IS OLD.rank AND NEW.account_id IS OLD.account_id AND NEW.node_id IS OLD.node_id) BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit SELECT account_id, 'podcast', podcast_id FROM main.podcasts
			WHERE (account_id = NEW.account_id AND creating_op = NEW.node_id) OR (account_id = OLD.account_id AND creating_op = OLD.node_id); END`,
	`CREATE TEMP TRIGGER view_audit_node_order_delete AFTER DELETE ON main.node_order BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit SELECT account_id, 'podcast', podcast_id FROM main.podcasts
			WHERE account_id = OLD.account_id AND creating_op = OLD.node_id; END`,
	// Accounts created or deleted, and changes of an account's view key:
	// its log identity (log, epoch, head) or its capture clock.
	`CREATE TEMP TRIGGER view_audit_accounts_insert AFTER INSERT ON main.accounts BEGIN
		INSERT INTO temp.view_audit_accounts VALUES (NEW.account_id); END`,
	`CREATE TEMP TRIGGER view_audit_accounts_delete AFTER DELETE ON main.accounts BEGIN
		INSERT INTO temp.view_audit_accounts VALUES (OLD.account_id); END`,
	`CREATE TEMP TRIGGER view_audit_logs_update AFTER UPDATE ON main.account_logs BEGIN
		INSERT INTO temp.view_audit_accounts VALUES (NEW.account_id), (OLD.account_id); END`,
	`CREATE TEMP TRIGGER view_audit_clock_insert AFTER INSERT ON main.view_clock BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit_accounts VALUES (NEW.account_id); END`,
	`CREATE TEMP TRIGGER view_audit_clock_update AFTER UPDATE ON main.view_clock BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit_accounts VALUES (NEW.account_id), (OLD.account_id); END`,
	`CREATE TEMP TRIGGER view_audit_clock_delete AFTER DELETE ON main.view_clock BEGIN ` + auditGuard + `
		INSERT INTO temp.view_audit_accounts VALUES (OLD.account_id); END`,
}

type auditQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// installViewAudit creates the audit's TEMP objects on the writer
// connection when they are missing or incomplete. A store opens with them
// installed outside any transaction (they then survive a transaction that
// rolls back), and each write installs them again if they went away.
func installViewAudit(ctx context.Context, q auditQuerier) error {
	var installed int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM temp.sqlite_schema WHERE type = 'trigger' AND name LIKE 'view\_audit\_%' ESCAPE '\'`).
		Scan(&installed); err != nil {
		return err
	}
	if installed == len(viewAuditTriggers) {
		return nil
	}
	names, err := queryStrings(ctx, q, `SELECT name FROM temp.sqlite_schema WHERE type = 'trigger' AND name LIKE 'view\_audit\_%' ESCAPE '\'`)
	if err != nil {
		return err
	}
	for _, n := range names {
		if _, err := q.ExecContext(ctx, `DROP TRIGGER temp.`+n); err != nil {
			return err
		}
	}
	for _, s := range append(slices.Clone(viewAuditTables), viewAuditTriggers...) {
		if _, err := q.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("storage: installing the capture audit: %w", err)
		}
	}
	return nil
}

// auditReady reports a schema with the capture (migration 0025).
func auditReady(ctx context.Context, q auditQuerier) (bool, error) {
	var ready bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = 'view_clock')`).Scan(&ready)
	return ready, err
}

// openViewAudit installs the audit's objects as a store opens.
func (s *Store) openViewAudit(ctx context.Context) error {
	if !viewAuditOn.Load() || !s.capture {
		return nil
	}
	return installViewAudit(ctx, s.writer)
}

// beginViewAudit starts the audit of a write transaction, once the schema
// has the capture: it installs the audit's objects when the connection lacks
// them, refuses to start over evidence an earlier write left unchecked,
// marks a Store.Write under way and keeps every account's clock as it is.
func (w *WriteTx) beginViewAudit() error {
	if !viewAuditOn.Load() {
		return nil
	}
	ctx, tx := w.ctx, w.tx
	if ready, err := auditReady(ctx, tx); err != nil || !ready {
		return err
	}
	if err := installViewAudit(ctx, tx); err != nil {
		return err
	}
	var left int
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM temp.view_audit) + (SELECT COUNT(*) FROM temp.view_audit_accounts)
		+ (SELECT COUNT(*) FROM temp.view_audit_active)`).Scan(&left); err != nil {
		return err
	}
	if left != 0 {
		return fmt.Errorf("storage: read-model capture audit: %d changes were made since the last write was checked", left)
	}
	for _, q := range []string{`DELETE FROM temp.view_audit_clock`, `INSERT INTO temp.view_audit_clock SELECT account_id, tick, generation FROM main.view_clock`,
		`INSERT INTO temp.view_audit_active VALUES (1)`} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	w.audit = &viewAudit{}
	return nil
}

// endViewAudit clears a checked transaction's evidence and its mark, just
// before the commit.
func (w *WriteTx) endViewAudit() error {
	if w.audit == nil {
		return nil
	}
	for _, q := range []string{`DELETE FROM temp.view_audit`, `DELETE FROM temp.view_audit_accounts`, `DELETE FROM temp.view_audit_clock`,
		`DELETE FROM temp.view_audit_active`} {
		if _, err := w.tx.ExecContext(w.ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// checkViewAudit checks an audited transaction's capture before it commits.
func (w *WriteTx) checkViewAudit() error {
	if w.audit == nil {
		return nil
	}
	ctx, tx := w.ctx, w.tx
	declared := map[string]bool{}
	if w.capture != nil {
		for _, id := range w.capture.accounts {
			declared[id] = true
		}
	}
	if w.audit.all {
		ids, err := queryStrings(ctx, tx, `SELECT account_id FROM accounts`)
		if err != nil {
			return err
		}
		for _, id := range ids {
			declared[id] = true
		}
	}
	var problems []string
	// Each changed entity of an undeclared account is stamped within
	// (its tick at the start, its tick now].
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT a.account_id, a.entity, a.entity_id FROM temp.view_audit a
		WHERE NOT EXISTS (SELECT 1 FROM main.view_changes c WHERE c.account_id = a.account_id AND c.entity = a.entity AND c.entity_id = a.entity_id
			AND c.tick > COALESCE((SELECT s.tick FROM temp.view_audit_clock s WHERE s.account_id = a.account_id), 0)
			AND c.tick <= COALESCE((SELECT k.tick FROM main.view_clock k WHERE k.account_id = a.account_id), 0))
		ORDER BY 1, 2, 3`)
	if err != nil {
		return err
	}
	changed := map[string]bool{}
	for rows.Next() {
		var account, entity, id string
		if err := rows.Scan(&account, &entity, &id); err != nil {
			rows.Close()
			return err
		}
		if !declared[account] {
			problems = append(problems, fmt.Sprintf("the change of %s %s of account %s is not stamped within the transaction's ticks", entity, id, account))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	changedIDs, err := queryStrings(ctx, tx, `SELECT DISTINCT account_id FROM temp.view_audit`)
	if err != nil {
		return err
	}
	for _, id := range changedIDs {
		changed[id] = true
	}
	clocks := func(q string) (map[string]ViewClock, error) {
		rows, err := tx.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[string]ViewClock{}
		for rows.Next() {
			var id string
			var c ViewClock
			if err := rows.Scan(&id, &c.Tick, &c.Generation); err != nil {
				return nil, err
			}
			out[id] = c
		}
		return out, rows.Err()
	}
	start, err := clocks(`SELECT account_id, tick, generation FROM temp.view_audit_clock`)
	if err != nil {
		return err
	}
	now, err := clocks(`SELECT account_id, tick, generation FROM main.view_clock`)
	if err != nil {
		return err
	}
	ids := map[string]bool{}
	for id := range start {
		ids[id] = true
	}
	for id := range now {
		ids[id] = true
	}
	for id := range declared {
		ids[id] = true
	}
	for id := range changed {
		ids[id] = true
	}
	for id := range ids {
		s, hadClock := start[id]
		n, hasClock := now[id]
		var future int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM main.view_changes WHERE account_id = ? AND tick > ?`, id, n.Tick).Scan(&future); err != nil {
			return err
		}
		if future > 0 {
			problems = append(problems, fmt.Sprintf("account %s has %d stamps above its tick %d", id, future, n.Tick))
		}
		if !declared[id] {
			switch {
			case hadClock && !hasClock:
				problems = append(problems, fmt.Sprintf("the clock of account %s went away undeclared", id))
			case hasClock && n.Generation != s.Generation:
				problems = append(problems, fmt.Sprintf("the generation of account %s moved from %d to %d undeclared", id, s.Generation, n.Generation))
			case hasClock && n.Tick < s.Tick:
				problems = append(problems, fmt.Sprintf("the tick of account %s went back from %d to %d", id, s.Tick, n.Tick))
			case changed[id] && n.Tick <= s.Tick:
				problems = append(problems, fmt.Sprintf("account %s changed but its tick stayed at %d", id, n.Tick))
			}
			continue
		}
		var exists, stamps bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM accounts WHERE account_id = ?1), EXISTS (SELECT 1 FROM view_changes WHERE account_id = ?1)`,
			id).Scan(&exists, &stamps); err != nil {
			return err
		}
		switch {
		case exists && (!hasClock || n.Generation != s.Generation+1 || n.Tick <= s.Tick):
			problems = append(problems, fmt.Sprintf("account %s was declared but its clock went from %+v to %+v (present %v)", id, s, n, hasClock))
		case exists && stamps:
			problems = append(problems, fmt.Sprintf("account %s was declared but keeps stamps of an older generation", id))
		case !exists && (hasClock || stamps):
			problems = append(problems, fmt.Sprintf("account %s was deleted but keeps its clock (%v) or stamps (%v)", id, hasClock, stamps))
		}
	}
	if len(problems) > 0 {
		slices.Sort(problems)
		return fmt.Errorf("storage: read-model capture audit: %s", strings.Join(problems, "; "))
	}
	return nil
}
