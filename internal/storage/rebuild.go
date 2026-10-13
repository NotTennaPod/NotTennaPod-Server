package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
)

// Account rebuilds (PROTOCOL §6.11): an account's derived state is a function
// of its archive in receipt order, so it can be cleared and derived again by
// replaying the archive. These are the storage steps; protocol.Service.Rebuild
// drives them inside one write transaction.

// rebuildCleared are the account tables a rebuild clears and its replay fills
// again, children before parents.
var rebuildCleared = []string{
	"contributions", "causal_edges", "node_order", "causal_frontier", "listening_facts", "queue_frontier", "queue_steps", "queue_events",
	"queue_keys",
	"session_facts", "session_continuations", "listening_sessions",
	"author_positions", "fact_authors", "author_lineage",
	"identity_occurrences", "identity_aliases", "identity_disputes", "identity_releases", "identity_release_additions", "episode_merges",
	"podcast_merges", "catalog_items", "generation_population", "generation_joins", "episodes", "generations", "podcast_aliases", "podcasts",
	"causal_nodes",
}

// rebuildSaved are the account tables a rebuild keeps although clearing
// removes their rows (foreign keys cascade from podcasts and causal nodes, and
// records are derived): records are copied aside and compared with the
// rebuilt ones; decisions are the archive, copied aside and restored verbatim
// as the replay makes each a causal node again (RestoreDecision); stored feed
// responses and refresh schedules are copied aside and restored for the
// podcasts the replay creates again.
//
// The rebuildCleared tables, causal_nodes included, are a function of the
// archive in receipt order. What is not (a recovered entry's publication
// position, PROTOCOL §6.11) belongs on the archive's rows
// (facts, decisions), which a rebuild keeps or restores whole.
var rebuildSaved = []string{"records", "decisions", "feed_responses", "refresh_schedule"}

// rebuildKept are the account tables a rebuild never touches: the account,
// its log, logins, installations and tokens, transport state, the fact
// archive and its superseded receipts, secrets, imports and recoveries.
var rebuildKept = []string{"accounts", "account_logs", "log_commits", "account_logins", "installations", "auth_tokens",
	"transport_slots", "push_parts", "recovery_frames", "facts", "superseded_receipts", "secrets", "imports", "recoveries"}

// FactPosition is an archived fact's receipt position (PROTOCOL §6.10): (epoch, seq,
// index, op_id) is admission order. Index is -1 for a receipt without one.
type FactPosition struct {
	OpID              string
	Epoch, Seq, Index int64
}

// FactPositions returns the receipt positions of every archived fact of the
// account, unordered.
func (a *AccountTx) FactPositions() ([]FactPosition, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT op_id, epoch, seq, receipt_index FROM facts WHERE account_id = ?`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FactPosition
	for rows.Next() {
		var p FactPosition
		if err := rows.Scan(&p.OpID, &p.Epoch, &p.Seq, &p.Index); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Decisions returns every archived decision of the account, unordered.
func (a *AccountTx) Decisions() ([]DecisionRow, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT decision_id, decision_type, subject, epoch, seq, normalized_time, decision
		FROM decisions WHERE account_id = ?`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DecisionRow
	for rows.Next() {
		var d DecisionRow
		if err := rows.Scan(&d.DecisionID, &d.DecisionType, &d.Subject, &d.Epoch, &d.Seq, &d.NormalizedTime, &d.Decision); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// AcceptingInstallations maps every fact admitted through a transport slot to
// the installation whose slot archived it (result accepted).
func (a *AccountTx) AcceptingInstallations() (map[string]string, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT op_id, install_id FROM transport_slots WHERE account_id = ? AND result = 'accepted'`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var op, install string
		if err := rows.Scan(&op, &install); err != nil {
			return nil, err
		}
		out[op] = install
	}
	return out, rows.Err()
}

// ClearDerived copies the account's records, decisions, stored feed
// responses and refresh schedules aside (temporary tables of this
// transaction) and clears its derived state: every rebuildCleared table and
// those copies' rows. The fact archive and everything in rebuildKept stay.
func (a *AccountTx) ClearDerived() error {
	if err := a.dropRebuildCopies(); err != nil {
		return err
	}
	for _, table := range rebuildSaved {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `CREATE TEMP TABLE rebuild_`+table+` AS SELECT * FROM main.`+table+` WHERE 0`); err != nil {
			return err
		}
		if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO temp.rebuild_`+table+` SELECT * FROM main.`+table+` WHERE account_id = ?`, a.id); err != nil {
			return err
		}
	}
	for _, table := range []string{"feed_responses", "refresh_schedule", "records", "decisions"} {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM main.`+table+` WHERE account_id = ?`, a.id); err != nil {
			return err
		}
	}
	for _, table := range rebuildCleared {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM main.`+table+` WHERE account_id = ?`, a.id); err != nil {
			return fmt.Errorf("storage: clearing %s: %w", table, err)
		}
	}
	return nil
}

// RestoreDecision restores an archived decision's row as ClearDerived copied
// it, every column included (its causal node must exist again).
func (a *AccountTx) RestoreDecision(decisionID string) error {
	res, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO main.decisions SELECT * FROM temp.rebuild_decisions WHERE decision_id = ?`, decisionID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("storage: decision %s is not in the rebuild's copy of the archive (%d rows): %v", decisionID, n, err)
	}
	return nil
}

// RestoreSaved restores the stored feed responses and refresh schedules of
// the podcasts the rebuild created again, and reports how many rows lost
// their podcast (and were not restored).
func (a *AccountTx) RestoreSaved() (int, error) {
	lost := 0
	for _, table := range []string{"feed_responses", "refresh_schedule"} {
		var saved int
		if err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM temp.rebuild_`+table).Scan(&saved); err != nil {
			return 0, err
		}
		res, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO main.`+table+` SELECT * FROM temp.rebuild_`+table+` t
			WHERE EXISTS (SELECT 1 FROM main.podcasts p WHERE p.account_id = t.account_id AND p.podcast_id = t.podcast_id)`)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		lost += saved - int(n)
	}
	return lost, nil
}

// RecordChanges compares the rebuilt records with the copy taken by
// ClearDerived: changed holds the records that are new or differ in value or
// provenance, removed the copied records the rebuild no longer has; both are
// ordered by (entity, id, field group).
func (a *AccountTx) RecordChanges() (changed, removed []RecordRow, err error) {
	scan := func(q string) ([]RecordRow, error) {
		rows, err := a.w.tx.QueryContext(a.w.ctx, q, a.id)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []RecordRow
		for rows.Next() {
			var r RecordRow
			if err := rows.Scan(&r.Entity, &r.EntityID, &r.FieldGroup, &r.Value, &r.Provenance); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, rows.Err()
	}
	changed, err = scan(`SELECT r.entity, r.entity_id, r.field_group, r.value, r.provenance FROM main.records r
		LEFT JOIN temp.rebuild_records o ON o.entity = r.entity AND o.entity_id = r.entity_id AND o.field_group = r.field_group
		WHERE r.account_id = ? AND (o.entity IS NULL OR o.value IS NOT r.value OR o.provenance IS NOT r.provenance)
		ORDER BY r.entity, r.entity_id, r.field_group`)
	if err != nil {
		return nil, nil, err
	}
	removed, err = scan(`SELECT o.entity, o.entity_id, o.field_group, o.value, o.provenance FROM temp.rebuild_records o
		WHERE NOT EXISTS (SELECT 1 FROM main.records r WHERE r.account_id = ? AND r.entity = o.entity AND r.entity_id = o.entity_id
			AND r.field_group = o.field_group)
		ORDER BY o.entity, o.entity_id, o.field_group`)
	return changed, removed, err
}

// DropRebuildCopies removes ClearDerived's temporary copies (a rolled-back
// transaction removes them too).
func (a *AccountTx) DropRebuildCopies() error { return a.dropRebuildCopies() }

func (a *AccountTx) dropRebuildCopies() error {
	for _, table := range rebuildSaved {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `DROP TABLE IF EXISTS temp.rebuild_`+table); err != nil {
			return err
		}
	}
	return nil
}

// DerivedState dumps every table a rebuild clears or saves, for one account:
// each table's rows as sorted strings (BLOBs in hex). Tests compare the
// state before and after a rebuild with it.
func (s *Store) DerivedState(ctx context.Context, accountID string) (map[string][]string, error) {
	out := map[string][]string{}
	err := s.Read(ctx, func(r *ReadTx) error {
		for _, table := range append(slices.Clone(rebuildCleared), rebuildSaved...) {
			rows, err := r.tx.QueryContext(ctx, `SELECT * FROM `+table+` WHERE account_id = ?`, accountID)
			if err != nil {
				return err
			}
			cols, err := rows.Columns()
			if err != nil {
				rows.Close()
				return err
			}
			list := []string{}
			for rows.Next() {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					rows.Close()
					return err
				}
				parts := make([]string, len(cols))
				for i, v := range vals {
					switch x := v.(type) {
					case []byte:
						parts[i] = cols[i] + "=x" + hex.EncodeToString(x)
					case nil:
						parts[i] = cols[i] + "=NULL"
					default:
						parts[i] = fmt.Sprintf("%s=%v", cols[i], x)
					}
				}
				list = append(list, strings.Join(parts, " "))
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			slices.Sort(list)
			out[table] = list
		}
		return nil
	})
	return out, err
}
