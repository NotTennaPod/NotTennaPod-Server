package storage

import (
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
)

// KeyWrite is a layout's key write for one (episode, generation) scope, at
// the layout's rank (PROTOCOL §7.6).
type KeyWrite struct {
	Scope, Key, PositionID, Node string
	Epoch, Seq, Index            int64
}

// WriteKey records a layout's key write for its scope if it ranks at or
// above the scope's current one.
func (a *AccountTx) WriteKey(k KeyWrite) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO queue_keys (account_id, scope, queue_key, position_id, node_id, epoch, seq, idx)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (account_id, scope) DO UPDATE SET queue_key = excluded.queue_key,
		position_id = excluded.position_id, node_id = excluded.node_id, epoch = excluded.epoch, seq = excluded.seq, idx = excluded.idx
		WHERE (excluded.epoch, excluded.seq, excluded.idx, excluded.node_id) >= (queue_keys.epoch, queue_keys.seq, queue_keys.idx, queue_keys.node_id)`,
		a.id, k.Scope, k.Key, k.PositionID, k.Node, k.Epoch, k.Seq, k.Index)
	return err
}

// KeyWrites returns every scope's latest layout key write.
func (a *AccountTx) KeyWrites() ([]KeyWrite, error) {
	return a.keyWrites(`SELECT scope, queue_key, position_id, node_id, epoch, seq, idx FROM queue_keys WHERE account_id = ?`, a.id)
}

// KeyWritesSince returns the layouts' latest key writes ranked after the
// rank (epoch, seq, idx, node), in rank order: one range seek
// (queue_keys_by_rank).
func (a *AccountTx) KeyWritesSince(epoch, seq, idx int64, node string) ([]KeyWrite, error) {
	return a.keyWrites(keyWritesSinceQuery, a.id, epoch, seq, idx, node)
}

const keyWritesSinceQuery = `SELECT scope, queue_key, position_id, node_id, epoch, seq, idx FROM queue_keys INDEXED BY queue_keys_by_rank
	WHERE account_id = ? AND (epoch, seq, idx, node_id) > (?, ?, ?, ?) ORDER BY epoch, seq, idx, node_id`

// KeyWriteOf returns a scope's latest layout key write (false for none).
func (a *AccountTx) KeyWriteOf(scope string) (KeyWrite, bool, error) {
	out, err := a.keyWrites(`SELECT scope, queue_key, position_id, node_id, epoch, seq, idx FROM queue_keys WHERE account_id = ? AND scope = ?`,
		a.id, scope)
	if err != nil || len(out) == 0 {
		return KeyWrite{}, false, err
	}
	return out[0], true, nil
}

func (a *AccountTx) keyWrites(query string, args ...any) ([]KeyWrite, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyWrite
	for rows.Next() {
		var k KeyWrite
		if err := rows.Scan(&k.Scope, &k.Key, &k.PositionID, &k.Node, &k.Epoch, &k.Seq, &k.Index); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	keyWriteRows.Add(int64(len(out)))
	return out, rows.Err()
}

// keyWriteRows counts the layout key write rows read since the process
// started (tests bound a queue command's reads).
var keyWriteRows atomic.Int64

// KeyWriteRowsRead is the number of layout key write rows read so far.
func KeyWriteRowsRead() int64 { return keyWriteRows.Load() }

// PriorRecord returns a record as it was published before this transaction's
// rebuild cleared it (the rebuild's copy), else as it is now (ErrNotFound
// for none): an unchanged record keeps its provenance (PROTOCOL §7.6).
func (a *AccountTx) PriorRecord(entity, id, group string) (RecordRow, error) {
	r, err := a.Record(entity, id, group)
	if err == nil || !errors.Is(err, ErrNotFound) {
		return r, err
	}
	var n int
	if err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM sqlite_temp_master WHERE type = 'table' AND name = 'rebuild_records'`).Scan(&n); err != nil {
		return r, err
	}
	if n == 0 {
		return r, ErrNotFound
	}
	r = RecordRow{Entity: entity, EntityID: id, FieldGroup: group}
	err = a.w.tx.QueryRowContext(a.w.ctx, `SELECT value, provenance FROM temp.rebuild_records WHERE account_id = ? AND entity = ? AND entity_id = ?
		AND field_group = ?`, a.id, entity, id, group).Scan(&r.Value, &r.Provenance)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// CausalFrontier returns the account's causal frontier (sorted).
func (a *AccountTx) CausalFrontier() ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT node_id FROM causal_frontier WHERE account_id = ? ORDER BY node_id`, a.id)
}

// LiveRecordIDs returns the IDs of an entity's live records of a field group.
func (a *AccountTx) LiveRecordIDs(entity, group string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT entity_id FROM records WHERE account_id = ? AND entity = ? AND field_group = ? ORDER BY entity_id`,
		a.id, entity, group)
}

// QueueFrontier returns the queue frontier (sorted).
func (a *AccountTx) QueueFrontier() ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT node_id FROM queue_frontier WHERE account_id = ? ORDER BY node_id`, a.id)
}

// ---- the queue replay's inputs (PROTOCOL §7.6)

// QueueStepRow is a replay step: its node, the decision resolving it ("" for
// none), the node's rank, and what the replay reads of it: the archived fact
// (type, outcome, content and receipt) or the decision it is, and the
// resolving decision.
type QueueStepRow struct {
	Node, Decision string
	Writes         string // a fact's key writers: the JSON array of its members' indexes
	Refused        string // a move's refused members: the JSON array of their indexes
	Closed         string // its members admitted as of a closed generation (same form)
	Rank           int64
	FactType       string // "" when the node is not an archived fact (a decision, or a fact being admitted)
	FactOutcome    string
	Fact, Receipt  []byte
	NodeDecision   []byte // the decision the node is (nil for a fact)
	Resolver       []byte // the resolving decision (nil for none)
}

// PutQueueStep records a node as a replay step; a decision, once recorded,
// stays when the step is recorded again without one.
func (a *AccountTx) PutQueueStep(node, decision string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO queue_steps (account_id, node_id, decision_id) VALUES (?, ?, ?)
		ON CONFLICT (account_id, node_id) DO UPDATE SET decision_id = CASE WHEN excluded.decision_id != '' THEN excluded.decision_id ELSE decision_id END`,
		a.id, node, decision)
	return err
}

// PutQueueStepWrites records a fact as a replay step with its key writers
// and its refused members (replacing those an earlier derivation recorded).
func (a *AccountTx) PutQueueStepWrites(node, writes, refused, closed string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO queue_steps (account_id, node_id, writes, refused, closed) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (account_id, node_id) DO UPDATE SET writes = excluded.writes, refused = excluded.refused, closed = excluded.closed`,
		a.id, node, writes, refused, closed)
	return err
}

// queueRange is the FROM clause of a replay input read ranked after rank: a
// read from the start scans the input table (its rows are a fraction of the
// account's nodes); a read from a late snapshot scans the few nodes ranked
// after it.
func queueRange(table, alias, key string, rank int64) string {
	if rank < 0 {
		return table + ` ` + alias + ` CROSS JOIN node_order o ON o.account_id = ` + alias + `.account_id AND o.node_id = ` + alias + `.` + key
	}
	// Each node after the snapshot is looked up by its ID (queue_events'
	// primary key leads with the scope: its node index, not a scan of the
	// account's events per node).
	index := ``
	if table == "queue_events" {
		index = ` INDEXED BY queue_events_by_node`
	}
	return `node_order o INDEXED BY node_order_rank CROSS JOIN ` + table + ` ` + alias + index + ` ON ` + alias + `.account_id = o.account_id AND ` +
		alias + `.` + key + ` = o.node_id`
}

// QueueStepsAfter returns the replay steps ranked after rank, in rank order,
// with what the replay reads of each.
func (a *AccountTx) QueueStepsAfter(rank int64) ([]QueueStepRow, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT s.node_id, s.decision_id, s.writes, s.refused, s.closed, o.rank, COALESCE(f.type, ''), COALESCE(f.outcome, ''), f.fact, f.receipt,
		d.decision, r.decision FROM `+queueRange("queue_steps", "s", "node_id", rank)+`
		LEFT JOIN facts f ON f.account_id = s.account_id AND f.op_id = s.node_id
		LEFT JOIN decisions d ON d.account_id = s.account_id AND d.decision_id = s.node_id
		LEFT JOIN decisions r ON r.account_id = s.account_id AND r.decision_id = s.decision_id
		WHERE s.account_id = ?1 AND o.account_id = ?1 AND o.rank > ?2 ORDER BY o.rank`, a.id, rank)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueStepRow
	for rows.Next() {
		var r QueueStepRow
		if err := rows.Scan(&r.Node, &r.Decision, &r.Writes, &r.Refused, &r.Closed, &r.Rank, &r.FactType, &r.FactOutcome, &r.Fact, &r.Receipt, &r.NodeDecision, &r.Resolver); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueueEventRow is a membership effect that took effect at its node in an
// episode scope (the node's rank when read in order).
type QueueEventRow struct {
	Scope, Node string
	In          bool
	Rank        int64
}

// ScopeQueueEvents returns an episode scope's recorded membership events.
func (a *AccountTx) ScopeQueueEvents(scope string) ([]QueueEventRow, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT node_id, in_queue FROM queue_events WHERE account_id = ? AND scope = ? ORDER BY node_id`, a.id, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueEventRow
	for rows.Next() {
		r := QueueEventRow{Scope: scope}
		var in int
		if err := rows.Scan(&r.Node, &in); err != nil {
			return nil, err
		}
		r.In = in == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetScopeQueueEvents replaces an episode scope's membership events.
func (a *AccountTx) SetScopeQueueEvents(scope string, events []QueueEventRow) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM queue_events WHERE account_id = ? AND scope = ?`, a.id, scope); err != nil {
		return err
	}
	for _, e := range events {
		in := 0
		if e.In {
			in = 1
		}
		if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO queue_events (account_id, scope, node_id, in_queue) VALUES (?, ?, ?, ?)`,
			a.id, scope, e.Node, in); err != nil {
			return err
		}
	}
	return nil
}

// QueueEventsAfter returns the membership events of nodes ranked after rank,
// in rank order (then by scope).
func (a *AccountTx) QueueEventsAfter(rank int64) ([]QueueEventRow, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT e.scope, e.node_id, e.in_queue, o.rank FROM `+queueRange("queue_events", "e", "node_id", rank)+`
		WHERE e.account_id = ?1 AND o.account_id = ?1 AND o.rank > ?2 ORDER BY o.rank, e.scope`, a.id, rank)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueEventRow
	for rows.Next() {
		var r QueueEventRow
		var in int
		if err := rows.Scan(&r.Scope, &r.Node, &in, &r.Rank); err != nil {
			return nil, err
		}
		r.In = in == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

// Memo is the store's in-memory cache of state derived from the database
// (the queue replay's snapshots): its users key it by account and check
// every entry against the database before trusting it.
func (a *AccountTx) Memo() *sync.Map { return &a.w.store.memo }

// IdentityMappingCount is the number of identity-mapping decisions held
// (episode and podcast merges, occurrence mappings, GUID changes): a layout's
// mapping version.
func (a *AccountTx) IdentityMappingCount() (int64, error) {
	var n int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM decisions WHERE account_id = ? AND decision_type IN
		('episode.merge', 'podcast.merge', 'episode.occurrence_mapping', 'episode.guid_change')`, a.id).Scan(&n)
	return n, err
}
