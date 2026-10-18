package storage

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

// The read model's change capture (ARCHITECTURE, "Read-model change capture").
//
// The browser's views (internal/views) read, for an account, its projection
// P: every records row, all columns; which episodes are registered; each
// catalog item's publication date and insertion rank (rowid); each podcast's
// creating node; and that node's causal rank. Every change to P stamps the
// entity it changed (a record's entity and ID, ('account', id), ('podcast',
// id) or ('episode', id); the other tables' rows are an episode's or a
// podcast's) in view_changes with the account's next tick from view_clock. The capture
// triggers below do it, in the transaction that makes the change, so a
// version of the views built at tick t catches up by reading the entities
// stamped above t (ViewChangesSince). A statement that changes no P column
// stamps nothing.
//
// Bulk rewrites (an import's or a staged rebuild's publication, a rebuild's
// clearing, an account's deletion) suspend capture instead
// (SuspendViewCapture): they cost nothing per row and raise the account's
// generation, which no version built before survives. Within a log lineage
// (account, log_id, epoch), (generation, tick) never decreases.

// captureTrigger is one of the canonical capture triggers: migration
// 0025_read_model_changes.sql creates them with exactly this text, and a
// suspended transaction creates them again from it.
type captureTrigger struct{ name, sql string }

// captureTriggers are the 15 canonical capture triggers, three on each table
// of the projection.
var captureTriggers = buildCaptureTriggers()

// captureTriggerPrefix names the capture triggers.
const captureTriggerPrefix = "view_capture_"

// captureKey is the SQL of an entity a row stamps: the account, the entity
// and its ID, as expressions over NEW or OLD.
type captureKey struct{ account, entity, id string }

// bumpTick advances an account's tick (its first change starts it at 1).
func bumpTick(account string) string {
	return "\tINSERT INTO view_clock (account_id, tick) VALUES (" + account + ", 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;\n"
}

// bumpOldTick advances the old row's account's tick when an update moved the
// row to another account (the new account's is advanced by bumpTick).
const bumpOldTick = "\tINSERT INTO view_clock (account_id, tick) SELECT OLD.account_id, 1 WHERE OLD.account_id IS NOT NEW.account_id\n" +
	"\t\tON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;\n"

// stamp marks an entity changed at its account's current tick; cond, when
// set, limits it (an update's old key, when the update moved the row).
func stamp(k captureKey, cond string) string {
	where := "account_id = " + k.account
	if cond != "" {
		where += " AND (" + cond + ")"
	}
	return "\tINSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT " + k.account + ", " + k.entity + ", " + k.id + ", tick FROM view_clock\n" +
		"\t\tWHERE " + where + "\n" +
		"\t\tON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;\n"
}

// stampCreated marks the podcasts a causal node created (node_order's
// rank of a podcast's creating node is the podcast's rank); cond as stamp.
func stampCreated(row, cond string) string {
	where := "p.account_id = " + row + ".account_id AND p.creating_op = " + row + ".node_id AND c.account_id = " + row + ".account_id"
	if cond != "" {
		where += "\n\t\t\tAND (" + cond + ")"
	}
	return "\tINSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT p.account_id, 'podcast', p.podcast_id, c.tick\n" +
		"\t\tFROM podcasts p, view_clock c WHERE " + where + "\n" +
		"\t\tON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;\n"
}

// createsPodcast is true when a node_order row is a podcast's creating node.
func createsPodcast(row string) string {
	return "EXISTS (SELECT 1 FROM podcasts WHERE account_id = " + row + ".account_id AND creating_op = " + row + ".node_id)"
}

func trigger(name, event, table, when, body string) captureTrigger {
	sql := "CREATE TRIGGER " + name + " AFTER " + event + " ON " + table
	if when != "" {
		sql += "\n\tWHEN " + when
	}
	return captureTrigger{name: name, sql: sql + "\nBEGIN\n" + body + "END"}
}

func buildCaptureTriggers() []captureTrigger {
	var out []captureTrigger
	// records, catalog_items, episodes and podcasts stamp the entity of the
	// row; an update stamps only when a P column changed, and stamps the old
	// entity too when it moved the row to another one.
	for _, t := range []struct {
		table, short string
		entity       func(row string) captureKey
		changed      string // an update changed a P column
		moved        string // an update moved the row to another entity
	}{
		{"records", "records",
			func(row string) captureKey {
				return captureKey{row + ".account_id", row + ".entity", row + ".entity_id"}
			},
			"NEW.value IS NOT OLD.value OR NEW.provenance IS NOT OLD.provenance OR NEW.field_group IS NOT OLD.field_group\n" +
				"\t\tOR NEW.entity IS NOT OLD.entity OR NEW.entity_id IS NOT OLD.entity_id OR NEW.account_id IS NOT OLD.account_id",
			"OLD.account_id IS NOT NEW.account_id OR OLD.entity IS NOT NEW.entity OR OLD.entity_id IS NOT NEW.entity_id"},
		{"catalog_items", "catalog",
			func(row string) captureKey { return captureKey{row + ".account_id", "'episode'", row + ".episode_id"} },
			"NEW.pub_date IS NOT OLD.pub_date OR NEW.episode_id IS NOT OLD.episode_id OR NEW.rowid IS NOT OLD.rowid\n" +
				"\t\tOR NEW.account_id IS NOT OLD.account_id",
			"OLD.account_id IS NOT NEW.account_id OR OLD.episode_id IS NOT NEW.episode_id"},
		{"episodes", "episodes",
			func(row string) captureKey { return captureKey{row + ".account_id", "'episode'", row + ".episode_id"} },
			"NEW.episode_id IS NOT OLD.episode_id OR NEW.account_id IS NOT OLD.account_id",
			"OLD.account_id IS NOT NEW.account_id OR OLD.episode_id IS NOT NEW.episode_id"},
		{"podcasts", "podcasts",
			func(row string) captureKey { return captureKey{row + ".account_id", "'podcast'", row + ".podcast_id"} },
			"NEW.podcast_id IS NOT OLD.podcast_id OR NEW.creating_op IS NOT OLD.creating_op OR NEW.account_id IS NOT OLD.account_id",
			"OLD.account_id IS NOT NEW.account_id OR OLD.podcast_id IS NOT NEW.podcast_id"},
	} {
		name := captureTriggerPrefix + t.short + "_"
		out = append(out,
			trigger(name+"insert", "INSERT", t.table, "", bumpTick("NEW.account_id")+stamp(t.entity("NEW"), "")),
			trigger(name+"update", "UPDATE", t.table, t.changed,
				bumpTick("NEW.account_id")+stamp(t.entity("NEW"), "")+bumpOldTick+stamp(t.entity("OLD"), t.moved)),
			trigger(name+"delete", "DELETE", t.table, "", bumpTick("OLD.account_id")+stamp(t.entity("OLD"), "")))
	}
	// node_order stamps the podcasts its node created, when it created any.
	name := captureTriggerPrefix + "node_order_"
	out = append(out,
		trigger(name+"insert", "INSERT", "node_order", createsPodcast("NEW"), bumpTick("NEW.account_id")+stampCreated("NEW", "")),
		trigger(name+"update", "UPDATE", "node_order",
			"(NEW.rank IS NOT OLD.rank OR NEW.node_id IS NOT OLD.node_id OR NEW.account_id IS NOT OLD.account_id)\n"+
				"\t\tAND ("+createsPodcast("NEW")+"\n\t\t\tOR "+createsPodcast("OLD")+")",
			bumpTick("NEW.account_id")+stampCreated("NEW", "")+bumpOldTick+
				stampCreated("OLD", "OLD.account_id IS NOT NEW.account_id OR OLD.node_id IS NOT NEW.node_id")),
		trigger(name+"delete", "DELETE", "node_order", createsPodcast("OLD"), bumpTick("OLD.account_id")+stampCreated("OLD", "")))
	return out
}

// captureState is a write transaction's suspended capture.
type captureState struct {
	accounts []string // declared, in order
}

// SuspendViewCapture suspends the read model's change capture for the rest
// of this transaction, for a bulk primitive that rewrites the accounts' view
// inputs: its first call drops the capture triggers, so the bulk statements
// cost nothing per row; later calls add accounts. Before the transaction
// commits (Store.Write), the triggers are created again and each declared
// account that still exists has its tick and generation raised and its old
// change stamps cleared; a declared account deleted in the transaction has
// its clock and stamps removed. The declared accounts' listeners are woken
// after the commit. A transaction that rolls back rolls the dropped
// triggers back too.
//
// Only the declared accounts' inputs may change while capture is suspended.
func (w *WriteTx) SuspendViewCapture(accounts ...string) error {
	if !w.store.capture {
		return nil
	}
	if w.capture == nil {
		// Every capture trigger exists after a successful open: one that does
		// not is an error, never skipped.
		for _, t := range captureTriggers {
			if _, err := w.tx.ExecContext(w.ctx, `DROP TRIGGER main.`+t.name); err != nil {
				return fmt.Errorf("storage: suspending the read model's change capture: %w", err)
			}
		}
		w.capture = &captureState{}
		if err := callPoint(w.store.hooks.viewCapture, "dropped"); err != nil {
			return err
		}
	}
	for _, id := range accounts {
		if !slices.Contains(w.capture.accounts, id) {
			w.capture.accounts = append(w.capture.accounts, id)
		}
	}
	return nil
}

// finishViewCapture ends a suspension before the commit (Store.Write).
func (w *WriteTx) finishViewCapture() error {
	if w.capture == nil {
		return nil
	}
	ctx, tx := w.ctx, w.tx
	for _, t := range captureTriggers {
		if _, err := tx.ExecContext(ctx, t.sql); err != nil {
			return fmt.Errorf("storage: restoring the read model's change capture: %w", err)
		}
		if err := callPoint(w.store.hooks.viewCapture, "recreated "+t.name); err != nil {
			return err
		}
	}
	if err := callPoint(w.store.hooks.viewCapture, "recreated"); err != nil {
		return err
	}
	for _, id := range w.capture.accounts {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM accounts WHERE account_id = ?)`, id).Scan(&exists); err != nil {
			return err
		}
		if exists {
			if _, err := tx.ExecContext(ctx, `INSERT INTO view_clock (account_id, tick, generation) VALUES (?, 1, 1)
				ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1, generation = generation + 1`, id); err != nil {
				return err
			}
		}
		// Stamps before a generation are never read: no version of an
		// older generation is applied to. A deleted account keeps neither.
		if _, err := tx.ExecContext(ctx, `DELETE FROM view_changes WHERE account_id = ?`, id); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.ExecContext(ctx, `DELETE FROM view_clock WHERE account_id = ?`, id); err != nil {
				return err
			}
		}
	}
	return callPoint(w.store.hooks.viewCapture, "clocked")
}

func callPoint(h func(string) error, point string) error {
	if h == nil {
		return nil
	}
	return h(point)
}

// repairViewCapture restores, at open, capture triggers that are absent
// although the database's migrations create them, when nothing else
// differs from that schema: in one write transaction it creates them again
// and raises every account's tick and generation (a change made while they
// were absent stamped nothing), and logs a warning. Any other difference,
// a capture trigger with other SQL or one of the database's own included,
// is left for checkSchema to refuse.
func (s *Store) repairViewCapture(ctx context.Context, migrations []migration) error {
	_, diffs, err := compareSchema(ctx, s.writer, migrations)
	if err != nil || len(diffs) == 0 {
		return nil // checkSchema reports it
	}
	var missing []schemaObject
	for _, d := range diffs {
		if d.kind != schemaLacks || d.object.typ != "trigger" || !isCaptureTrigger(d.object.name) {
			return nil // checkSchema refuses the database
		}
		missing = append(missing, d.object)
	}
	slices.SortFunc(missing, func(a, b schemaObject) int { return strings.Compare(a.name, b.name) })
	var accounts int64
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, o := range missing {
			if _, err := tx.tx.ExecContext(ctx, o.sql); err != nil {
				return fmt.Errorf("storage: restoring the read model's change capture: %w", err)
			}
		}
		res, err := tx.tx.ExecContext(ctx, `INSERT INTO view_clock (account_id, tick, generation) SELECT account_id, 1, 1 FROM accounts WHERE true
			ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1, generation = generation + 1`)
		if err != nil {
			return err
		}
		accounts, _ = res.RowsAffected()
		if tx.audit != nil {
			tx.audit.all = true
		}
		_, err = tx.tx.ExecContext(ctx, `DELETE FROM view_changes`)
		return err
	}); err != nil {
		return err
	}
	names := make([]string, len(missing))
	for i, o := range missing {
		names[i] = o.name
	}
	slog.Warn("storage: the database lacked read-model capture triggers; they were restored and every account's read views will be built again",
		"triggers", strings.Join(names, ","), "accounts", accounts)
	return nil
}

func isCaptureTrigger(name string) bool {
	return slices.ContainsFunc(captureTriggers, func(t captureTrigger) bool { return t.name == name })
}

// ---- delta readers

// ViewClock is an account's capture clock: every change to its view inputs
// raises Tick, and every bulk rewrite raises Generation too. An account
// whose inputs never changed since the clock began is at (0, 0).
type ViewClock struct {
	Tick, Generation int64
}

// ViewClock reads the account's capture clock in this snapshot.
func (a *AccountReader) ViewClock() (ViewClock, error) {
	var c ViewClock
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT COALESCE(MAX(tick), 0), COALESCE(MAX(generation), 0) FROM view_clock WHERE account_id = ?`, a.id).
		Scan(&c.Tick, &c.Generation)
	return c, err
}

// A ViewChange is an entity whose view inputs changed: Entity is a record's
// entity (account, podcast, episode), Tick the tick of its latest change.
type ViewChange struct {
	Entity, ID string
	Tick       int64
}

// ViewCursor is where a ViewChangesSince walk stands: after the change
// (Tick, Entity, ID), in that order.
type ViewCursor struct {
	Tick       int64
	Entity, ID string
}

// ViewChangesPage is how many changes ViewChangesSince returns at most.
const ViewChangesPage = 1000

// viewChangesSQL reads one page of an account's changes after a base tick,
// from a cursor, through the covering index view_changes_by_tick; it asks
// for one row more than a page, to know whether another page follows. The
// cursor is the index range's start, so a page reads its own rows only
// (the unary + keeps the base out of the range: it only skips the stamps
// at the base tick itself, on the first page).
const viewChangesSQL = `SELECT entity, entity_id, tick FROM view_changes
	WHERE account_id = ?1 AND (tick, entity, entity_id) > (?3, ?4, ?5) AND +tick > ?2
	ORDER BY tick, entity, entity_id LIMIT ?6`

// ViewChangesSince returns, in (tick, entity, ID) order, at most
// ViewChangesPage of the entities stamped above base, after the cursor (the
// first call passes ViewCursor{Tick: base}), the cursor after them and
// whether more follow. A walk reads every page in one read snapshot.
func (a *AccountReader) ViewChangesSince(base int64, after ViewCursor) (changes []ViewChange, next ViewCursor, more bool, err error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, viewChangesSQL, a.id, base, after.Tick, after.Entity, after.ID, ViewChangesPage+1)
	if err != nil {
		return nil, after, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var c ViewChange
		if err := rows.Scan(&c.Entity, &c.ID, &c.Tick); err != nil {
			return nil, after, false, err
		}
		changes = append(changes, c)
	}
	if err := rows.Err(); err != nil {
		return nil, after, false, err
	}
	if len(changes) > ViewChangesPage {
		changes, more = changes[:ViewChangesPage], true
	}
	next = after
	if n := len(changes); n > 0 {
		last := changes[n-1]
		next = ViewCursor{Tick: last.Tick, Entity: last.Entity, ID: last.ID}
	}
	return changes, next, more, nil
}

// ViewBatch is how many IDs one statement of the entity readers looks up.
const ViewBatch = 500

// inBatches runs read on consecutive batches of at most ViewBatch of the
// distinct IDs, in order, with the placeholders of their IN list.
func inBatches(ids []string, read func(batch []string, placeholders string) error) error {
	ids = slices.Compact(slices.Sorted(slices.Values(ids)))
	for start := 0; start < len(ids); start += ViewBatch {
		batch := ids[start:min(start+ViewBatch, len(ids))]
		if err := read(batch, "?"+strings.Repeat(",?", len(batch)-1)); err != nil {
			return err
		}
	}
	return nil
}

func batchArgs(first []any, batch []string) []any {
	args := slices.Clone(first)
	for _, id := range batch {
		args = append(args, id)
	}
	return args
}

// viewEntityRecordsSQL reads some entities' records as ViewRecords does
// (viewRecordValue); %s is the IN list's placeholders.
const viewEntityRecordsSQL = `SELECT entity, entity_id, field_group, ` + viewRecordValue + `, provenance FROM records
	WHERE account_id = ? AND entity = ? AND entity_id IN (%s)`

// ViewEntityRecords returns the live records of some entities of one kind,
// as ViewRecords returns them, ordered by (entity, id, field group).
func (a *AccountReader) ViewEntityRecords(entity string, ids []string) ([]RecordRow, error) {
	var out []RecordRow
	err := inBatches(ids, func(batch []string, in string) error {
		rows, err := a.r.tx.QueryContext(a.r.ctx, fmt.Sprintf(viewEntityRecordsSQL, in), batchArgs([]any{a.id, entity}, batch)...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r RecordRow
			if err := rows.Scan(&r.Entity, &r.EntityID, &r.FieldGroup, &r.Value, &r.Provenance); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	slices.SortFunc(out, func(x, y RecordRow) int {
		return cmpJoin(strings.Compare(x.EntityID, y.EntityID), strings.Compare(x.FieldGroup, y.FieldGroup))
	})
	return out, err
}

func cmpJoin(a, b int) int {
	if a != 0 {
		return a
	}
	return b
}

// registeredEpisodesSQL: %s is the IN list's placeholders.
const registeredEpisodesSQL = `SELECT episode_id FROM episodes WHERE account_id = ? AND episode_id IN (%s)`

// RegisteredEpisodes returns which of the episodes are registered, sorted.
func (a *AccountReader) RegisteredEpisodes(ids []string) ([]string, error) {
	var out []string
	err := inBatches(ids, func(batch []string, in string) error {
		got, err := queryStrings(a.r.ctx, a.r.tx, fmt.Sprintf(registeredEpisodesSQL, in), batchArgs([]any{a.id}, batch)...)
		out = append(out, got...)
		return err
	})
	slices.Sort(out)
	return out, err
}

// catalogRowsSQL: %s is the IN list's placeholders.
const catalogRowsSQL = `SELECT episode_id, pub_date, rowid FROM catalog_items WHERE account_id = ? AND episode_id IN (%s)`

// CatalogRowsFor returns the catalog items of the episodes the catalog
// knows, as CatalogRows does, ordered by episode.
func (a *AccountReader) CatalogRowsFor(ids []string) ([]CatalogRow, error) {
	var out []CatalogRow
	err := inBatches(ids, func(batch []string, in string) error {
		rows, err := a.r.tx.QueryContext(a.r.ctx, fmt.Sprintf(catalogRowsSQL, in), batchArgs([]any{a.id}, batch)...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c CatalogRow
			if err := rows.Scan(&c.EpisodeID, &c.PubDate, &c.Order); err != nil {
				return err
			}
			out = append(out, c)
		}
		return rows.Err()
	})
	slices.SortFunc(out, func(x, y CatalogRow) int { return strings.Compare(x.EpisodeID, y.EpisodeID) })
	return out, err
}

// podcastRanksSQL: %s is the IN list's placeholders.
const podcastRanksSQL = `SELECT p.podcast_id, COALESCE(o.rank, -1) FROM podcasts p
	LEFT JOIN node_order o ON o.account_id = p.account_id AND o.node_id = p.creating_op
	WHERE p.account_id = ? AND p.podcast_id IN (%s)`

// PodcastRanksFor returns each of the podcasts' creation ranks as
// PodcastRanks does (-1 for a podcast whose creating node has no rank), and
// 0 for an ID no podcast has, as a lookup in PodcastRanks' map gives.
func (a *AccountReader) PodcastRanksFor(ids []string) (map[string]int64, error) {
	out := make(map[string]int64, len(ids))
	for _, id := range ids {
		out[id] = 0
	}
	err := inBatches(ids, func(batch []string, in string) error {
		rows, err := a.r.tx.QueryContext(a.r.ctx, fmt.Sprintf(podcastRanksSQL, in), batchArgs([]any{a.id}, batch)...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var rank int64
			if err := rows.Scan(&id, &rank); err != nil {
				return err
			}
			out[id] = rank
		}
		return rows.Err()
	})
	return out, err
}

// ViewCapture reports whether the store captures changes to the read
// views' inputs (its schema has the capture: every server's does).
func (s *Store) ViewCapture() bool { return s.capture }
