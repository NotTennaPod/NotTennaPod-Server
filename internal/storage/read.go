package storage

import (
	"database/sql"
	"errors"
	"sort"
)

// Reads for the browser's views (PROTOCOL §12): one read snapshot gives a
// view its head, its records and its causal frontier.

// ViewRecords returns the account's live records for list views. Episode
// metadata comes without its description and chapters, which only the episode
// view shows.
func (a *AccountReader) ViewRecords() ([]RecordRow, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT entity, entity_id, field_group,
		CASE WHEN entity = 'episode' AND field_group = 'metadata' THEN json_remove(value, '$.description', '$.chapters') ELSE value END,
		provenance FROM records WHERE account_id = ? ORDER BY entity, entity_id, field_group`, a.id)
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

// Record returns one live record in a read snapshot (ErrNotFound if none).
func (a *AccountReader) Record(entity, id, group string) (RecordRow, error) {
	r := RecordRow{Entity: entity, EntityID: id, FieldGroup: group}
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT value, provenance FROM records
		WHERE account_id = ? AND entity = ? AND entity_id = ? AND field_group = ?`, a.id, entity, id, group).Scan(&r.Value, &r.Provenance)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// EpisodeRow is a registered episode with its first occurrence evidence.
type EpisodeRow struct {
	EpisodeID, PodcastID, Occurrence string
}

// EpisodeRows returns every registered episode.
func (a *AccountReader) EpisodeRows() ([]EpisodeRow, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT episode_id, podcast_id, occurrence FROM episodes WHERE account_id = ? ORDER BY episode_id`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EpisodeRow
	for rows.Next() {
		var e EpisodeRow
		if err := rows.Scan(&e.EpisodeID, &e.PodcastID, &e.Occurrence); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// CatalogRow is a catalog item's effective publication date and its place in
// the order the account's catalog learned of its items (insertion order: a
// fetch's items in feed order, as Android numbers the items it saves).
type CatalogRow struct {
	EpisodeID string
	PubDate   int64
	Order     int64
}

// CatalogRows returns every catalog item.
func (a *AccountReader) CatalogRows() ([]CatalogRow, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT episode_id, pub_date, rowid FROM catalog_items WHERE account_id = ?`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CatalogRow
	for rows.Next() {
		var c CatalogRow
		if err := rows.Scan(&c.EpisodeID, &c.PubDate, &c.Order); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// PodcastRanks returns each podcast's creation rank (the rank of its creating
// node; -1 when it has none), the order the account learned of its podcasts.
func (a *AccountReader) PodcastRanks() (map[string]int64, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT p.podcast_id, COALESCE(o.rank, -1) FROM podcasts p
		LEFT JOIN node_order o ON o.account_id = p.account_id AND o.node_id = p.creating_op
		WHERE p.account_id = ?`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var id string
		var rank int64
		if err := rows.Scan(&id, &rank); err != nil {
			return nil, err
		}
		out[id] = rank
	}
	return out, rows.Err()
}

// FrontierAt returns the account's causal frontier at a head: the causal
// nodes (admitted facts and decisions) published at or before (epoch, seq)
// that no node published by then depends on, sorted. A fact naming them as
// dependencies observes exactly that head's causal past (PROTOCOL §12).
// Publication, not the receipt, decides (PROTOCOL §6.11, §12): an entry joined
// by recovery keeps its original receipt but becomes visible in the commit
// that joins it, so a past head's frontier never changes.
func (a *AccountReader) FrontierAt(epoch, seq int64) ([]string, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT f.op_id FROM facts f
		JOIN causal_nodes c ON c.account_id = f.account_id AND c.node_id = f.op_id
		WHERE f.account_id = ?1 AND (f.node_pub_epoch < ?2 OR (f.node_pub_epoch = ?2 AND f.node_pub_seq <= ?3))
		UNION ALL
		SELECT d.decision_id FROM decisions d WHERE d.account_id = ?1 AND (d.pub_epoch < ?2 OR (d.pub_epoch = ?2 AND d.pub_seq <= ?3))`, a.id, epoch, seq)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		seen[id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	edges, err := a.r.tx.QueryContext(a.r.ctx, `SELECT node_id, dependency_id FROM causal_edges WHERE account_id = ?`, a.id)
	if err != nil {
		return nil, err
	}
	defer edges.Close()
	// A node published later (recovery made it a node after the head) is
	// not part of the head, but the coverage of a node that was runs through
	// it: before it was a node, its dependents' edges went to its
	// dependencies.
	covered := map[string]bool{}
	later := map[string][]string{} // a later-published node's dependencies
	var through []string
	for edges.Next() {
		var n, d string
		if err := edges.Scan(&n, &d); err != nil {
			return nil, err
		}
		if seen[n] {
			if !covered[d] {
				covered[d] = true
				through = append(through, d)
			}
		} else {
			later[n] = append(later[n], d)
		}
	}
	if err := edges.Err(); err != nil {
		return nil, err
	}
	for len(through) > 0 {
		n := through[len(through)-1]
		through = through[:len(through)-1]
		if seen[n] {
			continue // a node of the head covers on its own
		}
		for _, d := range later[n] {
			if !covered[d] {
				covered[d] = true
				through = append(through, d)
			}
		}
	}
	out := []string{}
	for id := range seen {
		if !covered[id] {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out, nil
}

// PodcastOwning returns the podcast owning a normalized feed URL, or "".
func (a *AccountReader) PodcastOwning(normalized string) (string, error) {
	var id string
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT podcast_id FROM podcast_aliases WHERE account_id = ? AND normalized_url = ?`,
		a.id, normalized).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}
