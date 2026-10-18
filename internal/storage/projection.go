package storage

import (
	"database/sql"
	"errors"
	"sync/atomic"
)

// rankGap spaces new ranks; renumbering restores it when a gap is exhausted.
const rankGap = 1 << 20

// rankPlacementRows counts the nodes read finding new nodes' places (tests
// bound it).
var rankPlacementRows atomic.Int64

// insertRank gives a new sink node its place in the account's causal order
// (PROTOCOL §7.2): immediately before the first node after its last
// dependency whose (normalized_time, node_id) is greater, else last. Its
// rank is the midpoint of the gap there, and a gap used up renumbers the
// account: the ranks the earlier servers gave, so a database one of them
// wrote rebuilds to the same ranks (the rebuild invariant).
func (a *AccountTx) insertRank(nodeID string, t int64, deps []string) error {
	last, err := a.lastRank(deps) // rank of the last dependency (0 if none)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 2; attempt++ {
		prev, next, found, err := a.placeAfter(last, t, nodeID)
		if err != nil {
			return err
		}
		var rank int64
		switch {
		case !found:
			rank = prev + rankGap
		case next-prev >= 2:
			rank = prev + (next-prev)/2
		default:
			if err := a.renumber(); err != nil {
				return err
			}
			if last, err = a.lastRank(deps); err != nil {
				return err
			}
			continue
		}
		_, err = a.w.tx.ExecContext(a.w.ctx, `INSERT INTO node_order (account_id, node_id, rank) VALUES (?, ?, ?)`, a.id, nodeID, rank)
		return err
	}
	return errors.New("storage: cannot place node in the causal order")
}

// placeAfter returns the rank of the first node ranked after last whose
// priority (normalized time, ID bytewise) is greater than (t, id) and the
// rank right before it (found), or the last rank (not found). Two scans take
// turns, a page each: the nodes after last in rank order, which ends at that
// node, and the nodes of greater priority, which ends when they are all
// read. So it reads about twice the smaller of the nodes it is placed after
// and the nodes of greater priority, never the archive.
func (a *AccountTx) placeAfter(last, t int64, id string) (prev, next int64, found bool, err error) {
	ctx, tx := a.w.ctx, a.w.tx
	prev = last
	afterRank := last                   // the rank scan's position
	afterTime, afterID := t, id         // the priority scan's position
	least, haveLeast := int64(0), false // the least rank after last of greater priority read so far
	for page := 64; ; page = min(2*page, 4096) {
		rows, err := tx.QueryContext(ctx, rankScanQuery, a.id, afterRank, page)
		if err != nil {
			return 0, 0, false, err
		}
		n := 0
		for rows.Next() {
			var r, nt int64
			var nid string
			if err := rows.Scan(&r, &nt, &nid); err != nil {
				rows.Close()
				return 0, 0, false, err
			}
			n++
			if nt > t || (nt == t && nid > id) {
				rows.Close()
				rankPlacementRows.Add(int64(n))
				return prev, r, true, nil
			}
			prev, afterRank = r, r
		}
		rows.Close()
		rankPlacementRows.Add(int64(n))
		if err := rows.Err(); err != nil {
			return 0, 0, false, err
		}
		if n < page {
			return prev, 0, false, nil // no later node has a greater priority
		}
		rows, err = tx.QueryContext(ctx, priorityScanQuery, a.id, afterTime, afterID, page)
		if err != nil {
			return 0, 0, false, err
		}
		n = 0
		for rows.Next() {
			var r int64
			if err := rows.Scan(&afterTime, &afterID, &r); err != nil {
				rows.Close()
				return 0, 0, false, err
			}
			n++
			if r > last && (!haveLeast || r < least) {
				least, haveLeast = r, true
			}
		}
		rows.Close()
		rankPlacementRows.Add(int64(n))
		if err := rows.Err(); err != nil {
			return 0, 0, false, err
		}
		if n < page {
			// Every node of greater priority is read.
			if !haveLeast {
				err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rank), ?2) FROM node_order WHERE account_id = ?1 AND rank > ?2`, a.id, last).Scan(&prev)
				return prev, 0, false, err
			}
			err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rank), ?2) FROM node_order WHERE account_id = ?1 AND rank > ?2 AND rank < ?3`,
				a.id, last, least).Scan(&prev)
			return prev, least, true, err
		}
	}
}

// The priority scan seeks straight to its cursor (time, then ID) with a row
// value: a separate time bound and an ID filter would step through every
// node of the cursor's time.
const (
	rankScanQuery = `SELECT o.rank, n.normalized_time, n.node_id FROM node_order o INDEXED BY node_order_rank
		CROSS JOIN causal_nodes n ON n.account_id = o.account_id AND n.node_id = o.node_id
		WHERE o.account_id = ? AND o.rank > ? ORDER BY o.rank LIMIT ?`
	priorityScanQuery = `SELECT n.normalized_time, n.node_id, o.rank FROM causal_nodes n INDEXED BY causal_nodes_priority
		CROSS JOIN node_order o ON o.account_id = n.account_id AND o.node_id = n.node_id
		WHERE n.account_id = ?1 AND (n.normalized_time, n.node_id) > (?2, ?3)
		ORDER BY n.normalized_time, n.node_id LIMIT ?4`
)

// lastRank is the greatest rank of the nodes (0 for none), read in batches:
// a node can follow more nodes than a statement takes variables (a server
// decision after a power user's import follows tens of thousands).
func (a *AccountTx) lastRank(nodes []string) (int64, error) {
	var last int64
	err := inBatches(nodes, func(batch []string, placeholders string) error {
		args := []any{a.id}
		for _, d := range batch {
			args = append(args, d)
		}
		var r int64
		if err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COALESCE(MAX(rank), 0) FROM node_order WHERE account_id = ? AND node_id IN (`+placeholders+`)`,
			args...).Scan(&r); err != nil {
			return err
		}
		last = max(last, r)
		return nil
	})
	return last, err
}

// renumber respaces every rank of the account, keeping the order.
func (a *AccountTx) renumber() error {
	ctx, tx := a.w.ctx, a.w.tx
	rows, err := tx.QueryContext(ctx, `SELECT node_id FROM node_order WHERE account_id = ? ORDER BY rank`, a.id)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	// Two passes so the unique index never sees a collision: negative, then final.
	for i, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE node_order SET rank = ? WHERE account_id = ? AND node_id = ?`, -int64(i+1), a.id, id); err != nil {
			return err
		}
	}
	for i, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE node_order SET rank = ? WHERE account_id = ? AND node_id = ?`, int64(i+1)*rankGap, a.id, id); err != nil {
			return err
		}
	}
	return nil
}

// OrderedNodeIDs returns the account's causal nodes in rank order (tests and checks).
func (a *AccountReader) OrderedNodeIDs() ([]string, error) {
	return queryStrings(a.r.ctx, a.r.tx, `SELECT node_id FROM node_order WHERE account_id = ? ORDER BY rank`, a.id)
}

// ---- contributions

// ScopedContribution is a stored contribution with its node's rank.
type ScopedContribution struct {
	NodeID string
	Rank   int64
	Effect string // canonical JSON
}

// Contribute records a node's effect on a scope (idempotent per scope and node).
func (a *AccountTx) Contribute(scope, nodeID, effect string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO contributions (account_id, scope, node_id, effect) VALUES (?, ?, ?, ?)
		ON CONFLICT (account_id, scope, node_id) DO UPDATE SET effect = excluded.effect`, a.id, scope, nodeID, effect)
	return err
}

// Contributions returns a scope's contributions in rank order.
func (a *AccountTx) Contributions(scope string) ([]ScopedContribution, error) {
	return contributions(a.w.tx, a, scope)
}

// contributionsQuery reads one scope's contributions. No ORDER BY: it would
// make SQLite walk the account's whole rank index per scope; the folds sort by
// rank themselves.
const contributionsQuery = `SELECT c.node_id, o.rank, c.effect FROM contributions c
	JOIN node_order o ON o.account_id = c.account_id AND o.node_id = c.node_id
	WHERE c.account_id = ? AND c.scope = ?`

func contributions(tx *sql.Tx, a *AccountTx, scope string) ([]ScopedContribution, error) {
	rows, err := tx.QueryContext(a.w.ctx, contributionsQuery, a.id, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScopedContribution
	for rows.Next() {
		var c ScopedContribution
		if err := rows.Scan(&c.NodeID, &c.Rank, &c.Effect); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ContributionsBetween returns a scope's contributions ranked strictly between
// low and high (high < 0: no upper bound), for checks that need only part of a
// scope's history.
func (a *AccountTx) ContributionsBetween(scope string, low, high int64) ([]ScopedContribution, error) {
	q := contributionsQuery + ` AND o.rank > ?`
	args := []any{a.id, scope, low}
	if high >= 0 {
		q += ` AND o.rank < ?`
		args = append(args, high)
	}
	rows, err := a.w.tx.QueryContext(a.w.ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScopedContribution
	for rows.Next() {
		var c ScopedContribution
		if err := rows.Scan(&c.NodeID, &c.Rank, &c.Effect); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ScopesWithPrefix lists scopes starting with prefix (an unsubscribe's fan-out).
func (a *AccountTx) ScopesWithPrefix(prefix string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT DISTINCT scope FROM contributions WHERE account_id = ? AND substr(scope, 1, length(?2)) = ?2 ORDER BY scope`,
		a.id, prefix)
}

// ---- records

// RecordRow is a live record.
type RecordRow struct {
	Entity, EntityID, FieldGroup string
	Value, Provenance            string // canonical JSON
}

// Record returns a live record.
func (a *AccountTx) Record(entity, id, group string) (RecordRow, error) {
	r := RecordRow{Entity: entity, EntityID: id, FieldGroup: group}
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT value, provenance FROM records WHERE account_id = ? AND entity = ? AND entity_id = ? AND field_group = ?`,
		a.id, entity, id, group).Scan(&r.Value, &r.Provenance)
	if errors.Is(err, sql.ErrNoRows) {
		return RecordRow{}, ErrNotFound
	}
	return r, err
}

// PutRecord stores a live record.
func (a *AccountTx) PutRecord(r RecordRow) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO records (account_id, entity, entity_id, field_group, value, provenance) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (account_id, entity, entity_id, field_group) DO UPDATE SET value = excluded.value, provenance = excluded.provenance`,
		a.id, r.Entity, r.EntityID, r.FieldGroup, r.Value, r.Provenance)
	return err
}

// DeleteRecord removes a live record (it became a tombstone); reports whether it existed.
func (a *AccountTx) DeleteRecord(entity, id, group string) (bool, error) {
	res, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM records WHERE account_id = ? AND entity = ? AND entity_id = ? AND field_group = ?`,
		a.id, entity, id, group)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// Records returns live records ordered by (entity, id, field group), optionally
// restricted to field groups.
func (a *AccountReader) Records(groups []string) ([]RecordRow, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT entity, entity_id, field_group, value, provenance FROM records
		WHERE account_id = ? ORDER BY entity, entity_id, field_group`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	want := map[string]bool{}
	for _, g := range groups {
		want[g] = true
	}
	var out []RecordRow
	for rows.Next() {
		var r RecordRow
		if err := rows.Scan(&r.Entity, &r.EntityID, &r.FieldGroup, &r.Value, &r.Provenance); err != nil {
			return nil, err
		}
		if len(want) == 0 || want[r.FieldGroup] {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

// ---- catalog skeleton

// PodcastByAlias returns the podcast owning a normalized URL, or "".
func (a *AccountTx) PodcastByAlias(normalized string) (string, error) {
	var p string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT podcast_id FROM podcast_aliases WHERE account_id = ? AND normalized_url = ?`, a.id, normalized).Scan(&p)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return p, err
}

// CreatePodcast registers a podcast with its first alias.
func (a *AccountTx) CreatePodcast(podcastID, creatingOp string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, ?)`, a.id, podcastID, creatingOp)
	return err
}

// PodcastExists reports whether the podcast is registered.
func (a *AccountTx) PodcastExists(podcastID string) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM podcasts WHERE account_id = ? AND podcast_id = ?`, a.id, podcastID).Scan(&n)
	return n > 0, err
}

// AddAlias claims a normalized URL for a podcast.
func (a *AccountTx) AddAlias(normalized, podcastID, raw, source, nodeID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO podcast_aliases (account_id, normalized_url, podcast_id, raw_url, source, node_id) VALUES (?, ?, ?, ?, ?, ?)`,
		a.id, normalized, podcastID, raw, source, nodeID)
	return err
}

// Alias is one feed URL alias.
type Alias struct{ Normalized, Raw string }

// Aliases lists a podcast's aliases by normalized URL.
func (a *AccountTx) Aliases(podcastID string) ([]Alias, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT normalized_url, raw_url FROM podcast_aliases WHERE account_id = ? AND podcast_id = ? ORDER BY normalized_url`, a.id, podcastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Alias
	for rows.Next() {
		var x Alias
		if err := rows.Scan(&x.Normalized, &x.Raw); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// GenerationRow is a generation's catalog entry.
type GenerationRow struct {
	ID, PodcastID, CreatingOp, Predecessor, ClosedBy string
}

// Generation returns a generation.
func (a *AccountTx) Generation(id string) (GenerationRow, error) {
	g := GenerationRow{ID: id}
	var pred, closed sql.NullString
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT podcast_id, creating_op, predecessor, closed_by FROM generations WHERE account_id = ? AND generation_id = ?`,
		a.id, id).Scan(&g.PodcastID, &g.CreatingOp, &pred, &closed)
	if errors.Is(err, sql.ErrNoRows) {
		return GenerationRow{}, ErrNotFound
	}
	g.Predecessor, g.ClosedBy = pred.String, closed.String
	return g, err
}

// Generations lists a podcast's generations in creation order (rank of the
// creating op); a generation joined into another (GenerationJoin) is none of
// its own.
func (a *AccountTx) Generations(podcastID string) ([]GenerationRow, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT g.generation_id, g.creating_op, COALESCE(g.predecessor, ''), COALESCE(g.closed_by, '')
		FROM generations g JOIN node_order o ON o.account_id = g.account_id AND o.node_id = g.creating_op
		WHERE g.account_id = ? AND g.podcast_id = ?
		AND NOT EXISTS (SELECT 1 FROM generation_joins j WHERE j.account_id = g.account_id AND j.generation_id = g.generation_id)
		ORDER BY o.rank`, a.id, podcastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GenerationRow
	for rows.Next() {
		g := GenerationRow{PodcastID: podcastID}
		if err := rows.Scan(&g.ID, &g.CreatingOp, &g.Predecessor, &g.ClosedBy); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// CreateGeneration registers a generation.
func (a *AccountTx) CreateGeneration(id, podcastID, creatingOp, predecessor string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO generations (account_id, generation_id, podcast_id, creating_op, predecessor) VALUES (?, ?, ?, ?, NULLIF(?, ''))`,
		a.id, id, podcastID, creatingOp, predecessor)
	return err
}

// CloseGeneration records the unsubscribe that closed a generation.
func (a *AccountTx) CloseGeneration(id, nodeID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE generations SET closed_by = ? WHERE account_id = ? AND generation_id = ? AND closed_by IS NULL`, nodeID, a.id, id)
	return err
}

// EpisodePodcast returns the podcast an episode was first seen with, or "".
func (a *AccountTx) EpisodePodcast(episodeID string) (string, error) {
	var p string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT podcast_id FROM episodes WHERE account_id = ? AND episode_id = ?`, a.id, episodeID).Scan(&p)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return p, err
}

// EpisodeOccurrence returns an episode's first occurrence evidence (canonical JSON).
func (a *AccountTx) EpisodeOccurrence(episodeID string) (string, error) {
	var o string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT occurrence FROM episodes WHERE account_id = ? AND episode_id = ?`, a.id, episodeID).Scan(&o)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return o, err
}

// AddEpisode registers an episode with its first occurrence evidence and the
// node whose acceptance registered it.
func (a *AccountTx) AddEpisode(episodeID, podcastID, occurrence, node string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO episodes (account_id, episode_id, podcast_id, occurrence, registered_by) VALUES (?, ?, ?, ?, ?)`,
		a.id, episodeID, podcastID, occurrence, node)
	return err
}

// Registration is the rank of the node that registered an episode: its
// commit (epoch, seq) (a fact's receipt, or the decision's), then the node's
// ID (a decision's rank, contract-v1; a fact the same way). OK is false when
// it is unknown (a registration before this was recorded).
type Registration struct {
	Epoch, Seq int64
	Node       string
	OK         bool
}

// Less orders registrations; unknown ones last.
func (r Registration) Less(o Registration) bool {
	if r.OK != o.OK {
		return r.OK
	}
	if r.Epoch != o.Epoch {
		return r.Epoch < o.Epoch
	}
	if r.Seq != o.Seq {
		return r.Seq < o.Seq
	}
	return r.Node < o.Node
}

// EpisodeRegistration is the rank of the node that registered an episode.
func (a *AccountTx) EpisodeRegistration(episodeID string) (Registration, error) {
	var r Registration
	var epoch, seq sql.NullInt64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT e.registered_by, COALESCE(f.epoch, d.epoch), COALESCE(f.seq, d.seq) FROM episodes e
		LEFT JOIN facts f ON f.account_id = e.account_id AND f.op_id = e.registered_by
		LEFT JOIN decisions d ON d.account_id = e.account_id AND d.decision_id = e.registered_by
		WHERE e.account_id = ? AND e.episode_id = ?`, a.id, episodeID).Scan(&r.Node, &epoch, &seq)
	if errors.Is(err, sql.ErrNoRows) {
		return r, nil
	}
	r.Epoch, r.Seq, r.OK = epoch.Int64, seq.Int64, epoch.Valid && r.Node != ""
	return r, err
}

// ---- secrets

// Secret returns a sealed secret, or ErrNotFound.
func (a *AccountTx) Secret(id string) ([]byte, error) {
	var b []byte
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT sealed FROM secrets WHERE account_id = ? AND secret_id = ?`, a.id, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

// PutSecret stores a sealed secret.
func (a *AccountTx) PutSecret(id string, sealed []byte) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO secrets (account_id, secret_id, sealed, created_at) VALUES (?, ?, ?, ?)`, a.id, id, sealed, a.w.nowMillis())
	return err
}

// Secret returns a sealed secret in a read snapshot.
func (a *AccountReader) Secret(id string) ([]byte, error) {
	var b []byte
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT sealed FROM secrets WHERE account_id = ? AND secret_id = ?`, a.id, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

// Namespace returns the account namespace (identity derivations).
func (a *AccountTx) Namespace() (string, error) {
	var ns string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT namespace FROM accounts WHERE account_id = ?`, a.id).Scan(&ns)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNoAccount
	}
	return ns, err
}

// ---- stored feed responses (PROTOCOL §9.2)

// PutFeedResponse replaces a podcast's stored response (sealed by the
// caller). A new response has no re-parse outcome.
func (a *AccountTx) PutFeedResponse(podcastID string, sealed []byte) error {
	return a.PutReparsedFeedResponse(podcastID, sealed, ReparseOutcome{})
}

// ReparseOutcome is a stored response's re-parse outcome, kept in the clear
// beside it for diagnostics (PROTOCOL §9.4): a category and counts, never publisher
// content.
type ReparseOutcome struct {
	Failure  string // "" or "timeout": given up after Timeouts timeouts for Parser
	Timeouts int    // re-parses for Parser that ran past their deadline
	Parser   string // the parser version they were for
}

// PutReparsedFeedResponse replaces a podcast's stored response and its
// re-parse outcome together.
func (a *AccountTx) PutReparsedFeedResponse(podcastID string, sealed []byte, o ReparseOutcome) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO feed_responses (account_id, podcast_id, sealed, stored_at, reparse_failure,
		reparse_timeouts, reparse_parser) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (account_id, podcast_id) DO UPDATE SET sealed = excluded.sealed, stored_at = excluded.stored_at,
		reparse_failure = excluded.reparse_failure, reparse_timeouts = excluded.reparse_timeouts, reparse_parser = excluded.reparse_parser`,
		a.id, podcastID, sealed, a.w.nowMillis(), o.Failure, o.Timeouts, o.Parser)
	return err
}

// FeedResponse returns a podcast's sealed stored response (ErrNotFound if none).
func (a *AccountTx) FeedResponse(podcastID string) ([]byte, error) {
	var b []byte
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT sealed FROM feed_responses WHERE account_id = ? AND podcast_id = ?`, a.id, podcastID).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

// AddGenerationJoin joins a provisional generation into the effective one
// (PROTOCOL §6.11): references to it route there.
func (a *AccountTx) AddGenerationJoin(generationID, into, nodeID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO generation_joins (account_id, generation_id, into_generation, node_id) VALUES (?, ?, ?, ?)`,
		a.id, generationID, into, nodeID)
	return err
}

// GenerationJoin returns the generation a generation was joined into ("" for
// none).
func (a *AccountTx) GenerationJoin(generationID string) (string, error) {
	var into string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT into_generation FROM generation_joins WHERE account_id = ? AND generation_id = ?`,
		a.id, generationID).Scan(&into)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return into, err
}
