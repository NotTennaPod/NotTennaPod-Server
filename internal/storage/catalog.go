package storage

import (
	"database/sql"
	"errors"
)

// CatalogItem is an episode the account's catalog knows from feeds (PROTOCOL §9.4).
type CatalogItem struct {
	EpisodeID, PodcastID string
	FirstNode            string
	PlayableNode         string // "" until an observation had an enclosure
	CatalogOnlyNode      string // the first observation without one, before it was playable
	PubDate              int64  // effective publication date
	ServerSeenNode       string // the server's first fetch that saw it
	ServerCatalogOnly    bool   // that fetch saw it without an enclosure
}

// CatalogItem returns an episode's catalog item, if the catalog knows it.
func (a *AccountTx) CatalogItem(episodeID string) (CatalogItem, bool, error) {
	it := CatalogItem{EpisodeID: episodeID}
	var playable, catalogOnly, serverSeen sql.NullString
	var serverCatalogOnly int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT podcast_id, first_node, playable_node, catalog_only_node, pub_date, server_seen_node, server_catalog_only
		FROM catalog_items WHERE account_id = ? AND episode_id = ?`, a.id, episodeID).
		Scan(&it.PodcastID, &it.FirstNode, &playable, &catalogOnly, &it.PubDate, &serverSeen, &serverCatalogOnly)
	if errors.Is(err, sql.ErrNoRows) {
		return CatalogItem{}, false, nil
	}
	it.PlayableNode, it.CatalogOnlyNode, it.ServerSeenNode = playable.String, catalogOnly.String, serverSeen.String
	it.ServerCatalogOnly = serverCatalogOnly != 0
	return it, err == nil, err
}

// PutCatalogItem inserts or replaces a catalog item.
func (a *AccountTx) PutCatalogItem(it CatalogItem) error {
	null := func(s string) any {
		if s == "" {
			return nil
		}
		return s
	}
	flag := 0
	if it.ServerCatalogOnly {
		flag = 1
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO catalog_items (account_id, episode_id, podcast_id, first_node, playable_node,
		catalog_only_node, pub_date, server_seen_node, server_catalog_only) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (account_id, episode_id) DO UPDATE SET podcast_id = excluded.podcast_id, first_node = excluded.first_node,
		playable_node = excluded.playable_node, catalog_only_node = excluded.catalog_only_node, pub_date = excluded.pub_date,
		server_seen_node = excluded.server_seen_node, server_catalog_only = excluded.server_catalog_only`,
		a.id, it.EpisodeID, it.PodcastID, it.FirstNode, null(it.PlayableNode), null(it.CatalogOnlyNode), it.PubDate,
		null(it.ServerSeenNode), flag)
	return err
}

// ServerNewest is the newest effective publication date among a podcast's
// items the server's fetches have seen (ok is false when they saw none). It is
// one seek in catalog_items_server_newest, which holds only those items,
// however many items devices or an import added.
func (a *AccountTx) ServerNewest(podcastID string) (int64, bool, error) {
	var v sql.NullInt64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT MAX(pub_date) FROM catalog_items
		WHERE account_id = ? AND podcast_id = ? AND server_seen_node IS NOT NULL`, a.id, podcastID).Scan(&v)
	return v.Int64, v.Valid, err
}

// Population returns the node that recorded a generation's first population by
// the server ("" if none yet).
func (a *AccountTx) Population(generationID string) (string, error) {
	var node string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT node_id FROM generation_population WHERE account_id = ? AND generation_id = ?`,
		a.id, generationID).Scan(&node)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return node, err
}

// SetPopulation records a generation's first population (the first record stands).
func (a *AccountTx) SetPopulation(generationID, node string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO generation_population (account_id, generation_id, node_id) VALUES (?, ?, ?)`,
		a.id, generationID, node)
	return err
}

// DecisionBySubject returns the effective decision of a type and subject, if
// any: decisions with equal type and subject compete, and the smallest rank
// (epoch, seq, decision_id) is effective (contract-v1; PROTOCOL §6.11), whatever the
// causal order a recovered rank was joined at.
func (a *AccountTx) DecisionBySubject(decisionType, subject string) (string, error) {
	var id string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT decision_id FROM decisions
		WHERE account_id = ? AND decision_type = ? AND subject = ? ORDER BY epoch, seq, decision_id LIMIT 1`,
		a.id, decisionType, subject).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// catalogRevisionsQuery reads a podcast's catalog items' metadata revisions.
// CROSS JOIN keeps the items the outer loop, through catalog_items_podcast, so
// the work scales with the podcast's items, not the account's episode records.
const catalogRevisionsQuery = `SELECT c.episode_id, json_extract(r.value, '$.revision') FROM catalog_items c
		CROSS JOIN records r ON r.account_id = c.account_id AND r.entity = 'episode' AND r.entity_id = c.episode_id AND r.field_group = 'metadata'
		WHERE c.account_id = ? AND c.podcast_id = ?`

// CatalogRevisions returns the metadata record revisions of a podcast's
// catalog items, by episode (the bases a server fetch captures, PROTOCOL §9.4).
func (a *AccountTx) CatalogRevisions(podcastID string) (map[string]string, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, catalogRevisionsQuery, a.id, podcastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var e string
		var rev sql.NullString
		if err := rows.Scan(&e, &rev); err != nil {
			return nil, err
		}
		if rev.Valid {
			out[e] = rev.String
		}
	}
	return out, rows.Err()
}
