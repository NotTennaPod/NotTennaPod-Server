package reference

import (
	"database/sql"
	"errors"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The oracle's own copies of the readers' queries (storage's Head,
// ViewRecords, PodcastRanks, CatalogRows and EpisodeRows as of the frozen
// revision), run on the read transaction itself.

type querier struct{ r *storage.ReadTx }

func (q querier) rows(query string, args []any, scan func(*sql.Rows) error) error {
	ctx, tx := storage.RawReadForTesting(q.r)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (q querier) head(account string) (storage.Head, error) {
	ctx, tx := storage.RawReadForTesting(q.r)
	var h storage.Head
	err := tx.QueryRowContext(ctx, "SELECT log_id, epoch, head_seq, head_digest FROM account_logs WHERE account_id = ?", account).
		Scan(&h.LogID, &h.Epoch, &h.Seq, &h.Digest)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.Head{}, storage.ErrNoAccount
	}
	return h, err
}

func (q querier) viewRecords(account string) ([]storage.RecordRow, error) {
	var out []storage.RecordRow
	err := q.rows(`SELECT entity, entity_id, field_group,
		CASE WHEN entity = 'episode' AND field_group = 'metadata' THEN json_remove(value, '$.description', '$.chapters') ELSE value END,
		provenance FROM records WHERE account_id = ? ORDER BY entity, entity_id, field_group`, []any{account}, func(rows *sql.Rows) error {
		var r storage.RecordRow
		err := rows.Scan(&r.Entity, &r.EntityID, &r.FieldGroup, &r.Value, &r.Provenance)
		out = append(out, r)
		return err
	})
	return out, err
}

func (q querier) podcastRanks(account string) (map[string]int64, error) {
	out := map[string]int64{}
	err := q.rows(`SELECT p.podcast_id, COALESCE(o.rank, -1) FROM podcasts p
		LEFT JOIN node_order o ON o.account_id = p.account_id AND o.node_id = p.creating_op
		WHERE p.account_id = ?`, []any{account}, func(rows *sql.Rows) error {
		var id string
		var rank int64
		err := rows.Scan(&id, &rank)
		out[id] = rank
		return err
	})
	return out, err
}

func (q querier) catalogRows(account string) ([]storage.CatalogRow, error) {
	var out []storage.CatalogRow
	err := q.rows(`SELECT episode_id, pub_date, rowid FROM catalog_items WHERE account_id = ?`, []any{account}, func(rows *sql.Rows) error {
		var c storage.CatalogRow
		err := rows.Scan(&c.EpisodeID, &c.PubDate, &c.Order)
		out = append(out, c)
		return err
	})
	return out, err
}

func (q querier) episodeRows(account string) ([]storage.EpisodeRow, error) {
	var out []storage.EpisodeRow
	err := q.rows(`SELECT episode_id, podcast_id, occurrence FROM episodes WHERE account_id = ? ORDER BY episode_id`, []any{account},
		func(rows *sql.Rows) error {
			var e storage.EpisodeRow
			err := rows.Scan(&e.EpisodeID, &e.PodcastID, &e.Occurrence)
			out = append(out, e)
			return err
		})
	return out, err
}
