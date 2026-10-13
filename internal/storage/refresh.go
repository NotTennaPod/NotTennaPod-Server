package storage

import (
	"database/sql"
	"errors"
)

// The server's refresh schedule and live status (PROTOCOL §9.4), kept per
// account and podcast in refresh_schedule. It is not part of the change log.

// RefreshRow is a podcast's refresh schedule and live status.
type RefreshRow struct {
	PodcastID         string
	NextAt            int64  // ms
	Failures          int64  // consecutive
	NotBefore         int64  // Retry-After, ms (0: none)
	LastAttempt       int64  // ms (0: never)
	LastSuccess       int64  // ms (0: never)
	LastError         string // canonical JSON {label, code?, detail?} ("" for none)
	StatusPublishedAt int64  // ms
	StatusKey         string // the published status's outcome class and error
	Context           string // the fetch context the row belongs to (generation, URL, credentials; "" before it was kept)
}

// RefreshCandidate is a podcast as the refresher sees it: its published
// subscription, policy, identity and credentials records, whether its live
// generation's first population is done, and its schedule row.
type RefreshCandidate struct {
	AccountID, PodcastID string
	State                string // the subscription record's state ("" without one)
	GenerationID         string
	MergedInto           string // a merged podcast's survivor (PROTOCOL §9.7)
	KeepUpdated          bool
	RefreshURL           string
	SecretID             string
	Populated            bool
	Schedule             *RefreshRow // nil without a row
}

const refreshRowColumns = `rs.podcast_id, rs.next_at, rs.failures, rs.not_before, rs.last_attempt, rs.last_success, rs.last_error,
	rs.status_published_at, rs.status_key, rs.context`

func scanRefreshRow(scan func(...any) error, extra ...any) (RefreshRow, error) {
	var r RefreshRow
	var attempt, success sql.NullInt64
	var lastError sql.NullString
	dest := append(extra, &r.PodcastID, &r.NextAt, &r.Failures, &r.NotBefore, &attempt, &success, &lastError,
		&r.StatusPublishedAt, &r.StatusKey, &r.Context)
	if err := scan(dest...); err != nil {
		return r, err
	}
	r.LastAttempt, r.LastSuccess, r.LastError = attempt.Int64, success.Int64, lastError.String
	return r, nil
}

// RefreshCandidates lists every podcast of an account ("" for every active
// account) with what the refresher needs, ordered by account and podcast.
func (r *ReadTx) RefreshCandidates(accountID string) ([]RefreshCandidate, error) {
	query := `SELECT pc.account_id, pc.podcast_id,
		COALESCE(json_extract(s.value, '$.state'), ''), COALESCE(json_extract(s.value, '$.generation_id'), ''),
		COALESCE(json_extract(s.value, '$.merged_into'), ''), COALESCE(json_extract(p.value, '$.keep_updated'), 0), COALESCE(json_extract(i.value, '$.refresh_url'), ''),
		COALESCE(json_extract(c.value, '$.secret_id'), ''),
		EXISTS (SELECT 1 FROM generation_population g WHERE g.account_id = pc.account_id
			AND g.generation_id = json_extract(s.value, '$.generation_id')),
		rs.podcast_id IS NOT NULL, COALESCE(rs.podcast_id, ''), COALESCE(rs.next_at, 0), COALESCE(rs.failures, 0),
		COALESCE(rs.not_before, 0), rs.last_attempt, rs.last_success, rs.last_error, COALESCE(rs.status_published_at, 0),
		COALESCE(rs.status_key, ''), COALESCE(rs.context, '')
	FROM podcasts pc
	JOIN accounts a ON a.account_id = pc.account_id AND a.state = 'active'
	LEFT JOIN records s ON s.account_id = pc.account_id AND s.entity = 'podcast' AND s.entity_id = pc.podcast_id AND s.field_group = 'subscription'
	LEFT JOIN records p ON p.account_id = pc.account_id AND p.entity = 'podcast' AND p.entity_id = pc.podcast_id AND p.field_group = 'policy'
	LEFT JOIN records i ON i.account_id = pc.account_id AND i.entity = 'podcast' AND i.entity_id = pc.podcast_id AND i.field_group = 'identity'
	LEFT JOIN records c ON c.account_id = pc.account_id AND c.entity = 'podcast' AND c.entity_id = pc.podcast_id AND c.field_group = 'credentials'
	LEFT JOIN refresh_schedule rs ON rs.account_id = pc.account_id AND rs.podcast_id = pc.podcast_id`
	var args []any
	if accountID != "" {
		query += ` WHERE pc.account_id = ?`
		args = append(args, accountID)
	}
	query += ` ORDER BY pc.account_id, pc.podcast_id`
	rows, err := r.tx.QueryContext(r.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RefreshCandidate
	for rows.Next() {
		var c RefreshCandidate
		var keep, populated, scheduled int64
		row, err := scanRefreshRow(rows.Scan, &c.AccountID, &c.PodcastID, &c.State, &c.GenerationID, &c.MergedInto, &keep, &c.RefreshURL,
			&c.SecretID, &populated, &scheduled)
		if err != nil {
			return nil, err
		}
		c.KeepUpdated, c.Populated = keep != 0, populated != 0
		if scheduled != 0 {
			c.Schedule = &row
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// RefreshSchedule returns a podcast's schedule row, if it has one.
func (a *AccountTx) RefreshSchedule(podcastID string) (RefreshRow, bool, error) {
	row, err := scanRefreshRow(a.w.tx.QueryRowContext(a.w.ctx, `SELECT `+refreshRowColumns+` FROM refresh_schedule rs
		WHERE rs.account_id = ? AND rs.podcast_id = ?`, a.id, podcastID).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return RefreshRow{}, false, nil
	}
	return row, err == nil, err
}

// PutRefreshSchedule inserts or replaces a podcast's schedule row (the podcast
// must exist).
func (a *AccountTx) PutRefreshSchedule(r RefreshRow) error {
	nullInt := func(v int64) any {
		if v == 0 {
			return nil
		}
		return v
	}
	var lastError any
	if r.LastError != "" {
		lastError = r.LastError
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO refresh_schedule (account_id, podcast_id, next_at, failures, not_before,
		last_attempt, last_success, last_error, status_published_at, status_key, context) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (account_id, podcast_id) DO UPDATE SET next_at = excluded.next_at, failures = excluded.failures,
		not_before = excluded.not_before, last_attempt = excluded.last_attempt, last_success = excluded.last_success,
		last_error = excluded.last_error, status_published_at = excluded.status_published_at, status_key = excluded.status_key,
		context = excluded.context`,
		a.id, r.PodcastID, r.NextAt, r.Failures, r.NotBefore, nullInt(r.LastAttempt), nullInt(r.LastSuccess), lastError,
		r.StatusPublishedAt, r.StatusKey, r.Context)
	return err
}

// CatalogStamp is a canonical metadata record's revision and fetch time.
type CatalogStamp struct {
	Revision  string
	FetchTime int64
}

// CatalogStamps returns the revision and fetch time of a podcast's metadata
// record (key "feed") and of its catalog items' records (by episode ID).
func (a *AccountTx) CatalogStamps(podcastID string) (map[string]CatalogStamp, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT 'feed', json_extract(value, '$.revision'), json_extract(value, '$.fetch_time')
		FROM records WHERE account_id = ? AND entity = 'podcast' AND entity_id = ? AND field_group = 'metadata'
		UNION ALL
		SELECT c.episode_id, json_extract(r.value, '$.revision'), json_extract(r.value, '$.fetch_time') FROM catalog_items c
		JOIN records r ON r.account_id = c.account_id AND r.entity = 'episode' AND r.entity_id = c.episode_id AND r.field_group = 'metadata'
		WHERE c.account_id = ? AND c.podcast_id = ?`, a.id, podcastID, a.id, podcastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]CatalogStamp{}
	for rows.Next() {
		var k string
		var rev sql.NullString
		var fetch sql.NullInt64
		if err := rows.Scan(&k, &rev, &fetch); err != nil {
			return nil, err
		}
		if rev.Valid {
			out[k] = CatalogStamp{Revision: rev.String, FetchTime: fetch.Int64}
		}
	}
	return out, rows.Err()
}

// FeedResponseKey names a stored feed response.
type FeedResponseKey struct{ AccountID, PodcastID string }

// StoredFeedResponses lists every stored feed response of the active accounts.
func (r *ReadTx) StoredFeedResponses() ([]FeedResponseKey, error) {
	rows, err := r.tx.QueryContext(r.ctx, `SELECT f.account_id, f.podcast_id FROM feed_responses f
		JOIN accounts a ON a.account_id = f.account_id AND a.state = 'active' ORDER BY f.account_id, f.podcast_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FeedResponseKey
	for rows.Next() {
		var k FeedResponseKey
		if err := rows.Scan(&k.AccountID, &k.PodcastID); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// ReparseOutcome returns a stored response's re-parse outcome (ErrNotFound
// if there is no response).
func (a *AccountReader) ReparseOutcome(podcastID string) (ReparseOutcome, error) {
	var o ReparseOutcome
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT reparse_failure, reparse_timeouts, reparse_parser FROM feed_responses
		WHERE account_id = ? AND podcast_id = ?`, a.id, podcastID).Scan(&o.Failure, &o.Timeouts, &o.Parser)
	if errors.Is(err, sql.ErrNoRows) {
		return o, ErrNotFound
	}
	return o, err
}

// ReparseFailure is a podcast whose stored response a re-parse gave up on.
type ReparseFailure struct {
	PodcastID string
	ReparseOutcome
}

// ReparseFailures lists the account's stored responses a re-parse gave up
// on, by podcast, without reading the responses (diagnostics).
func (a *AccountReader) ReparseFailures() ([]ReparseFailure, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT podcast_id, reparse_failure, reparse_timeouts, reparse_parser FROM feed_responses
		WHERE account_id = ? AND reparse_failure != '' ORDER BY podcast_id`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReparseFailure
	for rows.Next() {
		var f ReparseFailure
		if err := rows.Scan(&f.PodcastID, &f.Failure, &f.Timeouts, &f.Parser); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// FeedResponse returns a podcast's sealed stored response (ErrNotFound if none).
func (a *AccountReader) FeedResponse(podcastID string) ([]byte, error) {
	var b []byte
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT sealed FROM feed_responses WHERE account_id = ? AND podcast_id = ?`, a.id, podcastID).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}
