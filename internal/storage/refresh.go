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
	QuietSince        int64  // ms: the start of the fetch since which no successful fetch changed the catalog (0: unknown)
	// Refresh requests (PROTOCOL §9.6), read here and written only by
	// RecordRefreshRequest, FinishRefreshAttempt and ResolveRefreshRequests:
	// PutRefreshSchedule never writes them, so a fetch writing back the row
	// it captured cannot undo a request that arrived meanwhile.
	RequestSeq      int64 // requests made so far
	RequestForceSeq int64 // the newest single-podcast (forced) request's sequence
	DoneSeq         int64 // the newest request an ended attempt answered
}

// RequestPending reports a request no attempt has answered yet.
func (r RefreshRow) RequestPending() bool { return r.RequestSeq > r.DoneSeq }

// RequestForced reports a pending single-podcast request: the next attempt
// fetches without validators.
func (r RefreshRow) RequestForced() bool { return r.RequestForceSeq > r.DoneSeq }

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
	rs.status_published_at, rs.status_key, rs.context, rs.quiet_since, rs.request_seq, rs.request_force_seq, rs.done_seq`

func scanRefreshRow(scan func(...any) error, extra ...any) (RefreshRow, error) {
	var r RefreshRow
	var attempt, success, quiet sql.NullInt64
	var lastError sql.NullString
	dest := append(extra, &r.PodcastID, &r.NextAt, &r.Failures, &r.NotBefore, &attempt, &success, &lastError,
		&r.StatusPublishedAt, &r.StatusKey, &r.Context, &quiet, &r.RequestSeq, &r.RequestForceSeq, &r.DoneSeq)
	if err := scan(dest...); err != nil {
		return r, err
	}
	r.LastAttempt, r.LastSuccess, r.LastError, r.QuietSince = attempt.Int64, success.Int64, lastError.String, quiet.Int64
	return r, nil
}

// refreshCandidatesQuery reads podcasts with what the refresher needs; the
// caller appends the WHERE clause.
const refreshCandidatesQuery = `SELECT pc.account_id, pc.podcast_id,
		COALESCE(json_extract(s.value, '$.state'), ''), COALESCE(json_extract(s.value, '$.generation_id'), ''),
		COALESCE(json_extract(s.value, '$.merged_into'), ''), COALESCE(json_extract(p.value, '$.keep_updated'), 0), COALESCE(json_extract(i.value, '$.refresh_url'), ''),
		COALESCE(json_extract(c.value, '$.secret_id'), ''),
		EXISTS (SELECT 1 FROM generation_population g WHERE g.account_id = pc.account_id
			AND g.generation_id = json_extract(s.value, '$.generation_id')),
		rs.podcast_id IS NOT NULL, COALESCE(rs.podcast_id, ''), COALESCE(rs.next_at, 0), COALESCE(rs.failures, 0),
		COALESCE(rs.not_before, 0), rs.last_attempt, rs.last_success, rs.last_error, COALESCE(rs.status_published_at, 0),
		COALESCE(rs.status_key, ''), COALESCE(rs.context, ''), rs.quiet_since,
		COALESCE(rs.request_seq, 0), COALESCE(rs.request_force_seq, 0), COALESCE(rs.done_seq, 0)
	FROM podcasts pc
	JOIN accounts a ON a.account_id = pc.account_id AND a.state = 'active'
	LEFT JOIN records s ON s.account_id = pc.account_id AND s.entity = 'podcast' AND s.entity_id = pc.podcast_id AND s.field_group = 'subscription'
	LEFT JOIN records p ON p.account_id = pc.account_id AND p.entity = 'podcast' AND p.entity_id = pc.podcast_id AND p.field_group = 'policy'
	LEFT JOIN records i ON i.account_id = pc.account_id AND i.entity = 'podcast' AND i.entity_id = pc.podcast_id AND i.field_group = 'identity'
	LEFT JOIN records c ON c.account_id = pc.account_id AND c.entity = 'podcast' AND c.entity_id = pc.podcast_id AND c.field_group = 'credentials'
	LEFT JOIN refresh_schedule rs ON rs.account_id = pc.account_id AND rs.podcast_id = pc.podcast_id`

// refreshCandidateQuery reads one podcast of an account: a primary-key
// lookup in every library table it reads (PROTOCOL §9.6, the refresh status
// read).
const refreshCandidateQuery = refreshCandidatesQuery + ` WHERE pc.account_id = ? AND pc.podcast_id = ?`

func scanRefreshCandidate(scan func(...any) error) (RefreshCandidate, error) {
	var c RefreshCandidate
	var keep, populated, scheduled int64
	row, err := scanRefreshRow(scan, &c.AccountID, &c.PodcastID, &c.State, &c.GenerationID, &c.MergedInto, &keep, &c.RefreshURL,
		&c.SecretID, &populated, &scheduled)
	if err != nil {
		return c, err
	}
	c.KeepUpdated, c.Populated = keep != 0, populated != 0
	if scheduled != 0 {
		c.Schedule = &row
	}
	return c, nil
}

// RefreshCandidates lists every podcast of an account ("" for every active
// account) with what the refresher needs, ordered by account and podcast.
func (r *ReadTx) RefreshCandidates(accountID string) ([]RefreshCandidate, error) {
	query := refreshCandidatesQuery
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
		c, err := scanRefreshCandidate(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// RefreshStatusFor reads the named podcasts of an account as RefreshCandidates
// does, each by its keys alone, so the work grows with the podcasts named,
// never with the library (PROTOCOL §9.6). A podcast the account does not have
// is left out; the result keeps the order of the IDs, each once.
func (r *ReadTx) RefreshStatusFor(accountID string, podcastIDs []string) ([]RefreshCandidate, error) {
	stmt, err := r.tx.PrepareContext(r.ctx, refreshCandidateQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	out := make([]RefreshCandidate, 0, len(podcastIDs))
	seen := make(map[string]bool, len(podcastIDs))
	for _, id := range podcastIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		c, err := scanRefreshCandidate(stmt.QueryRowContext(r.ctx, accountID, id).Scan)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
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
		last_attempt, last_success, last_error, status_published_at, status_key, context, quiet_since)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (account_id, podcast_id) DO UPDATE SET next_at = excluded.next_at, failures = excluded.failures,
		not_before = excluded.not_before, last_attempt = excluded.last_attempt, last_success = excluded.last_success,
		last_error = excluded.last_error, status_published_at = excluded.status_published_at, status_key = excluded.status_key,
		context = excluded.context, quiet_since = excluded.quiet_since`,
		a.id, r.PodcastID, r.NextAt, r.Failures, r.NotBefore, nullInt(r.LastAttempt), nullInt(r.LastSuccess), lastError,
		r.StatusPublishedAt, r.StatusKey, r.Context, nullInt(r.QuietSince))
	return err
}

// RecordRefreshRequest records a refresh request for a podcast whose
// schedule row exists and returns its sequence. A single-podcast request
// (force) also becomes the newest forced one, so force, once asked for, stays
// until an attempt that fetched without validators answers it.
func (a *AccountTx) RecordRefreshRequest(podcastID string, force bool) (int64, error) {
	var seq int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `UPDATE refresh_schedule SET request_seq = request_seq + 1,
		request_force_seq = CASE WHEN ? THEN request_seq + 1 ELSE request_force_seq END
		WHERE account_id = ? AND podcast_id = ? RETURNING request_seq`, force, a.id, podcastID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return seq, err
}

// FinishRefreshAttempt records, in an ended attempt's outcome transaction,
// the requests it answered. startSeq is request_seq when the attempt
// started, and forced whether it fetched without validators. A forced attempt
// answers every request so far, those that joined it included. An ordinary
// one answers the requests that joined it too, unless any forced request was
// made after it started: then it answers only those it started with (all
// ordinary, as it was not forced), and the rest wait for a forced attempt of
// their own. Only the newest forced sequence is kept, so the first forced
// request after the start is not known, and nothing past the start is
// claimed.
func (a *AccountTx) FinishRefreshAttempt(podcastID string, startSeq int64, forced bool) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE refresh_schedule SET done_seq = MAX(done_seq,
		CASE WHEN ?1 OR request_force_seq <= ?2 THEN request_seq ELSE ?2 END)
		WHERE account_id = ?3 AND podcast_id = ?4`, forced, startSeq, a.id, podcastID)
	return err
}

// ResolveRefreshRequests answers a podcast's requests up to upTo without an
// attempt: those of a podcast merged into another, and an ordinary request
// left to the schedule by a Retry-After.
func (a *AccountTx) ResolveRefreshRequests(podcastID string, upTo int64) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE refresh_schedule SET done_seq = MAX(done_seq, MIN(?, request_seq))
		WHERE account_id = ? AND podcast_id = ?`, upTo, a.id, podcastID)
	return err
}

// CatalogStamp is a canonical metadata record's revision and fetch time.
type CatalogStamp struct {
	Revision  string
	FetchTime int64
}

// catalogStampsQuery reads the revision and fetch time of a podcast's metadata
// record and of its catalog items' metadata records; CROSS JOIN keeps the
// items the outer loop, as in catalogRevisionsQuery.
const catalogStampsQuery = `SELECT 'feed', json_extract(value, '$.revision'), json_extract(value, '$.fetch_time')
		FROM records WHERE account_id = ? AND entity = 'podcast' AND entity_id = ? AND field_group = 'metadata'
		UNION ALL
		SELECT c.episode_id, json_extract(r.value, '$.revision'), json_extract(r.value, '$.fetch_time') FROM catalog_items c
		CROSS JOIN records r ON r.account_id = c.account_id AND r.entity = 'episode' AND r.entity_id = c.episode_id AND r.field_group = 'metadata'
		WHERE c.account_id = ? AND c.podcast_id = ?`

// CatalogStamps returns the revision and fetch time of a podcast's metadata
// record (key "feed") and of its catalog items' records (by episode ID).
func (a *AccountTx) CatalogStamps(podcastID string) (map[string]CatalogStamp, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, catalogStampsQuery, a.id, podcastID, a.id, podcastID)
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
