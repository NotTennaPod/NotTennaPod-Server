package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
)

// Identity reconciliation (PROTOCOL §9.7): occurrence evidence, lookup
// aliases, release groups and merges.

// Occurrence is one observed (GUID, enclosure, publication date) of a
// podcast: identity evidence (PROTOCOL §9.7).
type Occurrence struct {
	GUID, Enclosure string // "" when absent (the enclosure normalized)
	PubDate         *int64
	EpisodeID       string // the episode it derived to when recorded ("" until known)
	Node            string // its witness: the first node that recorded it, an archived observation before a fetch (ClaimOccurrence)
	Kind            string // its GUID kind: atom-id or rss-guid
}

const occurrenceColumns = `guid, enclosure, pub_date, episode_id, node_id, kind`

func scanOccurrences(rows *sql.Rows) ([]Occurrence, error) {
	defer rows.Close()
	var out []Occurrence
	for rows.Next() {
		var o Occurrence
		var d sql.NullInt64
		if err := rows.Scan(&o.GUID, &o.Enclosure, &d, &o.EpisodeID, &o.Node, &o.Kind); err != nil {
			return nil, err
		}
		if d.Valid {
			v := d.Int64
			o.PubDate = &v
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// AddOccurrence records occurrence evidence; false when it was already known.
func (a *AccountTx) AddOccurrence(podcastID string, o Occurrence) (bool, error) {
	kind := o.Kind
	if kind == "" {
		kind = "rss-guid"
	}
	res, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO identity_occurrences (account_id, podcast_id, guid, enclosure, pub_date, episode_id, node_id, kind)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, a.id, podcastID, o.GUID, o.Enclosure, nullInt(o.PubDate), o.EpisodeID, o.Node, kind)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ClaimOccurrence makes o's node the witness of an occurrence already
// recorded when its witness is a podcast's identity node (the creating op a
// server fetch records its evidence with) and o's node is none. A fetch's
// evidence is not always named by an archived entry, so a rebuild may lose
// its row and record the occurrence with a later observation instead: an
// archived witness wins, and the witness is the same either way.
func (a *AccountTx) ClaimOccurrence(podcastID string, o Occurrence) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE identity_occurrences SET node_id = ?
		WHERE account_id = ? AND podcast_id = ? AND guid = ? AND enclosure = ? AND coalesce(pub_date, -1) = coalesce(?, -1)
		AND node_id IN (SELECT creating_op FROM podcasts WHERE account_id = ?)
		AND NOT EXISTS (SELECT 1 FROM podcasts WHERE account_id = ? AND creating_op = ?)`,
		o.Node, a.id, podcastID, o.GUID, o.Enclosure, nullInt(o.PubDate), a.id, a.id, o.Node)
	return err
}

// SetOccurrenceEpisode sets the episode of an occurrence recorded without one.
func (a *AccountTx) SetOccurrenceEpisode(podcastID string, o Occurrence, episodeID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE identity_occurrences SET episode_id = ? WHERE account_id = ? AND podcast_id = ? AND guid = ?
		AND enclosure = ? AND coalesce(pub_date, -1) = coalesce(?, -1) AND episode_id = ''`,
		episodeID, a.id, podcastID, o.GUID, o.Enclosure, nullInt(o.PubDate))
	return err
}

// Occurrences returns a podcast's evidence in the order it was recorded.
func (a *AccountTx) Occurrences(podcastID string) ([]Occurrence, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT `+occurrenceColumns+` FROM identity_occurrences
		WHERE account_id = ? AND podcast_id = ? ORDER BY rowid`, a.id, podcastID)
	if err != nil {
		return nil, err
	}
	return scanOccurrences(rows)
}

// OccurrencesOfGUID returns a podcast's occurrences of one GUID, in the order
// they were recorded.
func (a *AccountTx) OccurrencesOfGUID(podcastID, guid string) ([]Occurrence, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT `+occurrenceColumns+` FROM identity_occurrences
		WHERE account_id = ? AND podcast_id = ? AND guid = ? ORDER BY rowid`, a.id, podcastID, guid)
	if err != nil {
		return nil, err
	}
	return scanOccurrences(rows)
}

// HasOccurrence reports whether a podcast already holds this occurrence.
func (a *AccountTx) HasOccurrence(podcastID string, o Occurrence) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM identity_occurrences WHERE account_id = ? AND podcast_id = ? AND guid = ? AND enclosure = ?
		AND coalesce(pub_date, -1) = coalesce(?, -1)`, a.id, podcastID, o.GUID, o.Enclosure, nullInt(o.PubDate)).Scan(&n)
	return n > 0, err
}

// EvidenceOfEpisode returns every podcast's occurrences recorded for an
// episode, by podcast.
func (a *AccountTx) EvidenceOfEpisode(episodeID string) ([]Occurrence, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT `+occurrenceColumns+` FROM identity_occurrences
		WHERE account_id = ? AND episode_id = ? ORDER BY rowid`, a.id, episodeID)
	if err != nil {
		return nil, err
	}
	return scanOccurrences(rows)
}

// RepointEvidence moves every podcast's evidence of one episode to another
// (an episode merge).
func (a *AccountTx) RepointEvidence(from, into string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE identity_occurrences SET episode_id = ? WHERE account_id = ? AND episode_id = ?`, into, a.id, from)
	return err
}

// LookupAlias returns the episode a server-decided alias maps to ("" if none).
func (a *AccountTx) LookupAlias(podcastID, kind, value string) (string, error) {
	var e string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT episode_id FROM identity_aliases WHERE account_id = ? AND podcast_id = ? AND kind = ? AND value = ?`,
		a.id, podcastID, kind, value).Scan(&e)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return e, err
}

// SetLookupAlias records or replaces a podcast's lookup alias for a key.
func (a *AccountTx) SetLookupAlias(podcastID string, x LookupAliasRow) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO identity_aliases (account_id, podcast_id, kind, value, episode_id, node_id, src)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (account_id, podcast_id, kind, value) DO UPDATE SET episode_id = excluded.episode_id,
		node_id = excluded.node_id, src = excluded.src`, a.id, podcastID, x.Kind, x.Value, x.EpisodeID, x.Node, x.Src)
	return err
}

// LookupAliasOf returns a podcast's lookup alias for a key, if any.
func (a *AccountTx) LookupAliasOf(podcastID, kind, value string) (LookupAliasRow, bool, error) {
	x := LookupAliasRow{Kind: kind, Value: value}
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT episode_id, node_id, src FROM identity_aliases WHERE account_id = ? AND podcast_id = ? AND kind = ? AND value = ?`,
		a.id, podcastID, kind, value).Scan(&x.EpisodeID, &x.Node, &x.Src)
	if errors.Is(err, sql.ErrNoRows) {
		return LookupAliasRow{}, false, nil
	}
	return x, err == nil, err
}

// LookupAliasRow is one lookup alias: its key, episode, the node that made
// it and the merged episode it came from ("" for a GUID change).
type LookupAliasRow struct {
	Kind, Value, EpisodeID, Node, Src string
}

// LookupAliases returns a podcast's lookup aliases, by kind and value.
func (a *AccountTx) LookupAliases(podcastID string) ([]LookupAliasRow, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT kind, value, episode_id, node_id, src FROM identity_aliases WHERE account_id = ? AND podcast_id = ?
		ORDER BY kind, value`, a.id, podcastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LookupAliasRow
	for rows.Next() {
		var x LookupAliasRow
		if err := rows.Scan(&x.Kind, &x.Value, &x.EpisodeID, &x.Node, &x.Src); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// Release is one release group of a disputed GUID: the result of its
// episode.occurrence_mapping decision (PROTOCOL §9.7).
type Release struct {
	GUID, EpisodeID string
	PubDate         int64
	Enclosures      []string // normalized, first first
	Original        bool     // the release keeping the original ID
	Node            string   // the mapping decision
	// Additions are the release's later enclosures, each its own decision,
	// in their rank order (ReleaseAdditions; the caller attaches them).
	Additions []string
}

// Lists reports whether the release lists an enclosure: one of its own or an
// addition.
func (r Release) Lists(u string) bool {
	return slices.Contains(r.Enclosures, u) || slices.Contains(r.Additions, u)
}

// ReleaseAddition is an enclosure added to a disputed GUID's release by an
// episode.occurrence_mapping decision of its own.
type ReleaseAddition struct {
	EpisodeID, Enclosure, Node string
}

// ReleaseAdditions returns a disputed GUID's enclosure additions in the rank
// order of their decisions (epoch, seq, decision_id).
func (a *AccountTx) ReleaseAdditions(podcastID, guid string) ([]ReleaseAddition, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT r.episode_id, r.enclosure, r.node_id FROM identity_release_additions r
		LEFT JOIN decisions d ON d.account_id = r.account_id AND d.decision_id = r.node_id
		WHERE r.account_id = ? AND r.podcast_id = ? AND r.guid = ? ORDER BY d.epoch, d.seq, d.decision_id, r.rowid`, a.id, podcastID, guid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReleaseAddition
	for rows.Next() {
		var x ReleaseAddition
		if err := rows.Scan(&x.EpisodeID, &x.Enclosure, &x.Node); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// AddReleaseAddition records an enclosure addition of a disputed GUID's
// release.
func (a *AccountTx) AddReleaseAddition(podcastID, guid string, x ReleaseAddition) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO identity_release_additions (account_id, podcast_id, guid, episode_id, enclosure,
		node_id) VALUES (?, ?, ?, ?, ?, ?)`, a.id, podcastID, guid, x.EpisodeID, x.Enclosure, x.Node)
	return err
}

// Releases returns a disputed GUID's releases in the rank order of their
// occurrence_mapping decisions (epoch, seq, decision_id): the order they
// were decided, whatever order a merge or a rebuild recorded them in.
func (a *AccountTx) Releases(podcastID, guid string) ([]Release, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT r.guid, r.episode_id, r.pub_date, r.enclosures, r.original, r.node_id FROM identity_releases r
		LEFT JOIN decisions d ON d.account_id = r.account_id AND d.decision_id = r.node_id
		WHERE r.account_id = ? AND r.podcast_id = ? AND r.guid = ? ORDER BY d.epoch, d.seq, d.decision_id, r.rowid`, a.id, podcastID, guid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Release
	for rows.Next() {
		var r Release
		var encs string
		var original int
		if err := rows.Scan(&r.GUID, &r.EpisodeID, &r.PubDate, &encs, &original, &r.Node); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(encs), &r.Enclosures); err != nil {
			return nil, err
		}
		r.Original = original != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// AddRelease records a release of a disputed GUID.
func (a *AccountTx) AddRelease(podcastID string, r Release) error {
	encs, err := json.Marshal(r.Enclosures)
	if err != nil {
		return err
	}
	if r.Enclosures == nil {
		encs = []byte("[]")
	}
	first := ""
	if len(r.Enclosures) > 0 {
		first = r.Enclosures[0]
	}
	original := 0
	if r.Original {
		original = 1
	}
	_, err = a.w.tx.ExecContext(a.w.ctx, `INSERT OR IGNORE INTO identity_releases (account_id, podcast_id, guid, episode_id, pub_date, first_enclosure, node_id,
		enclosures, original) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, a.id, podcastID, r.GUID, r.EpisodeID, r.PubDate, first, r.Node, string(encs), original)
	return err
}

// GUIDDisputed reports whether a podcast's GUID has release groups.
func (a *AccountTx) GUIDDisputed(podcastID, guid string) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM identity_releases WHERE account_id = ? AND podcast_id = ? AND guid = ?`,
		a.id, podcastID, guid).Scan(&n)
	return n > 0, err
}

// DisputedGUIDs returns a podcast's GUIDs with release groups, sorted.
func (a *AccountTx) DisputedGUIDs(podcastID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT DISTINCT guid FROM identity_releases WHERE account_id = ? AND podcast_id = ? ORDER BY guid`,
		a.id, podcastID)
}

// MergeRoot returns the root survivor of a merged episode, or the episode itself.
func (a *AccountTx) MergeRoot(episodeID string) (string, error) {
	var into string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT into_episode FROM episode_merges WHERE account_id = ? AND from_episode = ?`,
		a.id, episodeID).Scan(&into)
	if errors.Is(err, sql.ErrNoRows) {
		return episodeID, nil
	}
	return into, err
}

// EpisodeMerge is a merged episode's redirect.
type EpisodeMerge struct {
	From, Into string
	Node       string
	PodcastID  string // the survivor's podcast when it merged ("" if not recorded)
}

// MergeOf returns an episode's merge, if it was merged.
func (a *AccountTx) MergeOf(episodeID string) (EpisodeMerge, bool, error) {
	m := EpisodeMerge{From: episodeID}
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT into_episode, node_id, podcast_id FROM episode_merges WHERE account_id = ? AND from_episode = ?`,
		a.id, episodeID).Scan(&m.Into, &m.Node, &m.PodcastID)
	if errors.Is(err, sql.ErrNoRows) {
		return EpisodeMerge{}, false, nil
	}
	return m, err == nil, err
}

// MergedInto returns the episodes whose root survivor is an episode, sorted.
func (a *AccountTx) MergedInto(episodeID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT from_episode FROM episode_merges WHERE account_id = ? AND into_episode = ? ORDER BY from_episode`,
		a.id, episodeID)
}

// AddMerge records a merge into a root survivor and repoints every earlier
// merge into the merged episode, so redirects stay one step.
func (a *AccountTx) AddMerge(from, into, node, podcastID string) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE episode_merges SET into_episode = ? WHERE account_id = ? AND into_episode = ?`,
		into, a.id, from); err != nil {
		return err
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO episode_merges (account_id, from_episode, into_episode, node_id, podcast_id) VALUES (?, ?, ?, ?, ?)`,
		a.id, from, into, node, podcastID)
	return err
}

// PodcastMerge is a merged podcast's routing.
type PodcastMerge struct {
	From, Into                     string
	FromGeneration, IntoGeneration string
	Node                           string
}

// PodcastMergeOf returns a podcast's merge, if it was merged, with Into the
// end of its merge chain (the podcast its records redirect to, PROTOCOL §9.7).
func (a *AccountTx) PodcastMergeOf(podcastID string) (PodcastMerge, bool, error) {
	m, merged, err := a.PodcastMergeStep(podcastID)
	if err != nil || !merged {
		return m, merged, err
	}
	seen := map[string]bool{podcastID: true}
	for !seen[m.Into] {
		seen[m.Into] = true
		next, more, err := a.PodcastMergeStep(m.Into)
		if err != nil {
			return m, merged, err
		}
		if !more {
			break
		}
		m.Into = next.Into
	}
	return m, true, nil
}

// PodcastMergeStep returns a podcast's own merge, if it was merged: the
// podcast it was merged into and its generation routing.
func (a *AccountTx) PodcastMergeStep(podcastID string) (PodcastMerge, bool, error) {
	m := PodcastMerge{From: podcastID}
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT into_podcast, from_generation, into_generation, node_id FROM podcast_merges
		WHERE account_id = ? AND from_podcast = ?`, a.id, podcastID).Scan(&m.Into, &m.FromGeneration, &m.IntoGeneration, &m.Node)
	if errors.Is(err, sql.ErrNoRows) {
		return PodcastMerge{}, false, nil
	}
	return m, err == nil, err
}

// PodcastsMergedInto returns the podcasts merged directly into a podcast.
func (a *AccountTx) PodcastsMergedInto(podcastID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT from_podcast FROM podcast_merges WHERE account_id = ? AND into_podcast = ? ORDER BY from_podcast`,
		a.id, podcastID)
}

// AddPodcastMerge records a podcast merge.
func (a *AccountTx) AddPodcastMerge(m PodcastMerge) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO podcast_merges (account_id, from_podcast, into_podcast, from_generation, into_generation, node_id)
		VALUES (?, ?, ?, ?, ?, ?)`, a.id, m.From, m.Into, m.FromGeneration, m.IntoGeneration, m.Node)
	return err
}

func nullInt(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// Contribution returns a node's effect on a scope, if it contributes.
func (a *AccountTx) Contribution(scope, nodeID string) (string, bool, error) {
	var e string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT effect FROM contributions WHERE account_id = ? AND scope = ? AND node_id = ?`,
		a.id, scope, nodeID).Scan(&e)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return e, err == nil, err
}

// RemoveContribution deletes a node's contribution to a scope.
func (a *AccountTx) RemoveContribution(scope, nodeID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM contributions WHERE account_id = ? AND scope = ? AND node_id = ?`, a.id, scope, nodeID)
	return err
}

// GenerationEpisodeScopes returns the episode scopes of one generation that
// hold contributions, sorted.
func (a *AccountTx) GenerationEpisodeScopes(generationID string) ([]string, error) {
	low := "episode:" + generationID + ":"
	high := "episode:" + generationID + ";"
	return queryStrings(a.w.ctx, a.w.tx, `SELECT DISTINCT scope FROM contributions WHERE account_id = ? AND scope > ? AND scope < ? ORDER BY scope`,
		a.id, low, high)
}

// RescopeListening moves listening facts and sessions of the given nodes from
// one episode scope to another (a merge or an occurrence mapping, PROTOCOL §9.7).
func (a *AccountTx) RescopeListening(fromScope, toScope string, nodes []string) error {
	for _, n := range nodes {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE listening_facts SET scope = ? WHERE account_id = ? AND scope = ? AND node_id = ?`,
			toScope, a.id, fromScope, n); err != nil {
			return err
		}
		if _, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE listening_sessions SET scope = ? WHERE account_id = ? AND scope = ? AND start_node = ?`,
			toScope, a.id, fromScope, n); err != nil {
			return err
		}
	}
	return nil
}

// DeleteCatalogItem removes an episode's catalog item (merged away).
func (a *AccountTx) DeleteCatalogItem(episodeID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM catalog_items WHERE account_id = ? AND episode_id = ?`, a.id, episodeID)
	return err
}

// MoveCatalogItems moves a podcast's catalog items to another (a podcast merge).
func (a *AccountTx) MoveCatalogItems(from, into string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE catalog_items SET podcast_id = ? WHERE account_id = ? AND podcast_id = ?`, into, a.id, from)
	return err
}

// ServerSeenEpisodes returns a podcast's catalog items the server's fetches saw.
func (a *AccountTx) ServerSeenEpisodes(podcastID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT episode_id FROM catalog_items WHERE account_id = ? AND podcast_id = ? AND server_seen_node IS NOT NULL
		ORDER BY episode_id`, a.id, podcastID)
}

// PodcastCreatingOp returns the fact that created a podcast.
func (a *AccountTx) PodcastCreatingOp(podcastID string) (string, error) {
	var op string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT creating_op FROM podcasts WHERE account_id = ? AND podcast_id = ?`, a.id, podcastID).Scan(&op)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return op, err
}

// EpisodesOfPodcast returns a podcast's registered episodes.
func (a *AccountTx) EpisodesOfPodcast(podcastID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT episode_id FROM episodes WHERE account_id = ? AND podcast_id = ? ORDER BY episode_id`, a.id, podcastID)
}

// RehomeGeneration moves a generation to another podcast (a podcast merge
// whose survivor had no live generation, PROTOCOL §9.7).
func (a *AccountTx) RehomeGeneration(generationID, podcastID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE generations SET podcast_id = ? WHERE account_id = ? AND generation_id = ?`, podcastID, a.id, generationID)
	return err
}

// MoveAliases moves every feed URL alias of one podcast to another.
func (a *AccountTx) MoveAliases(from, into string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE podcast_aliases SET podcast_id = ? WHERE account_id = ? AND podcast_id = ?`, into, a.id, from)
	return err
}

// MergedPodcasts returns every merged-away podcast of the account, sorted.
func (a *AccountTx) MergedPodcasts() ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT from_podcast FROM podcast_merges WHERE account_id = ? ORDER BY from_podcast`, a.id)
}

// GenerationsJoinedInto returns the source generations a podcast merge joined
// into a live generation class.
func (a *AccountTx) GenerationsJoinedInto(generationID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT from_generation FROM podcast_merges WHERE account_id = ? AND into_generation = ?
		AND from_generation != into_generation ORDER BY from_generation`, a.id, generationID)
}

// EpisodeScopes returns an episode's scopes that hold contributions: its
// catalog scope and its scope in every generation, sorted.
func (a *AccountTx) EpisodeScopes(episodeID string) ([]string, error) {
	return queryStrings(a.w.ctx, a.w.tx, `SELECT DISTINCT scope FROM contributions WHERE account_id = ? AND (scope = 'item:' || ?2
		OR (substr(scope, 1, 8) = 'episode:' AND substr(scope, -length(?2) - 1) = ':' || ?2)) ORDER BY scope`, a.id, episodeID)
}

// RebindEpisode moves a registered episode to another podcast (a podcast
// merge carrying an episode it cannot map).
func (a *AccountTx) RebindEpisode(episodeID, podcastID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE episodes SET podcast_id = ? WHERE account_id = ? AND episode_id = ?`, podcastID, a.id, episodeID)
	return err
}
