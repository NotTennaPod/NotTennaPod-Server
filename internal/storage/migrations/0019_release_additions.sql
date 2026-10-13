-- A release's enclosure additions: a
-- later enclosure of a disputed GUID's release, observed on a dated
-- occurrence the release's date resolves to, is an
-- episode.occurrence_mapping decision of its own. The resolver and the
-- identity_rules record list a release's enclosures, then its additions in
-- the rank order of their decisions.
CREATE TABLE identity_release_additions (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	guid TEXT NOT NULL,
	episode_id TEXT NOT NULL,        -- the release's episode, as the decision names it
	enclosure TEXT NOT NULL,         -- normalized
	node_id TEXT NOT NULL,           -- the addition's episode.occurrence_mapping decision
	PRIMARY KEY (account_id, podcast_id, guid, episode_id, enclosure)
) STRICT;
