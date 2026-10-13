-- Identity reconciliation (PROTOCOL §9.7).

-- Occurrence evidence: every distinct (GUID, enclosure, publication date) an
-- episode of a podcast was observed with. guid and enclosure are ''
-- when absent; pub_date is NULL when absent.
CREATE TABLE identity_occurrences (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	guid TEXT NOT NULL,
	enclosure TEXT NOT NULL,
	pub_date INTEGER,
	episode_id TEXT NOT NULL,
	node_id TEXT NOT NULL,          -- the first observation of it
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;

CREATE UNIQUE INDEX identity_occurrences_key ON identity_occurrences (account_id, podcast_id, guid, enclosure, coalesce(pub_date, -1));
CREATE INDEX identity_occurrences_guid ON identity_occurrences (account_id, podcast_id, guid);

-- Lookup aliases set by server decisions only: a GUID-change match or a merge.
CREATE TABLE identity_aliases (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK (kind IN ('guid', 'enclosure')),
	value TEXT NOT NULL,
	episode_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, podcast_id, kind, value)
) STRICT;

-- Disputed GUIDs and enclosures, not used for lookup.
CREATE TABLE identity_disputes (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK (kind IN ('guid', 'enclosure')),
	value TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, podcast_id, kind, value)
) STRICT;

-- A disputed GUID's releases: each with its episode and the
-- publication date that defines it (NULL never defines a release).
CREATE TABLE identity_releases (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	guid TEXT NOT NULL,
	episode_id TEXT NOT NULL,
	pub_date INTEGER NOT NULL,
	first_enclosure TEXT NOT NULL,
	node_id TEXT NOT NULL,           -- the episode.occurrence_mapping decision (or the dispute's for the original)
	PRIMARY KEY (account_id, podcast_id, guid, episode_id)
) STRICT;

-- Episode merges, always to the root survivor.
CREATE TABLE episode_merges (
	account_id TEXT NOT NULL,
	from_episode TEXT NOT NULL,
	into_episode TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, from_episode)
) STRICT;

CREATE INDEX episode_merges_into ON episode_merges (account_id, into_episode);

-- Podcast merges: the source podcast and its joined live generation.
CREATE TABLE podcast_merges (
	account_id TEXT NOT NULL,
	from_podcast TEXT NOT NULL,
	into_podcast TEXT NOT NULL,
	from_generation TEXT NOT NULL,   -- the source's live generation, '' if none
	into_generation TEXT NOT NULL,   -- the survivor's live generation class it joined, '' if none
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, from_podcast)
) STRICT;
