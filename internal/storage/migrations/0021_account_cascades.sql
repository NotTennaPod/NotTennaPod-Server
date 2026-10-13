-- Every per-account table belongs to its account: deleting an account
-- deletes its rows, as PROTOCOL §5.1 says (deletion deletes the server
-- library). Migrations 0012, 0013 and
-- 0019 declared no owner for these seven tables, so their rows outlived a
-- deleted account: lookup aliases, disputes, releases and their enclosure
-- additions (signed URLs among them), episode and podcast merges, and
-- population markers. Each table is made again with the account cascade,
-- keeping only the rows of accounts that still exist. Columns, keys and
-- indexes are unchanged.

CREATE TABLE generation_population_owned (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	generation_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, generation_id)
) STRICT;
INSERT INTO generation_population_owned SELECT account_id, generation_id, node_id FROM generation_population
	WHERE account_id IN (SELECT account_id FROM accounts);
DROP TABLE generation_population;
ALTER TABLE generation_population_owned RENAME TO generation_population;

CREATE TABLE identity_aliases_owned (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	podcast_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK (kind IN ('guid', 'enclosure')),
	value TEXT NOT NULL,
	episode_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	src TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (account_id, podcast_id, kind, value)
) STRICT;
INSERT INTO identity_aliases_owned SELECT account_id, podcast_id, kind, value, episode_id, node_id, src FROM identity_aliases
	WHERE account_id IN (SELECT account_id FROM accounts);
DROP TABLE identity_aliases;
ALTER TABLE identity_aliases_owned RENAME TO identity_aliases;

CREATE TABLE identity_disputes_owned (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	podcast_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK (kind IN ('guid', 'enclosure')),
	value TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, podcast_id, kind, value)
) STRICT;
INSERT INTO identity_disputes_owned SELECT account_id, podcast_id, kind, value, node_id FROM identity_disputes
	WHERE account_id IN (SELECT account_id FROM accounts);
DROP TABLE identity_disputes;
ALTER TABLE identity_disputes_owned RENAME TO identity_disputes;

CREATE TABLE identity_releases_owned (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	podcast_id TEXT NOT NULL,
	guid TEXT NOT NULL,
	episode_id TEXT NOT NULL,
	pub_date INTEGER NOT NULL,
	first_enclosure TEXT NOT NULL,
	node_id TEXT NOT NULL,           -- the episode.occurrence_mapping decision (or the dispute's for the original)
	enclosures TEXT NOT NULL DEFAULT '[]',
	original INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (account_id, podcast_id, guid, episode_id)
) STRICT;
INSERT INTO identity_releases_owned
	SELECT account_id, podcast_id, guid, episode_id, pub_date, first_enclosure, node_id, enclosures, original FROM identity_releases
	WHERE account_id IN (SELECT account_id FROM accounts);
DROP TABLE identity_releases;
ALTER TABLE identity_releases_owned RENAME TO identity_releases;

CREATE TABLE identity_release_additions_owned (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	podcast_id TEXT NOT NULL,
	guid TEXT NOT NULL,
	episode_id TEXT NOT NULL,        -- the release's episode, as the decision names it
	enclosure TEXT NOT NULL,         -- normalized
	node_id TEXT NOT NULL,           -- the addition's episode.occurrence_mapping decision
	PRIMARY KEY (account_id, podcast_id, guid, episode_id, enclosure)
) STRICT;
INSERT INTO identity_release_additions_owned SELECT account_id, podcast_id, guid, episode_id, enclosure, node_id FROM identity_release_additions
	WHERE account_id IN (SELECT account_id FROM accounts);
DROP TABLE identity_release_additions;
ALTER TABLE identity_release_additions_owned RENAME TO identity_release_additions;

CREATE TABLE episode_merges_owned (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	from_episode TEXT NOT NULL,
	into_episode TEXT NOT NULL,
	node_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (account_id, from_episode)
) STRICT;
INSERT INTO episode_merges_owned SELECT account_id, from_episode, into_episode, node_id, podcast_id FROM episode_merges
	WHERE account_id IN (SELECT account_id FROM accounts);
DROP TABLE episode_merges;
ALTER TABLE episode_merges_owned RENAME TO episode_merges;
CREATE INDEX episode_merges_into ON episode_merges (account_id, into_episode);

CREATE TABLE podcast_merges_owned (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	from_podcast TEXT NOT NULL,
	into_podcast TEXT NOT NULL,
	from_generation TEXT NOT NULL,   -- the source's live generation, '' if none
	into_generation TEXT NOT NULL,   -- the survivor's live generation class it joined, '' if none
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, from_podcast)
) STRICT;
INSERT INTO podcast_merges_owned SELECT account_id, from_podcast, into_podcast, from_generation, into_generation, node_id FROM podcast_merges
	WHERE account_id IN (SELECT account_id FROM accounts);
DROP TABLE podcast_merges;
ALTER TABLE podcast_merges_owned RENAME TO podcast_merges;
