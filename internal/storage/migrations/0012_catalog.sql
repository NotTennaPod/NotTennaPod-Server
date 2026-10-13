-- Catalog, discovery and server refresh (PROTOCOL §9.4 to §9.6).

-- The episodes the account's catalog knows from feeds; registering an
-- episode by any other fact (a listening placeholder) adds none. server_* is the
-- server's own fetch history, against which it discovers.
CREATE TABLE catalog_items (
	account_id TEXT NOT NULL,
	episode_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	first_node TEXT NOT NULL,          -- the first observation
	playable_node TEXT,                -- the first observation with an enclosure
	catalog_only_node TEXT,            -- the first observation without one, before it was playable
	pub_date INTEGER NOT NULL,         -- effective: the item's date, or its first observation's time
	server_seen_node TEXT,             -- the server's first fetch that saw it
	server_catalog_only INTEGER NOT NULL DEFAULT 0, -- that fetch saw it without an enclosure
	PRIMARY KEY (account_id, episode_id),
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX catalog_items_podcast ON catalog_items (account_id, podcast_id);

-- A generation's first population by the server: the catalog.refresh part
-- that recorded it.
CREATE TABLE generation_population (
	account_id TEXT NOT NULL,
	generation_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, generation_id)
) STRICT;

-- The server's refresh schedule and live status per account and podcast;
-- not part of the change log.
CREATE TABLE refresh_schedule (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	next_at INTEGER NOT NULL,             -- ms
	failures INTEGER NOT NULL DEFAULT 0,  -- consecutive
	not_before INTEGER NOT NULL DEFAULT 0, -- Retry-After
	last_attempt INTEGER,
	last_success INTEGER,
	last_error TEXT,                      -- canonical JSON {label, code, detail}, or NULL
	status_published_at INTEGER NOT NULL DEFAULT 0,
	status_key TEXT NOT NULL DEFAULT '',  -- the published status's outcome class and error
	PRIMARY KEY (account_id, podcast_id),
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX refresh_schedule_due ON refresh_schedule (next_at);
