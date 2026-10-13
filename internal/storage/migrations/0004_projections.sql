-- Projection engine, catalog skeleton, records and secrets
-- (PROTOCOL §7).

CREATE TABLE node_order (
	account_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	rank INTEGER NOT NULL,
	PRIMARY KEY (account_id, node_id),
	FOREIGN KEY (account_id, node_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE
) STRICT;

CREATE UNIQUE INDEX node_order_rank ON node_order (account_id, rank);

-- One row per (scope, contributing node); effect is canonical JSON.
CREATE TABLE contributions (
	account_id TEXT NOT NULL,
	scope TEXT NOT NULL, -- "episode:<G>:<E>", "podcast:<P>", "account"
	node_id TEXT NOT NULL,
	effect TEXT NOT NULL,
	PRIMARY KEY (account_id, scope, node_id),
	FOREIGN KEY (account_id, node_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE podcasts (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	podcast_id TEXT NOT NULL,
	creating_op TEXT NOT NULL,
	PRIMARY KEY (account_id, podcast_id)
) STRICT;

CREATE TABLE podcast_aliases (
	account_id TEXT NOT NULL,
	normalized_url TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	raw_url TEXT NOT NULL,
	source TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, normalized_url),
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE generations (
	account_id TEXT NOT NULL,
	generation_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	creating_op TEXT NOT NULL,
	predecessor TEXT,
	closed_by TEXT,
	PRIMARY KEY (account_id, generation_id),
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX generations_by_podcast ON generations (account_id, podcast_id);

CREATE TABLE episodes (
	account_id TEXT NOT NULL,
	episode_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	occurrence TEXT NOT NULL, -- canonical JSON of the first evidence seen
	PRIMARY KEY (account_id, episode_id),
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;

-- Current live records (contract-v1 records): what snapshots serve.
CREATE TABLE records (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	entity TEXT NOT NULL,
	entity_id TEXT NOT NULL,
	field_group TEXT NOT NULL,
	value TEXT NOT NULL,      -- canonical JSON
	provenance TEXT NOT NULL, -- canonical JSON array
	PRIMARY KEY (account_id, entity, entity_id, field_group)
) STRICT;

CREATE TABLE secrets (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	secret_id TEXT NOT NULL,
	sealed BLOB NOT NULL, -- AES-256-GCM under the key in secrets.key; compared by decrypting, never by a stored hash
	created_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, secret_id)
) STRICT;
