-- Listening sessions and cuts (PROTOCOL §8).

CREATE TABLE listening_sessions (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	session_id TEXT NOT NULL,
	scope TEXT NOT NULL,     -- the episode scope of its start
	author_id TEXT NOT NULL, -- the only author who may append to or finalize it
	start_node TEXT NOT NULL,
	sealed_by TEXT,          -- its end
	finalized_by TEXT,       -- its finalization
	PRIMARY KEY (account_id, session_id)
) STRICT;

-- A session's admitted start (ordinal 0), checkpoints and end, by ordinal.
CREATE TABLE session_facts (
	account_id TEXT NOT NULL,
	session_id TEXT NOT NULL,
	ordinal INTEGER NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, session_id, ordinal),
	FOREIGN KEY (account_id, session_id) REFERENCES listening_sessions (account_id, session_id) ON DELETE CASCADE
) STRICT;

-- Facts that cuts can cover, by scope: history entries (listening activities
-- and history marks) and session facts with an ordinal; also history removals,
-- which a restore names.
CREATE TABLE listening_facts (
	account_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	scope TEXT NOT NULL,
	history_entry INTEGER NOT NULL CHECK (history_entry IN (0, 1)),
	session_id TEXT,  -- with ordinal: a session fact a statistics reset can cut
	ordinal INTEGER,
	removal INTEGER NOT NULL CHECK (removal IN (0, 1)),
	restores TEXT,    -- for a history restore: the removal it cancels
	PRIMARY KEY (account_id, node_id),
	FOREIGN KEY (account_id, node_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX listening_facts_by_scope ON listening_facts (account_id, scope);
CREATE UNIQUE INDEX listening_facts_restores ON listening_facts (account_id, restores) WHERE restores IS NOT NULL;
