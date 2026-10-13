-- The listening index holds a node once per scope:
-- a grouped import's rows that a later dispute splits between releases are
-- one decision with history or statistics in each release's scope, and
-- cuts (history removals and clears, statistics resets) find what they cover
-- by scope. The key becomes (account, node, scope); every node so far has
-- one row, which is copied unchanged.
CREATE TABLE listening_facts_scoped (
	account_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	scope TEXT NOT NULL,
	history_entry INTEGER NOT NULL CHECK (history_entry IN (0, 1)),
	session_id TEXT,  -- with ordinal: a session fact a statistics reset can cut
	ordinal INTEGER,
	removal INTEGER NOT NULL CHECK (removal IN (0, 1)),
	restores TEXT,    -- for a history restore: the removal it cancels
	author TEXT,
	PRIMARY KEY (account_id, node_id, scope),
	FOREIGN KEY (account_id, node_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE
) STRICT;

INSERT INTO listening_facts_scoped (account_id, node_id, scope, history_entry, session_id, ordinal, removal, restores, author)
	SELECT account_id, node_id, scope, history_entry, session_id, ordinal, removal, restores, author FROM listening_facts;

DROP TABLE listening_facts;

ALTER TABLE listening_facts_scoped RENAME TO listening_facts;

CREATE INDEX listening_facts_by_scope ON listening_facts (account_id, scope);
CREATE UNIQUE INDEX listening_facts_restores ON listening_facts (account_id, restores) WHERE restores IS NOT NULL;
