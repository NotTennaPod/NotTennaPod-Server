-- Session continuations (PROTOCOL §8.5): a session's facts
-- belong to (session_id, effective author). The start's author holds the root
-- continuation; a descendant author's continuation follows its parent's up to its
-- base ordinal.

CREATE TABLE session_continuations (
	account_id TEXT NOT NULL,
	session_id TEXT NOT NULL,
	author TEXT NOT NULL,
	parent TEXT NOT NULL,         -- "" for the root continuation
	base INTEGER NOT NULL,        -- the parent's last ordinal it shares (0 for the root)
	sealed_by TEXT,
	finalized_by TEXT,
	PRIMARY KEY (account_id, session_id, author),
	FOREIGN KEY (account_id, session_id) REFERENCES listening_sessions (account_id, session_id) ON DELETE CASCADE
) STRICT;

INSERT INTO session_continuations (account_id, session_id, author, parent, base, sealed_by, finalized_by)
	SELECT account_id, session_id, author_id, '', 0, sealed_by, finalized_by FROM listening_sessions;

CREATE TABLE session_facts_v2 (
	account_id TEXT NOT NULL,
	session_id TEXT NOT NULL,
	author TEXT NOT NULL,
	ordinal INTEGER NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, session_id, author, ordinal),
	FOREIGN KEY (account_id, session_id, author) REFERENCES session_continuations (account_id, session_id, author) ON DELETE CASCADE
) STRICT;

INSERT INTO session_facts_v2 (account_id, session_id, author, ordinal, node_id)
	SELECT f.account_id, f.session_id, s.author_id, f.ordinal, f.node_id
	FROM session_facts f JOIN listening_sessions s ON s.account_id = f.account_id AND s.session_id = f.session_id;

DROP TABLE session_facts;
ALTER TABLE session_facts_v2 RENAME TO session_facts;

-- A statistics reset cuts each continuation separately.
ALTER TABLE listening_facts ADD COLUMN author TEXT;
UPDATE listening_facts SET author = (SELECT s.author_id FROM listening_sessions s
	WHERE s.account_id = listening_facts.account_id AND s.session_id = listening_facts.session_id)
	WHERE session_id IS NOT NULL;
