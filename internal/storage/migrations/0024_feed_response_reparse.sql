-- A stored feed response's re-parse outcome, in the clear beside the sealed
-- response so diagnostics list it without opening any. Only the
-- reason's category and counts, never publisher content: reparse_failure is
-- '' or 'timeout' (the re-parse ran past its deadline reparse_timeouts times
-- for parser version reparse_parser, and the response counts as unparseable
-- for it); while it is '', reparse_timeouts counts the timeouts so far for
-- reparse_parser. A new response has none.
--
-- The table is made again so that the sealed bytes come last: SQLite reads a
-- row's columns in order, and a column added after a large value is reached
-- only through that value's overflow pages. Rows, keys and the cascade are
-- unchanged; the index lists the failures alone.

CREATE TABLE feed_responses_reparse (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	stored_at INTEGER NOT NULL,
	reparse_failure TEXT NOT NULL DEFAULT '' CHECK (reparse_failure IN ('', 'timeout')),
	reparse_timeouts INTEGER NOT NULL DEFAULT 0 CHECK (reparse_timeouts >= 0),
	reparse_parser TEXT NOT NULL DEFAULT '',
	sealed BLOB NOT NULL, -- feed.StoredResponse, gzip-compressed, AES-256-GCM under the key in secrets.key
	PRIMARY KEY (account_id, podcast_id),
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;
INSERT INTO feed_responses_reparse (account_id, podcast_id, stored_at, sealed)
	SELECT account_id, podcast_id, stored_at, sealed FROM feed_responses;
DROP TABLE feed_responses;
ALTER TABLE feed_responses_reparse RENAME TO feed_responses;
CREATE INDEX feed_responses_reparse_failures ON feed_responses (account_id, podcast_id, reparse_failure, reparse_timeouts, reparse_parser)
	WHERE reparse_failure != '';
