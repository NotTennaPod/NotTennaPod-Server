-- Stored feed responses (PROTOCOL §9.3, §9.6): the
-- last successful response per account and podcast, so a parser upgrade can
-- re-parse without refetching. Sealed like secrets: a private feed's body can
-- hold credentials.

CREATE TABLE feed_responses (
	account_id TEXT NOT NULL,
	podcast_id TEXT NOT NULL,
	sealed BLOB NOT NULL, -- feed.StoredResponse, gzip-compressed, AES-256-GCM under the key in secrets.key
	stored_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, podcast_id),
	FOREIGN KEY (account_id, podcast_id) REFERENCES podcasts (account_id, podcast_id) ON DELETE CASCADE
) STRICT;
