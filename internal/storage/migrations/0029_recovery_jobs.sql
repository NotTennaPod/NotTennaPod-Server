-- Recovery upload jobs (PROTOCOL §6.11).
--
-- An upload whose derivation outlasts its request goes on as a job; the row
-- lets a restart resume it. A job deduplicates in-flight work only: it keeps
-- no result, and its publication deletes the row in the commit's
-- transaction. The request is sealed (it can hold feed credentials); only
-- its key, a SHA-256 of the request, is in the clear. The job's authority is
-- the submitting credential (its stored hash, never the secret), its kind,
-- the installation and the account's credentials version at submission; a
-- job that ran its own lost-timeline transition also has the credentials
-- version right after it (fence) and the timeline it made (fence_epoch, the
-- account's epoch then), written in the transition's transaction: that
-- exceptional authority holds only on that timeline.
CREATE TABLE recovery_jobs (
	account_id TEXT NOT NULL,
	recovery_id TEXT NOT NULL,
	request_key TEXT NOT NULL,
	install_id TEXT NOT NULL,
	state TEXT NOT NULL CHECK (state IN ('queued', 'running')),
	request BLOB NOT NULL,
	credential_hash BLOB NOT NULL,
	credential_kind TEXT NOT NULL,
	credentials_version INTEGER NOT NULL,
	transitioned INTEGER NOT NULL DEFAULT 0 CHECK (transitioned IN (0, 1)),
	fence INTEGER,
	fence_epoch INTEGER,
	attempts INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, recovery_id, request_key),
	FOREIGN KEY (account_id, install_id) REFERENCES installations (account_id, install_id) ON DELETE CASCADE
) STRICT;
