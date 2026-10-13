-- Library imports (PROTOCOL §11.5). Import IDs and fingerprints are kept
-- permanently.
CREATE TABLE imports (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	import_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK (kind IN ('android_db', 'opml')),
	fingerprint TEXT NOT NULL,       -- SHA-256 of the file, lowercase hex
	size INTEGER NOT NULL CHECK (size >= 0),
	file_name TEXT NOT NULL,
	status TEXT NOT NULL CHECK (status IN ('uploading', 'processing', 'published', 'failed')),
	reason TEXT,                     -- a failure's reason
	epoch INTEGER NOT NULL,          -- the account's epoch when reserved
	incarnation TEXT NOT NULL,       -- the server incarnation when reserved
	progress INTEGER NOT NULL DEFAULT 0, -- podcasts applied in staging
	result TEXT,                     -- the published result (canonical JSON)
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, import_id)
) STRICT;

-- At most one live import per account: reserved (uploading, processing) or
-- published. A failed import releases the reservation.
CREATE UNIQUE INDEX imports_live ON imports (account_id) WHERE status IN ('uploading', 'processing', 'published');
CREATE INDEX imports_by_fingerprint ON imports (account_id, fingerprint);
