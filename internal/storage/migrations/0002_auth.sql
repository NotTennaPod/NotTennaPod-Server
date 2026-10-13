-- Accounts' sign-in credentials, installations and authentication tokens
-- (PROTOCOL §5).

CREATE TABLE account_logins (
	account_id TEXT PRIMARY KEY REFERENCES accounts (account_id) ON DELETE CASCADE,
	username TEXT NOT NULL,
	username_key TEXT NOT NULL UNIQUE,
	password_hash TEXT NOT NULL,
	credentials_version INTEGER NOT NULL CHECK (credentials_version >= 1),
	is_admin INTEGER NOT NULL CHECK (is_admin IN (0, 1)),
	credentials_changed_at INTEGER NOT NULL
) STRICT;

CREATE TABLE installations (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	install_id TEXT NOT NULL,
	instance_nonce TEXT NOT NULL,
	name TEXT NOT NULL,
	platform TEXT NOT NULL CHECK (platform IN ('ios', 'android', 'web')),
	app_version TEXT NOT NULL,
	capabilities TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	last_seen_at INTEGER NOT NULL,
	revoked_at INTEGER,
	created_from TEXT,
	superseded_by TEXT,
	pending_predecessor TEXT,
	PRIMARY KEY (account_id, install_id)
) STRICT;

CREATE TABLE auth_tokens (
	token_hash BLOB PRIMARY KEY CHECK (length(token_hash) = 32),
	kind TEXT NOT NULL CHECK (kind IN ('device', 'web')),
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	install_id TEXT,
	created_at INTEGER NOT NULL,
	last_used_at INTEGER NOT NULL,
	expires_at INTEGER,
	FOREIGN KEY (account_id, install_id) REFERENCES installations (account_id, install_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX auth_tokens_installation ON auth_tokens (account_id, install_id);
