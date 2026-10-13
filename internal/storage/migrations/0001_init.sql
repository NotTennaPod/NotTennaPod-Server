-- Schema v1: accounts and their change logs (PROTOCOL §4.1, §6.2).

CREATE TABLE server_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
) STRICT;

CREATE TABLE accounts (
	account_id TEXT PRIMARY KEY,
	namespace TEXT NOT NULL UNIQUE,
	state TEXT NOT NULL CHECK (state IN ('active', 'disabled')),
	created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE account_logs (
	account_id TEXT PRIMARY KEY REFERENCES accounts (account_id) ON DELETE CASCADE,
	log_id TEXT NOT NULL UNIQUE,
	epoch INTEGER NOT NULL CHECK (epoch >= 1),
	head_seq INTEGER NOT NULL CHECK (head_seq >= 0),
	head_digest BLOB NOT NULL CHECK (length(head_digest) = 32)
) STRICT;

CREATE TABLE log_commits (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	seq INTEGER NOT NULL CHECK (seq >= 1),
	epoch INTEGER NOT NULL CHECK (epoch >= 1),
	digest BLOB NOT NULL CHECK (length(digest) = 32),
	commit_json BLOB NOT NULL,
	committed_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, seq)
) STRICT;
