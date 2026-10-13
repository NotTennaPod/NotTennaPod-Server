-- Fact archive, causal graph, transport slots, framed parts and receipt keys
-- (PROTOCOL §6).

ALTER TABLE installations ADD COLUMN consumed_seq INTEGER NOT NULL DEFAULT 0;

CREATE TABLE receipt_keys (
	key_id TEXT PRIMARY KEY,
	secret BLOB NOT NULL CHECK (length(secret) = 32),
	created_at INTEGER NOT NULL
) STRICT;

-- The immutable archive, admitted and rejected facts alike.
CREATE TABLE facts (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	op_id TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	fact BLOB NOT NULL,
	receipt BLOB NOT NULL,
	type TEXT NOT NULL,
	type_version INTEGER NOT NULL,
	outcome TEXT NOT NULL CHECK (outcome IN ('applied', 'superseded', 'not_applied', 'rejected')),
	frontier TEXT NOT NULL, -- canonical JSON array; meaningful for rejected facts
	epoch INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	PRIMARY KEY (account_id, op_id)
) STRICT;

CREATE INDEX facts_rejected ON facts (account_id, epoch, seq, op_id) WHERE outcome = 'rejected';

-- The causal graph of admitted facts and decisions.
CREATE TABLE causal_nodes (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	node_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK (kind IN ('fact', 'decision')),
	normalized_time INTEGER NOT NULL,
	PRIMARY KEY (account_id, node_id)
) STRICT;

CREATE TABLE causal_edges (
	account_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	dependency_id TEXT NOT NULL,
	PRIMARY KEY (account_id, node_id, dependency_id),
	FOREIGN KEY (account_id, node_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE,
	FOREIGN KEY (account_id, dependency_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE author_positions (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	author_id TEXT NOT NULL,
	author_seq INTEGER NOT NULL,
	op_id TEXT NOT NULL,
	PRIMARY KEY (account_id, author_id, author_seq)
) STRICT;

CREATE TABLE decisions (
	account_id TEXT NOT NULL,
	decision_id TEXT NOT NULL,
	decision_type TEXT NOT NULL,
	subject TEXT NOT NULL,
	epoch INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	normalized_time INTEGER NOT NULL,
	decision BLOB NOT NULL,
	PRIMARY KEY (account_id, decision_id),
	FOREIGN KEY (account_id, decision_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX decisions_subject ON decisions (account_id, decision_type, subject, epoch, seq, decision_id);

CREATE TABLE transport_slots (
	account_id TEXT NOT NULL,
	install_id TEXT NOT NULL,
	install_seq INTEGER NOT NULL CHECK (install_seq >= 1),
	op_id TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	result TEXT NOT NULL CHECK (result IN ('accepted', 'duplicate', 'op_id_conflict')),
	seq INTEGER NOT NULL,
	PRIMARY KEY (account_id, install_id, install_seq),
	FOREIGN KEY (account_id, install_id) REFERENCES installations (account_id, install_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX transport_slots_by_seq ON transport_slots (account_id, install_id, seq);

CREATE TABLE push_parts (
	account_id TEXT NOT NULL,
	install_id TEXT NOT NULL,
	install_seq INTEGER NOT NULL,
	part_index INTEGER NOT NULL CHECK (part_index >= 0),
	transaction_id TEXT NOT NULL,
	part_count INTEGER NOT NULL CHECK (part_count >= 1),
	content_hash TEXT NOT NULL,
	data BLOB NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, install_id, install_seq, part_index),
	FOREIGN KEY (account_id, install_id) REFERENCES installations (account_id, install_id) ON DELETE CASCADE
) STRICT;
