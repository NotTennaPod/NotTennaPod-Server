-- Effective authors, derived branches and lineage (PROTOCOL §6.10).
-- author_positions now holds chain positions keyed by effective author, for every
-- archived fact with well-formed author coordinates, rejected facts included.

CREATE TABLE fact_authors (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	op_id TEXT NOT NULL,
	raw_author TEXT NOT NULL,
	author_seq INTEGER NOT NULL,
	effective_author TEXT NOT NULL,
	PRIMARY KEY (account_id, op_id)
) STRICT;

INSERT INTO fact_authors (account_id, op_id, raw_author, author_seq, effective_author)
	SELECT account_id, op_id, author_id, author_seq, author_id FROM author_positions;

-- author_lineage: an effective author's parent, for derived branches (via = the root
-- fact) and local-detection authors (via = their installation.fork fact). tip is
-- the last fact of the parent's writing history the two share (the root's
-- predecessor, or the fork's head; "" for none).
CREATE TABLE author_lineage (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	author TEXT NOT NULL,
	parent TEXT NOT NULL,
	via TEXT NOT NULL,
	tip TEXT NOT NULL,
	PRIMARY KEY (account_id, author)
) STRICT;
