-- Recovery after a restore (PROTOCOL §6.11).

-- Receipt order is (epoch, seq, receipt_index, op_id): the
-- receipt's index, backfilled from the receipt (-1 for a receipt without
-- one), replaces rowid order.
ALTER TABLE facts ADD COLUMN receipt_index INTEGER NOT NULL DEFAULT -1;
UPDATE facts SET receipt_index = COALESCE(json_extract(CAST(receipt AS TEXT), '$.index'), -1) WHERE json_valid(CAST(receipt AS TEXT));
CREATE INDEX facts_receipt_order ON facts (account_id, epoch, seq, receipt_index, op_id);

-- The publication position: the (epoch, seq) of the commit that published an
-- archived fact or decision in the current log. A live entry is published by
-- the commit its receipt names; a recovered one keeps its original receipt
-- and is published by the commit that joins it. FrontierAt filters by it.
ALTER TABLE facts ADD COLUMN pub_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE facts ADD COLUMN pub_seq INTEGER NOT NULL DEFAULT 0;
UPDATE facts SET pub_epoch = epoch, pub_seq = seq;
ALTER TABLE decisions ADD COLUMN pub_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE decisions ADD COLUMN pub_seq INTEGER NOT NULL DEFAULT 0;
UPDATE decisions SET pub_epoch = epoch, pub_seq = seq;

-- Provisional receipts an uploaded original replaced, kept as
-- evidence: a later upload of one is known, never new evidence.
CREATE TABLE superseded_receipts (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	op_id TEXT NOT NULL,
	epoch INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	receipt_index INTEGER NOT NULL,
	receipt BLOB NOT NULL,
	replaced_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, op_id, epoch, seq, receipt_index)
) STRICT;

-- Recovery sessions: opened by a recovery snapshot, idempotent by
-- recovery_id; counts and problems (invalid receipts, conflicts) as
-- canonical JSON for diagnostics.
CREATE TABLE recoveries (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	recovery_id TEXT NOT NULL,
	install_id TEXT NOT NULL,
	started_at INTEGER NOT NULL,
	last_seen_at INTEGER NOT NULL,
	finished_at INTEGER,
	complete INTEGER CHECK (complete IN (0, 1)),
	counts TEXT NOT NULL DEFAULT '{}',
	problems TEXT NOT NULL DEFAULT '[]',
	PRIMARY KEY (account_id, recovery_id)
) STRICT;

CREATE INDEX recoveries_recent ON recoveries (last_seen_at);
