-- Recovery frames.
--
-- A recovery upload's framed fact is assembled in push_parts under a
-- negative slot. The slot is now allocated per framing transaction and found
-- by the full transaction ID (it was a CRC32 of it, so two transactions
-- could collide). The frame remembers the recovery that last used it and
-- when: finishing that recovery discards its incomplete frames, and a frame
-- unused for a day expires. Open frames are bounded per installation.
CREATE TABLE recovery_frames (
	account_id TEXT NOT NULL,
	install_id TEXT NOT NULL,
	transaction_id TEXT NOT NULL,
	slot INTEGER NOT NULL CHECK (slot < 0),
	recovery_id TEXT NOT NULL,
	used_at INTEGER NOT NULL,
	PRIMARY KEY (account_id, install_id, transaction_id),
	UNIQUE (account_id, install_id, slot),
	FOREIGN KEY (account_id, install_id) REFERENCES installations (account_id, install_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX recovery_frames_used ON recovery_frames (used_at);

-- A stored part's plaintext length: framing limits count plaintext, not the
-- sealed bytes (-1: stored before this column; its stored length is used).
ALTER TABLE push_parts ADD COLUMN size INTEGER NOT NULL DEFAULT -1;

-- Recovery parts stored under CRC32 slots have no frame row: they are
-- transient upload state, discarded (their uploads frame them again).
DELETE FROM push_parts WHERE install_seq < 0;
