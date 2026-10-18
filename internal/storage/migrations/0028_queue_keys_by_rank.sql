-- The layouts' key writes ranked after a given rank are one range seek
-- (PROTOCOL §7.6): a queue command's publication reads only the key writes
-- made since the last one it saw, never every layout write in the history.
CREATE INDEX queue_keys_by_rank ON queue_keys (account_id, epoch, seq, idx, node_id);
