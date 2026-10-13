-- The current queue (PROTOCOL §7.6): one row per episode
-- whose live generation has it in the queue, maintained by episode refolds;
-- sort results read it.
CREATE TABLE queue_entries (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	episode_id TEXT NOT NULL,
	generation_id TEXT NOT NULL,
	queue_key TEXT NOT NULL,    -- contract-v1 key "q1:<n>:<m>"
	position_id TEXT NOT NULL,  -- a UUID
	membership_by TEXT NOT NULL, -- the nodes that determine membership and position
	position_by TEXT NOT NULL,
	PRIMARY KEY (account_id, episode_id)
) STRICT;

-- The queue frontier: queue membership/position nodes (queue facts, starts with
-- a queue add, ends and finalizations carrying a queue removal, queue decisions)
-- that no other such node descends from. A queue decision depends on it.
CREATE TABLE queue_frontier (
	account_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, node_id),
	FOREIGN KEY (account_id, node_id) REFERENCES causal_nodes (account_id, node_id) ON DELETE CASCADE
) STRICT;
