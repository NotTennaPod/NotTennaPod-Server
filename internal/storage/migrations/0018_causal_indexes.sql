-- Indexes on the causal graph.

-- A node's dependents: foreign-key cascades and deferred checks look
-- children up by dependency, which otherwise scans every edge of the server
-- (clearing one account for a rebuild cost time in proportion to every
-- other account's edges).
CREATE INDEX causal_edges_dependency ON causal_edges (account_id, dependency_id);

-- A new node's place in the causal order (insertRank): the nodes whose
-- priority (normalized time, ID) is greater, usually none, found without
-- reading every later node.
CREATE INDEX causal_nodes_priority ON causal_nodes (account_id, normalized_time, node_id);

-- A provisional generation joined into the effective one (PROTOCOL §6.11,
-- §9.7): a restored server opened it for a registration whose recovered
-- original resolution joined an existing generation. References to it route
-- to the effective generation; it is no generation of its podcast's own.
CREATE TABLE generation_joins (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	generation_id TEXT NOT NULL,
	into_generation TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, generation_id)
) STRICT;

-- A fact's causal node's publication position: the commit from
-- which the fact is a causal node. It is the archive entry's publication
-- position, except for a fact whose original receipt admitted it after a
-- provisional rejection: its node is published by the commit that replaced
-- the receipt. FrontierAt filters by it.
ALTER TABLE facts ADD COLUMN node_pub_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE facts ADD COLUMN node_pub_seq INTEGER NOT NULL DEFAULT 0;
UPDATE facts SET node_pub_epoch = pub_epoch, node_pub_seq = pub_seq;
