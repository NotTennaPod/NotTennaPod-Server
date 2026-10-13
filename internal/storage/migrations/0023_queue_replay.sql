-- The queue replay (PROTOCOL §7.6): the replay reads the
-- account's replay steps and the episode folds' membership events in the
-- deterministic order, from its earliest affected step on.
--
-- queue_steps: every node that is a replay step (a placement, a move, a sort,
-- a sorting policy, an import's queue), with the decision that resolves it
-- (a play's server-resolved add, a filing's admission; '' for none).
CREATE TABLE queue_steps (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	node_id TEXT NOT NULL,
	decision_id TEXT NOT NULL DEFAULT '',
	-- A fact's key writers (the replay's selector): the JSON array of
	-- the indexes of its key-carrying members whose own checks passed ('' or
	-- [] for none: a refused fact, a session-ineligible start).
	writes TEXT NOT NULL DEFAULT '',
	-- A move's members whose own checks failed for another reason than a
	-- closed generation (the JSON array of their indexes): the replay leaves
	-- them out of the move (a closed generation's member only ghosts).
	refused TEXT NOT NULL DEFAULT '',
	-- Its members admitted as of a closed generation (generation_closed,
	-- live or as its receipt decided): a move only ghosts them.
	closed TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (account_id, node_id)
) STRICT;

-- queue_events: an episode scope's membership effects that took effect at
-- their place, by the node that made them, as the scope's last fold found
-- them (in_queue: in or out from there on).
CREATE TABLE queue_events (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	scope TEXT NOT NULL,
	node_id TEXT NOT NULL,
	in_queue INTEGER NOT NULL CHECK (in_queue IN (0, 1)),
	PRIMARY KEY (account_id, scope, node_id)
) STRICT;
CREATE INDEX queue_events_by_node ON queue_events (account_id, node_id);

-- The node whose acceptance registered an episode: of two aliases a merge
-- joins, the one registered first gives the survivor's recorded sort values
-- when the survivor has none of its own (by registration rank, then episode
-- ID).
ALTER TABLE episodes ADD COLUMN registered_by TEXT NOT NULL DEFAULT '';

-- Keys are a materialization: an in-member's
-- published key is its latest key write in rank order. The replay derives
-- the placement, move, filing, resolved-add and import writes from its steps
-- (a placement writes only for the members it actually places); queue_keys
-- holds the layouts' writes, each on its exact (episode, generation), the
-- latest per scope by its layout's rank. The former queue_entries (the current
-- queue by its folded keys) is retired.
DROP TABLE queue_entries;

CREATE TABLE queue_keys (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	scope TEXT NOT NULL,         -- the (episode, generation) the layout lists, never routed to a survivor
	queue_key TEXT NOT NULL,
	position_id TEXT NOT NULL,
	node_id TEXT NOT NULL,       -- the layout
	epoch INTEGER NOT NULL,      -- its rank (epoch, seq, index, node_id)
	seq INTEGER NOT NULL,
	idx INTEGER NOT NULL,
	PRIMARY KEY (account_id, scope)
) STRICT;

-- The account's causal frontier: the nodes no other node depends on (a
-- layout depends on it and names it in its inputs).
CREATE TABLE causal_frontier (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	node_id TEXT NOT NULL,
	PRIMARY KEY (account_id, node_id)
) STRICT;
-- A registration's own podcast.resolution and podcast.alias stay off it and
-- leave their dependencies on it (shared vectors leave them implicit).
INSERT INTO causal_frontier (account_id, node_id) SELECT n.account_id, n.node_id FROM causal_nodes n
	WHERE NOT EXISTS (SELECT 1 FROM decisions d WHERE d.account_id = n.account_id AND d.decision_id = n.node_id
			AND d.decision_type IN ('podcast.resolution', 'podcast.alias'))
		AND NOT EXISTS (SELECT 1 FROM causal_edges e WHERE e.account_id = n.account_id AND e.dependency_id = n.node_id
			AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.account_id = e.account_id AND d.decision_id = e.node_id
				AND d.decision_type IN ('podcast.resolution', 'podcast.alias')));
