-- The read model's change capture (docs/ARCHITECTURE.md, "Read-model change
-- capture"). The browser's views of an account read its records, which of its
-- episodes are registered, its catalog items' dates and insertion order, and
-- its podcasts' creating nodes and their causal ranks: its view inputs.
--
-- view_clock: per account, the tick every change to its view inputs raises,
-- and the generation every bulk rewrite of them raises (an import's or a
-- staged rebuild's publication, a rebuild, an account's deletion, a repair
-- of these triggers). An account without a row is at tick 0, generation 0.
--
-- view_changes: per account, each entity (account, podcast or episode) whose
-- view inputs changed, at the tick of its latest change; view_changes_by_tick
-- reads those above a tick in order.
--
-- The view_capture_ triggers stamp every change in the transaction that makes
-- it; a statement that changes no view input stamps nothing. A bulk rewrite
-- drops them for the rest of its transaction and creates them again before it
-- commits, from the same text (storage.captureTriggers).

CREATE TABLE view_clock (
	account_id TEXT PRIMARY KEY REFERENCES accounts (account_id) ON DELETE CASCADE,
	tick INTEGER NOT NULL,
	generation INTEGER NOT NULL DEFAULT 0
) STRICT, WITHOUT ROWID;

CREATE TABLE view_changes (
	account_id TEXT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
	entity TEXT NOT NULL,
	entity_id TEXT NOT NULL,
	tick INTEGER NOT NULL,
	PRIMARY KEY (account_id, entity, entity_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX view_changes_by_tick ON view_changes (account_id, tick, entity, entity_id);

-- A causal node's rank is the rank of the podcasts it created.
CREATE INDEX podcasts_by_creating_op ON podcasts (account_id, creating_op);

CREATE TRIGGER view_capture_records_insert AFTER INSERT ON records
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, NEW.entity, NEW.entity_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_records_update AFTER UPDATE ON records
	WHEN NEW.value IS NOT OLD.value OR NEW.provenance IS NOT OLD.provenance OR NEW.field_group IS NOT OLD.field_group
		OR NEW.entity IS NOT OLD.entity OR NEW.entity_id IS NOT OLD.entity_id OR NEW.account_id IS NOT OLD.account_id
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, NEW.entity, NEW.entity_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
	INSERT INTO view_clock (account_id, tick) SELECT OLD.account_id, 1 WHERE OLD.account_id IS NOT NEW.account_id
		ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, OLD.entity, OLD.entity_id, tick FROM view_clock
		WHERE account_id = OLD.account_id AND (OLD.account_id IS NOT NEW.account_id OR OLD.entity IS NOT NEW.entity OR OLD.entity_id IS NOT NEW.entity_id)
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_records_delete AFTER DELETE ON records
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (OLD.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, OLD.entity, OLD.entity_id, tick FROM view_clock
		WHERE account_id = OLD.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_catalog_insert AFTER INSERT ON catalog_items
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, 'episode', NEW.episode_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_catalog_update AFTER UPDATE ON catalog_items
	WHEN NEW.pub_date IS NOT OLD.pub_date OR NEW.episode_id IS NOT OLD.episode_id OR NEW.rowid IS NOT OLD.rowid
		OR NEW.account_id IS NOT OLD.account_id
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, 'episode', NEW.episode_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
	INSERT INTO view_clock (account_id, tick) SELECT OLD.account_id, 1 WHERE OLD.account_id IS NOT NEW.account_id
		ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, 'episode', OLD.episode_id, tick FROM view_clock
		WHERE account_id = OLD.account_id AND (OLD.account_id IS NOT NEW.account_id OR OLD.episode_id IS NOT NEW.episode_id)
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_catalog_delete AFTER DELETE ON catalog_items
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (OLD.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, 'episode', OLD.episode_id, tick FROM view_clock
		WHERE account_id = OLD.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_episodes_insert AFTER INSERT ON episodes
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, 'episode', NEW.episode_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_episodes_update AFTER UPDATE ON episodes
	WHEN NEW.episode_id IS NOT OLD.episode_id OR NEW.account_id IS NOT OLD.account_id
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, 'episode', NEW.episode_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
	INSERT INTO view_clock (account_id, tick) SELECT OLD.account_id, 1 WHERE OLD.account_id IS NOT NEW.account_id
		ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, 'episode', OLD.episode_id, tick FROM view_clock
		WHERE account_id = OLD.account_id AND (OLD.account_id IS NOT NEW.account_id OR OLD.episode_id IS NOT NEW.episode_id)
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_episodes_delete AFTER DELETE ON episodes
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (OLD.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, 'episode', OLD.episode_id, tick FROM view_clock
		WHERE account_id = OLD.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_podcasts_insert AFTER INSERT ON podcasts
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, 'podcast', NEW.podcast_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_podcasts_update AFTER UPDATE ON podcasts
	WHEN NEW.podcast_id IS NOT OLD.podcast_id OR NEW.creating_op IS NOT OLD.creating_op OR NEW.account_id IS NOT OLD.account_id
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT NEW.account_id, 'podcast', NEW.podcast_id, tick FROM view_clock
		WHERE account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
	INSERT INTO view_clock (account_id, tick) SELECT OLD.account_id, 1 WHERE OLD.account_id IS NOT NEW.account_id
		ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, 'podcast', OLD.podcast_id, tick FROM view_clock
		WHERE account_id = OLD.account_id AND (OLD.account_id IS NOT NEW.account_id OR OLD.podcast_id IS NOT NEW.podcast_id)
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_podcasts_delete AFTER DELETE ON podcasts
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (OLD.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT OLD.account_id, 'podcast', OLD.podcast_id, tick FROM view_clock
		WHERE account_id = OLD.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_node_order_insert AFTER INSERT ON node_order
	WHEN EXISTS (SELECT 1 FROM podcasts WHERE account_id = NEW.account_id AND creating_op = NEW.node_id)
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT p.account_id, 'podcast', p.podcast_id, c.tick
		FROM podcasts p, view_clock c WHERE p.account_id = NEW.account_id AND p.creating_op = NEW.node_id AND c.account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_node_order_update AFTER UPDATE ON node_order
	WHEN (NEW.rank IS NOT OLD.rank OR NEW.node_id IS NOT OLD.node_id OR NEW.account_id IS NOT OLD.account_id)
		AND (EXISTS (SELECT 1 FROM podcasts WHERE account_id = NEW.account_id AND creating_op = NEW.node_id)
			OR EXISTS (SELECT 1 FROM podcasts WHERE account_id = OLD.account_id AND creating_op = OLD.node_id))
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (NEW.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT p.account_id, 'podcast', p.podcast_id, c.tick
		FROM podcasts p, view_clock c WHERE p.account_id = NEW.account_id AND p.creating_op = NEW.node_id AND c.account_id = NEW.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
	INSERT INTO view_clock (account_id, tick) SELECT OLD.account_id, 1 WHERE OLD.account_id IS NOT NEW.account_id
		ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT p.account_id, 'podcast', p.podcast_id, c.tick
		FROM podcasts p, view_clock c WHERE p.account_id = OLD.account_id AND p.creating_op = OLD.node_id AND c.account_id = OLD.account_id
			AND (OLD.account_id IS NOT NEW.account_id OR OLD.node_id IS NOT NEW.node_id)
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;

CREATE TRIGGER view_capture_node_order_delete AFTER DELETE ON node_order
	WHEN EXISTS (SELECT 1 FROM podcasts WHERE account_id = OLD.account_id AND creating_op = OLD.node_id)
BEGIN
	INSERT INTO view_clock (account_id, tick) VALUES (OLD.account_id, 1) ON CONFLICT (account_id) DO UPDATE SET tick = tick + 1;
	INSERT INTO view_changes (account_id, entity, entity_id, tick) SELECT p.account_id, 'podcast', p.podcast_id, c.tick
		FROM podcasts p, view_clock c WHERE p.account_id = OLD.account_id AND p.creating_op = OLD.node_id AND c.account_id = OLD.account_id
		ON CONFLICT (account_id, entity, entity_id) DO UPDATE SET tick = excluded.tick;
END;
