-- How long the server's own fetches have found a podcast's catalog unchanged
-- (PROTOCOL §9.6): quiet_since is the start time (ms) of the fetch since which
-- no successful fetch has changed the catalog. NULL until the first successful
-- fetch after this migration, so every podcast starts with the observation
-- period. Its schedule widens from the refresh interval to 6 hours after 30
-- quiet days and to 24 hours after 90.
ALTER TABLE refresh_schedule ADD COLUMN quiet_since INTEGER;

-- The newest effective date among a podcast's items the server's fetches saw
-- (the date rule of its own discoveries, PROTOCOL §9.5) is one seek: the index
-- holds only those items, so the items only devices or an import saw are
-- never walked.
CREATE INDEX catalog_items_server_newest ON catalog_items (account_id, podcast_id, pub_date)
	WHERE server_seen_node IS NOT NULL;
