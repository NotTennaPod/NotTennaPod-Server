-- Identity reconciliation aligned with the independent reference (PROTOCOL
-- §9.7): evidence remembers its GUID kind (atom-id or rss-guid) and is
-- recorded before the episode it derives to is known ('' until then); a
-- release keeps its occurrence_mapping decision's enclosures (first first) and
-- whether it holds the original ID; a merge remembers the survivor's podcast
-- (where the merged episode's GUID becomes a lookup alias); a lookup alias
-- remembers the merged episode it came from ('' for a GUID change), which
-- breaks ties between two claims of one key.
ALTER TABLE identity_occurrences ADD COLUMN kind TEXT NOT NULL DEFAULT 'rss-guid';
ALTER TABLE identity_releases ADD COLUMN enclosures TEXT NOT NULL DEFAULT '[]';
ALTER TABLE identity_releases ADD COLUMN original INTEGER NOT NULL DEFAULT 0;
ALTER TABLE episode_merges ADD COLUMN podcast_id TEXT NOT NULL DEFAULT '';
ALTER TABLE identity_aliases ADD COLUMN src TEXT NOT NULL DEFAULT '';
