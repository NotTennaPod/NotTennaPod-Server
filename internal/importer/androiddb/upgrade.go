package androiddb

import (
	"context"
	"database/sql"
	"fmt"
)

// upgrade ports Android's DBUpgrader.upgrade (storage/database/.../DBUpgrader.java,
// tag 3.12.1), run only on the private copy, so an older backup reaches the
// 3.12.1 shape before it is read by column name. It follows NotTennaPod-iOS's
// port (AndroidDatabaseUpgrader.swift) statement for statement; the one
// Android-only step (MediaMetadataRetriever on downloaded files, ≤ 15) leaves
// those rows at -1, as Android does for a missing file.
func upgrade(ctx context.Context, tx *sql.Tx, old int64) error {
	run := func(statements ...string) error {
		for _, s := range statements {
			if _, err := tx.ExecContext(ctx, s); err != nil {
				return err
			}
		}
		return nil
	}
	steps := []struct {
		applies    bool
		statements []string
	}{
		{old <= 1, []string{"ALTER TABLE Feeds ADD COLUMN type TEXT"}},
		{old <= 2, []string{"ALTER TABLE SimpleChapters ADD COLUMN link TEXT"}},
		{old <= 3, []string{"ALTER TABLE FeedItems ADD COLUMN item_identifier TEXT"}},
		{old <= 4, []string{"ALTER TABLE Feeds ADD COLUMN feed_identifier TEXT"}},
		{old <= 5, []string{"ALTER TABLE DownloadLog ADD COLUMN reason_detailed TEXT", "ALTER TABLE DownloadLog ADD COLUMN title TEXT"}},
		{old <= 6, []string{"ALTER TABLE SimpleChapters ADD COLUMN type INTEGER"}},
		{old <= 7, []string{"ALTER TABLE FeedMedia ADD COLUMN playback_completion_date INTEGER"}},
		// DBUpgrader:51–75 selects every item and sets FeedMedia.feeditem for its
		// legacy media id in cursor (rowid) order: the last item wins.
		{old <= 8, []string{"ALTER TABLE FeedMedia ADD COLUMN feeditem INTEGER",
			"UPDATE FeedMedia SET feeditem = (SELECT id FROM FeedItems WHERE FeedItems.media = FeedMedia.id ORDER BY rowid DESC LIMIT 1) WHERE EXISTS (SELECT 1 FROM FeedItems WHERE FeedItems.media = FeedMedia.id)"}},
		{old <= 9, []string{"ALTER TABLE Feeds ADD COLUMN auto_download INTEGER DEFAULT 1"}},
		{old <= 10, []string{"ALTER TABLE Feeds ADD COLUMN flattr_status INTEGER", "ALTER TABLE FeedItems ADD COLUMN flattr_status INTEGER",
			"ALTER TABLE FeedMedia ADD COLUMN played_duration INTEGER"}},
		{old <= 11, []string{"ALTER TABLE Feeds ADD COLUMN username TEXT", "ALTER TABLE Feeds ADD COLUMN password TEXT",
			"ALTER TABLE FeedItems ADD COLUMN image INTEGER"}},
		{old <= 12, []string{"ALTER TABLE Feeds ADD COLUMN is_paged INTEGER DEFAULT 0", "ALTER TABLE Feeds ADD COLUMN next_page_link TEXT"}},
		{old <= 13, []string{"DELETE FROM SimpleChapters WHERE id NOT IN (SELECT MIN(id) as id FROM SimpleChapters GROUP BY title,start,feeditem,link,type)"}},
		{old <= 14, []string{"ALTER TABLE FeedItems ADD COLUMN auto_download INTEGER",
			"UPDATE FeedItems SET auto_download = (SELECT auto_download FROM Feeds WHERE Feeds.id = FeedItems.feed)",
			"ALTER TABLE Feeds ADD COLUMN hide TEXT",
			"ALTER TABLE Feeds ADD COLUMN last_update_failed INTEGER DEFAULT 0",
			"CREATE INDEX FeedItems_feed ON FeedItems (feed)",
			"CREATE INDEX FeedMedia_feeditem ON FeedMedia (feeditem)",
			"CREATE INDEX Queue_feeditem ON Queue (feeditem)",
			"CREATE INDEX SimpleChapters_feeditem ON SimpleChapters (feeditem)"}},
		{old <= 15, []string{"ALTER TABLE FeedMedia ADD COLUMN has_embedded_picture INTEGER DEFAULT -1",
			"UPDATE FeedMedia SET has_embedded_picture=0 WHERE downloaded=0"}},
		{old <= 16, []string{"UPDATE FeedItems SET read=-1 WHERE id IN (SELECT FeedItems.id FROM FeedItems INNER JOIN FeedMedia ON FeedItems.id=FeedMedia.feeditem LEFT OUTER JOIN Queue ON FeedItems.id=Queue.feeditem WHERE FeedItems.read = 0 AND FeedMedia.downloaded = 0 AND FeedMedia.position = 0 AND Queue.id IS NULL)"}},
		{old <= 17, []string{"ALTER TABLE Feeds ADD COLUMN auto_delete_action INTEGER DEFAULT 0"}},
		{old < 1_030_005, []string{"UPDATE FeedItems SET auto_download=0 WHERE (read=1 OR id IN (SELECT feeditem FROM FeedMedia WHERE position>0 OR downloaded=1)) AND id NOT IN (SELECT feeditem FROM Queue)"}},
		{old < 1_040_001, []string{"CREATE TABLE Favorites(id INTEGER PRIMARY KEY,feeditem INTEGER,feed INTEGER)"}},
		{old < 1_040_002, []string{"ALTER TABLE FeedMedia ADD COLUMN last_played_time INTEGER DEFAULT 0"}},
		{old < 1_040_013, []string{"CREATE INDEX FeedItems_pubDate ON FeedItems (pubDate)", "CREATE INDEX FeedItems_read ON FeedItems (read)"}},
		{old < 1_050_003, filterInversion()},
		{old < 1_050_004, []string{"UPDATE Feeds SET last_update=NULL"}},
		{old < 1_060_200, []string{"ALTER TABLE Feeds ADD COLUMN custom_title TEXT"}},
		{old < 1_060_596, []string{"ALTER TABLE Feeds ADD COLUMN image_url TEXT", "ALTER TABLE FeedItems ADD COLUMN image_url TEXT",
			"UPDATE FeedItems SET image_url  = ( SELECT download_url FROM FeedImages WHERE FeedImages.id = FeedItems.image)",
			"UPDATE Feeds SET image_url = ( SELECT download_url FROM FeedImages WHERE FeedImages.id = Feeds.image)",
			"DROP TABLE FeedImages"}},
		{old < 1_070_400, []string{"ALTER TABLE Feeds ADD COLUMN feed_playback_speed REAL DEFAULT -1.0"}},
		{old < 1_070_401, []string{"ALTER TABLE Feeds ADD COLUMN sort_order TEXT"}},
		{old < 1_090_000, []string{"ALTER TABLE Feeds ADD COLUMN feed_volume_adaption INTEGER DEFAULT 0",
			"ALTER TABLE SimpleChapters ADD COLUMN image_url TEXT DEFAULT NULL"}},
		{old < 1_090_001, []string{"ALTER TABLE Feeds ADD COLUMN feed_skip_intro INTEGER DEFAULT 0;",
			"ALTER TABLE Feeds ADD COLUMN feed_skip_ending INTEGER DEFAULT 0;"}},
		{old < 2_020_000, []string{"ALTER TABLE Feeds ADD COLUMN episode_notification INTEGER DEFAULT 0;"}},
		{old < 2_030_000, []string{"UPDATE FeedItems SET description = content_encoded, content_encoded = NULL WHERE length(description) < length(content_encoded)",
			"UPDATE FeedItems SET content_encoded = NULL",
			"ALTER TABLE Feeds ADD COLUMN tags TEXT;"}},
		{old < 2_050_000, []string{"ALTER TABLE Feeds ADD COLUMN minimal_duration_filter INTEGER DEFAULT -1"}},
		{old < 2_060_000, []string{"ALTER TABLE FeedItems ADD COLUMN podcastindex_chapter_url TEXT"}},
		{old < 3_010_000, []string{"ALTER TABLE Feeds ADD COLUMN new_episodes_action INTEGER DEFAULT 0"}},
		{old < 3_040_000, []string{"ALTER TABLE Feeds ADD COLUMN feed_skip_silence INTEGER DEFAULT 1"}},
		{old < 3_050_000, []string{"ALTER TABLE Feeds ADD COLUMN state INTEGER DEFAULT 0",
			"ALTER TABLE FeedItems ADD COLUMN podcastindex_transcript_url TEXT",
			"ALTER TABLE FeedItems ADD COLUMN podcastindex_transcript_type TEXT"}},
		{old < 3_080_000, []string{"ALTER TABLE FeedItems ADD COLUMN social_interact_url TEXT",
			"DELETE FROM Favorites WHERE feeditem NOT IN (SELECT id FROM FeedItems)"}},
	}
	for _, step := range steps {
		if !step.applies {
			continue
		}
		if err := run(step.statements...); err != nil {
			return err
		}
	}
	return run(fmt.Sprintf("PRAGMA user_version = %d", Version))
}

// filterInversion is DBUpgrader:219–265's token inversion of the episode filter,
// in the source's exact replace order, then the filter and keep-updated columns.
func filterInversion() []string {
	var out []string
	for _, r := range [][2]string{{"unplayed", "noplay"}, {"not_queued", "noqueue"}, {"not_downloaded", "nodl"},
		{"played", "unplayed"}, {"queued", "not_queued"}, {"downloaded", "not_downloaded"},
		{"noplay", "played"}, {"noqueue", "queued"}, {"nodl", "downloaded"}, {"paused", "unplayed"}} {
		out = append(out, fmt.Sprintf("UPDATE Feeds\nSET hide = replace(hide, '%s', '%s')", r[0], r[1]))
	}
	return append(out, "ALTER TABLE Feeds ADD COLUMN include_filter TEXT DEFAULT ''",
		"ALTER TABLE Feeds ADD COLUMN exclude_filter TEXT DEFAULT ''",
		"ALTER TABLE Feeds ADD COLUMN keep_updated INTEGER DEFAULT 1")
}
