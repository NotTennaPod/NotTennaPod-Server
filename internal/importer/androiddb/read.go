// Package androiddb reads an Android AntennaPod 3.12.1 database backup
// (Antennapod.db; NotTennaPod-iOS exports the same format) for the server-side
// import (PROTOCOL §11.5). It follows NotTennaPod-iOS's reader
// (AndroidDatabaseRestore.swift): the file is checked and, if older, upgraded
// on a private copy, then read by column name with Android cursor semantics
// (NULL integers read as 0).
package androiddb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Version is Android AntennaPod 3.12.1's database version (PodDBAdapter.VERSION).
const Version = 3110000

// LocalFeedPrefix marks a local-folder feed's download URL.
const LocalFeedPrefix = "antennapod_local:"

// ErrNewer reports a backup made by a newer AntennaPod (Android's
// import_no_downgrade).
var ErrNewer = errors.New("androiddb: the backup was made by a newer version of AntennaPod")

// InvalidError reports a file that is not a usable backup. Message follows the
// SQLite or Android wording a person would see on Android.
type InvalidError struct{ Message string }

func (e *InvalidError) Error() string { return "androiddb: " + e.Message }

func invalid(format string, args ...any) error {
	return &InvalidError{Message: fmt.Sprintf(format, args...)}
}

// Library is a validated, fully read backup.
type Library struct {
	SourceVersion int64
	Feeds         []Feed  // by id
	Queue         []int64 // episode IDs in queue order
	// Skipped counts rows Android would never show, or values it cannot read, by reason.
	Skipped map[string]int
}

// Feed is one Feeds row with its episodes.
type Feed struct {
	ID                int64
	Title             *string
	CustomTitle       *string
	URL               string // download_url
	Type              *string
	Link              *string
	Description       *string
	Author            *string
	ImageURL          *string
	Language          *string
	PaymentLink       *string
	FeedIdentifier    *string
	LastUpdate        *string
	IsPaged           bool
	NextPageLink      *string
	Username          *string
	Password          *string
	KeepUpdated       bool
	State             int32 // 0 subscribed, 1 not subscribed (preview), 2 archived
	NewEpisodesAction int32 // 0 global, 1 inbox, 2 nothing, 3 queue
	Settings          Settings
	Episodes          []Episode
}

// Settings are a feed's device-only settings (PROTOCOL §1.4: never synced
// or imported), read as Android's FeedCursor and FeedPreferencesCursor read
// them, so the import result can name the ones a person changed.
type Settings struct {
	AutoDownload        int32   // FeedPreferences.AutoDownloadSetting: 0 never, 1 global, 2 always
	AutoDeleteAction    int32   // 0 global, 1 always, 2 never
	PlaybackSpeed       float64 // -1: the global speed
	SkipSilence         int32   // 0 off, 1 global, 2 aggressive
	VolumeAdaption      int32   // 0 off, 1 to 5 a reduction or boost
	IncludeFilter       *string // the automatic-download episode filter
	ExcludeFilter       *string
	MinimalDuration     int32 // seconds; -1: none
	SkipIntro           int32 // seconds
	SkipEnding          int32 // seconds
	EpisodeNotification bool
	Tags                *string // U+001E-separated; empty: the root tag only
	SortOrder           *string // SortOrder code; NULL: the global order
	Hide                *string // the episode list's FeedItemFilter
}

// Local reports a local-folder feed, which the import skips.
func (f Feed) Local() bool { return strings.HasPrefix(f.URL, LocalFeedPrefix) }

// Episode is one FeedItems row joined with its lowest-id FeedMedia row.
type Episode struct {
	ID             int64
	FeedID         int64
	GUID           *string // item_identifier
	Title          *string
	PubDate        int64 // milliseconds
	Read           int32 // -1 new, 0 unplayed, 1 played
	Link           *string
	Description    *string
	ImageURL       *string
	PaymentLink    *string
	ChapterURL     *string
	TranscriptURL  *string
	TranscriptType *string
	SocialURL      *string
	Favorite       bool
	// AutoDownload is FeedItems.auto_download > 0 (FeedItemCursor): false when
	// a person stopped this episode from downloading automatically. Device-only.
	AutoDownload bool
	// Chapters are the stored chapters (SimpleChapters) when
	// has_simple_chapters is set, in id order; nil when the flag is not set
	// (whatever rows exist) or no rows are stored, as Android loads them.
	Chapters []Chapter
	Media    *Media
}

// Chapter is a SimpleChapters row.
type Chapter struct {
	Start    int64 // milliseconds
	Title    *string
	Link     *string
	ImageURL *string
}

// Media is a FeedMedia row.
type Media struct {
	ID             int64
	URL            *string
	Size           int64
	MimeType       *string
	Duration       int32 // milliseconds
	Position       int32 // milliseconds
	PlayedDuration int64 // milliseconds; NULL reads as 0
	LastPlayedTime int64 // milliseconds
	CompletionDate int64 // milliseconds; 0 means not in history
	Downloaded     bool
}

// requiredColumns are the 3.12.1 columns every table must have after upgrade
// (the same list as NotTennaPod-iOS's reader).
var requiredColumns = map[string][]string{
	"Feeds": {"id", "title", "custom_title", "download_url", "downloaded", "link", "description", "payment_link",
		"last_update", "language", "author", "image_url", "type", "feed_identifier", "auto_download", "username",
		"password", "include_filter", "exclude_filter", "minimal_duration_filter", "keep_updated", "is_paged",
		"next_page_link", "hide", "sort_order", "last_update_failed", "auto_delete_action", "feed_playback_speed",
		"feed_skip_silence", "feed_volume_adaption", "tags", "feed_skip_intro", "feed_skip_ending",
		"episode_notification", "state", "new_episodes_action"},
	"FeedItems": {"id", "title", "pubDate", "read", "link", "description", "payment_link", "feed", "has_simple_chapters",
		"item_identifier", "image_url", "auto_download", "podcastindex_chapter_url",
		"podcastindex_transcript_type", "podcastindex_transcript_url", "social_interact_url"},
	"FeedMedia": {"id", "duration", "file_url", "download_url", "downloaded", "position", "filesize", "mime_type",
		"playback_completion_date", "feeditem", "played_duration", "has_embedded_picture", "last_played_time"},
	"DownloadLog":    {"id", "feedfile", "feedfile_type", "reason", "successful", "completion_date", "reason_detailed", "title"},
	"Queue":          {"id", "feeditem", "feed"},
	"SimpleChapters": {"id", "title", "start", "feeditem", "link", "image_url"},
	"Favorites":      {"id", "feeditem", "feed"},
}

// Read copies the backup at path into staging (a private directory the caller
// owns and removes), validates and, if older, upgrades the copy, then reads it.
// The file at path is never modified.
func Read(ctx context.Context, path, staging string) (*Library, error) {
	if err := private.RealDir(staging); err != nil {
		return nil, err
	}
	copyPath := filepath.Join(staging, "Antennapod.db")
	if err := copyFile(path, copyPath); err != nil {
		return nil, err
	}
	header := make([]byte, 16)
	if f, err := os.Open(copyPath); err != nil {
		return nil, err
	} else {
		n, _ := io.ReadFull(f, header)
		f.Close()
		header = header[:n]
	}
	// SQLITE_NOTADB's text, as Android reports it.
	if !bytes.Equal(header, []byte("SQLite format 3\x00")) {
		return nil, invalid("file is not a database")
	}
	db, err := sql.Open("sqlite", copyPath+"?"+url.Values{"_pragma": {"trusted_schema(0)"}}.Encode())
	if err != nil {
		return nil, invalid("%v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	lib, err := read(ctx, db)
	if err != nil {
		var inv *InvalidError
		if errors.As(err, &inv) || errors.Is(err, ErrNewer) || ctx.Err() != nil {
			return nil, err
		}
		return nil, invalid("%s", sqliteMessage(err))
	}
	return lib, nil
}

func read(ctx context.Context, db *sql.DB) (*Library, error) {
	// A WAL-mode export or a stray journal is converted on the private copy only.
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = DELETE"); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return nil, err
	}
	var integrity []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			return nil, err
		}
		integrity = append(integrity, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// SQLITE_CORRUPT's text, as Android reports corruption.
	if len(integrity) != 1 || integrity[0] != "ok" {
		return nil, invalid("database disk image is malformed")
	}
	var version int64
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version > Version {
		return nil, fmt.Errorf("%w: database version %d is newer than %d", ErrNewer, version, Version)
	}
	if version < 1 {
		return nil, invalid("Unsupported database version: %d", version)
	}
	var executable int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type IN ('view', 'trigger')`).Scan(&executable); err != nil {
		return nil, err
	}
	if executable > 0 {
		return nil, invalid("the file holds views or triggers, which AntennaPod backups never do")
	}
	if version < Version {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		if err := upgrade(ctx, tx, version); err != nil {
			tx.Rollback()
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	if err := validateSchema(ctx, db); err != nil {
		return nil, err
	}
	if err := validateColumns(ctx, db); err != nil {
		return nil, err
	}
	lib, err := mapLibrary(ctx, db)
	if err != nil {
		return nil, err
	}
	lib.SourceVersion = version
	return lib, nil
}

// validateSchema accepts only what AntennaPod writes: the required tables are
// ordinary tables, and the file has no views or triggers (a crafted file
// could otherwise make a read endless).
func validateSchema(ctx context.Context, db *sql.DB) error {
	var executable int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type IN ('view', 'trigger')`).Scan(&executable); err != nil {
		return err
	}
	if executable > 0 {
		return invalid("the file holds views or triggers, which AntennaPod backups never do")
	}
	for table := range requiredColumns {
		var typ, ddl string
		err := db.QueryRowContext(ctx, `SELECT type, COALESCE(sql, '') FROM sqlite_master WHERE name = ? COLLATE NOCASE`, table).Scan(&typ, &ddl)
		if errors.Is(err, sql.ErrNoRows) {
			return invalid("no such table: %s", table)
		}
		if err != nil {
			return err
		}
		if typ != "table" || !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ddl)), "CREATE TABLE") {
			return invalid("%s is not an ordinary table", table)
		}
	}
	return nil
}

func validateColumns(ctx context.Context, db *sql.DB) error {
	tables := make([]string, 0, len(requiredColumns))
	for t := range requiredColumns {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, table := range tables {
		// pragma_table_xinfo also lists generated columns (hidden 2 or 3).
		rows, err := db.QueryContext(ctx, "SELECT name, hidden FROM pragma_table_xinfo(?)", table)
		if err != nil {
			return err
		}
		present := map[string]bool{}
		generated := map[string]bool{}
		for rows.Next() {
			var name string
			var hidden int
			if err := rows.Scan(&name, &hidden); err != nil {
				rows.Close()
				return err
			}
			present[name] = true
			generated[name] = hidden == 2 || hidden == 3
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(present) == 0 {
			return invalid("no such table: %s", table)
		}
		for _, c := range requiredColumns[table] {
			if !present[c] {
				return invalid("no such column: %s.%s", table, c)
			}
			// AntennaPod never writes a generated column; one could make
			// each row's value as large as it likes.
			if generated[c] {
				return invalid("%s.%s is a generated column, which AntennaPod backups never have", table, c)
			}
		}
	}
	return nil
}

func mapLibrary(ctx context.Context, db *sql.DB) (*Library, error) {
	lib := &Library{Skipped: map[string]int{}}
	skip := func(reason string) { lib.Skipped[reason]++ }
	feedIndex := map[int64]int{}
	// Only the columns used are read, by name: a crafted file may add
	// others, generated ones included, of any size.
	feeds, err := named(ctx, db, `SELECT id, title, custom_title, type, link, description, author, image_url, language,
		payment_link, feed_identifier, last_update, is_paged, next_page_link, username, password, keep_updated,
		download_url, state, new_episodes_action, auto_download, auto_delete_action, feed_playback_speed, feed_skip_silence,
		feed_volume_adaption, include_filter, exclude_filter, minimal_duration_filter, feed_skip_intro, feed_skip_ending,
		episode_notification, tags, sort_order, hide FROM Feeds ORDER BY id`)
	if err != nil {
		return nil, err
	}
	for _, r := range feeds {
		f := Feed{
			ID: r.long("id"), Title: r.str("title"), CustomTitle: r.str("custom_title"), Type: r.str("type"),
			Link: r.str("link"), Description: r.str("description"), Author: r.str("author"), ImageURL: r.str("image_url"),
			Language: r.str("language"), PaymentLink: r.str("payment_link"), FeedIdentifier: r.str("feed_identifier"),
			LastUpdate: r.str("last_update"), IsPaged: r.int("is_paged") > 0, NextPageLink: r.str("next_page_link"),
			Username: r.str("username"), Password: r.str("password"), KeepUpdated: r.int("keep_updated") > 0,
			Settings: Settings{AutoDownload: r.int("auto_download"), AutoDeleteAction: r.int("auto_delete_action"),
				PlaybackSpeed: r.float("feed_playback_speed"), SkipSilence: r.int("feed_skip_silence"),
				VolumeAdaption: r.int("feed_volume_adaption"), IncludeFilter: r.str("include_filter"),
				ExcludeFilter: r.str("exclude_filter"), MinimalDuration: r.int("minimal_duration_filter"),
				SkipIntro: r.int("feed_skip_intro"), SkipEnding: r.int("feed_skip_ending"),
				EpisodeNotification: r.int("episode_notification") > 0, Tags: r.str("tags"), SortOrder: r.str("sort_order"),
				Hide: r.str("hide")},
		}
		if u := r.str("download_url"); u != nil {
			f.URL = *u
		}
		// Every feed's ID is used, a skipped feed's too (the result lists it).
		if err := inRange("Feeds", "id", f.ID); err != nil {
			return nil, err
		}
		switch s := r.int("state"); s {
		case 0, 1, 2:
			f.State = s
		default:
			skip("Feeds.state unsupported") // Feed.STATE_SUBSCRIBED stays
		}
		switch a := r.int("new_episodes_action"); a {
		case 0, 1, 2, 3:
			f.NewEpisodesAction = a
		default:
			f.NewEpisodesAction = 1 // NewEpisodesAction.fromCode: unknown → ADD_TO_INBOX
		}
		feedIndex[f.ID] = len(lib.Feeds)
		lib.Feeds = append(lib.Feeds, f)
	}

	// Every item is joined with its lowest-id media row (PodDBAdapter LEFT JOIN).
	media := map[int64]Media{}
	mediaRows, err := named(ctx, db, `SELECT id, feeditem, download_url, filesize, mime_type, duration, position,
		played_duration, last_played_time, playback_completion_date, downloaded FROM FeedMedia ORDER BY id`)
	if err != nil {
		return nil, err
	}
	for _, r := range mediaRows {
		item := r.long("feeditem")
		if _, dup := media[item]; dup {
			skip("FeedMedia duplicate for item")
			continue
		}
		media[item] = Media{ID: r.long("id"), URL: r.str("download_url"), Size: r.long("filesize"), MimeType: r.str("mime_type"),
			Duration: r.int("duration"), Position: r.int("position"), PlayedDuration: int64(r.int("played_duration")),
			LastPlayedTime: r.long("last_played_time"), CompletionDate: r.long("playback_completion_date"),
			Downloaded: r.long("downloaded") > 0}
	}
	favorites := map[int64]bool{}
	favRows, err := named(ctx, db, "SELECT feeditem FROM Favorites")
	if err != nil {
		return nil, err
	}
	for _, r := range favRows {
		favorites[r.long("feeditem")] = true
	}
	// Stored chapters in id order: Android's query (PodDBAdapter
	// getSimpleChaptersOfFeedItemCursor) has no ORDER BY and reads them in
	// rowid order through SimpleChapters_feeditem.
	chapters := map[int64][]Chapter{}
	chapterRows, err := named(ctx, db, "SELECT id, feeditem, start, title, link, image_url FROM SimpleChapters ORDER BY id")
	if err != nil {
		return nil, err
	}
	for _, r := range chapterRows {
		item := r.long("feeditem")
		chapters[item] = append(chapters[item], Chapter{Start: r.long("start"), Title: r.str("title"), Link: r.str("link"),
			ImageURL: r.str("image_url")})
	}
	episodeFeed := map[int64]int64{}
	items, err := named(ctx, db, `SELECT id, feed, item_identifier, title, pubDate, read, link, description, image_url,
		payment_link, podcastindex_chapter_url, podcastindex_transcript_url, podcastindex_transcript_type,
		social_interact_url, has_simple_chapters, auto_download FROM FeedItems ORDER BY id`)
	if err != nil {
		return nil, err
	}
	for _, r := range items {
		id, feedID := r.long("id"), r.long("feed")
		index, ok := feedIndex[feedID]
		if !ok {
			skip("FeedItems without feed")
			continue
		}
		if id <= 0 {
			skip("FeedItems invalid id")
			continue
		}
		e := Episode{ID: id, FeedID: feedID, GUID: r.str("item_identifier"), Title: r.str("title"), PubDate: r.long("pubDate"),
			Link: r.str("link"), Description: r.str("description"), ImageURL: r.str("image_url"), PaymentLink: r.str("payment_link"),
			ChapterURL: r.str("podcastindex_chapter_url"), TranscriptURL: r.str("podcastindex_transcript_url"),
			TranscriptType: r.str("podcastindex_transcript_type"), SocialURL: r.str("social_interact_url"), Favorite: favorites[id],
			AutoDownload: r.long("auto_download") > 0}
		// FeedItemCursor: getInt(...) > 0; DBReader.loadChaptersOfFeedItem:
		// no rows is no chapters (null).
		if r.int("has_simple_chapters") > 0 && len(chapters[id]) > 0 {
			e.Chapters = append([]Chapter{}, chapters[id]...)
		}
		switch r.int("read") {
		case -1:
			e.Read = -1
		case 1:
			e.Read = 1
		}
		if err := inRange("FeedItems", "id", e.ID); err != nil {
			return nil, err
		}
		if err := inRange("FeedItems", "pubDate", e.PubDate); err != nil {
			return nil, err
		}
		if m, ok := media[id]; ok {
			m := m
			for _, v := range []struct {
				column string
				value  int64
			}{{"id", m.ID}, {"filesize", m.Size}, {"played_duration", m.PlayedDuration}, {"last_played_time", m.LastPlayedTime},
				{"playback_completion_date", m.CompletionDate}} {
				if err := inRange("FeedMedia", v.column, v.value); err != nil {
					return nil, err
				}
			}
			e.Media = &m
		}
		episodeFeed[id] = feedID
		lib.Feeds[index].Episodes = append(lib.Feeds[index].Episodes, e)
	}

	queued := map[int64]bool{}
	queue, err := named(ctx, db, "SELECT feeditem FROM Queue ORDER BY id")
	if err != nil {
		return nil, err
	}
	for _, r := range queue {
		item := r.long("feeditem")
		if _, ok := episodeFeed[item]; !ok {
			skip("Queue without item")
			continue
		}
		if queued[item] {
			skip("Queue duplicate")
			continue
		}
		queued[item] = true
		lib.Queue = append(lib.Queue, item)
	}
	return lib, nil
}

// inRange refuses a number the import uses that the contract cannot carry
// (contract.MaxSafeInteger): Android would read it, but no server record
// can hold it.
func inRange(table, column string, v int64) error {
	if v > contract.MaxSafeInteger || v < -contract.MaxSafeInteger {
		return invalid("%s.%s holds %d, a number out of range", table, column, v)
	}
	return nil
}

// row reads columns by name with Android Cursor semantics: NULL reads as 0 or
// nil, getInt truncates, text holding a number reads as that number.
type row map[string]any

func (r row) long(name string) int64 {
	switch v := r[name].(type) {
	case int64:
		return v
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0
		}
		return int64(math.Max(math.Min(v, 9.2e18), -9.2e18))
	case string:
		n, err := strconv.ParseInt(strings.Trim(v, " \t"), 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

func (r row) int(name string) int32 { return int32(r.long(name)) }

// float reads as Cursor.getFloat: NULL is 0, text holding a number is that
// number.
func (r row) float(name string) float64 {
	switch v := r[name].(type) {
	case int64:
		return float64(float32(v))
	case float64:
		return float64(float32(v))
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 32)
		if err != nil {
			return 0
		}
		return float64(float32(f))
	}
	return 0
}

func (r row) str(name string) *string {
	var s string
	switch v := r[name].(type) {
	case string:
		s = v
	case int64:
		s = strconv.FormatInt(v, 10)
	case float64:
		s = strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return nil
	}
	return &s
}

func named(ctx context.Context, db *sql.DB, query string) ([]row, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []row
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		r := row{}
		for i, c := range cols {
			if _, seen := r[c]; !seen { // the first of duplicate column names wins
				r[c] = vals[i]
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	os.Remove(to)
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// sqliteMessage strips the driver's prefix and code, leaving SQLite's own text.
func sqliteMessage(err error) string {
	msg := err.Error()
	if i := strings.Index(msg, ": "); i >= 0 && strings.Contains(msg[:i], "SQLITE") {
		msg = msg[i+2:]
	}
	if i := strings.LastIndex(msg, " ("); i > 0 && strings.HasSuffix(msg, ")") {
		msg = msg[:i]
	}
	return msg
}
