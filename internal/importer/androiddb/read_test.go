package androiddb

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// The fixtures come from NotTennaPod-iOS's TestSupport/Data/database-restore:
// real Android 3.12.1 libraries made on the emulator, DDL-generated older
// versions with Android's own upgrades of them, and invalid files.

func readFixture(t *testing.T, path string) *Library {
	t.Helper()
	lib, err := Read(context.Background(), path, t.TempDir())
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return lib
}

func TestRealAndroidLibraries(t *testing.T) {
	parity := readFixture(t, "testdata/android-3.12.1/emulator-parity-library.db")
	items := 0
	for _, f := range parity.Feeds {
		items += len(f.Episodes)
	}
	if len(parity.Feeds) != 16 || items != 58 || len(parity.Queue) != 9 {
		t.Fatalf("parity library: %d feeds, %d items, %d queued", len(parity.Feeds), items, len(parity.Queue))
	}
	creds := readFixture(t, "testdata/android-3.12.1/archived-credentials-library.db")
	states := map[int32]int{}
	withCredentials, favorites := 0, 0
	for _, f := range creds.Feeds {
		states[f.State]++
		if f.Username != nil || f.Password != nil {
			withCredentials++
		}
		for _, e := range f.Episodes {
			if e.Favorite {
				favorites++
			}
		}
	}
	if states[0] == 0 || states[1] == 0 || states[2] == 0 || withCredentials != 4 || favorites != 1 {
		t.Fatalf("credentials library: states %v, %d with credentials, %d favorites", states, withCredentials, favorites)
	}
	for _, name := range []string{"authentication-preview-library.db", "first-journey-downloaded.db"} {
		readFixture(t, filepath.Join("testdata/android-3.12.1", name))
	}
}

// Reading an older backup after the ported upgrade equals reading Android's own
// upgrade of the same file.
func TestOlderVersionsMatchAndroidUpgrades(t *testing.T) {
	entries, err := os.ReadDir("testdata/android-3.12.1-upgraded")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no upgraded twins")
	}
	for _, e := range entries {
		t.Run(e.Name(), func(t *testing.T) {
			ours := readFixture(t, filepath.Join("testdata/older-versions", e.Name()))
			android := readFixture(t, filepath.Join("testdata/android-3.12.1-upgraded", e.Name()))
			if ours.SourceVersion >= Version {
				t.Fatalf("older fixture has version %d", ours.SourceVersion)
			}
			if len(summary(ours)) == 0 {
				t.Fatal("no episodes read")
			}
			ours.SourceVersion, android.SourceVersion = 0, 0
			if !reflect.DeepEqual(ours, android) {
				t.Fatalf("ported upgrade differs from Android's:\nours:    %+v\nandroid: %+v", summary(ours), summary(android))
			}
		})
	}
}

func summary(l *Library) []string {
	var out []string
	for _, f := range l.Feeds {
		for _, e := range f.Episodes {
			m := "-"
			if e.Media != nil {
				m = strings.Join([]string{deref(e.Media.URL), itoa(int64(e.Media.Position)), itoa(e.Media.PlayedDuration), itoa(e.Media.LastPlayedTime), itoa(e.Media.CompletionDate)}, ",")
			}
			out = append(out, itoa(f.ID)+"/"+itoa(e.ID)+" read="+itoa(int64(e.Read))+" guid="+deref(e.GUID)+" media="+m)
		}
	}
	return out
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestEdgeVersionReads(t *testing.T) {
	readFixture(t, "testdata/older-versions/v3110000-edges.db")
}

func TestInvalidFilesAreRefused(t *testing.T) {
	cases := map[string]func(error) bool{
		"zero-bytes.db":                func(err error) bool { return isInvalid(err, "file is not a database") },
		"text-file.db":                 func(err error) bool { return isInvalid(err, "file is not a database") },
		"foreign-sqlite-v0.db":         func(err error) bool { return isInvalid(err, "Unsupported database version: 0") },
		"empty-sqlite-v0.db":           func(err error) bool { return isInvalid(err, "Unsupported database version: 0") },
		"newer-3110001.db":             func(err error) bool { return errors.Is(err, ErrNewer) },
		"negative-version.db":          func(err error) bool { return isInvalid(err, "Unsupported database version") },
		"missing-feedmedia-3110000.db": func(err error) bool { return isInvalid(err, "no such table: FeedMedia") },
		"truncated-half.db":            func(err error) bool { return isInvalid(err, "malformed") },
		"corrupt-feeditems-page.db":    func(err error) bool { return isInvalid(err, "malformed") },
	}
	for name, ok := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata/invalid", name)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Read(context.Background(), path, t.TempDir())
			if !ok(err) {
				t.Fatalf("got %v", err)
			}
			after, _ := os.ReadFile(path)
			if string(before) != string(after) {
				t.Fatal("the source file was modified")
			}
		})
	}
}

func isInvalid(err error, contains string) bool {
	var inv *InvalidError
	return errors.As(err, &inv) && strings.Contains(inv.Message, contains)
}

// A file with views or triggers is refused before
// anything reads it (a crafted view could make a read endless).
func TestViewsAndTriggersAreRefused(t *testing.T) {
	data, err := os.ReadFile("testdata/android-3.12.1/first-journey-downloaded.db")
	if err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		"CREATE VIEW Loop AS WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r) SELECT x FROM r",
		"CREATE TRIGGER t AFTER INSERT ON Queue BEGIN DELETE FROM Queue; END",
	} {
		path := filepath.Join(t.TempDir(), "crafted.db")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if _, err := Read(context.Background(), path, t.TempDir()); !isInvalid(err, "views or triggers") {
			t.Errorf("%s: %v", ddl[:20], err)
		}
	}
}

// crafted copies the first-journey library, runs the statements on the copy
// and returns its path.
func crafted(t *testing.T, statements ...string) string {
	t.Helper()
	data, err := os.ReadFile("testdata/android-3.12.1/first-journey-downloaded.db")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "crafted.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range statements {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	return path
}

// A number the import uses beyond the contract's integer range refuses the file
// with a clear message, a skipped local feed's ID included (it is listed in the
// result).
func TestOutOfRangeNumbersAreRefused(t *testing.T) {
	const big = "9223372036854775807"
	for _, tc := range []struct{ statement, message string }{
		{"UPDATE Feeds SET id = " + big + ", download_url = 'antennapod_local:/audit'", "Feeds.id holds " + big + ", a number out of range"},
		{"UPDATE Feeds SET id = " + big, "Feeds.id holds " + big},
		{"UPDATE FeedItems SET pubDate = -" + big, "FeedItems.pubDate holds -" + big},
		{"UPDATE FeedMedia SET filesize = 9007199254740992", "FeedMedia.filesize holds 9007199254740992"},
		{"UPDATE FeedMedia SET playback_completion_date = 1e300", "FeedMedia.playback_completion_date"},
	} {
		_, err := Read(context.Background(), crafted(t, tc.statement), t.TempDir())
		if !isInvalid(err, tc.message) {
			t.Errorf("%s: %v", tc.statement, err)
		}
	}
	// The largest number the contract carries still reads.
	lib, err := Read(context.Background(), crafted(t, "UPDATE FeedMedia SET filesize = 9007199254740991"), t.TempDir())
	if err != nil || lib.Feeds[0].Episodes[0].Media.Size != 9007199254740991 {
		t.Fatalf("the largest safe number: %v", err)
	}
}

// A column the reader does not use is never read, so a generated column cannot
// inflate the read; a used column that is generated refuses the file.
func TestGeneratedColumnsAreNotRead(t *testing.T) {
	path := crafted(t, "ALTER TABLE Feeds ADD COLUMN padding BLOB GENERATED ALWAYS AS (zeroblob(16 * 1024 * 1024)) VIRTUAL")
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	lib, err := Read(context.Background(), path, t.TempDir())
	runtime.ReadMemStats(&after)
	if err != nil || len(lib.Feeds) != 1 {
		t.Fatalf("a file with an unused generated column: %v", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 16<<20 {
		t.Fatalf("reading allocated %d bytes: the unused generated column was read", n)
	}
	path = crafted(t, "ALTER TABLE Feeds RENAME COLUMN description TO old_description",
		"ALTER TABLE Feeds ADD COLUMN description BLOB GENERATED ALWAYS AS (zeroblob(16 * 1024 * 1024)) VIRTUAL")
	if _, err := Read(context.Background(), path, t.TempDir()); !isInvalid(err, "Feeds.description is a generated column") {
		t.Fatalf("a generated column the reader uses: %v", err)
	}
}
