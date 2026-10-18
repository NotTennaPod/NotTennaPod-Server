package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/rebuildtest"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// craftedFile copies an Android fixture, runs the statements on the copy and
// returns its bytes and fingerprint.
func craftedFile(t *testing.T, fixture string, statements ...string) ([]byte, string) {
	t.Helper()
	data, err := os.ReadFile(fixture)
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
	for _, s := range statements {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	return fileOf(t, path)
}

const firstJourney = "androiddb/testdata/android-3.12.1/first-journey-downloaded.db"

// A backup with a number beyond the contract's range fails its import with a
// clear message and releases the account; the worker survives (a skipped local
// feed's ID used to panic it, stopping the server). The same holds for a feed
// that is imported.
func TestOutOfRangeBackupFailsItsImport(t *testing.T) {
	for _, statement := range []string{
		"UPDATE Feeds SET id = 9223372036854775807, download_url = 'antennapod_local:/audit'",
		"UPDATE Feeds SET id = 9223372036854775807",
	} {
		t.Run(statement, func(t *testing.T) {
			e := newEnv(t)
			r := e.runner(t)
			data, fp := craftedFile(t, firstJourney, statement)
			done := waitFor(t, r, e.account, upload(t, r, e.account, data, fp, "android_db").ImportID)
			if done.Status != "failed" || done.Reason != "invalid_file: Feeds.id holds 9223372036854775807, a number out of range" {
				t.Fatalf("an out-of-range backup: %+v", done)
			}
			good, gfp := fileOf(t, fixtureDB)
			if done := waitFor(t, r, e.account, upload(t, r, e.account, good, gfp, "android_db").ImportID); done.Status != "published" {
				t.Fatalf("the account after the failed import: %+v", done)
			}
		})
	}
}

// Any panic while processing fails the import with a message, releases the account and leaves the
// runner working. The message is a fixed one, and the panic's text is neither shown nor logged.
func TestPanicFailsTheImport(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	var log syncBuffer
	r.Log = slog.New(slog.NewTextHandler(&log, nil))
	r.OpenStaging = func(context.Context, string) (*storage.Store, error) { panic("staging exploded") }
	data, fp := fileOf(t, fixtureDB)
	done := waitFor(t, r, e.account, upload(t, r, e.account, data, fp, "android_db").ImportID)
	if done.Status != "failed" || done.Reason != serverError {
		t.Fatalf("a panicking import: %+v", done)
	}
	if strings.Contains(log.String(), "staging exploded") || !strings.Contains(log.String(), "import: failed on an internal error") {
		t.Errorf("log: %s", log.String())
	}
	r.OpenStaging = StagingOpener(e.svc.Store)
	other, ofp := fileOf(t, firstJourney)
	if done := waitFor(t, r, e.account, upload(t, r, e.account, other, ofp, "android_db").ImportID); done.Status != "published" {
		t.Fatalf("after the panic: %+v", done)
	}
}

// A storage error is retried, a bounded number of times; then
// the import fails with its message and the account is released. A retry
// that works publishes.
func TestStorageErrorsAreRetriedBoundedTimes(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	r.RetryWait = 1 // a nanosecond: the test does not wait
	var log syncBuffer
	r.Log = slog.New(slog.NewTextHandler(&log, nil))
	var tries atomic.Int32
	r.OpenStaging = func(context.Context, string) (*storage.Store, error) {
		tries.Add(1)
		return nil, errors.New("disk unavailable at /private/path https://u:secret_pw@example.org/x")
	}
	data, fp := fileOf(t, fixtureDB)
	done := waitFor(t, r, e.account, upload(t, r, e.account, data, fp, "android_db").ImportID)
	if done.Status != "failed" || done.Reason != serverError || tries.Load() != MaxAttempts {
		t.Fatalf("a lasting storage error: %+v after %d tries", done, tries.Load())
	}
	// The error itself goes to the log only, scrubbed.
	if !strings.Contains(log.String(), "disk unavailable at /private/path") || strings.Contains(log.String(), "secret_pw") {
		t.Errorf("log: %s", log.String())
	}
	// Once the error passes, a retry publishes.
	tries.Store(0)
	r.OpenStaging = func(ctx context.Context, dir string) (*storage.Store, error) {
		if tries.Add(1) == 1 {
			return nil, errors.New("disk unavailable")
		}
		return StagingOpener(e.svc.Store)(ctx, dir)
	}
	other, ofp := fileOf(t, firstJourney)
	if done := waitFor(t, r, e.account, upload(t, r, e.account, other, ofp, "android_db").ImportID); done.Status != "published" || tries.Load() != 2 {
		t.Fatalf("a passing storage error: %+v after %d tries", done, tries.Load())
	}
}

// Tries count across restarts, so an import that keeps stopping
// the server (killed while processing) fails rather than resuming forever;
// a stop is not a try.
func TestTriesCountAcrossRestarts(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "imports")
	held := make(chan struct{})
	var once sync.Once
	stopped := &Runner{Store: e.svc.Store, Service: e.svc, Dir: dir, OpenStaging: func(ctx context.Context, _ string) (*storage.Store, error) {
		once.Do(func() { close(held) })
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	if err := stopped.Start(ctx); err != nil {
		t.Fatal(err)
	}
	data, fp := fileOf(t, fixtureDB)
	im := upload(t, stopped, e.account, data, fp, "android_db")
	<-held
	stopped.Stop()
	if b, _ := os.ReadFile(stopped.triesPath(im.ImportID)); string(b) != "0" {
		t.Fatalf("a stop counted as a try: %q", b)
	}
	// As if killed during each of its tries.
	if err := os.WriteFile(stopped.triesPath(im.ImportID), []byte(strconv.Itoa(MaxAttempts)), 0o600); err != nil {
		t.Fatal(err)
	}
	r := &Runner{Store: e.svc.Store, Service: e.svc, Dir: dir, OpenStaging: StagingOpener(e.svc.Store)}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if done := waitFor(t, r, e.account, im.ImportID); done.Status != "failed" || !strings.HasPrefix(done.Reason, "interrupted: ") {
		t.Fatalf("an import interrupted %d times: %+v", MaxAttempts, done)
	}
}

// The chunks kept never add up to more than the reservation, a resent chunk
// replacing its earlier bytes; the reservation bounds the chunk index; assembly
// checks the size before it copies.
func TestUploadStaysWithinItsReservation(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	ctx := context.Background()
	tiny, _, err := r.Reserve(ctx, allow, e.account, "opml", strings.Repeat("0", 64), 1, "tiny.opml")
	if err != nil {
		t.Fatal(err)
	}
	full := make([]byte, MaxChunk)
	for _, n := range []int{0, 1, 513} {
		if err := r.Chunk(ctx, allow, e.account, tiny.ImportID, n, full); !errors.Is(err, ErrTooLarge) {
			t.Errorf("chunk %d of a one-byte file: %v", n, err)
		}
	}
	if err := r.Chunk(ctx, allow, e.account, tiny.ImportID, 1, []byte("x")); !errors.Is(err, ErrTooLarge) {
		t.Errorf("chunk 1 of a one-byte file: %v", err)
	}
	if kept, _ := r.uploaded(tiny.ImportID, ""); kept != 0 {
		t.Fatalf("%d bytes kept for a one-byte reservation", kept)
	}
	if err := r.fail(ctx, tiny, "test_done"); err != nil {
		t.Fatal(err)
	}

	data, fp := fileOf(t, fixtureDB)
	im, _, err := r.Reserve(ctx, allow, e.account, "android_db", fp, int64(len(data)), "a.db")
	if err != nil {
		t.Fatal(err)
	}
	half := len(data) / 2
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data[:half]); err != nil {
		t.Fatal(err)
	}
	// One byte over, counting chunk 0.
	over := append(append([]byte{}, data[half:]...), 0)
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 1, over); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("chunks over the reservation: %v", err)
	}
	// A short upload is refused at finish before anything is copied.
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); !errors.Is(err, ErrMismatch) {
		t.Fatalf("a short upload finished: %v", err)
	}
	if entries, _ := filepath.Glob(filepath.Join(r.Dir, "."+im.ImportID+".file.*")); len(entries) != 0 || exists(r.filePath(im.ImportID)) {
		t.Fatalf("a short upload was copied: %v", entries)
	}
	// Resending chunk 0 replaces it: the whole file in chunk 0 fits.
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data); err != nil {
		t.Fatalf("resending chunk 0: %v", err)
	}
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 1, []byte("x")); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a chunk after the whole file: %v", err)
	}
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); err != nil {
		t.Fatal(err)
	}
	if done := waitFor(t, r, e.account, im.ImportID); done.Status != "published" {
		t.Fatalf("after the refused chunks: %+v", done)
	}
}

// Reserving, each chunk and assembly keep the backups' headroom
// free (the larger of 1 GiB and twice the database).
func TestUploadKeepsTheBackupsHeadroom(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	ctx := context.Background()
	var free atomic.Int64
	r.FreeBytes = func(string) (int64, error) { return free.Load(), nil }
	data, fp := fileOf(t, fixtureDB)
	size := int64(len(data))
	free.Store(1<<30 + 3*size - 1)
	if _, _, err := r.Reserve(ctx, allow, e.account, "android_db", fp, size, "a.db"); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("a reservation without room: %v", err)
	}
	free.Store(1<<30 + 3*size)
	im, _, err := r.Reserve(ctx, allow, e.account, "android_db", fp, size, "a.db")
	if err != nil {
		t.Fatal(err)
	}
	free.Store(1<<30 + size - 1)
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("a chunk without room: %v", err)
	}
	free.Store(1<<30 + size)
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data); err != nil {
		t.Fatal(err)
	}
	free.Store(1<<30 + size - 1)
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("an assembly without room: %v", err)
	}
	r.FreeBytes = func(string) (int64, error) { return 0, errors.New("statfs failed") }
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("an assembly with unknown free space: %v", err)
	}
	if err := r.fail(ctx, im, "test_done"); err != nil {
		t.Fatal(err)
	}
}

// Reading OPML stops when its context is cancelled, and a stop
// leaves the import to resume rather than failing it.
func TestOPMLReadingStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadOPML(ctx, []byte(`<opml><body><outline xmlUrl="https://example.invalid/feed"/></body></opml>`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled read: %v", err)
	}
	r := &Runner{Dir: t.TempDir()}
	im := storage.Import{ImportID: "test", Kind: "opml"}
	if err := os.WriteFile(r.filePath(im.ImportID), []byte(`<opml><body><outline xmlUrl="https://example.invalid/feed"/></body></opml>`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.plan(ctx, im); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled plan: %v", err)
	}
}

// An imported feed's credentials are approved for its origin in the fetcher's
// form (scheme://host:port), or the fetcher would never send them.
func TestImportedCredentialsUseTheFetchersOrigin(t *testing.T) {
	const id = "c1450489-13c0-45c9-8a29-c6527f813d73"
	opml, err := ReadOPML(context.Background(), []byte(`<opml><body><outline xmlUrl="https://alice:secret@Example.org/feed"/></body></opml>`))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := craftedFile(t, firstJourney, "UPDATE Feeds SET download_url = 'http://bob:secret@example.org:8080/feed', username = NULL, password = NULL")
	path := filepath.Join(t.TempDir(), "a.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	lib, err := androiddb.Read(context.Background(), path, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	android, err := FromAndroid(lib, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	for want, plan := range map[string]Plan{"https://example.org:443": FromOPML(opml, id, 1), "http://example.org:8080": android} {
		if len(plan.Secrets) != 1 {
			t.Fatalf("secrets %+v", plan.Secrets)
		}
		for _, s := range plan.Secrets {
			if len(s.ApprovedOrigins) != 1 || s.ApprovedOrigins[0] != want {
				t.Errorf("approved origins %v, want %s", s.ApprovedOrigins, want)
			}
		}
	}
}

// Feeds and imports note: one import processes at a time on the server;
// another account's import waits, still processing, and then publishes.
func TestOneImportAtATime(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	var other string
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		other = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var running, most atomic.Int32
	r.OpenStaging = func(ctx context.Context, dir string) (*storage.Store, error) {
		n := running.Add(1)
		defer running.Add(-1)
		for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return StagingOpener(e.svc.Store)(ctx, dir)
	}
	first, ffp := fileOf(t, fixtureDB)
	second, sfp := fileOf(t, firstJourney)
	a := upload(t, r, e.account, first, ffp, "android_db")
	b := upload(t, r, other, second, sfp, "android_db")
	close(gate)
	for _, im := range []storage.Import{a, b} {
		if done := waitFor(t, r, im.AccountID, im.ImportID); done.Status != "published" {
			t.Fatalf("import %+v", done)
		}
	}
	if most.Load() != 1 {
		t.Fatalf("%d imports staged at once", most.Load())
	}
	t.Cleanup(func() { rebuildtest.Check(t, e.svc, other, nil) })
}

// Feeds and imports note: a try's deadline covers staging. A try that runs
// out of time while staging is tried again, a bounded number of times, then
// fails as too slow and releases the account.
func TestImportDeadlineCoversStaging(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	r.Limit, r.RetryWait = 200*time.Millisecond, 1
	var tries atomic.Int32
	r.OpenStaging = func(ctx context.Context, _ string) (*storage.Store, error) {
		tries.Add(1)
		<-ctx.Done() // staging that never ends by itself
		return nil, ctx.Err()
	}
	data, fp := fileOf(t, fixtureDB)
	done := waitFor(t, r, e.account, upload(t, r, e.account, data, fp, "android_db").ImportID)
	if done.Status != "failed" || !strings.HasPrefix(done.Reason, "too_slow: ") || tries.Load() != MaxAttempts {
		t.Fatalf("staging past its deadline: %+v after %d tries", done, tries.Load())
	}
}

// An imported item's GUID loses its URL userinfo before the episode is derived,
// as a fetched one does.
func TestImportedGUIDLosesItsCredentials(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	data, fp := craftedFile(t, fixtureDB, "UPDATE FeedItems SET item_identifier = 'https://alice:secret_pw@example.org/episode' WHERE id = 1")
	im := upload(t, r, e.account, data, fp, "android_db")
	r.wg.Wait()
	got, err := r.Get(context.Background(), e.account, im.ImportID)
	if err != nil || got.Status != "published" {
		t.Fatalf("%+v %v", got, err)
	}
	found := false
	for k, v := range e.records() {
		s := fmt.Sprint(v)
		if strings.Contains(s, "secret_pw") {
			t.Errorf("record %s keeps the credentials", k)
		}
		if strings.HasSuffix(k, "/identity") && strings.Contains(s, "https://example.org/episode") {
			found = true
		}
	}
	if !found {
		t.Error("no episode identity names the GUID without its userinfo")
	}
	err = e.svc.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
		cms, err := tx.Account(e.account).Commits(0, 1<<40, 0)
		for _, cm := range cms {
			if strings.Contains(string(cm.Canonical), "secret_pw") {
				t.Errorf("commit %d keeps the credentials", cm.Seq)
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// An imported feed URL loses userinfo by the one rule, so an @ written as a
// character reference leaves no credentials in the podcast's identity (its
// refresh URL and aliases).
func TestImportedFeedURLLosesEncodedCredentials(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	data, fp := craftedFile(t, fixtureDB,
		"UPDATE Feeds SET download_url = 'https://alice:secret_pw&#64;example.org/feed', username = NULL, password = NULL WHERE id = 1")
	im := upload(t, r, e.account, data, fp, "android_db")
	r.wg.Wait()
	got, err := r.Get(context.Background(), e.account, im.ImportID)
	if err != nil || got.Status != "published" {
		t.Fatalf("%+v %v", got, err)
	}
	found := false
	for k, v := range e.records() {
		s := fmt.Sprint(v)
		if strings.Contains(s, "secret_pw") {
			t.Errorf("record %s keeps the credentials: %s", k, s)
		}
		if strings.HasSuffix(k, "/identity") && strings.Contains(s, "https://example.org/feed") {
			found = true
		}
	}
	if !found {
		t.Error("no podcast identity has the feed URL without its userinfo")
	}
	err = e.svc.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
		cms, err := tx.Account(e.account).Commits(0, 1<<40, 0)
		for _, cm := range cms {
			if strings.Contains(string(cm.Canonical), "secret_pw") {
				t.Errorf("commit %d keeps the credentials", cm.Seq)
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// As the import reference has it, the import removes URL userinfo by the fetch's one rule. A feed
// URL whose userinfo is spelled with a character reference is stored and
// grouped without it and is no credentials (Android's client sends only a
// literal user:password@); an enclosure with such userinfo derives the
// episode its feed's fetch derives.
func TestImportRemovesUserinfoByTheFetchRule(t *testing.T) {
	const id = "4c1f0b5e-2d7a-4e9b-8f3c-6a5d4e3c2b1a"
	text := func(s string) *string { return &s }
	for _, tc := range []struct {
		url         string
		credentials bool
	}{{"https://me:pw&#64;u.example.invalid/f", false}, {"https://me:pw@u.example.invalid/f", true}} {
		lib := &androiddb.Library{Feeds: []androiddb.Feed{{ID: 1, Title: text("U"), URL: tc.url, KeepUpdated: true}}}
		plan, err := FromAndroid(lib, id, 1)
		if err != nil {
			t.Fatal(err)
		}
		p := plan.Podcasts[0]
		if p.URL != "https://u.example.invalid/f" || contract.NormalizeURL(p.URL) != "https://u.example.invalid/f" {
			t.Errorf("%s: stored as %s", tc.url, p.URL)
		}
		if (p.SecretID != "") != tc.credentials || (len(plan.Secrets) > 0) != tc.credentials {
			t.Errorf("%s: credentials %q %v, want %v", tc.url, p.SecretID, plan.Secrets, tc.credentials)
		}
	}
	e := newEnv(t)
	media := func(id int64, u string) *androiddb.Media { return &androiddb.Media{ID: id, URL: text(u), Size: 1000} }
	lib := &androiddb.Library{Feeds: []androiddb.Feed{{ID: 1, Title: text("F"), URL: "https://f.example.invalid/feed", KeepUpdated: true,
		Episodes: []androiddb.Episode{
			{ID: 1, FeedID: 1, Title: text("a"), PubDate: 1_700_000_000_000, Media: media(1, "https://u:p&#64;x.example.invalid/e.mp3")},
			{ID: 2, FeedID: 1, Title: text("b"), PubDate: 1_700_000_000_000, Media: media(2, "https://x.example.invalid/e.mp3")}}}}}
	plan, err := FromAndroid(lib, id, e.clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range plan.Podcasts[0].Rows {
		if r.Item.Occurrence["k"] != "enclosure" || r.Item.Occurrence["v"] != "https://x.example.invalid/e.mp3" {
			t.Errorf("row %d's occurrence %v", r.SourceItem, r.Item.Occurrence)
		}
	}
	pods, err := Apply(context.Background(), e.svc, e.account, plan, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Complete(context.Background(), e.svc, e.account, id, "", plan); err != nil {
		t.Fatal(err)
	}
	if pods[0].EpisodeOf[1] == "" || pods[0].EpisodeOf[1] != pods[0].EpisodeOf[2] {
		t.Errorf("the two enclosures are not one episode: %v", pods[0].EpisodeOf)
	}
	for k, v := range e.records() {
		if s := fmt.Sprint(v); strings.Contains(s, "u:p") {
			t.Errorf("record %s keeps the enclosure's userinfo: %s", k, s)
		}
	}
}

// Assembly checks the free space as it writes and before it publishes the file,
// so room another writer takes meanwhile fails it cleanly: no file, the chunks
// kept for a retry, which succeeds once there is room again.
func TestAssemblyChecksTheFreeSpaceAsItWrites(t *testing.T) {
	for _, fallsAt := range []int{2, 3} { // before the chunk is copied; before the file is published
		t.Run(strconv.Itoa(fallsAt), func(t *testing.T) {
			e := newEnv(t)
			r := e.runner(t)
			data, fp := fileOf(t, fixtureDB)
			ctx := context.Background()
			im, _, err := r.Reserve(ctx, allow, e.account, "android_db", fp, int64(len(data)), "a.db")
			if err != nil {
				t.Fatal(err)
			}
			defer r.fail(ctx, im, "test done") // the account is free for the rebuild check
			if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data); err != nil {
				t.Fatal(err)
			}
			checks := 0
			r.FreeBytes = func(string) (int64, error) {
				checks++
				if checks >= fallsAt {
					return 0, nil
				}
				return 1<<40 + int64(len(data)), nil
			}
			if err := r.assemble(im); !errors.Is(err, ErrNoSpace) {
				t.Fatalf("assembly with the room gone after %d checks: %v", fallsAt-1, err)
			}
			if exists(r.filePath(im.ImportID)) {
				t.Error("the file was published")
			}
			if entries, _ := os.ReadDir(r.Dir); len(entries) > 0 {
				for _, x := range entries {
					if strings.HasPrefix(x.Name(), "."+im.ImportID+".file.") {
						t.Errorf("a partial file stays: %s", x.Name())
					}
				}
			}
			r.FreeBytes = func(string) (int64, error) { return 1<<40 + int64(len(data)), nil }
			if err := r.assemble(im); err != nil || !exists(r.filePath(im.ImportID)) {
				t.Errorf("the retry with room: %v", err)
			}
		})
	}
}

// syncBuffer is a bytes.Buffer safe for a logger's concurrent writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// importFiles lists the files of an import in the runner's directory.
func importFiles(t *testing.T, r *Runner, importID string) []string {
	t.Helper()
	entries, err := os.ReadDir(r.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, x := range entries {
		if strings.Contains(x.Name(), importID) {
			out = append(out, x.Name())
		}
	}
	return out
}

// otherAccount creates another account, which a test may delete (the env's
// own ends with the rebuild invariant).
func otherAccount(t *testing.T, e *env) string {
	t.Helper()
	var id string
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		id = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// deleteAccount deletes an account's rows, as an administrator's deletion
// does in its transaction.
func deleteAccount(t *testing.T, e *env, account string) []string {
	t.Helper()
	var ids []string
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		var err error
		if ids, err = tx.Account(account).ImportIDs(); err != nil {
			return err
		}
		return tx.DeleteAccount(account)
	}); err != nil {
		t.Fatal(err)
	}
	return ids
}

// Deleting an account ends its import (processing is
// cancelled and waited for) and removes its files: chunks, the assembled
// file, staging and tries; nothing is recreated afterwards.
func TestDeletingAnAccountEndsItsImport(t *testing.T) {
	e := newEnv(t)
	other := otherAccount(t, e)
	r := e.runner(t)
	staging := make(chan struct{})
	r.OpenStaging = func(ctx context.Context, dir string) (*storage.Store, error) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
		close(staging)
		<-ctx.Done() // processing until cancelled
		return nil, ctx.Err()
	}
	data, fp := fileOf(t, fixtureDB)
	im := upload(t, r, other, data, fp, "android_db")
	<-staging
	if len(importFiles(t, r, im.ImportID)) == 0 {
		t.Fatal("precondition: the import has files")
	}
	r.Discard(deleteAccount(t, e, other))
	if files := importFiles(t, r, im.ImportID); len(files) != 0 {
		t.Errorf("files left: %v", files)
	}
	r.Stop() // whatever still ran has ended: it recreated nothing
	if files := importFiles(t, r, im.ImportID); len(files) != 0 {
		t.Errorf("files recreated: %v", files)
	}
}

// A chunk written for an import whose account is deleted meanwhile
// (after the chunk's check, before its write) does not stay.
func TestAChunkForADeletedImportDoesNotStay(t *testing.T) {
	e := newEnv(t)
	other := otherAccount(t, e)
	r := e.runner(t)
	data, fp := fileOf(t, fixtureDB)
	ctx := context.Background()
	im, _, err := r.Reserve(ctx, allow, other, "android_db", fp, int64(len(data)), "a.db")
	if err != nil {
		t.Fatal(err)
	}
	r.FreeBytes = func(string) (int64, error) { // checked right before the chunk is written
		deleteAccount(t, e, other)
		return 1 << 40, nil
	}
	if err := r.Chunk(ctx, allow, other, im.ImportID, 0, data); !errors.Is(err, ErrNotFound) {
		t.Errorf("a chunk for a deleted import: %v", err)
	}
	if files := importFiles(t, r, im.ImportID); len(files) != 0 {
		t.Errorf("files left: %v", files)
	}
}

// A crash after the deleting transaction, before the files are removed,
// is finished at the next start: files of imports without rows go.
func TestStartRemovesADeletedAccountsImportFiles(t *testing.T) {
	e := newEnv(t)
	other := otherAccount(t, e)
	r := e.runner(t)
	data, fp := fileOf(t, fixtureDB)
	ctx := context.Background()
	im, _, err := r.Reserve(ctx, allow, other, "android_db", fp, int64(len(data)), "a.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Chunk(ctx, allow, other, im.ImportID, 0, data); err != nil {
		t.Fatal(err)
	}
	r.Stop()
	deleteAccount(t, e, other) // and no Discard: the server stopped
	again := &Runner{Store: e.svc.Store, Service: e.svc, Dir: r.Dir, OpenStaging: StagingOpener(e.svc.Store)}
	if err := again.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer again.Stop()
	if files := importFiles(t, again, im.ImportID); len(files) != 0 {
		t.Errorf("files left: %v", files)
	}
}

// A file of a deleted account's import that cannot be
// removed is reported, not taken for removed: Discard says so, the error is
// logged by path (never content), PendingCleanup lists it, and every start
// and sweep try again until it goes.
func TestAFileThatCannotBeRemovedStaysPendingUntilItGoes(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	var log syncBuffer
	r.Log = slog.New(slog.NewTextHandler(&log, nil))
	other := otherAccount(t, e)
	ctx := context.Background()
	data := []byte(`<opml><body><outline xmlUrl="https://alice:secret_pw@example.test/feed"/></body></opml>`)
	sum := sha256.Sum256(data)
	im, _, err := r.Reserve(ctx, allow, other, "opml", hex.EncodeToString(sum[:]), int64(len(data)), "a.opml")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Chunk(ctx, allow, other, im.ImportID, 0, data); err != nil {
		t.Fatal(err)
	}
	upload := r.uploadDir(im.ImportID)
	if err := os.Chmod(upload, 0o500); err != nil { // its chunk cannot be unlinked
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(upload, 0o700) })
	if r.Discard(deleteAccount(t, e, other)) {
		t.Error("Discard reported every file removed")
	}
	pending := r.PendingCleanup()
	if len(pending) == 0 || !strings.Contains(strings.Join(pending, " "), upload) {
		t.Errorf("pending %v", pending)
	}
	if !strings.Contains(log.String(), "import: cannot remove an import's file") || !strings.Contains(log.String(), upload) ||
		strings.Contains(log.String(), "secret_pw") {
		t.Errorf("log: %s", log.String())
	}
	// A start tries again, and still reports it.
	r.Stop()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.PendingCleanup()) == 0 {
		t.Error("the start forgot the pending file")
	}
	// Once the file can go, the sweep removes it.
	if err := os.Chmod(upload, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(upload); !os.IsNotExist(err) || len(r.PendingCleanup()) != 0 {
		t.Errorf("after the sweep: %v, pending %v", err, r.PendingCleanup())
	}
}

// The sweep runs by itself, every SweepEvery.
func TestTheSweepRunsByItself(t *testing.T) {
	e := newEnv(t)
	dir := filepath.Join(t.TempDir(), "imports")
	orphan := filepath.Join(dir, "5d1c1c2e-7c1e-4b9a-9f0e-2a1b3c4d5e6f.upload")
	r := &Runner{Store: e.svc.Store, Service: e.svc, Dir: dir, OpenStaging: StagingOpener(e.svc.Store), SweepEvery: time.Millisecond}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if err := os.MkdirAll(orphan, 0o700); err != nil { // left after the start's own pass
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for exists(orphan) {
		if time.Now().After(deadline) {
			t.Fatal("the sweep did not remove the orphan")
		}
		runtime.Gosched()
	}
}

// The import folders inside the data directory must be
// real folders (private.RealDir): a symlinked imports folder stops the
// runner's start, and a symlinked staging folder refuses the backup's
// reading, each with private.ErrSymlink and nothing written through the
// link. (An upload folder is made the same way, under a fresh random name.)
func TestSymlinkedImportFoldersAreRefused(t *testing.T) {
	e := newEnv(t)
	outside := t.TempDir()
	dir := filepath.Join(t.TempDir(), "imports")
	if err := os.Symlink(outside, dir); err != nil {
		t.Fatal(err)
	}
	r := &Runner{Store: e.svc.Store, Service: e.svc, Dir: dir, OpenStaging: StagingOpener(e.svc.Store)}
	if err := r.Start(context.Background()); !errors.Is(err, private.ErrSymlink) {
		t.Errorf("a symlinked imports folder: %v", err)
	}
	r.Stop()
	staging := filepath.Join(t.TempDir(), "staging")
	if err := os.Symlink(outside, staging); err != nil {
		t.Fatal(err)
	}
	if _, err := androiddb.Read(context.Background(), fixtureDB, staging); !errors.Is(err, private.ErrSymlink) {
		t.Errorf("a symlinked staging folder: %v", err)
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Errorf("files written through a link: %v", entries)
	}
}
