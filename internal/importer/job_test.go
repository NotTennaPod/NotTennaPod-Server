package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

const fixtureDB = "androiddb/testdata/android-3.12.1/emulator-parity-library.db"

func allow(*storage.WriteTx) error { return nil }

func (e *env) runner(t *testing.T) *Runner {
	t.Helper()
	r := &Runner{Store: e.svc.Store, Service: e.svc, Dir: filepath.Join(t.TempDir(), "imports"),
		OpenStaging: StagingOpener(e.svc.Store)}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	return r
}

func fileOf(t *testing.T, path string) ([]byte, string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:])
}

// waitFor polls the import until it leaves processing, with a deadline.
func waitFor(t *testing.T, r *Runner, account, id string) storage.Import {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		im, err := r.Get(context.Background(), account, id)
		if err != nil {
			t.Fatal(err)
		}
		if im.Status == "published" || im.Status == "failed" {
			return im
		}
		if time.Now().After(deadline) {
			t.Fatalf("import still %s", im.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func upload(t *testing.T, r *Runner, account string, data []byte, fingerprint, kind string) storage.Import {
	t.Helper()
	ctx := context.Background()
	im, created, err := r.Reserve(ctx, allow, account, kind, fingerprint, int64(len(data)), "AntennaPodBackup.db")
	if err != nil || !created {
		t.Fatalf("reserve: %v %v", created, err)
	}
	half := len(data) / 2
	if err := r.Chunk(ctx, allow, account, im.ImportID, 0, data[:half]); err != nil {
		t.Fatal(err)
	}
	if err := r.Chunk(ctx, allow, account, im.ImportID, 1, data[half:]); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Finish(ctx, allow, account, im.ImportID); err != nil {
		t.Fatal(err)
	}
	return im
}

// The whole flow (PROTOCOL §11.5): chunks, a verified finish, staging, publication;
// the same file again returns the published import; another file is
// refused.
func TestImportFlow(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	ctx := context.Background()
	data, fp := fileOf(t, fixtureDB)
	// A wrong chunk fails the finish and keeps the upload for a resend.
	im, _, err := r.Reserve(ctx, allow, e.account, "android_db", fp, int64(len(data)), "a.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data[:10]); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); !errors.Is(err, ErrMismatch) {
		t.Fatalf("a short upload finished: %v", err)
	}
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); err != nil {
		t.Fatal(err)
	}
	done := waitFor(t, r, e.account, im.ImportID)
	if done.Status != "published" || !strings.Contains(done.Result, `"episodes":58`) || !strings.Contains(done.Result, `"podcasts":16`) {
		t.Fatalf("published import %+v", done)
	}
	if n := len(e.records()); n < 58*5 {
		t.Fatalf("only %d records after publication", n)
	}
	if again, created, err := r.Reserve(ctx, allow, e.account, "android_db", fp, int64(len(data)), "a.db"); err != nil || created || again.ImportID != im.ImportID {
		t.Fatalf("the same file again: %+v %v %v", again, created, err)
	}
	other, ofp := fileOf(t, "androiddb/testdata/android-3.12.1/first-journey-downloaded.db")
	if _, _, err := r.Reserve(ctx, allow, e.account, "android_db", ofp, int64(len(other)), "b.db"); !errors.Is(err, storage.ErrImportExists) {
		t.Fatalf("another file into the imported account: %v", err)
	}
	r.Stop() // the published import's cleanup has finished
	entries, _ := os.ReadDir(r.Dir)
	if len(entries) != 0 {
		t.Errorf("files left after publication: %v", entries)
	}
}

// A crash part-way through staging resumes at the next start from the
// podcasts already staged, and the result counts everything once.
func TestImportResumesAfterACrash(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		staged  int      // podcasts staged before the crash
		want    []string // in the published result
	}{
		{fixtureDB, 3, []string{`"podcasts":16`, `"episodes":58`}},
		// What is not carried counts every episode once, whichever run imported
		// it; merged episodes keep their stored chapters (the rebuild check at
		// the end covers them too).
		{"../../contract/vectors/import/device-only.db", 1, []string{`"episode_auto_download_off":3`,
			`"source_feed_id":2`}},
	} {
		t.Run(filepath.Base(tc.fixture), func(t *testing.T) { resumeAfterACrash(t, tc.fixture, tc.staged, tc.want) })
	}
}

func resumeAfterACrash(t *testing.T, fixture string, staged int, want []string) {
	resumeAfterACrashThen(t, fixture, staged, want, nil)
}

// TestResumedImportKeepsMergedChapters: a crash before the podcast with
// merged rows is imported, then the resumed run imports it (PROTOCOL §11.5).
func TestResumedImportKeepsMergedChapters(t *testing.T) {
	resumeAfterACrashThen(t, "../../contract/vectors/import/device-only.db", 0, nil, func(e *env) {
		titles := ""
		for k, v := range e.records() {
			if strings.HasSuffix(k, "/metadata") {
				b, _ := json.Marshal(v["chapters"])
				titles += string(b)
			}
		}
		for _, want := range []string{`"Row seven"`, `"Second row's"`, `"First row's"`, `"After an empty flag"`} {
			if !strings.Contains(titles, want) {
				t.Errorf("after the resumed import no episode has the chapter %s", want)
			}
		}
		if strings.Contains(titles, `"Row six"`) {
			t.Error("a merged episode took its second row's chapters over its first's")
		}
	})
}

func resumeAfterACrashThen(t *testing.T, fixture string, staged int, want []string, then func(*env)) {
	e := newEnv(t)
	ctx := context.Background()
	data, fp := fileOf(t, fixture)
	broken := &Runner{Store: e.svc.Store, Service: e.svc, Dir: filepath.Join(t.TempDir(), "imports"),
		OpenStaging: func(context.Context, string) (*storage.Store, error) { return nil, errors.New("disk unavailable") }}
	if err := broken.Start(ctx); err != nil {
		t.Fatal(err)
	}
	im := upload(t, broken, e.account, data, fp, "android_db")
	broken.Stop()
	if cur, _ := broken.Get(ctx, e.account, im.ImportID); cur.Status != "processing" {
		t.Fatalf("a storage error failed the import: %+v", cur)
	}
	// Stage the first podcasts, as a run that crashed after them.
	staging, err := StagingOpener(e.svc.Store)(ctx, broken.stagingDir(im.ImportID))
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{Store: e.svc.Store, Service: e.svc, Dir: broken.Dir}
	if _, err := r.stagingBase(ctx, staging, e.account); err != nil {
		t.Fatal(err)
	}
	lib, err := androiddb.Read(ctx, fixture, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := FromAndroid(lib, im.ImportID, im.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	partial := plan
	partial.Podcasts = plan.Podcasts[:staged]
	stagingSvc := *e.svc
	stagingSvc.Store = staging
	if _, err := Apply(ctx, &stagingSvc, e.account, partial, 0, nil); err != nil {
		t.Fatal(err)
	}
	staging.Close()
	r.OpenStaging = StagingOpener(e.svc.Store)
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	done := waitFor(t, r, e.account, im.ImportID)
	if done.Status != "published" {
		t.Fatalf("resumed import %+v", done)
	}
	for _, w := range want {
		if !strings.Contains(done.Result, w) {
			t.Errorf("resumed import's result %s lacks %s", done.Result, w)
		}
	}
	if then != nil {
		then(e)
	}
}

// A try that runs out of time after staging part of the library leaves its
// staging database, whose read-model capture triggers are kept (its writes
// are captured in staging's own clock); the next try opens it again under
// the exact schema check, resumes from its progress and publishes.
func TestImportResumesAfterAStagingTimeout(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	data, fp := fileOf(t, fixtureDB)
	r := &Runner{Store: e.svc.Store, Service: e.svc, Dir: filepath.Join(t.TempDir(), "imports"), Limit: 2 * time.Second, RetryWait: 1}
	var tries atomic.Int32
	var stagedTicks atomic.Int64
	r.OpenStaging = func(tryCtx context.Context, dir string) (*storage.Store, error) {
		if tries.Add(1) > 1 {
			return StagingOpener(e.svc.Store)(tryCtx, dir)
		}
		r.Limit = 0 // later tries have the default time
		// Stage the first three podcasts, then run out of time.
		staging, err := StagingOpener(e.svc.Store)(ctx, dir)
		if err != nil {
			return nil, err
		}
		defer staging.Close()
		if _, err := r.stagingBase(ctx, staging, e.account); err != nil {
			return nil, err
		}
		im, err := r.Get(ctx, e.account, strings.TrimSuffix(filepath.Base(dir), ".staging"))
		if err != nil {
			return nil, err
		}
		plan, err := r.plan(ctx, im)
		if err != nil {
			return nil, err
		}
		plan.Podcasts = plan.Podcasts[:3]
		stagingSvc := *e.svc
		stagingSvc.Store = staging
		if _, err := Apply(ctx, &stagingSvc, e.account, plan, 0, nil); err != nil {
			return nil, err
		}
		if err := staging.Read(ctx, func(rd *storage.ReadTx) error {
			c, err := rd.Account(e.account).ViewClock()
			stagedTicks.Store(c.Tick)
			return err
		}); err != nil {
			return nil, err
		}
		<-tryCtx.Done()
		return nil, tryCtx.Err()
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	im := upload(t, r, e.account, data, fp, "android_db")
	done := waitFor(t, r, e.account, im.ImportID)
	if done.Status != "published" || !strings.Contains(done.Result, `"podcasts":16`) || !strings.Contains(done.Result, `"episodes":58`) {
		t.Fatalf("resumed import %+v", done)
	}
	if tries.Load() != 2 || stagedTicks.Load() == 0 {
		t.Fatalf("%d tries; staging captured %d ticks", tries.Load(), stagedTicks.Load())
	}
}

// A file that cannot import fails the import and releases the account.
func TestInvalidFileReleasesTheAccount(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	bad, bfp := fileOf(t, "androiddb/testdata/invalid/text-file.db")
	im := upload(t, r, e.account, bad, bfp, "android_db")
	done := waitFor(t, r, e.account, im.ImportID)
	if done.Status != "failed" || !strings.HasPrefix(done.Reason, "invalid_file") {
		t.Fatalf("an invalid file: %+v", done)
	}
	good, gfp := fileOf(t, fixtureDB)
	im2 := upload(t, r, e.account, good, gfp, "android_db")
	if done := waitFor(t, r, e.account, im2.ImportID); done.Status != "published" {
		t.Fatalf("after a failed import: %+v", done)
	}
}

// Concurrent finishes of one import assemble the file once, from complete
// chunks; a refused authorization changes nothing.
func TestConcurrentFinishAndAuthorization(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	ctx := context.Background()
	data, fp := fileOf(t, fixtureDB)
	denied := errors.New("revoked")
	deny := func(*storage.WriteTx) error { return denied }
	if _, _, err := r.Reserve(ctx, deny, e.account, "android_db", fp, int64(len(data)), "a.db"); !errors.Is(err, denied) {
		t.Fatalf("a revoked caller reserved: %v", err)
	}
	im, _, err := r.Reserve(ctx, allow, e.account, "android_db", fp, int64(len(data)), "a.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Chunk(ctx, deny, e.account, im.ImportID, 0, data); !errors.Is(err, denied) {
		t.Fatalf("a revoked caller sent a chunk: %v", err)
	}
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Finish(ctx, deny, e.account, im.ImportID); !errors.Is(err, denied) {
		t.Fatalf("a revoked caller finished: %v", err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.Finish(ctx, allow, e.account, im.ImportID)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("a concurrent finish: %v", err)
		}
	}
	if done := waitFor(t, r, e.account, im.ImportID); done.Status != "published" || !strings.Contains(done.Result, `"episodes":58`) {
		t.Fatalf("after concurrent finishes: %+v", done)
	}
}
