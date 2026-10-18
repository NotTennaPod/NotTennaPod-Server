package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// opmlOf is an OPML file of n podcasts, padded with a comment to at least pad
// bytes, and its fingerprint.
func opmlOf(n, pad int) ([]byte, string) {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0"?><opml version="2.0"><head><title>t</title></head><body>`)
	for i := range n {
		fmt.Fprintf(&b, `<outline type="rss" text="Show %d" xmlUrl="https://example.test/feed-%d.xml"/>`, i, i)
	}
	b.WriteString(`</body></opml>`)
	if b.Len() < pad {
		b.WriteString("<!--" + strings.Repeat("x", pad-b.Len()-7) + "-->")
	}
	sum := sha256.Sum256(b.Bytes())
	return b.Bytes(), hex.EncodeToString(sum[:])
}

// reserveSent reserves data and sends it in chunks of size each, without
// finishing.
func reserveSent(t *testing.T, r *Runner, account string, data []byte, fp string, size int) storage.Import {
	t.Helper()
	ctx := context.Background()
	im, created, err := r.Reserve(ctx, allow, account, "opml", fp, int64(len(data)), "subscriptions.opml")
	if err != nil || !created {
		t.Fatalf("reserve: %v %v", created, err)
	}
	for n := 0; n*size < len(data); n++ {
		if err := r.Chunk(ctx, allow, account, im.ImportID, n, data[n*size:min(len(data), (n+1)*size)]); err != nil {
			t.Fatalf("chunk %d: %v", n, err)
		}
	}
	return im
}

// status is Status that must succeed.
func status(t *testing.T, r *Runner, account, id string) State {
	t.Helper()
	st, err := r.Status(context.Background(), account, id)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// webLayout is the web client's resume rule: every held chunk n has the length
// a chunk n of its own layout has.
func webLayout(size int64, chunks [][2]int64, chunk int64) bool {
	for _, c := range chunks {
		if c[1] != min(chunk, size-chunk*c[0]) {
			return false
		}
	}
	return true
}

// Cancelling an uploading import returns (the import's lock is never taken
// twice), fails it as cancelled, removes its chunks and releases the account:
// the same file can be reserved again, as a new import, and imported.
func TestCancelUploadingReturnsDeletesAndAllowsNewReservation(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	ctx := context.Background()
	data, fp := opmlOf(3, 0)
	im, _, err := r.Reserve(ctx, allow, e.account, "opml", fp, int64(len(data)), "subscriptions.opml")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 0, data[:10]); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var got storage.Import
	go func() {
		defer close(done)
		got, err = r.Cancel(ctx, allow, e.account, im.ImportID)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Cancel did not return")
	}
	if err != nil || got.Status != "failed" || got.Reason != "cancelled" || got.Result != "" {
		t.Fatalf("cancel: %+v %v", got, err)
	}
	if files := importFiles(t, r, im.ImportID); len(files) != 0 {
		t.Errorf("files left: %v", files)
	}
	if err := r.Chunk(ctx, allow, e.account, im.ImportID, 1, data[10:20]); !errors.Is(err, ErrNotUploading) {
		t.Errorf("a chunk after the cancel: %v", err)
	}
	again, created, err := r.Reserve(ctx, allow, e.account, "opml", fp, int64(len(data)), "subscriptions.opml")
	if err != nil || !created || again.ImportID == im.ImportID {
		t.Fatalf("the same file again: %+v %v %v", again, created, err)
	}
	if err := r.Chunk(ctx, allow, e.account, again.ImportID, 0, data); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Finish(ctx, allow, e.account, again.ImportID); err != nil {
		t.Fatal(err)
	}
	if done := waitFor(t, r, e.account, again.ImportID); done.Status != "published" {
		t.Fatalf("the new import: %+v", done)
	}
}

// A repeated cancel answers the cancelled import as it is, without an error.
func TestCancelRepeatIdempotent(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	data, fp := opmlOf(2, 0)
	im := reserveSent(t, r, e.account, data, fp, len(data))
	for i := range 2 {
		got, err := r.Cancel(context.Background(), allow, e.account, im.ImportID)
		if err != nil || got.Status != "failed" || got.Reason != "cancelled" {
			t.Fatalf("cancel %d: %+v %v", i, got, err)
		}
	}
}

// A processing or published import cannot be cancelled: 409
// import_not_uploading, nothing changed.
func TestCancelProcessingIs409(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	release := make(chan struct{})
	r.OpenStaging = func(ctx context.Context, dir string) (*storage.Store, error) {
		<-release
		return StagingOpener(e.svc.Store)(ctx, dir)
	}
	ctx := context.Background()
	data, fp := opmlOf(2, 0)
	im := reserveSent(t, r, e.account, data, fp, len(data))
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Cancel(ctx, allow, e.account, im.ImportID); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("cancel while processing: %v", err)
	}
	if cur, _ := r.Get(ctx, e.account, im.ImportID); cur.Status != "processing" {
		t.Fatalf("after the refused cancel: %+v", cur)
	}
	close(release)
	if done := waitFor(t, r, e.account, im.ImportID); done.Status != "published" {
		t.Fatalf("import %+v", done)
	}
	if _, err := r.Cancel(ctx, allow, e.account, im.ImportID); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("cancel when published: %v", err)
	}
}

// Another account's import is not found, and stays as it was.
func TestCancelOtherAccountNotFound(t *testing.T) {
	e := newEnv(t)
	other := otherAccount(t, e)
	r := e.runner(t)
	data, fp := opmlOf(2, 0)
	im := reserveSent(t, r, other, data, fp, len(data))
	if _, err := r.Cancel(context.Background(), allow, e.account, im.ImportID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another account's cancel: %v", err)
	}
	st := status(t, r, other, im.ImportID)
	if st.Import.Status != "uploading" || len(st.Chunks) != 1 {
		t.Fatalf("after another account's cancel: %+v", st)
	}
}

// A caller refused inside the cancel's transaction (revoked, disabled)
// changes nothing: the import stays uploading with its chunks.
func TestCancelRevokedOrDisabledCallerChangesNothing(t *testing.T) {
	e := newEnv(t)
	a := otherAccount(t, e) // left with its upload
	r := e.runner(t)
	data, fp := opmlOf(2, 0)
	im := reserveSent(t, r, a, data, fp, len(data))
	denied := errors.New("revoked")
	if _, err := r.Cancel(context.Background(), func(*storage.WriteTx) error { return denied }, a, im.ImportID); !errors.Is(err, denied) {
		t.Fatalf("a refused cancel: %v", err)
	}
	st := status(t, r, a, im.ImportID)
	if st.Import.Status != "uploading" || len(st.Chunks) != 1 || st.Received != int64(len(data)) {
		t.Fatalf("after a refused cancel: %+v", st)
	}
}

// Cancel, Finish and Chunk are serialized by the import's lock: in either
// order, and at once, the import ends consistent: cancelled without files,
// or processing (then published) with the cancel refused.
func TestCancelRacesFinishAndChunk(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	ctx := context.Background()
	data, fp := opmlOf(2, 0)
	// Finish first: the cancel is refused.
	a := otherAccount(t, e)
	im := reserveSent(t, r, a, data, fp, len(data))
	if _, err := r.Finish(ctx, allow, a, im.ImportID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Cancel(ctx, allow, a, im.ImportID); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("cancel after finish: %v", err)
	}
	if done := waitFor(t, r, a, im.ImportID); done.Status != "published" {
		t.Fatalf("finished import %+v", done)
	}
	// Cancel first: the finish answers the failed import, a chunk is refused.
	b := otherAccount(t, e)
	im = reserveSent(t, r, b, data, fp, len(data))
	if _, err := r.Cancel(ctx, allow, b, im.ImportID); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Finish(ctx, allow, b, im.ImportID); err != nil || got.Status != "failed" {
		t.Fatalf("finish after cancel: %+v %v", got, err)
	}
	if err := r.Chunk(ctx, allow, b, im.ImportID, 0, data); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("chunk after cancel: %v", err)
	}
	if files := importFiles(t, r, im.ImportID); len(files) != 0 {
		t.Errorf("files after cancel: %v", files)
	}
	// At once.
	for i := range 8 {
		acct := otherAccount(t, e)
		im := reserveSent(t, r, acct, data, fp, len(data))
		var wg sync.WaitGroup
		var cancelErr, finishErr, chunkErr error
		wg.Add(3)
		go func() { defer wg.Done(); _, cancelErr = r.Cancel(ctx, allow, acct, im.ImportID) }()
		go func() { defer wg.Done(); _, finishErr = r.Finish(ctx, allow, acct, im.ImportID) }()
		go func() { defer wg.Done(); chunkErr = r.Chunk(ctx, allow, acct, im.ImportID, 0, data) }()
		wg.Wait()
		if finishErr != nil || (chunkErr != nil && !errors.Is(chunkErr, ErrNotUploading)) {
			t.Fatalf("race %d: finish %v, chunk %v", i, finishErr, chunkErr)
		}
		done := waitFor(t, r, acct, im.ImportID)
		switch {
		case cancelErr == nil:
			if done.Status != "failed" || done.Reason != "cancelled" {
				t.Fatalf("race %d: cancelled, but %+v", i, done)
			}
			if files := importFiles(t, r, im.ImportID); len(files) != 0 {
				t.Fatalf("race %d: files after cancel: %v", i, files)
			}
		case errors.Is(cancelErr, ErrNotUploading):
			if done.Status != "published" {
				t.Fatalf("race %d: cancel refused, but %+v", i, done)
			}
		default:
			t.Fatalf("race %d: cancel %v", i, cancelErr)
		}
	}
}

// A cancelled upload whose chunks cannot be removed is still cancelled; its
// files stay pending (the service page lists them), and once they can go the
// next start removes them.
func TestCancelDeletionFailureStaysPendingAndRestartCleans(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	data, fp := opmlOf(2, 0)
	im := reserveSent(t, r, e.account, data, fp, len(data))
	dir := r.uploadDir(im.ImportID)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	got, err := r.Cancel(context.Background(), allow, e.account, im.ImportID)
	if err != nil || got.Status != "failed" || got.Reason != "cancelled" {
		t.Fatalf("cancel: %+v %v", got, err)
	}
	if p := r.PendingCleanup(); len(p) == 0 {
		t.Fatal("nothing pending")
	}
	r.Stop()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) || len(r.PendingCleanup()) != 0 {
		t.Errorf("after a start: %v, pending %v", err, r.PendingCleanup())
	}
}

// The chunks an upload holds are its durable chunk files, by index with
// their lengths: not a temporary still being written, nor anything else.
func TestReceivedListsOnlyDurableChunks(t *testing.T) {
	e := newEnv(t)
	a := otherAccount(t, e) // left with its upload
	r := e.runner(t)
	ctx := context.Background()
	data, fp := opmlOf(2, 0)
	im, _, err := r.Reserve(ctx, allow, a, "opml", fp, int64(len(data)), "subscriptions.opml")
	if err != nil {
		t.Fatal(err)
	}
	if st := status(t, r, a, im.ImportID); st.Import.Status != "uploading" || len(st.Chunks) != 0 || st.Chunks == nil {
		t.Fatalf("nothing sent yet: %+v", st)
	}
	for _, n := range []int{2, 0} {
		if err := r.Chunk(ctx, allow, a, im.ImportID, n, data[n*5:n*5+5]); err != nil {
			t.Fatal(err)
		}
	}
	dir := r.uploadDir(im.ImportID)
	for name, body := range map[string]string{".000001.12345": "temp", "notes": "x", "1": "short"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "000003"), 0o700); err != nil {
		t.Fatal(err)
	}
	st := status(t, r, a, im.ImportID)
	if !slices.Equal(st.Chunks, [][2]int64{{0, 5}, {2, 5}}) || st.Received != 10 || st.Import.Result != "" {
		t.Fatalf("received %+v", st)
	}
}

// An upload begun with other chunk boundaries (4 MiB, or 1 MiB) shows them in
// its chunk list, stale indices included, so a client sending 3 MiB chunks
// sees it cannot continue it: sending its own missing chunks, or all of them
// again, is refused part-way. Cancelling and reserving again uploads it.
func TestChangedChunkBoundariesDetected(t *testing.T) {
	const mib = 1 << 20
	e := newEnv(t)
	r := e.runner(t)
	ctx := context.Background()
	data, fp := opmlOf(3, 12*mib)
	size := int64(len(data))
	for _, c := range []struct {
		name    string
		chunk   int
		sent    int
		resend  []int // the 3 MiB chunks sent over them: the missing ones, or all
		refused int   // the index refused
		visible [][2]int64
	}{
		{"4 to 3 MiB", 4 * mib, 2, []int{2, 3}, 3, [][2]int64{{0, 4 * mib}, {1, 4 * mib}}},
		{"1 to 3 MiB", mib, 6, []int{0, 1, 2, 3}, 3, [][2]int64{{0, mib}, {1, mib}, {2, mib}, {3, mib}, {4, mib}, {5, mib}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			im, created, err := r.Reserve(ctx, allow, e.account, "opml", fp, size, "subscriptions.opml")
			if err != nil || !created {
				t.Fatalf("reserve: %v %v", created, err)
			}
			for n := range c.sent {
				if err := r.Chunk(ctx, allow, e.account, im.ImportID, n, data[n*c.chunk:(n+1)*c.chunk]); err != nil {
					t.Fatal(err)
				}
			}
			st := status(t, r, e.account, im.ImportID)
			if !slices.Equal(st.Chunks, c.visible) || webLayout(size, st.Chunks, 3*mib) {
				t.Fatalf("chunks %v", st.Chunks)
			}
			// 3 MiB chunks sent over them are refused before the file is complete.
			var refused error
			at := -1
			for _, n := range c.resend {
				if refused = r.Chunk(ctx, allow, e.account, im.ImportID, n, data[n*3*mib:min(len(data), (n+1)*3*mib)]); refused != nil {
					at = n
					break
				}
			}
			if !errors.Is(refused, ErrTooLarge) || at != c.refused {
				t.Fatalf("resend refused at %d: %v", at, refused)
			}
			if st := status(t, r, e.account, im.ImportID); webLayout(size, st.Chunks, 3*mib) {
				t.Fatalf("after the refused resend %v", st.Chunks)
			}
			if _, err := r.Cancel(ctx, allow, e.account, im.ImportID); err != nil {
				t.Fatal(err)
			}
		})
	}
	im := reserveSent(t, r, e.account, data, fp, 3*mib)
	if st := status(t, r, e.account, im.ImportID); !webLayout(size, st.Chunks, 3*mib) || st.Received != size {
		t.Fatalf("the new upload %v", st.Chunks)
	}
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); err != nil {
		t.Fatal(err)
	}
	if done := waitFor(t, r, e.account, im.ImportID); done.Status != "published" {
		t.Fatalf("import %+v", done)
	}
}

// A chunk index above the old bound of 513 is taken (a 2 GiB file needs 683
// chunks of 3 MiB, 2048 of 1 MiB); 2048 is not.
func TestChunkIndexAboveOldBound(t *testing.T) {
	e := newEnv(t)
	a := otherAccount(t, e) // left with its upload
	r := e.runner(t)
	ctx := context.Background()
	im, _, err := r.Reserve(ctx, allow, a, "opml", strings.Repeat("0", 64), 4096, "subscriptions.opml")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{514, 683, MaxChunks - 1} {
		if err := r.Chunk(ctx, allow, a, im.ImportID, n, []byte("x")); err != nil {
			t.Fatalf("chunk %d: %v", n, err)
		}
	}
	if err := r.Chunk(ctx, allow, a, im.ImportID, MaxChunks, []byte("x")); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("chunk %d: %v", MaxChunks, err)
	}
	if st := status(t, r, a, im.ImportID); !slices.Equal(st.Chunks, [][2]int64{{514, 1}, {683, 1}, {MaxChunks - 1, 1}}) {
		t.Fatalf("chunks %v", st.Chunks)
	}
}

// countingCohort is refresh.Cohort, counted.
type countingCohort struct {
	mu    sync.Mutex
	calls int
}

func (c *countingCohort) cohort(ctx context.Context, store *storage.Store, account string) ([]string, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return refresh.Cohort(ctx, store, account)
}

func (c *countingCohort) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// A published import lists its first refresh's podcasts once, at
// publication: polls read only its row and the counters (here 120 podcasts,
// 100 polls, one listing), and the refresher's ended attempts move the counts.
func TestProgressNeverScansPerPoll(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	cc := &countingCohort{}
	r.Cohort = cc.cohort
	ctx := context.Background()
	data, fp := opmlOf(120, 0)
	im := reserveSent(t, r, e.account, data, fp, len(data))
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); err != nil {
		t.Fatal(err)
	}
	if done := waitFor(t, r, e.account, im.ImportID); done.Status != "published" {
		t.Fatalf("import %+v", done)
	}
	members, err := refresh.Cohort(ctx, e.svc.Store, e.account)
	if err != nil || len(members) != 120 {
		t.Fatalf("members %d %v", len(members), err)
	}
	calls := cc.count()
	for i := range 100 {
		if i < len(members) {
			r.AfterAttempt(e.account, members[i], i%10 != 0)
		}
		st := status(t, r, e.account, im.ImportID)
		if st.Refresh == nil || st.Refresh.Status != "running" || st.Refresh.Total != 120 || st.Refresh.Attempted != i+1 || st.Import.Result != "" {
			t.Fatalf("poll %d: %+v", i, st.Refresh)
		}
	}
	if calls != 1 || cc.count() != 1 {
		t.Fatalf("Cohort called %d times at publication, %d in all", calls, cc.count())
	}
	for _, p := range members[100:] {
		r.AfterAttempt(e.account, p, true)
	}
	if st := status(t, r, e.account, im.ImportID); *st.Refresh != (RefreshProgress{Status: "done", Total: 120, Attempted: 120, Succeeded: 110,
		Failed: 10, Until: st.Import.UpdatedAt + RefreshReport.Milliseconds()}) {
		t.Fatalf("done: %+v", st.Refresh)
	}
}

// phases records a runner's steps.
type phases struct {
	mu  sync.Mutex
	got []string
}

func (p *phases) record(_ string, ph *Phase) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ph == nil {
		p.got = append(p.got, "-")
	} else if ph.Name == "importing" {
		p.got = append(p.got, fmt.Sprintf("importing %d/%d", ph.Done, ph.Total))
	} else {
		p.got = append(p.got, ph.Name)
	}
}

// A processing import reports its step: reading, importing (podcasts done
// of all), finishing. After a restart, and while it waits to try again, its
// step is not known (null, never zero); a resumed import counts on from the
// podcasts already staged.
func TestProcessingProgressCountersAndRestart(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	data, fp := fileOf(t, fixtureDB)
	broken := &Runner{Store: e.svc.Store, Service: e.svc, Dir: filepath.Join(t.TempDir(), "imports"),
		OpenStaging: func(context.Context, string) (*storage.Store, error) { return nil, errors.New("disk unavailable") }}
	var seen phases
	broken.onPhase = seen.record
	if err := broken.Start(ctx); err != nil {
		t.Fatal(err)
	}
	im := upload(t, broken, e.account, data, fp, "android_db")
	// Its first try failed: it waits to try again, its step unknown.
	deadline := time.Now().Add(30 * time.Second)
	for {
		seen.mu.Lock()
		n := len(seen.got)
		seen.mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no try")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st := status(t, broken, e.account, im.ImportID); st.Import.Status != "processing" || st.Processing != nil {
		t.Fatalf("waiting: %+v", st)
	}
	broken.Stop()
	if !slices.Equal(seen.got, []string{"reading", "-"}) {
		t.Fatalf("the failed try's steps %v", seen.got)
	}
	// Stage the first three podcasts, as a run that crashed after them.
	staging, err := StagingOpener(e.svc.Store)(ctx, broken.stagingDir(im.ImportID))
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{Store: e.svc.Store, Service: e.svc, Dir: broken.Dir}
	if _, err := r.stagingBase(ctx, staging, e.account); err != nil {
		t.Fatal(err)
	}
	lib, err := androiddb.Read(ctx, fixtureDB, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := FromAndroid(lib, im.ImportID, im.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	partial := plan
	partial.Podcasts = plan.Podcasts[:3]
	stagingSvc := *e.svc
	stagingSvc.Store = staging
	var applied []int
	if _, err := Apply(ctx, &stagingSvc, e.account, partial, 0, func(done int) { applied = append(applied, done) }); err != nil {
		t.Fatal(err)
	}
	staging.Close()
	if !slices.Equal(applied, []int{1, 2, 3}) {
		t.Fatalf("Apply's progress %v", applied)
	}
	// Just restarted: not known.
	if st := status(t, r, e.account, im.ImportID); st.Import.Status != "processing" || st.Processing != nil {
		t.Fatalf("after a restart: %+v", st)
	}
	var resumed phases
	r.onPhase = resumed.record
	r.OpenStaging = StagingOpener(e.svc.Store)
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if done := waitFor(t, r, e.account, im.ImportID); done.Status != "published" {
		t.Fatalf("resumed import %+v", done)
	}
	r.Stop()
	want := []string{"reading"}
	for i := 3; i <= len(plan.Podcasts); i++ {
		want = append(want, fmt.Sprintf("importing %d/%d", i, len(plan.Podcasts)))
	}
	want = append(want, "finishing", "-")
	if !slices.Equal(resumed.got, want) {
		t.Fatalf("steps %v, want %v", resumed.got, want)
	}
	if st := status(t, r, e.account, im.ImportID); st.Processing != nil {
		t.Fatalf("published: %+v", st)
	}
}

// publishWith imports an OPML of n podcasts with a runner whose Cohort lists
// members, and returns the published import.
func publishWith(t *testing.T, e *env, r *Runner, n int, members func() []string) storage.Import {
	t.Helper()
	r.Cohort = func(context.Context, *storage.Store, string) ([]string, error) { return members(), nil }
	ctx := context.Background()
	data, fp := opmlOf(n, 0)
	im := reserveSent(t, r, e.account, data, fp, len(data))
	if _, err := r.Finish(ctx, allow, e.account, im.ImportID); err != nil {
		t.Fatal(err)
	}
	done := waitFor(t, r, e.account, im.ImportID)
	if done.Status != "published" {
		t.Fatalf("import %+v", done)
	}
	return done
}

// testNow is a clock the runner reads.
type testNow struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testNow) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testNow) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// A first refresh counts each podcast once: attempted when one fetch ended,
// succeeded when one succeeded (a later failure does not undo it, a later
// success turns a failure into one); a podcast that stops being refreshed
// stays not attempted, so the report expires incomplete, and attempts after
// its end no longer count; then it is forgotten.
func TestRefreshCohortCountsAttemptsSuccessesFailuresAndExpiry(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	clock := &testNow{t: e.svc.Store.Now()}
	r.Now = clock.now
	members := []string{"a", "b", "c", "d"}
	im := publishWith(t, e, r, 2, func() []string { return members })
	until := im.UpdatedAt + RefreshReport.Milliseconds()
	check := func(what string, want RefreshProgress) {
		t.Helper()
		st := status(t, r, e.account, im.ImportID)
		if st.Refresh == nil || *st.Refresh != want {
			t.Fatalf("%s: %+v, want %+v", what, st.Refresh, want)
		}
	}
	check("at publication", RefreshProgress{Status: "running", Total: 4, Until: until})
	r.AfterAttempt(e.account, "a", false)
	r.AfterAttempt(e.account, "b", true)
	r.AfterAttempt(e.account, "not-a-member", true)
	check("one failed, one refreshed", RefreshProgress{Status: "running", Total: 4, Attempted: 2, Succeeded: 1, Failed: 1, Until: until})
	r.AfterAttempt(e.account, "a", true)  // a retry that succeeds
	r.AfterAttempt(e.account, "b", false) // a later failure
	r.AfterAttempt(e.account, "c", false)
	check("later attempts", RefreshProgress{Status: "running", Total: 4, Attempted: 3, Succeeded: 2, Failed: 1, Until: until})
	// d is never attempted (unsubscribed, say): the report expires incomplete.
	clock.set(time.UnixMilli(until))
	check("expired", RefreshProgress{Status: "expired", Total: 4, Attempted: 3, Succeeded: 2, Failed: 1, Until: until})
	r.AfterAttempt(e.account, "d", true)
	r.AfterAttempt(e.account, "c", true)
	check("after the end", RefreshProgress{Status: "expired", Total: 4, Attempted: 3, Succeeded: 2, Failed: 1, Until: until})
	clock.set(time.UnixMilli(im.UpdatedAt + refreshKeep.Milliseconds()))
	if st := status(t, r, e.account, im.ImportID); st.Refresh != nil {
		t.Fatalf("forgotten: %+v", st.Refresh)
	}
}

// emptyFeedBackup is the first-journey backup with its one feed's episodes
// removed and keep-updated off: imported, its podcast is subscribed, not
// scheduled, and awaits its first population, which makes it leave the
// schedule's eligible podcasts.
func emptyFeedBackup(t *testing.T) ([]byte, string) {
	t.Helper()
	return craftedFile(t, firstJourney, "DELETE FROM SimpleChapters", "DELETE FROM Queue", "DELETE FROM Favorites", "DELETE FROM FeedMedia",
		"DELETE FROM FeedItems", "UPDATE Feeds SET keep_updated = 0")
}

// feedFetcher answers every fetch with a feed titled Show without episodes.
type feedFetcher struct{}

func (feedFetcher) Fetch(context.Context, feed.FetchRequest) (*feed.FetchResult, error) {
	return &feed.FetchResult{Body: []byte(`<rss version="2.0"><channel><title>Show</title></channel></rss>`)}, nil
}

// importWithHook imports data (an Android backup) into the env's account with
// the real cohort listing; afterPublish runs right after the publication
// commits, before anything else of the import's. It returns the published
// import once afterPublish has run.
func importWithHook(t *testing.T, e *env, r *Runner, data []byte, fp string, afterPublish func()) storage.Import {
	t.Helper()
	r.Cohort = refresh.Cohort
	hooked := make(chan struct{})
	r.afterPublish = func(string, string) {
		defer close(hooked)
		afterPublish()
	}
	im := upload(t, r, e.account, data, fp, "android_db")
	done := waitFor(t, r, e.account, im.ImportID)
	if done.Status != "published" {
		t.Fatalf("import %+v", done)
	}
	select {
	case <-hooked:
	case <-time.After(2 * time.Minute):
		t.Fatal("the publication's hook did not run")
	}
	return done
}

// The first refresh's podcast is the one eligible at publication, even when
// its first population completes, through the real refresher, right after the
// publication commits and so makes it leave the schedule's eligible podcasts
// before anything else of the import runs: one podcast, attempted and
// refreshed (never "done 0 of 0").
func TestCohortPopulationBeforeFirstRead(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	data, fp := emptyFeedBackup(t)
	attempted := make(chan string, 4)
	ref := refresh.New(e.svc, refresh.Config{Fetcher: feedFetcher{}, AfterAttempt: func(account, podcast string, success bool) {
		r.AfterAttempt(account, podcast, success)
		if success {
			attempted <- podcast
		}
	}})
	ctx := context.Background()
	var populated string
	im := importWithHook(t, e, r, data, fp, func() {
		ref.Start(ctx)
		defer ref.Stop()
		select {
		case populated = <-attempted:
		case <-time.After(60 * time.Second):
			t.Error("no first population")
		}
	})
	if left, err := refresh.Cohort(ctx, e.svc.Store, e.account); err != nil || len(left) != 0 {
		t.Fatalf("eligible now: %v %v (the podcast should have left the schedule's list)", left, err)
	}
	st := status(t, r, e.account, im.ImportID)
	want := RefreshProgress{Status: "done", Total: 1, Attempted: 1, Succeeded: 1, Until: im.UpdatedAt + RefreshReport.Milliseconds()}
	if populated == "" || st.Refresh == nil || *st.Refresh != want {
		t.Fatalf("first refresh %+v, want %+v", st.Refresh, want)
	}
}

// A podcast eligible at publication that leaves eligibility before any
// attempt of it (here populated by a fetch the refresher does not report, as
// one turned off before its first fetch is) stays in the first refresh, not
// attempted: the report runs, then expires; it is never done.
func TestCohortEligibilityChangeBeforeAnyAttempt(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	clock := &testNow{t: e.svc.Store.Now()}
	r.Now = clock.now
	data, fp := emptyFeedBackup(t)
	ctx := context.Background()
	im := importWithHook(t, e, r, data, fp, func() {
		var cands []storage.RefreshCandidate
		if err := e.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
			var err error
			cands, err = tx.RefreshCandidates(e.account)
			return err
		}); err != nil || len(cands) != 1 {
			t.Errorf("podcasts %v %v", cands, err)
			return
		}
		e.fetch(cands[0].PodcastID, "out-of-band", "Show")
	})
	if left, err := refresh.Cohort(ctx, e.svc.Store, e.account); err != nil || len(left) != 0 {
		t.Fatalf("eligible now: %v %v", left, err)
	}
	until := im.UpdatedAt + RefreshReport.Milliseconds()
	if st := status(t, r, e.account, im.ImportID); st.Refresh == nil || *st.Refresh != (RefreshProgress{Status: "running", Total: 1, Until: until}) {
		t.Fatalf("after the change: %+v", st.Refresh)
	}
	clock.set(time.UnixMilli(until))
	if st := status(t, r, e.account, im.ImportID); st.Refresh == nil || *st.Refresh != (RefreshProgress{Status: "expired", Total: 1, Until: until}) {
		t.Fatalf("at the end: %+v", st.Refresh)
	}
}

// A finished first refresh's counts go at the periodic sweep once kept long
// enough, without a poll of that import, and at the next publication.
func TestFinishedCohortForgottenWithoutAPoll(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	clock := &testNow{t: e.svc.Store.Now()}
	r.Now = clock.now
	im := publishWith(t, e, r, 1, func() []string { return []string{"a"} })
	r.AfterAttempt(e.account, "a", true)
	count := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.cohorts)
	}
	r.evictRefresh()
	if count() != 1 {
		t.Fatal("forgotten too early")
	}
	clock.set(time.UnixMilli(im.UpdatedAt + refreshKeep.Milliseconds()))
	r.evictRefresh()
	if count() != 0 {
		t.Fatal("kept past refreshKeep")
	}
	// The next publication forgets it too.
	r.mu.Lock()
	r.cohorts[e.account] = &cohort{importID: im.ImportID, members: map[string]outcome{}, since: im.UpdatedAt}
	r.mu.Unlock()
	other := otherAccount(t, e)
	data, fp := opmlOf(1, 0)
	o := reserveSent(t, r, other, data, fp, len(data))
	if _, err := r.Finish(context.Background(), allow, other, o.ImportID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, r, other, o.ImportID)
	r.mu.Lock()
	_, kept := r.cohorts[e.account]
	r.mu.Unlock()
	if kept {
		t.Fatal("kept past refreshKeep after another publication")
	}
}

// Counts are never rebuilt after a restart: a podcast that stopped being
// refreshed before its first attempt stays counted until the restart, and
// afterwards the report says its counts are unavailable (never 1 of 1).
func TestCohortEligibilityChangeThenRestart(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	im := publishWith(t, e, r, 2, func() []string { return []string{"a", "b"} })
	r.AfterAttempt(e.account, "a", true)
	// b's keep-updated is turned off: it is not attempted.
	if st := status(t, r, e.account, im.ImportID); st.Refresh.Status != "running" || st.Refresh.Total != 2 || st.Refresh.Attempted != 1 {
		t.Fatalf("before the restart: %+v", st.Refresh)
	}
	r.Stop()
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.AfterAttempt(e.account, "b", true)
	want := RefreshProgress{Status: "unavailable", Until: im.UpdatedAt + RefreshReport.Milliseconds()}
	if st := status(t, r, e.account, im.ImportID); st.Refresh == nil || *st.Refresh != want {
		t.Fatalf("after the restart: %+v", st.Refresh)
	}
}

// A podcast counted only for its first population (not scheduled) stays
// counted once populated, though a list made now would leave it out; after a
// restart the report is unavailable rather than a smaller total.
func TestCohortPopulatedNonScheduledMemberThenRestart(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	listed := []string{"scheduled", "population-only"}
	im := publishWith(t, e, r, 2, func() []string { return listed })
	r.AfterAttempt(e.account, "population-only", true)
	listed = []string{"scheduled"} // populated: no longer listed
	if st := status(t, r, e.account, im.ImportID); st.Refresh.Total != 2 || st.Refresh.Succeeded != 1 || st.Refresh.Status != "running" {
		t.Fatalf("before the restart: %+v", st.Refresh)
	}
	r.Stop()
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := status(t, r, e.account, im.ImportID); st.Refresh == nil || st.Refresh.Status != "unavailable" || st.Refresh.Total != 0 {
		t.Fatalf("after the restart: %+v", st.Refresh)
	}
}

// An attempt whose outcome the refresher could not write (it reports it as a
// failure) counts as attempted and failed: the counts never depend on the
// schedule rows. A listing that fails leaves the counts unavailable.
func TestCohortAttemptNotPersistedAndListingFailure(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t)
	im := publishWith(t, e, r, 2, func() []string { return []string{"a", "b"} })
	r.AfterAttempt(e.account, "a", false) // the refresher could not write its outcome
	if st := status(t, r, e.account, im.ImportID); st.Refresh.Attempted != 1 || st.Refresh.Failed != 1 || st.Refresh.Status != "running" {
		t.Fatalf("an unwritten attempt: %+v", st.Refresh)
	}
	e2 := newEnv(t)
	r2 := e2.runner(t)
	r2.Cohort = func(context.Context, *storage.Store, string) ([]string, error) {
		return nil, errors.New("storage unavailable")
	}
	ctx := context.Background()
	data, fp := opmlOf(2, 0)
	im2 := reserveSent(t, r2, e2.account, data, fp, len(data))
	if _, err := r2.Finish(ctx, allow, e2.account, im2.ImportID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, r2, e2.account, im2.ImportID)
	if st := status(t, r2, e2.account, im2.ImportID); st.Refresh == nil || st.Refresh.Status != "unavailable" {
		t.Fatalf("a failed listing: %+v", st.Refresh)
	}
}
