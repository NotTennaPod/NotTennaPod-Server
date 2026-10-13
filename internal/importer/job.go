package importer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/backupsched"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Runner runs library imports (PROTOCOL §11.5). A file arrives in chunks under <Dir>/<id>.upload/;
// once verified it is assembled as <Dir>/<id>.file and staged in a private database under
// <Dir>/<id>.staging/, built against the account's own log and applied with the live server's
// keys; publication copies the staged library into the live store in one transaction. Every step
// is durable before the next, so a crash resumes or returns the same import.
type Runner struct {
	Store   *storage.Store
	Service *protocol.Service // the live service: its registry, secrets box and clock
	Dir     string
	// OpenStaging opens or creates a staging store in dir that signs with the
	// live server's receipt keys (storage.OpenStaging).
	OpenStaging func(ctx context.Context, dir string) (*storage.Store, error)
	Log         *slog.Logger
	// RetryWait is the pause before an import retries after a storage or
	// disk error (default a minute).
	RetryWait time.Duration
	// FreeBytes reports the free space of the file system holding dir
	// (default backupsched.FreeBytes).
	FreeBytes func(dir string) (int64, error)
	// Limit bounds one try of an import: reading, planning, staging and
	// publication (default ProcessLimit).
	Limit time.Duration
	// SweepEvery is how often the files of imports no longer live (deleted
	// or finished) that are still there are removed again (default
	// SweepInterval).
	SweepEvery time.Duration

	mu        sync.Mutex
	running   map[string]*work       // processing imports
	discarded map[string]bool        // imports of deleted accounts (Discard)
	pending   map[string][]string    // per import: its files the last removal could not remove
	locks     map[string]*sync.Mutex // per import: chunks, assembly and status changes
	turn      chan struct{}          // one import processes at a time on the server
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup // import work
	sweeping  sync.WaitGroup // the periodic sweep
}

// StagingOpener opens staging databases that carry the live server's identity
// and sign with its receipt keys (storage.OpenStaging), so a published
// import's decisions verify on the live server.
func StagingOpener(live *storage.Store) func(context.Context, string) (*storage.Store, error) {
	return func(ctx context.Context, dir string) (*storage.Store, error) {
		id, err := live.ServerID(ctx)
		if err != nil {
			return nil, err
		}
		keys, err := live.ReceiptKeys(ctx)
		if err != nil {
			return nil, err
		}
		return storage.OpenStaging(ctx, dir, id, keys, storage.WithClock(live.Now))
	}
}

// Limits.
const (
	MaxChunk     = 4 << 20          // a chunk's bytes: the request bound (cp4)
	MaxFile      = 2 << 30          // a whole file
	ProcessLimit = 30 * time.Minute // one try: reading, planning, staging and publication
	// MaxAttempts bounds an import's tries, counted across restarts: a try
	// that a stop interrupts does not count.
	MaxAttempts = 3
	// SweepInterval is the default of Runner.SweepEvery.
	SweepInterval = 10 * time.Minute
)

// work is a processing import's goroutine: its cancel, and done closed when
// it has ended.
type work struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Authorize re-validates the caller inside a write transaction (PROTOCOL §5: a revocation or a
// disabled account landing meanwhile wins).
type Authorize func(tx *storage.WriteTx) error

// lock serializes an import's chunks, assembly and status changes.
func (r *Runner) lock(importID string) func() {
	r.mu.Lock()
	if r.locks == nil {
		r.locks = map[string]*sync.Mutex{}
	}
	m := r.locks[importID]
	if m == nil {
		m = &sync.Mutex{}
		r.locks[importID] = m
	}
	r.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// Errors a caller maps to responses.
var (
	ErrNotFound     = errors.New("importer: no such import")
	ErrBadRequest   = errors.New("importer: invalid import request")
	ErrNotUploading = errors.New("importer: the import no longer accepts chunks")
	ErrMismatch     = errors.New("importer: the uploaded file does not match its size and fingerprint")
	ErrTooLarge     = errors.New("importer: the parts are larger than the file's size")
	ErrNoSpace      = errors.New("importer: not enough free space on the server")
)

var fingerprintForm = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Start resumes unfinished imports and makes the runner accept new ones.
// Imports of another restore epoch, or whose files are gone, fail and release
// their accounts (PROTOCOL §11.5); files of no live import are deleted.
func (r *Runner) Start(ctx context.Context) error {
	if r.Log == nil {
		r.Log = slog.Default()
	}
	r.mu.Lock()
	r.running = map[string]*work{}
	r.turn = make(chan struct{}, 1)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.mu.Unlock()
	if err := private.RealDir(r.Dir); err != nil {
		return err
	}
	unfinished, err := r.Store.UnfinishedImports(ctx)
	if err != nil {
		return err
	}
	live := map[string]bool{}
	var processing []storage.Import
	for _, im := range unfinished {
		epoch, err := r.epoch(ctx, im.AccountID)
		if err != nil {
			return err
		}
		switch {
		case epoch != im.Epoch:
			err = r.fail(ctx, im, "interrupted_by_restore")
		case im.Status == "uploading" && !exists(r.uploadDir(im.ImportID)):
			err = r.fail(ctx, im, "interrupted")
		case im.Status == "processing" && !exists(r.filePath(im.ImportID)):
			err = r.fail(ctx, im, "interrupted")
		default:
			live[im.ImportID] = true
			if im.Status == "processing" {
				processing = append(processing, im)
			}
		}
		if err != nil {
			return err
		}
	}
	// Files of no live import go (deleted accounts' imports, crashes), and
	// temporaries of every import, before any work resumes.
	if err := r.removeOrphans(live, true); err != nil {
		return err
	}
	for _, im := range processing {
		r.launch(im.AccountID, im.ImportID)
	}
	every := r.SweepEvery
	if every <= 0 {
		every = SweepInterval
	}
	r.mu.Lock()
	ctx2 := r.ctx
	r.sweeping.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.sweeping.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx2.Done():
				return
			case <-t.C:
				if err := r.sweep(ctx2); err != nil && ctx2.Err() == nil {
					r.logger().Error("import: cannot sweep the import files", "error", logged(err))
				}
			}
		}
	}()
	return nil
}

// sweep removes the files of imports no longer live: those of deleted accounts
// and leftovers of finished imports a removal could not remove. The directory
// is listed before the live imports are read: a file exists only after its
// import's row (Reserve), so a file listed of an import not live then is no
// longer needed.
func (r *Runner) sweep(ctx context.Context) error {
	entries, err := os.ReadDir(r.Dir)
	if err != nil {
		return err
	}
	unfinished, err := r.Store.UnfinishedImports(ctx)
	if err != nil {
		return err
	}
	live := map[string]bool{}
	for _, im := range unfinished {
		live[im.ImportID] = true
	}
	return r.removeOrphansOf(entries, live, false)
}

// removeOrphans removes the files of every import not in live, and at
// startup the temporaries of every import too.
func (r *Runner) removeOrphans(live map[string]bool, startup bool) error {
	entries, err := os.ReadDir(r.Dir)
	if err != nil {
		return err
	}
	return r.removeOrphansOf(entries, live, startup)
}

func (r *Runner) removeOrphansOf(entries []os.DirEntry, live map[string]bool, startup bool) error {
	orphans := map[string]bool{}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		temporary := strings.HasPrefix(name, ".")
		id, _, _ := strings.Cut(strings.TrimPrefix(name, "."), ".")
		switch {
		case !live[id]:
			if !orphans[id] {
				orphans[id] = true
				ids = append(ids, id)
			}
		case startup && temporary:
			if err := os.RemoveAll(filepath.Join(r.Dir, name)); err != nil {
				r.logger().Error("import: cannot remove an import's file", "import", id, "path", filepath.Join(r.Dir, name), "error", err)
			}
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		r.cleanup(id)
	}
	// The runner's own record of what is pending is what this pass found.
	r.mu.Lock()
	for id := range r.pending {
		if !orphans[id] && !live[id] {
			delete(r.pending, id)
		}
	}
	r.mu.Unlock()
	return nil
}

// Stop cancels running imports and waits for them; they resume at the next
// Start.
func (r *Runner) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	r.sweeping.Wait()
}

// Reserve claims the account for an import of this file, or returns the
// account's import of the same file (PROTOCOL §11.5).
func (r *Runner) Reserve(ctx context.Context, authorize Authorize, accountID, kind, fingerprint string, size int64, fileName string) (storage.Import, bool, error) {
	if (kind != "android_db" && kind != "opml") || !fingerprintForm.MatchString(fingerprint) || size <= 0 || size > MaxFile ||
		fileName == "" || len(fileName) > 1024 {
		return storage.Import{}, false, ErrBadRequest
	}
	// The upload, its assembled file and the reader's private copy all fit
	// above the backups' headroom.
	if err := r.room(3 * size); err != nil {
		return storage.Import{}, false, err
	}
	var im storage.Import
	var created bool
	err := r.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if err := authorize(tx); err != nil {
			return err
		}
		var err error
		im, created, err = tx.Account(accountID).ReserveImport(storage.Import{ImportID: newID(), Kind: kind, Fingerprint: fingerprint,
			Size: size, FileName: fileName})
		return err
	})
	if err != nil || !created {
		return im, false, err
	}
	// Under the import's lock, so a deletion of the account meanwhile
	// (Discard) leaves no directory behind.
	defer r.lock(im.ImportID)()
	if r.isDiscarded(im.ImportID) {
		return im, false, ErrNotFound
	}
	return im, true, private.RealDir(r.uploadDir(im.ImportID))
}

// Chunk stores chunk n of an uploading import, durably; sending the same
// chunk again replaces it. The chunks kept never add up to more than the
// reserved size, and each leaves the backups' headroom free.
func (r *Runner) Chunk(ctx context.Context, authorize Authorize, accountID, importID string, n int, data []byte) error {
	if n < 0 || n > MaxFile/MaxChunk+1 || len(data) == 0 || len(data) > MaxChunk {
		return ErrBadRequest
	}
	defer r.lock(importID)()
	var im storage.Import
	err := r.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if err := authorize(tx); err != nil {
			return err
		}
		var err error
		im, err = tx.Account(accountID).Import(importID)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if im.Status != "uploading" {
			return ErrNotUploading
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Every chunk holds at least a byte, so the reservation bounds the index.
	if int64(n) >= im.Size {
		return ErrTooLarge
	}
	name := fmt.Sprintf("%06d", n)
	kept, err := r.uploaded(importID, name)
	if err != nil {
		return err
	}
	if kept+int64(len(data)) > im.Size {
		return ErrTooLarge
	}
	if err := r.room(int64(len(data))); err != nil {
		return err
	}
	if err := writeDurably(filepath.Join(r.uploadDir(importID), name), data); err != nil {
		return err
	}
	// A chunk written for an import that no longer exists (its account
	// deleted meanwhile) does not stay.
	if _, err := r.get(context.WithoutCancel(ctx), accountID, importID); errors.Is(err, ErrNotFound) {
		r.cleanupLocked(importID)
		return ErrNotFound
	}
	return nil
}

// uploaded is the size of the import's kept chunks, without the one named
// except (which a resend replaces).
func (r *Runner) uploaded(importID, except string) (int64, error) {
	entries, err := os.ReadDir(r.uploadDir(importID))
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") || e.Name() == except {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// room refuses a write of need bytes that would leave less free space than
// the backups keep beside the database (PROTOCOL §14). Unknown free
// space refuses too, as the backups do.
func (r *Runner) room(need int64) error {
	free := r.FreeBytes
	if free == nil {
		free = backupsched.FreeBytes
	}
	n, err := free(r.Dir)
	if err != nil {
		return fmt.Errorf("%w: cannot tell the free space: %v", ErrNoSpace, err)
	}
	if keep := backupsched.Headroom(r.Store.Dir()); n-need < keep {
		return fmt.Errorf("%w: %d MB free, %d MB needed", ErrNoSpace, n>>20, (need+keep)>>20)
	}
	return nil
}

// Finish checks the uploaded chunks against the file's size and fingerprint,
// assembles the file and starts processing. A mismatch keeps the chunks for
// a resend.
func (r *Runner) Finish(ctx context.Context, authorize Authorize, accountID, importID string) (storage.Import, error) {
	unlock := r.lock(importID)
	defer unlock()
	im, err := r.get(ctx, accountID, importID)
	if err != nil {
		return im, err
	}
	switch im.Status {
	case "processing", "published", "failed":
		return im, nil // a repeated finish
	}
	if err := r.assemble(im); err != nil {
		return im, err
	}
	err = r.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if err := authorize(tx); err != nil {
			return err
		}
		cur, err := tx.Account(accountID).Import(importID)
		if err != nil {
			return err
		}
		if cur.Status != "uploading" {
			return nil
		}
		return tx.Account(accountID).SetImportStatus(importID, "processing", "")
	})
	if err != nil {
		return im, err
	}
	os.RemoveAll(r.uploadDir(importID))
	r.launch(accountID, importID)
	return r.get(ctx, accountID, importID)
}

// Get returns one of the account's imports.
func (r *Runner) Get(ctx context.Context, accountID, importID string) (storage.Import, error) {
	return r.get(ctx, accountID, importID)
}

// Current returns the account's current import (ErrNotFound for none).
func (r *Runner) Current(ctx context.Context, accountID string) (storage.Import, error) {
	var im storage.Import
	err := r.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		im, err = tx.Account(accountID).CurrentImport()
		return err
	})
	if errors.Is(err, storage.ErrNotFound) {
		return im, ErrNotFound
	}
	return im, err
}

func (r *Runner) get(ctx context.Context, accountID, importID string) (storage.Import, error) {
	var im storage.Import
	err := r.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		im, err = tx.Account(accountID).Import(importID)
		return err
	})
	if errors.Is(err, storage.ErrNotFound) {
		return im, ErrNotFound
	}
	return im, err
}

// assemble concatenates the chunks, which must be 0..k without gaps, into the import's file when
// their size and SHA-256 match. The free space is checked before each chunk is copied, for what
// remains to be written, and before the file is published, as each chunk's upload checks it:
// another writer may take the room meanwhile. Below the reserve the partial file goes and the
// chunks stay for a retry.
func (r *Runner) assemble(im storage.Import) error {
	if exists(r.filePath(im.ImportID)) {
		return nil
	}
	entries, err := os.ReadDir(r.uploadDir(im.ImportID))
	if err != nil {
		return ErrMismatch
	}
	var names []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	// The size and the free space are checked before anything is copied.
	var total int64
	for i, name := range names {
		info, err := os.Stat(filepath.Join(r.uploadDir(im.ImportID), name))
		if name != fmt.Sprintf("%06d", i) || err != nil {
			return ErrMismatch
		}
		total += info.Size()
	}
	if total != im.Size {
		return ErrMismatch
	}
	if err := r.room(im.Size); err != nil {
		return err
	}
	out, err := os.CreateTemp(r.Dir, "."+im.ImportID+".file.*") // exclusive, 0600
	if err != nil {
		return err
	}
	tmp := out.Name()
	defer os.Remove(tmp)
	h := sha256.New()
	var size int64
	for i, name := range names {
		if name != fmt.Sprintf("%06d", i) {
			out.Close()
			return ErrMismatch
		}
		if err := r.room(im.Size - size); err != nil {
			out.Close()
			return err
		}
		f, err := os.Open(filepath.Join(r.uploadDir(im.ImportID), name))
		if err != nil {
			out.Close()
			return err
		}
		n, err := io.Copy(io.MultiWriter(out, h), f)
		f.Close()
		if err != nil {
			out.Close()
			return err
		}
		size += n
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	out.Close()
	if size != im.Size || hex.EncodeToString(h.Sum(nil)) != im.Fingerprint {
		return ErrMismatch
	}
	if err := r.room(0); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.filePath(im.ImportID)); err != nil {
		return err
	}
	return syncDir(r.Dir)
}

func (r *Runner) launch(accountID, importID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running == nil || r.running[importID] != nil || r.ctx == nil || r.discarded[importID] {
		return
	}
	ctx, cancel := context.WithCancel(r.ctx)
	w := &work{cancel: cancel, done: make(chan struct{})}
	r.running[importID] = w
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(w.done)
		defer func() {
			r.mu.Lock()
			delete(r.running, importID)
			r.mu.Unlock()
			cancel()
		}()
		r.process(ctx, accountID, importID)
	}()
}

// Discard ends imports of an account being deleted, after the deleting
// transaction removed their rows: it cancels their processing and waits for it,
// then removes their files (chunks, the assembled file, staging and tries). An
// upload or a finish still running cannot recreate one: they hold the import's
// lock, which Discard takes before it removes the files, and they find the
// import gone afterwards. A crash before Discard leaves files of imports
// without rows, which Start deletes. It reports whether every file went; those
// that did not stay pending (PendingCleanup), and the sweep and every start
// remove them again.
func (r *Runner) Discard(importIDs []string) bool {
	removed := true
	for _, id := range importIDs {
		r.mu.Lock()
		if r.discarded == nil {
			r.discarded = map[string]bool{}
		}
		r.discarded[id] = true
		w := r.running[id]
		r.mu.Unlock()
		if w != nil {
			w.cancel()
			<-w.done
		}
		if !r.cleanup(id) {
			removed = false
		}
	}
	return removed
}

// PendingCleanup lists the files of imports no longer live (deleted
// accounts' or finished ones) that could not be removed, by path, sorted;
// the service page shows them.
func (r *Runner) PendingCleanup() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, paths := range r.pending {
		out = append(out, paths...)
	}
	sort.Strings(out)
	return out
}

func (r *Runner) isDiscarded(importID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.discarded[importID]
}

// fileFailure is a file that can never import: the import fails with it.
type fileFailure struct{ reason string }

func (f *fileFailure) Error() string { return f.reason }

// panicked is a panic while processing an import, recovered: its stack only,
// never its text.
type panicked struct{ stack []byte }

func (p *panicked) Error() string { return "panic" }

// process reads, plans, stages and publishes one import. A file or data
// error fails it with its message, releasing the account; so does a panic,
// which never stops the server. A stop leaves it processing, to resume at
// the next start. Any other error (storage, disk) is retried after
// RetryWait, and fails the import once MaxAttempts tries, counted across
// restarts, have not finished it.
func (r *Runner) process(ctx context.Context, accountID, importID string) {
	for {
		// One import processes at a time on the server; the others wait,
		// still processing.
		release, ok := r.takeTurn(ctx)
		if !ok {
			return
		}
		retry := r.try(ctx, accountID, importID)
		release()
		if !retry {
			return
		}
		wait := r.RetryWait
		if wait == 0 {
			wait = time.Minute
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// takeTurn waits for the server's import turn (no limit before Start).
func (r *Runner) takeTurn(ctx context.Context) (func(), bool) {
	r.mu.Lock()
	turn := r.turn
	r.mu.Unlock()
	if turn == nil {
		return func() {}, true
	}
	select {
	case turn <- struct{}{}:
		return func() { <-turn }, true
	case <-ctx.Done():
		return nil, false
	}
}

// try runs one counted try of an import, records its outcome, and reports
// whether it is to be tried again.
func (r *Runner) try(ctx context.Context, accountID, importID string) bool {
	tries, err := r.countTry(importID)
	if err != nil {
		r.Log.Error("import: will resume at the next start", "import", importID, "error", logged(err))
		return false
	}
	if tries > MaxAttempts {
		r.failNow(accountID, importID, fmt.Sprintf("interrupted: the import was stopped %d times before it could finish", MaxAttempts))
		return false
	}
	err = r.processSafely(ctx, accountID, importID)
	var ff *fileFailure
	var p *panicked
	switch {
	case err == nil:
		return false
	case ctx.Err() != nil:
		r.uncountTry(importID, tries) // a stop is not a try
		return false
	case errors.As(err, &ff):
		r.failNow(accountID, importID, ff.reason)
		return false
	case errors.As(err, &p):
		// A panic's text never leaves it (it can quote anything): the log
		// has a fixed message and the stack.
		r.Log.Error("import: failed on an internal error", "import", importID, "stack", string(p.stack))
		r.failNow(accountID, importID, serverError)
		return false
	case errors.Is(err, storage.ErrImportChanged):
		r.failNow(accountID, importID, "account_changed")
		return false
	case errors.Is(err, contract.ErrNotCanonicalizable), errors.Is(err, protocol.ErrInvalidPayload):
		r.Log.Error("import: the file holds a value the server cannot store", "import", importID, "error", logged(err))
		r.failNow(accountID, importID, "invalid_file: the file holds a value the server cannot store")
		return false
	case tries >= MaxAttempts && errors.Is(err, errStagingTime):
		r.failNow(accountID, importID, fmt.Sprintf("too_slow: the library could not be imported in %d tries", tries))
		return false
	case tries >= MaxAttempts:
		r.Log.Error("import: failed after retries", "import", importID, "error", logged(err))
		r.failNow(accountID, importID, serverError)
		return false
	}
	r.Log.Error("import: will retry", "import", importID, "error", logged(err))
	return true
}

// serverError is the reason an import shows its owner for any failure that is
// not the file's: a fixed message, as an internal error can name the server's
// paths. The error itself goes to the server's log (logged).
const serverError = "server_error: the server could not finish the import; its log has the details"

// logged is an error as the server's log keeps it: without URL userinfo.
func logged(err error) string { return ops.StripUserinfo(err.Error()) }

// processSafely is processOnce with any panic recovered as an error.
func (r *Runner) processSafely(ctx context.Context, accountID, importID string) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &panicked{stack: debug.Stack()}
		}
	}()
	return r.processOnce(ctx, accountID, importID)
}

// failNow fails a processing import with reason.
func (r *Runner) failNow(accountID, importID, reason string) {
	im, err := r.get(context.Background(), accountID, importID)
	if err == nil && im.Status == "processing" {
		err = r.fail(context.Background(), im, reason)
	}
	if err != nil {
		r.Log.Error("import: recording a failure", "import", importID, "error", logged(err))
	}
}

// countTry records the start of a try, durably, and returns its number.
func (r *Runner) countTry(importID string) (int, error) {
	n := 0
	if b, err := os.ReadFile(r.triesPath(importID)); err == nil {
		n, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	return n + 1, writeDurably(r.triesPath(importID), []byte(strconv.Itoa(n+1)))
}

// uncountTry takes back try n, which a stop interrupted.
func (r *Runner) uncountTry(importID string, n int) {
	if err := writeDurably(r.triesPath(importID), []byte(strconv.Itoa(n-1))); err != nil {
		r.Log.Error("import: recording a stop", "import", importID, "error", logged(err))
	}
}

// errStagingTime is a try whose time ran out while staging: what it staged
// is kept, and the next try goes on from there.
var errStagingTime = errors.New("importer: the try's time ran out while staging")

func (r *Runner) processOnce(ctx context.Context, accountID, importID string) error {
	im, err := r.get(ctx, accountID, importID)
	if err != nil || im.Status != "processing" {
		return err
	}
	// One try is bounded, staging included. A file that cannot be read and planned in time
	// fails; a try that runs out of time while staging is tried again from
	// its progress, a bounded number of times.
	limit := r.Limit
	if limit <= 0 {
		limit = ProcessLimit
	}
	tryCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	plan, err := r.plan(tryCtx, im)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return &fileFailure{"too_slow: the file could not be read in time"}
	}
	if err != nil {
		return err
	}
	err = r.stageAndPublish(tryCtx, accountID, importID, im, plan)
	if err != nil && tryCtx.Err() != nil && ctx.Err() == nil {
		return fmt.Errorf("%w: %v", errStagingTime, err)
	}
	return err
}

func (r *Runner) stageAndPublish(ctx context.Context, accountID, importID string, im storage.Import, plan Plan) error {
	var err error
	stagingDir := r.stagingDir(importID)
	staging, err := r.OpenStaging(ctx, stagingDir)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			staging.Close()
		}
	}()
	base, err := r.stagingBase(ctx, staging, accountID)
	if err != nil {
		return err
	}
	svc := &protocol.Service{Store: staging, Registry: r.Service.Registry, Box: r.Service.Box, Now: r.Service.Now}
	from, err := Progress(ctx, staging, accountID)
	if err != nil {
		return err
	}
	if _, err := Apply(ctx, svc, accountID, plan, from); err != nil {
		return err
	}
	res, err := Summarize(ctx, staging, accountID, plan)
	if err != nil {
		return err
	}
	result, err := WriteManifest(ctx, svc, accountID, importID, im.Fingerprint, plan, res)
	if err != nil {
		return err
	}
	if err := staging.Close(); err != nil {
		return err
	}
	closed = true
	if err := r.Store.PublishImport(ctx, filepath.Join(stagingDir, "nottennapod.db"), accountID, importID, base, result); err != nil {
		return err
	}
	r.cleanup(importID)
	return nil
}

// plan reads the assembled file and plans it, deterministically: the same
// file, import ID and reservation time give the same plan on a resume.
func (r *Runner) plan(ctx context.Context, im storage.Import) (Plan, error) {
	path := r.filePath(im.ImportID)
	switch im.Kind {
	case "opml":
		data, err := os.ReadFile(path)
		if err != nil {
			return Plan{}, err
		}
		entries, err := ReadOPML(ctx, data)
		if ctx.Err() != nil {
			return Plan{}, ctx.Err()
		}
		if err != nil {
			return Plan{}, &fileFailure{"not_opml: " + err.Error()}
		}
		return FromOPML(entries, im.ImportID, im.CreatedAt), nil
	default:
		read := filepath.Join(r.stagingDir(im.ImportID), "read")
		defer os.RemoveAll(read)
		lib, err := androiddb.Read(ctx, path, read)
		var inv *androiddb.InvalidError
		switch {
		case errors.Is(err, androiddb.ErrNewer):
			return Plan{}, &fileFailure{"newer_version: " + err.Error()}
		case errors.As(err, &inv):
			return Plan{}, &fileFailure{"invalid_file: " + inv.Message}
		case err != nil:
			return Plan{}, err
		}
		return FromAndroid(lib, im.ImportID, im.CreatedAt)
	}
}

// stagingBase mirrors the account into a fresh staging store at the live
// head (recorded there), or returns the head an earlier attempt recorded.
func (r *Runner) stagingBase(ctx context.Context, staging *storage.Store, accountID string) (storage.Head, error) {
	var base storage.Head
	if v, err := staging.Meta(ctx, "import_base"); err != nil {
		return base, err
	} else if v != "" {
		return base, json.Unmarshal([]byte(v), &base)
	}
	var namespace string
	err := r.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		if base, err = tx.Account(accountID).Head(); err != nil {
			return err
		}
		namespace, err = tx.Account(accountID).Namespace()
		return err
	})
	if err != nil {
		return base, err
	}
	b, err := json.Marshal(base)
	if err != nil {
		return base, err
	}
	return base, staging.Write(ctx, func(tx *storage.WriteTx) error {
		if err := tx.MirrorAccount(accountID, namespace, base); err != nil {
			return err
		}
		return tx.SetMeta("import_base", string(b))
	})
}

// fail marks an import failed, releasing the account, and deletes its files
// (they hold credentials). The reason keeps no URL userinfo (the feed
// fields' rule).
func (r *Runner) fail(ctx context.Context, im storage.Import, reason string) error {
	reason = ops.StripUserinfo(reason)
	err := r.Store.Write(ctx, func(tx *storage.WriteTx) error {
		cur, err := tx.Account(im.AccountID).Import(im.ImportID)
		if err != nil || cur.Status == "published" || cur.Status == "failed" {
			return err
		}
		return tx.Account(im.AccountID).SetImportStatus(im.ImportID, "failed", reason)
	})
	if err == nil {
		r.cleanup(im.ImportID)
	}
	return err
}

// cleanup removes an import's files under its lock (cleanupLocked).
func (r *Runner) cleanup(importID string) bool {
	defer r.lock(importID)()
	return r.cleanupLocked(importID)
}

// cleanupLocked removes an import's files: its chunks, the assembled file,
// staging, tries and their temporaries. A file it cannot remove is logged as an
// error, by its path (never its content), and stays pending: the sweep and
// every start try again. It reports whether every file went.
func (r *Runner) cleanupLocked(importID string) bool {
	paths := []string{r.uploadDir(importID), r.filePath(importID), r.stagingDir(importID), r.triesPath(importID)}
	if entries, err := os.ReadDir(r.Dir); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "."+importID+".") {
				paths = append(paths, filepath.Join(r.Dir, e.Name()))
			}
		}
	}
	var failed []string
	for _, p := range paths {
		if err := os.RemoveAll(p); err != nil {
			failed = append(failed, p)
			r.logger().Error("import: cannot remove an import's file", "import", importID, "path", p, "error", err)
		}
	}
	r.mu.Lock()
	if r.pending == nil {
		r.pending = map[string][]string{}
	}
	if failed == nil {
		delete(r.pending, importID)
	} else {
		r.pending[importID] = failed
	}
	r.mu.Unlock()
	return failed == nil
}

func (r *Runner) logger() *slog.Logger {
	if r.Log == nil {
		return slog.Default()
	}
	return r.Log
}

func (r *Runner) epoch(ctx context.Context, accountID string) (int64, error) {
	var h storage.Head
	err := r.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		h, err = tx.Account(accountID).Head()
		return err
	})
	return h.Epoch, err
}

func (r *Runner) uploadDir(id string) string  { return filepath.Join(r.Dir, id+".upload") }
func (r *Runner) filePath(id string) string   { return filepath.Join(r.Dir, id+".file") }
func (r *Runner) stagingDir(id string) string { return filepath.Join(r.Dir, id+".staging") }
func (r *Runner) triesPath(id string) string  { return filepath.Join(r.Dir, id+".tries") }

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func writeDurably(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
