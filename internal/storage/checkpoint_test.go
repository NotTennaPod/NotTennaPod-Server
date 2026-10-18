package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func walSize(t *testing.T, s *Store) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(s.Dir(), databaseName+"-wal"))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// bulkWrite writes mib MiB of incompressible rows in one transaction.
func bulkWrite(t *testing.T, s *Store, prefix string, mib int) {
	t.Helper()
	err := s.Write(context.Background(), func(tx *WriteTx) error {
		for i := range mib {
			if _, err := tx.tx.Exec(`INSERT INTO server_meta (key, value) VALUES (?, hex(randomblob(524288)))`, fmt.Sprintf("%s-%d", prefix, i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// smallWrite is a write the size of an ordinary push.
func smallWrite(s *Store, key string) error {
	return s.Write(context.Background(), func(tx *WriteTx) error {
		_, err := tx.tx.Exec(`INSERT INTO server_meta (key, value) VALUES (?, hex(randomblob(2048)))`, key)
		return err
	})
}

// pinReader holds a read transaction open, as a backup's copy or a snapshot
// does, until release is called: the log's later frames stay in the log.
func pinReader(t *testing.T, s *Store) (release func()) {
	t.Helper()
	started, done, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- s.Read(context.Background(), func(r *ReadTx) error {
			var n int
			if err := r.tx.QueryRow(`SELECT COUNT(*) FROM server_meta`).Scan(&n); err != nil {
				return err
			}
			close(started)
			<-done
			return nil
		})
	}()
	select {
	case <-started:
	case err := <-finished:
		t.Fatalf("the reader did not start: %v", err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			if err := <-finished; err != nil {
				t.Error(err)
			}
		})
	}
}

// PROTOCOL §14: a one-off large write no longer leaves its write-ahead log on
// disk; once the log is in the database, the next write cuts the file back
// to the limit.
func TestWALIsCutBackAfterALargeWrite(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	bulkWrite(t, s, "bulk", 80)
	if size := walSize(t, s); size <= walSizeLimit {
		t.Fatalf("the test needs a log above the limit: %d bytes", size)
	}
	if res := s.CheckpointWAL(context.Background(), "test"); !res.Complete() {
		t.Fatalf("checkpoint %+v", res)
	}
	if err := smallWrite(s, "after"); err != nil {
		t.Fatal(err)
	}
	if size := walSize(t, s); size > walSizeLimit {
		t.Fatalf("the log stays at %d bytes after a complete checkpoint and a write, above the %d limit", size, walSizeLimit)
	}
	var n int
	if err := s.reader.QueryRow(`SELECT COUNT(*) FROM server_meta WHERE key LIKE 'bulk-%' OR key = 'after'`).Scan(&n); err != nil || n != 81 {
		t.Fatalf("%d rows after the checkpoint (%v)", n, err)
	}
}

// The housekeeping waits for nothing: with a reader holding frames back it
// copies what it can and returns; and it copies the whole log while a write
// holds the writer, so it never takes the writer's lock (a RESTART or
// TRUNCATE checkpoint would, and would report busy here).
func TestWALCheckpointNeverWaits(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	release := pinReader(t, s)
	defer release()
	bulkWrite(t, s, "held", 8)
	began := time.Now()
	held := s.CheckpointWAL(context.Background(), "test")
	if took := time.Since(began); took > 2*time.Second || held.Busy || held.Complete() || held.Checkpointed >= held.Frames {
		t.Fatalf("with a reader holding frames: %+v after %v", held, took)
	}
	if err := smallWrite(s, "while held"); err != nil {
		t.Fatal(err)
	}
	release()
	done := make(chan WALCheckpoint, 1)
	err := s.Write(context.Background(), func(tx *WriteTx) error {
		if _, err := tx.tx.Exec(`INSERT INTO server_meta (key, value) VALUES ('during', 'x')`); err != nil {
			return err
		}
		go func() { done <- s.CheckpointWAL(context.Background(), "test") }()
		select {
		case res := <-done:
			if !res.Complete() || res.Frames <= held.Frames {
				return fmt.Errorf("checkpoint while a write holds the writer: %+v", res)
			}
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("the checkpoint waited for the write holding the writer")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Writes the size of a push run while the housekeeping runs over and over,
// with a reader holding frames back for the first half: every write succeeds
// and none meets a SQLite lock the housekeeping holds. (The account's own
// exclusion is the protocol's: TestRebuildCheckpointsTheWAL and the staged
// recovery test push through Service.Push.)
//
// This checks lock contention only: the writer's busy timeout is zero here,
// so a write that met such a lock fails at once instead of waiting. It is not
// a bound on how long a write takes; elapsed time is only logged. A commit
// ends with a flush to the disk (F_FULLFSYNC on macOS, fsync on Linux), whose
// time depends on other I/O on the machine, housekeeping or not.
func TestWritesNeverWaitForWALHousekeeping(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	if _, err := s.writer.Exec(`PRAGMA busy_timeout = 0`); err != nil {
		t.Fatal(err)
	}
	for i := range 20 { // the ordinary cost of a push here
		if err := smallWrite(s, fmt.Sprintf("warm-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	release := pinReader(t, s)
	defer release()
	bulkWrite(t, s, "backlog", 4)
	stop := make(chan struct{})
	var runs, partial atomic.Int64
	housekeeping := make(chan struct{})
	go func() {
		defer close(housekeeping)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if res := s.CheckpointWAL(context.Background(), "test"); !res.Complete() {
				partial.Add(1)
			}
			runs.Add(1)
		}
	}()
	// Every way out, a failed write included, stops the housekeeping and waits
	// for it before the store closes (deferred calls run before the cleanup).
	var stopOnce sync.Once
	stopHousekeeping := func() {
		stopOnce.Do(func() { close(stop) })
		<-housekeeping
	}
	defer stopHousekeeping()
	const pushes = 200
	latencies := make([]time.Duration, 0, pushes)
	for i := range pushes {
		if i == pushes/2 {
			release()
		}
		began := time.Now()
		if err := smallWrite(s, fmt.Sprintf("push-%d", i)); err != nil {
			t.Fatalf("write %d failed during the housekeeping: %v", i, err)
		}
		latencies = append(latencies, time.Since(began))
	}
	stopHousekeeping()
	slices.Sort(latencies)
	median, worst := latencies[pushes/2], latencies[pushes-1]
	t.Logf("%d writes during %d checkpoints (%d partial): median %v, worst %v", pushes, runs.Load(), partial.Load(), median, worst)
	if runs.Load() < 10 || partial.Load() == 0 {
		t.Fatalf("the housekeeping ran %d times, %d of them held back by the reader", runs.Load(), partial.Load())
	}
	var timeout int
	if err := s.writer.QueryRow(`PRAGMA busy_timeout`).Scan(&timeout); err != nil || timeout != 0 {
		t.Fatalf("the writer's busy timeout was %d during the writes (%v), so a wait for a lock went unseen", timeout, err)
	}
}

// walCalls records the housekeeping's runs; with fail set, each is reported
// as failed.
type walCalls struct {
	mu      sync.Mutex
	after   []string
	results []WALCheckpoint
	fail    atomic.Bool
}

func (w *walCalls) hook(after string, res WALCheckpoint, err error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.after, w.results = append(w.after, after), append(w.results, res)
	if w.fail.Load() {
		return errors.New("simulated checkpoint failure")
	}
	return err
}

func (w *walCalls) last() (string, WALCheckpoint, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.after) == 0 {
		return "", WALCheckpoint{}, 0
	}
	return w.after[len(w.after)-1], w.results[len(w.results)-1], len(w.after)
}

// A backup checkpoints the log after its copy's long read: the writes the
// read held in the log go into the database. A failed checkpoint, or one a
// reader holds back, never fails the backup.
func TestBackupCheckpointsTheWAL(t *testing.T) {
	ctx := context.Background()
	calls := &walCalls{}
	s := openTest(t, config{dataDir: t.TempDir(), hooks: hooks{walCheckpoint: calls.hook}})
	backups := t.TempDir()
	release := pinReader(t, s)
	bulkWrite(t, s, "during", 4)
	release()
	if _, err := s.Backup(ctx, backups); err != nil {
		t.Fatal(err)
	}
	if after, res, n := calls.last(); n != 1 || after != "backup" || !res.Complete() || res.Frames == 0 {
		t.Fatalf("after the backup: %d checkpoints, the last %q %+v", n, after, res)
	}
	calls.fail.Store(true)
	if info, err := s.Backup(ctx, backups); err != nil || info.Size == 0 {
		t.Fatalf("a backup whose checkpoint failed: %+v %v", info, err)
	}
	calls.fail.Store(false)
	release = pinReader(t, s)
	defer release()
	bulkWrite(t, s, "held", 2)
	if _, err := s.Backup(ctx, backups); err != nil {
		t.Fatalf("a backup whose checkpoint a reader held back: %v", err)
	}
	if after, res, n := calls.last(); n != 3 || after != "backup" || res.Complete() {
		t.Fatalf("with a reader holding frames: %d checkpoints, the last %q %+v", n, after, res)
	}
	entries, _ := os.ReadDir(backups)
	if len(entries) != 3 {
		t.Fatalf("%d backups published, want 3", len(entries))
	}
}

// An import's publication checkpoints the log after it commits, once the
// publication lock is free (a staged rebuild waiting for it holds its
// account's exclusion); a failed checkpoint, or one a reader holds back,
// leaves the publication done once.
func TestPublishImportCheckpointsTheWAL(t *testing.T) {
	ctx := context.Background()
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed=%v", failed), func(t *testing.T) {
			calls := &walCalls{}
			calls.fail.Store(failed)
			lockFree := false
			hook := func(after string, res WALCheckpoint, err error) error {
				if publishMu.TryLock() {
					lockFree = true
					publishMu.Unlock()
				}
				return calls.hook(after, res, err)
			}
			live := openTest(t, config{dataDir: t.TempDir(), hooks: hooks{walCheckpoint: hook}})
			account := createAccounts(t, live, 1)[0]
			const importID = "66666666-6666-4666-8666-666666666666"
			stagingPath, base, _ := stageImport(t, live, account, importID)
			release := pinReader(t, live)
			defer release()
			if err := live.PublishImport(ctx, stagingPath, account, importID, base, `{"imported":{}}`); err != nil {
				t.Fatalf("publication: %v", err)
			}
			if after, res, n := calls.last(); n != 1 || after != "import" || res.Complete() || !lockFree {
				t.Fatalf("%d checkpoints, the last %q %+v, the publication lock free: %v", n, after, res, lockFree)
			}
			release()
			var status string
			var podcasts int
			if err := live.reader.QueryRow(`SELECT status FROM imports WHERE account_id = ? AND import_id = ?`, account, importID).Scan(&status); err != nil ||
				status != "published" {
				t.Fatalf("import %q (%v)", status, err)
			}
			if err := live.PublishImport(ctx, stagingPath, account, importID, base, "{}"); !errors.Is(err, ErrImportChanged) {
				t.Fatalf("a second publication: %v", err)
			}
			if err := live.reader.QueryRow(`SELECT COUNT(*) FROM podcasts WHERE account_id = ?`, account).Scan(&podcasts); err != nil || podcasts != 1 {
				t.Fatalf("%d podcasts (%v)", podcasts, err)
			}
			if _, _, n := calls.last(); n != 1 {
				t.Fatalf("a refused publication checkpointed: %d", n)
			}
		})
	}
}
