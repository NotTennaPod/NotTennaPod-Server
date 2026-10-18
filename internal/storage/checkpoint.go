package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
)

// WAL retention (PROTOCOL §14). SQLite reuses its write-ahead log from the
// start once a checkpoint has copied all of it into the database, but never
// shrinks the file by itself: one large transaction (an import's
// publication, a rebuild) or the frames a long read holds back (a backup, a
// snapshot) used to leave a log that size on disk for good.
//
// journal_size_limit (openDB) cuts the file back to walSizeLimit whenever
// the log restarts, which the first write after a complete checkpoint does
// when no read still uses the old frames. CheckpointWAL helps after the
// large writes and the backup's long read: a passive checkpoint on a
// connection of its own copies what it can into the database. It never takes
// the writer's lock and never waits for a reader or another checkpoint, and
// it runs only after the flow that wrote has released what a push meets (the
// account's rebuild mark, the publication lock): a push never queues behind
// it or is answered server_busy because of it, though it competes for disk
// I/O. What it cannot copy now, the writer's own checkpoints copy later.
// The peak is not capped: the
// log still grows to the largest transaction, or to everything written while
// a long read is open, before it is cut back.

// walSizeLimit is the size the write-ahead log is cut back to.
const walSizeLimit = 64 << 20

// WALCheckpoint is a checkpoint's outcome.
type WALCheckpoint struct {
	Busy         bool  // another checkpoint was running: nothing was copied
	Frames       int64 // frames in the log
	Checkpointed int64 // frames now in the database; fewer while a reader holds the rest
}

// Complete reports whether the whole log is in the database, so the next
// write can restart it (and cut the file back).
func (c WALCheckpoint) Complete() bool { return !c.Busy && c.Checkpointed == c.Frames }

// WithWALCheckpointHook calls h after each CheckpointWAL with what it did;
// an error h returns is reported as the checkpoint's failure (tests).
func WithWALCheckpointHook(h func(after string, res WALCheckpoint, err error) error) OpenOption {
	return func(c *config) { c.hooks.walCheckpoint = h }
}

// CheckpointWAL copies what it can of the write-ahead log into the database
// after a large write or a long read (after names it, for the log), as a
// passive checkpoint on a short-lived connection with the writer's
// durability settings. It is best effort and never fails its caller: the
// write it follows is already durable, so an error is only logged.
func (s *Store) CheckpointWAL(ctx context.Context, after string) WALCheckpoint {
	if s.meta == nil {
		// Not the server's database: a staging or inspected copy, which is
		// deleted or left as it is.
		return WALCheckpoint{}
	}
	res, err := s.checkpointWAL(ctx)
	if h := s.hooks.walCheckpoint; h != nil {
		err = h(after, res, err)
	}
	switch {
	case err != nil:
		slog.Warn("could not checkpoint the write-ahead log; later writes do", "after", after, "error", err)
	case !res.Complete():
		slog.Info("checkpointed part of the write-ahead log; a reader or another checkpoint holds the rest", "after", after,
			"frames", res.Frames, "checkpointed", res.Checkpointed, "busy", res.Busy)
	default:
		slog.Info("checkpointed the write-ahead log", "after", after, "frames", res.Frames)
	}
	return res
}

func (s *Store) checkpointWAL(ctx context.Context) (WALCheckpoint, error) {
	// Close waits for it, as for a write.
	s.life.RLock()
	defer s.life.RUnlock()
	if s.closed.Load() {
		return WALCheckpoint{}, ErrClosed
	}
	q := url.Values{}
	for _, p := range []string{"synchronous(FULL)", "fullfsync(1)", "checkpoint_fullfsync(1)", "trusted_schema(0)", "busy_timeout(250)"} {
		q.Add("_pragma", p)
	}
	db, err := sql.Open("sqlite", filepath.Join(s.dir, s.dbName)+"?"+q.Encode())
	if err != nil {
		return WALCheckpoint{}, fmt.Errorf("storage: opening the checkpoint connection: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var busy int
	var res WALCheckpoint
	// PASSIVE: never the writer's lock, never a wait (PROTOCOL §14).
	if err := db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &res.Frames, &res.Checkpointed); err != nil {
		return WALCheckpoint{}, fmt.Errorf("storage: checkpoint: %w", err)
	}
	res.Busy = busy != 0
	return res, nil
}
