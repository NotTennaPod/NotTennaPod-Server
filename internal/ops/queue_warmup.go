package ops

// The queue replay's warm-up after a start (PROTOCOL §7.6): the replay's
// snapshots and published keys live in memory, so the first queue command
// after a restart would replay the whole history inside its own push. The
// server replays each account active in the last 30 days
// (storage.RecentlyActive, as the read model's warm-up) once in the
// background as soon as it serves.
// It makes no commit and publishes nothing (not ServerCommit, whose
// publication may record a layout). The replay reads one snapshot without
// holding the store's writer (protocol.Service.ProbeSnapshot); its result is
// kept as the account's cache only if the head is unchanged, checked in a
// short write transaction that is rolled back: exactly what the first
// command's own replay would have kept.

import (
	"context"
	"log/slog"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/queue"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// WarmQueues warms the queue replay of every account active in the last 30
// days (storage.RecentlyActiveAccounts), one account at a time, until done
// or ctx ends. A failure is logged; the account's first queue command
// then replays as it would without the warm-up.
func WarmQueues(ctx context.Context, svc *protocol.Service) {
	var accounts []string
	if err := svc.Store.Read(ctx, func(r *storage.ReadTx) error {
		var err error
		accounts, err = r.RecentlyActiveAccounts(svc.Now())
		return err
	}); err != nil {
		if ctx.Err() == nil {
			slog.Warn("queue warm-up: cannot list the accounts", "error", err)
		}
		return
	}
	for _, account := range accounts {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		warmed, err := WarmQueue(ctx, svc, account)
		switch {
		case err != nil && ctx.Err() == nil:
			slog.Warn("queue warm-up failed", "account", account, "error", err)
		case warmed:
			slog.Info("queue replay warmed", "account", account, "duration", time.Since(started).Round(time.Millisecond))
		}
	}
}

// warmupReplaying, when set (tests), runs inside each warm-up replay's
// snapshot, after the replay; warmupReplayed runs after the snapshot is
// closed, before the result is checked against the head and kept.
var warmupReplaying, warmupReplayed func(account string)

// warmupAttempts bounds the replays of one account's warm-up: each one that
// a commit overtook is discarded.
const warmupAttempts = 3

// WarmQueue replays an account's queue from the start and keeps the result as
// its cache, unless a valid cache is already kept; it reports whether it kept
// one. The replay reads one snapshot and does not hold the store's writer, so
// live writes go on meanwhile; the result is kept only if the account's head
// is still the snapshot's, checked and kept in a short write transaction that
// is rolled back. A replay a commit overtook is discarded and done again, up
// to warmupAttempts times. Nothing is written.
func WarmQueue(ctx context.Context, svc *protocol.Service, account string) (bool, error) {
	for range warmupAttempts {
		var keep *replayCache
		err := svc.ProbeSnapshot(ctx, account, func(c *protocol.ApplyContext) error {
			head, err := c.Account.Head()
			if err != nil {
				return err
			}
			if v, ok := c.Account.Memo().Load(memoKey{account}); ok && v.(*replayCache).valid(head) {
				return nil // a command was faster
			}
			cr, err := replay(c, false)
			if err != nil {
				return err
			}
			if touched, _ := c.QueueChanges(false); len(touched) > 0 {
				return nil // a step not archived yet: never outside a commit, but then nothing is kept
			}
			st := cr.snaps[len(cr.snaps)-1].state
			keys, top, err := publishedKeys(c, st)
			if err != nil {
				return err
			}
			keep = &replayCache{head: head, snaps: cr.snaps, layoutRank: top}
			// The keys are trusted (an unrelated commit may then keep the cache
			// without publishing) only when they give the replay's order, as a
			// commit's publication leaves them; otherwise the first command
			// publishes in full, as it would have.
			if pub := queue.KeyMapOf(keys); keysFit(st.Visible(), pub) {
				keep.pub, keep.pubOK = pub, true
			}
			if warmupReplaying != nil {
				warmupReplaying(account)
			}
			return nil
		})
		if err != nil || keep == nil {
			return false, err
		}
		if warmupReplayed != nil {
			warmupReplayed(account)
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		kept := false
		if err := svc.Probe(ctx, account, func(c *protocol.ApplyContext) error {
			head, err := c.Account.Head()
			if err != nil || !keep.valid(head) {
				return err // a commit overtook the replay
			}
			if v, ok := c.Account.Memo().Load(memoKey{account}); ok && v.(*replayCache).valid(head) {
				// A cache another transaction kept for this head meanwhile (a
				// rebuild's, which commits without moving the head when no
				// record differs) is never replaced by one replayed before it.
				return nil
			}
			installCache(c.Account.Memo(), account, keep)
			kept = true
			return nil
		}); err != nil {
			return false, err
		}
		if kept {
			return true, nil
		}
	}
	return false, nil
}
