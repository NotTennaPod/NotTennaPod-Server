package protocol

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// ServerCommit runs fn as the server itself, in one short write transaction:
// the decisions it makes, their contributions and the records they change go
// into one commit with no transport slot (PROTOCOL §9.4). Network I/O
// (fetching, parsing) belongs before it. fn sets OpID to the decision it is
// contributing for; Contribute and Record use it as the writes' origin.
func (s *Service) ServerCommit(ctx context.Context, accountID string, fn func(c *ApplyContext) error) error {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return fmt.Errorf("protocol: no receipt key: %v", err)
	}
	receive := s.Now().UnixMilli()
	var done *commitBuilder
	var head storage.Head
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		done = nil
		acct := tx.Account(accountID)
		namespace, err := acct.Namespace()
		if err != nil {
			return err
		}
		rev, err := acct.NextRevision()
		if err != nil {
			return err
		}
		b := &commitBuilder{key: keys[0], ids: map[string]bool{}, random: s.random()}
		template := ApplyContext{Account: acct, AccountID: accountID, Namespace: namespace, NormalizedTime: receive,
			ReceiveTime: receive, Revision: rev, commit: b, batch: true}
		flush := func() error { return flushDirty(b, template) }
		c := template
		c.flush = flush
		if err := fn(&c); err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
		pc := template
		pc.flush, pc.batch = flush, false
		if err := s.publishQueue(&pc, false, true); err != nil {
			return err
		}
		if len(b.decisions) > 0 || len(b.records) > 0 {
			if _, _, err = acct.AppendCommit(b.body()); err != nil {
				return err
			}
		}
		if head, err = acct.Head(); err != nil {
			return err
		}
		done = b
		return nil
	})
	if err == nil && done != nil {
		done.runAfter(head)
	}
	return err
}

// flushDirty refolds, once each, the scopes whose refold was deferred in this
// commit (a push's or a server commit's), with the last deferring node as the
// refold's origin.
func flushDirty(b *commitBuilder, template ApplyContext) error {
	// Batched work first (placements and identity rules): it may move contributions and mark scopes
	// dirty.
	for len(b.deferred) > 0 {
		keys, fns := b.deferred, b.deferredFns
		b.deferred, b.deferredFns = nil, nil
		for _, k := range keys {
			c := template
			c.commit, c.scopes, c.scopeSet, c.batch = b, nil, nil, false
			if err := fns[k](&c); err != nil {
				return err
			}
		}
	}
	scopes := make([]string, 0, len(b.dirty))
	for scope := range b.dirty {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	for _, scope := range scopes {
		c := template
		c.OpID, c.commit, c.scopes, c.scopeSet = b.dirty[scope], b, nil, nil
		if _, err := c.recompute(scope); err != nil {
			return err
		}
	}
	b.dirty = nil
	return nil
}

// MarkDirty schedules a scope's refold at the end of the commit, with the
// current OpID as origin (merges and occurrence mappings move contributions
// without contributing).
func (c *ApplyContext) MarkDirty(scope string) {
	if c.commit.dirty == nil {
		c.commit.dirty = map[string]string{}
	}
	c.commit.dirty[scope] = c.OpID
}

// TombstoneRecord removes a record and publishes its tombstone.
func (c *ApplyContext) TombstoneRecord(entity, id, group string) error {
	return c.tombstone(entity, id, group)
}

// Batched reports a server commit's context: work a step requests with
// Defer runs once at the commit's next flush rather than at each step.
func (c *ApplyContext) Batched() bool { return c.batch }

// Defer requests work at the commit's next flush, once per key: the last
// request's function runs, at the first request's place in the order.
func (c *ApplyContext) Defer(key string, fn func(*ApplyContext) error) {
	b := c.commit
	if b.deferredFns == nil {
		b.deferredFns = map[string]func(*ApplyContext) error{}
	}
	if _, ok := b.deferredFns[key]; !ok {
		b.deferred = append(b.deferred, key)
	}
	b.deferredFns[key] = fn
}

// ProbeSnapshot runs fn as the server on one read snapshot of the account,
// with a fresh commit, without holding the store's writer: fn derives state
// and can write nothing (storage.Store.Snapshot).
func (s *Service) ProbeSnapshot(ctx context.Context, accountID string, fn func(c *ApplyContext) error) error {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return fmt.Errorf("protocol: no receipt key: %v", err)
	}
	receive := s.Now().UnixMilli()
	return s.Store.Snapshot(ctx, func(tx *storage.WriteTx) error {
		acct := tx.Account(accountID)
		namespace, err := acct.Namespace()
		if err != nil {
			return err
		}
		rev, err := acct.NextRevision()
		if err != nil {
			return err
		}
		b := &commitBuilder{key: keys[0], ids: map[string]bool{}, random: s.random()}
		c := ApplyContext{Account: acct, AccountID: accountID, Namespace: namespace, NormalizedTime: receive, ReceiveTime: receive, Revision: rev,
			commit: b}
		c.flush = func() error { return flushDirty(b, c) }
		return fn(&c)
	})
}

// errProbe rolls a Probe's transaction back.
var errProbe = errors.New("protocol: probe rolled back")

// Probe runs fn as the server in a write transaction of the account, with a
// fresh commit, and rolls everything back: it derives state without changing
// the account (tests, and the queue replay's warm-up when it keeps its cache).
func (s *Service) Probe(ctx context.Context, accountID string, fn func(c *ApplyContext) error) error {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return fmt.Errorf("protocol: no receipt key: %v", err)
	}
	receive := s.Now().UnixMilli()
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		acct := tx.Account(accountID)
		namespace, err := acct.Namespace()
		if err != nil {
			return err
		}
		rev, err := acct.NextRevision()
		if err != nil {
			return err
		}
		b := &commitBuilder{key: keys[0], ids: map[string]bool{}, random: s.random()}
		c := ApplyContext{Account: acct, AccountID: accountID, Namespace: namespace, NormalizedTime: receive, ReceiveTime: receive, Revision: rev,
			commit: b}
		c.flush = func() error { return flushDirty(b, c) }
		if err := fn(&c); err != nil {
			return err
		}
		return errProbe
	})
	if errors.Is(err, errProbe) {
		return nil
	}
	return err
}
