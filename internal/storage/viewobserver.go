package storage

import (
	"database/sql"
	"errors"
	"maps"
	"sync/atomic"
)

// The test-only commit observer, for tests that check the read model commit
// by commit: it sees every commit of every store of the process, with the
// identity each account the commit touched has in the snapshot the commit
// produced, before any other write of the store begins.
//
// The writes a store makes while it opens (repair, migrations, startup and a
// restore transition) come before its readers exist: they are observed
// together, as one commit, once the store is open, with the identities the
// opening left, and still before any other write of the store.
//
// Installing it in a package's TestMain: the read model's observer
// (viewstest.Main) needs the read model, which imports this package, so
// storage's own TestMain is the external package's (main_test.go, package
// storage_test); it also runs the crash tests' child processes
// (RunTestChild), as a test binary has one TestMain. Tests that install an
// observer of their own chain the installed one (observeAlso).

// CommittedAccount is an account as a commit left it: whether it exists, its
// log head and its capture clock.
type CommittedAccount struct {
	Exists bool
	Head   Head
	Clock  ViewClock
}

// CommitObserver sees a commit of store s and the accounts it touched: every
// account whose view key changed (its view inputs, its log's identity or
// head, its capture clock) or that it declared (SuspendViewCapture),
// created or deleted. It runs while the store's write gate is held, so the
// store's newest snapshot is the commit's; it may read, and must never
// write.
type CommitObserver func(s *Store, accounts map[string]CommittedAccount)

var commitObserver atomic.Pointer[CommitObserver]

// ObserveCommitsForTesting installs f (nil removes it) for every store of
// this process, and turns the capture audit on, whose triggers name the
// accounts a commit changed. Tests call it from TestMain; a server never
// does.
func ObserveCommitsForTesting(f CommitObserver) {
	if f == nil {
		commitObserver.Store(nil)
		return
	}
	AuditViewCaptureForTesting()
	commitObserver.Store(&f)
}

// observedAccounts reads, just before the commit, the identity of every
// account the transaction touched.
func (w *WriteTx) observedAccounts() (map[string]CommittedAccount, error) {
	ids := map[string]bool{}
	for _, id := range w.committedAccounts() {
		ids[id] = true
	}
	touched, err := queryStrings(w.ctx, w.tx, `SELECT account_id FROM temp.view_audit UNION SELECT account_id FROM temp.view_audit_accounts`)
	if err != nil {
		return nil, err
	}
	for _, id := range touched {
		ids[id] = true
	}
	out := map[string]CommittedAccount{}
	for id := range ids {
		var a CommittedAccount
		err := w.tx.QueryRowContext(w.ctx, `SELECT log_id, epoch, head_seq, head_digest FROM account_logs WHERE account_id = ?`, id).
			Scan(&a.Head.LogID, &a.Head.Epoch, &a.Head.Seq, &a.Head.Digest)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return nil, err
		default:
			a.Exists = true
			if err := w.tx.QueryRowContext(w.ctx, `SELECT COALESCE(MAX(tick), 0), COALESCE(MAX(generation), 0) FROM view_clock WHERE account_id = ?`, id).
				Scan(&a.Clock.Tick, &a.Clock.Generation); err != nil {
				return nil, err
			}
		}
		out[id] = a
	}
	return out, nil
}

// observe hands a commit to the observer, or, while the store opens (before
// its readers exist), keeps it for the opening's one observation.
func (s *Store) observe(f CommitObserver, accounts map[string]CommittedAccount) {
	if s.reader == nil {
		if s.opening == nil {
			s.opening = map[string]CommittedAccount{}
		}
		maps.Copy(s.opening, accounts)
		return
	}
	f(s, accounts)
}

// observeOpening hands the opening's writes to the observer, once the
// store's readers exist.
func (s *Store) observeOpening() {
	accounts := s.opening
	s.opening = nil
	f := commitObserver.Load()
	if f == nil || accounts == nil {
		return
	}
	s.gate.Lock()
	defer s.gate.Unlock()
	(*f)(s, accounts)
}
