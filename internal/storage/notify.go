package storage

import "sync"

// Commit notifications (PROTOCOL §12): after a write transaction commits,
// every listener of an account it appended a commit to is woken, and a restore
// transition wakes every listener. They are hints: a listener that is already
// woken is not woken twice, and a missed hint loses nothing, since the listener
// re-reads.
type notifier struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]bool
	gens map[chan struct{}]bool // generation listeners (SubscribeGenerations)
}

// Subscribe returns a channel that receives a value after each commit of the
// account, and a function that ends the subscription.
func (s *Store) Subscribe(accountID string) (<-chan struct{}, func()) {
	n := &s.notes
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	if n.subs == nil {
		n.subs = map[string]map[chan struct{}]bool{}
	}
	if n.subs[accountID] == nil {
		n.subs[accountID] = map[chan struct{}]bool{}
	}
	n.subs[accountID][ch] = true
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		delete(n.subs[accountID], ch)
		if len(n.subs[accountID]) == 0 {
			delete(n.subs, accountID)
		}
		n.mu.Unlock()
	}
}

// SubscribeGenerations returns a channel that receives a value after each
// commit that opened a podcast generation of any account, or published a
// staged copy of an account (an import, a staged recovery), and a function
// that ends the subscription. The refresher fetches a new generation at once,
// not at its next scan (PROTOCOL §9.4).
func (s *Store) SubscribeGenerations() (<-chan struct{}, func()) {
	n := &s.notes
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	if n.gens == nil {
		n.gens = map[chan struct{}]bool{}
	}
	n.gens[ch] = true
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		delete(n.gens, ch)
		n.mu.Unlock()
	}
}

// GenerationOpened records that this transaction opened a podcast generation
// of the account: its generation listeners are woken after the commit.
func (a *AccountTx) GenerationOpened() { a.w.opened = true }

// notify wakes the listeners of the accounts (all of them when all is set),
// and the generation listeners when gens is set.
func (s *Store) notify(accounts []string, all, gens bool) {
	n := &s.notes
	n.mu.Lock()
	defer n.mu.Unlock()
	wake := func(set map[chan struct{}]bool) {
		for ch := range set {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	if gens {
		wake(n.gens)
	}
	if all {
		for _, set := range n.subs {
			wake(set)
		}
		return
	}
	for _, id := range accounts {
		wake(n.subs[id])
	}
}

// committedAccounts lists the accounts this transaction appended a commit to
// or whose log it otherwise advanced (an import's publication).
func (w *WriteTx) committedAccounts() []string {
	var out []string
	for id, a := range w.accounts {
		if a.appended || w.changed[id] {
			out = append(out, id)
		}
	}
	for id := range w.changed {
		if _, ok := w.accounts[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// markChanged records that this transaction advanced the account's log
// without AppendCommit, so its listeners are woken after the commit.
func (w *WriteTx) markChanged(accountID string) {
	if w.changed == nil {
		w.changed = map[string]bool{}
	}
	w.changed[accountID] = true
}
