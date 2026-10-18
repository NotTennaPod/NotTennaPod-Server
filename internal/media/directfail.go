package media

import (
	"container/list"
	"sync"
	"time"
)

// The direct-failure memo (PROTOCOL §13): an account's sources whose
// publisher URL failed in the browser, so its later descriptors choose the
// relay at once rather than direct playback that fails again. It lives in
// memory, bounded and expiring; the browser remembers the same failures
// across a server restart and asks with direct_failed.
const (
	directFailureTTL = 24 * time.Hour
	directFailureMax = 1000
)

type directFailure struct {
	key string // account, NUL, normalized source
	at  time.Time
}

// directFailures is the memo: the least recently remembered goes first when full.
type directFailures struct {
	mu    sync.Mutex
	order *list.List // of *directFailure, most recent at the front
	byKey map[string]*list.Element
}

func newDirectFailures() *directFailures {
	return &directFailures{order: list.New(), byKey: map[string]*list.Element{}}
}

func directKey(account, source string) string { return account + "\x00" + source }

// remember records that the account's source failed in the browser at now.
func (m *directFailures) remember(account, source string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := directKey(account, source)
	if el, ok := m.byKey[key]; ok {
		el.Value.(*directFailure).at = now
		m.order.MoveToFront(el)
		return
	}
	m.byKey[key] = m.order.PushFront(&directFailure{key: key, at: now})
	for m.order.Len() > directFailureMax {
		last := m.order.Back()
		m.order.Remove(last)
		delete(m.byKey, last.Value.(*directFailure).key)
	}
}

// failed reports whether the account's source failed in the browser within
// the memo's lifetime; an expired entry is dropped.
func (m *directFailures) failed(account, source string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := directKey(account, source)
	el, ok := m.byKey[key]
	if !ok {
		return false
	}
	if now.Sub(el.Value.(*directFailure).at) >= directFailureTTL {
		m.order.Remove(el)
		delete(m.byKey, key)
		return false
	}
	return true
}

// len is the number of entries held (tests).
func (m *directFailures) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.order.Len()
}
