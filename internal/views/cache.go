package views

import (
	"sync"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Cache keeps the most recently loaded libraries by account and head. A
// library is immutable once loaded and describes exactly its head, so every
// view of that head can share it; a later head loads anew.
type Cache struct {
	mu      sync.Mutex
	max     int
	entries []cached
}

type cached struct {
	account string
	head    storage.Head
	lib     *Library
}

// NewCache returns a cache of at most max libraries.
func NewCache(max int) *Cache { return &Cache{max: max} }

// Load returns the account's library at the snapshot's head.
func (c *Cache) Load(acct *storage.AccountReader, account string) (*Library, error) {
	head, err := acct.Head()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	for i, e := range c.entries {
		if e.account == account && e.head.LogID == head.LogID && e.head.Epoch == head.Epoch && e.head.Seq == head.Seq {
			c.entries = append(append([]cached{e}, c.entries[:i]...), c.entries[i+1:]...)
			c.mu.Unlock()
			return e.lib, nil
		}
	}
	c.mu.Unlock()
	lib, err := Load(acct)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := []cached{{account: account, head: head, lib: lib}}
	for _, e := range c.entries {
		if e.account != account && len(kept) < c.max { // one library per account: older heads are never read again
			kept = append(kept, e)
		}
	}
	c.entries = kept
	return lib, nil
}
