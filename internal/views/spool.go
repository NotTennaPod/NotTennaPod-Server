package views

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Spool keeps materialized list views (PROTOCOL §12): a list is
// computed at one head on its first request and written to a file, and its
// pages read that file whatever commits follow, so paging never mixes heads. A
// view expires after IdleExpiry without a read; the browser then starts again
// from the first page. Views do not survive a restart.
type Spool struct {
	dir   string
	now   func() time.Time
	mu    sync.Mutex
	views map[string]*spooled
}

type spooled struct {
	account string
	head    storage.Head
	params  string
	extra   json.RawMessage
	offsets []int64 // the start of each item, and the end of the last
	path    string
	used    time.Time
}

const (
	// IdleExpiry is how long an unread view is kept.
	IdleExpiry = 10 * time.Minute
	// maxPerAccount is how many views an account keeps; the least recently
	// read goes first.
	maxPerAccount = 16
)

// Errors from the spool.
var (
	ErrExpired = errors.New("views: the view expired")
	ErrParams  = errors.New("views: the page token belongs to other parameters")
	ErrToken   = errors.New("views: invalid page token")
)

// NewSpool returns a spool in dir, removing views left by an earlier run.
func NewSpool(dir string, now func() time.Time) (*Spool, error) {
	if now == nil {
		now = time.Now
	}
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := private.RealDir(dir); err != nil {
		return nil, err
	}
	return &Spool{dir: dir, now: now, views: map[string]*spooled{}}, nil
}

// Put materializes a view: its items (each one JSON value) and the values
// shown with it, at its head. It returns the view's ID.
func (s *Spool) Put(account string, head storage.Head, params string, extra any, items []any) (string, error) {
	var idb [16]byte
	if _, err := rand.Read(idb[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(idb[:])
	ex, err := json.Marshal(extra)
	if err != nil {
		return "", err
	}
	path := filepath.Join(s.dir, id+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	w := bufio.NewWriter(f)
	offsets := make([]int64, 0, len(items)+1)
	var at int64
	for _, it := range items {
		b, err := json.Marshal(it)
		if err != nil {
			f.Close()
			os.Remove(path)
			return "", err
		}
		offsets = append(offsets, at)
		n, _ := w.Write(b)
		at += int64(n)
	}
	offsets = append(offsets, at)
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(path)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.views[id] = &spooled{account: account, head: head, params: params, extra: ex, offsets: offsets, path: path, used: s.now()}
	s.trim(account)
	return id, nil
}

// trim drops an account's least recently read views beyond the limit.
func (s *Spool) trim(account string) {
	var mine []string
	for id, v := range s.views {
		if v.account == account {
			mine = append(mine, id)
		}
	}
	if len(mine) <= maxPerAccount {
		return
	}
	sort.Slice(mine, func(i, j int) bool { return s.views[mine[i]].used.Before(s.views[mine[j]].used) })
	for _, id := range mine[:len(mine)-maxPerAccount] {
		s.drop(id)
	}
}

func (s *Spool) drop(id string) {
	if v, ok := s.views[id]; ok {
		os.Remove(v.path)
		delete(s.views, id)
	}
}

// Page is one page of a view.
type Page struct {
	Head   storage.Head
	Extra  json.RawMessage
	Items  []json.RawMessage
	Total  int
	Next   string // the next page's token, "" after the last page
	ViewID string
}

// Read returns the page of view id starting at offset, if the view belongs
// to the account and was made with the same parameters.
func (s *Spool) Read(account, id, params string, offset, limit int) (Page, error) {
	s.mu.Lock()
	v, ok := s.views[id]
	if ok && s.now().Sub(v.used) > IdleExpiry {
		s.drop(id)
		ok = false
	}
	if !ok || v.account != account {
		s.mu.Unlock()
		return Page{}, ErrExpired
	}
	if v.params != params {
		s.mu.Unlock()
		return Page{}, ErrParams
	}
	v.used = s.now()
	total := len(v.offsets) - 1
	if offset > total {
		offset = total
	}
	end := min(total, offset+limit)
	start, stop := v.offsets[offset], v.offsets[end]
	bounds := append([]int64(nil), v.offsets[offset:end+1]...)
	path, head, extra := v.path, v.head, v.extra
	s.mu.Unlock()
	f, err := os.Open(path)
	if err != nil {
		return Page{}, ErrExpired // dropped meanwhile
	}
	defer f.Close()
	buf := make([]byte, stop-start)
	if _, err := f.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
		return Page{}, err
	}
	p := Page{Head: head, Extra: extra, Total: total, ViewID: id}
	for i := 0; i+1 < len(bounds); i++ {
		p.Items = append(p.Items, json.RawMessage(buf[bounds[i]-start:bounds[i+1]-start]))
	}
	if end < total {
		p.Next = PageToken(id, end)
	}
	return p, nil
}

// Sweep removes views idle for longer than IdleExpiry.
func (s *Spool) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, v := range s.views {
		if s.now().Sub(v.used) > IdleExpiry {
			s.drop(id)
		}
	}
}

// PageToken is the opaque token of a view's page.
func PageToken(id string, offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id + ":" + strconv.Itoa(offset)))
}

// ParsePageToken reads a page token.
func ParsePageToken(token string) (id string, offset int, err error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", 0, ErrToken
	}
	id, n, ok := strings.Cut(string(b), ":")
	if !ok || len(id) != 32 {
		return "", 0, ErrToken
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", 0, ErrToken
	}
	offset, err = strconv.Atoi(n)
	if err != nil || offset < 0 {
		return "", 0, ErrToken
	}
	return id, offset, nil
}
