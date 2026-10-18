package views

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Spool is the registry of list views (PROTOCOL §12). A list is planned at
// one head on its first request; its pages read that plan whatever commits
// follow, so paging never mixes heads. A view holds its version (shared with
// every view of that version), its plan and up to resumeEntries cursors, and
// no rows: a page is merged and encoded from the version when it is read.
//
// A view expires (ErrExpired, 410 view_expired) after IdleExpiry without a
// read; when its account has more than maxPerAccount views, the least
// recently read first; when its account's identity (log lineage and library
// generation) is no longer the one it was made at; and when its account is
// retired (Retire, a new incarnation); and when the read model no longer
// retains its version (Cache, retain): the account's published version, the
// one before it, and at most maxLeased older versions under a lease, those
// whose views were read in the last IdleExpiry (the most recently read
// first). A page is read only when the caller's
// reading of the account's current identity is the view's, and released
// only when the view is still live after it was encoded. Registration is
// fenced: a view made under an older incarnation, or of an identity older
// than the newest the registry has seen, or of a version no longer retained,
// // is refused (ErrRetired) and the request starts again. Identities are
// ordered within a log (epoch, then generation, both of which never go back); an account's log never changes
// while the server runs, so an identity of another log than the newest is
// never taken for a newer one. An identical first request (same account,
// incarnation, head, view key and parameters) reuses its view. Views do not
// survive a restart.
//
// Responses being made are admitted (Admit, a Hold): at most maxHolds per
// account at once, each pinning at most one library, so the versions that
// responses still being encoded keep reachable are bounded too. A page read
// is admitted before its view is looked up: a waiting read pins nothing.
type Spool struct {
	now      func() time.Time
	mu       sync.Mutex
	views    map[string]*view
	accounts map[string]*accountViews
	reads    uint64        // counts registrations and reads: a view's last is its recency
	pageHook func()        // tests: called before a page is encoded
	holdWait time.Duration // how long a page read waits for admission
	freed    chan struct{} // closed (and replaced) when a hold is released
}

type accountViews struct {
	inc    uint64
	newest Identity // the newest identity seen
	seen   bool

	// The publications of the versions the read model retains (Cache): its
	// published version, the one before it and the leased ones; tracked
	// once it published.
	retained map[uint64]bool
	tracked  bool

	// Admission of the responses being made, and what they pin (by
	// publication; a library made elsewhere counts as one each).
	slots chan struct{}
	holds map[*Hold]uint64
}

// keeps reports whether a view of the version with this publication number
// may live: any, before the read model published.
func (a *accountViews) keeps(pub uint64) bool {
	return !a.tracked || pub != 0 && a.retained[pub]
}

// Identity is what a view is valid for: its account's log lineage and
// library generation. A bulk replacement (an import's publication, a
// rebuild) raises the generation; a restore transition raises the epoch.
type Identity struct {
	LogID      string
	Epoch      int64
	Generation int64
}

// CurrentIdentity is the account's identity in a snapshot: two point reads,
// its log head and its capture clock.
func CurrentIdentity(acct *storage.AccountReader) (Identity, error) {
	h, err := acct.Head()
	if err != nil {
		return Identity{}, err
	}
	c, err := acct.ViewClock()
	if err != nil {
		return Identity{}, err
	}
	return Identity{h.LogID, h.Epoch, c.Generation}, nil
}

// Identity is the library's identity.
func (l *Library) Identity() Identity {
	return Identity{l.Head.LogID, l.Head.Epoch, l.V.Clock.Generation}
}

// after reports whether i is newer than o: the same log at a later (epoch,
// generation). Identities of different logs are not ordered.
func (i Identity) after(o Identity) bool {
	if i.LogID != o.LogID {
		return false
	}
	if i.Epoch != o.Epoch {
		return i.Epoch > o.Epoch
	}
	return i.Generation > o.Generation
}

// viewKey is a version's view key: its identity and tick.
type viewKey struct {
	Identity
	tick int64
}

func keyOf(lib *Library) viewKey { return viewKey{lib.Identity(), lib.V.Clock.Tick} }

type view struct {
	id      string
	account string
	inc     uint64
	head    storage.Head
	key     viewKey
	pub     uint64 // its version's publication (Library.pub)
	params  string
	src     Source
	extra   json.RawMessage
	used    time.Time // for the idle expiry
	recent  uint64    // the registry's count at its last registration or read

	mu     sync.Mutex
	resume []Cursor // the cursors after the latest pages read, newest last
}

const (
	// IdleExpiry is how long an unread view is kept.
	IdleExpiry = 10 * time.Minute
	// maxPerAccount is how many views an account keeps; the least recently
	// read goes first.
	maxPerAccount = 64
	// resumeEntries is how many cursors a view keeps for the next pages.
	resumeEntries = 4
	// maxLeased is how many versions older than the published one and the
	// one before it an account keeps for views read in the last IdleExpiry.
	maxLeased = 2
	// maxHolds is how many responses of an account are made at once.
	maxHolds = 2
	// holdWait is how long a page read waits for admission by default.
	holdWait = 10 * time.Second
)

// ErrBusy reports a caller that could not be served in time: no library
// could be read in 6 attempts (the account's library was purged, or its
// builds failed, again and again), or a response was not admitted.
var ErrBusy = errors.New("views: no library could be read in time")

// Errors from the spool.
var (
	ErrExpired = errors.New("views: the view expired")
	ErrParams  = errors.New("views: the page token belongs to other parameters")
	ErrToken   = errors.New("views: invalid page token")
	ErrRetired = errors.New("views: the view's incarnation, lineage or generation is no longer the account's")
)

// Source is a list view's rows, read at any offset of a version.
type Source interface {
	Total() int
	// Page encodes up to limit rows at the cursor (nil: at offset) and
	// returns the cursor after them.
	Page(offset, limit int, c *Cursor) ([]json.RawMessage, Cursor, error)
}

// NewSpool returns an empty registry. dir is where earlier versions kept
// views on disk: it is removed.
func NewSpool(dir string, now func() time.Time) (*Spool, error) {
	if now == nil {
		now = time.Now
	}
	if dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
	}
	return &Spool{now: now, views: map[string]*view{}, accounts: map[string]*accountViews{}, holdWait: holdWait}, nil
}

func (s *Spool) account(id string) *accountViews {
	a := s.accounts[id]
	if a == nil {
		a = &accountViews{slots: make(chan struct{}, maxHolds), holds: map[*Hold]uint64{}}
		s.accounts[id] = a
	}
	return a
}

// Hold is an admitted response of an account: until Release, it may pin
// one library (Pin), which the memory bound counts.
type Hold struct {
	s       *Spool
	a       *accountViews
	release sync.Once
}

// Admit waits, until ctx ends, for one of the account's maxHolds response
// slots (ErrBusy, or ctx's error, if none frees). The caller releases it.
func (s *Spool) Admit(ctx context.Context, account string) (*Hold, error) {
	s.mu.Lock()
	a := s.account(account)
	s.mu.Unlock()
	select {
	case a.slots <- struct{}{}:
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrBusy
		}
		return nil, ctx.Err()
	}
	h := &Hold{s: s, a: a}
	s.mu.Lock()
	a.holds[h] = 0
	s.mu.Unlock()
	return h, nil
}

// TryAdmit is Admit when a slot is free at once.
func (s *Spool) TryAdmit(account string) (*Hold, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	select {
	case a.slots <- struct{}{}:
	default:
		return nil, false
	}
	h := &Hold{s: s, a: a}
	a.holds[h] = 0
	return h, true
}

// Pin records the library the response is made from.
func (h *Hold) Pin(lib *Library) {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	if _, ok := h.a.holds[h]; ok {
		h.a.holds[h] = lib.pub
	}
}

// Release ends the hold; a second call does nothing.
func (h *Hold) Release() {
	h.release.Do(func() {
		h.s.mu.Lock()
		delete(h.a.holds, h)
		if h.s.freed != nil {
			close(h.s.freed)
			h.s.freed = nil
		}
		h.s.mu.Unlock()
		<-h.a.slots
	})
}

// released returns a channel closed when a hold is next released.
func (s *Spool) released() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.freed == nil {
		s.freed = make(chan struct{})
	}
	return s.freed
}

// heldAccounts lists the accounts a response being made pins a library of.
func (s *Spool) heldAccounts() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]bool{}
	for id, a := range s.accounts {
		for _, pub := range a.holds {
			if pub != 0 {
				out[id] = true
			}
		}
	}
	return out
}

// Incarnation is the account's current incarnation: a request reads it
// before its library and registers its view under it.
func (s *Spool) Incarnation(account string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.account(account).inc
}

// Retire starts a new incarnation of the account: its views expire, and a
// view planned under the old one is refused (a purge). The read API retires
// an account when a commit raises its generation (storage.Store.
// OnIdentityChange), before the commit's write returns.
func (s *Spool) Retire(account string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	a.inc++
	for id, v := range s.views {
		if v.account == account {
			delete(s.views, id)
		}
	}
}

// RetireAll retires every account (a restore transition moved every
// account's lineage).
func (s *Spool) RetireAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.accounts {
		a.inc++
	}
	clear(s.views)
}

// Observe records an identity the account was read at: a newer one
// expires the views of the older ones at once; an older one (a slow
// request's) changes nothing.
func (s *Spool) Observe(account string, lib *Library) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observe(account, lib.Identity())
}

// observe reports whether id is the account's newest identity, after
// taking it as the newest when it is newer (or the first seen).
func (s *Spool) observe(account string, id Identity) bool {
	a := s.account(account)
	switch {
	case a.seen && id == a.newest:
		return true
	case a.seen && !id.after(a.newest):
		return false // older, or of another log
	}
	a.newest, a.seen = id, true
	for vid, v := range s.views {
		if v.account == account && v.key.Identity != id {
			delete(s.views, vid)
		}
	}
	return true
}

// Put registers a view of lib under the incarnation the request read, or
// returns the identical view registered before. It refuses (ErrRetired) a
// retired incarnation, a version older than the account's newest lineage
// and generation, or a version the read model no longer retains.
func (s *Spool) Put(account string, inc uint64, lib *Library, params string, src Source, extra any) (string, error) {
	ex, err := json.Marshal(extra)
	if err != nil {
		return "", err
	}
	k := keyOf(lib)
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.account(account); a.inc != inc || !a.keeps(lib.pub) || !s.observe(account, k.Identity) {
		return "", ErrRetired
	}
	for id, v := range s.views {
		if v.account == account && v.inc == inc && v.key == k && v.pub == lib.pub && v.head.Seq == lib.Head.Seq && string(v.head.Digest) == string(lib.Head.Digest) &&
			v.params == params && s.now().Sub(v.used) <= IdleExpiry {
			v.used, v.recent = s.now(), s.touch()
			return id, nil
		}
	}
	var idb [16]byte
	if _, err := rand.Read(idb[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(idb[:])
	s.views[id] = &view{id: id, account: account, inc: inc, head: lib.Head, key: k, pub: lib.pub, params: params, src: src, extra: ex, used: s.now(), recent: s.touch()}
	s.trim(account)
	return id, nil
}

// touch is the next recency (s.mu is held): the cap drops the least
// recently read views whatever the clock's resolution.
func (s *Spool) touch() uint64 {
	s.reads++
	return s.reads
}

// trim drops an account's least recently read views beyond the limit.
func (s *Spool) trim(account string) {
	var mine []*view
	for _, v := range s.views {
		if v.account == account {
			mine = append(mine, v)
		}
	}
	if len(mine) <= maxPerAccount {
		return
	}
	sort.Slice(mine, func(i, j int) bool { return mine[i].recent < mine[j].recent })
	for _, v := range mine[:len(mine)-maxPerAccount] {
		delete(s.views, v.id)
	}
}

// Page is one page of a view.
type Page struct {
	Head     storage.Head
	Identity Identity // the view's
	inc      uint64   // the view's incarnation
	Extra    json.RawMessage
	Items    []json.RawMessage
	Total    int
	Offset   int    // the first item's index
	Next     string // the next page's token, "" after the last page
	ViewID   string
}

// Read returns the page of view id starting at offset (clamped to the
// list), if the view belongs to the account, was made with the same
// parameters and is still live: its incarnation is the account's and its
// identity is cur, the account's current identity as the caller read it
// (CurrentIdentity). The page is released only if the view is still live
// once it is encoded (a Retire or a newer identity seen meanwhile expires
// it). c, if not nil, is a cursor the caller found for that offset (an
// anchor's).
func (s *Spool) Read(account, id, params string, offset, limit int, c *Cursor, cur Identity) (Page, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.holdWait)
	defer cancel()
	h, err := s.Admit(ctx, account) // before the view is looked up: a waiting read pins nothing
	if err != nil {
		return Page{}, err
	}
	defer h.Release()
	return s.ReadHeld(h, account, id, params, offset, limit, c, cur)
}

// ReadHeld is Read under a hold the caller was admitted with.
func (s *Spool) ReadHeld(h *Hold, account, id, params string, offset, limit int, c *Cursor, cur Identity) (Page, error) {
	s.mu.Lock()
	v, ok := s.views[id]
	if ok && s.now().Sub(v.used) > IdleExpiry {
		delete(s.views, id)
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
	s.observe(account, cur)
	if !s.live(v) {
		delete(s.views, id)
		s.mu.Unlock()
		return Page{}, ErrExpired
	}
	if v.key.Identity != cur { // the caller's reading is not the view's (an older one)
		s.mu.Unlock()
		return Page{}, ErrExpired
	}
	v.used, v.recent = s.now(), s.touch()
	h.a.holds[h] = v.pub
	hook := s.pageHook
	s.mu.Unlock()

	total := v.src.Total()
	offset = min(max(offset, 0), total)
	v.mu.Lock()
	if c == nil {
		for i := range v.resume {
			if v.resume[i].Offset == offset {
				rc := v.resume[i] // a copy: the slots are reused
				c = &rc
			}
		}
	}
	v.mu.Unlock()
	if hook != nil {
		hook()
	}
	items, next, err := v.src.Page(offset, limit, c)
	if err != nil {
		return Page{}, err
	}
	s.mu.Lock()
	ok = s.live(v)
	s.mu.Unlock()
	if !ok {
		return Page{}, ErrExpired
	}
	v.mu.Lock()
	// At most resumeEntries cursors, in slots that are reused: a dropped
	// cursor's positions are not kept.
	if len(v.resume) == resumeEntries {
		copy(v.resume, v.resume[1:])
		v.resume[len(v.resume)-1] = next
	} else {
		if v.resume == nil {
			v.resume = make([]Cursor, 0, resumeEntries)
		}
		v.resume = append(v.resume, next)
	}
	v.mu.Unlock()
	p := Page{Head: v.head, Identity: v.key.Identity, inc: v.inc, Extra: v.extra, Items: items, Total: total, Offset: offset, ViewID: id}
	if end := offset + len(items); end < total {
		p.Next = PageToken(id, end)
	}
	return p, nil
}

// Current is a page's last check, when nothing but its response remains:
// whether its account's incarnation and newest identity are still its
// view's. A purge, or a commit that raised the generation or moved the
// lineage (which retires the account before its write returns), is either
// ordered before this check, and the page is not served, or after the
// response. O(1).
func (s *Spool) Current(account string, p Page) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	return p.inc == a.inc && a.seen && p.Identity == a.newest
}

// live reports whether a view is still valid: its account's incarnation and
// newest identity are its own, and its version is retained. s.mu is held.
func (s *Spool) live(v *view) bool {
	a := s.account(v.account)
	return v.inc == a.inc && a.seen && v.key.Identity == a.newest && a.keeps(v.pub)
}

// retain records the read model's publication of cur, prev being the
// version it replaced. Retained are cur, prev (under the same incarnation)
// and, under a lease, the versions of at most maxLeased other views of the
// same incarnation read in the last IdleExpiry, the most recently read
// first: a person's held list survives the commits a playing phone makes.
// The views of any other version expire at once, so no view keeps a version
// beyond these reachable.
func (s *Spool) retain(account string, cur, prev *Library) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	keep := map[uint64]bool{cur.pub: true}
	if prev != nil && prev.inc == cur.inc {
		keep[prev.pub] = true
	}
	recent := map[uint64]uint64{} // lease candidates: their latest read
	for _, v := range s.views {
		if v.account == account && v.inc == cur.inc && v.pub != 0 && !keep[v.pub] && s.now().Sub(v.used) <= IdleExpiry {
			recent[v.pub] = max(recent[v.pub], v.recent)
		}
	}
	leased := slices.SortedFunc(maps.Keys(recent), func(x, y uint64) int { return cmp.Compare(recent[y], recent[x]) })
	for _, pub := range leased[:min(len(leased), maxLeased)] {
		keep[pub] = true
	}
	a.retained, a.tracked = keep, true
	for id, v := range s.views {
		if v.account == account && !a.keeps(v.pub) {
			delete(s.views, id)
		}
	}
}

// pinned lists what the account's views and admitted responses keep
// reachable: the distinct publications, and the libraries made elsewhere
// (each counted alone).
func (s *Spool) pinned(account string) (pubs map[uint64]bool, others int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pubs = map[uint64]bool{}
	for _, v := range s.views {
		if v.account == account {
			pubs[v.pub] = true
		}
	}
	if a := s.accounts[account]; a != nil {
		for _, pub := range a.holds {
			pubs[pub] = true
		}
	}
	delete(pubs, 0)
	for _, v := range s.views {
		if v.account == account && v.pub == 0 {
			others++
		}
	}
	return pubs, others
}

// HookPagesForTesting has f called before each page is encoded (after the
// view was found live), for tests that act while a page is being made.
func (s *Spool) HookPagesForTesting(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageHook = f
}

// Sweep removes views idle for longer than IdleExpiry.
func (s *Spool) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, v := range s.views {
		if s.now().Sub(v.used) > IdleExpiry {
			delete(s.views, id)
		}
	}
}

// Views counts the registered views (for tests).
func (s *Spool) Views() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.views)
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

// ---- the sources

// Page encodes the list's rows at the cursor, as the read API's list rows
// (json.Marshal of EpisodeJSON).
func (l *EpisodeList) Page(offset, limit int, c *Cursor) ([]json.RawMessage, Cursor, error) {
	rows, next := l.src.window(offset, c, limit)
	items := make([]json.RawMessage, 0, len(rows))
	for _, e := range rows {
		m, err := EpisodeJSON(l.v, e)
		if err != nil {
			return nil, next, err
		}
		b, err := json.Marshal(m)
		if err != nil {
			return nil, next, err
		}
		items = append(items, b)
	}
	return items, next, nil
}

// SubscriptionList is the subscription list of a version, computed again
// for each page (O(P log P)): a view keeps no rows.
type SubscriptionList struct {
	lib *Library
	o   SubscriptionOptions
	n   int
}

// SubscriptionView is the subscription list as a view's source.
func (l *Library) SubscriptionView(o SubscriptionOptions) *SubscriptionList {
	return &SubscriptionList{lib: l, o: o, n: len(l.Subscriptions(o))}
}

// Total is the list's length.
func (s *SubscriptionList) Total() int { return s.n }

// Page encodes the rows at offset, each a podcast row with its counter.
func (s *SubscriptionList) Page(offset, limit int, _ *Cursor) ([]json.RawMessage, Cursor, error) {
	rows := s.lib.Subscriptions(s.o)
	end := min(offset+limit, len(rows))
	var items []json.RawMessage
	for _, r := range rows[min(offset, end):end] {
		p, err := PodcastJSON(r.Podcast)
		if err != nil {
			return nil, Cursor{}, err
		}
		p["counter"] = r.Counter
		b, err := json.Marshal(p)
		if err != nil {
			return nil, Cursor{}, err
		}
		work.rowsEncoded.Add(1)
		items = append(items, b)
	}
	return items, Cursor{Offset: end}, nil
}
