package views_test

// The views registry and its purge fence: pages at any offset, repeated and
// out of order and concurrent; pages pinned to their version; anchored
// windows at every offset; registration fenced by incarnation, lineage
// and generation; reuse only of an identical view; expiry by idleness, the
// cap, a generation change and a purge.

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// libOf is the shaped library's current version as a library, and the
// oracle's library at the same snapshot.
func libOf(t *testing.T, s *shaped) (*views.Library, *reference.Library) {
	t.Helper()
	var ref *reference.Library
	if err := s.store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		ref, err = reference.Load(r, s.account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if ref.Head.Seq != s.v.Head.Seq || ref.Head.LogID != s.v.Head.LogID {
		t.Fatal("the version is not the snapshot's")
	}
	return &views.Library{Head: s.v.Head, V: s.v}, ref
}

// clock is a settable registry clock.
type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.at = c.at.Add(d); c.mu.Unlock() }

func newRegistry(t *testing.T) (*views.Spool, *clock) {
	t.Helper()
	c := &clock{at: time.UnixMilli(viewstest.FixedNow)}
	s, err := views.NewSpool("", c.now)
	if err != nil {
		t.Fatal(err)
	}
	return s, c
}

// expect compares a page with the oracle's rows from its offset.
func expect(t *testing.T, what string, page views.Page, want []json.RawMessage, offset, limit int) {
	t.Helper()
	end := min(len(want), offset+limit)
	if page.Offset != offset || page.Total != len(want) || len(page.Items) != max(0, end-offset) {
		t.Fatalf("%s: offset %d, total %d, %d rows; want %d, %d, %d", what, page.Offset, page.Total, len(page.Items), offset, len(want), end-offset)
	}
	for i, it := range page.Items {
		if !bytes.Equal(it, want[offset+i]) {
			t.Fatalf("%s: row %d differs from the oracle's", what, offset+i)
		}
	}
}

var everyEpisode = url.Values{"filter": {"include_subscribed,include_archived,include_not_subscribed"}}

func everyEpisodeList(lib *views.Library) *views.EpisodeList {
	f, _ := views.ParseFilter(everyEpisode.Get("filter"))
	return lib.Episodes(views.ListQuery{Filter: f, Order: "date_new_old"})
}

// Pages of random sizes at random offsets, in any order and again,
// and read concurrently, are the oracle's rows.
func TestViewPagesAtAnyOffset(t *testing.T) {
	s := openShaped(t, "a", repeatShape(20, viewstest.Shape{Episodes: 40, Queued: 4, Months: 2}))
	lib, ref := libOf(t, s)
	want, _, _ := ref.ListItems("/v1/read/episodes", "", everyEpisode)
	reg, _ := newRegistry(t)
	id, err := reg.Put(s.account, 0, lib, "q", everyEpisodeList(lib), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(5, 6))
	for i := range 300 {
		offset, limit := r.IntN(len(want)+5), 1+r.IntN(200)
		page, err := reg.Read(s.account, id, "q", offset, limit, nil, lib.Identity())
		if err != nil {
			t.Fatal(err)
		}
		expect(t, fmt.Sprintf("read %d at %d", i, offset), page, want, min(offset, len(want)), limit)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 9))
			for range 50 {
				offset, limit := r.IntN(len(want)), 1+r.IntN(100)
				page, err := reg.Read(s.account, id, "q", offset, limit, nil, lib.Identity())
				if err != nil {
					t.Error(err)
					return
				}
				end := min(len(want), offset+limit)
				for i, it := range page.Items {
					if !bytes.Equal(it, want[offset+i]) || offset+len(page.Items) != end {
						t.Errorf("a concurrent read at %d differs", offset)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if reg.Views() != 1 {
		t.Fatalf("%d views", reg.Views())
	}
}

// A view's pages come from its own version: after commits change
// listed episodes (play state, title) and their podcast (title, state),
// and a view of the new snapshot is registered, the old view's later pages
// are still the old snapshot's rows; the new view's are the new ones.
func TestViewPagesPinTheirVersion(t *testing.T) {
	s := openShaped(t, "b", repeatShape(6, viewstest.Shape{Episodes: 30, Queued: 3, Months: 1}))
	lib0, ref0 := libOf(t, s)
	want0, _, _ := ref0.ListItems("/v1/read/episodes", "", everyEpisode)
	reg, _ := newRegistry(t)
	id0, err := reg.Put(s.account, 0, lib0, "q", everyEpisodeList(lib0), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := reg.Read(s.account, id0, "q", 0, 20, nil, lib0.Identity())
	if err != nil {
		t.Fatal(err)
	}
	expect(t, "the first page", first, want0, 0, 20)
	s.commit(func(a *storage.AccountTx) error {
		for _, e := range s.lib.Episodes[0][:10] {
			if err := put(a, "episode", e, "play_state", `{"generation_id":"g","play_state":"played"}`); err != nil {
				return err
			}
			if err := edit(a, "episode", e, "metadata", map[string]any{"title": "Changed"}, `["c"]`); err != nil {
				return err
			}
		}
		if err := edit(a, "podcast", s.lib.Podcasts[0], "metadata", map[string]any{"title": "Renamed"}, `["c"]`); err != nil {
			return err
		}
		return edit(a, "podcast", s.lib.Podcasts[1], "subscription", map[string]any{"state": "archived"}, `["c"]`)
	})
	lib1, ref1 := libOf(t, s)
	want1, _, _ := ref1.ListItems("/v1/read/episodes", "", everyEpisode)
	id1, err := reg.Put(s.account, 0, lib1, "q", everyEpisodeList(lib1), map[string]any{})
	if err != nil || id1 == id0 {
		t.Fatalf("the new snapshot's view: %v (%v)", id1, err)
	}
	for offset := 20; offset < len(want0); offset += 20 {
		page, err := reg.Read(s.account, id0, "q", offset, 20, nil, lib0.Identity())
		if err != nil {
			t.Fatal(err)
		}
		expect(t, fmt.Sprintf("the old view at %d", offset), page, want0, offset, 20)
	}
	page, err := reg.Read(s.account, id1, "q", 0, 50, nil, lib1.Identity())
	if err != nil {
		t.Fatal(err)
	}
	expect(t, "the new view", page, want1, 0, 50)
	if bytes.Equal(want0[0], want1[0]) && bytes.Equal(want0[20], want1[20]) {
		t.Fatal("the commits changed nothing listed")
	}
}

// Anchored windows at every offset of a 2,000-row list are the
// oracle's slices, and so are the pages before them and the start; an
// anchor the list does not show starts where it would be, and one the
// version does not store falls back to its index.
func TestAnchoredWindows(t *testing.T) {
	s := openShaped(t, "w", repeatShape(50, viewstest.Shape{Episodes: 40, Queued: 2, Months: 1}))
	lib, ref := libOf(t, s)
	all := everyEpisodeList(lib)
	want, _, _ := ref.ListItems("/v1/read/episodes", "", everyEpisode)
	if len(want) != 2000 {
		t.Fatalf("%d rows", len(want))
	}
	var ids []string
	for _, w := range want {
		var row struct {
			ID string `json:"episode_id"`
		}
		json.Unmarshal(w, &row)
		ids = append(ids, row.ID)
	}
	reg, _ := newRegistry(t)
	id, err := reg.Put(s.account, 0, lib, "q", all, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(3, 4))
	for o := range want {
		before, limit := o%201, 1+r.IntN(300)
		c, found := all.Anchor(ids[o], 0, before)
		if !found {
			t.Fatalf("anchor %s not found", ids[o])
		}
		start := max(0, o-before)
		page, err := reg.Read(s.account, id, "q", c.Offset, limit, &c, lib.Identity())
		if err != nil {
			t.Fatal(err)
		}
		expect(t, fmt.Sprintf("the window at %d, %d before", o, before), page, want, start, limit)
		if o%97 == 0 { // the page before, and the start
			prev, err := reg.Read(s.account, id, "q", max(0, start-limit), limit, nil, lib.Identity())
			if err != nil {
				t.Fatal(err)
			}
			expect(t, fmt.Sprintf("the page before the window at %d", o), prev, want, max(0, start-limit), limit)
			first, err := reg.Read(s.account, id, "q", 0, limit, nil, lib.Identity())
			if err != nil {
				t.Fatal(err)
			}
			expect(t, "the start", first, want, 0, limit)
		}
	}
	// An anchor the list leaves out: played episodes only, anchored at an
	// unplayed one, start where it would rank in the oracle's sort.
	played, _ := views.ParseFilter("played,include_subscribed,include_archived,include_not_subscribed")
	s.commit(func(a *storage.AccountTx) error {
		for i, eps := range s.lib.Episodes {
			for j, e := range eps {
				if (i+j)%3 == 0 {
					if err := put(a, "episode", e, "play_state", `{"generation_id":"g","play_state":"played"}`); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	lib, ref = libOf(t, s)
	list := lib.Episodes(views.ListQuery{Filter: played, Order: "date_new_old"})
	outside := s.lib.Episodes[0][1] // (0+1)%3 != 0: unplayed
	c, found := list.Anchor(outside, 0, 0)
	refPlayed := ref.EpisodesWhere(reference.Filter(played), "date_new_old", "")
	withAnchor := append([]*reference.Episode(nil), refPlayed...)
	withAnchor = append(withAnchor, ref.ByID[outside])
	slices.SortStableFunc(withAnchor, func(a, b *reference.Episode) int {
		return cmp.Or(cmp.Compare(a.Rank, b.Rank), strings.Compare(a.ID, b.ID))
	})
	reference.Sort(withAnchor, "date_new_old")
	at := slices.IndexFunc(withAnchor, func(e *reference.Episode) bool { return e.ID == outside })
	if !found || c.Offset != at {
		t.Fatalf("an anchor the list leaves out: at %d (%v), it would rank at %d", c.Offset, found, at)
	}
	// An anchor the version does not store: its index, clamped.
	for _, idx := range []int{0, 7, list.Total() + 50} {
		c, found := list.Anchor("e1:gone", idx, 3)
		if found || c.Offset != max(0, min(idx, list.Total())-3) {
			t.Fatalf("a gone anchor at index %d: %d (%v)", idx, c.Offset, found)
		}
	}
}

// The registry's fences. A view of an older
// generation than one the registry has seen is refused; identical first
// requests share a view, but another tick, generation, head or incarnation
// never does; a view planned under a retired incarnation is refused, and
// a token of a retired view expires.
func TestViewRegistrationFences(t *testing.T) {
	s := openShaped(t, "f", repeatShape(3, viewstest.Shape{Episodes: 10, Queued: 2}))
	lib, _ := libOf(t, s)
	reg, _ := newRegistry(t)
	src := everyEpisodeList(lib)
	a := s.account
	id, err := reg.Put(a, 0, lib, "q", src, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if again, err := reg.Put(a, 0, lib, "q", src, map[string]any{}); err != nil || again != id {
		t.Fatalf("an identical first request made another view: %s, %s (%v)", again, id, err)
	}
	variant := func(f func(v *views.Version, h *storage.Head)) *views.Library {
		v := *lib.V
		h := lib.Head
		f(&v, &h)
		return &views.Library{Head: h, V: &v}
	}
	for name, l := range map[string]*views.Library{
		"another tick": variant(func(v *views.Version, _ *storage.Head) { v.Clock.Tick++ }),
		"another head": variant(func(_ *views.Version, h *storage.Head) { h.Seq++ }),
		"other params": lib,
	} {
		params := "q"
		if name == "other params" {
			params = "q2"
		}
		other, err := reg.Put(a, 0, l, params, src, map[string]any{})
		if err != nil || other == id {
			t.Fatalf("%s: reused the view (%v)", name, err)
		}
	}
	// A newer generation expires the older views; a delayed Put of the
	// older generation is refused, not reused.
	newer := variant(func(v *views.Version, _ *storage.Head) { v.Clock.Generation++ })
	nid, err := reg.Put(a, 0, newer, "q", src, map[string]any{})
	if err != nil || nid == id {
		t.Fatalf("a newer generation's view: %v", err)
	}
	if _, err := reg.Read(a, id, "q", 0, 5, nil, lib.Identity()); err != views.ErrExpired {
		t.Fatalf("an older generation's view after a newer one: %v", err)
	}
	if _, err := reg.Put(a, 0, lib, "q", src, map[string]any{}); err != views.ErrRetired {
		t.Fatalf("a delayed Put of the older generation: %v", err)
	}
	// Planned under incarnation 0, purged before its registration.
	inc := reg.Incarnation(a)
	reg.Retire(a)
	if _, err := reg.Put(a, inc, newer, "q", src, map[string]any{}); err != views.ErrRetired {
		t.Fatalf("a Put under a retired incarnation: %v", err)
	}
	// A token of the view registered before the purge.
	if _, err := reg.Read(a, nid, "q", 0, 5, nil, newer.Identity()); err != views.ErrExpired {
		t.Fatalf("a token of a purged view: %v", err)
	}
	fresh, err := reg.Put(a, reg.Incarnation(a), newer, "q", src, map[string]any{})
	if err != nil || fresh == nid {
		t.Fatalf("the new incarnation's view: %v", err)
	}
	if _, err := reg.Read(a, fresh, "q", 0, 5, nil, newer.Identity()); err != nil {
		t.Fatal(err)
	}
	// Another account reads nothing of this one's.
	if _, err := reg.Read("another", fresh, "q", 0, 5, nil, newer.Identity()); err != views.ErrExpired {
		t.Fatalf("another account's read: %v", err)
	}
	if _, err := reg.Read(a, fresh, "q-other", 0, 5, nil, newer.Identity()); err != views.ErrParams {
		t.Fatalf("a token with other parameters: %v", err)
	}
}

// A view expires after 10 idle minutes, when its account has 64 newer
// views, at a generation change and at a purge.
func TestViewRetention(t *testing.T) {
	s := openShaped(t, "r", repeatShape(2, viewstest.Shape{Episodes: 5}))
	lib, _ := libOf(t, s)
	reg, clk := newRegistry(t)
	src := everyEpisodeList(lib)
	a := s.account
	put := func(params string) string {
		t.Helper()
		id, err := reg.Put(a, reg.Incarnation(a), lib, params, src, map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	cur := lib.Identity()
	read := func(id string) error {
		_, err := reg.Read(a, id, "p0", 0, 5, nil, cur)
		return err
	}
	idle := put("p0")
	clk.add(9 * time.Minute)
	if err := read(idle); err != nil {
		t.Fatalf("read after 9 minutes: %v", err)
	}
	clk.add(10*time.Minute + time.Second)
	if err := read(idle); err != views.ErrExpired {
		t.Fatalf("read after 10 idle minutes: %v", err)
	}
	// The cap: the least recently read of 65 goes.
	ids := []string{put("p0")}
	for i := 1; i <= 64; i++ {
		clk.add(time.Second)
		ids = append(ids, put(fmt.Sprintf("p%d", i)))
	}
	if _, err := reg.Read(a, ids[0], "p0", 0, 5, nil, lib.Identity()); err != views.ErrExpired {
		t.Fatalf("the 65th view kept the least recently read: %v", err)
	}
	if _, err := reg.Read(a, ids[1], "p1", 0, 5, nil, lib.Identity()); err != nil {
		t.Fatalf("a view within the cap: %v", err)
	}
	// The same with the clock standing still: still the least recently
	// read goes (a view just read, or just made, never does).
	still := []string{put("s0")}
	for i := 1; i <= 64; i++ {
		still = append(still, put(fmt.Sprintf("s%d", i)))
		if _, err := reg.Read(a, still[0], "s0", 0, 5, nil, lib.Identity()); err != nil {
			t.Fatalf("the first view, read after %d more: %v", i, err)
		}
	}
	if _, err := reg.Read(a, still[1], "s1", 0, 5, nil, lib.Identity()); err != views.ErrExpired {
		t.Fatalf("with the clock still, the cap kept the least recently read: %v", err)
	}
	if _, err := reg.Read(a, still[64], "s64", 0, 5, nil, lib.Identity()); err != nil {
		t.Fatalf("with the clock still, the newest view: %v", err)
	}
	// A generation change, then a purge.
	v := *lib.V
	v.Clock.Generation++
	reg.Observe(a, &views.Library{Head: lib.Head, V: &v})
	if _, err := reg.Read(a, ids[1], "p1", 0, 5, nil, lib.Identity()); err != views.ErrExpired {
		t.Fatalf("a view after a generation change: %v", err)
	}
	newer := &views.Library{Head: lib.Head, V: &v}
	cur = newer.Identity()
	id, err := reg.Put(a, reg.Incarnation(a), newer, "p0", src, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Retire(a)
	if err := read(id); err != views.ErrExpired {
		t.Fatalf("a view after a purge: %v", err)
	}
}
