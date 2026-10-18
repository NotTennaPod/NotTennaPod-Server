package views_test

// The registry's identity fence and final check, anchors in every order and
// the queue, and Home's independence of the history's length.

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// variant is lib with its identity or clock changed.
func variant(lib *views.Library, f func(v *views.Version, h *storage.Head)) *views.Library {
	v := *lib.V
	h := lib.Head
	f(&v, &h)
	return &views.Library{Head: h, V: &v}
}

// A delayed request of an older identity (an earlier epoch or generation of
// the same log) or of another log neither registers a view nor expires the
// newest one's; observations are ordered within a log, whatever order they
// arrive in.
func TestRegistryIdentityIsMonotone(t *testing.T) {
	s := openShaped(t, "i", repeatShape(2, viewstest.Shape{Episodes: 6, Queued: 1}))
	lib, _ := libOf(t, s)
	src := everyEpisodeList(lib)
	a := s.account
	for _, c := range []struct {
		name  string
		newer *views.Library
	}{
		{"a later epoch", variant(lib, func(_ *views.Version, h *storage.Head) { h.Epoch++ })},
		{"a later generation", variant(lib, func(v *views.Version, _ *storage.Head) { v.Clock.Generation++ })},
		{"a later epoch at a lower generation", variant(lib, func(v *views.Version, h *storage.Head) { h.Epoch++; v.Clock.Generation-- })},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, delayed := range []string{"Put", "Observe"} {
				reg, _ := newRegistry(t)
				old, err := reg.Put(a, 0, lib, "q", src, map[string]any{})
				if err != nil {
					t.Fatal(err)
				}
				id, err := reg.Put(a, 0, c.newer, "q", src, map[string]any{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := reg.Read(a, old, "q", 0, 3, nil, lib.Identity()); err != views.ErrExpired {
					t.Fatalf("the older view after the newer one: %v", err)
				}
				switch delayed {
				case "Put":
					if _, err := reg.Put(a, 0, lib, "q", src, map[string]any{}); err != views.ErrRetired {
						t.Fatalf("a delayed Put of the older identity: %v", err)
					}
				default:
					reg.Observe(a, lib)
				}
				if _, err := reg.Read(a, id, "q", 0, 3, nil, c.newer.Identity()); err != nil {
					t.Fatalf("the newer view after a delayed %s of the older identity: %v", delayed, err)
				}
			}
		})
	}
	// Another log: not ordered against the newest, so neither newer nor
	// accepted.
	reg, _ := newRegistry(t)
	id, err := reg.Put(a, 0, lib, "q", src, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	other := variant(lib, func(v *views.Version, h *storage.Head) {
		h.LogID = "another log"
		h.Epoch += 5
		v.Clock.Generation += 5
	})
	reg.Observe(a, other)
	if _, err := reg.Put(a, 0, other, "q", src, map[string]any{}); err != views.ErrRetired {
		t.Fatalf("a Put of another log: %v", err)
	}
	if _, err := reg.Read(a, id, "q", 0, 3, nil, lib.Identity()); err != nil {
		t.Fatalf("the view after another log was seen: %v", err)
	}
	// A reader's identity that is not the view's (the account moved on):
	// expired, and the view with it.
	later := variant(lib, func(v *views.Version, _ *storage.Head) { v.Clock.Generation++ })
	if _, err := reg.Read(a, id, "q", 0, 3, nil, later.Identity()); err != views.ErrExpired {
		t.Fatalf("a read at a newer identity: %v", err)
	}
	if _, err := reg.Read(a, id, "q", 0, 3, nil, lib.Identity()); err != views.ErrExpired {
		t.Fatalf("the view after a read at a newer identity: %v", err)
	}
}

// A page being made when its account is retired, or when a newer identity
// is seen, is not released: the read reports the view expired.
func TestPagesAreNotReleasedAfterInvalidation(t *testing.T) {
	s := openShaped(t, "x", repeatShape(3, viewstest.Shape{Episodes: 8, Queued: 1}))
	lib, _ := libOf(t, s)
	src := everyEpisodeList(lib)
	a := s.account
	newer := variant(lib, func(v *views.Version, _ *storage.Head) { v.Clock.Generation++ })
	for _, c := range []struct {
		name   string
		during func(reg *views.Spool)
		want   error
	}{
		{"nothing", func(*views.Spool) {}, nil},
		{"a purge", func(reg *views.Spool) { reg.Retire(a) }, views.ErrExpired},
		{"a newer generation", func(reg *views.Spool) { reg.Observe(a, newer) }, views.ErrExpired},
		{"another account's purge", func(reg *views.Spool) { reg.Retire("another") }, nil},
	} {
		reg, _ := newRegistry(t)
		id, err := reg.Put(a, 0, lib, "q", src, map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		reg.HookPagesForTesting(func() {
			if calls++; calls == 1 {
				c.during(reg)
			}
		})
		page, err := reg.Read(a, id, "q", 0, 5, nil, lib.Identity())
		if err != c.want || err == nil && len(page.Items) != 5 {
			t.Fatalf("%s while the page was made: %v, %d rows", c.name, err, len(page.Items))
		}
		if calls != 1 {
			t.Fatalf("%s: the page was made %d times", c.name, calls)
		}
	}
}

// Anchored windows in every order: on a library with ties in titles, dates, durations and
// sizes and a queue whose order is not its episodes' IDs', an anchor at
// any stored episode, in each sort order's list of every episode, of the
// played ones (most anchors left out) and of one podcast, starts where the
// oracle's sort puts it, before 0 or 3 rows; in the queue, at a queued
// episode's place and, for an episode not queued (or not stored), at its
// index.
func TestAnchorsInEveryOrder(t *testing.T) {
	store, account := openLibrary(t, viewstest.Spec{Seed: 4, Podcasts: []int{14, 9, 12, 7}, Queue: 14, Now: viewstest.FixedNow, Months: 6, Prefix: "k"}, false)
	ctx := context.Background()
	// Reorder the queue against the episodes' order.
	var queued []string
	if err := store.Read(ctx, func(r *storage.ReadTx) error {
		ref, err := reference.Load(r, account)
		for _, e := range ref.Queue {
			queued = append(queued, e.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a := tx.Account(account)
		perm := rand.New(rand.NewPCG(4, 4)).Perm(len(queued))
		for i, id := range queued {
			if err := put(a, "episode", id, "queue_position", fmt.Sprintf(`{"key":%q}`, viewstest.QueueKeyAt(perm[i]))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var lib *views.Library
	var ref *reference.Library
	if err := store.Read(ctx, func(r *storage.ReadTx) error {
		acct := r.Account(account)
		v, err := views.BuildVersion(acct, account)
		if err != nil {
			return err
		}
		head, err := acct.Head()
		lib = &views.Library{Head: head, V: v}
		if err != nil {
			return err
		}
		ref, err = reference.Load(r, account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(ref.Queue) < 10 || slices.IsSortedFunc(ref.Queue, func(a, b *reference.Episode) int { return strings.Compare(a.ID, b.ID) }) {
		t.Fatalf("a queue of %d in its IDs' order", len(ref.Queue))
	}
	idsOf := func(raws []json.RawMessage) []string {
		var out []string
		for _, r := range raws {
			out = append(out, rowID(r))
		}
		return out
	}
	var pods []string
	for id := range ref.Podcasts {
		pods = append(pods, id)
	}
	slices.Sort(pods)
	check := func(what string, list *views.EpisodeList, want []string, place func(e *reference.Episode) (int, bool)) {
		t.Helper()
		for _, e := range ref.Episodes {
			for _, before := range []int{0, 3} {
				at, ok := place(e)
				c, found := list.Anchor(e.ID, 5, before)
				if !ok { // no place: the index
					at = min(5, len(want))
				}
				start := max(0, at-before)
				if found != ok || c.Offset != start {
					t.Fatalf("%s: the anchor %s, %d before, at %d (%v); want %d (%v)", what, e.ID, before, c.Offset, found, start, ok)
				}
				rows, _ := list.Read(c, 7)
				for i, r := range rows {
					if r.ID != want[start+i] {
						t.Fatalf("%s: the window at %s, row %d is %s, the oracle's %s", what, e.ID, start+i, r.ID, want[start+i])
					}
				}
				if len(rows) != min(7, len(want)-start) {
					t.Fatalf("%s: the window at %s has %d rows", what, e.ID, len(rows))
				}
			}
		}
		// A gone anchor: its index, clamped.
		for _, idx := range []int{0, 4, len(want) + 9} {
			c, found := list.Anchor("e1:gone", idx, 2)
			if found || c.Offset != max(0, min(idx, len(want))-2) {
				t.Fatalf("%s: a gone anchor at %d: %d (%v)", what, idx, c.Offset, found)
			}
		}
	}
	lists := 0
	for _, order := range views.OrderNames {
		for _, fq := range []struct{ filter, podcast string }{
			{"include_subscribed,include_archived,include_not_subscribed", ""},
			{"played,include_subscribed,include_archived,include_not_subscribed", ""},
			{"", pods[1]},
		} {
			f, err := views.ParseFilter(fq.filter)
			if err != nil {
				t.Fatal(err)
			}
			if fq.podcast != "" {
				f = f.AllStates() // as the podcast's list is served
			}
			list := lib.Episodes(views.ListQuery{Filter: f, Order: order, Podcast: fq.podcast})
			path := "/v1/read/episodes"
			if fq.podcast != "" {
				path = "/v1/read/podcasts/{id}/episodes"
			}
			raws, _, _ := ref.ListItems(path, fq.podcast, url.Values{"filter": {fq.filter}, "sort": {order}})
			want := idsOf(raws)
			listed := []*reference.Episode{}
			for _, id := range want {
				listed = append(listed, ref.ByID[id])
			}
			place := func(e *reference.Episode) (int, bool) {
				if i := slices.Index(want, e.ID); i >= 0 {
					return i, true
				}
				// Where the oracle's sort puts it among the list's rows.
				with := append(slices.Clone(listed), e)
				slices.SortStableFunc(with, func(a, b *reference.Episode) int {
					return cmp.Or(cmp.Compare(a.Rank, b.Rank), strings.Compare(a.ID, b.ID))
				})
				reference.Sort(with, order)
				return slices.Index(with, e), true
			}
			check(fmt.Sprintf("%s %q %s", order, fq.filter, fq.podcast), list, want, place)
			lists++
		}
	}
	raws, _, _ := ref.ListItems("/v1/read/queue", "", nil)
	want := idsOf(raws)
	check("the queue", lib.Queue(), want, func(e *reference.Episode) (int, bool) {
		i := slices.Index(want, e.ID)
		return i, i >= 0
	})
	t.Logf("%d lists and the queue, %d anchors each", lists, 2*len(ref.Episodes))
}

// Home's work does not grow with the history's months: one podcast of 500
// episodes played over 1, 12, 120 and 240 months shows the same sections
// for the same work (the classics read the podcasts' rows, not the months).
func TestHomeWorkDoesNotGrowWithHistory(t *testing.T) {
	var base views.WorkCounts
	var baseHome string
	for i, months := range []int{1, 12, 120, 240} {
		s := openShaped(t, "h", []viewstest.Shape{{Episodes: 500, Queued: 20, Months: months}, {Episodes: 10, Queued: 2, Months: 1}})
		lib, _ := libOf(t, s)
		var h views.Home
		w := measure(func() { h = lib.Home(viewstest.FixedNow, 7, "date_new_old", false) })
		var b strings.Builder
		for _, list := range [][]*views.EpisodeItem{h.Continue, h.New, h.Surprise} {
			for _, e := range list {
				fmt.Fprintf(&b, "%s,", e.ID)
			}
			b.WriteString("|")
		}
		for _, p := range h.Classics {
			fmt.Fprintf(&b, "%s,", p.ID)
		}
		t.Logf("%d months: %+v; %s", months, w, b.String())
		if i == 0 {
			base, baseHome = w, b.String()
			continue
		}
		if b.String() != baseHome {
			t.Fatalf("%d months: Home %s, at one month %s", months, b.String(), baseHome)
		}
		// The trees' shapes differ a little; the months would add one node
		// each.
		if slack := 2*lg(510)*2 + 8; w.NodesVisited > base.NodesVisited+slack {
			t.Fatalf("Home over %d months visited %d nodes, %d over one month: more than %d more", months, w.NodesVisited, base.NodesVisited, slack)
		}
	}
}
