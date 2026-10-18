package views_test

// Work budgets of the apply: deterministic counters at the
// primitive loops (entities built, records and bytes decoded, tree nodes
// visited and allocated, rows read, full builds), each at two sizes of the
// dimension it must not depend on. A budget fails when the work grows with
// the library, the queue or the history instead of with the change.

import (
	"context"
	"fmt"
	"math/bits"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// shaped is a store whose account holds a shaped library, and its current
// version.
type shaped struct {
	t       *testing.T
	store   *storage.Store
	account string
	lib     *viewstest.Library
	v       *views.Version
	n       int // stored episodes

	applyTime, fullTime time.Duration // the last commit's advance and full build
}

func openShaped(t *testing.T, prefix string, shapes []viewstest.Shape) *shaped {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	viewstest.Skip(store) // compared after each commit here
	var account string
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	lib, err := viewstest.GenerateShaped(ctx, store, account, prefix, viewstest.FixedNow, shapes)
	if err != nil {
		t.Fatal(err)
	}
	s := &shaped{t: t, store: store, account: account, lib: lib}
	if err := store.Read(ctx, func(r *storage.ReadTx) error {
		s.v, err = views.BuildVersion(r.Account(account), account)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	_, s.n, _ = s.v.Counts()
	return s
}

// commit writes fn, advances the version to the commit's snapshot (checked
// against a full build there) and returns the work of the advance alone.
func (s *shaped) commit(fn func(a *storage.AccountTx) error) (views.WorkCounts, views.Path) {
	t := s.t
	t.Helper()
	ctx := context.Background()
	if err := s.store.Write(ctx, func(tx *storage.WriteTx) error { return fn(tx.Account(s.account)) }); err != nil {
		t.Fatal(err)
	}
	var w views.WorkCounts
	var path views.Path
	if err := s.store.Read(ctx, func(r *storage.ReadTx) error {
		acct := r.Account(s.account)
		before, start := views.Work(), time.Now()
		next, p, err := views.Advance(s.v, acct, s.account)
		w, path, s.applyTime = views.Work().Minus(before), p, time.Since(start)
		if err != nil {
			return err
		}
		start = time.Now()
		full, err := views.BuildVersion(acct, s.account)
		s.fullTime = time.Since(start)
		if err != nil {
			return err
		}
		head, err := acct.Head()
		if err != nil {
			return err
		}
		clock, err := acct.ViewClock()
		if err != nil {
			return err
		}
		for what, v := range map[string]*views.Version{p.String(): next, "full build": full} {
			if msg := viewstest.IdentityMismatch(v, s.account, head, clock); msg != "" {
				return fmt.Errorf("the %s %s", what, msg)
			}
		}
		if err := views.EqualVersions(next, full); err != nil {
			return fmt.Errorf("the %s differs from a full build: %w", p, err)
		}
		s.v = next
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return w, path
}

func put(a *storage.AccountTx, entity, id, group, value string) error {
	return a.PutRecord(storage.RecordRow{Entity: entity, EntityID: id, FieldGroup: group, Value: value, Provenance: `["b"]`})
}

// checkpoint is a playback checkpoint: position, last played and played
// time.
func checkpoint(e string, at int64) func(a *storage.AccountTx) error {
	return func(a *storage.AccountTx) error {
		if err := put(a, "episode", e, "position", fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, 60_000+at%1000)); err != nil {
			return err
		}
		if err := put(a, "episode", e, "last_played", fmt.Sprintf(`{"time":%d}`, at)); err != nil {
			return err
		}
		return put(a, "episode", e, "played_time", fmt.Sprintf(`{"played_ms":%d}`, 120_000+at%1000))
	}
}

func log2(n int) int64 { return int64(bits.Len(uint(n))) }

// The budget libraries: 5,000 and 50,400 stored episodes. Podcast 0 is a
// large one with everything queued and played in one month; in the large
// library podcast 1 has 5,000 episodes, 2,000 queued, played over 240
// months; the rest are small.
var (
	budgetSmallShapes  = append([]viewstest.Shape{{Episodes: 4600, Queued: 4600, Months: 1}}, repeatShape(10, viewstest.Shape{Episodes: 40, Queued: 5, Months: 3})...)
	budgetLargeShapes  = append([]viewstest.Shape{{Episodes: 45000, Queued: 45000, Months: 1}, {Episodes: 5000, Queued: 2000, Months: 240}}, repeatShape(10, viewstest.Shape{Episodes: 40, Queued: 5, Months: 3})...)
	budgetSmallPodcast = 1 // a small podcast of each library: the first after the large ones
	budgetLargePodcast = 2
)

func repeatShape(n int, s viewstest.Shape) []viewstest.Shape {
	out := make([]viewstest.Shape, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// TestApplyBudget runs the checkpoint, visibility and payload budgets on
// the two budget libraries.
func TestApplyBudget(t *testing.T) {
	skipUnderRace(t)
	small := openShaped(t, "s", budgetSmallShapes)
	large := openShaped(t, "l", budgetLargeShapes)
	if small.n != 5000 || large.n != 50400 {
		t.Fatalf("libraries of %d and %d episodes", small.n, large.n)
	}
	for _, c := range []struct {
		name string
		f    func(*testing.T, *shaped, *shaped)
	}{{"checkpoint", budgetCheckpoint}, {"visibility", budgetVisibility}, {"payload", budgetPayload}} {
		t.Run(c.name, func(t *testing.T) {
			small.t, large.t = t, t
			c.f(t, small, large)
		})
	}
}

// One playback checkpoint is one apply of one entity, at most 12
// records, nodes within 14 structures × 2 × 4·log₂n, no full build; at
// 50,400 episodes within 1.5 × the work at 5,000 plus a log term. The same
// for 100 checkpoints in one commit: the work is the change's, times its
// size.
func budgetCheckpoint(t *testing.T, small, large *shaped) {
	type result struct {
		one, hundred, inLarge views.WorkCounts
	}
	run := func(s *shaped, pod int) result {
		var r result
		var path views.Path
		r.one, path = s.commit(checkpoint(s.lib.Episodes[pod][3], viewstest.FixedNow-5_000))
		if path != views.PathApply {
			t.Fatalf("a checkpoint was a %s", path)
		}
		t.Logf("a checkpoint in a library of %d: the apply %v, a full build %v", s.n, s.applyTime, s.fullTime)
		r.inLarge, _ = s.commit(checkpoint(s.lib.Episodes[0][17], viewstest.FixedNow-4_000))
		r.hundred, _ = s.commit(func(a *storage.AccountTx) error {
			for i := range 100 {
				p := s.lib.Episodes[0]
				if err := checkpoint(p[(i*37)%len(p)], viewstest.FixedNow-3_000+int64(i))(a); err != nil {
					return err
				}
			}
			return nil
		})
		return r
	}
	rs, rl := run(small, budgetSmallPodcast), run(large, budgetLargePodcast)
	for _, c := range []struct {
		name string
		n    int
		w    views.WorkCounts
	}{{"small library", small.n, rs.one}, {"large library", large.n, rl.one}, {"small library, its large podcast", small.n, rs.inLarge},
		{"large library, its large podcast", large.n, rl.inLarge}} {
		t.Logf("checkpoint, %s: %+v", c.name, c.w)
		nodes := 14 * 2 * 4 * log2(c.n)
		if c.w.Applies != 1 || c.w.FullBuilds != 0 || c.w.EntitiesBuilt != 1 || c.w.RecordsDecoded > 12 || c.w.Changes != 1 || c.w.RowsRead > 14 ||
			c.w.NodesVisited > nodes || c.w.NodesAllocated > nodes {
			t.Errorf("checkpoint, %s: %+v; budget 1 apply, 0 full, 1 entity, ≤ 12 records, 1 change, ≤ 14 rows, ≤ %d nodes", c.name, c.w, nodes)
		}
	}
	t.Logf("100 checkpoints: %+v, then %+v", rs.hundred, rl.hundred)
	if rs.hundred.EntitiesBuilt != 100 || rl.hundred.EntitiesBuilt != 100 || rl.hundred.FullBuilds != 0 || rl.hundred.RecordsDecoded > 1200 {
		t.Errorf("100 checkpoints: %+v at 50,400", rl.hundred)
	}
	logTerm := 14 * 2 * 4 * (log2(large.n) - log2(small.n) + 1)
	for _, c := range []struct {
		name string
		s, l int64
		per  int64
	}{
		{"nodes visited", rs.one.NodesVisited, rl.one.NodesVisited, 1},
		{"nodes allocated", rs.one.NodesAllocated, rl.one.NodesAllocated, 1},
		{"records decoded", rs.one.RecordsDecoded, rl.one.RecordsDecoded, 1},
		{"rows read", rs.one.RowsRead, rl.one.RowsRead, 1},
		{"nodes visited, 100", rs.hundred.NodesVisited, rl.hundred.NodesVisited, 100},
		{"nodes allocated, 100", rs.hundred.NodesAllocated, rl.hundred.NodesAllocated, 100},
		{"rows read, 100", rs.hundred.RowsRead, rl.hundred.RowsRead, 100},
	} {
		if c.l > c.s*3/2+c.per*logTerm {
			t.Errorf("%s: %d at 50,400 episodes, %d at 5,000: more than 1.5 × plus %d", c.name, c.l, c.s, c.per*logTerm)
		}
	}
	if rl.hundred.NodesVisited > 100*14*2*4*log2(large.n) || rl.hundred.RowsRead > 100*14 {
		t.Errorf("100 checkpoints: %+v, more than 100 × one's budget", rl.hundred)
	}
}

// With 1, 12, 120 and 240 months of history, a checkpoint that moves
// played time between months copies O(log M) of the months, never the
// months: the work at 240 months is within 1.5 × the work at 1 plus a log
// term.
func TestApplyBudgetMonths(t *testing.T) {
	var work []views.WorkCounts
	months := []int{1, 12, 120, 240}
	for _, m := range months {
		s := openShaped(t, fmt.Sprintf("h%d", m), []viewstest.Shape{{Episodes: 500, Queued: 20, Months: m}, {Episodes: 10, Months: 1}})
		if got := views.MonthCount(s.v); got != m {
			t.Fatalf("%d months of history, want %d", got, m)
		}
		// Played now: this month's sum grows, the episode's old month's
		// shrinks.
		w, _ := s.commit(checkpoint(s.lib.Episodes[0][len(s.lib.Episodes[0])-1], viewstest.FixedNow-1_000))
		t.Logf("%d months: %+v", m, w)
		work = append(work, w)
	}
	first := work[0]
	for i, w := range work {
		term := 2 * 2 * 4 * (log2(months[i]) + 1)
		if w.NodesAllocated > first.NodesAllocated*3/2+term || w.NodesVisited > first.NodesVisited*3/2+term || w.FullBuilds != 0 || w.EntitiesBuilt != 1 {
			t.Errorf("%d months: %+v; at 1 month %+v (the log term %d)", months[i], w, first, term)
		}
	}
}

// A podcast hidden and shown again touches only the podcasts and its
// own months: no queue, Continue, list or statistics structure is re-keyed
// (they stay shared whole), and the nodes visited are within a constant
// plus M_p·log M, whatever the podcast's queue (4,600 or 45,000 queued) or
// the library (5,000 or 50,400).
func budgetVisibility(t *testing.T, small, large *shaped) {
	toggle := func(s *shaped, pod int, monthsOfPodcast int) (hide, show views.WorkCounts) {
		t.Helper()
		p := s.lib.Podcasts[pod]
		for i, state := range []string{"removed", "subscribed"} {
			before := s.v
			beforeMonths := views.MonthCount(before)
			w, path := s.commit(func(a *storage.AccountTx) error {
				return put(a, "podcast", p, "subscription", fmt.Sprintf(`{"generation_id":"g1:%016x","state":%q}`, pod, state))
			})
			if path != views.PathApply {
				t.Fatalf("a visibility change was a %s", path)
			}
			for name, same := range views.SharedStructures(before, s.v) {
				if !same && name != "podcasts" && name != "months" {
					t.Errorf("podcast %d %s: the %s changed", pod, state, name)
				}
			}
			m := int64(max(beforeMonths, views.MonthCount(s.v)))
			budget := 64 + int64(monthsOfPodcast)*2*4*(log2(int(m))+1)
			if w.NodesVisited > budget || w.EntitiesBuilt != 1 || w.FullBuilds != 0 {
				t.Errorf("podcast %d %s: %+v; budget %d nodes", pod, state, w, budget)
			}
			t.Logf("podcast %d (%d months) %s, library of %d: %+v", pod, monthsOfPodcast, state, s.n, w)
			if i == 0 {
				hide = w
			} else {
				show = w
			}
		}
		return hide, show
	}
	sh, ss := toggle(small, 0, 1)
	lh, ls := toggle(large, 0, 1)
	toggle(large, 1, 240)
	logTerm := 2 * 4 * (log2(large.n) - log2(small.n) + 1)
	if lh.NodesVisited > sh.NodesVisited*3/2+logTerm || ls.NodesVisited > ss.NodesVisited*3/2+logTerm {
		t.Errorf("hiding and showing a podcast of 45,000 queued in 50,400: %d, %d nodes; of 4,600 in 5,000: %d, %d", lh.NodesVisited, ls.NodesVisited,
			sh.NodesVisited, ss.NodesVisited)
	}
}

// One changed episode whose records are 1 KiB or 64 KiB: the apply
// decodes that episode's bytes and no others, and its other work does not
// change with them, nor with the library.
func budgetPayload(t *testing.T, small, large *shaped) {
	run := func(s *shaped, size int) (views.WorkCounts, int64) {
		e := s.lib.Episodes[budgetSmallPodcast][4]
		if s == large {
			e = s.lib.Episodes[budgetLargePodcast][4]
		}
		w, _ := s.commit(func(a *storage.AccountTx) error {
			title := "Episode 4 " + strings.Repeat("x", size)
			return put(a, "episode", e, "metadata", fmt.Sprintf(`{"chapter_url":null,"description":null,"image_url":null,"link":null,"media":{"duration":1800000,"mime_type":"audio/mpeg","size":"1000000","url":"https://example.invalid/a.mp3"},"pub_date":%d,"title":%q,"transcript_type":null,"transcript_url":null}`,
				viewstest.FixedNow, title))
		})
		// The bytes of the episode's records, as the apply reads them.
		var size64 int64
		if err := s.store.Read(context.Background(), func(r *storage.ReadTx) error {
			rows, err := r.Account(s.account).ViewEntityRecords("episode", []string{e})
			for _, row := range rows {
				size64 += int64(len(row.Value))
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return w, size64
	}
	s1, b1 := run(small, 1<<10)
	s64, b64 := run(small, 64<<10)
	l1, lb1 := run(large, 1<<10)
	l64, lb64 := run(large, 64<<10)
	t.Logf("1 KiB: %+v; 64 KiB: %+v; at 50,400: %+v, %+v", s1, s64, l1, l64)
	if d := s64.BytesDecoded - s1.BytesDecoded; d != 63<<10 {
		t.Errorf("64 KiB decoded %d bytes more than 1 KiB, want %d", d, 63<<10)
	}
	if s1.BytesDecoded != b1 || s64.BytesDecoded != b64 || l1.BytesDecoded != lb1 || l64.BytesDecoded != lb64 {
		t.Errorf("bytes decoded: %d, %d at 5,000, %d, %d at 50,400; the episode's records: %d, %d, %d, %d",
			s1.BytesDecoded, s64.BytesDecoded, l1.BytesDecoded, l64.BytesDecoded, b1, b64, lb1, lb64)
	}
	for _, c := range []struct {
		name     string
		one, big views.WorkCounts
		n        int
	}{{"5,000", s1, s64, small.n}, {"50,400", l1, l64, large.n}} {
		if c.big.RecordsDecoded != c.one.RecordsDecoded || c.big.EntitiesBuilt != 1 || c.big.RowsRead != c.one.RowsRead ||
			c.big.NodesAllocated > c.one.NodesAllocated+2*4*log2(c.n) || c.big.NodesVisited > c.one.NodesVisited+2*4*log2(c.n) {
			t.Errorf("at %s, 64 KiB: %+v; 1 KiB: %+v", c.name, c.big, c.one)
		}
	}
}
