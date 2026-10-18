package views_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
)

// Two tabs on one snapshot anchor the same list near rows 5,000 and
// 40,000 of 50,400; their previous, next, repeated and out-of-order pages,
// read one after another and concurrently, are the oracle's rows, each
// read within the seek budget whatever its depth; the registry holds one
// view, with no rows.
func TestTwoTabsAnchoredDeep(t *testing.T) {
	skipUnderRace(t)
	s := openShaped(t, "g", budgetLargeShapes)
	lib, ref := libOf(t, s)
	want, _, _ := ref.ListItems("/v1/read/episodes", "", everyEpisode)
	var ids []string
	for _, w := range want {
		ids = append(ids, rowID(w))
	}
	reg, _ := newRegistry(t)
	type tab struct {
		id     string
		offset int
	}
	var tabs []tab
	for _, at := range []int{5000, 40000} {
		list := everyEpisodeList(lib)
		id, err := reg.Put(s.account, 0, lib, "q", list, map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		c, found := list.Anchor(ids[at], 0, 40)
		if !found || c.Offset != at-40 {
			t.Fatalf("the anchor at %d: offset %d", at, c.Offset)
		}
		page, err := reg.Read(s.account, id, "q", c.Offset, 100, &c, lib.Identity())
		if err != nil {
			t.Fatal(err)
		}
		expect(t, fmt.Sprintf("the window at %d", at), page, want, at-40, 100)
		tabs = append(tabs, tab{id, c.Offset})
	}
	if tabs[0].id != tabs[1].id || reg.Views() != 1 {
		t.Fatalf("two identical first requests made %d views", reg.Views())
	}
	ranges := everyEpisodeList(lib).Ranges()
	budget := int64(ranges)*2*lg(len(want))*(2*lg(len(want))+2) + 100*(lg(ranges)+4)
	r := rand.New(rand.NewPCG(1, 2))
	read := func(offset int) {
		t.Helper()
		var page views.Page
		w := measure(func() {
			var err error
			if page, err = reg.Read(s.account, tabs[0].id, "q", offset, 100, nil, lib.Identity()); err != nil {
				t.Fatal(err)
			}
		})
		expect(t, fmt.Sprintf("a page at %d", offset), page, want, offset, 100)
		if w.NodesVisited > budget {
			t.Fatalf("a page at %d visited %d nodes, budget %d", offset, w.NodesVisited, budget)
		}
	}
	for range 40 {
		tb := tabs[r.IntN(2)]
		switch r.IntN(4) {
		case 0:
			read(max(0, tb.offset-100)) // previous
		case 1:
			read(tb.offset + 100) // next
		case 2:
			read(tb.offset) // again
		default:
			read(r.IntN(len(want))) // out of order
		}
	}
	var wg sync.WaitGroup
	for g := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 3))
			for range 10 {
				offset := tabs[g%2].offset + (r.IntN(5)-2)*100
				offset = max(0, offset)
				page, err := reg.Read(s.account, tabs[0].id, "q", offset, 100, nil, lib.Identity())
				if err != nil {
					t.Error(err)
					return
				}
				for i, it := range page.Items {
					if !bytes.Equal(it, want[offset+i]) {
						t.Errorf("a concurrent page at %d differs", offset)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if f, _ := views.ViewFootprint(reg, tabs[0].id); f.Positions > 4*ranges || f.Bytes > int64(40*ranges+1024+2) {
		t.Fatalf("the view keeps %d positions in %d bytes", f.Positions, f.Bytes)
	}
}

func rowID(raw []byte) string {
	i := bytes.Index(raw, []byte(`"episode_id":"`))
	if i < 0 {
		return ""
	}
	rest := raw[i+len(`"episode_id":"`):]
	return string(rest[:bytes.IndexByte(rest, '"')])
}
