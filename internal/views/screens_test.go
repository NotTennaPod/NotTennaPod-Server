package views

import (
	"testing"
)

func pod(id, title string, rank int64, state string) *Podcast {
	p := &Podcast{ID: id, State: state, Rank: rank}
	if title != "" {
		p.Title = &title
	}
	return p
}

func episodes(p *Podcast, reads ...int) []*Episode {
	var out []*Episode
	for i, r := range reads {
		out = append(out, &Episode{ID: p.ID + string(rune('a'+i)), Podcast: p, Read: r, HasMedia: true, PubDate: int64(i)})
	}
	return out
}

func order(rows []Subscription) string {
	s := ""
	for _, r := range rows {
		s += r.Podcast.ID
	}
	return s
}

// getNavDrawerData's orders over getFeedList's title order: counter (most
// first, ties by Java's compareToIgnoreCase), alphabetical (null titles last),
// most played, and most recent episode (ties keep the title order).
func TestSubscriptionOrders(t *testing.T) {
	a, b, c, d := pod("A", "beta", 1, "subscribed"), pod("B", "Alpha", 2, "subscribed"), pod("C", "alpha", 3, "subscribed"), pod("D", "", 4, "subscribed")
	arch := pod("E", "archived", 5, "archived")
	lib := &Library{Podcasts: map[string]*Podcast{"A": a, "B": b, "C": c, "D": d, "E": arch}}
	lib.Episodes = append(lib.Episodes, episodes(a, -1, -1, 1)...)
	lib.Episodes = append(lib.Episodes, episodes(b, -1, 0, 1, 1)...)
	lib.Episodes = append(lib.Episodes, episodes(c, 0)...)
	lib.Episodes = append(lib.Episodes, episodes(arch, -1, -1, -1)...)
	lib.Episodes[len(lib.Episodes)-1].PubDate = 99
	opts := func(o, counter, filter string) SubscriptionOptions {
		x, err := ParseSubscriptionOptions("", o, counter, filter)
		if err != nil {
			t.Fatal(err)
		}
		return x
	}
	for _, tc := range []struct {
		o, counter, filter, want string
	}{
		{"counter", "new", "", "ABDC"},             // A 2 new, B 1; C and D 0, and "" sorts before "alpha"
		{"counter", "unplayed", "", "BACD"},        // A and B 2 ("Alpha" before "beta"), C 1, D 0
		{"alphabetical", "new", "", "BCAD"},        // Alpha = alpha (title order, then creation), beta, no title last
		{"most_played", "new", "", "BADC"},         // B 2 played, A 1, then "" before "alpha"
		{"most_recent_episode", "new", "", "BADC"}, // B newest (3), A (2); C and D tie at 0 in title order ("" first)
		{"counter", "new", "counter_greater_zero", "AB"},
	} {
		got := order(lib.Subscriptions(opts(tc.o, tc.counter, tc.filter)))
		if got != tc.want {
			t.Errorf("%s/%s/%s: %s, want %s", tc.o, tc.counter, tc.filter, got, tc.want)
		}
	}
	// Archived podcasts list apart, and count nothing (Android counts subscribed feeds only).
	rows := lib.Subscriptions(SubscriptionOptions{State: "archived", Order: "counter", Counter: "new"})
	if len(rows) != 1 || rows[0].Podcast != arch || rows[0].Counter != 0 {
		t.Fatalf("archived %+v", rows)
	}
	if _, err := ParseSubscriptionOptions("", "", "", "enabled_auto_download"); err == nil {
		t.Fatal("a device-only filter was accepted")
	}
}

func TestCompareIgnoreCase(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		sign int
	}{
		{"alpha", "Alpha", 0}, {"alpha", "beta", -1}, {"Zeta", "alpha", 1}, {"", "a", -1},
		{"Émile", "émile", 0}, {"éclair", "zeta", 1}, {"ß", "SS", 1}, {"abc", "ab", 1},
	} {
		got := compareIgnoreCase(tc.a, tc.b)
		if (got < 0 && tc.sign >= 0) || (got > 0 && tc.sign <= 0) || (got == 0 && tc.sign != 0) {
			t.Errorf("compareIgnoreCase(%q, %q) = %d, want sign %d", tc.a, tc.b, got, tc.sign)
		}
	}
}

// Get surprised: one episode per subscribed podcast, new or unplayed, with
// media, from the last two years, not played more than an hour ago; eight at
// most; the same seed gives the same sample.
func TestSurprise(t *testing.T) {
	now := int64(2_000_000_000_000)
	lib := &Library{Podcasts: map[string]*Podcast{}}
	for i := 0; i < 12; i++ {
		p := pod(string(rune('A'+i)), "p", int64(i), "subscribed")
		if i == 11 {
			p.State = "archived"
		}
		lib.Podcasts[p.ID] = p
		for j, e := range episodes(p, -1, 0, 1, 0, 0) {
			e.PubDate = now - int64(j)*day
			switch j {
			case 3:
				e.LastPlayed = now - 2*3600_000 // played two hours ago
			case 4:
				e.PubDate = now - 2*356*day - 1
			}
			lib.Episodes = append(lib.Episodes, e)
		}
	}
	a := lib.Home(now, 42, "date_new_old", false).Surprise
	b := lib.Home(now, 42, "date_new_old", false).Surprise
	if len(a) != 8 {
		t.Fatalf("%d episodes", len(a))
	}
	seen := map[string]bool{}
	for i, e := range a {
		if e != b[i] {
			t.Fatal("the same seed gave another sample")
		}
		if seen[e.Podcast.ID] || e.Podcast.State != "subscribed" || e.Read == 1 || e.LastPlayed != 0 || e.PubDate <= now-2*356*day {
			t.Fatalf("ineligible pick %+v", e)
		}
		seen[e.Podcast.ID] = true
	}
}

// Check your classics counts the last three years, "include marked as played"
// too (Android's SubscriptionsSection passes the range with the setting).
func TestClassicsWindowWithMarkedPlayed(t *testing.T) {
	now := int64(2_000_000_000_000)
	old, recent := pod("O", "old", 1, "subscribed"), pod("R", "recent", 2, "subscribed")
	lib := &Library{Podcasts: map[string]*Podcast{"O": old, "R": recent}}
	rec := func(fields map[string]any) Record { return Record{fields: fields} }
	part := func(p *Podcast, id string, played, last int64) *Episode {
		return &Episode{ID: id, Podcast: p, HasMedia: true, Records: map[string]Record{
			"played_time": rec(map[string]any{"played_ms": float64(played)}),
			"last_played": rec(map[string]any{"time": float64(last)}),
		}}
	}
	lib.Episodes = []*Episode{
		part(old, "o1", 10*3600_000, now-4*365*day),
		part(recent, "r1", 3600_000, now-day),
	}
	for _, marked := range []bool{false, true} {
		h := lib.Home(now, 1, "date_new_old", marked)
		if len(h.Classics) != 2 || h.Classics[0] != recent {
			t.Errorf("marked %v: classics %v first", marked, h.Classics[0].ID)
		}
	}
	// The statistics screen keeps all time for marked-as-played.
	st := lib.Statistics(now-3*365*day, 1<<63-1, true, now)
	if st.Podcasts[0].Podcast != old {
		t.Errorf("statistics with marked played: %s first", st.Podcasts[0].Podcast.ID)
	}
}
