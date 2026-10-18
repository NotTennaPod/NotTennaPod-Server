package views

import (
	"fmt"
	"reflect"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// EqualVersions compares two versions' content: the account's records,
// every shown podcast and stored episode (every field, records byte for
// byte), every podcast's aggregates, and every structure's entries, values,
// subtree aggregates and shape (a function of its entries, as every tree is
// canonical). It leaves out what identifies a version rather than its
// content (head and clock). It returns nil, or the first difference.
func EqualVersions(a, b *Version) error {
	if a.Account != b.Account {
		return fmt.Errorf("accounts %s and %s", a.Account, b.Account)
	}
	if !reflect.DeepEqual(a.accountRows, b.accountRows) || !reflect.DeepEqual(a.AccountRecords, b.AccountRecords) {
		return fmt.Errorf("the account records differ")
	}
	eqPodcast := func(x, y *PodcastItem) bool { return x == y || reflect.DeepEqual(x, y) }
	eqStored := func(x, y *EpisodeItem) bool { return x == y || reflect.DeepEqual(x, y) }
	// Every other structure's episode is its version's stored one (the
	// stored ones being equal): a pointer comparison each.
	eqEpisode := func(x, y *EpisodeItem) bool {
		sx, okx := a.stored.Get(x.ID)
		sy, oky := b.stored.Get(y.ID)
		return okx && oky && sx == x && sy == y && x.ID == y.ID
	}
	str := func(k string) string { return k }
	checks := []func() string{
		func() string { return ptree.Diff(a.podcasts, b.podcasts, eqPodcast, str) },
		func() string { return ptree.Diff(a.stored, b.stored, eqStored, str) },
		func() string {
			return ptree.Diff(a.agg, b.agg, func(x, y *PodAgg) bool { return *x == *y }, str)
		},
		func() string { return ptree.Diff(a.queue, b.queue, eqEpisode, showQueueKey) },
		func() string { return ptree.Diff(a.pos, b.pos, eqEpisode, showQueueKey) },
		func() string { return ptree.Diff(a.recent, b.recent, eqEpisode, showQueueKey) },
		func() string { return ptree.Diff(a.zero, b.zero, eqEpisode, showQueueKey) },
		func() string { return ptree.Diff(a.timeZero, b.timeZero, eqEpisode, showQueueKey) },
		func() string { return ptree.Diff(a.neg, b.neg, eqEpisode, showQueueKey) },
		func() string {
			return ptree.Diff(a.surprise, b.surprise, eqEpisode, func(k surpriseKey) string { return fmt.Sprintf("%+v", k) })
		},
		func() string {
			return ptree.Diff(a.recentPlayed, b.recentPlayed, eqEpisode, func(k playedKey) string { return fmt.Sprintf("%+v", k) })
		},
		func() string {
			return ptree.Diff(a.stats, b.stats, func(x, y statValue) bool { return x == y }, func(k statKey) string { return fmt.Sprintf("%+v", k) })
		},
		func() string {
			return ptree.Diff(a.podMonths, b.podMonths, func(x, y int64) bool { return x == y }, func(k podMonthKey) string { return fmt.Sprintf("%+v", k) })
		},
		func() string {
			return ptree.Diff(a.months, b.months, func(x, y int64) bool { return x == y }, func(k int) string { return fmt.Sprint(k) })
		},
	}
	names := []string{"podcasts", "stored episodes", "podcast aggregates", "queue", "continue (positive)", "continue (recent)",
		"continue (zero)", "continue (recent, queue order)", "continue (negative)", "surprise", "recently played", "statistics",
		"podcast months", "months"}
	for i, c := range checks {
		if d := c(); d != "" {
			return fmt.Errorf("%s: %s", names[i], d)
		}
	}
	for i := range a.orders {
		if d := ptree.Diff(a.orders[i], b.orders[i], eqEpisode, func(k orderKey) string { return fmt.Sprintf("%+v", k) }); d != "" {
			return fmt.Errorf("order %s: %s", OrderNames[i], d)
		}
	}
	return nil
}

func showQueueKey(k queueKey) string {
	return fmt.Sprintf("{%s %d %d/%s %s}", k.podcast, k.last, k.key.N, k.key.M, k.id)
}
