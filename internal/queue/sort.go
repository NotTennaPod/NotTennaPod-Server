// Package queue implements the canonical keep-sorted order of PROTOCOL §7.6:
// Android 3.12.1's queue comparators (FeedItemPermutors) on canonical values,
// stable over the current order.
package queue

import (
	"errors"
	"sort"
	"strings"
	"unicode"
)

// Item is a queue member with its canonical sort values.
type Item struct {
	EpisodeID, PodcastID string
	Title, PodcastTitle  string
	PublicationTime      int64 // ms; 0 when unknown (Android's Date(0))
	Duration             int64 // ms; 0 when unknown
}

// Sort orders offered by Android's queue sort dialog.
var Orders = map[string]bool{
	"episode_title_a_z": true, "episode_title_z_a": true,
	"feed_title_a_z": true, "feed_title_z_a": true,
	"duration_short_long": true, "duration_long_short": true,
	"date_old_new": true, "date_new_old": true,
	"random":                true,
	"smart_shuffle_old_new": true, "smart_shuffle_new_old": true,
}

// ErrNotDeterministic is returned for random, which keep-sorted never uses.
var ErrNotDeterministic = errors.New("queue: random is not a keep-sorted order")

// Sort returns items, given in current queue order, in the canonical order.
// Ties keep their current relative order (Java's stable Collections.sort).
func Sort(order string, items []Item) ([]Item, error) {
	out := append([]Item(nil), items...)
	var less func(a, b Item) int
	switch order {
	case "episode_title_a_z":
		less = func(a, b Item) int { return strings.Compare(fold(a.Title), fold(b.Title)) }
	case "episode_title_z_a":
		less = func(a, b Item) int { return strings.Compare(fold(b.Title), fold(a.Title)) }
	case "feed_title_a_z":
		less = func(a, b Item) int { return strings.Compare(fold(a.PodcastTitle), fold(b.PodcastTitle)) }
	case "feed_title_z_a":
		less = func(a, b Item) int { return strings.Compare(fold(b.PodcastTitle), fold(a.PodcastTitle)) }
	case "duration_short_long":
		less = func(a, b Item) int { return cmp(a.Duration, b.Duration) }
	case "duration_long_short":
		less = func(a, b Item) int { return cmp(b.Duration, a.Duration) }
	case "date_old_new":
		less = func(a, b Item) int { return cmp(a.PublicationTime, b.PublicationTime) }
	case "date_new_old":
		less = func(a, b Item) int { return cmp(b.PublicationTime, a.PublicationTime) }
	case "smart_shuffle_old_new":
		return smartShuffle(out, true), nil
	case "smart_shuffle_new_old":
		return smartShuffle(out, false), nil
	case "random":
		return nil, ErrNotDeterministic
	default:
		return nil, errors.New("queue: unknown sort order " + order)
	}
	sort.SliceStable(out, func(i, j int) bool { return less(out[i], out[j]) < 0 })
	return out, nil
}

// fold is the canonical title collation key: Unicode simple lowercase mapping
// per code point (strings then compare by code point, as UTF-8 bytes do).
func fold(s string) string {
	return strings.Map(unicode.ToLower, s)
}

func cmp(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// smartShuffle reproduces FeedItemPermutors.smartShuffle: episodes grouped by
// podcast, each group stably sorted by publication time, groups placed largest
// first into evenly spread empty slots. Android breaks ties between equally
// large groups by hash order; the canonical order uses the podcast ID.
func smartShuffle(items []Item, ascending bool) []Item {
	groups := map[string][]Item{}
	var podcasts []string
	for _, it := range items {
		if _, ok := groups[it.PodcastID]; !ok {
			podcasts = append(podcasts, it.PodcastID)
		}
		groups[it.PodcastID] = append(groups[it.PodcastID], it)
	}
	for _, p := range podcasts {
		g := groups[p]
		sort.SliceStable(g, func(i, j int) bool {
			if ascending {
				return g[i].PublicationTime < g[j].PublicationTime
			}
			return g[j].PublicationTime < g[i].PublicationTime
		})
	}
	sort.Strings(podcasts)
	sort.SliceStable(podcasts, func(i, j int) bool { return len(groups[podcasts[i]]) > len(groups[podcasts[j]]) })
	out := make([]Item, len(items))
	empty := make([]int, len(items))
	for i := range empty {
		empty[i] = i
	}
	for _, p := range podcasts {
		g := groups[p]
		spread := float64(len(empty)) / float64(len(g)+1)
		skipped, placed := 0, 0
		remaining := empty[:0:0]
		for k, slot := range empty {
			if placed == len(g) {
				remaining = append(remaining, empty[k:]...)
				break
			}
			skipped++
			if float64(skipped) >= spread*float64(placed+1) {
				out[slot] = g[placed]
				placed++
				continue
			}
			remaining = append(remaining, slot)
		}
		empty = remaining
	}
	return out
}
