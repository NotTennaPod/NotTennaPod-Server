package views

import (
	"container/heap"
	"sort"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// ---- Home (HomeFragment's sections)

// Home is Android Home's sections that sync: continue listening (the queue's
// paused episodes), see what's new (the Inbox), get surprised and check your
// classics. Downloads stay on devices; the Echo review is seasonal.
type Home struct {
	Continue []*EpisodeItem
	New      []*EpisodeItem
	NewCount int
	Surprise []*EpisodeItem
	Classics []*PodcastItem
}

const (
	homeContinue = 8 // QueueSection.NUM_EPISODES
	homeNew      = 2 // InboxSection.NUM_EPISODES
	homeSurprise = 8 // EpisodesSurpriseSection.NUM_EPISODES
	homeClassics = 8 // SubscriptionsSection.NUM_FEEDS
	day          = int64(24 * 3600 * 1000)
	recentWindow = 30_000    // a play this recent counts in Continue whatever the position
	surpriseHour = 3_600_000 // a candidate played this recently stays one
	surpriseAge  = 2 * 356 * day
)

// Home computes the Home sections at time now. seed picks the Surprise
// sample; includeMarked is the statistics screen's setting, which Android's
// classics section follows.
func (l *Library) Home(now int64, seed int64, inboxOrder string, includeMarked bool) Home {
	var h Home
	h.Continue = l.Continue(now)
	inbox := l.Episodes(ListQuery{Filter: Filter{New: true}, Order: inboxOrder})
	h.NewCount = inbox.Total()
	h.New, _ = inbox.Read(inbox.At(0), homeNew)
	h.Surprise = l.Surprise(now, seed)
	// SubscriptionsSection: the last three years' statistics by played time.
	st := l.statistics(now-3*365*day, stats.Forever, includeMarked, true, now)
	for _, r := range st.Podcasts {
		if len(h.Classics) == homeClassics {
			break
		}
		if r.Podcast.State == "subscribed" {
			h.Classics = append(h.Classics, r.Podcast)
		}
	}
	return h
}

// Continue is getPausedQueue: the shown queue stably sorted by recent
// (last played, for an episode with media whose position is at least 1 s
// or that was played in the last 30 s; else 0) descending, first 8. Read
// from the partitions: the positive recents (by last played, then queue
// order), then the zero ones in queue order, then the negative ones. O(P
// log n + 8 + the plays of the last 30 s).
func (l *Library) Continue(now int64) []*EpisodeItem {
	v := l.V
	cutoff := now - recentWindow
	var shown []string
	for id := range v.podcasts.All() {
		shown = append(shown, id)
	}
	var out []*EpisodeItem
	// 1. Positive recents: every pos entry, and the recent entries played
	// in the window (the first of each podcast's range, by last played).
	m := newQueueMerge()
	inWindow := func(e *EpisodeItem) bool { return e.LastPlayed >= cutoff }
	for _, id := range shown {
		m.add(v.pos, id, nil, nil)
		m.add(v.recent, id, inWindow, nil)
	}
	out = m.take(out, homeContinue)
	if len(out) < homeContinue {
		// 2. Zero recents in queue order: the zero partition, and the
		// recent one once out of the window.
		m = newQueueMerge()
		for _, id := range shown {
			m.add(v.zero, id, nil, nil)
			m.add(v.timeZero, id, nil, inWindow)
		}
		out = m.take(out, homeContinue)
	}
	if len(out) < homeContinue {
		// 3. Negative recents, by last played.
		m = newQueueMerge()
		for _, id := range shown {
			m.add(v.neg, id, nil, nil)
		}
		out = m.take(out, homeContinue)
	}
	return out
}

// queueMerge merges podcasts' ranges of queue-keyed trees.
type queueMerge struct {
	runs []queueRun
	h    mergeHeap[queueKey] // head.r indexes runs
}

type queueRun struct {
	it    *ptree.Iter[queueKey, *EpisodeItem]
	id    string
	while func(*EpisodeItem) bool // the run ends at the first entry failing it
	skip  func(*EpisodeItem) bool // entries passing it are left out
}

func newQueueMerge() *queueMerge { return &queueMerge{h: mergeHeap[queueKey]{cmp: compareQueueRows}} }

func (m *queueMerge) add(t ptree.Tree[queueKey, *EpisodeItem], podcast string, while, skip func(*EpisodeItem) bool) {
	m.runs = append(m.runs, queueRun{it: t.Seek(queueKey{podcast: podcast, bound: -1}), id: podcast, while: while, skip: skip})
	m.advance(len(m.runs) - 1)
}

// advance puts run r's next entry in the heap.
func (m *queueMerge) advance(r int) {
	run := &m.runs[r]
	for {
		k, e, ok := run.it.Next()
		if !ok || k.podcast != run.id || run.while != nil && !run.while(e) {
			return
		}
		if run.skip != nil && run.skip(e) {
			work.candidates.Add(1)
			continue
		}
		heap.Push(&m.h, head[queueKey]{r: r, k: k, e: e})
		return
	}
}

func (m *queueMerge) take(out []*EpisodeItem, n int) []*EpisodeItem {
	for len(out) < n && m.h.Len() > 0 {
		top := heap.Pop(&m.h).(head[queueKey])
		out = append(out, top.e)
		m.advance(top.r)
	}
	return out
}

// ---- Surprise (PROTOCOL §12): one random eligible episode per subscribed
// podcast, reshuffled by the seed.
//
// Eligible, as getRandomEpisodesCursor: a new or unplayed episode with media
// of a subscribed podcast, published in the last 712 days (Android's
// 2 × 356), never played or played in the last hour. Each podcast's pick has
// the highest score (u(e) − v(seed)) mod 2⁶⁴, u the episode ID's hash
// (SurpriseBase) and v SplitMix64(seed), ties to the lowest (rank, ID); the
// section is the picks by (u₂(e) − v₂(seed)) mod 2⁶⁴ descending (ties by
// ID), the first 8. A pick is the largest u below v among the podcast's
// never-played candidates published in the window (else the largest u: the
// scores wrap around), found in O(log n) by the publication dates' maxima,
// or one of its plays of the last hour.

// SurpriseSeed is v(seed).
func SurpriseSeed(seed int64) uint64 { return ptree.SplitMix64(uint64(seed)) }

// SurpriseOrder is a pick's place in the section: (u₂(e) − v₂(seed)) mod 2⁶⁴.
func SurpriseOrder(seed int64, id string) uint64 {
	return ptree.Hash("surprise order", id) - ptree.SplitMix64(uint64(seed)^0x5eed5eed5eed5eed)
}

// surpriseBetter reports whether a's score beats b's.
func surpriseBetter(a, b *EpisodeItem, v uint64) bool {
	return betterScore(SurpriseBase(a.ID)-v, SurpriseBase(b.ID)-v, a, b)
}

// betterScore reports whether score sa of a beats score sb of b: the higher
// score, a tie to the lower (rank, ID).
func betterScore(sa, sb uint64, a, b *EpisodeItem) bool {
	switch {
	case sa != sb:
		return sa > sb
	case a.Rank != b.Rank:
		return a.Rank < b.Rank
	}
	return a.ID < b.ID
}

// Surprise is the Surprise section at time now for the seed: O(P log n +
// the plays of the last hour).
func (l *Library) Surprise(now, seed int64) []*EpisodeItem {
	v := l.V
	cutoff := now - surpriseAge
	sv := SurpriseSeed(seed)
	var picks []*EpisodeItem
	for id, p := range v.podcasts.All() {
		if p.State != "subscribed" {
			continue
		}
		if e := l.surprisePick(id, sv, cutoff, now); e != nil {
			picks = append(picks, e)
		}
	}
	sort.Slice(picks, func(i, j int) bool {
		a, b := SurpriseOrder(seed, picks[i].ID), SurpriseOrder(seed, picks[j].ID)
		if a != b {
			return a > b
		}
		return picks[i].ID < picks[j].ID
	})
	return picks[:min(len(picks), homeSurprise)]
}

func (l *Library) surprisePick(podcast string, sv uint64, cutoff, now int64) *EpisodeItem {
	v := l.V
	var best *EpisodeItem
	consider := func(e *EpisodeItem) {
		work.candidates.Add(1)
		if e.PubDate > cutoff && (best == nil || surpriseBetter(e, best, sv)) {
			best = e
		}
	}
	// Never played: the best score is the largest u below v, else the
	// largest u, among those published in the window.
	k, _, ok := v.surprise.MaxBelowWithMax(surpriseKey{podcast: podcast, u: sv}, cutoff)
	if !ok || k.podcast != podcast {
		k, _, ok = v.surprise.MaxBelowWithMax(surpriseKey{podcast: podcast + "\x00"}, cutoff)
	}
	if ok && k.podcast == podcast {
		it := v.surprise.Seek(surpriseKey{podcast: podcast, u: k.u})
		for {
			kk, e, ok := it.Next()
			if !ok || kk.podcast != podcast || kk.u != k.u {
				break
			}
			consider(e) // the candidates with this score
		}
	}
	// Played in the last hour.
	it := v.recentPlayed.Seek(playedKey{podcast: podcast, last: now - surpriseHour + 1})
	for {
		kk, e, ok := it.Next()
		if !ok || kk.podcast != podcast {
			break
		}
		consider(e)
	}
	return best
}
