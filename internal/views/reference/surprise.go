package reference

import "sort"

// SurpriseF5 is Home's Surprise section by its specification (PROTOCOL §12),
// naively: every eligible episode is scored. Eligible as
// getRandomEpisodesCursor: new or unplayed with media, of a subscribed
// podcast, published in the last 2 × 356 days, never played or played in
// the last hour. A podcast's pick has the highest score (u − v) mod 2⁶⁴,
// u = SplitMix64(FNV-1a(ID)) and v = SplitMix64(seed), ties to the lowest
// (rank, ID); the section is the picks by (u₂ − v₂) mod 2⁶⁴ descending, u₂
// = SplitMix64(FNV-1a("surprise order", 0xff, ID)) and v₂ =
// SplitMix64(seed XOR 0x5eed5eed5eed5eed), ties by ID, the first 8.
func (l *Library) SurpriseF5(now, seed int64) []*Episode {
	v := splitMix64(uint64(seed))
	best := map[string]*Episode{}
	for _, e := range l.Episodes {
		if (e.Read != -1 && e.Read != 0) || !e.HasMedia || e.Podcast.State != "subscribed" ||
			e.PubDate <= now-2*356*day || !(e.LastPlayed == 0 || e.LastPlayed > now-3600_000) {
			continue
		}
		cur := best[e.Podcast.ID]
		if cur == nil {
			best[e.Podcast.ID] = e
			continue
		}
		se, sc := fnvSplit("", e.ID)-v, fnvSplit("", cur.ID)-v
		if se > sc || se == sc && (e.Rank < cur.Rank || e.Rank == cur.Rank && e.ID < cur.ID) {
			best[e.Podcast.ID] = e
		}
	}
	var picks []*Episode
	for _, e := range best {
		picks = append(picks, e)
	}
	v2 := splitMix64(uint64(seed) ^ 0x5eed5eed5eed5eed)
	sort.Slice(picks, func(i, j int) bool {
		a, b := fnvSplit("surprise order", picks[i].ID)-v2, fnvSplit("surprise order", picks[j].ID)-v2
		if a != b {
			return a > b
		}
		return picks[i].ID < picks[j].ID
	})
	return picks[:min(len(picks), 8)]
}

// fnvSplit is SplitMix64 of FNV-1a over the prefix (then a 0xff separator)
// and the ID; without a prefix, over the ID alone.
func fnvSplit(prefix, id string) uint64 {
	h := uint64(14695981039346656037)
	if prefix != "" {
		for i := 0; i < len(prefix); i++ {
			h ^= uint64(prefix[i])
			h *= 1099511628211
		}
		h ^= 0xff
		h *= 1099511628211
	}
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	return splitMix64(h)
}

func splitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
