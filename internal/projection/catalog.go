package projection

import "sort"

// Catalog (PROTOCOL §9.4): canonical metadata records, folded per record from
// observations, an episode generation's admission, and a podcast's refresh
// status.

// Catalog effect kinds.
const (
	KindObservation   = "observation"    // one record's observation (podcast scope: the feed; item scope: an episode)
	KindAdmission     = "admission"      // an episode generation's admission and its filing (episode scope)
	KindRefreshStatus = "refresh_status" // the server's refresh status (podcast scope)
	KindMove          = "move"           // a podcast.moved decision: a refresh-URL write when it has one (podcast scope)
)

// IsCatalogKind reports the kinds the podcast scope's catalog records fold, not
// its subscription state.
func IsCatalogKind(kind string) bool { return kind == KindObservation || kind == KindRefreshStatus }

// CatalogRecord is a canonical record: its value, the revision (node) that
// wrote it and the fetch time it carries.
type CatalogRecord struct {
	Value     map[string]any
	Revision  string
	FetchTime int64
}

// FoldCatalog folds one record's observations in rank order (PROTOCOL §9.4): an
// observation replaces the current record if there is none, if it names the
// current revision as its base, or if it is concurrent with a later fetch time.
// A node's several observations in one scope (a compound, PROTOCOL §9.7) are
// folded in the compound's order (a compound used to be skipped, losing the whole record).
func FoldCatalog(cs []Contribution) (CatalogRecord, bool) {
	cs = Expand(cs)
	sortByRank(cs)
	var cur CatalogRecord
	has := false
	for _, c := range cs {
		e := c.Effect
		if e.Kind != KindObservation {
			continue
		}
		if e.Observed == nil {
			continue // a refresh status alone
		}
		if !has || e.RevisionBase == cur.Revision || e.FetchTime > cur.FetchTime {
			cur, has = CatalogRecord{Value: e.Observed, Revision: c.Node, FetchTime: e.FetchTime}, true
		}
	}
	return cur, has
}

// LastRefreshStatus is the podcast's latest refresh status (rank order).
func LastRefreshStatus(cs []Contribution) (map[string]any, string) {
	sortByRank(cs)
	var v map[string]any
	by := ""
	for _, c := range cs {
		if c.Effect.Refresh != nil {
			v, by = c.Effect.Refresh, c.Node
		}
	}
	return v, by
}

// filing is an admission's filing effect (PROTOCOL §9.4): one node contributes one
// effect per scope, so the admission carries its filing. ok is false for the
// action nothing.
func filing(e Effect) (Effect, bool) {
	switch e.Admission["action"] {
	case "inbox":
		return Effect{Kind: KindAdmissionNew}, true
	case "queue":
		return Effect{Kind: KindQueueAdd, Automatic: true, QueueKey: e.QueueKey, PositionID: e.PositionID}, true
	}
	return Effect{}, false
}

// Compose makes one effect of a node's several effects in one scope (PROTOCOL §9.7),
// in a fixed order whatever order they arrived in: membership effects before
// position-only ones, then by the episode each was for (its member, or the
// scope's own episode for "").
func Compose(effects ...Effect) Effect {
	var parts []Effect
	for _, e := range effects {
		if e.Kind == KindCompound {
			parts = append(parts, e.Compound...)
		} else {
			parts = append(parts, e)
		}
	}
	positionOnly := func(e Effect) bool { return e.Kind == KindQueueMove }
	sort.SliceStable(parts, func(i, j int) bool {
		if a, b := positionOnly(parts[i]), positionOnly(parts[j]); a != b {
			return b
		}
		return parts[i].Member < parts[j].Member
	})
	return Effect{Kind: KindCompound, Compound: parts}
}

// ComposeAt composes effects in the scope of episode: a part without a
// member is the scope's own episode's, and is ordered as that member (an empty member once sorted
// first).
func ComposeAt(episode string, effects ...Effect) Effect {
	for i := range effects {
		if effects[i].Member == "" && effects[i].Kind != KindCompound {
			effects[i].Member = episode
		}
	}
	return Compose(effects...)
}

// KindCompound is a node's several effects in one scope.
const KindCompound = "compound"

// Expand replaces each compound contribution by its parts, in order, at the
// compound's rank.
func Expand(cs []Contribution) []Contribution {
	n := 0
	for _, c := range cs {
		if c.Effect.Kind == KindCompound {
			n++
		}
	}
	if n == 0 {
		return cs
	}
	out := make([]Contribution, 0, len(cs)+n)
	for _, c := range cs {
		if c.Effect.Kind != KindCompound {
			out = append(out, c)
			continue
		}
		for _, e := range c.Effect.Compound {
			out = append(out, Contribution{Node: c.Node, Rank: c.Rank, Effect: e})
		}
	}
	return out
}
