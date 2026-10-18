package views

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// ErrNeedFullBuild reports a snapshot a version cannot be applied to: another
// account, log lineage (log ID and epoch) or generation, or an older tick.
var ErrNeedFullBuild = errors.New("views: the snapshot needs a full build")

// ErrInconsistent reports an apply that found the version not as it built
// it (an entity's place missing): nothing is published from it.
var ErrInconsistent = errors.New("views: the version is inconsistent")

// Path is how a version was made.
type Path int

const (
	PathFirst      Path = iota // a full build: no earlier version
	PathGeneration             // a full build: another lineage or generation
	PathApply                  // the earlier version with the changed entities applied
	PathExact                  // the earlier version, at the same tick
)

func (p Path) String() string {
	return [...]string{"first build", "generation build", "apply", "exact"}[p]
}

// Advance makes the account's version at the snapshot from base: applied
// when base is of the same lineage and generation at a tick at or below the
// snapshot's, else built in full.
func Advance(base *Version, acct *storage.AccountReader, account string) (*Version, Path, error) {
	if base == nil {
		v, err := BuildVersion(acct, account)
		return v, PathFirst, err
	}
	v, err := base.Apply(acct)
	switch {
	case errors.Is(err, ErrNeedFullBuild):
		v, err = BuildVersion(acct, account)
		return v, PathGeneration, err
	case err != nil:
		return nil, PathApply, err
	case v.Clock.Tick == base.Clock.Tick:
		return v, PathExact, nil
	}
	return v, PathApply, nil
}

// Apply returns the version at the snapshot made from v: every entity the
// change capture stamped above v's tick is read again from the snapshot and
// replaces its old self in every structure. It requires the snapshot to be
// of v's account, lineage and generation, at v's tick or later
// (ErrNeedFullBuild otherwise). v itself never changes.
func (v *Version) Apply(acct *storage.AccountReader) (*Version, error) {
	head, err := acct.Head()
	if err != nil {
		return nil, err
	}
	clock, err := acct.ViewClock()
	if err != nil {
		return nil, err
	}
	if head.LogID != v.Head.LogID || head.Epoch != v.Head.Epoch || clock.Generation != v.Clock.Generation || clock.Tick < v.Clock.Tick {
		return nil, ErrNeedFullBuild
	}
	nv := *v
	nv.Head, nv.Clock = head, clock
	if clock.Tick == v.Clock.Tick {
		return &nv, nil // no view input changed since
	}
	work.applies.Add(1)
	changed := map[string][]string{}
	cur := storage.ViewCursor{Tick: v.Clock.Tick}
	for {
		page, next, more, err := acct.ViewChangesSince(v.Clock.Tick, cur)
		if err != nil {
			return nil, err
		}
		for _, c := range page {
			changed[c.Entity] = append(changed[c.Entity], c.ID)
		}
		work.changes.Add(int64(len(page)))
		if !more {
			break
		}
		cur = next
	}
	a := &applier{v: &nv, acct: acct}
	if ids := changed["account"]; len(ids) > 0 {
		if err := a.account(ids); err != nil {
			return nil, err
		}
	}
	if ids := changed["podcast"]; len(ids) > 0 {
		if err := a.podcasts(ids); err != nil {
			return nil, err
		}
	}
	if ids := changed["episode"]; len(ids) > 0 {
		if err := a.episodes(ids); err != nil {
			return nil, err
		}
	}
	return &nv, nil
}

type applier struct {
	v    *Version
	acct *storage.AccountReader
	err  error
}

func (a *applier) fail(format string, args ...any) {
	if a.err == nil {
		a.err = fmt.Errorf("%w: "+format, append([]any{ErrInconsistent}, args...)...)
	}
}

// account reads the account entity's records again (every entity ID it had
// or was stamped with) and merges them.
func (a *applier) account(ids []string) error {
	all := slices.Collect(maps.Keys(a.v.accountRows))
	all = append(all, ids...)
	rows, err := a.acct.ViewEntityRecords("account", all)
	if err != nil {
		return err
	}
	work.rowsRead.Add(int64(len(rows)))
	work.entitiesBuilt.Add(1)
	a.v.accountRows = entityRecords(rows)
	a.v.AccountRecords = mergeAccount(a.v.accountRows)
	return nil
}

// podcasts rebuilds the stamped podcasts; one whose shown state changed adds
// its statistics months to the library's, or takes them away.
func (a *applier) podcasts(ids []string) error {
	rows, err := a.acct.ViewEntityRecords("podcast", ids)
	if err != nil {
		return err
	}
	ranks, err := a.acct.PodcastRanksFor(ids)
	if err != nil {
		return err
	}
	work.rowsRead.Add(int64(len(rows) + len(ranks)))
	recs := entityRecords(rows)
	v := a.v
	for _, id := range ids {
		p, err := buildPodcast(id, recs[id], ranks[id])
		if err != nil {
			return err
		}
		_, wasShown := v.podcasts.Get(id)
		if p != nil {
			v.podcasts = v.podcasts.Set(id, p)
		} else {
			v.podcasts = v.podcasts.Delete(id)
		}
		if isShown := p != nil; isShown != wasShown {
			sign := int64(1)
			if !isShown {
				sign = -1
			}
			it := v.podMonths.Seek(podMonthKey{podcast: id, month: math.MinInt})
			for {
				k, ms, ok := it.Next()
				if !ok || k.podcast != id {
					break
				}
				a.addMonth(k.month, sign*ms)
			}
		}
	}
	return a.err
}

// episodes rebuilds the stamped episodes: each one's old self leaves every
// structure, and its new self, if stored, enters them.
func (a *applier) episodes(ids []string) error {
	rows, err := a.acct.ViewEntityRecords("episode", ids)
	if err != nil {
		return err
	}
	registered, err := a.acct.RegisteredEpisodes(ids)
	if err != nil {
		return err
	}
	catalog, err := a.acct.CatalogRowsFor(ids)
	if err != nil {
		return err
	}
	work.rowsRead.Add(int64(len(rows) + len(registered) + len(catalog)))
	recs := entityRecords(rows)
	reg := make(map[string]bool, len(registered))
	for _, id := range registered {
		reg[id] = true
	}
	cat := make(map[string]*storage.CatalogRow, len(catalog))
	for i := range catalog {
		cat[catalog[i].EpisodeID] = &catalog[i]
	}
	slices.Sort(ids)
	for _, id := range slices.Compact(ids) {
		e, err := buildEpisode(id, recs[id], reg[id], cat[id])
		if err != nil {
			return err
		}
		if old, ok := a.v.stored.Get(id); ok {
			a.remove(old)
		}
		if e != nil {
			a.add(e)
		}
		if a.err != nil {
			return a.err
		}
	}
	return nil
}

func del[K, V any](a *applier, t ptree.Tree[K, V], k K, what string) ptree.Tree[K, V] {
	n := t.Delete(k)
	if n.Len() == t.Len() {
		a.fail("%s has no entry for %v", what, k)
	}
	return n
}

func (a *applier) shown(podcast string) bool {
	_, ok := a.v.podcasts.Get(podcast)
	return ok
}

func (a *applier) addMonth(m int, ms int64) {
	v := a.v
	n, _ := v.months.Get(m)
	if n += ms; n == 0 {
		v.months = del(a, v.months, m, "months")
	} else {
		v.months = v.months.Set(m, n)
	}
}

func (a *applier) addPodMonth(k podMonthKey, ms int64) {
	v := a.v
	n, _ := v.podMonths.Get(k)
	if n += ms; n == 0 {
		v.podMonths = del(a, v.podMonths, k, "podcast months")
	} else {
		v.podMonths = v.podMonths.Set(k, n)
	}
}

// contribute adds (sign 1) or takes away (−1) an episode's aggregates.
func (a *applier) contribute(e *EpisodeItem, sign int64) {
	v := a.v
	old, _ := v.agg.Get(e.PodcastID)
	agg := &PodAgg{}
	if old != nil {
		*agg = *old
	}
	agg.contribute(e, sign)
	switch {
	case agg.empty():
		v.agg = del(a, v.agg, e.PodcastID, "podcast aggregates")
	default:
		v.agg = v.agg.Set(e.PodcastID, agg)
	}
	shown := a.shown(e.PodcastID)
	for m, ms := range monthly(e) {
		a.addPodMonth(podMonthKey{e.PodcastID, m}, sign*ms)
		if shown {
			a.addMonth(m, sign*ms)
		}
	}
}

func (a *applier) remove(e *EpisodeItem) {
	v := a.v
	v.stored = del(a, v.stored, e.ID, "stored")
	for i, d := range orderDefs {
		v.orders[i] = del(a, v.orders[i], d.key(e), OrderNames[i])
	}
	if e.Queued {
		v.queue = del(a, v.queue, queueOrder(e), "queue")
		p, r, z, n := partitions(e)
		switch {
		case p:
			v.pos = del(a, v.pos, recentOrder(e), "continue (positive)")
		case r:
			v.recent = del(a, v.recent, recentOrder(e), "continue (recent)")
			v.timeZero = del(a, v.timeZero, queueOrder(e), "continue (recent, queue order)")
		case z:
			v.zero = del(a, v.zero, queueOrder(e), "continue (zero)")
		case n:
			v.neg = del(a, v.neg, recentOrder(e), "continue (negative)")
		}
	}
	switch never, played := surpriseCandidate(e); {
	case never:
		v.surprise = del(a, v.surprise, surpriseKey{e.PodcastID, SurpriseBase(e.ID), e.ID}, "surprise")
	case played:
		v.recentPlayed = del(a, v.recentPlayed, playedKey{e.PodcastID, e.LastPlayed, e.ID}, "recently played")
	}
	for i, x := range e.Parts {
		v.stats = del(a, v.stats, statKey{e.PodcastID, x.LastPlayed, e.ID, i}, "statistics")
	}
	a.contribute(e, -1)
}

func (a *applier) add(e *EpisodeItem) {
	v := a.v
	v.stored = v.stored.Set(e.ID, e)
	for i, d := range orderDefs {
		v.orders[i] = v.orders[i].Set(d.key(e), e)
	}
	if e.Queued {
		v.queue = v.queue.Set(queueOrder(e), e)
		p, r, z, n := partitions(e)
		switch {
		case p:
			v.pos = v.pos.Set(recentOrder(e), e)
		case r:
			v.recent = v.recent.Set(recentOrder(e), e)
			v.timeZero = v.timeZero.Set(queueOrder(e), e)
		case z:
			v.zero = v.zero.Set(queueOrder(e), e)
		case n:
			v.neg = v.neg.Set(recentOrder(e), e)
		}
	}
	switch never, played := surpriseCandidate(e); {
	case never:
		v.surprise = v.surprise.Set(surpriseKey{e.PodcastID, SurpriseBase(e.ID), e.ID}, e)
	case played:
		v.recentPlayed = v.recentPlayed.Set(playedKey{e.PodcastID, e.LastPlayed, e.ID}, e)
	}
	for i, x := range e.Parts {
		v.stats = v.stats.Set(statKey{e.PodcastID, x.LastPlayed, e.ID, i}, statValue{x.PlayedMS, markedValue(x)})
	}
	a.contribute(e, 1)
}
