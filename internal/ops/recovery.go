package ops

import (
	"errors"
	"sort"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The end of a recovery upload request (PROTOCOL §6.11), made after the rebuild in the request's
// commit: registrations still without an effective resolution are resolved as live ones would be
// (provisional); equivalent identity decisions are unified, never split. It follows the
// independent reference (contract/reference/rules.py, _accept_recovery_end, _unify). The queue
// needs no step of its own: the commit ends with its publication (PROTOCOL §7.6).

func registerRecovery(r *protocol.Registry) { r.RegisterRecoveryEnd(recoveryEnd) }

func recoveryEnd(c *protocol.ApplyContext, steps protocol.RecoveryEndSteps) error {
	if steps.Resolutions {
		if err := provisionalResolutions(c); err != nil {
			return err
		}
	}
	if steps.Unify {
		return unify(c)
	}
	return nil
}

// ---- provisional resolutions

// provisionalResolutions resolves every admitted registration without a
// podcast.resolution, in receipt order, as its live acceptance would have
// (its podcast.alias and podcast.resolution are made now, provisional with
// respect to originals recovered later), and applies it.
func provisionalResolutions(c *protocol.ApplyContext) error {
	ops, err := c.Account.UnresolvedRegistrations()
	if err != nil {
		return err
	}
	for _, op := range ops {
		row, err := c.Account.Fact(op)
		if err != nil {
			return err
		}
		v, err := contract.DecodeJSON(row.Fact)
		if err != nil {
			return err
		}
		a, err := protocol.ParseFact(v)
		if err != nil || a.Fact == nil {
			continue
		}
		t, err := c.Account.NodeTime(op)
		if err != nil {
			return err
		}
		fc := *c
		fc.OpID, fc.NormalizedTime = op, t
		if _, err := (resolveOp{preview: a.Fact.Type == "podcast.register_preview"}).Apply(&fc, a.Fact); err != nil {
			return err
		}
	}
	return nil
}

// ---- unification (PROTOCOL §6.11)

// unifiedTypes are the identity decisions unified per (type, subject).
var unifiedTypes = []string{"podcast.alias", "podcast.resolution", "podcast.merge", "episode.merge", "episode.guid_change"}

type rankedDecision struct {
	row    storage.DecisionRow
	obj    map[string]any
	result map[string]any
}

func rankedDecisions(acct *storage.AccountTx, types ...string) ([]rankedDecision, error) {
	rows, err := acct.DecisionsOf(types...)
	if err != nil {
		return nil, err
	}
	out := make([]rankedDecision, 0, len(rows))
	for _, r := range rows {
		v, err := contract.DecodeJSON(r.Decision)
		if err != nil {
			return nil, err
		}
		obj, _ := object(v)
		res, _ := object(obj["result"])
		out = append(out, rankedDecision{r, obj, res})
	}
	return out, nil
}

func rankLess(a, b storage.DecisionRow) bool {
	if a.Epoch != b.Epoch {
		return a.Epoch < b.Epoch
	}
	if a.Seq != b.Seq {
		return a.Seq < b.Seq
	}
	return a.DecisionID < b.DecisionID
}

// unify makes merges until no two effective identity decisions with the
// same evidence key disagree (the reference's _unify).
func unify(c *protocol.ApplyContext) error {
	for range 10000 {
		changed, err := unifyStep(c)
		if err != nil || !changed {
			return err
		}
		if err := c.Flush(); err != nil {
			return err
		}
	}
	return errors.New("ops: unification does not converge")
}

func unifyStep(c *protocol.ApplyContext) (bool, error) {
	all, err := rankedDecisions(c.Account, unifiedTypes...)
	if err != nil {
		return false, err
	}
	groups := map[[2]string][]rankedDecision{}
	var keys [][2]string
	for _, d := range all {
		k := [2]string{d.row.DecisionType, d.row.Subject}
		if _, ok := groups[k]; !ok {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], d)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	for _, k := range keys {
		es := groups[k] // in rank order
		eff := es[0]
		for _, o := range es[1:] {
			var changed bool
			var err error
			switch k[0] {
			case "podcast.alias":
				changed, err = unifyPodcasts(c, resultString(eff, "podcast_id"), resultString(o, "podcast_id"))
			case "podcast.resolution":
				if m := resultString(o, "mode"); m == "created" || m == "opened" {
					changed, err = unifyPodcasts(c, resultString(eff, "podcast_id"), resultString(o, "podcast_id"))
				}
			case "podcast.merge":
				changed, err = unifyPodcasts(c, resultString(eff, "into"), resultString(o, "into"))
			case "episode.merge":
				changed, err = unifyEpisodes(c, resultString(eff, "into"), resultString(o, "into"), eff, o)
			default:
				changed, err = unifyEpisodes(c, resultString(eff, "episode_id"), resultString(o, "episode_id"), eff, o)
			}
			if err != nil || changed {
				return changed, err
			}
		}
	}
	// PROTOCOL §6.11: releases of one podcast and GUID decided on preserved evidence.
	maps, err := rankedDecisions(c.Account, "episode.occurrence_mapping")
	if err != nil {
		return false, err
	}
	byGUID := map[[2]string][]rankedDecision{}
	var gkeys [][2]string
	for _, m := range maps {
		if _, addition := m.result["enclosure"]; addition {
			continue // an enclosure addition is no release
		}
		inputs, _ := object(m.obj["inputs"])
		p, _ := str(inputs, "podcast_id")
		g, _ := str(inputs, "guid")
		k := [2]string{p, g}
		if _, ok := byGUID[k]; !ok {
			gkeys = append(gkeys, k)
		}
		byGUID[k] = append(byGUID[k], m)
	}
	sort.Slice(gkeys, func(i, j int) bool {
		if gkeys[i][0] != gkeys[j][0] {
			return gkeys[i][0] < gkeys[j][0]
		}
		return gkeys[i][1] < gkeys[j][1]
	})
	for _, k := range gkeys {
		for _, pair := range equivalentReleases(byGUID[k]) {
			changed, err := unifyEpisodes(c, resultString(pair[0], "episode_id"), resultString(pair[1], "episode_id"), pair[0], pair[1])
			if err != nil || changed {
				return changed, err
			}
		}
	}
	return false, nil
}

func resultString(d rankedDecision, k string) string {
	s, _ := str(d.result, k)
	return s
}

// equivalentReleases pairs (earlier rank, later) the releases of one
// podcast and GUID that are the same (PROTOCOL §6.11): they share an occurrence (the
// publication time and an enclosure), or each is the other's only release
// of another commit within the 7-day window. Releases decided in one commit
// are never the same (reference CHOICE).
func equivalentReleases(ms []rankedDecision) [][2]rankedDecision {
	commit := func(m rankedDecision) [2]int64 { return [2]int64{m.row.Epoch, m.row.Seq} }
	dated := func(m rankedDecision) (int64, bool) { return integer(m.result["publication_time"]) }
	near := func(x rankedDecision) []string {
		var out []string
		dx, okx := dated(x)
		for _, m := range ms {
			dm, okm := dated(m)
			if commit(m) != commit(x) && okm && okx && abs64(dm-dx) <= releaseWindow {
				out = append(out, m.row.DecisionID)
			}
		}
		return out
	}
	encs := func(m rankedDecision) map[string]bool {
		out := map[string]bool{}
		list, _ := m.result["enclosures"].([]any)
		for _, x := range list {
			if u, ok := x.(string); ok {
				out[u] = true
			}
		}
		return out
	}
	var out [][2]rankedDecision
	for i, a := range ms {
		for _, b := range ms[i+1:] {
			if commit(a) == commit(b) {
				continue
			}
			da, oka := dated(a)
			db, okb := dated(b)
			shared := false
			if oka == okb && (!oka || da == db) {
				eb := encs(b)
				for u := range encs(a) {
					if eb[u] {
						shared = true
					}
				}
			}
			window := false
			if oka && okb && abs64(da-db) <= releaseWindow {
				na, nb := near(a), near(b)
				window = len(na) == 1 && na[0] == b.row.DecisionID && len(nb) == 1 && nb[0] == a.row.DecisionID
			}
			if shared || window {
				if rankLess(a.row, b.row) {
					out = append(out, [2]rankedDecision{a, b})
				} else {
					out = append(out, [2]rankedDecision{b, a})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0].row.DecisionID != out[j][0].row.DecisionID {
			return rankLess(out[i][0].row, out[j][0].row)
		}
		return rankLess(out[i][1].row, out[j][1].row)
	})
	return out
}

// unifyPodcasts merges two podcasts that one identity decides on (PROTOCOL §6.11): the
// survivor is the one created first, by the rank of the decision that
// created it (its podcast.alias or creating podcast.resolution: a recovered
// original wins over a podcast created after the restore), else by its creating node's rank
// (PROTOCOL §9.7).
func unifyPodcasts(c *protocol.ApplyContext, pa, pb string) (bool, error) {
	v := newView(c)
	ra, rb := v.podcastRoot(pa), v.podcastRoot(pb)
	if v.err != nil {
		return false, v.err
	}
	if ra == rb || ra == "" || rb == "" {
		return false, nil
	}
	for _, p := range []string{ra, rb} {
		if exists, err := c.Account.PodcastExists(p); err != nil || !exists {
			return false, err
		}
	}
	ka, err := creationRank(c.Account, ra)
	if err != nil {
		return false, err
	}
	kb, err := creationRank(c.Account, rb)
	if err != nil {
		return false, err
	}
	survivor, source := ra, rb
	if kb.less(ka) {
		survivor, source = rb, ra
	}
	if err := mergePodcastInto(c, survivor, source, nil, map[string]any{"source": source, "into": survivor, "cause": "recovered"}); err != nil {
		return false, err
	}
	return true, nil
}

type creation struct {
	byNode   bool
	row      storage.DecisionRow
	nodeRank int64
}

func (a creation) less(b creation) bool {
	if a.byNode != b.byNode {
		return !a.byNode
	}
	if a.byNode {
		return a.nodeRank < b.nodeRank
	}
	return rankLess(a.row, b.row)
}

func creationRank(acct *storage.AccountTx, p string) (creation, error) {
	ds, err := rankedDecisions(acct, "podcast.alias", "podcast.resolution")
	if err != nil {
		return creation{}, err
	}
	for _, d := range ds { // rank order: the first is the smallest
		if resultString(d, "podcast_id") != p {
			continue
		}
		if d.row.DecisionType == "podcast.alias" || resultString(d, "mode") == "created" {
			return creation{row: d.row}, nil
		}
	}
	op, err := acct.PodcastCreatingOp(p)
	if err != nil {
		return creation{}, err
	}
	r, err := acct.NodeRank(op)
	return creation{byNode: true, nodeRank: r}, err
}

// unifyEpisodes merges what the later decision eb named into what the
// effective ea named, by an episode.merge {podcast_id, cause: recovered, by}.
func unifyEpisodes(c *protocol.ApplyContext, ea, eb string, a, b rankedDecision) (bool, error) {
	v := newView(c)
	ra, rb := v.root(ea), v.root(eb)
	if v.err != nil {
		return false, v.err
	}
	if ra == rb || ra == "" || rb == "" {
		return false, nil
	}
	inputs, _ := object(a.obj["inputs"])
	p, _ := str(inputs, "podcast_id")
	if p == "" {
		if p, _ = c.Account.EpisodePodcast(ra); p == "" {
			return false, nil
		}
	}
	p = v.podcastRoot(p)
	if v.err != nil {
		return false, v.err
	}
	d, err := c.Decide(protocol.DecisionInput{Type: "episode.merge", Version: 1, Subject: "merge:" + rb,
		Dependencies: []string{a.row.DecisionID, b.row.DecisionID},
		Inputs:       map[string]any{"podcast_id": p, "cause": "recovered", "by": a.row.DecisionID},
		Result:       map[string]any{"from": rb, "into": ra}})
	if err != nil || d == nil {
		return false, err
	}
	return true, admitEpisodeMerge(c, d)
}
