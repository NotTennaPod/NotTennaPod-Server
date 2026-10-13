package ops

import (
	"encoding/json"
	"errors"
	"slices"
	"sort"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Identity reconciliation (PROTOCOL §9.7): occurrence evidence, the derivation
// with evidence, release groups of disputed GUIDs, lookup aliases, routing of
// episode references, the placement of contributions, and episode and podcast
// merges. It follows the independent Python reference (contract/reference/
// rules.py) decision for decision.

const releaseWindow = int64(7 * 24 * 3600 * 1000) // ms: one release (PROTOCOL §3.3, §9.7)

// occurrence is identity evidence (PROTOCOL §9.7): a trimmed GUID, a
// normalized enclosure and a publication time ("" and nil when absent).
type occurrence struct {
	guid, enclosure string
	pub             *int64
}

func (o occurrence) is(x storage.Occurrence) bool {
	return o.guid == x.GUID && o.enclosure == x.Enclosure && samePub(o.pub, x.PubDate)
}

func (o occurrence) row() storage.Occurrence {
	return storage.Occurrence{GUID: o.guid, Enclosure: o.enclosure, PubDate: o.pub}
}

func samePub(a, b *int64) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

// occurrenceOf reads an occurrence object's evidence: its guid,
// enclosure_url and publication_time members (k and v are not evidence).
func occurrenceOf(o map[string]any) occurrence {
	var x occurrence
	if g, ok := str(o, "guid"); ok {
		if t, ok := contract.TrimGUID(g); ok {
			x.guid = t
		}
	}
	if u, ok := str(o, "enclosure_url"); ok && u != "" {
		x.enclosure = contract.NormalizeURL(u)
	}
	if t, ok := integer(o["publication_time"]); ok {
		x.pub = &t
	}
	return x
}

// guidKind is an occurrence's GUID kind: atom-id, else rss-guid.
func guidKind(o map[string]any) string {
	if k, _ := str(o, "k"); k == contract.KindAtomID {
		return contract.KindAtomID
	}
	return contract.KindRSSGUID
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// ---- the identity state, read with memoization

// idView reads the identity state for one step (between changes): merge
// roots, podcast merges, release groups, lookup aliases and disputed
// enclosures. Its first error sticks in err.
type idView struct {
	acct     *storage.AccountTx
	ns       string
	err      error
	roots    map[string]string
	pmerges  map[string]*storage.PodcastMerge
	pmEps    map[string]map[string]string
	releases map[[2]string][]storage.Release
	aliases  map[[3]string]string
	denc     map[string]map[string]bool
	genPod   map[string]string
}

func newView(ctx *protocol.ApplyContext) *idView { return newViewOf(ctx.Account, ctx.Namespace) }

func newViewOf(acct *storage.AccountTx, ns string) *idView {
	return &idView{acct: acct, ns: ns, roots: map[string]string{}, pmerges: map[string]*storage.PodcastMerge{},
		pmEps: map[string]map[string]string{}, releases: map[[2]string][]storage.Release{}, aliases: map[[3]string]string{},
		denc: map[string]map[string]bool{}, genPod: map[string]string{}}
}

func (v *idView) fail(err error) {
	if v.err == nil && err != nil {
		v.err = err
	}
}

func (v *idView) root(e string) string {
	if e == "" {
		return ""
	}
	if r, ok := v.roots[e]; ok {
		return r
	}
	r, err := v.acct.MergeRoot(e)
	v.fail(err)
	v.roots[e] = r
	return r
}

func (v *idView) eid(podcast, kind, value string) string {
	id, err := contract.EpisodeID(v.ns, podcast, kind, value)
	v.fail(err)
	return id
}

func (v *idView) guidEnclosureID(podcast, guid, enclosure string) string {
	return v.eid(podcast, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue(guid, enclosure))
}

// podcastMerge is a podcast's merge (nil when it was not merged).
func (v *idView) podcastMerge(p string) *storage.PodcastMerge {
	if m, ok := v.pmerges[p]; ok {
		return m
	}
	m, merged, err := v.acct.PodcastMergeStep(p)
	v.fail(err)
	var out *storage.PodcastMerge
	if merged {
		out = &m
	}
	v.pmerges[p] = out
	return out
}

// podcastRoot is the end of a podcast's merge chain.
func (v *idView) podcastRoot(p string) string {
	seen := map[string]bool{}
	for !seen[p] {
		seen[p] = true
		m := v.podcastMerge(p)
		if m == nil {
			break
		}
		p = m.Into
	}
	return p
}

// sameFamily reports whether two podcasts are one merge family: a reference
// naming either is its own podcast's (reference CHOICE), as an episode a merge carried names the
// survivor.
func (v *idView) sameFamily(a, b string) bool {
	return a == b || v.podcastRoot(a) == v.podcastRoot(b)
}

// mergeMode is how a podcast merge treats a source generation: its live one
// joined into the survivor's live class, or rehomed as the survivor's live
// generation; every other one closed.
func mergeMode(m *storage.PodcastMerge, generation string) (string, string) {
	if m.FromGeneration == "" || generation != m.FromGeneration {
		return "closed", ""
	}
	if m.IntoGeneration == m.FromGeneration {
		return "rehomed", generation
	}
	return "joined", m.IntoGeneration
}

// mergeEpisodes is a podcast merge decision's episode mappings.
func (v *idView) mergeEpisodes(m *storage.PodcastMerge) map[string]string {
	if out, ok := v.pmEps[m.Node]; ok {
		return out
	}
	out := map[string]string{}
	v.pmEps[m.Node] = out
	raw, err := v.acct.DecisionBytes(m.Node)
	if err != nil {
		v.fail(err)
		return out
	}
	d, err := contract.DecodeJSON(raw)
	if err != nil {
		v.fail(err)
		return out
	}
	obj, _ := object(d)
	res, _ := object(obj["result"])
	list, _ := res["episodes"].([]any)
	for _, x := range list {
		mm, _ := object(x)
		from, _ := str(mm, "from")
		to, _ := str(mm, "to")
		out[from] = to
	}
	return out
}

// joined is where a provisional generation joined into another goes (PROTOCOL §6.11, §9.7's join):
// the effective generation, else itself.
func (v *idView) joined(g string) string {
	for seen := map[string]bool{}; g != "" && !seen[g]; {
		seen[g] = true
		into, err := v.acct.GenerationJoin(g)
		if err != nil {
			v.fail(err)
			return g
		}
		if into == "" {
			return g
		}
		g = into
	}
	return g
}

func (v *idView) generationPodcast(g string) string {
	if p, ok := v.genPod[g]; ok {
		return p
	}
	row, err := v.acct.Generation(g)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		v.fail(err)
	}
	v.genPod[g] = row.PodcastID
	return row.PodcastID
}

// routeScope is where a generation's effects live now: a source generation a
// podcast merge joined into the survivor's live class goes there.
func (v *idView) routeScope(g string) string {
	g = v.joined(g)
	seen := map[string]bool{}
	for g != "" && !seen[g] {
		seen[g] = true
		p := v.generationPodcast(g)
		if p == "" {
			break
		}
		m := v.podcastMerge(p)
		if m == nil {
			break
		}
		mode, g2 := mergeMode(m, g)
		if mode != "joined" {
			break
		}
		g = g2
	}
	return g
}

// releasesOf are a GUID's release groups (its mapping decisions' results,
// in rank order); of releases whose episodes are one (merged), the earliest.
// Each lists the enclosures added to its episode, in rank order.
func (v *idView) releasesOf(p, g string) []storage.Release {
	k := [2]string{p, g}
	if rs, ok := v.releases[k]; ok {
		return rs
	}
	all, err := v.acct.Releases(p, g)
	v.fail(err)
	var rs []storage.Release
	at := map[string]int{}
	for _, r := range all {
		if root := v.root(r.EpisodeID); !hasKey(at, root) {
			at[root] = len(rs)
			r.Additions = nil
			rs = append(rs, r)
		}
	}
	if len(rs) > 0 {
		adds, err := v.acct.ReleaseAdditions(p, g)
		v.fail(err)
		for _, x := range adds {
			if i, ok := at[v.root(x.EpisodeID)]; ok && !rs[i].Lists(x.Enclosure) {
				rs[i].Additions = append(rs[i].Additions, x.Enclosure)
			}
		}
	}
	v.releases[k] = rs
	return rs
}

func hasKey[K comparable, V any](m map[K]V, k K) bool {
	_, ok := m[k]
	return ok
}

func (v *idView) disputed(p, g string) bool { return g != "" && len(v.releasesOf(p, g)) > 0 }

// original is the root of the release keeping a disputed GUID's original ID.
func (v *idView) original(p, g string) string {
	for _, r := range v.releasesOf(p, g) {
		if r.Original {
			return v.root(r.EpisodeID)
		}
	}
	return ""
}

func (v *idView) family(p, g string) map[string]bool {
	out := map[string]bool{}
	for _, r := range v.releasesOf(p, g) {
		out[v.root(r.EpisodeID)] = true
	}
	return out
}

// releaseFor is the PROTOCOL §9.7 resolver: a dated item goes to the release whose
// publication time lies within 7 days of its own (the nearest; ties to the
// earlier release, then the smaller episode ID), and to none when no release
// is that near; an undated one to the release holding its enclosure if
// exactly one does, else to the original.
func (v *idView) releaseFor(p, g, u string, d *int64) (string, bool) {
	rels := v.releasesOf(p, g)
	if d != nil {
		best := nearestRelease(rels, *d)
		if best < 0 {
			return "", false
		}
		return v.root(rels[best].EpisodeID), true
	}
	if u != "" {
		var with []storage.Release
		for _, r := range rels {
			if r.Lists(u) {
				with = append(with, r)
			}
		}
		if len(with) == 1 {
			return v.root(with[0].EpisodeID), true
		}
	}
	o := v.original(p, g)
	return o, o != ""
}

// nearestRelease is the index of the release whose publication time lies
// nearest d within 7 days (ties to the earlier release, then the smaller
// episode ID); -1 for none.
func nearestRelease(rels []storage.Release, d int64) int {
	best := -1
	for i, r := range rels {
		dist := abs64(d - r.PubDate)
		if dist > releaseWindow {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		b := rels[best]
		bd := abs64(d - b.PubDate)
		if dist < bd || (dist == bd && (r.PubDate < b.PubDate || (r.PubDate == b.PubDate && r.EpisodeID < b.EpisodeID))) {
			best = i
		}
	}
	return best
}

// lookupAlias is the root of a server-decided alias ("" for none).
func (v *idView) lookupAlias(p, kind, value string) string {
	k := [3]string{p, kind, value}
	if e, ok := v.aliases[k]; ok {
		return e
	}
	e, err := v.acct.LookupAlias(p, kind, value)
	v.fail(err)
	e = v.root(e)
	v.aliases[k] = e
	return e
}

// disputedEnclosures are a podcast's enclosures observed for two episodes
// (roots) with different GUIDs (an absent GUID differs from any), from the
// evidence (PROTOCOL §9.7).
func (v *idView) disputedEnclosures(p string) map[string]bool {
	if out, ok := v.denc[p]; ok {
		return out
	}
	occs, err := v.acct.Occurrences(p)
	v.fail(err)
	type pair struct{ episode, guid string }
	byU := map[string]map[pair]bool{}
	for _, o := range occs {
		if o.Enclosure == "" || o.EpisodeID == "" {
			continue
		}
		if byU[o.Enclosure] == nil {
			byU[o.Enclosure] = map[pair]bool{}
		}
		byU[o.Enclosure][pair{v.root(o.EpisodeID), o.GUID}] = true
	}
	out := map[string]bool{}
	for u, pairs := range byU {
	search:
		for a := range pairs {
			for b := range pairs {
				if a.episode != b.episode && a.guid != b.guid {
					out[u] = true
					break search
				}
			}
		}
	}
	v.denc[p] = out
	return out
}

// derive is the server's derivation with the evidence (PROTOCOL §9.7, §3.3 rules
// 1–4): a disputed GUID's release (rss-guid+enclosure when a dated item
// matches none, the original for an undated one without enclosure), an
// undisputed GUID's lookup alias or ID, and without a GUID the enclosure's
// lookup alias (when undisputed) or ID. "" without GUID and enclosure.
func (v *idView) derive(p string, o occurrence, kfam string, rooted bool) string {
	rt := v.root
	if !rooted {
		rt = func(e string) string { return e }
	}
	switch {
	case o.guid != "":
		if v.disputed(p, o.guid) {
			if r, ok := v.releaseFor(p, o.guid, o.enclosure, o.pub); ok {
				return r
			}
			if o.enclosure != "" {
				return rt(v.guidEnclosureID(p, o.guid, o.enclosure))
			}
			return v.original(p, o.guid)
		}
		if a := v.lookupAlias(p, "guid", o.guid); a != "" {
			return a
		}
		return rt(v.eid(p, kfam, o.guid))
	case o.enclosure != "":
		if !v.disputedEnclosures(p)[o.enclosure] {
			if a := v.lookupAlias(p, "enclosure", o.enclosure); a != "" {
				return a
			}
		}
		return rt(v.eid(p, contract.KindEnclosure, o.enclosure))
	}
	return ""
}

// plainIDs are an occurrence's plain derivations (rules 2–4), as roots.
func (v *idView) plainIDs(p string, o occurrence, kfam string) map[string]bool {
	out := map[string]bool{}
	switch {
	case o.guid != "":
		out[v.root(v.eid(p, kfam, o.guid))] = true
		if o.enclosure != "" {
			out[v.root(v.guidEnclosureID(p, o.guid, o.enclosure))] = true
		}
	case o.enclosure != "":
		out[v.root(v.eid(p, contract.KindEnclosure, o.enclosure))] = true
	}
	return out
}

// routeIdentity: a reference to a plain derivation of its occurrence, or to
// its disputed GUID's release family, goes where the server's rules put the
// occurrence (the release its date or enclosure resolves to; a lookup
// alias); any other reference keeps what it names (PROTOCOL §9.7).
func (v *idView) routeIdentity(p, e0 string, o occurrence, kfam string) string {
	switch {
	case o.guid != "" && v.disputed(p, o.guid):
		if v.family(p, o.guid)[e0] || v.plainIDs(p, o, kfam)[e0] {
			if r, ok := v.releaseFor(p, o.guid, o.enclosure, o.pub); ok {
				return r
			}
		}
	case o.guid != "":
		if e0 == v.root(v.eid(p, kfam, o.guid)) {
			if a := v.lookupAlias(p, "guid", o.guid); a != "" {
				return a
			}
		}
	case o.enclosure != "":
		if e0 == v.root(v.eid(p, contract.KindEnclosure, o.enclosure)) && !v.disputedEnclosures(p)[o.enclosure] {
			if a := v.lookupAlias(p, "enclosure", o.enclosure); a != "" {
				return a
			}
		}
	}
	return e0
}

// route routes an episode reference (PROTOCOL §9.7): through podcast
// merges by its podcast and generation ("" for a catalog record: no
// generation to route; a closed source generation answers
// generation_closed), the source episode to its mapping, else where the
// survivor's rules derive it; then merge redirects and routeIdentity.
func (v *idView) route(p, g, e string, o occurrence, kfam string) (string, string, string, string) {
	g = v.joined(g)
	seen := map[string]bool{}
	for !seen[p] {
		m := v.podcastMerge(p)
		if m == nil {
			break
		}
		seen[p] = true
		g2 := ""
		if g != "" {
			mode, into := mergeMode(m, g)
			if mode == "closed" {
				return "", "", "", "generation_closed"
			}
			g2 = into
		}
		dest := ""
		if r := v.root(e); r != e {
			dest = r
		} else if to := v.mergeEpisodes(m)[e]; to != "" {
			dest = to
		} else if d := v.derive(m.Into, o, kfam, true); d != "" {
			dest = d
		} else {
			dest = e
		}
		e, p, g = dest, m.Into, g2
	}
	e = v.root(e)
	return v.routeIdentity(p, e, o, kfam), p, g, ""
}

// ---- evidence and release groups (PROTOCOL §9.7)

// recordEvidence records an occurrence as evidence of a podcast, with its
// GUID kind and the node recording it; true when it was new.
func recordEvidence(ctx *protocol.ApplyContext, p string, o occurrence, node, kfam string) (bool, error) {
	if o.guid == "" && o.enclosure == "" {
		return false, nil
	}
	r := o.row()
	r.Node, r.Kind = node, kfam
	return ctx.Account.AddOccurrence(p, r)
}

// observeEvidence is recordEvidence for a device's archived observation: of
// evidence a server fetch recorded first, it becomes the witness
// (storage.ClaimOccurrence), so a rebuild that loses the fetch's row gives
// the same row.
func observeEvidence(ctx *protocol.ApplyContext, p string, o occurrence, node, kfam string) (bool, error) {
	isNew, err := recordEvidence(ctx, p, o, node, kfam)
	if err != nil || isNew || (o.guid == "" && o.enclosure == "") {
		return isNew, err
	}
	r := o.row()
	r.Node = node
	return false, ctx.Account.ClaimOccurrence(p, r)
}

func setEvidenceEpisode(ctx *protocol.ApplyContext, p string, o occurrence, e string) error {
	if e == "" {
		return nil
	}
	return ctx.Account.SetOccurrenceEpisode(p, o.row(), e)
}

// detect is the reused-GUID rule (PROTOCOL §9.7) for a newly recorded dated
// occurrence (g, u, d) with an enclosure. The dispute rule holds for an
// occurrence when g was observed with another enclosure more than 7 days
// away.
//
// At the first dispute the GUID's evidence is partitioned into releases (reference CHOICE): its
// dated occurrences with an enclosure, in (date, enclosure) order; the first makes the original
// release, a later one a release when it is more than 7 days from every release so far and the
// dispute rule holds for it; each other occurrence's enclosure then joins the nearest release
// within 7 days (none: the resolver places it). Undated occurrences and those without an
// enclosure join no release. The original release keeps the original episode (the lookup alias
// of g, else its plain ID). Once disputed, an occurrence near no release starts a new one when
// the dispute rule holds for it; one whose release (by its date) does not list its enclosure
// adds the enclosure to that release. A later release's ID is rss-guid+enclosure of its first
// enclosure, or an allocated e0: ID when a release already has that one. Each release is an
// episode.occurrence_mapping decision, the original first:
//
// episode.occurrence_mapping (version 1), subject occurrence:<P>:<guid>:<E>:
// inputs {podcast_id, guid, found_by (the observing fact; the podcast merge
// for joining evidence; null for a server fetch)}; result {episode_id,
// original, publication_time, enclosures (normalized, first first),
// allocated}. Dependencies: deps (the observing fact, the podcast merge, or
// the podcast's creating fact for a fetch). before runs before the first
// decision (a fetch's part 0).
//
// An addition is an episode.occurrence_mapping too: subject
// occurrence:<P>:<guid>:<E>:<enclosure>, the same inputs and dependencies,
// result {episode_id (the release's), enclosure}. It never competes with a
// release and is no release in recovery's unification.
func detect(ctx *protocol.ApplyContext, p string, o occurrence, kfam, foundBy string, deps []string, before func() error) error {
	if o.guid == "" || o.enclosure == "" || o.pub == nil {
		return nil
	}
	acct := ctx.Account
	all, err := acct.OccurrencesOfGUID(p, o.guid)
	if err != nil {
		return err
	}
	// disputes: the dispute rule for a dated occurrence with an enclosure.
	disputes := func(x storage.Occurrence) bool {
		for _, y := range all {
			if y.Enclosure != "" && y.Enclosure != x.Enclosure && y.PubDate != nil && abs64(*x.PubDate-*y.PubDate) > releaseWindow {
				return true
			}
		}
		return false
	}
	var found any
	if foundBy != "" {
		found = foundBy
	}
	decide := func(subject string, result map[string]any) error {
		if before != nil {
			if err := before(); err != nil {
				return err
			}
			before = nil
		}
		d, err := ctx.Decide(protocol.DecisionInput{Type: "episode.occurrence_mapping", Version: 1,
			Subject: subject, Dependencies: deps,
			Inputs: map[string]any{"podcast_id": p, "guid": o.guid, "found_by": found}, Result: result})
		if err != nil || d == nil {
			return err // nil: a recovery rebuild admits the decision at its own position
		}
		return admitOccurrenceMapping(ctx, d)
	}
	v := newView(ctx)
	type group struct {
		pt       int64
		encs     []string
		original bool
	}
	var groups []group
	orig := ""
	if rels := v.releasesOf(p, o.guid); len(rels) > 0 {
		if i := nearestRelease(rels, *o.pub); i >= 0 {
			if rels[i].Lists(o.enclosure) {
				return v.err
			}
			// An enclosure addition.
			e := v.root(rels[i].EpisodeID)
			if v.err != nil {
				return v.err
			}
			return decide("occurrence:"+p+":"+o.guid+":"+e+":"+o.enclosure, map[string]any{"episode_id": e, "enclosure": o.enclosure})
		}
		if !disputes(o.row()) {
			return v.err
		}
		groups = []group{{*o.pub, []string{o.enclosure}, false}}
	} else {
		if !disputes(o.row()) {
			return v.err
		}
		var dated []storage.Occurrence
		for _, x := range all {
			if x.PubDate != nil && x.Enclosure != "" {
				dated = append(dated, x)
			}
		}
		sort.SliceStable(dated, func(i, j int) bool {
			if *dated[i].PubDate != *dated[j].PubDate {
				return *dated[i].PubDate < *dated[j].PubDate
			}
			return dated[i].Enclosure < dated[j].Enclosure
		})
		makes := make([]bool, len(dated))
		for i, x := range dated {
			near := false
			for _, gr := range groups {
				if abs64(*x.PubDate-gr.pt) <= releaseWindow {
					near = true
					break
				}
			}
			if i == 0 || (!near && disputes(x)) {
				makes[i] = true
				groups = append(groups, group{pt: *x.PubDate, encs: []string{x.Enclosure}, original: i == 0})
			}
		}
		for i, x := range dated {
			if makes[i] {
				continue
			}
			best := -1
			for j, gr := range groups {
				dist := abs64(*x.PubDate - gr.pt)
				if dist <= releaseWindow && (best < 0 || dist < abs64(*x.PubDate-groups[best].pt)) {
					best = j
				}
			}
			if best >= 0 && !slices.Contains(groups[best].encs, x.Enclosure) {
				groups[best].encs = append(groups[best].encs, x.Enclosure)
			}
		}
		if a := v.lookupAlias(p, "guid", o.guid); a != "" {
			orig = a
		} else {
			orig = v.root(v.eid(p, kfam, o.guid))
		}
	}
	if v.err != nil {
		return v.err
	}
	for _, gr := range groups {
		e := orig
		allocated := false
		if !gr.original {
			id, err := contract.EpisodeID(ctx.Namespace, p, contract.KindGUIDAndEnclosure, contract.GUIDAndEnclosureValue(o.guid, gr.encs[0]))
			if err != nil {
				return err
			}
			fresh := newView(ctx)
			taken := map[string]bool{}
			for _, r := range fresh.releasesOf(p, o.guid) {
				taken[r.EpisodeID] = true
			}
			if fresh.err != nil {
				return fresh.err
			}
			if orig != "" {
				taken[orig] = true
			}
			e, allocated = id, taken[id]
			if allocated {
				e = "e0:" + ctx.Random().DecisionID("occurrence")
			}
		}
		encs := make([]any, len(gr.encs))
		for i, u := range gr.encs {
			encs[i] = u
		}
		if err := decide("occurrence:"+p+":"+o.guid+":"+e, map[string]any{"episode_id": e, "original": gr.original,
			"publication_time": gr.pt, "enclosures": encs, "allocated": allocated}); err != nil {
			return err
		}
	}
	return nil
}

// admitMapping: the release joins the GUID's resolver, its episode is
// registered (by the decision) if new, and every contribution is placed
// again.
func admitMapping(ctx *protocol.ApplyContext, p, g string, r storage.Release) error {
	if err := ctx.Account.AddRelease(p, r); err != nil {
		return err
	}
	occ := map[string]any{"k": contract.KindRSSGUID, "v": g, "guid": g}
	if !r.Original {
		occ = map[string]any{"k": contract.KindGUIDAndEnclosure, "v": contract.GUIDAndEnclosureValue(g, r.Enclosures[0]), "guid": g,
			"enclosure_url": r.Enclosures[0]}
	}
	occ["publication_time"] = r.PubDate
	if err := withOrigin(ctx, r.Node, func() error { return register(ctx, r.EpisodeID, p, occ) }); err != nil {
		return err
	}
	if err := placeAll(ctx, r.Node, p); err != nil {
		return err
	}
	return publishIdentityRules(ctx, r.Node, p)
}

// withOrigin runs fn with node as the origin of the records it publishes.
func withOrigin(ctx *protocol.ApplyContext, node string, fn func() error) error {
	saved := ctx.OpID
	ctx.OpID = node
	defer func() { ctx.OpID = saved }()
	return fn()
}

// ---- lookup aliases

// aliasPrec orders two claims of one lookup alias key (the reference's
// _alias_prec): a decision's (by rank) before a fact's (by receipt), then the
// smaller merged episode ID.
type aliasPrec struct {
	class            int
	epoch, seq, rank int64
	id, src          string
}

func (a aliasPrec) less(b aliasPrec) bool {
	switch {
	case a.class != b.class:
		return a.class < b.class
	case a.epoch != b.epoch:
		return a.epoch < b.epoch
	case a.seq != b.seq:
		return a.seq < b.seq
	case a.rank != b.rank:
		return a.rank < b.rank
	case a.id != b.id:
		return a.id < b.id
	}
	return a.src < b.src
}

func aliasPrecOf(acct *storage.AccountTx, node, src string) (aliasPrec, error) {
	if raw, err := acct.DecisionBytes(node); err == nil {
		v, err := contract.DecodeJSON(raw)
		if err != nil {
			return aliasPrec{}, err
		}
		d, _ := object(v)
		epoch, _ := integer(d["epoch"])
		seq, _ := integer(d["seq"])
		return aliasPrec{class: 0, epoch: epoch, seq: seq, id: node, src: src}, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return aliasPrec{}, err
	}
	if f, err := acct.Fact(node); err == nil {
		rank, err := acct.NodeRank(node)
		if err != nil {
			return aliasPrec{}, err
		}
		return aliasPrec{class: 1, epoch: f.Epoch, seq: f.Seq, rank: rank, id: node, src: src}, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return aliasPrec{}, err
	}
	return aliasPrec{class: 2, src: src}, nil
}

// putAlias adds a lookup alias (the reference's _put_alias): of two claims of
// one key the earlier-ranked one stands (aliasPrec), so recovery order does
// not matter. It reports whether the alias was added or replaced.
func putAlias(ctx *protocol.ApplyContext, p, kind, value, e, node, src string) (bool, error) {
	acct := ctx.Account
	cur, ok, err := acct.LookupAliasOf(p, kind, value)
	if err != nil {
		return false, err
	}
	if ok {
		a, err := aliasPrecOf(acct, node, src)
		if err != nil {
			return false, err
		}
		b, err := aliasPrecOf(acct, cur.Node, cur.Src)
		if err != nil {
			return false, err
		}
		if !a.less(b) {
			return false, nil
		}
	}
	return true, acct.SetLookupAlias(p, storage.LookupAliasRow{Kind: kind, Value: value, EpisodeID: e, Node: node, Src: src})
}

// ---- registration and identity records

// register registers an episode (if new) under a podcast with its
// occurrence, publishing its identity record. An episode a merge redirected
// before it was registered gains the lookup alias that merge would have
// added (its GUID, else its enclosure, in the survivor's podcast); true then.
func register(ctx *protocol.ApplyContext, e, p string, occ map[string]any) error {
	aliased, err := registerEpisode(ctx, e, p, occ)
	if err != nil || !aliased {
		return err
	}
	return placeAll(ctx, ctx.OpID, p)
}

func registerEpisode(ctx *protocol.ApplyContext, e, p string, occ map[string]any) (bool, error) {
	acct := ctx.Account
	if e == "" {
		return false, nil
	}
	known, err := acct.EpisodePodcast(e)
	if err != nil || known != "" {
		return false, err
	}
	raw, err := contract.Canonical(normalizeJSON(occ))
	if err != nil {
		return false, err
	}
	if err := acct.AddEpisode(e, p, string(raw), ctx.OpID); err != nil {
		return false, err
	}
	aliased := false
	if m, merged, err := acct.MergeOf(e); err != nil {
		return false, err
	} else if merged {
		o := occurrenceOf(occ)
		kind, value := "guid", o.guid
		if value == "" {
			kind, value = "enclosure", o.enclosure
		}
		if value != "" {
			pd := m.PodcastID
			if pd == "" {
				pd = p
			}
			top := newView(ctx).podcastRoot(pd)
			if aliased, err = putAlias(ctx, top, kind, value, m.Into, m.Node, e); err != nil {
				return false, err
			}
		}
	}
	return aliased, publishIdentity(ctx, e)
}

// publishIdentity publishes an episode's identity record: its podcast, the
// occurrence it was registered with, and its survivor once merged.
func publishIdentity(ctx *protocol.ApplyContext, e string) error {
	acct := ctx.Account
	p, err := acct.EpisodePodcast(e)
	if err != nil || p == "" {
		return err
	}
	raw, err := acct.EpisodeOccurrence(e)
	if err != nil {
		return err
	}
	occ, err := contract.DecodeJSON([]byte(raw))
	if err != nil {
		return err
	}
	value := map[string]any{"podcast_id": p, "occurrence": occ}
	if m, merged, err := acct.MergeOf(e); err != nil {
		return err
	} else if merged {
		value["merged_into"] = m.Into
	}
	return ctx.PublishRecord("episode", e, "identity", value)
}

// publishIdentityRules publishes a podcast's identity_rules record (PROTOCOL §9.7), or removes it
// when empty or merged away: its disputed GUIDs with their releases (by publication time, then
// episode), its disputed enclosures, and the lookup aliases whose episode differs from rules 2–4
// (aliases of disputed GUIDs or enclosures are not used, so not published).
func publishIdentityRules(ctx *protocol.ApplyContext, origin string, podcasts ...string) error {
	if ctx.Batched() {
		// Once per podcast at the commit's next flush.
		for _, p := range podcasts {
			ctx.Defer("rules:"+p, func(c *protocol.ApplyContext) error { return publishIdentityRulesNow(c, origin, p) })
		}
		return nil
	}
	return publishIdentityRulesNow(ctx, origin, podcasts...)
}

// publishIdentityRulesNow publishes the rules records now. Their provenance
// is the decisions that determine them (the releases' occurrence mappings
// and the lookup aliases' nodes listed), not the step that published them.
func publishIdentityRulesNow(ctx *protocol.ApplyContext, origin string, podcasts ...string) error {
	return withOrigin(ctx, origin, func() error {
		v := newView(ctx)
		done := map[string]bool{}
		for _, p0 := range podcasts {
			for _, p := range []string{p0, v.podcastRoot(p0)} {
				if done[p] {
					continue
				}
				done[p] = true
				value, provenance, err := identityRules(ctx, v, p)
				if err != nil {
					return err
				}
				if value == nil {
					if err := ctx.TombstoneRecord("podcast", p, "identity_rules"); err != nil {
						return err
					}
					continue
				}
				if err := ctx.PublishDerived("podcast", p, "identity_rules", value, provenance); err != nil {
					return err
				}
			}
		}
		return v.err
	})
}

func identityRules(ctx *protocol.ApplyContext, v *idView, p string) (map[string]any, []string, error) {
	acct := ctx.Account
	if v.podcastMerge(p) != nil {
		return nil, nil, v.err // a merged-away podcast keeps only its subscription record
	}
	var provenance []string
	guids, err := acct.DisputedGUIDs(p)
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(guids, func(i, j int) bool { return guids[i] < guids[j] })
	dg := []any{}
	for _, g := range guids {
		rels := v.releasesOf(p, g)
		type rel struct {
			episode string
			pt      int64
			encs    []string
		}
		list := make([]rel, len(rels))
		for i, r := range rels {
			// A release's enclosures, then its additions.
			list[i] = rel{v.root(r.EpisodeID), r.PubDate, append(slices.Clip(r.Enclosures), r.Additions...)}
			provenance = append(provenance, r.Node)
		}
		adds, err := acct.ReleaseAdditions(p, g)
		if err != nil {
			return nil, nil, err
		}
		for _, x := range adds {
			provenance = append(provenance, x.Node)
		}
		sort.SliceStable(list, func(i, j int) bool {
			if list[i].pt != list[j].pt {
				return list[i].pt < list[j].pt
			}
			return list[i].episode < list[j].episode
		})
		out := []any{}
		for _, r := range list {
			encs := []any{}
			for _, u := range r.encs {
				encs = append(encs, u)
			}
			out = append(out, map[string]any{"episode_id": r.episode, "publication_time": r.pt, "enclosures": encs})
		}
		dg = append(dg, map[string]any{"guid": g, "releases": out})
	}
	denc := v.disputedEnclosures(p)
	de := make([]string, 0, len(denc))
	for u := range denc {
		de = append(de, u)
	}
	sort.Strings(de)
	deList := []any{}
	for _, u := range de {
		deList = append(deList, u)
	}
	rows, err := acct.LookupAliases(p)
	if err != nil {
		return nil, nil, err
	}
	al := []any{}
	for _, a := range rows {
		e := v.root(a.EpisodeID)
		switch a.Kind {
		case "guid":
			if v.disputed(p, a.Value) || e == v.eid(p, contract.KindRSSGUID, a.Value) || e == v.eid(p, contract.KindAtomID, a.Value) {
				continue
			}
		default:
			if denc[a.Value] || e == v.eid(p, contract.KindEnclosure, a.Value) {
				continue
			}
		}
		al = append(al, map[string]any{"kind": a.Kind, "value": a.Value, "episode_id": e})
		provenance = append(provenance, a.Node)
	}
	if v.err != nil {
		return nil, nil, v.err
	}
	if len(dg) == 0 && len(deList) == 0 && len(al) == 0 {
		return nil, nil, nil
	}
	return map[string]any{"disputed_guids": dg, "disputed_enclosures": deList, "aliases": al}, provenance, nil
}

// ---- placement of contributions (the reference's _target and _place_all)

// refOf is the Ref of an episode reference as the fact named it.
func refOf(r EpisodeRef) *projection.Ref {
	nE, nP, nG := r.EpisodeID, r.PodcastID, r.GenerationID
	if r.named {
		nE, nP, nG = r.NamedEpisode, r.NamedPodcast, r.NamedGeneration
	}
	o := occurrenceOf(r.Occurrence)
	ref := &projection.Ref{Episode: nE, Podcast: nP, Generation: nG, GUID: o.guid, Enclosure: o.enclosure, Pub: o.pub}
	if guidKind(r.Occurrence) == contract.KindAtomID {
		ref.Kind = contract.KindAtomID
	}
	return ref
}

// withRef is a fact's effect made through an episode reference: it carries
// the reference as named, and the episode it named when it was routed
// elsewhere.
func withRef(e projection.Effect, r EpisodeRef) projection.Effect {
	e.Ref = refOf(r)
	if r.Original != "" && e.Member == "" {
		e.Member = r.Original
	}
	return e
}

func refOccurrence(r *projection.Ref) (occurrence, string) {
	kfam := contract.KindRSSGUID
	if r.Kind == contract.KindAtomID {
		kfam = contract.KindAtomID
	}
	return occurrence{guid: r.GUID, enclosure: r.Enclosure, pub: r.Pub}, kfam
}

// scopeEpisode splits an episode or catalog scope: its episode, its
// generation ("" for a catalog scope).
func scopeEpisode(scope string) (string, string) {
	if rest, ok := cutPrefix(scope, "item:"); ok {
		return rest, ""
	}
	rest, _ := cutPrefix(scope, "episode:")
	// generation IDs and episode IDs both hold one ':' ("g1:…", "e1:…").
	n := 0
	for i := 0; i < len(rest); i++ {
		if rest[i] == ':' {
			n++
			if n == 2 {
				return rest[i+1:], rest[:i]
			}
		}
	}
	return rest, ""
}

func cutPrefix(s, p string) (string, bool) {
	if len(s) >= len(p) && s[:len(p)] == p {
		return s[len(p):], true
	}
	return s, false
}

// target is where one effect lives now: a fact's effect through a reference
// where that reference routes (a closed source generation: where it is); any
// other effect at the root of the episode it was made for, in the live class
// its generation joined.
func (v *idView) target(e projection.Effect, scope string) string {
	ep, gen := scopeEpisode(scope)
	item := gen == ""
	if e.Ref != nil {
		o, kfam := refOccurrence(e.Ref)
		g := e.Ref.Generation
		if item {
			g = ""
		}
		re, _, rg, reason := v.route(e.Ref.Podcast, g, e.Ref.Episode, o, kfam)
		if reason != "" || re == "" {
			return scope
		}
		if item {
			return protocol.ItemScope(re)
		}
		return protocol.EpisodeScope(rg, re)
	}
	orig := e.Member
	if orig == "" {
		orig = ep
	}
	root := v.root(orig)
	if item {
		return protocol.ItemScope(root)
	}
	return protocol.EpisodeScope(v.routeScope(gen), root)
}

func parts(e projection.Effect) []projection.Effect {
	if e.Kind == projection.KindCompound {
		return append([]projection.Effect(nil), e.Compound...)
	}
	return []projection.Effect{e}
}

// placedParts are an effect's parts as placement places them: an import that
// grouped several source rows is one part per row, routed by the row's own
// occurrence.
func placedParts(e projection.Effect) []projection.Effect {
	var out []projection.Effect
	for _, x := range parts(e) {
		if x.Kind != projection.KindImport || x.Imported == nil || len(x.Imported.Rows) == 0 {
			out = append(out, x)
			continue
		}
		for _, r := range x.Imported.Rows {
			out = append(out, projection.Effect{Kind: projection.KindImport, Ref: r.Ref,
				Imported: &projection.Imported{Rows: []projection.ImportedRow{r}}})
		}
	}
	return out
}

// importRows reports a part placedParts made of an import row.
func importRows(e projection.Effect) bool {
	return e.Kind == projection.KindImport && e.Imported != nil && len(e.Imported.Rows) > 0
}

// joinAt makes one effect of a node's parts in one scope: the rows of an
// import aggregated again (importedAt), the rest composed with it (PROTOCOL §9.7).
func joinAt(scope string, es []projection.Effect) projection.Effect {
	var rows []projection.ImportedRow
	seen := map[int64]bool{}
	var rest []projection.Effect
	for _, x := range es {
		if !importRows(x) {
			rest = append(rest, x)
			continue
		}
		for _, r := range x.Imported.Rows {
			if !seen[r.SourceItem] {
				seen[r.SourceItem] = true
				rows = append(rows, r)
			}
		}
	}
	ep, _ := scopeEpisode(scope)
	if len(rows) > 0 {
		rest = append(rest, importedAt(scope, rows))
	}
	return composeAt(ep, rest)
}

// composeAt makes one effect of a node's effects in one scope (PROTOCOL §9.7); an
// effect without a member is the scope's own episode's.
func composeAt(episode string, es []projection.Effect) projection.Effect {
	if len(es) == 1 {
		return es[0]
	}
	for i := range es {
		if es[i].Member == "" {
			es[i].Member = episode
		}
	}
	return projection.Compose(es...)
}

func listeningKind(k string) bool {
	switch k {
	case projection.KindSessionStart, projection.KindCheckpoint, projection.KindSessionEnd, projection.KindFinalize,
		projection.KindHistoryRemove, projection.KindHistoryRestore, projection.KindImport, projection.KindLeave:
		return true
	}
	return false
}

// placeAll recomputes where every episode and catalog contribution of the
// podcasts (and of every podcast merged into them) lives after an identity
// change, moving those whose place changed, with their listening indexes;
// account-level cuts then follow the entries they cover. A pure function of
// the identity state, so the arrival order of identity decisions and facts
// does not matter.
// In a server commit (a fetch, an import, their admit steps) the placement
// runs once per podcast at the commit's next flush (a whole-feed GUID change once placed every
// contribution per alias).
func placeAll(ctx *protocol.ApplyContext, origin string, podcasts ...string) error {
	if ctx.Batched() {
		for _, p := range podcasts {
			ctx.Defer("place:"+p, func(c *protocol.ApplyContext) error {
				return withOrigin(c, origin, func() error { return placeAllNow(c, []string{p}) })
			})
		}
		return nil
	}
	return withOrigin(ctx, origin, func() error { return placeAllNow(ctx, podcasts) })
}

func placeAllNow(ctx *protocol.ApplyContext, podcasts []string) error {
	acct := ctx.Account
	v := newView(ctx)
	set := map[string]bool{}
	var queue []string
	for _, p := range podcasts {
		queue = append(queue, p, v.podcastRoot(p))
	}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		if set[p] {
			continue
		}
		set[p] = true
		srcs, err := acct.PodcastsMergedInto(p)
		if err != nil {
			return err
		}
		queue = append(queue, srcs...)
	}
	pods := make([]string, 0, len(set))
	for p := range set {
		pods = append(pods, p)
	}
	sort.Strings(pods)
	scopeSet := map[string]bool{}
	var scopes []string
	add := func(s string) {
		if !scopeSet[s] {
			scopeSet[s] = true
			scopes = append(scopes, s)
		}
	}
	for _, p := range pods {
		gens, err := acct.Generations(p)
		if err != nil {
			return err
		}
		for _, g := range gens {
			ss, err := acct.GenerationEpisodeScopes(g.ID)
			if err != nil {
				return err
			}
			for _, s := range ss {
				add(s)
			}
		}
		eps, err := acct.EpisodesOfPodcast(p)
		if err != nil {
			return err
		}
		for _, e := range eps {
			add(protocol.ItemScope(e))
		}
	}
	sort.Strings(scopes)
	return placeScopes(ctx, v, scopes)
}

// placeScopes moves the contributions of these scopes whose place changed.
// The identity state changed: the queue replay routes its steps again, from
// the start (PROTOCOL §7.6: a merge, an identity decision or a
// generation join affects the earliest command naming the identity).
func placeScopes(ctx *protocol.ApplyContext, v *idView, scopes []string) error {
	ctx.QueueInvalidateAll()
	acct := ctx.Account
	scopeSet := map[string]bool{}
	for _, s := range scopes {
		scopeSet[s] = true
	}
	type placed struct {
		e      projection.Effect
		target string
	}
	type move struct {
		from, node string
		parts      []placed
	}
	var moves []move
	for _, scope := range scopes {
		rows, err := acct.Contributions(scope)
		if err != nil {
			return err
		}
		for _, r := range rows {
			e, err := decodeEffect(r.Effect)
			if err != nil {
				return err
			}
			changed := false
			var ps []placed
			for _, x := range placedParts(e) {
				t := v.target(x, scope)
				if t != scope {
					changed = true
				}
				ps = append(ps, placed{x, t})
			}
			if v.err != nil {
				return v.err
			}
			if changed {
				moves = append(moves, move{scope, r.NodeID, ps})
			}
		}
	}
	if len(moves) == 0 {
		return nil
	}
	type key struct{ scope, node string }
	incoming := map[key][]projection.Effect{}
	var order []key
	touched := map[string]bool{}
	for _, m := range moves {
		if err := acct.RemoveContribution(m.from, m.node); err != nil {
			return err
		}
		fromEp, _ := scopeEpisode(m.from)
		touched[fromEp] = true
		ctx.MarkDirty(m.from)
		for _, x := range m.parts {
			if x.target != m.from && x.e.Member == "" && !importRows(x.e) {
				x.e.Member = fromEp
			}
			k := key{x.target, m.node}
			if _, ok := incoming[k]; !ok {
				order = append(order, k)
			}
			incoming[k] = append(incoming[k], x.e)
		}
	}
	for _, k := range order {
		ep, _ := scopeEpisode(k.scope)
		touched[ep] = true
		es := incoming[k]
		if existing, ok, err := acct.Contribution(k.scope, k.node); err != nil {
			return err
		} else if ok {
			x, err := decodeEffect(existing)
			if err != nil {
				return err
			}
			es = append(placedParts(x), es...)
		}
		joined := joinAt(k.scope, es)
		canonical, err := canonicalEffect(joined)
		if err != nil {
			return err
		}
		if err := acct.Contribute(k.scope, k.node, canonical); err != nil {
			return err
		}
		ctx.MarkDirty(k.scope)
	}
	// Listening indexes follow their node's listening effect; a grouped
	// import's are set per scope from the rows it holds there.
	for _, m := range moves {
		if importRows(m.parts[0].e) {
			held := []string{m.from}
			seen := map[string]bool{m.from: true}
			for _, x := range m.parts {
				if !seen[x.target] {
					seen[x.target] = true
					held = append(held, x.target)
				}
			}
			for _, sc := range held {
				var im *projection.Imported
				if raw, ok, err := acct.Contribution(sc, m.node); err != nil {
					return err
				} else if ok {
					x, err := decodeEffect(raw)
					if err != nil {
						return err
					}
					for _, p := range parts(x) {
						if p.Kind == projection.KindImport {
							im = p.Imported
						}
					}
				}
				if err := indexImported(ctx, m.node, sc, im); err != nil {
					return err
				}
			}
			continue
		}
		to := ""
		for _, x := range m.parts {
			if listeningKind(x.e.Kind) {
				to = x.target
				break
			}
		}
		if to == "" {
			to = m.parts[0].target
		}
		if to != m.from {
			if err := acct.RescopeListening(m.from, to, []string{m.node}); err != nil {
				return err
			}
		}
	}
	for _, k := range order {
		if !scopeSet[k.scope] {
			scopeSet[k.scope] = true
			scopes = append(scopes, k.scope)
		}
	}
	return refanCuts(ctx, touched, scopes)
}

// refanCuts regroups the account-level cuts (history clears, statistics
// resets) over the touched episodes' scopes of each generation: each scope
// gets the cut of exactly the entries (sessions) it holds.
func refanCuts(ctx *protocol.ApplyContext, touched map[string]bool, scopes []string) error {
	acct := ctx.Account
	byGen := map[string][]string{}
	var gens []string
	for _, s := range scopes {
		e, g := scopeEpisode(s)
		if g == "" || !touched[e] {
			continue
		}
		if _, ok := byGen[g]; !ok {
			gens = append(gens, g)
		}
		byGen[g] = append(byGen[g], s)
	}
	sort.Strings(gens)
	for _, g := range gens {
		keys := byGen[g]
		sort.Slice(keys, func(i, j int) bool {
			a, _ := scopeEpisode(keys[i])
			b, _ := scopeEpisode(keys[j])
			return a < b
		})
		type cut struct {
			kind    string
			rank    int64
			covered map[string]bool
			cuts    map[projection.SessionCut]bool
		}
		cuts := map[string]*cut{}
		for _, k := range keys {
			rows, err := acct.Contributions(k)
			if err != nil {
				return err
			}
			for _, r := range rows {
				e, err := decodeEffect(r.Effect)
				if err != nil {
					return err
				}
				if e.Kind != projection.KindHistoryClear && e.Kind != projection.KindStatsReset {
					continue
				}
				c := cuts[r.NodeID]
				if c == nil {
					c = &cut{kind: e.Kind, rank: r.Rank, covered: map[string]bool{}, cuts: map[projection.SessionCut]bool{}}
					cuts[r.NodeID] = c
				}
				for _, n := range e.Covered {
					c.covered[n] = true
				}
				for _, sc := range e.Cuts {
					c.cuts[sc] = true
				}
				if err := acct.RemoveContribution(k, r.NodeID); err != nil {
					return err
				}
				ctx.MarkDirty(k)
			}
		}
		if len(cuts) == 0 {
			continue
		}
		nodes := make([]string, 0, len(cuts))
		for n := range cuts {
			nodes = append(nodes, n)
		}
		sort.Slice(nodes, func(i, j int) bool { return cuts[nodes[i]].rank < cuts[nodes[j]].rank })
		for _, n := range nodes {
			c := cuts[n]
			for _, k := range keys {
				rows, err := acct.Contributions(k)
				if err != nil {
					return err
				}
				holds := map[string]bool{}
				sessions := map[string]bool{}
				for _, r := range rows {
					holds[r.NodeID] = true
					e, err := decodeEffect(r.Effect)
					if err != nil {
						return err
					}
					for _, x := range parts(e) {
						switch x.Kind {
						case projection.KindSessionStart, projection.KindCheckpoint, projection.KindSessionEnd, projection.KindFinalize:
							sessions[x.Session] = true
						case projection.KindImport:
							// An import decision is a one-ordinal session of its
							// own node (PROTOCOL §11.5): its statistics cut follows it.
							sessions[r.NodeID] = true
						}
					}
				}
				e := projection.Effect{Kind: c.kind}
				if c.kind == projection.KindHistoryClear {
					for x := range c.covered {
						if holds[x] {
							e.Covered = append(e.Covered, x)
						}
					}
					sort.Strings(e.Covered)
					if len(e.Covered) == 0 {
						continue
					}
				} else {
					for sc := range c.cuts {
						if sessions[sc.Session] {
							e.Cuts = append(e.Cuts, sc)
						}
					}
					if len(e.Cuts) == 0 {
						continue
					}
					sort.Slice(e.Cuts, func(i, j int) bool {
						a, b := e.Cuts[i], e.Cuts[j]
						if a.Session != b.Session {
							return a.Session < b.Session
						}
						return a.Author < b.Author
					})
				}
				canonical, err := canonicalEffect(e)
				if err != nil {
					return err
				}
				if err := acct.Contribute(k, n, canonical); err != nil {
					return err
				}
				ctx.MarkDirty(k)
			}
		}
	}
	return nil
}

func canonicalEffect(e projection.Effect) (string, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	v, err := contract.DecodeJSON(raw)
	if err != nil {
		return "", err
	}
	b, err := contract.Canonical(v)
	return string(b), err
}

// moveContribution moves a node's effect from one scope to another, composing
// it with the node's effect already there (PROTOCOL §9.7).
func moveContribution(acct *storage.AccountTx, from, to, node, effect, member string) error {
	e, err := decodeEffect(effect)
	if err != nil {
		return err
	}
	if e.Member == "" && e.Kind != projection.KindCompound {
		e.Member = member
	}
	if existing, ok, err := acct.Contribution(to, node); err != nil {
		return err
	} else if ok {
		x, err := decodeEffect(existing)
		if err != nil {
			return err
		}
		ep, _ := scopeEpisode(to)
		e = projection.ComposeAt(ep, x, e)
	}
	canonical, err := canonicalEffect(e)
	if err != nil {
		return err
	}
	if err := acct.RemoveContribution(from, node); err != nil {
		return err
	}
	return acct.Contribute(to, node, canonical)
}

// ---- merges (PROTOCOL §9.7)

// mergeEpisode merges root(from) into root(into) (PROTOCOL §9.7): a one-step redirect;
// the catalog item, the server's fetch history and every podcast's evidence
// move to the survivor; the merged episode's GUIDs (its enclosures without a
// GUID) become lookup aliases of the survivor in podcast (if not ""); its
// identity record names the survivor and its catalog record goes. Its
// contributions are placed again by the caller (placeAll).
func mergeEpisode(ctx *protocol.ApplyContext, podcast, from, into, node string) error {
	acct := ctx.Account
	v := newView(ctx)
	src, dst := v.root(from), v.root(into)
	if v.err != nil {
		return v.err
	}
	if src == dst || src == "" || dst == "" {
		return nil
	}
	earlier, err := acct.MergedInto(src)
	if err != nil {
		return err
	}
	if err := acct.AddMerge(src, dst, node, podcast); err != nil {
		return err
	}
	if ci, ok, err := acct.CatalogItem(src); err != nil {
		return err
	} else if ok {
		d, has, err := acct.CatalogItem(dst)
		if err != nil {
			return err
		}
		if !has {
			ci.EpisodeID = dst
			if podcast != "" {
				ci.PodcastID = podcast
			}
			d = ci
		} else {
			if d.PlayableNode == "" {
				d.PlayableNode = ci.PlayableNode
			}
			switch {
			case ci.ServerSeenNode == "":
			case d.ServerSeenNode == "":
				d.ServerSeenNode, d.ServerCatalogOnly = ci.ServerSeenNode, ci.ServerCatalogOnly
			default:
				d.ServerCatalogOnly = d.ServerCatalogOnly && ci.ServerCatalogOnly
			}
		}
		if err := acct.PutCatalogItem(d); err != nil {
			return err
		}
		if err := acct.DeleteCatalogItem(src); err != nil {
			return err
		}
	}
	var keys []occurrence
	if raw, err := acct.EpisodeOccurrence(src); err == nil {
		if occ, err := contract.DecodeJSON([]byte(raw)); err == nil {
			m, _ := object(occ)
			keys = append(keys, occurrenceOf(m))
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	ev, err := acct.EvidenceOfEpisode(src)
	if err != nil {
		return err
	}
	for _, x := range ev {
		keys = append(keys, occurrence{guid: x.GUID, enclosure: x.Enclosure, pub: x.PubDate})
	}
	if err := acct.RepointEvidence(src, dst); err != nil {
		return err
	}
	if podcast != "" {
		for _, k := range keys {
			kind, value := "guid", k.guid
			if value == "" {
				kind, value = "enclosure", k.enclosure
			}
			if value == "" {
				continue
			}
			if _, err := putAlias(ctx, podcast, kind, value, dst, node, src); err != nil {
				return err
			}
		}
	}
	return withOrigin(ctx, node, func() error {
		// The merged episode's contributions, in every generation (another
		// podcast's too), go where they belong now.
		scopes, err := acct.EpisodeScopes(src)
		if err != nil {
			return err
		}
		if err := placeScopes(ctx, newView(ctx), scopes); err != nil {
			return err
		}
		if err := ctx.TombstoneRecord("episode", src, "metadata"); err != nil {
			return err
		}
		for _, e := range append([]string{src}, earlier...) {
			if err := publishIdentity(ctx, e); err != nil {
				return err
			}
		}
		return nil
	})
}

// settleMerged: every registered episode of a merged-away podcast that is not
// merged yet goes where the survivor's rules derive it (registered there if
// new), through every merge of a chain.
func settleMerged(ctx *protocol.ApplyContext) error {
	acct := ctx.Account
	for changed := true; changed; {
		changed = false
		srcs, err := acct.MergedPodcasts()
		if err != nil {
			return err
		}
		for _, p := range srcs {
			v := newView(ctx)
			m := v.podcastMerge(p)
			if m == nil {
				continue
			}
			eps, err := acct.EpisodesOfPodcast(p)
			if err != nil {
				return err
			}
			for _, e := range eps {
				if v.root(e) != e {
					continue
				}
				raw, err := acct.EpisodeOccurrence(e)
				if err != nil {
					return err
				}
				occV, _ := contract.DecodeJSON([]byte(raw))
				occ, _ := object(occV)
				to := v.mergeEpisodes(m)[e]
				if to == "" {
					to = v.derive(m.Into, occurrenceOf(occ), guidKind(occ), false)
				}
				if v.err != nil {
					return v.err
				}
				if to == "" || to == e {
					// No evidence to map it by (no GUID, no enclosure: an
					// occurrence ID): the episode itself moves to the end of
					// the survivor's chain, with its state, so the merge hides
					// nothing.
					top := v.podcastRoot(m.Into)
					if v.err != nil {
						return v.err
					}
					if err := acct.RebindEpisode(e, top); err != nil {
						return err
					}
					if err := withOrigin(ctx, m.Node, func() error { return publishIdentity(ctx, e) }); err != nil {
						return err
					}
					changed = true
					v = newView(ctx)
					continue
				}
				if err := withOrigin(ctx, m.Node, func() error { return register(ctx, to, m.Into, occ) }); err != nil {
					return err
				}
				if err := mergeEpisode(ctx, m.Into, e, to, m.Node); err != nil {
					return err
				}
				changed = true
				v = newView(ctx)
			}
		}
	}
	return nil
}

// walkMerges: through each podcast merge of a chain, the episode a reference
// to a merged-away podcast reaches at each survivor is registered (if new)
// and the previous one redirected to it, whether or not the merge had seen
// the episode.
func walkMerges(ctx *protocol.ApplyContext, e, p string, occ map[string]any, node string) error {
	o, kfam := occurrenceOf(occ), guidKind(occ)
	seen := map[string]bool{}
	for !seen[p] {
		v := newView(ctx)
		m := v.podcastMerge(p)
		if m == nil {
			return v.err
		}
		seen[p] = true
		next := v.mergeEpisodes(m)[e]
		if next == "" {
			next = v.derive(m.Into, o, kfam, false)
		}
		if v.err != nil {
			return v.err
		}
		if next == "" || next == e {
			return nil
		}
		if err := register(ctx, next, m.Into, occ); err != nil {
			return err
		}
		if v.root(e) == e {
			if err := mergeEpisode(ctx, m.Into, e, next, node); err != nil {
				return err
			}
			if err := placeAll(ctx, node, m.Into); err != nil {
				return err
			}
		}
		e, p = next, m.Into
	}
	return nil
}

// ---- GUID changes (PROTOCOL §9.7)

// fetchItem is one item of a server fetch with its identity evidence.
type fetchItem struct {
	it   ServerItem
	o    occurrence
	kfam string
	ok   bool // it has a GUID or an enclosure
}

func fetchItems(items []ServerItem) []fetchItem {
	out := make([]fetchItem, len(items))
	for i, it := range items {
		o := occurrenceOf(it.Occurrence)
		out[i] = fetchItem{it: it, o: o, kfam: guidKind(it.Occurrence), ok: o.guid != "" || o.enclosure != ""}
	}
	return out
}

// guidChanges applies PROTOCOL §3.3 GUID-change rule to a complete successful
// server fetch (PROTOCOL §9.7). Old candidates are the roots the server's earlier
// fetches saw that no item of this fetch resolves to; new candidates are the
// items with a GUID no other item has that resolve to an episode new to that
// history. A pair matches on identical non-empty titles and identical publication
// seconds (an old root's title and date are its canonical metadata's); only a
// one-to-one match across both whole sets counts. Each match is:
//
// episode.guid_change (version 1), subject guid:<P>:<guid>: inputs
// {podcast_id, refresh_id, kind (the new GUID's kind)}; result {guid,
// episode_id (the old root)}. Dependencies: the podcast's creating fact. The
// new GUID becomes a lookup alias of the old root.
//
// episode.merge (version 1), subject merge:<from>, when the new item's
// episode is registered: inputs {podcast_id, cause: guid_change, by (the
// guid_change)}; result {from, into}. Dependencies: the guid_change.
func guidChanges(ctx *protocol.ApplyContext, x ServerFetch, items []fetchItem, creating string, before func() error) error {
	acct := ctx.Account
	p := x.PodcastID
	v := newView(ctx)
	seenList, err := acct.ServerSeenEpisodes(p)
	if err != nil {
		return err
	}
	seenRoots := map[string]bool{}
	for _, e := range seenList {
		seenRoots[v.root(e)] = true
	}
	pre := make([]string, len(items))
	resolved := map[string]bool{}
	for i, it := range items {
		if it.ok {
			pre[i] = v.derive(p, it.o, it.kfam, true)
		}
		if pre[i] != "" {
			resolved[pre[i]] = true
		}
	}
	if v.err != nil {
		return v.err
	}
	var old []string
	for e := range seenRoots {
		if !resolved[e] {
			old = append(old, e)
		}
	}
	sort.Strings(old)
	// A new GUID several items of the fetch share is a reused GUID, not a
	// GUID change: it is no candidate, and its items become its releases (reference CHOICE).
	shared := map[string]int{}
	for _, it := range items {
		if it.o.guid != "" {
			shared[it.o.guid]++
		}
	}
	var news []int
	for i, it := range items {
		if it.o.guid != "" && shared[it.o.guid] == 1 && (pre[i] == "" || !seenRoots[pre[i]]) {
			news = append(news, i)
		}
	}
	if len(news) == 0 || len(old) == 0 {
		return nil
	}
	type tkey struct {
		title string
		pub   *int64
	}
	same := func(a, b tkey) bool {
		return a.title != "" && a.title == b.title && a.pub != nil && b.pub != nil && floorDiv(*a.pub, 1000) == floorDiv(*b.pub, 1000)
	}
	ok := map[string]tkey{}
	for _, e := range old {
		meta, err := recordValue(acct, "episode", e, "metadata")
		if err != nil {
			return err
		}
		var k tkey
		if meta != nil {
			k.title, _ = meta["title"].(string)
			if t, isInt := integer(meta["pub_date"]); isInt {
				k.pub = &t
			}
		}
		ok[e] = k
	}
	nk := map[int]tkey{}
	for _, i := range news {
		var k tkey
		k.title, _ = str(items[i].it.Fields, "title")
		if t, isInt := integer(items[i].it.Fields["pub_date"]); isInt {
			k.pub = &t
		}
		nk[i] = k
	}
	mn := map[int][]string{}
	mo := map[string][]int{}
	for _, i := range news {
		for _, e := range old {
			if same(nk[i], ok[e]) {
				mn[i] = append(mn[i], e)
				mo[e] = append(mo[e], i)
			}
		}
	}
	for _, i := range news {
		if len(mn[i]) != 1 || len(mo[mn[i][0]]) != 1 {
			continue
		}
		eOld := mn[i][0]
		g, kfam := items[i].o.guid, items[i].kfam
		eNew := pre[i]
		exists := false
		if eNew != "" && eNew != eOld {
			if bound, err := acct.EpisodePodcast(eNew); err != nil {
				return err
			} else {
				exists = bound != ""
			}
		}
		if err := before(); err != nil {
			return err
		}
		gc, err := ctx.Decide(protocol.DecisionInput{Type: "episode.guid_change", Version: 1, Subject: "guid:" + p + ":" + g,
			Dependencies: []string{creating}, Inputs: map[string]any{"podcast_id": p, "refresh_id": x.RefreshID, "kind": kfam},
			Result: map[string]any{"guid": g, "episode_id": eOld}})
		if err != nil {
			return err
		}
		gcID := gc["decision_id"].(string)
		if err := admitGUIDChange(ctx, p, g, eOld, gcID); err != nil {
			return err
		}
		if exists {
			d, err := ctx.Decide(protocol.DecisionInput{Type: "episode.merge", Version: 1, Subject: "merge:" + eNew,
				Dependencies: []string{gcID}, Inputs: map[string]any{"podcast_id": p, "cause": "guid_change", "by": gcID},
				Result: map[string]any{"from": eNew, "into": eOld}})
			if err != nil {
				return err
			}
			node := d["decision_id"].(string)
			if err := mergeEpisode(ctx, p, eNew, eOld, node); err != nil {
				return err
			}
			if err := placeAll(ctx, node, p); err != nil {
				return err
			}
			if err := publishIdentityRules(ctx, node, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// admitGUIDChange: the new GUID becomes a lookup alias of the old root in the
// podcast's merge survivor; it merges nothing (an alias routes).
func admitGUIDChange(ctx *protocol.ApplyContext, p, g, eOld, node string) error {
	top := newView(ctx).podcastRoot(p)
	if _, err := putAlias(ctx, top, "guid", g, eOld, node, ""); err != nil {
		return err
	}
	if err := settleMerged(ctx); err != nil {
		return err
	}
	if err := placeAll(ctx, node, p); err != nil {
		return err
	}
	return publishIdentityRules(ctx, node, p)
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// ---- podcast merges (PROTOCOL §9.7)

// episodeMapping is one episode of a merged-away podcast and the survivor
// episode its occurrence derives to (a podcast.merge result's episodes).
type episodeMapping struct{ from, to string }

// applyPodcastMerge applies a podcast.merge decision (made here, or admitted
// from the archive): the source's live generation fromGen is rehomed as the
// survivor's or joined into the survivor's intoGen; the source keeps only its
// subscription record; its aliases, lookup aliases, evidence and catalog
// items join the end of the survivor's merge chain; every mapped episode is
// merged into its survivor episode and every other registered source
// episode settles where the survivor's rules derive it.
func applyPodcastMerge(ctx *protocol.ApplyContext, node, source, survivor, fromGen, intoGen string, rehome bool, maps []episodeMapping) error {
	acct := ctx.Account
	return withOrigin(ctx, node, func() error {
		if rehome {
			// The source's live generation becomes the survivor's (PROTOCOL §3.3), with its
			// subscription state, policy and credentials.
			if err := acct.RehomeGeneration(fromGen, survivor); err != nil {
				return err
			}
			rows, err := acct.Contributions(protocol.PodcastScope(source))
			if err != nil {
				return err
			}
			for _, r := range rows {
				e, err := decodeEffect(r.Effect)
				if err != nil {
					return err
				}
				if e.Generation == fromGen {
					if err := moveContribution(acct, protocol.PodcastScope(source), protocol.PodcastScope(survivor), r.NodeID, r.Effect, ""); err != nil {
						return err
					}
				}
			}
		}
		if err := acct.AddPodcastMerge(storage.PodcastMerge{From: source, Into: survivor, FromGeneration: fromGen, IntoGeneration: intoGen, Node: node}); err != nil {
			return err
		}
		// A merged-away podcast keeps only its subscription record: its catalog
		// records go, and so do the observations that would fold them again.
		rows, err := acct.Contributions(protocol.PodcastScope(source))
		if err != nil {
			return err
		}
		for _, r := range rows {
			e, err := decodeEffect(r.Effect)
			if err != nil {
				return err
			}
			if projection.IsCatalogKind(e.Kind) {
				if err := acct.RemoveContribution(protocol.PodcastScope(source), r.NodeID); err != nil {
					return err
				}
			}
		}
		for _, g := range []string{"identity", "metadata", "refresh_status"} {
			if err := ctx.TombstoneRecord("podcast", source, g); err != nil {
				return err
			}
		}
		top := newView(ctx).podcastRoot(survivor)
		if err := acct.MoveAliases(source, top); err != nil {
			return err
		}
		for _, m := range maps {
			raw, err := acct.EpisodeOccurrence(m.from)
			if err != nil {
				return err
			}
			occV, _ := contract.DecodeJSON([]byte(raw))
			occ, _ := object(occV)
			if err := register(ctx, m.to, survivor, occ); err != nil {
				return err
			}
			if err := mergeEpisode(ctx, survivor, m.from, m.to, node); err != nil {
				return err
			}
		}
		if err := settleMerged(ctx); err != nil {
			return err
		}
		// The source's lookup aliases and evidence join the survivor's.
		v := newView(ctx)
		srcAliases, err := acct.LookupAliases(source)
		if err != nil {
			return err
		}
		for _, al := range srcAliases {
			if _, err := putAlias(ctx, top, al.Kind, al.Value, v.root(al.EpisodeID), al.Node, al.Src); err != nil {
				return err
			}
		}
		srcEv, err := acct.Occurrences(source)
		if err != nil {
			return err
		}
		for _, x := range srcEv {
			if x.EpisodeID != "" {
				x.EpisodeID = v.root(x.EpisodeID)
			}
			if added, err := acct.AddOccurrence(top, x); err != nil {
				return err
			} else if !added {
				// An observation's witness wins over a fetch's, as when it was
				// recorded (observeEvidence).
				if err := acct.ClaimOccurrence(top, x); err != nil {
					return err
				}
			}
		}
		if v.err != nil {
			return v.err
		}
		if err := acct.MoveCatalogItems(source, top); err != nil {
			return err
		}
		if err := placeAllNow(ctx, []string{source, survivor}); err != nil {
			return err
		}
		// Every merged-away podcast's record names the end of its chain.
		merged, err := acct.MergedPodcasts()
		if err != nil {
			return err
		}
		for _, p := range merged {
			ctx.MarkDirty(protocol.PodcastScope(p))
		}
		ctx.MarkDirty(protocol.PodcastScope(survivor))
		return publishIdentityRules(ctx, node, source, survivor)
	})
}

// mergePodcasts merges the later-created of two podcasts into the earlier
// (PROTOCOL §9.7, §3.3, §7), at the end of the fetch of p that landed
// on url, an alias of q:
//
// podcast.merge (version 1), subject podcast-merge:<source>: inputs {source,
// into, refresh_id, url}; result {source, into, aliases (normalized, moved),
// generations: [{generation_id, mode (joined|rehomed|closed), into}],
// episodes: [{from, to}] (every source root its occurrence derives to under
// the survivor's rules), properties (survivor|source)}. Dependencies: the
// fetch's part 0, both podcasts' creating facts and the opening facts of the
// live generations it joins or rehomes. Then the source's evidence joins the
// survivor's, disputes found then being occurrence mappings of this commit.
func mergePodcasts(ctx *protocol.ApplyContext, a, b, part0, refreshID, landing string) error {
	acct := ctx.Account
	rankOf := func(p string) (int64, error) {
		op, err := acct.PodcastCreatingOp(p)
		if err != nil {
			return 0, err
		}
		return acct.NodeRank(op)
	}
	ra, err := rankOf(a)
	if err != nil {
		return err
	}
	rb, err := rankOf(b)
	if err != nil {
		return err
	}
	survivor, source := a, b
	if rb < ra {
		survivor, source = b, a
	}
	return mergePodcastInto(ctx, survivor, source, []string{part0},
		map[string]any{"source": source, "into": survivor, "refresh_id": refreshID, "url": landing})
}

// mergePodcastInto makes and applies the podcast.merge of source into
// survivor (PROTOCOL §9.7): dependencies extra (a fetch's part 0) and both
// podcasts' creating nodes and live generations' opening facts; inputs as
// given (a fetch's {source, into, refresh_id, url}; recovery's unification {source, into, cause:
// recovered}).
func mergePodcastInto(ctx *protocol.ApplyContext, survivor, source string, extra []string, inputs map[string]any) error {
	acct := ctx.Account
	if _, merged, err := acct.PodcastMergeStep(source); err != nil || merged {
		return err
	}
	fold := func(p string) (projection.PodcastState, error) {
		rows, err := acct.Contributions(protocol.PodcastScope(p))
		if err != nil {
			return projection.PodcastState{}, err
		}
		var cs []projection.Contribution
		for _, r := range rows {
			e, err := decodeEffect(r.Effect)
			if err != nil {
				return projection.PodcastState{}, err
			}
			if !projection.IsCatalogKind(e.Kind) {
				cs = append(cs, projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e})
			}
		}
		return projection.FoldPodcast(cs), nil
	}
	sv, err := fold(survivor)
	if err != nil {
		return err
	}
	src, err := fold(source)
	if err != nil {
		return err
	}
	fromGen, intoGen := "", ""
	if src.Live != nil {
		fromGen, intoGen = src.Live.ID, src.Live.ID
		if sv.Live != nil {
			intoGen = sv.Live.ID
		}
	}
	// Episode mappings under the survivor's rules (PROTOCOL §9.7).
	v := newView(ctx)
	episodes, err := acct.EpisodesOfPodcast(source)
	if err != nil {
		return err
	}
	var maps []episodeMapping
	list := []any{}
	for _, e := range episodes {
		if v.root(e) != e {
			continue // already merged
		}
		raw, err := acct.EpisodeOccurrence(e)
		if err != nil {
			return err
		}
		occV, _ := contract.DecodeJSON([]byte(raw))
		occ, _ := object(occV)
		to := v.derive(survivor, occurrenceOf(occ), guidKind(occ), true)
		if to == "" {
			continue // no evidence to map by: it stays with the source
		}
		maps = append(maps, episodeMapping{e, to})
		list = append(list, map[string]any{"from": e, "to": to})
	}
	if v.err != nil {
		return v.err
	}
	// The source's evidence that joins the survivor's (detected afterwards).
	srcEv, err := acct.Occurrences(source)
	if err != nil {
		return err
	}
	var joining []storage.Occurrence
	for _, x := range srcEv {
		has, err := acct.HasOccurrence(survivor, x)
		if err != nil {
			return err
		}
		if !has {
			joining = append(joining, x)
		}
	}
	srcCreating, err := acct.PodcastCreatingOp(source)
	if err != nil {
		return err
	}
	svCreating, err := acct.PodcastCreatingOp(survivor)
	if err != nil {
		return err
	}
	deps := append(slices.Clone(extra), srcCreating, svCreating)
	for _, g := range []*projection.GenerationState{src.Live, sv.Live} {
		if g != nil {
			row, err := acct.Generation(g.ID)
			if err != nil {
				return err
			}
			deps = append(deps, row.CreatingOp)
		}
	}
	gens := []any{}
	srcGens, err := acct.Generations(source)
	if err != nil {
		return err
	}
	for _, g := range srcGens {
		switch {
		case src.Live != nil && g.ID == src.Live.ID && sv.Live != nil:
			gens = append(gens, map[string]any{"generation_id": g.ID, "mode": "joined", "into": sv.Live.ID})
		case src.Live != nil && g.ID == src.Live.ID:
			gens = append(gens, map[string]any{"generation_id": g.ID, "mode": "rehomed", "into": g.ID})
		default:
			gens = append(gens, map[string]any{"generation_id": g.ID, "mode": "closed", "into": nil})
		}
	}
	aliasRows, err := acct.Aliases(source)
	if err != nil {
		return err
	}
	aliasNames := make([]string, 0, len(aliasRows))
	for _, al := range aliasRows {
		aliasNames = append(aliasNames, al.Normalized)
	}
	sort.Strings(aliasNames)
	aliasList := []any{}
	for _, n := range aliasNames {
		aliasList = append(aliasList, n)
	}
	properties := "survivor"
	if src.Live != nil && sv.Live == nil {
		properties = "source"
	}
	d, err := ctx.Decide(protocol.DecisionInput{Type: "podcast.merge", Version: 1, Subject: "podcast-merge:" + source, Dependencies: deps,
		Inputs: inputs,
		Result: map[string]any{"source": source, "into": survivor, "aliases": aliasList, "generations": gens, "episodes": list,
			"properties": properties}})
	if err != nil {
		return err
	}
	node := d["decision_id"].(string)
	if err := applyPodcastMerge(ctx, node, source, survivor, fromGen, intoGen, fromGen != "" && sv.Live == nil, maps); err != nil {
		return err
	}
	for _, x := range joining {
		o := occurrence{guid: x.GUID, enclosure: x.Enclosure, pub: x.PubDate}
		if err := detect(ctx, survivor, o, x.Kind, node, []string{node}, nil); err != nil {
			return err
		}
	}
	return nil
}
