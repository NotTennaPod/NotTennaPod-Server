package projection

import "sort"

// A session's family: its continuations, one per effective author, and the tree
// their exported saves form (PROTOCOL §6.10). Everything here depends only on
// the set of admitted facts, never on their order, so it is computed once per
// fold.

type exportInfo struct {
	node         string
	ordinal      int64
	contribution int64
	save         SaveMeta
	legacy       bool // no save identity (a single-continuation session)
	resume       bool // an intentional resume: an explicit claim on its path (PROTOCOL §8.1)
}

type contInfo struct {
	author, parent string
	base           int64
	exports        []exportInfo // by ordinal
}

type family struct {
	start, root, baseline string
	conts                 map[string]*contInfo
	paths                 map[string][]exportInfo

	// Derived.
	parent     map[string]string // tree: save ID → parent save ID ("" for none)
	kids       map[string][]string
	contrib    map[string]int64 // save ID → contribution
	hasChild   map[string]bool
	claims     map[string]bool            // exports that claim the position at their place
	claimOf    map[string]string          // an export without a claim → the claim its path holds
	copyPoints map[string]bool            // exports of a copy point: shared history
	exporters  map[string]map[string]bool // save ID → continuations (authors) exporting it
}

func contKey(session, author string) string { return session + "\x00" + author }

// families collects every session family of a scope's contributions.
func families(cs []Contribution) map[string]*family {
	fs := map[string]*family{}
	get := func(session string) *family {
		f := fs[session]
		if f == nil {
			f = &family{conts: map[string]*contInfo{}}
			fs[session] = f
		}
		return f
	}
	cont := func(f *family, e Effect) *contInfo {
		k := f.conts[e.Author]
		if k == nil {
			k = &contInfo{author: e.Author, parent: e.Parent, base: e.Base}
			f.conts[e.Author] = k
		}
		return k
	}
	for _, c := range cs {
		e := c.Effect
		switch e.Kind {
		case KindSessionStart:
			f := get(e.Session)
			f.start, f.root, f.baseline = c.Node, e.Author, e.Baseline
			cont(f, e)
		case KindCheckpoint, KindSessionEnd:
			f := get(e.Session)
			x := exportInfo{node: c.Node, ordinal: e.Ordinal, contribution: e.Contribution, resume: e.Kind == KindCheckpoint && e.Claiming}
			if e.Save != nil {
				x.save = *e.Save
			} else {
				x.legacy = true
				x.save = SaveMeta{ID: c.Node, Index: e.Ordinal, First: c.Node}
			}
			k := cont(f, e)
			k.exports = append(k.exports, x)
		}
	}
	for _, f := range fs {
		for _, k := range f.conts {
			sort.Slice(k.exports, func(i, j int) bool { return k.exports[i].ordinal < k.exports[j].ordinal })
		}
		f.derive()
	}
	return fs
}

// path is a continuation's exports preceded by those it shares with its parent.
func (f *family) path(author string) []exportInfo {
	if p, ok := f.paths[author]; ok {
		return p
	}
	k := f.conts[author]
	if k == nil {
		return nil
	}
	f.paths[author] = nil // a lineage cycle cannot form; guard anyway
	var out []exportInfo
	if k.parent != "" {
		for _, x := range f.path(k.parent) {
			if x.ordinal <= k.base {
				out = append(out, x)
			}
		}
	}
	out = append(out, k.exports...)
	f.paths[author] = out
	return out
}

// branch returns where two paths diverge: after their last common export, if
// their first exports of new saves start at the same save, the one of the
// smaller save index (the copy's restart or tail export) is the copy point;
// otherwise the last common export. common is the last common export.
func branch(p, q []exportInfo) (point, common *exportInfo, copyPoint bool) {
	l := 0
	for l < len(p) && l < len(q) && p[l].node == q[l].node {
		l++
	}
	if l > 0 {
		common = &p[l-1]
	}
	fp, fq := firstNew(p[l:]), firstNew(q[l:])
	if fp != nil && fq != nil && !fp.legacy && !fq.legacy && fp.save.First == fq.save.First {
		if fq.save.Index < fp.save.Index {
			return fq, common, true
		}
		return fp, common, true
	}
	return common, common, false
}

func firstNew(xs []exportInfo) *exportInfo {
	for i := range xs {
		if xs[i].save.First != "" {
			return &xs[i]
		}
	}
	return nil
}

func (f *family) authors() []string {
	out := make([]string, 0, len(f.conts))
	for a := range f.conts {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

func (f *family) derive() {
	f.paths = map[string][]exportInfo{}
	f.parent, f.contrib, f.hasChild = map[string]string{}, map[string]int64{}, map[string]bool{}
	f.kids = map[string][]string{}
	f.exporters = map[string]map[string]bool{}
	for a, k := range f.conts {
		for _, x := range k.exports {
			if f.exporters[x.save.ID] == nil {
				f.exporters[x.save.ID] = map[string]bool{}
			}
			f.exporters[x.save.ID][a] = true
		}
	}
	f.claims, f.copyPoints = map[string]bool{}, map[string]bool{}
	// Intentional resumes are claims on their own writing path, with or
	// without a divergence (PROTOCOL §8.1): each later export of
	// a path that holds one, the shared part included, writes under it. A
	// resume whose save its path already covers (an earlier export of the
	// same save, of the save it names as first, or of the same or a higher
	// index) is no claim: acceptance refuses such a fact, and the fold
	// guards recovered receipts the same way.
	for a, k := range f.conts {
		p := f.path(a)
		own := len(p) - len(k.exports)
		seen := map[string]bool{}
		var top int64
		for i, x := range p {
			if i >= own && x.resume && !x.legacy && !seen[x.save.ID] && !seen[x.save.First] && x.save.Index > top {
				f.claims[x.node] = true
			}
			if !x.legacy {
				seen[x.save.ID] = true
				top = max(top, x.save.Index)
			}
		}
	}
	authors := f.authors()
	// Every path's saves, plus every branch point it shares with another path,
	// in save order: consecutive saves are parent and child.
	for _, a := range authors {
		p := f.path(a)
		type node struct {
			id    string
			index int64
		}
		seen := map[string]bool{}
		var seq []node
		add := func(x exportInfo) {
			if !seen[x.save.ID] {
				seen[x.save.ID] = true
				seq = append(seq, node{x.save.ID, x.save.Index})
				if _, ok := f.contrib[x.save.ID]; !ok {
					f.contrib[x.save.ID] = x.contribution
				}
			}
		}
		for _, x := range p {
			add(x)
		}
		for _, b := range authors {
			if b == a {
				continue
			}
			if point, _, copyPoint := branch(p, f.path(b)); point != nil && copyPoint {
				add(*point)
			}
		}
		sort.SliceStable(seq, func(i, j int) bool { return seq[i].index < seq[j].index })
		for i, n := range seq {
			if _, set := f.parent[n.id]; set {
				continue
			}
			if i > 0 {
				f.parent[n.id] = seq[i-1].id
				f.hasChild[seq[i-1].id] = true
				f.kids[seq[i-1].id] = append(f.kids[seq[i-1].id], n.id)
			} else {
				f.parent[n.id] = ""
			}
		}
	}
	if len(f.conts) < 2 {
		f.deriveClaimOf()
		return
	}
	// Copy points and claims (PROTOCOL §8.5 Ownership).
	for _, a := range authors {
		k := f.conts[a]
		var point *exportInfo
		if k.parent != "" {
			var copyPoint bool
			point, _, copyPoint = branch(f.path(k.parent), f.path(a))
			if copyPoint {
				f.markCopyPoint(f.path(k.parent), f.path(a), point)
			}
		} else {
			// The root diverges at the earliest branch point with its children
			// (PROTOCOL §8.5.5). A child sharing no export with it diverged at the start,
			// before every export.
			for _, b := range authors {
				if f.conts[b].parent != a {
					continue
				}
				p, _, _ := branch(f.path(a), f.path(b))
				if p == nil {
					point = nil
					break
				}
				if point == nil || p.save.Index < point.save.Index {
					point = p
				}
			}
		}
		witnessed := map[string]bool{f.baseline: true}
		limit := int64(0)
		if point != nil {
			limit = point.save.Index
			witnessed[point.save.Baseline] = true
		}
		for _, x := range f.path(a) {
			if x.save.Index <= limit {
				witnessed[x.save.Baseline] = true
			}
		}
		claimed := map[string]bool{}
		for _, x := range k.exports {
			if x.legacy || x.save.Index <= limit || x.save.Passive || x.save.First == "" {
				continue
			}
			if witnessed[x.save.Baseline] || claimed[x.save.Baseline] {
				continue
			}
			claimed[x.save.Baseline] = true
			f.claims[x.node] = true
		}
	}
	f.deriveClaimOf()
}

// deriveClaimOf records, for every export that does not claim, the claim its
// continuation writes under: the latest claiming export before it on its path,
// the shared part included, else the session's start (PROTOCOL §8.5.11).
func (f *family) deriveClaimOf() {
	f.claimOf = map[string]string{}
	for a, k := range f.conts {
		p := f.path(a)
		own := len(p) - len(k.exports)
		cur := f.start
		for i, x := range p {
			if f.claims[x.node] {
				cur = x.node
			} else if i >= own {
				f.claimOf[x.node] = cur
			}
		}
	}
}

// history is a finalizer's writing history in the family: its own path, or for
// a copied finalizer without exports of its own, its target's path up to the
// ordinal the two share.
func (f *family) history(author, target string, shared int64) []exportInfo {
	if _, ok := f.conts[author]; ok {
		return f.path(author)
	}
	var out []exportInfo
	for _, x := range f.path(target) {
		if x.ordinal <= shared {
			out = append(out, x)
		}
	}
	return out
}

// markCopyPoint marks both paths' exports of the copy point's save.
func (f *family) markCopyPoint(p, q []exportInfo, point *exportInfo) {
	for _, xs := range [][]exportInfo{p, q} {
		for _, x := range xs {
			if x.save.ID == point.save.ID {
				f.copyPoints[x.node] = true
			}
		}
	}
}

// exportAt resolves an ordinal named from a continuation's point of view: its
// own above its base, otherwise its parent's.
func (f *family) exportAt(author string, ordinal int64) *exportInfo {
	for n := 0; n < 1024; n++ {
		k := f.conts[author]
		if k == nil {
			return nil
		}
		if ordinal > k.base || k.parent == "" {
			for i := range k.exports {
				if k.exports[i].ordinal == ordinal {
					return &k.exports[i]
				}
			}
			return nil
		}
		author = k.parent
	}
	return nil
}

// eligible reports whether a finalization still stands in a family of several
// continuations: no export of another continuation descends from the save it
// names (PROTOCOL §8.5, as in the independent reference, for every finalization, not
// only copied ones, so the outcome does not depend on which library kept the
// author).
func (f *family) eligible(finalizer, target string, ordinal int64) bool {
	if len(f.conts) < 2 && finalizer == f.root {
		return true // a single continuation keeps the single-path rule
	}
	x := f.exportAt(target, ordinal)
	if x == nil {
		return true
	}
	stack := append([]string(nil), f.kids[x.save.ID]...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for a := range f.exporters[id] {
			if a != finalizer {
				return false
			}
		}
		stack = append(stack, f.kids[id]...)
	}
	return true
}

// playedTime is the tree's edge sum over saves not covered by a statistics
// reset, at least zero (PROTOCOL §8.5). cuts maps a continuation's author to its greatest
// covered ordinal.
func (f *family) playedTime(cuts map[string]int64) int64 {
	covered := map[string]bool{}
	for author, ordinal := range cuts {
		x := f.exportAt(author, ordinal)
		if x == nil {
			continue
		}
		for id := x.save.ID; id != "" && !covered[id]; id = f.parent[id] {
			covered[id] = true
		}
	}
	var total int64
	for id, c := range f.contrib {
		if covered[id] {
			continue
		}
		total += c - f.contrib[f.parent[id]]
	}
	return max(total, 0)
}
