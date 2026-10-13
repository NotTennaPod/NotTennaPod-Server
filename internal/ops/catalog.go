package ops

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Catalog observations, discovery proposals and the server's own fetches
// (PROTOCOL §9.4).

// RegisterCatalog adds PROTOCOL §9.4 operation types (version 1).
func RegisterCatalog(r *protocol.Registry) {
	r.Register("catalog.observe", 1, observeOp{})
	r.Register("discovery.propose", 1, proposeOp{})
}

// The canonical metadata record fields (PROTOCOL §9.4).
var (
	podcastMetadataFields = []string{"title", "description", "author", "language", "link", "image_url", "type", "funding", "paged", "next_page_url"}
	episodeMetadataFields = []string{"title", "description", "link", "pub_date", "image_url", "payment_link", "media", "chapters",
		"chapter_url", "transcript_url", "transcript_type", "social_interact_url"}
)

// ---- catalog.observe (PROTOCOL §9.4)

type observeOp struct{}

func (observeOp) Capability() string { return "" }

type observedRecord struct {
	ref    EpisodeRef // items only (no generation)
	base   string
	fields map[string]any
}

type catalogObservation struct {
	podcast   string
	fetchTime int64
	feed      *observedRecord
	items     []observedRecord
}

func parseObservation(f *protocol.Fact) (catalogObservation, error) {
	var o catalogObservation
	if err := fields(f.Payload, []string{"podcast_id", "batch_id", "parser_version", "fetch_time", "items"}, "feed"); err != nil {
		return o, err
	}
	o.podcast, _ = str(f.Payload, "podcast_id")
	batch, _ := str(f.Payload, "batch_id")
	parser, _ := str(f.Payload, "parser_version")
	t, ok := integer(f.Payload["fetch_time"])
	if !prefixedHex(o.podcast, "p1:") || !contract.ValidUUID(batch) || parser == "" || !ok {
		return o, invalidf("bad podcast_id, batch_id, parser_version or fetch_time")
	}
	o.fetchTime = t
	targets := []protocol.Target{{Entity: "podcast", ID: o.podcast}}
	if v, present := f.Payload["feed"]; present {
		r, err := parseObservedRecord(v, "", validFeedFields)
		if err != nil {
			return o, err
		}
		o.feed = &r
	}
	list, ok := f.Payload["items"].([]any)
	if !ok {
		return o, invalidf("items must be an array")
	}
	seen := map[string]bool{}
	for _, x := range list {
		r, err := parseObservedRecord(x, o.podcast, validItemFields)
		if err != nil {
			return o, err
		}
		if seen[r.ref.EpisodeID] {
			return o, invalidf("an episode observed twice")
		}
		seen[r.ref.EpisodeID] = true
		o.items = append(o.items, r)
		targets = append(targets, protocol.Target{Entity: "episode", ID: r.ref.EpisodeID})
	}
	if o.feed == nil && len(o.items) == 0 {
		return o, invalidf("an observation needs a feed or items")
	}
	return o, checkTargets(f, targets)
}

// itemGeneration stands in for the generation an episode reference requires:
// observations are catalog evidence of no generation.
const itemGeneration = "g1:0000000000000000000000000000000000000000000000000000000000000000"

// parseObservedRecord reads {base?, fields} (the feed) or {episode, base?,
// fields} (an item, when podcast is its observation's podcast). An item's
// episode is a whole EpisodeRef naming that podcast; its generation is
// well-formed but not used, as observations are catalog evidence of no
// generation (PROTOCOL §9.4). A null base is an absent one.
func parseObservedRecord(v any, podcast string, valid func(map[string]any) error) (observedRecord, error) {
	var r observedRecord
	obj, ok := object(v)
	if !ok {
		return r, invalidf("an observed record must be an object")
	}
	item := podcast != ""
	required := []string{"fields"}
	if item {
		required = append(required, "episode")
	}
	if err := fields(obj, required, "base"); err != nil {
		return r, err
	}
	if b := obj["base"]; b != nil {
		if r.base, ok = b.(string); !ok || !contract.ValidUUID(r.base) {
			return r, invalidf("base must be a node ID or null")
		}
	}
	if r.fields, ok = object(obj["fields"]); !ok {
		return r, invalidf("fields must be an object")
	}
	if err := valid(r.fields); err != nil {
		return r, err
	}
	if item {
		ref, err := parseEpisodeRef(obj["episode"], "")
		if err != nil {
			return r, err
		}
		if ref.PodcastID != podcast {
			return r, invalidf("an observed item names another podcast")
		}
		ref.GenerationID = itemGeneration
		r.ref = ref
	}
	return r, nil
}

// validFeedFields checks a podcast metadata record value (PROTOCOL §9.4): exactly its
// fields, text and URLs as strings or null, paged a boolean, funding an array
// of {url, content}.
func validFeedFields(v map[string]any) error {
	if err := fields(v, podcastMetadataFields); err != nil {
		return err
	}
	for _, k := range []string{"title", "description", "author", "language", "link", "image_url", "type", "next_page_url"} {
		if !optString(v[k]) {
			return invalidf("feed %s must be a string or null", k)
		}
	}
	if _, ok := v["paged"].(bool); !ok {
		return invalidf("feed paged must be a boolean")
	}
	list, ok := v["funding"].([]any)
	if !ok {
		return invalidf("feed funding must be an array")
	}
	for _, x := range list {
		m, ok := object(x)
		if !ok || fields(m, []string{"url", "content"}) != nil || !optString(m["url"]) || !optString(m["content"]) {
			return invalidf("bad funding entry")
		}
	}
	return nil
}

// validItemFields checks an episode metadata record value (PROTOCOL §9.4): exactly its
// fields; pub_date an integer or null; media null or {url, size (decimal
// string or null), mime_type, duration (integer >= 0)}; chapters null or
// [{start (decimal string), title, link, image_url}].
func validItemFields(v map[string]any) error {
	if err := fields(v, episodeMetadataFields); err != nil {
		return err
	}
	for _, k := range []string{"title", "description", "link", "image_url", "payment_link", "chapter_url", "transcript_url",
		"transcript_type", "social_interact_url"} {
		if !optString(v[k]) {
			return invalidf("item %s must be a string or null", k)
		}
	}
	if d := v["pub_date"]; d != nil {
		if _, ok := integer(d); !ok {
			return invalidf("item pub_date must be an integer or null")
		}
	}
	if m := v["media"]; m != nil {
		obj, ok := object(m)
		if !ok || fields(obj, []string{"url", "size", "mime_type", "duration"}) != nil {
			return invalidf("bad item media")
		}
		if _, ok := obj["url"].(string); !ok {
			return invalidf("item media url must be a string")
		}
		if size := obj["size"]; size != nil && !decimalString(size) {
			return invalidf("item media size must be a decimal string or null")
		}
		if !optString(obj["mime_type"]) {
			return invalidf("item media mime_type must be a string or null")
		}
		if n, ok := integer(obj["duration"]); !ok || n < 0 {
			return invalidf("item media duration must be an integer >= 0")
		}
	}
	if ch := v["chapters"]; ch != nil {
		list, ok := ch.([]any)
		if !ok {
			return invalidf("item chapters must be an array or null")
		}
		for _, x := range list {
			c, ok := object(x)
			if !ok || fields(c, []string{"start", "title", "link", "image_url"}) != nil || !decimalString(c["start"]) ||
				!optString(c["title"]) || !optString(c["link"]) || !optString(c["image_url"]) {
				return invalidf("bad chapter")
			}
		}
	}
	return nil
}

func optString(v any) bool {
	if v == nil {
		return true
	}
	_, ok := v.(string)
	return ok
}

// decimalString: the parser's canonical form writes Java longs a feed can make
// arbitrarily large as decimal strings.
func decimalString(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func (observeOp) Validate(f *protocol.Fact) error {
	_, err := parseObservation(f)
	return err
}

func (observeOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	o, err := parseObservation(f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	acct := ctx.Account
	if exists, err := acct.PodcastExists(o.podcast); err != nil || !exists {
		return protocol.Outcome{Result: "not_applied", Reason: "unknown_podcast"}, err
	}
	// A device's fetch time never passes its fact's normalized time (PROTOCOL §9.4).
	fetchTime := min(o.fetchTime, ctx.NormalizedTime)
	contributed := 0
	firstReason := ""
	// An observation naming a merged-away podcast applies its items to the
	// survivor's episodes; its feed record stays with the podcast it names,
	// hidden (a merged-away podcast keeps only its subscription record).
	pr := newView(ctx).podcastRoot(o.podcast)
	newEvidence := false
	if o.feed != nil {
		if pr == o.podcast {
			if err := ctx.Contribute(protocol.PodcastScope(o.podcast), observationEffect(*o.feed, fetchTime)); err != nil {
				return protocol.Outcome{}, err
			}
		}
		contributed++
	}
	// Two passes (reference CHOICE): first every item's identity evidence
	// and the occurrence mappings it makes, then the items' effects, which see
	// every mapping the observation made.
	type pass struct {
		it     observedRecord
		ev     occurrence
		kfam   string
		counts bool
	}
	var passing []pass
	// e0 is where an item's named episode goes before routing: its root, or
	// for a merged-away podcast its merge mapping or the survivor's derivation.
	e0Of := func(v *idView, e string, ev occurrence, kfam string) string {
		e0 := v.root(e)
		if pr != o.podcast && e0 == e {
			m := v.podcastMerge(o.podcast)
			if to := v.mergeEpisodes(m)[e]; to != "" {
				e0 = to
			} else if d := v.derive(pr, ev, kfam, true); d != "" {
				e0 = d
			}
		}
		return e0
	}
	for _, it := range o.items {
		it.ref.PodcastID = o.podcast
		e := it.ref.EpisodeID
		if known, err := acct.EpisodePodcast(e); err != nil {
			return protocol.Outcome{}, err
		} else if v := newView(ctx); known != "" && known != o.podcast && !v.sameFamily(known, o.podcast) {
			if v.err != nil {
				return protocol.Outcome{}, v.err
			}
			if firstReason == "" {
				firstReason = "episode_podcast_mismatch"
			}
			continue // the episode belongs to another podcast
		}
		// PROTOCOL §9.7: the item's identity evidence is the observed record: its
		// reference's GUID with the observed enclosure and publication date. It
		// counts only for the plain derivation of that occurrence or the episode
		// the server's rules give it.
		ev := observedOccurrence(it.ref.Occurrence, it.fields)
		kfam := guidKind(it.ref.Occurrence)
		v := newView(ctx)
		e0 := e0Of(v, e, ev, kfam)
		counts := v.plainIDs(pr, ev, kfam)[e0] || e0 == v.derive(pr, ev, kfam, true)
		if v.err != nil {
			return protocol.Outcome{}, v.err
		}
		if counts {
			if isNew, err := observeEvidence(ctx, pr, ev, f.OpID, kfam); err != nil {
				return protocol.Outcome{}, err
			} else if isNew {
				newEvidence = true
				if err := detect(ctx, pr, ev, kfam, f.OpID, []string{f.OpID}, nil); err != nil {
					return protocol.Outcome{}, err
				}
			}
		}
		passing = append(passing, pass{it, ev, kfam, counts})
	}
	aliased := false
	for _, x := range passing {
		ref, e := x.it.ref, x.it.ref.EpisodeID
		v := newView(ctx)
		er := v.routeIdentity(pr, v.root(e0Of(v, e, x.ev, x.kfam)), x.ev, x.kfam)
		if v.err != nil {
			return protocol.Outcome{}, v.err
		}
		if x.counts {
			if err := setEvidenceEpisode(ctx, pr, x.ev, er); err != nil {
				return protocol.Outcome{}, err
			}
		}
		regs := [][2]string{{e, o.podcast}, {er, pr}}
		if e == er {
			regs = regs[1:] // where it goes: the routed podcast's
		}
		for _, r := range regs {
			a, err := registerEpisode(ctx, r[0], r[1], ref.Occurrence)
			if err != nil {
				return protocol.Outcome{}, err
			}
			aliased = aliased || a
		}
		if pr != o.podcast {
			if err := walkMerges(ctx, e, o.podcast, ref.Occurrence, f.OpID); err != nil {
				return protocol.Outcome{}, err
			}
		}
		eff := observationEffect(x.it, fetchTime)
		eff.Ref = observedRef(e, o.podcast, x.it.ref.Occurrence, x.it.fields)
		if er != e {
			eff.Member = e
		}
		if err := contributeBeside(ctx, f.OpID, protocol.ItemScope(er), eff); err != nil {
			return protocol.Outcome{}, err
		}
		rref := ref
		rref.EpisodeID, rref.PodcastID = er, pr
		if _, err := noteCatalogItem(ctx, rref, x.it.fields, f.OpID, ctx.NormalizedTime, false); err != nil {
			return protocol.Outcome{}, err
		}
		contributed++
	}
	if aliased {
		// A registration added a lookup alias: every contribution goes where it
		// routes now.
		if err := placeAll(ctx, f.OpID, pr); err != nil {
			return protocol.Outcome{}, err
		}
	}
	if newEvidence {
		// New evidence may dispute enclosures (identity_rules).
		if err := publishIdentityRules(ctx, f.OpID, pr); err != nil {
			return protocol.Outcome{}, err
		}
	}
	if contributed == 0 {
		return protocol.Outcome{Result: "not_applied", Reason: firstReason}, nil
	}
	return ctx.SettleLater(), nil
}

// observedWithoutMedia reports whether the observation node (a device's
// catalog.observe or a server catalog.refresh part) saw the episode without
// media: the evidence a gained-media proposal must name (PROTOCOL §9.4).
func observedWithoutMedia(ctx *protocol.ApplyContext, node, episode string) (bool, error) {
	// The observation's record of the episode where it lives now (an item routed to another
	// episode, through a lookup alias or a release).
	if raw, ok, err := ctx.Account.Contribution(protocol.ItemScope(episode), node); err != nil {
		return false, err
	} else if ok {
		e, err := decodeEffect(raw)
		if err != nil {
			return false, err
		}
		for _, x := range parts(e) {
			if x.Kind == projection.KindObservation && x.Observed != nil && x.Observed["media"] == nil {
				return true, nil
			}
		}
	}
	var items []any
	if row, err := ctx.Account.Fact(node); err == nil {
		if row.Type != "catalog.observe" || row.Outcome == "rejected" {
			return false, nil
		}
		v, err := contract.DecodeJSON(row.Fact)
		if err != nil {
			return false, err
		}
		fact, _ := object(v)
		payload, _ := object(fact["payload"])
		items, _ = payload["items"].([]any)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return false, err
	} else {
		raw, err := ctx.Account.DecisionBytes(node)
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		v, err := contract.DecodeJSON(raw)
		if err != nil {
			return false, err
		}
		d, _ := object(v)
		if d["decision_type"] != "catalog.refresh" {
			return false, nil
		}
		result, _ := object(d["result"])
		items, _ = result["items"].([]any)
	}
	for _, x := range items {
		it, _ := object(x)
		id, _ := str(it, "episode_id")
		if e, ok := object(it["episode"]); ok {
			id, _ = str(e, "episode_id")
		}
		if id == "" {
			continue
		}
		root, err := ctx.Account.MergeRoot(id)
		if err != nil {
			return false, err
		}
		if fields, ok := object(it["fields"]); ok && root == episode && fields["media"] == nil {
			return true, nil
		}
	}
	return false, nil
}

// contributeBeside adds a node's effect to a scope beside the node's effect
// already there: two items of one observation that route to one episode are a
// compound, as a merge makes them (PROTOCOL §9.7), and the fold applies them in
// the compound's order. Before, the later item replaced the earlier.
func contributeBeside(ctx *protocol.ApplyContext, node, scope string, e projection.Effect) error {
	if raw, ok, err := ctx.Account.Contribution(scope, node); err != nil {
		return err
	} else if ok {
		x, err := decodeEffect(raw)
		if err != nil {
			return err
		}
		ep, _ := scopeEpisode(scope)
		e = projection.ComposeAt(ep, x, e)
	}
	return ctx.ContributeAs(node, scope, e)
}

// observedOccurrence is an observed item's identity evidence (PROTOCOL §9.7): its
// reference's GUID with its record's enclosure and publication date.
func observedOccurrence(occ, fields map[string]any) occurrence {
	ev := occurrenceOf(occ)
	ev.enclosure, ev.pub = "", nil
	if m, ok := object(fields["media"]); ok {
		if u, ok := str(m, "url"); ok && u != "" {
			ev.enclosure = contract.NormalizeURL(u)
		}
	}
	if t, ok := integer(fields["pub_date"]); ok {
		ev.pub = &t
	}
	return ev
}

// observedRef is the reference an item observation's effect is routed by
// (PROTOCOL §9.7): the episode and podcast it names, with the item's evidence. A
// device's observation and a server fetch's part carry it alike, so when a
// later first dispute splits a GUID's evidence into releases, each
// observation's record moves to its own release.
func observedRef(episode, podcast string, occ, fields map[string]any) *projection.Ref {
	ev := observedOccurrence(occ, fields)
	ref := &projection.Ref{Episode: episode, Podcast: podcast, GUID: ev.guid, Enclosure: ev.enclosure, Pub: ev.pub}
	if guidKind(occ) == contract.KindAtomID {
		ref.Kind = contract.KindAtomID
	}
	return ref
}

func observationEffect(r observedRecord, fetchTime int64) projection.Effect {
	return projection.Effect{Kind: projection.KindObservation, Observed: r.fields, RevisionBase: r.base, FetchTime: fetchTime}
}

// noteCatalogItem registers an observed episode (with its occurrence, as any
// first fact naming one) and updates its catalog item; false when the episode
// belongs to another podcast. at is the observation's time, the effective
// publication date of an item without one (Android persists "now").
func noteCatalogItem(ctx *protocol.ApplyContext, ref EpisodeRef, fieldValues map[string]any, node string, at int64, server bool) (bool, error) {
	acct := ctx.Account
	known, err := acct.EpisodePodcast(ref.EpisodeID)
	if err != nil {
		return false, err
	}
	if known != "" && known != ref.PodcastID {
		return false, nil
	}
	if known == "" {
		occ, err := contract.Canonical(ref.Occurrence)
		if err != nil {
			return false, err
		}
		if err := acct.AddEpisode(ref.EpisodeID, ref.PodcastID, string(occ), node); err != nil {
			return false, err
		}
		if err := ctx.PublishRecord("episode", ref.EpisodeID, "identity", map[string]any{"podcast_id": ref.PodcastID, "occurrence": ref.Occurrence}); err != nil {
			return false, err
		}
	}
	it, ok, err := acct.CatalogItem(ref.EpisodeID)
	if err != nil {
		return false, err
	}
	pub, dated := integer(fieldValues["pub_date"])
	switch {
	case !ok:
		it = storage.CatalogItem{EpisodeID: ref.EpisodeID, PodcastID: ref.PodcastID, FirstNode: node, PubDate: at}
		if dated {
			it.PubDate = pub
		}
	case dated:
		it.PubDate = pub
	}
	playable := hasEnclosure(fieldValues)
	if playable && it.PlayableNode == "" {
		it.PlayableNode = node
	}
	if !playable && it.PlayableNode == "" && it.CatalogOnlyNode == "" {
		it.CatalogOnlyNode = node
	}
	if server {
		if it.ServerSeenNode == "" {
			it.ServerSeenNode, it.ServerCatalogOnly = node, !playable
		} else if playable {
			it.ServerCatalogOnly = false
		}
	}
	return true, acct.PutCatalogItem(it)
}

func hasEnclosure(fieldValues map[string]any) bool {
	m, ok := object(fieldValues["media"])
	if !ok {
		return false
	}
	u, ok := str(m, "url")
	return ok && u != ""
}

// ---- discovery.propose (PROTOCOL §9.4)

type proposeOp struct{}

func (proposeOp) Capability() string { return "" }

type proposal struct {
	ref           EpisodeRef
	add           *placement // the device's resolved filing (its bundle)
	bundlePlace   place
	pubDate       *int64
	newestBefore  *int64
	gainedFrom    string // gained_media's catalog-only observation ("" without)
	action        string
	policyNode    string
	installSource string
}

func parseProposal(f *protocol.Fact) (proposal, error) {
	var p proposal
	if err := fields(f.Payload, []string{"podcast_id", "generation_id", "episode", "batch_id", "pub_date", "newest_before_batch", "action", "policy_node"},
		"gained_media", "queue_bundle"); err != nil {
		return p, err
	}
	ref, add, at, err := startRefs("", f)
	if err != nil {
		return p, err
	}
	p.ref, p.add, p.bundlePlace = ref, add, at
	podcast, _ := str(f.Payload, "podcast_id")
	generation, _ := str(f.Payload, "generation_id")
	batch, _ := str(f.Payload, "batch_id")
	if podcast != ref.PodcastID || generation != ref.GenerationID || !contract.ValidUUID(batch) {
		return p, invalidf("podcast_id, generation_id and batch_id must match the episode")
	}
	for _, k := range []string{"pub_date", "newest_before_batch"} {
		v := f.Payload[k]
		if v == nil {
			continue
		}
		n, ok := integer(v)
		if !ok {
			return p, invalidf("%s must be an integer or null", k)
		}
		if k == "pub_date" {
			p.pubDate = &n
		} else {
			p.newestBefore = &n
		}
	}
	p.action, _ = str(f.Payload, "action")
	if p.action != "inbox" && p.action != "queue" && p.action != "nothing" {
		return p, invalidf("bad action")
	}
	if p.add != nil && p.action != "queue" {
		return p, invalidf("a queue bundle needs the queue action")
	}
	if v := f.Payload["policy_node"]; v != nil {
		if p.policyNode, _ = v.(string); !contract.ValidUUID(p.policyNode) {
			return p, invalidf("policy_node must be a node ID or null")
		}
	}
	if v, present := f.Payload["gained_media"]; present {
		g, ok := object(v)
		if !ok || fields(g, []string{"catalog_only_observation"}) != nil {
			return p, invalidf("bad gained_media")
		}
		if p.gainedFrom, _ = str(g, "catalog_only_observation"); !contract.ValidUUID(p.gainedFrom) {
			return p, invalidf("bad catalog_only_observation")
		}
	}
	p.installSource = f.AuthorID
	return p, nil
}

func (proposeOp) Validate(f *protocol.Fact) error {
	_, err := parseProposal(f)
	return err
}

// NeededCapabilities: a queue bundle is a queue write (PROTOCOL §10.1 rule 2).
func (proposeOp) NeededCapabilities(f *protocol.Fact) []string {
	if _, present := f.Payload["queue_bundle"]; present {
		return []string{"queue"}
	}
	return nil
}

func (proposeOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	p, err := parseProposal(f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	notApplied := func(reason string) (protocol.Outcome, error) {
		return protocol.Outcome{Result: "not_applied", Reason: reason}, nil
	}
	if reason, err := checkEpisode(ctx, &p.ref); err != nil || reason != "" {
		return protocol.Outcome{Result: "not_applied", Reason: reason}, err
	}
	g, err := generationState(ctx, p.ref.PodcastID, p.ref.GenerationID)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if g == nil || g.State != "subscribed" {
		return notApplied("not_subscribed")
	}
	it, known, err := ctx.Account.CatalogItem(p.ref.EpisodeID)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if !known || it.PlayableNode == "" {
		return notApplied("not_playable")
	}
	// Android's date rule, or the gained-media exception (PROTOCOL §9.4).
	eligible := p.pubDate == nil || p.newestBefore == nil || *p.pubDate >= *p.newestBefore
	if !eligible && p.gainedFrom != "" {
		if eligible, err = observedWithoutMedia(ctx, p.gainedFrom, p.ref.EpisodeID); err != nil {
			return protocol.Outcome{}, err
		}
	}
	if !eligible {
		return notApplied("ineligible")
	}
	if done, err := admitted(ctx, p.ref); err != nil || done {
		if err != nil {
			return protocol.Outcome{}, err
		}
		return notApplied("already_admitted")
	}
	// The filing acts at the proposal's place, with the proposal as origin.
	key, positionID := "", ""
	if p.add != nil {
		key, positionID = p.add.key.String(), formatUUID(p.add.positionID)
	}
	var at *place
	if p.add != nil {
		at = &p.bundlePlace
	}
	if _, err := admit(ctx, p.ref, f.OpID, p.installSource, p.action, f.OpID, key, positionID, at, ""); err != nil {
		return protocol.Outcome{}, err
	}
	return ctx.SettleLater(), nil
}

// generationState is a podcast generation's folded state (nil if unknown).
func generationState(ctx *protocol.ApplyContext, podcast, generation string) (*projection.GenerationState, error) {
	rows, err := ctx.Account.Contributions(protocol.PodcastScope(podcast))
	if err != nil {
		return nil, err
	}
	var cs []projection.Contribution
	for _, r := range rows {
		e, err := decodeEffect(r.Effect)
		if err != nil {
			return nil, err
		}
		if !projection.IsCatalogKind(e.Kind) {
			cs = append(cs, projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e})
		}
	}
	return projection.FoldPodcast(cs).Generations[generation], nil
}

func admissionSubject(ref EpisodeRef) string {
	return "admission:" + ref.GenerationID + ":" + ref.EpisodeID
}

// admitted reports whether an episode generation already has its admission:
// its own, or one of an episode merged into it, or of a generation a podcast
// merge joined into its live class (PROTOCOL §9.7: the earlier filing of a
// merged episode counts).
func admitted(ctx *protocol.ApplyContext, ref EpisodeRef) (bool, error) {
	acct := ctx.Account
	eps, err := acct.MergedInto(ref.EpisodeID)
	if err != nil {
		return false, err
	}
	eps = append([]string{ref.EpisodeID}, eps...)
	gens := []string{ref.GenerationID}
	for i := 0; i < len(gens); i++ {
		more, err := acct.GenerationsJoinedInto(gens[i])
		if err != nil {
			return false, err
		}
		gens = append(gens, more...)
	}
	for _, g := range gens {
		for _, e := range eps {
			id, err := acct.DecisionBySubject("discovery.admission", "admission:"+g+":"+e)
			if err != nil || id != "" {
				return id != "", err
			}
		}
	}
	return false, nil
}

// admit records the admission decision and the filing at place (a proposal
// fact, or the server's admission decision itself when proposalID is ""),
// with the given queue key or, for a queue filing without one, the server's
// resolution. foundBy is what found the episode: the proposal, or the
// catalog.refresh part that saw it.
//
// The decision (version 1): inputs {found_by, enqueue_location (exactly when
// the server resolved the placement), values (for the action queue: the
// canonical sort values of the filed episode and, under keep-sorted, of
// every member then in the queue, PROTOCOL §7.6)}; result {episode_id,
// generation_id, action, proposal_id (null for the server's own), source,
// key and position_id (exactly for the action queue), place (exactly when the
// server resolved it)}.
func admit(ctx *protocol.ApplyContext, ref EpisodeRef, proposalID, source, action, foundBy, key, positionID string, at *place, after string) (string, error) {
	in, err := admissionInput(ctx, ref, proposalID, source, action, foundBy, key, positionID, at, after)
	if err != nil {
		return "", err
	}
	d, err := ctx.Decide(in)
	if err != nil || d == nil {
		return "", err // a recovery rebuild admits the decision at its own position
	}
	id, _ := d["decision_id"].(string)
	return id, admitAdmission(ctx, d)
}

// admissionInput is the admission a filing asks for: in a rebuild the
// recorded one (its place, key and values were resolved against the queue
// as it was then), else the server's resolution now.
//
// at is the place a bundle resolved (nil: the server resolves it); after is
// the fetch's previous queue admission, which this one depends on so the
// fetch's filings replay in item order (the reference's reading).
func admissionInput(ctx *protocol.ApplyContext, ref EpisodeRef, proposalID, source, action, foundBy, key, positionID string, at *place,
	after string) (protocol.DecisionInput, error) {
	if ctx.Rebuilding() {
		if rec, ok := ctx.RecordedDecision("discovery.admission", admissionSubject(ref)); ok {
			return recordedInput(rec), nil
		}
	}
	inputs := map[string]any{"found_by": foundBy}
	var proposal any // null for the server's own discovery
	if proposalID != "" {
		proposal = proposalID
	}
	result := map[string]any{"episode_id": ref.EpisodeID, "generation_id": ref.GenerationID, "action": action, "proposal_id": proposal,
		"source": source}
	deps := []string{foundBy}
	if action == "queue" {
		if key == "" {
			resolved, k, u, location, err := resolvedPlacement(ctx)
			if err != nil {
				return protocol.DecisionInput{}, err
			}
			key, positionID = k.String(), formatUUID(u)
			inputs["enqueue_location"] = location
			result["place"] = resolved.value()
		} else if at != nil {
			result["place"] = at.value()
		}
		if after != "" {
			deps = append(deps, after)
		}
		// Under keep-sorted at its place a filing sorts the queue (PROTOCOL §7.6): the
		// server's own filing is newest, a proposal's place is the proposal.
		var trigger bool
		var err error
		if proposalID != "" {
			trigger, err = keepSortedAt(ctx, proposalID)
		} else {
			_, trigger, _, err = keepSorted(ctx)
		}
		if err != nil {
			return protocol.DecisionInput{}, err
		}
		values, err := recordValues(ctx, []string{protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID)}, trigger)
		if err != nil {
			return protocol.DecisionInput{}, err
		}
		result["values"] = values
		result["key"], result["position_id"] = key, positionID
	}
	return protocol.DecisionInput{Type: "discovery.admission", Version: 1, Subject: admissionSubject(ref),
		Dependencies: deps, Inputs: inputs, Result: result}, nil
}

// applyAdmission applies a discovery.admission decision (made here, or
// admitted from the archive): its record and filing at its place, the
// proposal (proposalID) or, for the server's own discovery (""), the
// admission itself (PROTOCOL §9.4).
func applyAdmission(ctx *protocol.ApplyContext, d map[string]any, admissionID string, ref EpisodeRef, proposalID, source, action, key,
	positionID string) error {
	place := proposalID
	var proposal any // null for the server's own discovery
	if proposalID == "" {
		place = admissionID // the server's own discovery is its own place (PROTOCOL §9.4)
	} else {
		proposal = proposalID
	}
	record := map[string]any{"admission_id": admissionID, "source": source, "action": action, "proposal_id": proposal}
	// One effect: the admission record and its filing (a node contributes one
	// effect per scope).
	scope := protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID)
	e := projection.Effect{Kind: projection.KindAdmission, Admission: record}
	if action == "queue" {
		// Queue nodes (PROTOCOL §7.6): the place holding the filing, and the admission
		// itself, as a resolved add is. The filing is a replay step at its
		// place (PROTOCOL §7.6), resolved by the admission.
		e.QueueKey, e.PositionID = key, positionID
		if err := ctx.Account.AddQueueNode(place); err != nil {
			return err
		}
		if err := ctx.Account.AddQueueNode(admissionID); err != nil {
			return err
		}
		decision := admissionID
		if place == admissionID {
			decision = ""
		}
		// The filing's key is a key write when the replay finds it places
		// the episode (an automatic filing that does not apply writes none).
		if err := ctx.QueueStep(place, decision); err != nil {
			return err
		}
	}
	return ctx.ContributeAs(place, scope, e)
}

// ---- the server's own fetches (PROTOCOL §9.4)

// ServerFetch is one server fetch, applied by ApplyServerFetch inside a
// server commit. Its parse is Feed or, at record level, FeedFields and Items
// (as a fetch arrival has them, docs/CONTRACT.md); without a parse (and
// not NotModified) the fetch or its parse failed.
type ServerFetch struct {
	PodcastID          string
	GenerationID       string // the live generation when the fetch started ("" for none)
	RefreshBy          string // that generation's refresh-URL write when the fetch started
	LatestGenerationID string // without a live one, the latest generation then ("": the latest now); it names the parts and a move
	RefreshID          string // a UUID
	FetchTime          int64
	Feed               *feed.Feed        // nil when the fetch or parse failed, or when FeedFields and Items hold the parse
	FeedFields         map[string]any    // the parse's podcast metadata fields (PROTOCOL §9.4), used when Feed is nil
	Items              []ServerItem      // the parse's items in feed order, used when Feed is nil
	NotModified        bool              // a 304: a success confirming the stored response, with no records, evidence or discovery
	Bases              map[string]string // "feed" or an episode ID: the revision current when the fetch started
	Move               string            // the refresh URL's permanent move ("" for none)
	MovedTo            string            // the permanent redirect's final target, even when a new-feed-url wins the move ("" for none)
	Aliases            []string          // 301/308 hop targets, then a fetched new-feed-url
	NewFeedURL         string            // the fetched new-feed-url among Aliases (Feed.RedirectURL when "")
	Reparse            bool              // a re-parse of a stored response: no discovery, move or status
	MetadataOnly       bool              // an imported podcast without items: its record only, the generation left unpopulated (PROTOCOL §11.5)
	Status             map[string]any    // the refresh_status record to publish (nil when not due)
	CurrentStatus      map[string]any    // the fetch's status when none is due: a part 0 written anyway carries it
	Namespace          string
	// Revisions, when set, receives the revision each record of the parse
	// stands at for this fetch ("feed" or an episode ID): the part that wrote
	// it, else the canonical revision its unchanged record matched. The
	// refresher stores them with the response, so a re-parse names its own
	// revisions as bases.
	Revisions func(map[string]string)
}

// refreshPartSize: at most this many item records per catalog.refresh part.
const refreshPartSize = 200

// ApplyServerFetch writes a server fetch (PROTOCOL §9.4, §9.7): its
// catalog.refresh parts, the identity decisions its evidence makes, its
// admissions (each followed by the sort result it triggers), its move and a
// podcast merge. An unchanged fetch writes nothing unless a status is due.
//
// catalog.refresh (version 1), subject refresh:<P>:<refresh id>:<part>:
// inputs {podcast_id, refresh_id, part, outcome (success, failure or
// re-parse), generation_id (the generation the fetch started under, else the
// podcast's latest; null for a re-parse), fetch_time}; result {status?,
// population? (true), feed? ({base, fields}), items: [{episode_id,
// occurrence, base, fields}]}. Part 0 holds the status, the population
// marker, the feed record and the first refreshPartSize changed items, and
// exists whenever anything is written: admissions and the move depend on it.
func ApplyServerFetch(ctx *protocol.ApplyContext, x ServerFetch) error {
	acct := ctx.Account
	if exists, err := acct.PodcastExists(x.PodcastID); err != nil || !exists {
		return err
	}
	feedFields, items := x.records()
	if x.MetadataOnly && len(items) > 0 {
		return errors.New("ops: a metadata-only fetch carries no items")
	}
	parsed := feedFields != nil
	// The trace's URLs lose their userinfo as the parse's values do (the
	// reference strips every string of a fetch).
	x.Move, x.MovedTo, x.NewFeedURL = StripUserinfo(x.Move), StripUserinfo(x.MovedTo), StripUserinfo(x.NewFeedURL)
	x.Aliases = append([]string(nil), x.Aliases...)
	for i, a := range x.Aliases {
		x.Aliases[i] = StripUserinfo(a)
	}
	full := parsed && !x.Reparse // a successful fetch with its parse
	var g *projection.GenerationState
	populated := ""
	if x.GenerationID != "" {
		var err error
		if g, err = generationState(ctx, x.PodcastID, x.GenerationID); err != nil {
			return err
		}
		if populated, err = acct.Population(x.GenerationID); err != nil {
			return err
		}
	}
	live := g != nil && g.State != "removed"
	// An imported podcast without items records the backup's podcast record
	// and nothing else: no population marker and no seen history, so its
	// first real fetch is still its first population and files nothing (PROTOCOL §11.5).
	firstPopulation := live && full && populated == "" && !x.MetadataOnly
	discover := live && g.State == "subscribed" && full && populated != ""
	w := &refreshParts{ctx: ctx, x: x, population: firstPopulation, itemPart: map[string]string{}}
	switch {
	case x.Reparse:
		w.outcome = "re-parse"
	case parsed || x.NotModified:
		w.outcome = "success"
	default:
		w.outcome = "failure"
	}
	if !x.Reparse {
		if w.generation = x.GenerationID; w.generation == "" {
			w.generation = x.LatestGenerationID
		}
		if w.generation == "" {
			var err error
			if w.generation, err = latestGeneration(ctx, x.PodcastID); err != nil {
				return err
			}
		}
		if w.status = x.Status; w.status == nil {
			w.status = x.CurrentStatus
		}
	}
	var err error
	if w.creating, err = acct.PodcastCreatingOp(x.PodcastID); err != nil {
		return err
	}
	if parsed {
		same, err := sameAsCanonical(acct, "podcast", x.PodcastID, feedFields)
		if err != nil {
			return err
		}
		if !same {
			w.feed = feedFields
		}
	}
	// The move (PROTOCOL §9.4): the refresh URL if the generation and its refresh-URL
	// write are still the ones captured, and every alias no podcast claims. A
	// target another podcast claims is that podcast's feed: a merge (PROTOCOL §9.7).
	moveRefresh := false
	var aliases []map[string]any
	var mergeWith, landing string
	if !x.Reparse {
		nf := x.NewFeedURL
		if nf == "" && x.Feed != nil && x.Feed.RedirectURL != nil {
			nf = StripUserinfo(*x.Feed.RedirectURL)
		}
		nfAt := -1
		for i, a := range x.Aliases {
			if a == nf {
				nfAt = i
			}
		}
		seen := map[string]bool{}
		claim := func(raw string, alias bool, i int) error {
			n := contract.NormalizeURL(raw)
			if alias && seen[n] {
				return nil
			}
			seen[n] = true
			owner, err := acct.PodcastByAlias(n)
			if err != nil {
				return err
			}
			switch {
			case owner == "" && alias:
				source := "redirect"
				if i == nfAt {
					source = "new_feed_url"
				}
				aliases = append(aliases, map[string]any{"url": raw, "normalized": n, "source": source})
			}
			return nil
		}
		for i, a := range x.Aliases {
			if err := claim(a, true, i); err != nil {
				return err
			}
		}
		if x.Move != "" {
			if err := claim(x.Move, false, -1); err != nil {
				return err
			}
		}
		// A podcast merge (PROTOCOL §9.7): the first landing URL another podcast owns,
		// in the order 301/308 hops, the permanent move's target, the fetched
		// new-feed-url (reference CHOICE).
		var landings []string
		for i, a := range x.Aliases {
			if i != nfAt {
				landings = append(landings, a)
			}
		}
		if x.MovedTo != "" {
			landings = append(landings, x.MovedTo)
		}
		if nf != "" {
			landings = append(landings, nf)
		}
		for _, l := range landings {
			n := contract.NormalizeURL(l)
			owner, err := acct.PodcastByAlias(n)
			if err != nil {
				return err
			}
			if owner != "" && owner != x.PodcastID {
				mergeWith, landing = owner, n
				break
			}
		}
		// A move is a different normalized URL.
		moveRefresh = x.Move != "" && live && g.RefreshBy == x.RefreshBy &&
			contract.NormalizeURL(g.RefreshURL) != contract.NormalizeURL(x.Move)
	}
	// Identity (PROTOCOL §9.7, the reference's _identify_items): on a complete fetch,
	// GUID-change matching, then the items' evidence, the disputes it finds and
	// the items' derivations, each in item order; a re-parse only derives. Part
	// 0 (status, population, the feed) is written before the fetch's first
	// identity decision; the items then follow in parts 1, ….
	fitems := fetchItems(items)
	derived := make([]string, len(items))
	if parsed {
		before := func() error {
			_, err := w.part0Now()
			return err
		}
		if full {
			if err := guidChanges(ctx, x, fitems, w.creating, before); err != nil {
				return err
			}
		}
		// A complete fetch records all its evidence, then settles the
		// disputes its new evidence finds in item order, then derives its
		// items.
		newEvidence := false
		if full {
			fresh := make([]bool, len(fitems))
			for i, it := range fitems {
				if !it.ok {
					continue
				}
				isNew, err := recordEvidence(ctx, x.PodcastID, it.o, w.creating, it.kfam)
				if err != nil {
					return err
				}
				fresh[i] = isNew
				newEvidence = newEvidence || isNew
			}
			for i, it := range fitems {
				if fresh[i] {
					if err := detect(ctx, x.PodcastID, it.o, it.kfam, "", []string{w.creating}, before); err != nil {
						return err
					}
				}
			}
		}
		for i, it := range fitems {
			if !it.ok {
				continue // neither GUID nor enclosure: devices identify it (rule 5)
			}
			v := newView(ctx)
			e := v.derive(x.PodcastID, it.o, it.kfam, true)
			if v.err != nil {
				return v.err
			}
			if full {
				if err := setEvidenceEpisode(ctx, x.PodcastID, it.o, e); err != nil {
					return err
				}
			}
			derived[i] = e
		}
		if newEvidence {
			// New evidence may dispute enclosures (identity_rules).
			if err := publishIdentityRules(ctx, ctx.OpID, x.PodcastID); err != nil {
				return err
			}
		}
		if w.part0 != "" {
			// Identity decisions moved contributions: records are current again
			// before the fetch compares its records with them.
			if err := ctx.Flush(); err != nil {
				return err
			}
		}
	}
	// What changed, under the identities this fetch established: the items
	// the fetch keeps (identifiable, once, of this podcast) and those whose
	// record differs from the canonical one.
	var kept, changed []keptItem
	seen := map[string]bool{}
	for i, it := range items {
		e := derived[i]
		if e == "" || seen[e] {
			continue // unidentifiable, or one episode twice in a feed: the first stands
		}
		seen[e] = true
		if bound, err := acct.EpisodePodcast(e); err != nil {
			return err
		} else if bound != "" && bound != x.PodcastID {
			continue // another podcast's episode: no record, no discovery
		}
		k := keptItem{EpisodeRef{EpisodeID: e, PodcastID: x.PodcastID, Occurrence: it.Occurrence}, it.Fields}
		kept = append(kept, k)
		same, err := sameAsCanonical(acct, "episode", e, it.Fields)
		if err != nil {
			return err
		}
		if !same {
			changed = append(changed, k)
		}
	}
	// The server's own discoveries (PROTOCOL §9.4), decided before anything is written:
	// items new to the server's fetch history, or that it knew as
	// catalog-only and that now have an enclosure, under Android's date rule
	// against the newest effective date its history held before this fetch.
	var candidates []keptItem
	if discover {
		newest, hasNewest, err := acct.ServerNewest(x.PodcastID)
		if err != nil {
			return err
		}
		if !hasNewest {
			newest = x.FetchTime // Android: "now" when the feed had no items
		}
		for _, c := range kept {
			if !hasEnclosure(c.fields) {
				continue
			}
			before, known, err := acct.CatalogItem(c.ref.EpisodeID)
			if err != nil {
				return err
			}
			newToServer := !known || before.ServerSeenNode == ""
			gained := known && before.ServerSeenNode != "" && before.ServerCatalogOnly
			if !newToServer && !gained {
				continue
			}
			if pub, dated := integer(c.fields["pub_date"]); !gained && dated && pub < newest {
				continue // backfill (Android's date rule)
			}
			gref := c.ref
			gref.GenerationID = x.GenerationID
			if done, err := admitted(ctx, gref); err != nil {
				return err
			} else if !done {
				candidates = append(candidates, c)
			}
		}
	}
	// The server's fetch history changes only with archived evidence: kept items it newly sees, or
	// that gain an enclosure, without a changed record are listed in part 0's seen, so recovery
	// (PROTOCOL §11) restores the history exactly.
	if parsed {
		changedSet := map[string]bool{}
		for _, c := range changed {
			changedSet[c.ref.EpisodeID] = true
		}
		for _, c := range kept {
			if changedSet[c.ref.EpisodeID] {
				continue
			}
			before, known, err := acct.CatalogItem(c.ref.EpisodeID)
			if err != nil {
				return err
			}
			if !known || before.ServerSeenNode == "" || (before.ServerCatalogOnly && hasEnclosure(c.fields)) {
				w.seen = append(w.seen, c.ref.EpisodeID)
			}
		}
		sort.Strings(w.seen)
	}
	w.seenReady = true
	write := w.part0 != "" || w.feed != nil || len(changed) > 0 || firstPopulation || len(candidates) > 0 || moveRefresh ||
		len(aliases) > 0 || mergeWith != "" || (x.Status != nil && !x.Reparse) || len(w.seen) > 0
	if !write && !parsed {
		return nil
	}
	if write {
		if err := w.writeItems(changed); err != nil {
			return err
		}
		if len(w.seen) > 0 && !w.seenDone {
			if _, err := w.part(nil); err != nil {
				return err
			}
		}
	}
	// Catalog items: what the catalog and the server's fetch history know,
	// from what the archive names (an item's part, or part 0's seen), so a
	// rebuild notes the same: an unchanged item already in the history is
	// not noted, also when part 0 is written for another reason (its date
	// would replace a later observation's).
	seenNow := map[string]bool{}
	for _, e := range w.seen {
		seenNow[e] = true
	}
	for _, c := range kept {
		node := w.itemPart[c.ref.EpisodeID]
		if node == "" && seenNow[c.ref.EpisodeID] {
			node = w.part0
		}
		if node == "" {
			continue // unchanged and already in the history: nothing to note
		}
		// A re-parse reads the server's own stored response, so it updates the
		// server's fetch history too.
		if _, err := noteCatalogItemAs(ctx, node, c.ref, c.fields, x.FetchTime, parsed); err != nil {
			return err
		}
	}
	if x.Revisions != nil && parsed {
		stamps, err := acct.CatalogStamps(x.PodcastID)
		if err != nil {
			return err
		}
		revs := map[string]string{}
		if w.feed != nil && w.part0 != "" {
			revs["feed"] = w.part0
		} else if st, ok := stamps["feed"]; ok {
			revs["feed"] = st.Revision
		}
		for _, c := range kept {
			if part := w.itemPart[c.ref.EpisodeID]; part != "" {
				revs[c.ref.EpisodeID] = part
			} else if st, ok := stamps[c.ref.EpisodeID]; ok {
				revs[c.ref.EpisodeID] = st.Revision
			}
		}
		x.Revisions(revs)
	}
	// The server's admissions, in the fetch's item order, each its own filing
	// place and sort trigger (PROTOCOL §9.4).
	if len(candidates) > 0 {
		action, err := discoveryAction(ctx, g)
		if err != nil {
			return err
		}
		prev := ""
		for _, c := range candidates {
			gref := c.ref
			gref.GenerationID = x.GenerationID
			found := w.itemPart[c.ref.EpisodeID]
			if found == "" {
				found = w.part0
			}
			adm, err := admit(ctx, gref, "", "server", action, found, "", "", nil, prev)
			if err != nil {
				return err
			}
			if action == "queue" && adm != "" {
				prev = adm
			}
		}
	}
	// podcast.moved (version 1), subject moved:<G>:<refresh id> (G as in the
	// parts' inputs): inputs {podcast_id, generation_id, refresh_id}; result
	// {refresh_url (or null), aliases: [{url, normalized, source}]}.
	if moveRefresh || len(aliases) > 0 {
		list := make([]any, len(aliases))
		for i, a := range aliases {
			list[i] = a
		}
		var refreshURL any
		if moveRefresh {
			refreshURL = x.Move
		}
		d, err := ctx.Decide(protocol.DecisionInput{Type: "podcast.moved", Version: 1, Subject: "moved:" + w.generation + ":" + x.RefreshID,
			Dependencies: []string{w.part0}, Inputs: map[string]any{"podcast_id": x.PodcastID, "generation_id": w.generation, "refresh_id": x.RefreshID},
			Result: map[string]any{"refresh_url": refreshURL, "aliases": list}})
		if err != nil {
			return err
		}
		node := d["decision_id"].(string)
		if err := applyMove(ctx, node, x.PodcastID, w.generation, refreshURL, aliases); err != nil {
			return err
		}
	}
	if mergeWith != "" {
		if err := mergePodcasts(ctx, x.PodcastID, mergeWith, w.part0, x.RefreshID, landing); err != nil {
			return err
		}
	}
	return nil
}

// applyMove applies a podcast.moved decision (made here, or admitted from
// the archive): its aliases are claimed, and the move refolds the podcast
// (its identity record lists the aliases); with a refresh URL it writes the
// generation's refresh URL like change_url.
func applyMove(ctx *protocol.ApplyContext, node, podcast, generation string, refreshURL any, aliases []map[string]any) error {
	for _, a := range aliases {
		normalized, _ := a["normalized"].(string)
		url, _ := a["url"].(string)
		source, _ := a["source"].(string)
		if err := ctx.Account.AddAlias(normalized, podcast, url, source, node); err != nil {
			return err
		}
	}
	move := projection.Effect{Kind: projection.KindMove}
	if u, ok := refreshURL.(string); ok && u != "" {
		move.Generation, move.RefreshURL = generation, u
	}
	return ctx.ContributeAs(node, protocol.PodcastScope(podcast), move)
}

// keptItem is a fetched item the fetch keeps: its reference and record fields.
type keptItem struct {
	ref    EpisodeRef
	fields map[string]any
}

// refreshParts writes one fetch's catalog.refresh parts.
type refreshParts struct {
	ctx        *protocol.ApplyContext
	x          ServerFetch
	outcome    string
	generation string // inputs.generation_id ("" for null: a re-parse)
	creating   string // the podcast's identity node
	status     map[string]any
	population bool
	feed       map[string]any    // the changed feed record (part 0)
	n          int               // the next part
	part0      string            // part 0's decision ID once written
	itemPart   map[string]string // episode → the part holding its record
	seen       []string          // episodes whose server history this fetch changes without a record
	seenDone   bool              // a part carries seen
	seenReady  bool              // seen is computed (the items are derived)
}

// part0Now is part 0, written now (without items) if it is not yet.
func (w *refreshParts) part0Now() (string, error) {
	if w.part0 != "" {
		return w.part0, nil
	}
	return w.part(nil)
}

// writeItems writes the changed item records: the first refreshPartSize in
// part 0 when it is not yet written, the rest in parts of at most
// refreshPartSize. Part 0 is written even without items.
func (w *refreshParts) writeItems(changed []keptItem) error {
	if w.part0 == "" {
		n := min(refreshPartSize, len(changed))
		if _, err := w.part(changed[:n]); err != nil {
			return err
		}
		changed = changed[n:]
	}
	for i := 0; i < len(changed); i += refreshPartSize {
		if _, err := w.part(changed[i:min(i+refreshPartSize, len(changed))]); err != nil {
			return err
		}
	}
	return nil
}

// part decides the next part with these item records. A part depends on the
// podcast's identity node and on the revisions its records replace (captured
// before the fetch's I/O). Its records are observations at its place, routed
// by their items' evidence (observedRef).
func (w *refreshParts) part(chunk []keptItem) (string, error) {
	ctx, x := w.ctx, w.x
	deps := []string{w.creating}
	base := func(key string) any {
		b := x.Bases[key]
		if b == "" {
			return nil
		}
		if _, err := ctx.Account.NodeTime(b); err == nil {
			deps = append(deps, b)
		}
		return b
	}
	result := map[string]any{}
	if w.n == 0 {
		if w.status != nil {
			result["status"] = w.status
		}
		if w.population {
			result["population"] = true
		}
		if w.feed != nil {
			result["feed"] = map[string]any{"base": base("feed"), "fields": w.feed}
		}
	}
	// seen goes in the first part written once the items are derived: part
	// 0, or the next part when part 0 was written earlier for identity
	// evidence.
	if len(w.seen) > 0 && !w.seenDone && w.seenReady {
		list := make([]any, len(w.seen))
		for i, id := range w.seen {
			list[i] = id
		}
		result["seen"] = list
		w.seenDone = true
	}
	list := make([]any, len(chunk))
	for i, c := range chunk {
		list[i] = map[string]any{"episode_id": c.ref.EpisodeID, "occurrence": c.ref.Occurrence, "base": base(c.ref.EpisodeID), "fields": c.fields}
	}
	result["items"] = list
	var generation any
	if w.generation != "" {
		generation = w.generation
	}
	d, err := ctx.Decide(protocol.DecisionInput{Type: "catalog.refresh", Version: 1,
		Subject: "refresh:" + x.PodcastID + ":" + x.RefreshID + ":" + strconv.Itoa(w.n), Dependencies: deps,
		Inputs: map[string]any{"podcast_id": x.PodcastID, "refresh_id": x.RefreshID, "part": w.n, "outcome": w.outcome,
			"generation_id": generation, "fetch_time": x.FetchTime}, Result: result})
	if err != nil {
		return "", err
	}
	id := d["decision_id"].(string)
	if w.n == 0 {
		w.part0 = id
		if w.population {
			if err := ctx.Account.SetPopulation(x.GenerationID, id); err != nil {
				return "", err
			}
		}
		// Part 0's one podcast-scope effect: the feed observation and the status.
		if w.feed != nil || w.status != nil {
			e := projection.Effect{Kind: projection.KindRefreshStatus, Refresh: w.status}
			if w.feed != nil {
				e.Kind, e.Observed, e.RevisionBase, e.FetchTime = projection.KindObservation, w.feed, x.Bases["feed"], x.FetchTime
			}
			if err := ctx.ContributeAs(id, protocol.PodcastScope(x.PodcastID), e); err != nil {
				return "", err
			}
		}
	}
	w.n++
	for _, c := range chunk {
		w.itemPart[c.ref.EpisodeID] = id
		if err := ctx.ContributeAs(id, protocol.ItemScope(c.ref.EpisodeID), projection.Effect{Kind: projection.KindObservation,
			Observed: c.fields, RevisionBase: x.Bases[c.ref.EpisodeID], FetchTime: x.FetchTime,
			Ref: observedRef(c.ref.EpisodeID, x.PodcastID, c.ref.Occurrence, c.fields)}); err != nil {
			return "", err
		}
	}
	return id, nil
}

// noteCatalogItemAs is noteCatalogItem with the server's node as origin.
func noteCatalogItemAs(ctx *protocol.ApplyContext, node string, ref EpisodeRef, fv map[string]any, at int64, server bool) (bool, error) {
	saved := ctx.OpID
	ctx.OpID = node
	defer func() { ctx.OpID = saved }()
	return noteCatalogItem(ctx, ref, fv, node, at, server)
}

// discoveryAction is the generation's new-episodes action with global
// resolved through the account default (Android's default: inbox).
func discoveryAction(ctx *protocol.ApplyContext, g *projection.GenerationState) (string, error) {
	if g != nil && g.NewEpisodes != "" && g.NewEpisodes != "global" {
		return g.NewEpisodes, nil
	}
	rows, err := ctx.Account.Contributions(protocol.AccountScope)
	if err != nil {
		return "", err
	}
	cs := make([]projection.Contribution, 0, len(rows))
	for _, r := range rows {
		e, err := decodeEffect(r.Effect)
		if err != nil {
			return "", err
		}
		cs = append(cs, projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e})
	}
	if a := projection.FoldAccount(cs).DiscoveryPolicy; a != "" {
		return a, nil
	}
	return "inbox", nil
}

// sameAsCanonical reports whether a record's canonical value already equals
// these fields (its revision and fetch time aside).
func sameAsCanonical(acct *storage.AccountTx, entity, id string, fv map[string]any) (bool, error) {
	r, err := acct.Record(entity, id, "metadata")
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	v, err := contract.DecodeJSON([]byte(r.Value))
	if err != nil {
		return false, err
	}
	cur, _ := v.(map[string]any)
	delete(cur, "revision")
	delete(cur, "fetch_time")
	a, err := contract.Canonical(cur)
	if err != nil {
		return false, err
	}
	b, err := contract.Canonical(normalizeJSON(fv))
	if err != nil {
		return false, err
	}
	return string(a) == string(b), nil
}

// normalizeJSON gives Go values the shapes DecodeJSON produces, for comparison.
func normalizeJSON(v any) any {
	b, err := contract.Canonical(v)
	if err != nil {
		return v
	}
	out, err := contract.DecodeJSON(b)
	if err != nil {
		return v
	}
	return out
}

// podcastFields are a parsed feed's metadata record fields (PROTOCOL §9.4), without URL
// userinfo (a private feed's credentials reach devices only through the
// credentials secret).
func podcastFields(f *feed.Feed) map[string]any {
	o := f.Object()
	out := map[string]any{}
	for _, k := range podcastMetadataFields {
		out[k] = o[k]
	}
	return stripAll(out).(map[string]any)
}

// itemFields are a parsed item's metadata record fields (PROTOCOL §9.4).
func itemFields(it *feed.Item) map[string]any {
	o := it.Object()
	out := map[string]any{}
	for _, k := range episodeMetadataFields {
		out[k] = o[k]
	}
	return stripAll(out).(map[string]any)
}

func decodeEffect(s string) (projection.Effect, error) {
	var e projection.Effect
	err := json.Unmarshal([]byte(s), &e)
	return e, err
}

// RefreshContext is what a server fetch captures before its network I/O
// (PROTOCOL §9.4): the live generation and its state, its refresh URL and the write
// that set it, its credentials secret, and the current revisions (bases).
type RefreshContext struct {
	PodcastID    string
	GenerationID string // "" when every generation is closed
	State        string // not_subscribed, subscribed, archived ("" without a live generation)
	KeepUpdated  bool
	RefreshURL   string // the live generation's, else the latest generation's
	RefreshBy    string
	SecretID     string
	Bases        map[string]string // "feed" or an episode ID → its metadata revision
}

// CaptureRefresh reads a podcast's refresh context (inside any transaction,
// for example a ServerCommit that writes nothing).
func CaptureRefresh(ctx *protocol.ApplyContext, podcastID string) (RefreshContext, error) {
	rc := RefreshContext{PodcastID: podcastID}
	rows, err := ctx.Account.Contributions(protocol.PodcastScope(podcastID))
	if err != nil {
		return rc, err
	}
	var cs []projection.Contribution
	for _, r := range rows {
		e, err := decodeEffect(r.Effect)
		if err != nil {
			return rc, err
		}
		if !projection.IsCatalogKind(e.Kind) {
			cs = append(cs, projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e})
		}
	}
	st := projection.FoldPodcast(cs)
	g := st.Live
	if g == nil {
		g = st.Latest
	} else {
		rc.GenerationID, rc.State, rc.KeepUpdated, rc.SecretID = g.ID, g.State, g.KeepUpdated, g.SecretID
	}
	if g != nil {
		rc.RefreshURL, rc.RefreshBy = g.RefreshURL, g.RefreshBy
	}
	if rc.Bases, err = ctx.Account.CatalogRevisions(podcastID); err != nil {
		return rc, err
	}
	if r, err := ctx.Account.Record("podcast", podcastID, "metadata"); err == nil {
		if v, err := contract.DecodeJSON([]byte(r.Value)); err == nil {
			if m, ok := v.(map[string]any); ok {
				if rev, ok := m["revision"].(string); ok {
					rc.Bases["feed"] = rev
				}
			}
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return rc, err
	}
	return rc, nil
}
