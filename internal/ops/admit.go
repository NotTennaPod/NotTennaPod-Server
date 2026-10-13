package ops

import (
	"errors"
	"fmt"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Admit steps (PROTOCOL §6.11): each server decision type applies its recorded result to the
// derived state as the code that made it did, with no new randomness and no new decision, so an
// account rebuilds from its archive in receipt order. The decisions a decision's making triggered
// (a filing's sort result, a merge's occurrence mappings) are archived entries of their own,
// admitted in their turn. The decision is a causal node with its recorded edges, and its archived
// row is restored, before its admit step runs.

// registerAdmits adds the admit steps of every server decision type.
func registerAdmits(r *protocol.Registry) {
	r.RegisterAdmit("catalog.refresh", admitRefreshPart)
	r.RegisterAdmit("discovery.admission", admitAdmission)
	r.RegisterAdmit("podcast.moved", admitMoved)
	r.RegisterAdmit("episode.guid_change", admitGUIDChangeDecision)
	r.RegisterAdmit("episode.merge", admitEpisodeMerge)
	r.RegisterAdmit("episode.occurrence_mapping", admitOccurrenceMapping)
	r.RegisterAdmit("podcast.merge", admitPodcastMerge)
	r.RegisterAdmit("queue.resolved_add", admitResolvedAdd)
	r.RegisterAdmit("queue.layout", admitLayout)
	r.RegisterAdmit("podcast.resolution", admitProvisionalResolution)
	r.RegisterAdmit("import.podcast", admitImportPodcast)
	r.RegisterAdmit("import.episode", admitImportEpisode)
	// The podcast.alias an import.podcast makes records the claim its
	// import.podcast already applied.
	r.RegisterAdmit("podcast.alias", admitNothing)
	r.RegisterAdmit("import.manifest", admitImportManifest)
}

func admitNothing(*protocol.ApplyContext, map[string]any) error { return nil }

// admitImportManifest admits an import.manifest: the import's whole queue is
// one replay step at its place (PROTOCOL §7.6).
func admitImportManifest(c *protocol.ApplyContext, d map[string]any) error {
	id, _, _ := decisionParts(d)
	return c.QueueStep(id, "")
}

// decisionParts reads an archived decision's ID, inputs and result.
func decisionParts(d map[string]any) (string, map[string]any, map[string]any) {
	id, _ := str(d, "decision_id")
	inputs, _ := object(d["inputs"])
	result, _ := object(d["result"])
	return id, inputs, result
}

func malformed(d map[string]any, what string) error {
	id, _ := str(d, "decision_id")
	t, _ := str(d, "decision_type")
	return fmt.Errorf("ops: admitting %s %s: %s", t, id, what)
}

// admitRefreshPart admits a catalog.refresh part (ApplyServerFetch's part,
// writeItems and the catalog items it notes): part 0's population marker and
// its podcast-scope effect (the feed observation, or the refresh status);
// each item's record as an observation at the part, routed by the item's
// evidence as ApplyServerFetch's part routes it; on a successful fetch
// the items' identity evidence; the catalog items the part's records and its
// seen list change.
func admitRefreshPart(c *protocol.ApplyContext, d map[string]any) error {
	id, inputs, result := decisionParts(d)
	acct := c.Account
	podcast, _ := str(inputs, "podcast_id")
	refreshID, _ := str(inputs, "refresh_id")
	part, _ := integer(inputs["part"])
	outcome, _ := str(inputs, "outcome")
	generation, _ := str(inputs, "generation_id")
	fetchTime, _ := integer(inputs["fetch_time"])
	if podcast == "" {
		return malformed(d, "no podcast")
	}
	if part == 0 {
		if population, _ := boolean(result["population"]); population {
			if err := acct.SetPopulation(generation, id); err != nil {
				return err
			}
		}
		status, _ := object(result["status"])
		feed, hasFeed := object(result["feed"])
		if hasFeed || status != nil {
			e := projection.Effect{Kind: projection.KindRefreshStatus, Refresh: status}
			if hasFeed {
				fields, _ := object(feed["fields"])
				base, _ := str(feed, "base")
				e.Kind, e.Observed, e.RevisionBase, e.FetchTime = projection.KindObservation, fields, base, fetchTime
			}
			if err := c.ContributeAs(id, protocol.PodcastScope(podcast), e); err != nil {
				return err
			}
		}
	}
	items, _ := result["items"].([]any)
	type item struct {
		ref    EpisodeRef
		fields map[string]any
	}
	list := make([]item, 0, len(items))
	for _, raw := range items {
		x, _ := object(raw)
		e, _ := str(x, "episode_id")
		occ, _ := object(x["occurrence"])
		fields, _ := object(x["fields"])
		base, _ := str(x, "base")
		if e == "" {
			return malformed(d, "an item without its episode")
		}
		list = append(list, item{EpisodeRef{EpisodeID: e, PodcastID: podcast, Occurrence: occ}, fields})
		if err := c.ContributeAs(id, protocol.ItemScope(e), projection.Effect{Kind: projection.KindObservation, Observed: fields,
			RevisionBase: base, FetchTime: fetchTime, Ref: observedRef(e, podcast, occ, fields)}); err != nil {
			return err
		}
	}
	// A complete fetch records every item's identity evidence (by the
	// podcast's identity node), and where it derived.
	newEvidence := false
	if outcome == "success" && len(list) > 0 {
		creating, err := acct.PodcastCreatingOp(podcast)
		if err != nil {
			return err
		}
		for _, it := range list {
			o := occurrenceOf(it.ref.Occurrence)
			if o.guid == "" && o.enclosure == "" {
				continue
			}
			isNew, err := recordEvidence(c, podcast, o, creating, guidKind(it.ref.Occurrence))
			if err != nil {
				return err
			}
			newEvidence = newEvidence || isNew
			if err := setEvidenceEpisode(c, podcast, o, it.ref.EpisodeID); err != nil {
				return err
			}
		}
	}
	server := outcome == "success" || outcome == "re-parse"
	for _, it := range list {
		if _, err := noteCatalogItemAs(c, id, it.ref, it.fields, fetchTime, server); err != nil {
			return err
		}
	}
	// The kept items whose server history the fetch changed without a
	// changed record: noted by part 0, with their canonical record.
	if seen, _ := result["seen"].([]any); len(seen) > 0 {
		part0 := id
		if part != 0 {
			var err error
			if part0, err = acct.DecisionBySubject("catalog.refresh", "refresh:"+podcast+":"+refreshID+":0"); err != nil {
				return err
			}
		}
		if err := c.Flush(); err != nil {
			return err
		}
		for _, raw := range seen {
			e, _ := raw.(string)
			fields, err := recordValue(acct, "episode", e, "metadata")
			if err != nil {
				return err
			}
			occRaw, err := acct.EpisodeOccurrence(e)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			var occ map[string]any
			if occRaw != "" {
				if v, err := contract.DecodeJSON([]byte(occRaw)); err == nil {
					occ, _ = object(v)
				}
			}
			if _, err := noteCatalogItemAs(c, part0, EpisodeRef{EpisodeID: e, PodcastID: podcast, Occurrence: occ}, fields, fetchTime, server); err != nil {
				return err
			}
		}
	}
	if newEvidence {
		// New evidence may dispute enclosures (identity_rules).
		return publishIdentityRules(c, c.OpID, podcast)
	}
	return nil
}

// admitAdmission admits a discovery.admission (admit): its record and filing
// at its place (the proposal, or the admission itself for the server's own
// discovery).
func admitAdmission(c *protocol.ApplyContext, d map[string]any) error {
	id, _, result := decisionParts(d)
	ref := EpisodeRef{}
	ref.EpisodeID, _ = str(result, "episode_id")
	ref.GenerationID, _ = str(result, "generation_id")
	action, _ := str(result, "action")
	source, _ := str(result, "source")
	proposal, _ := str(result, "proposal_id")
	key, _ := str(result, "key")
	positionID, _ := str(result, "position_id")
	return applyAdmission(c, d, id, ref, proposal, source, action, key, positionID)
}

// admitMoved admits a podcast.moved (ApplyServerFetch's move).
func admitMoved(c *protocol.ApplyContext, d map[string]any) error {
	id, inputs, result := decisionParts(d)
	podcast, _ := str(inputs, "podcast_id")
	generation, _ := str(inputs, "generation_id")
	var aliases []map[string]any
	list, _ := result["aliases"].([]any)
	for _, x := range list {
		a, _ := object(x)
		aliases = append(aliases, a)
	}
	return applyMove(c, id, podcast, generation, result["refresh_url"], aliases)
}

// admitGUIDChangeDecision admits an episode.guid_change (guidChanges).
func admitGUIDChangeDecision(c *protocol.ApplyContext, d map[string]any) error {
	id, inputs, result := decisionParts(d)
	podcast, _ := str(inputs, "podcast_id")
	guid, _ := str(result, "guid")
	episode, _ := str(result, "episode_id")
	return admitGUIDChange(c, podcast, guid, episode, id)
}

// admitEpisodeMerge admits an episode.merge (guidChanges' merge of a
// registered new item; recovery's unification merges).
func admitEpisodeMerge(c *protocol.ApplyContext, d map[string]any) error {
	id, inputs, result := decisionParts(d)
	podcast, _ := str(inputs, "podcast_id")
	from, _ := str(result, "from")
	into, _ := str(result, "into")
	return applyEpisodeMerge(c, id, podcast, from, into)
}

// applyEpisodeMerge applies an episode.merge decision: the merge, the
// placement of every contribution, and the podcast's identity rules.
func applyEpisodeMerge(c *protocol.ApplyContext, node, podcast, from, into string) error {
	if err := mergeEpisode(c, podcast, from, into, node); err != nil {
		return err
	}
	if err := placeAll(c, node, podcast); err != nil {
		return err
	}
	return publishIdentityRules(c, node, podcast)
}

// admitOccurrenceMapping admits an episode.occurrence_mapping (detect).
func admitOccurrenceMapping(c *protocol.ApplyContext, d map[string]any) error {
	id, inputs, result := decisionParts(d)
	podcast, _ := str(inputs, "podcast_id")
	guid, _ := str(inputs, "guid")
	r := storage.Release{GUID: guid, Node: id}
	r.EpisodeID, _ = str(result, "episode_id")
	if u, ok := str(result, "enclosure"); ok {
		// An enclosure addition: the release lists it after its
		// own enclosures; every contribution is placed again.
		if u == "" || r.EpisodeID == "" {
			return malformed(d, "an enclosure addition without its release or enclosure")
		}
		if err := c.Account.AddReleaseAddition(podcast, guid, storage.ReleaseAddition{EpisodeID: r.EpisodeID, Enclosure: u, Node: id}); err != nil {
			return err
		}
		if err := placeAll(c, id, podcast); err != nil {
			return err
		}
		return publishIdentityRules(c, id, podcast)
	}
	r.PubDate, _ = integer(result["publication_time"])
	r.Original, _ = boolean(result["original"])
	encs, _ := result["enclosures"].([]any)
	for _, x := range encs {
		if u, ok := x.(string); ok {
			r.Enclosures = append(r.Enclosures, u)
		}
	}
	if len(r.Enclosures) == 0 && !r.Original {
		return malformed(d, "a later release without an enclosure")
	}
	return admitMapping(c, podcast, guid, r)
}

// admitPodcastMerge admits a podcast.merge (mergePodcasts): the generation
// it joins or rehomes and the episode mappings it lists. The occurrence
// mappings its joining evidence made follow as their own entries.
func admitPodcastMerge(c *protocol.ApplyContext, d map[string]any) error {
	id, _, result := decisionParts(d)
	source, _ := str(result, "source")
	survivor, _ := str(result, "into")
	fromGen, intoGen, rehome := "", "", false
	gens, _ := result["generations"].([]any)
	for _, x := range gens {
		g, _ := object(x)
		gid, _ := str(g, "generation_id")
		switch mode, _ := str(g, "mode"); mode {
		case "joined":
			fromGen, _ = str(g, "generation_id")
			intoGen, _ = str(g, "into")
		case "rehomed":
			fromGen, intoGen, rehome = gid, gid, true
		}
	}
	var maps []episodeMapping
	eps, _ := result["episodes"].([]any)
	for _, x := range eps {
		m, _ := object(x)
		from, _ := str(m, "from")
		to, _ := str(m, "to")
		maps = append(maps, episodeMapping{from, to})
	}
	return applyPodcastMerge(c, id, source, survivor, fromGen, intoGen, rehome, maps)
}

// admitImportPodcast admits an import.podcast (ApplyImportPodcast): the
// podcast, its alias and its generation with the backup's state.
func admitImportPodcast(c *protocol.ApplyContext, d map[string]any) error {
	id, _, result := decisionParts(d)
	podcast, _ := str(result, "podcast_id")
	generation, _ := str(result, "generation_id")
	url, _ := str(result, "url")
	state, _ := str(result, "state")
	keep, _ := boolean(result["keep_updated"])
	action, _ := str(result, "new_episodes_action")
	secret, _ := str(result, "secret_id")
	if err := c.Account.CreatePodcast(podcast, id); err != nil {
		return err
	}
	if err := c.Account.AddAlias(contract.NormalizeURL(url), podcast, url, "import", id); err != nil {
		return err
	}
	return importGeneration(c, id, podcast, generation, url, state, keep, action, secret)
}

// admitImportEpisode admits an import.episode (importEpisode).
func admitImportEpisode(c *protocol.ApplyContext, d map[string]any) error {
	_, _, result := decisionParts(d)
	ref := EpisodeRef{}
	ref.EpisodeID, _ = str(result, "episode_id")
	ref.Occurrence, _ = object(result["occurrence"])
	generation, _ := str(result, "generation_id")
	g, err := c.Account.Generation(generation)
	if err != nil {
		return err
	}
	ref.PodcastID = g.PodcastID
	catalog, _ := object(result["catalog"])
	// A group of several rows: each row's own state and occurrence.
	inputs, _ := object(d["inputs"])
	importID, _ := str(inputs, "import_id")
	list, _ := result["rows"].([]any)
	if rows := importedRowsFrom(importID, generation, ref, list); len(rows) > 0 {
		return applyImportEpisode(c, d, generation, ref, importedEffect(rows, true), catalog)
	}
	imported, _ := object(result["imported"])
	e := projection.Effect{Kind: projection.KindImport, Imported: importedFrom(imported)}
	e.QueueKey, _ = str(result, "queue_key")
	e.PositionID, _ = str(result, "position_id")
	if duration, _ := integer(result["duration"]); duration > 0 {
		e.Duration = &duration
	}
	return applyImportEpisode(c, d, generation, ref, e, catalog)
}

// admitProvisionalResolution admits a podcast.resolution its registration
// did not make again: one of a recovery rebuild that a smaller rank's
// resolution of the same registration outranks (PROTOCOL §6.11). Identity never
// splits: what it created or opened (a podcast, a generation) still exists,
// without the subscription's effect; the end of a recovery request unifies
// it with the effective resolution's.
func admitProvisionalResolution(c *protocol.ApplyContext, d map[string]any) error {
	id, _, result := decisionParts(d)
	op, _ := str(d, "subject")
	eff, ok := c.RecordedDecision("podcast.resolution", op)
	if !ok || eff["decision_id"] == id {
		return nil
	}
	mode, _ := str(result, "mode")
	if mode != "created" && mode != "opened" {
		return nil
	}
	podcast, _ := str(result, "podcast_id")
	generation, _ := str(result, "generation_id")
	effResult, _ := object(eff["result"])
	acct := c.Account
	if ep, _ := str(effResult, "podcast_id"); ep == podcast {
		eg, _ := str(effResult, "generation_id")
		if eg == generation || eg == "" || mode != "opened" {
			return nil
		}
		// The effective resolution's podcast: the provisional generation joins
		// the effective one (PROTOCOL §9.7's join), so what was accepted on it stays.
		if _, err := acct.Generation(generation); errors.Is(err, storage.ErrNotFound) {
			if err := acct.CreateGeneration(generation, podcast, op, ""); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		return acct.AddGenerationJoin(generation, eg, id)
	}
	if mode == "created" {
		exists, err := acct.PodcastExists(podcast)
		if err != nil {
			return err
		}
		if !exists {
			if err := acct.CreatePodcast(podcast, op); err != nil {
				return err
			}
		}
	}
	if generation == "" {
		return nil
	}
	if _, err := acct.Generation(generation); !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	row, err := acct.Fact(op)
	if err != nil {
		return err
	}
	v, err := contract.DecodeJSON(row.Fact)
	if err != nil {
		return err
	}
	a, err := protocol.ParseFact(v)
	if err != nil || a.Fact == nil {
		return err
	}
	o := resolveOp{preview: a.Fact.Type == "podcast.register_preview"}
	feedURL, obs, policy, secret, err := o.parse(a.Fact)
	if err != nil {
		return nil
	}
	predecessor := ""
	if mode == "opened" && obs != nil {
		predecessor = obs.GenerationID
	}
	if err := acct.CreateGeneration(generation, podcast, op, predecessor); err != nil {
		return err
	}
	// The generation is live (identity never splits): a unification at the
	// request's end joins it into the effective resolution's.
	e := projection.Effect{Kind: projection.KindResolve, Generation: generation, Mode: mode, Preview: o.preview, Predecessor: predecessor,
		RefreshURL: feedURL}
	if policy != nil {
		e.NewEpisodes, e.KeepUpdated = policy.NewEpisodes, policy.KeepUpdated
	}
	if secret != "" {
		e.SecretID = &secret
	}
	if _, has, err := acct.Contribution(protocol.PodcastScope(podcast), op); err != nil || has {
		return err
	}
	return c.ContributeAs(op, protocol.PodcastScope(podcast), e)
}
