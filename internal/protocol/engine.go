package protocol

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Scope names (PROTOCOL §7.2): an episode within one generation, a
// podcast, or the account.
func EpisodeScope(generationID, episodeID string) string {
	return "episode:" + generationID + ":" + episodeID
}
func PodcastScope(podcastID string) string { return "podcast:" + podcastID }

// ItemScope is an episode's catalog scope: its canonical metadata, which is not
// per generation (PROTOCOL §9.4).
func ItemScope(episodeID string) string { return "item:" + episodeID }

const AccountScope = "account"

// Contribute records this fact's effect on a scope; Settle recomputes it.
func (c *ApplyContext) Contribute(scope string, e projection.Effect) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	v, err := contract.DecodeJSON(b)
	if err != nil {
		return err
	}
	canonical, err := contract.Canonical(v)
	if err != nil {
		return err
	}
	if c.Resolved {
		// Two references of this fact resolved to one episode: its effects
		// for different members compose instead of overwriting (PROTOCOL §9.7).
		if existing, ok, err := c.Account.Contribution(scope, c.OpID); err != nil {
			return err
		} else if ok {
			var x projection.Effect
			if err := json.Unmarshal([]byte(existing), &x); err != nil {
				return err
			}
			if x.Member != e.Member || x.Kind == projection.KindCompound {
				if canonical, err = contract.Canonical(mustDecode(projection.ComposeAt(scopeEpisode(scope), x, e))); err != nil {
					return err
				}
			}
		}
	}
	if err := c.Account.Contribute(scope, c.OpID, string(canonical)); err != nil {
		return err
	}
	if c.scopeSet == nil {
		c.scopeSet = map[string]bool{}
	}
	if !c.scopeSet[scope] {
		c.scopeSet[scope] = true
		c.scopes = append(c.scopes, scope)
	}
	return nil
}

// ContributeAs records another node's effect on a scope (a decision this fact
// caused); its refold is deferred like SettleLater's.
func (c *ApplyContext) ContributeAs(node, scope string, e projection.Effect) error {
	if e.Kind == projection.KindAdmission {
		// Two admissions of one proposal file at the proposal's node: one a merge moved here from
		// another episode stays beside this one (PROTOCOL §9.7), so the fold keeps the earlier by
		// rank (PROTOCOL §6 point 5). This admission's own earlier effect is replaced.
		if existing, ok, err := c.Account.Contribution(scope, node); err != nil {
			return err
		} else if ok {
			var x projection.Effect
			if err := json.Unmarshal([]byte(existing), &x); err != nil {
				return err
			}
			ep := scopeEpisode(scope)
			parts := []projection.Effect{x}
			if x.Kind == projection.KindCompound {
				parts = x.Compound
			}
			var moved []projection.Effect
			for _, p := range parts {
				if p.Member != "" && p.Member != ep {
					moved = append(moved, p)
				}
			}
			if len(moved) > 0 {
				e = projection.ComposeAt(ep, append(moved, e)...)
			}
		}
	}
	canonical, err := canonicalString(e)
	if err != nil {
		return err
	}
	if err := c.Account.Contribute(scope, node, canonical); err != nil {
		return err
	}
	if c.commit.dirty == nil {
		c.commit.dirty = map[string]string{}
	}
	c.commit.dirty[scope] = c.OpID
	return nil
}

// FoldEpisodeScope folds an episode scope's contributions as they stand,
// publishing nothing: for a decision that depends on the fact's own effect.
func (c *ApplyContext) FoldEpisodeScope(scope string) (projection.EpisodeState, error) {
	cs, err := c.load(scope)
	if err != nil {
		return projection.EpisodeState{}, err
	}
	return projection.FoldEpisode(cs), nil
}

// Flush settles every refold deferred so far in this push, so records and the
// queue projection are current (queue decisions read them).
func (c *ApplyContext) Flush() error {
	if c.flush == nil {
		return nil
	}
	return c.flush()
}

// Random is the source of the server's choices in decisions.
func (c *ApplyContext) Random() Randomness { return c.commit.random }

// SettleLater defers the refold of the scopes this fact contributed to until
// the end of the push (one refold per scope, however many facts touched it) and
// reports applied. Only for facts whose outcome does not depend on the fold.
// Until then the deferred scopes' records are stale: a handler that reads
// records (rather than folding contributions) must not rely on them.
func (c *ApplyContext) SettleLater() Outcome {
	if c.commit.dirty == nil {
		c.commit.dirty = map[string]string{}
	}
	for _, scope := range c.scopes {
		c.commit.dirty[scope] = c.OpID
	}
	return Outcome{Result: "applied"}
}

// Settle recomputes every scope this fact contributed to, publishes changed
// records, and returns the fact's outcome: applied if any scope applied it,
// otherwise not_applied (with the first reason) or superseded.
func (c *ApplyContext) Settle() (Outcome, error) {
	var applied, superseded bool
	var reason string
	for _, scope := range c.scopes {
		statuses, err := c.recompute(scope)
		if err != nil {
			return Outcome{}, err
		}
		st, ok := statuses[c.OpID]
		switch {
		case !ok || st.Outcome == "applied":
			applied = true
		case st.Outcome == "superseded":
			superseded = true
		case reason == "":
			reason = st.Reason
		}
	}
	switch {
	case applied:
		return Outcome{Result: "applied"}, nil
	case superseded && reason == "":
		return Outcome{Result: "superseded"}, nil
	}
	return Outcome{Result: "not_applied", Reason: reason}, nil
}

func (c *ApplyContext) load(scope string) ([]projection.Contribution, error) {
	rows, err := c.Account.Contributions(scope)
	if err != nil {
		return nil, err
	}
	out := make([]projection.Contribution, len(rows))
	ranks := map[string]*projection.DecisionRank{}
	for i, r := range rows {
		var e projection.Effect
		if err := json.Unmarshal([]byte(r.Effect), &e); err != nil {
			return nil, err
		}
		// An admission's own rank orders two admissions of one scope (PROTOCOL §9.7 point 5), whatever
		// place their filings sit at.
		if err := c.rankAdmission(&e, ranks); err != nil {
			return nil, err
		}
		for k := range e.Compound {
			if err := c.rankAdmission(&e.Compound[k], ranks); err != nil {
				return nil, err
			}
		}
		out[i] = projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e}
	}
	return out, nil
}

// rankAdmission attaches an admission effect's decision rank (nil when the
// decision is not archived).
func (c *ApplyContext) rankAdmission(e *projection.Effect, ranks map[string]*projection.DecisionRank) error {
	if e.Kind != projection.KindAdmission {
		return nil
	}
	id, _ := e.Admission["admission_id"].(string)
	if id == "" {
		return nil
	}
	if r, ok := ranks[id]; ok {
		e.AdmissionRank = r
		return nil
	}
	d, err := c.Account.ArchivedDecision(id)
	if errors.Is(err, storage.ErrNotFound) {
		ranks[id] = nil
		return nil
	} else if err != nil {
		return err
	}
	r := &projection.DecisionRank{Epoch: d.Epoch, Seq: d.Seq, ID: id}
	ranks[id] = r
	e.AdmissionRank = r
	return nil
}

// recompute folds a scope and publishes the records it determines.
func (c *ApplyContext) recompute(scope string) (map[string]projection.Status, error) {
	switch {
	case strings.HasPrefix(scope, "episode:"):
		parts := strings.SplitN(strings.TrimPrefix(scope, "episode:"), ":", 3)
		// generation IDs and episode IDs both contain one ':' ("g1:…", "e1:…").
		generation := parts[0] + ":" + parts[1]
		episode := parts[2]
		return c.recomputeEpisode(generation, episode)
	case strings.HasPrefix(scope, "podcast:"):
		return c.recomputePodcast(strings.TrimPrefix(scope, "podcast:"))
	case strings.HasPrefix(scope, "item:"):
		return c.recomputeItem(strings.TrimPrefix(scope, "item:"))
	default:
		cs, err := c.load(AccountScope)
		if err != nil {
			return nil, err
		}
		st := projection.FoldAccount(cs)
		if st.DiscoveryPolicy != "" {
			if err := c.publish("account", c.AccountID, "discovery_policy", map[string]any{"new_episodes_action": st.DiscoveryPolicy}, []string{st.DiscoveryBy}); err != nil {
				return nil, err
			}
		}
		if st.EnqueueBy != "" || st.SortingBy != "" {
			location, keepSorted, order := st.QueuePolicy()
			if err := c.publish("account", c.AccountID, "queue_policy", map[string]any{"enqueue_location": location, "keep_sorted": keepSorted, "sort_order": order},
				[]string{st.EnqueueBy, st.SortingBy}); err != nil {
				return nil, err
			}
		}
		return st.Status, nil
	}
}

// liveGeneration returns the podcast's live generation ("" if none).
func (c *ApplyContext) liveGeneration(podcastID string) (string, error) {
	gens, err := c.Account.Generations(podcastID)
	if err != nil {
		return "", err
	}
	live := ""
	for _, g := range gens {
		if g.ClosedBy == "" {
			live = g.ID
		}
	}
	return live, nil
}

func (c *ApplyContext) recomputeEpisode(generation, episode string) (map[string]projection.Status, error) {
	scope := EpisodeScope(generation, episode)
	cs, err := c.load(scope)
	if err != nil {
		return nil, err
	}
	st := projection.FoldEpisode(cs)
	podcast, err := c.Account.EpisodePodcast(episode)
	if err != nil {
		return nil, err
	}
	live, err := c.liveGeneration(podcast)
	if err != nil {
		return nil, err
	}
	// Only the live generation's entries are ever members: records show it
	// alone (a closed one is hidden; an open one that is not live is a
	// partial recovery's state), so the published keys show what the records
	// show (the reference's reading).
	events := st.Events
	if live != generation {
		events = nil
	}
	if err := c.setQueueEvents(scope, events); err != nil {
		return nil, err
	}
	// Records always show the live generation's state of the episode.
	if live != generation {
		if live == "" {
			return st.Status, c.tombstoneEpisode(episode)
		}
		liveCS, err := c.load(EpisodeScope(live, episode))
		if err != nil {
			return nil, err
		}
		if err := c.publishEpisode(episode, live, projection.FoldEpisode(liveCS)); err != nil {
			return nil, err
		}
		return st.Status, nil
	}
	return st.Status, c.publishEpisode(episode, generation, st)
}

func (c *ApplyContext) publishEpisode(episode, generation string, st projection.EpisodeState) error {
	if st.Hidden {
		return c.tombstoneEpisode(episode)
	}
	var history any // null when every history entry is cut
	if st.History != nil {
		history = *st.History
	}
	groups := []struct {
		name  string
		has   bool
		value map[string]any
		by    []string
	}{
		{"play_state", st.HasPlayState, map[string]any{"generation_id": generation, "play_state": st.PlayState}, []string{st.PlayStateBy}},
		{"position", st.HasPosition, positionValue(generation, st), []string{st.PositionBy}},
		{"favorite", st.HasFavorite, map[string]any{"generation_id": generation, "on": st.Favorite}, []string{st.FavoriteBy}},
		{"last_played", st.HasLastPlayed, map[string]any{"generation_id": generation, "time": st.LastPlayed}, []string{st.LastPlayedBy}},
		{"history", st.HasHistory, map[string]any{"generation_id": generation, "time": history}, st.HistoryBy},
		{"played_time", st.HasPlayedTime, map[string]any{"generation_id": generation, "played_ms": st.PlayedTime}, st.PlayedTimeBy},
		{"measured_duration", st.HasDuration, map[string]any{"generation_id": generation, "duration_ms": st.Duration, "installation_id": st.DurationAuthor}, []string{st.DurationBy}},
		{"queue_membership", st.HasMembership, membershipValue(generation, st), []string{st.MembershipBy}},
		{"admission", st.Admission != nil, admissionValue(generation, st), []string{st.AdmissionBy}},
		{"statistics_units", st.HasUnits, unitsValue(generation, st), st.UnitsBy},
	}
	// The queue's keys (queue_entries, queue_position records) are the queue
	// publication's (PROTOCOL §7.6), at the end of the commit.
	for _, g := range groups {
		if !g.has {
			if err := c.tombstone("episode", episode, g.name); err != nil {
				return err
			}
			continue
		}
		if err := c.publish("episode", episode, g.name, g.value, g.by); err != nil {
			return err
		}
	}
	return nil
}

// episodeFieldGroups are the episode records a scope fold determines
// (queue_position is the queue publication's).
var episodeFieldGroups = []string{"play_state", "position", "favorite", "last_played", "history", "played_time", "measured_duration",
	"queue_membership", "admission", "statistics_units"}

// setQueueEvents records an episode scope's membership events as its fold
// found them; when they changed, the queue replay reruns from the earliest
// node whose event appeared, disappeared or flipped (PROTOCOL §7.6: a
// listening change that makes a removal fire or not, an automatic add whose
// applicability changes in either direction, a closed generation).
func (c *ApplyContext) setQueueEvents(scope string, events []projection.QueueEvent) error {
	old, err := c.Account.ScopeQueueEvents(scope)
	if err != nil {
		return err
	}
	was := make(map[string]bool, len(old))
	for _, e := range old {
		was[e.Node] = e.In
	}
	now := make(map[string]bool, len(events))
	for _, e := range events {
		now[e.Node] = e.In
	}
	var changed []string
	for n, in := range now {
		if prev, ok := was[n]; !ok || prev != in {
			changed = append(changed, n)
		}
	}
	for n := range was {
		if _, ok := now[n]; !ok {
			changed = append(changed, n)
		}
	}
	if len(changed) == 0 {
		return nil
	}
	rows := make([]storage.QueueEventRow, len(events))
	for i, e := range events {
		rows[i] = storage.QueueEventRow{Scope: scope, Node: e.Node, In: e.In}
	}
	if err := c.Account.SetScopeQueueEvents(scope, rows); err != nil {
		return err
	}
	c.QueueTouch(changed...)
	return nil
}

// unitsValue lists an episode's imported statistical units that a reset has
// not covered (PROTOCOL §11.5): each counts in statistics as its own
// episode would, by its own last-played date, history and duration.
func unitsValue(generation string, st projection.EpisodeState) map[string]any {
	list := []any{}
	for _, u := range st.Units {
		var history any
		if u.History != nil {
			history = *u.History
		}
		list = append(list, map[string]any{"id": u.ID, "played_ms": u.PlayedTime, "last_played": u.LastPlayed, "history": history,
			"duration_ms": u.Duration, "played": u.Played, "position": u.Position})
	}
	return map[string]any{"generation_id": generation, "units": list}
}

func admissionValue(generation string, st projection.EpisodeState) map[string]any {
	v := map[string]any{"generation_id": generation}
	for k, x := range st.Admission {
		v[k] = x
	}
	return v
}

func membershipValue(generation string, st projection.EpisodeState) map[string]any {
	v := map[string]any{"generation_id": generation, "in_queue": st.InQueue}
	if !st.InQueue && st.QueueReason != "" {
		v["reason"] = st.QueueReason
	}
	return v
}

func positionValue(generation string, st projection.EpisodeState) map[string]any {
	v := map[string]any{"generation_id": generation, "position": st.Position}
	if st.ClaimID != "" {
		v["claim_id"] = st.ClaimID
	}
	return v
}

func (c *ApplyContext) tombstoneEpisode(episode string) error {
	for _, g := range episodeFieldGroups {
		if err := c.tombstone("episode", episode, g); err != nil {
			return err
		}
	}
	return nil
}

// recomputeItem publishes an episode's canonical metadata (PROTOCOL §9.4).
func (c *ApplyContext) recomputeItem(episode string) (map[string]projection.Status, error) {
	cs, err := c.load(ItemScope(episode))
	if err != nil {
		return nil, err
	}
	return map[string]projection.Status{}, c.publishCatalog("episode", episode, cs)
}

// publishCatalog publishes a canonical metadata record: the folded value with
// its revision and fetch time.
func (c *ApplyContext) publishCatalog(entity, id string, cs []projection.Contribution) error {
	rec, ok := projection.FoldCatalog(cs)
	if !ok {
		return nil
	}
	v := map[string]any{}
	for k, x := range rec.Value {
		v[k] = x
	}
	v["revision"], v["fetch_time"] = rec.Revision, rec.FetchTime
	return c.publish(entity, id, "metadata", v, []string{rec.Revision})
}

func (c *ApplyContext) recomputePodcast(podcast string) (map[string]projection.Status, error) {
	all, err := c.load(PodcastScope(podcast))
	if err != nil {
		return nil, err
	}
	// The catalog records (PROTOCOL §9.4) fold apart from the subscription state.
	var cs, catalog []projection.Contribution
	for _, x := range all {
		if projection.IsCatalogKind(x.Effect.Kind) {
			catalog = append(catalog, x)
		} else {
			cs = append(cs, x)
		}
	}
	if err := c.publishCatalog("podcast", podcast, catalog); err != nil {
		return nil, err
	}
	if v, by := projection.LastRefreshStatus(catalog); v != nil {
		if err := c.publish("podcast", podcast, "refresh_status", v, []string{by}); err != nil {
			return nil, err
		}
	}
	st := projection.FoldPodcast(cs)
	// A merged podcast (PROTOCOL §9.7) is only a redirect to its survivor.
	if m, merged, err := c.Account.PodcastMergeOf(podcast); err != nil {
		return nil, err
	} else if merged {
		if err := c.publish("podcast", podcast, "subscription", map[string]any{"state": "merged", "merged_into": m.Into}, []string{m.Node}); err != nil {
			return nil, err
		}
		for _, g := range []string{"policy", "credentials"} {
			if err := c.tombstone("podcast", podcast, g); err != nil {
				return nil, err
			}
		}
		return st.Status, nil
	}
	aliases, err := c.Account.Aliases(podcast)
	if err != nil {
		return nil, err
	}
	list := []any{}
	for _, a := range aliases {
		list = append(list, map[string]any{"url": a.Raw, "normalized": a.Normalized})
	}
	refreshURL, refreshBy := st.RefreshURL()
	identity := map[string]any{"aliases": list, "refresh_url": refreshURL}
	prov := []string{}
	if refreshBy != "" {
		prov = append(prov, refreshBy)
	}
	if err := c.publish("podcast", podcast, "identity", identity, prov); err != nil {
		return nil, err
	}
	if st.Live == nil {
		latest := ""
		if st.Latest != nil {
			latest = st.Latest.ID
		}
		if err := c.publish("podcast", podcast, "subscription", map[string]any{"state": "removed", "generation_id": latest}, []string{}); err != nil {
			return nil, err
		}
		for _, g := range []string{"policy", "credentials"} {
			if err := c.tombstone("podcast", podcast, g); err != nil {
				return nil, err
			}
		}
		return st.Status, nil
	}
	g := st.Live
	if err := c.publish("podcast", podcast, "subscription", map[string]any{"state": g.State, "generation_id": g.ID}, []string{}); err != nil {
		return nil, err
	}
	policyProv := []string{}
	for _, n := range g.Policy {
		policyProv = append(policyProv, n)
	}
	if err := c.publish("podcast", podcast, "policy", map[string]any{"new_episodes_action": g.NewEpisodes, "keep_updated": g.KeepUpdated}, policyProv); err != nil {
		return nil, err
	}
	if g.SecretID != "" {
		if err := c.publish("podcast", podcast, "credentials", map[string]any{"secret_id": g.SecretID}, []string{g.SecretBy}); err != nil {
			return nil, err
		}
	} else if err := c.tombstone("podcast", podcast, "credentials"); err != nil {
		return nil, err
	}
	return st.Status, nil
}

// publish stores a record if it changed and adds it to the commit.
func (c *ApplyContext) publish(entity, id, group string, value map[string]any, provenance []string) error {
	v, err := canonicalString(value)
	if err != nil {
		return err
	}
	sorted := slices.Clone(provenance)
	slices.Sort(sorted)
	prov := make([]any, 0, len(sorted))
	seen := map[string]bool{}
	for _, p := range sorted {
		if p != "" && !seen[p] {
			seen[p] = true
			prov = append(prov, p)
		}
	}
	p, err := canonicalString(prov)
	if err != nil {
		return err
	}
	old, err := c.Account.Record(entity, id, group)
	if err == nil && old.Value == v && old.Provenance == p {
		return nil
	}
	if err != nil && err != storage.ErrNotFound {
		return err
	}
	had := err == nil
	if err := c.Account.PutRecord(storage.RecordRow{Entity: entity, EntityID: id, FieldGroup: group, Value: v, Provenance: p}); err != nil {
		return err
	}
	if entity == "podcast" && group == "subscription" && opensGeneration(value, old.Value, had) {
		c.Account.GenerationOpened()
	}
	decoded, _ := contract.DecodeJSON([]byte(v))
	provList := []string{}
	for _, x := range prov {
		provList = append(provList, x.(string))
	}
	c.commit.record(Record{Entity: entity, ID: id, FieldGroup: group, Value: decoded, Provenance: provList})
	return nil
}

// opensGeneration reports a subscription record naming a live generation the
// record did not name before: a subscription or a registration, whose first
// population the refresher fetches at once (PROTOCOL §9.4).
func opensGeneration(value map[string]any, old string, had bool) bool {
	gen, _ := value["generation_id"].(string)
	switch value["state"] {
	case "subscribed", "archived", "not_subscribed":
	default:
		return false
	}
	if gen == "" || !had {
		return gen != ""
	}
	prev, err := contract.DecodeJSON([]byte(old))
	m, ok := prev.(map[string]any)
	return err != nil || !ok || m["generation_id"] != gen
}

// tombstone deletes a live record and publishes its tombstone if it existed.
func (c *ApplyContext) tombstone(entity, id, group string) error {
	existed, err := c.Account.DeleteRecord(entity, id, group)
	if err != nil || !existed {
		return err
	}
	c.commit.record(Record{Entity: entity, ID: id, FieldGroup: group, Tombstone: true, Provenance: []string{c.OpID}})
	return nil
}

func canonicalString(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	d, err := contract.DecodeJSON(b)
	if err != nil {
		return "", err
	}
	out, err := contract.Canonical(d)
	return string(out), err
}

// PublishDerived publishes a record a handler derives from state other than
// one fact, with the given provenance (the nodes that determine it).
func (c *ApplyContext) PublishDerived(entity, id, group string, value map[string]any, provenance []string) error {
	return c.publish(entity, id, group, value, provenance)
}

// PublishRecord publishes a record a handler determines directly (an episode's
// identity on first sight), with this fact (or decision) as its provenance.
// Publishing the value the record already holds changes nothing: the node
// that first published the value keeps it, so the provenance does not depend
// on how often an unchanged value is published again (a rebuild admits a fetch's decisions one by
// one).
func (c *ApplyContext) PublishRecord(entity, id, group string, value map[string]any) error {
	v, err := canonicalString(value)
	if err != nil {
		return err
	}
	if old, err := c.Account.Record(entity, id, group); err == nil && old.Value == v {
		return nil
	} else if err != nil && err != storage.ErrNotFound {
		return err
	}
	return c.publish(entity, id, group, value, []string{c.OpID})
}

// mustDecode gives a value the shapes contract.Canonical accepts.
func mustDecode(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	out, err := contract.DecodeJSON(b)
	if err != nil {
		return v
	}
	return out
}

// scopeEpisode is the episode of an episode or item scope ("" otherwise).
func scopeEpisode(scope string) string {
	if rest, ok := strings.CutPrefix(scope, "item:"); ok {
		return rest
	}
	rest, ok := strings.CutPrefix(scope, "episode:")
	if !ok {
		return ""
	}
	// generation IDs and episode IDs both hold one ':' ("g1:…", "e1:…").
	if i := strings.Index(rest, ":"); i >= 0 {
		if j := strings.Index(rest[i+1:], ":"); j >= 0 {
			return rest[i+1+j+1:]
		}
	}
	return ""
}
