package ops

import (
	"errors"
	"sort"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Register adds every operation type (version 1) of subscriptions, episodes, listening and the
// queue.
func Register(r *protocol.Registry) {
	RegisterListening(r)
	RegisterQueue(r)
	RegisterCatalog(r)
	r.Register("podcast.subscribe", 1, resolveOp{preview: false})
	r.Register("podcast.register_preview", 1, resolveOp{preview: true})
	r.Register("podcast.archive", 1, lifecycleOp{kind: projection.KindArchive})
	r.Register("podcast.restore", 1, lifecycleOp{kind: projection.KindRestore})
	r.Register("podcast.unsubscribe", 1, lifecycleOp{kind: projection.KindUnsubscribe})
	r.Register("podcast.set_policy", 1, lifecycleOp{kind: projection.KindSetPolicy})
	r.Register("podcast.set_credentials", 1, lifecycleOp{kind: projection.KindSetCredentials})
	r.Register("podcast.change_url", 1, lifecycleOp{kind: projection.KindChangeURL})
	r.Register("episode.mark_played", 1, episodeOp{kind: projection.KindMarkPlayed})
	r.Register("episode.mark_unplayed", 1, episodeOp{kind: projection.KindMarkUnplayed})
	r.Register("episode.reset_position", 1, episodeOp{kind: projection.KindResetPosition})
	r.Register("episode.restore_state", 1, episodeOp{kind: projection.KindRestoreState})
	r.Register("favorite.set", 1, episodeOp{kind: projection.KindFavorite, capability: "favorites"})
	r.Register("inbox.dismiss", 1, episodeOp{kind: projection.KindDismiss, capability: "inbox", bulk: true})
	r.Register("inbox.dismiss_all", 1, episodeOp{kind: projection.KindDismiss, capability: "inbox", bulk: true})
	r.Register("account.set_discovery_policy", 1, accountOp{})
	r.Register("installation.fork", 1, forkOp{})
	registerAdmits(r)
	registerRecovery(r)
}

var newEpisodesActions = map[string]bool{"global": true, "inbox": true, "queue": true, "nothing": true}

// ---- subscribe and register_preview

type resolveOp struct{ preview bool }

func (resolveOp) Capability() string { return "" }

type observation struct {
	PodcastID, GenerationID, State string
}

func (o resolveOp) parse(f *protocol.Fact) (feedURL string, obs *observation, policy *projection.Effect, secret string, err error) {
	allowed := []string{"observed"}
	if !o.preview {
		allowed = append(allowed, "policy", "credentials_secret")
	}
	if err = fields(f.Payload, []string{"feed_url"}, allowed...); err != nil {
		return
	}
	feedURL, ok := str(f.Payload, "feed_url")
	if !ok || feedURL == "" {
		err = invalidf("feed_url must be a non-empty string")
		return
	}
	if raw, present := f.Payload["observed"]; present {
		m, ok := object(raw)
		if !ok {
			err = invalidf("observed must be an object")
			return
		}
		if err = fields(m, []string{"podcast_id", "generation_id", "state"}); err != nil {
			return
		}
		obs = &observation{}
		obs.PodcastID, _ = str(m, "podcast_id")
		obs.GenerationID, _ = str(m, "generation_id")
		obs.State, _ = str(m, "state")
		if !prefixedHex(obs.PodcastID, "p1:") || !prefixedHex(obs.GenerationID, "g1:") ||
			!map[string]bool{"not_subscribed": true, "subscribed": true, "archived": true, "removed": true}[obs.State] {
			err = invalidf("bad observed")
			return
		}
	}
	if raw, present := f.Payload["policy"]; present {
		m, ok := object(raw)
		if !ok {
			err = invalidf("policy must be an object")
			return
		}
		// A subscribe's initial policy is complete (PROTOCOL §7.2), unlike set_policy.
		if err = fields(m, []string{"new_episodes_action", "keep_updated"}); err != nil {
			return
		}
		if policy, err = parsePolicy(m); err != nil {
			return
		}
	}
	if raw, present := f.Payload["credentials_secret"]; present {
		if secret, ok = raw.(string); !ok || !contract.ValidUUID(secret) {
			err = invalidf("credentials_secret must be a UUID")
		}
	}
	return
}

func parsePolicy(m map[string]any) (*projection.Effect, error) {
	if err := fields(m, nil, "new_episodes_action", "keep_updated"); err != nil {
		return nil, err
	}
	e := &projection.Effect{}
	if raw, present := m["new_episodes_action"]; present {
		s, ok := raw.(string)
		if !ok || !newEpisodesActions[s] {
			return nil, invalidf("bad new_episodes_action")
		}
		e.NewEpisodes = &s
	}
	if raw, present := m["keep_updated"]; present {
		b, ok := boolean(raw)
		if !ok {
			return nil, invalidf("keep_updated must be a boolean")
		}
		e.KeepUpdated = &b
	}
	if e.NewEpisodes == nil && e.KeepUpdated == nil {
		return nil, invalidf("policy changes nothing")
	}
	return e, nil
}

func (o resolveOp) Validate(f *protocol.Fact) error {
	_, _, _, _, err := o.parse(f)
	if err != nil {
		return err
	}
	// Targets: none, or the one podcast the device knows.
	if len(f.Targets) > 1 || (len(f.Targets) == 1 && f.Targets[0].Entity != "podcast") {
		return invalidf("targets must be empty or the podcast")
	}
	return nil
}

// Prerequisites names the secret only for a valid payload: a malformed
// reference is rejected (invalid_payload) rather than waited for forever.
func (o resolveOp) Prerequisites(f *protocol.Fact) []string {
	if _, _, _, secret, err := o.parse(f); err == nil && secret != "" {
		return []string{secret}
	}
	return nil
}

func (o resolveOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	feedURL, obs, policy, secret, err := o.parse(f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	acct := ctx.Account
	if obs != nil {
		// Reference CHOICE: an observed merged-away podcast is read as
		// the survivor's, by the merge's generation routing (a joined
		// generation is the survivor's live class; a closed one stays the
		// source's closed generation).
		p, g := obs.PodcastID, obs.GenerationID
		for seen := map[string]bool{}; !seen[p]; {
			seen[p] = true
			m, merged, err := acct.PodcastMergeStep(p)
			if err != nil {
				return protocol.Outcome{}, err
			}
			if !merged {
				obs.PodcastID, obs.GenerationID = p, g
				break
			}
			mode, g2 := mergeMode(&m, g)
			if mode == "closed" {
				break
			}
			p, g = m.Into, g2
		}
	}
	normalized := contract.NormalizeURL(feedURL)
	if ctx.RecoveryReplay() {
		// Recovery (PROTOCOL §6.11): a registration takes the archive's effective
		// resolution, made whenever it was; without one it waits for it and
		// contributes nothing now.
		rec, ok := ctx.RecordedDecision("podcast.resolution", f.OpID)
		if !ok {
			if ctx.Fixed != nil {
				return *ctx.Fixed, nil
			}
			return protocol.Outcome{Result: "applied"}, nil
		}
		return o.applyResolution(ctx, f, feedURL, normalized, obs, policy, secret, rec)
	}
	podcast, err := acct.PodcastByAlias(normalized)
	if err != nil {
		return protocol.Outcome{}, err
	}
	mode, generation, predecessor := "", "", ""
	if podcast == "" {
		mode = "created"
		podcast = contract.PodcastID(ctx.Namespace, f.OpID)
		generation = contract.GenerationID(ctx.Namespace, podcast, f.OpID)
		if err := acct.CreatePodcast(podcast, f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
		if err := acct.AddAlias(normalized, podcast, feedURL, "subscribe", f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
		if _, err := ctx.Decide(protocol.DecisionInput{Type: "podcast.alias", Version: 1, Subject: "alias:" + normalized,
			Dependencies: []string{f.OpID}, Result: map[string]any{"podcast_id": podcast}}); err != nil {
			return protocol.Outcome{}, err
		}
	} else {
		gens, err := acct.Generations(podcast)
		if err != nil {
			return protocol.Outcome{}, err
		}
		// Generations form a chain through their predecessors (acceptance
		// order): the latest is the one no other generation follows.
		var live, latest *storage.GenerationRow
		followed := map[string]bool{}
		for _, g := range gens {
			if g.Predecessor != "" {
				followed[g.Predecessor] = true
			}
		}
		for i := range gens {
			if gens[i].ClosedBy == "" {
				live = &gens[i]
			}
			if !followed[gens[i].ID] {
				latest = &gens[i]
			}
		}
		var latestClosed *storage.GenerationRow
		if latest != nil && latest.ClosedBy != "" {
			latestClosed = latest
		}
		samePodcast := obs == nil || obs.PodcastID == podcast
		switch {
		case live != nil && samePodcast && (obs == nil || obs.GenerationID == live.ID || (obs.GenerationID == live.Predecessor && obs.State == "removed")):
			mode, generation = "joined", live.ID
		case live == nil && latestClosed != nil && obs != nil && samePodcast && obs.GenerationID == latestClosed.ID && obs.State == "removed":
			mode, predecessor = "opened", latestClosed.ID
			generation = contract.GenerationID(ctx.Namespace, podcast, f.OpID)
		default:
			mode = "stale"
			// For information: the live generation, else the latest closed one.
			if live != nil {
				generation = live.ID
			} else if latestClosed != nil {
				generation = latestClosed.ID
			}
		}
	}
	if mode == "created" || mode == "opened" {
		if err := acct.CreateGeneration(generation, podcast, f.OpID, predecessor); err != nil {
			return protocol.Outcome{}, err
		}
	}
	result := map[string]any{"podcast_id": podcast, "mode": mode}
	if generation != "" {
		result["generation_id"] = generation
	}
	if _, err := ctx.Decide(protocol.DecisionInput{Type: "podcast.resolution", Version: 1, Subject: f.OpID,
		Dependencies: []string{f.OpID}, Result: result}); err != nil {
		return protocol.Outcome{}, err
	}
	e := projection.Effect{Kind: projection.KindResolve, Generation: generation, Mode: mode, Preview: o.preview, Predecessor: predecessor}
	if mode == "created" || mode == "opened" {
		e.RefreshURL = feedURL
	}
	if policy != nil && (mode == "created" || mode == "opened") {
		e.NewEpisodes, e.KeepUpdated = policy.NewEpisodes, policy.KeepUpdated
	}
	if secret != "" {
		e.SecretID = &secret
	}
	if err := ctx.Contribute(protocol.PodcastScope(podcast), e); err != nil {
		return protocol.Outcome{}, err
	}
	return ctx.Settle()
}

// applyResolution applies a registration with a recorded resolution (a
// recovery rebuild, PROTOCOL §6.11): what the resolution created or opened exists
// (the first claim of the feed URL keeps it; a later creation is unified at
// the end of the recovery request), its alias and resolution decisions are
// made again, and the registration contributes as a live one would.
func (o resolveOp) applyResolution(ctx *protocol.ApplyContext, f *protocol.Fact, feedURL, normalized string, obs *observation,
	policy *projection.Effect, secret string, rec map[string]any) (protocol.Outcome, error) {
	acct := ctx.Account
	res, _ := object(rec["result"])
	mode, _ := str(res, "mode")
	podcast, _ := str(res, "podcast_id")
	generation, _ := str(res, "generation_id")
	predecessor := ""
	if mode == "opened" && obs != nil {
		predecessor = obs.GenerationID
	}
	if mode == "created" {
		exists, err := acct.PodcastExists(podcast)
		if err != nil {
			return protocol.Outcome{}, err
		}
		if !exists {
			if err := acct.CreatePodcast(podcast, f.OpID); err != nil {
				return protocol.Outcome{}, err
			}
		}
		if owner, err := acct.PodcastByAlias(normalized); err != nil {
			return protocol.Outcome{}, err
		} else if owner == "" {
			if err := acct.AddAlias(normalized, podcast, feedURL, "subscribe", f.OpID); err != nil {
				return protocol.Outcome{}, err
			}
		}
		if _, err := ctx.Decide(protocol.DecisionInput{Type: "podcast.alias", Version: 1, Subject: "alias:" + normalized,
			Dependencies: []string{f.OpID}, Result: map[string]any{"podcast_id": podcast}}); err != nil {
			return protocol.Outcome{}, err
		}
	}
	if mode == "created" || mode == "opened" {
		if _, err := acct.Generation(generation); errors.Is(err, storage.ErrNotFound) {
			if err := acct.CreateGeneration(generation, podcast, f.OpID, predecessor); err != nil {
				return protocol.Outcome{}, err
			}
		} else if err != nil {
			return protocol.Outcome{}, err
		}
	}
	if _, err := ctx.Decide(protocol.DecisionInput{Type: "podcast.resolution", Version: 1, Subject: f.OpID,
		Dependencies: []string{f.OpID}, Result: res}); err != nil {
		return protocol.Outcome{}, err
	}
	if exists, err := acct.PodcastExists(podcast); err != nil {
		return protocol.Outcome{}, err
	} else if !exists {
		// A podcast the archive does not create (yet): nothing to join.
		if ctx.Fixed != nil {
			return *ctx.Fixed, nil
		}
		return protocol.Outcome{Result: "applied"}, nil
	}
	if mode == "joined" && generation != "" {
		// The generation's current podcast (a merge may have rehomed it).
		if g, err := acct.Generation(generation); err == nil {
			podcast = g.PodcastID
		} else if !errors.Is(err, storage.ErrNotFound) {
			return protocol.Outcome{}, err
		}
	}
	e := projection.Effect{Kind: projection.KindResolve, Generation: generation, Mode: mode, Preview: o.preview, Predecessor: predecessor}
	if mode == "created" || mode == "opened" {
		e.RefreshURL = feedURL
	}
	if policy != nil && (mode == "created" || mode == "opened") {
		e.NewEpisodes, e.KeepUpdated = policy.NewEpisodes, policy.KeepUpdated
	}
	if secret != "" {
		e.SecretID = &secret
	}
	if err := ctx.Contribute(protocol.PodcastScope(podcast), e); err != nil {
		return protocol.Outcome{}, err
	}
	return ctx.Settle()
}

// ---- podcast lifecycle, policy, credentials and URL

type lifecycleOp struct{ kind string }

func (lifecycleOp) Capability() string { return "" }

func (o lifecycleOp) payload(f *protocol.Fact) (projection.Effect, string, error) {
	e := projection.Effect{Kind: o.kind}
	url := ""
	switch o.kind {
	case projection.KindArchive, projection.KindRestore, projection.KindUnsubscribe:
		if err := fields(f.Payload, nil); err != nil {
			return e, "", err
		}
	case projection.KindSetPolicy:
		p, err := parsePolicy(f.Payload)
		if err != nil {
			return e, "", err
		}
		e.NewEpisodes, e.KeepUpdated = p.NewEpisodes, p.KeepUpdated
	case projection.KindSetCredentials:
		if _, clear := f.Payload["clear"]; clear {
			if err := fields(f.Payload, []string{"clear"}); err != nil {
				return e, "", err
			}
			if b, ok := boolean(f.Payload["clear"]); !ok || !b {
				return e, "", invalidf("clear must be true")
			}
			empty := ""
			e.SecretID = &empty
		} else {
			if err := fields(f.Payload, []string{"secret_id"}); err != nil {
				return e, "", err
			}
			s, ok := str(f.Payload, "secret_id")
			if !ok || !contract.ValidUUID(s) {
				return e, "", invalidf("secret_id must be a UUID")
			}
			e.SecretID = &s
		}
	case projection.KindChangeURL:
		if err := fields(f.Payload, []string{"url"}); err != nil {
			return e, "", err
		}
		u, ok := str(f.Payload, "url")
		if !ok || u == "" {
			return e, "", invalidf("url must be a non-empty string")
		}
		url = u
		e.RefreshURL = u
	}
	return e, url, nil
}

func (o lifecycleOp) Validate(f *protocol.Fact) error {
	if _, _, err := o.payload(f); err != nil {
		return err
	}
	if len(f.Targets) != 1 || f.Targets[0].Entity != "podcast" || !prefixedHex(f.Targets[0].ID, "p1:") || !prefixedHex(f.Targets[0].Generation, "g1:") {
		return invalidf("targets must be the podcast with its generation")
	}
	return nil
}

func (o lifecycleOp) Prerequisites(f *protocol.Fact) []string {
	if e, _, err := o.payload(f); err == nil && e.SecretID != nil && *e.SecretID != "" {
		return []string{*e.SecretID}
	}
	return nil
}

func (o lifecycleOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	e, url, err := o.payload(f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	podcast, generation := f.Targets[0].ID, f.Targets[0].Generation
	acct := ctx.Account
	// A provisional generation joined into the effective one acts there (PROTOCOL §6.11).
	generation = newView(ctx).joined(generation)
	if fx := ctx.Fixed; fx != nil && fx.Result == "not_applied" && (fx.Reason == "generation_merged" || fx.Reason == "alias_conflict" ||
		fx.Reason == "generation_closed") {
		// Recovery (PROTOCOL §6.11; reference CHOICE): a receipt that refused the fact
		// decides, wherever recovery admits the merge or the closing fact.
		return *fx, nil
	}
	if exists, err := acct.PodcastExists(podcast); err != nil || !exists {
		return protocol.Outcome{Result: "not_applied", Reason: "unknown_podcast"}, err
	}
	g, err := acct.Generation(generation)
	if errors.Is(err, storage.ErrNotFound) {
		return protocol.Outcome{Result: "not_applied", Reason: "unknown_generation"}, nil
	}
	if err != nil {
		return protocol.Outcome{}, err
	}
	if g.PodcastID != podcast {
		// A generation a podcast merge rehomed stays one of its source's.
		if m, merged, err := acct.PodcastMergeStep(podcast); err != nil {
			return protocol.Outcome{}, err
		} else if !merged || m.FromGeneration != generation || m.IntoGeneration != generation {
			return protocol.Outcome{Result: "not_applied", Reason: "unknown_generation"}, nil
		}
	}
	// A merged podcast (PROTOCOL §9.7), through its merge chain: a closed source
	// generation is closed; one joined into the survivor's live class answers
	// generation_merged (a lifecycle fact naming it cannot close that class);
	// a rehomed one is the survivor's live generation.
	for seen := map[string]bool{}; !seen[podcast]; {
		seen[podcast] = true
		m, merged, err := acct.PodcastMergeStep(podcast)
		if err != nil {
			return protocol.Outcome{}, err
		}
		if !merged {
			break
		}
		switch mode, _ := mergeMode(&m, generation); mode {
		case "closed":
			return protocol.Outcome{Result: "not_applied", Reason: "generation_closed"}, nil
		case "joined":
			return protocol.Outcome{Result: "not_applied", Reason: "generation_merged"}, nil
		}
		podcast = m.Into
	}
	if g.ClosedBy != "" {
		return protocol.Outcome{Result: "not_applied", Reason: "generation_closed"}, nil
	}
	e.Generation = generation
	if o.kind == projection.KindChangeURL {
		normalized := contract.NormalizeURL(url)
		owner, err := acct.PodcastByAlias(normalized)
		if err != nil {
			return protocol.Outcome{}, err
		}
		if owner != "" && owner != podcast && ctx.Fixed == nil {
			return protocol.Outcome{Result: "not_applied", Reason: "alias_conflict"}, nil
		}
		if owner == "" {
			if err := acct.AddAlias(normalized, podcast, url, "change_url", f.OpID); err != nil {
				return protocol.Outcome{}, err
			}
			e.Mode = "claimed" // a newly claimed alias is always applied
		}
	}
	if err := ctx.Contribute(protocol.PodcastScope(podcast), e); err != nil {
		return protocol.Outcome{}, err
	}
	if o.kind == projection.KindUnsubscribe {
		if err := acct.CloseGeneration(generation, f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
		scopes, err := acct.ScopesWithPrefix("episode:" + generation + ":")
		if err != nil {
			return protocol.Outcome{}, err
		}
		for _, s := range scopes {
			if err := ctx.Contribute(s, projection.Effect{Kind: projection.KindClose, Generation: generation}); err != nil {
				return protocol.Outcome{}, err
			}
		}
	}
	return ctx.Settle()
}

// ---- episode state, favorites, Inbox

type episodeOp struct {
	kind       string
	capability string
	bulk       bool
}

func (o episodeOp) Capability() string { return o.capability }

type episodePayload struct {
	refs          []any
	resetPosition bool
	playState     string
	position      int64
	on            bool
}

func (o episodeOp) parse(f *protocol.Fact) (episodePayload, error) {
	var p episodePayload
	switch {
	case o.bulk:
		if err := fields(f.Payload, []string{"episodes"}); err != nil {
			return p, err
		}
		list, ok := f.Payload["episodes"].([]any)
		if !ok || len(list) == 0 {
			return p, invalidf("episodes must be a non-empty list")
		}
		p.refs = list
	case o.kind == projection.KindMarkPlayed:
		if err := fields(f.Payload, []string{"episode", "reset_position"}); err != nil {
			return p, err
		}
		b, ok := boolean(f.Payload["reset_position"])
		if !ok {
			return p, invalidf("reset_position must be a boolean")
		}
		p.resetPosition = b
	case o.kind == projection.KindRestoreState:
		if err := fields(f.Payload, []string{"episode", "play_state", "position"}); err != nil {
			return p, err
		}
		s, ok := str(f.Payload, "play_state")
		if !ok || (s != "new" && s != "unplayed" && s != "played") {
			return p, invalidf("bad play_state")
		}
		n, ok := integer(f.Payload["position"])
		if !ok || n < 0 {
			return p, invalidf("position must be a non-negative integer")
		}
		p.playState, p.position = s, n
	case o.kind == projection.KindFavorite:
		if err := fields(f.Payload, []string{"episode", "on"}); err != nil {
			return p, err
		}
		b, ok := boolean(f.Payload["on"])
		if !ok {
			return p, invalidf("on must be a boolean")
		}
		p.on = b
	default:
		if err := fields(f.Payload, []string{"episode"}); err != nil {
			return p, err
		}
	}
	if !o.bulk {
		p.refs = []any{f.Payload["episode"]}
	}
	return p, nil
}

func (o episodeOp) Validate(f *protocol.Fact) error {
	_, err := o.parse(f)
	return err
}

func (o episodeOp) refs(namespace string, f *protocol.Fact) ([]EpisodeRef, episodePayload, error) {
	p, err := o.parse(f)
	if err != nil {
		return nil, p, err
	}
	var refs []EpisodeRef
	var targets []protocol.Target
	for _, raw := range p.refs {
		r, err := parseEpisodeRef(raw, namespace)
		if err != nil {
			return nil, p, err
		}
		if len(refs) > 0 && refs[len(refs)-1].EpisodeID >= r.EpisodeID {
			return nil, p, invalidf("episodes must be sorted by episode_id without repeats")
		}
		refs = append(refs, r)
		targets = append(targets, protocol.Target{Entity: "episode", ID: r.EpisodeID, Generation: r.GenerationID})
	}
	if err := checkTargets(f, targets); err != nil {
		return nil, p, err
	}
	return refs, p, nil
}

func (o episodeOp) ValidateInAccount(account protocol.AccountInfo, f *protocol.Fact) error {
	_, _, err := o.refs(account.Namespace, f)
	return err
}

func (o episodeOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	refs, p, err := o.refs(ctx.Namespace, f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	acct := ctx.Account
	firstReason := ""
	contributed := 0
	for _, r := range refs {
		reason, err := checkEpisode(ctx, &r)
		if err != nil {
			return protocol.Outcome{}, err
		}
		if reason != "" {
			if firstReason == "" {
				firstReason = reason
			}
			continue
		}
		e := projection.Effect{Kind: o.kind}
		switch o.kind {
		case projection.KindMarkPlayed:
			e.ResetPosition = p.resetPosition
		case projection.KindRestoreState:
			pos := p.position
			e.PlayState, e.Position = p.playState, &pos
		case projection.KindFavorite:
			on := p.on
			e.On = &on
		}
		if err := ctx.Contribute(protocol.EpisodeScope(r.GenerationID, r.EpisodeID), withRef(e, r)); err != nil {
			return protocol.Outcome{}, err
		}
		contributed++
	}
	_ = acct
	if contributed == 0 {
		return protocol.Outcome{Result: "not_applied", Reason: firstReason}, nil
	}
	return ctx.Settle()
}

// checkEpisode applies the acceptance checks of PROTOCOL §7.2 and registers a new
// episode with its occurrence evidence (publishing its identity record).
//
// The reference is first routed (PROTOCOL §9.7, the reference's _check_ref): the
// checks on what it names, then podcast merges, merge redirects and the
// server's derivation (lookup aliases, the disputed-GUID resolver), then the
// checks on where it goes. A passing fact registers both the episode it names
// and the one it is applied to, if new; a passing reference to a merged-away
// podcast redirects the episode it names through the merges.
func checkEpisode(ctx *protocol.ApplyContext, r *EpisodeRef) (string, error) {
	v := newView(ctx)
	routeReason, reason, err := routeRef(v, r)
	if err != nil {
		return "", err
	}
	if r.Original != "" {
		ctx.Resolved = true
	}
	if routeReason != "" {
		return routeReason, nil
	}
	if reason != "" {
		return reason, nil
	}
	if fx := ctx.Fixed; fx != nil && fx.Result == "not_applied" && fx.Reason == "generation_closed" {
		// Recovery (PROTOCOL §6.11; reference CHOICE): a receipt that refused the
		// fact as generation_closed decides, whatever this order says.
		return fx.Reason, nil
	}
	// A passing reference to a merged-away podcast redirects the episode it
	// names through the merges (only a passing fact, so that what registers
	// does not depend on whether the merge was admitted before the fact).
	if _, merged, err := ctx.Account.PodcastMergeStep(r.NamedPodcast); err != nil {
		return "", err
	} else if merged {
		if err := walkMerges(ctx, r.NamedEpisode, r.NamedPodcast, r.Occurrence, ctx.OpID); err != nil {
			return "", err
		}
	}
	aliased := false
	regs := [][2]string{{r.NamedEpisode, r.NamedPodcast}, {r.EpisodeID, r.PodcastID}}
	if r.NamedEpisode == r.EpisodeID {
		// An episode the reference names where it goes is the routed
		// podcast's: a reference naming either podcast of a merge family is
		// its own podcast's.
		regs = regs[1:]
	}
	for _, x := range regs {
		a, err := registerEpisode(ctx, x[0], x[1], r.Occurrence)
		if err != nil {
			return "", err
		}
		aliased = aliased || a
	}
	if aliased {
		// A registration added a lookup alias: every contribution, and this
		// reference, go where it routes now.
		if err := placeAll(ctx, ctx.OpID, r.PodcastID); err != nil {
			return "", err
		}
		r.EpisodeID, r.PodcastID, r.GenerationID = r.NamedEpisode, r.NamedPodcast, r.NamedGeneration
		r.named, r.Original = false, ""
		if _, _, err := routeRef(newView(ctx), r); err != nil {
			return "", err
		}
		if r.Original != "" {
			ctx.Resolved = true
		}
	}
	return "", nil
}

// episodeReason is checkEpisode's read-only part: the not_applied reason, if
// any, and whether the episode it is applied to is already known.
func episodeReason(acct *storage.AccountTx, namespace string, r *EpisodeRef) (string, bool, error) {
	v := newViewOf(acct, namespace)
	routeReason, reason, err := routeRef(v, r)
	if err != nil {
		return "", false, err
	}
	if routeReason != "" {
		return routeReason, false, nil
	}
	known, err := acct.EpisodePodcast(r.EpisodeID)
	if err != nil {
		return "", false, err
	}
	return reason, known != "", nil
}

// joinedInto reports whether a podcast merge into p joined a source
// generation into g.
func joinedInto(acct *storage.AccountTx, p, g string) (bool, error) {
	srcs, err := acct.GenerationsJoinedInto(g)
	if err != nil {
		return false, err
	}
	for _, s := range srcs {
		row, err := acct.Generation(s)
		if err != nil {
			return false, err
		}
		if m, merged, err := acct.PodcastMergeStep(row.PodcastID); err != nil {
			return false, err
		} else if merged && m.Into == p {
			return true, nil
		}
	}
	return false, nil
}

// routeRef routes a reference in place (the reference's _route_inplace
// and _ref_reason), keeping what it named: routeReason is a failure of the
// checks on what it names or of the routing (a closed source generation);
// reason a failure of the checks on where it goes.
func routeRef(v *idView, r *EpisodeRef) (routeReason, reason string, err error) {
	acct := v.acct
	if !r.named {
		r.named = true
		r.NamedEpisode, r.NamedPodcast, r.NamedGeneration = r.EpisodeID, r.PodcastID, r.GenerationID
	}
	nE, nP, nG := r.NamedEpisode, r.NamedPodcast, r.NamedGeneration
	r.EpisodeID, r.PodcastID, r.GenerationID, r.Original = nE, nP, nG, ""
	// named: the checks on what the fact names (a generation a podcast merge
	// rehomed stays one of its source's; closure is checked where it goes).
	checks := func(e, p, g string, named bool) (string, error) {
		exists, err := acct.PodcastExists(p)
		if err != nil || !exists {
			return "unknown_podcast", err
		}
		row, err := acct.Generation(g)
		if errors.Is(err, storage.ErrNotFound) {
			return "unknown_generation", nil
		}
		if err != nil {
			return "", err
		}
		if row.PodcastID != p {
			if named {
				// A generation a podcast merge rehomed stays one of its source's.
				m, merged, err := acct.PodcastMergeStep(p)
				if err != nil {
					return "", err
				}
				if !merged || m.FromGeneration != g || m.IntoGeneration != g {
					return "unknown_generation", nil
				}
			} else if joined, err := joinedInto(acct, p, g); err != nil {
				return "", err
			} else if !joined {
				// (A generation a merge into p joined a source generation into
				// counts as p's: reference CHOICE.)
				return "unknown_generation", nil
			}
		}
		known, err := acct.EpisodePodcast(e)
		if err != nil {
			return "", err
		}
		if known != "" && known != p && !v.sameFamily(known, p) {
			return "episode_podcast_mismatch", v.err
		}
		if !named && row.ClosedBy != "" {
			return "generation_closed", nil
		}
		return "", nil
	}
	if routeReason, err = checks(nE, nP, nG, true); err != nil || routeReason != "" {
		return routeReason, "", err
	}
	o, kfam := occurrenceOf(r.Occurrence), guidKind(r.Occurrence)
	e, p, g, why := v.route(nP, nG, nE, o, kfam)
	if v.err != nil {
		return "", "", v.err
	}
	if why != "" {
		return why, "", nil
	}
	r.EpisodeID, r.PodcastID, r.GenerationID = e, p, g
	if e != nE {
		r.Original = nE
	}
	reason, err = checks(e, p, g, false)
	return "", reason, err
}

// ---- account

type accountOp struct{}

func (accountOp) Capability() string { return "" }

func (accountOp) Validate(f *protocol.Fact) error {
	if err := fields(f.Payload, []string{"new_episodes_action"}); err != nil {
		return err
	}
	s, ok := str(f.Payload, "new_episodes_action")
	if !ok || (s != "inbox" && s != "queue" && s != "nothing") {
		return invalidf("bad new_episodes_action")
	}
	if len(f.Targets) != 1 || f.Targets[0].Entity != "account" || f.Targets[0].Generation != "" {
		return invalidf("targets must be the account")
	}
	return nil
}

// ValidateInAccount rejects a target naming another account (an invalid fact).
func (accountOp) ValidateInAccount(account protocol.AccountInfo, f *protocol.Fact) error {
	if f.Targets[0].ID != account.ID {
		return invalidf("the target is not this account")
	}
	return nil
}

func (accountOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	s, _ := str(f.Payload, "new_episodes_action")
	if err := ctx.Contribute(protocol.AccountScope, projection.Effect{Kind: projection.KindDiscovery, DiscoveryDefault: s}); err != nil {
		return protocol.Outcome{}, err
	}
	return ctx.Settle()
}

var _ = sort.Strings
