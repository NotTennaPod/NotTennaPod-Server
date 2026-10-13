package ops

// The queue (PROTOCOL §7.6): membership is folded per episode scope; the order is the replay's
// (queue_replay.go).
// Every placement command carries the place its device resolved.

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/queue"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// RegisterQueue adds PROTOCOL §7.6 operation types (version 1).
func RegisterQueue(r *protocol.Registry) {
	r.Register("queue.add", 1, queueOp{kind: projection.KindQueueAdd})
	r.Register("queue.restore", 1, queueOp{kind: projection.KindQueueRestore})
	r.Register("queue.remove", 1, queueOp{kind: projection.KindQueueRemove})
	r.Register("queue.move", 1, queueOp{kind: projection.KindQueueMove})
	r.Register("queue.reorder", 1, queueOp{kind: projection.KindQueueMove, bulk: true})
	r.Register("queue.clear", 1, queueOp{kind: projection.KindQueueRemove, clear: true})
	r.Register("account.set_queue_policy", 1, queuePolicyOp{})
	r.RegisterQueuePublisher(publishQueue)
}

var enqueueLocations = map[string]bool{"back": true, "front": true, "after_current": true, "random": true}

// placement is a validated {episode, key, position_id}.
type placement struct {
	ref        EpisodeRef
	key        contract.QueueKey
	positionID [16]byte
}

func (p placement) effect(kind string) projection.Effect {
	return projection.Effect{Kind: kind, QueueKey: p.key.String(), PositionID: formatUUID(p.positionID)}
}

// parsePositionID accepts a lowercase UUID; its 128 bits are the allocator's U.
func parsePositionID(v any) ([16]byte, bool) {
	var u [16]byte
	s, ok := v.(string)
	if !ok || !contract.ValidUUID(s) {
		return u, false
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil {
		return u, false
	}
	copy(u[:], b)
	return u, true
}

// formatUUID writes 128 bits as a lowercase hyphenated UUID.
func formatUUID(u [16]byte) string {
	h := hex.EncodeToString(u[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// parseKeyAndID validates a key and the position ID it must carry (PROTOCOL §7.6).
func parseKeyAndID(obj map[string]any) (contract.QueueKey, [16]byte, error) {
	s, ok := str(obj, "key")
	if !ok {
		return contract.QueueKey{}, [16]byte{}, invalidf("key must be a string")
	}
	k, err := contract.ParseQueueKey(s)
	if err != nil {
		return k, [16]byte{}, invalidf("bad key")
	}
	u, ok := parsePositionID(obj["position_id"])
	if !ok {
		return k, u, invalidf("position_id must be a lowercase UUID")
	}
	if !contract.QueueKeyCarries(k, u) {
		return k, u, invalidf("the key was not allocated for its position_id")
	}
	return k, u, nil
}

func parsePlacement(v any, namespace string) (placement, error) {
	obj, ok := object(v)
	if !ok {
		return placement{}, invalidf("a placement must be an object")
	}
	if err := fields(obj, []string{"episode", "key", "position_id"}); err != nil {
		return placement{}, err
	}
	ref, err := parseEpisodeRef(obj["episode"], namespace)
	if err != nil {
		return placement{}, err
	}
	k, u, err := parseKeyAndID(obj)
	return placement{ref: ref, key: k, positionID: u}, err
}

func parsePlacements(v any, namespace string, nonEmpty bool) ([]placement, error) {
	list, ok := v.([]any)
	if !ok || (nonEmpty && len(list) == 0) {
		return nil, invalidf("placements must be a list")
	}
	var out []placement
	for _, raw := range list {
		p, err := parsePlacement(raw, namespace)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// place is a validated placement intent (PROTOCOL §7.6): the gap the
// device resolved when it executed the command. The anchor of "after" is
// context, never a mutation target: it is checked as a well-formed episode
// reference only.
type place struct {
	kind  string // start, after, end
	after *EpisodeRef
}

func parsePlace(v any, namespace string) (place, error) {
	obj, ok := object(v)
	if !ok {
		return place{}, invalidf("place must be an object")
	}
	kind, _ := str(obj, "kind")
	switch kind {
	case "start", "end":
		if err := fields(obj, []string{"kind"}); err != nil {
			return place{}, err
		}
		return place{kind: kind}, nil
	case "after":
		if err := fields(obj, []string{"kind", "after"}); err != nil {
			return place{}, err
		}
		r, err := parseEpisodeRef(obj["after"], namespace)
		if err != nil {
			return place{}, err
		}
		return place{kind: kind, after: &r}, nil
	}
	return place{}, invalidf("place kind must be start, after or end")
}

// value is the place as a decision records it: an anchor is
// {episode_id, generation_id} (the reference's reading).
func (p place) value() map[string]any {
	v := map[string]any{"kind": p.kind}
	if p.after != nil {
		v["after"] = map[string]any{"episode_id": p.after.EpisodeID, "generation_id": p.after.GenerationID}
	}
	return v
}

// decisionPlace reads a place a decision recorded: an anchor's episode and
// generation ("" for start and end).
func decisionPlace(raw any) (kind, episode, generation string, ok bool) {
	obj, isObj := object(raw)
	if !isObj {
		return "", "", "", false
	}
	kind, _ = str(obj, "kind")
	switch kind {
	case "start", "end":
		return kind, "", "", true
	case "after":
		a, isObj := object(obj["after"])
		if !isObj {
			return "", "", "", false
		}
		episode, _ = str(a, "episode_id")
		generation, _ = str(a, "generation_id")
		return kind, episode, generation, episode != ""
	}
	return "", "", "", false
}

// episodeTargets checks that the targets are exactly the named episodes, each
// named once.
func episodeTargets(f *protocol.Fact, refs []EpisodeRef) error {
	seen := map[string]bool{}
	var targets []protocol.Target
	for _, r := range refs {
		if seen[r.EpisodeID] {
			return invalidf("an episode appears twice")
		}
		seen[r.EpisodeID] = true
		targets = append(targets, protocol.Target{Entity: "episode", ID: r.EpisodeID, Generation: r.GenerationID})
	}
	return checkTargets(f, targets)
}

// parseRefs parses a list of episode references (no episode twice).
func parseRefs(v any, namespace string, nonEmpty bool) ([]EpisodeRef, error) {
	list, ok := v.([]any)
	if !ok || (nonEmpty && len(list) == 0) {
		return nil, invalidf("episodes must be a list")
	}
	seen := map[string]bool{}
	var out []EpisodeRef
	for _, raw := range list {
		r, err := parseEpisodeRef(raw, namespace)
		if err != nil {
			return nil, err
		}
		if seen[r.EpisodeID] {
			return nil, invalidf("an episode appears twice")
		}
		seen[r.EpisodeID] = true
		out = append(out, r)
	}
	return out, nil
}

// ---- queue operations

type queueOp struct {
	kind  string
	bulk  bool // queue.reorder
	clear bool // queue.clear
}

func (queueOp) Capability() string { return "queue" }

type queuePayload struct {
	members  []placement  // add, restore, move (one), a run's key predictions
	run      []EpisodeRef // a multi-item move's complete run, in its resulting order
	place    place
	refs     []EpisodeRef // remove and clear
	reason   string
	sort     string       // the sort dialog's order
	perm     []EpisodeRef // a random sort's permutation of what it observed
	observed []EpisodeRef // the sort dialog's observed queue
}

func (o queueOp) parse(namespace string, f *protocol.Fact) (queuePayload, error) {
	var p queuePayload
	var err error
	var targets []EpisodeRef
	switch {
	case o.kind == projection.KindQueueAdd, o.kind == projection.KindQueueRestore:
		if err = fields(f.Payload, []string{"members", "place"}); err != nil {
			return p, err
		}
		if p.members, err = parsePlacements(f.Payload["members"], namespace, true); err != nil {
			return p, err
		}
		if p.place, err = parsePlace(f.Payload["place"], namespace); err != nil {
			return p, err
		}
		for _, m := range p.members {
			targets = append(targets, m.ref)
		}
	case o.bulk:
		_, isRun := f.Payload["run"]
		_, isSort := f.Payload["sort"]
		switch {
		case isRun && !isSort:
			if err = fields(f.Payload, []string{"run", "place"}, "members"); err != nil {
				return p, err
			}
			if p.run, err = parseRefs(f.Payload["run"], namespace, true); err != nil {
				return p, err
			}
			if p.place, err = parsePlace(f.Payload["place"], namespace); err != nil {
				return p, err
			}
			if _, present := f.Payload["members"]; present {
				// The device's key predictions for the run (sparse).
				if p.members, err = parsePlacements(f.Payload["members"], namespace, false); err != nil {
					return p, err
				}
				// Non-empty when present, one per episode, each naming one of
				// the run's references exactly as the run names it.
				if len(p.members) == 0 {
					return p, invalidf("members, when present, are non-empty")
				}
				inRun := map[string]string{}
				for i, r := range p.run {
					b, _ := contract.Canonical(f.Payload["run"].([]any)[i])
					inRun[r.EpisodeID] = string(b)
				}
				seen := map[string]bool{}
				list, _ := f.Payload["members"].([]any)
				for i, m := range p.members {
					obj, _ := object(list[i])
					b, _ := contract.Canonical(obj["episode"])
					if inRun[m.ref.EpisodeID] != string(b) || seen[m.ref.EpisodeID] {
						return p, invalidf("a predicted key names an episode outside the run")
					}
					seen[m.ref.EpisodeID] = true
				}
			}
			targets = p.run
		case isSort && !isRun:
			if err = fields(f.Payload, []string{"sort", "observed"}); err != nil {
				return p, err
			}
			if err = p.parseSort(f.Payload, namespace); err != nil {
				return p, err
			}
			// A sort's target is the account; its observed list is context
			// (the reference's reading).
			if len(f.Targets) != 1 || f.Targets[0].Entity != "account" || f.Targets[0].Generation != "" {
				return p, invalidf("a sort's target is the account")
			}
			return p, nil
		default:
			return p, invalidf("a reorder is a run or a sort")
		}
	case o.kind == projection.KindQueueMove:
		if err = fields(f.Payload, []string{"episode", "key", "position_id", "place"}); err != nil {
			return p, err
		}
		ref, err := parseEpisodeRef(f.Payload["episode"], namespace)
		if err != nil {
			return p, err
		}
		k, u, err := parseKeyAndID(f.Payload)
		if err != nil {
			return p, err
		}
		p.members = []placement{{ref: ref, key: k, positionID: u}}
		if p.place, err = parsePlace(f.Payload["place"], namespace); err != nil {
			return p, err
		}
		targets = []EpisodeRef{ref}
	case o.clear:
		if err = fields(f.Payload, []string{"observed"}); err != nil {
			return p, err
		}
		if p.refs, err = parseRefList(f.Payload["observed"], namespace); err != nil {
			return p, err
		}
		p.reason = "cleared"
		targets = p.refs
	default: // queue.remove
		if err = fields(f.Payload, []string{"episodes", "reason"}); err != nil {
			return p, err
		}
		if p.refs, err = parseRefList(f.Payload["episodes"], namespace); err != nil {
			return p, err
		}
		p.reason, _ = str(f.Payload, "reason")
		if p.reason != "user" && p.reason != "download_deleted" {
			return p, invalidf("bad reason")
		}
		targets = p.refs
	}
	return p, episodeTargets(f, targets)
}

// parseSort validates the sort dialog's command (PROTOCOL §7.6):
// {sort: {sort_order, permutation?}, observed}: a canonical order, or random
// with the device's resulting order of exactly the episodes it observed. An
// empty observed queue is valid: another phone's unseen add can make the
// sort matter.
func (p *queuePayload) parseSort(payload map[string]any, namespace string) error {
	obj, ok := object(payload["sort"])
	if !ok {
		return invalidf("sort must be an object")
	}
	if err := fields(obj, []string{"sort_order"}, "permutation"); err != nil {
		return err
	}
	order, _ := str(obj, "sort_order")
	if !queue.Orders[order] {
		return invalidf("bad sort_order")
	}
	p.sort = order
	var err error
	if p.observed, err = parseRefs(payload["observed"], namespace, false); err != nil {
		return err
	}
	_, hasPerm := obj["permutation"]
	if hasPerm != (order == "random") {
		return invalidf("a random sort, and only it, carries its permutation")
	}
	if !hasPerm {
		return nil
	}
	if p.perm, err = parseRefs(obj["permutation"], namespace, false); err != nil {
		return err
	}
	if len(p.perm) != len(p.observed) {
		return invalidf("the permutation orders exactly the observed episodes")
	}
	seen := map[string]bool{}
	for _, r := range p.observed {
		seen[r.EpisodeID] = true
	}
	for _, r := range p.perm {
		if !seen[r.EpisodeID] {
			return invalidf("the permutation orders exactly the observed episodes")
		}
	}
	return nil
}

// parseRefList parses a non-empty list of EpisodeRefs sorted by episode ID.
func parseRefList(v any, namespace string) ([]EpisodeRef, error) {
	list, ok := v.([]any)
	if !ok || len(list) == 0 {
		return nil, invalidf("episodes must be a non-empty list")
	}
	var out []EpisodeRef
	for _, raw := range list {
		r, err := parseEpisodeRef(raw, namespace)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 && out[len(out)-1].EpisodeID >= r.EpisodeID {
			return nil, invalidf("episodes must be sorted by episode_id without repeats")
		}
		out = append(out, r)
	}
	return out, nil
}

func (o queueOp) Validate(f *protocol.Fact) error {
	_, err := o.parse("", f)
	return err
}

func (o queueOp) ValidateInAccount(account protocol.AccountInfo, f *protocol.Fact) error {
	p, err := o.parse(account.Namespace, f)
	if err == nil && p.sort != "" && f.Targets[0].ID != account.ID {
		return invalidf("the target is not this account")
	}
	return err
}

func (o queueOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	p, err := o.parse(ctx.Namespace, f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if p.sort != "" {
		return o.applySort(ctx, f)
	}
	placing := o.kind == projection.KindQueueAdd || o.kind == projection.KindQueueRestore
	moving := o.kind == projection.KindQueueMove
	firstReason := ""
	contributed := 0
	var own []string // the scopes of the members that passed their checks
	// writers are the members whose key can be a key write (PROTOCOL §7.6 selector): those carrying
	// a key (a placement's device key, a move's, a run move's prediction) whose own checks passed,
	// by their index in the payload's members (a run move: in its run). The replay decides which of
	// them write: a placement's only for the members it actually places.
	var writers []int
	// refused are the members whose own checks failed for another reason
	// than a closed generation: a move leaves them out (a closed
	// generation's member only creates its missing ghost, the replay decides).
	var refused []int
	// closed are the members admitted as of a closed generation (live, or as
	// a recovered receipt decided): a move only ghosts them, even when their
	// generation is live again in the order the replay reads.
	var closed []int
	contribute := func(ref EpisodeRef, e projection.Effect, key *placement, index int) error {
		reason, err := checkEpisode(ctx, &ref)
		if err != nil || reason != "" {
			if firstReason == "" {
				firstReason = reason
			}
			switch {
			case reason == "generation_closed":
				closed = append(closed, index)
			case reason != "":
				refused = append(refused, index)
			}
			return err
		}
		scope := protocol.EpisodeScope(ref.GenerationID, ref.EpisodeID)
		e.Member = ref.Original
		if err := ctx.Contribute(scope, withRef(e, ref)); err != nil {
			return err
		}
		if key != nil {
			writers = append(writers, index)
		}
		own = append(own, scope)
		contributed++
		return nil
	}
	switch {
	case placing:
		for i := range p.members {
			m := &p.members[i]
			if err := contribute(m.ref, m.effect(o.kind), m, i); err != nil {
				return protocol.Outcome{}, err
			}
		}
	case moving && len(p.run) > 0:
		predicted := map[string]*placement{}
		for i := range p.members {
			predicted[p.members[i].ref.EpisodeID] = &p.members[i]
		}
		for i, r := range p.run {
			if err := contribute(r, projection.Effect{Kind: projection.KindQueueMove}, predicted[r.EpisodeID], i); err != nil {
				return protocol.Outcome{}, err
			}
		}
	case moving:
		for i := range p.members {
			m := &p.members[i]
			if err := contribute(m.ref, projection.Effect{Kind: projection.KindQueueMove}, m, i); err != nil {
				return protocol.Outcome{}, err
			}
		}
	default:
		for _, r := range p.refs {
			if err := contribute(r, projection.Effect{Kind: projection.KindQueueRemove, Reason: p.reason}, nil, 0); err != nil {
				return protocol.Outcome{}, err
			}
		}
	}
	// Every placement command is a replay step, applied or not: a placement
	// that never applies still leaves its ghost, so later anchors resolve
	// (PROTOCOL §7.6).
	if placing || moving {
		if err := ctx.QueueStepWrites(f.OpID, writers, refused, closed); err != nil {
			return protocol.Outcome{}, err
		}
	}
	// A placement command is a queue membership/position node whatever its
	// outcome (PROTOCOL §7.6's queue frontier): it is a replay step, applied or not.
	// A removal is one only when it contributes.
	if placing || moving || contributed > 0 {
		if err := ctx.Account.AddQueueNode(f.OpID); err != nil {
			return protocol.Outcome{}, err
		}
	}
	if contributed == 0 {
		outcome := protocol.Outcome{Result: "not_applied", Reason: firstReason}
		if placing && !ctx.Rebuilding() {
			// Under keep-sorted at its place, an add's or a restore's receipt
			// records the queue even when none of its members passes the bulk
			// checks (whenever keep-sorted is on at its place).
			sorted, err := keepSortedAt(ctx, f.OpID)
			if err != nil || !sorted {
				return outcome, err
			}
			outcome.Values, err = recordValues(ctx, nil, true)
			return outcome, err
		}
		return outcome, nil
	}
	outcome, err := ctx.Settle()
	if err != nil || !(placing || moving) {
		return outcome, err
	}
	// A placement is applied whenever one member passes the bulk checks: its
	// effect is its place in the replay's order.
	outcome = protocol.Outcome{Result: "applied"}
	// The receipt records the values of the episodes it places or moves (a move is a placement too)
	// and, for an add or a restore at a place where keep-sorted is on, of every member then in the
	// queue (PROTOCOL §7.6; a restore never sorts, but its receipt records the queue as an add's
	// does). A move never sorts.
	trigger := placing
	if trigger {
		if trigger, err = keepSortedAt(ctx, f.OpID); err != nil {
			return outcome, err
		}
	}
	outcome.Values, err = recordValues(ctx, own, trigger)
	return outcome, err
}

// applySort admits the sort dialog's command: a replay step at its place
// that sorts the in-members there (PROTOCOL §7.6); its receipt
// records the values of every member then in the queue. Its observed
// episodes are context (they need not be live): the command is applied.
func (o queueOp) applySort(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	if err := ctx.QueueStep(f.OpID, ""); err != nil {
		return protocol.Outcome{}, err
	}
	if err := ctx.Account.AddQueueNode(f.OpID); err != nil {
		return protocol.Outcome{}, err
	}
	values, err := recordValues(ctx, nil, true)
	return protocol.Outcome{Result: "applied", Values: values}, err
}

// ---- sort values (PROTOCOL §7.6)

// keepSorted folds the account's queue policy.
func keepSorted(ctx *protocol.ApplyContext) (location string, sorted bool, order string, err error) {
	cs, err := accountContributions(ctx.Account, -1)
	if err != nil {
		return "", false, "", err
	}
	location, sorted, order = projection.FoldAccount(cs).QueuePolicy()
	return location, sorted, order, nil
}

// keepSortedAt reports whether keep-sorted is on at a node's place in the
// order: the sorting register folded over the policy writes ranked before it.
func keepSortedAt(ctx *protocol.ApplyContext, node string) (bool, error) {
	rank, err := ctx.Account.NodeRank(node)
	if err != nil {
		return false, err
	}
	cs, err := accountContributions(ctx.Account, rank)
	if err != nil {
		return false, err
	}
	_, sorted, _ := projection.FoldAccount(cs).QueuePolicy()
	return sorted, nil
}

// accountContributions are the account scope's contributions ranked before
// rank (every one for a negative rank).
func accountContributions(acct *storage.AccountTx, rank int64) ([]projection.Contribution, error) {
	rows, err := acct.Contributions(protocol.AccountScope)
	if err != nil {
		return nil, err
	}
	cs := make([]projection.Contribution, 0, len(rows))
	for _, r := range rows {
		if rank >= 0 && r.Rank >= rank {
			continue
		}
		var e projection.Effect
		if err := json.Unmarshal([]byte(r.Effect), &e); err != nil {
			return nil, err
		}
		cs = append(cs, projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e})
	}
	return cs, nil
}

// recordValues are the canonical sort values the server records with a
// placement or a sort trigger (PROTOCOL §7.6): those of the episode
// scopes it places and, for a trigger, of every member then in the queue,
// sorted by episode and generation. They never travel in a phone's fact.
func recordValues(ctx *protocol.ApplyContext, scopes []string, trigger bool) ([]any, error) {
	if ctx.Rebuilding() {
		return nil, nil // a rebuild's replay keeps the archived receipts and decisions
	}
	// Values at admission, the commit's own effects included (a start's
	// measured duration, say): the fact's scopes are refolded first.
	ctx.SettleLater()
	if err := ctx.Flush(); err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, s := range scopes {
		set[s] = true
	}
	if trigger {
		st, err := replayNow(ctx)
		if err != nil {
			return nil, err
		}
		for _, e := range st.Visible() {
			set[string(e)] = true
		}
	}
	keys := make([]string, 0, len(set))
	for s := range set {
		keys = append(keys, s)
	}
	sort.Slice(keys, func(i, j int) bool {
		ei, gi := scopeEpisode(keys[i])
		ej, gj := scopeEpisode(keys[j])
		if ei != ej {
			return ei < ej
		}
		return gi < gj
	})
	out := make([]any, 0, len(keys))
	for _, s := range keys {
		ep, gen := scopeEpisode(s)
		it, err := sortValues(ctx.Account, ep)
		if err != nil {
			return nil, err
		}
		out = append(out, valuesObject(ep, gen, it))
	}
	return out, nil
}

func valuesObject(episode, generation string, it queue.Item) map[string]any {
	return map[string]any{"episode_id": episode, "generation_id": generation, "values": map[string]any{"title": it.Title, "podcast_id": it.PodcastID,
		"podcast_title": it.PodcastTitle, "publication_time": it.PublicationTime, "duration": it.Duration}}
}

// sortValues is the one canonical metadata accessor for sorting (PROTOCOL §7.6):
// catalog metadata over occurrence evidence, the measured duration first.
func sortValues(acct *storage.AccountTx, episode string) (queue.Item, error) {
	it := queue.Item{EpisodeID: episode}
	podcast, err := acct.EpisodePodcast(episode)
	if err != nil {
		return it, err
	}
	it.PodcastID = podcast
	occ, err := acct.EpisodeOccurrence(episode)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return it, err
	}
	if v, err := contract.DecodeJSON([]byte(occ)); err == nil {
		o, _ := v.(map[string]any)
		it.Title, _ = o["title"].(string)
		if t, ok := integer(o["publication_time"]); ok {
			it.PublicationTime = t
		}
	}
	// Catalog metadata replaces occurrence evidence (PROTOCOL §9.4): title,
	// publication date and the feed's duration; the podcast's title.
	meta, err := recordValue(acct, "episode", episode, "metadata")
	if err != nil {
		return it, err
	}
	if t, ok := meta["title"].(string); ok {
		it.Title = t
	}
	if t, ok := integer(meta["pub_date"]); ok {
		it.PublicationTime = t
	}
	if m, ok := meta["media"].(map[string]any); ok {
		if d, ok := integer(m["duration"]); ok {
			it.Duration = d
		}
	}
	pmeta, err := recordValue(acct, "podcast", podcast, "metadata")
	if err != nil {
		return it, err
	}
	it.PodcastTitle, _ = pmeta["title"].(string)
	// The measured duration, when there is one, comes first (PROTOCOL §7.6).
	measured, err := recordValue(acct, "episode", episode, "measured_duration")
	if err != nil {
		return it, err
	}
	if d, ok := integer(measured["duration_ms"]); ok {
		it.Duration = d
	}
	return it, nil
}

// recordValue is a live record's value (nil when there is none).
func recordValue(acct *storage.AccountTx, entity, id, group string) (map[string]any, error) {
	r, err := acct.Record(entity, id, group)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v, err := contract.DecodeJSON([]byte(r.Value))
	if err != nil {
		return nil, err
	}
	m, _ := v.(map[string]any)
	return m, nil
}

// ---- server-resolved placements (PROTOCOL §7.6, §9.4)

// resolvedPlacement is the server's resolution of an add at the account's
// enqueue location, as a phone would resolve it without a current episode
// or downloads (PROTOCOL §7.6: after_current is the back, front the front; random
// draws once): its place in the current queue (the replay's order) and a key
// there (a candidate for the published key).
func resolvedPlacement(ctx *protocol.ApplyContext) (place, contract.QueueKey, [16]byte, string, error) {
	location, _, _, err := keepSorted(ctx)
	if err != nil {
		return place{}, contract.QueueKey{}, [16]byte{}, "", err
	}
	if err := ctx.Flush(); err != nil {
		return place{}, contract.QueueKey{}, [16]byte{}, "", err
	}
	st, err := replayNow(ctx)
	if err != nil {
		return place{}, contract.QueueKey{}, [16]byte{}, "", err
	}
	order := st.Visible()
	at, p := len(order), place{kind: "end"}
	switch location {
	case "front":
		at, p = 0, place{kind: "start"}
	case "random":
		// Index 0 is the start; index i after the episode at i-1, the tail
		// included (never the end, PROTOCOL §7.6). The draw is the decision's.
		if at = ctx.Random().Index(len(order) + 1); at == 0 {
			p = place{kind: "start"}
		} else {
			ep, gen := scopeEpisode(string(order[at-1]))
			p = place{kind: "after", after: &EpisodeRef{EpisodeID: ep, GenerationID: gen}}
		}
	}
	keys, err := currentKeys(ctx, st, order)
	if err != nil {
		return place{}, contract.QueueKey{}, [16]byte{}, "", err
	}
	var low, high *contract.QueueKey
	if at > 0 {
		low = keys[at-1]
	}
	if at < len(order) {
		high = keys[at]
	}
	if low != nil && high != nil && contract.CompareQueueKeys(*low, *high) >= 0 {
		high = nil // unpublished or reordered neighbours: any key is a candidate
	}
	u := ctx.Random().PositionID()
	k, err := contract.QueueKeyBetween(low, high, u)
	return p, k, u, location, err
}

// currentKeys are the members' published keys in order (nil for none).
func currentKeys(ctx *protocol.ApplyContext, st *queue.State, order []queue.Entry) ([]*contract.QueueKey, error) {
	published, err := publishedKeys(ctx, st)
	if err != nil {
		return nil, err
	}
	out := make([]*contract.QueueKey, len(order))
	for i, e := range order {
		if k, ok := published[e]; ok {
			if q, err := contract.ParseQueueKey(k.Key); err == nil {
				out[i] = &q
			}
		}
	}
	return out, nil
}

// ---- account queue policy

type queuePolicyOp struct{}

func (queuePolicyOp) Capability() string { return "queue" }

func (queuePolicyOp) parse(f *protocol.Fact) (projection.Effect, error) {
	e := projection.Effect{Kind: projection.KindQueuePolicy}
	if err := fields(f.Payload, nil, "enqueue_location", "sorting"); err != nil {
		return e, err
	}
	if len(f.Payload) == 0 {
		return e, invalidf("nothing to set")
	}
	if v, present := f.Payload["enqueue_location"]; present {
		s, ok := v.(string)
		if !ok || !enqueueLocations[s] {
			return e, invalidf("bad enqueue_location")
		}
		e.EnqueueLocation = s
	}
	if v, present := f.Payload["sorting"]; present {
		obj, ok := object(v)
		if !ok {
			return e, invalidf("sorting must be an object")
		}
		if err := fields(obj, []string{"keep_sorted", "sort_order"}); err != nil {
			return e, err
		}
		keep, ok := boolean(obj["keep_sorted"])
		order, ok2 := str(obj, "sort_order")
		if !ok || !ok2 || !queue.Orders[order] || (keep && order == "random") {
			return e, invalidf("bad sorting")
		}
		e.KeepSorted, e.SortOrder = keep, order
	}
	if len(f.Targets) != 1 || f.Targets[0].Entity != "account" || f.Targets[0].Generation != "" {
		return e, invalidf("targets must be the account")
	}
	return e, nil
}

func (o queuePolicyOp) Validate(f *protocol.Fact) error {
	_, err := o.parse(f)
	return err
}

func (queuePolicyOp) ValidateInAccount(account protocol.AccountInfo, f *protocol.Fact) error {
	if f.Targets[0].ID != account.ID {
		return invalidf("the target is not this account")
	}
	return nil
}

func (o queuePolicyOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	e, err := o.parse(f)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if err := ctx.Contribute(protocol.AccountScope, e); err != nil {
		return protocol.Outcome{}, err
	}
	if e.SortOrder != "" {
		// The sorting register is replayed at its place (PROTOCOL §7.6): a policy change is a step.
		// Not a queue membership/position node: PROTOCOL §7.6's frontier lists queue
		// facts, starts, ends and decisions, not policy writes.
		if err := ctx.QueueStep(f.OpID, ""); err != nil {
			return protocol.Outcome{}, err
		}
	}
	return ctx.Settle()
}
