package ops

// The queue replay's inputs, snapshots and publication
// (PROTOCOL §7.6).
//
// The order is a pure function of the archive: the replay steps
// (storage.QueueSteps: placements, moves, sorts, policy changes, an import's
// queue) read from their facts and decisions and routed through the identity
// and generation mapping in force now (PROTOCOL §7.6), and the episode folds'
// membership events (storage.QueueEvents), merged in the deterministic order.
// Snapshots of the replay's state (queue.State) are kept in memory per
// account; a commit reruns the replay from the latest snapshot before the
// earliest node whose inputs it changed (ApplyContext.QueueTouch), or from the
// start when the mapping changed, the cache is cold or stale, or the account
// was just rebuilt. Every commit then publishes keys that give that order
// (PROTOCOL §7.6), recording a queue.layout when the published keys do not.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"math/bits"
	"sort"
	"sync"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/queue"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// snapshot is the replay's state after a node (immutable once kept).
type snapshot struct {
	node  string // the last node processed ("" for the empty queue)
	state *queue.State
}

// replayCache is an account's snapshots, ascending; the last is the state
// after every node. head is the account's head after the commit that left
// it, so a cache is trusted only while the head has not moved since.
type replayCache struct {
	head  storage.Head
	snaps []snapshot
	pub   map[queue.Entry]queue.Key // the published keys it left (nil: none trusted)
}

func (rc *replayCache) valid(h storage.Head) bool {
	return rc != nil && rc.head.LogID == h.LogID && rc.head.Epoch == h.Epoch && rc.head.Seq == h.Seq && string(rc.head.Digest) == string(h.Digest)
}

// installCache keeps next as the account's cache unless the cache already kept
// is for a later head of the same log: commit callbacks run after the store's
// transaction ends, so a slower one must never replace a newer cache. A cache
// of another log (a restored server) is replaced.
func installCache(memo *sync.Map, account string, next *replayCache) {
	key := memoKey{account}
	for {
		v, ok := memo.Load(key)
		if !ok {
			if _, loaded := memo.LoadOrStore(key, next); !loaded {
				return
			}
			continue
		}
		cur := v.(*replayCache)
		if cur.head.LogID == next.head.LogID &&
			(cur.head.Epoch > next.head.Epoch || cur.head.Epoch == next.head.Epoch && cur.head.Seq > next.head.Seq) {
			return
		}
		if memo.CompareAndSwap(key, v, next) {
			return
		}
	}
}

// commitReplay is a commit's replay state (ApplyContext.QueueMemo): the
// snapshots as of the inputs it last replayed.
type commitReplay struct {
	snaps []snapshot
}

type memoKey struct{ account string }

var replayStats struct {
	sync.Mutex
	full, incremental int
}

// replayCounts reports how many replays ran from the start and from a
// snapshot (tests).
func replayCounts() (int, int) {
	replayStats.Lock()
	defer replayStats.Unlock()
	return replayStats.full, replayStats.incremental
}

// replayNow is the replay's state for the commit's inputs so far (the
// current fact's own step is not archived yet and is skipped; its place is
// replayed again at the commit's publication).
func replayNow(c *protocol.ApplyContext) (*queue.State, error) {
	cr, err := replay(c, false)
	if err != nil {
		return nil, err
	}
	return cr.snaps[len(cr.snaps)-1].state, nil
}

// replay brings the commit's replay state up to date with its inputs: from
// the start when rebuilt, when the mapping changed or when there is no
// trusted state, else from the latest snapshot before the earliest node
// touched since the last replay. A step whose fact is still being admitted
// is replayed again later (it stays touched).
func replay(c *protocol.ApplyContext, rebuilt bool) (*commitReplay, error) {
	memo := c.QueueMemo()
	touched, all := c.QueueChanges(true)
	var base []snapshot
	if cr, ok := (*memo).(*commitReplay); ok && cr != nil {
		base = cr.snaps
	} else {
		head, err := c.Account.Head()
		if err != nil {
			return nil, err
		}
		if v, ok := c.Account.Memo().Load(memoKey{c.AccountID}); ok {
			if rc := v.(*replayCache); rc.valid(head) {
				base = rc.snaps
			}
		}
	}
	if rebuilt {
		base = nil
	}
	start := -1 // the snapshot to resume from (-1: the empty queue)
	if base != nil && !all {
		if len(touched) == 0 {
			start = len(base) - 1
		} else {
			earliest := int64(-1)
			for _, n := range touched {
				r, err := c.Account.NodeRank(n)
				if errors.Is(err, storage.ErrNotFound) {
					continue // not a node (any more): nothing of it is replayed
				} else if err != nil {
					return nil, err
				}
				if earliest < 0 || r < earliest {
					earliest = r
				}
			}
			start = len(base) - 1
			if earliest >= 0 {
				for start >= 0 {
					s := base[start]
					if s.node == "" {
						break
					}
					r, err := c.Account.NodeRank(s.node)
					if err != nil && !errors.Is(err, storage.ErrNotFound) {
						return nil, err
					}
					if err == nil && r < earliest {
						break
					}
					start--
				}
			}
		}
	}
	var kept []snapshot
	var from snapshot
	after := int64(-1)
	if start >= 0 {
		kept = append(kept, base[:start+1]...)
		from = base[start]
		if from.node != "" {
			r, err := c.Account.NodeRank(from.node)
			if err != nil {
				return nil, err
			}
			after = r
		}
	} else {
		from = snapshot{state: queue.NewState()}
		kept = []snapshot{from}
	}
	if start == len(base)-1 && start >= 0 && len(touched) == 0 && !all {
		cr := &commitReplay{snaps: base}
		*memo = cr
		return cr, nil
	}
	nodes, deferred, err := loadReplayNodes(c, after)
	if err != nil {
		return nil, err
	}
	c.QueueTouch(deferred...)
	replayStats.Lock()
	if start < 0 {
		replayStats.full++
	} else {
		replayStats.incremental++
	}
	replayStats.Unlock()
	st := from.state.Clone()
	st.Fallback = func(e queue.Entry) queue.Values {
		ep, _ := scopeEpisode(string(e))
		it, err := sortValues(c.Account, ep)
		if err != nil {
			return queue.Values{}
		}
		slog.Info("queue replay: values_missing: a sort uses an episode's current values", "account", c.AccountID, "episode", ep)
		return queue.Values{Title: it.Title, PodcastID: it.PodcastID, PodcastTitle: it.PodcastTitle, PublicationTime: it.PublicationTime,
			Duration: it.Duration}
	}
	total := st.Count + len(nodes)
	marks := snapshotMarks(st.Count, total)
	// A client fault (PROTOCOL §7.6): logged when its command is replayed from a
	// snapshot (a full replay meets the history's again, at debug level).
	level := slog.LevelInfo
	if start < 0 {
		level = slog.LevelDebug
	}
	for i, n := range nodes {
		for _, d := range st.Apply(n) {
			slog.Log(context.Background(), level, "queue replay: an anchor without an entry goes to the end", "account", c.AccountID, "node", d.Node,
				"anchor", string(d.Anchor))
		}
		if marks[st.Count] && i < len(nodes)-1 {
			kept = append(kept, snapshot{node: n.ID, state: st.Clone()})
		}
	}
	last := from.node
	if len(nodes) > 0 {
		last = nodes[len(nodes)-1].ID
	}
	kept = append(kept, snapshot{node: last, state: st})
	cr := &commitReplay{snaps: thin(kept, total)}
	*memo = cr
	return cr, nil
}

// snapshotMarks are the node counts at which a forward replay keeps a
// snapshot: ages 1, 2, 4, … nodes before the end (PROTOCOL §7.6: thinned
// geometrically by age).
func snapshotMarks(from, total int) map[int]bool {
	out := map[int]bool{}
	for age := 1; age <= total; age *= 2 {
		if n := total - age; n > from {
			out[n] = true
		}
	}
	return out
}

// thin keeps, of every power-of-two age bucket, the newest snapshot, and
// always the empty queue and the final state.
func thin(snaps []snapshot, total int) []snapshot {
	if len(snaps) <= 2 {
		return snaps
	}
	out := []snapshot{snaps[0]}
	seen := map[int]bool{}
	var mid []snapshot
	for i := len(snaps) - 2; i >= 1; i-- {
		age := total - snaps[i].state.Count
		if age < 1 {
			continue
		}
		b := bits.Len(uint(age))
		if seen[b] {
			continue
		}
		seen[b] = true
		mid = append(mid, snaps[i])
	}
	for i := len(mid) - 1; i >= 0; i-- {
		out = append(out, mid[i])
	}
	return append(out, snaps[len(snaps)-1])
}

// ---- inputs

// loadReplayNodes reads the replay's nodes ranked after rank, in the
// deterministic order: each step node's step and each node's membership
// events, routed through the mapping in force now.
func loadReplayNodes(c *protocol.ApplyContext, after int64) ([]queue.Node, []string, error) {
	acct := c.Account
	steps, err := acct.QueueStepsAfter(after)
	if err != nil {
		return nil, nil, err
	}
	events, err := acct.QueueEventsAfter(after)
	if err != nil {
		return nil, nil, err
	}
	v := newView(c)
	l := &stepLoader{c: c, v: v, imports: map[string][]importMember{}, byRef: map[refKey]queue.Entry{}, byDecision: map[[2]string]queue.Entry{}}
	var out []queue.Node
	i, j := 0, 0
	for i < len(steps) || j < len(events) {
		var rank int64
		var id string
		switch {
		case j >= len(events) || (i < len(steps) && steps[i].Rank <= events[j].Rank):
			rank, id = steps[i].Rank, steps[i].Node
		default:
			rank, id = events[j].Rank, events[j].Node
		}
		n := queue.Node{ID: id}
		for i < len(steps) && steps[i].Rank == rank {
			st, err := l.step(steps[i])
			if err != nil {
				return nil, nil, err
			}
			n.Step = st
			i++
		}
		for j < len(events) && events[j].Rank == rank {
			n.Events = append(n.Events, queue.Event{Entry: queue.Entry(events[j].Scope), In: events[j].In})
			j++
		}
		out = append(out, n)
	}
	if v.err != nil {
		return nil, nil, v.err
	}
	return out, l.deferred, nil
}

// stepLoader reads replay steps from the archive.
type stepLoader struct {
	c            *protocol.ApplyContext
	v            *idView
	imports      map[string][]importMember // by import ID, loaded once
	importsOK    bool
	importCounts map[string]importCount
	deferred     []string // steps whose fact is being admitted (not archived yet)
	// Routing memos: the identity state is fixed for the whole read.
	byRef      map[refKey]queue.Entry
	byDecision map[[2]string]queue.Entry
	regs       map[string]storage.Registration
	unknowns   map[[2]string]bool
	lives      map[queue.Entry]bool
}

// live reports whether an entry's generation is its episode's podcast's live
// one: only such an entry can be a member (the episode folds record
// membership for the live generation alone).
func (l *stepLoader) live(e queue.Entry) bool {
	if v, ok := l.lives[e]; ok {
		return v
	}
	ep, gen := scopeEpisode(string(e))
	podcast, err := l.c.Account.EpisodePodcast(ep)
	if err != nil {
		l.v.fail(err)
	}
	live := ""
	if podcast != "" {
		gens, err := l.c.Account.Generations(podcast)
		if err != nil {
			l.v.fail(err)
		}
		for _, g := range gens {
			if g.ClosedBy == "" {
				live = g.ID
			}
		}
	}
	if l.lives == nil {
		l.lives = map[queue.Entry]bool{}
	}
	l.lives[e] = gen == live
	return gen == live
}

// unknown reports a reference to a podcast or generation the account does
// not have (a fact's unknown_podcast or unknown_generation): a client fault,
// since a device can only name what it synced. Such a member is neither
// placed nor ghosted, so an anchor to it has no entry and resolves to the end
// (PROTOCOL §7.6; the reference's reading, generated recovery case
// seed 8002 #3).
func (l *stepLoader) unknown(r EpisodeRef) bool {
	k := [2]string{r.PodcastID, r.GenerationID}
	if u, ok := l.unknowns[k]; ok {
		return u
	}
	exists, err := l.c.Account.PodcastExists(r.PodcastID)
	if err != nil {
		l.v.fail(err)
	}
	u := !exists
	if exists {
		if _, err := l.c.Account.Generation(r.GenerationID); errors.Is(err, storage.ErrNotFound) {
			u = true
		} else if err != nil {
			l.v.fail(err)
		}
	}
	if l.unknowns == nil {
		l.unknowns = map[[2]string]bool{}
	}
	l.unknowns[k] = u
	return u
}

// refKey is what routing an episode reference reads.
type refKey struct {
	episode, podcast, generation, kind, guid, enclosure string
	pub                                                 int64
	dated                                               bool
}

type importMember struct {
	index    int64
	decision string
	entry    queue.Entry
	key      queue.Key
	values   *queue.Values
}

// entryOf is where an episode reference's effects live now (the placement of a fact's effect
// through its reference): the queue entry it names.
func (l *stepLoader) entryOf(r EpisodeRef) queue.Entry {
	ref := refOf(r)
	k := refKey{episode: ref.Episode, podcast: ref.Podcast, generation: ref.Generation, kind: ref.Kind, guid: ref.GUID, enclosure: ref.Enclosure}
	if ref.Pub != nil {
		k.pub, k.dated = *ref.Pub, true
	}
	if e, ok := l.byRef[k]; ok {
		return e
	}
	e := queue.Entry(l.v.target(projection.Effect{Ref: ref}, protocol.EpisodeScope(r.GenerationID, r.EpisodeID)))
	l.byRef[k] = e
	return e
}

// decisionEntry is where a decision's episode lives now (a server-resolved
// add, a filing: the root of its episode in the live class its generation
// joined).
func (l *stepLoader) decisionEntry(episode, generation string) queue.Entry {
	k := [2]string{episode, generation}
	if e, ok := l.byDecision[k]; ok {
		return e
	}
	e := queue.Entry(l.v.target(projection.Effect{}, protocol.EpisodeScope(generation, episode)))
	l.byDecision[k] = e
	return e
}

func (l *stepLoader) step(row storage.QueueStepRow) (*queue.Step, error) {
	if row.FactType == "" {
		if row.NodeDecision == nil {
			// The fact being admitted: its place is replayed once it is archived.
			l.deferred = append(l.deferred, row.Node)
			return nil, nil
		}
		d, err := decodeObject(row.NodeDecision)
		if err != nil {
			return nil, err
		}
		return l.decisionStep(d)
	}
	if row.FactOutcome == "rejected" {
		return nil, nil
	}
	// The fact was validated when it was admitted: its payload is read, not
	// checked again (its content hash is the archive's).
	obj, err := decodeObject(row.Fact)
	if err != nil {
		return nil, err
	}
	payload, _ := object(obj["payload"])
	var values map[queue.Entry]queue.Values
	// The fact's key writes rank at its original receipt (PROTOCOL §7.6); its writers are the members
	// whose own checks passed.
	rank := queue.Rank{ID: row.Node}
	if row.Receipt != nil {
		receipt, err := decodeObject(row.Receipt)
		if err != nil {
			return nil, err
		}
		values = l.values(receipt["sort_values"])
		rank.Epoch, _ = integer(receipt["epoch"])
		rank.Seq, _ = integer(receipt["seq"])
		rank.Index, _ = integer(receipt["index"])
	}
	indexes := func(raw string) (map[int]bool, error) {
		out := map[int]bool{}
		if raw == "" {
			return out, nil
		}
		var list []int
		if err := json.Unmarshal([]byte(raw), &list); err != nil {
			return nil, err
		}
		for _, i := range list {
			out[i] = true
		}
		return out, nil
	}
	writers, err := indexes(row.Writes)
	if err != nil {
		return nil, err
	}
	// A move leaves out its members whose own checks failed (other than for
	// a closed generation); a member whose generation is not live only
	// creates its missing ghost. The operation's overall outcome never filters members.
	refused, err := indexes(row.Refused)
	if err != nil {
		return nil, err
	}
	// A member admitted as of a closed generation (live, or as a recovered
	// receipt decided), or whose generation is no longer live now, is
	// ghost-only (the reference requires admission and a live scope).
	closed, err := indexes(row.Closed)
	if err != nil {
		return nil, err
	}
	moveMember := func(st *queue.Step, e queue.Entry, i int) {
		st.Run = append(st.Run, e)
		if closed[i] || !l.live(e) {
			if st.GhostOnly == nil {
				st.GhostOnly = map[queue.Entry]bool{}
			}
			st.GhostOnly[e] = true
		}
	}
	// keyOf is member i's key write for e (the first eligible occurrence of
	// an entry in one command is its write: aliases are one member, PROTOCOL §7.6).
	keyOf := func(keys map[queue.Entry]queue.Key, i int, e queue.Entry, m map[string]any) {
		if !writers[i] {
			return
		}
		if _, seen := keys[e]; seen {
			return
		}
		k, _ := str(m, "key")
		pid, _ := str(m, "position_id")
		keys[e] = queue.Key{Key: k, PositionID: pid, Rank: rank}
	}
	at := func(raw any) (queue.Place, bool) {
		p, err := parsePlace(raw, "")
		return l.place(p), err == nil
	}
	ref := func(raw any) (EpisodeRef, bool) {
		r, err := parseEpisodeRef(raw, "")
		return r, err == nil && !l.unknown(r)
	}
	switch row.FactType {
	case "queue.add", "queue.restore":
		where, ok := at(payload["place"])
		if !ok {
			return nil, nil
		}
		st := &queue.Step{Kind: queue.StepPlace, Restore: row.FactType == "queue.restore", Place: where, Values: values,
			Keys: map[queue.Entry]queue.Key{}}
		list, _ := payload["members"].([]any)
		for i, raw := range list {
			m, _ := object(raw)
			r, ok := ref(m["episode"])
			if !ok {
				continue
			}
			e := l.entryOf(r)
			st.Run = append(st.Run, e)
			keyOf(st.Keys, i, e, m)
		}
		return st, nil
	case "queue.move":
		r, ok := ref(payload["episode"])
		where, ok2 := at(payload["place"])
		if !ok || !ok2 {
			return nil, nil
		}
		if refused[0] {
			return nil, nil
		}
		e := l.entryOf(r)
		st := &queue.Step{Kind: queue.StepMove, Place: where, Values: values, Keys: map[queue.Entry]queue.Key{}}
		moveMember(st, e, 0)
		keyOf(st.Keys, 0, e, payload)
		return st, nil
	case "queue.reorder":
		if sorting, ok := object(payload["sort"]); ok {
			order, _ := str(sorting, "sort_order")
			st := &queue.Step{Kind: queue.StepSort, Order: order, Values: values}
			perm, _ := sorting["permutation"].([]any)
			for _, raw := range perm {
				if r, ok := ref(raw); ok {
					st.Perm = append(st.Perm, l.entryOf(r))
				}
			}
			// A random sort permutes only the in-members it observed, as
			// mapped now (PROTOCOL §7.6).
			observed, _ := payload["observed"].([]any)
			seen := map[queue.Entry]bool{}
			for _, raw := range observed {
				if r, ok := ref(raw); ok {
					if e := l.entryOf(r); !seen[e] {
						seen[e] = true
						st.Observed = append(st.Observed, e)
					}
				}
			}
			return st, nil
		}
		where, ok := at(payload["place"])
		if !ok {
			return nil, nil
		}
		st := &queue.Step{Kind: queue.StepMove, Place: where, Values: values, Keys: map[queue.Entry]queue.Key{}}
		// A run move's predictions name the run's members exactly; each writes for its member.
		predicted := map[string]map[string]any{}
		members, _ := payload["members"].([]any)
		for _, raw := range members {
			m, _ := object(raw)
			if ep, ok := object(m["episode"]); ok {
				id, _ := str(ep, "episode_id")
				if _, seen := predicted[id]; !seen {
					predicted[id] = m
				}
			}
		}
		run, _ := payload["run"].([]any)
		for i, raw := range run {
			r, ok := ref(raw)
			if !ok || refused[i] {
				continue
			}
			e := l.entryOf(r)
			moveMember(st, e, i)
			if m, ok := predicted[r.EpisodeID]; ok {
				keyOf(st.Keys, i, e, m)
			}
		}
		return st, nil
	case "account.set_queue_policy":
		sorting, ok := object(payload["sorting"])
		if !ok {
			return nil, nil
		}
		keep, _ := boolean(sorting["keep_sorted"])
		order, _ := str(sorting, "sort_order")
		return &queue.Step{Kind: queue.StepPolicy, Sorted: keep, SortBy: order}, nil
	case "session.start", "discovery.propose":
		r, ok := ref(payload["episode"])
		if !ok {
			return nil, nil
		}
		var resolver map[string]any
		if row.Resolver != nil {
			if resolver, err = decodeObject(row.Resolver); err != nil {
				return nil, err
			}
		}
		if row.FactType == "discovery.propose" {
			if action, _ := str(payload, "action"); action != "queue" {
				return nil, nil
			}
		}
		if bundle, ok := object(payload["queue_bundle"]); ok {
			// The device resolved the place (its bundle). A filing lives where
			// its admission's episode lives.
			where, ok := at(bundle["place"])
			if !ok {
				return nil, nil
			}
			e := l.entryOf(r)
			keys := map[queue.Entry]queue.Key{}
			if row.FactType == "discovery.propose" {
				// The filing's key write is its admission's (none for a
				// proposal the server never admitted).
				if resolver == nil {
					values = nil
				} else {
					_, _, result := decisionParts(resolver)
					ep, _ := str(result, "episode_id")
					gen, _ := str(result, "generation_id")
					e = l.decisionEntry(ep, gen)
					values = l.values(result["values"])
					if k, ok := str(result, "key"); ok {
						pid, _ := str(result, "position_id")
						keys[e] = queue.Key{Key: k, PositionID: pid, Rank: decisionRank(resolver)}
					}
				}
			} else {
				keyOf(keys, 0, e, bundle)
			}
			return &queue.Step{Kind: queue.StepPlace, Run: []queue.Entry{e}, Place: where, Values: values, Keys: keys}, nil
		}
		if resolver == nil {
			return nil, nil // no add (the device saw it queued), or a filing the server never admitted
		}
		return l.resolvedStep(resolver)
	}
	return nil, nil
}

// decisionStep is a step a decision is: the server's own filing (an
// admission at its own place) or an import's queue (its manifest).
func (l *stepLoader) decisionStep(d map[string]any) (*queue.Step, error) {
	switch typ, _ := str(d, "decision_type"); typ {
	case "discovery.admission":
		return l.filingStep(d)
	case "import.manifest":
		return l.importStep(d)
	case "import.episode":
		// The imported key is a key write at once (PROTOCOL §7.6), where the episode
		// lives now; its place waits for the import's queue step.
		st := &queue.Step{Kind: queue.StepHold}
		if m, ok := l.importMember(d); ok {
			st.Keys = map[queue.Entry]queue.Key{m.entry: m.key}
		}
		return st, nil
	}
	return nil, nil
}

// decisionRank is a decision's key-write rank: its commit, its index there,
// its ID (PROTOCOL §7.6). A recorded decision without an index ranks
// after every indexed entry of its revision.
func decisionRank(d map[string]any) queue.Rank {
	id, _ := str(d, "decision_id")
	epoch, _ := integer(d["epoch"])
	seq, _ := integer(d["seq"])
	index, ok := integer(d["index"])
	if !ok {
		index = math.MaxInt64
	}
	return queue.Rank{Epoch: epoch, Seq: seq, Index: index, ID: id}
}

// filingStep is the server's own filing (its admission is its place).
func (l *stepLoader) filingStep(d map[string]any) (*queue.Step, error) {
	_, _, result := decisionParts(d)
	if action, _ := str(result, "action"); action != "queue" {
		return nil, nil
	}
	return l.resolvedStep(d)
}

// resolvedStep is a server-resolved placement at its origin's place: a
// play's resolved add, or a filing whose place the server resolved (the
// decision names the place, the key and the values).
func (l *stepLoader) resolvedStep(d map[string]any) (*queue.Step, error) {
	_, _, result := decisionParts(d)
	ep, _ := str(result, "episode_id")
	gen, _ := str(result, "generation_id")
	e := l.decisionEntry(ep, gen)
	st := &queue.Step{Kind: queue.StepPlace, Run: []queue.Entry{e}, Values: l.values(result["values"]), Place: l.decisionPlace(result["place"])}
	if k, ok := str(result, "key"); ok {
		pid, _ := str(result, "position_id")
		st.Keys = map[queue.Entry]queue.Key{e: {Key: k, PositionID: pid, Rank: decisionRank(d)}}
	}
	return st, nil
}

// importStep is an import's whole queue, in the backup's global Queue order
// (PROTOCOL §7.6): every import.episode of the manifest's import
// that holds a queue place, by queue_index (ties by decision ID). It is
// placed only once every decision the manifest's counts call for is
// present (imported.podcasts import.podcast decisions, imported.episodes
// import.episode decisions, imported.queue of them queued): no imported
// subset ever goes live.
func (l *stepLoader) importStep(d map[string]any) (*queue.Step, error) {
	_, inputs, result := decisionParts(d)
	importID, _ := str(inputs, "import_id")
	if err := l.loadImports(); err != nil {
		return nil, err
	}
	counts := l.importCounts[importID]
	inner, _ := object(result["result"])
	imported, _ := object(inner["imported"])
	wantPodcasts, okP := integer(imported["podcasts"])
	wantEpisodes, okE := integer(imported["episodes"])
	wantQueue, okQ := integer(imported["queue"])
	if !okP || !okE || !okQ || counts.podcasts != wantPodcasts || counts.episodes != wantEpisodes || int64(len(l.imports[importID])) != wantQueue {
		return &queue.Step{Kind: queue.StepHold}, nil // incomplete evidence: nothing of the imported queue is placed
	}
	members := append([]importMember(nil), l.imports[importID]...)
	sort.SliceStable(members, func(i, j int) bool {
		if members[i].index != members[j].index {
			return members[i].index < members[j].index
		}
		return members[i].decision < members[j].decision
	})
	// The members' keys were written at their import.episode decisions.
	st := &queue.Step{Kind: queue.StepImport, Values: map[queue.Entry]queue.Values{}}
	seen := map[queue.Entry]bool{}
	for _, m := range members {
		st.Run = append(st.Run, m.entry)
		if !seen[m.entry] {
			seen[m.entry] = true
			if m.values != nil {
				st.Values[m.entry] = *m.values
			}
		}
	}
	return st, nil
}

type importCount struct{ podcasts, episodes int64 }

// loadImports reads, once per load, every import's podcast and episode
// decisions: the counts and the queued episodes.
func (l *stepLoader) loadImports() error {
	if l.importsOK {
		return nil
	}
	l.importsOK = true
	l.importCounts = map[string]importCount{}
	pods, err := l.c.Account.DecisionsOf("import.podcast")
	if err != nil {
		return err
	}
	for _, row := range pods {
		x, err := decodeObject(row.Decision)
		if err != nil {
			return err
		}
		_, in, _ := decisionParts(x)
		id, _ := str(in, "import_id")
		c := l.importCounts[id]
		c.podcasts++
		l.importCounts[id] = c
	}
	rows, err := l.c.Account.DecisionsOf("import.episode")
	if err != nil {
		return err
	}
	for _, row := range rows {
		x, err := decodeObject(row.Decision)
		if err != nil {
			return err
		}
		did, in, result := decisionParts(x)
		id, _ := str(in, "import_id")
		c := l.importCounts[id]
		c.episodes++
		l.importCounts[id] = c
		m, queued, err := l.importMemberOf(did, x, result)
		if err != nil {
			return err
		}
		if queued {
			l.imports[id] = append(l.imports[id], m)
		}
	}
	return nil
}

// importMember is an import.episode decision's queue entry, key write and
// values (ok false when it holds no queue place).
func (l *stepLoader) importMember(d map[string]any) (importMember, bool) {
	did, _, result := decisionParts(d)
	m, ok, err := l.importMemberOf(did, d, result)
	if err != nil {
		l.v.fail(err)
		return m, false
	}
	return m, ok
}

func (l *stepLoader) importMemberOf(did string, d, result map[string]any) (importMember, bool, error) {
	index, ok := integer(result["queue_index"])
	if _, queued := result["queue_key"]; !ok || !queued {
		return importMember{}, false, nil
	}
	ep, _ := str(result, "episode_id")
	gen, _ := str(result, "generation_id")
	g, err := l.c.Account.Generation(gen)
	if err != nil {
		return importMember{}, false, err
	}
	occ, _ := object(result["occurrence"])
	// A group of rows is queued by its earliest queued row, which goes
	// where its own occurrence routes.
	best := int64(-1)
	rowsList, _ := result["rows"].([]any)
	for _, x := range rowsList {
		row, _ := object(x)
		if qi, ok := integer(row["queue_index"]); ok && qi >= 0 && (best < 0 || qi < best) {
			best = qi
			occ, _ = object(row["occurrence"])
		}
	}
	r := EpisodeRef{EpisodeID: ep, PodcastID: g.PodcastID, GenerationID: gen, Occurrence: occ}
	k, _ := str(result, "queue_key")
	pid, _ := str(result, "position_id")
	e := l.entryOf(r)
	var vals *queue.Values
	for _, v := range l.values(result["values"]) {
		vals = &v // the import's one record, wherever its episode lives now
	}
	return importMember{index: index, decision: did, entry: e, key: queue.Key{Key: k, PositionID: pid, Rank: decisionRank(d)}, values: vals}, true, nil
}

// decisionPlace resolves a decision's recorded place: its anchor names an
// episode and generation, routed where they live now.
func (l *stepLoader) decisionPlace(raw any) queue.Place {
	kind, ep, gen, ok := decisionPlace(raw)
	switch {
	case !ok:
		return queue.Place{Kind: queue.PlaceEnd}
	case kind == "start":
		return queue.Place{Kind: queue.PlaceStart}
	case kind == "after":
		return queue.Place{Kind: queue.PlaceAfter, After: l.decisionEntry(ep, gen)}
	}
	return queue.Place{Kind: queue.PlaceEnd}
}

// place resolves a placement intent's anchor to its entry.
func (l *stepLoader) place(p place) queue.Place {
	switch p.kind {
	case "start":
		return queue.Place{Kind: queue.PlaceStart}
	case "after":
		return queue.Place{Kind: queue.PlaceAfter, After: l.entryOf(*p.after)}
	}
	return queue.Place{Kind: queue.PlaceEnd}
}

// values reads recorded sort values (a receipt's sort_values, a decision's inputs values) by
// entry. Values recorded for two aliases of one survivor resolve to the survivor's own record if
// it has one, else to the alias registered first: the rank of its registering node, then its
// episode ID (PROTOCOL §7.6). Podcast IDs follow podcast merges.
func (l *stepLoader) values(raw any) map[queue.Entry]queue.Values {
	list, _ := raw.([]any)
	if len(list) == 0 {
		return nil
	}
	out := map[queue.Entry]queue.Values{}
	from := map[queue.Entry]string{}
	for _, x := range list {
		m, _ := object(x)
		ep, _ := str(m, "episode_id")
		gen, _ := str(m, "generation_id")
		e := l.decisionEntry(ep, gen)
		own, _ := scopeEpisode(string(e))
		if prev, ok := from[e]; ok && (prev == own || (ep != own && !l.registeredBefore(ep, prev))) {
			continue
		}
		from[e] = ep
		v, _ := object(m["values"])
		out[e] = l.valuesOf(v)
	}
	return out
}

// registeredBefore orders two aliases by their registration rank, then
// episode ID.
func (l *stepLoader) registeredBefore(a, b string) bool {
	ra, rb := l.registration(a), l.registration(b)
	if ra != rb {
		return ra.Less(rb)
	}
	return a < b
}

func (l *stepLoader) registration(e string) storage.Registration {
	if r, ok := l.regs[e]; ok {
		return r
	}
	r, err := l.c.Account.EpisodeRegistration(e)
	if err != nil {
		l.v.fail(err)
	}
	if l.regs == nil {
		l.regs = map[string]storage.Registration{}
	}
	l.regs[e] = r
	return r
}

func (l *stepLoader) valuesOf(m map[string]any) queue.Values {
	var v queue.Values
	v.Title, _ = str(m, "title")
	v.PodcastID, _ = str(m, "podcast_id")
	v.PodcastTitle, _ = str(m, "podcast_title")
	v.PublicationTime, _ = integer(m["publication_time"])
	v.Duration, _ = integer(m["duration"])
	if v.PodcastID != "" {
		v.PodcastID = l.v.podcastRoot(v.PodcastID)
	}
	return v
}

func decodeObject(raw []byte) (map[string]any, error) {
	v, err := contract.DecodeJSON(raw)
	if err != nil {
		return nil, err
	}
	m, _ := v.(map[string]any)
	return m, nil
}

// ---- publication (PROTOCOL §7.6)

// publishedKeys are the entries' published keys: each one's latest key write
// in rank order (PROTOCOL §7.6). The replay derives the writes of its
// steps (a placement's for the members it actually places, a move's, a
// filing's, a resolved add's, an import's); a layout's writes stay on their
// exact (episode, generation).
func publishedKeys(c *protocol.ApplyContext, st *queue.State) (map[queue.Entry]queue.Key, error) {
	writes := st.KeyWrites()
	out := make(map[queue.Entry]queue.Key, len(writes))
	for e, k := range writes {
		out[e] = k
	}
	rows, err := c.Account.KeyWrites()
	if err != nil {
		return nil, err
	}
	for _, w := range rows {
		e := queue.Entry(w.Scope)
		k := queue.Key{Key: w.Key, PositionID: w.PositionID, Rank: queue.Rank{Epoch: w.Epoch, Seq: w.Seq, Index: w.Index, ID: w.Node}}
		if cur, ok := out[e]; ok && !cur.Rank.Less(k.Rank) {
			continue
		}
		out[e] = k
	}
	return out, nil
}

// keysFit reports whether every in-member has a published key and the keys
// strictly increase in the replay's order (missing or equal keys are a
// mismatch, never a display tie-break).
func keysFit(order []queue.Entry, pub map[queue.Entry]queue.Key) bool {
	var prev *contract.QueueKey
	for _, e := range order {
		k, ok := pub[e]
		if !ok {
			return false
		}
		q, err := contract.ParseQueueKey(k.Key)
		if err != nil || (prev != nil && contract.CompareQueueKeys(*prev, q) >= 0) {
			return false
		}
		prev = &q
	}
	return true
}

// publishQueue is the queue's publication at the end of a commit
// (protocol.QueuePublisher): no commit publishes keys that differ from the
// replay's order. An in-member's published key is its latest key write; when
// those do not strictly increase in the replay's order, and layout is set,
// one layout records fresh keys for the members off the first longest
// increasing run of published keys, and only for them (PROTOCOL §7.6).
// A queue_position record is the published key of the scope an episode's
// membership record shows; it is republished only when its value changes
// (an unchanged record keeps its provenance). full replays from the start
// and checks every record (a rebuild, an upload that joined earlier entries).
func publishQueue(c *protocol.ApplyContext, full, layout bool) error {
	touched, all := c.QueueChanges(false)
	keysChanged := c.QueueKeysChangedSince(true)
	changed := c.CommitRecordIDs("episode", "queue_membership")
	cr0, haveMemo := (*c.QueueMemo()).(*commitReplay)
	haveMemo = haveMemo && cr0 != nil
	head, err := c.Account.Head()
	if err != nil {
		return err
	}
	var cache *replayCache
	if v, ok := c.Account.Memo().Load(memoKey{c.AccountID}); ok && v.(*replayCache).valid(head) {
		cache = v.(*replayCache)
	}
	if !full && !all && !keysChanged && len(touched) == 0 && len(changed) == 0 && !haveMemo && cache != nil && cache.pub != nil {
		// Nothing the replay, the keys or the records read changed, and the
		// last publication was consistent: the same immutable snapshots and
		// keys stay valid for the head this commit leaves (an unrelated commit must not cost the
		// next queue command a full replay). The shared cache is never mutated.
		memo, account, snaps, pub := c.Account.Memo(), c.AccountID, cache.snaps, cache.pub
		c.AfterCommit(func(head storage.Head) {
			installCache(memo, account, &replayCache{head: head, snaps: snaps, pub: pub})
		})
		return nil
	}
	cr, err := replay(c, full)
	if err != nil {
		return err
	}
	st := cr.snaps[len(cr.snaps)-1].state
	order := st.Visible()
	pub, err := publishedKeys(c, st)
	if err != nil {
		return err
	}
	consistent := keysFit(order, pub)
	if !consistent && layout {
		cands := make(map[queue.Entry]queue.Candidate, len(order))
		for _, e := range order {
			k, ok := pub[e]
			if !ok {
				continue
			}
			q, err := contract.ParseQueueKey(k.Key)
			u, okID := parsePositionID(k.PositionID)
			if err != nil || !okID {
				continue
			}
			cands[e] = queue.Candidate{Key: q, PositionID: u, OK: true}
		}
		assigned, err := queue.Materialize(order, cands, c.Random().PositionID)
		if err != nil {
			return err
		}
		var fresh []queue.Assigned
		for _, a := range assigned {
			if !a.Kept {
				fresh = append(fresh, a)
			}
		}
		d, err := recordLayout(c, fresh)
		if err != nil {
			return err
		}
		if d != nil {
			if err := applyLayout(c, d); err != nil {
				return err
			}
			r := decisionRank(d)
			for _, a := range fresh {
				pub[a.Entry] = queue.Key{Key: a.Key.String(), PositionID: formatUUID(a.PositionID), Rank: r}
			}
			consistent = true
		}
	}
	// The episodes whose queue_position record may change: membership
	// records this commit changed, and entries whose published key changed
	// since the last publication (every keyed or recorded episode when there
	// is no trusted last publication).
	dirty := map[string]bool{}
	for _, ep := range changed {
		dirty[ep] = true
	}
	if full || all || cache == nil || cache.pub == nil {
		for _, group := range []string{"queue_membership", "queue_position"} {
			ids, err := c.Account.LiveRecordIDs("episode", group)
			if err != nil {
				return err
			}
			for _, ep := range ids {
				dirty[ep] = true
			}
		}
		for e := range pub {
			ep, _ := scopeEpisode(string(e))
			dirty[ep] = true
		}
	} else {
		for e, k := range pub {
			if p, ok := cache.pub[e]; !ok || p != k {
				ep, _ := scopeEpisode(string(e))
				dirty[ep] = true
			}
		}
		for e := range cache.pub {
			if _, ok := pub[e]; !ok {
				ep, _ := scopeEpisode(string(e))
				dirty[ep] = true
			}
		}
	}
	eps := make([]string, 0, len(dirty))
	for ep := range dirty {
		eps = append(eps, ep)
	}
	sort.Strings(eps)
	for _, ep := range eps {
		if err := publishPosition(c, ep, pub); err != nil {
			return err
		}
	}
	if consistent {
		installReplay(c, cr, pub)
	}
	return nil
}

// publishPosition publishes an episode's queue_position record: the
// published key of the scope its membership record shows, with the winning
// write's node as provenance; absent without one. A record whose value does
// not change keeps its provenance (in a rebuild, the one it had before), which
// never decides the winning write (PROTOCOL §7.6).
func publishPosition(c *protocol.ApplyContext, ep string, pub map[queue.Entry]queue.Key) error {
	m, err := recordValue(c.Account, "episode", ep, "queue_membership")
	if err != nil {
		return err
	}
	var want map[string]any
	var writer string
	if m != nil {
		gen, _ := str(m, "generation_id")
		if k, ok := pub[queue.Entry(protocol.EpisodeScope(gen, ep))]; ok {
			want = map[string]any{"generation_id": gen, "key": k.Key, "position_id": k.PositionID}
			writer = k.Rank.ID
		}
	}
	if want == nil {
		return c.TombstoneRecord("episode", ep, "queue_position")
	}
	b, err := contract.Canonical(want)
	if err != nil {
		return err
	}
	provenance := []string{writer}
	if prior, err := c.Account.PriorRecord("episode", ep, "queue_position"); err == nil && prior.Value == string(b) {
		var kept []string
		if err := json.Unmarshal([]byte(prior.Provenance), &kept); err == nil {
			provenance = kept
		}
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return c.PublishDerived("episode", ep, "queue_position", want, provenance)
}

// recordLayout records a queue.layout (PROTOCOL §7.6): subject the account's queue;
// dependencies and inputs.frontier the account's whole causal frontier (which
// covers every earlier layout write); inputs.mapping_version the number of
// identity-mapping decisions held; inputs.policy the node that decided the
// sorting register (or null); result the members whose key changes, in the
// replay's order, with their fresh keys and position IDs. It ranks after
// every write it corrects: it is the commit's last decision, in the
// account's current epoch (PROTOCOL §6.11 moved it past any recovered write).
func recordLayout(c *protocol.ApplyContext, fresh []queue.Assigned) (map[string]any, error) {
	acct := c.Account
	deps, err := acct.CausalFrontier()
	if err != nil {
		return nil, err
	}
	cs, err := accountContributions(acct, -1)
	if err != nil {
		return nil, err
	}
	var policy any
	if by := projection.FoldAccount(cs).SortingBy; by != "" {
		policy = by
	}
	mapping, err := acct.IdentityMappingCount()
	if err != nil {
		return nil, err
	}
	frontier := make([]any, len(deps))
	for i, d := range deps {
		frontier[i] = d
	}
	members := make([]any, len(fresh))
	for i, a := range fresh {
		ep, gen := scopeEpisode(string(a.Entry))
		members[i] = map[string]any{"episode_id": ep, "generation_id": gen, "key": a.Key.String(), "position_id": formatUUID(a.PositionID)}
	}
	return c.Decide(protocol.DecisionInput{Type: "queue.layout", Version: 1, Subject: "queue", Dependencies: deps,
		Inputs: map[string]any{"frontier": frontier, "mapping_version": mapping, "policy": policy}, Result: map[string]any{"members": members}})
}

// applyLayout applies a queue.layout, made here or archived: its keys are
// key writes at its rank on the exact (episode, generation) it lists (never
// routed to a survivor); it joins the queue frontier. It is never applied as
// order (PROTOCOL §7.6: a recovered layout is evidence of what was shown).
func applyLayout(c *protocol.ApplyContext, d map[string]any) error {
	id, _, result := decisionParts(d)
	r := decisionRank(d)
	list, _ := result["members"].([]any)
	for _, x := range list {
		m, _ := object(x)
		ep, _ := str(m, "episode_id")
		gen, _ := str(m, "generation_id")
		key, _ := str(m, "key")
		pid, _ := str(m, "position_id")
		if err := c.Account.WriteKey(storage.KeyWrite{Scope: protocol.EpisodeScope(gen, ep), Key: key, PositionID: pid, Node: id,
			Epoch: r.Epoch, Seq: r.Seq, Index: r.Index}); err != nil {
			return err
		}
	}
	return c.Account.AddQueueNode(id)
}

// installReplay keeps the commit's snapshots and published keys for the
// account once the commit is durable, tagged with the head it leaves.
func installReplay(c *protocol.ApplyContext, cr *commitReplay, pub map[queue.Entry]queue.Key) {
	memo, account := c.Account.Memo(), c.AccountID
	c.AfterCommit(func(head storage.Head) {
		installCache(memo, account, &replayCache{head: head, snaps: cr.snaps, pub: pub})
	})
}

// admitLayout admits an archived queue.layout (PROTOCOL §6.11): evidence of what was
// shown and key writes like any other, never applied as order; if the keys
// it leaves do not give the replay's order, the publication rule records a
// corrective layout (PROTOCOL §7.6).
func admitLayout(c *protocol.ApplyContext, d map[string]any) error {
	c.QueueKeysChanged()
	return applyLayout(c, d)
}
