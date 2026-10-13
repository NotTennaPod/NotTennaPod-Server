package ops

// The queue replay in the server (PROTOCOL §7.6):
// incremental replay against a full replay after every commit, the
// publication invariant, and the cases the replay decides only with the
// server's other folds (listening claims, filings, identities, recovery).

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/queue"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// checkReplay compares, after a commit, the replay state the commit left
// for the account (its snapshots, if they are current) with a full replay
// from the start, and the published queue with the full replay's order
// (PROTOCOL §7.6 every-prefix cross-check; PROTOCOL §7.6 invariant).
func checkReplay(t testing.TB, svc *protocol.Service, account string) {
	t.Helper()
	err := svc.Probe(context.Background(), account, func(c *protocol.ApplyContext) error {
		head, err := c.Account.Head()
		if err != nil {
			return err
		}
		var cached *replayCache
		if v, ok := c.Account.Memo().Load(memoKey{account}); ok {
			if rc := v.(*replayCache); rc.valid(head) {
				cached = rc
			}
		}
		*c.QueueMemo() = nil
		full, err := replay(c, true)
		if err != nil {
			return err
		}
		fs := full.snaps[len(full.snaps)-1].state
		if cached != nil {
			cs := cached.snaps[len(cached.snaps)-1].state
			if a, b := replayEntries(cs.Structure()), replayEntries(fs.Structure()); a != b {
				t.Errorf("incremental replay structure\n %s\nfull replay\n %s", a, b)
			}
			if a, b := replayEntries(cs.Visible()), replayEntries(fs.Visible()); a != b {
				t.Errorf("incremental replay queue %s, full replay %s", a, b)
			}
			// Every kept snapshot is the full replay's state after its node.
			for _, s := range cached.snaps {
				if s.node == "" {
					continue
				}
				var at *queue.State
				for _, x := range full.snaps {
					if x.node == s.node {
						at = x.state
					}
				}
				if at != nil && replayEntries(at.Structure()) != replayEntries(s.state.Structure()) {
					t.Errorf("snapshot after %s differs from the full replay's", s.node)
				}
			}
		}
		// What a phone shows: the in-members (membership records in the
		// queue) sorted by their queue_position records.
		ids, err := c.Account.LiveRecordIDs("episode", "queue_membership")
		if err != nil {
			return err
		}
		type shown struct {
			ep  string
			key contract.QueueKey
		}
		var list []shown
		for _, ep := range ids {
			m, err := recordValue(c.Account, "episode", ep, "queue_membership")
			if err != nil {
				return err
			}
			if in, _ := m["in_queue"].(bool); !in {
				continue
			}
			pos, err := recordValue(c.Account, "episode", ep, "queue_position")
			if err != nil {
				return err
			}
			if pos == nil || pos["generation_id"] != m["generation_id"] {
				t.Errorf("in-member %s has no queue_position record of its generation", ep)
				continue
			}
			k, err := contract.ParseQueueKey(pos["key"].(string))
			if err != nil {
				return err
			}
			list = append(list, shown{ep, k})
		}
		sort.Slice(list, func(i, j int) bool { return contract.CompareQueueKeys(list[i].key, list[j].key) < 0 })
		var pub, want []string
		for _, e := range list {
			pub = append(pub, e.ep)
		}
		for _, e := range fs.Visible() {
			ep, _ := scopeEpisode(string(e))
			want = append(want, ep)
		}
		if strings.Join(pub, ",") != strings.Join(want, ",") {
			t.Errorf("published queue\n %v\ndiffers from the replay's order\n %v", pub, want)
		}
		return nil
	})
	if err != nil && !errorsIsClosed(err) {
		t.Errorf("replay check: %v", err)
	}
}

func errorsIsClosed(err error) bool { return err == storage.ErrClosed }

func replayEntries(es []queue.Entry) string {
	out := make([]string, len(es))
	for i, e := range es {
		ep, gen := scopeEpisode(string(e))
		if len(ep) > 11 {
			ep = ep[:11]
		}
		if len(gen) > 7 {
			gen = gen[:7]
		}
		out[i] = gen + "/" + ep
	}
	return strings.Join(out, " ")
}

// PROTOCOL §7.6: a resume claim ordered before a completion cancels its queue
// removal; a later sort then sorts both members, and the commit that only
// carries the listening fact publishes the new order (no commit publishes
// keys that differ from the replay's order).
func TestClaimCancellingCompletionRepublishesOrder(t *testing.T) {
	q := newQueueEnv(t, "Z", "A")
	p, b := q.device("P"), q.device("B")
	q.seed(t, p, "Z", "A")
	sp, sb := newID(), newID()
	q.clock += minute
	startB := q.lfact(b, "session.start", q.refs["Z"], start(sb, 0, true), p.last) // t5
	q.pushOK(t, b, startB, "applied")
	q.clock += minute
	q.pushOK(t, b, q.lfact(b, "session.checkpoint", q.refs["Z"], checkpoint(sb, 1, minute, minute)), "applied")
	q.clock += minute
	startP := q.lfact(p, "session.start", q.refs["Z"], start(sp, minute, true), b.last) // t10: P takes over
	q.pushOK(t, p, startP, "applied")
	// B, offline and unaware of P, resumes (t25).
	q.clock += minute
	resumeB := q.lfact(b, "session.checkpoint", q.refs["Z"], resume(sb, 2, minute, minute, startB["op_id"].(string)))
	// P, offline and unaware of B's resume, finishes Z (t30) and sorts by
	// title (t40), seeing only A.
	q.clock += minute
	endP := q.lfact(p, "session.end", q.refs["Z"], end(sp, 1, 59*minute, 58*minute, "ended", true, true, true))
	q.clock += minute
	sortP := q.sortCmd(p, "episode_title_a_z", []string{"A"}, nil)
	q.clock += 10 * minute
	q.pushOK(t, p, endP, "applied")
	q.pushOK(t, p, sortP, "applied")
	q.expectOrder(t, "A")
	before := q.layouts(t)
	q.pushOK(t, b, resumeB, "applied")
	q.expectOrder(t, "A,Z")
	if q.layouts(t) == before {
		t.Fatal("the listening fact's commit published the new order without a layout")
	}
}

// Through the server: D's automatic filing (a proposal, filed after Z by
// its device) is suppressed by a concurrent explicit removal; X, added after
// D by a phone that saw the filing, stays between Z and Y, after D's ghost,
// in both arrival orders.
func TestGhostOfSuppressedFilingAnchorsAnAdd(t *testing.T) {
	for _, removalFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "add first", true: "removal first"}[removalFirst], func(t *testing.T) {
			c := newCatalogEnv(t, "Z", "Y", "D", "X")
			a, b, w := c.device("A"), c.device("B"), c.device("W")
			c.seed(t, a, "Z", "Y")
			c.pushOK(t, a, c.observe(a, map[string]int64{"D": c.clock}, "D"), "applied")
			c.clock += minute
			prop := c.propose(w, "D", "queue", c.clock, c.clock-minute)
			k, id := keyBetween(t, "", "")
			prop["payload"].(map[string]any)["queue_bundle"] = map[string]any{"key": k, "position_id": id, "place": c.after("Z")}
			c.pushOK(t, w, prop, "applied")
			c.expectOrder(t, "Z,D,Y")
			c.sees(a, w.last)
			c.sees(b, w.last)
			c.clock += minute
			add := c.add(t, a, c.after("D"), "", "X")
			removal := c.remove(b, "D")
			if removalFirst {
				c.pushOK(t, b, removal, "applied")
				c.pushOK(t, a, add, "applied")
			} else {
				c.pushOK(t, a, add, "applied")
				c.pushOK(t, b, removal, "applied")
			}
			c.expectOrder(t, "Z,X,Y")
		})
	}
}

// PROTOCOL §7.6: old and new generations are different entries. Q's X was
// queued in G1, which closes; re-subscribed (G2), X is queued again at the
// end. A phone that saw G1's queue adds Y after G1's X: Y lands at G1's X's
// place (its ghost), not after G2's X.
func TestReplayKeepsGenerationClasses(t *testing.T) {
	q := newQueueEnv(t, "B", "Y")
	a, off := q.device("A"), q.device("Off")
	sub := q.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/other"})
	q.pushOK(t, a, sub, "applied")
	p2 := contract.PodcastID(q.namespace, sub["op_id"].(string))
	g1 := contract.GenerationID(q.namespace, p2, sub["op_id"].(string))
	x1 := q.episodeRef(p2, g1, "x")
	q.refs["X"] = x1
	q.seed(t, a, "X", "B")
	q.sees(off, a.last)
	q.clock += minute
	// Off (offline, saw [X, B]) adds Y after X (G1).
	addY := q.add(t, off, q.after("X"), "", "Y")
	q.clock += minute
	unsub := q.fact(a, "podcast.unsubscribe", []string{a.last}, podTarget(p2, g1), map[string]any{})
	q.pushOK(t, a, unsub, "applied")
	q.clock += minute
	sub2 := q.fact(a, "podcast.subscribe", []string{a.last}, []any{}, map[string]any{"feed_url": "https://example.com/other",
		"observed": map[string]any{"podcast_id": p2, "generation_id": g1, "state": "removed"}})
	q.pushOK(t, a, sub2, "applied")
	g2 := contract.GenerationID(q.namespace, p2, sub2["op_id"].(string))
	x2 := q.episodeRef(p2, g2, "x")
	q.refs["X"] = x2
	q.clock += minute
	q.pushOK(t, a, q.add(t, a, atEnd(), "", "X"), "applied")
	q.expectOrder(t, "B,X")
	q.pushOK(t, off, addY, "applied")
	q.expectOrder(t, "Y,B,X")
}

// Through the server: an import's queue is placed in the backup's global
// order at its manifest, and later placements anchor to its entries.
func TestImportThenPlacements(t *testing.T) {
	e := newEnv(t)
	e.replayCheck = true
	day := int64(86_400_000)
	x := importPodcast(e.clock,
		importItem{"guid-a1", "https://cdn.example.com/a1.mp3", e.clock - 3*day, ImportRow{QueueIndex: 0, QueueKey: "", PositionID: ""}},
		importItem{"guid-a2", "https://cdn.example.com/a2.mp3", e.clock - 2*day, ImportRow{QueueIndex: 1}})
	x.Rows[0].QueueIndex, x.Rows[1].QueueIndex = 1, 0 // the backup's queue: a2, then a1
	for i := range x.Rows {
		u := [16]byte{byte(i + 1)}
		prev := (*contract.QueueKey)(nil)
		if i == 1 {
			k, _ := contract.ParseQueueKey(x.Rows[0].QueueKey)
			prev = &k
		}
		k, err := contract.QueueKeyBetween(prev, nil, u)
		if err != nil {
			t.Fatal(err)
		}
		x.Rows[i].QueueKey, x.Rows[i].PositionID = k.String(), formatUUID(u)
	}
	p := e.applyImport(t, x)
	q := &queueEnv{listenEnv: &listenEnv{env: e, p: p.PodcastID, g: p.GenerationID}, refs: map[string]map[string]any{}}
	for i, title := range []string{"A1", "A2"} {
		ep := p.EpisodeOf[int64(i+1)]
		guid := "guid-" + strings.ToLower(title)
		q.refs[title] = map[string]any{"episode_id": ep, "podcast_id": p.PodcastID, "generation_id": p.GenerationID,
			"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": guid, "guid": guid}}
	}
	q.expectOrder(t, "A2,A1")
	d := q.device("A")
	sub := q.fact(d, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/other"})
	q.pushOK(t, d, sub, "applied")
	pod := contract.PodcastID(q.namespace, sub["op_id"].(string))
	q.refs["X"] = q.episodeRef(pod, contract.GenerationID(q.namespace, pod, sub["op_id"].(string)), "x")
	q.clock += minute
	q.pushOK(t, d, q.add(t, d, q.after("A2"), "", "X"), "applied")
	q.expectOrder(t, "A2,X,A1")
}

// The incremental replay is used: a live add after an earlier commit resumes
// from the cached state (PROTOCOL §7.6).
func TestReplayResumesFromSnapshot(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	a := q.device("A")
	q.replayCheck = false // the check's full replays would count too
	q.seed(t, a, "A", "B")
	_, before := replayCounts()
	q.clock += minute
	q.pushOK(t, a, q.add(t, a, atEnd(), "", "C"), "applied")
	if _, after := replayCounts(); after <= before {
		t.Fatalf("no incremental replay (%d → %d)", before, after)
	}
	checkReplay(t, q.svc, q.account)
}

// PROTOCOL §7.6 and §6.11: recovery from one fixed archive. After a restore to before
// a week of queue commands (moves, adds, a sort, under keep-sorted for a
// while), recovering the lost entries gives the replay's order of the run
// without a restore, whether they come in one request, commit by commit, or
// as facts first and layouts last (a request carrying only layouts
// still publishes keys that give the replay's order; layouts are never
// applied as order).
func TestQueueRecoveryFromFixedArchive(t *testing.T) {
	type variant struct {
		name    string
		uploads func(lost []replicaCommit, x, y *device) [][]protocol.RecoveryEntry
	}
	variants := []variant{
		{"one request", func(lost []replicaCommit, x, y *device) [][]protocol.RecoveryEntry {
			return [][]protocol.RecoveryEntry{entries(lost)}
		}},
		{"commit by commit", func(lost []replicaCommit, x, y *device) [][]protocol.RecoveryEntry {
			var out [][]protocol.RecoveryEntry
			for _, c := range lost {
				out = append(out, c.entries)
			}
			return out
		}},
		{"facts, then only layouts", func(lost []replicaCommit, x, y *device) [][]protocol.RecoveryEntry {
			var facts, layouts []protocol.RecoveryEntry
			for _, c := range lost {
				for _, e := range c.entries {
					if e.Decision != nil && e.Decision["decision_type"] == "queue.layout" {
						layouts = append(layouts, e)
					} else {
						facts = append(facts, e)
					}
				}
			}
			return [][]protocol.RecoveryEntry{facts, layouts}
		}},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			q := newQueueEnv(t, "A", "B", "C", "D", "E")
			a, x, y := q.device("A"), q.device("X"), q.device("Y")
			q.seed(t, a, "A", "B", "C")
			backup := q.backup(t)
			atBackup := q.headSeq(t)
			q.sees(x, a.last)
			q.sees(y, a.last)
			q.clock += minute
			q.pushOK(t, x, q.move(t, x, "C", atStart()), "applied")
			q.clock += minute
			q.pushOK(t, y, q.add(t, y, q.after("A"), "", "D"), "applied")
			q.clock += minute
			q.pushOK(t, x, q.policy(x, titleSorting(true)), "applied")
			q.clock += minute
			q.pushOK(t, y, q.add(t, y, atStart(), "", "E"), "applied")
			q.clock += minute
			q.pushOK(t, x, q.policy(x, titleSorting(false)), "applied")
			q.clock += minute
			q.pushOK(t, x, q.move(t, x, "A", atEnd()), "applied")
			want := q.order(t)
			if want != "B,C,D,E,A" {
				t.Fatalf("before the restore: %s", want)
			}
			if q.layouts(t) == 0 {
				t.Fatal("the archive holds no layout")
			}
			lost := q.logCommits(t, atBackup)
			q.restore(t, backup)
			q.expectOrder(t, "A,B,C")
			x2 := q.successor(t, x)
			rec, _ := q.startRecovery(t, x2, nil)
			for i, list := range v.uploads(lost, x, y) {
				resp := rec.upload(t, list)
				for _, r := range resp.Results {
					if r.Status != "joined" && r.Status != "known" {
						t.Fatalf("request %d: %s %s", i, r.ID, r.Status)
					}
				}
				checkReplay(t, q.svc, q.account)
			}
			q.expectOrder(t, want)
		})
	}
}

// PROTOCOL §7.6 through the server: two identities of one episode were queued
// on two phones (the old ID at the back, the new one at the front); a GUID
// change merges them. The replay applies the final mapping from the start:
// the later add is a duplicate of the survivor's, so the queue is Z,A, not
// one of the two places merged.
func TestReplayAppliesTheFinalMappingFromTheStart(t *testing.T) {
	c := newCatalogEnv(t, "Z")
	x, y := c.device("X"), c.device("Y")
	d1 := c.clock - 3*day
	c.serverFetch(t, idFeed(t, c.clock, idItem{"old-a", "Episode A", "https://old.example.com/a.mp3", d1}), "")
	oldA := c.idOf("old-a")
	c.refs["A"] = c.idRef("old-a", "https://old.example.com/a.mp3", d1)
	newRef := c.idRef("new-a", "https://new.example.com/a.mp3", d1)
	obs := map[string]any{"podcast_id": c.p, "batch_id": newID(), "parser_version": "phone/1", "fetch_time": c.clock,
		"items": []any{map[string]any{"episode": newRef,
			"fields": fullItem(map[string]any{"title": "Episode A", "pub_date": d1,
				"media": map[string]any{"url": "https://new.example.com/a.mp3", "size": "20000", "mime_type": "audio/mpeg", "duration": int64(0)}})}}}
	bb, _ := contract.Canonical(obs)
	v, _ := contract.DecodeJSON(bb)
	targets := []any{map[string]any{"entity": "episode", "id": newRef["episode_id"]}, map[string]any{"entity": "podcast", "id": c.p}}
	c.pushOK(t, y, c.fact(y, "catalog.observe", nil, targets, v.(map[string]any)), "applied")
	c.seed(t, x, "Z")
	c.clock += minute
	c.pushOK(t, x, c.add(t, x, atEnd(), "", "A"), "applied")
	c.refs["newA"] = newRef
	c.clock += minute
	c.pushOK(t, y, c.add(t, y, atStart(), "", "newA"), "applied")
	delete(c.refs, "newA")
	c.expectOrderByEpisode(t, newRef["episode_id"].(string), "Z", "A")
	c.clock += minute
	c.serverFetch(t, idFeed(t, c.clock, idItem{"new-a", "Episode A", "https://new.example.com/a.mp3", d1}), "")
	if root := c.mergeRoot(t, newRef["episode_id"].(string)); root != oldA {
		t.Fatalf("precondition: the new ID merges into the old (root %s)", root)
	}
	c.expectOrder(t, "Z,A")
	checkReplay(t, c.svc, c.account)
}

// expectOrderByEpisode checks the queue before a merge: an extra episode
// first, then titles.
func (q *queueEnv) expectOrderByEpisode(t *testing.T, first string, titles ...string) {
	t.Helper()
	recs := q.records()
	if !strings.Contains(recs["episode/"+first+"/queue_membership"], `"in_queue":true`) {
		t.Fatalf("precondition: %s queued", first)
	}
	q.expectOrder(t, strings.Join(titles, ","))
}

func (q *queueEnv) mergeRoot(t *testing.T, episode string) string {
	t.Helper()
	var root string
	if err := q.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		var err error
		root, err = tx.Account(q.account).MergeRoot(episode)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return root
}

// A move's receipt records its member's values (moves are placements too).
func TestMoveReceiptRecordsValues(t *testing.T) {
	q := newQueueEnv(t, "A", "B")
	a := q.device("A")
	q.seed(t, a, "A", "B")
	q.clock += minute
	r := q.push(a, q.move(t, a, "A", atEnd()))
	vals, _ := r[0].Receipt["sort_values"].([]any)
	if len(vals) != 1 || vals[0].(map[string]any)["episode_id"] != q.refs["A"]["episode_id"] ||
		vals[0].(map[string]any)["values"].(map[string]any)["title"] != "A" {
		t.Fatalf("a move's receipt records %v", r[0].Receipt["sort_values"])
	}
	q.expectOrder(t, "B,A")
}

// A refused move never moves an entry. A move (and a run move without
// predictions) naming B under another podcast is
// not_applied/unknown_generation, and the queue stays [A, B], also after a
// rebuild (the env's rebuild invariant).
func TestRefusedMoveMovesNothing(t *testing.T) {
	for _, run := range []bool{false, true} {
		t.Run(map[bool]string{false: "move", true: "run move"}[run], func(t *testing.T) {
			q := newQueueEnv(t, "A", "B")
			a := q.device("A")
			sub := q.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/other"})
			q.pushOK(t, a, sub, "applied")
			other := contract.PodcastID(q.namespace, sub["op_id"].(string))
			q.seed(t, a, "A", "B")
			bad := map[string]any{}
			for k, v := range q.refs["B"] {
				bad[k] = v
			}
			bad["podcast_id"] = other // B's real generation, another podcast
			q.clock += minute
			var f map[string]any
			if run {
				f = q.qfact(a, "queue.reorder", map[string]any{"run": []any{bad}, "place": atStart()}, []string{"B"})
			} else {
				k, id := keyBetween(t, "", "")
				f = q.qfact(a, "queue.move", map[string]any{"episode": bad, "key": k, "position_id": id, "place": atStart()}, []string{"B"})
			}
			if r := q.push(a, f); outcome(r[0]) != "not_applied/unknown_generation" {
				t.Fatalf("the move: %s", outcome(r[0]))
			}
			q.expectOrder(t, "A,B")
		})
	}
}

// [A, B] of G1; an offline phone moves B to the front, then adds X after A and
// Y after B (another, open podcast); another phone closes G1 before those
// arrive. The move only ghosts (B's entry stays), so X and Y keep the
// structure's order: [X, Y], also after a rebuild.
func TestClosedGenerationMoveOnlyGhosts(t *testing.T) {
	q := newQueueEnv(t, "A", "B")
	a, off := q.device("A"), q.device("Off")
	sub := q.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/other"})
	q.pushOK(t, a, sub, "applied")
	p2 := contract.PodcastID(q.namespace, sub["op_id"].(string))
	g2 := contract.GenerationID(q.namespace, p2, sub["op_id"].(string))
	q.refs["X"], q.refs["Y"] = q.episodeRef(p2, g2, "x"), q.episodeRef(p2, g2, "y")
	q.seed(t, a, "A", "B")
	q.sees(off, a.last)
	q.clock += minute
	move := q.move(t, off, "B", atStart())
	q.clock += minute
	addX := q.add(t, off, q.after("A"), "", "X")
	q.clock += minute
	addY := q.add(t, off, q.after("B"), "", "Y")
	q.clock += minute
	unsub := q.fact(a, "podcast.unsubscribe", []string{a.last}, podTarget(q.p, q.g), map[string]any{})
	q.pushOK(t, a, unsub, "applied")
	if r := q.push(off, move); outcome(r[0]) != "not_applied/generation_closed" {
		t.Fatalf("the move: %s", outcome(r[0]))
	}
	q.pushOK(t, off, addX, "applied")
	q.pushOK(t, off, addY, "applied")
	q.expectOrder(t, "X,Y")
}

// A random sort permutes only the in-members it observed as mapped now.
// Observed under an unknown (well formed) generation, the live entries are not
// observed: nothing moves.
func TestRandomSortOutsideObservedMovesNothing(t *testing.T) {
	q := newQueueEnv(t, "A", "B")
	a := q.device("A")
	q.seed(t, a, "A", "B")
	unknown := contract.GenerationID(q.namespace, q.p, "00000000-0000-4000-8000-00000000beef")
	under := func(title string) map[string]any {
		r := map[string]any{}
		for k, v := range q.refs[title] {
			r[k] = v
		}
		r["generation_id"] = unknown
		return r
	}
	q.clock += minute
	payload := map[string]any{"sort": map[string]any{"sort_order": "random", "permutation": []any{q.refs["B"], q.refs["A"]}},
		"observed": []any{under("A"), under("B")}}
	b, _ := contract.Canonical(payload)
	v, _ := contract.DecodeJSON(b)
	f := q.fact(a, "queue.reorder", []string{a.last}, []any{map[string]any{"entity": "account", "id": q.account}}, v.(map[string]any))
	q.pushOK(t, a, f, "applied")
	q.expectOrder(t, "A,B")
}

// A commit that changes no queue input keeps the replay's cache valid for the
// head it leaves, so non-queue commits between queue commands cost no full
// replay.
func TestNonQueueCommitsKeepTheReplayWarm(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	q.replayCheck = false // the cross-check replays from the start itself
	a := q.device("A")
	q.seed(t, a, "A", "B")
	full0, _ := replayCounts()
	for i := 0; i < 3; i++ {
		q.clock += minute
		q.pushOK(t, a, q.lfactRef(a, "favorite.set", q.refs["A"], map[string]any{"on": i%2 == 0}), "applied")
		q.clock += minute
		q.pushOK(t, a, q.lfactRef(a, "favorite.set", q.refs["B"], map[string]any{"on": i%2 == 0}), "applied")
		q.clock += minute
		k, id := keyBetween(t, "", "")
		q.pushOK(t, a, q.qfact(a, "queue.move", map[string]any{"episode": q.refs["C"], "key": k, "position_id": id, "place": atEnd()}, []string{"C"}), "applied")
	}
	if full, _ := replayCounts(); full != full0 {
		t.Fatalf("%d full replays for non-queue commits between queue commands, want 0", full-full0)
	}
}

// A recovered move whose receipt refused it as
// generation_closed only ghosts, even while its generation is live in the
// order recovery has so far. [A, B] is backed up; an offline phone moves B
// to the start; another phone unsubscribes, so the move is refused; after a
// restore, recovery joins only the offline phone's fact with its signed
// receipt, before the unsubscribe: the queue stays [A, B], also through a
// rebuild.
func TestRecoveredClosedMoveOnlyGhosts(t *testing.T) {
	for _, run := range []bool{false, true} {
		t.Run(map[bool]string{false: "move", true: "run move"}[run], func(t *testing.T) {
			q := newQueueEnv(t, "A", "B")
			a, off := q.device("A"), q.device("Off")
			q.seed(t, a, "A", "B")
			q.sees(off, a.last)
			backup := q.backup(t)
			at := q.headSeq(t)
			q.clock += minute
			var move map[string]any
			if run {
				move = q.qfact(off, "queue.reorder", map[string]any{"run": []any{q.refs["B"]}, "place": atStart()}, []string{"B"})
			} else {
				move = q.move(t, off, "B", atStart())
			}
			q.clock += minute
			q.pushOK(t, a, q.fact(a, "podcast.unsubscribe", []string{a.last}, podTarget(q.p, q.g), map[string]any{}), "applied")
			if r := q.push(off, move); outcome(r[0]) != "not_applied/generation_closed" {
				t.Fatalf("the move: %s", outcome(r[0]))
			}
			lost := own(q.logCommits(t, at), off)
			q.restore(t, backup)
			off2 := q.successor(t, off)
			r, _ := q.startRecovery(t, off2, nil)
			if st := statuses(r.upload(t, lost)); st["joined"] != 1 || len(st) != 1 {
				t.Fatalf("the upload: %v", st)
			}
			q.expectOrder(t, "A,B")
			checkRebuild(t, q.svc, q.account)
			q.expectOrder(t, "A,B")
		})
	}
}

// Commit callbacks run after the store's transaction ends, so a slower one must
// not replace a newer head's cache; a later head or another log (a restored
// server) replaces it.
func TestInstallCacheNeverGoesBack(t *testing.T) {
	var memo sync.Map
	at := func(log string, epoch, seq int64) *replayCache {
		return &replayCache{head: storage.Head{LogID: log, Epoch: epoch, Seq: seq}}
	}
	kept := func() storage.Head { v, _ := memo.Load(memoKey{"a"}); return v.(*replayCache).head }
	installCache(&memo, "a", at("L", 1, 4))
	installCache(&memo, "a", at("L", 1, 3))
	if h := kept(); h.Seq != 4 {
		t.Fatalf("an older head replaced the cache: seq %d", h.Seq)
	}
	installCache(&memo, "a", at("L", 1, 5))
	if h := kept(); h.Seq != 5 {
		t.Fatalf("a later head was not kept: seq %d", h.Seq)
	}
	installCache(&memo, "a", at("L", 2, 1))
	if h := kept(); h.Epoch != 2 || h.Seq != 1 {
		t.Fatalf("a later epoch was not kept: %d/%d", h.Epoch, h.Seq)
	}
	installCache(&memo, "a", at("L", 1, 9))
	if h := kept(); h.Epoch != 2 {
		t.Fatalf("an earlier epoch replaced the cache: %d/%d", h.Epoch, h.Seq)
	}
	installCache(&memo, "a", at("M", 1, 1))
	if h := kept(); h.LogID != "M" {
		t.Fatalf("another log's cache was not kept: %s", h.LogID)
	}
}
