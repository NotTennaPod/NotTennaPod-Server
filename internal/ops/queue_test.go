package ops

// Queue acceptance tests (PROTOCOL §7.6): membership by the episode fold, the
// order by the replay, keys by the publication.

import (
	"crypto/rand"
	"encoding/json"
	mrand "math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// queueEnv is a subscribed podcast with titled episodes.
type queueEnv struct {
	*listenEnv
	refs map[string]map[string]any // by title
	seen map[*device][]string      // facts a device saw before it went offline (dependencies of its next facts)
}

// sees makes a device's later facts depend on facts it saw (another
// device's), as a phone that pulled them before going offline.
func (q *queueEnv) sees(d *device, facts ...string) {
	if q.seen == nil {
		q.seen = map[*device][]string{}
	}
	q.seen[d] = append(q.seen[d], facts...)
}

func newQueueEnv(t *testing.T, titles ...string) *queueEnv {
	l := newListenEnv(t)
	l.replayCheck = true
	q := &queueEnv{listenEnv: l, refs: map[string]map[string]any{}}
	for _, title := range titles {
		ref := l.episodeRef(l.p, l.g, "guid-"+title)
		ref["occurrence"].(map[string]any)["title"] = title
		q.refs[title] = ref
	}
	return q
}

// keyBetween allocates a key for a fresh position ID.
func keyBetween(t *testing.T, low, high string) (string, string) {
	var u [16]byte
	rand.Read(u[:])
	var lo, hi *contract.QueueKey
	if low != "" {
		k, _ := contract.ParseQueueKey(low)
		lo = &k
	}
	if high != "" {
		k, _ := contract.ParseQueueKey(high)
		hi = &k
	}
	k, err := contract.QueueKeyBetween(lo, hi, u)
	if err != nil {
		t.Fatal(err)
	}
	return k.String(), formatUUID(u)
}

func (q *queueEnv) placement(title, key, id string) map[string]any {
	return map[string]any{"episode": q.refs[title], "key": key, "position_id": id}
}

// Places a device resolved (PROTOCOL §7.6).
func atEnd() map[string]any   { return map[string]any{"kind": "end"} }
func atStart() map[string]any { return map[string]any{"kind": "start"} }
func (q *queueEnv) after(title string) map[string]any {
	return map[string]any{"kind": "after", "after": q.refs[title]}
}

// qfact builds a queue fact naming episodes (targets sorted by the server's rule).
func (q *queueEnv) qfact(d *device, typ string, payload map[string]any, titles []string, deps ...string) map[string]any {
	if d.last != "" {
		deps = append(deps, d.last)
	}
	deps = append(deps, q.seen[d]...)
	var targets []map[string]any
	for _, title := range titles {
		r := q.refs[title]
		targets = append(targets, map[string]any{"entity": "episode", "id": r["episode_id"], "generation": r["generation_id"]})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i]["id"].(string) < targets[j]["id"].(string) })
	ts := []any{}
	for _, x := range targets {
		ts = append(ts, x)
	}
	b, _ := contract.Canonical(payload)
	v, _ := contract.DecodeJSON(b)
	slicesSortCompact(&deps)
	return q.fact(d, typ, deps, ts, v.(map[string]any))
}

func slicesSortCompact(s *[]string) {
	sort.Strings(*s)
	out := (*s)[:0]
	for i, x := range *s {
		if i == 0 || x != (*s)[i-1] {
			out = append(out, x)
		}
	}
	*s = out
}

func (q *queueEnv) policy(d *device, payload map[string]any) map[string]any {
	b, _ := contract.Canonical(payload)
	v, _ := contract.DecodeJSON(b)
	deps := append([]string{}, q.seen[d]...)
	if d.last != "" {
		deps = append(deps, d.last)
	}
	slicesSortCompact(&deps)
	return q.fact(d, "account.set_queue_policy", deps, []any{map[string]any{"entity": "account", "id": q.account}}, v.(map[string]any))
}

func titleSorting(keep bool) map[string]any {
	return map[string]any{"sorting": map[string]any{"keep_sorted": keep, "sort_order": "episode_title_a_z"}}
}

// add builds a queue.add of titles at a place, with keys the device
// predicts after low (its last key; "" for none).
func (q *queueEnv) add(t *testing.T, d *device, at map[string]any, low string, titles ...string) map[string]any {
	var members []any
	last := low
	for _, title := range titles {
		k, id := keyBetween(t, last, "")
		last = k
		members = append(members, q.placement(title, k, id))
	}
	return q.qfact(d, "queue.add", map[string]any{"members": members, "place": at}, titles)
}

// move builds a queue.move of one title to a place.
func (q *queueEnv) move(t *testing.T, d *device, title string, at map[string]any) map[string]any {
	k, id := keyBetween(t, "", "")
	p := q.placement(title, k, id)
	p["place"] = at
	return q.qfact(d, "queue.move", p, []string{title})
}

func (q *queueEnv) remove(d *device, titles ...string) map[string]any {
	refs := []any{}
	for _, title := range titles {
		refs = append(refs, q.refs[title])
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].(map[string]any)["episode_id"].(string) < refs[j].(map[string]any)["episode_id"].(string)
	})
	return q.qfact(d, "queue.remove", map[string]any{"episodes": refs, "reason": "user"}, titles)
}

// sortCmd builds the sort dialog's command: observed titles, and for random
// the device's resulting order.
func (q *queueEnv) sortCmd(d *device, order string, observed []string, perm []string) map[string]any {
	obs := []any{}
	for _, title := range observed {
		obs = append(obs, q.refs[title])
	}
	s := map[string]any{"sort_order": order}
	if perm != nil {
		p := []any{}
		for _, title := range perm {
			p = append(p, q.refs[title])
		}
		s["permutation"] = p
	}
	// The sort dialog's command targets the account; its
	// observed episodes are context.
	b, _ := contract.Canonical(map[string]any{"sort": s, "observed": obs})
	v, _ := contract.DecodeJSON(b)
	deps := append([]string{}, q.seen[d]...)
	if d.last != "" {
		deps = append(deps, d.last)
	}
	slicesSortCompact(&deps)
	return q.fact(d, "queue.reorder", deps, []any{map[string]any{"entity": "account", "id": q.account}}, v.(map[string]any))
}

// order returns the queue's titles in key order.
func (q *queueEnv) order(t *testing.T) string {
	t.Helper()
	recs := q.records()
	type entry struct {
		title string
		key   contract.QueueKey
	}
	var es []entry
	for title, ref := range q.refs {
		ep := ref["episode_id"].(string)
		m, ok := recs["episode/"+ep+"/queue_membership"]
		if !ok || !strings.Contains(m, `"in_queue":true`) {
			// An out-member may keep its scope's last published key: the
			// queue_position record is the published key of the scope the
			// membership record shows (docs/CONTRACT.md, "Publication rule").
			continue
		}
		pos, has := recs["episode/"+ep+"/queue_position"]
		if !has {
			t.Fatalf("%s is in the queue without a published key", title)
		}
		v, _ := contract.DecodeJSON([]byte(pos))
		k, err := contract.ParseQueueKey(v.(map[string]any)["key"].(string))
		if err != nil {
			t.Fatalf("%s: %v (record %v)", title, err, v)
		}
		es = append(es, entry{title, k})
	}
	sort.Slice(es, func(i, j int) bool { return contract.CompareQueueKeys(es[i].key, es[j].key) < 0 })
	var out []string
	for _, e := range es {
		out = append(out, e.title)
	}
	return strings.Join(out, ",")
}

func (q *queueEnv) expectOrder(t *testing.T, want string) {
	t.Helper()
	if got := q.order(t); got != want {
		t.Fatalf("queue %s, want %s", got, want)
	}
}

// seed adds titles at the back, in order, from device d (keep-sorted off).
func (q *queueEnv) seed(t *testing.T, d *device, titles ...string) {
	q.pushOK(t, d, q.add(t, d, atEnd(), "", titles...), "applied")
}

// layouts counts the account's queue.layout decisions.
func (q *queueEnv) layouts(t *testing.T) int {
	t.Helper()
	n := 0
	if err := q.svc.Store.Read(t.Context(), func(r *storage.ReadTx) error {
		ds, err := r.Account(q.account).DecisionsOfType("queue.layout")
		n = len(ds)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// pushInOrder pushes each (device, fact) pair in turn, all applied.
func (q *queueEnv) pushInOrder(t *testing.T, pairs ...any) {
	t.Helper()
	for i := 0; i < len(pairs); i += 2 {
		d, f := pairs[i].(*device), pairs[i+1].(map[string]any)
		if r := q.push(d, f); outcome(r[0]) != "applied" {
			t.Fatalf("%s: %s", f["type"], outcome(r[0]))
		}
	}
}

// PROTOCOL §7.6 examples through the server, in both reconnection orders: what one
// device would have shown.
func TestQueueReplayExamples(t *testing.T) {
	type step struct {
		dev  string
		fact func(q *queueEnv, d *device) map[string]any
	}
	cases := []struct {
		name   string
		titles []string
		setup  func(q *queueEnv, d *device) // online, before the outage
		phones [2][]step                    // each phone's offline facts, in time order (phone 0 first)
		want   string
	}{
		{"back adds", []string{"Z", "X", "Y"}, func(q *queueEnv, d *device) { q.seed(t, d, "Z") },
			[2][]step{{{"P", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "X") }}},
				{{"C", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "Y") }}}}, "Z,X,Y"},
		{"front adds", []string{"Z", "X", "Y"}, func(q *queueEnv, d *device) { q.seed(t, d, "Z") },
			[2][]step{{{"P", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atStart(), "", "X") }}},
				{{"C", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atStart(), "", "Y") }}}}, "Y,X,Z"},
		{"two phones, three back adds", []string{"Z", "X", "Y", "W"}, func(q *queueEnv, d *device) { q.seed(t, d, "Z") },
			[2][]step{{{"P", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "X") }}},
				{{"C", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "Y") }},
					{"C", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "W") }}}}, "Z,X,Y,W"},
		{"keep-sorted turned on, then an add elsewhere", []string{"Z", "A", "B"}, func(q *queueEnv, d *device) { q.seed(t, d, "Z", "A") },
			[2][]step{{{"P", func(q *queueEnv, d *device) map[string]any { return q.policy(d, titleSorting(true)) }}},
				{{"C", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "B") }}}}, "A,B,Z"},
		{"two drags to just after P", []string{"P", "Q", "E", "F"}, func(q *queueEnv, d *device) { q.seed(t, d, "P", "Q", "E", "F") },
			[2][]step{{{"P", func(q *queueEnv, d *device) map[string]any { return q.move(t, d, "E", q.after("P")) }}},
				{{"C", func(q *queueEnv, d *device) map[string]any { return q.move(t, d, "F", q.after("P")) }}}}, "P,F,E,Q"},
		{"an add of an episode already queued", []string{"A", "X", "Y"}, func(q *queueEnv, d *device) { q.seed(t, d, "A") },
			[2][]step{{{"P", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "X") }}},
				{{"C", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "Y") }},
					{"C", func(q *queueEnv, d *device) map[string]any { return q.add(t, d, atEnd(), "", "X") }}}}, "A,X,Y"},
	}
	for _, c := range cases {
		for _, firstPhone := range []int{0, 1} {
			t.Run(c.name+map[int]string{0: ", P reconnects first", 1: ", C reconnects first"}[firstPhone], func(t *testing.T) {
				q := newQueueEnv(t, c.titles...)
				a := q.device("A")
				c.setup(q, a)
				devs := map[string]*device{"P": q.device("P"), "C": q.device("C")}
				var facts [2][]map[string]any
				// Phone 0's facts are made first in time (Monday), phone 1's
				// later (Tuesday); within a phone, in order.
				for p := 0; p < 2; p++ {
					for _, s := range c.phones[p] {
						q.clock += minute
						d := devs[s.dev]
						q.sees(d, a.last) // it saw the queue before the outage
						facts[p] = append(facts[p], s.fact(q, d))
					}
				}
				q.clock += 60 * minute
				for _, p := range []int{firstPhone, 1 - firstPhone} {
					for i, f := range facts[p] {
						// An add whose membership a later duplicate add rewrites is
						// superseded by the membership rule, though it is the one
						// that places the episode: outcomes follow arrival.
						if r := q.push(devs[c.phones[p][i].dev], f); outcome(r[0]) != "applied" && outcome(r[0]) != "superseded" {
							t.Fatalf("%s: %s", f["type"], outcome(r[0]))
						}
					}
				}
				q.expectOrder(t, c.want)
			})
		}
	}
}

// Scenario 8: a removal and a concurrent offline move leave the episode out.
func TestQueueRemoveBeatsOfflineMove(t *testing.T) {
	q := newQueueEnv(t, "A", "B")
	a, b := q.device("A"), q.device("B")
	q.seed(t, a, "A", "B")
	q.sees(b, a.last)
	q.clock += minute
	move := q.move(t, b, "A", q.after("B"))
	q.pushOK(t, a, q.remove(a, "A"), "applied")
	q.pushOK(t, b, move, "applied")
	q.expectOrder(t, "B")
}

// Scenario 9: a clear removes only what it observed; a concurrent add stays.
func TestQueueClearKeepsConcurrentAdd(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	a, b := q.device("A"), q.device("B")
	q.seed(t, a, "A", "B")
	q.sees(b, a.last)
	q.clock += minute
	add := q.add(t, b, atEnd(), "", "C")
	observed := []any{q.refs["A"], q.refs["B"]}
	sort.Slice(observed, func(i, j int) bool {
		return observed[i].(map[string]any)["episode_id"].(string) < observed[j].(map[string]any)["episode_id"].(string)
	})
	q.pushOK(t, a, q.qfact(a, "queue.clear", map[string]any{"observed": observed}, []string{"A", "B"}), "applied")
	q.pushOK(t, b, add, "applied")
	q.expectOrder(t, "C")
}

// Scenario 35: concurrent sorted adds of B and C between A and Z converge to
// A,B,C,Z: each add sorts at its place with the values the server recorded.
// The devices' keys are chosen in the wrong order: only the publication's
// layout gives the order.
func TestConcurrentSortedAdds(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C", "Z")
	a, x, y := q.device("A"), q.device("X"), q.device("Y")
	q.seed(t, a, "A", "Z")
	q.pushOK(t, a, q.policy(a, titleSorting(true)), "applied")
	q.sees(x, a.last)
	q.sees(y, a.last)
	q.clock += minute
	recs := q.records()
	kA := mustField(t, recs["episode/"+q.refs["A"]["episode_id"].(string)+"/queue_position"], "key")
	kZ := mustField(t, recs["episode/"+q.refs["Z"]["episode_id"].(string)+"/queue_position"], "key")
	var kb, ib, kc, ic string
	for {
		kb, ib = keyBetween(t, kA, kZ)
		kc, ic = keyBetween(t, kA, kZ)
		b, _ := contract.ParseQueueKey(kb)
		c, _ := contract.ParseQueueKey(kc)
		if contract.CompareQueueKeys(c, b) < 0 {
			break
		}
	}
	addB := q.qfact(x, "queue.add", map[string]any{"members": []any{q.placement("B", kb, ib)}, "place": q.after("A")}, []string{"B"})
	q.clock += minute
	addC := q.qfact(y, "queue.add", map[string]any{"members": []any{q.placement("C", kc, ic)}, "place": q.after("A")}, []string{"C"})
	before := q.layouts(t)
	q.pushOK(t, y, addC, "applied")
	q.pushOK(t, x, addB, "applied")
	q.expectOrder(t, "A,B,C,Z")
	if q.layouts(t) == before {
		t.Fatal("keys in the wrong order were published without a layout")
	}
}

func mustField(t *testing.T, record, field string) string {
	t.Helper()
	v, err := contract.DecodeJSON([]byte(record))
	if err != nil {
		t.Fatal(err)
	}
	return v.(map[string]any)[field].(string)
}

// Undo of a removal restores at the place resolved at Undo time, without
// sorting, even under keep-sorted.
func TestRestoreDoesNotSort(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	a := q.device("A")
	q.seed(t, a, "C", "A", "B") // unsorted before keep-sorted is turned on
	q.pushOK(t, a, q.policy(a, titleSorting(true)), "applied")
	q.pushOK(t, a, q.remove(a, "C"), "applied")
	k, id := keyBetween(t, "", "")
	q.pushOK(t, a, q.qfact(a, "queue.restore", map[string]any{"members": []any{q.placement("C", k, id)}, "place": atStart()}, []string{"C"}), "applied")
	q.expectOrder(t, "C,A,B")
}

// Under the replay (PROTOCOL §7.6): [Z,A]; offline, add B
// under keep-sorted (it sorts at its place: A,B,Z), disable keep-sorted, move
// Z to the front: Z,A,B, however late the server sees them.
func TestLaterOfflineMoveFollowsTheSort(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "Z")
	a, x := q.device("A"), q.device("X")
	q.seed(t, a, "Z", "A")
	q.pushOK(t, a, q.policy(a, titleSorting(true)), "applied")
	q.sees(x, a.last)
	q.clock += minute
	add := q.add(t, x, q.after("A"), "", "B")
	q.clock += minute
	disable := q.policy(x, titleSorting(false))
	q.clock += minute
	move := q.move(t, x, "Z", atStart())
	q.clock += 10 * minute
	for _, res := range q.push(x, add, disable, move) {
		if outcome(res) != "applied" {
			t.Fatalf("%+v", res)
		}
	}
	q.expectOrder(t, "Z,A,B")
}

// The policy in force at an add's place decides whether it sorts (PROTOCOL §7.6): Y
// disables keep-sorted and moves A behind Z; X, unaware and later, adds B after A under
// its own keep-sorted. The replay does not sort: Z,A,B (X's sorted result A,B,Z does not
// survive).
func TestConcurrentDisableDecidesAtThePlace(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "Z")
	a, x, y := q.device("A"), q.device("X"), q.device("Y")
	q.seed(t, a, "A", "Z")
	q.pushOK(t, a, q.policy(a, titleSorting(true)), "applied")
	q.sees(x, a.last)
	q.sees(y, a.last)
	q.clock += minute
	disable := q.policy(y, titleSorting(false))
	moveA := q.move(t, y, "A", q.after("Z"))
	q.clock += minute
	add := q.add(t, x, q.after("A"), "", "B")
	q.pushInOrder(t, y, disable, y, moveA)
	q.pushOK(t, x, add, "applied")
	q.expectOrder(t, "Z,A,B")
}

// The sorting register: keep_sorted with random is invalid; concurrent writes
// resolve as one register.
func TestSortingRegister(t *testing.T) {
	q := newQueueEnv(t)
	a, b := q.device("A"), q.device("B")
	q.pushOK(t, a, q.policy(a, map[string]any{"sorting": map[string]any{"keep_sorted": true, "sort_order": "random"}}), "rejected/invalid_payload")
	on := q.policy(a, titleSorting(true))
	q.clock += minute
	random := q.policy(b, map[string]any{"sorting": map[string]any{"keep_sorted": false, "sort_order": "random"}, "enqueue_location": "front"})
	q.pushOK(t, b, random, "applied")
	q.pushOK(t, a, on, "superseded")
	if got := q.records()["account/"+q.account+"/queue_policy"]; got != `{"enqueue_location":"front","keep_sorted":false,"sort_order":"random"}` {
		t.Fatalf("policy %s", got)
	}
}

// Scenario 25: a client without the queue capability plays an
// episode: the server adds it at the account's location (sorted under
// keep-sorted), and its completion while offline takes it out again.
func TestQueueLessPlay(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "Z")
	a := q.device("A")
	web := q.device("web")
	q.setCapabilities(t, web, `["favorites","history","inbox","statistics"]`)
	q.seed(t, a, "B", "Z")
	q.pushOK(t, a, q.policy(a, titleSorting(true)), "applied")
	q.clock += minute
	s := newID()
	q.pushOK(t, web, q.lfactRef(web, "session.start", q.refs["A"], start(s, 0, true)), "applied")
	q.expectOrder(t, "A,B,Z")
	// Offline: play B and complete it before the server sees either.
	s2 := newID()
	st := q.lfactRef(web, "session.start", q.refs["B"], start(s2, 0, true))
	q.clock += minute
	done := q.lfactRef(web, "session.end", q.refs["B"], end(s2, 1, minute, minute, "ended", true, true, true))
	q.clock += 10 * minute
	q.push(web, st, done)
	q.expectOrder(t, "A,Z")
}

// A queue-less start ranked before a completion it supersedes leaves the
// episode where it was, whichever arrives first: the server resolves no add
// once the start's own effects count (the completion once moved E to the back).
func TestQueueLessStartBeforeSupersededCompletion(t *testing.T) {
	for _, copyFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion first", true: "start first"}[copyFirst], func(t *testing.T) {
			q := newQueueEnv(t, "E", "X")
			a := q.device("A")
			q.seed(t, a, "E", "X")
			q.setCapabilities(t, a, `["favorites","history","statistics"]`)
			s := newID()
			q.clock += second
			st := q.lfact(a, "session.start", q.refs["E"], start(s, 0, true))
			q.pushOK(t, a, st, "applied")
			q.clock += second
			q.pushOK(t, a, q.lfact(a, "session.checkpoint", q.refs["E"], checkpoint(s, 1, 300*second, 300*second)), "applied")
			b := q.copyDevice(a, "B")
			q.setCapabilities(t, b, `["favorites","history","statistics"]`)
			q.clock += second
			bst := q.lfact(b, "session.start", q.refs["E"], start(newID(), 300*second, true))
			q.clock += second
			fin := q.lfact(a, "session.finalize", q.refs["E"], finalize(s, 1, st["op_id"].(string), true))
			if copyFirst {
				q.pushOK(t, b, bst, "applied")
				q.pushOK(t, a, fin, "superseded")
			} else {
				q.pushOK(t, a, fin, "applied")
				q.pushOK(t, b, bst, "applied")
			}
			q.expectOrder(t, "E,X")
		})
	}
}

func (q *queueEnv) lfactRef(d *device, typ string, ref map[string]any, payload map[string]any) map[string]any {
	return q.lfact(d, typ, ref, payload)
}

func (q *queueEnv) setCapabilities(t *testing.T, d *device, caps string) {
	t.Helper()
	d.inst.Capabilities = caps
	if err := q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
		return tx.Account(q.account).UpdateInstallationDevice(d.inst.InstallID, d.inst.Name, d.inst.AppVersion, caps)
	}); err != nil {
		t.Fatal(err)
	}
}

// Completion and skip removals: completion takes the episode out; a skip with
// remove_from_queue too; a skip without it leaves it queued.
func TestListeningQueueRemovals(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	a := q.device("A")
	q.seed(t, a, "A", "B", "C")
	for _, c := range []struct {
		title     string
		completed bool
		remove    bool
	}{{"A", true, false}, {"B", false, true}, {"C", false, false}} {
		s := newID()
		q.clock += minute
		q.pushOK(t, a, q.lfact(a, "session.start", q.refs[c.title], start(s, 0, true)), "applied")
		payload := end(s, 1, minute, minute, "skipped", c.completed, true, true)
		payload["remove_from_queue"] = c.remove
		q.pushOK(t, a, q.lfact(a, "session.end", q.refs[c.title], payload), "applied")
	}
	q.expectOrder(t, "C")
	ep := q.refs["A"]["episode_id"].(string)
	if got := q.records()["episode/"+ep+"/queue_membership"]; !strings.Contains(got, `"reason":"completed"`) {
		t.Fatalf("A membership %s", got)
	}
}

// Wire validation (PROTOCOL §7.6): a key not allocated for its position
// ID, a placement without place, a retired add moves list, a malformed place,
// a reorder that is neither a run nor a sort, and a random sort whose
// permutation is not of what it observed are invalid; an anchor is context:
// an anchor on a closed generation never makes a command fail.
func TestQueueWireValidation(t *testing.T) {
	q := newQueueEnv(t, "A", "B")
	a := q.device("A")
	k, _ := keyBetween(t, "", "")
	_, other := keyBetween(t, "", "")
	k2, id2 := keyBetween(t, "", "")
	bad := []map[string]any{
		q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement("A", k, other)}, "place": atEnd()}, []string{"A"}),
		q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement("A", k2, id2)}}, []string{"A"}),
		q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement("A", k2, id2)}, "moves": []any{}, "place": atEnd()}, []string{"A"}),
		q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement("A", k2, id2)}, "place": map[string]any{"kind": "before", "before": q.refs["B"]}}, []string{"A"}),
		q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement("A", k2, id2)}, "place": map[string]any{"kind": "end", "after": q.refs["B"]}}, []string{"A"}),
		q.qfact(a, "queue.add", map[string]any{"members": []any{q.placement("A", k2, id2)}, "place": q.after("B")}, []string{"A", "B"}),
		q.qfact(a, "queue.reorder", map[string]any{"members": []any{q.placement("A", k2, id2)}}, []string{"A"}),
		q.sortCmd(a, "random", []string{"A", "B"}, []string{"A"}),
		q.sortCmd(a, "episode_title_a_z", []string{"A"}, []string{"A"}),
		q.sortCmd(a, "random", []string{"A"}, nil),
	}
	for i, f := range bad {
		if r := q.push(a, f); outcome(r[0]) != "rejected/invalid_payload" {
			t.Fatalf("case %d: %s, want rejected/invalid_payload", i, outcome(r[0]))
		}
	}
	// An empty sort is valid (another phone's unseen add can make it matter).
	q.pushOK(t, a, q.sortCmd(a, "episode_title_a_z", nil, nil), "applied")
}

// Sorts through the server: a canonical sort orders the queue (members
// another phone added unseen included), a random sort replays its recorded
// permutation and leaves an unobserved member's slot alone.
func TestQueueSortCommands(t *testing.T) {
	q := newQueueEnv(t, "D", "B", "C", "A")
	a, b := q.device("A"), q.device("B")
	q.seed(t, a, "D", "B", "C")
	q.sees(b, a.last)
	q.clock += minute
	unseen := q.add(t, b, atEnd(), "", "A")
	q.clock += minute
	q.pushOK(t, a, q.sortCmd(a, "episode_title_a_z", []string{"B", "C", "D"}, nil), "applied")
	q.pushOK(t, b, unseen, "applied")
	q.expectOrder(t, "A,B,C,D")
	q.clock += minute
	q.pushOK(t, b, q.sortCmd(b, "random", []string{"B", "C", "D"}, []string{"D", "B", "C"}), "applied")
	q.expectOrder(t, "A,D,B,C")
}

// Publication (PROTOCOL §7.6): an in-member's published key is
// its latest key write, so a placement whose device key fits needs no layout;
// a conflict records a layout listing only the members whose key changes
// (PROTOCOL §7.6 over the published keys); an add of a member already
// in the queue writes no key.
func TestLayoutsOnlyOnConflictAndSparse(t *testing.T) {
	q := newQueueEnv(t, "A", "B", "C")
	a := q.device("A")
	seed := q.add(t, a, atEnd(), "", "A", "B")
	q.pushOK(t, a, seed, "applied")
	members := seed["payload"].(map[string]any)["members"].([]any)
	keyOf := func(i int) [2]string {
		m := members[i].(map[string]any)
		return [2]string{m["key"].(string), m["position_id"].(string)}
	}
	if n := q.layouts(t); n != 0 {
		t.Fatalf("%d layouts for device keys that fit", n)
	}
	if got := q.positions(t); got["A"] != keyOf(0) || got["B"] != keyOf(1) {
		t.Fatalf("published keys %v, want the device's", got)
	}
	// C at the start with a key after B: only C gets a fresh key.
	q.clock += minute
	add := q.add(t, a, atStart(), keyOf(1)[0], "C")
	q.pushOK(t, a, add, "applied")
	q.expectOrder(t, "C,A,B")
	if n := q.layouts(t); n != 1 {
		t.Fatalf("%d layouts after a conflict, want 1", n)
	}
	c := add["payload"].(map[string]any)["members"].([]any)[0].(map[string]any)
	if last := q.lastLayout(t); len(last) != 1 || last["C"] == [2]string{c["key"].(string), c["position_id"].(string)} {
		t.Fatalf("the layout lists %v, want C alone with a fresh key", last)
	}
	if got := q.positions(t); got["A"] != keyOf(0) || got["B"] != keyOf(1) {
		t.Fatalf("A and B's keys changed: %v", got)
	}
	// A move to the end whose device key fits: published as is, no layout.
	q.clock += minute
	k, id := keyBetween(t, keyOf(1)[0], "")
	mv := q.placement("C", k, id)
	mv["place"] = atEnd()
	q.pushOK(t, a, q.qfact(a, "queue.move", mv, []string{"C"}), "applied")
	q.expectOrder(t, "A,B,C")
	if n := q.layouts(t); n != 1 {
		t.Fatalf("%d layouts after a fitting move, want 1", n)
	}
	if got := q.positions(t); got["C"] != [2]string{k, id} {
		t.Fatalf("C's published key %v, want the move's", got["C"])
	}
	// An add of A, already queued, writes no key: nothing changes.
	q.clock += minute
	k2, _ := keyBetween(t, k, "")
	before := q.records()
	q.pushOK(t, a, q.add(t, a, atEnd(), k2, "A"), "applied")
	if got := q.positions(t); got["A"] != keyOf(0) {
		t.Fatalf("an add of a queued member wrote its key: %v", got["A"])
	}
	if n := q.layouts(t); n != 1 {
		t.Fatalf("%d layouts after a duplicate add, want 1", n)
	}
	after := q.records()
	for _, title := range []string{"A", "B", "C"} {
		r := "episode/" + q.refs[title]["episode_id"].(string) + "/queue_position"
		if before[r] != after[r] {
			t.Fatalf("%s's position record changed", title)
		}
	}
}

// positions are the queue_position records' keys and position IDs by title.
func (q *queueEnv) positions(t *testing.T) map[string][2]string {
	t.Helper()
	recs := q.records()
	out := map[string][2]string{}
	for title, ref := range q.refs {
		v, ok := recs["episode/"+ref["episode_id"].(string)+"/queue_position"]
		if !ok {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			t.Fatal(err)
		}
		out[title] = [2]string{m["key"].(string), m["position_id"].(string)}
	}
	return out
}

// The key-write selector (PROTOCOL §7.6): a placement writes only for
// the members it actually places, as the replay decides at its place. Two
// phones add X; B's add, made earlier offline, places it and A's finds it
// queued, in both arrival orders: X's published key is B's.
func TestKeyWriteOnlyForWhatAStepPlaces(t *testing.T) {
	for _, bFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "A arrives first", true: "B arrives first"}[bFirst], func(t *testing.T) {
			q := newQueueEnv(t, "W", "X")
			a, b := q.device("A"), q.device("B")
			q.seed(t, a, "W")
			q.sees(b, a.last)
			w := q.positions(t)["W"][0]
			q.clock += minute
			addB := q.add(t, b, atEnd(), w, "X") // B, offline: earlier
			q.clock += minute
			addA := q.add(t, a, atEnd(), w, "X")
			if bFirst {
				q.pushOK(t, b, addB, "applied")
				q.pushOK(t, a, addA, "applied")
			} else {
				q.pushOK(t, a, addA, "applied")
				q.pushOK(t, b, addB, "applied")
			}
			m := addB["payload"].(map[string]any)["members"].([]any)[0].(map[string]any)
			if got := q.positions(t)["X"]; got != [2]string{m["key"].(string), m["position_id"].(string)} {
				t.Fatalf("X's published key %v, want B's", got)
			}
			q.expectOrder(t, "W,X")
		})
	}
}

// A restore reasserting X's key and position ID (PROTOCOL §7.6) leaves
// its position record's value, so the record keeps its provenance; a rebuild
// keeps it too.
func TestUnchangedPositionRecordKeepsProvenance(t *testing.T) {
	q := newQueueEnv(t, "W", "X")
	a := q.device("A")
	add := q.add(t, a, atEnd(), "", "W", "X")
	q.pushOK(t, a, add, "applied")
	m := add["payload"].(map[string]any)["members"].([]any)[1].(map[string]any)
	prov := func() string {
		var p string
		q.svc.Store.Read(t.Context(), func(r *storage.ReadTx) error {
			row, err := r.Account(q.account).Record("episode", q.refs["X"]["episode_id"].(string), "queue_position")
			p = row.Provenance
			return err
		})
		return p
	}
	first := prov()
	q.clock += minute
	q.pushOK(t, a, q.remove(a, "X"), "applied")
	q.clock += minute
	q.pushOK(t, a, q.qfact(a, "queue.restore", map[string]any{"members": []any{m}, "place": atEnd()}, []string{"X"}), "applied")
	q.expectOrder(t, "W,X")
	if p := prov(); p != first {
		t.Fatalf("the restore's unchanged record changed provenance from %s to %s", first, p)
	}
}

// lastLayout is the highest-ranked layout's keys and position IDs by title.
func (q *queueEnv) lastLayout(t *testing.T) map[string][2]string {
	t.Helper()
	var raw [][]byte
	if err := q.svc.Store.Read(t.Context(), func(r *storage.ReadTx) error {
		var err error
		raw, err = r.Account(q.account).DecisionsOfType("queue.layout")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("no layout")
	}
	d, err := decodeObject(raw[len(raw)-1])
	if err != nil {
		t.Fatal(err)
	}
	titles := map[string]string{}
	for title, ref := range q.refs {
		titles[ref["episode_id"].(string)] = title
	}
	out := map[string][2]string{}
	result, _ := d["result"].(map[string]any)
	for _, m := range result["members"].([]any) {
		m := m.(map[string]any)
		out[titles[m["episode_id"].(string)]] = [2]string{m["key"].(string), m["position_id"].(string)}
	}
	return out
}

// Arrival order never changes the queue: several offline devices' adds,
// moves, removals, restores and clears arrive in different interleavings
// (each device's facts in order). Every record but the published keys is the
// same, and the keys give the same order.
func TestQueueArrivalOrderDoesNotChangeProjections(t *testing.T) {
	r := mrand.New(mrand.NewPCG(31, 32))
	titles := []string{"A", "B", "C", "D", "E"}
	for trial := 0; trial < 6; trial++ {
		base := newEnv(t)
		a := base.device("A")
		sub := base.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
		p := contract.PodcastID(base.namespace, sub["op_id"].(string))
		g := contract.GenerationID(base.namespace, p, sub["op_id"].(string))
		q := &queueEnv{listenEnv: &listenEnv{env: base, p: p, g: g}, refs: map[string]map[string]any{}}
		for _, title := range titles {
			ref := base.episodeRef(p, g, "guid-"+title)
			ref["occurrence"].(map[string]any)["title"] = title
			q.refs[title] = ref
		}
		var members []any
		last := ""
		for _, title := range titles {
			k, id := keyBetween(t, last, "")
			last = k
			members = append(members, q.placement(title, k, id))
		}
		seed := q.qfact(a, "queue.add", map[string]any{"members": members, "place": atEnd()}, titles, sub["op_id"].(string))
		type plan struct {
			device string
			facts  []map[string]any
		}
		var plans []plan
		for _, name := range []string{"X", "Y", "Z"} {
			d := base.device(name)
			var facts []map[string]any
			for i := 0; i < 2+r.IntN(4); i++ {
				base.clock = 1_700_000_000_000 + r.Int64N(100*minute)
				title := titles[r.IntN(len(titles))]
				anchor := titles[r.IntN(len(titles))]
				places := []map[string]any{atEnd(), atStart(), q.after(anchor)}
				at := places[r.IntN(len(places))]
				k, id := keyBetween(t, "", "")
				deps := []string{}
				if i == 0 {
					deps = append(deps, seed["op_id"].(string))
				}
				switch r.IntN(5) {
				case 0:
					facts = append(facts, q.qfact(d, "queue.add", map[string]any{"members": []any{q.placement(title, k, id)}, "place": at}, []string{title}, deps...))
				case 1:
					pl := q.placement(title, k, id)
					pl["place"] = at
					facts = append(facts, q.qfact(d, "queue.move", pl, []string{title}, deps...))
				case 2:
					facts = append(facts, q.qfact(d, "queue.remove", map[string]any{"episodes": []any{q.refs[title]}, "reason": "user"}, []string{title}, deps...))
				case 3:
					facts = append(facts, q.qfact(d, "queue.restore", map[string]any{"members": []any{q.placement(title, k, id)}, "place": at}, []string{title}, deps...))
				default:
					facts = append(facts, q.qfact(d, "queue.clear", map[string]any{"observed": []any{q.refs[title]}}, []string{title}, deps...))
				}
			}
			plans = append(plans, plan{name, facts})
		}
		var reference map[string]string
		var refOrder string
		for order := 0; order < 4; order++ {
			e := newEnvWithAccount(t, base)
			e.clock = 1_700_000_000_000 + 200*minute
			e.push(e.device("A"), sub, seed)
			next := make([]int, len(plans))
			for {
				var ready []int
				for i, pl := range plans {
					if next[i] < len(pl.facts) {
						ready = append(ready, i)
					}
				}
				if len(ready) == 0 {
					break
				}
				i := ready[r.IntN(len(ready))]
				pl := plans[i]
				if res := e.push(e.device(pl.device), pl.facts[next[i]]); res[0].Status != "accepted" || outcome(res[0]) == "rejected" {
					t.Fatalf("trial %d: %+v", trial, res[0])
				}
				next[i]++
			}
			got := e.records()
			eq := &queueEnv{listenEnv: &listenEnv{env: e, p: p, g: g}, refs: q.refs}
			gotOrder := eq.order(t)
			for k := range got {
				if strings.HasSuffix(k, "/queue_position") {
					delete(got, k)
				}
			}
			if reference == nil {
				reference, refOrder = got, gotOrder
				continue
			}
			if gotOrder != refOrder {
				t.Fatalf("trial %d order %d: queue %s, first order %s", trial, order, gotOrder, refOrder)
			}
			for k, v := range reference {
				if got[k] != v {
					t.Fatalf("trial %d order %d: %s = %s, first order %s", trial, order, k, got[k], v)
				}
			}
			if len(got) != len(reference) {
				t.Fatalf("trial %d order %d: %d records, first order %d", trial, order, len(got), len(reference))
			}
		}
	}
}
