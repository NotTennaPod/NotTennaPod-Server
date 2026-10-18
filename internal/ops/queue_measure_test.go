package ops

// The queue replay's cost (PROTOCOL §7.6, measured separately per dimension;
// server budgets, not the phones' 100 ms). NTP_QUEUE_MEASURE=<commands> runs
// it, e.g. 10000 or 100000; the live queue is kept near NTP_QUEUE_LIVE (default
// 1000) items; NTP_QUEUE_BATCH (default
// 250) commands go in one push (one commit: one publication, at most one
// layout).

import (
	"encoding/json"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/queue"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

func TestQueueReplayMeasure(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("NTP_QUEUE_MEASURE"))
	if n == 0 {
		t.Skip("set NTP_QUEUE_MEASURE to the number of queue commands")
	}
	live := 1000
	if s := os.Getenv("NTP_QUEUE_LIVE"); s != "" {
		live, _ = strconv.Atoi(s)
	}
	batchSize := 250
	if s := os.Getenv("NTP_QUEUE_BATCH"); s != "" {
		batchSize, _ = strconv.Atoi(s)
	}
	episodes := max(live*2, n/10)
	if s := os.Getenv("NTP_QUEUE_EPISODES"); s != "" {
		episodes, _ = strconv.Atoi(s)
	}
	titles := make([]string, episodes)
	for i := range titles {
		titles[i] = fmt.Sprintf("E%06d", i)
	}
	q := newQueueEnv(t, titles...)
	q.replayCheck = false
	q.noRebuild = "a measurement: the rebuild of a large history is measured by TestRebuildLoad"
	viewstest.Skip(q.svc.Store)      // a measurement of the queue: the read model's own checks would dominate it
	skipPublicationCheck.Store(true) // the check of every publication against the full computation, too
	defer skipPublicationCheck.Store(false)
	r := mrand.New(mrand.NewPCG(7, 11))
	devs := []*device{q.device("A"), q.device("B"), q.device("C")}
	// Each phone predicts keys as a device does, between its view's
	// neighbours at the place (PROTOCOL §7.6), and pulls before a push every
	// pullEvery of its pushes (1: always; more: phones offline in between,
	// so concurrent predictions conflict).
	pullEvery := 1
	if s := os.Getenv("NTP_QUEUE_PULL_EVERY"); s != "" {
		pullEvery, _ = strconv.Atoi(s)
	}
	views := make([]*queueView, len(devs))
	for i := range views {
		views[i] = &queueView{key: map[string]string{}, pid: map[string]string{}}
	}
	// Fill the live queue first.
	began := time.Now()
	for i := 0; i < live; i += 250 {
		v := views[0]
		var members []any
		for _, x := range titles[i:min(i+250, live)] {
			members = append(members, v.place(t, q, x, atEnd(), ""))
		}
		q.clock += second
		q.pushOK(t, devs[0], q.qfact(devs[0], "queue.add", map[string]any{"members": members, "place": atEnd()}, titles[i:min(i+250, live)]), "applied")
	}
	t.Logf("filled a %d-item queue in %s", live, time.Since(began))
	var fillSeq int64
	q.svc.Store.Read(t.Context(), func(r *storage.ReadTx) error {
		h, err := r.Account(q.account).Head()
		fillSeq = h.Seq
		return err
	})
	pushes := make([]int, len(devs))
	done := live / 250
	began = time.Now()
	var pushTime time.Duration
	for done < n {
		di := r.IntN(len(devs))
		d, v := devs[di], views[di]
		if pushes[di]%pullEvery == 0 {
			v.sync(t, q)
		}
		pushes[di]++
		pick := func() string { return v.order[r.IntN(len(v.order))] }
		place := func() (map[string]any, string) {
			switch r.IntN(3) {
			case 0:
				return atStart(), ""
			case 1:
				return atEnd(), ""
			}
			a := pick()
			return q.after(a), a
		}
		var batch []map[string]any
		for j := 0; j < batchSize && done < n; j++ {
			q.clock += second
			done++
			switch k := r.IntN(1000); {
			case k < 4: // a sort command now and then (its receipt records every member)
				batch = append(batch, q.sortCmd(d, "episode_title_a_z", nil, nil))
				sort.Strings(v.order)
			case k < 450 || len(v.order) < live/2:
				x := titles[r.IntN(len(titles))]
				if len(v.order) > live {
					x = pick() // a duplicate add keeps the queue near its size
				}
				at, anchor := place()
				batch = append(batch, q.qfact(d, "queue.add", map[string]any{"members": []any{v.place(t, q, x, at, anchor)}, "place": at}, []string{x}))
			case k < 750:
				x := pick()
				at, anchor := place()
				m := v.move(t, q, x, at, anchor)
				m["place"] = at
				batch = append(batch, q.qfact(d, "queue.move", m, []string{x}))
			default:
				x := pick()
				v.remove(x)
				batch = append(batch, q.remove(d, x))
			}
		}
		pb := time.Now()
		for _, res := range q.push(d, batch...) {
			if o := outcome(res); o == "rejected/invalid_payload" {
				t.Fatalf("%v", res)
			}
		}
		pushTime += time.Since(pb)
		if done%5000 < batchSize {
			t.Logf("%d commands, %s pushing", done, pushTime)
		}
	}
	t.Logf("%d queue commands pushed in %s (%s in pushes)", n, time.Since(began), pushTime)
	var steps, events, live2 int
	if err := q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
		a := tx.Account(q.account)
		s, err := a.QueueStepsAfter(-1)
		if err != nil {
			return err
		}
		e, err := a.QueueEventsAfter(-1)
		if err != nil {
			return err
		}
		qe, err := a.LiveRecordIDs("episode", "queue_position")
		steps, events, live2 = len(s), len(e), len(qe)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("history: %d replay steps, %d membership events, %d distinct episodes, %d in the queue", steps, events, len(titles), live2)
	// Archive and pull growth (PROTOCOL §7.6): layouts are
	// recorded only on order conflicts and list only the members whose key
	// changes; a queue_position record is republished only when it changes.
	st := q.queueGrowth(t, fillSeq)
	var dbBytes int64
	if entries, err := os.ReadDir(q.svc.Store.Dir()); err == nil {
		for _, e := range entries {
			if info, err := e.Info(); err == nil && !e.IsDir() {
				dbBytes += info.Size()
			}
		}
	}
	t.Logf("archive after the fill: %d layouts listing %d members in all (%.2f MB); %d queue_position records published in %d commits (%.2f per command); data directory %.1f MB",
		st.layouts, st.members, float64(st.layoutBytes)/1e6, st.positions, st.commits, float64(st.positions)/float64(n-live/250), float64(dbBytes)/1e6)
	// Cold cache: a full replay with the materialization's candidates and
	// keys (no layout is needed: the published keys fit).
	for _, run := range []string{"cold", "cold again"} {
		var load, replayDur, mat time.Duration
		if err := q.svc.Probe(t.Context(), q.account, func(c *protocol.ApplyContext) error {
			c.Account.Memo().Delete(memoKey{q.account})
			*c.QueueMemo() = nil
			t0 := time.Now()
			nodes, _, err := loadReplayNodes(c, -1)
			if err != nil {
				return err
			}
			load = time.Since(t0)
			t1 := time.Now()
			st := queue.NewState()
			for _, nd := range nodes {
				st.Apply(nd)
			}
			replayDur = time.Since(t1)
			t2 := time.Now()
			*c.QueueMemo() = &commitReplay{snaps: []snapshot{{state: st}}, fresh: true}
			c.QueueInvalidateAll()
			if err := publishQueue(c, false, true); err != nil {
				return err
			}
			mat = time.Since(t2)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: load %d nodes %s, replay %s, full publication (replay again + layout check) %s", run, steps+events, load, replayDur, mat)
	}
	// The publication of a live add's commit, isolated: warm cache, the
	// replay resumes from the last snapshot (the add's own node touched).
	q.clock += second
	q.push(devs[0], q.add(t, devs[0], atEnd(), "", titles[r.IntN(len(titles))]))
	var lastStep string
	q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
		s, err := tx.Account(q.account).QueueStepsAfter(-1)
		lastStep = s[len(s)-1].Node
		return err
	})
	for i := 0; i < 3; i++ {
		if err := q.svc.Probe(t.Context(), q.account, func(c *protocol.ApplyContext) error {
			c.QueueTouch(lastStep)
			t0 := time.Now()
			err := publishQueue(c, false, true)
			t.Logf("incremental publication of the last step (warm cache): %s", time.Since(t0))
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A live add: warm cache (incremental from the last snapshot), then cold.
	for _, cold := range []bool{false, true} {
		if cold {
			q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
				tx.Account(q.account).Memo().Delete(memoKey{q.account})
				return nil
			})
		}
		q.clock += second
		views[0].sync(t, q)
		var x string
		for x = titles[r.IntN(len(titles))]; views[0].has(x); x = titles[r.IntN(len(titles))] {
		}
		f := q.qfact(devs[0], "queue.add", map[string]any{"members": []any{views[0].place(t, q, x, atEnd(), "")}, "place": atEnd()}, []string{x})
		t0 := time.Now()
		q.push(devs[0], f)
		t.Logf("a live add of a new member, %s cache: %s", map[bool]string{false: "warm", true: "cold"}[cold], time.Since(t0))
		if !cold {
			// What the add's commit carries.
			var head int64
			q.svc.Store.Read(t.Context(), func(r *storage.ReadTx) error {
				h, err := r.Account(q.account).Head()
				head = h.Seq
				return err
			})
			g := q.queueGrowth(t, head-1)
			t.Logf("that add's commit: %d layouts (%d members), %d queue_position records", g.layouts, g.members, g.positions)
		}
	}
	// Steady state, as a person uses the queue: single removes and adds,
	// one per push, with a warm cache; then the first command after a
	// restart (the in-memory cache gone). NTP_QUEUE_CPUPROFILE writes a CPU
	// profile of the warm pushes.
	q.measureSteadyState(t, devs[0], views[0], titles, r)
	// A months-late upload: an offline device's move ordered near the start
	// of the history (the earliest invalidation).
	late := q.device("Late")
	saved := q.clock
	q.clock = 1_700_000_000_000 + 2*second
	views[0].sync(t, q)
	f := q.move(t, late, views[0].order[0], atStart())
	q.clock = saved
	t0 := time.Now()
	q.push(late, f)
	t.Logf("a months-late move ordered near the start: %s", time.Since(t0))
	checkReplay(t, q.svc, q.account)
}

// measureSteadyState times single-command pushes with a warm cache and the
// first one after a restart.
func (q *queueEnv) measureSteadyState(t *testing.T, d *device, v *queueView, titles []string, r *mrand.Rand) {
	one := func() (string, time.Duration) {
		q.clock += second
		v.sync(t, q)
		var f map[string]any
		kind := "remove"
		if r.IntN(2) == 0 && len(v.order) > 1 {
			x := v.order[r.IntN(len(v.order))]
			v.remove(x)
			f = q.remove(d, x)
		} else {
			kind = "add"
			var x string
			for x = titles[r.IntN(len(titles))]; v.has(x); x = titles[r.IntN(len(titles))] {
			}
			f = q.qfact(d, "queue.add", map[string]any{"members": []any{v.place(t, q, x, atEnd(), "")}, "place": atEnd()}, []string{x})
		}
		t0 := time.Now()
		q.push(d, f)
		return kind, time.Since(t0)
	}
	stop := func() {}
	if path := os.Getenv("NTP_QUEUE_CPUPROFILE"); path != "" {
		out, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		pprof.StartCPUProfile(out)
		stop = func() { pprof.StopCPUProfile(); out.Close() }
	}
	var times []time.Duration
	for i := 0; i < 40; i++ {
		_, dur := one()
		times = append(times, dur)
	}
	stop()
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	t.Logf("warm single-command pushes: median %s, p90 %s, max %s", times[len(times)/2], times[len(times)*9/10], times[len(times)-1])
	for i := 0; i < 2; i++ {
		q.svc.Store.Write(t.Context(), func(tx *storage.WriteTx) error {
			tx.Account(q.account).Memo().Delete(memoKey{q.account})
			return nil
		})
		full0, _ := replayCounts()
		kind, dur := one()
		full1, _ := replayCounts()
		_, warm := one()
		t.Logf("after a restart: first command (%s) %s (%d full replays), the next %s", kind, dur, full1-full0, warm)
	}
	q.measureWarmup(t, one)
}

type growth struct {
	commits, layouts, members, layoutBytes, positions int
}

// queueGrowth counts, in the commits after seq, the layouts recorded, the
// members they list, their size, and the queue_position records published.
func (q *queueEnv) queueGrowth(t *testing.T, after int64) growth {
	t.Helper()
	var g growth
	if err := q.svc.Store.Read(t.Context(), func(r *storage.ReadTx) error {
		a := r.Account(q.account)
		h, err := a.Head()
		if err != nil {
			return err
		}
		for seq := after; seq < h.Seq; {
			page, err := a.Commits(seq, h.Seq, 200)
			if err != nil || len(page) == 0 {
				return err
			}
			for _, c := range page {
				seq = c.Seq
				g.commits++
				var body struct {
					Decisions []map[string]any `json:"decisions"`
					Records   []map[string]any `json:"records"`
				}
				if err := json.Unmarshal(c.Canonical, &body); err != nil {
					return err
				}
				for _, d := range body.Decisions {
					if d["decision_type"] == "queue.layout" {
						b, _ := json.Marshal(d)
						g.layouts++
						g.layoutBytes += len(b)
						result, _ := d["result"].(map[string]any)
						members, _ := result["members"].([]any)
						g.members += len(members)
					}
				}
				for _, rec := range body.Records {
					if rec["field_group"] == "queue_position" {
						g.positions++
					}
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

// queueView is a phone's view of the queue: its order, keys and position
// IDs as last pulled, with its own commands since applied.
type queueView struct {
	order    []string
	key, pid map[string]string
}

func (v *queueView) has(x string) bool { _, ok := v.key[x]; return ok }

// sync pulls the server's queue: the in-members in key order.
func (v *queueView) sync(t *testing.T, q *queueEnv) {
	recs := q.records()
	type entry struct {
		title    string
		key      contract.QueueKey
		raw, pid string
	}
	var es []entry
	for title, ref := range q.refs {
		ep := ref["episode_id"].(string)
		if m, ok := recs["episode/"+ep+"/queue_membership"]; !ok || !strings.Contains(m, `"in_queue":true`) {
			continue
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(recs["episode/"+ep+"/queue_position"]), &p); err != nil {
			t.Fatalf("%s: %v", title, err)
		}
		k, err := contract.ParseQueueKey(p["key"].(string))
		if err != nil {
			t.Fatal(err)
		}
		es = append(es, entry{title, k, p["key"].(string), p["position_id"].(string)})
	}
	sort.Slice(es, func(i, j int) bool { return contract.CompareQueueKeys(es[i].key, es[j].key) < 0 })
	v.order, v.key, v.pid = v.order[:0], map[string]string{}, map[string]string{}
	for _, e := range es {
		v.order = append(v.order, e.title)
		v.key[e.title], v.pid[e.title] = e.raw, e.pid
	}
}

func (v *queueView) remove(x string) {
	for i, y := range v.order {
		if y == x {
			v.order = append(v.order[:i], v.order[i+1:]...)
			break
		}
	}
	delete(v.key, x)
	delete(v.pid, x)
}

// insert puts x at the place (after anchor, if any) and predicts its key
// between its neighbours there (any key past a neighbour pair out of order).
func (v *queueView) insert(t *testing.T, x string, at map[string]any, anchor string) (string, string) {
	i := len(v.order)
	switch at["kind"] {
	case "start":
		i = 0
	case "after":
		for j, y := range v.order {
			if y == anchor {
				i = j + 1
				break
			}
		}
	}
	low, high := "", ""
	if i > 0 {
		low = v.key[v.order[i-1]]
	}
	if i < len(v.order) {
		high = v.key[v.order[i]]
	}
	if low != "" && high != "" {
		a, _ := contract.ParseQueueKey(low)
		b, _ := contract.ParseQueueKey(high)
		if contract.CompareQueueKeys(a, b) >= 0 {
			high = ""
		}
	}
	k, id := keyBetween(t, low, high)
	v.order = append(v.order, "")
	copy(v.order[i+1:], v.order[i:])
	v.order[i] = x
	v.key[x], v.pid[x] = k, id
	return k, id
}

// place is an add's member: a queued episode keeps its key (Android skips
// it); another gets a predicted key at the place.
func (v *queueView) place(t *testing.T, q *queueEnv, x string, at map[string]any, anchor string) map[string]any {
	if v.has(x) {
		return q.placement(x, v.key[x], v.pid[x])
	}
	k, id := v.insert(t, x, at, anchor)
	return q.placement(x, k, id)
}

// move is a move's payload: the episode at its new place with a predicted key.
func (v *queueView) move(t *testing.T, q *queueEnv, x string, at map[string]any, anchor string) map[string]any {
	v.remove(x)
	k, id := v.insert(t, x, at, anchor)
	return q.placement(x, k, id)
}
