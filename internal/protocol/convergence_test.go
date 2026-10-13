package protocol

// Concurrency and convergence (PROTOCOL §6.4, §6.9). Several
// installations push randomized interleavings with retries, duplicates, lost
// responses, framed slots, copied facts, op_id conflicts, rejections and
// decisions, while other clients pull with random page sizes, repeated tokens
// and lost page responses. Every pulling replica, built only from pulled
// commits verified against the digest chain from genesis and with every receipt
// and decision MAC checked under the receipt keys, must end with exactly the
// server's facts, receipts, decisions and slots.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

type convSlot struct {
	op     string
	obj    map[string]any
	framed bool
}

type convReplica struct {
	install   string
	cursor    contract.Cursor
	facts     map[string][2][]byte
	decisions map[string][]byte
	slots     map[string][]byte // "install/seq" → canonical slot entry
	pulls     int
}

func slotKey(install string, seq int64) string { return fmt.Sprintf("%s/%d", install, seq) }

// pullOnce runs one complete pull view from the replica's cursor and applies it.
func (rp *convReplica) pullOnce(t *testing.T, h *harness, ctx context.Context, keys map[string][]byte, rng *rand.Rand) bool {
	req := PullRequest{AccountID: h.account, Cursor: rp.cursor, InstallID: rp.install, Limit: 1 + rng.IntN(5)}
	var staged [][]byte
	fr := &frames{kind: "commit_part"}
	head, watermark := "", int64(-1)
	for {
		resp, err := h.svc.Pull(ctx, req)
		if err != nil {
			t.Errorf("pull: %v", err)
			return false
		}
		page := wireJSON(t, resp)
		if req.Page != "" && rng.IntN(4) == 0 {
			again, err := h.svc.Pull(ctx, req)
			if err != nil || !bytes.Equal(wireJSON(t, again), page) {
				t.Errorf("a repeated page token returned a different page (%v)", err)
				return false
			}
		}
		if rng.IntN(5) == 0 {
			continue // the response was lost; ask again with what the client holds
		}
		var p wirePage
		if err := json.Unmarshal(page, &p); err != nil {
			t.Error(err)
			return false
		}
		if head == "" {
			head, watermark = p.Head, p.InstallWatermark
		} else if p.Head != head || p.InstallWatermark != watermark {
			t.Errorf("view head or watermark changed between pages")
			return false
		}
		for _, item := range p.Items {
			c, done, _, err := fr.add("commit_part", item)
			if err != nil {
				t.Errorf("pull item: %v", err)
				return false
			}
			if done {
				staged = append(staged, c)
			}
		}
		if !p.HasMore {
			break
		}
		req.Page = p.Next
	}
	hc, err := contract.DecodeCursor(head)
	if err != nil {
		t.Errorf("head: %v", err)
		return false
	}
	// Apply only after the whole view verifies against the chain.
	digest := rp.cursor.Digest
	for i, c := range staged {
		v, err := contract.DecodeJSON(c)
		if err != nil || !bytes.Equal(canon(v), c) || num(v.(map[string]any)["seq"]) != rp.cursor.Seq+int64(i)+1 {
			t.Errorf("pulled commit %d is not the canonical commit at its seq", i)
			return false
		}
		digest, _ = contract.ChainDigest(digest, c)
	}
	if hc.LogID != rp.cursor.LogID || int64(len(staged)) != hc.Seq-rp.cursor.Seq || !bytes.Equal(digest, hc.Digest) {
		t.Errorf("staged commits do not verify from %d to %d", rp.cursor.Seq, hc.Seq)
		return false
	}
	for _, c := range staged {
		v, _ := contract.DecodeJSON(c)
		commit := v.(map[string]any)
		for _, e := range commit["accepted"].([]any) {
			entry := e.(map[string]any)
			f, r := entry["fact"].(map[string]any), entry["receipt"].(map[string]any)
			op := f["op_id"].(string)
			if _, dup := rp.facts[op]; dup {
				t.Errorf("fact %s first committed twice", op)
			}
			if key, ok := keys[r["key_id"].(string)]; !ok || !contract.VerifyMAC(key, h.account, r) {
				t.Errorf("receipt of %s does not verify", op)
			}
			if hsh, _ := contract.ContentHash(f); r["content_hash"] != hsh {
				t.Errorf("receipt of %s names another content", op)
			}
			rp.facts[op] = [2][]byte{canon(f), canon(r)}
		}
		for _, d := range commit["decisions"].([]any) {
			m := d.(map[string]any)
			id := m["decision_id"].(string)
			if _, dup := rp.decisions[id]; dup {
				t.Errorf("decision %s committed twice", id)
			}
			if key, ok := keys[m["key_id"].(string)]; !ok || !contract.VerifyMAC(key, h.account, m) {
				t.Errorf("decision %s does not verify", id)
			}
			rp.decisions[id] = canon(m)
		}
		for _, s := range commit["slots"].([]any) {
			m := s.(map[string]any)
			k := slotKey(m["install_id"].(string), num(m["install_seq"]))
			if _, dup := rp.slots[k]; dup {
				t.Errorf("slot %s committed twice", k)
			}
			rp.slots[k] = canon(m)
		}
	}
	rp.cursor = hc
	// install_watermark is exact at the view's head.
	var mine int64
	for k := range rp.slots {
		if install, seq, ok := strings.Cut(k, "/"); ok && install == rp.install {
			var n int64
			fmt.Sscan(seq, &n)
			mine = max(mine, n)
		}
	}
	if watermark != mine {
		t.Errorf("install_watermark %d, but the log up to the head has %d slots for the installation", watermark, mine)
	}
	rp.pulls++
	return true
}

func TestConvergenceUnderConcurrentPushesAndPulls(t *testing.T) {
	h := newHarness(t)
	const installs, perInstall = 4, 30
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	gen := rand.New(rand.NewPCG(seed, 1))
	ids := make([]string, installs)
	authors := make([]string, installs)
	for i := range ids {
		ids[i], authors[i] = h.addInstall("queue"), randomID()
	}
	// Generate every installation's outbox in one global order: dependencies only
	// name earlier facts, so every interleaving can make progress.
	outboxes := make([][]convSlot, installs)
	var global []convSlot
	lastOwn := make([]string, installs)
	authorSeq := make([]int64, installs)
	ct := h.clock.millis() - 3_600_000
	for g := 0; g < installs*perInstall; g++ {
		i := gen.IntN(installs)
		ct += int64(gen.IntN(2000)) - 500
		var deps []string
		for k := gen.IntN(4); k > 0 && len(global) > 0; k-- {
			d := global[gen.IntN(len(global))].op
			if !slices.Contains(deps, d) {
				deps = append(deps, d)
			}
		}
		kind := gen.IntN(100)
		var s convSlot
		switch {
		case kind < 7 && len(global) > 0: // a copied library resends another installation's fact
			src := global[gen.IntN(len(global))]
			s = convSlot{op: src.op, obj: src.obj}
		case kind < 11 && len(global) > 0: // another fact reusing a known op_id
			src := global[gen.IntN(len(global))]
			s = convSlot{op: src.op, obj: fs{op: src.op, author: randomID(), seq: 1, deps: deps, ct: ct, payload: text("conflict " + randomID())}.object()}
			if slices.Contains(deps, src.op) {
				s.obj["dependencies"] = []any{} // never a self-dependency here
			}
		default:
			op := randomID()
			authorSeq[i]++
			f := fs{op: op, author: authors[i], seq: authorSeq[i], prev: lastOwn[i], deps: deps, ct: ct}
			switch {
			case kind < 20:
				f.typ, f.payload = "test.decide", text("subject-"+op)
			case kind < 25:
				f.payload = map[string]any{"invalid": true}
			case kind < 28:
				// an envelope violation (type_version 0), set below
			case kind < 31:
				f.typ = "test.inbox_note" // outside the declared capabilities
			case kind < 35:
				f.typ = "test.queue_note"
			}
			obj := f.object()
			if kind >= 25 && kind < 28 {
				obj["type_version"] = 0
			}
			lastOwn[i] = op
			s = convSlot{op: op, obj: obj, framed: kind >= 90}
			global = append(global, s)
		}
		outboxes[i] = append(outboxes[i], s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	genesis := h.genesis()
	keys := h.keys()
	var pushers sync.WaitGroup
	for i := range ids {
		pushers.Add(1)
		go func(i int) {
			defer pushers.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(100+i)))
			outbox := outboxes[i]
			n := int64(len(outbox))
			acked := int64(0)
			present := map[int64]map[int]bool{}
			receipts := map[int64][]byte{}
			conflicts := map[int64]bool{}
			partsOf := map[int64][]map[string]any{}
			for acked < n {
				if ctx.Err() != nil {
					t.Errorf("installation %d did not finish (acked %d of %d)", i, acked, n)
					return
				}
				start := acked + 1
				if acked > 0 && rng.IntN(5) == 0 {
					start = max(1, acked+1-int64(1+rng.IntN(3))) // resend acknowledged slots
				}
				var ops []PushOp
				for s := start; s <= n && len(ops) < 1+rng.IntN(6); s++ {
					sl := outbox[s-1]
					if !sl.framed {
						ops = append(ops, fop(s, sl.obj))
						continue
					}
					if partsOf[s] == nil {
						partsOf[s] = partObjects(sl.obj, len(canon(sl.obj))/3+1, randomID())
					}
					var missing []int
					for idx := range partsOf[s] {
						if !present[s][idx] {
							missing = append(missing, idx)
						}
					}
					idx := rng.IntN(len(partsOf[s]))
					if len(missing) > 0 && rng.IntN(4) != 0 {
						idx = missing[rng.IntN(len(missing))]
					}
					ops = append(ops, wirePart(s, partsOf[s][idx]))
					break // a part that does not complete its slot stops the push
				}
				resp, err := h.svc.Push(ctx, PushRequest{AccountID: h.account, Cursor: genesis, InstallID: ids[i], AckedInstallSeq: acked,
					Operations: ops}, authorizer(h.account, ids[i]))
				if err != nil {
					t.Errorf("installation %d push: %v", i, err)
					return
				}
				if rng.IntN(5) == 0 {
					continue // the response was lost
				}
				if resp.InstallWatermark < acked {
					t.Errorf("watermark went back from %d to %d", acked, resp.InstallWatermark)
				}
				wait := false
				for _, r := range resp.Results {
					switch r.Status {
					case "accepted", "duplicate":
						b := canon(r.Receipt)
						if prev, ok := receipts[r.InstallSeq]; (ok && !bytes.Equal(prev, b)) || conflicts[r.InstallSeq] || r.Receipt == nil {
							t.Errorf("installation %d slot %d: a replay changed its result", i, r.InstallSeq)
						}
						receipts[r.InstallSeq] = b
					case "op_id_conflict":
						if receipts[r.InstallSeq] != nil || r.Receipt != nil {
							t.Errorf("installation %d slot %d: op_id_conflict after a receipt", i, r.InstallSeq)
						}
						conflicts[r.InstallSeq] = true
					case "part_stored":
						present[r.InstallSeq] = map[int]bool{}
						for _, p := range r.PartsPresent {
							present[r.InstallSeq][p] = true
						}
					case "dependency_missing":
						wait = true
					default:
						t.Errorf("installation %d: unexpected %s", i, wireJSON(t, r))
						return
					}
				}
				acked = max(acked, resp.InstallWatermark)
				if wait {
					time.Sleep(time.Millisecond) // retry backoff while another installation supplies the dependency
				}
			}
		}(i)
	}
	pushed := make(chan struct{})
	go func() { pushers.Wait(); close(pushed) }()

	replicas := make([]*convReplica, 3)
	var pullers sync.WaitGroup
	for j := range replicas {
		replicas[j] = &convReplica{install: ids[j], cursor: genesis, facts: map[string][2][]byte{}, decisions: map[string][]byte{},
			slots: map[string][]byte{}}
		pullers.Add(1)
		go func(rp *convReplica, j int) {
			defer pullers.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(200+j)))
			for {
				select {
				case <-pushed:
					// Final catch-up to the head after every push finished.
					for rp.cursor.Seq < h.head().Seq {
						if !rp.pullOnce(t, h, ctx, keys, rng) || ctx.Err() != nil {
							return
						}
					}
					return
				default:
				}
				if !rp.pullOnce(t, h, ctx, keys, rng) {
					return
				}
				time.Sleep(time.Duration(rng.IntN(3)) * time.Millisecond)
			}
		}(replicas[j], j)
	}
	pullers.Wait()
	if t.Failed() {
		return
	}

	// The server's state, read independently from the database.
	serverFacts := map[string][2][]byte{}
	rows, err := h.db().Query(`SELECT op_id, fact, receipt FROM facts WHERE account_id = ?`, h.account)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var op string
		var f, r []byte
		rows.Scan(&op, &f, &r)
		serverFacts[op] = [2][]byte{f, r}
	}
	rows.Close()
	serverDecisions := map[string][]byte{}
	rows, _ = h.db().Query(`SELECT decision_id, decision FROM decisions WHERE account_id = ?`, h.account)
	for rows.Next() {
		var id string
		var d []byte
		rows.Scan(&id, &d)
		serverDecisions[id] = d
	}
	rows.Close()
	serverSlots := map[string][]byte{}
	rows, _ = h.db().Query(`SELECT install_id, install_seq, op_id, content_hash, result FROM transport_slots WHERE account_id = ?`, h.account)
	for rows.Next() {
		var install, op, hash, result string
		var seq int64
		rows.Scan(&install, &seq, &op, &hash, &result)
		serverSlots[slotKey(install, seq)] = canon(map[string]any{"install_id": install, "install_seq": seq, "op_id": op,
			"content_hash": hash, "result": result})
	}
	rows.Close()
	for j, rp := range replicas {
		if len(rp.facts) != len(serverFacts) || len(rp.decisions) != len(serverDecisions) || len(rp.slots) != len(serverSlots) {
			t.Errorf("replica %d holds %d facts, %d decisions, %d slots; the server %d, %d, %d", j, len(rp.facts), len(rp.decisions),
				len(rp.slots), len(serverFacts), len(serverDecisions), len(serverSlots))
		}
		for op, fr := range serverFacts {
			if got := rp.facts[op]; !bytes.Equal(got[0], fr[0]) || !bytes.Equal(got[1], fr[1]) {
				t.Errorf("replica %d: fact or receipt %s differs", j, op)
			}
		}
		for id, d := range serverDecisions {
			if !bytes.Equal(rp.decisions[id], d) {
				t.Errorf("replica %d: decision %s differs", j, id)
			}
		}
		for k, s := range serverSlots {
			if !bytes.Equal(rp.slots[k], s) {
				t.Errorf("replica %d: slot %s differs", j, k)
			}
		}
	}
	// Every installation's slots were consumed in order, each with its own content.
	for i, outbox := range outboxes {
		if got := h.consumed(ids[i]); got != int64(len(outbox)) {
			t.Errorf("installation %d consumed %d of %d", i, got, len(outbox))
		}
		for s, sl := range outbox {
			var m map[string]any
			v, _ := contract.DecodeJSON(serverSlots[slotKey(ids[i], int64(s+1))])
			if v != nil {
				m = v.(map[string]any)
			}
			if m == nil || m["op_id"] != sl.op || m["content_hash"] != hashOf(sl.obj) {
				t.Errorf("installation %d slot %d consumed %v, want %s", i, s+1, m, sl.op)
			}
		}
	}
	pulls := 0
	for _, rp := range replicas {
		pulls += rp.pulls
	}
	results := map[string]int{}
	for _, sl := range serverSlots {
		v, _ := contract.DecodeJSON(sl)
		results[v.(map[string]any)["result"].(string)]++
	}
	rejected, framed := 0, 0
	for _, fr := range serverFacts {
		if bytes.Contains(fr[1], []byte(`"outcome":"rejected"`)) {
			rejected++
		}
	}
	for _, outbox := range outboxes {
		for _, sl := range outbox {
			if sl.framed {
				framed++
			}
		}
	}
	t.Logf("%d facts (%d rejected), %d decisions, %d slots %v (%d framed) in %d commits; %d complete pull views", len(serverFacts),
		rejected, len(serverDecisions), len(serverSlots), results, framed, h.head().Seq, pulls)
	h.verifyLog(h.account)
	h.verifySnapshot(ids[0])
}

// A phone that retries while its earlier request is still in flight: concurrent
// pushes of overlapping prefixes of the same outbox consume each slot once, and
// every response carries the same receipt for a slot.
func TestConcurrentRetriesOfTheSameSlotsConsumeEachOnce(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	var ops []PushOp
	prev := ""
	for i := 1; i <= 10; i++ {
		op := uid(5, i)
		ops = append(ops, fop(int64(i), h.note(op, int64(i), prev)))
		prev = op
	}
	results := make([]*PushResponse, 8)
	var wg sync.WaitGroup
	for g := range results {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r, err := h.tryPush(in, 0, ops[:5+g%6]...)
			if err != nil {
				t.Error(err)
				return
			}
			results[g] = r
		}(g)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	receipts := map[int64][]byte{}
	accepted := map[int64]int{}
	for _, r := range results {
		for _, res := range r.Results {
			if res.Status != "accepted" && res.Status != "duplicate" {
				t.Errorf("slot %d: %s", res.InstallSeq, res.Status)
				continue
			}
			if res.Status == "accepted" {
				accepted[res.InstallSeq]++
			}
			b := canon(res.Receipt)
			if prev, ok := receipts[res.InstallSeq]; ok && !bytes.Equal(prev, b) {
				t.Errorf("slot %d answered with two different receipts", res.InstallSeq)
			}
			receipts[res.InstallSeq] = b
		}
	}
	for seq := int64(1); seq <= 10; seq++ {
		if accepted[seq] != 1 {
			t.Errorf("slot %d was accepted %d times", seq, accepted[seq])
		}
	}
	if h.consumed(in) != 10 {
		t.Errorf("consumed %d", h.consumed(in))
	}
	h.verifyLog(h.account)
}
