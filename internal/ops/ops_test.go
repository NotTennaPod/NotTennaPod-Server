package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

type env struct {
	t         *testing.T
	svc       *protocol.Service
	account   string
	namespace string
	devices   map[string]*device
	clock     int64
	noRebuild string // why the rebuild invariant does not apply ("" = it does)
	// replayCheck compares the queue replay's incremental state with a full
	// replay, and the published queue with its order, after every push.
	replayCheck bool
}

// derivedWithoutArchive exempts a test from the rebuild invariant: it seeds
// causal nodes and contributions directly (a load test's history), which no
// archive holds.
func (e *env) derivedWithoutArchive() { e.noRebuild = "it seeds derived state without an archive" }

type device struct {
	inst     storage.Installation
	seq      int64 // install_seq
	author   int64
	authorID string // the author it writes as ("": its installation; a copy writes as its original)
	last     string
	pending  []protocol.PushOp
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, devices: map[string]*device{}, clock: 1_700_000_000_000}
	err = store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		e.account, e.namespace = a.ID, a.Namespace
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewRegistry()
	Register(reg)
	views, err := protocol.NewViewRegistry(filepath.Join(store.Dir(), "views"), nil)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = &protocol.Service{Store: store, Registry: reg, Views: views, Box: box, Now: func() time.Time { return time.UnixMilli(e.clock) }}
	// Every test ends with the rebuild invariant (PROTOCOL §6.11) on the env's account
	// as it is then (before the store closes).
	t.Cleanup(func() {
		if e.noRebuild != "" {
			t.Logf("rebuild invariant: not applicable: %s", e.noRebuild)
			return
		}
		checkRebuild(t, e.svc, e.account)
	})
	return e
}

var idCounter int

func newID() string {
	idCounter++
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", idCounter)
}

func (e *env) device(name string) *device {
	if d, ok := e.devices[name]; ok {
		return d
	}
	d := &device{inst: storage.Installation{InstallID: newID(), InstanceNonce: newID(), Name: name, Platform: "ios", AppVersion: "1",
		Capabilities: `["favorites","history","inbox","queue","statistics"]`}}
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(e.account).InsertInstallation(d.inst)
	})
	if err != nil {
		e.t.Fatal(err)
	}
	e.devices[name] = d
	return d
}

// fact builds a fact from device d (author = its installation).
func (e *env) fact(d *device, typ string, deps []string, targets []any, payload map[string]any) map[string]any {
	d.author++
	op := newID()
	depList := []any{}
	for _, x := range deps {
		depList = append(depList, x)
	}
	author := d.inst.InstallID
	if d.authorID != "" {
		author = d.authorID
	}
	f := map[string]any{"op_id": op, "author_id": author, "author_seq": d.author, "dependencies": depList,
		"type": typ, "type_version": int64(1), "client_time": e.clock, "targets": targets, "payload": payload}
	if d.last != "" {
		f["previous_op_id"] = d.last
	}
	d.last = op
	return f
}

func (e *env) head() contract.Cursor {
	var c contract.Cursor
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		h, err := r.Account(e.account).Head()
		c = contract.Cursor{LogID: h.LogID, Epoch: h.Epoch, Seq: h.Seq, Digest: h.Digest}
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// push sends facts from d and returns their results.
func (e *env) push(d *device, facts ...map[string]any) []protocol.PushResult {
	e.t.Helper()
	var ops []protocol.PushOp
	acked := d.seq
	for _, f := range facts {
		a, err := protocol.ParseFact(f)
		if err != nil {
			e.t.Fatal(err)
		}
		d.seq++
		ops = append(ops, protocol.PushOp{InstallSeq: d.seq, Fact: a})
	}
	inst := d.inst
	resp, err := e.svc.Push(context.Background(), protocol.PushRequest{AccountID: e.account, Cursor: e.head(), InstallID: inst.InstallID,
		AckedInstallSeq: acked, Operations: ops}, func(tx *storage.WriteTx) (*storage.Installation, error) {
		i, err := tx.Account(e.account).Installation(inst.InstallID)
		return &i, err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	if e.replayCheck {
		checkReplay(e.t, e.svc, e.account)
	}
	return resp.Results
}

func (e *env) records() map[string]string {
	out := map[string]string{}
	err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
		rows, err := r.Account(e.account).Records(nil)
		for _, x := range rows {
			out[x.Entity+"/"+x.EntityID+"/"+x.FieldGroup] = x.Value
		}
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}

func outcome(r protocol.PushResult) string {
	o, _ := r.Receipt["outcome"].(string)
	if reason, ok := r.Receipt["reason"].(string); ok {
		return o + "/" + reason
	}
	return o
}

func (e *env) episodeRef(podcast, generation, guid string) map[string]any {
	id, _ := contract.EpisodeID(e.namespace, podcast, contract.KindRSSGUID, guid)
	return map[string]any{"episode_id": id, "podcast_id": podcast, "generation_id": generation,
		"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": guid, "guid": guid, "title": "Episode " + guid}}
}

func epTarget(ref map[string]any) []any {
	return []any{map[string]any{"entity": "episode", "id": ref["episode_id"], "generation": ref["generation_id"]}}
}

func podTarget(p, g string) []any {
	return []any{map[string]any{"entity": "podcast", "id": p, "generation": g}}
}

func TestSubscribeMarkUnsubscribeAndStaleResubscribe(t *testing.T) {
	e := newEnv(t)
	a, b := e.device("A"), e.device("B")
	sub := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://Example.com/feed.xml"})
	r := e.push(a, sub)
	if outcome(r[0]) != "applied" {
		t.Fatalf("subscribe: %+v", r[0])
	}
	p := contract.PodcastID(e.namespace, sub["op_id"].(string))
	g := contract.GenerationID(e.namespace, p, sub["op_id"].(string))
	recs := e.records()
	if recs["podcast/"+p+"/subscription"] != `{"generation_id":"`+g+`","state":"subscribed"}` {
		t.Fatalf("subscription record: %v", recs)
	}
	if recs["podcast/"+p+"/policy"] != `{"keep_updated":true,"new_episodes_action":"global"}` {
		t.Fatalf("policy record: %v", recs["podcast/"+p+"/policy"])
	}
	ref := e.episodeRef(p, g, "ep-1")
	e.clock += 1000
	mark := e.fact(a, "episode.mark_played", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episode": ref, "reset_position": true})
	if r := e.push(a, mark); outcome(r[0]) != "applied" {
		t.Fatalf("mark: %+v", r[0])
	}
	recs = e.records()
	ep := ref["episode_id"].(string)
	if recs["episode/"+ep+"/play_state"] != `{"generation_id":"`+g+`","play_state":"played"}` {
		t.Fatalf("play state: %v", recs)
	}
	// B, offline before seeing A's mark, favorites the episode; then A unsubscribes.
	e.clock += 1000
	fav := e.fact(b, "favorite.set", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episode": ref, "on": true})
	unsub := e.fact(a, "podcast.unsubscribe", []string{mark["op_id"].(string)}, podTarget(p, g), map[string]any{})
	if r := e.push(a, unsub); outcome(r[0]) != "applied" {
		t.Fatalf("unsubscribe: %+v", r[0])
	}
	recs = e.records()
	if _, ok := recs["episode/"+ep+"/play_state"]; ok {
		t.Fatalf("episode state survived the unsubscribe: %v", recs)
	}
	if recs["podcast/"+p+"/subscription"] != `{"generation_id":"`+g+`","state":"removed"}` {
		t.Fatalf("removed: %v", recs["podcast/"+p+"/subscription"])
	}
	// B's later listening-era action on the removed generation is recorded, not applied (PROTOCOL
	// §7.2).
	if r := e.push(b, fav); outcome(r[0]) != "not_applied/generation_closed" {
		t.Fatalf("late favorite: %+v", r[0])
	}
	// B re-subscribes without having observed the removal: stale.
	stale := e.fact(b, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed.xml",
		"observed": map[string]any{"podcast_id": p, "generation_id": g, "state": "subscribed"}})
	if r := e.push(b, stale); outcome(r[0]) != "not_applied/stale_subscribe" {
		t.Fatalf("stale subscribe: %+v", r[0])
	}
	// A re-subscribes having observed the removal: a fresh generation with fresh state.
	resub := e.fact(a, "podcast.subscribe", []string{unsub["op_id"].(string)}, []any{}, map[string]any{"feed_url": "https://example.com/feed.xml",
		"observed": map[string]any{"podcast_id": p, "generation_id": g, "state": "removed"}})
	if r := e.push(a, resub); outcome(r[0]) != "applied" {
		t.Fatalf("resubscribe: %+v", r[0])
	}
	g2 := contract.GenerationID(e.namespace, p, resub["op_id"].(string))
	recs = e.records()
	if recs["podcast/"+p+"/subscription"] != `{"generation_id":"`+g2+`","state":"subscribed"}` {
		t.Fatalf("resubscribed: %v", recs["podcast/"+p+"/subscription"])
	}
	if err := e.svc.Store.VerifyChain(context.Background(), e.account); err != nil {
		t.Fatal(err)
	}
}

// A snapshot carries the live records after the archive (PROTOCOL §6.7).
func TestSnapshotCarriesRecords(t *testing.T) {
	e := newEnv(t)
	a := e.device("A")
	sub := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
	e.push(a, sub)
	p := contract.PodcastID(e.namespace, sub["op_id"].(string))
	g := contract.GenerationID(e.namespace, p, sub["op_id"].(string))
	ref := e.episodeRef(p, g, "x")
	e.push(a, e.fact(a, "favorite.set", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episode": ref, "on": true}))
	resp, err := e.svc.Snapshot(context.Background(), protocol.SnapshotRequest{AccountID: e.account, Mode: "attach", InstallID: a.inst.InstallID})
	if err != nil {
		t.Fatal(err)
	}
	records := 0
	for _, it := range resp.Items {
		raw, _ := it.(json.RawMessage)
		v, err := contract.DecodeJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		if rec, ok := v.(map[string]any)["record"].(map[string]any); ok {
			records++
			if rec["entity"] == "episode" && rec["field_group"] == "favorite" && rec["value"].(map[string]any)["on"] != true {
				t.Fatalf("favorite record %v", rec)
			}
		}
	}
	if want := len(e.records()); records != want || records == 0 {
		t.Fatalf("snapshot has %d records, store %d", records, want)
	}
}
