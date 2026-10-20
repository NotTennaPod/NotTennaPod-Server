package ops

// A subscription made during an outage (PROTOCOL §15.2, §16 scenario 4a): the
// facts a phone records while its registration waits (its drafts) depend on the
// registration's podcast.resolution, which takes the registration's normalized
// time (§6.1). The drafts keep the time the person acted (§6.9), so they merge
// with another device's changes by time instead of all landing after them.

import (
	"context"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func TestPendingRegistrationKeepsItsTime(t *testing.T) {
	// A's offline listening and queue add come before B's reset and removal:
	// B's later changes win.
	t.Run("the other device's later changes win", func(t *testing.T) { pendingRegistration(t, false) })
	// A's offline actions come after B's: A's stand.
	t.Run("the phone's later actions stand", func(t *testing.T) { pendingRegistration(t, true) })
}

func pendingRegistration(t *testing.T, phoneLater bool) {
	e := newEnv(t)
	e.replayCheck = true
	a, b := e.device("A"), e.device("B")
	base := e.clock

	// A, offline: subscribes at base (sent much later).
	subA := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})

	// B subscribes at base+10m, queues the episode, resets its position and removes it.
	e.clock = base + 10*minute
	p, g, _ := subscribeOne(t, e, b)
	ref := e.episodeRef(p, g, "one")
	ep := ref["episode_id"].(string)
	e.clock = base + 11*minute
	k, id := keyBetween(t, "", "")
	e.pushOK(t, b, queueAdd(e, b, ref, k, id), "applied")
	e.clock = base + 12*minute
	e.pushOK(t, b, e.lfact(b, "episode.reset_position", ref, map[string]any{}), "applied")
	e.clock = base + 13*minute
	rm := e.lfact(b, "queue.remove", nil, map[string]any{"episodes": []any{ref}, "reason": "user"})
	rm["targets"] = epTarget(ref)
	e.pushOK(t, b, rm, "applied")

	// A reconnects at base+60m: its registration joins B's podcast.
	e.clock = base + 60*minute
	if r := e.push(a, subA); outcome(r[0]) != "applied" {
		t.Fatalf("A's subscribe: %v", r[0].Receipt)
	}
	var decision string
	var decisionTime, subTime int64
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		acct := tx.Account(e.account)
		var err error
		if decision, err = acct.DecisionBySubject("podcast.resolution", subA["op_id"].(string)); err != nil {
			return err
		}
		if decisionTime, err = acct.NodeTime(decision); err != nil {
			return err
		}
		subTime, err = acct.NodeTime(subA["op_id"].(string))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if subTime != base || decisionTime != subTime {
		t.Errorf("A's subscribe at base+%dms, its resolution at base+%dms: want both at base", subTime-base, decisionTime-base)
	}

	// A's drafts, made offline, materialized now: each depends on the
	// resolution (and on A's previous fact).
	offset := int64(0)
	if phoneLater {
		offset = 20
	}
	e.clock = base + 61*minute
	drafts := []map[string]any{}
	at := func(m int64, f func() map[string]any) {
		saved := e.clock
		e.clock = base + (offset+m)*minute
		drafts = append(drafts, f())
		e.clock = saved
	}
	sa := newID()
	at(1, func() map[string]any { return e.lfact(a, "session.start", ref, start(sa, 0, true), decision) })
	at(2, func() map[string]any {
		return e.lfact(a, "session.checkpoint", ref, checkpoint(sa, 1, minute, minute), decision)
	})
	ka, ida := keyBetween(t, "", "")
	at(3, func() map[string]any { return queueAdd(e, a, ref, ka, ida, decision) })
	for i, r := range e.push(a, drafts...) {
		nt, _ := integer(r.Receipt["normalized_time"])
		if want := base + (offset+int64(i)+1)*minute; nt != want || outcome(r) != "applied" {
			t.Errorf("draft %d: %s at base+%dms, want applied at base+%dms", i, outcome(r), nt-base, want-base)
		}
	}

	recs := e.records()
	pos, membership := recs["episode/"+ep+"/position"], recs["episode/"+ep+"/queue_membership"]
	if phoneLater {
		if !contains(pos, `"position":60000`) {
			t.Errorf("A's later listening did not stand: %s", pos)
		}
		if !contains(membership, `"in_queue":true`) {
			t.Errorf("A's later queue add did not stand: %s", membership)
		}
		return
	}
	if !contains(pos, `"position":0`) {
		t.Errorf("A's older listening replaced B's later reset: %s", pos)
	}
	if !contains(membership, `"in_queue":false`) || !contains(membership, `"reason":"user"`) {
		t.Errorf("A's older queue add resurrected B's later removal: %s", membership)
	}
}

// An offline registration that creates its podcast: its resolution is at the
// registration's time, its alias keeps today's rule, max(server_now, the
// registration's) (PROTOCOL §6.1; docs/CONTRACT.md "Recovery").
func TestOfflineRegistrationDecisionTimes(t *testing.T) {
	e := newEnv(t)
	a := e.device("A")
	base := e.clock
	sub := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
	e.clock = base + 60*minute
	if r := e.push(a, sub); outcome(r[0]) != "applied" {
		t.Fatalf("A's subscribe: %v", r[0].Receipt)
	}
	times := map[string]int64{}
	if err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		acct := tx.Account(e.account)
		for typ, subject := range map[string]string{"podcast.resolution": sub["op_id"].(string),
			"podcast.alias": "alias:https://example.com/feed"} {
			id, err := acct.DecisionBySubject(typ, subject)
			if err != nil {
				return err
			}
			if times[typ], err = acct.NodeTime(id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if times["podcast.resolution"] != base || times["podcast.alias"] != base+60*minute {
		t.Errorf("resolution at base+%dms, alias at base+%dms: want base and base+%dms",
			times["podcast.resolution"]-base, times["podcast.alias"]-base, 60*minute)
	}
}

// queueAdd is a queue.add of one episode at the end, depending on deps.
func queueAdd(e *env, d *device, ref map[string]any, key, id string, deps ...string) map[string]any {
	f := e.lfact(d, "queue.add", nil, map[string]any{"members": []any{map[string]any{"episode": ref, "key": key, "position_id": id}},
		"place": atEnd()}, deps...)
	f["targets"] = epTarget(ref)
	return f
}
