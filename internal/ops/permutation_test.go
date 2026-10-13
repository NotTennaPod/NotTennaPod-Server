package ops

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// TestArrivalOrderDoesNotChangeProjections: after a fixed subscription (so no
// acceptance-time allocation depends on the order), several offline devices'
// episode, Inbox, favorite and policy facts arrive in different interleavings
// (each device's own facts in order); every interleaving gives the same records.
func TestArrivalOrderDoesNotChangeProjections(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 12))
	for trial := 0; trial < 8; trial++ {
		type plan struct {
			device string
			facts  []map[string]any
		}
		// Build the facts once in a template env; replay them in several orders.
		base := newEnv(t)
		a := base.device("A")
		sub := base.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
		p := contract.PodcastID(base.namespace, sub["op_id"].(string))
		g := contract.GenerationID(base.namespace, p, sub["op_id"].(string))
		var plans []plan
		for _, name := range []string{"B", "C", "D"} {
			d := base.device(name)
			var facts []map[string]any
			for i := 0; i < 2+r.IntN(5); i++ {
				ref := base.episodeRef(p, g, fmt.Sprintf("ep-%d", r.IntN(3)))
				base.clock = 1_700_000_000_000 + r.Int64N(100_000)
				switch r.IntN(6) {
				case 0:
					facts = append(facts, base.fact(d, "episode.mark_played", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episode": ref, "reset_position": r.IntN(2) == 0}))
				case 1:
					facts = append(facts, base.fact(d, "episode.mark_unplayed", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episode": ref}))
				case 2:
					facts = append(facts, base.fact(d, "favorite.set", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episode": ref, "on": r.IntN(2) == 0}))
				case 3:
					facts = append(facts, base.fact(d, "inbox.dismiss", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episodes": []any{ref}}))
				case 4:
					facts = append(facts, base.fact(d, "episode.restore_state", []string{sub["op_id"].(string)}, epTarget(ref), map[string]any{"episode": ref, "play_state": []string{"new", "unplayed", "played"}[r.IntN(3)], "position": int64(r.IntN(5000))}))
				default:
					facts = append(facts, base.fact(d, "podcast.set_policy", []string{sub["op_id"].(string)}, podTarget(p, g), map[string]any{"keep_updated": r.IntN(2) == 0}))
				}
			}
			plans = append(plans, plan{name, facts})
		}
		var reference map[string]string
		for order := 0; order < 4; order++ {
			e := newEnvWithAccount(t, base)
			e.push(e.device("A"), sub)
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
				e.clock = pl.facts[next[i]]["client_time"].(int64) + 1000
				if res := e.push(e.device(pl.device), pl.facts[next[i]]); res[0].Status != "accepted" {
					t.Fatalf("trial %d: %+v", trial, res[0])
				}
				next[i]++
			}
			got := e.records()
			if reference == nil {
				reference = got
				continue
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

// newEnvWithAccount creates a fresh store whose account and installations have
// the same IDs as base's, so base's facts can be replayed into it.
func newEnvWithAccount(t *testing.T, base *env) *env {
	e := newEnv(t)
	ctx := context.Background()
	e.devices = map[string]*device{}
	err := e.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if _, err := tx.CreateAccountWithIDs(base.account, base.namespace); err != nil {
			return err
		}
		for name, d := range base.devices {
			if err := tx.Account(base.account).InsertInstallation(d.inst); err != nil {
				return err
			}
			e.devices[name] = &device{inst: d.inst}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	e.account, e.namespace = base.account, base.namespace
	return e
}

var _ = protocol.PushOp{}
