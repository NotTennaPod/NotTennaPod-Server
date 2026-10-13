package ops

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// A history clear and a statistics reset over years of listening: the cut's
// traversal of its causal past plus the refold of every covered episode must
// stay workable (PROTOCOL §8). NTP_LISTEN_LOAD sets the number
// of checkpoints (about 220,000 for five years of two hours a day at one
// exported checkpoint a minute); make check runs a smaller size.
func TestHistoryClearAndResetLoad(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("load test (make check runs it without the race detector)")
	}
	n := 20_000
	if s := os.Getenv("NTP_LISTEN_LOAD"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	const perEpisode = 110
	e := newEnv(t)
	d := e.device("A")
	p, g, sub := subscribeOne(t, e, d)
	e.derivedWithoutArchive()
	seedStart := time.Now()
	var lastSeed string
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		prev := sub["op_id"].(string)
		var session, scope string
		for i := 0; i < n; i++ {
			node := newID()
			if err := a.InsertNode(node, "fact", e.clock+int64(i+1), []string{prev}); err != nil {
				return err
			}
			prev = node
			ordinal := int64(i % perEpisode)
			if ordinal == 0 {
				ep, _ := contract.EpisodeID(e.namespace, p, contract.KindRSSGUID, fmt.Sprint(i))
				if err := a.AddEpisode(ep, p, `{"k":"rss-guid","v":"x"}`, ""); err != nil {
					return err
				}
				scope, session = protocol.EpisodeScope(g, ep), newID()
				if err := a.StartSession(session, scope, d.inst.InstallID, node); err != nil {
					return err
				}
				if err := a.Contribute(scope, node, fmt.Sprintf(`{"activity":true,"kind":"session_start","position":0,"session":%q,"time":%d}`, session, e.clock+int64(i+1))); err != nil {
					return err
				}
			} else {
				if err := a.AddSessionFact(session, d.inst.InstallID, ordinal, node); err != nil {
					return err
				}
				if err := a.Contribute(scope, node, fmt.Sprintf(`{"activity":true,"contribution":%d,"kind":"session_checkpoint","ordinal":%d,"position":%d,"session":%q,"time":%d}`,
					ordinal*60_000, ordinal, ordinal*60_000, session, e.clock+int64(i+1))); err != nil {
					return err
				}
			}
			if err := a.AddListeningFact(storage.ListeningFact{Node: node, Scope: scope, HistoryEntry: true, Session: session, Ordinal: ordinal}); err != nil {
				return err
			}
		}
		lastSeed = prev
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d checkpoints in %d episodes in %s", n, (n+perEpisode-1)/perEpisode, time.Since(seedStart))
	e.clock += int64(n + 100)
	for _, typ := range []string{"history.clear", "statistics.reset"} {
		deps := []string{lastSeed, d.last}
		slices.Sort(deps)
		f := e.fact(d, typ, deps, []any{}, map[string]any{})
		begin := time.Now()
		r := e.push(d, f)
		elapsed := time.Since(begin)
		if outcome(r[0]) != "applied" {
			t.Fatal(r)
		}
		t.Logf("%s over %d checkpoints: %s", typ, n, elapsed)
		if elapsed > time.Duration(n/1000+20)*time.Second {
			t.Fatalf("%s took %s", typ, elapsed)
		}
	}
	recs := e.records()
	cleared := 0
	for k, v := range recs {
		if strings.HasSuffix(k, "/history") {
			cleared++
		}
		if strings.HasSuffix(k, "/history") && !strings.HasSuffix(v, `"time":null}`) {
			t.Fatalf("%s = %s after a clear", k, v)
		}
	}
	if want := (n + perEpisode - 1) / perEpisode; cleared != want {
		t.Fatalf("%d history records, want %d", cleared, want)
	}
}

// A long offline session uploaded in batches: listening facts refold their
// episode once per push, so a batch's cost does not grow with the number of
// saves in it.
func TestLongOfflineSessionUpload(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("load test (make check runs it without the race detector)")
	}
	l := newListenEnv(t)
	a := l.device("A")
	s := newID()
	st := l.lfact(a, "session.start", l.ref, start(s, 0, true))
	l.pushOK(t, a, st, "applied")
	for batch := 0; batch < 6; batch++ {
		facts := make([]map[string]any, 500)
		for i := range facts {
			ord := int64(batch*500 + i + 1)
			l.clock += minute
			facts[i] = l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, ord, ord*minute, ord*minute))
		}
		begin := time.Now()
		r := l.push(a, facts...)
		elapsed := time.Since(begin)
		if outcome(r[len(r)-1]) != "applied" {
			t.Fatal(r)
		}
		t.Logf("batch ending at %d saves: %s", (batch+1)*500, elapsed)
		if elapsed > 10*time.Second {
			t.Fatalf("batch took %s", elapsed)
		}
	}
	l.expect(t, "position", `{"claim_id":"`+st["op_id"].(string)+`","generation_id":"`+l.g+`","position":180000000}`)
}
