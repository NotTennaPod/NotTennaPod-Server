package ops

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// openUpgradeFixture opens testdata/account-6b.sql, an account an earlier server version synced, with
// today's migrations, and returns its listening environment, device A, the session and its
// start.
func openUpgradeFixture(t *testing.T) (*listenEnv, *device, string, string) {
	t.Helper()
	var x struct {
		Account, Namespace, P, G, Ep, Session, Start, Last string
		Ref                                                map[string]any
		Clock, Seq, Author                                 int64
		IDs                                                int
		Install                                            storage.Installation
	}
	b, err := os.ReadFile("testdata/account-6b.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &x); err != nil {
		t.Fatal(err)
	}
	dump, err := os.ReadFile("testdata/account-6b.sql")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "nottennapod.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(dump)); err != nil {
		t.Fatal(err)
	}
	db.Close()
	ctx := context.Background()
	store, err := storage.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewRegistry()
	Register(reg)
	views, err := protocol.NewViewRegistry(filepath.Join(store.Dir(), "views"), nil)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, account: x.Account, namespace: x.Namespace, clock: x.Clock, devices: map[string]*device{}}
	e.svc = &protocol.Service{Store: store, Registry: reg, Views: views, Box: box, Now: func() time.Time { return time.UnixMilli(e.clock) }}
	t.Cleanup(func() { checkRebuildUpgraded(t, e.svc, e.account) })
	idCounter = max(idCounter, x.IDs)
	a := &device{inst: x.Install, seq: x.Seq, author: x.Author, last: x.Last}
	e.devices["A"] = a
	return &listenEnv{env: e, p: x.P, g: x.G, ref: x.Ref, ep: x.Ep}, a, x.Session, x.Start
}

// Upgrading from the fixture: the stored session effects and statistics cuts name no
// continuation, and migrations 0007–0009 must carry them over, so listening
// after the upgrade continues the same continuation (300 s then 360 s once counted 660 s, and a
// reset left 300 s).
func TestUpgradeFromAnEarlierServer(t *testing.T) {
	l, a, s, startOp := openUpgradeFixture(t)
	if got := l.playedMS(t); got != 100*second {
		t.Fatalf("played time before listening on: %d ms, want 100 s (400 s after the reset at 300 s)", got)
	}
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.checkpoint", l.ref, checkpoint(s, 3, 460*second, 460*second)), "applied")
	if got := l.playedMS(t); got != 160*second {
		t.Errorf("played time after the upgrade: %d ms, want 160 s", got)
	}
	l.expect(t, "position", `{"claim_id":"`+startOp+`","generation_id":"`+l.g+`","position":460000}`)
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "statistics.reset", nil, map[string]any{}), "applied")
	if got := l.playedMS(t); got != 0 {
		t.Errorf("played time after a reset: %d ms, want 0", got)
	}
	// The pre-upgrade start's claim still holds: a finalization naming it completes.
	l.clock += minute
	l.pushOK(t, a, l.lfact(a, "session.finalize", l.ref, finalize(s, 3, startOp, true)), "applied")
	if !contains(l.record("play_state"), `"play_state":"played"`) {
		t.Errorf("finalization after the upgrade: %s", l.record("play_state"))
	}
}

// A copy detected after the upgrade forks from the device's writing tip, a fact the fixture's
// server rejected: migration 0010 placed it, so the fork stands and the
// copy's tail export continues the session.
func TestUpgradeFromAnEarlierServerForkFromRejectedTip(t *testing.T) {
	l, a, s, _ := openUpgradeFixture(t)
	b := l.device("B")
	l.clock += minute
	l.pushOK(t, b, l.fact(b, "installation.fork", []string{a.last}, []any{}, map[string]any{"author": a.inst.InstallID, "head": a.last}), "applied")
	l.clock += minute
	l.pushOK(t, b, l.lfact(b, "session.checkpoint", l.ref, checkpoint(s, 3, 460*second, 460*second)), "applied")
	if got := l.playedMS(t); got != 160*second {
		t.Errorf("played time after the copy's tail: %d ms, want 160 s", got)
	}
}
