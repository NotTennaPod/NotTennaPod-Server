package protocol

// Load (PROTOCOL §6.13): a 500-operation push and a 4 MiB framed fact while other clients pull.
// The write transaction's duration and peak memory are recorded; only generous bounds are
// asserted.

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadFiveHundredOperationPushAndFourMiBFactUnderPulls(t *testing.T) {
	if testing.Short() {
		t.Skip("load measurement")
	}
	h := newHarness(t)
	in, reader := h.addInstall(), h.addInstall()
	h.fill(in, 3)
	genesis := h.genesis()

	var peak atomic.Uint64
	stopSampler, samplerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(samplerDone)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			select {
			case <-stopSampler:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	var pages atomic.Int64
	stopPullers := make(chan struct{})
	var pullers sync.WaitGroup
	for i := 0; i < 3; i++ {
		pullers.Add(1)
		go func(limit int) {
			defer pullers.Done()
			for {
				req := PullRequest{AccountID: h.account, Cursor: genesis, InstallID: reader, Limit: limit}
				for {
					select {
					case <-stopPullers:
						return
					default:
					}
					r, err := h.svc.Pull(h.ctx, req)
					if err != nil {
						t.Errorf("pull during load: %v", err)
						return
					}
					pages.Add(1)
					if !r.HasMore {
						break
					}
					req.Page = r.Next
					// A phone pulls page by page, not in a tight loop.
					time.Sleep(time.Millisecond)
				}
			}
		}(1 + 4*i)
	}
	// The transaction body ends at the Service's beforeCommit hook.
	var bodyEnd time.Time
	h.svc.beforeCommit = func() error { bodyEnd = time.Now(); return nil }

	author, prev := randomID(), ""
	var ops []PushOp
	for i := 1; i <= MaxOperations; i++ {
		op := randomID()
		var deps []string
		if prev != "" {
			deps = []string{prev}
		}
		f := fs{op: op, author: author, seq: int64(i), prev: prev, deps: deps, ct: h.clock.millis() - 1000,
			targets: []any{map[string]any{"entity": "episode", "id": "e1:" + strings.Repeat("ab", 32)}}, payload: text(strings.Repeat("n", 200))}.object()
		ops = append(ops, fop(int64(3+i), f))
		prev = op
	}
	runtime.GC()
	peak.Store(0)
	start := time.Now()
	r := h.push(in, 3, ops...)
	push500, body500 := time.Since(start), bodyEnd.Sub(start)
	peak500 := peak.Load()
	if len(r.Results) != MaxOperations || r.InstallWatermark != 3+MaxOperations {
		t.Fatalf("500-operation push: %d results, watermark %d", len(r.Results), r.InstallWatermark)
	}
	for _, res := range r.Results {
		if res.Status != "accepted" || res.Receipt["outcome"] != "applied" {
			t.Fatalf("500-operation push result %+v", res)
		}
	}
	commit500 := len(h.commitsOf(h.account)[h.head().Seq-1])
	bound := 10 * time.Second
	if raceEnabled {
		bound = 30 * time.Second // the race detector slows SQLite (pure Go) by about 20×
	}
	if push500 > bound {
		t.Errorf("a 500-operation push took %v", push500)
	}

	big := fs{op: randomID(), author: randomID(), seq: 1, ct: 1, payload: text(strings.Repeat("m", 4<<20-400))}.object()
	size := len(canon(big))
	parts := partOps(504, big, MaxPartBytes, randomID())
	runtime.GC()
	peak.Store(0)
	start = time.Now()
	var completing, completingBody time.Duration
	for i, op := range parts {
		s := time.Now()
		r := h.push(in, 503, op)
		want := "part_stored"
		if i == len(parts)-1 {
			completing, completingBody = time.Since(s), bodyEnd.Sub(s)
			want = "accepted"
		}
		if r.Results[0].Status != want {
			t.Fatalf("part %d: %s", i, wireJSON(t, r.Results[0]))
		}
	}
	framedTotal := time.Since(start)
	peakBig := peak.Load()
	if completing > bound {
		t.Errorf("the push completing a 4 MiB fact took %v", completing)
	}
	close(stopPullers)
	pullers.Wait()
	close(stopSampler)
	<-samplerDone
	t.Logf("500-operation push: %v (transaction body %v), commit %d bytes, peak heap %.1f MiB", push500.Round(time.Millisecond),
		body500.Round(time.Millisecond), commit500, float64(peak500)/(1<<20))
	t.Logf("4 MiB framed fact (%d bytes, %d parts): all parts %v; completing push %v (transaction body %v); peak heap %.1f MiB",
		size, len(parts), framedTotal.Round(time.Millisecond), completing.Round(time.Millisecond), completingBody.Round(time.Millisecond),
		float64(peakBig)/(1<<20))
	t.Logf("%d pull pages served concurrently (race detector: %v)", pages.Load(), raceEnabled)

	if raceEnabled {
		return // framing of large commits and items is verified by the view tests
	}
	// The 4 MiB commit reaches a puller as frames and verifies; so does the
	// snapshot item.
	p, err := h.pullAll(h.account, reader, h.headAt(h.head().Seq-1), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.framed < 4 || len(p.commits) != 1 {
		t.Errorf("4 MiB commit: %d frames, %d commits", p.framed, len(p.commits))
	}
	s, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: reader})
	if err != nil {
		t.Fatal(err)
	}
	if s.framed < 4 || s.count != 3+MaxOperations+1 {
		t.Errorf("snapshot: %d frames, %d items", s.framed, s.count)
	}
	h.verifyLog(h.account)
}
