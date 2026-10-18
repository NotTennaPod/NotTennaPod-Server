package api

// Recovery uploads as durable jobs (PROTOCOL §6.11): a derivation that
// outlasts its request goes on and the request answers server_busy with
// retry_after; nothing is kept against the request, so a repeat is evaluated
// afresh; the job publishes only with the authority the request would have
// at publication; a restart resumes it.

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func (r *recoveryEnv) svc() *protocol.Service { return r.e.srv.cfg.Protocol }

// jobEnded waits for the account's running recovery job to end.
func (r *recoveryEnv) jobEnded() {
	r.e.t.Helper()
	select {
	case <-r.svc().RecoveryJobDoneForTest(r.account):
	case <-time.After(5 * time.Minute):
		r.e.t.Fatal("the recovery job did not end")
	}
}

func (r *recoveryEnv) jobRows() int {
	return r.e.count("SELECT count(*) FROM recovery_jobs WHERE account_id=?", r.account)
}

// gate holds every job attempt at its start until the test lets it go.
type gate struct {
	mu      sync.Mutex
	held    bool
	entered chan struct{}
	release chan struct{}
	runs    atomic.Int32
}

// newGate makes a gate the test lets go of at its end at the latest (its
// env waits for running jobs then).
func newGate(t *testing.T) *gate {
	g := &gate{entered: make(chan struct{}, 64)}
	t.Cleanup(g.let)
	return g
}

// hold makes the next attempts wait for let.
func (g *gate) hold() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.held, g.release = true, make(chan struct{})
}

func (g *gate) let() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held {
		close(g.release)
	}
	g.held = false
}

func (g *gate) point(name string) error {
	if name != "attempt" {
		return nil
	}
	g.runs.Add(1)
	g.mu.Lock()
	held, release := g.held, g.release
	g.mu.Unlock()
	select {
	case g.entered <- struct{}{}:
	default:
	}
	if held {
		<-release
	}
	return nil
}

// latePoll sends an upload as a client on a slow server does: every job is
// slower than the request's wait (the gate holds each until the request
// answered server_busy), and the client comes back only after the job
// ended. It returns the first answer with results and how many busy answers
// came before it; a server that cannot answer a late repeat within the
// request never gives one (bounded at rounds).
func (r *recoveryEnv) latePoll(g *gate, rounds int, entries ...any) (string, int) {
	t := r.e.t
	t.Helper()
	r.svc().SetRecoveryJobHooksForTest(g.point, 50*time.Millisecond, 0)
	for busy := 0; busy < rounds; busy++ {
		g.hold()
		res := r.upload(entries...)
		if res.status != http.StatusServiceUnavailable {
			return uploadStatus(t, res), busy
		}
		res.expect(http.StatusServiceUnavailable, "server_busy")
		if res.retryAfter() < 10 || res.retryAfter() > 30 {
			t.Fatalf("retry_after %d", res.retryAfter())
		}
		g.let()
		r.jobEnded()
		if n := r.jobRows(); n != 0 {
			t.Fatalf("%d job rows after the job ended", n)
		}
	}
	t.Fatalf("no answer after %d late polls", rounds)
	return "", 0
}

// A request that stops waiting is answered server_busy with retry_after;
// the job goes on and publishes. An identical request meanwhile waits for
// it, any other upload is busy, and only one derivation runs. Afterwards an
// identical repeat is evaluated afresh (known).
func TestRecoveryUploadBusyWhileRunning(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	g := newGate(t)
	r.svc().SetRecoveryJobHooksForTest(g.point, 50*time.Millisecond, 0)
	f, rc := r.fact(1)
	entry := map[string]any{"fact": f, "receipt": rc}
	g.hold()
	r.upload(entry).expect(http.StatusServiceUnavailable, "server_busy")
	<-g.entered
	if n := r.jobRows(); n != 1 {
		t.Fatalf("%d job rows while it runs", n)
	}
	again := r.upload(entry).expect(http.StatusServiceUnavailable, "server_busy")
	if again.retryAfter() < 10 {
		t.Errorf("retry_after %d", again.retryAfter())
	}
	other, oc := r.fact(1)
	r.upload(map[string]any{"fact": other, "receipt": oc}).expect(http.StatusServiceUnavailable, "server_busy")
	g.let()
	r.jobEnded()
	if n := g.runs.Load(); n != 1 {
		t.Errorf("%d attempts; want one derivation for the identical requests", n)
	}
	if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=? AND op_id=?", r.account, f["op_id"]); n != 1 {
		t.Fatalf("the job did not publish: %d", n)
	}
	if n := r.jobRows(); n != 0 {
		t.Fatalf("%d job rows after publication", n)
	}
	r.svc().SetRecoveryJobHooksForTest(g.point, 5*time.Minute, 0) // the other upload's job answers within its request
	if st := uploadStatus(t, r.upload(entry)); st != "known" {
		t.Fatalf("repeat after publication: %s", st)
	}
	if st := uploadStatus(t, r.upload(map[string]any{"fact": other, "receipt": oc})); st != "joined" {
		t.Fatalf("the other upload after the job: %s", st)
	}
}

// The existing dependency and frame sequence (TestFrameRequestsAndDependencies),
// in place and staged, on a server where every job outlasts its request and
// the client comes back only after it ended: what joins nothing (parts, a
// part waiting for its parent) is answered within the request, never by a
// job; what joins is answered known by the late repeat. The identical
// last-part request joins once its parent arrived: a dependency_missing
// answer is never replayed.
func TestRecoveryJobFrameRequestsAndDependencies(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "in place", true: "staged"}[staged], func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			g := newGate(t)
			if staged {
				r.svc().StagingThreshold = 1
				seed, rc := r.fact(1)
				uploadStatus(t, r.upload(map[string]any{"fact": seed, "receipt": rc}))
			}
			parent, pr := r.fact(1)
			child, cr := r.fact(1)
			child["dependencies"] = []any{parent["op_id"]}
			cr["content_hash"] = contentHash(t, child)
			r.sign(cr, r.account)
			parts := frame(t, child, 80)
			expect := func(want string, wantBusy int, entries ...any) {
				t.Helper()
				if st, busy := r.latePoll(g, 3, entries...); st != want || busy != wantBusy {
					t.Fatalf("%s after %d busy answers; want %s after %d", st, busy, want, wantBusy)
				}
			}
			for i := 0; i < len(parts)-1; i++ {
				expect("part_stored", 0, map[string]any{"part": parts[i], "receipt": cr})
			}
			last := map[string]any{"part": parts[len(parts)-1], "receipt": cr}
			expect("dependency_missing", 0, last)
			expect("dependency_missing", 0, last)
			expect("known", 1, map[string]any{"fact": parent, "receipt": pr})
			// The completing part joins the frame in a job; its late repeat
			// finds the frame done with and is stored as a new one's part
			// (as the synchronous path stores a lone part); the client sends
			// the other parts again, each answered within its request, and
			// the one completing the frame finds the fact known.
			expect("part_stored", 1, last)
			for i := 0; i < len(parts)-1; i++ {
				want := "part_stored"
				if i == len(parts)-2 {
					want = "known"
				}
				expect(want, 0, map[string]any{"part": parts[i], "receipt": cr})
			}
			if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=? AND op_id=?", r.account, child["op_id"]); n != 1 {
				t.Fatalf("the child did not join: %d", n)
			}
			if n := r.e.count("SELECT count(*) FROM push_parts WHERE account_id=? AND install_seq<0", r.account); n != 0 {
				t.Errorf("%d completed parts left", n)
			}
			if n := g.runs.Load(); n != 2 {
				t.Errorf("%d job attempts; want one for each request that joins", n)
			}
		})
	}
}

// Another device's delayed recovery supplies the missing parent: the first
// device's identical retry then joins.
func TestRecoveryJobOtherDeviceUnblocksRetry(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	b := r.e.phoneOf("alice", "alice-password-1")
	rb := &recoveryEnv{r.e, b, r.account, newID()}
	rb.open()
	parent := b.noteFact("the other device's note")
	pr := map[string]any{"op_id": parent["op_id"], "content_hash": contentHash(t, parent), "outcome": "applied",
		"normalized_time": parent["client_time"], "epoch": 1, "seq": 1, "index": 0}
	r.sign(pr, r.account)
	child, cr := r.fact(1)
	child["dependencies"] = []any{parent["op_id"]}
	cr["content_hash"] = contentHash(t, child)
	cr["index"] = 1
	r.sign(cr, r.account)
	upload := map[string]any{"fact": child, "receipt": cr}
	if st := uploadStatus(t, r.upload(upload)); st != "dependency_missing" {
		t.Fatal(st)
	}
	if st := uploadStatus(t, rb.upload(map[string]any{"fact": parent, "receipt": pr})); st != "joined" {
		t.Fatalf("the other device's upload: %s", st)
	}
	if st := uploadStatus(t, r.upload(upload)); st != "joined" {
		t.Fatalf("the identical retry after the other device's recovery: %s", st)
	}
}

// A frame's stored parts expire after a day: the identical part requests
// store them again (part_stored is no lasting answer), and the frame joins.
func TestRecoveryJobFrameExpiry(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	child, cr := r.fact(1)
	parts := frame(t, child, 80)
	requests := make([]map[string]any, len(parts))
	for i := range parts {
		requests[i] = map[string]any{"part": parts[i], "receipt": cr}
	}
	for _, req := range requests[:len(requests)-1] {
		if st := uploadStatus(t, r.upload(req)); st != "part_stored" {
			t.Fatal(st)
		}
	}
	r.e.clock.Advance(25 * time.Hour)
	last := requests[len(requests)-1]
	if st := uploadStatus(t, r.upload(last)); st != "part_stored" {
		t.Fatalf("the last part after the frame expired: %s", st)
	}
	// The identical requests are evaluated afresh: each part is stored
	// again, and the one completing the frame joins it.
	for i, req := range requests[:len(requests)-1] {
		want := "part_stored"
		if i == len(requests)-2 {
			want = "joined"
		}
		if st := uploadStatus(t, r.upload(req)); st != want {
			t.Fatalf("part %d sent again: %s, want %s", i, st, want)
		}
	}
	if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=? AND op_id=?", r.account, child["op_id"]); n != 1 {
		t.Fatalf("the re-uploaded frame did not join: %d", n)
	}
}

// reopen restarts the server on the data directory (the test's process
// stays): the jobs stored are resumed.
func (r *recoveryEnv) restart(staging int) { r.restartAfter(staging, nil) }

// restartAfter restarts the server, running between (if set) while it is
// stopped.
func (r *recoveryEnv) restartAfter(staging int, between func()) {
	r.e.t.Helper()
	if err := r.e.store.Close(); err != nil {
		r.e.t.Fatal(err)
	}
	if between != nil {
		between()
	}
	r.e.reopen(withSyncBox(r.e.t))
	r.svc().Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error { return nil })
	r.svc().StagingThreshold = staging
	ctx, cancel := context.WithCancel(context.Background())
	wait, err := r.svc().StartRecoveryJobs(ctx)
	if err != nil {
		r.e.t.Fatal(err)
	}
	r.e.t.Cleanup(func() { cancel(); wait() })
}

// crashOnce crashes the job at the named point, once.
func crashOnce(point string) func(string) error {
	var done atomic.Bool
	return func(name string) error {
		if name == point && done.CompareAndSwap(false, true) {
			return protocol.ErrCrashForTest
		}
		return nil
	}
}

// A crash inside the publication of a job holding a conditional entry (the
// last part of a frame whose parent is missing, beside an entry that joins):
// after the restart the job publishes, the part answering dependency_missing
// and nothing kept; once the parent arrives the identical request joins.
func TestRecoveryJobRestartWithConditionalEntry(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	parent, pr := r.fact(1)
	child, cr := r.fact(1)
	child["dependencies"] = []any{parent["op_id"]}
	cr["content_hash"] = contentHash(t, child)
	r.sign(cr, r.account)
	other, oc := r.fact(1)
	parts := frame(t, child, 80)
	for i := 0; i < len(parts)-1; i++ {
		if st := uploadStatus(t, r.upload(map[string]any{"part": parts[i], "receipt": cr})); st != "part_stored" {
			t.Fatal(st)
		}
	}
	request := []any{map[string]any{"fact": other, "receipt": oc}, map[string]any{"part": parts[len(parts)-1], "receipt": cr}}
	r.svc().SetRecoveryJobHooksForTest(crashOnce("publish"), 0, 0)
	r.upload(request...).expect(http.StatusServiceUnavailable, "server_busy")
	r.jobEnded()
	if n := r.jobRows(); n != 1 {
		t.Fatalf("%d job rows after the crash", n)
	}
	r.restart(0)
	r.jobEnded()
	if n := r.jobRows(); n != 0 {
		t.Fatalf("%d job rows after the resumed job", n)
	}
	if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=? AND op_id=?", r.account, other["op_id"]); n != 1 {
		t.Fatal("the resumed job did not publish")
	}
	if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=? AND op_id=?", r.account, child["op_id"]); n != 0 {
		t.Fatal("the child joined without its parent")
	}
	if st := uploadStatus(t, r.upload(map[string]any{"fact": parent, "receipt": pr})); st != "joined" {
		t.Fatal(st)
	}
	res := r.upload(request...).must(http.StatusOK)
	if st := res.body["results"].([]any)[1].(map[string]any)["status"]; st != "joined" {
		t.Fatalf("the identical request after the restart and the parent: %s", res.raw)
	}
}

// Transient failures are tried at most five times; then the job is removed
// and the request answered server_busy. A later identical request starts a
// new job, which publishes.
func TestRecoveryJobAttemptCap(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	var failing atomic.Bool
	failing.Store(true)
	var attempts atomic.Int32
	r.svc().SetRecoveryJobHooksForTest(func(name string) error {
		if name == "attempt" && failing.Load() {
			attempts.Add(1)
			return errors.New("a transient storage failure")
		}
		return nil
	}, time.Minute, time.Millisecond)
	f, rc := r.fact(1)
	entry := map[string]any{"fact": f, "receipt": rc}
	r.upload(entry).expect(http.StatusServiceUnavailable, "server_busy")
	if n := attempts.Load(); n != 5 {
		t.Errorf("%d attempts, want 5", n)
	}
	if n := r.jobRows(); n != 0 {
		t.Fatalf("%d job rows after the last attempt", n)
	}
	failing.Store(false)
	if st := uploadStatus(t, r.upload(entry)); st != "joined" {
		t.Fatal(st)
	}
}

// A job publishes only with the authority the request would have at
// publication: its own credential, checked again in the publishing
// transaction. Each change lands while the staged job derives.
func TestRecoveryJobAuthority(t *testing.T) {
	for _, change := range []string{"none", "sign-out", "revoke", "reset", "supersede"} {
		t.Run(change, func(t *testing.T) {
			r := newRecoveryEnv(t)
			r.raise()
			r.svc().StagingThreshold = 1
			seed, sc := r.fact(1)
			uploadStatus(t, r.upload(map[string]any{"fact": seed, "receipt": sc}))
			r.svc().SetRecoveryJobHooksForTest(nil, 5*time.Minute, 0)
			entered, release := make(chan struct{}), make(chan struct{})
			var once, released sync.Once
			let := func() { released.Do(func() { close(release) }) }
			t.Cleanup(let)
			r.svc().Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error {
				once.Do(func() { close(entered); <-release })
				return nil
			})
			d := r.decision(1)
			done := make(chan *response, 1)
			go func() { done <- r.upload(map[string]any{"decision": d}) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Minute):
				t.Fatal("the staged admission was not reached")
			}
			root := r.e.session("root", "root-password-1")
			switch change {
			case "sign-out":
				r.e.do(http.MethodPost, "/v1/auth/device/sign-out", nil, r.p.cred).must(http.StatusOK)
			case "revoke":
				r.e.revoke(browser(r.e.session("alice", "alice-password-1")), r.p.install).must(http.StatusOK)
			case "reset":
				r.e.resetPassword(root, r.account, "replacement-password").must(http.StatusOK)
			case "supersede":
				if res := r.e.successor(r.p.cred, newID(), r.p.nonce); res.status >= 300 {
					t.Fatalf("successor: %d %s", res.status, res.raw)
				}
			}
			let()
			got := <-done
			r.jobEnded()
			published := r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"])
			if n := r.jobRows(); n != 0 {
				t.Errorf("%d job rows left", n)
			}
			want := map[string]int{"none": 200, "sign-out": 401, "revoke": 401, "reset": 401, "supersede": 409}[change]
			if got.status != want || (published == 1) != (change == "none") {
				t.Fatalf("%s during the job: status %d (want %d), published %d: %s", change, got.status, want, published, got.raw)
			}
			if change == "sign-out" {
				// Signed in again, the identical retry joins.
				r.p.token = r.e.token("alice", "alice-password-1", r.p.install, r.p.nonce)
				r.p.cred = bearer(r.p.token)
				if st := uploadStatus(t, r.upload(map[string]any{"decision": d})); st != "joined" {
					t.Fatalf("the retry after signing in again: %s", st)
				}
			}
		})
	}
}

// A crash just before and just after the job's own lost-timeline
// transition: its fence is stored atomically with the transition or not at
// all, and a restart never substitutes fresh authority. Before the
// transition the resumed job acts with its credential (a sign-out meanwhile
// wins); after it, with its installation and the stored fence (a password
// reset meanwhile wins).
func TestRecoveryJobTransitionCrash(t *testing.T) {
	for _, c := range []struct {
		point, downtime string
	}{{"transition", ""}, {"transition", "sign-out"}, {"transitioned", ""}, {"transitioned", "reset"}} {
		t.Run(c.point+"/"+map[string]string{"": "nothing meanwhile"}[c.downtime]+c.downtime, func(t *testing.T) {
			r := newRecoveryEnv(t)
			pushed(t, r.p.push(op{seq: 1, fact: r.p.noteFact("baseline")}))
			r.svc().StagingThreshold = 1
			r.svc().SetRecoveryJobHooksForTest(crashOnce(c.point), 0, 0)
			d := r.decision(1)
			r.upload(map[string]any{"decision": d}).expect(http.StatusServiceUnavailable, "server_busy")
			r.jobEnded()
			var transitioned, fenced int
			var fence, version int64
			if err := r.e.store.Read(context.Background(), func(tx *storage.ReadTx) error {
				jobs, err := tx.RecoveryJobs()
				if err != nil || len(jobs) != 1 {
					t.Fatalf("stored jobs %v: %v", jobs, err)
				}
				if jobs[0].Transitioned {
					transitioned = 1
				}
				fence = jobs[0].Fence
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			fenced = r.e.count("SELECT count(*) FROM recovery_jobs WHERE account_id=? AND fence IS NOT NULL", r.account)
			version = int64(r.e.count("SELECT credentials_version FROM account_logins WHERE account_id=?", r.account))
			switch c.point {
			case "transition":
				if r.epoch() != 1 || transitioned != 0 || fenced != 0 {
					t.Fatalf("a crash inside the transition left epoch %d, transitioned %d, fenced %d", r.epoch(), transitioned, fenced)
				}
			case "transitioned":
				if r.epoch() != 2 || transitioned != 1 || fence != version {
					t.Fatalf("after the transition: epoch %d, transitioned %d, fence %d (version %d)", r.epoch(), transitioned, fence, version)
				}
			}
			switch c.downtime {
			case "sign-out":
				r.e.do(http.MethodPost, "/v1/auth/device/sign-out", nil, r.p.cred).must(http.StatusOK)
			case "reset":
				r.e.resetPassword(r.e.session("root", "root-password-1"), r.account, "replacement-password").must(http.StatusOK)
			}
			r.restart(1)
			r.jobEnded()
			published := r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"])
			if want := c.downtime == ""; (published == 1) != want {
				t.Fatalf("resumed job published %d; want published %v", published, want)
			}
			if n := r.jobRows(); n != 0 {
				t.Errorf("%d job rows left", n)
			}
			// (A crash inside the transition leaves the restore state's
			// epoch high-water raised: the resumed transition goes past it.)
			if c.downtime == "" && r.epoch() < 2 {
				t.Errorf("epoch %d after the resumed job", r.epoch())
			}
		})
	}
}

// A job's request is sealed at rest: an upload's feed credential never
// reaches the database file in plaintext.
func TestRecoveryJobSecretsSealed(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	g := newGate(t)
	r.svc().SetRecoveryJobHooksForTest(g.point, 50*time.Millisecond, 0)
	marker := "plaintext-" + strings.ReplaceAll(newID(), "-", "")
	f, rc := r.fact(1)
	body := map[string]any{"recovery_id": r.rid, "entries": []any{map[string]any{"fact": f, "receipt": rc}},
		"secrets": []any{map[string]any{"secret_id": newID(), "username": "listener", "password": marker}}}
	g.hold()
	r.e.do(http.MethodPost, "/v1/sync/recovery/upload", body, r.p.cred).expect(http.StatusServiceUnavailable, "server_busy")
	<-g.entered
	if n := r.jobRows(); n != 1 {
		t.Fatalf("%d job rows", n)
	}
	if err := r.e.store.Read(context.Background(), func(tx *storage.ReadTx) error {
		jobs, err := tx.RecoveryJobs()
		if err == nil && bytes.Contains(jobs[0].Request, []byte(marker)) {
			t.Error("the stored request holds the credential in plaintext")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nottennapod.db", "nottennapod.db-wal"} {
		data, err := os.ReadFile(filepath.Join(r.e.dir, name))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(marker)) {
			t.Errorf("%s holds the credential in plaintext", name)
		}
	}
	g.let()
	r.jobEnded()
	if n := r.jobRows(); n != 0 {
		t.Fatalf("%d job rows after publication", n)
	}
}

// The power user's library (NTP_SCALE=1; minutes to hours): the parity
// backup enlarged to 80 podcasts and 26,390 episodes (the owner's library
// has 70 and 26,271). A recovery upload joining one lost entry derives the
// whole account in a staged copy, which outlasts the request: the phone is
// answered server_busy with retry_after until the job published, and the
// identical repeat then answers known, at once (it joins nothing, so it
// derives nothing). Run with -timeout 3h.
func TestRecoveryJobScale(t *testing.T) {
	if os.Getenv("NTP_SCALE") == "" {
		t.Skip("the power user's library: set NTP_SCALE=1")
	}
	r := newReadEnv(t, withImports(t))
	r.importOverHTTP(enlargedBackup(t, 5, 91))
	if err := r.store.Write(context.Background(), func(tx *storage.WriteTx) error { return tx.EvidenceTransition(1) }); err != nil {
		t.Fatal(err)
	}
	rec := &recoveryEnv{r.env, r.readPhone(), r.account, newID()}
	rec.open()
	f, rc := rec.fact(1)
	entry := map[string]any{"fact": f, "receipt": rc}
	start := time.Now()
	busy := 0
	var res *response
	for {
		res = rec.upload(entry)
		if res.status != http.StatusServiceUnavailable {
			break
		}
		res.expect(http.StatusServiceUnavailable, "server_busy")
		if a := res.retryAfter(); a < 10 || a > 30 {
			t.Fatalf("retry_after %d", a)
		}
		busy++
		<-rec.svc().RecoveryJobDoneForTest(rec.account) // minutes at this size; bounded by -timeout
	}
	st := uploadStatus(t, res)
	t.Logf("%d busy answers, then %s, in %.0f s", busy, st, time.Since(start).Seconds())
	if busy > 0 && st != "known" || busy == 0 && st != "joined" {
		t.Fatalf("after %d busy answers: %s", busy, st)
	}
	if n := rec.jobRows(); n != 0 {
		t.Fatalf("%d job rows", n)
	}
	// A repeat that joins nothing is answered within its request, without
	// deriving the account.
	start = time.Now()
	res = rec.upload(entry)
	if st := uploadStatus(t, res); st != "known" {
		t.Fatalf("the no-change repeat: %s", st)
	}
	t.Logf("a no-change repeat answered in %.2f s", time.Since(start).Seconds())
}

// A frame on a server where every job outlasts its request and the client
// comes back only after the job ended: each part is answered part_stored
// within its request, so the client's frame progress advances, and the part
// completing the frame joins it (its late repeat is stored as a new frame's
// part; the parts sent again complete that frame, which finds the fact
// known).
func TestRecoveryJobLatePollAdvancesPart(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	r.svc().StagingThreshold = 1
	seed, sc := r.fact(1)
	uploadStatus(t, r.upload(map[string]any{"fact": seed, "receipt": sc}))
	g := newGate(t)
	f, rc := r.fact(1)
	parts := frame(t, f, 80)
	send := func(i int, want string, wantBusy int) {
		t.Helper()
		if st, busy := r.latePoll(g, 4, map[string]any{"part": parts[i], "receipt": rc}); st != want || busy != wantBusy {
			t.Fatalf("part %d: %s after %d busy answers; want %s after %d", i, st, busy, want, wantBusy)
		}
	}
	for i := range parts {
		wantBusy := 0
		if i == len(parts)-1 {
			wantBusy = 1 // joined by the job; the late repeat is stored as a new frame's part
		}
		send(i, "part_stored", wantBusy)
	}
	for i := 0; i < len(parts)-1; i++ {
		want := "part_stored"
		if i == len(parts)-2 {
			want = "known"
		}
		send(i, want, 0)
	}
	if n := g.runs.Load(); n != 1 {
		t.Errorf("%d job attempts; want one, for the part that joins", n)
	}
	if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=? AND op_id=?", r.account, f["op_id"]); n != 1 {
		t.Fatalf("the framed fact did not join: %d", n)
	}
}

// A recovery as the iOS app resumes it, at each exchange: the recovery
// snapshot's first page (busy while a staged derivation runs), the
// inventory of entries without an answer, then the upload of what still
// needs one (whole facts in one request, a framed fact one part per request,
// its progress kept only from answers). Every job outlasts its request (the
// production 20 s wait, scaled down: 50 ms against jobs the test holds), and
// the app comes back only after the job ended. The recovery finishes.
func TestRecoveryJobClientResumeOrder(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	r.svc().StagingThreshold = 1
	seed, sc := r.fact(1)
	uploadStatus(t, r.upload(map[string]any{"fact": seed, "receipt": sc}))
	g := newGate(t)
	r.svc().SetRecoveryJobHooksForTest(g.point, 50*time.Millisecond, 0)
	type entry struct {
		fact, receipt map[string]any
		parts         []map[string]any
	}
	var entries []*entry
	for i := 0; i < 3; i++ {
		f, rc := r.fact(1)
		entries = append(entries, &entry{fact: f, receipt: rc})
	}
	framed, fr := r.fact(1)
	entries = append(entries, &entry{fact: framed, receipt: fr, parts: frame(t, framed, 80)})
	progress := map[string]string{} // op ID → the last answer
	present := map[int]bool{}       // the framed fact's parts the server answered stored
	recovered := func(st string) bool { return st == "have" || st == "joined" || st == "known" || st == "replaced" }
	busy := func(res *response) bool {
		if res.status != http.StatusServiceUnavailable {
			return false
		}
		res.expect(http.StatusServiceUnavailable, "server_busy")
		g.let() // the app waits; the job ends meanwhile
		r.jobEnded()
		return true
	}
	finished := false
	for exchange := 0; exchange < 12 && !finished; exchange++ {
		g.hold()
		snap := r.e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": r.rid, "install_id": r.p.install,
			"acked_install_seq": 0, "limit": 1}, r.p.cred)
		if busy(snap) {
			continue
		}
		snap.must(http.StatusOK)
		var inventory []any
		for _, e := range entries {
			if progress[e.fact["op_id"].(string)] == "" {
				inventory = append(inventory, map[string]any{"kind": "fact", "op_id": e.fact["op_id"], "content_hash": e.receipt["content_hash"],
					"position": []any{e.receipt["epoch"], e.receipt["seq"], e.receipt["index"]}})
			}
		}
		if len(inventory) > 0 {
			res := r.e.do(http.MethodPost, "/v1/sync/recovery/inventory", map[string]any{"recovery_id": r.rid, "entries": inventory}, r.p.cred)
			if busy(res) {
				continue
			}
			for _, x := range res.must(http.StatusOK).body["results"].([]any) {
				m := x.(map[string]any)
				progress[m["id"].(string)] = m["status"].(string)
			}
		}
		var whole []any
		for _, e := range entries {
			if e.parts == nil && !recovered(progress[e.fact["op_id"].(string)]) {
				whole = append(whole, map[string]any{"fact": e.fact, "receipt": e.receipt})
			}
		}
		stopped := false
		if len(whole) > 0 {
			res := r.upload(whole...)
			if stopped = busy(res); !stopped {
				for _, x := range res.must(http.StatusOK).body["results"].([]any) {
					m := x.(map[string]any)
					progress[m["id"].(string)] = m["status"].(string)
				}
			}
		}
		for _, e := range entries {
			for !stopped && e.parts != nil && !recovered(progress[e.fact["op_id"].(string)]) {
				next := len(e.parts) - 1
				for i := range e.parts {
					if !present[i] {
						next = i
						break
					}
				}
				res := r.upload(map[string]any{"part": e.parts[next], "receipt": e.receipt})
				if stopped = busy(res); stopped {
					break
				}
				m := res.must(http.StatusOK).body["results"].([]any)[0].(map[string]any)
				progress[e.fact["op_id"].(string)] = m["status"].(string)
				if m["status"] != "part_stored" {
					break
				}
				// The app keeps the answer's parts_present as its frame's
				// progress (ServerSyncEngine+Recovery uploadParts).
				present = map[int]bool{}
				for _, i := range m["parts_present"].([]any) {
					present[int(i.(float64))] = true
				}
			}
		}
		if stopped {
			continue
		}
		done := true
		for _, e := range entries {
			done = done && recovered(progress[e.fact["op_id"].(string)])
		}
		if done {
			finish := r.finish()
			if finish.body["complete"] != true {
				t.Fatalf("finish: %s", finish.raw)
			}
			finished = true
		}
	}
	if !finished {
		t.Fatalf("the recovery did not finish: %v", progress)
	}
	t.Logf("finished after %d job attempts", g.runs.Load())
	for _, e := range entries {
		if n := r.e.count("SELECT count(*) FROM facts WHERE account_id=? AND op_id=?", r.account, e.fact["op_id"]); n != 1 {
			t.Fatalf("%s not joined", e.fact["op_id"])
		}
	}
}

// A job's own lost-timeline transition authorizes it only on the timeline
// that transition made. Its fence is stored with that timeline (the
// account's epoch then); a later transition, which signs every device out
// again (another account's recovery, receipt evidence, a backup restored),
// voids it: after a restart, or in the live publication, the job is refused,
// publishes nothing and is removed. With no later transition the resumed job
// publishes (TestRecoveryJobTransitionCrash).
func TestRecoveryJobOwnFenceOnlyOnItsTimeline(t *testing.T) {
	for _, later := range []string{"another account's recovery", "receipt evidence", "backup restored", "receipt evidence during publication"} {
		t.Run(later, func(t *testing.T) {
			ctx := context.Background()
			r := newRecoveryEnv(t)
			pushed(t, r.p.push(op{seq: 1, fact: r.p.noteFact("baseline")}))
			r.svc().StagingThreshold = 1
			d := r.decision(1)
			published := func() int {
				return r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"])
			}
			if later == "receipt evidence during publication" {
				var once sync.Once
				r.svc().Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error {
					if r.epoch() < 2 {
						return nil // the staged copy before the job's own transition
					}
					var err error
					once.Do(func() {
						err = r.e.store.Write(ctx, func(tx *storage.WriteTx) error { return tx.EvidenceTransition(2) })
					})
					return err
				})
				r.svc().SetRecoveryJobHooksForTest(nil, 5*time.Minute, 0)
				got := r.upload(map[string]any{"decision": d})
				r.jobEnded()
				if got.status != http.StatusUnauthorized || published() != 0 || r.jobRows() != 0 || r.epoch() != 3 {
					t.Fatalf("status %d, published %d, job rows %d, epoch %d: %s", got.status, published(), r.jobRows(), r.epoch(), got.raw)
				}
				return
			}
			r.svc().SetRecoveryJobHooksForTest(crashOnce("transitioned"), 0, 0)
			r.upload(map[string]any{"decision": d}).expect(http.StatusServiceUnavailable, "server_busy")
			r.jobEnded()
			if r.epoch() != 2 || r.e.count("SELECT count(*) FROM recovery_jobs WHERE account_id=? AND fence_epoch=2", r.account) != 1 {
				t.Fatalf("after the job's own transition: epoch %d, rows %d", r.epoch(), r.jobRows())
			}
			var between func()
			switch later {
			case "another account's recovery":
				root := r.e.session("root", "root-password-1")
				bob := r.e.createAccount(root, "bob", "bob-password-1", false)
				rb := &recoveryEnv{r.e, r.e.phoneOf("bob", "bob-password-1"), bob, newID()}
				rb.open()
				if st := uploadStatus(t, rb.upload(map[string]any{"decision": rb.decision(2)})); st != "joined" {
					t.Fatalf("the other account's upload: %s", st)
				}
			case "receipt evidence":
				if err := r.e.store.Write(ctx, func(tx *storage.WriteTx) error { return tx.EvidenceTransition(2) }); err != nil {
					t.Fatal(err)
				}
			case "backup restored":
				backup, err := r.e.store.Backup(ctx, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				kp := filepath.Join(t.TempDir(), "keys.json")
				pass := []byte("test bundle passphrase")
				if err := r.e.store.ExportKeys(ctx, kp, pass); err != nil {
					t.Fatal(err)
				}
				bundle, err := os.ReadFile(kp)
				if err != nil {
					t.Fatal(err)
				}
				between = func() {
					if _, err := storage.Restore(ctx, storage.RestoreOptions{DataDir: r.e.dir, Backup: backup.Path, Bundle: bundle, Passphrase: pass}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if later != "backup restored" && r.epoch() != 3 {
				t.Fatalf("the later transition left epoch %d", r.epoch())
			}
			r.restartAfter(1, between)
			r.jobEnded()
			if n := published(); n != 0 {
				t.Fatalf("the job published on a timeline it did not make (epoch %d)", r.epoch())
			}
			if n := r.jobRows(); n != 0 {
				t.Fatalf("%d job rows left", n)
			}
		})
	}
}

// An upload answered at once and another device's staged job of the same
// account: installation A's part of
// an incomplete frame is evaluated while installation B's upload, which
// joins, starts its job. A's evaluation holds the account's rebuild claim
// across its transaction, so B is answered busy rather than copying the
// account under it (or, had B's copy come first, A would be busy and write
// nothing). After B's job published, A's acknowledged part and its
// recovery's request count are intact.
func TestRecoveryJobCheapUploadRacesStagedJob(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	r.svc().StagingThreshold = 1
	seed, sc := r.fact(1)
	uploadStatus(t, r.upload(map[string]any{"fact": seed, "receipt": sc}))
	b := &recoveryEnv{r.e, r.e.phoneOf("alice", "alice-password-1"), r.account, newID()}
	b.open()
	f, rc := r.fact(1)
	parts := frame(t, f, 80)
	requests := func() int {
		return r.e.count("SELECT json_extract(counts, '$.requests') FROM recoveries WHERE account_id=? AND recovery_id=?", r.account, r.rid)
	}
	before := requests()
	aEntered, releaseA := make(chan struct{}), make(chan struct{})
	bEntered, releaseB := make(chan struct{}), make(chan struct{})
	var aFirst, bFirstAdmit atomic.Bool
	var letA, letB sync.Once
	t.Cleanup(func() { letA.Do(func() { close(releaseA) }); letB.Do(func() { close(releaseB) }) })
	r.svc().SetRecoveryJobHooksForTest(func(point string) error {
		if point == "cheap" && aFirst.CompareAndSwap(false, true) {
			close(aEntered) // A's evaluation, before its transaction
			<-releaseA
		}
		return nil
	}, 5*time.Minute, time.Millisecond)
	r.svc().Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error {
		if bFirstAdmit.CompareAndSwap(false, true) {
			close(bEntered) // in B's staged copy
			<-releaseB
		}
		return nil
	})
	aDone := make(chan *response, 1)
	go func() { aDone <- r.upload(map[string]any{"part": parts[0], "receipt": rc}) }()
	<-aEntered
	d := b.decision(1)
	bDone := make(chan *response, 1)
	go func() { bDone <- b.upload(map[string]any{"decision": d}) }()
	var bFirst *response
	select {
	case bFirst = <-bDone: // answered at once: A holds the account
	case <-bEntered: // B's job copied the account while A was about to write
	case <-time.After(5 * time.Minute):
		t.Fatal("B neither answered nor staged")
	}
	letA.Do(func() { close(releaseA) })
	aRes := <-aDone
	letB.Do(func() { close(releaseB) })
	if bFirst == nil {
		bFirst = <-bDone
	} else {
		bFirst.expect(http.StatusServiceUnavailable, "server_busy")
		if st := uploadStatus(t, b.upload(map[string]any{"decision": d})); st != "joined" {
			t.Fatalf("B's retry: %s", st)
		}
	}
	r.jobEnded()
	if n := r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"]); n != 1 {
		t.Fatalf("B's decision published %d times: %s", n, bFirst.raw)
	}
	stored := r.e.count("SELECT count(*) FROM push_parts WHERE account_id=? AND install_id=? AND install_seq<0", r.account, r.p.install)
	switch aRes.status {
	case http.StatusOK:
		if st := uploadStatus(t, aRes); st != "part_stored" {
			t.Fatalf("A: %s", st)
		}
		if stored != 1 || requests() != before+1 {
			t.Fatalf("B's publication lost what A was answered: %d parts stored, request count %d (was %d)", stored, requests(), before)
		}
	case http.StatusServiceUnavailable:
		if stored != 0 || requests() != before {
			t.Fatalf("A was answered busy but wrote: %d parts, request count %d (was %d)", stored, requests(), before)
		}
	default:
		t.Fatalf("A: %d %s", aRes.status, aRes.raw)
	}
}

// Another installation opens a recovery while a staged upload job of the
// account is about to copy it. The opening holds the account live across its
// transaction, so the upload is busy meanwhile (or its job's attempt tries
// again) and the copy is taken only after it, never under it: the publication,
// which replaces the account's recovery sessions with the copy's, keeps the
// opened recovery, and its inventory is answered afterwards.
func TestRecoveryOpenRacesStagedJob(t *testing.T) {
	r := newRecoveryEnv(t)
	r.raise()
	r.svc().StagingThreshold = 1
	seed, sc := r.fact(1)
	uploadStatus(t, r.upload(map[string]any{"fact": seed, "receipt": sc}))
	b := &recoveryEnv{r.e, r.e.phoneOf("alice", "alice-password-1"), r.account, newID()}
	openEntered, releaseOpen, openDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	staged, releaseStaged, retried := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var openFirst, stagedFirst atomic.Bool
	var attempts atomic.Int32
	var letOpen, letStaged sync.Once
	t.Cleanup(func() { letOpen.Do(func() { close(releaseOpen) }); letStaged.Do(func() { close(releaseStaged) }) })
	r.svc().SetRecoveryJobHooksForTest(func(point string) error {
		switch {
		case point == "open" && openFirst.CompareAndSwap(false, true):
			close(openEntered) // B's opening holds the account, before its transaction
			<-releaseOpen
		case point == "attempt" && attempts.Add(1) == 2:
			close(retried) // the first attempt found the account held
			<-openDone
		}
		return nil
	}, 5*time.Minute, time.Millisecond)
	r.svc().Registry.RegisterAdmit("test.decision", func(*protocol.ApplyContext, map[string]any) error {
		if stagedFirst.CompareAndSwap(false, true) {
			close(staged) // in the job's staged copy
			<-releaseStaged
		}
		return nil
	})
	opened := make(chan *response, 1)
	go func() {
		opened <- r.e.do("POST", "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": b.rid, "install_id": b.p.install,
			"acked_install_seq": 0}, b.p.cred)
	}()
	<-openEntered
	d := r.decision(1)
	uploaded := make(chan *response, 1)
	go func() { uploaded <- r.upload(map[string]any{"decision": d}) }()
	var first *response
	select {
	case first = <-uploaded: // answered at once: B's opening holds the account
	case <-retried: // a job's attempt found the account held
	case <-staged: // the job copied the account while B's opening was about to write
	case <-time.After(5 * time.Minute):
		t.Fatal("the upload was neither answered nor staged")
	}
	letOpen.Do(func() { close(releaseOpen) })
	(<-opened).must(http.StatusOK)
	close(openDone)
	letStaged.Do(func() { close(releaseStaged) })
	if first != nil {
		first.expect(http.StatusServiceUnavailable, "server_busy")
		if st := uploadStatus(t, r.upload(map[string]any{"decision": d})); st != "joined" {
			t.Fatalf("the upload's retry: %s", st)
		}
	} else {
		<-uploaded
	}
	r.jobEnded()
	if n := r.e.count("SELECT count(*) FROM decisions WHERE account_id=? AND decision_id=?", r.account, d["decision_id"]); n != 1 {
		t.Fatalf("the job's decision published %d times", n)
	}
	if n := r.e.count("SELECT count(*) FROM recoveries WHERE account_id=? AND recovery_id=?", r.account, b.rid); n != 1 {
		t.Fatalf("the publication lost the recovery B opened: %d rows", n)
	}
	f, _ := r.fact(1)
	r.e.do("POST", "/v1/sync/recovery/inventory", map[string]any{"recovery_id": b.rid, "entries": []any{map[string]any{"kind": "fact",
		"op_id": f["op_id"], "content_hash": contentHash(t, f), "position": []int{1, 1, 0}}}}, b.p.cred).must(http.StatusOK)
}
