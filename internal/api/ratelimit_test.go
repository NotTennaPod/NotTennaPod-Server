package api

// Login rate limiting (PROTOCOL §5.5).

import (
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
)

func rateEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	e.createAccount(admin, "bob", "bob-password-1", false)
	e.createAccount(admin, "carol", "carol-password-1", false)
	return e
}

// fail makes one failed sign-in that is verified (401), alternating device and
// web sign-in and the username's case: they all count for the same username.
func (e *env) fail(i int, username string, opts ...opt) {
	e.t.Helper()
	names := []string{username, "ALICE", "Alice"}
	if username != "alice" {
		names = []string{username}
	}
	name := names[i%len(names)]
	var r *response
	if i%2 == 0 {
		r = e.signIn(name, fmt.Sprintf("wrong-password-%d", i), newID(), newID(), opts...)
	} else {
		r = e.webSignIn(name, fmt.Sprintf("wrong-password-%d", i), opts...)
	}
	r.expect(http.StatusUnauthorized, "unauthorized")
}

func TestSixthFailedSignInIsRefusedAndRefusalsDoNotExtendTheLock(t *testing.T) {
	t.Parallel()
	e := rateEnv(t)
	existing := e.token("alice", "alice-password-1", newID(), newID())
	for i := range 5 {
		e.fail(i, "alice")
	}
	// The sixth attempt is refused without verifying, even with the right password.
	r := e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusTooManyRequests, "rate_limited").refused()
	if r.retryAfter() != 1 {
		t.Errorf("retry_after %d after 5 failures, want 2^(5-5) = 1", r.retryAfter())
	}
	if c := e.webSignIn("alice", "alice-password-1").expect(http.StatusTooManyRequests, "rate_limited").cookie(); c != nil {
		t.Error("a refused web sign-in set a cookie")
	}
	// Existing devices keep working while the username is locked.
	e.list(bearer(existing)).must(http.StatusOK)
	// Refused attempts do not count, so many of them do not extend the lock.
	for i := range 10 {
		e.signIn("alice", fmt.Sprintf("guess-%d", i), newID(), newID()).expect(http.StatusTooManyRequests, "rate_limited")
	}
	e.clock.Advance(time.Duration(r.retryAfter()) * time.Second)
	e.token("alice", "alice-password-1", newID(), newID())
}

func TestLockDoublesWithEachFurtherFailure(t *testing.T) {
	t.Parallel()
	e := rateEnv(t)
	for i := range 5 {
		e.fail(i, "alice")
	}
	for i, want := range []int{1, 2, 4, 8, 16} {
		r := e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusTooManyRequests, "rate_limited")
		if r.retryAfter() != want {
			t.Errorf("after %d failures retry_after is %d, want %d", 5+i, r.retryAfter(), want)
		}
		e.clock.Advance(time.Duration(want) * time.Second)
		e.fail(5+i, "alice")
	}
}

func TestFailuresExpireAfterFifteenMinutes(t *testing.T) {
	t.Parallel()
	e := rateEnv(t)
	for i := range 5 {
		e.fail(i, "alice")
	}
	e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusTooManyRequests, "rate_limited")
	e.clock.Advance(15*time.Minute + time.Second)
	// Only failures from the last 15 minutes count: four more are still verified.
	for i := range 4 {
		e.fail(i, "alice")
	}
	e.token("alice", "alice-password-1", newID(), newID())

	// Failures within the window do count.
	for i := range 4 {
		e.fail(i, "alice")
	}
	e.clock.Advance(14*time.Minute + 59*time.Second)
	e.fail(4, "alice")
	e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusTooManyRequests, "rate_limited")
}

func TestSuccessResetsTheCount(t *testing.T) {
	t.Parallel()
	e := rateEnv(t)
	for i := range 4 {
		e.fail(i, "alice")
	}
	e.token("alice", "alice-password-1", newID(), newID())
	for i := range 5 {
		e.fail(i, "alice") // all five are verified again
	}
	e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusTooManyRequests, "rate_limited")
}

// Behind a local proxy every request comes from loopback; locking one username
// must not lock out the household.
func TestLockedAccountDoesNotBlockOthersBehindALoopbackProxy(t *testing.T) {
	t.Parallel()
	e := rateEnv(t)
	proxied := hdr("X-Forwarded-For", "100.64.0.7")
	for i := range 5 {
		e.fail(i, "alice", proxied)
	}
	e.signIn("alice", "alice-password-1", newID(), newID(), proxied).expect(http.StatusTooManyRequests, "rate_limited")
	e.signIn("bob", "bob-password-1", newID(), newID(), proxied).must(http.StatusOK)
	e.webSignIn("carol", "carol-password-1", proxied).must(http.StatusOK)
	e.signIn("alice", "alice-password-1", newID(), newID(), proxied).expect(http.StatusTooManyRequests, "rate_limited")
}

// Non-loopback peers are also counted per IP.
func TestNonLoopbackPeersAreCountedPerIP(t *testing.T) {
	t.Parallel()
	e := rateEnv(t)
	for i, name := range []string{"alice", "bob", "carol", "nobody", "ghost"} {
		e.fail(i, name, from(remotePeer))
	}
	e.signIn("bob", "bob-password-1", newID(), newID(), from(remotePeer)).expect(http.StatusTooManyRequests, "rate_limited")
	e.signIn("bob", "bob-password-1", newID(), newID(), from(otherPeer)).must(http.StatusOK)
	e.signIn("carol", "carol-password-1", newID(), newID()).must(http.StatusOK)
	// A success from the IP resets its count.
	e.clock.Advance(time.Second)
	e.signIn("alice", "alice-password-1", newID(), newID(), from(remotePeer)).must(http.StatusOK)
	for i, name := range []string{"x1", "x2", "x3", "x4"} {
		e.fail(i, name, from(remotePeer))
	}
	e.signIn("bob", "bob-password-1", newID(), newID(), from(remotePeer)).must(http.StatusOK)
}

// Unknown and known usernames cost the same Argon2id verification and get the
// same answer (median within 20%, real parameters). Timing on a shared
// machine is noisy, so up to three rounds of five samples each are taken; a real
// difference (an unknown username skipping Argon2id) fails every round.
func TestUnknownAndKnownUsernamesAreIndistinguishable(t *testing.T) {
	e := newEnv(t, withHasher(auth.NewHasher(auth.DefaultParams)))
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	e.signIn("warmup", "warmup-password", newID(), newID()).expect(http.StatusUnauthorized, "unauthorized")
	message := ""
	measure := func(username string, i int) time.Duration {
		runtime.GC() // collect the previous verification's 64 MiB outside the measurement
		start := time.Now()
		r := e.signIn(username, fmt.Sprintf("wrong-password-%d", i), newID(), newID())
		d := time.Since(start)
		r.expect(http.StatusUnauthorized, "unauthorized")
		if message == "" {
			message = r.str("message")
		} else if r.str("message") != message {
			t.Errorf("%s: message %q differs from %q", username, r.str("message"), message)
		}
		return d
	}
	median := func(d []time.Duration) time.Duration { s := slices.Clone(d); slices.Sort(s); return s[len(s)/2] }
	var rounds []string
	for range 3 {
		var known, unknown []time.Duration
		for i := range 5 { // five failures each: within the rate limit
			if i%2 == 0 {
				known = append(known, measure("alice", i))
				unknown = append(unknown, measure("nobody", i))
			} else {
				unknown = append(unknown, measure("nobody", i))
				known = append(known, measure("alice", i))
			}
		}
		mk, mu := median(known), median(unknown)
		t.Logf("median known %v, unknown %v", mk, mu)
		if lo, hi := min(mk, mu), max(mk, mu); float64(hi-lo) <= 0.2*float64(lo) {
			return
		}
		rounds = append(rounds, fmt.Sprintf("known %v / unknown %v", mk, mu))
		e.clock.Advance(16 * time.Minute) // the failures expire before the next round
	}
	t.Errorf("known and unknown username medians differ by more than 20%% in every round: %v", rounds)
}

// Argon2id runs at most two at a time with a small queue; the excess gets 503
// server_busy with retry_after, and the server recovers.
func TestBusyServerAnswersServerBusy(t *testing.T) {
	t.Parallel()
	e := newEnv(t, withHasher(auth.NewHasher(auth.Params{Memory: 4 * 1024, Time: 8, Threads: 1})))
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	const n = 32
	results := make([]*response, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = e.signIn("alice", "alice-password-1", newID(), newID())
		}()
	}
	close(start)
	wg.Wait()
	ok, busy := 0, 0
	for _, r := range results {
		switch {
		case r.status == http.StatusOK:
			ok++
		case r.status == http.StatusServiceUnavailable && r.str("error") == "server_busy":
			busy++
		default:
			t.Errorf("concurrent sign-in: %d %s", r.status, r.raw)
		}
	}
	t.Logf("%d signed in, %d told the server is busy", ok, busy)
	if busy == 0 || ok < 2 {
		t.Errorf("%d of %d concurrent sign-ins were refused as busy and %d succeeded; want some of each", busy, n, ok)
	}
	e.token("alice", "alice-password-1", newID(), newID())
}
