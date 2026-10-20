package api

// Web sessions against requests in flight (PROTOCOL §5.4): a sign-out that a
// successor's first use overtakes, a repeated handoff paused across a second
// sign-out, expiry against renewal, and a database error.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
)

// withContext serves the request under ctx.
func withContext(ctx context.Context) opt {
	return func(r *http.Request) { *r = *r.WithContext(ctx) }
}

func (e *env) storedExpiry(session string) int64 {
	e.t.Helper()
	db := e.count(`SELECT COUNT(*) FROM auth_tokens WHERE token_hash = ?`, auth.HashCredential(session))
	if db != 1 {
		e.t.Fatalf("the session is not stored (%d rows)", db)
	}
	return int64(e.count(`SELECT expires_at FROM auth_tokens WHERE token_hash = ?`, auth.HashCredential(session)))
}

// A successor's first use lands between a sign-out's authentication and its
// write: the sign-out still ends the successor's session,
// and the browser stays signed out; a copy's and a phone's credentials are
// untouched. Signing in again binds the old installation, and because the
// successor was used, its next use revokes that session again:
// a repeat already authenticated with it, paused across the handoff and a
// second sign-out, issues nothing.
func TestSignOutOvertakenByFirstUseAndTheWayBack(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	j, s, x, nonce := signedInProfile(t, e)
	phone := e.token("alice", "alice-password-1", newID(), newID())
	copyInstall := newID()
	copySession := issued(t, e.successor(browser(s), copyInstall, newID()).must(http.StatusOK)).Value
	y := newID()
	late := e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK)
	next := issued(t, late).Value

	// Another tab signs out with the old session; the successor is used first.
	e.beforeSignOut.set(func() { e.list(browser(next)).must(http.StatusOK) })
	j.apply(e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, j.opt()).must(http.StatusOK))
	if e.beforeSignOut.pending() {
		t.Fatal("the hook did not run")
	}
	j.apply(late)
	r, _ := e.sessionInstall(j.opt())
	j.apply(r.expect(http.StatusUnauthorized, "unauthorized"))
	if len(j.sessions()) != 0 {
		t.Fatal("the browser still holds a session after signing out")
	}
	if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE install_id = ?`, y) != 0 {
		t.Error("the successor kept a session")
	}
	if e.current(browser(copySession)) != copyInstall {
		t.Error("the sign-out ended a copy's session")
	}
	e.list(bearer(phone)).must(http.StatusOK)

	// Signing in again: the old installation binds, and retirement is armed again.
	bound := issued(t, j.apply(e.webSignIn("alice", "alice-password-1").must(http.StatusOK))).Value
	e.do(http.MethodPost, "/v1/installations", registration(x, nonce), j.opt()).must(http.StatusOK)
	if e.count(`SELECT COUNT(*) FROM installations WHERE install_id = ? AND pending_predecessor = ?`, y, x) != 1 {
		t.Error("the bind did not arm the successor's retirement of the old installation")
	}
	e.beforeIssue.set(func() {
		// Meanwhile the plan's repeat completes the handoff, the successor
		// is used, and the browser signs out again.
		j.apply(e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK))
		r, id := e.sessionInstall(j.opt())
		j.apply(r.must(http.StatusOK))
		if id != y {
			t.Errorf("after the repeat the browser acts as %q, want %s", id, y)
		}
		j.apply(e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, j.opt()).must(http.StatusOK))
	})
	paused := e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), cookies(&http.Cookie{Name: sessionCookieOf(bound).Name, Value: bound}))
	if e.beforeIssue.pending() {
		t.Fatal("the hook did not run")
	}
	paused.expect(http.StatusUnauthorized, "unauthorized").refused()
	j.apply(paused)
	if c := paused.cookie(); c != nil {
		t.Fatal("the paused repeat issued a session")
	}
	r, _ = e.sessionInstall(j.opt())
	r.expect(http.StatusUnauthorized, "unauthorized")
	if n := e.count(`SELECT COUNT(*) FROM auth_tokens WHERE install_id IN (?, ?) AND kind = 'web'`, x, y); n != 0 {
		t.Errorf("%d web sessions remain on the profile's installations", n)
	}
}

// Expiry is decided in the write transaction, by its clock, against a
// renewal: a renewal that wins keeps the session usable and
// uncleared; an expiry that wins deletes it, and the renewal then extends
// nothing and answers 401; a renewal whose own write comes after the expiry
// extends nothing either.
func TestExpiryAgainstAnInFlightRenewal(t *testing.T) {
	t.Parallel()
	month := 30 * 24 * time.Hour
	t.Run("renewal wins", func(t *testing.T) {
		t.Parallel()
		e, _ := aliceEnv(t)
		s := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK)).Value
		e.clock.Advance(month + time.Minute) // the late request reads the session as expired
		e.afterSessionRead.set(func() {
			e.clock.Advance(-2 * time.Minute)
			r := e.list(browser(s)).must(http.StatusOK)
			if c := r.cookie(); c == nil || c.Value != s {
				t.Errorf("the renewal set %v", r.cookies)
			}
			e.clock.Advance(2 * time.Minute)
		})
		r := e.list(browser(s)).must(http.StatusOK)
		if e.afterSessionRead.pending() {
			t.Fatal("the hook did not run")
		}
		if got := clearedNames(r); len(got) != 0 {
			t.Errorf("a renewed session's cookie was cleared: %v", got)
		}
		e.storedExpiry(s)
	})
	t.Run("expiry wins", func(t *testing.T) {
		t.Parallel()
		e, _ := aliceEnv(t)
		s := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK)).Value
		e.clock.Advance(month - time.Minute) // a renewal is due
		e.afterSessionRead.set(func() {
			e.clock.Advance(2 * time.Minute)
			r := e.list(browser(s)).expect(http.StatusUnauthorized, "unauthorized")
			if got := clearedNames(r); len(got) != 1 {
				t.Errorf("the expired session's cookie was not cleared: %v", r.cookies)
			}
		})
		r := e.list(browser(s)).expect(http.StatusUnauthorized, "unauthorized")
		if e.afterSessionRead.pending() {
			t.Fatal("the hook did not run")
		}
		if got := clearedNames(r); len(got) != 1 || r.cookie() != nil {
			t.Errorf("the renewal answered %v, want only the cookie cleared", r.cookies)
		}
		if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE token_hash = ?`, auth.HashCredential(s)) != 0 {
			t.Error("the expired session is still stored")
		}
	})
	t.Run("the renewal's write after expiry", func(t *testing.T) {
		t.Parallel()
		e, _ := aliceEnv(t)
		s := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK)).Value
		e.clock.Advance(month - time.Minute)
		e.afterSessionRead.set(func() { e.clock.Advance(2 * time.Minute) })
		r := e.list(browser(s)).expect(http.StatusUnauthorized, "unauthorized")
		if got := clearedNames(r); len(got) != 1 || r.cookie() != nil {
			t.Errorf("the late renewal answered %v, want only the cookie cleared", r.cookies)
		}
		if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE token_hash = ?`, auth.HashCredential(s)) != 0 {
			t.Error("the expired session is still stored")
		}
	})
}

// A renewal whose clock stepped back behind an earlier renewal does not
// shorten the stored expiry.
func TestRenewalNeverShortensTheExpiry(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	s := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK)).Value
	e.clock.Advance(3 * time.Hour)
	var want int64
	e.afterSessionRead.set(func() {
		e.list(browser(s)).must(http.StatusOK) // renews to now + 30 days
		want = e.storedExpiry(s)
		e.clock.Advance(-90 * time.Minute)
	})
	e.list(browser(s)).must(http.StatusOK)
	if e.afterSessionRead.pending() {
		t.Fatal("the hook did not run")
	}
	if got := e.storedExpiry(s); got != want {
		t.Errorf("the expiry moved from %d to %d", want, got)
	}
}

// A database error while deciding which sessions are gone refuses the request
// and clears no cookie, not even a signed-out session's.
func TestDatabaseErrorClearsNoCookie(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	dead := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK))
	e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(dead.Value)).must(http.StatusOK)
	live := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK))
	both := cookies(&http.Cookie{Name: dead.Name, Value: dead.Value}, &http.Cookie{Name: live.Name, Value: live.Value})
	ctx, cancel := context.WithCancel(context.Background())
	e.afterSessionRead.set(cancel)
	r := e.do(http.MethodGet, "/v1/installations", nil, both, withContext(ctx)).expect(http.StatusServiceUnavailable, "server_busy")
	if len(r.cookies) != 0 {
		t.Errorf("a failed request set cookies: %v", r.cookies)
	}
	r = e.do(http.MethodGet, "/v1/installations", nil, both).must(http.StatusOK)
	if got := clearedNames(r); len(got) != 1 || got[0] != dead.Name {
		t.Errorf("cleared %v, want the signed-out session's cookie", got)
	}
}
