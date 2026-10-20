package api

// Web sessions in a cookie of their own each (PROTOCOL §5.2, §5.4, §16 row
// 6a): a browser's answers that arrive late, after a newer session cookie or
// after signing out, never sign it out or back in. A cookieJar applies each
// response's Set-Cookie headers when the test delivers the response, so a
// test chooses the order a browser receives its answers in.

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
)

// cookieJar is a browser profile's cookies for the server: each name holds
// one value; a cookie set with a non-positive Max-Age is removed.
type cookieJar struct {
	names  []string // in the order first set
	values map[string]string
}

func newJar() *cookieJar { return &cookieJar{values: map[string]string{}} }

// apply delivers a response's Set-Cookie headers, in their order.
func (j *cookieJar) apply(r *response) *response {
	for _, c := range r.cookies {
		if c.MaxAge < 0 {
			delete(j.values, c.Name)
			j.names = slices.DeleteFunc(j.names, func(n string) bool { return n == c.Name })
			continue
		}
		if _, ok := j.values[c.Name]; !ok {
			j.names = append(j.names, c.Name)
		}
		j.values[c.Name] = c.Value
	}
	return r
}

// opt sends every cookie of the jar, in the order first set, with our Origin.
func (j *cookieJar) opt() opt {
	return func(r *http.Request) {
		for _, n := range j.names {
			r.AddCookie(&http.Cookie{Name: n, Value: j.values[n]})
		}
		r.Header.Set("Origin", testOrigin)
	}
}

// sessions lists the session values the jar holds.
func (j *cookieJar) sessions() []string {
	var out []string
	for _, n := range j.names {
		out = append(out, j.values[n])
	}
	slices.Sort(out)
	return out
}

// holdsOnly reports whether the jar holds exactly these sessions.
func (j *cookieJar) holdsOnly(sessions ...string) bool {
	want := slices.Clone(sessions)
	slices.Sort(want)
	return slices.Equal(j.sessions(), want)
}

// cookies sends exactly these name=value pairs, in this order, with our Origin.
func cookies(pairs ...*http.Cookie) opt {
	return func(r *http.Request) {
		for _, c := range pairs {
			r.AddCookie(c)
		}
		r.Header.Set("Origin", testOrigin)
	}
}

// issued returns the session cookie a response sets, failing without one.
func issued(t *testing.T, r *response) *http.Cookie {
	t.Helper()
	c := r.cookie()
	if c == nil {
		t.Fatalf("%s set no session cookie: %v", r.what, r.cookies)
	}
	return c
}

// clearedNames lists the cookie names a response clears.
func clearedNames(r *response) []string {
	var out []string
	for _, c := range r.cookies {
		if c.MaxAge < 0 {
			out = append(out, c.Name)
		}
	}
	slices.Sort(out)
	return out
}

func registration(installID, nonce string) map[string]any {
	return map[string]any{"install_id": installID, "instance_nonce": nonce, "name": "Firefox", "app_version": "1.0", "capabilities": []string{"queue"}}
}

func successorBody(installID, nonce string) map[string]any {
	return map[string]any{"install_id": installID, "instance_nonce": nonce, "device": device("successor")}
}

// sessionInstall asks /v1/session which installation the browser acts as.
func (e *env) sessionInstall(o opt) (*response, string) {
	e.t.Helper()
	r := e.do(http.MethodGet, "/v1/session", nil, o)
	id, _ := r.body["install_id"].(string)
	return r, id
}

// aliceEnv is a server with the administrator and alice.
func aliceEnv(t *testing.T) (*env, string) {
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	return e, admin
}

// signedInProfile signs alice in on a fresh browser profile and registers its
// installation T.
func signedInProfile(t *testing.T, e *env) (j *cookieJar, session, install, nonce string) {
	t.Helper()
	j = newJar()
	session = issued(t, j.apply(e.webSignIn("alice", "alice-password-1").must(http.StatusOK))).Value
	install, nonce = newID(), newID()
	e.do(http.MethodPost, "/v1/installations", registration(install, nonce), j.opt()).must(http.StatusOK)
	return j, session, install, nonce
}

// A renewal answer that arrives after the handoff, before or after the
// successor's first use, does not take the browser back to its predecessor
// (it used to: 409 installation_superseded, or signed out once the
// predecessor's session was revoked).
func TestLateRenewalAfterAHandoffKeepsTheSuccessor(t *testing.T) {
	t.Parallel()
	for _, used := range []bool{false, true} {
		t.Run(fmt.Sprintf("successor used before: %v", used), func(t *testing.T) {
			t.Parallel()
			e, _ := aliceEnv(t)
			j, s, _, nonce := signedInProfile(t, e)
			e.clock.Advance(2 * time.Hour)
			late := e.list(j.opt()).must(http.StatusOK) // renews S; its answer is late
			if c := late.cookie(); c == nil || c.Value != s {
				t.Fatalf("the request did not renew the session: %v", late.cookies)
			}
			y := newID()
			next := issued(t, j.apply(e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK))).Value
			if used {
				j.apply(e.list(j.opt()).must(http.StatusOK))
			}
			j.apply(late)
			for i := range 2 {
				r, id := e.sessionInstall(j.opt())
				j.apply(r.must(http.StatusOK))
				if id != y {
					t.Fatalf("request %d acts as %q, want the successor %s", i+1, id, y)
				}
			}
			if !j.holdsOnly(next) {
				t.Errorf("the browser still holds the predecessor's revoked session")
			}
		})
	}
}

// A renewal answer that arrives after signing out and in again does not sign
// the browser out.
func TestLateRenewalAfterSignOutAndSignInKeepsTheNewSession(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	j, s, _, _ := signedInProfile(t, e)
	e.clock.Advance(2 * time.Hour)
	late := e.list(j.opt()).must(http.StatusOK)
	j.apply(e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, j.opt()).must(http.StatusOK))
	if len(j.sessions()) != 0 {
		t.Fatal("sign-out left a session cookie")
	}
	s2 := issued(t, j.apply(e.webSignIn("alice", "alice-password-1").must(http.StatusOK))).Value
	j.apply(late)
	r := j.apply(e.list(j.opt()).must(http.StatusOK))
	if got := clearedNames(r); len(got) != 1 {
		t.Errorf("the signed-out session's cookie was not cleared: %v", r.cookies)
	}
	if !j.holdsOnly(s2) || slices.Contains(j.sessions(), s) {
		t.Errorf("the browser holds %d sessions, want only the new one", len(j.sessions()))
	}
}

// A first successor answer that arrives after another tab's repeat (which
// replaced its session) does not sign the browser out.
func TestLateFirstSuccessorAnswerAfterARepeat(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	j, _, _, nonce := signedInProfile(t, e)
	y := newID()
	first := e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK)
	second := issued(t, j.apply(e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK))).Value
	j.apply(first)
	for i := range 2 {
		r, id := e.sessionInstall(j.opt())
		j.apply(r.must(http.StatusOK))
		if id != y {
			t.Fatalf("request %d acts as %q, want the successor %s", i+1, id, y)
		}
	}
	if !j.holdsOnly(second) {
		t.Errorf("the browser holds stale sessions after the late answer")
	}
}

// A successor answer that arrives after signing out signs nothing back in;
// signing in again on that profile then registers its old installation and
// the plan's repeat completes the handoff (it used to answer 409
// installation_superseded, a dead end).
func TestSuccessorAnswerAfterSignOutAndTheWayBack(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	j, _, x, nonce := signedInProfile(t, e)
	y := newID()
	late := e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK)
	j.apply(e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, j.opt()).must(http.StatusOK))
	j.apply(late)
	r, _ := e.sessionInstall(j.opt())
	j.apply(r.expect(http.StatusUnauthorized, "unauthorized"))
	if len(j.sessions()) != 0 {
		t.Fatal("the cancelled successor's cookie was not cleared")
	}

	r2 := issued(t, j.apply(e.webSignIn("alice", "alice-password-1").must(http.StatusOK))).Value
	e.do(http.MethodPost, "/v1/installations", registration(x, nonce), j.opt()).must(http.StatusOK)
	e.list(j.opt()).expect(http.StatusConflict, "installation_superseded")
	next := issued(t, j.apply(e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK))).Value
	for i := range 2 {
		r, id := e.sessionInstall(j.opt())
		j.apply(r.must(http.StatusOK))
		if id != y {
			t.Fatalf("request %d acts as %q, want the successor %s", i+1, id, y)
		}
	}
	if !j.holdsOnly(next) {
		t.Errorf("after the handoff the browser holds stale sessions")
	}
	e.list(browser(r2)).expect(http.StatusUnauthorized, "unauthorized") // revoked by the successor's first use
}

// setCreated sets a session's issue time.
func (e *env) setCreated(session string, at int64) {
	e.t.Helper()
	e.writeDB(`UPDATE auth_tokens SET created_at = ? WHERE token_hash = ?`, at, auth.HashCredential(session))
}

// The choice among several sessions is a function of the set: A (bound to T,
// issued last) is dropped because B's installation descends from T, and the
// unbound U, issued after B, wins in every order. A
// successor and its predecessor give the successor in both orders.
func TestSessionChoiceIgnoresCookieOrder(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	j, a, _, _ := signedInProfile(t, e)
	aCookie := &http.Cookie{Name: j.names[0], Value: a}
	// B: a copy's session, bound to Y (created from T with another nonce).
	y := newID()
	b := issued(t, e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, newID()), browser(a)).must(http.StatusOK))
	u := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK))
	base := e.clock.Now().UnixMilli()
	e.setCreated(a, base+3)
	e.setCreated(b.Value, base+1)
	e.setCreated(u.Value, base+2)
	set := []*http.Cookie{aCookie, {Name: b.Name, Value: b.Value}, {Name: u.Name, Value: u.Value}}
	orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	for _, o := range orders {
		r := e.do(http.MethodGet, "/v1/session", nil, cookies(set[o[0]], set[o[1]], set[o[2]])).must(http.StatusOK)
		if r.body["install_id"] != nil {
			t.Errorf("order %v: chose the session bound to %v, want the unbound U", o, r.body["install_id"])
		}
	}

	// Each order in a server of its own: the successor's first use revokes
	// the predecessor.
	for _, predFirst := range []bool{true, false} {
		e2, _ := aliceEnv(t)
		j2, s, _, nonce := signedInProfile(t, e2)
		pred := &http.Cookie{Name: j2.names[0], Value: s}
		z := newID()
		succ := issued(t, e2.do(http.MethodPost, "/v1/installations/successor", successorBody(z, nonce), j2.opt()).must(http.StatusOK))
		e2.setCreated(s, e2.clock.Now().UnixMilli()+10) // issued last, still dropped
		order := []*http.Cookie{succ, pred}
		if predFirst {
			order = []*http.Cookie{pred, succ}
		}
		if _, id := e2.sessionInstall(cookies(order...)); id != z {
			t.Errorf("predecessor first %v: acts as %q, want the successor", predFirst, id)
		}
	}
}

// Every session cookie counts, however many: ten dead ones are cleared, the
// live successor is chosen wherever it comes, and its usable superseded
// predecessor is neither chosen nor cleared.
func TestManySessionCookies(t *testing.T) {
	t.Parallel()
	for _, at := range []string{"first", "middle", "last"} {
		t.Run(at, func(t *testing.T) {
			t.Parallel()
			e, _ := aliceEnv(t)
			var dead []*http.Cookie
			for range 10 {
				c := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK))
				e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(c.Value)).must(http.StatusOK)
				dead = append(dead, &http.Cookie{Name: c.Name, Value: c.Value})
			}
			j, s, _, nonce := signedInProfile(t, e)
			pred := &http.Cookie{Name: j.names[0], Value: s}
			y := newID()
			live := issued(t, e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK))
			list := append(slices.Clone(dead[:5]), pred)
			list = append(list, dead[5:]...)
			switch at {
			case "first":
				list = append([]*http.Cookie{live}, list...)
			case "middle":
				list = slices.Insert(list, 3, live)
			case "last":
				list = append(list, live)
			}
			r, id := e.sessionInstall(cookies(list...))
			r.must(http.StatusOK)
			if id != y {
				t.Fatalf("acts as %q, want the live successor %s", id, y)
			}
			var want []string
			for _, c := range dead {
				want = append(want, c.Name)
			}
			slices.Sort(want)
			if got := clearedNames(r); !slices.Equal(got, want) {
				t.Errorf("cleared %v, want exactly the ten dead sessions %v", got, want)
			}
		})
	}
}

// A session issued by an earlier server, in ntp_session, keeps working and is
// renewed under that name.
func TestLegacySessionCookieKeepsWorking(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	s := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK)).Value
	legacy := cookies(&http.Cookie{Name: "ntp_session", Value: s})
	e.list(legacy).must(http.StatusOK)
	e.clock.Advance(2 * time.Hour)
	r := e.list(legacy).must(http.StatusOK)
	if len(r.cookies) != 1 || r.cookies[0].Name != "ntp_session" || r.cookies[0].Value != s || r.cookies[0].MaxAge != int(sessionLifetime/time.Second) {
		t.Errorf("renewal of a legacy session set %v, want ntp_session for 30 days", r.cookies)
	}
	out := e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, legacy).must(http.StatusOK)
	if got := clearedNames(out); !slices.Equal(got, []string{"ntp_session"}) {
		t.Errorf("sign-out cleared %v", got)
	}
	e.list(legacy).expect(http.StatusUnauthorized, "unauthorized")
}

// With X-NotTennaPod-Account only that account's sessions are considered.
func TestAccountHeaderChoosesAmongSessions(t *testing.T) {
	t.Parallel()
	e, admin := aliceEnv(t)
	bobID := e.createAccount(admin, "bob", "bob-password-1", false)
	alice := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK))
	bob := issued(t, e.webSignIn("bob", "bob-password-1").must(http.StatusOK))
	e.setCreated(alice.Value, e.clock.Now().UnixMilli()-1000)
	both := []*http.Cookie{{Name: alice.Name, Value: alice.Value}, {Name: bob.Name, Value: bob.Value}}
	account := func(r *response) string { id, _ := r.obj("account")["account_id"].(string); return id }
	aliceID := account(e.do(http.MethodGet, "/v1/session", nil, cookies(both[0])).must(http.StatusOK))
	if got := account(e.do(http.MethodGet, "/v1/session", nil, cookies(both...)).must(http.StatusOK)); got != bobID {
		t.Errorf("without the header: account %s, want the latest session's (bob)", got)
	}
	if got := account(e.do(http.MethodGet, "/v1/session", nil, cookies(both...), hdr("X-NotTennaPod-Account", aliceID)).must(http.StatusOK)); got != aliceID {
		t.Errorf("with alice's header: account %s, want alice", got)
	}
	e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, cookies(both[0])).must(http.StatusOK)
	e.do(http.MethodGet, "/v1/session", nil, cookies(both...), hdr("X-NotTennaPod-Account", aliceID)).expect(http.StatusConflict, "account_changed")
}

// A session cookie whose name is not its value's is ignored, never cleared.
func TestSessionCookieNameMustMatchItsValue(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	s := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK))
	other := issued(t, e.webSignIn("alice", "alice-password-1").must(http.StatusOK))
	w := newID()
	e.registerWeb(other.Value, w, newID()).must(http.StatusOK)
	e.setCreated(s.Value, e.clock.Now().UnixMilli()+1000) // would be chosen if it counted
	wrong := &http.Cookie{Name: other.Name, Value: s.Value}
	if wrong.Name == s.Name {
		t.Fatal("two sessions share a cookie name")
	}
	r := e.do(http.MethodGet, "/v1/installations", nil, cookies(wrong)).expect(http.StatusUnauthorized, "unauthorized")
	if len(r.cookies) != 0 {
		t.Errorf("a mismatched cookie was answered with %v", r.cookies)
	}
	misnamed := &http.Cookie{Name: "ntp_s_0123456789abcdef", Value: s.Value}
	r, id := e.sessionInstall(cookies(misnamed, &http.Cookie{Name: other.Name + "x", Value: other.Value},
		&http.Cookie{Name: other.Name, Value: other.Value}))
	r.must(http.StatusOK)
	if id != w {
		t.Errorf("acts as %q, want the one properly named session's %s", id, w)
	}
	if len(r.cookies) != 0 {
		t.Errorf("mismatched cookies were answered with %v", r.cookies)
	}
	e.do(http.MethodGet, "/v1/session", nil, cookies(misnamed)).expect(http.StatusUnauthorized, "unauthorized")
}

// A copied profile holding its own session: its sign-out clears the
// original's cookie there without deleting the original's session (the late
// renewal into a copy is a documented limitation, §5.4).
func TestCopiedProfileSignOutClearsWithoutDeletingTheOriginal(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	original, s, x, _ := signedInProfile(t, e)
	copied := newJar()
	for _, n := range original.names {
		copied.names = append(copied.names, n)
		copied.values[n] = original.values[n]
	}
	y := newID()
	c := issued(t, copied.apply(e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, newID()), copied.opt()).must(http.StatusOK))).Value
	if _, id := e.sessionInstall(copied.opt()); id != y {
		t.Fatalf("the copy acts as %q, want its own %s", id, y)
	}
	out := copied.apply(e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, copied.opt()).must(http.StatusOK))
	if len(clearedNames(out)) != 2 || len(copied.sessions()) != 0 {
		t.Errorf("the copy's sign-out cleared %v", clearedNames(out))
	}
	if _, id := e.sessionInstall(original.opt()); id != x {
		t.Errorf("the original acts as %q, want %s", id, x)
	}
	if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE token_hash = ?`, auth.HashCredential(s)) != 1 {
		t.Error("the copy's sign-out deleted the original's session")
	}
	e.list(browser(c)).expect(http.StatusUnauthorized, "unauthorized")
}

// The successor was used by another tab before the profile renumbered, and
// its cookie then expired in the browser, which stops sending it: signing in
// again still binds the old installation (the expired successor session is
// no session left) and arms its retirement again, and the plan's repeat
// completes the handoff.
func TestRecoveryBindAfterTheSuccessorsSessionExpired(t *testing.T) {
	t.Parallel()
	e, _ := aliceEnv(t)
	j, _, x, nonce := signedInProfile(t, e)
	y := newID()
	next := issued(t, e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK)).Value
	e.list(browser(next)).must(http.StatusOK) // another tab's first use; this profile never renumbers
	e.clock.Advance(31 * 24 * time.Hour)
	if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE install_id = ? AND kind = 'web'`, y) != 1 {
		t.Fatal("the successor's expired session is not stored")
	}

	issued(t, j.apply(e.webSignIn("alice", "alice-password-1").must(http.StatusOK)))
	j.apply(e.do(http.MethodPost, "/v1/installations", registration(x, nonce), j.opt()).must(http.StatusOK))
	if e.count(`SELECT COUNT(*) FROM installations WHERE install_id = ? AND pending_predecessor = ?`, y, x) != 1 {
		t.Error("the bind did not arm the successor's retirement of the old installation")
	}
	if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE install_id = ?`, y) != 0 {
		t.Error("the successor's expired session is still stored")
	}
	j.apply(e.do(http.MethodPost, "/v1/installations/successor", successorBody(y, nonce), j.opt()).must(http.StatusOK))
	r, id := e.sessionInstall(j.opt())
	j.apply(r.must(http.StatusOK))
	if id != y {
		t.Errorf("after the repeat the browser acts as %q, want %s", id, y)
	}
}
