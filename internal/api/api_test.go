package api

// Acceptance tests for accounts and authentication (PROTOCOL §5) and the rules
// they rely on.

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
)

// ---- 1. Setup

func TestInfoReportsSetupState(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r := e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK)
	serverID := r.str("server_id")
	if len(serverID) != 36 {
		t.Errorf("server_id %q is not a UUID", serverID)
	}
	if p := r.obj("protocol"); p["min"] != float64(1) || p["max"] != float64(1) {
		t.Errorf("protocol %v, want {min: 1, max: 1}", p)
	}
	if r.body["setup_required"] != true {
		t.Errorf("setup_required %v before setup, want true", r.body["setup_required"])
	}
	if r.obj("bounds") == nil {
		t.Errorf("bounds missing: %s", r.raw)
	}
	e.setupAdmin("owner", "owner-password-1")
	r = e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK)
	if r.body["setup_required"] != false {
		t.Errorf("setup_required %v after setup, want false", r.body["setup_required"])
	}
	if r.str("server_id") != serverID {
		t.Errorf("server_id changed")
	}
}

// 32 concurrent setups produce exactly one administrator.
func TestConcurrentSetupsCreateExactlyOneAdministrator(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ts := httptest.NewServer(e.srv)
	defer ts.Close()
	code := e.setup.Code()
	const n = 32
	results := make([]*response, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = e.netDo(ts.Client(), ts.URL, http.MethodPost, "/v1/setup",
				map[string]any{"username": fmt.Sprintf("owner%02d", i), "password": "owner-password", "setup_code": code},
				map[string]string{"Origin": ts.URL})
		}()
	}
	close(start)
	wg.Wait()
	winner, winnerSession := -1, ""
	for i, r := range results {
		switch {
		case r.status == http.StatusCreated:
			if winner >= 0 {
				t.Errorf("setups %d and %d both succeeded", winner, i)
			}
			winner = i
			if c := r.cookie(); c != nil {
				winnerSession = c.Value
			} else {
				t.Errorf("the successful setup did not sign the browser in")
			}
		case r.status == http.StatusConflict && r.str("error") == "setup_complete":
		case r.status == http.StatusServiceUnavailable && r.str("error") == "server_busy":
		default:
			t.Errorf("setup %d: got %d %s, want 201, 409 setup_complete or 503 server_busy", i, r.status, r.raw)
		}
	}
	if winner < 0 {
		t.Fatal("no setup succeeded")
	}
	accounts := e.accounts(winnerSession)
	if len(accounts) != 1 {
		t.Errorf("%d accounts after concurrent setup, want 1: %v", len(accounts), accounts)
	}
	if a := accounts[fmt.Sprintf("owner%02d", winner)]; a == nil || a["is_admin"] != true {
		t.Errorf("the winner is not the administrator: %v", accounts)
	}
	if n := e.count(`SELECT COUNT(*) FROM account_logins WHERE is_admin = 1`); n != 1 {
		t.Errorf("%d administrators stored, want 1", n)
	}
	if e.fileExists("setup-code") {
		t.Error("the setup-code file still exists after setup")
	}
	if e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK).body["setup_required"] != false {
		t.Error("setup still required")
	}
	// Later setups, even with the old code, get 409.
	e.do(http.MethodPost, "/v1/setup", map[string]any{"username": "late", "password": "late-password", "setup_code": code},
		withOrigin(testOrigin)).expect(http.StatusConflict, "setup_complete")
}

// The code is required (even from loopback), wrong codes change
// nothing and count as failed sign-ins; the code file is private and removed.
func TestSetupRequiresTheSetupCode(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	code := e.setup.Code()
	if len(code) != 10 || strings.Trim(code, auth.SetupCodeAlphabet) != "" {
		t.Errorf("setup code %q is not 10 characters from the unambiguous alphabet", code)
	}
	path := filepath.Join(e.dir, "setup-code")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("setup-code file: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("setup-code file mode %v, want 0600", fi.Mode().Perm())
	}
	if b, _ := os.ReadFile(path); strings.TrimSpace(string(b)) != code {
		t.Errorf("setup-code file holds %q, want the code", b)
	}
	body := func(code string) map[string]any {
		return map[string]any{"username": "owner", "password": "owner-password-1", "setup_code": code}
	}
	// Missing code from loopback (a local proxy makes everyone look local).
	e.do(http.MethodPost, "/v1/setup", map[string]any{"username": "owner", "password": "owner-password-1"},
		withOrigin(testOrigin)).expect(http.StatusForbidden, "forbidden")
	// Wrong code from a tailnet peer.
	e.do(http.MethodPost, "/v1/setup", body("WRONGCODE2"), withOrigin(testOrigin), from(remotePeer)).expect(http.StatusForbidden, "forbidden")
	// Wrong code from loopback.
	e.do(http.MethodPost, "/v1/setup", body("ABCDEFGHJK"), withOrigin(testOrigin)).expect(http.StatusForbidden, "forbidden")
	if e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK).body["setup_required"] != true {
		t.Error("a refused setup completed setup")
	}
	if e.count(`SELECT COUNT(*) FROM accounts`) != 0 {
		t.Error("a refused setup created an account")
	}
	if !e.fileExists("setup-code") {
		t.Error("a refused setup removed the setup-code file")
	}
	// Wrong codes count as failed sign-ins: after five, even the right code is refused for a while.
	e.do(http.MethodPost, "/v1/setup", body("ABCDEFGHJM"), withOrigin(testOrigin)).expect(http.StatusForbidden, "forbidden")
	e.do(http.MethodPost, "/v1/setup", body("ABCDEFGHJN"), withOrigin(testOrigin)).expect(http.StatusForbidden, "forbidden")
	r := e.do(http.MethodPost, "/v1/setup", body(code), withOrigin(testOrigin)).expect(http.StatusTooManyRequests, "rate_limited")
	if r.retryAfter() < 1 {
		t.Fatalf("rate-limited setup without retry_after")
	}
	e.clock.Advance(time.Duration(r.retryAfter()) * time.Second)
	e.do(http.MethodPost, "/v1/setup", body(code), withOrigin(testOrigin)).must(http.StatusCreated)
	if e.fileExists("setup-code") {
		t.Error("the setup-code file still exists after setup")
	}
	if e.setup.Code() != "" {
		t.Error("the setup code is still accepted after setup")
	}
}

// ---- 2. Secrets at rest

func TestSecretsAreNeverStored(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	setupCode := e.setup.Code()
	admin, _ := e.setupAdmin("root", "root-Secret-Pw-1")
	aliceID := e.createAccount(admin, "alice", "alice-Secret-Pw-1", false)
	e.createAccount(admin, "bob", "bob-Secret-Pw-1", false)
	tokA := e.token("alice", "alice-Secret-Pw-1", newID(), newID())
	tokB := e.token("bob", "bob-Secret-Pw-1", newID(), newID())
	sessA := e.session("alice", "alice-Secret-Pw-1")
	e.registerWeb(sessA, newID(), newID()).must(http.StatusOK)
	e.session("bob", "bob-Secret-Pw-1")
	nonce := newID()
	tokA2 := e.token("alice", "alice-Secret-Pw-1", newID(), nonce)
	e.successor(bearer(tokA2), newID(), nonce).must(http.StatusOK)
	e.addSecret("bob-Secret-Pw-2")
	e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": "bob-Secret-Pw-1", "new": "bob-Secret-Pw-2"},
		bearer(tokB)).must(http.StatusOK)
	e.resetPassword(admin, aliceID, "alice-Secret-Pw-2").must(http.StatusOK)
	e.token("alice", "alice-Secret-Pw-2", newID(), newID())
	e.session("bob", "bob-Secret-Pw-2")
	_ = tokA

	if n := e.count(`SELECT COUNT(*) FROM account_logins WHERE password_hash LIKE '$argon2id$v=19$m=%'`); n != 3 {
		t.Errorf("%d of 3 passwords stored as Argon2id PHC strings", n)
	}
	sum := sha256.Sum256([]byte(tokB))
	if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE token_hash = ?`, sum[:]) != 1 {
		t.Error("a device token is not found by its SHA-256")
	}

	secrets := append(e.allSecrets(), setupCode)
	scan := func(when string) {
		entries, err := os.ReadDir(e.dir)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, entry := range entries {
			if !entry.Type().IsRegular() {
				continue
			}
			names = append(names, entry.Name())
			data, err := os.ReadFile(filepath.Join(e.dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range secrets {
				needles := [][]byte{[]byte(s)}
				if raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(s, auth.TokenPrefix)); err == nil && len(raw) == 32 {
					needles = append(needles, raw) // the token's random bytes
				}
				for _, needle := range needles {
					if bytes.Contains(data, needle) {
						t.Errorf("%s: %s contains a secret (%d bytes)", when, entry.Name(), len(s))
					}
				}
			}
		}
		if !slices.Contains(names, "nottennapod.db") {
			t.Errorf("%s: database file not found among %v", when, names)
		}
		t.Logf("%s: scanned %v for %d secrets", when, names, len(secrets))
	}
	scan("while running (database and WAL)")
	e.store.Close()
	scan("after closing")
}

// ---- 3. Lifecycle

// A password reset landing between a sign-in's password check and its token
// issue leaves no token (device sign-in, web sign-in and password change).
func TestResetDuringSignInLeavesNoCredential(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	tokens := func() int { return e.count(`SELECT COUNT(*) FROM auth_tokens WHERE account_id = ?`, aliceID) }

	// Device sign-in.
	x := newID()
	e.afterVerify.set(func() { e.resetPassword(admin, aliceID, "alice-password-2").must(http.StatusOK) })
	r := e.signIn("alice", "alice-password-1", x, newID())
	if e.afterVerify.pending() {
		t.Fatal("the hook did not run")
	}
	r.expect(http.StatusUnauthorized, "unauthorized").refused()
	if n := tokens(); n != 0 {
		t.Errorf("%d tokens after a sign-in raced by a reset, want 0", n)
	}
	if n := e.count(`SELECT COUNT(*) FROM installations WHERE account_id = ?`, aliceID); n != 0 {
		t.Errorf("the raced sign-in registered %d installations", n)
	}
	e.signIn("alice", "alice-password-1", x, newID()).expect(http.StatusUnauthorized, "unauthorized")

	// Web sign-in.
	e.afterVerify.set(func() { e.resetPassword(admin, aliceID, "alice-password-3").must(http.StatusOK) })
	e.webSignIn("alice", "alice-password-2").expect(http.StatusUnauthorized, "unauthorized")
	if e.afterVerify.pending() {
		t.Fatal("the hook did not run")
	}
	if n := tokens(); n != 0 {
		t.Errorf("%d sessions after a web sign-in raced by a reset, want 0", n)
	}

	// Own password change.
	tok := e.token("alice", "alice-password-3", newID(), newID())
	e.addSecret("alice-chosen-pw")
	e.afterVerify.set(func() { e.resetPassword(admin, aliceID, "alice-password-4").must(http.StatusOK) })
	e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": "alice-password-3", "new": "alice-chosen-pw"},
		bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")
	if e.afterVerify.pending() {
		t.Fatal("the hook did not run")
	}
	e.signIn("alice", "alice-chosen-pw", newID(), newID()).expect(http.StatusUnauthorized, "unauthorized")
	e.token("alice", "alice-password-4", newID(), newID())
}

// The account being disabled between the password check and the issue also
// leaves no token.
func TestDisableDuringSignInLeavesNoCredential(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	e.afterVerify.set(func() { e.setState(admin, aliceID, "disabled").must(http.StatusOK) })
	r := e.signIn("alice", "alice-password-1", newID(), newID())
	if e.afterVerify.pending() {
		t.Fatal("the hook did not run")
	}
	r.refused()
	if n := e.count(`SELECT COUNT(*) FROM auth_tokens WHERE account_id = ?`, aliceID); n != 0 {
		t.Errorf("%d tokens after a sign-in raced by disabling, want 0", n)
	}
}

// DESIGN MISMATCH. Design, "Credential check without races": "in one write
// transaction, re-read the login and issue the token only if
// credentials_version is unchanged and the account is still active (otherwise
// 401, as if the password were wrong)". The implementation answers 403 when the
// account was disabled in between (verifiedLogin.recheck returns errForbidden).
func TestDisabledDuringSignInAnswers401(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	e.afterVerify.set(func() { e.setState(admin, aliceID, "disabled").must(http.StatusOK) })
	e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusUnauthorized, "unauthorized")
}

func TestRevokedDeviceSignsInAgainAndResumes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	x, nonce := newID(), newID()
	tok := e.token("alice", "alice-password-1", x, nonce)
	other := e.token("alice", "alice-password-1", newID(), newID())

	e.revoke(bearer(other), x).must(http.StatusOK)
	e.list(bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "still here?"}, bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")
	inst := e.installations(bearer(other))
	if inst[x] == nil || inst[x]["revoked"] != true {
		t.Fatalf("the revoked installation is not listed as revoked: %v", inst[x])
	}

	tok2 := e.token("alice", "alice-password-1", x, nonce)
	inst = e.installations(bearer(tok2))
	if len(inst) != 2 || inst[x]["revoked"] != false || inst[x]["current"] != true {
		t.Errorf("signing in again did not resume the same installation: %v", inst)
	}
	e.list(bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")

	// A device can also revoke itself, and sign-out deletes only its own token.
	e.revoke(bearer(tok2), x).must(http.StatusOK)
	e.list(bearer(tok2)).expect(http.StatusUnauthorized, "unauthorized")
	tok3 := e.token("alice", "alice-password-1", x, nonce)
	e.do(http.MethodPost, "/v1/auth/device/sign-out", nil, bearer(tok3)).must(http.StatusOK)
	e.list(bearer(tok3)).expect(http.StatusUnauthorized, "unauthorized")
	e.list(bearer(other)).must(http.StatusOK)
	if inst := e.installations(bearer(other)); inst[x]["revoked"] != false {
		t.Errorf("signing out revoked the installation: %v", inst[x])
	}
}

func TestCopiedInstallationIDWithAnotherNonceConflicts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	x := newID()
	original := e.token("alice", "alice-password-1", x, newID())

	copied := map[string]any{"name": "Restored iPad", "platform": "ios", "app_version": "9.9", "capabilities": []string{"queue"}}
	e.signInAs("alice", "alice-password-1", x, newID(), copied).expect(http.StatusConflict, "installation_conflict").refused()
	inst := e.installations(bearer(original))
	if len(inst) != 1 || inst[x]["name"] != "iPhone" || inst[x]["app_version"] != "1.0" || inst[x]["current"] != true {
		t.Errorf("the conflicting sign-in changed the original installation: %v", inst)
	}
	// The copy takes a fresh installation ID with the password it already has.
	fresh := newID()
	copyTok := e.signInAs("alice", "alice-password-1", fresh, newID(), copied).must(http.StatusOK).str("token")
	if e.current(bearer(copyTok)) != fresh || e.current(bearer(original)) != x {
		t.Error("the original and the copy do not each keep their own installation")
	}
}

func TestSuccessorWithTheSameNonceSupersedes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	x, y, nonce := newID(), newID(), newID()
	old := e.token("alice", "alice-password-1", x, nonce)

	r := e.successor(bearer(old), y, nonce).must(http.StatusOK) // no password needed
	first := r.str("token")
	if r.str("install_id") != y || !auth.WellFormed(first) || first == old {
		t.Fatalf("successor response %s", r.raw)
	}
	// Until the successor's credential is used, the old one may only repeat the
	// handoff or sign out; everything else answers installation_superseded.
	e.list(bearer(old)).expect(http.StatusConflict, "installation_superseded")
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "renamed"}, bearer(old)).expect(http.StatusConflict, "installation_superseded")
	e.revoke(bearer(old), y).expect(http.StatusConflict, "installation_superseded")
	e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": "alice-password-1", "new": "alice-password-2"},
		bearer(old)).expect(http.StatusConflict, "installation_superseded")
	z := newID()
	e.successor(bearer(old), z, nonce).refused()

	// The response was lost: repeating gives a fresh credential for the same
	// successor, and the earlier successor credential stops working.
	r = e.successor(bearer(old), y, nonce).must(http.StatusOK)
	second := r.str("token")
	if r.str("install_id") != y || second == first || !auth.WellFormed(second) {
		t.Fatalf("repeated successor response %s", r.raw)
	}
	e.list(bearer(first)).expect(http.StatusUnauthorized, "unauthorized")

	// The successor's first use revokes the old credential.
	inst := e.installations(bearer(second))
	if inst[y] == nil || inst[y]["current"] != true || inst[x] == nil {
		t.Errorf("after the handoff the list is %v", inst)
	}
	if inst[z] != nil {
		t.Errorf("a refused handoff created installation %s", z)
	}
	e.list(bearer(old)).expect(http.StatusUnauthorized, "unauthorized")
	e.successor(bearer(old), y, nonce).expect(http.StatusUnauthorized, "unauthorized")
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "moved on"}, bearer(second)).must(http.StatusOK)
	e.list(bearer(second)).must(http.StatusOK)
	if e.count(`SELECT COUNT(*) FROM installations WHERE install_id = ?`, x) != 1 {
		t.Error("the superseded installation's row was deleted")
	}
}

func TestSupersededCredentialMaySignOut(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	nonce := newID()
	old := e.token("alice", "alice-password-1", newID(), nonce)
	y := newID()
	next := e.successor(bearer(old), y, nonce).must(http.StatusOK).str("token")
	e.do(http.MethodPost, "/v1/auth/device/sign-out", nil, bearer(old)).must(http.StatusOK)
	e.successor(bearer(old), y, nonce).expect(http.StatusUnauthorized, "unauthorized")
	if e.current(bearer(next)) != y {
		t.Error("the successor does not work after the old credential signed out")
	}
}

func TestSuccessorWithAnotherNonceLeavesTheOriginal(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	x, y := newID(), newID()
	// A restored copy of the phone carries the original's working token.
	shared := e.token("alice", "alice-password-1", x, newID())
	copyNonce := newID()
	first := e.successor(bearer(shared), y, copyNonce).must(http.StatusOK).str("token")
	if e.current(bearer(first)) != y {
		t.Error("the copy's successor credential is not bound to the successor")
	}
	if e.current(bearer(shared)) != x {
		t.Error("the original stopped working after the copy took a successor")
	}
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "original"}, bearer(shared)).must(http.StatusOK)
	// The copy's response was lost: it repeats with the shared token.
	second := e.successor(bearer(shared), y, copyNonce).must(http.StatusOK).str("token")
	if second == first {
		t.Fatal("the repeated handoff returned the same token")
	}
	e.list(bearer(first)).expect(http.StatusUnauthorized, "unauthorized")
	if e.current(bearer(second)) != y || e.current(bearer(shared)) != x {
		t.Error("after the repeated handoff the original and the copy are not both working")
	}
	inst := e.installations(bearer(shared))
	if inst[x]["revoked"] != false || inst[y]["revoked"] != false {
		t.Errorf("installations after a copy's handoff: %v", inst)
	}
}

// A password reset, a revocation or disabling forced between authentication and
// successor issuance leaves no successor credential.
func TestSuccessorRacedByResetOrRevocationIssuesNothing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		interfere func(e *env, admin, aliceID, x, other string)
		status    int
		code      string
	}{
		{"password reset", func(e *env, admin, aliceID, x, other string) {
			e.resetPassword(admin, aliceID, "alice-password-2").must(http.StatusOK)
		}, http.StatusUnauthorized, "unauthorized"},
		{"revocation", func(e *env, admin, aliceID, x, other string) {
			e.revoke(bearer(other), x).must(http.StatusOK)
		}, http.StatusUnauthorized, "unauthorized"},
		{"disabled", func(e *env, admin, aliceID, x, other string) {
			e.setState(admin, aliceID, "disabled").must(http.StatusOK)
		}, http.StatusForbidden, "forbidden"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			admin, _ := e.setupAdmin("root", "root-password-1")
			aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
			x, y, nonce := newID(), newID(), newID()
			tok := e.token("alice", "alice-password-1", x, nonce)
			other := e.token("alice", "alice-password-1", newID(), newID())
			e.beforeIssue.set(func() { tc.interfere(e, admin, aliceID, x, other) })
			e.successor(bearer(tok), y, nonce).expect(tc.status, tc.code).refused()
			if e.beforeIssue.pending() {
				t.Fatal("the hook did not run")
			}
			if n := e.count(`SELECT COUNT(*) FROM auth_tokens WHERE account_id = ? AND install_id = ?`, aliceID, y); n != 0 {
				t.Errorf("%d successor credentials issued", n)
			}
			if n := e.count(`SELECT COUNT(*) FROM installations WHERE account_id = ? AND install_id = ?`, aliceID, y); n != 0 {
				t.Errorf("the successor installation was created")
			}
			if n := e.count(`SELECT COUNT(*) FROM installations WHERE superseded_by IS NOT NULL`); n != 0 {
				t.Errorf("the original was marked superseded")
			}
		})
	}
}

func TestWebSessionSuccessorIsANewCookie(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	w, y, nonce := newID(), newID(), newID()
	s := e.session("alice", "alice-password-1")
	e.registerWeb(s, w, nonce).must(http.StatusOK)

	r := e.successor(browser(s), y, nonce).must(http.StatusOK)
	c := r.cookie()
	if c == nil || c.Value == s || !auth.WellFormed(c.Value) {
		t.Fatalf("the successor did not set a new session cookie: %v", r.cookies)
	}
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" {
		t.Errorf("successor cookie flags: %+v", c)
	}
	if r.str("token") != "" {
		t.Error("a browser successor returned a bearer token")
	}
	// The original session stays bound to the original, superseded installation.
	e.list(browser(s)).expect(http.StatusConflict, "installation_superseded")
	// Lost response: repeating issues a fresh cookie for the same successor.
	s3 := e.successor(browser(s), y, nonce).must(http.StatusOK).cookie()
	if s3 == nil || s3.Value == c.Value || s3.Value == s {
		t.Fatalf("the repeated handoff did not issue a fresh cookie")
	}
	e.list(browser(c.Value)).expect(http.StatusUnauthorized, "unauthorized")
	if e.current(browser(s3.Value)) != y {
		t.Error("the successor session is not bound to the successor")
	}
	e.list(browser(s)).expect(http.StatusUnauthorized, "unauthorized")

	// An unbound session cannot take a successor.
	u := e.session("alice", "alice-password-1")
	z := newID()
	e.successor(browser(u), z, newID()).refused()
	if inst := e.installations(browser(u)); inst[z] != nil {
		t.Error("an unbound session created a successor")
	}
}

// Two browser profiles share one session (a copied profile); the copy takes a
// successor with its own nonce; both keep working independently.
func TestCopiedBrowserSharingASessionKeepsBothWorking(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	w, y, nonce, copyNonce := newID(), newID(), newID(), newID()
	shared := e.session("alice", "alice-password-1")
	e.registerWeb(shared, w, nonce).must(http.StatusOK)

	first := e.successor(browser(shared), y, copyNonce).must(http.StatusOK).cookie()
	if first == nil {
		t.Fatal("no session cookie for the copy")
	}
	if e.current(browser(shared)) != w || e.current(browser(first.Value)) != y {
		t.Fatal("the browsers are not bound to their own installations")
	}
	// The copy's response was lost; it repeats with the shared session.
	second := e.successor(browser(shared), y, copyNonce).must(http.StatusOK).cookie()
	if second == nil || second.Value == first.Value {
		t.Fatal("no fresh cookie on the repeated handoff")
	}
	e.list(browser(first.Value)).expect(http.StatusUnauthorized, "unauthorized")
	if e.current(browser(shared)) != w || e.current(browser(second.Value)) != y {
		t.Error("after the repeated handoff the browsers are not both working")
	}
	e.registerWeb(shared, w, nonce).must(http.StatusOK)
	e.registerWeb(second.Value, y, copyNonce).must(http.StatusOK)
}

func TestDisableAndEnable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	x := newID()
	tok := e.token("alice", "alice-password-1", x, newID())
	bound := e.session("alice", "alice-password-1")
	e.registerWeb(bound, newID(), newID()).must(http.StatusOK)
	unbound := e.session("alice", "alice-password-1")
	creds := map[string]opt{"device token": bearer(tok), "bound session": browser(bound), "unbound session": browser(unbound)}

	e.setState(admin, aliceID, "disabled").must(http.StatusOK)
	for name, c := range creds {
		e.list(c).expect(http.StatusForbidden, "forbidden")
		e.revoke(c, x).expect(http.StatusForbidden, "forbidden")
		_ = name
	}
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "x"}, bearer(tok)).expect(http.StatusForbidden, "forbidden")
	e.signIn("alice", "alice-password-1", newID(), newID()).refused()
	if e.webSignIn("alice", "alice-password-1").refused().cookie() != nil {
		t.Error("a disabled account got a session")
	}
	if a := e.accounts(admin)["alice"]; a["state"] != "disabled" {
		t.Errorf("admin list shows %v", a)
	}

	e.setState(admin, aliceID, "active").must(http.StatusOK)
	for name, c := range creds {
		if r := e.list(c); r.status != http.StatusOK {
			t.Errorf("%s does not work again after enabling: %d %s", name, r.status, r.raw)
		}
	}
	if inst := e.installations(bearer(tok)); inst[x]["revoked"] != false {
		t.Error("disabling revoked the installation")
	}
}

func TestPasswordResetRevokesEveryCredential(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	x1, n1, x2 := newID(), newID(), newID()
	tok1 := e.token("alice", "alice-password-1", x1, n1)
	tok2 := e.token("alice", "alice-password-1", x2, newID())
	s := e.session("alice", "alice-password-1")
	e.registerWeb(s, newID(), newID()).must(http.StatusOK)
	rootTok := e.token("root", "root-password-1", newID(), newID())

	e.resetPassword(admin, aliceID, "alice-password-2").must(http.StatusOK)
	for _, c := range []opt{bearer(tok1), bearer(tok2), browser(s)} {
		e.list(c).expect(http.StatusUnauthorized, "unauthorized")
	}
	e.list(bearer(rootTok)).must(http.StatusOK)
	e.list(browser(admin)).must(http.StatusOK)
	e.signIn("alice", "alice-password-1", x1, n1).expect(http.StatusUnauthorized, "unauthorized")
	again := e.token("alice", "alice-password-2", x1, n1)
	inst := e.installations(bearer(again))
	if len(inst) != 3 || inst[x1]["current"] != true || inst[x1]["revoked"] != false || inst[x2] == nil {
		t.Errorf("after signing in again: %v", inst)
	}
}

func TestOwnPasswordChangeKeepsOnlyTheCaller(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	tok1 := e.token("alice", "alice-password-1", newID(), newID())
	tok2 := e.token("alice", "alice-password-1", newID(), newID())
	s := e.session("alice", "alice-password-1")
	change := func(c opt, current, next string) *response {
		e.addSecret(next)
		return e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": current, "new": next}, c)
	}

	change(bearer(tok1), "not-the-password", "alice-password-2").refused()
	change(bearer(tok1), "alice-password-1", "short").expect(http.StatusBadRequest, "invalid_request")
	e.list(bearer(tok2)).must(http.StatusOK)
	e.list(browser(s)).must(http.StatusOK)

	change(bearer(tok1), "alice-password-1", "alice-password-2").must(http.StatusOK)
	e.list(bearer(tok1)).must(http.StatusOK)
	e.list(bearer(tok2)).expect(http.StatusUnauthorized, "unauthorized")
	e.list(browser(s)).expect(http.StatusUnauthorized, "unauthorized")
	e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusUnauthorized, "unauthorized")

	s2 := e.session("alice", "alice-password-2")
	change(browser(s2), "alice-password-2", "alice-password-3").must(http.StatusOK)
	e.list(browser(s2)).must(http.StatusOK)
	e.list(bearer(tok1)).expect(http.StatusUnauthorized, "unauthorized")
	e.token("alice", "alice-password-3", newID(), newID())
}

func TestLastAdministratorCannotBeRemoved(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	root, rootID := e.setupAdmin("root", "root-password-1")
	e.setState(root, rootID, "disabled").refused()
	e.adminDo(root, http.MethodPatch, "/v1/admin/accounts/"+rootID, map[string]any{"is_admin": false}).refused()
	e.adminDo(root, http.MethodPost, "/v1/admin/accounts/"+rootID+"/delete", map[string]any{"confirm_username": "root"}).refused()
	if a := e.accounts(root)["root"]; a["is_admin"] != true || a["state"] != "active" {
		t.Fatalf("the last administrator changed: %v", a)
	}

	// Disabled administrators do not count.
	bobID := e.createAccount(root, "bob", "bob-password-1", true)
	e.setState(root, bobID, "disabled").must(http.StatusOK)
	e.adminDo(root, http.MethodPatch, "/v1/admin/accounts/"+rootID, map[string]any{"is_admin": false}).refused()
	e.setState(root, rootID, "disabled").refused()
	e.adminDo(root, http.MethodPost, "/v1/admin/accounts/"+rootID+"/delete", map[string]any{"confirm_username": "root"}).refused()

	// With another active administrator, root may step down; bob is then the last one.
	e.setState(root, bobID, "active").must(http.StatusOK)
	e.adminDo(root, http.MethodPatch, "/v1/admin/accounts/"+rootID, map[string]any{"is_admin": false}).must(http.StatusOK)
	e.adminDo(root, http.MethodGet, "/v1/admin/accounts", nil).expect(http.StatusForbidden, "forbidden")
	bob := e.session("bob", "bob-password-1")
	e.adminDo(bob, http.MethodPatch, "/v1/admin/accounts/"+bobID, map[string]any{"is_admin": false}).refused()
	e.setState(bob, bobID, "disabled").refused()
	e.adminDo(bob, http.MethodPost, "/v1/admin/accounts/"+bobID+"/delete", map[string]any{"confirm_username": "bob"}).refused()
	if a := e.accounts(bob)["bob"]; a["is_admin"] != true || a["state"] != "active" {
		t.Errorf("the last administrator changed: %v", a)
	}
}

// "The check and the change are one transaction": two administrators removing
// each other at the same moment never leave the server without one.
func TestLastAdministratorUnderConcurrentChanges(t *testing.T) {
	t.Parallel()
	actions := map[string]func(id, username string) (string, string, any){
		"delete": func(id, username string) (string, string, any) {
			return http.MethodPost, "/v1/admin/accounts/" + id + "/delete", map[string]any{"confirm_username": username}
		},
		"demote": func(id, _ string) (string, string, any) {
			return http.MethodPatch, "/v1/admin/accounts/" + id, map[string]any{"is_admin": false}
		},
		"disable": func(id, _ string) (string, string, any) {
			return http.MethodPatch, "/v1/admin/accounts/" + id, map[string]any{"state": "disabled"}
		},
	}
	for name, action := range actions {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for range 3 {
				e := newEnv(t)
				root, rootID := e.setupAdmin("root", "root-password-1")
				bobID := e.createAccount(root, "bob", "bob-password-1", true)
				bob := e.session("bob", "bob-password-1")
				start := make(chan struct{})
				var wg sync.WaitGroup
				for _, p := range []struct{ session, target, username string }{{root, bobID, "bob"}, {bob, rootID, "root"}} {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						m, path, body := action(p.target, p.username)
						e.do(m, path, body, browser(p.session))
					}()
				}
				close(start)
				wg.Wait()
				n := e.count(`SELECT COUNT(*) FROM account_logins l JOIN accounts a USING (account_id) WHERE l.is_admin = 1 AND a.state = 'active'`)
				if n < 1 {
					t.Fatalf("no active administrator left after concurrent %s", name)
				}
			}
		})
	}
}

func TestDeleteAccountCascadesAndSparesOthers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	bobID := e.createAccount(admin, "bob", "bob-password-1", false)
	tokA := e.token("alice", "alice-password-1", newID(), newID())
	sA := e.session("alice", "alice-password-1")
	e.registerWeb(sA, newID(), newID()).must(http.StatusOK)
	xb := newID()
	tokB := e.token("bob", "bob-password-1", xb, newID())
	before := e.installations(bearer(tokB))

	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+aliceID+"/delete", map[string]any{"confirm_username": "bob"}).refused()
	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+aliceID+"/delete", map[string]any{}).refused()
	e.list(bearer(tokA)).must(http.StatusOK)

	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+aliceID+"/delete", map[string]any{"confirm_username": "alice"}).must(http.StatusOK)
	e.list(bearer(tokA)).expect(http.StatusUnauthorized, "unauthorized")
	e.list(browser(sA)).expect(http.StatusUnauthorized, "unauthorized")
	e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusUnauthorized, "unauthorized")
	for _, table := range []string{"accounts", "account_logins", "installations", "auth_tokens"} {
		if n := e.count(`SELECT COUNT(*) FROM `+table+` WHERE account_id = ?`, aliceID); n != 0 {
			t.Errorf("%d rows of the deleted account remain in %s", n, table)
		}
	}
	if _, ok := e.accounts(admin)["alice"]; ok {
		t.Error("the deleted account is still listed")
	}
	after := e.installations(bearer(tokB))
	if len(after) != len(before) || after[xb]["revoked"] != false || after[xb]["name"] != before[xb]["name"] {
		t.Errorf("bob's installations changed: %v -> %v", before, after)
	}
	if n := e.count(`SELECT COUNT(*) FROM auth_tokens WHERE account_id = ?`, bobID); n != 1 {
		t.Errorf("bob has %d tokens, want 1", n)
	}
	e.token("bob", "bob-password-1", newID(), newID())
	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+aliceID+"/delete", map[string]any{"confirm_username": "alice"}).
		expect(http.StatusNotFound, "not_found")
}

func TestRenameKeepsTheAccount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	bobID := e.createAccount(admin, "bob", "bob-password-1", false)
	r := e.signIn("alice", "alice-password-1", newID(), newID()).must(http.StatusOK)
	tok, before := r.str("token"), r.obj("account")

	e.adminDo(admin, http.MethodPatch, "/v1/admin/accounts/"+aliceID, map[string]any{"username": "Alicia"}).must(http.StatusOK)
	e.list(bearer(tok)).must(http.StatusOK)
	after := e.signIn("ALICIA", "alice-password-1", newID(), newID()).must(http.StatusOK).obj("account")
	if after["account_id"] != before["account_id"] || after["namespace"] != before["namespace"] || after["username"] != "Alicia" {
		t.Errorf("renaming changed the account: %v -> %v", before, after)
	}
	e.signIn("alice", "alice-password-1", newID(), newID()).expect(http.StatusUnauthorized, "unauthorized")
	// Usernames are unique case-insensitively.
	e.adminDo(admin, http.MethodPatch, "/v1/admin/accounts/"+bobID, map[string]any{"username": "alicia"}).refused()
	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts", map[string]any{"username": "ALICIA", "password": "another-password", "is_admin": false}).refused()
	if a := e.accounts(admin); len(a) != 3 || a["bob"] == nil {
		t.Errorf("accounts after refused renames: %v", a)
	}
}

func TestUsernameAndPasswordRules(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	create := func(username, password string) *response {
		return e.adminDo(admin, http.MethodPost, "/v1/admin/accounts", map[string]any{"username": username, "password": password, "is_admin": false})
	}
	for _, u := range []string{"", "with space", "émile", "a/b", "name@home", strings.Repeat("a", 65)} {
		create(u, "long-enough-pw").expect(http.StatusBadRequest, "invalid_request")
	}
	for _, p := range []string{"", "1234567", strings.Repeat("p", 1025)} {
		create("carol", p).expect(http.StatusBadRequest, "invalid_request")
	}
	if len(e.accounts(admin)) != 1 {
		t.Fatal("an invalid account was created")
	}
	for _, c := range []struct{ u, p string }{
		{"A.b_c-9", "12345678"},
		{strings.Repeat("z", 64), strings.Repeat("p", 1024)},
		{"multibyte", "ééé日"}, // 3*2 + 3 = 9 bytes of UTF-8
	} {
		create(c.u, c.p).must(http.StatusCreated)
		e.token(c.u, c.p, newID(), newID())
	}
}

func TestRehashOnSignInAfterParametersAreRaised(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	tok := e.token("alice", "alice-password-1", newID(), newID())
	stronger := auth.Params{Memory: 2048, Time: 2, Threads: 1}
	srv, err := New(Config{Store: e.store, Hasher: auth.NewHasher(stronger), Limiter: auth.NewLimiter(e.clock.Now), Setup: e.setup, Now: e.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	e.srv = srv // the server restarted with stronger parameters
	e.token("alice", "alice-password-1", newID(), newID())
	if n := e.count(`SELECT COUNT(*) FROM account_logins WHERE account_id = ? AND password_hash LIKE '$argon2id$v=19$m=2048,t=2,p=1$%'`, aliceID); n != 1 {
		t.Error("the password was not rehashed with the raised parameters")
	}
	e.list(bearer(tok)).must(http.StatusOK) // rehashing revokes nothing
	e.token("alice", "alice-password-1", newID(), newID())
}

func TestTokensDoNotExpireButIdleSessionsDo(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	tok := e.token("alice", "alice-password-1", newID(), newID())
	used := e.session("alice", "alice-password-1")
	idle := e.session("alice", "alice-password-1")
	day := 24 * time.Hour

	e.clock.Advance(29 * day)
	e.list(browser(used)).must(http.StatusOK)
	e.clock.Advance(29 * day) // day 58: 29 days after the last use
	e.list(browser(used)).must(http.StatusOK)
	e.list(browser(idle)).expect(http.StatusUnauthorized, "unauthorized")
	e.list(browser(admin)).expect(http.StatusUnauthorized, "unauthorized")
	e.clock.Advance(30*day + time.Minute)
	e.list(browser(used)).expect(http.StatusUnauthorized, "unauthorized")
	e.clock.Advance(400 * day)
	e.list(bearer(tok)).must(http.StatusOK)
}

// browserJar models how a browser keeps the session cookie: it expires Max-Age
// seconds after the response that set it.
type browserJar struct {
	value   string
	expires time.Time // zero: kept until the browser closes
}

func (b *browserJar) receive(cookies []*http.Cookie, now time.Time) {
	for _, c := range cookies {
		if c.Name != sessionCookie {
			continue
		}
		switch {
		case c.MaxAge < 0:
			b.value = ""
		case c.MaxAge > 0:
			b.value, b.expires = c.Value, now.Add(time.Duration(c.MaxAge)*time.Second)
		case !c.Expires.IsZero():
			b.value, b.expires = c.Value, c.Expires
		default:
			b.value, b.expires = c.Value, time.Time{}
		}
	}
}

func (b *browserJar) has(now time.Time) bool {
	return b.value != "" && (b.expires.IsZero() || now.Before(b.expires))
}

// DESIGN MISMATCH. Design, "Web session": "Sessions expire after 30 days without
// use; each use (at most once an hour) extends them." The server extends its own
// expiry, but the cookie is set only at sign-in with Max-Age 30 days and never
// refreshed, so the browser discards it 30 days after sign-in even when the web
// app is used every day: an active user is signed out every month.
func TestUsedSessionCookieOutlivesThirtyDays(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	var jar browserJar
	jar.receive(e.webSignIn("alice", "alice-password-1").must(http.StatusOK).cookies, e.clock.Now())
	for day := 1; day <= 40; day++ {
		e.clock.Advance(24 * time.Hour)
		if !jar.has(e.clock.Now()) {
			t.Fatalf("day %d: the browser no longer holds the session cookie although the session was used every day", day)
		}
		r := e.list(browser(jar.value)).must(http.StatusOK)
		jar.receive(r.cookies, e.clock.Now())
	}
}

// ---- web installations

func TestWebInstallationRegistration(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	w, nonce := newID(), newID()
	s := e.session("alice", "alice-password-1")
	if e.current(browser(s)) != "" {
		t.Error("an unbound session has a current installation")
	}
	e.registerWeb(s, w, nonce).must(http.StatusOK)
	e.registerWeb(s, w, nonce).must(http.StatusOK) // idempotent
	inst := e.installations(browser(s))
	if len(inst) != 1 || inst[w]["platform"] != "web" || inst[w]["current"] != true {
		t.Fatalf("after registering: %v", inst)
	}
	w2 := newID()
	e.registerWeb(s, w2, nonce).refused() // a session binds only once
	if e.installations(browser(s))[w2] != nil {
		t.Error("a second binding created an installation")
	}
	// Another browser profile with a copied ID but its own nonce.
	s2 := e.session("alice", "alice-password-1")
	e.registerWeb(s2, w, newID()).expect(http.StatusConflict, "installation_conflict")
	if e.current(browser(s2)) != "" {
		t.Error("a conflicting registration bound the session")
	}
	// Signing in again in the same profile resumes the revoked installation.
	tok := e.token("alice", "alice-password-1", newID(), newID())
	e.revoke(bearer(tok), w).must(http.StatusOK)
	e.list(browser(s)).expect(http.StatusUnauthorized, "unauthorized")
	s3 := e.session("alice", "alice-password-1")
	e.registerWeb(s3, w, nonce).must(http.StatusOK)
	if inst := e.installations(browser(s3)); inst[w]["current"] != true || inst[w]["revoked"] != false {
		t.Errorf("re-registration did not resume: %v", inst[w])
	}
	// Device tokens cannot register web installations.
	e.do(http.MethodPost, "/v1/installations", map[string]any{"install_id": newID(), "instance_nonce": newID(), "name": "x",
		"app_version": "1", "capabilities": []string{}}, bearer(tok)).refused()
}

// DESIGN MISMATCH. Design, "Web sign-in": "a known install_id with the same nonce
// resumes it", and "Device sign-in": resuming means "the new token replaces every
// earlier token of that installation". Registering a known web installation from
// a new session leaves the installation's earlier sessions working.
func TestWebResumeReplacesEarlierSessions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	w, nonce := newID(), newID()
	earlier := e.session("alice", "alice-password-1")
	e.registerWeb(earlier, w, nonce).must(http.StatusOK)
	later := e.session("alice", "alice-password-1")
	e.registerWeb(later, w, nonce).must(http.StatusOK)
	e.list(browser(later)).must(http.StatusOK)
	e.list(browser(earlier)).expect(http.StatusUnauthorized, "unauthorized")
}

func TestInstallationListAndSelfUpdate(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	x := newID()
	r := e.signInAs("alice", "alice-password-1", x, newID(), device("Kitchen iPhone", "queue", "inbox")).must(http.StatusOK)
	tok := r.str("token")
	if !strings.HasPrefix(tok, auth.TokenPrefix) || !auth.WellFormed(tok) || r.str("install_id") != x {
		t.Errorf("sign-in response %s", r.raw)
	}
	if a := r.obj("account"); len(fmt.Sprint(a["account_id"])) != 36 || len(fmt.Sprint(a["namespace"])) != 36 || a["username"] != "alice" {
		t.Errorf("sign-in account %v", a)
	}
	caps := func(m map[string]any) []string {
		var out []string
		for _, c := range m["capabilities"].([]any) {
			out = append(out, c.(string))
		}
		slices.Sort(out)
		return out
	}
	inst := e.installations(bearer(tok))[x]
	if inst["name"] != "Kitchen iPhone" || inst["platform"] != "ios" || inst["app_version"] != "1.0" || inst["revoked"] != false ||
		inst["current"] != true || !slices.Equal(caps(inst), []string{"inbox", "queue"}) {
		t.Errorf("listed installation %v", inst)
	}
	for _, k := range []string{"created_at", "last_seen_at"} {
		if v, _ := inst[k].(float64); v <= 0 {
			t.Errorf("%s missing: %v", k, inst)
		}
	}

	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "Bedroom iPhone", "capabilities": []string{"queue", "inbox", "favorites"}},
		bearer(tok)).must(http.StatusOK)
	inst = e.installations(bearer(tok))[x]
	if inst["name"] != "Bedroom iPhone" || inst["app_version"] != "1.0" || !slices.Equal(caps(inst), []string{"favorites", "inbox", "queue"}) {
		t.Errorf("after the update: %v", inst)
	}
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"capabilities": []string{"queue", "teleport"}}, bearer(tok)).
		expect(http.StatusBadRequest, "invalid_request")
	if inst = e.installations(bearer(tok))[x]; len(caps(inst)) != 3 {
		t.Errorf("a refused update changed the capabilities: %v", inst)
	}
	s := e.session("alice", "alice-password-1")
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "web"}, browser(s)).refused()

	// Unknown capability names are rejected at sign-in, and nothing is registered.
	y := newID()
	e.signInAs("alice", "alice-password-1", y, newID(), device("x", "queue", "teleport")).expect(http.StatusBadRequest, "invalid_request")
	if e.installations(bearer(tok))[y] != nil {
		t.Error("a refused sign-in registered an installation")
	}

	// last_seen_at follows use, written at most once a minute.
	seen := func() float64 { return e.installations(bearer(tok))[x]["last_seen_at"].(float64) }
	l0 := seen()
	e.clock.Advance(30 * time.Second)
	if seen() != l0 {
		t.Error("last_seen_at written twice within a minute")
	}
	e.clock.Advance(61 * time.Second)
	if l1 := seen(); l1 <= l0 {
		t.Errorf("last_seen_at not updated after a minute: %v -> %v", l0, l1)
	}
}

// ---- 4. Cross-account

func TestCrossAccountRequestsChangeNothingOfTheOtherAccount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	bobID := e.createAccount(admin, "bob", "bob-password-1", false)

	xb, nb, wb, nwb := newID(), newID(), newID(), newID()
	tokB := e.signInAs("bob", "bob-password-1", xb, nb, device("Bob's phone", "queue")).must(http.StatusOK).str("token")
	sB := e.session("bob", "bob-password-1")
	e.registerWeb(sB, wb, nwb).must(http.StatusOK)
	snapshot := func() map[string]string {
		out := map[string]string{}
		for id, m := range e.installations(bearer(tokB)) {
			out[id] = fmt.Sprint(m["name"], m["app_version"], m["capabilities"], m["revoked"], m["superseded"], m["created_at"])
		}
		return out
	}
	before := snapshot()

	tokA := e.token("alice", "alice-password-1", newID(), newID())
	sA := e.session("alice", "alice-password-1")
	e.registerWeb(sA, newID(), newID()).must(http.StatusOK)
	for _, c := range []opt{bearer(tokA), browser(sA)} {
		listed := e.installations(c)
		if listed[xb] != nil || listed[wb] != nil {
			t.Errorf("alice's list shows bob's installations: %v", listed)
		}
		for _, id := range []string{xb, wb} {
			e.revoke(c, id).expect(http.StatusNotFound, "not_found")
		}
	}
	// Endpoints that take an installation ID create alice's own installation
	// with that ID (IDs are unique per account) and never touch bob's.
	e.successor(bearer(tokA), xb, nb).must(http.StatusOK)
	tokA2 := e.signIn("alice", "alice-password-1", wb, nwb).must(http.StatusOK).str("token")
	e.signIn("alice", "alice-password-1", xb, newID()).refused() // alice's own xb now has another nonce
	sA2 := e.session("alice", "alice-password-1")
	e.registerWeb(sA2, xb, nb).refused()
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "renamed by alice"}, bearer(tokA2)).must(http.StatusOK)
	e.revoke(bearer(tokA2), wb).must(http.StatusOK) // alice's own wb
	// Administration with a non-administrator's credentials.
	for _, c := range []opt{browser(sA), bearer(tokA)} {
		e.do(http.MethodGet, "/v1/admin/accounts", nil, c).expect(http.StatusForbidden, "forbidden")
		e.do(http.MethodPatch, "/v1/admin/accounts/"+bobID, map[string]any{"state": "disabled"}, c).expect(http.StatusForbidden, "forbidden")
		e.do(http.MethodPatch, "/v1/admin/accounts/"+bobID, map[string]any{"username": "robert"}, c).expect(http.StatusForbidden, "forbidden")
		e.do(http.MethodPost, "/v1/admin/accounts/"+bobID+"/password", map[string]any{"password": "taken-over-pw"}, c).expect(http.StatusForbidden, "forbidden")
		e.do(http.MethodPost, "/v1/admin/accounts/"+bobID+"/delete", map[string]any{"confirm_username": "bob"}, c).expect(http.StatusForbidden, "forbidden")
	}
	// Alice changing her own password revokes only her own credentials.
	e.addSecret("alice-password-2")
	e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": "alice-password-1", "new": "alice-password-2"}, browser(sA)).must(http.StatusOK)
	e.list(bearer(tokA)).expect(http.StatusUnauthorized, "unauthorized")

	if after := snapshot(); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("bob's installations changed:\n before %v\n after  %v", before, after)
	}
	e.list(bearer(tokB)).must(http.StatusOK)
	if e.current(browser(sB)) != wb {
		t.Error("bob's web session lost its installation")
	}
	e.token("bob", "bob-password-1", newID(), newID())
	if a := e.accounts(admin)["bob"]; a == nil || a["state"] != "active" {
		t.Errorf("bob's account changed: %v", a)
	}
}

func TestAdministrationNeedsAnAdministratorsWebSession(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, rootID := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	rootTok := e.token("root", "root-password-1", newID(), newID())
	alice := e.session("alice", "alice-password-1")
	calls := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/admin/accounts", nil},
		{http.MethodPost, "/v1/admin/accounts", map[string]any{"username": "mallory", "password": "mallory-password", "is_admin": true}},
		{http.MethodPatch, "/v1/admin/accounts/" + aliceID, map[string]any{"is_admin": true}},
		{http.MethodPatch, "/v1/admin/accounts/" + rootID, map[string]any{"state": "disabled"}},
		{http.MethodPost, "/v1/admin/accounts/" + rootID + "/password", map[string]any{"password": "taken-over-pw"}},
		{http.MethodPost, "/v1/admin/accounts/" + rootID + "/delete", map[string]any{"confirm_username": "root"}},
	}
	for _, c := range calls {
		e.do(c.method, c.path, c.body, browser(alice)).expect(http.StatusForbidden, "forbidden")
		e.do(c.method, c.path, c.body, bearer(rootTok)).expect(http.StatusForbidden, "forbidden")
		e.do(c.method, c.path, c.body, withOrigin(testOrigin)).expect(http.StatusUnauthorized, "unauthorized")
	}
	accounts := e.accounts(admin)
	if len(accounts) != 2 || accounts["alice"]["is_admin"] != false || accounts["root"]["state"] != "active" {
		t.Errorf("refused administration changed accounts: %v", accounts)
	}
	e.token("root", "root-password-1", newID(), newID())
}

// A successor that itself takes a successor before being used: once the last one
// is used, every earlier installation's credentials are revoked.
func TestSuccessorChainIsFinishedAllTheWayBack(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	x, y, z, nonce := newID(), newID(), newID(), newID()
	first := e.token("alice", "alice-password-1", x, nonce)
	second := e.successor(bearer(first), y, nonce).must(http.StatusOK).str("token")
	third := e.successor(bearer(second), z, nonce).must(http.StatusOK).str("token")
	e.list(bearer(first)).expect(http.StatusConflict, "installation_superseded")
	e.list(bearer(second)).expect(http.StatusConflict, "installation_superseded")
	e.list(bearer(third)).must(http.StatusOK) // first use of the last successor
	e.list(bearer(first)).expect(http.StatusUnauthorized, "unauthorized")
	e.list(bearer(second)).expect(http.StatusUnauthorized, "unauthorized")
	e.list(bearer(third)).must(http.StatusOK)
}
