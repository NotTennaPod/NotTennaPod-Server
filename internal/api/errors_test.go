package api

// Errors (PROTOCOL §10.3).
// Every response these tests receive also passes env.check: JSON, no-store, the
// PROTOCOL §10 error body with a matching status, and no secrets in error bodies.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
)

func TestMissingUnknownOrMisusedCredentialsAre401(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	tok := e.token("alice", "alice-password-1", newID(), newID())
	s := e.session("alice", "alice-password-1")
	unknown, _ := auth.NewCredential()
	for name, o := range map[string]opt{
		"none":                 func(*http.Request) {},
		"malformed bearer":     bearer("abc"),
		"unknown token":        bearer(unknown),
		"basic auth":           hdr("Authorization", "Basic YWxpY2U6cGFzc3dvcmQ="),
		"token without scheme": hdr("Authorization", tok),
		"session as bearer":    bearer(s),
		"token as cookie":      browser(tok),
		"unknown cookie":       browser(unknown),
	} {
		if r := e.list(o); r.status != http.StatusUnauthorized || r.str("error") != "unauthorized" {
			t.Errorf("%s: %d %s", name, r.status, r.raw)
		}
	}
	e.list(bearer(tok)).must(http.StatusOK)
	e.list(browser(s)).must(http.StatusOK)
	// Device-only endpoints refuse a web session, and change nothing.
	e.do(http.MethodPost, "/v1/auth/device/sign-out", nil, browser(s)).refused()
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "web"}, browser(s)).refused()
	e.list(browser(s)).must(http.StatusOK)
}

// Malformed and oversized bodies get 400/413 without side effects.
func TestMalformedAndOversizedBodiesChangeNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	e.addSecret("carol-password-1")
	malformed := []rawBody{
		`{"username": "carol", "password": `,
		`[]`,
		`"carol"`,
		`{"username": "carol", "password": "carol-password-1", "is_admin": false, "role": "owner"}`,
		`{"username": "carol", "password": "carol-password-1", "is_admin": false} {"x": 1}`,
		`{"username": 5, "password": "carol-password-1"}`,
		`{"username": "carol", "password": ["carol-password-1"]}`,
	}
	for _, body := range malformed {
		e.adminDo(admin, http.MethodPost, "/v1/admin/accounts", body).expect(http.StatusBadRequest, "invalid_request")
	}
	huge := rawBody(`{"username": "carol", "password": "` + strings.Repeat("p", 70<<10) + `", "is_admin": false}`)
	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts", huge).expect(http.StatusRequestEntityTooLarge, "payload_too_large")
	if _, ok := e.accounts(admin)["carol"]; ok {
		t.Fatal("a refused request created an account")
	}

	// Refused sign-in bodies create nothing and do not count as failed sign-ins.
	x := newID()
	for range 6 {
		e.do(http.MethodPost, "/v1/auth/device", rawBody(`{"username": "alice", "password": "wrong"`)).expect(http.StatusBadRequest, "invalid_request")
		e.do(http.MethodPost, "/v1/auth/device", map[string]any{"username": "alice", "password": "alice-password-1", "install_id": x,
			"instance_nonce": newID(), "device": device("x"), "remember_me": true}).expect(http.StatusBadRequest, "invalid_request")
		e.do(http.MethodPost, "/v1/auth/device", map[string]any{"username": "alice", "password": "alice-password-1", "install_id": x,
			"instance_nonce": newID(), "device": device(strings.Repeat("n", 70<<10))}).expect(http.StatusRequestEntityTooLarge, "payload_too_large")
		e.do(http.MethodPost, "/v1/auth/device", map[string]any{"username": "alice", "password": "alice-password-1", "install_id": "not-a-uuid",
			"instance_nonce": newID(), "device": device("x")}).expect(http.StatusBadRequest, "invalid_request")
	}
	if n := e.count(`SELECT COUNT(*) FROM installations`); n != 0 {
		t.Errorf("refused sign-ins registered %d installations", n)
	}
	e.token("alice", "alice-password-1", x, newID())
}

func TestUnknownPathsAndMethodsAnswerJSON(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/nope"},
		{http.MethodPost, "/v1/nope"},
		{http.MethodGet, "/v1/installations/" + newID()},
		{http.MethodGet, "/v1/info/"},
	} {
		e.do(c.method, c.path, nil).expect(http.StatusNotFound, "not_found")
	}
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/setup"},
		{http.MethodGet, "/v1/auth/device"},
		{http.MethodPost, "/v1/info"},
		{http.MethodDelete, "/v1/installations"},
		{http.MethodPut, "/v1/admin/accounts"},
		{http.MethodGet, "/v1/installations/self"},
		{http.MethodGet, "/v1/installations/" + newID() + "/revoke"},
	} {
		if r := e.do(c.method, c.path, nil); r.status != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: %d %s, want 405", c.method, c.path, r.status, r.raw)
		}
	}
}
