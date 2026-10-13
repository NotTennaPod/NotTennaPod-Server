package api

import (
	"context"
	"net/http"
	"testing"
)

func withService(v map[string]any) func(*Config) {
	return func(c *Config) {
		c.Service = func(context.Context) (any, error) { return v, nil }
	}
}

// The service page is an administrator's web session's only (PROTOCOL §14): refused without
// credentials, to an ordinary account, to a device token, and to an administrator demoted
// or signed out mid-session.
func TestServicePageIsForAdministratorsOnly(t *testing.T) {
	e := newEnv(t, withService(map[string]any{"version": "test"}))
	admin, _ := e.setupAdmin("owner", "correct horse battery")
	e.do(http.MethodGet, "/v1/admin/service", nil).expect(http.StatusUnauthorized, "unauthorized")
	if got := e.do(http.MethodGet, "/v1/admin/service", nil, browser(admin)).must(http.StatusOK).str("version"); got != "test" {
		t.Errorf("version %q", got)
	}
	e.createAccount(admin, "member", "member password 1", false)
	member := e.session("member", "member password 1")
	e.do(http.MethodGet, "/v1/admin/service", nil, browser(member)).expect(http.StatusForbidden, "forbidden")
	tok := e.token("owner", "correct horse battery", newID(), newID())
	e.do(http.MethodGet, "/v1/admin/service", nil, bearer(tok)).expect(http.StatusForbidden, "forbidden")
	// A second administrator, demoted while signed in.
	id := e.createAccount(admin, "deputy", "deputy password 1", true)
	deputy := e.session("deputy", "deputy password 1")
	e.do(http.MethodGet, "/v1/admin/service", nil, browser(deputy)).must(http.StatusOK)
	e.do(http.MethodPatch, "/v1/admin/accounts/"+id, map[string]any{"is_admin": false}, browser(admin)).must(http.StatusOK)
	e.do(http.MethodGet, "/v1/admin/service", nil, browser(deputy)).expect(http.StatusForbidden, "forbidden")
	// Signed out: the session no longer works.
	e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(admin), withOrigin(testOrigin)).must(http.StatusOK)
	e.do(http.MethodGet, "/v1/admin/service", nil, browser(admin)).expect(http.StatusUnauthorized, "unauthorized")
}
