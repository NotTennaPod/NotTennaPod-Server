package api

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// afterAuthentication runs f once, the first time the server reads its clock
// in a request: just after the request's credential was looked up.
func afterAuthentication(e *env, f func()) {
	var once oneShot
	once.set(f)
	e.srv.cfg.Now = func() time.Time { once.run(); return e.clock.Now() }
}

// Reads that must not answer after a reset, disable or demotion landed during the request: the
// installations list, the session, the import status, the media types and
// the administrator's media check. Each checks its caller again before it
// answers.
func TestProtectedReadsCheckTheCallerAgain(t *testing.T) {
	routes := []struct {
		path  string
		admin bool
	}{
		{"/v1/installations", false},
		{"/v1/session", false},
		{"/v1/import", false},
		{"/v1/media/types", false},
		{"/v1/admin/media", true},
	}
	for _, change := range []string{"reset", "disable", "demote"} {
		for _, rt := range routes {
			if (change == "demote") != rt.admin {
				continue
			}
			t.Run(change+rt.path, func(t *testing.T) {
				m := newMediaEnv(t)
				e := m.env
				withImports(t)(&e.srv.cfg)
				id := e.createAccount(m.session, "bob", "bob-password-1", true)
				bob := e.session("bob", "bob-password-1")
				e.registerWeb(bob, newID(), newID()).must(http.StatusOK)
				e.do(http.MethodGet, rt.path, nil, browser(bob)).must(http.StatusOK)
				afterAuthentication(e, func() {
					switch change {
					case "reset":
						e.resetPassword(m.session, id, "bob-password-2").must(http.StatusOK)
					case "disable":
						e.adminDo(m.session, http.MethodPatch, "/v1/admin/accounts/"+id, map[string]any{"state": "disabled"}).must(http.StatusOK)
					case "demote":
						e.adminDo(m.session, http.MethodPatch, "/v1/admin/accounts/"+id, map[string]any{"is_admin": false}).must(http.StatusOK)
					}
				})
				got := e.do(http.MethodGet, rt.path, nil, browser(bob))
				if got.status < 400 {
					t.Errorf("%s after a %s during the request: %d %s", rt.path, change, got.status, strings.TrimSpace(string(got.raw)))
				}
			})
		}
	}
}
