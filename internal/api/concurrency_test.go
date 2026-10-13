package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// The request body is held after the headers passed authentication while
// another real handler commits a concurrent change.
type heldBody struct {
	io.Reader
	once             sync.Once
	entered, release chan struct{}
}

func (g *heldBody) Read(p []byte) (int, error) {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Reader.Read(p)
}
func delayed(e *env, method, path, body string, credential opt, interfere func()) *response {
	g := &heldBody{Reader: strings.NewReader(body), entered: make(chan struct{}), release: make(chan struct{})}
	r := httptest.NewRequest(method, testOrigin+path, g)
	r.RemoteAddr = loopbackPeer
	r.Header.Set("Content-Type", "application/json")
	credential(r)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); e.srv.ServeHTTP(w, r) }()
	<-g.entered
	interfere()
	close(g.release)
	<-done
	return e.check(method+" "+path, w.Result())
}

func TestDelayedRequestOfDemotedAdminCannotRepromote(t *testing.T) {
	e := newEnv(t)
	root, _ := e.setupAdmin("root", "root-password-1")
	bobID := e.createAccount(root, "bob", "bob-password-1", true)
	bob := e.session("bob", "bob-password-1")
	result := delayed(e, http.MethodPatch, "/v1/admin/accounts/"+bobID, `{"is_admin":true}`, browser(bob), func() {
		e.adminDo(root, http.MethodPatch, "/v1/admin/accounts/"+bobID, map[string]any{"is_admin": false}).must(200)
		e.adminDo(bob, http.MethodGet, "/v1/admin/accounts", nil).expect(403, "forbidden")
	})
	result.expect(403, "forbidden")
	if e.accounts(root)["bob"]["is_admin"] != false {
		t.Error("demoted admin regained administrator access via the delayed request")
	}
}

func TestDelayedRequestOfRevokedTokenCannotRevokeFreshDevice(t *testing.T) {
	e := newEnv(t)
	root, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(root, "alice", "alice-password-1", false)
	stolen := e.token("alice", "alice-password-1", newID(), newID())
	deviceID, nonce := newID(), newID()
	e.token("alice", "alice-password-1", deviceID, nonce)
	var fresh string
	result := delayed(e, http.MethodPost, "/v1/installations/"+deviceID+"/revoke", `{}`, bearer(stolen), func() {
		e.resetPassword(root, aliceID, "alice-password-2").must(200)
		e.list(bearer(stolen)).expect(401, "unauthorized")
		fresh = e.token("alice", "alice-password-2", deviceID, nonce)
	})
	result.expect(401, "unauthorized")
	e.list(bearer(fresh)).expect(200, "")
}

func TestWebRegistrationAcceptsPlatformWeb(t *testing.T) {
	e := newEnv(t)
	root, _ := e.setupAdmin("root", "root-password-1")
	e.do(http.MethodPost, "/v1/installations", map[string]any{
		"install_id": newID(), "instance_nonce": newID(), "name": "Browser", "platform": "web", "app_version": "1", "capabilities": []string{"queue"},
	}, browser(root)).must(200)
}

func TestDelayedRequestOfSupersededInstallationCannotChangeCapabilities(t *testing.T) {
	e := newEnv(t)
	root, _ := e.setupAdmin("root", "root-password-1")
	oldID, nonce := newID(), newID()
	old := e.token("root", "root-password-1", oldID, nonce)
	result := delayed(e, http.MethodPut, "/v1/installations/self", `{"capabilities":["statistics"]}`, bearer(old), func() {
		successor := e.successor(bearer(old), newID(), nonce).must(200).str("token")
		e.list(bearer(successor)).must(200)
		e.list(bearer(old)).expect(401, "unauthorized")
	})
	result.expect(401, "unauthorized")
	if e.count(`SELECT COUNT(*) FROM installations WHERE install_id = ? AND capabilities = '["statistics"]'`, oldID) != 0 {
		t.Error("revoked predecessor changed its capability declaration")
	}
	e.list(browser(root)).must(200)
}

func TestDelayedWebBindingRechecksDisable(t *testing.T) {
	e := newEnv(t)
	root, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(root, "alice", "alice-password-1", false)
	session := e.session("alice", "alice-password-1")
	installID := newID()
	body := `{"install_id":"` + installID + `","instance_nonce":"` + newID() + `","name":"Browser"}`
	result := delayed(e, http.MethodPost, "/v1/installations", body, browser(session), func() {
		e.setState(root, aliceID, "disabled").must(200)
	})
	result.expect(403, "forbidden")
	if e.count(`SELECT COUNT(*) FROM installations WHERE install_id = ?`, installID) != 0 {
		t.Error("disabled account bound a new credential")
	}
}

func TestResumeRejectsDifferentPlatform(t *testing.T) {
	e := newEnv(t)
	root, _ := e.setupAdmin("root", "root-password-1")
	installID, nonce := newID(), newID()
	device := e.token("root", "root-password-1", installID, nonce)
	e.registerWeb(root, installID, nonce).refused()
	e.list(bearer(device)).must(200)
	if e.count(`SELECT COUNT(*) FROM auth_tokens WHERE kind = 'web' AND install_id = ?`, installID) != 0 {
		t.Error("web session bound to iOS installation")
	}
}

// A request authenticated before its installation was superseded (successor
// created but not yet used) cannot complete an ordinary change.
func TestDelayedRequestOfJustSupersededInstallationIsRefused(t *testing.T) {
	e := newEnv(t)
	e.setupAdmin("root", "root-password-1")
	oldID, nonce := newID(), newID()
	old := e.token("root", "root-password-1", oldID, nonce)
	result := delayed(e, http.MethodPut, "/v1/installations/self", `{"capabilities":["statistics"]}`, bearer(old), func() {
		e.successor(bearer(old), newID(), nonce).must(200)
	})
	result.expect(409, "installation_superseded")
	if e.count(`SELECT COUNT(*) FROM installations WHERE install_id = ? AND capabilities = '["statistics"]'`, oldID) != 0 {
		t.Error("a superseded installation changed its capability declaration")
	}
}
