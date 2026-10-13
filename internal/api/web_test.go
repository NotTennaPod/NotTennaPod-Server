package api

// Web protection: web sessions, the browser-facing origin and cross-site
// request protection (PROTOCOL §5.4).

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSessionCookieFlags(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		publicURL string
		request   []opt
		origin    string // the page's origin, which the browser sends
		foreign   string // an origin that must be refused for this server
		secure    bool
	}{
		{"plain HTTP", "", nil, testOrigin, "https://" + testHost, false},
		{"TLS", "", []opt{viaTLS()}, "https://" + testHost, testOrigin, true},
		{"loopback proxy sending X-Forwarded-Proto https", "", []opt{hdr("X-Forwarded-Proto", "https")}, "https://" + testHost, testOrigin, true},
		{"--public-url https", "https://macbook.tailnet.ts.net", nil, "https://macbook.tailnet.ts.net", testOrigin, true},
		{"--public-url https behind a loopback proxy", "https://macbook.tailnet.ts.net", []opt{hdr("X-Forwarded-Proto", "https")},
			"https://macbook.tailnet.ts.net", "https://" + testHost, true},
		{"X-Forwarded-Proto from a non-loopback peer is ignored", "", []opt{from(remotePeer), hdr("X-Forwarded-Proto", "https")},
			testOrigin, "https://" + testHost, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var opts []func(*Config)
			if tc.publicURL != "" {
				opts = append(opts, withPublicURL(tc.publicURL))
			}
			e := newEnv(t, opts...)
			as := func(origin string, extra ...opt) []opt {
				return append(append([]opt{withOrigin(origin)}, tc.request...), extra...)
			}
			flags := func(what string, c *http.Cookie) {
				t.Helper()
				if c == nil {
					t.Fatalf("%s set no session cookie", what)
				}
				if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.Secure != tc.secure {
					t.Errorf("%s cookie: HttpOnly=%v SameSite=%v Path=%q Secure=%v; want HttpOnly, Strict, /, Secure=%v",
						what, c.HttpOnly, c.SameSite, c.Path, c.Secure, tc.secure)
				}
			}
			setup := map[string]any{"username": "root", "password": "root-password-1", "setup_code": e.setup.Code()}
			e.do(http.MethodPost, "/v1/setup", setup, as(tc.foreign)...).expect(http.StatusForbidden, "forbidden")
			admin := e.do(http.MethodPost, "/v1/setup", setup, as(tc.origin)...).must(http.StatusCreated).cookie()
			flags("setup", admin)
			e.do(http.MethodPost, "/v1/admin/accounts", map[string]any{"username": "alice", "password": "alice-password-1", "is_admin": false},
				as(tc.origin, cookieOnly(admin.Value))...).must(http.StatusCreated)

			signIn := map[string]any{"username": "alice", "password": "alice-password-1"}
			e.do(http.MethodPost, "/v1/auth/web", signIn, as(tc.foreign)...).expect(http.StatusForbidden, "forbidden")
			s := e.do(http.MethodPost, "/v1/auth/web", signIn, as(tc.origin)...).must(http.StatusOK).cookie()
			flags("web sign-in", s)

			w, nonce := newID(), newID()
			e.do(http.MethodPost, "/v1/installations", map[string]any{"install_id": w, "instance_nonce": nonce, "name": "Safari",
				"app_version": "1", "capabilities": []string{}}, as(tc.origin, cookieOnly(s.Value))...).must(http.StatusOK)
			succ := e.do(http.MethodPost, "/v1/installations/successor", map[string]any{"install_id": newID(), "instance_nonce": newID(),
				"device": device("Safari copy")}, as(tc.origin, cookieOnly(s.Value))...).must(http.StatusOK).cookie()
			flags("successor", succ)

			// A cookie-authenticated change with the wrong scheme or host is refused.
			e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, as(tc.foreign, cookieOnly(s.Value))...).expect(http.StatusForbidden, "forbidden")
			out := e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, as(tc.origin, cookieOnly(s.Value))...).must(http.StatusOK)
			for _, c := range out.cookies {
				if c.Name == sessionCookie && (c.MaxAge >= 0 || !c.HttpOnly || c.Secure != tc.secure) {
					t.Errorf("sign-out cookie %+v", c)
				}
			}
			e.do(http.MethodGet, "/v1/installations", nil, as(tc.origin, cookieOnly(s.Value))...).expect(http.StatusUnauthorized, "unauthorized")
		})
	}
}

// The same over a real TLS connection.
func TestSecureCookieOverTLS(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ts := httptest.NewTLSServer(e.srv)
	defer ts.Close()
	r := e.netDo(ts.Client(), ts.URL, http.MethodPost, "/v1/setup",
		map[string]any{"username": "root", "password": "root-password-1", "setup_code": e.setup.Code()}, map[string]string{"Origin": ts.URL})
	r.must(http.StatusCreated)
	if c := r.cookie(); c == nil || !c.Secure || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode {
		t.Errorf("session cookie over TLS: %+v", c)
	}
}

func TestCrossOriginSetupAndWebSignInAreRefused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	bad := map[string][]opt{
		"no Origin":       {withOrigin("")},
		"foreign Origin":  {withOrigin("http://evil.example")},
		"scheme mismatch": {withOrigin("https://" + testHost)},
		"port mismatch":   {withOrigin(testOrigin + ":8080")},
		"opaque Origin":   {withOrigin("null")},
		"subdomain":       {withOrigin("http://evil." + testHost)},
	}
	setup := map[string]any{"username": "root", "password": "root-password-1", "setup_code": e.setup.Code()}
	for name, opts := range bad {
		r := e.do(http.MethodPost, "/v1/setup", setup, opts...)
		if r.status != http.StatusForbidden {
			t.Errorf("setup with %s: %d %s", name, r.status, r.raw)
		}
	}
	if e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK).body["setup_required"] != true || !e.fileExists("setup-code") {
		t.Fatal("a cross-origin setup completed setup")
	}
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	for name, opts := range bad {
		r := e.do(http.MethodPost, "/v1/auth/web", map[string]any{"username": "alice", "password": "alice-password-1"}, opts...)
		if r.status != http.StatusForbidden {
			t.Errorf("web sign-in with %s: %d %s", name, r.status, r.raw)
		}
	}
	if n := e.count(`SELECT COUNT(*) FROM auth_tokens WHERE kind = 'web'`); n != 1 {
		t.Errorf("%d web sessions exist, want only the administrator's", n)
	}
}

// A cookie-authenticated state change without our Origin, with a foreign one or
// with a form content type is refused and changes nothing.
func TestCookieStateChangesNeedOurOriginAndJSON(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	x := newID()
	tok := e.token("alice", "alice-password-1", x, newID())
	s := e.session("alice", "alice-password-1")
	w, nonce := newID(), newID()

	attempts := map[string]func(session string) []opt{
		"no Origin":       func(v string) []opt { return []opt{cookieOnly(v)} },
		"foreign Origin":  func(v string) []opt { return []opt{cookieOnly(v), withOrigin("http://evil.example")} },
		"scheme mismatch": func(v string) []opt { return []opt{cookieOnly(v), withOrigin("https://" + testHost)} },
		"form":            func(v string) []opt { return []opt{browser(v), ctype("application/x-www-form-urlencoded")} },
		"multipart form":  func(v string) []opt { return []opt{browser(v), ctype("multipart/form-data; boundary=x")} },
		"text/plain":      func(v string) []opt { return []opt{browser(v), ctype("text/plain")} },
		"no content type": func(v string) []opt { return []opt{browser(v), ctype("")} },
	}
	y := newID()
	for name, attempt := range attempts {
		refuse := func(what string, r *response) {
			t.Helper()
			if r.status < 400 {
				t.Errorf("%s with %s: %d %s", what, name, r.status, r.raw)
			}
		}
		refuse("register", e.do(http.MethodPost, "/v1/installations", map[string]any{"install_id": w, "instance_nonce": nonce, "name": "Firefox",
			"app_version": "1", "capabilities": []string{}}, attempt(s)...))
		refuse("revoke", e.do(http.MethodPost, "/v1/installations/"+x+"/revoke", nil, attempt(s)...))
		refuse("password change", e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": "alice-password-1", "new": "attacker-pw-1"}, attempt(s)...))
		refuse("sign-out", e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, attempt(s)...))
		refuse("admin create", e.do(http.MethodPost, "/v1/admin/accounts", map[string]any{"username": "mallory", "password": "mallory-password", "is_admin": true}, attempt(admin)...))
		refuse("admin reset", e.do(http.MethodPost, "/v1/admin/accounts/"+aliceID+"/password", map[string]any{"password": "attacker-pw-2"}, attempt(admin)...))
		refuse("admin disable", e.do(http.MethodPatch, "/v1/admin/accounts/"+aliceID, map[string]any{"state": "disabled"}, attempt(admin)...))
	}
	// Nothing changed.
	e.list(bearer(tok)).must(http.StatusOK)
	if e.current(browser(s)) != "" {
		t.Error("a refused registration bound the session")
	}
	accounts := e.accounts(admin)
	if len(accounts) != 2 || accounts["alice"]["state"] != "active" {
		t.Errorf("refused administration changed accounts: %v", accounts)
	}
	e.token("alice", "alice-password-1", newID(), newID())

	// The bound session's successor is protected the same way.
	e.registerWeb(s, w, nonce).must(http.StatusOK)
	for name, attempt := range attempts {
		r := e.do(http.MethodPost, "/v1/installations/successor", map[string]any{"install_id": y, "instance_nonce": newID(), "device": device("copy")}, attempt(s)...)
		if r.status < 400 {
			t.Errorf("successor with %s: %d %s", name, r.status, r.raw)
		}
	}
	if inst := e.installations(browser(s)); inst[y] != nil {
		t.Error("a refused successor request created an installation")
	}
	// With our Origin and JSON the same changes work.
	e.revoke(browser(s), x).must(http.StatusOK)
	e.list(bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")
}

// Bearer requests from the apps carry no Origin and are unaffected; an Origin
// that is present must still match.
func TestBearerRequestsNeedNoOrigin(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	nonce := newID()
	tok := e.token("alice", "alice-password-1", newID(), nonce)
	other := newID()
	e.token("alice", "alice-password-1", other, newID())

	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "evil"}, bearer(tok), withOrigin("http://evil.example")).
		expect(http.StatusForbidden, "forbidden")
	e.list(bearer(tok)).must(http.StatusOK)
	e.do(http.MethodPut, "/v1/installations/self", map[string]any{"name": "Phone"}, bearer(tok)).must(http.StatusOK)
	e.revoke(bearer(tok), other).must(http.StatusOK)
	e.addSecret("alice-password-2")
	e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": "alice-password-1", "new": "alice-password-2"}, bearer(tok)).must(http.StatusOK)
	next := e.successor(bearer(tok), newID(), nonce).must(http.StatusOK).str("token")
	e.do(http.MethodPost, "/v1/auth/device/sign-out", nil, bearer(next)).must(http.StatusOK)
	e.list(bearer(next)).expect(http.StatusUnauthorized, "unauthorized")
}
