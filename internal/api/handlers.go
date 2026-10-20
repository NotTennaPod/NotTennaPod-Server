package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/version"
)

func (s *Server) routes() {
	s.route("/v1/info", endpoint{method: http.MethodGet, handle: (*request).info})
	s.route("/v1/setup", endpoint{method: http.MethodPost, originNeeded: true, handle: (*request).setup})
	s.route("/v1/auth/device", endpoint{method: http.MethodPost, handle: (*request).signInDevice})
	s.route("/v1/auth/web", endpoint{method: http.MethodPost, originNeeded: true, handle: (*request).signInWeb})
	s.route("/v1/auth/device/sign-out", endpoint{method: http.MethodPost, auth: authDevice, supersededOK: true, handle: (*request).signOut})
	s.route("/v1/auth/web/sign-out", endpoint{method: http.MethodPost, auth: authWeb, supersededOK: true, endsSession: true, handle: (*request).signOut})
	s.route("/v1/installations",
		endpoint{method: http.MethodGet, auth: authAny, handle: (*request).listInstallations},
		endpoint{method: http.MethodPost, auth: authWeb, handle: (*request).registerWeb})
	s.route("/v1/installations/successor", endpoint{method: http.MethodPost, auth: authSync, supersededOK: true, successor: true, handle: (*request).successor})
	s.route("/v1/installations/self", endpoint{method: http.MethodPut, auth: authDevice, handle: (*request).updateSelf})
	s.route("/v1/installations/{id}/revoke", endpoint{method: http.MethodPost, auth: authAny, handle: (*request).revoke})
	s.route("/v1/account/password", endpoint{method: http.MethodPost, auth: authAny, handle: (*request).changePassword})
	s.route("/v1/admin/accounts",
		endpoint{method: http.MethodGet, auth: authAdmin, handle: (*request).adminList},
		endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).adminCreate})
	s.route("/v1/admin/accounts/{id}", endpoint{method: http.MethodPatch, auth: authAdmin, handle: (*request).adminUpdate})
	s.route("/v1/admin/accounts/{id}/password", endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).adminResetPassword})
	s.route("/v1/admin/accounts/{id}/delete", endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).adminDelete})
	s.route("/v1/admin/backup", endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).adminBackup})
	s.route("/v1/admin/keys", endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).adminKeys})
	s.route("/v1/admin/keys/{id}/saved", endpoint{method: http.MethodPost, auth: authAdmin, handle: (*request).adminKeysSaved})
	s.syncRoutes()
	s.importRoutes()
	s.readRoutes()
	s.eventRoutes()
	s.diagnosticsRoutes()
	s.previewRoutes()
	s.sessionRoutes()
	s.serviceRoutes()
	s.mediaRoutes()
	s.mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) { writeError(w, r, errNotFound) })
}

// ---- validation

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func usernameKey(username string) (string, error) {
	if !usernamePattern.MatchString(username) {
		return "", invalid("Usernames have 1 to 64 letters, digits, dots, underscores or hyphens.")
	}
	return strings.ToLower(username), nil
}

func checkPassword(password string) error {
	if len(password) < 8 || len(password) > 1024 || !utf8.ValidString(password) {
		return invalid("Passwords have 8 to 1024 characters.")
	}
	return nil
}

var knownCapabilities = []string{"favorites", "history", "inbox", "queue", "statistics"}

func canonicalCapabilities(caps []string) (string, error) {
	sorted := slices.Clone(caps)
	slices.Sort(sorted)
	if len(slices.Compact(slices.Clone(sorted))) != len(sorted) {
		return "", invalid("Capabilities must not repeat.")
	}
	list := make([]any, len(sorted))
	for i, c := range sorted {
		if !slices.Contains(knownCapabilities, c) {
			return "", invalid("Unknown capability: " + c)
		}
		list[i] = c
	}
	out, err := contract.Canonical(list)
	return string(out), err
}

type deviceInfo struct {
	Name         string   `json:"name"`
	Platform     string   `json:"platform"`
	AppVersion   string   `json:"app_version"`
	Capabilities []string `json:"capabilities"`
}

func (d deviceInfo) check(platforms ...string) (string, error) {
	if !slices.Contains(platforms, d.Platform) {
		return "", invalid("Unsupported platform.")
	}
	if d.Name == "" || len(d.Name) > 200 || len(d.AppVersion) > 100 || !utf8.ValidString(d.Name) || !utf8.ValidString(d.AppVersion) {
		return "", invalid("Device name and app version must be valid text.")
	}
	return canonicalCapabilities(d.Capabilities)
}

func checkUUID(id, what string) error {
	if !contract.ValidUUID(id) {
		return invalid(what + " must be a lowercase UUID.")
	}
	return nil
}

// ---- info and setup

func (req *request) info() (int, any, error) {
	var setupRequired bool
	var serverID string
	err := req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		has, err := r.HasAdmin()
		setupRequired = !has
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	if serverID, err = req.s.cfg.Store.ServerID(req.ctx); err != nil {
		return 0, nil, err
	}
	out := map[string]any{
		"server_id": serverID, "version": version.Version, "protocol": map[string]any{"min": 1, "max": 1},
		"setup_required": setupRequired, "bounds": protocol.Bounds(),
	}
	// This run's instance ID, for service update's readiness check.
	if id := req.s.cfg.Store.InstanceID(); id != "" {
		out["instance_id"] = id
	}
	return http.StatusOK, out, nil
}

func (req *request) setup() (int, any, error) {
	var body struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		SetupCode string `json:"setup_code"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	key, err := usernameKey(body.Username)
	if err != nil {
		return 0, nil, err
	}
	if err := checkPassword(body.Password); err != nil {
		return 0, nil, err
	}
	keys := auth.Keys("setup", peerIP(req.r), isLoopback(req.r))
	if err := req.rateLimit(keys); err != nil {
		return 0, nil, err
	}
	code := req.s.cfg.Setup.Code()
	if code == "" {
		return 0, nil, fail(http.StatusConflict, "setup_complete", "This server is already set up.")
	}
	if subtle.ConstantTimeCompare([]byte(strings.ToUpper(strings.TrimSpace(body.SetupCode))), []byte(code)) != 1 {
		req.s.cfg.Limiter.Fail(keys)
		return 0, nil, fail(http.StatusForbidden, "forbidden", "The setup code is incorrect. It is shown in the server's log and in the setup-code file in its data directory.")
	}
	hash, err := req.s.cfg.Hasher.Hash(req.ctx, body.Password)
	if err != nil {
		return 0, nil, err
	}
	secret, credHash := auth.NewCredential()
	var account storage.Account
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if has, err := tx.HasAdmin(); err != nil || has {
			if err == nil {
				err = fail(http.StatusConflict, "setup_complete", "This server is already set up.")
			}
			return err
		}
		if account, err = tx.CreateAccount(); err != nil {
			return err
		}
		if err := tx.CreateLogin(account.ID, body.Username, key, hash, true); err != nil {
			return err
		}
		expires := req.s.cfg.Now().Add(sessionLifetime).UnixMilli()
		return tx.Account(account.ID).InsertCredential(credHash, "web", "", &expires)
	})
	if err != nil {
		return 0, nil, err
	}
	req.s.cfg.Limiter.Succeed(keys)
	req.s.cfg.Setup.Complete()
	req.clearPresentedSessionCookies()
	req.issueSessionCookie(secret)
	return http.StatusCreated, map[string]any{"account": accountJSON(account, body.Username, true)}, nil
}

func accountJSON(a storage.Account, username string, admin bool) map[string]any {
	return map[string]any{"account_id": a.ID, "namespace": a.Namespace, "username": username, "is_admin": admin}
}

func (req *request) rateLimit(keys []string) error {
	if ok, wait := req.s.cfg.Limiter.Allow(keys); !ok {
		return &apiError{status: http.StatusTooManyRequests, code: "rate_limited", message: "Too many failed attempts; try again later.", retryAfter: wait}
	}
	return nil
}

// ---- password sign-in

// verifiedLogin is the result of a password check made outside transactions.
type verifiedLogin struct {
	login       storage.Login
	rehash      string
	limiterKeys []string
}

// verifyPassword checks username and password: rate limit, read the login,
// verify Argon2id outside any transaction (an unknown username costs the same).
func (req *request) verifyPassword(username, password string) (verifiedLogin, error) {
	key, err := usernameKey(username)
	if err != nil {
		return verifiedLogin{}, errBadCreds
	}
	keys := auth.Keys(key, peerIP(req.r), isLoopback(req.r))
	if err := req.rateLimit(keys); err != nil {
		return verifiedLogin{}, err
	}
	var login storage.Login
	err = req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		var err error
		login, err = r.LoginByUsername(key)
		if errors.Is(err, storage.ErrNotFound) {
			login, err = storage.Login{}, nil
		}
		return err
	})
	if err != nil {
		return verifiedLogin{}, err
	}
	ok, needsRehash, err := req.s.cfg.Hasher.Verify(req.ctx, password, login.PasswordHash)
	if err != nil {
		return verifiedLogin{}, err
	}
	if !ok {
		req.s.cfg.Limiter.Fail(keys)
		return verifiedLogin{}, errBadCreds
	}
	req.s.cfg.Limiter.Succeed(keys)
	v := verifiedLogin{login: login, limiterKeys: keys}
	if needsRehash {
		if v.rehash, err = req.s.cfg.Hasher.Hash(req.ctx, password); err != nil {
			v.rehash = ""
		}
	}
	if login.State != "active" {
		return verifiedLogin{}, errForbidden
	}
	if req.s.cfg.afterPasswordVerify != nil {
		req.s.cfg.afterPasswordVerify()
	}
	return v, nil
}

// recheck re-reads the login inside the issuing transaction: a password reset or
// a disabled account since verification wins.
func (v verifiedLogin) recheck(tx *storage.WriteTx) error {
	current, err := tx.Account(v.login.AccountID).Login()
	if errors.Is(err, storage.ErrNoAccount) || errors.Is(err, storage.ErrNotFound) {
		return errBadCreds
	}
	if err != nil {
		return err
	}
	if current.CredentialsVersion != v.login.CredentialsVersion {
		return errBadCreds
	}
	if current.State != "active" {
		return errBadCreds // as if the password were wrong (no token, 401)
	}
	if v.rehash != "" {
		return tx.Account(v.login.AccountID).RehashPassword(v.rehash, v.login.CredentialsVersion)
	}
	return nil
}

func loginAccountJSON(l storage.Login) map[string]any {
	return map[string]any{"account_id": l.AccountID, "namespace": l.Namespace, "username": l.Username, "is_admin": l.IsAdmin}
}

func (req *request) signInDevice() (int, any, error) {
	var body struct {
		Username      string     `json:"username"`
		Password      string     `json:"password"`
		InstallID     string     `json:"install_id"`
		InstanceNonce string     `json:"instance_nonce"`
		Device        deviceInfo `json:"device"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	if err := checkUUID(body.InstallID, "install_id"); err != nil {
		return 0, nil, err
	}
	if err := checkUUID(body.InstanceNonce, "instance_nonce"); err != nil {
		return 0, nil, err
	}
	caps, err := body.Device.check("ios", "android")
	if err != nil {
		return 0, nil, err
	}
	v, err := req.verifyPassword(body.Username, body.Password)
	if err != nil {
		return 0, nil, err
	}
	secret, hash := auth.NewCredential()
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if err := v.recheck(tx); err != nil {
			return err
		}
		acct := tx.Account(v.login.AccountID)
		if err := attachInstallation(acct, storage.Installation{InstallID: body.InstallID, InstanceNonce: body.InstanceNonce,
			Name: body.Device.Name, Platform: body.Device.Platform, AppVersion: body.Device.AppVersion, Capabilities: caps}); err != nil {
			return err
		}
		if err := acct.DeleteInstallationCredentials(body.InstallID); err != nil {
			return err
		}
		return acct.InsertCredential(hash, "device", body.InstallID, nil)
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"token": secret, "install_id": body.InstallID, "account": loginAccountJSON(v.login)}, nil
}

// attachInstallation registers a new installation or resumes a known one with
// the same instance nonce; another nonce means a copy (409, nothing changes).
func attachInstallation(acct *storage.AccountTx, inst storage.Installation) error {
	existing, err := acct.Installation(inst.InstallID)
	if errors.Is(err, storage.ErrNotFound) {
		return acct.InsertInstallation(inst)
	}
	if err != nil {
		return err
	}
	if existing.InstanceNonce != inst.InstanceNonce {
		return fail(http.StatusConflict, "installation_conflict", "This installation ID belongs to another device; create a new installation.")
	}
	if existing.SupersededBy != "" {
		return fail(http.StatusConflict, "installation_superseded", "This installation was replaced; use its successor.")
	}
	if existing.Platform != inst.Platform {
		return invalid("This installation belongs to another platform.")
	}
	if err := acct.ResumeInstallation(inst.InstallID); err != nil {
		return err
	}
	return acct.UpdateInstallationDevice(inst.InstallID, inst.Name, inst.AppVersion, inst.Capabilities)
}

func (req *request) signInWeb() (int, any, error) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	v, err := req.verifyPassword(body.Username, body.Password)
	if err != nil {
		return 0, nil, err
	}
	secret, hash := auth.NewCredential()
	expires := req.s.cfg.Now().Add(sessionLifetime).UnixMilli()
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if err := v.recheck(tx); err != nil {
			return err
		}
		return tx.Account(v.login.AccountID).InsertCredential(hash, "web", "", &expires)
	})
	if err != nil {
		return 0, nil, err
	}
	// The browser's earlier sessions go; the new one has a cookie of its own.
	req.clearPresentedSessionCookies()
	req.issueSessionCookie(secret)
	return http.StatusOK, map[string]any{"account": loginAccountJSON(v.login)}, nil
}

func (req *request) signOut() (int, any, error) {
	if err := req.decodeEmpty(); err != nil {
		return 0, nil, err
	}
	p := req.principal
	if p.viaWeb && req.s.cfg.beforeSignOut != nil {
		req.s.cfg.beforeSignOut()
	}
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		// Signing out deletes the caller's own credential (already gone if a
		// successor's first use revoked it meanwhile); it needs no other right.
		acct := tx.Account(p.accountID())
		if err := acct.DeleteCredential(p.cred.Hash); err != nil {
			return err
		}
		if !p.viaWeb || p.cred.InstallID == "" {
			return nil
		}
		// A browser's sign-out also ends its handoffs, used or not, from the
		// installation its session was bound to when it was authenticated, so a
		// successor's answer that arrives later signs nothing back in
		// (PROTOCOL §5.4). Never device tokens, never a copy.
		return acct.DeleteWebSessionsOnSupersessionPath(p.cred.InstallID)
	})
	if err != nil {
		return 0, nil, err
	}
	if p.viaWeb {
		req.clearPresentedSessionCookies()
	}
	return http.StatusOK, nil, nil
}

// ---- installations

func installationJSON(i storage.Installation, current string) map[string]any {
	var caps []string
	_ = json.Unmarshal([]byte(i.Capabilities), &caps)
	out := map[string]any{"install_id": i.InstallID, "name": i.Name, "platform": i.Platform, "app_version": i.AppVersion,
		"capabilities": caps, "created_at": i.CreatedAt, "last_seen_at": i.LastSeenAt, "revoked": i.RevokedAt != nil,
		"superseded": i.SupersededBy != "", "current": i.InstallID == current}
	return out
}

func (req *request) listInstallations() (int, any, error) {
	var list []storage.Installation
	// The caller is checked again in the read that serves the list.
	err := req.read(func(r *storage.ReadTx) error {
		var err error
		list, err = r.Account(req.principal.accountID()).Installations()
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	out := []any{}
	for _, i := range list {
		out = append(out, installationJSON(i, req.principal.cred.InstallID))
	}
	return http.StatusOK, map[string]any{"installations": out}, nil
}

func (req *request) registerWeb() (int, any, error) {
	var body struct {
		InstallID     string   `json:"install_id"`
		InstanceNonce string   `json:"instance_nonce"`
		Name          string   `json:"name"`
		Platform      string   `json:"platform"`
		AppVersion    string   `json:"app_version"`
		Capabilities  []string `json:"capabilities"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	if err := checkUUID(body.InstallID, "install_id"); err != nil {
		return 0, nil, err
	}
	if err := checkUUID(body.InstanceNonce, "instance_nonce"); err != nil {
		return 0, nil, err
	}
	if body.Platform == "" {
		body.Platform = "web"
	}
	caps, err := deviceInfo{Name: body.Name, Platform: body.Platform, AppVersion: body.AppVersion, Capabilities: body.Capabilities}.check("web")
	if err != nil {
		return 0, nil, err
	}
	p := req.principal
	if req.s.cfg.beforeIssue != nil {
		req.s.cfg.beforeIssue()
	}
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		cred, err := req.revalidate(tx)
		if err != nil {
			return err
		}
		acct := tx.Account(p.accountID())
		if cred.InstallID != "" && cred.InstallID != body.InstallID {
			return fail(http.StatusConflict, "installation_conflict", "This browser session already belongs to another installation.")
		}
		recovered := false
		if cred.InstallID == "" {
			if recovered, err = bindSuperseded(acct, body.InstallID, body.InstanceNonce, req.s.cfg.Now().UnixMilli()); err != nil {
				return err
			}
		}
		if !recovered {
			if err := attachInstallation(acct, storage.Installation{InstallID: body.InstallID, InstanceNonce: body.InstanceNonce,
				Name: body.Name, Platform: "web", AppVersion: body.AppVersion, Capabilities: caps}); err != nil {
				return err
			}
		}
		if cred.InstallID == "" {
			// Resuming replaces the installation's earlier sessions, as for devices.
			if err := acct.DeleteInstallationCredentialsOfKind(body.InstallID, "web"); err != nil {
				return err
			}
			return acct.BindCredential(cred.Hash, body.InstallID)
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"install_id": body.InstallID}, nil
}

// bindSuperseded reports whether a new browser session may bind to a web
// installation its successor superseded, as a superseded credential that
// may only repeat the handoff or sign out (PROTOCOL §5.4). That is the way
// back for a browser whose handoff a sign-out cancelled, or whose handoff
// answer was lost before it signed out: the installation is superseded by a
// same-nonce successor that is either never used, or has no web session left.
// In the second case the successor's next use revokes the bound session
// again, as a first use does: its predecessor is armed again here. A
// successor session expired at now (the transaction's time) is no session
// left: it is deleted here, as when it is presented, since a browser stops
// sending an expired cookie.
func bindSuperseded(acct *storage.AccountTx, installID, nonce string, now int64) (bool, error) {
	old, err := acct.Installation(installID)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if old.SupersededBy == "" || old.InstanceNonce != nonce || old.Platform != "web" || old.RevokedAt != nil {
		return false, nil // attachInstallation answers as for any registration
	}
	next, err := acct.Installation(old.SupersededBy)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if next.InstanceNonce != old.InstanceNonce || next.RevokedAt != nil {
		return false, nil
	}
	if next.PendingPredecessor == old.InstallID {
		return true, nil // never used: its first use revokes the bound session
	}
	if err := acct.DeleteExpiredWebSessions(next.InstallID, now); err != nil {
		return false, err
	}
	live, err := acct.HasCredentialsOfKind(next.InstallID, "web")
	if err != nil || live {
		return false, err
	}
	return true, acct.RearmPendingPredecessor(next.InstallID, old.InstallID)
}

func (req *request) successor() (int, any, error) {
	var body struct {
		InstallID     string     `json:"install_id"`
		InstanceNonce string     `json:"instance_nonce"`
		Device        deviceInfo `json:"device"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	if err := checkUUID(body.InstallID, "install_id"); err != nil {
		return 0, nil, err
	}
	if err := checkUUID(body.InstanceNonce, "instance_nonce"); err != nil {
		return 0, nil, err
	}
	p := req.principal
	old := p.cred.Installation
	body.Device.Platform = old.Platform
	caps, err := body.Device.check(old.Platform)
	if err != nil {
		return 0, nil, err
	}
	if body.InstallID == old.InstallID {
		return 0, nil, invalid("A successor needs a new installation ID.")
	}
	if req.s.cfg.beforeIssue != nil {
		req.s.cfg.beforeIssue()
	}
	kind := "device"
	var expires *int64
	if p.viaWeb {
		kind = "web"
		v := req.s.cfg.Now().Add(sessionLifetime).UnixMilli()
		expires = &v
	}
	secret, hash := auth.NewCredential()
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		cred, err := req.revalidate(tx)
		if err != nil {
			return err
		}
		acct := tx.Account(p.accountID())
		authorizing := cred.Installation
		if authorizing.SupersededBy != "" && authorizing.SupersededBy != body.InstallID {
			return fail(http.StatusConflict, "installation_superseded", "This installation was already replaced by another one.")
		}
		existing, err := acct.Installation(body.InstallID)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			if err := acct.InsertInstallation(storage.Installation{InstallID: body.InstallID, InstanceNonce: body.InstanceNonce,
				Name: body.Device.Name, Platform: authorizing.Platform, AppVersion: body.Device.AppVersion, Capabilities: caps,
				CreatedFrom: authorizing.InstallID}); err != nil {
				return err
			}
			if body.InstanceNonce == authorizing.InstanceNonce {
				if err := acct.Supersede(authorizing.InstallID, body.InstallID); err != nil {
					return err
				}
			}
		case err != nil:
			return err
		case existing.CreatedFrom != authorizing.InstallID || existing.InstanceNonce != body.InstanceNonce || existing.RevokedAt != nil:
			return fail(http.StatusConflict, "installation_conflict", "This installation ID is already in use.")
		default:
			// A repeated handoff: replace the successor's earlier credential of this kind.
			if err := acct.DeleteInstallationCredentialsOfKind(body.InstallID, kind); err != nil {
				return err
			}
		}
		return acct.InsertCredential(hash, kind, body.InstallID, expires)
	})
	if err != nil {
		return 0, nil, err
	}
	out := map[string]any{"install_id": body.InstallID}
	if p.viaWeb {
		req.issueSessionCookie(secret)
	} else {
		out["token"] = secret
	}
	return http.StatusOK, out, nil
}

func (req *request) updateSelf() (int, any, error) {
	var body struct {
		Name         *string   `json:"name"`
		AppVersion   *string   `json:"app_version"`
		Capabilities *[]string `json:"capabilities"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	p := req.principal
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		cred, err := req.revalidate(tx)
		if err != nil {
			return err
		}
		inst := *cred.Installation
		d := deviceInfo{Name: inst.Name, Platform: inst.Platform, AppVersion: inst.AppVersion}
		_ = json.Unmarshal([]byte(inst.Capabilities), &d.Capabilities)
		if body.Name != nil {
			d.Name = *body.Name
		}
		if body.AppVersion != nil {
			d.AppVersion = *body.AppVersion
		}
		if body.Capabilities != nil {
			d.Capabilities = *body.Capabilities
		}
		caps, err := d.check(inst.Platform)
		if err != nil {
			return err
		}
		return tx.Account(p.accountID()).UpdateInstallationDevice(inst.InstallID, d.Name, d.AppVersion, caps)
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, nil, nil
}

func (req *request) revoke() (int, any, error) {
	if err := req.decodeEmpty(); err != nil {
		return 0, nil, err
	}
	id := req.r.PathValue("id")
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if _, err := req.revalidate(tx); err != nil {
			return err
		}
		acct := tx.Account(req.principal.accountID())
		if _, err := acct.Installation(id); errors.Is(err, storage.ErrNotFound) {
			return errNotFound
		} else if err != nil {
			return err
		}
		return acct.RevokeInstallation(id)
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, nil, nil
}

// ---- passwords

func (req *request) changePassword() (int, any, error) {
	var body struct {
		Current string `json:"current"`
		New     string `json:"new"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	if err := checkPassword(body.New); err != nil {
		return 0, nil, err
	}
	p := req.principal
	var login storage.Login
	err := req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		var err error
		login, err = r.LoginByAccount(p.accountID())
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	v, err := req.verifyPassword(login.Username, body.Current)
	if errors.Is(err, errBadCreds) {
		return 0, nil, fail(http.StatusBadRequest, "password_incorrect", "The current password is incorrect.")
	}
	if err != nil {
		return 0, nil, err
	}
	hash, err := req.s.cfg.Hasher.Hash(req.ctx, body.New)
	if err != nil {
		return 0, nil, err
	}
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if _, err := req.revalidate(tx); err != nil {
			return err
		}
		v.rehash = ""
		if err := v.recheck(tx); err != nil {
			return err
		}
		return tx.Account(p.accountID()).SetPassword(hash, p.cred.Hash)
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, nil, nil
}

// ---- administration

func (req *request) adminList() (int, any, error) {
	var list []storage.AccountSummary
	// The administrator is checked again in the read.
	err := req.read(func(r *storage.ReadTx) error {
		var err error
		list, err = r.ListAccounts()
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	out := []any{}
	for _, a := range list {
		out = append(out, map[string]any{"account_id": a.AccountID, "username": a.Username, "is_admin": a.IsAdmin,
			"state": a.State, "created_at": a.CreatedAt})
	}
	return http.StatusOK, map[string]any{"accounts": out}, nil
}

func (req *request) adminCreate() (int, any, error) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		IsAdmin  bool   `json:"is_admin"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	key, err := usernameKey(body.Username)
	if err != nil {
		return 0, nil, err
	}
	if err := checkPassword(body.Password); err != nil {
		return 0, nil, err
	}
	hash, err := req.s.cfg.Hasher.Hash(req.ctx, body.Password)
	if err != nil {
		return 0, nil, err
	}
	var account storage.Account
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if _, err := req.revalidate(tx); err != nil {
			return err
		}
		if _, err := tx.LoginByUsername(key); err == nil {
			return fail(http.StatusConflict, "username_taken", "That username is already in use.")
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if account, err = tx.CreateAccount(); err != nil {
			return err
		}
		return tx.CreateLogin(account.ID, body.Username, key, hash, body.IsAdmin)
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusCreated, map[string]any{"account": accountJSON(account, body.Username, body.IsAdmin)}, nil
}

// lastAdminGuard refuses a change that would leave no active administrator.
func lastAdminGuard(tx *storage.WriteTx, target storage.Login, losesAdmin bool) error {
	if !losesAdmin || !target.IsAdmin || target.State != "active" {
		return nil
	}
	n, err := tx.CountActiveAdmins()
	if err != nil {
		return err
	}
	if n <= 1 {
		return fail(http.StatusConflict, "last_admin", "The last active administrator cannot be disabled, demoted or deleted.")
	}
	return nil
}

func (req *request) targetLogin(tx *storage.WriteTx) (*storage.AccountTx, storage.Login, error) {
	acct := tx.Account(req.r.PathValue("id"))
	login, err := acct.Login()
	if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNoAccount) {
		return nil, storage.Login{}, errNotFound
	}
	return acct, login, err
}

func (req *request) adminUpdate() (int, any, error) {
	var body struct {
		Username *string `json:"username"`
		State    *string `json:"state"`
		IsAdmin  *bool   `json:"is_admin"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	var key string
	if body.Username != nil {
		var err error
		if key, err = usernameKey(*body.Username); err != nil {
			return 0, nil, err
		}
	}
	if body.State != nil && *body.State != "active" && *body.State != "disabled" {
		return 0, nil, invalid("State is active or disabled.")
	}
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if _, err := req.revalidate(tx); err != nil {
			return err
		}
		acct, login, err := req.targetLogin(tx)
		if err != nil {
			return err
		}
		losesAdmin := (body.State != nil && *body.State == "disabled") || (body.IsAdmin != nil && !*body.IsAdmin)
		if err := lastAdminGuard(tx, login, losesAdmin); err != nil {
			return err
		}
		if body.Username != nil {
			if other, err := tx.LoginByUsername(key); err == nil && other.AccountID != login.AccountID {
				return fail(http.StatusConflict, "username_taken", "That username is already in use.")
			}
			if err := acct.Rename(*body.Username, key); err != nil {
				return err
			}
		}
		if body.State != nil {
			if err := acct.SetState(*body.State); err != nil {
				return err
			}
		}
		if body.IsAdmin != nil {
			return acct.SetAdmin(*body.IsAdmin)
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, nil, nil
}

func (req *request) adminResetPassword() (int, any, error) {
	var body struct {
		Password string `json:"password"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	if err := checkPassword(body.Password); err != nil {
		return 0, nil, err
	}
	hash, err := req.s.cfg.Hasher.Hash(req.ctx, body.Password)
	if err != nil {
		return 0, nil, err
	}
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if _, err := req.revalidate(tx); err != nil {
			return err
		}
		acct, login, err := req.targetLogin(tx)
		if err != nil {
			return err
		}
		var keep []byte
		if login.AccountID == req.principal.accountID() {
			keep = req.principal.cred.Hash
		}
		return acct.SetPassword(hash, keep)
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, nil, nil
}

func (req *request) adminDelete() (int, any, error) {
	var body struct {
		ConfirmUsername string `json:"confirm_username"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	var imports []string
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if _, err := req.revalidate(tx); err != nil {
			return err
		}
		_, login, err := req.targetLogin(tx)
		if err != nil {
			return err
		}
		if body.ConfirmUsername != login.Username {
			return invalid("Type the account's username to confirm deletion.")
		}
		if err := lastAdminGuard(tx, login, true); err != nil {
			return err
		}
		// The account's imports go with it, rows here and files below.
		if imports, err = tx.Account(login.AccountID).ImportIDs(); err != nil {
			return err
		}
		return tx.DeleteAccount(login.AccountID)
	})
	if err != nil {
		return 0, nil, err
	}
	if req.s.cfg.Imports != nil && !req.s.cfg.Imports.Discard(imports) {
		// The account is gone; some of its import files are not yet: the
		// runner retries them and the service page lists them.
		return http.StatusOK, map[string]any{"cleanup_pending": true}, nil
	}
	return http.StatusOK, nil, nil
}
