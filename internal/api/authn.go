package api

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// authenticate resolves the request's credential to a principal and applies the
// endpoint's credential rules (authorization matrix).
func (req *request) authenticate(e endpoint) error {
	secret, viaWeb := "", false
	if h := req.r.Header.Get("Authorization"); h != "" {
		token, ok := strings.CutPrefix(h, "Bearer ")
		if !ok {
			return errUnauthorized
		}
		secret = token
	} else if c, err := req.r.Cookie(sessionCookie); err == nil {
		secret, viaWeb = c.Value, true
	}
	if secret == "" || !auth.WellFormed(secret) {
		return errUnauthorized
	}
	// A cookie authenticates only browser requests: state changes need our Origin.
	if viaWeb && req.r.Method != http.MethodGet && req.r.Method != http.MethodHead && req.r.Header.Get("Origin") == "" {
		return fail(http.StatusForbidden, "forbidden", "This request must come from the NotTennaPod web app.")
	}
	hash := auth.HashCredential(secret)
	var cred storage.Credential
	err := req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		var err error
		cred, err = r.CredentialByHash(hash)
		return err
	})
	if errors.Is(err, storage.ErrNotFound) {
		return errUnauthorized
	}
	if err != nil {
		return err
	}
	now := req.s.cfg.Now()
	if (cred.Kind == "web") != viaWeb {
		return errUnauthorized // a session secret in a header or a token in a cookie
	}
	if cred.ExpiresAt != nil && now.UnixMilli() >= *cred.ExpiresAt {
		return errUnauthorized
	}
	if cred.AccountState != "active" {
		return errForbidden
	}
	if cred.Installation != nil && cred.Installation.RevokedAt != nil {
		return errUnauthorized
	}
	p := &principal{cred: cred, viaWeb: viaWeb, secret: secret}
	switch e.auth {
	case authDevice:
		if viaWeb {
			return errUnauthorized
		}
	case authWeb:
		if !viaWeb {
			return errUnauthorized
		}
	case authAdmin:
		if !viaWeb || !cred.IsAdmin {
			return errForbidden
		}
	case authSync:
		if cred.Installation == nil {
			return fail(http.StatusForbidden, "forbidden", "Register this browser before syncing.")
		}
	case authRead:
		if !viaWeb {
			return errUnauthorized
		}
		if cred.Installation == nil {
			return fail(http.StatusForbidden, "forbidden", "Register this browser before syncing.")
		}
	}
	// A browser tab names the account it shows; a tab of another account
	// reloads (PROTOCOL §12).
	if want := req.r.Header.Get("X-NotTennaPod-Account"); want != "" && viaWeb && want != cred.AccountID {
		return errAccountChanged
	}
	// A superseded installation may only repeat its successor handoff or sign out.
	superseded := cred.Installation != nil && cred.Installation.SupersededBy != ""
	if superseded && !e.allowSuperseded() {
		return fail(http.StatusConflict, "installation_superseded", "This installation was replaced; use its successor.")
	}
	req.principal = p
	return req.afterAuthentication(now, e)
}

func (e endpoint) allowSuperseded() bool { return e.supersededOK }

// afterAuthentication performs the rare writes that authentication implies:
// revoking a predecessor once its successor's credential is used, recording
// activity at most once a minute, and extending a session at most once an hour.
func (req *request) afterAuthentication(now time.Time, e endpoint) error {
	cred := req.principal.cred
	inst := cred.Installation
	finish := inst != nil && inst.PendingPredecessor != "" && !e.successor
	touchInst := inst != nil && now.UnixMilli()-inst.LastSeenAt >= touchInterval.Milliseconds()
	var extend *int64
	if cred.Kind == "web" && now.UnixMilli()-cred.LastUsedAt >= sessionExtend.Milliseconds() {
		v := now.Add(sessionLifetime).UnixMilli()
		extend = &v
	}
	if !finish && !touchInst && extend == nil {
		return nil
	}
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		// Re-check inside the transaction: the credential may have been revoked meanwhile.
		current, err := tx.CredentialByHash(cred.Hash)
		if err != nil || !bytes.Equal(current.Hash, cred.Hash) {
			return errUnauthorized
		}
		acct := tx.Account(cred.AccountID)
		if finish {
			if err := acct.FinishSupersession(inst.InstallID); err != nil {
				return err
			}
		}
		if touchInst {
			if err := acct.TouchInstallation(inst.InstallID, now.UnixMilli()); err != nil {
				return err
			}
		}
		if extend != nil {
			return acct.TouchCredential(cred.Hash, now.UnixMilli(), extend)
		}
		return nil
	})
	if err == nil && extend != nil && req.principal.viaWeb {
		// The browser keeps the cookie only for its Max-Age: renew it with the session.
		req.setSessionCookie(req.principal.secret)
	}
	return err
}

// revalidate re-applies every authorization rule of the endpoint to the
// credential as it is now, inside the handler's write transaction. Every
// authenticated write calls it first, so a reset, revocation, disable,
// demotion or supersession that landed after authentication always wins.
func (req *request) revalidate(tx *storage.WriteTx) (storage.Credential, error) {
	return req.recheck(tx.CredentialByHash(req.principal.cred.Hash))
}

// revalidateRead is revalidate inside a read transaction: a handler that
// serves archived content (a recovery snapshot) checks the credential in the
// same snapshot it reads.
func (req *request) revalidateRead(tx *storage.ReadTx) (storage.Credential, error) {
	return req.recheck(tx.CredentialByHash(req.principal.cred.Hash))
}

func (req *request) recheck(cred storage.Credential, err error) (storage.Credential, error) {
	if errors.Is(err, storage.ErrNotFound) {
		return storage.Credential{}, errUnauthorized
	}
	if err != nil {
		return storage.Credential{}, err
	}
	// The rules are storage's (Credential.Refusal), which a recovery job's
	// publication applies to its stored credential too.
	refusal := cred.Refusal(req.s.cfg.Now().UnixMilli())
	switch refusal {
	case storage.CredentialSignedOut:
		return storage.Credential{}, errUnauthorized
	case storage.CredentialInactive:
		return storage.Credential{}, errForbidden
	}
	if req.endpoint.auth == authAdmin && !cred.IsAdmin {
		return storage.Credential{}, errForbidden
	}
	if refusal == storage.CredentialSuperseded && !req.endpoint.supersededOK {
		return storage.Credential{}, fail(http.StatusConflict, "installation_superseded", "This installation was replaced; use its successor.")
	}
	return cred, nil
}
