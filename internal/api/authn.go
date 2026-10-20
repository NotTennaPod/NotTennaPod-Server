package api

import (
	"bytes"
	"errors"
	"net/http"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// authenticate resolves the request's credential to a principal and applies the
// endpoint's credential rules (authorization matrix). A bearer token takes
// precedence; otherwise the request's session cookies are chosen among
// (chooseSession).
func (req *request) authenticate(e endpoint) error {
	if h := req.r.Header.Get("Authorization"); h != "" {
		token, ok := strings.CutPrefix(h, "Bearer ")
		if !ok || !auth.WellFormed(token) {
			return errUnauthorized
		}
		var cred storage.Credential
		err := req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
			var err error
			cred, err = r.CredentialByHash(auth.HashCredential(token))
			return err
		})
		if errors.Is(err, storage.ErrNotFound) {
			return errUnauthorized
		}
		if err != nil {
			return err
		}
		if cred.Kind != "device" {
			return errUnauthorized // a session secret in a header
		}
		switch cred.Refusal(req.s.cfg.Now().UnixMilli()) {
		case storage.CredentialSignedOut:
			return errUnauthorized
		case storage.CredentialInactive:
			return errForbidden
		}
		return req.admit(e, &principal{cred: cred, secret: token})
	}
	candidates := req.sessionCandidates()
	if len(candidates) == 0 {
		return errUnauthorized
	}
	// A cookie authenticates only browser requests: state changes need our Origin.
	if req.r.Method != http.MethodGet && req.r.Method != http.MethodHead && req.r.Header.Get("Origin") == "" {
		return fail(http.StatusForbidden, "forbidden", "This request must come from the NotTennaPod web app.")
	}
	p, err := req.chooseSession(candidates)
	if err != nil {
		return err
	}
	return req.admit(e, p)
}

// admit applies the endpoint's credential rules to the chosen credential.
func (req *request) admit(e endpoint, p *principal) error {
	cred, viaWeb := p.cred, p.viaWeb
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
	return req.afterAuthentication(e)
}

func (e endpoint) allowSuperseded() bool { return e.supersededOK }

// sessionCandidate is one session cookie the request carries.
type sessionCandidate struct {
	name   string // the cookie's name
	secret string
	hash   []byte
}

// sessionCandidates lists every session cookie the request carries, each
// session once (PROTOCOL §5.4). A cookie whose value is not a credential, or
// whose own name does not match its value's hash, is ignored.
func (req *request) sessionCandidates() []sessionCandidate {
	var out []sessionCandidate
	index := map[string]int{}
	for _, c := range req.r.Cookies() {
		if !isSessionCookie(c.Name) || !auth.WellFormed(c.Value) {
			continue
		}
		hash := auth.HashCredential(c.Value)
		if c.Name != legacySessionCookie && c.Name != sessionCookieName(hash) {
			continue
		}
		if i, ok := index[string(hash)]; ok {
			// The same session under both names: renewed under its own.
			if out[i].name == legacySessionCookie {
				out[i].name = c.Name
			}
			continue
		}
		index[string(hash)] = len(out)
		out = append(out, sessionCandidate{name: c.Name, secret: c.Value, hash: hash})
	}
	return out
}

// chooseSession chooses the session a browser request acts as, among every
// session cookie it carries (PROTOCOL §5.4):
//
//   - the usable ones are those Credential.Refusal accepts, a superseded one
//     included (it may repeat its handoff or sign out); with
//     X-NotTennaPod-Account, only that account's (none of it but another's
//     usable: 409 account_changed);
//   - of them, each whose installation has a successor or copy (following
//     created_from) bound to another usable one is dropped;
//   - the rest's latest issued (created_at, then the larger hash) is chosen.
//
// Both steps are functions of the set, so the order the cookies come in does
// not matter. A cookie is cleared only once a write transaction has found its
// session absent, or has deleted it as expired there (unless a renewal won);
// a database error while deciding that refuses the request and clears nothing
// (a later error leaves the clears of sessions already known gone).
func (req *request) chooseSession(candidates []sessionCandidate) (*principal, error) {
	hashes := make([][]byte, len(candidates))
	for i, c := range candidates {
		hashes[i] = c.hash
	}
	var creds map[string]storage.Credential
	ancestors := map[string][]string{} // by string(hash): the installations its own descends from
	err := req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		var err error
		if creds, err = r.CredentialsByHashes(hashes); err != nil {
			return err
		}
		bound := 0
		for _, c := range creds {
			if c.Installation != nil {
				bound++
			}
		}
		if bound < 2 {
			return nil
		}
		for h, c := range creds {
			if c.Installation == nil {
				continue
			}
			if ancestors[h], err = r.Account(c.AccountID).CreatedFromAncestors(c.InstallID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	now := req.s.cfg.Now().UnixMilli()
	usable := map[string]storage.Credential{}
	var absent, expired []sessionCandidate
	inactive := false
	for _, c := range candidates {
		cred, ok := creds[string(c.hash)]
		switch {
		case !ok:
			absent = append(absent, c)
		case cred.Kind != "web":
			// a device token in a cookie authenticates nothing
		case cred.ExpiresAt != nil && now >= *cred.ExpiresAt:
			expired = append(expired, c)
		default:
			switch cred.Refusal(now) {
			case storage.CredentialUsable, storage.CredentialSuperseded:
				usable[string(c.hash)] = cred
			case storage.CredentialInactive:
				inactive = true
			}
		}
	}
	if req.s.cfg.afterSessionRead != nil {
		req.s.cfg.afterSessionRead()
	}
	if len(absent) > 0 || len(expired) > 0 {
		// Whether a session is gone for good is decided in a write
		// transaction, by its own clock: an expired one is deleted there
		// unless a renewal won meanwhile.
		var gone []string
		err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
			gone = gone[:0]
			at := req.s.cfg.Now().UnixMilli()
			for _, c := range absent {
				if _, err := tx.CredentialByHash(c.hash); errors.Is(err, storage.ErrNotFound) {
					gone = append(gone, c.name)
				} else if err != nil {
					return err
				}
			}
			for _, c := range expired {
				deleted, err := tx.DeleteIfExpired(c.hash, at)
				if err != nil {
					return err
				}
				if deleted {
					gone = append(gone, c.name)
					continue
				}
				cred, err := tx.CredentialByHash(c.hash)
				if errors.Is(err, storage.ErrNotFound) {
					gone = append(gone, c.name)
					continue
				}
				if err != nil {
					return err
				}
				switch cred.Refusal(at) {
				case storage.CredentialUsable, storage.CredentialSuperseded:
					usable[string(c.hash)] = cred // renewed meanwhile
				case storage.CredentialInactive:
					inactive = true
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, name := range gone {
			req.clearCookie(name)
		}
	}
	if want := req.r.Header.Get("X-NotTennaPod-Account"); want != "" {
		other := len(usable) > 0
		for h, c := range usable {
			if c.AccountID != want {
				delete(usable, h)
			}
		}
		if len(usable) == 0 && other {
			return nil, errAccountChanged
		}
	}
	if len(usable) == 0 {
		if inactive {
			return nil, errForbidden
		}
		return nil, errUnauthorized
	}
	// Drop each session whose installation has a descendant bound to another
	// usable session.
	dropped := map[string]bool{}
	for h, c := range usable {
		if c.Installation == nil {
			continue
		}
		for _, anc := range ancestors[h] {
			for h2, t := range usable {
				if h2 != h && t.AccountID == c.AccountID && t.InstallID == anc {
					dropped[h2] = true
				}
			}
		}
	}
	var chosen storage.Credential
	found := false
	for h, c := range usable {
		if dropped[h] {
			continue
		}
		if !found || c.CreatedAt > chosen.CreatedAt || (c.CreatedAt == chosen.CreatedAt && bytes.Compare(c.Hash, chosen.Hash) > 0) {
			chosen, found = c, true
		}
	}
	if !found {
		return nil, errUnauthorized // unreachable: created_from is acyclic
	}
	for _, c := range candidates {
		if bytes.Equal(c.hash, chosen.Hash) {
			return &principal{cred: chosen, viaWeb: true, secret: c.secret, cookie: c.name}, nil
		}
	}
	return nil, errUnauthorized
}

// afterAuthentication performs the rare writes that authentication implies:
// revoking a predecessor once its successor's credential is used, recording
// activity at most once a minute, and extending a session at most once an hour.
// The write decides by its own clock: a session expired by then is deleted
// and refused, never extended, and an extension never shortens the expiry.
func (req *request) afterAuthentication(e endpoint) error {
	now := req.s.cfg.Now()
	cred := req.principal.cred
	inst := cred.Installation
	finish := inst != nil && inst.PendingPredecessor != "" && !e.successor
	touchInst := inst != nil && now.UnixMilli()-inst.LastSeenAt >= touchInterval.Milliseconds()
	extend := cred.Kind == "web" && !e.endsSession && now.UnixMilli()-cred.LastUsedAt >= sessionExtend.Milliseconds()
	if !finish && !touchInst && !extend {
		return nil
	}
	gone, expired, renewed := false, false, false
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		gone, expired, renewed = false, false, false
		at := req.s.cfg.Now().UnixMilli()
		// Re-check inside the transaction: the credential may have been revoked meanwhile.
		current, err := tx.CredentialByHash(cred.Hash)
		if errors.Is(err, storage.ErrNotFound) {
			gone = true
			return nil
		}
		if err != nil {
			return err // refused (503), nothing cleared
		}
		if !bytes.Equal(current.Hash, cred.Hash) {
			return errUnauthorized
		}
		if current.ExpiresAt != nil && at >= *current.ExpiresAt {
			expired = true
			gone, err = tx.DeleteIfExpired(cred.Hash, at)
			return err
		}
		acct := tx.Account(cred.AccountID)
		if extend {
			to := at + sessionLifetime.Milliseconds()
			if renewed, err = acct.TouchCredential(cred.Hash, at, &to); err != nil {
				return err
			}
		}
		if finish {
			if err := acct.FinishSupersession(inst.InstallID); err != nil {
				return err
			}
		}
		if touchInst {
			if err := acct.TouchInstallation(inst.InstallID, at); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if gone || expired {
		if gone && req.principal.viaWeb {
			req.clearCookie(req.principal.cookie)
		}
		req.principal = nil
		return errUnauthorized
	}
	if renewed && req.principal.viaWeb {
		// The browser keeps the cookie only for its Max-Age: renew it with
		// the session, under the name it came in.
		req.setSessionCookie(req.principal.cookie, req.principal.secret)
	}
	return nil
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
