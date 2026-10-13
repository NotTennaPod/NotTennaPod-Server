package api

import (
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func (s *Server) syncRoutes() {
	if s.cfg.Protocol == nil {
		return
	}
	s.route("/v1/push", endpoint{method: http.MethodPost, auth: authSync, maxBody: protocol.MaxRequestBytes, handle: (*request).push})
	s.route("/v1/pull", endpoint{method: http.MethodPost, auth: authSync, handle: (*request).pull})
	s.route("/v1/snapshot", endpoint{method: http.MethodPost, auth: authSync, handle: (*request).snapshot})
	s.route("/v1/secrets/{id}", endpoint{method: http.MethodGet, auth: authSync, handle: (*request).secret})
	s.route("/v1/refresh", endpoint{method: http.MethodPost, auth: authSync, handle: (*request).refresh})
	s.recoveryRoutes()
}

// readProtocolBody reads the whole body and decodes it strictly (PROTOCOL §3.1).
func (req *request) readProtocolBody() (map[string]any, error) {
	data, err := io.ReadAll(req.r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "The request is too large; frame large operations.")
		}
		return nil, invalid("The request body could not be read.")
	}
	v, err := contract.DecodeJSON(data)
	if err != nil {
		return nil, invalid("The request body is not valid protocol JSON.")
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, invalid("The request body must be an object.")
	}
	return obj, nil
}

func onlyFields(obj map[string]any, allowed ...string) error {
	for k := range obj {
		ok := false
		for _, a := range allowed {
			if k == a {
				ok = true
			}
		}
		if !ok {
			return invalid("Unknown field: " + k)
		}
	}
	return nil
}

func intField(obj map[string]any, k string, required bool) (int64, error) {
	v, present := obj[k]
	if !present {
		if required {
			return 0, invalid("Missing field: " + k)
		}
		return 0, nil
	}
	n, ok := v.(interface{ String() string })
	if !ok {
		return 0, invalid(k + " must be an integer.")
	}
	var x int64
	for _, c := range n.String() {
		if c < '0' || c > '9' {
			return 0, invalid(k + " must be a non-negative integer.")
		}
		x = x*10 + int64(c-'0')
	}
	return x, nil
}

func (req *request) syncCommon(obj map[string]any) (installID string, acked int64, err error) {
	installID, _ = obj["install_id"].(string)
	if installID != req.principal.cred.InstallID {
		return "", 0, fail(http.StatusForbidden, "forbidden", "install_id is not this credential's installation.")
	}
	acked, err = intField(obj, "acked_install_seq", true)
	return installID, acked, err
}

func cursorField(obj map[string]any) (contract.Cursor, error) {
	s, _ := obj["cursor"].(string)
	c, err := contract.DecodeCursor(s)
	if err != nil {
		return contract.Cursor{}, invalid("The cursor is not valid.")
	}
	return c, nil
}

func protocolError(err error) error {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		e := fail(pe.Status, pe.Code, pe.Message)
		e.retryAfter = pe.RetryAfter
		return e
	}
	return err
}

func (req *request) authorizeSync() protocol.Authorize {
	return func(tx *storage.WriteTx) (*storage.Installation, error) {
		cred, err := req.revalidate(tx)
		if err != nil {
			return nil, err
		}
		if cred.Installation == nil {
			return nil, errForbidden
		}
		return cred.Installation, nil
	}
}

// authorizeSyncRead is authorizeSync inside a read transaction.
func (req *request) authorizeSyncRead() protocol.AuthorizeRead {
	return func(tx *storage.ReadTx) (*storage.Installation, error) {
		cred, err := req.revalidateRead(tx)
		if err != nil {
			return nil, err
		}
		if cred.Installation == nil {
			return nil, errForbidden
		}
		return cred.Installation, nil
	}
}

func (req *request) push() (int, any, error) {
	obj, err := req.readProtocolBody()
	if err != nil {
		return 0, nil, err
	}
	if err := onlyFields(obj, "cursor", "install_id", "acked_install_seq", "operations", "secrets"); err != nil {
		return 0, nil, err
	}
	installID, acked, err := req.syncCommon(obj)
	if err != nil {
		return 0, nil, err
	}
	cursor, err := cursorField(obj)
	if err != nil {
		return 0, nil, err
	}
	list, ok := obj["operations"].([]any)
	if !ok {
		return 0, nil, invalid("operations must be a list.")
	}
	if len(list) > protocol.MaxOperations {
		return 0, nil, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "Too many operations in one push; send fewer at a time.")
	}
	preq := protocol.PushRequest{AccountID: req.principal.accountID(), Cursor: cursor, InstallID: installID, AckedInstallSeq: acked}
	if raw, present := obj["secrets"]; present {
		if preq.Secrets, err = parseSecrets(raw); err != nil {
			return 0, nil, err
		}
	}
	var last int64
	for i, item := range list {
		op, ok := item.(map[string]any)
		if !ok {
			return 0, nil, invalid("Each operation must be an object.")
		}
		seq, err := intField(op, "install_seq", true)
		if err != nil || seq < 1 || (i > 0 && seq != last+1) {
			return 0, nil, invalid("install_seq must be contiguous and ascending from 1.")
		}
		last = seq
		pop := protocol.PushOp{InstallSeq: seq}
		factObj, hasFact := op["fact"]
		partObj, hasPart := op["part"].(map[string]any)
		switch {
		case hasFact && !hasPart && len(op) == 2:
			if contract.Depth(factObj) > protocol.MaxFactDepth {
				return 0, nil, invalid("A fact nests deeper than the advertised bound.")
			}
			archived, err := protocol.ParseFact(factObj)
			if err != nil {
				return 0, nil, invalid("An operation's fact cannot be archived: " + err.Error())
			}
			if len(archived.Canonical) > protocol.MaxUnframedFact {
				return 0, nil, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "Facts above the advertised size must be framed as parts.")
			}
			pop.Fact = archived
		case hasPart && !hasFact && len(op) == 2:
			part, err := protocol.DecodePart(partObj)
			if err != nil {
				return 0, nil, protocolError(err)
			}
			pop.Part = part
		default:
			return 0, nil, invalid("Each operation has install_seq and exactly one of fact or part.")
		}
		preq.Operations = append(preq.Operations, pop)
	}
	resp, err := req.s.cfg.Protocol.Push(req.ctx, preq, req.authorizeSync())
	if err != nil {
		return 0, nil, protocolError(err)
	}
	return http.StatusOK, resp, nil
}

// Bounds of one secret.
const (
	maxSecretField   = 4096 // bytes of a username or a password
	maxSecretOrigins = 16
)

// parseSecrets decodes the credentials sent beside facts (PROTOCOL §7.2): push and recovery upload.
//
// A request carries at most as many secrets as operations, each of bounded
// size, checked here before any transaction: sealing them holds the
// writer.
func parseSecrets(raw any) ([]protocol.Secret, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, invalid("secrets must be a list.")
	}
	if len(list) > protocol.MaxOperations {
		return nil, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "Too many secrets in one request; send fewer at a time.")
	}
	var out []protocol.Secret
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok || onlyFields(m, "secret_id", "username", "password", "approved_origins") != nil {
			return nil, invalid("Each secret is {secret_id, username, password[, approved_origins]}.")
		}
		id, _ := m["secret_id"].(string)
		user, ok1 := m["username"].(string)
		pass, ok2 := m["password"].(string)
		if !contract.ValidUUID(id) || !ok1 || !ok2 {
			return nil, invalid("Each secret is {secret_id, username, password[, approved_origins]}.")
		}
		if len(user) > maxSecretField || len(pass) > maxSecretField {
			return nil, invalid("A secret's username and password are at most 4096 bytes each.")
		}
		sec := protocol.Secret{ID: id, Username: user, Password: pass}
		if raw, present := m["approved_origins"]; present {
			origins, ok := raw.([]any)
			if !ok || len(origins) > maxSecretOrigins {
				return nil, invalid("approved_origins must be a list of at most 16 origins.")
			}
			sec.ApprovedOrigins = []string{}
			for _, o := range origins {
				s, ok := o.(string)
				if !ok || !validOrigin(s) {
					return nil, invalid("Each approved origin is scheme://host:port, lower case, http or https.")
				}
				sec.ApprovedOrigins = append(sec.ApprovedOrigins, s)
			}
		}
		out = append(out, sec)
	}
	return out, nil
}

func (req *request) pull() (int, any, error) {
	obj, err := req.readProtocolBody()
	if err != nil {
		return 0, nil, err
	}
	if err := onlyFields(obj, "cursor", "install_id", "acked_install_seq", "page", "limit"); err != nil {
		return 0, nil, err
	}
	installID, acked, err := req.syncCommon(obj)
	if err != nil {
		return 0, nil, err
	}
	cursor, err := cursorField(obj)
	if err != nil {
		return 0, nil, err
	}
	page, _ := obj["page"].(string)
	limit, err := intField(obj, "limit", false)
	if err != nil {
		return 0, nil, err
	}
	resp, err := req.s.cfg.Protocol.Pull(req.ctx, protocol.PullRequest{AccountID: req.principal.accountID(), Cursor: cursor,
		InstallID: installID, AckedInstallSeq: acked, Page: page, Limit: int(limit), AuthorizeRead: req.authorizeSyncRead()})
	if err != nil {
		return 0, nil, protocolError(err)
	}
	body, err := resp.Body()
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, encodedBody(body), nil
}

func (req *request) snapshot() (int, any, error) {
	obj, err := req.readProtocolBody()
	if err != nil {
		return 0, nil, err
	}
	if err := onlyFields(obj, "mode", "cursor", "field_groups", "install_id", "acked_install_seq", "page", "limit", "recovery_id"); err != nil {
		return 0, nil, err
	}
	installID, acked, err := req.syncCommon(obj)
	if err != nil {
		return 0, nil, err
	}
	// Every mode checks the caller again in the reads that build and serve
	// its view.
	sreq := protocol.SnapshotRequest{AccountID: req.principal.accountID(), InstallID: installID, AckedInstallSeq: acked,
		AuthorizeRead: req.authorizeSyncRead()}
	sreq.Mode, _ = obj["mode"].(string)
	sreq.Page, _ = obj["page"].(string)
	switch sreq.Mode {
	case "attach":
		if _, has := obj["cursor"]; has {
			return 0, nil, invalid("A first attachment has no cursor.")
		}
	case "upgrade":
		c, err := cursorField(obj)
		if err != nil {
			return 0, nil, err
		}
		sreq.Cursor = &c
		if groups, ok := obj["field_groups"].([]any); ok {
			for _, g := range groups {
				if s, ok := g.(string); ok {
					sreq.FieldGroups = append(sreq.FieldGroups, s)
				}
			}
		}
	case "recovery":
		// PROTOCOL §6.11: opens or resumes the recovery; a cursor the restored log
		// still holds answers the prefix shortcut (PROTOCOL §5).
		sreq.RecoveryID, _ = obj["recovery_id"].(string)
		if !contract.ValidUUID(sreq.RecoveryID) {
			return 0, nil, invalid("A recovery snapshot names its recovery_id.")
		}
		if _, has := obj["cursor"]; has && sreq.Page == "" {
			c, err := cursorField(obj)
			if err != nil {
				return 0, nil, err
			}
			sreq.Cursor = &c
		}
		// Archived content: the credential is also checked again in the
		// transaction that opens the recovery.
		sreq.Authorize = req.authorizeSync()
	default:
		return 0, nil, invalid("mode is attach, upgrade or recovery.")
	}
	if _, has := obj["recovery_id"]; has && sreq.Mode != "recovery" {
		return 0, nil, invalid("recovery_id belongs to a recovery snapshot.")
	}
	limit, err := intField(obj, "limit", false)
	if err != nil {
		return 0, nil, err
	}
	sreq.Limit = int(limit)
	resp, err := req.s.cfg.Protocol.Snapshot(req.ctx, sreq)
	if err != nil {
		return 0, nil, protocolError(err)
	}
	body, err := resp.Body()
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, encodedBody(body), nil
}

// secret serves a feed credential to an installation of its account (PROTOCOL §7.2).
func (req *request) secret() (int, any, error) {
	id := req.r.PathValue("id")
	if !contract.ValidUUID(id) {
		return 0, nil, errNotFound
	}
	v, err := req.s.cfg.Protocol.AuthorizedSecretValue(req.ctx, req.principal.accountID(), id, req.authorizeSyncRead())
	if err != nil {
		return 0, nil, protocolError(err)
	}
	return http.StatusOK, v, nil
}

// validOrigin accepts an origin in the fetcher's normal form (feed.Origin):
// scheme://host:port with an explicit port, lower case, nothing else.
func validOrigin(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.Port() != "" &&
		u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && feed.Origin(u) == s
}
