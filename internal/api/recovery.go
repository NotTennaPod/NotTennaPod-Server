package api

import (
	"net/http"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// Recovery after a restore (PROTOCOL §6.11): the inventory, the upload and the
// finish of a recovery that a recovery snapshot (POST /v1/snapshot with mode
// "recovery") opened. Each takes a device token or a web session bound to an
// installation; the account is the credential's.
func (s *Server) recoveryRoutes() {
	if s.cfg.Protocol == nil {
		return
	}
	// The inventory's advertised batch (bounds.recovery_inventory entries)
	// takes far more than the default 64 KiB: it has the push's body bound.
	s.route("/v1/sync/recovery/inventory", endpoint{method: http.MethodPost, auth: authSync, maxBody: protocol.MaxRequestBytes,
		handle: (*request).recoveryInventory})
	s.route("/v1/sync/recovery/upload", endpoint{method: http.MethodPost, auth: authSync, maxBody: protocol.MaxRequestBytes,
		handle: (*request).recoveryUpload})
	s.route("/v1/sync/recovery/finish", endpoint{method: http.MethodPost, auth: authSync, handle: (*request).recoveryFinish})
}

// recoveryCommon checks the recovery ID and the optional install_id (the
// credential's installation) and returns them.
func (req *request) recoveryCommon(obj map[string]any) (recoveryID, installID string, err error) {
	recoveryID, _ = obj["recovery_id"].(string)
	if !contract.ValidUUID(recoveryID) {
		return "", "", invalid("recovery_id must be a UUID.")
	}
	installID = req.principal.cred.InstallID
	if installID == "" {
		return "", "", errForbidden
	}
	if raw, present := obj["install_id"]; present {
		if id, _ := raw.(string); id != installID {
			return "", "", fail(http.StatusForbidden, "forbidden", "install_id is not this credential's installation.")
		}
	}
	return recoveryID, installID, nil
}

func position(v any, n int) ([]int64, bool) {
	list, ok := v.([]any)
	if !ok || len(list) != n {
		return nil, false
	}
	out := make([]int64, n)
	for i, x := range list {
		num, ok := x.(interface{ String() string })
		if !ok {
			return nil, false
		}
		var y int64
		for _, c := range num.String() {
			if c < '0' || c > '9' {
				return nil, false
			}
			y = y*10 + int64(c-'0')
		}
		out[i] = y
	}
	return out, true
}

// recoveryInventory answers have, need or conflict per entry (PROTOCOL §6.11).
func (req *request) recoveryInventory() (int, any, error) {
	obj, err := req.readProtocolBody()
	if err != nil {
		return 0, nil, err
	}
	if err := onlyFields(obj, "recovery_id", "install_id", "entries"); err != nil {
		return 0, nil, err
	}
	recoveryID, installID, err := req.recoveryCommon(obj)
	if err != nil {
		return 0, nil, err
	}
	list, ok := obj["entries"].([]any)
	if !ok {
		return 0, nil, invalid("entries must be a list.")
	}
	if len(list) > protocol.MaxRecoveryInventory {
		return 0, nil, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "Too many inventory entries; send at most bounds.recovery_inventory.")
	}
	entries := make([]protocol.InventoryEntry, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return 0, nil, invalid("Each inventory entry must be an object.")
		}
		kind, _ := m["kind"].(string)
		e := protocol.InventoryEntry{Kind: kind}
		switch kind {
		case "fact":
			if onlyFields(m, "kind", "op_id", "content_hash", "position") != nil {
				return 0, nil, invalid("A fact entry is {kind, op_id, content_hash, position}.")
			}
			e.ID, _ = m["op_id"].(string)
			e.ContentHash, _ = m["content_hash"].(string)
			p, ok := position(m["position"], 3)
			if !ok || !contract.ValidUUID(e.ID) || len(e.ContentHash) != 64 {
				return 0, nil, invalid("A fact entry is {kind, op_id, content_hash, position: [epoch, seq, index]}.")
			}
			copy(e.Position[:], p)
		case "decision":
			if onlyFields(m, "kind", "decision_id", "rank") != nil {
				return 0, nil, invalid("A decision entry is {kind, decision_id, rank}.")
			}
			e.ID, _ = m["decision_id"].(string)
			r, ok := position(m["rank"], 2)
			if !ok || !contract.ValidUUID(e.ID) {
				return 0, nil, invalid("A decision entry is {kind, decision_id, rank: [epoch, seq]}.")
			}
			copy(e.Rank[:], r)
		default:
			return 0, nil, invalid("An inventory entry's kind is fact or decision.")
		}
		entries[i] = e
	}
	results, err := req.s.cfg.Protocol.RecoveryInventory(req.ctx, req.principal.accountID(), installID, recoveryID, entries, req.authorizeSync())
	if err != nil {
		return 0, nil, protocolError(err)
	}
	return http.StatusOK, map[string]any{"results": results}, nil
}

// recoveryUpload joins one upload request (PROTOCOL §6.11): entries {fact, receipt},
// {part, receipt} (a framed fact, as push's parts) or {decision}; secrets
// beside them as in push. A derivation that outlasts the request goes on and
// the request answers server_busy with retry_after.
func (req *request) recoveryUpload() (int, any, error) {
	obj, err := req.readProtocolBody()
	if err != nil {
		return 0, nil, err
	}
	if err := onlyFields(obj, "recovery_id", "install_id", "entries", "secrets"); err != nil {
		return 0, nil, err
	}
	recoveryID, installID, err := req.recoveryCommon(obj)
	if err != nil {
		return 0, nil, err
	}
	list, ok := obj["entries"].([]any)
	if !ok {
		return 0, nil, invalid("entries must be a list.")
	}
	if len(list) > protocol.MaxRecoveryEntries {
		return 0, nil, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "Too many entries in one upload; send fewer at a time.")
	}
	ureq := protocol.RecoveryUploadRequest{AccountID: req.principal.accountID(), InstallID: installID, RecoveryID: recoveryID,
		Steps: protocol.AllRecoveryEndSteps}
	if raw, present := obj["secrets"]; present {
		if ureq.Secrets, err = parseSecrets(raw); err != nil {
			return 0, nil, err
		}
	}
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return 0, nil, invalid("Each entry must be an object.")
		}
		var e protocol.RecoveryEntry
		_, hasFact := m["fact"]
		partObj, hasPart := m["part"].(map[string]any)
		decision, hasDecision := m["decision"].(map[string]any)
		receipt, hasReceipt := m["receipt"].(map[string]any)
		switch {
		case hasDecision && len(m) == 1:
			if contract.Depth(decision) > protocol.MaxFactDepth {
				return 0, nil, invalid("A decision nests deeper than the advertised bound.")
			}
			e.Decision = decision
		case hasFact && hasReceipt && len(m) == 2:
			if contract.Depth(m["fact"]) > protocol.MaxFactDepth {
				return 0, nil, invalid("A fact nests deeper than the advertised bound.")
			}
			archived, err := protocol.ParseFact(m["fact"])
			if err != nil {
				return 0, nil, invalid("An entry's fact cannot be archived: " + err.Error())
			}
			if len(archived.Canonical) > protocol.MaxUnframedFact {
				return 0, nil, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "Facts above the advertised size must be framed as parts.")
			}
			e.Fact, e.Receipt = archived, receipt
		case hasPart && hasReceipt && len(m) == 2:
			part, err := protocol.DecodePart(partObj)
			if err != nil {
				return 0, nil, protocolError(err)
			}
			e.Part, e.Receipt = part, receipt
		default:
			return 0, nil, invalid("Each entry is {fact, receipt}, {part, receipt} or {decision}.")
		}
		ureq.Entries = append(ureq.Entries, e)
	}
	// The upload runs as a job that outlives the request (PROTOCOL §6.11),
	// with this credential's authority, checked again at its publication.
	cred := protocol.StoredCredential{Hash: req.principal.cred.Hash, Kind: req.principal.cred.Kind}
	resp, err := req.s.cfg.Protocol.SubmitRecoveryUpload(req.ctx, ureq, req.authorizeSync(), cred)
	if err != nil {
		return 0, nil, protocolError(err)
	}
	return http.StatusOK, resp, nil
}

// recoveryFinish records the recovery's outcome (PROTOCOL §6.11).
func (req *request) recoveryFinish() (int, any, error) {
	obj, err := req.readProtocolBody()
	if err != nil {
		return 0, nil, err
	}
	if err := onlyFields(obj, "recovery_id", "install_id", "complete"); err != nil {
		return 0, nil, err
	}
	recoveryID, installID, err := req.recoveryCommon(obj)
	if err != nil {
		return 0, nil, err
	}
	complete, ok := obj["complete"].(bool)
	if !ok {
		return 0, nil, invalid("complete must be a boolean.")
	}
	sum, err := req.s.cfg.Protocol.RecoveryFinish(req.ctx, req.principal.accountID(), installID, recoveryID, complete, req.authorizeSync())
	if err != nil {
		return 0, nil, protocolError(err)
	}
	return http.StatusOK, sum, nil
}
