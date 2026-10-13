// Package protocol executes the sync protocol of PROTOCOL §6.4: typed
// envelopes, fact admission with transport slots, receipts and decisions, pulls
// and snapshots (PROTOCOL §6.4).
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// Target is one entity a fact affects.
type Target struct {
	Entity     string
	ID         string
	Generation string // "" when absent
}

// Fact is a parsed, envelope-valid fact (PROTOCOL §6.1).
type Fact struct {
	OpID         string
	AuthorID     string
	AuthorSeq    int64
	PreviousOpID string // "" when absent
	Dependencies []string
	Type         string
	TypeVersion  int64
	ClientTime   int64
	Targets      []Target
	Payload      map[string]any
}

// Archived is a fact as received: its canonical bytes, content hash and op_id
// are known even if its envelope is invalid.
type Archived struct {
	OpID        string
	Canonical   []byte
	ContentHash string
	Object      map[string]any
	Fact        *Fact  // nil when the envelope is invalid
	Violation   string // envelope violation reason, "" when valid
}

// ErrUnarchivable reports a fact that cannot be hashed or keyed (PROTOCOL §6.1):
// the request is invalid_request and consumes nothing.
var ErrUnarchivable = errors.New("protocol: fact cannot be archived")

var factFields = map[string]bool{"op_id": true, "author_id": true, "author_seq": true, "previous_op_id": true,
	"dependencies": true, "type": true, "type_version": true, "client_time": true, "targets": true, "payload": true}

// ParseFact classifies a decoded fact object (from contract.DecodeJSON): an
// unarchivable fact returns ErrUnarchivable; otherwise the Archived value carries
// either the typed Fact or the envelope violation.
func ParseFact(v any) (*Archived, error) {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: not an object", ErrUnarchivable)
	}
	opID, ok := obj["op_id"].(string)
	if !ok || !contract.ValidUUID(opID) {
		return nil, fmt.Errorf("%w: op_id missing or not a UUID", ErrUnarchivable)
	}
	canonical, err := contract.Canonical(obj)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnarchivable, err)
	}
	hash, err := contract.ContentHash(obj)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnarchivable, err)
	}
	a := &Archived{OpID: opID, Canonical: canonical, ContentHash: hash, Object: obj}
	f, violation := checkEnvelope(obj)
	if violation != "" {
		a.Violation = violation
	} else {
		a.Fact = f
	}
	return a, nil
}

func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		i, err := strconv.ParseInt(string(n), 10, 64)
		return i, err == nil
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}

func checkEnvelope(obj map[string]any) (*Fact, string) {
	for k := range obj {
		if !factFields[k] {
			return nil, "unknown_field"
		}
	}
	for _, k := range []string{"op_id", "author_id", "author_seq", "dependencies", "type", "type_version", "client_time", "targets", "payload"} {
		if _, ok := obj[k]; !ok {
			return nil, "missing_field"
		}
	}
	f := &Fact{OpID: obj["op_id"].(string)}
	var ok bool
	if f.AuthorID, ok = obj["author_id"].(string); !ok || !contract.ValidUUID(f.AuthorID) {
		return nil, "invalid_author_id"
	}
	if f.AuthorSeq, ok = integer(obj["author_seq"]); !ok || f.AuthorSeq < 1 {
		return nil, "invalid_author_seq"
	}
	if p, present := obj["previous_op_id"]; present {
		if f.PreviousOpID, ok = p.(string); !ok || !contract.ValidUUID(f.PreviousOpID) || f.PreviousOpID == f.OpID {
			return nil, "invalid_previous_op_id"
		}
	}
	deps, ok := obj["dependencies"].([]any)
	if !ok {
		return nil, "invalid_dependencies"
	}
	for _, d := range deps {
		s, ok := d.(string)
		if !ok || !contract.ValidUUID(s) || s == f.OpID {
			return nil, "invalid_dependencies"
		}
		if n := len(f.Dependencies); n > 0 && f.Dependencies[n-1] >= s {
			return nil, "unsorted_dependencies"
		}
		f.Dependencies = append(f.Dependencies, s)
	}
	if f.Type, ok = obj["type"].(string); !ok || f.Type == "" || len(f.Type) > 200 {
		return nil, "invalid_type"
	}
	if f.TypeVersion, ok = integer(obj["type_version"]); !ok || f.TypeVersion < 1 {
		return nil, "invalid_type_version"
	}
	if f.ClientTime, ok = integer(obj["client_time"]); !ok {
		return nil, "invalid_client_time"
	}
	targets, ok := obj["targets"].([]any)
	if !ok {
		return nil, "invalid_targets"
	}
	for _, t := range targets {
		m, ok := t.(map[string]any)
		if !ok {
			return nil, "invalid_targets"
		}
		var tg Target
		for k, v := range m {
			s, isString := v.(string)
			if !isString || s == "" {
				return nil, "invalid_targets"
			}
			switch k {
			case "entity":
				tg.Entity = s
			case "id":
				tg.ID = s
			case "generation":
				tg.Generation = s
			default:
				return nil, "invalid_targets"
			}
		}
		if tg.Entity == "" || tg.ID == "" {
			return nil, "invalid_targets"
		}
		if n := len(f.Targets); n > 0 && compareTargets(f.Targets[n-1], tg) >= 0 {
			return nil, "unsorted_targets"
		}
		f.Targets = append(f.Targets, tg)
	}
	if f.Payload, ok = obj["payload"].(map[string]any); !ok {
		return nil, "invalid_payload"
	}
	return f, ""
}

func compareTargets(a, b Target) int {
	for _, c := range [][2]string{{a.Entity, b.Entity}, {a.ID, b.ID}, {a.Generation, b.Generation}} {
		if c[0] != c[1] {
			if c[0] < c[1] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Receipt is a signed receipt (PROTOCOL §6.1).
type Receipt struct {
	OpID           string
	ContentHash    string
	Outcome        string // applied, superseded, not_applied, rejected
	Reason         string
	NormalizedTime int64
	Epoch, Seq     int64
	Index          int64    // position in its commit: (epoch, seq, index) is admission order (PROTOCOL §6.10)
	Frontier       []string // rejected receipts only (PROTOCOL §6.4, §8.1)
	// SortValues are the canonical sort values the server recorded when it
	// admitted a queue placement or sort trigger (PROTOCOL §7.6).
	SortValues []any
	KeyID      string
	MAC        string
}

// Object returns the receipt as a canonicalizable map (with mac if set).
func (r Receipt) Object() map[string]any {
	m := map[string]any{"op_id": r.OpID, "content_hash": r.ContentHash, "outcome": r.Outcome,
		"normalized_time": r.NormalizedTime, "epoch": r.Epoch, "seq": r.Seq, "index": r.Index, "key_id": r.KeyID}
	if r.Reason != "" {
		m["reason"] = r.Reason
	}
	if r.Outcome == "rejected" {
		frontier := make([]any, len(r.Frontier))
		for i, id := range r.Frontier {
			frontier[i] = id
		}
		m["frontier"] = frontier
	}
	if r.SortValues != nil {
		m["sort_values"] = r.SortValues
	}
	if r.MAC != "" {
		m["mac"] = r.MAC
	}
	return m
}

// Sign sets KeyID and MAC.
func (r *Receipt) Sign(keyID string, key []byte, accountID string) error {
	r.KeyID, r.MAC = keyID, ""
	mac, err := contract.SignedMAC(key, accountID, r.Object())
	if err != nil {
		return err
	}
	r.MAC = mac
	return nil
}

// Slot is a commit's record of one consumed transport slot.
type Slot struct {
	InstallID   string
	InstallSeq  int64
	OpID        string
	ContentHash string
	Result      string // accepted, duplicate, op_id_conflict
}

// Object returns the slot entry.
func (s Slot) Object() map[string]any {
	return map[string]any{"install_id": s.InstallID, "install_seq": s.InstallSeq, "op_id": s.OpID,
		"content_hash": s.ContentHash, "result": s.Result}
}

// Record is a changed projection record.
type Record struct {
	Entity     string
	ID         string
	FieldGroup string
	Value      any // nil with Tombstone
	Tombstone  bool
	Provenance []string
}

// Key identifies the record.
func (r Record) Key() [3]string { return [3]string{r.Entity, r.ID, r.FieldGroup} }

// Object returns the record entry.
func (r Record) Object() map[string]any {
	prov := slices.Clone(r.Provenance)
	slices.Sort(prov)
	prov = slices.Compact(prov)
	list := make([]any, len(prov))
	for i, p := range prov {
		list[i] = p
	}
	m := map[string]any{"entity": r.Entity, "id": r.ID, "field_group": r.FieldGroup, "provenance": list}
	if r.Tombstone {
		m["tombstone"] = true
	} else {
		m["value"] = r.Value
	}
	return m
}
