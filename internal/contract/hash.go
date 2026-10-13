package contract

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
)

// ContentHash is hex(SHA-256(JCS(fact))) over exactly the fact's own fields (PROTOCOL §3).
func ContentHash(fact map[string]any) (string, error) {
	data, err := Canonical(fact)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// GenesisDigest is d(0): 32 zero bytes.
var GenesisDigest = make([]byte, 32)

// NextDigest returns d(seq) = SHA-256(d(prev) || JCS(commit)).
func NextDigest(previous []byte, commit map[string]any) ([]byte, error) {
	data, err := Canonical(commit)
	if err != nil {
		return nil, err
	}
	return ChainDigest(previous, data)
}

// ChainDigest returns SHA-256(previous || canonical) for bytes that are already
// the canonical encoding of a commit or snapshot item.
func ChainDigest(previous, canonical []byte) ([]byte, error) {
	if len(previous) != 32 {
		return nil, fmt.Errorf("contract: previous digest must be 32 bytes")
	}
	h := sha256.New()
	h.Write(previous)
	h.Write(canonical)
	return h.Sum(nil), nil
}

// SignedMAC returns hex(HMAC-SHA256(key, JCS(object with account_id, without mac))).
// The account ID is bound into the MAC input but not sent on the wire (PROTOCOL §3).
func SignedMAC(key []byte, accountID string, object map[string]any) (string, error) {
	input := make(map[string]any, len(object)+1)
	for k, v := range object {
		if k == "mac" {
			continue
		}
		input[k] = v
	}
	if _, clash := input["account_id"]; clash {
		return "", errors.New("contract: object already has account_id")
	}
	input["account_id"] = accountID
	data, err := Canonical(input)
	if err != nil {
		return "", err
	}
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return hex.EncodeToString(m.Sum(nil)), nil
}

// VerifyMAC checks a MAC produced by SignedMAC in constant time.
func VerifyMAC(key []byte, accountID string, object map[string]any) bool {
	mac, ok := object["mac"].(string)
	if !ok {
		return false
	}
	want, err := SignedMAC(key, accountID, object)
	if err != nil {
		return false
	}
	return hmac.Equal([]byte(mac), []byte(want))
}

// decodeBase64URL decodes strict base64url (PROTOCOL §3): only the RFC 4648 PROTOCOL §3 alphabet
// (Go's decoder would otherwise skip CR and LF), no padding, zero unused
// trailing bits.
func decodeBase64URL(s string) ([]byte, error) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return nil, errors.New("contract: invalid base64url")
		}
	}
	return base64.RawURLEncoding.Strict().DecodeString(s)
}

// Cursor is the server's position in an account's change log (PROTOCOL §6).
type Cursor struct {
	LogID  string
	Epoch  int64
	Seq    int64
	Digest []byte
}

// ErrInvalidCursor reports a cursor that does not decode.
var ErrInvalidCursor = errors.New("contract: invalid cursor")

// Encode returns base64url (no padding) of JCS({log_id, epoch, seq, digest}).
func (c Cursor) Encode() (string, error) {
	data, err := Canonical(map[string]any{
		"log_id": c.LogID, "epoch": c.Epoch, "seq": c.Seq, "digest": hex.EncodeToString(c.Digest),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

// DecodeCursor parses an encoded cursor and checks it is in canonical form.
func DecodeCursor(s string) (Cursor, error) {
	data, err := decodeBase64URL(s)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	v, err := DecodeJSON(data)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	obj, ok := v.(map[string]any)
	if !ok || len(obj) != 4 {
		return Cursor{}, ErrInvalidCursor
	}
	canon, err := Canonical(obj)
	if err != nil || string(canon) != string(data) {
		return Cursor{}, ErrInvalidCursor
	}
	logID, ok1 := obj["log_id"].(string)
	digestHex, ok2 := obj["digest"].(string)
	epoch, err1 := numberField(obj["epoch"])
	seq, err2 := numberField(obj["seq"])
	digest, err3 := hex.DecodeString(digestHex)
	if !ok1 || !ok2 || err1 != nil || err2 != nil || err3 != nil || len(digest) != 32 || epoch < 0 || seq < 0 ||
		!ValidUUID(logID) || hex.EncodeToString(digest) != digestHex {
		return Cursor{}, ErrInvalidCursor
	}
	return Cursor{LogID: logID, Epoch: epoch, Seq: seq, Digest: digest}, nil
}

func numberField(v any) (int64, error) {
	n, ok := v.(interface{ String() string })
	if !ok {
		return 0, ErrInvalidCursor
	}
	return canonicalInteger(n.String())
}
