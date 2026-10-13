package contract

import (
	"bytes"
	"encoding/base64"
	"errors"
)

// ErrInvalidParts reports framed parts that do not reassemble to a canonical fact
// with the declared content hash (PROTOCOL §3).
var ErrInvalidParts = errors.New("contract: invalid parts")

// SplitCanonical returns base64url (no padding) slices of at most size bytes of
// UTF-8(JCS(v)), in order.
func SplitCanonical(v any, size int) ([]string, error) {
	data, err := Canonical(v)
	if err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, ErrInvalidParts
	}
	var parts []string
	for start := 0; start < len(data); start += size {
		end := min(start+size, len(data))
		parts = append(parts, base64.RawURLEncoding.EncodeToString(data[start:end]))
	}
	return parts, nil
}

// ReassembleFact concatenates the parts of one framed slot, checks the bytes are
// the canonical encoding of a JSON object and that their content hash matches.
func ReassembleFact(parts []string, contentHash string) (map[string]any, error) {
	if len(parts) == 0 {
		return nil, ErrInvalidParts
	}
	var buf bytes.Buffer
	for _, p := range parts {
		data, err := decodeBase64URL(p)
		if err != nil || len(data) == 0 {
			return nil, ErrInvalidParts
		}
		buf.Write(data)
	}
	v, err := DecodeJSON(buf.Bytes())
	if err != nil {
		return nil, ErrInvalidParts
	}
	fact, ok := v.(map[string]any)
	if !ok {
		return nil, ErrInvalidParts
	}
	canon, err := Canonical(fact)
	if err != nil || !bytes.Equal(canon, buf.Bytes()) {
		return nil, ErrInvalidParts
	}
	hash, err := ContentHash(fact)
	if err != nil || hash != contentHash {
		return nil, ErrInvalidParts
	}
	return fact, nil
}
