package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

const tokenDomain = "ntp-page-token-v1\x00"

func tokenMAC(key []byte, payload []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(tokenDomain))
	m.Write(payload)
	return m.Sum(nil)
}

// sealToken encodes and authenticates a page token under the current key.
func sealToken(key storage.ReceiptKey, fields map[string]any) (string, error) {
	fields["key_id"] = key.ID
	payload, err := contract.Canonical(fields)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(tokenMAC(key.Secret, payload)), nil
}

// openToken verifies a page token against the retained keys and returns its fields.
func openToken(keys []storage.ReceiptKey, token string) (map[string]any, bool) {
	payloadPart, macPart, ok := strings.Cut(token, ".")
	if !ok {
		return nil, false
	}
	payload, err1 := decodeBase64URL(payloadPart)
	mac, err2 := decodeBase64URL(macPart)
	if err1 != nil || err2 != nil {
		return nil, false
	}
	v, err := contract.DecodeJSON(payload)
	if err != nil {
		return nil, false
	}
	fields, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	keyID, _ := fields["key_id"].(string)
	for _, k := range keys {
		if k.ID == keyID && hmac.Equal(mac, tokenMAC(k.Secret, payload)) {
			return fields, true
		}
	}
	return nil, false
}

func tokenString(f map[string]any, k string) string {
	s, _ := f[k].(string)
	return s
}

func tokenInt(f map[string]any, k string) (int64, bool) { return integer(f[k]) }
