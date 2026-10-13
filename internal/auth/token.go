package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"strings"
)

// TokenPrefix marks NotTennaPod credentials (device tokens and web sessions).
const TokenPrefix = "ntp1_"

// NewCredential returns a random 256-bit credential and the hash to store.
func NewCredential() (secret string, hash []byte) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	secret = TokenPrefix + base64.RawURLEncoding.EncodeToString(b[:])
	return secret, HashCredential(secret)
}

// HashCredential returns SHA-256 of a credential (a fast hash suffices for a
// 256-bit random secret).
func HashCredential(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// WellFormed reports whether s looks like a credential (cheap pre-check).
func WellFormed(s string) bool {
	if !strings.HasPrefix(s, TokenPrefix) || len(s) != len(TokenPrefix)+43 {
		return false
	}
	_, err := base64.RawURLEncoding.Strict().DecodeString(s[len(TokenPrefix):])
	return err == nil
}

// SetupCodeAlphabet avoids look-alike characters.
const SetupCodeAlphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

// NewSetupCode returns a random 10-character setup code.
func NewSetupCode() string {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	out := make([]byte, len(b))
	for i, v := range b {
		out[i] = SetupCodeAlphabet[int(v)%len(SetupCodeAlphabet)]
	}
	return string(out)
}
