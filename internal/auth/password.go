// Package auth holds the server's authentication primitives: Argon2id password
// hashing with bounded concurrency, random credentials stored as hashes, and the
// sign-in rate limiter (PROTOCOL §5).
package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"golang.org/x/crypto/argon2"
)

// Params are Argon2id parameters.
type Params struct {
	Memory  uint32 // KiB
	Time    uint32
	Threads uint8
}

// DefaultParams: 64 MiB, 3 passes, 4 lanes (about 0.1 s on a recent MacBook).
var DefaultParams = Params{Memory: 64 * 1024, Time: 3, Threads: 4}

const (
	saltLen = 16
	keyLen  = 32
)

// Errors.
var (
	ErrBusy          = errors.New("auth: too many password checks in progress")
	ErrMalformedHash = errors.New("auth: malformed password hash")
)

// Hasher runs Argon2id with at most two computations at once and a short queue;
// further requests fail fast with ErrBusy rather than exhausting memory.
type Hasher struct {
	params  Params
	running chan struct{}
	waiting atomic.Int32
	maxWait int32
	dummy   string
}

// NewHasher returns a hasher using params for new hashes.
func NewHasher(params Params) *Hasher {
	h := &Hasher{params: params, running: make(chan struct{}, 2), maxWait: 8}
	d, err := h.encode("not a real password", params)
	if err != nil {
		panic(err)
	}
	h.dummy = d
	return h
}

func (h *Hasher) acquire(ctx context.Context) error {
	if h.waiting.Add(1) > h.maxWait {
		h.waiting.Add(-1)
		return ErrBusy
	}
	defer h.waiting.Add(-1)
	select {
	case h.running <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Hasher) release() { <-h.running }

// Hash returns a PHC string for password.
func (h *Hasher) Hash(ctx context.Context, password string) (string, error) {
	if err := h.acquire(ctx); err != nil {
		return "", err
	}
	defer h.release()
	return h.encode(password, h.params)
}

func (h *Hasher) encode(password string, p Params) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(password), salt, p.Time, p.Memory, p.Threads, keyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, p.Memory, p.Time, p.Threads,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)), nil
}

// Verify checks password against a PHC string. An empty encoded hash (unknown
// username) is checked against a dummy hash, so it costs the same and fails.
// needsRehash reports weaker parameters than the current ones.
func (h *Hasher) Verify(ctx context.Context, password, encoded string) (ok, needsRehash bool, err error) {
	known := encoded != ""
	if !known {
		encoded = h.dummy
	}
	p, salt, want, err := decode(encoded)
	if err != nil {
		return false, false, err
	}
	if err := h.acquire(ctx); err != nil {
		return false, false, err
	}
	got := argon2.IDKey([]byte(password), salt, p.Time, p.Memory, p.Threads, uint32(len(want)))
	h.release()
	ok = subtle.ConstantTimeCompare(got, want) == 1 && known
	return ok, ok && p != h.params, nil
}

func decode(encoded string) (Params, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return Params{}, nil, nil, ErrMalformedHash
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil || version != argon2.Version {
		return Params{}, nil, nil, ErrMalformedHash
	}
	var p Params
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.Memory, &p.Time, &p.Threads); err != nil || p.Time == 0 || p.Threads == 0 {
		return Params{}, nil, nil, ErrMalformedHash
	}
	salt, err1 := base64.RawStdEncoding.DecodeString(parts[4])
	key, err2 := base64.RawStdEncoding.DecodeString(parts[5])
	if err1 != nil || err2 != nil || len(salt) < 8 || len(key) < 16 {
		return Params{}, nil, nil, ErrMalformedHash
	}
	return p, salt, key, nil
}
