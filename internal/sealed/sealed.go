// Package sealed encrypts data at rest with a server key kept outside the
// database (feed credentials and staged parts; PROTOCOL §7.2).
package sealed

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// KeyFile is the key's file name in the data directory.
const KeyFile = "secrets.key"

// Box seals and opens data with AES-256-GCM.
type Box struct{ aead cipher.AEAD }

// ErrOpen reports data that does not authenticate under this key.
var ErrOpen = errors.New("sealed: cannot open")

// Load reads the existing key from dir; it never makes one (an error
// satisfying errors.Is(err, os.ErrNotExist) when there is none).
func Load(dir string) (*Box, error) {
	path := filepath.Join(dir, KeyFile)
	key, err := private.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("sealed: %s must hold 32 bytes", path)
	}
	return New(key)
}

// LoadOrCreate reads the key from dir, creating it (mode 0600, synced) on first use.
func LoadOrCreate(dir string) (*Box, error) {
	path := filepath.Join(dir, KeyFile)
	// The key is a regular file of the server's user, never a symlink, and
	// private.
	key, err := private.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, err
		}
		tmp := path + ".tmp"
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		if _, err := f.Write(key); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
		if err := os.Rename(tmp, path); err != nil {
			return nil, err
		}
		if d, err := os.Open(dir); err == nil {
			_ = d.Sync()
			d.Close()
		}
	} else if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("sealed: %s must hold 32 bytes", path)
	}
	return New(key)
}

// New returns a box for a 32-byte key.
func New(key []byte) (*Box, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Box{aead: aead}, nil
}

// Seal encrypts plaintext bound to additional data; the nonce is prefixed.
func (b *Box) Seal(plaintext, additional []byte) []byte {
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	return b.aead.Seal(nonce, nonce, plaintext, additional)
}

// Open decrypts data sealed with the same additional data.
func (b *Box) Open(sealed, additional []byte) ([]byte, error) {
	n := b.aead.NonceSize()
	if len(sealed) < n {
		return nil, ErrOpen
	}
	out, err := b.aead.Open(nil, sealed[:n], sealed[n:], additional)
	if err != nil {
		return nil, ErrOpen
	}
	return out, nil
}
