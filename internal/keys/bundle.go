// Package keys writes and reads the protected key bundle: the server's receipt
// keys and feed-credential key, encrypted under a passphrase, kept apart from
// database backups (PROTOCOL §11).
package keys

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/crypto/argon2"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Format and Version identify a bundle file.
const (
	Format  = "nottennapod-keys"
	Version = 1
)

// Argon2id parameters (PROTOCOL §5 password parameters).
const (
	kdfMemory  = 64 * 1024
	kdfTime    = 3
	kdfThreads = 4
	saltLen    = 16
	keyLen     = 32
)

// Errors returned by Open.
var (
	ErrFormat     = errors.New("keys: not a key bundle")
	ErrPassphrase = errors.New("keys: wrong passphrase or damaged bundle")
)

// ReceiptKey is one receipt MAC key.
type ReceiptKey struct {
	ID  string
	Key []byte
}

// Bundle is the plaintext of a key bundle.
type Bundle struct {
	ServerID    string
	ReceiptKeys []ReceiptKey
	SecretsKey  []byte
}

type kdf struct {
	Alg  string `json:"alg"`
	M    uint32 `json:"m"`
	T    uint32 `json:"t"`
	P    uint8  `json:"p"`
	Salt string `json:"salt"`
}

type file struct {
	Format     string `json:"format"`
	Version    int    `json:"version"`
	ServerID   string `json:"server_id"`
	CreatedAt  int64  `json:"created_at"`
	KDF        kdf    `json:"kdf"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

// header is the canonical additional data: every field except nonce and ciphertext.
func (f file) header() ([]byte, error) {
	return contract.Canonical(map[string]any{
		"format": f.Format, "version": int64(f.Version), "server_id": f.ServerID, "created_at": f.CreatedAt,
		"kdf": map[string]any{"alg": f.KDF.Alg, "m": int64(f.KDF.M), "t": int64(f.KDF.T), "p": int64(f.KDF.P), "salt": f.KDF.Salt},
	})
}

func (b Bundle) plaintext() ([]byte, error) {
	if b.ServerID == "" || len(b.SecretsKey) != 32 || len(b.ReceiptKeys) == 0 {
		return nil, fmt.Errorf("keys: incomplete bundle")
	}
	ks := append([]ReceiptKey(nil), b.ReceiptKeys...)
	sort.Slice(ks, func(i, j int) bool { return ks[i].ID < ks[j].ID })
	list := make([]any, len(ks))
	for i, k := range ks {
		if k.ID == "" || len(k.Key) == 0 || (i > 0 && ks[i-1].ID == k.ID) {
			return nil, fmt.Errorf("keys: invalid receipt key %q", k.ID)
		}
		list[i] = map[string]any{"key_id": k.ID, "key": base64.StdEncoding.EncodeToString(k.Key)}
	}
	return contract.Canonical(map[string]any{
		"server_id": b.ServerID, "receipt_keys": list, "secrets_key": base64.StdEncoding.EncodeToString(b.SecretsKey),
	})
}

func aeadFor(passphrase []byte, k kdf) (cipher.AEAD, error) {
	salt, err := base64.StdEncoding.DecodeString(k.Salt)
	if err != nil || len(salt) != saltLen {
		return nil, ErrFormat
	}
	key := argon2.IDKey(passphrase, salt, k.T, k.M, k.P, keyLen)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Seal encrypts b under passphrase and returns the bundle file's bytes.
func Seal(b Bundle, passphrase []byte, createdAt int64) ([]byte, error) {
	if len(passphrase) == 0 {
		return nil, fmt.Errorf("keys: empty passphrase")
	}
	plain, err := b.plaintext()
	if err != nil {
		return nil, err
	}
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	f := file{Format: Format, Version: Version, ServerID: b.ServerID, CreatedAt: createdAt,
		KDF: kdf{Alg: "argon2id", M: kdfMemory, T: kdfTime, P: kdfThreads, Salt: base64.StdEncoding.EncodeToString(salt)}}
	aead, err := aeadFor(passphrase, f.KDF)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ad, err := f.header()
	if err != nil {
		return nil, err
	}
	f.Nonce = base64.StdEncoding.EncodeToString(nonce)
	f.Ciphertext = base64.StdEncoding.EncodeToString(aead.Seal(nil, nonce, plain, ad))
	out, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// Open decrypts a bundle file.
func Open(data, passphrase []byte) (Bundle, error) {
	var f file
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil || f.Format != Format {
		return Bundle{}, ErrFormat
	}
	if f.Version != Version {
		return Bundle{}, fmt.Errorf("%w: version %d", ErrFormat, f.Version)
	}
	// Bounded parameters: a crafted file must not demand unbounded memory or time.
	if f.KDF.Alg != "argon2id" || f.KDF.M < 8*1024 || f.KDF.M > 1024*1024 || f.KDF.T < 1 || f.KDF.T > 16 || f.KDF.P < 1 || f.KDF.P > 16 {
		return Bundle{}, ErrFormat
	}
	aead, err := aeadFor(passphrase, f.KDF)
	if err != nil {
		return Bundle{}, err
	}
	nonce, err := base64.StdEncoding.DecodeString(f.Nonce)
	if err != nil || len(nonce) != aead.NonceSize() {
		return Bundle{}, ErrFormat
	}
	ct, err := base64.StdEncoding.DecodeString(f.Ciphertext)
	if err != nil {
		return Bundle{}, ErrFormat
	}
	ad, err := f.header()
	if err != nil {
		return Bundle{}, ErrFormat
	}
	plain, err := aead.Open(nil, nonce, ct, ad)
	if err != nil {
		return Bundle{}, ErrPassphrase
	}
	var p struct {
		ServerID    string `json:"server_id"`
		ReceiptKeys []struct {
			KeyID string `json:"key_id"`
			Key   string `json:"key"`
		} `json:"receipt_keys"`
		SecretsKey string `json:"secrets_key"`
	}
	if err := json.Unmarshal(plain, &p); err != nil || p.ServerID != f.ServerID {
		return Bundle{}, ErrFormat
	}
	b := Bundle{ServerID: p.ServerID}
	if b.SecretsKey, err = base64.StdEncoding.DecodeString(p.SecretsKey); err != nil || len(b.SecretsKey) != 32 {
		return Bundle{}, ErrFormat
	}
	for _, k := range p.ReceiptKeys {
		key, err := base64.StdEncoding.DecodeString(k.Key)
		if err != nil || k.KeyID == "" || len(key) == 0 {
			return Bundle{}, ErrFormat
		}
		b.ReceiptKeys = append(b.ReceiptKeys, ReceiptKey{ID: k.KeyID, Key: key})
	}
	if len(b.ReceiptKeys) == 0 {
		return Bundle{}, ErrFormat
	}
	return b, nil
}

// WriteFile writes data to path with mode 0600, durably: a temporary file in the
// same directory is synced and renamed, then the directory is synced.
func WriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	// No ACL entry, as its directory's inheritable ones would give.
	if err := private.Protect(tmp); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
