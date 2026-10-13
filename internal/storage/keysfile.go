package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/keys"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

// Receipt keys live outside the database (PROTOCOL §11.1): receipt.keys in the data
// directory, mode 0600, beside secrets.key, so no backup holds them. The receipt_keys table
// stays, empty.

const (
	receiptKeysName = "receipt.keys"
	keysSecretName  = sealed.KeyFile
)

// ReceiptKey is a server MAC key (receipts, decisions, page tokens).
type ReceiptKey struct {
	ID     string
	Secret []byte
}

type receiptKeysJSON struct {
	ServerID string `json:"server_id"`
	Keys     []struct {
		KeyID string `json:"key_id"`
		Key   string `json:"key"`
	} `json:"keys"`
}

// readReceiptKeysFile returns the file's server ID and keys (newest first, the
// order they are written in); exists is false when there is no file.
func readReceiptKeysFile(dir string) (serverID string, ks []ReceiptKey, exists bool, err error) {
	data, err := private.ReadFile(filepath.Join(dir, receiptKeysName))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil, false, nil
	}
	if err != nil {
		return "", nil, false, err
	}
	var f receiptKeysJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil || f.ServerID == "" || len(f.Keys) == 0 {
		return "", nil, true, fmt.Errorf("%w: %s is damaged", ErrKeysMismatch, receiptKeysName)
	}
	for _, k := range f.Keys {
		secret, err := base64.StdEncoding.DecodeString(k.Key)
		if err != nil || k.KeyID == "" || len(secret) != 32 {
			return "", nil, true, fmt.Errorf("%w: %s is damaged", ErrKeysMismatch, receiptKeysName)
		}
		ks = append(ks, ReceiptKey{ID: k.KeyID, Secret: secret})
	}
	return f.ServerID, ks, true, nil
}

func writeReceiptKeysFile(dir, serverID string, ks []ReceiptKey) error {
	var f receiptKeysJSON
	f.ServerID = serverID
	for _, k := range ks {
		f.Keys = append(f.Keys, struct {
			KeyID string `json:"key_id"`
			Key   string `json:"key"`
		}{k.ID, base64.StdEncoding.EncodeToString(k.Secret)})
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	if err := keys.WriteFile(filepath.Join(dir, receiptKeysName), append(data, '\n')); err != nil {
		return fmt.Errorf("storage: writing %s: %w", receiptKeysName, err)
	}
	return nil
}

// sameKeys compares two key sets by ID and value, ignoring order.
func sameKeys(a, b []ReceiptKey) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string][]byte{}
	for _, k := range a {
		m[k.ID] = k.Secret
	}
	for _, k := range b {
		if v, ok := m[k.ID]; !ok || !bytes.Equal(v, k.Secret) {
			return false
		}
	}
	return len(m) == len(a)
}

// newestFirst orders key IDs k1, k2, … newest (highest number) first.
func newestFirst(ks []ReceiptKey) []ReceiptKey {
	out := append([]ReceiptKey(nil), ks...)
	sort.SliceStable(out, func(i, j int) bool {
		if len(out[i].ID) != len(out[j].ID) {
			return len(out[i].ID) > len(out[j].ID)
		}
		return out[i].ID > out[j].ID
	})
	return out
}

// loadReceiptKeys is the startup step that replaced EnsureReceiptKey (PROTOCOL §11.1). It
// moves keys still in the database into receipt.keys (the file is written and
// synced before the rows are deleted, so an interruption leaves both, which the
// next start checks are equal and finishes), and creates k1 on a first start.
func (s *Store) loadReceiptKeys(ctx context.Context) error {
	serverID, _, err := getMeta(ctx, s.writer, "server_id")
	if err != nil {
		return err
	}
	rows, err := s.writer.QueryContext(ctx, `SELECT key_id, secret FROM receipt_keys ORDER BY created_at DESC, key_id DESC`)
	if err != nil {
		return err
	}
	var inDB []ReceiptKey
	for rows.Next() {
		var k ReceiptKey
		if err := rows.Scan(&k.ID, &k.Secret); err != nil {
			rows.Close()
			return err
		}
		inDB = append(inDB, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	fileServer, inFile, exists, err := readReceiptKeysFile(s.dir)
	if err != nil {
		return err
	}
	switch {
	case exists && fileServer != serverID:
		return fmt.Errorf("%w: %s belongs to server %s, the database to server %s; to put back a backup, use the restore command",
			ErrOtherServer, receiptKeysName, fileServer, serverID)
	case exists && len(inDB) > 0 && !sameKeys(inDB, inFile):
		return fmt.Errorf("%w: the database holds receipt keys that differ from %s; it belongs to another server", ErrKeysMismatch, receiptKeysName)
	case exists:
		s.receiptKeys = inFile
	case len(inDB) > 0:
		if err := writeReceiptKeysFile(s.dir, serverID, inDB); err != nil {
			return err
		}
		if err := call(s.hooks.afterReceiptKeysFile); err != nil {
			return err
		}
		s.receiptKeys = inDB
	default:
		// Only a new database gets new keys: one holding signed receipts or
		// decisions needs its own keys back, or two keys would share one ID.
		var signed bool
		if err := s.writer.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM facts) OR EXISTS (SELECT 1 FROM decisions)`).Scan(&signed); err != nil {
			return err
		}
		if signed {
			return fmt.Errorf("%w: %s is gone but the database holds signed receipts; put the keys back with "+
				"`nottennapod-server keys install --data-dir %s --keys BUNDLE` (the bundle from keys export)", ErrKeysMissing, receiptKeysName, s.dir)
		}
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return err
		}
		s.receiptKeys = []ReceiptKey{{ID: "k1", Secret: secret}}
		if err := writeReceiptKeysFile(s.dir, serverID, s.receiptKeys); err != nil {
			return err
		}
	}
	if len(inDB) > 0 {
		return s.Write(ctx, func(w *WriteTx) error {
			_, err := w.tx.ExecContext(ctx, `DELETE FROM receipt_keys`)
			return err
		})
	}
	return nil
}

// ReceiptKeys returns every key, the current (newest) one first.
func (s *Store) ReceiptKeys(ctx context.Context) ([]ReceiptKey, error) {
	out := make([]ReceiptKey, len(s.receiptKeys))
	copy(out, s.receiptKeys)
	return out, nil
}

// ---- the key bundle (PROTOCOL §11.1)

// ExportKeys writes the protected key bundle to path (mode 0600, durably) and
// records the export time in the restore metadata. The secrets key is created
// first if this server has never made one.
func (s *Store) ExportKeys(ctx context.Context, path string, passphrase []byte) error {
	if s.meta == nil {
		return errors.New("storage: keys can only be exported from the server's data directory")
	}
	if _, err := sealed.LoadOrCreate(s.dir); err != nil {
		return err
	}
	secret, err := private.ReadFile(filepath.Join(s.dir, keysSecretName))
	if err != nil {
		return err
	}
	serverID, err := s.ServerID(ctx)
	if err != nil {
		return err
	}
	b := keys.Bundle{ServerID: serverID, SecretsKey: secret}
	for _, k := range s.receiptKeys {
		b.ReceiptKeys = append(b.ReceiptKeys, keys.ReceiptKey{ID: k.ID, Key: k.Secret})
	}
	now := time.Now().UnixMilli()
	data, err := keys.Seal(b, passphrase, now)
	if err != nil {
		return err
	}
	if err := keys.WriteFile(path, data); err != nil {
		return fmt.Errorf("storage: writing the key bundle: %w", err)
	}
	return s.meta.update("keys-exported", func(st *restoreState) { st.KeysExportedAt = now })
}

// KeysExportedAt returns when the key bundle was last exported (Unix
// milliseconds; 0 if never), for the startup log and PROTOCOL §14 admin page.
func (s *Store) KeysExportedAt() int64 {
	if s.meta == nil {
		return 0
	}
	if st := s.meta.state(); st != nil {
		return st.KeysExportedAt
	}
	return 0
}
