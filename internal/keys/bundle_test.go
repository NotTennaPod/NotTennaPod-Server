package keys

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func sample() Bundle {
	return Bundle{
		ServerID:    "6f1c2b8e-2f61-4c1e-9d55-0a7b3c9e1f20",
		ReceiptKeys: []ReceiptKey{{ID: "k1", Key: bytes.Repeat([]byte{7}, 32)}},
		SecretsKey:  bytes.Repeat([]byte{9}, 32),
	}
}

func TestBundleRoundTrip(t *testing.T) {
	data, err := Seal(sample(), []byte("correct horse"), 1_700_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Open(data, []byte("correct horse"))
	if err != nil {
		t.Fatal(err)
	}
	want := sample()
	if got.ServerID != want.ServerID || !bytes.Equal(got.SecretsKey, want.SecretsKey) ||
		len(got.ReceiptKeys) != 1 || got.ReceiptKeys[0].ID != "k1" || !bytes.Equal(got.ReceiptKeys[0].Key, want.ReceiptKeys[0].Key) {
		t.Fatalf("round trip: %+v", got)
	}
	if bytes.Contains(data, want.SecretsKey) || bytes.Contains(data, []byte("BwcHBwcH")) {
		t.Fatal("bundle file contains key material in the clear")
	}
}

func TestBundleRefusals(t *testing.T) {
	data, err := Seal(sample(), []byte("correct horse"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(data, []byte("wrong")); !errors.Is(err, ErrPassphrase) {
		t.Fatalf("wrong passphrase: %v", err)
	}
	// The header is authenticated: changing the server ID breaks the bundle.
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	f["server_id"] = "00000000-0000-4000-8000-000000000000"
	moved, _ := json.Marshal(f)
	if _, err := Open(moved, []byte("correct horse")); !errors.Is(err, ErrPassphrase) {
		t.Fatalf("altered header: %v", err)
	}
	// Unbounded KDF parameters are refused before any work.
	f["server_id"] = sample().ServerID
	f["kdf"].(map[string]any)["m"] = 1 << 30
	heavy, _ := json.Marshal(f)
	if _, err := Open(heavy, []byte("correct horse")); !errors.Is(err, ErrFormat) {
		t.Fatalf("huge memory parameter: %v", err)
	}
	if _, err := Open([]byte(`{"format":"other"}`), []byte("x")); !errors.Is(err, ErrFormat) {
		t.Fatalf("other format: %v", err)
	}
	if _, err := Seal(Bundle{ServerID: "x"}, []byte("p"), 1); err == nil {
		t.Fatal("incomplete bundle sealed")
	}
	if _, err := Seal(sample(), nil, 1); err == nil {
		t.Fatal("empty passphrase accepted")
	}
}

func TestWriteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := WriteFile(path, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(path, []byte("two")); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v %v", st.Mode(), err)
	}
	if b, _ := os.ReadFile(path); string(b) != "two" {
		t.Fatalf("content %q", b)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("%d entries left", len(entries))
	}
}
