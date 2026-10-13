package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The backup, keys export and restore commands (PROTOCOL §11) run
// end to end on a stopped server's data directory.
func TestBackupKeysAndRestoreCommands(t *testing.T) {
	ctx := context.Background()
	work := t.TempDir()
	data, backups, bundle, passFile := filepath.Join(work, "data"), filepath.Join(work, "backups"), filepath.Join(work, "keys.json"), filepath.Join(work, "pass")
	os.WriteFile(passFile, []byte("a passphrase\n"), 0o600)
	cmd := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		devNull, _ := os.Open(os.DevNull)
		defer devNull.Close()
		code := run(ctx, args, devNull, &out, &errOut)
		return code, out.String(), errOut.String()
	}
	if code, _, errs := cmd("backup", "--data-dir", data, "--out", backups); code != 1 || !strings.Contains(errs, "holds no NotTennaPod server database") {
		t.Fatalf("backup of a directory without a server: %d %s", code, errs)
	}
	store, err := storage.Open(ctx, data) // the server's first start
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	if code, out, errs := cmd("backup", "--data-dir", data, "--out", backups); code != 0 || !strings.Contains(out, "Wrote ") {
		t.Fatalf("backup: %d %s %s", code, out, errs)
	}
	entries, _ := os.ReadDir(backups)
	if len(entries) != 1 {
		t.Fatalf("backups %v", entries)
	}
	if code, _, errs := cmd("keys", "export", "--data-dir", data, "--out", bundle); code != 1 || !strings.Contains(errs, "--passphrase-file") {
		t.Fatalf("keys export without a terminal: %d %s", code, errs)
	}
	if code, out, errs := cmd("keys", "export", "--data-dir", data, "--out", bundle, "--passphrase-file", passFile); code != 0 || !strings.Contains(out, bundle) {
		t.Fatalf("keys export: %d %s %s", code, out, errs)
	}
	if fi, err := os.Stat(bundle); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("bundle %v %v", fi, err)
	}
	backup := filepath.Join(backups, entries[0].Name())
	os.WriteFile(filepath.Join(work, "wrong"), []byte("wrong\n"), 0o600)
	if code, _, errs := cmd("restore", "--data-dir", data, "--from", backup, "--keys", bundle, "--passphrase-file", filepath.Join(work, "wrong")); code != 1 ||
		!strings.Contains(errs, "passphrase") {
		t.Fatalf("restore with a wrong passphrase: %d %s", code, errs)
	}
	code, out, errs := cmd("restore", "--data-dir", data, "--from", backup, "--keys", bundle, "--passphrase-file", passFile)
	if code != 0 || !strings.Contains(out, "Restored the database from") || !strings.Contains(out, "pre-restore") {
		t.Fatalf("restore: %d %s %s", code, out, errs)
	}
	for _, args := range [][]string{{"keys"}, {"keys", "import"}, {"frob"}, {"backup", "--data-dir", data}, {"restore", "--data-dir", data}} {
		if code, _, _ := cmd(args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
}
