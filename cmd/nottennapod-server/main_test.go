package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/backupsched"
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
	if len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".db.gz") {
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

// The start log says sync is available, and a server restored onto a new
// machine with the key bundle does not ask for a key export it already has.
func TestStartLogAfterARestoreWithTheBundle(t *testing.T) {
	ctx := context.Background()
	work := t.TempDir()
	data, backups, bundle, passFile := filepath.Join(work, "data"), filepath.Join(work, "backups"), filepath.Join(work, "keys.json"), filepath.Join(work, "pass")
	os.WriteFile(passFile, []byte("a passphrase\n"), 0o600)
	cmd := func(args ...string) {
		t.Helper()
		var out, errOut bytes.Buffer
		devNull, _ := os.Open(os.DevNull)
		defer devNull.Close()
		if code := run(ctx, args, devNull, &out, &errOut); code != 0 {
			t.Fatalf("%v: %d %s %s", args, code, out.String(), errOut.String())
		}
	}
	store, err := storage.Open(ctx, data) // the old machine's server
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	cmd("keys", "export", "--data-dir", data, "--out", bundle, "--passphrase-file", passFile)
	cmd("backup", "--data-dir", data, "--out", backups)
	entries, _ := os.ReadDir(backups)
	if len(entries) != 1 {
		t.Fatalf("backups %v", entries)
	}
	moved := filepath.Join(work, "new-machine", "data")
	cmd("restore", "--data-dir", moved, "--from", filepath.Join(backups, entries[0].Name()), "--keys", bundle, "--passphrase-file", passFile)
	logs := captureLog(t)
	startServe(t, moved).stop(t)
	var listening string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, `msg="server listening"`) {
			listening = line
		}
	}
	if !strings.Contains(listening, "sync_available=true") {
		t.Errorf("the start log: %q", listening)
	}
	if strings.Contains(logs.String(), "recovery key") {
		t.Errorf("a server restored with its key bundle asks for a recovery key:\n%s", logs.String())
	}
}

// A new server's start log asks for the recovery key in plain words, with no
// command: the web app's Settings save it (PROTOCOL §11.1).
func TestStartLogAsksForTheRecoveryKeyInPlainWords(t *testing.T) {
	logs := captureLog(t)
	startServe(t, filepath.Join(t.TempDir(), "data")).stop(t)
	if !strings.Contains(logs.String(), noRecoveryKeyWarning) {
		t.Fatalf("no recovery key warning in the start log:\n%s", logs.String())
	}
	for _, s := range []string{"Settings", "Save recovery key"} {
		if !strings.Contains(noRecoveryKeyWarning, s) {
			t.Errorf("the warning does not point to %q: %s", s, noRecoveryKeyWarning)
		}
	}
	for _, s := range []string{"nottennapod-server", "`", "--", "keys export", "docker"} {
		if strings.Contains(noRecoveryKeyWarning, s) {
			t.Errorf("the warning holds a command (%q): %s", s, noRecoveryKeyWarning)
		}
	}
}

// A recovery key saved from the web app is the keys export's bundle: keys
// install puts lost keys back with it, and restore takes it, through the
// commands and a passphrase file, whose passphrase keeps its spaces and
// multi-byte characters exactly.
func TestRecoveryKeyFromTheWebWorksWithKeysInstallAndRestore(t *testing.T) {
	ctx := context.Background()
	work := t.TempDir()
	data, backups := filepath.Join(work, "data"), filepath.Join(work, "backups")
	srv := startServe(t, data, "--backup-time", "off")
	p := srv.phone(t)
	p.push(t, srv, p.fact("podcast.subscribe", nil, nil, 0, map[string]any{"feed_url": "http://127.0.0.1:9/feed.xml"}))
	pass := "  Grüße, 世界 é passe  "
	browser := map[string]string{"Origin": srv.base, "Cookie": "ntp_session=" + p.session}
	r := srv.do(t, "/v1/admin/keys", map[string]any{"password": "a long enough password", "passphrase": pass}, browser)
	bundle, _ := r["bundle"].(string)
	name, _ := r["file_name"].(string)
	id, _ := r["export_id"].(string)
	if bundle == "" || !strings.HasPrefix(name, "nottennapod-recovery-key-") || id == "" {
		t.Fatalf("export: %v", r)
	}
	if saved := srv.do(t, "/v1/admin/keys/"+id+"/saved", map[string]any{}, browser); saved["keys_exported_at"] != r["created_at"] {
		t.Fatalf("saved: %v, created %v", saved, r["created_at"])
	}
	srv.stop(t)
	file, passFile, trimmed := filepath.Join(work, name), filepath.Join(work, "pass"), filepath.Join(work, "trimmed")
	os.WriteFile(file, []byte(bundle), 0o600)
	os.WriteFile(passFile, []byte(pass+"\n"), 0o600)
	os.WriteFile(trimmed, []byte(strings.TrimSpace(pass)+"\n"), 0o600)
	cmd := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		devNull, _ := os.Open(os.DevNull)
		defer devNull.Close()
		code := run(ctx, args, devNull, &out, &errOut)
		return code, out.String(), errOut.String()
	}
	if code, out, errs := cmd("backup", "--data-dir", data, "--out", backups); code != 0 {
		t.Fatalf("backup: %d %s %s", code, out, errs)
	}
	entries, _ := os.ReadDir(backups)
	if len(entries) != 1 {
		t.Fatalf("backups %v", entries)
	}
	want, err := os.ReadFile(filepath.Join(data, "receipt.keys"))
	if err != nil {
		t.Fatal(err)
	}
	wantSecret, _ := os.ReadFile(filepath.Join(data, "secrets.key"))
	os.Remove(filepath.Join(data, "receipt.keys"))
	os.Remove(filepath.Join(data, "secrets.key"))
	if s, err := storage.Open(ctx, data); err == nil {
		s.Close()
		t.Fatal("a server that lost its keys started")
	}
	if code, _, errs := cmd("keys", "install", "--data-dir", data, "--keys", file, "--passphrase-file", trimmed); code != 1 || !strings.Contains(errs, "passphrase") {
		t.Fatalf("keys install with the passphrase trimmed: %d %s", code, errs)
	}
	if code, out, errs := cmd("keys", "install", "--data-dir", data, "--keys", file, "--passphrase-file", passFile); code != 0 || !strings.Contains(out, "The keys are back") {
		t.Fatalf("keys install: %d %s %s", code, out, errs)
	}
	got, _ := os.ReadFile(filepath.Join(data, "receipt.keys"))
	gotSecret, _ := os.ReadFile(filepath.Join(data, "secrets.key"))
	if !bytes.Equal(got, want) || !bytes.Equal(gotSecret, wantSecret) {
		t.Fatal("keys install put back other keys")
	}
	moved := filepath.Join(work, "new-machine", "data")
	if code, out, errs := cmd("restore", "--data-dir", moved, "--from", filepath.Join(backups, entries[0].Name()), "--keys", file, "--passphrase-file", passFile); code != 0 {
		t.Fatalf("restore: %d %s %s", code, out, errs)
	}
	restored, _ := os.ReadFile(filepath.Join(moved, "receipt.keys"))
	if !bytes.Equal(restored, want) {
		t.Fatal("the restore put back other keys")
	}
	s, err := storage.Open(ctx, moved)
	if err != nil {
		t.Fatalf("the restored server: %v", err)
	}
	s.Close()
}

// The backup command keeps the disks safe as the server's backups do: too
// little free space refuses it with nothing written.
func TestCLIBackupGuarded(t *testing.T) {
	ctx := context.Background()
	work := t.TempDir()
	data, backups := filepath.Join(work, "data"), filepath.Join(work, "backups")
	store, err := storage.Open(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	backupGuard = backupsched.Guard{Free: func(string) (int64, error) { return 512 << 20, nil }}
	t.Cleanup(func() { backupGuard = backupsched.Guard{} })
	code, _, errs := runCmd(t, "backup", "--data-dir", data, "--out", backups)
	if code != 1 || !strings.Contains(errs, "not enough free space for a backup") {
		t.Fatalf("a backup without room: %d %s", code, errs)
	}
	if entries, _ := os.ReadDir(backups); len(entries) != 0 {
		t.Fatalf("a refused backup left %v", entries)
	}
	backupGuard = backupsched.Guard{}
	if code, out, errs := runCmd(t, "backup", "--data-dir", data, "--out", backups); code != 0 || !strings.Contains(out, ".db.gz") {
		t.Fatalf("a backup with room: %d %s %s", code, out, errs)
	}
}
