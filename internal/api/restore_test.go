package api

// PROTOCOL §11 over HTTP: the administrator's backup (PROTOCOL §5) and clients after a
// restore (PROTOCOL §5): old cursors get lineage_mismatch, old tokens 401, signing in
// again with the same installation ID and nonce resumes the installation, a
// token revoked after the backup stays dead, a password reset after the backup
// survives, and an account deleted after the backup returns disabled.

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/auth"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func init() {
	errorStatus["backup_running"] = []int{409}
}

// withSyncBox is withSync with the data directory's secrets key, as the server
// runs: secrets and staged parts are sealed.
func withSyncBox(t *testing.T) func(*Config) {
	return func(c *Config) {
		withSync(t)(c)
		box, err := sealed.LoadOrCreate(c.Store.Dir())
		if err != nil {
			t.Fatal(err)
		}
		c.Protocol.Box = box
	}
}

// reopen starts the server again on the data directory, as after a restore.
func (e *env) reopen(opts ...func(*Config)) {
	e.t.Helper()
	store, err := storage.Open(context.Background(), e.dir)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { store.Close() })
	e.store = store
	cfg := Config{Store: store, Hasher: auth.NewHasher(fastParams), Limiter: auth.NewLimiter(e.clock.Now), Setup: &SetupState{},
		Now: e.clock.Now, afterPasswordVerify: e.afterVerify.run, beforeIssue: e.beforeIssue.run}
	for _, o := range opts {
		o(&cfg)
	}
	if e.srv, err = New(cfg); err != nil {
		e.t.Fatal(err)
	}
}

func TestAdministratorBackup(t *testing.T) {
	t.Parallel()
	running := make(chan struct{})
	release := make(chan struct{})
	e := newEnv(t, func(c *Config) {
		c.duringBackup = func() {
			select {
			case running <- struct{}{}:
				<-release
			default:
			}
		}
	})
	admin, _ := e.setupAdmin("root", "root-password-1")
	e.createAccount(admin, "alice", "alice-password-1", false)
	r := e.adminDo(admin, http.MethodPost, "/v1/admin/backup", nil).must(http.StatusOK)
	file := r.str("file")
	size, _ := r.body["size"].(float64)
	taken, _ := r.body["taken_at"].(float64)
	dir := filepath.Join(e.dir, "backups")
	fi, err := os.Stat(filepath.Join(dir, file))
	if err != nil || fi.Size() != int64(size) || fi.Mode().Perm() != 0o600 || taken < float64(time.Now().Add(-time.Minute).UnixMilli()) {
		t.Fatalf("backup %s: %v %v (size %v, taken %v)", r.raw, fi, err, size, taken)
	}
	if di, _ := os.Stat(dir); di.Mode().Perm() != 0o700 {
		t.Fatalf("backup directory mode %v", di.Mode().Perm())
	}
	if err := storage.VerifyDatabaseFile(context.Background(), filepath.Join(dir, file), -1); err != nil {
		t.Fatal(err)
	}
	// Only an administrator's web session.
	e.do(http.MethodPost, "/v1/admin/backup", nil, browser(e.session("alice", "alice-password-1"))).expect(http.StatusForbidden, "forbidden")
	e.do(http.MethodPost, "/v1/admin/backup", nil, bearer(e.token("root", "root-password-1", newID(), newID()))).expect(http.StatusForbidden, "forbidden")
	e.do(http.MethodPost, "/v1/admin/backup", nil).expect(http.StatusUnauthorized, "unauthorized")
	// One at a time.
	done := make(chan *response, 1)
	go func() { done <- e.adminDo(admin, http.MethodPost, "/v1/admin/backup", nil) }()
	<-running
	e.adminDo(admin, http.MethodPost, "/v1/admin/backup", nil).expect(http.StatusConflict, "backup_running")
	close(release)
	(<-done).must(http.StatusOK)
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Fatalf("%d backups, want 2", len(entries))
	}
}

func TestClientsAfterARestore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newEnv(t, withSyncBox(t))
	admin, _ := e.setupAdmin("root", "root-password-1")
	aliceID := e.createAccount(admin, "alice", "alice-password-1", false)
	e.createAccount(admin, "bob", "bob-password-1", false)
	carolID := e.createAccount(admin, "carol", "carol-password-1", false)
	rootToken := e.token("root", "root-password-1", newID(), newID())
	alice := e.phoneOf("alice", "alice-password-1")
	bob := e.phoneOf("bob", "bob-password-1")
	secretID := newID()
	body := alice.pushBody(op{seq: 1, fact: alice.noteFact("before the backup")})
	body["secrets"] = []any{map[string]any{"secret_id": secretID, "username": "feed-user", "password": "feed-password"}}
	if got := alice.statuses(e.do(http.MethodPost, "/v1/push", body, alice.cred)); !slices.Equal(got, []string{"accepted"}) {
		t.Fatalf("push: %v", got)
	}
	backup := e.adminDo(admin, http.MethodPost, "/v1/admin/backup", nil).must(http.StatusOK).str("file")

	// After the backup.
	if got := alice.statuses(alice.push(op{seq: 2, fact: alice.noteFact("after the backup")})); !slices.Equal(got, []string{"accepted"}) {
		t.Fatalf("push: %v", got)
	}
	e.revoke(bob.cred, bob.install).must(http.StatusOK)
	e.resetPassword(admin, aliceID, "alice-password-2").must(http.StatusOK)
	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+carolID+"/delete", map[string]any{"confirm_username": "carol"}).must(http.StatusOK)
	bundle := filepath.Join(t.TempDir(), "keys.json")
	pass := []byte("a long passphrase")
	if err := e.store.ExportKeys(ctx, bundle, pass); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(bundle)

	// The restore, with the server stopped.
	e.store.Close()
	rep, err := storage.Restore(ctx, storage.RestoreOptions{DataDir: e.dir, Backup: filepath.Join(e.dir, "backups", backup), Bundle: data, Passphrase: pass})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rep.ReturnedDisabled, []string{"carol"}) || !slices.Contains(rep.LoginsCarried, "alice") {
		t.Fatalf("report %+v", rep)
	}
	e.reopen(withSyncBox(t))

	// Old tokens and sessions: 401.
	for name, cred := range map[string]opt{"root's token": bearer(rootToken), "alice's token": alice.cred, "root's session": browser(admin)} {
		if r := e.list(cred); r.status != http.StatusUnauthorized {
			t.Errorf("%s after the restore: %d", name, r.status)
		}
	}
	// A token revoked after the backup never works again (its installation is
	// not revoked in the backup, but every token is gone).
	e.list(bob.cred).expect(http.StatusUnauthorized, "unauthorized")
	// A password reset after the backup survives.
	e.signIn("alice", "alice-password-1", alice.install, alice.nonce).expect(http.StatusUnauthorized, "unauthorized")
	// Signing in again with the same installation ID and nonce resumes it.
	token := e.token("alice", "alice-password-2", alice.install, alice.nonce)
	alice.cred, alice.token = bearer(token), token
	installs := e.installations(alice.cred)
	if len(installs) != 1 || installs[alice.install]["revoked"] != false || installs[alice.install]["current"] != true {
		t.Fatalf("installations after signing in again: %v", installs)
	}
	// Old cursors get lineage_mismatch.
	alice.push(op{seq: 3, fact: alice.noteFact("on the old cursor")}).expect(http.StatusConflict, "lineage_mismatch")
	e.do(http.MethodPost, "/v1/pull", map[string]any{"cursor": alice.cursor, "install_id": alice.install, "acked_install_seq": 1},
		alice.cred).expect(http.StatusConflict, "lineage_mismatch")
	// The secret sealed before the backup opens after it.
	if r := e.do(http.MethodGet, "/v1/secrets/"+secretID, nil, alice.cred).must(http.StatusOK); r.str("username") != "feed-user" {
		t.Fatalf("secret %s", r.raw)
	}
	// An account deleted after the backup returns disabled.
	admin = e.session("root", "root-password-1")
	if state := e.accounts(admin)["carol"]["state"]; state != "disabled" {
		t.Fatalf("carol is %v", state)
	}
	e.signIn("carol", "carol-password-1", newID(), newID()).expect(http.StatusForbidden, "forbidden")
	// A new attachment sees the backup's library at the new epoch.
	fresh := e.phoneOf("alice", "alice-password-2")
	if fresh.cursor == alice.cursor {
		t.Fatal("the head did not change lineage")
	}
}
