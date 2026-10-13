package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Deleting an account removes its import's files: an uploaded OPML
// chunk holding feed credentials does not stay on disk.
func TestDeletingAnAccountRemovesItsImportFiles(t *testing.T) {
	e := newEnv(t, withImports(t), withReads(t))
	admin, _ := e.setupAdmin("owner", "owner-password")
	a := e.createAccount(admin, "alice", "alice-password", false)
	sess := e.session("alice", "alice-password")
	e.registerWeb(sess, newID(), newID()).must(http.StatusOK)
	data := []byte(`<opml><body><outline xmlUrl="https://alice:secret_pw@example.test/feed"/></body></opml>`)
	sum := sha256.Sum256(data)
	id := e.do(http.MethodPost, "/v1/import", map[string]any{"kind": "opml", "fingerprint": hex.EncodeToString(sum[:]), "size": len(data),
		"file_name": "subscriptions.opml"}, browser(sess)).must(http.StatusCreated).str("import_id")
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString(data)},
		browser(sess)).must(http.StatusOK)
	dir := e.srv.cfg.Imports.Dir
	if _, err := os.Stat(filepath.Join(dir, id+".upload", "000000")); err != nil {
		t.Fatalf("precondition: the chunk is stored: %v", err)
	}
	e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+a+"/delete", map[string]any{"confirm_username": "alice"}).must(http.StatusOK)
	if n := e.count("SELECT COUNT(*) FROM imports WHERE account_id = ?", a); n != 0 {
		t.Fatalf("import rows remain: %d", n)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range entries {
		if strings.Contains(x.Name(), id) {
			t.Errorf("the deleted account's import file %s stays", x.Name())
		}
	}
}

// An import's failure reason shown to its owner is a fixed message; a
// raw internal error, here a path of the server's, goes to its log only.
func TestImportFailureReasonNamesNoServerPath(t *testing.T) {
	e := newEnv(t, withImports(t))
	admin, _ := e.setupAdmin("owner", "owner-password")
	e.createAccount(admin, "alice", "alice-password", false)
	sess := e.session("alice", "alice-password")
	e.registerWeb(sess, newID(), newID()).must(http.StatusOK)
	missing := filepath.Join(e.dir, "private-staging-parent", "missing.db")
	r := e.srv.cfg.Imports
	r.RetryWait = time.Nanosecond
	r.OpenStaging = func(context.Context, string) (*storage.Store, error) {
		_, err := os.Open(missing)
		return nil, err
	}
	data := []byte(`<opml version="2.0"><body><outline xmlUrl="https://example.test/feed"/></body></opml>`)
	sum := sha256.Sum256(data)
	id := e.do(http.MethodPost, "/v1/import", map[string]any{"kind": "opml", "fingerprint": hex.EncodeToString(sum[:]), "size": len(data),
		"file_name": "subscriptions.opml"}, browser(sess)).must(http.StatusCreated).str("import_id")
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString(data)},
		browser(sess)).must(http.StatusOK)
	e.do(http.MethodPost, "/v1/import/"+id+"/finish", nil, browser(sess)).must(http.StatusOK)
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := e.do(http.MethodGet, "/v1/import/"+id, nil, browser(sess)).must(http.StatusOK)
		if got.str("status") == "failed" {
			if reason := got.str("reason"); strings.Contains(reason, e.dir) || !strings.HasPrefix(reason, "server_error: ") {
				t.Errorf("reason %q", reason)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the import did not finish")
		}
		runtime.Gosched()
	}
}

// When a deleted account's import file cannot be removed, the deletion still
// succeeds (the account is gone) but says so with cleanup_pending, and the
// runner keeps the file pending; once the file can go, the next start removes
// it.
func TestAnAccountDeletionSaysWhenItsImportFilesRemain(t *testing.T) {
	e := newEnv(t, withImports(t))
	admin, _ := e.setupAdmin("owner", "owner-password")
	a := e.createAccount(admin, "alice", "alice-password", false)
	sess := e.session("alice", "alice-password")
	e.registerWeb(sess, newID(), newID()).must(http.StatusOK)
	data := []byte(`<opml><body><outline xmlUrl="https://alice:secret_pw@host.test/feed"/></body></opml>`)
	sum := sha256.Sum256(data)
	id := e.do(http.MethodPost, "/v1/import", map[string]any{"kind": "opml", "fingerprint": hex.EncodeToString(sum[:]), "size": len(data),
		"file_name": "a.opml"}, browser(sess)).must(http.StatusCreated).str("import_id")
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString(data)},
		browser(sess)).must(http.StatusOK)
	r := e.srv.cfg.Imports
	dir := filepath.Join(r.Dir, id+".upload")
	if err := os.Chmod(dir, 0o500); err != nil { // its chunk cannot be unlinked
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	got := e.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+a+"/delete", map[string]any{"confirm_username": "alice"}).must(http.StatusOK)
	if !strings.Contains(string(got.raw), `"cleanup_pending":true`) {
		t.Errorf("the deletion did not say its cleanup is pending: %s", got.raw)
	}
	if n := e.count("SELECT COUNT(*) FROM imports WHERE account_id = ?", a); n != 0 {
		t.Errorf("import rows remain: %d", n)
	}
	if p := r.PendingCleanup(); len(p) == 0 {
		t.Error("the runner has nothing pending")
	}
	r.Stop()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) || len(r.PendingCleanup()) != 0 {
		t.Errorf("after a start: %v, pending %v", err, r.PendingCleanup())
	}
}
