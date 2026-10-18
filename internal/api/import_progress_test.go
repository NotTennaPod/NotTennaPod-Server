package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// opmlUpload reserves an OPML import for the session and sends its first n
// bytes as chunk 0; it returns the import's ID.
func (e *env) opmlUpload(session string, data []byte, n int) string {
	e.t.Helper()
	sum := sha256.Sum256(data)
	id := e.do(http.MethodPost, "/v1/import", map[string]any{"kind": "opml", "fingerprint": hex.EncodeToString(sum[:]), "size": len(data),
		"file_name": "subscriptions.opml"}, browser(session)).must(http.StatusCreated).str("import_id")
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString(data[:n])},
		browser(session)).must(http.StatusOK)
	return id
}

// writeDB runs one statement on the server's database, as a test arranges a
// state the API cannot reach quickly.
func (e *env) writeDB(query string, args ...any) {
	e.t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(e.dir, "nottennapod.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		e.t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(query, args...); err != nil {
		e.t.Fatalf("%s: %v", query, err)
	}
}

const opmlBody = `<opml version="2.0"><body><outline xmlUrl="https://example.test/feed"/></body></opml>`

// The progress route answers an upload's chunks; cancel ends it (repeatable),
// releasing the account for a new reservation of the same file; another
// account's import is not found, and an unbound session cannot use either.
func TestImportProgressAndCancelEndpoints(t *testing.T) {
	e := newEnv(t, withImports(t))
	admin, _ := e.setupAdmin("owner", "owner-password")
	e.createAccount(admin, "alice", "alice-password", false)
	e.createAccount(admin, "bob", "bob-password", false)
	alice := e.session("alice", "alice-password")
	bob := e.session("bob", "bob-password")
	data := []byte(opmlBody)
	e.do(http.MethodGet, "/v1/import/"+newID()+"/progress", nil, browser(alice)).expect(http.StatusForbidden, "forbidden")
	e.do(http.MethodPost, "/v1/import/"+newID()+"/cancel", nil, browser(alice)).expect(http.StatusForbidden, "forbidden")
	e.registerWeb(alice, newID(), newID()).must(http.StatusOK)
	e.registerWeb(bob, newID(), newID()).must(http.StatusOK)
	id := e.opmlUpload(alice, data, 10)
	p := e.do(http.MethodGet, "/v1/import/"+id+"/progress", nil, browser(alice)).must(http.StatusOK)
	if p.str("status") != "uploading" || !strings.Contains(string(p.raw), `"received":{"bytes":10,"chunks":[[0,10]]}`) || strings.Contains(string(p.raw), "result") {
		t.Fatalf("progress %s", p.raw)
	}
	e.do(http.MethodGet, "/v1/import/"+id+"/progress", nil, browser(bob)).expect(http.StatusNotFound, "not_found")
	e.do(http.MethodPost, "/v1/import/"+id+"/cancel", nil, browser(bob)).expect(http.StatusNotFound, "not_found")
	for range 2 {
		c := e.do(http.MethodPost, "/v1/import/"+id+"/cancel", nil, browser(alice)).must(http.StatusOK)
		if c.str("status") != "failed" || c.str("reason") != "cancelled" || c.str("import_id") != id {
			t.Fatalf("cancel %s", c.raw)
		}
	}
	if p := e.do(http.MethodGet, "/v1/import/"+id+"/progress", nil, browser(alice)).must(http.StatusOK); p.str("status") != "failed" {
		t.Fatalf("after cancel %s", p.raw)
	}
	again := e.opmlUpload(alice, data, len(data))
	if again == id {
		t.Fatal("the same import again")
	}
	e.do(http.MethodPost, "/v1/import/"+again+"/finish", nil, browser(alice)).must(http.StatusOK)
	e.do(http.MethodPost, "/v1/import/"+again+"/cancel", nil, browser(alice)).expect(http.StatusConflict, "import_not_uploading")
}

// A cancel by a session that was signed out, or of an account disabled
// meanwhile, changes nothing: the import stays uploading with its chunk.
func TestCancelRevokedOrDisabledCallerChangesNothingOverHTTP(t *testing.T) {
	e := newEnv(t, withImports(t))
	admin, _ := e.setupAdmin("owner", "owner-password")
	a := e.createAccount(admin, "alice", "alice-password", false)
	alice := e.session("alice", "alice-password")
	e.registerWeb(alice, newID(), newID()).must(http.StatusOK)
	id := e.opmlUpload(alice, []byte(opmlBody), 10)
	still := func(what string) {
		t.Helper()
		if n := e.count("SELECT COUNT(*) FROM imports WHERE import_id = ? AND status = 'uploading'", id); n != 1 {
			t.Fatalf("%s: the import is no longer uploading", what)
		}
		if _, err := os.Stat(filepath.Join(e.srv.cfg.Imports.Dir, id+".upload", "000000")); err != nil {
			t.Fatalf("%s: its chunk: %v", what, err)
		}
	}
	e.setState(admin, a, "disabled").must(http.StatusOK)
	if r := e.do(http.MethodPost, "/v1/import/"+id+"/cancel", nil, browser(alice)); r.status < 400 {
		t.Fatalf("a disabled account's cancel: %s", r.raw)
	}
	still("disabled")
	e.setState(admin, a, "active").must(http.StatusOK)
	alice = e.session("alice", "alice-password")
	e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(alice)).must(http.StatusOK)
	if r := e.do(http.MethodPost, "/v1/import/"+id+"/cancel", nil, browser(alice)).refused(); r.status != http.StatusUnauthorized {
		t.Fatalf("a signed-out session's cancel: %d %s", r.status, r.raw)
	}
	still("signed out")
}

// A progress poll's answer is a few hundred bytes whatever the report holds
// (here 50,000 entries): it never reads or sends the result, which GET
// /v1/import/{id} still returns once. An upload's chunk list is bounded by the
// chunk cap instead.
func TestProgressResponseIsConstantSize(t *testing.T) {
	e := newEnv(t, withImports(t))
	session, account := e.setupAdmin("owner", "owner-password")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	id := e.opmlUpload(session, []byte(opmlBody), 10)
	// An upload holding every chunk the cap allows.
	dir := filepath.Join(e.srv.cfg.Imports.Dir, id+".upload")
	for n := range 2048 {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%06d", n)), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	up := e.do(http.MethodGet, "/v1/import/"+id+"/progress", nil, browser(session)).must(http.StatusOK)
	if n := len(up.raw); n > 32<<10 || !strings.Contains(string(up.raw), "[2047,1]") {
		t.Fatalf("an upload of 2048 chunks answers %d bytes", n)
	}
	var b strings.Builder
	b.WriteString(`{"ambiguous":[],"imported":{"episodes":50000,"favorites":0,"history":0,"inbox":0,"podcasts":1,"queue":0},"merged":[],"skipped":[`)
	for i := range 50000 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"reason":"local_folder","title":"Episode %d"}`, i)
	}
	b.WriteString(`]}`)
	result := b.String()
	for _, st := range []string{"processing", "published"} {
		e.writeDB(`UPDATE imports SET status = ?, result = ?, updated_at = ? WHERE import_id = ?`, st, result, e.srv.cfg.Store.Now().UnixMilli(), id)
		p := e.do(http.MethodGet, "/v1/import/"+id+"/progress", nil, browser(session)).must(http.StatusOK)
		if len(p.raw) >= 2048 || strings.Contains(string(p.raw), "Episode") || p.str("status") != st {
			t.Fatalf("%s: progress answers %d bytes: %.300s", st, len(p.raw), p.raw)
		}
	}
	if full := e.do(http.MethodGet, "/v1/import/"+id, nil, browser(session)).must(http.StatusOK); len(full.raw) < len(result) {
		t.Fatalf("the import's own answer lost its result: %d bytes", len(full.raw))
	}
	var im storage.Import
	if err := e.srv.cfg.Store.Read(context.Background(), func(tx *storage.ReadTx) error {
		var err error
		im, err = tx.Account(account).ImportState(id)
		return err
	}); err != nil || im.Result != "" || im.Status != "published" {
		t.Fatalf("ImportState %v: %d result bytes, %s", err, len(im.Result), im.Status)
	}
}
