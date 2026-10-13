package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Service, files at rest, listening and admin.

// A backup changed to keep revoked tokens (a trigger that
// puts deleted tokens back) is refused, so a token revoked by a password
// reset never works again.
func TestRevokedTokenStaysDeadAfterAModifiedBackupIsRestored(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	root, _ := e.setupAdmin("owner", "correct horse battery")
	id := e.createAccount(root, "alice", "alice password 1", false)
	tok := e.token("alice", "alice password 1", newID(), newID())
	info, err := e.store.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "bundle")
	pass := []byte("fixture passphrase")
	if err := e.store.ExportKeys(ctx, keyFile, pass); err != nil {
		t.Fatal(err)
	}
	bundle, _ := os.ReadFile(keyFile)
	e.resetPassword(root, id, "alice password 2").must(http.StatusOK)
	e.list(bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")
	e.store.Close()
	db, err := sql.Open("sqlite", info.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER keep_tokens AFTER DELETE ON auth_tokens BEGIN INSERT INTO auth_tokens VALUES(OLD.token_hash, OLD.kind,
		OLD.account_id, OLD.install_id, OLD.created_at, OLD.last_used_at, OLD.expires_at); END`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	_, restoreErr := storage.Restore(ctx, storage.RestoreOptions{DataDir: e.dir, Backup: info.Path, Bundle: bundle, Passphrase: pass})
	if restoreErr == nil || !strings.Contains(restoreErr.Error(), "restore refused") {
		t.Errorf("the modified backup was restored: %v", restoreErr)
	}
	reopened, err := storage.Open(ctx, e.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	e.store = reopened
	e.srv.cfg.Store = reopened
	e.list(bearer(tok)).expect(http.StatusUnauthorized, "unauthorized")
}

// /v1/info reports this run's instance ID, the one in the
// data directory's server.instance, for service update's readiness check.
func TestInfoReportsTheInstanceID(t *testing.T) {
	e := newEnv(t)
	if r := e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK); r.str("instance_id") != "" {
		t.Fatalf("an instance ID before the server wrote one: %s", r.raw)
	}
	id, err := e.store.WriteInstance()
	if err != nil {
		t.Fatal(err)
	}
	if onDisk, err := storage.ReadInstance(e.dir); err != nil || onDisk != id {
		t.Fatalf("server.instance holds %q (%v), want %q", onDisk, err, id)
	}
	if got := e.do(http.MethodGet, "/v1/info", nil).must(http.StatusOK).str("instance_id"); got != id {
		t.Fatalf("/v1/info reports %q, want %q", got, id)
	}
}

// Diagnostics are meant to be shared, so every URL in
// them is cut to scheme://host[:port]/…, path and query secrets included.
func TestDiagnosticsCutURLsToTheirHost(t *testing.T) {
	r := newReadEnv(t)
	_, parseErr := feed.Parse(context.Background(),
		[]byte(`<html><head><title>https://user:pw@feeds.example:8443/private/PATH_SECRET/rss?token=QUERY_SECRET</title></head></html>`),
		"https://feeds.example/rss", time.Now())
	var pe *feed.ParseError
	if !errors.As(parseErr, &pe) {
		t.Fatal(parseErr)
	}
	err := r.srv.cfg.Protocol.ServerCommit(context.Background(), r.account, func(c *protocol.ApplyContext) error {
		return c.PublishRecord("podcast", "p1:"+strings.Repeat("ab", 32), "refresh_status", map[string]any{"error": map[string]any{"detail": pe.Detail}})
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.get("/v1/diagnostics").must(http.StatusOK)
	for _, secret := range []string{"QUERY_SECRET", "PATH_SECRET", "user:pw", "private"} {
		if strings.Contains(string(d.raw), secret) {
			t.Errorf("diagnostics keep %q: %s", secret, d.raw)
		}
	}
	if !strings.Contains(string(d.raw), "https://feeds.example:8443/…") {
		t.Errorf("the URL is not cut to its host: %s", d.raw)
	}
}

func TestReduceURLs(t *testing.T) {
	for in, want := range map[string]string{
		"https://example.org": "https://example.org",
		"see http://a.example/x?y=1 and ftp://u@b.example:21": "see http://a.example/… and ftp://b.example:21",
		`"https://a.example/p" ok`:                            `"https://a.example/…" ok`,
		"HTTPS://h.example?token=1":                           "HTTPS://h.example/…",
		"no url here":                                         "no url here",
		// Userinfo whatever its spelling.
		"https://o'brien:pw@host.example/x":      "https://host.example/…",
		`<a href="https://u:pw&#64;h.example/">`: `<a href="https://h.example/…">`,
	} {
		if got := reduceURLs(in); got != want {
			t.Errorf("reduceURLs(%q) = %q, want %q", in, got, want)
		}
	}
}

// A backup request held after authentication while its administrator is demoted
// makes no backup.
func TestBackupChecksTheAdministratorAgain(t *testing.T) {
	e := newEnv(t)
	root, _ := e.setupAdmin("owner", "correct horse battery")
	id := e.createAccount(root, "deputy", "deputy password 1", true)
	deputy := e.session("deputy", "deputy password 1")
	res := delayed(e, http.MethodPost, "/v1/admin/backup", `{}`, browser(deputy), func() {
		e.do(http.MethodPatch, "/v1/admin/accounts/"+id, map[string]any{"is_admin": false}, browser(root)).must(http.StatusOK)
	})
	if res.status != http.StatusForbidden {
		t.Fatalf("a demoted administrator's backup: %d %s", res.status, res.raw)
	}
	if entries, _ := os.ReadDir(filepath.Join(e.dir, "backups")); len(entries) != 0 {
		t.Fatalf("a backup was written: %v", entries)
	}
}

// Diagnostics of another account check the administrator
// flag again in their read; the account list and the service page check the
// caller again.
func TestAdminReadsCheckTheCallerAgain(t *testing.T) {
	t.Run("diagnostics after demotion", func(t *testing.T) {
		e := newEnv(t)
		root, _ := e.setupAdmin("root", "root-password-1")
		id := e.createAccount(root, "bob", "bob-password-1", true)
		bob := e.session("bob", "bob-password-1")
		victim := e.createAccount(root, "victim", "victim-password-1", false)
		afterCredentialRead(e, func() {
			e.adminDo(root, http.MethodPatch, "/v1/admin/accounts/"+id, map[string]any{"is_admin": false}).must(http.StatusOK)
		})
		if got := e.do(http.MethodGet, "/v1/diagnostics?account="+victim, nil, browser(bob)); got.status != http.StatusForbidden {
			t.Fatalf("a demoted administrator read another account's diagnostics: %d %s", got.status, got.raw)
		}
	})
	for _, path := range []string{"/v1/admin/accounts", "/v1/admin/service"} {
		t.Run(path+" after disable", func(t *testing.T) {
			e := newReadEnv(t)
			e.createAccount(e.session, "second", "second-password", true)
			second := e.env.session("second", "second-password")
			e.srv.cfg.Service = func(context.Context) (any, error) { return map[string]any{"data_dir": "/private/service/data"}, nil }
			e.srv.serviceRoutes()
			e.get(path).must(http.StatusOK)
			afterCredentialRead(e.env, func() { e.setState(second, e.account, "disabled").must(http.StatusOK) })
			if got := e.get(path); got.status < 400 || strings.Contains(string(got.raw), "/private/service/data") {
				t.Fatalf("a disabled administrator read %s: %d %s", path, got.status, got.raw)
			}
		})
	}
}

// Repeating a snapshot request does not keep a copy each
// time; the equivalent request reuses its view.
func TestRepeatedSnapshotsShareOneView(t *testing.T) {
	e := newEnv(t, withSync(t))
	e.setupAdmin("owner", "correct horse battery")
	p := e.phoneOf("owner", "correct horse battery")
	p.push(op{seq: 1, fact: p.noteFact(strings.Repeat("a", 200<<10))}).must(http.StatusOK)
	before, _ := filepath.Glob(filepath.Join(e.dir, "views", "*.jsonl"))
	for range 24 {
		e.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": p.install, "acked_install_seq": 0}, p.cred).must(http.StatusOK)
	}
	if views, _ := filepath.Glob(filepath.Join(e.dir, "views", "*.jsonl")); len(views) > len(before)+1 {
		t.Fatalf("24 equivalent snapshot requests kept %d views (%d before)", len(views), len(before))
	}
}

// Diagnostics find URLs by the userinfo rule's own scan, so a secret after an
// apostrophe in a path, a URL written with character references or an encoded
// scheme, and a scheme-relative URL are cut to their host too. Tested through
// the HTTP response.
func TestDiagnosticsCutEveryURLSpelling(t *testing.T) {
	r := newReadEnv(t)
	details := map[string]string{
		"apostrophe":            `https://feeds.example/o'neil/PATH_SECRET1/rss?token=QUERY_SECRET1`,
		"quoted":                `href='https://feeds.example/PATH_SECRET2' title=x`,
		"encoded at":            `https://u:pw&#64;feeds.example/PATH_SECRET3?token=QUERY_SECRET3`,
		"encoded scheme":        `https&colon;&sol;&sol;feeds.example/PATH_SECRET4`,
		"numeric scheme":        `&#104;ttps://feeds.example/PATH_SECRET5`,
		"scheme-relative":       `see //feeds.example/PATH_SECRET6?token=QUERY_SECRET6`,
		"scheme-relative start": `//feeds.example/PATH_SECRET7`,
		"after a control":       "x\x01//feeds.example/PATH_SECRET8",
	}
	n := 0
	for name, detail := range details {
		n++
		id := fmt.Sprintf("p1:%064x", n)
		err := r.srv.cfg.Protocol.ServerCommit(context.Background(), r.account, func(c *protocol.ApplyContext) error {
			return c.PublishRecord("podcast", id, "refresh_status", map[string]any{"error": map[string]any{"detail": detail, "case": name}})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	d := r.get("/v1/diagnostics").must(http.StatusOK)
	for i := 1; i <= 8; i++ {
		for _, secret := range []string{fmt.Sprintf("PATH_SECRET%d", i), fmt.Sprintf("QUERY_SECRET%d", i)} {
			if strings.Contains(string(d.raw), secret) {
				t.Errorf("diagnostics keep %s: %s", secret, d.raw)
			}
		}
	}
	for _, want := range []string{"https://feeds.example/…", "//feeds.example/…"} {
		if !strings.Contains(string(d.raw), want) {
			t.Errorf("diagnostics lack %q: %s", want, d.raw)
		}
	}
}
