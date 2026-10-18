package api

// "Save recovery key" (PROTOCOL §11.1): POST /v1/admin/keys seals the key
// bundle for an administrator's browser, and POST /v1/admin/keys/{id}/saved
// records it once the person confirms the file is saved.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/keys"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

const (
	keyAdmin    = "root"
	keyPassword = "the admin password"
	keyPass     = "a passphrase for the key file"
)

// keyHooks counts the expensive steps: a successful password check and a
// sealing. before runs between reading the login and checking its password,
// during after sealing and before the issue.
type keyHooks struct {
	verified atomic.Int32
	sealed   atomic.Int32
	before   oneShot
	during   oneShot
}

func newKeyEnv(t *testing.T) (*env, *keyHooks) {
	t.Helper()
	h := &keyHooks{}
	var e *env
	e = newEnv(t, func(c *Config) {
		c.afterPasswordVerify = func() { h.verified.Add(1); e.afterVerify.run() }
		c.beforeKeyPassword = h.before.run
		c.duringKeySeal = func() { h.sealed.Add(1); h.during.run() }
	})
	return e, h
}

func (e *env) exportKeys(session, password, passphrase string, opts ...opt) *response {
	e.t.Helper()
	e.addSecret(passphrase)
	return e.do(http.MethodPost, "/v1/admin/keys", map[string]any{"password": password, "passphrase": passphrase},
		append([]opt{browser(session)}, opts...)...)
}

func (e *env) keysSaved(session, id string) *response {
	e.t.Helper()
	return e.do(http.MethodPost, "/v1/admin/keys/"+id+"/saved", nil, browser(session))
}

// noBundle checks a refused export released nothing and recorded nothing.
func (e *env) noBundle(r *response) {
	e.t.Helper()
	r.refused()
	if _, ok := r.body["bundle"]; ok || bytes.Contains(r.raw, []byte("ciphertext")) || r.str("export_id") != "" {
		e.t.Errorf("%s: a refused export released a bundle: %s", r.what, r.raw)
	}
	if n := e.srv.keyExports.pendingCount(); n != 0 {
		e.t.Errorf("%s: %d exports wait for confirmation", r.what, n)
	}
	if at := e.store.KeysExportedAt(); at != 0 {
		e.t.Errorf("%s: keys recorded as exported at %d", r.what, at)
	}
}

// openBundle decrypts an export's bundle and checks it holds this server's
// own keys: the same server ID, receipt keys and secrets key (the sealing is
// randomized, so ciphertexts are never compared).
func (e *env) openBundle(r *response, passphrase []byte) keys.Bundle {
	e.t.Helper()
	b, err := keys.Open([]byte(r.str("bundle")), passphrase)
	if err != nil {
		e.t.Fatalf("the bundle does not open with the passphrase: %v", err)
	}
	serverID, _ := e.store.ServerID(context.Background())
	ks, _ := e.store.ReceiptKeys(context.Background())
	secret, err := os.ReadFile(filepath.Join(e.dir, "secrets.key"))
	if err != nil {
		e.t.Fatal(err)
	}
	if b.ServerID != serverID || !bytes.Equal(b.SecretsKey, secret) || len(b.ReceiptKeys) != len(ks) {
		e.t.Fatalf("the bundle holds other keys: %s %d", b.ServerID, len(b.ReceiptKeys))
	}
	for i, k := range ks {
		if b.ReceiptKeys[i].ID != k.ID || !bytes.Equal(b.ReceiptKeys[i].Key, k.Secret) {
			e.t.Fatalf("receipt key %s differs", k.ID)
		}
	}
	if created, _ := r.body["created_at"].(float64); int64(created) != b.CreatedAt {
		e.t.Errorf("created_at %v, the bundle says %d", r.body["created_at"], b.CreatedAt)
	}
	return b
}

// Only an administrator's own browser session asks for the key bundle;
// everything else is refused before any password check or sealing.
func TestKeyExportAuthorization(t *testing.T) {
	e, h := newKeyEnv(t)
	admin, _ := e.setupAdmin(keyAdmin, keyPassword)
	e.createAccount(admin, "member", "member password", false)
	member := e.session("member", "member password")
	token := e.token(keyAdmin, keyPassword, newID(), newID())
	verified := h.verified.Load()
	body := map[string]any{"password": keyPassword, "passphrase": keyPass}
	e.do(http.MethodPost, "/v1/admin/keys", body, bearer(token)).expect(http.StatusForbidden, "forbidden")
	e.do(http.MethodPost, "/v1/admin/keys", body, bearer(admin), withOrigin(testOrigin)).expect(http.StatusUnauthorized, "unauthorized")
	e.exportKeys(member, "member password", keyPass).expect(http.StatusForbidden, "forbidden")
	e.exportKeys(admin, keyPassword, keyPass, withOrigin("http://elsewhere.test")).expect(http.StatusForbidden, "forbidden")
	e.exportKeys(admin, keyPassword, keyPass, withOrigin("")).expect(http.StatusForbidden, "forbidden")
	e.exportKeys(admin, keyPassword, keyPass, hdr("X-NotTennaPod-Account", newID())).expect(http.StatusConflict, "account_changed")
	e.do(http.MethodPost, "/v1/admin/keys/"+newID()+"/saved", nil, bearer(token)).expect(http.StatusForbidden, "forbidden")
	e.keysSaved(member, newID()).expect(http.StatusForbidden, "forbidden")
	e.clock.Advance(sessionLifetime + time.Hour)
	r := e.exportKeys(admin, keyPassword, keyPass).expect(http.StatusUnauthorized, "unauthorized")
	e.noBundle(r)
	if h.verified.Load() != verified || h.sealed.Load() != 0 {
		t.Errorf("refused requests checked the password %d times and sealed %d", h.verified.Load()-verified, h.sealed.Load())
	}
}

// Authorization is checked again at the moment of issue, after the password
// check and after sealing: a demotion, disable, password reset or change,
// sign-out or expiry meanwhile wins, and no bundle leaves. A rename does not
// change the account; a username taken over by another account is refused.
func TestKeyExportRevalidatesAfterPasswordAndSealing(t *testing.T) {
	type change struct {
		name   string
		status int
		code   string
		run    func(e *env, admin, adminID, other string)
	}
	changes := []change{
		{"demotion", http.StatusForbidden, "forbidden", func(e *env, admin, adminID, other string) {
			e.adminDo(other, http.MethodPatch, "/v1/admin/accounts/"+adminID, map[string]any{"is_admin": false}).must(http.StatusOK)
		}},
		{"disable", http.StatusForbidden, "forbidden", func(e *env, admin, adminID, other string) {
			e.setState(other, adminID, "disabled").must(http.StatusOK)
		}},
		{"password reset", http.StatusUnauthorized, "unauthorized", func(e *env, admin, adminID, other string) {
			e.resetPassword(other, adminID, "a reset password").must(http.StatusOK)
		}},
		{"password changed in this session", http.StatusBadRequest, "password_incorrect", func(e *env, admin, adminID, other string) {
			e.do(http.MethodPost, "/v1/account/password", map[string]any{"current": keyPassword, "new": "a changed password"},
				browser(admin)).must(http.StatusOK)
		}},
		{"sign-out", http.StatusUnauthorized, "unauthorized", func(e *env, admin, adminID, other string) {
			e.do(http.MethodPost, "/v1/auth/web/sign-out", nil, browser(admin)).must(http.StatusOK)
		}},
		{"expiry", http.StatusUnauthorized, "unauthorized", func(e *env, admin, adminID, other string) {
			e.clock.Advance(sessionLifetime + time.Hour)
		}},
	}
	for _, point := range []string{"after the password check", "during sealing"} {
		for _, c := range changes {
			t.Run(point+": "+c.name, func(t *testing.T) {
				e, h := newKeyEnv(t)
				admin, adminID := e.setupAdmin(keyAdmin, keyPassword)
				e.createAccount(admin, "second", "second password", true)
				other := e.session("second", "second password")
				hook := func() { c.run(e, admin, adminID, other) }
				if point == "during sealing" {
					h.during.set(hook)
				} else {
					e.afterVerify.set(hook)
				}
				r := e.exportKeys(admin, keyPassword, keyPass).expect(c.status, c.code)
				e.noBundle(r) // sealed, but never issued
				if h.sealed.Load() != 1 {
					t.Errorf("sealed %d times", h.sealed.Load())
				}
			})
		}
	}
	t.Run("a rename keeps the account", func(t *testing.T) {
		e, h := newKeyEnv(t)
		admin, adminID := e.setupAdmin(keyAdmin, keyPassword)
		e.createAccount(admin, "second", "second password", true)
		other := e.session("second", "second password")
		h.during.set(func() {
			e.adminDo(other, http.MethodPatch, "/v1/admin/accounts/"+adminID, map[string]any{"username": "renamed"}).must(http.StatusOK)
		})
		r := e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
		e.openBundle(r, []byte(keyPass))
	})
	t.Run("a username taken over by another account", func(t *testing.T) {
		e, h := newKeyEnv(t)
		admin, adminID := e.setupAdmin(keyAdmin, keyPassword)
		otherID := e.createAccount(admin, "second", keyPassword, true) // the same password
		other := e.session("second", keyPassword)
		h.before.set(func() {
			e.adminDo(other, http.MethodPatch, "/v1/admin/accounts/"+adminID, map[string]any{"username": "renamed"}).must(http.StatusOK)
			e.adminDo(other, http.MethodPatch, "/v1/admin/accounts/"+otherID, map[string]any{"username": keyAdmin}).must(http.StatusOK)
		})
		e.noBundle(e.exportKeys(admin, keyPassword, keyPass).expect(http.StatusBadRequest, "password_incorrect"))
		if h.sealed.Load() != 0 {
			t.Error("sealed with another account's password")
		}
	})
}

// A wrong password counts in the sign-in limiter, and the limiter's refusal
// applies to both: export failures block signing in and the reverse, without
// any password check while refused.
func TestKeyExportPasswordLimiterSharedWithSignIn(t *testing.T) {
	e, h := newKeyEnv(t)
	admin, _ := e.setupAdmin(keyAdmin, keyPassword)
	for i := 0; i < 5; i++ {
		e.exportKeys(admin, "a wrong password", keyPass).expect(http.StatusBadRequest, "password_incorrect")
	}
	verified := h.verified.Load()
	r := e.exportKeys(admin, keyPassword, keyPass).expect(http.StatusTooManyRequests, "rate_limited")
	if r.retryAfter() < 1 {
		t.Errorf("no retry_after: %s", r.raw)
	}
	e.noBundle(r)
	e.webSignIn(keyAdmin, keyPassword).expect(http.StatusTooManyRequests, "rate_limited")
	if h.verified.Load() != verified || h.sealed.Load() != 0 {
		t.Error("a refused attempt checked the password or sealed")
	}
	e.clock.Advance(2 * time.Second)
	e.openBundle(e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK), []byte(keyPass)) // a success resets the counters
	for i := 0; i < 5; i++ {
		e.webSignIn(keyAdmin, "a wrong password").expect(http.StatusUnauthorized, "unauthorized")
	}
	e.exportKeys(admin, keyPassword, keyPass).expect(http.StatusTooManyRequests, "rate_limited")
	if h.sealed.Load() != 1 {
		t.Errorf("sealed %d times", h.sealed.Load())
	}
}

// The passphrase floor: 12 to 1024 code points, not only whitespace, not the
// password; a body that is not valid UTF-8 or escapes an unpaired surrogate
// is refused before decoding could change it. A valid passphrase is kept
// byte for byte: spaces, multi-byte characters, an escaped surrogate pair
// and an escaped backslash followed by u.
func TestKeyExportPassphraseFloorAndExactBytes(t *testing.T) {
	e, h := newKeyEnv(t)
	admin, _ := e.setupAdmin(keyAdmin, keyPassword)
	for _, p := range []string{
		"eleven char",                       // 11
		strings.Repeat("€", 11),             // 33 bytes, 11 code points
		strings.Repeat(" ", 12),             // whitespace only
		strings.Repeat("\u3000", 12) + "\t", // Unicode whitespace only
		keyPassword,                         // the password
		strings.Repeat("x", 1025),
	} {
		e.noBundle(e.exportKeys(admin, keyPassword, p).expect(http.StatusBadRequest, "passphrase_weak"))
	}
	raw := func(passphraseJSON string) rawBody {
		return rawBody(`{"password":"` + keyPassword + `","passphrase":` + passphraseJSON + `}`)
	}
	for _, body := range []rawBody{
		raw("\"invalid UTF-8 \xff\xfe here\""),
		raw(`"a lone high surrogate \ud800 here"`),
		raw(`"a lone low surrogate \uDC00 here"`),
		raw(`"a high surrogate before a letter \ud83c\u0041"`),
		raw(`"a high surrogate at the end \ud83c"`),
		raw(`"two high surrogates \ud83c\ud83c\udfa7"`),
		rawBody(`{"password":"` + keyPassword + ` \ud800","passphrase":"` + keyPass + `"}`),
	} {
		e.noBundle(e.do(http.MethodPost, "/v1/admin/keys", body, browser(admin)).expect(http.StatusBadRequest, "passphrase_invalid"))
	}
	if h.sealed.Load() != 0 || h.verified.Load() != 0 {
		t.Errorf("refused passphrases sealed %d times, checked passwords %d times", h.sealed.Load(), h.verified.Load())
	}
	exact := []struct {
		body rawBody
		want string
	}{
		{raw(`"  Grüße, 世界 \u00e9 passe  "`), "  Grüße, 世界 é passe  "},
		{raw(`"an escaped pair \ud83c\udfa7 is kept"`), "an escaped pair 🎧 is kept"},
		{raw(`"\\ud800 is a backslash and text"`), `\ud800 is a backslash and text`},
	}
	for _, x := range exact {
		r := e.do(http.MethodPost, "/v1/admin/keys", x.body, browser(admin)).must(http.StatusOK)
		e.openBundle(r, []byte(x.want))
		for _, near := range []string{strings.TrimSpace(x.want), strings.ReplaceAll(x.want, "é", "e\u0301"), x.want + " "} {
			if near == x.want {
				continue
			}
			if _, err := keys.Open([]byte(r.str("bundle")), []byte(near)); err == nil {
				t.Errorf("the bundle sealed under %q opens with %q", x.want, near)
			}
		}
	}
	if !unpairedSurrogate([]byte(`["\\\ud800"]`)) || unpairedSurrogate([]byte(`["\\\\ud800"]`)) {
		t.Error("escapes after backslashes are misread")
	}
}

// The bundle is the keys export's format and opens to this server's keys, and
// the real install path takes it: storage.InstallKeys puts the keys back into
// a stopped copy that lost them.
func TestKeyExportInteroperates(t *testing.T) {
	ctx := context.Background()
	e, _ := newKeyEnv(t)
	admin, _ := e.setupAdmin(keyAdmin, keyPassword)
	r := e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
	b := e.openBundle(r, []byte(keyPass))
	if want := "nottennapod-recovery-key-" + time.UnixMilli(b.CreatedAt).UTC().Format("20060102") + ".json"; r.str("file_name") != want {
		t.Errorf("file name %q, want %q", r.str("file_name"), want)
	}
	var header struct {
		Format  string `json:"format"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal([]byte(r.str("bundle")), &header); err != nil || header.Format != keys.Format || header.Version != keys.Version {
		t.Errorf("bundle header %+v %v", header, err)
	}
	again := e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
	e.openBundle(again, []byte(keyPass))
	if again.str("bundle") == r.str("bundle") || again.str("export_id") == r.str("export_id") {
		t.Error("two exports are identical")
	}
	wantKeys, _ := e.store.ReceiptKeys(ctx)
	secret, _ := os.ReadFile(filepath.Join(e.dir, "secrets.key"))
	e.store.Close()
	copyDir := t.TempDir()
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		if !en.Type().IsRegular() || en.Name() == "receipt.keys" || en.Name() == "secrets.key" || en.Name() == "server.lock" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(e.dir, en.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copyDir, en.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bundle := []byte(r.str("bundle"))
	if err := storage.InstallKeys(ctx, copyDir, bundle, []byte(" "+keyPass)); err == nil {
		t.Fatal("installed with another passphrase")
	}
	if err := storage.InstallKeys(ctx, copyDir, bundle, []byte(keyPass)); err != nil {
		t.Fatalf("keys install with the web's bundle: %v", err)
	}
	s, err := storage.Open(ctx, copyDir)
	if err != nil {
		t.Fatalf("after installing the keys: %v", err)
	}
	defer s.Close()
	gotKeys, _ := s.ReceiptKeys(ctx)
	gotSecret, _ := os.ReadFile(filepath.Join(copyDir, "secrets.key"))
	if len(gotKeys) != len(wantKeys) || gotKeys[0].ID != wantKeys[0].ID || !bytes.Equal(gotKeys[0].Secret, wantKeys[0].Secret) || !bytes.Equal(gotSecret, secret) {
		t.Error("the installed keys differ from the server's")
	}
}

// Sealing is not saving: the export is recorded only when the person
// confirms, at the bundle's time, once per export; a repeat answers the
// same. Unknown, other accounts', expired and forgotten (restart) exports
// are not found, and expired ones are dropped.
func TestKeyExportRecordsOnlyWhenSaved(t *testing.T) {
	e, _ := newKeyEnv(t)
	admin, _ := e.setupAdmin(keyAdmin, keyPassword)
	e.createAccount(admin, "second", "second password", true)
	other := e.session("second", "second password")
	r := e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
	b := e.openBundle(r, []byte(keyPass))
	if e.store.KeysExportedAt() != 0 {
		t.Fatal("sealing recorded an export")
	}
	id := r.str("export_id")
	e.keysSaved(admin, newID()).expect(http.StatusNotFound, "not_found")
	e.keysSaved(other, id).expect(http.StatusNotFound, "not_found")
	if e.store.KeysExportedAt() != 0 {
		t.Fatal("another account's confirmation recorded the export")
	}
	for i := 0; i < 2; i++ {
		s := e.keysSaved(admin, id).must(http.StatusOK)
		if at, _ := s.body["keys_exported_at"].(float64); int64(at) != b.CreatedAt || e.store.KeysExportedAt() != b.CreatedAt {
			t.Fatalf("confirmation %d: %s, store %d, bundle %d", i+1, s.raw, e.store.KeysExportedAt(), b.CreatedAt)
		}
	}
	e.clock.Advance(30 * time.Minute)
	late := e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
	e.clock.Advance(keyExportLife)
	e.adminDo(admin, http.MethodGet, "/v1/admin/accounts", nil).must(http.StatusOK) // the hourly session renewal, first
	e.keysSaved(admin, late.str("export_id")).expect(http.StatusNotFound, "not_found")
	e.keysSaved(admin, id).expect(http.StatusNotFound, "not_found")
	if n := e.srv.keyExports.pendingCount(); n != 0 {
		t.Errorf("%d expired exports are kept", n)
	}
	if e.store.KeysExportedAt() != b.CreatedAt {
		t.Error("an expired export changed the record")
	}
	before := e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
	srv, err := New(e.srv.cfg) // a restart forgets what waits for confirmation
	if err != nil {
		t.Fatal(err)
	}
	e.srv = srv
	e.keysSaved(admin, before.str("export_id")).expect(http.StatusNotFound, "not_found")
}

// One sealing at a time: parallel requests are told the server is busy
// without a password check. At most ten sealings an hour for the whole
// server, refused before any password check or sealing; wrong passwords do
// not use them up.
func TestKeyExportConcurrencyAndCap(t *testing.T) {
	e, h := newKeyEnv(t)
	admin, _ := e.setupAdmin(keyAdmin, keyPassword)
	entered, release := make(chan struct{}), make(chan struct{})
	h.during.set(func() { close(entered); <-release })
	first := make(chan *response, 1)
	go func() { first <- e.exportKeys(admin, keyPassword, keyPass) }()
	<-entered
	verified := h.verified.Load()
	var wg sync.WaitGroup
	statuses := make([]int, 4)
	for i := range statuses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := e.exportKeys(admin, keyPassword, keyPass)
			if r.str("error") == "server_busy" && r.retryAfter() >= 1 {
				statuses[i] = r.status
			}
		}()
	}
	wg.Wait()
	close(release)
	if r := <-first; r.status != http.StatusOK {
		t.Fatalf("the first export: %d %s", r.status, r.raw)
	}
	for i, s := range statuses {
		if s != http.StatusServiceUnavailable {
			t.Errorf("parallel export %d: %d, want 503 server_busy", i, s)
		}
	}
	if h.verified.Load() != verified {
		t.Error("a busy refusal checked the password")
	}
	for i := 1; i < keySealsPerHour; i++ {
		if i == keySealsPerHour-1 {
			for j := 0; j < 4; j++ {
				e.exportKeys(admin, "a wrong password", keyPass).expect(http.StatusBadRequest, "password_incorrect")
			}
		}
		e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
	}
	verified, sealed := h.verified.Load(), h.sealed.Load()
	r := e.exportKeys(admin, keyPassword, keyPass).expect(http.StatusTooManyRequests, "rate_limited")
	if r.retryAfter() < 1 || h.verified.Load() != verified || h.sealed.Load() != sealed {
		t.Errorf("the eleventh sealing: %s, password checks %d, sealings %d", r.raw, h.verified.Load()-verified, h.sealed.Load()-sealed)
	}
	if sealed != keySealsPerHour {
		t.Errorf("%d sealings", sealed)
	}
	e.clock.Advance(time.Hour)
	e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
}

// Neither handler logs the password, the passphrase or the bundle, at any
// level: on success, a wrong password, a refusal at issue, a refused
// passphrase, or a panic while sealing (logged with a fixed message).
func TestKeyExportNothingLogged(t *testing.T) {
	var buf syncBuf
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	e, h := newKeyEnv(t)
	admin, adminID := e.setupAdmin(keyAdmin, keyPassword)
	e.createAccount(admin, "second", "second password", true)
	other := e.session("second", "second password")
	r := e.exportKeys(admin, keyPassword, keyPass).must(http.StatusOK)
	e.keysSaved(admin, r.str("export_id")).must(http.StatusOK)
	bundle := r.str("bundle")
	e.exportKeys(admin, "a wrong password", keyPass).expect(http.StatusBadRequest, "password_incorrect")
	e.exportKeys(admin, keyPassword, "eleven char").expect(http.StatusBadRequest, "passphrase_weak")
	e.do(http.MethodPost, "/v1/admin/keys", rawBody(`{"password":"`+keyPassword+`","passphrase":"`+keyPass+`\ud800"}`), browser(admin)).
		expect(http.StatusBadRequest, "passphrase_invalid")
	h.during.set(func() { panic("sealing broke: " + keyPass + " " + keyPassword) })
	p := e.exportKeys(admin, keyPassword, keyPass).expect(http.StatusServiceUnavailable, "server_busy")
	if bytes.Contains(p.raw, []byte(keyPass)) || bytes.Contains(p.raw, []byte("ciphertext")) {
		t.Errorf("the panic's answer: %s", p.raw)
	}
	h.during.set(func() {
		e.adminDo(other, http.MethodPatch, "/v1/admin/accounts/"+adminID, map[string]any{"is_admin": false}).must(http.StatusOK)
	})
	e.exportKeys(admin, keyPassword, keyPass).expect(http.StatusForbidden, "forbidden")
	logs := buf.String()
	if !strings.Contains(logs, "making a recovery key failed") {
		t.Errorf("the panic was not logged:\n%s", logs)
	}
	var f struct {
		Ciphertext string `json:"ciphertext"`
		KDF        struct {
			Salt string `json:"salt"`
		} `json:"kdf"`
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal([]byte(bundle), &f); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{keyPassword, keyPass, "a wrong password", f.Ciphertext, f.Ciphertext[:16], f.KDF.Salt, f.Nonce} {
		if strings.Contains(logs, secret) {
			t.Errorf("the log holds %q:\n%s", secret, logs)
		}
	}
}

// syncBuf is a log buffer safe for the server's goroutines.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var _ io.Writer = (*syncBuf)(nil)
