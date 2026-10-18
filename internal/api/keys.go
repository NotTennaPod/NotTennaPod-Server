package api

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// "Save recovery key" (PROTOCOL §11.1): an administrator's web session asks
// for the key bundle with the account's password and a passphrase of their
// choosing, and the browser saves it as a file. Sealing is not saving: the
// export counts only once the person confirms the file is saved, through
// POST /v1/admin/keys/{export_id}/saved. Neither handler logs or quotes the
// body, and no bundle or passphrase is kept.

const (
	keySealsPerHour = 10        // sealings per hour, for the whole server
	keyExportLife   = time.Hour // how long an export can be confirmed
	minPassphrase   = 12        // code points
	maxPassphrase   = 1024
)

var (
	errPassphraseInvalid = fail(http.StatusBadRequest, "passphrase_invalid", "The password and the passphrase must be valid text.")
	errPassphraseWeak    = fail(http.StatusBadRequest, "passphrase_weak",
		"The passphrase needs 12 to 1024 characters, not only spaces, and must not be the password.")
	errPasswordIncorrect = fail(http.StatusBadRequest, "password_incorrect", "The password is incorrect.")
	errKeySealFailed     = &apiError{status: http.StatusServiceUnavailable, code: "server_busy",
		message: "The server could not make the recovery key; try again.", retryAfter: 5 * time.Second}
)

// keyExports is the running server's recovery-key state: the one sealing at
// a time, the sealings of the last hour, and the exports waiting for their
// confirmation (forgotten by a restart, which leaves the warning).
type keyExports struct {
	busy    atomic.Bool
	mu      sync.Mutex
	seals   []time.Time // when each sealing of the last hour was reserved
	pending map[string]pendingExport
}

type pendingExport struct {
	account   string
	createdAt int64 // the bundle's created_at
	expires   time.Time
}

func newKeyExports() *keyExports { return &keyExports{pending: map[string]pendingExport{}} }

// reserve takes one of the hour's sealings, or says how long until one is free.
func (k *keyExports) reserve(now time.Time) (time.Duration, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	kept := k.seals[:0]
	oldest := now
	for _, t := range k.seals {
		if now.Sub(t) < time.Hour {
			kept = append(kept, t)
			if t.Before(oldest) {
				oldest = t
			}
		}
	}
	k.seals = kept
	if len(k.seals) >= keySealsPerHour {
		return max(oldest.Add(time.Hour).Sub(now), time.Second), false
	}
	k.seals = append(k.seals, now)
	return 0, true
}

// release gives back a reservation whose sealing never ran.
func (k *keyExports) release(at time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for i, t := range k.seals {
		if t.Equal(at) {
			k.seals = append(k.seals[:i], k.seals[i+1:]...)
			return
		}
	}
}

// pruneLocked drops expired exports.
func (k *keyExports) pruneLocked(now time.Time) {
	for id, p := range k.pending {
		if !now.Before(p.expires) {
			delete(k.pending, id)
		}
	}
}

func (k *keyExports) note(id string, p pendingExport, now time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.pruneLocked(now)
	k.pending[id] = p
}

// find returns the account's unexpired export. It stays until it expires,
// so a repeated confirmation gets the same answer.
func (k *keyExports) find(id, account string, now time.Time) (pendingExport, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.pruneLocked(now)
	p, ok := k.pending[id]
	if !ok || p.account != account {
		return pendingExport{}, false
	}
	return p, true
}

func (k *keyExports) pendingCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.pending)
}

func newExportID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type keysBody struct {
	Password   string `json:"password"`
	Passphrase string `json:"passphrase"`
}

// decodeKeysBody reads the body without changing a byte of the passphrase:
// invalid UTF-8 and a \u escape of an unpaired surrogate, which decoding
// would turn into U+FFFD, are refused first. Errors never quote the body.
func (req *request) decodeKeysBody() (keysBody, error) {
	var body keysBody
	raw, err := io.ReadAll(req.r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return body, fail(http.StatusRequestEntityTooLarge, "payload_too_large", "The request is too large.")
		}
		return body, invalid("The request body could not be read.")
	}
	if !utf8.Valid(raw) || unpairedSurrogate(raw) {
		return body, errPassphraseInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return body, invalid("The request body is not valid: it takes a password and a passphrase.")
	}
	if _, err := dec.Token(); err != io.EOF {
		return body, invalid("Unexpected data after the request body.")
	}
	return body, nil
}

// unpairedSurrogate reports whether a JSON text escapes a UTF-16 surrogate
// (\uD800–\uDFFF) that is not half of a high-low pair. A valid pair, and an
// escaped backslash followed by u (\\u), are not. Malformed escapes are left
// to the decoder, which refuses them.
func unpairedSurrogate(raw []byte) bool {
	hex4 := func(i int) (rune, bool) {
		if i+4 > len(raw) {
			return 0, false
		}
		var r rune
		for _, c := range raw[i : i+4] {
			switch {
			case c >= '0' && c <= '9':
				r = r<<4 | rune(c-'0')
			case c >= 'a' && c <= 'f':
				r = r<<4 | rune(c-'a'+10)
			case c >= 'A' && c <= 'F':
				r = r<<4 | rune(c-'A'+10)
			default:
				return 0, false
			}
		}
		return r, true
	}
	inString := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !inString {
			inString = c == '"'
			continue
		}
		switch {
		case c == '"':
			inString = false
		case c == '\\' && i+1 < len(raw) && raw[i+1] != 'u':
			i++ // a one-character escape, such as \\ or \"
		case c == '\\' && i+1 < len(raw):
			r, ok := hex4(i + 2)
			if !ok {
				return false
			}
			i += 5
			switch {
			case r >= 0xDC00 && r <= 0xDFFF:
				return true
			case r >= 0xD800 && r <= 0xDBFF:
				if i+2 < len(raw) && raw[i+1] == '\\' && raw[i+2] == 'u' {
					if lo, ok := hex4(i + 3); ok && lo >= 0xDC00 && lo <= 0xDFFF {
						i += 6
						continue
					}
				}
				return true
			}
		}
	}
	return false
}

// checkPassphrase applies the passphrase floor: 12 to 1024 code points, not
// only whitespace, not the password. Nothing is trimmed or normalized.
func checkPassphrase(passphrase, password string) error {
	n := utf8.RuneCountInString(passphrase)
	if n < minPassphrase || n > maxPassphrase || strings.TrimSpace(passphrase) == "" || passphrase == password {
		return errPassphraseWeak
	}
	return nil
}

// adminKeys is POST /v1/admin/keys {password, passphrase}: the key bundle,
// sealed under the passphrase, for the browser to save. The order is fixed:
// the floor, the hour's cap (before any Argon2 work), the one-at-a-time
// sealing, the password of the caller's own login, sealing outside any
// transaction, and a final check of the session and the login at the
// moment of issue. Nothing is recorded until the saved confirmation.
func (req *request) adminKeys() (int, any, error) {
	body, err := req.decodeKeysBody()
	if err != nil {
		return 0, nil, err
	}
	if err := checkPassphrase(body.Passphrase, body.Password); err != nil {
		return 0, nil, err
	}
	k := req.s.keyExports
	reserved := req.s.cfg.Now()
	if wait, ok := k.reserve(reserved); !ok {
		return 0, nil, &apiError{status: http.StatusTooManyRequests, code: "rate_limited",
			message: "Recovery keys were made too often in the last hour; try again later.", retryAfter: wait}
	}
	sealing := false
	defer func() {
		if !sealing {
			k.release(reserved) // only sealings count toward the cap
		}
	}()
	if !k.busy.CompareAndSwap(false, true) {
		return 0, nil, errBusy
	}
	defer k.busy.Store(false)
	account := req.principal.accountID()
	var login storage.Login
	err = req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		var err error
		login, err = r.LoginByAccount(account)
		return err
	})
	if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNoAccount) {
		return 0, nil, errUnauthorized
	}
	if err != nil {
		return 0, nil, err
	}
	if req.s.cfg.beforeKeyPassword != nil {
		req.s.cfg.beforeKeyPassword()
	}
	// The sign-in limiter and its failures are shared with signing in.
	v, err := req.verifyPassword(login.Username, body.Password)
	if errors.Is(err, errBadCreds) {
		return 0, nil, errPasswordIncorrect
	}
	if err != nil {
		return 0, nil, err
	}
	if v.login.AccountID != account {
		return 0, nil, errPasswordIncorrect // the username moved to another account meanwhile
	}
	sealing = true
	bundle, createdAt, err := req.sealKeyBundle([]byte(body.Passphrase))
	if err != nil {
		return 0, nil, err
	}
	// The moment of issue: a sign-out, expiry, disable, demotion or
	// password change since authentication wins, and the bundle is dropped.
	err = req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		if _, err := req.revalidate(tx); err != nil {
			return err
		}
		v.rehash = ""
		if err := v.recheck(tx); err != nil {
			if errors.Is(err, errBadCreds) {
				return errPasswordIncorrect
			}
			return err
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	now := req.s.cfg.Now()
	id := newExportID()
	k.note(id, pendingExport{account: account, createdAt: createdAt, expires: now.Add(keyExportLife)}, now)
	return http.StatusOK, map[string]any{
		"export_id":  id,
		"file_name":  "nottennapod-recovery-key-" + time.UnixMilli(createdAt).UTC().Format("20060102") + ".json",
		"bundle":     string(bundle),
		"created_at": createdAt,
	}, nil
}

// sealKeyBundle seals outside any transaction. A failure or a panic is
// logged with a fixed message only, and answered as a temporary failure.
func (req *request) sealKeyBundle(passphrase []byte) (bundle []byte, createdAt int64, err error) {
	defer func() {
		if recover() != nil {
			bundle, createdAt, err = nil, 0, errors.New("panic")
		}
		if err != nil {
			slog.Error("making a recovery key failed")
			bundle, createdAt, err = nil, 0, errKeySealFailed
		}
	}()
	bundle, createdAt, err = req.s.cfg.Store.SealKeyBundle(req.ctx, passphrase)
	if err == nil && req.s.cfg.duringKeySeal != nil {
		req.s.cfg.duringKeySeal()
	}
	return bundle, createdAt, err
}

// adminKeysSaved is POST /v1/admin/keys/{export_id}/saved {}: the person
// confirms the file is saved, and the export is recorded at the bundle's
// time. An unknown, expired or other account's export is not found (the
// browser asks to save the key again); a repeat answers the same.
func (req *request) adminKeysSaved() (int, any, error) {
	if err := req.decodeEmpty(); err != nil {
		return 0, nil, err
	}
	err := req.s.cfg.Store.Write(req.ctx, func(tx *storage.WriteTx) error {
		_, err := req.revalidate(tx)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	p, ok := req.s.keyExports.find(req.r.PathValue("id"), req.principal.accountID(), req.s.cfg.Now())
	if !ok {
		return 0, nil, errNotFound
	}
	if err := req.s.cfg.Store.RecordKeysExported(p.createdAt); err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"keys_exported_at": req.s.cfg.Store.KeysExportedAt()}, nil
}
