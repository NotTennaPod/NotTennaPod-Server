package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Login is an account's sign-in record joined with its account state.
type Login struct {
	AccountID          string
	Namespace          string
	Username           string
	PasswordHash       string
	CredentialsVersion int64
	IsAdmin            bool
	State              string
}

// Installation is a registered client installation (PROTOCOL §5).
type Installation struct {
	AccountID          string
	InstallID          string
	InstanceNonce      string
	Name               string
	Platform           string
	AppVersion         string
	Capabilities       string // canonical JSON array
	CreatedAt          int64
	LastSeenAt         int64
	RevokedAt          *int64
	CreatedFrom        string // authorizing installation of a successor, or ""
	SupersededBy       string
	PendingPredecessor string
}

// Credential is a device token or web session, found by its hash, with what the
// authentication middleware needs to decide.
type Credential struct {
	Hash         []byte
	Kind         string // "device" or "web"
	AccountID    string
	InstallID    string // "" for a web session not yet bound
	CreatedAt    int64
	LastUsedAt   int64
	ExpiresAt    *int64
	AccountState string
	IsAdmin      bool
	Installation *Installation // nil if unbound
}

// CredentialRefusal is why a stored credential no longer authorizes a
// request (Credential.Refusal).
type CredentialRefusal int

const (
	CredentialUsable     CredentialRefusal = iota
	CredentialSignedOut                    // expired, or its installation is revoked
	CredentialInactive                     // the account is not active
	CredentialSuperseded                   // its installation was replaced by a successor
)

// Refusal applies the rules every authenticated request is checked by again,
// in their order, to the credential as stored at nowMillis (the API's
// re-validation, and a recovery job's authority at publication). A deleted
// credential (signed out, reset) is not found at all.
func (c Credential) Refusal(nowMillis int64) CredentialRefusal {
	switch {
	case c.ExpiresAt != nil && nowMillis >= *c.ExpiresAt:
		return CredentialSignedOut
	case c.AccountState != "active":
		return CredentialInactive
	case c.Installation != nil && c.Installation.RevokedAt != nil:
		return CredentialSignedOut
	case c.Installation != nil && c.Installation.SupersededBy != "":
		return CredentialSuperseded
	}
	return CredentialUsable
}

// ErrNotFound reports a missing row.
var ErrNotFound = errors.New("storage: not found")

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

const loginColumns = `l.account_id, a.namespace, l.username, l.password_hash, l.credentials_version, l.is_admin, a.state`

func scanLogin(row interface{ Scan(...any) error }) (Login, error) {
	var l Login
	var admin int
	err := row.Scan(&l.AccountID, &l.Namespace, &l.Username, &l.PasswordHash, &l.CredentialsVersion, &admin, &l.State)
	if errors.Is(err, sql.ErrNoRows) {
		return Login{}, ErrNotFound
	}
	l.IsAdmin = admin == 1
	return l, err
}

func loginByUsername(ctx context.Context, q rowQuerier, usernameKey string) (Login, error) {
	return scanLogin(q.QueryRowContext(ctx, `SELECT `+loginColumns+` FROM account_logins l JOIN accounts a USING (account_id) WHERE l.username_key = ?`, usernameKey))
}

func loginByAccount(ctx context.Context, q rowQuerier, accountID string) (Login, error) {
	return scanLogin(q.QueryRowContext(ctx, `SELECT `+loginColumns+` FROM account_logins l JOIN accounts a USING (account_id) WHERE l.account_id = ?`, accountID))
}

const installationColumns = `account_id, install_id, instance_nonce, name, platform, app_version, capabilities, created_at, last_seen_at,
	revoked_at, COALESCE(created_from, ''), COALESCE(superseded_by, ''), COALESCE(pending_predecessor, '')`

func scanInstallation(row interface{ Scan(...any) error }) (Installation, error) {
	var i Installation
	var revoked sql.NullInt64
	err := row.Scan(&i.AccountID, &i.InstallID, &i.InstanceNonce, &i.Name, &i.Platform, &i.AppVersion, &i.Capabilities,
		&i.CreatedAt, &i.LastSeenAt, &revoked, &i.CreatedFrom, &i.SupersededBy, &i.PendingPredecessor)
	if errors.Is(err, sql.ErrNoRows) {
		return Installation{}, ErrNotFound
	}
	if revoked.Valid {
		i.RevokedAt = &revoked.Int64
	}
	return i, err
}

const credentialSelect = `SELECT t.token_hash, t.kind, t.account_id, t.install_id, t.created_at, t.last_used_at, t.expires_at,
		a.state, l.is_admin
	FROM auth_tokens t JOIN accounts a USING (account_id) LEFT JOIN account_logins l USING (account_id)`

func scanCredential(row interface{ Scan(...any) error }) (Credential, error) {
	var c Credential
	var install sql.NullString
	var expires sql.NullInt64
	var admin sql.NullInt64
	err := row.Scan(&c.Hash, &c.Kind, &c.AccountID, &install, &c.CreatedAt, &c.LastUsedAt, &expires, &c.AccountState, &admin)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, err
	}
	c.IsAdmin = admin.Valid && admin.Int64 == 1
	if expires.Valid {
		c.ExpiresAt = &expires.Int64
	}
	if install.Valid {
		c.InstallID = install.String
	}
	return c, nil
}

func credentialByHash(ctx context.Context, q rowQuerier, hash []byte) (Credential, error) {
	c, err := scanCredential(q.QueryRowContext(ctx, credentialSelect+` WHERE t.token_hash = ?`, hash))
	if err != nil {
		return Credential{}, err
	}
	if c.InstallID != "" {
		inst, err := scanInstallation(q.QueryRowContext(ctx, `SELECT `+installationColumns+` FROM installations WHERE account_id = ? AND install_id = ?`,
			c.AccountID, c.InstallID))
		if err != nil {
			return Credential{}, err
		}
		c.Installation = &inst
	}
	return c, nil
}

// credentialsByHashes resolves several hashes in one batch: one query for the
// credentials and one for their installations. Unknown hashes are absent
// from the result; duplicates are looked up once.
func credentialsByHashes(ctx context.Context, q rowQuerier, hashes [][]byte) (map[string]Credential, error) {
	out := map[string]Credential{}
	if len(hashes) == 0 {
		return out, nil
	}
	args := make([]any, len(hashes))
	for i, h := range hashes {
		args[i] = h
	}
	rows, err := q.QueryContext(ctx, credentialSelect+` WHERE t.token_hash IN (?`+strings.Repeat(", ?", len(hashes)-1)+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type key struct{ account, install string }
	bound := map[key][]string{}
	for rows.Next() {
		c, err := scanCredential(rows)
		if err != nil {
			return nil, err
		}
		out[string(c.Hash)] = c
		if c.InstallID != "" {
			k := key{c.AccountID, c.InstallID}
			bound[k] = append(bound[k], string(c.Hash))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(bound) == 0 {
		return out, nil
	}
	args = args[:0]
	for k := range bound {
		args = append(args, k.account, k.install)
	}
	irows, err := q.QueryContext(ctx, `SELECT `+installationColumns+` FROM installations WHERE (account_id, install_id) IN (VALUES (?, ?)`+
		strings.Repeat(", (?, ?)", len(bound)-1)+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer irows.Close()
	for irows.Next() {
		inst, err := scanInstallation(irows)
		if err != nil {
			return nil, err
		}
		for _, h := range bound[key{inst.AccountID, inst.InstallID}] {
			c := out[h]
			i := inst
			c.Installation = &i
			out[h] = c
		}
	}
	if err := irows.Err(); err != nil {
		return nil, err
	}
	for _, c := range out {
		if c.InstallID != "" && c.Installation == nil {
			return nil, fmt.Errorf("storage: credential bound to a missing installation")
		}
	}
	return out, nil
}

// createdFromAncestors follows created_from from the installation (exclusive)
// back to its first installation: every installation it descends from.
func createdFromAncestors(ctx context.Context, q rowQuerier, accountID, installID string) ([]string, error) {
	var out []string
	seen := map[string]bool{installID: true}
	for current := installID; ; {
		var from sql.NullString
		err := q.QueryRowContext(ctx, `SELECT created_from FROM installations WHERE account_id = ? AND install_id = ?`, accountID, current).Scan(&from)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && (!from.Valid || from.String == "" || seen[from.String])) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		seen[from.String] = true
		out = append(out, from.String)
		current = from.String
	}
}

func hasAdmin(ctx context.Context, q rowQuerier) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_logins WHERE is_admin = 1`).Scan(&n)
	return n > 0, err
}

// ---- reads

// LoginByUsername finds a sign-in record by its lowercase username key.
func (r *ReadTx) LoginByUsername(usernameKey string) (Login, error) {
	return loginByUsername(r.ctx, r.tx, usernameKey)
}

// LoginByAccount returns an account's sign-in record.
func (r *ReadTx) LoginByAccount(accountID string) (Login, error) {
	return loginByAccount(r.ctx, r.tx, accountID)
}

// CredentialByHash resolves a token or session hash.
func (r *ReadTx) CredentialByHash(hash []byte) (Credential, error) {
	return credentialByHash(r.ctx, r.tx, hash)
}

// CredentialsByHashes resolves several token or session hashes, with their
// installations, in one batch, keyed by string(hash). Unknown hashes are
// absent from the result.
func (r *ReadTx) CredentialsByHashes(hashes [][]byte) (map[string]Credential, error) {
	return credentialsByHashes(r.ctx, r.tx, hashes)
}

// CreatedFromAncestors lists the installations the given one descends from,
// following created_from (a successor's or a copy's authorizing
// installation), nearest first.
func (a *AccountReader) CreatedFromAncestors(installID string) ([]string, error) {
	return createdFromAncestors(a.r.ctx, a.r.tx, a.id, installID)
}

// HasAdmin reports whether setup has completed.
func (r *ReadTx) HasAdmin() (bool, error) { return hasAdmin(r.ctx, r.tx) }

// AccountSummary is one row of the administrator's account list.
type AccountSummary struct {
	AccountID string
	Username  string
	IsAdmin   bool
	State     string
	CreatedAt int64
}

// ListAccounts lists accounts with sign-in records, by username key.
func (r *ReadTx) ListAccounts() ([]AccountSummary, error) {
	rows, err := r.tx.QueryContext(r.ctx, `SELECT a.account_id, l.username, l.is_admin, a.state, a.created_at
		FROM accounts a JOIN account_logins l USING (account_id) ORDER BY l.username_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountSummary
	for rows.Next() {
		var s AccountSummary
		var admin int
		if err := rows.Scan(&s.AccountID, &s.Username, &admin, &s.State, &s.CreatedAt); err != nil {
			return nil, err
		}
		s.IsAdmin = admin == 1
		out = append(out, s)
	}
	return out, rows.Err()
}

// Installations lists the account's installations by creation time.
func (a *AccountReader) Installations() ([]Installation, error) {
	return listInstallations(a.r.ctx, a.r.tx, a.id)
}

func listInstallations(ctx context.Context, q rowQuerier, accountID string) ([]Installation, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+installationColumns+` FROM installations WHERE account_id = ? ORDER BY created_at, install_id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Installation
	for rows.Next() {
		i, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// ---- writes: server level

// LoginByUsername, CredentialByHash and HasAdmin re-read inside the write
// transaction (credential re-validation, setup).
func (w *WriteTx) LoginByUsername(usernameKey string) (Login, error) {
	return loginByUsername(w.ctx, w.tx, usernameKey)
}

// CredentialByHash resolves a credential inside the write transaction.
func (w *WriteTx) CredentialByHash(hash []byte) (Credential, error) {
	return credentialByHash(w.ctx, w.tx, hash)
}

// HasAdmin reports whether any administrator exists.
func (w *WriteTx) HasAdmin() (bool, error) { return hasAdmin(w.ctx, w.tx) }

// CountActiveAdmins counts administrators whose accounts are active.
func (w *WriteTx) CountActiveAdmins() (int, error) {
	var n int
	err := w.tx.QueryRowContext(w.ctx, `SELECT COUNT(*) FROM account_logins l JOIN accounts a USING (account_id)
		WHERE l.is_admin = 1 AND a.state = 'active'`).Scan(&n)
	return n, err
}

// CreateLogin gives an account its sign-in record (credentials_version 1).
func (w *WriteTx) CreateLogin(accountID, username, usernameKey, passwordHash string, isAdmin bool) error {
	_, err := w.tx.ExecContext(w.ctx, `INSERT INTO account_logins (account_id, username, username_key, password_hash, credentials_version, is_admin, credentials_changed_at)
		VALUES (?, ?, ?, ?, 1, ?, ?)`, accountID, username, usernameKey, passwordHash, boolInt(isAdmin), w.nowMillis())
	return err
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---- writes: account scoped

// Login returns the account's sign-in record.
func (a *AccountTx) Login() (Login, error) { return loginByAccount(a.w.ctx, a.w.tx, a.id) }

// SetPassword stores a new password hash, increases credentials_version and
// deletes every credential of the account except keep (which may be nil). A
// password change or reset removes access: it raises the auth revision (PROTOCOL §11).
func (a *AccountTx) SetPassword(passwordHash string, keep []byte) error {
	ctx, tx := a.w.ctx, a.w.tx
	res, err := tx.ExecContext(ctx, `UPDATE account_logins SET password_hash = ?, credentials_version = credentials_version + 1, credentials_changed_at = ?
		WHERE account_id = ?`, passwordHash, a.w.nowMillis(), a.id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNoAccount
	}
	a.w.markAuthChange()
	_, err = tx.ExecContext(ctx, `DELETE FROM auth_tokens WHERE account_id = ? AND token_hash IS NOT ?`, a.id, keep)
	return err
}

// RehashPassword replaces the stored hash with an equivalent one (stronger
// parameters) only if credentials_version is still version; it does not revoke.
func (a *AccountTx) RehashPassword(passwordHash string, version int64) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE account_logins SET password_hash = ? WHERE account_id = ? AND credentials_version = ?`,
		passwordHash, a.id, version)
	return err
}

// SetAdmin grants or revokes administrator rights; revoking them raises the
// auth revision (PROTOCOL §11).
func (a *AccountTx) SetAdmin(admin bool) error {
	if !admin {
		a.w.markAuthChange()
	}
	return a.execOne(`UPDATE account_logins SET is_admin = ? WHERE account_id = ?`, boolInt(admin), a.id)
}

// Rename changes the username (and its lowercase key).
func (a *AccountTx) Rename(username, usernameKey string) error {
	return a.execOne(`UPDATE account_logins SET username = ?, username_key = ? WHERE account_id = ?`, username, usernameKey, a.id)
}

// SetState enables or disables the account; credentials are kept. Disabling
// raises the auth revision (PROTOCOL §11).
func (a *AccountTx) SetState(state string) error {
	if state != "active" {
		a.w.markAuthChange()
	}
	return a.execOne(`UPDATE accounts SET state = ? WHERE account_id = ?`, state, a.id)
}

func (a *AccountTx) execOne(query string, args ...any) error {
	res, err := a.w.tx.ExecContext(a.w.ctx, query, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNoAccount
	}
	return nil
}

// Installation returns one of the account's installations.
func (a *AccountTx) Installation(installID string) (Installation, error) {
	return scanInstallation(a.w.tx.QueryRowContext(a.w.ctx, `SELECT `+installationColumns+` FROM installations WHERE account_id = ? AND install_id = ?`,
		a.id, installID))
}

// Installations lists the account's installations.
func (a *AccountTx) Installations() ([]Installation, error) {
	return listInstallations(a.w.ctx, a.w.tx, a.id)
}

// InsertInstallation registers a new installation in this account.
func (a *AccountTx) InsertInstallation(i Installation) error {
	now := a.w.nowMillis()
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO installations (account_id, install_id, instance_nonce, name, platform, app_version, capabilities,
			created_at, last_seen_at, created_from) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''))`,
		a.id, i.InstallID, i.InstanceNonce, i.Name, i.Platform, i.AppVersion, i.Capabilities, now, now, i.CreatedFrom)
	return err
}

// UpdateInstallationDevice updates the reported name, version and capabilities.
func (a *AccountTx) UpdateInstallationDevice(installID, name, appVersion, capabilities string) error {
	return a.execOne(`UPDATE installations SET name = ?, app_version = ?, capabilities = ? WHERE account_id = ? AND install_id = ?`,
		name, appVersion, capabilities, a.id, installID)
}

// ResumeInstallation clears a revocation (signing in again).
func (a *AccountTx) ResumeInstallation(installID string) error {
	return a.execOne(`UPDATE installations SET revoked_at = NULL WHERE account_id = ? AND install_id = ?`, a.id, installID)
}

// RevokeInstallation deletes the installation's credentials and records the
// revocation; the row stays (PROTOCOL §5). It raises the auth revision (PROTOCOL §11).
func (a *AccountTx) RevokeInstallation(installID string) error {
	a.w.markAuthChange()
	if err := a.execOne(`UPDATE installations SET revoked_at = COALESCE(revoked_at, ?) WHERE account_id = ? AND install_id = ?`,
		a.w.nowMillis(), a.id, installID); err != nil {
		return err
	}
	return a.DeleteInstallationCredentials(installID)
}

// Supersede marks old as replaced by successor on the same device; the
// successor remembers that old's credentials await revocation.
func (a *AccountTx) Supersede(old, successor string) error {
	if err := a.execOne(`UPDATE installations SET superseded_by = ? WHERE account_id = ? AND install_id = ?`, successor, a.id, old); err != nil {
		return err
	}
	return a.execOne(`UPDATE installations SET pending_predecessor = ? WHERE account_id = ? AND install_id = ?`, old, a.id, successor)
}

// FinishSupersession revokes the predecessor's credentials once the successor's
// credential has been used.
// A chain (a successor that itself got a successor before being used) is
// finished all the way back.
func (a *AccountTx) FinishSupersession(successor string) error {
	for current := successor; current != ""; {
		inst, err := a.Installation(current)
		if err != nil || inst.PendingPredecessor == "" {
			return err
		}
		// Revoking a superseded installation's credentials raises the auth
		// revision (PROTOCOL §11).
		a.w.markAuthChange()
		if err := a.DeleteInstallationCredentials(inst.PendingPredecessor); err != nil {
			return err
		}
		if err := a.execOne(`UPDATE installations SET pending_predecessor = NULL WHERE account_id = ? AND install_id = ?`, a.id, current); err != nil {
			return err
		}
		current = inst.PendingPredecessor
	}
	return nil
}

// TouchInstallation records activity.
func (a *AccountTx) TouchInstallation(installID string, at int64) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE installations SET last_seen_at = ? WHERE account_id = ? AND install_id = ?`, at, a.id, installID)
	return err
}

// InsertCredential stores a credential hash bound to installID ("" = unbound web session).
func (a *AccountTx) InsertCredential(hash []byte, kind, installID string, expiresAt *int64) error {
	now := a.w.nowMillis()
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO auth_tokens (token_hash, kind, account_id, install_id, created_at, last_used_at, expires_at)
		VALUES (?, ?, ?, NULLIF(?, ''), ?, ?, ?)`, hash, kind, a.id, installID, now, now, expiresAt)
	return err
}

// BindCredential binds an unbound web session to an installation.
func (a *AccountTx) BindCredential(hash []byte, installID string) error {
	return a.execOne(`UPDATE auth_tokens SET install_id = ? WHERE token_hash = ? AND account_id = ? AND install_id IS NULL`, installID, hash, a.id)
}

// TouchCredential records use at `at` and, given extendTo, extends the
// session's expiry to at least extendTo, never shortening it (a clock that
// stepped back). It changes nothing and reports false when the credential is
// already expired at `at`, the transaction's own time: an expired session is
// never extended.
func (a *AccountTx) TouchCredential(hash []byte, at int64, extendTo *int64) (bool, error) {
	res, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE auth_tokens SET last_used_at = ?,
			expires_at = CASE WHEN ? IS NULL OR expires_at IS NULL THEN expires_at ELSE max(expires_at, ?) END
		WHERE token_hash = ? AND account_id = ? AND (expires_at IS NULL OR expires_at > ?)`,
		at, extendTo, extendTo, hash, a.id, at)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// DeleteIfExpired deletes the session if it is expired at now, the
// transaction's own time, and reports whether it did: deciding expiry in the
// write transaction orders it against a renewal (TouchCredential). An expired
// session no longer gives access, so this is not an access-removing change.
func (w *WriteTx) DeleteIfExpired(hash []byte, now int64) (bool, error) {
	res, err := w.tx.ExecContext(w.ctx, `DELETE FROM auth_tokens WHERE token_hash = ? AND expires_at IS NOT NULL AND expires_at <= ?`, hash, now)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// DeleteCredential deletes one credential of this account (signing out), which
// raises the auth revision (PROTOCOL §11).
func (a *AccountTx) DeleteCredential(hash []byte) error {
	res, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM auth_tokens WHERE token_hash = ? AND account_id = ?`, hash, a.id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		a.w.markAuthChange()
	}
	return nil
}

// DeleteInstallationCredentials deletes every credential bound to the installation.
func (a *AccountTx) DeleteInstallationCredentials(installID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM auth_tokens WHERE account_id = ? AND install_id = ?`, a.id, installID)
	return err
}

// DeleteInstallationCredentialsOfKind deletes the installation's credentials of one kind.
func (a *AccountTx) DeleteInstallationCredentialsOfKind(installID, kind string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM auth_tokens WHERE account_id = ? AND install_id = ? AND kind = ?`, a.id, installID, kind)
	return err
}

// DeleteExpiredWebSessions deletes the installation's web sessions that are
// expired at now, the transaction's own time: an expired session never comes
// back (DeleteIfExpired), even one whose cookie is no longer presented. Device
// tokens never expire and are never touched.
func (a *AccountTx) DeleteExpiredWebSessions(installID string, now int64) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM auth_tokens WHERE account_id = ? AND install_id = ? AND kind = 'web'
		AND expires_at IS NOT NULL AND expires_at <= ?`, a.id, installID, now)
	return err
}

// HasCredentialsOfKind reports whether the installation holds a credential of the kind.
func (a *AccountTx) HasCredentialsOfKind(installID, kind string) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT COUNT(*) FROM auth_tokens WHERE account_id = ? AND install_id = ? AND kind = ?`,
		a.id, installID, kind).Scan(&n)
	return n > 0, err
}

// DeleteWebSessionsOnSupersessionPath ends a browser's handoffs at its sign-out
// (PROTOCOL §5.4): from the installation, it follows superseded_by while the
// next installation has the same instance nonce, and deletes the web sessions
// of every installation after the first, used or not. pending_predecessor is
// not consulted, device tokens are never deleted, and a copy (another nonce,
// linked only by created_from) is never reached.
func (a *AccountTx) DeleteWebSessionsOnSupersessionPath(installID string) error {
	from, err := a.Installation(installID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	seen := map[string]bool{from.InstallID: true}
	for from.SupersededBy != "" && !seen[from.SupersededBy] {
		next, err := a.Installation(from.SupersededBy)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if next.InstanceNonce != from.InstanceNonce {
			return nil
		}
		res, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM auth_tokens WHERE account_id = ? AND install_id = ? AND kind = 'web'`, a.id, next.InstallID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			a.w.markAuthChange()
		}
		seen[next.InstallID] = true
		from = next
	}
	return nil
}

// RearmPendingPredecessor makes predecessor's credentials await revocation
// again at successor's next use (FinishSupersession), as at a handoff: a
// session bound to a superseded installation after its successor's first use
// is then retired like any other (PROTOCOL §5.4). The links stay as they are.
func (a *AccountTx) RearmPendingPredecessor(successor, predecessor string) error {
	return a.execOne(`UPDATE installations SET pending_predecessor = ? WHERE account_id = ? AND install_id = ?`, predecessor, a.id, successor)
}

// DeleteExpiredSessions removes web sessions past their expiry, server-wide.
// They no longer give access, so this is not an access-removing change.
func (w *WriteTx) DeleteExpiredSessions(now int64) error {
	_, err := w.tx.ExecContext(w.ctx, `DELETE FROM auth_tokens WHERE expires_at IS NOT NULL AND expires_at < ?`, now)
	return err
}
