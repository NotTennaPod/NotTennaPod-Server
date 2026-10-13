package storage

import (
	"database/sql"
	"errors"
)

// Recovery frames: a recovery upload's framed fact is assembled in push_parts
// under a negative slot, allocated per framing transaction and found by its
// full transaction ID. A frame remembers the recovery that last used it and
// when.

// RecoveryFrameSlot returns the slot of an installation's framing
// transaction (0 when it has none).
func (a *AccountTx) RecoveryFrameSlot(installID, transactionID string) (int64, error) {
	var slot int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT slot FROM recovery_frames WHERE account_id = ? AND install_id = ? AND transaction_id = ?`,
		a.id, installID, transactionID).Scan(&slot)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return slot, err
}

// UseRecoveryFrame returns a framing transaction's slot, allocating one for a
// new frame, and records the recovery using it and when.
func (a *AccountTx) UseRecoveryFrame(installID, transactionID, recoveryID string, now int64) (int64, error) {
	slot, err := a.RecoveryFrameSlot(installID, transactionID)
	if err != nil {
		return 0, err
	}
	if slot != 0 {
		return slot, a.execOne(`UPDATE recovery_frames SET recovery_id = ?, used_at = ? WHERE account_id = ? AND install_id = ? AND transaction_id = ?`,
			recoveryID, now, a.id, installID, transactionID)
	}
	if err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT MIN(COALESCE((SELECT MIN(slot) FROM recovery_frames WHERE account_id = ?1 AND install_id = ?2), 0),
		COALESCE((SELECT MIN(install_seq) FROM push_parts WHERE account_id = ?1 AND install_id = ?2), 0)) - 1`, a.id, installID).Scan(&slot); err != nil {
		return 0, err
	}
	_, err = a.w.tx.ExecContext(a.w.ctx, `INSERT INTO recovery_frames (account_id, install_id, transaction_id, slot, recovery_id, used_at)
		VALUES (?, ?, ?, ?, ?, ?)`, a.id, installID, transactionID, slot, recoveryID, now)
	return slot, err
}

// RestoreRecoveryFrame records a frame again at its slot with its stored
// parts (a live epoch transition discarded them; protocol's
// recoveryTransition keeps the frames its request completes).
func (a *AccountTx) RestoreRecoveryFrame(installID, transactionID, recoveryID string, slot, now int64, parts []PartRow) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT OR REPLACE INTO recovery_frames (account_id, install_id, transaction_id, slot, recovery_id, used_at)
		VALUES (?, ?, ?, ?, ?, ?)`, a.id, installID, transactionID, slot, recoveryID, now); err != nil {
		return err
	}
	for _, p := range parts {
		if err := a.StorePart(installID, slot, p); err != nil {
			return err
		}
	}
	return nil
}

// RecoveryFrameUsage returns an installation's open recovery frames and the
// plaintext bytes of their stored parts.
func (a *AccountTx) RecoveryFrameUsage(installID string) (int, int64, error) {
	var frames int
	var bytes int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT (SELECT COUNT(*) FROM recovery_frames WHERE account_id = ?1 AND install_id = ?2),
		(SELECT COALESCE(SUM(CASE WHEN size >= 0 THEN size ELSE length(data) END), 0) FROM push_parts
		WHERE account_id = ?1 AND install_id = ?2 AND install_seq < 0)`, a.id, installID).Scan(&frames, &bytes)
	return frames, bytes, err
}

// DropRecoveryFrame discards a frame and its stored parts.
func (a *AccountTx) DropRecoveryFrame(installID, transactionID string) error {
	slot, err := a.RecoveryFrameSlot(installID, transactionID)
	if err != nil || slot == 0 {
		return err
	}
	if err := a.DeleteParts(installID, slot); err != nil {
		return err
	}
	_, err = a.w.tx.ExecContext(a.w.ctx, `DELETE FROM recovery_frames WHERE account_id = ? AND install_id = ? AND transaction_id = ?`,
		a.id, installID, transactionID)
	return err
}

// DropRecoveryFramesOf discards the frames a recovery last used, with their
// stored parts (finishing it leaves no incomplete frame behind).
func (a *AccountTx) DropRecoveryFramesOf(recoveryID string) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM push_parts WHERE account_id = ?1 AND install_seq < 0 AND EXISTS (SELECT 1
		FROM recovery_frames f WHERE f.account_id = ?1 AND f.install_id = push_parts.install_id AND f.slot = push_parts.install_seq
		AND f.recovery_id = ?2)`, a.id, recoveryID); err != nil {
		return err
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM recovery_frames WHERE account_id = ? AND recovery_id = ?`, a.id, recoveryID)
	return err
}

// ExpireRecoveryFrames discards, server-wide, the recovery frames unused
// since before (abandoned uploads), with their stored parts, and stored
// recovery parts without a frame.
func (w *WriteTx) ExpireRecoveryFrames(before int64) (int64, error) {
	res, err := w.tx.ExecContext(w.ctx, `DELETE FROM push_parts WHERE install_seq < 0 AND NOT EXISTS (SELECT 1 FROM recovery_frames f
		WHERE f.account_id = push_parts.account_id AND f.install_id = push_parts.install_id AND f.slot = push_parts.install_seq AND f.used_at >= ?)`,
		before)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	_, err = w.tx.ExecContext(w.ctx, `DELETE FROM recovery_frames WHERE used_at < ?`, before)
	return n, err
}

// OldestRecoveryFrame returns an installation's least recently used recovery
// frame other than those named in except ("" when there is none).
func (a *AccountTx) OldestRecoveryFrame(installID string, except map[string]bool) (string, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT transaction_id FROM recovery_frames WHERE account_id = ? AND install_id = ?
		ORDER BY used_at, slot DESC`, a.id, installID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var tx string
		if err := rows.Scan(&tx); err != nil {
			return "", err
		}
		if !except[tx] {
			return tx, nil
		}
	}
	return "", rows.Err()
}
