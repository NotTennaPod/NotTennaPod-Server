package storage

// Recovery upload jobs (PROTOCOL §6.11; migration 0029): an upload whose
// derivation outlasts its request goes on as a job, resumed after a restart.
// A row is the job's request (sealed) and its authority; it holds no result.

// RecoveryJob is one stored recovery upload job.
type RecoveryJob struct {
	AccountID          string
	RecoveryID         string
	RequestKey         string // hex SHA-256 of the request
	InstallID          string
	State              string // queued or running
	Request            []byte // sealed
	CredentialHash     []byte
	CredentialKind     string
	CredentialsVersion int64 // at submission
	// Transitioned says the job ran its own lost-timeline transition; Fence
	// is the credentials version right after it and FenceEpoch the timeline
	// it made (the account's epoch then): its authority holds only there.
	Transitioned bool
	Fence        int64
	FenceEpoch   int64
	Attempts     int
	CreatedAt    int64
}

const recoveryJobColumns = `account_id, recovery_id, request_key, install_id, state, request, credential_hash, credential_kind,
	credentials_version, transitioned, COALESCE(fence, 0), COALESCE(fence_epoch, 0), attempts, created_at`

// PutRecoveryJob stores a submitted job, queued. The account runs one job at
// a time: any other row of the account is a leftover no job runs for, and is
// replaced.
func (a *AccountTx) PutRecoveryJob(j RecoveryJob) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM recovery_jobs WHERE account_id = ?`, a.id); err != nil {
		return err
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO recovery_jobs (account_id, recovery_id, request_key, install_id, state, request,
		credential_hash, credential_kind, credentials_version, created_at) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)`,
		a.id, j.RecoveryID, j.RequestKey, j.InstallID, j.Request, j.CredentialHash, j.CredentialKind, j.CredentialsVersion, j.CreatedAt)
	return err
}

// StartRecoveryJobAttempt marks the job running its attempts-th attempt. It
// reports false when the job's row is gone.
func (a *AccountTx) StartRecoveryJobAttempt(recoveryID, requestKey string, attempts int) (bool, error) {
	res, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE recovery_jobs SET state = 'running', attempts = ? WHERE account_id = ? AND recovery_id = ?
		AND request_key = ?`, attempts, a.id, recoveryID, requestKey)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// SetRecoveryJobFence records, in the transaction of the job's own
// lost-timeline transition, that it ran, the credentials version right after
// it and the epoch it moved the account to.
func (a *AccountTx) SetRecoveryJobFence(recoveryID, requestKey string, fence, epoch int64) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `UPDATE recovery_jobs SET transitioned = 1, fence = ?, fence_epoch = ? WHERE account_id = ?
		AND recovery_id = ? AND request_key = ?`, fence, epoch, a.id, recoveryID, requestKey)
	return err
}

// DeleteRecoveryJob removes a job: at its publication, in the commit's
// transaction, or when it ends without one.
func (a *AccountTx) DeleteRecoveryJob(recoveryID, requestKey string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM recovery_jobs WHERE account_id = ? AND recovery_id = ? AND request_key = ?`,
		a.id, recoveryID, requestKey)
	return err
}

// RecoveryJobs returns every stored job, oldest first (resumed at start).
func (r *ReadTx) RecoveryJobs() ([]RecoveryJob, error) {
	rows, err := r.tx.QueryContext(r.ctx, `SELECT `+recoveryJobColumns+` FROM recovery_jobs ORDER BY created_at, account_id, recovery_id, request_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RecoveryJob
	for rows.Next() {
		var j RecoveryJob
		var transitioned int
		if err := rows.Scan(&j.AccountID, &j.RecoveryID, &j.RequestKey, &j.InstallID, &j.State, &j.Request, &j.CredentialHash, &j.CredentialKind,
			&j.CredentialsVersion, &transitioned, &j.Fence, &j.FenceEpoch, &j.Attempts, &j.CreatedAt); err != nil {
			return nil, err
		}
		j.Transitioned = transitioned == 1
		out = append(out, j)
	}
	return out, rows.Err()
}
