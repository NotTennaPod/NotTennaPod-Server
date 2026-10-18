package protocol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Recovery upload jobs (PROTOCOL §6.11). An upload's derivation may outlast
// its request: the server goes on with it as a job, in the server's lifetime
// rather than the request's, and answers server_busy with retry_after, to the
// uploader too. A job deduplicates in-flight work only: an identical request
// meanwhile waits for it, any other upload of the account is busy. Nothing is
// kept against the request: the job's publication deletes its row in the
// commit's transaction, and a repeat afterwards is evaluated afresh (entries
// joined since answer known; dependency_missing and part_stored are
// evaluated again). A job publishes only with the authority the request
// would have had at publication: its own credential, re-checked there, or,
// after the lost-timeline transition the job itself ran, its installation and
// the credentials version right after that transition and the timeline it
// made, stored with it: a later transition voids that exception.

// RecoveryUploadWait is how long an upload request waits for its job before
// it answers server_busy: under every client's 30 s idle timeout.
const RecoveryUploadWait = 20 * time.Second

// recoveryJobAttempts bounds a job's attempts after transient failures; then
// the job is removed and a later identical request starts a new one.
const recoveryJobAttempts = 5

// StoredCredential is the credential a recovery job acts with: its stored
// hash (never the secret) and kind.
type StoredCredential struct {
	Hash []byte
	Kind string
}

// AuthorizeStoredCredential authorizes as the stored credential is now: the
// API's re-validation rules for a sync request (storage.Credential.Refusal),
// applied to the credential a recovery job was submitted with. A credential
// signed out, reset or expired since is unauthorized; a revoked installation
// too; a disabled account forbidden; a superseded installation superseded.
func AuthorizeStoredCredential(now func() time.Time, cred StoredCredential) Authorize {
	return func(tx *storage.WriteTx) (*storage.Installation, error) {
		c, err := tx.CredentialByHash(cred.Hash)
		if errors.Is(err, storage.ErrNotFound) {
			return nil, errSignIn
		} else if err != nil {
			return nil, err
		}
		if c.Kind != cred.Kind {
			return nil, errSignIn
		}
		switch c.Refusal(now().UnixMilli()) {
		case storage.CredentialSignedOut:
			return nil, errSignIn
		case storage.CredentialInactive:
			return nil, errNotAllowed
		case storage.CredentialSuperseded:
			return nil, protoErr(409, "installation_superseded", "This installation was replaced; use its successor.")
		}
		if c.Installation == nil {
			return nil, errNotAllowed
		}
		return c.Installation, nil
	}
}

var (
	errSignIn     = protoErr(401, "unauthorized", "Sign in again.")
	errNotAllowed = protoErr(403, "forbidden", "This account is disabled or not allowed to do this.")
)

// recoveryJob is one running job, in memory.
type recoveryJob struct {
	accountID, recoveryID, key string
	req                        RecoveryUploadRequest // as stored: decoded from the sealed request
	cred                       StoredCredential
	attempts                   int
	fence                      *timelineFence // set once the job ran its own transition
	started                    time.Time
	done                       chan struct{}
	resp                       *RecoveryUploadResponse
	err                        error
}

// busy is the answer to a request that stops waiting for the job.
func (j *recoveryJob) busy() error {
	wait := min(max(time.Since(j.started)/6, 10*time.Second), 30*time.Second)
	return &Error{Status: 503, Code: "server_busy", Message: "This server is still joining this recovery upload; send it again shortly.",
		RetryAfter: wait}
}

// recoveryJobs are a store's running jobs, at most one per account.
type recoveryJobs struct {
	mu      sync.Mutex
	ctx     context.Context // the server's lifetime
	wg      sync.WaitGroup
	running map[string]*recoveryJob // by account

	// Test seams (SetRecoveryJobHooksForTest).
	hook          func(point string) error
	wait, backoff time.Duration
}

// seams returns the store's test seams.
func (r *recoveryJobs) seams() (func(string) error, time.Duration, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hook, r.wait, r.backoff
}

var jobsByStore sync.Map // *storage.Store → *recoveryJobs

func (s *Service) recoveryJobs() *recoveryJobs {
	v, _ := jobsByStore.LoadOrStore(s.Store, &recoveryJobs{ctx: context.Background(), running: map[string]*recoveryJob{}})
	return v.(*recoveryJobs)
}

func (r *recoveryJobs) forget(j *recoveryJob) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running[j.accountID] == j {
		delete(r.running, j.accountID)
	}
}

func (j *recoveryJob) finish(resp *RecoveryUploadResponse, err error) {
	j.resp, j.err = resp, err
	close(j.done)
}

// SubmitRecoveryUpload joins one upload request (PROTOCOL §6.11). One that
// joins nothing is answered at once (recoveryUploadCheap); one that joins
// something runs as a job: authorized by the request's credential, stored and
// run in the server's
// lifetime with the authority of cred. The request waits for it up to
// RecoveryUploadWait and then answers server_busy with retry_after; an
// identical request meanwhile waits for the same job and receives its results
// once its own credential passes again; any other upload of the account is
// busy until the job ends.
func (s *Service) SubmitRecoveryUpload(ctx context.Context, req RecoveryUploadRequest, authorize Authorize,
	cred StoredCredential) (*RecoveryUploadResponse, error) {
	data, key, err := encodeRecoveryJob(req)
	if err != nil {
		return nil, err
	}
	stored, err := decodeRecoveryJob(data, req.AccountID)
	if err != nil {
		return nil, err
	}
	reg := s.recoveryJobs()
	// running answers a request while a job of the account runs: an
	// identical one waits for it, any other is busy.
	running := func(j *recoveryJob) (*RecoveryUploadResponse, error) {
		if j.recoveryID != req.RecoveryID || j.key != key {
			return nil, j.busy()
		}
		return s.awaitRecoveryJob(ctx, j, authorize, false)
	}
	reg.mu.Lock()
	if j := reg.running[req.AccountID]; j != nil {
		reg.mu.Unlock()
		return running(j)
	}
	reg.mu.Unlock()
	// An upload that joins nothing (a repeat, a frame's part) is answered at
	// once from the archive as it is now: it never waits for a derivation of
	// the account, so a repeat after a job ended is never lost to another.
	if resp, err := s.recoveryUploadCheap(ctx, stored, authorize); !errors.Is(err, errWouldJoin) {
		return resp, err
	}
	reg.mu.Lock()
	if j := reg.running[req.AccountID]; j != nil {
		reg.mu.Unlock()
		return running(j)
	}
	if err := s.busy(req.AccountID); err != nil {
		reg.mu.Unlock()
		return nil, err
	}
	j := &recoveryJob{accountID: req.AccountID, recoveryID: req.RecoveryID, key: key, req: stored, cred: cred, attempts: 0,
		started: time.Now(), done: make(chan struct{})}
	reg.running[req.AccountID] = j
	reg.wg.Add(1)
	reg.mu.Unlock()
	err = s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		if _, err := s.uploadable(tx, req, authorize); err != nil {
			return err
		}
		acct := tx.Account(req.AccountID)
		version, err := acct.CredentialsVersion()
		if err != nil {
			return err
		}
		return acct.PutRecoveryJob(storage.RecoveryJob{RecoveryID: req.RecoveryID, RequestKey: key, InstallID: req.InstallID,
			Request: s.sealRecoveryJob(req.AccountID, req.RecoveryID, key, data), CredentialHash: cred.Hash, CredentialKind: cred.Kind,
			CredentialsVersion: version, CreatedAt: s.Now().UnixMilli()})
	})
	if err != nil {
		reg.forget(j)
		j.finish(nil, err)
		reg.wg.Done()
		return nil, err
	}
	go s.runRecoveryJob(reg, j)
	return s.awaitRecoveryJob(ctx, j, authorize, true)
}

// awaitRecoveryJob waits for a job. The submitting request receives what the
// job published, as the synchronous path's request would. An identical
// request that joined it receives the results only if its own credential
// passes again now; a job that ended refused for its own credential is,
// for it, busy: its repeat runs with its own.
func (s *Service) awaitRecoveryJob(ctx context.Context, j *recoveryJob, authorize Authorize, submitter bool) (*RecoveryUploadResponse, error) {
	timer := time.NewTimer(s.recoveryWait())
	defer timer.Stop()
	select {
	case <-j.done:
	case <-timer.C:
		return nil, j.busy()
	case <-ctx.Done():
		return nil, j.busy()
	}
	if submitter {
		return j.resp, j.err
	}
	if err := s.Store.Write(ctx, func(tx *storage.WriteTx) error { return authorized(tx, authorize, j.req.InstallID) }); err != nil {
		return nil, err
	}
	var pe *Error
	if errors.As(j.err, &pe) && (pe.Status == 401 || pe.Status == 403 || pe.Code == "installation_superseded") {
		return nil, j.busy()
	}
	return j.resp, j.err
}

func (s *Service) recoveryWait() time.Duration {
	if _, wait, _ := s.recoveryJobs().seams(); wait > 0 {
		return wait
	}
	return RecoveryUploadWait
}

// runRecoveryJob runs a job to its end: published, refused, out of attempts,
// or stopped with the server (its row stays; the next start resumes it).
func (s *Service) runRecoveryJob(reg *recoveryJobs, j *recoveryJob) {
	defer reg.wg.Done()
	resp, err := s.recoveryJobAttempts(reg.ctx, j)
	reg.forget(j)
	j.finish(resp, err)
}

func (s *Service) recoveryJobAttempts(ctx context.Context, j *recoveryJob) (*RecoveryUploadResponse, error) {
	for {
		if ctx.Err() != nil {
			return nil, j.busy()
		}
		if j.attempts >= recoveryJobAttempts {
			slog.Warn("recovery: an upload job failed every attempt; the device's next upload starts it again", "account", j.accountID,
				"attempts", j.attempts)
			s.dropRecoveryJob(ctx, j)
			return nil, j.busy()
		}
		j.attempts++
		resp, err := s.recoveryJobAttempt(ctx, j)
		var pe *Error
		switch {
		case err == nil:
			return resp, nil // the publication deleted the row
		case jobStopped(ctx, err):
			return nil, j.busy()
		case errors.Is(err, errRebuildClaimed):
			// Another upload's live transaction, or a staged rebuild, holds
			// the account for now: tried again, as a transient failure is.
		case errors.As(err, &pe):
			// The request's own answer (refused, invalid, busy): what the
			// synchronous path answers. The job ends without a publication.
			s.dropRecoveryJob(ctx, j)
			return nil, err
		}
		slog.Warn("recovery: an upload job's attempt failed; trying again", "account", j.accountID, "attempt", j.attempts, "error", err)
		backoff := time.NewTimer(s.recoveryJobBackoff() << (j.attempts - 1))
		select {
		case <-ctx.Done():
			backoff.Stop()
			return nil, j.busy()
		case <-backoff.C:
		}
	}
}

func (s *Service) recoveryJobAttempt(ctx context.Context, j *recoveryJob) (*RecoveryUploadResponse, error) {
	if err := s.jobPoint("attempt"); err != nil {
		return nil, err
	}
	var present bool
	if err := s.Store.Write(ctx, func(tx *storage.WriteTx) error {
		var err error
		present, err = tx.Account(j.accountID).StartRecoveryJobAttempt(j.recoveryID, j.key, j.attempts)
		return err
	}); err != nil {
		return nil, err
	}
	if !present {
		return nil, errBusy // removed meanwhile (the installation or account deleted)
	}
	authorize := AuthorizeStoredCredential(s.Now, j.cred)
	return s.recoveryUpload(ctx, j.req, authorize, &uploadHooks{s: s, job: j, fence: j.fence})
}

func (s *Service) recoveryJobBackoff() time.Duration {
	if _, _, backoff := s.recoveryJobs().seams(); backoff > 0 {
		return backoff
	}
	return time.Second
}

// jobStopped reports an attempt that ended with the server (or a test's
// crash): the job's row stays as it was, for the next start.
func jobStopped(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, storage.ErrClosed) || errors.Is(err, ErrCrashForTest)
}

func (s *Service) dropRecoveryJob(ctx context.Context, j *recoveryJob) {
	err := s.Store.Write(ctx, func(tx *storage.WriteTx) error { return tx.Account(j.accountID).DeleteRecoveryJob(j.recoveryID, j.key) })
	if err != nil && !jobStopped(ctx, err) {
		slog.Warn("recovery: cannot remove an ended upload job", "account", j.accountID, "error", err)
	}
}

// uploadHooks are a job's part in an upload: its fence, stored in the
// transaction of the transition the job runs, and its row, deleted in the
// transaction that publishes. Nil for a request run synchronously.
type uploadHooks struct {
	s     *Service
	job   *recoveryJob
	fence *timelineFence // the job ran its own transition before this attempt
}

func (h *uploadHooks) publish(tx *storage.WriteTx) error {
	if h == nil {
		return nil
	}
	if err := h.s.jobPoint("publish"); err != nil {
		return err
	}
	return tx.Account(h.job.accountID).DeleteRecoveryJob(h.job.recoveryID, h.job.key)
}

func (h *uploadHooks) transitioned(acct *storage.AccountTx, fence timelineFence) error {
	if h == nil {
		return nil
	}
	if err := acct.SetRecoveryJobFence(h.job.recoveryID, h.job.key, fence.version, fence.epoch); err != nil {
		return err
	}
	return h.s.jobPoint("transition")
}

func (h *uploadHooks) afterTransition(fence timelineFence) error {
	if h == nil {
		return nil
	}
	h.job.fence = &fence
	return h.s.jobPoint("transitioned")
}

// StartRecoveryJobs runs recovery jobs in ctx, the server's lifetime, and
// resumes the stored ones (queued or running when the server last stopped).
// A row whose request cannot be opened is removed: its device sends the
// upload again. The returned function waits for every job to stop; cancel
// ctx first.
func (s *Service) StartRecoveryJobs(ctx context.Context) (func(), error) {
	reg := s.recoveryJobs()
	reg.mu.Lock()
	reg.ctx = ctx
	reg.mu.Unlock()
	var rows []storage.RecoveryJob
	if err := s.Store.Read(ctx, func(r *storage.ReadTx) error {
		var err error
		rows, err = r.RecoveryJobs()
		return err
	}); err != nil {
		return nil, err
	}
	for _, row := range rows {
		j, err := s.resumedRecoveryJob(row)
		reg.mu.Lock()
		taken := reg.running[row.AccountID] != nil
		if err == nil && !taken {
			reg.running[row.AccountID] = j
			reg.wg.Add(1)
		}
		reg.mu.Unlock()
		switch {
		case err != nil:
			slog.Warn("recovery: a stored upload job cannot be resumed; removing it", "account", row.AccountID, "error", err)
			s.dropRecoveryJob(ctx, &recoveryJob{accountID: row.AccountID, recoveryID: row.RecoveryID, key: row.RequestKey})
		case taken:
			continue // a newer submission replaced it
		default:
			slog.Info("recovery: resuming an upload job", "account", row.AccountID, "attempts", row.Attempts)
			go s.runRecoveryJob(reg, j)
		}
	}
	return reg.wg.Wait, nil
}

func (s *Service) resumedRecoveryJob(row storage.RecoveryJob) (*recoveryJob, error) {
	data, err := s.openRecoveryJob(row.AccountID, row.RecoveryID, row.RequestKey, row.Request)
	if err != nil {
		return nil, err
	}
	req, err := decodeRecoveryJob(data, row.AccountID)
	if err != nil {
		return nil, err
	}
	j := &recoveryJob{accountID: row.AccountID, recoveryID: row.RecoveryID, key: row.RequestKey, req: req,
		cred: StoredCredential{Hash: row.CredentialHash, Kind: row.CredentialKind}, attempts: row.Attempts, started: time.Now(),
		done: make(chan struct{})}
	if row.Transitioned {
		j.fence = &timelineFence{version: row.Fence, epoch: row.FenceEpoch}
	}
	return j, nil
}

// ---- the stored request

// jobRequest is a job's request at rest: the decoded upload, its facts and
// receipts in their canonical bytes. Its SHA-256 is the request key.
type jobRequest struct {
	RecoveryID string           `json:"recovery_id"`
	InstallID  string           `json:"install_id"`
	Entries    []jobEntry       `json:"entries"`
	Secrets    []Secret         `json:"secrets,omitempty"`
	Steps      RecoveryEndSteps `json:"steps"`
}

type jobEntry struct {
	Fact     json.RawMessage `json:"fact,omitempty"`
	Part     *Part           `json:"part,omitempty"`
	Receipt  json.RawMessage `json:"receipt,omitempty"`
	Decision json.RawMessage `json:"decision,omitempty"`
}

func encodeRecoveryJob(req RecoveryUploadRequest) ([]byte, string, error) {
	doc := jobRequest{RecoveryID: req.RecoveryID, InstallID: req.InstallID, Secrets: req.Secrets, Steps: req.Steps, Entries: []jobEntry{}}
	canonical := func(v map[string]any) (json.RawMessage, error) {
		if v == nil {
			return nil, nil
		}
		return contract.Canonical(v)
	}
	for _, e := range req.Entries {
		var je jobEntry
		var err error
		if e.Fact != nil {
			je.Fact = e.Fact.Canonical
		}
		je.Part = e.Part
		if je.Receipt, err = canonical(e.Receipt); err != nil {
			return nil, "", protoErr(400, "invalid_request", "An entry's receipt is not valid protocol JSON.")
		}
		if je.Decision, err = canonical(e.Decision); err != nil {
			return nil, "", protoErr(400, "invalid_request", "A decision is not valid protocol JSON.")
		}
		doc.Entries = append(doc.Entries, je)
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

func decodeRecoveryJob(data []byte, accountID string) (RecoveryUploadRequest, error) {
	var doc jobRequest
	if err := json.Unmarshal(data, &doc); err != nil {
		return RecoveryUploadRequest{}, err
	}
	req := RecoveryUploadRequest{AccountID: accountID, InstallID: doc.InstallID, RecoveryID: doc.RecoveryID, Secrets: doc.Secrets,
		Steps: doc.Steps}
	object := func(raw json.RawMessage) (map[string]any, error) {
		if raw == nil {
			return nil, nil
		}
		v, err := contract.DecodeJSON(raw)
		if err != nil {
			return nil, err
		}
		m, _ := v.(map[string]any)
		return m, nil
	}
	for _, je := range doc.Entries {
		var e RecoveryEntry
		var err error
		if je.Fact != nil {
			v, err := contract.DecodeJSON(je.Fact)
			if err != nil {
				return RecoveryUploadRequest{}, err
			}
			if e.Fact, err = ParseFact(v); err != nil {
				return RecoveryUploadRequest{}, err
			}
		}
		e.Part = je.Part
		if e.Receipt, err = object(je.Receipt); err != nil {
			return RecoveryUploadRequest{}, err
		}
		if e.Decision, err = object(je.Decision); err != nil {
			return RecoveryUploadRequest{}, err
		}
		req.Entries = append(req.Entries, e)
	}
	return req, nil
}

// recoveryJobAD is a stored request's additional data (storage sealedTables).
func recoveryJobAD(accountID, recoveryID, key string) []byte {
	return []byte("recovery-job\x00" + accountID + "\x00" + recoveryID + "\x00" + key)
}

func (s *Service) sealRecoveryJob(accountID, recoveryID, key string, data []byte) []byte {
	if s.Box == nil {
		return data
	}
	return s.Box.Seal(data, recoveryJobAD(accountID, recoveryID, key))
}

func (s *Service) openRecoveryJob(accountID, recoveryID, key string, data []byte) ([]byte, error) {
	if s.Box == nil {
		return data, nil
	}
	return s.Box.Open(data, recoveryJobAD(accountID, recoveryID, key))
}

// ---- tests

// ErrCrashForTest, returned by a job hook (SetRecoveryJobHooksForTest), stops
// the job as a crash there would: the transaction it is in rolls back and
// the job's row stays for the next start.
var ErrCrashForTest = errors.New("protocol: a test crashed the recovery job")

// SetRecoveryJobHooksForTest sets a hook called at a job's named points
// ("attempt" before each attempt, "open" when a recovery snapshot holds the
// account live (holdLive), before its transaction, "cheap" when an upload evaluated at once
// holds the account's rebuild claim, before its transaction, "transition"
// inside its own transition's
// transaction, "transitioned" after that transaction, "publish" inside the
// publishing transaction), how long an upload request waits for its job, and
// the first retry's backoff (zero: the defaults).
func (s *Service) SetRecoveryJobHooksForTest(point func(name string) error, wait, backoff time.Duration) {
	reg := s.recoveryJobs()
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.hook, reg.wait, reg.backoff = point, wait, backoff
}

// WaitRecoveryJobsForTest waits for every running job of the store to end
// (a test's cleanup, before its data directory goes).
func (s *Service) WaitRecoveryJobsForTest() { s.recoveryJobs().wg.Wait() }

// RecoveryJobDoneForTest returns a channel closed once the account's running
// job (if any) has ended.
func (s *Service) RecoveryJobDoneForTest(accountID string) <-chan struct{} {
	reg := s.recoveryJobs()
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if j := reg.running[accountID]; j != nil {
		return j.done
	}
	done := make(chan struct{})
	close(done)
	return done
}

func (s *Service) jobPoint(name string) error {
	if hook, _, _ := s.recoveryJobs().seams(); hook != nil {
		return hook(name)
	}
	return nil
}
