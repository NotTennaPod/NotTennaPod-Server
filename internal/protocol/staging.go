package protocol

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The staging fallback of a rebuild (PROTOCOL §6): an account whose archive holds at least
// StagingThreshold entries is rebuilt, and a recovery request joined, in a copy of the
// database, so the server's one writer is held only while the result is published.

// DefaultStagingThreshold is the archive size (facts and decisions) from
// which rebuilds run in a staging copy: about 2–3 s of the write lock in
// place on a loaded development machine (lowered until the cost estimate below is measured
// further).
const DefaultStagingThreshold = 2_000

// StagingConcentration is the scope concentration (Store.RebuildCost) from
// which a rebuild is staged whatever the archive's size: a history of about
// 1,000 contributions in one episode. A concentrated 9,018-fact history held
// the writer for 66 s in place.
const StagingConcentration = 1_000_000

// stagingAttempts bounds how often a staged rebuild starts again because the
// account's head moved before publication (a server fetch's commit); then
// it runs in place.
const stagingAttempts = 3

func (s *Service) stagingThreshold() int64 {
	switch {
	case s.StagingThreshold < 0:
		return -1
	case s.StagingThreshold == 0:
		return DefaultStagingThreshold
	}
	return int64(s.StagingThreshold)
}

// staged runs fn on a service over a staging copy of the database and
// publishes the account's result (storage.PublishStaged). It reports false
// when the account is below the threshold or the copy could not be published
// (the caller then runs in place); the account is busy meanwhile.
//
// check, when set, runs first in the live publication transaction: an
// uploader is authorized again there (a revocation during the staged rebuild wins).
func (s *Service) staged(ctx context.Context, accountID string, check func(*storage.WriteTx) error,
	fn func(staged *Service) error) (bool, time.Duration, error) {
	threshold := s.stagingThreshold()
	if threshold < 0 {
		return false, 0, nil
	}
	size, concentration, err := s.Store.RebuildCost(ctx, accountID)
	if err != nil {
		return false, 0, err
	}
	if size < threshold && (s.StagingThreshold != 0 || concentration < StagingConcentration) {
		return false, 0, nil
	}
	done, ok := s.claimRebuild(accountID)
	if !ok {
		return true, 0, errBusy
	}
	defer done()
	dir := s.Store.RebuildStagingDir(accountID)
	for range stagingAttempts {
		st, err := s.Store.StageCopy(ctx, dir)
		if err != nil {
			return true, 0, err
		}
		base, err := st.Head(ctx, accountID)
		if err != nil {
			st.Close()
			return true, 0, err
		}
		ss := &Service{Store: st, Registry: s.Registry, Box: s.Box, Now: s.Now, Random: s.Random, StagingThreshold: -1}
		err = fn(ss)
		if cerr := st.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.RemoveAll(dir)
			return true, 0, err
		}
		began := time.Now()
		err = s.Store.PublishStaged(ctx, filepath.Join(dir, "nottennapod.db"), accountID, base, check)
		locked := time.Since(began)
		_ = os.RemoveAll(dir)
		switch {
		case errors.Is(err, storage.ErrImportChanged):
			continue // the head moved: stage again
		case errors.Is(err, storage.ErrStagedEvidence):
			return true, 0, errStagedEvidence // the caller runs the transition live, then stages again
		}
		return true, locked, err
	}
	return false, 0, nil
}

// errStagedEvidence reports a staged recovery upload that proved a lost
// timeline: its epoch transition must run live first.
var errStagedEvidence = errors.New("protocol: a staged upload proved a lost timeline")

var errBusy = &Error{Status: 503, Code: "server_busy", Message: "This library is being rebuilt from its archive; try again shortly.",
	RetryAfter: 10 * time.Second}

// ClaimRebuildForTest marks a rebuild of the account as running, as a staged
// rebuild does (tests of the busy answers).
func (s *Service) ClaimRebuildForTest(accountID string) (func(), bool) {
	return s.claimRebuild(accountID)
}
