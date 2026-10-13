package refresh

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Status is a podcast's live refresh status, as the refresh API answers it
// (live values, not the published refresh_status record).
type Status struct {
	PodcastID   string
	LastAttempt int64          // ms (0: never)
	LastSuccess int64          // ms (0: never)
	Error       map[string]any // {label, code?, detail?} (nil: none)
	NextAttempt int64          // ms (0: none planned)
	InProgress  bool           // a fetch runs or is about to
}

// ErrUnknownPodcast reports a podcast the account does not have.
var ErrUnknownPodcast = errors.New("refresh: unknown podcast")

// Request asks for a refresh now: of one podcast, whatever its state (a merged
// podcast's survivor for a merged one), or ("") of every podcast the schedule
// keeps updated. A podcast whose fetch is running joins it; any other runs its
// next fetch now, a single podcast's without validators (Android forces a
// single feed's refresh, not a refresh of all). It returns at once with each
// podcast's status.
func (r *Refresher) Request(ctx context.Context, accountID, podcastID string) ([]Status, error) {
	return r.AuthorizedRequest(ctx, accountID, podcastID, nil)
}

// AuthorizedRequest is Request for a caller: authorize runs in the
// transaction that schedules the refresh, and nothing is queued unless that
// transaction commits.
func (r *Refresher) AuthorizedRequest(ctx context.Context, accountID, podcastID string, authorize func(tx *storage.WriteTx) error) ([]Status, error) {
	var cands []storage.RefreshCandidate
	err := r.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		cands, err = tx.RefreshCandidates(accountID)
		return err
	})
	if err != nil {
		return nil, err
	}
	// A merged podcast's request is its survivor's (PROTOCOL §9.7 routing; server bug
	// hunt 2026-10, SB-1).
	byID := make(map[string]storage.RefreshCandidate, len(cands))
	for _, c := range cands {
		byID[c.PodcastID] = c
	}
	for seen := map[string]bool{}; podcastID != "" && !seen[podcastID]; {
		c, ok := byID[podcastID]
		if !ok || c.State != "merged" || c.MergedInto == "" {
			break
		}
		seen[podcastID] = true
		if _, ok := byID[c.MergedInto]; !ok {
			break
		}
		podcastID = c.MergedInto
	}
	var targets []storage.RefreshCandidate
	for _, c := range cands {
		scheduled, _ := eligibility(c)
		if (podcastID == "" && scheduled) || (c.PodcastID == podcastID && c.State != "merged") {
			targets = append(targets, c)
		}
	}
	if podcastID != "" && len(targets) == 0 {
		return nil, ErrUnknownPodcast
	}
	now := r.nowMs()
	var queued []string
	r.mu.Lock()
	for _, c := range targets {
		if _, running := r.running[key{accountID, c.PodcastID}]; running {
			continue // joins the fetch in progress
		}
		queued = append(queued, c.PodcastID)
	}
	r.mu.Unlock()
	if len(queued) > 0 || authorize != nil {
		// Persisted too, so a restart still runs a scheduled podcast now.
		err := r.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
			if authorize != nil {
				if err := authorize(tx); err != nil {
					return err
				}
			}
			acct := tx.Account(accountID)
			for _, p := range queued {
				row, _, err := acct.RefreshSchedule(p)
				if err != nil {
					return err
				}
				if row.PodcastID == "" {
					row.PodcastID = p
				} else if row.NextAt <= now {
					continue
				}
				row.NextAt = now
				if err := acct.PutRefreshSchedule(row); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		for _, p := range queued {
			k := key{accountID, p}
			r.manual[k] = r.manual[k] || podcastID != ""
		}
		r.mu.Unlock()
		r.signal()
	}
	return r.statuses(accountID, targets, now), nil
}

// statuses are the targets' live statuses from the candidates' rows and the
// fetches in progress or queued.
func (r *Refresher) statuses(accountID string, targets []storage.RefreshCandidate, now int64) []Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Status, 0, len(targets))
	for _, c := range targets {
		k := key{accountID, c.PodcastID}
		_, running := r.running[k]
		_, queued := r.manual[k]
		st := Status{PodcastID: c.PodcastID, InProgress: running || queued}
		scheduled, needsPop := eligibility(c)
		if row := c.Schedule; row != nil {
			st.LastAttempt, st.LastSuccess = row.LastAttempt, row.LastSuccess
			if row.LastError != "" {
				var e map[string]any
				if json.Unmarshal([]byte(row.LastError), &e) == nil {
					st.Error = e
				}
			}
			if scheduled || (needsPop && !running) {
				st.NextAttempt = max(row.NextAt, row.NotBefore)
			}
		}
		if queued {
			st.NextAttempt = now
		}
		out = append(out, st)
	}
	return out
}
