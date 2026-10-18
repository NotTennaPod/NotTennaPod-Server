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
	InProgress  bool           // a fetch runs, or a request waits for one
	// Requests (PROTOCOL §9.6). In a request's answer RequestSeq is the
	// sequence that request got, 0 when it recorded none (a refresh of every
	// podcast leaving this one to its Retry-After); in a status read, the
	// newest request's. A request is answered once DoneSeq reaches its
	// sequence.
	RequestSeq int64
	DoneSeq    int64
	Running    bool   // a fetch runs now
	MergedInto string // a merged podcast's survivor: its requests are answered
}

// ErrUnknownPodcast reports a podcast the account does not have.
var ErrUnknownPodcast = errors.New("refresh: unknown podcast")

// MaxStatusPodcasts is the most podcasts one status read names.
const MaxStatusPodcasts = 500

// Request asks for a refresh now: of one podcast, whatever its state (a merged
// podcast's survivor for a merged one), or ("") of every podcast the schedule
// keeps updated. Each request is recorded with the podcast's schedule, so a
// restart loses none, and gets a sequence its answer carries. A podcast whose
// fetch is running joins it; any other runs its next fetch now, a single
// podcast's without validators (Android forces a single feed's refresh, not a
// refresh of all). A refresh of every podcast brings none forward past its
// Retry-After and records no request for it: such a podcast is fetched when it
// ends (PROTOCOL §9.6). It returns at once with each podcast's status.
func (r *Refresher) Request(ctx context.Context, accountID, podcastID string) ([]Status, error) {
	return r.AuthorizedRequest(ctx, accountID, podcastID, nil)
}

// AuthorizedRequest is Request for a caller: authorize runs in the
// transaction that schedules the refresh, and nothing is queued unless that
// transaction commits.
func (r *Refresher) AuthorizedRequest(ctx context.Context, accountID, podcastID string, authorize func(tx *storage.WriteTx) error) ([]Status, error) {
	var targets []storage.RefreshCandidate
	err := r.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
		if podcastID == "" {
			cands, err := tx.RefreshCandidates(accountID)
			for _, c := range cands {
				if scheduled, _ := eligibility(c); scheduled {
					targets = append(targets, c)
				}
			}
			return err
		}
		// A merged podcast's request is its survivor's (PROTOCOL §9.7 routing),
		// read podcast by podcast.
		for seen := map[string]bool{}; !seen[podcastID]; {
			seen[podcastID] = true
			found, err := tx.RefreshStatusFor(accountID, []string{podcastID})
			if err != nil || len(found) == 0 {
				return err
			}
			c := found[0]
			if c.State != "merged" {
				targets = []storage.RefreshCandidate{c}
				return nil
			}
			if c.MergedInto == "" {
				return nil
			}
			podcastID = c.MergedInto
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if podcastID != "" && len(targets) == 0 {
		return nil, ErrUnknownPodcast
	}
	if r.beforeRequestWrite != nil {
		r.beforeRequestWrite()
	}
	now := r.nowMs()
	running := map[string]bool{}
	r.mu.Lock()
	for _, c := range targets {
		if _, ok := r.running[key{accountID, c.PodcastID}]; ok {
			running[c.PodcastID] = true // joins the fetch in progress
		}
	}
	r.mu.Unlock()
	// A refresh of every podcast leaves one whose Retry-After has not passed
	// (read in the transaction that queues, so a Retry-After recorded since
	// the candidates were read wins) due when it passes: its schedule's.
	rows := map[string]storage.RefreshRow{}
	seqs := map[string]int64{}
	if len(targets) > 0 || authorize != nil {
		err := r.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
			clear(rows)
			clear(seqs)
			if authorize != nil {
				if err := authorize(tx); err != nil {
					return err
				}
			}
			acct := tx.Account(accountID)
			for _, c := range targets {
				p := c.PodcastID
				row, ok, err := acct.RefreshSchedule(p)
				if err != nil {
					return err
				}
				if !ok {
					if exists, err := acct.PodcastExists(p); err != nil || !exists {
						if err == nil && podcastID != "" {
							err = ErrUnknownPodcast
						}
						if err != nil {
							return err
						}
						continue // gone meanwhile
					}
					row = storage.RefreshRow{PodcastID: p, NextAt: now}
					if err := acct.PutRefreshSchedule(row); err != nil {
						return err
					}
				}
				switch {
				case running[p]:
				case podcastID == "" && row.NotBefore > now:
					if row.NextAt > row.NotBefore {
						row.NextAt = row.NotBefore
						if err := acct.PutRefreshSchedule(row); err != nil {
							return err
						}
					}
					rows[p] = row
					continue
				case row.NextAt > now:
					row.NextAt = now
					if err := acct.PutRefreshSchedule(row); err != nil {
						return err
					}
				}
				seq, err := acct.RecordRefreshRequest(p, podcastID != "")
				if err != nil {
					return err
				}
				if row, _, err = acct.RefreshSchedule(p); err != nil {
					return err
				}
				rows[p], seqs[p] = row, seq
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(seqs) > 0 {
			r.signal()
		}
	}
	answered := targets[:0]
	for _, c := range targets {
		row, ok := rows[c.PodcastID]
		if !ok {
			continue
		}
		c.Schedule = &row
		answered = append(answered, c)
	}
	out := r.statuses(accountID, answered, now)
	for i := range out {
		out[i].RequestSeq = seqs[out[i].PodcastID]
	}
	return out, nil
}

// Status reads the named podcasts' live refresh statuses (at most
// MaxStatusPodcasts), each by its keys alone, and schedules nothing: the
// refresh API's status read (PROTOCOL §9.6). authorize, when set, runs in the
// same read. A podcast the account does not have is left out.
func (r *Refresher) Status(ctx context.Context, accountID string, podcastIDs []string, authorize func(tx *storage.ReadTx) error) ([]Status, error) {
	if len(podcastIDs) > MaxStatusPodcasts {
		return nil, errors.New("refresh: too many podcasts in one status read")
	}
	var cands []storage.RefreshCandidate
	err := r.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
		if authorize != nil {
			if err := authorize(tx); err != nil {
				return err
			}
		}
		var err error
		cands, err = tx.RefreshStatusFor(accountID, podcastIDs)
		return err
	})
	if err != nil {
		return nil, err
	}
	return r.statuses(accountID, cands, r.nowMs()), nil
}

// statuses are the targets' live statuses from the candidates' rows and the
// fetches in progress.
func (r *Refresher) statuses(accountID string, targets []storage.RefreshCandidate, now int64) []Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Status, 0, len(targets))
	for _, c := range targets {
		k := key{accountID, c.PodcastID}
		_, running := r.running[k]
		st := Status{PodcastID: c.PodcastID, Running: running, InProgress: running}
		if c.State == "merged" {
			st.MergedInto = c.MergedInto
		}
		scheduled, needsPop := eligibility(c)
		if row := c.Schedule; row != nil {
			st.LastAttempt, st.LastSuccess = row.LastAttempt, row.LastSuccess
			st.RequestSeq, st.DoneSeq = row.RequestSeq, row.DoneSeq
			if row.LastError != "" {
				var e map[string]any
				if json.Unmarshal([]byte(row.LastError), &e) == nil {
					st.Error = e
				}
			}
			if scheduled || (needsPop && !running) {
				st.NextAttempt = max(row.NextAt, row.NotBefore)
			}
			if c.State != "merged" && row.RequestPending() && !running {
				st.InProgress, st.NextAttempt = true, now
			}
		}
		out = append(out, st)
	}
	return out
}
