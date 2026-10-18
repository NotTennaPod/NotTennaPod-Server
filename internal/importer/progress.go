package importer

import (
	"context"
	"errors"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// An import's progress (PROTOCOL §11.5): the chunks an upload holds, the step
// a processing import is at, and the first refresh of a published import's
// podcasts. A poll reads the import's row without its result and the runner's
// own counters: its cost never grows with the library or the report.

const (
	// MaxChunks bounds a chunk's index (0 to MaxChunks-1): a whole file in
	// 1 MiB chunks. An upload's chunk list is never longer.
	MaxChunks = 2048
	// RefreshReport is how long after publication an import reports the first
	// refresh of its podcasts; what it has not attempted by then is left to the
	// schedule, never reported as refreshed.
	RefreshReport = time.Hour
	// refreshKeep is when the runner forgets a first refresh's counts: at the
	// next poll, publication or sweep after it.
	refreshKeep = 2 * time.Hour
)

// Phase is a processing import's step: reading (the file), importing (its
// podcasts, Done of Total) or finishing (the report and the publication).
type Phase struct {
	Name        string
	Done, Total int
}

// RefreshProgress is a published import's first refresh. Status is running,
// done (every podcast attempted), expired (RefreshReport passed first) or
// unavailable (no exact counts: the server restarted since publication, or the
// podcasts could not be listed; none are claimed).
// The podcasts are exactly those the schedule fetches at publication (scheduled,
// or awaiting their first population), listed from the staged library that the
// publication copies, before it commits; later changes (a first population, an
// unsubscription, keep-updated turned off) never change the set: a podcast that
// stops being refreshed stays counted as not attempted. A podcast counts as
// attempted once one fetch of it ended after publication, and as succeeded once
// one of them succeeded (a later failure does not undo it); failed is attempted
// but not succeeded.
type RefreshProgress struct {
	Status                              string
	Total, Attempted, Succeeded, Failed int
	Until                               int64 // ms: the end of the report
}

// State is an import as a progress poll answers it.
type State struct {
	Import     storage.Import   // without its result
	Chunks     [][2]int64       // uploading: the chunks held, [index, length], by index
	Received   int64            // uploading: their bytes
	Processing *Phase           // processing: nil when not known (waiting for its turn, or resumed after a restart)
	Refresh    *RefreshProgress // published within RefreshReport (or while its counts are kept)
}

// outcome is a first-refresh podcast's state; it only moves up.
type outcome uint8

const (
	notAttempted outcome = iota
	failedOnly
	refreshed
)

// cohort is a published import's first refresh, counted in memory: its
// podcasts, fixed from the staged library, and each one's outcome. It is
// registered complete before the publication commits, so every attempt that
// ends after publication finds it; an attempt is then one map lookup.
type cohort struct {
	importID             string
	members              map[string]outcome
	since, until         int64 // ms: publication, end of the report
	attempted, succeeded int
	unavailable          bool // the podcasts could not be listed
}

var chunkName = regexp.MustCompile(`^\d{6}$`)

// Status answers an import's progress poll (GET /v1/import/{id}/progress). An
// uploading import's chunks are listed under its lock, its state read again
// there, so the status and the folder agree.
func (r *Runner) Status(ctx context.Context, accountID, importID string) (State, error) {
	im, err := r.state(ctx, accountID, importID)
	if err != nil {
		return State{}, err
	}
	if im.Status == "uploading" {
		unlock := r.lock(importID)
		defer unlock()
		if im, err = r.state(ctx, accountID, importID); err != nil {
			return State{}, err
		}
		if im.Status == "uploading" {
			chunks, total, err := r.received(importID)
			if err != nil {
				return State{}, err
			}
			return State{Import: im, Chunks: chunks, Received: total}, nil
		}
	}
	st := State{Import: im}
	switch im.Status {
	case "processing":
		r.mu.Lock()
		if p := r.phases[importID]; p != nil {
			cp := *p
			st.Processing = &cp
		}
		r.mu.Unlock()
	case "published":
		st.Refresh = r.refreshProgress(accountID, im)
	}
	return st, nil
}

// state reads one of the account's imports without its result.
func (r *Runner) state(ctx context.Context, accountID, importID string) (storage.Import, error) {
	var im storage.Import
	err := r.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		im, err = tx.Account(accountID).ImportState(importID)
		return err
	})
	if errors.Is(err, storage.ErrNotFound) {
		return im, ErrNotFound
	}
	return im, err
}

// received lists the durable chunks of an upload ([index, length], by index;
// the import's lock held): regular files named by their index; a temporary
// still being written is not one.
func (r *Runner) received(importID string) ([][2]int64, int64, error) {
	entries, err := os.ReadDir(r.uploadDir(importID))
	if errors.Is(err, os.ErrNotExist) {
		return [][2]int64{}, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	out := [][2]int64{}
	var total int64
	for _, e := range entries {
		if !e.Type().IsRegular() || !chunkName.MatchString(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, 0, err
		}
		n, _ := strconv.Atoi(e.Name())
		out = append(out, [2]int64{int64(n), info.Size()})
		total += info.Size()
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out, total, nil
}

// setPhase records a processing import's step (nil: not known).
func (r *Runner) setPhase(importID string, p *Phase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.onPhase != nil {
		r.onPhase(importID, p)
	}
	if p == nil {
		delete(r.phases, importID)
		return
	}
	if r.phases == nil {
		r.phases = map[string]*Phase{}
	}
	r.phases[importID] = p
}

// importedPodcast advances the importing step after a podcast's commit.
func (r *Runner) importedPodcast(importID string, done, total int) {
	r.setPhase(importID, &Phase{Name: "importing", Done: done, Total: total})
}

func (r *Runner) now() int64 {
	if r.Now != nil {
		return r.Now().UnixMilli()
	}
	return r.Store.Now().UnixMilli()
}

// watchRefresh registers an import's first refresh just before its
// publication commits: its podcasts are listed from the staged library, which
// the publication copies whole into the empty account, so they are exactly
// those eligible once it commits. A listing that fails registers the counts as
// unavailable. Counts of first refreshes past refreshKeep go at the same time.
func (r *Runner) watchRefresh(ctx context.Context, staging *storage.Store, accountID, importID string) {
	if r.Cohort == nil {
		return
	}
	members, err := r.Cohort(ctx, staging, accountID)
	if err != nil {
		r.logger().Error("import: cannot list the podcasts of the first refresh", "import", importID, "error", logged(err))
	}
	now := r.now()
	c := &cohort{importID: importID, since: now, until: now + RefreshReport.Milliseconds(), unavailable: err != nil}
	if err == nil {
		c.members = make(map[string]outcome, len(members))
		for _, p := range members {
			c.members[p] = notAttempted
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictRefreshLocked(now)
	if r.cohorts == nil {
		r.cohorts = map[string]*cohort{}
	}
	r.cohorts[accountID] = c
}

// unwatchRefresh forgets an import's first refresh (its publication failed).
func (r *Runner) unwatchRefresh(accountID, importID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c := r.cohorts[accountID]; c != nil && c.importID == importID {
		delete(r.cohorts, accountID)
	}
}

// publishedRefresh dates a first refresh from its publication's own time (the
// import's updated_at, as polls see it), once that committed.
func (r *Runner) publishedRefresh(ctx context.Context, accountID, importID string) {
	im, err := r.state(ctx, accountID, importID)
	if err != nil {
		return // dated from just before publication
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c := r.cohorts[accountID]; c != nil && c.importID == importID {
		c.since, c.until = im.UpdatedAt, im.UpdatedAt+RefreshReport.Milliseconds()
	}
}

// raise records an ended attempt of a member.
func (c *cohort) raise(podcast string, o outcome) {
	cur, ok := c.members[podcast]
	if !ok || o <= cur {
		return
	}
	if cur == notAttempted {
		c.attempted++
	}
	if o == refreshed {
		c.succeeded++
	}
	c.members[podcast] = o
}

// AfterAttempt counts a fetch that ended (refresh.Config.AfterAttempt): a map
// lookup under the runner's lock, nothing more. Attempts after the report's
// end no longer count, so an expired report stays as it was.
func (r *Runner) AfterAttempt(accountID, podcastID string, success bool) {
	o := failedOnly
	if success {
		o = refreshed
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if c := r.cohorts[accountID]; c != nil && !c.unavailable && now < c.until {
		c.raise(podcastID, o)
	}
}

// evictRefreshLocked forgets the counts of first refreshes past refreshKeep
// (r.mu held).
func (r *Runner) evictRefreshLocked(now int64) {
	for a, c := range r.cohorts {
		if now >= c.since+refreshKeep.Milliseconds() {
			delete(r.cohorts, a)
		}
	}
}

// evictRefresh is evictRefreshLocked for the periodic sweep.
func (r *Runner) evictRefresh() {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictRefreshLocked(now)
}

// refreshProgress is a published import's first refresh, if it is still
// reported. Counts forgotten (a restart) or never listed are unavailable,
// never rebuilt from the schedule.
func (r *Runner) refreshProgress(accountID string, im storage.Import) *RefreshProgress {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictRefreshLocked(now)
	c := r.cohorts[accountID]
	if c != nil && c.importID != im.ImportID {
		c = nil
	}
	// The report ends an hour after the publication, as the import's row dates it.
	until := im.UpdatedAt + RefreshReport.Milliseconds()
	switch {
	case c != nil && !c.unavailable:
		p := &RefreshProgress{Status: "running", Total: len(c.members), Attempted: c.attempted, Succeeded: c.succeeded,
			Failed: c.attempted - c.succeeded, Until: until}
		switch {
		case c.attempted == len(c.members):
			p.Status = "done"
		case now >= until:
			p.Status = "expired"
		}
		return p
	case now < until:
		return &RefreshProgress{Status: "unavailable", Until: until}
	}
	return nil
}

// forgetRefresh drops the first refresh of discarded imports.
func (r *Runner) forgetRefresh(importID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for a, c := range r.cohorts {
		if c.importID == importID {
			delete(r.cohorts, a)
		}
	}
}
