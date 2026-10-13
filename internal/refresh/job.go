package refresh

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// failure is a failed fetch or parse as refresh_status shows it: Android's
// download-error label, the HTTP status for http_status, a sanitized detail.
type failure struct {
	label      string
	code       int
	detail     string
	retryAfter time.Duration
}

func (f *failure) object() map[string]any {
	o := map[string]any{"label": f.label}
	if f.code != 0 {
		o["code"] = int64(f.code)
	}
	if f.detail != "" {
		o["detail"] = f.detail
	}
	return o
}

// statusKey is the outcome class and error a published status is compared by
// (PROTOCOL §9.4). The detail is left out: a retry failing the same way with
// another wording does not republish.
func statusKey(f *failure) string {
	if f == nil {
		return "ok"
	}
	return "error/" + f.label + "/" + strconv.Itoa(f.code)
}

// A failure's detail is stored and replicated (refresh_status), so it keeps
// no URL userinfo, by the feed fields' rule: a website's title can quote a
// URL with credentials.
func fetchFailure(err error) *failure {
	var fe *feed.FetchError
	if errors.As(err, &fe) {
		return &failure{label: fe.Label, code: fe.Code, detail: ops.StripUserinfo(fe.Detail), retryAfter: fe.RetryAfter}
	}
	return &failure{label: "io"}
}

func parseFailure(err error) *failure {
	var pe *feed.ParseError
	if errors.As(err, &pe) {
		return &failure{label: pe.Label, detail: ops.StripUserinfo(pe.Detail)}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &failure{label: "time_limit"} // the fetch's deadline passed while parsing
	}
	return &failure{label: "parser_exception"}
}

// captured is what a fetch reads before its network I/O.
type captured struct {
	rc        ops.RefreshContext
	populated bool // the live generation's first population is done
	row       storage.RefreshRow
	stored    *feed.StoredResponse
	credURL   string // the user's URL when the secret was set (legacy credentials' origin)
}

var errGone = errors.New("refresh: the podcast no longer exists")

// capture reads the podcast's refresh context in a server commit that writes
// nothing.
func (r *Refresher) capture(ctx context.Context, k key) (captured, error) {
	var cp captured
	var sealedBytes []byte
	err := r.svc.ServerCommit(ctx, k.account, func(c *protocol.ApplyContext) error {
		if exists, err := c.Account.PodcastExists(k.podcast); err != nil || !exists {
			if err == nil {
				err = errGone
			}
			return err
		}
		var err error
		if cp.rc, err = ops.CaptureRefresh(c, k.podcast); err != nil {
			return err
		}
		if cp.credURL, err = ops.CredentialURL(c, k.podcast); err != nil {
			return err
		}
		if cp.rc.GenerationID != "" {
			node, err := c.Account.Population(cp.rc.GenerationID)
			if err != nil {
				return err
			}
			cp.populated = node != ""
		}
		if cp.row, _, err = c.Account.RefreshSchedule(k.podcast); err != nil {
			return err
		}
		cp.row.PodcastID = k.podcast
		sealedBytes, err = c.Account.FeedResponse(k.podcast)
		if errors.Is(err, storage.ErrNotFound) {
			err = nil
		}
		return err
	})
	if err != nil {
		return cp, err
	}
	if sealedBytes != nil && r.svc.Box != nil {
		if s, err := feed.OpenStoredResponse(r.svc.Box, k.account, k.podcast, sealedBytes); err == nil {
			cp.stored = &s
		} else {
			r.log.Warn("refresh: a stored response does not open; fetching without validators", "account", k.account, "podcast", k.podcast)
		}
	}
	return cp, nil
}

// credentials builds the fetch's credentials from the generation's secret: its
// approved origins, or, for a secret stored without them, the origin of the
// refresh URL the user had set when the secret was set: never one a server
// move or a later URL change set (PROTOCOL §9.4).
func (r *Refresher) credentials(ctx context.Context, account string, rc ops.RefreshContext, credURL string) *feed.Credentials {
	if rc.SecretID == "" || r.svc.Box == nil {
		return nil
	}
	v, err := r.svc.SecretValue(ctx, account, rc.SecretID)
	if err != nil {
		r.log.Warn("refresh: the feed credentials cannot be read; fetching without them", "account", account, "podcast", rc.PodcastID)
		return nil
	}
	c := &feed.Credentials{Revision: rc.SecretID}
	c.Username, _ = v["username"].(string)
	c.Password, _ = v["password"].(string)
	if list, ok := v["approved_origins"].([]any); ok {
		c.ApprovedOrigins = []string{}
		for _, o := range list {
			if s, ok := o.(string); ok {
				c.ApprovedOrigins = append(c.ApprovedOrigins, s)
			}
		}
	} else if u, err := url.Parse(credURL); err == nil && u.Host != "" {
		c.ApprovedOrigins = []string{feed.Origin(u)}
	}
	return c
}

// credentialRevision is the credential part of a representation key.
func credentialRevision(representationKey string) string {
	i := strings.LastIndexByte(representationKey, 0)
	if i < 0 {
		return ""
	}
	return representationKey[i+1:]
}

// hopTargets are the targets of the trace's 301 and 308 hops (PROTOCOL §3.3 aliases).
func hopTargets(trace []feed.Hop) []string {
	var out []string
	for i, h := range trace {
		if (h.Status == 301 || h.Status == 308) && i+1 < len(trace) {
			out = append(out, trace[i+1].URL)
		}
	}
	return out
}

// newFeedURL is a parsed itunes:new-feed-url usable as a refresh URL: http(s),
// without userinfo or fragment.
func newFeedURL(f *feed.Feed) string {
	if f == nil || f.RedirectURL == nil {
		return ""
	}
	u, err := url.Parse(strings.TrimSpace(*f.RedirectURL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return ""
	}
	u.User, u.Fragment, u.RawFragment = nil, "", ""
	return u.String()
}

// verified fetches a new-feed-url: it counts once it answers a feed itself,
// without moving on permanently. The fetch and the parse share one deadline.
// A panic is no answer.
func (r *Refresher) verified(ctx context.Context, k key, target string, creds *feed.Credentials) bool {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.FetchLimit)
	defer cancel()
	ok := false
	err := safely(func() error {
		res, err := r.cfg.Fetcher.Fetch(ctx, feed.FetchRequest{URL: target, Credentials: creds, Force: true})
		if err != nil || res.NotModified || res.PermanentMove != "" {
			return nil
		}
		_, err = feed.Parse(ctx, res.Body, target, r.cfg.Clock.Now())
		ok = err == nil
		return nil
	})
	if p := (*panicked)(nil); errors.As(err, &p) {
		r.logPanic(k, p)
	}
	return ok
}

// refresh runs one fetch of a podcast: capture, fetch and parse outside any
// transaction, then one server commit with its catalog effects, the stored
// response, its schedule and, when due, its status.
func (r *Refresher) refresh(ctx context.Context, k key, force bool) error {
	cp, err := r.capture(ctx, k)
	if errors.Is(err, errGone) {
		return nil
	}
	if err != nil {
		return err
	}
	rc := cp.rc
	creds := r.credentials(ctx, k.account, rc, cp.credURL)
	credRev := ""
	if creds != nil {
		credRev = creds.Revision
	}
	req := feed.FetchRequest{URL: rc.RefreshURL, Credentials: creds, Force: true}
	// Validators: not for a manual refresh, the first fetch after a failure, a
	// first population, or a changed URL or credentials (PROTOCOL §9.2, §9.4).
	if s := cp.stored; !force && cp.row.Failures == 0 && (rc.GenerationID == "" || cp.populated) && s != nil &&
		s.Source == rc.RefreshURL && credentialRevision(s.RepresentationKey) == credRev {
		req.Force = false
		req.Validators = &feed.Validators{Key: s.RepresentationKey, ETag: s.ETag, LastModified: s.LastModified}
	}
	start := r.nowMs()
	var fail *failure
	var parsed *feed.Feed
	var res *feed.FetchResult
	// The fetch and the parse run under one deadline (PROTOCOL §9.2): a feed that
	// parses too slowly fails with time_limit.
	fetchCtx, cancelFetch := context.WithTimeout(ctx, r.cfg.FetchLimit)
	defer cancelFetch()
	err = safely(func() error {
		var err error
		res, err = r.cfg.Fetcher.Fetch(fetchCtx, req)
		return err
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var p *panicked
	switch {
	case errors.As(err, &p):
		// A panic is a failed refresh like any other; feed.Parse turns its own into
		// parser_exception.
		r.logPanic(k, p)
		res, fail = nil, panicFailure()
	case err != nil:
		fail = fetchFailure(err)
	case !res.NotModified:
		parsed, err = feed.Parse(fetchCtx, res.Body, rc.RefreshURL, time.UnixMilli(start))
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			fail = parseFailure(err)
		}
	}
	// Applying runs under the panic net, its preparation included; a panic
	// there or in its commit (rolled back) is returned as *panicked, and the
	// worker records the refresh as failed (failAfterPanic).
	return safely(func() error { return r.apply(ctx, k, cp, creds, start, res, parsed, fail) })
}

// failAfterPanic records a refresh a panic stopped (reading its context, or
// applying its fetch) as a failed refresh like any other: from its context read again, its status
// parser_exception, its attempt and error, and the normal backoff, in one
// commit. When that cannot be written (storage unavailable, or another
// panic), the podcast still backs off from its schedule as it stands
// (deferAfterPanic), and the failure is logged without the panic's text.
func (r *Refresher) failAfterPanic(ctx context.Context, k key) {
	applied := false
	err := safely(func() error {
		cp, err := r.capture(ctx, k)
		if errors.Is(err, errGone) {
			return nil
		}
		if err != nil {
			return err
		}
		applied = true
		return r.apply(ctx, k, cp, nil, r.nowMs(), nil, nil, panicFailure())
	})
	if err == nil || ctx.Err() != nil {
		return
	}
	var p *panicked
	if errors.As(err, &p) {
		r.log.Error("refresh: a panic stopped recording a failed refresh", "account", k.account, "podcast", k.podcast, "stack", p.stack)
	} else {
		r.log.Error("refresh: cannot record a failed refresh after a panic", "account", k.account, "podcast", k.podcast, "error", err)
		if applied {
			return // apply backed the podcast off (deferAfterError)
		}
	}
	if err := safely(func() error { r.deferAfterPanic(ctx, k); return nil }); errors.As(err, &p) {
		r.log.Error("refresh: a panic stopped backing a podcast off", "account", k.account, "podcast", k.podcast, "stack", p.stack)
	}
}

// panicFailure is a refresh a panic failed: parser_exception, without a
// detail (the panic's text can quote a URL with credentials).
func panicFailure() *failure { return &failure{label: "parser_exception"} }

// logPanic logs a panic a refresh turned into a failure: a fixed message and
// the stack, never the panic's text.
func (r *Refresher) logPanic(k key, p *panicked) {
	r.log.Error("refresh: a panic failed the refresh", "account", k.account, "podcast", k.podcast, "stack", p.stack)
}

// apply writes a fetch's outcome in one server commit: its catalog effects,
// the stored response, its schedule and, when due, its status. A panic in
// the commit is returned as *panicked, the commit rolled back.
func (r *Refresher) apply(ctx context.Context, k key, cp captured, creds *feed.Credentials, start int64, res *feed.FetchResult,
	parsed *feed.Feed, fail *failure) error {
	rc := cp.rc
	scheduled := rc.State == "subscribed" && rc.KeepUpdated
	retries := rc.GenerationID != "" && !cp.populated && fail != nil // the first population is retried
	newKey := statusKey(fail)
	done := r.nowMs()
	notModified := res != nil && res.NotModified && fail == nil
	// Moves: a fetched new-feed-url wins over a permanent redirect (A redirects
	// to B, whose feed names C: the URL becomes C, and B and C are both
	// aliases). A 304 can still carry a permanent move.
	var move, newFeed string
	var aliases []string
	if parsed != nil || notModified {
		move, aliases = res.PermanentMove, hopTargets(res.Trace)
	}
	if parsed != nil {
		if nf := newFeedURL(parsed); nf != "" && nf != rc.RefreshURL {
			if nf == res.FinalURL || r.verified(ctx, k, nf, creds) {
				move, newFeed, aliases = nf, nf, append(aliases, nf)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			done = r.nowMs()
		}
	}
	// The response is sealed inside the commit, with the revisions its fetch
	// stands for (a re-parse's bases).
	var stored *feed.StoredResponse
	if parsed != nil && r.svc.Box != nil {
		stored = &feed.StoredResponse{RepresentationKey: res.RepresentationKey, ETag: res.ETag, LastModified: res.LastModified,
			Source: rc.RefreshURL, Now: start, ParserVersion: feed.ParserVersion, Body: res.Body}
	}
	// The schedule after this attempt.
	next := storage.RefreshRow{PodcastID: k.podcast, LastAttempt: start, LastSuccess: cp.row.LastSuccess,
		StatusPublishedAt: cp.row.StatusPublishedAt, StatusKey: cp.row.StatusKey, Context: cp.row.Context}
	if rc.GenerationID != "" && rc.State != "removed" {
		next.Context = rc.GenerationID + "\x00" + rc.RefreshURL + "\x00" + rc.SecretID
	}
	if fail == nil {
		next.LastSuccess = start
		next.NextAt = done + r.jitter(r.cfg.Interval).Milliseconds()
	} else {
		next.Failures = cp.row.Failures + 1
		next.NextAt = done + r.backoff(next.Failures).Milliseconds()
		if fail.retryAfter > 0 {
			// A lower bound that jitter never brings forward.
			next.NotBefore = done + min(fail.retryAfter, retryAfterMax).Milliseconds()
			next.NextAt = max(next.NextAt, next.NotBefore)
		}
		b, err := contract.Canonical(fail.object())
		if err != nil {
			return err
		}
		next.LastError = string(b)
	}
	// The status (ops.RefreshStatus): published when due, otherwise carried
	// by a part 0 written for another reason (PROTOCOL §9.4).
	outcome := ops.FetchOutcome{FetchTime: start}
	if fail != nil {
		outcome.Error = fail.object()
		if cp.row.LastSuccess != 0 {
			last := cp.row.LastSuccess
			outcome.LastSuccess = &last
		}
	}
	if scheduled || retries {
		nextAt := next.NextAt
		outcome.NextAttempt = &nextAt
	}
	x := ops.ServerFetch{PodcastID: k.podcast, GenerationID: rc.GenerationID, RefreshBy: rc.RefreshBy, RefreshID: newRefreshID(),
		FetchTime: start, Feed: parsed, NotModified: notModified, Bases: rc.Bases, Move: move, Aliases: aliases, NewFeedURL: newFeed}
	if parsed != nil || notModified {
		x.MovedTo = res.PermanentMove
	}
	if stored != nil {
		x.Revisions = func(revs map[string]string) { stored.Revisions = revs }
	}
	moved := false
	err := safely(func() error {
		return r.svc.ServerCommit(ctx, k.account, func(c *protocol.ApplyContext) error {
			if exists, err := c.Account.PodcastExists(k.podcast); err != nil || !exists {
				return err
			}
			x.Namespace = c.Namespace
			current, err := ops.RefreshStatusRecord(c.Account, k.podcast)
			if err != nil {
				return err
			}
			status, due := ops.RefreshStatus(current, outcome)
			x.Status, x.CurrentStatus = nil, nil
			if due {
				x.Status = status
				next.StatusKey, next.StatusPublishedAt = newKey, done
			} else {
				x.CurrentStatus = status
			}
			if err := ops.ApplyServerFetch(c, x); err != nil {
				return err
			}
			if move != "" {
				after, err := ops.CaptureRefresh(c, k.podcast)
				if err != nil {
					return err
				}
				moved = after.RefreshURL == move && after.GenerationID == rc.GenerationID
				// The schedule belongs to the context after this fetch's own move,
				// so the scheduler does not take the move for a user's change; a
				// move left out (the user changed the URL meanwhile) keeps the
				// context the fetch started from.
				if moved && after.GenerationID != "" && after.State != "removed" {
					next.Context = after.GenerationID + "\x00" + after.RefreshURL + "\x00" + after.SecretID
				}
			}
			if stored != nil {
				sealedResp, err := stored.Seal(r.svc.Box, k.account, k.podcast)
				if err != nil {
					return err
				}
				if err := c.Account.PutFeedResponse(k.podcast, sealedResp); err != nil {
					return err
				}
			}
			// A request's next_at = now written during the fetch was joined by it.
			return c.Account.PutRefreshSchedule(next)
		})
	})
	if p := (*panicked)(nil); errors.As(err, &p) {
		return err
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.deferAfterError(ctx, k, cp.row)
		return err
	}
	if moved {
		// The fetch's own move is not a change of context: the next scheduled
		// fetch uses the new URL (without validators, as its source differs).
		before := fetchContext(storage.RefreshCandidate{GenerationID: rc.GenerationID, RefreshURL: rc.RefreshURL, SecretID: rc.SecretID})
		r.mu.Lock()
		if cur, seen := r.contexts[k]; !seen || cur == before {
			r.contexts[k] = fetchContext(storage.RefreshCandidate{GenerationID: rc.GenerationID, RefreshURL: move, SecretID: rc.SecretID})
		}
		r.mu.Unlock()
	}
	return nil
}

// deferAfterPanic backs a podcast off after a panic when no failed refresh
// could be recorded (failAfterPanic), from its schedule as it stands.
func (r *Refresher) deferAfterPanic(ctx context.Context, k key) {
	err := r.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		acct := tx.Account(k.account)
		if exists, err := acct.PodcastExists(k.podcast); err != nil || !exists {
			return err
		}
		row, _, err := acct.RefreshSchedule(k.podcast)
		if err != nil {
			return err
		}
		row.PodcastID = k.podcast
		row.Failures++
		row.NextAt = r.nowMs() + r.backoff(row.Failures).Milliseconds()
		return acct.PutRefreshSchedule(row)
	})
	if err != nil && ctx.Err() == nil {
		r.log.Error("refresh: cannot defer a podcast after a panic", "account", k.account, "podcast", k.podcast, "error", err)
	}
}

// deferAfterError backs a podcast off after a fetch that could not be applied
// (a storage error), so it is not retried in a loop. Its status is unchanged.
func (r *Refresher) deferAfterError(ctx context.Context, k key, row storage.RefreshRow) {
	err := r.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		acct := tx.Account(k.account)
		if exists, err := acct.PodcastExists(k.podcast); err != nil || !exists {
			return err
		}
		row.PodcastID = k.podcast
		row.Failures++
		row.NextAt = r.nowMs() + r.backoff(row.Failures).Milliseconds()
		return acct.PutRefreshSchedule(row)
	})
	if err != nil && ctx.Err() == nil {
		r.log.Error("refresh: cannot defer a podcast after an error", "account", k.account, "podcast", k.podcast, "error", err)
	}
}
