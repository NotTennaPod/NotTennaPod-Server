package refresh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// maxReparseTimeouts is how many re-parses of a stored response for one
// parser version may run past their deadline before the response counts as
// unparseable for it: a hostile one cannot take every start's whole deadline
// for ever, and a slow start is tried again.
const maxReparseTimeouts = 3

// ReparseAll re-parses every stored response whose parser version is not the
// server's (PROTOCOL §9.4). Errors are logged; a response that no longer parses keeps
// the catalog as it is. One whose re-parse runs past its deadline stays to be
// re-parsed at the next pass (the next start), up to maxReparseTimeouts times;
// then it counts as unparseable. Its outcome is kept in the clear beside it
// (storage.ReparseOutcome), where diagnostics read it, and logged.
func (r *Refresher) ReparseAll(ctx context.Context) {
	if r.svc.Box == nil {
		return
	}
	var keys []storage.FeedResponseKey
	err := r.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		keys, err = tx.StoredFeedResponses()
		return err
	})
	if err != nil {
		if ctx.Err() == nil {
			r.log.Error("refresh: cannot list stored responses", "error", err)
		}
		return
	}
	for _, k := range keys {
		if ctx.Err() != nil {
			return
		}
		if err := safely(func() error { return r.reparse(ctx, k) }); err != nil && ctx.Err() == nil {
			r.log.Error("refresh: re-parse failed", "account", k.AccountID, "podcast", k.PodcastID, "error", err)
		}
	}
}

// reparse re-parses one stored response with its own source and evaluation
// time, as catalog.refresh parts with the outcome re-parse: no discovery,
// move or status. Each record's base is its current revision when nothing
// newer than the original fetch has been observed since, so the re-parse
// replaces its own observations; otherwise it competes by the original fetch
// time, and a newer observation stands (decided by the fetch time alone).
func (r *Refresher) reparse(ctx context.Context, k storage.FeedResponseKey) error {
	var sealedBytes []byte
	var outcome storage.ReparseOutcome
	err := r.svc.Store.Read(ctx, func(tx *storage.ReadTx) error {
		var err error
		if sealedBytes, err = tx.Account(k.AccountID).FeedResponse(k.PodcastID); err != nil {
			return err
		}
		outcome, err = tx.Account(k.AccountID).ReparseOutcome(k.PodcastID)
		return err
	})
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	s, err := feed.OpenStoredResponse(r.svc.Box, k.AccountID, k.PodcastID, sealedBytes)
	if err != nil {
		return err
	}
	if s.ParserVersion == feed.ParserVersion {
		return nil
	}
	parseCtx, cancel := context.WithTimeout(ctx, r.cfg.FetchLimit)
	parsed, perr := feed.Parse(parseCtx, s.Body, s.Source, time.UnixMilli(s.Now))
	cancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch {
	case errors.Is(perr, context.DeadlineExceeded) || errors.Is(perr, context.Canceled):
		// A re-parse cut short by its deadline (a loaded server, or a
		// hostile feed) is not done: it is tried again at the next pass,
		// until it has timed out maxReparseTimeouts times for this parser.
		if outcome.Parser != feed.ParserVersion {
			outcome = storage.ReparseOutcome{Parser: feed.ParserVersion}
		}
		if outcome.Timeouts++; outcome.Timeouts < maxReparseTimeouts {
			r.log.Warn("refresh: a re-parse ran past its deadline; it is tried again later", "account", k.AccountID, "podcast", k.PodcastID,
				"timeouts", outcome.Timeouts)
			break
		}
		outcome.Failure = "timeout"
		s.ParserVersion = feed.ParserVersion
		r.log.Warn("refresh: a stored response cannot be re-parsed in time; the catalog keeps its records", "account", k.AccountID,
			"podcast", k.PodcastID, "reason", fmt.Sprintf("the re-parse for %s ran past its %s deadline %d times", feed.ParserVersion,
				r.cfg.FetchLimit, outcome.Timeouts))
	case perr != nil:
		r.log.Warn("refresh: a stored response no longer parses; the catalog keeps its records", "account", k.AccountID, "podcast", k.PodcastID)
		outcome = storage.ReparseOutcome{}
		s.ParserVersion = feed.ParserVersion
	default:
		outcome = storage.ReparseOutcome{}
		s.ParserVersion = feed.ParserVersion
	}
	return r.svc.ServerCommit(ctx, k.AccountID, func(c *protocol.ApplyContext) error {
		current, err := c.Account.FeedResponse(k.PodcastID)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(current, sealedBytes) {
			return nil // a newer fetch replaced it
		}
		if perr == nil {
			// The bases are the revisions the response stood for (stored with
			// it): a record someone replaced since then keeps its newer value.
			bases := s.Revisions
			if bases == nil {
				// A response stored before revisions were kept: its own
				// records are those observed before its fetch time; at an
				// equal time the record may be a correction, so no base.
				stamps, err := c.Account.CatalogStamps(k.PodcastID)
				if err != nil {
					return err
				}
				bases = map[string]string{}
				for id, st := range stamps {
					if st.FetchTime < s.Now {
						bases[id] = st.Revision
					}
				}
			}
			x := ops.ServerFetch{PodcastID: k.PodcastID, RefreshID: newRefreshID(), FetchTime: s.Now,
				Feed: parsed, Bases: bases, Reparse: true, Namespace: c.Namespace}
			x.Revisions = func(revs map[string]string) { s.Revisions = revs }
			if err := ops.ApplyServerFetch(c, x); err != nil {
				return err
			}
		}
		resealed, err := s.Seal(r.svc.Box, k.AccountID, k.PodcastID)
		if err != nil {
			return err
		}
		return c.Account.PutReparsedFeedResponse(k.PodcastID, resealed, outcome)
	})
}
