package ops

import (
	"errors"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Session continuations (PROTOCOL §6.10): a session's facts belong to (session,
// effective author). The start's author holds the root continuation; a
// descendant author's continuation follows its parent's.

// continuationFor returns the continuation an author's session fact belongs to
// or refers to: the author's own if it has one, otherwise its nearest ancestor's;
// "" when the author does not descend from the session's author.
func continuationFor(acct *storage.AccountTx, session, author, root string) (string, error) {
	for a, n := author, 0; a != "" && n < 1024; n++ {
		if _, err := acct.Continuation(session, a); err == nil {
			return a, nil
		} else if !errors.Is(err, storage.ErrNotFound) {
			return "", err
		}
		if a == root {
			return "", nil
		}
		parent, _, err := acct.Lineage(a)
		if errors.Is(err, storage.ErrNotFound) {
			return "", nil
		} else if err != nil {
			return "", err
		}
		a = parent
	}
	return "", nil
}

// ordinalOwner resolves an ordinal named from a continuation's point of view: its
// own above its base, otherwise its parent's (ErrNotFound when no such fact).
func ordinalOwner(acct *storage.AccountTx, session, author string, ordinal int64) (string, string, error) {
	for n := 0; author != "" && n < 1024; n++ {
		c, err := acct.Continuation(session, author)
		if err != nil {
			return "", "", err
		}
		if ordinal > c.Base || c.Parent == "" {
			node, err := acct.SessionFact(session, author, ordinal)
			return author, node, err
		}
		author = c.Parent
	}
	return "", "", storage.ErrNotFound
}

// newContinuation creates a descendant author's continuation of a session: its
// parent is the nearest ancestor holding one, and its base the greatest ordinal
// the two share in the parent's writing history, up to the last shared fact of
// the lineage step that reaches the parent. That history is fixed at the
// divergence, whatever either library pulls later (PROTOCOL §6.10, as in the
// independent reference: not the whole causal past, which can include facts the
// copy pulled from the original after diverging).
func newContinuation(acct *storage.AccountTx, session, author, root, startNode string) (storage.ContinuationRow, error) {
	parent, base, err := branchOf(acct, session, author)
	if err != nil {
		return storage.ContinuationRow{}, err
	}
	if err := acct.AddContinuation(session, author, parent, base); err != nil {
		return storage.ContinuationRow{}, err
	}
	return acct.Continuation(session, author)
}

// branchOf is the continuation a descendant author without one of its own
// would get (newContinuation), read without writing: its parent and base.
func branchOf(acct *storage.AccountTx, session, author string) (string, int64, error) {
	a, tip := author, ""
	for n := 0; n < 1024; n++ {
		parent, t, err := acct.Lineage(a)
		if err != nil {
			return "", 0, err
		}
		if _, err := acct.Continuation(session, parent); err == nil {
			a, tip = parent, t
			break
		} else if !errors.Is(err, storage.ErrNotFound) {
			return "", 0, err
		}
		a = parent
	}
	parent := a
	pc, err := acct.Continuation(session, parent)
	if err != nil {
		return "", 0, err
	}
	base := pc.Base
	if tip != "" {
		fa, placed, err := acct.FactAuthorOf(tip)
		if err != nil {
			return "", 0, err
		}
		if placed && fa.EffectiveAuthor == parent {
			if o, ok, err := acct.MaxOrdinalUpTo(session, parent, fa.AuthorSeq); err != nil {
				return "", 0, err
			} else if ok && o > base {
				base = o
			}
		}
	}
	return parent, base, nil
}

// sharedOrdinal returns the greatest ordinal of a continuation that an author
// descending from it holds in its own writing history: the lineage step that
// reaches the continuation's author shares that author's facts up to its tip. A
// copied finalization can name only such an ordinal (as in the
// independent reference: it resolves in the finalizer's own history).
func sharedOrdinal(acct *storage.AccountTx, session, author, target string) (int64, error) {
	tc, err := acct.Continuation(session, target)
	if err != nil {
		return 0, err
	}
	a, tip := author, ""
	for n := 0; n < 1024 && a != target; n++ {
		parent, t, err := acct.Lineage(a)
		if err != nil {
			return 0, err
		}
		a, tip = parent, t
	}
	if a != target || tip == "" {
		return tc.Base, nil
	}
	fa, placed, err := acct.FactAuthorOf(tip)
	if err != nil || !placed {
		return tc.Base, err
	}
	if o, ok, err := acct.MaxOrdinalUpTo(session, target, fa.AuthorSeq); err != nil {
		return 0, err
	} else if ok && o > tc.Base {
		return o, nil
	}
	return tc.Base, nil
}
