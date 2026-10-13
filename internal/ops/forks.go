package ops

import (
	"slices"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// installation.fork (PROTOCOL §6.10): written by a copy that detected itself
// locally, as its new author's first fact. It records the new author's lineage:
// the effective author of the copied writing tip.
type forkOp struct{}

func (forkOp) Capability() string { return "" }

func (forkOp) Validate(f *protocol.Fact) error {
	if err := fields(f.Payload, []string{"author", "head"}); err != nil {
		return err
	}
	author, err := uuidField(f.Payload, "author")
	if err != nil {
		return err
	}
	head, err := uuidField(f.Payload, "head")
	if err != nil {
		return err
	}
	switch {
	case author == f.AuthorID:
		return invalidf("a fork's author must differ from the author it copies")
	case f.PreviousOpID != "":
		return invalidf("a fork is its author's first fact")
	case !slices.Contains(f.Dependencies, head):
		return invalidf("a fork depends on its head")
	case len(f.Targets) != 0:
		return invalidf("a fork has no targets")
	}
	return nil
}

// ValidateState: the head is a fact of the copied author, and the fork's author
// has written nothing else.
func (forkOp) ValidateState(acct *storage.AccountTx, _ protocol.AccountInfo, f *protocol.Fact) error {
	author, _ := str(f.Payload, "author")
	head, _ := str(f.Payload, "head")
	fa, placed, err := acct.FactAuthorOf(head)
	if err != nil {
		return err
	}
	if !placed || fa.RawAuthor != author {
		return protocol.ErrInvalidPayload
	}
	if n, err := acct.OtherFactsOf(f.AuthorID, f.OpID); err != nil {
		return err
	} else if n > 0 {
		return protocol.ErrInvalidPayload
	}
	return nil
}

func (forkOp) Apply(ctx *protocol.ApplyContext, f *protocol.Fact) (protocol.Outcome, error) {
	head, _ := str(f.Payload, "head")
	fa, _, err := ctx.Account.FactAuthorOf(head)
	if err != nil {
		return protocol.Outcome{}, err
	}
	if err := ctx.Account.SetLineage(ctx.Author, fa.EffectiveAuthor, f.OpID, head); err != nil {
		return protocol.Outcome{}, err
	}
	return protocol.Outcome{Result: "applied"}, nil
}
