package protocol

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

var errBadToken = protoErr(400, "invalid_request", "The page token is not valid.")

// PullRequest is a decoded pull.
type PullRequest struct {
	AccountID       string
	Cursor          contract.Cursor
	InstallID       string
	AckedInstallSeq int64
	Page            string
	Limit           int
	// AuthorizeRead checks the caller again in the read that serves the
	// page (nil: none): a revocation after authentication wins.
	AuthorizeRead AuthorizeRead
}

// PullResponse is one page.
type PullResponse struct {
	View             string `json:"-"`
	Head             string `json:"head"`
	From             string `json:"from"`
	HasMore          bool   `json:"has_more"`
	Next             string `json:"next,omitempty"`
	InstallWatermark int64  `json:"install_watermark"`
	Items            []any  `json:"items"`
}

// Pull serves one page of commits after the cursor up to a fixed head
// (PROTOCOL §6.6). It only reads.
func (s *Service) Pull(ctx context.Context, req PullRequest) (*PullResponse, error) {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	var resp *PullResponse
	err = s.Store.Read(ctx, func(r *storage.ReadTx) error {
		if err := callerRead(r, req.AuthorizeRead, req.InstallID); err != nil {
			return err
		}
		acct := r.Account(req.AccountID)
		consumed, err := acct.ConsumedSeq(req.InstallID)
		if err != nil {
			return err
		}
		if err := checkLineage(acct, req.Cursor, req.AckedInstallSeq, consumed); err != nil {
			return err
		}
		head, err := acct.Head()
		if err != nil {
			return err
		}
		headSeq, headDigest := head.Seq, head.Digest
		nextSeq, nextPart := req.Cursor.Seq+1, int64(0)
		limit := req.Limit
		if limit <= 0 || limit > DefaultPageCommits {
			limit = DefaultPageCommits
		}
		if req.Page != "" {
			f, ok := openToken(keys, req.Page)
			if !ok || tokenString(f, "kind") != "pull" || tokenString(f, "account") != req.AccountID ||
				tokenString(f, "install") != req.InstallID {
				return errBadToken
			}
			from, _ := tokenInt(f, "from")
			if tokenString(f, "log") != head.LogID {
				return errLineageMismatch
			}
			if epoch, _ := tokenInt(f, "epoch"); epoch != head.Epoch {
				return errLineageMismatch
			}
			if from != req.Cursor.Seq {
				return errBadToken
			}
			headSeq, _ = tokenInt(f, "head_seq")
			headDigest, _ = hex.DecodeString(tokenString(f, "head_digest"))
			nextSeq, _ = tokenInt(f, "next_seq")
			nextPart, _ = tokenInt(f, "next_part")
			if l, _ := tokenInt(f, "limit"); l > 0 {
				limit = int(l)
			}
			// The fixed head must still be in this lineage (commits are immutable).
			d, err := acct.CommitDigest(headSeq)
			if err != nil || hex.EncodeToString(d) != hex.EncodeToString(headDigest) {
				return errLineageMismatch
			}
		}
		headCursor, err := contract.Cursor{LogID: head.LogID, Epoch: head.Epoch, Seq: headSeq, Digest: headDigest}.Encode()
		if err != nil {
			return err
		}
		fromCursor, err := req.Cursor.Encode()
		if err != nil {
			return err
		}
		watermark, err := acct.WatermarkAt(req.InstallID, headSeq)
		if err != nil {
			return err
		}
		resp = &PullResponse{Head: headCursor, From: fromCursor, InstallWatermark: watermark, Items: []any{}}
		budget := PageBytes
		commits := 0
		for nextSeq <= headSeq && commits < limit {
			page, err := acct.Commits(nextSeq-1, nextSeq, 1)
			if err != nil {
				return err
			}
			if len(page) != 1 {
				return errLineageMismatch
			}
			c := page[0].Canonical
			if nextPart == 0 && len(c) <= PageBytes {
				if len(c) > budget && len(resp.Items) > 0 {
					break
				}
				resp.Items = append(resp.Items, map[string]any{"commit": json.RawMessage(c)})
				budget -= len(c)
				nextSeq++
				commits++
				continue
			}
			// A commit larger than a page travels as parts, one per page.
			if len(resp.Items) > 0 {
				break
			}
			count := (len(c) + PageBytes - 1) / PageBytes
			start := int(nextPart) * PageBytes
			end := min(start+PageBytes, len(c))
			resp.Items = append(resp.Items, map[string]any{"commit_part": map[string]any{"seq": nextSeq, "part_index": nextPart,
				"part_count": count, "data": base64.RawURLEncoding.EncodeToString(c[start:end])}})
			nextPart++
			if int(nextPart) == count {
				nextSeq, nextPart = nextSeq+1, 0
				commits++
			}
			break
		}
		if nextSeq <= headSeq {
			resp.HasMore = true
			resp.Next, err = sealToken(keys[0], map[string]any{"kind": "pull", "account": req.AccountID, "install": req.InstallID,
				"log": head.LogID, "epoch": head.Epoch, "from": req.Cursor.Seq, "head_seq": headSeq,
				"head_digest": hex.EncodeToString(headDigest), "next_seq": nextSeq, "next_part": nextPart, "limit": int64(limit)})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
