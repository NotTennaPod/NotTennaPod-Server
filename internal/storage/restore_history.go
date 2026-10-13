package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// The signed history behind the backup MAC. Besides every receipt's and decision's MAC
// (verifyMACs), a restore checks that the stored copies agree with what was signed:
// every superseded receipt verifies and belongs to its fact; every fact's and
// decision's stored position, rank, subject and time are the signed ones; and every
// account's log is a contiguous chain of canonical commits whose digests and head are
// what the stored bytes give.

// signedInt reads an integer member of a signed object.
func signedInt(obj map[string]any, key string) (int64, bool) {
	n, ok := obj[key].(json.Number)
	if !ok {
		return 0, false
	}
	v, err := n.Int64()
	return v, err == nil
}

func decodeSigned(data []byte) (map[string]any, bool) {
	v, err := contract.DecodeJSON(data)
	if err != nil {
		return nil, false
	}
	obj, ok := v.(map[string]any)
	return obj, ok
}

// receiptPlaced reports a receipt whose op ID and position (epoch, seq,
// index; -1 for a receipt without an index) are the stored ones.
func receiptPlaced(obj map[string]any, opID string, epoch, seq, index int64) bool {
	e, eok := signedInt(obj, "epoch")
	s, sok := signedInt(obj, "seq")
	i, iok := signedInt(obj, "index")
	if !iok {
		i = -1
	}
	return obj["op_id"] == opID && eok && sok && e == epoch && s == seq && i == index
}

func verifyHistory(ctx context.Context, q rowQuerier, keyByID map[string][]byte) error {
	if err := verifyFactPositions(ctx, q); err != nil {
		return err
	}
	if err := verifySuperseded(ctx, q, keyByID); err != nil {
		return err
	}
	if err := verifyDecisionRows(ctx, q); err != nil {
		return err
	}
	return verifyLogs(ctx, q)
}

func verifyFactPositions(ctx context.Context, q rowQuerier) error {
	rows, err := q.QueryContext(ctx, `SELECT account_id, op_id, receipt, epoch, seq, receipt_index FROM facts`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var account, id string
		var receipt []byte
		var epoch, seq, index int64
		if err := rows.Scan(&account, &id, &receipt, &epoch, &seq, &index); err != nil {
			return err
		}
		obj, ok := decodeSigned(receipt)
		if !ok || !receiptPlaced(obj, id, epoch, seq, index) {
			return fmt.Errorf("the fact %s in account %s is stored at a position its signed receipt does not give", id, account)
		}
	}
	return rows.Err()
}

func verifySuperseded(ctx context.Context, q rowQuerier, keyByID map[string][]byte) error {
	rows, err := q.QueryContext(ctx, `SELECT s.account_id, s.op_id, s.epoch, s.seq, s.receipt_index, s.receipt, f.content_hash
		FROM superseded_receipts s LEFT JOIN facts f ON f.account_id = s.account_id AND f.op_id = s.op_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var account, id string
		var epoch, seq, index int64
		var receipt []byte
		var hash *string
		if err := rows.Scan(&account, &id, &epoch, &seq, &index, &receipt, &hash); err != nil {
			return err
		}
		obj, ok := decodeSigned(receipt)
		switch {
		case !verifySigned(keyByID, account, receipt):
			return fmt.Errorf("a superseded receipt of %s in account %s does not verify with the key bundle's receipt keys", id, account)
		case hash == nil:
			return fmt.Errorf("a superseded receipt of %s in account %s has no fact", id, account)
		case !ok || obj["content_hash"] != *hash || !receiptPlaced(obj, id, epoch, seq, index):
			return fmt.Errorf("a superseded receipt of %s in account %s does not match its fact or its stored position", id, account)
		}
	}
	return rows.Err()
}

func verifyDecisionRows(ctx context.Context, q rowQuerier) error {
	rows, err := q.QueryContext(ctx, `SELECT account_id, decision_id, decision_type, subject, epoch, seq, normalized_time, decision FROM decisions`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var account, id, typ, subject string
		var epoch, seq, normalized int64
		var decision []byte
		if err := rows.Scan(&account, &id, &typ, &subject, &epoch, &seq, &normalized, &decision); err != nil {
			return err
		}
		obj, ok := decodeSigned(decision)
		e, eok := signedInt(obj, "epoch")
		s, sok := signedInt(obj, "seq")
		n, nok := signedInt(obj, "normalized_time")
		if !ok || obj["decision_id"] != id || obj["decision_type"] != typ || obj["subject"] != subject ||
			!eok || !sok || !nok || e != epoch || s != seq || n != normalized {
			return fmt.Errorf("the decision %s in account %s is stored with a rank, subject or time it was not signed with", id, account)
		}
	}
	return rows.Err()
}

// verifyLogs checks every account's log: seqs 1 to the head, each commit's
// stored bytes canonical with its own epoch and seq, each digest
// SHA-256(previous || bytes), and the head the last digest.
func verifyLogs(ctx context.Context, q rowQuerier) error {
	heads, err := q.QueryContext(ctx, `SELECT account_id, head_seq, head_digest FROM account_logs ORDER BY account_id`)
	if err != nil {
		return err
	}
	type head struct {
		seq    int64
		digest []byte
	}
	want := map[string]head{}
	for heads.Next() {
		var id string
		var h head
		if err := heads.Scan(&id, &h.seq, &h.digest); err != nil {
			heads.Close()
			return err
		}
		want[id] = h
	}
	heads.Close()
	if err := heads.Err(); err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, `SELECT account_id, seq, epoch, digest, commit_json FROM log_commits ORDER BY account_id, seq`)
	if err != nil {
		return err
	}
	defer rows.Close()
	got := map[string]head{}
	broken := func(account string, seq int64, why string) error {
		return fmt.Errorf("the log of account %s is broken at seq %d: %s", account, seq, why)
	}
	for rows.Next() {
		var id string
		var seq, epoch int64
		var digest, body []byte
		if err := rows.Scan(&id, &seq, &epoch, &digest, &body); err != nil {
			return err
		}
		prev, ok := got[id]
		if !ok {
			prev = head{digest: contract.GenesisDigest}
		}
		if seq != prev.seq+1 {
			return broken(id, seq, fmt.Sprintf("it follows seq %d", prev.seq))
		}
		if err := checkCanonical(body); err != nil {
			return broken(id, seq, err.Error())
		}
		obj, _ := decodeSigned(body)
		if e, ok := signedInt(obj, "epoch"); !ok || e != epoch {
			return broken(id, seq, "the commit's epoch is not the stored one")
		}
		if s, ok := signedInt(obj, "seq"); !ok || s != seq {
			return broken(id, seq, "the commit's seq is not the stored one")
		}
		d, err := contract.ChainDigest(prev.digest, body)
		if err != nil {
			return err
		}
		if !bytes.Equal(d, digest) {
			return broken(id, seq, "the stored digest is not the one the commit's bytes give")
		}
		got[id] = head{seq: seq, digest: d}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for id, h := range want {
		g, ok := got[id]
		if !ok {
			g = head{digest: contract.GenesisDigest}
		}
		if g.seq != h.seq || !bytes.Equal(g.digest, h.digest) {
			return fmt.Errorf("the log of account %s does not end at its head (seq %d)", id, h.seq)
		}
	}
	if len(got) > len(want) {
		return fmt.Errorf("the backup holds log commits of an account without a log")
	}
	return nil
}
