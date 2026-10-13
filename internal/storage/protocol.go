package storage

import (
	"context"
	"database/sql"
	"errors"
)

// ---- transport

// ConsumedSeq returns the installation's transport watermark.
func (a *AccountTx) ConsumedSeq(installID string) (int64, error) {
	var n int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT consumed_seq FROM installations WHERE account_id = ? AND install_id = ?`, a.id, installID).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return n, err
}

// SlotRow is a consumed transport slot.
type SlotRow struct {
	InstallID   string
	InstallSeq  int64
	OpID        string
	ContentHash string
	Result      string
	Seq         int64
}

// Slot returns a consumed slot.
func (a *AccountTx) Slot(installID string, installSeq int64) (SlotRow, error) {
	r := SlotRow{InstallID: installID, InstallSeq: installSeq}
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT op_id, content_hash, result, seq FROM transport_slots
		WHERE account_id = ? AND install_id = ? AND install_seq = ?`, a.id, installID, installSeq).Scan(&r.OpID, &r.ContentHash, &r.Result, &r.Seq)
	if errors.Is(err, sql.ErrNoRows) {
		return SlotRow{}, ErrNotFound
	}
	return r, err
}

// ConsumeSlot records a consumed slot and advances the installation's watermark
// to it (the caller guarantees it is the next slot).
func (a *AccountTx) ConsumeSlot(r SlotRow) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO transport_slots (account_id, install_id, install_seq, op_id, content_hash, result, seq)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, a.id, r.InstallID, r.InstallSeq, r.OpID, r.ContentHash, r.Result, r.Seq); err != nil {
		return err
	}
	return a.execOne(`UPDATE installations SET consumed_seq = ? WHERE account_id = ? AND install_id = ? AND consumed_seq = ?`,
		r.InstallSeq, a.id, r.InstallID, r.InstallSeq-1)
}

// WatermarkAt returns the installation's consumed watermark as of commit seq.
func (a *AccountReader) WatermarkAt(installID string, seq int64) (int64, error) {
	var n int64
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT COALESCE(MAX(install_seq), 0) FROM transport_slots
		WHERE account_id = ? AND install_id = ? AND seq <= ?`, a.id, installID, seq).Scan(&n)
	return n, err
}

// PartRow is one stored part of a framed slot.
type PartRow struct {
	Index         int
	TransactionID string
	Count         int
	ContentHash   string
	Data          []byte // as stored (sealed)
	Size          int    // the plaintext length (StorePart; -1 unknown)
}

// Parts returns the stored parts of a slot by index.
func (a *AccountTx) Parts(installID string, installSeq int64) ([]PartRow, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT part_index, transaction_id, part_count, content_hash, data, size FROM push_parts
		WHERE account_id = ? AND install_id = ? AND install_seq = ? ORDER BY part_index`, a.id, installID, installSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartRow
	for rows.Next() {
		var p PartRow
		if err := rows.Scan(&p.Index, &p.TransactionID, &p.Count, &p.ContentHash, &p.Data, &p.Size); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// StorePart stores one part (the caller checked it does not conflict).
func (a *AccountTx) StorePart(installID string, installSeq int64, p PartRow) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO push_parts (account_id, install_id, install_seq, part_index, transaction_id, part_count, content_hash, data,
		created_at, size) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`, a.id, installID, installSeq, p.Index, p.TransactionID, p.Count,
		p.ContentHash, p.Data, a.w.nowMillis(), p.Size)
	return err
}

// DeleteParts removes a slot's parts once it is consumed.
func (a *AccountTx) DeleteParts(installID string, installSeq int64) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM push_parts WHERE account_id = ? AND install_id = ? AND install_seq = ?`, a.id, installID, installSeq)
	return err
}

// ---- archive and causal graph

// FactRow is an archived fact.
type FactRow struct {
	OpID        string
	ContentHash string
	Fact        []byte
	Receipt     []byte
	Type        string
	TypeVersion int64
	Outcome     string
	Frontier    string // canonical JSON array
	Epoch, Seq  int64
	// ReceiptIndex is the receipt's index in its commit (-1 for a receipt
	// without one): receipt order is (epoch, seq, index, op_id).
	ReceiptIndex int64
	// PubEpoch and PubSeq are the publication position: the commit that
	// published the fact in the current log (PROTOCOL §6.11, §12). Zero means
	// the receipt's own (epoch, seq), as for every live acceptance.
	PubEpoch, PubSeq int64
	// NodePubEpoch and NodePubSeq are the commit from which the fact is a
	// causal node (zero: its publication position).
	NodePubEpoch, NodePubSeq int64
}

const factColumns = `op_id, content_hash, fact, receipt, type, type_version, outcome, frontier, epoch, seq, receipt_index, pub_epoch, pub_seq,
	node_pub_epoch, node_pub_seq`

func scanFact(row interface{ Scan(...any) error }) (FactRow, error) {
	var f FactRow
	err := row.Scan(&f.OpID, &f.ContentHash, &f.Fact, &f.Receipt, &f.Type, &f.TypeVersion, &f.Outcome, &f.Frontier, &f.Epoch, &f.Seq,
		&f.ReceiptIndex, &f.PubEpoch, &f.PubSeq, &f.NodePubEpoch, &f.NodePubSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return FactRow{}, ErrNotFound
	}
	return f, err
}

// Fact returns an archived fact.
func (a *AccountTx) Fact(opID string) (FactRow, error) {
	return scanFact(a.w.tx.QueryRowContext(a.w.ctx, `SELECT `+factColumns+` FROM facts WHERE account_id = ? AND op_id = ?`, a.id, opID))
}

// InsertFact archives a fact.
func (a *AccountTx) InsertFact(f FactRow) error {
	if f.PubEpoch == 0 && f.PubSeq == 0 {
		f.PubEpoch, f.PubSeq = f.Epoch, f.Seq
	}
	if f.NodePubEpoch == 0 && f.NodePubSeq == 0 {
		f.NodePubEpoch, f.NodePubSeq = f.PubEpoch, f.PubSeq
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO facts (account_id, `+factColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.id, f.OpID, f.ContentHash, f.Fact, f.Receipt, f.Type, f.TypeVersion, f.Outcome, f.Frontier, f.Epoch, f.Seq, f.ReceiptIndex, f.PubEpoch, f.PubSeq,
		f.NodePubEpoch, f.NodePubSeq)
	return err
}

// NodeTime returns a causal node's normalized time.
func (a *AccountTx) NodeTime(nodeID string) (int64, error) {
	var t int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT normalized_time FROM causal_nodes WHERE account_id = ? AND node_id = ?`, a.id, nodeID).Scan(&t)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return t, err
}

// InsertNode adds a causal node with its edges (its resolved frontier).
func (a *AccountTx) InsertNode(nodeID, kind string, normalizedTime int64, dependencies []string) error {
	return a.insertNode(nodeID, kind, normalizedTime, dependencies, true)
}

// InsertImplicitNode inserts a node that stays off the account's causal
// frontier and leaves its dependencies on it: a registration's own
// podcast.resolution and podcast.alias, which shared vectors leave implicit
// (docs/CONTRACT.md "Recovery"), so a layout's frontier is what every
// implementation computes.
func (a *AccountTx) InsertImplicitNode(nodeID, kind string, normalizedTime int64, dependencies []string) error {
	return a.insertNode(nodeID, kind, normalizedTime, dependencies, false)
}

func (a *AccountTx) insertNode(nodeID, kind string, normalizedTime int64, dependencies []string, frontier bool) error {
	if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO causal_nodes (account_id, node_id, kind, normalized_time) VALUES (?, ?, ?, ?)`,
		a.id, nodeID, kind, normalizedTime); err != nil {
		return err
	}
	for _, d := range dependencies {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO causal_edges (account_id, node_id, dependency_id) VALUES (?, ?, ?)`, a.id, nodeID, d); err != nil {
			return err
		}
		if !frontier {
			continue
		}
		if _, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM causal_frontier WHERE account_id = ? AND node_id = ?`, a.id, d); err != nil {
			return err
		}
	}
	// A node's dependencies are nodes before it, so a node no later node
	// depends on stays on the frontier.
	if frontier {
		if _, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO causal_frontier (account_id, node_id) VALUES (?, ?)`, a.id, nodeID); err != nil {
			return err
		}
	}
	return a.insertRank(nodeID, normalizedTime, dependencies)
}

// AuthorPosition returns the admitted fact holding (author, seq), or "".
func (a *AccountTx) AuthorPosition(authorID string, authorSeq int64) (string, error) {
	var op string
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT op_id FROM author_positions WHERE account_id = ? AND author_id = ? AND author_seq = ?`,
		a.id, authorID, authorSeq).Scan(&op)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return op, err
}

// AuthorPositionOf returns the (author, seq) of an admitted fact, if any.
func (a *AccountTx) AuthorPositionOf(opID string) (string, int64, bool, error) {
	var author string
	var seq int64
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT author_id, author_seq FROM author_positions WHERE account_id = ? AND op_id = ?`, a.id, opID).Scan(&author, &seq)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	return author, seq, err == nil, err
}

// InsertAuthorPosition records an admitted fact's author position.
func (a *AccountTx) InsertAuthorPosition(authorID string, authorSeq int64, opID string) error {
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO author_positions (account_id, author_id, author_seq, op_id) VALUES (?, ?, ?, ?)`,
		a.id, authorID, authorSeq, opID)
	return err
}

// DecisionRow is a stored server decision.
type DecisionRow struct {
	DecisionID     string
	DecisionType   string
	Subject        string
	Epoch, Seq     int64
	NormalizedTime int64
	Decision       []byte
	// PubEpoch and PubSeq are the publication position (see FactRow); zero
	// means the decision's own (epoch, seq).
	PubEpoch, PubSeq int64
}

// InsertDecision stores a decision (its causal node must exist).
func (a *AccountTx) InsertDecision(d DecisionRow) error {
	if d.PubEpoch == 0 && d.PubSeq == 0 {
		d.PubEpoch, d.PubSeq = d.Epoch, d.Seq
	}
	_, err := a.w.tx.ExecContext(a.w.ctx, `INSERT INTO decisions (account_id, decision_id, decision_type, subject, epoch, seq, normalized_time, decision,
		pub_epoch, pub_seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, a.id, d.DecisionID, d.DecisionType, d.Subject, d.Epoch, d.Seq, d.NormalizedTime,
		d.Decision, d.PubEpoch, d.PubSeq)
	return err
}

// IsNodeOrFact reports whether id is a causal node or an archived fact.
func (a *AccountTx) IsNodeOrFact(id string) (bool, error) {
	var n int
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT (SELECT COUNT(*) FROM causal_nodes WHERE account_id = ?1 AND node_id = ?2)
		+ (SELECT COUNT(*) FROM facts WHERE account_id = ?1 AND op_id = ?2)`, a.id, id).Scan(&n)
	return n > 0, err
}

// ---- reads for pull and snapshot

// Node is a causal node with its edges.
type Node struct {
	ID             string
	Kind           string
	NormalizedTime int64
	Dependencies   []string
}

// Nodes returns every causal node of the account with its edges.
func (a *AccountReader) Nodes() ([]Node, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT node_id, kind, normalized_time FROM causal_nodes WHERE account_id = ? ORDER BY node_id`, a.id)
	if err != nil {
		return nil, err
	}
	var nodes []Node
	index := map[string]int{}
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.Kind, &n.NormalizedTime); err != nil {
			rows.Close()
			return nil, err
		}
		index[n.ID] = len(nodes)
		nodes = append(nodes, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	edges, err := a.r.tx.QueryContext(a.r.ctx, `SELECT node_id, dependency_id FROM causal_edges WHERE account_id = ? ORDER BY node_id, dependency_id`, a.id)
	if err != nil {
		return nil, err
	}
	defer edges.Close()
	for edges.Next() {
		var n, d string
		if err := edges.Scan(&n, &d); err != nil {
			return nil, err
		}
		if i, ok := index[n]; ok {
			nodes[i].Dependencies = append(nodes[i].Dependencies, d)
		}
	}
	return nodes, edges.Err()
}

// FactBytes returns an archived fact's canonical bytes and receipt.
func (a *AccountReader) FactBytes(opID string) (fact, receipt []byte, err error) {
	err = a.r.tx.QueryRowContext(a.r.ctx, `SELECT fact, receipt FROM facts WHERE account_id = ? AND op_id = ?`, a.id, opID).Scan(&fact, &receipt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	return fact, receipt, err
}

// DecisionBytes returns a decision's canonical bytes.
func (a *AccountTx) DecisionBytes(decisionID string) ([]byte, error) {
	var d []byte
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT decision FROM decisions WHERE account_id = ? AND decision_id = ?`, a.id, decisionID).Scan(&d)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// DecisionBytes returns a decision's canonical bytes.
func (a *AccountReader) DecisionBytes(decisionID string) ([]byte, error) {
	var d []byte
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT decision FROM decisions WHERE account_id = ? AND decision_id = ?`, a.id, decisionID).Scan(&d)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// RejectedFacts returns rejected facts ordered by (epoch, seq, op_id).
func (a *AccountReader) RejectedFacts() ([]FactRow, error) {
	rows, err := a.r.tx.QueryContext(a.r.ctx, `SELECT `+factColumns+` FROM facts WHERE account_id = ? AND outcome = 'rejected' ORDER BY epoch, seq, op_id`, a.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FactRow
	for rows.Next() {
		f, err := scanFact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// CommitDigest returns the digest of commit seq (seq 0 is the genesis digest).
func (a *AccountTx) CommitDigest(seq int64) ([]byte, error) {
	return commitDigest(a.w.ctx, a.w.tx, a.id, seq)
}

// CommitDigest returns the digest of commit seq in this snapshot.
func (a *AccountReader) CommitDigest(seq int64) ([]byte, error) {
	return commitDigest(a.r.ctx, a.r.tx, a.id, seq)
}

func commitDigest(ctx context.Context, q rowQuerier, accountID string, seq int64) ([]byte, error) {
	if seq == 0 {
		return make([]byte, 32), nil
	}
	var d []byte
	err := q.QueryRowContext(ctx, `SELECT digest FROM log_commits WHERE account_id = ? AND seq = ?`, accountID, seq).Scan(&d)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// Installation returns one of the account's installations in a read snapshot.
func (a *AccountReader) Installation(installID string) (Installation, error) {
	return scanInstallation(a.r.tx.QueryRowContext(a.r.ctx, `SELECT `+installationColumns+` FROM installations WHERE account_id = ? AND install_id = ?`,
		a.id, installID))
}

// ConsumedSeq returns the installation's current transport watermark.
func (a *AccountReader) ConsumedSeq(installID string) (int64, error) {
	var n int64
	err := a.r.tx.QueryRowContext(a.r.ctx, `SELECT consumed_seq FROM installations WHERE account_id = ? AND install_id = ?`, a.id, installID).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return n, err
}

// PartMeta describes a stored part without its data.
type PartMeta struct {
	Index         int
	TransactionID string
	Count         int
	ContentHash   string
	Size          int
}

// PartsMeta returns the stored parts of a slot without loading their data;
// Size is a part's plaintext length (framing limits count plaintext, not the
// sealed bytes).
func (a *AccountTx) PartsMeta(installID string, installSeq int64) ([]PartMeta, error) {
	rows, err := a.w.tx.QueryContext(a.w.ctx, `SELECT part_index, transaction_id, part_count, content_hash,
		CASE WHEN size >= 0 THEN size ELSE length(data) END FROM push_parts
		WHERE account_id = ? AND install_id = ? AND install_seq = ? ORDER BY part_index`, a.id, installID, installSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartMeta
	for rows.Next() {
		var p PartMeta
		if err := rows.Scan(&p.Index, &p.TransactionID, &p.Count, &p.ContentHash, &p.Size); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// PartData returns one stored part's data.
func (a *AccountTx) PartData(installID string, installSeq int64, index int) ([]byte, error) {
	var d []byte
	err := a.w.tx.QueryRowContext(a.w.ctx, `SELECT data FROM push_parts WHERE account_id = ? AND install_id = ? AND install_seq = ? AND part_index = ?`,
		a.id, installID, installSeq, index).Scan(&d)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}
