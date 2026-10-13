package protocol

import (
	"bufio"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/backupsched"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// ViewLifetime is how long an unused snapshot view is kept.
const ViewLifetime = 24 * time.Hour

// SupersededViewIdle is how long an installation's older view of a mode is
// kept unread once it has a newer one.
const SupersededViewIdle = 10 * time.Minute

var errViewExpired = protoErr(410, "view_expired", "This snapshot expired; start it again.")

var errViewSpace = &Error{Status: 503, Code: "server_busy", Message: "The server is short of disk space for snapshots; try again later.",
	RetryAfter: time.Minute}

// ViewRegistry tracks snapshot views being built or read, so cleanup never removes them (PROTOCOL
// §6.7). It also knows every published view, so an equivalent request reuses one and a newer view
// supersedes older ones.
type ViewRegistry struct {
	dir      string
	mu       sync.Mutex
	building map[string]bool
	builds   map[string]*viewBuild // equivalent requests' shared builds
	pinned   map[string]int
	offsets  map[string][]int64
	views    map[string]viewMeta // published views by ID
	order    map[string]uint64   // their publication order (views kept from a previous run first)
	counter  uint64
	now      func() time.Time
	// FreeBytes and Headroom bound view storage like the backups' check:
	// a view is built only while the views' disk keeps Headroom free.
	FreeBytes func(dir string) (int64, error)
	Headroom  func() int64
}

// NewViewRegistry prepares dir (a real, private folder: private.RealDir) and
// removes orphaned .tmp files, which can only exist from a previous run: call
// before serving.
func NewViewRegistry(dir string, now func() time.Time) (*ViewRegistry, error) {
	if now == nil {
		now = time.Now
	}
	if err := private.RealDir(dir); err != nil {
		return nil, err
	}
	dataDir := filepath.Dir(dir)
	v := &ViewRegistry{dir: dir, building: map[string]bool{}, builds: map[string]*viewBuild{}, pinned: map[string]int{},
		offsets: map[string][]int64{},
		views:   map[string]viewMeta{}, order: map[string]uint64{}, now: now, FreeBytes: backupsched.FreeBytes,
		Headroom: func() int64 { return backupsched.Headroom(dataDir) }}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
	}
	for name := range names {
		// Orphans of an interrupted build or publication: temporary files, and
		// data files whose metadata (renamed last) never appeared.
		if strings.HasSuffix(name, ".tmp") {
			_ = os.Remove(filepath.Join(dir, name))
		} else if id, ok := strings.CutSuffix(name, ".jsonl"); ok && !names[id+".json"] {
			_ = os.Remove(filepath.Join(dir, name))
		} else if id, ok := strings.CutSuffix(name, ".json"); ok {
			if m, err := readMeta(filepath.Join(dir, name)); err == nil {
				v.views[id] = m
			}
		}
	}
	loaded := slices.Collect(maps.Keys(v.views))
	slices.SortFunc(loaded, func(a, b string) int {
		return cmp.Or(cmp.Compare(v.views[a].CreatedAt, v.views[b].CreatedAt), strings.Compare(a, b))
	})
	for _, id := range loaded {
		v.counter++
		v.order[id] = v.counter
	}
	return v, nil
}

type viewMeta struct {
	AccountID      string `json:"account_id"`
	LogID          string `json:"log_id"`
	Epoch          int64  `json:"epoch"`
	HeadSeq        int64  `json:"head_seq"`
	HeadDigest     string `json:"head_digest"`
	InstallID      string `json:"install_id"`
	ItemCount      int64  `json:"item_count"`
	SnapshotDigest string `json:"snapshot_digest"`
	CreatedAt      int64  `json:"created_at"`
	// Mode and FieldGroups, with the installation and head, make a view's
	// key: an equivalent request reuses the view.
	Mode        string `json:"mode,omitempty"`
	FieldGroups string `json:"field_groups,omitempty"`
}

// fieldGroupsKey is a request's field groups, sorted, as the view key holds them.
func fieldGroupsKey(groups []string) string {
	sorted := slices.Clone(groups)
	slices.Sort(sorted)
	b, _ := json.Marshal(sorted)
	return string(b)
}

func (v *ViewRegistry) paths(id string) (data, meta string) {
	return filepath.Join(v.dir, id+".jsonl"), filepath.Join(v.dir, id+".json")
}

// Sweep deletes views unused for ViewLifetime, views whose lineage is no
// longer current (current reports the account's current log and epoch) and
// superseded views unread for SupersededViewIdle, skipping views being built
// or read.
func (v *ViewRegistry) Sweep(current func(accountID string) (logID string, epoch int64, ok bool)) {
	entries, err := os.ReadDir(v.dir)
	if err != nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, e := range entries {
		id, isMeta := strings.CutSuffix(e.Name(), ".json")
		if !isMeta || v.building[id] || v.pinned[id] > 0 {
			continue
		}
		_, metaPath := v.paths(id)
		info, err := os.Stat(metaPath)
		if err != nil {
			continue
		}
		stale := v.now().Sub(info.ModTime()) > ViewLifetime
		if !stale && current != nil {
			if m, err := readMeta(metaPath); err == nil {
				logID, epoch, ok := current(m.AccountID)
				stale = !ok || logID != m.LogID || epoch != m.Epoch
			}
		}
		if stale {
			v.removeLocked(id)
		}
	}
	v.pruneSupersededLocked()
}

// removeLocked deletes a published view (v.mu held).
func (v *ViewRegistry) removeLocked(id string) {
	data, metaPath := v.paths(id)
	_ = os.Remove(metaPath)
	_ = os.Remove(data)
	delete(v.offsets, id)
	delete(v.views, id)
	delete(v.order, id)
}

// pruneSupersededLocked deletes an installation's older views of a mode,
// once it has a newer one and none of their pages has been read for
// SupersededViewIdle (v.mu held). Views being built or read stay.
func (v *ViewRegistry) pruneSupersededLocked() {
	type group struct{ account, install, mode string }
	newestID := map[group]string{}
	for id, m := range v.views {
		g := group{m.AccountID, m.InstallID, m.Mode}
		if n, ok := newestID[g]; !ok || v.order[id] > v.order[n] {
			newestID[g] = id
		}
	}
	for id, m := range v.views {
		g := group{m.AccountID, m.InstallID, m.Mode}
		if id == newestID[g] || v.building[id] || v.pinned[id] > 0 {
			continue
		}
		_, metaPath := v.paths(id)
		if v.now().Sub(modTime(metaPath)) >= SupersededViewIdle {
			v.removeLocked(id)
		}
	}
}

// find returns a published view equivalent to a request at head: the same
// account, installation, mode, head and field groups ("" if none).
func (v *ViewRegistry) find(req SnapshotRequest, head storage.Head) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.findLocked(req, head)
}

func (v *ViewRegistry) findLocked(req SnapshotRequest, head storage.Head) string {
	digest := hex.EncodeToString(head.Digest)
	groups := fieldGroupsKey(req.FieldGroups)
	for id, m := range v.views {
		if m.AccountID == req.AccountID && m.InstallID == req.InstallID && m.Mode == req.Mode && m.FieldGroups == groups &&
			m.LogID == head.LogID && m.Epoch == head.Epoch && m.HeadSeq == head.Seq && m.HeadDigest == digest && !v.building[id] {
			_, metaPath := v.paths(id)
			if v.now().Sub(modTime(metaPath)) <= ViewLifetime {
				return id
			}
		}
	}
	return ""
}

// viewSpaceStep is how much of a view is written between free-space checks.
const viewSpaceStep = 256 << 10

// viewBuild is a view being built for equivalent requests, which share it.
type viewBuild struct {
	done chan struct{}
	id   string
	err  error
}

// coalesce returns the view equivalent requests at head share: one already
// published, the one being built, or a new one build makes.
func (v *ViewRegistry) coalesce(ctx context.Context, req SnapshotRequest, head storage.Head, build func() (string, error)) (string, error) {
	key := strings.Join([]string{req.AccountID, req.InstallID, req.Mode, fieldGroupsKey(req.FieldGroups), head.LogID,
		strconv.FormatInt(head.Epoch, 10), strconv.FormatInt(head.Seq, 10), hex.EncodeToString(head.Digest)}, "\x00")
	v.mu.Lock()
	if id := v.findLocked(req, head); id != "" {
		v.mu.Unlock()
		return id, nil
	}
	if b := v.builds[key]; b != nil {
		v.mu.Unlock()
		select {
		case <-b.done:
			return b.id, b.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	b := &viewBuild{done: make(chan struct{})}
	v.builds[key] = b
	v.mu.Unlock()
	b.id, b.err = build()
	v.mu.Lock()
	delete(v.builds, key)
	v.mu.Unlock()
	close(b.done)
	return b.id, b.err
}

// checkSpace refuses a new view (server_busy) unless the views' disk keeps
// the backups' headroom free.
func (v *ViewRegistry) checkSpace() error {
	if v.FreeBytes == nil || v.Headroom == nil {
		return nil
	}
	free, err := v.FreeBytes(v.dir)
	if err != nil || free < v.Headroom() {
		return errViewSpace
	}
	return nil
}

func readMeta(path string) (viewMeta, error) {
	var m viewMeta
	b, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(b, &m)
	}
	return m, err
}

// SnapshotRequest is a decoded snapshot request.
type SnapshotRequest struct {
	AccountID       string
	Mode            string // attach, upgrade or recovery (PROTOCOL §6.11)
	RecoveryID      string // recovery: the recovery the snapshot opens or resumes
	Cursor          *contract.Cursor
	FieldGroups     []string
	InstallID       string
	AckedInstallSeq int64
	Page            string
	Limit           int
	// Authorize checks the caller again in the transaction that opens a
	// recovery (recovery snapshots; nil otherwise). AuthorizeRead checks it
	// in every read that builds or serves a view, of every mode (nil: none).
	Authorize     Authorize
	AuthorizeRead AuthorizeRead
}

// SnapshotResponse is one snapshot page.
type SnapshotResponse struct {
	Head             string         `json:"head"`
	HasMore          bool           `json:"has_more"`
	Next             string         `json:"next,omitempty"`
	InstallWatermark int64          `json:"install_watermark"`
	Items            []any          `json:"items"`
	Manifest         map[string]any `json:"manifest,omitempty"`
	// Prefix answers a recovery snapshot whose cursor names a commit the
	// restored log still has (PROTOCOL §6.11): Cursor re-issues it in the current
	// epoch and the device catches up by pulls.
	Prefix bool   `json:"prefix,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// Snapshot serves one page of a durable snapshot view (PROTOCOL §6.4).
func (s *Service) Snapshot(ctx context.Context, req SnapshotRequest) (*SnapshotResponse, error) {
	keys, err := s.Store.ReceiptKeys(ctx)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	viewID, index, part := "", int64(0), int64(0)
	limit := req.Limit
	if req.Mode == "recovery" && req.Page == "" {
		if prefix, err := s.openRecovery(ctx, req); err != nil || prefix != nil {
			return prefix, err
		}
	}
	if req.Page == "" {
		viewID, err = s.view(ctx, req)
		if err != nil {
			return nil, err
		}
	} else {
		// A page token serves only the mode (and recovery) it was issued
		// for: a recovery's page asked as an attach would skip its checks.
		f, ok := openToken(keys, req.Page)
		if !ok || tokenString(f, "kind") != "snapshot" || tokenString(f, "account") != req.AccountID || tokenString(f, "install") != req.InstallID ||
			tokenString(f, "mode") != req.Mode || tokenString(f, "recovery") != req.RecoveryID {
			return nil, errBadToken
		}
		viewID = tokenString(f, "view")
		index, _ = tokenInt(f, "next_index")
		part, _ = tokenInt(f, "next_part")
		if l, _ := tokenInt(f, "limit"); l > 0 {
			limit = int(l)
		}
	}
	if limit <= 0 || limit > DefaultPageCommits {
		limit = DefaultPageCommits
	}
	return s.Views.page(keys[0], req, viewID, index, part, limit, func(m viewMeta) (int64, error) {
		// A view of a lineage that is no longer current is expired; the
		// watermark is the installation's at the view's head.
		var watermark int64
		err := s.Store.Read(ctx, func(r *storage.ReadTx) error {
			// Every page is the account's content: the caller is checked
			// again in the read that serves it.
			if err := authorizedRead(r, req); err != nil {
				return err
			}
			acct := r.Account(req.AccountID)
			h, err := acct.Head()
			if err != nil {
				return err
			}
			if h.LogID != m.LogID || h.Epoch != m.Epoch || h.Seq < m.HeadSeq {
				return errViewExpired
			}
			// The view's head must still be in the log (a rolled-back database
			// with the same log and epoch would otherwise serve a foreign view).
			if d, err := acct.CommitDigest(m.HeadSeq); err != nil || hex.EncodeToString(d) != m.HeadDigest {
				return errViewExpired
			}
			watermark, err = acct.WatermarkAt(req.InstallID, m.HeadSeq)
			return err
		})
		return watermark, err
	})
}

// authorizedRead runs a snapshot's authorization in a read transaction
// (nil: none) and checks the installation.
func authorizedRead(r *storage.ReadTx, req SnapshotRequest) error {
	return callerRead(r, req.AuthorizeRead, req.InstallID)
}

// callerRead runs authorize in a read transaction (nil: none) and checks
// that the caller is installation installID.
func callerRead(r *storage.ReadTx, authorize AuthorizeRead, installID string) error {
	if authorize == nil {
		return nil
	}
	inst, err := authorize(r)
	if err != nil {
		return err
	}
	if inst.InstallID != installID {
		return protoErr(403, "forbidden", "install_id is not this credential's installation.")
	}
	return nil
}

// CurrentLineage reports an account's current log and epoch (for Sweep).
func (s *Service) CurrentLineage(ctx context.Context) func(accountID string) (string, int64, bool) {
	return func(accountID string) (string, int64, bool) {
		var logID string
		var epoch int64
		err := s.Store.Read(ctx, func(r *storage.ReadTx) error {
			h, err := r.Account(accountID).Head()
			logID, epoch = h.LogID, h.Epoch
			return err
		})
		return logID, epoch, err == nil
	}
}

// view returns a view for a first page: an equivalent one already published
// (its checks run again here), or a new one if the views' disk has room.
func (s *Service) view(ctx context.Context, req SnapshotRequest) (string, error) {
	var head storage.Head
	err := s.Store.Read(ctx, func(r *storage.ReadTx) error {
		if err := authorizedRead(r, req); err != nil {
			return err
		}
		acct := r.Account(req.AccountID)
		if req.Mode == "upgrade" {
			consumed, err := acct.ConsumedSeq(req.InstallID)
			if err != nil {
				return err
			}
			if err := checkLineage(acct, *req.Cursor, req.AckedInstallSeq, consumed); err != nil {
				return err
			}
		}
		var err error
		head, err = acct.Head()
		return err
	})
	if err != nil {
		return "", err
	}
	if id := s.Views.find(req, head); id != "" {
		return id, nil
	}
	if err := s.Views.checkSpace(); err != nil {
		return "", err
	}
	return s.Views.coalesce(ctx, req, head, func() (string, error) { return s.buildView(ctx, req) })
}

// buildView materializes the account's snapshot at its current head from one
// read transaction into a published view and returns its ID.
func (s *Service) buildView(ctx context.Context, req SnapshotRequest) (string, error) {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(u[:])
	v := s.Views
	v.mu.Lock()
	v.building[id] = true
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		delete(v.building, id)
		v.mu.Unlock()
	}()
	tmp := filepath.Join(v.dir, id+".tmp")
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp) // no-op once renamed
	w := bufio.NewWriterSize(file, 1<<20)
	meta := viewMeta{AccountID: req.AccountID, InstallID: req.InstallID, CreatedAt: s.Now().UnixMilli(), Mode: req.Mode,
		FieldGroups: fieldGroupsKey(req.FieldGroups)}
	digest := contract.GenesisDigest
	var count, written, checked int64
	emit := func(item map[string]any) error {
		c, err := contract.Canonical(item)
		if err != nil {
			return err
		}
		if digest, err = contract.ChainDigest(digest, c); err != nil {
			return err
		}
		count++
		if _, err := w.Write(c); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
		// Free space is checked as the view is written: below the reserve the
		// build stops.
		if written += int64(len(c)) + 1; written-checked >= viewSpaceStep {
			checked = written
			if err := w.Flush(); err != nil {
				return err
			}
			return v.checkSpace()
		}
		return nil
	}
	err = s.Store.Read(ctx, func(r *storage.ReadTx) error {
		if err := authorizedRead(r, req); err != nil {
			return err
		}
		acct := r.Account(req.AccountID)
		if req.Mode == "upgrade" {
			consumed, err := acct.ConsumedSeq(req.InstallID)
			if err != nil {
				return err
			}
			if err := checkLineage(acct, *req.Cursor, req.AckedInstallSeq, consumed); err != nil {
				return err
			}
		}
		head, err := acct.Head()
		if err != nil {
			return err
		}
		meta.LogID, meta.Epoch, meta.HeadSeq, meta.HeadDigest = head.LogID, head.Epoch, head.Seq, hex.EncodeToString(head.Digest)
		nodes, err := acct.Nodes()
		if err != nil {
			return err
		}
		ordered := make([]contract.OrderedFact, len(nodes))
		kinds := map[string]string{}
		for i, n := range nodes {
			ordered[i] = contract.OrderedFact{ID: n.ID, Dependencies: n.Dependencies, NormalizedTime: n.NormalizedTime}
			kinds[n.ID] = n.Kind
		}
		order, err := contract.CausalOrder(ordered)
		if err != nil {
			return fmt.Errorf("protocol: causal order of the archive: %w", err)
		}
		// A node's kept receipts travel beside its own, in receipt order: a node whose own receipt
		// rejects it is a node by a kept provisional receipt, which takes its place here; its
		// rejection follows with the rejected facts.
		superseded, err := acct.SupersededByFact()
		if err != nil {
			return err
		}
		for _, id := range order {
			if kinds[id] == "decision" {
				d, err := acct.DecisionBytes(id)
				if err != nil {
					return err
				}
				obj, err := contract.DecodeJSON(d)
				if err != nil {
					return err
				}
				if err := emit(map[string]any{"decision": obj}); err != nil {
					return err
				}
				continue
			}
			if err := emitNode(acct, id, superseded[id], emit); err != nil {
				return err
			}
		}
		rejected, err := acct.RejectedFacts()
		if err != nil {
			return err
		}
		for _, f := range rejected {
			if err := emitFact(acct, f.OpID, emit); err != nil {
				return err
			}
		}
		records, err := acct.Records(req.FieldGroups)
		if err != nil {
			return err
		}
		for _, rec := range records {
			value, err := contract.DecodeJSON([]byte(rec.Value))
			if err != nil {
				return err
			}
			prov, err := contract.DecodeJSON([]byte(rec.Provenance))
			if err != nil {
				return err
			}
			item := map[string]any{"entity": rec.Entity, "id": rec.EntityID, "field_group": rec.FieldGroup, "value": value, "provenance": prov}
			if err := emit(map[string]any{"record": item}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		file.Close()
		return "", err
	}
	meta.ItemCount, meta.SnapshotDigest = count, hex.EncodeToString(digest)
	if err := w.Flush(); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", err
	}
	if err := v.checkSpace(); err != nil {
		file.Close()
		return "", err // the whole view must leave the reserve free
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	data, metaPath := v.paths(id)
	if err := os.Rename(tmp, data); err != nil {
		return "", err
	}
	mb, _ := json.Marshal(meta)
	metaTmp := metaPath + ".tmp"
	if err := writeSynced(metaTmp, mb); err != nil {
		return "", err
	}
	if err := os.Rename(metaTmp, metaPath); err != nil {
		return "", err
	}
	if err := syncDir(v.dir); err != nil {
		return "", err
	}
	// The new view supersedes the installation's older ones of this mode.
	v.mu.Lock()
	v.views[id] = meta
	v.counter++
	v.order[id] = v.counter
	v.pruneSupersededLocked()
	v.mu.Unlock()
	return id, nil
}

func emitFact(acct *storage.AccountReader, opID string, emit func(map[string]any) error) error {
	factBytes, receiptBytes, err := acct.FactBytes(opID)
	if err != nil {
		return err
	}
	return emitAccepted(factBytes, receiptBytes, emit)
}

// emitNode emits a fact node: its own receipt, unless that rejects it, then
// the receipts kept beside it, in receipt order.
func emitNode(acct *storage.AccountReader, opID string, kept [][]byte, emit func(map[string]any) error) error {
	factBytes, receiptBytes, err := acct.FactBytes(opID)
	if err != nil {
		return err
	}
	var own struct {
		Outcome string `json:"outcome"`
	}
	if err := json.Unmarshal(receiptBytes, &own); err != nil {
		return err
	}
	if own.Outcome != "rejected" {
		if err := emitAccepted(factBytes, receiptBytes, emit); err != nil {
			return err
		}
	}
	for _, r := range kept {
		if err := emitAccepted(factBytes, r, emit); err != nil {
			return err
		}
	}
	return nil
}

func emitAccepted(factBytes, receiptBytes []byte, emit func(map[string]any) error) error {
	fact, err := contract.DecodeJSON(factBytes)
	if err != nil {
		return err
	}
	receipt, err := contract.DecodeJSON(receiptBytes)
	if err != nil {
		return err
	}
	return emit(map[string]any{"accepted": map[string]any{"fact": fact, "receipt": receipt}})
}

func writeSynced(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// page serves one page of view id starting at (index, part).
func (v *ViewRegistry) page(key storage.ReceiptKey, req SnapshotRequest, id string, index, part int64, limit int,
	check func(viewMeta) (int64, error)) (*SnapshotResponse, error) {
	data, metaPath := v.paths(id)
	v.mu.Lock()
	if v.building[id] {
		v.mu.Unlock()
		return nil, errViewExpired
	}
	meta, err := readMeta(metaPath)
	if err != nil || meta.AccountID != req.AccountID || v.now().Sub(modTime(metaPath)) > ViewLifetime {
		v.mu.Unlock()
		return nil, errViewExpired
	}
	v.pinned[id]++
	_ = os.Chtimes(metaPath, v.now(), v.now())
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		v.pinned[id]--
		if v.pinned[id] == 0 {
			delete(v.pinned, id)
		}
		v.mu.Unlock()
	}()
	watermark, err := check(meta)
	if err != nil {
		return nil, err
	}
	offsets, err := v.offsetsOf(id, data)
	if err != nil {
		return nil, errViewExpired
	}
	digest, _ := hex.DecodeString(meta.HeadDigest)
	head, err := contract.Cursor{LogID: meta.LogID, Epoch: meta.Epoch, Seq: meta.HeadSeq, Digest: digest}.Encode()
	if err != nil {
		return nil, err
	}
	resp := &SnapshotResponse{Head: head, InstallWatermark: watermark, Items: []any{}}
	f, err := os.Open(data)
	if err != nil {
		return nil, errViewExpired
	}
	defer f.Close()
	budget := PageBytes
	for index < meta.ItemCount && len(resp.Items) < limit {
		start, end := offsets[index], offsets[index+1]-1 // without the newline
		size := int(end - start)
		if part == 0 && size <= PageBytes {
			if size > budget && len(resp.Items) > 0 {
				break
			}
			buf := make([]byte, size)
			if _, err := f.ReadAt(buf, start); err != nil {
				return nil, err
			}
			var item map[string]any
			if v, err := contract.DecodeJSON(buf); err == nil {
				item, _ = v.(map[string]any)
			}
			if item == nil {
				return nil, errors.New("protocol: corrupt snapshot view")
			}
			resp.Items = append(resp.Items, json.RawMessage(buf))
			budget -= size
			index++
			continue
		}
		if len(resp.Items) > 0 {
			break
		}
		count := (size + PageBytes - 1) / PageBytes
		from := start + part*PageBytes
		to := min(from+PageBytes, end)
		buf := make([]byte, to-from)
		if _, err := f.ReadAt(buf, from); err != nil && err != io.EOF {
			return nil, err
		}
		resp.Items = append(resp.Items, map[string]any{"item_part": map[string]any{"index": index + 1, "part_index": part,
			"part_count": count, "data": base64.RawURLEncoding.EncodeToString(buf)}})
		part++
		if int(part) == count {
			index, part = index+1, 0
		}
		break
	}
	if index < meta.ItemCount {
		resp.HasMore = true
		next, err := sealToken(key, map[string]any{"kind": "snapshot", "account": req.AccountID, "install": req.InstallID,
			"mode": req.Mode, "recovery": req.RecoveryID, "view": id, "next_index": index, "next_part": part, "limit": int64(limit)})
		if err != nil {
			return nil, err
		}
		resp.Next = next
	} else {
		resp.Manifest = map[string]any{"item_count": meta.ItemCount, "snapshot_digest": meta.SnapshotDigest}
	}
	return resp, nil
}

func modTime(path string) time.Time {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime()
}

// offsetsOf returns item start offsets (plus the end) for a published view.
func (v *ViewRegistry) offsetsOf(id, path string) ([]int64, error) {
	v.mu.Lock()
	cached := v.offsets[id]
	v.mu.Unlock()
	if cached != nil {
		return cached, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	offsets := []int64{0}
	var pos int64
	for {
		line, err := r.ReadSlice('\n')
		pos += int64(len(line))
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, pos)
	}
	v.mu.Lock()
	v.offsets[id] = offsets
	v.mu.Unlock()
	return offsets, nil
}

// SweepViews runs view cleanup, and the expiry of abandoned recovery
// frames, every interval until ctx ends.
func (s *Service) SweepViews(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Views.Sweep(s.CurrentLineage(ctx))
			// Abandoned recovery frames expire too.
			if err := s.Store.Write(ctx, s.expireFrames); err != nil && ctx.Err() == nil {
				slog.Warn("expiring abandoned recovery frames", "error", err)
			}
		}
	}
}
