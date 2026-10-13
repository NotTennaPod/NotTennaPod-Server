package protocol

// Pull and snapshot pages (PROTOCOL §6.6, §6.7).

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// fill pushes n single-fact commits from install.
func (h *harness) fill(install string, n int) {
	h.t.Helper()
	start := h.consumed(install)
	for i := 1; i <= n; i++ {
		seq := start + int64(i)
		f := fs{op: randomID(), author: randomID(), seq: 1, ct: h.clock.millis() - 1000}.object()
		r := h.push(install, seq-1, fop(seq, f))
		if r.Results[0].Status != "accepted" {
			h.t.Fatalf("fill: %+v", r.Results[0])
		}
	}
}

func (h *harness) currentLineage() func(string) (string, int64, bool) {
	return func(account string) (string, int64, bool) {
		var logID string
		var epoch int64
		err := h.store.Read(h.ctx, func(r *storage.ReadTx) error {
			hd, err := r.Account(account).Head()
			logID, epoch = hd.LogID, hd.Epoch
			return err
		})
		return logID, epoch, err == nil
	}
}

func staleEverything(string) (string, int64, bool) { return "", -1, true }

func viewFiles(t testing.TB, h *harness, pattern string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(h.store.Dir(), "views", pattern))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestPullRepeatedTokenReturnsTheSamePage(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 6)
	from := h.genesis()
	req := PullRequest{AccountID: h.account, Cursor: from, InstallID: in, AckedInstallSeq: 6, Limit: 2}
	first, err := h.svc.Pull(h.ctx, req)
	if err != nil || !first.HasMore || len(first.Items) != 2 || first.InstallWatermark != 6 {
		t.Fatalf("first page: %s %v", wireJSON(t, first), err)
	}
	req.Page = first.Next
	second, err := h.svc.Pull(h.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	page := wireJSON(t, second)
	// More pushes land; the token still reproduces the same page of the fixed view.
	h.fill(in, 2)
	again, err := h.svc.Pull(h.ctx, req)
	if err != nil || !bytes.Equal(wireJSON(t, again), page) {
		t.Fatalf("the same token returned a different page after new pushes: %v\n%s\n%s", err, page, wireJSON(t, again))
	}
	// The view ends at its fixed head; install_watermark is exact at that head.
	p, err := h.pullAll(h.account, in, from, 8, 2)
	if err != nil {
		t.Fatal(err)
	}
	if p.head.Seq != 8 || p.watermark != 8 {
		t.Errorf("a fresh first request sees head %d watermark %d, want 8 and 8", p.head.Seq, p.watermark)
	}
	req.Page = second.Next
	for {
		r, err := h.svc.Pull(h.ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if r.InstallWatermark != 6 || r.Head != first.Head {
			t.Errorf("a page of the old view reports watermark %d head %s, want 6 and the view's head", r.InstallWatermark, r.Head)
		}
		if !r.HasMore {
			break
		}
		req.Page = r.Next
	}
}

func TestPullFramesACommitLargerThanThePage(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	var ops []PushOp
	for i := 1; i <= 5; i++ {
		f := fs{op: uid(3, i), author: randomID(), seq: 1, ct: 1, payload: text(strings.Repeat(string(rune('a'+i)), 240<<10))}.object()
		ops = append(ops, fop(int64(i), f))
	}
	h.push(in, 0, ops...)
	h.fill(in, 2)
	if n := len(h.commitsOf(h.account)[0]); n <= PageBytes {
		t.Fatalf("the large commit is only %d bytes", n)
	}
	p, err := h.pullAll(h.account, in, h.genesis(), 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.framed < 2 || len(p.commits) != 3 {
		t.Errorf("framed %d commit parts, reassembled %d commits; want the large commit framed", p.framed, len(p.commits))
	}
	// Pulling from the middle of the log works the same way.
	mid := h.headAt(1)
	if _, err := h.pullAll(h.account, in, mid, 7, 1); err != nil {
		t.Fatal(err)
	}
}

// headAt returns the cursor of commit seq.
func (h *harness) headAt(seq int64) contract.Cursor {
	h.t.Helper()
	c := h.head()
	var d []byte
	if err := h.store.Read(h.ctx, func(r *storage.ReadTx) error {
		var err error
		d, err = r.Account(h.account).CommitDigest(seq)
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
	return contract.Cursor{LogID: c.LogID, Epoch: c.Epoch, Seq: seq, Digest: d}
}

func TestPullLineageChecks(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	pull := func(c contract.Cursor, acked int64, page string) error {
		_, err := h.svc.Pull(h.ctx, PullRequest{AccountID: h.account, Cursor: c, InstallID: in, AckedInstallSeq: acked, Page: page, Limit: 1})
		return err
	}
	head := h.head()
	wrongDigest := h.headAt(2)
	wrongDigest.Digest = bytes.Repeat([]byte{7}, 32)
	cases := map[string]struct {
		c     contract.Cursor
		acked int64
		code  string
	}{
		"unknown log":        {contract.Cursor{LogID: randomID(), Epoch: 1, Seq: 0, Digest: contract.GenesisDigest}, 0, "lineage_unknown"},
		"other epoch":        {contract.Cursor{LogID: head.LogID, Epoch: 2, Seq: 0, Digest: contract.GenesisDigest}, 0, "lineage_mismatch"},
		"digest differs":     {wrongDigest, 0, "lineage_mismatch"},
		"seq above the head": {contract.Cursor{LogID: head.LogID, Epoch: 1, Seq: 4, Digest: head.Digest}, 0, "lineage_mismatch"},
		"lost acknowledged":  {head, 4, "lineage_mismatch"},
	}
	for name, tc := range cases {
		err := pull(tc.c, tc.acked, "")
		if protoCode(err) != tc.code || protoStatus(err) != 409 {
			t.Errorf("%s: %v, want 409 %s", name, err, tc.code)
		}
	}
	// A lineage change mid-pull (a restore moved the log to a new epoch).
	first, err := h.svc.Pull(h.ctx, PullRequest{AccountID: h.account, Cursor: h.genesis(), InstallID: in, AckedInstallSeq: 3, Limit: 1})
	if err != nil || first.Next == "" {
		t.Fatalf("first page: %v", err)
	}
	genesis := h.genesis()
	h.bumpEpoch(h.account)
	if err := pull(genesis, 3, first.Next); protoCode(err) != "lineage_mismatch" {
		t.Errorf("next page after a lineage change: %v, want lineage_mismatch", err)
	}
	// A push with the old lineage consumes nothing.
	f := fs{op: randomID(), author: randomID(), seq: 1, ct: 1}.object()
	_, err = h.svc.Push(h.ctx, PushRequest{AccountID: h.account, Cursor: head, InstallID: in, AckedInstallSeq: 3,
		Operations: []PushOp{fop(4, f)}}, authorizer(h.account, in))
	if protoCode(err) != "lineage_mismatch" || h.consumed(in) != 3 {
		t.Errorf("push with a stale cursor: %v, consumed %d", err, h.consumed(in))
	}
}

func TestPullAndSnapshotTokensAreBoundToAccountAndInstallation(t *testing.T) {
	h := newHarness(t)
	in, sibling := h.addInstall(), h.addInstall()
	h.fill(in, 3)
	other := h.newAccount()
	stranger := h.addInstallIn(other)
	p, err := h.svc.Pull(h.ctx, PullRequest{AccountID: h.account, Cursor: h.genesis(), InstallID: in, AckedInstallSeq: 3, Limit: 1})
	if err != nil || p.Next == "" {
		t.Fatal(err)
	}
	s, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1})
	if err != nil || s.Next == "" {
		t.Fatal(err)
	}
	otherGenesis := h.headOf(other)
	forged := p.Next[:len(p.Next)-2] + map[bool]string{true: "AA", false: "AB"}[strings.HasSuffix(p.Next, "AB")]
	refusals := map[string]error{}
	_, refusals["pull token in a sibling installation"] = h.svc.Pull(h.ctx, PullRequest{AccountID: h.account, Cursor: h.genesis(), InstallID: sibling, Page: p.Next})
	_, refusals["pull token in another account"] = h.svc.Pull(h.ctx, PullRequest{AccountID: other, Cursor: otherGenesis, InstallID: stranger, Page: p.Next})
	_, refusals["forged pull token"] = h.svc.Pull(h.ctx, PullRequest{AccountID: h.account, Cursor: h.genesis(), InstallID: in, AckedInstallSeq: 3, Page: forged})
	_, refusals["pull token as a snapshot token"] = h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: p.Next})
	_, refusals["snapshot token in a sibling installation"] = h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: sibling, Page: s.Next})
	_, refusals["snapshot token in another account"] = h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: other, Mode: "attach", InstallID: stranger, Page: s.Next})
	for name, err := range refusals {
		if err == nil || protoStatus(err) < 400 || protoStatus(err) >= 500 {
			t.Errorf("%s: %v, want a refusal", name, err)
		}
	}
}

func TestSnapshotFramesAnItemLargerThanThePage(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	f := fs{op: uid(4, 1), author: randomID(), seq: 1, ct: 1, payload: text(strings.Repeat("s", 1536<<10))}.object()
	ops := partOps(1, f, MaxPartBytes, randomID())
	for i, op := range ops {
		r := h.push(in, 0, op)
		if want := map[bool]string{true: "accepted", false: "part_stored"}[i == len(ops)-1]; r.Results[0].Status != want {
			t.Fatalf("part %d: %s", i, wireJSON(t, r.Results[0]))
		}
	}
	h.fill(in, 2)
	s, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in})
	if err != nil {
		t.Fatal(err)
	}
	if s.framed < 2 {
		t.Errorf("the 1.5 MiB item was not framed (%d frames)", s.framed)
	}
	items := checkSnapshotItems(t, h.keys(), h.account, s.items)
	if i := indexOf(items, uid(4, 1)); i < 0 || !bytes.Equal(canon(items[i].fact), canon(f)) {
		t.Error("the framed item does not reassemble to the fact")
	}
}

func TestSnapshotOrdersRejectedEntriesByEpochSeqAndOpID(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	rejected := func(op string) map[string]any {
		return fs{op: op, author: randomID(), seq: 1, ct: 1, payload: map[string]any{"bad": 1}}.object()
	}
	admitted := uid(9, 9)
	h.push(in, 0, fop(1, rejected(uid(0xf, 1))), fop(2, rejected(uid(0x1, 1))), fop(3, fs{op: admitted, author: randomID(), seq: 1, ct: 1}.object()))
	h.push(in, 3, fop(4, rejected(uid(0x0, 2))))
	h.bumpEpoch(h.account)
	h.push(in, 4, fop(5, rejected(uid(0x0, 0))))
	items := h.verifySnapshot(in)
	var order []string
	for _, it := range items {
		if it.kind == "accepted" && it.receipt["outcome"] == "rejected" {
			order = append(order, it.id)
		}
	}
	want := []string{uid(0x1, 1), uid(0xf, 1), uid(0x0, 2), uid(0x0, 0)}
	if !slices.Equal(order, want) {
		t.Errorf("rejected entries %v, want %v", order, want)
	}
	if indexOf(items, admitted) != 0 {
		t.Error("the admitted fact does not come first")
	}
}

func TestSnapshotUpgradeChecksLineage(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 2)
	head := h.head()
	bad := head
	bad.Digest = bytes.Repeat([]byte{1}, 32)
	unknown := contract.Cursor{LogID: randomID(), Epoch: 1, Seq: 0, Digest: contract.GenesisDigest}
	for name, tc := range map[string]struct {
		c     contract.Cursor
		acked int64
		code  string
	}{
		"digest differs":    {bad, 2, "lineage_mismatch"},
		"unknown log":       {unknown, 0, "lineage_unknown"},
		"lost acknowledged": {head, 3, "lineage_mismatch"},
	} {
		c := tc.c
		_, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "upgrade", Cursor: &c, InstallID: in, AckedInstallSeq: tc.acked})
		if protoCode(err) != tc.code {
			t.Errorf("%s: %v, want %s", name, err, tc.code)
		}
	}
	s, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "upgrade", Cursor: &head, InstallID: in, AckedInstallSeq: 2, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := head.Encode(); s.headStr != want {
		t.Errorf("upgrade snapshot head %s, want %s", s.headStr, want)
	}
}

// PROTOCOL §6.4: snapshot pages carry install_watermark; for pulls it is "the
// installation's consumed watermark at H, so the client may retire outbox slots
// ≤ it". A capability upgrade by an installation that has pushed must report it.
func TestSnapshotReportsTheInstallWatermarkAtItsHead(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	head := h.head()
	s, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "upgrade", Cursor: &head, InstallID: in, AckedInstallSeq: 3, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if s.watermark != 3 {
		t.Errorf("upgrade snapshot install_watermark %d, want 3 (the installation's consumed watermark at H)", s.watermark)
	}
}

func TestSnapshotViewSurvivesARestart(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 5)
	reference, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	first, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 2})
	if err != nil || first.Next == "" {
		t.Fatal(err)
	}
	h.restart()
	var prior [][]byte
	for _, it := range first.Items {
		prior = append(prior, it.(json.RawMessage))
	}
	s, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: first.Next}, prior...)
	if err != nil {
		t.Fatalf("continuing a view after a restart: %v", err)
	}
	items := s.items
	if len(items) != len(reference.items) {
		t.Fatalf("%d items after the restart, want %d", len(items), len(reference.items))
	}
	for i := range items {
		if !bytes.Equal(items[i], reference.items[i]) {
			t.Errorf("item %d differs after the restart", i)
		}
	}
	if s.digest != reference.digest || s.count != reference.count {
		t.Errorf("manifest after the restart (%d %s) differs from (%d %s)", s.count, s.digest, reference.count, reference.digest)
	}
}

func TestSnapshotViewExpiresWhenUnusedForADay(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 4)
	req := SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1}
	r, err := h.svc.Snapshot(h.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	req.Page = r.Next
	// Every use refreshes the view's last use.
	for i := 0; i < 2; i++ {
		h.vclock.Advance(23 * time.Hour)
		if r, err = h.svc.Snapshot(h.ctx, req); err != nil {
			t.Fatalf("a view used within a day expired: %v", err)
		}
		req.Page = r.Next
	}
	h.vclock.Advance(25 * time.Hour)
	_, err = h.svc.Snapshot(h.ctx, req)
	if protoCode(err) != "view_expired" || protoStatus(err) != 410 {
		t.Errorf("a view unused for 25 hours: %v, want 410 view_expired", err)
	}
	h.svc.Views.Sweep(h.currentLineage())
	if left := viewFiles(t, h, "*"); len(left) != 0 {
		t.Errorf("cleanup kept an expired view: %v", left)
	}
	if _, err = h.svc.Snapshot(h.ctx, req); protoCode(err) != "view_expired" {
		t.Errorf("a deleted view: %v, want view_expired", err)
	}
}

// An expired or stale-lineage view answers view_expired.
func TestSnapshotViewOfAStaleLineageAnswersViewExpired(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	r, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	h.bumpEpoch(h.account)
	_, err = h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: r.Next})
	if protoCode(err) != "view_expired" {
		t.Errorf("next page of a view whose lineage changed (no cleanup run yet): %v, want 410 view_expired", err)
	}
	// After a cleanup run it certainly must.
	h.svc.Views.Sweep(h.currentLineage())
	_, err = h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: r.Next})
	if protoCode(err) != "view_expired" || protoStatus(err) != 410 {
		t.Errorf("after cleanup: %v, want 410 view_expired", err)
	}
}

func TestSweepKeepsCurrentViewsAndRemovesStaleOnes(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	r, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	h.svc.Views.Sweep(h.currentLineage())
	if _, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: r.Next}); err != nil {
		t.Fatalf("cleanup removed a current view: %v", err)
	}
	h.bumpEpoch(h.account)
	h.svc.Views.Sweep(h.currentLineage())
	if _, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: r.Next}); protoCode(err) != "view_expired" {
		t.Errorf("a stale-lineage view after cleanup: %v, want view_expired", err)
	}
}

// Runtime cleanup interleaved deterministically with a view being built: the
// build is paused after it registered and created its temporary file (the
// Service's clock is read there); cleanup with every deletion criterion met
// runs; the build then completes and serves.
func TestSweepDuringAViewBuildLeavesTheBuildIntact(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 4)
	old, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	// A new head, so the next request builds a view rather than reusing the
	// equivalent one.
	h.fill(in, 1)
	gate := h.clock.hold()
	type result struct {
		r   *SnapshotResponse
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 2})
		done <- result{r, err}
	}()
	<-gate.entered
	if tmp := viewFiles(t, h, "*.tmp"); len(tmp) != 1 {
		t.Fatalf("the paused build has %d temporary files", len(tmp))
	}
	h.vclock.Advance(48 * time.Hour)
	h.svc.Views.Sweep(staleEverything)
	h.vclock.Advance(-48 * time.Hour)
	if tmp := viewFiles(t, h, "*.tmp"); len(tmp) != 1 {
		t.Fatal("cleanup deleted the temporary file of a view being built")
	}
	if _, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: old.Next}); protoCode(err) != "view_expired" {
		t.Errorf("the idle old view survived the same cleanup: %v", err)
	}
	close(gate.release)
	res := <-done
	if res.err != nil {
		t.Fatalf("the build failed after an interleaved cleanup: %v", res.err)
	}
	var prior [][]byte
	for _, it := range res.r.Items {
		prior = append(prior, it.(json.RawMessage))
	}
	s, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: res.r.Next}, prior...)
	if err != nil || s.count != 5 {
		t.Fatalf("the built view does not serve: %v (count %d)", err, s.count)
	}
}

// Runtime cleanup interleaved with a page being read. The registry has no seam
// inside a page read, so the deterministic part holds the pin a reader holds
// while reading; the concurrent part races cleanup against real page reads and
// accepts only complete, correct pages or view_expired.
func TestSweepDuringAPageReadLeavesTheReadIntact(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 30)
	first, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	metas := viewFiles(t, h, "*.json")
	if len(metas) != 1 {
		t.Fatalf("%d views", len(metas))
	}
	id := strings.TrimSuffix(filepath.Base(metas[0]), ".json")
	v := h.svc.Views
	v.mu.Lock()
	v.pinned[id]++ // a page read in progress
	v.mu.Unlock()
	h.vclock.Advance(48 * time.Hour)
	v.Sweep(staleEverything)
	h.vclock.Advance(-48 * time.Hour)
	if len(viewFiles(t, h, "*")) != 2 {
		t.Fatal("cleanup deleted a view while a page was being read from it")
	}
	page, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: first.Next})
	if err != nil || len(page.Items) != 4 {
		t.Fatalf("reading the pinned view after cleanup: %v", err)
	}
	v.mu.Lock()
	if v.pinned[id]--; v.pinned[id] == 0 {
		delete(v.pinned, id)
	}
	v.mu.Unlock()
	v.Sweep(staleEverything)
	if _, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: first.Next}); protoCode(err) != "view_expired" {
		t.Errorf("once unpinned, the stale view: %v, want view_expired", err)
	}

	// Concurrent: readers page through a fresh view while cleanup runs; the
	// cleanup first keeps the view (current lineage), then deletes it.
	fresh, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{} // token → page bytes
	req := SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: fresh.Next}
	for {
		r, err := h.svc.Snapshot(h.ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		expected[req.Page] = wireJSON(t, r)
		if !r.HasMore {
			break
		}
		req.Page = r.Next
	}
	var readers sync.WaitGroup
	for g := 0; g < 3; g++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			expired := false
			for round := 0; round < 15; round++ {
				for token, want := range expected {
					r, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: token})
					switch {
					case err == nil && !expired:
						if !bytes.Equal(wireJSON(t, r), want) {
							t.Errorf("a page read during cleanup differs")
						}
					case protoCode(err) == "view_expired":
						expired = true
					default:
						t.Errorf("page read during cleanup: %v (expired before: %v)", err, expired)
					}
				}
			}
		}()
	}
	stop := make(chan struct{})
	sweeper := make(chan struct{})
	go func() {
		defer close(sweeper)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i < 40 {
				v.Sweep(h.currentLineage())
			} else {
				v.Sweep(staleEverything)
			}
		}
	}()
	readers.Wait()
	close(stop)
	<-sweeper
}

// Startup cleanup removes only what an interrupted build or publication left.
func TestStartupCleanupRemovesOnlyOrphans(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	r, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(h.store.Dir(), "views")
	for _, name := range []string{"orphan.tmp", "half.jsonl", "meta.json.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h.restart()
	for _, name := range []string{"orphan.tmp", "half.jsonl", "meta.json.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Errorf("startup kept %s", name)
		}
	}
	if _, err := h.snapshotAll(SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Page: r.Next}, r.Items[0].(json.RawMessage)); err != nil {
		t.Errorf("startup cleanup broke a published view: %v", err)
	}
}
