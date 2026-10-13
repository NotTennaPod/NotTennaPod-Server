package protocol

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// Free space is checked as a view is written, equivalent concurrent requests
// share one build, and views are keyed by installation, account, mode and head.

func TestViewHeadroomDuringBuild(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	f := fs{op: randomID(), author: randomID(), seq: 1, ct: 1, payload: text(strings.Repeat("x", 200<<10))}.object()
	h.push(in, 0, fop(1, f))
	const reserve = int64(1 << 30)
	var checks atomic.Int32
	usage := func() int64 {
		var n int64
		entries, _ := os.ReadDir(h.svc.Views.dir)
		for _, e := range entries {
			st, _ := e.Info()
			n += st.Size()
		}
		return n
	}
	h.svc.Views.Headroom = func() int64 { return reserve }
	h.svc.Views.FreeBytes = func(string) (int64, error) { checks.Add(1); return reserve + 1024 - usage(), nil }
	_, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in})
	t.Logf("result=%v space_checks=%d bytes_written=%d available_budget=1024", err, checks.Load(), usage())
	if protoCode(err) != "server_busy" {
		t.Errorf("a view exceeded the headroom budget without refusal: %v", err)
	}
	if usage() > 1024 {
		t.Errorf("retained %d bytes after exceeding free-space allowance", usage())
	}
}

func TestConcurrentEquivalentViews(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	const n = 8
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	h.svc.Views.Headroom = func() int64 { return 1 }
	h.svc.Views.FreeBytes = func(string) (int64, error) { entered <- struct{}{}; <-release; return 1 << 40, nil }
	done := make(chan error, n)
	for range n {
		go func() {
			_, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in})
			done <- err
		}()
	}
	for range n {
		<-entered
	}
	close(release)
	for range n {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	paths, _ := filepath.Glob(filepath.Join(h.svc.Views.dir, "*.jsonl"))
	t.Logf("concurrent equivalent requests=%d retained views=%d", n, len(paths))
	if len(paths) != 1 {
		t.Errorf("equivalent requests built %d duplicate views", len(paths))
	}
}

func TestViewKeysAndFreshness(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	req := SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in}
	id, err := h.svc.view(h.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := h.svc.view(h.ctx, req)
	if err != nil || again != id {
		t.Fatalf("reuse %s %v", again, err)
	}
	otherInstall := h.addInstall()
	otherAccount := h.newAccount()
	otherAccountInstall := h.addInstallIn(otherAccount)
	cur := h.head()
	for _, alt := range []SnapshotRequest{
		{AccountID: h.account, Mode: "attach", InstallID: otherInstall},
		{AccountID: otherAccount, Mode: "attach", InstallID: otherAccountInstall},
		{AccountID: h.account, Mode: "upgrade", InstallID: in, Cursor: &cur, FieldGroups: []string{"text"}},
		{AccountID: h.account, Mode: "recovery", InstallID: in},
	} {
		got, err := h.svc.view(h.ctx, alt)
		if err != nil || got == id {
			t.Errorf("foreign key %+v reused=%v err=%v", alt, got == id, err)
		}
	}
	h.fill(in, 1)
	fresh, err := h.svc.view(h.ctx, req)
	if err != nil || fresh == id {
		t.Fatalf("stale head reuse: %s %v", fresh, err)
	}
}
