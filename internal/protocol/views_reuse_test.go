package protocol

import (
	"testing"
	"time"
)

// Repeated snapshot requests do not keep a full copy each.

// An equivalent request (same installation, mode, head and field groups)
// reuses its view; a different one builds its own.
func TestEquivalentSnapshotRequestsReuseTheirView(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	var first string
	for i := 0; i < 24; i++ {
		r, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1 + i%2})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = r.Head
		} else if r.Head != first {
			t.Fatalf("an equivalent request has another head: %s", r.Head)
		}
	}
	if views := viewFiles(t, h, "*.jsonl"); len(views) != 1 {
		t.Fatalf("24 equivalent requests left %d views", len(views))
	}
	if _, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, FieldGroups: []string{"metadata"}}); err != nil {
		t.Fatal(err)
	}
	other := h.addInstall()
	if _, err := h.svc.Snapshot(h.ctx, SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: other}); err != nil {
		t.Fatal(err)
	}
	if views := viewFiles(t, h, "*.jsonl"); len(views) != 3 {
		t.Fatalf("other field groups and another installation: %d views, want 3", len(views))
	}
}

// An installation's new view of a mode supersedes its older views once none
// of their pages has been read for ten minutes.
func TestANewerViewSupersedesOlderOnes(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 3)
	req := SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in, Limit: 1}
	old, err := h.svc.Snapshot(h.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	h.fill(in, 1)
	if _, err := h.svc.Snapshot(h.ctx, req); err != nil {
		t.Fatal(err)
	}
	// Read within ten minutes, the older view stays.
	h.vclock.Advance(9 * time.Minute)
	next := req
	next.Page = old.Next
	if old, err = h.svc.Snapshot(h.ctx, next); err != nil {
		t.Fatalf("a superseded view still being read: %v", err)
	}
	h.vclock.Advance(9 * time.Minute)
	h.svc.Views.Sweep(h.currentLineage())
	if views := viewFiles(t, h, "*.jsonl"); len(views) != 2 {
		t.Fatalf("a superseded view read 9 minutes ago was removed: %d views", len(views))
	}
	// Unread for ten minutes, it goes.
	h.vclock.Advance(2 * time.Minute)
	h.svc.Views.Sweep(h.currentLineage())
	if views := viewFiles(t, h, "*.jsonl"); len(views) != 1 {
		t.Fatalf("a superseded view unread for 11 minutes stayed: %d views", len(views))
	}
	next.Page = old.Next
	if _, err := h.svc.Snapshot(h.ctx, next); protoCode(err) != "view_expired" {
		t.Fatalf("the superseded view's next page: %v", err)
	}
	// Publishing a newer view prunes too, without waiting for a sweep.
	h.vclock.Advance(11 * time.Minute)
	h.fill(in, 1)
	if _, err := h.svc.Snapshot(h.ctx, req); err != nil {
		t.Fatal(err)
	}
	if views := viewFiles(t, h, "*.jsonl"); len(views) != 1 {
		t.Fatalf("publishing a newer view left %d views", len(views))
	}
}

// View storage keeps the backups' free space; beyond it a new view answers
// server_busy, while an existing equivalent view is still served.
func TestViewsKeepTheBackupsHeadroom(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	h.fill(in, 2)
	req := SnapshotRequest{AccountID: h.account, Mode: "attach", InstallID: in}
	if _, err := h.svc.Snapshot(h.ctx, req); err != nil {
		t.Fatal(err)
	}
	h.svc.Views.FreeBytes = func(string) (int64, error) { return 10 << 20, nil }
	h.svc.Views.Headroom = func() int64 { return 1 << 30 }
	if _, err := h.svc.Snapshot(h.ctx, req); err != nil {
		t.Fatalf("an existing equivalent view: %v", err)
	}
	h.fill(in, 1)
	_, err := h.svc.Snapshot(h.ctx, req)
	if protoCode(err) != "server_busy" || protoStatus(err) != 503 {
		t.Fatalf("a new view without free space: %v, want 503 server_busy", err)
	}
	if views := viewFiles(t, h, "*.jsonl"); len(views) != 1 {
		t.Fatalf("%d views", len(views))
	}
}
