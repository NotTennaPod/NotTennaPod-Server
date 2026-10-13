package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The version check returns within its bound even when the candidate leaves a
// descendant holding its output, and that descendant ends with it.
func TestVersionCheckIsBounded(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "candidate")
	pidFile := filepath.Join(dir, "child.pid")
	source := fmt.Sprintf("#!/bin/sh\n/bin/sleep 60 &\nprintf '%%s' \"$!\" > '%s'\nprintf 'nottennapod-server 2\\n'\n", pidFile)
	if err := os.WriteFile(script, []byte(source), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := versionOf(ctx, script); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("a candidate that leaves a descendant holding its output passed the check")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the version check did not return")
	}
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			syscall.Kill(pid, syscall.SIGKILL)
			t.Fatal("the candidate's descendant outlived the check")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// An ordinary candidate passes.
	ok := filepath.Join(dir, "ok")
	os.WriteFile(ok, []byte("#!/bin/sh\necho 'nottennapod-server 3'\n"), 0o700)
	if v, err := versionOf(context.Background(), ok); err != nil || v != "3" {
		t.Fatalf("an ordinary candidate: %q %v", v, err)
	}
}
