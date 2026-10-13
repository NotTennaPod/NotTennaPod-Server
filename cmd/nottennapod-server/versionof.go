package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/service"
)

// versionOf runs a candidate binary with --version (update's and install's check that it
// runs). The wait is bounded: the process runs in its own group, which ends on time-out, and
// the output is read for at most a moment after the process exits, so a descendant holding
// its output cannot hold the update (and its lock).
func versionOf(ctx context.Context, binary string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "--version")
	ownGroup(cmd)
	cmd.WaitDelay = 100 * time.Millisecond
	out, err := cmd.Output()
	if err != nil {
		endGroup(cmd) // what it left behind
		return "", err
	}
	// One line, "nottennapod-server VERSION".
	v, ok := strings.CutPrefix(strings.TrimSpace(string(out)), "nottennapod-server ")
	if !ok {
		return "", fmt.Errorf("it printed %q, not a nottennapod-server version", truncate(string(out), 80))
	}
	if err := service.ValidVersion(v); err != nil {
		return "", fmt.Errorf("its version is not usable: %v", err)
	}
	return v, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
