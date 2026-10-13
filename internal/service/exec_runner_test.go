package service

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A service-manager command that hangs ends at its time limit, with a message
// saying so (launchctl and systemctl have no bound of their own).
func TestAServiceCommandThatHangsEnds(t *testing.T) {
	old := serviceCommandTimeout
	serviceCommandTimeout = 300 * time.Millisecond
	defer func() { serviceCommandTimeout = old }()
	start := time.Now()
	_, err := ExecRunner(context.Background(), "sleep", "30")
	if err == nil || !strings.Contains(err.Error(), "did not finish within") {
		t.Fatalf("got %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("took %s", d)
	}
	if out, err := ExecRunner(context.Background(), "echo", "ok"); err != nil || strings.TrimSpace(string(out)) != "ok" {
		t.Fatalf("an ordinary command: %q %v", out, err)
	}
}
