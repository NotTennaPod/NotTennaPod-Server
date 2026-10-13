package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/service"
)

// --allow-host is repeatable, refuses what is not a host
// name, and round-trips through service install into the service
// definition, as --public-url does. The manager runs no launchctl or
// systemctl: its commands are recorded only.
func TestAllowHostRoundTripsThroughServiceInstall(t *testing.T) {
	var l hostList
	for _, bad := range []string{"", "https://nas.example/", "user@nas.example", "nas example"} {
		if err := l.Set(bad); err == nil {
			t.Errorf("--allow-host %q was accepted", bad)
		}
	}
	home := t.TempDir()
	layout, err := service.DefaultLayout(runtime.GOOS, home, "app.nottennapod.server.test")
	if err != nil {
		t.Skip(err)
	}
	var ran []string
	m := &service.Manager{Layout: layout, UID: os.Getuid(),
		Run: func(_ context.Context, name string, args ...string) ([]byte, error) {
			ran = append(ran, name+" "+strings.Join(args, " "))
			return nil, nil
		},
		VersionOf: func(context.Context, string) (string, error) { return "test", nil }}
	binary := filepath.Join(t.TempDir(), "nottennapod-server")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := serviceInstall(context.Background(), m, []string{"--data-dir", filepath.Join(home, "data"), "--new-server", "--binary", binary,
		"--allow-host", "nas.example", "--allow-host", "192.168.1.5:8080"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("install: %d %s", code, errOut.String())
	}
	b, err := os.ReadFile(m.ManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	var manifest service.Manifest
	if err := json.Unmarshal(b, &manifest); err != nil {
		t.Fatal(err)
	}
	i := slices.Index(manifest.Args, "--allow-host")
	if i < 0 || !slices.Equal(manifest.Args[i:i+4], []string{"--allow-host", "nas.example", "--allow-host", "192.168.1.5:8080"}) {
		t.Fatalf("the service's arguments %v", manifest.Args)
	}
	agent, err := os.ReadFile(m.AgentFile)
	if err != nil || !strings.Contains(string(agent), "nas.example") || !strings.Contains(string(agent), "192.168.1.5:8080") {
		t.Fatalf("the service definition: %v\n%s", err, agent)
	}
}
