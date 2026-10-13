package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/service"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Another process answering on the service's port with the new version does not
// make an update ready; the previous version is put back. Nothing here installs
// a service: the service manager is a fake.
func TestUpdateIsNotSatisfiedByAnotherProcessOnThePort(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"version":"fake-v2","instance_id":"0123456789abcdef0123456789abcdef"}`))
	}))
	defer fake.Close()
	m, err := newManager(service.Label)
	if err != nil {
		t.Fatal(err)
	}
	d := t.TempDir()
	data := filepath.Join(t.TempDir(), "data")
	s, err := storage.Open(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	m.Layout = service.Layout{GOOS: "darwin", Label: service.Label, InstallDir: d, LogDir: t.TempDir()}
	// Every start of the "service" writes a new instance ID, as serve does;
	// the process on the port is another one.
	m.Run = func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "launchctl" && args[0] == "kickstart" {
			if _, err := storage.NewInstance(data); err != nil {
				t.Error(err)
			}
		}
		return nil, nil
	}
	m.HealthWait = time.Second
	os.WriteFile(m.BinaryPath(), []byte("working-old"), 0o700)
	manifest, _ := json.Marshal(service.Manifest{Version: "old", Listen: strings.TrimPrefix(fake.URL, "http://"), Args: []string{"serve", "--data-dir", data}})
	os.WriteFile(m.ManifestPath(), manifest, 0o600)
	candidate := filepath.Join(t.TempDir(), "candidate")
	os.WriteFile(candidate, []byte("#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'nottennapod-server fake-v2'; exit 0; fi\nexit 1\n"), 0o700)
	v, err := m.Update(context.Background(), candidate)
	if err == nil {
		t.Fatalf("the update was satisfied by another process on the port: version %s", v)
	}
	if !strings.Contains(err.Error(), "not the one just started") {
		t.Errorf("the failure does not say why: %v", err)
	}
	if b, _ := os.ReadFile(m.BinaryPath()); string(b) != "working-old" {
		t.Errorf("the previous binary is not back: %q", b)
	}
}
