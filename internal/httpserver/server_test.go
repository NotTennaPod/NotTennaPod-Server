package httpserver

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/webui"
)

func TestServerLifecycle(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- Run(ctx, listener, nil, nil) }()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	base := "http://" + listener.Addr().String()
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
		{http.MethodGet, "/info", http.StatusNotFound},
	} {
		req, err := http.NewRequestWithContext(ctx, tc.method, base+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != tc.status {
			t.Errorf("%s %s: got %d, want %d", tc.method, tc.path, res.StatusCode, tc.status)
		}
		if tc.status == http.StatusOK && string(body) != "ok\n" {
			t.Errorf("unexpected health response %q", body)
		}
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after cancellation")
	}
	if conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second); err == nil {
		conn.Close()
		t.Fatal("listener is still accepting connections after shutdown")
	}
}

func TestCancellationBeforeStartClosesListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	finished := make(chan error, 1)
	go func() { finished <- Run(ctx, listener, nil, nil) }()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server hung with an already canceled context")
	}
	if conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second); err == nil {
		conn.Close()
		t.Fatal("cancellation leaked the listener")
	}
}

// The web app (PROTOCOL §12): index.html revalidated, app routes served
// index.html, unknown API paths left to the API, and the security headers on
// every response, the API's and the liveness check's included.
func TestWebAppAndSecurityHeaders(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found","message":"Not found."}`))
	})
	finished := make(chan error, 1)
	go func() { finished <- Run(ctx, listener, api, webui.Handler()) }()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	base := "http://" + listener.Addr().String()
	for _, tc := range []struct {
		method, path, contentType, cache string
		status                           int
	}{
		{http.MethodGet, "/", "text/html", "no-cache", 200},
		{http.MethodGet, "/podcasts/p1:abc/episodes", "text/html", "no-cache", 200},
		{http.MethodHead, "/queue", "text/html", "no-cache", 200},
		{http.MethodPost, "/", "text/plain", "", 405},
		{http.MethodGet, "/v1/nothing", "application/json", "", 404},
		{http.MethodGet, "/healthz", "text/plain", "no-store", 200},
	} {
		req, _ := http.NewRequestWithContext(ctx, tc.method, base+tc.path, nil)
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != tc.status || !strings.HasPrefix(res.Header.Get("Content-Type"), tc.contentType) ||
			(tc.cache != "" && res.Header.Get("Cache-Control") != tc.cache) {
			t.Errorf("%s %s: %d %q %q", tc.method, tc.path, res.StatusCode, res.Header.Get("Content-Type"), res.Header.Get("Cache-Control"))
		}
		if tc.contentType == "text/html" && tc.method == http.MethodGet && !strings.Contains(string(body), "NotTennaPod") {
			t.Errorf("%s: not the app's index: %q", tc.path, body)
		}
		if res.Header.Get("Content-Security-Policy") != webui.CSP || res.Header.Get("Referrer-Policy") != "no-referrer" ||
			res.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s %s: security headers missing: %v", tc.method, tc.path, res.Header)
		}
	}
	cancel()
	<-finished
}
