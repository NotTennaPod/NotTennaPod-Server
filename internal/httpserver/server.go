// Package httpserver provides the process lifecycle: it serves the liveness
// check and the API until cancellation, then shuts down gracefully.
package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/webui"
)

// Config is what Serve serves.
type Config struct {
	API http.Handler // everything under /v1/ (nil: nothing)
	Web http.Handler // every other path but the liveness check (nil: nothing)
	// Hosts are the Host names answered; nil answers loopback names and the
	// address each connection arrived at.
	Hosts *Hosts
}

// Run serves api and web with the default Hosts (see Serve).
func Run(ctx context.Context, listener net.Listener, api, web http.Handler) error {
	return Serve(ctx, listener, Config{API: api, Web: web})
}

// Serve owns listener and serves until cancellation or a listener error. An
// API with a Shutdown method has it called when shutdown begins, to end its
// long-lived streams. Every request first gets its body and response
// deadlines, then is checked for its Host, so a refused request is bounded
// too.
func Serve(ctx context.Context, listener net.Listener, cfg Config) error {
	api, web := cfg.API, cfg.Web
	mux := http.NewServeMux()
	if api != nil {
		mux.Handle("/v1/", api)
	}
	if web != nil {
		mux.Handle("/", web)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte("ok\n"))
	})
	secured := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webui.SecurityHeaders(w.Header())
		w, r = withDeadlines(w, r)
		if !cfg.Hosts.Allowed(r) {
			misdirected(w)
			// Nothing more is read from a refused request: its connection ends
			// with the answer instead of waiting for its body.
			_ = http.NewResponseController(w).SetReadDeadline(time.Now())
			return
		}
		mux.ServeHTTP(w, r)
	})
	server := &http.Server{
		Handler:           secured,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	if s, ok := api.(interface{ Shutdown() }); ok {
		server.RegisterOnShutdown(s.Shutdown)
	}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return err
		}
		<-finished
		return nil
	}
}
