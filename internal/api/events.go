package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The event stream (PROTOCOL §12): server-sent events telling a
// browser or a phone that its account changed. Events are hints carrying only
// the head; the client re-reads or pulls. A stream belongs to one account and
// closes when its credential stops being valid. Phones use it with their
// device token, polling when it is closed.
func (s *Server) eventRoutes() {
	if s.cfg.Views == nil {
		return
	}
	s.route("/v1/events", endpoint{method: http.MethodGet, auth: authSync, handle: (*request).events})
}

// streamed marks a response the handler already wrote.
type streamed struct{}

var (
	heartbeatInterval  = 25 * time.Second
	credentialInterval = 5 * time.Second
	// streamWriteTimeout is how long one write of the stream may wait for a
	// client that stopped reading; it is renewed before every write.
	streamWriteTimeout = 2 * time.Minute
)

// Shutdown ends every event stream, so a graceful shutdown need not wait for them.
func (s *Server) Shutdown() {
	s.streamsOnce.Do(func() { close(s.streamsDone) })
}

func (req *request) events() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	account := req.principal.accountID()
	store := req.s.cfg.Store
	changes, cancel := store.Subscribe(account)
	defer cancel()
	rc := http.NewResponseController(req.w)
	// Every write renews the stream's deadline; the client going away or
	// the server shutting down interrupts a blocked write at once.
	write := func(s string) error {
		_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
		if _, err := io.WriteString(req.w, s); err != nil {
			return err
		}
		return rc.Flush()
	}
	ended := make(chan struct{})
	defer close(ended)
	go func() {
		select {
		case <-req.ctx.Done():
		case <-req.s.streamsDone:
		case <-ended:
			return
		}
		_ = rc.SetWriteDeadline(time.Now())
	}()
	_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
	h := req.w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	req.w.WriteHeader(http.StatusOK)
	var last storage.Head
	sent := false
	closed := func() { _ = write("event: closed\ndata: {}\n\n") }
	send := func() error {
		var head storage.Head
		ended := false
		if err := store.Read(req.ctx, func(r *storage.ReadTx) error {
			// Each event checks the caller in the read that supplies it.
			if _, err := req.revalidateRead(r); err != nil {
				ended = true
				return err
			}
			var err error
			head, err = r.Account(account).Head()
			return err
		}); err != nil {
			if ended {
				closed()
			}
			return err
		}
		if sent && head.LogID == last.LogID && head.Epoch == last.Epoch && head.Seq == last.Seq {
			return nil
		}
		sent, last = true, head
		return write(fmt.Sprintf("event: changed\ndata: {\"log_id\":%q,\"epoch\":%d,\"seq\":%d}\n\n", head.LogID, head.Epoch, head.Seq))
	}
	if err := send(); err != nil {
		return 0, streamed{}, nil
	}
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	check := time.NewTicker(credentialInterval)
	defer check.Stop()
	for {
		select {
		case <-req.ctx.Done():
			return 0, streamed{}, nil
		case <-req.s.streamsDone:
			return 0, streamed{}, nil
		case <-changes:
			if err := send(); err != nil {
				return 0, streamed{}, nil
			}
		case <-heartbeat.C:
			if write(": heartbeat\n\n") != nil {
				return 0, streamed{}, nil
			}
		case <-check.C:
			if !req.credentialValid() {
				closed()
				return 0, streamed{}, nil
			}
		}
	}
}

// credentialValid re-applies the endpoint's credential rules to the
// credential as it is now.
func (req *request) credentialValid() bool {
	var cred storage.Credential
	err := req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		var err error
		cred, err = r.CredentialByHash(req.principal.cred.Hash)
		return err
	})
	if err != nil || !bytes.Equal(cred.Hash, req.principal.cred.Hash) || cred.AccountID != req.principal.accountID() {
		return false
	}
	if cred.ExpiresAt != nil && req.s.cfg.Now().UnixMilli() >= *cred.ExpiresAt {
		return false
	}
	inst := cred.Installation
	return cred.AccountState == "active" && inst != nil && inst.RevokedAt == nil && inst.SupersededBy == ""
}
