package httpserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// refusedConn records the write deadlines the server sets on a connection.
type refusedConn struct {
	net.Conn
	mu     sync.Mutex
	writes []time.Time
}

func (c *refusedConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writes = append(c.writes, t)
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(t)
}

func (c *refusedConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.writes = append(c.writes, t)
	c.mu.Unlock()
	return c.Conn.SetDeadline(t)
}

type refusedListener struct {
	net.Listener
	mu    sync.Mutex
	conns []*refusedConn
}

func (l *refusedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	dc := &refusedConn{Conn: c}
	l.mu.Lock()
	l.conns = append(l.conns, dc)
	l.mu.Unlock()
	return dc, nil
}

// A request refused for its Host is bounded like any other: its answer has a write deadline, and
// the connection ends with the answer instead of waiting for a body that may never come.
func TestARefusedHostIsBounded(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &refusedListener{Listener: inner}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, l, Config{API: http.NotFoundHandler()}) }()
	defer func() {
		cancel()
		<-done
	}()
	for _, body := range []string{"", "Content-Length: 100\r\n", "Transfer-Encoding: chunked\r\n"} {
		t.Run(fmt.Sprintf("body=%q", body), func(t *testing.T) {
			c, err := net.Dial("tcp", inner.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			fmt.Fprintf(c, "POST /v1/push HTTP/1.1\r\nHost: foreign.invalid\r\n%s\r\n", body)
			_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
			br := bufio.NewReader(c)
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != http.StatusMisdirectedRequest {
				t.Fatalf("status %d", resp.StatusCode)
			}
			// The server closes the connection instead of reading on.
			if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatalf("the connection stayed open after the refusal: %v", err)
			}
		})
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, c := range l.conns {
		c.mu.Lock()
		set := false
		for _, d := range c.writes {
			if !d.IsZero() {
				set = true
			}
		}
		c.mu.Unlock()
		if !set {
			t.Errorf("refused request %d was answered without a write deadline", i)
		}
	}
}
