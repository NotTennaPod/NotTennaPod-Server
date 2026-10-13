package httpserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// serve runs Serve on a loopback listener until the test ends.
func serve(t *testing.T, cfg Config) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, ln, cfg) }()
	t.Cleanup(func() { cancel(); <-done })
	return ln.Addr().String()
}

// A request body that stalls after its headers hits its read deadline; a body
// read in full leaves the handler's context alone however long the handler then
// runs.
func TestRequestBodiesHaveADeadline(t *testing.T) {
	old := bodyGrace
	bodyGrace = 200 * time.Millisecond
	t.Cleanup(func() { bodyGrace = old })
	read := make(chan error, 1)
	survived := make(chan bool, 1)
	addr := serve(t, Config{API: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		if r.URL.Path == "/v1/stalled" {
			read <- err
			return
		}
		select {
		case <-r.Context().Done():
			survived <- false
		case <-time.After(3 * bodyGrace):
			survived <- true
		}
	})})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "POST /v1/stalled HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{")
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("a stalled body read without an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled body was held past its deadline")
	}
	res, err := http.Post("http://"+addr+"/v1/whole", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if !<-survived {
		t.Fatal("the body's deadline cancelled the handler after the body was read")
	}
}

type deadlineConn struct {
	net.Conn
	mu        sync.Mutex
	deadlines []time.Time
	wrote     chan struct{}
	once      sync.Once
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.wrote) })
	return c.Conn.Write(p)
}

func (c *deadlineConn) SetWriteDeadline(d time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, d)
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(d)
}

func (c *deadlineConn) last() (time.Time, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.deadlines) == 0 {
		return time.Time{}, 0
	}
	return c.deadlines[len(c.deadlines)-1], len(c.deadlines)
}

type oneConn struct {
	c      net.Conn
	once   sync.Once
	closed chan struct{}
}

func (l *oneConn) Accept() (net.Conn, error) {
	var out net.Conn
	l.once.Do(func() { out = l.c })
	if out != nil {
		return out, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *oneConn) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *oneConn) Addr() net.Addr { return l.c.LocalAddr() }

// An ordinary response gets a write deadline when it starts, so a client that
// stops reading cannot hold the handler; a handler that sets its own deadline
// (a stream) keeps it.
func TestResponsesHaveAWriteDeadline(t *testing.T) {
	for _, own := range []bool{false, true} {
		a, b := net.Pipe()
		c := &deadlineConn{Conn: a, wrote: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		handled := make(chan struct{})
		far := time.Now().Add(time.Hour)
		go func() {
			done <- Serve(ctx, &oneConn{c: c, closed: make(chan struct{})}, Config{API: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handled)
				if own {
					http.NewResponseController(w).SetWriteDeadline(far)
				}
				io.WriteString(w, strings.Repeat("x", 64<<10))
			})})
		}()
		fmt.Fprint(b, "GET /v1/large HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
		<-c.wrote
		d, n := c.last()
		switch {
		case n == 0:
			t.Errorf("own=%v: the response was written without a deadline", own)
		case own && !d.Equal(far):
			t.Errorf("the handler's own deadline was replaced by %v", d)
		case !own && (d.Before(time.Now()) || d.After(time.Now().Add(WriteTimeout))):
			t.Errorf("an ordinary response's deadline %v", d)
		}
		b.Close()
		<-handled
		cancel()
		<-done
	}
}

// A request for a Host the server does not answer is
// refused with 421, naming --allow-host, before routing or authentication;
// loopback names, configured names and Tailscale's names are answered.
func TestOnlyKnownHostsAreAnswered(t *testing.T) {
	var reached, asked atomic.Int32
	tailscale := func(context.Context) ([]netip.Addr, string, error) {
		asked.Add(1)
		return []netip.Addr{netip.MustParseAddr("100.101.102.103")}, "machine.tailnet.ts.net.", nil
	}
	hosts := NewHosts([]string{"nas.example:8080", "MacBook.tailnet.ts.net", "[fd7a:115c:a1e0::1]"}, tailscale)
	addr := serve(t, Config{Hosts: hosts, API: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	})})
	ask := func(host string) (int, string) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		fmt.Fprintf(conn, "POST /v1/auth/web HTTP/1.1\r\nHost: %s\r\nOrigin: http://%s\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}", host, host)
		res, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}
	_, port, _ := net.SplitHostPort(addr)
	for _, host := range []string{"127.0.0.1:" + port, "localhost:" + port, "LOCALHOST", "[::1]:" + port, "127.0.0.2",
		"nas.example", "nas.example.:9", "macbook.tailnet.ts.net", "[fd7a:115c:a1e0::1]:443",
		"machine.tailnet.ts.net:" + port, "100.101.102.103"} {
		if status, _ := ask(host); status != http.StatusUnauthorized {
			t.Errorf("Host %s: %d", host, status)
		}
	}
	before := reached.Load()
	for _, host := range []string{"rebind.attacker.test", "rebind.attacker.test:" + port, "192.0.2.1", "nas.example.evil", ""} {
		status, body := ask(host)
		if status != http.StatusMisdirectedRequest || !strings.Contains(body, "--allow-host") {
			t.Errorf("Host %q: %d %s", host, status, body)
		}
	}
	if reached.Load() != before {
		t.Error("a request for an unknown host reached the API")
	}
	if n := asked.Load(); n != 1 {
		t.Errorf("Tailscale was asked %d times within its recheck interval", n)
	}
}
