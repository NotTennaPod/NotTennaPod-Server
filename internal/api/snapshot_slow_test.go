package api

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/httpserver"
)

// A healthy slow snapshot download (a client reading 8 KiB/s) is not cut by the
// ordinary response deadline. It runs over two minutes.

type fixSnapshotListener struct {
	c      net.Conn
	once   sync.Once
	closed chan struct{}
}

func (l *fixSnapshotListener) Accept() (net.Conn, error) {
	var c net.Conn
	l.once.Do(func() { c = l.c })
	if c != nil {
		return c, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *fixSnapshotListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (l *fixSnapshotListener) Addr() net.Addr { return l.c.LocalAddr() }

func TestHealthySlowSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("runs over two minutes")
	}
	e := newEnv(t, withSync(t))
	e.setupAdmin("owner", "correct horse battery")
	p := e.phoneOf("owner", "correct horse battery")
	for i := int64(1); i <= 5; i++ {
		p.push(op{seq: i, fact: p.noteFact(strings.Repeat("a", 200<<10))}).must(200)
	}
	a, b := net.Pipe()
	ln := &fixSnapshotListener{c: a, closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- httpserver.Serve(ctx, ln, httpserver.Config{API: e.srv, Hosts: httpserver.NewHosts([]string{"server.test"}, nil)})
	}()
	defer func() { b.Close(); cancel(); <-done }()
	body := fmt.Sprintf(`{"mode":"attach","install_id":%q,"acked_install_seq":0}`, p.install)
	req, _ := http.NewRequest("POST", "http://server.test/v1/snapshot", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	p.cred(req)
	if err := req.Write(b); err != nil {
		t.Fatal(err)
	}
	res, err := http.ReadResponse(bufio.NewReader(b), req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		data, _ := io.ReadAll(res.Body)
		t.Fatalf("%d %s", res.StatusCode, data)
	}
	start := time.Now()
	var n int64
	buf := make([]byte, 4096)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		<-tick.C
		got, e := res.Body.Read(buf)
		n += int64(got)
		if e != nil {
			err = e
			break
		}
		if time.Since(start) > 160*time.Second {
			t.Fatal("fixture read overlong")
		}
	}
	t.Logf("continuous reader received %d bytes in %v: %v", n, time.Since(start), err)
	if err != io.EOF {
		t.Errorf("healthy slow snapshot truncated by ordinary absolute write timeout: %v", err)
	}
}
