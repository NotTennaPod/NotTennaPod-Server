package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/acceptancecheck"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// --dev-clock-offset moves the server's one wall clock (PROTOCOL §16), through
// the real serve and its HTTP API, across restarts on one data directory:
//   - three days ahead: a warning at start; the receive time, so time
//     normalization, is the moved clock's (a client time past it is clamped
//     to it), a refresh is attempted at the moved clock's time, the store
//     records the moved clock's time (an installation's), the read API's
//     "now" is the moved clock's, and the daily backup's next slot is the
//     moved clock's;
//   - the backward-clock case: S accepts A with its clock three days ahead,
//     and A's author's action clock is three days ahead too, so A's
//     normalized time is in the future. S restarts with its clock corrected
//     (a backward jump), and the test confirms that A's normalized time is
//     later than the corrected clock. P, its action clock reset, observes A
//     and makes B (it starts listening to one of A's podcast's episodes),
//     which depends on A. B's normalized time is A's, later than B's receive
//     time, and so are the episode's history and last-played times, by which
//     History and the statistics month go;
//   - three days behind: a client time past the receive time is clamped to
//     the moved clock's, and the backup schedule follows the jump.
//
// Afterwards the witness passes on the data directory.
func TestDevClockOffsetMovesTheServersClock(t *testing.T) {
	logs := captureLog(t)
	data := filepath.Join(t.TempDir(), "data")
	const days3 = 72 * time.Hour
	ms := func(d time.Duration) int64 { return time.Now().Add(d).UnixMilli() }

	// The server's clock three days ahead.
	srv := startServe(t, data, "--dev-clock-offset", "72h")
	if n := strings.Count(logs.String(), "--dev-clock-offset"); n != 1 || !strings.Contains(logs.String(), "offset=72h0m0s") {
		t.Errorf("the start's warning (%d):\n%s", n, logs.String())
	}
	if due := backupDue(t, data); due <= ms(days3) || due > ms(days3+25*time.Hour) {
		t.Errorf("the next daily backup at %v, want the clock three days ahead's next quiet hour", time.UnixMilli(due))
	}
	before := ms(days3)
	p := srv.phone(t)
	var created float64
	for _, i := range srv.get(t, "/v1/installations", p.token, "")["installations"].([]any) {
		if i := i.(map[string]any); i["install_id"] == p.install {
			created, _ = i["created_at"].(float64)
		}
	}
	if int64(created) < before || int64(created) > ms(days3) {
		t.Errorf("the phone's installation was created at %v, want the moved clock's time, after %d", created, before)
	}
	// With nothing played, the statistics' oldest date is the read API's now.
	before = ms(days3)
	oldest, _ := srv.get(t, "/v1/read/statistics", "", p.session)["oldest_date"].(float64)
	if int64(oldest) < before || int64(oldest) > ms(days3) {
		t.Errorf("the statistics' oldest date %v, want the moved clock's now, after %d", oldest, before)
	}
	before = ms(days3)
	far := p.push(t, srv, p.fact("podcast.subscribe", nil, nil, ms(100*time.Hour), map[string]any{"feed_url": "http://127.0.0.1:9/far.xml"}))
	if after := ms(days3); far.normalized < before || far.normalized > after {
		t.Errorf("a client time 100 h ahead normalized to %v, want the receive time, within [%v, %v]", far.normalized, before, after)
	}
	aTime := ms(days3)
	a := p.push(t, srv, p.fact("podcast.subscribe", nil, nil, aTime, map[string]any{"feed_url": "http://127.0.0.1:9/a.xml"}))
	if a.normalized != aTime {
		t.Fatalf("A normalized to %d, want its client time %d", a.normalized, aTime)
	}
	// Every attempt to refresh A's podcast (asked for here, or its first
	// population's) follows A's acceptance.
	podcast := contract.PodcastID(p.namespace, a.op)
	srv.do(t, "/v1/refresh", map[string]any{"podcast_id": podcast}, map[string]string{"Authorization": "Bearer " + p.token})
	status := p.record(t, srv, "podcast", podcast, "refresh_status") // its feed is refused: a failure, published
	if attempt, _ := status["last_attempt"].(float64); int64(attempt) < aTime || int64(attempt) > ms(days3) {
		t.Errorf("the refresh status %v, want an attempt at the moved clock's time, after %d", status, aTime)
	}
	srv.stop(t)

	// Restarted with the clock corrected: a backward jump.
	srv = startServe(t, data)
	if n := strings.Count(logs.String(), "--dev-clock-offset"); n != 1 {
		t.Errorf("a start without an offset warned (%d)", n)
	}
	if now := ms(0); a.normalized <= now {
		t.Fatalf("A's normalized time %d is not later than the corrected clock %d", a.normalized, now)
	}
	if due := backupDue(t, data); due <= ms(0) || due > ms(25*time.Hour) {
		t.Errorf("after the clock went back the next daily backup is at %v, want within a day", time.UnixMilli(due))
	}
	generation := contract.GenerationID(p.namespace, podcast, a.op)
	episode, err := contract.EpisodeID(p.namespace, podcast, contract.KindRSSGUID, "one")
	if err != nil {
		t.Fatal(err)
	}
	ref := map[string]any{"episode_id": episode, "podcast_id": podcast, "generation_id": generation,
		"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": "one", "guid": "one", "title": "One"}}
	bFact := p.fact("session.start", []string{a.op}, []any{map[string]any{"entity": "episode", "id": episode, "generation": generation}}, ms(0),
		map[string]any{"episode": ref, "session_id": "00000000-0000-4000-8000-0000000000b0", "start_position": 0, "activity": true})
	if deps, _ := bFact["dependencies"].([]any); len(deps) != 1 || deps[0] != a.op {
		t.Fatalf("B's dependencies %v, want A", bFact["dependencies"])
	}
	b := p.push(t, srv, bFact)
	if received := ms(0); b.normalized != a.normalized || b.normalized <= received {
		t.Errorf("B normalized to %d, want A's %d, later than its receive time (at most %d)", b.normalized, a.normalized, received)
	}
	srv.stop(t)
	records := readRecords(t, data, p.account)
	for _, group := range []string{"last_played", "history"} {
		want := fmt.Sprintf(`{"generation_id":"%s","time":%d}`, generation, a.normalized)
		if got := records["episode/"+episode+"/"+group]; got != want {
			t.Errorf("%s = %s, want %s", group, got, want)
		}
	}

	// The clock three days behind.
	srv = startServe(t, data, "--dev-clock-offset=-72h")
	before = ms(-days3)
	c := p.push(t, srv, p.fact("podcast.subscribe", nil, nil, ms(0), map[string]any{"feed_url": "http://127.0.0.1:9/c.xml"}))
	if after := ms(-days3); c.normalized < before || c.normalized > after {
		t.Errorf("a client time now normalized to %v, want the receive time three days behind, within [%v, %v]", c.normalized, before, after)
	}
	if due := backupDue(t, data); due <= ms(-days3) || due > ms(-days3+25*time.Hour) {
		t.Errorf("with the clock three days behind the next daily backup is at %v", time.UnixMilli(due))
	}
	srv.stop(t)
	if n := strings.Count(logs.String(), "offset=-72h0m0s"); n != 1 {
		t.Errorf("the third start's warning (%d)", n)
	}

	acceptancecheck.Check(t, data)
}

// ---- the real server, in this process

// syncBuffer collects the server's log.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// captureLog sends the default logger, which serve logs to, to a buffer.
func captureLog(t *testing.T) *syncBuffer {
	buf := &syncBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

type server struct {
	base   string
	data   string
	cancel context.CancelFunc
	done   chan int
	client *http.Client
}

// startServe runs serve on data and waits until it answers.
func startServe(t *testing.T, data string, args ...string) *server {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	s := &server{base: "http://" + addr, data: data, cancel: cancel, done: make(chan int, 1), client: &http.Client{Timeout: time.Minute}}
	// No ffmpeg: the media checks would only load the machine.
	args = append([]string{"--data-dir", data, "--listen", addr, "--ffmpeg", "/nonexistent/ffmpeg", "--ffprobe", "/nonexistent/ffprobe"}, args...)
	go func() { s.done <- serve(ctx, args) }()
	t.Cleanup(func() { s.stopped() })
	deadline := time.Now().Add(2 * time.Minute)
	for {
		select {
		case code := <-s.done:
			s.done <- code
			t.Fatalf("serve %v exited with %d before answering", args, code)
		default:
		}
		if res, err := s.client.Get(s.base + "/v1/info"); err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return s
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve %v never answered", args)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stopped stops the server as SIGTERM does and returns its exit code.
func (s *server) stopped() int {
	s.cancel()
	code := <-s.done
	s.done <- code
	return code
}

func (s *server) stop(t *testing.T) {
	t.Helper()
	if code := s.stopped(); code != 0 {
		t.Fatalf("serve exited with %d", code)
	}
}

func (s *server) do(t *testing.T, path string, body any, header map[string]string) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, s.base+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	return s.send(t, req)
}

// get asks as the phone (token) or as the administrator's browser (session).
func (s *server) get(t *testing.T, path, token, session string) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, s.base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if session != "" {
		req.AddCookie(&http.Cookie{Name: "ntp_session", Value: session})
	}
	return s.send(t, req)
}

func (s *server) send(t *testing.T, req *http.Request) map[string]any {
	t.Helper()
	path := req.URL.Path
	res, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	if res.StatusCode/100 != 2 {
		t.Fatalf("%s %s: %d %s", req.Method, path, res.StatusCode, out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// phone is a device signed in to the server's administrator account.
type phone struct {
	session                            string // the administrator's browser session, from setup
	token, install, account, namespace string
	attached, cursor                   string
	watermark, seq                     int64
	last                               string
}

// phone sets the server up, signs a phone in and attaches it.
func (s *server) phone(t *testing.T) *phone {
	t.Helper()
	code, err := os.ReadFile(filepath.Join(s.data, "setup-code"))
	if err != nil {
		t.Fatal(err)
	}
	p := &phone{install: "00000000-0000-4000-8000-0000000000a1"}
	raw, _ := json.Marshal(map[string]any{"username": "owner", "password": "a long enough password", "setup_code": strings.TrimSpace(string(code))})
	req, err := http.NewRequest(http.MethodPost, s.base+"/v1/setup", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", s.base)
	res, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	for _, c := range res.Cookies() {
		// The session's own cookie (PROTOCOL §5.2); the tests send it back as
		// a session from an earlier server, in ntp_session, which works alike.
		if strings.HasPrefix(c.Name, "ntp_s_") {
			p.session = c.Value
		}
	}
	if res.StatusCode != http.StatusCreated || p.session == "" {
		t.Fatalf("setup: %d, session %q", res.StatusCode, p.session)
	}
	s.do(t, "/v1/installations", map[string]any{"install_id": "00000000-0000-4000-8000-0000000000c1", "instance_nonce": "00000000-0000-4000-8000-0000000000c2",
		"name": "Firefox", "app_version": "1.0", "capabilities": []string{"queue"}}, map[string]string{"Origin": s.base, "Cookie": "ntp_session=" + p.session})
	r := s.do(t, "/v1/auth/device", map[string]any{"username": "owner", "password": "a long enough password", "install_id": p.install,
		"instance_nonce": "00000000-0000-4000-8000-0000000000a2", "device": map[string]any{"name": "iPhone", "platform": "ios", "app_version": "1.0",
			"capabilities": []string{"favorites", "history", "inbox", "queue", "statistics"}}}, nil)
	p.token, _ = r["token"].(string)
	acct, _ := r["account"].(map[string]any)
	p.account, _ = acct["account_id"].(string)
	p.namespace, _ = acct["namespace"].(string)
	page := s.do(t, "/v1/snapshot", map[string]any{"mode": "attach", "install_id": p.install, "acked_install_seq": 0},
		map[string]string{"Authorization": "Bearer " + p.token})
	p.cursor, _ = page["head"].(string)
	p.attached = p.cursor
	if p.token == "" || p.namespace == "" || p.cursor == "" || page["has_more"] != false {
		t.Fatalf("sign-in %v, attachment %v", r, page)
	}
	return p
}

// fact is the phone's next fact, at the phone's action clock's clientTime,
// following its previous one.
func (p *phone) fact(typ string, deps []string, targets []any, clientTime int64, payload map[string]any) map[string]any {
	p.seq++
	op := fmt.Sprintf("00000000-0000-4000-8000-%012x", 0xf000+p.seq)
	list := []any{}
	for _, d := range deps {
		list = append(list, d)
	}
	if targets == nil {
		targets = []any{}
	}
	f := map[string]any{"op_id": op, "author_id": p.install, "author_seq": p.seq, "dependencies": list, "type": typ, "type_version": 1,
		"client_time": clientTime, "targets": targets, "payload": payload}
	if p.last != "" {
		f["previous_op_id"] = p.last
	}
	p.last = op
	return f
}

type accepted struct {
	op         string
	normalized int64
}

// push sends one fact and expects it applied.
func (p *phone) push(t *testing.T, s *server, f map[string]any) accepted {
	t.Helper()
	r := s.do(t, "/v1/push", map[string]any{"cursor": p.cursor, "install_id": p.install, "acked_install_seq": p.watermark,
		"operations": []any{map[string]any{"install_seq": f["author_seq"], "fact": f}}}, map[string]string{"Authorization": "Bearer " + p.token})
	results, _ := r["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("push: %v", r)
	}
	receipt, _ := results[0].(map[string]any)["receipt"].(map[string]any)
	if receipt["outcome"] != "applied" {
		t.Fatalf("%s: %v", f["type"], results[0])
	}
	p.cursor, _ = r["head"].(string)
	w, _ := r["install_watermark"].(float64)
	p.watermark = int64(w)
	n, _ := receipt["normalized_time"].(float64)
	return accepted{op: f["op_id"].(string), normalized: int64(n)}
}

// record waits until a published record of the account appears in what the
// phone pulls from its attachment, and returns its value.
func (p *phone) record(t *testing.T, s *server, entity, id, group string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		body := map[string]any{"cursor": p.attached, "install_id": p.install, "acked_install_seq": p.watermark}
		for {
			page := s.do(t, "/v1/pull", body, map[string]string{"Authorization": "Bearer " + p.token})
			items, _ := page["items"].([]any)
			for _, it := range items {
				commit, _ := it.(map[string]any)["commit"].(map[string]any)
				records, _ := commit["records"].([]any)
				for _, r := range records {
					rec, _ := r.(map[string]any)
					if rec["entity"] == entity && rec["id"] == id && rec["field_group"] == group {
						value, _ := rec["value"].(map[string]any)
						return value
					}
				}
			}
			if page["has_more"] != true {
				break
			}
			body["page"] = page["next"]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s/%s/%s record was published", entity, id, group)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// backupDue is the daily backup's next due time, as the schedule file of a
// running server says.
func backupDue(t *testing.T, data string) int64 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(data, "backup-schedule.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		NextDue int64 `json:"next_due"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	return st.NextDue
}

// readRecords reads an account's published records from a stopped server's
// data directory.
func readRecords(t *testing.T, data, account string) map[string]string {
	t.Helper()
	store, err := storage.Open(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	out := map[string]string{}
	if err := store.Read(context.Background(), func(r *storage.ReadTx) error {
		rows, err := r.Account(account).Records(nil)
		for _, x := range rows {
			out[x.Entity+"/"+x.EntityID+"/"+x.FieldGroup] = x.Value
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
