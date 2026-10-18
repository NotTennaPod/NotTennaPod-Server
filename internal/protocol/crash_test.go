package protocol

// Crash safety (scenario 2, PROTOCOL §16: "The server restarts
// in the middle of a push; the phone retries; no operation is applied twice or
// lost"). A child process (this test binary re-executed) runs a Service on the
// data directory and answers push requests from the parent, which plays the
// phone: it keeps an outbox and retries until every slot is acknowledged. The
// child is killed with SIGKILL at random moments and while paused at the
// Service's beforeCommit hook inside the push transaction.

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// Every write transaction of the package's tests is audited for the read
// model's change capture (storage.AuditViewCaptureForTesting), and every
// commit of the tests (not of their child processes) is checked by the read
// model's commit observer (viewstest.Main).
func TestMain(m *testing.M) {
	storage.AuditViewCaptureForTesting()
	if os.Getenv("NTP_PROTOCOL_CHILD") == "push-server" {
		runPushServerChild()
		os.Exit(0)
	}
	os.Exit(viewstest.Main(m))
}

func TestReadModelObserverActive(t *testing.T) { viewstest.RequireActive(t) }

func childFail(err error) {
	fmt.Println("error", strings.ReplaceAll(err.Error(), "\n", " "))
	os.Exit(1)
}

// runPushServerChild serves one push per stdin line and answers on stdout.
func runPushServerChild() {
	ctx := context.Background()
	store, err := storage.Open(ctx, os.Getenv("NTP_PROTOCOL_DIR"))
	if err != nil {
		childFail(err)
	}
	account, install := os.Getenv("NTP_PROTOCOL_ACCOUNT"), os.Getenv("NTP_PROTOCOL_INSTALL")
	svc := &Service{Store: store, Registry: acceptanceRegistry(), Now: time.Now}
	if n, _ := strconv.Atoi(os.Getenv("NTP_PAUSE_AFTER")); n > 0 {
		calls := 0
		svc.beforeCommit = func() error {
			if calls++; calls == n {
				fmt.Println("paused")
				for {
					time.Sleep(time.Hour)
				}
			}
			return nil
		}
	}
	fmt.Println("ready")
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	for in.Scan() {
		resp, err := serveChildPush(ctx, svc, account, install, in.Bytes())
		if err != nil {
			fmt.Println("fail", strings.ReplaceAll(err.Error(), "\n", " "))
			continue
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		enc.Encode(resp)
		fmt.Printf("resp %s", buf.Bytes())
	}
}

func serveChildPush(ctx context.Context, svc *Service, account, install string, line []byte) (*PushResponse, error) {
	v, err := contract.DecodeJSON(line)
	if err != nil {
		return nil, err
	}
	req := v.(map[string]any)
	var ops []PushOp
	for _, o := range req["ops"].([]any) {
		m := o.(map[string]any)
		op := PushOp{InstallSeq: num(m["install_seq"])}
		if f, ok := m["fact"]; ok {
			if op.Fact, err = ParseFact(f); err != nil {
				return nil, err
			}
		} else if op.Part, err = DecodePart(m["part"].(map[string]any)); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	var head storage.Head
	if err := svc.Store.Read(ctx, func(r *storage.ReadTx) error {
		head, err = r.Account(account).Head()
		return err
	}); err != nil {
		return nil, err
	}
	genesis := contract.Cursor{LogID: head.LogID, Epoch: head.Epoch, Seq: 0, Digest: contract.GenesisDigest}
	return svc.Push(ctx, PushRequest{AccountID: account, Cursor: genesis, InstallID: install, AckedInstallSeq: num(req["acked"]),
		Operations: ops}, authorizer(account, install))
}

// ---- the parent: a phone with an outbox

type crashSlot struct {
	op    string
	obj   map[string]any
	parts []map[string]any // framed slots
}

type crashClient struct {
	took     time.Duration // smoothed duration of one push
	slots    []crashSlot
	acked    int64
	present  map[int64]map[int]bool
	receipts map[int64][]byte
	rng      *rand.Rand
}

func (c *crashClient) next() []byte {
	var ops []any
	size := 1 + c.rng.IntN(3)
	for s := c.acked + 1; s <= int64(len(c.slots)) && len(ops) < size; s++ {
		sl := c.slots[s-1]
		if sl.parts == nil {
			ops = append(ops, map[string]any{"install_seq": s, "fact": sl.obj})
			continue
		}
		idx := -1
		for i := range sl.parts {
			if !c.present[s][i] {
				idx = i
				break
			}
		}
		if idx < 0 || c.rng.IntN(5) == 0 {
			idx = c.rng.IntN(len(sl.parts)) // a resend
		}
		ops = append(ops, map[string]any{"install_seq": s, "part": sl.parts[idx]})
		break
	}
	return append(canon(map[string]any{"acked": c.acked, "ops": ops}), '\n')
}

func (c *crashClient) process(t *testing.T, line string) {
	var resp struct {
		InstallWatermark int64 `json:"install_watermark"`
		Results          []struct {
			InstallSeq   int64           `json:"install_seq"`
			Status       string          `json:"status"`
			Receipt      json.RawMessage `json:"receipt"`
			PartsPresent []int           `json:"parts_present"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("response %q: %v", line, err)
	}
	if resp.InstallWatermark < c.acked {
		t.Errorf("the server's watermark went back from %d to %d: an acknowledged slot was lost", c.acked, resp.InstallWatermark)
	}
	for _, r := range resp.Results {
		switch r.Status {
		case "accepted", "duplicate":
			v, err := contract.DecodeJSON(r.Receipt)
			if err != nil {
				t.Fatalf("receipt: %v", err)
			}
			b := canon(v)
			if prev, ok := c.receipts[r.InstallSeq]; ok && !bytes.Equal(prev, b) {
				t.Errorf("slot %d: the receipt changed between responses", r.InstallSeq)
			}
			c.receipts[r.InstallSeq] = b
		case "part_stored":
			c.present[r.InstallSeq] = map[int]bool{}
			for _, p := range r.PartsPresent {
				c.present[r.InstallSeq][p] = true
			}
		default:
			t.Errorf("unexpected result %+v", r)
		}
	}
	c.acked = max(c.acked, resp.InstallWatermark)
}

type pushChild struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	lines chan string
}

func startPushChild(t *testing.T, dir, account, install string, env ...string) *pushChild {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), append(env, "NTP_PROTOCOL_CHILD=push-server", "NTP_PROTOCOL_DIR="+dir,
		"NTP_PROTOCOL_ACCOUNT="+account, "NTP_PROTOCOL_INSTALL="+install)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &pushChild{cmd: cmd, stdin: stdin, lines: make(chan string, 64)}
	go func() {
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 1<<20), 64<<20)
		for sc.Scan() {
			c.lines <- sc.Text()
		}
		close(c.lines)
	}()
	select {
	case line, ok := <-c.lines:
		if !ok || line != "ready" {
			c.kill()
			t.Fatalf("child did not start: %q", line)
		}
	case <-time.After(60 * time.Second):
		c.kill()
		t.Fatal("child start timed out")
	}
	return c
}

func (c *pushChild) kill() []string {
	c.cmd.Process.Kill()
	c.cmd.Wait()
	var rest []string
	for l := range c.lines {
		rest = append(rest, l)
	}
	return rest
}

// drive sends up to `requests` requests one at a time (0 = until the outbox is
// acknowledged), waiting for each response. If killWithin > 0 the last request
// is abandoned after a random delay below killWithin (its response is lost).
// It reports whether the child paused inside a push transaction and whether a
// request was in flight when driving stopped.
func (c *crashClient) drive(t *testing.T, ch *pushChild, requests int, killWithin time.Duration) (paused, inflight bool) {
	for sent := 0; c.acked < int64(len(c.slots)) && (requests == 0 || sent < requests); sent++ {
		if _, err := ch.stdin.Write(c.next()); err != nil {
			return false, false
		}
		var deadline <-chan time.Time
		if killWithin > 0 && sent == requests-1 {
			deadline = time.After(time.Duration(c.rng.Int64N(int64(killWithin))))
		}
		start := time.Now()
		select {
		case line, ok := <-ch.lines:
			switch {
			case !ok:
				t.Fatal("the child exited")
			case line == "paused":
				return true, true
			case strings.HasPrefix(line, "resp "):
				c.process(t, line[5:])
				c.took = (c.took*3 + time.Since(start)) / 4
			case strings.HasPrefix(line, "fail "):
				// A transient failure: nothing was consumed; the client retries.
			default:
				t.Fatalf("child: %s", line)
			}
		case <-deadline:
			return false, true
		}
	}
	return false, false
}

// committedState is what a reader sees of the data directory: the head, the
// watermark and the stored parts.
func committedState(t *testing.T, dir, account, install string) string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "nottennapod.db")+"?_pragma=busy_timeout(10000)&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var head, consumed, parts int64
	var digest []byte
	if err := db.QueryRow(`SELECT head_seq, head_digest FROM account_logs WHERE account_id = ?`, account).Scan(&head, &digest); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(`SELECT consumed_seq FROM installations WHERE account_id = ? AND install_id = ?`, account, install).Scan(&consumed)
	db.QueryRow(`SELECT COUNT(*) FROM push_parts`).Scan(&parts)
	return fmt.Sprintf("head %d %x consumed %d parts %d", head, digest, consumed, parts)
}

func TestCrashDuringPushAppliesEveryOperationExactlyOnce(t *testing.T) {
	h := newHarness(t)
	in := h.addInstall()
	dir, account := h.dir, h.account
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, 7))
	const n = 80
	client := &crashClient{present: map[int64]map[int]bool{}, receipts: map[int64][]byte{}, rng: rng}
	prev := ""
	for i := 1; i <= n; i++ {
		op := uid(0x77, i)
		f := fs{op: op, author: h.author, seq: int64(i), prev: prev, ct: int64(1_700_000_000_000 + i)}
		if i > 2 {
			f.deps = []string{uid(0x77, 1+rng.IntN(i-1))}
		}
		switch i {
		case 11, 29, 61:
			f.payload = map[string]any{"invalid": i} // rejected, still consumed once
		case 17:
			f.typ, f.payload = "test.decide", text("crash-subject-"+randomID())
		case 7, 23, 47:
			f.payload = text(strings.Repeat("framed ", 120))
		}
		s := crashSlot{op: op, obj: f.object()}
		if i == 7 || i == 23 || i == 47 {
			s.parts = partObjects(s.obj, len(canon(s.obj))/3+1, randomID())
		}
		client.slots = append(client.slots, s)
		prev = op
	}
	h.store.Close() // the child owns the data directory from here

	check := func() {
		t.Helper()
		store, err := storage.Open(context.Background(), dir)
		if err != nil {
			t.Fatalf("reopen after a kill: %v", err)
		}
		defer store.Close()
		if err := store.VerifyChain(context.Background(), account); err != nil {
			t.Fatalf("chain after a kill: %v", err)
		}
		var consumed int64
		store.Read(context.Background(), func(r *storage.ReadTx) error {
			consumed, err = r.Account(account).ConsumedSeq(in)
			return err
		})
		if consumed < client.acked {
			t.Fatalf("acknowledged slot lost: consumed %d, acknowledged %d", consumed, client.acked)
		}
	}
	random, synchronized := 18, 6
	if testing.Short() {
		random, synchronized = 4, 2
	}
	client.took = 20 * time.Millisecond
	interrupted, pauses := 0, 0
	step := (random + synchronized) / synchronized
	for i := 0; i < random+synchronized; i++ {
		if i%step != step-1 || pauses >= synchronized {
			// A kill at a random moment: some requests complete, the last one is
			// abandoned after a random delay around one push's duration.
			ch := startPushChild(t, dir, account, in)
			_, inflight := client.drive(t, ch, 1+rng.IntN(2), 2*client.took)
			for _, l := range ch.kill() {
				if strings.HasPrefix(l, "resp ") {
					client.process(t, l[5:]) // it arrived just before the kill
					inflight = false
				}
			}
			if inflight {
				interrupted++
			}
			check()
			continue
		}
		// A kill inside the push transaction, at the beforeCommit hook.
		ch := startPushChild(t, dir, account, in, "NTP_PAUSE_AFTER="+strconv.Itoa(1+rng.IntN(2)))
		paused, _ := client.drive(t, ch, 0, 0)
		if !paused {
			ch.kill()
			t.Fatal("the outbox was acknowledged before the pause point")
		}
		pauses++
		before := committedState(t, dir, account, in)
		ch.kill()
		if after := committedState(t, dir, account, in); after != before {
			t.Fatalf("a push killed inside its transaction left effects:\n before %s\n after  %s", before, after)
		}
		check()
	}
	// The phone keeps retrying until everything is acknowledged.
	ch := startPushChild(t, dir, account, in)
	client.drive(t, ch, 0, 0)
	ch.kill()
	if client.acked != n {
		t.Fatalf("only %d of %d slots acknowledged", client.acked, n)
	}

	store, err := storage.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	h.store = store
	h.startService()
	h.verifyLog(account)
	// Every slot consumed once, in order, with its own operation.
	for i, s := range client.slots {
		var op, hash, result string
		if err := h.db().QueryRow(`SELECT op_id, content_hash, result FROM transport_slots WHERE install_id = ? AND install_seq = ?`, in, i+1).
			Scan(&op, &hash, &result); err != nil {
			t.Fatalf("slot %d: %v", i+1, err)
		}
		if op != s.op || hash != hashOf(s.obj) || result != "accepted" {
			t.Errorf("slot %d consumed %s %s %s, want %s accepted", i+1, op, hash, result, s.op)
		}
		// A watermark may acknowledge slots whose responses were lost (the phone
		// then gets their receipts from a pull); every receipt it did receive is
		// the archived one.
		_, receipt := h.archived(s.op)
		if held, ok := client.receipts[int64(i+1)]; ok && !bytes.Equal(receipt, held) {
			t.Errorf("slot %d: the receipt the phone holds differs from the archive", i+1)
		}
	}
	// Every operation applied exactly once: one first commit per fact (checked
	// by verifyLog), one record per applied note, one decision.
	records := map[string]int{}
	decisions := 0
	for _, c := range h.commitsOf(account) {
		v, _ := contract.DecodeJSON(c)
		commit := v.(map[string]any)
		for _, r := range commit["records"].([]any) {
			records[r.(map[string]any)["id"].(string)]++
		}
		decisions += len(commit["decisions"].([]any))
	}
	for i, s := range client.slots {
		want := 1
		if i+1 == 11 || i+1 == 29 || i+1 == 61 {
			want = 0
		}
		if records[s.op] != want {
			t.Errorf("slot %d's effect was applied %d times", i+1, records[s.op])
		}
	}
	if decisions != 1 {
		t.Errorf("the decision was made %d times", decisions)
	}
	t.Logf("%d random kills (%d with a request in flight) and %d kills inside the push transaction; %d commits", random,
		interrupted, pauses, h.head().Seq)
	if (interrupted == 0 && !testing.Short()) || pauses != synchronized {
		t.Errorf("the kills did not interrupt pushes (%d in flight, %d paused)", interrupted, pauses)
	}
}
