package storage

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// RunTestChild runs the child process NTP_STORAGE_CHILD names (the test
// binary re-executed by a crash test), if any, and reports whether it did;
// the TestMain (main_test.go) calls it.
func RunTestChild() bool {
	mode := os.Getenv("NTP_STORAGE_CHILD")
	if mode == "" {
		return false
	}
	runChild(mode, os.Getenv("NTP_STORAGE_DIR"))
	return true
}

// block reports what and never returns (sleeping, so the runtime's deadlock
// detector does not end the process before the parent kills it).
func block(what string) error {
	fmt.Println(what)
	for {
		time.Sleep(time.Hour)
	}
}

func runChild(mode, dir string) {
	ctx := context.Background()
	cfg := config{dataDir: dir}
	switch mode {
	case "capture":
		runCaptureChild(ctx, cfg)
	case "lock":
		s, err := open(ctx, cfg)
		if err != nil {
			fmt.Println("error", err)
			os.Exit(1)
		}
		defer s.Close()
		block("locked")
	case "migrate":
		cfg.migrations = withExtra(newMigration(nextName("extra"), "CREATE TABLE extra (x INTEGER) STRICT; INSERT INTO extra VALUES (1);"))
		cfg.hooks.duringMigration = func(v int) error {
			if v == len(embeddedMigrations)+1 {
				return block("migrating")
			}
			return nil
		}
		_, err := open(ctx, cfg)
		fmt.Println("error", err)
		os.Exit(1)
	case "append":
		s, err := open(ctx, cfg)
		if err != nil {
			fmt.Println("error", err)
			os.Exit(1)
		}
		// Hooks are installed after startup, so every pause interrupts an append.
		after, _ := strconv.Atoi(os.Getenv("NTP_PAUSE_AFTER"))
		calls := 0
		var current string // "<account> <seq>" of the append in progress
		hook := func() error {
			calls++
			if calls == after {
				return block("paused " + current)
			}
			return nil
		}
		switch os.Getenv("NTP_PAUSE") {
		case "afterLogInsert":
			s.hooks.afterLogInsert = hook
		case "afterHeadUpdate":
			s.hooks.afterHeadUpdate = hook
		case "beforeCommit":
			s.hooks.beforeCommit = hook
		}
		var ids []string
		err = s.Read(ctx, func(r *ReadTx) error { ids, err = r.AccountIDs(); return err })
		if err != nil {
			fmt.Println("error", err)
			os.Exit(1)
		}
		fmt.Println("ready")
		for i := 0; ; i++ {
			id := ids[i%len(ids)]
			var head Head
			if err := s.Read(ctx, func(r *ReadTx) error { head, err = r.Account(id).Head(); return err }); err != nil {
				fmt.Println("error", err)
				os.Exit(1)
			}
			current = fmt.Sprintf("%s %d", id, head.Seq+1)
			seq, err := appendWithCallerRow(ctx, s, id)
			if err != nil {
				fmt.Println("error", err)
				os.Exit(1)
			}
			fmt.Printf("ack %s %d\n", id, seq)
		}
	}
}

type child struct {
	cmd   *exec.Cmd
	lines chan string
}

func startChild(t *testing.T, mode, dir string, env ...string) *child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), append(env, "NTP_STORAGE_CHILD="+mode, "NTP_STORAGE_DIR="+dir)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &child{cmd: cmd, lines: make(chan string, 1024)}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			c.lines <- sc.Text()
		}
		close(c.lines)
	}()
	return c
}

// waitFor returns the lines read until one starts with prefix; it fails on
// "error" lines, on exit, or after a generous timeout.
func (c *child) waitFor(t *testing.T, prefix string) []string {
	t.Helper()
	var seen []string
	timeout := time.After(60 * time.Second)
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				t.Fatalf("child exited before %q; output %v", prefix, seen)
			}
			if strings.HasPrefix(line, "error") {
				t.Fatalf("child: %s", line)
			}
			seen = append(seen, line)
			if strings.HasPrefix(line, prefix) {
				return seen
			}
		case <-timeout:
			c.kill()
			t.Fatalf("timed out waiting for %q", prefix)
		}
	}
}

func (c *child) kill() []string {
	c.cmd.Process.Kill()
	c.cmd.Wait()
	var rest []string
	for line := range c.lines {
		rest = append(rest, line)
	}
	return rest
}

func acks(lines []string) map[string]int64 {
	out := map[string]int64{}
	for _, l := range lines {
		var id string
		var seq int64
		if n, _ := fmt.Sscanf(l, "ack %s %d", &id, &seq); n == 2 && seq > out[id] {
			out[id] = seq
		}
	}
	return out
}

// TestCrashSafety kills appending child processes with SIGKILL at random moments
// and at synchronized points inside the transaction, then reopens and checks that
// the database is intact, every chain verifies, every acknowledged commit is
// present and no effect of an unacknowledged transaction survived.
func TestCrashSafety(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s := openTest(t, config{dataDir: dir})
	createAccounts(t, s, 4)
	s.Close()

	random := 200
	synchronized := 10
	if testing.Short() {
		random, synchronized = 20, 2
	}
	r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	acked := map[string]int64{}
	check := func(lines []string) {
		for id, seq := range acks(lines) {
			if seq > acked[id] {
				acked[id] = seq
			}
		}
		s := openTest(t, config{dataDir: dir})
		heads := verifyStore(t, s)
		for id, seq := range acked {
			if heads[id] < seq {
				t.Fatalf("account %s: acknowledged seq %d lost (head %d)", id, seq, heads[id])
			}
		}
		s.Close()
	}
	for i := 0; i < random; i++ {
		c := startChild(t, "append", dir)
		lines := c.waitFor(t, "ready")
		time.Sleep(time.Duration(r.IntN(40)) * time.Millisecond) // the kill moment is the random input
		lines = append(lines, c.kill()...)
		check(lines)
	}
	for _, point := range []string{"afterLogInsert", "afterHeadUpdate", "beforeCommit"} {
		for i := 0; i < synchronized; i++ {
			c := startChild(t, "append", dir, "NTP_PAUSE="+point, "NTP_PAUSE_AFTER="+strconv.Itoa(1+r.IntN(5)))
			lines := c.waitFor(t, "paused")
			var id string
			var seq int64
			if n, _ := fmt.Sscanf(lines[len(lines)-1], "paused %s %d", &id, &seq); n != 2 {
				t.Fatalf("pause line %q", lines[len(lines)-1])
			}
			lines = append(lines, c.kill()...)
			check(lines)
			// The interrupted transaction left nothing: no commit at its seq and
			// (verifyStore) no caller row without a commit.
			s := openTest(t, config{dataDir: dir})
			if heads := verifyStore(t, s); heads[id] >= seq {
				t.Fatalf("%s: interrupted append at seq %d survived (head %d)", point, seq, heads[id])
			}
			s.Close()
		}
	}
	var total int64
	for _, seq := range acked {
		total += seq
	}
	t.Logf("%d random and %d synchronized kills; %d acknowledged commits survived", random, 3*synchronized, total)
}

func TestMigrationCrash(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	s := openTest(t, config{dataDir: dir})
	ids := createAccounts(t, s, 1)
	if _, err := appendWithCallerRow(ctx, s, ids[0]); err != nil {
		t.Fatal(err)
	}
	s.Close()
	c := startChild(t, "migrate", dir)
	c.waitFor(t, "migrating")
	c.kill()
	s = openTest(t, config{dataDir: dir})
	if v, _ := s.SchemaVersion(ctx); v != len(embeddedMigrations) {
		t.Fatalf("schema version %d after a killed migration", v)
	}
	if heads := verifyStore(t, s); heads[ids[0]] != 1 {
		t.Fatalf("data changed: %v", heads)
	}
	s.Close()
	extra := withExtra(newMigration(nextName("extra"), "CREATE TABLE extra (x INTEGER) STRICT; INSERT INTO extra VALUES (1);"))
	s = openTest(t, config{dataDir: dir, migrations: extra})
	var n int
	if err := s.reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM extra").Scan(&n); err != nil || n != 1 {
		t.Fatalf("migration did not apply cleanly: %d %v", n, err)
	}
}

func TestSecondProcessIsLockedOut(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	c := startChild(t, "lock", dir)
	c.waitFor(t, "locked")
	defer c.kill()
	db := filepath.Join(dir, databaseName)
	before, err := os.Stat(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open(context.Background(), config{dataDir: dir, syncDir: noSync}); err == nil || !strings.Contains(err.Error(), "another server") {
		t.Fatalf("second open: %v", err)
	}
	after, _ := os.Stat(db)
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatal("the locked-out process touched the database")
	}
}
