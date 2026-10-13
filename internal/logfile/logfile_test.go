package logfile

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotationKeepsThreeAndRedirectsEachFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logs")
	var dups int
	l, err := OpenWith(dir, Options{MaxSize: 1000, Dup: func(int) error { dups++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	line := strings.Repeat("x", 99) + "\n"
	for i := 0; i < 100; i++ { // 10,000 bytes: ten files' worth
		if _, err := l.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	names, _ := filepath.Glob(filepath.Join(dir, Name+"*"))
	if len(names) != Keep+1 {
		t.Fatalf("files %v, want the current one and %d rotated", names, Keep)
	}
	for _, n := range names {
		fi, _ := os.Stat(n)
		if fi.Size() > 1000 {
			t.Errorf("%s is %d bytes, over the limit", n, fi.Size())
		}
		if fi.Mode().Perm() != 0o600 {
			t.Errorf("%s mode %v, want 0600", n, fi.Mode().Perm())
		}
	}
	if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
		t.Errorf("directory mode %v, want 0700", fi.Mode().Perm())
	}
	if dups < 2 {
		t.Errorf("every new file must become standard output and error again (dup called %d times)", dups)
	}
}

func TestOpenEmptiesEarlyLog(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, EarlyName), []byte("launchd: before the log was open\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenWith(dir, Options{Dup: func(int) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if fi, _ := os.Stat(filepath.Join(dir, EarlyName)); fi.Size() != 0 {
		t.Errorf("early.log is %d bytes after start, want 0", fi.Size())
	}
}

// TestEveryWriterLandsInTheLog runs a child process that makes the log its
// standard output and error for real, writes through the log, straight to
// stderr and stdout, rotates several times, and then panics: everything is in
// the files, and they stay bounded (PROTOCOL §14).
func TestEveryWriterLandsInTheLog(t *testing.T) {
	if dir := os.Getenv("NTP_LOGFILE_CHILD"); dir != "" {
		child(dir)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestEveryWriterLandsInTheLog$")
	cmd.Env = append(os.Environ(), "NTP_LOGFILE_CHILD="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("the child should have panicked")
	}
	if len(out) != 0 {
		t.Errorf("output escaped the log: %q", out)
	}
	var all strings.Builder
	var total int64
	names, _ := filepath.Glob(filepath.Join(dir, Name+"*"))
	for _, n := range names {
		b, _ := os.ReadFile(n)
		all.Write(b)
		total += int64(len(b))
	}
	text := all.String()
	for _, want := range []string{"through the log 299", "straight to stderr", "straight to stdout", "panic: the last words"} {
		if !strings.Contains(text, want) {
			t.Errorf("the log lacks %q", want)
		}
	}
	if limit := int64(Keep+1) * 4000 * 2; total > limit {
		t.Errorf("logs total %d bytes, want at most %d", total, limit)
	}
}

func child(dir string) {
	l, err := OpenWith(dir, Options{MaxSize: 4000})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 300; i++ {
		fmt.Fprintf(l, "through the log %d %s\n", i, strings.Repeat("-", 40))
		if i%50 == 0 {
			fmt.Fprintln(os.Stderr, "straight to stderr")
			fmt.Println("straight to stdout")
			_ = l.Check()
		}
	}
	panic("the last words")
}
