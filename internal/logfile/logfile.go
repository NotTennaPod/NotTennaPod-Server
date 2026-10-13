// Package logfile is the server's own log when it runs as a service
// (PROTOCOL §14): one private file in a private directory,
// duplicated onto standard output and standard error so that panics and
// anything else written there land in it too, rotated at 10 MiB with three
// older files kept. launchd's early.log, which only receives failures before
// the log is open, is emptied at each start so it stays bounded.
package logfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

const (
	// Name is the current log file; rotated ones are Name.1 (newest) to Name.3.
	Name = "server.log"
	// EarlyName is the file launchd writes the agent's stderr to before the log is open.
	EarlyName = "early.log"
	// MaxSize is the size at which the log rotates.
	MaxSize = 10 << 20
	// Keep is how many rotated files are kept.
	Keep = 3
)

// Options adjust a log for tests.
type Options struct {
	MaxSize int64
	// Dup duplicates a file descriptor onto standard output and error. Nil
	// uses the real dup2; tests pass their own so their output stays theirs.
	Dup func(fd int) error
}

// Log is the open log. It is safe for concurrent use.
type Log struct {
	mu   sync.Mutex
	dir  string
	f    *os.File
	size int64
	max  int64
	dup  func(fd int) error
}

// Open opens dir's log, creating dir (0700) and the file (0600), and makes it
// standard output and error.
func Open(dir string) (*Log, error) {
	return OpenWith(dir, Options{})
}

// OpenWith is Open with options.
func OpenWith(dir string, o Options) (*Log, error) {
	// The directory belongs to the server's user and is made 0700; the logs
	// in it are regular files, never followed symlinks.
	if err := private.Dir(dir); err != nil {
		return nil, fmt.Errorf("logfile: %w", err)
	}
	if err := private.Tree(dir); err != nil {
		return nil, fmt.Errorf("logfile: %w", err)
	}
	l := &Log{dir: dir, max: o.MaxSize, dup: o.Dup}
	if l.max <= 0 {
		l.max = MaxSize
	}
	if l.dup == nil {
		l.dup = dupStd
	}
	if err := l.open(); err != nil {
		return nil, err
	}
	if err := emptyEarly(filepath.Join(dir, EarlyName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(l.f, "logfile: cannot empty %s: %v\n", EarlyName, err)
	}
	return l, nil
}

// emptyEarly empties early.log through a descriptor that did not follow a
// symlink.
func emptyEarly(path string) error {
	f, err := private.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(0)
}

func (l *Log) path(i int) string {
	if i == 0 {
		return filepath.Join(l.dir, Name)
	}
	return filepath.Join(l.dir, fmt.Sprintf("%s.%d", Name, i))
}

func (l *Log) open() error {
	f, err := private.OpenFile(l.path(0), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("logfile: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if err := l.dup(int(f.Fd())); err != nil {
		f.Close()
		return fmt.Errorf("logfile: cannot make the log standard output and error: %w", err)
	}
	old := l.f
	l.f, l.size = f, fi.Size()
	if old != nil {
		old.Close()
	}
	return nil
}

// Write appends p, rotating first when p would take the file past the limit.
func (l *Log) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.size > 0 && l.size+int64(len(p)) > l.max {
		if err := l.rotate(); err != nil {
			fmt.Fprintf(l.f, "logfile: rotation failed: %v\n", err)
		}
	}
	n, err := l.f.Write(p)
	l.size += int64(n)
	return n, err
}

// Check rotates when output written straight to standard error (a panic, a
// library) has grown the file past the limit. The server calls it every minute.
func (l *Log) Check() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	fi, err := l.f.Stat()
	if err != nil {
		return err
	}
	l.size = fi.Size()
	if l.size <= l.max {
		return nil
	}
	return l.rotate()
}

// rotate renames Name → Name.1 → … → Name.Keep (the oldest is dropped), opens
// a new Name and duplicates it onto standard output and error again.
func (l *Log) rotate() error {
	if err := os.Remove(l.path(Keep)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for i := Keep - 1; i >= 0; i-- {
		if err := os.Rename(l.path(i), l.path(i+1)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return l.open()
}

// Dir is the log's directory.
func (l *Log) Dir() string { return l.dir }

// Close closes the current file (standard output and error keep it open).
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
