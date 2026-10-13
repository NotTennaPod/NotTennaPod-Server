package logfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Logs are opened without following symlinks, in a private directory of the
// server's user.

func TestEarlyLogSymlinkIsNotFollowed(t *testing.T) {
	d := t.TempDir()
	v := filepath.Join(t.TempDir(), "valuable-file")
	os.WriteFile(v, []byte("valuable"), 0o600)
	os.Symlink(v, filepath.Join(d, EarlyName))
	// The log folder holds no symlinks: opening it is
	// refused, and the link's target is left alone.
	if l, err := OpenWith(d, Options{Dup: func(int) error { return nil }}); !errors.Is(err, private.ErrSymlink) {
		if err == nil {
			l.Close()
		}
		t.Fatalf("a log folder with a planted symlink: %v", err)
	}
	if b, _ := os.ReadFile(v); string(b) != "valuable" {
		t.Fatalf("opening the log emptied early.log's symlink target: %q", b)
	}
}

func TestLogSymlinkIsRefused(t *testing.T) {
	d := t.TempDir()
	v := filepath.Join(t.TempDir(), "external-log")
	os.WriteFile(v, nil, 0o600)
	os.Symlink(v, filepath.Join(d, Name))
	l, err := OpenWith(d, Options{MaxSize: 4, Dup: func(int) error { return nil }})
	if err == nil {
		l.Close()
		t.Fatal("a symlinked log was opened")
	}
	if !errors.Is(err, private.ErrSymlink) {
		t.Fatalf("got %v", err)
	}
	if fi, _ := os.Stat(v); fi.Size() != 0 {
		t.Fatalf("the symlink's target was written: %d bytes", fi.Size())
	}
}

func TestExistingLogDirectoryIsMadePrivate(t *testing.T) {
	d := filepath.Join(t.TempDir(), "logs")
	os.Mkdir(d, 0o777)
	os.Chmod(d, 0o777)
	l, err := OpenWith(d, Options{Dup: func(int) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	if fi, _ := os.Stat(d); fi.Mode().Perm() != 0o700 {
		t.Fatalf("the log directory is %04o", fi.Mode().Perm())
	}
}
