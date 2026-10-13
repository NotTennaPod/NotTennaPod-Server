package private

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mode(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}

func TestDirIsMadePrivateThroughASymlink(t *testing.T) {
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o777); err != nil {
		t.Fatal(err)
	}
	os.Chmod(target, 0o777)
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := Dir(link); err != nil {
		t.Fatal(err)
	}
	if m := mode(t, target); m != 0o700 {
		t.Fatalf("the symlinked directory is %04o", m)
	}
	fresh := filepath.Join(t.TempDir(), "a", "b")
	if err := Dir(fresh); err != nil {
		t.Fatal(err)
	}
	if m := mode(t, fresh); m != 0o700 {
		t.Fatalf("a new directory is %04o", m)
	}
}

func TestOtherUsersDirectoriesAndFilesAreRefused(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root owns the system's directories")
	}
	// The root directory belongs to root on every system the server runs on.
	if err := Dir("/"); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("a directory of root: %v", err)
	}
	for _, p := range []string{"/etc/hosts", "/etc/passwd"} {
		if _, err := os.Lstat(p); err != nil {
			continue
		}
		real, _ := filepath.EvalSymlinks(p)
		if err := File(real); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("a file of root: %v", err)
		}
	}
}

func TestFilesAreRegularOwnedAndPrivate(t *testing.T) {
	d := t.TempDir()
	loose := filepath.Join(d, "loose")
	os.WriteFile(loose, []byte("x"), 0o666)
	os.Chmod(loose, 0o666)
	if err := File(loose); err != nil {
		t.Fatal(err)
	}
	if m := mode(t, loose); m != 0o600 {
		t.Fatalf("a loose file is %04o", m)
	}
	victim := filepath.Join(t.TempDir(), "victim")
	os.WriteFile(victim, []byte("valuable"), 0o644)
	link := filepath.Join(d, "link")
	os.Symlink(victim, link)
	if err := File(link); !errors.Is(err, ErrSymlink) {
		t.Fatalf("a symlink: %v", err)
	}
	if _, err := ReadFile(link); !errors.Is(err, ErrSymlink) {
		t.Fatalf("reading a symlink: %v", err)
	}
	if _, err := OpenFile(link, os.O_WRONLY|os.O_CREATE, 0o600); !errors.Is(err, ErrSymlink) {
		t.Fatalf("opening a symlink to write: %v", err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "valuable" || mode(t, victim) != 0o644 {
		t.Fatalf("the symlink's target changed: %q %04o", b, mode(t, victim))
	}
	if err := File(d); !errors.Is(err, ErrNotRegular) {
		t.Fatalf("a directory: %v", err)
	}
	if err := File(filepath.Join(d, "missing")); err != nil {
		t.Fatalf("a missing file: %v", err)
	}
}

func TestKeyFilesMustNotBeReadableByOthers(t *testing.T) {
	d := t.TempDir()
	key := filepath.Join(d, "key.pem")
	os.WriteFile(key, []byte("key"), 0o600)
	if b, err := ReadKeyFile(key); err != nil || string(b) != "key" {
		t.Fatal(b, err)
	}
	link := filepath.Join(t.TempDir(), "link.pem")
	os.Symlink(key, link)
	if _, err := ReadKeyFile(link); err != nil {
		t.Fatalf("a symlink to a private key: %v", err)
	}
	for _, m := range []os.FileMode{0o640, 0o604, 0o644, 0o660} {
		os.Chmod(key, m)
		if _, err := ReadKeyFile(link); !errors.Is(err, ErrExposed) || !strings.Contains(err.Error(), "chmod 600") {
			t.Fatalf("mode %04o: %v", m, err)
		}
	}
	if m := mode(t, key); m != 0o660 {
		t.Fatalf("the check changed the key's mode to %04o", m)
	}
}
