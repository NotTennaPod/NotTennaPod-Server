package private

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The startup scan makes every file private, refuses a file with a name
// outside, and refuses a symlink inside; the update's binary pair may share its
// file.
func TestScanMakesFilesPrivateAndRefusesOtherNames(t *testing.T) {
	d := t.TempDir()
	loose := filepath.Join(d, "backups", "old.db")
	os.MkdirAll(filepath.Dir(loose), 0o755)
	os.WriteFile(loose, []byte("backup"), 0o644)
	os.Chmod(loose, 0o644)
	if err := Tree(d); err != nil {
		t.Fatal(err)
	}
	for p, want := range map[string]os.FileMode{loose: 0o600, filepath.Dir(loose): 0o700} {
		if fi, _ := os.Stat(p); fi.Mode().Perm() != want {
			t.Errorf("%s is %04o, want %04o", filepath.Base(p), fi.Mode().Perm(), want)
		}
	}
	alias := filepath.Join(t.TempDir(), "alias")
	os.Link(loose, alias)
	err := Tree(d)
	if !errors.Is(err, ErrLinked) || !strings.Contains(err.Error(), loose) {
		t.Fatalf("a file with another name: %v", err)
	}
	os.Remove(alias)
	outside := t.TempDir()
	os.Symlink(outside, filepath.Join(d, "views"))
	if err := Tree(d); !errors.Is(err, ErrSymlink) {
		t.Fatalf("a symlink inside: %v", err)
	}
	os.Remove(filepath.Join(d, "views"))
	// The binary pair.
	bin := filepath.Join(d, "nottennapod-server")
	prev := bin + ".prev"
	os.WriteFile(bin, []byte("binary"), 0o700)
	os.Link(bin, prev)
	if err := Tree(d, bin, prev); err != nil {
		t.Fatalf("the recorded pair: %v", err)
	}
	if err := Tree(d); !errors.Is(err, ErrLinked) {
		t.Fatalf("the pair without its exception: %v", err)
	}
	third := filepath.Join(t.TempDir(), "third")
	os.Link(bin, third)
	if err := Tree(d, bin, prev); !errors.Is(err, ErrLinked) {
		t.Fatalf("the pair with a third name outside: %v", err)
	}
}
