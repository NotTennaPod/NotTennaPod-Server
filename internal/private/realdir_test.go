package private

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A server folder inside the data directory must be a real folder; a symlink
// there is refused and its target is left alone. And a refusal changes nothing:
// missing folders are not created on an untrusted way.
func TestServerFoldersMustBeRealFolders(t *testing.T) {
	data := t.TempDir()
	outside := filepath.Join(t.TempDir(), "shared")
	os.Mkdir(outside, 0o755)
	views := filepath.Join(data, "views")
	os.Symlink(outside, views)
	if err := RealDir(views); !errors.Is(err, ErrSymlink) {
		t.Fatalf("a symlinked server folder: %v", err)
	}
	if fi, _ := os.Stat(outside); fi.Mode().Perm() != 0o755 {
		t.Fatalf("the link's target changed to %04o", fi.Mode().Perm())
	}
	if err := RealDir(filepath.Join(data, "imports", "upload")); err != nil {
		t.Fatalf("a new folder: %v", err)
	}
	if fi, err := os.Lstat(filepath.Join(data, "imports")); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Fatalf("the new folder: %v %v", fi, err)
	}
	// The data directory itself may be a symlink; its folders inside are real.
	link := filepath.Join(t.TempDir(), "data-link")
	os.Symlink(data, link)
	if err := Dir(link); err != nil {
		t.Fatalf("a symlinked data directory: %v", err)
	}
	if err := RealDir(filepath.Join(link, "media-spool")); err != nil {
		t.Fatalf("a real folder under a symlinked data directory: %v", err)
	}
	open := filepath.Join(t.TempDir(), "open")
	os.Mkdir(open, 0o777)
	os.Chmod(open, 0o777)
	for _, f := range []func(string) error{Dir, RealDir, MkdirTrusted} {
		if err := f(filepath.Join(open, "a", "b")); !errors.Is(err, ErrUntrusted) {
			t.Fatalf("under an open directory: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(open, "a")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("a refused call created a folder")
		}
	}
}
