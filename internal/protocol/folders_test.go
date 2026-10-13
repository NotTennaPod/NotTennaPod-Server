package protocol

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// The snapshot views' folder inside the data directory must be a real folder; a
// symlink to an outside folder is refused, and nothing is written there.
func TestViewsFolderMustBeReal(t *testing.T) {
	data := t.TempDir()
	outside := t.TempDir()
	views := filepath.Join(data, "views")
	if err := os.Symlink(outside, views); err != nil {
		t.Fatal(err)
	}
	if _, err := NewViewRegistry(views, nil); !errors.Is(err, private.ErrSymlink) {
		t.Fatalf("a symlinked views folder: %v", err)
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("the outside folder was written: %v", entries)
	}
	os.Remove(views)
	if _, err := NewViewRegistry(views, nil); err != nil {
		t.Fatalf("a real views folder: %v", err)
	}
	if fi, _ := os.Lstat(views); !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Fatalf("the views folder: %v", fi.Mode())
	}
}
