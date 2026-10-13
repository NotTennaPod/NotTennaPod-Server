package views

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// The read views' folder must be a real folder; a symlink there is removed by
// the spool's reset, never followed.
func TestReadViewsFolderIsReal(t *testing.T) {
	data, outside := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(outside, "keep"), []byte("x"), 0o600)
	dir := filepath.Join(data, "read-views")
	os.Symlink(outside, dir)
	if _, err := NewSpool(dir, nil); err != nil && !errors.Is(err, private.ErrSymlink) {
		t.Fatal(err)
	}
	if fi, err := os.Lstat(dir); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		t.Fatal("the spool uses a symlinked folder")
	}
	if _, err := os.Stat(filepath.Join(outside, "keep")); err != nil {
		t.Fatalf("the outside folder was emptied: %v", err)
	}
}
