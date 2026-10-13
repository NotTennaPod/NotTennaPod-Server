package media

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// The media spool folder must be a real folder: a symlinked one is refused, and
// its target is never emptied.
func TestSpoolFolderMustBeReal(t *testing.T) {
	data, outside := t.TempDir(), t.TempDir()
	keep := filepath.Join(outside, "keep")
	os.WriteFile(keep, []byte("valuable"), 0o600)
	spool := filepath.Join(data, "media-spool")
	os.Symlink(outside, spool)
	if _, err := New(Config{SpoolDir: spool, DataDir: data}); !errors.Is(err, private.ErrSymlink) {
		t.Fatalf("a symlinked spool folder: %v", err)
	}
	if b, err := os.ReadFile(keep); err != nil || string(b) != "valuable" {
		t.Fatalf("the outside folder was emptied: %q %v", b, err)
	}
}
