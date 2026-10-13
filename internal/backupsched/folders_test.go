package backupsched

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Backup folders are made private, and the write check creates a new, randomly
// named file, so a planted symlink is never followed.
func TestWriteCheckFollowsNothingPlanted(t *testing.T) {
	d, b := t.TempDir(), t.TempDir()
	os.Chmod(b, 0o777)
	s := New(&fakeStore{dir: d}, Config{BackupDir: b})
	os.Mkdir(s.dailyDir(), 0o777)
	os.Chmod(s.dailyDir(), 0o777)
	victim := filepath.Join(t.TempDir(), "valuable")
	os.WriteFile(victim, []byte("valuable"), 0o600)
	os.Symlink(victim, filepath.Join(s.dailyDir(), ".nottennapod-write-check"))
	s.checkWritable()
	if got, _ := os.ReadFile(victim); string(got) != "valuable" {
		t.Errorf("the write check followed a planted symlink: the victim is %q", got)
	}
	// The backup folder holds no symlinks: the check
	// reports it.
	if !strings.Contains(s.state.WriteCheck, "symbolic link") {
		t.Errorf("write check %q", s.state.WriteCheck)
	}
	for _, dir := range []string{b, s.dailyDir()} {
		if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
			t.Errorf("%s stays %04o", dir, fi.Mode().Perm())
		}
	}
	if left, _ := filepath.Glob(filepath.Join(s.dailyDir(), ".nottennapod-write-check-*")); len(left) != 0 {
		t.Errorf("the write check left %v", left)
	}
}

// Backups that inherited an ACL entry
// before the folder was cleaned keep none.
func TestBackupsLoseInheritedACLs(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS ACLs")
	}
	d, b := t.TempDir(), t.TempDir()
	if out, err := exec.Command("/bin/chmod", "+a", "everyone allow read,file_inherit,directory_inherit", b).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	old := filepath.Join(b, "nottennapod-20261001-000000-1.db")
	os.WriteFile(old, []byte("backup"), 0o600)
	s := New(&fakeStore{dir: d}, Config{BackupDir: b})
	s.checkWritable()
	for _, p := range []string{b, old} {
		if out, _ := exec.Command("/bin/ls", "-lde", p).CombinedOutput(); strings.Contains(string(out), "allow") {
			t.Errorf("%s keeps an ACL entry: %s", filepath.Base(p), out)
		}
	}
}
