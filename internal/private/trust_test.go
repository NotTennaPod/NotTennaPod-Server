package private

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// addACL adds a macOS ACL entry to a path in a test's temporary directory.
func addACL(t *testing.T, entry, path string) {
	t.Helper()
	if out, err := exec.Command("/bin/chmod", "+a", entry, path).CombinedOutput(); err != nil {
		t.Fatalf("chmod +a %q: %v %s", entry, err, out)
	}
}

func entriesAt(t *testing.T, path string) []ace {
	t.Helper()
	e, err := aclAt(path)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func needACLs(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS ACLs (Linux: TestLinuxACLsAreRemoved)")
	}
}

// The server's own files and directories lose every ACL entry, and new
// files no longer inherit one.
func TestServerPathsLoseTheirACLs(t *testing.T) {
	needACLs(t)
	d := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(d, 0o700); err != nil {
		t.Fatal(err)
	}
	addACL(t, "everyone allow read,write,execute,file_inherit,directory_inherit", d)
	addACL(t, "everyone allow list,search,add_file,delete_child", d)
	// Created before the server's check: inherits everyone's access.
	old := filepath.Join(d, "old.db")
	os.WriteFile(old, []byte("x"), 0o600)
	sub := filepath.Join(d, "views")
	os.Mkdir(sub, 0o700)
	if len(entriesAt(t, old)) == 0 || len(entriesAt(t, sub)) == 0 {
		t.Fatal("the fixture did not inherit")
	}
	if err := Dir(d); err != nil {
		t.Fatal(err)
	}
	if e := entriesAt(t, d); len(e) != 0 {
		t.Fatalf("the directory keeps %d ACL entries", len(e))
	}
	if err := Tree(d); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{old, sub} {
		if e := entriesAt(t, p); len(e) != 0 {
			t.Fatalf("%s keeps %d inherited ACL entries", filepath.Base(p), len(e))
		}
	}
	f, err := OpenFile(filepath.Join(d, "new.key"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if e := entriesAt(t, filepath.Join(d, "new.key")); len(e) != 0 {
		t.Fatalf("a new file inherited %d ACL entries", len(e))
	}
	// A file given an entry directly loses it when the server opens it.
	addACL(t, "everyone allow read,write", old)
	if err := File(old); err != nil {
		t.Fatal(err)
	}
	if e := entriesAt(t, old); len(e) != 0 {
		t.Fatalf("an opened file keeps %d ACL entries", len(e))
	}
}

// A TLS key whose ACL allows access is refused, with a message, and left
// as it is; a deny entry is fine.
func TestKeyWithAnAllowingACLIsRefused(t *testing.T) {
	needACLs(t)
	p := filepath.Join(t.TempDir(), "key.pem")
	os.WriteFile(p, []byte("key"), 0o600)
	addACL(t, "user:nobody deny read", p)
	if _, err := ReadKeyFile(p); err != nil {
		t.Fatalf("a deny entry: %v", err)
	}
	addACL(t, "everyone allow read", p)
	_, err := ReadKeyFile(p)
	if !errors.Is(err, ErrExposed) || !strings.Contains(err.Error(), "chmod -N") {
		t.Fatalf("an allowing entry: %v", err)
	}
	if len(entriesAt(t, p)) != 2 {
		t.Fatal("the check changed the owner's key")
	}
	// Inherited from its directory, as a new key would be.
	d := t.TempDir()
	addACL(t, "everyone allow read,write,file_inherit", d)
	k := filepath.Join(d, "new.pem")
	os.WriteFile(k, []byte("key"), 0o600)
	if _, err := ReadKeyFile(k); !errors.Is(err, ErrExposed) {
		t.Fatalf("an inherited entry: %v", err)
	}
}

// The hard-link note: the server's files have one name.
func TestHardLinkedFilesAreRefused(t *testing.T) {
	d := t.TempDir()
	victim := filepath.Join(d, "victim")
	os.WriteFile(victim, []byte("valuable"), 0o600)
	link := filepath.Join(d, "early.log")
	if err := os.Link(victim, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFile(link, os.O_WRONLY, 0o600); !errors.Is(err, ErrLinked) {
		t.Fatalf("a hard link: %v", err)
	}
	if err := File(link); !errors.Is(err, ErrLinked) {
		t.Fatalf("checking a hard link: %v", err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "valuable" {
		t.Fatalf("the other name changed: %q", b)
	}
	if _, err := OpenFile(filepath.Join(d, "x"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600); err == nil {
		t.Fatal("O_TRUNC before the checks was accepted")
	}
}

// A private directory is refused when a directory on the way to it lets
// other users replace it; a sticky one is fine.
func TestDirectoriesNeedTrustedAncestors(t *testing.T) {
	base := t.TempDir()
	open := filepath.Join(base, "open")
	os.Mkdir(open, 0o700)
	os.Chmod(open, 0o777)
	if err := Dir(filepath.Join(open, "data")); !errors.Is(err, ErrUntrusted) || !strings.Contains(err.Error(), "chmod go-w") {
		t.Fatalf("under a directory anyone can write: %v", err)
	}
	if fi, _ := os.Stat(open); fi.Mode().Perm() != 0o777 {
		t.Fatal("the check changed a directory the server does not own")
	}
	sticky := filepath.Join(base, "sticky")
	os.Mkdir(sticky, 0o700)
	os.Chmod(sticky, 0o777|os.ModeSticky)
	if err := Dir(filepath.Join(sticky, "data")); err != nil {
		t.Fatalf("under a sticky directory: %v", err)
	}
	// A symlink whose own directory others can write.
	target := filepath.Join(base, "target")
	os.Mkdir(target, 0o700)
	link := filepath.Join(open, "link")
	os.Symlink(target, link)
	if err := Dir(link); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("through a symlink in a directory anyone can write: %v", err)
	}
	// A symlink in a trusted directory to a directory under an open one.
	inner := filepath.Join(open, "inner")
	os.Mkdir(inner, 0o700)
	link2 := filepath.Join(base, "link2")
	os.Symlink(inner, link2)
	if err := Dir(link2); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("to a directory under one anyone can write: %v", err)
	}
	if err := TrustedDir(open); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("TrustedDir of an open directory: %v", err)
	}
	if err := TrustedDir(target); err != nil {
		t.Fatalf("TrustedDir of a private one: %v", err)
	}
	if runtime.GOOS == "darwin" {
		acl := filepath.Join(base, "acl")
		os.Mkdir(acl, 0o755)
		addACL(t, "everyone allow add_file,delete_child", acl)
		if err := TrustedDir(acl); !errors.Is(err, ErrUntrusted) || !strings.Contains(err.Error(), "ACL") {
			t.Fatalf("a directory whose ACL lets anyone add files: %v", err)
		}
		if err := Dir(filepath.Join(acl, "data")); !errors.Is(err, ErrUntrusted) {
			t.Fatalf("under it: %v", err)
		}
	}
}

// A command is trusted only when nobody else can replace it or anything
// on its way, symlinks and their targets included.
func TestExecutablesNeedATrustedPath(t *testing.T) {
	base := t.TempDir()
	good := filepath.Join(base, "bin", "tool")
	os.MkdirAll(filepath.Dir(good), 0o755)
	os.WriteFile(good, []byte("#!/bin/sh\n"), 0o755)
	if real, err := TrustedExecutable(good); err != nil || real != mustReal(t, good) {
		t.Fatalf("a private tool: %q %v", real, err)
	}
	open := filepath.Join(base, "open")
	os.Mkdir(open, 0o777)
	os.Chmod(open, 0o777)
	bad := filepath.Join(open, "tool")
	os.WriteFile(bad, []byte("#!/bin/sh\n"), 0o755)
	if _, err := TrustedExecutable(bad); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("a tool in a directory anyone can write: %v", err)
	}
	link := filepath.Join(base, "bin", "link")
	os.Symlink(bad, link)
	if _, err := TrustedExecutable(link); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("a symlink to it: %v", err)
	}
	os.Chmod(good, 0o757)
	if _, err := TrustedExecutable(good); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("a tool anyone can write: %v", err)
	}
}

func mustReal(t *testing.T, p string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A symlink on a trusted path must not be replaceable itself: on macOS an ACL
// entry allowing its deletion beats a sticky directory, so it is refused as the
// command and on the way to one.
func TestSymlinksOnTheWayMustNotBeReplaceable(t *testing.T) {
	needACLs(t)
	base := t.TempDir()
	sticky := filepath.Join(base, "sticky")
	os.Mkdir(sticky, 0o700)
	os.Chmod(sticky, 0o777|os.ModeSticky)
	target := filepath.Join(base, "target")
	os.Mkdir(target, 0o700)
	tool := filepath.Join(target, "tailscale")
	os.WriteFile(tool, []byte("#!/bin/sh\n"), 0o700)
	toolLink := filepath.Join(sticky, "tailscale")
	os.Symlink(tool, toolLink)
	dirLink := filepath.Join(sticky, "alias")
	os.Symlink(target, dirLink)
	if _, err := TrustedExecutable(toolLink); err != nil {
		t.Fatalf("a protected link in a sticky directory: %v", err)
	}
	for _, link := range []string{toolLink, dirLink} {
		if out, err := exec.Command("/bin/chmod", "-h", "+a", "everyone allow delete", link).CombinedOutput(); err != nil {
			t.Fatalf("%v %s", err, out)
		}
	}
	if _, err := TrustedExecutable(toolLink); !errors.Is(err, ErrUntrusted) || !strings.Contains(err.Error(), "chmod -h -N") {
		t.Fatalf("the command through a deletable link: %v", err)
	}
	if _, err := TrustedExecutable(filepath.Join(dirLink, "tailscale")); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("a command under a deletable link: %v", err)
	}
	if err := Dir(filepath.Join(dirLink, "data")); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("a folder under a deletable link: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(target, "data")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the refused folder was created")
	}
}
