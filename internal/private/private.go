// Package private keeps the server's files at rest private to the user it
// runs as: its directories belong to that user and are
// 0700, its files are regular files of that user with one name and are 0600,
// they carry no ACL entries (acl.go), and they are opened without following
// symlinks. Every directory on the way to them must be beyond other users'
// reach (trust.go). Anything owned by another user is refused with a message
// saying what to do.
//
// The checks protect against other users. They cannot protect against a
// process of the same user, which can do anything the server can. Nor can
// taking access away (chmod, removing an ACL entry) revoke a descriptor
// another user opened while the access was there.
package private

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

// Errors the checks return (wrapped with the path and what to do).
var (
	ErrNotOwned   = errors.New("belongs to another user")
	ErrNotRegular = errors.New("is not a regular file")
	ErrNotDir     = errors.New("is not a directory")
	ErrSymlink    = errors.New("is a symbolic link")
	ErrExposed    = errors.New("is readable by other users")
	ErrLinked     = errors.New("has another name (a hard link)")
)

// owner is the user ID that owns a file.
func owner(fi os.FileInfo) (int, error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("no file owner on this system")
	}
	return int(st.Uid), nil
}

func ownedHere(path string, fi os.FileInfo) error {
	uid, err := owner(fi)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if me := os.Getuid(); uid != me {
		return fmt.Errorf("%s %w (uid %d), not to the server's user (uid %d): give it to the server's user with chown, or choose another path",
			path, ErrNotOwned, uid, me)
	}
	return nil
}

// Dir makes dir (and any missing parents, 0700), and checks that it, or the
// directory a symlink at dir points to, is a directory owned by this user
// and that every directory on the way to it is trusted (trust.go); what
// exists is checked before anything is created, so a refusal changes
// nothing. It then takes away any access of group and others, and every ACL
// entry. Only the data directory and folders the owner chooses (backups,
// logs, the installation) may be symlinks; the server's own folders inside
// them use RealDir.
func Dir(dir string) error {
	return makeDir(dir, true)
}

// RealDir is Dir for a server folder inside the data directory (snapshot
// views, imports, the media spool, staging): it must be a real directory,
// and a symlink there is refused.
func RealDir(dir string) error {
	return makeDir(dir, false)
}

func makeDir(dir string, link bool) error {
	if err := existing(dir); err != nil {
		return err
	}
	if !link {
		if fi, err := os.Lstat(dir); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s %w: the server's folders must be real folders; remove the link (choose another place with the server's options, such as --backup-dir)",
				dir, ErrSymlink)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	real, err := resolve(dir)
	if err != nil {
		return err
	}
	if !link && real != filepath.Clean(mustAbs(dir)) {
		// A parent of the folder may be a symlink (the data directory), not
		// the folder itself.
		if fi, err := os.Lstat(dir); err != nil {
			return err
		} else if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s %w: the server's folders must be real folders; remove the link", dir, ErrSymlink)
		}
	}
	d, err := os.OpenFile(real, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return symlinkError(dir, err)
	}
	defer d.Close()
	fi, err := d.Stat()
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s %w", dir, ErrNotDir)
	}
	if err := ownedHere(dir, fi); err != nil {
		return err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		if err := d.Chmod(0o700); err != nil {
			return err
		}
	}
	return clearACL(d)
}

func mustAbs(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		return abs
	}
	return p
}

// Tree makes private what dir holds, files and directories at any depth:
// each must belong to this user, a file must have no other name, group and
// others lose any access, and every ACL entry goes (files a directory's
// inheritable entries passed on before the server removed them). A symlink inside is
// refused: the server's folders and files must be real. linked names files
// that may share their one file with each other (the update's binary and
// its kept previous name).
func Tree(dir string, linked ...string) error {
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	// The walk sees resolved paths; so must the pair's names.
	linked = slices.Clone(linked)
	for i, l := range linked {
		if d, err := filepath.EvalSymlinks(filepath.Dir(l)); err == nil {
			linked[i] = filepath.Join(d, filepath.Base(l))
		}
	}
	return filepath.WalkDir(real, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == real {
			return nil
		}
		if e.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%s %w: the server's folders and files must be real; remove the link (choose another place with the server's options, such as --backup-dir)",
				path, ErrSymlink)
		}
		if !(e.Type().IsRegular() || e.IsDir()) {
			return nil
		}
		f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if errors.Is(err, os.ErrNotExist) {
			return nil // removed meanwhile
		}
		if err != nil {
			return symlinkError(path, err)
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		if err := ownedHere(path, fi); err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			if err := oneName(path, fi, linked); err != nil {
				return err
			}
		}
		if fi.Mode().Perm()&0o077 != 0 {
			if err := f.Chmod(fi.Mode().Perm() &^ 0o077); err != nil {
				return err
			}
		}
		return clearACL(f)
	})
}

// oneName refuses a file with a name outside dir's own: its links must all
// be among linked, when path is one of them.
func oneName(path string, fi os.FileInfo, linked []string) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Nlink) <= 1 {
		return nil
	}
	names := 0
	if slices.Contains(linked, path) {
		for _, l := range linked {
			if li, err := os.Lstat(l); err == nil && os.SameFile(li, fi) {
				names++
			}
		}
	}
	if uint64(st.Nlink) > uint64(names) {
		return fmt.Errorf("%s %w (%d names): another name gives access the server cannot take away; find it (ls -i shows the file number) and remove it, or replace this file with a copy",
			path, ErrLinked, st.Nlink)
	}
	return nil
}

// Protect takes every ACL entry away from a file the server has just
// created outside OpenFile (a temporary file it will rename).
func Protect(f *os.File) error { return clearACL(f) }

// checkOpen checks an opened file: regular, owned by this user, with no
// other name, and with no access for group and others (taken away if it had
// any, ACL entries included).
func checkOpen(f *os.File, perm os.FileMode) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s %w", f.Name(), ErrNotRegular)
	}
	if err := ownedHere(f.Name(), fi); err != nil {
		return err
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && uint64(st.Nlink) > 1 {
		return fmt.Errorf("%s %w: the server's own files must have one name; remove the other one", f.Name(), ErrLinked)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		if err := f.Chmod(perm.Perm() &^ 0o077); err != nil {
			return err
		}
	}
	return clearACL(f)
}

// clearNonblock takes back the O_NONBLOCK that kept a planted FIFO from
// blocking the open.
func clearNonblock(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := rc.Control(func(fd uintptr) { serr = syscall.SetNonblock(int(fd), false) }); err != nil {
		return err
	}
	return serr
}

func symlinkError(path string, err error) error {
	if errors.Is(err, syscall.ELOOP) {
		return fmt.Errorf("%s %w: the server's own files must be regular files", path, ErrSymlink)
	}
	return err
}

// OpenFile is os.OpenFile that does not follow a symlink at path. The opened
// file must be a regular file owned by this user with no other name; group
// and others lose any access it gave them (perm is the mode for a new file).
// O_TRUNC is refused: a file is truncated through its checked descriptor.
func OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	if flag&os.O_TRUNC != 0 {
		return nil, fmt.Errorf("%s: private.OpenFile does not truncate before its checks; truncate the checked file", path)
	}
	f, err := os.OpenFile(path, flag|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, perm)
	if err != nil {
		return nil, symlinkError(path, err)
	}
	if err := checkOpen(f, perm); err != nil {
		f.Close()
		return nil, err
	}
	if err := clearNonblock(f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// File checks an existing file like OpenFile; a missing file is fine.
func File(path string) error {
	f, err := OpenFile(path, os.O_RDONLY, 0o600)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return f.Close()
}

// ReadFile reads a file checked like OpenFile.
func ReadFile(path string) ([]byte, error) {
	f, err := OpenFile(path, os.O_RDONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// ReadKeyFile reads a private key the owner provides (a TLS key). The file,
// or the file a symlink points to, must belong to this user or to root and
// must not be readable or writable by group or others, by its mode or an ACL
// entry; it is checked through the descriptor it is read from. Nothing is
// changed: the error says how to fix it.
func ReadKeyFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s %w", path, ErrNotRegular)
	}
	uid, err := owner(fi)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if me := os.Getuid(); uid != me && uid != 0 {
		return nil, fmt.Errorf("%s %w (uid %d): a private key must belong to the server's user (uid %d) or to root", path, ErrNotOwned, uid, me)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s %w (mode %04o): make it private with chmod 600", path, ErrExposed, fi.Mode().Perm())
	}
	// An ACL can allow what the mode does not show. The owner's key is the owner's to change.
	entries, err := aclOf(f)
	if err != nil {
		return nil, err
	}
	if allows(entries, rightsAccessContent) {
		return nil, fmt.Errorf("%s %w: an ACL entry allows access (ls -le shows it); remove it with chmod -N", path, ErrExposed)
	}
	if err := clearNonblock(f); err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}
