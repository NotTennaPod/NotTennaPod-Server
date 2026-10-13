package private

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Trusted paths. A private file in a private directory is still replaceable
// when some directory on the way to it lets another user rename or replace an
// entry. A path is trusted when every directory it passes through, from the
// root, including the ones that hold symlinks and the ones symlinks lead to,
// belongs to this user or to root and lets nobody else change its entries:
//   - no write for group or others, unless the directory is sticky (others
//     can then add entries but not rename or remove ours), or on macOS the
//     group is the administrators (who can become root anyway, as with
//     /Applications);
//   - and no ACL entry allowing anyone to add, delete or change entries
//     (acl.go).
//
// The checks change nothing: a shared directory is the owner's to fix.

// ErrUntrusted reports a directory or command another user can change.
var ErrUntrusted = errors.New("can be changed by other users")

// maxLinks bounds the symlinks one path may pass through, as the kernel does.
const maxLinks = 40

// trusted checks one directory, or a command, already resolved.
func trusted(path string, fi os.FileInfo) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s: no file owner on this system", path)
	}
	if uid := int(st.Uid); uid != os.Getuid() && uid != 0 {
		return fmt.Errorf("%s %w: it belongs to uid %d, not to the server's user or root; choose another path", path, ErrUntrusted, uid)
	}
	w := fi.Mode().Perm() & 0o022
	if adminGroupWrite(st) {
		w &^= 0o020
	}
	if w != 0 && (fi.Mode()&os.ModeSticky == 0 || !fi.IsDir()) {
		return fmt.Errorf("%s %w (mode %04o): take their write access away with chmod go-w, or choose another path", path, ErrUntrusted, fi.Mode().Perm())
	}
	entries, err := aclAt(path)
	if err != nil {
		return err
	}
	if allows(entries, rightsChange) {
		return fmt.Errorf("%s %w: an ACL entry allows changing it (ls -le shows it); remove the entry (chmod -a or chmod -N), or choose another path", path, ErrUntrusted)
	}
	return nil
}

// trustedLink checks a symlink on the way: its own owner must be this user
// or root, and no ACL entry may let anyone delete or change it (on macOS a
// DELETE entry on the link beats a sticky directory). A symlink's mode means
// nothing.
func trustedLink(path string, fi os.FileInfo) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s: no file owner on this system", path)
	}
	if uid := int(st.Uid); uid != os.Getuid() && uid != 0 {
		return fmt.Errorf("%s %w: the symbolic link belongs to uid %d, not to the server's user or root; replace it with your own, or choose another path",
			path, ErrUntrusted, uid)
	}
	entries, err := aclAt(path)
	if err != nil {
		return err
	}
	if allows(entries, rightsChange) {
		return fmt.Errorf("%s %w: an ACL entry on the symbolic link allows deleting or changing it (ls -le shows it); remove it with chmod -h -N",
			path, ErrUntrusted)
	}
	return nil
}

// existing validates what already exists of the way to p, before anything
// is created on it: the deepest existing path, and every directory on the
// way to it.
func existing(p string) error {
	abs, err := filepath.Abs(p)
	if err != nil {
		return err
	}
	for {
		if _, err := os.Lstat(abs); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return nil
		}
		abs = parent
	}
	real, err := resolve(abs)
	if err != nil {
		return err
	}
	fi, err := os.Stat(real)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s %w", abs, ErrNotDir)
	}
	if abs == filepath.Clean(p) {
		return nil // the directory itself: its caller checks it
	}
	return trusted(real, fi)
}

// MkdirTrusted makes dir (0700 for what it creates) only on a trusted way:
// what exists is checked first, so a refusal changes nothing; then dir must
// be trusted as TrustedDir says. A directory that exists is not changed.
func MkdirTrusted(dir string) error {
	if err := existing(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return TrustedDir(dir)
}

// walk resolves the absolute path p one component at a time, as the kernel
// does, checking every directory it passes through with trusted, including
// the ones holding symlinks and those symlinks lead to. The last component
// is resolved but not checked; walk returns its resolved path.
func walk(p string, links *int) (string, error) {
	if !filepath.IsAbs(p) {
		return "", fmt.Errorf("%s is not an absolute path", p)
	}
	p = filepath.Clean(p)
	cur := string(filepath.Separator)
	fi, err := os.Stat(cur)
	if err != nil {
		return "", err
	}
	if err := trusted(cur, fi); err != nil {
		return "", err
	}
	if p == cur {
		return cur, nil
	}
	parts := strings.Split(strings.TrimPrefix(p, cur), string(filepath.Separator))
	for i, name := range parts {
		next := filepath.Join(cur, name)
		fi, err := os.Lstat(next)
		if err != nil {
			return "", err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			if *links++; *links > maxLinks {
				return "", fmt.Errorf("%s: %w", p, syscall.ELOOP)
			}
			if err := trustedLink(next, fi); err != nil {
				return "", err
			}
			target, err := os.Readlink(next)
			if err != nil {
				return "", err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(cur, target)
			}
			if next, err = walk(target, links); err != nil {
				return "", err
			}
			if fi, err = os.Stat(next); err != nil {
				return "", err
			}
		}
		if i == len(parts)-1 {
			return next, nil
		}
		if !fi.IsDir() {
			return "", fmt.Errorf("%s: %w", next, syscall.ENOTDIR)
		}
		if err := trusted(next, fi); err != nil {
			return "", err
		}
		cur = next
	}
	return cur, nil
}

// resolve is walk from the start.
func resolve(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	links := 0
	return walk(abs, &links)
}

// TrustedDir checks that no other user can change dir or any directory on
// the way to it (a service definition's directory). Nothing is changed.
func TrustedDir(dir string) error {
	real, err := resolve(dir)
	if err != nil {
		return err
	}
	fi, err := os.Stat(real)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s %w", dir, ErrNotDir)
	}
	return trusted(real, fi)
}

// TrustedExecutable checks a command the server runs with authority (the
// tailscale command): every directory on the way to it, symlinks and their
// targets included, and the file itself, a regular file of this user or
// root, must be beyond other users' reach. It returns the resolved path.
func TrustedExecutable(path string) (string, error) {
	real, err := resolve(path)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(real)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("%s %w", real, ErrNotRegular)
	}
	return real, trusted(real, fi)
}
