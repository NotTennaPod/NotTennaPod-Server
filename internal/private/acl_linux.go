package private

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Linux keeps POSIX ACLs in the system.posix_acl_access and
// system.posix_acl_default extended attributes. Their entries for other
// users and groups are limited by the mask, which the mode's group bits
// show, so the mode check is the effective check (acl.go); the server still
// removes both attributes from the paths it owns.

var aclAttrs = []string{"system.posix_acl_access", "system.posix_acl_default"}

// aclOf returns no entries: the mode is the effective access on Linux.
func aclOf(*os.File) ([]ace, error) { return nil, nil }

// aclAt returns no entries, as aclOf.
func aclAt(string) ([]ace, error) { return nil, nil }

// clearACL removes the access and default ACLs of an open file the server
// owns; a file system without ACLs has none to remove.
func clearACL(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var errs []error
	if err := rc.Control(func(fd uintptr) {
		for _, name := range aclAttrs {
			p, err := syscall.BytePtrFromString(name)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			_, _, e := syscall.Syscall(syscall.SYS_FREMOVEXATTR, fd, uintptr(unsafe.Pointer(p)), 0)
			if e != 0 && e != syscall.ENODATA && e != syscall.EOPNOTSUPP {
				errs = append(errs, fmt.Errorf("%s: removing %s: %w", f.Name(), name, e))
			}
		}
	}); err != nil {
		return err
	}
	return errors.Join(errs...)
}

func adminGroupWrite(*syscall.Stat_t) bool { return false }
