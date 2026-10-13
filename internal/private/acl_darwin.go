package private

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// macOS reads a file's ACL with getattrlist(2) (ATTR_CMN_EXTENDED_SECURITY:
// a kauth_filesec in host byte order) and removes it with
// fchmod_extended(2), as chmod -N does.

type attrList struct {
	bitmapCount uint16
	reserved    uint16
	commonAttr  uint32
	volAttr     uint32
	dirAttr     uint32
	fileAttr    uint32
	forkAttr    uint32
}

const (
	attrCmnExtendedSecurity = 0x00400000
	fsoptNoFollow           = 0x1
	filesecMagic            = 0x012cc16d
	kauthNone               = ^uint32(0) - 100 // KAUTH_UID_NONE, KAUTH_GID_NONE
	filesecRemoveACL        = 1                // _FILESEC_REMOVE_ACL
	adminGID                = 80               // the administrators group
)

// parseFilesec reads the ACL entries of getattrlist's answer.
func parseFilesec(buf []byte) ([]ace, error) {
	le := binary.LittleEndian
	if len(buf) < 12 {
		return nil, errors.New("short attribute buffer")
	}
	ref := buf[4:]
	off, n := int(int32(le.Uint32(ref))), int(le.Uint32(ref[4:]))
	if n == 0 {
		return nil, nil // no ACL
	}
	if off < 0 || 4+off+n > len(buf) || n < 44 {
		return nil, errors.New("damaged ACL attribute")
	}
	fsec := buf[4+off : 4+off+n]
	if le.Uint32(fsec) != filesecMagic {
		return nil, errors.New("unknown ACL format")
	}
	count := le.Uint32(fsec[36:])
	if count == ^uint32(0) { // KAUTH_FILESEC_NOACL
		return nil, nil
	}
	var out []ace
	for i, p := uint32(0), 44; i < count; i, p = i+1, p+24 {
		if p+24 > len(fsec) {
			return nil, errors.New("damaged ACL attribute")
		}
		var e ace
		copy(e.who[:], fsec[p:p+16])
		e.flags, e.rights = le.Uint32(fsec[p+16:]), le.Uint32(fsec[p+20:])
		out = append(out, e)
	}
	return out, nil
}

func getattrlist(trap uintptr, target uintptr, options uintptr) ([]ace, error) {
	al := attrList{bitmapCount: 5, commonAttr: attrCmnExtendedSecurity}
	buf := make([]byte, 16<<10)
	_, _, e := syscall.Syscall6(trap, target, uintptr(unsafe.Pointer(&al)), uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), options, 0)
	if e != 0 {
		return nil, e
	}
	return parseFilesec(buf)
}

// aclOf returns the ACL entries of an open file.
func aclOf(f *os.File) ([]ace, error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return nil, err
	}
	var entries []ace
	var aerr error
	if err := rc.Control(func(fd uintptr) { entries, aerr = getattrlist(syscall.SYS_FGETATTRLIST, fd, 0) }); err != nil {
		return nil, err
	}
	if aerr != nil {
		return nil, fmt.Errorf("%s: reading its ACL: %w", f.Name(), aerr)
	}
	return entries, nil
}

// aclAt returns the ACL entries of path (a symlink at path is not followed).
func aclAt(path string) ([]ace, error) {
	p, err := syscall.BytePtrFromString(path)
	if err != nil {
		return nil, err
	}
	entries, err := getattrlist(syscall.SYS_GETATTRLIST, uintptr(unsafe.Pointer(p)), fsoptNoFollow)
	if err != nil {
		return nil, fmt.Errorf("%s: reading its ACL: %w", path, err)
	}
	return entries, nil
}

// clearACL removes every ACL entry of an open file the server owns.
func clearACL(f *os.File) error {
	entries, err := aclOf(f)
	if err != nil || len(entries) == 0 {
		return err
	}
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var e syscall.Errno
	if err := rc.Control(func(fd uintptr) {
		_, _, e = syscall.Syscall6(syscall.SYS_FCHMOD_EXTENDED, fd, uintptr(kauthNone), uintptr(kauthNone), ^uintptr(0), filesecRemoveACL, 0)
	}); err != nil {
		return err
	}
	if e != 0 {
		return fmt.Errorf("%s: removing its ACL: %w", f.Name(), e)
	}
	if entries, err := aclOf(f); err != nil {
		return err
	} else if len(entries) > 0 {
		return fmt.Errorf("%s: its ACL could not be removed", f.Name())
	}
	return nil
}

// adminGroupWrite reports a directory whose group write belongs to the
// administrators, who can become root anyway (/Applications is root:admin
// 0775).
func adminGroupWrite(st *syscall.Stat_t) bool { return st.Gid == adminGID }
