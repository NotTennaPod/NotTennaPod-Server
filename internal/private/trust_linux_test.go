package private

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// posixACL is a system.posix_acl_access value: the owner rw, uid 65534 rw,
// the group nothing, the mask rw, others nothing.
func posixACL() []byte {
	b := binary.LittleEndian.AppendUint32(nil, 2)
	for _, e := range []struct {
		tag, perm uint16
		id        uint32
	}{{1, 6, ^uint32(0)}, {2, 6, 65534}, {4, 0, ^uint32(0)}, {0x10, 6, ^uint32(0)}, {0x20, 0, ^uint32(0)}} {
		b = binary.LittleEndian.AppendUint16(b, e.tag)
		b = binary.LittleEndian.AppendUint16(b, e.perm)
		b = binary.LittleEndian.AppendUint32(b, e.id)
	}
	return b
}

// On Linux, the server's own files lose their POSIX ACLs, and the mode
// (whose group bits are the mask) shows no access for anyone else.
func TestLinuxACLsAreRemoved(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "receipt.keys")
	os.WriteFile(p, []byte("x"), 0o600)
	if err := syscall.Setxattr(p, "system.posix_acl_access", posixACL(), 0); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) {
			t.Skip("no POSIX ACLs on this file system")
		}
		t.Fatal(err)
	}
	if fi, _ := os.Stat(p); fi.Mode().Perm()&0o070 == 0 {
		t.Fatalf("the ACL's mask does not show in the mode: %04o", fi.Mode().Perm())
	}
	if err := File(p); err != nil {
		t.Fatal(err)
	}
	if _, err := syscall.Getxattr(p, "system.posix_acl_access", make([]byte, 256)); !errors.Is(err, syscall.ENODATA) {
		t.Fatalf("the ACL is still there: %v", err)
	}
	if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %04o", fi.Mode().Perm())
	}
}
