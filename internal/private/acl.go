package private

// Access control lists. On macOS a file's mode is not the whole story: an ACL
// can allow other users or groups access that the mode bits do not show, and a
// directory's inheritable entries pass to the files created in it. The server
// therefore checks ACL entries, not only the mode:
//   - On the paths it owns it removes every ACL entry (the owner needs none).
//     Removing a directory's entries also ends their inheritance.
//   - It refuses a TLS key carrying an entry that allows access, and treats a
//     directory or command as replaceable when an entry allows changing it.
//
// On Linux, POSIX ACL entries for other users and groups are limited by the
// mask, which the mode's group bits show, so a mode without group and other
// access is the effective access. The server still removes access and
// default ACLs from the paths it owns.

// An ACL entry (macOS kauth_ace): who it applies to, its kind and
// inheritance flags, and the rights it allows or denies.
type ace struct {
	who    [16]byte
	flags  uint32
	rights uint32
}

const (
	aceKindMask    = 0xf
	acePermit      = 1
	aceOnlyInherit = 1 << 8
)

// Rights (macOS KAUTH_VNODE_*): the ones that give access to a file's
// contents or let its entries, its ACL or its owner change.
const (
	rightReadData       = 1 << 1  // also LIST_DIRECTORY
	rightWriteData      = 1 << 2  // also ADD_FILE
	rightDelete         = 1 << 4  //
	rightAppendData     = 1 << 5  // also ADD_SUBDIRECTORY
	rightDeleteChild    = 1 << 6  //
	rightWriteSecurity  = 1 << 12 //
	rightTakeOwnership  = 1 << 13 //
	rightGenericAll     = 1 << 21 //
	rightGenericWrite   = 1 << 23 //
	rightGenericRead    = 1 << 24 //
	rightsChange        = rightWriteData | rightDelete | rightAppendData | rightDeleteChild | rightWriteSecurity | rightTakeOwnership | rightGenericAll | rightGenericWrite
	rightsAccessContent = rightsChange | rightReadData | rightGenericRead
)

// allows reports an entry that allows any of rights on the object itself
// (an only-inherit entry applies to new children only). The server cannot
// tell which user an entry names without the directory service, so any
// such entry counts, the server's own user's included.
func allows(entries []ace, rights uint32) bool {
	for _, e := range entries {
		if e.flags&aceKindMask == acePermit && e.flags&aceOnlyInherit == 0 && e.rights&rights != 0 {
			return true
		}
	}
	return false
}
