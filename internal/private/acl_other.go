//go:build !darwin && !linux

package private

import (
	"os"
	"syscall"
)

// The server runs as a service only on macOS and Linux; elsewhere ACLs are
// not checked.

func aclOf(*os.File) ([]ace, error)        { return nil, nil }
func aclAt(string) ([]ace, error)          { return nil, nil }
func clearACL(*os.File) error              { return nil }
func adminGroupWrite(*syscall.Stat_t) bool { return false }
