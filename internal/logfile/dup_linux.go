package logfile

import "syscall"

// Linux on arm64 has no dup2 system call; dup3 with no flags is the same.
func dupStd(fd int) error {
	if err := syscall.Dup3(fd, 1, 0); err != nil {
		return err
	}
	return syscall.Dup3(fd, 2, 0)
}
