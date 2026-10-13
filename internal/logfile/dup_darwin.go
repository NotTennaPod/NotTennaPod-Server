package logfile

import "syscall"

func dupStd(fd int) error {
	if err := syscall.Dup2(fd, 1); err != nil {
		return err
	}
	return syscall.Dup2(fd, 2)
}
