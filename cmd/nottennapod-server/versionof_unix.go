//go:build unix

package main

import (
	"os/exec"
	"syscall"
)

// ownGroup runs cmd in a process group of its own, which ends with it on
// time-out.
func ownGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		endGroup(cmd)
		return nil
	}
}

// endGroup ends cmd's process group, if it was started.
func endGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
