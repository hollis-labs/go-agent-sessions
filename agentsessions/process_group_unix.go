//go:build !windows

package agentsessions

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureCommandProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// configurePTYCommandProcessGroup intentionally leaves Setpgid unset: pty.Start
// sets Setsid+Setctty, which already creates a process group with pgid == pid.
func configurePTYCommandProcessGroup(_ *exec.Cmd) {}

func signalProcessGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	if pid <= 0 {
		return nil
	}
	err := syscall.Kill(-pid, sig)
	if err == nil {
		return err
	}
	fallbackErr := cmd.Process.Signal(sig)
	if fallbackErr == nil || errors.Is(fallbackErr, syscall.ESRCH) {
		return nil
	}
	return fallbackErr
}
