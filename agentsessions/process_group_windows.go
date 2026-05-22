//go:build windows

package agentsessions

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func configureCommandProcessGroup(_ *exec.Cmd) {}

func configurePTYCommandProcessGroup(_ *exec.Cmd) {}

func signalProcessGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if sig == syscall.SIGKILL {
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		return nil
	}
	if err := cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}
