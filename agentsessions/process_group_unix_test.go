//go:build !windows

package agentsessions

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestKillWithGraceTerminatesChildProcessGroup(t *testing.T) {
	dir := t.TempDir()
	childPIDPath := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "spawn-child.sh")
	body := `#!/bin/sh
sleep 30 &
echo "$!" > child.pid
while :; do
  sleep 30
done
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	cmd := exec.Command(script)
	cmd.Dir = dir
	configureCommandProcessGroup(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatal("configureCommandProcessGroup did not enable Setpgid")
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start script: %v", err)
	}

	childPID := waitForPIDFile(t, childPIDPath)
	procDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(procDone)
	}()

	killWithGrace(cmd, 100*time.Millisecond, procDone)
	select {
	case <-procDone:
	case <-time.After(2 * time.Second):
		t.Fatal("parent process did not exit after process-group kill")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !processExists(childPID) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("child process %d survived process-group kill", childPID)
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if convErr != nil {
				t.Fatalf("parse child pid: %v", convErr)
			}
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
	return 0
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
