package agentsessions

import (
	"errors"
	"fmt"
	"log"
	"os/exec"
	"time"
)

// logAbnormalWait emits a single diagnostic line when a long-lived
// runtime's cmd.Wait return looks suspicious — either a non-*exec.ExitError
// (indicating an OS-level wait failure rather than a clean child-exit) OR
// a rapid exit (< 1 second from spawn). Healthy long-lived sessions
// produce no log output.
//
// Captures the symptom class reported as "session reported as failed but
// spawned process is still alive": cmd.Wait returns instantly with a
// non-exit error while the agent process the operator can see via ps
// keeps running. Most common causes:
//
//   - The configured binary is a wrapper script that forks the real agent
//     instead of execing it (the wrapper exits cleanly, cmd.Wait returns,
//     the orphan agent keeps running with init as its parent).
//   - A sandbox helper or resource-limit wrapper is intercepting the
//     spawn and detaching from the lib's view of the child.
//   - "wait: no child processes" / ECHILD from the OS, typically a sign of
//     signal-handler interference reaping the child before cmd.Wait runs.
//
// The log shape is stable: a single line with key=value pairs so operators
// can grep or stream-parse it. kind is the runtime-kind string
// ("streaming-stdio" / "jsonrpc-stdio" / "pty"); runtimeID is the
// AdapterRuntimeConfig.ID the caller assigned; pid is cmd.Process.Pid
// (or 0 if the process never started); elapsed is the time between
// spawn and cmd.Wait return; err is the raw cmd.Wait error (may be nil
// on the rapid-exit branch).
//
// Added in v0.9.2.
func logAbnormalWait(kind, runtimeID string, pid int, elapsed time.Duration, err error) {
	abnormalErr := false
	if err != nil {
		abnormalErr = true
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			abnormalErr = false
		}
	}
	rapidExit := elapsed > 0 && elapsed < time.Second
	if !abnormalErr && !rapidExit {
		return
	}

	var errType, errMsg string
	if err != nil {
		errType = fmt.Sprintf("%T", err)
		errMsg = err.Error()
	}
	log.Printf("agentsessions: %s waiter abnormal: session=%s pid=%d elapsed=%s err_type=%s err=%q",
		kind, runtimeID, pid, elapsed, errType, errMsg)
}
