//go:build !windows

package agentsessions

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestManager_WaitSession_PropagatesExitError verifies the supervised
// PTY runtime's *ExitError reaches a Manager.WaitSession caller. Pattern
// adapts TestPTYSupervisor_IdleKill_TerminatesIdleChild (which calls
// rt.Start + sess.Wait directly) to route through Manager so the watch
// goroutine's exitErr-capture path is exercised.
func TestManager_WaitSession_PropagatesExitError(t *testing.T) {
	dir := t.TempDir()
	script := writeSilentReadLoopScript(t, dir)
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-mgr-idle",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	m := NewManager(nil)
	startCtx := context.Background()
	if err := m.Start(startCtx, StartRequest{
		ID:      "sess-idle",
		Runtime: rt,
		Options: StartOptions{
			Workdir: dir,
			LogPath: filepath.Join(dir, "session.log"),
			Supervisor: &SupervisorOptions{
				IdleKill: 300 * time.Millisecond,
			},
		},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, waitErr := m.WaitSession(waitCtx, "sess-idle")
	if waitErr == nil {
		t.Fatal("WaitSession err = nil; supervisor-killed session must surface non-nil error")
	}
	var xe *ExitError
	if !errors.As(waitErr, &xe) {
		t.Fatalf("errors.As(*ExitError) = false; WaitSession err = %v (%T)", waitErr, waitErr)
	}
	if xe.Cause != CauseIdleTimeout {
		t.Errorf("ExitError.Cause = %q, want %q", xe.Cause, CauseIdleTimeout)
	}
	if xe.Signal == 0 {
		t.Errorf("ExitError.Signal = 0, want SIGTERM or SIGKILL")
	}
}
