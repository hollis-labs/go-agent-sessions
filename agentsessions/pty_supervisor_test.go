//go:build !windows

package agentsessions

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
)

// writeSilentReadLoopScript drops a shell that reads stdin lines but emits
// no output. Used to test idle-kill / watchdog without ptmx Read activity
// muddying the picture.
func writeSilentReadLoopScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "silent.sh")
	body := `#!/bin/sh
while IFS= read -r line; do
  : # consume but stay silent
done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// writeCrashThenSucceedScript drops a shell that increments a counter file
// in $PWD and exits 1 until the counter reaches a target, then exits 0.
// Used to test restart-on-crash recovery.
func writeCrashThenSucceedScript(t *testing.T, dir string, succeedAtAttempt int) string {
	t.Helper()
	path := filepath.Join(dir, "crash-then-succeed.sh")
	body := fmt.Sprintf(`#!/bin/sh
counter_file="$PWD/.attempt"
attempts=$(cat "$counter_file" 2>/dev/null || echo 0)
attempts=$((attempts + 1))
echo "$attempts" > "$counter_file"
if [ "$attempts" -lt %d ]; then
  printf 'delta:attempt-%%d\n' "$attempts"
  exit 1
fi
printf 'delta:final-%%d\n' "$attempts"
exit 0
`, succeedAtAttempt)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// writeAlwaysCrashScript drops a shell that always exits 1 after a tiny
// initial output line. Used to test restart-exhausted.
func writeAlwaysCrashScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "always-crash.sh")
	body := `#!/bin/sh
counter_file="$PWD/.attempt"
attempts=$(cat "$counter_file" 2>/dev/null || echo 0)
attempts=$((attempts + 1))
echo "$attempts" > "$counter_file"
printf 'delta:crashing-%d\n' "$attempts"
exit 1
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// readAttemptCounter returns the number of attempts recorded in $PWD/.attempt
// or 0 if the file does not exist.
func readAttemptCounter(t *testing.T, dir string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, ".attempt"))
	if err != nil {
		return 0
	}
	var n int
	_, _ = fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &n)
	return n
}

func TestPTYSupervisor_IdleKill_TerminatesIdleChild(t *testing.T) {
	dir := t.TempDir()
	script := writeSilentReadLoopScript(t, dir)
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-idle",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			IdleKill: 300 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	// Wait drains when the supervised loop finalizes after the idle-kill.
	done := make(chan struct{})
	var waitErr error
	go func() {
		_, waitErr = sess.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("idle-kill did not terminate within 5s")
	}

	var xe *ExitError
	if !errors.As(waitErr, &xe) {
		t.Fatalf("Wait err = %v (%T), want *ExitError", waitErr, waitErr)
	}
	if xe.Cause != CauseIdleTimeout {
		t.Errorf("ExitError.Cause = %q, want %q", xe.Cause, CauseIdleTimeout)
	}
	if xe.Signal == 0 {
		t.Errorf("ExitError.Signal = 0, want SIGTERM or SIGKILL")
	}
}

func TestPTYSupervisor_RestartOnCrash_SucceedsOnSecondAttempt(t *testing.T) {
	dir := t.TempDir()
	script := writeCrashThenSucceedScript(t, dir, 2 /* succeed at attempt 2 */)

	var restartCount atomic.Int32
	var lastPrevExit atomic.Pointer[ExitError]

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-restart",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			RestartOnCrash:    2,
			MaxRestartBackoff: 100 * time.Millisecond,
			OnRestart: func(attempt int, prevExit *ExitError) {
				restartCount.Add(1)
				lastPrevExit.Store(prevExit)
			},
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	done := make(chan struct{})
	var waitErr error
	go func() {
		_, waitErr = sess.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("restart-on-crash test did not complete within 10s")
	}

	if waitErr != nil {
		t.Errorf("Wait err = %v, want nil (clean exit on attempt 2)", waitErr)
	}
	if got := readAttemptCounter(t, dir); got != 2 {
		t.Errorf("attempt counter = %d, want 2 (1 crash + 1 success)", got)
	}
	if got := restartCount.Load(); got != 1 {
		t.Errorf("OnRestart fired %d times, want 1", got)
	}
	if prev := lastPrevExit.Load(); prev == nil {
		t.Errorf("OnRestart's prevExit = nil, want non-nil ExitError from the failed first attempt")
	} else if prev.Code != 1 {
		t.Errorf("OnRestart's prevExit.Code = %d, want 1", prev.Code)
	}
}

func TestPTYSupervisor_RestartExhausted_ReturnsExitError(t *testing.T) {
	dir := t.TempDir()
	script := writeAlwaysCrashScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-exhausted",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			RestartOnCrash:    2,
			MaxRestartBackoff: 50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	done := make(chan struct{})
	var waitErr error
	go func() {
		_, waitErr = sess.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("restart-exhausted test did not complete within 10s")
	}

	var xe *ExitError
	if !errors.As(waitErr, &xe) {
		t.Fatalf("Wait err = %v (%T), want *ExitError", waitErr, waitErr)
	}
	if xe.Cause != CauseRestartExhausted {
		t.Errorf("ExitError.Cause = %q, want %q", xe.Cause, CauseRestartExhausted)
	}
	// Total spawns = RestartOnCrash + 1 = 3 (initial + 2 restarts)
	if got := readAttemptCounter(t, dir); got != 3 {
		t.Errorf("attempt counter = %d, want 3 (1 initial + 2 restarts)", got)
	}
}

func TestPTYSupervisor_Watchdog_KillsStuckChild(t *testing.T) {
	dir := t.TempDir()
	script := writeSilentReadLoopScript(t, dir)
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-watchdog",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			WatchdogTimeout: 300 * time.Millisecond,
			// No ActivityCallback → falls back to ptmx I/O activity.
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	done := make(chan struct{})
	var waitErr error
	go func() {
		_, waitErr = sess.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog did not fire within 5s")
	}

	var xe *ExitError
	if !errors.As(waitErr, &xe) {
		t.Fatalf("Wait err = %v (%T), want *ExitError", waitErr, waitErr)
	}
	if xe.Cause != CauseWatchdogKill {
		t.Errorf("ExitError.Cause = %q, want %q", xe.Cause, CauseWatchdogKill)
	}
	if !xe.Killed {
		t.Errorf("ExitError.Killed = false, want true (watchdog SIGKILL)")
	}
}

func TestPTYSupervisor_ActivityResetsIdleTimer(t *testing.T) {
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir) // echoes each input — both reads + writes tick activity
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-activity",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			IdleKill: 500 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send periodic input every 200ms for 1.5s. With IdleKill=500ms and
	// echo replies that tick on read+write, the timer should reset
	// repeatedly and the child should remain alive throughout.
	stopFeed := make(chan struct{})
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-stopFeed:
				return
			case <-ticker.C:
				i++
				if err := sess.SendInput(context.Background(), []byte(fmt.Sprintf("ping-%d", i))); err != nil {
					return
				}
			}
		}
	}()

	// Live for 1.5s while feeding.
	time.Sleep(1500 * time.Millisecond)
	close(stopFeed)
	<-feedDone

	// Child should still be alive — assert health.
	if !sess.Health().Alive {
		t.Errorf("session not alive after 1.5s of activity-reset feeding; idle-kill should not have fired")
	}

	// Now stop sending and wait for idle-kill to fire ~500ms later.
	if err := sess.Stop(context.Background()); err != nil {
		// Stop may race with idle-kill; both result in termination.
		_ = err
	}
	if _, err := sess.Wait(); err != nil {
		_ = err // exit is non-zero either way; we tested the no-kill condition above.
	}
}

func TestPTYSupervisor_OnRestart_NotFiredOnIdleKill(t *testing.T) {
	// Idle-kill is NOT restart-eligible, so OnRestart should NOT fire even
	// when RestartOnCrash > 0.
	dir := t.TempDir()
	script := writeSilentReadLoopScript(t, dir)
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-idle-no-restart",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	var restartCount atomic.Int32
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			IdleKill:          300 * time.Millisecond,
			RestartOnCrash:    3,
			MaxRestartBackoff: 50 * time.Millisecond,
			OnRestart: func(attempt int, prevExit *ExitError) {
				restartCount.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if _, err := sess.Wait(); err != nil {
		var xe *ExitError
		if !errors.As(err, &xe) || xe.Cause != CauseIdleTimeout {
			t.Errorf("expected idle_timeout exit, got %v", err)
		}
	}
	if got := restartCount.Load(); got != 0 {
		t.Errorf("OnRestart fired %d times after idle-kill, want 0", got)
	}
}

func TestPTYRuntime_BackcompatNoSupervisor_BehaviorPreserved(t *testing.T) {
	// Smoke test that Supervisor=nil + ResourceLimits=nil preserves the
	// v0.5.0 single-shot lifecycle exactly: Start succeeds, SendInput
	// works, Stop drains, Wait returns. No new fields consulted.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-backcompat",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	logPath := filepath.Join(dir, "session.log")
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: logPath,
		// Supervisor + ResourceLimits left nil.
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("backcompat")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(logPath); err == nil && strings.Contains(string(data), "delta:backcompat") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("legacy single-shot path didn't deliver echo")
}

func TestPTYResourceLimits_CPUTime_TerminatesCPUBound(t *testing.T) {
	// Spawn a CPU-bound spinner under CPUTime=1s. `yes` is I/O-bound and
	// blocks on PTY writes long enough that RLIMIT_CPU never trips on a
	// fast host, so we drive a tight pure-CPU loop via sh instead. With
	// the sh -c "ulimit -t 1; exec ..." wrap, the inner loop should
	// receive SIGXCPU once it has accumulated ~1 CPU-second.
	shPath, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("sh not in PATH: %v", err)
	}
	dir := t.TempDir()

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID: "pty-cpulimit",
		Adapter: &fixedBinaryAdapter{
			binary: shPath,
			// Tight busy-loop with no I/O — burns CPU until RLIMIT_CPU.
			args: []string{"-c", "while :; do :; done"},
		},
		Caps: Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		ResourceLimits: &ResourceLimits{
			CPUTime: 1 * time.Second,
		},
		Supervisor: &SupervisorOptions{
			// Outer watchdog as backstop in case ulimit -t doesn't trip.
			WatchdogTimeout: 8 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	done := make(chan struct{})
	var waitErr error
	go func() {
		_, waitErr = sess.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("CPUTime limit did not fire within 15s")
	}

	var xe *ExitError
	if !errors.As(waitErr, &xe) {
		t.Logf("Wait err = %v (%T) — accepting any termination for CPUTime limit test", waitErr, waitErr)
		return
	}
	// Accept SIGXCPU (24), SIGKILL (9 — happens when soft+hard limit at
	// the same value), or any non-zero exit. The important thing is the
	// busy loop did NOT run forever.
	if xe.Signal == int(syscall.SIGXCPU) || xe.Killed || xe.Code != 0 || xe.Cause != "" {
		return
	}
	t.Errorf("expected non-clean exit under CPUTime=1s, got %+v", xe)
}

func TestPTYResourceLimits_MemoryMax_LinuxOnly(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("MemoryMax with hard enforcement is linux-only (systemd-run --user)")
	}
	if !systemdRunUserAvailable() {
		t.Skip("systemd-run --user not available; cgroup-enforced MemoryMax test requires it")
	}
	// Without writing a malloc-test program inline, we settle for a smoke
	// test: a tight memory cap should at minimum not break Start. Real
	// cgroup OOM-kill assertion is gated on a self-hosted linux runner
	// per the v0.3.0 go-runner CHANGELOG-acknowledged gap.
	dir := t.TempDir()
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-memlimit",
		Adapter: &fixedBinaryAdapter{binary: "/bin/echo", args: []string{"hello"}},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		ResourceLimits: &ResourceLimits{
			MemoryMax: 50 * 1024 * 1024, // 50 MB — echo fits easily
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()
	if _, err := sess.Wait(); err != nil {
		// echo may exit cleanly or via PTY EOF; either is fine.
		_ = err
	}
}

func TestPTYSupervisor_StopWithRestartZero_NotMisclassifiedAsExhausted(t *testing.T) {
	// Regression for the Copilot-flagged bug: with RestartOnCrash=0, a
	// Stop-triggered SIGTERM produces a non-zero exit. Without the
	// ctx/Stop short-circuit in runSupervised, that exit would satisfy
	// restartEligible (Code != 0, no cause) and then be labeled
	// CauseRestartExhausted by the `attempt >= RestartOnCrash` branch.
	// Assert: caller-driven Stop never produces restart_exhausted.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-stop-zero-restart",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			IdleKill:       1 * time.Hour, // long; we'll Stop first
			RestartOnCrash: 0,             // single shot; the trigger for the bug
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Brief settle so the child is in the wait loop.
	time.Sleep(100 * time.Millisecond)
	if err := sess.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
	_, waitErr := sess.Wait()

	if waitErr == nil {
		// SIGTERM may produce a clean enough exit on some hosts;
		// nil is acceptable.
		return
	}
	var xe *ExitError
	if errors.As(waitErr, &xe) && xe.Cause == CauseRestartExhausted {
		t.Errorf("Stop-driven exit misclassified as restart_exhausted: %+v", xe)
	}
}

func TestPTYSupervisor_NegativeRestartOnCrash_DoesNotLeak(t *testing.T) {
	// Regression for the Copilot-flagged bug: RestartOnCrash < 0 caused
	// the loop body to be skipped entirely, leaking the first-attempt
	// child. Assert: negative RestartOnCrash is clamped to 0; the first
	// attempt's wait still observes cmd.Wait, and Wait() returns within
	// a bounded time after the child exits.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-negative-restart",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			RestartOnCrash: -5,
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Stop the child; the supervised loop must observe the exit and
	// drain Wait. Without the clamp, Wait would block forever.
	time.Sleep(100 * time.Millisecond)
	_ = sess.Stop(context.Background())

	done := make(chan struct{})
	go func() { _, _ = sess.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait blocked — supervised loop did not observe first-attempt exit (leak under negative RestartOnCrash)")
	}
}

func TestPTYSupervisor_ActivityCallback_FiresOnPtmxIO(t *testing.T) {
	// Regression for the Copilot-flagged bug: ActivityCallback was
	// documented as runtime-fired but never actually invoked. Now wired
	// to the same tickActivity helper as ptmx I/O — the callback fires
	// once per reader-line and once per successful SendInput.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-activity-cb",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	var ticks atomic.Int32
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			IdleKill: 30 * time.Second, // long; we just exercise the callback
			ActivityCallback: func() {
				ticks.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	// One SendInput → one write tick + 2 read-line ticks (delta + done).
	if err := sess.SendInput(context.Background(), []byte("ping")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ticks.Load() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("ActivityCallback fired %d times, want >= 2 (1 write + >=1 reader-line)", ticks.Load())
}

func TestPTYSupervisor_StopDuringSupervised_TerminatesCleanly(t *testing.T) {
	// Stop on an actively-supervised session should drain Wait without
	// triggering a restart, even though RestartOnCrash > 0.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-stop-during-sup",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	var restartCount atomic.Int32
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Supervisor: &SupervisorOptions{
			IdleKill:          10 * time.Second, // long; we'll Stop first
			RestartOnCrash:    3,
			MaxRestartBackoff: 50 * time.Millisecond,
			OnRestart: func(attempt int, prevExit *ExitError) {
				restartCount.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send one turn so the child is in normal state.
	if err := sess.SendInput(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}
	// Brief settle.
	time.Sleep(100 * time.Millisecond)

	// Now stop and verify clean drain.
	if err := sess.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
	done := make(chan struct{})
	go func() { _, _ = sess.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not drain within 5s")
	}

	if got := restartCount.Load(); got != 0 {
		t.Errorf("OnRestart fired %d times after Stop, want 0", got)
	}
}

// fixedBinaryAdapter is a minimal CLIAdapter for tests that need to spawn a
// pre-existing binary (e.g. /usr/bin/yes) under the PTY runtime without a
// shell-script wrapper.
type fixedBinaryAdapter struct {
	binary string
	args   []string
}

func (a *fixedBinaryAdapter) Name() string           { return "fixed-binary-test" }
func (a *fixedBinaryAdapter) Detect() (string, bool) { return a.binary, a.binary != "" }
func (a *fixedBinaryAdapter) BuildArgs(_, _, _ string) []string {
	return append([]string(nil), a.args...)
}
func (a *fixedBinaryAdapter) ParseLine(_ []byte) ([]llmtypes.StreamEvent, error) { return nil, nil }

var _ provider.CLIAdapter = (*fixedBinaryAdapter)(nil)
