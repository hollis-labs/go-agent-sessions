package agentsessions

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// SupervisorOptions configures opt-in supervision for a Session — idle-kill,
// restart-on-crash, and watchdog. The zero value (StartOptions.Supervisor ==
// nil) preserves go-agent-sessions's default "spawn once, run to completion,
// return" behavior.
//
// v0.6.0 scope: PTY runtime (Caps.PTY=true) only. Supervision is implemented
// natively against the long-lived child — idle-kill / restart / watchdog
// goroutines observe cmd.Wait and the ptmx I/O streams directly, without
// going through go-runner. Restart preserves the provider-side
// agent_session_id when Caps.ProviderSessionID is true.
//
// On the adapter runtime (Caps.PTY=false), this struct is currently NOT
// consulted — adapter-path forwarding to runner.Config.Supervisor is a
// v0.6.x follow-up blocked on go-runner publishing its v0.3.0 supervision
// API. See StartOptions.Supervisor godoc and the v0.6.0 CHANGELOG.
//
// Field shape mirrors go-runner's SupervisorOptions (same names, same units)
// so consumers can build a single config that targets both paths once the
// adapter-path forwarding lands. The PTY-specific OnRestart hook is the
// lone addition.
type SupervisorOptions struct {
	// IdleKill terminates the child when no I/O activity (ptmx Read or
	// Write on the PTY path; runner stdout/stderr on the adapter path)
	// occurs for this duration. SIGTERM is sent first, followed by a 5s
	// grace, then SIGKILL if the process is still alive.
	//
	// Zero disables idle-kill. Recommended for chat: 15m.
	IdleKill time.Duration

	// RestartOnCrash sets the maximum number of restart attempts after a
	// non-zero exit. Zero disables restart (single shot). Total spawns =
	// RestartOnCrash + 1.
	//
	// Backoff between attempts is exponential (1s, 2s, 4s, 8s, ...) capped
	// at MaxRestartBackoff. The parent context is honored during backoff:
	// cancellation aborts further restarts.
	//
	// Idle-kill, watchdog-kill, and context-cancel exits are NOT
	// restart-eligible — only crashes (non-zero exit with no supervisor
	// cause set) trigger a restart.
	RestartOnCrash int

	// MaxRestartBackoff caps the exponential restart backoff. Zero defaults
	// to 30s.
	MaxRestartBackoff time.Duration

	// WatchdogTimeout fires SIGKILL with Cause=watchdog_kill if no
	// ActivityCallback ticks (or, in fallback mode, no ptmx I/O) occur
	// within this duration. Zero disables the watchdog.
	//
	// Watchdog kills are immediate (no SIGTERM grace) — they exist to
	// terminate stuck processes that won't respond to graceful signals.
	WatchdogTimeout time.Duration

	// ActivityCallback, when non-nil, is invoked by the PTY runtime each
	// time it observes ptmx I/O activity (one call per reader-line, one
	// per successful SendInput). Useful for telemetry, heartbeat hooks,
	// or feeding an app-level "still alive" signal upstream.
	//
	// Sends are synchronous; treat the callback like an io.Writer's
	// Write — keep the work short or hand off to your own goroutine.
	//
	// v0.6.0 scope: the watchdog and idle-kill always use the runtime's
	// internal ptmx-I/O activity tracker, regardless of whether
	// ActivityCallback is set. A future increment may add a separate
	// app-level activity tracker that the consumer can tick from outside
	// the per-line parser path. Until then, ActivityCallback is purely
	// observational from the consumer's side.
	ActivityCallback func()

	// OnRestart fires after backoff completes, just before the new child
	// is spawned. Useful for app-level state reset, retry-counter
	// increment, or telemetry. Nil-safe.
	//
	// attempt is 1-indexed (the second spawn fires OnRestart with
	// attempt=1, the third with attempt=2, etc.). prevExit carries the
	// structured exit info from the prior attempt.
	//
	// On the adapter path, this fires from the runner.EventRestart
	// observation inside the per-turn runner.Run call.
	OnRestart func(attempt int, prevExit *ExitError)
}

// ResourceLimits applies OS-level resource caps to the spawned process. All
// fields are zero-default-disabled. The wrap is identical to go-runner's
// v0.3.0 ResourceLimits — one shared sh -c "ulimit ...; exec" path on both
// runtimes, plus systemd-run --user --scope MemoryMax layering on Linux when
// available.
//
//   - Linux: when systemd-run --user is available, MemoryMax is enforced as
//     cgroup v2 memory.max (real OOM-kill on overshoot). Otherwise, RLIMIT_AS
//     via ulimit -v (advisory; the kernel enforces but apps that don't
//     malloc-fail-gracefully may still overshoot via mmap or stack growth).
//   - macOS: setrlimit-only via the same sh -c wrap. MemoryMax is silently
//     dropped (RLIMIT_AS is not exposed via bash's ulimit -v on darwin and
//     systemd-run is unavailable). Callers wanting hard memory limits on
//     macOS must use VM-based isolation (Lima / OrbStack).
//
// Limits compose with sandbox.Apply: the rlimit-setting shell exec's into the
// sandbox helper which exec's into the real binary. Limits inherit through
// the chain.
//
// Vendored from go-runner v0.3.0 to avoid a cross-repo version coordination
// just for this unification. Field shape is byte-compatible — a single
// declaration translates to either path.
type ResourceLimits struct {
	// CPUTime is the hard CPU-seconds limit (RLIMIT_CPU). The kernel sends
	// SIGXCPU at the soft limit and SIGKILL at the hard limit.
	CPUTime time.Duration

	// MemoryMax is the maximum virtual memory in bytes. See type godoc for
	// per-platform enforcement strength.
	MemoryMax uint64

	// MaxOpenFiles is the maximum number of open file descriptors
	// (RLIMIT_NOFILE).
	MaxOpenFiles uint64

	// MaxProcesses is the maximum number of child processes (RLIMIT_NPROC).
	MaxProcesses uint64

	// MaxFileSize is the maximum size of any single file the process can
	// create or extend, in bytes (RLIMIT_FSIZE).
	MaxFileSize uint64
}

// IsZero reports whether all fields are zero (no limits configured).
func (r ResourceLimits) IsZero() bool {
	return r.CPUTime == 0 &&
		r.MemoryMax == 0 &&
		r.MaxOpenFiles == 0 &&
		r.MaxProcesses == 0 &&
		r.MaxFileSize == 0
}

// Cause* describe the high-level reason a session terminated when the
// runtime triggered the termination directly. Empty Cause indicates a
// normal-but-non-zero exit or signal not driven by the supervisor /
// resource-limits subsystems.
const (
	// CauseIdleTimeout — idle-kill fired after no I/O activity for IdleKill.
	CauseIdleTimeout = "idle_timeout"

	// CauseWatchdogKill — watchdog fired (no activity within
	// WatchdogTimeout). Process was SIGKILL'd directly.
	CauseWatchdogKill = "watchdog_kill"

	// CauseRestartExhausted — RestartOnCrash attempts were used up; the
	// final attempt's exit info is carried on the returned *ExitError.
	CauseRestartExhausted = "restart_exhausted"

	// CauseOOMKill — process was killed by an OS-level OOM event.
	// Detection is best-effort; not all paths can distinguish this from a
	// generic SIGKILL.
	CauseOOMKill = "oom_kill"

	// CauseResourceLimit — process was killed for exceeding a configured
	// ResourceLimits cap (e.g. CPU time → SIGXCPU). Detection is
	// best-effort; not all platforms surface the underlying cause.
	CauseResourceLimit = "resource_limit"
)

// ExitError is the structured outcome of a non-clean session exit. The PTY
// runtime returns *ExitError (wrapping the underlying wait error) for any
// non-zero or signal-terminated exit when supervision is active; callers
// extract structured info via errors.As. Clean exits return nil.
//
// Field shape mirrors go-runner v0.3.0's ExitError so consumers can write
// one classification routine that handles both paths.
type ExitError struct {
	// Code is the process exit code, or -1 if the process was terminated
	// by a signal before exit.
	Code int

	// Signal is the signal number that terminated the process, or 0 if
	// the process exited normally.
	Signal int

	// Killed is true when the termination was forced via SIGKILL
	// (idle-kill, watchdog-kill, OOM-kill, or other forced kill).
	Killed bool

	// ProcessState is the raw os.ProcessState for callers that need
	// platform-specific details beyond the structured fields.
	ProcessState *os.ProcessState

	// Cause classifies the termination when go-agent-sessions triggered
	// it directly. One of the Cause* constants, or empty string for
	// normal-but-non-zero exits not driven by supervisor / limits.
	Cause string

	waitErr error
}

func (e *ExitError) Error() string {
	switch {
	case e == nil:
		return "<nil>"
	case e.Cause != "":
		return fmt.Sprintf("agentsessions: process terminated (cause=%s, signal=%d, code=%d)", e.Cause, e.Signal, e.Code)
	case e.Signal != 0:
		return fmt.Sprintf("agentsessions: process terminated by signal %d", e.Signal)
	default:
		return fmt.Sprintf("agentsessions: process exited %d", e.Code)
	}
}

// Unwrap returns the underlying wait error (typically *exec.ExitError or a
// context error) so errors.Is / errors.As keep working against stdlib types.
func (e *ExitError) Unwrap() error { return e.waitErr }

// buildExitError translates the (ProcessState, waitErr) pair returned by
// cmd.Wait into a structured *ExitError, or nil for clean exits. cause is
// set by the supervisor / resource-limits subsystems when they triggered
// the termination directly; pass empty string for ordinary exits.
func buildExitError(ps *os.ProcessState, waitErr error, cause string) *ExitError {
	if waitErr == nil && cause == "" {
		return nil
	}
	xe := &ExitError{
		Code:         -1,
		ProcessState: ps,
		Cause:        cause,
		waitErr:      waitErr,
	}
	if ps != nil {
		xe.Code = ps.ExitCode()
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok {
			if ws.Signaled() {
				sig := int(ws.Signal())
				xe.Signal = sig
				xe.Killed = ws.Signal() == syscall.SIGKILL
			}
		}
	}
	var ee *exec.ExitError
	if xe.Code == -1 && errors.As(waitErr, &ee) {
		xe.Code = ee.ExitCode()
	}
	return xe
}

// activityTracker is a lock-free last-activity timestamp store shared
// between the reader goroutine, the SendInput write path, and the
// supervisor goroutines. Time is stored as unix nanoseconds in an atomic
// int64 (zero = never ticked).
type activityTracker struct {
	last atomic.Int64
}

func (a *activityTracker) tick() {
	a.last.Store(time.Now().UnixNano())
}

// idleSince returns time.Since(lastTick), or time.Since(start) if never
// ticked. Callers treat the never-ticked case as "idle from start."
func (a *activityTracker) idleSince(start time.Time) time.Duration {
	last := a.last.Load()
	if last == 0 {
		return time.Since(start)
	}
	return time.Since(time.Unix(0, last))
}

// supState carries supervisor-driven termination cause across goroutines
// per-attempt so the post-Wait error can be classified.
type supState struct {
	mu    sync.Mutex
	cause string
}

func (s *supState) trySetCause(c string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cause != "" {
		return false
	}
	s.cause = c
	return true
}

func (s *supState) getCause() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

// computeRestartBackoff returns the wait duration before restart attempt
// `attempt` (1-indexed). Doubles per attempt, capped at maxBackoff.
// Defaults to 30s cap when maxBackoff <= 0.
func computeRestartBackoff(attempt int, maxBackoff time.Duration) time.Duration {
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 30 {
		return maxBackoff
	}
	base := time.Duration(1<<uint(attempt-1)) * time.Second
	if base > maxBackoff {
		return maxBackoff
	}
	return base
}

// restartEligible reports whether an attempt's exit warrants a restart
// under RestartOnCrash. Restart-eligible means: a non-zero exit with no
// supervisor cause set (i.e. a crash, not an idle/watchdog/limit kill).
// Clean exits (nil ExitError) and supervisor-driven kills are not
// restart-eligible.
func restartEligible(xe *ExitError) bool {
	if xe == nil {
		return false
	}
	if xe.Cause != "" {
		// Supervisor or resource-limits triggered — don't loop back into
		// the same condition.
		return false
	}
	// Crash: non-zero exit code or signal kill we didn't initiate.
	return xe.Code != 0 || xe.Signal != 0
}

// killWithGrace sends SIGTERM, waits grace, then SIGKILL if the process is
// still alive. Returns when procDone is signalled (process exited) or the
// grace + kill sequence completes.
func killWithGrace(cmd *exec.Cmd, grace time.Duration, procDone <-chan struct{}) {
	if cmd.Process == nil {
		return
	}
	_ = signalProcessGroup(cmd, syscall.SIGTERM)
	select {
	case <-procDone:
		return
	case <-time.After(grace):
	}
	if cmd.Process != nil {
		_ = signalProcessGroup(cmd, syscall.SIGKILL)
	}
}
