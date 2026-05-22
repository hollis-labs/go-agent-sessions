//go:build !windows

package agentsessions

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/creack/pty"
	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
	pevents "github.com/hollis-labs/go-providers/provider/events"
	"github.com/hollis-labs/go-sandbox/sandbox"
)

// ptyRuntime is the agentsessions.Runtime backed by a long-lived PTY-spawned
// CLI subprocess. Selected by NewFromAdapter when AdapterRuntimeConfig.Caps.PTY
// is true. The child process persists conversation / MCP / tool-affordance
// state across turns; SendInput writes new turns to the PTY master. Exit
// drains via a single wait goroutine that also nil-clears the ptmx pointer
// so in-flight writes can return ErrNoInputChannel cleanly rather than
// racing against fd close.
//
// When StartOptions.Supervisor is non-nil, the runtime adds idle-kill,
// restart-on-crash, and watchdog policies natively against the PTY child —
// no go-runner involvement on this path. Restart preserves the provider-
// side agent_session_id when Caps.ProviderSessionID is true.
//
// Pattern lifted from agent-mux's claudecode runtime + session runtime
// (RWMutex + wait + lifecycle), generalized: any provider.CLIAdapter that
// opts in via Caps.PTY=true gets the same lifecycle. The legacy
// adapterSession (subprocess-per-turn) is untouched; consumers that don't
// flip the cap see no behavior change.
type ptyRuntime struct {
	cfg AdapterRuntimeConfig
}

func (r *ptyRuntime) ID() string         { return r.cfg.ID }
func (r *ptyRuntime) Kind() string       { return r.cfg.Kind }
func (r *ptyRuntime) Caps() Capabilities { return r.cfg.Caps }

func (r *ptyRuntime) Prepare(_ context.Context) error {
	if !r.cfg.Caps.BinaryRequired {
		return nil
	}
	if _, ok := r.cfg.Adapter.Detect(); !ok {
		return fmt.Errorf("agentsessions: adapter %q binary not found", r.cfg.Adapter.Name())
	}
	return nil
}

func (r *ptyRuntime) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Workdir == "" {
		return nil, errors.New("agentsessions: StartOptions.Workdir is required for pty runtime")
	}

	bootDir, planted, sessionAdapter, err := preparePlant(opts, r.cfg.Adapter, r.cfg.ID)
	if err != nil {
		return nil, err
	}
	opts = planted

	logPath, err := resolvePTYLogPath(opts)
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, err
	}
	logF, err := os.Create(logPath) //nolint:gosec // G304: workspace-managed path
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, fmt.Errorf("agentsessions: open log: %w", err)
	}

	s := &ptySession{
		runtime:       r,
		adapter:       sessionAdapter,
		bootDir:       bootDir,
		opts:          opts,
		logFile:       logF,
		done:          make(chan error, 1),
		copyDone:      make(chan struct{}),
		stopRequested: make(chan struct{}),
	}
	s.alive.Store(true)
	s.state.Store(int32(LiveStateIdle))
	// Pre-seed lastSessionID from preset so ProviderSessionID() returns
	// the resume id immediately after Start (before any TUI-side init
	// event lands). Mirrors adapterSession's behavior. Compliance
	// harness CapsProviderSessionID/PresetCarriedBeforeTurn pins this.
	s.lastSessionID.Store(opts.SessionIDPreset)

	// First spawn happens synchronously so AutoFireFirstTurn delivery can
	// resolve before Start returns.
	cmd, ptmx, attemptCleanup, err := s.spawnAttempt(0)
	if err != nil {
		_ = logF.Close()
		cleanupBootDir(bootDir)
		return nil, err
	}

	readerReady := make(chan struct{})
	var supervisorStart chan struct{}
	var supervisorStartOnce sync.Once
	signalSupervisorStart := func() {
		if supervisorStart != nil {
			supervisorStartOnce.Do(func() { close(supervisorStart) })
		}
	}
	if opts.Supervisor == nil {
		// v0.5.0 single-shot lifecycle — preserved exactly. The legacy
		// spawnReader / spawnWaiter pair captures the local ptmx and
		// nil-clears the struct field at exit; no restart, no idle-kill,
		// no watchdog.
		s.legacyCleanup = attemptCleanup
		s.spawnReaderLegacy(ptmx, readerReady)
		s.spawnWaiterLegacy(ptmx, cmd)
	} else {
		// Supervised lifecycle: a per-attempt reader + waiter + supervisor
		// goroutines, plus the restart loop. The first attempt is already
		// spawned above; runSupervised assumes ownership of the lifecycle
		// from here. logFile + sandbox/limit cleanups for the first
		// attempt are owned by runSupervised.
		supervisorStart = make(chan struct{})
		go s.runSupervised(ctx, cmd, ptmx, attemptCleanup, readerReady, supervisorStart)
	}

	if opts.BootMode == "stdin" && opts.BootPrompt != "" {
		select {
		case <-readerReady:
		case <-ctx.Done():
			signalSupervisorStart()
			_ = s.Stop(context.Background())
			return nil, ctx.Err()
		}
		if err := s.writeRawInputString(opts.BootPrompt); err != nil {
			signalSupervisorStart()
			_ = s.Stop(context.Background())
			return nil, fmt.Errorf("agentsessions: write boot prompt: %w", err)
		}
	}
	signalSupervisorStart()

	if opts.AutoFireFirstTurn && len(opts.FirstTurnPayload) > 0 {
		// Skip when the boot-prompt-on-stdin convention already delivered
		// a kickoff into the PTY earlier in Start.
		if !(opts.BootMode == "stdin" && opts.BootPrompt != "") {
			if err := s.SendInput(ctx, opts.FirstTurnPayload); err != nil {
				_ = s.Stop(ctx)
				return nil, fmt.Errorf("agentsessions: auto-fire first turn: %w", err)
			}
		}
	}

	return s, nil
}

// resolvePTYLogPath picks the log destination per the two-dir convention:
// LogPath if set, otherwise <WorkspaceDir>/logs/session.log. One must be set;
// the PTY runtime does not silently discard its log stream.
func resolvePTYLogPath(opts StartOptions) (string, error) {
	if opts.LogPath != "" {
		return opts.LogPath, nil
	}
	if opts.WorkspaceDir == "" {
		return "", errors.New("agentsessions: pty runtime requires StartOptions.LogPath or StartOptions.WorkspaceDir")
	}
	logPath := filepath.Join(opts.WorkspaceDir, "logs", "session.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return "", fmt.Errorf("agentsessions: ensure log dir: %w", err)
	}
	return logPath, nil
}

// ptySession is the long-lived PTY-backed Session implementation.
type ptySession struct {
	runtime *ptyRuntime
	// adapter is the per-session CLIAdapter — usually a pointer alias to
	// runtime.cfg.Adapter, but a per-session clone when AutoPlantBootDir
	// fired bare-mode injection. Always non-nil after Start.
	adapter provider.CLIAdapter
	// bootDir is the absolute path of the AutoPlantBootDir-planted tempdir,
	// or "" when no plant happened. Cleaned up exactly once at terminal
	// state via cleanupBootDir.
	bootDir string
	opts    StartOptions

	// cmd / ptmx are the CURRENT attempt's process + master fd. On the
	// supervised path they are replaced each restart. SendInput / Resize
	// take ptmxLock and consult these directly.
	cmd     *exec.Cmd
	ptmx    *os.File
	logFile *os.File

	// legacyCleanup is the sandbox/limits cleanup for the v0.5.0 single-
	// shot path. Run by the legacy waiter goroutine after cmd.Wait. nil
	// on the supervised path (where runSupervised owns per-attempt
	// cleanups directly).
	legacyCleanup func()

	// ptmxLock serializes SendInput / Resize against (a) each other and
	// (b) the wait goroutine's pointer-clear + Close pair. Exclusive
	// (sync.Mutex, not RWMutex) so concurrent SendInput callers do not
	// interleave bytes on the PTY master — turn boundaries on a long-
	// lived TUI matter and Go's *os.File internal locking is not granular
	// enough to guarantee atomic per-input writes for arbitrary payloads.
	//
	// The reader goroutine captures the ptmx local at spawn and does not
	// take this lock at all (see spawnReaderLegacy / runReaderAttempt).
	// Only writers + the destroyer touch this mutex.
	ptmxLock sync.Mutex

	state      atomic.Int32 // LiveState
	alive      atomic.Bool
	startedPID atomic.Int32 // PID at first start; stable across the session
	lastPID    atomic.Int32 // PID of most-recent process (== startedPID for PTY)
	// spawnedAt is the most-recent successful pty.Start time as unix
	// nanoseconds. Set inside spawnAttempt after pty.Start; read by the
	// waiter paths to compute elapsed-since-spawn for the abnormal-wait
	// diagnostic. Zero before the first attempt.
	spawnedAt atomic.Int64

	// lastSessionID stores the most-recently observed provider session ID
	// from EventSessionID. Used by the supervised restart path to feed
	// the next BuildArgs's session-resume slot when Caps.ProviderSessionID
	// is true. Always tracked; only consulted on restart.
	lastSessionID atomic.Value // string

	// activity tracks the most recent ptmx I/O timestamp for idle-kill
	// and the watchdog fallback. Ticks happen unconditionally on Read/
	// Write; the supervisor goroutines are the only consumers.
	activity activityTracker

	done     chan error
	waitOnce sync.Once
	waitCode atomic.Int32
	waitErr  atomic.Value // error

	copyDone chan struct{}

	stopOnce      sync.Once
	stopRequested chan struct{}
}

// spawnAttempt creates a fresh PTY child for attempt N (0-indexed). On
// success, sets s.cmd / s.ptmx (under ptmxLock), updates startedPID /
// lastPID.
// Returns the cmd + ptmx locals (so callers can capture them) and a
// cleanup func to invoke after cmd.Wait completes.
func (s *ptySession) spawnAttempt(attempt int) (*exec.Cmd, *os.File, func(), error) {
	binary, ok := s.adapter.Detect()
	if !ok {
		return nil, nil, nil, fmt.Errorf("agentsessions: adapter %q binary not found", s.adapter.Name())
	}

	// Boot prompt is delivered through exactly one channel — either stdin
	// (BootMode == "stdin", in which case BuildArgs receives an empty
	// system prompt to avoid double injection) or the adapter's argv-
	// supplied system prompt. Per-turn prompts arrive via SendInput.
	systemPrompt := s.opts.BootPrompt
	if s.opts.BootMode == "stdin" {
		systemPrompt = ""
	}

	// SessionID preservation across restart: when the adapter advertises
	// ProviderSessionID and we already observed a session ID on a prior
	// attempt, feed it into BuildArgs's resume slot in place of the
	// original SessionIDPreset. This is the difference between "restart
	// preserves the conversation" and "restart starts a fresh
	// conversation."
	sessionIDPreset := s.opts.SessionIDPreset
	if attempt > 0 && s.runtime.cfg.Caps.ProviderSessionID {
		if sid, _ := s.lastSessionID.Load().(string); sid != "" {
			sessionIDPreset = sid
		}
	}
	args := s.adapter.BuildArgs("", systemPrompt, sessionIDPreset)
	if len(s.opts.ExtraArgs) > 0 {
		args = append(args, s.opts.ExtraArgs...)
	}

	cmd := exec.Command(binary, args...) //nolint:gosec // G204: adapter-sourced binary + args
	configurePTYCommandProcessGroup(cmd)
	cmd.Dir = s.opts.Workdir
	if len(s.opts.Env) > 0 {
		cmd.Env = s.opts.Env
	} else {
		cmd.Env = os.Environ()
	}
	if len(s.opts.ExtraFiles) > 0 {
		cmd.ExtraFiles = s.opts.ExtraFiles
	}

	var sandboxCleanup func()
	if s.opts.Profile.ID != "" {
		cleanup, err := sandbox.Apply(cmd, s.opts.Profile, s.opts.Workdir)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("agentsessions: sandbox apply: %w", err)
		}
		sandboxCleanup = cleanup
	}

	limitCleanup, err := applyResourceLimits(cmd, s.opts.ResourceLimits)
	if err != nil {
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, fmt.Errorf("agentsessions: apply resource limits: %w", err)
	}

	ptmx, err := pty.Start(cmd)
	if err != nil {
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, fmt.Errorf("agentsessions: pty start: %w", err)
	}

	s.ptmxLock.Lock()
	s.cmd = cmd
	s.ptmx = ptmx
	s.ptmxLock.Unlock()

	if cmd.Process != nil {
		if attempt == 0 {
			s.startedPID.Store(int32(cmd.Process.Pid))
		}
		s.lastPID.Store(int32(cmd.Process.Pid))
	}
	s.spawnedAt.Store(time.Now().UnixNano())

	cleanup := func() {
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
	}
	return cmd, ptmx, cleanup, nil
}

// spawnReaderLegacy is the v0.5.0 reader: scans line-delimited output, tees
// to logFile + Fanout, fans events out, ticks activity. Used only on the
// non-supervised lifecycle. Closes copyDone on EOF.
func (s *ptySession) spawnReaderLegacy(ptmx *os.File, ready chan<- struct{}) {
	go func() {
		defer close(s.copyDone)
		close(ready)
		s.runReaderLoop(ptmx)
	}()
}

// runReaderLoop is the body of the reader: scans ptmx, ticks activity per
// line, fans bytes + parsed events out. Returns when the scanner sees EOF
// (typically because the ptmx Close in the waiter / supervisor unblocks
// it). Shared between the legacy waiter and the per-attempt supervised
// reader.
func (s *ptySession) runReaderLoop(ptmx *os.File) {
	var sink io.Writer = s.logFile
	if s.opts.Fanout != nil {
		sink = io.MultiWriter(s.logFile, s.opts.Fanout)
	}

	scanner := bufio.NewScanner(ptmx)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	_, hasParser := s.adapter.(provider.EventParser)

	for scanner.Scan() {
		raw := scanner.Bytes()
		s.tickActivity()
		// Copy the line because scanner.Bytes() reuses its buffer on the
		// next Scan() call. Append a newline back so log readers see line
		// boundaries.
		line := make([]byte, len(raw)+1)
		copy(line, raw)
		line[len(raw)] = '\n'
		_, _ = sink.Write(line)

		// Legacy stream-event parse — feed EventFanout + OnSessionID, and
		// capture the session ID into lastSessionID for restart-preservation
		// on the supervised path.
		if evs, perr := s.adapter.ParseLine(raw); perr == nil {
			for _, ev := range evs {
				if ev.Type == llmtypes.EventSessionID && ev.SessionID != "" {
					s.lastSessionID.Store(ev.SessionID)
					if s.opts.OnSessionID != nil {
						s.opts.OnSessionID(ev.SessionID)
					}
				}
				tryEventFanout(s.opts.EventFanout, ev)
			}
		}

		// Typed-event parse — only when the consumer asked for it.
		if s.opts.TypedEventCallback != nil {
			var typed []pevents.Event
			if hasParser {
				if t, perr := s.adapter.(provider.EventParser).ParseLineEvents(raw); perr == nil {
					typed = t
				}
			}
			for _, te := range typed {
				s.opts.TypedEventCallback(te)
			}
		}
	}
	// Scanner errors on PTY EOF (EIO) are expected; ignored.
}

// spawnWaiterLegacy is the v0.5.0 waiter: blocks on cmd.Wait, nil-clears
// ptmx under the write lock, closes ptmx (which unblocks the reader),
// closes logFile after copyDone, records terminal state, signals s.done.
// Used only on the non-supervised lifecycle.
func (s *ptySession) spawnWaiterLegacy(ptmx *os.File, cmd *exec.Cmd) {
	go func() {
		err := cmd.Wait()

		pid := 0
		if cmd.Process != nil {
			pid = cmd.Process.Pid
		}
		elapsed := time.Duration(0)
		if t0 := s.spawnedAt.Load(); t0 > 0 {
			elapsed = time.Since(time.Unix(0, t0))
		}
		logAbnormalWait("pty", s.runtime.cfg.ID, pid, elapsed, err)

		s.ptmxLock.Lock()
		s.ptmx = nil
		s.ptmxLock.Unlock()

		_ = ptmx.Close()
		<-s.copyDone
		_ = s.logFile.Close()
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))

		s.waitOnce.Do(func() {
			switch {
			case err == nil:
				s.waitCode.Store(0)
			default:
				var ee *exec.ExitError
				if errors.As(err, &ee) {
					s.waitCode.Store(int32(ee.ExitCode()))
				} else {
					s.waitCode.Store(-1)
					s.waitErr.Store(err)
				}
			}
			s.done <- err
			close(s.done)
		})
		if s.legacyCleanup != nil {
			s.legacyCleanup()
		}
		cleanupBootDir(s.bootDir)
	}()
}

// runSupervised owns the supervised lifecycle. On entry, attempt 0 has
// already been spawned (by Start) and its cmd/ptmx/cleanup are passed in.
// The loop:
//
//  1. Run the per-attempt reader + supervisor goroutines.
//  2. Wait for cmd.Wait; tear down supervisor goroutines.
//  3. Examine exit; classify cause.
//  4. If clean exit (PTY EOF) → finalize and return.
//  5. If supervisor-driven kill (idle / watchdog) → finalize, NOT
//     restart-eligible.
//  6. If crash and attempts remain → backoff, OnRestart, spawn next
//     attempt.
//  7. Otherwise (exhausted) → finalize with restart_exhausted.
func (s *ptySession) runSupervised(ctx context.Context, firstCmd *exec.Cmd, firstPtmx *os.File, firstCleanup func(), firstReaderReady chan<- struct{}, firstSupervisorStart <-chan struct{}) {
	sup := s.opts.Supervisor
	// Clamp RestartOnCrash to a non-negative value. A negative value
	// would skip the loop body entirely on the first iteration, leaving
	// the already-spawned first attempt's cmd.Wait unobserved — a leak.
	maxRestarts := sup.RestartOnCrash
	if maxRestarts < 0 {
		maxRestarts = 0
	}
	cmd := firstCmd
	ptmx := firstPtmx
	cleanup := firstCleanup
	var lastExit *ExitError

	defer func() {
		// All-attempts cleanup: log close, doneOnce signal, copyDone close.
		_ = s.logFile.Close()
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
		// copyDone is closed only once at the very end — any external
		// observer waiting on it sees the session terminated.
		select {
		case <-s.copyDone:
		default:
			close(s.copyDone)
		}
		s.waitOnce.Do(func() {
			if lastExit == nil {
				s.waitCode.Store(0)
				s.done <- nil
			} else {
				s.waitCode.Store(int32(lastExit.Code))
				s.waitErr.Store(lastExit)
				s.done <- lastExit
			}
			close(s.done)
		})
		cleanupBootDir(s.bootDir)
	}()

	for attempt := 0; attempt <= maxRestarts; attempt++ {
		var ready chan<- struct{}
		var supervisorStart <-chan struct{}
		if attempt == 0 {
			ready = firstReaderReady
			supervisorStart = firstSupervisorStart
		}
		exit := s.waitOnceSupervised(ctx, cmd, ptmx, attempt, ready, supervisorStart)
		cleanup()
		lastExit = exit

		// Caller-driven Stop or context cancel: never restart, never label
		// restart_exhausted. Stop's SIGTERM may produce a non-zero exit
		// that would otherwise satisfy restartEligible; the explicit
		// short-circuit prevents that misclassification. The exit's Cause
		// stays as observed (empty for Stop's SIGTERM, idle_timeout /
		// watchdog_kill if those fired during drain).
		if ctx.Err() != nil || s.isStopRequested() {
			return
		}

		if exit == nil {
			return // clean exit
		}
		if !restartEligible(exit) {
			return // supervisor-driven kill (idle/watchdog)
		}
		if attempt >= maxRestarts {
			lastExit.Cause = CauseRestartExhausted
			return
		}

		// Restart-eligible: backoff, OnRestart, respawn.
		backoff := computeRestartBackoff(attempt+1, sup.MaxRestartBackoff)
		select {
		case <-ctx.Done():
			return
		case <-s.stopRequested:
			return
		case <-time.After(backoff):
		}
		if sup.OnRestart != nil {
			sup.OnRestart(attempt+1, exit)
		}
		nextCmd, nextPtmx, nextCleanup, err := s.spawnAttempt(attempt + 1)
		if err != nil {
			lastExit = &ExitError{Code: -1, Cause: CauseRestartExhausted, waitErr: err}
			return
		}
		cmd = nextCmd
		ptmx = nextPtmx
		cleanup = nextCleanup
	}
}

// waitOnceSupervised is a single per-attempt lifecycle: spawn reader,
// supervisor goroutines, watch for caller Stop, await cmd.Wait, tear
// everything down, classify the exit. Returns the per-attempt *ExitError
// (nil only on a clean exit).
func (s *ptySession) waitOnceSupervised(ctx context.Context, cmd *exec.Cmd, ptmx *os.File, attempt int, readerReady chan<- struct{}, supervisorStart <-chan struct{}) *ExitError {
	procDone := make(chan struct{})
	readerDone := make(chan struct{})
	cause := &supState{}
	startedAt := time.Now()

	// Reader: drains ptmx until EOF (which fires when ptmx is Close'd
	// after cmd.Wait returns).
	go func() {
		defer close(readerDone)
		if readerReady != nil {
			close(readerReady)
		}
		s.runReaderLoop(ptmx)
	}()

	if supervisorStart != nil {
		select {
		case <-supervisorStart:
		case <-ctx.Done():
		case <-s.stopRequested:
		}
	}

	startedAt = time.Now()
	// Reset activity to attempt-start so idle-kill measures from now, not
	// from the prior attempt's last tick or initial boot-prompt delivery.
	s.activity.tick()

	sup := s.opts.Supervisor
	var supWG sync.WaitGroup
	if sup.IdleKill > 0 {
		supWG.Add(1)
		go func() {
			defer supWG.Done()
			s.superviseIdle(cmd, cause, procDone, startedAt)
		}()
	}
	if sup.WatchdogTimeout > 0 {
		supWG.Add(1)
		go func() {
			defer supWG.Done()
			s.superviseWatchdog(cmd, cause, procDone, startedAt)
		}()
	}

	// Stop / ctx watcher: caller-driven shutdown converts to SIGTERM
	// followed by 5s grace then SIGKILL. cause is left empty so the loop
	// classifies this as ctx cancel (non-restart-eligible) but distinct
	// from idle/watchdog.
	stopWatcherDone := make(chan struct{})
	go func() {
		defer close(stopWatcherDone)
		select {
		case <-procDone:
			return
		case <-s.stopRequested:
			killWithGrace(cmd, 5*time.Second, procDone)
		case <-ctx.Done():
			killWithGrace(cmd, 5*time.Second, procDone)
		}
	}()

	waitErr := cmd.Wait()
	close(procDone)
	supWG.Wait()
	<-stopWatcherDone

	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	elapsed := time.Duration(0)
	if t0 := s.spawnedAt.Load(); t0 > 0 {
		elapsed = time.Since(time.Unix(0, t0))
	}
	logAbnormalWait("pty", s.runtime.cfg.ID, pid, elapsed, waitErr)

	// Nil-clear ptmx so in-flight SendInput / Resize sees ErrNoInputChannel
	// before the close runs. Close unblocks the reader's scanner.
	s.ptmxLock.Lock()
	s.ptmx = nil
	s.ptmxLock.Unlock()
	_ = ptmx.Close()
	<-readerDone

	_ = attempt // reserved for future per-attempt telemetry hooks
	return buildExitError(cmd.ProcessState, waitErr, cause.getCause())
}

func (s *ptySession) isStopRequested() bool {
	select {
	case <-s.stopRequested:
		return true
	default:
		return false
	}
}

// tickActivity records ptmx I/O activity into the internal tracker (used by
// the idle-kill / watchdog goroutines) and fires the consumer's
// Supervisor.ActivityCallback when set, so consumers can observe per-tick
// activity for telemetry / heartbeat purposes. Called from the reader
// goroutine on each line, from SendInput after a successful write, and from
// raw stdin boot-prompt delivery after a successful write.
func (s *ptySession) tickActivity() {
	s.activity.tick()
	if sup := s.opts.Supervisor; sup != nil && sup.ActivityCallback != nil {
		sup.ActivityCallback()
	}
}

// superviseIdle polls activity; if idle exceeds IdleKill, sets cause =
// idle_timeout, sends SIGTERM, then SIGKILL after 5s grace.
func (s *ptySession) superviseIdle(cmd *exec.Cmd, cause *supState, procDone <-chan struct{}, startedAt time.Time) {
	threshold := s.opts.Supervisor.IdleKill
	tick := threshold / 4
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	timer := time.NewTicker(tick)
	defer timer.Stop()

	for {
		select {
		case <-procDone:
			return
		case <-timer.C:
			idle := s.activity.idleSince(startedAt)
			if idle < threshold {
				continue
			}
			if !cause.trySetCause(CauseIdleTimeout) {
				return
			}
			killWithGrace(cmd, 5*time.Second, procDone)
			return
		}
	}
}

// superviseWatchdog polls activity; if no tick within WatchdogTimeout,
// SIGKILLs directly (no SIGTERM grace) and sets cause = watchdog_kill.
//
// When ActivityCallback is non-nil, the watchdog observes it as activity
// (the callback's invocations come from the per-line parser; ptmx I/O
// continues to tick the same activity tracker). When ActivityCallback is
// nil, the watchdog falls back to ptmx Read+Write activity, effectively a
// stricter idle-kill.
func (s *ptySession) superviseWatchdog(cmd *exec.Cmd, cause *supState, procDone <-chan struct{}, startedAt time.Time) {
	threshold := s.opts.Supervisor.WatchdogTimeout
	tick := threshold / 4
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	timer := time.NewTicker(tick)
	defer timer.Stop()

	for {
		select {
		case <-procDone:
			return
		case <-timer.C:
			idle := s.activity.idleSince(startedAt)
			if idle < threshold {
				continue
			}
			if !cause.trySetCause(CauseWatchdogKill) {
				return
			}
			if cmd.Process != nil {
				_ = signalProcessGroup(cmd, syscall.SIGKILL)
			}
			return
		}
	}
}

func (s *ptySession) Wait() (int, error) {
	<-s.done
	code := int(s.waitCode.Load())
	var err error
	if v := s.waitErr.Load(); v != nil {
		err, _ = v.(error)
	}
	return code, err
}

func (s *ptySession) Stop(ctx context.Context) error {
	var killErr error
	s.stopOnce.Do(func() {
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
		// Signal the supervised loop / stop-watcher that caller wants to
		// shut down. Both legacy and supervised paths observe this.
		close(s.stopRequested)

		// Snapshot current cmd under lock — the supervised path may
		// already be between attempts (cmd nil briefly during respawn).
		s.ptmxLock.Lock()
		cmd := s.cmd
		s.ptmxLock.Unlock()
		if cmd == nil || cmd.Process == nil {
			return
		}

		// On the supervised path, the stop-watcher inside waitOnceSupervised
		// will issue the kill — we don't need to here. But we still issue
		// SIGTERM as a belt-and-suspenders to cover the legacy path
		// (which has no stop-watcher) and the supervised between-attempts
		// race window.
		if err := signalProcessGroup(cmd, syscall.SIGTERM); err != nil {
			return
		}
		const grace = 5 * time.Second
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-s.done:
			// Exit observed within grace; the lifecycle goroutines have
			// already torn down.
		case <-timer.C:
			if cmd.Process != nil {
				killErr = signalProcessGroup(cmd, syscall.SIGKILL)
			}
		case <-ctx.Done():
			if cmd.Process != nil {
				killErr = signalProcessGroup(cmd, syscall.SIGKILL)
			}
		}
	})
	return killErr
}

func (s *ptySession) SendInput(_ context.Context, data []byte) error {
	if !s.alive.Load() {
		return ErrNoInputChannel
	}
	s.ptmxLock.Lock()
	defer s.ptmxLock.Unlock()
	if s.ptmx == nil {
		return ErrNoInputChannel
	}
	// Append a trailing newline so the CLI sees a complete line — many TUI
	// agents wait for \n before treating input as a turn boundary.
	payload := data
	if len(payload) == 0 || payload[len(payload)-1] != '\n' {
		payload = append(append([]byte(nil), data...), '\n')
	}
	if _, err := s.ptmx.Write(payload); err != nil {
		return err
	}
	s.tickActivity()
	return nil
}

func (s *ptySession) writeRawInputString(data string) error {
	if !s.alive.Load() {
		return ErrNoInputChannel
	}
	s.ptmxLock.Lock()
	defer s.ptmxLock.Unlock()
	if s.ptmx == nil {
		return ErrNoInputChannel
	}
	if _, err := io.WriteString(s.ptmx, data); err != nil {
		return err
	}
	s.tickActivity()
	return nil
}

func (s *ptySession) Resize(_ context.Context, rows, cols uint16) error {
	// Honor the Capabilities.Resize declaration: when the runtime declares
	// Resize unsupported, this is a no-op even on a PTY-backed session.
	if !s.runtime.cfg.Caps.Resize {
		return nil
	}
	s.ptmxLock.Lock()
	defer s.ptmxLock.Unlock()
	if s.ptmx == nil {
		return nil
	}
	return pty.Setsize(s.ptmx, &pty.Winsize{Rows: rows, Cols: cols})
}

func (s *ptySession) Health() HealthStatus {
	pid := 0
	if s.alive.Load() {
		pid = int(s.startedPID.Load())
	}
	return HealthStatus{
		Alive: s.alive.Load(),
		PID:   pid,
		State: LiveState(s.state.Load()),
	}
}

func (s *ptySession) CheckpointHints() (CheckpointHint, bool) {
	return nil, false
}

// LivePID — PIDReporter.
func (s *ptySession) LivePID() int {
	if !s.alive.Load() {
		return 0
	}
	return int(s.startedPID.Load())
}

// LastPID — PIDReporter.
func (s *ptySession) LastPID() int {
	return int(s.lastPID.Load())
}

// ProviderSessionID — SessionIDer (when Caps.ProviderSessionID=true).
// Returns the most-recently observed session ID, or empty string if none.
func (s *ptySession) ProviderSessionID() string {
	id, _ := s.lastSessionID.Load().(string)
	return id
}

var (
	_ Runtime     = (*ptyRuntime)(nil)
	_ Session     = (*ptySession)(nil)
	_ PIDReporter = (*ptySession)(nil)
)
