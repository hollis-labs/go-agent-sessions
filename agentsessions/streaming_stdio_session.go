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

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
	pevents "github.com/hollis-labs/go-providers/provider/events"
	"github.com/hollis-labs/go-sandbox/sandbox"
)

// streamingStdioRuntime is the agentsessions.Runtime backed by a long-lived
// non-PTY CLI subprocess speaking NDJSON over stdin/stdout. Selected by
// NewFromAdapter when AdapterRuntimeConfig.Caps.StreamingStdio is true.
//
// Target consumer: Claude mode-5 (`claude -p --input-format stream-json
// --output-format stream-json --verbose`). The child stays alive across
// turns; SendInput frames one JSON object per call (the runtime appends '\n'
// if absent) and writes to stdin under a write lock. The reader goroutine
// scans stdout line by line and dispatches each line through the same
// EventFanout + TypedEventCallback surfaces ptySession uses.
//
// Lifecycle (supervisor, idle-kill, restart-on-crash, watchdog, exit-cause
// classification, restart-preserves-session-id) is identical to ptySession
// — only the I/O loop changes.
type streamingStdioRuntime struct {
	cfg AdapterRuntimeConfig
}

func (r *streamingStdioRuntime) ID() string         { return r.cfg.ID }
func (r *streamingStdioRuntime) Kind() string       { return r.cfg.Kind }
func (r *streamingStdioRuntime) Caps() Capabilities { return r.cfg.Caps }

func (r *streamingStdioRuntime) Prepare(_ context.Context) error {
	if !r.cfg.Caps.BinaryRequired {
		return nil
	}
	if _, ok := r.cfg.Adapter.Detect(); !ok {
		return fmt.Errorf("agentsessions: adapter %q binary not found", r.cfg.Adapter.Name())
	}
	return nil
}

func (r *streamingStdioRuntime) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Workdir == "" {
		return nil, errors.New("agentsessions: StartOptions.Workdir is required for streaming-stdio runtime")
	}

	bootDir, planted, sessionAdapter, err := preparePlant(opts, r.cfg.Adapter, r.cfg.ID)
	if err != nil {
		return nil, err
	}
	opts = planted

	logPath, err := resolveStreamingStdioLogPath(opts)
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, err
	}
	logF, err := os.Create(logPath) //nolint:gosec // G304: workspace-managed path
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, fmt.Errorf("agentsessions: open log: %w", err)
	}

	s := &streamingStdioSession{
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
	// the resume id immediately after Start (before any agent-side
	// session/init event lands). Mirrors adapterSession's behavior.
	// Compliance harness CapsProviderSessionID/PresetCarriedBeforeTurn
	// pins this invariant.
	s.lastSessionID.Store(opts.SessionIDPreset)

	cmd, stdin, stdout, attemptCleanup, err := s.spawnAttempt(0)
	if err != nil {
		_ = logF.Close()
		cleanupBootDir(bootDir)
		return nil, err
	}

	if opts.Supervisor == nil {
		s.legacyCleanup = attemptCleanup
		s.spawnReaderLegacy(stdout)
		s.spawnWaiterLegacy(cmd, stdin, stdout)
	} else {
		go s.runSupervised(ctx, cmd, stdin, stdout, attemptCleanup)
	}

	if opts.AutoFireFirstTurn && len(opts.FirstTurnPayload) > 0 {
		// Skip when the boot-prompt-on-stdin convention already wrote a
		// kickoff during spawnAttempt(0).
		if !(opts.BootMode == "stdin" && opts.BootPrompt != "") {
			if err := s.SendInput(ctx, opts.FirstTurnPayload); err != nil {
				_ = s.Stop(ctx)
				return nil, fmt.Errorf("agentsessions: auto-fire first turn: %w", err)
			}
		}
	}

	return s, nil
}

// resolveStreamingStdioLogPath picks the log destination per the two-dir
// convention: LogPath if set, otherwise <WorkspaceDir>/logs/session.log. One
// must be set — the runtime does not silently discard its log stream.
func resolveStreamingStdioLogPath(opts StartOptions) (string, error) {
	if opts.LogPath != "" {
		return opts.LogPath, nil
	}
	if opts.WorkspaceDir == "" {
		return "", errors.New("agentsessions: streaming-stdio runtime requires StartOptions.LogPath or StartOptions.WorkspaceDir")
	}
	logPath := filepath.Join(opts.WorkspaceDir, "logs", "session.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return "", fmt.Errorf("agentsessions: ensure log dir: %w", err)
	}
	return logPath, nil
}

// streamingStdioSession is the long-lived stdio-backed Session implementation.
type streamingStdioSession struct {
	runtime *streamingStdioRuntime
	// adapter is the per-session CLIAdapter — usually a pointer alias to
	// runtime.cfg.Adapter, but a per-session clone when AutoPlantBootDir
	// fired bare-mode injection. Always non-nil after Start.
	adapter provider.CLIAdapter
	// bootDir is the absolute path of the AutoPlantBootDir-planted tempdir,
	// or "" when no plant happened. Cleaned up exactly once at terminal
	// state via cleanupBootDir.
	bootDir string
	opts    StartOptions

	// cmd / stdin / stdout point at the CURRENT attempt's process + pipes.
	// On the supervised path they are replaced each restart. SendInput
	// takes ioLock and consults stdin directly.
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	logFile *os.File

	legacyCleanup func()

	// ioLock serializes SendInput against itself (so concurrent payloads
	// don't interleave bytes on stdin) and against the wait goroutine's
	// nil-clear + Close pair. The reader goroutine captures stdout at
	// spawn and does not take this lock.
	ioLock sync.Mutex

	state      atomic.Int32 // LiveState
	alive      atomic.Bool
	startedPID atomic.Int32
	lastPID    atomic.Int32
	// spawnedAt is the most-recent successful cmd.Start time as unix
	// nanoseconds. Set inside spawnAttempt after cmd.Start; read by the
	// waiter paths to compute elapsed-since-spawn for the abnormal-wait
	// diagnostic. Zero before the first attempt.
	spawnedAt atomic.Int64

	lastSessionID atomic.Value // string

	activity activityTracker

	done     chan error
	waitOnce sync.Once
	waitCode atomic.Int32
	waitErr  atomic.Value // error

	copyDone chan struct{}

	stopOnce      sync.Once
	stopRequested chan struct{}
}

// spawnAttempt creates a fresh non-PTY child for attempt N (0-indexed). On
// success, sets s.cmd / s.stdin / s.stdout (under ioLock), updates pid
// counters, and writes the boot prompt on attempt 0 when BootMode=stdin.
// Returns the cmd + pipes (so callers can capture them) and a cleanup func
// to invoke after cmd.Wait completes.
func (s *streamingStdioSession) spawnAttempt(attempt int) (*exec.Cmd, io.WriteCloser, io.ReadCloser, func(), error) {
	binary, ok := s.adapter.Detect()
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: adapter %q binary not found", s.adapter.Name())
	}

	systemPrompt := s.opts.BootPrompt
	if s.opts.BootMode == "stdin" {
		systemPrompt = ""
	}

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
	configureCommandProcessGroup(cmd)
	cmd.Dir = s.opts.Workdir
	if len(s.opts.Env) > 0 {
		cmd.Env = s.opts.Env
	} else {
		cmd.Env = os.Environ()
	}
	if len(s.opts.ExtraFiles) > 0 {
		cmd.ExtraFiles = s.opts.ExtraFiles
	}
	// Route stderr: caller-supplied writer wins; otherwise tee into the
	// log file so diagnostics aren't lost. Keep stderr separate from
	// stdout so NDJSON parsing isn't confused by interleaved diagnostics.
	if s.opts.Stderr != nil {
		cmd.Stderr = s.opts.Stderr
	} else {
		cmd.Stderr = s.logFile
	}

	var sandboxCleanup func()
	if s.opts.Profile.ID != "" {
		cleanup, err := sandbox.Apply(cmd, s.opts.Profile, s.opts.Workdir)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("agentsessions: sandbox apply: %w", err)
		}
		sandboxCleanup = cleanup
	}

	limitCleanup, err := applyResourceLimits(cmd, s.opts.ResourceLimits)
	if err != nil {
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: apply resource limits: %w", err)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: start: %w", err)
	}

	s.ioLock.Lock()
	s.cmd = cmd
	s.stdin = stdin
	s.stdout = stdout
	s.ioLock.Unlock()

	if cmd.Process != nil {
		if attempt == 0 {
			s.startedPID.Store(int32(cmd.Process.Pid))
		}
		s.lastPID.Store(int32(cmd.Process.Pid))
	}
	s.spawnedAt.Store(time.Now().UnixNano())

	if attempt == 0 && s.opts.BootMode == "stdin" && s.opts.BootPrompt != "" {
		// Caller frames the boot prompt — we append nothing here. SendInput
		// is the framing path; the boot-prompt write goes verbatim.
		if _, werr := io.WriteString(stdin, s.opts.BootPrompt); werr != nil {
			_ = signalProcessGroup(cmd, syscall.SIGKILL)
			_ = cmd.Wait()
			_ = stdin.Close()
			_ = stdout.Close()
			limitCleanup()
			if sandboxCleanup != nil {
				sandboxCleanup()
			}
			return nil, nil, nil, nil, fmt.Errorf("agentsessions: write boot prompt: %w", werr)
		}
		s.tickActivity()
	}

	cleanup := func() {
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
	}
	return cmd, stdin, stdout, cleanup, nil
}

func (s *streamingStdioSession) spawnReaderLegacy(stdout io.Reader) {
	go func() {
		defer close(s.copyDone)
		s.runReaderLoop(stdout)
	}()
}

// runReaderLoop scans stdout line by line, tees to logFile + Fanout, fans
// out parsed events, ticks activity. Shared between legacy and supervised
// paths. Returns when the scanner sees EOF (typically because the child
// exited and the pipe closed).
func (s *streamingStdioSession) runReaderLoop(stdout io.Reader) {
	var sink io.Writer = s.logFile
	if s.opts.Fanout != nil {
		sink = io.MultiWriter(s.logFile, s.opts.Fanout)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	_, hasParser := s.adapter.(provider.EventParser)

	for scanner.Scan() {
		raw := scanner.Bytes()
		s.tickActivity()
		line := make([]byte, len(raw)+1)
		copy(line, raw)
		line[len(raw)] = '\n'
		_, _ = sink.Write(line)

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
}

// spawnWaiterLegacy is the single-shot waiter: blocks on cmd.Wait, closes
// pipes (which unblocks the reader), records terminal state, signals
// s.done.
func (s *streamingStdioSession) spawnWaiterLegacy(cmd *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser) {
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
		logAbnormalWait("streaming-stdio", s.runtime.cfg.ID, pid, elapsed, err)

		s.ioLock.Lock()
		s.stdin = nil
		s.stdout = nil
		s.ioLock.Unlock()

		_ = stdin.Close()
		_ = stdout.Close()
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

// runSupervised owns the supervised lifecycle. Mirrors ptySession.runSupervised
// — only the per-attempt I/O setup (waitOnceSupervised) differs.
func (s *streamingStdioSession) runSupervised(ctx context.Context, firstCmd *exec.Cmd, firstStdin io.WriteCloser, firstStdout io.ReadCloser, firstCleanup func()) {
	sup := s.opts.Supervisor
	maxRestarts := sup.RestartOnCrash
	if maxRestarts < 0 {
		maxRestarts = 0
	}
	cmd := firstCmd
	stdin := firstStdin
	stdout := firstStdout
	cleanup := firstCleanup
	var lastExit *ExitError

	defer func() {
		_ = s.logFile.Close()
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
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
		exit := s.waitOnceSupervised(ctx, cmd, stdin, stdout, attempt)
		cleanup()
		lastExit = exit

		if ctx.Err() != nil || s.isStopRequested() {
			return
		}

		if exit == nil {
			return // clean exit
		}
		if !restartEligible(exit) {
			return // supervisor-driven kill
		}
		if attempt >= maxRestarts {
			lastExit.Cause = CauseRestartExhausted
			return
		}

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
		nextCmd, nextStdin, nextStdout, nextCleanup, err := s.spawnAttempt(attempt + 1)
		if err != nil {
			lastExit = &ExitError{Code: -1, Cause: CauseRestartExhausted, waitErr: err}
			return
		}
		cmd = nextCmd
		stdin = nextStdin
		stdout = nextStdout
		cleanup = nextCleanup
	}
}

func (s *streamingStdioSession) waitOnceSupervised(ctx context.Context, cmd *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, attempt int) *ExitError {
	procDone := make(chan struct{})
	readerDone := make(chan struct{})
	cause := &supState{}
	startedAt := time.Now()
	s.activity.tick()

	go func() {
		defer close(readerDone)
		s.runReaderLoop(stdout)
	}()

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
	logAbnormalWait("streaming-stdio", s.runtime.cfg.ID, pid, elapsed, waitErr)

	s.ioLock.Lock()
	s.stdin = nil
	s.stdout = nil
	s.ioLock.Unlock()
	_ = stdin.Close()
	_ = stdout.Close()
	<-readerDone

	_ = attempt
	return buildExitError(cmd.ProcessState, waitErr, cause.getCause())
}

func (s *streamingStdioSession) isStopRequested() bool {
	select {
	case <-s.stopRequested:
		return true
	default:
		return false
	}
}

func (s *streamingStdioSession) tickActivity() {
	s.activity.tick()
	if sup := s.opts.Supervisor; sup != nil && sup.ActivityCallback != nil {
		sup.ActivityCallback()
	}
}

func (s *streamingStdioSession) superviseIdle(cmd *exec.Cmd, cause *supState, procDone <-chan struct{}, startedAt time.Time) {
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

func (s *streamingStdioSession) superviseWatchdog(cmd *exec.Cmd, cause *supState, procDone <-chan struct{}, startedAt time.Time) {
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

func (s *streamingStdioSession) Wait() (int, error) {
	<-s.done
	code := int(s.waitCode.Load())
	var err error
	if v := s.waitErr.Load(); v != nil {
		err, _ = v.(error)
	}
	return code, err
}

func (s *streamingStdioSession) Stop(ctx context.Context) error {
	var killErr error
	s.stopOnce.Do(func() {
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
		close(s.stopRequested)

		s.ioLock.Lock()
		cmd := s.cmd
		stdin := s.stdin
		s.ioLock.Unlock()
		// Close stdin first — well-behaved CLI agents (Claude mode-5
		// included) exit cleanly on EOF. Give the child a short grace
		// window to do so before escalating to SIGTERM/SIGKILL.
		if stdin != nil {
			_ = stdin.Close()
		}
		if cmd == nil || cmd.Process == nil {
			return
		}

		const eofGrace = 2 * time.Second
		eofTimer := time.NewTimer(eofGrace)
		defer eofTimer.Stop()
		select {
		case <-s.done:
			return
		case <-eofTimer.C:
		case <-ctx.Done():
			// caller-cancelled wait — fall through to signal-based escalation
		}

		if err := signalProcessGroup(cmd, syscall.SIGTERM); err != nil {
			return
		}
		const grace = 5 * time.Second
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-s.done:
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

func (s *streamingStdioSession) SendInput(_ context.Context, data []byte) error {
	if !s.alive.Load() {
		return ErrNoInputChannel
	}
	s.ioLock.Lock()
	defer s.ioLock.Unlock()
	if s.stdin == nil {
		return ErrNoInputChannel
	}
	payload := data
	if len(payload) == 0 || payload[len(payload)-1] != '\n' {
		payload = append(append([]byte(nil), data...), '\n')
	}
	if _, err := s.stdin.Write(payload); err != nil {
		return err
	}
	s.tickActivity()
	return nil
}

func (s *streamingStdioSession) Resize(_ context.Context, _ uint16, _ uint16) error {
	// No PTY to resize. Honor the Caps.Resize declaration: this runtime
	// declares it false by default; even if a caller flips it, Resize on a
	// stdio child is a no-op.
	return nil
}

func (s *streamingStdioSession) Health() HealthStatus {
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

func (s *streamingStdioSession) CheckpointHints() (CheckpointHint, bool) {
	if !s.runtime.cfg.Caps.CheckpointResume {
		return nil, false
	}
	id, _ := s.lastSessionID.Load().(string)
	if id == "" {
		return nil, false
	}
	return CheckpointHint(id), true
}

func (s *streamingStdioSession) LivePID() int {
	if !s.alive.Load() {
		return 0
	}
	return int(s.startedPID.Load())
}

func (s *streamingStdioSession) LastPID() int {
	return int(s.lastPID.Load())
}

func (s *streamingStdioSession) ProviderSessionID() string {
	id, _ := s.lastSessionID.Load().(string)
	return id
}

var (
	_ Runtime     = (*streamingStdioRuntime)(nil)
	_ Session     = (*streamingStdioSession)(nil)
	_ PIDReporter = (*streamingStdioSession)(nil)
)
