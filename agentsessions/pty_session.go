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
// Pattern lifted from agent-mux/internal/provider/cli/claudecode/runtime.go
// + agent-mux/internal/session/runtime.go (RWMutex + wait + lifecycle),
// generalized: any provider.CLIAdapter that opts in via Caps.PTY=true gets
// the same lifecycle. The legacy adapterSession (subprocess-per-turn) is
// untouched; consumers that don't flip the cap see no behavior change.
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
	binary, ok := r.cfg.Adapter.Detect()
	if !ok {
		return nil, fmt.Errorf("agentsessions: adapter %q binary not found", r.cfg.Adapter.Name())
	}

	logPath, err := resolvePTYLogPath(opts)
	if err != nil {
		return nil, err
	}

	// Args: long-lived PTY spawns once. Empty prompt slot; the boot prompt is
	// delivered through exactly one channel — either stdin (BootMode == "stdin",
	// in which case BuildArgs receives an empty system prompt to avoid double
	// injection) or the adapter's argv-supplied system prompt (any other
	// BootMode, in which case BuildArgs receives opts.BootPrompt). Per-turn
	// prompts arrive via SendInput regardless.
	systemPrompt := opts.BootPrompt
	if opts.BootMode == "stdin" {
		systemPrompt = ""
	}
	args := r.cfg.Adapter.BuildArgs("", systemPrompt, opts.SessionIDPreset)

	cmd := exec.Command(binary, args...) //nolint:gosec // G204: adapter-sourced binary + args
	cmd.Dir = opts.Workdir
	if len(opts.Env) > 0 {
		cmd.Env = opts.Env
	} else {
		cmd.Env = os.Environ()
	}
	if len(opts.ExtraFiles) > 0 {
		cmd.ExtraFiles = opts.ExtraFiles
	}

	var sandboxCleanup func()
	if opts.Profile.ID != "" {
		cleanup, err := sandbox.Apply(cmd, opts.Profile, opts.Workdir)
		if err != nil {
			return nil, fmt.Errorf("agentsessions: sandbox apply: %w", err)
		}
		sandboxCleanup = cleanup
	}

	logF, err := os.Create(logPath) //nolint:gosec // G304: workspace-managed path
	if err != nil {
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, fmt.Errorf("agentsessions: open log: %w", err)
	}

	ptmx, err := pty.Start(cmd)
	if err != nil {
		_ = logF.Close()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, fmt.Errorf("agentsessions: pty start: %w", err)
	}

	s := &ptySession{
		runtime:        r,
		opts:           opts,
		cmd:            cmd,
		ptmx:           ptmx,
		logFile:        logF,
		sandboxCleanup: sandboxCleanup,
		done:           make(chan error, 1),
		copyDone:       make(chan struct{}),
	}
	s.alive.Store(true)
	s.state.Store(int32(LiveStateIdle))
	if cmd.Process != nil {
		s.startedPID.Store(int32(cmd.Process.Pid))
		s.lastPID.Store(int32(cmd.Process.Pid))
	}

	if opts.BootMode == "stdin" && opts.BootPrompt != "" {
		if _, werr := io.WriteString(ptmx, opts.BootPrompt); werr != nil {
			// Reap the child to avoid a zombie — pty.Start succeeded so cmd
			// owns a live process. Kill + Wait is the minimal-cleanup pair;
			// errors from either are not actionable here.
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			_ = ptmx.Close()
			_ = logF.Close()
			if sandboxCleanup != nil {
				sandboxCleanup()
			}
			return nil, fmt.Errorf("agentsessions: write boot prompt: %w", werr)
		}
	}

	// Reader and waiter both close over the local ptmx — this avoids a
	// race between the reader reading s.ptmx and the waiter nil-clearing
	// it. The struct field exists only so SendInput/Resize (which take the
	// ptmxLock) can detect "the wait goroutine has cleared this fd."
	s.spawnReader(ptmx)
	s.spawnWaiter(ptmx)

	if opts.AutoFireFirstTurn && len(opts.FirstTurnPayload) > 0 {
		// Skip when the boot-prompt-on-stdin convention already wrote a kickoff.
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
	runtime        *ptyRuntime
	opts           StartOptions
	cmd            *exec.Cmd
	ptmx           *os.File
	logFile        *os.File
	sandboxCleanup func()

	// ptmxLock serializes SendInput / Resize against (a) each other and
	// (b) the wait goroutine's pointer-clear + Close pair. Exclusive
	// (sync.Mutex, not RWMutex) so concurrent SendInput callers do not
	// interleave bytes on the PTY master — turn boundaries on a long-
	// lived TUI matter and Go's *os.File internal locking is not granular
	// enough to guarantee atomic per-input writes for arbitrary payloads.
	//
	// The reader goroutine captures the ptmx local at spawn and does not
	// take this lock at all (see spawnReader). Only writers + the
	// destroyer touch this mutex.
	ptmxLock sync.Mutex

	state      atomic.Int32 // LiveState
	alive      atomic.Bool
	startedPID atomic.Int32 // PID at first start; stable across the session
	lastPID    atomic.Int32 // PID of most-recent process (== startedPID for PTY)

	done     chan error
	waitOnce sync.Once
	waitCode atomic.Int32
	waitErr  atomic.Value // error

	copyDone chan struct{}

	stopOnce           sync.Once
	sandboxCleanupOnce sync.Once
}

// spawnReader runs the PTY-master read goroutine: scans line-delimited
// output, tees raw bytes to logFile + StartOptions.Fanout, and (when the
// adapter parses) fans events out to EventFanout + TypedEventCallback.
//
// PTY merges stderr into stdout at the kernel level — there is no separate
// stderr stream to capture (consumers needing stderr separation should use
// the subprocess-per-turn adapter runtime instead).
func (s *ptySession) spawnReader(ptmx *os.File) {
	go func() {
		defer close(s.copyDone)

		var sink io.Writer = s.logFile
		if s.opts.Fanout != nil {
			sink = io.MultiWriter(s.logFile, s.opts.Fanout)
		}

		scanner := bufio.NewScanner(ptmx)
		scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

		_, hasParser := s.runtime.cfg.Adapter.(provider.EventParser)

		for scanner.Scan() {
			raw := scanner.Bytes()
			// Copy the line because scanner.Bytes() reuses its buffer on
			// the next Scan() call. Append a newline back so log readers
			// see line boundaries.
			line := make([]byte, len(raw)+1)
			copy(line, raw)
			line[len(raw)] = '\n'
			_, _ = sink.Write(line)

			// Legacy stream-event parse — feed EventFanout + OnSessionID.
			if evs, perr := s.runtime.cfg.Adapter.ParseLine(raw); perr == nil {
				for _, ev := range evs {
					if ev.Type == provider.EventSessionID && ev.SessionID != "" && s.opts.OnSessionID != nil {
						s.opts.OnSessionID(ev.SessionID)
					}
					tryEventFanout(s.opts.EventFanout, ev)
				}
			}

			// Typed-event parse — only when the consumer asked for it.
			if s.opts.TypedEventCallback != nil {
				var typed []pevents.Event
				if hasParser {
					if t, perr := s.runtime.cfg.Adapter.(provider.EventParser).ParseLineEvents(raw); perr == nil {
						typed = t
					}
				}
				for _, te := range typed {
					s.opts.TypedEventCallback(te)
				}
			}
		}
		// Scanner errors on PTY EOF (EIO) are expected; ignored. The wait
		// goroutine drives terminal-state recording via cmd.Wait() return.
	}()
}

// spawnWaiter blocks on cmd.Wait, then nil-clears ptmx under the write
// lock so in-flight writers see ErrNoInputChannel before the FD-destroying
// Close runs. Closing ptmx unblocks the reader's scanner.
func (s *ptySession) spawnWaiter(ptmx *os.File) {
	go func() {
		err := s.cmd.Wait()

		// Clear the struct field under the write lock so any in-flight
		// SendInput/Resize sees ptmx==nil and returns ErrNoInputChannel
		// before we close the captured local. Close blocks until the
		// reader goroutine unwinds — we don't hold the lock across it.
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
		s.doSandboxCleanup()
	}()
}

func (s *ptySession) doSandboxCleanup() {
	s.sandboxCleanupOnce.Do(func() {
		if s.sandboxCleanup != nil {
			s.sandboxCleanup()
		}
	})
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
		if s.cmd.Process == nil {
			return
		}
		// SIGTERM → 5s grace → SIGKILL. cmd.Wait observes the resulting exit.
		if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			// Process already gone; nothing left to do.
			return
		}
		const grace = 5 * time.Second
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-s.done:
			// exit observed within grace
		case <-timer.C:
			killErr = s.cmd.Process.Kill()
		case <-ctx.Done():
			killErr = s.cmd.Process.Kill()
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
	return nil
}

func (s *ptySession) Resize(_ context.Context, rows, cols uint16) error {
	// Honor the Capabilities.Resize declaration: when the runtime declares
	// Resize unsupported, this is a no-op even on a PTY-backed session.
	// Matches the Session.Resize doc + Capabilities.Resize semantics.
	if !s.runtime.cfg.Caps.Resize {
		return nil
	}
	s.ptmxLock.Lock()
	defer s.ptmxLock.Unlock()
	if s.ptmx == nil {
		// Session terminated; resize is a clean no-op (no caller-visible error).
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

var (
	_ Runtime     = (*ptyRuntime)(nil)
	_ Session     = (*ptySession)(nil)
	_ PIDReporter = (*ptySession)(nil)
)
