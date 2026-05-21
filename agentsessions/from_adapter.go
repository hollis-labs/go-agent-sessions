package agentsessions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
	"github.com/hollis-labs/go-runner/runner"
)

// AdapterRuntimeConfig configures a Runtime backed by a
// go-providers.CLIAdapter. Each SendInput drives a fresh runner.Run that
// invokes the underlying CLI binary; a turn ends when the runner emits
// EventProcessExited / EventProcessTimeout.
type AdapterRuntimeConfig struct {
	// ID is the runtime's stable identifier. Required.
	ID string

	// Kind is the free-form classification token (e.g. "cli"). Required.
	Kind string

	// Adapter is the go-providers CLIAdapter to drive. Required.
	Adapter provider.CLIAdapter

	// Caps declares the static capability set Sessions produced by this
	// Runtime expose. Default zero value declares no capabilities.
	Caps Capabilities

	// BuildArgs, when non-nil, overrides the adapter's BuildArgs for
	// constructing per-turn CLI argv. The default uses adapter.BuildArgs
	// with the prompt = SendInput payload, an empty system prompt, and
	// the StartOptions.SessionIDPreset on the first turn (then the most
	// recent observed session_id). Most consumers leave this nil.
	BuildArgs func(prompt, sessionID string) []string

	// WaitDelay is the grace period between SIGTERM (on context cancel)
	// and SIGKILL passed to runner.Run. Zero uses the runner's default.
	WaitDelay time.Duration
}

// NewFromAdapter constructs a Runtime backed by cfg.Adapter. Runtime
// shape is selected by the lifecycle flag in cfg.Caps:
//
//   - cfg.Caps.PTY == false (default): subprocess-per-turn runtime.
//     Each SendInput drives a fresh runner.Run that invokes the underlying
//     CLI binary; a turn ends when the runner emits EventProcessExited.
//     Single-turn-in-flight semantics — a second SendInput while a turn is
//     running returns ErrTurnInFlight without queueing.
//
//   - cfg.Caps.PTY == true: long-lived PTY runtime. The adapter binary is
//     spawned once at Start time under a creack/pty master; SendInput writes
//     bytes to the PTY master. Conversation / MCP / tool-affordance state
//     persists across turns inside the long-lived child. Resize works.
//     SendInput is non-blocking; the consumer is responsible for any
//     turn-boundary discipline at the application layer (the lib does not
//     impose ErrTurnInFlight on PTY because turn boundaries on a PTY are
//     CLI-defined, not lib-defined).
//
// Caps().BinaryRequired is honored on both shapes — Prepare returns an
// error if the adapter's Detect() finds no binary.
//
// Capability-driven selection means consumers do not pick a constructor;
// they declare what their adapter supports via Caps and the lib routes to
// the right implementation. The acceptance contract is that flipping
// cfg.Caps.PTY does not silently change other observable behavior beyond
// the lifecycle shape (existing adapters with Caps.PTY=false are
// unaffected).
func NewFromAdapter(cfg AdapterRuntimeConfig) (Runtime, error) {
	if cfg.ID == "" {
		return nil, errors.New("agentsessions: AdapterRuntimeConfig.ID is required")
	}
	if cfg.Adapter == nil {
		return nil, errors.New("agentsessions: AdapterRuntimeConfig.Adapter is required")
	}
	if err := cfg.Caps.validateLifecycle(); err != nil {
		return nil, err
	}
	if cfg.Kind == "" {
		cfg.Kind = "cli"
	}
	switch {
	case cfg.Caps.PTY:
		return &ptyRuntime{cfg: cfg}, nil
	case cfg.Caps.StreamingStdio:
		return &streamingStdioRuntime{cfg: cfg}, nil
	case cfg.Caps.JsonRpcStdio:
		return &jsonRpcStdioRuntime{cfg: cfg}, nil
	case cfg.Caps.ServeHTTP:
		return &serveHTTPRuntime{cfg: cfg}, nil
	default:
		return &adapterRuntime{cfg: cfg}, nil
	}
}

// adapterRuntime wraps a CLIAdapter as a Runtime.
type adapterRuntime struct {
	cfg AdapterRuntimeConfig
}

func (r *adapterRuntime) ID() string         { return r.cfg.ID }
func (r *adapterRuntime) Kind() string       { return r.cfg.Kind }
func (r *adapterRuntime) Caps() Capabilities { return r.cfg.Caps }

func (r *adapterRuntime) Prepare(ctx context.Context) error {
	if !r.cfg.Caps.BinaryRequired {
		return nil
	}
	if _, ok := r.cfg.Adapter.Detect(); !ok {
		return fmt.Errorf("agentsessions: adapter %q binary not found", r.cfg.Adapter.Name())
	}
	return nil
}

func (r *adapterRuntime) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Workdir == "" {
		return nil, errors.New("agentsessions: StartOptions.Workdir is required for adapter runtime")
	}

	bootDir, planted, sessionAdapter, err := preparePlant(opts, r.cfg.Adapter, r.cfg.ID)
	if err != nil {
		return nil, err
	}
	opts = planted

	s := &adapterSession{
		runtime:   r,
		adapter:   sessionAdapter,
		bootDir:   bootDir,
		opts:      opts,
		buildArgs: r.cfg.BuildArgs,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	s.sessionID.Store(opts.SessionIDPreset)
	s.alive.Store(true)
	s.state.Store(int32(LiveStateIdle))
	if s.buildArgs == nil {
		s.buildArgs = func(prompt, sessionID string) []string {
			return sessionAdapter.BuildArgs(prompt, "", sessionID)
		}
	}
	// ExtraArgs splice composes over whatever buildArgs is in use (caller-
	// supplied or default). Captures len at Start; opts is value-copied
	// into the session struct so post-Start mutation by the caller does
	// not retroactively rewrite per-turn argv.
	if len(opts.ExtraArgs) > 0 {
		inner := s.buildArgs
		extra := append([]string(nil), opts.ExtraArgs...)
		s.buildArgs = func(prompt, sessionID string) []string {
			args := inner(prompt, sessionID)
			return append(args, extra...)
		}
	}

	if opts.AutoFireFirstTurn && len(opts.FirstTurnPayload) > 0 {
		// Synchronous first turn: subprocess-per-turn semantics mean the
		// runner.Run completes before SendInput returns. Start blocks until
		// the kickoff turn finishes — eliminates the Launch/SendInput race
		// at the cost of a longer Start. Consumers that don't want this
		// latency leave AutoFireFirstTurn=false and call SendInput on their
		// own schedule.
		if err := s.SendInput(ctx, opts.FirstTurnPayload); err != nil {
			_ = s.Stop(ctx)
			return nil, fmt.Errorf("agentsessions: auto-fire first turn: %w", err)
		}
	}
	return s, nil
}

// adapterSession is the Session a CLIAdapter produces. SendInput drives
// runner.Run; Stop closes stopCh, which triggers any in-flight runner
// context cancel; Wait blocks on done (closed when Stop has fully
// drained).
type adapterSession struct {
	runtime *adapterRuntime
	// adapter is the per-session CLIAdapter — usually a pointer alias to
	// runtime.cfg.Adapter, but a per-session clone when AutoPlantBootDir
	// fired bare-mode injection. Always non-nil after Start.
	adapter provider.CLIAdapter
	// bootDir is the absolute path of the AutoPlantBootDir-planted tempdir,
	// or "" when no plant happened. Cleaned up exactly once at terminal
	// state (Stop) via cleanupBootDir.
	bootDir   string
	opts      StartOptions
	buildArgs func(prompt, sessionID string) []string

	sessionID atomic.Value // string — last observed provider session_id

	state   atomic.Int32 // LiveState
	alive   atomic.Bool
	pid     atomic.Int32 // live PID — resets to 0 between turns
	lastPID atomic.Int32 // sticky most-recent PID — survives turn boundaries
	turnID  atomic.Value // string

	stopCh   chan struct{}
	stopOnce sync.Once

	done     chan struct{}
	doneOnce sync.Once
	exitCode atomic.Int32

	// turnMu serializes SendInput at the adapter layer. The Manager
	// also serializes via inputMu, but turnMu lets adapter consumers
	// (compliance harness, integration tests) hit the same guarantee
	// when bypassing the Manager.
	turnMu       sync.Mutex
	turnInFlight atomic.Bool
}

func (s *adapterSession) Wait() (int, error) {
	<-s.done
	return int(s.exitCode.Load()), nil
}

func (s *adapterSession) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
		close(s.stopCh)
	})
	// Stop is non-blocking on adapter sessions: there is no long-lived
	// process to drain; turn-in-flight is cancelled via stopCh, which
	// the runner's context observes.
	s.doneOnce.Do(func() {
		close(s.done)
		cleanupBootDir(s.bootDir)
	})
	return nil
}

func (s *adapterSession) SendInput(ctx context.Context, data []byte) error {
	if !s.alive.Load() {
		return ErrNoInputChannel
	}
	if !s.turnInFlight.CompareAndSwap(false, true) {
		return ErrTurnInFlight
	}
	defer s.turnInFlight.Store(false)

	s.turnMu.Lock()
	defer s.turnMu.Unlock()

	prompt := string(data)
	sessionID, _ := s.sessionID.Load().(string)
	args := s.buildArgs(prompt, sessionID)

	turnID := defaultIDFn()
	s.turnID.Store(turnID)
	s.state.Store(int32(LiveStateProcessing))
	defer s.state.Store(int32(LiveStateIdle))

	// Cancel the run if Stop fires while the runner is mid-flight.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-s.stopCh:
			cancel()
		case <-runCtx.Done():
		}
	}()

	cfg := runner.Config{
		Provider:   s.adapter,
		Profile:    s.opts.Profile,
		Workspace:  s.opts.Workdir,
		Args:       args,
		Env:        s.opts.Env,
		Stderr:     s.opts.Stderr,
		ExtraFiles: s.opts.ExtraFiles,
		WaitDelay:  s.runtime.cfg.WaitDelay,
		OnEvent:    s.handleRunnerEvent,
	}
	// StartOptions.Supervisor + ResourceLimits are PTY-only in v0.6.0.
	// Forwarding them to runner.Config.Supervisor / .ResourceLimits on
	// the adapter path remains a follow-up increment — see CHANGELOG
	// "Out of scope".
	err := runner.Run(runCtx, cfg)
	if err != nil {
		s.exitCode.Store(1)
		return err
	}
	return nil
}

// handleRunnerEvent forwards runner events to the Fanout writer (which
// the Manager has wired to the attach broker if AttachEnabled). It also
// captures process pid + the first observed session id so Health reports
// the live state and OnSessionID fires once per turn.
func (s *adapterSession) handleRunnerEvent(ev runner.Event) {
	switch ev.Kind {
	case runner.EventProcessStarted:
		if pid, ok := ev.Payload["pid"].(int); ok {
			s.pid.Store(int32(pid))
			s.lastPID.Store(int32(pid))
		}
	case runner.EventProviderEvent:
		if pe, ok := ev.Payload["event"].(llmtypes.StreamEvent); ok {
			if pe.Type == llmtypes.EventSessionID && pe.SessionID != "" {
				s.sessionID.Store(pe.SessionID)
				if s.opts.OnSessionID != nil {
					s.opts.OnSessionID(pe.SessionID)
				}
			}
			tryEventFanout(s.opts.EventFanout, pe)
			if s.opts.Fanout != nil {
				if line, ok := encodeStreamEvent(pe); ok {
					_, _ = s.opts.Fanout.Write(line)
				}
			}
		}
	case runner.EventProcessExited:
		s.pid.Store(0)
	case runner.EventProcessTimeout:
		s.pid.Store(0)
	}
}

func (s *adapterSession) Resize(ctx context.Context, rows, cols uint16) error {
	// Subprocess adapters have no PTY to resize.
	return nil
}

func (s *adapterSession) Health() HealthStatus {
	turnID, _ := s.turnID.Load().(string)
	return HealthStatus{
		Alive:  s.alive.Load(),
		PID:    int(s.pid.Load()),
		State:  LiveState(s.state.Load()),
		TurnID: turnID,
	}
}

func (s *adapterSession) CheckpointHints() (CheckpointHint, bool) {
	if !s.runtime.cfg.Caps.CheckpointResume {
		return nil, false
	}
	id, _ := s.sessionID.Load().(string)
	if id == "" {
		return nil, false
	}
	return CheckpointHint(id), true
}

// ProviderSessionID implements SessionIDer when the runtime declares
// Caps().ProviderSessionID == true. Returns the empty string before the
// first turn observes a session id.
func (s *adapterSession) ProviderSessionID() string {
	id, _ := s.sessionID.Load().(string)
	return id
}

// LivePID — PIDReporter. Subprocess-per-turn semantics: 0 between turns,
// non-zero only while a turn's runner.Run is active.
func (s *adapterSession) LivePID() int { return int(s.pid.Load()) }

// LastPID — PIDReporter. Sticky most-recent PID across turn boundaries;
// useful for log correlation post-exit.
func (s *adapterSession) LastPID() int { return int(s.lastPID.Load()) }

var _ PIDReporter = (*adapterSession)(nil)

// encodeStreamEvent renders ev for the attach broker. Plain-text
// EventDelta is forwarded as-is; everything else is rendered as a short
// labelled line so subscribers can see turn boundaries / errors / usage
// without the lib having to define a wire format. Adapters that want a
// richer wire format (jsonl, etc.) substitute their own Fanout-shaped
// writer at the consumer layer — this is just the default tee.
func encodeStreamEvent(ev llmtypes.StreamEvent) ([]byte, bool) {
	switch ev.Type {
	case llmtypes.EventDelta:
		if ev.Content == "" {
			return nil, false
		}
		return []byte(ev.Content), true
	case llmtypes.EventToolUse:
		if ev.ToolUse == nil {
			return nil, false
		}
		return []byte(fmt.Sprintf("\n[tool_use:%s]\n", ev.ToolUse.Name)), true
	case llmtypes.EventError:
		return []byte(fmt.Sprintf("\n[error] %s\n", ev.Error)), true
	case llmtypes.EventDone:
		return []byte("\n[turn_done]\n"), true
	}
	return nil, false
}
