package agentsessions

import (
	"context"
	"errors"
	"io"

	"github.com/hollis-labs/go-providers/provider"
	"github.com/hollis-labs/go-sandbox/sandbox"
)

// State is the process-level lifecycle state the Manager records and
// publishes to consumer StateSinks. The set is fixed at four values;
// consumers map their domain FSM (clockwork tasks, mux logical agents)
// on top.
type State string

const (
	StateLaunching State = "launching"
	StateRunning   State = "running"
	StateDone      State = "done"
	StateFailed    State = "failed"
)

// LiveState describes the within-running fine-grained sub-state reported
// by Session.Health. It disambiguates idle (waiting for input) from
// processing (a turn is in flight) so consumers can decide whether to
// send another turn without polling for ErrTurnInFlight.
type LiveState int

const (
	// LiveStateIdle: session is alive and waiting for input. No turn in flight.
	LiveStateIdle LiveState = iota
	// LiveStateProcessing: a turn or subprocess is currently running.
	LiveStateProcessing
	// LiveStateStopped: Stop has been called; Wait will return soon.
	LiveStateStopped
)

// String returns the JSON/API string representation of a LiveState.
func (s LiveState) String() string {
	switch s {
	case LiveStateIdle:
		return "idle"
	case LiveStateProcessing:
		return "processing"
	case LiveStateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Capabilities declares what a Runtime's Sessions support beyond the
// baseline Runtime + Session contract. All fields default to false. Caps
// returns the static capability declaration; the value is immutable for
// the Runtime's lifetime and safe to call concurrently.
type Capabilities struct {
	// PTY: Session.SendInput writes to a live PTY master. Resize is
	// meaningful. False for turn-based subprocess adapters.
	PTY bool

	// Resize: Session.Resize has an observable effect. Requires PTY=true
	// to be meaningful; non-PTY adapters no-op Resize.
	Resize bool

	// ProviderSessionID: the adapter observes and stores a provider-side
	// session ID (e.g. claude --resume ID) for cross-session continuity.
	// When true, the Session implements SessionIDer.
	ProviderSessionID bool

	// CheckpointResume: Session.CheckpointHints returns a non-trivial
	// hint (_, true). Consumers may use it for cross-session continuity.
	CheckpointResume bool

	// BinaryRequired: Prepare returns an error if the underlying binary
	// is absent. False only for in-process providers.
	BinaryRequired bool
}

// HealthStatus snapshots a live session's liveness. PID is meaningful
// only for PTY runtimes; turn-based adapters report PID=0 between turns
// or the PID of the current subprocess. State and TurnID are always set
// by Health(); the zero value (LiveStateIdle, "") is safe for adapters
// that don't distinguish idle from processing.
type HealthStatus struct {
	Alive  bool
	PID    int
	State  LiveState
	TurnID string
}

// CheckpointHint is opaque, provider-defined data the adapter exposes for
// cross-session continuity. The shape is deliberately unpinned — consumers
// that interpret hints know the adapter they're paired with.
type CheckpointHint []byte

// CheckpointHinter is the optional interface a Session implements when
// the underlying adapter has a meaningful checkpoint hint. Caps()
// .CheckpointResume == true is the discoverability signal; Session.
// CheckpointHints is the always-callable accessor that returns
// (zero, false) when the adapter has nothing to report.
type CheckpointHinter interface {
	CheckpointHint() (CheckpointHint, bool)
}

// SessionIDer is an optional interface implemented by Session when the
// adapter tracks a provider-side session ID (Caps().ProviderSessionID ==
// true). Callers must type-assert to this interface; it is not part of
// the core Session contract.
type SessionIDer interface {
	ProviderSessionID() string
}

// StartOptions bundles the runtime-agnostic state a Runtime needs to
// spawn a Session. Fanout, when non-nil, receives a copy of the session's
// output stream so the Manager can broadcast to attach subscribers — CLI
// runtimes tee PTY bytes / parsed event lines into it; API runtimes
// forward streamed assistant tokens. EventFanout, when non-nil, receives
// the parsed provider.StreamEvent values alongside the byte Fanout.
type StartOptions struct {
	// Workdir is the absolute path used as the spawned process's working
	// directory and as the workspace argument to sandbox.Apply.
	Workdir string

	// LogPath is the absolute path of the per-session log file the
	// adapter writes to. Optional.
	LogPath string

	// BootPrompt is the initial system / boot prompt the adapter feeds
	// to the underlying agent on first start. Adapter-defined.
	BootPrompt string

	// BootMode is a free-form mode token interpreted by the adapter
	// (e.g. "interactive", "noninteractive", "review"). Adapter-defined.
	BootMode string

	// Env is the environment to forward to the spawned process.
	// Consumers that want allowlisted egress merge go-egress-proxy's
	// EnvVars() into this slice before calling Start.
	Env []string

	// Stderr, when non-nil, is forwarded to runner.Config.Stderr in the
	// adapter-driven runtime so the spawned subprocess's stderr writes
	// land on the caller-supplied writer. Use io.MultiWriter to fan out
	// (e.g. an in-memory tail buffer plus a sidecar log file). Nil leaves
	// runner-level stderr handling at its default (cmd.Stderr unset →
	// os/exec routes to os.DevNull). This field is a no-op for the
	// provider-driven runtime (HTTP transport, no subprocess).
	Stderr io.Writer

	// Profile is the resolved go-sandbox profile. The zero-value
	// sandbox.Profile (empty ID) disables sandbox wrapping. A non-zero
	// profile on an unsupported platform is a hard launch failure (per
	// go-sandbox).
	Profile sandbox.Profile

	// Fanout receives a tee of session output for the attach broker. Set
	// by the Manager — adapters do not allocate this.
	Fanout io.Writer

	// EventFanout receives parsed provider.StreamEvent values as a
	// best-effort mirror of the session stream. Sends are non-blocking;
	// when the channel is full the event is dropped silently. Callers
	// should supply a buffered channel sized to their consumer's
	// tolerance and must not close it before the session is done.
	EventFanout chan<- provider.StreamEvent

	// SessionIDPreset, when non-empty, is the provider-side session id
	// the adapter should use as `--resume <id>` on the very first turn.
	// Used by claude-stream-style adapters that support resume.
	// Adapters that don't understand this field ignore it silently.
	SessionIDPreset string

	// OnSessionID, when non-nil, is invoked by the adapter the first
	// time it observes a provider-assigned session id (e.g. claude's
	// system/init event). Callers typically persist it to survive
	// daemon restarts. Called from the adapter's read goroutine —
	// callers must not block inside it.
	OnSessionID func(id string)

	// AttachEnabled, when true, asks the Manager to spin an attach
	// broker for this Session. When false, attach calls return
	// ErrAttachDisabled and no broker memory is allocated. Default off
	// — turn it on per-session for sessions that expect interactive
	// attach.
	AttachEnabled bool

	// RingBytes overrides the attach-broker ring size for this Session.
	// Zero uses the Manager default (64 KiB).
	RingBytes int

	// SubscriberDepth overrides the attach-broker subscriber channel
	// depth. Zero uses the Manager default (64 chunks).
	SubscriberDepth int
}

// Runtime is the high-level contract a session-spawner satisfies. It
// names itself, declares capabilities, validates configuration via
// Prepare, and spawns a Session via Start.
//
// Runtimes are immutable after construction and safe for concurrent calls
// — the same Runtime instance can spawn many Sessions in parallel.
type Runtime interface {
	// ID returns the runtime's stable identifier (e.g.
	// "claudestream", "anthropic-api", "pty-claude").
	ID() string

	// Kind returns a free-form classification token consumers may branch
	// on (e.g. "cli", "api", "pty"). Not enumerated by the library.
	Kind() string

	// Caps returns the static capability declaration. Immutable for the
	// lifetime of the Runtime; safe to call concurrently.
	Caps() Capabilities

	// Prepare runs configuration-dependent validation that should
	// surface errors before Start (missing binary, invalid API key,
	// etc.). Called once per launch by Manager.Start; consumers can call
	// it independently when probing.
	Prepare(ctx context.Context) error

	// Start spawns a Session with opts. The returned Session must be
	// alive on return — Health().Alive == true — or Start should return
	// an error. Errors include: sandbox apply failure, missing binary,
	// invalid options.
	Start(ctx context.Context, opts StartOptions) (Session, error)
}

// Session is a running session's control surface. The Manager holds one
// Session per registered entry and drives it through its lifecycle.
//
// SendInput is not required to be safe for concurrent callers; the
// Manager serializes it behind a per-Session lock. Adapters that want to
// reject in-flight overlap independently of the Manager surface
// ErrTurnInFlight.
type Session interface {
	// Wait blocks until the session terminates and returns its exit
	// code. Stop will cause Wait to return; once Wait returns, the
	// session is finished and its resources released.
	Wait() (int, error)

	// Stop requests termination. The watch goroutine observes the
	// resulting Wait return and records the terminal state. Calling
	// Stop on an already-terminated session is a no-op and returns nil.
	Stop(ctx context.Context) error

	// SendInput pushes input bytes into the session. CLI sessions write
	// to the PTY master; API sessions append to the conversation. May
	// return ErrNoInputChannel if the session has terminated, or
	// ErrTurnInFlight if a turn is already running.
	SendInput(ctx context.Context, data []byte) error

	// Resize updates the session's terminal winsize so child TUI apps
	// redraw correctly. For non-PTY sessions Resize is a no-op and
	// returns nil.
	Resize(ctx context.Context, rows, cols uint16) error

	// Health reports current liveness. Always non-blocking.
	Health() HealthStatus

	// CheckpointHints optionally surfaces an opaque hint for the
	// consumer's checkpointer. Returns (zero, false) when the adapter
	// has no hint to report.
	CheckpointHints() (CheckpointHint, bool)
}

// Sentinel errors.
var (
	// ErrNoInputChannel is returned by SendInput when a session has no
	// writable input channel — typically a session that has terminated
	// or a runtime kind that does not accept mid-flight input.
	ErrNoInputChannel = errors.New("agentsessions: session has no input channel")

	// ErrTurnInFlight is returned by SendInput when a turn is already
	// running. Adapters that enforce single-turn-in-flight semantics
	// surface this; the Manager additionally serializes via a
	// per-Session lock.
	ErrTurnInFlight = errors.New("agentsessions: turn already in flight")

	// ErrSessionNotRunning is returned by Manager methods when the
	// named session is not currently registered (already exited or
	// never started).
	ErrSessionNotRunning = errors.New("agentsessions: session not running")

	// ErrManagerStopped is returned by Manager methods after Shutdown.
	ErrManagerStopped = errors.New("agentsessions: manager stopped")

	// ErrAttachDisabled is returned by Manager.Attach* when the session
	// was started with AttachEnabled=false.
	ErrAttachDisabled = errors.New("agentsessions: attach not enabled for session")
)
