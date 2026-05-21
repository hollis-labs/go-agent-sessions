package agentsessions

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	llmtypes "github.com/hollis-labs/go-llm-types"
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
//
// Lifecycle flags (PTY / StreamingStdio / JsonRpcStdio / ServeHTTP) are mutually
// exclusive — at most one may be true for a given Capabilities value.
// NewFromAdapter validates this at construction time. When none of the
// lifecycle flags are set, the adapter runtime (subprocess-per-turn) is selected.
type Capabilities struct {
	// PTY: Session.SendInput writes to a live PTY master. Resize is
	// meaningful. False for turn-based subprocess adapters. Selects the
	// long-lived PTY runtime kind; mutually exclusive with StreamingStdio
	// and JsonRpcStdio.
	PTY bool

	// StreamingStdio: long-lived non-PTY child speaking NDJSON over
	// stdin/stdout. SendInput writes a frame followed by '\n' to the
	// child's stdin. Conversation state persists across turns in the
	// long-lived process (e.g. Claude's stream-json input mode).
	// Mutually exclusive with PTY and JsonRpcStdio.
	StreamingStdio bool

	// JsonRpcStdio: long-lived non-PTY child speaking JSON-RPC 2.0 over
	// stdin/stdout. The runtime layers a JSON-RPC client (id allocator,
	// pending-request map, notification dispatcher) over raw stdio.
	// Typed requests go through the runtime's Call(method, params)
	// surface; SendInput remains as a raw-bytes escape hatch.
	// Mutually exclusive with PTY and StreamingStdio.
	JsonRpcStdio bool

	// ServeHTTP: long-lived child exposing an HTTP API. The runtime
	// spawns the adapter command, discovers the printed listen URL,
	// subscribes to server-sent events, and sends turns through HTTP.
	// Mutually exclusive with PTY, StreamingStdio, and JsonRpcStdio.
	ServeHTTP bool

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

// validateLifecycle returns an error when more than one of the lifecycle
// flags (PTY, StreamingStdio, JsonRpcStdio, ServeHTTP) is set. Called by
// NewFromAdapter to reject ambiguous runtime selection at construction
// time rather than silently preferring one flag.
func (c Capabilities) validateLifecycle() error {
	n := 0
	if c.PTY {
		n++
	}
	if c.StreamingStdio {
		n++
	}
	if c.JsonRpcStdio {
		n++
	}
	if c.ServeHTTP {
		n++
	}
	if n > 1 {
		return errors.New("agentsessions: Capabilities declares more than one lifecycle flag (PTY / StreamingStdio / JsonRpcStdio / ServeHTTP); at most one may be true")
	}
	return nil
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

// JsonRpcCaller is the optional interface a Session implements when the
// runtime speaks JSON-RPC 2.0 over its stdio channel
// (Caps().JsonRpcStdio == true). Call sends a typed request, blocks on
// the response (or ctx.Done()), and returns the raw result envelope.
// Errors from the JSON-RPC error response shape are returned as a
// *JsonRpcError so callers can introspect code / message / data via
// errors.As. Callers must type-assert to this interface; it is not part
// of the core Session contract.
type JsonRpcCaller interface {
	Call(ctx context.Context, method string, params any) (json.RawMessage, error)
}

// JsonRpcError is the structured form of a JSON-RPC 2.0 error response
// (per the spec's error object: code int, message string, optional data).
// Returned by JsonRpcCaller.Call when the remote returns an error
// envelope; callers extract via errors.As.
type JsonRpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *JsonRpcError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return "agentsessions: jsonrpc error " + jsonRpcErrorString(e.Code) + ": " + e.Message
}

// jsonRpcErrorString is small to avoid pulling fmt into types.go's hot path.
func jsonRpcErrorString(code int) string {
	// Minimal int → decimal-string conversion; not perf-critical.
	if code == 0 {
		return "0"
	}
	negative := code < 0
	if negative {
		code = -code
	}
	var buf [20]byte
	pos := len(buf)
	for code > 0 {
		pos--
		buf[pos] = byte('0' + code%10)
		code /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// PIDReporter is an optional Session extension that distinguishes the PID
// of a process currently running from the most-recently-started process.
// The two diverge for subprocess-per-turn runtimes: between turns no
// process is running, so LivePID returns 0; LastPID still surfaces the
// PID of the most-recent turn for log-correlation purposes.
//
// PTY runtimes implement this trivially — both return the long-lived
// child PID across the session's lifetime.
//
// Callers type-assert to this interface; it is not part of the core
// Session contract. Health().PID returns LivePID when the runtime
// implements this interface; otherwise it returns whatever the runtime
// provides (typically the legacy "PID-or-zero" semantics).
type PIDReporter interface {
	// LivePID returns the PID of the running process, or 0 when no
	// process is currently running.
	LivePID() int

	// LastPID returns the PID of the most-recently-started process, or
	// 0 if no process has started yet on this Session.
	LastPID() int
}

// StartOptions bundles the runtime-agnostic state a Runtime needs to
// spawn a Session. Fanout, when non-nil, receives a copy of the session's
// output stream so the Manager can broadcast to attach subscribers — CLI
// runtimes tee PTY bytes / parsed event lines into it; API runtimes
// forward streamed assistant tokens. EventFanout, when non-nil, receives
// the parsed llmtypes.StreamEvent values alongside the byte Fanout.
type StartOptions struct {
	// Workdir is the absolute path used as the spawned process's working
	// directory and as the workspace argument to sandbox.Apply.
	Workdir string

	// WorkspaceDir is the per-session persistent root for the lib's own
	// state — distinct from Workdir (the spawned process's cwd). When
	// non-empty, the PTY runtime falls back to <WorkspaceDir>/logs/session.log
	// for its log file when LogPath is empty. The lib never writes to
	// WorkspaceDir except on the explicit LogPath fallback path; any other
	// state (checkpoints, plan capture) is consumer-owned.
	//
	// Zero-value preserves existing behavior: the PTY runtime requires
	// LogPath in that case, and the adapter runtime ignores this field
	// entirely. See README "Two-dir model" for the layered Workdir/
	// WorkspaceDir convention adopted across the agent-boot portfolio.
	WorkspaceDir string

	// LogPath is the absolute path of the per-session log file the
	// adapter writes to. Optional. For the PTY runtime, if LogPath is
	// empty and WorkspaceDir is set, the runtime falls back to
	// <WorkspaceDir>/logs/session.log. If both are empty, the PTY runtime
	// returns an error from Start.
	LogPath string

	// BootPrompt is the initial system / boot prompt the adapter feeds
	// to the underlying agent on first start. Adapter-defined.
	//
	// Under AutoPlantBootDir, this populates provider.PlantContext.SystemPrompt
	// (the durable, always-loaded persona content rendered into CLAUDE.md /
	// AGENTS.md / agents/<name>.md by the per-provider BootDirSpec renderers).
	BootPrompt string

	// BootContent is the per-task kickoff body the adapter plants into the
	// transient boot file (boot.md / equivalent) referenced from the system
	// prompt via `@./boot.md`. Distinct from BootPrompt so consumers can
	// separate agent identity (durable, survives compaction via auto-reload
	// of CLAUDE.md) from task scope (per-run instructions re-anchored after
	// compaction via the @-reference).
	//
	// Only consulted under AutoPlantBootDir. Populates
	// provider.PlantContext.BootContent. When empty, the runtime falls back
	// to BootPrompt for back-compat with consumers that conflate the two
	// (the v0.9.0–v0.9.2 behavior). Adapter-defined.
	//
	// Added in v0.9.3.
	BootContent string

	// PlantContext, when non-zero, supplies caller-owned
	// provider.PlantContext fields the lib does not manage. Used only under
	// AutoPlantBootDir.
	//
	// The lib overrides four fields regardless of what the caller sets here:
	// SystemPrompt from BootPrompt, BootContent from BootContent (falling
	// back to BootPrompt), ProjectDir from Workdir, and BootDir from the
	// absolute path of the planted dir.
	//
	// Caller-owned fields flow through verbatim, including AgentName,
	// MCPLoopbackURL, MuxCommand, MuxArgs, and MuxEnv. Future
	// provider.PlantContext fields flow through automatically as renderers
	// begin consuming them.
	//
	// Default zero value preserves v0.9.0–v0.9.3 behavior exactly.
	//
	// Added in v0.9.4.
	PlantContext provider.PlantContext

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

	// ExtraFiles, when non-nil, is forwarded to runner.Config.ExtraFiles
	// in the adapter-driven runtime so the spawned subprocess inherits the
	// listed open files at FD 3+. Used by consumers that plumb
	// out-of-band channels into the spawned binary. Nil leaves the
	// default empty. This field is a no-op for the provider-driven
	// runtime (HTTP transport, no subprocess).
	ExtraFiles []*os.File

	// Profile is the resolved go-sandbox profile. The zero-value
	// sandbox.Profile (empty ID) disables sandbox wrapping. A non-zero
	// profile on an unsupported platform is a hard launch failure (per
	// go-sandbox).
	Profile sandbox.Profile

	// Fanout receives a tee of session output for the attach broker. Set
	// by the Manager — adapters do not allocate this.
	Fanout io.Writer

	// EventFanout receives parsed llmtypes.StreamEvent values as a
	// best-effort mirror of the session stream. Sends are non-blocking;
	// when the channel is full the event is dropped silently. Callers
	// should supply a buffered channel sized to their consumer's
	// tolerance and must not close it before the session is done.
	EventFanout chan<- llmtypes.StreamEvent

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

	// AutoFireFirstTurn, when true, instructs the runtime to deliver
	// FirstTurnPayload as the first SendInput automatically after Start
	// succeeds. Eliminates the Launch/SendInput race that bit early
	// consumers — Start does not return until the payload has been
	// delivered to the input channel.
	//
	// For PTY runtimes with BootMode == "stdin" and a non-empty BootPrompt,
	// this is a no-op (the boot prompt is already written to ptmx during
	// Start, so the kickoff is in flight before this hook would fire).
	//
	// Default false preserves existing behavior; the caller is responsible
	// for the first SendInput when this is unset.
	AutoFireFirstTurn bool

	// FirstTurnPayload is the bytes sent on the auto-fired first turn when
	// AutoFireFirstTurn is true. Typically the kickoff string (e.g.
	// "Boot @./boot.md") or the first user message. Ignored when
	// AutoFireFirstTurn is false.
	FirstTurnPayload []byte

	// TypedEventCallback, when non-nil, receives parsed typed events
	// (provider/events.Event) from the runtime. Mirrors the existing
	// EventFanout chan<- llmtypes.StreamEvent surface but uses go-providers
	// Tier-2 typed events: callers see events.Delta / events.ToolUse /
	// events.ToolResult / events.SubagentSpawn / events.SessionID /
	// events.Done / events.Error / events.Heartbeat / events.Thinking.
	//
	// v0.5.0 scope:
	//   - PTY runtime (Caps.PTY=true): the callback fires per-line from the
	//     reader goroutine, ONLY when the adapter implements the optional
	//     provider.EventParser interface. Adapters without EventParser
	//     produce no typed events through this callback (no fallback
	//     translation in v0.5.0; the legacy EventFanout surface still
	//     receives ParseLine output).
	//   - Adapter runtime (Caps.PTY=false, subprocess-per-turn): this field
	//     is currently NOT consulted. The adapter runtime drives runner.Run
	//     which surfaces provider events as runner.EventProviderEvent
	//     (StreamEvent shape). Typed events on the adapter path remain a
	//     follow-up increment — see CHANGELOG "Out of scope".
	//
	// Sends are synchronous; treat the callback the way you'd treat an
	// io.Writer's Write — keep the work short or hand off to your own
	// goroutine. Default nil disables the surface; nothing is allocated.
	TypedEventCallback provider.EventsCallback

	// Supervisor, when non-nil, enables process supervision: idle-kill,
	// restart-on-crash, and watchdog. See SupervisorOptions godoc. Nil
	// preserves v0.5.0 default "spawn once, run to completion" behavior.
	//
	// v0.6.0 scope: PTY runtime (Caps.PTY=true) only. Supervision is
	// implemented natively — idle-kill / watchdog goroutines observe
	// ptmx I/O and a per-attempt cmd.Wait; the restart loop wraps the
	// wait. Restart preserves the provider-side agent_session_id when
	// Caps.ProviderSessionID is true (the most-recent observed session
	// ID is fed into the next spawn's BuildArgs in place of
	// SessionIDPreset).
	//
	// On the adapter runtime (Caps.PTY=false), this field is currently
	// NOT consulted — adapter-path forwarding to go-runner is blocked on
	// go-runner publishing its v0.3.0 supervision API (which exists
	// locally but isn't in the published module). Tracked as a v0.6.x
	// follow-up. Setting Supervisor with Caps.PTY=false has no effect.
	//
	// Added in v0.6.0.
	Supervisor *SupervisorOptions

	// JsonRpcNotificationHook, when non-nil, is invoked from the
	// jsonRpcStdioSession reader goroutine for every JSON-RPC notification
	// (a frame with no `id` field) received from the child. The raw method
	// name and params are forwarded verbatim; the runtime does no
	// adapter-specific translation. Consumers that want typed events from
	// notifications either implement provider.EventParser on the adapter
	// (the runtime calls ParseLineEvents per-line, identical to the
	// streaming-stdio path) or do their own decode inside this hook.
	//
	// Sends are synchronous; treat the hook like an io.Writer's Write —
	// keep the work short or hand off to your own goroutine. Default nil
	// drops notifications (the EventParser path still fires).
	//
	// Added in v0.8.0.
	JsonRpcNotificationHook func(method string, params json.RawMessage)

	// JsonRpcRequestHook, when non-nil, is invoked from the
	// jsonRpcStdioSession reader goroutine for every server-INITIATED
	// JSON-RPC request received from the child — a frame carrying BOTH a
	// `method` and an `id` (as opposed to a notification, which has no
	// `id`, or a response, which has no `method`). The canonical example
	// is codex app-server's tool-approval / elicitation requests.
	//
	// The hook's return value is marshaled into the JSON-RPC response the
	// runtime sends back to the child: a non-nil *JsonRpcError becomes an
	// `error` response; otherwise `result` (which may be nil → JSON
	// `null`) becomes a `result` response.
	//
	// JSON-RPC 2.0 requires a response for every request that carries an
	// id — without one the child blocks forever. When this hook is nil the
	// runtime still answers, with a method-not-handled error, so the child
	// fails fast instead of deadlocking. Set this hook to participate in
	// the child's request/response protocol (approvals, elicitations, …).
	//
	// Sends are synchronous; treat the hook like an io.Writer's Write —
	// keep the work short or hand off to your own goroutine.
	//
	// Added in v0.9.5.
	JsonRpcRequestHook func(method string, params json.RawMessage) (result any, rpcErr *JsonRpcError)

	// AutoPlantBootDir, when true AND the adapter implements
	// provider.BootDirProvider, instructs the runtime to materialize the
	// adapter's BootDirSpec into a per-session tempdir on Start and remove
	// it on terminal state. The lib:
	//   - creates a tempdir under BootDirRoot (or WorkspaceDir+"/boot/",
	//     or os.TempDir() if both are empty)
	//   - walks BootDirSpec.PlantedFiles, calls each Render(plantCtx), and
	//     writes the content (mode 0o600 for .mcp.json / settings.json,
	//     0o644 default; PlantedFile.Mode overrides when non-zero)
	//   - substitutes `{{.BootDir}}` / `{{.ProjectDir}}` in
	//     BootDirSpec.EnvAmendments and appends to Env
	//   - substitutes the same tokens in BootDirSpec.ProjectDirArg and
	//     appends to ExtraArgs (which the runtime splices into argv after
	//     adapter.BuildArgs)
	//   - sets Workdir to BootDirSpec.SpawnWorkdir(bootDir, originalWorkdir)
	//   - for Claude bare-mode adapters, applies BareInjectionPaths and
	//     mutates a per-session clone of the adapter so the planted paths
	//     thread into BuildArgs
	//   - emits OnBootDirPlanted (when set) once on successful plant
	//   - calls os.RemoveAll(bootDir) once on terminal state regardless of
	//     exit cause; cleanup failures are logged but never surface as
	//     session errors
	//
	// Default false preserves v0.8.0 behavior exactly — no filesystem
	// activity, no opts mutation. When true on an adapter that does NOT
	// implement BootDirProvider (or whose BootDirSpec has no PlantedFiles),
	// the runtime no-ops without error so generic consumers can leave the
	// flag on across heterogeneous adapter fleets.
	//
	// Added in v0.9.0.
	AutoPlantBootDir bool

	// BootDirRoot overrides the parent directory under which AutoPlantBootDir
	// creates the per-session tempdir. Default ordering when empty:
	// WorkspaceDir+"/boot/" if WorkspaceDir is set, otherwise os.TempDir().
	// When non-empty, the runtime MkdirAll's it (mode 0o750) before
	// MkdirTemp. Ignored when AutoPlantBootDir is false.
	//
	// Added in v0.9.0.
	BootDirRoot string

	// OnBootDirPlanted, when non-nil, is invoked once on successful
	// AutoPlantBootDir with the absolute path of the planted bootdir.
	// Useful for debugging (operators want to inspect what got written
	// before the session ends). The call fires synchronously from Start
	// before the spawn — keep it short or hand off. Never invoked when
	// AutoPlantBootDir is false or when no PlantedFiles were materialized.
	//
	// Added in v0.9.0.
	OnBootDirPlanted func(path string)

	// ExtraArgs, when non-nil, is appended to the runtime's argv after
	// adapter.BuildArgs(...). Used internally by AutoPlantBootDir to thread
	// BootDirSpec.ProjectDirArg through to the spawn (e.g. claude's
	// `--add-dir <projectDir>`) without changing the adapter contract.
	// Consumers may also set it directly when they need to splice
	// per-session argv without wrapping the adapter; in that case the
	// runtime does NOT do template substitution — pre-resolve any
	// placeholders before passing the slice.
	//
	// Added in v0.9.0.
	ExtraArgs []string

	// ResourceLimits, when non-nil and non-zero, applies OS-level resource
	// caps to spawned children (CPU time, virtual memory, open files,
	// processes, file size). Wraps the spawn argv with
	// `sh -c "ulimit ...; exec ..."` and, on Linux when systemd-run --user
	// is available, layers `systemd-run --scope --property=MemoryMax=...`.
	//
	// v0.6.0 scope: PTY runtime (Caps.PTY=true) only. The wrap composes
	// with sandbox.Apply — limits inherit through the sandbox-exec →
	// real binary chain.
	//
	// On the adapter runtime (Caps.PTY=false), this field is currently
	// NOT consulted — see the Supervisor godoc above for the same
	// follow-up note.
	//
	// macOS caveat: MemoryMax is silently dropped (RLIMIT_AS unavailable
	// via bash's ulimit -v on darwin and systemd-run is linux-only).
	// Callers needing hard memory limits on macOS use VM-based isolation.
	//
	// Added in v0.6.0.
	ResourceLimits *ResourceLimits
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

	// ErrSessionNotJsonRpcCapable is returned by Manager.JsonRpcCall when
	// the named session exists but its underlying runtime does not
	// implement JsonRpcCaller (e.g. PTY / streaming-stdio / adapter
	// runtime). Type-narrowed alternative to exposing raw Session: the
	// Manager dispatches to the JSON-RPC capability when it is present
	// and reports this typed error when it is not. Added in v0.9.0.
	ErrSessionNotJsonRpcCapable = errors.New("agentsessions: session does not implement JsonRpcCaller")
)
