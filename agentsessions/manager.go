// Manager owns the registry of running sessions and their state
// transitions. It is safe for concurrent use: registry access is guarded
// by a sync.RWMutex and watch goroutines cooperate via a WaitGroup so
// Shutdown blocks until terminal states are recorded.
//
// Adapted from agent-mux's runtime manager. Differences from the source:
//   - State enum is the library's four-value process-level set (consumer
//     domain FSMs map on top).
//   - Sinks are interfaces (StateSink, AttachmentSink, EventSink) — the
//     library defines them and ships no implementation.
//   - StartRequest is a value type with library-only fields. Mux's
//     launch.Plan / workspace.Session belong upstream.
//   - AttachOptions.RingBytes / SubscriberDepth are honored per-session
//     (mux pinned them to constants).
//   - Single-turn-in-flight surfacing (ErrTurnInFlight) is the adapter's
//     responsibility; the Manager serializes SendInput per-entry but
//     does not enforce turn boundaries on its own.
package agentsessions

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// StateSink persists state transitions. Production implementations write
// to a database (clockwork, mux, nanite all wire one). Tests can pass nil
// for state-tracking-disabled flows; the Manager treats a nil sink as a
// no-op for state writes.
type StateSink interface {
	UpdateSessionState(id string, state State, pid int, exit *int) error
}

// AttachmentSink persists the lifecycle of a client attach subscription.
// Optional — a nil AttachmentSink disables attach persistence; the
// in-memory attach counter still runs.
type AttachmentSink interface {
	CreateClientAttachment(attachID, sessionID, clientKind string, attachedAt time.Time) error
	DetachClientAttachment(attachID string, detachedAt time.Time) error
}

// EventSink receives lifecycle events the Manager emits at every state
// transition. Optional — a nil EventSink disables event emission.
type EventSink interface {
	Emit(ctx context.Context, ev LifecycleEvent)
}

// LifecycleEvent describes a Manager-emitted state transition. Consumers
// branch on Kind; From/To give the state pair, ExitCode is set on
// terminal transitions, Reason carries optional free-form context.
type LifecycleEvent struct {
	SessionID string
	Kind      LifecycleEventKind
	From      State
	To        State
	ExitCode  *int
	Reason    string
}

// LifecycleEventKind names the shape of a LifecycleEvent. The library
// emits exactly one kind today; the type is exported so consumers can
// extend without re-defining the event struct.
type LifecycleEventKind string

const (
	// EventKindStateChanged fires on every state transition: created →
	// launching → running → done|failed.
	EventKindStateChanged LifecycleEventKind = "session.state_changed"
)

// StartRequest bundles the inputs for launching a session under Manager
// ownership. ID and Runtime are required; Options carries the
// runtime-agnostic spawn config; SessionMeta is opaque metadata the
// Manager echoes back via SessionInfo for consumer convenience.
type StartRequest struct {
	ID          string
	Runtime     Runtime
	Options     StartOptions
	SessionMeta map[string]string
}

// SessionInfo is the public snapshot of a registered session. The raw
// Session is intentionally not exposed — callers go through the Manager.
type SessionInfo struct {
	ID              string
	PID             int
	State           State
	RuntimeID       string
	RuntimeKind     string
	Caps            Capabilities
	Workdir         string
	AttachedClients int
	Meta            map[string]string
}

// HealthSnapshot is a point-in-time health + capability snapshot. It
// combines the live HealthStatus from the Session with the static
// Capabilities the Runtime declared.
type HealthSnapshot struct {
	SessionID   string
	RuntimeID   string
	RuntimeKind string
	Caps        Capabilities
	Health      HealthStatus
}

// Manager is the long-lived owner of running Sessions.
type Manager struct {
	sink       StateSink
	attachSink AttachmentSink
	events     EventSink
	nowFn      func() time.Time
	idFn       func() string

	mu       sync.RWMutex
	registry map[string]*entry
	results  map[string]*sessionResult
	stopped  bool

	wg sync.WaitGroup
}

// entry is the Manager's per-session bookkeeping.
type entry struct {
	info        SessionInfo
	sess        Session
	broker      *attachBroker
	killing     bool
	attachCount int
	// inputMu serializes SendInput so concurrent callers never interleave
	// partial writes on the session's input channel.
	inputMu sync.Mutex
}

// sessionResult holds the exit code and Session.Wait error of a
// terminated session. done is closed once the watch goroutine records
// the terminal state; exitCode and exitErr are safe to read after that
// (close is a synchronization barrier).
type sessionResult struct {
	done     chan struct{}
	exitCode int
	exitErr  error
}

// NewManager constructs a Manager. sink may be nil — the Manager treats
// nil as a no-op for state writes (useful in tests / one-shot tools).
func NewManager(sink StateSink) *Manager {
	return &Manager{
		sink:     sink,
		nowFn:    time.Now,
		idFn:     defaultIDFn,
		registry: map[string]*entry{},
		results:  map[string]*sessionResult{},
	}
}

// WithAttachmentSink returns m with the attachment sink set. Safe to
// call on a freshly-constructed Manager before any Start.
func (m *Manager) WithAttachmentSink(sink AttachmentSink) *Manager {
	m.attachSink = sink
	return m
}

// WithEventSink returns m with the lifecycle event sink set. Nil is a
// no-op (events silently skipped).
func (m *Manager) WithEventSink(sink EventSink) *Manager {
	m.events = sink
	return m
}

// WithNowFn overrides the Manager's clock for tests. Production callers
// should not need this.
func (m *Manager) WithNowFn(fn func() time.Time) *Manager {
	m.nowFn = fn
	return m
}

// WithIDFn overrides the attach-id generator for tests.
func (m *Manager) WithIDFn(fn func() string) *Manager {
	m.idFn = fn
	return m
}

// Start launches a session under Manager ownership: records launching →
// running, registers the session, and spawns a watch goroutine that
// records the terminal state when the session exits.
//
// Errors from Runtime.Start are recorded as StateFailed before returning.
// Returns ErrManagerStopped if called after Shutdown.
//
// Start does NOT block on a "first state event" — it returns as soon as
// the session is registered. Caller observes lifecycle via WaitSession,
// the EventSink, or Health polling. This matches mux's behavior and
// keeps the Manager non-blocking on adapter slowness.
func (m *Manager) Start(ctx context.Context, req StartRequest) error {
	if req.ID == "" {
		return fmt.Errorf("agentsessions: StartRequest.ID is required")
	}
	if req.Runtime == nil {
		return fmt.Errorf("agentsessions: StartRequest.Runtime is required")
	}

	m.mu.RLock()
	stopped := m.stopped
	m.mu.RUnlock()
	if stopped {
		return ErrManagerStopped
	}

	m.recordState(req.ID, StateLaunching, 0, nil)
	m.emit(ctx, req.ID, "created", StateLaunching, nil, "")

	var broker *attachBroker
	opts := req.Options
	if opts.AttachEnabled {
		broker = newAttachBroker(opts.RingBytes, opts.SubscriberDepth)
		opts.Fanout = combineWriters(opts.Fanout, broker)
	}

	sess, err := req.Runtime.Start(ctx, opts)
	if err != nil {
		if broker != nil {
			broker.close()
		}
		exit := 1
		m.recordState(req.ID, StateFailed, 0, &exit)
		m.emit(ctx, req.ID, StateLaunching, StateFailed, &exit, err.Error())
		return err
	}

	pid := sess.Health().PID
	m.recordState(req.ID, StateRunning, pid, nil)
	m.emit(ctx, req.ID, StateLaunching, StateRunning, nil, "")

	info := SessionInfo{
		ID:          req.ID,
		PID:         pid,
		State:       StateRunning,
		RuntimeID:   req.Runtime.ID(),
		RuntimeKind: req.Runtime.Kind(),
		Caps:        req.Runtime.Caps(),
		Workdir:     req.Options.Workdir,
		Meta:        req.SessionMeta,
	}

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		if broker != nil {
			broker.close()
		}
		_ = sess.Stop(ctx)
		return ErrManagerStopped
	}
	m.registry[req.ID] = &entry{info: info, sess: sess, broker: broker}
	m.results[req.ID] = &sessionResult{done: make(chan struct{})}
	m.wg.Add(1)
	m.mu.Unlock()

	// watch is session-scoped, not request-scoped.
	go m.watch(req.ID, sess, broker)
	return nil
}

func (m *Manager) recordState(id string, state State, pid int, exit *int) {
	if m.sink == nil {
		return
	}
	_ = m.sink.UpdateSessionState(id, state, pid, exit)
}

func (m *Manager) emit(ctx context.Context, id string, from, to State, exit *int, reason string) {
	if m.events == nil {
		return
	}
	m.events.Emit(ctx, LifecycleEvent{
		SessionID: id,
		Kind:      EventKindStateChanged,
		From:      from,
		To:        to,
		ExitCode:  exit,
		Reason:    reason,
	})
}

// watch blocks on the session's Wait, records the terminal state,
// unregisters the entry, closes the attach broker so subscribers drain
// cleanly, and delivers the exit code to any WaitSession callers.
func (m *Manager) watch(id string, sess Session, broker *attachBroker) {
	defer m.wg.Done()
	code, waitErr := sess.Wait()

	m.mu.Lock()
	var killing bool
	if e, ok := m.registry[id]; ok {
		killing = e.killing
		delete(m.registry, id)
	}
	result := m.results[id]
	m.mu.Unlock()

	var state State
	switch {
	case killing:
		state = StateDone
	case code == 0:
		state = StateDone
	default:
		state = StateFailed
	}
	m.recordState(id, state, sess.Health().PID, &code)
	m.emit(context.Background(), id, StateRunning, state, &code, "")

	if broker != nil {
		broker.close()
	}
	if result != nil {
		result.exitCode = code
		result.exitErr = waitErr
		close(result.done)
	}
}

// Stop signals the named session to terminate. Returns after the
// Session.Stop call has been made; the watch goroutine records the
// terminal state asynchronously.
func (m *Manager) Stop(ctx context.Context, id string) error {
	m.mu.Lock()
	e, ok := m.registry[id]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotRunning
	}
	e.killing = true
	sess := e.sess
	m.mu.Unlock()
	return sess.Stop(ctx)
}

// Get returns a snapshot for a registered session.
func (m *Manager) Get(id string) (SessionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.registry[id]
	if !ok {
		return SessionInfo{}, false
	}
	info := e.info
	info.AttachedClients = e.attachCount
	return info, true
}

// List returns snapshots of all currently registered sessions.
func (m *Manager) List() []SessionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SessionInfo, 0, len(m.registry))
	for _, e := range m.registry {
		info := e.info
		info.AttachedClients = e.attachCount
		out = append(out, info)
	}
	return out
}

// Health returns a live health snapshot for the named session, combining
// the current HealthStatus with the static Capabilities the Runtime
// declared.
func (m *Manager) Health(id string) (HealthSnapshot, bool) {
	m.mu.RLock()
	e, ok := m.registry[id]
	m.mu.RUnlock()
	if !ok {
		return HealthSnapshot{}, false
	}
	return HealthSnapshot{
		SessionID:   id,
		RuntimeID:   e.info.RuntimeID,
		RuntimeKind: e.info.RuntimeKind,
		Caps:        e.info.Caps,
		Health:      e.sess.Health(),
	}, true
}

// SendInput writes data to the named session's input channel. Concurrent
// callers are serialized through a per-entry lock so no two callers
// interleave partial writes.
func (m *Manager) SendInput(id string, data []byte) error {
	m.mu.RLock()
	e, ok := m.registry[id]
	m.mu.RUnlock()
	if !ok {
		return ErrSessionNotRunning
	}
	e.inputMu.Lock()
	defer e.inputMu.Unlock()
	return e.sess.SendInput(context.Background(), data)
}

// JsonRpcCall dispatches a JSON-RPC 2.0 request to the named session and
// blocks for the response. The Manager preserves the invariant that raw
// Sessions are never exposed to callers; this method type-narrows to the
// JsonRpcCaller capability and forwards Call(ctx, method, params).
//
// Errors:
//   - ErrSessionNotRunning if no session with id is registered.
//   - ErrSessionNotJsonRpcCapable if the session exists but its underlying
//     runtime does not implement JsonRpcCaller (PTY, streaming-stdio, or
//     adapter runtime).
//   - *JsonRpcError (errors.As-extractable) when the remote returns a
//     JSON-RPC error response.
//   - ctx.Err() when the caller cancels before the response arrives.
//   - Other errors propagated verbatim from JsonRpcCaller.Call.
//
// The Manager-level registry lock is released before invoking Call so
// long-running JSON-RPC requests do not block concurrent Manager
// operations (lookup-then-release pattern, symmetric with SendInput and
// Resize).
//
// Added in v0.9.0. See package godoc on Manager for the capability-dispatch
// framing: per-session capabilities live on the Session itself; the Manager
// exposes them as Manager-level methods (Stop, SendInput, Resize,
// JsonRpcCall) that internally route to the right session. Direct access
// to the raw Session remains intentionally unavailable.
func (m *Manager) JsonRpcCall(ctx context.Context, id string, method string, params any) (json.RawMessage, error) {
	m.mu.RLock()
	e, ok := m.registry[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrSessionNotRunning
	}
	caller, ok := e.sess.(JsonRpcCaller)
	if !ok {
		return nil, ErrSessionNotJsonRpcCapable
	}
	return caller.Call(ctx, method, params)
}

// Resize forwards a (rows, cols) winsize update. Not guarded by inputMu
// — pty.Setsize is a single ioctl on the master fd and doesn't race with
// input/output goroutines.
func (m *Manager) Resize(id string, rows, cols uint16) error {
	m.mu.RLock()
	e, ok := m.registry[id]
	m.mu.RUnlock()
	if !ok {
		return ErrSessionNotRunning
	}
	return e.sess.Resize(context.Background(), rows, cols)
}

// AttachOptions controls optional metadata on a subscription. Zero value
// picks "cli" client_kind and a full-ring replay.
type AttachOptions struct {
	ClientKind string

	// SinceSeq is a byte-count hint for resume. >0 replays only ring
	// bytes beyond that offset; 0 replays the full ring; values older
	// than the ring's start cause a silent gap.
	SinceSeq int64

	// Depth overrides the subscriber channel depth for this attach.
	// Zero uses the broker's default.
	Depth int
}

// Attach subscribes w to the named session's live output stream. Attach
// writes recent history (tail replay) to w first, then streams live
// output until ctx is canceled or the session exits. Multiple concurrent
// Attach callers are supported; one detaching does not affect the others.
//
// Returns ErrSessionNotRunning if not registered, ErrAttachDisabled if
// the session was started with AttachEnabled=false.
func (m *Manager) Attach(ctx context.Context, id string, w io.Writer) error {
	return m.AttachWith(ctx, id, w, AttachOptions{})
}

// AttachWith is Attach with caller-controlled subscription metadata.
func (m *Manager) AttachWith(ctx context.Context, id string, w io.Writer, opts AttachOptions) error {
	m.mu.RLock()
	e, ok := m.registry[id]
	m.mu.RUnlock()
	if !ok {
		return ErrSessionNotRunning
	}
	if e.broker == nil {
		return ErrAttachDisabled
	}

	kind := opts.ClientKind
	if kind == "" {
		kind = "cli"
	}
	attachID := m.idFn()
	now := m.nowFn().UTC()

	if m.attachSink != nil {
		_ = m.attachSink.CreateClientAttachment(attachID, id, kind, now)
	}

	m.mu.Lock()
	e.attachCount++
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		if e.attachCount > 0 {
			e.attachCount--
		}
		m.mu.Unlock()
		if m.attachSink != nil {
			_ = m.attachSink.DetachClientAttachment(attachID, m.nowFn().UTC())
		}
	}()

	replay, ch, cancel := e.broker.subscribeSince(opts.Depth, opts.SinceSeq)
	defer cancel()
	return copyStream(ctx, w, replay, ch)
}

// WaitSession blocks until the named session's watch goroutine records
// its terminal state and returns the exit code along with the underlying
// error from Session.Wait. Returns ErrSessionNotRunning if the session
// was never registered.
//
// The returned error is the verbatim Session.Wait return value:
//   - nil for clean exits (code == 0 with no underlying wait error).
//   - *ExitError (extractable via errors.As) for any non-clean exit
//     under PTY supervision. ExitError.Cause classifies
//     supervisor-triggered kills as idle_timeout, watchdog_kill,
//     restart_exhausted, oom_kill, or resource_limit; Cause is empty
//     for ordinary non-zero exits or Stop/ctx-cancel under supervision
//     (the supervisor did not trigger the termination, so it carries
//     no cause label even though *ExitError is still returned).
//   - The underlying wait error (typically *exec.ExitError) for non-zero
//     exits on non-supervised sessions.
//
// Consumers classifying terminations should errors.As(err, &xe) first;
// branch on xe.Cause when non-empty, fall through to *exec.ExitError or
// other underlying-error checks otherwise. Never .Error() string-match.
//
// Behavior change vs. earlier versions: WaitSession previously discarded
// Session.Wait's error and always returned nil on terminal state.
// Consumers that treated err != nil as unexpected must update; the
// terminations were always errors at the Session.Wait layer.
//
// WaitSession returns on terminal state regardless of attach subscribers
// — drain ordering is the consumer's problem. Subscribers see EOF
// naturally when the broker closes during watch.
func (m *Manager) WaitSession(ctx context.Context, id string) (int, error) {
	m.mu.RLock()
	r, ok := m.results[id]
	m.mu.RUnlock()
	if !ok {
		return 0, ErrSessionNotRunning
	}
	select {
	case <-r.done:
		return r.exitCode, r.exitErr
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Shutdown marks the Manager stopped and blocks until all in-flight
// watch goroutines return or ctx is done. Idempotent.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.stopped = true
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// combineWriters merges the caller-supplied Fanout (if any) with the
// broker. Returns the broker alone if there is no caller fanout.
func combineWriters(caller io.Writer, broker io.Writer) io.Writer {
	if caller == nil {
		return broker
	}
	return io.MultiWriter(caller, broker)
}
