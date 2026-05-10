package agentsessions

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// fakeSession is a manually-driven Session for testing the Manager's
// lifecycle and attach broker. complete(code) unblocks Wait; emit(b)
// pushes bytes to the Fanout writer the Manager records at Start time.
//
// Adapted from agent-mux's runtime manager test fakes.
type fakeSession struct {
	pid    int
	done   chan struct{}
	once   sync.Once
	code   atomic.Int32
	killed atomic.Bool
	fanout io.Writer

	// waitErr is the error fakeSession.Wait returns alongside code. Set
	// under once.Do via completeWithErr; safe to read after <-done since
	// the close is a happens-before barrier.
	waitErr error

	// inputSink, if non-nil, receives SendInput bytes.
	inputSink io.Writer

	resizeRows atomic.Uint32
	resizeCols atomic.Uint32

	state atomic.Int32 // LiveState
}

func newFakeSession(pid int) *fakeSession {
	return &fakeSession{pid: pid, done: make(chan struct{})}
}

func (f *fakeSession) Wait() (int, error) {
	<-f.done
	return int(f.code.Load()), f.waitErr
}

func (f *fakeSession) Stop(_ context.Context) error {
	f.killed.Store(true)
	f.state.Store(int32(LiveStateStopped))
	f.once.Do(func() {
		f.code.Store(-1)
		close(f.done)
	})
	return nil
}

func (f *fakeSession) SendInput(_ context.Context, data []byte) error {
	if f.killed.Load() {
		return ErrNoInputChannel
	}
	if f.inputSink == nil {
		return ErrNoInputChannel
	}
	_, err := f.inputSink.Write(data)
	return err
}

func (f *fakeSession) Resize(_ context.Context, rows, cols uint16) error {
	f.resizeRows.Store(uint32(rows))
	f.resizeCols.Store(uint32(cols))
	return nil
}

func (f *fakeSession) Health() HealthStatus {
	return HealthStatus{
		Alive: !f.killed.Load(),
		PID:   f.pid,
		State: LiveState(f.state.Load()),
	}
}

func (f *fakeSession) CheckpointHints() (CheckpointHint, bool) {
	return nil, false
}

func (f *fakeSession) complete(code int) {
	f.once.Do(func() {
		f.code.Store(int32(code))
		close(f.done)
	})
}

// completeWithErr is complete that also stages an error for Wait to
// return. Used to drive WaitSession's error-propagation paths from
// tests; mirrors what go-runner / supervised PTY return on non-clean
// exits (e.g. *exec.ExitError, *ExitError).
func (f *fakeSession) completeWithErr(code int, err error) {
	f.once.Do(func() {
		f.code.Store(int32(code))
		f.waitErr = err
		close(f.done)
	})
}

func (f *fakeSession) emit(b []byte) {
	if f.fanout != nil {
		_, _ = f.fanout.Write(b)
	}
}

// fakeRuntime spawns fakeSessions. The factory captures the Fanout the
// Manager assigns at Start time so tests can drive output.
type fakeRuntime struct {
	id   string
	kind string
	caps Capabilities

	mu         sync.Mutex
	last       *fakeSession
	startErr   error
	prepareErr error

	// pid counter for sessions
	nextPID atomic.Int32

	// inputSinkOnStart, if non-nil, is wired into the next fakeSession's
	// inputSink so SendInput tests can capture writes.
	inputSinkOnStart io.Writer
}

func newFakeRuntime(id, kind string) *fakeRuntime {
	rt := &fakeRuntime{id: id, kind: kind}
	rt.nextPID.Store(99)
	return rt
}

func (r *fakeRuntime) ID() string         { return r.id }
func (r *fakeRuntime) Kind() string       { return r.kind }
func (r *fakeRuntime) Caps() Capabilities { return r.caps }
func (r *fakeRuntime) Prepare(_ context.Context) error {
	return r.prepareErr
}

func (r *fakeRuntime) Start(_ context.Context, opts StartOptions) (Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.startErr != nil {
		return nil, r.startErr
	}
	pid := int(r.nextPID.Add(1))
	s := newFakeSession(pid)
	s.fanout = opts.Fanout
	s.inputSink = r.inputSinkOnStart
	r.last = s
	return s, nil
}

func (r *fakeRuntime) lastSession() *fakeSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// memSink is an in-memory StateSink that records every transition. Used
// by manager_test to assert state-write order.
type memSink struct {
	mu  sync.Mutex
	log []string
}

func (s *memSink) UpdateSessionState(id string, state State, pid int, exit *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	exitStr := ""
	if exit != nil {
		exitStr = " exit=" + intToStr(*exit)
	}
	s.log = append(s.log, id+":"+string(state)+":"+intToStr(pid)+exitStr)
	return nil
}

func (s *memSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.log))
	copy(out, s.log)
	return out
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// memEvents records every LifecycleEvent.
type memEvents struct {
	mu sync.Mutex
	ev []LifecycleEvent
}

func (e *memEvents) Emit(_ context.Context, ev LifecycleEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ev = append(e.ev, ev)
}

func (e *memEvents) snapshot() []LifecycleEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]LifecycleEvent, len(e.ev))
	copy(out, e.ev)
	return out
}

// syncBuf is a sync-safe bytes.Buffer for tests where multiple
// goroutines may share access (attach copies bytes from a goroutine,
// asserts read in the main test).
type syncBuf struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// errStartRuntime is a Runtime whose Start always errors.
type errStartRuntime struct{ err error }

func (r errStartRuntime) ID() string                      { return "err" }
func (r errStartRuntime) Kind() string                    { return "fake" }
func (r errStartRuntime) Caps() Capabilities              { return Capabilities{} }
func (r errStartRuntime) Prepare(_ context.Context) error { return nil }
func (r errStartRuntime) Start(_ context.Context, _ StartOptions) (Session, error) {
	return nil, r.err
}

var errFakeStart = errors.New("fake start error")

// waitFor polls cond up to timeout, returning true once cond returns true.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}
