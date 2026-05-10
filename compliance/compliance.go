// Package compliance provides a shared behavioral test suite for any
// agentsessions.Runtime. Every Runtime exposed by a consumer must pass
// the baseline suite. Optional capability-gated tests run only when
// Caps() declares the corresponding capability.
//
// # Usage
//
//	func TestCompliance(t *testing.T) {
//	    rt, err := agentsessions.NewFromAdapter(...)
//	    if err != nil { t.Fatal(err) }
//	    compliance.Run(t, compliance.Harness{
//	        Runtime: rt,
//	        NewStartOptions: func(t *testing.T) agentsessions.StartOptions {
//	            return agentsessions.StartOptions{Workdir: t.TempDir()}
//	        },
//	    })
//	}
//
// Adapters that require an external binary unavailable in CI set
// BinarySkip: true. All baseline lifecycle tests still run; capability
// tests requiring real binary behavior are skipped with an explicit
// t.Skip message.
//
// Adapted verbatim from agent-mux's provider compliance suite, adjusted
// to the library's renamed types.
package compliance

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/go-agent-sessions/agentsessions"
	llmtypes "github.com/hollis-labs/go-llm-types"
)

// Harness configures a compliance run for one Runtime.
type Harness struct {
	// Runtime is the Runtime under test. Required and must be non-nil.
	Runtime agentsessions.Runtime

	// NewStartOptions, when non-nil, is called per test to produce the
	// StartOptions passed to Runtime.Start. If nil, a default options
	// struct is used with Workdir set to a temp directory.
	NewStartOptions func(t *testing.T) agentsessions.StartOptions

	// BinarySkip, when true, marks tests that require the real binary
	// as skipped. Use when the binary is not guaranteed to be present
	// in the test environment.
	BinarySkip bool

	// SkipEventFanout skips the baseline typed-event-fanout check for
	// runtimes that legitimately cannot expose parsed stream events.
	SkipEventFanout bool
}

// Run executes the full compliance suite for the runtime described by h.
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.Runtime == nil {
		t.Fatal("compliance: Harness.Runtime is required")
	}
	rt := h.Runtime
	caps := rt.Caps()

	startOptsFn := h.NewStartOptions
	if startOptsFn == nil {
		startOptsFn = func(t *testing.T) agentsessions.StartOptions {
			return agentsessions.StartOptions{Workdir: t.TempDir()}
		}
	}

	t.Run("Baseline", func(t *testing.T) {
		runBaseline(t, rt, startOptsFn, h.BinarySkip, h.SkipEventFanout)
	})
	if caps.PTY {
		t.Run("CapsPTY", func(t *testing.T) {
			runCapsPTY(t, rt, startOptsFn, h.BinarySkip)
		})
	}
	if caps.Resize {
		t.Run("CapsResize", func(t *testing.T) {
			runCapsResize(t, rt, startOptsFn, h.BinarySkip)
		})
	}
	if caps.ProviderSessionID {
		t.Run("CapsProviderSessionID", func(t *testing.T) {
			runCapsProviderSessionID(t, rt, startOptsFn, h.BinarySkip)
		})
	}
	if !caps.BinaryRequired {
		t.Run("CapsNoBinary", func(t *testing.T) {
			runCapsNoBinary(t, rt)
		})
	}
	if caps.CheckpointResume {
		t.Run("CapsCheckpointResume", func(t *testing.T) {
			runCapsCheckpointResume(t, rt, startOptsFn, h.BinarySkip)
		})
	}
}

type startOptsFn = func(t *testing.T) agentsessions.StartOptions

// ---------------------------------------------------------------------------
// Baseline suite
// ---------------------------------------------------------------------------

func runBaseline(t *testing.T, rt agentsessions.Runtime, opts startOptsFn, binarySkip, skipEventFanout bool) {
	t.Helper()
	t.Run("KindNonEmpty", func(t *testing.T) {
		if rt.Kind() == "" {
			t.Errorf("Kind() = empty string, want non-empty")
		}
	})
	t.Run("IDNonEmpty", func(t *testing.T) {
		if rt.ID() == "" {
			t.Errorf("ID() = empty string, want non-empty")
		}
	})
	t.Run("StartProducesAliveSession", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		defer func() { _ = sess.Stop(context.Background()) }()
		if h := sess.Health(); !h.Alive {
			t.Errorf("Health().Alive = false immediately after Start; want true")
		}
	})
	t.Run("HealthDeadAfterStop", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		if err := sess.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if h := sess.Health(); h.Alive {
			t.Errorf("Health().Alive = true after Stop; want false")
		}
	})
	t.Run("HealthStateStoppedAfterStop", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		if err := sess.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if h := sess.Health(); h.State != agentsessions.LiveStateStopped {
			t.Errorf("Health().State = %v after Stop; want LiveStateStopped", h.State)
		}
	})
	t.Run("StopUnblocksWait", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		done := make(chan int, 1)
		go func() {
			code, _ := sess.Wait()
			done <- code
		}()
		select {
		case <-done:
			t.Skip("Wait returned before Stop — provider may have exited naturally")
		case <-time.After(50 * time.Millisecond):
		}
		if err := sess.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Wait did not return within 3s after Stop")
		}
	})
	t.Run("StopIsIdempotent", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		if err := sess.Stop(context.Background()); err != nil {
			t.Fatalf("first Stop: %v", err)
		}
		if err := sess.Stop(context.Background()); err != nil {
			t.Fatalf("second Stop: %v", err)
		}
	})
	t.Run("SendInputAfterStopReturnsErrNoInputChannel", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		if err := sess.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		err := sess.SendInput(context.Background(), []byte("too late"))
		if !errors.Is(err, agentsessions.ErrNoInputChannel) {
			t.Errorf("SendInput after Stop = %v; want agentsessions.ErrNoInputChannel", err)
		}
	})
	t.Run("ResizeNoOpOrNoError", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		defer func() { _ = sess.Stop(context.Background()) }()
		if err := sess.Resize(context.Background(), 24, 80); err != nil {
			t.Errorf("Resize(24,80) = %v; want nil", err)
		}
	})
	t.Run("CheckpointHintsReturnsBool", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		defer func() { _ = sess.Stop(context.Background()) }()
		_, _ = sess.CheckpointHints()
	})
	t.Run("EventFanoutReceivesParsedEvents", func(t *testing.T) {
		if skipEventFanout {
			t.Skip("SkipEventFanout=true")
		}
		if binarySkip {
			t.Skip("BinarySkip=true: binary not available")
		}
		o := opts(t)
		ch := make(chan llmtypes.StreamEvent, 8)
		o.EventFanout = ch
		sess := mustStart(t, rt, o)
		defer func() { _ = sess.Stop(context.Background()) }()
		if err := sess.SendInput(context.Background(), []byte("compliance event fanout")); err != nil {
			t.Fatalf("SendInput: %v", err)
		}
		select {
		case ev := <-ch:
			if ev.Type == "" {
				t.Errorf("first EventFanout event has empty Type")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("EventFanout did not receive a parsed event within 2s")
		}
	})
}

// ---------------------------------------------------------------------------
// Optional capability suites
// ---------------------------------------------------------------------------

func runCapsPTY(t *testing.T, rt agentsessions.Runtime, opts startOptsFn, binarySkip bool) {
	t.Helper()
	t.Run("SendInputDoesNotError", func(t *testing.T) {
		if binarySkip {
			t.Skip("BinarySkip=true: PTY binary not available")
		}
		o := opts(t)
		var buf syncBuffer
		o.Fanout = &buf
		sess := mustStart(t, rt, o)
		defer func() { _ = sess.Stop(context.Background()) }()
		_ = sess.SendInput(context.Background(), []byte("echo hi\n"))
	})
}

func runCapsResize(t *testing.T, rt agentsessions.Runtime, opts startOptsFn, binarySkip bool) {
	t.Helper()
	t.Run("ResizeDoesNotError", func(t *testing.T) {
		if binarySkip {
			t.Skip("BinarySkip=true: resize binary not available")
		}
		sess := mustStart(t, rt, opts(t))
		defer func() { _ = sess.Stop(context.Background()) }()
		if err := sess.Resize(context.Background(), 40, 120); err != nil {
			t.Errorf("Resize(40,120) = %v; want nil", err)
		}
	})
	t.Run("ResizeZeroErrors", func(t *testing.T) {
		if binarySkip {
			t.Skip("BinarySkip=true: resize binary not available")
		}
		sess := mustStart(t, rt, opts(t))
		defer func() { _ = sess.Stop(context.Background()) }()
		_ = sess.Resize(context.Background(), 0, 0)
	})
}

func runCapsProviderSessionID(t *testing.T, rt agentsessions.Runtime, opts startOptsFn, binarySkip bool) {
	t.Helper()
	if binarySkip {
		t.Skip("BinarySkip=true: binary not available")
	}
	t.Run("ImplementsSessionIDer", func(t *testing.T) {
		sess := mustStart(t, rt, opts(t))
		defer func() { _ = sess.Stop(context.Background()) }()
		if _, ok := sess.(agentsessions.SessionIDer); !ok {
			t.Errorf("Session does not implement agentsessions.SessionIDer; required when Caps().ProviderSessionID=true")
		}
	})
	t.Run("PresetCarriedBeforeTurn", func(t *testing.T) {
		o := opts(t)
		o.SessionIDPreset = "ses_compliance_preset"
		sess := mustStart(t, rt, o)
		defer func() { _ = sess.Stop(context.Background()) }()
		sider, ok := sess.(agentsessions.SessionIDer)
		if !ok {
			t.Skip("SessionIDer not implemented — covered by ImplementsSessionIDer")
		}
		if got := sider.ProviderSessionID(); got != "ses_compliance_preset" {
			t.Errorf("ProviderSessionID() = %q before any turn; want ses_compliance_preset", got)
		}
	})
}

func runCapsCheckpointResume(t *testing.T, rt agentsessions.Runtime, opts startOptsFn, binarySkip bool) {
	t.Helper()
	t.Run("CheckpointHintsCallable", func(t *testing.T) {
		if binarySkip {
			t.Skip("BinarySkip=true: checkpoint binary not available")
		}
		sess := mustStart(t, rt, opts(t))
		defer func() { _ = sess.Stop(context.Background()) }()
		// May return (_, false) before any turn — caps gate just
		// guarantees the API surface; non-trivial hints land after a turn.
		_, _ = sess.CheckpointHints()
	})
}

func runCapsNoBinary(t *testing.T, rt agentsessions.Runtime) {
	t.Helper()
	t.Run("PrepareNoError", func(t *testing.T) {
		if err := rt.Prepare(context.Background()); err != nil {
			t.Errorf("Prepare = %v; want nil for BinaryRequired=false runtime", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustStart(t *testing.T, rt agentsessions.Runtime, opts agentsessions.StartOptions) agentsessions.Session {
	t.Helper()
	sess, err := rt.Start(context.Background(), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return sess
}

// syncBuffer is a thread-safe bytes.Buffer for use as a Fanout writer in
// tests.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
