package agentsessions

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var _ = bytes.NewBuffer // bytes import retained for SendInput test

func TestManager_Start_RegistersAndRecordsRunning(t *testing.T) {
	sink := &memSink{}
	events := &memEvents{}
	m := NewManager(sink).WithEventSink(events)

	rt := newFakeRuntime("fake-rt", "test")
	if err := m.Start(context.Background(), StartRequest{
		ID:      "s-1",
		Runtime: rt,
		Options: StartOptions{Workdir: t.TempDir()},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	info, ok := m.Get("s-1")
	if !ok {
		t.Fatal("Get returned !ok after Start")
	}
	if info.State != StateRunning {
		t.Errorf("State = %v, want %v", info.State, StateRunning)
	}
	if info.RuntimeID != "fake-rt" {
		t.Errorf("RuntimeID = %q, want fake-rt", info.RuntimeID)
	}
	if info.PID == 0 {
		t.Errorf("PID = 0, want >0")
	}

	logSnapshot := sink.snapshot()
	if len(logSnapshot) < 2 {
		t.Fatalf("sink log too short: %v", logSnapshot)
	}
	if !strings.HasPrefix(logSnapshot[0], "s-1:launching") {
		t.Errorf("first sink log = %q, want s-1:launching*", logSnapshot[0])
	}
	if !strings.HasPrefix(logSnapshot[1], "s-1:running") {
		t.Errorf("second sink log = %q, want s-1:running*", logSnapshot[1])
	}

	evs := events.snapshot()
	if len(evs) < 2 {
		t.Fatalf("events too short: %v", evs)
	}
	if evs[0].To != StateLaunching || evs[1].To != StateRunning {
		t.Errorf("event transitions = %v→%v / %v→%v, want →launching / →running",
			evs[0].From, evs[0].To, evs[1].From, evs[1].To)
	}

	// Cleanup.
	rt.lastSession().complete(0)
	_, _ = m.WaitSession(context.Background(), "s-1")
}

func TestManager_Start_RuntimeError_RecordsFailed(t *testing.T) {
	sink := &memSink{}
	m := NewManager(sink)
	err := m.Start(context.Background(), StartRequest{
		ID:      "s-err",
		Runtime: errStartRuntime{err: errFakeStart},
		Options: StartOptions{},
	})
	if !errors.Is(err, errFakeStart) {
		t.Fatalf("Start error = %v, want errFakeStart", err)
	}

	log := sink.snapshot()
	if len(log) < 2 {
		t.Fatalf("sink log = %v", log)
	}
	if !strings.HasPrefix(log[0], "s-err:launching") {
		t.Errorf("first = %q", log[0])
	}
	if !strings.HasPrefix(log[1], "s-err:failed") {
		t.Errorf("second = %q, want s-err:failed*", log[1])
	}
}

func TestManager_Stop_SetsKillingAndCallsSessionStop(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	if err := m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Stop(context.Background(), "s"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !rt.lastSession().killed.Load() {
		t.Errorf("session.killed = false, want true")
	}
	// WaitSession returns the killed exit code.
	code, err := m.WaitSession(context.Background(), "s")
	if err != nil {
		t.Fatalf("WaitSession: %v", err)
	}
	if code != -1 {
		t.Errorf("exit code = %d, want -1 (killed)", code)
	}
}

func TestManager_NaturalExit_CompletedTransition(t *testing.T) {
	sink := &memSink{}
	m := NewManager(sink)
	rt := newFakeRuntime("fake", "test")
	if err := m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	rt.lastSession().complete(0)
	code, err := m.WaitSession(context.Background(), "s")
	if err != nil {
		t.Fatalf("WaitSession: %v", err)
	}
	if code != 0 {
		t.Errorf("code = %d, want 0", code)
	}
	// Final sink log: done with exit=0
	log := sink.snapshot()
	if last := log[len(log)-1]; !strings.Contains(last, "done") || !strings.Contains(last, "exit=0") {
		t.Errorf("final sink log = %q, want *done* with exit=0", last)
	}
}

func TestManager_NaturalExit_NonZero_FailedTransition(t *testing.T) {
	sink := &memSink{}
	m := NewManager(sink)
	rt := newFakeRuntime("fake", "test")
	_ = m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	})
	rt.lastSession().complete(7)
	_, _ = m.WaitSession(context.Background(), "s")
	last := sink.snapshot()[len(sink.snapshot())-1]
	if !strings.Contains(last, "failed") || !strings.Contains(last, "exit=7") {
		t.Errorf("final = %q, want *failed* exit=7", last)
	}
}

func TestManager_SendInput_SerializedThroughEntryLock(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	var buf bytes.Buffer
	rt.inputSinkOnStart = &buf
	_ = m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	})

	if err := m.SendInput("s", []byte("hello\n")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}
	if got := buf.String(); got != "hello\n" {
		t.Errorf("buf = %q", got)
	}
	rt.lastSession().complete(0)
}

func TestManager_Resize_ForwardsToSession(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	_ = m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	})
	if err := m.Resize("s", 40, 120); err != nil {
		t.Fatalf("Resize: %v", err)
	}
	sess := rt.lastSession()
	if r, c := sess.resizeRows.Load(), sess.resizeCols.Load(); r != 40 || c != 120 {
		t.Errorf("resize = (%d,%d), want (40,120)", r, c)
	}
	rt.lastSession().complete(0)
}

func TestManager_Attach_ReplaysAndStreams(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	_ = m.Start(context.Background(), StartRequest{
		ID:      "s",
		Runtime: rt,
		Options: StartOptions{Workdir: t.TempDir(), AttachEnabled: true},
	})
	sess := rt.lastSession()

	// Pre-attach output (goes into ring + dropped — no subscribers yet)
	sess.emit([]byte("history-bytes\n"))
	if !waitFor(time.Second, func() bool { return true }) {
		// no condition; just yield
	}

	out := &syncBuf{}
	ctx, cancel := context.WithCancel(context.Background())
	attachDone := make(chan struct{})
	go func() {
		defer close(attachDone)
		_ = m.Attach(ctx, "s", out)
	}()
	time.Sleep(50 * time.Millisecond) // let Attach subscribe
	sess.emit([]byte("live-bytes\n"))
	time.Sleep(50 * time.Millisecond) // let live bytes drain

	cancel()
	<-attachDone
	sess.complete(0)
	_, _ = m.WaitSession(context.Background(), "s")

	if got := out.String(); !strings.Contains(got, "history-bytes") || !strings.Contains(got, "live-bytes") {
		t.Errorf("attach output = %q, want both history and live", got)
	}
}

func TestManager_Attach_Disabled_ReturnsErrAttachDisabled(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	_ = m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt,
		Options: StartOptions{Workdir: t.TempDir()}, // AttachEnabled defaults false
	})
	defer func() {
		rt.lastSession().complete(0)
	}()
	var buf bytes.Buffer
	err := m.Attach(context.Background(), "s", &buf)
	if !errors.Is(err, ErrAttachDisabled) {
		t.Errorf("Attach err = %v, want ErrAttachDisabled", err)
	}
}

func TestManager_Shutdown_DrainsWatchGoroutines(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	_ = m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	})

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- m.Shutdown(ctx)
	}()

	// Shutdown must not return while session is alive.
	select {
	case <-done:
		t.Fatal("Shutdown returned before session terminated")
	case <-time.After(100 * time.Millisecond):
	}

	rt.lastSession().complete(0)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after session completed")
	}
}

func TestManager_Start_AfterShutdown_ReturnsErrManagerStopped(t *testing.T) {
	m := NewManager(&memSink{})
	_ = m.Shutdown(context.Background())
	err := m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: newFakeRuntime("fake", "test"),
	})
	if !errors.Is(err, ErrManagerStopped) {
		t.Errorf("Start err = %v, want ErrManagerStopped", err)
	}
}

func TestManager_Health_CombinesHealthAndCaps(t *testing.T) {
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	rt.caps = Capabilities{PTY: true, Resize: true}
	_ = m.Start(context.Background(), StartRequest{
		ID: "s", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	})

	hs, ok := m.Health("s")
	if !ok {
		t.Fatal("Health !ok")
	}
	if !hs.Caps.PTY || !hs.Caps.Resize {
		t.Errorf("Caps not propagated: %+v", hs.Caps)
	}
	if !hs.Health.Alive {
		t.Errorf("Health.Alive false")
	}
	rt.lastSession().complete(0)
}

func TestManager_WaitSession_CleanExit(t *testing.T) {
	// Regression guard: a clean (code=0) Session.Wait must continue to
	// return (0, nil) from WaitSession after the error-propagation change.
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	if err := m.Start(context.Background(), StartRequest{
		ID: "s-clean", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	rt.lastSession().complete(0)

	code, waitErr := m.WaitSession(context.Background(), "s-clean")
	if waitErr != nil {
		t.Errorf("WaitSession err = %v, want nil for clean exit", waitErr)
	}
	if code != 0 {
		t.Errorf("code = %d, want 0", code)
	}
}

func TestManager_WaitSession_NonZeroExitNoSupervisor(t *testing.T) {
	// A non-supervised session whose Session.Wait returned a non-nil error
	// must surface the underlying error verbatim through WaitSession. The
	// error must NOT errors.As to *ExitError because no supervision was in
	// play — supervised classification is the only producer of *ExitError.
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	if err := m.Start(context.Background(), StartRequest{
		ID: "s-nonzero", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	sentinel := errors.New("non-zero exit from unsupervised runtime")
	rt.lastSession().completeWithErr(1, sentinel)

	code, waitErr := m.WaitSession(context.Background(), "s-nonzero")
	if !errors.Is(waitErr, sentinel) {
		t.Errorf("WaitSession err = %v, want %v (verbatim from Session.Wait)", waitErr, sentinel)
	}
	var xe *ExitError
	if errors.As(waitErr, &xe) {
		t.Errorf("errors.As(*ExitError) = true; non-supervised exits must not produce *ExitError")
	}
	if code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
}

func TestManager_WaitSession_StopDrivenTermination(t *testing.T) {
	// A caller-driven Stop sets entry.killing in watch(); verify the
	// killing-flag branch does not swallow whatever Session.Wait produced.
	// fakeSession's Stop closes done with code=-1 and waitErr unset (nil),
	// modeling a clean kill — WaitSession must surface (-1, nil), not a
	// synthetic error from the killing branch.
	m := NewManager(&memSink{})
	rt := newFakeRuntime("fake", "test")
	if err := m.Start(context.Background(), StartRequest{
		ID: "s-stop", Runtime: rt, Options: StartOptions{Workdir: t.TempDir()},
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Stop(context.Background(), "s-stop"); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	code, waitErr := m.WaitSession(context.Background(), "s-stop")
	if waitErr != nil {
		t.Errorf("WaitSession err = %v, want nil (fakeSession Stop yields nil Wait err)", waitErr)
	}
	if code != -1 {
		t.Errorf("code = %d, want -1 (killed)", code)
	}
}
