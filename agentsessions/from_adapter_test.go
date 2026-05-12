package agentsessions

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-sandbox/sandbox"
)

// echoAdapter is a minimal provider.CLIAdapter for end-to-end testing.
// It wraps a host binary that emits one line per StreamEvent, and parses
// each line as a stream-json-shaped event:
//
//	delta:<text>     → EventDelta with Content=<text>
//	session:<id>     → EventSessionID with SessionID=<id>
//	done             → EventDone
//
// The harness creates a small shell script at test time that emits the
// events the test wants to observe; the adapter's BuildArgs returns
// "<script>" so the runner spawns it with no further argv.
type echoAdapter struct {
	script string
}

func (a *echoAdapter) Name() string { return "echo-test" }

func (a *echoAdapter) BuildArgs(prompt, _, sessionID string) []string {
	// Single-arg invocation: the script ignores its argv.
	return []string{}
}

func (a *echoAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error) {
	// Trim only CR/LF (the runner's bufio.Scanner already strips the
	// trailing newline, but we defensively cover \r). Do not TrimSpace —
	// that would eat trailing spaces in delta content.
	s := strings.TrimRight(string(line), "\r\n")
	switch {
	case strings.HasPrefix(s, "delta:"):
		return []llmtypes.StreamEvent{{Type: llmtypes.EventDelta, Content: strings.TrimPrefix(s, "delta:")}}, nil
	case strings.HasPrefix(s, "session:"):
		return []llmtypes.StreamEvent{{Type: llmtypes.EventSessionID, SessionID: strings.TrimPrefix(s, "session:")}}, nil
	case s == "done":
		return []llmtypes.StreamEvent{{Type: llmtypes.EventDone}}, nil
	}
	return nil, nil
}

func (a *echoAdapter) Detect() (string, bool) {
	return a.script, a.script != ""
}

// writeTestScript drops a tiny shell script in dir that emits the given
// lines and exits 0. Returns the absolute path.
func writeTestScript(t *testing.T, dir string, lines []string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "fake-cli.sh")
	body := "#!/bin/sh\n"
	for _, l := range lines {
		// Use printf to avoid shell-interpreting the line.
		body += "printf '%s\\n' " + shellQuote(l) + "\n"
	}
	body += "exit 0\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// writeTestScriptWithStderr is writeTestScript plus a single line emitted
// to stderr before the stdout stream. Used for stderr-passthrough tests.
func writeTestScriptWithStderr(t *testing.T, dir, stderrLine string, stdoutLines []string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "fake-cli-stderr.sh")
	body := "#!/bin/sh\n"
	body += "printf '%s\\n' " + shellQuote(stderrLine) + " 1>&2\n"
	for _, l := range stdoutLines {
		body += "printf '%s\\n' " + shellQuote(l) + "\n"
	}
	body += "exit 0\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func writeFD3Script(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "fake-cli-fd3.sh")
	body := `#!/bin/sh
IFS= read -r payload <&3
printf 'delta:%s\n' "$payload"
printf 'done\n'
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func TestStartOptions_Stderr_CapturesToWriter(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScriptWithStderr(t, dir, "diagnostic-line",
		[]string{"delta:hi", "done"})

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "stderr-passthrough",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var stderrBuf bytes.Buffer
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		Stderr:  &stderrBuf,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	got := stderrBuf.String()
	if !strings.Contains(got, "diagnostic-line") {
		t.Errorf("Stderr buffer = %q, want to contain %q", got, "diagnostic-line")
	}
}

func TestStartOptions_Stderr_NilLeavesStderrUnset(t *testing.T) {
	// Sanity: nil Stderr (the v0.2.0 zero-value default) does not regress
	// the no-knob behavior — Run completes cleanly and any subprocess
	// stderr is discarded per os/exec's default for cmd.Stderr=nil.
	dir := t.TempDir()
	script := writeTestScriptWithStderr(t, dir, "should-be-discarded",
		[]string{"delta:hi", "done"})

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "stderr-nil",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	sess, err := rt.Start(context.Background(), StartOptions{Workdir: dir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}
}

func TestStartOptions_ExtraFiles_PassesThroughToRunner(t *testing.T) {
	dir := t.TempDir()
	script := writeFD3Script(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "extra-files",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer readEnd.Close()

	if _, err := writeEnd.WriteString("fd3-through-runner\n"); err != nil {
		t.Fatalf("write pipe: %v", err)
	}
	if err := writeEnd.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}

	var fanout bytes.Buffer
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:    dir,
		Fanout:     &fanout,
		ExtraFiles: []*os.File{readEnd},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	if got := fanout.String(); got != "fd3-through-runner\n[turn_done]\n" {
		t.Fatalf("fanout = %q, want %q", got, "fd3-through-runner\n[turn_done]\n")
	}
}

func TestStartOptions_ExtraFiles_WithSandboxProfile_PreservesFDs(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skipf("sandbox.Apply unsupported on %s", runtime.GOOS)
	}
	if runtime.GOOS == "linux" {
		if _, err := exec.LookPath("bwrap"); err != nil {
			t.Skip("bwrap not installed")
		}
	}
	if runtime.GOOS == "darwin" {
		if _, err := exec.LookPath("sandbox-exec"); err != nil {
			t.Skip("sandbox-exec not installed")
		}
	}

	dir := t.TempDir()
	script := writeFD3Script(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "extra-files-sandbox",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer readEnd.Close()

	if _, err := writeEnd.WriteString("fd3-through-sandbox\n"); err != nil {
		t.Fatalf("write pipe: %v", err)
	}
	if err := writeEnd.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}

	var fanout bytes.Buffer
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:    dir,
		Fanout:     &fanout,
		ExtraFiles: []*os.File{readEnd},
		Profile: sandbox.Profile{
			ID:          "extra-files-sandbox",
			Description: "test extra file inheritance through sandbox wrapper",
			FS:          sandbox.FSSpec{Read: []string{"workspace"}, Write: []string{"workspace"}},
			Net:         false,
			Subprocess:  true,
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sess.SendInput(ctx, []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	if got := fanout.String(); got != "fd3-through-sandbox\n[turn_done]\n" {
		t.Fatalf("fanout = %q, want %q", got, "fd3-through-sandbox\n[turn_done]\n")
	}
}

func TestAdapterRuntime_EndToEnd_DrivesRunnerAndCapturesEvents(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"session:ses_abc123",
		"delta:hello, ",
		"delta:world",
		"done",
	})

	adapter := &echoAdapter{script: script}
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "echo-test",
		Kind:    "cli",
		Adapter: adapter,
		Caps:    Capabilities{ProviderSessionID: true, BinaryRequired: true, CheckpointResume: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	if err := rt.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	var fanout bytes.Buffer
	var observedSessionID string
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     dir,
		Fanout:      &fanout,
		OnSessionID: func(id string) { observedSessionID = id },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored prompt")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	if observedSessionID != "ses_abc123" {
		t.Errorf("OnSessionID got %q, want ses_abc123", observedSessionID)
	}

	out := fanout.String()
	if !strings.Contains(out, "hello, ") || !strings.Contains(out, "world") {
		t.Errorf("fanout = %q, want both hello and world", out)
	}
	if !strings.Contains(out, "[turn_done]") {
		t.Errorf("fanout missing turn_done marker: %q", out)
	}

	// SessionIDer surface
	sider, ok := sess.(SessionIDer)
	if !ok {
		t.Fatal("session does not implement SessionIDer")
	}
	if sider.ProviderSessionID() != "ses_abc123" {
		t.Errorf("ProviderSessionID = %q", sider.ProviderSessionID())
	}

	// CheckpointHints honors the cap
	hint, ok := sess.CheckpointHints()
	if !ok {
		t.Errorf("CheckpointHints !ok with CheckpointResume cap + observed session id")
	}
	if string(hint) != "ses_abc123" {
		t.Errorf("hint = %q", hint)
	}
}

func TestAdapterRuntime_DrivenThroughManager_AttachStreamsLive(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"delta:streamed-",
		"delta:bytes",
		"done",
	})

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "echo-mgr",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps:    Capabilities{BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	m := NewManager(&memSink{})
	if err := m.Start(context.Background(), StartRequest{
		ID:      "s",
		Runtime: rt,
		Options: StartOptions{Workdir: dir, AttachEnabled: true},
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}

	// Attach in the background to capture live output.
	var attached bytes.Buffer
	attachCtx, attachCancel := context.WithCancel(context.Background())
	defer attachCancel()
	attachDone := make(chan struct{})
	go func() {
		defer close(attachDone)
		_ = m.Attach(attachCtx, "s", &attached)
	}()

	// Give Attach a moment to subscribe.
	time.Sleep(50 * time.Millisecond)

	if err := m.SendInput("s", []byte("hi")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	// Stop after the turn drains.
	time.Sleep(50 * time.Millisecond)
	if err := m.Stop(context.Background(), "s"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := m.WaitSession(context.Background(), "s"); err != nil {
		t.Fatalf("WaitSession: %v", err)
	}
	attachCancel()
	select {
	case <-attachDone:
	case <-time.After(2 * time.Second):
		t.Fatal("attach goroutine did not exit")
	}

	got := attached.String()
	if !strings.Contains(got, "streamed-") || !strings.Contains(got, "bytes") {
		t.Errorf("attach output = %q, want streamed-bytes", got)
	}
}

func TestAdapterRuntime_TurnInFlight_RejectsConcurrentSendInput(t *testing.T) {
	// Use a script that sleeps briefly so we can fire a second SendInput
	// while the first is still in flight. sh's `sleep` is universal.
	dir := t.TempDir()
	path := filepath.Join(dir, "slow.sh")
	if runtime.GOOS == "windows" {
		t.Skip("needs sh")
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nsleep 0.3\nprintf 'done\\n'\n"), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "slow",
		Kind:    "cli",
		Adapter: &echoAdapter{script: path},
	})
	sess, err := rt.Start(context.Background(), StartOptions{Workdir: dir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	firstDone := make(chan error, 1)
	go func() { firstDone <- sess.SendInput(context.Background(), []byte("first")) }()
	// Wait briefly for the first SendInput to claim the turn lock.
	time.Sleep(80 * time.Millisecond)

	err2 := sess.SendInput(context.Background(), []byte("second"))
	if !errors.Is(err2, ErrTurnInFlight) {
		t.Errorf("second SendInput = %v, want ErrTurnInFlight", err2)
	}
	if err1 := <-firstDone; err1 != nil {
		t.Errorf("first SendInput error: %v", err1)
	}
}

func TestNewFromAdapter_RejectsMultipleLifecycleFlags(t *testing.T) {
	cases := []struct {
		name string
		caps Capabilities
	}{
		{"pty+streaming", Capabilities{PTY: true, StreamingStdio: true}},
		{"pty+jsonrpc", Capabilities{PTY: true, JsonRpcStdio: true}},
		{"streaming+jsonrpc", Capabilities{StreamingStdio: true, JsonRpcStdio: true}},
		{"all-three", Capabilities{PTY: true, StreamingStdio: true, JsonRpcStdio: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFromAdapter(AdapterRuntimeConfig{
				ID:      "mutex-" + tc.name,
				Kind:    "cli",
				Adapter: &echoAdapter{script: ""},
				Caps:    tc.caps,
			})
			if err == nil {
				t.Fatalf("NewFromAdapter accepted mutually-exclusive lifecycle flags %+v", tc.caps)
			}
			if !strings.Contains(err.Error(), "lifecycle flag") {
				t.Errorf("error = %v, want one mentioning 'lifecycle flag'", err)
			}
		})
	}
}

func TestNewFromAdapter_SingleLifecycleFlagAccepted(t *testing.T) {
	// Each lifecycle flag in isolation routes to its dedicated runtime;
	// default (no flags) routes to the subprocess-per-turn adapter runtime.
	cases := []struct {
		name        string
		caps        Capabilities
		wantErrSubs string // empty when construction should succeed
	}{
		{"default-no-flags", Capabilities{}, ""},
		{"pty-only", Capabilities{PTY: true}, ""},
		{"streaming-only", Capabilities{StreamingStdio: true}, ""},
		{"jsonrpc-only", Capabilities{JsonRpcStdio: true}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFromAdapter(AdapterRuntimeConfig{
				ID:      "single-" + tc.name,
				Kind:    "cli",
				Adapter: &echoAdapter{script: ""},
				Caps:    tc.caps,
			})
			if tc.wantErrSubs == "" {
				if err != nil {
					t.Fatalf("NewFromAdapter rejected single-flag caps %+v: %v", tc.caps, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("NewFromAdapter accepted not-yet-wired caps %+v", tc.caps)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubs) {
				t.Errorf("error = %v, want substring %q", err, tc.wantErrSubs)
			}
		})
	}
}

func TestAdapterRuntime_PrepareReportsMissingBinary(t *testing.T) {
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "missing",
		Kind:    "cli",
		Adapter: &echoAdapter{script: ""}, // empty path → Detect returns !ok
		Caps:    Capabilities{BinaryRequired: true},
	})
	if err := rt.Prepare(context.Background()); err == nil {
		t.Errorf("Prepare = nil, want error for missing binary")
	}
}

func TestStartOptions_EventFanout_DeliversToBothFanouts(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"session:ses_abc123",
		"delta:hello",
		"done",
	})
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "event-fanout-both",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var fanout bytes.Buffer
	eventCh := make(chan llmtypes.StreamEvent, 8)
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     dir,
		Fanout:      &fanout,
		EventFanout: eventCh,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	want := []llmtypes.StreamEvent{
		{Type: llmtypes.EventSessionID, SessionID: "ses_abc123"},
		{Type: llmtypes.EventDelta, Content: "hello"},
		{Type: llmtypes.EventDone},
	}
	if got := takeEvents(eventCh, len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("EventFanout events = %#v, want %#v", got, want)
	}
	if got := fanout.String(); got != "hello\n[turn_done]\n" {
		t.Fatalf("byte Fanout = %q, want %q", got, "hello\n[turn_done]\n")
	}
}

func TestStartOptions_EventFanout_NilByteFanout_StillWorks(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"session:ses_onlytyped",
		"delta:typed-only",
		"done",
	})
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "event-fanout-typed-only",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	eventCh := make(chan llmtypes.StreamEvent, 8)
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     dir,
		EventFanout: eventCh,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	want := []llmtypes.StreamEvent{
		{Type: llmtypes.EventSessionID, SessionID: "ses_onlytyped"},
		{Type: llmtypes.EventDelta, Content: "typed-only"},
		{Type: llmtypes.EventDone},
	}
	if got := takeEvents(eventCh, len(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("EventFanout events = %#v, want %#v", got, want)
	}
}

func TestStartOptions_EventFanout_SlowConsumer_DropsNotBlocks(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"delta:one",
		"delta:two",
		"delta:three",
		"done",
	})
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "event-fanout-slow",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	eventCh := make(chan llmtypes.StreamEvent, 1)
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     dir,
		EventFanout: eventCh,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	done := make(chan error, 1)
	go func() {
		done <- sess.SendInput(context.Background(), []byte("ignored"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendInput: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendInput blocked on slow EventFanout consumer")
	}

	got := drainEvents(eventCh)
	if len(got) != 1 {
		t.Fatalf("received %d typed events, want exactly 1 buffered event after drops", len(got))
	}
}

func TestStartOptions_EventFanout_NilPreservesV010Behavior(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"delta:hello, ",
		"delta:world",
		"done",
	})
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "event-fanout-nil",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var fanout bytes.Buffer
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		Fanout:  &fanout,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ignored")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}
	if got := fanout.String(); got != "hello, world\n[turn_done]\n" {
		t.Fatalf("byte Fanout = %q, want %q", got, "hello, world\n[turn_done]\n")
	}
}

func takeEvents(ch <-chan llmtypes.StreamEvent, n int) []llmtypes.StreamEvent {
	out := make([]llmtypes.StreamEvent, 0, n)
	for i := 0; i < n; i++ {
		select {
		case ev := <-ch:
			out = append(out, ev)
		case <-time.After(2 * time.Second):
			return out
		}
	}
	return out
}

func drainEvents(ch <-chan llmtypes.StreamEvent) []llmtypes.StreamEvent {
	var out []llmtypes.StreamEvent
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}
