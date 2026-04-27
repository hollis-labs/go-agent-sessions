package agentsessions

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/go-providers/provider"
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

func (a *echoAdapter) ParseLine(line []byte) ([]provider.StreamEvent, error) {
	// Trim only CR/LF (the runner's bufio.Scanner already strips the
	// trailing newline, but we defensively cover \r). Do not TrimSpace —
	// that would eat trailing spaces in delta content.
	s := strings.TrimRight(string(line), "\r\n")
	switch {
	case strings.HasPrefix(s, "delta:"):
		return []provider.StreamEvent{{Type: provider.EventDelta, Content: strings.TrimPrefix(s, "delta:")}}, nil
	case strings.HasPrefix(s, "session:"):
		return []provider.StreamEvent{{Type: provider.EventSessionID, SessionID: strings.TrimPrefix(s, "session:")}}, nil
	case s == "done":
		return []provider.StreamEvent{{Type: provider.EventDone}}, nil
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

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
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
		Workdir: dir,
		Fanout:  &fanout,
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
