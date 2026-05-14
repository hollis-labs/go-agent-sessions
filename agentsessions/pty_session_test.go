//go:build !windows

package agentsessions

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
	pevents "github.com/hollis-labs/go-providers/provider/events"
)

// ptyEchoAdapter is a CLIAdapter for a long-lived shell that loops: read
// a stdin line, emit `delta:<line>` then `done`, repeat until EOF.
//
// It implements EventParser so the PTY runtime exercises the typed-event
// fan-out path; ParseLine produces the legacy llmtypes.StreamEvent.
type ptyEchoAdapter struct {
	scriptPath string
}

func (a *ptyEchoAdapter) Name() string                      { return "pty-echo-test" }
func (a *ptyEchoAdapter) Detect() (string, bool)            { return a.scriptPath, a.scriptPath != "" }
func (a *ptyEchoAdapter) BuildArgs(_, _, _ string) []string { return []string{} }

func (a *ptyEchoAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error) {
	s := strings.TrimRight(string(line), "\r\n")
	switch {
	case strings.HasPrefix(s, "delta:"):
		return []llmtypes.StreamEvent{{Type: llmtypes.EventDelta, Content: strings.TrimPrefix(s, "delta:")}}, nil
	case s == "done":
		return []llmtypes.StreamEvent{{Type: llmtypes.EventDone}}, nil
	}
	return nil, nil
}

func (a *ptyEchoAdapter) ParseLineEvents(line []byte) ([]pevents.Event, error) {
	s := strings.TrimRight(string(line), "\r\n")
	switch {
	case strings.HasPrefix(s, "delta:"):
		return []pevents.Event{pevents.Delta{Text: strings.TrimPrefix(s, "delta:")}}, nil
	case s == "done":
		return []pevents.Event{pevents.Done{}}, nil
	}
	return nil, nil
}

// writePTYEchoScript drops a shell loop that responds to each stdin line.
// Behavior: per line read on stdin, print "delta:<line>" then "done".
// Exits 0 on EOF.
func writePTYEchoScript(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "pty-echo.sh")
	body := `#!/bin/sh
while IFS= read -r line; do
  printf 'delta:%s\n' "$line"
  printf 'done\n'
done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func TestPTYRuntime_HappyPath_StartSendInputStop(t *testing.T) {
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-happy",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, Resize: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	if rt.Caps().PTY != true {
		t.Fatalf("Caps.PTY = %v, want true", rt.Caps().PTY)
	}
	if rt.Kind() != "cli" {
		t.Fatalf("Kind = %q, want cli", rt.Kind())
	}

	logPath := filepath.Join(dir, "session.log")
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: logPath,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	// PIDReporter: live PID is non-zero on PTY
	pidr, ok := sess.(PIDReporter)
	if !ok {
		t.Fatal("ptySession does not implement PIDReporter")
	}
	if pidr.LivePID() == 0 {
		t.Errorf("LivePID = 0, want non-zero (PTY child)")
	}
	if pidr.LastPID() != pidr.LivePID() {
		t.Errorf("LastPID (%d) != LivePID (%d) on PTY", pidr.LastPID(), pidr.LivePID())
	}

	if err := sess.SendInput(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	// Wait for the response to land in the log file. Poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(logPath); err == nil {
			if strings.Contains(string(data), "delta:hello") && strings.Contains(string(data), "done") {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, _ := os.ReadFile(logPath)
	t.Fatalf("log never received delta+done (got %q)", string(data))
}

func TestPTYRuntime_FanoutAndEventCallbacks(t *testing.T) {
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-fanout",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	var fanoutBuf syncBuf
	eventCh := make(chan llmtypes.StreamEvent, 16)
	var typedMu sync.Mutex
	var typed []pevents.Event

	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     dir,
		LogPath:     filepath.Join(dir, "session.log"),
		Fanout:      &fanoutBuf,
		EventFanout: eventCh,
		TypedEventCallback: func(ev pevents.Event) {
			typedMu.Lock()
			typed = append(typed, ev)
			typedMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}
	// wait for events to land
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(fanoutBuf.String(), "delta:alpha") && strings.Contains(fanoutBuf.String(), "done") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := fanoutBuf.String(); !strings.Contains(got, "delta:alpha") {
		t.Errorf("byte fanout missing delta:alpha; got %q", got)
	}

	streamEvents := drainEvents(eventCh)
	var sawDelta, sawDone bool
	for _, ev := range streamEvents {
		if ev.Type == llmtypes.EventDelta && ev.Content == "alpha" {
			sawDelta = true
		}
		if ev.Type == llmtypes.EventDone {
			sawDone = true
		}
	}
	if !sawDelta || !sawDone {
		t.Errorf("EventFanout missing delta/done: %+v", streamEvents)
	}

	typedMu.Lock()
	gotTyped := append([]pevents.Event(nil), typed...)
	typedMu.Unlock()
	var typedDelta, typedDone bool
	for _, ev := range gotTyped {
		if d, ok := ev.(pevents.Delta); ok && d.Text == "alpha" {
			typedDelta = true
		}
		if _, ok := ev.(pevents.Done); ok {
			typedDone = true
		}
	}
	if !typedDelta || !typedDone {
		t.Errorf("TypedEventCallback missing delta/done: %+v", gotTyped)
	}
}

func TestPTYRuntime_ConcurrentSendInput_NoInterleaving(t *testing.T) {
	// Two writers racing on SendInput must serialize at the ptmxLock
	// — bytes from one call don't interleave with bytes from another.
	// We can't observe the PTY's stdout deterministically here, but we
	// can assert no goroutine wedges and Stop drains cleanly.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-concurrent",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte("turn-" + string(rune('a'+i)))
			if err := sess.SendInput(context.Background(), payload); err != nil {
				t.Errorf("SendInput[%d]: %v", i, err)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent SendInput goroutines did not return")
	}
}

func TestPTYRuntime_SendInputAfterStop_ReturnsCleanError(t *testing.T) {
	// After the wait goroutine nil-clears ptmx, in-flight SendInput must
	// return ErrNoInputChannel rather than writing to a closed fd.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-stop",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := sess.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Wait for the wait goroutine to drain.
	if _, err := sess.Wait(); err != nil {
		// SIGTERM-killed process; non-nil err is expected, we don't gate on it
		_ = err
	}

	err = sess.SendInput(context.Background(), []byte("after-stop"))
	if !errors.Is(err, ErrNoInputChannel) {
		t.Errorf("SendInput after Stop = %v, want ErrNoInputChannel", err)
	}
}

func TestPTYRuntime_AutoFireFirstTurn_DeliversPayload(t *testing.T) {
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-autofire",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	logPath := filepath.Join(dir, "session.log")
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:           dir,
		LogPath:           logPath,
		AutoFireFirstTurn: true,
		FirstTurnPayload:  []byte("kickoff-payload"),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(logPath); err == nil {
			if strings.Contains(string(data), "delta:kickoff-payload") {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, _ := os.ReadFile(logPath)
	t.Fatalf("AutoFireFirstTurn did not deliver kickoff payload (log: %q)", string(data))
}

func TestPTYRuntime_AutoFireFirstTurn_StdinBootMode_NoOp(t *testing.T) {
	// When BootMode=="stdin" and BootPrompt is non-empty, AutoFireFirstTurn
	// should NOT also fire FirstTurnPayload — the boot-prompt-on-stdin
	// convention already kicked the agent.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-stdin-boot",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	logPath := filepath.Join(dir, "session.log")
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:           dir,
		LogPath:           logPath,
		BootMode:          "stdin",
		BootPrompt:        "boot-prompt-line\n",
		AutoFireFirstTurn: true,
		FirstTurnPayload:  []byte("would-be-kickoff"),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(logPath); err == nil {
			if strings.Contains(string(data), "delta:boot-prompt-line") {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	data, _ := os.ReadFile(logPath)
	got := string(data)
	if !strings.Contains(got, "delta:boot-prompt-line") {
		t.Errorf("BootMode=stdin path didn't deliver boot prompt; log = %q", got)
	}
	if strings.Contains(got, "delta:would-be-kickoff") {
		t.Errorf("AutoFireFirstTurn fired alongside BootMode=stdin; log = %q", got)
	}
}

func TestPTYRuntime_StdinBootPromptLarge_StartReturns(t *testing.T) {
	tests := []struct {
		name       string
		supervisor *SupervisorOptions
	}{
		{name: "legacy"},
		{
			name: "supervised",
			supervisor: &SupervisorOptions{
				IdleKill: 5 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			script := writePTYEchoScript(t, dir)

			rt, _ := NewFromAdapter(AdapterRuntimeConfig{
				ID:      "pty-large-stdin-boot-" + tt.name,
				Kind:    "cli",
				Adapter: &ptyEchoAdapter{scriptPath: script},
				Caps:    Capabilities{PTY: true, BinaryRequired: true},
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			logPath := filepath.Join(dir, "session.log")
			bootPrompt := strings.Repeat("boot-prompt-line\n", 20000)
			started := make(chan struct{})
			var sess Session
			var startErr error
			go func() {
				defer close(started)
				sess, startErr = rt.Start(ctx, StartOptions{
					Workdir:    dir,
					LogPath:    logPath,
					BootMode:   "stdin",
					BootPrompt: bootPrompt,
					Supervisor: tt.supervisor,
				})
			}()

			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("Start blocked on large BootMode=stdin prompt")
			}
			if startErr != nil {
				t.Fatalf("Start: %v", startErr)
			}
			defer func() { _ = sess.Stop(context.Background()) }()

			pidr, ok := sess.(PIDReporter)
			if !ok {
				t.Fatal("pty session does not implement PIDReporter")
			}
			if pidr.LivePID() == 0 {
				t.Fatal("session did not reach running state")
			}

			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if data, err := os.ReadFile(logPath); err == nil && strings.Contains(string(data), "delta:boot-prompt-line") {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			data, _ := os.ReadFile(logPath)
			t.Fatalf("large stdin boot prompt did not reach PTY child (log prefix: %q)", string(data[:min(len(data), 256)]))
		})
	}
}

func TestPTYRuntime_WorkspaceDir_FallbackLogPath(t *testing.T) {
	// LogPath empty, WorkspaceDir set: log lands at <WorkspaceDir>/logs/session.log.
	dir := t.TempDir()
	workspace := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-workspacedir",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})

	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:      dir,
		WorkspaceDir: workspace,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("ws-test")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	expected := filepath.Join(workspace, "logs", "session.log")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(expected); err == nil && strings.Contains(string(data), "delta:ws-test") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("WorkspaceDir log never populated at %s", expected)
}

func TestPTYRuntime_NoLogPathOrWorkspaceDir_StartFails(t *testing.T) {
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-no-log",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	_, err := rt.Start(context.Background(), StartOptions{Workdir: dir})
	if err == nil {
		t.Fatal("Start with no LogPath/WorkspaceDir = nil, want error")
	}
}

func TestNewFromAdapter_CapabilitySelection(t *testing.T) {
	// Caps.PTY=true → ptyRuntime; Caps.PTY=false → adapterRuntime.
	dir := t.TempDir()
	script := writePTYEchoScript(t, dir)

	rtPTY, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "select-pty",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter PTY: %v", err)
	}
	if _, ok := rtPTY.(*ptyRuntime); !ok {
		t.Errorf("Caps.PTY=true returned %T, want *ptyRuntime", rtPTY)
	}

	rtAdapter, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "select-adapter",
		Adapter: &ptyEchoAdapter{scriptPath: script},
		Caps:    Capabilities{PTY: false, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter adapter: %v", err)
	}
	if _, ok := rtAdapter.(*adapterRuntime); !ok {
		t.Errorf("Caps.PTY=false returned %T, want *adapterRuntime", rtAdapter)
	}
}

func TestAdapterRuntime_AutoFireFirstTurn_SyncDelivery(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"delta:auto-fired",
		"done",
	})

	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "adapter-autofire",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps:    Capabilities{BinaryRequired: true},
	})

	var fanoutBuf syncBuf
	startCh := make(chan struct{})
	var sess Session
	var startErr error
	go func() {
		defer close(startCh)
		sess, startErr = rt.Start(context.Background(), StartOptions{
			Workdir:           dir,
			Fanout:            &fanoutBuf,
			AutoFireFirstTurn: true,
			FirstTurnPayload:  []byte("kickoff"),
		})
	}()
	select {
	case <-startCh:
	case <-time.After(3 * time.Second):
		t.Fatal("adapter Start blocked indefinitely on AutoFireFirstTurn")
	}
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	// Synchronous delivery means the fanout already has the response
	// by the time Start returns.
	got := fanoutBuf.String()
	if !strings.Contains(got, "auto-fired") || !strings.Contains(got, "[turn_done]") {
		t.Errorf("AutoFireFirstTurn delivered late or not at all; fanout = %q", got)
	}
}

func TestAdapterRuntime_LivePIDLastPID_DivergeBetweenTurns(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, []string{
		"delta:hi",
		"done",
	})
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "adapter-pid",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps:    Capabilities{BinaryRequired: true},
	})
	sess, err := rt.Start(context.Background(), StartOptions{Workdir: dir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	pidr, ok := sess.(PIDReporter)
	if !ok {
		t.Fatal("adapterSession does not implement PIDReporter")
	}

	// Pre-turn: both 0.
	if pidr.LivePID() != 0 || pidr.LastPID() != 0 {
		t.Errorf("pre-turn LivePID=%d LastPID=%d, want both 0", pidr.LivePID(), pidr.LastPID())
	}

	if err := sess.SendInput(context.Background(), []byte("hi")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	// Post-turn: LivePID resets to 0; LastPID stays set.
	if pidr.LivePID() != 0 {
		t.Errorf("post-turn LivePID = %d, want 0 (no process running)", pidr.LivePID())
	}
	if pidr.LastPID() == 0 {
		t.Errorf("post-turn LastPID = 0, want >0 (sticky last)")
	}
}

func TestPTYRuntime_PrepareReportsMissingBinary(t *testing.T) {
	rt, _ := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "pty-missing",
		Kind:    "cli",
		Adapter: &ptyEchoAdapter{scriptPath: ""},
		Caps:    Capabilities{PTY: true, BinaryRequired: true},
	})
	if err := rt.Prepare(context.Background()); err == nil {
		t.Errorf("Prepare = nil, want error for missing binary")
	}
}
