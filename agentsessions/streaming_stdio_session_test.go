//go:build !windows

package agentsessions

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// syncBuffer is a thread-safe bytes.Buffer wrapper for fanout in tests.
// The reader goroutine writes to it concurrently with the test goroutine
// polling String(); the bare bytes.Buffer is unsafe under -race.
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

// writeStreamingEchoScript drops a tiny shell script that emits a session
// init line, then reads stdin line by line and emits one delta + done per
// line. Used by streaming-stdio tests to drive a real long-lived child.
func writeStreamingEchoScript(t *testing.T, dir, sessionID string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "fake-streaming.sh")
	body := "#!/bin/sh\n"
	if sessionID != "" {
		body += "printf 'session:%s\\n' " + shellQuote(sessionID) + "\n"
	}
	body += `while IFS= read -r line; do
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

func TestStreamingStdioSession_HappyPath_BootSendInputStop(t *testing.T) {
	dir := t.TempDir()
	script := writeStreamingEchoScript(t, dir, "ses_streaming")

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "streaming-happy",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps: Capabilities{
			StreamingStdio:    true,
			BinaryRequired:    true,
			ProviderSessionID: true,
			CheckpointResume:  true,
		},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	if err := rt.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	var fanout syncBuffer
	var observedSessionID atomic.Value
	observedSessionID.Store("")
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:     dir,
		LogPath:     filepath.Join(dir, "session.log"),
		Fanout:      &fanout,
		OnSessionID: func(id string) { observedSessionID.Store(id) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	// Wait briefly for the reader goroutine to drain the echo + session id.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sid, _ := observedSessionID.Load().(string)
		if sid != "" && strings.Contains(fanout.String(), "delta:hello") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	gotSID, _ := observedSessionID.Load().(string)
	if gotSID != "ses_streaming" {
		t.Errorf("OnSessionID got %q, want ses_streaming", gotSID)
	}
	out := fanout.String()
	if !strings.Contains(out, "session:ses_streaming") {
		t.Errorf("fanout missing session line: %q", out)
	}
	if !strings.Contains(out, "delta:hello") {
		t.Errorf("fanout missing delta:hello: %q", out)
	}

	sider, ok := sess.(SessionIDer)
	if !ok {
		t.Fatal("session does not implement SessionIDer")
	}
	if sider.ProviderSessionID() != "ses_streaming" {
		t.Errorf("ProviderSessionID = %q", sider.ProviderSessionID())
	}

	hint, ok := sess.CheckpointHints()
	if !ok || string(hint) != "ses_streaming" {
		t.Errorf("CheckpointHints = (%q, %v); want (ses_streaming, true)", string(hint), ok)
	}

	if err := sess.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if _, err := sess.Wait(); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

func TestStreamingStdioSession_MultiTurnSameProcess(t *testing.T) {
	dir := t.TempDir()
	script := writeStreamingEchoScript(t, dir, "")

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "streaming-multiturn",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var fanout syncBuffer
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Fanout:  &fanout,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	reporter, ok := sess.(PIDReporter)
	if !ok {
		t.Fatal("session does not implement PIDReporter")
	}
	pid1 := reporter.LivePID()
	if pid1 == 0 {
		t.Fatal("LivePID = 0 after Start")
	}

	if err := sess.SendInput(context.Background(), []byte("first")); err != nil {
		t.Fatalf("SendInput first: %v", err)
	}
	if err := sess.SendInput(context.Background(), []byte("second")); err != nil {
		t.Fatalf("SendInput second: %v", err)
	}

	// Wait for both echoes to flow through the reader goroutine.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(fanout.String(), "delta:first") && strings.Contains(fanout.String(), "delta:second") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	out := fanout.String()
	if !strings.Contains(out, "delta:first") || !strings.Contains(out, "delta:second") {
		t.Errorf("fanout = %q, want both turns", out)
	}

	pid2 := reporter.LivePID()
	if pid2 != pid1 {
		t.Errorf("PID changed across turns: %d -> %d (expected same long-lived child)", pid1, pid2)
	}
}

func TestStreamingStdioSession_AutoFireFirstTurn(t *testing.T) {
	dir := t.TempDir()
	script := writeStreamingEchoScript(t, dir, "")

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "streaming-autofire",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var fanout syncBuffer
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:           dir,
		LogPath:           filepath.Join(dir, "session.log"),
		Fanout:            &fanout,
		AutoFireFirstTurn: true,
		FirstTurnPayload:  []byte("auto-kickoff"),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(fanout.String(), "delta:auto-kickoff") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("autofire kickoff never observed in fanout: %q", fanout.String())
}

func TestStreamingStdioSession_StopClosesStdinAndExits(t *testing.T) {
	dir := t.TempDir()
	script := writeStreamingEchoScript(t, dir, "")

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "streaming-stop",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

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
	code, werr := sess.Wait()
	if werr != nil {
		t.Errorf("Wait error after Stop: %v", werr)
	}
	// sh script exits 0 on stdin EOF.
	if code != 0 {
		t.Errorf("exit code = %d, want 0 (clean stdin-EOF exit)", code)
	}

	// SendInput after Stop is a write to a closed channel.
	if err := sess.SendInput(context.Background(), []byte("after-stop")); err != ErrNoInputChannel {
		t.Errorf("SendInput after Stop = %v, want ErrNoInputChannel", err)
	}
}

func TestStreamingStdioSession_RequiresWorkdir(t *testing.T) {
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "streaming-no-workdir",
		Kind:    "cli",
		Adapter: &echoAdapter{script: "/bin/true"},
		Caps:    Capabilities{StreamingStdio: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	_, err = rt.Start(context.Background(), StartOptions{})
	if err == nil {
		t.Fatal("Start without Workdir = nil, want error")
	}
	if !strings.Contains(err.Error(), "Workdir") {
		t.Errorf("error = %v, want one mentioning Workdir", err)
	}
}
