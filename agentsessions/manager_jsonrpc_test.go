//go:build !windows

package agentsessions

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestManager_JsonRpcCall_HappyPath(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcEchoScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-mgr-happy",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	if err := mgr.Start(context.Background(), StartRequest{
		ID:      "s1",
		Runtime: rt,
		Options: StartOptions{
			Workdir: dir,
			LogPath: filepath.Join(dir, "session.log"),
		},
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background(), "s1") }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := mgr.JsonRpcCall(ctx, "s1", "thread.start", map[string]any{"workspace": "."})
	if err != nil {
		t.Fatalf("JsonRpcCall: %v", err)
	}
	var got struct {
		Echoed bool   `json:"echoed"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("decode result: %v (raw=%s)", err, string(result))
	}
	if !got.Echoed || got.Method != "thread.start" {
		t.Errorf("Manager.JsonRpcCall result = %+v, want echoed=true method=thread.start", got)
	}
}

func TestManager_JsonRpcCall_SessionNotFound(t *testing.T) {
	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	_, err := mgr.JsonRpcCall(context.Background(), "nonexistent", "any.method", nil)
	if !errors.Is(err, ErrSessionNotRunning) {
		t.Errorf("err = %v, want ErrSessionNotRunning", err)
	}
}

func TestManager_JsonRpcCall_NotJsonRpcCapable_FakeRuntime(t *testing.T) {
	rt := newFakeRuntime("fake-not-capable", "fake")
	mgr := NewManager(nil)
	defer func() {
		_ = mgr.Stop(context.Background(), "fake1")
		_ = mgr.Shutdown(context.Background())
	}()

	if err := mgr.Start(context.Background(), StartRequest{
		ID:      "fake1",
		Runtime: rt,
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}

	_, err := mgr.JsonRpcCall(context.Background(), "fake1", "any.method", nil)
	if !errors.Is(err, ErrSessionNotJsonRpcCapable) {
		t.Errorf("err = %v, want ErrSessionNotJsonRpcCapable", err)
	}
}

func TestManager_JsonRpcCall_NotJsonRpcCapable_StreamingStdio(t *testing.T) {
	dir := t.TempDir()
	// Streaming-stdio fixture: a child that just consumes stdin and exits
	// on EOF. The session doesn't implement JsonRpcCaller, only the JSON-RPC
	// runtime does — so Manager.JsonRpcCall against it returns the typed
	// error.
	script := writeNoopStdinScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "streaming-not-jsonrpc",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{StreamingStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	if err := mgr.Start(context.Background(), StartRequest{
		ID:      "s2",
		Runtime: rt,
		Options: StartOptions{
			Workdir: dir,
			LogPath: filepath.Join(dir, "session.log"),
		},
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background(), "s2") }()

	_, err = mgr.JsonRpcCall(context.Background(), "s2", "thread.start", nil)
	if !errors.Is(err, ErrSessionNotJsonRpcCapable) {
		t.Errorf("err = %v, want ErrSessionNotJsonRpcCapable", err)
	}
}

func TestManager_JsonRpcCall_NotJsonRpcCapable_AdapterRuntime(t *testing.T) {
	// Adapter runtime (subprocess-per-turn) — never implements JsonRpcCaller.
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "adapter-not-jsonrpc",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: "/bin/true"},
		// No lifecycle flag → adapter runtime selected
		Caps: Capabilities{},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	if err := mgr.Start(context.Background(), StartRequest{
		ID:      "s3",
		Runtime: rt,
		Options: StartOptions{Workdir: t.TempDir()},
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background(), "s3") }()

	_, err = mgr.JsonRpcCall(context.Background(), "s3", "any.method", nil)
	if !errors.Is(err, ErrSessionNotJsonRpcCapable) {
		t.Errorf("err = %v, want ErrSessionNotJsonRpcCapable", err)
	}
}

func TestManager_JsonRpcCall_PassesThroughJsonRpcError(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcEchoScript(t, dir)
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-mgr-errpath",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	if err := mgr.Start(context.Background(), StartRequest{
		ID:      "s4",
		Runtime: rt,
		Options: StartOptions{
			Workdir: dir,
			LogPath: filepath.Join(dir, "session.log"),
		},
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background(), "s4") }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = mgr.JsonRpcCall(ctx, "s4", "boom.method", nil)
	if err == nil {
		t.Fatal("expected JsonRpcError, got nil")
	}
	var rpcErr *JsonRpcError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err type = %T, want *JsonRpcError", err)
	}
	if rpcErr.Code != -32601 {
		t.Errorf("rpcErr.Code = %d, want -32601", rpcErr.Code)
	}
	if !strings.Contains(rpcErr.Message, "boom from boom.method") {
		t.Errorf("rpcErr.Message = %q", rpcErr.Message)
	}
}

func TestManager_JsonRpcCall_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	// Use a JSON-RPC fixture that NEVER responds, so the Call blocks until
	// ctx is cancelled. The "silent" script reads forever without writing
	// a response frame.
	script := writeJsonRpcSilentScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-mgr-ctxcancel",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	if err := mgr.Start(context.Background(), StartRequest{
		ID:      "s5",
		Runtime: rt,
		Options: StartOptions{
			Workdir: dir,
			LogPath: filepath.Join(dir, "session.log"),
		},
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background(), "s5") }()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = mgr.JsonRpcCall(ctx, "s5", "any.method", nil)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.DeadlineExceeded or Canceled", err)
	}
}

func TestManager_JsonRpcCall_ConcurrentSessions(t *testing.T) {
	dir1, dir2 := t.TempDir(), t.TempDir()
	script1 := writeJsonRpcEchoScript(t, dir1)
	script2 := writeJsonRpcEchoScript(t, dir2)

	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	for _, tc := range []struct {
		id, script, dir string
	}{
		{"a", script1, dir1},
		{"b", script2, dir2},
	} {
		rt, err := NewFromAdapter(AdapterRuntimeConfig{
			ID:      "rt-" + tc.id,
			Kind:    "cli",
			Adapter: &minimalAdapter{binary: tc.script},
			Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
		})
		if err != nil {
			t.Fatalf("NewFromAdapter %s: %v", tc.id, err)
		}
		if err := mgr.Start(context.Background(), StartRequest{
			ID:      tc.id,
			Runtime: rt,
			Options: StartOptions{
				Workdir: tc.dir,
				LogPath: filepath.Join(tc.dir, "session.log"),
			},
		}); err != nil {
			t.Fatalf("Manager.Start %s: %v", tc.id, err)
		}
		defer func(id string) { _ = mgr.Stop(context.Background(), id) }(tc.id)
	}

	// Fire one Call per session in parallel; each result echoes the
	// distinct method name. Cross-talk would surface as method-mismatch.
	var wg sync.WaitGroup
	results := make(map[string]string)
	var mu sync.Mutex
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			method := "method-" + id
			res, err := mgr.JsonRpcCall(ctx, id, method, nil)
			if err != nil {
				t.Errorf("JsonRpcCall %s: %v", id, err)
				return
			}
			mu.Lock()
			results[id] = string(res)
			mu.Unlock()
		}(id)
	}
	wg.Wait()

	for id, raw := range results {
		want := "\"method\":\"method-" + id + "\""
		if !strings.Contains(raw, want) {
			t.Errorf("session %s result %s does not contain %q", id, raw, want)
		}
	}
}

func TestManager_JsonRpcCall_AfterStop(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcEchoScript(t, dir)
	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-mgr-afterstop",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	mgr := NewManager(nil)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	if err := mgr.Start(context.Background(), StartRequest{
		ID:      "s6",
		Runtime: rt,
		Options: StartOptions{
			Workdir: dir,
			LogPath: filepath.Join(dir, "session.log"),
		},
	}); err != nil {
		t.Fatalf("Manager.Start: %v", err)
	}

	if err := mgr.Stop(context.Background(), "s6"); err != nil {
		t.Fatalf("Manager.Stop: %v", err)
	}
	// Wait for the watch goroutine to finish unregistering.
	_, _ = mgr.WaitSession(context.Background(), "s6")

	_, err = mgr.JsonRpcCall(context.Background(), "s6", "any.method", nil)
	if !errors.Is(err, ErrSessionNotRunning) {
		t.Errorf("err = %v, want ErrSessionNotRunning", err)
	}
}

// writeNoopStdinScript drops a shell script that reads stdin and exits
// cleanly on EOF — used by the streaming-stdio not-capable tests where we
// only need a long-lived child that won't crash before JsonRpcCall fires.
func writeNoopStdinScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "noop-stdin.sh")
	body := `#!/bin/sh
while IFS= read -r _; do :; done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// writeJsonRpcSilentScript drops a shell script that emits the boot
// notification only — no responses to any request. Used by the
// context-cancel test where Call must block until the caller's ctx fires.
func writeJsonRpcSilentScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "silent-jsonrpc.sh")
	body := `#!/bin/sh
printf '%s\n' '{"jsonrpc":"2.0","method":"server.ready","params":{}}'
while IFS= read -r _; do :; done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}
