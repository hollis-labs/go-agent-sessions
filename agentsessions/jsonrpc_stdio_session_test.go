//go:build !windows

package agentsessions

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
)

// writeJsonRpcEchoScript drops a shell script that:
//
//  1. Emits one notification at boot ("server.ready").
//  2. For each request line on stdin, emits one delta notification then
//     a response that echoes the id with result={"echoed":true,...}.
//
// Trivially extracts the numeric id via sed — fine for tests where the
// runtime allocates int ids monotonically. Error responses are emitted
// when the test method contains the literal substring "boom".
func writeJsonRpcEchoScript(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "fake-jsonrpc.sh")
	body := `#!/bin/sh
printf '%s\n' '{"jsonrpc":"2.0","method":"server.ready","params":{"port":0}}'
while IFS= read -r line; do
    id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    method=$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
    if [ -n "$id" ]; then
        printf '%s\n' '{"jsonrpc":"2.0","method":"item.delta","params":{"text":"hi"}}'
        case "$method" in
        *boom*)
            printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"boom from %s"}}\n' "$id" "$method"
            ;;
        *)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"echoed":true,"method":"%s"}}\n' "$id" "$method"
            ;;
        esac
    fi
done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// minimalAdapter is a stub provider.CLIAdapter for JSON-RPC tests. It does
// no per-line parsing (the JSON-RPC reader does its own framing); ParseLine
// returns nothing. BuildArgs forwards an empty slice so the test script
// runs with no argv.
type minimalAdapter struct {
	binary string
}

func (a *minimalAdapter) Name() string                                       { return "minimal-jsonrpc" }
func (a *minimalAdapter) BuildArgs(_, _, _ string) []string                  { return []string{} }
func (a *minimalAdapter) ParseLine(_ []byte) ([]llmtypes.StreamEvent, error) { return nil, nil }
func (a *minimalAdapter) Detect() (string, bool)                             { return a.binary, a.binary != "" }

func TestJsonRpcStdioSession_HappyPath_CallStop(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcEchoScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-happy",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	if err := rt.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	caller, ok := sess.(JsonRpcCaller)
	if !ok {
		t.Fatal("session does not implement JsonRpcCaller")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := caller.Call(ctx, "thread.start", map[string]any{"workspace": "."})
	if err != nil {
		t.Fatalf("Call thread.start: %v", err)
	}
	var got struct {
		Echoed bool   `json:"echoed"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("decode result: %v (raw=%s)", err, string(result))
	}
	if !got.Echoed || got.Method != "thread.start" {
		t.Errorf("Call result = %+v, want echoed=true method=thread.start", got)
	}

	// Second call to confirm id allocation increments and routing still
	// works on the same long-lived child.
	result2, err := caller.Call(ctx, "turn.start", nil)
	if err != nil {
		t.Fatalf("Call turn.start: %v", err)
	}
	if !strings.Contains(string(result2), "turn.start") {
		t.Errorf("Call result2 = %s, want method=turn.start echoed", result2)
	}

	if err := sess.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if _, err := sess.Wait(); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

func TestJsonRpcStdioSession_CallReturnsJsonRpcError(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcEchoScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-errpath",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
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
	defer func() { _ = sess.Stop(context.Background()) }()

	caller := sess.(JsonRpcCaller)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = caller.Call(ctx, "boom.method", nil)
	if err == nil {
		t.Fatal("Call returned nil, want JsonRpcError")
	}
	var rpcErr *JsonRpcError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("Call error type = %T, want *JsonRpcError; err=%v", err, err)
	}
	if rpcErr.Code != -32601 {
		t.Errorf("error code = %d, want -32601", rpcErr.Code)
	}
	if !strings.Contains(rpcErr.Message, "boom from boom.method") {
		t.Errorf("error message = %q", rpcErr.Message)
	}
}

func TestJsonRpcStdioSession_NotificationHookFanout(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcEchoScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-notif",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var (
		mu      sync.Mutex
		methods []string
	)
	hook := func(method string, _ json.RawMessage) {
		mu.Lock()
		defer mu.Unlock()
		methods = append(methods, method)
	}

	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:                 dir,
		LogPath:                 filepath.Join(dir, "session.log"),
		JsonRpcNotificationHook: hook,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	caller := sess.(JsonRpcCaller)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := caller.Call(ctx, "ping", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}

	// Wait for both notifications (server.ready at boot, item.delta after
	// the request) to flow through the hook.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(methods)
		mu.Unlock()
		if count >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(methods) < 2 {
		t.Fatalf("notification methods = %v, want at least 2", methods)
	}
	if methods[0] != "server.ready" {
		t.Errorf("first notification = %q, want server.ready", methods[0])
	}
	foundDelta := false
	for _, m := range methods {
		if m == "item.delta" {
			foundDelta = true
			break
		}
	}
	if !foundDelta {
		t.Errorf("did not observe item.delta notification; got %v", methods)
	}
}

func TestJsonRpcStdioSession_ContextCancelUnblocksCall(t *testing.T) {
	dir := t.TempDir()
	// Script that NEVER responds — every line is silently consumed.
	path := filepath.Join(dir, "silent-jsonrpc.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\ncat > /dev/null\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-cancel",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: path},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
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
	defer func() { _ = sess.Stop(context.Background()) }()

	caller := sess.(JsonRpcCaller)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = caller.Call(ctx, "never.responds", nil)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Call err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Call took %v, want unblock within deadline", elapsed)
	}
}

func TestJsonRpcStdioSession_StopUnblocksPendingCall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "silent-jsonrpc.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\ncat > /dev/null\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-stop-unblock",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: path},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
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

	caller := sess.(JsonRpcCaller)
	type callResult struct {
		err error
	}
	done := make(chan callResult, 1)
	go func() {
		_, err := caller.Call(context.Background(), "never.responds", nil)
		done <- callResult{err: err}
	}()

	// Give the call a moment to register its pending entry.
	time.Sleep(80 * time.Millisecond)
	if err := sess.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case res := <-done:
		if res.err == nil {
			t.Error("pending Call returned nil after Stop, want error")
		}
		var rpcErr *JsonRpcError
		if !errors.As(res.err, &rpcErr) {
			t.Errorf("pending Call error type = %T, want *JsonRpcError; err=%v", res.err, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending Call did not unblock after Stop")
	}
}

// writeJsonRpcServerRequestScript drops a shell script that:
//
//  1. Emits ONE server-initiated request at boot — a frame with both a
//     `method` ("tool/approval") and an `id` ("srv-1"). This is the shape
//     codex app-server uses for tool-approval elicitations.
//  2. Echoes whatever response the runtime writes back (matched by the
//     "srv-1" id) as a `server.got_response` notification, so the test can
//     assert the runtime actually answered the request rather than dropping
//     it (the pre-fix deadlock).
func writeJsonRpcServerRequestScript(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "fake-jsonrpc-server-request.sh")
	body := `#!/bin/sh
printf '%s\n' '{"jsonrpc":"2.0","id":"srv-1","method":"tool/approval","params":{"tool":"write_file"}}'
while IFS= read -r line; do
    case "$line" in
    *'"id":"srv-1"'*)
        printf '{"jsonrpc":"2.0","method":"server.got_response","params":%s}\n' "$line"
        ;;
    esac
done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// awaitNotification waits up to 2s for a notification whose method equals want
// and returns its raw params. Fails the test on timeout.
func awaitNotification(t *testing.T, recv func() (string, json.RawMessage, bool), want string) json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if method, params, ok := recv(); ok && method == want {
			return params
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("notification %q not received within deadline", want)
	return nil
}

// TestJsonRpcStdioSession_ServerRequestHook pins that a server-initiated
// request (method + id) is routed to JsonRpcRequestHook and the hook's result
// is sent back to the child as a JSON-RPC response. Pre-fix, the frame was
// misrouted to deliverResponse and dropped, leaving the child blocked.
func TestJsonRpcStdioSession_ServerRequestHook(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcServerRequestScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-server-request",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var (
		mu         sync.Mutex
		hookMethod string
		hookParams string
		notifs     = map[string]json.RawMessage{}
	)
	requestHook := func(method string, params json.RawMessage) (any, *JsonRpcError) {
		mu.Lock()
		hookMethod = method
		hookParams = string(params)
		mu.Unlock()
		return map[string]any{"decision": "approved"}, nil
	}
	notifHook := func(method string, params json.RawMessage) {
		mu.Lock()
		notifs[method] = append(json.RawMessage(nil), params...)
		mu.Unlock()
	}

	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:                 dir,
		LogPath:                 filepath.Join(dir, "session.log"),
		JsonRpcRequestHook:      requestHook,
		JsonRpcNotificationHook: notifHook,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	recv := func() (string, json.RawMessage, bool) {
		mu.Lock()
		defer mu.Unlock()
		for m, p := range notifs {
			if m == "server.got_response" {
				return m, p, true
			}
		}
		return "", nil, false
	}
	params := awaitNotification(t, recv, "server.got_response")

	mu.Lock()
	gotMethod, gotParams := hookMethod, hookParams
	mu.Unlock()
	if gotMethod != "tool/approval" {
		t.Errorf("hook method = %q, want tool/approval", gotMethod)
	}
	if !strings.Contains(gotParams, "write_file") {
		t.Errorf("hook params = %q, want it to contain write_file", gotParams)
	}

	// The echoed response frame must carry our hook result and the verbatim
	// string id.
	resp := string(params)
	if !strings.Contains(resp, `"approved"`) {
		t.Errorf("response echoed by child = %q, want it to contain the hook result \"approved\"", resp)
	}
	if !strings.Contains(resp, `"srv-1"`) {
		t.Errorf("response echoed by child = %q, want it to echo id srv-1", resp)
	}
}

// TestJsonRpcStdioSession_ServerRequestNoHook pins that, with no
// JsonRpcRequestHook configured, a server-initiated request is still answered
// — with a method-not-handled error — so the child fails fast instead of
// deadlocking.
func TestJsonRpcStdioSession_ServerRequestNoHook(t *testing.T) {
	dir := t.TempDir()
	script := writeJsonRpcServerRequestScript(t, dir)

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "jsonrpc-server-request-nohook",
		Kind:    "cli",
		Adapter: &minimalAdapter{binary: script},
		Caps:    Capabilities{JsonRpcStdio: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}

	var (
		mu     sync.Mutex
		notifs = map[string]json.RawMessage{}
	)
	notifHook := func(method string, params json.RawMessage) {
		mu.Lock()
		notifs[method] = append(json.RawMessage(nil), params...)
		mu.Unlock()
	}

	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir:                 dir,
		LogPath:                 filepath.Join(dir, "session.log"),
		JsonRpcNotificationHook: notifHook,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	recv := func() (string, json.RawMessage, bool) {
		mu.Lock()
		defer mu.Unlock()
		for m, p := range notifs {
			if m == "server.got_response" {
				return m, p, true
			}
		}
		return "", nil, false
	}
	params := awaitNotification(t, recv, "server.got_response")

	resp := string(params)
	if !strings.Contains(resp, "-32601") {
		t.Errorf("no-hook response = %q, want a -32601 method-not-handled error", resp)
	}
	if !strings.Contains(resp, `"error"`) {
		t.Errorf("no-hook response = %q, want an error envelope", resp)
	}
}
