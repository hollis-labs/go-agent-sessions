//go:build !windows

package agentsessions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeServeHTTPFakeBinary(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test script needs sh; not running on Windows")
	}
	path := filepath.Join(dir, "fake-opencode-serve.sh")
	body := `#!/bin/sh
printf 'opencode server listening on %s\n' "$TEST_SERVER_URL"
trap 'exit 0' TERM INT
while true; do sleep 1; done
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write fake serve binary: %v", err)
	}
	return path
}

func TestServeHTTPRuntime_HappyPath_BootSendInputStop(t *testing.T) {
	dir := t.TempDir()
	script := writeServeHTTPFakeBinary(t, dir)

	events := make(chan string, 8)
	var gotPrompt string
	var gotDirectory string
	var promptCalls int
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/global/health":
			_, _ = w.Write([]byte(`{"healthy":true,"version":"test"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			gotDirectory = r.URL.Query().Get("directory")
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"id":"ses_http_test"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/event":
			w.Header().Set("content-type", "text/event-stream")
			flusher, ok := w.(http.Flusher)
			if !ok {
				t.Errorf("response writer is not a flusher")
				return
			}
			for {
				select {
				case ev := <-events:
					_, _ = fmt.Fprintf(w, "data: %s\n\n", ev)
					flusher.Flush()
				case <-r.Context().Done():
					return
				}
			}
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_http_test/prompt_async":
			mu.Lock()
			promptCalls++
			mu.Unlock()
			var body struct {
				Parts []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode prompt body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(body.Parts) != 1 {
				t.Errorf("parts len = %d, want 1", len(body.Parts))
			} else {
				gotPrompt = body.Parts[0].Text
			}
			w.WriteHeader(http.StatusNoContent)
			events <- `{"type":"message.part.delta","properties":{"sessionID":"ses_http_test","delta":"hello back"}}`
			events <- `{"type":"session.idle","properties":{"sessionID":"ses_http_test"}}`
		case r.Method == http.MethodPost && r.URL.Path == "/global/dispose":
			_, _ = w.Write([]byte(`true`))
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_http_test/abort":
			_, _ = w.Write([]byte(`true`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "serve-http-happy",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps: Capabilities{
			ServeHTTP:         true,
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
	var observedSessionID string
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Env:     append(os.Environ(), "TEST_SERVER_URL="+server.URL),
		Fanout:  &fanout,
		OnSessionID: func(id string) {
			observedSessionID = id
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if observedSessionID != "ses_http_test" {
		t.Fatalf("OnSessionID = %q, want ses_http_test", observedSessionID)
	}
	if gotDirectory != dir {
		t.Fatalf("session create directory = %q, want %q", gotDirectory, dir)
	}

	if err := sess.SendInput(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("SendInput: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(fanout.String(), "hello back") && sess.Health().State == LiveStateIdle {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if gotPrompt != "hello" {
		t.Errorf("prompt text = %q, want hello", gotPrompt)
	}
	if !strings.Contains(fanout.String(), "hello back") {
		t.Errorf("fanout missing SSE delta: %q", fanout.String())
	}

	sider, ok := sess.(SessionIDer)
	if !ok {
		t.Fatal("session does not implement SessionIDer")
	}
	if sider.ProviderSessionID() != "ses_http_test" {
		t.Errorf("ProviderSessionID = %q, want ses_http_test", sider.ProviderSessionID())
	}
	hint, ok := sess.CheckpointHints()
	if !ok || string(hint) != "ses_http_test" {
		t.Errorf("CheckpointHints = (%q, %v); want (ses_http_test, true)", string(hint), ok)
	}

	if err := sess.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := sess.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if promptCalls != 1 {
		t.Errorf("prompt calls = %d, want 1", promptCalls)
	}
}

func TestServeHTTPRuntime_SendInputRejectsConcurrentTurn(t *testing.T) {
	dir := t.TempDir()
	script := writeServeHTTPFakeBinary(t, dir)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/global/health":
			_, _ = w.Write([]byte(`{"healthy":true,"version":"test"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			_, _ = w.Write([]byte(`{"id":"ses_busy"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/event":
			w.Header().Set("content-type", "text/event-stream")
			<-r.Context().Done()
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_busy/prompt_async":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && (r.URL.Path == "/global/dispose" || r.URL.Path == "/session/ses_busy/abort"):
			_, _ = w.Write([]byte(`true`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rt, err := NewFromAdapter(AdapterRuntimeConfig{
		ID:      "serve-http-busy",
		Kind:    "cli",
		Adapter: &echoAdapter{script: script},
		Caps:    Capabilities{ServeHTTP: true, BinaryRequired: true},
	})
	if err != nil {
		t.Fatalf("NewFromAdapter: %v", err)
	}
	sess, err := rt.Start(context.Background(), StartOptions{
		Workdir: dir,
		LogPath: filepath.Join(dir, "session.log"),
		Env:     append(os.Environ(), "TEST_SERVER_URL="+server.URL),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = sess.Stop(context.Background()) }()

	if err := sess.SendInput(context.Background(), []byte("first")); err != nil {
		t.Fatalf("SendInput first: %v", err)
	}
	if err := sess.SendInput(context.Background(), []byte("second")); err != ErrTurnInFlight {
		t.Fatalf("SendInput second = %v, want ErrTurnInFlight", err)
	}
}

func TestParseServeHTTPListenURL(t *testing.T) {
	got := parseServeHTTPListenURL("opencode server listening on http://127.0.0.1:4096")
	if got != "http://127.0.0.1:4096" {
		t.Fatalf("parse URL = %q", got)
	}
	if got := parseServeHTTPListenURL("INFO no url here"); got != "" {
		t.Fatalf("parse non-url = %q, want empty", got)
	}
}
