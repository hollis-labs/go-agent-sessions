//go:build !windows

package agentsessions

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
	"github.com/hollis-labs/go-sandbox/sandbox"
)

const serveHTTPReadyTimeout = 10 * time.Second

// serveHTTPRuntime is the agentsessions.Runtime backed by a long-lived CLI
// subprocess exposing an HTTP API. The target shape is opencode
// `serve --port 0 --hostname 127.0.0.1`: stdout/stderr carries the bound URL,
// /event carries server-sent events, and session turns are sent through
// /session/{id}/prompt_async.
type serveHTTPRuntime struct {
	cfg AdapterRuntimeConfig
}

func (r *serveHTTPRuntime) ID() string         { return r.cfg.ID }
func (r *serveHTTPRuntime) Kind() string       { return r.cfg.Kind }
func (r *serveHTTPRuntime) Caps() Capabilities { return r.cfg.Caps }

func (r *serveHTTPRuntime) Prepare(_ context.Context) error {
	if !r.cfg.Caps.BinaryRequired {
		return nil
	}
	if _, ok := r.cfg.Adapter.Detect(); !ok {
		return fmt.Errorf("agentsessions: adapter %q binary not found", r.cfg.Adapter.Name())
	}
	return nil
}

func (r *serveHTTPRuntime) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Workdir == "" {
		return nil, errors.New("agentsessions: StartOptions.Workdir is required for serve-http runtime")
	}

	bootDir, planted, sessionAdapter, err := preparePlant(opts, r.cfg.Adapter, r.cfg.ID)
	if err != nil {
		return nil, err
	}
	opts = planted

	logPath, err := resolveServeHTTPLogPath(opts)
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, err
	}
	logF, err := os.Create(logPath) //nolint:gosec // G304: workspace-managed path
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, fmt.Errorf("agentsessions: open log: %w", err)
	}

	s := &serveHTTPSession{
		runtime:       r,
		adapter:       sessionAdapter,
		bootDir:       bootDir,
		opts:          opts,
		logFile:       logF,
		httpClient:    &http.Client{},
		readyURL:      make(chan string, 1),
		processDone:   make(chan error, 1),
		done:          make(chan error, 1),
		stopRequested: make(chan struct{}),
	}
	s.alive.Store(true)
	s.state.Store(int32(LiveStateIdle))
	s.lastSessionID.Store(opts.SessionIDPreset)

	if err := s.spawn(); err != nil {
		_ = logF.Close()
		cleanupBootDir(bootDir)
		return nil, err
	}
	go s.finishOnProcessExit()

	select {
	case base := <-s.readyURL:
		s.baseURL = strings.TrimRight(base, "/")
	case <-s.done:
		_, err := s.Wait()
		if err == nil {
			err = errors.New("process exited before printing listen URL")
		}
		return nil, fmt.Errorf("agentsessions: serve-http start: %w", err)
	case <-time.After(serveHTTPReadyTimeout):
		_ = s.Stop(context.Background())
		return nil, errors.New("agentsessions: serve-http start: timed out waiting for listen URL")
	case <-ctx.Done():
		_ = s.Stop(context.Background())
		return nil, ctx.Err()
	}

	if err := s.waitHealthy(ctx); err != nil {
		_ = s.Stop(context.Background())
		return nil, err
	}

	if opts.SessionIDPreset != "" {
		s.sessionID = opts.SessionIDPreset
	} else if err := s.createSession(ctx); err != nil {
		_ = s.Stop(context.Background())
		return nil, err
	}

	go s.runEventStream()
	go s.finishOnProcessExit()

	if opts.AutoFireFirstTurn && len(opts.FirstTurnPayload) > 0 {
		if err := s.SendInput(ctx, opts.FirstTurnPayload); err != nil {
			_ = s.Stop(ctx)
			return nil, fmt.Errorf("agentsessions: auto-fire first turn: %w", err)
		}
	}

	return s, nil
}

func resolveServeHTTPLogPath(opts StartOptions) (string, error) {
	if opts.LogPath != "" {
		return opts.LogPath, nil
	}
	if opts.WorkspaceDir == "" {
		return "", errors.New("agentsessions: serve-http runtime requires StartOptions.LogPath or StartOptions.WorkspaceDir")
	}
	logPath := filepath.Join(opts.WorkspaceDir, "logs", "session.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return "", fmt.Errorf("agentsessions: ensure log dir: %w", err)
	}
	return logPath, nil
}

type serveHTTPSession struct {
	runtime *serveHTTPRuntime
	adapter provider.CLIAdapter
	bootDir string
	opts    StartOptions

	cmd      *exec.Cmd
	logFile  *os.File
	baseURL  string
	clientMu sync.Mutex

	httpClient *http.Client
	sessionID  string

	state      atomic.Int32
	alive      atomic.Bool
	startedPID atomic.Int32
	lastPID    atomic.Int32

	lastSessionID atomic.Value // string

	readyURL    chan string
	processDone chan error
	done        chan error
	waitOnce    sync.Once
	waitCode    atomic.Int32
	waitErr     atomic.Value // error

	stopOnce      sync.Once
	stopRequested chan struct{}

	turnMu       sync.Mutex
	turnInFlight bool

	streamCancel context.CancelFunc
}

func (s *serveHTTPSession) spawn() error {
	binary, ok := s.adapter.Detect()
	if !ok {
		return fmt.Errorf("agentsessions: adapter %q binary not found", s.adapter.Name())
	}

	args := s.adapter.BuildArgs("", s.opts.BootPrompt, s.opts.SessionIDPreset)
	if len(s.opts.ExtraArgs) > 0 {
		args = append(args, s.opts.ExtraArgs...)
	}

	cmd := exec.Command(binary, args...) //nolint:gosec // G204: adapter-sourced binary + args
	configureCommandProcessGroup(cmd)
	cmd.Dir = s.opts.Workdir
	if len(s.opts.Env) > 0 {
		cmd.Env = s.opts.Env
	} else {
		cmd.Env = os.Environ()
	}
	if len(s.opts.ExtraFiles) > 0 {
		cmd.ExtraFiles = s.opts.ExtraFiles
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("agentsessions: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdout.Close()
		return fmt.Errorf("agentsessions: stderr pipe: %w", err)
	}

	var sandboxCleanup func()
	if s.opts.Profile.ID != "" {
		cleanup, err := sandbox.Apply(cmd, s.opts.Profile, s.opts.Workdir)
		if err != nil {
			return fmt.Errorf("agentsessions: sandbox apply: %w", err)
		}
		sandboxCleanup = cleanup
	}

	limitCleanup, err := applyResourceLimits(cmd, s.opts.ResourceLimits)
	if err != nil {
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return fmt.Errorf("agentsessions: apply resource limits: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return fmt.Errorf("agentsessions: start: %w", err)
	}

	s.cmd = cmd
	if cmd.Process != nil {
		s.startedPID.Store(int32(cmd.Process.Pid))
		s.lastPID.Store(int32(cmd.Process.Pid))
	}

	go s.scanProcessOutput(stdout)
	go s.scanProcessOutput(stderr)
	go func() {
		err := cmd.Wait()
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		s.processDone <- err
		close(s.processDone)
	}()
	return nil
}

func (s *serveHTTPSession) scanProcessOutput(r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		s.writeOutput([]byte(line + "\n"))
		if u := parseServeHTTPListenURL(line); u != "" {
			select {
			case s.readyURL <- u:
			default:
			}
		}
	}
}

func parseServeHTTPListenURL(line string) string {
	idx := strings.Index(line, "http://")
	if idx < 0 {
		idx = strings.Index(line, "https://")
	}
	if idx < 0 {
		return ""
	}
	fields := strings.Fields(line[idx:])
	if len(fields) == 0 {
		return ""
	}
	raw := strings.TrimRight(fields[0], ".,;)")
	if _, err := url.ParseRequestURI(raw); err != nil {
		return ""
	}
	return raw
}

func (s *serveHTTPSession) waitHealthy(ctx context.Context) error {
	deadline := time.Now().Add(serveHTTPReadyTimeout)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/global/health", nil)
		if err != nil {
			return err
		}
		resp, err := s.httpClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("agentsessions: serve-http health: %w", err)
			}
			return errors.New("agentsessions: serve-http health: timeout")
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *serveHTTPSession) createSession(ctx context.Context) error {
	endpoint := s.withWorkdirQuery(s.baseURL + "/session")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte("{}")))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("agentsessions: create serve-http session: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("agentsessions: create serve-http session: status %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return fmt.Errorf("agentsessions: decode serve-http session: %w", err)
	}
	if out.ID == "" {
		return errors.New("agentsessions: create serve-http session: empty id")
	}
	s.sessionID = out.ID
	s.lastSessionID.Store(out.ID)
	if s.opts.OnSessionID != nil {
		s.opts.OnSessionID(out.ID)
	}
	return nil
}

func (s *serveHTTPSession) withWorkdirQuery(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	q.Set("directory", s.opts.Workdir)
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *serveHTTPSession) runEventStream() {
	ctx, cancel := context.WithCancel(context.Background())
	s.clientMu.Lock()
	s.streamCancel = cancel
	s.clientMu.Unlock()
	endpoint := s.withWorkdirQuery(s.baseURL + "/event")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return
	}
	req.Header.Set("accept", "text/event-stream")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var data bytes.Buffer
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if data.Len() > 0 {
				s.handleSSEData(data.Bytes())
				data.Reset()
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			chunk := strings.TrimPrefix(line, "data:")
			chunk = strings.TrimPrefix(chunk, " ")
			data.WriteString(chunk)
		}
	}
}

func (s *serveHTTPSession) handleSSEData(data []byte) {
	line := append(append([]byte(nil), data...), '\n')
	s.writeOutput(line)

	var ev struct {
		Type       string          `json:"type"`
		Directory  string          `json:"directory"`
		Payload    json.RawMessage `json:"payload"`
		Properties struct {
			SessionID string `json:"sessionID"`
			Delta     string `json:"delta"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(data, &ev); err != nil {
		return
	}

	payload := ev.Payload
	if len(payload) > 0 {
		var wrapped struct {
			Type       string `json:"type"`
			Properties struct {
				SessionID string `json:"sessionID"`
				Delta     string `json:"delta"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(payload, &wrapped); err == nil && wrapped.Type != "" {
			ev.Type = wrapped.Type
			ev.Properties = wrapped.Properties
		}
	}

	if ev.Properties.SessionID != "" && ev.Properties.SessionID != s.sessionID {
		return
	}

	switch ev.Type {
	case "session.created":
		if ev.Properties.SessionID != "" {
			s.lastSessionID.Store(ev.Properties.SessionID)
			if s.opts.OnSessionID != nil {
				s.opts.OnSessionID(ev.Properties.SessionID)
			}
			tryEventFanout(s.opts.EventFanout, llmtypes.StreamEvent{Type: llmtypes.EventSessionID, SessionID: ev.Properties.SessionID})
		}
	case "message.part.delta", "session.next.text.delta":
		if ev.Properties.Delta != "" {
			tryEventFanout(s.opts.EventFanout, llmtypes.StreamEvent{Type: llmtypes.EventDelta, Content: ev.Properties.Delta})
		}
	case "session.idle", "session.next.step.ended":
		s.markTurnDone()
		tryEventFanout(s.opts.EventFanout, llmtypes.StreamEvent{Type: llmtypes.EventDone})
	case "session.error", "session.next.step.failed":
		s.markTurnDone()
		tryEventFanout(s.opts.EventFanout, llmtypes.StreamEvent{Type: llmtypes.EventError, Error: string(data)})
	}
}

func (s *serveHTTPSession) writeOutput(p []byte) {
	var sink io.Writer = s.logFile
	if s.opts.Fanout != nil {
		sink = io.MultiWriter(s.logFile, s.opts.Fanout)
	}
	_, _ = sink.Write(p)
}

func (s *serveHTTPSession) finishOnProcessExit() {
	err := <-s.processDone
	s.alive.Store(false)
	s.state.Store(int32(LiveStateStopped))
	s.waitOnce.Do(func() {
		switch {
		case err == nil:
			s.waitCode.Store(0)
		default:
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				s.waitCode.Store(int32(ee.ExitCode()))
			} else {
				s.waitCode.Store(-1)
				s.waitErr.Store(err)
			}
		}
		_ = s.logFile.Close()
		cleanupBootDir(s.bootDir)
		s.done <- err
		close(s.done)
	})
}

func (s *serveHTTPSession) markTurnDone() {
	s.turnMu.Lock()
	s.turnInFlight = false
	s.turnMu.Unlock()
	if s.alive.Load() {
		s.state.Store(int32(LiveStateIdle))
	}
}

func (s *serveHTTPSession) Wait() (int, error) {
	<-s.done
	code := int(s.waitCode.Load())
	var err error
	if v := s.waitErr.Load(); v != nil {
		err, _ = v.(error)
	}
	return code, err
}

func (s *serveHTTPSession) Stop(ctx context.Context) error {
	var killErr error
	s.stopOnce.Do(func() {
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
		close(s.stopRequested)
		s.clientMu.Lock()
		cancel := s.streamCancel
		s.clientMu.Unlock()
		if cancel != nil {
			cancel()
		}

		_ = s.postNoBody(ctx, s.baseURL+"/global/dispose")
		if s.sessionID != "" {
			_ = s.postNoBody(ctx, s.withWorkdirQuery(s.baseURL+"/session/"+url.PathEscape(s.sessionID)+"/abort"))
		}

		cmd := s.cmd
		if cmd == nil || cmd.Process == nil {
			return
		}
		select {
		case <-s.done:
			return
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
		}
		if err := signalProcessGroup(cmd, syscall.SIGTERM); err != nil {
			return
		}
		select {
		case <-s.done:
		case <-time.After(5 * time.Second):
			killErr = signalProcessGroup(cmd, syscall.SIGKILL)
		case <-ctx.Done():
			killErr = signalProcessGroup(cmd, syscall.SIGKILL)
		}
	})
	return killErr
}

func (s *serveHTTPSession) postNoBody(ctx context.Context, endpoint string) error {
	if endpoint == "" || s.baseURL == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

func (s *serveHTTPSession) SendInput(ctx context.Context, data []byte) error {
	if !s.alive.Load() {
		return ErrNoInputChannel
	}
	s.turnMu.Lock()
	if s.turnInFlight {
		s.turnMu.Unlock()
		return ErrTurnInFlight
	}
	s.turnInFlight = true
	s.turnMu.Unlock()

	body, err := json.Marshal(map[string]any{
		"parts": []map[string]any{{
			"type": "text",
			"text": string(data),
		}},
	})
	if err != nil {
		s.markTurnDone()
		return err
	}
	endpoint := s.withWorkdirQuery(s.baseURL + "/session/" + url.PathEscape(s.sessionID) + "/prompt_async")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		s.markTurnDone()
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.markTurnDone()
		return fmt.Errorf("agentsessions: serve-http send input: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.markTurnDone()
		return fmt.Errorf("agentsessions: serve-http send input: status %d: %s", resp.StatusCode, string(respBody))
	}
	s.state.Store(int32(LiveStateProcessing))
	return nil
}

func (s *serveHTTPSession) Resize(_ context.Context, _ uint16, _ uint16) error {
	return nil
}

func (s *serveHTTPSession) Health() HealthStatus {
	pid := 0
	if s.alive.Load() {
		pid = int(s.startedPID.Load())
	}
	return HealthStatus{
		Alive: s.alive.Load(),
		PID:   pid,
		State: LiveState(s.state.Load()),
	}
}

func (s *serveHTTPSession) CheckpointHints() (CheckpointHint, bool) {
	if !s.runtime.cfg.Caps.CheckpointResume {
		return nil, false
	}
	id, _ := s.lastSessionID.Load().(string)
	if id == "" {
		return nil, false
	}
	return CheckpointHint(id), true
}

func (s *serveHTTPSession) LivePID() int {
	if !s.alive.Load() {
		return 0
	}
	return int(s.startedPID.Load())
}

func (s *serveHTTPSession) LastPID() int {
	return int(s.lastPID.Load())
}

func (s *serveHTTPSession) ProviderSessionID() string {
	id, _ := s.lastSessionID.Load().(string)
	return id
}

var (
	_ Runtime     = (*serveHTTPRuntime)(nil)
	_ Session     = (*serveHTTPSession)(nil)
	_ PIDReporter = (*serveHTTPSession)(nil)
)
