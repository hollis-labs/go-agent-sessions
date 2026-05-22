//go:build !windows

package agentsessions

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"
	"github.com/hollis-labs/go-sandbox/sandbox"
)

// jsonRpcStdioRuntime is the agentsessions.Runtime backed by a long-lived
// non-PTY CLI subprocess speaking JSON-RPC 2.0 over stdin/stdout. Selected
// by NewFromAdapter when AdapterRuntimeConfig.Caps.JsonRpcStdio is true.
//
// Target consumer: Codex `app-server` (the daemon shape that backs OpenAI's
// VS Code extension). The runtime layers a JSON-RPC 2.0 client on top of
// raw stdio: id allocator, pending-request map, notification dispatcher.
// Typed requests go through Session.(JsonRpcCaller).Call(method, params);
// SendInput remains as a raw-bytes escape hatch for callers that need to
// inject pre-framed bytes.
//
// Lifecycle (supervisor, idle-kill, restart-on-crash, watchdog, exit-cause
// classification, restart-preserves-session-id) is identical to
// streamingStdioSession — only the framing layer changes.
type jsonRpcStdioRuntime struct {
	cfg AdapterRuntimeConfig
}

func (r *jsonRpcStdioRuntime) ID() string         { return r.cfg.ID }
func (r *jsonRpcStdioRuntime) Kind() string       { return r.cfg.Kind }
func (r *jsonRpcStdioRuntime) Caps() Capabilities { return r.cfg.Caps }

func (r *jsonRpcStdioRuntime) Prepare(_ context.Context) error {
	if !r.cfg.Caps.BinaryRequired {
		return nil
	}
	if _, ok := r.cfg.Adapter.Detect(); !ok {
		return fmt.Errorf("agentsessions: adapter %q binary not found", r.cfg.Adapter.Name())
	}
	return nil
}

func (r *jsonRpcStdioRuntime) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Workdir == "" {
		return nil, errors.New("agentsessions: StartOptions.Workdir is required for jsonrpc-stdio runtime")
	}

	bootDir, planted, sessionAdapter, err := preparePlant(opts, r.cfg.Adapter, r.cfg.ID)
	if err != nil {
		return nil, err
	}
	opts = planted

	logPath, err := resolveJsonRpcStdioLogPath(opts)
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, err
	}
	logF, err := os.Create(logPath) //nolint:gosec // G304: workspace-managed path
	if err != nil {
		cleanupBootDir(bootDir)
		return nil, fmt.Errorf("agentsessions: open log: %w", err)
	}

	s := &jsonRpcStdioSession{
		runtime:       r,
		adapter:       sessionAdapter,
		bootDir:       bootDir,
		opts:          opts,
		logFile:       logF,
		done:          make(chan error, 1),
		copyDone:      make(chan struct{}),
		stopRequested: make(chan struct{}),
		pending:       make(map[int64]chan jsonRpcResponse),
	}
	s.alive.Store(true)
	s.state.Store(int32(LiveStateIdle))
	// Pre-seed lastSessionID from preset so ProviderSessionID() returns
	// the resume id immediately after Start (before any session-id
	// notification lands). Mirrors adapterSession's behavior. Compliance
	// harness CapsProviderSessionID/PresetCarriedBeforeTurn pins this.
	s.lastSessionID.Store(opts.SessionIDPreset)

	cmd, stdin, stdout, attemptCleanup, err := s.spawnAttempt(0)
	if err != nil {
		_ = logF.Close()
		cleanupBootDir(bootDir)
		return nil, err
	}

	if opts.Supervisor == nil {
		s.legacyCleanup = attemptCleanup
		s.spawnReaderLegacy(stdout)
		s.spawnWaiterLegacy(cmd, stdin, stdout)
	} else {
		go s.runSupervised(ctx, cmd, stdin, stdout, attemptCleanup)
	}

	if opts.AutoFireFirstTurn && len(opts.FirstTurnPayload) > 0 {
		if !(opts.BootMode == "stdin" && opts.BootPrompt != "") {
			if err := s.SendInput(ctx, opts.FirstTurnPayload); err != nil {
				_ = s.Stop(ctx)
				return nil, fmt.Errorf("agentsessions: auto-fire first turn: %w", err)
			}
		}
	}

	return s, nil
}

func resolveJsonRpcStdioLogPath(opts StartOptions) (string, error) {
	if opts.LogPath != "" {
		return opts.LogPath, nil
	}
	if opts.WorkspaceDir == "" {
		return "", errors.New("agentsessions: jsonrpc-stdio runtime requires StartOptions.LogPath or StartOptions.WorkspaceDir")
	}
	logPath := filepath.Join(opts.WorkspaceDir, "logs", "session.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return "", fmt.Errorf("agentsessions: ensure log dir: %w", err)
	}
	return logPath, nil
}

// jsonRpcResponse is the internal envelope handed from the reader to a
// pending Call. Exactly one of result / err is set per response frame.
type jsonRpcResponse struct {
	result json.RawMessage
	err    *JsonRpcError
}

// jsonRpcFrame is the minimal shape the reader cares about when
// classifying inbound frames into response-vs-notification.
type jsonRpcFrame struct {
	JsonRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method,omitempty"`
	Params  json.RawMessage  `json:"params,omitempty"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *JsonRpcError    `json:"error,omitempty"`
}

// jsonRpcStdioSession is the long-lived JSON-RPC 2.0 Session implementation.
type jsonRpcStdioSession struct {
	runtime *jsonRpcStdioRuntime
	// adapter is the per-session CLIAdapter — usually a pointer alias to
	// runtime.cfg.Adapter, but a per-session clone when AutoPlantBootDir
	// fired bare-mode injection. Always non-nil after Start.
	adapter provider.CLIAdapter
	// bootDir is the absolute path of the AutoPlantBootDir-planted tempdir,
	// or "" when no plant happened. Cleaned up exactly once at terminal
	// state via cleanupBootDir.
	bootDir string
	opts    StartOptions

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	logFile *os.File

	legacyCleanup func()

	ioLock sync.Mutex

	state      atomic.Int32
	alive      atomic.Bool
	startedPID atomic.Int32
	lastPID    atomic.Int32
	// spawnedAt is the most-recent successful cmd.Start time as unix
	// nanoseconds. Set inside spawnAttempt after cmd.Start; read by the
	// waiter paths to compute elapsed-since-spawn for the abnormal-wait
	// diagnostic. Zero before the first attempt.
	spawnedAt atomic.Int64

	lastSessionID atomic.Value // string

	activity activityTracker

	done     chan error
	waitOnce sync.Once
	waitCode atomic.Int32
	waitErr  atomic.Value // error

	copyDone chan struct{}

	stopOnce      sync.Once
	stopRequested chan struct{}

	// JSON-RPC client state.
	nextID  atomic.Int64
	pendMu  sync.Mutex
	pending map[int64]chan jsonRpcResponse
}

func (s *jsonRpcStdioSession) spawnAttempt(attempt int) (*exec.Cmd, io.WriteCloser, io.ReadCloser, func(), error) {
	binary, ok := s.adapter.Detect()
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: adapter %q binary not found", s.adapter.Name())
	}

	systemPrompt := s.opts.BootPrompt
	if s.opts.BootMode == "stdin" {
		systemPrompt = ""
	}

	sessionIDPreset := s.opts.SessionIDPreset
	if attempt > 0 && s.runtime.cfg.Caps.ProviderSessionID {
		if sid, _ := s.lastSessionID.Load().(string); sid != "" {
			sessionIDPreset = sid
		}
	}
	args := s.adapter.BuildArgs("", systemPrompt, sessionIDPreset)
	if len(s.opts.ExtraArgs) > 0 {
		args = append(args, s.opts.ExtraArgs...)
	}

	cmd := exec.Command(binary, args...) //nolint:gosec // G204
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
	if s.opts.Stderr != nil {
		cmd.Stderr = s.opts.Stderr
	} else {
		cmd.Stderr = s.logFile
	}

	var sandboxCleanup func()
	if s.opts.Profile.ID != "" {
		cleanup, err := sandbox.Apply(cmd, s.opts.Profile, s.opts.Workdir)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("agentsessions: sandbox apply: %w", err)
		}
		sandboxCleanup = cleanup
	}

	limitCleanup, err := applyResourceLimits(cmd, s.opts.ResourceLimits)
	if err != nil {
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: apply resource limits: %w", err)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
		return nil, nil, nil, nil, fmt.Errorf("agentsessions: start: %w", err)
	}

	s.ioLock.Lock()
	s.cmd = cmd
	s.stdin = stdin
	s.stdout = stdout
	s.ioLock.Unlock()

	if cmd.Process != nil {
		if attempt == 0 {
			s.startedPID.Store(int32(cmd.Process.Pid))
		}
		s.lastPID.Store(int32(cmd.Process.Pid))
	}
	s.spawnedAt.Store(time.Now().UnixNano())

	if attempt == 0 && s.opts.BootMode == "stdin" && s.opts.BootPrompt != "" {
		if _, werr := io.WriteString(stdin, s.opts.BootPrompt); werr != nil {
			_ = signalProcessGroup(cmd, syscall.SIGKILL)
			_ = cmd.Wait()
			_ = stdin.Close()
			_ = stdout.Close()
			limitCleanup()
			if sandboxCleanup != nil {
				sandboxCleanup()
			}
			return nil, nil, nil, nil, fmt.Errorf("agentsessions: write boot prompt: %w", werr)
		}
		s.tickActivity()
	}

	cleanup := func() {
		limitCleanup()
		if sandboxCleanup != nil {
			sandboxCleanup()
		}
	}
	return cmd, stdin, stdout, cleanup, nil
}

func (s *jsonRpcStdioSession) spawnReaderLegacy(stdout io.Reader) {
	go func() {
		defer close(s.copyDone)
		s.runReaderLoop(stdout)
	}()
}

// runReaderLoop scans stdout line by line. Each line is parsed as a
// JSON-RPC frame: an `id` field marks a response (routed to the pending
// caller); absence of `id` marks a notification (forwarded to the consumer
// via TypedEventCallback / JsonRpcNotificationHook). Frames that fail to
// parse are written to the log (via Fanout/logFile tee) but otherwise
// ignored — the spec mandates well-formed frames.
func (s *jsonRpcStdioSession) runReaderLoop(stdout io.Reader) {
	var sink io.Writer = s.logFile
	if s.opts.Fanout != nil {
		sink = io.MultiWriter(s.logFile, s.opts.Fanout)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	_, hasParser := s.adapter.(provider.EventParser)

	for scanner.Scan() {
		raw := scanner.Bytes()
		s.tickActivity()
		line := make([]byte, len(raw)+1)
		copy(line, raw)
		line[len(raw)] = '\n'
		_, _ = sink.Write(line)

		var frame jsonRpcFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			// Non-JSON-RPC line — already logged. Skip frame routing.
			continue
		}

		// Adapter-level ParseLine / ParseLineEvents fire on every line
		// (response or notification) to keep parity with the streaming
		// path; adapters that don't want this simply return empty.
		if evs, perr := s.adapter.ParseLine(raw); perr == nil {
			for _, ev := range evs {
				if ev.Type == llmtypes.EventSessionID && ev.SessionID != "" {
					s.lastSessionID.Store(ev.SessionID)
					if s.opts.OnSessionID != nil {
						s.opts.OnSessionID(ev.SessionID)
					}
				}
				tryEventFanout(s.opts.EventFanout, ev)
			}
		}
		if s.opts.TypedEventCallback != nil && hasParser {
			if typed, perr := s.adapter.(provider.EventParser).ParseLineEvents(raw); perr == nil {
				for _, te := range typed {
					s.opts.TypedEventCallback(te)
				}
			}
		}

		// Classify the frame. Per JSON-RPC 2.0 a Response never carries a
		// `method`; a Request and a Notification always do, and a Request
		// additionally carries an `id`. The discriminator MUST key on
		// `method` first: an earlier version keyed purely on `id != nil`,
		// so a server-initiated request (method + id) was misrouted to
		// deliverResponse, found no pending Call, and was dropped — the
		// child then blocked forever waiting for a response it never got
		// (the codex app-server approval-elicitation deadlock).
		switch {
		case frame.Method != "" && frame.ID != nil:
			// Server-initiated request — JSON-RPC 2.0 requires a response.
			s.respondToServerRequest(frame)
		case frame.Method != "":
			// Notification — no response expected.
			if s.opts.JsonRpcNotificationHook != nil {
				s.opts.JsonRpcNotificationHook(frame.Method, frame.Params)
			}
		case frame.ID != nil:
			// Response to one of our outbound Calls.
			s.deliverResponse(*frame.ID, frame.Result, frame.Error)
		}
	}

	// Reader exiting (EOF / pipe close) — fail any still-pending calls so
	// blocked Call goroutines don't leak.
	s.failPendingOnClose(errors.New("agentsessions: jsonrpc reader exited before response"))
}

// deliverResponse routes an inbound response frame to the matching pending
// Call. Decodes the id (which may be a JSON number or string) into the
// internal int64 key; only numeric ids are allocated by Call, so non-numeric
// ids are silently dropped (out-of-band responses we didn't request).
func (s *jsonRpcStdioSession) deliverResponse(idRaw json.RawMessage, result json.RawMessage, errEnv *JsonRpcError) {
	var id int64
	if err := json.Unmarshal(idRaw, &id); err != nil {
		// Non-numeric id (e.g. string) — we never allocated one. Drop.
		return
	}
	s.pendMu.Lock()
	ch, ok := s.pending[id]
	if ok {
		delete(s.pending, id)
	}
	s.pendMu.Unlock()
	if !ok {
		return
	}
	ch <- jsonRpcResponse{result: result, err: errEnv}
	close(ch)
}

// respondToServerRequest answers a server-initiated JSON-RPC request from the
// child (a frame carrying both `method` and `id`). JSON-RPC 2.0 requires a
// response for every request that carries an id; without one the child blocks
// forever — this is the codex app-server tool-approval deadlock.
//
// When StartOptions.JsonRpcRequestHook is set the consumer's (result, error)
// is marshaled into the response. When it is nil the runtime still answers,
// with a method-not-handled error, so the child fails fast instead of hanging.
// The request id is echoed back verbatim (it may be a string or a number).
func (s *jsonRpcStdioSession) respondToServerRequest(frame jsonRpcFrame) {
	resp := map[string]any{"jsonrpc": "2.0", "id": frame.ID}
	if hook := s.opts.JsonRpcRequestHook; hook != nil {
		result, rpcErr := hook(frame.Method, frame.Params)
		if rpcErr != nil {
			resp["error"] = rpcErr
		} else {
			// result may be nil — that marshals to JSON `null`, a valid
			// JSON-RPC result.
			resp["result"] = result
		}
	} else {
		resp["error"] = &JsonRpcError{
			Code:    -32601,
			Message: "agentsessions: no JsonRpcRequestHook configured for server-initiated request " + frame.Method,
		}
	}

	encoded, err := json.Marshal(resp)
	if err != nil {
		return
	}
	s.ioLock.Lock()
	stdin := s.stdin
	if stdin != nil {
		_, _ = stdin.Write(append(encoded, '\n'))
	}
	s.ioLock.Unlock()
	s.tickActivity()
}

// failPendingOnClose drains the pending map at reader-exit time, signalling
// every blocked Call with the supplied error so they unblock cleanly. Called
// from runReaderLoop on EOF, and from the lifecycle teardown paths.
func (s *jsonRpcStdioSession) failPendingOnClose(err error) {
	s.pendMu.Lock()
	pending := s.pending
	s.pending = make(map[int64]chan jsonRpcResponse)
	s.pendMu.Unlock()
	for _, ch := range pending {
		ch <- jsonRpcResponse{err: &JsonRpcError{Code: -32000, Message: err.Error()}}
		close(ch)
	}
}

func (s *jsonRpcStdioSession) spawnWaiterLegacy(cmd *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser) {
	go func() {
		err := cmd.Wait()

		pid := 0
		if cmd.Process != nil {
			pid = cmd.Process.Pid
		}
		elapsed := time.Duration(0)
		if t0 := s.spawnedAt.Load(); t0 > 0 {
			elapsed = time.Since(time.Unix(0, t0))
		}
		logAbnormalWait("jsonrpc-stdio", s.runtime.cfg.ID, pid, elapsed, err)

		s.ioLock.Lock()
		s.stdin = nil
		s.stdout = nil
		s.ioLock.Unlock()

		_ = stdin.Close()
		_ = stdout.Close()
		<-s.copyDone
		_ = s.logFile.Close()
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
			s.done <- err
			close(s.done)
		})
		if s.legacyCleanup != nil {
			s.legacyCleanup()
		}
		cleanupBootDir(s.bootDir)
	}()
}

func (s *jsonRpcStdioSession) runSupervised(ctx context.Context, firstCmd *exec.Cmd, firstStdin io.WriteCloser, firstStdout io.ReadCloser, firstCleanup func()) {
	sup := s.opts.Supervisor
	maxRestarts := sup.RestartOnCrash
	if maxRestarts < 0 {
		maxRestarts = 0
	}
	cmd := firstCmd
	stdin := firstStdin
	stdout := firstStdout
	cleanup := firstCleanup
	var lastExit *ExitError

	defer func() {
		_ = s.logFile.Close()
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
		select {
		case <-s.copyDone:
		default:
			close(s.copyDone)
		}
		s.waitOnce.Do(func() {
			if lastExit == nil {
				s.waitCode.Store(0)
				s.done <- nil
			} else {
				s.waitCode.Store(int32(lastExit.Code))
				s.waitErr.Store(lastExit)
				s.done <- lastExit
			}
			close(s.done)
		})
		cleanupBootDir(s.bootDir)
	}()

	for attempt := 0; attempt <= maxRestarts; attempt++ {
		exit := s.waitOnceSupervised(ctx, cmd, stdin, stdout, attempt)
		cleanup()
		lastExit = exit

		if ctx.Err() != nil || s.isStopRequested() {
			return
		}
		if exit == nil {
			return
		}
		if !restartEligible(exit) {
			return
		}
		if attempt >= maxRestarts {
			lastExit.Cause = CauseRestartExhausted
			return
		}

		backoff := computeRestartBackoff(attempt+1, sup.MaxRestartBackoff)
		select {
		case <-ctx.Done():
			return
		case <-s.stopRequested:
			return
		case <-time.After(backoff):
		}
		if sup.OnRestart != nil {
			sup.OnRestart(attempt+1, exit)
		}
		nextCmd, nextStdin, nextStdout, nextCleanup, err := s.spawnAttempt(attempt + 1)
		if err != nil {
			lastExit = &ExitError{Code: -1, Cause: CauseRestartExhausted, waitErr: err}
			return
		}
		cmd = nextCmd
		stdin = nextStdin
		stdout = nextStdout
		cleanup = nextCleanup
	}
}

func (s *jsonRpcStdioSession) waitOnceSupervised(ctx context.Context, cmd *exec.Cmd, stdin io.WriteCloser, stdout io.ReadCloser, attempt int) *ExitError {
	procDone := make(chan struct{})
	readerDone := make(chan struct{})
	cause := &supState{}
	startedAt := time.Now()
	s.activity.tick()

	go func() {
		defer close(readerDone)
		s.runReaderLoop(stdout)
	}()

	sup := s.opts.Supervisor
	var supWG sync.WaitGroup
	if sup.IdleKill > 0 {
		supWG.Add(1)
		go func() {
			defer supWG.Done()
			s.superviseIdle(cmd, cause, procDone, startedAt)
		}()
	}
	if sup.WatchdogTimeout > 0 {
		supWG.Add(1)
		go func() {
			defer supWG.Done()
			s.superviseWatchdog(cmd, cause, procDone, startedAt)
		}()
	}

	stopWatcherDone := make(chan struct{})
	go func() {
		defer close(stopWatcherDone)
		select {
		case <-procDone:
			return
		case <-s.stopRequested:
			killWithGrace(cmd, 5*time.Second, procDone)
		case <-ctx.Done():
			killWithGrace(cmd, 5*time.Second, procDone)
		}
	}()

	waitErr := cmd.Wait()
	close(procDone)
	supWG.Wait()
	<-stopWatcherDone

	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	elapsed := time.Duration(0)
	if t0 := s.spawnedAt.Load(); t0 > 0 {
		elapsed = time.Since(time.Unix(0, t0))
	}
	logAbnormalWait("jsonrpc-stdio", s.runtime.cfg.ID, pid, elapsed, waitErr)

	s.ioLock.Lock()
	s.stdin = nil
	s.stdout = nil
	s.ioLock.Unlock()
	_ = stdin.Close()
	_ = stdout.Close()
	<-readerDone

	_ = attempt
	return buildExitError(cmd.ProcessState, waitErr, cause.getCause())
}

func (s *jsonRpcStdioSession) isStopRequested() bool {
	select {
	case <-s.stopRequested:
		return true
	default:
		return false
	}
}

func (s *jsonRpcStdioSession) tickActivity() {
	s.activity.tick()
	if sup := s.opts.Supervisor; sup != nil && sup.ActivityCallback != nil {
		sup.ActivityCallback()
	}
}

func (s *jsonRpcStdioSession) superviseIdle(cmd *exec.Cmd, cause *supState, procDone <-chan struct{}, startedAt time.Time) {
	threshold := s.opts.Supervisor.IdleKill
	tick := threshold / 4
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	timer := time.NewTicker(tick)
	defer timer.Stop()
	for {
		select {
		case <-procDone:
			return
		case <-timer.C:
			idle := s.activity.idleSince(startedAt)
			if idle < threshold {
				continue
			}
			if !cause.trySetCause(CauseIdleTimeout) {
				return
			}
			killWithGrace(cmd, 5*time.Second, procDone)
			return
		}
	}
}

func (s *jsonRpcStdioSession) superviseWatchdog(cmd *exec.Cmd, cause *supState, procDone <-chan struct{}, startedAt time.Time) {
	threshold := s.opts.Supervisor.WatchdogTimeout
	tick := threshold / 4
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	timer := time.NewTicker(tick)
	defer timer.Stop()
	for {
		select {
		case <-procDone:
			return
		case <-timer.C:
			idle := s.activity.idleSince(startedAt)
			if idle < threshold {
				continue
			}
			if !cause.trySetCause(CauseWatchdogKill) {
				return
			}
			if cmd.Process != nil {
				_ = signalProcessGroup(cmd, syscall.SIGKILL)
			}
			return
		}
	}
}

func (s *jsonRpcStdioSession) Wait() (int, error) {
	<-s.done
	code := int(s.waitCode.Load())
	var err error
	if v := s.waitErr.Load(); v != nil {
		err, _ = v.(error)
	}
	return code, err
}

func (s *jsonRpcStdioSession) Stop(ctx context.Context) error {
	var killErr error
	s.stopOnce.Do(func() {
		s.alive.Store(false)
		s.state.Store(int32(LiveStateStopped))
		close(s.stopRequested)

		s.ioLock.Lock()
		cmd := s.cmd
		stdin := s.stdin
		s.ioLock.Unlock()

		if stdin != nil {
			_ = stdin.Close()
		}
		// Fail any in-flight Call goroutines so they unblock immediately;
		// the reader's own EOF-failure pass would catch them eventually,
		// but Stop callers expect a prompt return.
		s.failPendingOnClose(errors.New("agentsessions: session stopped"))

		if cmd == nil || cmd.Process == nil {
			return
		}

		const eofGrace = 2 * time.Second
		eofTimer := time.NewTimer(eofGrace)
		defer eofTimer.Stop()
		select {
		case <-s.done:
			return
		case <-eofTimer.C:
		case <-ctx.Done():
		}

		if err := signalProcessGroup(cmd, syscall.SIGTERM); err != nil {
			return
		}
		const grace = 5 * time.Second
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-s.done:
		case <-timer.C:
			if cmd.Process != nil {
				killErr = signalProcessGroup(cmd, syscall.SIGKILL)
			}
		case <-ctx.Done():
			if cmd.Process != nil {
				killErr = signalProcessGroup(cmd, syscall.SIGKILL)
			}
		}
	})
	return killErr
}

// SendInput is the raw-bytes escape hatch — callers that need to inject
// pre-framed JSON-RPC bytes (or non-spec frames) use this. The runtime
// appends a trailing newline if missing. Most consumers should use Call
// instead, which handles request encoding + response correlation.
func (s *jsonRpcStdioSession) SendInput(_ context.Context, data []byte) error {
	if !s.alive.Load() {
		return ErrNoInputChannel
	}
	s.ioLock.Lock()
	defer s.ioLock.Unlock()
	if s.stdin == nil {
		return ErrNoInputChannel
	}
	payload := data
	if len(payload) == 0 || payload[len(payload)-1] != '\n' {
		payload = append(append([]byte(nil), data...), '\n')
	}
	if _, err := s.stdin.Write(payload); err != nil {
		return err
	}
	s.tickActivity()
	return nil
}

// Call sends a JSON-RPC 2.0 request, blocks on the matching response, and
// returns the raw result envelope (or *JsonRpcError when the remote returns
// an error response). Respects ctx.Done() — cancelled callers stop blocking
// and the in-flight pending entry is removed (a late response from the
// child will be dropped).
func (s *jsonRpcStdioSession) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if !s.alive.Load() {
		return nil, ErrNoInputChannel
	}
	id := s.nextID.Add(1)
	respCh := make(chan jsonRpcResponse, 1)
	s.pendMu.Lock()
	s.pending[id] = respCh
	s.pendMu.Unlock()

	cleanup := func() {
		s.pendMu.Lock()
		delete(s.pending, id)
		s.pendMu.Unlock()
	}

	// Encode the request frame. Spec field order doesn't matter, but we
	// keep it canonical for log readability.
	type reqFrame struct {
		JsonRPC string `json:"jsonrpc"`
		ID      int64  `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
	}
	encoded, err := json.Marshal(reqFrame{JsonRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("agentsessions: jsonrpc encode: %w", err)
	}

	s.ioLock.Lock()
	stdin := s.stdin
	s.ioLock.Unlock()
	if stdin == nil {
		cleanup()
		return nil, ErrNoInputChannel
	}

	s.ioLock.Lock()
	_, werr := stdin.Write(append(encoded, '\n'))
	s.ioLock.Unlock()
	if werr != nil {
		cleanup()
		return nil, fmt.Errorf("agentsessions: jsonrpc write: %w", werr)
	}
	s.tickActivity()

	select {
	case resp := <-respCh:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.result, nil
	case <-ctx.Done():
		cleanup()
		return nil, ctx.Err()
	}
}

func (s *jsonRpcStdioSession) Resize(_ context.Context, _ uint16, _ uint16) error {
	return nil
}

func (s *jsonRpcStdioSession) Health() HealthStatus {
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

func (s *jsonRpcStdioSession) CheckpointHints() (CheckpointHint, bool) {
	if !s.runtime.cfg.Caps.CheckpointResume {
		return nil, false
	}
	id, _ := s.lastSessionID.Load().(string)
	if id == "" {
		return nil, false
	}
	return CheckpointHint(id), true
}

func (s *jsonRpcStdioSession) LivePID() int {
	if !s.alive.Load() {
		return 0
	}
	return int(s.startedPID.Load())
}

func (s *jsonRpcStdioSession) LastPID() int { return int(s.lastPID.Load()) }

func (s *jsonRpcStdioSession) ProviderSessionID() string {
	id, _ := s.lastSessionID.Load().(string)
	return id
}

var (
	_ Runtime       = (*jsonRpcStdioRuntime)(nil)
	_ Session       = (*jsonRpcStdioSession)(nil)
	_ PIDReporter   = (*jsonRpcStdioSession)(nil)
	_ JsonRpcCaller = (*jsonRpcStdioSession)(nil)
)
