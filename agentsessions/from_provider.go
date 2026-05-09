package agentsessions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	llmcontracts "github.com/hollis-labs/go-llm-contracts"
	llmtypes "github.com/hollis-labs/go-llm-types"
)

// ProviderRuntimeConfig configures a Runtime backed by a
// go-providers.Provider. Each SendInput appends a user message to the
// rolling conversation and drives a StreamChat call; events are fanned
// out via StartOptions.Fanout. The conversation persists across turns
// inside the Session.
type ProviderRuntimeConfig struct {
	// ID is the runtime's stable identifier. Required.
	ID string

	// Kind is the free-form classification token (e.g. "api", "pty").
	// Required.
	Kind string

	// Provider is the go-providers Provider to drive. Required.
	Provider llmcontracts.Provider

	// Caps declares the static capability set. Default zero value
	// declares no capabilities.
	Caps Capabilities

	// Model is the model identifier passed to ChatRequest.Model. May be
	// overridden per-Session by setting StartOptions.BootMode (see
	// StartConfigure below). Required if non-empty here is preferred —
	// otherwise the Provider falls through to its own default.
	Model string

	// SystemPrompt is the system prompt passed to ChatRequest. Optional.
	// If StartOptions.BootPrompt is non-empty it takes precedence per-
	// Session.
	SystemPrompt string
}

// NewFromProvider constructs a Runtime backed by cfg.Provider. Sessions
// are turn-based: each SendInput sends a user message and streams the
// assistant response. Provider state (model, system prompt) is fixed at
// Runtime construction; conversation state is per-Session.
func NewFromProvider(cfg ProviderRuntimeConfig) (Runtime, error) {
	if cfg.ID == "" {
		return nil, errors.New("agentsessions: ProviderRuntimeConfig.ID is required")
	}
	if cfg.Provider == nil {
		return nil, errors.New("agentsessions: ProviderRuntimeConfig.Provider is required")
	}
	if cfg.Kind == "" {
		cfg.Kind = "api"
	}
	return &providerRuntime{cfg: cfg}, nil
}

type providerRuntime struct {
	cfg ProviderRuntimeConfig
}

func (r *providerRuntime) ID() string         { return r.cfg.ID }
func (r *providerRuntime) Kind() string       { return r.cfg.Kind }
func (r *providerRuntime) Caps() Capabilities { return r.cfg.Caps }

func (r *providerRuntime) Prepare(ctx context.Context) error {
	// Provider-backed runtimes are in-process; nothing to validate at
	// Prepare time. Consumers that want to probe the Provider (key
	// validity, model availability) call provider.Capabilities() or
	// run a no-op Complete themselves.
	return nil
}

func (r *providerRuntime) Start(ctx context.Context, opts StartOptions) (Session, error) {
	system := r.cfg.SystemPrompt
	if opts.BootPrompt != "" {
		system = opts.BootPrompt
	}
	s := &providerSession{
		runtime: r,
		opts:    opts,
		system:  system,
	}
	s.alive.Store(true)
	s.state.Store(int32(LiveStateIdle))
	s.done = make(chan struct{})
	return s, nil
}

// providerSession turns a llmcontracts.Provider into a Session. SendInput
// appends a user message and drives StreamChat; turn ends when the
// stream channel closes.
type providerSession struct {
	runtime *providerRuntime
	opts    StartOptions
	system  string

	state  atomic.Int32 // LiveState
	alive  atomic.Bool
	turnID atomic.Value // string

	mu       sync.Mutex
	messages []llmtypes.ChatMessage

	turnInFlight atomic.Bool

	done     chan struct{}
	doneOnce sync.Once
	exitCode atomic.Int32
}

func (s *providerSession) Wait() (int, error) {
	<-s.done
	return int(s.exitCode.Load()), nil
}

func (s *providerSession) Stop(ctx context.Context) error {
	if !s.alive.CompareAndSwap(true, false) {
		return nil
	}
	s.state.Store(int32(LiveStateStopped))
	s.doneOnce.Do(func() { close(s.done) })
	return nil
}

func (s *providerSession) SendInput(ctx context.Context, data []byte) error {
	if !s.alive.Load() {
		return ErrNoInputChannel
	}
	if !s.turnInFlight.CompareAndSwap(false, true) {
		return ErrTurnInFlight
	}
	defer s.turnInFlight.Store(false)

	s.mu.Lock()
	s.messages = append(s.messages, llmtypes.ChatMessage{Role: "user", Content: string(data)})
	msgs := append([]llmtypes.ChatMessage(nil), s.messages...)
	s.mu.Unlock()

	turnID := defaultIDFn()
	s.turnID.Store(turnID)
	s.state.Store(int32(LiveStateProcessing))
	defer s.state.Store(int32(LiveStateIdle))

	req := llmtypes.ChatRequest{
		Model:        s.runtime.cfg.Model,
		SystemPrompt: s.system,
		Messages:     msgs,
	}
	stream, err := s.runtime.cfg.Provider.StreamChat(ctx, req)
	if err != nil {
		return err
	}

	var assistant string
	for ev := range stream {
		tryEventFanout(s.opts.EventFanout, ev)
		switch ev.Type {
		case llmtypes.EventDelta:
			assistant += ev.Content
			if s.opts.Fanout != nil {
				_, _ = s.opts.Fanout.Write([]byte(ev.Content))
			}
		case llmtypes.EventError:
			if s.opts.Fanout != nil {
				_, _ = s.opts.Fanout.Write([]byte(fmt.Sprintf("\n[error] %s\n", ev.Error)))
			}
			return errors.New(ev.Error)
		case llmtypes.EventDone:
			if s.opts.Fanout != nil {
				_, _ = s.opts.Fanout.Write([]byte("\n[turn_done]\n"))
			}
		}
	}

	if assistant != "" {
		s.mu.Lock()
		s.messages = append(s.messages, llmtypes.ChatMessage{Role: "assistant", Content: assistant})
		s.mu.Unlock()
	}
	return nil
}

func (s *providerSession) Resize(ctx context.Context, rows, cols uint16) error {
	// API-backed sessions have no terminal concept.
	return nil
}

func (s *providerSession) Health() HealthStatus {
	turnID, _ := s.turnID.Load().(string)
	return HealthStatus{
		Alive:  s.alive.Load(),
		PID:    0,
		State:  LiveState(s.state.Load()),
		TurnID: turnID,
	}
}

func (s *providerSession) CheckpointHints() (CheckpointHint, bool) {
	return nil, false
}
