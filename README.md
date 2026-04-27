# go-agent-sessions

`go-agent-sessions` is a small, standalone Go library that turns a
[go-providers](https://github.com/hollis-labs/go-providers) `provider.Provider` or `provider.CLIAdapter` into a long-lived agent **Session** with a uniform control surface — `Wait`, `Stop`, `SendInput`, `Resize`, `Health`, `CheckpointHints` — and a **Manager** that registers Sessions, persists state transitions through caller-supplied sinks, watches for terminal exits, and broadcasts session output to attach subscribers via an in-memory ring buffer.

It is the composition layer above the hollis-labs primitive libs (`go-providers`, `go-runner`, `go-sandbox`) that consolidates the long-running-agent abstraction `agent-mux`, `clockwork`, and `nanite` each grew independently.

## Status

v0 — extracted from agent-mux's `internal/runtime` and `internal/provider` packages (1,800+ LOC + tests, ADRs 0004/0006/0013/0025) and rebased onto the public hollis-labs primitive libs. The compliance harness, the attach broker (drop-oldest ring + drop-on-slow-subscriber), and the per-Session `inputMu` serialization carry over verbatim.

## Install

```bash
go get github.com/hollis-labs/go-agent-sessions
```

## Usage

### Adapter-driven Session (per-turn subprocess)

```go
import (
    "github.com/hollis-labs/go-agent-sessions/agentsessions"
    "github.com/hollis-labs/go-providers/provider"
    "github.com/hollis-labs/go-sandbox/sandbox"
)

rt, err := agentsessions.NewFromAdapter(agentsessions.AdapterRuntimeConfig{
    ID:      "claudestream",
    Kind:    "cli",
    Adapter: claudestream.NewAdapter(...),  // a provider.CLIAdapter
    Caps: agentsessions.Capabilities{
        BinaryRequired:    true,
        ProviderSessionID: true,
        CheckpointResume:  true,
    },
})
if err != nil { /* ... */ }

m := agentsessions.NewManager(myStateSink).
    WithEventSink(myEventSink).
    WithAttachmentSink(myAttachmentSink)

err = m.Start(ctx, agentsessions.StartRequest{
    ID:      "session-abc",
    Runtime: rt,
    Options: agentsessions.StartOptions{
        Workdir:       "/path/to/workspace",
        Profile:       sandbox.Profile{ID: "workspace-only", FS: ..., Net: false},
        AttachEnabled: true,
        OnSessionID:   func(id string) { /* persist for --resume */ },
    },
})
```

Each `m.SendInput("session-abc", payload)` drives a fresh
`runner.Run` invocation: spawn the binary inside the sandbox, parse
stdout through the adapter, fan parsed events to the broker, and
exit when the runner emits the terminal `EventProcessExited`.

### Provider-driven Session (long-lived API/PTY)

```go
rt, err := agentsessions.NewFromProvider(agentsessions.ProviderRuntimeConfig{
    ID:           "anthropic-api",
    Kind:         "api",
    Provider:     anthropic.New(...),  // a provider.Provider
    Model:        "claude-opus-4-7",
    SystemPrompt: "...",
    Caps:         agentsessions.Capabilities{},
})
```

Each `SendInput` appends a user message to the rolling conversation and
drives `Provider.StreamChat`; deltas land on `Fanout`; `EventDone`
finishes the turn. Conversation state lives in the Session.

### Typed event fanout

If a consumer needs parsed `provider.StreamEvent` values directly, set
`StartOptions.EventFanout`. It mirrors typed events alongside the
existing byte `Fanout`; it does not replace it.

```go
events := make(chan provider.StreamEvent, 64) // buffer for your consumer

err = m.Start(ctx, agentsessions.StartRequest{
    ID:      "session-abc",
    Runtime: rt,
    Options: agentsessions.StartOptions{
        Workdir:     "/path/to/workspace",
        Fanout:      rawLogWriter, // optional: raw bytes still flow here
        EventFanout: events,       // optional: parsed events mirror here
    },
})

go func() {
    for ev := range events {
        switch ev.Type {
        case provider.EventDelta:
            render(ev.Content)
        case provider.EventToolUse:
            showTool(ev.ToolUse)
        }
    }
}()
```

Sends are non-blocking. A full `EventFanout` channel drops events
silently, so callers should provide a buffered channel and must not
close it until the session is fully done.

### Compose with go-egress-proxy

```go
import "github.com/hollis-labs/go-egress-proxy/egress"

proxy := egress.New(egress.Config{AllowedDomains: []string{"api.anthropic.com"}})
proxy.Start()
defer proxy.Stop()

env := append(os.Environ()[:0:0], os.Environ()...)
for k, v := range proxy.EnvVars() {
    env = append(env, k+"="+v)
}

m.Start(ctx, agentsessions.StartRequest{
    Runtime: rt,
    Options: agentsessions.StartOptions{
        Workdir: ws,
        Env:     env,                 // proxy env merged in
        Profile: sandbox.Profile{Net: false}, // sandbox blocks direct net
    },
})
```

Combined: the sandboxed child has no direct network access, only the
proxy URL through the merged env vars. The proxy enforces the domain
allowlist + SSRF guard.

## Public API

- **Types** — `Session`, `Runtime`, `StartOptions`, `Capabilities`,
  `HealthStatus`, `LiveState`, `CheckpointHint`, `CheckpointHinter`,
  `SessionIDer`, `State` (4-value process-level enum).
- **Manager** — `NewManager`, `WithAttachmentSink`, `WithEventSink`,
  `Start`, `Stop`, `Get`, `List`, `Health`, `SendInput`, `Resize`,
  `Attach`, `AttachWith`, `WaitSession`, `Shutdown`.
- **Sinks** — `StateSink`, `AttachmentSink`, `EventSink`. Defined as
  interfaces; library ships no implementation.
- **Constructors** — `NewFromAdapter(AdapterRuntimeConfig) (Runtime,
  error)`, `NewFromProvider(ProviderRuntimeConfig) (Runtime, error)`.
- **Errors** — `ErrTurnInFlight`, `ErrNoInputChannel`,
  `ErrSessionNotRunning`, `ErrManagerStopped`, `ErrAttachDisabled`.
- **Compliance** — subpackage `compliance`; consumers run `compliance
  .Run(t, compliance.Harness{Runtime: yourRuntime})` to gate adapter
  conformance.

## What this library is — and isn't

In:

- Long-lived `Session` abstraction over `provider.Provider` /
  `provider.CLIAdapter`, with single-turn-in-flight semantics
  (`ErrTurnInFlight`).
- `Manager` that records launching → running → done|failed transitions
  through a caller-supplied `StateSink`, watches for terminal exits in
  background goroutines, and exposes `Attach` / `AttachWith` for
  multi-subscriber live streaming with byte-offset resume.
- Compliance harness ported verbatim from agent-mux ADR 0025; consumers
  pass their `Runtime` and the harness runs the baseline + capability-
  gated tests.
- Process-level `State` enum: launching, running, done, failed.
  Consumer domain FSMs (clockwork tasks, mux logical agents) map on top.
- Compose with `go-sandbox` (`StartOptions.Profile` is applied by
  `go-runner` under the hood) and `go-egress-proxy` (consumer wires the
  env merge).

Out (intentionally):

- **Persistence implementations.** `StateSink`, `AttachmentSink`, and
  `EventSink` are interfaces; the library ships none. Mux wires
  `*store.Store`; clockwork wires its own SQLite store; nanite wires
  its own. Library does not pick a persistence story.
- **MCP tool surface, HTTP API, TUI.** Consumer concerns. The library
  exposes a Go surface; consumers translate to whatever IPC their app
  uses.
- **Logical-agent identity.** Mux's logical-agent layer (`logical_agents`
  table, ADR 0003) is mux-specific. The library tracks process-level
  sessions; "which logical agent does this belong to" is consumer
  metadata, attached via `StartRequest.SessionMeta`.
- **Multi-process coordination / distributed sessions.** A `Manager` is
  per-process. Cross-process coordination (mux daemon vs CLI vs TUI)
  is the consumer's job — typically a thin RPC over a Unix socket.
- **Hot-reload of capabilities.** `Caps()` is immutable for the
  Runtime's lifetime. Adapters that need version-dependent caps
  construct a fresh Runtime per launch.
- **Provider events on EventSink.** EventSink emits only Manager-owned
  lifecycle events. Per-turn provider events flow through `Fanout`
  and, optionally, `EventFanout`. See
  `docs/decisions/0002-eventsink-shape.md` and
  `docs/decisions/0005-typed-event-fanout.md`.

## Wave-0 design decisions

Resolved during the v0.1.0 implementation; full rationale in
`docs/decisions/`:

| Question | Resolution | Doc |
|---|---|---|
| Does Manager.Start block on first state event? | No — returns once registered. Use Attach / EventSink / Health for first-event semantics. | [0001](docs/decisions/0001-manager-start-non-blocking.md) |
| Should EventSink flatten runner + provider events? | No — EventSink is for Manager lifecycle only. Provider events flow through Fanout. | [0002](docs/decisions/0002-eventsink-shape.md) |
| Default ring + subscriber depth? | 64 KiB / 64 chunks (mux baseline), tunable per-Session via StartOptions. | [0003](docs/decisions/0003-ring-defaults-tunable.md) |
| Does Wait wait for attach subscribers to drain? | No — returns on terminal state. Subscribers see EOF when the broker closes. | [0004](docs/decisions/0004-wait-returns-on-terminal.md) |
| How do consumers receive parsed provider events? | Optional `EventFanout` mirrors typed events alongside byte `Fanout`; sends drop rather than block. | [0005](docs/decisions/0005-typed-event-fanout.md) |

## Repository layout

```
go-agent-sessions/
├── go.mod                            # requires go-providers, go-runner, go-sandbox
├── LICENSE                           # MIT
├── README.md
├── agentsessions/                    # main package
│   ├── doc.go
│   ├── types.go                      # Session, Runtime, StartOptions, etc.
│   ├── manager.go                    # Manager + sinks
│   ├── attach.go                     # ring-buffer attach broker
│   ├── from_adapter.go               # NewFromAdapter (CLIAdapter + runner.Run)
│   ├── from_provider.go              # NewFromProvider (Provider.StreamChat)
│   ├── id.go                         # default attach-id generator
│   ├── attach_test.go
│   ├── manager_test.go
│   ├── from_adapter_test.go          # integration smoke (echo CLI script)
│   └── fake_test.go                  # shared test fakes
├── compliance/                       # behavioral test suite for Runtime impls
│   ├── compliance.go
│   └── compliance_test.go            # smoke run against an adapter Runtime
├── docs/decisions/                   # design decisions resolved at v0.1.0
└── examples/runner_session/main.go   # adapter Session driven through Manager
```

## Lineage

- **Public types** (`Session`, `Runtime`, `StartOptions`, `Capabilities`,
  `HealthStatus`, `LiveState`, `CheckpointHint`, `SessionIDer`,
  `ErrNoInputChannel`) — extracted from `agent-mux/internal/provider/provider.go` (ADR 0006).
- **Manager + watch goroutine** — extracted from `agent-mux/internal/runtime/manager.go` (ADR 0004 attach broker pin).
- **Attach broker** — lifted verbatim from `agent-mux/internal/runtime/attach.go` (ring buffer + drop-oldest, drop-on-slow-subscriber, `subscribeSince` byte-offset resume).
- **Compliance harness** — adapted from `agent-mux/internal/provider/compliance/compliance.go` (ADR 0025).
- **Process-level State enum** — new (4 values: launching, running, done, failed). Mux's mux-domain `session.State` had additional values (Killed, etc.) that belong in mux's logical-agent layer; the library state is process-level only.

## License

MIT — see `LICENSE`.
