# Changelog

All notable changes to `go-agent-sessions` are documented in this file. Per-release notes are also published as GitHub Releases.

## v0.5.0 — 2026-05-08

Tier 3 of the portfolio agent-boot foundation: long-lived PTY runtime, AutoFireFirstTurn hook, two-dir model (Workdir + WorkspaceDir), capability-driven runtime selection, structured PID propagation across turn boundaries, and an attach-broker stability audit.

> **Version note.** Originally drafted as v0.4.0 in the implementer prompt, cut as v0.5.0 because a stale `v0.4.0` tag from 2026-04-27 already pointed at an unrelated commit (`StartOptions.ExtraFiles` passthrough) without an associated release. Skipping `v0.4.0` is cleaner than rewriting tag history.

Authoritative cross-app design: `agent-workspaces/planning/agent-boot-unification/2026-05-07-cross-app-design.md` §9 (PTY analysis, path c) + §10 (lib gaps).

### Public API additions

- **Long-lived PTY runtime (path c).** New `ptySession` runtime alongside the existing `adapterSession`. Selected by `NewFromAdapter` when `AdapterRuntimeConfig.Caps.PTY == true` — single entry point routes to the right implementation. Reuses the manager surface uniformly. Pattern lifted from `agent-mux/internal/provider/cli/claudecode/runtime.go` + `agent-mux/internal/session/runtime.go` (RWMutex against ptmx-close-during-use, wait goroutine that nil-clears the master fd, log + fanout tee), generalized so any `provider.CLIAdapter` whose CLI plays well with a long-lived PTY can opt in.
- **`StartOptions.AutoFireFirstTurn bool` + `StartOptions.FirstTurnPayload []byte`.** When `AutoFireFirstTurn` is true, `Runtime.Start` delivers `FirstTurnPayload` as the first input synchronously before returning — eliminates the Launch/SendInput race that bit consumers (clockwork CW-20260507-0011). Default false preserves existing behavior. PTY runtimes with `BootMode == "stdin"` and a non-empty `BootPrompt` skip the auto-fire (the boot prompt is already in flight via stdin write at Start).
- **`StartOptions.WorkspaceDir string`.** Two-dir model adopted across the agent-boot portfolio: `Workdir` is the spawned process's cwd; `WorkspaceDir` is the lib's persistent state root. The PTY runtime falls back to `<WorkspaceDir>/logs/session.log` for its log file when `LogPath` is empty. Adapter runtime ignores this field. Zero-value preserves existing behavior.
- **`StartOptions.TypedEventCallback provider.EventsCallback`.** Mirrors the existing `EventFanout chan<- provider.StreamEvent` surface but uses go-providers Tier-2 typed events (`events.Delta` / `events.ToolUse` / `events.ToolResult` / `events.SubagentSpawn` / `events.SessionID` / `events.Done` / `events.Error` / `events.Heartbeat` / `events.Thinking`). PTY runtime fires the callback per parsed line via the adapter's `EventParser` interface (or no-op when the adapter doesn't implement it). Adapter runtime continues to fire only legacy `StreamEvent` via `EventFanout` — typed events on the adapter path is a future increment.
- **`PIDReporter` optional interface.** New `LivePID() / LastPID()` distinction. `LivePID` returns the PID of a process *currently* running, or 0 when no process is active (subprocess-per-turn between turns). `LastPID` is the sticky most-recently-started PID for log correlation. PTY runtimes return the long-lived child PID for both. `Health().PID` continues to track `LivePID`-style semantics — the new interface is for callers who need both.

### Capability-driven selection

`NewFromAdapter(cfg)` now switches on `cfg.Caps.PTY`:

- `false` (default) → subprocess-per-turn `adapterRuntime` (existing behavior, unchanged).
- `true` → long-lived PTY `ptyRuntime`.

Existing adapters (`Caps.PTY=false`) see no behavior change. The claude-code adapter is the only adapter that opts into PTY initially per the cross-app design; opencode / codex / gemini / aider / junie / copilot stay subprocess-per-turn until they explicitly opt in.

### Attach broker — public surface frozen

The attach broker behind `Manager.Attach` / `Manager.AttachWith` (in-memory drop-oldest ring + per-subscriber drop-on-slow fanout) is documented as a v0.5.0 stable surface. `AttachOptions.SinceSeq` round-trip semantics — disconnect at byte N, reconnect with `SinceSeq=N`, receive exactly bytes N+1..head with no duplication or gap — are now covered by `TestManager_AttachSinceSeq_RoundTrip_NoGapNoDup`. Drop policy + retention are documented in `attach.go`. No code changes; reform (drop-policy alternatives, larger rings) is separate work.

### Tests

- New `pty_session_test.go` — happy path Start/SendInput/Stop, fanout + EventFanout + TypedEventCallback delivery, concurrent SendInput serialization, SendInput-after-Stop returns `ErrNoInputChannel`, AutoFireFirstTurn delivery (sync), AutoFireFirstTurn no-op when `BootMode=stdin`, `WorkspaceDir` log fallback, missing-log error, capability-driven selection, missing-binary `Prepare`.
- New `from_adapter_test.go` cases — adapter-path AutoFireFirstTurn synchronous delivery; LivePID/LastPID divergence between turns.
- `attach_test.go` adds `TestManager_AttachSinceSeq_RoundTrip_NoGapNoDup` for the public resume contract.
- Race-clean: `go test -race -count=10 -run 'PTY|AdapterRuntime|Attach' ./agentsessions/` flake-free.

### Other

- Bumps `github.com/hollis-labs/go-providers` from `v0.5.0` to `v0.8.0` (Tier 2 dependency: per-line typed events + `EventParser` interface used by ptySession).
- `github.com/creack/pty v1.1.24` is now a direct dependency (was indirect via mux's earlier import lineage).
- README gains "Long-lived PTY runtime", "AutoFireFirstTurn", "Two-dir model", and "Attach broker — since_seq resume" sections.

### Out of scope (deferred)

- `StartOptions.Supervisor` / `StartOptions.ResourceLimits` wiring. The implementer prompt describes them as conditional hooks inside ptySession but they don't compose naturally with `creack/pty.Start` (go-runner's supervision wraps a `runner.Run` call; PTY bypasses that path). The acceptance criteria don't require them. Forward-compat: when consumers need PTY supervision, the design will include a restart-loop wrapper inside ptyRuntime that observes `cmd.Wait` rather than threading through go-runner.
- Typed events on the adapter path. The adapter runtime drives `runner.Run` which doesn't currently surface raw lines through go-providers' `WithEvents` plumbing; typed events on adapter sessions remain a v0.5.0+ increment.
- Per-adapter PTY opt-in beyond claude-code. Other adapters (opencode / codex / gemini / aider / junie / copilot) stay subprocess-per-turn; their Caps.PTY remains false.
- Recovery broker (nanite's D-SUBAGENT-RECOVERY-BROKER) — app-internal first.
- Live attach broker HTTP endpoint — consumer concern (mux already has its own; nanite would build on top of `Manager.Attach`).

### Verification

- darwin host (Darwin 24.6.0):
  ```
  go vet ./...
  go test -race -count=1 -timeout 180s ./...
  go test -race -count=10 -run 'PTY|AdapterRuntime|Attach' ./agentsessions/
  ```
  All green.

- linux cross-compile:
  ```
  GOOS=linux go build ./...
  GOOS=linux go vet ./...
  ```
  ok.

### Origin

Cross-app design: `agent-workspaces/planning/agent-boot-unification/2026-05-07-cross-app-design.md`. Portfolio decision: Vanta `decisions.portfolio.architecture.agent_boot_pattern` rev `01KR2Y2PYBR7FM8VXJBB831WG3`. Nanite consumer decision: `decisions.nanite.architecture.cli_pty_long_lived_default` rev `01KR2Y16TZJC8X88E6P497JBH3`. Tier 1 dep: go-runner v0.3.0. Tier 2 dep: go-providers v0.8.0.

## v0.3.0 — 2026-04-27

Adds `StartOptions.Stderr io.Writer` for caller-controlled stderr capture in adapter-driven sessions. Companion to `go-runner` v0.2.0 (which adds the same field on `runner.Config`); the agent-sessions field forwards directly. Filed in clockwork as `CW-20260427-0056`; consumed by clockwork's wrapper-driven executor (`CW-20260427-0040`) to preserve its per-run stderr sidecar log (`CW-20260417-0024`).

### Public API additions

- `agentsessions.StartOptions.Stderr io.Writer` — when non-nil, forwarded to `runner.Config.Stderr` in the adapter-driven runtime so the spawned subprocess's stderr lands on the caller-supplied writer. Use `io.MultiWriter` to fan out. Nil leaves the runner-level default intact (cmd.Stderr unset → os/exec routes to os.DevNull). No-op for the provider-driven runtime (HTTP transport, no subprocess).

### Other

- README "Usage" section gains a "Stderr passthrough" subsection with the canonical `io.MultiWriter(tailBuf, sidecarFile)` example.
- New tests: `TestStartOptions_Stderr_CapturesToWriter`, `TestStartOptions_Stderr_NilLeavesStderrUnset`.
- New test helper: `writeTestScriptWithStderr` (`from_adapter_test.go`).
- Bumps `github.com/hollis-labs/go-runner` from `v0.1.0` to `v0.2.0`.

### Verification

- darwin host: `go build ./...`, `go vet ./...`, `go test -race -timeout 120s ./...` — pass
- linux cross-compile: `GOOS=linux go build ./...`, `go vet ./...` — ok

### Origin

Clockwork ticket `CW-20260427-0056` under epic `EP-20260427-0001` (clockwork-side adoption of CLI substrate libs + signal-protocol redesign). Bundles with `CW-20260427-0044` (go-runner v0.2.0).

## v0.2.0 — 2026-04-27

Adds optional typed `EventFanout chan<- provider.StreamEvent` to `StartOptions` so consumers can receive parsed stream events directly without re-parsing bytes from the byte `Fanout`. Both `NewFromAdapter` and `NewFromProvider` honor it. Drop-not-block on slow consumers; nil preserves v0.1.0 behavior exactly.

Decision: `docs/decisions/0005-typed-event-fanout.md`.

### Origin

Clockwork ticket `CW-20260427-0045`.

## v0.1.0 — 2026-04-27

Initial release. Extracted from agent-mux's `internal/runtime` and `internal/provider` packages (1,800+ LOC + tests, ADRs 0004/0006/0013/0025) and rebased onto the public hollis-labs primitive libs.

### Public API

- `agentsessions.Session`, `Runtime`, `Manager`, `StartOptions`, `Capabilities`, `HealthStatus`, `LiveState`, `CheckpointHint` / `CheckpointHinter`, `SessionIDer`, `State`.
- `NewManager(StateSink) *Manager` with chainable `.WithEventSink(...)`, `.WithAttachmentSink(...)`.
- `NewFromAdapter(AdapterRuntimeConfig) (Runtime, error)` for `provider.CLIAdapter` runtimes.
- `NewFromProvider(ProviderRuntimeConfig) (Runtime, error)` for `provider.Provider` runtimes (HTTP / API).
- `compliance.Run(t, compliance.Harness{...})` for adapter conformance gating.
