# Changelog

All notable changes to `go-agent-sessions` are documented in this file. Per-release notes are also published as GitHub Releases.

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
