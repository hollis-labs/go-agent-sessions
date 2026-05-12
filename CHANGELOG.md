# Changelog

All notable changes to `go-agent-sessions` are documented in this file. Per-release notes are also published as GitHub Releases.

## v0.9.4 — 2026-05-12

Adds `StartOptions.PlantContext` so `AutoPlantBootDir` consumers can pass
caller-owned `provider.PlantContext` fields through to per-provider bootdir
renderers.

### What it does

`preparePlant` now starts from the caller-supplied `PlantContext` and then
overrides the four fields the library owns:

- `PlantContext.SystemPrompt` ← `StartOptions.BootPrompt`
- `PlantContext.BootContent` ← `StartOptions.BootContent`, falling back to
  `StartOptions.BootPrompt`
- `PlantContext.ProjectDir` ← `StartOptions.Workdir`
- `PlantContext.BootDir` ← the absolute planted bootdir path

Caller-owned fields flow through verbatim, including `AgentName`,
`MCPLoopbackURL`, `MuxCommand`, `MuxArgs`, and `MuxEnv`. Future
`provider.PlantContext` fields also flow through automatically as
go-providers renderers begin consuming them.

### Why it exists

Surfaced by clockwork-manifold's adoption sprint (`CW-20260512-0125`).
Clockwork keys its per-task MCP surface on `MCPLoopbackURL`; v0.9.0–v0.9.3
constructed a fresh `PlantContext` inside `preparePlant`, so the loopback
URL and Mux MCP entry fields were dropped during `AutoPlantBootDir`.

### Compatibility

Strictly additive. The zero-value `PlantContext` preserves v0.9.0–v0.9.3
behavior exactly, and callers that use only `BootPrompt` / `BootContent`
continue to receive the same rendered context. Mux's current callers do not
need changes.

Regression-guarded by
`TestPreparePlant_PlantContextOverlay_FlowsThrough_To_Renderer`,
`TestPreparePlant_PlantContextOverlay_LibFieldsOverridden`, and
`TestPreparePlant_PlantContextOverlay_Empty_NoChange`.

## v0.9.3 — 2026-05-12

Adds `StartOptions.BootContent` so consumers can pass distinct values for
the persona system prompt (rendered into CLAUDE.md / AGENTS.md /
agents/<name>.md) and the per-task kickoff body (rendered into boot.md
and referenced from the system prompt via `@./boot.md`).

### What it does

`AutoPlantBootDir` now plumbs two distinct fields into the per-provider
`PlantContext`:

- `PlantContext.SystemPrompt` ← `StartOptions.BootPrompt` (unchanged)
- `PlantContext.BootContent`  ← `StartOptions.BootContent`, falling back
  to `StartOptions.BootPrompt` when `BootContent` is empty.

Per-provider renderers in go-providers (`bootdir_claude.go`,
`bootdir_codex.go`, `bootdir_opencode.go`) already consume
`ctx.SystemPrompt` and `ctx.BootContent` as separate inputs; v0.9.0–v0.9.2
fed both fields the same `BootPrompt` value, which forced consumers to
either conflate the two (Mux pattern) or skip `AutoPlantBootDir`
adoption entirely (clockwork's blocker).

### Why it exists

Surfaced by clockwork-manifold's `go-agent-sessions v0.9.x` adoption
sprint. Clockwork's per-task agent boot pattern distinguishes:

- **Agent identity** (`composeSystemPrompt` — persona + inherited
  project context, durable, planted into CLAUDE.md and auto-reloaded
  post-compaction).
- **Task scope** (`kickoffMarkdown` — per-run instructions, planted into
  boot.md and re-anchored post-compaction via the `@./boot.md`
  reference from the system prompt).

Conflating the two would either bloat CLAUDE.md with stale per-task
text after compaction or strip the persona from the agent's first turn.
Mux's single-prompt pipeline doesn't need the split; clockwork's does.

### Compatibility

Strictly additive. v0.9.0–v0.9.2 callers that set only `BootPrompt`
keep their existing behavior verbatim (the empty-`BootContent`
fallback). Mux's `LaunchSession` site at `internal/app/service.go`
needs no changes; clockwork-manifold's adoption sprint sets both fields
explicitly.

Regression-guarded by `TestPreparePlant_BootContent_Distinct_FromBootPrompt`
and `TestPreparePlant_BootContent_Empty_FallsBack_To_BootPrompt`.

## v0.9.2 — 2026-05-11

Diagnostic-only patch: lib-side logging when a long-lived runtime's
`cmd.Wait()` return signature looks suspicious. No behavior change for
healthy sessions; no API surface added.

### What it logs

A single `log.Printf` line is emitted on the failure path of each
long-lived runtime's waiter (legacy + supervised) when either:

- `cmd.Wait()` returns a non-`*exec.ExitError` (indicating an OS-level
  wait failure rather than a clean child-exit), OR
- The elapsed time between spawn and `cmd.Wait` return is < 1 second
  (the "instant exit" symptom).

Log shape (stable, grep-friendly):

```
agentsessions: <kind> waiter abnormal: session=<runtime-id> pid=<pid> elapsed=<dur> err_type=<go-type> err=<quoted-msg>
```

Where `<kind>` is one of `pty` / `streaming-stdio` / `jsonrpc-stdio`.

Healthy long-lived sessions (clean exit, elapsed >= 1s) produce **no
log output**. The diagnostic is failure-only by design — operators
running production daemons get high-signal stderr lines for the bug
class without noise during normal operation.

### Why it exists

Mux reported a streaming-stdio session transitioning launching → running
→ failed with `exit_code=-1` within ~10ms of launch, while the spawned
agent process remained alive (verified via `ps` + parent-PID check).
`exit_code=-1` comes from the legacy waiter's `default` branch (non-
`*exec.ExitError` from `cmd.Wait`) but the actual `waitErr` was stored
into `s.waitErr atomic.Value` without any logging — invisible to
operators not capturing `Manager.WaitSession`'s return.

The diagnostic surfaces the actual error string and timing so the
underlying cause (likely a wrapper script that forks instead of execs
the agent, leaving an orphan; or sandbox/resource-limit wrapping that
detaches from the lib's child view) becomes diagnosable from stderr
alone.

### Implementation

- New file `agentsessions/diagnostics.go`: `logAbnormalWait(kind,
  runtimeID, pid, elapsed, err)` helper. Centralizes the gating
  logic so all three long-lived runtimes log with identical shape.
- New `spawnedAt atomic.Int64` field on `ptySession`,
  `streamingStdioSession`, `jsonRpcStdioSession` — set in
  `spawnAttempt` right after the process starts.
- Each runtime's legacy waiter and supervised `waitOnceSupervised`
  call `logAbnormalWait(...)` after `cmd.Wait()` returns. Wired in
  six total spots (3 runtimes × 2 paths).

### Tests

- `TestLogAbnormalWait` — covers all six gating cases: clean-exit/
  healthy-duration (no log), `*exec.ExitError`/healthy (no log),
  non-exit-error/healthy (log), clean-exit/rapid (log), non-exit-
  error/rapid (log, single line), zero-elapsed (no log — `spawnedAt`
  was never set).

`go test -race -count=1 -timeout 240s ./...` — green.

### Reference

- Surfaced by: Mux's "session reported failed, exit_code=-1 within
  ~10ms, but spawned process alive afterward" report on
  `streamingStdioSession`.
- This is a diagnostic patch, not a fix. The actual root cause
  surfaces in the new log lines once Mux deploys against v0.9.2. The
  follow-up fix (likely a wrapper-script `exec` vs `fork` correction
  in Mux's catalog binary path, OR a lib-level guard against orphan-
  spawning binaries) lands in v0.9.3 or v0.10.0 once diagnosed.
- Companion: `decisions_go_agent_sessions_v091_providersessionid_preseed`
  (v0.9.1, unrelated bug fix on the same release line).

## v0.9.1 — 2026-05-11

Bug fix: `ProviderSessionID()` now returns `StartOptions.SessionIDPreset`
between `Start` and the first agent-observed session id, across all three
long-lived runtime kinds (`pty` / `streamingStdio` / `jsonRpcStdio`).

### What was broken

The three long-lived sessions stored the most-recent agent-observed
session id in `s.lastSessionID atomic.Value` and never seeded it from
`StartOptions.SessionIDPreset` at `Start`. `ProviderSessionID()` returned
the empty string until the first session-id event landed — typically
mid-first-turn — which broke any caller that needed the preset to be
visible immediately (e.g. compliance-harness assertions, dispatch flows
that read `ProviderSessionID()` between `Start` and the first turn,
multi-consumer setups where one consumer needs to know the resume id
before another consumer drives a turn).

`adapterSession` (subprocess-per-turn) was already correct
(`s.sessionID.Store(opts.SessionIDPreset)` at Start); the three
long-lived kinds were latent buggy. Mux's flip of the `claude-stream`
path to `Capabilities.ProviderSessionID = true` exposed it (reported by
nanite).

### What the fix does

Each long-lived runtime's `Start()` now stores
`opts.SessionIDPreset` into `s.lastSessionID` immediately after the
struct is constructed and before any spawn. Storing the empty string
when no preset was passed is a no-op semantically (the type-assert in
`ProviderSessionID()` returns `""` for both unset and empty-string
states).

Restart preservation is unchanged: `spawnAttempt` continues to read
`lastSessionID` for `attempt > 0` and falls back to the preset when
empty; pre-seeding makes the fallback unconditional but functionally
equivalent (the previous code's else-branch already used the preset).

### Compliance harness assertion

`compliance.CapsProviderSessionID/PresetCarriedBeforeTurn` pins this
invariant — it asserts `ProviderSessionID() == "ses_compliance_preset"`
immediately after `Start` when `SessionIDPreset` is set. Consumers
running the compliance harness against their integrated runtime will
now see this case pass on all four runtime kinds.

### Tests

- `TestProviderSessionID_PresetCarriedBeforeTurn` — new lib-level
  regression guard with four subtests (`streamingStdio` /
  `jsonRpcStdio` / `pty` / `adapter`). Pre-fix: three subtests fail
  with `ProviderSessionID() = "" before any turn`. Post-fix: all four
  green.

`go test -race -count=1 -timeout 240s ./...` — green.

### Reference

- Issue surfaced by nanite during compliance integration after Mux
  flipped claude-stream's Caps.ProviderSessionID to true.
- Companion: `decisions_go_agent_sessions_v009_bootdir_and_jsonrpc`
  (v0.9.0 decisions, unchanged).
- Tag: `v0.9.1`.

## v0.9.0 — 2026-05-11

Two additive lib improvements: BootDirSpec planting absorbed into the
runtime, and a Manager-mediated JSON-RPC passthrough. Both are surfaced
from consumer-side gaps that emerged after v0.8.0 — the per-app planter
duplication across mux/nanite/clockwork, and the inability for Manager-
mediated consumers to reach the per-session `JsonRpcCaller`.

### Part 1 — BootDirSpec absorption

Opt-in via `StartOptions.AutoPlantBootDir bool`. When true AND the
adapter implements `provider.BootDirProvider`, all four runtime kinds
(`pty`, `streamingStdio`, `jsonRpcStdio`, adapter) now:

- Materialize `BootDirSpec.PlantedFiles` into a per-session tempdir
  under `BootDirRoot` / `WorkspaceDir+"/boot/"` / `os.TempDir()`.
- Render each file with `PlantContext{SystemPrompt, BootContent,
  ProjectDir, BootDir}`; write at the spec-appropriate mode (0o600
  for `.mcp.json` / `settings.json`, 0o644 default).
- Substitute `{{.BootDir}}` / `{{.ProjectDir}}` in `EnvAmendments`
  (appended to `StartOptions.Env`) and `ProjectDirArg` (appended via
  the new `StartOptions.ExtraArgs`).
- Set the spawn cwd to `BootDirSpec.SpawnWorkdir(bootDir, projectDir)`.
- For Claude bare-mode adapters, apply `BareInjectionPaths` to a
  per-session clone of the adapter (preserves runtime-level adapter
  purity for concurrent sessions; suppresses the `ExtraArgs` splice
  to avoid double-adding `--add-dir` since bare `BuildArgs` already
  emits it).
- Fire the optional `StartOptions.OnBootDirPlanted(path)` callback once
  on successful plant.
- Call `os.RemoveAll(bootDir)` exactly once at terminal state — covers
  every exit path the runtime classifies (Stop, clean exit, idle-kill,
  watchdog-kill, restart-exhausted, ctx-cancel).

Default `AutoPlantBootDir=false` preserves v0.8.0 behavior exactly —
no filesystem activity, no `StartOptions` mutation. Default-on flip is
planned for v0.10.0 once mux / nanite / clockwork have all migrated.

New `StartOptions` fields:

- `AutoPlantBootDir bool` — feature flag.
- `BootDirRoot string` — tempdir parent (override default).
- `OnBootDirPlanted func(path string)` — debug-friendly success hook.
- `ExtraArgs []string` — generic argv splice (also usable directly by
  consumers that don't need plant but want per-session argv injection
  without wrapping the adapter; templates are not substituted on this
  path — pre-resolve before passing).

When the flag is on for an adapter that doesn't implement
`BootDirProvider`, the runtime no-ops cleanly (no error) so generic
consumers can leave the flag on across heterogeneous adapter fleets.

### Part 2 — `Manager.JsonRpcCall` passthrough

New method on `Manager`:

```go
func (m *Manager) JsonRpcCall(ctx context.Context, id string, method string, params any) (json.RawMessage, error)
```

Preserves the existing invariant that raw `Session` references are not
exposed by Manager. Internally looks the session up, releases the
Manager-wide lock, type-asserts to `JsonRpcCaller`, and forwards
`Call`. New typed error `ErrSessionNotJsonRpcCapable` is returned when
the session exists but its runtime kind is not JSON-RPC capable
(PTY / streaming-stdio / adapter). All other errors pass through
unchanged: `ErrSessionNotRunning` for unknown id, `*JsonRpcError`
(`errors.As`-extractable) for protocol errors, `ctx.Err()` for caller
cancellation.

Unconditional in v0.9.0 — no feature flag, additive only. Consumers
that don't use `jsonRpcStdio` sessions are unaffected.

### Out of scope

- Default-on `AutoPlantBootDir` — sequenced as v0.10.0 after consumer
  migration.
- Adapter contract changes — `BootDirSpec` in go-providers is consumed
  verbatim; no API additions there.
- Per-app planter deletion — consumer-side, each app migrates on its
  own timeline.
- Typed `events.BootDirPlanted` event — substituted by the simpler
  `OnBootDirPlanted` callback on `StartOptions`; the existing
  `TypedEventCallback` surface is untouched.

### Tests

- `TestPreparePlant_*` (12 helper-level): flag-off, missing
  `BootDirProvider`, empty `PlantedFiles`, files written with correct
  content + mode, env amendments resolved, project-dir-arg threading
  + empty-project skip, spawn cwd (`CwdBootDir` / `CwdProjectDir`),
  render-failure cleanup, `OnBootDirPlanted` firing, bare-mode
  injection (clone identity + path threading + no double-add).
- `TestAutoPlantBootDir_Runtime_*` (5 integration): per-runtime plant
  + cleanup against a shell-script fixture; verifies files exist
  mid-session, cleanup on Stop, cleanup on clean exit, child process
  sees planted env, flag-off path has no filesystem activity.
- `TestResolveBootDirRoot_*` (3), `TestSanitizeBootDirID` (1).
- `TestManager_JsonRpcCall_*` (8): happy path, session-not-found,
  not-capable across three runtime kinds (fake / streaming-stdio /
  adapter), `*JsonRpcError` pass-through, ctx-cancel against a silent
  fixture, concurrent sessions (cross-talk check), after-stop returns
  `ErrSessionNotRunning`.

`go test -race -count=1 -timeout 240s ./...` — green.

### Reference

- ADR: `docs/adr/0001-bootdirspec-absorption-and-manager-jsonrpc-passthrough.md`.
- Proposal that surfaced both items: `agent-workspaces/execution/agent-mux/v005-05-long-lived-integration/2026-05-11/bootdirspec-absorption-proposal.md`.
- Portfolio rationale (the 4-mode lifecycle axis that v0.8.0 closed):
  `agent-workspaces/knowledge/portfolio/cli-agent-long-lived-modes.md`.

## v0.8.0 — 2026-05-11

Two new long-lived runtime kinds for headless agent sessions that aren't
PTY-driven. Closes the portfolio gap between "subprocess-per-turn"
(`adapterRuntime`) and "long-lived PTY + TUI" (`ptySession`): vendor-
documented headless modes that retain in-process conversation state across
turns but speak a programmatic protocol over stdin/stdout.

### New runtime kinds

- **`streamingStdioSession`** — long-lived child speaking NDJSON over
  stdin/stdout. Target consumer: Claude `claude -p --input-format
  stream-json --output-format stream-json --verbose` ("Streaming Input
  Mode" per Anthropic's Agent SDK docs). Empirically retains kv-cache
  across turns in the same PID.
- **`jsonRpcStdioSession`** — long-lived child speaking JSON-RPC 2.0 over
  stdin/stdout. Target consumer: Codex `app-server` (the same engine that
  backs OpenAI's VS Code extension). The runtime layers a JSON-RPC client
  (id allocator, pending-request map, notification dispatcher) on top of
  raw stdio.

Both reuse the existing supervisor / idle-kill / restart-on-crash /
watchdog / exit-cause classification primitives. Only the I/O loop is
new. `ptySession` and `adapterRuntime` are unchanged.

### Capability flags + selection

`Capabilities` gains two booleans:

- `StreamingStdio bool` — selects `streamingStdioRuntime`.
- `JsonRpcStdio bool` — selects `jsonRpcStdioRuntime`.

`NewFromAdapter` enforces mutual exclusion: at most one of `PTY` /
`StreamingStdio` / `JsonRpcStdio` may be true; a config that sets more
than one returns an error. When none are set, the existing subprocess-
per-turn adapter runtime is selected (no behavior change for current
callers).

### JSON-RPC client surface

- `JsonRpcCaller` interface: `Call(ctx, method, params) (json.RawMessage, error)`.
  Sessions produced by `jsonRpcStdioRuntime` implement it; callers type-
  assert as they do for `SessionIDer` / `PIDReporter`.
- `JsonRpcError` (typed) — JSON-RPC 2.0 error envelope (`code` /
  `message` / `data`). Returned by `Call` when the remote returns an
  error response; extract via `errors.As(err, &rpcErr)`.
- `StartOptions.JsonRpcNotificationHook func(method string, params json.RawMessage)` —
  consumer hook for inbound notifications (frames with no `id`). The
  runtime does no adapter-specific translation. Consumers that want
  typed events implement `provider.EventParser` on the adapter (same
  per-line path the streaming runtime uses) or decode raw notifications
  inside the hook.
- `SendInput` remains as a raw-bytes escape hatch for both new runtimes;
  the streaming runtime appends a trailing `'\n'` if absent, matching
  the PTY runtime's convention.

### Lifecycle parity with PTY

- Supervisor (`SupervisorOptions`) — idle-kill / restart-on-crash /
  watchdog / `OnRestart` / `ActivityCallback` all apply to both new
  runtimes via the same goroutine shape as `ptySession`.
- Restart preserves the provider-side agent session id when
  `Caps.ProviderSessionID = true` (the most-recently observed id is fed
  into the next spawn's `BuildArgs` in place of the original
  `SessionIDPreset`).
- `Stop` closes stdin first to let well-behaved agents exit cleanly on
  EOF (a 2-second grace), then escalates to SIGTERM (5-second grace) and
  finally SIGKILL — friendlier than PTY's immediate SIGTERM since stdio
  children typically honor EOF.
- Exit classification (`*ExitError` with `Cause` = `idle_timeout` /
  `watchdog_kill` / `restart_exhausted` / etc.) is identical to the PTY
  runtime.
- `Resize` is a no-op on both new runtimes (no PTY to resize).

### Out of scope

- Removing or changing `ptySession` / `adapterRuntime` — both preserved
  unchanged.
- Codex `app-server` argv emission lives in `go-providers` (parallel
  v0.17 sprint); this library only carries the runtime that consumes it.
- Unix-socket / websocket transports for JSON-RPC — stdio only this
  release. The Codex app-server docs mark unix/ws as supported but ws as
  experimental; stdio is the stable surface.
- Typed `ThreadStart` / `TurnStart` / `TurnInterrupt` / `TurnSteer`
  wrappers around `Call` — left to consumer-level Codex helpers; the
  generic `Call` surface is the spec-compliant baseline.
- Mux / Nanite / Clockwork integration — lands as drop-in upgrades in
  follow-up sprints after this release.

### Tests

New race-clean tests against shell-script subprocess fixtures:

- `TestStreamingStdioSession_HappyPath_BootSendInputStop` — session id
  surface, multi-turn fanout, `CheckpointHints` honor cap.
- `TestStreamingStdioSession_MultiTurnSameProcess` — two `SendInput`
  calls serviced by one PID (long-lived contract).
- `TestStreamingStdioSession_AutoFireFirstTurn` — kickoff payload
  delivered synchronously on `Start`.
- `TestStreamingStdioSession_StopClosesStdinAndExits` — clean stdin-EOF
  exit (code 0); `SendInput` after Stop returns `ErrNoInputChannel`.
- `TestStreamingStdioSession_RequiresWorkdir` — construction guard.
- `TestJsonRpcStdioSession_HappyPath_CallStop` — `Call` request/response
  correlation, id allocation increments on the same long-lived child.
- `TestJsonRpcStdioSession_CallReturnsJsonRpcError` — `*JsonRpcError`
  extractable via `errors.As`.
- `TestJsonRpcStdioSession_NotificationHookFanout` — boot-time and
  per-request notifications forwarded to consumer hook.
- `TestJsonRpcStdioSession_ContextCancelUnblocksCall` /
  `..._StopUnblocksPendingCall` — blocked `Call` unblocks promptly on
  ctx cancel or session Stop; pending map drains cleanly without
  goroutine leaks.
- `TestNewFromAdapter_RejectsMultipleLifecycleFlags` — mutual exclusion
  enforced across all flag pairs and the all-three case.
- `TestNewFromAdapter_SingleLifecycleFlagAccepted` — each single
  lifecycle flag routes to its dedicated runtime.

`go test -race -count=1 -timeout 240s ./...` — green.

### Reference

Portfolio rationale for the 4-mode lifecycle axis (oneShot / streamingStdio /
jsonRpcStdio / httpServer / pty) lives in the agent-workspaces knowledge
base. The companion `go-providers` v0.17 sprint emits the argv these
runtimes consume; this library carries the runtime-kind selection and
per-shape I/O loops.

## v0.7.2 — 2026-05-10

Public-release prep. No public Go API changes vs v0.7.1.

### Documentation

- README, CHANGELOG, and in-source godoc cleaned up for OSS publication:
  internal-tooling references (workspace paths, ticket IDs, internal
  decision IDs) dropped; mentions of downstream consumers (`agent-mux`,
  `clockwork`, `nanite`) retained as illustrative integration examples.
- Added `.gitignore` covering Go build artifacts, editor noise, local
  env files, and internal-agent tooling files (`.claude/`, `.agentrc/`,
  `.nanite/`, `CLAUDE.md`, `AGENTS.md`, `AUDIT_RESULTS.md`).

### Module hygiene

- `go.mod`: `github.com/hollis-labs/go-llm-types` and
  `github.com/hollis-labs/go-llm-contracts` promoted from `// indirect`
  to direct requires (the `examples/runner_session/` example imports
  `llmtypes` directly).
- `go mod tidy`, `go vet ./...`, `GOOS=linux go vet ./...`,
  `gofmt -l .` all clean.
- `go test -race -count=1 -timeout 240s ./...` — green.
- `govulncheck ./...` — no vulnerabilities affecting library code.
- `examples/runner_session/main.go` builds and runs end-to-end against
  the manager surface.

## v0.7.1 — 2026-05-09

`go-providers` v0.12.0 compatibility migration. Consumer-side patch
release — no public-API breakage at the `agentsessions` boundary;
internal imports moved to `go-llm-types` / `go-llm-contracts`.

### Dependency bumps

- `github.com/hollis-labs/go-providers`: v0.8.0 → v0.12.0
- `github.com/hollis-labs/go-runner`: v0.3.0 → v0.4.0
- Added `github.com/hollis-labs/go-llm-types` v0.1.0
- Added `github.com/hollis-labs/go-llm-contracts` v0.1.0

### Internal migration (no consumer-facing API changes)

`go-providers` v0.12.0 dropped the transitional aliases for the LLM
type model. References across `agentsessions/`, `compliance/`, and
`examples/` migrated to canonical homes:

- `provider.StreamEvent`, `ChatMessage`, `ChatRequest`, `Usage`,
  `ProviderCapabilities`, and the `Event*` constants
  (`EventDelta`, `EventDone`, `EventError`, `EventToolUse`,
  `EventUsage`, `EventSessionID`) → `llmtypes.X`.
- `provider.Provider` (the long-lived provider interface) →
  `llmcontracts.Provider`.
- `provider.CLIAdapter`, `provider.EventParser`, and
  `provider.EventsCallback` continue to live in `go-providers`
  (CLI/PTY/subprocess surface) and are unchanged.

### Public API surface

The `Session` interface, `StartOptions`, `Manager`, `NewFromAdapter`,
and `NewFromProvider` signatures are unchanged at the
`agentsessions` package level. Consumers passing
`go-providers`-constructed values get the migration transparently.
Consumers implementing their own provider/adapter must move to the
canonical type homes (a one-shot perl + goimports sweep).

### Verification

- darwin host: `go vet ./...`, `go build ./...`,
  `go test -race -count=1 ./...` — green.
- Race-stress (per `go-agent-sessions`'s convention for supervisor /
  PTY runtime tests): `go test -race -count=10 -run "Pty|Supervisor|Manager_WaitSession" ./agentsessions/...` — flake-free.

### Origin

Driven by the `go-providers` v0.12.0 release, which dropped the
transitional `provider.X` aliases for the new `go-llm-types` /
`go-llm-contracts` homes.

## v0.7.0 — 2026-05-08

`Manager.WaitSession` now propagates the underlying `Session.Wait` error verbatim. Supervised PTY non-clean exits surface as `*ExitError` (extractable via `errors.As`); when the supervisor itself triggered the kill, `ExitError.Cause` classifies the termination as `idle_timeout / watchdog_kill / restart_exhausted / oom_kill / resource_limit`. For ordinary non-zero exits or Stop/ctx-cancel under supervision, `*ExitError` is still returned but `Cause` is empty (the supervisor didn't drive the termination). Recovery brokers and post-mortem hooks can classify terminations without a side-channel.

### Behavior change

- **`Manager.WaitSession(ctx, id) (int, error)` returns the underlying error from `Session.Wait` instead of always returning nil on terminal state.** Clean exits (code 0) still return `(0, nil)`. Supervised terminations return `*ExitError` (extract via `errors.As`); non-supervised non-zero exits return whatever `Session.Wait` produced (typically `*exec.ExitError` from go-runner). Existing callers that did `code, err := mgr.WaitSession(...); if err != nil { ... }` and treated `err != nil` as unexpected must update — those terminations were always errors at the `Session.Wait` layer; the lib was previously hiding them.

### Implementation notes

- `sessionResult` (unexported) gained an `exitErr error` field captured by the watch goroutine alongside `exitCode`. No new public types; only the `WaitSession` return value's error semantics change.
- The internal `killing`-flag branch in `watch` (used to record state as `done` on caller-driven Stop) does not swallow `r.exitErr` — Stop-driven exits surface whatever `Session.Wait` returns. For the supervised PTY runtime that may be `nil` or a non-nil `*ExitError` with empty `Cause`, depending on how the child responds to SIGTERM (an exit that satisfies `cmd.Wait()` cleanly produces nil; one that returns a non-zero/signal exit produces `*ExitError` with `Cause == ""`). Existing `pty_supervisor_test.go::TestPTYSupervisor_StopWithRestartZero_NotMisclassifiedAsExhausted` accepts both shapes; consumers handling Stop must too. The new `TestManager_WaitSession_StopDrivenTermination` documents the in-test fakeSession's specific (-1, nil) shape.

### Tests

- New `TestManager_WaitSession_PropagatesExitError` (in `manager_wait_session_pty_test.go`, `!windows` build tag) — supervised PTY session under `IdleKill: 300ms`; asserts `Manager.WaitSession` error `errors.As` to `*ExitError` with `Cause == CauseIdleTimeout`. Pattern adapts `TestPTYSupervisor_IdleKill_TerminatesIdleChild` to route through `Manager`.
- Three fake-runtime cases in `manager_test.go`: clean-exit regression guard, non-zero-exit-no-supervisor (sentinel error round-trip; `errors.As(*ExitError) == false`), Stop-driven termination (killing-flag branch does not synthesize a spurious error).
- All v0.6.0 supervisor tests pass unchanged. Race-clean: `go test -race -count=10 ./agentsessions/...` flake-free.

## v0.6.0 — 2026-05-08

PTY supervision and OS-level resource limits land natively on the long-lived PTY runtime. The v0.5.0 deferred wiring is now implemented as a PTY-native restart loop wrapping `cmd.Wait`, with idle-kill / watchdog goroutines observing ptmx I/O directly — no retrofit of `go-runner.Supervisor` (the impedance mismatch with `creack/pty.Start` is unchanged from the v0.5.0 analysis).

### Public API additions

- **`StartOptions.Supervisor *SupervisorOptions`.** Opt-in supervision: `IdleKill`, `RestartOnCrash` + `MaxRestartBackoff`, `WatchdogTimeout`, `ActivityCallback`, and `OnRestart`. Field shape mirrors `go-runner` v0.3.0's `SupervisorOptions` — same names, same units — so a single supervisor config can target either runtime path once go-runner publishes its v0.3.0 API. Default nil preserves v0.5.0 single-shot behavior exactly.
- **`StartOptions.ResourceLimits *ResourceLimits`.** Opt-in OS-level resource caps (`CPUTime`, `MemoryMax`, `MaxOpenFiles`, `MaxProcesses`, `MaxFileSize`). The wrap is `sh -c "ulimit ...; exec ..."` on both platforms, plus `systemd-run --user --scope --property=MemoryMax=...` on Linux when available (cgroup v2 OOM-kill). Composes with `sandbox.Apply` — limits inherit through the sandbox-exec → real binary chain. Default nil preserves v0.5.0 behavior.
- **`*ExitError`.** Structured exit info returned via `Session.Wait` on the supervised path. Carries `Code`, `Signal`, `Killed`, `ProcessState`, `Cause`. `Cause` is one of `idle_timeout`, `watchdog_kill`, `restart_exhausted`, `oom_kill`, `resource_limit`, or empty for ordinary non-zero exits / Stop-driven termination. Field shape mirrors `go-runner` v0.3.0's `ExitError`.
- **`SessionIDer` on `ptySession`.** The PTY runtime now exposes `ProviderSessionID()` returning the most-recently observed provider session ID. Powers the restart-preserves-conversation behavior described below.

### PTY supervision behavior

- **Idle-kill** terminates the child after `IdleKill` of inactivity (no ptmx Read or Write). SIGTERM → 5 s grace → SIGKILL. `ExitError.Cause = "idle_timeout"`. Not restart-eligible (idle is the user walking away, not a crash).
- **Restart-on-crash** re-spawns the binary up to `RestartOnCrash` times after a non-zero exit. Backoff is exponential (1 s, 2 s, 4 s, ... capped at `MaxRestartBackoff` — default 30 s). Context cancellation aborts further restarts.
- **`agent_session_id` preservation.** When `Caps.ProviderSessionID = true` and a session ID has been observed via `EventSessionID` on a prior attempt, the most-recent ID is fed into the next spawn's `BuildArgs` in place of the original `SessionIDPreset`. Restart preserves the conversation rather than starting fresh.
- **Watchdog** SIGKILLs immediately (no SIGTERM grace) when no activity within `WatchdogTimeout`. Uses `ActivityCallback` ticks when non-nil; falls back to ptmx I/O activity otherwise. `ExitError.Cause = "watchdog_kill"`. Not restart-eligible.
- **`OnRestart`** fires after backoff completes and just before the new spawn. Receives the 1-indexed attempt number and the previous attempt's `*ExitError`.

### Adapter-runtime path

`StartOptions.Supervisor` and `StartOptions.ResourceLimits` are PTY-only in v0.6.0. On the adapter runtime (`Caps.PTY=false`), these fields are currently NOT consulted. Forwarding them to `runner.Config.Supervisor` / `.ResourceLimits` is blocked on `go-runner` publishing its v0.3.0 supervision API — the supervisor commits exist locally on the `feat/supervision-and-limits` branch but the `v0.3.0` tag points at a pre-supervision commit (`723606c`). Once go-runner ships a clean v0.3.x with supervision exported, the adapter-path forwarding is a small, additive follow-up.

### Tests

- New `pty_supervisor_test.go` — idle-kill, restart-on-crash (succeeds on attempt 2), restart-exhausted (3 spawns, cause `restart_exhausted`), watchdog (immediate SIGKILL), activity reset (periodic SendInput keeps idle-kill from firing), OnRestart-not-fired-on-idle-kill, Stop-during-supervised, CPUTime ResourceLimits (CPU-bound spinner under `ulimit -t 1`), MemoryMax linux-only smoke (skipped without `systemd-run --user`), backcompat (Supervisor=nil + ResourceLimits=nil preserves v0.5.0).
- Race-clean: `go test -race -count=10 -run 'PTY.*Supervisor|PTYResource' ./agentsessions/` — flake-free across 10 stress runs.
- All v0.5.0 tests pass unchanged on the supervised-disabled path. Compliance harness still green.

### Known limitations

- **macOS `MemoryMax` is silently dropped.** Same as `go-runner` v0.3.0 — `RLIMIT_AS` isn't exposed via bash's `ulimit -v` on darwin and `systemd-run` is linux-only. Hard memory caps on macOS require VM-based isolation (Lima / OrbStack).
- **No process-tree termination via `Setpgid`.** Same pre-existing limitation as `go-runner` v0.3.0 — children of children may survive a SIGTERM/SIGKILL on the direct child. Documented; not addressed here.
- **Cgroups v2 direct manipulation** (via `containerd/cgroups` or similar) is not used — we shell out via `systemd-run --user` only when available. Deferred per `go-runner` v0.3.0's same out-of-scope decision.

### Out of scope (deferred)

- **Adapter-path Supervisor/ResourceLimits forwarding.** Blocked on go-runner publishing v0.3.0's supervision API. v0.6.x increment.
- **`events.Heartbeat` lib-side synthesis.** Adapter responsibility — the lib does not synthesize.
- **Recovery broker integration.** App-internal pattern; the lib provides the supervision hooks (`OnRestart`, `ExitError.Cause`), the app builds its own broker on top.
- **Live-linux integration tests on a self-hosted runner.** Same as `go-runner` v0.3.0 — gap acknowledged here, not blocking.

## v0.5.0 — 2026-05-08

Long-lived PTY runtime, AutoFireFirstTurn hook, two-dir model (Workdir + WorkspaceDir), capability-driven runtime selection, structured PID propagation across turn boundaries, and an attach-broker stability audit.

> **Version note.** Originally drafted as v0.4.0, cut as v0.5.0 because a stale `v0.4.0` tag from 2026-04-27 already pointed at an unrelated commit (`StartOptions.ExtraFiles` passthrough) without an associated release. Skipping `v0.4.0` is cleaner than rewriting tag history.

### Public API additions

- **Long-lived PTY runtime (path c).** New `ptySession` runtime alongside the existing `adapterSession`. Selected by `NewFromAdapter` when `AdapterRuntimeConfig.Caps.PTY == true` — single entry point routes to the right implementation. Reuses the manager surface uniformly. Pattern lifted from `agent-mux/internal/provider/cli/claudecode/runtime.go` + `agent-mux/internal/session/runtime.go` (RWMutex against ptmx-close-during-use, wait goroutine that nil-clears the master fd, log + fanout tee), generalized so any `provider.CLIAdapter` whose CLI plays well with a long-lived PTY can opt in.
- **`StartOptions.AutoFireFirstTurn bool` + `StartOptions.FirstTurnPayload []byte`.** When `AutoFireFirstTurn` is true, `Runtime.Start` delivers `FirstTurnPayload` as the first input synchronously before returning — eliminates the Launch/SendInput race that bit early consumers. Default false preserves existing behavior. PTY runtimes with `BootMode == "stdin"` and a non-empty `BootPrompt` skip the auto-fire (the boot prompt is already in flight via stdin write at Start).
- **`StartOptions.WorkspaceDir string`.** Two-dir model: `Workdir` is the spawned process's cwd; `WorkspaceDir` is the lib's persistent state root. The PTY runtime falls back to `<WorkspaceDir>/logs/session.log` for its log file when `LogPath` is empty. Adapter runtime ignores this field. Zero-value preserves existing behavior.
- **`StartOptions.TypedEventCallback provider.EventsCallback`.** Mirrors the existing `EventFanout chan<- provider.StreamEvent` surface but uses go-providers Tier-2 typed events (`events.Delta` / `events.ToolUse` / `events.ToolResult` / `events.SubagentSpawn` / `events.SessionID` / `events.Done` / `events.Error` / `events.Heartbeat` / `events.Thinking`). PTY runtime fires the callback per parsed line via the adapter's `EventParser` interface (or no-op when the adapter doesn't implement it). Adapter runtime continues to fire only legacy `StreamEvent` via `EventFanout` — typed events on the adapter path is a future increment.
- **`PIDReporter` optional interface.** New `LivePID() / LastPID()` distinction. `LivePID` returns the PID of a process *currently* running, or 0 when no process is active (subprocess-per-turn between turns). `LastPID` is the sticky most-recently-started PID for log correlation. PTY runtimes return the long-lived child PID for both. `Health().PID` continues to track `LivePID`-style semantics — the new interface is for callers who need both.

### Capability-driven selection

`NewFromAdapter(cfg)` now switches on `cfg.Caps.PTY`:

- `false` (default) → subprocess-per-turn `adapterRuntime` (existing behavior, unchanged).
- `true` → long-lived PTY `ptyRuntime`.

Existing adapters (`Caps.PTY=false`) see no behavior change. The claude-code adapter is the only adapter that opts into PTY initially; opencode / codex / gemini / aider / junie / copilot stay subprocess-per-turn until they explicitly opt in.

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

- `StartOptions.Supervisor` / `StartOptions.ResourceLimits` wiring on the PTY path. These don't compose naturally with `creack/pty.Start` (go-runner's supervision wraps a `runner.Run` call; PTY bypasses that path). Forward-compat: when consumers need PTY supervision, the design will include a restart-loop wrapper inside ptyRuntime that observes `cmd.Wait` rather than threading through go-runner. (Landed in v0.6.0.)
- Typed events on the adapter path. The adapter runtime drives `runner.Run` which doesn't currently surface raw lines through go-providers' `WithEvents` plumbing; typed events on adapter sessions remain a v0.5.0+ increment.
- Per-adapter PTY opt-in beyond claude-code. Other adapters (opencode / codex / gemini / aider / junie / copilot) stay subprocess-per-turn; their Caps.PTY remains false.
- Recovery broker — app-internal pattern; the lib stops at the supervision hooks.
- Live attach broker HTTP endpoint — consumer concern; downstream apps build their own on top of `Manager.Attach`.

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

### Dependencies

Tier 1 dep: `go-runner` v0.3.0. Tier 2 dep: `go-providers` v0.8.0.

## v0.3.0 — 2026-04-27

Adds `StartOptions.Stderr io.Writer` for caller-controlled stderr capture in adapter-driven sessions. Companion to `go-runner` v0.2.0 (which adds the same field on `runner.Config`); the agent-sessions field forwards directly.

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

## v0.2.0 — 2026-04-27

Adds optional typed `EventFanout chan<- provider.StreamEvent` to `StartOptions` so consumers can receive parsed stream events directly without re-parsing bytes from the byte `Fanout`. Both `NewFromAdapter` and `NewFromProvider` honor it. Drop-not-block on slow consumers; nil preserves v0.1.0 behavior exactly.

Decision: `docs/decisions/0005-typed-event-fanout.md`.

## v0.1.0 — 2026-04-27

Initial release. Extracted from agent-mux's `internal/runtime` and `internal/provider` packages (1,800+ LOC + tests, ADRs 0004/0006/0013/0025) and rebased onto the public hollis-labs primitive libs.

### Public API

- `agentsessions.Session`, `Runtime`, `Manager`, `StartOptions`, `Capabilities`, `HealthStatus`, `LiveState`, `CheckpointHint` / `CheckpointHinter`, `SessionIDer`, `State`.
- `NewManager(StateSink) *Manager` with chainable `.WithEventSink(...)`, `.WithAttachmentSink(...)`.
- `NewFromAdapter(AdapterRuntimeConfig) (Runtime, error)` for `provider.CLIAdapter` runtimes.
- `NewFromProvider(ProviderRuntimeConfig) (Runtime, error)` for `provider.Provider` runtimes (HTTP / API).
- `compliance.Run(t, compliance.Harness{...})` for adapter conformance gating.
